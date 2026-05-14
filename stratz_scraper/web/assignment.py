"""Task assignment helpers."""

from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Final

from ..database import (
    db_connection,
    release_incomplete_assignments,
    retryable_execute,
    row_value,
)

ASSIGNMENT_CLEANUP_KEY = "last_assignment_cleanup"
HERO_ASSIGNMENT_CURSOR_KEY = "hero_assignment_cursor"
ASSIGNMENT_CLEANUP_INTERVAL = timedelta(seconds=60)
ASSIGNMENT_RETRY_INTERVAL = 0.05
MAX_HERO_TASK_SIZE: Final[int] = 5
MAX_DISCOVERY_TASK_SIZE: Final[int] = 5

_LOGGER = logging.getLogger(__name__)


class _DiscoveryThrottle:
    """Sentinel object returned when discovery assignment is throttled."""

    __slots__ = ()


_DISCOVERY_THROTTLED: Final = _DiscoveryThrottle()

_cleanup_thread: threading.Thread | None = None
_cleanup_stop_event: threading.Event | None = None
_cleanup_lock = threading.Lock()

_restart_executor = ThreadPoolExecutor(max_workers=1)
_RESTART_LOCK_ID = int.from_bytes(b"restart", "big")

__all__ = [
    "ASSIGNMENT_CLEANUP_INTERVAL",
    "ASSIGNMENT_CLEANUP_KEY",
    "assign_next_task",
    "ensure_assignment_cleanup_scheduler",
    "maybe_run_assignment_cleanup",
]


def _cleanup_worker(stop_event: threading.Event) -> None:
    interval_seconds = max(int(ASSIGNMENT_CLEANUP_INTERVAL.total_seconds()), 1)
    while not stop_event.is_set():
        try:
            with db_connection(write=True) as conn:
                maybe_run_assignment_cleanup(conn)
        except Exception:  # pragma: no cover - best effort logging
            _LOGGER.exception("Assignment cleanup worker failed")
        stop_event.wait(interval_seconds)


def ensure_assignment_cleanup_scheduler() -> None:
    """Start the background worker that periodically releases stale assignments."""

    global _cleanup_thread, _cleanup_stop_event
    with _cleanup_lock:
        if _cleanup_thread and _cleanup_thread.is_alive():
            return
        stop_event = threading.Event()
        thread = threading.Thread(
            target=_cleanup_worker,
            args=(stop_event,),
            name="assignment-cleanup",
            daemon=True,
        )
        thread.start()
        _cleanup_thread = thread
        _cleanup_stop_event = stop_event


def maybe_run_assignment_cleanup(conn) -> bool:
    """Release stale assignments if the cleanup interval has elapsed."""
    cur = conn.cursor()
    now = datetime.now(timezone.utc)
    last_cleanup_row = cur.execute(
        "SELECT value FROM meta WHERE key=%s",
        (ASSIGNMENT_CLEANUP_KEY,),
    ).fetchone()
    if last_cleanup_row:
        try:
            last_cleanup = datetime.fromisoformat(last_cleanup_row["value"])
        except (TypeError, ValueError):
            pass
        else:
            if last_cleanup.tzinfo is None:
                last_cleanup = last_cleanup.replace(tzinfo=timezone.utc)
            if now - last_cleanup < ASSIGNMENT_CLEANUP_INTERVAL:
                return False
    release_incomplete_assignments(existing=conn)
    retryable_execute(
        cur,
        """
        INSERT INTO meta (key, value)
        VALUES (%s, %s)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value
        """,
        (ASSIGNMENT_CLEANUP_KEY, now.isoformat()),
        retry_interval=ASSIGNMENT_RETRY_INTERVAL,
    )
    return True


def _discovery_backlog_exceeded(cur) -> bool:
    backlog_row = retryable_execute(
        cur,
        """
        SELECT EXISTS (
            SELECT 1
            FROM players
            WHERE discover_done=TRUE
              AND full_write_done=FALSE
              AND highest_match_id IS NOT NULL
            OFFSET 100 LIMIT 1
        ) AS backlog_exceeded
        """,
        retry_interval=ASSIGNMENT_RETRY_INTERVAL,
    ).fetchone()

    if backlog_row is None:
        return False
    return bool(row_value(backlog_row, "backlog_exceeded"))


def _assign_discovery(cur) -> dict | _DiscoveryThrottle | None:
    if _discovery_backlog_exceeded(cur):
        return _DISCOVERY_THROTTLED

    assigned_rows = retryable_execute(
        cur,
        """
        WITH candidate AS (
            SELECT steamAccountId, depth, highest_match_id
            FROM players
            WHERE hero_done=TRUE
              AND discover_done=FALSE
              AND assigned_to IS NULL
            ORDER BY depth ASC,
                     steamAccountId ASC
            LIMIT %s
            FOR UPDATE SKIP LOCKED
        )
        UPDATE players
        SET assigned_to='discover',
            assigned_at=CURRENT_TIMESTAMP
        WHERE steamAccountId IN (SELECT steamAccountId FROM candidate)
          AND assigned_to IS NULL
        RETURNING steamAccountId, depth, highest_match_id
        """,
        (MAX_DISCOVERY_TASK_SIZE,),
        retry_interval=ASSIGNMENT_RETRY_INTERVAL,
    ).fetchall()

    if not assigned_rows:
        return None

    players: list[dict] = []
    for assigned in assigned_rows:
        steam_account_id_raw = row_value(assigned, "steamAccountId")
        try:
            steam_account_id = int(steam_account_id_raw)
        except (TypeError, ValueError):
            continue
        if steam_account_id == 0:
            cur.execute(
                "UPDATE players SET discover_done=TRUE, full_write_done=TRUE WHERE steamAccountId=0"
            )
            continue

        depth_value = row_value(assigned, "depth")
        try:
            depth = int(depth_value)
        except (TypeError, ValueError):
            depth = None

        highest_match_id_value = row_value(assigned, "highest_match_id")
        try:
            highest_match_id = (
                int(highest_match_id_value)
                if highest_match_id_value is not None
                else None
            )
        except (TypeError, ValueError):
            highest_match_id = None
        if highest_match_id is not None and highest_match_id < 0:
            highest_match_id = None

        players.append(
            {
                "steamAccountId": steam_account_id,
                "depth": depth,
                "highestMatchId": highest_match_id,
            }
        )

    if not players:
        return _assign_discovery(cur)

    players.sort(key=lambda entry: (entry.get("depth") or 0, entry["steamAccountId"]))
    steam_account_ids = [player["steamAccountId"] for player in players]
    payload = {
        "type": "discover_matches",
        "steamAccountId": steam_account_ids[0],
        "steamAccountIds": steam_account_ids,
        "players": players,
    }
    first_depth = players[0].get("depth")
    if first_depth is not None:
        payload["depth"] = first_depth
    first_highest = players[0].get("highestMatchId")
    if first_highest is not None:
        payload["highestMatchId"] = first_highest
    return payload

def _restart_discovery_cycle(cur) -> bool:
    cur.execute(f"SELECT pg_try_advisory_xact_lock({_RESTART_LOCK_ID})")
    if not cur.fetchone()[0]:
        return True

    def _task():
        retryable_execute(
            cur,
            """
            UPDATE players
            SET discover_done=FALSE,
                full_write_done=FALSE,
                assigned_at=CASE WHEN assigned_to='discover' THEN NULL ELSE assigned_at END,
                assigned_to=CASE WHEN assigned_to='discover' THEN NULL ELSE assigned_to END
            """,
            retry_interval=ASSIGNMENT_RETRY_INTERVAL,
        )

    _restart_executor.submit(_task)
    return True

def _assign_next_hero(cur) -> dict | None:
    last_cursor_row = retryable_execute(
        cur,
        "SELECT value FROM meta WHERE key=%s",
        (HERO_ASSIGNMENT_CURSOR_KEY,),
        retry_interval=ASSIGNMENT_RETRY_INTERVAL,
    ).fetchone()
    try:
        last_cursor = int(last_cursor_row["value"]) if last_cursor_row else 0
    except (TypeError, ValueError):
        last_cursor = 0

    for _ in range(2):
        assigned_rows = retryable_execute(
            cur,
            """
            WITH candidate AS (
                SELECT steamAccountId
                FROM players
                WHERE hero_done=FALSE
                  AND assigned_to IS NULL
                  AND steamAccountId > %s
                ORDER BY steamAccountId ASC
                LIMIT %s
                FOR UPDATE SKIP LOCKED
            ),
            fallback AS (
                SELECT steamAccountId
                FROM players
                WHERE hero_done=FALSE
                  AND assigned_to IS NULL
                  AND steamAccountId > 0
                ORDER BY steamAccountId ASC
                LIMIT %s
                FOR UPDATE SKIP LOCKED
            ),
            selected AS (
                SELECT steamAccountId FROM candidate
                UNION ALL
                SELECT steamAccountId FROM fallback
                WHERE NOT EXISTS (SELECT 1 FROM candidate)
                LIMIT %s
            )
            UPDATE players
            SET assigned_to='hero',
                assigned_at=CURRENT_TIMESTAMP
            WHERE steamAccountId IN (SELECT steamAccountId FROM selected)
              AND hero_done=FALSE
              AND assigned_to IS NULL
            RETURNING steamAccountId
            """,
            (
                last_cursor,
                MAX_HERO_TASK_SIZE,
                MAX_HERO_TASK_SIZE,
                MAX_HERO_TASK_SIZE,
            ),
            retry_interval=ASSIGNMENT_RETRY_INTERVAL,
        ).fetchall()
        if assigned_rows:
            steam_account_ids = sorted(
                {
                    int(row_value(assigned_row, "steamAccountId"))
                    for assigned_row in assigned_rows
                }
            )
            if not steam_account_ids:
                continue
            steam_account_id = steam_account_ids[-1]
            retryable_execute(
                cur,
                """
                INSERT INTO meta (key, value)
                VALUES (%s, %s)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value
                """,
                (HERO_ASSIGNMENT_CURSOR_KEY, str(steam_account_id)),
                retry_interval=ASSIGNMENT_RETRY_INTERVAL,
            )
            return {
                "type": "fetch_hero_stats",
                "steamAccountId": steam_account_ids[0],
                "steamAccountIds": steam_account_ids,
            }

    return None


def assign_next_task(
    *,
    run_cleanup: bool = False,
    connection=None,
) -> dict | None:
    """Select the next task to hand to a worker.

    When ``connection`` is provided the caller is responsible for committing or
    rolling back the surrounding transaction. Otherwise a managed write
    connection is opened for the duration of the scheduler work.
    """

    if connection is None:
        with db_connection(write=True) as managed_conn:
            return _assign_next_task_on_connection(
                managed_conn,
                run_cleanup=run_cleanup,
            )
    return _assign_next_task_on_connection(connection, run_cleanup=run_cleanup)


def _assign_next_task_on_connection(connection, *, run_cleanup: bool) -> dict | None:
    if run_cleanup:
        maybe_run_assignment_cleanup(connection)

    with connection.cursor() as cur:
        candidate_payload = _assign_next_hero(cur)

        if candidate_payload is None:
            candidate_payload = _assign_discovery(cur)
            if candidate_payload is _DISCOVERY_THROTTLED:
                return None

        if candidate_payload is None:
            assigned_rows = retryable_execute(
                cur,
                """
                WITH candidate AS (
                    SELECT steamAccountId
                    FROM players
                    WHERE hero_done=TRUE
                      AND discover_done=TRUE
                      AND assigned_to IS NULL
                    ORDER BY hero_refreshed_at ASC NULLS FIRST,
                             steamAccountId ASC
                    LIMIT %s
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE players
                SET hero_done=FALSE,
                    assigned_to='refresh',
                    assigned_at=CURRENT_TIMESTAMP
                WHERE steamAccountId IN (SELECT steamAccountId FROM candidate)
                  AND hero_done=TRUE
                  AND discover_done=TRUE
                  AND assigned_to IS NULL
                RETURNING steamAccountId, depth, highest_match_id
                """,
                (MAX_HERO_TASK_SIZE,),
                retry_interval=ASSIGNMENT_RETRY_INTERVAL,
            ).fetchall()

            if not assigned_rows:
                return None

            players: list[dict] = []
            for assigned_row in assigned_rows:
                try:
                    steam_account_id = int(row_value(assigned_row, "steamAccountId"))
                except (TypeError, ValueError):
                    continue
                if steam_account_id <= 0:
                    continue
                try:
                    depth = int(row_value(assigned_row, "depth"))
                except (TypeError, ValueError):
                    depth = None
                highest_match_id_value = row_value(assigned_row, "highest_match_id")
                try:
                    highest_match_id = (
                        int(highest_match_id_value)
                        if highest_match_id_value is not None
                        else None
                    )
                except (TypeError, ValueError):
                    highest_match_id = None
                if highest_match_id is not None and highest_match_id < 0:
                    highest_match_id = None
                players.append(
                    {
                        "steamAccountId": steam_account_id,
                        "depth": depth,
                        "highestMatchId": highest_match_id,
                    }
                )

            if not players:
                return None

            players.sort(
                key=lambda entry: (entry.get("depth") or 0, entry["steamAccountId"])
            )
            steam_account_ids = [p["steamAccountId"] for p in players]
            candidate_payload = {
                "type": "refresh_player_data",
                "steamAccountId": steam_account_ids[0],
                "steamAccountIds": steam_account_ids,
                "players": players,
            }
            first = players[0]
            if first.get("depth") is not None:
                candidate_payload["depth"] = first["depth"]
            if first.get("highestMatchId") is not None:
                candidate_payload["highestMatchId"] = first["highestMatchId"]

        if candidate_payload and candidate_payload is not _DISCOVERY_THROTTLED:
            return candidate_payload

    return None



