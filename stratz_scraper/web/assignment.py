"""Task assignment helpers."""

from __future__ import annotations

import logging
import threading
from datetime import datetime, timedelta, timezone
from typing import Final

from ..database import (
    db_connection,
    release_incomplete_assignments,
    retryable_execute,
    row_value,
)

ASSIGNMENT_CLEANUP_KEY = "last_assignment_cleanup"
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
        discovery_throttled = _discovery_backlog_exceeded(cur)
        discovery_limit = 0 if discovery_throttled else MAX_DISCOVERY_TASK_SIZE

        assigned_rows = retryable_execute(
            cur,
            """
            WITH hero_candidates AS (
                SELECT steamAccountId, depth, highest_match_id,
                       'hero'::text AS kind
                FROM players
                WHERE hero_done=FALSE
                  AND assigned_to IS NULL
                  AND steamAccountId > 0
                ORDER BY steamAccountId ASC
                LIMIT %s
                FOR UPDATE SKIP LOCKED
            ),
            discovery_candidates AS (
                SELECT steamAccountId, depth, highest_match_id,
                       'discover'::text AS kind
                FROM players
                WHERE hero_done=TRUE
                  AND discover_done=FALSE
                  AND assigned_to IS NULL
                  AND steamAccountId > 0
                ORDER BY depth ASC, steamAccountId ASC
                LIMIT %s
                FOR UPDATE SKIP LOCKED
            ),
            refresh_candidates AS (
                SELECT steamAccountId, depth, highest_match_id,
                       'refresh'::text AS kind
                FROM players
                WHERE hero_done=TRUE
                  AND discover_done=TRUE
                  AND assigned_to IS NULL
                  AND steamAccountId > 0
                ORDER BY hero_refreshed_at ASC NULLS FIRST, steamAccountId ASC
                LIMIT %s
                FOR UPDATE SKIP LOCKED
            ),
            chosen AS (
                SELECT * FROM hero_candidates
                UNION ALL
                SELECT * FROM discovery_candidates
                WHERE NOT EXISTS (SELECT 1 FROM hero_candidates)
                UNION ALL
                SELECT * FROM refresh_candidates
                WHERE NOT EXISTS (SELECT 1 FROM hero_candidates)
                  AND NOT EXISTS (SELECT 1 FROM discovery_candidates)
            )
            UPDATE players
            SET assigned_to = chosen.kind,
                assigned_at = CURRENT_TIMESTAMP,
                hero_done = CASE
                    WHEN chosen.kind='refresh' THEN FALSE
                    ELSE players.hero_done
                END
            FROM chosen
            WHERE players.steamAccountId = chosen.steamAccountId
            RETURNING players.steamAccountId, players.depth,
                      players.highest_match_id, chosen.kind
            """,
            (
                MAX_HERO_TASK_SIZE,
                discovery_limit,
                MAX_HERO_TASK_SIZE,
            ),
            retry_interval=ASSIGNMENT_RETRY_INTERVAL,
        ).fetchall()

        if not assigned_rows:
            return None

        kind = str(row_value(assigned_rows[0], "kind"))
        players: list[dict] = []
        for assigned in assigned_rows:
            try:
                steam_account_id = int(row_value(assigned, "steamAccountId"))
            except (TypeError, ValueError):
                continue
            if steam_account_id <= 0:
                continue
            depth_value = row_value(assigned, "depth")
            try:
                depth = int(depth_value) if depth_value is not None else None
            except (TypeError, ValueError):
                depth = None
            highest_value = row_value(assigned, "highest_match_id")
            try:
                highest = int(highest_value) if highest_value is not None else None
            except (TypeError, ValueError):
                highest = None
            if highest is not None and highest < 0:
                highest = None
            players.append(
                {
                    "steamAccountId": steam_account_id,
                    "depth": depth,
                    "highestMatchId": highest,
                }
            )

        if not players:
            return None

        if kind == "hero":
            ids = sorted({p["steamAccountId"] for p in players})
            return {
                "type": "fetch_hero_stats",
                "steamAccountId": ids[0],
                "steamAccountIds": ids,
            }

        players.sort(key=lambda e: (e.get("depth") or 0, e["steamAccountId"]))
        ids = [p["steamAccountId"] for p in players]
        payload_type = (
            "discover_matches" if kind == "discover" else "refresh_player_data"
        )
        payload: dict = {
            "type": payload_type,
            "steamAccountId": ids[0],
            "steamAccountIds": ids,
            "players": players,
        }
        first_depth = players[0].get("depth")
        if first_depth is not None:
            payload["depth"] = first_depth
        first_highest = players[0].get("highestMatchId")
        if first_highest is not None:
            payload["highestMatchId"] = first_highest
        return payload
