"""Task assignment helpers."""

from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

from ..database import (
    db_connection,
    release_incomplete_assignments,
    retryable_execute,
)

ASSIGNMENT_CLEANUP_KEY = "last_assignment_cleanup"
HERO_ASSIGNMENT_CURSOR_KEY = "hero_assignment_cursor"
ASSIGNMENT_CLEANUP_INTERVAL = timedelta(seconds=60)
ASSIGNMENT_RETRY_INTERVAL = 0.05

_LOGGER = logging.getLogger(__name__)

_cleanup_thread: threading.Thread | None = None
_cleanup_stop_event: threading.Event | None = None
_cleanup_lock = threading.Lock()

_checkpoint_executor = ThreadPoolExecutor(max_workers=1)
_checkpoint_state_lock = threading.Lock()
_checkpoint_pending = False


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


def _run_wal_checkpoint() -> None:
    global _checkpoint_pending
    try:
        with db_connection(write=True) as checkpoint_conn:
            retryable_execute(
                checkpoint_conn,
                "PRAGMA wal_checkpoint(TRUNCATE);",
            )
    except Exception:  # pragma: no cover - best effort logging
        _LOGGER.exception("Background WAL checkpoint failed")
    finally:
        with _checkpoint_state_lock:
            _checkpoint_pending = False


def _schedule_checkpoint() -> None:
    global _checkpoint_pending
    with _checkpoint_state_lock:
        if _checkpoint_pending:
            return
        _checkpoint_pending = True
    _checkpoint_executor.submit(_run_wal_checkpoint)


def maybe_run_assignment_cleanup(conn) -> bool:
    """Release stale assignments if the cleanup interval has elapsed."""
    cur = conn.cursor()
    now = datetime.now(timezone.utc)
    last_cleanup_row = cur.execute(
        "SELECT value FROM meta WHERE key=?",
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
        VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value
        """,
        (ASSIGNMENT_CLEANUP_KEY, now.isoformat()),
        retry_interval=ASSIGNMENT_RETRY_INTERVAL,
    )
    return True


def _assign_discovery(cur) -> dict | None:
    assigned = retryable_execute(
        cur,
        """
        WITH candidate AS (
            SELECT steamAccountId, depth
            FROM players
            WHERE hero_done=1
              AND discover_done=0
              AND (assigned_to IS NULL OR assigned_to='discover')
            ORDER BY (assigned_to IS NOT NULL),
                     seen_count DESC,
                     COALESCE(depth, 0) ASC,
                     steamAccountId ASC
            LIMIT 1
        )
        UPDATE players
        SET assigned_to='discover',
            assigned_at=CURRENT_TIMESTAMP
        WHERE steamAccountId IN (SELECT steamAccountId FROM candidate)
          AND (assigned_to IS NULL OR assigned_to='discover')
        RETURNING steamAccountId, depth
        """,
        retry_interval=ASSIGNMENT_RETRY_INTERVAL,
    ).fetchone()
    if not assigned:
        return None
    depth_value = assigned["depth"]
    return {
        "type": "discover_matches",
        "steamAccountId": int(assigned["steamAccountId"]),
        "depth": int(depth_value) if depth_value is not None else 0,
    }


def _restart_discovery_cycle(cur) -> bool:
    retryable_execute(
        cur,
        """
        UPDATE players
        SET discover_done=0,
            seen_count=0,
            depth=CASE WHEN depth=0 THEN 0 ELSE NULL END,
            assigned_at=CASE WHEN assigned_to='discover' THEN NULL ELSE assigned_at END,
            assigned_to=CASE WHEN assigned_to='discover' THEN NULL ELSE assigned_to END
        """,
        retry_interval=ASSIGNMENT_RETRY_INTERVAL,
    )
    return True


def _assign_next_hero(cur) -> dict | None:
    last_cursor_row = retryable_execute(
        cur,
        "SELECT value FROM meta WHERE key=?",
        (HERO_ASSIGNMENT_CURSOR_KEY,),
        retry_interval=ASSIGNMENT_RETRY_INTERVAL,
    ).fetchone()
    try:
        last_cursor = int(last_cursor_row["value"]) if last_cursor_row else 0
    except (TypeError, ValueError):
        last_cursor = 0

    for offset in (last_cursor, 0):
        assigned_row = retryable_execute(
            cur,
            """
            WITH candidate AS (
                SELECT steamAccountId
                FROM players
                WHERE hero_done=0
                  AND assigned_to IS NULL
                  AND steamAccountId > ?
                ORDER BY steamAccountId ASC
                LIMIT 1
            )
            UPDATE players
            SET assigned_to='hero',
                assigned_at=CURRENT_TIMESTAMP
            WHERE steamAccountId IN (SELECT steamAccountId FROM candidate)
              AND hero_done=0
              AND assigned_to IS NULL
            RETURNING steamAccountId
            """,
            (offset,),
            retry_interval=ASSIGNMENT_RETRY_INTERVAL,
        ).fetchone()
        if assigned_row:
            steam_account_id = int(assigned_row["steamAccountId"])
            retryable_execute(
                cur,
                """
                INSERT INTO meta (key, value)
                VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value
                """,
                (HERO_ASSIGNMENT_CURSOR_KEY, str(steam_account_id)),
                retry_interval=ASSIGNMENT_RETRY_INTERVAL,
            )
            return {
                "type": "fetch_hero_stats",
                "steamAccountId": steam_account_id,
            }

    return None


def assign_next_task(*, run_cleanup: bool = False) -> dict | None:
    """Select the next task to hand to a worker."""
    task_payload: dict | None = None
    should_checkpoint = False

    with db_connection(write=True) as conn:
        if run_cleanup:
            maybe_run_assignment_cleanup(conn)

        retryable_execute(
            conn,
            "BEGIN IMMEDIATE",
            retry_interval=ASSIGNMENT_RETRY_INTERVAL,
        )
        cur = conn.cursor()

        counter_row = cur.execute(
            "SELECT value FROM meta WHERE key=?",
            ("task_assignment_counter",),
        ).fetchone()
        try:
            current_count = int(counter_row["value"]) if counter_row else 0
        except (TypeError, ValueError):
            current_count = 0

        loop_count = current_count
        should_checkpoint = False
        task_payload = None

        try:
            while True:
                next_count = loop_count + 1
                refresh_due = next_count % 10 == 0
                discovery_due = next_count % 10000 == 0

                should_truncate_wal = False
                candidate_payload = None

                if discovery_due:
                    candidate_payload = _assign_discovery(cur)
                    if candidate_payload is None and _restart_discovery_cycle(cur):
                        should_truncate_wal = True
                        candidate_payload = _assign_discovery(cur)

                if candidate_payload is None and refresh_due:
                    assigned_row = retryable_execute(
                        cur,
                        """
                        WITH candidate AS (
                            SELECT steamAccountId
                            FROM players
                            WHERE hero_done=1
                              AND assigned_to IS NULL
                            ORDER BY COALESCE(hero_refreshed_at, '1970-01-01') ASC,
                                     seen_count DESC,
                                     steamAccountId ASC
                            LIMIT 1
                        )
                        UPDATE players
                        SET hero_done=0,
                            assigned_to='hero',
                            assigned_at=CURRENT_TIMESTAMP
                        WHERE steamAccountId IN (SELECT steamAccountId FROM candidate)
                          AND hero_done=1
                          AND assigned_to IS NULL
                        RETURNING steamAccountId
                        """,
                        retry_interval=ASSIGNMENT_RETRY_INTERVAL,
                    ).fetchone()
                    if assigned_row:
                        candidate_payload = {
                            "type": "fetch_hero_stats",
                            "steamAccountId": int(assigned_row["steamAccountId"]),
                        }

                if candidate_payload is None:
                    candidate_payload = _assign_next_hero(cur)

                if candidate_payload is None:
                    hero_pending = cur.execute(
                        "SELECT 1 FROM players WHERE hero_done=0 LIMIT 1"
                    ).fetchone()
                    if not hero_pending and not discovery_due:
                        candidate_payload = _assign_discovery(cur)

                if candidate_payload is not None:
                    counter_value = _increment_assignment_counter(cur)
                    if should_truncate_wal or counter_value % 10000 == 0:
                        should_checkpoint = True
                    task_payload = candidate_payload
                    break

                if refresh_due or discovery_due:
                    break

                loop_count = next_count
        except Exception:
            conn.rollback()
            raise
        else:
            conn.commit()

    if should_checkpoint:
        _schedule_checkpoint()

    return task_payload


def _increment_assignment_counter(cur) -> int:
    row = retryable_execute(
        cur,
        """
        INSERT INTO meta (key, value)
        VALUES (?, '1')
        ON CONFLICT(key) DO UPDATE SET value=CAST(value AS INTEGER) + 1
        RETURNING CAST(value AS INTEGER) AS value
        """,
        ("task_assignment_counter",),
        retry_interval=ASSIGNMENT_RETRY_INTERVAL,
    ).fetchone()
    if not row:
        return 0
    try:
        return int(row["value"])
    except (TypeError, ValueError):
        return 0
