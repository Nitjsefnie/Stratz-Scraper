"""Task assignment helpers."""

from __future__ import annotations

import logging
import threading
from datetime import datetime, timedelta, timezone

from ..database import (
    db_connection,
    release_incomplete_assignments,
    retryable_execute,
    row_value,
)

ASSIGNMENT_CLEANUP_KEY = "last_assignment_cleanup"
ASSIGNMENT_CLEANUP_INTERVAL = timedelta(seconds=60)
ASSIGNMENT_RETRY_INTERVAL = 0.05

_LOGGER = logging.getLogger(__name__)

_cleanup_thread: threading.Thread | None = None
_cleanup_stop_event: threading.Event | None = None
_cleanup_lock = threading.Lock()

__all__ = [
    "ASSIGNMENT_CLEANUP_INTERVAL",
    "ASSIGNMENT_CLEANUP_KEY",
    "ASSIGNMENT_RETRY_INTERVAL",
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


def assign_next_task(
    *,
    run_cleanup: bool = False,
    connection=None,
) -> dict | None:
    """Select the next task to hand to a worker.

    When ``connection`` is provided the caller is responsible for committing
    or rolling back the surrounding transaction. Otherwise a managed write
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
        assigned = retryable_execute(
            cur,
            """
            WITH frontier AS (
                SELECT steamAccountId, depth FROM players
                WHERE scraped_at IS NULL
                  AND assigned_to IS NULL
                  AND steamAccountId > 0
                ORDER BY depth ASC, steamAccountId ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            ),
            refresh_pool AS (
                SELECT steamAccountId, depth FROM players
                WHERE scraped_at IS NOT NULL
                  AND assigned_to IS NULL
                  AND steamAccountId > 0
                ORDER BY scraped_at ASC, steamAccountId ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            ),
            chosen AS (
                SELECT * FROM frontier
                UNION ALL
                SELECT * FROM refresh_pool
                WHERE NOT EXISTS (SELECT 1 FROM frontier)
            )
            UPDATE players
            SET assigned_to = 'scrape',
                assigned_at = CURRENT_TIMESTAMP
            FROM chosen
            WHERE players.steamAccountId = chosen.steamAccountId
            RETURNING players.steamAccountId, players.depth, players.latest_match_id
            """,
            retry_interval=ASSIGNMENT_RETRY_INTERVAL,
        ).fetchone()

        if assigned is None:
            return None

        try:
            steam_account_id = int(row_value(assigned, "steamAccountId"))
        except (TypeError, ValueError):
            return None
        if steam_account_id <= 0:
            return None

        depth_value = row_value(assigned, "depth")
        try:
            depth = int(depth_value) if depth_value is not None else 0
        except (TypeError, ValueError):
            depth = 0

        latest_match_value = row_value(assigned, "latest_match_id")
        try:
            latest_match_id = (
                int(latest_match_value) if latest_match_value is not None else None
            )
        except (TypeError, ValueError):
            latest_match_id = None

        return {
            "type": "scrape_player",
            "steamAccountId": steam_account_id,
            "depth": depth,
            "latestMatchId": latest_match_id,
        }
