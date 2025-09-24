"""Task assignment helpers."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Callable, Tuple

from ..database import (
    db_connection,
    release_incomplete_assignments,
    retryable_execute,
)

ASSIGNMENT_CLEANUP_KEY = "last_assignment_cleanup"
ASSIGNMENT_CLEANUP_INTERVAL = timedelta(seconds=60)

__all__ = [
    "ASSIGNMENT_CLEANUP_INTERVAL",
    "ASSIGNMENT_CLEANUP_KEY",
    "assign_next_task",
    "maybe_run_assignment_cleanup",
]


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
              AND assigned_to IS NULL
            ORDER BY COALESCE(depth, 0) ASC, steamAccountId ASC
            LIMIT 1
        )
        UPDATE players
        SET assigned_to='discover',
            assigned_at=CURRENT_TIMESTAMP
        WHERE steamAccountId IN (SELECT steamAccountId FROM candidate)
          AND assigned_to IS NULL
        RETURNING steamAccountId, depth
        """,
    ).fetchone()
    if not assigned:
        assigned = retryable_execute(
            cur,
            """
            WITH candidate AS (
                SELECT steamAccountId, depth
                FROM players
                WHERE hero_done=1
                  AND discover_done=0
                  AND assigned_to='discover'
                ORDER BY COALESCE(depth, 0) ASC, steamAccountId ASC
                LIMIT 1
            )
            UPDATE players
            SET assigned_to='discover',
                assigned_at=CURRENT_TIMESTAMP
            WHERE steamAccountId IN (SELECT steamAccountId FROM candidate)
              AND assigned_to='discover'
            RETURNING steamAccountId, depth
            """,
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
            depth=CASE WHEN depth=0 THEN 0 ELSE NULL END,
            assigned_at=CASE WHEN assigned_to='discover' THEN NULL ELSE assigned_at END,
            assigned_to=CASE WHEN assigned_to='discover' THEN NULL ELSE assigned_to END
        """,
    )
    return True


def assign_next_task(*, run_cleanup: bool = True) -> dict | None:
    """Select the next task to hand to a worker."""
    task_payload: dict | None = None
    should_checkpoint = False

    with db_connection(write=True) as conn:
        if run_cleanup:
            maybe_run_assignment_cleanup(conn)
        cur = conn.cursor()

        def callback(next_count: int) -> Tuple[dict | None, bool]:
            refresh_due = next_count % 10 == 0
            discovery_due = next_count % 2000 == 0
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
                ).fetchone()
                if assigned_row:
                    candidate_payload = {
                        "type": "fetch_hero_stats",
                        "steamAccountId": int(assigned_row["steamAccountId"]),
                    }

            if candidate_payload is None:
                assigned_row = retryable_execute(
                    cur,
                    """
                    WITH candidate AS (
                        SELECT steamAccountId
                        FROM players
                        WHERE hero_done=0
                          AND assigned_to IS NULL
                        ORDER BY COALESCE(depth, 0) ASC, steamAccountId ASC
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
                ).fetchone()
                if assigned_row:
                    candidate_payload = {
                        "type": "fetch_hero_stats",
                        "steamAccountId": int(assigned_row["steamAccountId"]),
                    }

            if candidate_payload is None:
                hero_pending = cur.execute(
                    "SELECT 1 FROM players WHERE hero_done=0 LIMIT 1"
                ).fetchone()
                if not hero_pending and not discovery_due:
                    candidate_payload = _assign_discovery(cur)

            return candidate_payload, should_truncate_wal

        task_payload, should_checkpoint = _with_counter(cur, callback)

    if should_checkpoint:
        with db_connection(write=True) as checkpoint_conn:
            retryable_execute(
                checkpoint_conn,
                "PRAGMA wal_checkpoint(TRUNCATE);",
            )

    return task_payload


def _with_counter(cur, callback: Callable[[int], Tuple[dict | None, bool]]) -> tuple[dict | None, bool]:
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
    task_payload: dict | None = None

    while True:
        next_count = loop_count + 1
        refresh_due = next_count % 10 == 0
        discovery_due = next_count % 100 == 0
        checkpoint_due = next_count % 10000 == 0

        candidate_payload, should_truncate_wal = callback(next_count)
        if candidate_payload is not None:
            task_payload = candidate_payload
            retryable_execute(
                cur,
                """
                INSERT INTO meta (key, value)
                VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value
                """,
                ("task_assignment_counter", str(next_count)),
            )
            if checkpoint_due or should_truncate_wal:
                should_checkpoint = True
            break

        if refresh_due or discovery_due:
            break

        loop_count = next_count

    return task_payload, should_checkpoint
