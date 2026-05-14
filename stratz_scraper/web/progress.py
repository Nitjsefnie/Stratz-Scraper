"""Progress reporting helpers."""

from __future__ import annotations

import logging
import threading
from datetime import datetime, timedelta, timezone
from typing import Mapping

from ..database import db_connection, retryable_execute

__all__ = [
    "ensure_progress_snapshotter",
    "fetch_progress",
    "list_progress_snapshots",
    "record_progress_snapshot",
]


_LOGGER = logging.getLogger(__name__)

_SNAPSHOT_THREAD: threading.Thread | None = None
_SNAPSHOT_STOP_EVENT: threading.Event | None = None
_SNAPSHOT_LOCK = threading.Lock()
_SNAPSHOT_INTERVAL = timedelta(minutes=5)


def fetch_progress() -> dict:
    with db_connection() as conn:
        row = conn.execute(
            """
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE scraped_at IS NOT NULL) AS scraped
            FROM players
            """
        ).fetchone()
        if row is None:
            return {"players_total": 0, "scraped": 0}
        total = row["total"] or 0
        scraped = row["scraped"] or 0
    return {
        "players_total": total,
        "scraped": scraped,
    }


def _normalize_captured_at(captured_at: datetime | None) -> datetime:
    if captured_at is None:
        captured_at = datetime.now(timezone.utc)
    elif captured_at.tzinfo is None:
        captured_at = captured_at.replace(tzinfo=timezone.utc)
    interval_seconds = int(_SNAPSHOT_INTERVAL.total_seconds())
    normalized_timestamp = (int(captured_at.timestamp()) // interval_seconds) * interval_seconds
    return datetime.fromtimestamp(normalized_timestamp, tz=timezone.utc)


def record_progress_snapshot(
    progress: Mapping[str, int] | None = None,
    *,
    captured_at: datetime | None = None,
) -> dict:
    """Persist a snapshot of current progress metrics.

    Parameters
    ----------
    progress:
        Optional mapping containing ``players_total`` and ``scraped`` counters.
        When omitted the values are pulled from the live ``/progress`` view.
    captured_at:
        Optional timestamp indicating when the snapshot was captured. The value
        is normalized to the start of the five-minute interval in UTC to keep a
        single row per interval.
    """

    captured_at = _normalize_captured_at(captured_at)
    if progress is None:
        progress = fetch_progress()
    else:
        progress = dict(progress)
    required_keys = ("players_total", "scraped")
    normalized: dict[str, int] = {}
    for key in required_keys:
        value = int(progress.get(key, 0))
        normalized[key] = value

    with db_connection(write=True) as conn:
        cur = conn.cursor()
        retryable_execute(
            cur,
            """
            INSERT INTO progress_snapshots (
                captured_at,
                players_total,
                scraped
            )
            VALUES (%s, %s, %s)
            ON CONFLICT (captured_at) DO UPDATE
            SET
                players_total=EXCLUDED.players_total,
                scraped=EXCLUDED.scraped
            """,
            (
                captured_at,
                normalized["players_total"],
                normalized["scraped"],
            ),
        )

    snapshot = {
        "captured_at": captured_at,
        **normalized,
    }
    return snapshot


def _seconds_until_next_interval(reference: datetime) -> float:
    normalized = _normalize_captured_at(reference)
    next_tick = normalized + _SNAPSHOT_INTERVAL
    wait_seconds = (next_tick - reference).total_seconds()
    if wait_seconds <= 0:
        return 1.0
    return wait_seconds


def _progress_snapshot_worker(stop_event: threading.Event) -> None:
    while not stop_event.is_set():
        try:
            record_progress_snapshot()
        except Exception:  # pragma: no cover - best effort logging
            _LOGGER.exception("Progress snapshot worker failed")
        now = datetime.now(timezone.utc)
        if stop_event.wait(_seconds_until_next_interval(now)):
            break


def ensure_progress_snapshotter() -> None:
    """Start the background worker that records five-minute progress snapshots."""

    global _SNAPSHOT_THREAD, _SNAPSHOT_STOP_EVENT
    with _SNAPSHOT_LOCK:
        if _SNAPSHOT_THREAD and _SNAPSHOT_THREAD.is_alive():
            return
        if _SNAPSHOT_STOP_EVENT is not None:
            _SNAPSHOT_STOP_EVENT.set()
        stop_event = threading.Event()
        thread = threading.Thread(
            target=_progress_snapshot_worker,
            args=(stop_event,),
            name="progress-snapshotter",
            daemon=True,
        )
        thread.start()
        _SNAPSHOT_THREAD = thread
        _SNAPSHOT_STOP_EVENT = stop_event


def list_progress_snapshots(
    *, start: datetime | None = None, end: datetime | None = None
) -> list[dict]:
    """Return stored progress snapshots filtered by the provided time range."""

    clauses: list[str] = []
    params: list[datetime] = []
    if start is not None:
        clauses.append("captured_at >= %s")
        params.append(start)
    if end is not None:
        clauses.append("captured_at <= %s")
        params.append(end)

    where_sql = ""
    if clauses:
        where_sql = " WHERE " + " AND ".join(clauses)

    sql = (
        """
        SELECT captured_at, players_total, scraped
        FROM progress_snapshots
        """
        + where_sql
        + " ORDER BY captured_at ASC"
    )

    with db_connection() as conn:
        rows = conn.execute(sql, tuple(params)).fetchall()

    return [
        {
            "captured_at": row["captured_at"],
            "players_total": row["players_total"],
            "scraped": row["scraped"],
        }
        for row in rows
    ]
