"""Background thread that periodically rebuilds the hero_top100 cache."""

from __future__ import annotations

import logging
import threading
from datetime import timedelta

from ..database import refresh_leaderboard_views

LEADERBOARD_REFRESH_INTERVAL = timedelta(minutes=5)

_LOGGER = logging.getLogger(__name__)
_refresher_thread: threading.Thread | None = None
_refresher_stop_event: threading.Event | None = None
_refresher_lock = threading.Lock()

__all__ = [
    "LEADERBOARD_REFRESH_INTERVAL",
    "ensure_leaderboard_refresher",
]


def _refresher_worker(stop_event: threading.Event) -> None:
    interval_seconds = max(int(LEADERBOARD_REFRESH_INTERVAL.total_seconds()), 1)
    while not stop_event.is_set():
        try:
            refresh_leaderboard_views()
        except Exception:  # pragma: no cover - best effort logging
            _LOGGER.exception("Leaderboard refresher failed")
        stop_event.wait(interval_seconds)


def ensure_leaderboard_refresher() -> None:
    """Start the background worker that rebuilds hero_top100 periodically."""

    global _refresher_thread, _refresher_stop_event
    with _refresher_lock:
        if _refresher_thread and _refresher_thread.is_alive():
            return
        stop_event = threading.Event()
        thread = threading.Thread(
            target=_refresher_worker,
            args=(stop_event,),
            name="leaderboard-refresher",
            daemon=True,
        )
        thread.start()
        _refresher_thread = thread
        _refresher_stop_event = stop_event
