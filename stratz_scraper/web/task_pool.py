"""Background task pool for pre-generating assignments."""

from __future__ import annotations

import queue
import threading
import time
from typing import Optional

from .assignment import assign_next_task

__all__ = ["TaskPool"]


class TaskPool:
    """Prefetch task assignments in a background thread."""

    def __init__(
        self,
        *,
        max_size: int = 500,
        refill_ratio: float = 0.2,
        retry_interval: float = 1.0,
    ) -> None:
        if max_size <= 0:
            raise ValueError("max_size must be positive")
        if not 0 < refill_ratio <= 1:
            raise ValueError("refill_ratio must be between 0 and 1")
        if retry_interval <= 0:
            raise ValueError("retry_interval must be positive")

        self._queue: "queue.Queue[Optional[dict]]" = queue.Queue(maxsize=max_size)
        self._max_size = max_size
        self._refill_threshold = max(1, int(max_size * refill_ratio))
        self._retry_interval = retry_interval
        self._stop_event = threading.Event()
        self._refill_event = threading.Event()
        self._thread = threading.Thread(
            target=self._fill_loop,
            name="task-pool-refill",
            daemon=True,
        )
        self._thread.start()
        # Trigger the initial fill on startup.
        self._refill_event.set()

    def stop(self) -> None:
        """Stop the background refill thread."""

        self._stop_event.set()
        self._refill_event.set()
        if self._thread.is_alive():
            self._thread.join(timeout=1)

    def get(self) -> Optional[dict]:
        """Return the next pre-generated task, refilling in the background."""

        try:
            task = self._queue.get_nowait()
        except queue.Empty:
            task = assign_next_task()
        else:
            if self._queue.qsize() <= self._refill_threshold:
                self._refill_event.set()
        if task is None:
            # Ensure we keep trying to find new tasks when available.
            self._refill_event.set()
        return task

    def _fill_loop(self) -> None:
        while not self._stop_event.is_set():
            self._refill_event.wait()
            if self._stop_event.is_set():
                break
            self._refill_event.clear()

            while not self._stop_event.is_set() and not self._queue.full():
                task = assign_next_task()
                if task is None:
                    break
                try:
                    self._queue.put(task, timeout=0.1)
                except queue.Full:
                    break

            if self._queue.full():
                continue

            time.sleep(self._retry_interval)
            self._refill_event.set()
