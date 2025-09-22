from __future__ import annotations

from os import O_CREAT, O_EXCL, O_WRONLY
from os import close as osclose
from os import getpid
from os import open as osopen
from os import remove
from os import write as oswrite
from pathlib import Path
from time import sleep, time


class FileLock:
    def __init__(self, path: Path | str, interval: float = 0.05, timeout: float | None = None) -> None:
        self.path = Path(path)
        self.interval = interval
        self.timeout = timeout
        self._owned = False
        self._pid_bytes = str(getpid()).encode("ascii")

    def __enter__(self) -> "FileLock":
        start = time()
        while True:
            try:
                fd = osopen(self.path.as_posix(), O_CREAT | O_EXCL | O_WRONLY)
                try:
                    oswrite(fd, self._pid_bytes)
                finally:
                    osclose(fd)
                self._owned = True
                return self
            except (FileExistsError, PermissionError) as exc:
                if self.timeout is not None and time() - start >= self.timeout:
                    raise TimeoutError from exc
                sleep(self.interval)

    def __exit__(self, *_: object) -> None:
        try:
            if self._owned:
                try:
                    with self.path.open("rb") as handle:
                        if handle.read().strip() != self._pid_bytes:
                            return
                except FileNotFoundError:
                    return
                remove(self.path)
        finally:
            self._owned = False

    @staticmethod
    def cleanup_locks(lock_dir: Path) -> None:
        try:
            for lock_path in lock_dir.glob("*.lock"):
                try:
                    with lock_path.open("rb") as handle:
                        if handle.read().strip() == str(getpid()).encode("ascii"):
                            remove(lock_path)
                except Exception:
                    continue
        except FileNotFoundError:
            return
