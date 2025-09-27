from __future__ import annotations

from typing import Any


def create_app(*args: Any, **kwargs: Any):
    """Return a configured Flask application instance.

    The import is deferred so environments that only need the desktop client do not have
    to install the Flask backend's dependencies. This keeps ``stratz_scraper`` importable
    without Flask while preserving the public API used by ``app.py``.
    """

    from .web.app import create_app as _create_app

    return _create_app(*args, **kwargs)


__all__ = ["create_app"]
