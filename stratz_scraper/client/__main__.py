"""Entry point for launching the PyQt client."""
from __future__ import annotations

import sys
from pathlib import Path


def _resolve_main():
    """Import and return the :func:`main` callable.

    When this module is executed as a script (``python stratz_scraper/client/__main__.py``)
    Python does not treat ``stratz_scraper`` as a package, so relative imports fail with
    ``ImportError: attempted relative import with no known parent package``. To keep the
    launch command flexible we detect that situation, ensure the repository root is on
    ``sys.path``, and then perform an absolute import instead.
    """

    if __package__:
        from .application import main as entrypoint
        return entrypoint

    repo_root = Path(__file__).resolve().parents[2]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

    from stratz_scraper.client.application import main as entrypoint
    return entrypoint


def _run() -> int:
    main = _resolve_main()
    return main()


if __name__ == "__main__":
    sys.exit(_run())
