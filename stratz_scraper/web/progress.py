"""Progress reporting helpers."""

from __future__ import annotations

from ..database import db_connection

__all__ = ["fetch_progress"]


def fetch_progress() -> dict:
    with db_connection() as conn:
        total = conn.execute("SELECT COUNT(*) AS c FROM players").fetchone()["c"]
        hero_done = (
            conn.execute("SELECT COUNT(*) AS c FROM players WHERE hero_done=TRUE").fetchone()["c"]
        )
        discover_done = (
            conn.execute(
                "SELECT COUNT(*) AS c FROM players WHERE discover_done=TRUE"
            ).fetchone()["c"]
        )
    return {
        "players_total": total,
        "hero_done": hero_done,
        "discover_done": discover_done,
    }
