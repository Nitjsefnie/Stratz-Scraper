"""Leaderboard helpers."""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple

from ..database import db_connection
from ..heroes import HERO_SLUGS, hero_slug

__all__ = ["fetch_best_payload", "fetch_hero_leaderboard", "fetch_overall_leaderboard"]


def fetch_hero_leaderboard(slug: str) -> Optional[Tuple[str, str, List[dict]]]:
    normalized = slug.strip().replace(" ", "_").lower()
    hero_entry = HERO_SLUGS.get(normalized)
    if not hero_entry:
        return None
    hero_id, hero_name = hero_entry
    with db_connection() as conn:
        rows = conn.execute(
            """
            SELECT steamAccountId, matches, wins
            FROM hero_stats
            WHERE heroId=?
            ORDER BY matches DESC, wins DESC, steamAccountId ASC
            LIMIT 100
            """,
            (hero_id,),
        ).fetchall()
    players = [
        {
            "steamAccountId": row["steamAccountId"],
            "matches": row["matches"],
            "wins": row["wins"],
        }
        for row in rows
    ]
    return hero_name, normalized, players


def fetch_overall_leaderboard() -> List[Dict[str, int]]:
    with db_connection() as conn:
        rows = conn.execute(
            """
            SELECT steamAccountId, SUM(matches) AS matches, SUM(wins) AS wins
            FROM hero_stats
            GROUP BY steamAccountId
            ORDER BY matches DESC, wins DESC, steamAccountId ASC
            LIMIT 100
            """
        ).fetchall()
    players: List[Dict[str, int]] = []
    for row in rows:
        players.append(
            {
                "steamAccountId": row["steamAccountId"],
                "matches": row["matches"] or 0,
                "wins": row["wins"] or 0,
            }
        )
    return players


def fetch_best_payload() -> List[Dict]:
    with db_connection() as conn:
        rows = conn.execute("SELECT * FROM best ORDER BY matches DESC").fetchall()
    payload: List[Dict] = []
    for row in rows:
        row_dict = dict(row)
        name = row_dict.get("hero_name")
        row_dict["hero_slug"] = hero_slug(name) if isinstance(name, str) else None
        payload.append(row_dict)
    return payload
