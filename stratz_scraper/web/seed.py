"""Utility helpers for seeding players."""

from __future__ import annotations

from ..database import db_connection, retryable_execute

__all__ = ["seed_players"]


def seed_players(start: int, end: int) -> None:
    with db_connection(write=True) as conn:
        cur = conn.cursor()
        for pid in range(start, end + 1):
            retryable_execute(
                cur,
                """
                INSERT INTO players (
                    steamAccountId,
                    depth,
                    hero_done,
                    discover_done
                )
                VALUES (%s,%s,FALSE,FALSE)
                ON CONFLICT (steamAccountId) DO NOTHING
                """,
                (pid, 0),
            )
