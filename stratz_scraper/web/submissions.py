"""Background submission helpers."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from typing import Iterable, Sequence

from ..database import db_connection, retryable_execute, retryable_executemany

BACKGROUND_EXECUTOR = ThreadPoolExecutor(max_workers=4)

__all__ = [
    "BACKGROUND_EXECUTOR",
    "process_discover_submission",
    "process_hero_submission",
    "submit_discover_submission",
    "submit_hero_submission",
]


def _unmark_hero_task(steam_account_id: int) -> None:
    try:
        with db_connection(write=True) as conn:
            cur = conn.cursor()
            retryable_execute(
                cur,
                """
                UPDATE players
                SET hero_done=0,
                    hero_refreshed_at=NULL,
                    assigned_to=NULL,
                    assigned_at=NULL
                WHERE steamAccountId=?
                """,
                (steam_account_id,),
            )
    except Exception:
        import traceback

        traceback.print_exc()


def _unmark_discover_task(steam_account_id: int) -> None:
    try:
        with db_connection(write=True) as conn:
            cur = conn.cursor()
            retryable_execute(
                cur,
                """
                UPDATE players
                SET discover_done=0,
                    assigned_to=NULL,
                    assigned_at=NULL
                WHERE steamAccountId=?
                """,
                (steam_account_id,),
            )
    except Exception:
        import traceback

        traceback.print_exc()


def process_hero_submission(
    steam_account_id: int,
    hero_stats_rows: Sequence[tuple[int, int, int, int]],
    best_rows: Sequence[tuple[int, str, int, int, int]],
    assigned_at_value: str | None,
) -> None:
    try:
        with db_connection(write=True) as conn:
            cur = conn.cursor()
            retryable_executemany(
                cur,
                """
                INSERT INTO hero_stats (steamAccountId, heroId, matches, wins)
                VALUES (?,?,?,?)
                ON CONFLICT(steamAccountId, heroId) DO UPDATE SET
                    matches = CASE
                        WHEN excluded.matches > hero_stats.matches
                        THEN excluded.matches
                        ELSE hero_stats.matches
                    END,
                    wins = CASE
                        WHEN excluded.matches > hero_stats.matches
                        THEN excluded.wins
                        ELSE hero_stats.wins
                    END
                """,
                hero_stats_rows or [],
            )
            if best_rows:
                retryable_executemany(
                    cur,
                    """
                    INSERT INTO best (hero_id, hero_name, player_id, matches, wins)
                    VALUES (?,?,?,?,?)
                    ON CONFLICT(hero_id) DO UPDATE SET
                        matches=excluded.matches,
                        wins=excluded.wins,
                        player_id=excluded.player_id
                    WHERE excluded.matches > best.matches
                    """,
                    best_rows,
                )
            retryable_execute(
                cur,
                """
                UPDATE players
                SET hero_done=1,
                    hero_refreshed_at=CURRENT_TIMESTAMP
                WHERE steamAccountId=?
                """,
                (steam_account_id,),
            )
    except Exception:
        import traceback

        print(
            f"[submit-background] failed to process hero stats for {steam_account_id}",
            flush=True,
        )
        traceback.print_exc()
        _unmark_hero_task(steam_account_id)


def process_discover_submission(
    steam_account_id: int,
    discovered_counts: Iterable[tuple[int, int]],
    next_depth_value: int,
    assigned_at_value: str | None,
) -> None:
    try:
        with db_connection(write=True) as conn:
            cur = conn.cursor()
            child_rows = [
                (new_id, next_depth_value, max(count, 0))
                for new_id, count in discovered_counts
                if new_id != steam_account_id and count > 0
            ]
            if child_rows:
                retryable_executemany(
                    cur,
                    """
                    INSERT INTO players (
                        steamAccountId,
                        depth,
                        hero_done,
                        discover_done,
                        seen_count
                    )
                    VALUES (?,?,0,0,?)
                    ON CONFLICT(steamAccountId) DO UPDATE SET
                        depth=CASE
                            WHEN players.depth IS NULL THEN excluded.depth
                            WHEN excluded.depth < players.depth THEN excluded.depth
                            ELSE players.depth
                        END,
                        seen_count=players.seen_count + excluded.seen_count
                    """,
                    child_rows,
                )
            retryable_execute(
                cur,
                """
                UPDATE players
                SET discover_done=1,
                    assigned_to=NULL,
                    assigned_at=NULL
                WHERE steamAccountId=?
                """,
                (steam_account_id,),
            )
    except Exception:
        import traceback

        print(
            f"[submit-background] failed to process discovery for {steam_account_id}",
            flush=True,
        )
        traceback.print_exc()
        _unmark_discover_task(steam_account_id)


def submit_hero_submission(
    steam_account_id: int,
    hero_stats_rows: Sequence[tuple[int, int, int, int]],
    best_rows: Sequence[tuple[int, str, int, int, int]],
    assigned_at_value: str | None,
) -> None:
    BACKGROUND_EXECUTOR.submit(
        process_hero_submission,
        steam_account_id,
        hero_stats_rows,
        best_rows,
        assigned_at_value,
    )


def submit_discover_submission(
    steam_account_id: int,
    discovered_counts: Iterable[tuple[int, int]],
    next_depth_value: int,
    assigned_at_value: str | None,
) -> None:
    BACKGROUND_EXECUTOR.submit(
        process_discover_submission,
        steam_account_id,
        tuple(discovered_counts),
        next_depth_value,
        assigned_at_value,
    )
