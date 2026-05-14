"""Background submission helpers."""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from typing import Iterable, List

from psycopg import errors

from ..database import (
    close_cached_connections,
    db_connection,
    retryable_execute,
    retryable_executemany,
)

BACKGROUND_EXECUTOR = ThreadPoolExecutor(max_workers=1)
_DISCOVERY_SUBMISSION_LOCK_ID = int.from_bytes(b"discover", "big")

_RETRYABLE_ERRORS = (
    errors.DeadlockDetected,
    errors.SerializationFailure,
    errors.LockNotAvailable,
)

__all__ = [
    "BACKGROUND_EXECUTOR",
    "process_scrape_submission",
    "submit_scrape_submission",
]


def _submit_background(func, /, *args, **kwargs) -> None:
    def _runner() -> None:
        try:
            func(*args, **kwargs)
        finally:
            close_cached_connections()

    BACKGROUND_EXECUTOR.submit(_runner)


def _extract_hero_rows(
    steam_account_id: int, heroes_payload: Iterable[dict] | None
) -> List[tuple[int, int, int, int]]:
    rows: List[tuple[int, int, int, int]] = []
    seen: set[int] = set()
    if heroes_payload is None:
        return rows
    for hero in heroes_payload:
        try:
            hero_id = int(hero["heroId"])
            matches_value = hero.get("matches", hero.get("games"))
            if matches_value is None:
                continue
            matches = int(matches_value)
            wins = int(hero.get("wins", 0))
        except (KeyError, TypeError, ValueError):
            continue
        if hero_id in seen:
            continue
        seen.add(hero_id)
        rows.append((steam_account_id, hero_id, matches, wins))
    return rows


def _copy_discovered_rows(
    conn,
    steam_account_ids: list[int],
    next_depth: int,
) -> None:
    """Bulk-insert discovered children using COPY + a temp table upsert."""

    while True:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TEMP TABLE discovered_tmp (
                        steamAccountId BIGINT NOT NULL,
                        depth INTEGER NOT NULL
                    ) ON COMMIT DROP
                    """
                )
                with cur.copy(
                    "COPY discovered_tmp (steamAccountId, depth) FROM STDIN"
                ) as copy:
                    for sid in steam_account_ids:
                        copy.write_row((sid, next_depth))
                cur.execute(
                    """
                    INSERT INTO players (steamAccountId, depth)
                    SELECT steamAccountId, depth FROM discovered_tmp
                    ON CONFLICT (steamAccountId) DO UPDATE
                    SET depth = excluded.depth
                    WHERE excluded.depth < players.depth
                    """
                )
            conn.commit()
            return
        except _RETRYABLE_ERRORS:
            try:
                conn.rollback()
            except Exception:
                pass
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT pg_advisory_xact_lock(%s)",
                    (_DISCOVERY_SUBMISSION_LOCK_ID,),
                )
            time.sleep(0.5)


def _unmark_scrape_task(steam_account_id: int) -> None:
    """Clear a failed scrape's lease so the player is eligible for re-scrape."""
    try:
        with db_connection(write=True) as conn:
            cur = conn.cursor()
            retryable_execute(
                cur,
                """
                UPDATE players
                SET assigned_to=NULL, assigned_at=NULL, scraped_at=NULL
                WHERE steamAccountId=%s
                """,
                (steam_account_id,),
            )
    except Exception:
        import traceback
        traceback.print_exc()


def process_scrape_submission(
    steam_account_id: int,
    heroes_payload: Iterable[dict] | None,
    discovered_ids: list[int],
    latest_match_id: int | None,
    next_depth: int,
) -> None:
    hero_rows = _extract_hero_rows(steam_account_id, heroes_payload)
    try:
        with db_connection(write=True) as conn:
            cur = conn.cursor()
            if hero_rows:
                retryable_executemany(
                    cur,
                    """
                    INSERT INTO hero_stats (steamAccountId, heroId, matches, wins)
                    VALUES (%s,%s,%s,%s)
                    ON CONFLICT(steamAccountId, heroId) DO UPDATE SET
                        matches = excluded.matches,
                        wins = excluded.wins
                    """,
                    hero_rows,
                )
            if discovered_ids:
                _copy_discovered_rows(conn, discovered_ids, next_depth)
            retryable_execute(
                cur,
                """
                UPDATE players
                SET scraped_at = NOW(),
                    latest_match_id = COALESCE(%s, latest_match_id)
                WHERE steamAccountId=%s
                """,
                (latest_match_id, steam_account_id),
            )
    except Exception:
        import traceback
        print(
            f"[submit-background] scrape failed for {steam_account_id}",
            flush=True,
        )
        traceback.print_exc()
        _unmark_scrape_task(steam_account_id)


def submit_scrape_submission(
    steam_account_id: int,
    heroes_payload: Iterable[dict] | None,
    discovered_ids: list[int],
    latest_match_id: int | None,
    next_depth: int,
) -> None:
    _submit_background(
        process_scrape_submission,
        steam_account_id,
        heroes_payload,
        discovered_ids,
        latest_match_id,
        next_depth,
    )
