"""Background submission helpers."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from typing import Iterable, List

from psycopg import errors

from ..database import (
    close_cached_connections,
    db_connection,
    retryable_execute,
    retryable_executemany,
    row_value,
)

BACKGROUND_EXECUTOR = ThreadPoolExecutor(max_workers=1)
_DISCOVERY_SUBMISSION_LOCK_ID = int.from_bytes(b"discover", "big")

_RETRYABLE_ERRORS = (
    errors.DeadlockDetected,
    errors.SerializationFailure,
    errors.LockNotAvailable,
)

import time

__all__ = [
    "BACKGROUND_EXECUTOR",
    "process_discover_submission",
    "process_hero_submission",
    "submit_discover_submission",
    "submit_hero_submission",
]


def _submit_background(func, /, *args, **kwargs) -> None:
    def _runner() -> None:
        try:
            func(*args, **kwargs)
        finally:
            close_cached_connections()

    BACKGROUND_EXECUTOR.submit(_runner)


def _unmark_hero_task(steam_account_id: int) -> None:
    try:
        with db_connection(write=True) as conn:
            cur = conn.cursor()
            retryable_execute(
                cur,
                """
                UPDATE players
                SET hero_done=FALSE,
                    hero_refreshed_at=NULL,
                    assigned_to=NULL,
                    assigned_at=NULL
                WHERE steamAccountId=%s
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
                SET discover_done=FALSE,
                    full_write_done=FALSE,
                    assigned_to=NULL,
                    assigned_at=NULL
                WHERE steamAccountId=%s
                """,
                (steam_account_id,),
            )
    except Exception:
        import traceback

        traceback.print_exc()


def _extract_hero_rows(
    steam_account_id: int, heroes_payload: Iterable[dict] | None
) -> tuple[List[tuple[int, int, int, int]], List[int]]:
    hero_stats_rows: List[tuple[int, int, int, int]] = []
    hero_ids: List[int] = []
    seen: set[int] = set()
    if heroes_payload is None:
        return hero_stats_rows, hero_ids
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
        hero_stats_rows.append((steam_account_id, hero_id, matches, wins))
        if hero_id not in seen:
            hero_ids.append(hero_id)
            seen.add(hero_id)
    return hero_stats_rows, hero_ids


def _iter_consuming_values(values: Iterable[object]) -> Iterator[object]:
    if isinstance(values, list):
        while values:
            yield values.pop()
        return
    for value in values:
        yield value


def _iter_discovered_candidate_ids(
    values: Iterable[object] | None,
) -> Iterable[int]:
    if values is None:
        return
    for value in values:
        candidate_id: object | None
        if isinstance(value, dict):
            candidate_id = value.get("steamAccountId")
            if candidate_id is None:
                candidate_id = value.get("id")
        else:
            candidate_id = value
        try:
            normalized_id = int(candidate_id)
        except (TypeError, ValueError):
            continue
        if normalized_id <= 0:
            continue
        yield normalized_id


def _resolve_next_depth(
    provided_next_depth: int | None,
    provided_depth: int | None,
    assignment_depth: int | None,
) -> int:
    if provided_next_depth is not None:
        return provided_next_depth
    parent_depth_value = provided_depth
    if parent_depth_value is None:
        if assignment_depth is not None:
            parent_depth_value = assignment_depth
        else:
            parent_depth_value = 0
    return parent_depth_value + 1


def _coerce_optional_int(value: object | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def process_hero_submission(
    steam_account_id: int,
    heroes_payload: Iterable[dict] | None,
) -> None:
    hero_stats_rows, _hero_ids = _extract_hero_rows(steam_account_id, heroes_payload)
    if not hero_stats_rows:
        return
    try:
        with db_connection(write=True) as conn:
            cur = conn.cursor()
            retryable_executemany(
                cur,
                """
                INSERT INTO hero_stats (steamAccountId, heroId, matches, wins)
                VALUES (%s,%s,%s,%s)
                ON CONFLICT(steamAccountId, heroId) DO UPDATE SET
                    matches = excluded.matches,
                    wins = excluded.wins
                """,
                hero_stats_rows,
            )
    except Exception:
        import traceback
        print(
            f"[submit-background] failed to process hero stats for {steam_account_id}",
            flush=True,
        )
        traceback.print_exc()
        _unmark_hero_task(steam_account_id)


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
                    INSERT INTO players (
                        steamAccountId, depth, hero_done, discover_done
                    )
                    SELECT steamAccountId, depth, FALSE, FALSE
                    FROM discovered_tmp
                    ON CONFLICT (steamAccountId) DO UPDATE
                    SET depth = excluded.depth,
                        highest_match_id = NULL,
                        discover_done = FALSE
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
            # Reacquire advisory lock to match prior retry semantic.
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT pg_advisory_xact_lock(%s)",
                    (_DISCOVERY_SUBMISSION_LOCK_ID,),
                )
            time.sleep(0.5)


def process_discover_submission(
    steam_account_id: int,
    discovered_payload: Iterable[object] | None,
    provided_next_depth: int | None,
    provided_depth: int | None,
    assignment_depth: int | None,
) -> None:
    parsed_next_depth = _coerce_optional_int(provided_next_depth)
    parsed_depth = _coerce_optional_int(provided_depth)
    parsed_assignment_depth = _coerce_optional_int(assignment_depth)
    next_depth_value = _resolve_next_depth(
        parsed_next_depth,
        parsed_depth,
        parsed_assignment_depth,
    )

    deduped: list[int] = []
    seen: set[int] = set()
    for candidate_id in _iter_discovered_candidate_ids(discovered_payload):
        if candidate_id == steam_account_id or candidate_id in seen:
            continue
        seen.add(candidate_id)
        deduped.append(candidate_id)

    try:
        with db_connection(write=True) as conn:
            if deduped:
                _copy_discovered_rows(conn, deduped, next_depth_value)
            with conn.cursor() as cur:
                retryable_execute(
                    cur,
                    """
                    UPDATE players
                    SET full_write_done=TRUE
                    WHERE steamAccountId=%s
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
    heroes_payload: Iterable[dict] | None,
) -> None:
    _submit_background(
        process_hero_submission,
        steam_account_id,
        heroes_payload,
    )


def submit_discover_submission(
    steam_account_id: int,
    discovered_payload: Iterable[object] | None,
    provided_next_depth: int | None,
    provided_depth: int | None,
    assignment_depth: int | None,
) -> None:
    _submit_background(
        process_discover_submission,
        steam_account_id,
        discovered_payload,
        provided_next_depth,
        provided_depth,
        assignment_depth,
    )
