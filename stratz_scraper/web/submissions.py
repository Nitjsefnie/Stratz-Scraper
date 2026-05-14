"""Background submission helpers."""

from __future__ import annotations

from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from typing import Iterable, Iterator, List

from ..database import (
    close_cached_connections,
    db_connection,
    retryable_execute,
    retryable_executemany,
    row_value,
)

BACKGROUND_EXECUTOR = ThreadPoolExecutor(max_workers=1)
_DISCOVERY_SUBMISSION_LOCK_ID = int.from_bytes(b"discover", "big")
_DISCOVERY_BATCH_SIZE = 50

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
) -> Iterator[int]:
    if values is None:
        return
    for value in _iter_consuming_values(values):
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


def _iter_discovered_child_rows(
    discovered_payload: Iterable[object] | None,
    *,
    parent_id: int,
    next_depth: int,
    batch_size: int,
) -> Iterator[List[tuple[int, int]]]:
    effective_batch_size = max(1, batch_size)
    pending: OrderedDict[int, None] = OrderedDict()

    def _drain_pending(limit: int | None) -> List[tuple[int, int]]:
        batch: List[tuple[int, int]] = []
        while pending and (limit is None or len(batch) < limit):
            candidate, _ = pending.popitem(last=False)
            batch.append((candidate, next_depth))
        return batch

    for candidate_id in _iter_discovered_candidate_ids(discovered_payload):
        if candidate_id == parent_id:
            continue
        if candidate_id in pending:
            continue
        pending[candidate_id] = None
        if len(pending) >= effective_batch_size:
            batch = _drain_pending(effective_batch_size)
            if batch:
                yield batch
    if pending:
        batch = _drain_pending(None)
        if batch:
            yield batch


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
    try:
        with db_connection(write=True) as conn:
            for child_rows in _iter_discovered_child_rows(
                discovered_payload,
                parent_id=steam_account_id,
                next_depth=next_depth_value,
                batch_size=_DISCOVERY_BATCH_SIZE,
            ):
                retryable_executemany(
                    conn,
                    """
                    INSERT INTO players (
                        steamAccountId,
                        depth,
                        hero_done,
                        discover_done
                    )
                    VALUES (%s, %s, FALSE, FALSE)
                    ON CONFLICT (steamAccountId) DO UPDATE
                    SET
                        depth = excluded.depth, highest_match_id = NULL, discover_done = FALSE
                    WHERE excluded.depth < players.depth
                    """,
                    child_rows,
                    reacquire_advisory_lock=_DISCOVERY_SUBMISSION_LOCK_ID,
                )
                conn.commit()
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
                retryable_execute(
                    cur,
                    """
                    UPDATE meta
                    SET value = '-1'
                    WHERE key = 'hero_assignment_cursor';
                    """
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
