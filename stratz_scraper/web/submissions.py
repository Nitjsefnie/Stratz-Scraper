"""Background submission helpers."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from typing import Iterable, List

from ..database import db_connection, retryable_execute, retryable_executemany
from ..heroes import HEROES

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


def _extract_hero_rows(
    steam_account_id: int, heroes_payload: Iterable[dict] | None
) -> tuple[
    List[tuple[int, int, int, int]],
    List[tuple[int, str, int, int, int]],
]:
    hero_stats_rows: List[tuple[int, int, int, int]] = []
    best_rows: List[tuple[int, str, int, int, int]] = []
    if heroes_payload is None:
        return hero_stats_rows, best_rows
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
        hero_name = HEROES.get(hero_id)
        if hero_name:
            best_rows.append((hero_id, hero_name, steam_account_id, matches, wins))
    return hero_stats_rows, best_rows


def _extract_discovered_counts(values: Iterable[object] | None) -> List[tuple[int, int]]:
    aggregated: dict[int, int] = {}
    order: List[int] = []
    if values is None:
        return []
    for value in values:
        candidate_id = None
        count_value: int | None = 1
        if isinstance(value, dict):
            candidate_id = value.get("steamAccountId")
            if candidate_id is None:
                candidate_id = value.get("id")
            count_raw = value.get("count")
            if count_raw is None:
                count_raw = value.get("seenCount")
            if count_raw is not None:
                try:
                    count_value = int(count_raw)
                except (TypeError, ValueError):
                    count_value = 0
        else:
            candidate_id = value
        try:
            candidate_id = int(candidate_id)
        except (TypeError, ValueError):
            continue
        if candidate_id <= 0:
            continue
        if count_value is None:
            count_value = 0
        if not isinstance(count_value, int):
            try:
                count_value = int(count_value)
            except (TypeError, ValueError):
                count_value = 0
        if count_value <= 0:
            continue
        if candidate_id not in aggregated:
            aggregated[candidate_id] = count_value
            order.append(candidate_id)
        else:
            aggregated[candidate_id] += count_value
    return [(pid, aggregated[pid]) for pid in order]


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
    hero_stats_rows, best_rows = _extract_hero_rows(steam_account_id, heroes_payload)
    try:
        with db_connection(write=True) as conn:
            cur = conn.cursor()
            if hero_stats_rows:
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
                    hero_stats_rows,
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
                    assigned_to=NULL,
                    assigned_at=NULL,
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
    discovered_payload: Iterable[object] | None,
    provided_next_depth: int | None,
    provided_depth: int | None,
    assignment_depth: int | None,
) -> None:
    parsed_next_depth = _coerce_optional_int(provided_next_depth)
    parsed_depth = _coerce_optional_int(provided_depth)
    parsed_assignment_depth = _coerce_optional_int(assignment_depth)
    discovered_counts = _extract_discovered_counts(discovered_payload)
    next_depth_value = _resolve_next_depth(
        parsed_next_depth,
        parsed_depth,
        parsed_assignment_depth,
    )
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
    BACKGROUND_EXECUTOR.submit(
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
    BACKGROUND_EXECUTOR.submit(
        process_discover_submission,
        steam_account_id,
        discovered_payload,
        provided_next_depth,
        provided_depth,
        assignment_depth,
    )
