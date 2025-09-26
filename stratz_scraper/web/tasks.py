"""Task management helpers."""

from __future__ import annotations

from typing import Optional

from ..database import db_connection, retryable_execute

__all__ = ["reset_player_task"]


def _reset_hero_task(cur, steam_account_id: int) -> int:
    has_existing_stats = cur.execute(
        "SELECT 1 FROM hero_stats WHERE steamAccountId=? LIMIT 1",
        (steam_account_id,),
    ).fetchone()
    hero_done_value = 1 if has_existing_stats else 0
    update_cursor = retryable_execute(
        cur,
        """
        UPDATE players
        SET hero_done=?,
            hero_refreshed_at=CASE WHEN ? THEN hero_refreshed_at ELSE NULL END,
            assigned_to=NULL,
            assigned_at=NULL
        WHERE steamAccountId=?
        """,
        (hero_done_value, hero_done_value, steam_account_id),
    )
    return update_cursor.rowcount if update_cursor.rowcount is not None else 0


def _reset_discover_task(cur, steam_account_id: int) -> int:
    update_cursor = retryable_execute(
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
    return update_cursor.rowcount if update_cursor.rowcount is not None else 0


def _reset_generic_task(cur, steam_account_id: int) -> int:
    update_cursor = retryable_execute(
        cur,
        """
        UPDATE players
        SET assigned_to=NULL,
            assigned_at=NULL
        WHERE steamAccountId=?
        """,
        (steam_account_id,),
    )
    return update_cursor.rowcount if update_cursor.rowcount is not None else 0


def reset_player_task(steam_account_id: int, task_type: Optional[str]) -> bool:
    """Reset the task assignment for ``steam_account_id``."""

    with db_connection(write=True) as conn:
        cur = conn.cursor()
        if task_type == "fetch_hero_stats":
            updated = _reset_hero_task(cur, steam_account_id)
        elif task_type == "discover_matches":
            updated = _reset_discover_task(cur, steam_account_id)
        else:
            updated = _reset_generic_task(cur, steam_account_id)
    return updated > 0
