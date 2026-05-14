"""Task reset helpers."""

from __future__ import annotations

from typing import Optional

from ..database import db_connection, retryable_execute

__all__ = ["reset_player_task"]


def reset_player_task(steam_account_id: int, _task_type: Optional[str] = None) -> bool:
    """Release a scrape lease for ``steam_account_id``.

    The ``_task_type`` parameter is accepted for API compatibility with the
    existing /task/reset request body but is ignored — every scrape task
    resets the same way.
    """

    with db_connection(write=True) as conn:
        cur = conn.cursor()
        update_cursor = retryable_execute(
            cur,
            """
            UPDATE players
            SET assigned_to=NULL, assigned_at=NULL
            WHERE steamAccountId=%s
            """,
            (steam_account_id,),
        )
        return (update_cursor.rowcount or 0) > 0
