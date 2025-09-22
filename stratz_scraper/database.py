from __future__ import annotations

from contextlib import contextmanager, nullcontext
from pathlib import Path
import sqlite3

from .locking import FileLock

DB_PATH = Path("dota.db")
LOCK_PATH = DB_PATH.with_suffix(".lock")
INITIAL_PLAYER_ID = 293053907


def ensure_schema_exists() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    if DB_PATH.exists():
        return
    with FileLock(LOCK_PATH):
        if DB_PATH.exists():
            return
        ensure_schema(lock_acquired=True)


def connect() -> sqlite3.Connection:
    ensure_schema_exists()
    connection = sqlite3.connect(DB_PATH, timeout=30)
    connection.execute("PRAGMA busy_timeout = 5000")
    connection.row_factory = sqlite3.Row
    return connection


@contextmanager
def db_connection(write: bool = False) -> sqlite3.Connection:
    ensure_schema_exists()
    lock_ctx = FileLock(LOCK_PATH) if write else nullcontext()
    with lock_ctx:
        conn = connect()
        try:
            yield conn
            if write:
                conn.commit()
        except Exception:
            if write:
                conn.rollback()
            raise
        finally:
            conn.close()


def ensure_schema(*, lock_acquired: bool = False) -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    lock_ctx = nullcontext() if lock_acquired else FileLock(LOCK_PATH)
    with lock_ctx:
        with sqlite3.connect(DB_PATH, timeout=30) as conn:
            conn.execute("PRAGMA busy_timeout = 5000")
            conn.executescript(
                """
                DROP TABLE IF EXISTS hero_stats;
                DROP TABLE IF EXISTS players;
                DROP TABLE IF EXISTS meta;
                DROP TABLE IF EXISTS best;

                CREATE TABLE players (
                    steamAccountId INTEGER PRIMARY KEY,
                    depth INTEGER,
                    assigned_to TEXT,
                    assigned_at DATETIME,
                    hero_refreshed_at DATETIME,
                    hero_done INTEGER DEFAULT 0,
                    discover_done INTEGER DEFAULT 0
                );

                CREATE TABLE hero_stats (
                    steamAccountId INTEGER,
                    heroId INTEGER,
                    matches INTEGER,
                    wins INTEGER,
                    PRIMARY KEY (steamAccountId, heroId)
                );

                CREATE TABLE best (
                    hero_id INTEGER PRIMARY KEY,
                    hero_name TEXT,
                    player_id INTEGER,
                    matches INTEGER,
                    wins INTEGER
                );

                CREATE TABLE meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                """
            )
            conn.execute(
                """
                INSERT OR IGNORE INTO players (steamAccountId, depth)
                VALUES (?, 0)
                """,
                (INITIAL_PLAYER_ID,),
            )


def release_incomplete_assignments(max_age_minutes: int = 5, existing: sqlite3.Connection | None = None) -> int:
    age_modifier = f"-{int(max_age_minutes)} minutes"
    if existing is None:
        with db_connection(write=True) as conn:
            cursor = conn.execute(
                """
                UPDATE players
                SET assigned_to=NULL,
                    assigned_at=NULL
                WHERE assigned_to IS NOT NULL
                  AND (
                      assigned_at IS NULL
                      OR assigned_at <= datetime('now', ?)
                  )
                """,
                (age_modifier,),
            )
            return cursor.rowcount if cursor.rowcount is not None else 0
    cursor = existing.execute(
        """
        UPDATE players
        SET assigned_to=NULL,
            assigned_at=NULL
        WHERE assigned_to IS NOT NULL
          AND (
              assigned_at IS NULL
              OR assigned_at <= datetime('now', ?)
          )
        """,
        (age_modifier,),
    )
    return cursor.rowcount if cursor.rowcount is not None else 0


__all__ = [
    "connect",
    "db_connection",
    "ensure_schema_exists",
    "ensure_schema",
    "release_incomplete_assignments",
    "DB_PATH",
    "LOCK_PATH",
    "INITIAL_PLAYER_ID",
]
