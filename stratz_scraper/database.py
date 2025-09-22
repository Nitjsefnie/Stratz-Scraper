from __future__ import annotations

from contextlib import contextmanager, nullcontext
from pathlib import Path
import sqlite3

from .locking import FileLock

DB_PATH = Path("dota.db")
LOCK_PATH = DB_PATH.with_suffix(".lock")


def connect() -> sqlite3.Connection:
    if not DB_PATH.exists():
        ensure_schema()
    connection = sqlite3.connect(DB_PATH, timeout=30)
    connection.execute("PRAGMA busy_timeout = 5000")
    connection.row_factory = sqlite3.Row
    return connection


@contextmanager
def db_connection(write: bool = False) -> sqlite3.Connection:
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


def ensure_schema() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with FileLock(LOCK_PATH):
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


def ensure_hero_refresh_column() -> None:
    with db_connection(write=True) as conn:
        columns = conn.execute("PRAGMA table_info(players)").fetchall()
        if not any(column[1] == "hero_refreshed_at" for column in columns):
            conn.execute("ALTER TABLE players ADD COLUMN hero_refreshed_at DATETIME")


def reset_hero_refresh_once() -> None:
    with db_connection(write=True) as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        conn.execute("BEGIN")
        reset_marker = conn.execute(
            "SELECT value FROM meta WHERE key=?",
            ("hero_refresh_reset_done",),
        ).fetchone()
        if reset_marker is None:
            conn.execute("UPDATE players SET hero_refreshed_at=NULL")
            conn.execute(
                "INSERT INTO meta (key, value) VALUES (?, ?)",
                ("hero_refresh_reset_done", "1"),
            )


__all__ = [
    "connect",
    "db_connection",
    "ensure_schema",
    "release_incomplete_assignments",
    "ensure_hero_refresh_column",
    "reset_hero_refresh_once",
    "DB_PATH",
    "LOCK_PATH",
]
