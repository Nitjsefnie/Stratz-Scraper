"""Database helpers for the Stratz scraper application."""
from pathlib import Path
import sqlite3

DB_PATH = "dota.db"


def db():
    """Return a connection to the application database."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_schema() -> None:
    """Recreate the SQLite schema used by the scraper."""

    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    conn = db()
    cur = conn.cursor()
    cur.executescript(
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
    conn.commit()
    conn.close()


def release_incomplete_assignments(max_age_minutes: int = 5, connection=None) -> int:
    """Release stale task assignments.

    Parameters
    ----------
    max_age_minutes:
        Tasks assigned longer ago than this threshold are released. The default
        value of five minutes mirrors the requirement that assignments time out
        quickly so other workers may continue processing.
    connection:
        Optional existing database connection. When provided, the caller is
        responsible for committing the transaction. Otherwise the helper will
        create, commit, and close its own connection.

    Returns
    -------
    int
        The number of assignments that were released.
    """

    age_modifier = f"-{int(max_age_minutes)} minutes"
    owns_connection = connection is None
    conn = connection or db()
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
    released = cursor.rowcount if cursor.rowcount is not None else 0
    if owns_connection:
        conn.commit()
        conn.close()
    return released


def ensure_hero_refresh_column() -> None:
    """Ensure the players table has the hero_refreshed_at column."""

    conn = db()
    cur = conn.cursor()
    columns = cur.execute("PRAGMA table_info(players)").fetchall()
    if not any(col[1] == "hero_refreshed_at" for col in columns):
        cur.execute("ALTER TABLE players ADD COLUMN hero_refreshed_at DATETIME")
        conn.commit()
    conn.close()


def reset_hero_refresh_once() -> None:
    """Reset hero_refreshed_at for all players the first time the app restarts."""

    conn = db()
    cur = conn.cursor()
    cur.execute(
        "CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
    )
    conn.commit()
    cur.execute("BEGIN")
    reset_marker = cur.execute(
        "SELECT value FROM meta WHERE key=?",
        ("hero_refresh_reset_done",),
    ).fetchone()
    if reset_marker is None:
        cur.execute("UPDATE players SET hero_refreshed_at=NULL")
        cur.execute(
            "INSERT INTO meta (key, value) VALUES (?, ?)",
            ("hero_refresh_reset_done", "1"),
        )
    conn.commit()
    conn.close()


__all__ = [
    "db",
    "ensure_schema",
    "release_incomplete_assignments",
    "ensure_hero_refresh_column",
    "reset_hero_refresh_once",
    "DB_PATH",
]
