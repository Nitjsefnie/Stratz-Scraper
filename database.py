"""Database helpers for the Stratz scraper application."""
from pathlib import Path
import sqlite3

DB_PATH = "dota.db"


def db():
    """Return a connection to the application database."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _table_columns(cur: sqlite3.Cursor, table: str) -> set[str]:
    """Return the set of column names that currently exist on *table*."""

    try:
        rows = cur.execute(f"PRAGMA table_info({table})").fetchall()
    except sqlite3.DatabaseError:
        return set()
    return {row[1] for row in rows}


def ensure_schema() -> None:
    """Ensure the SQLite schema exists and upgrade it in place if required."""

    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    conn = db()
    cur = conn.cursor()

    # Players table -----------------------------------------------------------------
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS players (
            steamAccountId INTEGER PRIMARY KEY,
            depth INTEGER,
            assigned_to TEXT,
            assigned_at DATETIME,
            hero_done INTEGER DEFAULT 0,
            discover_done INTEGER DEFAULT 0
        )
        """
    )
    player_columns = _table_columns(cur, "players")
    if "depth" not in player_columns:
        cur.execute("ALTER TABLE players ADD COLUMN depth INTEGER")
    if "assigned_to" not in player_columns:
        cur.execute("ALTER TABLE players ADD COLUMN assigned_to TEXT")
    if "assigned_at" not in player_columns:
        cur.execute("ALTER TABLE players ADD COLUMN assigned_at DATETIME")
    if "hero_done" not in player_columns:
        cur.execute("ALTER TABLE players ADD COLUMN hero_done INTEGER DEFAULT 0")
    if "discover_done" not in player_columns:
        cur.execute(
            "ALTER TABLE players ADD COLUMN discover_done INTEGER DEFAULT 0"
        )

    # Hero stats table ---------------------------------------------------------------
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS hero_stats (
            steamAccountId INTEGER,
            heroId INTEGER,
            matches INTEGER,
            wins INTEGER,
            PRIMARY KEY (steamAccountId, heroId)
        )
        """
    )
    hero_stat_columns = _table_columns(cur, "hero_stats")
    if "wins" not in hero_stat_columns:
        cur.execute("ALTER TABLE hero_stats ADD COLUMN wins INTEGER")

    # Best table --------------------------------------------------------------------
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS best (
            hero_id INTEGER PRIMARY KEY,
            hero_name TEXT,
            player_id INTEGER,
            matches INTEGER,
            wins INTEGER
        )
        """
    )
    best_columns = _table_columns(cur, "best")
    if "hero_name" not in best_columns:
        cur.execute("ALTER TABLE best ADD COLUMN hero_name TEXT")
    if "player_id" not in best_columns:
        cur.execute("ALTER TABLE best ADD COLUMN player_id INTEGER")
    if "matches" not in best_columns:
        cur.execute("ALTER TABLE best ADD COLUMN matches INTEGER")
    if "wins" not in best_columns:
        cur.execute("ALTER TABLE best ADD COLUMN wins INTEGER")

    # Meta table --------------------------------------------------------------------
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """
    )

    conn.commit()
    conn.close()


def release_incomplete_assignments() -> None:
    """Release tasks that were assigned but never completed."""

    conn = db()
    conn.execute(
        """
        UPDATE players
        SET assigned_to=NULL,
            assigned_at=NULL
        WHERE assigned_to IS NOT NULL
        """
    )
    conn.commit()
    conn.close()


__all__ = ["db", "ensure_schema", "release_incomplete_assignments", "DB_PATH"]
