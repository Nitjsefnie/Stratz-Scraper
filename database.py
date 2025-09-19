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
    """Ensure the SQLite schema exists before the app starts."""
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    conn = db()
    cur = conn.cursor()
    cur.execute(
        """
    CREATE TABLE IF NOT EXISTS players (
        id INTEGER PRIMARY KEY,
        assigned_to TEXT,
        assigned_at INTEGER,
        done INTEGER DEFAULT 0
    )
    """
    )
    cur.execute(
        """
    CREATE TABLE IF NOT EXISTS hero_stats (
        player_id INTEGER,
        hero_id INTEGER,
        hero_name TEXT,
        matches INTEGER,
        wins INTEGER,
        PRIMARY KEY (player_id, hero_id)
    )
    """
    )
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
        WHERE done=0
          AND assigned_to IS NOT NULL
        """
    )
    conn.commit()
    conn.close()


__all__ = ["db", "ensure_schema", "release_incomplete_assignments", "DB_PATH"]
