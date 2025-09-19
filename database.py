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
