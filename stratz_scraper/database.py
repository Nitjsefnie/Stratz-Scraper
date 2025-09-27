from __future__ import annotations

from contextlib import contextmanager, nullcontext
from pathlib import Path
import sqlite3
import time

from .locking import FileLock

DB_PATH = Path("dota.db")
LOCK_PATH = DB_PATH.with_suffix(".lock")
INITIAL_PLAYER_ID = 293053907

_INDEXES_ENSURED = False


def ensure_schema_exists() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    if DB_PATH.exists():
        ensure_indexes()
        return
    with FileLock(LOCK_PATH):
        if DB_PATH.exists():
            ensure_indexes(lock_acquired=True)
            return
        ensure_schema(lock_acquired=True)


def connect() -> sqlite3.Connection:
    ensure_schema_exists()
    connection = sqlite3.connect(DB_PATH, timeout=20, isolation_level=None)
    connection.execute("PRAGMA busy_timeout = 20000")
    connection.row_factory = sqlite3.Row
    return connection


@contextmanager
def db_connection(write: bool = False) -> sqlite3.Connection:
    ensure_schema_exists()
    conn = connect()
    try:
        yield conn
    finally:
        conn.close()


SQL_WRITE_KEYWORDS = {
    "INSERT",
    "UPDATE",
    "DELETE",
    "REPLACE",
    "CREATE",
    "DROP",
    "ALTER",
    "PRAGMA",
    "VACUUM",
    "REINDEX",
    "ATTACH",
    "DETACH",
    "ANALYZE",
}


def _sql_requires_lock(sql: str) -> bool:
    stripped = sql.lstrip()
    if not stripped:
        return False
    upper_sql = stripped.upper()
    first_token = upper_sql.split(None, 1)[0]
    if first_token == "SELECT":
        return False
    if first_token == "WITH":
        return any(keyword in upper_sql for keyword in SQL_WRITE_KEYWORDS)
    return True


def retryable_execute(
    target: sqlite3.Connection | sqlite3.Cursor,
    sql: str,
    parameters=(),
    *,
    use_file_lock: bool | None = None,
):
    #should_lock = _sql_requires_lock(sql) if use_file_lock is None else use_file_lock
    #if should_lock:
    #    with FileLock(LOCK_PATH):
    #        return target.execute(sql, parameters)
    while True:
        try:
            return target.execute(sql, parameters)
        except sqlite3.OperationalError as exc:
            message = str(exc).lower()
            if "locked" in message or "busy" in message:
                time.sleep(0.05)
                continue
            raise


def retryable_executemany(
    target: sqlite3.Connection | sqlite3.Cursor, sql: str, seq_of_parameters
):
    if not isinstance(seq_of_parameters, (list, tuple)):
        seq_of_parameters = list(seq_of_parameters)
    connection = target if isinstance(target, sqlite3.Connection) else target.connection
    while True:
        try:
            connection.execute("BEGIN IMMEDIATE")
            break
        except sqlite3.OperationalError as exc:
            message = str(exc).lower()
            if "locked" in message or "busy" in message:
                time.sleep(0.05)
                continue
            raise

    try:
        result = target.executemany(sql, seq_of_parameters)
        connection.execute("COMMIT")
        return result
    except Exception:
        try:
            connection.execute("ROLLBACK")
        except sqlite3.OperationalError:
            pass
        raise


def ensure_schema(*, lock_acquired: bool = False) -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    lock_ctx = nullcontext() if lock_acquired else FileLock(LOCK_PATH)
    with lock_ctx:
        with sqlite3.connect(DB_PATH, timeout=30, isolation_level=None) as conn:
            conn.execute("PRAGMA busy_timeout = 5000")
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.executescript(
                """
                DROP TABLE IF EXISTS hero_stats;
                DROP TABLE IF EXISTS players;
                DROP TABLE IF EXISTS meta;
                DROP TABLE IF EXISTS best;
                DROP TABLE IF EXISTS task_durations;

                CREATE TABLE players (
                    steamAccountId INTEGER PRIMARY KEY,
                    depth INTEGER,
                    assigned_to TEXT,
                    assigned_at DATETIME,
                    hero_refreshed_at DATETIME,
                    hero_done INTEGER DEFAULT 0,
                    discover_done INTEGER DEFAULT 0,
                    seen_count INTEGER NOT NULL DEFAULT 0
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

                CREATE TABLE task_durations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    steamAccountId INTEGER,
                    task_type TEXT NOT NULL,
                    assigned_at DATETIME,
                    submitted_at DATETIME,
                    duration_seconds REAL
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
        ensure_indexes(lock_acquired=True)


def ensure_indexes(*, lock_acquired: bool = False) -> None:
    global _INDEXES_ENSURED
    if _INDEXES_ENSURED:
        return
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    lock_ctx = nullcontext() if lock_acquired else FileLock(LOCK_PATH)
    with lock_ctx:
        with sqlite3.connect(DB_PATH, timeout=30, isolation_level=None) as conn:
            conn.execute("PRAGMA busy_timeout = 5000")
            try:
                conn.execute(
                    "ALTER TABLE players ADD COLUMN seen_count INTEGER NOT NULL DEFAULT 0"
                )
            except sqlite3.OperationalError as exc:
                message = str(exc).lower()
                if "duplicate column name" not in message:
                    raise
            conn.execute(
                "UPDATE players SET seen_count=0 WHERE seen_count IS NULL"
            )
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS task_durations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    steamAccountId INTEGER,
                    task_type TEXT NOT NULL,
                    assigned_at DATETIME,
                    submitted_at DATETIME,
                    duration_seconds REAL
                );
                CREATE INDEX IF NOT EXISTS idx_task_durations_account
                    ON task_durations (steamAccountId);
                CREATE INDEX IF NOT EXISTS idx_task_durations_type
                    ON task_durations (task_type);
                CREATE INDEX IF NOT EXISTS idx_task_durations_submitted
                    ON task_durations (submitted_at);
                CREATE INDEX IF NOT EXISTS idx_players_hero_queue
                    ON players (
                        hero_done,
                        assigned_to,
                        COALESCE(depth, 0),
                        steamAccountId
                    );
                CREATE INDEX IF NOT EXISTS idx_players_hero_queue_seen
                    ON players (
                        hero_done,
                        assigned_to,
                        seen_count DESC,
                        COALESCE(depth, 0),
                        steamAccountId
                    );
                CREATE INDEX IF NOT EXISTS idx_players_hero_refresh
                    ON players (
                        hero_done,
                        assigned_to,
                        COALESCE(hero_refreshed_at, '1970-01-01'),
                        steamAccountId
                    );
                DROP INDEX IF EXISTS idx_players_hero_refresh_seen;
                CREATE INDEX idx_players_hero_refresh_seen
                    ON players (
                        hero_done,
                        assigned_to,
                        COALESCE(hero_refreshed_at, '1970-01-01'),
                        seen_count DESC,
                        steamAccountId
                    )
                    WHERE hero_done=1 AND assigned_to IS NULL;
                DROP INDEX IF EXISTS idx_players_discover_queue;
                DROP INDEX IF EXISTS idx_players_discover_queue_seen;
                CREATE INDEX IF NOT EXISTS idx_players_discover_assignment
                    ON players (
                        hero_done,
                        discover_done,
                        (assigned_to IS NOT NULL),
                        seen_count DESC,
                        COALESCE(depth, 0),
                        steamAccountId
                    )
                    WHERE hero_done=1
                      AND discover_done=0
                      AND (assigned_to IS NULL OR assigned_to='discover');
                CREATE INDEX IF NOT EXISTS idx_players_assignment_state
                    ON players (
                        assigned_to,
                        assigned_at
                    );
                CREATE INDEX IF NOT EXISTS idx_hero_stats_leaderboard
                    ON hero_stats (
                        heroId,
                        matches DESC,
                        wins DESC,
                        steamAccountId
                    );
                """
            )
    _INDEXES_ENSURED = True


def release_incomplete_assignments(max_age_minutes: int = 10, existing: sqlite3.Connection | None = None) -> int:
    age_modifier = f"-{int(max_age_minutes)} minutes"
    if existing is None:
        with db_connection(write=True) as conn:
            cursor = retryable_execute(
                conn,
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
    cursor = retryable_execute(
        existing,
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
    "ensure_indexes",
    "release_incomplete_assignments",
    "retryable_execute",
    "retryable_executemany",
    "DB_PATH",
    "LOCK_PATH",
    "INITIAL_PLAYER_ID",
]
