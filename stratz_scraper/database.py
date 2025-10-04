from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
import os
import sqlite3
import threading
import time
from typing import Iterable, Sequence

from psycopg import Connection, Cursor, Error, connect, errors
from psycopg.rows import dict_row

DB_PATH = Path("dota.db")
INITIAL_PLAYER_ID = 293053907

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/stratz_scraper",
)

_THREAD_LOCAL = threading.local()
_SCHEMA_INITIALIZED = False

_RETRYABLE_ERRORS: tuple[type[BaseException], ...] = (
    errors.DeadlockDetected,
    errors.SerializationFailure,
    errors.LockNotAvailable,
)


def _create_connection(*, autocommit: bool) -> Connection:
    connection = connect(DATABASE_URL, autocommit=autocommit)
    connection.row_factory = dict_row
    return connection


def ensure_schema_exists() -> None:
    global _SCHEMA_INITIALIZED
    if _SCHEMA_INITIALIZED:
        return
    with _create_connection(autocommit=False) as conn:
        ensure_schema(existing=conn)
        ensure_indexes(existing=conn)
        maybe_convert_sqlite(existing=conn)
        conn.commit()
    _SCHEMA_INITIALIZED = True


def connect_pg(*, autocommit: bool = True) -> Connection:
    ensure_schema_exists()
    return _create_connection(autocommit=autocommit)


@contextmanager
def db_connection(*, write: bool = False) -> Iterable[Connection]:
    ensure_schema_exists()
    connection: Connection | None = None
    if write:
        cache = getattr(_THREAD_LOCAL, "connections", None)
        if cache is None:
            cache = {}
            _THREAD_LOCAL.connections = cache
        connection = cache.get("write")
        if connection is not None:
            try:
                with connection.cursor() as cur:
                    cur.execute("SELECT 1")
            except Error:
                try:
                    connection.close()
                except Error:
                    pass
                connection = None
                cache.pop("write", None)
        if connection is None:
            connection = connect_pg(autocommit=False)
            cache["write"] = connection
    else:
        connection = connect_pg(autocommit=True)
    try:
        yield connection
        if write and connection is not None:
            try:
                connection.commit()
            except Error:
                connection.rollback()
                raise
    except Exception:
        if write and connection is not None:
            try:
                connection.rollback()
            except Error:
                pass
        raise
    finally:
        if not write and connection is not None:
            try:
                connection.close()
            except Error:
                pass


def close_cached_connections() -> None:
    cache = getattr(_THREAD_LOCAL, "connections", None)
    if not cache:
        return
    for key in list(cache.keys()):
        conn = cache.pop(key, None)
        if conn is None:
            continue
        try:
            conn.close()
        except Error:
            pass
    _THREAD_LOCAL.connections = {}


def retryable_execute(
    target: Connection | Cursor,
    sql: str,
    parameters: Sequence | None = None,
    *,
    retry_interval: float = 0.5,
):
    if parameters is None:
        parameters = ()
    while True:
        try:
            return target.execute(sql, parameters)
        except _RETRYABLE_ERRORS:
            time.sleep(retry_interval)
            continue
        except Error:
            raise


def retryable_executemany(
    target: Connection | Cursor,
    sql: str,
    seq_of_parameters: Iterable[Sequence],
    *,
    retry_interval: float = 0.5,
):
    if not isinstance(seq_of_parameters, (list, tuple)):
        seq_of_parameters = list(seq_of_parameters)
    connection = target if isinstance(target, Connection) else target.connection
    while True:
        try:
            with connection.transaction():
                cursor = target if isinstance(target, Cursor) else connection.cursor()
                result = cursor.executemany(sql, seq_of_parameters)
            return result
        except _RETRYABLE_ERRORS:
            time.sleep(retry_interval)
            continue
        except Error:
            raise


def ensure_schema(*, existing: Connection | None = None) -> None:
    close_after = False
    if existing is None:
        existing = connect_pg(autocommit=False)
        close_after = True
    try:
        with existing.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS players (
                    steamAccountId BIGINT PRIMARY KEY,
                    depth INTEGER,
                    assigned_to TEXT,
                    assigned_at TIMESTAMPTZ,
                    hero_refreshed_at TIMESTAMPTZ,
                    hero_done BOOLEAN DEFAULT FALSE,
                    discover_done BOOLEAN DEFAULT FALSE,
                    seen_count INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS hero_stats (
                    steamAccountId BIGINT,
                    heroId INTEGER,
                    matches INTEGER,
                    wins INTEGER,
                    PRIMARY KEY (steamAccountId, heroId)
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS best (
                    hero_id INTEGER PRIMARY KEY,
                    hero_name TEXT,
                    player_id BIGINT,
                    matches INTEGER,
                    wins INTEGER
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
                """
            )
            cur.execute(
                """
                INSERT INTO players (steamAccountId, depth)
                VALUES (%s, 0)
                ON CONFLICT (steamAccountId) DO NOTHING
                """,
                (INITIAL_PLAYER_ID,),
            )
    finally:
        if close_after:
            existing.commit()
            existing.close()


def ensure_indexes(*, existing: Connection | None = None) -> None:
    close_after = False
    if existing is None:
        existing = connect_pg(autocommit=False)
        close_after = True
    try:
        with existing.cursor() as cur:
            cur.execute(
                """
                ALTER TABLE players
                ADD COLUMN IF NOT EXISTS seen_count INTEGER NOT NULL DEFAULT 0
                """
            )
            cur.execute(
                "UPDATE players SET seen_count=0 WHERE seen_count IS NULL"
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_players_hero_queue
                    ON players (
                        hero_done,
                        assigned_to,
                        COALESCE(depth, 0),
                        steamAccountId
                    )
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_players_hero_assignment_cursor
                    ON players (
                        hero_done,
                        assigned_to,
                        steamAccountId
                    )
                    WHERE hero_done=FALSE AND assigned_to IS NULL
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_players_hero_queue_seen
                    ON players (
                        hero_done,
                        assigned_to,
                        seen_count DESC,
                        COALESCE(depth, 0),
                        steamAccountId
                    )
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_players_hero_cursor
                    ON players (steamAccountId)
                    WHERE hero_done=FALSE AND assigned_to IS NULL
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_players_hero_pending
                    ON players (steamAccountId)
                    WHERE hero_done=FALSE
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_players_hero_refresh
                    ON players (
                        hero_done,
                        assigned_to,
                        COALESCE(hero_refreshed_at, '1970-01-01'::timestamptz),
                        steamAccountId
                    )
                """
            )
            cur.execute(
                "DROP INDEX IF EXISTS idx_players_hero_refresh_seen"
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_players_hero_refresh_seen
                    ON players (
                        hero_done,
                        assigned_to,
                        COALESCE(hero_refreshed_at, '1970-01-01'::timestamptz),
                        seen_count DESC,
                        steamAccountId
                    )
                    WHERE hero_done=TRUE AND assigned_to IS NULL
                """
            )
            cur.execute(
                "DROP INDEX IF EXISTS idx_players_discover_queue"
            )
            cur.execute(
                "DROP INDEX IF EXISTS idx_players_discover_queue_seen"
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_players_discover_assignment
                    ON players (
                        hero_done,
                        discover_done,
                        (assigned_to IS NOT NULL),
                        seen_count DESC,
                        COALESCE(depth, 0),
                        steamAccountId
                    )
                    WHERE hero_done=TRUE
                      AND discover_done=FALSE
                      AND (assigned_to IS NULL OR assigned_to='discover')
                """
            )
            cur.execute(
                "DROP INDEX IF EXISTS idx_players_assignment_state"
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_players_assignment_state
                    ON players (
                        assigned_to,
                        assigned_at
                    )
                    WHERE assigned_to IS NOT NULL
                """
            )
            cur.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_meta_key
                    ON meta (key)
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_hero_stats_leaderboard
                    ON hero_stats (
                        heroId,
                        matches DESC,
                        wins DESC,
                        steamAccountId
                    )
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_hero_stats_order
                    ON hero_stats (
                        matches DESC,
                        wins DESC,
                        steamAccountId ASC
                    )
                """
            )
    finally:
        if close_after:
            existing.commit()
            existing.close()


def maybe_convert_sqlite(*, existing: Connection | None = None) -> None:
    if not DB_PATH.exists():
        return
    close_after = False
    if existing is None:
        existing = connect_pg(autocommit=False)
        close_after = True
    with existing.cursor() as cur:
        cur.execute("SELECT COUNT(*) AS count FROM players")
        row = cur.fetchone()
        if row and row["count"]:
            return
    with sqlite3.connect(DB_PATH) as legacy_conn:
        legacy_conn.row_factory = sqlite3.Row
        legacy_cur = legacy_conn.cursor()
        with existing.cursor() as cur:
            legacy_cur.execute("SELECT * FROM players")
            player_rows = legacy_cur.fetchall()
            if player_rows:
                retryable_executemany(
                    cur,
                    """
                    INSERT INTO players (
                        steamAccountId,
                        depth,
                        assigned_to,
                        assigned_at,
                        hero_refreshed_at,
                        hero_done,
                        discover_done,
                        seen_count
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (steamAccountId) DO UPDATE SET
                        depth=EXCLUDED.depth,
                        assigned_to=EXCLUDED.assigned_to,
                        assigned_at=EXCLUDED.assigned_at,
                        hero_refreshed_at=EXCLUDED.hero_refreshed_at,
                        hero_done=EXCLUDED.hero_done,
                        discover_done=EXCLUDED.discover_done,
                        seen_count=EXCLUDED.seen_count
                    """,
                    [
                        (
                            row["steamAccountId"],
                            row["depth"],
                            row["assigned_to"],
                            row["assigned_at"],
                            row["hero_refreshed_at"],
                            bool(row["hero_done"]),
                            bool(row["discover_done"]),
                            row["seen_count"],
                        )
                        for row in player_rows
                    ],
                )
            legacy_cur.execute("SELECT * FROM hero_stats")
            hero_rows = legacy_cur.fetchall()
            if hero_rows:
                retryable_executemany(
                    cur,
                    """
                    INSERT INTO hero_stats (steamAccountId, heroId, matches, wins)
                    VALUES (%s,%s,%s,%s)
                    ON CONFLICT (steamAccountId, heroId) DO UPDATE SET
                        matches=EXCLUDED.matches,
                        wins=EXCLUDED.wins
                    """,
                    [
                        (
                            row["steamAccountId"],
                            row["heroId"],
                            row["matches"],
                            row["wins"],
                        )
                        for row in hero_rows
                    ],
                )
            legacy_cur.execute("SELECT * FROM best")
            best_rows = legacy_cur.fetchall()
            if best_rows:
                retryable_executemany(
                    cur,
                    """
                    INSERT INTO best (hero_id, hero_name, player_id, matches, wins)
                    VALUES (%s,%s,%s,%s,%s)
                    ON CONFLICT (hero_id) DO UPDATE SET
                        hero_name=EXCLUDED.hero_name,
                        player_id=EXCLUDED.player_id,
                        matches=EXCLUDED.matches,
                        wins=EXCLUDED.wins
                    """,
                    [
                        (
                            row["hero_id"],
                            row["hero_name"],
                            row["player_id"],
                            row["matches"],
                            row["wins"],
                        )
                        for row in best_rows
                    ],
                )
            legacy_cur.execute("SELECT * FROM meta")
            meta_rows = legacy_cur.fetchall()
            if meta_rows:
                retryable_executemany(
                    cur,
                    """
                    INSERT INTO meta (key, value)
                    VALUES (%s,%s)
                    ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value
                    """,
                    [
                        (
                            row["key"],
                            row["value"],
                        )
                        for row in meta_rows
                    ],
                )
    if close_after:
        existing.commit()
        existing.close()
    backup_path = DB_PATH.with_suffix(".converted")
    try:
        DB_PATH.rename(backup_path)
    except OSError:
        pass


def release_incomplete_assignments(
    max_age_minutes: int = 10,
    existing: Connection | None = None,
) -> int:
    age_interval = f"{int(max_age_minutes)} minutes"
    close_after = False
    if existing is None:
        existing = connect_pg(autocommit=False)
        close_after = True
    try:
        with existing.cursor() as cur:
            cursor = retryable_execute(
                cur,
                """
                UPDATE players
                SET assigned_to=NULL,
                    assigned_at=NULL
                WHERE assigned_to IS NOT NULL
                  AND (
                      assigned_at IS NULL
                      OR assigned_at <= NOW() - (%s)::interval
                  )
                """,
                (age_interval,),
            )
            return cursor.rowcount if cursor.rowcount is not None else 0
    finally:
        if close_after:
            existing.commit()
            existing.close()


__all__ = [
    "connect_pg",
    "db_connection",
    "close_cached_connections",
    "ensure_schema_exists",
    "ensure_schema",
    "ensure_indexes",
    "maybe_convert_sqlite",
    "release_incomplete_assignments",
    "retryable_execute",
    "retryable_executemany",
    "DB_PATH",
    "INITIAL_PLAYER_ID",
    "DATABASE_URL",
]
