from __future__ import annotations

from collections.abc import Mapping, Sequence as SequenceCollection
from contextlib import contextmanager
import os
import threading
import time
from typing import Callable, Iterable, Sequence

from dotenv import load_dotenv
from psycopg import Connection, Cursor, Error, connect, errors
from psycopg.rows import dict_row

from flask import has_app_context

load_dotenv()

INITIAL_PLAYER_ID = 293053907

def _build_database_url() -> str:
    env_database_url = os.environ.get("DATABASE_URL")
    if env_database_url:
        return env_database_url
    user = os.environ.get("POSTGRES_USER", "postgres")
    password = os.environ.get("POSTGRES_PASSWORD", "postgres")
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "5432")
    database = os.environ.get("POSTGRES_DB", "stratz_scraper")
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


DATABASE_URL = _build_database_url()

_THREAD_LOCAL = threading.local()
_SCHEMA_INITIALIZED = False
_SCHEMA_ADVISORY_LOCK_ID = int.from_bytes(b"stratzSC", "big")

_RETRYABLE_ERRORS: tuple[type[BaseException], ...] = (
    errors.DeadlockDetected,
    errors.SerializationFailure,
    errors.LockNotAvailable,
    errors.OperationalError,
)


def _create_connection(*, autocommit: bool) -> Connection:
    connection = connect(DATABASE_URL, autocommit=autocommit)
    connection.row_factory = dict_row
    return connection


def row_value(row: Mapping[str, object] | object, key: str) -> object:
    """Return a column value from a database row regardless of key casing."""

    if isinstance(row, Mapping):
        mapping = row
    else:  # pragma: no cover
        mapping = dict(row)

    for candidate in (key, key.lower(), key.upper()):
        if candidate in mapping:
            return mapping[candidate]
    raise KeyError(key)


def ensure_schema_exists() -> None:
    global _SCHEMA_INITIALIZED
    if _SCHEMA_INITIALIZED:
        return
    refresh_needed = False
    with _create_connection(autocommit=False) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT pg_advisory_xact_lock(%s)",
                (_SCHEMA_ADVISORY_LOCK_ID,),
            )
        try:
            ensure_schema(existing=conn)
            ensure_indexes(existing=conn)
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM public.hero_top100 LIMIT 1")
                refresh_needed = cur.fetchone() is None
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    if refresh_needed:
        try:
            refresh_leaderboard_views()
        except Exception:
            # If the cache refresh fails during initialization we still allow the
            # application to start. Submissions keep the leaderboard up to date.
            pass
    _SCHEMA_INITIALIZED = True


def connect_pg(*, autocommit: bool = True) -> Connection:
    ensure_schema_exists()
    return _create_connection(autocommit=autocommit)


@contextmanager
def db_connection(*, write: bool = False) -> Iterable[Connection]:
    ensure_schema_exists()
    connection: Connection | None = None
    cached_connection = False
    if write:
        use_cache = False
        try:
            use_cache = has_app_context()
        except RuntimeError:
            use_cache = False
        if use_cache:
            cache = getattr(_THREAD_LOCAL, "connections", None)
            if cache is None:
                cache = {}
                _THREAD_LOCAL.connections = cache
            connection = cache.get("write")
            if connection is None:
                connection = connect_pg(autocommit=False)
                cache["write"] = connection
            cached_connection = True
        else:
            connection = connect_pg(autocommit=False)
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
        if connection is not None and (not write or not cached_connection):
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
        except errors.OperationalError as e:
            print(e)
            cache = getattr(_THREAD_LOCAL, "connections", None)
            if cache:
                conn_to_close = cache.pop("write", None)
                if conn_to_close is not None:
                    try:
                        conn_to_close.close()
                    except Error:
                        pass
            time.sleep(retry_interval)
            raise
        except _RETRYABLE_ERRORS as e:
            print(e)
            time.sleep(retry_interval)
            continue
        except Error:
            raise


def _reacquire_advisory_lock(
    connection: Connection,
    target: Connection | Cursor,
    lock_key: Sequence | object,
) -> None:
    if isinstance(lock_key, SequenceCollection) and not isinstance(
        lock_key, (bytes, bytearray, str)
    ):
        parameters = tuple(lock_key)
    else:
        parameters = (lock_key,)
    placeholders = ", ".join(["%s"] * len(parameters))
    cursor: Cursor
    close_cursor = False
    if isinstance(target, Cursor):
        cursor = target
    else:
        cursor = connection.cursor()
        close_cursor = True
    try:
        cursor.execute(
            f"SELECT pg_advisory_xact_lock({placeholders})",
            parameters,
        )
    finally:
        if close_cursor:
            cursor.close()


def retryable_executemany(
    target: Connection | Cursor,
    sql: str,
    seq_of_parameters: Iterable[Sequence],
    *,
    retry_interval: float = 0.5,
    reacquire_advisory_lock: Sequence | object | None = None,
    on_rollback: Callable[[], None] | None = None,
):
    """Execute ``executemany`` with automatic retries for transient errors.

    When ``on_rollback`` is provided it will be invoked after a rollback
    triggered by a retryable error.  Callers can use the callback to reset
    any local state that depended on the transaction succeeding (for example
    to restart a batched insert loop from the beginning)."""
    if not isinstance(seq_of_parameters, (list, tuple)):
        seq_of_parameters = list(seq_of_parameters)
    connection = target if isinstance(target, Connection) else target.connection
    while True:
        try:
            cursor: Cursor
            close_cursor = False
            if isinstance(target, Cursor):
                cursor = target
            else:
                cursor = connection.cursor()
                close_cursor = True
            try:
                result = cursor.executemany(sql, seq_of_parameters)
            finally:
                if close_cursor:
                    cursor.close()
            return result
        except _RETRYABLE_ERRORS as e:
            try:
                connection.rollback()
            except Error:
                pass
            if on_rollback is not None:
                on_rollback()
            if reacquire_advisory_lock is not None:
                _reacquire_advisory_lock(
                    connection,
                    target,
                    reacquire_advisory_lock,
                )
            time.sleep(retry_interval)
            continue
        except Error:
            try:
                connection.rollback()
            except Error:
                pass
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
                    depth INTEGER NOT NULL,
                    assigned_to TEXT,
                    assigned_at TIMESTAMPTZ,
                    hero_refreshed_at TIMESTAMPTZ,
                    hero_done BOOLEAN DEFAULT FALSE,
                    highest_match_id BIGINT,
                    discover_done BOOLEAN DEFAULT FALSE,
                    full_write_done BOOLEAN DEFAULT FALSE
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
                CREATE TABLE IF NOT EXISTS hero_top100 (
                    heroId INTEGER NOT NULL,
                    steamAccountId BIGINT NOT NULL,
                    matches INTEGER NOT NULL,
                    wins INTEGER NOT NULL,
                    PRIMARY KEY (heroId, steamAccountId)
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
                CREATE TABLE IF NOT EXISTS progress_snapshots (
                    captured_at TIMESTAMPTZ PRIMARY KEY,
                    players_total BIGINT NOT NULL,
                    hero_done BIGINT NOT NULL,
                    discover_done BIGINT NOT NULL
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
            cur.execute(
                """
                DELETE FROM meta
                WHERE key IN ('hero_assignment_cursor', 'task_assignment_counter')
                """
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
            # Retire obsolete indexes before creating the current set. This keeps the
            # schema lean without leaving behind redundant definitions.
            cur.execute(
                """
                -- stratz_scraper.web.assignment hero assignment lookups
                CREATE INDEX IF NOT EXISTS idx_players_hero_unassigned_queue
                    ON players (steamAccountId)
                    WHERE hero_done=FALSE AND assigned_to IS NULL
                """
            )
            cur.execute(
                """
                -- stratz_scraper.web.assignment._assign_discovery
                CREATE INDEX IF NOT EXISTS idx_players_discover_queue
                    ON players (
                        depth ASC,
                        steamAccountId ASC
                    )
                    WHERE hero_done=TRUE
                      AND discover_done=FALSE
                      AND assigned_to IS NULL
                """
            )
            cur.execute(
                "DROP INDEX IF EXISTS idx_players_hero_refresh_queue"
            )
            cur.execute(
                """
                -- stratz_scraper.web.assignment.assign_next_task refresh scheduling
                CREATE INDEX IF NOT EXISTS idx_players_refresh_queue
                    ON players (
                        hero_refreshed_at ASC NULLS FIRST,
                        steamAccountId ASC
                    )
                    WHERE hero_done=TRUE
                      AND discover_done=TRUE
                      AND assigned_to IS NULL
                """
            )
            cur.execute(
                """
                -- stratz_scraper.web.progress.fetch_progress
                CREATE INDEX IF NOT EXISTS idx_players_hero_completed
                    ON players (steamAccountId)
                    WHERE hero_done=TRUE
                """
            )
            cur.execute(
                """
                -- stratz_scraper.web.assignment._discovery_backlog_exceeded
                CREATE INDEX IF NOT EXISTS idx_players_discover_fullwrite_backlog
                    ON players (steamAccountId)
                    WHERE discover_done=TRUE
                      AND full_write_done=FALSE
                      AND highest_match_id IS NOT NULL
                """
            )
            cur.execute(
                """
                -- stratz_scraper.database.release_incomplete_assignments
                CREATE INDEX IF NOT EXISTS idx_players_assignment_state
                    ON players (
                        assigned_to,
                        assigned_at
                    )
                    WHERE assigned_to IS NOT NULL
                """
            )
            # ``hero_top100`` tops out at roughly 20k rows (100 players per hero)
            # so dedicated indexes are unnecessary. Sequential scans remain cheap
            # while keeping rebuilds simple.
    finally:
        if close_after:
            existing.commit()
            existing.close()


def refresh_leaderboard_views(*, concurrently: bool = True) -> None:
    """Rebuild the cached hero leaderboard table."""

    # ``concurrently`` is kept for API compatibility. The rebuild always runs in
    # a single transaction so the flag is ignored.
    del concurrently

    connection: Connection | None = None
    try:
        connection = _create_connection(autocommit=False)
        with connection.cursor() as cur:
            retryable_execute(cur, "DELETE FROM public.hero_top100")
            retryable_execute(
                cur,
                """
                INSERT INTO public.hero_top100 (heroId, steamAccountId, matches, wins)
                SELECT heroId, steamAccountId, matches, wins
                FROM (
                    SELECT
                        heroId,
                        steamAccountId,
                        matches,
                        wins,
                        ROW_NUMBER() OVER (
                            PARTITION BY heroId
                            ORDER BY matches DESC, wins DESC, steamAccountId
                        ) AS rn
                    FROM public.hero_stats
                ) ranked
                WHERE ranked.rn <= 100
                """,
            )
        connection.commit()
    finally:
        if connection is not None:
            try:
                connection.close()
            except Error:  # pragma: no cover - cleanup best effort
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
                WHERE steamAccountId IN (
                    SELECT steamAccountId
                    FROM players
                    WHERE assigned_to IS NOT NULL
                      AND (
                          assigned_at IS NULL
                          OR assigned_at <= NOW() - (%s)::interval
                      )
                    LIMIT 1000
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
    "refresh_leaderboard_views",
    "release_incomplete_assignments",
    "retryable_execute",
    "retryable_executemany",
    "INITIAL_PLAYER_ID",
    "DATABASE_URL",
]
