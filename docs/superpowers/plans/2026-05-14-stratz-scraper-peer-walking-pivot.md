# Stratz-Scraper Peer-Walking Pivot Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace match-history discovery with the Stratz `peers` endpoint. Collapse hero/discover/refresh three-phase BFS into one `scrape_player` task. Fresh schema, no migration.

**Architecture:** One GraphQL document per scrape (heroes + WITH peers + AGAINST peers + latest match ID), paginated per side until short page. Worker submits union of teammates ∪ opponents to a unified `/submit` endpoint that upserts hero_stats, COPYs discovered peers, and marks the player scraped. Spec at `docs/superpowers/specs/2026-05-14-stratz-scraper-peer-walking-pivot-design.md`.

**Tech Stack:** Python 3, Flask, `psycopg` (binary), PostgreSQL, vanilla JavaScript worker.

**Verification mode (per design):** Code review of the diff only. No new tests. Each task ends with `python3 -c "from stratz_scraper import create_app; create_app()"` to confirm the module imports and the app factory still runs. Then commit. Every commit body MUST end with the trailer `Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>` (or whichever model is doing the commit, per CLAUDE.md).

**Pre-flight:** Drop the existing `stratz_scraper` PostgreSQL database before testing the running server. The schema changes are not backwards compatible. This is fine because the DB is empty/nonexistent.

---

## Task Ordering Notes

- Each task is independently committable, but smoke-checks may temporarily fail between Task 2 (backend supports `scrape_player`) and Task 5 (worker JS knows about `scrape_player`). That's acceptable since the DB is empty and no live workers will hit the broken state.
- Task ordering within the backend tier is chosen so that import-graph consistency is maintained at every step.

---

## Task 1: Rewrite the database schema

**Files:**
- Modify: `stratz_scraper/database.py` (the `ensure_schema` and `ensure_indexes` functions)

- [ ] **Step 1: Replace `ensure_schema`**

Replace the body of `ensure_schema` (lines ~282-361) with:

```python
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
                    scraped_at TIMESTAMPTZ,
                    latest_match_id BIGINT
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
                    scraped BIGINT NOT NULL
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
```

Note: the `DELETE FROM meta WHERE key IN ('hero_assignment_cursor', 'task_assignment_counter')` migration is gone — those keys never exist in the new schema.

- [ ] **Step 2: Replace `ensure_indexes`**

Replace the body of `ensure_indexes` (lines ~364-445) with:

```python
def ensure_indexes(*, existing: Connection | None = None) -> None:
    close_after = False
    if existing is None:
        existing = connect_pg(autocommit=False)
        close_after = True
    try:
        with existing.cursor() as cur:
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_players_scrape_queue
                    ON players (depth ASC, steamAccountId ASC)
                    WHERE scraped_at IS NULL AND assigned_to IS NULL
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_players_rescrape_queue
                    ON players (scraped_at ASC, steamAccountId ASC)
                    WHERE scraped_at IS NOT NULL AND assigned_to IS NULL
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_players_assignment_state
                    ON players (assigned_to, assigned_at)
                    WHERE assigned_to IS NOT NULL
                """
            )
    finally:
        if close_after:
            existing.commit()
            existing.close()
```

- [ ] **Step 3: Smoke-check imports**

Run: `python3 -c "from stratz_scraper.database import ensure_schema, ensure_indexes"`
Expected: exit 0.

- [ ] **Step 4: Commit**

```bash
git add stratz_scraper/database.py
git commit -m "$(cat <<'EOF'
schema: rewrite players + progress_snapshots for peer-walking pivot

Drops hero_done / discover_done / hero_refreshed_at / highest_match_id
/ full_write_done from players. Adds scraped_at, latest_match_id.
Drops six old partial indexes, adds two new ones (frontier, rescrape).
Drops the orphan-meta migration; those keys never appear in the new
schema. progress_snapshots column 'hero_done' renamed 'scraped',
column 'discover_done' dropped.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: Collapse submission handlers + /submit route

**Files:**
- Modify: `stratz_scraper/web/submissions.py` (major rewrite)
- Modify: `stratz_scraper/web/app.py` (/submit route + imports)

These two files MUST land together: dropping `submit_hero_submission` / `submit_discover_submission` from submissions.py breaks app.py's imports unless app.py is updated in the same commit.

- [ ] **Step 1: Rewrite `submissions.py`**

Replace the entire file with:

```python
"""Background submission helpers."""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from typing import Iterable, List

from psycopg import errors

from ..database import (
    close_cached_connections,
    db_connection,
    retryable_execute,
    retryable_executemany,
)

BACKGROUND_EXECUTOR = ThreadPoolExecutor(max_workers=1)
_DISCOVERY_SUBMISSION_LOCK_ID = int.from_bytes(b"discover", "big")

_RETRYABLE_ERRORS = (
    errors.DeadlockDetected,
    errors.SerializationFailure,
    errors.LockNotAvailable,
)

__all__ = [
    "BACKGROUND_EXECUTOR",
    "process_scrape_submission",
    "submit_scrape_submission",
]


def _submit_background(func, /, *args, **kwargs) -> None:
    def _runner() -> None:
        try:
            func(*args, **kwargs)
        finally:
            close_cached_connections()

    BACKGROUND_EXECUTOR.submit(_runner)


def _extract_hero_rows(
    steam_account_id: int, heroes_payload: Iterable[dict] | None
) -> List[tuple[int, int, int, int]]:
    rows: List[tuple[int, int, int, int]] = []
    seen: set[int] = set()
    if heroes_payload is None:
        return rows
    for hero in heroes_payload:
        try:
            hero_id = int(hero["heroId"])
            matches_value = hero.get("matches", hero.get("games"))
            if matches_value is None:
                continue
            matches = int(matches_value)
            wins = int(hero.get("wins", 0))
        except (KeyError, TypeError, ValueError):
            continue
        if hero_id in seen:
            continue
        seen.add(hero_id)
        rows.append((steam_account_id, hero_id, matches, wins))
    return rows


def _copy_discovered_rows(
    conn,
    steam_account_ids: list[int],
    next_depth: int,
) -> None:
    """Bulk-insert discovered children using COPY + a temp table upsert."""

    while True:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TEMP TABLE discovered_tmp (
                        steamAccountId BIGINT NOT NULL,
                        depth INTEGER NOT NULL
                    ) ON COMMIT DROP
                    """
                )
                with cur.copy(
                    "COPY discovered_tmp (steamAccountId, depth) FROM STDIN"
                ) as copy:
                    for sid in steam_account_ids:
                        copy.write_row((sid, next_depth))
                cur.execute(
                    """
                    INSERT INTO players (steamAccountId, depth)
                    SELECT steamAccountId, depth FROM discovered_tmp
                    ON CONFLICT (steamAccountId) DO UPDATE
                    SET depth = excluded.depth
                    WHERE excluded.depth < players.depth
                    """
                )
            conn.commit()
            return
        except _RETRYABLE_ERRORS:
            try:
                conn.rollback()
            except Exception:
                pass
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT pg_advisory_xact_lock(%s)",
                    (_DISCOVERY_SUBMISSION_LOCK_ID,),
                )
            time.sleep(0.5)


def _unmark_scrape_task(steam_account_id: int) -> None:
    """Clear a failed scrape's lease so the player is eligible for re-scrape."""
    try:
        with db_connection(write=True) as conn:
            cur = conn.cursor()
            retryable_execute(
                cur,
                """
                UPDATE players
                SET assigned_to=NULL, assigned_at=NULL, scraped_at=NULL
                WHERE steamAccountId=%s
                """,
                (steam_account_id,),
            )
    except Exception:
        import traceback
        traceback.print_exc()


def process_scrape_submission(
    steam_account_id: int,
    heroes_payload: Iterable[dict] | None,
    discovered_ids: list[int],
    latest_match_id: int | None,
    next_depth: int,
) -> None:
    hero_rows = _extract_hero_rows(steam_account_id, heroes_payload)
    try:
        with db_connection(write=True) as conn:
            cur = conn.cursor()
            if hero_rows:
                retryable_executemany(
                    cur,
                    """
                    INSERT INTO hero_stats (steamAccountId, heroId, matches, wins)
                    VALUES (%s,%s,%s,%s)
                    ON CONFLICT(steamAccountId, heroId) DO UPDATE SET
                        matches = excluded.matches,
                        wins = excluded.wins
                    """,
                    hero_rows,
                )
            if discovered_ids:
                _copy_discovered_rows(conn, discovered_ids, next_depth)
            retryable_execute(
                cur,
                """
                UPDATE players
                SET scraped_at = NOW(),
                    latest_match_id = COALESCE(%s, latest_match_id)
                WHERE steamAccountId=%s
                """,
                (latest_match_id, steam_account_id),
            )
    except Exception:
        import traceback
        print(
            f"[submit-background] scrape failed for {steam_account_id}",
            flush=True,
        )
        traceback.print_exc()
        _unmark_scrape_task(steam_account_id)


def submit_scrape_submission(
    steam_account_id: int,
    heroes_payload: Iterable[dict] | None,
    discovered_ids: list[int],
    latest_match_id: int | None,
    next_depth: int,
) -> None:
    _submit_background(
        process_scrape_submission,
        steam_account_id,
        heroes_payload,
        discovered_ids,
        latest_match_id,
        next_depth,
    )
```

- [ ] **Step 2: Rewrite the `/submit` route in `web/app.py`**

In `stratz_scraper/web/app.py`, update the imports near the top:

Replace:
```python
from .submissions import submit_discover_submission, submit_hero_submission
```

with:
```python
from .submissions import submit_scrape_submission
```

Then replace the entire `submit()` function body (lines ~100-296) with:

```python
    @app.post("/submit")
    def submit():
        data = request.get_json(force=True) or {}
        task_type = data.get("type")
        request_new_task = data.get("task") is True

        if task_type != "scrape_player":
            return jsonify({"status": "error", "message": "Unknown submit type"}), 400

        try:
            steam_account_id = int(data["steamAccountId"])
        except (KeyError, TypeError, ValueError):
            return jsonify({"status": "error", "message": "steamAccountId is required"}), 400
        if steam_account_id <= 0:
            return jsonify({"status": "error", "message": "steamAccountId must be positive"}), 400

        heroes_payload = data.get("heroes") or []
        if not isinstance(heroes_payload, list):
            heroes_payload = []

        # Normalise discovered IDs (accepts either ints or {steamAccountId} dicts).
        raw_discovered = data.get("discovered") or []
        discovered_ids: list[int] = []
        seen: set[int] = set()
        if isinstance(raw_discovered, list):
            for entry in raw_discovered:
                candidate = entry.get("steamAccountId") if isinstance(entry, dict) else entry
                try:
                    cid = int(candidate) if candidate is not None else None
                except (TypeError, ValueError):
                    continue
                if cid is None or cid <= 0 or cid == steam_account_id or cid in seen:
                    continue
                seen.add(cid)
                discovered_ids.append(cid)

        # Normalise latest_match_id.
        latest_match_id_raw = data.get("latestMatchId")
        try:
            latest_match_id = int(latest_match_id_raw) if latest_match_id_raw is not None else None
        except (TypeError, ValueError):
            latest_match_id = None
        if latest_match_id is not None and latest_match_id < 0:
            latest_match_id = None

        # Normalise depth (optional client-supplied; falls back to DB row's depth).
        depth_raw = data.get("depth")
        try:
            provided_depth = int(depth_raw) if depth_raw is not None else None
        except (TypeError, ValueError):
            provided_depth = None

        # Foreground: mark player scraped, release the lease, fetch next task.
        next_task = None
        with db_connection(write=True) as conn:
            cur = conn.cursor()
            update_row = retryable_execute(
                cur,
                """
                UPDATE players
                SET scraped_at = NOW(),
                    latest_match_id = COALESCE(%s, latest_match_id),
                    assigned_to = NULL,
                    assigned_at = NULL
                WHERE steamAccountId=%s
                RETURNING depth
                """,
                (latest_match_id, steam_account_id),
                retry_interval=ASSIGNMENT_RETRY_INTERVAL,
            ).fetchone()
            if update_row is None:
                return jsonify({"status": "error", "message": "Player not found"}), 404
            assignment_depth = int(update_row["depth"])
            if request_new_task:
                next_task = assign_next_task(connection=conn)

        next_depth = (provided_depth if provided_depth is not None else assignment_depth) + 1

        submit_scrape_submission(
            steam_account_id,
            heroes_payload,
            discovered_ids,
            latest_match_id,
            next_depth,
        )

        response_payload = {"status": "ok"}
        if request_new_task:
            response_payload["task"] = next_task
        return jsonify(response_payload)
```

- [ ] **Step 3: Smoke-check imports + app factory**

Run: `python3 -c "from stratz_scraper.web.submissions import process_scrape_submission, submit_scrape_submission"`
Expected: exit 0.

Run: `python3 -c "from stratz_scraper import create_app; create_app()"`
Expected: exit 0.

- [ ] **Step 4: Commit**

```bash
git add stratz_scraper/web/submissions.py stratz_scraper/web/app.py
git commit -m "$(cat <<'EOF'
submissions: collapse hero+discover handlers into process_scrape_submission

One background handler that upserts hero_stats, COPYs discovered
peer IDs into players, and marks the player scraped. /submit accepts
the unified 'scrape_player' task type; old fetch_hero_stats /
discover_matches branches removed. Foreground UPDATE clears the lease
and sets scraped_at + latest_match_id; background does the heavy I/O
and an idempotent re-stamp. On background failure _unmark_scrape_task
nulls scraped_at so the player is immediately re-eligible.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: Collapse the assignment scheduler

**Files:**
- Modify: `stratz_scraper/web/assignment.py` (major rewrite)

- [ ] **Step 1: Rewrite `assignment.py`**

Replace the entire file with:

```python
"""Task assignment helpers."""

from __future__ import annotations

import logging
import threading
from datetime import datetime, timedelta, timezone

from ..database import (
    db_connection,
    release_incomplete_assignments,
    retryable_execute,
    row_value,
)

ASSIGNMENT_CLEANUP_KEY = "last_assignment_cleanup"
ASSIGNMENT_CLEANUP_INTERVAL = timedelta(seconds=60)
ASSIGNMENT_RETRY_INTERVAL = 0.05

_LOGGER = logging.getLogger(__name__)

_cleanup_thread: threading.Thread | None = None
_cleanup_stop_event: threading.Event | None = None
_cleanup_lock = threading.Lock()

__all__ = [
    "ASSIGNMENT_CLEANUP_INTERVAL",
    "ASSIGNMENT_CLEANUP_KEY",
    "ASSIGNMENT_RETRY_INTERVAL",
    "assign_next_task",
    "ensure_assignment_cleanup_scheduler",
    "maybe_run_assignment_cleanup",
]


def _cleanup_worker(stop_event: threading.Event) -> None:
    interval_seconds = max(int(ASSIGNMENT_CLEANUP_INTERVAL.total_seconds()), 1)
    while not stop_event.is_set():
        try:
            with db_connection(write=True) as conn:
                maybe_run_assignment_cleanup(conn)
        except Exception:  # pragma: no cover - best effort logging
            _LOGGER.exception("Assignment cleanup worker failed")
        stop_event.wait(interval_seconds)


def ensure_assignment_cleanup_scheduler() -> None:
    """Start the background worker that periodically releases stale assignments."""

    global _cleanup_thread, _cleanup_stop_event
    with _cleanup_lock:
        if _cleanup_thread and _cleanup_thread.is_alive():
            return
        stop_event = threading.Event()
        thread = threading.Thread(
            target=_cleanup_worker,
            args=(stop_event,),
            name="assignment-cleanup",
            daemon=True,
        )
        thread.start()
        _cleanup_thread = thread
        _cleanup_stop_event = stop_event


def maybe_run_assignment_cleanup(conn) -> bool:
    """Release stale assignments if the cleanup interval has elapsed."""
    cur = conn.cursor()
    now = datetime.now(timezone.utc)
    last_cleanup_row = cur.execute(
        "SELECT value FROM meta WHERE key=%s",
        (ASSIGNMENT_CLEANUP_KEY,),
    ).fetchone()
    if last_cleanup_row:
        try:
            last_cleanup = datetime.fromisoformat(last_cleanup_row["value"])
        except (TypeError, ValueError):
            pass
        else:
            if last_cleanup.tzinfo is None:
                last_cleanup = last_cleanup.replace(tzinfo=timezone.utc)
            if now - last_cleanup < ASSIGNMENT_CLEANUP_INTERVAL:
                return False
    release_incomplete_assignments(existing=conn)
    retryable_execute(
        cur,
        """
        INSERT INTO meta (key, value)
        VALUES (%s, %s)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value
        """,
        (ASSIGNMENT_CLEANUP_KEY, now.isoformat()),
        retry_interval=ASSIGNMENT_RETRY_INTERVAL,
    )
    return True


def assign_next_task(
    *,
    run_cleanup: bool = False,
    connection=None,
) -> dict | None:
    """Select the next task to hand to a worker.

    When ``connection`` is provided the caller is responsible for committing
    or rolling back the surrounding transaction. Otherwise a managed write
    connection is opened for the duration of the scheduler work.
    """

    if connection is None:
        with db_connection(write=True) as managed_conn:
            return _assign_next_task_on_connection(
                managed_conn,
                run_cleanup=run_cleanup,
            )
    return _assign_next_task_on_connection(connection, run_cleanup=run_cleanup)


def _assign_next_task_on_connection(connection, *, run_cleanup: bool) -> dict | None:
    if run_cleanup:
        maybe_run_assignment_cleanup(connection)

    with connection.cursor() as cur:
        assigned = retryable_execute(
            cur,
            """
            WITH frontier AS (
                SELECT steamAccountId, depth FROM players
                WHERE scraped_at IS NULL
                  AND assigned_to IS NULL
                  AND steamAccountId > 0
                ORDER BY depth ASC, steamAccountId ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            ),
            refresh_pool AS (
                SELECT steamAccountId, depth FROM players
                WHERE scraped_at IS NOT NULL
                  AND assigned_to IS NULL
                  AND steamAccountId > 0
                ORDER BY scraped_at ASC, steamAccountId ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            ),
            chosen AS (
                SELECT * FROM frontier
                UNION ALL
                SELECT * FROM refresh_pool
                WHERE NOT EXISTS (SELECT 1 FROM frontier)
            )
            UPDATE players
            SET assigned_to = 'scrape',
                assigned_at = CURRENT_TIMESTAMP
            FROM chosen
            WHERE players.steamAccountId = chosen.steamAccountId
            RETURNING players.steamAccountId, players.depth, players.latest_match_id
            """,
            retry_interval=ASSIGNMENT_RETRY_INTERVAL,
        ).fetchone()

        if assigned is None:
            return None

        try:
            steam_account_id = int(row_value(assigned, "steamAccountId"))
        except (TypeError, ValueError):
            return None
        if steam_account_id <= 0:
            return None

        depth_value = row_value(assigned, "depth")
        try:
            depth = int(depth_value) if depth_value is not None else 0
        except (TypeError, ValueError):
            depth = 0

        latest_match_value = row_value(assigned, "latest_match_id")
        try:
            latest_match_id = (
                int(latest_match_value) if latest_match_value is not None else None
            )
        except (TypeError, ValueError):
            latest_match_id = None

        return {
            "type": "scrape_player",
            "steamAccountId": steam_account_id,
            "depth": depth,
            "latestMatchId": latest_match_id,
        }
```

- [ ] **Step 2: Smoke-check imports + app factory**

Run: `python3 -c "from stratz_scraper.web.assignment import assign_next_task, ensure_assignment_cleanup_scheduler, ASSIGNMENT_RETRY_INTERVAL"`
Expected: exit 0.

Run: `python3 -c "from stratz_scraper import create_app; create_app()"`
Expected: exit 0.

- [ ] **Step 3: Commit**

```bash
git add stratz_scraper/web/assignment.py
git commit -m "$(cat <<'EOF'
assignment: collapse hero+discover+refresh into single scrape_player query

One CTE-based UPDATE picks from the unscraped frontier first, falling
back to the LRU re-scrape pool. Returns one task per call (no batching).
Drops _DiscoveryThrottle / _discovery_backlog_exceeded / hero cursor
remnants entirely.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: Simplify task reset

**Files:**
- Modify: `stratz_scraper/web/tasks.py`

- [ ] **Step 1: Replace the entire file**

Replace `stratz_scraper/web/tasks.py` with:

```python
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
```

- [ ] **Step 2: Smoke-check**

Run: `python3 -c "from stratz_scraper.web.tasks import reset_player_task"`
Expected: exit 0.

- [ ] **Step 3: Commit**

```bash
git add stratz_scraper/web/tasks.py
git commit -m "$(cat <<'EOF'
tasks: collapse three reset variants into one

Every scrape task resets the same way: clear assigned_to and
assigned_at. task_type kept in signature for API compatibility with
clients sending the old shape; ignored internally.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: Update seeding

**Files:**
- Modify: `stratz_scraper/web/seed.py`

- [ ] **Step 1: Replace the file**

Replace `stratz_scraper/web/seed.py` with:

```python
"""Utility helpers for seeding players."""

from __future__ import annotations

from ..database import db_connection, retryable_execute

__all__ = ["seed_players"]


def seed_players(start: int, end: int) -> None:
    with db_connection(write=True) as conn:
        cur = conn.cursor()
        for pid in range(start, end + 1):
            retryable_execute(
                cur,
                """
                INSERT INTO players (steamAccountId, depth)
                VALUES (%s, 0)
                ON CONFLICT (steamAccountId) DO NOTHING
                """,
                (pid,),
            )
```

- [ ] **Step 2: Smoke-check**

Run: `python3 -c "from stratz_scraper.web.seed import seed_players"`
Expected: exit 0.

- [ ] **Step 3: Commit**

```bash
git add stratz_scraper/web/seed.py
git commit -m "$(cat <<'EOF'
seed: drop hero_done / discover_done columns from INSERT

The peer-walking schema only carries steamAccountId + depth at seed
time; scraped_at and latest_match_id default to NULL.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: Update progress reporting

**Files:**
- Modify: `stratz_scraper/web/progress.py`

- [ ] **Step 1: Replace `fetch_progress`**

Find `fetch_progress` (lines ~28-48) and replace with:

```python
def fetch_progress() -> dict:
    with db_connection() as conn:
        row = conn.execute(
            """
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE scraped_at IS NOT NULL) AS scraped
            FROM players
            """
        ).fetchone()
        if row is None:
            return {"players_total": 0, "scraped": 0}
        total = row["total"] or 0
        scraped = row["scraped"] or 0
    return {
        "players_total": total,
        "scraped": scraped,
    }
```

- [ ] **Step 2: Update `record_progress_snapshot`**

Find the snapshot insert (lines ~91-115) and replace with:

```python
    with db_connection(write=True) as conn:
        cur = conn.cursor()
        retryable_execute(
            cur,
            """
            INSERT INTO progress_snapshots (
                captured_at,
                players_total,
                scraped
            )
            VALUES (%s, %s, %s)
            ON CONFLICT (captured_at) DO UPDATE
            SET
                players_total=EXCLUDED.players_total,
                scraped=EXCLUDED.scraped
            """,
            (
                captured_at,
                normalized["players_total"],
                normalized["scraped"],
            ),
        )
```

Update the `required_keys` constant near the top of `record_progress_snapshot` (line ~85):

```python
    required_keys = ("players_total", "scraped")
```

- [ ] **Step 3: Update `list_progress_snapshots`**

Replace the SQL string (lines ~183-189) and the row dict (lines ~195-202):

```python
    sql = (
        """
        SELECT captured_at, players_total, scraped
        FROM progress_snapshots
        """
        + where_sql
        + " ORDER BY captured_at ASC"
    )

    with db_connection() as conn:
        rows = conn.execute(sql, tuple(params)).fetchall()

    return [
        {
            "captured_at": row["captured_at"],
            "players_total": row["players_total"],
            "scraped": row["scraped"],
        }
        for row in rows
    ]
```

- [ ] **Step 4: Smoke-check**

Run: `python3 -c "from stratz_scraper.web.progress import fetch_progress, record_progress_snapshot, list_progress_snapshots, ensure_progress_snapshotter"`
Expected: exit 0.

Run: `python3 -c "from stratz_scraper import create_app; create_app()"`
Expected: exit 0.

- [ ] **Step 5: Commit**

```bash
git add stratz_scraper/web/progress.py
git commit -m "$(cat <<'EOF'
progress: rename hero_done -> scraped, drop discover_done

Single 'scraped' counter for the peer-walking model. Snapshot table
schema matches database.py's new shape.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 7: Rewrite the worker JS to use the single scrape task

**Files:**
- Modify: `stratz_scraper/web/static/js/app.js` (large rewrite — replace 5 functions, add 2 new, update work loop)

This is the largest single task. Workers will not be able to process tasks until this lands.

- [ ] **Step 1: Delete the five old GraphQL functions**

In `stratz_scraper/web/static/js/app.js`, delete these function definitions entirely:
- `fetchPlayerHeroes` (lines ~1543-1648)
- `discoverMatches` (lines ~1650-1823)
- `runDiscoveryTask` (lines ~1920-2012)
- `submitHeroStats` (lines ~1825-1873)
- `submitDiscovery` (lines ~1875-1918)

Also delete the helper `getDiscoveryTaskPlayers` (lines ~185-229) — it has no callers after the above are removed.

- [ ] **Step 2: Add `scrapePlayer` and `submitScrape`**

Insert the following two functions in the same location where the deleted functions lived (just above the `PROGRESS_REFRESH_INTERVAL` constant around line 2014):

```javascript
async function scrapePlayer(steamAccountId, token) {
  const sid = Number(steamAccountId);
  if (!Number.isFinite(sid) || sid <= 0) {
    throw new Error('Invalid steamAccountId');
  }

  const COMBINED = `
    {
      player(steamAccountId: ${sid}) {
        matches(request: { take: 1 }) { id }
        heroesPerformance(request: { take: 999999, gameModeIds: [1, 22] }, take: 200) {
          heroId matchCount winCount
        }
      }
      stratz { page { player(steamAccountId: ${sid}) {
        teammates: peers(
          request: { playerTeammateSort: WITH, matchGroupOrderBy: MATCH_COUNT, take: 1000 }
          take: 2000
        ) { steamAccountId }
        opponents: peers(
          request: { playerTeammateSort: AGAINST, matchGroupOrderBy: MATCH_COUNT, take: 1000 }
          take: 2000
        ) { steamAccountId }
      } } }
    }
  `;

  const initialPayload = await executeStratzQuery(COMBINED, {}, token);
  const initial = initialPayload?.data ?? {};
  const player = initial.player ?? {};
  const pp = initial.stratz?.page?.player ?? {};

  const heroes = (Array.isArray(player.heroesPerformance) ? player.heroesPerformance : []).map(
    (h) => ({
      heroId: h.heroId,
      games: h.matchCount,
      wins: h.winCount,
    }),
  );

  let latestMatchId = null;
  const matches = Array.isArray(player.matches) ? player.matches : [];
  if (matches.length > 0) {
    const rawId = matches[0]?.id;
    const parsed = typeof rawId === 'number' ? rawId : Number.parseInt(rawId, 10);
    if (Number.isFinite(parsed) && parsed > 0) {
      latestMatchId = Math.trunc(parsed);
    }
  }

  const peers = new Set();
  const collectPeers = (rows) => {
    if (!Array.isArray(rows)) return;
    for (const row of rows) {
      const raw = row?.steamAccountId;
      const id = typeof raw === 'number' ? raw : Number.parseInt(raw, 10);
      if (Number.isFinite(id) && id > 0 && id !== sid) {
        peers.add(Math.trunc(id));
      }
    }
  };
  collectPeers(pp.teammates);
  collectPeers(pp.opponents);

  const PAGE = 2000;
  const paginateSide = async (sort, initialRows) => {
    if (!Array.isArray(initialRows) || initialRows.length < PAGE) {
      return;
    }
    let skip = PAGE;
    while (true) {
      const PAGE_Q = `
        {
          stratz { page { player(steamAccountId: ${sid}) {
            peers(
              request: { playerTeammateSort: ${sort}, matchGroupOrderBy: MATCH_COUNT, take: 1000 }
              take: ${PAGE}
              skip: ${skip}
            ) { steamAccountId }
          } } }
        }
      `;
      const pagePayload = await executeStratzQuery(PAGE_Q, {}, token);
      const rows = pagePayload?.data?.stratz?.page?.player?.peers ?? [];
      collectPeers(rows);
      if (!Array.isArray(rows) || rows.length < PAGE) {
        return;
      }
      skip += PAGE;
    }
  };
  await paginateSide('WITH', pp.teammates);
  await paginateSide('AGAINST', pp.opponents);

  return {
    steamAccountId: sid,
    heroes,
    latestMatchId,
    discovered: Array.from(peers),
  };
}

async function submitScrape(result, depth, requestNextTask = true) {
  const payload = {
    type: 'scrape_player',
    steamAccountId: result.steamAccountId,
    heroes: result.heroes,
    discovered: result.discovered,
    latestMatchId: result.latestMatchId,
    task: Boolean(requestNextTask),
  };
  if (Number.isFinite(depth)) {
    payload.depth = depth;
  }
  const response = await fetch('/submit', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  if (!response.ok) {
    throw new Error(`Submit failed with status ${response.status}`);
  }
  const body = await response.json();
  return body?.task ?? null;
}
```

- [ ] **Step 3: Replace the work-loop task switch**

Find the `while (!token.stopRequested) {` loop in `workLoopForToken` (around line 2184). Inside the `try` block, find the three if/else branches that match `task.type === "fetch_hero_stats"`, `task.type === "discover_matches"`, and `task.type === "refresh_player_data"` (lines ~2212-2268).

Replace those three branches with one:

```javascript
      if (task.type === 'scrape_player') {
        const sid = Number(task.steamAccountId);
        if (!Number.isFinite(sid) || sid <= 0) {
          logToken(token, 'Scrape task missing steamAccountId. Resetting task.');
          await resetTask(task).catch(() => {});
          break;
        }
        logToken(token, `Scrape task for ${sid}.`);
        const result = await scrapePlayer(sid, token.activeToken);
        logToken(
          token,
          `Scraped ${result.heroes.length} heroes, ${result.discovered.length} peers from ${sid}.`,
        );
        const depthValue = Number.isFinite(task.depth) ? Math.trunc(task.depth) : null;
        nextTask = await submitScrape(result, depthValue, true);
      } else {
        logToken(
          token,
          `Received unknown task type ${task.type}. Resetting task ${taskLabel}.`,
        );
        await resetTask(task).catch(() => {});
        break;
      }
```

- [ ] **Step 4: Update `updateProgressDisplay`**

Find `updateProgressDisplay` (lines ~2022-2029) and replace with:

```javascript
function updateProgressDisplay(payload) {
  if (!payload) {
    return;
  }
  const total = payload.players_total ?? 0;
  const scraped = payload.scraped ?? 0;
  elements.progressText.textContent = `Scraped: ${scraped} / ${total}`;
}
```

- [ ] **Step 5: Smoke-check by serving the file**

The Python smoke check doesn't exercise JS. Two checks instead:

Run: `python3 -c "from stratz_scraper import create_app; create_app()"`
Expected: exit 0 (the Flask static-file serving doesn't load app.js until a browser fetches it; this just confirms backend imports still work).

Run: `node --check stratz_scraper/web/static/js/app.js`
Expected: exit 0 with no output. Confirms the JS parses without syntax errors.

If `node` is not available, run: `python3 -c "import ast; print('JS files are not Python; skipping')"` — but at minimum verify the file size dropped substantially with `wc -l stratz_scraper/web/static/js/app.js` and confirm it's well under the original 2572 lines.

- [ ] **Step 6: Commit**

```bash
git add stratz_scraper/web/static/js/app.js
git commit -m "$(cat <<'EOF'
worker: pivot to peer-walking scrape_player handler

One combined GraphQL document fetches heroes + latest match +
WITH/AGAINST peers; paginates per side via outer skip when a side hits
the 2000-row page cap. Submits union of teammates and opponents as
discovered IDs. Removes fetchPlayerHeroes / discoverMatches /
runDiscoveryTask / submitHeroStats / submitDiscovery /
getDiscoveryTaskPlayers and the three task-type branches in the work
loop. Token UI, JWT decode, multi-token rotation, backoff and
retry-after handling unchanged.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 8: Update the progress graph chart

**Files:**
- Modify: `stratz_scraper/web/static/js/progress_graph.js`

- [ ] **Step 1: Replace the dataset mapping and chart config**

Find the section that builds `heroDone` / `discoverDone` / `playersTotal` (lines ~29-45) and the `datasets:` array (lines ~62-83).

Replace the `timeSeries` mapping:

```javascript
  const timeSeries = snapshots
    .map((entry) => ({
      x: new Date(entry.captured_at).getTime(),
      scraped: entry.scraped,
      playersTotal: entry.players_total,
    }))
    .filter((entry) => Number.isFinite(entry.x))
    .sort((a, b) => a.x - b.x);

  if (timeSeries.length === 0 || typeof Chart === "undefined") {
    return;
  }

  const scraped = timeSeries.map((entry) => ({ x: entry.x, y: entry.scraped }));
  const playersTotal = timeSeries.map((entry) => ({ x: entry.x, y: entry.playersTotal }));
```

And the chart datasets:

```javascript
      datasets: [
        {
          label: "Scraped",
          data: scraped,
          borderColor: "rgba(75, 192, 192, 1)",
          backgroundColor: "rgba(75, 192, 192, 0.1)",
          tension: 0.2,
        },
        {
          label: "Players Total",
          data: playersTotal,
          borderColor: "rgba(54, 162, 235, 1)",
          backgroundColor: "rgba(54, 162, 235, 0.1)",
          tension: 0.2,
        },
      ],
```

- [ ] **Step 2: Smoke-check parse**

Run: `node --check stratz_scraper/web/static/js/progress_graph.js`
Expected: exit 0.

- [ ] **Step 3: Commit**

```bash
git add stratz_scraper/web/static/js/progress_graph.js
git commit -m "$(cat <<'EOF'
progress_graph: 2-line chart (Scraped + Players Total)

Drops the Hero Done / Discover Done series; the single 'Scraped'
counter replaces both.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 9: Rewrite README

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Replace the whole file**

Replace `README.md` with:

```markdown
# Stratz Distributed Scraper

## Overview
Coordinates browser workers that call the [Stratz GraphQL API](https://stratz.com/) to discover Dota 2 player accounts via the `peers` endpoint and aggregate hero performance. Each worker scrapes one player per task: it pulls the player's hero-performance breakdown, the latest match ID, and the full set of teammates / opponents from Stratz, then submits everything in one call to the backend. The backend runs a single-phase BFS — new accounts go into the queue at depth+1, and once every queued account is scraped the workers re-scrape the oldest entries on a LRU rotation.

## Components

### Flask Backend (`app.py`)
Serves the worker dashboard and a small JSON API. Routes:

- `GET /`: Operator dashboard. A localhost-only seeding form is shown when the request originates from a loopback address.
- `POST /task`: Returns the next `scrape_player` task. Picks the lowest-depth unscraped account first; if none exist, picks the oldest previously scraped account.
- `POST /task/reset`: Releases a task back to the queue (clears the assignment lease).
- `POST /submit`: Accepts a `scrape_player` payload: hero stats + discovered peer IDs + latest match ID + depth. Returns the next task in the same response.
- `GET /progress`: Reports `{ players_total, scraped }`.
- `GET /progress/graph`: Time-series chart of progress snapshots.
- `GET /seed?start=N&end=M`: Localhost-only bulk insert of seed accounts at depth 0.
- `GET /leaderboards`, `GET /leaderboards/<hero_slug>`, `GET /best`: Aggregated leaderboards sourced from the cached `hero_top100` table.

### Database Layer (`stratz_scraper/database.py`)
PostgreSQL via `psycopg`. `ensure_schema_exists()` creates tables and indexes on startup. Connections are pooled per-thread for writers; reads open on-demand. A default connection string of `postgresql://postgres:postgres@localhost:5432/stratz_scraper` is used when `DATABASE_URL` isn't set.

### Worker (`stratz_scraper/web/static/js/app.js`)
Polls `/task`, fires a single combined GraphQL document against `api.stratz.com/graphql` (heroes + latest match + WITH/AGAINST peers), paginates per side if a page returns exactly 2000 rows, and submits to `/submit`. The Stratz token lives only in the browser's `localStorage` and is transmitted exclusively to Stratz. Multi-token rotation, JWT decoding, exponential backoff, and Retry-After handling are all in place.

## Database Schema

| Table | Purpose | Key Columns |
|---|---|---|
| `players` | BFS queue with one bit of progress state. | `steamAccountId`, `depth`, `assigned_to`, `assigned_at`, `scraped_at`, `latest_match_id` |
| `hero_stats` | Per-(player, hero) match and win counts. | `steamAccountId`, `heroId`, `matches`, `wins` |
| `hero_top100` | Cached leaderboard (100 players per hero). Rebuilt every 5 minutes by a background thread. | `heroId`, `steamAccountId`, `matches`, `wins` |
| `meta` | Scheduler key/value (e.g. `last_assignment_cleanup`). | `key`, `value` |
| `progress_snapshots` | 5-minute samples of (`players_total`, `scraped`). | `captured_at`, `players_total`, `scraped` |

## Task Flow
1. **Startup**: Server ensures the schema exists and seeds the root account (`293053907`).
2. **Frontier scrape**: While any account has `scraped_at IS NULL`, workers receive its `steamAccountId` as a `scrape_player` task. Lowest depth first, lowest steamAccountId tie-breaker.
3. **Submission**: Worker submits heroes + discovered peer IDs + latest match ID. Backend upserts `hero_stats`, COPYs new peers into `players` at depth+1, sets `scraped_at = NOW()`.
4. **Re-scrape**: Once the frontier is empty, workers re-scrape previously scraped accounts in LRU order (oldest `scraped_at` first), discovering new peers as the player network evolves.
5. **Leaderboard**: A background thread rebuilds `hero_top100` from `hero_stats` every 5 minutes.

## Running the App
1. Create a PostgreSQL database (default: `stratz_scraper` owned by `postgres`).
2. Export `DATABASE_URL` if credentials differ.
3. `pip install -r requirements.txt`.
4. `python app.py` — listens on `0.0.0.0:80`.

Behind a proxy, forward `X-Forwarded-For` so `/seed` stays localhost-only via `is_local_request`.

## Security & Failure Handling
- **Tokens stay in the browser.** Workers transmit them only to Stratz.
- **Stale leases**: Background cleanup releases assignments older than 10 minutes every 60 seconds; up to 1000 rows per cycle.
- **Partial scrapes**: If the background processing fails after the foreground commit, `_unmark_scrape_task` nulls `scraped_at` so the player is immediately re-eligible. Successful re-scrapes are idempotent because hero_stats UPSERT is trust-newer and discovered_id INSERT-ON-CONFLICT preserves the minimum depth.
- **Backoff**: Exponential backoff in the worker with Retry-After header honoured for 429s. UI surfaces the minimum active backoff across all running tokens.
```

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "$(cat <<'EOF'
docs: rewrite README for peer-walking pivot

Updates Task Flow and Database Schema Reference to reflect the new
single-phase BFS, the scrape_player task type, and the simplified
players schema (scraped_at + latest_match_id replace five flags).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 10: Delete `reset.py`

**Files:**
- Delete: `reset.py`

- [ ] **Step 1: Confirm no importers**

Run: `grep -rn 'import reset\|from reset' --include='*.py' .`
Expected: no matches (or only matches inside `docs/` / `.git/`).

- [ ] **Step 2: Delete the file**

Run: `git rm reset.py`

- [ ] **Step 3: Smoke-check app factory still works**

Run: `python3 -c "from stratz_scraper import create_app; create_app()"`
Expected: exit 0.

- [ ] **Step 4: Commit**

```bash
git commit -m "$(cat <<'EOF'
chore: delete reset.py

The hero-phase salvage script has no use case post-pivot. The new
schema's _unmark_scrape_task handles failed scrapes inline.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Final smoke check after all tasks

- [ ] **Step 1: Full app boot**

Run: `python3 -c "from stratz_scraper import create_app; app = create_app(); print('app factory OK')"`
Expected: prints `app factory OK` and exits 0.

- [ ] **Step 2: Confirm no dead identifiers remain in source**

Run:
```bash
grep -rn 'hero_done\|discover_done\|hero_refreshed_at\|highest_match_id\|full_write_done\|fetch_hero_stats\|discover_matches\|refresh_player_data\|process_hero_submission\|process_discover_submission\|_iter_consuming_values' \
  --include='*.py' --include='*.js' stratz_scraper/ app.py
```
Expected: no matches.

- [ ] **Step 3: `git log` review**

Run: `git log --oneline -12`
Expected: 10 commits (the 10 tasks above), plus the spec commit from earlier, plus prior history.

- [ ] **Step 4: Manual smoke run (optional but recommended)**

```bash
# Drop and recreate the DB (assumes psql access)
dropdb --if-exists stratz_scraper && createdb stratz_scraper
# Start the server
python3 app.py &
SERVER_PID=$!
sleep 2
# Poll for a task — should return scrape_player for 293053907
curl -sf -XPOST http://localhost:80/task -H 'content-type: application/json' -d '{}' | python3 -m json.tool
# Tear down
kill $SERVER_PID
```

Expected `/task` response: `{ "task": { "type": "scrape_player", "steamAccountId": 293053907, "depth": 0, "latestMatchId": null } }`.

---

## Self-Review (filled in during planning)

**Spec coverage:** Every spec section maps to at least one task:
- Schema (database.py) → Task 1
- Submissions (submissions.py + /submit) → Task 2
- Assignment (assignment.py) → Task 3
- Tasks reset (tasks.py) → Task 4
- Seed (seed.py) → Task 5
- Progress (progress.py) → Task 6
- Worker JS (app.js) → Task 7
- Progress graph JS (progress_graph.js) → Task 8
- README → Task 9
- reset.py deletion → Task 10

**Placeholder scan:** No TBDs. The "(optional but recommended)" manual smoke run isn't a placeholder — it's a clearly-marked optional step.

**Type consistency:**
- `process_scrape_submission(steam_account_id, heroes_payload, discovered_ids, latest_match_id, next_depth)` — same signature used in submissions.py and called from web/app.py via `submit_scrape_submission`.
- Task payload shape `{type, steamAccountId, depth, latestMatchId}` — consistent between assignment.py emit, worker JS consume, and submit response.
- Submit body shape `{type, steamAccountId, heroes, discovered, latestMatchId, depth, task}` — consistent between worker JS produce and /submit consume.
- `_unmark_scrape_task` zeros `scraped_at` (per design risk #4); the foreground UPDATE in /submit sets it. Consistent.
- Heroes payload `{heroId, games, wins}` from the worker; the backend's `_extract_hero_rows` accepts both `games` and `matches` keys. Consistent.

**Scope check:** Focused on the peer-walking pivot. Doesn't bundle unrelated refactors.
