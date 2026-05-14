# Stratz-Scraper Efficiency Pass Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Resolve all 13 inefficiencies identified in the November 2026 audit of `Nitjsefnie/Stratz-Scraper` without adding tests or changing the wire protocol.

**Architecture:** Move expensive leaderboard maintenance out of the submission hot path into a periodic background refresher; replace per-row `executemany` discovery inserts with `COPY` via a temp table; collapse three serial assignment queries into one; delete dead reads/writes; tighten cleanup. Spec lives at `docs/superpowers/specs/2026-05-14-stratz-scraper-efficiency-pass-design.md`.

**Tech Stack:** Python 3, Flask, `psycopg` (binary), PostgreSQL.

**Verification mode (per design):** Code review of the diff only. No new tests. Each task ends with `python -c "import stratz_scraper; from stratz_scraper import create_app; create_app()"` as a smoke check to confirm the module imports and the app factory still runs without raising. Then commit.

---

## Task Ordering Notes

- Tier 1 → Tier 2 → Tier 3, but within Tier 2 the assignment-query rewrite (Task 8) is sequenced LAST in that tier because it subsumes the hero-cursor deletion and shares files with the smaller Tier 2 tasks.
- All tasks are independently committable. If you stop after any task the codebase is still in a working state.
- The `hero_assignment_cursor` and `task_assignment_counter` orphan-row deletion from `meta` happens once, inside Task 8's schema migration step.

---

## Task 1: Strip leaderboard maintenance from `process_hero_submission`

**Files:**
- Modify: `stratz_scraper/web/submissions.py:200-372`

- [ ] **Step 1: Open `stratz_scraper/web/submissions.py` and replace the body of `process_hero_submission`**

The new function body keeps the `hero_stats` upsert and deletes everything from the `if hero_ids:` block onward. After this task the function looks like:

```python
def process_hero_submission(
    steam_account_id: int,
    heroes_payload: Iterable[dict] | None,
) -> None:
    hero_stats_rows, _hero_ids = _extract_hero_rows(steam_account_id, heroes_payload)
    if not hero_stats_rows:
        return
    try:
        with db_connection(write=True) as conn:
            cur = conn.cursor()
            retryable_executemany(
                cur,
                """
                INSERT INTO hero_stats (steamAccountId, heroId, matches, wins)
                VALUES (%s,%s,%s,%s)
                ON CONFLICT(steamAccountId, heroId) DO UPDATE SET
                    matches = CASE
                        WHEN excluded.matches > hero_stats.matches
                        THEN excluded.matches
                        ELSE hero_stats.matches
                    END,
                    wins = CASE
                        WHEN excluded.matches > hero_stats.matches
                        THEN excluded.wins
                        ELSE hero_stats.wins
                    END
                """,
                hero_stats_rows,
            )
    except Exception:
        import traceback
        print(
            f"[submit-background] failed to process hero stats for {steam_account_id}",
            flush=True,
        )
        traceback.print_exc()
        _unmark_hero_task(steam_account_id)
```

> Note: the monotonic `CASE WHEN` clause is intentionally kept here. Task 12 (T3.4) replaces it with trust-newer; keeping it here means each commit is minimal and reviewable.

The unused `_extract_hero_rows` return value `hero_ids` is renamed `_hero_ids` so a future linter pass doesn't flag it; if your linter accepts a bare `_`, that works too.

- [ ] **Step 2: Smoke-check imports**

Run: `python -c "from stratz_scraper.web.submissions import process_hero_submission"`
Expected: no output, exit 0.

- [ ] **Step 3: Smoke-check app factory**

Run: `python -c "from stratz_scraper import create_app; create_app()"`
Expected: no output (or just any logger lines), exit 0. The factory must still start.

- [ ] **Step 4: Commit**

```bash
git add stratz_scraper/web/submissions.py
git commit -m "perf(submissions): drop inline hero_top100 maintenance"
```

---

## Task 2: Add periodic leaderboard refresher and wire it into `create_app`

**Files:**
- Create: `stratz_scraper/web/leaderboard_refresh.py`
- Modify: `stratz_scraper/web/app.py:1-50`

- [ ] **Step 1: Create `stratz_scraper/web/leaderboard_refresh.py`**

```python
"""Background thread that periodically rebuilds the hero_top100 cache."""

from __future__ import annotations

import logging
import threading
from datetime import timedelta

from ..database import refresh_leaderboard_views

LEADERBOARD_REFRESH_INTERVAL = timedelta(minutes=5)

_LOGGER = logging.getLogger(__name__)
_refresher_thread: threading.Thread | None = None
_refresher_stop_event: threading.Event | None = None
_refresher_lock = threading.Lock()

__all__ = [
    "LEADERBOARD_REFRESH_INTERVAL",
    "ensure_leaderboard_refresher",
]


def _refresher_worker(stop_event: threading.Event) -> None:
    interval_seconds = max(int(LEADERBOARD_REFRESH_INTERVAL.total_seconds()), 1)
    while not stop_event.is_set():
        try:
            refresh_leaderboard_views()
        except Exception:  # pragma: no cover - best effort logging
            _LOGGER.exception("Leaderboard refresher failed")
        stop_event.wait(interval_seconds)


def ensure_leaderboard_refresher() -> None:
    """Start the background worker that rebuilds hero_top100 periodically."""

    global _refresher_thread, _refresher_stop_event
    with _refresher_lock:
        if _refresher_thread and _refresher_thread.is_alive():
            return
        stop_event = threading.Event()
        thread = threading.Thread(
            target=_refresher_worker,
            args=(stop_event,),
            name="leaderboard-refresher",
            daemon=True,
        )
        thread.start()
        _refresher_thread = thread
        _refresher_stop_event = stop_event
```

- [ ] **Step 2: Wire it into `stratz_scraper/web/app.py`**

Add to imports (next to other `from .X import Y` lines):

```python
from .leaderboard_refresh import ensure_leaderboard_refresher
```

In `create_app()`, add the call next to the existing two scheduler starts. Replace this block:

```python
    release_incomplete_assignments()
    ensure_assignment_cleanup_scheduler()
    ensure_progress_snapshotter()
```

with:

```python
    release_incomplete_assignments()
    ensure_assignment_cleanup_scheduler()
    ensure_progress_snapshotter()
    ensure_leaderboard_refresher()
```

- [ ] **Step 3: Smoke-check app factory**

Run: `python -c "from stratz_scraper import create_app; create_app()"`
Expected: exit 0. The new daemon thread starts in the background; that's fine.

- [ ] **Step 4: Commit**

```bash
git add stratz_scraper/web/leaderboard_refresh.py stratz_scraper/web/app.py
git commit -m "perf(leaderboard): rebuild hero_top100 on a 5-minute timer"
```

---

## Task 3: Replace discovery `executemany` with `COPY` via temp table

**Files:**
- Modify: `stratz_scraper/web/submissions.py` (the discovery section, lines roughly 375–443 plus the helper iterators above)

- [ ] **Step 1: Replace `process_discover_submission` and trim now-unused helpers**

Delete the constants and helpers `_DISCOVERY_BATCH_SIZE`, `_iter_consuming_values`, `_iter_discovered_child_rows`. Keep `_iter_discovered_candidate_ids` (still used). Then rewrite `process_discover_submission`:

```python
def process_discover_submission(
    steam_account_id: int,
    discovered_payload: Iterable[object] | None,
    provided_next_depth: int | None,
    provided_depth: int | None,
    assignment_depth: int | None,
) -> None:
    parsed_next_depth = _coerce_optional_int(provided_next_depth)
    parsed_depth = _coerce_optional_int(provided_depth)
    parsed_assignment_depth = _coerce_optional_int(assignment_depth)
    next_depth_value = _resolve_next_depth(
        parsed_next_depth,
        parsed_depth,
        parsed_assignment_depth,
    )

    deduped: list[int] = []
    seen: set[int] = set()
    for candidate_id in _iter_discovered_candidate_ids(discovered_payload):
        if candidate_id == steam_account_id or candidate_id in seen:
            continue
        seen.add(candidate_id)
        deduped.append(candidate_id)

    try:
        with db_connection(write=True) as conn:
            if deduped:
                _copy_discovered_rows(conn, deduped, next_depth_value)
            with conn.cursor() as cur:
                retryable_execute(
                    cur,
                    """
                    UPDATE players
                    SET full_write_done=TRUE
                    WHERE steamAccountId=%s
                    """,
                    (steam_account_id,),
                )
                retryable_execute(
                    cur,
                    """
                    UPDATE meta
                    SET value = '-1'
                    WHERE key = 'hero_assignment_cursor'
                    """,
                )
    except Exception:
        import traceback
        print(
            f"[submit-background] failed to process discovery for {steam_account_id}",
            flush=True,
        )
        traceback.print_exc()
        _unmark_discover_task(steam_account_id)
```

> The `UPDATE meta SET value = '-1' WHERE key = 'hero_assignment_cursor'` line is kept here intentionally; Task 8 deletes it as part of removing the hero cursor entirely. Keeping it in this commit means this task is a pure swap of the insert mechanism — no behaviour change beyond the COPY win.

- [ ] **Step 2: Add the `_copy_discovered_rows` helper**

Add this above `process_discover_submission`:

```python
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
                    INSERT INTO players (
                        steamAccountId, depth, hero_done, discover_done
                    )
                    SELECT steamAccountId, depth, FALSE, FALSE
                    FROM discovered_tmp
                    ON CONFLICT (steamAccountId) DO UPDATE
                    SET depth = excluded.depth,
                        highest_match_id = NULL,
                        discover_done = FALSE
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
            # Reacquire advisory lock to match prior retry semantic.
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT pg_advisory_xact_lock(%s)",
                    (_DISCOVERY_SUBMISSION_LOCK_ID,),
                )
            time.sleep(0.5)
```

Add these imports at the top of `submissions.py` if not already present:

```python
import time
from psycopg import errors

_RETRYABLE_ERRORS = (
    errors.DeadlockDetected,
    errors.SerializationFailure,
    errors.LockNotAvailable,
)
```

(Or — preferred — import `_RETRYABLE_ERRORS` from `..database` by adding it to `database.py`'s `__all__` and importing here. Up to the implementer; either works as long as the tuple is identical to the one in `database.py`.)

- [ ] **Step 3: Smoke-check**

Run: `python -c "from stratz_scraper.web.submissions import process_discover_submission, _copy_discovered_rows"`
Expected: exit 0.

Run: `python -c "from stratz_scraper import create_app; create_app()"`
Expected: exit 0.

- [ ] **Step 4: Commit**

```bash
git add stratz_scraper/web/submissions.py
git commit -m "perf(discovery): use COPY + temp table instead of executemany"
```

---

## Task 4: Drop the unused `task_assignment_counter` read+write

**Files:**
- Modify: `stratz_scraper/web/assignment.py:355-482`

- [ ] **Step 1: Remove the dead counter read**

In `_assign_next_task_on_connection`, delete this block (lines ~360-367):

```python
        counter_row = cur.execute(
            "SELECT value FROM meta WHERE key=%s",
            ("task_assignment_counter",),
        ).fetchone()
        try:
            current_count = int(counter_row["value"]) if counter_row else 0
        except (TypeError, ValueError):
            current_count = 0
```

- [ ] **Step 2: Remove the dead counter increment**

Replace the trailing block in `_assign_next_task_on_connection`:

```python
        if candidate_payload and candidate_payload is not _DISCOVERY_THROTTLED:
            _increment_assignment_counter(cur)
            return candidate_payload

    return None
```

with:

```python
        if candidate_payload and candidate_payload is not _DISCOVERY_THROTTLED:
            return candidate_payload

    return None
```

- [ ] **Step 3: Delete `_increment_assignment_counter`**

Delete the entire function definition (lines ~465-482).

- [ ] **Step 4: Smoke-check**

Run: `python -c "from stratz_scraper.web.assignment import assign_next_task"`
Expected: exit 0.

Run: `python -c "from stratz_scraper import create_app; create_app()"`
Expected: exit 0.

- [ ] **Step 5: Commit**

```bash
git add stratz_scraper/web/assignment.py
git commit -m "perf(assignment): drop unused task_assignment_counter read/write"
```

---

## Task 5: `EXISTS` short-circuit for discovery backlog probe

**Files:**
- Modify: `stratz_scraper/web/assignment.py:114-132`

- [ ] **Step 1: Replace `_discovery_backlog_exceeded`**

Replace the existing function with:

```python
def _discovery_backlog_exceeded(cur) -> bool:
    backlog_row = retryable_execute(
        cur,
        """
        SELECT EXISTS (
            SELECT 1
            FROM players
            WHERE discover_done=TRUE
              AND full_write_done=FALSE
              AND highest_match_id IS NOT NULL
            OFFSET 100 LIMIT 1
        ) AS backlog_exceeded
        """,
        retry_interval=ASSIGNMENT_RETRY_INTERVAL,
    ).fetchone()

    if backlog_row is None:
        return False
    return bool(row_value(backlog_row, "backlog_exceeded"))
```

- [ ] **Step 2: Smoke-check**

Run: `python -c "from stratz_scraper.web.assignment import _discovery_backlog_exceeded"`
Expected: exit 0.

- [ ] **Step 3: Commit**

```bash
git add stratz_scraper/web/assignment.py
git commit -m "perf(assignment): short-circuit backlog probe with EXISTS"
```

---

## Task 6: Drop the `SELECT 1` ping on cached write connections

**Files:**
- Modify: `stratz_scraper/database.py:36-43, 102-158`

- [ ] **Step 1: Add `OperationalError` to retryable set**

Replace the `_RETRYABLE_ERRORS` tuple at line ~38:

```python
_RETRYABLE_ERRORS: tuple[type[BaseException], ...] = (
    errors.DeadlockDetected,
    errors.SerializationFailure,
    errors.LockNotAvailable,
    errors.OperationalError,
)
```

- [ ] **Step 2: Remove the connection-health ping**

In `db_connection`, delete this block (lines ~118-128):

```python
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
```

and replace with:

```python
            connection = cache.get("write")
```

- [ ] **Step 3: Make `retryable_execute` reconnect cached writes on OperationalError**

Replace `retryable_execute` (lines ~175-192) with:

```python
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
```

> The `OperationalError` branch closes the cached connection and re-raises so the caller's `with db_connection(write=True)` opens a fresh one on the next try. This preserves the prior "broken-connection auto-recovers" behavior without the per-call `SELECT 1`.

- [ ] **Step 4: Smoke-check**

Run: `python -c "from stratz_scraper.database import db_connection, retryable_execute"`
Expected: exit 0.

Run: `python -c "from stratz_scraper import create_app; create_app()"`
Expected: exit 0.

- [ ] **Step 5: Commit**

```bash
git add stratz_scraper/database.py
git commit -m "perf(db): drop SELECT 1 ping, recover via OperationalError retry"
```

---

## Task 7: Bound `release_incomplete_assignments` per cycle

**Files:**
- Modify: `stratz_scraper/database.py:481-510`

- [ ] **Step 1: Replace the unbounded UPDATE**

Replace the SQL inside `release_incomplete_assignments` (lines ~492-505):

```python
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
```

- [ ] **Step 2: Smoke-check**

Run: `python -c "from stratz_scraper.database import release_incomplete_assignments"`
Expected: exit 0.

- [ ] **Step 3: Commit**

```bash
git add stratz_scraper/database.py
git commit -m "perf(db): cap release_incomplete_assignments at 1000 rows/cycle"
```

---

## Task 8: Replace assignment scheduler with one combined query and drop the hero cursor

**Files:**
- Modify: `stratz_scraper/web/assignment.py` (most of the file)
- Modify: `stratz_scraper/web/submissions.py` (remove cursor reset in `process_discover_submission`)
- Modify: `stratz_scraper/database.py` (add orphan-meta-key cleanup to `ensure_schema`)

This is the biggest single change. It deletes `_assign_next_hero`, `_assign_discovery`, and the inline refresh block in `_assign_next_task_on_connection`, replacing them with one `WITH ... UNION ALL ... UPDATE ... RETURNING` query.

- [ ] **Step 1: Add orphan-key cleanup to `ensure_schema`**

In `stratz_scraper/database.py`, at the end of the `cur.execute(...)` block inside `ensure_schema` (after the `INSERT INTO players (steamAccountId, depth) VALUES (%s, 0) ON CONFLICT ...`), add:

```python
            cur.execute(
                """
                DELETE FROM meta
                WHERE key IN ('hero_assignment_cursor', 'task_assignment_counter')
                """
            )
```

- [ ] **Step 2: Replace `_assign_next_task_on_connection` body and delete the now-unused helpers**

In `stratz_scraper/web/assignment.py`, delete:
- `_assign_next_hero` (entire function)
- `_assign_discovery` (entire function)
- The constant `HERO_ASSIGNMENT_CURSOR_KEY`

Replace `_assign_next_task_on_connection` with:

```python
def _assign_next_task_on_connection(connection, *, run_cleanup: bool) -> dict | None:
    if run_cleanup:
        maybe_run_assignment_cleanup(connection)

    with connection.cursor() as cur:
        discovery_throttled = _discovery_backlog_exceeded(cur)
        discovery_limit = 0 if discovery_throttled else MAX_DISCOVERY_TASK_SIZE

        assigned_rows = retryable_execute(
            cur,
            """
            WITH hero_candidates AS (
                SELECT steamAccountId, depth, highest_match_id,
                       'hero'::text AS kind
                FROM players
                WHERE hero_done=FALSE
                  AND assigned_to IS NULL
                  AND steamAccountId > 0
                ORDER BY steamAccountId ASC
                LIMIT %s
                FOR UPDATE SKIP LOCKED
            ),
            discovery_candidates AS (
                SELECT steamAccountId, depth, highest_match_id,
                       'discover'::text AS kind
                FROM players
                WHERE hero_done=TRUE
                  AND discover_done=FALSE
                  AND assigned_to IS NULL
                  AND steamAccountId > 0
                ORDER BY depth ASC, steamAccountId ASC
                LIMIT %s
                FOR UPDATE SKIP LOCKED
            ),
            refresh_candidates AS (
                SELECT steamAccountId, depth, highest_match_id,
                       'refresh'::text AS kind
                FROM players
                WHERE hero_done=TRUE
                  AND discover_done=TRUE
                  AND assigned_to IS NULL
                  AND steamAccountId > 0
                ORDER BY hero_refreshed_at ASC NULLS FIRST, steamAccountId ASC
                LIMIT %s
                FOR UPDATE SKIP LOCKED
            ),
            chosen AS (
                SELECT * FROM hero_candidates
                UNION ALL
                SELECT * FROM discovery_candidates
                WHERE NOT EXISTS (SELECT 1 FROM hero_candidates)
                UNION ALL
                SELECT * FROM refresh_candidates
                WHERE NOT EXISTS (SELECT 1 FROM hero_candidates)
                  AND NOT EXISTS (SELECT 1 FROM discovery_candidates)
            )
            UPDATE players
            SET assigned_to = chosen.kind,
                assigned_at = CURRENT_TIMESTAMP,
                hero_done = CASE
                    WHEN chosen.kind='refresh' THEN FALSE
                    ELSE players.hero_done
                END
            FROM chosen
            WHERE players.steamAccountId = chosen.steamAccountId
            RETURNING players.steamAccountId, players.depth,
                      players.highest_match_id, chosen.kind
            """,
            (
                MAX_HERO_TASK_SIZE,
                discovery_limit,
                MAX_HERO_TASK_SIZE,
            ),
            retry_interval=ASSIGNMENT_RETRY_INTERVAL,
        ).fetchall()

        if not assigned_rows:
            return None

        kind = str(row_value(assigned_rows[0], "kind"))
        players: list[dict] = []
        for assigned in assigned_rows:
            try:
                steam_account_id = int(row_value(assigned, "steamAccountId"))
            except (TypeError, ValueError):
                continue
            if steam_account_id <= 0:
                continue
            depth_value = row_value(assigned, "depth")
            try:
                depth = int(depth_value) if depth_value is not None else None
            except (TypeError, ValueError):
                depth = None
            highest_value = row_value(assigned, "highest_match_id")
            try:
                highest = int(highest_value) if highest_value is not None else None
            except (TypeError, ValueError):
                highest = None
            if highest is not None and highest < 0:
                highest = None
            players.append(
                {
                    "steamAccountId": steam_account_id,
                    "depth": depth,
                    "highestMatchId": highest,
                }
            )

        if not players:
            return None

        if kind == "hero":
            ids = sorted({p["steamAccountId"] for p in players})
            return {
                "type": "fetch_hero_stats",
                "steamAccountId": ids[0],
                "steamAccountIds": ids,
            }

        players.sort(key=lambda e: (e.get("depth") or 0, e["steamAccountId"]))
        ids = [p["steamAccountId"] for p in players]
        payload_type = (
            "discover_matches" if kind == "discover" else "refresh_player_data"
        )
        payload: dict = {
            "type": payload_type,
            "steamAccountId": ids[0],
            "steamAccountIds": ids,
            "players": players,
        }
        first_depth = players[0].get("depth")
        if first_depth is not None:
            payload["depth"] = first_depth
        first_highest = players[0].get("highestMatchId")
        if first_highest is not None:
            payload["highestMatchId"] = first_highest
        return payload
```

- [ ] **Step 3: Trim `__all__` and unused imports in `assignment.py`**

Remove `_restart_discovery_cycle` if it's no longer referenced (search the codebase: `grep -rn _restart_discovery_cycle stratz_scraper/`). If unused, delete the function and the `_restart_executor` / `_RESTART_LOCK_ID` constants.

If `_restart_discovery_cycle` is still used elsewhere, keep it.

- [ ] **Step 4: Remove cursor reset from `process_discover_submission`**

In `stratz_scraper/web/submissions.py`, delete this block from `process_discover_submission`:

```python
                retryable_execute(
                    cur,
                    """
                    UPDATE meta
                    SET value = '-1'
                    WHERE key = 'hero_assignment_cursor'
                    """,
                )
```

- [ ] **Step 5: Smoke-check**

Run: `grep -rn 'hero_assignment_cursor\|task_assignment_counter\|HERO_ASSIGNMENT_CURSOR_KEY\|_increment_assignment_counter\|_assign_next_hero\|_assign_discovery' stratz_scraper/`
Expected: no matches (only matches inside docs/ or the schema migration in `database.py` are allowed).

Run: `python -c "from stratz_scraper import create_app; create_app()"`
Expected: exit 0.

- [ ] **Step 6: Commit**

```bash
git add stratz_scraper/web/assignment.py stratz_scraper/web/submissions.py stratz_scraper/database.py
git commit -m "perf(assignment): collapse hero+discovery+refresh into one query"
```

---

## Task 9: Production-mode `app.py`

**Files:**
- Modify: `app.py`

- [ ] **Step 1: Read the current entry point**

Run: `cat app.py`
Expected output:

```python
from stratz_scraper import create_app
app = create_app()
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=80, debug=True, threaded=True)
```

- [ ] **Step 2: Replace `debug=True` with `debug=False`**

The file becomes:

```python
from stratz_scraper import create_app
app = create_app()
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=80, debug=False, threaded=True)
```

- [ ] **Step 3: Commit**

```bash
git add app.py
git commit -m "chore(app): run without debug mode by default"
```

---

## Task 10: `reset.py` reads credentials from environment

**Files:**
- Modify: `reset.py`

- [ ] **Step 1: Inspect `reset.py` to confirm structure**

Run: `wc -l reset.py && head -30 reset.py`
Expected: ~94 lines, and the first ~30 lines include `psycopg.connect(...)` with hardcoded credentials.

- [ ] **Step 2: Replace the hardcoded connect call**

Find the `psycopg.connect(...)` call and replace it with:

```python
from stratz_scraper.database import connect_pg

conn = connect_pg(autocommit=False)
```

Remove the local `import psycopg` and the hardcoded `dbname=... user=... password=NewStr0ngPass host=localhost` string. Keep the rest of the batching/iteration logic.

If `reset.py` uses `psycopg`-specific attributes that `connect_pg` doesn't expose, leave the `import psycopg` but only use it for type/exception references — don't reintroduce credential strings.

- [ ] **Step 3: Smoke-check imports**

Run: `python -c "import reset"`
Expected: exit 0. Do NOT run the script — it touches the live database.

- [ ] **Step 4: Commit**

```bash
git add reset.py
git commit -m "chore(reset): read credentials from environment via connect_pg"
```

---

## Task 11: Delete `locking.py` and the `concurrently` parameter

**Files:**
- Delete: `stratz_scraper/locking.py`
- Modify: `stratz_scraper/database.py:440-478`

- [ ] **Step 1: Confirm `locking.py` has no importers**

Run: `grep -rn 'from .locking\|from stratz_scraper.locking\|import locking' stratz_scraper/ app.py reset.py`
Expected: no matches.

- [ ] **Step 2: Delete the file**

Run: `rm stratz_scraper/locking.py`

- [ ] **Step 3: Strip `concurrently` from `refresh_leaderboard_views`**

In `stratz_scraper/database.py`, the current signature:

```python
def refresh_leaderboard_views(*, concurrently: bool = True) -> None:
    """Rebuild the cached hero leaderboard table."""

    # ``concurrently`` is kept for API compatibility. The rebuild always runs in
    # a single transaction so the flag is ignored.
    del concurrently
```

becomes:

```python
def refresh_leaderboard_views() -> None:
    """Rebuild the cached hero leaderboard table."""
```

Check the rest of the function body stays untouched.

- [ ] **Step 4: Smoke-check no remaining `concurrently=` callers**

Run: `grep -rn 'refresh_leaderboard_views(concurrently' stratz_scraper/`
Expected: no matches.

Run: `python -c "from stratz_scraper.database import refresh_leaderboard_views"`
Expected: exit 0.

- [ ] **Step 5: Commit**

```bash
git add stratz_scraper/locking.py stratz_scraper/database.py
git commit -m "chore: delete dead FileLock module and concurrently param"
```

---

## Task 12: Trust-newer hero stats UPSERT

**Files:**
- Modify: `stratz_scraper/web/submissions.py:208-227`

- [ ] **Step 1: Replace the monotonic UPSERT clause**

In `process_hero_submission`, replace:

```python
            retryable_executemany(
                cur,
                """
                INSERT INTO hero_stats (steamAccountId, heroId, matches, wins)
                VALUES (%s,%s,%s,%s)
                ON CONFLICT(steamAccountId, heroId) DO UPDATE SET
                    matches = CASE
                        WHEN excluded.matches > hero_stats.matches
                        THEN excluded.matches
                        ELSE hero_stats.matches
                    END,
                    wins = CASE
                        WHEN excluded.matches > hero_stats.matches
                        THEN excluded.wins
                        ELSE hero_stats.wins
                    END
                """,
                hero_stats_rows,
            )
```

with:

```python
            retryable_executemany(
                cur,
                """
                INSERT INTO hero_stats (steamAccountId, heroId, matches, wins)
                VALUES (%s,%s,%s,%s)
                ON CONFLICT(steamAccountId, heroId) DO UPDATE SET
                    matches = excluded.matches,
                    wins = excluded.wins
                """,
                hero_stats_rows,
            )
```

- [ ] **Step 2: Smoke-check**

Run: `python -c "from stratz_scraper.web.submissions import process_hero_submission"`
Expected: exit 0.

- [ ] **Step 3: Commit**

```bash
git add stratz_scraper/web/submissions.py
git commit -m "fix(submissions): trust newer hero stats so refresh task overwrites"
```

---

## Final smoke check after all tasks

- [ ] **Step 1: Full app boot**

Run: `python -c "from stratz_scraper import create_app; app = create_app(); print('app factory OK')"`
Expected: prints `app factory OK` and exits 0.

- [ ] **Step 2: Confirm no leftover dead identifiers**

Run:
```bash
grep -rn 'hero_assignment_cursor\|task_assignment_counter\|HERO_ASSIGNMENT_CURSOR_KEY\|_increment_assignment_counter\|FileLock\|concurrently=True\|debug=True' stratz_scraper/ app.py reset.py
```
Expected: no matches (matches inside `docs/` are fine).

- [ ] **Step 3: `git log` review**

Run: `git log --oneline master..HEAD` (or `git log --oneline -15`)
Expected: 12 commits in order, each scoped to one task.

---

## Self-Review (filled in during planning)

**Spec coverage:** Every spec item (T1.1 → T3.4) has at least one task:
- T1.1 → Tasks 1 + 2
- T1.2 → Task 3
- T1.3 → folded into Task 8
- T2.1 → Task 4
- T2.2 → Task 5
- T2.3 → Task 6
- T2.4 → Task 8
- T2.5 → Task 7
- T3.1 → Task 9
- T3.2 → Task 10
- T3.3 → Task 11
- T3.4 → Task 12

**Placeholder scan:** No TBDs. The "if linter accepts a bare `_`" line in Task 1 is implementer flexibility, not a placeholder.

**Type consistency:** All identifier references (`row_value`, `retryable_execute`, `retryable_executemany`, `_RETRYABLE_ERRORS`, `MAX_HERO_TASK_SIZE`, `MAX_DISCOVERY_TASK_SIZE`, `ASSIGNMENT_RETRY_INTERVAL`, `_DISCOVERY_SUBMISSION_LOCK_ID`) match what's in the current source.
