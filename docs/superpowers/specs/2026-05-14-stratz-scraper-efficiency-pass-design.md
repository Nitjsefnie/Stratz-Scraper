# Stratz-Scraper Efficiency Pass — Design

**Date:** 2026-05-14
**Status:** Approved (brainstorming complete)
**Scope decisions:** All 13 audit items, tiered. Code-review-only verification (no
benchmark harness, no unit tests added). Single-thread executor stays;
leaderboard maintenance moves out of the hot path.

## Goal

Resolve the 13 inefficiencies identified in the November 2026 efficiency audit
of `Nitjsefnie/Stratz-Scraper`. Work is grouped into three independently
landable tiers so any prefix of the work is a meaningful improvement.

## Architecture changes (summary)

- **Submission hot path becomes cheap.** Hero submissions stop maintaining the
  `hero_top100` leaderboard inline. They do a single `executemany` upsert into
  `hero_stats` and return.
- **Leaderboard refreshes periodically.** A new background thread (alongside
  `assignment-cleanup` and `progress-snapshotter`) calls
  `refresh_leaderboard_views()` every 5 minutes.
- **Discovery inserts use `COPY` via a temp table.** Replaces 50-row
  `executemany` batches.
- **Assignment scheduling collapses.** Three serial query plans (hero, discovery,
  refresh) become one `WITH ... UNION ALL ...` plan. Hero cursor is deleted.
- **Background executor stays at `max_workers=1`.** Since the work it does is
  now small, one worker is sufficient and the per-account ordering guarantees
  are preserved.

## Tier 1 — Major

### T1.1 — Lazy leaderboard + cheap submissions

**Audit items:** #1 (single-thread executor bottleneck) + #3 (per-hero N+5
queries in submission path).

**Change:**
- In `stratz_scraper/web/submissions.py::process_hero_submission`, delete
  lines 228–363 (all the `hero_top100` maintenance after the `hero_stats`
  upsert). The function ends after the `executemany` for `hero_stats`.
- Add a new module-level scheduler `ensure_leaderboard_refresher()` (place it
  in a new file `stratz_scraper/web/leaderboard_refresh.py` to keep
  `submissions.py` focused). Follows the same pattern as
  `ensure_assignment_cleanup_scheduler` and `ensure_progress_snapshotter`:
  daemon thread, stop-event, `time.sleep` between runs.
- Default interval: 5 minutes (`LEADERBOARD_REFRESH_INTERVAL =
  timedelta(minutes=5)`). Matches the existing progress-snapshot cadence.
- `create_app()` in `stratz_scraper/web/app.py` adds a call to
  `ensure_leaderboard_refresher()` next to the existing two scheduler calls.

**Behavior change visible to users:** Leaderboard entries may lag real
submissions by up to 5 minutes. Acceptable — the leaderboard is not a
real-time view.

### T1.2 — `COPY` for discovery inserts

**Audit item:** #2.

**Change:**
- In `process_discover_submission`, replace the `_iter_discovered_child_rows`
  + `retryable_executemany` loop with:
  1. Drain all candidate IDs into a deduplicated list.
  2. `CREATE TEMP TABLE discovered_tmp (steamAccountId BIGINT, depth INTEGER)
     ON COMMIT DROP`.
  3. `cursor.copy("COPY discovered_tmp (steamAccountId, depth) FROM STDIN")`
     and write rows.
  4. `INSERT INTO players (steamAccountId, depth, hero_done, discover_done)
     SELECT steamAccountId, depth, FALSE, FALSE FROM discovered_tmp
     ON CONFLICT (steamAccountId) DO UPDATE SET depth = excluded.depth,
       highest_match_id = NULL, discover_done = FALSE
     WHERE excluded.depth < players.depth`.
- Keep the existing advisory-lock reacquire path (the `_DISCOVERY_SUBMISSION_LOCK_ID`
  pattern) for the temp-table-driven upsert, via a new `retryable_block`
  helper or inlined retry loop. The retry semantic must hold across both the
  `COPY` and the subsequent `INSERT`.
- Delete `_iter_discovered_child_rows`, `_iter_consuming_values`,
  `_DISCOVERY_BATCH_SIZE`. Keep `_iter_discovered_candidate_ids` (still used
  to dedup before the copy).

### T1.3 — Drop the hero cursor

**Audit item:** #4.

**Change:**
- In `assignment.py::_assign_next_hero`:
  - Delete the `last_cursor_row` SELECT (lines 247–256).
  - Delete the `candidate` CTE branch — keep only what was the `fallback`
    CTE, renamed to the sole `candidate`. The cursor was supposed to make
    `candidate` faster but the `-1` reset on every discovery defeated it,
    so we just always use what was the fallback path.
  - Delete the `meta` insert that persists `HERO_ASSIGNMENT_CURSOR_KEY`
    (lines 315–324) and the constant `HERO_ASSIGNMENT_CURSOR_KEY`.
  - The `for _ in range(2)` retry loop becomes unnecessary (it existed
    because the cursor sometimes returned no rows). Replace with a single
    attempt.
- In `submissions.py::process_discover_submission`, delete the
  `UPDATE meta SET value = '-1' WHERE key = 'hero_assignment_cursor'`
  (lines 427–434).
- Add a one-time startup migration in `ensure_schema`:
  `DELETE FROM meta WHERE key IN ('hero_assignment_cursor',
  'task_assignment_counter')`. This drops the orphaned rows.

## Tier 2 — Notable

### T2.1 — Delete unused counter read AND write

**Audit item:** #5.

**Change:** The `task_assignment_counter` meta key is neither read meaningfully
nor consumed externally — both the read and the write are dead.
- In `assignment.py::_assign_next_task_on_connection`, delete lines 360–367
  (the `counter_row = cur.execute(...)` SELECT and the `current_count`
  parsing).
- Delete the call to `_increment_assignment_counter(cur)` at line 459 and
  flatten the surrounding `if candidate_payload and candidate_payload is not
  _DISCOVERY_THROTTLED: ... return candidate_payload` to just
  `return candidate_payload`.
- Delete the function `_increment_assignment_counter` itself
  (lines 465–482).
- The orphan-key deletion in T1.3's migration already drops the row from
  `meta`.

### T2.2 — `EXISTS` for backlog probe

**Audit item:** #6.

**Change:** In `assignment.py::_discovery_backlog_exceeded`, replace:

```sql
SELECT COUNT(*) AS backlog FROM players WHERE ...
```

with:

```sql
SELECT EXISTS (
  SELECT 1 FROM players
  WHERE discover_done=TRUE
    AND full_write_done=FALSE
    AND highest_match_id IS NOT NULL
  OFFSET 100 LIMIT 1
) AS backlog_exceeded
```

The `OFFSET 100 LIMIT 1` short-circuits at the 101st row. Update the calling
code to read `backlog_exceeded` as a boolean.

### T2.3 — Drop the `SELECT 1` ping

**Audit item:** #7.

**Change:** In `database.py::db_connection`, delete lines 118–128 (the
`cur.execute("SELECT 1")` health check on cached connections). When the
cached connection is dead, `psycopg` raises `OperationalError` on the next
real query; `retryable_execute` already retries on
`_RETRYABLE_ERRORS`. Add `errors.OperationalError` to the `_RETRYABLE_ERRORS`
tuple so reconnects happen automatically. On `OperationalError`, before the
retry sleep, close the cached connection and pop it from
`_THREAD_LOCAL.connections` so the next iteration opens fresh.

### T2.4 — Single assignment query

**Audit item:** #8.

**Change:** In `assignment.py`, replace the three separate query plans
(`_assign_next_hero`, `_assign_discovery`, and the inline refresh fallback
in `_assign_next_task_on_connection`) with one combined query:

```sql
WITH candidates AS (
  SELECT steamAccountId, depth, highest_match_id, 1 AS priority, 'hero' AS kind
  FROM players
  WHERE hero_done=FALSE AND assigned_to IS NULL AND steamAccountId > 0
  ORDER BY steamAccountId ASC
  LIMIT %s
  FOR UPDATE SKIP LOCKED
),
discovery_candidates AS (
  SELECT steamAccountId, depth, highest_match_id, 2 AS priority, 'discover' AS kind
  FROM players
  WHERE hero_done=TRUE AND discover_done=FALSE
    AND assigned_to IS NULL AND steamAccountId > 0
  ORDER BY depth ASC, steamAccountId ASC
  LIMIT %s
  FOR UPDATE SKIP LOCKED
),
refresh_candidates AS (
  SELECT steamAccountId, depth, highest_match_id, 3 AS priority, 'refresh' AS kind
  FROM players
  WHERE hero_done=TRUE AND discover_done=TRUE
    AND assigned_to IS NULL AND steamAccountId > 0
  ORDER BY hero_refreshed_at ASC NULLS FIRST, steamAccountId ASC
  LIMIT %s
  FOR UPDATE SKIP LOCKED
),
chosen AS (
  SELECT * FROM candidates
  UNION ALL
  SELECT * FROM discovery_candidates WHERE NOT EXISTS (SELECT 1 FROM candidates)
  UNION ALL
  SELECT * FROM refresh_candidates
  WHERE NOT EXISTS (SELECT 1 FROM candidates)
    AND NOT EXISTS (SELECT 1 FROM discovery_candidates)
)
UPDATE players
SET assigned_to = chosen.kind,
    assigned_at = CURRENT_TIMESTAMP,
    hero_done = CASE WHEN chosen.kind = 'refresh' THEN FALSE ELSE players.hero_done END
FROM chosen
WHERE players.steamAccountId = chosen.steamAccountId
RETURNING players.steamAccountId, players.depth, players.highest_match_id, chosen.kind
```

Discovery backlog throttle (`_discovery_backlog_exceeded`) becomes a separate
guard query that runs BEFORE the combined query, and when true, removes
`discovery_candidates` from the chosen pool. (Implementation detail: pass
`MAX_HERO_TASK_SIZE` or `0` as the discovery limit when throttled.)

The Python post-processing into hero/discover/refresh task payloads stays —
it dispatches on `kind` returned from the combined query. The
`steam_account_id == 0` special-case cleanup in the old `_assign_discovery`
(lines 174–178) is dropped: the `AND steamAccountId > 0` filter in the
candidate CTEs already excludes any 0 row from being chosen.

### T2.5 — Bounded cleanup

**Audit item:** #9.

**Change:** In `database.py::release_incomplete_assignments`, replace the
unbounded `UPDATE` with:

```sql
UPDATE players SET assigned_to=NULL, assigned_at=NULL
WHERE steamAccountId IN (
  SELECT steamAccountId FROM players
  WHERE assigned_to IS NOT NULL
    AND (assigned_at IS NULL OR assigned_at <= NOW() - (%s)::interval)
  LIMIT 1000
)
```

`LIMIT 1000` per cycle bounds the lock duration. The 60-second cleanup
interval means stale assignments still get released within roughly
`60s * ceil(stale_count / 1000)`.

## Tier 3 — Sloppy

### T3.1 — Production-mode `app.py`

**Audit item:** #10.

**Change:** In `app.py`, change `debug=True` to `debug=False`. Keep
`threaded=True` and the `host="0.0.0.0", port=80` defaults.

### T3.2 — Env-driven `reset.py`

**Audit item:** #11.

**Change:** In `reset.py`:
- Replace the hardcoded `psycopg.connect(...)` call with
  `from stratz_scraper.database import connect_pg` and
  `connect_pg(autocommit=False)`.
- Delete the inline credential string.

### T3.3 — Delete dead code

**Audit item:** #12.

**Change:**
- Delete `stratz_scraper/locking.py` (FileLock class, no importers).
- In `database.py::refresh_leaderboard_views`, remove the `concurrently`
  parameter, the `del concurrently` line, and the
  `# ``concurrently`` is kept for API compatibility...` comment block.
  Callers don't pass this parameter.

### T3.4 — Trust-newer hero stats UPSERT

**Audit item:** #13.

**Change:** In `submissions.py::process_hero_submission`, change the
`ON CONFLICT(steamAccountId, heroId) DO UPDATE SET` clause from the
monotonic `CASE WHEN excluded.matches > hero_stats.matches` form to a
plain trust-newer:

```sql
ON CONFLICT(steamAccountId, heroId) DO UPDATE SET
  matches = excluded.matches,
  wins = excluded.wins
```

The refresh task's purpose is to re-fetch and supersede stored stats; the
monotonic guard defeated this. If Stratz pagination is later found to
return partial data, the guard should live worker-side, not in the DB.

## File touch map

| File | Tier touches |
|---|---|
| `stratz_scraper/web/submissions.py` | T1.1 (delete leaderboard maint), T1.2 (COPY), T1.3 (delete cursor reset), T3.4 (UPSERT) |
| `stratz_scraper/web/assignment.py` | T1.3 (drop cursor), T2.1 (drop counter read), T2.2 (EXISTS), T2.4 (one query) |
| `stratz_scraper/database.py` | T1.3 (schema migration for orphan meta keys), T2.3 (drop ping), T2.5 (LIMIT cleanup), T3.3 (drop `concurrently` param) |
| `stratz_scraper/web/app.py` | T1.1 (register new scheduler) |
| `stratz_scraper/web/leaderboard_refresh.py` | T1.1 (new file — periodic refresher) |
| `app.py` | T3.1 (debug=False) |
| `reset.py` | T3.2 (env-driven creds) |
| `stratz_scraper/locking.py` | T3.3 (delete file) |

## Out of scope

- No new tests (verification is code-review-only per scope decision).
- No worker-side (browser JS) changes. The wire protocol stays unchanged.
- No deployment / WSGI / systemd setup work. README note about
  `debug=False` deployment implications is acceptable but optional.
- No changes to the `hero_top100` schema or the `refresh_leaderboard_views`
  query itself.

## Risks

1. **Lazy leaderboard means `/leaderboards` shows up to 5-minute-stale data.**
   Acceptable per scope decision but worth noting if a downstream consumer
   expects real-time.
2. **`COPY` + temp-table approach (T1.2) needs the same advisory-lock
   reacquisition** the current code does. If the retry path doesn't
   reacquire the lock correctly, concurrent discovery submissions could
   race. Plan must include explicit test of the retry path during
   implementation.
3. **Combined assignment query (T2.4) needs verification** that Postgres
   chooses sensible plans — three partial indexes (`idx_players_hero_*`,
   `idx_players_discover_queue`, `idx_players_refresh_queue`) already exist.
   If Postgres picks a seq scan, query may regress. Run `EXPLAIN ANALYZE` on
   a populated database during implementation.
4. **Trust-newer UPSERT (T3.4) is a semantic change.** If Stratz API ever
   returns partial data (paginated hero list), counts could regress.
   Marked low-risk given the refresh task exists specifically to overwrite,
   but flagged for code reviewer awareness.

## Open decisions

None — all clarifications resolved during brainstorming.
