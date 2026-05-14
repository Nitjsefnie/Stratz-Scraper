# Stratz-Scraper Peer-Walking Pivot — Design

**Date:** 2026-05-14
**Status:** Approved (brainstorming complete)

**Scope decisions (locked):**
- **Full pivot, no migration concerns.** Database is empty / nonexistent. Schema is rewritten from scratch.
- **Peer endpoint replaces match-history discovery entirely.** Match-history walking is fully dropped.
- **Per-peer aggregates discarded.** `matchCount` / `winCount` returned by the peers endpoint are not stored. The endpoint is used purely as a discovery oracle for steamAccountIds.
- **Collapsed to one phase.** No more hero / discover / refresh trichotomy. Every player has one task type: `scrape_player`. The single GraphQL document fetches hero stats, peers, and latest match in one round-trip.
- **One account per task.** Assignment hands out one player at a time, not batches of five.
- **Worker paginates peers per side until a short page.** Page size 2000 (server cap on outer `take`); a page that returns <2000 rows ends the stream.
- **No tests added.** Verification = diff review and one end-to-end smoke run.

## Goal

Replace the hero+discover+refresh three-phase pipeline with a single peer-walking scrape, using the Stratz `peers` endpoint as the discovery source. Discovers ~6× more unique peers per scrape unit than the existing match-history approach.

## Verified GraphQL query

Tested 2026-05-14 against `293053907` with an anonymous JWT. Results: HTTP 200, ~122 KB, ~1.3s for the combined call; full pagination per side returns ~2587 WITH + ~2881 AGAINST = 4292 unique peers.

```graphql
{
  player(steamAccountId: $sid) {
    matches(request: { take: 1 }) { id }
    heroesPerformance(request: { take: 999999, gameModeIds: [1, 22] }, take: 200) {
      heroId matchCount winCount
    }
  }
  stratz {
    page {
      player(steamAccountId: $sid) {
        teammates: peers(
          request: { playerTeammateSort: WITH,    matchGroupOrderBy: MATCH_COUNT, take: 1000 }
          take: 2000
        ) { steamAccountId }
        opponents: peers(
          request: { playerTeammateSort: AGAINST, matchGroupOrderBy: MATCH_COUNT, take: 1000 }
          take: 2000
        ) { steamAccountId }
      }
    }
  }
}
```

Pagination query (used when first page returns exactly 2000 rows on a side):

```graphql
{
  stratz { page { player(steamAccountId: $sid) {
    peers(
      request: { playerTeammateSort: $sort, matchGroupOrderBy: MATCH_COUNT, take: 1000 }
      take: 2000
      skip: $skip
    ) { steamAccountId }
  } } }
}
```

**Notes verified during probing:**
- `request.take` controls how many matches to SCAN (look-back window). Outer `take` controls how many ROWS to return.
- Outer `take` is server-capped at 2000 on the anonymous tier. `skip` paginates past it.
- `heroesPerformance` uses `PlayerType` (outer `take` accepted). `PagePlayerQuery.heroesPerformance` does NOT accept outer `take`.
- **Bug to work around:** putting `gameModeIds` on the peer queries in the SAME document as `heroesPerformance` reproducibly zeros out heroesPerformance. Peers therefore do NOT filter by game mode; the design accepts that peers come from all game modes (acceptable — peers are peers, mode-agnostic).

## Schema

Drop the existing `players` schema entirely. New shape:

```sql
CREATE TABLE players (
  steamAccountId BIGINT PRIMARY KEY,
  depth INTEGER NOT NULL,
  assigned_to TEXT,           -- 'scrape' or NULL
  assigned_at TIMESTAMPTZ,
  scraped_at TIMESTAMPTZ,     -- NULL = pending; else last scrape time
  latest_match_id BIGINT      -- match id from `matches(take:1)` at last scrape
);
```

Dropped columns: `hero_refreshed_at`, `hero_done`, `discover_done`, `full_write_done`, `highest_match_id`. Replaced by `scraped_at` (one bit of state per player: pending vs scraped + LRU ordering) and `latest_match_id` (used only for freshness display / future cursor work).

`hero_stats`, `hero_top100`, `meta` unchanged.

`progress_snapshots`: column `hero_done` → `scraped`, column `discover_done` dropped. Snapshotter and `/progress/graph` updated to match. Old snapshots are wiped along with the rest of the DB.

Indexes:

```sql
-- Frontier (unscraped, BFS-ordered)
CREATE INDEX idx_players_scrape_queue
  ON players (depth ASC, steamAccountId ASC)
  WHERE scraped_at IS NULL AND assigned_to IS NULL;

-- Re-scrape pool (scraped, LRU-ordered)
CREATE INDEX idx_players_rescrape_queue
  ON players (scraped_at ASC, steamAccountId ASC)
  WHERE scraped_at IS NOT NULL AND assigned_to IS NULL;

-- Assignment cleanup (kept from current schema)
CREATE INDEX idx_players_assignment_state
  ON players (assigned_to, assigned_at)
  WHERE assigned_to IS NOT NULL;
```

Drop all hero / discover / refresh-specific partial indexes (`idx_players_hero_unassigned_queue`, `idx_players_discover_queue`, `idx_players_refresh_queue`, `idx_players_discover_fullwrite_backlog`, `idx_players_hero_completed`).

Drop the orphan-key migration too (`DELETE FROM meta WHERE key IN ('hero_assignment_cursor', 'task_assignment_counter')`); the keys never exist in the new schema.

## File-by-file changes

### `stratz_scraper/database.py` (~535 lines today)

- Rewrite `ensure_schema()` to the new shape above. Keep the `INSERT ... (steamAccountId, depth) VALUES (INITIAL_PLAYER_ID, 0)` seed.
- Rewrite `ensure_indexes()` to create the three new indexes above. Remove all the old `CREATE INDEX IF NOT EXISTS idx_players_hero_*` / `idx_players_discover_*` / `idx_players_refresh_*` blocks.
- Remove the `DELETE FROM meta WHERE key IN ('hero_assignment_cursor', 'task_assignment_counter')` migration (orphan keys don't exist post-pivot).
- `release_incomplete_assignments()`: unchanged.
- `refresh_leaderboard_views()`: unchanged (operates on `hero_stats` → `hero_top100`).
- `retryable_execute`, `retryable_executemany`, `db_connection`, etc.: unchanged.

### `stratz_scraper/web/assignment.py` (~287 lines today)

Most of the file goes. Replace `_assign_next_task_on_connection` with one CTE+UPDATE that picks from the unscraped frontier first, falling back to the LRU re-scrape pool:

```python
def _assign_next_task_on_connection(connection, *, run_cleanup: bool) -> dict | None:
    if run_cleanup:
        maybe_run_assignment_cleanup(connection)
    with connection.cursor() as cur:
        assigned = retryable_execute(
            cur,
            """
            WITH frontier AS (
              SELECT steamAccountId, depth FROM players
              WHERE scraped_at IS NULL AND assigned_to IS NULL AND steamAccountId > 0
              ORDER BY depth ASC, steamAccountId ASC
              LIMIT 1 FOR UPDATE SKIP LOCKED
            ),
            refresh_pool AS (
              SELECT steamAccountId, depth FROM players
              WHERE scraped_at IS NOT NULL AND assigned_to IS NULL AND steamAccountId > 0
              ORDER BY scraped_at ASC, steamAccountId ASC
              LIMIT 1 FOR UPDATE SKIP LOCKED
            ),
            chosen AS (
              SELECT * FROM frontier
              UNION ALL
              SELECT * FROM refresh_pool WHERE NOT EXISTS (SELECT 1 FROM frontier)
            )
            UPDATE players
            SET assigned_to='scrape', assigned_at=CURRENT_TIMESTAMP
            FROM chosen
            WHERE players.steamAccountId = chosen.steamAccountId
            RETURNING players.steamAccountId, players.depth, players.latest_match_id
            """,
            retry_interval=ASSIGNMENT_RETRY_INTERVAL,
        ).fetchone()
        if assigned is None:
            return None
        return {
            "type": "scrape_player",
            "steamAccountId": int(row_value(assigned, "steamAccountId")),
            "depth": int(row_value(assigned, "depth")),
            "latestMatchId": (
                int(row_value(assigned, "latest_match_id"))
                if row_value(assigned, "latest_match_id") is not None else None
            ),
        }
```

Delete: `_DiscoveryThrottle`, `_DISCOVERY_THROTTLED`, `_discovery_backlog_exceeded`, `MAX_HERO_TASK_SIZE`, `MAX_DISCOVERY_TASK_SIZE`. Keep: `assign_next_task`, `maybe_run_assignment_cleanup`, `ensure_assignment_cleanup_scheduler`, `ASSIGNMENT_CLEANUP_*`, `ASSIGNMENT_RETRY_INTERVAL`.

### `stratz_scraper/web/submissions.py` (~333 lines today)

Replace `process_hero_submission` + `process_discover_submission` + `_unmark_hero_task` + `_unmark_discover_task` with one `process_scrape_submission`:

```python
def process_scrape_submission(
    steam_account_id: int,
    heroes_payload: Iterable[dict] | None,
    discovered_ids: list[int],
    latest_match_id: int | None,
    next_depth: int,
) -> None:
    hero_stats_rows, _ = _extract_hero_rows(steam_account_id, heroes_payload)
    try:
        with db_connection(write=True) as conn:
            cur = conn.cursor()
            if hero_stats_rows:
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
            if discovered_ids:
                _copy_discovered_rows(conn, discovered_ids, next_depth)
            retryable_execute(
                cur,
                """
                UPDATE players
                SET scraped_at = NOW(),
                    latest_match_id = COALESCE(%s, latest_match_id),
                    assigned_to = NULL,
                    assigned_at = NULL
                WHERE steamAccountId=%s
                """,
                (latest_match_id, steam_account_id),
            )
    except Exception:
        import traceback
        print(f"[submit-background] scrape failed for {steam_account_id}", flush=True)
        traceback.print_exc()
        _unmark_scrape_task(steam_account_id)


def _unmark_scrape_task(steam_account_id: int) -> None:
    try:
        with db_connection(write=True) as conn:
            cur = conn.cursor()
            retryable_execute(
                cur,
                """
                UPDATE players
                SET assigned_to=NULL, assigned_at=NULL
                WHERE steamAccountId=%s
                """,
                (steam_account_id,),
            )
    except Exception:
        import traceback
        traceback.print_exc()
```

`_copy_discovered_rows` stays — it's the COPY-into-temp-table upsert from the prior pass, reusable.

Delete:
- `_iter_consuming_values` (currently dead code; references `Iterator` not in imports).
- `submit_hero_submission`, `submit_discover_submission`, `process_hero_submission`, `process_discover_submission`.

Add: `submit_scrape_submission(steam_account_id, heroes_payload, discovered_ids, latest_match_id, next_depth)` that dispatches `process_scrape_submission` through `BACKGROUND_EXECUTOR`.

Move `import time` to the top of the file (currently mid-file at line 27).

`BACKGROUND_EXECUTOR` stays at `max_workers=1` — consistent with the Tier 1 decision; submissions are still cheap.

### `stratz_scraper/web/app.py` (~398 lines today)

`/submit` collapses to one body. The two existing branches (`fetch_hero_stats` and `discover_matches`) become:

```python
if task_type != "scrape_player":
    return jsonify({"status": "error", "message": "Unknown submit type"}), 400
try:
    steam_account_id = int(data["steamAccountId"])
except (KeyError, TypeError, ValueError):
    return jsonify({"status": "error", "message": "steamAccountId is required"}), 400
if steam_account_id <= 0:
    return jsonify({"status": "error", "message": "steamAccountId must be positive"}), 400

heroes_payload = data.get("heroes") or []
discovered_raw = data.get("discovered") or []
latest_match_id_raw = data.get("latestMatchId")
depth_raw = data.get("depth")

# normalise discovered ids
discovered_ids: list[int] = []
seen: set[int] = set()
for entry in discovered_raw if isinstance(discovered_raw, list) else []:
    candidate = entry.get("steamAccountId") if isinstance(entry, dict) else entry
    try:
        cid = int(candidate)
    except (TypeError, ValueError):
        continue
    if cid <= 0 or cid == steam_account_id or cid in seen:
        continue
    seen.add(cid)
    discovered_ids.append(cid)

# normalise latest_match_id
try:
    latest_match_id = int(latest_match_id_raw) if latest_match_id_raw is not None else None
except (TypeError, ValueError):
    latest_match_id = None
if latest_match_id is not None and latest_match_id < 0:
    latest_match_id = None

# normalise depth
try:
    provided_depth = int(depth_raw) if depth_raw is not None else None
except (TypeError, ValueError):
    provided_depth = None

# foreground: mark the player as scraped, fetch the next task
next_task = None
with db_connection(write=True) as conn:
    cur = conn.cursor()
    row = retryable_execute(
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
    if row is None:
        return jsonify({"status": "error", "message": "Player not found"}), 404
    assignment_depth = int(row["depth"])
    if request_new_task:
        next_task = assign_next_task(connection=conn)

next_depth = (provided_depth if provided_depth is not None else assignment_depth) + 1

# background: upsert hero stats + COPY discovered peers
submit_scrape_submission(
    steam_account_id,
    heroes_payload,
    discovered_ids,
    latest_match_id,
    next_depth,
)

response = {"status": "ok"}
if request_new_task:
    response["task"] = next_task
return jsonify(response)
```

Note: the foreground UPDATE is duplicated by the background `process_scrape_submission` (both clear `assigned_to` + set `scraped_at`). The foreground call is the authoritative one; the background's UPDATE is idempotent and safe to repeat (NOW() will be slightly later but accurate).

The actual hero_stats UPSERT and `_copy_discovered_rows` happen ONLY in the background path, so the foreground stays cheap and the worker can move on quickly. The deduped `discovered_ids` list is computed once and passed to the background.

Update imports: drop `submit_discover_submission`, `submit_hero_submission`; add `submit_scrape_submission`.

### `stratz_scraper/web/tasks.py` (~77 lines today)

Three reset helpers collapse to one:

```python
def reset_player_task(steam_account_id: int, _task_type: object = None) -> bool:
    with db_connection(write=True) as conn:
        cur = conn.cursor()
        cursor = retryable_execute(
            cur,
            """
            UPDATE players SET assigned_to=NULL, assigned_at=NULL
            WHERE steamAccountId=%s
            """,
            (steam_account_id,),
        )
        return (cursor.rowcount or 0) > 0
```

The `task_type` parameter is preserved for API compatibility with the existing `/task/reset` body but is now ignored — every scrape task resets the same way.

### `stratz_scraper/web/seed.py` (~27 lines today)

Drop `hero_done`, `discover_done` from the INSERT:

```python
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

### `stratz_scraper/web/progress.py` (~203 lines today)

`fetch_progress` returns the new shape:

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
    total = (row["total"] if row else None) or 0
    scraped = (row["scraped"] if row else None) or 0
    return {"players_total": total, "scraped": scraped}
```

`record_progress_snapshot` writes the new columns. `progress_snapshots` table schema in `database.py` becomes:

```sql
CREATE TABLE progress_snapshots (
  captured_at TIMESTAMPTZ PRIMARY KEY,
  players_total BIGINT NOT NULL,
  scraped BIGINT NOT NULL
);
```

`list_progress_snapshots` selects the new columns.

### `stratz_scraper/web/static/js/app.js` (2572 lines today)

Replace:
- `fetchPlayerHeroes`
- `discoverMatches`
- `runDiscoveryTask`
- `submitHeroStats`
- `submitDiscovery`

with two new functions:

```javascript
async function scrapePlayer(steamAccountId, token) {
  const sid = Number(steamAccountId);
  if (!Number.isFinite(sid) || sid <= 0) {
    throw new Error('Invalid steamAccountId');
  }

  // First call: heroes + matches.id + page-0 peers (WITH + AGAINST)
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

  const heroes = (player.heroesPerformance ?? []).map((h) => ({
    heroId: h.heroId, games: h.matchCount, wins: h.winCount,
  }));
  const latestMatchId = (player.matches ?? [{}])[0]?.id ?? null;

  const peers = new Set();
  const collectPeers = (rows) => {
    if (!Array.isArray(rows)) return;
    for (const row of rows) {
      const id = Number(row?.steamAccountId);
      if (Number.isFinite(id) && id > 0 && id !== sid) {
        peers.add(id);
      }
    }
  };
  collectPeers(pp.teammates);
  collectPeers(pp.opponents);

  // Pagination: if a side returned exactly the page-size cap, fetch more.
  const PAGE = 2000;
  const paginateSide = async (sort, initialRows) => {
    if (!Array.isArray(initialRows) || initialRows.length < PAGE) return;
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
      if (rows.length < PAGE) return;
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
    depth: depth,
    task: Boolean(requestNextTask),
  };
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

The `workLoopForToken` task switch drops the three handlers and adds one:

```javascript
if (task.type === 'scrape_player') {
  const sid = task.steamAccountId;
  logToken(token, `Scrape task for ${sid}.`);
  const result = await scrapePlayer(sid, token.activeToken);
  logToken(
    token,
    `Scraped ${result.heroes.length} heroes, ${result.discovered.length} peers from ${sid}.`,
  );
  nextTask = await submitScrape(result, task.depth, true);
}
```

Remove the `fetch_hero_stats` / `discover_matches` / `refresh_player_data` branches.

Helper functions that become unused after deletion (`getDiscoveryTaskPlayers`, etc.) are removed.

Progress display in `updateProgressDisplay`:

```javascript
function updateProgressDisplay(payload) {
  if (!payload) return;
  elements.progressText.textContent = `Scraped: ${payload.scraped} / ${payload.players_total}`;
}
```

### `stratz_scraper/web/templates/index.html` (~114 lines today)

The progress text initial value is the only template-side reference (`<span id="progressText">0 / 0</span>`) — keep as-is, the JS rewrites it on refresh.

### `stratz_scraper/web/templates/progress_graph.html` (~138 lines today)

No structural changes — the template just renders a canvas and embeds snapshot JSON.

### `stratz_scraper/web/static/js/progress_graph.js` (~154 lines today)

Chart datasets change from 3 series (Hero Done / Discover Done / Players Total) to 2 (Scraped / Players Total):

```javascript
const scraped = timeSeries.map((entry) => ({ x: entry.x, y: entry.scraped }));
const playersTotal = timeSeries.map((entry) => ({ x: entry.x, y: entry.playersTotal }));

new Chart(canvasElement, {
  type: 'line',
  data: {
    datasets: [
      { label: 'Scraped',       data: scraped,      borderColor: 'rgba(75, 192, 192, 1)',  backgroundColor: 'rgba(75, 192, 192, 0.1)', tension: 0.2 },
      { label: 'Players Total', data: playersTotal, borderColor: 'rgba(54, 162, 235, 1)',  backgroundColor: 'rgba(54, 162, 235, 0.1)', tension: 0.2 },
    ],
  },
  // options unchanged
});
```

And `timeSeries` mapping reads `entry.scraped` instead of `entry.hero_done` + `entry.discover_done`.

### `reset.py` (~92 lines today)

**Delete the file.** The clean slate means there's no salvage workflow to run. Add a one-line note in the README if needed.

### `README.md` (~62 lines today)

Rewrite the "Task Flow" + "Database Schema Reference" sections. New summary:

- App serves a single-page worker UI and a JSON API.
- Workers poll `/task`, get `scrape_player` assignments, fire one combined GraphQL call (heroes + peers + latest match), paginate per side if necessary, submit results to `/submit`.
- Backend maintains `players` (BFS queue + scraped state), `hero_stats` (per-(player, hero) counts), `hero_top100` (leaderboard cache rebuilt every 5 min by background thread).

## Out of scope

- No new tests added. Verification is diff review + one end-to-end smoke run after deploy.
- Worker token UI, multi-token rotation, JWT decode, exponential backoff, Retry-After handling — all unchanged.
- `leaderboard.py`, `leaderboard_refresh.py`, `heroes.py`, `request_utils.py`, `config.py` — unchanged.
- Background executor stays at `max_workers=1`.
- `BACKGROUND_EXECUTOR` keeps its current discovery advisory lock (`_DISCOVERY_SUBMISSION_LOCK_ID`) for `_copy_discovered_rows` retries.

## Risks

1. **`gameModeIds` on peers zeros out hero stats in the same document.** Worked around by NOT filtering peers by game mode. Documented above; can be revisited if Stratz fixes the bug.
2. **2000-row outer-take cap is anonymous-tier.** A paid Stratz key may raise it, reducing pagination overhead. Not blocking; pagination handles whatever the cap is.
3. **One-account-per-task means workers do more `/task` polls per unit work.** Acceptable: each call is now larger (multiple GraphQL round-trips, more peers discovered) so the polling overhead ratio actually improves.
4. **Foreground `/submit` UPDATE + background process UPDATE both clear `assigned_to`.** Race-free because the second UPDATE is idempotent and uses NOW() for `scraped_at`. The foreground UPDATE happens first; the background's UPDATE either matches or refreshes `scraped_at` slightly later (acceptable).
5. **Combined query failure modes.** If hero query 500s but peers succeed (or vice versa), the worker receives a GraphQL error and the existing retry/reset path kicks in. No partial-state special-casing needed.

## Open decisions

None — all clarifications resolved during brainstorming.
