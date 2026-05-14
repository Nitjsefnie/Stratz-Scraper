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
