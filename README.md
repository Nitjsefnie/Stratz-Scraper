# Stratz Distributed Scraper Documentation

## Overview
The Stratz Distributed Scraper coordinates browser workers that call the [Stratz GraphQL API](https://stratz.com/) to gather player information. The backend runs in two phases: workers first fetch hero statistics for every discovered account, then they branch out to discover additional accounts from recent matches. A breadth-first queue stored in PostgreSQL tracks which phase each account is currently in so discovery only begins once every known player has complete hero data.

## Application Components

### Flask Backend (`app.py`)
The Flask application serves both the single-page front-end and the JSON API used by workers. On startup it ensures the PostgreSQL schema exists, seeds the root Steam account (`293053907`), clears any lingering task assignments, and starts a background cleanup loop that releases stale work. Key routes include:

- `GET /`: Renders the operator dashboard and exposes a local-only seeding form when the request originates from localhost.
- `POST /task`: Returns the next unit of work. While any account has unfinished hero statistics, workers receive `fetch_hero_stats` tasks. When every player is marked complete for hero stats the API hands out `discover_matches` tasks instead.
- `POST /task/reset`: Releases a task back into the queue. Hero tasks clear any partial hero rows, discovery tasks re-open the player for future crawling, and unknown task types simply clear the assignment flag.
- `POST /submit`: Accepts either hero statistics or discovery payloads. Hero submissions upsert per-hero performance, update the leaderboard, and flip the player's `hero_done` flag. Discovery submissions insert any newly found accounts (with incremented depth) and mark the submitting account's discovery phase as complete.
- `GET /progress`: Reports total players along with counts of accounts that have completed hero statistics and discovery.
- `GET /seed`: Local-only endpoint for inserting a contiguous range of seed accounts at depth 0.
- `GET /best` and `/leaderboards`: Render aggregated leaderboards for all heroes or individual picks.

### Database Layer (`stratz_scraper/database.py`)
The database module now targets PostgreSQL via `psycopg`. Connections are pooled per-thread for writers and opened on-demand for read-only operations. `ensure_schema_exists()` creates the schema when needed, makes sure all indexes exist, and automatically migrates data from an existing `dota.db` SQLite file into PostgreSQL the first time the server starts after the upgrade. The module exposes helpers for retrying statements that might be affected by transient locks, performing batched writes inside transactions, and releasing stale task assignments.

A default connection string of `postgresql://postgres:postgres@localhost:5432/stratz_scraper` is used when the `DATABASE_URL` environment variable is not provided.

### Hero Metadata (`stratz_scraper/heroes.py`)
Hero names remain bundled in `heroes.py`, providing a mapping that the backend uses to label leaderboard entries. Unknown hero IDs from Stratz are ignored to avoid polluting the tables.

## Task Flow
1. **Startup**: The server ensures the PostgreSQL schema exists and seeds the initial Steam account. If a legacy `dota.db` file is present its contents are copied into PostgreSQL and the file is renamed with a `.converted` suffix.
2. **Hero Phase**: Workers repeatedly receive `fetch_hero_stats` tasks until every known account is marked complete. Each submission updates aggregated hero statistics and the per-hero leaderboard.
3. **Discovery Phase**: Once hero coverage is complete, workers switch to `discover_matches` tasks. Newly discovered accounts are inserted at the next BFS depth with both phase flags reset, returning the system to the hero phase for those accounts.
4. **Progress Monitoring**: The `/progress` endpoint exposes total players and per-phase completion counts so operators can see how far the crawl has progressed.
5. **Leaderboard**: Leaderboard endpoints aggregate the highest match counts per hero and across all heroes for display in the UI.

## Running the App
1. Ensure a PostgreSQL instance is available and create a database (the defaults assume a database named `stratz_scraper` owned by the `postgres` user).
2. Export `DATABASE_URL` if different credentials or hosts are required.
3. Install dependencies: `pip install Flask psycopg[binary]`.
4. Start the development server with `python app.py`. The app listens on `0.0.0.0:80`.

When deploying behind a proxy, forward the original client IP so the `/seed` endpoint remains restricted to local administrators via `is_local_request`.

## Database Schema Reference

| Table | Purpose | Key Columns |
|-------|---------|-------------|
| `players` | BFS queue of discovered accounts with per-phase status. | `steamAccountId`, `depth`, `hero_done`, `discover_done`, `assigned_to`, `assigned_at` |
| `hero_stats` | Hero performance per account. | `steamAccountId`, `heroId`, `matches`, `wins` |
| `best` | Best-performing player per hero. | `hero_id`, `player_id`, `matches`, `wins` |
| `meta` | Key/value metadata for scheduler features. | `key`, `value` |

## Security and Error Handling Considerations
- **Token Privacy**: Stratz API tokens remain in the browser's `localStorage` and are only transmitted in GraphQL requests to Stratz. Removing a token row deletes it from storage.
- **Task Recovery**: Workers reset tasks with both the Steam ID and task type so the backend can reopen the correct phase without data corruption. Startup cleanup also clears any half-finished assignments after crashes.
- **Backoff Strategy**: The exponential backoff loop avoids hammering the API during failure storms, with the UI surfacing the minimum active backoff.

## Extending the Application
- Adjust BFS depth handling or implement depth limits to bound how far the scraper explores from the seed account.
- Add additional progress metrics (e.g., number of newly discovered accounts per depth) by extending the `/progress` endpoint and UI display.
- Enhance the leaderboard with win-rate calculations or timestamps to provide richer insights from the aggregated stats.
