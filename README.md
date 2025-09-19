# Stratz Distributed Scraper Documentation

## Overview
The Stratz Distributed Scraper coordinates browser workers that call the [Stratz GraphQL API](https://stratz.com/) to gather player information. The backend now runs in two phases: workers first fetch hero statistics for every discovered account, then they branch out to discover additional accounts from recent matches. A lightweight breadth-first search (BFS) queue stored in SQLite tracks which phase each account is currently in, ensuring that discovery only begins once every known player has complete hero data.

## Application Components

### Flask Backend (`app.py`)
The Flask application serves both the single-page front-end and the JSON API used by workers. On startup it recreates the schema if the database file is missing, seeds the root Steam account (`293053907`), and clears any lingering task assignments so work can be reassigned cleanly.【F:app.py†L1-L25】

Key routes include:

- `GET /`: Renders the user interface template and exposes a local-only seeding form when the request originates from localhost.【F:app.py†L44-L58】【F:templates/index.html†L1-L67】
- `POST /task`: Returns the next unit of work. While any account has unfinished hero statistics, workers receive `fetch_hero_stats` tasks. Only after every player is marked complete for hero stats do workers receive `discover_matches` tasks. Assignments are persisted in the `players` table so a task can be safely retried if a worker crashes.【F:app.py†L60-L115】
- `POST /task/reset`: Releases a task back into the queue. Hero tasks clear any partial hero rows, discovery tasks re-open the player for future crawling, and unknown task types simply clear the assignment flag.【F:app.py†L117-L152】
- `POST /submit`: Accepts either hero statistics or discovery payloads. Hero submissions upsert per-hero performance, update the leaderboard, and flip the player's `hero_done` flag. Discovery submissions insert any newly found accounts (with incremented depth) and mark the submitting account's discovery phase as complete.【F:app.py†L154-L222】
- `GET /progress`: Reports total players along with counts of accounts that have completed hero statistics and discovery. The UI displays both numbers side by side.【F:app.py†L224-L241】【F:static/js/app.js†L216-L226】
- `GET /seed`: Local-only endpoint for inserting a contiguous range of seed accounts at depth 0.【F:app.py†L243-L270】
- `GET /best`: Returns the current leaderboard of best performers per hero.【F:app.py†L272-L277】

The app still runs in debug mode on port 8000 when launched directly.【F:app.py†L279-L280】

### Database Layer (`database.py`)
`ensure_schema` now drops the legacy tables and recreates the schema with explicit BFS metadata: each player row stores the Steam account ID, depth, assignment metadata, and completion flags for both hero and discovery phases. Hero statistics are keyed by `(steamAccountId, heroId)`, the `best` table retains its existing structure, and the `meta` table persists simple key/value settings. `release_incomplete_assignments` clears any `assigned_to` markers regardless of phase so the queue is ready after restarts.【F:database.py†L1-L52】【F:database.py†L55-L68】

### Hero Metadata (`heroes.py`)
Hero names remain bundled in `heroes.py`, providing a mapping that the backend uses to label leaderboard entries. Unknown hero IDs from Stratz are ignored to avoid polluting the tables.【F:heroes.py†L1-L87】【F:app.py†L181-L203】

## Front-End Behavior

### Template and Layout (`templates/index.html`)
The HTML template structures the UI into cards for token management, worker controls, optional seeding, an activity log, and the leaderboard table. It loads the updated JavaScript bundle and stylesheet to drive the application.【F:templates/index.html†L1-L67】

### Styling (`static/css/styles.css`)
Styling remains unchanged, providing responsive layouts, log formatting, and status chips that surface worker state at a glance.【F:static/css/styles.css†L1-L207】

### Client-Side Logic (`static/js/app.js`)
The browser script orchestrates tokens, workers, and API interactions:

- **State Management & Persistence**: Tokens, request limits, and worker status are tracked in `state` and persisted to `localStorage`. UI helpers keep the chip, backoff, and quota displays in sync.【F:static/js/app.js†L1-L229】【F:static/js/app.js†L372-L452】
- **Task Handling**: Workers now interpret typed tasks. Hero tasks call the `PlayerHeroes` GraphQL query and submit `{heroId, matches, wins}` payloads. Discovery tasks call the `PlayerMatches` query, collect unique Steam IDs from match rosters, and send them back so the backend can grow the BFS frontier.【F:static/js/app.js†L231-L347】【F:static/js/app.js†L481-L538】
- **Recovery Logic**: If a worker errors out, the task is reset with both its Steam ID and type so the backend can restore the correct phase state. Exponential backoff continues to throttle repeated failures.【F:static/js/app.js†L481-L538】
- **Initialization**: On load the script restores saved tokens, refreshes progress (showing both hero and discovery completion counts), and fetches the leaderboard.【F:static/js/app.js†L539-L606】

## Data Flow
1. **Startup**: If `dota.db` is missing, the server rebuilds the schema and seeds the initial Steam account. Any stale assignments are cleared so workers start fresh.【F:app.py†L1-L25】【F:database.py†L55-L68】
2. **Hero Phase**: Workers repeatedly receive `fetch_hero_stats` tasks until every known account is marked complete. Each submission updates aggregated hero statistics and the per-hero leaderboard.【F:app.py†L60-L203】【F:static/js/app.js†L481-L506】
3. **Discovery Phase**: Once hero coverage is complete, workers switch to `discover_matches` tasks. Newly discovered accounts are inserted at the next BFS depth with both phase flags reset, returning the system to the hero phase for those accounts.【F:app.py†L82-L222】【F:static/js/app.js†L506-L538】
4. **Progress Monitoring**: The `/progress` endpoint exposes total players and per-phase completion counts so operators can see how far the crawl has progressed.【F:app.py†L224-L241】【F:static/js/app.js†L216-L226】
5. **Leaderboard**: The `/best` endpoint aggregates the highest match counts per hero, allowing the UI to display top performers across all crawled accounts.【F:app.py†L202-L221】【F:app.py†L272-L277】

## Running the App
Start the development server with:

```bash
python app.py
```

The app listens on `0.0.0.0:8000`. Ensure the process has permission to create `dota.db`; on first boot it will populate the schema and seed the root account automatically. When deploying behind a proxy, forward the original client IP so the `/seed` endpoint remains restricted to local administrators via `is_local_request`.【F:app.py†L14-L43】【F:app.py†L243-L280】

## Database Schema Reference

| Table | Purpose | Key Columns |
|-------|---------|-------------|
| `players` | BFS queue of discovered accounts with per-phase status. | `steamAccountId`, `depth`, `hero_done`, `discover_done`, `assigned_to`, `assigned_at` |
| `hero_stats` | Hero performance per account. | `steamAccountId`, `heroId`, `matches`, `wins` |
| `best` | Best-performing player per hero. | `hero_id`, `player_id`, `matches`, `wins` |
| `meta` | Key/value metadata for scheduler features. | `key`, `value` |

`ensure_schema` recreates these tables when invoked, enabling quick resets during development, while `release_incomplete_assignments` clears any stuck tasks during startup.【F:database.py†L1-L68】

## Security and Error Handling Considerations
- **Token Privacy**: Stratz API tokens remain in the browser's `localStorage` and are only transmitted in GraphQL requests to Stratz. Removing a token row deletes it from storage.【F:static/js/app.js†L121-L229】
- **Task Recovery**: Workers reset tasks with both the Steam ID and task type so the backend can reopen the correct phase without data corruption. Startup cleanup also clears any half-finished assignments after crashes.【F:static/js/app.js†L481-L538】【F:database.py†L55-L68】
- **Backoff Strategy**: The exponential backoff loop remains in place to avoid hammering the API during failure storms, with the UI surfacing the minimum active backoff.【F:static/js/app.js†L1-L119】【F:static/js/app.js†L481-L538】

## Extending the Application
- Adjust BFS depth handling or implement depth limits to bound how far the scraper explores from the seed account.【F:app.py†L60-L222】
- Add additional progress metrics (e.g., number of newly discovered accounts per depth) by extending the `/progress` endpoint and UI display.【F:app.py†L224-L241】【F:static/js/app.js†L216-L226】
- Enhance the leaderboard with win-rate calculations or timestamps to provide richer insights from the aggregated stats.【F:app.py†L181-L203】【F:app.py†L272-L277】

This documentation reflects the BFS-based scraping workflow implemented in this repository.
