# Stratz Distributed Scraper Documentation

## Overview
The Stratz Distributed Scraper is a Flask-based coordination service and single-page web application for collecting hero performance statistics from the [Stratz GraphQL API](https://stratz.com/). Browser clients supply Stratz API tokens and execute scraping work in parallel. The Flask backend exposes a REST API for scheduling player IDs to scrape, storing aggregated results in a SQLite database, and sharing progress across workers. The front-end provides controls for managing tokens, monitoring progress, and inspecting top-performing heroes for each player.

## Application Components

### Flask Backend (`app.py`)
The backend runs a Flask app that serves the HTML UI and several JSON endpoints that workers call while scraping. Schema initialization (`ensure_schema`) and recovery of in-progress assignments (`release_incomplete_assignments`) are executed on startup to guarantee a consistent database state.【F:app.py†L1-L12】【F:database.py†L15-L54】

Key routes include:

- `GET /`: Renders the `index.html` template that bootstraps the front-end interface. The template optionally exposes a seeding form when the request originates from localhost, determined by `is_local_request`.【F:app.py†L34-L38】【F:app.py†L14-L33】【F:templates/index.html†L1-L56】
- `POST /task`: Assigns the next available player ID to the requesting worker. It increments a `task_counter` in the `meta` table, periodically requeues completed players for reruns (`RERUN_INTERVAL`), alternates between ascending and descending order to balance the queue, and marks the chosen player as assigned. If no work is available it returns `null`.【F:app.py†L41-L115】
- `POST /task/reset`: Releases a player ID back to the queue and clears any partial hero data when a worker fails mid-task.【F:app.py†L118-L138】
- `POST /submit`: Accepts hero performance payloads from workers, upserts the data into the `hero_stats` table, updates per-hero best performers, and marks the player as complete.【F:app.py†L141-L181】
- `GET /progress`: Returns total and completed player counts for progress monitoring.【F:app.py†L184-L189】
- `GET /seed`: Local-only endpoint that seeds a contiguous range of player IDs into the `players` table for processing.【F:app.py†L192-L215】
- `GET /best`: Provides the aggregated best-hero leaderboard ordered by match count.【F:app.py†L218-L223】

The app runs in development mode on port 8000 when executed directly.【F:app.py†L225-L226】

### Database Layer (`database.py`)
The application stores all data in a SQLite database (`dota.db`). The `ensure_schema` function creates four tables if they do not already exist:【F:database.py†L3-L54】

- `players`: Tracks the scraping queue with assignment metadata and completion flags.
- `hero_stats`: Stores scraped hero performance per player.
- `best`: Maintains the top-performing player for each hero based on match count (with wins as metadata).
- `meta`: Provides application metadata such as the `task_counter` used to vary queue ordering and trigger reruns.

`release_incomplete_assignments` clears the `assigned_to` and `assigned_at` columns for any tasks that were previously checked out but not finished, ensuring stranded work is redispatched when the server restarts.【F:database.py†L56-L70】

### Hero Metadata (`heroes.py`)
Hero names are loaded from a bundled list of localized hero records. The module exports the `HEROES` dictionary that maps hero IDs to their names, enabling the backend to label stats during submission. Any unknown hero IDs received from the API are ignored.【F:heroes.py†L1-L86】【F:app.py†L152-L173】

## Front-End Behavior

### Template and Layout (`templates/index.html`)
The root template structures the interface into cards for token management, worker controls, optional queue seeding, activity logging, and the top-heroes table. It loads a single deferred JavaScript file (`static/js/app.js`) and a CSS stylesheet for styling. The seeding section is conditionally rendered only for localhost requests, matching the backend access policy.【F:templates/index.html†L1-L67】

### Styling (`static/css/styles.css`)
The stylesheet defines a responsive, glassmorphism-inspired layout with light/dark mode support, status chips, log styling, and token entry rows. Controls such as buttons and grid layouts adapt to small screens via media queries.【F:static/css/styles.css†L1-L207】

### Client-Side Logic (`static/js/app.js`)
The front-end script orchestrates token storage, worker loops, and UI updates. Major responsibilities include:

- **State Management**: Tracks tokens, running status, exponential backoff, and request limits per token in the `state` object. Helpers update UI elements such as progress, backoff timers, and remaining requests.【F:static/js/app.js†L1-L119】【F:static/js/app.js†L322-L371】
- **Token Persistence**: Renders token rows with inputs for API keys and optional request caps, persists them to `localStorage`, and migrates any legacy cookie-stored tokens. Removal and migration logic keeps browser storage authoritative.【F:static/js/app.js†L121-L274】【F:static/js/app.js†L374-L452】
- **Worker Loop**: For each active token, `workLoopForToken` continuously fetches tasks, queries Stratz for hero performance data, submits results, and handles exponential backoff on errors. It also decrements per-token request quotas and stops when limits are reached or work is exhausted.【F:static/js/app.js†L276-L373】
- **API Integrations**: Implements client calls for `/task`, `/task/reset`, `/submit`, `/progress`, `/seed`, and `/best`, formatting requests and parsing responses. GraphQL queries to Stratz gather hero performance for standard and turbo game modes (mode IDs 1 and 22).【F:static/js/app.js†L208-L338】
- **Initialization and Event Wiring**: On load, the script restores saved tokens, refreshes progress and top-hero data, and binds click handlers for starting/stopping workers, refreshing data, and seeding IDs (when allowed).【F:static/js/app.js†L454-L527】

## Data Flow
1. **Queue Preparation**: Administrators seed player IDs via `/seed` or prepopulate the `players` table. Upon startup any lingering assignments are cleared to avoid stuck tasks.【F:app.py†L192-L215】【F:database.py†L56-L70】
2. **Worker Start**: Users open the UI, add one or more Stratz tokens (optionally with request limits), and press **Start**. The browser persists tokens locally for subsequent sessions.【F:static/js/app.js†L121-L274】【F:static/js/app.js†L479-L509】
3. **Task Assignment**: Each worker loop calls `/task` to lock the next player ID, honoring rerun intervals and alternating queue order to distribute load.【F:app.py†L41-L115】
4. **Data Retrieval**: The worker issues a GraphQL request to Stratz for hero performance. If successful, the hero array is submitted back to `/submit`; on failure the task is reset for reassignment and the loop backs off before retrying.【F:static/js/app.js†L232-L335】【F:static/js/app.js†L276-L343】
5. **Result Storage**: The backend upserts hero stats, updates the `best` leaderboard, and flags the player as complete. Progress counters and top-hero tables reflect the new data on subsequent refreshes.【F:app.py†L141-L189】【F:static/js/app.js†L234-L338】

## Running the App
Run the Flask application directly to start the development server:

```bash
python app.py
```

The server listens on `0.0.0.0:8000`, serving the UI and API endpoints. Ensure a valid `dota.db` database file is writable in the working directory (created automatically). When deploying, configure HTTPS termination and restrict `/seed` access to trusted clients by running the server behind a reverse proxy that preserves the client IP for `is_local_request` checks.【F:app.py†L14-L33】【F:app.py†L225-L226】

## Database Schema Reference

| Table | Purpose | Key Columns |
|-------|---------|-------------|
| `players` | Queue of player IDs to scrape, with assignment metadata. | `id`, `assigned_to`, `assigned_at`, `done` |
| `hero_stats` | Hero performance data per player. | `player_id`, `hero_id`, `matches`, `wins` |
| `best` | Best-performing player per hero across the dataset. | `hero_id`, `player_id`, `matches`, `wins` |
| `meta` | Application metadata such as counters for queue behavior. | `key`, `value` |

All tables are created automatically on startup if missing, and the `task_counter` meta entry is initialized to `0` when absent.【F:database.py†L15-L54】

## Security and Error Handling Considerations
- **Token Storage**: Stratz tokens never leave the browser aside from request headers to Stratz. They are stored in `localStorage`, with fallback migration paths for legacy cookies. Clearing tokens removes them from both storage mechanisms.【F:static/js/app.js†L121-L209】
- **Task Recovery**: Workers encountering errors reset tasks via `/task/reset`, ensuring the queue remains consistent. The server also purges assignments on startup for robustness against worker crashes.【F:app.py†L118-L138】【F:database.py†L56-L70】
- **Backoff Strategy**: Worker loops exponentially back off (capped at 24 hours) after failures, displaying the minimum backoff across running tokens in the UI to inform operators.【F:static/js/app.js†L1-L119】【F:static/js/app.js†L276-L343】

## Extending the Application
Potential extension points include:
- Adjusting `RERUN_INTERVAL` or queue ordering logic to tune how frequently completed players are refreshed.【F:app.py†L41-L115】
- Enriching the `best` leaderboard with additional metrics (win rate, last updated timestamps) by expanding the `/best` endpoint and table schema.【F:app.py†L218-L223】
- Adding authentication or rate limiting to backend endpoints before internet-facing deployments.

This documentation reflects the current state of the application as provided in the repository.
