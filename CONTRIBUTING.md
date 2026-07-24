# Contributing to Stratz-Scraper

Issues and pull requests are welcome — especially if the crawl misbehaved
against real data. This is a distributed BFS with leases and retries, so
"the queue stalled", "a player got scraped twice", and "depth came out
wrong" are the most valuable reports you can send. Include what the
database looked like when it happened.

## LLM and agent contributions are welcome

You may use an LLM or a coding agent to write your contribution. There is
no penalty, no separate review queue, and no expectation that you rewrite
its output by hand. Much of this repo was built that way.

Two conditions, and they are about honesty rather than provenance:

1. **Disclose the model** with a trailer on each commit it authored:

   ```
   Co-Authored-By: <Model Name> <noreply@example.com>
   ```

   e.g. `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`. One
   primary-author trailer per commit.

2. **Do not submit claims you have not verified.** Concurrency bugs are
   easy to reason about incorrectly and hard to spot in review. If you say
   a change fixes a race or speeds up the crawl, paste the run — the row
   counts, the timings, the actual output. An argument that it should work
   is not evidence.

If a maintainer's reply reads like it was drafted by an agent, it probably
was. That is fine in both directions.

## The invariants that reject the most patches

| Invariant | What it forbids |
|---|---|
| Tokens stay in the browser | Workers hold their own Stratz token and talk to Stratz directly. The backend must never receive, store, or proxy a token. |
| `/seed` is localhost-only | The seeding form and route are gated by `is_local_request`. Behind a proxy that means forwarding `X-Forwarded-For`, not removing the check. |
| Re-scrapes are idempotent | `hero_stats` UPSERT is trust-newer; `discovered_id` INSERT-ON-CONFLICT preserves the *minimum* depth. A patch that lets a later discovery raise an account's depth is a bug. |
| Failures release the lease | If background processing fails after the foreground commit, `_unmark_scrape_task` nulls `scraped_at` so the player is re-eligible. Do not swallow the error and leave the row marked done. |
| Backoff is honoured | Exponential backoff in the worker, `Retry-After` respected on 429. Do not tighten the loop to go faster; the API is not yours. |

## Getting it running

Requires **Python 3** and a **PostgreSQL** database.

```bash
createdb stratz_scraper            # default name; override with DATABASE_URL
pip install -r requirements.txt
python app.py                      # listens on 0.0.0.0:80
```

Then open the dashboard from localhost and seed a starting account. You
need a Stratz API token in the browser to run a worker; the backend does
not need one.

## Tests

There is no automated suite yet, which means **a PR that adds one is
genuinely welcome** — start with `stratz_scraper/database.py`, where the
queue and depth logic lives and where a regression is most expensive.

Until then, verification is manual and must be stated in the PR: seed a
small graph, run one worker, and show the queue draining and the depths
coming out right.

## House style

- **Python** — Flask for the API, raw psycopg3 against Postgres, no ORM.
- **SQL** — parameterised always. Never interpolate a value into a query
  string.
- **Frontend** — the worker page under `stratz_scraper/web/` is plain JS,
  no build step. Keep it that way.
- There is no linter or formatter config. Match the surrounding file.

## Pull requests

Small and single-purpose beats large and comprehensive. Include what
changed and why, and for anything touching the queue, leases, or depth,
the actual before/after state of the affected rows.

If you are unsure whether something is a bug or intended, open an issue and
ask — a wrong premise caught early is cheaper than a correct fix to the
wrong problem.
