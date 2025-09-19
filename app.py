#!/usr/bin/env python3
"""Flask application providing the Stratz distributed scraping UI and API."""
from pathlib import Path

from flask import Flask, Response, abort, jsonify, render_template, request

from database import (
    DB_PATH,
    db,
    ensure_hero_refresh_column,
    ensure_schema,
    release_incomplete_assignments,
    reset_hero_refresh_once,
)
from heroes import HEROES, HERO_SLUGS

app = Flask(__name__, static_folder="static", template_folder="templates")

if not Path(DB_PATH).exists():
    ensure_schema()
    conn = db()
    conn.execute('PRAGMA journal_mode=WAL;')
    conn.execute(
        """
        INSERT OR IGNORE INTO players (steamAccountId, depth, hero_done, discover_done)
        VALUES (293053907, 0, 0, 0)
        """
    )
    conn.commit()
    conn.close()

ensure_hero_refresh_column()
reset_hero_refresh_once()
release_incomplete_assignments()


def is_local_request() -> bool:
    """Return True if the active request originates from localhost."""

    local_hosts = {"127.0.0.1", "::1"}
    remote_addr = (request.remote_addr or "").strip()
    if remote_addr in local_hosts or remote_addr.startswith("127."):
        return True

    access_route = request.access_route or []
    for addr in access_route:
        addr = (addr or "").strip()
        if addr in local_hosts or addr.startswith("127."):
            return True

    forwarded_for = request.headers.get("X-Forwarded-For", "")
    for addr in forwarded_for.split(","):
        addr = addr.strip()
        if addr and (addr in local_hosts or addr.startswith("127.")):
            return True

    return False


@app.get("/")
def index():
    """Serve the main web interface."""
    return render_template("index.html", show_seed=is_local_request())


@app.post("/task")
def task():
    """Assign the next available task to the requesting worker."""

    conn = db()
    conn.execute("BEGIN IMMEDIATE")
    cur = conn.cursor()

    counter_row = cur.execute(
        "SELECT value FROM meta WHERE key=?",
        ("task_assignment_counter",),
    ).fetchone()
    try:
        current_count = int(counter_row["value"]) if counter_row else 0
    except (TypeError, ValueError):
        current_count = 0
    next_count = current_count + 1
    refresh_due = next_count % 10 == 0

    task_payload = None

    if refresh_due:
        refresh_candidate = cur.execute(
            """
            SELECT steamAccountId
            FROM players
            WHERE hero_done=1
              AND assigned_to IS NULL
            ORDER BY COALESCE(hero_refreshed_at, '1970-01-01') ASC,
                     steamAccountId ASC
            LIMIT 1
            """
        ).fetchone()
        if refresh_candidate:
            assigned_row = cur.execute(
                """
                UPDATE players
                SET hero_done=0,
                    assigned_to='hero',
                    assigned_at=CURRENT_TIMESTAMP
                WHERE steamAccountId=?
                  AND hero_done=1
                  AND assigned_to IS NULL
                RETURNING steamAccountId
                """,
                (refresh_candidate["steamAccountId"],),
            ).fetchone()
            if assigned_row:
                task_payload = {
                    "type": "fetch_hero_stats",
                    "steamAccountId": int(assigned_row["steamAccountId"]),
                }

    if task_payload is None:
        hero_candidate = cur.execute(
            """
            SELECT steamAccountId
            FROM players
            WHERE hero_done=0
              AND assigned_to IS NULL
            ORDER BY COALESCE(depth, 0) ASC, steamAccountId ASC
            LIMIT 1
            """
        ).fetchone()
        if hero_candidate:
            assigned_row = cur.execute(
                """
                UPDATE players
                SET assigned_to='hero',
                    assigned_at=CURRENT_TIMESTAMP
                WHERE steamAccountId=?
                  AND hero_done=0
                  AND assigned_to IS NULL
                RETURNING steamAccountId
                """,
                (hero_candidate["steamAccountId"],),
            ).fetchone()
            if assigned_row:
                task_payload = {
                    "type": "fetch_hero_stats",
                    "steamAccountId": int(assigned_row["steamAccountId"]),
                }

    if task_payload is None:
        hero_pending = cur.execute(
            "SELECT 1 FROM players WHERE hero_done=0 LIMIT 1"
        ).fetchone()
        if not hero_pending:
            discover_candidate = cur.execute(
                """
                SELECT steamAccountId, depth
                FROM players
                WHERE hero_done=1
                  AND discover_done=0
                  AND (assigned_to IS NULL OR assigned_to='discover')
                ORDER BY COALESCE(depth, 0) ASC, steamAccountId ASC
                LIMIT 1
                """
            ).fetchone()
            if discover_candidate:
                assigned_row = cur.execute(
                    """
                    UPDATE players
                    SET assigned_to='discover',
                        assigned_at=CURRENT_TIMESTAMP
                    WHERE steamAccountId=?
                      AND (assigned_to IS NULL OR assigned_to='discover')
                    RETURNING steamAccountId, depth
                    """,
                    (discover_candidate["steamAccountId"],),
                ).fetchone()
                if assigned_row:
                    depth_value = assigned_row["depth"]
                    task_payload = {
                        "type": "discover_matches",
                        "steamAccountId": int(assigned_row["steamAccountId"]),
                        "depth": int(depth_value) if depth_value is not None else 0,
                    }

    if task_payload:
        cur.execute(
            """
            INSERT INTO meta (key, value)
            VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """,
            ("task_assignment_counter", str(next_count)),
        )

    conn.commit()
    conn.close()
    return jsonify({"task": task_payload})


@app.post("/task/reset")
def reset_task():
    """Release a task so that it can be reassigned after an error."""
    data = request.get_json(force=True) or {}
    try:
        steam_account_id = int(data["steamAccountId"])
    except (KeyError, TypeError, ValueError):
        return (
            jsonify({"status": "error", "message": "steamAccountId is required"}),
            400,
        )

    task_type = data.get("type")
    conn = db()
    cur = conn.cursor()
    if task_type == "fetch_hero_stats":
        cur.execute(
            """
            UPDATE players
            SET hero_done=0,
                hero_refreshed_at=NULL,
                assigned_to=NULL,
                assigned_at=NULL
            WHERE steamAccountId=?
            """,
            (steam_account_id,),
        )
    elif task_type == "discover_matches":
        cur.execute(
            """
            UPDATE players
            SET discover_done=0,
                assigned_to=NULL,
                assigned_at=NULL
            WHERE steamAccountId=?
            """,
            (steam_account_id,),
        )
    else:
        cur.execute(
            """
            UPDATE players
            SET assigned_to=NULL,
                assigned_at=NULL
            WHERE steamAccountId=?
            """,
            (steam_account_id,),
        )
    conn.commit()
    conn.close()
    return jsonify({"status": "ok"})


@app.post("/submit")
def submit():
    """Persist hero statistics returned by the worker and mark the task complete."""
    data = request.get_json(force=True)
    task_type = data.get("type")

    if task_type == "fetch_hero_stats":
        try:
            steam_account_id = int(data["steamAccountId"])
        except (KeyError, TypeError, ValueError):
            return (
                jsonify({"status": "error", "message": "steamAccountId is required"}),
                400,
            )

        heroes = data.get("heroes", [])
        conn = db()
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute(
            "DELETE FROM hero_stats WHERE steamAccountId = ?",
            (steam_account_id,),
        )
        for hero in heroes:
            try:
                hero_id = int(hero["heroId"])
                matches_value = hero.get("matches", hero.get("games"))
                if matches_value is None:
                    continue
                matches = int(matches_value)
                wins = int(hero.get("wins", 0))
            except (KeyError, TypeError, ValueError):
                continue

            cur.execute(
                """
                INSERT INTO hero_stats (steamAccountId, heroId, matches, wins)
                VALUES (?,?,?,?)
                ON CONFLICT(steamAccountId, heroId) DO UPDATE SET
                    matches=excluded.matches,
                    wins=excluded.wins
                """,
                (steam_account_id, hero_id, matches, wins),
            )

            hero_name = HEROES.get(hero_id)
            if not hero_name:
                continue

            cur.execute(
                """
                INSERT INTO best (hero_id, hero_name, player_id, matches, wins)
                VALUES (?,?,?,?,?)
                ON CONFLICT(hero_id) DO UPDATE SET
                    matches=excluded.matches,
                    wins=excluded.wins,
                    player_id=excluded.player_id
                WHERE excluded.matches > best.matches
                """,
                (hero_id, hero_name, steam_account_id, matches, wins),
            )

        cur.execute(
            """
            UPDATE players
            SET hero_done=1,
                hero_refreshed_at=CURRENT_TIMESTAMP,
                assigned_to=NULL,
                assigned_at=NULL
            WHERE steamAccountId=?
            """,
            (steam_account_id,),
        )
        conn.commit()
        conn.close()
        return jsonify({"status": "ok"})

    if task_type == "discover_matches":
        try:
            steam_account_id = int(data["steamAccountId"])
        except (KeyError, TypeError, ValueError):
            return (
                jsonify({"status": "error", "message": "steamAccountId is required"}),
                400,
            )

        discovered_ids = set()
        for value in data.get("discovered", []):
            try:
                discovered_ids.add(int(value))
            except (TypeError, ValueError):
                continue

        conn = db()
        cur = conn.cursor()
        cur.execute("BEGIN")
        parent_row = cur.execute(
            "SELECT depth FROM players WHERE steamAccountId=?",
            (steam_account_id,),
        ).fetchone()
        parent_depth = (
            int(parent_row["depth"])
            if parent_row and parent_row["depth"] is not None
            else 0
        )

        next_depth = parent_depth + 1
        for new_id in discovered_ids:
            if new_id == steam_account_id:
                continue
            cur.execute(
                """
                INSERT OR IGNORE INTO players (
                    steamAccountId,
                    depth,
                    hero_done,
                    discover_done
                )
                VALUES (?,?,0,0)
                """,
                (new_id, next_depth),
            )

        cur.execute(
            """
            UPDATE players
            SET discover_done=1,
                assigned_to=NULL,
                assigned_at=NULL
            WHERE steamAccountId=?
            """,
            (steam_account_id,),
        )
        conn.commit()
        conn.close()
        return jsonify({"status": "ok"})

    return (
        jsonify({"status": "error", "message": "Unknown submit type"}),
        400,
    )


@app.get("/progress")
def progress():
    conn = db()
    total = conn.execute("SELECT COUNT(*) AS c FROM players").fetchone()["c"]
    hero_done = (
        conn.execute(
            "SELECT COUNT(*) AS c FROM players WHERE hero_done=1"
        ).fetchone()["c"]
    )
    discover_done = (
        conn.execute(
            "SELECT COUNT(*) AS c FROM players WHERE discover_done=1"
        ).fetchone()["c"]
    )
    conn.close()
    return jsonify(
        {
            "players_total": total,
            "hero_done": hero_done,
            "discover_done": discover_done,
        }
    )


@app.get("/seed")
def seed():
    if not is_local_request():
        return Response("Forbidden", status=403)

    try:
        start = int(request.args.get("start"))
        end = int(request.args.get("end"))
    except (TypeError, ValueError):
        return Response("Use /seed?start=1&end=100", status=400)

    if end < start:
        return Response("End must be >= start", status=400)

    conn = db()
    cur = conn.cursor()
    cur.execute("BEGIN")
    for pid in range(start, end + 1):
        cur.execute(
            """
            INSERT OR IGNORE INTO players (
                steamAccountId,
                depth,
                hero_done,
                discover_done
            )
            VALUES (?,?,0,0)
            """,
            (pid, 0),
        )
    conn.commit()
    conn.close()
    return jsonify({"seeded": [start, end]})


@app.get("/leaderboards/<hero_slug>")
def hero_leaderboard(hero_slug: str):
    """Display the leaderboard for the requested hero."""

    slug = hero_slug.strip().replace(" ", "_").lower()
    hero_entry = HERO_SLUGS.get(slug)
    if not hero_entry:
        abort(404)

    hero_id, hero_name = hero_entry
    conn = db()
    rows = conn.execute(
        """
        SELECT steamAccountId, matches, wins
        FROM hero_stats
        WHERE heroId=?
        ORDER BY matches DESC, wins DESC, steamAccountId ASC
        LIMIT 100
        """,
        (hero_id,),
    ).fetchall()
    conn.close()

    players = [
        {
            "steamAccountId": row["steamAccountId"],
            "matches": row["matches"],
            "wins": row["wins"],
        }
        for row in rows
    ]

    return render_template(
        "leaderboard.html",
        hero_name=hero_name,
        hero_slug=slug,
        players=players,
    )


@app.get("/best")
def best():
    conn = db()
    rows = conn.execute("SELECT * FROM best ORDER BY matches DESC").fetchall()
    conn.close()
    return jsonify([dict(row) for row in rows])


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=80, debug=True)
