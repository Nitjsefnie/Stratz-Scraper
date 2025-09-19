#!/usr/bin/env python3
"""Flask application providing the Stratz distributed scraping UI and API."""
from flask import Flask, Response, jsonify, render_template, request

from database import db, ensure_schema, release_incomplete_assignments
from heroes import HEROES

app = Flask(__name__, static_folder="static", template_folder="templates")
ensure_schema()
release_incomplete_assignments()

RERUN_INTERVAL = 10


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
    """Assign the next available player id to the requesting worker."""
    conn = db()
    conn.execute("BEGIN IMMEDIATE")

    counter_row = conn.execute(
        "UPDATE meta SET value = value + 1 WHERE key='task_counter' RETURNING value"
    ).fetchone()
    if counter_row is None:
        conn.execute(
            "INSERT INTO meta (key, value) VALUES ('task_counter', 1)"
        )
        counter = 1
    else:
        counter = int(counter_row["value"])

    candidate_id = None
    if RERUN_INTERVAL and counter % RERUN_INTERVAL == 0:
        rerun_row = conn.execute(
            """
            SELECT id
            FROM players
            WHERE done=1
            ORDER BY assigned_at ASC, id ASC
            LIMIT 1
            """
        ).fetchone()
        if rerun_row:
            candidate_id = rerun_row["id"]
            conn.execute(
                "DELETE FROM hero_stats WHERE player_id=?",
                (candidate_id,),
            )

    if candidate_id is None:
        row = conn.execute(
            """
            SELECT id
            FROM players
            WHERE done=0 AND assigned_to IS NULL
            ORDER BY id
            LIMIT 1
            """
        ).fetchone()
        candidate_id = row["id"] if row else None

    assigned_row = None
    if candidate_id is not None:
        assigned_row = conn.execute(
            """
            UPDATE players
            SET done=0,
                assigned_to='browser',
                assigned_at=strftime('%s','now')
            WHERE id=? AND assigned_to IS NULL
            RETURNING id
            """,
            (candidate_id,),
        ).fetchone()

    conn.commit()
    conn.close()
    return jsonify({"task": assigned_row["id"] if assigned_row else None})


@app.post("/task/reset")
def reset_task():
    """Release a task so that it can be reassigned after an error."""
    data = request.get_json(force=True) or {}
    try:
        player_id = int(data["player_id"])
    except (KeyError, TypeError, ValueError):
        return jsonify({"status": "error", "message": "player_id is required"}), 400

    conn = db()
    cur = conn.cursor()
    cur.execute("DELETE FROM hero_stats WHERE player_id = ?", (player_id,))
    cur.execute(
        "UPDATE players SET assigned_to=NULL, assigned_at=NULL, done=0 WHERE id=?",
        (player_id,),
    )
    conn.commit()
    conn.close()
    return jsonify({"status": "ok"})


@app.post("/submit")
def submit():
    """Persist hero statistics returned by the worker and mark the task complete."""
    data = request.get_json(force=True)
    pid = int(data["player_id"])
    heroes = data.get("heroes", [])

    conn = db()
    cur = conn.cursor()
    for hero in heroes:
        hid = int(hero["hero_id"])
        matches = int(hero["games"])
        wins = int(hero.get("wins", 0))
        name = HEROES.get(hid)
        if not name:
            continue
        cur.execute(
            """
            INSERT INTO hero_stats (player_id, hero_id, hero_name, matches, wins)
            VALUES (?,?,?,?,?)
            ON CONFLICT(player_id, hero_id) DO UPDATE SET
                matches=excluded.matches,
                wins=excluded.wins
            """,
            (pid, hid, name, matches, wins),
        )
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
            (hid, name, pid, matches, wins),
        )

    cur.execute("UPDATE players SET done=1, assigned_to=NULL WHERE id=?", (pid,))
    conn.commit()
    conn.close()
    return jsonify({"status": "ok"})


@app.get("/progress")
def progress():
    conn = db()
    total = conn.execute("SELECT COUNT(*) AS c FROM players").fetchone()["c"]
    done = conn.execute("SELECT COUNT(*) AS c FROM players WHERE done=1").fetchone()["c"]
    conn.close()
    return jsonify({"total": total, "done": done})


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
        cur.execute("INSERT OR IGNORE INTO players (id) VALUES (?)", (pid,))
    conn.commit()
    conn.close()
    return jsonify({"seeded": [start, end]})


@app.get("/best")
def best():
    conn = db()
    rows = conn.execute("SELECT * FROM best ORDER BY matches DESC").fetchall()
    conn.close()
    return jsonify([dict(row) for row in rows])


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
