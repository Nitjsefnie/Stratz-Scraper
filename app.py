#!/usr/bin/env python3
"""Flask application providing the Stratz distributed scraping UI and API."""
from flask import Flask, Response, jsonify, render_template, request

from database import db, ensure_schema, release_incomplete_assignments
from heroes import HEROES

app = Flask(__name__, static_folder="static", template_folder="templates")
ensure_schema()
release_incomplete_assignments()


@app.get("/")
def index():
    """Serve the main web interface."""
    return render_template("index.html")


@app.post("/task")
def task():
    """Assign the next available player id to the requesting worker."""
    conn = db()
    cur = conn.execute(
        """
        UPDATE players
        SET assigned_to='browser', assigned_at=strftime('%s','now')
        WHERE id = (
            SELECT id
            FROM players
            WHERE done=0 AND assigned_to IS NULL
            LIMIT 1
        )
        RETURNING id
        """
    )
    row = cur.fetchone()
    conn.commit()
    conn.close()
    return jsonify({"task": row["id"] if row else None})


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
