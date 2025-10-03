"""Flask application factory and route definitions."""

from __future__ import annotations


from flask import Flask, Response, abort, jsonify, render_template, request

from ..database import (
    close_cached_connections,
    db_connection,
    retryable_execute,
    release_incomplete_assignments,
)
from .assignment import assign_next_task, ensure_assignment_cleanup_scheduler
from .config import STATIC_DIR, TEMPLATE_DIR
from .leaderboard import fetch_best_payload, fetch_hero_leaderboard
from .progress import fetch_progress
from .request_utils import is_local_request
from .seed import seed_players
from .submissions import submit_discover_submission, submit_hero_submission
from .tasks import reset_player_task

__all__ = ["create_app"]


def create_app() -> Flask:
    app = Flask(
        __name__,
        static_folder=str(STATIC_DIR),
        template_folder=str(TEMPLATE_DIR),
    )

    release_incomplete_assignments()
    ensure_assignment_cleanup_scheduler()

    @app.teardown_appcontext
    def _teardown_connections(exception: object | None) -> None:
        close_cached_connections()

    @app.get("/")
    def index() -> str:
        return render_template("index.html", show_seed=is_local_request())

    @app.post("/task")
    def task():
        task_payload = assign_next_task()
        return jsonify({"task": task_payload})

    @app.post("/task/reset")
    def reset_task():
        data = request.get_json(force=True) or {}
        try:
            steam_account_id = int(data["steamAccountId"])
        except (KeyError, TypeError, ValueError):
            return jsonify({"status": "error", "message": "steamAccountId is required"}), 400
        task_type = data.get("type")
        if not reset_player_task(steam_account_id, task_type):
            return (
                jsonify({"status": "error", "message": "Player not found"}),
                404,
            )
        return jsonify({"status": "ok"})

    @app.post("/submit")
    def submit():
        data = request.get_json(force=True)
        task_type = data.get("type")
        request_new_task = data.get("task") is True
        if task_type == "fetch_hero_stats":
            try:
                steam_account_id = int(data["steamAccountId"])
            except (KeyError, TypeError, ValueError):
                return jsonify({"status": "error", "message": "steamAccountId is required"}), 400
            heroes_payload = data.get("heroes", [])
            with db_connection(write=True) as conn:
                cur = conn.cursor()
                retryable_execute(
                    cur,
                    """
                    UPDATE players
                    SET hero_done=1,
                        assigned_to=NULL,
                        assigned_at=NULL,
                        hero_refreshed_at=CURRENT_TIMESTAMP
                    WHERE steamAccountId=?
                    """,
                    (steam_account_id,),
                )
                if cur.rowcount == 0:
                    return (
                        jsonify({"status": "error", "message": "Player not found"}),
                        404,
                    )
            submit_hero_submission(
                steam_account_id,
                heroes_payload,
            )
            next_task = assign_next_task() if request_new_task else None
            response_payload = {"status": "ok"}
            if request_new_task:
                response_payload["task"] = next_task
            return jsonify(response_payload)
        if task_type == "discover_matches":
            try:
                steam_account_id = int(data["steamAccountId"])
            except (KeyError, TypeError, ValueError):
                return jsonify({"status": "error", "message": "steamAccountId is required"}), 400
            provided_next_depth = None
            provided_depth = None
            next_depth_raw = data.get("nextDepth")
            if next_depth_raw is not None:
                try:
                    provided_next_depth = int(next_depth_raw)
                except (TypeError, ValueError):
                    provided_next_depth = None
            depth_raw = data.get("depth")
            if depth_raw is not None:
                try:
                    provided_depth = int(depth_raw)
                except (TypeError, ValueError):
                    provided_depth = None
            discovered_payload = data.get("discovered", [])
            assignment_depth = None
            with db_connection(write=True) as conn:
                cur = conn.cursor()
                assignment_row = retryable_execute(
                    cur,
                    "SELECT depth FROM players WHERE steamAccountId=?",
                    (steam_account_id,),
                ).fetchone()
                if assignment_row is None:
                    return (
                        jsonify({"status": "error", "message": "Player not found"}),
                        404,
                    )
                assignment_depth = assignment_row["depth"] if assignment_row is not None else None
                retryable_execute(
                    cur,
                    """
                    UPDATE players
                    SET discover_done=1,
                        assigned_to=NULL,
                        assigned_at=NULL
                    WHERE steamAccountId=?
                    """,
                    (steam_account_id,),
                )
                if cur.rowcount == 0:
                    return (
                        jsonify({"status": "error", "message": "Player not found"}),
                        404,
                    )
            submit_discover_submission(
                steam_account_id,
                discovered_payload,
                provided_next_depth,
                provided_depth,
                assignment_depth,
            )
            next_task = assign_next_task() if request_new_task else None
            response_payload = {"status": "ok"}
            if request_new_task:
                response_payload["task"] = next_task
            return jsonify(response_payload)
        return jsonify({"status": "error", "message": "Unknown submit type"}), 400

    @app.get("/progress")
    def progress():
        return jsonify(fetch_progress())

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
        seed_players(start, end)
        return jsonify({"seeded": [start, end]})

    @app.get("/leaderboards/<hero_slug>")
    def hero_leaderboard(hero_slug: str):
        hero_payload = fetch_hero_leaderboard(hero_slug)
        if hero_payload is None:
            abort(404)
        hero_name, slug, players = hero_payload
        return render_template(
            "leaderboard.html",
            hero_name=hero_name,
            hero_slug=slug,
            players=players,
        )

    @app.get("/best")
    def best():
        return jsonify(fetch_best_payload())

    return app
