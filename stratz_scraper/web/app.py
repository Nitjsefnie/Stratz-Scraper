"""Flask application factory and route definitions."""

from __future__ import annotations

from typing import Iterable, List

from flask import Flask, Response, abort, jsonify, render_template, request

from ..database import (
    close_cached_connections,
    db_connection,
    retryable_execute,
    release_incomplete_assignments,
)
from ..heroes import HEROES
from .assignment import assign_next_task, ensure_assignment_cleanup_scheduler
from .config import STATIC_DIR, TEMPLATE_DIR
from .leaderboard import fetch_best_payload, fetch_hero_leaderboard
from .progress import fetch_progress
from .request_utils import is_local_request
from .seed import seed_players
from .submissions import submit_discover_submission, submit_hero_submission
from .tasks import reset_player_task

__all__ = ["create_app"]


def _extract_hero_rows(steam_account_id: int, heroes_payload: Iterable[dict]) -> tuple[List[tuple[int, int, int, int]], List[tuple[int, str, int, int, int]]]:
    hero_stats_rows: List[tuple[int, int, int, int]] = []
    best_rows: List[tuple[int, str, int, int, int]] = []
    for hero in heroes_payload:
        try:
            hero_id = int(hero["heroId"])
            matches_value = hero.get("matches", hero.get("games"))
            if matches_value is None:
                continue
            matches = int(matches_value)
            wins = int(hero.get("wins", 0))
        except (KeyError, TypeError, ValueError):
            continue
        hero_stats_rows.append((steam_account_id, hero_id, matches, wins))
        hero_name = HEROES.get(hero_id)
        if hero_name:
            best_rows.append((hero_id, hero_name, steam_account_id, matches, wins))
    return hero_stats_rows, best_rows


def _extract_discovered_counts(values: Iterable[object]) -> List[tuple[int, int]]:
    aggregated: dict[int, int] = {}
    order: List[int] = []
    for value in values:
        candidate_id = None
        count_value = 1
        if isinstance(value, dict):
            candidate_id = value.get("steamAccountId")
            if candidate_id is None:
                candidate_id = value.get("id")
            count_raw = value.get("count")
            if count_raw is None:
                count_raw = value.get("seenCount")
            if count_raw is not None:
                try:
                    count_value = int(count_raw)
                except (TypeError, ValueError):
                    count_value = 0
        else:
            candidate_id = value
        try:
            candidate_id = int(candidate_id)
        except (TypeError, ValueError):
            continue
        if candidate_id <= 0:
            continue
        if count_value is None:
            count_value = 0
        if not isinstance(count_value, int):
            try:
                count_value = int(count_value)
            except (TypeError, ValueError):
                count_value = 0
        if count_value <= 0:
            continue
        if candidate_id not in aggregated:
            aggregated[candidate_id] = count_value
            order.append(candidate_id)
        else:
            aggregated[candidate_id] += count_value
    return [(pid, aggregated[pid]) for pid in order]


def _resolve_next_depth(data: dict, assignment_row) -> int:
    provided_next_depth = data.get("nextDepth")
    if provided_next_depth is not None:
        try:
            return int(provided_next_depth)
        except (TypeError, ValueError):
            pass
    provided_depth = data.get("depth")
    parent_depth_value = None
    if provided_depth is not None:
        try:
            parent_depth_value = int(provided_depth)
        except (TypeError, ValueError):
            parent_depth_value = None
    if parent_depth_value is None:
        if assignment_row and assignment_row["depth"] is not None:
            try:
                parent_depth_value = int(assignment_row["depth"])
            except (TypeError, ValueError):
                parent_depth_value = 0
        else:
            parent_depth_value = 0
    return parent_depth_value + 1


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
            hero_stats_rows, best_rows = _extract_hero_rows(
                steam_account_id,
                data.get("heroes", []),
            )
            with db_connection(write=True) as conn:
                cur = conn.cursor()
                update_cursor = retryable_execute(
                    cur,
                    """
                    UPDATE players
                    SET hero_done=1,
                        assigned_to=NULL,
                        assigned_at=NULL
                    WHERE steamAccountId=?
                    """,
                    (steam_account_id,),
                )
            if update_cursor.rowcount == 0:
                return (
                    jsonify({"status": "error", "message": "Player not found"}),
                    404,
                )
            submit_hero_submission(
                steam_account_id,
                hero_stats_rows,
                best_rows,
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
            discovered_counts = _extract_discovered_counts(data.get("discovered", []))
            with db_connection(write=True) as conn:
                cur = conn.cursor()
                assignment_row = retryable_execute(
                    cur,
                    "SELECT depth FROM players WHERE steamAccountId=?",
                    (steam_account_id,),
                ).fetchone()
                next_depth_value = _resolve_next_depth(data, assignment_row)
                update_cursor = retryable_execute(
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
            if update_cursor.rowcount == 0:
                return (
                    jsonify({"status": "error", "message": "Player not found"}),
                    404,
                )
            if discovered_counts:
                submit_discover_submission(
                    steam_account_id,
                    discovered_counts,
                    next_depth_value,
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
