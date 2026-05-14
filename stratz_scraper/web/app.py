"""Flask application factory and route definitions."""

from __future__ import annotations

from datetime import datetime, timezone

from flask import Flask, Response, abort, jsonify, render_template, request

from ..database import (
    close_cached_connections,
    db_connection,
    retryable_execute,
    release_incomplete_assignments,
)
from .assignment import (
    ASSIGNMENT_RETRY_INTERVAL,
    assign_next_task,
    ensure_assignment_cleanup_scheduler,
)
from .config import STATIC_DIR, TEMPLATE_DIR
from .leaderboard import (
    fetch_best_payload,
    fetch_hero_leaderboard,
    fetch_overall_leaderboard,
)
from .leaderboard_refresh import ensure_leaderboard_refresher
from .progress import (
    ensure_progress_snapshotter,
    fetch_progress,
    list_progress_snapshots,
)
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
    ensure_progress_snapshotter()
    ensure_leaderboard_refresher()

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
        raw_ids = data.get("steamAccountIds")
        steam_account_ids: list[int] = []
        if isinstance(raw_ids, list):
            for raw_id in raw_ids:
                try:
                    parsed = int(raw_id)
                except (TypeError, ValueError):
                    continue
                if parsed > 0:
                    steam_account_ids.append(parsed)
        if not steam_account_ids:
            try:
                steam_account_id = int(data["steamAccountId"])
            except (KeyError, TypeError, ValueError):
                return (
                    jsonify({"status": "error", "message": "steamAccountId is required"}),
                    400,
                )
            if steam_account_id > 0:
                steam_account_ids = [steam_account_id]
        task_type = data.get("type")
        any_success = False
        for steam_account_id in steam_account_ids:
            if reset_player_task(steam_account_id, task_type):
                any_success = True
        if not any_success:
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
            players_payload = []
            raw_players = data.get("players")
            if isinstance(raw_players, list):
                seen: set[int] = set()
                for entry in raw_players:
                    if not isinstance(entry, dict):
                        continue
                    try:
                        steam_account_id = int(entry["steamAccountId"])
                    except (KeyError, TypeError, ValueError):
                        continue
                    if steam_account_id <= 0 or steam_account_id in seen:
                        continue
                    heroes_payload = entry.get("heroes", [])
                    players_payload.append((steam_account_id, heroes_payload))
                    seen.add(steam_account_id)
            if not players_payload:
                try:
                    steam_account_id = int(data["steamAccountId"])
                except (KeyError, TypeError, ValueError):
                    return (
                        jsonify({"status": "error", "message": "steamAccountId is required"}),
                        400,
                    )
                if steam_account_id <= 0:
                    return (
                        jsonify({"status": "error", "message": "steamAccountId must be positive"}),
                        400,
                    )
                heroes_payload = data.get("heroes", [])
                players_payload.append((steam_account_id, heroes_payload))
            next_task = None
            successful_payloads: list[tuple[int, object]] = []
            try:
                with db_connection(write=True) as conn:
                    cur = conn.cursor()
                    for steam_account_id, heroes_payload in players_payload:
                        update_cursor = retryable_execute(
                            cur,
                            """
                            UPDATE players
                            SET hero_done=TRUE,
                                assigned_to=NULL,
                                assigned_at=NULL,
                                hero_refreshed_at=CURRENT_TIMESTAMP
                            WHERE steamAccountId=%s
                            """,
                            (steam_account_id,),
                            retry_interval=ASSIGNMENT_RETRY_INTERVAL,
                        )
                        updated_rows = (
                            update_cursor.rowcount
                            if update_cursor.rowcount is not None
                            else 0
                        )
                        if updated_rows == 0:
                            raise LookupError
                        successful_payloads.append((steam_account_id, heroes_payload))
                    if request_new_task:
                        next_task = assign_next_task(connection=conn)
            except LookupError:
                return (
                    jsonify({"status": "error", "message": "Player not found"}),
                    404,
                )
            for steam_account_id, heroes_payload in successful_payloads:
                submit_hero_submission(
                    steam_account_id,
                    heroes_payload,
                )
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
            highest_match_id_raw = data.get("highestMatchId")
            highest_match_id: int | None
            if highest_match_id_raw is None:
                highest_match_id = None
            else:
                try:
                    highest_match_id = int(highest_match_id_raw)
                except (TypeError, ValueError):
                    highest_match_id = None
            if highest_match_id is not None and highest_match_id < 0:
                highest_match_id = None
            raw_discovered = data.pop("discovered", [])
            if isinstance(raw_discovered, list):
                discovered_payload = raw_discovered
            else:
                discovered_payload = []
            has_discovered_accounts = False
            if discovered_payload:
                seen_candidates: set[int] = set()
                for entry in discovered_payload:
                    candidate: object | None
                    if isinstance(entry, dict):
                        candidate = entry.get("steamAccountId")
                        if candidate is None:
                            candidate = entry.get("id")
                    else:
                        candidate = entry
                    try:
                        candidate_id = int(candidate) if candidate is not None else None
                    except (TypeError, ValueError):
                        continue
                    if candidate_id is None:
                        continue
                    if candidate_id <= 0 or candidate_id == steam_account_id:
                        continue
                    if candidate_id in seen_candidates:
                        continue
                    seen_candidates.add(candidate_id)
                    has_discovered_accounts = True
                    break
            retain_assignment = data.get("retainAssignment") is True
            assignment_depth = None
            next_task = None
            with db_connection(write=True) as conn:
                cur = conn.cursor()
                set_clauses = [
                    "discover_done=TRUE",
                    "full_write_done=%s",
                ]
                set_parameters: list[object] = [not has_discovered_accounts]
                if not retain_assignment:
                    set_clauses.extend(["assigned_to=NULL", "assigned_at=NULL"])
                set_clauses.append(
                    "highest_match_id = CASE\n"
                    "                        WHEN CAST(%s AS BIGINT) IS NULL THEN highest_match_id\n"
                    "                        WHEN highest_match_id IS NULL THEN CAST(%s AS BIGINT)\n"
                    "                        ELSE GREATEST(highest_match_id, CAST(%s AS BIGINT))\n"
                    "                    END"
                )
                update_query = (
                    "UPDATE players\n"
                    "SET "
                    + ",\n        ".join(set_clauses)
                    + "\nWHERE steamAccountId=%s\nRETURNING depth"
                )
                update_row = retryable_execute(
                    cur,
                    update_query,
                    (
                        *set_parameters,
                        highest_match_id,
                        highest_match_id,
                        highest_match_id,
                        steam_account_id,
                    ),
                    retry_interval=ASSIGNMENT_RETRY_INTERVAL,
                ).fetchone()
                if update_row is None:
                    return (
                        jsonify({"status": "error", "message": "Player not found"}),
                        404,
                    )
                assignment_depth = update_row["depth"] if update_row is not None else None
                if request_new_task:
                    next_task = assign_next_task(connection=conn)
            if has_discovered_accounts:
                submit_discover_submission(
                    steam_account_id,
                    discovered_payload,
                    provided_next_depth,
                    provided_depth,
                    assignment_depth,
                )
            response_payload = {"status": "ok"}
            if request_new_task:
                response_payload["task"] = next_task
            return jsonify(response_payload)
        return jsonify({"status": "error", "message": "Unknown submit type"}), 400

    @app.get("/progress")
    def progress():
        return jsonify(fetch_progress())

    def _parse_time_param(value: str | None) -> datetime | None:
        if value is None or value.strip() == "":
            return None
        cleaned = value.strip()
        if cleaned.endswith("Z"):
            cleaned = cleaned[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(cleaned)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    def _format_datetime_local(value: datetime | None) -> str:
        if value is None:
            return ""
        localized = value.astimezone(timezone.utc).replace(tzinfo=None)
        return localized.isoformat(timespec="minutes")

    @app.get("/progress/graph")
    def progress_graph() -> str:
        start_raw = request.args.get("start")
        end_raw = request.args.get("end")

        start_dt = _parse_time_param(start_raw)
        if start_raw and start_dt is None:
            return Response(
                "Invalid 'start' parameter. Use an ISO 8601 timestamp.", status=400
            )

        end_dt = _parse_time_param(end_raw)
        if end_raw and end_dt is None:
            return Response(
                "Invalid 'end' parameter. Use an ISO 8601 timestamp.", status=400
            )

        if start_dt and end_dt and end_dt < start_dt:
            return Response("'end' must be greater than or equal to 'start'.", status=400)

        snapshots = list_progress_snapshots(start=start_dt, end=end_dt)
        return render_template(
            "progress_graph.html",
            snapshots=snapshots,
            start_value=_format_datetime_local(start_dt) if start_raw else "",
            end_value=_format_datetime_local(end_dt) if end_raw else "",
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
        seed_players(start, end)
        return jsonify({"seeded": [start, end]})

    @app.get("/leaderboards")
    @app.get("/leaderboards/")
    def leaderboards():
        players = fetch_overall_leaderboard()
        return render_template(
            "leaderboard.html",
            hero_name="Overall",
            hero_slug=None,
            players=players,
            heading="Overall Leaderboard",
            page_title="Overall Leaderboard",
            description="Top 100 players by matches played across all heroes.",
        )

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
            heading=f"{hero_name} Leaderboard",
            page_title=f"{hero_name} Leaderboard",
            description=f"Top 100 players by matches played on {hero_name}.",
        )

    @app.get("/best")
    def best():
        return jsonify(fetch_best_payload())

    return app
