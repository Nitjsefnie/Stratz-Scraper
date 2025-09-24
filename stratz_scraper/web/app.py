from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
import traceback

from flask import Flask, Response, abort, jsonify, render_template, request

from ..database import (
    db_connection,
    retryable_execute,
    retryable_executemany,
    release_incomplete_assignments,
)
from ..heroes import HEROES, HERO_SLUGS, hero_slug

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
TEMPLATE_DIR = BASE_DIR / "templates"

ASSIGNMENT_CLEANUP_KEY = "last_assignment_cleanup"
ASSIGNMENT_CLEANUP_INTERVAL = timedelta(seconds=60)

BACKGROUND_EXECUTOR = ThreadPoolExecutor(max_workers=2)


def _parse_sqlite_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def record_task_duration(
    cur, steam_account_id: int, task_type: str, assigned_at_value: str | None
) -> float | None:
    submitted_at = datetime.now(timezone.utc)
    assigned_at = _parse_sqlite_timestamp(assigned_at_value)
    duration_seconds = None
    if assigned_at is not None:
        duration_seconds = (submitted_at - assigned_at).total_seconds()
    retryable_execute(
        cur,
        """
        INSERT INTO task_durations (
            steamAccountId,
            task_type,
            assigned_at,
            submitted_at,
            duration_seconds
        ) VALUES (?, ?, ?, ?, ?)
        """,
        (
            steam_account_id,
            task_type,
            assigned_at.isoformat() if assigned_at is not None else assigned_at_value,
            submitted_at.isoformat(),
            duration_seconds,
        ),
    )
    if duration_seconds is not None:
        print(
            f"[task-duration] {task_type} for {steam_account_id} took {duration_seconds:.2f}s",
            flush=True,
        )
    else:
        print(
            f"[task-duration] {task_type} for {steam_account_id} has no assignment timestamp",
            flush=True,
        )
    return duration_seconds


def _unmark_hero_task(steam_account_id: int) -> None:
    try:
        with db_connection(write=True) as conn:
            cur = conn.cursor()
            retryable_execute(
                cur,
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
    except Exception:
        traceback.print_exc()


def _unmark_discover_task(steam_account_id: int) -> None:
    try:
        with db_connection(write=True) as conn:
            cur = conn.cursor()
            retryable_execute(
                cur,
                """
                UPDATE players
                SET discover_done=0,
                    assigned_to=NULL,
                    assigned_at=NULL
                WHERE steamAccountId=?
                """,
                (steam_account_id,),
            )
    except Exception:
        traceback.print_exc()


def _process_hero_submission(
    steam_account_id: int,
    hero_stats_rows: list[tuple[int, int, int, int]],
    best_rows: list[tuple[int, str, int, int, int]],
    assigned_at_value: str | None,
) -> None:
    try:
        with db_connection(write=True) as conn:
            cur = conn.cursor()
            retryable_execute(
                cur,
                "DELETE FROM hero_stats WHERE steamAccountId = ?",
                (steam_account_id,),
            )
            if hero_stats_rows:
                retryable_executemany(
                    cur,
                    """
                    INSERT INTO hero_stats (steamAccountId, heroId, matches, wins)
                    VALUES (?,?,?,?)
                    """,
                    hero_stats_rows,
                )
            if best_rows:
                retryable_executemany(
                    cur,
                    """
                    INSERT INTO best (hero_id, hero_name, player_id, matches, wins)
                    VALUES (?,?,?,?,?)
                    ON CONFLICT(hero_id) DO UPDATE SET
                        matches=excluded.matches,
                        wins=excluded.wins,
                        player_id=excluded.player_id
                    WHERE excluded.matches > best.matches
                    """,
                    best_rows,
                )
            retryable_execute(
                cur,
                """
                UPDATE players
                SET hero_done=1,
                    hero_refreshed_at=CURRENT_TIMESTAMP
                WHERE steamAccountId=?
                """,
                (steam_account_id,),
            )
            record_task_duration(
                cur,
                steam_account_id,
                "fetch_hero_stats",
                assigned_at_value,
            )
    except Exception:
        print(
            f"[submit-background] failed to process hero stats for {steam_account_id}",
            flush=True,
        )
        traceback.print_exc()
        _unmark_hero_task(steam_account_id)


def _process_discover_submission(
    steam_account_id: int,
    discovered_ids: list[int],
    next_depth_value: int,
    assigned_at_value: str | None,
) -> None:
    try:
        with db_connection(write=True) as conn:
            cur = conn.cursor()
            child_rows = [
                (new_id, next_depth_value)
                for new_id in discovered_ids
                if new_id != steam_account_id
            ]
            if child_rows:
                retryable_executemany(
                    cur,
                    """
                    INSERT INTO players (
                        steamAccountId,
                        depth,
                        hero_done,
                        discover_done
                    )
                    VALUES (?,?,0,0)
                    ON CONFLICT(steamAccountId) DO UPDATE SET
                        depth=excluded.depth
                    """,
                    child_rows,
                )
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
            record_task_duration(
                cur,
                steam_account_id,
                "discover_matches",
                assigned_at_value,
            )
    except Exception:
        print(
            f"[submit-background] failed to process discovery for {steam_account_id}",
            flush=True,
        )
        traceback.print_exc()
        _unmark_discover_task(steam_account_id)


def maybe_run_assignment_cleanup(conn) -> bool:
    cur = conn.cursor()
    now = datetime.now(timezone.utc)
    last_cleanup_row = cur.execute(
        "SELECT value FROM meta WHERE key=?",
        (ASSIGNMENT_CLEANUP_KEY,),
    ).fetchone()
    if last_cleanup_row:
        try:
            last_cleanup = datetime.fromisoformat(last_cleanup_row["value"])
        except (TypeError, ValueError):
            pass
        else:
            if last_cleanup.tzinfo is None:
                last_cleanup = last_cleanup.replace(tzinfo=timezone.utc)
            if now - last_cleanup < ASSIGNMENT_CLEANUP_INTERVAL:
                return False
    release_incomplete_assignments(existing=conn)
    retryable_execute(
        cur,
        """
        INSERT INTO meta (key, value)
        VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value
        """,
        (ASSIGNMENT_CLEANUP_KEY, now.isoformat()),
    )
    return True


def assign_next_task(*, run_cleanup: bool = True) -> dict | None:
    task_payload: dict | None = None
    should_checkpoint = False

    with db_connection(write=True) as conn:
        if run_cleanup:
            maybe_run_assignment_cleanup(conn)
        cur = conn.cursor()

        def assign_discovery() -> dict | None:
            assigned = retryable_execute(
                cur,
                """
                WITH candidate AS (
                    SELECT steamAccountId, depth
                    FROM players
                    WHERE hero_done=1
                      AND discover_done=0
                      AND assigned_to IS NULL
                    ORDER BY COALESCE(depth, 0) ASC, steamAccountId ASC
                    LIMIT 1
                )
                UPDATE players
                SET assigned_to='discover',
                    assigned_at=CURRENT_TIMESTAMP
                WHERE steamAccountId IN (SELECT steamAccountId FROM candidate)
                  AND assigned_to IS NULL
                RETURNING steamAccountId, depth
                """,
            ).fetchone()
            if not assigned:
                assigned = retryable_execute(
                    cur,
                    """
                    WITH candidate AS (
                        SELECT steamAccountId, depth
                        FROM players
                        WHERE hero_done=1
                          AND discover_done=0
                          AND assigned_to='discover'
                        ORDER BY COALESCE(depth, 0) ASC, steamAccountId ASC
                        LIMIT 1
                    )
                    UPDATE players
                    SET assigned_to='discover',
                        assigned_at=CURRENT_TIMESTAMP
                    WHERE steamAccountId IN (SELECT steamAccountId FROM candidate)
                      AND assigned_to='discover'
                    RETURNING steamAccountId, depth
                    """,
                ).fetchone()
            if not assigned:
                return None
            depth_value = assigned["depth"]
            return {
                "type": "discover_matches",
                "steamAccountId": int(assigned["steamAccountId"]),
                "depth": int(depth_value) if depth_value is not None else 0,
            }

        def restart_discovery_cycle() -> bool:
            retryable_execute(
                cur,
                """
                UPDATE players
                SET discover_done=0,
                    depth=CASE WHEN depth=0 THEN 0 ELSE NULL END,
                    assigned_at=CASE WHEN assigned_to='discover' THEN NULL ELSE assigned_at END,
                    assigned_to=CASE WHEN assigned_to='discover' THEN NULL ELSE assigned_to END
                """,
            )
            return True

        counter_row = cur.execute(
            "SELECT value FROM meta WHERE key=?",
            ("task_assignment_counter",),
        ).fetchone()
        try:
            current_count = int(counter_row["value"]) if counter_row else 0
        except (TypeError, ValueError):
            current_count = 0
        loop_count = current_count
        while True:
            next_count = loop_count + 1
            refresh_due = next_count % 10 == 0
            discovery_due = next_count % 100 == 0
            checkpoint_due = next_count % 10000 == 0
            should_truncate_wal = False

            candidate_payload = None

            if discovery_due:
                candidate_payload = assign_discovery()
                if candidate_payload is None and restart_discovery_cycle():
                    should_truncate_wal = True
                    candidate_payload = assign_discovery()

            if candidate_payload is None and refresh_due:
                assigned_row = retryable_execute(
                    cur,
                    """
                    WITH candidate AS (
                        SELECT steamAccountId
                        FROM players
                        WHERE hero_done=1
                          AND assigned_to IS NULL
                        ORDER BY COALESCE(hero_refreshed_at, '1970-01-01') ASC,
                                 steamAccountId ASC
                        LIMIT 1
                    )
                    UPDATE players
                    SET hero_done=0,
                        assigned_to='hero',
                        assigned_at=CURRENT_TIMESTAMP
                    WHERE steamAccountId IN (SELECT steamAccountId FROM candidate)
                      AND hero_done=1
                      AND assigned_to IS NULL
                    RETURNING steamAccountId
                    """,
                ).fetchone()
                if assigned_row:
                    candidate_payload = {
                        "type": "fetch_hero_stats",
                        "steamAccountId": int(assigned_row["steamAccountId"]),
                    }

            if candidate_payload is None:
                assigned_row = retryable_execute(
                    cur,
                    """
                    WITH candidate AS (
                        SELECT steamAccountId
                        FROM players
                        WHERE hero_done=0
                          AND assigned_to IS NULL
                        ORDER BY COALESCE(depth, 0) ASC, steamAccountId ASC
                        LIMIT 1
                    )
                    UPDATE players
                    SET assigned_to='hero',
                        assigned_at=CURRENT_TIMESTAMP
                    WHERE steamAccountId IN (SELECT steamAccountId FROM candidate)
                      AND hero_done=0
                      AND assigned_to IS NULL
                    RETURNING steamAccountId
                    """,
                ).fetchone()
                if assigned_row:
                    candidate_payload = {
                        "type": "fetch_hero_stats",
                        "steamAccountId": int(assigned_row["steamAccountId"]),
                    }

            if candidate_payload is None:
                hero_pending = cur.execute(
                    "SELECT 1 FROM players WHERE hero_done=0 LIMIT 1"
                ).fetchone()
                if not hero_pending and not discovery_due:
                    candidate_payload = assign_discovery()

            if candidate_payload is not None:
                task_payload = candidate_payload
                retryable_execute(
                    cur,
                    """
                    INSERT INTO meta (key, value)
                    VALUES (?, ?)
                    ON CONFLICT(key) DO UPDATE SET value=excluded.value
                    """,
                    ("task_assignment_counter", str(next_count)),
                )
                if checkpoint_due or should_truncate_wal:
                    should_checkpoint = True
                break

            if refresh_due or discovery_due:
                break

            loop_count = next_count

    if should_checkpoint:
        with db_connection(write=True) as checkpoint_conn:
            retryable_execute(
                checkpoint_conn,
                "PRAGMA wal_checkpoint(TRUNCATE);",
            )

    return task_payload


def is_local_request() -> bool:
    local_hosts = {"127.0.0.1", "::1"}
    remote_addr = (request.remote_addr or "").strip()
    if remote_addr in local_hosts or remote_addr.startswith("127."):
        return True
    for addr in request.access_route or []:
        addr = (addr or "").strip()
        if addr in local_hosts or addr.startswith("127."):
            return True
    forwarded_for = request.headers.get("X-Forwarded-For", "")
    for addr in forwarded_for.split(","):
        addr = addr.strip()
        if addr and (addr in local_hosts or addr.startswith("127.")):
            return True
    return False


def create_app() -> Flask:
    app = Flask(
        __name__,
        static_folder=str(STATIC_DIR),
        template_folder=str(TEMPLATE_DIR),
    )

    release_incomplete_assignments()

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
        with db_connection(write=True) as conn:
            cur = conn.cursor()
            if task_type == "fetch_hero_stats":
                has_existing_stats = cur.execute(
                    "SELECT 1 FROM hero_stats WHERE steamAccountId=? LIMIT 1",
                    (steam_account_id,),
                ).fetchone()
                hero_done_value = 1 if has_existing_stats else 0
                retryable_execute(
                    cur,
                    """
                    UPDATE players
                    SET hero_done=?,
                        hero_refreshed_at=CASE WHEN ? THEN hero_refreshed_at ELSE NULL END,
                        assigned_to=NULL,
                        assigned_at=NULL
                    WHERE steamAccountId=?
                    """,
                    (hero_done_value, hero_done_value, steam_account_id),
                )
            elif task_type == "discover_matches":
                retryable_execute(
                    cur,
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
                retryable_execute(
                    cur,
                    """
                    UPDATE players
                    SET assigned_to=NULL,
                        assigned_at=NULL
                    WHERE steamAccountId=?
                    """,
                    (steam_account_id,),
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
            hero_stats_rows = []
            best_rows = []
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
                    best_rows.append(
                        (hero_id, hero_name, steam_account_id, matches, wins)
                    )
            assigned_at_value = None
            update_count = 0
            with db_connection(write=True) as conn:
                cur = conn.cursor()
                assignment_row = retryable_execute(
                    cur,
                    "SELECT assigned_at FROM players WHERE steamAccountId=?",
                    (steam_account_id,),
                ).fetchone()
                if assignment_row is not None:
                    assigned_at_value = assignment_row["assigned_at"]
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
                update_count = update_cursor.rowcount
            if update_count == 0:
                return (
                    jsonify(
                        {
                            "status": "error",
                            "message": "Player not found",
                        }
                    ),
                    404,
                )
            BACKGROUND_EXECUTOR.submit(
                _process_hero_submission,
                steam_account_id,
                hero_stats_rows,
                best_rows,
                assigned_at_value,
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
            discovered_ids: list[int] = []
            seen_ids: set[int] = set()
            for value in data.get("discovered", []):
                try:
                    candidate_id = int(value)
                except (TypeError, ValueError):
                    continue
                if candidate_id in seen_ids:
                    continue
                seen_ids.add(candidate_id)
                discovered_ids.append(candidate_id)
            assigned_at_value = None
            next_depth_value = None
            update_count = 0
            with db_connection(write=True) as conn:
                cur = conn.cursor()
                assignment_row = retryable_execute(
                    cur,
                    "SELECT assigned_at, depth FROM players WHERE steamAccountId=?",
                    (steam_account_id,),
                ).fetchone()
                if assignment_row is not None:
                    assigned_at_value = assignment_row["assigned_at"]
                provided_next_depth = data.get("nextDepth")
                if provided_next_depth is not None:
                    try:
                        next_depth_value = int(provided_next_depth)
                    except (TypeError, ValueError):
                        next_depth_value = None
                if next_depth_value is None:
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
                    next_depth_value = parent_depth_value + 1
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
                update_count = update_cursor.rowcount
            if update_count == 0:
                return (
                    jsonify(
                        {
                            "status": "error",
                            "message": "Player not found",
                        }
                    ),
                    404,
                )
            if next_depth_value is None:
                next_depth_value = 1
            BACKGROUND_EXECUTOR.submit(
                _process_discover_submission,
                steam_account_id,
                discovered_ids,
                next_depth_value,
                assigned_at_value,
            )
            next_task = assign_next_task() if request_new_task else None
            response_payload = {"status": "ok"}
            if request_new_task:
                response_payload["task"] = next_task
            return jsonify(response_payload)
        return jsonify({"status": "error", "message": "Unknown submit type"}), 400

    @app.get("/progress")
    def progress():
        with db_connection() as conn:
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
        with db_connection(write=True) as conn:
            cur = conn.cursor()
            for pid in range(start, end + 1):
                retryable_execute(
                    cur,
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
        return jsonify({"seeded": [start, end]})

    @app.get("/leaderboards/<hero_slug>")
    def hero_leaderboard(hero_slug: str):
        slug = hero_slug.strip().replace(" ", "_").lower()
        hero_entry = HERO_SLUGS.get(slug)
        if not hero_entry:
            abort(404)
        hero_id, hero_name = hero_entry
        with db_connection() as conn:
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
        with db_connection() as conn:
            rows = conn.execute("SELECT * FROM best ORDER BY matches DESC").fetchall()
        payload = []
        for row in rows:
            row_dict = dict(row)
            name = row_dict.get("hero_name")
            row_dict["hero_slug"] = hero_slug(name) if isinstance(name, str) else None
            payload.append(row_dict)
        return jsonify(payload)

    return app
