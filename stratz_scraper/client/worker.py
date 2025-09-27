"""Background worker processes for the PyQt client."""
from __future__ import annotations

import math
import time
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from multiprocessing import Event, Queue
from typing import Any, Dict, List, Optional

import requests

DAY_IN_MS = 86_400_000
INITIAL_BACKOFF_MS = 1_000
MAX_BACKOFF_MS = DAY_IN_MS
NO_TASK_WAIT_MS = 60_000
NO_TASK_RETRY_DELAY_MS = 100
TASK_RESET_TIMEOUT = 10


class RateLimitError(RuntimeError):
    """Raised when the Stratz API responds with a retry hint."""

    def __init__(self, message: str, retry_after_ms: Optional[int] = None) -> None:
        super().__init__(message)
        self.retry_after_ms = retry_after_ms


@dataclass
class WorkerConfig:
    token_id: str
    token_value: str
    server_url: str
    max_requests: Optional[int] = None
    client_name: str = "pyqt"


@dataclass
class WorkerState:
    requests_remaining: Optional[int]
    completed_tasks: int
    runtime_ms: float
    backoff_ms: int


@dataclass
class WorkerMessage:
    type: str
    token_id: str
    payload: Dict[str, Any]


def _now_ms() -> int:
    return int(time.time() * 1000)


def _monotonic_ms() -> float:
    return time.monotonic() * 1000.0


def _format_server_url(url: str) -> str:
    url = url.strip()
    if not url:
        return "http://127.0.0.1:80"
    if url.endswith("/"):
        url = url[:-1]
    return url


def _parse_retry_after(value: Optional[str]) -> Optional[int]:
    if not value:
        return None
    value = value.strip()
    if not value:
        return None
    try:
        seconds = float(value)
    except ValueError:
        try:
            parsed = parsedate_to_datetime(value)
        except (TypeError, ValueError):
            return None
        if parsed is None:
            return None
        delta = parsed.timestamp() - time.time()
        if delta <= 0:
            return None
        return int(math.ceil(delta * 1000))
    else:
        if not math.isfinite(seconds) or seconds < 0:
            return None
        return int(math.ceil(seconds * 1000))


def _wait(stop_event: Event, duration_ms: int) -> None:
    if duration_ms <= 0:
        return
    deadline = time.monotonic() + duration_ms / 1000
    while not stop_event.is_set():
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        time.sleep(min(0.5, remaining))


def _server_post(session: requests.Session, url: str, path: str, json_payload: dict, timeout: int = 30) -> dict:
    response = session.post(f"{url}{path}", json=json_payload, timeout=timeout)
    response.raise_for_status()
    if not response.content:
        return {}
    try:
        return response.json()
    except ValueError:
        return {}


def _fetch_task(session: requests.Session, url: str, client_name: str) -> Optional[dict]:
    payload = _server_post(session, url, "/task", {"client": client_name})
    task = payload.get("task")
    if task is None:
        return None
    if isinstance(task, dict):
        return task
    return None


def _reset_task(session: requests.Session, url: str, task: dict) -> None:
    if not task:
        return
    json_payload = {
        "steamAccountId": task.get("steamAccountId"),
        "type": task.get("type"),
    }
    try:
        _server_post(session, url, "/task/reset", json_payload, timeout=TASK_RESET_TIMEOUT)
    except requests.RequestException:
        pass


def _submit_hero_stats(session: requests.Session, url: str, player_id: int, heroes: list) -> Optional[dict]:
    payload = _server_post(
        session,
        url,
        "/submit",
        {
            "type": "fetch_hero_stats",
            "steamAccountId": player_id,
            "heroes": heroes,
            "task": True,
        },
    )
    return payload.get("task") if isinstance(payload, dict) else None


def _submit_discovery(
    session: requests.Session,
    url: str,
    player_id: int,
    discovered: List[dict],
    depth: Optional[int],
) -> Optional[dict]:
    payload = _server_post(
        session,
        url,
        "/submit",
        {
            "type": "discover_matches",
            "steamAccountId": player_id,
            "discovered": discovered,
            "depth": depth,
            "task": True,
        },
    )
    return payload.get("task") if isinstance(payload, dict) else None


def _execute_stratz_query(session: requests.Session, token: str, query: str, variables: dict) -> dict:
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    response = session.post(
        "https://api.stratz.com/graphql",
        headers=headers,
        json={"query": query, "variables": variables},
        timeout=60,
    )
    if response.status_code == 429:
        retry_after = _parse_retry_after(response.headers.get("retry-after"))
        raise RateLimitError("Stratz API rate limit", retry_after_ms=retry_after)
    response.raise_for_status()
    payload = response.json()
    if payload and isinstance(payload, dict) and payload.get("errors"):
        first_error = payload["errors"][0]
        message = first_error.get("message") if isinstance(first_error, dict) else None
        raise RuntimeError(message or "Stratz API returned errors")
    return payload


def _fetch_player_heroes(session: requests.Session, token: str, player_id: int) -> List[dict]:
    query = """
        query HeroPerf($id: Long!) {
          player(steamAccountId: $id) {
            heroesPerformance(request: { take: 999999, gameModeIds: [1, 22] }, take: 200) {
              heroId
              matchCount
              winCount
            }
          }
        }
    """
    payload = _execute_stratz_query(session, token, query, {"id": player_id})
    heroes = (
        payload.get("data", {})
        .get("player", {})
        .get("heroesPerformance", [])
    )
    results: List[dict] = []
    if isinstance(heroes, list):
        for entry in heroes:
            if not isinstance(entry, dict):
                continue
            hero_id = entry.get("heroId")
            match_count = entry.get("matchCount")
            win_count = entry.get("winCount")
            try:
                hero_id = int(hero_id)
                match_count = int(match_count)
                win_count = int(win_count)
            except (TypeError, ValueError):
                continue
            results.append(
                {
                    "heroId": hero_id,
                    "matches": match_count,
                    "wins": win_count,
                }
            )
    return results


def _discover_matches(
    session: requests.Session,
    token: str,
    player_id: int,
    take: int = 100,
) -> List[dict]:
    query = """
        query PlayerMatches($steamAccountId: Long!, $take: Int!, $skip: Int!) {
          player(steamAccountId: $steamAccountId) {
            matches(request: { take: $take, skip: $skip }) {
              id
              players {
                steamAccountId
              }
            }
          }
        }
    """
    discovered: dict[int, int] = {}
    order: List[int] = []
    skip = 0
    take = max(1, int(take))
    while True:
        payload = _execute_stratz_query(
            session,
            token,
            query,
            {"steamAccountId": player_id, "take": take, "skip": skip},
        )
        matches = (
            payload.get("data", {})
            .get("player", {})
            .get("matches", [])
        )
        if not isinstance(matches, list) or not matches:
            break
        for match in matches:
            players = match.get("players") if isinstance(match, dict) else None
            if not isinstance(players, list):
                continue
            for participant in players:
                if not isinstance(participant, dict):
                    continue
                candidate = participant.get("steamAccountId")
                try:
                    candidate = int(candidate)
                except (TypeError, ValueError):
                    continue
                if candidate <= 0 or candidate == player_id:
                    continue
                if candidate not in discovered:
                    discovered[candidate] = 1
                    order.append(candidate)
                else:
                    discovered[candidate] += 1
        if len(matches) < take:
            break
        skip += len(matches)
    return [
        {"steamAccountId": account_id, "count": discovered[account_id]}
        for account_id in order
    ]


def _send(queue: Queue, message: WorkerMessage) -> None:
    queue.put({
        "type": message.type,
        "token_id": message.token_id,
        "payload": message.payload,
    })


def _send_log(queue: Queue, token_id: str, text: str) -> None:
    _send(queue, WorkerMessage("log", token_id, {"message": text, "timestamp": _now_ms()}))


def _send_status(
    queue: Queue,
    token_id: str,
    running: bool,
    state: WorkerState,
    stop_requested: bool,
) -> None:
    average_ms = None
    tasks_per_day = None
    if state.completed_tasks > 0 and state.runtime_ms > 0:
        average_ms = state.runtime_ms / state.completed_tasks
        if average_ms > 0:
            tasks_per_day = DAY_IN_MS / average_ms
    payload = {
        "running": running,
        "backoff_ms": state.backoff_ms,
        "requests_remaining": state.requests_remaining,
        "completed_tasks": state.completed_tasks,
        "runtime_ms": state.runtime_ms,
        "average_task_ms": average_ms,
        "tasks_per_day": tasks_per_day,
        "stop_requested": stop_requested,
        "timestamp": _now_ms(),
    }
    _send(queue, WorkerMessage("status", token_id, payload))


def _send_refresh_progress(queue: Queue) -> None:
    queue.put({"type": "refresh_progress"})


def worker_entry(config: WorkerConfig, stop_event: Event, queue: Queue) -> None:
    server_url = _format_server_url(config.server_url)
    token_value = config.token_value.strip()
    session = requests.Session()
    stratz_session = requests.Session()
    requests_remaining = config.max_requests
    completed_tasks = 0
    start_ms = _monotonic_ms()
    backoff_ms = INITIAL_BACKOFF_MS
    state = WorkerState(requests_remaining=requests_remaining, completed_tasks=completed_tasks, runtime_ms=0, backoff_ms=backoff_ms)
    _send_log(queue, config.token_id, "Worker started.")
    if requests_remaining is not None:
        _send_log(queue, config.token_id, f"Request limit {requests_remaining}.")
    _send_status(queue, config.token_id, True, state, stop_requested=False)

    task: Optional[dict] = None

    try:
        while not stop_event.is_set():
            current_runtime_ms = _monotonic_ms() - start_ms
            state.runtime_ms = current_runtime_ms
            state.backoff_ms = backoff_ms
            state.requests_remaining = requests_remaining
            state.completed_tasks = completed_tasks
            _send_status(
                queue,
                config.token_id,
                True,
                state,
                stop_requested=stop_event.is_set(),
            )

            try:
                if task is None:
                    task = _fetch_task(session, server_url, config.client_name)
                    if stop_event.is_set():
                        break
                    if task is None:
                        backoff_ms = NO_TASK_WAIT_MS
                        state.runtime_ms = _monotonic_ms() - start_ms
                        state.backoff_ms = backoff_ms
                        _send_status(
                            queue,
                            config.token_id,
                            True,
                            state,
                            stop_requested=stop_event.is_set(),
                        )
                        _send_log(
                            queue,
                            config.token_id,
                            "No tasks available. Waiting 60 seconds before retrying.",
                        )
                        _wait(stop_event, NO_TASK_WAIT_MS)
                        continue

                if stop_event.is_set():
                    if task is not None:
                        _reset_task(session, server_url, task)
                        task = None
                    break

                task_id = task.get("steamAccountId")
                task_type = task.get("type")
                next_task: Optional[dict] = None

                if task_type == "fetch_hero_stats":
                    try:
                        player_id = int(task_id)
                    except (TypeError, ValueError):
                        _send_log(queue, config.token_id, f"Invalid task id {task_id}. Resetting task.")
                        _reset_task(session, server_url, task)
                        task = None
                        backoff_ms = INITIAL_BACKOFF_MS
                        continue
                    _send_log(queue, config.token_id, f"Hero stats task for {player_id}.")
                    heroes = _fetch_player_heroes(stratz_session, token_value, player_id)
                    _send_log(queue, config.token_id, f"Fetched {len(heroes)} heroes for {task_id}.")
                    next_task = _submit_hero_stats(session, server_url, player_id, heroes)
                    _send_log(queue, config.token_id, f"Submitted {len(heroes)} heroes for {task_id}.")
                elif task_type == "discover_matches":
                    depth = task.get("depth")
                    try:
                        player_id = int(task_id)
                    except (TypeError, ValueError):
                        _send_log(queue, config.token_id, f"Invalid task id {task_id}. Resetting task.")
                        _reset_task(session, server_url, task)
                        task = None
                        backoff_ms = INITIAL_BACKOFF_MS
                        continue
                    _send_log(
                        queue,
                        config.token_id,
                        f"Discovery task for {player_id} (depth {depth if depth is not None else 0}).",
                    )
                    discovered = _discover_matches(stratz_session, token_value, player_id)
                    _send_log(queue, config.token_id, f"Discovered {len(discovered)} accounts from {task_id}.")
                    next_task = _submit_discovery(session, server_url, player_id, discovered, depth)
                    _send_log(queue, config.token_id, f"Submitted discovery results for {task_id}.")
                else:
                    _send_log(queue, config.token_id, f"Unknown task type {task_type}. Resetting {task_id}.")
                    _reset_task(session, server_url, task)
                    task = None
                    backoff_ms = INITIAL_BACKOFF_MS
                    continue

                completed_tasks += 1
                state.completed_tasks = completed_tasks
                state.runtime_ms = _monotonic_ms() - start_ms
                state.backoff_ms = backoff_ms
                _send_status(
                    queue,
                    config.token_id,
                    True,
                    state,
                    stop_requested=stop_event.is_set(),
                )
                _send_refresh_progress(queue)

                if requests_remaining is not None:
                    requests_remaining = max(0, requests_remaining - 1)
                    state.requests_remaining = requests_remaining
                    if requests_remaining == 0:
                        if next_task is not None:
                            _reset_task(session, server_url, next_task)
                        _send_log(queue, config.token_id, "Reached request limit. Stopping worker.")
                        break

                task = next_task
                backoff_ms = 10_000
                if task is None:
                    _wait(stop_event, NO_TASK_RETRY_DELAY_MS)
            except RateLimitError as rate_error:
                retry_after_ms = rate_error.retry_after_ms
                wait_ms = retry_after_ms if retry_after_ms is not None else backoff_ms
                wait_ms = max(0, min(int(wait_ms), MAX_BACKOFF_MS))
                backoff_ms = wait_ms
                state.runtime_ms = _monotonic_ms() - start_ms
                state.backoff_ms = wait_ms
                _send_status(
                    queue,
                    config.token_id,
                    True,
                    state,
                    stop_requested=stop_event.is_set(),
                )
                _send_log(queue, config.token_id, f"Rate limited. Waiting {wait_ms / 1000:.1f}s.")
                _wait(stop_event, wait_ms)
                if retry_after_ms is None:
                    backoff_ms = min(int(math.ceil(max(backoff_ms, 1000) * 1.2)), MAX_BACKOFF_MS)
                task = None
            except requests.RequestException as request_error:
                message = str(request_error)
                _send_log(queue, config.token_id, f"Network error: {message}")
                if task is not None:
                    _reset_task(session, server_url, task)
                    task = None
                wait_ms = min(max(backoff_ms, 1_000), MAX_BACKOFF_MS)
                state.runtime_ms = _monotonic_ms() - start_ms
                state.backoff_ms = wait_ms
                _send_status(
                    queue,
                    config.token_id,
                    True,
                    state,
                    stop_requested=stop_event.is_set(),
                )
                _wait(stop_event, wait_ms)
                backoff_ms = min(int(math.ceil(max(backoff_ms, 1_000) * 1.2)), MAX_BACKOFF_MS)
            except Exception as exc:  # noqa: BLE001
                message = str(exc)
                _send_log(queue, config.token_id, f"Error: {message}")
                if task is not None:
                    _reset_task(session, server_url, task)
                    task = None
                wait_ms = min(max(backoff_ms, 5_000), MAX_BACKOFF_MS)
                state.runtime_ms = _monotonic_ms() - start_ms
                state.backoff_ms = wait_ms
                _send_status(
                    queue,
                    config.token_id,
                    True,
                    state,
                    stop_requested=stop_event.is_set(),
                )
                _wait(stop_event, wait_ms)
                backoff_ms = min(int(math.ceil(max(backoff_ms, 5_000) * 1.2)), MAX_BACKOFF_MS)
    finally:
        state.runtime_ms = _monotonic_ms() - start_ms
        state.backoff_ms = INITIAL_BACKOFF_MS
        state.requests_remaining = requests_remaining
        state.completed_tasks = completed_tasks
        _send_status(
            queue,
            config.token_id,
            False,
            state,
            stop_requested=stop_event.is_set(),
        )
        _send_log(queue, config.token_id, "Worker stopped.")
