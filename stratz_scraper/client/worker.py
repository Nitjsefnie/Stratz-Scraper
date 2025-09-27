"""Background worker processes for the PyQt client."""
from __future__ import annotations

import json
import math
import time
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from multiprocessing import Event, Queue
from typing import Any, Dict, List, Optional, Tuple

import requests

try:
    from selenium import webdriver
    from selenium.common.exceptions import WebDriverException
    from selenium.webdriver.chrome.options import Options as ChromeOptions
    from selenium.webdriver.common.by import By
    from selenium.webdriver.edge.options import Options as EdgeOptions
    from selenium.webdriver.firefox.options import Options as FirefoxOptions
except Exception:  # pragma: no cover - selenium is optional at runtime
    webdriver = None
    WebDriverException = Exception
    ChromeOptions = None
    EdgeOptions = None
    FirefoxOptions = None
    By = None

DAY_IN_MS = 86_400_000
INITIAL_BACKOFF_MS = 1_000
MAX_BACKOFF_MS = DAY_IN_MS
NO_TASK_WAIT_MS = 60_000
NO_TASK_RETRY_DELAY_MS = 100
TASK_RESET_TIMEOUT = 10
HTTPBIN_HEADERS_URL = "https://httpbin.org/headers"

DEFAULT_STRATZ_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/126.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Origin": "https://stratz.com",
    "Referer": "https://stratz.com/",
    "Sec-Ch-Ua": '"Not/A)Brand";v="8", "Chromium";v="126", "Google Chrome";v="126"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site",
}

BROWSER_HEADER_KEYS = {
    "Accept",
    "Accept-Encoding",
    "Accept-Language",
    "User-Agent",
    "Sec-Ch-Ua",
    "Sec-Ch-Ua-Mobile",
    "Sec-Ch-Ua-Platform",
    "Sec-Fetch-Dest",
    "Sec-Fetch-Mode",
    "Sec-Fetch-Site",
}


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


def _create_chrome_driver() -> Any:
    assert ChromeOptions is not None  # nosec - guarded by caller
    options = ChromeOptions()
    # Allow Selenium Manager to discover the binary but prefer headless execution.
    if hasattr(options, "add_argument"):
        options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-extensions")
        options.add_argument("--window-size=1280,800")
    return webdriver.Chrome(options=options)


def _create_edge_driver() -> Any:
    assert EdgeOptions is not None  # nosec - guarded by caller
    options = EdgeOptions()
    if hasattr(options, "use_chromium"):
        options.use_chromium = True
    if hasattr(options, "add_argument"):
        options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-extensions")
        options.add_argument("--window-size=1280,800")
    return webdriver.Edge(options=options)


def _create_firefox_driver() -> Any:
    assert FirefoxOptions is not None  # nosec - guarded by caller
    options = FirefoxOptions()
    if hasattr(options, "add_argument"):
        options.add_argument("-headless")
    else:
        options.headless = True
    return webdriver.Firefox(options=options)


def _available_browser_factories() -> List[Tuple[str, Any]]:
    factories: List[Tuple[str, Any]] = []
    if webdriver is None or By is None:
        return factories
    if ChromeOptions is not None:
        factories.append(("Chrome", _create_chrome_driver))
    if EdgeOptions is not None:
        factories.append(("Edge", _create_edge_driver))
    if FirefoxOptions is not None:
        factories.append(("Firefox", _create_firefox_driver))
    return factories


def _collect_headers_from_driver(driver: Any) -> Optional[Dict[str, str]]:
    if By is None:
        return None
    user_agent: Optional[str]
    languages: Optional[Any]
    data: Dict[str, Any]
    try:
        driver.get(HTTPBIN_HEADERS_URL)
        time.sleep(1)
        body_element = driver.find_element(By.TAG_NAME, "body")
        text = body_element.text if body_element is not None else ""
        data = json.loads(text) if text else {}
    except Exception:
        data = {}
    try:
        user_agent = driver.execute_script("return navigator.userAgent")
    except Exception:
        user_agent = None
    try:
        languages = driver.execute_script("return navigator.languages || []")
    except Exception:
        languages = None
    headers: Dict[str, str] = {}
    payload_headers = data.get("headers") if isinstance(data, dict) else None
    if isinstance(payload_headers, dict):
        for key, value in payload_headers.items():
            if key in BROWSER_HEADER_KEYS and isinstance(value, str):
                headers[key] = value
    if user_agent and "User-Agent" not in headers:
        headers["User-Agent"] = str(user_agent)
    if languages and isinstance(languages, list) and "Accept-Language" not in headers:
        language_values = [str(lang) for lang in languages if isinstance(lang, str)]
        if language_values:
            headers["Accept-Language"] = ",".join(language_values)
    return headers or None


def _probe_browser_headers() -> Tuple[Optional[Dict[str, str]], Optional[str], List[str]]:
    factories = _available_browser_factories()
    if not factories:
        return None, None, ["Selenium webdriver is unavailable"]

    attempts: List[str] = []
    for name, factory in factories:
        driver = None
        try:
            driver = factory()
        except WebDriverException as exc:  # pragma: no cover - requires browsers
            attempts.append(f"{name} launch failed: {exc}")
            continue
        except Exception as exc:  # pragma: no cover - unexpected failures
            attempts.append(f"{name} launch error: {exc}")
            continue
        try:
            headers = _collect_headers_from_driver(driver)
        finally:
            try:
                driver.quit()
            except Exception:
                pass
        if headers:
            return headers, name, attempts
        attempts.append(f"{name} did not yield headers")
    return None, None, attempts


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
        "Accept": session.headers.get("Accept", "application/json, text/plain, */*"),
        "apollographql-client-name": "web",
        "apollographql-client-version": "1.0",
    }
    response = session.post(
        "https://api.stratz.com/graphql",
        headers=headers,
        json={"query": query, "variables": variables},
        timeout=60,
    )
    if response.status_code == 403:
        raise RuntimeError(
            "Stratz API returned 403 Forbidden. Double-check that your token is valid and that the "
            "request is not being blocked by Stratz."
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
    browser_headers, browser_name, header_attempts = _probe_browser_headers()
    if browser_headers:
        stratz_session.headers.clear()
        stratz_session.headers.update(browser_headers)
        for key, value in DEFAULT_STRATZ_HEADERS.items():
            stratz_session.headers.setdefault(key, value)
        _send_log(
            queue,
            config.token_id,
            f"Captured {browser_name} headers via Selenium for Stratz requests.",
        )
    else:
        stratz_session.headers.update(DEFAULT_STRATZ_HEADERS)
        if header_attempts:
            truncated = "; ".join(header_attempts[:2])
            if len(header_attempts) > 2:
                truncated += "; ..."
            _send_log(
                queue,
                config.token_id,
                f"Falling back to bundled headers. Selenium errors: {truncated}",
            )
        else:
            _send_log(
                queue,
                config.token_id,
                "Falling back to bundled headers. Selenium not available.",
            )
    stratz_session.headers.setdefault("Origin", "https://stratz.com")
    stratz_session.headers.setdefault("Referer", "https://stratz.com/")
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
