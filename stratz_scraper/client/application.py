"""PyQt application replicating the web interface for the Stratz scraper."""
from __future__ import annotations

import json
import os
import queue
import sys
from dataclasses import dataclass, field
from functools import partial
from multiprocessing import Event, Process, Queue as MPQueue, freeze_support
from pathlib import Path
from typing import Any, Dict, List, Optional

from PyQt6.QtCore import QTimer
from PyQt6.QtGui import QAction, QTextCursor
from PyQt6.QtWidgets import (
    QApplication,
    QFileDialog,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMenu,
    QMenuBar,
    QMessageBox,
    QPushButton,
    QSpinBox,
    QTableWidget,
    QTableWidgetItem,
    QTabWidget,
    QPlainTextEdit,
    QVBoxLayout,
    QWidget,
)

from .worker import WorkerConfig, worker_entry

TOKENS_FILE = Path.home() / ".stratz_scraper_tokens.json"
LOG_MAX_ENTRIES = 200


@dataclass
class TokenStatus:
    running: bool = False
    backoff_ms: int = 10_000
    requests_remaining: Optional[int] = None
    completed_tasks: int = 0
    runtime_ms: float = 0.0
    average_task_ms: Optional[float] = None
    tasks_per_day: Optional[float] = None
    stop_requested: bool = False
    timestamp_ms: Optional[int] = None


@dataclass
class TokenWidgets:
    token_edit: QLineEdit
    max_edit: QSpinBox
    status_label: QLabel
    backoff_label: QLabel
    requests_label: QLabel
    average_label: QLabel
    projection_label: QLabel
    start_button: QPushButton
    stop_button: QPushButton
    remove_button: QPushButton


@dataclass
class TokenItem:
    identifier: str
    value: str = ""
    max_requests: Optional[int] = None
    status: TokenStatus = field(default_factory=TokenStatus)
    widgets: Optional[TokenWidgets] = None
    worker: Optional[Process] = None
    stop_event: Optional[Event] = None
    log_entries: List[str] = field(default_factory=list)

    @property
    def running(self) -> bool:
        return bool(self.status.running)


class TokenTable(QTableWidget):
    def __init__(self) -> None:
        super().__init__(0, 9)
        self.setHorizontalHeaderLabels(
            [
                "Token",
                "Max Requests",
                "Status",
                "Backoff",
                "Requests Left",
                "Avg Task",
                "24h Projection",
                "Start",
                "Stop",
            ]
        )
        header = self.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        for i in range(2, 7):
            header.setSectionResizeMode(i, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(7, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(8, QHeaderView.ResizeMode.ResizeToContents)
        self.verticalHeader().setVisible(False)
        self.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.setSelectionMode(QTableWidget.SelectionMode.SingleSelection)
        self.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)


class ClientWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("Stratz Scraper Client")
        self.resize(1200, 800)

        self.message_queue: MPQueue = MPQueue()
        self.tokens: List[TokenItem] = []
        self.token_counter = 1

        self._setup_menu()
        self._init_ui()
        self._load_tokens()
        self._update_controls()
        self._update_global_metrics()

        self.queue_timer = QTimer(self)
        self.queue_timer.timeout.connect(self.process_worker_messages)
        self.queue_timer.start(200)

        self.monitor_timer = QTimer(self)
        self.monitor_timer.timeout.connect(self._cleanup_workers)
        self.monitor_timer.start(1000)

    # ----- UI setup -----
    def _setup_menu(self) -> None:
        menu_bar = QMenuBar(self)
        file_menu = QMenu("File", self)
        import_action = QAction("Import Tokens…", self)
        import_action.triggered.connect(self.import_tokens)
        export_action = QAction("Export Tokens…", self)
        export_action.triggered.connect(self.export_tokens)
        file_menu.addAction(import_action)
        file_menu.addAction(export_action)
        menu_bar.addMenu(file_menu)
        self.setMenuBar(menu_bar)

    def _init_ui(self) -> None:
        central = QWidget(self)
        layout = QVBoxLayout(central)

        server_layout = QHBoxLayout()
        server_label = QLabel("Server URL:")
        self.server_input = QLineEdit("http://127.0.0.1:80")
        self.refresh_progress_button = QPushButton("Refresh Progress")
        self.refresh_progress_button.clicked.connect(self.refresh_progress)
        self.refresh_best_button = QPushButton("Refresh Leaderboard")
        self.refresh_best_button.clicked.connect(self.load_best)
        server_layout.addWidget(server_label)
        server_layout.addWidget(self.server_input)
        server_layout.addWidget(self.refresh_progress_button)
        server_layout.addWidget(self.refresh_best_button)
        layout.addLayout(server_layout)

        stats_layout = QHBoxLayout()
        self.status_chip = QLabel("Idle")
        self.status_chip.setObjectName("statusChip")
        self.progress_label = QLabel("Progress: —")
        self.backoff_label = QLabel("Min Backoff: —")
        self.requests_label = QLabel("Requests Remaining: —")
        self.avg_label = QLabel("Avg Task: —")
        self.tasks_label = QLabel("24h Projection: —")
        stats_layout.addWidget(self.status_chip)
        stats_layout.addWidget(self.progress_label)
        stats_layout.addWidget(self.backoff_label)
        stats_layout.addWidget(self.requests_label)
        stats_layout.addWidget(self.avg_label)
        stats_layout.addWidget(self.tasks_label)
        layout.addLayout(stats_layout)

        controls_layout = QHBoxLayout()
        self.add_token_button = QPushButton("Add Token")
        self.add_token_button.clicked.connect(self.add_token_row)
        self.start_all_button = QPushButton("Start All")
        self.start_all_button.clicked.connect(self.start_all_tokens)
        self.stop_all_button = QPushButton("Stop All")
        self.stop_all_button.clicked.connect(self.stop_all_tokens)
        controls_layout.addWidget(self.add_token_button)
        controls_layout.addWidget(self.start_all_button)
        controls_layout.addWidget(self.stop_all_button)
        controls_layout.addStretch(1)
        layout.addLayout(controls_layout)

        self.table = TokenTable()
        layout.addWidget(self.table)

        tabs = QTabWidget()
        self.log_view = QPlainTextEdit()
        self.log_view.setReadOnly(True)
        self.log_view.setMaximumBlockCount(2000)
        tabs.addTab(self.log_view, "Logs")

        self.best_table = QTableWidget(0, 5)
        self.best_table.setHorizontalHeaderLabels([
            "Hero",
            "Hero ID",
            "Player ID",
            "Matches",
            "Wins",
        ])
        self.best_table.verticalHeader().setVisible(False)
        self.best_table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self.best_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        for index in range(1, 5):
            self.best_table.horizontalHeader().setSectionResizeMode(
                index, QHeaderView.ResizeMode.ResizeToContents
            )
        tabs.addTab(self.best_table, "Leaderboard")

        seed_widget = QWidget()
        seed_layout = QVBoxLayout(seed_widget)
        seed_box = QGroupBox("Seed Players")
        seed_box_layout = QHBoxLayout()
        seed_box.setLayout(seed_box_layout)
        self.seed_start = QLineEdit()
        self.seed_start.setPlaceholderText("Start ID")
        self.seed_end = QLineEdit()
        self.seed_end.setPlaceholderText("End ID")
        self.seed_button = QPushButton("Seed Range")
        self.seed_button.clicked.connect(self.seed_range)
        seed_box_layout.addWidget(QLabel("Start:"))
        seed_box_layout.addWidget(self.seed_start)
        seed_box_layout.addWidget(QLabel("End:"))
        seed_box_layout.addWidget(self.seed_end)
        seed_box_layout.addWidget(self.seed_button)
        seed_layout.addWidget(seed_box)
        seed_layout.addStretch(1)
        tabs.addTab(seed_widget, "Seed")

        layout.addWidget(tabs)

        self.setCentralWidget(central)

    # ----- Token management -----
    def add_token_row(self, value: str = "", max_requests: Optional[int] = None, persist: bool = True) -> None:
        token_id = f"token-{self.token_counter}"
        self.token_counter += 1
        item = TokenItem(identifier=token_id, value=value.strip(), max_requests=max_requests)
        row = self.table.rowCount()
        self.table.insertRow(row)

        token_edit = QLineEdit(item.value)
        token_edit.textChanged.connect(partial(self._on_token_changed, item))

        max_edit = QSpinBox()
        max_edit.setRange(0, 1_000_000_000)
        max_edit.setSpecialValueText("∞")
        if max_requests is not None:
            max_edit.setValue(max_requests)
        else:
            max_edit.setValue(0)
        max_edit.valueChanged.connect(partial(self._on_max_changed, item))

        status_label = QLabel("Idle")
        backoff_label = QLabel("—")
        requests_label = QLabel("—")
        average_label = QLabel("—")
        projection_label = QLabel("—")

        start_button = QPushButton("Start")
        start_button.clicked.connect(partial(self.start_token, item))
        stop_button = QPushButton("Stop")
        stop_button.clicked.connect(partial(self.stop_token, item))
        stop_button.setEnabled(False)
        remove_button = QPushButton("Remove")
        remove_button.clicked.connect(partial(self.remove_token, item))

        widgets = TokenWidgets(
            token_edit=token_edit,
            max_edit=max_edit,
            status_label=status_label,
            backoff_label=backoff_label,
            requests_label=requests_label,
            average_label=average_label,
            projection_label=projection_label,
            start_button=start_button,
            stop_button=stop_button,
            remove_button=remove_button,
        )
        item.widgets = widgets

        self.table.setCellWidget(row, 0, token_edit)
        self.table.setCellWidget(row, 1, max_edit)
        self.table.setCellWidget(row, 2, status_label)
        self.table.setCellWidget(row, 3, backoff_label)
        self.table.setCellWidget(row, 4, requests_label)
        self.table.setCellWidget(row, 5, average_label)
        self.table.setCellWidget(row, 6, projection_label)
        self.table.setCellWidget(row, 7, start_button)
        self.table.setCellWidget(row, 8, stop_button)

        self.tokens.append(item)
        self._update_status_display(item)

        if persist:
            self._persist_tokens()
        self._update_controls()
        self._update_global_metrics()

    def _on_token_changed(self, item: TokenItem, text: str) -> None:
        item.value = text.strip()
        self._persist_tokens()
        self._update_controls()

    def _on_max_changed(self, item: TokenItem, value: int) -> None:
        item.max_requests = value if value > 0 else None
        if item.status.requests_remaining is not None and not item.running:
            item.status.requests_remaining = item.max_requests
        self._persist_tokens()
        self._update_status_display(item)
        self._update_global_metrics()

    def remove_token(self, item: TokenItem) -> None:
        if item.running:
            QMessageBox.warning(self, "Token running", "Stop the worker before removing the token.")
            return
        try:
            row = self.tokens.index(item)
        except ValueError:
            return
        self.tokens.pop(row)
        self.table.removeRow(row)
        self._persist_tokens()
        self._update_controls()
        self._update_global_metrics()
        self._append_log(f"Removed token #{row + 1}.")

    def start_token(self, item: TokenItem) -> None:
        if item.running:
            return
        token_value = item.value.strip()
        if not token_value:
            QMessageBox.warning(self, "Token missing", "Enter a Stratz API token before starting.")
            return
        max_requests = item.max_requests
        stop_event = Event()
        config = WorkerConfig(
            token_id=item.identifier,
            token_value=token_value,
            server_url=self.server_input.text(),
            max_requests=max_requests,
        )
        process = Process(target=worker_entry, args=(config, stop_event, self.message_queue))
        process.daemon = True
        process.start()
        item.worker = process
        item.stop_event = stop_event
        item.status.running = True
        item.status.stop_requested = False
        item.status.requests_remaining = max_requests
        item.log_entries.clear()
        self._append_log(f"Token {self._token_label(item)}: Worker started.")
        self._update_status_display(item)
        self._update_controls()
        self._update_global_metrics()

    def stop_token(self, item: TokenItem) -> None:
        if not item.worker or not item.worker.is_alive():
            return
        if item.stop_event and not item.stop_event.is_set():
            item.stop_event.set()
        item.status.stop_requested = True
        self._append_log(f"Token {self._token_label(item)}: Stop requested.")
        self._update_status_display(item)
        self._update_controls()
        self._update_global_metrics()

    def start_all_tokens(self) -> None:
        for item in self.tokens:
            if not item.running and item.value.strip():
                self.start_token(item)

    def stop_all_tokens(self) -> None:
        for item in self.tokens:
            self.stop_token(item)

    def _cleanup_workers(self) -> None:
        for item in self.tokens:
            process = item.worker
            if process and not process.is_alive():
                process.join(timeout=0.1)
                item.worker = None
                item.stop_event = None
                item.status.running = False
                item.status.stop_requested = False
                item.status.requests_remaining = item.max_requests
                self._update_status_display(item)
        self._update_controls()
        self._update_global_metrics()

    # ----- Persistence -----
    def _persist_tokens(self) -> None:
        try:
            payload = [
                {
                    "token": token.value,
                    "maxRequests": token.max_requests,
                }
                for token in self.tokens
                if token.value
            ]
            with TOKENS_FILE.open("w", encoding="utf-8") as handle:
                json.dump(payload, handle, indent=2)
        except OSError:
            pass

    def _load_tokens(self) -> None:
        if TOKENS_FILE.exists():
            try:
                data = json.loads(TOKENS_FILE.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                data = []
            if isinstance(data, list):
                for entry in data:
                    if not isinstance(entry, dict):
                        continue
                    token_value = entry.get("token")
                    max_requests = entry.get("maxRequests")
                    try:
                        max_int = int(max_requests) if max_requests is not None else None
                    except (TypeError, ValueError):
                        max_int = None
                    if isinstance(token_value, str):
                        self.add_token_row(token_value, max_int, persist=False)

    def import_tokens(self) -> None:
        path, _ = QFileDialog.getOpenFileName(self, "Import Tokens", os.getcwd(), "JSON Files (*.json)")
        if not path:
            return
        try:
            with open(path, "r", encoding="utf-8") as handle:
                data = json.load(handle)
        except (OSError, json.JSONDecodeError) as error:
            QMessageBox.critical(self, "Import failed", f"Failed to load tokens: {error}")
            return
        imported = 0
        for entry in data if isinstance(data, list) else []:
            token_value = None
            max_requests = None
            if isinstance(entry, str):
                token_value = entry
            elif isinstance(entry, dict):
                token_value = entry.get("token") or entry.get("value")
                max_candidate = entry.get("maxRequests") or entry.get("max")
                if max_candidate is not None:
                    try:
                        max_requests = int(max_candidate)
                    except (TypeError, ValueError):
                        max_requests = None
            if isinstance(token_value, str) and token_value.strip():
                self.add_token_row(token_value.strip(), max_requests, persist=False)
                imported += 1
        self._persist_tokens()
        self._update_global_metrics()
        QMessageBox.information(self, "Import complete", f"Imported {imported} token(s).")

    def export_tokens(self) -> None:
        path, _ = QFileDialog.getSaveFileName(self, "Export Tokens", os.getcwd(), "JSON Files (*.json)")
        if not path:
            return
        payload = [
            {
                "token": token.value,
                "maxRequests": token.max_requests,
            }
            for token in self.tokens
            if token.value
        ]
        try:
            with open(path, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, indent=2)
        except OSError as error:
            QMessageBox.critical(self, "Export failed", f"Unable to export tokens: {error}")
            return
        QMessageBox.information(self, "Export complete", f"Saved {len(payload)} token(s).")

    # ----- Worker message handling -----
    def process_worker_messages(self) -> None:
        handled_any = False
        while True:
            try:
                message = self.message_queue.get_nowait()
            except queue.Empty:
                break
            handled_any = True
            message_type = message.get("type")
            if message_type == "log":
                self._handle_log(message)
            elif message_type == "status":
                self._handle_status(message)
            elif message_type == "refresh_progress":
                self.refresh_progress()
        if handled_any:
            self._update_global_metrics()
            self._update_controls()

    def _handle_log(self, message: Dict[str, Any]) -> None:
        token_id = message.get("token_id")
        text = message.get("payload", {}).get("message")
        if not isinstance(text, str):
            return
        token = self._find_token(token_id)
        label = self._token_label(token) if token else token_id
        line = f"[{label}] {text}"
        if token:
            token.log_entries.append(line)
            if len(token.log_entries) > LOG_MAX_ENTRIES:
                token.log_entries = token.log_entries[-LOG_MAX_ENTRIES:]
        self._append_log(line)

    def _handle_status(self, message: Dict[str, Any]) -> None:
        token_id = message.get("token_id")
        payload = message.get("payload", {})
        token = self._find_token(token_id)
        if not token:
            return
        status = token.status
        status.running = bool(payload.get("running"))
        status.backoff_ms = int(payload.get("backoff_ms", 0) or 0)
        requests_remaining = payload.get("requests_remaining")
        if requests_remaining in (None, "∞"):
            status.requests_remaining = None
        else:
            try:
                status.requests_remaining = int(requests_remaining)
            except (TypeError, ValueError):
                status.requests_remaining = None
        status.completed_tasks = int(payload.get("completed_tasks", 0) or 0)
        status.runtime_ms = float(payload.get("runtime_ms", 0) or 0)
        avg_ms = payload.get("average_task_ms")
        status.average_task_ms = float(avg_ms) if avg_ms is not None else None
        tasks_day = payload.get("tasks_per_day")
        status.tasks_per_day = float(tasks_day) if tasks_day is not None else None
        status.stop_requested = bool(payload.get("stop_requested"))
        status.timestamp_ms = payload.get("timestamp")
        if not status.running:
            token.worker = None
            token.stop_event = None
            status.stop_requested = False
        self._update_status_display(token)

    def _find_token(self, identifier: Optional[str]) -> Optional[TokenItem]:
        for token in self.tokens:
            if token.identifier == identifier:
                return token
        return None

    # ----- Display helpers -----
    def _token_label(self, token: Optional[TokenItem]) -> str:
        if not token:
            return "?"
        try:
            index = self.tokens.index(token)
        except ValueError:
            return token.identifier
        return f"Token #{index + 1}"

    def _append_log(self, text: str) -> None:
        self.log_view.appendPlainText(text)
        cursor = self.log_view.textCursor()
        cursor.movePosition(QTextCursor.MoveOperation.End)
        self.log_view.setTextCursor(cursor)

    def _update_status_display(self, token: TokenItem) -> None:
        if not token.widgets:
            return
        status = token.status
        widgets = token.widgets

        running = status.running
        if status.stop_requested and running:
            widgets.status_label.setText("Stopping…")
        elif running:
            widgets.status_label.setText("Running")
        else:
            widgets.status_label.setText("Idle")

        widgets.backoff_label.setText(self._format_duration(status.backoff_ms))
        if status.requests_remaining is None:
            widgets.requests_label.setText("∞")
        else:
            widgets.requests_label.setText(str(status.requests_remaining))
        widgets.average_label.setText(self._format_average(status.average_task_ms))
        widgets.projection_label.setText(self._format_tasks(status.tasks_per_day))

        widgets.start_button.setEnabled(not running and not status.stop_requested)
        widgets.stop_button.setEnabled(running and not status.stop_requested)
        widgets.remove_button.setEnabled(not running)
        widgets.token_edit.setEnabled(not running)
        widgets.max_edit.setEnabled(not running)

    def _format_duration(self, ms: Optional[float]) -> str:
        if ms is None or ms <= 0:
            return "—"
        if ms < 1000:
            return f"{int(ms)} ms"
        if ms < 60_000:
            return f"{ms / 1000:.1f} s"
        minutes = round(ms / 60_000)
        return f"{minutes} min"

    def _format_average(self, ms: Optional[float]) -> str:
        if ms is None or ms <= 0:
            return "—"
        return self._format_duration(ms)

    def _format_tasks(self, tasks: Optional[float]) -> str:
        if tasks is None or tasks <= 0:
            return "—"
        if tasks >= 100:
            return f"{tasks:.0f}"
        return f"{tasks:.1f}"

    def _update_controls(self) -> None:
        any_running = any(token.status.running for token in self.tokens)
        self.start_all_button.setEnabled(any(token.value.strip() and not token.status.running for token in self.tokens))
        self.stop_all_button.setEnabled(any_running)
        if any_running:
            self.status_chip.setText("Running")
        else:
            self.status_chip.setText("Idle")

    def _update_global_metrics(self) -> None:
        running_tokens = [token for token in self.tokens if token.status.running]
        if running_tokens:
            min_backoff = min(token.status.backoff_ms for token in running_tokens)
            self.backoff_label.setText(f"Min Backoff: {self._format_duration(min_backoff)}")
        else:
            self.backoff_label.setText("Min Backoff: —")

        request_sum = 0
        unknown = False
        for token in running_tokens:
            if token.status.requests_remaining is None:
                unknown = True
                break
            request_sum += token.status.requests_remaining
        if not running_tokens:
            self.requests_label.setText("Requests Remaining: —")
        elif unknown:
            self.requests_label.setText("Requests Remaining: ∞")
        else:
            self.requests_label.setText(f"Requests Remaining: {request_sum}")

        total_runtime = 0.0
        total_tasks = 0
        total_projection = 0.0
        projection_count = 0
        for token in self.tokens:
            status = token.status
            total_runtime += status.runtime_ms
            total_tasks += status.completed_tasks
            if status.tasks_per_day and status.tasks_per_day > 0:
                total_projection += status.tasks_per_day
                projection_count += 1
        if total_tasks > 0 and total_runtime > 0:
            average_ms = total_runtime / total_tasks
            self.avg_label.setText(f"Avg Task: {self._format_duration(average_ms)}")
        else:
            self.avg_label.setText("Avg Task: —")
        if projection_count:
            self.tasks_label.setText(f"24h Projection: {self._format_tasks(total_projection)}")
        else:
            self.tasks_label.setText("24h Projection: —")

    # ----- Networking helpers -----
    def _server_url(self) -> str:
        value = self.server_input.text().strip()
        if not value:
            return "http://127.0.0.1:80"
        return value.rstrip("/")

    def refresh_progress(self) -> None:
        import requests

        try:
            response = requests.get(f"{self._server_url()}/progress", timeout=15)
            response.raise_for_status()
            payload = response.json()
        except requests.RequestException as error:
            self.progress_label.setText(f"Progress: error ({error})")
            return
        hero_done = payload.get("hero_done")
        discover_done = payload.get("discover_done")
        total_players = payload.get("players_total")
        if all(isinstance(value, int) for value in (hero_done, discover_done, total_players)):
            hero_text = f"Hero: {hero_done} / {total_players}"
            discover_text = f"Discover: {discover_done} / {total_players}"
            self.progress_label.setText(f"Progress: {hero_text} • {discover_text}")
        else:
            self.progress_label.setText("Progress: unavailable")

    def load_best(self) -> None:
        import requests

        try:
            response = requests.get(f"{self._server_url()}/best", timeout=15)
            response.raise_for_status()
            rows = response.json()
        except requests.RequestException as error:
            QMessageBox.warning(self, "Leaderboard", f"Failed to load leaderboard: {error}")
            return
        if not isinstance(rows, list):
            rows = []
        self.best_table.setRowCount(0)
        for row_data in rows:
            row = self.best_table.rowCount()
            self.best_table.insertRow(row)
            hero_name = row_data.get("hero_name") or "—"
            hero_id = row_data.get("hero_id") or "—"
            player_id = row_data.get("player_id") or "—"
            matches = row_data.get("matches") or "—"
            wins = row_data.get("wins") or "—"
            for column, value in enumerate([hero_name, hero_id, player_id, matches, wins]):
                item = QTableWidgetItem(str(value))
                self.best_table.setItem(row, column, item)

    def seed_range(self) -> None:
        import requests

        try:
            start = int(self.seed_start.text())
            end = int(self.seed_end.text())
        except ValueError:
            QMessageBox.warning(self, "Seed", "Enter numeric start and end IDs.")
            return
        if end < start:
            QMessageBox.warning(self, "Seed", "End ID must be greater than start ID.")
            return
        try:
            response = requests.get(
                f"{self._server_url()}/seed",
                params={"start": start, "end": end},
                timeout=30,
            )
            response.raise_for_status()
            payload = response.json()
        except requests.RequestException as error:
            QMessageBox.warning(self, "Seed", f"Failed to seed range: {error}")
            return
        seeded = payload.get("seeded")
        if isinstance(seeded, list) and len(seeded) == 2:
            self._append_log(f"Seeded IDs {seeded[0]} - {seeded[1]}.")
        else:
            self._append_log("Seed request completed.")

    # ----- Qt overrides -----
    def closeEvent(self, event) -> None:  # type: ignore[override]
        self.stop_all_tokens()
        for item in self.tokens:
            if item.worker and item.worker.is_alive():
                item.worker.join(timeout=1)
        return super().closeEvent(event)


def main() -> int:
    freeze_support()
    app = QApplication(sys.argv)
    window = ClientWindow()
    window.show()
    return app.exec()
