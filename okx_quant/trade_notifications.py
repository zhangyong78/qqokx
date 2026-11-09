"""Persistent, idempotent notification queue for trade-plan events.

The queue deliberately does not infer fills from account positions. Execution
adapters must enqueue an event only after confirming the plan's own order.
"""
from __future__ import annotations

import json
import threading
import time
import uuid
from pathlib import Path
from typing import Callable

from okx_quant.app_paths import state_dir_path

_LOCK = threading.RLock()
_KINDS = {"signal", "accepted", "filled", "error"}
_ACTIVE_CLAIMS: set[str] = set()
_SENDING_LEASE_SECONDS = 300
_WORKER_THREAD: threading.Thread | None = None
_WORKER_WAKE = threading.Event()
_WORKER_STOP = threading.Event()
_WORKER_LAST_ERROR_TS = 0.0


def normalize_plan_notification_events(raw: object, *, legacy_enabled: object | None = None,
                                      legacy_event: str | None = None,
                                      is_new_plan: bool = False) -> dict[str, bool]:
    """Resolve explicit event settings and documented legacy defaults."""
    defaults = {"signal": False, "accepted": False, "filled": False, "error": False}
    if is_new_plan:
        defaults.update(filled=True, error=True)
    if isinstance(raw, dict):
        for kind in _KINDS:
            if kind in raw:
                defaults[kind] = bool(raw[kind])
        return defaults
    if legacy_event in _KINDS and legacy_enabled is not None:
        defaults[legacy_event] = bool(legacy_enabled)
    return defaults


def merged_recipients(global_recipients: object, api_recipients: object) -> list[str]:
    """Merge global and API-specific addresses, preserving order and uniqueness."""
    result: list[str] = []
    for raw in (global_recipients, api_recipients):
        values = raw if isinstance(raw, (list, tuple, set)) else str(raw or "").replace(";", ",").replace("\n", ",").split(",")
        for value in values:
            address = str(value or "").strip()
            if address and address.casefold() not in {item.casefold() for item in result}:
                result.append(address)
    return result


class TradeNotificationQueue:
    """JSON-backed event log; injectable path and sender make it testable offline."""

    def __init__(self, path: str | Path | None = None, sender: Callable[[dict], object] | None = None):
        self.path = Path(path) if path is not None else state_dir_path() / "trade_notifications.json"
        self.sender = sender

    def _read(self) -> list[dict]:
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
            if not isinstance(raw, list) or any(not isinstance(item, dict) for item in raw):
                raise ValueError("invalid notification store schema")
            required = {"event_id", "plan_id", "kind", "payload", "status", "attempts"}
            if any(not required.issubset(item) or item.get("kind") not in _KINDS for item in raw):
                raise ValueError("invalid notification row schema")
            return [dict(item) for item in raw]
        except FileNotFoundError:
            return []
        except (OSError, ValueError) as exc:
            raise RuntimeError(f"trade notification store cannot be read: {self.path}") from exc

    def _write(self, rows: list[dict]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temp = self.path.with_suffix(self.path.suffix + ".tmp")
        temp.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
        temp.replace(self.path)

    def rows(self) -> list[dict]:
        """Return a snapshot for status UI and acceptance checks."""
        with _LOCK:
            return [dict(row) for row in self._read()]

    def enqueue(self, *, event_id: str, plan_id: str, kind: str, payload: dict,
                recipients: list[str] | None = None, eligible: bool = True) -> dict:
        if kind not in _KINDS:
            raise ValueError(f"unsupported trade notification kind: {kind}")
        event_id = str(event_id or "").strip()
        if not event_id:
            raise ValueError("event_id is required for idempotency")
        with _LOCK:
            rows = self._read()
            context = "|".join(str(payload.get(key, "") or "").strip().casefold()
                               for key in ("profile", "environment", "inst_id"))
            dedupe_key = f"{context}|{plan_id}|{event_id}"
            existing = next((row for row in rows if row.get("dedupe_key", row.get("event_id")) == dedupe_key), None)
            if existing is not None:
                return dict(existing)
            row = {"event_id": event_id, "plan_id": str(plan_id), "kind": kind,
                   "dedupe_key": dedupe_key,
                   "payload": dict(payload), "recipients": list(recipients or []),
                   "status": "pending" if eligible else "suppressed", "attempts": 0, "id": uuid.uuid4().hex}
            rows.append(row)
            self._write(rows)
            _WORKER_WAKE.set()
            return dict(row)

    def deliver_pending(self, *, enabled: bool, notify_signals: bool, notify_trade_fills: bool,
                        notify_errors: bool, sender: Callable[[dict], object] | None = None) -> int:
        send = sender or self.sender
        if send is None:
            return 0
        switches = {"signal": enabled and notify_signals, "accepted": enabled and notify_signals,
                    "filled": enabled and notify_trade_fills, "error": enabled and notify_errors}
        delivered = 0
        claimed: list[dict] = []
        with _LOCK:
            rows = self._read()
            for row in rows:
                if row.get("status") == "sending":
                    claimed_at = float(row.get("claimed_at", 0) or 0)
                    claim_key = str(row.get("dedupe_key", row.get("event_id", "")))
                    if claim_key in _ACTIVE_CLAIMS or time.time() - claimed_at < _SENDING_LEASE_SECONDS:
                        continue
                    row["status"] = "pending"  # reclaim only an expired worker lease
                if row.get("status") != "pending":
                    continue
                if not switches.get(row.get("kind"), False):
                    row["status"] = "suppressed"
                    continue
                row["status"] = "sending"
                row["claimed_at"] = time.time()
                row["attempts"] = int(row.get("attempts", 0)) + 1
                _ACTIVE_CLAIMS.add(str(row.get("dedupe_key", row.get("event_id", ""))))
                claimed.append(dict(row))
            self._write(rows)
        for item in claimed:
            try:
                result = send(item)
                if result is not True:
                    raise RuntimeError("sender did not confirm SMTP delivery")
                status, error = "sent", ""
                delivered += 1
            except Exception as exc:
                status, error = "failed", str(exc)
            with _LOCK:
                _ACTIVE_CLAIMS.discard(str(item.get("dedupe_key", item.get("event_id", ""))))
                rows = self._read()
                row = next((value for value in rows if value.get("dedupe_key", value.get("event_id")) == item.get("dedupe_key", item.get("event_id"))), None)
                if row is not None and row.get("status") == "sending":
                    row["status"] = status
                    if error:
                        row["last_error"] = error
                    self._write(rows)
        return delivered

    def retry_failed(self, event_id: str | None = None) -> int:
        with _LOCK:
            rows = self._read()
            count = 0
            for row in rows:
                if row.get("status") == "failed" and (event_id is None or row.get("event_id") == event_id):
                    row["status"] = "pending"
                    count += 1
            self._write(rows)
            return count


def start_background_worker(config_provider: Callable[[], dict], sender: Callable[[dict, dict], bool],
                            *, queue: TradeNotificationQueue | None = None) -> None:
    """Start one process-wide sender worker; SMTP runs outside the UI thread."""
    global _WORKER_THREAD
    target_queue = queue or TradeNotificationQueue()
    with _LOCK:
        if _WORKER_THREAD is not None and _WORKER_THREAD.is_alive():
            _WORKER_WAKE.set()
            return
        _WORKER_STOP.clear()

        def run() -> None:
            global _WORKER_LAST_ERROR_TS
            while not _WORKER_STOP.is_set():
                try:
                    settings = config_provider()
                    target_queue.deliver_pending(
                        enabled=bool(settings.get("enabled", False)),
                        notify_signals=bool(settings.get("notify_signals", False)),
                        notify_trade_fills=bool(settings.get("notify_trade_fills", False)),
                        notify_errors=bool(settings.get("notify_errors", False)),
                        sender=lambda row: sender(row, settings),
                    )
                except Exception as exc:
                    # Worker failures never escape to the trading/UI caller.
                    if time.time() - _WORKER_LAST_ERROR_TS > 60:
                        _WORKER_LAST_ERROR_TS = time.time()
                        try:
                            from okx_quant.log_utils import append_log_line
                            append_log_line(f"[trade_notification] dispatcher failed: {exc}")
                        except Exception:
                            pass
                _WORKER_WAKE.wait(5.0)
                _WORKER_WAKE.clear()

        _WORKER_THREAD = threading.Thread(target=run, daemon=True, name="qqokx-trade-notifications")
        _WORKER_THREAD.start()
        _WORKER_WAKE.set()


def stop_background_worker(timeout: float = 3.0) -> None:
    """Stop the process-wide worker without joining under its store lock."""
    global _WORKER_THREAD
    _WORKER_STOP.set()
    _WORKER_WAKE.set()
    worker = _WORKER_THREAD
    if worker is not None and worker is not threading.current_thread():
        worker.join(timeout=max(0.0, float(timeout)))
    with _LOCK:
        if _WORKER_THREAD is worker and (worker is None or not worker.is_alive()):
            _WORKER_THREAD = None
