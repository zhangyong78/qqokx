"""Persistent subscriptions and de-duplicated history for background shape signals."""

from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from okx_quant.app_paths import state_dir_path


SUBSCRIPTIONS_FILE_NAME = "shape_signal_subscriptions.json"
EVENTS_FILE_NAME = "shape_signal_events.json"
SUPPORTED_PERIODS = ("1H", "4H", "1D", "1W")
SUPPORTED_PATTERNS = (
    "big_bullish",
    "big_bearish",
    "long_upper_shadow",
    "long_lower_shadow",
    "double_reversal_up",
    "double_reversal_down",
    "inside_bar",
    "top_fractal",
    "bottom_fractal",
)
_LOCK = threading.RLock()


def _path(name: str) -> Path:
    return state_dir_path() / name


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _safe_list(value: object) -> list[object]:
    return list(value) if isinstance(value, list) else []


def normalize_subscription(value: object) -> dict[str, object]:
    raw = value if isinstance(value, dict) else {}
    periods = [str(item).strip().upper() for item in _safe_list(raw.get("periods"))]
    patterns = [str(item).strip().lower() for item in _safe_list(raw.get("patterns"))]
    try:
        top_n = int(raw.get("top_n", 4) or 4)
    except (TypeError, ValueError):
        top_n = 4
    return {
        "id": str(raw.get("id") or uuid4().hex[:12]).strip(),
        "symbol": str(raw.get("symbol") or "BTC-USDT-SWAP").strip().upper(),
        "environment": str(raw.get("environment") or "demo").strip().lower() or "demo",
        "periods": [item for item in SUPPORTED_PERIODS if item in periods] or list(SUPPORTED_PERIODS),
        "patterns": [item for item in SUPPORTED_PATTERNS if item in patterns] or list(SUPPORTED_PATTERNS),
        "metric": "range" if str(raw.get("metric") or "body").strip().lower() == "range" else "body",
        "top_n": max(1, min(10, top_n)),
        "require_ma_touch": bool(raw.get("require_ma_touch", False)),
        "enabled": bool(raw.get("enabled", True)),
        "popup_enabled": bool(raw.get("popup_enabled", True)),
        "email_enabled": bool(raw.get("email_enabled", False)),
        "startup_email_enabled": bool(raw.get("startup_email_enabled", False)),
        "created_at": str(raw.get("created_at") or _now_iso()),
        "updated_at": _now_iso(),
    }


def load_subscriptions() -> list[dict[str, object]]:
    target = _path(SUBSCRIPTIONS_FILE_NAME)
    with _LOCK:
        try:
            payload = json.loads(target.read_text(encoding="utf-8")) if target.exists() else {}
        except Exception:
            return []
    raw = payload.get("subscriptions", []) if isinstance(payload, dict) else []
    return [normalize_subscription(item) for item in _safe_list(raw) if isinstance(item, dict)]


def save_subscriptions(items: list[dict[str, object]]) -> Path:
    target = _path(SUBSCRIPTIONS_FILE_NAME)
    payload = {"version": 1, "subscriptions": [normalize_subscription(item) for item in items], "updated_at": _now_iso()}
    with _LOCK:
        target.parent.mkdir(parents=True, exist_ok=True)
        temp = target.with_suffix(target.suffix + ".tmp")
        temp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        temp.replace(target)
    return target


def _event_key(item: dict[str, object]) -> tuple[str, str, str, int, str, str]:
    return (
        str(item.get("symbol") or "").strip().upper(),
        str(item.get("period") or "").strip().upper(),
        str(item.get("pattern_id") or "").strip().lower(),
        int(item.get("candle_ts", 0) or 0),
        str(item.get("direction") or "").strip().lower(),
        str(item.get("environment") or "demo").strip().lower(),
    )


def load_events(*, limit: int = 500, start_ts: int | None = None, end_ts: int | None = None) -> list[dict[str, object]]:
    target = _path(EVENTS_FILE_NAME)
    with _LOCK:
        try:
            payload = json.loads(target.read_text(encoding="utf-8")) if target.exists() else {}
        except Exception:
            return []
    raw = payload.get("events", []) if isinstance(payload, dict) else []
    events = [dict(item) for item in _safe_list(raw) if isinstance(item, dict)]
    if start_ts is not None:
        events = [item for item in events if int(item.get("candle_ts", 0) or 0) >= int(start_ts)]
    if end_ts is not None:
        events = [item for item in events if int(item.get("candle_ts", 0) or 0) < int(end_ts)]
    events.sort(key=lambda item: (int(item.get("candle_ts", 0) or 0), str(item.get("detected_at") or "")), reverse=True)
    return events[: max(1, int(limit))]


def append_events(items: list[dict[str, object]], *, max_records: int = 5000) -> list[dict[str, object]]:
    if not items:
        return []
    target = _path(EVENTS_FILE_NAME)
    with _LOCK:
        try:
            payload = json.loads(target.read_text(encoding="utf-8")) if target.exists() else {}
        except Exception:
            payload = {}
        existing = [dict(item) for item in _safe_list(payload.get("events", [])) if isinstance(item, dict)]
        seen = {_event_key(item) for item in existing}
        added: list[dict[str, object]] = []
        for item in items:
            event = dict(item)
            event.setdefault("detected_at", _now_iso())
            key = _event_key(event)
            if key in seen:
                continue
            seen.add(key)
            added.append(event)
        if not added:
            return []
        merged = [*existing, *added]
        merged.sort(key=lambda item: (int(item.get("candle_ts", 0) or 0), str(item.get("detected_at") or "")), reverse=True)
        merged = merged[: max(100, int(max_records))]
        target.parent.mkdir(parents=True, exist_ok=True)
        temp = target.with_suffix(target.suffix + ".tmp")
        temp.write_text(json.dumps({"version": 1, "events": merged, "updated_at": _now_iso()}, ensure_ascii=False, indent=2), encoding="utf-8")
        temp.replace(target)
        return added


def mark_events_read(items: list[dict[str, object]]) -> None:
    """Acknowledge only the displayed snapshot, preserving concurrently arriving signals."""
    keys = {_event_key(item) for item in items}
    if not keys:
        return
    target = _path(EVENTS_FILE_NAME)
    with _LOCK:
        if not target.exists():
            return
        payload = json.loads(target.read_text(encoding="utf-8"))
        changed = False
        for event in payload.get("events", []):
            if _event_key(event) in keys and not event.get("read_at"):
                event["read_at"] = _now_iso()
                changed = True
        if changed:
            temp = target.with_suffix(target.suffix + ".tmp")
            temp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            temp.replace(target)
