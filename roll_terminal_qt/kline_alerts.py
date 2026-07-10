from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from okx_quant.analysis import TrendlineDetectionConfig, detect_trendlines
from okx_quant.analysis.box_detector import BoxDetectionConfig, detect_boxes
from roll_terminal_qt.kline_box_rules import AUTO_BOX_MAX_CANDIDATES, is_auto_box_candidate_valid


_LINE_KINDS = {"horizontal", "trend"}
_LINE_TRIGGERS = {"cross_above", "cross_below", "touch"}
_LINE_ACTIONS = {"notify", "long", "short"}
_LINE_TRADE_MANAGEMENT_MODES = {"fixed_tp", "trail_after_1r", "trail_after_2r", "trail_after_3r"}
_LINE_TRADE_ENTRY_MODES = {"limit", "market", "chase_best_quote"}
_EVENT_LIMIT = 120
_STRUCTURE_SCAN_BAR_LIMIT = 180


def build_workspace_key(symbol: str, period: str) -> str:
    return f"{symbol.strip().upper()}|{period.strip()}"


def normalize_workspace_entry(payload: dict[str, object] | None) -> dict[str, object]:
    raw = payload if isinstance(payload, dict) else {}
    alerts = raw.get("alerts") if isinstance(raw.get("alerts"), dict) else {}
    ma_cross = alerts.get("ma_cross") if isinstance(alerts, dict) and isinstance(alerts.get("ma_cross"), dict) else {}
    box_breakout = alerts.get("box_breakout") if isinstance(alerts, dict) and isinstance(alerts.get("box_breakout"), dict) else {}
    return {
        "lines": [_normalize_line_rule(item) for item in _safe_list(raw.get("lines"))],
        "rr": [dict(item) for item in _safe_list(raw.get("rr")) if isinstance(item, dict)],
        "alerts": {
            "ma_cross": {
                "enabled": _safe_bool(ma_cross.get("enabled"), default=True),
                "last_event_candle_time": _safe_int(ma_cross.get("last_event_candle_time")),
            },
            "box_breakout": {
                "enabled": _safe_bool(box_breakout.get("enabled"), default=False),
                "last_event_candle_time": _safe_int(box_breakout.get("last_event_candle_time")),
            },
        },
        "events": _normalize_events(_safe_list(raw.get("events"))),
    }


def evaluate_workspace_alerts(
    *,
    workspace_entry: dict[str, object],
    candles: list[dict[str, Any]],
    ema_fast: list[dict[str, Any]],
    ma_slow: list[dict[str, Any]],
    raw_candles: list[Any],
) -> tuple[dict[str, object], list[dict[str, object]], dict[str, object]]:
    entry = normalize_workspace_entry(workspace_entry)
    alerts = dict(entry.get("alerts", {}))
    lines = [_normalize_line_rule(item) for item in _safe_list(entry.get("lines"))]
    rr_items = [dict(item) for item in _safe_list(entry.get("rr")) if isinstance(item, dict)]
    events = _normalize_events(_safe_list(entry.get("events")))

    new_events: list[dict[str, object]] = []

    ma_alert = dict(alerts.get("ma_cross", {}))
    ma_event = _evaluate_ma_cross_alert(candles, ema_fast, ma_slow, ma_alert)
    if ma_event is not None:
        new_events.append(ma_event)
        ma_alert["last_event_candle_time"] = ma_event.get("candle_time", 0)
    alerts["ma_cross"] = ma_alert

    box_alert = dict(alerts.get("box_breakout", {}))
    box_event = _evaluate_box_breakout_alert(raw_candles, box_alert)
    if box_event is not None:
        new_events.append(box_event)
        box_alert["last_event_candle_time"] = box_event.get("candle_time", 0)
    alerts["box_breakout"] = box_alert

    updated_lines: list[dict[str, object]] = []
    for line in lines:
        updated_line = dict(line)
        line_event = _evaluate_line_alert(candles, updated_line)
        if line_event is not None:
            new_events.append(line_event)
            updated_line["last_event_candle_time"] = line_event.get("candle_time", 0)
            updated_line["triggered"] = True
        updated_lines.append(updated_line)

    merged_events = _trim_events([*new_events, *events])
    structure = build_structure_summary(raw_candles)
    return {
        "lines": updated_lines,
        "rr": rr_items,
        "alerts": alerts,
        "events": merged_events,
    }, new_events, structure


def build_structure_summary(raw_candles: list[Any]) -> dict[str, object]:
    scan_candles = _tail_candles(raw_candles, limit=_STRUCTURE_SCAN_BAR_LIMIT)
    if len(scan_candles) < 24:
        return {"note": "K 线数量不足，无法扫描箱体和趋势线", "box_count": 0, "trendline_count": 0}
    boxes = detect_boxes(scan_candles, BoxDetectionConfig(max_candidates=3))
    trendlines = detect_trendlines(scan_candles, TrendlineDetectionConfig(max_candidates=3))
    parts: list[str] = []
    if boxes:
        box = boxes[0]
        parts.append(f"箱体 {float(box.lower):.2f}-{float(box.upper):.2f}")
    if trendlines:
        trend = trendlines[0]
        trend_kind = "压力线" if str(trend.kind).lower() == "resistance" else "支撑线"
        parts.append(f"{trend_kind} 触点 {trend.touches}")
    if not parts:
        parts.append("暂无明显箱体或趋势线结构")
    return {
        "note": " | ".join(parts),
        "box_count": len(boxes),
        "trendline_count": len(trendlines),
    }


def make_line_rule(
    *,
    kind: str,
    label: str,
    trigger: str,
    action: str,
    time_a: int,
    price_a: float,
    time_b: int,
    price_b: float,
    enabled: bool = True,
    color: str = "#1d4ed8",
    trade_enabled: bool = False,
    risk_amount: float = 100.0,
    stop_loss_price: float = 0.0,
    direct_take_profit_r: float = 2.0,
    management_mode: str = "fixed_tp",
    entry_execution_mode: str = "limit",
    fee_offset_enabled: bool = False,
) -> dict[str, object]:
    return _normalize_line_rule(
        {
            "id": uuid4().hex[:10],
            "kind": kind,
            "label": label,
            "trigger": trigger,
            "action": action,
            "time_a": time_a,
            "price_a": price_a,
            "time_b": time_b,
            "price_b": price_b,
            "enabled": enabled,
            "color": color,
            "trade_enabled": trade_enabled,
            "risk_amount": risk_amount,
            "stop_loss_price": stop_loss_price,
            "direct_take_profit_r": direct_take_profit_r,
            "management_mode": management_mode,
            "entry_execution_mode": entry_execution_mode,
            "fee_offset_enabled": fee_offset_enabled,
        }
    )


def line_value_at(line: dict[str, object], candle_time: int) -> float:
    kind = str(line.get("kind", "horizontal") or "horizontal").strip().lower()
    price_a = _safe_float(line.get("price_a"))
    price_b = _safe_float(line.get("price_b"), default=price_a)
    time_a = _safe_int(line.get("time_a"))
    time_b = _safe_int(line.get("time_b"), default=time_a)
    if kind == "horizontal" or time_a == time_b:
        return price_a
    slope = (price_b - price_a) / max(1, time_b - time_a)
    return price_a + (slope * (candle_time - time_a))


def _ordered_line_endpoints(
    time_a: int,
    price_a: float,
    time_b: int,
    price_b: float,
) -> tuple[int, float, int, float]:
    left_time = int(time_a)
    left_price = float(price_a)
    right_time = int(time_b)
    right_price = float(price_b)
    if right_time < left_time:
        left_time, right_time = right_time, left_time
        left_price, right_price = right_price, left_price
    if right_time == left_time:
        right_time = left_time + 1
    return left_time, left_price, right_time, right_price


def _evaluate_ma_cross_alert(
    candles: list[dict[str, Any]],
    ema_fast: list[dict[str, Any]],
    ma_slow: list[dict[str, Any]],
    rule: dict[str, object],
) -> dict[str, object] | None:
    if not _safe_bool(rule.get("enabled"), default=True):
        return None
    if len(candles) < 2 or len(ema_fast) < 2 or len(ma_slow) < 2:
        return None
    prev_fast = _safe_float(ema_fast[-2].get("value"))
    curr_fast = _safe_float(ema_fast[-1].get("value"))
    prev_slow = _safe_float(ma_slow[-2].get("value"))
    curr_slow = _safe_float(ma_slow[-1].get("value"))
    candle_time = _safe_int(candles[-1].get("time"))
    if candle_time <= _safe_int(rule.get("last_event_candle_time")):
        return None
    if prev_fast <= prev_slow and curr_fast > curr_slow:
        return _build_event("ma_cross", "cross_above", "EMA 15 上穿 SMA 50", candle_time, "EMA 15 上穿 SMA 50")
    if prev_fast >= prev_slow and curr_fast < curr_slow:
        return _build_event("ma_cross", "cross_below", "EMA 15 下穿 SMA 50", candle_time, "EMA 15 下穿 SMA 50")
    return None


def _evaluate_box_breakout_alert(raw_candles: list[Any], rule: dict[str, object]) -> dict[str, object] | None:
    if not _safe_bool(rule.get("enabled"), default=False):
        return None
    if len(raw_candles) < 24:
        return None
    reference = _tail_candles(raw_candles[:-1], limit=_STRUCTURE_SCAN_BAR_LIMIT)
    if len(reference) < 24:
        return None
    boxes = detect_boxes(reference, BoxDetectionConfig(max_candidates=AUTO_BOX_MAX_CANDIDATES))
    box = next((item for item in boxes if is_auto_box_candidate_valid(item, reference)), None)
    if box is None:
        return None
    previous = raw_candles[-2]
    current = raw_candles[-1]
    candle_time = _safe_int(getattr(current, "ts", 0)) // 1000
    if candle_time <= _safe_int(rule.get("last_event_candle_time")):
        return None
    upper = float(box.upper)
    lower = float(box.lower)
    prev_close = _safe_float(getattr(previous, "close", 0.0))
    curr_close = _safe_float(getattr(current, "close", 0.0))
    if prev_close <= upper and curr_close > upper:
        return _build_event("box_breakout", "cross_above", "自动箱体", candle_time, f"箱体向上突破 {upper:.2f}")
    if prev_close >= lower and curr_close < lower:
        return _build_event("box_breakout", "cross_below", "自动箱体", candle_time, f"箱体向下跌破 {lower:.2f}")
    return None


def _evaluate_line_alert(candles: list[dict[str, Any]], line: dict[str, object]) -> dict[str, object] | None:
    if not _safe_bool(line.get("enabled"), default=True):
        return None
    if len(candles) < 2:
        return None
    prev_candle = candles[-2]
    curr_candle = candles[-1]
    candle_time = _safe_int(curr_candle.get("time"))
    if candle_time <= _safe_int(line.get("last_event_candle_time")):
        return None
    prev_time = _safe_int(prev_candle.get("time"))
    prev_close = _safe_float(prev_candle.get("close"))
    curr_close = _safe_float(curr_candle.get("close"))
    curr_low = _safe_float(curr_candle.get("low"))
    curr_high = _safe_float(curr_candle.get("high"))
    prev_line = line_value_at(line, prev_time)
    curr_line = line_value_at(line, candle_time)
    trigger = str(line.get("trigger", "cross_above") or "cross_above").strip().lower()
    label = str(line.get("label", "线条预警") or "线条预警").strip()
    if trigger == "cross_above" and prev_close <= prev_line and curr_close > curr_line:
        return _build_line_event(line, trigger, label, candle_time, f"{label} 向上突破")
    if trigger == "cross_below" and prev_close >= prev_line and curr_close < curr_line:
        return _build_line_event(line, trigger, label, candle_time, f"{label} 向下跌破")
    tolerance = max(abs(curr_line) * 0.0005, 0.00000001)
    if trigger == "touch" and curr_low <= curr_line + tolerance and curr_high >= curr_line - tolerance:
        return _build_line_event(line, trigger, label, candle_time, f"{label} 已触发触碰")
    return None


def _normalize_line_rule(item: object) -> dict[str, object]:
    raw = item if isinstance(item, dict) else {}
    kind = _safe_choice(raw.get("kind"), _LINE_KINDS, default="horizontal")
    time_a = _safe_int(raw.get("time_a"))
    price_a = _safe_float(raw.get("price_a"))
    time_b = _safe_int(raw.get("time_b"), default=time_a)
    price_b = _safe_float(raw.get("price_b"), default=price_a)
    if kind == "trend":
        time_a, price_a, time_b, price_b = _ordered_line_endpoints(time_a, price_a, time_b, price_b)
    return {
        "id": _safe_text(raw.get("id")) or uuid4().hex[:10],
        "kind": kind,
        "label": _safe_text(raw.get("label")) or ("趋势线" if kind == "trend" else "水平线"),
        "trigger": _safe_choice(raw.get("trigger"), _LINE_TRIGGERS, default="cross_above"),
        "action": _safe_choice(raw.get("action"), _LINE_ACTIONS, default="notify"),
        "time_a": time_a,
        "price_a": price_a,
        "time_b": time_b,
        "price_b": price_b,
        "enabled": _safe_bool(raw.get("enabled"), default=True),
        "trade_enabled": _safe_bool(raw.get("trade_enabled"), default=False),
        "risk_amount": _safe_float(raw.get("risk_amount"), default=100.0),
        "stop_loss_price": _safe_float(raw.get("stop_loss_price")),
        "direct_take_profit_r": _safe_float(raw.get("direct_take_profit_r"), default=2.0),
        "management_mode": _safe_choice(raw.get("management_mode"), _LINE_TRADE_MANAGEMENT_MODES, default="fixed_tp"),
        "entry_execution_mode": _safe_choice(raw.get("entry_execution_mode"), _LINE_TRADE_ENTRY_MODES, default="limit"),
        "fee_offset_enabled": _safe_bool(raw.get("fee_offset_enabled"), default=False),
        "triggered": _safe_bool(raw.get("triggered"), default=False),
        "last_event_candle_time": _safe_int(raw.get("last_event_candle_time")),
        "color": _safe_text(raw.get("color")) or "#1d4ed8",
    }


def _build_line_event(
    line: dict[str, object],
    direction: str,
    label: str,
    candle_time: int,
    message: str,
) -> dict[str, object]:
    event = _build_event("line_alert", direction, label, candle_time, message)
    event.update(
        {
            "line_id": _safe_text(line.get("id")),
            "trade_action": _safe_choice(line.get("action"), _LINE_ACTIONS, default="notify"),
            "trade_enabled": _safe_bool(line.get("trade_enabled"), default=False),
        }
    )
    return event


def _normalize_events(items: list[object]) -> list[dict[str, object]]:
    out: list[dict[str, object]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        out.append(
            {
                "event_time": _safe_text(item.get("event_time")) or _now_iso(),
                "candle_time": _safe_int(item.get("candle_time")),
                "kind": _safe_text(item.get("kind")),
                "direction": _safe_text(item.get("direction")),
                "label": _safe_text(item.get("label")),
                "message": _safe_text(item.get("message")),
            }
        )
    return _trim_events(out)


def _build_event(kind: str, direction: str, label: str, candle_time: int, message: str) -> dict[str, object]:
    return {
        "event_time": _now_iso(),
        "candle_time": candle_time,
        "kind": kind,
        "direction": direction,
        "label": label,
        "message": f"{message} | {_format_candle_time(candle_time)}",
    }


def _trim_events(items: list[dict[str, object]]) -> list[dict[str, object]]:
    ordered = sorted(
        items,
        key=lambda item: (str(item.get("event_time", "")), _safe_int(item.get("candle_time"))),
        reverse=True,
    )
    return ordered[:_EVENT_LIMIT]


def _format_candle_time(candle_time: int) -> str:
    if candle_time <= 0:
        return "-"
    return datetime.fromtimestamp(candle_time).strftime("%m-%d %H:%M")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _safe_list(value: object) -> list[object]:
    return list(value) if isinstance(value, list) else []


def _safe_text(value: object) -> str:
    return str(value or "").strip()


def _safe_bool(value: object, *, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return default


def _safe_int(value: object, *, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_float(value: object, *, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_choice(value: object, allowed: set[str], *, default: str) -> str:
    text = str(value or "").strip().lower()
    return text if text in allowed else default


def _tail_candles(candles: list[Any], *, limit: int) -> list[Any]:
    if limit <= 0 or len(candles) <= limit:
        return list(candles)
    return list(candles[-limit:])
