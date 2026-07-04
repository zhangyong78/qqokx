from __future__ import annotations

import json
import math
import os
import time
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

from PySide6.QtCore import QDateTime, QMargins, QPointF, QRectF, QTimer, Qt, QThread, Signal, Slot
from PySide6.QtGui import QColor, QPainter, QPen
try:
    from PySide6.QtCharts import QCandlestickSeries, QCandlestickSet, QChart, QChartView, QDateTimeAxis, QLineSeries, QValueAxis
except Exception:  # pragma: no cover - fallback for environments without QtCharts
    QCandlestickSeries = None  # type: ignore[assignment]
    QCandlestickSet = None  # type: ignore[assignment]
    QChart = None  # type: ignore[assignment]
    QChartView = None  # type: ignore[assignment]
    QDateTimeAxis = None  # type: ignore[assignment]
    QLineSeries = None  # type: ignore[assignment]
    QValueAxis = None  # type: ignore[assignment]
try:
    from PySide6.QtWebEngineWidgets import QWebEngineView
except Exception:  # pragma: no cover - fallback for environments without QtWebEngine
    QWebEngineView = None  # type: ignore[assignment]
from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QFrame,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QPushButton,
    QSplitter,
    QSpinBox,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from okx_quant.candle_cache import load_candle_cache
from okx_quant.deribit_client import DeribitRestClient
from okx_quant.deribit_volatility_ui import (
    DERIBIT_BASE_HOURLY_RESOLUTION,
    DERIBIT_FULL_HISTORY_START_TS,
    OKX_SPOT_SYMBOLS,
    _aggregate_candles_to_resolution,
    _hourly_fetch_start_ts,
    _hourly_history_limit,
    _merge_deribit_candles,
    _merge_price_candles,
    _to_average_volatility_candles,
)
from okx_quant.okx_client import OkxRestClient
from okx_quant.persistence import load_kline_analysis_workspace_entries, save_kline_analysis_workspace_entries
from okx_quant.signal_replay_engine import SignalReplayConfig, build_signal_replay_dataset
from roll_terminal_qt.deribit_volatility_window import _load_cached_hourly_series, _save_cached_hourly_series
from roll_terminal_qt.kline_alerts import (
    build_workspace_key,
    evaluate_workspace_alerts,
    line_value_at,
    make_line_rule,
    normalize_workspace_entry,
)


_INITIAL_WINDOW_LOAD_DELAY_MS = 80
_NATIVE_BOOTSTRAP_RENDER_BARS = 360
_NATIVE_BOOTSTRAP_RENDER_DELAY_MS = 90
_AUTO_REFRESH_DEFAULT_ENABLED = True
_NATIVE_RIGHT_PADDING_BARS = 24
_KLINE_SPLITTER_LEFT_RATIO = 0.11
_VOLUME_OVERLAY_HEIGHT_RATIO = 0.18
_EMA15_LINE_WIDTH = 2
_SMA50_LINE_WIDTH = 3
_SECONDARY_CHART_TOP_RATIO = 0.31
_SECONDARY_CHART_SIDE_RATIO = 0.56
_SECONDARY_CHART_SPLITTER_HANDLE_WIDTH = 10
_BOX_HISTORY_SCAN_LIMIT = 240
_BOX_HISTORY_MAX_SEGMENTS = 8
_BOX_HISTORY_OUTLINE_COLOR = "#f97316"
_BOX_HISTORY_FILL_COLOR = "#f97316"
_BOX_ACTIVE_OUTLINE_COLOR = "#fb923c"
_BOX_ACTIVE_FILL_COLOR = "#f97316"
_BOX_LIVE_OUTLINE_COLOR = "#0ea5e9"
_BOX_LIVE_FILL_COLOR = "#0ea5e9"
_CHART_BACKGROUND_COLOR = "#0b0f14"
_CHART_GRID_COLOR = "#1f2937"
_CHART_AXIS_TEXT_COLOR = "#8b95a5"
_CHART_AXIS_LINE_COLOR = "#18202b"
_CHART_UP_COLOR = "#22c55e"
_CHART_DOWN_COLOR = "#e04f84"
_CHART_EMA15_COLOR = "#ff4d6d"
_CHART_SMA50_COLOR = "#58c66d"
_CHART_CROSSHAIR_COLOR = "#6b7280"
_REPLAY_SIGNAL_NEAR_MA_MAX_PCT = 0.006
_REPLAY_SIGNAL_LONG_COLOR = "#38bdf8"
_REPLAY_SIGNAL_SHORT_COLOR = "#f97316"
_SOURCE_STATUS_LABELS = {
    "local_cache": "本地缓存",
    "local_cache_partial": "本地缓存不足",
    "local_cache_synced": "本地缓存已刷新",
    "local_plus_remote": "本地缓存 + 接口补齐",
    "remote_plus_local": "接口 + 本地缓存",
    "local_cache_replaced": "本地缓存已回补",
    "remote": "接口",
}
_BOX_TREND_LABELS = {
    "up": "上涨后整理",
    "down": "下跌后整理",
}
# Keep user-visible labels in plain UTF-8 literals. Do not replace them with
# mojibake fallbacks or ASCII placeholders.
_REPLAY_SIGNAL_LABELS = {
    "big_bullish": "大阳线",
    "big_bearish": "大阴线",
    "long_upper_shadow": "长上影",
    "long_lower_shadow": "长下影",
    "false_breakdown": "假跌破",
    "false_breakout": "假突破",
    "inside_bar": "孕线",
    "top_fractal": "顶分型",
    "bottom_fractal": "底分型",
}
_DAILY_TREND_NEUTRAL_BIAS = 0.008
_DAILY_TREND_STRONG_BIAS = 0.015
_DAILY_TREND_STRONG_SLOPE = 0.006
_DAILY_TREND_BAND_COLOR = {
    "strong_bull": "#1d4ed8",
    "weak_bull": "#60a5fa",
    "neutral": "#94a3b8",
    "weak_bear": "#fb923c",
    "strong_bear": "#f97316",
}
_DAILY_TREND_BAND_ALPHA = 245
_DAILY_TREND_BAND_MAX_HEIGHT = 28.0
_DAILY_TREND_BAND_MIN_HEIGHT = 12.0
_DAILY_TREND_BAND_SPLIT_RATIO = 0.30
_DAILY_TREND_BAND_GAP_PX = 7.0
_DAILY_TREND_BAND_LABEL = {
    "strong_bull": "强多",
    "weak_bull": "弱多",
    "neutral": "中性",
    "weak_bear": "弱空",
    "strong_bear": "强空",
}
_DEFAULT_SINGLE_CHART_PERIOD = "4H"
_DEFAULT_DUAL_PRIMARY_PERIOD = "1D"
_DEFAULT_DUAL_SECONDARY_PERIOD = "4H"
_PRIMARY_PERIOD_OPTIONS = (
    ("15m", "15m"),
    ("1H", "1H"),
    ("4H", "4H"),
    ("1D", "1D"),
)


def _to_ms_seconds(ts: int) -> int:
    return int(ts // 1000)


def _to_float(value: Decimal) -> float:
    return float(value)


def _format_bar_time(ts: int) -> str:
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")


def _to_ema(values: list[float], period: int) -> list[float]:
    if period <= 0:
        return []
    multiplier = 2.0 / (period + 1)
    ema: float | None = None
    result: list[float] = []
    for value in values:
        if ema is None:
            ema = value
        else:
            ema = (value - ema) * multiplier + ema
        result.append(ema)
    return result


def _to_sma(values: list[float], period: int) -> list[float]:
    if period <= 0:
        return []
    result: list[float] = []
    for index in range(1, len(values) + 1):
        window_start = max(0, index - period)
        window = values[window_start:index]
        result.append(sum(window) / len(window))
    return result


def _format_compact_number(value: float) -> str:
    absolute = abs(value)
    if absolute >= 1_000_000_000:
        return f"{value / 1_000_000_000:.2f}B"
    if absolute >= 1_000_000:
        return f"{value / 1_000_000:.2f}M"
    if absolute >= 1_000:
        return f"{value / 1_000:.2f}K"
    if absolute >= 1.0:
        return f"{value:.2f}"
    return f"{value:.6f}".rstrip("0").rstrip(".")


def _line_time_tolerance_seconds(display_step_ms: int, *, bars: int = 4) -> int:
    return max(1, int(math.ceil(max(1, display_step_ms) / 1000.0)) * max(1, bars))


def _resolve_interaction_cursor_mode(
    requested_mode: str,
    *,
    interaction_locked: bool,
    draw_mode_enabled: bool,
) -> str:
    normalized = str(requested_mode or "default").strip().lower() or "default"
    if interaction_locked:
        return "dragging" if normalized == "dragging" else normalized
    if draw_mode_enabled and normalized == "default":
        return "crosshair"
    return normalized


def _line_price_tolerance(price_span: float, price: float, *, emphasis: str = "body") -> float:
    span = max(float(price_span), 1.0)
    anchor = abs(float(price))
    if emphasis == "endpoint":
        return max(span * 0.024, anchor * 0.0032, 2.0)
    return max(span * 0.014, anchor * 0.0020, 1.2)


def _line_handle_visual(endpoint_key: str, *, hovered_drag_mode: str | None) -> dict[str, object]:
    normalized_endpoint = str(endpoint_key or "").strip().lower()
    normalized_hover = str(hovered_drag_mode or "").strip().lower() or None
    visual = {
        "radius": 6.0,
        "fill": "#0ea5e9",
        "inner_fill": "#f8fafc",
    }
    if normalized_hover == normalized_endpoint:
        visual["radius"] = 8.0
        visual["fill"] = "#7dd3fc"
        return visual
    if normalized_hover == "move":
        visual["radius"] = 6.8
        visual["fill"] = "#38bdf8"
        return visual
    return visual


def _ordered_trend_endpoints(
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


def _apply_drag_to_line_rule(
    line: dict[str, object],
    *,
    drag_mode: str,
    candle_time: int,
    price: float,
    anchor_line: dict[str, object] | None = None,
    anchor_candle_time: int | None = None,
    anchor_price: float | None = None,
) -> dict[str, object]:
    updated = dict(line)
    kind = str(updated.get("kind", "horizontal") or "horizontal").strip().lower()
    if kind == "horizontal":
        updated["price_a"] = float(price)
        updated["price_b"] = float(price)
        if "time_a" not in updated:
            updated["time_a"] = int(candle_time)
        if "time_b" not in updated:
            updated["time_b"] = int(candle_time)
        return updated
    current_time_a = int(updated.get("time_a", 0) or 0)
    current_price_a = float(updated.get("price_a", 0.0) or 0.0)
    current_time_b = int(updated.get("time_b", current_time_a) or current_time_a)
    current_price_b = float(updated.get("price_b", current_price_a) or current_price_a)
    if drag_mode == "move" and anchor_line is not None and anchor_candle_time is not None and anchor_price is not None:
        delta_time = int(candle_time) - int(anchor_candle_time)
        delta_price = float(price) - float(anchor_price)
        current_time_a = int(anchor_line.get("time_a", current_time_a) or current_time_a) + delta_time
        current_time_b = int(anchor_line.get("time_b", current_time_b) or current_time_b) + delta_time
        current_price_a = float(anchor_line.get("price_a", current_price_a) or current_price_a) + delta_price
        current_price_b = float(anchor_line.get("price_b", current_price_b) or current_price_b) + delta_price
    elif drag_mode == "endpoint_a":
        current_time_a = int(candle_time)
        current_price_a = float(price)
    elif drag_mode == "endpoint_b":
        current_time_b = int(candle_time)
        current_price_b = float(price)
    ordered = _ordered_trend_endpoints(current_time_a, current_price_a, current_time_b, current_price_b)
    updated["time_a"], updated["price_a"], updated["time_b"], updated["price_b"] = ordered
    return updated


def _bar_to_ms(period: str) -> int:
    normalized = period.strip().upper()
    if not normalized:
        return 0
    if len(normalized) < 2:
        return 0
    unit = normalized[-1]
    try:
        value = int(normalized[:-1])
    except ValueError:
        return 0
    if value <= 0:
        return 0
    if unit == "M":
        return value * 60_000
    if unit == "H":
        return value * 60 * 60_000
    if unit == "D":
        return value * 24 * 60 * 60_000
    return 0


def _is_local_cache_stale(local_candles: list[Any], period: str, *, now_ms: int | None = None) -> bool:
    if not local_candles:
        return True
    bar_ms = _bar_to_ms(period)
    if bar_ms <= 0:
        return False
    latest = local_candles[-1]
    latest_ts = int(latest.ts)
    now_ms = int(now_ms if now_ms is not None else time.time() * 1000)
    if not getattr(latest, "confirmed", True) and now_ms < latest_ts + bar_ms:
        return False
    if now_ms >= latest_ts + bar_ms:
        return True
    if len(local_candles) < 2:
        return False
    max_gap = 0
    for left, right in zip(local_candles, local_candles[1:]):
        gap = int(right.ts) - int(left.ts)
        if gap > max_gap:
            max_gap = gap
    return max_gap > (bar_ms * 2)


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(value, upper))


def _full_native_x_range(total_bars: int) -> tuple[float, float]:
    if total_bars <= 1:
        return 0.0, 1.0
    return 0.0, float(total_bars - 1)


def _default_native_visible_range(total_bars: int, *, target_visible_bars: int = 240) -> tuple[float, float]:
    full_min, full_max = _full_native_x_range(total_bars)
    if total_bars <= 1:
        return full_min, full_max
    visible_bars = max(30, min(total_bars, target_visible_bars))
    span = max(float(visible_bars - 1), 1.0)
    return max(full_min, full_max - span), full_max


def _native_right_padding_ms(display_step_ms: int, *, padding_bars: int = _NATIVE_RIGHT_PADDING_BARS) -> float:
    return float(max(0, padding_bars) * max(1, display_step_ms))


def _default_native_x_range_with_right_padding(
    display_times_ms: list[int],
    *,
    display_step_ms: int,
    target_visible_bars: int = 240,
    right_padding_bars: int = _NATIVE_RIGHT_PADDING_BARS,
) -> tuple[float, float]:
    if not display_times_ms:
        padding_ms = _native_right_padding_ms(display_step_ms, padding_bars=right_padding_bars)
        return 0.0, max(float(display_step_ms), padding_ms)
    left_index, right_index = _default_native_visible_range(len(display_times_ms), target_visible_bars=target_visible_bars)
    start_x = float(display_times_ms[int(left_index)])
    end_x = float(display_times_ms[int(right_index)])
    span = max(end_x - start_x, float(max(1, display_step_ms)))
    padded_end_x = float(display_times_ms[-1]) + _native_right_padding_ms(display_step_ms, padding_bars=right_padding_bars)
    padded_start_x = max(float(display_times_ms[0]), padded_end_x - span)
    return padded_start_x, padded_end_x


def _default_kline_splitter_sizes(
    total_width: int,
    *,
    left_ratio: float = _KLINE_SPLITTER_LEFT_RATIO,
    minimum_left: int = 220,
) -> tuple[int, int]:
    safe_total = max(400, int(total_width))
    requested_left = int(round(safe_total * max(0.05, min(left_ratio, 0.4))))
    left_width = max(minimum_left, requested_left)
    max_left = max(220, safe_total - 320)
    left_width = min(left_width, max_left)
    right_width = max(320, safe_total - left_width)
    return left_width, right_width


def _default_chart_stack_splitter_sizes(
    total_height: int,
    *,
    top_ratio: float = _SECONDARY_CHART_TOP_RATIO,
    minimum_top: int = 260,
    minimum_bottom: int = 180,
) -> tuple[int, int]:
    safe_total = max(minimum_top + minimum_bottom, int(total_height))
    requested_top = int(round(safe_total * max(0.2, min(top_ratio, 0.8))))
    top_height = max(minimum_top, requested_top)
    max_top = max(minimum_top, safe_total - minimum_bottom)
    top_height = min(top_height, max_top)
    bottom_height = max(minimum_bottom, safe_total - top_height)
    return top_height, bottom_height


def _default_chart_stack_horizontal_sizes(
    total_width: int,
    *,
    left_ratio: float = _SECONDARY_CHART_SIDE_RATIO,
    minimum_left: int = 420,
    minimum_right: int = 380,
) -> tuple[int, int]:
    safe_total = max(minimum_left + minimum_right, int(total_width))
    requested_left = int(round(safe_total * max(0.3, min(left_ratio, 0.75))))
    left_width = max(minimum_left, requested_left)
    max_left = max(minimum_left, safe_total - minimum_right)
    left_width = min(left_width, max_left)
    right_width = max(minimum_right, safe_total - left_width)
    return left_width, right_width


def _display_x_for_candle_time(
    candles: list[dict[str, Any]],
    display_times_ms: list[int],
    *,
    candle_time: int,
    display_step_ms: int,
) -> float:
    if not candles or not display_times_ms:
        return 0.0
    first_time = int(candles[0]["time"])
    last_time = int(candles[-1]["time"])
    first_display = float(display_times_ms[0])
    last_display = float(display_times_ms[-1])
    step_ms = max(1, int(display_step_ms))
    step_seconds = max(1, step_ms // 1000)
    display_by_time = {int(candle["time"]): float(display_times_ms[index]) for index, candle in enumerate(candles)}
    if candle_time in display_by_time:
        return display_by_time[candle_time]
    if candle_time > last_time:
        future_steps = max(1, math.ceil((candle_time - last_time) / float(step_seconds)))
        return last_display + (future_steps * step_ms)
    if candle_time < first_time:
        past_steps = max(1, math.ceil((first_time - candle_time) / float(step_seconds)))
        return first_display - (past_steps * step_ms)
    nearest_time = min(display_by_time, key=lambda item: abs(item - candle_time))
    return display_by_time[nearest_time]


def _resolve_candle_time_from_x_value(
    candles: list[dict[str, Any]],
    display_times_ms: list[int],
    *,
    x_value: float,
    display_step_ms: int,
) -> int:
    if not candles:
        return 0
    if not display_times_ms:
        return int(candles[-1]["time"])
    step_ms = max(1, int(display_step_ms))
    step_seconds = max(1, step_ms // 1000)
    first_display = float(display_times_ms[0])
    last_display = float(display_times_ms[-1])
    if x_value > last_display:
        future_steps = max(1, math.ceil((x_value - last_display) / float(step_ms)))
        return int(candles[-1]["time"]) + (future_steps * step_seconds)
    raw_index = round((x_value - first_display) / float(step_ms))
    candle_index = int(_clamp(float(raw_index), 0.0, float(len(candles) - 1)))
    return int(candles[candle_index]["time"])


def _display_step_ms(period: str, candles: list[dict[str, Any]]) -> int:
    bar_ms = _bar_to_ms(period)
    if bar_ms > 0:
        return bar_ms
    if len(candles) >= 2:
        diffs = [
            max(1, (int(right["time"]) - int(left["time"])) * 1000)
            for left, right in zip(candles, candles[1:])
            if int(right["time"]) > int(left["time"])
        ]
        if diffs:
            return min(diffs)
    return 60_000


def _build_display_times_ms(candles: list[dict[str, Any]], period: str) -> list[int]:
    if not candles:
        return []
    step_ms = _display_step_ms(period, candles)
    last_real_ms = int(candles[-1]["time"]) * 1000
    start_display_ms = last_real_ms - ((len(candles) - 1) * step_ms)
    return [start_display_ms + (index * step_ms) for index in range(len(candles))]


def _prefer_native_chart_backend() -> bool:
    if os.environ.get("QQOKX_KLINE_NATIVE", "").strip() == "1":
        return True
    return os.environ.get("QTWEBENGINE_DISABLE_GPU", "").strip() == "1"


def _line_kind_text(kind: str) -> str:
    mapping = {
        "horizontal": "水平线",
        "trend": "趋势线",
    }
    return mapping.get(str(kind or "").strip().lower(), str(kind or "-") or "-")


def _line_trigger_text(trigger: str) -> str:
    mapping = {
        "cross_above": "上穿",
        "cross_below": "下破",
        "touch": "触碰",
    }
    return mapping.get(str(trigger or "").strip().lower(), str(trigger or "-") or "-")


def _line_action_text(action: str) -> str:
    mapping = {
        "notify": "提醒",
        "long": "做多",
        "short": "做空",
    }
    return mapping.get(str(action or "").strip().lower(), str(action or "-") or "-")


def _line_state_text(enabled: bool) -> str:
    return "启用" if enabled else "停用"


def _source_status_text(source: str) -> str:
    normalized = str(source or "").strip()
    return _SOURCE_STATUS_LABELS.get(normalized, normalized or "-")


def _supports_trend_indicator(period: str) -> bool:
    return str(period or "").strip().upper() in {"1H", "4H", "1D"}


def _trend_indicator_title(period: str) -> str:
    normalized = str(period or "").strip().upper()
    if normalized == "1H":
        return "1小时趋势"
    if normalized == "4H":
        return "4小时趋势"
    if normalized == "1D":
        return "日线趋势"
    return f"{normalized or '-'}趋势"


def _daily_trend_state(bias: float, slope: float) -> str:
    if slope != slope or bias != bias:
        return "neutral"
    return "strong_bull" if bias >= 0 else "strong_bear"


def _build_daily_trend_indicator(
    *,
    period: str,
    times: list[int],
    closes: list[float],
    sma50: list[float],
) -> list[dict[str, Any]]:
    trend_points: list[dict[str, Any]] = []
    title = _trend_indicator_title(period)
    point_count = min(len(times), len(closes), len(sma50))
    for index in range(point_count):
        ma50 = sma50[index]
        prev_ma50 = sma50[index - 5] if index >= 5 else None
        if ma50 <= 0 or not prev_ma50 or prev_ma50 <= 0:
            state = "neutral"
            bias = 0.0
            slope = 0.0
        else:
            bias = (closes[index] - ma50) / ma50
            slope = (ma50 - prev_ma50) / prev_ma50
            state = _daily_trend_state(bias=bias, slope=slope)
        trend_points.append(
            {
                "time": times[index],
                "value": 1.0,
                "state": state,
                "label": _DAILY_TREND_BAND_LABEL[state],
                "color": _DAILY_TREND_BAND_COLOR[state],
                "title": title,
                "bias": bias,
                "slope": slope,
            }
        )
    return trend_points


def _distance_to_line_pct(candle: Any, line_value: float) -> float | None:
    if line_value <= 0:
        return None
    low = _to_float(candle.low)
    high = _to_float(candle.high)
    close = _to_float(candle.close)
    if low <= line_value <= high:
        return 0.0
    distance = min(abs(low - line_value), abs(high - line_value), abs(close - line_value))
    return distance / line_value


def _candle_crosses_line(candle: Any, line_value: float) -> bool:
    if line_value <= 0:
        return False
    return _to_float(candle.low) <= line_value <= _to_float(candle.high)


def _daily_signal_has_recent_ma_cross(
    *,
    candles: list[Any],
    signal_index: int,
    candle_count: int,
    ema15_values: list[float],
    sma50_values: list[float],
    include_ema15: bool = True,
    include_sma50: bool = True,
) -> bool:
    pattern_start = max(0, signal_index - max(int(candle_count), 1) + 1)
    check_start = max(0, pattern_start - 2)
    for index in range(check_start, signal_index + 1):
        if index >= len(candles) or index >= len(ema15_values) or index >= len(sma50_values):
            continue
        candle = candles[index]
        ema15_crossed = include_ema15 and _candle_crosses_line(candle, float(ema15_values[index]))
        sma50_crossed = include_sma50 and _candle_crosses_line(candle, float(sma50_values[index]))
        if ema15_crossed or sma50_crossed:
            return True
    return False


def _signal_fractal_valid(
    *,
    candles: list[Any],
    signal_index: int,
    pattern_id: str,
) -> bool:
    pivot_index = signal_index - 1
    if pivot_index < 0 or pivot_index >= len(candles):
        return False
    if pivot_index < 4:
        return False
    window_start = pivot_index - 4
    window_end = pivot_index + 1
    pivot_high = _to_float(candles[pivot_index].high)
    pivot_low = _to_float(candles[pivot_index].low)
    window_high = max(_to_float(item.high) for item in candles[window_start:window_end])
    window_low = min(_to_float(item.low) for item in candles[window_start:window_end])
    normalized_pattern_id = pattern_id.strip().lower()
    if normalized_pattern_id == "top_fractal":
        return pivot_high == window_high
    if normalized_pattern_id == "bottom_fractal":
        return pivot_low == window_low
    return True


def _build_replay_signal_markers(
    *,
    candles: list[Any],
    period: str,
    ema15_values: list[float],
    sma50_values: list[float],
) -> list[dict[str, Any]]:
    normalized_period = period.strip().upper()
    if normalized_period not in {"4H", "1H", "1D"} or not candles:
        return []
    include_ema15 = normalized_period != "1H"
    include_sma50 = True
    dataset = build_signal_replay_dataset(
        list(candles),
        config=SignalReplayConfig(
            confirmed_only=False,
            include_long=False,
            include_short=False,
            enable_pattern_signals=True,
        ),
    )
    markers: list[dict[str, Any]] = []
    seen: set[tuple[int, str]] = set()
    for signal in dataset.signals:
        if signal.pattern_id not in _REPLAY_SIGNAL_LABELS:
            continue
        index = int(signal.index)
        if index < 0 or index >= len(candles) or index >= len(ema15_values) or index >= len(sma50_values):
            continue
        candle = candles[index]
        if not bool(getattr(candle, "confirmed", True)):
            continue
        if not _signal_fractal_valid(
            candles=candles,
            signal_index=index,
            pattern_id=str(signal.pattern_id),
        ):
            continue
        if not _daily_signal_has_recent_ma_cross(
            candles=candles,
            signal_index=index,
            candle_count=int(signal.candle_count),
            ema15_values=ema15_values,
            sma50_values=sma50_values,
            include_ema15=include_ema15,
            include_sma50=include_sma50,
        ):
            continue
        ema_distance = _distance_to_line_pct(candle, float(ema15_values[index]))
        sma_distance = _distance_to_line_pct(candle, float(sma50_values[index]))
        near_items: list[tuple[str, float | None]] = []
        if include_ema15:
            near_items.append(("EMA15", ema_distance))
        if include_sma50:
            near_items.append(("MA50", sma_distance))
        valid_near = [(name, value) for name, value in near_items if value is not None and value <= _REPLAY_SIGNAL_NEAR_MA_MAX_PCT]
        if not valid_near:
            continue
        key = (index, signal.pattern_id)
        if key in seen:
            continue
        seen.add(key)
        nearest_name, nearest_distance = min(valid_near, key=lambda item: item[1])
        direction = str(signal.direction or "neutral").strip().lower()
        color = _REPLAY_SIGNAL_LONG_COLOR if direction == "long" else _REPLAY_SIGNAL_SHORT_COLOR
        label = _REPLAY_SIGNAL_LABELS.get(signal.pattern_id, signal.pattern_name)
        markers.append(
            {
                "index": index,
                "time": _to_ms_seconds(int(signal.ts)),
                "direction": direction,
                "pattern_id": signal.pattern_id,
                "label": label,
                "text": f"{label} @{nearest_name}",
                "near_ma": nearest_name,
                "distance_pct": float(nearest_distance or 0.0) * 100.0,
                "score": int(signal.score),
                "color": color,
            }
        )
    return markers


def _build_box_history_overlays(candles: list[Any]) -> list[dict[str, Any]]:
    if len(candles) < 48:
        return []
    scan_candles = list(candles[-_BOX_HISTORY_SCAN_LIMIT:])
    scan_offset = len(candles) - len(scan_candles)
    opens = [_to_float(item.open) for item in scan_candles]
    highs = [_to_float(item.high) for item in scan_candles]
    lows = [_to_float(item.low) for item in scan_candles]
    closes = [_to_float(item.close) for item in scan_candles]
    ema15 = _to_ema(closes, 15)
    ma50 = _to_sma(closes, 50)
    atr_values = _simple_atr_values(highs=highs, lows=lows, closes=closes, period=14)

    candidates: list[dict[str, Any]] = []
    min_box_bars = 12
    max_box_bars = 54
    trend_lookback = 22
    for end_index in range(trend_lookback + min_box_bars, len(scan_candles)):
        for box_len in range(min_box_bars, min(max_box_bars, end_index - trend_lookback) + 1):
            start_index = end_index - box_len + 1
            candidate = _score_manual_style_box_window(
                start_index=start_index,
                end_index=end_index,
                opens=opens,
                highs=highs,
                lows=lows,
                closes=closes,
                ema15=ema15,
                ma50=ma50,
                atr_values=atr_values,
                trend_lookback=trend_lookback,
            )
            if candidate is not None:
                candidates.append(candidate)
    if not candidates:
        return []

    selected: list[dict[str, Any]] = []
    for candidate in sorted(candidates, key=lambda item: float(item.get("score", 0.0)), reverse=True):
        if any(_box_window_overlap_ratio(candidate, item) > 0.45 for item in selected):
            continue
        selected.append(candidate)
        if len(selected) >= _BOX_HISTORY_MAX_SEGMENTS:
            break
    selected.sort(key=lambda item: int(item.get("start_index", 0)))

    overlays: list[dict[str, Any]] = []
    for index, box in enumerate(selected):
        is_active = index == len(selected) - 1
        upper = float(box["upper"])
        lower = float(box["lower"])
        overlays.append(
            {
                "start_index": int(box["start_index"]) + scan_offset,
                "end_index": int(box["end_index"]) + scan_offset,
                "upper": upper,
                "lower": lower,
                "mode": "history",
                "label": f"绠变綋 {lower:.2f}-{upper:.2f}",
                "touches": int(box["touches"]),
                "violations": int(box["violations"]),
                "trend": str(box.get("trend", "")),
                "score": float(box.get("score", 0.0)),
                "active": is_active,
                "outline": _BOX_ACTIVE_OUTLINE_COLOR if is_active else _BOX_HISTORY_OUTLINE_COLOR,
                "fill": _BOX_ACTIVE_FILL_COLOR if is_active else _BOX_HISTORY_FILL_COLOR,
            }
        )
    return overlays


def _build_box_realtime_overlay(candles: list[Any]) -> list[dict[str, Any]]:
    if len(candles) < 48:
        return []
    scan_candles = list(candles[-_BOX_HISTORY_SCAN_LIMIT:])
    scan_offset = len(candles) - len(scan_candles)
    opens = [_to_float(item.open) for item in scan_candles]
    highs = [_to_float(item.high) for item in scan_candles]
    lows = [_to_float(item.low) for item in scan_candles]
    closes = [_to_float(item.close) for item in scan_candles]
    ema15 = _to_ema(closes, 15)
    ma50 = _to_sma(closes, 50)
    atr_values = _simple_atr_values(highs=highs, lows=lows, closes=closes, period=14)

    min_box_bars = 12
    max_box_bars = 54
    trend_lookback = 22
    if len(scan_candles) <= trend_lookback + min_box_bars:
        return []

    candidates: list[dict[str, Any]] = []
    for end_index in range(trend_lookback + min_box_bars, len(scan_candles)):
        for box_len in range(min_box_bars, min(max_box_bars, end_index - trend_lookback) + 1):
            start_index = end_index - box_len + 1
            candidate = _score_manual_style_box_window(
                start_index=start_index,
                end_index=end_index,
                opens=opens,
                highs=highs,
                lows=lows,
                closes=closes,
                ema15=ema15,
                ma50=ma50,
                atr_values=atr_values,
                trend_lookback=trend_lookback,
            )
            if candidate is not None:
                candidates.append(candidate)
    if not candidates:
        return []

    selected: list[dict[str, Any]] = []
    for candidate in sorted(candidates, key=lambda item: float(item.get("score", 0.0)), reverse=True):
        if any(_box_window_overlap_ratio(candidate, item) > 0.45 for item in selected):
            continue
        selected.append(candidate)
        if len(selected) >= _BOX_HISTORY_MAX_SEGMENTS:
            break
    if not selected:
        return []

    selected.sort(key=lambda item: int(item.get("start_index", 0)))
    overlays: list[dict[str, Any]] = []
    for index, box in enumerate(selected):
        is_active = index == len(selected) - 1
        upper = float(box["upper"])
        lower = float(box["lower"])
        overlays.append(
            {
                "start_index": int(box["start_index"]) + scan_offset,
                "end_index": int(box["end_index"]) + scan_offset,
                "upper": upper,
                "lower": lower,
                "mode": "realtime",
                "label": f"实盘箱体 {lower:.2f}-{upper:.2f}",
                "touches": int(box["touches"]),
                "violations": int(box["violations"]),
                "trend": str(box.get("trend", "")),
                "score": float(box.get("score", 0.0)),
                "active": is_active,
                "outline": _BOX_ACTIVE_OUTLINE_COLOR if is_active else _BOX_LIVE_OUTLINE_COLOR,
                "fill": _BOX_ACTIVE_FILL_COLOR if is_active else _BOX_LIVE_FILL_COLOR,
            }
        )
    return overlays
def _score_manual_style_box_window(
    *,
    start_index: int,
    end_index: int,
    opens: list[float],
    highs: list[float],
    lows: list[float],
    closes: list[float],
    ema15: list[float],
    ma50: list[float],
    atr_values: list[float],
    trend_lookback: int,
) -> dict[str, Any] | None:
    trend_start = start_index - trend_lookback
    if trend_start < 0 or end_index <= start_index:
        return None
    window_len = end_index - start_index + 1
    price = max(abs(closes[end_index]), 1.0)
    prev_price = max(abs(closes[start_index - 1]), 1.0)
    atr = max(_mean([value for value in atr_values[start_index : end_index + 1] if value > 0]), price * 0.001)
    atr_pct = atr / price

    trend_move = closes[start_index - 1] - closes[trend_start]
    trend_abs_pct = abs(trend_move) / prev_price
    if trend_abs_pct < max(0.012, atr_pct * 2.2):
        return None
    trend_direction = "up" if trend_move > 0 else "down"

    body_highs = [max(opens[index], closes[index]) for index in range(start_index, end_index + 1)]
    body_lows = [min(opens[index], closes[index]) for index in range(start_index, end_index + 1)]
    upper = max(_quantile(highs[start_index : end_index + 1], 0.82), _quantile(body_highs, 0.88))
    lower = min(_quantile(lows[start_index : end_index + 1], 0.18), _quantile(body_lows, 0.12))
    if upper <= lower:
        return None
    width = upper - lower
    width_pct = width / price
    trend_range = max(highs[trend_start : start_index]) - min(lows[trend_start : start_index])
    max_width_pct = min(0.075, max(0.018, atr_pct * 7.0))
    if width_pct > max_width_pct:
        return None
    if trend_range > 0 and width > trend_range * 0.82:
        return None

    boundary_tolerance = max(width * 0.12, atr * 0.35)
    contained = 0
    upper_touches = 0
    lower_touches = 0
    ma_touches = 0
    for index in range(start_index, end_index + 1):
        if body_highs[index - start_index] <= upper + boundary_tolerance and body_lows[index - start_index] >= lower - boundary_tolerance:
            contained += 1
        if abs(highs[index] - upper) <= boundary_tolerance or abs(body_highs[index - start_index] - upper) <= boundary_tolerance:
            upper_touches += 1
        if abs(lows[index] - lower) <= boundary_tolerance or abs(body_lows[index - start_index] - lower) <= boundary_tolerance:
            lower_touches += 1
        near_threshold = max(width * 0.24, atr * 0.55)
        if (
            lows[index] <= ema15[index] <= highs[index]
            or lows[index] <= ma50[index] <= highs[index]
            or abs(closes[index] - ema15[index]) <= near_threshold
            or abs(closes[index] - ma50[index]) <= near_threshold
        ):
            ma_touches += 1

    containment_ratio = contained / window_len
    ma_touch_ratio = ma_touches / window_len
    if containment_ratio < 0.72:
        return None
    if upper_touches < 2 or lower_touches < 2:
        return None
    if ma_touch_ratio < 0.34:
        return None

    close_drift_pct = abs(closes[end_index] - closes[start_index]) / max(width, 1.0)
    if close_drift_pct > 0.95:
        return None

    ma50_slope = abs(ma50[end_index] - ma50[start_index]) / price
    ema15_slope = abs(ema15[end_index] - ema15[start_index]) / price
    slope_gap = abs((ema15[end_index] - ema15[start_index]) - (ma50[end_index] - ma50[start_index])) / price
    ma_distance = _mean([abs(ema15[index] - ma50[index]) / max(abs(closes[index]), 1.0) for index in range(start_index, end_index + 1)])
    if ma50_slope > max(0.018, atr_pct * 3.2):
        return None
    if slope_gap > max(0.020, atr_pct * 3.6):
        return None
    if ma_distance > max(0.045, atr_pct * 6.5):
        return None

    score = (
        (containment_ratio * 28.0)
        + (ma_touch_ratio * 24.0)
        + (min(upper_touches + lower_touches, 12) * 2.0)
        + (min(trend_abs_pct, 0.08) * 120.0)
        - (width_pct * 180.0)
        - (close_drift_pct * 5.0)
        - (ma50_slope * 120.0)
        - (slope_gap * 100.0)
    )
    return {
        "start_index": start_index,
        "end_index": end_index,
        "upper": upper,
        "lower": lower,
        "touches": upper_touches + lower_touches,
        "violations": window_len - contained,
        "trend": trend_direction,
        "score": score,
    }


def _simple_atr_values(*, highs: list[float], lows: list[float], closes: list[float], period: int) -> list[float]:
    true_ranges: list[float] = []
    for index, high in enumerate(highs):
        low = lows[index]
        prev_close = closes[index - 1] if index > 0 else closes[index]
        true_ranges.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
    values: list[float] = []
    for index in range(len(true_ranges)):
        start = max(0, index - period + 1)
        values.append(_mean(true_ranges[start : index + 1]))
    return values


def _box_window_overlap_ratio(left: dict[str, Any], right: dict[str, Any]) -> float:
    left_start = int(left.get("start_index", 0))
    left_end = int(left.get("end_index", 0))
    right_start = int(right.get("start_index", 0))
    right_end = int(right.get("end_index", 0))
    overlap = max(0, min(left_end, right_end) - max(left_start, right_start) + 1)
    shortest = max(1, min(left_end - left_start + 1, right_end - right_start + 1))
    return overlap / shortest


def _quantile(values: list[float], ratio: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(float(value) for value in values)
    position = _clamp(float(ratio), 0.0, 1.0) * float(len(ordered) - 1)
    lower_index = int(math.floor(position))
    upper_index = int(math.ceil(position))
    if lower_index == upper_index:
        return ordered[lower_index]
    weight = position - float(lower_index)
    return ordered[lower_index] * (1.0 - weight) + ordered[upper_index] * weight


def _mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _trend_state_at_time(trend_payload: list[dict[str, Any]], candle_time_ms: int) -> str | None:
    if not trend_payload:
        return None

    matched_time = -1
    matched_state: str | None = None
    for item in trend_payload:
        if not isinstance(item, dict):
            continue
        try:
            item_time = int(item.get("time", -1))
        except (TypeError, ValueError):
            continue
        if item_time > candle_time_ms:
            if matched_time >= 0:
                break
            return None
        state = item.get("state")
        if item_time >= matched_time and isinstance(state, str):
            matched_state = state
            matched_time = item_time
    return matched_state


def _is_bull_trend(state: str | None) -> bool:
    return isinstance(state, str) and "bull" in state


def _is_bear_trend(state: str | None) -> bool:
    return isinstance(state, str) and "bear" in state


def _normalize_signal_time(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


@dataclass(frozen=True)
class KlineChartPayload:
    candles: list[dict[str, Any]]
    ema_9: list[dict[str, Any]]
    ema_21: list[dict[str, Any]]
    ema_55: list[dict[str, Any]]
    trend_indicator: list[dict[str, Any]]
    signal_markers: list[dict[str, Any]]
    box_overlays: list[dict[str, Any]]
    raw_candles: list[Any]
    stats: dict[str, Any]
    alert_snapshot: KlineAlertSnapshot | None = None


@dataclass(frozen=True)
class KlineAlertSnapshot:
    workspace_entry: dict[str, object]
    new_events: list[dict[str, object]]
    structure: dict[str, object]


def _slice_chart_payload_tail(payload: KlineChartPayload, count: int) -> KlineChartPayload:
    if count <= 0 or len(payload.candles) <= count:
        return payload
    offset = len(payload.candles) - count
    return KlineChartPayload(
        candles=payload.candles[-count:],
        ema_9=payload.ema_9[-count:],
        ema_21=payload.ema_21[-count:],
        ema_55=payload.ema_55[-count:],
        trend_indicator=payload.trend_indicator[-count:],
        signal_markers=[
            dict(item, index=int(item.get("index", 0)) - offset)
            for item in payload.signal_markers
            if int(item.get("index", -1)) >= offset
        ],
        box_overlays=[
            dict(
                item,
                start_index=max(0, int(item.get("start_index", 0)) - offset),
                end_index=max(0, int(item.get("end_index", 0)) - offset),
            )
            for item in payload.box_overlays
            if int(item.get("end_index", -1)) >= offset
        ],
        raw_candles=payload.raw_candles[-count:],
        stats=payload.stats,
        alert_snapshot=payload.alert_snapshot,
    )


if QChartView is not None:
    class InteractiveKlineChartView(QChartView):
        chartPointClicked = Signal(float, float)
        hoverTimeChanged = Signal(object)
        xRangeChanged = Signal(float, float)
        chartActivated = Signal()
        chartPointerPressed = Signal(float, float)
        chartPointerMoved = Signal(float, float)
        chartPointerReleased = Signal(float, float)

        def __init__(self, chart: QChart, parent: QWidget | None = None) -> None:
            super().__init__(chart, parent)
            self.setRenderHint(QPainter.RenderHint.Antialiasing, False)
            self.setMouseTracking(True)
            self.viewport().setMouseTracking(True)
            self.setFrameShape(QFrame.Shape.NoFrame)
            self._draw_mode_enabled = False
            self._axis_x: QDateTimeAxis | None = None
            self._axis_y: QValueAxis | None = None
            self._candles: list[dict[str, Any]] = []
            self._overlay_values: list[list[float]] = []
            self._trend_indicators: list[dict[str, Any]] = []
            self._display_times_ms: list[int] = []
            self._display_step_ms = 60_000
            self._period = "15m"
            self._symbol = ""
            self._venue_label = "OKX"
            self._indicator_series: list[dict[str, Any]] = []
            self._chart_note_lines: list[str] = []
            self._workspace_lines: list[dict[str, object]] = []
            self._signal_markers: list[dict[str, Any]] = []
            self._box_overlays: list[dict[str, Any]] = []
            self._selected_workspace_line_index = -1
            self._hovered_workspace_line_index = -1
            self._hovered_workspace_drag_mode: str | None = None
            self._preview_line: dict[str, object] | None = None
            self._full_x_min = 0.0
            self._full_x_max = 1.0
            self._full_y_min = 0.0
            self._full_y_max = 1.0
            self._hover_pos: QPointF | None = None
            self._press_pos: QPointF | None = None
            self._pan_anchor_x: float | None = None
            self._dragging = False
            self._interaction_locked = False
            self._external_hover_time: int | None = None
            self._suppress_x_range_signal = False
            self._price_badge = self._create_hover_label(multiline=False, center=True)
            self._time_badge = self._create_hover_label(multiline=False, center=True)
            self._tooltip_badge = self._create_hover_label(multiline=True, center=False)
            self._hide_hover_overlays()
            self._apply_cursor_mode("default")

        def set_draw_mode_enabled(self, enabled: bool) -> None:
            self._draw_mode_enabled = enabled
            self._pan_anchor_x = None
            self._press_pos = None
            self._dragging = False
            self._apply_cursor_mode("crosshair" if enabled else "default")

        def set_interaction_locked(self, locked: bool) -> None:
            self._interaction_locked = bool(locked)
            if locked:
                self._pan_anchor_x = None
                self._press_pos = None
                self._dragging = False
            else:
                self._apply_cursor_mode(_resolve_interaction_cursor_mode("default", interaction_locked=False, draw_mode_enabled=self._draw_mode_enabled))

        def set_interaction_cursor_mode(self, mode: str) -> None:
            self._apply_cursor_mode(
                _resolve_interaction_cursor_mode(
                    mode,
                    interaction_locked=self._interaction_locked,
                    draw_mode_enabled=self._draw_mode_enabled,
                )
            )

        def _apply_cursor_mode(self, mode: str) -> None:
            cursor_map = {
                "default": Qt.CursorShape.ArrowCursor,
                "crosshair": Qt.CursorShape.CrossCursor,
                "move": Qt.CursorShape.SizeAllCursor,
                "endpoint": Qt.CursorShape.PointingHandCursor,
                "dragging": Qt.CursorShape.ClosedHandCursor,
            }
            self.viewport().setCursor(cursor_map.get(mode, Qt.CursorShape.ArrowCursor))

        def set_external_hover_time(self, candle_time: int | None) -> None:
            self._external_hover_time = int(candle_time) if candle_time is not None else None
            if self._hover_pos is None:
                self.viewport().update()

        def set_preview_line(self, preview_line: dict[str, object] | None) -> None:
            self._preview_line = dict(preview_line) if isinstance(preview_line, dict) else None
            self.viewport().update()

        def set_hovered_workspace_interaction(
            self,
            hovered_index: int,
            hovered_drag_mode: str | None,
        ) -> None:
            normalized_mode = str(hovered_drag_mode or "").strip().lower() or None
            if (
                self._hovered_workspace_line_index == int(hovered_index)
                and self._hovered_workspace_drag_mode == normalized_mode
            ):
                return
            self._hovered_workspace_line_index = int(hovered_index)
            self._hovered_workspace_drag_mode = normalized_mode
            self.viewport().update()

        def capture_view_state(self) -> dict[str, float | bool] | None:
            if self._axis_x is None:
                return None
            start_x, end_x = self.current_x_range()
            return {
                "start_x": start_x,
                "end_x": end_x,
                "stick_to_right": abs(self._full_x_max - end_x) <= 2.0,
            }

        def current_x_range(self) -> tuple[float, float]:
            if self._axis_x is None:
                return self._full_x_min, self._full_x_max
            return (
                float(self._axis_x.min().toMSecsSinceEpoch()),
                float(self._axis_x.max().toMSecsSinceEpoch()),
            )

        def set_chart_context(
            self,
            *,
            axis_x: QDateTimeAxis,
            axis_y: QValueAxis,
            candles: list[dict[str, Any]],
            overlay_values: list[list[float]],
            display_times_ms: list[int],
            period: str,
            symbol: str,
            venue_label: str = "OKX",
            indicator_series: list[dict[str, Any]] | None = None,
            chart_note_lines: list[str] | None = None,
            workspace_lines: list[dict[str, object]] | None = None,
            trend_indicators: list[dict[str, Any]] | None = None,
            signal_markers: list[dict[str, Any]] | None = None,
            box_overlays: list[dict[str, Any]] | None = None,
            selected_workspace_line_index: int = -1,
            hovered_workspace_line_index: int = -1,
            hovered_workspace_drag_mode: str | None = None,
            restore_state: dict[str, float | bool] | None = None,
        ) -> None:
            self._axis_x = axis_x
            self._axis_y = axis_y
            self._candles = list(candles)
            self._overlay_values = [list(item) for item in overlay_values]
            self._display_times_ms = list(display_times_ms)
            self._period = period.strip() or "15m"
            self._symbol = symbol.strip().upper()
            self._venue_label = venue_label.strip().upper() or "OKX"
            self._indicator_series = list(indicator_series or [])
            self._chart_note_lines = [str(item).strip() for item in (chart_note_lines or []) if str(item).strip()]
            self._trend_indicators = [dict(item) for item in (trend_indicators or []) if isinstance(item, dict)]
            self._workspace_lines = [dict(item) for item in (workspace_lines or []) if isinstance(item, dict)]
            self._signal_markers = [dict(item) for item in (signal_markers or []) if isinstance(item, dict)]
            self._box_overlays = [dict(item) for item in (box_overlays or []) if isinstance(item, dict)]
            self._selected_workspace_line_index = int(selected_workspace_line_index)
            self._hovered_workspace_line_index = int(hovered_workspace_line_index)
            self._hovered_workspace_drag_mode = str(hovered_workspace_drag_mode or "").strip().lower() or None
            self._display_step_ms = _display_step_ms(self._period, candles)
            if len(self._display_times_ms) >= 2:
                self._display_step_ms = max(1, self._display_times_ms[1] - self._display_times_ms[0])
            if self._display_times_ms:
                self._full_x_min = float(self._display_times_ms[0])
                self._full_x_max = float(self._display_times_ms[-1]) + _native_right_padding_ms(self._display_step_ms)
            else:
                self._full_x_min = 0.0
                self._full_x_max = max(float(self._display_step_ms), _native_right_padding_ms(self._display_step_ms))
            if candles:
                self._full_y_min = min(float(item["low"]) for item in candles)
                self._full_y_max = max(float(item["high"]) for item in candles)
            else:
                self._full_y_min = 0.0
                self._full_y_max = 1.0
            start_x, end_x = self._restore_x_range(restore_state)
            self._apply_x_range(start_x, end_x, emit_signal=False)
            self._fit_y_axis_to_visible_range()
            self._hover_pos = None
            self._hide_hover_overlays()
            self.viewport().update()

        def reset_view(self) -> None:
            if self._axis_x is None:
                return
            start_x, end_x = self._default_x_range()
            self._apply_x_range(start_x, end_x, emit_signal=True)
            self._fit_y_axis_to_visible_range()
            self._hover_pos = None
            self._hide_hover_overlays()
            self.viewport().update()

        def set_external_x_range(self, start_x: float, end_x: float) -> None:
            if self._axis_x is None or not self._candles:
                return
            bounded_start, bounded_end = self._bounded_x_range(float(start_x), float(end_x))
            self._apply_x_range(bounded_start, bounded_end, emit_signal=False)
            self._fit_y_axis_to_visible_range()
            self.viewport().update()

        def wheelEvent(self, event) -> None:  # noqa: ANN001
            axis_x = self._axis_x
            if axis_x is None or not self._candles:
                super().wheelEvent(event)
                return
            plot_area = self.chart().plotArea()
            if not plot_area.contains(event.position()):
                super().wheelEvent(event)
                return
            current_x_min, current_x_max = self.current_x_range()
            current_x_span = max(current_x_max - current_x_min, 1.0)
            full_x_span = max(self._full_x_max - self._full_x_min, 1.0)
            min_x_span = min(full_x_span, max(float(self._display_step_ms * 20), float(self._display_step_ms * 60)))
            factor = 0.82 if event.angleDelta().y() > 0 else 1.22
            anchor_ratio = _clamp(
                (float(event.position().x()) - float(plot_area.left())) / max(float(plot_area.width()), 1.0),
                0.0,
                1.0,
            )
            anchor_x = current_x_min + (current_x_span * anchor_ratio)
            new_x_span = _clamp(current_x_span * factor, min_x_span, full_x_span)
            new_x_min = anchor_x - (new_x_span * anchor_ratio)
            new_x_max = new_x_min + new_x_span
            if new_x_min < self._full_x_min:
                new_x_min = self._full_x_min
                new_x_max = new_x_min + new_x_span
            if new_x_max > self._full_x_max:
                new_x_max = self._full_x_max
                new_x_min = new_x_max - new_x_span
            self._apply_x_range(new_x_min, new_x_max, emit_signal=True)
            self._fit_y_axis_to_visible_range()
            self.viewport().update()
            event.accept()

        def mousePressEvent(self, event) -> None:  # noqa: ANN001
            plot_area = self.chart().plotArea()
            if event.button() == Qt.MouseButton.LeftButton and plot_area.contains(event.position()):
                self.chartActivated.emit()
                point = self.chart().mapToValue(event.position())
                self.chartPointerPressed.emit(float(point.x()), float(point.y()))
                self._press_pos = QPointF(event.position())
                self._pan_anchor_x = float(event.position().x())
                self._dragging = False
            super().mousePressEvent(event)

        def mouseMoveEvent(self, event) -> None:  # noqa: ANN001
            plot_area = self.chart().plotArea()
            if plot_area.contains(event.position()):
                point = self.chart().mapToValue(event.position())
                self.chartPointerMoved.emit(float(point.x()), float(point.y()))
            if self._interaction_locked and event.buttons() & Qt.MouseButton.LeftButton:
                self._hover_pos = QPointF(event.position())
                self.viewport().update()
                event.accept()
                return
            if (
                not self._draw_mode_enabled
                and not self._interaction_locked
                and self._pan_anchor_x is not None
                and self._press_pos is not None
                and event.buttons() & Qt.MouseButton.LeftButton
            ):
                movement = abs(float(event.position().x()) - float(self._press_pos.x()))
                if movement >= 4.0:
                    self._dragging = True
                if self._dragging:
                    current_x = float(event.position().x())
                    self._pan_by_pixels(current_x - self._pan_anchor_x, max(float(plot_area.width()), 1.0))
                    self._pan_anchor_x = current_x
                    self._hover_pos = QPointF(event.position())
                    self._emit_hover_time_from_position(QPointF(event.position()))
                    self.viewport().update()
                    event.accept()
                    return
            self._hover_pos = QPointF(event.position())
            self._emit_hover_time_from_position(self._hover_pos)
            self.viewport().update()
            super().mouseMoveEvent(event)

        def mouseReleaseEvent(self, event) -> None:  # noqa: ANN001
            plot_area = self.chart().plotArea()
            if event.button() == Qt.MouseButton.LeftButton:
                point = self.chart().mapToValue(event.position())
                self.chartPointerReleased.emit(float(point.x()), float(point.y()))
                if plot_area.contains(event.position()) and not self._dragging and not self._interaction_locked:
                    self.chartPointClicked.emit(float(point.x()), float(point.y()))
                if plot_area.contains(event.position()):
                    event.accept()
            self._press_pos = None
            self._pan_anchor_x = None
            self._dragging = False
            super().mouseReleaseEvent(event)

        def mouseDoubleClickEvent(self, event) -> None:  # noqa: ANN001
            self.reset_view()
            event.accept()

        def leaveEvent(self, event) -> None:  # noqa: ANN001
            self._hover_pos = None
            self._press_pos = None
            self._pan_anchor_x = None
            self._dragging = False
            self.hoverTimeChanged.emit(None)
            self._hide_hover_overlays()
            self.viewport().update()
            super().leaveEvent(event)

        def paintEvent(self, event) -> None:  # noqa: ANN001
            super().paintEvent(event)
            try:
                plot_area = self.chart().plotArea()
                if not self._candles or plot_area.width() <= 0 or plot_area.height() <= 0:
                    self._hide_hover_overlays()
                    return
                painter = QPainter(self.viewport())
                painter.setRenderHint(QPainter.RenderHint.Antialiasing, True)
                self._draw_volume_overlay(painter, plot_area)
                self._draw_box_overlays(painter, plot_area)
                self._draw_signal_markers(painter, plot_area)
                self._draw_selected_line_handles(painter, plot_area)
                self._draw_preview_line(painter, plot_area)
                hover_context = self._resolve_hover_context()
                if hover_context is None:
                    external_hover = self._resolve_external_hover_context()
                    if external_hover is not None:
                        candle, hover_index, candle_time, snapped_x = external_hover
                        self._draw_chart_header(painter, plot_area, candle, hover_index)
                        candle_color = QColor(_CHART_UP_COLOR if float(candle["close"]) >= float(candle["open"]) else _CHART_DOWN_COLOR)
                        marker_y = self._y_for_value(float(candle["close"]), plot_area)
                        cross_pen = QPen(QColor(_CHART_CROSSHAIR_COLOR), 1)
                        cross_pen.setStyle(Qt.PenStyle.DashLine)
                        painter.setPen(cross_pen)
                        painter.drawLine(QPointF(snapped_x, plot_area.top()), QPointF(snapped_x, plot_area.bottom()))
                        painter.drawLine(QPointF(plot_area.left(), marker_y), QPointF(plot_area.right(), marker_y))
                        painter.setPen(QPen(candle_color, 2))
                        painter.setBrush(QColor("#ffffff"))
                        painter.drawEllipse(QPointF(snapped_x, marker_y), 4.0, 4.0)
                        painter.end()
                        candle_dt = QDateTime.fromMSecsSinceEpoch(int(candle["time"]) * 1000)
                        self._update_hover_overlays(
                            bounds=plot_area,
                            anchor=QPointF(snapped_x, marker_y),
                            candle_color=candle_color,
                            price_text=self._format_hover_value(float(candle["close"])),
                            time_text=candle_dt.toString("MM-dd HH:mm"),
                            tooltip_lines=(
                                f"联动定位 {QDateTime.fromSecsSinceEpoch(int(candle_time)).toString('yyyy-MM-dd HH:mm')}",
                                candle_dt.toString("yyyy-MM-dd HH:mm"),
                                f"O {self._format_hover_value(float(candle['open']))}  H {self._format_hover_value(float(candle['high']))}",
                                f"L {self._format_hover_value(float(candle['low']))}  C {self._format_hover_value(float(candle['close']))}",
                                f"成交量 {self._format_hover_value(float(candle['volume']))}",
                                *self._box_tooltip_lines(hover_index),
                                *self._signal_tooltip_lines(hover_index),
                            ),
                        )
                        return
                    self._draw_chart_header(painter, plot_area, self._candles[-1], len(self._candles) - 1)
                    painter.end()
                    self._hide_hover_overlays()
                    return
                candle, hover_index, hover_value, hover_y = hover_context
                self._draw_chart_header(painter, plot_area, candle, hover_index)
                snapped_x = self._x_for_index(hover_index, plot_area)
                candle_color = QColor(_CHART_UP_COLOR if float(candle["close"]) >= float(candle["open"]) else _CHART_DOWN_COLOR)
                marker_y = self._y_for_value(float(candle["close"]), plot_area)
                cross_pen = QPen(QColor(_CHART_CROSSHAIR_COLOR), 1)
                cross_pen.setStyle(Qt.PenStyle.DashLine)
                painter.setPen(cross_pen)
                painter.drawLine(QPointF(snapped_x, plot_area.top()), QPointF(snapped_x, plot_area.bottom()))
                painter.drawLine(QPointF(plot_area.left(), hover_y), QPointF(plot_area.right(), hover_y))
                painter.setPen(QPen(candle_color, 2))
                painter.setBrush(QColor("#ffffff"))
                painter.drawEllipse(QPointF(snapped_x, marker_y), 4.0, 4.0)
                painter.end()
                candle_dt = QDateTime.fromMSecsSinceEpoch(int(candle["time"]) * 1000)
                self._update_hover_overlays(
                    bounds=plot_area,
                    anchor=QPointF(snapped_x, hover_y),
                    candle_color=candle_color,
                    price_text=self._format_hover_value(hover_value),
                    time_text=candle_dt.toString("MM-dd HH:mm"),
                    tooltip_lines=(
                        candle_dt.toString("yyyy-MM-dd HH:mm"),
                        f"O {self._format_hover_value(float(candle['open']))}  H {self._format_hover_value(float(candle['high']))}",
                        f"L {self._format_hover_value(float(candle['low']))}  C {self._format_hover_value(float(candle['close']))}",
                        f"成交量 {self._format_hover_value(float(candle['volume']))}",
                        *self._box_tooltip_lines(hover_index),
                        *self._signal_tooltip_lines(hover_index),
                    ),
                )
            except Exception:
                self._hide_hover_overlays()

        def _draw_chart_header(
            self,
            painter: QPainter,
            plot_area: QRectF,
            candle: dict[str, Any],
            candle_index: int,
        ) -> None:
            base_x = float(plot_area.left()) + 10.0
            line_y = float(plot_area.top()) + 18.0

            title_font = painter.font()
            title_font.setPointSize(9)
            title_font.setBold(True)
            painter.setFont(title_font)
            self._draw_colored_segments(
                painter,
                base_x,
                line_y,
                [
                    (f"{self._symbol or '-'} 路 {self._period}", QColor("#f8fafc")),
                    (f" 路 {self._venue_label}", QColor(_CHART_AXIS_TEXT_COLOR)),
                ],
            )

            info_font = painter.font()
            info_font.setPointSize(8)
            info_font.setBold(False)
            painter.setFont(info_font)
            info_base_y = line_y + 18.0
            if self._chart_note_lines:
                for note_index, note_line in enumerate(self._chart_note_lines):
                    self._draw_colored_segments(
                        painter,
                        base_x,
                        info_base_y + (note_index * 16.0),
                        [(note_line, QColor("#93c5fd"))],
                    )
            ohlc_y = info_base_y + (len(self._chart_note_lines) * 16.0)

            open_value = float(candle["open"])
            high_value = float(candle["high"])
            low_value = float(candle["low"])
            close_value = float(candle["close"])
            previous_close = float(self._candles[candle_index - 1]["close"]) if candle_index > 0 else open_value
            delta_value = close_value - previous_close
            delta_ratio = (delta_value / previous_close * 100.0) if previous_close else 0.0
            delta_color = QColor(_CHART_UP_COLOR if delta_value >= 0 else _CHART_DOWN_COLOR)
            self._draw_colored_segments(
                painter,
                base_x,
                ohlc_y,
                [
                    ("O ", QColor(_CHART_AXIS_TEXT_COLOR)),
                    (self._format_hover_value(open_value), QColor("#f8fafc")),
                    ("  H ", QColor(_CHART_AXIS_TEXT_COLOR)),
                    (self._format_hover_value(high_value), QColor("#f8fafc")),
                    ("  L ", QColor(_CHART_AXIS_TEXT_COLOR)),
                    (self._format_hover_value(low_value), QColor("#f8fafc")),
                    ("  C ", QColor(_CHART_AXIS_TEXT_COLOR)),
                    (self._format_hover_value(close_value), delta_color),
                    ("  ", QColor(_CHART_AXIS_TEXT_COLOR)),
                    (f"{delta_value:+.2f} ({delta_ratio:+.2f}%)", delta_color),
                ],
            )

            indicator_segments: list[tuple[str, QColor]] = []
            for item in self._indicator_series:
                values = item.get("values", [])
                if not values:
                    continue
                safe_index = min(candle_index, len(values) - 1)
                value = float(values[safe_index])
                indicator_segments.extend(
                    [
                        (f"{item.get('label', '')} ", QColor(_CHART_AXIS_TEXT_COLOR)),
                        (self._format_hover_value(value), QColor(str(item.get("color", "#f8fafc")))),
                        ("   ", QColor(_CHART_AXIS_TEXT_COLOR)),
                    ]
                )
            if indicator_segments:
                self._draw_colored_segments(
                    painter,
                    base_x,
                    ohlc_y + 18.0,
                    indicator_segments,
                )

            signal_segments: list[tuple[str, QColor]] = []
            for marker in self._signal_markers:
                if int(marker.get("index", -1)) != candle_index:
                    continue
                signal_segments.extend(
                    [
                        ("Signal ", QColor(_CHART_AXIS_TEXT_COLOR)),
                        (str(marker.get("text", marker.get("label", ""))), QColor(str(marker.get("color", "#f8fafc")))),
                        ("   ", QColor(_CHART_AXIS_TEXT_COLOR)),
                    ]
                )
            if signal_segments:
                self._draw_colored_segments(
                    painter,
                    base_x,
                    ohlc_y + (54.0 if not self._trend_indicators else 72.0),
                    signal_segments,
                )

            if self._trend_indicators:
                safe_trend_index = min(candle_index, len(self._trend_indicators) - 1)
                trend_item = self._trend_indicators[safe_trend_index] if safe_trend_index >= 0 else None
                trend_segments: list[tuple[str, QColor]] = []
                if isinstance(trend_item, dict):
                    trend_segments.extend(
                        [
                            (f"{_trend_indicator_title(self._period)} ", QColor(_CHART_AXIS_TEXT_COLOR)),
                            (str(trend_item.get("label", "")), QColor(str(trend_item.get("color", "#f8fafc")))),
                        ]
                    )
                if trend_segments:
                    self._draw_colored_segments(
                        painter,
                        base_x,
                        ohlc_y + 36.0,
                        trend_segments,
                    )

            volume_band_height = max(42.0, float(plot_area.height()) * _VOLUME_OVERLAY_HEIGHT_RATIO)
            volume_text_y = float(plot_area.bottom()) - volume_band_height + 16.0
            self._draw_colored_segments(
                painter,
                base_x,
                volume_text_y,
                [
                    ("Volume ", QColor(_CHART_AXIS_TEXT_COLOR)),
                    (_format_compact_number(float(candle.get("volume", 0.0) or 0.0)), delta_color),
                ],
            )

        def _draw_colored_segments(
            self,
            painter: QPainter,
            x: float,
            baseline_y: float,
            segments: list[tuple[str, QColor]],
        ) -> None:
            metrics = painter.fontMetrics()
            cursor_x = float(x)
            for text, color in segments:
                if not text:
                    continue
                painter.setPen(color)
                painter.drawText(QPointF(cursor_x, baseline_y), text)
                cursor_x += float(metrics.horizontalAdvance(text))

        def _index_for_candle_time(self, candle_time: int) -> int:
            if not self._candles:
                return 0
            for index, candle in enumerate(self._candles):
                if int(candle["time"]) >= int(candle_time):
                    return index
            return len(self._candles) - 1

        def _restore_x_range(self, restore_state: dict[str, float | bool] | None) -> tuple[float, float]:
            default_range = self._default_x_range()
            if not restore_state:
                return default_range
            start_x = float(restore_state.get("start_x", default_range[0]))
            end_x = float(restore_state.get("end_x", default_range[1]))
            span = max(end_x - start_x, float(self._display_step_ms))
            if bool(restore_state.get("stick_to_right", False)):
                return self._bounded_x_range(self._full_x_max - span, self._full_x_max)
            return self._bounded_x_range(start_x, end_x)

        def _bounded_x_range(self, start_x: float, end_x: float) -> tuple[float, float]:
            if len(self._candles) <= 1:
                return self._full_x_min, self._full_x_max
            span = max(end_x - start_x, float(self._display_step_ms))
            if start_x < self._full_x_min:
                start_x = self._full_x_min
                end_x = start_x + span
            if end_x > self._full_x_max:
                end_x = self._full_x_max
                start_x = end_x - span
            start_x = _clamp(start_x, self._full_x_min, self._full_x_max)
            end_x = _clamp(end_x, self._full_x_min, self._full_x_max)
            if end_x <= start_x:
                end_x = min(self._full_x_max, start_x + float(self._display_step_ms))
            return start_x, end_x

        def _resolve_hover_context(self) -> tuple[dict[str, Any], int, float, float] | None:
            if not self._candles or self._axis_x is None or self._axis_y is None:
                return None
            plot_area = self.chart().plotArea()
            if plot_area.width() <= 0 or plot_area.height() <= 0:
                return None
            hover_pos = self._hover_pos
            if hover_pos is None or not plot_area.contains(hover_pos):
                return None
            hover_value = self._value_for_y(float(hover_pos.y()), plot_area)
            chart_point = self.chart().mapToValue(hover_pos)
            hover_index = self.nearest_candle_index_for_x_value(float(chart_point.x()))
            return self._candles[hover_index], hover_index, hover_value, float(hover_pos.y())

        def _resolve_external_hover_context(self) -> tuple[dict[str, Any], int, int, float] | None:
            if self._external_hover_time is None or self._axis_x is None:
                return None
            plot_area = self.chart().plotArea()
            if plot_area.width() <= 0 or not self._candles:
                return None
            hover_index = self._index_for_candle_time(self._external_hover_time)
            candle = self._candles[hover_index]
            snapped_x = self._x_for_index(hover_index, plot_area)
            return candle, hover_index, int(self._external_hover_time), snapped_x

        def _emit_hover_time_from_position(self, hover_pos: QPointF) -> None:
            if not self._candles:
                self.hoverTimeChanged.emit(None)
                return
            plot_area = self.chart().plotArea()
            if not plot_area.contains(hover_pos):
                self.hoverTimeChanged.emit(None)
                return
            chart_point = self.chart().mapToValue(hover_pos)
            candle_time = _resolve_candle_time_from_x_value(
                self._candles,
                self._display_times_ms,
                x_value=float(chart_point.x()),
                display_step_ms=self._display_step_ms,
            )
            self.hoverTimeChanged.emit(int(candle_time))

        def nearest_candle_index_for_x_value(self, x_value: float) -> int:
            if not self._candles:
                return 0
            if len(self._display_times_ms) <= 1:
                return 0
            raw_index = round((x_value - self._full_x_min) / float(self._display_step_ms))
            return int(_clamp(float(raw_index), 0.0, float(len(self._candles) - 1)))

        def _fit_y_axis_to_visible_range(self) -> None:
            axis_y = self._axis_y
            if axis_y is None or not self._candles:
                return
            start_x, end_x = self.current_x_range()
            left_index = self._index_for_display_x(start_x, mode="floor")
            right_index = self._index_for_display_x(end_x, mode="ceil")
            visible = self._candles[left_index : right_index + 1]
            if not visible:
                visible = list(self._candles)
            lows = [float(item["low"]) for item in visible]
            highs = [float(item["high"]) for item in visible]
            for series in self._overlay_values:
                if not series:
                    continue
                visible_values = series[left_index : right_index + 1] or series
                lows.extend(float(value) for value in visible_values)
                highs.extend(float(value) for value in visible_values)
            min_price = min(lows) if lows else self._full_y_min
            max_price = max(highs) if highs else self._full_y_max
            if min_price == max_price:
                padding = max(abs(min_price) * 0.02, 1.0)
            else:
                padding = max((max_price - min_price) * 0.08, abs(max_price) * 0.002, 1.0)
            axis_y.setRange(min_price - padding, max_price + padding)

        def _visible_index_range(self) -> tuple[int, int]:
            if not self._candles:
                return 0, 0
            start_x, end_x = self.current_x_range()
            left_index = self._index_for_display_x(start_x, mode="floor")
            right_index = self._index_for_display_x(end_x, mode="ceil")
            return left_index, right_index

        def _draw_volume_overlay(self, painter: QPainter, plot_area: QRectF) -> None:
            if not self._candles:
                return
            left_index, right_index = self._visible_index_range()
            if right_index < left_index:
                return
            visible = self._candles[left_index : right_index + 1]
            if not visible:
                return
            volume_band_height = max(42.0, float(plot_area.height()) * _VOLUME_OVERLAY_HEIGHT_RATIO)
            volume_band_top = float(plot_area.bottom()) - volume_band_height
            trend_band_top = float(plot_area.bottom())
            volume_draw_area_bottom = volume_band_top
            show_daily_trend = _supports_trend_indicator(self._period) and len(self._trend_indicators) > 0
            if show_daily_trend and volume_band_height >= 28.0:
                trend_band_height = max(_DAILY_TREND_BAND_MIN_HEIGHT, min(_DAILY_TREND_BAND_MAX_HEIGHT, volume_band_height * _DAILY_TREND_BAND_SPLIT_RATIO))
                trend_band_top = float(plot_area.bottom()) - trend_band_height - _DAILY_TREND_BAND_GAP_PX
                volume_draw_area_bottom = trend_band_top - _DAILY_TREND_BAND_GAP_PX
            if show_daily_trend and volume_draw_area_bottom <= volume_band_top:
                trend_band_top = float(plot_area.bottom())
                volume_draw_area_bottom = volume_band_top
                show_daily_trend = False
            if trend_band_top <= volume_band_top:
                trend_band_top = volume_band_top
                show_daily_trend = False
            if not show_daily_trend:
                trend_band_top = float(plot_area.bottom())
                volume_draw_area_bottom = volume_band_top
            volume_draw_height = max(volume_draw_area_bottom - volume_band_top, 0.0)

            max_volume = max(float(item.get("volume", 0.0) or 0.0) for item in visible)
            separator_pen = QPen(QColor(_CHART_GRID_COLOR), 1)
            painter.setPen(separator_pen)
            painter.drawLine(QPointF(plot_area.left(), volume_band_top), QPointF(plot_area.right(), volume_band_top))
            if show_daily_trend:
                painter.drawLine(QPointF(plot_area.left(), trend_band_top), QPointF(plot_area.right(), trend_band_top))
                painter.drawLine(QPointF(plot_area.left(), volume_draw_area_bottom), QPointF(plot_area.right(), volume_draw_area_bottom))
                painter.save()
                painter.setBrush(Qt.BrushStyle.NoBrush)
                painter.setPen(QPen(QColor(_CHART_AXIS_TEXT_COLOR), 1))
                painter.drawRect(
                    QRectF(
                        float(plot_area.left()),
                        float(trend_band_top),
                        float(plot_area.width()),
                        float(plot_area.bottom() - trend_band_top),
                    )
                )
                painter.restore()
            step_px = 10.0
            if len(self._candles) >= 2:
                anchor_index = min(left_index + 1, len(self._candles) - 1)
                step_px = max(2.0, abs(self._x_for_index(anchor_index, plot_area) - self._x_for_index(left_index, plot_area)))
            bar_width = max(1.0, min(10.0, step_px * 0.42))
            painter.setPen(Qt.PenStyle.NoPen)
            for index in range(left_index, right_index + 1):
                candle = self._candles[index]
                volume = float(candle.get("volume", 0.0) or 0.0)
                x_center = self._x_for_index(index, plot_area)
                x_left = x_center - (bar_width / 2.0)
                if volume > 0 and volume_draw_height > 0 and max_volume > 0:
                    bar_height = (volume / max_volume) * (volume_draw_height - 2.0)
                    if bar_height > 0:
                        y_top = volume_draw_area_bottom - bar_height
                        color = QColor(_CHART_UP_COLOR if float(candle["close"]) >= float(candle["open"]) else _CHART_DOWN_COLOR)
                        color.setAlpha(170)
                        painter.setBrush(color)
                        painter.drawRect(QRectF(x_left, y_top, bar_width, bar_height))
                if show_daily_trend:
                    trend_item = self._trend_indicators[index] if index < len(self._trend_indicators) else None
                    trend_color_text = str(trend_item.get("color", "#94a3b8")) if isinstance(trend_item, dict) else "#94a3b8"
                    trend_color = QColor(trend_color_text)
                    trend_color.setAlpha(_DAILY_TREND_BAND_ALPHA)
                    painter.setBrush(trend_color)
                    painter.drawRect(QRectF(x_left, trend_band_top, bar_width, float(plot_area.bottom()) - trend_band_top))

        def _draw_signal_markers(self, painter: QPainter, plot_area: QRectF) -> None:
            if not self._signal_markers or not self._candles:
                return
            left_index, right_index = self._visible_index_range()
            font = painter.font()
            font.setPointSize(8)
            font.setBold(True)
            painter.setFont(font)
            metrics = painter.fontMetrics()
            visible_groups: dict[tuple[int, str], list[dict[str, Any]]] = {}
            for marker in self._signal_markers:
                index = int(marker.get("index", -1))
                if index < left_index or index > right_index or index >= len(self._candles):
                    continue
                direction = str(marker.get("direction", "") or "").strip().lower()
                visible_groups.setdefault((index, direction), []).append(marker)

            marker_radius = 4.5
            marker_base_gap = 14.0
            marker_stack_gap = 16.0
            label_gap = 7.0
            text_height = float(metrics.height())
            top_limit = float(plot_area.top()) + text_height + 6.0
            bottom_limit = float(plot_area.bottom()) - 6.0

            for (index, direction), markers in visible_groups.items():
                candle = self._candles[index]
                x_center = self._x_for_index(index, plot_area)
                stack_down = direction == "long"
                primary_anchor = float(candle["low"] if stack_down else candle["high"])
                alternate_anchor = float(candle["high"] if stack_down else candle["low"])
                primary_y_anchor = self._y_for_value(primary_anchor, plot_area)
                alternate_y_anchor = self._y_for_value(alternate_anchor, plot_area)
                stack_count = len(markers)
                stack_depth = marker_base_gap + ((stack_count - 1) * marker_stack_gap)
                can_fit_primary = (
                    (stack_down and (primary_y_anchor + stack_depth + text_height) <= bottom_limit)
                    or ((not stack_down) and (primary_y_anchor - stack_depth - text_height) >= top_limit)
                )
                if not can_fit_primary:
                    stack_down = not stack_down
                    y_anchor = alternate_y_anchor
                else:
                    y_anchor = primary_y_anchor

                for stack_index, marker in enumerate(markers):
                    offset = marker_base_gap + (stack_index * marker_stack_gap)
                    y_pos = y_anchor + offset if stack_down else y_anchor - offset
                    y_pos = _clamp(y_pos, top_limit, bottom_limit)
                    color = QColor(str(marker.get("color", _REPLAY_SIGNAL_LONG_COLOR)))
                    painter.setPen(QPen(color, 2))
                    painter.setBrush(color)
                    painter.drawEllipse(QPointF(x_center, y_pos), marker_radius, marker_radius)
                    label = str(marker.get("label", "") or "")
                    if not label:
                        continue
                    text_width = float(metrics.horizontalAdvance(label))
                    text_x = _clamp(
                        x_center + label_gap,
                        float(plot_area.left()) + 2.0,
                        float(plot_area.right()) - text_width - 2.0,
                    )
                    text_y = y_pos + 4.0 if stack_down else y_pos - 6.0
                    painter.setPen(color)
                    painter.drawText(QPointF(text_x, text_y), label)

        def _draw_box_overlays(self, painter: QPainter, plot_area: QRectF) -> None:
            if not self._box_overlays or not self._candles:
                return
            left_index, right_index = self._visible_index_range()
            for overlay in self._box_overlays:
                start_index = int(overlay.get("start_index", -1))
                end_index = int(overlay.get("end_index", -1))
                if end_index < left_index or start_index > right_index:
                    continue
                visible_start = max(left_index, start_index)
                visible_end = min(right_index, end_index)
                if visible_end < visible_start:
                    continue
                x1 = self._x_for_index(visible_start, plot_area)
                x2 = self._x_for_index(visible_end, plot_area)
                y1 = self._y_for_value(float(overlay.get("upper", 0.0) or 0.0), plot_area)
                y2 = self._y_for_value(float(overlay.get("lower", 0.0) or 0.0), plot_area)
                rect = QRectF(
                    min(x1, x2),
                    min(y1, y2),
                    max(abs(x2 - x1), 1.0),
                    max(abs(y2 - y1), 1.0),
                )
                fill = QColor(str(overlay.get("fill", _BOX_HISTORY_FILL_COLOR)))
                fill.setAlpha(18 if bool(overlay.get("active", False)) else 8)
                outline = QColor(str(overlay.get("outline", _BOX_HISTORY_OUTLINE_COLOR)))
                if overlay.get("mode") == "realtime":
                    fill.setAlpha(26 if bool(overlay.get("active", False)) else 12)
                    outline.setAlpha(230 if bool(overlay.get("active", False)) else 200)
                else:
                    fill.setAlpha(18 if bool(overlay.get("active", False)) else 8)
                    outline.setAlpha(245 if bool(overlay.get("active", False)) else 210)
                pen = QPen(outline, 2)
                pen.setCosmetic(True)
                if str(overlay.get("mode", "history")).strip().lower() == "realtime":
                    pen.setStyle(Qt.PenStyle.DashLine)
                painter.setPen(pen)
                painter.setBrush(fill)
                painter.drawRect(rect)

        def _box_tooltip_lines(self, candle_index: int) -> tuple[str, ...]:
            lines: list[str] = []
            for overlay in self._box_overlays:
                start_index = int(overlay.get("start_index", -1))
                end_index = int(overlay.get("end_index", -1))
                if not start_index <= candle_index <= end_index:
                    continue
                upper = float(overlay.get("upper", 0.0) or 0.0)
                lower = float(overlay.get("lower", 0.0) or 0.0)
                touches = int(overlay.get("touches", 0) or 0)
                trend = str(overlay.get("trend", "") or "")
                mode = str(overlay.get("mode", "history") or "").strip().lower()
                trend_text = f" | {_BOX_TREND_LABELS.get(trend, trend)}" if trend else ""
                lines.append(
                    f"{'实盘' if mode == 'realtime' else '历史'}箱体 {lower:.2f}-{upper:.2f} | 触点 {touches}{trend_text}"
                )
            return tuple(lines)

        def _signal_tooltip_lines(self, candle_index: int) -> tuple[str, ...]:
            lines: list[str] = []
            for marker in self._signal_markers:
                if int(marker.get("index", -1)) != candle_index:
                    continue
                label = str(marker.get("text", marker.get("label", "")) or "")
                distance = float(marker.get("distance_pct", 0.0) or 0.0)
                score = int(marker.get("score", 0) or 0)
                lines.append(f"信号 {label} | 均线距离 {distance:.2f}% | 评分 {score}")
            return tuple(lines)

        def _draw_selected_line_handles(self, painter: QPainter, plot_area: QRectF) -> None:
            if not (0 <= self._selected_workspace_line_index < len(self._workspace_lines)):
                return
            line = self._workspace_lines[self._selected_workspace_line_index]
            kind = str(line.get("kind", "horizontal") or "horizontal").strip().lower()
            handle_pen = QPen(QColor("#38bdf8"), 2)
            handle_pen.setCosmetic(True)
            painter.setPen(handle_pen)
            hovered_mode = self._hovered_workspace_drag_mode if self._hovered_workspace_line_index == self._selected_workspace_line_index else None
            if kind == "horizontal":
                body_visual = _line_handle_visual("move", hovered_drag_mode=hovered_mode)
                y_pos = self._y_for_value(float(line.get("price_a", 0.0) or 0.0), plot_area)
                x_pos = float(plot_area.right()) - 18.0
                painter.setBrush(QColor(str(body_visual["fill"])))
                painter.drawEllipse(QPointF(x_pos, y_pos), float(body_visual["radius"]), float(body_visual["radius"]))
                painter.setBrush(QColor(str(body_visual["inner_fill"])))
                painter.drawEllipse(QPointF(x_pos, y_pos), 2.2, 2.2)
                return
            endpoint_points = (
                ("endpoint_a", int(line.get("time_a", 0) or 0), float(line.get("price_a", 0.0) or 0.0)),
                ("endpoint_b", int(line.get("time_b", 0) or 0), float(line.get("price_b", 0.0) or 0.0)),
            )
            for endpoint_key, candle_time, price in endpoint_points:
                visual = _line_handle_visual(endpoint_key, hovered_drag_mode=hovered_mode)
                display_x = _display_x_for_candle_time(
                    self._candles,
                    self._display_times_ms,
                    candle_time=candle_time,
                    display_step_ms=self._display_step_ms,
                )
                x_pos = self._x_for_display_value(display_x, plot_area)
                y_pos = self._y_for_value(price, plot_area)
                painter.setBrush(QColor(str(visual["fill"])))
                painter.drawEllipse(QPointF(x_pos, y_pos), float(visual["radius"]), float(visual["radius"]))
                painter.setBrush(QColor(str(visual["inner_fill"])))
                painter.drawEllipse(QPointF(x_pos, y_pos), 2.2, 2.2)

        def _draw_preview_line(self, painter: QPainter, plot_area: QRectF) -> None:
            preview_line = self._preview_line
            if preview_line is None or not self._candles:
                return
            preview_pen = QPen(QColor("#93c5fd"), 2)
            preview_pen.setStyle(Qt.PenStyle.DashLine)
            preview_pen.setCosmetic(True)
            painter.setPen(preview_pen)
            kind = str(preview_line.get("kind", "horizontal") or "horizontal").strip().lower()
            if kind == "horizontal":
                y_pos = self._y_for_value(float(preview_line.get("price_a", 0.0) or 0.0), plot_area)
                painter.drawLine(QPointF(plot_area.left(), y_pos), QPointF(plot_area.right(), y_pos))
                return
            endpoint_points = (
                (int(preview_line.get("time_a", 0) or 0), float(preview_line.get("price_a", 0.0) or 0.0)),
                (int(preview_line.get("time_b", 0) or 0), float(preview_line.get("price_b", 0.0) or 0.0)),
            )
            mapped_points: list[QPointF] = []
            for candle_time, price in endpoint_points:
                display_x = _display_x_for_candle_time(
                    self._candles,
                    self._display_times_ms,
                    candle_time=candle_time,
                    display_step_ms=self._display_step_ms,
                )
                mapped_points.append(
                    QPointF(
                        self._x_for_display_value(display_x, plot_area),
                        self._y_for_value(price, plot_area),
                    )
                )
            if len(mapped_points) == 2:
                painter.drawLine(mapped_points[0], mapped_points[1])

        def _x_for_index(self, index: int, plot_area: QRectF) -> float:
            start_x, end_x = self.current_x_range()
            span = max(end_x - start_x, 1.0)
            display_x = float(self._display_times_ms[index]) if index < len(self._display_times_ms) else float(index)
            ratio = (display_x - start_x) / span
            return float(plot_area.left()) + (_clamp(ratio, 0.0, 1.0) * float(plot_area.width()))

        def _x_for_display_value(self, display_x: float, plot_area: QRectF) -> float:
            start_x, end_x = self.current_x_range()
            span = max(end_x - start_x, 1.0)
            ratio = (float(display_x) - start_x) / span
            return float(plot_area.left()) + (_clamp(ratio, 0.0, 1.0) * float(plot_area.width()))

        def _y_for_value(self, value: float, plot_area: QRectF) -> float:
            if self._axis_y is None:
                return float(plot_area.center().y())
            min_y = float(self._axis_y.min())
            max_y = float(self._axis_y.max())
            span = max(max_y - min_y, 1e-9)
            ratio = (max_y - value) / span
            return float(plot_area.top()) + (_clamp(ratio, 0.0, 1.0) * float(plot_area.height()))

        def _value_for_y(self, y: float, plot_area: QRectF) -> float:
            if self._axis_y is None:
                return 0.0
            min_y = float(self._axis_y.min())
            max_y = float(self._axis_y.max())
            ratio = _clamp((y - float(plot_area.top())) / max(float(plot_area.height()), 1.0), 0.0, 1.0)
            return max_y - (ratio * (max_y - min_y))

        def _pan_by_pixels(self, delta_px: float, plot_width: float) -> None:
            axis_x = self._axis_x
            if axis_x is None or not self._candles:
                return
            current_x_min, current_x_max = self.current_x_range()
            current_x_span = max(current_x_max - current_x_min, 1.0)
            shift_x = (float(delta_px) / max(float(plot_width), 1.0)) * current_x_span
            new_x_min = current_x_min - shift_x
            new_x_max = current_x_max - shift_x
            new_x_min, new_x_max = self._bounded_x_range(new_x_min, new_x_max)
            self._apply_x_range(new_x_min, new_x_max, emit_signal=True)
            self._fit_y_axis_to_visible_range()

        def _apply_x_range(self, start_x: float, end_x: float, *, emit_signal: bool) -> None:
            if self._axis_x is None:
                return
            self._axis_x.setRange(
                QDateTime.fromMSecsSinceEpoch(int(start_x)),
                QDateTime.fromMSecsSinceEpoch(int(end_x)),
            )
            if emit_signal and not self._suppress_x_range_signal:
                self.xRangeChanged.emit(float(start_x), float(end_x))

        def _default_x_range(self) -> tuple[float, float]:
            if not self._display_times_ms:
                return self._full_x_min, self._full_x_max
            return _default_native_x_range_with_right_padding(
                self._display_times_ms,
                display_step_ms=self._display_step_ms,
            )

        def _index_for_display_x(self, x_value: float, *, mode: str) -> int:
            if not self._candles or len(self._display_times_ms) <= 1:
                return 0
            raw_index = (x_value - self._full_x_min) / float(self._display_step_ms)
            if mode == "floor":
                index = math.floor(raw_index)
            elif mode == "ceil":
                index = math.ceil(raw_index)
            else:
                index = round(raw_index)
            return int(_clamp(float(index), 0.0, float(len(self._candles) - 1)))

        def _create_hover_label(self, *, multiline: bool, center: bool) -> QLabel:
            label = QLabel(self)
            label.setAttribute(Qt.WidgetAttribute.WA_TransparentForMouseEvents, True)
            label.setVisible(False)
            font = label.font()
            font.setFamily("Consolas")
            font.setPointSize(8)
            font.setBold(True)
            label.setFont(font)
            label.setWordWrap(multiline)
            label.setAlignment(Qt.AlignmentFlag.AlignCenter if center else Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
            label.setStyleSheet(
                "QLabel {"
                "background-color: rgba(15, 23, 42, 238);"
                "color: #f8fafc;"
                "border: 1px solid #334155;"
                "border-radius: 5px;"
                "padding: 4px 8px;"
                "}"
            )
            return label

        def _hide_hover_overlays(self) -> None:
            self._price_badge.hide()
            self._time_badge.hide()
            self._tooltip_badge.hide()

        def _update_external_time_overlay(self, *, bounds: QRectF, snapped_x: float, time_text: str) -> None:
            self._price_badge.hide()
            self._tooltip_badge.hide()
            self._time_badge.setText(time_text)
            self._time_badge.adjustSize()
            time_size = self._time_badge.sizeHint()
            viewport = self.viewport().rect()
            time_x = _clamp(
                snapped_x - (float(time_size.width()) / 2.0),
                float(viewport.left()) + 4.0,
                float(viewport.right()) - float(time_size.width()) - 4.0,
            )
            time_y = min(
                float(viewport.bottom()) - float(time_size.height()) - 4.0,
                float(bounds.bottom()) + 8.0,
            )
            self._time_badge.move(int(round(time_x)), int(round(time_y)))
            self._time_badge.raise_()
            self._time_badge.show()

        def _update_hover_overlays(
            self,
            *,
            bounds: QRectF,
            anchor: QPointF,
            candle_color: QColor,
            price_text: str,
            time_text: str,
            tooltip_lines: tuple[str, ...],
        ) -> None:
            viewport_geom = self.viewport().geometry()
            viewport = QRectF(self.rect())
            mapped_bounds = QRectF(
                float(viewport_geom.left()) + float(bounds.left()),
                float(viewport_geom.top()) + float(bounds.top()),
                float(bounds.width()),
                float(bounds.height()),
            )
            mapped_anchor = QPointF(
                float(viewport_geom.left()) + float(anchor.x()),
                float(viewport_geom.top()) + float(anchor.y()),
            )
            self._price_badge.setText(price_text)
            self._price_badge.adjustSize()
            price_size = self._price_badge.sizeHint()
            price_x = min(
                float(viewport.right()) - float(price_size.width()) - 4.0,
                float(mapped_bounds.right()) + 8.0,
            )
            price_y = max(
                float(viewport.top()) + 4.0,
                min(
                    float(mapped_anchor.y()) - (float(price_size.height()) / 2.0),
                    float(viewport.bottom()) - float(price_size.height()) - 4.0,
                ),
            )
            self._price_badge.move(int(round(price_x)), int(round(price_y)))
            self._price_badge.raise_()
            self._price_badge.show()

            self._time_badge.setText(time_text)
            self._time_badge.adjustSize()
            time_size = self._time_badge.sizeHint()
            time_x = max(
                float(viewport.left()) + 4.0,
                min(
                    float(mapped_anchor.x()) - (float(time_size.width()) / 2.0),
                    float(viewport.right()) - float(time_size.width()) - 4.0,
                ),
            )
            time_y = min(
                float(viewport.bottom()) - float(time_size.height()) - 4.0,
                float(mapped_bounds.bottom()) + 8.0,
            )
            self._time_badge.move(int(round(time_x)), int(round(time_y)))
            self._time_badge.raise_()
            self._time_badge.show()

            self._tooltip_badge.setStyleSheet(
                "QLabel {"
                "background-color: rgba(11, 18, 32, 236);"
                f"border: 1px solid {candle_color.name()};"
                "color: #f8fafc;"
                "border-radius: 7px;"
                "padding: 6px 10px;"
                "}"
            )
            self._tooltip_badge.setText("\n".join(tooltip_lines))
            self._tooltip_badge.adjustSize()
            tooltip_size = self._tooltip_badge.sizeHint()
            place_right = float(mapped_anchor.x()) <= float(mapped_bounds.center().x())
            place_above = float(mapped_anchor.y()) > float(mapped_bounds.center().y())
            tooltip_x = (
                float(mapped_anchor.x()) + 18.0
                if place_right
                else float(mapped_anchor.x()) - float(tooltip_size.width()) - 18.0
            )
            tooltip_y = (
                float(mapped_anchor.y()) - float(tooltip_size.height()) - 18.0
                if place_above
                else float(mapped_anchor.y()) + 18.0
            )
            tooltip_x = max(
                float(mapped_bounds.left()) + 8.0,
                min(tooltip_x, float(mapped_bounds.right()) - float(tooltip_size.width()) - 8.0),
            )
            tooltip_y = max(
                float(mapped_bounds.top()) + 8.0,
                min(tooltip_y, float(mapped_bounds.bottom()) - float(tooltip_size.height()) - 8.0),
            )
            self._tooltip_badge.move(int(round(tooltip_x)), int(round(tooltip_y)))
            self._tooltip_badge.raise_()
            self._tooltip_badge.show()

        @staticmethod
        def _format_hover_value(value: float) -> str:
            if abs(value) >= 1_000:
                return f"{value:,.2f}"
            if abs(value) >= 1.0:
                return f"{value:.2f}"
            return f"{value:.6f}".rstrip("0").rstrip(".")
else:
    InteractiveKlineChartView = None  # type: ignore[assignment]


def _build_source_report(
    *,
    requested_limit: int,
    local_count: int,
    remote_added_count: int,
    returned_count: int,
    start_ms: int | None,
    end_ms: int | None,
    local_only: bool,
    has_network_fallback: bool,
    local_stale: bool,
) -> dict[str, Any]:
    source = "local_cache"
    if local_only:
        if returned_count < requested_limit and requested_limit > 0:
            source = "local_cache_partial"
    else:
        if returned_count >= requested_limit and requested_limit > 0:
            if local_count >= requested_limit:
                source = "local_cache_synced" if (local_stale and has_network_fallback) else "local_cache"
            elif remote_added_count > 0:
                source = "local_plus_remote"
            else:
                source = "remote_plus_local"
        elif local_count > 0 and has_network_fallback:
            source = "local_cache_replaced"
        elif local_count > 0:
            source = "local_cache"
        else:
            source = "remote"

    return {
        "requested": requested_limit,
        "returned": returned_count,
        "local_count": local_count,
        "remote_added_count": remote_added_count,
        "truncated": returned_count < requested_limit if requested_limit > 0 else False,
        "source": source,
        "start_ms": start_ms,
        "end_ms": end_ms,
        "local_stale": local_stale,
        "network_refreshed": has_network_fallback,
    }

class KlineDataLoader(QThread):
    loaded = Signal(int, KlineChartPayload)
    failed = Signal(int, str)

    def __init__(
        self,
        *,
        request_id: int,
        symbol: str,
        period: str,
        limit: int = 1200,
        local_only: bool = False,
        workspace_entry: dict[str, object] | None = None,
        enable_alerts: bool = True,
    ) -> None:
        super().__init__()
        self._request_id = request_id
        self._symbol = symbol.strip().upper()
        self._period = period.strip()
        self._limit = max(50, limit)
        self._local_only = local_only
        self._workspace_entry = normalize_workspace_entry(workspace_entry)
        self._enable_alerts = enable_alerts

    def _build_payload(
        self,
        *,
        candles: list[Any],
        local_count: int,
        remote_added_count: int,
        has_network_fallback: bool,
        local_stale: bool,
        include_alerts: bool,
    ) -> KlineChartPayload:
        if self._limit > 0 and len(candles) > self._limit:
            candles = candles[-self._limit:]
        if not candles:
            raise ValueError(f"没有K线数据：{self._symbol} {self._period}")

        start_ms = _to_ms_seconds(candles[0].ts)
        end_ms = _to_ms_seconds(candles[-1].ts)
        stats = _build_source_report(
            requested_limit=self._limit,
            local_count=local_count,
            remote_added_count=remote_added_count if not self._local_only else 0,
            returned_count=len(candles),
            start_ms=start_ms,
            end_ms=end_ms,
            local_only=self._local_only,
            has_network_fallback=has_network_fallback,
            local_stale=local_stale,
        )
        stats["cache_synced"] = has_network_fallback

        chart_candles = [
            {
                "time": _to_ms_seconds(item.ts),
                "open": _to_float(item.open),
                "high": _to_float(item.high),
                "low": _to_float(item.low),
                "close": _to_float(item.close),
                "volume": _to_float(item.volume),
            }
            for item in candles
        ]

        closes = [_to_float(item.close) for item in candles]
        time_points = [_to_ms_seconds(item.ts) for item in candles]
        ema9_values = _to_ema(closes, 15)
        sma50_values = _to_sma(closes, 50)
        ema55_values = _to_ema(closes, 55)
        ema9 = [
            {"time": time_points[i], "value": value}
            for i, value in enumerate(ema9_values)
        ]
        ema21 = [
            {"time": time_points[i], "value": value}
            for i, value in enumerate(sma50_values)
        ]
        ema55 = [
            {"time": time_points[i], "value": value}
            for i, value in enumerate(ema55_values)
        ]
        daily_trend_indicator = _build_daily_trend_indicator(
            period=self._period,
            times=time_points,
            closes=closes,
            sma50=sma50_values,
        )
        signal_markers = _build_replay_signal_markers(
            candles=list(candles),
            period=self._period,
            ema15_values=ema9_values,
            sma50_values=sma50_values,
        )
        box_overlays = list(_build_box_history_overlays(list(candles)))
        box_overlays.extend(_build_box_realtime_overlay(list(candles)))

        alert_snapshot = None
        if include_alerts:
            updated_entry, new_events, structure = evaluate_workspace_alerts(
                workspace_entry=self._workspace_entry,
                candles=chart_candles,
                ema_fast=ema9,
                ma_slow=ema21,
                raw_candles=list(candles),
            )
            alert_snapshot = KlineAlertSnapshot(
                workspace_entry=updated_entry,
                new_events=list(new_events),
                structure=dict(structure),
            )

        return KlineChartPayload(
            candles=chart_candles,
            ema_9=ema9,
            ema_21=ema21,
            ema_55=ema55,
            trend_indicator=daily_trend_indicator,
            signal_markers=signal_markers,
            box_overlays=box_overlays,
            raw_candles=list(candles),
            stats=stats,
            alert_snapshot=alert_snapshot,
        )

    def run(self) -> None:
        try:
            client = OkxRestClient()
            local_candles = load_candle_cache(
                self._symbol,
                self._period,
                limit=None if self._limit <= 0 else self._limit,
            )
            local_count = len(local_candles)
            is_local_stale = _is_local_cache_stale(local_candles, self._period)
            local_preview_emitted = False
            if local_candles:
                self.loaded.emit(
                    self._request_id,
                    self._build_payload(
                        candles=local_candles,
                        local_count=local_count,
                        remote_added_count=0,
                        has_network_fallback=False,
                        local_stale=is_local_stale,
                        include_alerts=False,
                    ),
                )
                local_preview_emitted = True

            if self.isInterruptionRequested():
                return

            if self._local_only:
                candles = local_candles
                remote_added_count = 0
                has_network_fallback = False
            else:
                try:
                    remote_candles = client.get_candles_history(self._symbol, self._period, limit=self._limit)
                    local_ts = {item.ts for item in local_candles}
                    remote_ts = {item.ts for item in remote_candles}
                    remote_added_count = len(remote_ts - local_ts)
                    has_network_fallback = True
                    candles = remote_candles or local_candles
                    is_local_stale = _is_local_cache_stale(remote_candles, self._period) if remote_candles else is_local_stale
                except Exception:
                    if local_preview_emitted:
                        candles = local_candles
                        remote_added_count = 0
                        has_network_fallback = False
                    else:
                        raise

            if not candles:
                if self._local_only:
                    raise ValueError(f"本地缓存没有数据：{self._symbol} {self._period}")
                raise ValueError(f"没有K线数据：{self._symbol} {self._period}")

            if self._limit > 0 and len(candles) > self._limit:
                candles = candles[-self._limit:]

            if not candles:
                raise ValueError(f"没有K线数据：{self._symbol} {self._period}")

            self.loaded.emit(
                self._request_id,
                self._build_payload(
                    candles=candles,
                    local_count=local_count,
                    remote_added_count=remote_added_count,
                    has_network_fallback=has_network_fallback,
                    local_stale=is_local_stale,
                    include_alerts=self._enable_alerts,
                ),
            )
        except Exception as exc:  # noqa: BLE001
            self.failed.emit(self._request_id, str(exc))


class SecondaryVolatilityDataLoader(QThread):
    loaded = Signal(int, object)
    failed = Signal(int, str)

    def __init__(
        self,
        *,
        request_id: int,
        period: str,
        limit: int,
        average_kline: bool,
    ) -> None:
        super().__init__()
        self._request_id = request_id
        self._period = period.strip()
        self._limit = limit
        self._average_kline = average_kline

    def run(self) -> None:
        try:
            payload = self._build_payload()
            self.loaded.emit(self._request_id, payload)
        except Exception as exc:  # noqa: BLE001
            self.failed.emit(self._request_id, str(exc))

    def _target_resolution(self) -> str:
        requested_period = self._period.strip().upper()
        return {
            "1H": DERIBIT_BASE_HOURLY_RESOLUTION,
            "4H": "14400",
            "1D": "1D",
        }.get(requested_period, DERIBIT_BASE_HOURLY_RESOLUTION)

    def _build_resolution_candles(self, hourly_candles: list[Any], *, resolution: str) -> list[Any]:
        if resolution == "14400":
            candles = _aggregate_candles_to_resolution(hourly_candles, 14_400_000)
        elif resolution == "1D":
            candles_4h = _aggregate_candles_to_resolution(hourly_candles, 14_400_000)
            candles = _aggregate_candles_to_resolution(candles_4h, 86_400_000, anchor_offset_ms=16 * 3_600_000)
        else:
            candles = list(hourly_candles)
        limit = max(50, self._limit)
        if limit > 0:
            candles = candles[-limit:]
        if self._average_kline:
            candles = _to_average_volatility_candles(candles)
        return candles

    def _make_payload(
        self,
        *,
        candles: list[Any],
        source: str,
        local_count: int,
        remote_added_count: int,
        cache_synced: bool,
    ) -> KlineChartPayload:
        if not candles:
            raise ValueError("BTC DVOL 没有可用数据")

        chart_candles = [
            {
                "time": _to_ms_seconds(item.ts),
                "open": _to_float(item.open),
                "high": _to_float(item.high),
                "low": _to_float(item.low),
                "close": _to_float(item.close),
                "volume": 0.0,
            }
            for item in candles
        ]
        closes = [_to_float(item.close) for item in candles]
        time_points = [_to_ms_seconds(item.ts) for item in candles]
        ema9_values = _to_ema(closes, 15)
        sma50_values = _to_sma(closes, 50)
        ema55_values = _to_ema(closes, 55)

        return KlineChartPayload(
            candles=chart_candles,
            ema_9=[{"time": time_points[i], "value": value} for i, value in enumerate(ema9_values)],
            ema_21=[{"time": time_points[i], "value": value} for i, value in enumerate(sma50_values)],
            ema_55=[{"time": time_points[i], "value": value} for i, value in enumerate(ema55_values)],
            trend_indicator=_build_daily_trend_indicator(
                period=self._period,
                times=time_points,
                closes=closes,
                sma50=sma50_values,
            ),
            signal_markers=[],
            box_overlays=[],
            raw_candles=list(candles),
            stats={
                "returned": len(candles),
                "source": source,
                "local_count": local_count,
                "remote_added_count": remote_added_count,
                "local_stale": False,
                "cache_synced": cache_synced,
                "start_ms": time_points[0] if time_points else None,
                "end_ms": time_points[-1] if time_points else None,
            },
            alert_snapshot=None,
        )

    def _build_payload(self) -> KlineChartPayload:
        resolution = self._target_resolution()
        cached_hourly = _load_cached_hourly_series("BTC")
        cached_volatility: list[Any] = []
        cached_spot: list[Any] = []
        spot_inst_id = OKX_SPOT_SYMBOLS["BTC"]
        local_count = 0
        local_preview_emitted = False

        if cached_hourly is not None:
            cached_spot_inst_id, cached_volatility, cached_spot, _cached_fetched_at = cached_hourly
            spot_inst_id = cached_spot_inst_id or spot_inst_id
            cached_candles = self._build_resolution_candles(cached_volatility, resolution=resolution)
            local_count = len(cached_candles)
            if cached_candles:
                self.loaded.emit(
                    self._request_id,
                    self._make_payload(
                        candles=cached_candles,
                        source="Deribit BTC DVOL（本地缓存）",
                        local_count=local_count,
                        remote_added_count=0,
                        cache_synced=False,
                    ),
                )
                local_preview_emitted = True

        if self.isInterruptionRequested():
            raise InterruptedError("波动率副图加载已取消")

        now_ms = int(time.time() * 1000)
        fetch_start_ts = (
            _hourly_fetch_start_ts(cached_volatility=cached_volatility, cached_spot=cached_spot)
            if cached_hourly is not None
            else DERIBIT_FULL_HISTORY_START_TS
        )

        try:
            fetched_volatility = DeribitRestClient().get_volatility_index_candles(
                "BTC",
                DERIBIT_BASE_HOURLY_RESOLUTION,
                start_ts=fetch_start_ts,
                end_ts=now_ms,
                max_records=None,
            )
            fetched_spot = OkxRestClient().get_candles_history_range(
                spot_inst_id,
                "1H",
                start_ts=fetch_start_ts,
                end_ts=now_ms,
                limit=_hourly_history_limit(fetch_start_ts, now_ms),
            )
            fetched_spot = [candle for candle in fetched_spot if candle.confirmed]
        except Exception:
            if local_preview_emitted:
                return self._make_payload(
                    candles=self._build_resolution_candles(cached_volatility, resolution=resolution),
                    source="Deribit BTC DVOL（本地缓存）",
                    local_count=local_count,
                    remote_added_count=0,
                    cache_synced=False,
                )
            raise

        if cached_hourly is not None:
            hourly_candles = _merge_deribit_candles(cached_volatility, fetched_volatility)
            spot_hourly = _merge_price_candles(cached_spot, fetched_spot)
            cached_ts = {item.ts for item in cached_volatility}
            remote_added_count = len({item.ts for item in fetched_volatility} - cached_ts)
        else:
            hourly_candles = list(fetched_volatility)
            spot_hourly = list(fetched_spot)
            remote_added_count = len(hourly_candles)

        _save_cached_hourly_series(
            "BTC",
            spot_inst_id=spot_inst_id,
            volatility_candles=hourly_candles,
            spot_candles=spot_hourly,
            fetched_at=datetime.now(),
        )

        candles = self._build_resolution_candles(hourly_candles, resolution=resolution)
        return self._make_payload(
            candles=candles,
            source="Deribit BTC DVOL（本地缓存已刷新）" if cached_hourly is not None else "Deribit BTC DVOL",
            local_count=local_count,
            remote_added_count=remote_added_count,
            cache_synced=True,
        )


class KlineAnalysisWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("K线分析")
        self.resize(1680, 980)

        self._request_id = 0
        self._active_request_id = 0
        self._active_primary_request_key: tuple[Any, ...] | None = None
        self._loaded_primary_request_key: tuple[Any, ...] | None = None
        self._loader: KlineDataLoader | None = None
        self._secondary_request_id = 0
        self._active_secondary_request_id = 0
        self._active_secondary_request_key: tuple[Any, ...] | None = None
        self._loaded_secondary_request_key: tuple[Any, ...] | None = None
        self._secondary_loader: KlineDataLoader | None = None
        self._use_native_chart = bool(
            QChartView is not None and (_prefer_native_chart_backend() or QWebEngineView is None)
        )
        self._page_ready = self._use_native_chart
        self._pending_payload: KlineChartPayload | None = None
        self._secondary_pending_payload: KlineChartPayload | None = None
        self._workspace_entries = load_kline_analysis_workspace_entries()
        self._selected_line_index = -1
        self._hovered_line_index = -1
        self._hovered_line_drag_mode: str | None = None
        self._draw_tool = "none"
        self._pending_line_start: tuple[int, float] | None = None
        self._line_drag_state: dict[str, object] | None = None
        self._web = None
        self._native_chart = None
        self._native_chart_view = None
        self._primary_chart_frame = None
        self._secondary_native_chart = None
        self._secondary_native_chart_view = None
        self._secondary_chart_frame = None
        self._chart_stack_splitter = None
        self._primary_period_buttons: dict[str, QPushButton] = {}
        self._active_chart_target = "primary"
        self._initial_load_requested = False
        self._splitter_default_applied = False
        self._native_chart_bootstrap_complete = False
        self._deferred_chart_payload: KlineChartPayload | None = None
        self._deferred_chart_request_id = 0
        self._body_splitter: QSplitter | None = None
        self._control_panel: QFrame | None = None
        self._left_panel_hidden = False
        self._secondary_volatility_loader: SecondaryVolatilityDataLoader | None = None
        self._syncing_chart_range = False
        self._pending_reload_after_load = False
        self._primary_chart_status_text = ""
        self._secondary_chart_status_text = ""

        root = QWidget()
        self.setCentralWidget(root)

        main_layout = QVBoxLayout(root)
        main_layout.setContentsMargins(10, 10, 10, 10)
        main_layout.setSpacing(10)

        self._build_header(main_layout)
        self._build_body(main_layout)
        self._sync_primary_period_buttons()
        self._reload_workspace_view()
        self._build_refresh_timer()
        self._deferred_chart_render_timer = QTimer(self)
        self._deferred_chart_render_timer.setSingleShot(True)
        self._deferred_chart_render_timer.timeout.connect(self._render_deferred_full_chart)

    def showEvent(self, event) -> None:  # noqa: ANN001
        super().showEvent(event)
        self._apply_default_splitter_sizes()
        if self._initial_load_requested:
            return
        self._initial_load_requested = True
        if self._auto_refresh_btn.isChecked() and not self._refresh_timer.isActive():
            self._refresh_timer.start()
        self._set_status("窗口已就绪，正在加载首屏图表...")
        QTimer.singleShot(_INITIAL_WINDOW_LOAD_DELAY_MS, self._load_data)

    def _build_header(self, parent_layout: QVBoxLayout) -> None:
        header = QFrame()
        header.setObjectName("Panel")
        header_layout = QVBoxLayout(header)
        header_layout.setContentsMargins(12, 8, 12, 8)
        header_layout.setSpacing(6)

        top_row = QHBoxLayout()
        top_row.setSpacing(8)

        title = QLabel("全球 K线分析")
        title.setObjectName("SectionTitle")
        top_row.addWidget(title, 0)

        top_row.addSpacing(12)
        top_row.addWidget(QLabel("交易对"))
        self._symbol_input = QLineEdit("BTC-USDT-SWAP")
        self._symbol_input.setMinimumWidth(220)
        self._symbol_input.editingFinished.connect(self._on_symbol_confirmed)
        top_row.addWidget(self._symbol_input, 2)

        self._period_combo = QComboBox()
        self._period_combo.addItems([period for _, period in _PRIMARY_PERIOD_OPTIONS])
        self._period_combo.setCurrentText(_DEFAULT_SINGLE_CHART_PERIOD)
        self._period_combo.hide()
        self._period_combo.currentTextChanged.connect(self._on_period_changed)

        period_toolbar = QWidget()
        period_toolbar_layout = QHBoxLayout(period_toolbar)
        period_toolbar_layout.setContentsMargins(0, 0, 0, 0)
        period_toolbar_layout.setSpacing(6)
        period_toolbar_layout.addWidget(QLabel("级别"))
        for label, period_value in _PRIMARY_PERIOD_OPTIONS:
            button = QPushButton(label)
            button.setCheckable(True)
            button.clicked.connect(lambda _checked=False, value=period_value: self._on_period_button_clicked(value))
            self._primary_period_buttons[period_value] = button
            period_toolbar_layout.addWidget(button)
        top_row.addWidget(period_toolbar, 0)

        self._toggle_left_panel_btn = QPushButton("隐藏左栏")
        self._toggle_left_panel_btn.setCheckable(True)
        self._toggle_left_panel_btn.toggled.connect(self._toggle_left_panel)
        top_row.addWidget(self._toggle_left_panel_btn, 0)

        top_row.addSpacing(10)
        top_row.addWidget(QLabel("均线"))
        self._ema9 = QCheckBox("EMA 15")
        self._ema9.setChecked(True)
        self._ema9.setToolTip("显示或隐藏 EMA 15 均线。")
        self._ema9.toggled.connect(self._sync_chart_options)
        top_row.addWidget(self._ema9)

        self._ema21 = QCheckBox("SMA 50")
        self._ema21.setChecked(True)
        self._ema21.setToolTip("显示或隐藏 SMA 50 均线。")
        self._ema21.toggled.connect(self._sync_chart_options)
        top_row.addWidget(self._ema21)

        shape_signal_tooltip = (
            "形态说明：1H/4H/1D 仅显示均线附近的形态信号。\n"
            "1H 只参考 SMA50；4H/1D 参考 EMA15 或 MA50。\n"
            "规则为形态K线加前2根K线中，至少有1根触碰或穿越对应均线。"
        )
        shape_label = QLabel("形态")
        shape_label.setToolTip(shape_signal_tooltip)
        top_row.addWidget(shape_label)

        self._show_1h_shape_signal_check = QCheckBox("1H")
        self._show_1h_shape_signal_check.setChecked(True)
        self._show_1h_shape_signal_check.setToolTip(shape_signal_tooltip)
        self._show_1h_shape_signal_check.toggled.connect(self._sync_chart_options)
        top_row.addWidget(self._show_1h_shape_signal_check)

        self._show_4h_shape_signal_check = QCheckBox("4H")
        self._show_4h_shape_signal_check.setChecked(True)
        self._show_4h_shape_signal_check.setToolTip(shape_signal_tooltip)
        self._show_4h_shape_signal_check.toggled.connect(self._sync_chart_options)
        top_row.addWidget(self._show_4h_shape_signal_check)

        self._show_1d_shape_signal_check = QCheckBox("1D")
        self._show_1d_shape_signal_check.setChecked(True)
        self._show_1d_shape_signal_check.setToolTip(shape_signal_tooltip)
        self._show_1d_shape_signal_check.toggled.connect(self._sync_chart_options)
        top_row.addWidget(self._show_1d_shape_signal_check)

        self._status = QLabel("就绪")
        self._status.setObjectName("Subtle")
        top_row.addStretch(1)
        top_row.addWidget(self._status, 3, Qt.AlignmentFlag.AlignRight)
        header_layout.addLayout(top_row)

        action_row = QHBoxLayout()
        action_row.setSpacing(10)
        self._secondary_chart_check = QCheckBox("双图联动")
        self._secondary_chart_check.toggled.connect(self._on_secondary_chart_toggled)
        action_row.addWidget(self._secondary_chart_check)

        self._secondary_period_combo = QComboBox()
        self._secondary_period_combo.addItems([period for _, period in _PRIMARY_PERIOD_OPTIONS])
        self._secondary_period_combo.setCurrentText(_DEFAULT_DUAL_SECONDARY_PERIOD)
        self._secondary_period_combo.setEnabled(False)
        self._secondary_period_combo.hide()
        self._secondary_period_combo.currentTextChanged.connect(self._on_secondary_period_changed)

        self._secondary_layout_combo = QComboBox()
        self._secondary_layout_combo.addItem("上下分屏", "vertical")
        self._secondary_layout_combo.addItem("左右分屏", "horizontal")
        self._secondary_layout_combo.setEnabled(False)
        self._secondary_layout_combo.currentIndexChanged.connect(self._on_secondary_layout_changed)
        action_row.addWidget(self._secondary_layout_combo)

        self._secondary_chart_kind_combo = QComboBox()
        self._secondary_chart_kind_combo.addItem("副图K线", "kline")
        self._secondary_chart_kind_combo.addItem("BTC波动率", "volatility")
        self._secondary_chart_kind_combo.setEnabled(False)
        self._secondary_chart_kind_combo.currentIndexChanged.connect(self._on_secondary_chart_kind_changed)
        action_row.addWidget(self._secondary_chart_kind_combo)

        action_row.addSpacing(12)
        action_row.addWidget(QLabel("数量"))
        self._limit_spin = QSpinBox()
        self._limit_spin.setRange(50, 5000)
        self._limit_spin.setSingleStep(50)
        self._limit_spin.setValue(1200)
        self._limit_spin.valueChanged.connect(self._load_data)
        action_row.addWidget(self._limit_spin)

        self._prefer_local_checkbox = QCheckBox("本地优先")
        self._prefer_local_checkbox.setChecked(False)
        self._prefer_local_checkbox.toggled.connect(self._load_data)
        action_row.addWidget(self._prefer_local_checkbox)

        self._secondary_average_kline_check = QCheckBox("平均K线")
        self._secondary_average_kline_check.setEnabled(False)
        self._secondary_average_kline_check.toggled.connect(self._load_data)
        action_row.addWidget(self._secondary_average_kline_check)
        action_row.addStretch(1)

        load_btn = QPushButton("加载")
        load_btn.setObjectName("Primary")
        load_btn.clicked.connect(self._load_data)
        action_row.addWidget(load_btn)

        self._auto_refresh_btn = QPushButton("自动刷新:开" if _AUTO_REFRESH_DEFAULT_ENABLED else "自动刷新:关")
        self._auto_refresh_btn.setCheckable(True)
        self._auto_refresh_btn.setChecked(_AUTO_REFRESH_DEFAULT_ENABLED)
        self._auto_refresh_btn.toggled.connect(self._toggle_auto_refresh)
        action_row.addWidget(self._auto_refresh_btn)

        reset_btn = QPushButton("重置视图")
        reset_btn.clicked.connect(self._reset_chart_view)
        action_row.addWidget(reset_btn)
        header_layout.addLayout(action_row)

        parent_layout.addWidget(header)

    def _build_body(self, parent_layout: QVBoxLayout) -> None:
        splitter = QSplitter(Qt.Orientation.Horizontal)
        splitter.setChildrenCollapsible(False)
        self._body_splitter = splitter

        control = QFrame()
        self._control_panel = control
        control.setObjectName("Panel")
        control.setStyleSheet(
            """
            QPushButton {
                min-height: 26px;
                padding: 2px 8px;
            }
            QPushButton:pressed {
                padding-top: 3px;
                padding-right: 7px;
                padding-bottom: 1px;
                padding-left: 9px;
            }
            QToolButton {
                min-height: 24px;
                padding: 2px 8px;
            }
            """
        )
        control_layout = QVBoxLayout(control)
        control_layout.setContentsMargins(12, 12, 12, 12)
        control_layout.setSpacing(10)

        self._backend_hint = QLabel("")
        self._backend_hint.setObjectName("Subtle")
        self._backend_hint.setWordWrap(True)
        control_layout.addWidget(self._backend_hint)

        control_layout.addWidget(QLabel("告警引擎"))
        self._ma_cross_alert_check = QCheckBox("EMA 15 与 SMA 50 交叉")
        self._ma_cross_alert_check.toggled.connect(self._save_workspace_settings)
        control_layout.addWidget(self._ma_cross_alert_check)

        self._box_breakout_alert_check = QCheckBox("自动箱体突破")
        self._box_breakout_alert_check.toggled.connect(self._save_workspace_settings)
        self._box_breakout_alert_check.toggled.connect(self._sync_chart_options)
        control_layout.addWidget(self._box_breakout_alert_check)

        self._live_box_check = QCheckBox("实盘箱体")
        self._live_box_check.setChecked(False)
        self._live_box_check.toggled.connect(self._save_workspace_settings)
        self._live_box_check.toggled.connect(self._sync_chart_options)
        control_layout.addWidget(self._live_box_check)

        self._structure_hint = QLabel("")
        self._structure_hint.setObjectName("Subtle")
        self._structure_hint.setWordWrap(True)
        control_layout.addWidget(self._structure_hint)

        control_layout.addWidget(QLabel("画线预警"))
        line_toolbar = QHBoxLayout()
        cursor_btn = QPushButton("光标")
        cursor_btn.clicked.connect(lambda: self._set_draw_tool("none"))
        line_toolbar.addWidget(cursor_btn)
        hline_btn = QPushButton("水平线")
        hline_btn.clicked.connect(lambda: self._set_draw_tool("horizontal"))
        line_toolbar.addWidget(hline_btn)
        trend_btn = QPushButton("趋势线")
        trend_btn.clicked.connect(lambda: self._set_draw_tool("trend"))
        line_toolbar.addWidget(trend_btn)
        control_layout.addLayout(line_toolbar)

        self._line_label_edit = QLineEdit()
        self._line_label_edit.setPlaceholderText("线条名称")
        control_layout.addWidget(self._line_label_edit)

        line_rule_row = QHBoxLayout()
        self._line_trigger_combo = QComboBox()
        self._line_trigger_combo.addItem("上穿", "cross_above")
        self._line_trigger_combo.addItem("下破", "cross_below")
        self._line_trigger_combo.addItem("触碰", "touch")
        line_rule_row.addWidget(self._line_trigger_combo, 1)
        self._line_action_combo = QComboBox()
        self._line_action_combo.addItem("提醒", "notify")
        self._line_action_combo.addItem("做多", "long")
        self._line_action_combo.addItem("做空", "short")
        line_rule_row.addWidget(self._line_action_combo, 1)
        control_layout.addLayout(line_rule_row)

        self._line_enabled_check = QCheckBox("启用当前线条")
        control_layout.addWidget(self._line_enabled_check)

        line_manage_row = QHBoxLayout()
        update_line_btn = QPushButton("更新")
        update_line_btn.clicked.connect(self._update_selected_line)
        line_manage_row.addWidget(update_line_btn)
        delete_line_btn = QPushButton("删除")
        delete_line_btn.clicked.connect(self._delete_selected_line)
        line_manage_row.addWidget(delete_line_btn)
        control_layout.addLayout(line_manage_row)

        self._line_table = QTableWidget(0, 5)
        self._line_table.setHorizontalHeaderLabels(["标签", "类型", "触发", "操作", "状态"])
        self._line_table.verticalHeader().setVisible(False)
        self._line_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self._line_table.itemSelectionChanged.connect(self._on_line_selected)
        self._line_table.setMinimumHeight(150)
        control_layout.addWidget(self._line_table)

        control_layout.addWidget(QLabel("事件日志"))
        self._event_log = QTextEdit()
        self._event_log.setReadOnly(True)
        self._event_log.setMinimumHeight(160)
        control_layout.addWidget(self._event_log)

        control_layout.addStretch(1)

        chart_host = QFrame()
        chart_host.setObjectName("Panel")
        chart_layout = QVBoxLayout(chart_host)
        chart_layout.setContentsMargins(8, 8, 8, 8)
        chart_layout.setSpacing(0)

        if self._use_native_chart:
            self._create_native_chart(chart_layout)
        elif QWebEngineView is None:
            fallback = QLabel("当前环境未检测到QWebEngine")
            fallback.setObjectName("Subtle")
            fallback.setAlignment(Qt.AlignmentFlag.AlignCenter)
            chart_layout.addWidget(fallback, 1)
        else:
            self._web = QWebEngineView()
            self._web.setHtml(self._chart_html())
            self._web.loadFinished.connect(self._on_chart_ready)
            chart_layout.addWidget(self._web, 1)

        splitter.addWidget(control)
        splitter.addWidget(chart_host)
        splitter.setStretchFactor(0, 1)
        splitter.setStretchFactor(1, 5)
        parent_layout.addWidget(splitter, 1)

        self._refresh_timer = QTimer(self)
        self._refresh_timer.timeout.connect(self._load_data)

    @staticmethod
    def _auto_refresh_interval_ms(period: str) -> int:
        period = period.strip()
        mapping = {
            "1m": 15000,
            "3m": 20000,
            "5m": 25000,
            "15m": 45000,
            "1H": 60_000,
            "4H": 3 * 60_000,
            "1D": 10 * 60_000,
        }
        return mapping.get(period, 60_000)

    def _build_refresh_timer(self) -> None:
        self._refresh_timer.setInterval(self._auto_refresh_interval_ms(self._period_combo.currentText()))

    def _apply_default_splitter_sizes(self) -> None:
        splitter = self._body_splitter
        if splitter is None or self._splitter_default_applied:
            return
        available_width = splitter.width() or self.width() or 1680
        left_width, right_width = _default_kline_splitter_sizes(available_width)
        splitter.setSizes([left_width, right_width])
        self._splitter_default_applied = True

    def _secondary_chart_kind(self) -> str:
        combo = getattr(self, "_secondary_chart_kind_combo", None)
        if combo is None:
            return "kline"
        value = str(combo.currentData() or "kline").strip().lower()
        return value if value in {"kline", "volatility"} else "kline"

    def _secondary_layout_mode(self) -> str:
        combo = getattr(self, "_secondary_layout_combo", None)
        if combo is None:
            return "vertical"
        value = str(combo.currentData() or "vertical").strip().lower()
        return value if value in {"vertical", "horizontal"} else "vertical"

    def _secondary_display_symbol(self) -> str:
        if self._secondary_chart_kind() == "volatility":
            return "BTC DVOL"
        return self._symbol_input.text().strip().upper()

    def _secondary_chart_venue_label(self) -> str:
        return "DERIBIT" if self._secondary_chart_kind() == "volatility" else "OKX"

    def _secondary_chart_note_lines(self, payload: KlineChartPayload, *, period: str) -> list[str]:
        if self._secondary_chart_kind() != "volatility":
            return []
        normalized_period = period.strip().upper()
        aggregation_text = "1H直连" if normalized_period == "1H" else f"{normalized_period}本地聚合"
        average_text = "开" if self._secondary_average_kline_check.isChecked() else "关"
        source_text = str(payload.stats.get("source", "Deribit BTC DVOL") or "Deribit BTC DVOL")
        source_text = source_text.replace("（本地聚合）", "")
        return [f"BTC波动率 | 平均K线 {average_text} | {aggregation_text} | 来源 {source_text}"]

    def _sync_chart_range_to_other(self, *, target: str, start_x: float, end_x: float) -> None:
        if self._syncing_chart_range or not self._secondary_chart_check.isChecked() or not self._use_native_chart:
            return
        target_view = self._secondary_native_chart_view if target == "secondary" else self._native_chart_view
        if not isinstance(target_view, InteractiveKlineChartView):
            return
        self._syncing_chart_range = True
        try:
            target_view.set_external_x_range(float(start_x), float(end_x))
        finally:
            self._syncing_chart_range = False

    def _sync_secondary_chart_range_from_primary(self) -> None:
        if not self._secondary_chart_check.isChecked():
            return
        if not isinstance(self._native_chart_view, InteractiveKlineChartView):
            return
        start_x, end_x = self._native_chart_view.current_x_range()
        self._sync_chart_range_to_other(target="secondary", start_x=start_x, end_x=end_x)

    def _toggle_left_panel(self, hidden: bool) -> None:
        self._left_panel_hidden = bool(hidden)
        if self._toggle_left_panel_btn is not None:
            self._toggle_left_panel_btn.setText("显示左栏" if hidden else "隐藏左栏")
        splitter = self._body_splitter
        control = self._control_panel
        if splitter is None or control is None:
            return
        control.setVisible(not hidden)
        available_width = splitter.width() or self.width() or 1680
        if hidden:
            splitter.setSizes([0, available_width])
            return
        left_width, right_width = _default_kline_splitter_sizes(available_width)
        splitter.setSizes([left_width, right_width])

    def _update_secondary_controls_state(self) -> None:
        enabled = bool(self._secondary_chart_check.isChecked())
        is_volatility = self._secondary_chart_kind() == "volatility"
        self._secondary_period_combo.setEnabled(enabled)
        self._secondary_layout_combo.setEnabled(enabled)
        self._secondary_chart_kind_combo.setEnabled(enabled)
        self._secondary_average_kline_check.setEnabled(enabled and is_volatility)

    def _apply_secondary_chart_layout(self) -> None:
        splitter = self._chart_stack_splitter
        if splitter is None:
            return
        enabled = bool(self._secondary_chart_check.isChecked())
        layout_mode = self._secondary_layout_mode()
        splitter.setOrientation(Qt.Orientation.Horizontal if layout_mode == "horizontal" else Qt.Orientation.Vertical)
        if not enabled:
            splitter.setSizes([1, 0])
            return
        if layout_mode == "horizontal":
            available_width = splitter.width() or self.width() or 1600
            left_width, right_width = _default_chart_stack_horizontal_sizes(available_width)
            splitter.setSizes([left_width, right_width])
            return
        available_height = splitter.height() or self.height() or 920
        top_height, bottom_height = _default_chart_stack_splitter_sizes(available_height)
        splitter.setSizes([top_height, bottom_height])

    def _apply_secondary_chart_visibility(self) -> None:
        if self._secondary_chart_frame is None:
            return
        enabled = bool(self._secondary_chart_check.isChecked())
        self._secondary_chart_frame.setVisible(enabled)
        self._update_secondary_controls_state()
        self._apply_secondary_chart_layout()

    def _active_period_value(self) -> str:
        if self._active_chart_target == "secondary" and self._secondary_chart_check.isChecked():
            return self._secondary_period_combo.currentText().strip()
        return self._period_combo.currentText().strip()

    def _sync_primary_period_buttons(self) -> None:
        current_period = self._active_period_value()
        for period_value, button in self._primary_period_buttons.items():
            button.setChecked(period_value == current_period)

    def _chart_frame_style(self, *, active: bool) -> str:
        border_color = "#60a5fa" if active else "#111827"
        border_width = 2 if active else 1
        return (
            "QFrame { "
            f"background: {_CHART_BACKGROUND_COLOR}; "
            f"border: {border_width}px solid {border_color}; "
            "}"
        )

    def _refresh_chart_selection_visuals(self) -> None:
        if self._primary_chart_frame is not None:
            self._primary_chart_frame.setStyleSheet(
                self._chart_frame_style(active=self._active_chart_target == "primary")
            )
        if self._secondary_chart_frame is not None:
            self._secondary_chart_frame.setStyleSheet(
                self._chart_frame_style(
                    active=self._active_chart_target == "secondary" and self._secondary_chart_check.isChecked()
                )
            )

    def _set_active_chart_target(self, target: str) -> None:
        resolved = "secondary" if target == "secondary" and self._secondary_chart_check.isChecked() else "primary"
        if self._active_chart_target == resolved:
            self._refresh_chart_selection_visuals()
            self._sync_primary_period_buttons()
            return
        self._active_chart_target = resolved
        self._refresh_chart_selection_visuals()
        self._sync_primary_period_buttons()

    def _apply_chart_mode_period_defaults(self, *, dual_enabled: bool) -> None:
        primary_period = _DEFAULT_DUAL_PRIMARY_PERIOD if dual_enabled else _DEFAULT_SINGLE_CHART_PERIOD
        secondary_period = _DEFAULT_DUAL_SECONDARY_PERIOD
        self._period_combo.blockSignals(True)
        self._secondary_period_combo.blockSignals(True)
        self._period_combo.setCurrentText(primary_period)
        self._secondary_period_combo.setCurrentText(secondary_period)
        self._period_combo.blockSignals(False)
        self._secondary_period_combo.blockSignals(False)
        self._sync_primary_period_buttons()
        self._refresh_timer.setInterval(self._auto_refresh_interval_ms(primary_period))
        self._reload_workspace_view()

    @Slot(str)
    def _on_period_button_clicked(self, period_value: str) -> None:
        if self._active_chart_target == "secondary" and self._secondary_chart_check.isChecked():
            if self._secondary_chart_kind() == "volatility" and period_value not in {"1H", "4H", "1D"}:
                period_value = "1H"
            if self._secondary_period_combo.currentText().strip() != period_value:
                self._secondary_period_combo.setCurrentText(period_value)
            else:
                self._load_data()
            return
        if self._period_combo.currentText().strip() != period_value:
            self._period_combo.setCurrentText(period_value)
        else:
            self._load_data()

    def closeEvent(self, event) -> None:  # noqa: ANN001
        if self._loader is not None and self._loader.isRunning():
            self._loader.requestInterruption()
            self._loader.wait(1000)
        if self._secondary_loader is not None and self._secondary_loader.isRunning():
            self._secondary_loader.requestInterruption()
            self._secondary_loader.wait(1000)
        if self._secondary_volatility_loader is not None and self._secondary_volatility_loader.isRunning():
            self._secondary_volatility_loader.requestInterruption()
            self._secondary_volatility_loader.wait(1000)
        if self._deferred_chart_render_timer.isActive():
            self._deferred_chart_render_timer.stop()
        if self._refresh_timer.isActive():
            self._refresh_timer.stop()
        super().closeEvent(event)

    def keyPressEvent(self, event) -> None:  # noqa: ANN001
        if event.key() == Qt.Key.Key_Escape and self._draw_tool != "none":
            self._set_draw_tool("none")
            event.accept()
            return
        if event.key() in (Qt.Key.Key_Delete, Qt.Key.Key_Backspace) and self._selected_line_index >= 0:
            self._delete_selected_line()
            event.accept()
            return
        super().keyPressEvent(event)

    def _has_active_loaders(self) -> bool:
        return bool(
            (self._loader is not None and self._loader.isRunning())
            or (self._secondary_loader is not None and self._secondary_loader.isRunning())
            or (
                self._secondary_volatility_loader is not None
                and self._secondary_volatility_loader.isRunning()
            )
        )

    def _current_primary_request_key(self) -> tuple[Any, ...]:
        return (
            "primary",
            self._symbol_input.text().strip().upper(),
            self._period_combo.currentText().strip().upper(),
            max(50, self._limit_spin.value()),
            bool(self._prefer_local_checkbox.isChecked()),
        )

    def _current_secondary_request_key(self, *, symbol: str | None = None) -> tuple[Any, ...] | None:
        if not self._secondary_chart_check.isChecked() or not self._use_native_chart:
            return None
        secondary_period = self._secondary_period_combo.currentText().strip().upper()
        requested_limit = max(50, self._limit_spin.value())
        chart_kind = self._secondary_chart_kind()
        if chart_kind == "volatility":
            return (
                "secondary",
                chart_kind,
                secondary_period,
                requested_limit,
                bool(self._secondary_average_kline_check.isChecked()),
            )
        return (
            "secondary",
            chart_kind,
            (symbol or self._symbol_input.text().strip().upper()),
            secondary_period,
            requested_limit,
            bool(self._prefer_local_checkbox.isChecked()),
        )

    def _schedule_pending_reload_if_ready(self) -> None:
        if not self._pending_reload_after_load or self._has_active_loaders():
            return
        self._pending_reload_after_load = False
        QTimer.singleShot(10, self._load_data)

    @Slot()
    def _load_data(self) -> None:
        symbol = self._symbol_input.text().strip().upper()
        period = self._period_combo.currentText()
        if not symbol:
            self._set_status("请输入交易对")
            return
        if self._has_active_loaders():
            self._pending_reload_after_load = True
            self._set_status("当前仍在加载，已排队刷新最新选择，请稍候...")
            return
        requested_limit = max(50, self._limit_spin.value())
        workspace_entry = self._workspace_entry(symbol=symbol, period=period)
        self._reload_workspace_view(symbol=symbol, period=period)
        if self._deferred_chart_render_timer.isActive():
            self._deferred_chart_render_timer.stop()
        self._deferred_chart_payload = None
        self._deferred_chart_request_id = 0
        self._pending_reload_after_load = False
        self._request_id += 1
        self._active_request_id = self._request_id
        self._active_primary_request_key = self._current_primary_request_key()
        self._set_status(
            f"正在加载 {symbol} {period} | K线={requested_limit} | "
            f"{'本地缓存' if self._prefer_local_checkbox.isChecked() else '本地优先'}"
        )

        self._loader = KlineDataLoader(
            request_id=self._request_id,
            symbol=symbol,
            period=period,
            limit=requested_limit,
            local_only=self._prefer_local_checkbox.isChecked(),
            workspace_entry=workspace_entry,
        )
        self._loader.loaded.connect(self._on_data_loaded)
        self._loader.failed.connect(self._on_data_failed)
        self._loader.finished.connect(self._on_loader_finished)
        self._loader.start()
        if self._secondary_chart_check.isChecked() and self._use_native_chart:
            self._load_secondary_data(symbol=symbol)
        else:
            self._active_secondary_request_key = None
            self._loaded_secondary_request_key = None
            self._secondary_pending_payload = None
            if isinstance(self._secondary_native_chart_view, InteractiveKlineChartView):
                self._secondary_native_chart_view.set_external_hover_time(None)
            if self._secondary_native_chart is not None:
                self._secondary_native_chart.setTitle("副图")

    @Slot()
    def _on_loader_finished(self) -> None:
        if self._loader is not None:
            self._loader.deleteLater()
            self._loader = None
        self._schedule_pending_reload_if_ready()

    def _load_secondary_data(self, *, symbol: str) -> None:
        secondary_period = self._secondary_period_combo.currentText().strip()
        requested_limit = max(50, self._limit_spin.value())
        self._secondary_request_id += 1
        self._active_secondary_request_id = self._secondary_request_id
        self._active_secondary_request_key = self._current_secondary_request_key(symbol=symbol)
        if self._secondary_chart_kind() == "volatility":
            self._secondary_volatility_loader = SecondaryVolatilityDataLoader(
                request_id=self._secondary_request_id,
                period=secondary_period,
                limit=requested_limit,
                average_kline=bool(self._secondary_average_kline_check.isChecked()),
            )
            self._secondary_volatility_loader.loaded.connect(self._on_secondary_data_loaded)
            self._secondary_volatility_loader.failed.connect(self._on_secondary_data_failed)
            self._secondary_volatility_loader.finished.connect(self._on_secondary_volatility_loader_finished)
            self._secondary_volatility_loader.start()
            return
        self._secondary_loader = KlineDataLoader(
            request_id=self._secondary_request_id,
            symbol=symbol,
            period=secondary_period,
            limit=requested_limit,
            local_only=self._prefer_local_checkbox.isChecked(),
            workspace_entry={},
            enable_alerts=False,
        )
        self._secondary_loader.loaded.connect(self._on_secondary_data_loaded)
        self._secondary_loader.failed.connect(self._on_secondary_data_failed)
        self._secondary_loader.finished.connect(self._on_secondary_loader_finished)
        self._secondary_loader.start()

    @Slot()
    def _on_secondary_loader_finished(self) -> None:
        if self._secondary_loader is not None:
            self._secondary_loader.deleteLater()
            self._secondary_loader = None
        self._schedule_pending_reload_if_ready()

    @Slot()
    def _on_secondary_volatility_loader_finished(self) -> None:
        if self._secondary_volatility_loader is not None:
            self._secondary_volatility_loader.deleteLater()
            self._secondary_volatility_loader = None
        self._schedule_pending_reload_if_ready()

    @Slot()
    def _on_symbol_confirmed(self) -> None:
        self._reload_workspace_view()
        self._load_data()

    @Slot(str)
    def _on_period_changed(self, _value: str) -> None:
        self._sync_primary_period_buttons()
        self._refresh_timer.setInterval(self._auto_refresh_interval_ms(_value))
        self._reload_workspace_view()
        self._sync_chart_options()
        if self._auto_refresh_btn.isChecked():
            self._load_data()

    @Slot(bool)
    def _on_secondary_chart_toggled(self, enabled: bool) -> None:
        self._apply_secondary_chart_visibility()
        self._apply_chart_mode_period_defaults(dual_enabled=enabled)
        if not enabled:
            self._set_active_chart_target("primary")
        else:
            self._refresh_chart_selection_visuals()
            self._sync_primary_period_buttons()
        if enabled:
            self._load_data()
        else:
            self._secondary_chart_status_text = ""
            if self._primary_chart_status_text:
                self._set_status(f"主图：{self._primary_chart_status_text}")
            self._secondary_pending_payload = None
            if isinstance(self._secondary_native_chart_view, InteractiveKlineChartView):
                self._secondary_native_chart_view.set_external_hover_time(None)
            if isinstance(self._native_chart_view, InteractiveKlineChartView):
                self._native_chart_view.set_external_hover_time(None)

    @Slot()
    def _on_secondary_layout_changed(self) -> None:
        self._apply_secondary_chart_layout()

    @Slot()
    def _on_secondary_chart_kind_changed(self) -> None:
        if self._secondary_chart_kind() == "volatility":
            current_period = self._secondary_period_combo.currentText().strip().upper()
            if current_period not in {"1H", "4H", "1D"}:
                self._secondary_period_combo.blockSignals(True)
                self._secondary_period_combo.setCurrentText("1H")
                self._secondary_period_combo.blockSignals(False)
        else:
            self._secondary_chart_status_text = ""
            if self._primary_chart_status_text:
                self._set_status(f"主图：{self._primary_chart_status_text}")
        self._update_secondary_controls_state()
        if self._secondary_chart_check.isChecked():
            self._load_data()

    @Slot(str)
    def _on_secondary_period_changed(self, _value: str) -> None:
        if self._secondary_chart_kind() == "volatility" and _value.strip().upper() not in {"1H", "4H", "1D"}:
            self._secondary_period_combo.blockSignals(True)
            self._secondary_period_combo.setCurrentText("1H")
            self._secondary_period_combo.blockSignals(False)
            _value = "1H"
        if self._active_chart_target == "secondary":
            self._sync_primary_period_buttons()
        if self._secondary_chart_check.isChecked():
            self._load_data()

    @Slot(int, KlineChartPayload)
    def _on_data_loaded(self, request_id: int, payload: KlineChartPayload) -> None:
        try:
            if request_id != self._active_request_id:
                return
            if self._active_primary_request_key != self._current_primary_request_key():
                self._set_status("主图结果已过期，正在刷新最新选择...")
                return
            self._pending_payload = payload
            self._loaded_primary_request_key = self._active_primary_request_key
            start_ts = payload.stats.get("start_ms")
            end_ts = payload.stats.get("end_ms")
            time_text = (
                f"{_format_bar_time(start_ts)} -> {_format_bar_time(end_ts)}"
                if isinstance(start_ts, int) and isinstance(end_ts, int)
                else "时间范围未知"
            )
            loaded = int(payload.stats.get("returned", 0) or 0)
            source = str(payload.stats.get("source", ""))
            local_count = int(payload.stats.get("local_count", 0) or 0)
            api_count = int(payload.stats.get("remote_added_count", 0) or 0)
            local_stale = bool(payload.stats.get("local_stale", False))
            cache_synced = bool(payload.stats.get("cache_synced", False))
            sync_text = " | 已同步最新缓存" if cache_synced else ""
            cache_state = "陈旧" if local_stale else "最新"
            self._primary_chart_status_text = (
                f"{loaded}条 | {_source_status_text(source)} | 缓存{cache_state} | 本地{local_count} | 新增{api_count} | {time_text}"
            )
            if sync_text:
                self._primary_chart_status_text = f"{self._primary_chart_status_text}{sync_text}"
            combined_status_parts = [f"主图：{self._primary_chart_status_text}"]
            if self._secondary_chart_status_text:
                combined_status_parts.append(f"副图：{self._secondary_chart_status_text}")
            self._set_status(" | ".join(combined_status_parts))
            if self._page_ready:
                self._render_loaded_payload(payload)
                if self._secondary_chart_check.isChecked() and self._secondary_pending_payload is not None and self._use_native_chart:
                    self._sync_secondary_chart_range_from_primary()
            self._apply_alert_snapshot(payload.alert_snapshot)
            self._update_refresh_hint()
        except Exception as exc:
            self._set_status(f"数据处理异常：{exc}")

    @Slot(int, KlineChartPayload)
    def _on_secondary_data_loaded(self, request_id: int, payload: KlineChartPayload) -> None:
        if request_id != self._active_secondary_request_id:
            return
        if self._active_secondary_request_key != self._current_secondary_request_key():
            self._set_status("副图结果已过期，正在刷新最新选择...")
            return
        self._secondary_pending_payload = payload
        self._loaded_secondary_request_key = self._active_secondary_request_key
        if self._secondary_chart_kind() == "volatility":
            loaded = int(payload.stats.get("returned", 0) or 0)
            local_count = int(payload.stats.get("local_count", 0) or 0)
            api_count = int(payload.stats.get("remote_added_count", 0) or 0)
            cache_synced = bool(payload.stats.get("cache_synced", False))
            if cache_synced:
                if local_count > 0:
                    self._secondary_chart_status_text = (
                        f"波动率已刷新 | {loaded}条 | 新增{api_count}条"
                    )
                else:
                    self._secondary_chart_status_text = f"波动率已写入缓存 | {loaded}条"
            else:
                self._secondary_chart_status_text = f"波动率缓存预览 | {loaded}条 | 后台同步中"
            combined_status_parts: list[str] = []
            if self._primary_chart_status_text:
                combined_status_parts.append(f"主图：{self._primary_chart_status_text}")
            if self._secondary_chart_status_text:
                combined_status_parts.append(f"副图：{self._secondary_chart_status_text}")
            if combined_status_parts:
                self._set_status(" | ".join(combined_status_parts))
        if self._secondary_chart_check.isChecked() and self._use_native_chart:
            self._render_secondary_chart(payload)
            self._sync_secondary_chart_range_from_primary()
        if (
            self._secondary_chart_check.isChecked()
            and self._period_combo.currentText().strip().upper() == "4H"
            and self._secondary_period_combo.currentText().strip().upper() == "1D"
            and self._pending_payload is not None
            and self._page_ready
            and self._secondary_pending_payload is not None
            and self._use_native_chart
        ):
            self._sync_chart_options()

    @Slot(int, str)
    def _on_data_failed(self, request_id: int, message: str) -> None:
        if request_id != self._active_request_id:
            return
        self._set_status(f"加载失败：{message}")
        if self._use_native_chart and self._native_chart is not None:
            self._native_chart.setTitle(f"加载失败：{message}")
        else:
            self._run_js("window.handleChartWarning(%s);" % json.dumps(message))

    @Slot(int, str)
    def _on_secondary_data_failed(self, request_id: int, message: str) -> None:
        if request_id != self._active_secondary_request_id:
            return
        if self._secondary_native_chart is not None:
            self._secondary_native_chart.setTitle(f"副图加载失败：{message}")

    @Slot(object)
    def _on_primary_hover_time_changed(self, candle_time: object) -> None:
        if isinstance(self._secondary_native_chart_view, InteractiveKlineChartView):
            self._secondary_native_chart_view.set_external_hover_time(None if candle_time is None else int(candle_time))

    @Slot(object)
    def _on_secondary_hover_time_changed(self, candle_time: object) -> None:
        if isinstance(self._native_chart_view, InteractiveKlineChartView):
            self._native_chart_view.set_external_hover_time(None if candle_time is None else int(candle_time))

    @Slot(float, float)
    def _on_primary_x_range_changed(self, start_x: float, end_x: float) -> None:
        self._sync_chart_range_to_other(target="secondary", start_x=start_x, end_x=end_x)

    @Slot(float, float)
    def _on_secondary_x_range_changed(self, start_x: float, end_x: float) -> None:
        self._sync_chart_range_to_other(target="primary", start_x=start_x, end_x=end_x)

    @Slot(bool)
    def _toggle_auto_refresh(self, enabled: bool) -> None:
        if enabled:
            self._auto_refresh_btn.setText("自动刷新:开")
            self._refresh_timer.start()
            self._load_data()
        else:
            self._auto_refresh_btn.setText("自动刷新:关")
            self._refresh_timer.stop()

    @Slot(bool)
    def _on_chart_ready(self, _ok: bool) -> None:
        self._page_ready = True
        if self._pending_payload is not None:
            self._render_to_chart(self._pending_payload)

    @Slot()
    def _reset_chart_view(self) -> None:
        if self._use_native_chart:
            if self._deferred_chart_payload is not None:
                if self._deferred_chart_render_timer.isActive():
                    self._deferred_chart_render_timer.stop()
                payload = self._deferred_chart_payload
                self._deferred_chart_payload = None
                self._deferred_chart_request_id = 0
                self._render_to_native_chart(payload)
                self._native_chart_bootstrap_complete = True
            if isinstance(self._native_chart_view, InteractiveKlineChartView):
                self._native_chart_view.reset_view()
            elif self._pending_payload is not None:
                self._render_to_native_chart(self._pending_payload)
            if isinstance(self._secondary_native_chart_view, InteractiveKlineChartView):
                self._sync_secondary_chart_range_from_primary()
            elif self._secondary_pending_payload is not None:
                self._render_secondary_chart(self._secondary_pending_payload)
            return
        self._run_js("window.resetChartView();")

    @Slot()
    def _sync_chart_options(self) -> None:
        if not self._page_ready or self._pending_payload is None:
            return
        if self._loaded_primary_request_key != self._current_primary_request_key():
            return
        self._render_loaded_payload(self._pending_payload)
        if (
            self._secondary_pending_payload is not None
            and self._secondary_chart_check.isChecked()
            and self._loaded_secondary_request_key == self._current_secondary_request_key()
        ):
            self._render_secondary_chart(self._secondary_pending_payload)

    def _render_to_chart(self, payload: KlineChartPayload) -> None:
        if self._use_native_chart:
            self._render_to_native_chart(payload)
            return
        period = self._period_combo.currentText().strip()
        signal_markers = self._filter_replay_signal_markers_for_chart(
            payload.signal_markers,
            period=period,
            is_secondary=False,
        )
        trend_payload = payload.trend_indicator if _supports_trend_indicator(period) else []
        if not isinstance(trend_payload, list):
            trend_payload = []
        payload_map: dict[str, Any] = {
            "candles": payload.candles,
            "ema": {
                "ema9": payload.ema_9,
                "ema21": payload.ema_21,
            },
            "show": {
                "ema9": self._ema9.isChecked(),
                "ema21": self._ema21.isChecked(),
            },
            "period": period,
            "trend": trend_payload,
            "signals": signal_markers,
            "boxes": self._visible_box_overlays(payload),
        }
        try:
            self._run_js(f"window.applyChartData({json.dumps(payload_map)});")
        except Exception as exc:
            self._set_status(f"图表渲染异常：{exc}")

    def _daily_trend_reference(self, *, period: str, is_secondary: bool) -> list[dict[str, Any]]:
        if period.strip().upper() != "4H":
            return []
        if is_secondary:
            if self._period_combo.currentText().strip().upper() != "1D":
                return []
            if self._pending_payload is None:
                return []
            return [dict(item) for item in self._pending_payload.trend_indicator if isinstance(item, dict)]
        if (
            not self._secondary_chart_check.isChecked()
            or self._secondary_chart_kind() != "kline"
            or self._secondary_period_combo.currentText().strip().upper() != "1D"
            or self._secondary_pending_payload is None
        ):
            return []
        return [dict(item) for item in self._secondary_pending_payload.trend_indicator if isinstance(item, dict)]

    def _filter_replay_signal_markers_for_chart(
        self,
        signal_markers: list[dict[str, Any]],
        *,
        period: str,
        is_secondary: bool,
    ) -> list[dict[str, Any]]:
        normalized_period = period.strip().upper()
        if normalized_period == "4H":
            signal_check = getattr(self, "_show_4h_shape_signal_check", None)
        elif normalized_period == "1H":
            signal_check = getattr(self, "_show_1h_shape_signal_check", None)
        elif normalized_period == "1D":
            signal_check = getattr(self, "_show_1d_shape_signal_check", None)
        else:
            return []
        if signal_check is None or not signal_check.isChecked():
            return []

        trend_ref = self._daily_trend_reference(period=period, is_secondary=is_secondary)
        if not signal_markers or not trend_ref:
            return signal_markers

        filtered: list[dict[str, Any]] = []
        for marker in signal_markers:
            if not isinstance(marker, dict):
                continue
            marker_time = _normalize_signal_time(marker.get("time"))
            if marker_time is None:
                filtered.append(marker)
                continue
            direction = str(marker.get("direction", "")).strip().lower()
            trend_state = _trend_state_at_time(trend_ref, marker_time)
            if direction == "long" and _is_bear_trend(trend_state):
                continue
            if direction == "short" and _is_bull_trend(trend_state):
                continue
            filtered.append(marker)
        return filtered

    def _visible_box_overlays(self, payload: KlineChartPayload) -> list[dict[str, Any]]:
        show_history = self._box_breakout_alert_check.isChecked()
        show_realtime = getattr(self, "_live_box_check", None)
        show_realtime = bool(show_realtime.isChecked()) if show_realtime is not None else False
        if not show_history and not show_realtime:
            return []
        visible: list[dict[str, Any]] = []
        for item in payload.box_overlays:
            if not isinstance(item, dict):
                continue
            mode = str(item.get("mode", "history")).strip().lower()
            if mode == "realtime" and not show_realtime:
                continue
            if mode != "realtime" and not show_history:
                continue
            visible.append(dict(item))
        return visible

    def _render_loaded_payload(self, payload: KlineChartPayload) -> None:
        if not self._use_native_chart:
            self._render_to_chart(payload)
            return
        if self._should_stage_native_bootstrap(payload):
            preview_payload = _slice_chart_payload_tail(payload, _NATIVE_BOOTSTRAP_RENDER_BARS)
            self._render_to_native_chart(preview_payload)
            self._deferred_chart_payload = payload
            self._deferred_chart_request_id = self._active_request_id
            self._deferred_chart_render_timer.start(_NATIVE_BOOTSTRAP_RENDER_DELAY_MS)
            return
        if self._deferred_chart_render_timer.isActive():
            self._deferred_chart_render_timer.stop()
        self._deferred_chart_payload = None
        self._deferred_chart_request_id = 0
        self._render_to_native_chart(payload)
        self._native_chart_bootstrap_complete = True

    @Slot()
    def _render_deferred_full_chart(self) -> None:
        if (
            self._deferred_chart_payload is None
            or self._deferred_chart_request_id != self._active_request_id
            or not self._page_ready
        ):
            return
        payload = self._deferred_chart_payload
        self._deferred_chart_payload = None
        self._deferred_chart_request_id = 0
        self._render_to_native_chart(payload)
        self._native_chart_bootstrap_complete = True

    def _should_stage_native_bootstrap(self, payload: KlineChartPayload) -> bool:
        return (
            self._use_native_chart
            and not self._native_chart_bootstrap_complete
            and len(payload.candles) > _NATIVE_BOOTSTRAP_RENDER_BARS
        )

    def _run_js(self, js: str) -> None:
        if self._web is None:
            return
        page = self._web.page()
        if page is None:
            return
        page.runJavaScript(js)

    def _create_native_chart(self, chart_layout: QVBoxLayout) -> None:
        if QChart is None or QChartView is None:
            fallback = QLabel("QtCharts is not available in this environment.")
            fallback.setObjectName("Subtle")
            fallback.setAlignment(Qt.AlignmentFlag.AlignCenter)
            chart_layout.addWidget(fallback, 1)
            self._page_ready = False
            return
        chart_splitter = QSplitter(Qt.Orientation.Vertical)
        chart_splitter.setChildrenCollapsible(False)
        chart_splitter.setOpaqueResize(True)
        chart_splitter.setHandleWidth(_SECONDARY_CHART_SPLITTER_HANDLE_WIDTH)
        chart_splitter.setStyleSheet(
            """
            QSplitter::handle {
                background: #d7e0ea;
            }
            QSplitter::handle:horizontal {
                margin: 0 2px;
                border-left: 1px solid #cbd5e1;
                border-right: 1px solid #f8fafc;
            }
            QSplitter::handle:vertical {
                margin: 2px 0;
                border-top: 1px solid #cbd5e1;
                border-bottom: 1px solid #f8fafc;
            }
            QSplitter::handle:horizontal:hover,
            QSplitter::handle:vertical:hover {
                background: #bfdbfe;
            }
            QSplitter::handle:pressed {
                background: #93c5fd;
            }
            """
        )
        self._chart_stack_splitter = chart_splitter

        primary_frame = QFrame()
        self._primary_chart_frame = primary_frame
        primary_frame.setStyleSheet(self._chart_frame_style(active=True))
        primary_layout = QVBoxLayout(primary_frame)
        primary_layout.setContentsMargins(0, 0, 0, 0)
        self._native_chart, self._native_chart_view = self._create_native_chart_view(
            title="正在准备主图...",
            allow_draw_clicks=True,
        )
        primary_layout.addWidget(self._native_chart_view, 1)
        chart_splitter.addWidget(primary_frame)

        secondary_frame = QFrame()
        secondary_frame.setStyleSheet(self._chart_frame_style(active=False))
        secondary_layout = QVBoxLayout(secondary_frame)
        secondary_layout.setContentsMargins(0, 0, 0, 0)
        self._secondary_native_chart, self._secondary_native_chart_view = self._create_native_chart_view(
            title="正在准备副图...",
            allow_draw_clicks=False,
        )
        secondary_layout.addWidget(self._secondary_native_chart_view, 1)
        self._secondary_chart_frame = secondary_frame
        chart_splitter.addWidget(secondary_frame)
        chart_splitter.setStretchFactor(0, 5)
        chart_splitter.setStretchFactor(1, 2)

        if isinstance(self._native_chart_view, InteractiveKlineChartView) and isinstance(self._secondary_native_chart_view, InteractiveKlineChartView):
            self._native_chart_view.hoverTimeChanged.connect(self._on_primary_hover_time_changed)
            self._secondary_native_chart_view.hoverTimeChanged.connect(self._on_secondary_hover_time_changed)
            self._native_chart_view.xRangeChanged.connect(self._on_primary_x_range_changed)
            self._secondary_native_chart_view.xRangeChanged.connect(self._on_secondary_x_range_changed)
            self._native_chart_view.chartActivated.connect(lambda: self._set_active_chart_target("primary"))
            self._secondary_native_chart_view.chartActivated.connect(lambda: self._set_active_chart_target("secondary"))
        elif isinstance(self._native_chart_view, InteractiveKlineChartView):
            self._native_chart_view.chartActivated.connect(lambda: self._set_active_chart_target("primary"))

        chart_layout.addWidget(chart_splitter, 1)
        self._apply_secondary_chart_visibility()
        self._refresh_chart_selection_visuals()
        self._page_ready = True

    def _create_native_chart_view(self, *, title: str, allow_draw_clicks: bool) -> tuple[QChart, QChartView]:
        chart = QChart()
        chart.legend().setVisible(False)
        chart.setBackgroundVisible(True)
        chart.setBackgroundRoundness(0)
        chart.setMargins(QMargins(0, 0, 0, 0))
        chart.layout().setContentsMargins(0, 0, 0, 0)
        chart.setBackgroundBrush(QColor(_CHART_BACKGROUND_COLOR))
        chart.setBackgroundPen(QPen(QColor(_CHART_BACKGROUND_COLOR)))
        chart.setPlotAreaBackgroundVisible(True)
        chart.setPlotAreaBackgroundBrush(QColor(_CHART_BACKGROUND_COLOR))
        chart.setPlotAreaBackgroundPen(QPen(QColor(_CHART_BACKGROUND_COLOR)))
        chart.setTitle(title)
        if InteractiveKlineChartView is not None:
            view: QChartView = InteractiveKlineChartView(chart)
            if allow_draw_clicks:
                view.chartPointClicked.connect(self._on_native_chart_clicked)  # type: ignore[attr-defined]
                view.chartPointerPressed.connect(self._on_chart_pointer_pressed)  # type: ignore[attr-defined]
                view.chartPointerMoved.connect(self._on_chart_pointer_moved)  # type: ignore[attr-defined]
                view.chartPointerReleased.connect(self._on_chart_pointer_released)  # type: ignore[attr-defined]
            view.set_draw_mode_enabled(False)  # type: ignore[attr-defined]
        else:
            view = QChartView(chart)
        view.setRenderHint(QPainter.RenderHint.Antialiasing, False)
        view.setStyleSheet(f"background: {_CHART_BACKGROUND_COLOR}; border: none;")
        return chart, view

    def _render_to_native_chart(self, payload: KlineChartPayload) -> None:
        if (
            self._native_chart is None
            or QCandlestickSeries is None
            or QCandlestickSet is None
            or QDateTimeAxis is None
            or QLineSeries is None
            or QValueAxis is None
        ):
            return
        self._render_native_chart_target(
            chart=self._native_chart,
            chart_view=self._native_chart_view,
            payload=payload,
            period=self._period_combo.currentText().strip(),
            title_suffix="主图",
            include_workspace_lines=True,
            is_secondary=False,
        )

    def _render_secondary_chart(self, payload: KlineChartPayload) -> None:
        if (
            self._secondary_native_chart is None
            or self._secondary_native_chart_view is None
            or QCandlestickSeries is None
            or QCandlestickSet is None
            or QDateTimeAxis is None
            or QLineSeries is None
            or QValueAxis is None
        ):
            return
        self._render_native_chart_target(
            chart=self._secondary_native_chart,
            chart_view=self._secondary_native_chart_view,
            payload=payload,
            period=self._secondary_period_combo.currentText().strip(),
            title_suffix="副图",
            include_workspace_lines=False,
            is_secondary=True,
            display_symbol=self._secondary_display_symbol(),
            venue_label=self._secondary_chart_venue_label(),
            chart_note_lines=self._secondary_chart_note_lines(
                payload,
                period=self._secondary_period_combo.currentText().strip(),
            ),
        )

    def _render_native_chart_target(
        self,
        *,
        chart: QChart,
        chart_view: QChartView,
        payload: KlineChartPayload,
        period: str,
        title_suffix: str,
        include_workspace_lines: bool,
        is_secondary: bool,
        display_symbol: str | None = None,
        venue_label: str = "OKX",
        chart_note_lines: list[str] | None = None,
    ) -> None:
        restore_state = None
        if isinstance(chart_view, InteractiveKlineChartView):
            restore_state = chart_view.capture_view_state()

        chart.removeAllSeries()
        for axis in list(chart.axes()):
            chart.removeAxis(axis)

        candles = payload.candles
        if not candles:
            chart.setTitle(f"{title_suffix}暂无K线数据")
            return
        display_times_ms = _build_display_times_ms(candles, period)
        display_step_ms = _display_step_ms(period, candles)
        if len(display_times_ms) >= 2:
            display_step_ms = max(1, int(display_times_ms[1] - display_times_ms[0]))

        candle_series = QCandlestickSeries()
        candle_series.setName("K线")
        candle_series.setIncreasingColor(QColor(_CHART_UP_COLOR))
        candle_series.setDecreasingColor(QColor(_CHART_DOWN_COLOR))
        candle_series.setBodyOutlineVisible(False)
        candle_series.setCapsVisible(False)
        if hasattr(candle_series, "setBodyWidth"):
            candle_series.setBodyWidth(0.72)

        min_price = min(float(item["low"]) for item in candles)
        max_price = max(float(item["high"]) for item in candles)
        overlay_values: list[list[float]] = []
        indicator_series: list[dict[str, Any]] = []
        workspace_lines: list[dict[str, object]] = []
        trend_indicators = payload.trend_indicator if _supports_trend_indicator(period) else []

        for index, item in enumerate(candles):
            candle_set = QCandlestickSet(
                float(item["open"]),
                float(item["high"]),
                float(item["low"]),
                float(item["close"]),
                float(display_times_ms[index]),
            )
            candle_color = QColor(_CHART_UP_COLOR if float(item["close"]) >= float(item["open"]) else _CHART_DOWN_COLOR)
            candle_pen = QPen(candle_color)
            candle_pen.setWidth(1)
            candle_set.setPen(candle_pen)
            candle_set.setBrush(candle_color)
            candle_series.append(candle_set)

        chart.addSeries(candle_series)

        line_specs = (
            ("EMA 15", payload.ema_9, self._ema9.isChecked(), _CHART_EMA15_COLOR, _EMA15_LINE_WIDTH),
            ("SMA 50", payload.ema_21, self._ema21.isChecked(), _CHART_SMA50_COLOR, _SMA50_LINE_WIDTH),
        )
        for label, points, enabled, color, width in line_specs:
            if not enabled:
                continue
            line_series = QLineSeries()
            line_series.setName(label)
            line_pen = QPen(QColor(color))
            line_pen.setWidth(width)
            line_pen.setCosmetic(True)
            line_series.setPen(line_pen)
            values: list[float] = []
            for index, point in enumerate(points):
                value = float(point["value"])
                if index >= len(display_times_ms):
                    break
                line_series.append(float(display_times_ms[index]), value)
                values.append(value)
                min_price = min(min_price, value)
                max_price = max(max_price, value)
            chart.addSeries(line_series)
            overlay_values.append(values)
            indicator_series.append({"label": label, "color": color, "values": values})

        if include_workspace_lines:
            entry = self._workspace_entry()
            line_rules = entry.get("lines", [])
            records = list(line_rules) if isinstance(line_rules, list) else []
            workspace_lines = [dict(item) for item in records if isinstance(item, dict)]
            last_candle_time = int(candles[-1]["time"])
            for index, item in enumerate(records):
                if not isinstance(item, dict):
                    continue
                series = QLineSeries()
                series.setName(str(item.get("label", "线条") or "线条"))
                color = QColor(str(item.get("color", "#1d4ed8") or "#1d4ed8"))
                if not bool(item.get("enabled", True)):
                    color = QColor("#94a3b8")
                if index == self._selected_line_index:
                    color = QColor("#38bdf8")
                pen = QPen(color)
                pen.setWidth(4 if index == self._selected_line_index else 2)
                pen.setCosmetic(True)
                series.setPen(pen)
                line_values: list[float] = []
                for candle_index, candle in enumerate(candles):
                    projected_value = float(line_value_at(item, int(candle["time"])))
                    series.append(float(display_times_ms[candle_index]), projected_value)
                    line_values.append(projected_value)
                    min_price = min(min_price, projected_value)
                    max_price = max(max_price, projected_value)
                future_line_time = max(int(item.get("time_a", 0) or 0), int(item.get("time_b", 0) or 0))
                if future_line_time > last_candle_time:
                    future_display_x = _display_x_for_candle_time(
                        candles,
                        display_times_ms,
                        candle_time=future_line_time,
                        display_step_ms=display_step_ms,
                    )
                    future_value = float(line_value_at(item, future_line_time))
                    series.append(future_display_x, future_value)
                    line_values.append(future_value)
                    min_price = min(min_price, future_value)
                    max_price = max(max_price, future_value)
                chart.addSeries(series)
                overlay_values.append(line_values)

        axis_x = QDateTimeAxis()
        axis_x.setFormat("MM-dd" if _bar_to_ms(period) >= 86_400_000 else "MM-dd HH:mm")
        axis_x.setTickCount(min(8, max(3, len(candles) // 180 + 3)))
        axis_x.setLabelsColor(QColor(_CHART_AXIS_TEXT_COLOR))
        axis_x.setGridLineColor(QColor(_CHART_GRID_COLOR))
        axis_x.setLinePenColor(QColor(_CHART_AXIS_LINE_COLOR))
        axis_x.setGridLineVisible(False)

        axis_y = QValueAxis()
        price_span = max_price - min_price
        padding = max(price_span * 0.08, max_price * 0.002 if max_price else 1.0, 1.0)
        axis_y.setRange(min_price - padding, max_price + padding)
        axis_y.setLabelFormat("%.2f")
        axis_y.setTickCount(8)
        axis_y.setLabelsColor(QColor(_CHART_AXIS_TEXT_COLOR))
        axis_y.setGridLineColor(QColor(_CHART_GRID_COLOR))
        axis_y.setLinePenColor(QColor(_CHART_AXIS_LINE_COLOR))

        chart.addAxis(axis_x, Qt.AlignmentFlag.AlignBottom)
        chart.addAxis(axis_y, Qt.AlignmentFlag.AlignRight)
        for series in chart.series():
            series.attachAxis(axis_x)
            series.attachAxis(axis_y)

        symbol = (display_symbol or self._symbol_input.text().strip().upper()).strip().upper()
        chart.setTitle("")
        if isinstance(chart_view, InteractiveKlineChartView):
            chart_view.set_chart_context(
                axis_x=axis_x,
                axis_y=axis_y,
                candles=candles,
                overlay_values=overlay_values,
                display_times_ms=display_times_ms,
                period=period,
                symbol=symbol,
                venue_label=venue_label,
                indicator_series=indicator_series,
                chart_note_lines=chart_note_lines,
                trend_indicators=trend_indicators,
                signal_markers=self._filter_replay_signal_markers_for_chart(
                    payload.signal_markers,
                    period=period,
                    is_secondary=is_secondary,
                ),
                box_overlays=self._visible_box_overlays(payload),
                workspace_lines=workspace_lines if include_workspace_lines else [],
                selected_workspace_line_index=self._selected_line_index if include_workspace_lines else -1,
                hovered_workspace_line_index=self._hovered_line_index if include_workspace_lines else -1,
                hovered_workspace_drag_mode=self._hovered_line_drag_mode if include_workspace_lines else None,
                restore_state=restore_state,
            )
        else:
            axis_x.setRange(
                QDateTime.fromMSecsSinceEpoch(display_times_ms[0]),
                QDateTime.fromMSecsSinceEpoch(display_times_ms[-1]),
            )

    def _current_workspace_key(self, *, symbol: str | None = None, period: str | None = None) -> str:
        resolved_symbol = (symbol or self._symbol_input.text()).strip().upper()
        resolved_period = (period or self._period_combo.currentText()).strip()
        return build_workspace_key(resolved_symbol, resolved_period)

    def _workspace_entry(self, *, symbol: str | None = None, period: str | None = None) -> dict[str, object]:
        key = self._current_workspace_key(symbol=symbol, period=period)
        entry = normalize_workspace_entry(self._workspace_entries.get(key))
        self._workspace_entries[key] = entry
        return entry

    def _save_workspace_snapshot(self) -> None:
        save_kline_analysis_workspace_entries(self._workspace_entries)

    def _reload_workspace_view(self, symbol: str | None = None, period: str | None = None) -> None:
        entry = self._workspace_entry(symbol=symbol, period=period)
        alerts = entry.get("alerts", {})
        ma_cross = alerts.get("ma_cross", {}) if isinstance(alerts, dict) else {}
        box_breakout = alerts.get("box_breakout", {}) if isinstance(alerts, dict) else {}
        box_realtime = alerts.get("box_realtime", {}) if isinstance(alerts, dict) else {}
        self._ma_cross_alert_check.blockSignals(True)
        self._box_breakout_alert_check.blockSignals(True)
        self._live_box_check.blockSignals(True)
        self._ma_cross_alert_check.setChecked(bool(ma_cross.get("enabled", True)))
        self._box_breakout_alert_check.setChecked(bool(box_breakout.get("enabled", False)))
        self._live_box_check.setChecked(bool(box_realtime.get("enabled", False)))
        self._ma_cross_alert_check.blockSignals(False)
        self._box_breakout_alert_check.blockSignals(False)
        self._live_box_check.blockSignals(False)
        self._backend_hint.setText(
            "当前采用Qt绘图。支持：K线 | 成交量 | 形态显示 | 画线功能 | 实盘箱体 | 历史仓位"
            if self._use_native_chart
            else "当前采用Web版绘图。支持：K线 | 成交量 | 形态显示 | 画线功能 | 实盘箱体 | 历史仓位"
        )
        self._populate_line_table()
        self._refresh_event_log()

    @Slot()
    def _save_workspace_settings(self) -> None:
        entry = self._workspace_entry()
        alerts = entry.get("alerts", {})
        if not isinstance(alerts, dict):
            alerts = {}
            entry["alerts"] = alerts
        ma_cross = alerts.get("ma_cross", {}) if isinstance(alerts.get("ma_cross"), dict) else {}
        box_breakout = alerts.get("box_breakout", {}) if isinstance(alerts.get("box_breakout"), dict) else {}
        box_realtime = alerts.get("box_realtime", {}) if isinstance(alerts.get("box_realtime"), dict) else {}
        ma_cross["enabled"] = self._ma_cross_alert_check.isChecked()
        box_breakout["enabled"] = self._box_breakout_alert_check.isChecked()
        box_realtime["enabled"] = self._live_box_check.isChecked()
        alerts["ma_cross"] = ma_cross
        alerts["box_breakout"] = box_breakout
        alerts["box_realtime"] = box_realtime
        self._save_workspace_snapshot()

    def _populate_line_table(self, selected_index: int | None = None) -> None:
        entry = self._workspace_entry()
        lines = entry.get("lines", [])
        records = list(lines) if isinstance(lines, list) else []
        target_index = self._selected_line_index if selected_index is None else selected_index
        if target_index < 0 or target_index >= len(records):
            target_index = -1
        self._line_table.blockSignals(True)
        self._line_table.setRowCount(len(records))
        for row, item in enumerate(records):
            label = str(item.get("label", "") or "")
            kind = _line_kind_text(str(item.get("kind", "") or ""))
            trigger = _line_trigger_text(str(item.get("trigger", "") or ""))
            action = _line_action_text(str(item.get("action", "") or ""))
            state = _line_state_text(bool(item.get("enabled", True)))
            for column, value in enumerate((label, kind, trigger, action, state)):
                self._line_table.setItem(row, column, QTableWidgetItem(value))
        self._line_table.blockSignals(False)
        if target_index >= 0:
            self._line_table.setCurrentCell(target_index, 0)
            self._apply_line_record_to_form(target_index, records[target_index])
            return
        self._selected_line_index = -1
        self._line_table.clearSelection()
        self._line_enabled_check.setChecked(True)

    def _refresh_event_log(self) -> None:
        entry = self._workspace_entry()
        events = entry.get("events", [])
        records = list(events) if isinstance(events, list) else []
        lines = []
        for item in records[:40]:
            timestamp = str(item.get("event_time", "") or "")
            message = str(item.get("message", "") or "")
            lines.append(f"{timestamp} | {message}")
        self._event_log.setPlainText("\n".join(lines))

    @Slot()
    def _on_line_selected(self) -> None:
        row = self._line_table.currentRow()
        entry = self._workspace_entry()
        lines = entry.get("lines", [])
        records = list(lines) if isinstance(lines, list) else []
        if row < 0 or row >= len(records):
            self._selected_line_index = -1
            if self._pending_payload is not None:
                self._render_to_chart(self._pending_payload)
            return
        self._apply_line_record_to_form(row, records[row])
        if self._pending_payload is not None:
            self._render_to_chart(self._pending_payload)

    def _apply_line_record_to_form(self, row: int, item: dict[str, object]) -> None:
        self._selected_line_index = row
        self._line_label_edit.setText(str(item.get("label", "") or ""))
        self._line_trigger_combo.setCurrentIndex(max(0, self._line_trigger_combo.findData(item.get("trigger"))))
        self._line_action_combo.setCurrentIndex(max(0, self._line_action_combo.findData(item.get("action"))))
        self._line_enabled_check.setChecked(bool(item.get("enabled", True)))

    def _set_draw_tool(self, tool: str) -> None:
        self._draw_tool = tool
        self._pending_line_start = None
        self._clear_line_drag_state(unlock_view=True)
        self._set_hovered_line_interaction()
        if isinstance(self._native_chart_view, InteractiveKlineChartView):
            self._native_chart_view.set_draw_mode_enabled(tool != "none")
            if tool == "none":
                self._native_chart_view.set_preview_line(None)
        label = {
            "none": "光标模式",
            "horizontal": "点击开始绘制水平线",
            "trend": "点击开始绘制趋势线",
        }.get(tool, "光标模式")
        self._set_status(label)

    def _clear_line_drag_state(self, *, unlock_view: bool) -> None:
        self._line_drag_state = None
        if unlock_view and isinstance(self._native_chart_view, InteractiveKlineChartView):
            self._native_chart_view.set_interaction_locked(False)

    def _set_hovered_line_interaction(self, index: int = -1, drag_mode: str | None = None) -> None:
        normalized_mode = str(drag_mode or "").strip().lower() or None
        if self._hovered_line_index == int(index) and self._hovered_line_drag_mode == normalized_mode:
            return
        self._hovered_line_index = int(index)
        self._hovered_line_drag_mode = normalized_mode
        if isinstance(self._native_chart_view, InteractiveKlineChartView):
            self._native_chart_view.set_hovered_workspace_interaction(self._hovered_line_index, self._hovered_line_drag_mode)

    @Slot(float, float)
    def _on_chart_pointer_pressed(self, x_value: float, y_value: float) -> None:
        if self._draw_tool != "none" or self._pending_payload is None:
            return
        resolved = self._resolve_primary_chart_click(x_value=x_value, y_value=y_value)
        if resolved is None:
            return
        candle_time, price = resolved
        hit = self._line_hit_test(candle_time=candle_time, price=price)
        if hit is None:
            return
        index = int(hit["index"])
        self._populate_line_table(selected_index=index)
        self._line_drag_state = {
            "index": index,
            "drag_mode": str(hit["drag_mode"]),
            "anchor_candle_time": candle_time,
            "anchor_price": price,
            "anchor_line": dict(hit["line"]),
        }
        self._set_hovered_line_interaction(index, str(hit["drag_mode"]))
        if isinstance(self._native_chart_view, InteractiveKlineChartView):
            self._native_chart_view.set_interaction_locked(True)
            self._native_chart_view.set_interaction_cursor_mode("dragging")
        if self._pending_payload is not None:
            self._render_to_chart(self._pending_payload)

    @Slot(float, float)
    def _on_chart_pointer_moved(self, x_value: float, y_value: float) -> None:
        if self._draw_tool != "none":
            self._set_hovered_line_interaction()
            resolved_preview = self._resolve_primary_chart_click(x_value=x_value, y_value=y_value)
            if resolved_preview is not None:
                preview_time, preview_price = resolved_preview
                self._update_draw_preview(candle_time=preview_time, price=preview_price)
            return
        if self._line_drag_state is None:
            resolved_hover = self._resolve_primary_chart_click(x_value=x_value, y_value=y_value)
            if resolved_hover is not None:
                hover_time, hover_price = resolved_hover
                hit = self._line_hit_test(candle_time=hover_time, price=hover_price)
                if hit is None:
                    self._set_hovered_line_interaction()
                else:
                    self._set_hovered_line_interaction(int(hit["index"]), str(hit.get("drag_mode", "")))
                if isinstance(self._native_chart_view, InteractiveKlineChartView):
                    if hit is None:
                        self._native_chart_view.set_interaction_cursor_mode("default")
                    elif str(hit.get("drag_mode", "")) in {"endpoint_a", "endpoint_b"}:
                        self._native_chart_view.set_interaction_cursor_mode("endpoint")
                    else:
                        self._native_chart_view.set_interaction_cursor_mode("move")
            else:
                self._set_hovered_line_interaction()
            return
        resolved = self._resolve_primary_chart_click(x_value=x_value, y_value=y_value)
        if resolved is None:
            return
        candle_time, price = resolved
        if self._apply_line_drag_update(candle_time=candle_time, price=price) and self._pending_payload is not None:
            self._render_to_chart(self._pending_payload)

    @Slot(float, float)
    def _on_chart_pointer_released(self, x_value: float, y_value: float) -> None:
        if self._line_drag_state is None:
            return
        resolved = self._resolve_primary_chart_click(x_value=x_value, y_value=y_value)
        if resolved is not None:
            candle_time, price = resolved
            self._apply_line_drag_update(candle_time=candle_time, price=price)
        self._save_workspace_snapshot()
        if self._pending_payload is not None:
            self._render_to_chart(self._pending_payload)
        entry = self._workspace_entry()
        lines = entry.get("lines", [])
        selected_index = int(self._line_drag_state.get("index", -1) or -1)
        label = ""
        if isinstance(lines, list) and 0 <= selected_index < len(lines) and isinstance(lines[selected_index], dict):
            label = str(lines[selected_index].get("label", "") or "")
        self._clear_line_drag_state(unlock_view=True)
        self._set_hovered_line_interaction(selected_index, "move" if selected_index >= 0 else None)
        if label:
            self._set_status(f"已选中标注: {label}")

    @Slot(float, float)
    def _on_native_chart_clicked(self, x_value: float, y_value: float) -> None:
        if self._pending_payload is None:
            return
        resolved = self._resolve_primary_chart_click(x_value=x_value, y_value=y_value)
        if resolved is None:
            return
        candle_time, price = resolved
        if self._draw_tool == "none":
            self._select_nearest_line_from_chart(candle_time=candle_time, price=price)
            return
        if self._draw_tool == "horizontal":
            line = make_line_rule(
                kind="horizontal",
                label=self._next_line_label("horizontal"),
                trigger=str(self._line_trigger_combo.currentData()),
                action=str(self._line_action_combo.currentData()),
                time_a=candle_time,
                price_a=price,
                time_b=candle_time,
                price_b=price,
                enabled=self._line_enabled_check.isChecked() if self._selected_line_index >= 0 else True,
            )
            self._append_line_rule(line)
            if isinstance(self._native_chart_view, InteractiveKlineChartView):
                self._native_chart_view.set_preview_line(None)
            self._set_draw_tool("none")
            return
        if self._pending_line_start is None:
            self._pending_line_start = (candle_time, price)
            self._update_draw_preview(candle_time=candle_time, price=price)
            self._set_status(f"趋势线起点已记录 | {_format_bar_time(candle_time)} | {price:.2f}")
            return
        start_time, start_price = self._pending_line_start
        if start_time == candle_time:
            candle_time += 1
        line = make_line_rule(
            kind="trend",
            label=self._next_line_label("trend"),
            trigger=str(self._line_trigger_combo.currentData()),
            action=str(self._line_action_combo.currentData()),
            time_a=start_time,
            price_a=start_price,
            time_b=candle_time,
            price_b=price,
            enabled=self._line_enabled_check.isChecked() if self._selected_line_index >= 0 else True,
        )
        self._append_line_rule(line)
        self._pending_line_start = None
        if isinstance(self._native_chart_view, InteractiveKlineChartView):
            self._native_chart_view.set_preview_line(None)
        self._set_draw_tool("none")

    def _resolve_primary_chart_click(self, *, x_value: float, y_value: float) -> tuple[int, float] | None:
        if self._pending_payload is None:
            return None
        candles = self._pending_payload.candles
        if not candles:
            return None
        period = self._period_combo.currentText().strip()
        display_times_ms = _build_display_times_ms(candles, period)
        display_step_ms = _display_step_ms(period, candles)
        if len(display_times_ms) >= 2:
            display_step_ms = max(1, int(display_times_ms[1] - display_times_ms[0]))
        candle_time = _resolve_candle_time_from_x_value(
            candles,
            display_times_ms,
            x_value=x_value,
            display_step_ms=display_step_ms,
        )
        return candle_time, float(y_value)

    def _line_hit_test(self, *, candle_time: int, price: float) -> dict[str, object] | None:
        entry = self._workspace_entry()
        lines = entry.get("lines", [])
        records = list(lines) if isinstance(lines, list) else []
        if not records or self._pending_payload is None or not self._pending_payload.candles:
            return None
        period = self._period_combo.currentText().strip()
        display_times_ms = _build_display_times_ms(self._pending_payload.candles, period)
        display_step_ms = _display_step_ms(period, self._pending_payload.candles)
        if len(display_times_ms) >= 2:
            display_step_ms = max(1, int(display_times_ms[1] - display_times_ms[0]))
        body_time_tolerance = _line_time_tolerance_seconds(display_step_ms, bars=6)
        endpoint_time_tolerance = _line_time_tolerance_seconds(display_step_ms, bars=8)
        lows = [float(item["low"]) for item in self._pending_payload.candles]
        highs = [float(item["high"]) for item in self._pending_payload.candles]
        price_span = max(max(highs) - min(lows), 1.0)
        endpoint_price_tolerance = _line_price_tolerance(price_span, price, emphasis="endpoint")
        body_price_tolerance = _line_price_tolerance(price_span, price, emphasis="body")
        candidate_indexes = list(range(len(records)))
        if 0 <= self._selected_line_index < len(records):
            candidate_indexes = [self._selected_line_index, *[index for index in candidate_indexes if index != self._selected_line_index]]
        best_hit: dict[str, object] | None = None
        best_score = float("inf")
        for index in candidate_indexes:
            item = records[index]
            if not isinstance(item, dict):
                continue
            kind = str(item.get("kind", "horizontal") or "horizontal").strip().lower()
            if kind == "trend":
                endpoint_candidates = (
                    ("endpoint_a", int(item.get("time_a", 0) or 0), float(item.get("price_a", 0.0) or 0.0)),
                    ("endpoint_b", int(item.get("time_b", 0) or 0), float(item.get("price_b", 0.0) or 0.0)),
                )
                for drag_mode, endpoint_time, endpoint_price in endpoint_candidates:
                    time_score = abs(endpoint_time - candle_time) / max(float(endpoint_time_tolerance), 1.0)
                    price_score = abs(endpoint_price - price) / max(endpoint_price_tolerance, 1.0)
                    score = max(time_score, price_score)
                    if score <= 1.0 and score < best_score:
                        best_score = score
                        best_hit = {"index": index, "line": item, "drag_mode": drag_mode}
                line_distance = abs(float(line_value_at(item, candle_time)) - float(price))
                lower_time = min(int(item.get("time_a", 0) or 0), int(item.get("time_b", 0) or 0)) - endpoint_time_tolerance
                upper_time = max(int(item.get("time_a", 0) or 0), int(item.get("time_b", 0) or 0)) + endpoint_time_tolerance
                if lower_time <= candle_time <= upper_time:
                    score = line_distance / max(body_price_tolerance, 1.0)
                    if score <= 1.0 and score < best_score:
                        best_score = score
                        best_hit = {"index": index, "line": item, "drag_mode": "move"}
                continue
            line_distance = abs(float(line_value_at(item, candle_time)) - float(price))
            score = line_distance / max(body_price_tolerance, 1.0)
            if score <= 1.0 and score < best_score:
                best_score = score
                best_hit = {"index": index, "line": item, "drag_mode": "move"}
        return best_hit

    def _apply_line_drag_update(self, *, candle_time: int, price: float) -> bool:
        state = self._line_drag_state
        if state is None:
            return False
        entry = self._workspace_entry()
        lines = entry.get("lines")
        index = int(state.get("index", -1) or -1)
        if not isinstance(lines, list) or index < 0 or index >= len(lines) or not isinstance(lines[index], dict):
            return False
        updated = _apply_drag_to_line_rule(
            lines[index],
            drag_mode=str(state.get("drag_mode", "move") or "move"),
            candle_time=candle_time,
            price=price,
            anchor_line=dict(state.get("anchor_line", {})) if isinstance(state.get("anchor_line"), dict) else None,
            anchor_candle_time=int(state.get("anchor_candle_time", candle_time) or candle_time),
            anchor_price=float(state.get("anchor_price", price) or price),
        )
        lines[index] = updated
        self._selected_line_index = index
        self._apply_line_record_to_form(index, updated)
        return True

    def _update_draw_preview(self, *, candle_time: int, price: float) -> None:
        if not isinstance(self._native_chart_view, InteractiveKlineChartView):
            return
        if self._draw_tool == "horizontal":
            self._native_chart_view.set_preview_line(
                {
                    "kind": "horizontal",
                    "time_a": candle_time,
                    "price_a": price,
                    "time_b": candle_time,
                    "price_b": price,
                }
            )
            return
        if self._draw_tool == "trend" and self._pending_line_start is not None:
            start_time, start_price = self._pending_line_start
            line_time_a, line_price_a, line_time_b, line_price_b = _ordered_trend_endpoints(
                start_time,
                start_price,
                candle_time,
                price,
            )
            self._native_chart_view.set_preview_line(
                {
                    "kind": "trend",
                    "time_a": line_time_a,
                    "price_a": line_price_a,
                    "time_b": line_time_b,
                    "price_b": line_price_b,
                }
            )
            return
        self._native_chart_view.set_preview_line(None)

    def _select_nearest_line_from_chart(self, *, candle_time: int, price: float) -> None:
        entry = self._workspace_entry()
        lines = entry.get("lines", [])
        records = list(lines) if isinstance(lines, list) else []
        if not records:
            return
        nearest_index = self._nearest_line_index(candle_time=candle_time, price=price, records=records)
        if nearest_index < 0:
            self._selected_line_index = -1
            self._line_table.clearSelection()
            if self._pending_payload is not None:
                self._render_to_chart(self._pending_payload)
            return
        self._populate_line_table(selected_index=nearest_index)
        if self._pending_payload is not None:
            self._render_to_chart(self._pending_payload)
        label = str(records[nearest_index].get("label", "") or "")
        self._set_status(f"选择最近线条：{label}")

    def _nearest_line_index(self, *, candle_time: int, price: float, records: list[object]) -> int:
        if self._pending_payload is None or not self._pending_payload.candles:
            return -1
        lows = [float(item["low"]) for item in self._pending_payload.candles]
        highs = [float(item["high"]) for item in self._pending_payload.candles]
        price_span = max(max(highs) - min(lows), 1.0)
        tolerance = max(price_span * 0.012, abs(price) * 0.0015, 1.0)
        nearest_index = -1
        nearest_distance = float("inf")
        for index, item in enumerate(records):
            if not isinstance(item, dict):
                continue
            projected_price = float(line_value_at(item, candle_time))
            distance = abs(projected_price - float(price))
            if distance < nearest_distance:
                nearest_distance = distance
                nearest_index = index
        return nearest_index if nearest_distance <= tolerance else -1

    def _next_line_label(self, kind: str) -> str:
        explicit = self._line_label_edit.text().strip()
        if explicit:
            return explicit
        entry = self._workspace_entry()
        lines = entry.get("lines", [])
        count = len(lines) + 1 if isinstance(lines, list) else 1
        prefix = "趋势线" if kind == "trend" else "水平线"
        return f"{prefix}-{count:02d}"

    def _append_line_rule(self, line: dict[str, object]) -> None:
        entry = self._workspace_entry()
        lines = entry.get("lines")
        if not isinstance(lines, list):
            lines = []
            entry["lines"] = lines
        lines.append(line)
        new_index = len(lines) - 1
        self._selected_line_index = new_index
        self._save_workspace_snapshot()
        self._populate_line_table(selected_index=new_index)
        if self._pending_payload is not None:
            self._render_to_chart(self._pending_payload)
        self._set_status(f"已添加画线：{line.get('label', '')}")

    @Slot()
    def _update_selected_line(self) -> None:
        if self._selected_line_index < 0:
            return
        entry = self._workspace_entry()
        lines = entry.get("lines")
        if not isinstance(lines, list) or self._selected_line_index >= len(lines):
            return
        item = dict(lines[self._selected_line_index])
        item["label"] = self._line_label_edit.text().strip() or str(item.get("label", "") or "")
        item["trigger"] = str(self._line_trigger_combo.currentData())
        item["action"] = str(self._line_action_combo.currentData())
        item["enabled"] = self._line_enabled_check.isChecked()
        lines[self._selected_line_index] = item
        selected_index = self._selected_line_index
        self._save_workspace_snapshot()
        self._populate_line_table(selected_index=selected_index)
        if self._pending_payload is not None:
            self._render_to_chart(self._pending_payload)
        self._set_status(f"已更新画线：{item.get('label', '')}")

    @Slot()
    def _delete_selected_line(self) -> None:
        if self._selected_line_index < 0:
            return
        entry = self._workspace_entry()
        lines = entry.get("lines")
        if not isinstance(lines, list) or self._selected_line_index >= len(lines):
            return
        deleted_index = self._selected_line_index
        del lines[deleted_index]
        next_index = min(deleted_index, len(lines) - 1)
        self._selected_line_index = next_index
        self._save_workspace_snapshot()
        self._populate_line_table(selected_index=next_index)
        if self._pending_payload is not None:
            self._render_to_chart(self._pending_payload)
        self._set_status("删除线条成功")

    def _apply_alert_snapshot(self, snapshot: KlineAlertSnapshot | None) -> None:
        if snapshot is None:
            return
        updated_entry = normalize_workspace_entry(snapshot.workspace_entry)
        new_events = list(snapshot.new_events)
        structure = dict(snapshot.structure)
        self._workspace_entries[self._current_workspace_key()] = updated_entry
        self._save_workspace_snapshot()
        self._structure_hint.setText(str(structure.get("note", "") or ""))
        self._refresh_event_log()
        self._populate_line_table()
        if new_events:
            latest = str(new_events[0].get("message", "") or "")
            self._set_status(f"{self._status.text()} | 事件：{latest}")

    def _set_status(self, text: str) -> None:
        self._status.setText(text)

    def _update_refresh_hint(self) -> None:
        if not self._auto_refresh_btn.isChecked():
            return
        if self._pending_payload is None:
            return
        last_ts = self._pending_payload.candles[-1]["time"] if self._pending_payload.candles else None
        if last_ts is None:
            return
        dt = datetime.fromtimestamp(last_ts).strftime("%Y-%m-%d %H:%M:%S")
        self._set_status(f"{self._status.text()} | 最新K线：{dt}")

    def _chart_html(self) -> str:
        return """
            <!doctype html>
            <html lang="en">
            <head>
              <meta charset="UTF-8" />
              <meta name="viewport" content="width=device-width, initial-scale=1.0" />
              <title>K线图表</title>
              <style>
                html, body {
                    margin: 0;
                    width: 100%;
                    height: 100%;
                    overflow: hidden;
                    background: #0b0f14;
                    font-family: -apple-system, Segoe UI, Roboto, Arial, sans-serif;
                }
                #chart {
                    position: absolute;
                    inset: 0;
                    padding: 0;
                }
                #crosshairTooltip {
                    position: absolute;
                    z-index: 1;
                    left: 12px;
                    top: 12px;
                    background: rgba(11, 15, 20, 0.92);
                    color: #f8fafc;
                    padding: 7px 9px;
                    border: 1px solid #263140;
                    border-radius: 6px;
                    font-size: 11px;
                    line-height: 1.45;
                    pointer-events: none;
                    white-space: pre;
                    opacity: 0;
                    transition: opacity 120ms linear;
                    max-width: 320px;
                }
                #fallback {
                    position: absolute;
                    inset: 0;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                    color: #64748b;
                    font-size: 12px;
                }
              </style>
              <script src="https://unpkg.com/lightweight-charts@4.2.1/dist/lightweight-charts.standalone.production.js"></script>
            </head>
            <body>
              <div id="chart"></div>
              <div id="crosshairTooltip"></div>
              <div id="fallback" style="display:none;">图表脚本加载失败</div>
              <script>
                const chartEl = document.getElementById('chart');
                const fallback = document.getElementById('fallback');
                const tooltip = document.getElementById('crosshairTooltip');

                let chart = null;
                let candlestickSeries = null;
                const lineSeries = { ema9: null, ema21: null };
                let trendSeries = null;
                let volumeSeries = null;
                let trendByTime = {};
                let signalByTime = {};

                function toTooltipText(time, payload, trendPayload) {
                  if (!time || !payload) {
                    return '';
                  }
                  const o = payload.open;
                  const h = payload.high;
                  const l = payload.low;
                  const c = payload.close;
                  const v = payload.value;
                  const trendText = trendPayload
                    ? `${trendPayload.label || '中性'} (${trendPayload.state || 'neutral'})`
                    : '中性';
                  const date = new Date(time * 1000);
                  const dateText = `${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')} ${String(date.getHours()).padStart(2, '0')}:${String(date.getMinutes()).padStart(2, '0')}`;
                  const trendTitle = trendPayload?.title || trendTitleForPeriod(payload?.period);
                  return `时间 ${dateText}\n开 ${formatNumber(o)}  高 ${formatNumber(h)}\n低 ${formatNumber(l)}  收 ${formatNumber(c)}\n量 ${formatNumber(v)}\n${trendTitle} ${trendText}`;
                }

                function trendTitleForPeriod(period) {
                  const normalized = String(period || '').trim().toUpperCase();
                  if (normalized === '1H') return '1小时趋势';
                  if (normalized === '4H') return '4小时趋势';
                  if (normalized === '1D') return '日线趋势';
                  return `${normalized || '-'}趋势`;
                }

                function formatNumber(value) {
                  if (value === undefined || value === null || Number.isNaN(value)) {
                    return '-';
                  }
                  return Number(value).toLocaleString('en-US', {
                    minimumFractionDigits: 2,
                    maximumFractionDigits: 6,
                  });
                }

                function ensureChart() {
                  if (chart !== null) return;
                  if (!window.LightweightCharts || !window.LightweightCharts.createChart) {
                    fallback.style.display = 'flex';
                    return;
                  }
                    chart = LightweightCharts.createChart(chartEl, {
                    width: chartEl.clientWidth,
                    height: chartEl.clientHeight,
                    layout: {
                      background: { color: '#0b0f14' },
                      textColor: '#8b95a5',
                    },
                    grid: {
                      vertLines: { visible: false },
                      horzLines: { color: '#1f2937' },
                    },
                    timeScale: {
                      timeVisible: true,
                      secondsVisible: false,
                      rightOffset: 6,
                      barSpacing: 8,
                      borderColor: '#18202b',
                    },
                    crosshair: {
                      mode: 1,
                      vertLine: { color: '#6b7280', style: 2 },
                      horzLine: { color: '#6b7280', style: 2 },
                    },
                    rightPriceScale: {
                      borderColor: '#18202b',
                    },
                  });
                  candlestickSeries = chart.addCandlestickSeries({
                    upColor: '#22c55e',
                    downColor: '#e04f84',
                    wickUpColor: '#22c55e',
                    wickDownColor: '#e04f84',
                    borderVisible: false,
                  });
                  volumeSeries = chart.addHistogramSeries({
                    priceScaleId: '',
                    color: '#22c55e',
                    priceLineVisible: false,
                    lastValueVisible: false,
                    priceFormat: {
                      type: 'volume',
                    },
                  });
                  chart.priceScale('').applyOptions({
                    scaleMargins: {
                      top: 0.82,
                      bottom: 0.0,
                    },
                  });
                  initCrosshair();
                  window.addEventListener('resize', handleResize);
                  handleResize();
                }

                function handleResize() {
                  if (!chart) return;
                  const width = chartEl.clientWidth;
                  const height = chartEl.clientHeight;
                  chart.resize(width, height);
                }

                function setCrosshairTooltip(point, isVisible, payload) {
                  if (!isVisible || !tooltip) {
                    if (tooltip) tooltip.style.opacity = '0';
                    return;
                  }
                  tooltip.textContent = payload;
                  const x = Math.max(12, point.x + 12);
                  const y = Math.max(12, point.y + 12);
                  tooltip.style.left = `${Math.min(x, Math.max(12, chartEl.clientWidth - 210))}px`;
                  tooltip.style.top = `${Math.min(y, Math.max(12, chartEl.clientHeight - 120))}px`;
                  tooltip.style.opacity = '1';
                }

                function syncLineSeries(name, points, enabled, options) {
                  const existing = lineSeries[name];
                  if (enabled) {
                    if (!existing) {
                      lineSeries[name] = chart.addLineSeries(options);
                    }
                    lineSeries[name].setData(points || []);
                    return;
                  }
                  if (existing) {
                    chart.removeSeries(existing);
                    lineSeries[name] = null;
                  }
                }

                function syncTrendSeries(points, enabled) {
                  if (trendSeries) {
                    chart.removeSeries(trendSeries);
                    trendSeries = null;
                  }
                  trendByTime = {};
                  if (!enabled || !Array.isArray(points) || points.length === 0) {
                    return;
                  }
                  trendSeries = chart.addHistogramSeries({
                    title: trendTitleForPeriod(window.__chartPeriod || ''),
                    color: '#94a3b8',
                    priceScaleId: 'trend',
                    priceLineVisible: false,
                    lastValueVisible: false,
                  });
                  chart.priceScale('trend').applyOptions({
                    scaleMargins: {
                      top: 0.76,
                      bottom: 0.18,
                    },
                  });
                  const trendPoints = [];
                  const nextTrendByTime = {};
                  for (const point of points) {
                    const pointTime = point?.time;
                    const value = point?.value;
                    if (pointTime === undefined || pointTime === null || !Number.isFinite(Number(value))) {
                      continue;
                    }
                    const color = typeof point?.color === 'string' ? point.color : '#94a3b8';
                    const normalizedValue = Number(value);
                    trendPoints.push({ time: pointTime, value: normalizedValue, color });
                    nextTrendByTime[String(pointTime)] = {
                      label: point?.label || '中性',
                      state: point?.state || 'neutral',
                      title: point?.title || trendTitleForPeriod(window.__chartPeriod || ''),
                    };
                  }
                  trendByTime = nextTrendByTime;
                  trendSeries.setData(trendPoints);
                }

                function applyChartData(payload) {
                  try {
                    ensureChart();
                    if (!chart || !candlestickSeries) {
                      return;
                    }
                    const safePayload = payload || {};
                    window.__chartPeriod = safePayload.period || '';
                    const candles = Array.isArray(safePayload.candles) ? safePayload.candles : [];
                    candlestickSeries.setData(candles);
                    const signalMarkers = Array.isArray(safePayload.signals)
                      ? safePayload.signals.map((item) => ({
                          time: Number(item?.time) || 0,
                          position: item?.direction === 'long' ? 'belowBar' : 'aboveBar',
                          color: typeof item?.color === 'string' ? item.color : '#38bdf8',
                          shape: item?.direction === 'long' ? 'arrowUp' : 'arrowDown',
                          text: item?.label || '',
                        })).filter((item) => item.time > 0)
                      : [];
                    signalByTime = {};
                    for (const item of Array.isArray(safePayload.signals) ? safePayload.signals : []) {
                      const key = String(Number(item?.time) || 0);
                      if (key === '0') continue;
                      if (!signalByTime[key]) signalByTime[key] = [];
                      signalByTime[key].push(item);
                    }
                    if (typeof candlestickSeries.setMarkers === 'function') {
                      candlestickSeries.setMarkers(signalMarkers);
                    }
                    const volumePoints = candles.map((candle) => {
                      const color = candle?.close >= candle?.open
                        ? 'rgba(34, 197, 94, 0.85)'
                        : 'rgba(224, 79, 132, 0.85)';
                      return { time: Number(candle?.time) || 0, value: Number(candle?.volume) || 0, color };
                    });
                    if (volumeSeries) {
                      volumeSeries.setData(volumePoints);
                    }
                    syncTrendSeries(
                      Array.isArray(safePayload.trend) ? safePayload.trend : [],
                      safePayload.period === '1D'
                    );
                    const emaData = safePayload.ema || {};
                    syncLineSeries(
                      'ema9',
                      emaData.ema9,
                      safePayload.show?.ema9,
                      { color: '#ff4d6d', lineWidth: 2, title: 'EMA 15', priceLineVisible: false, lastValueVisible: false }
                    );
                    syncLineSeries(
                      'ema21',
                      emaData.ema21,
                      safePayload.show?.ema21,
                      { color: '#58c66d', lineWidth: 3, title: 'SMA 50', priceLineVisible: false, lastValueVisible: false }
                    );
                    chart.timeScale().fitContent();
                  } catch (error) {
                    if (window.console && window.console.error) {
                      window.console.error('[applyChartData]', error);
                    }
                    handleChartWarning(`璧板娍鍥炬覆鏌撳紓甯革細${String(error)}`);
                  }
                }

                function initCrosshair() {
                  if (!chart) return;
                  chart.subscribeCrosshairMove((param) => {
                    if (
                      !param ||
                      !param.point ||
                      !param.time ||
                      !param.seriesData
                    ) {
                      setCrosshairTooltip({ x: 0, y: 0 }, false, '');
                      return;
                    }
                    const candleData = param.seriesData.get(candlestickSeries);
                    if (!candleData) {
                      setCrosshairTooltip({ x: 0, y: 0 }, false, '');
                      return;
                    }
                    const trendPayload = trendByTime[String(param.time)] || null;
                    const text = toTooltipText(param.time, candleData, trendPayload);
                    setCrosshairTooltip(param.point, true, text);
                  });
                  chart.subscribeClick(() => {
                    setCrosshairTooltip({ x: 0, y: 0 }, false, '');
                  });
                }

                function resetChartView() {
                  if (!chart) return;
                  chart.timeScale().fitContent();
                }

                function handleChartWarning(message) {
                  if (!message) return;
                  if (!chart && fallback) {
                    fallback.textContent = message;
                    fallback.style.display = 'flex';
                  }
                }
              </script>
            </body>
            </html>
        """
