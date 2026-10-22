from __future__ import annotations

import json
import math
import os
import random
import re
import sys
import time
import copy
from bisect import bisect_right
from dataclasses import dataclass, field, replace
from datetime import datetime
from decimal import Decimal, InvalidOperation
from statistics import median
from typing import Any, Callable
from uuid import uuid4

from PySide6.QtCore import QDateTime, QMargins, QObject, QPointF, QRectF, QTimer, Qt, QThread, Signal, Slot
from PySide6.QtGui import QAction, QColor, QGuiApplication, QPainter, QPen, QPolygonF
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
    QDialog,
    QDialogButtonBox,
    QDoubleSpinBox,
    QFormLayout,
    QFrame,
    QGridLayout,
    QHeaderView,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMenu,
    QMessageBox,
    QPushButton,
    QScrollArea,
    QSizePolicy,
    QSplitter,
    QTabBar,
    QSpinBox,
    QTabWidget,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QVBoxLayout,
    QWidget,
    QWidgetAction,
)

from okx_quant.candle_cache import load_candle_cache, merge_candles, replace_candle_cache, save_candle_cache
from okx_quant.analysis import ChannelDetectionConfig
from okx_quant.analysis.box_detector import BoxDetectionConfig, detect_boxes
from okx_quant.strategy_live_chart import build_auto_channel_live_chart_snapshot
from okx_quant.deribit_client import DeribitRestClient, DeribitVolatilityCandle
from okx_quant.models import Candle, EmailNotificationConfig
from okx_quant.notifications import EmailNotifier
from okx_quant.okx_candle_ws import CandleStreamKey
from okx_quant.deribit_volatility_ui import (
    DERIBIT_BASE_HOURLY_RESOLUTION,
    DERIBIT_FULL_HISTORY_START_TS,
    OKX_SPOT_SYMBOLS,
    _aggregate_candles_to_resolution,
    _aggregate_price_candles_to_resolution,
    _hourly_fetch_start_ts,
    _hourly_history_limit,
    _load_valid_cached_deribit_hourly_series,
    _merge_deribit_candles,
    _merge_price_candles,
    _to_average_price_candles,
    _to_average_volatility_candles,
)
from okx_quant.kline_rr_execution import RRTradeExecutionService
from okx_quant.kline_rr_trade import RRTradeLedgerEntry, RRTradePlan, build_rr_trade_plan
from okx_quant.area_shape_trade import AREA_DYNAMIC_TAKE_PROFIT_R, AreaShapeConfig, AreaShapePlan
from okx_quant.analysis.auto_zones import find_auto_zone_pair
from okx_quant.kline_trade_recommendations import KlineTradeIdea, build_kline_trade_idea
from roll_terminal_qt.area_shape_dialog import AreaShapeConfigDialog
from roll_terminal_qt.kline_trade_recommendation_dialog import KlineTradeRecommendationDialog
from okx_quant.okx_client import OkxFillHistoryItem, OkxPosition, OkxPositionHistoryItem, OkxRestClient, infer_inst_type
from okx_quant.engine import _dynamic_two_taker_fee_offset_live
from okx_quant.pricing import format_decimal, format_decimal_by_increment, format_decimal_fixed, snap_to_increment
from okx_quant.ui_shell import _format_history_size_amount, _history_display_amount
from okx_quant.persistence import (
    deribit_volatility_cache_file_path,
    load_kline_analysis_workspace_entries,
    load_kline_rr_trade_ledger_snapshot,
    load_notification_snapshot,
    save_kline_analysis_workspace_entries,
    save_kline_rr_trade_ledger_snapshot,
)
from okx_quant.arbitrage.arbitrage_executor import _build_strategy_config
from okx_quant.signal_replay_engine import SignalReplayConfig, build_signal_replay_dataset
from okx_quant.shape_salience import formation_range_rank, pattern_quality_score, significant_range_core
from okx_quant.strategy_catalog import STRATEGY_DYNAMIC_LONG_ID, STRATEGY_EMA55_SLOPE_SHORT_ID
from okx_quant.strategy_symbol_defaults import get_strategy_symbol_parameter_defaults
from roll_terminal_qt.line_trading_core import (
    compute_rr_target,
    decimal_to_text,
    drag_rr_annotation,
    rr_annotation_from_payload,
    rr_annotation_to_payload,
)
from roll_terminal_qt.kline_box_rules import AUTO_BOX_MAX_CANDIDATES, is_auto_box_candidate_valid
from roll_terminal_qt.history_service import load_local_position_history, merge_position_history_cache
from roll_terminal_qt.perf_metrics import measure_ui_step
from okx_quant.log_utils import append_log_line
from roll_terminal_qt.kline_alerts import (
    build_workspace_key,
    evaluate_workspace_alerts,
    line_value_at,
    make_line_rule,
    normalize_workspace_entry,
)
from roll_terminal_qt.kline_account_drawer import KlineAccountDrawer
from roll_terminal_qt.order_service import OrderStatusView
from roll_terminal_qt.shared_order_store import get_shared_order_store
from roll_terminal_qt.profile_access import ensure_profile_unlocked, load_profile_snapshots
from roll_terminal_qt.runtime import load_runtime, profile_names
from roll_terminal_qt.workspace_shell import LocalTaskCount, merge_local_task_counts


_INITIAL_WINDOW_LOAD_DELAY_MS = 80
_NATIVE_BOOTSTRAP_RENDER_BARS = 360
_NATIVE_BOOTSTRAP_RENDER_DELAY_MS = 90
_AUTO_REFRESH_DEFAULT_ENABLED = True
# Keep a visible tail after the latest candle.  Four-chart layouts make the
# previous 24-bar gap look cramped, especially on the 1W/1D panels.
_NATIVE_RIGHT_PADDING_BARS = 32
_NATIVE_MAX_RIGHT_PADDING_BARS = 5_000
# Recent view keeps the 1H chart at about 250 candles.  Higher timeframes
# need fewer candles to keep their structure readable in linked layouts.
_RECENT_VIEW_BARS = 250
_LINKED_MIN_VISIBLE_BARS = {
    "1W": 120,
    "1D": 180,
    "4H": 220,
    "1H": 250,
}
_KLINE_PAYLOAD_CACHE_LIMIT = 8
_KLINE_SPLITTER_LEFT_RATIO = 0.11
_VOLUME_OVERLAY_HEIGHT_RATIO = 0.18
_ATR_OVERLAY_HEIGHT_RATIO = 0.13
_EMA15_LINE_WIDTH = 2
_SMA50_LINE_WIDTH = 3
_SECONDARY_CHART_TOP_RATIO = 0.31
_SECONDARY_CHART_SIDE_RATIO = 0.56
_SECONDARY_CHART_SPLITTER_HANDLE_WIDTH = 10
_HEADER_SYMBOL_INPUT_MIN_WIDTH = 160
_HEADER_SYMBOL_INPUT_MAX_WIDTH = 190
_RR_BOX_WIDTH_BARS = 6
_RR_MULTIPLE_STEP = Decimal("0.1")
_RR_DRAG_ACTIVATION_DISTANCE_PX = 6.0
_BOX_HISTORY_SCAN_LIMIT = 240
_BOX_HISTORY_MAX_SEGMENTS = 8
# The chart layer renders only the currently active channel.  Searching the
# full 1200-bar history is prohibitively expensive and does not improve the
# current structure shown to the user.
_AUTO_CHANNEL_SCAN_LIMIT = 240
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
_CHART_EMA21_COLOR = "#38bdf8"
_CHART_EMA55_COLOR = "#a78bfa"
_CHART_BEST_INDICATOR_COLORS = ("#f59e0b", "#8b5cf6", "#38bdf8", "#ec4899")
_HISTORY_TRADE_OPEN_COLOR = "#38bdf8"
_HISTORY_TRADE_CLOSE_COLOR = "#f97316"
_HISTORY_TRADE_LONG_COLOR = "#22d3ee"
_HISTORY_TRADE_SHORT_COLOR = "#c084fc"
_HISTORY_TRADE_PROFIT_COLOR = "#22c55e"
_HISTORY_TRADE_LOSS_COLOR = "#ef4444"
_HISTORY_TRADE_FLAT_COLOR = "#94a3b8"
_CHART_CROSSHAIR_COLOR = "#6b7280"
_REPLAY_SIGNAL_NEAR_MA_MAX_PCT = 0.006


def _build_kline_line_email_notifier() -> EmailNotifier | None:
    snapshot = load_notification_snapshot()
    recipients = tuple(
        item.strip()
        for item in re.split(r"[,\n;]+", str(snapshot.get("recipient_emails", "")))
        if item.strip()
    )
    notifier = EmailNotifier(
        EmailNotificationConfig(
            enabled=bool(snapshot.get("enabled", False)),
            smtp_host=str(snapshot.get("smtp_host", "")),
            smtp_port=int(snapshot.get("smtp_port", 465)),
            smtp_username=str(snapshot.get("smtp_username", "")),
            smtp_password=str(snapshot.get("smtp_password", "")),
            sender_email=str(snapshot.get("sender_email", "")),
            recipient_emails=recipients,
            use_ssl=bool(snapshot.get("use_ssl", True)),
            notify_trade_fills=bool(snapshot.get("notify_trade_fills", True)),
            notify_signals=bool(snapshot.get("notify_signals", True)),
            notify_errors=bool(snapshot.get("notify_errors", True)),
        )
    )
    return notifier if notifier.signal_notifications_enabled else None


def _deliver_line_alert_emails(
    *,
    workspace_entry: dict[str, object],
    events: list[dict[str, object]],
    symbol: str,
    period: str,
    notifier: EmailNotifier | None,
) -> int:
    if notifier is None or not notifier.signal_notifications_enabled:
        return 0
    lines = workspace_entry.get("lines")
    if not isinstance(lines, list):
        return 0
    lines_by_id = {
        str(line.get("id", "") or "").strip(): line
        for line in lines
        if isinstance(line, dict) and str(line.get("id", "") or "").strip()
    }
    sent_count = 0
    for event in events:
        if str(event.get("kind", "") or "") != "line_alert":
            continue
        if str(event.get("trade_action", "") or "").strip().lower() != "notify":
            continue
        line = lines_by_id.get(str(event.get("line_id", "") or "").strip())
        if not isinstance(line, dict) or not bool(line.get("email_enabled", False)):
            continue
        delivery_mode = str(line.get("email_delivery_mode", "once") or "once").strip().lower()
        if delivery_mode == "once" and bool(line.get("email_sent_once", False)):
            continue
        label = str(line.get("label", "画线预警") or "画线预警").strip()
        trigger = _line_trigger_text(str(line.get("trigger", "") or ""))
        direction = _line_trigger_text(str(event.get("direction", "") or ""))
        candle_time = int(event.get("candle_time", 0) or 0)
        candle_label = _format_bar_time(candle_time) if candle_time > 0 else "-"
        subject = f"[QQOKX] K线画线提醒 | {symbol} | {period} | {label}"
        body = "\n".join(
            (
                f"交易对：{symbol}",
                f"周期：{period}",
                f"线条：{label}",
                f"预警条件：{trigger}",
                f"触发方向：{direction}",
                f"K线时间：{candle_label}",
                f"事件：{str(event.get('message', '') or '').strip()}",
            )
        )
        notifier.notify_async(subject, body)
        sent_count += 1
        if delivery_mode == "once":
            line["email_sent_once"] = True
    return sent_count
_REPLAY_SIGNAL_LONG_COLOR = "#38bdf8"
_REPLAY_SIGNAL_SHORT_COLOR = "#f97316"
_PRIMARY_PERIOD_BUTTON_WIDTH = 48
_PRIMARY_PERIOD_BUTTON_HEIGHT = 28
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
KLINE_SYMBOL_OPTIONS = (
    "BTC-USDT-SWAP",
    "ETH-USDT-SWAP",
    "SOL-USDT-SWAP",
    "DOGE-USDT-SWAP",
    "BNB-USDT-SWAP",
    "OKB-USDT-SWAP",
    "ETH-BTC",
    "SOL-BTC",
)
VOLATILITY_TAB_OPTIONS = (
    ("BTC-DVOL", "__DVOL_BTC__"),
    ("ETH-DVOL", "__DVOL_ETH__"),
)
_VOLATILITY_CURRENCY_BY_SYMBOL = {
    "BTC-USDT-SWAP": "BTC",
    "ETH-USDT-SWAP": "ETH",
    "ETH-BTC": "ETH",
}
# Keep user-visible labels in plain UTF-8 literals. Do not replace them with
# mojibake fallbacks or ASCII placeholders.
_REPLAY_SIGNAL_LABELS = {
    "big_bullish": "大阳线",
    "big_bearish": "大阴线",
    "long_upper_shadow": "长上影",
    "long_lower_shadow": "长下影",
    "false_breakdown": "双线向上反转",
    "false_breakout": "双线向下反转",
    "double_reversal_up": "双线向上反转",
    "double_reversal_down": "双线向下反转",
    "inside_bar": "孕线",
    "top_fractal": "顶分型",
    "bottom_fractal": "底分型",
}


def _debug_log(message: str) -> None:
    stream = getattr(sys, "stdout", None)
    if stream is None:
        return
    try:
        stream.write(f"{message}\n")
        stream.flush()
    except Exception:
        return


def _volatility_currency_for_symbol(symbol: str) -> str | None:
    return _VOLATILITY_CURRENCY_BY_SYMBOL.get(symbol.strip().upper())


def _load_deribit_volatility_cache_payload() -> dict[str, Any]:
    path = deribit_volatility_cache_file_path()
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _save_deribit_volatility_cache_payload(payload: dict[str, Any]) -> None:
    path = deribit_volatility_cache_file_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")


def _deribit_hourly_cache_key(currency: str) -> str:
    return f"{currency}|hourly_base"


def _load_cached_deribit_hourly_series(currency: str) -> tuple[str, list[DeribitVolatilityCandle], list[Candle], datetime] | None:
    return _load_valid_cached_deribit_hourly_series(currency)


def _save_cached_deribit_hourly_series(
    currency: str,
    *,
    spot_inst_id: str,
    volatility_candles: list[DeribitVolatilityCandle],
    spot_candles: list[Candle],
    fetched_at: datetime,
) -> None:
    payload = _load_deribit_volatility_cache_payload()
    payload[_deribit_hourly_cache_key(currency)] = {
        "spot_inst_id": spot_inst_id,
        "fetched_at": fetched_at.isoformat(),
        "volatility_hourly": [
            {
                "ts": candle.ts,
                "open": str(candle.open),
                "high": str(candle.high),
                "low": str(candle.low),
                "close": str(candle.close),
            }
            for candle in volatility_candles
        ],
        "spot_hourly": [
            {
                "ts": candle.ts,
                "open": str(candle.open),
                "high": str(candle.high),
                "low": str(candle.low),
                "close": str(candle.close),
                "volume": str(candle.volume),
                "confirmed": candle.confirmed,
            }
            for candle in spot_candles
        ],
    }
    _save_deribit_volatility_cache_payload(payload)
_DAILY_TREND_NEUTRAL_BIAS = 0.008
_DAILY_TREND_STRONG_BIAS = 0.015
_DAILY_TREND_STRONG_SLOPE = 0.006
_DAILY_TREND_BAND_COLOR = {
    "strong_bull": "#22c55e",
    "weak_bull": "#4ade80",
    "neutral": "#94a3b8",
    "weak_bear": "#f87171",
    "strong_bear": "#ef4444",
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
    ("1W", "1W"),
)


def _to_ms_seconds(ts: int) -> int:
    return int(ts // 1000)


def _to_float(value: Decimal) -> float:
    return float(value)


def _format_bar_time(ts: int) -> str:
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")


def _format_chart_time(ts: int) -> str:
    moment = datetime.fromtimestamp(int(ts))
    weekday = ("周一", "周二", "周三", "周四", "周五", "周六", "周日")[moment.weekday()]
    return f"{moment:%Y-%m-%d} {weekday} {moment:%H:%M}"


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


def _ema_chart_points(candles: list[dict[str, Any]], period: int) -> list[dict[str, float | int]]:
    values = _to_ema([float(item["close"]) for item in candles], period)
    return [
        {"time": int(candle["time"]), "value": value}
        for candle, value in zip(candles, values)
    ]


def _to_sma(values: list[float], period: int) -> list[float]:
    if period <= 0:
        return []
    result: list[float] = []
    for index in range(1, len(values) + 1):
        window_start = max(0, index - period)
        window = values[window_start:index]
        result.append(sum(window) / len(window))
    return result


def _future_moving_average_points(
    candles: list[dict[str, Any]], bars: list[dict[str, Any]], period: int, *, exponential: bool,
) -> list[tuple[float, float]]:
    """Continue a chart moving average through projected candle closes."""
    closes = [float(item["close"]) for item in candles]
    future_bars = [item for item in bars if isinstance(item, dict)]
    if not closes or not future_bars or period <= 0:
        return []
    closes.extend(float(item["close"]) for item in future_bars)
    values = _to_ema(closes, period) if exponential else _to_sma(closes, period)
    return [
        (float(item["time_ms"]), float(values[len(candles) + index]))
        for index, item in enumerate(future_bars)
    ]


def _candle_body_pen_width(open_price: float, close_price: float, *, price_span: float) -> int:
    """Keep near-doji candle bodies visible without changing their OHLC values."""
    near_doji_threshold = max(abs(float(price_span)) * 0.002, 1e-12)
    return 2 if abs(float(close_price) - float(open_price)) <= near_doji_threshold else 1


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
    if kind in {"horizontal", "ray"}:
        if kind == "ray" and drag_mode == "endpoint_a":
            updated["time_a"] = int(candle_time)
            updated["time_b"] = int(candle_time)
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
    if normalized.endswith("UTC"):
        normalized = normalized[:-3]
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
    if unit == "W":
        return value * 7 * 24 * 60 * 60_000
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


_UTC8_WEEK_ANCHOR_MS = (3 * 86_400_000) + (16 * 3_600_000)


def _ensure_local_weekly_cache(symbol: str) -> tuple[list[Candle], bool, str | None]:
    """Build and persist a UTC+8 weekly cache from the best local source available."""
    normalized_symbol = str(symbol or "").strip().upper()
    if not normalized_symbol:
        return [], False, None

    existing_weekly = load_candle_cache(normalized_symbol, "1W", limit=None)
    source_bar: str | None = None
    source_candles: list[Candle] = []
    for candidate_bar in ("1D", "4H", "1H", "15m", "5m", "1m"):
        candidate = load_candle_cache(normalized_symbol, candidate_bar, limit=None)
        if candidate:
            source_bar = candidate_bar
            source_candles = candidate
            break
    if not source_candles:
        return existing_weekly, False, None

    generated_weekly = _aggregate_price_candles_to_resolution(
        source_candles,
        7 * 86_400_000,
        anchor_offset_ms=_UTC8_WEEK_ANCHOR_MS,
    )
    # Weekly cache rows can be polluted by bars from a different boundary or
    # timeframe. Rebuild it from the local source instead of preserving stale
    # timestamps via an append-only merge.
    changed = generated_weekly != existing_weekly
    if changed:
        replace_candle_cache(normalized_symbol, "1W", generated_weekly)
    return generated_weekly, changed, source_bar


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(value, upper))


def _compute_hover_overlay_layout(
    *,
    viewport_top: float,
    viewport_bottom: float,
    bounds_top: float,
    bounds_bottom: float,
    anchor_y: float,
    price_height: float,
    tooltip_height: float,
    volume_reserved_height: float,
) -> dict[str, float | str]:
    top_padding = 4.0
    inner_padding = 8.0
    tooltip_gap = 18.0

    safe_bottom = float(bounds_bottom) - max(float(volume_reserved_height), 0.0)
    safe_bottom = min(safe_bottom, float(viewport_bottom) - top_padding)
    if safe_bottom - float(bounds_top) < max(float(price_height), float(tooltip_height)) + inner_padding:
        safe_bottom = min(float(bounds_bottom), float(viewport_bottom) - top_padding)

    price_bottom_limit = max(float(bounds_top) + float(price_height), safe_bottom)
    price_y = _clamp(
        float(anchor_y) - (float(price_height) / 2.0),
        float(viewport_top) + top_padding,
        price_bottom_limit - float(price_height),
    )

    visible_mid_y = (float(bounds_top) + safe_bottom) / 2.0
    prefer_above = float(anchor_y) >= visible_mid_y
    above_y = float(anchor_y) - float(tooltip_height) - tooltip_gap
    below_y = float(anchor_y) + tooltip_gap
    min_tooltip_y = float(bounds_top) + inner_padding
    max_tooltip_y = safe_bottom - float(tooltip_height) - inner_padding
    can_fit_above = above_y >= min_tooltip_y
    can_fit_below = below_y <= max_tooltip_y

    if prefer_above and can_fit_above:
        tooltip_y = above_y
        tooltip_side = "above"
    elif (not prefer_above) and can_fit_below:
        tooltip_y = below_y
        tooltip_side = "below"
    elif can_fit_above:
        tooltip_y = above_y
        tooltip_side = "above"
    elif can_fit_below:
        tooltip_y = below_y
        tooltip_side = "below"
    else:
        tooltip_side = "above" if prefer_above else "below"
        if max_tooltip_y < min_tooltip_y:
            tooltip_y = min_tooltip_y
        else:
            fallback_y = above_y if tooltip_side == "above" else below_y
            tooltip_y = _clamp(fallback_y, min_tooltip_y, max_tooltip_y)

    return {
        "price_y": price_y,
        "tooltip_y": tooltip_y,
        "tooltip_side": tooltip_side,
        "safe_bottom": safe_bottom,
    }


def _compute_hover_tooltip_x(
    *,
    bounds_left: float,
    bounds_right: float,
    anchor_x: float,
    data_right_x: float,
    tooltip_width: float,
) -> float:
    edge_padding = 8.0
    data_gap = 12.0
    safe_left = float(bounds_left) + edge_padding
    safe_right = max(safe_left, float(bounds_right) - float(tooltip_width) - edge_padding)
    right_padding_x = float(data_right_x) + data_gap
    if safe_left <= right_padding_x <= safe_right:
        return right_padding_x
    return safe_right if float(anchor_x) <= (float(bounds_left) + float(bounds_right)) / 2.0 else safe_left


def _compute_axis_y_padding(min_price: float, max_price: float, *, show_atr: bool = False) -> tuple[float, float]:
    price_span = max(float(max_price) - float(min_price), 0.0)
    reference_price = max(abs(float(min_price)), abs(float(max_price)))
    baseline = max(price_span, reference_price * 0.002, 1e-8)
    top_padding = max(price_span * 0.08, reference_price * 0.002, 1e-8)
    overlay_ratio = _VOLUME_OVERLAY_HEIGHT_RATIO + (_ATR_OVERLAY_HEIGHT_RATIO if show_atr else 0.0)
    reserved_ratio = min(0.42, max(overlay_ratio + 0.03, 0.12))
    visible_ratio = max(0.18, 1.0 - reserved_ratio)
    bottom_padding = max((price_span + top_padding) * (reserved_ratio / visible_ratio), baseline * 0.12)
    return top_padding, bottom_padding


def _atr_chart_values(candles: list[dict[str, Any]], period: int = 10) -> list[float | None]:
    """Calculate Wilder ATR values for chart candles, aligned to candle indexes."""
    if period <= 0:
        raise ValueError("period must be positive")
    values: list[float | None] = [None] * len(candles)
    if len(candles) < period:
        return values
    true_ranges: list[float] = []
    previous_close: float | None = None
    for candle in candles:
        high = float(candle["high"])
        low = float(candle["low"])
        close = float(candle["close"])
        true_ranges.append(
            high - low
            if previous_close is None
            else max(high - low, abs(high - previous_close), abs(low - previous_close))
        )
        previous_close = close
    current_atr = sum(true_ranges[:period]) / period
    values[period - 1] = current_atr
    for index in range(period, len(true_ranges)):
        current_atr = ((current_atr * (period - 1)) + true_ranges[index]) / period
        values[index] = current_atr
    return values


def _axis_y_label_format(min_price: float, max_price: float) -> str:
    reference_price = max(abs(float(min_price)), abs(float(max_price)))
    if reference_price < 0.1:
        return "%.6f"
    if reference_price < 1.0:
        return "%.5f"
    if reference_price < 1_000.0:
        return "%.4f"
    return "%.2f"


def _full_native_x_range(total_bars: int) -> tuple[float, float]:
    if total_bars <= 1:
        return 0.0, 1.0
    return 0.0, float(total_bars - 1)


def _default_native_visible_range(total_bars: int, *, target_visible_bars: int = _RECENT_VIEW_BARS) -> tuple[float, float]:
    full_min, full_max = _full_native_x_range(total_bars)
    if total_bars <= 1:
        return full_min, full_max
    visible_bars = max(30, min(total_bars, target_visible_bars))
    span = max(float(visible_bars - 1), 1.0)
    return max(full_min, full_max - span), full_max


def _native_right_padding_ms(display_step_ms: int, *, padding_bars: int = _NATIVE_RIGHT_PADDING_BARS) -> float:
    return float(max(0, padding_bars) * max(1, display_step_ms))


def _right_padding_after_drag(
    *, start_padding_bars: int, delta_px: float, plot_width: float,
    visible_span_ms: float, display_step_ms: int,
) -> int:
    step = max(1, int(display_step_ms))
    bars_visible = max(float(visible_span_ms) / step, 1.0)
    pixels_per_bar = max(float(plot_width), 1.0) / bars_visible
    delta_bars = int(round(float(delta_px) / max(pixels_per_bar, 1e-6)))
    return int(_clamp(float(start_padding_bars - delta_bars), 0.0, float(_NATIVE_MAX_RIGHT_PADDING_BARS)))


def _one_hour_right_padding_bars(total_bars: int, *, full: bool = False) -> int:
    # Recent view has a fixed total span; full view adds padding to all history.
    span = max(1, total_bars - 1)
    return min(_NATIVE_MAX_RIGHT_PADDING_BARS, math.ceil(span / 2 if full else min(span, _RECENT_VIEW_BARS - 1) / 3))


def _default_native_x_range_with_right_padding(
    display_times_ms: list[int],
    *,
    display_step_ms: int,
    target_visible_bars: int = _RECENT_VIEW_BARS,
    right_padding_bars: int = _NATIVE_RIGHT_PADDING_BARS,
) -> tuple[float, float]:
    if not display_times_ms:
        padding_ms = _native_right_padding_ms(display_step_ms, padding_bars=right_padding_bars)
        return 0.0, max(float(display_step_ms), padding_ms)
    if len(display_times_ms) == 1:
        start_x = float(display_times_ms[0])
        padding_ms = _native_right_padding_ms(display_step_ms, padding_bars=right_padding_bars)
        return start_x, start_x + max(float(display_step_ms), padding_ms)
    left_index, right_index = _default_native_visible_range(len(display_times_ms), target_visible_bars=target_visible_bars)
    last_index = len(display_times_ms) - 1
    left_index = min(max(int(left_index), 0), last_index)
    right_index = min(max(int(right_index), 0), last_index)
    start_x = float(display_times_ms[left_index])
    end_x = float(display_times_ms[right_index])
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


def _next_secondary_layout_button_text(layout_mode: str) -> str:
    normalized = str(layout_mode or "").strip().lower()
    return "左右分屏" if normalized == "vertical" else "上下分屏"


def _next_secondary_chart_kind_button_text(chart_kind: str, volatility_currency: str = "BTC") -> str:
    normalized = str(chart_kind or "").strip().lower()
    return f"{volatility_currency}波动率" if normalized == "kline" else "副图K线"


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


def _display_value_for_bar_index(
    display_times_ms: list[int],
    *,
    display_step_ms: int,
    bar_index: float,
) -> float:
    if not display_times_ms:
        return 0.0
    step_ms = max(1, int(display_step_ms))
    return float(display_times_ms[0]) + (float(bar_index) * step_ms)


def _candle_time_for_bar_index(
    candles: list[dict[str, Any]],
    *,
    bar_index: float,
    display_step_ms: int,
) -> int:
    if not candles:
        return 0
    rounded_index = int(round(float(bar_index)))
    if 0 <= rounded_index < len(candles):
        return int(candles[rounded_index]["time"])
    step_seconds = max(1, int(display_step_ms) // 1000)
    if rounded_index >= len(candles):
        future_steps = rounded_index - (len(candles) - 1)
        return int(candles[-1]["time"]) + (future_steps * step_seconds)
    past_steps = abs(rounded_index)
    return int(candles[0]["time"]) - (past_steps * step_seconds)


def _rr_box_end_display_x(
    display_times_ms: list[int],
    *,
    display_step_ms: int,
    bar_entry: int,
    width_bars: int = _RR_BOX_WIDTH_BARS,
) -> float:
    if not display_times_ms:
        return 0.0
    step_ms = max(1, int(display_step_ms))
    start_display_x = _display_value_for_bar_index(display_times_ms, display_step_ms=step_ms, bar_index=float(bar_entry))
    return start_display_x + (max(1, int(width_bars)) * step_ms)


def _format_rr_table_price(value: object, increment: Decimal | None = None) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    try:
        parsed = Decimal(text)
    except Exception:
        return text
    if increment is not None and increment > 0:
        return format_decimal_by_increment(parsed, increment)
    magnitude = abs(parsed)
    if magnitude >= Decimal("1000"):
        return format_decimal(Decimal(format_decimal_fixed(parsed, 2)))
    if magnitude >= Decimal("1"):
        return format_decimal(Decimal(format_decimal_fixed(parsed, 4)))
    return format_decimal(Decimal(format_decimal_fixed(parsed, 6)))


def _line_price_table_text(line: dict[str, object], increment: Decimal | None = None) -> str:
    price_a = _format_rr_table_price(line.get("price_a", ""), increment) or "-"
    kind = str(line.get("kind", "horizontal") or "horizontal").strip().lower()
    if kind != "trend":
        return price_a
    price_b = _format_rr_table_price(line.get("price_b", ""), increment) or "-"
    return f"{price_a} → {price_b}"


def _parse_rr_optional_decimal(value: object) -> Decimal | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return Decimal(text)
    except Exception:
        return None


def _rr_fee_offset_enabled(value: object) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    return text in {"1", "true", "yes", "on", "y"}


def _normalize_rr_management_mode(value: object) -> str:
    text = str(value or "").strip().lower()
    if text in {"trail_after_1r", "trail_after_2r", "trail_after_3r"}:
        return text
    return "fixed_tp"


def _rr_management_trigger_r(value: object) -> Decimal | None:
    mode = _normalize_rr_management_mode(value)
    if mode == "trail_after_1r":
        return Decimal("1")
    if mode == "trail_after_2r":
        return Decimal("2")
    if mode == "trail_after_3r":
        return Decimal("3")
    return None


def _rr_condition_status_text(entry: RRTradeLedgerEntry | None) -> str:
    if entry is None:
        return "条件单：未启用交易"

    def _link_text(label: str, link: object | None) -> str:
        if link is None:
            return f"{label}条件单：未提交"
        algo_id = str(getattr(link, "algo_id", "") or "").strip()
        client_id = str(getattr(link, "client_id", "") or "").strip()
        state = str(getattr(link, "state", "") or "").strip().lower()
        if algo_id:
            state_text = {
                "live": "已挂",
                "partially_filled": "部分触发",
                "filled": "已触发",
                "canceled": "已撤销",
                "mmp_canceled": "已撤销",
            }.get(state, "已回传")
            return f"{label}条件单：{state_text}"
        if client_id or state == "pending":
            return f"{label}条件单：等待 OKX 回传"
        return f"{label}条件单：未提交"

    return " | ".join(
        (
            _link_text("止损", getattr(entry, "stop_loss_order", None)),
            _link_text("止盈", getattr(entry, "take_profit_order", None)),
        )
    )


def _rr_ledger_blocks_editing(entry: object | None) -> bool:
    if entry is None:
        return False
    return str(getattr(entry, "status", "") or "").strip().lower() in {
        "entry_working",
        "entry_partially_filled",
        "protected",
        "protected_break_even",
        "protected_trailing",
        "protected_cancelled_remainder",
        "cancel_confirmation_required",
    }


def _rr_order_link_looks_local_placeholder(link: object | None) -> bool:
    if link is None:
        return False
    order_id = str(getattr(link, "order_id", "") or "").strip()
    client_id = str(getattr(link, "client_id", "") or "").strip()
    algo_id = str(getattr(link, "algo_id", "") or "").strip()
    if order_id and not order_id.isdigit():
        return True
    if algo_id and not algo_id.isdigit():
        return True
    return client_id in {"cl-1", "algo-cl-sl-1", "algo-cl-tp-1"}


def _rr_entry_looks_local_placeholder(entry: object | None) -> bool:
    if entry is None:
        return False
    return any(
        _rr_order_link_looks_local_placeholder(getattr(entry, attr, None))
        for attr in ("entry_order", "stop_loss_order", "take_profit_order")
    )


def _rr_plan_position_text(plan: RRTradePlan) -> str:
    contracts_text = f"{format_decimal(plan.sizing.contract_size)}张"
    base_size = plan.sizing.base_size
    base_ccy = str(plan.instrument_ct_val_ccy or "").strip().upper()
    if base_size is not None and base_size > 0:
        return f"{format_decimal(base_size)} {base_ccy or '币'} ({contracts_text})"
    return contracts_text


def _rr_management_mode_text(value: object) -> str:
    mode = _normalize_rr_management_mode(value)
    if mode == "trail_after_1r":
        return "1:1到保本"
    if mode == "trail_after_2r":
        return "1:2到保本"
    if mode == "trail_after_3r":
        return "1:3到保本"
    return "固定止盈"


def _normalize_rr_multiple_step(value: Decimal) -> Decimal:
    if value <= 0:
        raise ValueError("r_multiple must be positive")
    normalized = snap_to_increment(value, _RR_MULTIPLE_STEP, "nearest")
    return normalized if normalized > 0 else _RR_MULTIPLE_STEP


def _compute_rr_take_profit(
    side: str,
    entry_price: Decimal,
    stop_price: Decimal,
    r_multiple: Decimal,
    *,
    fee_offset_enabled: bool,
    price_increment: Decimal | None,
) -> Decimal:
    normalized_r = _normalize_rr_multiple_step(r_multiple)
    take_profit = compute_rr_target(side, entry_price, stop_price, normalized_r)
    if fee_offset_enabled:
        fee_offset = _dynamic_two_taker_fee_offset_live(entry_price, enabled=True)
        take_profit = take_profit + fee_offset if side.strip().lower() == "long" else take_profit - fee_offset
    if price_increment is not None and price_increment > 0:
        take_profit = snap_to_increment(take_profit, price_increment, "nearest")
    return take_profit


def _compute_rr_multiple_from_take_profit(
    side: str,
    entry_price: Decimal,
    stop_price: Decimal,
    take_profit: Decimal,
    *,
    fee_offset_enabled: bool,
) -> Decimal:
    normalized_side = side.strip().lower()
    fee_offset = _dynamic_two_taker_fee_offset_live(entry_price, enabled=fee_offset_enabled)
    if normalized_side == "long":
        adjusted_take_profit = take_profit - fee_offset
        risk = entry_price - stop_price
        if risk <= 0:
            raise ValueError("long stop must be below entry")
        return _normalize_rr_multiple_step((adjusted_take_profit - entry_price) / risk)
    if normalized_side == "short":
        adjusted_take_profit = take_profit + fee_offset
        risk = stop_price - entry_price
        if risk <= 0:
            raise ValueError("short stop must be above entry")
        return _normalize_rr_multiple_step((entry_price - adjusted_take_profit) / risk)
    raise ValueError(f"unsupported side: {side!r}")


class _WrappedStatusLabel(QLabel):
    """Keep wrapped status text readable inside the scrollable tab panel."""

    def __init__(self, text: str) -> None:
        super().__init__(text)
        self.setSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Minimum)
        self.setWordWrap(True)
        self.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop)

    def setText(self, text: str) -> None:
        super().setText(text)
        self._fit_text_height()

    def resizeEvent(self, event) -> None:  # noqa: ANN001
        super().resizeEvent(event)
        self._fit_text_height()

    def _fit_text_height(self) -> None:
        # QLabel clamps heightForWidth to its current minimum; clear it so
        # shorter text and wider panels can shrink again.
        self.setMinimumHeight(0)
        self.setMinimumHeight(max(0, self.heightForWidth(self.width())))


class RMultipleSpinBox(QDoubleSpinBox):
    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setDecimals(1)
        self.setMinimum(0.1)
        self.setMaximum(1000.0)
        self.setSingleStep(0.1)
        self.setValue(2.0)
        self.setKeyboardTracking(False)

    def setText(self, text: str) -> None:
        self.setValue(float(str(text or "").strip() or "0"))


def _format_rr_percent(value: Decimal | None) -> str:
    if value is None:
        return "-"
    return f"{format_decimal(Decimal(format_decimal_fixed(value, 2)))}%"


def _compute_rr_percent(entry_price: Decimal, target_price: Decimal) -> Decimal | None:
    if entry_price <= 0:
        return None
    return (abs(target_price - entry_price) / entry_price) * Decimal("100")


def _build_rr_overlay_snapshot(
    item: dict[str, object],
    *,
    instrument: object | None,
    price_increment: Decimal | None,
) -> dict[str, str]:
    side = str(item.get("side", "long") or "long").strip().lower()
    entry_price = _parse_rr_optional_decimal(item.get("price_entry")) or Decimal("0")
    stop_price = _parse_rr_optional_decimal(item.get("price_stop")) or Decimal("0")
    take_profit_price = _parse_rr_optional_decimal(item.get("price_tp")) or Decimal("0")
    r_multiple = _parse_rr_optional_decimal(item.get("r_multiple")) or Decimal("0")
    management_mode = _normalize_rr_management_mode(item.get("management_mode"))
    direct_take_profit_r = _parse_rr_optional_decimal(item.get("direct_take_profit_r")) or r_multiple
    risk_amount = _parse_rr_optional_decimal(item.get("risk_amount")) or Decimal("100")
    leverage = _parse_rr_optional_decimal(item.get("leverage")) or Decimal("1")
    rr_ratio_text = f"1:{format_decimal(r_multiple)}" if r_multiple > 0 else "-"
    tp_pct = _format_rr_percent(_compute_rr_percent(entry_price, take_profit_price)) if take_profit_price > 0 else "-"
    stop_pct = _format_rr_percent(_compute_rr_percent(entry_price, stop_price)) if stop_price > 0 else "-"
    quantity_text = "-"
    base_text = "-"
    risk_text = "-"
    if (
        instrument is not None
        and getattr(instrument, "inst_type", "") == "SWAP"
        and entry_price > 0
        and stop_price > 0
        and risk_amount > 0
    ):
        try:
            plan = build_rr_trade_plan(
                plan_id=str(item.get("rr_id", "") or "rr-preview"),
                profile_name="",
                environment="",
                instrument=instrument,
                direction="short" if side == "short" else "long",
                entry_execution_mode="limit",
                management_mode=management_mode,
                trigger_price_type="last",
                risk_amount=risk_amount,
                entry_price=entry_price,
                stop_loss_price=stop_price,
                direct_take_profit_r=direct_take_profit_r if direct_take_profit_r > 0 else Decimal("1"),
                round_trip_fee_rate=Decimal("0"),
            )
            quantity_text = f"{format_decimal(plan.sizing.contract_size)}张"
            base_size = plan.sizing.base_size
            base_ccy = str(getattr(instrument, "ct_val_ccy", "") or "").strip().upper()
            if base_size is not None and base_size > 0:
                base_text = f"{format_decimal(base_size)} {base_ccy or '币'}"
            risk_text = format_decimal(plan.sizing.actual_risk_amount)
        except Exception:
            pass
    entry_text = _format_rr_table_price(entry_price, price_increment) if entry_price > 0 else "-"
    stop_text = _format_rr_table_price(stop_price, price_increment) if stop_price > 0 else "-"
    tp_text = _format_rr_table_price(take_profit_price, price_increment) if take_profit_price > 0 else "-"
    position_text = base_text
    if base_text != "-" and quantity_text != "-":
        position_text = f"{base_text} ({quantity_text})"
    elif quantity_text != "-":
        position_text = f"{quantity_text}"
    return {
        "overlay_tp_text": f"止盈 {tp_text} ({tp_pct})",
        "overlay_entry_text": f"入场 {entry_text}",
        "overlay_mid_text": f"入场 {entry_text} | RR {rr_ratio_text}\n币量 {position_text}",
        "overlay_stop_text": f"止损 {stop_text} ({stop_pct})",
        "card_risk_text": format_decimal(risk_amount),
        "card_leverage_text": format_decimal(leverage),
        "card_qty_text": quantity_text,
        "card_base_qty_text": base_text,
        "card_position_text": position_text,
        "card_rr_text": rr_ratio_text,
        "card_profit_pct_text": tp_pct,
        "card_stop_pct_text": stop_pct,
        "card_actual_risk_text": risk_text,
    }


class RRCardDialog(QDialog):
    def __init__(
        self,
        *,
        parent: QWidget | None,
        item: dict[str, object],
        instrument: object | None,
        symbol: str,
        period: str,
        price_increment: Decimal | None,
    ) -> None:
        super().__init__(parent)
        self._original_item = dict(item)
        self._instrument = instrument
        self._symbol = symbol
        self._period = period
        self._price_increment = price_increment
        self._result_payload: dict[str, object] | None = None
        self.setWindowTitle(f"RR 参数卡片 | {str(item.get('rr_id', '') or 'RR')}")
        self.setWindowFlags(
            self.windowFlags()
            | Qt.WindowType.WindowMinimizeButtonHint
            | Qt.WindowType.WindowMaximizeButtonHint
        )
        self.resize(560, 520)
        self.setStyleSheet(
            """
            QDialog {
                background: #0f172a;
                color: #e5edf7;
            }
            QWidget {
                background: #0f172a;
                color: #e5edf7;
            }
            QFrame {
                background: #111827;
            }
            QLabel {
                color: #dbe6f3;
            }
            QLabel#CardTitle {
                background: #111827;
                color: #f8fafc;
                font-size: 18px;
                font-weight: 700;
            }
            QLabel#CardSubtle {
                background: #111827;
                color: #9fb0c7;
                font-size: 12px;
            }
            QLabel#Subtle {
                color: #d6e2f0;
                background: #1e293b;
                border: 1px solid #334155;
                border-radius: 10px;
                padding: 10px 12px;
            }
            QTabWidget::pane {
                border: 1px solid #334155;
                background: #111827;
                border-radius: 10px;
                top: -1px;
            }
            QTabBar::tab {
                background: #0f172a;
                color: #a8b6c8;
                padding: 10px 14px;
                margin-right: 6px;
                border: 1px solid #334155;
                border-bottom: 2px solid transparent;
                border-top-left-radius: 8px;
                border-top-right-radius: 8px;
            }
            QTabBar::tab:selected {
                background: #111827;
                color: #f8fafc;
                border-bottom: 2px solid #38bdf8;
            }
            QLineEdit, QComboBox, QDoubleSpinBox {
                background: #111827;
                color: #f8fafc;
                border: 1px solid #334155;
                border-radius: 8px;
                padding: 7px 10px;
                min-height: 18px;
                selection-background-color: #2563eb;
                selection-color: #f8fafc;
            }
            QLineEdit:focus, QComboBox:focus, QDoubleSpinBox:focus {
                border: 1px solid #3b82f6;
            }
            QComboBox::drop-down, QDoubleSpinBox::up-button, QDoubleSpinBox::down-button {
                background: #162033;
                border-left: 1px solid #334155;
                width: 22px;
            }
            QComboBox::down-arrow, QDoubleSpinBox::up-arrow, QDoubleSpinBox::down-arrow {
                color: #dbe6f3;
            }
            QAbstractSpinBox::up-button:hover, QAbstractSpinBox::down-button:hover, QComboBox::drop-down:hover {
                background: #1d2a40;
            }
            QCheckBox {
                background: transparent;
                color: #dbe6f3;
                spacing: 8px;
            }
            QCheckBox::indicator {
                width: 16px;
                height: 16px;
                background: #0f172a;
                border: 1px solid #64748b;
                border-radius: 4px;
            }
            QCheckBox::indicator:checked {
                background: #2563eb;
                border: 1px solid #3b82f6;
            }
            QDialogButtonBox QPushButton {
                min-width: 88px;
                min-height: 32px;
                border-radius: 8px;
                border: 1px solid #334155;
                background: #111827;
                color: #e5edf7;
                padding: 6px 14px;
            }
            QDialogButtonBox QPushButton:hover {
                background: #1e293b;
            }
            """
        )

        root = QVBoxLayout(self)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(10)
        header = QFrame()
        header_layout = QVBoxLayout(header)
        header_layout.setContentsMargins(4, 0, 4, 0)
        header_layout.setSpacing(2)
        title = QLabel(f"{'多头' if str(item.get('side', 'long')).strip().lower() != 'short' else '空头'}盈亏比")
        title.setObjectName("CardTitle")
        subtitle = QLabel(f"{symbol or '-'} | {period or '-'} | {str(item.get('rr_id', '') or 'RR')}")
        subtitle.setObjectName("CardSubtle")
        header_layout.addWidget(title)
        header_layout.addWidget(subtitle)
        root.addWidget(header)
        tabs = QTabWidget()
        root.addWidget(tabs, 1)

        parameter_tab = QWidget()
        parameter_layout = QFormLayout(parameter_tab)
        parameter_layout.setContentsMargins(12, 12, 12, 12)
        parameter_layout.setSpacing(8)
        self._side_combo = QComboBox()
        self._side_combo.addItem("多头", "long")
        self._side_combo.addItem("空头", "short")
        self._side_combo.setCurrentIndex(1 if str(item.get("side", "long") or "long").strip().lower() == "short" else 0)
        self._management_mode_combo = QComboBox()
        self._management_mode_combo.addItem("固定止盈", "fixed_tp")
        self._management_mode_combo.addItem("1:1 到保本", "trail_after_1r")
        self._management_mode_combo.addItem("1:2 到保本", "trail_after_2r")
        self._management_mode_combo.addItem("1:3 到保本", "trail_after_3r")
        self._management_mode_combo.setCurrentIndex(
            max(0, self._management_mode_combo.findData(_normalize_rr_management_mode(item.get("management_mode"))))
        )
        self._entry_execution_mode_combo = QComboBox()
        self._entry_execution_mode_combo.addItem("限价", "limit")
        self._entry_execution_mode_combo.addItem("市价", "market")
        self._entry_execution_mode_combo.addItem("最优盘口限价（单次）", "chase_best_quote")
        self._entry_execution_mode_combo.setToolTip("提交时读取一次最优盘口并按限价提交，不会持续追价。")
        self._management_hint = QLabel("")
        self._management_hint.setWordWrap(True)
        self._management_hint.setObjectName("CardSubtle")
        self._entry_execution_mode_combo.setCurrentIndex(
            max(0, self._entry_execution_mode_combo.findData(str(item.get("entry_execution_mode", "limit") or "limit")))
        )
        self._risk_edit = QLineEdit(str(item.get("risk_amount", "100") or "100"))
        self._entry_edit = QLineEdit(_format_rr_table_price(item.get("price_entry", ""), price_increment))
        self._stop_edit = QLineEdit(_format_rr_table_price(item.get("price_stop", ""), price_increment))
        self._r_edit = RMultipleSpinBox()
        self._r_edit.setValue(float(str(item.get("r_multiple", "2") or "2")))
        self._leverage_edit = QLineEdit(str(item.get("leverage", "1") or "1"))
        self._fee_offset_check = QCheckBox("2倍手续费偏移")
        self._fee_offset_check.setChecked(_rr_fee_offset_enabled(item.get("fee_offset_enabled", False)))
        self._locked_check = QCheckBox("锁定")
        self._locked_check.setChecked(bool(item.get("locked", False)))
        self._summary_label = QLabel("")
        self._summary_label.setWordWrap(True)
        self._summary_label.setObjectName("Subtle")
        parameter_layout.addRow("方向", self._side_combo)
        parameter_layout.addRow("管理方式", self._management_mode_combo)
        parameter_layout.addRow("入场方式", self._entry_execution_mode_combo)
        parameter_layout.addRow("风险金(U)", self._risk_edit)
        parameter_layout.addRow("入场价", self._entry_edit)
        parameter_layout.addRow("止损价", self._stop_edit)
        parameter_layout.addRow("R 倍数", self._r_edit)
        parameter_layout.addRow("杠杆", self._leverage_edit)
        parameter_layout.addRow(self._fee_offset_check)
        parameter_layout.addRow(self._locked_check)
        parameter_layout.addRow("成交后管理", self._management_hint)
        parameter_layout.addRow("计算结果", self._summary_label)
        tabs.addTab(parameter_tab, "参数")

        coordinate_tab = QWidget()
        coordinate_layout = QFormLayout(coordinate_tab)
        coordinate_layout.setContentsMargins(12, 12, 12, 12)
        coordinate_layout.setSpacing(8)
        coordinate_layout.addRow("RR ID", QLabel(str(item.get("rr_id", "") or "-")))
        coordinate_layout.addRow("K线序号", QLabel(str(item.get("bar_entry", "") or "0")))
        coordinate_layout.addRow("交易对", QLabel(symbol or "-"))
        coordinate_layout.addRow("周期", QLabel(period or "-"))
        tabs.addTab(coordinate_tab, "坐标")

        visibility_tab = QWidget()
        visibility_layout = QVBoxLayout(visibility_tab)
        visibility_layout.setContentsMargins(12, 12, 12, 12)
        visibility_layout.addWidget(QLabel(f"当前应用范围：{symbol or '-'} | {period or '-'}"))
        visibility_layout.addWidget(QLabel("第一版先固定为当前图表可见范围，后续再扩展独立可见周期。"))
        visibility_layout.addStretch(1)
        tabs.addTab(visibility_tab, "可见周期")

        style_tab = QWidget()
        style_layout = QVBoxLayout(style_tab)
        style_layout.setContentsMargins(12, 12, 12, 12)
        style_layout.addWidget(QLabel("第一版样式跟随系统 RR 配色。"))
        style_layout.addWidget(QLabel("图上会显示：止盈、入场、止损、仓量、盈亏比。"))
        style_layout.addStretch(1)
        tabs.addTab(style_tab, "样式")

        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        root.addWidget(buttons)

        for widget in (self._side_combo, self._management_mode_combo, self._entry_execution_mode_combo, self._risk_edit, self._entry_edit, self._stop_edit, self._r_edit, self._leverage_edit):
            if isinstance(widget, QComboBox):
                widget.currentIndexChanged.connect(self._refresh_summary)
            elif isinstance(widget, QDoubleSpinBox):
                widget.valueChanged.connect(self._refresh_summary)
            else:
                widget.textChanged.connect(self._refresh_summary)
        self._fee_offset_check.toggled.connect(self._refresh_summary)
        self._locked_check.toggled.connect(self._refresh_summary)
        self._refresh_summary()

    def _refresh_summary(self) -> None:
        management_mode = str(self._management_mode_combo.currentData() or "fixed_tp")
        if management_mode == "fixed_tp":
            management_hint = "固定止盈：到达 RR 目标止盈后退出；不执行保本或阶梯锁盈。"
        else:
            management_hint = "监控观察到所选 R 档后移至保本；之后每跨整数 R 锁入前一整数 R，RR 目标止盈保留。"
        self._management_hint.setText(management_hint)
        self._management_mode_combo.setToolTip(management_hint)
        try:
            side = str(self._side_combo.currentData() or "long")
            entry_price = Decimal(self._entry_edit.text().strip())
            stop_price = Decimal(self._stop_edit.text().strip())
            r_multiple = Decimal(self._r_edit.text())
            risk_amount = Decimal(self._risk_edit.text().strip())
            take_profit = _compute_rr_take_profit(
                side,
                entry_price,
                stop_price,
                r_multiple,
                fee_offset_enabled=self._fee_offset_check.isChecked(),
                price_increment=self._price_increment,
            )
            snapshot = _build_rr_overlay_snapshot(
                {
                    "side": side,
                    "price_entry": decimal_to_text(entry_price),
                    "price_stop": decimal_to_text(stop_price),
                    "price_tp": decimal_to_text(take_profit),
                    "r_multiple": decimal_to_text(r_multiple),
                    "management_mode": management_mode,
                    "direct_take_profit_r": decimal_to_text(_normalize_rr_multiple_step(r_multiple)),
                    "risk_amount": decimal_to_text(risk_amount),
                    "leverage": self._leverage_edit.text().strip() or "1",
                    "fee_offset_enabled": self._fee_offset_check.isChecked(),
                },
                instrument=self._instrument,
                price_increment=self._price_increment,
            )
            self._summary_label.setText(
                "\n".join(
                    [
                        f"自动止盈：{_format_rr_table_price(take_profit, self._price_increment)}",
                        f"币数量：{snapshot['card_position_text']}",
                        f"实际风险(U)：{snapshot['card_actual_risk_text']}",
                        f"盈亏比：{snapshot['card_rr_text']}",
                        f"管理方式：{_rr_management_mode_text(management_mode)}",
                        f"手续费偏移：{'开启' if self._fee_offset_check.isChecked() else '关闭'}",
                    ]
                )
            )
        except Exception:
            self._summary_label.setText("请填写有效的入场价、止损价、R 倍数和风险金额。")

    def accept(self) -> None:
        side = str(self._side_combo.currentData() or "long")
        management_mode = str(self._management_mode_combo.currentData() or "fixed_tp")
        entry_price = Decimal(self._entry_edit.text().strip())
        stop_price = Decimal(self._stop_edit.text().strip())
        r_multiple = Decimal(self._r_edit.text())
        take_profit = _compute_rr_take_profit(
            side,
            entry_price,
            stop_price,
            r_multiple,
            fee_offset_enabled=self._fee_offset_check.isChecked(),
            price_increment=self._price_increment,
        )
        self._result_payload = {
            "side": side,
            "price_entry": decimal_to_text(entry_price),
            "price_stop": decimal_to_text(stop_price),
            "price_tp": decimal_to_text(take_profit),
            "r_multiple": decimal_to_text(_normalize_rr_multiple_step(r_multiple)),
            "management_mode": management_mode,
            "entry_execution_mode": str(self._entry_execution_mode_combo.currentData() or "limit"),
            "direct_take_profit_r": decimal_to_text(_normalize_rr_multiple_step(r_multiple)),
            "risk_amount": self._risk_edit.text().strip() or "100",
            "leverage": self._leverage_edit.text().strip() or "1",
            "fee_offset_enabled": self._fee_offset_check.isChecked(),
            "locked": self._locked_check.isChecked(),
        }
        super().accept()

    def result_payload(self) -> dict[str, object] | None:
        return dict(self._result_payload) if isinstance(self._result_payload, dict) else None


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


def _build_trading_day_spans(candles: list[dict[str, Any]], period: str) -> list[tuple[str, int, int]]:
    """Group real candle opening dates, using the same local timezone as chart tooltips."""
    if period.strip().upper() not in {"1H", "4H"}:
        return []
    spans: list[tuple[str, int, int]] = []
    for index, candle in enumerate(candles):
        moment = datetime.fromtimestamp(int(candle["time"]))
        weekday = ("周一", "周二", "周三", "周四", "周五", "周六", "周日")[moment.weekday()]
        day = f"{moment:%m-%d} {weekday}"
        if spans and spans[-1][0] == day:
            spans[-1] = (day, spans[-1][1], index + 1)
        else:
            spans.append((day, index, index + 1))
    return spans


def _prefer_native_chart_backend() -> bool:
    if os.environ.get("QQOKX_KLINE_NATIVE", "").strip() == "1":
        return True
    return os.environ.get("QTWEBENGINE_DISABLE_GPU", "").strip() == "1"


def _line_kind_text(kind: str) -> str:
    mapping = {
        "horizontal": "水平线",
        "ray": "射线",
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
    return str(period or "").strip().upper() in {"1H", "4H", "1D", "1W"}


def _trend_indicator_title(period: str) -> str:
    normalized = str(period or "").strip().upper()
    if normalized == "1H":
        return "1小时趋势"
    if normalized == "4H":
        return "4小时趋势"
    if normalized == "1D":
        return "日线趋势"
    if normalized == "1W":
        return "周线趋势"
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


def _signal_pattern_index_range(*, signal_index: int, candle_count: int) -> range:
    pattern_start = max(0, int(signal_index) - max(int(candle_count), 1) + 1)
    return range(pattern_start, int(signal_index) + 1)


def _candle_amplitude(candle: Any) -> float:
    return max(0.0, _to_float(candle.high) - _to_float(candle.low))


def _candle_body_size(candle: Any) -> float:
    return abs(_to_float(candle.close) - _to_float(candle.open))


def _candle_shadow_size(candle: Any, *, side: str) -> float:
    normalized = str(side or "").strip().lower()
    if normalized == "upper":
        return max(0.0, _to_float(candle.high) - max(_to_float(candle.open), _to_float(candle.close)))
    if normalized == "lower":
        return max(0.0, min(_to_float(candle.open), _to_float(candle.close)) - _to_float(candle.low))
    return 0.0


def _is_bullish_candle(candle: Any) -> bool:
    return _to_float(candle.close) > _to_float(candle.open)


def _is_bearish_candle(candle: Any) -> bool:
    return _to_float(candle.close) < _to_float(candle.open)


def _rank_in_recent_window(
    candles: list[Any],
    index: int,
    *,
    lookback: int = 10,
    metric: str = "body",
    reference_start: int | None = None,
) -> int | None:
    if index < 0 or index >= len(candles):
        return None
    lookback = max(int(lookback), 1)
    if reference_start is None:
        start = max(0, index - lookback + 1)
        end = index + 1
    else:
        start = int(reference_start)
        end = start + lookback
        if start < 0 or end > len(candles):
            return None
        if any(not bool(getattr(candles[item], "confirmed", True)) for item in range(start, end)):
            return None
    metric_func = _candle_amplitude if str(metric).strip().lower() == "range" else _candle_body_size
    target = metric_func(candles[index])
    if target <= 0:
        return None
    larger_count = 0
    for candle in candles[start:end]:
        candidate_value = metric_func(candle)
        exceeds_rank_threshold = candidate_value >= target if reference_start is not None else candidate_value > target
        if exceeds_rank_threshold:
            larger_count += 1
    return larger_count + 1


def _shadow_rank_in_recent_window(
    candles: list[Any],
    index: int,
    *,
    lookback: int = 10,
    side: str,
    reference_start: int | None = None,
) -> int | None:
    if index < 0 or index >= len(candles):
        return None
    lookback = max(int(lookback), 1)
    if reference_start is None:
        start = max(0, index - lookback + 1)
        end = index + 1
    else:
        start = int(reference_start)
        end = start + lookback
        if start < 0 or end > len(candles):
            return None
        if any(not bool(getattr(candles[item], "confirmed", True)) for item in range(start, end)):
            return None
    target = _candle_shadow_size(candles[index], side=side)
    if target <= 0:
        return None
    larger_count = 0
    for candle in candles[start:end]:
        candidate_value = _candle_shadow_size(candle, side=side)
        exceeds_rank_threshold = candidate_value >= target if reference_start is not None else candidate_value > target
        if exceeds_rank_threshold:
            larger_count += 1
    return larger_count + 1


def _signal_core_amplitude_candle(
    *,
    candles: list[Any],
    signal_index: int,
    candle_count: int,
    lookback: int = 10,
    top_n: int = 4,
    metric: str = "body",
    pattern_start: int | None = None,
) -> tuple[int, int] | None:
    if signal_index < 0 or signal_index >= len(candles):
        return None
    if pattern_start is not None and str(metric).strip().lower() == "range":
        return significant_range_core(
            candles,
            tuple(range(int(pattern_start), int(signal_index) + 1)),
            lookback=lookback,
            top_n=top_n,
        )
    reference_start = None
    if pattern_start is not None:
        reference_start = int(pattern_start) - max(int(lookback), 1)
        if reference_start < 0:
            return None
        pattern_end = int(signal_index) + 1
        if any(not bool(getattr(candles[item], "confirmed", True)) for item in range(reference_start, pattern_end)):
            return None
    best: tuple[int, int] | None = None
    for index in _signal_pattern_index_range(signal_index=signal_index, candle_count=candle_count):
        if index < 0 or index >= len(candles):
            continue
        rank = _rank_in_recent_window(
            candles, index, lookback=lookback, metric=metric, reference_start=reference_start,
        )
        if rank is None or rank > top_n:
            continue
        if best is None or rank < best[1]:
            best = (index, rank)
    return best


def _nearest_enabled_ma_distance(
    *,
    candle: Any,
    index: int,
    ema15_values: list[float],
    sma50_values: list[float],
    include_ema15: bool,
    include_sma50: bool,
) -> tuple[str, float | None]:
    near_items: list[tuple[str, float | None]] = []
    if include_ema15 and 0 <= index < len(ema15_values):
        near_items.append(("EMA15", _distance_to_line_pct(candle, float(ema15_values[index]))))
    if include_sma50 and 0 <= index < len(sma50_values):
        near_items.append(("MA50", _distance_to_line_pct(candle, float(sma50_values[index]))))
    valid_near = [(name, value) for name, value in near_items if value is not None]
    return min(valid_near, key=lambda item: item[1]) if valid_near else ("-", None)


def _candle_touches_enabled_ma(
    *,
    candle: Any,
    index: int,
    ema15_values: list[float],
    sma50_values: list[float],
    include_ema15: bool,
    include_sma50: bool,
) -> bool:
    if index < 0 or index >= len(ema15_values) or index >= len(sma50_values):
        return False
    return (
        (include_ema15 and _candle_crosses_line(candle, float(ema15_values[index])))
        or (include_sma50 and _candle_crosses_line(candle, float(sma50_values[index])))
    )


def _double_reversal_reclaims_first_body(
    *,
    first: Any,
    second: Any,
    direction: str,
) -> bool:
    first_open = _to_float(first.open)
    first_close = _to_float(first.close)
    second_close = _to_float(second.close)
    if direction == "long":
        if first_close >= first_open:
            return False
        reclaim_level = first_close + (first_open - first_close) * 0.6
        return second_close >= reclaim_level
    if first_close <= first_open:
        return False
    reclaim_level = first_close - (first_close - first_open) * 0.6
    return second_close <= reclaim_level


def _double_reversal_is_local_extreme(
    *,
    candles: list[Any],
    signal_index: int,
    direction: str,
    lookback: int = 8,
) -> bool:
    window_start = max(0, signal_index - lookback + 1)
    window = candles[window_start : signal_index + 1]
    if len(window) < lookback:
        return False
    if direction == "long":
        pair_low = min(_to_float(candles[signal_index - 1].low), _to_float(candles[signal_index].low))
        return pair_low <= min(_to_float(candle.low) for candle in window)
    pair_high = max(_to_float(candles[signal_index - 1].high), _to_float(candles[signal_index].high))
    return pair_high >= max(_to_float(candle.high) for candle in window)


def _double_reversal_reclaims_consolidation_break(
    *,
    candles: list[Any],
    signal_index: int,
    direction: str,
    lookback: int = 6,
) -> bool:
    first_index = signal_index - 1
    window_start = first_index - lookback
    if window_start < 0:
        return False
    box = candles[window_start:first_index]
    if len(box) != lookback:
        return False
    box_high = max(_to_float(candle.high) for candle in box)
    box_low = min(_to_float(candle.low) for candle in box)
    average_range = sum(_to_float(candle.high) - _to_float(candle.low) for candle in box) / len(box)
    if average_range <= 0.0 or (box_high - box_low) > average_range * 3.0:
        return False
    first = candles[first_index]
    second = candles[signal_index]
    if direction == "long":
        return _to_float(first.low) < box_low and _to_float(second.close) >= box_low
    return _to_float(first.high) > box_high and _to_float(second.close) <= box_high


def _double_reversal_reclaims_ma(
    *,
    first: Any,
    second: Any,
    first_index: int,
    second_index: int,
    direction: str,
    ema15_values: list[float],
    sma50_values: list[float],
    include_ema15: bool,
    include_sma50: bool,
) -> bool:
    line_sets: list[list[float]] = []
    if include_ema15:
        line_sets.append(ema15_values)
    if include_sma50:
        line_sets.append(sma50_values)
    first_close = _to_float(first.close)
    second_close = _to_float(second.close)
    for values in line_sets:
        if first_index >= len(values) or second_index >= len(values):
            continue
        first_line = float(values[first_index])
        second_line = float(values[second_index])
        if direction == "long" and first_close < first_line and second_close >= second_line:
            return True
        if direction == "short" and first_close > first_line and second_close <= second_line:
            return True
    return False


def _double_reversal_has_structure(
    *,
    candles: list[Any],
    signal_index: int,
    direction: str,
    ema15_values: list[float],
    sma50_values: list[float],
    include_ema15: bool,
    include_sma50: bool,
) -> bool:
    first = candles[signal_index - 1]
    second = candles[signal_index]
    if not _double_reversal_reclaims_first_body(first=first, second=second, direction=direction):
        return False
    return (
        _double_reversal_is_local_extreme(candles=candles, signal_index=signal_index, direction=direction)
        or _double_reversal_reclaims_consolidation_break(candles=candles, signal_index=signal_index, direction=direction)
        or _double_reversal_reclaims_ma(
            first=first,
            second=second,
            first_index=signal_index - 1,
            second_index=signal_index,
            direction=direction,
            ema15_values=ema15_values,
            sma50_values=sma50_values,
            include_ema15=include_ema15,
            include_sma50=include_sma50,
        )
    )


def _build_replay_signal_markers(
    *,
    candles: list[Any],
    period: str,
    ema15_values: list[float],
    sma50_values: list[float],
) -> list[dict[str, Any]]:
    normalized_period = period.strip().upper()
    if normalized_period not in {"4H", "1H", "1D", "1W"} or not candles:
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
        if signal.pattern_id in {
            "false_breakdown", "false_breakout", "double_reversal_up", "double_reversal_down",
        }:
            continue
        if signal.pattern_id not in _REPLAY_SIGNAL_LABELS:
            continue
        index = int(signal.index)
        if index < 0 or index >= len(candles) or index >= len(ema15_values) or index >= len(sma50_values):
            continue
        candle = candles[index]
        if not bool(getattr(candle, "confirmed", True)):
            continue
        pattern_start = _signal_pattern_index_range(
            signal_index=index, candle_count=int(signal.candle_count),
        ).start
        if pattern_start < 0 or any(
            not bool(getattr(candles[item], "confirmed", True))
            for item in range(pattern_start, index + 1)
        ):
            continue
        core_body_candle = _signal_core_amplitude_candle(
            candles=candles,
            signal_index=index,
            candle_count=int(signal.candle_count),
            lookback=10,
            top_n=4,
            metric="body",
            pattern_start=pattern_start,
        )
        ranked_range_candle = formation_range_rank(
            candles,
            tuple(range(pattern_start, index + 1)),
            lookback=10,
        )
        core_index, core_rank = ranked_range_candle if ranked_range_candle is not None else (None, None)
        core_item = candles[core_index] if core_index is not None else None
        core_ma_touch = bool(core_item is not None and _candle_touches_enabled_ma(
            candle=core_item,
            index=int(core_index),
            ema15_values=ema15_values,
            sma50_values=sma50_values,
            include_ema15=include_ema15,
            include_sma50=include_sma50,
        ))
        nearest_name, nearest_distance = _nearest_enabled_ma_distance(
            candle=candle,
            index=index,
            ema15_values=ema15_values,
            sma50_values=sma50_values,
            include_ema15=include_ema15,
            include_sma50=include_sma50,
        )
        key = (index, signal.pattern_id)
        if key in seen:
            continue
        direction = str(signal.direction or "neutral").strip().lower()
        color = _REPLAY_SIGNAL_LONG_COLOR if direction == "long" else _REPLAY_SIGNAL_SHORT_COLOR
        label = _REPLAY_SIGNAL_LABELS.get(signal.pattern_id, signal.pattern_name)
        text_suffix = f"核心K振幅第{core_rank}" if core_rank is not None else "核心K振幅排名不可用"
        shadow_rank = None
        if signal.pattern_id == "long_upper_shadow":
            signal_range = _candle_amplitude(candle)
            if signal_range <= 0 or _candle_shadow_size(candle, side="upper") < signal_range * 0.55:
                continue
            shadow_rank = _shadow_rank_in_recent_window(
                candles, index, lookback=10, side="upper", reference_start=pattern_start - 10,
            )
        elif signal.pattern_id == "long_lower_shadow":
            signal_range = _candle_amplitude(candle)
            if signal_range <= 0 or _candle_shadow_size(candle, side="lower") < signal_range * 0.55:
                continue
            shadow_rank = _shadow_rank_in_recent_window(
                candles, index, lookback=10, side="lower", reference_start=pattern_start - 10,
            )
        if shadow_rank is not None:
            text_suffix = f"核心K振幅第{core_rank if core_rank is not None else '—'} 影线第{shadow_rank}"
        if nearest_name != "-" and nearest_distance is not None and nearest_distance <= _REPLAY_SIGNAL_NEAR_MA_MAX_PCT:
            text_suffix = f"{text_suffix} @{nearest_name}"
        seen.add(key)
        markers.append(
            {
                "index": index,
                "time": _to_ms_seconds(int(signal.ts)),
                "direction": direction,
                "pattern_id": signal.pattern_id,
                "label": label,
                "text": f"{label} {text_suffix}",
                "near_ma": nearest_name,
                "distance_pct": float(nearest_distance or 0.0) * 100.0,
                "core_index": core_index,
                "core_amplitude_rank": core_rank,
                "core_body_index": core_body_candle[0] if core_body_candle is not None else None,
                "core_body_rank": core_body_candle[1] if core_body_candle is not None else None,
                "core_range_index": core_index,
                "core_range_rank": core_rank,
                "shadow_rank": shadow_rank,
                "core_ma_touch": core_ma_touch,
                "score": int(signal.score),
                "score_detail": str(getattr(signal, "score_detail", "") or ""),
                "color": color,
            }
        )
    for index in range(1, len(candles)):
        if index >= len(ema15_values) or index >= len(sma50_values):
            continue
        first = candles[index - 1]
        second = candles[index]
        if not bool(getattr(first, "confirmed", True)) or not bool(getattr(second, "confirmed", True)):
            continue
        pattern_id = ""
        direction = "neutral"
        if _is_bearish_candle(first) and _is_bullish_candle(second):
            pattern_id = "double_reversal_up"
            direction = "long"
        elif _is_bullish_candle(first) and _is_bearish_candle(second):
            pattern_id = "double_reversal_down"
            direction = "short"
        if not pattern_id:
            continue
        if not _double_reversal_has_structure(
            candles=candles,
            signal_index=index,
            direction=direction,
            ema15_values=ema15_values,
            sma50_values=sma50_values,
            include_ema15=include_ema15,
            include_sma50=include_sma50,
        ):
            continue
        pattern_start = index - 1
        core_body_candle = _signal_core_amplitude_candle(
            candles=candles,
            signal_index=index,
            candle_count=2,
            lookback=10,
            top_n=4,
            metric="body",
            pattern_start=pattern_start,
        )
        ranked_range_candle = formation_range_rank(candles, (pattern_start, index), lookback=10)
        core_index, core_rank = ranked_range_candle if ranked_range_candle is not None else (None, None)
        key = (index, pattern_id)
        if key in seen:
            continue
        seen.add(key)
        core_item = candles[core_index] if core_index is not None else None
        core_ma_touch = _candle_touches_enabled_ma(
            candle=core_item if core_item is not None else second,
            index=int(core_index) if core_index is not None else index,
            ema15_values=ema15_values,
            sma50_values=sma50_values,
            include_ema15=include_ema15,
            include_sma50=include_sma50,
        )
        nearest_name, nearest_distance = _nearest_enabled_ma_distance(
            candle=second,
            index=index,
            ema15_values=ema15_values,
            sma50_values=sma50_values,
            include_ema15=include_ema15,
            include_sma50=include_sma50,
        )
        label = _REPLAY_SIGNAL_LABELS[pattern_id]
        text_suffix = f"核心K振幅第{core_rank}" if core_rank is not None else "核心K振幅排名不可用"
        score, score_rank = pattern_quality_score(candles, (pattern_start, index))
        score_detail = f"结构60 + 振幅显著性{score - 60}（振幅第{score_rank}）" if score_rank is not None else "结构60 + 振幅排名不可用"
        if nearest_name != "-" and nearest_distance is not None and nearest_distance <= _REPLAY_SIGNAL_NEAR_MA_MAX_PCT:
            text_suffix = f"{text_suffix} @{nearest_name}"
        markers.append(
            {
                "index": index,
                "time": _to_ms_seconds(int(second.ts)),
                "direction": direction,
                "pattern_id": pattern_id,
                "label": label,
                "text": f"{label} {text_suffix}",
                "near_ma": nearest_name,
                "distance_pct": float(nearest_distance or 0.0) * 100.0,
                "core_index": core_index,
                "core_amplitude_rank": core_rank,
                "core_body_index": core_body_candle[0] if core_body_candle is not None else None,
                "core_body_rank": core_body_candle[1] if core_body_candle is not None else None,
                "core_range_index": core_index,
                "core_range_rank": core_rank,
                "core_ma_touch": core_ma_touch,
                "score": score,
                "score_detail": score_detail,
                "color": _REPLAY_SIGNAL_LONG_COLOR if direction == "long" else _REPLAY_SIGNAL_SHORT_COLOR,
            }
        )
    for index, candle in enumerate(candles):
        if index >= len(ema15_values) or index >= len(sma50_values):
            continue
        if not bool(getattr(candle, "confirmed", True)):
            continue
        body = _candle_body_size(candle)
        upper_shadow = _candle_shadow_size(candle, side="upper")
        lower_shadow = _candle_shadow_size(candle, side="lower")
        pattern_start = index
        reference_start = pattern_start - 10
        candle_range = _candle_amplitude(candle)
        if candle_range <= 0:
            continue
        range_core = formation_range_rank(candles, (index,), lookback=10)
        range_rank = range_core[1] if range_core is not None else None
        candidates: list[tuple[str, str, str, int | None]] = []
        if upper_shadow >= (body * 2.0) and upper_shadow >= (candle_range * 0.55) and upper_shadow >= lower_shadow:
            candidates.append(("long_upper_shadow", "short", "upper", _shadow_rank_in_recent_window(
                candles, index, lookback=10, side="upper", reference_start=reference_start,
            )))
        if lower_shadow >= (body * 2.0) and lower_shadow >= (candle_range * 0.55) and lower_shadow >= upper_shadow:
            candidates.append(("long_lower_shadow", "long", "lower", _shadow_rank_in_recent_window(
                candles, index, lookback=10, side="lower", reference_start=reference_start,
            )))
        if not candidates:
            continue
        for pattern_id, direction, side, shadow_rank in candidates:
            key = (index, pattern_id)
            if key in seen:
                continue
            seen.add(key)
            core_ma_touch = _candle_touches_enabled_ma(
                candle=candle,
                index=index,
                ema15_values=ema15_values,
                sma50_values=sma50_values,
                include_ema15=include_ema15,
                include_sma50=include_sma50,
            )
            nearest_name, nearest_distance = _nearest_enabled_ma_distance(
                candle=candle,
                index=index,
                ema15_values=ema15_values,
                sma50_values=sma50_values,
                include_ema15=include_ema15,
                include_sma50=include_sma50,
            )
            label = _REPLAY_SIGNAL_LABELS[pattern_id]
            text_suffix = (
                f"核心K振幅第{range_rank if range_rank is not None else '—'} "
                f"影线第{shadow_rank if shadow_rank is not None else '—'}"
            )
            if nearest_name != "-" and nearest_distance is not None and nearest_distance <= _REPLAY_SIGNAL_NEAR_MA_MAX_PCT:
                text_suffix = f"{text_suffix} @{nearest_name}"
            marker_score, marker_rank = pattern_quality_score(candles, (index,))
            markers.append(
                {
                    "index": index,
                    "time": _to_ms_seconds(int(candle.ts)),
                    "direction": direction,
                    "pattern_id": pattern_id,
                    "label": label,
                    "text": f"{label} {text_suffix}",
                    "near_ma": nearest_name,
                    "distance_pct": float(nearest_distance or 0.0) * 100.0,
                    "core_index": index,
                    "core_amplitude_rank": range_rank,
                    "core_body_index": None,
                    "core_body_rank": None,
                    "core_range_index": index,
                    "core_range_rank": range_rank,
                    "shadow_rank": shadow_rank,
                    "core_ma_touch": core_ma_touch,
                    "score": marker_score,
                    "score_detail": (
                        f"结构60 + 振幅显著性{marker_score - 60}"
                        f"（振幅第{marker_rank if marker_rank is not None else '—'}）"
                    ),
                    "color": _REPLAY_SIGNAL_LONG_COLOR if direction == "long" else _REPLAY_SIGNAL_SHORT_COLOR,
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
        history_end_index = _extend_history_box_end_index(
            start_index=int(box["start_index"]),
            end_index=int(box["end_index"]),
            upper=upper,
            lower=lower,
            opens=opens,
            highs=highs,
            lows=lows,
            closes=closes,
            atr_values=atr_values,
        )
        overlays.append(
            {
                "start_index": int(box["start_index"]) + scan_offset,
                "end_index": history_end_index + scan_offset,
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


def _build_box_current_overlay(candles: list[Any]) -> list[dict[str, Any]]:
    if len(candles) < 24:
        return []
    scan_candles = list(candles[-_BOX_HISTORY_SCAN_LIMIT:])
    scan_offset = len(candles) - len(scan_candles)
    boxes = detect_boxes(scan_candles, BoxDetectionConfig(max_candidates=AUTO_BOX_MAX_CANDIDATES))
    box = next((item for item in boxes if is_auto_box_candidate_valid(item, scan_candles)), None)
    if box is None:
        return []
    upper = float(box.upper)
    lower = float(box.lower)
    return [
        {
            "start_index": int(box.start_index) + scan_offset,
            "end_index": int(box.end_index) + scan_offset,
            "upper": upper,
            "lower": lower,
            "mode": "current",
            "label": f"自动箱体 {lower:.2f}-{upper:.2f}",
            "touches": int(box.upper_touches + box.lower_touches),
            "violations": int(box.violations),
            "trend": "",
            "score": float(box.score),
            "active": True,
            "outline": _BOX_ACTIVE_OUTLINE_COLOR,
            "fill": _BOX_ACTIVE_FILL_COLOR,
        }
    ]


def _build_channel_current_overlays(
    candles: list[Any],
    *,
    config: ChannelDetectionConfig | None = None,
) -> list[dict[str, Any]]:
    """Reuse the research module's channel detector as a K-line chart layer."""
    if len(candles) < 12:
        return []
    scan_candles = list(candles[-_AUTO_CHANNEL_SCAN_LIMIT:])
    scan_offset = len(candles) - len(scan_candles)
    snapshot = build_auto_channel_live_chart_snapshot(
        session_id="kline-auto-channel",
        candles=scan_candles,
        channel_config=config,
        max_channels=1,
        max_boxes=0,
        max_trendlines=0,
        max_triangles=0,
        show_pivots=False,
    )
    overlays: list[dict[str, Any]] = []
    for item in snapshot.band_overlays:
        start_index = int(item.start_index)
        end_index = int(item.end_index)
        if start_index < 0 or end_index < start_index:
            continue
        overlays.append(
            {
                "mode": "current",
                "start_index": start_index + scan_offset,
                "end_index": end_index + scan_offset,
                "upper_start": float(item.upper_line.value_at(start_index)),
                "upper_end": float(item.upper_line.value_at(end_index)),
                "lower_start": float(item.lower_line.value_at(start_index)),
                "lower_end": float(item.lower_line.value_at(end_index)),
                "label": str(item.label or "自动通道"),
                "outline": str(item.outline or "#2563eb"),
                "fill": str(item.fill or "#dbeafe"),
            }
        )
    return overlays


def _extend_history_box_end_index(
    *,
    start_index: int,
    end_index: int,
    upper: float,
    lower: float,
    opens: list[float],
    highs: list[float],
    lows: list[float],
    closes: list[float],
    atr_values: list[float],
) -> int:
    if end_index >= len(closes) - 1:
        return end_index
    width = max(upper - lower, 0.0)
    price = max(abs(closes[end_index]), 1.0)
    atr = max(_mean([value for value in atr_values[start_index : end_index + 1] if value > 0]), price * 0.001)
    boundary_tolerance = max(width * 0.12, atr * 0.35)
    extended_end = end_index
    for index in range(end_index + 1, len(closes)):
        body_high = max(opens[index], closes[index])
        body_low = min(opens[index], closes[index])
        if body_high > upper + boundary_tolerance or body_low < lower - boundary_tolerance:
            break
        extended_end = index
    return extended_end


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
    channel_overlays: list[dict[str, Any]] = field(default_factory=list)
    alert_snapshot: KlineAlertSnapshot | None = None


@dataclass(frozen=True)
class KlineAlertSnapshot:
    workspace_entry: dict[str, object]
    new_events: list[dict[str, object]]
    structure: dict[str, object]


@dataclass(frozen=True)
class KlineHistoryTradeMarker:
    """One opening or closing event from an account position rendered on the K-line chart."""

    at_seconds: int
    price: float
    direction: str
    event: str
    realized_pnl: Decimal | None = None
    pnl_currency: str = ""
    size: Decimal | None = None
    lifecycle_id: str = ""


def _history_position_direction(item: OkxPositionHistoryItem) -> str:
    for raw_value in (getattr(item, "pos_side", None), getattr(item, "direction", None)):
        value = str(raw_value or "").strip().lower()
        if value in {"long", "short"}:
            return value
        if "long" in value or "多" in value:
            return "long"
        if "short" in value or "空" in value:
            return "short"
    return ""


def _current_position_direction(item: OkxPosition) -> str:
    raw = item.raw if isinstance(item.raw, dict) else {}
    for raw_value in (getattr(item, "pos_side", None), raw.get("posSide"), raw.get("direction")):
        value = str(raw_value or "").strip().lower()
        if value in {"long", "short"}:
            return value
        if "long" in value or "多" in value:
            return "long"
        if "short" in value or "空" in value:
            return "short"
    try:
        return "long" if Decimal(str(getattr(item, "position", 0) or 0)) > 0 else "short"
    except (TypeError, ValueError):
        return ""


def _history_time_to_seconds(value: object) -> int:
    try:
        timestamp = int(value or 0)
    except (TypeError, ValueError):
        return 0
    return timestamp // 1000 if timestamp > 10_000_000_000 else timestamp


def _history_position_time(item: OkxPositionHistoryItem, *, event: str) -> int:
    raw = item.raw if isinstance(item.raw, dict) else {}
    if event == "open":
        values = (raw.get("openTime"), raw.get("openTs"), raw.get("cTime"), raw.get("createdTime"))
    else:
        values = (
            raw.get("closeTime"),
            raw.get("closeTs"),
            raw.get("uTime"),
            raw.get("updateTime"),
            item.update_time,
        )
    for value in values:
        timestamp = _history_time_to_seconds(value)
        if timestamp > 0:
            return timestamp
    return 0


def _build_kline_history_trade_markers(
    items: list[OkxPositionHistoryItem] | tuple[OkxPositionHistoryItem, ...],
    *,
    symbol: str,
) -> list[KlineHistoryTradeMarker]:
    """Map each exchange position record to opening and closing K-line markers."""

    normalized_symbol = symbol.strip().upper()
    markers: list[KlineHistoryTradeMarker] = []
    seen_openings: set[tuple[str, str, str]] = set()
    for item in items:
        if str(getattr(item, "inst_id", "") or "").strip().upper() != normalized_symbol:
            continue
        direction = _history_position_direction(item)
        if direction not in {"long", "short"}:
            continue
        try:
            open_price = float(getattr(item, "open_avg_price", None) or 0.0)
            close_price = float(getattr(item, "close_avg_price", None) or 0.0)
        except (TypeError, ValueError):
            open_price = 0.0
            close_price = 0.0
        opened_at = _history_position_time(item, event="open")
        raw = item.raw if isinstance(item.raw, dict) else {}
        lifecycle_id = "|".join(
            (
                normalized_symbol,
                str(raw.get("posId") or "").strip(),
                str(raw.get("cTime") or opened_at or "").strip(),
            )
        )
        if opened_at > 0 and open_price > 0.0:
            # Use the exchange lifecycle id so distinct positions opened close
            # together do not collapse into one marker.
            opening_key = (normalized_symbol, direction, lifecycle_id)
            if opening_key not in seen_openings:
                seen_openings.add(opening_key)
                markers.append(
                    KlineHistoryTradeMarker(
                        at_seconds=opened_at,
                        price=open_price,
                        direction=direction,
                        event="open",
                        size=getattr(item, "close_size", None),
                        lifecycle_id=lifecycle_id,
                    )
                )

        closed_at = _history_position_time(item, event="close")
        if closed_at <= 0 or close_price <= 0.0:
            continue
        realized_pnl = getattr(item, "realized_pnl", None)
        if realized_pnl is None:
            realized_pnl = getattr(item, "pnl", None)
        pnl_currency = str(raw.get("ccy") or raw.get("pnlCcy") or raw.get("settleCcy") or "").strip().upper()
        markers.append(
            KlineHistoryTradeMarker(
                at_seconds=closed_at,
                price=close_price,
                direction=direction,
                event="close",
                realized_pnl=realized_pnl if isinstance(realized_pnl, Decimal) else None,
                pnl_currency=pnl_currency,
                size=getattr(item, "close_size", None),
                lifecycle_id=lifecycle_id,
            )
        )
    markers.sort(key=lambda item: (item.at_seconds, item.direction, item.event, item.price))
    return markers


def _build_kline_fill_history_trade_markers(
    items: list[OkxFillHistoryItem] | tuple[OkxFillHistoryItem, ...],
    *,
    symbol: str,
) -> list[KlineHistoryTradeMarker]:
    """Map actual fills to chart events, combining partial fills from one order."""

    normalized_symbol = symbol.strip().upper()
    fills: list[tuple[int, OkxFillHistoryItem, Decimal, Decimal, str, str]] = []
    for item in items:
        if str(getattr(item, "inst_id", "") or "").strip().upper() != normalized_symbol:
            continue
        timestamp = _history_time_to_seconds(getattr(item, "fill_time", None))
        side = str(getattr(item, "side", "") or "").strip().lower()
        try:
            price = Decimal(str(getattr(item, "fill_price", None)))
            size = Decimal(str(getattr(item, "fill_size", None)))
        except Exception:
            continue
        if timestamp <= 0 or price <= 0 or size <= 0 or side not in {"buy", "sell"}:
            continue
        raw = item.raw if isinstance(item.raw, dict) else {}
        pnl_currency = str(raw.get("pnlCcy") or raw.get("settleCcy") or raw.get("ccy") or "").strip().upper()
        fills.append((timestamp, item, price, size, side, pnl_currency))
    fills.sort(key=lambda row: (row[0], str(getattr(row[1], "trade_id", "") or "")))

    # Net-mode accounts do not identify each fill as open or close. Track the
    # observed signed position, and use non-zero realized fill PnL to recognize
    # closes when the locally cached window starts mid-position.
    net_position = Decimal("0")
    grouped: dict[tuple[str, str, str], dict[str, Any]] = {}
    for timestamp, item, price, size, side, pnl_currency in fills:
        pos_side = str(getattr(item, "pos_side", "") or "").strip().lower()
        pnl = getattr(item, "pnl", None)
        try:
            pnl_value = Decimal(str(pnl)) if pnl is not None else None
        except Exception:
            pnl_value = None
        if pos_side in {"long", "short"}:
            direction = pos_side
            is_open = (direction == "long" and side == "buy") or (direction == "short" and side == "sell")
            parts = [("open" if is_open else "close", direction, size)]
        else:
            parts: list[tuple[str, str, Decimal]] = []
            remaining = size
            if side == "buy":
                if net_position < 0:
                    closed = min(remaining, -net_position)
                    parts.append(("close", "short", closed))
                    net_position += closed
                    remaining -= closed
                if remaining > 0:
                    parts.append(("open", "long", remaining))
                    net_position += remaining
                elif not parts and pnl_value not in (None, Decimal("0")):
                    parts.append(("close", "short", size))
            else:
                if net_position > 0:
                    closed = min(remaining, net_position)
                    parts.append(("close", "long", closed))
                    net_position -= closed
                    remaining -= closed
                if remaining > 0:
                    parts.append(("open", "short", remaining))
                    net_position -= remaining
                elif not parts and pnl_value not in (None, Decimal("0")):
                    parts.append(("close", "long", size))

        order_id = str(getattr(item, "order_id", "") or getattr(item, "trade_id", "") or timestamp)
        for event, direction, part_size in parts:
            key = (order_id, event, direction)
            aggregate = grouped.setdefault(
                key,
                {
                    "first_time": timestamp,
                    "last_time": timestamp,
                    "size": Decimal("0"),
                    "price_value": Decimal("0"),
                    "pnl": Decimal("0"),
                    "has_pnl": False,
                    "pnl_currency": pnl_currency,
                },
            )
            aggregate["first_time"] = min(aggregate["first_time"], timestamp)
            aggregate["last_time"] = max(aggregate["last_time"], timestamp)
            aggregate["size"] += part_size
            aggregate["price_value"] += price * part_size
            if event == "close" and pnl_value is not None:
                aggregate["pnl"] += pnl_value
                aggregate["has_pnl"] = True

    markers: list[KlineHistoryTradeMarker] = []
    for (_order_id, event, direction), aggregate in grouped.items():
        total_size = aggregate["size"]
        if total_size <= 0:
            continue
        at_seconds = aggregate["first_time"] if event == "open" else aggregate["last_time"]
        markers.append(
            KlineHistoryTradeMarker(
                at_seconds=at_seconds,
                price=float(aggregate["price_value"] / total_size),
                direction=direction,
                event=event,
                realized_pnl=aggregate["pnl"] if aggregate["has_pnl"] else None,
                pnl_currency=aggregate["pnl_currency"],
                size=total_size,
            )
        )
    markers.sort(key=lambda marker: (marker.at_seconds, marker.direction, marker.event, marker.price))
    return markers


def _build_kline_current_position_markers(
    items: list[OkxPosition] | tuple[OkxPosition, ...],
    *,
    symbol: str,
) -> list[KlineHistoryTradeMarker]:
    """Map currently held positions to opening markers without inventing a close."""

    normalized_symbol = symbol.strip().upper()
    markers: list[KlineHistoryTradeMarker] = []
    seen: set[tuple[str, str, int]] = set()
    for item in items:
        if str(getattr(item, "inst_id", "") or "").strip().upper() != normalized_symbol:
            continue
        direction = _current_position_direction(item)
        raw = item.raw if isinstance(item.raw, dict) else {}
        opened_at = 0
        for value in (raw.get("openTime"), raw.get("openTs"), raw.get("cTime"), raw.get("createdTime")):
            opened_at = _history_time_to_seconds(value)
            if opened_at > 0:
                break
        try:
            open_price = float(getattr(item, "avg_price", None) or raw.get("avgPx") or 0.0)
        except (TypeError, ValueError):
            open_price = 0.0
        if direction not in {"long", "short"} or opened_at <= 0 or open_price <= 0.0:
            continue
        key = (normalized_symbol, direction, opened_at)
        if key in seen:
            continue
        seen.add(key)
        markers.append(
            KlineHistoryTradeMarker(
                at_seconds=opened_at,
                price=open_price,
                direction=direction,
                event="open",
                size=abs(item.position) if isinstance(item.position, Decimal) else None,
                lifecycle_id="|".join(
                    (
                        normalized_symbol,
                        str(raw.get("posId") or "").strip(),
                        str(raw.get("cTime") or opened_at or "").strip(),
                    )
                ),
            )
        )
    markers.sort(key=lambda item: (item.at_seconds, item.direction, item.price))
    return markers


def _merge_kline_trade_markers(
    history_markers: list[KlineHistoryTradeMarker] | tuple[KlineHistoryTradeMarker, ...],
    current_markers: list[KlineHistoryTradeMarker] | tuple[KlineHistoryTradeMarker, ...],
) -> tuple[KlineHistoryTradeMarker, ...]:
    """Merge current openings with history while keeping one opening per position."""

    merged = list(history_markers)
    existing_openings = {
        (item.direction, item.lifecycle_id or str(item.at_seconds))
        for item in merged
        if item.event == "open"
    }
    for item in current_markers:
        key = (item.direction, item.lifecycle_id or str(item.at_seconds))
        if item.event == "open" and key in existing_openings:
            continue
        merged.append(item)
        if item.event == "open":
            existing_openings.add(key)
    merged.sort(key=lambda item: (item.at_seconds, item.direction, item.event, item.price))
    return tuple(merged)


def _filter_kline_history_trade_markers(
    markers: list[KlineHistoryTradeMarker] | tuple[KlineHistoryTradeMarker, ...],
    *,
    direction_filter: str,
) -> list[KlineHistoryTradeMarker]:
    normalized_filter = str(direction_filter or "all").strip().lower()
    if normalized_filter not in {"long", "short"}:
        return list(markers)
    return [item for item in markers if item.direction == normalized_filter]


def _history_trade_markers_for_candles(
    markers: list[KlineHistoryTradeMarker] | tuple[KlineHistoryTradeMarker, ...],
    candles: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Attach opening and closing events to the candles containing their timestamps."""

    if not markers or not candles:
        return []
    times = [int(item.get("time", 0) or 0) for item in candles]
    result: list[dict[str, Any]] = []
    for marker in markers:
        index = bisect_right(times, marker.at_seconds) - 1
        if index < 0 or index >= len(candles):
            continue
        is_open = marker.event == "open"
        pnl_text = ""
        if not is_open and marker.realized_pnl is not None:
            pnl_text = f" {marker.realized_pnl:+.2f}"
        is_long = marker.direction == "long"
        result.append(
            {
                "index": index,
                "price": marker.price,
                "direction": marker.direction,
                "event": marker.event,
                "timestamp": marker.at_seconds,
                "realized_pnl": marker.realized_pnl,
                "pnl_currency": marker.pnl_currency,
                "size": marker.size,
                "lifecycle_id": marker.lifecycle_id,
                "label": f"{'开' if is_open else '平'}{'多' if is_long else '空'}{pnl_text}",
                "color": _HISTORY_TRADE_OPEN_COLOR if is_open else _HISTORY_TRADE_CLOSE_COLOR,
            }
        )
    return result


def _pair_kline_trade_markers(
    markers: list[dict[str, Any]] | tuple[dict[str, Any], ...],
) -> tuple[
    list[tuple[dict[str, Any], dict[str, Any], float]],
    dict[int, tuple[dict[str, Any], float]],
    dict[str, list[dict[str, Any]]],
]:
    """Match closes to opening quantities in FIFO order, preserving partial positions."""

    def marker_size(marker: dict[str, Any]) -> Decimal | None:
        try:
            size = Decimal(str(marker.get("size")))
        except Exception:
            return None
        return size if size > 0 else None

    def result_percent(entry: dict[str, Any], close: dict[str, Any]) -> float:
        try:
            entry_price = float(entry.get("logical_price", entry.get("price", 0.0)) or 0.0)
            close_price = float(close.get("logical_price", close.get("price", 0.0)) or 0.0)
        except (TypeError, ValueError):
            return 0.0
        if entry_price <= 0.0:
            return 0.0
        change = close_price - entry_price
        if str(entry.get("direction", "") or "").strip().lower() == "short":
            change = -change
        return change / entry_price * 100.0

    pending: dict[str, list[dict[str, Any]]] = {"long": [], "short": []}
    remaining: dict[int, Decimal | None] = {}
    segments: list[tuple[dict[str, Any], dict[str, Any], float]] = []
    exit_results: dict[int, tuple[dict[str, Any], float]] = {}
    lifecycle_entries = {
        str(marker.get("lifecycle_id") or "").strip(): marker
        for marker in markers
        if str(marker.get("event", "") or "").strip().lower() == "open"
        and str(marker.get("lifecycle_id") or "").strip()
    }
    for marker in markers:
        direction = str(marker.get("direction", "") or "").strip().lower()
        event = str(marker.get("event", "") or "").strip().lower()
        if direction not in pending:
            continue
        if event == "open":
            pending[direction].append(marker)
            remaining[id(marker)] = marker_size(marker)
            continue
        if event != "close" or not pending[direction]:
            continue

        lifecycle_id = str(marker.get("lifecycle_id") or "").strip()
        direct_entry = lifecycle_entries.get(lifecycle_id) if lifecycle_id else None
        if direct_entry is not None and str(direct_entry.get("direction", "") or "").strip().lower() == direction:
            segments.append((direct_entry, marker, result_percent(direct_entry, marker)))
            exit_results[id(marker)] = (direct_entry, result_percent(direct_entry, marker))
            pending[direction] = [entry for entry in pending[direction] if entry is not direct_entry]
            remaining.pop(id(direct_entry), None)
            lifecycle_entries.pop(lifecycle_id, None)
            continue

        close_remaining = marker_size(marker)
        allocations: list[tuple[dict[str, Any], Decimal | None]] = []
        while pending[direction]:
            entry = pending[direction][0]
            entry_remaining = remaining.get(id(entry))
            if close_remaining is None or entry_remaining is None:
                pending[direction].pop(0)
                remaining.pop(id(entry), None)
                allocations.append((entry, None))
                break

            allocated = min(close_remaining, entry_remaining)
            if allocated <= 0:
                pending[direction].pop(0)
                remaining.pop(id(entry), None)
                continue
            allocations.append((entry, allocated))
            close_remaining -= allocated
            entry_remaining -= allocated
            if entry_remaining <= 0:
                pending[direction].pop(0)
                remaining.pop(id(entry), None)
            else:
                remaining[id(entry)] = entry_remaining
            if close_remaining <= 0:
                break

        if not allocations:
            continue
        weighted_entry = 0.0
        weighted_size = Decimal("0")
        for entry, allocated in allocations:
            percent = result_percent(entry, marker)
            segments.append((entry, marker, percent))
            if allocated is None:
                weighted_entry = float(entry.get("logical_price", entry.get("price", 0.0)) or 0.0)
                weighted_size = Decimal("0")
                break
            weighted_entry += float(entry.get("logical_price", entry.get("price", 0.0)) or 0.0) * float(allocated)
            weighted_size += allocated
        if weighted_size > 0:
            representative = dict(allocations[0][0])
            representative["price"] = weighted_entry / float(weighted_size)
            representative.pop("logical_price", None)
            exit_results[id(marker)] = (representative, result_percent(representative, marker))
        else:
            entry = allocations[0][0]
            exit_results[id(marker)] = (entry, result_percent(entry, marker))
    return segments, exit_results, pending


def _best_parameter_indicator_specs(
    symbol: str,
    period: str,
    *,
    direction_filter: str = "all",
) -> list[tuple[str, str, int, str]]:
    """Return the selected best-parameter indicator lines for one symbol and period."""

    normalized_period = str(period or "").strip().upper()
    if normalized_period not in {"15M", "1H", "4H", "1D", "1W"}:
        return []
    normalized_direction = str(direction_filter or "all").strip().lower()
    include_long = normalized_direction != "short"
    include_short = normalized_direction != "long"
    seen: set[tuple[str, int]] = set()
    result: list[tuple[str, str, int, str]] = []
    defaults_by_strategy: list[dict[str, object]] = []
    if include_long:
        defaults_by_strategy.append(
            get_strategy_symbol_parameter_defaults(STRATEGY_DYNAMIC_LONG_ID, symbol, "launcher")
        )
    if include_short:
        defaults_by_strategy.append(
            get_strategy_symbol_parameter_defaults(STRATEGY_EMA55_SLOPE_SHORT_ID, symbol, "launcher")
        )
    for defaults in defaults_by_strategy:
        for type_key, period_key in (
            ("ema_type", "ema_period"),
            ("trend_ema_type", "trend_ema_period"),
            ("entry_reference_ema_type", "entry_reference_ema_period"),
        ):
            raw_type = str(defaults.get(type_key, "ema") or "ema").strip().lower()
            try:
                line_period = int(defaults.get(period_key, 0) or 0)
            except (TypeError, ValueError):
                line_period = 0
            if raw_type not in {"ema", "ma", "sma"} or line_period <= 0:
                continue
            normalized_type = "ma" if raw_type in {"ma", "sma"} else "ema"
            key = (normalized_type, line_period)
            if key in seen:
                continue
            seen.add(key)
            label = f"最佳 {'EMA' if normalized_type == 'ema' else 'MA'} {line_period}"
            color = _CHART_BEST_INDICATOR_COLORS[len(result) % len(_CHART_BEST_INDICATOR_COLORS)]
            result.append((label, normalized_type, line_period, color))
    return result


def _merge_realtime_candle_payload(payload: KlineChartPayload, candle: Candle) -> KlineChartPayload:
    """Replace an open bar or append a new bar without loading history again."""
    raw_candles = list(payload.raw_candles)
    replacement_index = next((index for index, item in enumerate(raw_candles) if item.ts == candle.ts), None)
    if replacement_index is None:
        raw_candles.append(candle)
        raw_candles.sort(key=lambda item: item.ts)
    else:
        raw_candles[replacement_index] = candle
    limit = max(1, len(payload.raw_candles))
    raw_candles = raw_candles[-limit:]
    chart_candles = [
        {
            "time": _to_ms_seconds(item.ts),
            "open": _to_float(item.open),
            "high": _to_float(item.high),
            "low": _to_float(item.low),
            "close": _to_float(item.close),
            "volume": _to_float(item.volume),
        }
        for item in raw_candles
    ]
    closes = [_to_float(item.close) for item in raw_candles]
    times = [item["time"] for item in chart_candles]
    ema15_values = _to_ema(closes, 15)
    sma50_values = _to_sma(closes, 50)
    ema55_values = _to_ema(closes, 55)
    stats = dict(payload.stats)
    stats.update({"returned": len(raw_candles), "end_ms": _to_ms_seconds(raw_candles[-1].ts)})
    return KlineChartPayload(
        candles=chart_candles,
        ema_9=[{"time": times[index], "value": value} for index, value in enumerate(ema15_values)],
        ema_21=[{"time": times[index], "value": value} for index, value in enumerate(sma50_values)],
        ema_55=[{"time": times[index], "value": value} for index, value in enumerate(ema55_values)],
        trend_indicator=payload.trend_indicator,
        signal_markers=payload.signal_markers,
        box_overlays=payload.box_overlays,
        raw_candles=raw_candles,
        stats=stats,
        channel_overlays=payload.channel_overlays,
        alert_snapshot=payload.alert_snapshot,
    )


def _slice_chart_payload_tail(payload: KlineChartPayload, count: int) -> KlineChartPayload:
    if count <= 0 or len(payload.candles) <= count:
        return payload
    offset = len(payload.candles) - count

    def _slice_channel_overlay(item: dict[str, Any]) -> dict[str, Any] | None:
        start_index = int(item.get("start_index", -1))
        end_index = int(item.get("end_index", -1))
        if start_index < 0 or end_index < start_index or end_index < offset:
            return None
        clipped_start = max(start_index, offset)
        clipped_end = min(end_index, len(payload.candles) - 1)
        if clipped_end < clipped_start:
            return None
        span = max(1, end_index - start_index)
        result = dict(item, start_index=clipped_start - offset, end_index=clipped_end - offset)
        for prefix in ("upper", "lower"):
            start_price = float(item.get(f"{prefix}_start", 0.0) or 0.0)
            end_price = float(item.get(f"{prefix}_end", 0.0) or 0.0)
            result[f"{prefix}_start"] = start_price + (end_price - start_price) * ((clipped_start - start_index) / span)
            result[f"{prefix}_end"] = start_price + (end_price - start_price) * ((clipped_end - start_index) / span)
        return result

    channel_overlays = [
        sliced
        for item in payload.channel_overlays
        if isinstance(item, dict)
        for sliced in [_slice_channel_overlay(item)]
        if sliced is not None
    ]
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
        channel_overlays=channel_overlays,
        alert_snapshot=payload.alert_snapshot,
    )


def _reverse_kline_price(value: float, anchor_price: float) -> float:
    return float(anchor_price) * 2.0 - float(value)


def _reverse_kline_anchor_price(payload: KlineChartPayload) -> float:
    if payload.candles:
        first_close = payload.candles[0].get("close")
        if isinstance(first_close, (int, float)):
            return float(first_close)
    return 0.0


def _reverse_kline_chart_payload(payload: KlineChartPayload) -> KlineChartPayload:
    if not payload.candles:
        return payload
    anchor_price = _reverse_kline_anchor_price(payload)

    reversed_candles: list[dict[str, Any]] = []
    for item in payload.candles:
        if not isinstance(item, dict):
            continue
        original_open = float(item.get("open", 0.0) or 0.0)
        original_close = float(item.get("close", 0.0) or 0.0)
        reversed_open = _reverse_kline_price(original_open, anchor_price)
        reversed_close = _reverse_kline_price(original_close, anchor_price)
        if original_close >= original_open:
            display_open = min(reversed_open, reversed_close)
            display_close = max(reversed_open, reversed_close)
        else:
            display_open = max(reversed_open, reversed_close)
            display_close = min(reversed_open, reversed_close)
        reversed_candles.append(
            {
                **item,
                "open": display_open,
                "high": _reverse_kline_price(float(item.get("low", 0.0) or 0.0), anchor_price),
                "low": _reverse_kline_price(float(item.get("high", 0.0) or 0.0), anchor_price),
                "close": display_close,
            }
        )

    def _reverse_line(points: list[dict[str, Any]]) -> list[dict[str, Any]]:
        reversed_points: list[dict[str, Any]] = []
        for point in points:
            if not isinstance(point, dict):
                continue
            reversed_points.append(
                {
                    **point,
                    "value": _reverse_kline_price(float(point.get("value", 0.0) or 0.0), anchor_price),
                }
            )
        return reversed_points

    reversed_boxes: list[dict[str, Any]] = []
    for item in payload.box_overlays:
        if not isinstance(item, dict):
            continue
        upper = float(item.get("upper", 0.0) or 0.0)
        lower = float(item.get("lower", 0.0) or 0.0)
        reversed_boxes.append(
            {
                **item,
                "upper": _reverse_kline_price(lower, anchor_price),
                "lower": _reverse_kline_price(upper, anchor_price),
            }
        )

    reversed_channels: list[dict[str, Any]] = []
    for item in payload.channel_overlays:
        if not isinstance(item, dict):
            continue
        reversed_channels.append(
            {
                **item,
                "upper_start": _reverse_kline_price(float(item.get("lower_start", 0.0) or 0.0), anchor_price),
                "upper_end": _reverse_kline_price(float(item.get("lower_end", 0.0) or 0.0), anchor_price),
                "lower_start": _reverse_kline_price(float(item.get("upper_start", 0.0) or 0.0), anchor_price),
                "lower_end": _reverse_kline_price(float(item.get("upper_end", 0.0) or 0.0), anchor_price),
            }
        )

    reversed_stats = dict(payload.stats)
    reversed_stats["reverse_kline"] = True
    reversed_stats["reverse_anchor_price"] = anchor_price

    return KlineChartPayload(
        candles=reversed_candles,
        ema_9=_reverse_line(payload.ema_9),
        ema_21=_reverse_line(payload.ema_21),
        ema_55=_reverse_line(payload.ema_55),
        trend_indicator=[dict(item) for item in payload.trend_indicator if isinstance(item, dict)],
        signal_markers=[dict(item) for item in payload.signal_markers if isinstance(item, dict)],
        box_overlays=reversed_boxes,
        raw_candles=list(payload.raw_candles),
        stats=reversed_stats,
        channel_overlays=reversed_channels,
        alert_snapshot=payload.alert_snapshot,
    )


if QChartView is not None:
    class InteractiveKlineChartView(QChartView):
        chartPointClicked = Signal(float, float)
        chartDoubleClicked = Signal(float, float)
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
            self.setToolTip("1H图表中，左键拖动最后一根K线右侧的空白区可调整留白；双击图表可恢复近期视图。")
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
            self._right_padding_bars = _NATIVE_RIGHT_PADDING_BARS
            self._right_padding_drag: tuple[float, int, float] | None = None
            self._symbol = ""
            self._venue_label = "OKX"
            # Latest market price is a paint-only annotation.  It is deliberately
            # kept outside the candle/axis data so it cannot change scaling.
            self._latest_price: float | None = None
            self._indicator_series: list[dict[str, Any]] = []
            self._atr_values: list[float | None] = []
            self._show_atr = False
            self._chart_note_lines: list[str] = []
            self._workspace_lines: list[dict[str, object]] = []
            self._workspace_rr_items: list[dict[str, object]] = []
            self._signal_markers: list[dict[str, Any]] = []
            self._history_trade_markers: list[dict[str, Any]] = []
            self._area_shape_replay_trades: list[dict[str, Any]] = []
            self._trading_day_spans: list[tuple[str, int, int]] = []
            self._area_replay_label_rects: list[QRectF] = []
            self._area_replay_label_pressed = False
            self._box_overlays: list[dict[str, Any]] = []
            self._channel_overlays: list[dict[str, Any]] = []
            self._future_projection: dict[str, Any] | None = None
            self._future_ma_specs: list[tuple[str, int, str, int]] = []
            self._selected_workspace_line_index = -1
            self._selected_workspace_rr_index = -1
            self._hovered_workspace_line_index = -1
            self._hovered_workspace_drag_mode: str | None = None
            self._preview_line: dict[str, object] | None = None
            self._preview_rr_item: dict[str, object] | None = None
            self._full_x_min = 0.0
            self._full_x_max = 1.0
            self._full_y_min = 0.0
            self._full_y_max = 1.0
            self._hover_pos: QPointF | None = None
            self._press_pos: QPointF | None = None
            self._last_pointer_pos: QPointF | None = None
            self._pan_anchor_x: float | None = None
            self._dragging = False
            self._interaction_locked = False
            self._external_hover_time: int | None = None
            self._external_hover_price: float | None = None
            self._suppress_x_range_signal = False
            self._price_badge = self._create_hover_label(multiline=False, center=True)
            self._time_badge = self._create_hover_label(multiline=False, center=True)
            self._tooltip_badge = self._create_hover_label(multiline=True, center=False)
            self._hide_hover_overlays()
            self._apply_cursor_mode("default")

        def set_draw_mode_enabled(self, enabled: bool) -> None:
            self._draw_mode_enabled = enabled
            self._pan_anchor_x = None
            self._right_padding_drag = None
            self._press_pos = None
            self._dragging = False
            self._apply_cursor_mode("crosshair" if enabled else "default")

        def set_interaction_locked(self, locked: bool) -> None:
            self._interaction_locked = bool(locked)
            if locked:
                self._pan_anchor_x = None
                self._right_padding_drag = None
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

        def set_external_hover_time(self, candle_time: int | None, price: float | None = None) -> None:
            self._external_hover_time = int(candle_time) if candle_time is not None else None
            self._external_hover_price = (
                float(price)
                if candle_time is not None and price is not None and math.isfinite(price)
                else None
            )
            if self._hover_pos is None:
                self.viewport().update()

        def hover_price_for_link(self) -> float | None:
            context = self._resolve_hover_context()
            if context is None:
                return None
            _candle, index, price, hover_y = context
            atr_geometry = self._atr_overlay_geometry(self.chart().plotArea())
            if (
                atr_geometry is not None
                and atr_geometry[0] <= hover_y <= atr_geometry[1]
                and index < len(self._atr_values)
                and self._atr_values[index] is not None
            ):
                return None  # ATR has its own units, not the chart's price units.
            return price

        def _external_hover_y(self, plot_area: QRectF) -> float | None:
            price = self._external_hover_price
            if price is None or self._axis_y is None:
                return None
            if not float(self._axis_y.min()) <= price <= float(self._axis_y.max()):
                return None  # Do not clamp an off-screen price to the chart edge.
            return self._y_for_value(price, plot_area)

        def set_latest_price(self, price: float | None) -> None:
            """Set the paint-only latest-price marker without changing the axis."""
            try:
                value = float(price) if price is not None else None
            except (TypeError, ValueError):
                value = None
            self._latest_price = value if value is not None and math.isfinite(value) and value > 0 else None
            self.viewport().update()

        def set_preview_line(self, preview_line: dict[str, object] | None) -> None:
            self._preview_line = dict(preview_line) if isinstance(preview_line, dict) else None
            self.viewport().update()

        def set_preview_rr_item(self, preview_rr_item: dict[str, object] | None) -> None:
            self._preview_rr_item = dict(preview_rr_item) if isinstance(preview_rr_item, dict) else None
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
                "right_padding_bars": float(self._right_padding_bars),
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
            atr_values: list[float | None] | None = None,
            show_atr: bool = False,
            chart_note_lines: list[str] | None = None,
            workspace_lines: list[dict[str, object]] | None = None,
            workspace_rr_items: list[dict[str, object]] | None = None,
            trend_indicators: list[dict[str, Any]] | None = None,
            signal_markers: list[dict[str, Any]] | None = None,
            history_trade_markers: list[dict[str, Any]] | None = None,
            box_overlays: list[dict[str, Any]] | None = None,
            channel_overlays: list[dict[str, Any]] | None = None,
            selected_workspace_line_index: int = -1,
            selected_workspace_rr_index: int = -1,
            hovered_workspace_line_index: int = -1,
            hovered_workspace_drag_mode: str | None = None,
            latest_price: float | None = None,
            restore_state: dict[str, float | bool] | None = None,
        ) -> None:
            self._axis_x = axis_x
            self._axis_y = axis_y
            self._candles = list(candles)
            self._overlay_values = [list(item) for item in overlay_values]
            self._display_times_ms = list(display_times_ms)
            self._period = period.strip() or "15m"
            self._trading_day_spans = _build_trading_day_spans(self._candles, self._period)
            if self._period.strip().upper() == "1H" and restore_state is None:
                self._right_padding_bars = _one_hour_right_padding_bars(len(candles))
            elif self._period.strip().upper() == "1H" and restore_state is not None:
                try:
                    restored_padding = int(restore_state.get("right_padding_bars", self._right_padding_bars))
                except (TypeError, ValueError):
                    restored_padding = self._right_padding_bars
                self._right_padding_bars = int(_clamp(float(restored_padding), 0.0, float(_NATIVE_MAX_RIGHT_PADDING_BARS)))
            self._symbol = symbol.strip().upper()
            self._venue_label = venue_label.strip().upper() or "OKX"
            try:
                resolved_latest_price = float(latest_price) if latest_price is not None else None
            except (TypeError, ValueError):
                resolved_latest_price = None
            self._latest_price = (
                resolved_latest_price
                if resolved_latest_price is not None and math.isfinite(resolved_latest_price) and resolved_latest_price > 0
                else (float(candles[-1]["close"]) if candles else None)
            )
            self._indicator_series = list(indicator_series or [])
            self._atr_values = list(atr_values or [])
            self._show_atr = bool(show_atr and self._atr_values)
            self._chart_note_lines = [str(item).strip() for item in (chart_note_lines or []) if str(item).strip()]
            self._trend_indicators = [dict(item) for item in (trend_indicators or []) if isinstance(item, dict)]
            self._workspace_lines = [dict(item) for item in (workspace_lines or []) if isinstance(item, dict)]
            self._workspace_rr_items = [dict(item) for item in (workspace_rr_items or []) if isinstance(item, dict)]
            self._signal_markers = [dict(item) for item in (signal_markers or []) if isinstance(item, dict)]
            self._history_trade_markers = [dict(item) for item in (history_trade_markers or []) if isinstance(item, dict)]
            self._box_overlays = [dict(item) for item in (box_overlays or []) if isinstance(item, dict)]
            self._channel_overlays = [dict(item) for item in (channel_overlays or []) if isinstance(item, dict)]
            self._selected_workspace_line_index = int(selected_workspace_line_index)
            self._selected_workspace_rr_index = int(selected_workspace_rr_index)
            self._hovered_workspace_line_index = int(hovered_workspace_line_index)
            self._hovered_workspace_drag_mode = str(hovered_workspace_drag_mode or "").strip().lower() or None
            self._display_step_ms = _display_step_ms(self._period, candles)
            if len(self._display_times_ms) >= 2:
                self._display_step_ms = max(1, self._display_times_ms[1] - self._display_times_ms[0])
            if self._display_times_ms:
                self._full_x_min = float(self._display_times_ms[0])
                self._full_x_max = float(self._display_times_ms[-1]) + _native_right_padding_ms(
                    self._display_step_ms, padding_bars=self._active_right_padding_bars(),
                )
            else:
                self._full_x_min = 0.0
                self._full_x_max = max(
                    float(self._display_step_ms),
                    _native_right_padding_ms(self._display_step_ms, padding_bars=self._active_right_padding_bars()),
                )
            if candles:
                self._full_y_min = min(float(item["low"]) for item in candles)
                self._full_y_max = max(float(item["high"]) for item in candles)
                trade_prices = [
                    float(item.get("price", 0.0) or 0.0)
                    for item in self._history_trade_markers
                    if float(item.get("price", 0.0) or 0.0) > 0.0
                ]
                if trade_prices:
                    self._full_y_min = min(self._full_y_min, min(trade_prices))
                    self._full_y_max = max(self._full_y_max, max(trade_prices))
            else:
                self._full_y_min = 0.0
                self._full_y_max = 1.0
            start_x, end_x = self._restore_x_range(restore_state)
            self._apply_x_range(start_x, end_x, emit_signal=False)
            self._fit_y_axis_to_visible_range()
            self._hover_pos = None
            self._hide_hover_overlays()
            self.viewport().update()

        def set_future_projection(self, projection: dict[str, Any] | None) -> None:
            self._future_projection = dict(projection) if isinstance(projection, dict) else None
            if self._future_projection and self._axis_x is not None and self._display_times_ms:
                bars = self._future_projection.get("bars", [])
                if isinstance(bars, list) and bars:
                    future_end = max(float(item.get("time_ms", 0) or 0) for item in bars if isinstance(item, dict))
                    old_min, old_max = self.current_x_range()
                    self._full_x_max = max(self._full_x_max, future_end + self._display_step_ms)
                    span = max(old_max - old_min, self._display_step_ms * 30)
                    new_max = self._full_x_max
                    self._apply_x_range(max(self._full_x_min, new_max - span), new_max, emit_signal=True)
                    prices = [float(item[key]) for item in bars if isinstance(item, dict) for key in ("low", "high")]
                    prices.extend(float(self._future_projection[key]) for key in ("entry", "stop", "target") if self._future_projection.get(key))
                    if prices and self._axis_y is not None:
                        current_low, current_high = float(self._axis_y.min()), float(self._axis_y.max())
                        padding = max((max(prices) - min(prices)) * 0.08, 0.01)
                        self._axis_y.setRange(min(current_low, min(prices) - padding), max(current_high, max(prices) + padding))
            elif self._axis_x is not None and self._display_times_ms:
                self._full_x_max = float(self._display_times_ms[-1]) + _native_right_padding_ms(
                    self._display_step_ms, padding_bars=self._active_right_padding_bars(),
                )
                self.reset_view()
                self._fit_y_axis_to_visible_range()
            self.viewport().update()

        def set_area_shape_replay_trades(
            self, rows: list[dict[str, Any]] | None, *, focus: bool = False, fit: bool = False,
        ) -> None:
            self._area_shape_replay_trades = [dict(item) for item in (rows or []) if isinstance(item, dict)]
            self._area_replay_label_rects = []
            if focus and self._area_shape_replay_trades and self._display_times_ms:
                indices: list[int] = []
                for item in self._area_shape_replay_trades:
                    for key in ("signal_ts", "entry_ts", "exit_ts"):
                        try:
                            timestamp = int(item.get(key, 0) or 0)
                        except (TypeError, ValueError):
                            continue
                        if timestamp > 0:
                            indices.append(self._index_for_candle_time(timestamp // 1000))
                if indices:
                    # Keep enough surrounding candles to inspect the setup without
                    # magnifying a short trade into a handful of oversized bars.
                    candle_count = len(self._display_times_ms)
                    trade_span = max(indices) - min(indices) + 1
                    visible_count = min(candle_count, max(240, math.ceil(trade_span * 1.7)))
                    left = max(0, min(
                        min(indices) - (visible_count - trade_span) // 2,
                        candle_count - visible_count,
                    ))
                    right = left + visible_count - 1
                    target_start = float(self._display_times_ms[left])
                    target_end = float(self._display_times_ms[right]) + self._display_step_ms
                    current_start, current_end = self.current_x_range()
                    if fit or target_start < current_start or target_end > current_end:
                        self._apply_x_range(target_start, target_end, emit_signal=True)
                        self._fit_y_axis_to_visible_range()
            self.viewport().update()

        def set_future_ma_specs(self, specs: list[tuple[str, int, str, int]]) -> None:
            self._future_ma_specs = [
                (str(ma_type), int(period), str(color), int(width))
                for ma_type, period, color, width in specs
                if str(ma_type).strip().lower() in {"ema", "ma", "sma"} and int(period) > 0
            ]
            self.viewport().update()

        def clear_chart_context(self) -> None:
            self._trading_day_spans = []
            self._area_replay_label_rects = []
            self._area_replay_label_pressed = False
            self._axis_x = None
            self._axis_y = None
            self._candles = []
            self._latest_price = None
            self._overlay_values = []
            self._trend_indicators = []
            self._display_times_ms = []
            self._indicator_series = []
            self._chart_note_lines = []
            self._workspace_lines = []
            self._workspace_rr_items = []
            self._signal_markers = []
            self._history_trade_markers = []
            self._box_overlays = []
            self._channel_overlays = []
            self._future_projection = None
            self._selected_workspace_rr_index = -1
            self._preview_rr_item = None
            self._hover_pos = None
            self._press_pos = None
            self._last_pointer_pos = None
            self._pan_anchor_x = None
            self._right_padding_drag = None
            self._dragging = False
            self._external_hover_time = None
            self._external_hover_price = None
            self._hide_hover_overlays()
            self.viewport().update()

        def set_workspace_lines(
            self,
            workspace_lines: list[dict[str, object]],
            *,
            selected_index: int = -1,
        ) -> None:
            """Update drawing overlays without rebuilding the candle chart."""
            self._workspace_lines = [dict(item) for item in workspace_lines if isinstance(item, dict)]
            self._selected_workspace_line_index = int(selected_index)
            self.viewport().update()

        def reset_view(self) -> None:
            if self._axis_x is None:
                return
            if self._period.strip().upper() == "1H":
                self._set_right_padding_bars(_one_hour_right_padding_bars(len(self._candles)))
            start_x, end_x = self._default_x_range()
            self._apply_x_range(start_x, end_x, emit_signal=True)
            self._fit_y_axis_to_visible_range()
            self._hover_pos = None
            self._hide_hover_overlays()
            self.viewport().update()

        def set_recent_view_range(self) -> None:
            self.reset_view()

        def set_full_view_range(self) -> None:
            if self._axis_x is None:
                return
            if self._period.strip().upper() == "1H":
                self._set_right_padding_bars(_one_hour_right_padding_bars(len(self._candles), full=True))
            start_x, end_x = self._full_x_min, self._full_x_max
            self._apply_x_range(start_x, end_x, emit_signal=True)
            self._fit_y_axis_to_visible_range()
            self._hover_pos = None
            self._hide_hover_overlays()
            self.viewport().update()

        def set_external_x_range(self, start_x: float, end_x: float) -> None:
            if self._axis_x is None or not self._candles:
                return
            if self._period.strip().upper() == "1H" and self._display_times_ms and end_x > self._full_x_max:
                required_padding = math.ceil((float(end_x) - float(self._display_times_ms[-1])) / max(self._display_step_ms, 1))
                self._right_padding_bars = int(_clamp(float(required_padding), 0.0, float(_NATIVE_MAX_RIGHT_PADDING_BARS)))
                self._full_x_max = float(self._display_times_ms[-1]) + _native_right_padding_ms(
                    self._display_step_ms, padding_bars=self._right_padding_bars,
                )
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

        def _area_replay_label_at(self, position: QPointF) -> bool:
            return any(rect.contains(position) for rect in self._area_replay_label_rects)

        def mousePressEvent(self, event) -> None:  # noqa: ANN001
            if event.button() == Qt.MouseButton.LeftButton and self._area_replay_label_at(event.position()):
                # Painted report labels are read-only: do not select/drag live drawings beneath them.
                self._area_replay_label_pressed = True
                event.accept()
                return
            self._area_replay_label_pressed = False
            plot_area = self.chart().plotArea()
            if event.button() == Qt.MouseButton.LeftButton and plot_area.contains(event.position()):
                self.chartActivated.emit()
                self._press_pos = QPointF(event.position())
                self._last_pointer_pos = QPointF(event.position())
                if self._is_right_padding_position(float(event.position().x()), plot_area):
                    self._right_padding_drag = (
                        float(event.position().x()), self._right_padding_bars,
                        max(self.current_x_range()[1] - self.current_x_range()[0], 1.0),
                    )
                    self._pan_anchor_x = None
                else:
                    self._right_padding_drag = None
                    self._pan_anchor_x = float(event.position().x())
                self._dragging = False
                if self._right_padding_drag is None:
                    point = self.chart().mapToValue(event.position())
                    self.chartPointerPressed.emit(float(point.x()), float(point.y()))
            super().mousePressEvent(event)

        def mouseMoveEvent(self, event) -> None:  # noqa: ANN001
            if self._area_replay_label_pressed:
                event.accept()
                return
            plot_area = self.chart().plotArea()
            if plot_area.contains(event.position()) and self._right_padding_drag is None:
                self._last_pointer_pos = QPointF(event.position())
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
                and self._right_padding_drag is not None
                and event.buttons() & Qt.MouseButton.LeftButton
            ):
                start_x, start_padding, visible_span = self._right_padding_drag
                if abs(float(event.position().x()) - start_x) >= 4.0:
                    self._dragging = True
                if self._dragging:
                    padding = _right_padding_after_drag(
                        start_padding_bars=start_padding,
                        delta_px=float(event.position().x()) - start_x,
                        plot_width=max(float(plot_area.width()), 1.0),
                        visible_span_ms=visible_span,
                        display_step_ms=self._display_step_ms,
                    )
                    old_max = self._full_x_max
                    self._right_padding_bars = padding
                    self._full_x_max = float(self._display_times_ms[-1]) + _native_right_padding_ms(
                        self._display_step_ms, padding_bars=self._active_right_padding_bars(),
                    )
                    if self._future_projection:
                        future_bars = self._future_projection.get("bars", [])
                        future_end = max((float(item.get("time_ms", 0) or 0) for item in future_bars if isinstance(item, dict)), default=0.0)
                        self._full_x_max = max(self._full_x_max, future_end + self._display_step_ms)
                    current_min, current_max = self.current_x_range()
                    shift = self._full_x_max - old_max
                    new_min, new_max = self._bounded_x_range(current_min + shift, current_max + shift)
                    self._apply_x_range(new_min, new_max, emit_signal=True)
                    self._fit_y_axis_to_visible_range()
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
            if event.button() == Qt.MouseButton.LeftButton and self._area_replay_label_pressed:
                self._area_replay_label_pressed = False
                event.accept()
                return
            plot_area = self.chart().plotArea()
            was_padding_drag = self._right_padding_drag is not None
            if event.button() == Qt.MouseButton.LeftButton:
                self._last_pointer_pos = QPointF(event.position())
                if not was_padding_drag:
                    point = self.chart().mapToValue(event.position())
                    self.chartPointerReleased.emit(float(point.x()), float(point.y()))
                if (plot_area.contains(event.position()) and not self._dragging and not self._interaction_locked
                        and not was_padding_drag):
                    self.chartPointClicked.emit(float(point.x()), float(point.y()))
                if plot_area.contains(event.position()):
                    event.accept()
            self._press_pos = None
            self._pan_anchor_x = None
            self._right_padding_drag = None
            self._dragging = False
            super().mouseReleaseEvent(event)

        def mouseDoubleClickEvent(self, event) -> None:  # noqa: ANN001
            if event.button() == Qt.MouseButton.LeftButton and self._area_replay_label_at(event.position()):
                self._area_replay_label_pressed = True
                event.accept()
                return
            plot_area = self.chart().plotArea()
            if plot_area.contains(event.position()):
                point = self.chart().mapToValue(event.position())
                self.chartDoubleClicked.emit(float(point.x()), float(point.y()))
                event.accept()
                return
            self.reset_view()
            event.accept()

        def leaveEvent(self, event) -> None:  # noqa: ANN001
            self._hover_pos = None
            self._press_pos = None
            self._last_pointer_pos = None
            self._pan_anchor_x = None
            self._dragging = False
            self.hoverTimeChanged.emit(None)
            self._hide_hover_overlays()
            self.viewport().update()
            super().leaveEvent(event)

        def last_pointer_scene_pos(self) -> QPointF | None:
            if self._last_pointer_pos is None:
                return None
            return QPointF(float(self._last_pointer_pos.x()), float(self._last_pointer_pos.y()))

        def paintEvent(self, event) -> None:  # noqa: ANN001
            super().paintEvent(event)
            try:
                plot_area = self.chart().plotArea()
                if not self._candles or plot_area.width() <= 0 or plot_area.height() <= 0:
                    self._hide_hover_overlays()
                    return
                painter = QPainter(self.viewport())
                painter.setRenderHint(QPainter.RenderHint.Antialiasing, True)
                self._draw_latest_price_indicator(painter, plot_area)
                self._draw_atr_overlay(painter, plot_area)
                self._draw_volume_overlay(painter, plot_area)
                self._draw_trading_day_separators(painter, plot_area)
                self._draw_channel_overlays(painter, plot_area)
                self._draw_box_overlays(painter, plot_area)
                self._draw_workspace_ray_extensions(painter, plot_area)
                self._draw_workspace_rr_items(painter, plot_area)
                self._draw_preview_rr_item(painter, plot_area)
                self._draw_signal_markers(painter, plot_area)
                self._draw_history_trade_markers(painter, plot_area)
                self._draw_area_shape_replay_trades(painter, plot_area)
                self._draw_future_projection(painter, plot_area)
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
                        crosshair_y = self._external_hover_y(plot_area)
                        cross_pen = QPen(QColor(_CHART_CROSSHAIR_COLOR), 1)
                        cross_pen.setStyle(Qt.PenStyle.DashLine)
                        painter.setPen(cross_pen)
                        painter.drawLine(QPointF(snapped_x, plot_area.top()), QPointF(snapped_x, plot_area.bottom()))
                        if crosshair_y is not None:
                            painter.drawLine(QPointF(plot_area.left(), crosshair_y), QPointF(plot_area.right(), crosshair_y))
                        painter.setPen(QPen(candle_color, 2))
                        painter.setBrush(QColor("#ffffff"))
                        painter.drawEllipse(QPointF(snapped_x, marker_y), 4.0, 4.0)
                        painter.end()
                        self._update_hover_overlays(
                            bounds=plot_area,
                            anchor=QPointF(snapped_x, crosshair_y if crosshair_y is not None else marker_y),
                            candle_color=candle_color,
                            price_text=self._format_hover_value(self._external_hover_price) if crosshair_y is not None else "",
                            time_text=_format_chart_time(int(candle["time"])),
                            tooltip_lines=(
                                f"联动定位 {_format_chart_time(int(candle_time))}",
                                _format_chart_time(int(candle["time"])),
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
                atr_cursor_value = (
                    self._atr_values[hover_index]
                    if self._show_atr and hover_index < len(self._atr_values)
                    else None
                )
                atr_geometry = self._atr_overlay_geometry(plot_area)
                pointer_in_atr = bool(
                    atr_geometry is not None
                    and atr_geometry[0] <= hover_y <= atr_geometry[1]
                    and atr_cursor_value is not None
                )
                crosshair_y = (
                    self._atr_overlay_y(float(atr_cursor_value), plot_area)
                    if pointer_in_atr and atr_cursor_value is not None
                    else hover_y
                )
                if crosshair_y is None:
                    crosshair_y = hover_y
                cross_pen = QPen(QColor(_CHART_CROSSHAIR_COLOR), 1)
                cross_pen.setStyle(Qt.PenStyle.DashLine)
                painter.setPen(cross_pen)
                painter.drawLine(QPointF(snapped_x, plot_area.top()), QPointF(snapped_x, plot_area.bottom()))
                painter.drawLine(QPointF(plot_area.left(), crosshair_y), QPointF(plot_area.right(), crosshair_y))
                painter.setPen(QPen(candle_color, 2))
                painter.setBrush(QColor("#ffffff"))
                painter.drawEllipse(QPointF(snapped_x, marker_y), 4.0, 4.0)
                painter.end()
                self._update_hover_overlays(
                    bounds=plot_area,
                    anchor=QPointF(snapped_x, crosshair_y),
                    candle_color=candle_color,
                    price_text=self._format_hover_value(float(atr_cursor_value)) if pointer_in_atr else self._format_hover_value(hover_value),
                    time_text=_format_chart_time(int(candle["time"])),
                    tooltip_lines=(
                        _format_chart_time(int(candle["time"])),
                        f"O {self._format_hover_value(float(candle['open']))}  H {self._format_hover_value(float(candle['high']))}",
                        f"L {self._format_hover_value(float(candle['low']))}  C {self._format_hover_value(float(candle['close']))}",
                        f"成交量 {self._format_hover_value(float(candle['volume']))}",
                        *( (f"ATR 10 {self._format_hover_value(float(atr_cursor_value))}",) if atr_cursor_value is not None else () ),
                        *self._box_tooltip_lines(hover_index),
                        *self._signal_tooltip_lines(hover_index),
                    ),
                )
            except Exception:
                self._hide_hover_overlays()

        def _draw_latest_price_indicator(self, painter: QPainter, plot_area: QRectF) -> None:
            """Draw a small right-axis price tag without adding a chart series."""
            price = self._latest_price
            if price is None or self._axis_y is None or not math.isfinite(price) or price <= 0:
                return
            min_y = float(self._axis_y.min())
            max_y = float(self._axis_y.max())
            if not math.isfinite(min_y) or not math.isfinite(max_y) or max_y <= min_y:
                return
            y = self._y_for_value(price, plot_area)
            viewport = self.viewport().rect()
            text = self._format_hover_value(price)
            font = painter.font()
            font.setPointSize(8)
            font.setBold(True)
            painter.setFont(font)
            metrics = painter.fontMetrics()
            padding_x = 7.0
            padding_y = 3.0
            width = float(metrics.horizontalAdvance(text)) + padding_x * 2.0
            height = float(metrics.height()) + padding_y * 2.0
            right = min(float(viewport.right()) - 3.0, float(plot_area.right()) + width + 5.0)
            left = max(float(plot_area.right()) + 2.0, right - width)
            top = _clamp(y - height / 2.0, float(plot_area.top()) + 2.0, float(plot_area.bottom()) - height - 2.0)
            last_close = float(self._candles[-1].get("close", price)) if self._candles else price
            color = QColor(_CHART_UP_COLOR if price >= last_close else _CHART_DOWN_COLOR)
            painter.setPen(QPen(color, 1))
            painter.setBrush(QColor(15, 23, 42, 235))
            painter.drawRoundedRect(QRectF(left, top, right - left, height), 4.0, 4.0)
            painter.setPen(color)
            painter.drawText(QRectF(left + padding_x, top, width - padding_x * 2.0, height), Qt.AlignmentFlag.AlignCenter, text)

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
            if self._show_atr and candle_index < len(self._atr_values):
                atr_value = self._atr_values[candle_index]
                if atr_value is not None:
                    indicator_segments.extend(
                        [
                            ("ATR 10 ", QColor(_CHART_AXIS_TEXT_COLOR)),
                            (self._format_hover_value(float(atr_value)), QColor("#f59e0b")),
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
            text_metrics = painter.fontMetrics()
            min_volume_text_gap = float(text_metrics.height()) + 6.0
            if self._candles and self._axis_y is not None:
                left_index, right_index = self._visible_index_range()
                visible = self._candles[left_index : right_index + 1] if right_index >= left_index else []
                if not visible:
                    visible = list(self._candles)
                try:
                    min_visible = min(float(item["low"]) for item in visible)
                    min_label_y = self._y_for_value(min_visible, plot_area)
                    collision_floor = min_label_y - min_volume_text_gap
                    volume_text_y = _clamp(volume_text_y, 0.0, collision_floor)
                except Exception:
                    pass
            volume_band_top = float(plot_area.bottom()) - volume_band_height
            min_text_y = volume_band_top + 4.0
            max_text_y = float(plot_area.bottom()) - 4.0
            volume_text_y = _clamp(volume_text_y, min_text_y, max_text_y)
            volume_segments = [
                ("Volume ", QColor(_CHART_AXIS_TEXT_COLOR)),
                (_format_compact_number(float(candle.get("volume", 0.0) or 0.0)), delta_color),
            ]
            volume_text_width = sum(float(text_metrics.horizontalAdvance(text)) for text, _ in volume_segments)
            volume_text_height = float(text_metrics.height())
            text_padding = 5.0
            volume_label_rect = QRectF(
                base_x - 2.0,
                volume_text_y - volume_text_height - 1.0,
                volume_text_width + 4.0,
                volume_text_height + 2.0,
            ).adjusted(-text_padding, 0.0, text_padding, 1.0)
            painter.save()
            painter.setRenderHint(QPainter.RenderHint.Antialiasing, True)
            painter.setPen(QPen(QColor(_CHART_AXIS_TEXT_COLOR), 1))
            painter.setBrush(QColor(11, 15, 20, 190))
            painter.drawRoundedRect(volume_label_rect, 4.0, 4.0)
            painter.restore()
            self._draw_colored_segments(
                painter,
                base_x,
                volume_text_y,
                volume_segments,
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
            external_time = int(self._external_hover_time)
            # Do not clamp a linked cursor to the first/last candle when the
            # source chart is outside this chart's data or visible range.
            # In that case this panel should simply have no linked cursor.
            try:
                first_time = int(self._candles[0]["time"])
                last_time = int(self._candles[-1]["time"])
            except (KeyError, TypeError, ValueError):
                return None
            if external_time < first_time or external_time > last_time:
                return None
            hover_index = self._index_for_candle_time(self._external_hover_time)
            candle = self._candles[hover_index]
            if hover_index >= len(self._display_times_ms):
                return None
            display_time = float(self._display_times_ms[hover_index])
            start_x, end_x = self.current_x_range()
            if display_time < start_x or display_time > end_x:
                return None
            snapped_x = self._x_for_index(hover_index, plot_area)
            return candle, hover_index, external_time, snapped_x

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
            for marker in self._history_trade_markers:
                index = int(marker.get("index", -1))
                if not left_index <= index <= right_index:
                    continue
                try:
                    price = float(marker.get("price", 0.0) or 0.0)
                except (TypeError, ValueError):
                    continue
                if price > 0.0:
                    lows.append(price)
                    highs.append(price)
            if len(self._area_shape_replay_trades) == 1:
                record = self._area_shape_replay_trades[0]
                record_visible = False
                for key in ("signal_ts", "entry_ts", "exit_ts"):
                    try:
                        timestamp = int(record.get(key, 0) or 0)
                    except (TypeError, ValueError):
                        continue
                    if timestamp > 0 and left_index <= self._index_for_candle_time(timestamp // 1000) <= right_index:
                        record_visible = True
                        break
                if record_visible:
                    for key in ("entry", "stop", "exit"):
                        try:
                            price = float(record.get(key, 0) or 0)
                        except (TypeError, ValueError):
                            continue
                        if math.isfinite(price) and price > 0:
                            lows.append(price)
                            highs.append(price)
            min_price = min(lows) if lows else self._full_y_min
            max_price = max(highs) if highs else self._full_y_max
            top_padding, bottom_padding = _compute_axis_y_padding(min_price, max_price, show_atr=self._show_atr)
            axis_y.setRange(min_price - bottom_padding, max_price + top_padding)

        def _visible_index_range(self) -> tuple[int, int]:
            if not self._candles:
                return 0, 0
            start_x, end_x = self.current_x_range()
            left_index = self._index_for_display_x(start_x, mode="floor")
            right_index = self._index_for_display_x(end_x, mode="ceil")
            return left_index, right_index

        def _draw_trading_day_separators(self, painter: QPainter, plot_area: QRectF) -> None:
            if not self._trading_day_spans or not self._display_times_ms:
                return
            start_x, end_x = self.current_x_range()
            span = max(end_x - start_x, 1.0)
            painter.save()
            painter.setClipRect(plot_area)
            pen = QPen(QColor("#7189a6"), 1.2)
            pen.setStyle(Qt.PenStyle.DashLine)
            painter.setPen(pen)
            font = painter.font()
            font.setPointSize(9)
            font.setBold(True)
            painter.setFont(font)
            metrics = painter.fontMetrics()
            for day, first, end in self._trading_day_spans:
                # Candles are centred on their opening time; split between 23:00
                # and 00:00 (20:00 and 00:00 on 4H), never through a candle body.
                boundary = self._display_times_ms[first] - self._display_step_ms / 2
                day_end = self._display_times_ms[end - 1] + self._display_step_ms / 2
                if day_end < start_x or boundary > end_x:
                    continue
                x = plot_area.left() + (boundary - start_x) / span * plot_area.width()
                right = plot_area.left() + (day_end - start_x) / span * plot_area.width()
                if start_x <= boundary <= end_x:
                    painter.setPen(pen)
                    painter.drawLine(QPointF(x, plot_area.top()), QPointF(x, plot_area.bottom()))
                if end == len(self._candles) and start_x <= day_end <= end_x:
                    last_open = int(self._candles[-1]["time"])
                    if datetime.fromtimestamp(last_open + self._display_step_ms / 1000).date() != datetime.fromtimestamp(last_open).date():
                        painter.setPen(pen)
                        painter.drawLine(QPointF(right, plot_area.top()), QPointF(right, plot_area.bottom()))
                label_width = metrics.horizontalAdvance(day) + 12
                left = max(x, plot_area.left())
                right = min(right, plot_area.right())
                if right - left < label_width + 6:
                    continue  # Keep every boundary; omit labels that would overlap.
                rect = QRectF((left + right - label_width) / 2,
                              plot_area.bottom() - metrics.height() - 5,
                              label_width, metrics.height() + 4)
                painter.setPen(QPen(QColor("#7189a6"), 1))
                painter.setBrush(QColor("#111c2a"))
                painter.drawRoundedRect(rect, 3, 3)
                painter.setPen(QColor("#dbeafe"))
                painter.drawText(rect, Qt.AlignmentFlag.AlignCenter, day)
            painter.restore()

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

        def _atr_overlay_geometry(self, plot_area: QRectF) -> tuple[float, float, float, float, float, float] | None:
            if not self._show_atr or not self._atr_values or not self._candles:
                return None
            left_index, right_index = self._visible_index_range()
            if right_index < left_index:
                return None
            volume_band_height = max(42.0, float(plot_area.height()) * _VOLUME_OVERLAY_HEIGHT_RATIO)
            volume_band_top = float(plot_area.bottom()) - volume_band_height
            atr_band_height = max(36.0, float(plot_area.height()) * _ATR_OVERLAY_HEIGHT_RATIO)
            atr_band_bottom = volume_band_top - 1.0
            atr_band_top = atr_band_bottom - atr_band_height
            if atr_band_top <= float(plot_area.top()) + 8.0:
                return None
            visible_values = [
                float(self._atr_values[index])
                for index in range(left_index, min(right_index + 1, len(self._atr_values)))
                if self._atr_values[index] is not None
            ]
            if not visible_values:
                return None
            value_min = min(visible_values)
            value_max = max(visible_values)
            value_span = max(value_max - value_min, max(value_max, 1e-8) * 0.08)
            value_min -= value_span * 0.08
            value_max += value_span * 0.08
            return atr_band_top, atr_band_bottom, atr_band_top + 15.0, atr_band_bottom - 3.0, value_min, value_max

        def _atr_overlay_y(self, value: float, plot_area: QRectF) -> float | None:
            geometry = self._atr_overlay_geometry(plot_area)
            if geometry is None:
                return None
            _band_top, _band_bottom, draw_top, draw_bottom, value_min, value_max = geometry
            return draw_bottom - ((float(value) - value_min) / (value_max - value_min)) * (draw_bottom - draw_top)

        def _draw_atr_overlay(self, painter: QPainter, plot_area: QRectF) -> None:
            if not self._show_atr or not self._atr_values or not self._candles:
                return
            left_index, right_index = self._visible_index_range()
            if right_index < left_index:
                return
            geometry = self._atr_overlay_geometry(plot_area)
            if geometry is None:
                return
            atr_band_top, atr_band_bottom, draw_top, draw_bottom, value_min, value_max = geometry
            atr_band_height = atr_band_bottom - atr_band_top
            painter.save()
            painter.fillRect(
                QRectF(float(plot_area.left()), atr_band_top, float(plot_area.width()), atr_band_height),
                QColor(11, 15, 20, 205),
            )
            painter.setPen(QPen(QColor(_CHART_GRID_COLOR), 1))
            painter.drawLine(QPointF(plot_area.left(), atr_band_top), QPointF(plot_area.right(), atr_band_top))
            painter.setPen(QPen(QColor("#f59e0b"), 1.5))
            previous_point: QPointF | None = None
            for index in range(left_index, min(right_index + 1, len(self._atr_values), len(self._candles))):
                value = self._atr_values[index]
                if value is None:
                    previous_point = None
                    continue
                y = draw_bottom - ((float(value) - value_min) / (value_max - value_min)) * (draw_bottom - draw_top)
                point = QPointF(self._x_for_index(index, plot_area), y)
                if previous_point is not None:
                    painter.drawLine(previous_point, point)
                previous_point = point
            latest = self._atr_values[min(right_index, len(self._atr_values) - 1)]
            selected_index: int | None = None
            hover_context = self._resolve_hover_context()
            if hover_context is not None:
                selected_index = hover_context[1]
            elif self._external_hover_time is not None:
                selected_index = self._index_for_candle_time(self._external_hover_time)
            selected = (
                self._atr_values[selected_index]
                if selected_index is not None and 0 <= selected_index < len(self._atr_values)
                else latest
            )
            if selected_index is not None and selected is not None:
                selected_y = self._atr_overlay_y(float(selected), plot_area)
                if selected_y is not None:
                    painter.setPen(QPen(QColor("#fbbf24"), 1.0, Qt.PenStyle.DashLine))
                    painter.drawLine(QPointF(plot_area.left(), selected_y), QPointF(plot_area.right(), selected_y))
                    painter.setPen(QPen(QColor("#fbbf24"), 1.5))
                    painter.setBrush(QColor("#0b0f14"))
                    painter.drawEllipse(QPointF(self._x_for_index(selected_index, plot_area), selected_y), 4.0, 4.0)
            label = f"ATR 10  {float(selected):,.2f}" if selected is not None else "ATR 10"
            painter.setPen(QPen(QColor("#fbbf24")))
            painter.drawText(QPointF(float(plot_area.left()) + 8.0, atr_band_top + 12.0), label)
            painter.restore()

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

        def _draw_history_trade_markers(self, painter: QPainter, plot_area: QRectF) -> None:
            if not self._history_trade_markers or not self._candles:
                return
            left_index, right_index = self._visible_index_range()
            font = painter.font()
            font.setPointSize(8)
            font.setBold(True)
            painter.setFont(font)
            metrics = painter.fontMetrics()
            def marker_timestamp(marker: dict[str, Any]) -> int:
                try:
                    return int(marker.get("timestamp", 0) or 0)
                except (TypeError, ValueError):
                    return 0

            def direction_color(direction: str) -> QColor:
                """Keep both legs of a trade pair in the same direction colour."""

                return QColor(_HISTORY_TRADE_SHORT_COLOR if direction == "short" else _HISTORY_TRADE_LONG_COLOR)

            def outcome_color(marker: dict[str, Any], result: tuple[dict[str, Any], float] | None) -> QColor:
                """Return the close result colour without changing the pair colour."""

                if result is not None:
                    result_percent = result[1]
                    if result_percent > 0.0:
                        return QColor(_HISTORY_TRADE_PROFIT_COLOR)
                    if result_percent < 0.0:
                        return QColor(_HISTORY_TRADE_LOSS_COLOR)
                    return QColor(_HISTORY_TRADE_FLAT_COLOR)
                try:
                    realized_pnl = float(marker.get("realized_pnl"))
                except (TypeError, ValueError):
                    return QColor(_HISTORY_TRADE_FLAT_COLOR)
                if realized_pnl > 0.0:
                    return QColor(_HISTORY_TRADE_PROFIT_COLOR)
                if realized_pnl < 0.0:
                    return QColor(_HISTORY_TRADE_LOSS_COLOR)
                return QColor(_HISTORY_TRADE_FLAT_COLOR)

            ordered_markers = sorted(
                (item for item in self._history_trade_markers if isinstance(item, dict)),
                key=lambda item: (
                    marker_timestamp(item),
                    str(item.get("event", "") or "").strip().lower() != "open",
                ),
            )
            pair_segments, exit_results, pending_entries = _pair_kline_trade_markers(ordered_markers)

            visible_markers: list[tuple[int, float, dict[str, Any]]] = []
            for marker in ordered_markers:
                index = int(marker.get("index", -1))
                if index < left_index or index > right_index or index >= len(self._candles):
                    continue
                try:
                    price = float(marker.get("price", 0.0) or 0.0)
                except (TypeError, ValueError):
                    continue
                if price > 0.0:
                    visible_markers.append((index, price, marker))

            # Connect each matched opening and closing trade with profit/loss colour.
            for entry, exit_marker, result_percent in pair_segments:
                entry_index = int(entry.get("index", -1))
                exit_index = int(exit_marker.get("index", -1))
                if not (left_index <= entry_index <= right_index and left_index <= exit_index <= right_index):
                    continue
                try:
                    entry_price = float(entry.get("price", 0.0) or 0.0)
                    exit_price = float(exit_marker.get("price", 0.0) or 0.0)
                except (TypeError, ValueError):
                    continue
                if entry_price <= 0.0 or exit_price <= 0.0:
                    continue
                connector_color = QColor("#22c55e" if result_percent > 0.0 else "#ef4444" if result_percent < 0.0 else "#94a3b8")
                connector_color.setAlpha(190)
                painter.setPen(QPen(connector_color, 2))
                entry_x = self._x_for_index(entry_index, plot_area)
                exit_x = self._x_for_index(exit_index, plot_area)
                gap = 10.0
                if exit_x >= entry_x:
                    entry_x += gap
                    exit_x -= gap
                else:
                    entry_x -= gap
                    exit_x += gap
                painter.drawLine(
                    QPointF(entry_x, self._y_for_value(entry_price, plot_area)),
                    QPointF(exit_x, self._y_for_value(exit_price, plot_area)),
                )

            # An unmatched opening is still held.  Retain the latest local rule: a
            # short blue dashed price guide rather than inventing a closing result.
            for direction in ("long", "short"):
                for entry in pending_entries[direction]:
                    index = int(entry.get("index", -1))
                    if not left_index <= index <= right_index:
                        continue
                    try:
                        price = float(entry.get("price", 0.0) or 0.0)
                    except (TypeError, ValueError):
                        continue
                    if price <= 0.0:
                        continue
                    guide_color = direction_color(direction)
                    guide_color.setAlpha(155)
                    guide_pen = QPen(guide_color, 1)
                    guide_pen.setStyle(Qt.PenStyle.DashLine)
                    painter.setPen(guide_pen)
                    start_x = self._x_for_index(index, plot_area)
                    end_x = self._x_for_index(min(index + 5, len(self._candles) - 1), plot_area)
                    painter.drawLine(QPointF(start_x, self._y_for_value(price, plot_area)), QPointF(max(start_x, end_x), self._y_for_value(price, plot_area)))

            occupied_label_rects: list[QRectF] = []
            text_height = float(metrics.height())
            text_ascent = float(metrics.ascent())
            padding_x = 6.0
            padding_y = 3.0
            for index, price, marker in visible_markers:
                event = str(marker.get("event", "") or "").strip().lower()
                direction = str(marker.get("direction", "") or "").strip().lower()
                is_sell = (event == "open" and direction == "short") or (event == "close" and direction == "long")
                result = exit_results.get(id(marker))
                color = direction_color(direction)
                result_color = outcome_color(marker, result) if event == "close" else None
                candle = self._candles[index]
                candle_top = self._y_for_value(float(candle["high"]), plot_area)
                candle_bottom = self._y_for_value(float(candle["low"]), plot_area)
                x_center = self._x_for_index(index, plot_area)
                time_text = _format_chart_time(marker_timestamp(marker))
                trade_label = (
                    "开多" if event == "open" and direction == "long" else
                    "开空" if event == "open" and direction == "short" else
                    "平多" if event == "close" and direction == "long" else
                    "平空"
                )
                label_lines = [trade_label, self._format_hover_value(price)]
                outcome_line_indices: set[int] = set()
                if event == "close" and result is not None:
                    outcome_text = "盈利" if result[1] > 0.0 else "亏损" if result[1] < 0.0 else "持平"
                    label_lines.append(f"{outcome_text} {result[1]:+.2f}%")
                    outcome_line_indices.add(len(label_lines) - 1)
                    pnl = marker.get("realized_pnl")
                    try:
                        pnl_value = float(pnl) if pnl is not None else None
                    except (TypeError, ValueError):
                        pnl_value = None
                    if pnl_value is not None:
                        currency = str(marker.get("pnl_currency", "") or "").strip().upper()
                        label_lines.append(f"盈亏 {pnl_value:+.2f}{(' ' + currency) if currency else ''}")
                        outcome_line_indices.add(len(label_lines) - 1)
                label_lines.append(time_text)
                label_width = max(float(metrics.horizontalAdvance(line)) for line in label_lines) + (padding_x * 2.0)
                label_height = (text_height * len(label_lines)) + (padding_y * 2.0)
                arrow_tip = candle_top if is_sell else candle_bottom
                arrow_length = 26.0
                if is_sell:
                    painter.setPen(QPen(color, 1.5))
                    painter.drawLine(QPointF(x_center, max(arrow_tip - arrow_length, float(plot_area.top()) + 3.0)), QPointF(x_center, arrow_tip - 7.0))
                    triangle = QPolygonF((QPointF(x_center - 5.0, arrow_tip - 7.0), QPointF(x_center + 5.0, arrow_tip - 7.0), QPointF(x_center, arrow_tip)))
                else:
                    painter.setPen(QPen(color, 1.5))
                    painter.drawLine(QPointF(x_center, min(arrow_tip + arrow_length, float(plot_area.bottom()) - 3.0)), QPointF(x_center, arrow_tip + 7.0))
                    triangle = QPolygonF((QPointF(x_center - 5.0, arrow_tip + 7.0), QPointF(x_center + 5.0, arrow_tip + 7.0), QPointF(x_center, arrow_tip)))
                painter.setPen(Qt.PenStyle.NoPen)
                painter.setBrush(color)
                painter.drawPolygon(triangle)

                label_rect: QRectF | None = None
                text_x = x_center + 8.0
                for side in (-1 if is_sell else 1, 1 if is_sell else -1):
                    for level in range(12):
                        candidate_top = (
                            arrow_tip - 6.0 - label_height - (level * (label_height + 8.0))
                            if side < 0
                            else arrow_tip + 6.0 + (level * (label_height + 8.0))
                        )
                        candidate_top = _clamp(
                            candidate_top,
                            float(plot_area.top()) + 2.0,
                            float(plot_area.bottom()) - label_height - 2.0,
                        )
                        for candidate_x in (
                            _clamp(x_center + 8.0, float(plot_area.left()) + 2.0, float(plot_area.right()) - label_width - 2.0),
                            _clamp(x_center - label_width - 8.0, float(plot_area.left()) + 2.0, float(plot_area.right()) - label_width - 2.0),
                        ):
                            candidate = QRectF(candidate_x, candidate_top, label_width, label_height)
                            if any(candidate.intersects(existing) for existing in occupied_label_rects):
                                continue
                            label_rect = candidate
                            text_x = candidate_x + padding_x
                            break
                        if label_rect is not None:
                            break
                    if label_rect is not None:
                        break
                if label_rect is None:
                    fallback_top = _clamp(
                        arrow_tip - 6.0 - label_height if is_sell else arrow_tip + 6.0,
                        float(plot_area.top()) + 2.0,
                        float(plot_area.bottom()) - label_height - 2.0,
                    )
                    fallback_x = _clamp(x_center + 8.0, float(plot_area.left()) + 2.0, float(plot_area.right()) - label_width - 2.0)
                    label_rect = QRectF(fallback_x, fallback_top, label_width, label_height)
                    text_x = fallback_x + padding_x
                occupied_label_rects.append(label_rect)
                background = QColor(_CHART_BACKGROUND_COLOR)
                background.setAlpha(218)
                painter.setPen(QPen(color, 1))
                painter.setBrush(background)
                painter.drawRoundedRect(label_rect, 3.0, 3.0)
                if result_color is not None:
                    # A narrow outcome stripe keeps the long/short pair colour
                    # intact while making close profit/loss immediately visible.
                    painter.setPen(Qt.PenStyle.NoPen)
                    painter.setBrush(result_color)
                    painter.drawRoundedRect(
                        QRectF(label_rect.left(), label_rect.top(), 4.0, label_rect.height()),
                        2.0,
                        2.0,
                    )
                for line_index, line in enumerate(label_lines):
                    line_color = color
                    if result_color is not None and line_index in outcome_line_indices:
                        line_color = result_color
                    painter.setPen(line_color)
                    painter.drawText(
                        QPointF(text_x, float(label_rect.top()) + padding_y + text_ascent + (line_index * text_height)),
                        line,
                    )

        def _draw_area_shape_replay_trades(self, painter: QPainter, plot_area: QRectF) -> None:
            self._area_replay_label_rects = []
            if not self._area_shape_replay_trades or not self._candles:
                return
            start_x, end_x = self.current_x_range()
            replay_colors = {
                ("trade", "long"): QColor("#4ade80"),
                ("trade", "short"): QColor("#f87171"),
                ("opportunity", "long"): QColor("#38bdf8"),
                ("opportunity", "short"): QColor("#c4b5fd"),
            }
            font = painter.font()
            font.setPointSize(8)
            font.setBold(True)
            painter.setFont(font)
            metrics = painter.fontMetrics()

            for trade in self._area_shape_replay_trades:
                try:
                    record_type = str(trade.get("record_type", "trade") or "trade").strip().lower()
                    signal_ts = int(trade.get("signal_ts", 0) or 0)
                    entry_ts = int(trade.get("entry_ts", 0) or 0)
                    exit_ts = int(trade.get("exit_ts", 0) or 0)
                    entry_price = float(trade.get("entry", 0) or 0)
                    exit_price = float(trade.get("exit", 0) or 0)
                    stop_price = float(trade.get("stop", 0) or 0)
                    net_r = float(trade.get("r", 0) or 0)
                    space_r = float(trade.get("space_r", 0) or 0)
                except (TypeError, ValueError):
                    continue
                if entry_ts <= 0 or entry_price <= 0:
                    continue
                is_opportunity = record_type == "opportunity"
                marker_ts = signal_ts if is_opportunity and signal_ts > 0 else entry_ts
                if not is_opportunity and (exit_ts <= 0 or exit_price <= 0):
                    continue
                marker_index = self._index_for_candle_time(marker_ts // 1000)
                entry_index = self._index_for_candle_time(entry_ts // 1000)
                if not (0 <= entry_index < len(self._candles) and 0 <= marker_index < len(self._candles)):
                    continue
                if is_opportunity:
                    marker_display_x = self._display_times_ms[marker_index]
                    if marker_display_x < start_x or marker_display_x > end_x:
                        continue
                    exit_index = -1
                else:
                    exit_index = self._index_for_candle_time(exit_ts // 1000)
                    if not (0 <= exit_index < len(self._candles)):
                        continue
                    entry_display_x = self._display_times_ms[entry_index]
                    exit_display_x = self._display_times_ms[exit_index]
                    if exit_display_x < start_x or entry_display_x > end_x:
                        continue

                # Show the risk boundary for at most eight candles. Opportunities
                # have no exit yet; completed/held trades also keep the line
                # within their active interval.
                if stop_price > 0 and self._axis_y is not None:
                    axis_low = float(self._axis_y.min())
                    axis_high = float(self._axis_y.max())
                    if axis_low <= stop_price <= axis_high:
                        stop_y = self._y_for_value(stop_price, plot_area)
                        stop_start_x = self._x_for_index(entry_index, plot_area)
                        trade_end_index = exit_index if not is_opportunity else len(self._candles) - 1
                        stop_end_index = min(entry_index + 8, trade_end_index)
                        stop_end_x = self._x_for_index(stop_end_index, plot_area)
                        if stop_end_x > stop_start_x:
                            stop_color = QColor("#fb7185")
                            stop_pen = QPen(stop_color, 1.4)
                            stop_pen.setStyle(Qt.PenStyle.DashLine)
                            painter.setPen(stop_pen)
                            painter.drawLine(QPointF(stop_start_x, stop_y), QPointF(stop_end_x, stop_y))
                            stop_label = f"{'多' if str(trade.get('side', 'long')).lower() == 'long' else '空'}止损 {self._format_hover_value(stop_price)}"
                            label_width = float(metrics.horizontalAdvance(stop_label)) + 10.0
                            label_height = float(metrics.height()) + 6.0
                            label_x = _clamp(
                                stop_start_x + 5.0, float(plot_area.left()) + 2.0,
                                float(plot_area.right()) - label_width - 2.0,
                            )
                            label_y = _clamp(
                                stop_y - label_height - 2.0,
                                float(plot_area.top()) + 2.0,
                                float(plot_area.bottom()) - label_height - 2.0,
                            )
                            stop_rect = QRectF(label_x, label_y, label_width, label_height)
                            self._area_replay_label_rects.append(stop_rect)
                            painter.setPen(QPen(stop_color, 1))
                            painter.setBrush(QColor("#111827"))
                            painter.drawRoundedRect(stop_rect, 3.0, 3.0)
                            painter.setPen(stop_color.lighter(125))
                            painter.drawText(
                                QPointF(stop_rect.left() + 5.0, stop_rect.top() + metrics.ascent() + 3.0),
                                stop_label,
                            )

                side = str(trade.get("side", "long")).strip().lower()
                side_label = "多" if side == "long" else "空"
                replay_color = replay_colors.get(
                    ("opportunity" if is_opportunity else "trade", side),
                    QColor("#eab308"),
                )
                if is_opportunity:
                    x_pos = self._x_for_index(marker_index, plot_area)
                    y_pos = self._y_for_value(entry_price, plot_area)
                    if side == "long":
                        marker = QPolygonF((
                            QPointF(x_pos, y_pos - 7), QPointF(x_pos + 7, y_pos + 6),
                            QPointF(x_pos - 7, y_pos + 6),
                        ))
                    else:
                        marker = QPolygonF((
                            QPointF(x_pos, y_pos + 7), QPointF(x_pos + 7, y_pos - 6),
                            QPointF(x_pos - 7, y_pos - 6),
                        ))
                    painter.setPen(QPen(replay_color, 2))
                    painter.setBrush(QColor("#111827"))
                    painter.drawPolygon(marker)
                    label = f"潜在{side_label} · 空间 {space_r:.2f}R"
                    label_width = float(metrics.horizontalAdvance(label)) + 10.0
                    label_height = float(metrics.height()) + 6.0
                    label_x = _clamp(
                        x_pos + 9.0, float(plot_area.left()) + 2.0,
                        float(plot_area.right()) - label_width - 2.0,
                    )
                    label_y = _clamp(
                        y_pos - label_height - 8.0 if side == "short" else y_pos + 8.0,
                        float(plot_area.top()) + 2.0,
                        float(plot_area.bottom()) - label_height - 2.0,
                    )
                    rect = QRectF(label_x, label_y, label_width, label_height)
                    self._area_replay_label_rects.append(rect)
                    painter.setPen(QPen(replay_color, 1))
                    painter.setBrush(QColor("#111827"))
                    painter.drawRoundedRect(rect, 3.0, 3.0)
                    painter.setPen(replay_color.lighter(145))
                    painter.drawText(QPointF(rect.left() + 5.0, rect.top() + metrics.ascent() + 3.0), label)
                    continue

                entry_x = self._x_for_index(entry_index, plot_area)
                exit_x = self._x_for_index(exit_index, plot_area)
                entry_y = self._y_for_value(entry_price, plot_area)
                exit_y = self._y_for_value(exit_price, plot_area)
                replay_pen = QPen(replay_color, 2)
                replay_pen.setStyle(Qt.PenStyle.DashLine)
                painter.setPen(replay_pen)
                painter.drawLine(QPointF(entry_x, entry_y), QPointF(exit_x, exit_y))

                for x_pos, price, label, above in (
                    (entry_x, entry_price, f"回测{side_label}入场", side == "short"),
                    (
                        exit_x, exit_price,
                        f"回测{side_label}{'持仓中' if str(trade.get('outcome', '')) == '持仓中' else '出场'} {net_r:+.2f}R",
                        side == "long",
                    ),
                ):
                    y_pos = self._y_for_value(price, plot_area)
                    diamond = QPolygonF((
                        QPointF(x_pos, y_pos - 6), QPointF(x_pos + 6, y_pos),
                        QPointF(x_pos, y_pos + 6), QPointF(x_pos - 6, y_pos),
                    ))
                    painter.setPen(QPen(replay_color, 1.5))
                    painter.setBrush(QColor("#111827"))
                    painter.drawPolygon(diamond)
                    label_width = float(metrics.horizontalAdvance(label)) + 10.0
                    label_height = float(metrics.height()) + 6.0
                    label_x = _clamp(
                        x_pos + 8.0, float(plot_area.left()) + 2.0,
                        float(plot_area.right()) - label_width - 2.0,
                    )
                    label_y = y_pos - label_height - 8.0 if above else y_pos + 8.0
                    label_y = _clamp(
                        label_y, float(plot_area.top()) + 2.0,
                        float(plot_area.bottom()) - label_height - 2.0,
                    )
                    label_rect = QRectF(label_x, label_y, label_width, label_height)
                    self._area_replay_label_rects.append(label_rect)
                    painter.setPen(QPen(replay_color, 1))
                    painter.setBrush(QColor("#111827"))
                    painter.drawRoundedRect(label_rect, 3.0, 3.0)
                    painter.setPen(replay_color.lighter(145))
                    painter.drawText(
                        QPointF(label_rect.left() + 5.0, label_rect.top() + metrics.ascent() + 3.0),
                        label,
                    )

        def _draw_channel_overlays(self, painter: QPainter, plot_area: QRectF) -> None:
            if not self._channel_overlays or not self._candles:
                return
            left_index, right_index = self._visible_index_range()
            for overlay in self._channel_overlays:
                start_index = int(overlay.get("start_index", -1))
                end_index = int(overlay.get("end_index", -1))
                if start_index < 0 or end_index < start_index or end_index < left_index or start_index > right_index:
                    continue
                span = max(1, end_index - start_index)

                def _price_at(prefix: str, index: int) -> float:
                    start = float(overlay.get(f"{prefix}_start", 0.0) or 0.0)
                    end = float(overlay.get(f"{prefix}_end", 0.0) or 0.0)
                    return start + (end - start) * ((index - start_index) / span)

                visible_start = max(left_index, start_index)
                visible_end = min(right_index, end_index)
                outline = QColor(str(overlay.get("outline", "#2563eb")))
                outline.setAlpha(230)
                pen = QPen(outline, 2)
                pen.setCosmetic(True)
                painter.setPen(pen)
                for prefix in ("upper", "lower"):
                    painter.drawLine(
                        QPointF(self._x_for_index(visible_start, plot_area), self._y_for_value(_price_at(prefix, visible_start), plot_area)),
                        QPointF(self._x_for_index(visible_end, plot_area), self._y_for_value(_price_at(prefix, visible_end), plot_area)),
                    )

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
                    f"{'自动' if mode == 'current' else '历史'}箱体 {lower:.2f}-{upper:.2f} | 触点 {touches}{trend_text}"
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
                score_detail = str(marker.get("score_detail", "") or "")
                lines.append(
                    f"信号 {label} | 均线距离 {distance:.2f}% | 评分 {score}"
                    + (f"（{score_detail}）" if score_detail else "")
                )
            return tuple(lines)

        def _draw_future_projection(self, painter: QPainter, plot_area: QRectF) -> None:
            projection = self._future_projection
            bars = projection.get("bars", []) if isinstance(projection, dict) else []
            if not isinstance(bars, list) or not bars:
                return
            ordered = [item for item in bars if isinstance(item, dict)]
            if not ordered:
                return
            step_width = float(plot_area.width()) * self._display_step_ms / max(self.current_x_range()[1] - self.current_x_range()[0], 1.0)
            body_width = max(1.0, min(18.0, step_width * 0.72))
            first_x = self._x_for_display_value(float(ordered[0].get("time_ms", 0) or 0), plot_area)
            last_x = self._x_for_display_value(float(ordered[-1].get("time_ms", 0) or 0), plot_area)
            boundary_x = first_x - step_width * 0.5
            painter.save()
            painter.setClipRect(plot_area)
            painter.setBrush(Qt.BrushStyle.NoBrush)
            region_color = QColor("#a78bfa")
            region_color.setAlpha(14)
            painter.fillRect(QRectF(boundary_x, plot_area.top(), last_x - boundary_x + step_width * 0.5, plot_area.height()), region_color)
            boundary_pen = QPen(QColor("#a78bfa"), 1.0, Qt.PenStyle.DashLine)
            painter.setPen(boundary_pen)
            painter.drawLine(QPointF(boundary_x, plot_area.top()), QPointF(boundary_x, plot_area.bottom()))
            for bar in ordered:
                x = self._x_for_display_value(float(bar.get("time_ms", 0) or 0), plot_area)
                open_y = self._y_for_value(float(bar.get("open", 0) or 0), plot_area)
                close_y = self._y_for_value(float(bar.get("close", 0) or 0), plot_area)
                high_y = self._y_for_value(float(bar.get("high", 0) or 0), plot_area)
                low_y = self._y_for_value(float(bar.get("low", 0) or 0), plot_area)
                candle_color = QColor(_CHART_UP_COLOR if float(bar.get("close", 0) or 0) >= float(bar.get("open", 0) or 0) else _CHART_DOWN_COLOR)
                candle_color.setAlpha(220)
                painter.setPen(QPen(candle_color, 1.2))
                painter.drawLine(QPointF(x, high_y), QPointF(x, low_y))
                body = QRectF(x - body_width / 2, min(open_y, close_y), body_width, max(2.0, abs(close_y - open_y)))
                candle_color.setAlpha(225)
                painter.fillRect(body, candle_color)
                painter.setPen(QPen(candle_color, 1.0))
                painter.drawRect(body)
            for ma_type, period, color, width in self._future_ma_specs:
                if not self._candles:
                    continue
                points = _future_moving_average_points(
                    self._candles, ordered, period, exponential=ma_type == "ema",
                )
                if not points:
                    continue
                pen = QPen(QColor(color), width)
                pen.setCosmetic(True)
                pen.setStyle(Qt.PenStyle.DashLine)
                painter.setPen(pen)
                # Keep a visible empty interval between historical and future MAs.
                previous: QPointF | None = None
                for time_ms, value in points:
                    current = QPointF(self._x_for_display_value(time_ms, plot_area), self._y_for_value(value, plot_area))
                    if previous is not None:
                        painter.drawLine(previous, current)
                    previous = current
            painter.restore()
            for key, color, label, width, style in (
                ("entry", "#38bdf8", "入场关键线", 2.2, Qt.PenStyle.SolidLine),
                ("target", "#34d399", "目标关键线", 1.8, Qt.PenStyle.DashLine),
                ("stop", "#f87171", "止损参考", 1.2, Qt.PenStyle.DotLine),
            ):
                try:
                    value = float(projection[key])
                except (KeyError, TypeError, ValueError):
                    continue
                y = self._y_for_value(value, plot_area)
                pen = QPen(QColor(color), width)
                pen.setStyle(style)
                painter.setPen(pen)
                painter.drawLine(QPointF(first_x, y), QPointF(last_x + body_width, y))
                painter.drawText(QPointF(last_x + body_width + 4, y - 3), f"{label} {value:.2f}")
            painter.setPen(QColor("#c4b5fd"))
            painter.drawText(QPointF(first_x, float(plot_area.top()) + 18), f"未来推演 · {projection.get('label', '')}（示意）")

        def _draw_workspace_rr_items(self, painter: QPainter, plot_area: QRectF) -> None:
            if not self._workspace_rr_items or not self._candles:
                return
            fill_long = QColor("#10b981")
            fill_long.setAlpha(44)
            fill_short = QColor("#ef4444")
            fill_short.setAlpha(44)
            fill_stop = QColor("#ef4444")
            fill_stop.setAlpha(40)
            fill_profit = QColor("#10b981")
            fill_profit.setAlpha(40)
            for index, item in enumerate(self._workspace_rr_items):
                try:
                    entry_price = float(item.get("price_entry", 0.0) or 0.0)
                    stop_price = float(item.get("price_stop", 0.0) or 0.0)
                    take_profit = float(item.get("price_tp", 0.0) or 0.0)
                    if entry_price <= 0.0 or stop_price <= 0.0 or take_profit <= 0.0:
                        continue
                    bar_entry = int(round(float(item.get("bar_entry", 0.0) or 0.0)))
                except (TypeError, ValueError):
                    continue
                start_x = self._x_for_index(bar_entry, plot_area)
                end_display_x = _rr_box_end_display_x(
                    self._display_times_ms,
                    display_step_ms=self._display_step_ms,
                    bar_entry=bar_entry,
                )
                box_end_x = self._x_for_display_value(end_display_x, plot_area)
                if box_end_x <= start_x:
                    continue
                side = str(item.get("side", "long") or "long").strip().lower()
                y_entry = self._y_for_value(entry_price, plot_area)
                y_stop = self._y_for_value(stop_price, plot_area)
                y_take_profit = self._y_for_value(take_profit, plot_area)
                if side == "short":
                    profit_rect = QRectF(
                        QPointF(start_x, min(y_entry, y_take_profit)),
                        QPointF(box_end_x, max(y_entry, y_take_profit)),
                    )
                    stop_rect = QRectF(
                        QPointF(start_x, min(y_entry, y_stop)),
                        QPointF(box_end_x, max(y_entry, y_stop)),
                    )
                else:
                    profit_rect = QRectF(
                        QPointF(start_x, min(y_entry, y_take_profit)),
                        QPointF(box_end_x, max(y_entry, y_take_profit)),
                    )
                    stop_rect = QRectF(
                        QPointF(start_x, min(y_entry, y_stop)),
                        QPointF(box_end_x, max(y_entry, y_stop)),
                    )
                painter.fillRect(profit_rect, fill_profit if side == "long" else fill_long)
                painter.fillRect(stop_rect, fill_stop if side == "long" else fill_short)

                rr_pen = QPen(QColor("#f59e0b" if index == self._selected_workspace_rr_index else "#94a3b8"), 1)
                rr_pen.setCosmetic(True)
                rr_pen.setStyle(Qt.PenStyle.DashLine)
                painter.setPen(rr_pen)
                painter.drawLine(QPointF(start_x, y_entry), QPointF(box_end_x, y_entry))
                painter.drawLine(QPointF(start_x, y_stop), QPointF(box_end_x, y_stop))
                painter.drawLine(QPointF(start_x, y_take_profit), QPointF(box_end_x, y_take_profit))
                self._draw_rr_text_badge(
                    painter,
                    plot_area,
                    QRectF(start_x + 8.0, y_take_profit - 26.0, min(260.0, max(180.0, box_end_x - start_x - 12.0)), 22.0),
                    str(item.get("overlay_tp_text", "") or f"止盈 {take_profit}"),
                    QColor("#0f766e"),
                )
                self._draw_rr_text_badge(
                    painter,
                    plot_area,
                    QRectF(start_x + 8.0, y_entry - 12.0, min(300.0, max(210.0, box_end_x - start_x - 12.0)), 24.0),
                    str(item.get("overlay_mid_text", "") or str(item.get("overlay_entry_text", "") or "")),
                    QColor(15, 23, 42, 228),
                )
                self._draw_rr_text_badge(
                    painter,
                    plot_area,
                    QRectF(start_x + 8.0, y_stop + 4.0, min(260.0, max(180.0, box_end_x - start_x - 12.0)), 22.0),
                    str(item.get("overlay_stop_text", "") or f"止损 {stop_price}"),
                    QColor("#991b1b"),
                )

        def _draw_preview_rr_item(self, painter: QPainter, plot_area: QRectF) -> None:
            item = self._preview_rr_item
            if not isinstance(item, dict) or not self._candles:
                return
            try:
                entry_price = float(item.get("price_entry", 0.0) or 0.0)
                stop_price = float(item.get("price_stop", 0.0) or 0.0)
                take_profit = float(item.get("price_tp", 0.0) or 0.0)
                bar_entry = int(round(float(item.get("bar_entry", 0.0) or 0.0)))
            except (TypeError, ValueError):
                return
            if entry_price <= 0.0 or stop_price <= 0.0 or take_profit <= 0.0:
                return
            start_x = self._x_for_index(bar_entry, plot_area)
            end_display_x = _rr_box_end_display_x(
                self._display_times_ms,
                display_step_ms=self._display_step_ms,
                bar_entry=bar_entry,
            )
            box_end_x = self._x_for_display_value(end_display_x, plot_area)
            if box_end_x <= start_x:
                return
            side = str(item.get("side", "long") or "long").strip().lower()
            y_entry = self._y_for_value(entry_price, plot_area)
            y_stop = self._y_for_value(stop_price, plot_area)
            y_take_profit = self._y_for_value(take_profit, plot_area)
            gain_fill = QColor("#22c55e")
            gain_fill.setAlpha(28)
            loss_fill = QColor("#ef4444")
            loss_fill.setAlpha(28)
            painter.fillRect(
                QRectF(QPointF(start_x, min(y_entry, y_take_profit)), QPointF(box_end_x, max(y_entry, y_take_profit))),
                gain_fill,
            )
            painter.fillRect(
                QRectF(QPointF(start_x, min(y_entry, y_stop)), QPointF(box_end_x, max(y_entry, y_stop))),
                loss_fill,
            )
            preview_pen = QPen(QColor("#fde68a"), 1)
            preview_pen.setCosmetic(True)
            preview_pen.setStyle(Qt.PenStyle.DotLine)
            painter.setPen(preview_pen)
            painter.drawLine(QPointF(start_x, y_entry), QPointF(box_end_x, y_entry))
            painter.drawLine(QPointF(start_x, y_stop), QPointF(box_end_x, y_stop))
            painter.drawLine(QPointF(start_x, y_take_profit), QPointF(box_end_x, y_take_profit))

        def _draw_rr_text_badge(self, painter: QPainter, plot_area: QRectF, rect: QRectF, text: str, fill: QColor) -> None:
            if not text:
                return
            metrics = painter.fontMetrics()
            text_rect = metrics.boundingRect(
                QRectF(0.0, 0.0, 320.0, 160.0).toRect(),
                int(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter | Qt.TextFlag.TextWordWrap),
                text,
            )
            badge_rect = QRectF(rect)
            badge_rect.setWidth(max(120.0, min(320.0, float(text_rect.width()) + 18.0)))
            badge_rect.setHeight(max(20.0, float(text_rect.height()) + 10.0))
            badge_rect.moveLeft(
                max(
                    float(plot_area.left()) + 4.0,
                    min(float(badge_rect.left()), float(plot_area.right()) - float(badge_rect.width()) - 4.0),
                )
            )
            badge_rect.moveTop(
                max(
                    float(plot_area.top()) + 4.0,
                    min(float(badge_rect.top()), float(plot_area.bottom()) - float(badge_rect.height()) - 4.0),
                )
            )
            fill_color = QColor(fill)
            fill_color.setAlpha(228)
            painter.setPen(QPen(QColor(255, 255, 255, 36), 1))
            painter.setBrush(fill_color)
            painter.drawRoundedRect(badge_rect, 6.0, 6.0)
            painter.setPen(QColor("#f8fafc"))
            painter.drawText(
                badge_rect.adjusted(8.0, 4.0, -8.0, -4.0),
                Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignLeft | Qt.TextFlag.TextWordWrap,
                text,
            )

        def _draw_workspace_ray_extensions(self, painter: QPainter, plot_area: QRectF) -> None:
            if not self._display_times_ms:
                return
            painter.save()
            painter.setClipRect(plot_area)
            for index, line in enumerate(self._workspace_lines):
                if line.get("kind") != "ray":
                    continue
                start = max(float(self._display_times_ms[-1]), _display_x_for_candle_time(
                    self._candles, self._display_times_ms, candle_time=int(line.get("time_a", 0) or 0),
                    display_step_ms=self._display_step_ms,
                ))
                x = self._x_for_display_value(start, plot_area)
                if x >= plot_area.right():
                    continue
                color = str(line.get("color", "#1d4ed8")) if line.get("enabled", True) else "#94a3b8"
                selected = index == self._selected_workspace_line_index
                pen = QPen(QColor("#38bdf8" if selected else color), 4 if selected else 2)
                pen.setCosmetic(True)
                painter.setPen(pen)
                price = float(line.get("price_a", 0) or 0)
                if self._axis_y is not None and not self._axis_y.min() <= price <= self._axis_y.max():
                    continue
                y = self._y_for_value(price, plot_area)
                painter.drawLine(QPointF(max(x, plot_area.left()), y), QPointF(plot_area.right(), y))
            painter.restore()

        def _draw_selected_line_handles(self, painter: QPainter, plot_area: QRectF) -> None:
            if not (0 <= self._selected_workspace_line_index < len(self._workspace_lines)):
                return
            line = self._workspace_lines[self._selected_workspace_line_index]
            kind = str(line.get("kind", "horizontal") or "horizontal").strip().lower()
            handle_pen = QPen(QColor("#38bdf8"), 2)
            handle_pen.setCosmetic(True)
            painter.setPen(handle_pen)
            hovered_mode = self._hovered_workspace_drag_mode if self._hovered_workspace_line_index == self._selected_workspace_line_index else None
            if kind in {"horizontal", "ray"}:
                body_visual = _line_handle_visual("move", hovered_drag_mode=hovered_mode)
                y_pos = self._y_for_value(float(line.get("price_a", 0.0) or 0.0), plot_area)
                x_pos = float(plot_area.right()) - 18.0
                painter.setBrush(QColor(str(body_visual["fill"])))
                painter.drawEllipse(QPointF(x_pos, y_pos), float(body_visual["radius"]), float(body_visual["radius"]))
                painter.setBrush(QColor(str(body_visual["inner_fill"])))
                painter.drawEllipse(QPointF(x_pos, y_pos), 2.2, 2.2)
                if kind == "ray":
                    start_time = int(line.get("time_a", 0) or 0)
                    start_x = self._x_for_display_value(
                        _display_x_for_candle_time(self._candles, self._display_times_ms, candle_time=start_time, display_step_ms=self._display_step_ms),
                        plot_area,
                    )
                    visual = _line_handle_visual("endpoint_a", hovered_drag_mode=hovered_mode)
                    painter.setBrush(QColor(str(visual["fill"])))
                    painter.drawEllipse(QPointF(start_x, y_pos), float(visual["radius"]), float(visual["radius"]))
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
            if kind == "ray":
                y_pos = self._y_for_value(float(preview_line.get("price_a", 0.0) or 0.0), plot_area)
                start_x = self._x_for_display_value(
                    _display_x_for_candle_time(self._candles, self._display_times_ms, candle_time=int(preview_line.get("time_a", 0) or 0), display_step_ms=self._display_step_ms),
                    plot_area,
                )
                painter.drawLine(QPointF(start_x, y_pos), QPointF(plot_area.right(), y_pos))
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
            display_x = _display_value_for_bar_index(
                self._display_times_ms,
                display_step_ms=self._display_step_ms,
                bar_index=float(index),
            )
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
            if self._period.strip().upper() == "1H" and new_x_max > self._full_x_max:
                padding = math.ceil((new_x_max - float(self._display_times_ms[-1])) / self._display_step_ms)
                self._set_right_padding_bars(padding)
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
                target_visible_bars=_LINKED_MIN_VISIBLE_BARS.get(
                    self._period.strip().upper(),
                    _RECENT_VIEW_BARS,
                ),
                right_padding_bars=self._active_right_padding_bars(),
            )

        def _active_right_padding_bars(self) -> int:
            return self._right_padding_bars if self._period.strip().upper() == "1H" else _NATIVE_RIGHT_PADDING_BARS

        def _set_right_padding_bars(self, bars: int) -> None:
            self._right_padding_bars = int(_clamp(float(bars), 0.0, float(_NATIVE_MAX_RIGHT_PADDING_BARS)))
            if not self._display_times_ms:
                return
            self._full_x_max = float(self._display_times_ms[-1]) + _native_right_padding_ms(
                self._display_step_ms, padding_bars=self._active_right_padding_bars(),
            )
            if self._future_projection:
                future_bars = self._future_projection.get("bars", [])
                self._full_x_max = max(self._full_x_max, max(
                    (float(item.get("time_ms", 0) or 0) + self._display_step_ms for item in future_bars if isinstance(item, dict)),
                    default=self._full_x_max,
                ))

        def _is_right_padding_position(self, x: float, plot_area: QRectF) -> bool:
            if (self._period.strip().upper() != "1H" or self._draw_mode_enabled or self._interaction_locked
                    or not self._display_times_ms or self._axis_x is None):
                return False
            start_x, end_x = self.current_x_range()
            span = max(end_x - start_x, 1.0)
            latest_x = float(plot_area.left()) + (
                (float(self._display_times_ms[-1]) - start_x) / span
            ) * float(plot_area.width())
            return float(plot_area.left()) <= latest_x < float(plot_area.right()) and x >= latest_x + 3.0

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
            reserved_ratio = _VOLUME_OVERLAY_HEIGHT_RATIO + (_ATR_OVERLAY_HEIGHT_RATIO if self._show_atr else 0.0)
            volume_reserved_height = max(42.0, float(bounds.height()) * reserved_ratio)
            overlay_layout = _compute_hover_overlay_layout(
                viewport_top=float(viewport.top()),
                viewport_bottom=float(viewport.bottom()),
                bounds_top=float(mapped_bounds.top()),
                bounds_bottom=float(mapped_bounds.bottom()),
                anchor_y=float(mapped_anchor.y()),
                price_height=float(price_size.height()),
                tooltip_height=0.0,
                volume_reserved_height=volume_reserved_height,
            )
            price_x = min(
                float(viewport.right()) - float(price_size.width()) - 4.0,
                float(mapped_bounds.right()) + 8.0,
            )
            price_y = float(overlay_layout["price_y"])
            self._price_badge.move(int(round(price_x)), int(round(price_y)))
            self._price_badge.raise_()
            self._price_badge.setVisible(bool(price_text))

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
            overlay_layout = _compute_hover_overlay_layout(
                viewport_top=float(viewport.top()),
                viewport_bottom=float(viewport.bottom()),
                bounds_top=float(mapped_bounds.top()),
                bounds_bottom=float(mapped_bounds.bottom()),
                anchor_y=float(mapped_anchor.y()),
                price_height=float(price_size.height()),
                tooltip_height=float(tooltip_size.height()),
                volume_reserved_height=volume_reserved_height,
            )
            data_right_x = float(mapped_bounds.left())
            if self._candles:
                data_right_x = float(viewport_geom.left()) + self._x_for_index(len(self._candles) - 1, bounds)
            tooltip_x = _compute_hover_tooltip_x(
                bounds_left=float(mapped_bounds.left()),
                bounds_right=float(mapped_bounds.right()),
                anchor_x=float(mapped_anchor.x()),
                data_right_x=data_right_x,
                tooltip_width=float(tooltip_size.width()),
            )
            tooltip_y = float(overlay_layout["tooltip_y"])
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
        average_kline: bool = False,
        workspace_entry: dict[str, object] | None = None,
        enable_alerts: bool = True,
        enable_shape_signals: bool = False,
    ) -> None:
        super().__init__()
        self._request_id = request_id
        self._symbol = symbol.strip().upper()
        self._period = period.strip()
        self._limit = max(50, limit)
        self._local_only = local_only
        self._average_kline = bool(average_kline)
        self._workspace_entry = normalize_workspace_entry(workspace_entry)
        self._enable_alerts = enable_alerts
        self._enable_shape_signals = bool(enable_shape_signals)
        self._weekly_cache_generated = False
        self._weekly_cache_source_bar: str | None = None

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
        if self._average_kline:
            candles = _to_average_price_candles(candles)
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
        stats["average_kline"] = self._average_kline
        stats["weekly_cache_generated"] = self._weekly_cache_generated
        stats["weekly_cache_source_bar"] = self._weekly_cache_source_bar

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
        visuals = self._workspace_entry.get("visuals", {})
        visuals = visuals if isinstance(visuals, dict) else {}
        signal_markers = (
            _build_replay_signal_markers(
                candles=list(candles),
                period=self._period,
                ema15_values=ema9_values,
                sma50_values=sma50_values,
            )
            if self._enable_shape_signals
            else []
        )
        box_overlays: list[dict[str, Any]] = []
        if bool(visuals.get("history_box_visible", False)):
            box_overlays.extend(_build_box_history_overlays(list(candles)))
        if bool(visuals.get("auto_box_visible", False)):
            box_overlays.extend(_build_box_current_overlay(list(candles)))
        show_auto_channel = bool(visuals.get("auto_channel_visible", False))
        channel_settings = self._workspace_entry.get("auto_channel", {})
        channel_settings = channel_settings if isinstance(channel_settings, dict) else {}
        channel_config = ChannelDetectionConfig(
            min_anchor_distance=max(1, int(channel_settings.get("anchor_distance", 8) or 8)),
            min_channel_bars=max(2, int(channel_settings.get("min_bars", 18) or 18)),
            max_violations=max(0, int(channel_settings.get("max_violations", 8) or 8)),
        )
        channel_overlays = _build_channel_current_overlays(list(candles), config=channel_config) if show_auto_channel else []

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
            channel_overlays=channel_overlays,
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
            is_weekly = self._period.strip().upper() == "1W"
            if is_weekly:
                weekly_candles, generated, source_bar = _ensure_local_weekly_cache(self._symbol)
                if weekly_candles:
                    local_candles = weekly_candles
                    self._weekly_cache_generated = generated
                    self._weekly_cache_source_bar = source_bar
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

            if self._local_only or (is_weekly and local_candles):
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


class WeeklyCacheBackfillLoader(QThread):
    """Populate local UTC+8 weekly bars for the symbols shown in the K-line tabs."""

    progress = Signal(int, int, str, int)
    completed = Signal(int, int)

    def run(self) -> None:
        total = len(KLINE_SYMBOL_OPTIONS)
        completed = 0
        for symbol in KLINE_SYMBOL_OPTIONS:
            if self.isInterruptionRequested():
                return
            try:
                candles, changed, _source_bar = _ensure_local_weekly_cache(symbol)
                count = len(candles)
                completed += 1
                self.progress.emit(completed, total, symbol, count if changed else -count)
            except Exception as exc:  # noqa: BLE001
                completed += 1
                _debug_log(f"[kline] weekly cache backfill failed | symbol={symbol} | error={exc}")
                self.progress.emit(completed, total, symbol, 0)
        self.completed.emit(completed, total)


class KlineHistoryTradeLoader(QThread):
    """Fetch position lifecycles and current positions without blocking the chart."""

    loaded = Signal(int, object)
    failed = Signal(int, str)

    def __init__(self, *, request_id: int, runtime: object, symbol: str) -> None:
        super().__init__()
        self._request_id = request_id
        self._runtime = runtime
        self._symbol = symbol.strip().upper()

    def run(self) -> None:
        try:
            credentials = getattr(self._runtime, "credentials", None)
            environment = str(getattr(self._runtime, "environment", "") or "").strip()
            if credentials is None or not environment:
                self.loaded.emit(self._request_id, ())
                return
            inst_type = infer_inst_type(self._symbol)
            if inst_type not in {"SWAP", "FUTURES", "OPTION"}:
                self.loaded.emit(self._request_id, ())
                return
            client = OkxRestClient()
            profile_name = str(getattr(credentials, "profile_name", "") or "").strip()
            cached_history: list[OkxPositionHistoryItem] = []
            cache_error: Exception | None = None
            if profile_name:
                try:
                    cached_history = load_local_position_history(profile_name, environment, limit=1000)
                except Exception as exc:  # noqa: BLE001
                    cache_error = exc
            history_error: Exception | None = None
            current_error: Exception | None = None
            try:
                remote_history = client.get_positions_history(
                    credentials,
                    environment=environment,
                    inst_types=(inst_type,),
                    limit=100,
                    fetch_all=True,
                )
            except Exception as exc:  # noqa: BLE001
                remote_history = []
                history_error = exc
            try:
                current_positions = client.get_positions(
                    credentials,
                    environment=environment,
                    inst_type=inst_type,
                    prefer_cache=False,
                )
            except Exception as exc:  # noqa: BLE001
                current_positions = []
                current_error = exc
            if history_error is not None and not cached_history and current_error is not None:
                raise history_error
            if cache_error is not None and history_error is not None and current_error is not None:
                raise cache_error
            history_items = cached_history
            if profile_name and remote_history:
                history_items = merge_position_history_cache(
                    profile_name=profile_name,
                    environment=environment,
                    remote_items=remote_history,
                    limit=1000,
                )
            elif remote_history:
                history_items = remote_history
            history_markers = _build_kline_history_trade_markers(history_items, symbol=self._symbol)
            current_markers = _build_kline_current_position_markers(current_positions, symbol=self._symbol)
            self.loaded.emit(self._request_id, _merge_kline_trade_markers(history_markers, current_markers))
        except Exception as exc:  # noqa: BLE001
            self.failed.emit(self._request_id, str(exc))


class SecondaryVolatilityDataLoader(QThread):
    loaded = Signal(int, object)
    failed = Signal(int, str)

    def __init__(
        self,
        *,
        request_id: int,
        currency: str,
        period: str,
        limit: int,
        average_kline: bool,
        local_only: bool = False,
    ) -> None:
        super().__init__()
        self._request_id = request_id
        self._currency = currency.strip().upper()
        self._period = period.strip()
        self._limit = limit
        self._average_kline = average_kline
        self._local_only = local_only

    def run(self) -> None:
        try:
            payload = self._build_payload()
            if not self.isInterruptionRequested():
                self.loaded.emit(self._request_id, payload)
        except Exception as exc:  # noqa: BLE001
            if not self.isInterruptionRequested():
                self.failed.emit(self._request_id, str(exc))

    def _target_resolution(self) -> str:
        requested_period = self._period.strip().upper()
        return {
            "1H": DERIBIT_BASE_HOURLY_RESOLUTION,
            "4H": "14400",
            "1D": "1D",
            "1W": "1W",
        }.get(requested_period, DERIBIT_BASE_HOURLY_RESOLUTION)

    def _build_resolution_candles(self, hourly_candles: list[Any], *, resolution: str) -> list[Any]:
        if resolution == "14400":
            candles = _aggregate_candles_to_resolution(hourly_candles, 14_400_000)
        elif resolution == "1D":
            candles_4h = _aggregate_candles_to_resolution(hourly_candles, 14_400_000)
            # UTC+8 00:00 is 16:00 UTC; keep daily buckets aligned with the local K-line view.
            candles = _aggregate_candles_to_resolution(candles_4h, 86_400_000, anchor_offset_ms=16 * 3_600_000)
        elif resolution == "1W":
            candles_4h = _aggregate_candles_to_resolution(hourly_candles, 14_400_000)
            # Deribit timestamps are UTC.  Monday 00:00 in UTC+8 is Sunday
            # 16:00 UTC; this epoch-relative anchor keeps weekly buckets on
            # the same boundary as the local OKX week.
            utc8_monday_anchor_ms = (3 * 86_400_000) + (16 * 3_600_000)
            candles = _aggregate_candles_to_resolution(
                candles_4h,
                7 * 86_400_000,
                anchor_offset_ms=utc8_monday_anchor_ms,
            )
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
            raise ValueError(f"{self._currency} DVOL 没有可用数据")

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
        cached_hourly = _load_cached_deribit_hourly_series(self._currency)
        cached_volatility: list[Any] = []
        cached_spot: list[Any] = []
        spot_inst_id = OKX_SPOT_SYMBOLS[self._currency]
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
                        source=f"Deribit {self._currency} DVOL（本地缓存）",
                        local_count=local_count,
                        remote_added_count=0,
                        cache_synced=False,
                    ),
                )
                local_preview_emitted = True

        if self.isInterruptionRequested():
            raise InterruptedError("波动率副图加载已取消")

        if self._local_only:
            return self._make_payload(
                candles=self._build_resolution_candles(cached_volatility, resolution=resolution),
                source=f"Deribit {self._currency} DVOL（本地缓存）",
                local_count=local_count, remote_added_count=0, cache_synced=False,
            )

        now_ms = int(time.time() * 1000)
        fetch_start_ts = (
            _hourly_fetch_start_ts(cached_volatility=cached_volatility, cached_spot=cached_spot)
            if cached_hourly is not None
            else DERIBIT_FULL_HISTORY_START_TS
        )

        try:
            fetched_volatility = DeribitRestClient().get_volatility_index_candles(
                self._currency,
                DERIBIT_BASE_HOURLY_RESOLUTION,
                start_ts=fetch_start_ts,
                end_ts=now_ms,
                max_records=None,
            )
            if self.isInterruptionRequested():
                raise InterruptedError("波动率副图加载已取消")
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
                    source=f"Deribit {self._currency} DVOL（本地缓存）",
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

        if self.isInterruptionRequested():
            raise InterruptedError("波动率副图加载已取消")
        _save_cached_deribit_hourly_series(
            self._currency,
            spot_inst_id=spot_inst_id,
            volatility_candles=hourly_candles,
            spot_candles=spot_hourly,
            fetched_at=datetime.now(),
        )

        candles = self._build_resolution_candles(hourly_candles, resolution=resolution)
        return self._make_payload(
            candles=candles,
            source=(
                f"Deribit {self._currency} DVOL（本地缓存已刷新）"
                if cached_hourly is not None
                else f"Deribit {self._currency} DVOL"
            ),
            local_count=local_count,
            remote_added_count=remote_added_count,
            cache_synced=True,
        )


class RRTradeExecutionThread(QThread):
    completed = Signal(object)
    failed = Signal(str)

    def __init__(self, action: Callable[[], RRTradeLedgerEntry], parent: QObject | None = None) -> None:
        super().__init__(parent)
        self._action = action

    def run(self) -> None:
        try:
            self.completed.emit(self._action())
        except Exception as exc:  # noqa: BLE001
            self.failed.emit(str(exc))


class AreaShapeDemoActionThread(QThread):
    completed = Signal(object)
    failed = Signal(str)

    def __init__(self, action: Callable[[], object], parent: QObject | None = None) -> None:
        super().__init__(parent)
        self._action = action

    def run(self) -> None:
        try:
            self.completed.emit(self._action())
        except Exception as exc:  # noqa: BLE001
            self.failed.emit(str(exc))


class KlineAnalysisWindow(QMainWindow):
    _realtime_candle_received = Signal(object)

    def __init__(self, *, embedded: bool = False, preview_mode: bool = False) -> None:
        super().__init__()
        _debug_log("[kline] __init__ begin")
        self._embedded = bool(embedded)
        self._preview_mode = bool(preview_mode)
        if self._embedded:
            self.setWindowFlags(Qt.WindowType.Widget)
        else:
            self.setWindowTitle("K线分析预览" if self._preview_mode else "K线分析")
            self.resize(1680, 980)

        self._request_id = 0
        self._active_request_id = 0
        self._active_primary_request_key: tuple[Any, ...] | None = None
        self._loaded_primary_request_key: tuple[Any, ...] | None = None
        self._loader: KlineDataLoader | None = None
        self._retired_loaders: set[QThread] = set()
        self._area_shape_replay_thread: QThread | None = None
        self._area_shape_replay_context: tuple[AreaShapePlan, int, tuple[str, ...], str, dict[str, int]] | None = None
        self._area_shape_replay_overlay: dict[str, Any] | None = None
        self._area_shape_replay_dialog: QDialog | None = None
        self._weekly_cache_backfill_loader: WeeklyCacheBackfillLoader | None = None
        self._history_trade_request_id = 0
        self._active_history_trade_request_id = 0
        self._history_trade_loader: KlineHistoryTradeLoader | None = None
        self._history_trade_context_by_request: dict[int, tuple[str, str, str]] = {}
        self._pending_history_trade_reload = False
        self._history_trade_markers_by_context: dict[tuple[str, str, str], tuple[KlineHistoryTradeMarker, ...]] = {}
        self._history_trade_sync_error_by_context: dict[tuple[str, str, str], str] = {}
        self._secondary_request_id = 0
        self._active_secondary_request_id = 0
        self._active_secondary_request_key: tuple[Any, ...] | None = None
        self._loaded_secondary_request_key: tuple[Any, ...] | None = None
        self._secondary_loader: KlineDataLoader | None = None
        self._tertiary_request_id = 0
        self._active_tertiary_request_id = 0
        self._active_tertiary_request_key: tuple[Any, ...] | None = None
        self._loaded_tertiary_request_key: tuple[Any, ...] | None = None
        self._tertiary_loader: KlineDataLoader | None = None
        self._quaternary_request_id = 0
        self._active_quaternary_request_id = 0
        self._active_quaternary_request_key: tuple[Any, ...] | None = None
        self._loaded_quaternary_request_key: tuple[Any, ...] | None = None
        self._quaternary_loader: KlineDataLoader | None = None
        self._use_native_chart = bool(
            QChartView is not None and (_prefer_native_chart_backend() or QWebEngineView is None)
        )
        self._page_ready = self._use_native_chart
        self._pending_payload: KlineChartPayload | None = None
        self._secondary_pending_payload: KlineChartPayload | None = None
        self._tertiary_pending_payload: KlineChartPayload | None = None
        self._quaternary_pending_payload: KlineChartPayload | None = None
        self._primary_payload_cache: dict[tuple[Any, ...], KlineChartPayload] = {}
        self._secondary_payload_cache: dict[tuple[Any, ...], KlineChartPayload] = {}
        self._tertiary_payload_cache: dict[tuple[Any, ...], KlineChartPayload] = {}
        self._quaternary_payload_cache: dict[tuple[Any, ...], KlineChartPayload] = {}
        self._workspace_entries = load_kline_analysis_workspace_entries()
        self._selected_line_index = -1
        self._selected_rr_index = -1
        self._hovered_line_index = -1
        self._hovered_line_drag_mode: str | None = None
        self._draw_tool = "none"
        self._pending_line_start: tuple[int, float] | None = None
        self._pending_rr_start: tuple[str, int, float] | None = None
        self._suppress_next_chart_click = False
        self._line_drag_state: dict[str, object] | None = None
        self._rr_drag_state: dict[str, object] | None = None
        self._web = None
        self._native_chart = None
        self._native_chart_view = None
        self._area_shape_projection: dict[str, Any] | None = None
        self._primary_chart_frame = None
        self._secondary_native_chart = None
        self._secondary_native_chart_view = None
        self._secondary_chart_frame = None
        self._tertiary_native_chart = None
        self._tertiary_native_chart_view = None
        self._tertiary_chart_frame = None
        self._quaternary_native_chart = None
        self._quaternary_native_chart_view = None
        self._quaternary_chart_frame = None
        self._chart_stack_splitter = None
        self._quad_grid_host = None
        self._quad_grid_layout = None
        self._primary_period_buttons: dict[str, QPushButton] = {}
        self._active_chart_target = "primary"
        self._trade_recommendation_dialog: KlineTradeRecommendationDialog | None = None
        self._symbol_tab_bar: QTabBar | None = None
        self._symbol_link_all_check: QCheckBox | None = None
        self._updating_symbol_tabs = False
        self._chart_mode_cycle_btn: QPushButton | None = None
        self._chart_range_mode_btn: QPushButton | None = None
        self._chart_view_range_mode = "recent"
        self._secondary_layout_cycle_btn: QPushButton | None = None
        self._secondary_chart_kind_btn: QPushButton | None = None
        self._secondary_sync_period_btn: QPushButton | None = None
        # 双图联动默认左右并排，便于同时比较两个周期/交易对。
        self._secondary_layout_mode_value = "horizontal"
        self._secondary_chart_kind_mode = "kline"
        # 波动率视图不是普通交易对图的附属状态。记住当前 DVOL
        # 标的，避免用户在该视图中误点其他自选品种后替换四张图。
        self._active_volatility_currency: str | None = None
        self._shape_signal_size_metric = "body"
        self._initial_load_requested = False
        self._page_active = False
        self._profile_reload_pending = False
        self._splitter_default_applied = False
        self._native_chart_bootstrap_complete = False
        self._deferred_chart_payload: KlineChartPayload | None = None
        self._deferred_chart_request_id = 0
        self._body_splitter: QSplitter | None = None
        self._chart_account_splitter: QSplitter | None = None
        self._chart_host: QWidget | None = None
        self._chart_host_layout: QVBoxLayout | None = None
        self._chart_frame: QFrame | None = None
        self._header_panel: QFrame | None = None
        self._control_panel: QFrame | None = None
        self._control_scroll: QScrollArea | None = None
        self._control_tabs: QTabWidget | None = None
        self._layout_context_panel: QFrame | None = None
        self._account_drawer: KlineAccountDrawer | None = None
        self._chart_fullscreen_button: QPushButton | None = None
        self._chart_fullscreen_enabled = False
        self._chart_fullscreen_restore_body_sizes: list[int] = []
        self._chart_fullscreen_restore_account_sizes: list[int] = []
        self._chart_fullscreen_restore_header_visible = True
        self._chart_fullscreen_restore_control_visible = True
        self._chart_fullscreen_restore_drawer_visible = False
        self._chart_fullscreen_restore_workspace_header: QWidget | None = None
        self._chart_fullscreen_restore_workspace_header_visible = False
        self._chart_fullscreen_restore_main_margins = (10, 10, 10, 10)
        self._chart_fullscreen_restore_main_spacing = 10
        self._chart_fullscreen_window_target: QWidget | None = None
        self._chart_fullscreen_restore_window_maximized = False
        self._chart_fullscreen_owns_window_state = False
        self._account_prefetch_context: tuple[str, str] | None = None
        self._account_prefetch_timer = QTimer(self)
        self._account_prefetch_timer.setSingleShot(True)
        self._account_prefetch_timer.timeout.connect(self._prefetch_account_drawer_after_chart_ready)
        self._left_panel_hidden = True
        self._secondary_volatility_loader: SecondaryVolatilityDataLoader | None = None
        self._syncing_chart_range = False
        self._pending_reload_after_load = False
        self._pending_secondary_reload = False
        self._pending_tertiary_reload = False
        self._pending_quaternary_reload = False
        self._primary_chart_status_text = ""
        self._secondary_chart_status_text = ""
        self._runtime = load_runtime("159") or load_runtime()
        self._market_client = OkxRestClient()
        self._realtime_candle_key: CandleStreamKey | None = None
        self._realtime_candle_unsubscribe: Callable[[], None] | None = None
        self._shape_signal_monitor: object | None = None
        self._area_shape_monitor: object | None = None
        self._area_shape_execution_monitor: object | None = None
        self._area_shape_demo_workers: set[AreaShapeDemoActionThread] = set()
        self._area_shape_demo_action_pending = False
        self._area_shape_demo_arm_pending = False
        self._area_shape_demo_ui_error = ""
        self._realtime_candle_received.connect(self._apply_realtime_candle)
        self._instrument_cache: dict[str, object | None] = {}
        self._rr_trade_ledger_snapshot = load_kline_rr_trade_ledger_snapshot()
        self._rr_trade_execution_service = RRTradeExecutionService()
        self._rr_execution_thread: QThread | None = None
        self._rr_execution_in_flight = False
        self._rr_monitor_cursor = 0
        self._pending_rr_execution_requests: list[dict[str, object]] = []
        self._pending_rr_activation_plan_ids: set[str] = set()
        self._rr_execution_clients: dict[tuple[str, str, str], OkxRestClient] = {}
        self._line_trade_execution_queue: list[RRTradePlan] = []
        self._suppress_api_profile_change = False
        self._profile_snapshots: dict[str, dict[str, str]] = {}
        self._unlocked_profiles: set[str] = set()
        self._last_profile_name = ""

        root = QWidget()
        self.setCentralWidget(root)

        main_layout = QVBoxLayout(root)
        main_layout.setContentsMargins(10, 10, 10, 10)
        main_layout.setSpacing(10)
        self._main_layout = main_layout

        self._build_header(main_layout)
        self._build_body(main_layout)
        self._toggle_left_panel(self._left_panel_hidden)
        self._refresh_history_trade_availability()
        self._sync_primary_period_buttons()
        self._refresh_chart_mode_cycle_button()
        self._refresh_chart_view_range_button()
        self._refresh_secondary_layout_button()
        self._refresh_secondary_chart_kind_button()
        self._refresh_secondary_sync_period_button()
        self._refresh_shape_signal_size_metric_button()
        self._reload_workspace_view()
        self._build_refresh_timer()
        self._rr_monitor_timer = QTimer(self)
        self._rr_monitor_timer.setInterval(1300)
        self._rr_monitor_timer.timeout.connect(self._monitor_active_rr_trades)
        if not self._preview_mode:
            self._rr_monitor_timer.start()
        self._deferred_chart_render_timer = QTimer(self)
        self._deferred_chart_render_timer.setSingleShot(True)
        self._deferred_chart_render_timer.timeout.connect(self._render_deferred_full_chart)
        self._layout_refresh_timer = QTimer(self)
        self._layout_refresh_timer.setSingleShot(True)
        self._layout_refresh_timer.timeout.connect(self._refresh_chart_layout_after_window_change)
        self._load_request_timer = QTimer(self)
        self._load_request_timer.setSingleShot(True)
        self._load_request_timer.timeout.connect(self._load_data)
        self._pending_reload_retry_timer = QTimer(self)
        self._pending_reload_retry_timer.setSingleShot(True)
        self._pending_reload_retry_timer.timeout.connect(self._schedule_pending_reload_if_ready)
        self._clamping_window_to_screen = False
        _debug_log("[kline] __init__ ready")

    def showEvent(self, event) -> None:  # noqa: ANN001
        super().showEvent(event)
        self._page_active = True
        _debug_log("[kline] showEvent")
        self._apply_default_splitter_sizes()
        self._schedule_chart_layout_refresh(80)
        if self._initial_load_requested:
            return
        self._initial_load_requested = True
        if self._auto_refresh_btn.isChecked() and not self._refresh_timer.isActive():
            self._refresh_timer.start()
        self._set_status("窗口已就绪，正在加载首屏图表...")
        self._start_weekly_cache_backfill()
        self._schedule_load_data(_INITIAL_WINDOW_LOAD_DELAY_MS)

    def _start_weekly_cache_backfill(self) -> None:
        if self._preview_mode or self._weekly_cache_backfill_loader is not None:
            return
        loader = WeeklyCacheBackfillLoader()
        self._weekly_cache_backfill_loader = loader
        loader.progress.connect(self._on_weekly_cache_backfill_progress)
        loader.completed.connect(self._on_weekly_cache_backfill_completed)
        loader.finished.connect(self._on_weekly_cache_backfill_finished)
        loader.start()

    @Slot(int, int, str, int)
    def _on_weekly_cache_backfill_progress(self, completed: int, total: int, symbol: str, count: int) -> None:
        if self._loader is not None and self._loader.isRunning():
            return
        if count != 0:
            action = "已生成" if count > 0 else "已有"
            self._set_status(f"周线本地缓存补齐 {completed}/{total}：{symbol} {action} {abs(count)} 条")
        else:
            self._set_status(f"周线本地缓存补齐 {completed}/{total}：{symbol} 无可用本地源")

    @Slot(int, int)
    def _on_weekly_cache_backfill_completed(self, completed: int, total: int) -> None:
        if self._loader is None or not self._loader.isRunning():
            self._set_status(f"周线本地缓存补齐完成：{completed}/{total} 个交易对")

    @Slot()
    def _on_weekly_cache_backfill_finished(self) -> None:
        loader = self.sender()
        if loader is None:
            loader = self._weekly_cache_backfill_loader
        if loader is not None and loader.isRunning():
            return
        if self._weekly_cache_backfill_loader is loader:
            self._weekly_cache_backfill_loader = None
        if loader is not None:
            loader.deleteLater()

    def set_page_active(self, active: bool) -> None:
        self._page_active = bool(active)
        account_store = getattr(getattr(self, "_account_drawer", None), "_realtime_store", None)
        set_reconcile_interval = getattr(account_store, "set_reconcile_interval_seconds", None)
        if callable(set_reconcile_interval):
            set_reconcile_interval(60 if self._page_active else 300)
        if not active:
            self._refresh_timer.stop()
            if self._load_request_timer.isActive():
                self._load_request_timer.stop()
            return
        if self._auto_refresh_btn.isChecked():
            self._refresh_timer.start()
        if self._pending_payload is not None and self._page_ready:
            self._render_loaded_payload(self._pending_payload)
        if self._pending_reload_after_load and not self._has_active_loaders():
            self._pending_reload_after_load = False
            self._schedule_load_data(0)
        if self._profile_reload_pending and not self._has_active_loaders():
            self._profile_reload_pending = False
            self._schedule_load_data(0)

    def workspace_profile_name(self) -> str:
        return self._active_profile_name()

    def apply_workspace_profile(self, profile_name: str) -> None:
        target = profile_name.strip()
        if not target or target == self._last_profile_name:
            return
        runtime = load_runtime(target)
        if runtime is None:
            return
        self._invalidate_profile_loaders()
        if hasattr(self, "_api_profile_combo"):
            index = self._api_profile_combo.findText(target)
            if index >= 0 and self._api_profile_combo.currentIndex() != index:
                self._suppress_api_profile_change = True
                try:
                    self._api_profile_combo.setCurrentIndex(index)
                finally:
                    self._suppress_api_profile_change = False
        self._runtime = runtime
        self._last_profile_name = target
        self._sync_account_context()
        self._sync_account_drawer_context()
        self._history_trade_markers_by_context.clear()
        self._history_trade_sync_error_by_context.clear()
        self._profile_reload_pending = True
        if self._page_active:
            self._profile_reload_pending = False
        self._load_data()

    def pattern_signals_enabled(self) -> bool:
        return any(
            checkbox.isChecked()
            for checkbox in (
                self._show_1h_shape_signal_check,
                self._show_4h_shape_signal_check,
                self._show_1d_shape_signal_check,
                self._show_1w_shape_signal_check,
            )
        )

    def set_shape_signal_monitor(self, monitor: object | None) -> None:
        self._shape_signal_monitor = monitor

    def set_area_shape_monitor(self, monitor: object | None) -> None:
        self._area_shape_monitor = monitor
        if monitor is not None:
            monitor.plan_updated.connect(self._on_area_plan_updated)
            monitor.status_changed.connect(self._on_area_monitor_status)
            self._refresh_area_shape_status()
            if self._pending_payload is not None and self._native_chart_view is not None:
                self._restore_area_shape_replay(
                    self._native_chart_view, symbol=self._selected_symbol(),
                    period=self._period_combo.currentText(),
                )

    def set_area_shape_execution_monitor(self, monitor: object | None) -> None:
        self._area_shape_execution_monitor = monitor
        if monitor is not None:
            monitor.session_updated.connect(self._on_area_demo_session_updated)
            monitor.status_changed.connect(self._on_area_demo_monitor_status)
            self._area_shape_demo_refresh_timer = QTimer(self)
            self._area_shape_demo_refresh_timer.setInterval(2000)
            self._area_shape_demo_refresh_timer.timeout.connect(self._refresh_area_shape_execution_status)
            self._area_shape_demo_refresh_timer.start()
        self._refresh_area_shape_execution_status()

    def configure_shape_signal_preview(
        self,
        *,
        symbol: str,
        period: str,
        candle_ts: int,
        pattern_name: str = "",
        direction: str = "",
        score: object = "",
        marker_text: str = "",
    ) -> None:
        """Configure a cache-only K-line view for one historical signal."""
        self._preview_signal_ts = int(candle_ts or 0)
        self._preview_signal_text = str(marker_text or pattern_name or "")
        self._preview_signal_context = {
            "pattern_name": str(pattern_name or ""),
            "direction": str(direction or ""),
            "score": str(score or ""),
        }
        normalized_period = str(period or "1H").strip().upper()
        if normalized_period not in {"1H", "4H", "1D", "1W"}:
            normalized_period = "1H"
        self._symbol_combo.setCurrentText(str(symbol or "").strip().upper())
        self._period_combo.setCurrentText(normalized_period)
        self._prefer_local_checkbox.setChecked(True)
        self._auto_refresh_btn.setChecked(False)
        self._show_history_trades_check.setChecked(False)
        self._history_trade_group.hide()
        self._limit_spin.setValue(max(240, self._limit_spin.value()))
        period_checks = {
            "1H": self._show_1h_shape_signal_check,
            "4H": self._show_4h_shape_signal_check,
            "1D": self._show_1d_shape_signal_check,
            "1W": self._show_1w_shape_signal_check,
        }
        for value, checkbox in period_checks.items():
            checkbox.blockSignals(True)
            checkbox.setChecked(value == normalized_period)
            checkbox.blockSignals(False)
        self._refresh_compact_shape_button()

    def show_shape_signal_context(self, *, symbol: str, period: str, candle_ts: int) -> None:
        """Switch the existing K-line page to a signal's context."""
        self._symbol_combo.setCurrentText(str(symbol or "").strip().upper())
        normalized_period = str(period or "1H").strip().upper()
        if normalized_period not in {"1H", "4H", "1D", "1W"}:
            normalized_period = "1H"
        self._on_period_button_clicked(normalized_period)
        if not self._page_active:
            QTimer.singleShot(0, self._load_data)
        self.show()
        self.raise_()
        self.activateWindow()

    def _open_shape_subscription_dialog(self) -> None:
        monitor = self._shape_signal_monitor
        if monitor is None:
            QMessageBox.information(self, "后台形态订阅", "当前窗口没有连接主程序后台监控服务。请从本地版本主窗口打开。")
            return
        from roll_terminal_qt.shape_signal_dialog import ShapeSignalSubscriptionDialog

        ShapeSignalSubscriptionDialog(monitor=monitor, parent=self).exec()

    def _open_shape_signal_history(self) -> None:
        owner = self.parentWidget()
        while owner is not None:
            opener = getattr(owner, "_open_shape_message_center", None)
            if callable(opener):
                opener()
                return
            owner = owner.parentWidget()
        from roll_terminal_qt.shape_signal_dialog import ShapeSignalHistoryDialog

        ShapeSignalHistoryDialog(parent=self).exec()

    def _refresh_compact_shape_button(self, *_args: object) -> None:
        if hasattr(self, "_shape_settings_button"):
            self._shape_settings_button.setText("形态：开" if self.pattern_signals_enabled() else "形态：关")

    @Slot(bool)
    def _on_shape_signal_visibility_changed(self, _enabled: bool) -> None:
        self._load_data()

    def begin_shutdown(self, callback: Callable[[], None] | None = None) -> None:
        callbacks = getattr(self, "_shutdown_callbacks", None)
        if callbacks is None:
            callbacks = []
            self._shutdown_callbacks = callbacks
        if callback is not None:
            callbacks.append(callback)
        if bool(getattr(self, "_shutdown_requested", False)):
            if bool(getattr(self, "_shutdown_complete", False)) and callback is not None:
                callbacks.remove(callback)
                callback()
            return
        self._shutdown_requested = True
        replay_dialog = getattr(self, "_area_shape_replay_dialog", None)
        if replay_dialog is not None:
            replay_dialog.close()
        # Signal preview charts belong to this page, rather than launcher's
        # separately registered windows. Keep their workers alive until joined.
        finder = getattr(self, "findChildren", None)
        self._shutdown_child_charts = list(finder(KlineAnalysisWindow)) if callable(finder) else []
        for child in self._shutdown_child_charts:
            child.begin_shutdown()
        self._refresh_timer.stop()
        self._rr_monitor_timer.stop()
        for name in (
            "_load_request_timer",
            "_pending_reload_retry_timer",
            "_deferred_chart_render_timer",
            "_layout_refresh_timer",
            "_account_prefetch_timer",
            "_area_shape_demo_refresh_timer",
        ):
            timer = getattr(self, name, None)
            if timer is not None:
                timer.stop()
        if self._realtime_candle_unsubscribe is not None:
            try:
                self._realtime_candle_unsubscribe()
            except Exception:
                pass
            self._realtime_candle_unsubscribe = None
        active_loaders = (
            getattr(self, "_loader", None),
            getattr(self, "_secondary_loader", None),
            getattr(self, "_tertiary_loader", None),
            getattr(self, "_quaternary_loader", None),
            getattr(self, "_secondary_volatility_loader", None),
            getattr(self, "_history_trade_loader", None),
            getattr(self, "_weekly_cache_backfill_loader", None),
            *tuple(getattr(self, "_area_shape_demo_workers", ())),
        )
        for loader in (*active_loaders, *tuple(self._retired_loaders)):
            if loader is not None and loader.isRunning():
                loader.requestInterruption()
        KlineAnalysisWindow._poll_shutdown_loaders(self)

    def _invalidate_profile_loaders(self) -> None:
        """Stop old-profile work before a workspace API runtime is replaced."""
        # Invalidate every callback first.  A loader may finish after the
        # interruption request, so stopping alone is not enough to prevent an
        # old API result from being rendered into the new profile's chart.
        self._request_id += 1
        self._active_request_id = self._request_id
        self._secondary_request_id += 1
        self._active_secondary_request_id = self._secondary_request_id
        self._tertiary_request_id += 1
        self._active_tertiary_request_id = self._tertiary_request_id
        self._quaternary_request_id += 1
        self._active_quaternary_request_id = self._quaternary_request_id
        self._history_trade_request_id += 1
        self._active_history_trade_request_id = self._history_trade_request_id

        history_loader = getattr(self, "_history_trade_loader", None)
        history_loader_active = history_loader is not None and history_loader.isRunning()
        for loader in (
            getattr(self, "_loader", None),
            getattr(self, "_secondary_loader", None),
            getattr(self, "_tertiary_loader", None),
            getattr(self, "_quaternary_loader", None),
            getattr(self, "_secondary_volatility_loader", None),
            history_loader,
        ):
            if loader is not None and loader.isRunning():
                loader.requestInterruption()

        self._pending_payload = None
        self._secondary_pending_payload = None
        self._tertiary_pending_payload = None
        self._quaternary_pending_payload = None
        self._pending_reload_after_load = True
        self._pending_history_trade_reload = history_loader_active

    def _schedule_load_data(self, delay_ms: int = 0) -> None:
        if bool(getattr(self, "_shutdown_requested", False)):
            return
        self._load_request_timer.start(max(0, int(delay_ms)))

    def _poll_shutdown_loaders(self) -> None:
        active_loaders = (
            getattr(self, "_loader", None),
            getattr(self, "_secondary_loader", None),
            getattr(self, "_tertiary_loader", None),
            getattr(self, "_quaternary_loader", None),
            getattr(self, "_secondary_volatility_loader", None),
            getattr(self, "_history_trade_loader", None),
            getattr(self, "_weekly_cache_backfill_loader", None),
            getattr(self, "_rr_execution_thread", None),
            *tuple(getattr(self, "_area_shape_demo_workers", ())),
            *tuple(self._retired_loaders),
        )
        active = any(
            loader is not None and loader.isRunning()
            for loader in active_loaders
        )
        active = active or any(
            not bool(getattr(child, "_shutdown_complete", False))
            for child in getattr(self, "_shutdown_child_charts", [])
        )
        if active:
            QTimer.singleShot(50, lambda: KlineAnalysisWindow._poll_shutdown_loaders(self))
            return
        drawer = getattr(self, "_account_drawer", None)
        if drawer is not None and not drawer.shutdown(wait_ms=0):
            QTimer.singleShot(50, lambda: KlineAnalysisWindow._poll_shutdown_loaders(self))
            return
        self._shutdown_complete = True
        for rr_client in getattr(self, "_rr_execution_clients", {}).values():
            rr_client.close()
        self._rr_execution_clients = {}
        callbacks = list(getattr(self, "_shutdown_callbacks", []))
        self._shutdown_callbacks = []
        for callback in callbacks:
            callback()

    def local_task_summary(self) -> dict[str, int]:
        counts = self.local_task_counts()
        return {
            "rr": sum(item.rr for item in counts),
            "line_conditions": sum(item.line_conditions for item in counts),
            "arbitrage": 0,
        }

    def local_task_counts(self) -> tuple[LocalTaskCount, ...]:
        counts: list[LocalTaskCount] = []
        for entry in self._monitorable_rr_trade_ledger_entries():
            profile_name = str(entry.plan.profile_name or "").strip()
            if profile_name:
                counts.append(LocalTaskCount(profile_name, rr=1))
        fallback_profile = self._active_profile_name()
        for workspace_entry in self._workspace_entries.values():
            if not isinstance(workspace_entry, dict):
                continue
            lines = workspace_entry.get("lines", [])
            if not isinstance(lines, list):
                continue
            for line in lines:
                if not isinstance(line, dict):
                    continue
                if not bool(line.get("enabled", True)) or not _rr_fee_offset_enabled(line.get("trade_enabled", False)):
                    continue
                profile_name = str(line.get("trade_profile_name", "") or "").strip() or fallback_profile
                if profile_name:
                    counts.append(LocalTaskCount(profile_name, line_conditions=1))
        return merge_local_task_counts(counts)

    def connection_snapshot(self) -> dict[str, object]:
        return {
            "public_online": self._realtime_candle_unsubscribe is not None or self._pending_payload is not None,
            "private_online": False,
            "private_status": "",
        }

    def resizeEvent(self, event) -> None:  # noqa: ANN001
        super().resizeEvent(event)
        self._schedule_chart_layout_refresh(80)

    def _build_header(self, parent_layout: QVBoxLayout) -> None:
        header = QFrame()
        self._header_panel = header
        header.setObjectName("Panel")
        # Dynamic dual/triple-chart controls must not propagate their combined
        # horizontal minimum width to the top-level window.
        header.setSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Preferred)
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
        self._symbol_combo = QComboBox()
        self._symbol_combo.addItems(KLINE_SYMBOL_OPTIONS)
        self._symbol_combo.setMinimumWidth(_HEADER_SYMBOL_INPUT_MIN_WIDTH)
        self._symbol_combo.setMaximumWidth(_HEADER_SYMBOL_INPUT_MAX_WIDTH)
        self._symbol_combo.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Fixed)
        self._symbol_combo.currentTextChanged.connect(lambda _symbol: self._on_symbol_confirmed())
        top_row.addWidget(self._symbol_combo, 0)

        top_row.addSpacing(8)
        self._api_profile_label = QLabel("API")
        top_row.addWidget(self._api_profile_label)
        self._api_profile_combo = QComboBox()
        self._api_profile_combo.setMinimumWidth(110)
        self._api_profile_combo.currentIndexChanged.connect(lambda _index: self._on_api_profile_changed())
        top_row.addWidget(self._api_profile_combo, 0)
        if self._embedded:
            self._api_profile_label.hide()
            self._api_profile_combo.hide()

        self._account_context = QLabel("")
        self._account_context.setObjectName("Subtle")
        top_row.addWidget(self._account_context, 0)

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
            button.setMinimumWidth(_PRIMARY_PERIOD_BUTTON_WIDTH)
            button.setFixedHeight(_PRIMARY_PERIOD_BUTTON_HEIGHT)
            button.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)
            button.clicked.connect(lambda _checked=False, value=period_value: self._on_period_button_clicked(value))
            self._primary_period_buttons[period_value] = button
            period_toolbar_layout.addWidget(button)
        top_row.addWidget(period_toolbar, 0)

        self._toggle_left_panel_btn = QPushButton("显示左栏")
        self._toggle_left_panel_btn.setCheckable(True)
        self._toggle_left_panel_btn.toggled.connect(self._toggle_left_panel)
        self._toggle_left_panel_btn.setChecked(self._left_panel_hidden)
        top_row.addWidget(self._toggle_left_panel_btn, 0)

        top_row.addSpacing(10)
        ma_group = QFrame()
        ma_group.setObjectName("ToolbarGroup")
        ma_group.setStyleSheet(
            """
            QFrame#ToolbarGroup {
                background: #f8fafc;
                border: 1px solid #cbd5e1;
                border-radius: 7px;
            }
            """
        )
        ma_group_layout = QHBoxLayout(ma_group)
        ma_group_layout.setContentsMargins(10, 3, 10, 3)
        ma_group_layout.setSpacing(8)
        ma_group_layout.addWidget(QLabel("均线"))
        self._ema9 = QCheckBox("EMA 15")
        self._ema9.setChecked(True)
        self._ema9.setToolTip("显示或隐藏 EMA 15 均线。")
        self._ema9.toggled.connect(self._sync_chart_options)
        ma_group_layout.addWidget(self._ema9)

        self._ema21 = QCheckBox("SMA 50")
        self._ema21.setChecked(True)
        self._ema21.setToolTip("显示或隐藏 SMA 50 均线。")
        self._ema21.toggled.connect(self._sync_chart_options)
        ma_group_layout.addWidget(self._ema21)

        self._ema21_55_check = QCheckBox("EMA 21/55")
        self._ema21_55_check.setToolTip("同时显示或隐藏 EMA 21 与 EMA 55。")
        self._ema21_55_check.toggled.connect(self._sync_chart_options)
        ma_group_layout.addWidget(self._ema21_55_check)

        self._atr10_check = QCheckBox("ATR 10")
        self._atr10_check.setToolTip("在K线图下方显示或隐藏 Wilder ATR(10) 波动率曲线。")
        self._atr10_check.toggled.connect(self._sync_chart_options)
        ma_group_layout.addWidget(self._atr10_check)

        self._best_parameter_indicators_check = QCheckBox("最佳参数指标")
        self._best_parameter_indicators_check.setToolTip(
            "显示当前品种已固化的最佳参数均线；会按当前 K 线周期重新计算，开启时隐藏手动选择的均线。"
        )
        self._best_parameter_indicators_check.toggled.connect(self._on_best_parameter_indicators_changed)

        self._history_trade_group = QFrame()
        self._history_trade_group.setObjectName("ToolbarGroup")
        self._history_trade_group.setStyleSheet(
            """
            QFrame#ToolbarGroup {
                background: #f8fafc;
                border: 1px solid #cbd5e1;
                border-radius: 7px;
            }
            """
        )
        history_trade_layout = QHBoxLayout(self._history_trade_group)
        history_trade_layout.setContentsMargins(10, 3, 10, 3)
        history_trade_layout.setSpacing(6)
        self._show_history_trades_check = QCheckBox("历史交易")
        self._show_history_trades_check.setChecked(True)
        self._show_history_trades_check.setToolTip("显示当前 API、环境和品种对应的 OKX 历史交易标记：开多、平多、开空、平空。")
        self._show_history_trades_check.toggled.connect(self._on_history_trade_visibility_changed)
        history_trade_layout.addWidget(self._show_history_trades_check)
        self._history_trade_direction_combo = QComboBox()
        self._history_trade_direction_combo.addItem("全部", "all")
        self._history_trade_direction_combo.addItem("多头", "long")
        self._history_trade_direction_combo.addItem("空头", "short")
        self._history_trade_direction_combo.setToolTip("筛选 K 线上的历史交易标记，并切换最佳参数指标的多头/空头配置。")
        self._history_trade_direction_combo.currentIndexChanged.connect(self._sync_chart_options)
        history_trade_layout.addWidget(self._history_trade_direction_combo)
        self._sync_history_trades_button = QPushButton("同步")
        self._sync_history_trades_button.setToolTip("重新从 OKX 同步当前品种的历史开多、平多、开空、平空记录。")
        self._sync_history_trades_button.clicked.connect(lambda: self._load_history_trades(force=True))
        history_trade_layout.addWidget(self._sync_history_trades_button)

        shape_signal_tooltip = (
            "形态说明：1H/4H/1D/1W 显示核心标志K触发的形态信号。\n"
            "核心K按振幅排序，需在含自身最近10根K线中排前4。\n"
            "勾选碰均线后，核心K还需触碰对应均线；1H 只参考 SMA50，4H/1D/1W 参考 EMA15 或 MA50。"
        )
        shape_group = QFrame()
        self._shape_signal_group = shape_group
        shape_group.setObjectName("ToolbarGroup")
        shape_group.setStyleSheet(
            """
            QFrame#ToolbarGroup {
                background: #f8fafc;
                border: 1px solid #cbd5e1;
                border-radius: 7px;
            }
            """
        )
        shape_group_layout = QHBoxLayout(shape_group)
        shape_group_layout.setContentsMargins(10, 3, 10, 3)
        shape_group_layout.setSpacing(8)
        shape_label = QLabel("形态")
        shape_label.setToolTip(shape_signal_tooltip)
        shape_group_layout.addWidget(shape_label)

        self._show_1h_shape_signal_check = QCheckBox("1H")
        self._show_1h_shape_signal_check.setChecked(False)
        self._show_1h_shape_signal_check.setToolTip(shape_signal_tooltip)
        self._show_1h_shape_signal_check.toggled.connect(self._sync_chart_options)
        self._show_1h_shape_signal_check.toggled.connect(self._on_shape_signal_visibility_changed)
        shape_group_layout.addWidget(self._show_1h_shape_signal_check)

        self._show_4h_shape_signal_check = QCheckBox("4H")
        self._show_4h_shape_signal_check.setChecked(False)
        self._show_4h_shape_signal_check.setToolTip(shape_signal_tooltip)
        self._show_4h_shape_signal_check.toggled.connect(self._sync_chart_options)
        self._show_4h_shape_signal_check.toggled.connect(self._on_shape_signal_visibility_changed)
        shape_group_layout.addWidget(self._show_4h_shape_signal_check)

        self._show_1d_shape_signal_check = QCheckBox("1D")
        self._show_1d_shape_signal_check.setChecked(False)
        self._show_1d_shape_signal_check.setToolTip(shape_signal_tooltip)
        self._show_1d_shape_signal_check.toggled.connect(self._sync_chart_options)
        self._show_1d_shape_signal_check.toggled.connect(self._on_shape_signal_visibility_changed)
        shape_group_layout.addWidget(self._show_1d_shape_signal_check)

        self._show_1w_shape_signal_check = QCheckBox("1W")
        self._show_1w_shape_signal_check.setChecked(False)
        self._show_1w_shape_signal_check.setToolTip(shape_signal_tooltip)
        self._show_1w_shape_signal_check.toggled.connect(self._sync_chart_options)
        self._show_1w_shape_signal_check.toggled.connect(self._on_shape_signal_visibility_changed)
        shape_group_layout.addWidget(self._show_1w_shape_signal_check)

        self._shape_signal_size_metric_btn = QPushButton("")
        self._shape_signal_size_metric_btn.setToolTip(shape_signal_tooltip)
        self._shape_signal_size_metric_btn.clicked.connect(self._toggle_shape_signal_size_metric)
        shape_group_layout.addWidget(self._shape_signal_size_metric_btn)

        self._shape_signal_ma_touch_check = QCheckBox("碰均线")
        self._shape_signal_ma_touch_check.setChecked(False)
        self._shape_signal_ma_touch_check.setToolTip(shape_signal_tooltip)
        self._shape_signal_ma_touch_check.toggled.connect(self._sync_chart_options)
        shape_group_layout.addWidget(self._shape_signal_ma_touch_check)

        self._shape_settings_button = QPushButton("形态：关")
        self._shape_settings_button.setToolTip(shape_signal_tooltip)
        shape_menu = QMenu(self._shape_settings_button)
        self._shape_setting_actions: dict[str, QAction] = {}
        for label, checkbox in (
            ("1H", self._show_1h_shape_signal_check),
            ("4H", self._show_4h_shape_signal_check),
            ("1D", self._show_1d_shape_signal_check),
            ("1W", self._show_1w_shape_signal_check),
            ("碰均线", self._shape_signal_ma_touch_check),
        ):
            action = shape_menu.addAction(label)
            action.setCheckable(True)
            action.setChecked(checkbox.isChecked())
            action.toggled.connect(checkbox.setChecked)
            checkbox.toggled.connect(action.setChecked)
            checkbox.toggled.connect(self._refresh_compact_shape_button)
            self._shape_setting_actions[label] = action
        shape_menu.addSeparator()
        subscribe_action = shape_menu.addAction("后台形态订阅")
        subscribe_action.triggered.connect(self._open_shape_subscription_dialog)
        history_action = shape_menu.addAction("历史形态信号")
        history_action.triggered.connect(self._open_shape_signal_history)
        self._shape_settings_button.setMenu(shape_menu)
        if self._embedded:
            self._shape_signal_group.hide()
        else:
            self._shape_settings_button.hide()

        self._status = QLabel("就绪")
        self._status.setObjectName("Subtle")
        top_row.addStretch(1)
        top_row.addWidget(self._status, 3, Qt.AlignmentFlag.AlignRight)
        self._layout_context_panel = QFrame()
        linkage_row = QHBoxLayout(self._layout_context_panel)
        linkage_row.setContentsMargins(0, 0, 0, 0)
        linkage_row.setSpacing(10)
        self._secondary_chart_check = QCheckBox("双图联动")
        self._secondary_chart_check.toggled.connect(self._on_secondary_chart_toggled)
        linkage_row.addWidget(self._secondary_chart_check)

        self._secondary_symbol_label = QLabel("副图交易对")
        linkage_row.addWidget(self._secondary_symbol_label)
        self._secondary_symbol_combo = QComboBox()
        self._secondary_symbol_combo.addItems(KLINE_SYMBOL_OPTIONS)
        self._secondary_symbol_combo.setMinimumWidth(_HEADER_SYMBOL_INPUT_MIN_WIDTH)
        self._secondary_symbol_combo.setMaximumWidth(_HEADER_SYMBOL_INPUT_MAX_WIDTH)
        self._secondary_symbol_combo.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Fixed)
        self._secondary_symbol_combo.setEnabled(False)
        self._secondary_symbol_combo.hide()
        self._secondary_symbol_combo.currentTextChanged.connect(self._on_secondary_symbol_changed)
        linkage_row.addWidget(self._secondary_symbol_combo, 0)
        self._secondary_symbol_label.hide()

        self._tertiary_chart_check = QCheckBox("三图联动")
        self._tertiary_chart_check.toggled.connect(self._on_tertiary_chart_toggled)
        linkage_row.addWidget(self._tertiary_chart_check)
        self._tertiary_symbol_label = QLabel("第三图交易对")
        linkage_row.addWidget(self._tertiary_symbol_label)
        self._tertiary_symbol_combo = QComboBox()
        self._tertiary_symbol_combo.addItems(KLINE_SYMBOL_OPTIONS)
        self._tertiary_symbol_combo.setMinimumWidth(_HEADER_SYMBOL_INPUT_MIN_WIDTH)
        self._tertiary_symbol_combo.setMaximumWidth(_HEADER_SYMBOL_INPUT_MAX_WIDTH)
        self._tertiary_symbol_combo.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Fixed)
        self._tertiary_symbol_combo.setEnabled(False)
        self._tertiary_symbol_combo.hide()
        self._tertiary_symbol_combo.currentTextChanged.connect(self._on_tertiary_symbol_changed)
        linkage_row.addWidget(self._tertiary_symbol_combo, 0)
        self._tertiary_symbol_label.hide()
        self._tertiary_period_label = QLabel("第三图周期")
        linkage_row.addWidget(self._tertiary_period_label)
        self._tertiary_period_combo = QComboBox()
        self._tertiary_period_combo.addItems([period for _, period in _PRIMARY_PERIOD_OPTIONS])
        self._tertiary_period_combo.setCurrentText(_DEFAULT_DUAL_SECONDARY_PERIOD)
        self._tertiary_period_combo.setEnabled(False)
        self._tertiary_period_combo.hide()
        self._tertiary_period_combo.currentTextChanged.connect(self._on_tertiary_period_changed)
        linkage_row.addWidget(self._tertiary_period_combo, 0)
        self._tertiary_period_label.hide()

        self._quaternary_chart_check = QCheckBox("四图联动")
        self._quaternary_chart_check.toggled.connect(self._on_quaternary_chart_toggled)
        linkage_row.addWidget(self._quaternary_chart_check)
        self._quaternary_symbol_label = QLabel("第四图交易对")
        linkage_row.addWidget(self._quaternary_symbol_label)
        self._quaternary_symbol_combo = QComboBox()
        self._quaternary_symbol_combo.addItems(KLINE_SYMBOL_OPTIONS)
        self._quaternary_symbol_combo.setMinimumWidth(_HEADER_SYMBOL_INPUT_MIN_WIDTH)
        self._quaternary_symbol_combo.setMaximumWidth(_HEADER_SYMBOL_INPUT_MAX_WIDTH)
        self._quaternary_symbol_combo.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Fixed)
        self._quaternary_symbol_combo.setEnabled(False)
        self._quaternary_symbol_combo.hide()
        self._quaternary_symbol_combo.currentTextChanged.connect(self._on_quaternary_symbol_changed)
        linkage_row.addWidget(self._quaternary_symbol_combo, 0)
        self._quaternary_symbol_label.hide()
        self._quaternary_period_label = QLabel("第四图周期")
        linkage_row.addWidget(self._quaternary_period_label)
        self._quaternary_period_combo = QComboBox()
        self._quaternary_period_combo.addItems([period for _, period in _PRIMARY_PERIOD_OPTIONS])
        self._quaternary_period_combo.setCurrentText("1W")
        self._quaternary_period_combo.setEnabled(False)
        self._quaternary_period_combo.hide()
        self._quaternary_period_combo.currentTextChanged.connect(self._on_quaternary_period_changed)
        linkage_row.addWidget(self._quaternary_period_combo, 0)
        self._quaternary_period_label.hide()

        self._secondary_period_label = QLabel("副图周期")
        linkage_row.addWidget(self._secondary_period_label)
        self._secondary_period_combo = QComboBox()
        self._secondary_period_combo.addItems([period for _, period in _PRIMARY_PERIOD_OPTIONS])
        self._secondary_period_combo.addItem("1Dutc")
        self._secondary_period_combo.setCurrentText(_DEFAULT_DUAL_SECONDARY_PERIOD)
        self._secondary_period_combo.setEnabled(False)
        self._secondary_period_combo.hide()
        self._secondary_period_combo.currentTextChanged.connect(self._on_secondary_period_changed)
        linkage_row.addWidget(self._secondary_period_combo, 0)
        self._secondary_period_label.hide()

        self._secondary_layout_cycle_btn = QPushButton("")
        self._secondary_layout_cycle_btn.setEnabled(False)
        self._secondary_layout_cycle_btn.clicked.connect(self._on_secondary_layout_cycle_clicked)
        linkage_row.addWidget(self._secondary_layout_cycle_btn)

        self._secondary_chart_kind_btn = QPushButton("")
        self._secondary_chart_kind_btn.setEnabled(False)
        self._secondary_chart_kind_btn.clicked.connect(self._on_secondary_chart_kind_cycle_clicked)
        linkage_row.addWidget(self._secondary_chart_kind_btn)

        self._secondary_sync_period_btn = QPushButton("")
        self._secondary_sync_period_btn.setEnabled(False)
        self._secondary_sync_period_btn.setToolTip("副图为K线时：主图1D、副图4H并切换到最近视图")
        self._secondary_sync_period_btn.clicked.connect(self._on_secondary_sync_period_clicked)
        linkage_row.addWidget(self._secondary_sync_period_btn)

        self._daily_timezone_compare_btn = QPushButton("UTC+8/UTC日线")
        self._daily_timezone_compare_btn.setToolTip("同一交易对左右比较：左图 UTC+8 日线，右图 UTC 日线")
        self._daily_timezone_compare_btn.clicked.connect(self._on_daily_timezone_compare_clicked)
        linkage_row.addWidget(self._daily_timezone_compare_btn)
        linkage_row.addStretch(1)

        view_options_menu = QMenu(header)
        view_options_widget = QWidget(view_options_menu)
        view_options_layout = QVBoxLayout(view_options_widget)
        view_options_layout.setContentsMargins(8, 8, 8, 8)
        view_options_layout.setSpacing(6)
        data_options_row = QHBoxLayout()
        data_options_row.addWidget(QLabel("K线数量"))
        self._limit_spin = QSpinBox()
        self._limit_spin.setRange(50, 5000)
        self._limit_spin.setSingleStep(50)
        self._limit_spin.setValue(1200)
        self._limit_spin.valueChanged.connect(self._load_data)
        data_options_row.addWidget(self._limit_spin)

        self._prefer_local_checkbox = QCheckBox("本地优先")
        self._prefer_local_checkbox.setChecked(False)
        self._prefer_local_checkbox.toggled.connect(self._load_data)
        data_options_row.addWidget(self._prefer_local_checkbox)
        data_options_row.addStretch(1)
        view_options_layout.addLayout(data_options_row)

        self._secondary_average_kline_check = QCheckBox("平均K线")
        self._secondary_average_kline_check.setToolTip("开启后，主图和副图都使用平均K线算法显示K线。")
        self._secondary_average_kline_check.toggled.connect(self._on_secondary_average_kline_toggled)

        self._primary_average_secondary_normal_check = QCheckBox("主均副普")
        self._primary_average_secondary_normal_check.setToolTip(
            "仅在双图且副图为K线时生效：左右分屏为左图平均、右图正常；上下分屏为上图平均、下图正常。"
        )
        self._primary_average_secondary_normal_check.setEnabled(False)
        self._primary_average_secondary_normal_check.toggled.connect(
            self._on_primary_average_secondary_normal_toggled
        )

        self._reverse_kline_check = QCheckBox("K线反转")
        self._reverse_kline_check.setToolTip("开启后，将当前主图及副图K线按价格镜像反转显示；波动率副图不参与反转。")
        self._reverse_kline_check.toggled.connect(self._load_data)

        # 这三个选项使用频率较高，直接放到主工具栏；数量、本地优先等
        # 低频设置仍保留在“视图选项”菜单中，避免每次操作都要展开菜单。
        kline_display_group = QFrame()
        kline_display_group.setObjectName("ToolbarGroup")
        kline_display_group.setStyleSheet(
            """
            QFrame#ToolbarGroup {
                background: #f8fafc;
                border: 1px solid #cbd5e1;
                border-radius: 7px;
            }
            """
        )
        kline_display_layout = QHBoxLayout(kline_display_group)
        kline_display_layout.setContentsMargins(10, 3, 10, 3)
        kline_display_layout.setSpacing(8)
        kline_display_layout.addWidget(QLabel("显示"))
        for display_widget in (
            self._secondary_average_kline_check,
            self._primary_average_secondary_normal_check,
            self._reverse_kline_check,
        ):
            kline_display_layout.addWidget(display_widget)

        chart_options_row = QHBoxLayout()
        self._hide_chart_btn = QPushButton("隐藏图表")
        self._hide_chart_btn.setCheckable(True)
        self._hide_chart_btn.toggled.connect(self._toggle_chart_visibility)
        chart_options_row.addWidget(self._hide_chart_btn)

        self._chart_fullscreen_button = QPushButton("全屏图表")
        self._chart_fullscreen_button.setToolTip("最大化显示当前 K 线布局并保留窗口标题栏；按 F11 切换，Esc 退出。")
        self._chart_fullscreen_button.clicked.connect(self._toggle_chart_fullscreen)
        chart_options_row.addWidget(self._chart_fullscreen_button)

        self._chart_screenshot_button = QPushButton("截图到剪贴板")
        self._chart_screenshot_button.setToolTip("将当前双图、三图或四图 K 线布局复制到系统剪贴板。")
        self._chart_screenshot_button.clicked.connect(self._copy_chart_screenshot_to_clipboard)
        chart_options_row.addWidget(self._chart_screenshot_button)

        chart_options_row.addStretch(1)
        view_options_layout.addLayout(chart_options_row)
        view_options_action = QWidgetAction(view_options_menu)
        view_options_action.setDefaultWidget(view_options_widget)
        view_options_menu.addAction(view_options_action)
        view_options_button = QPushButton("视图选项")
        view_options_button.setToolTip("设置 K 线数量、数据来源和图表显示选项。")
        view_options_button.setMenu(view_options_menu)
        self._trade_recommendation_button = QPushButton("做单参考")
        self._trade_recommendation_button.setToolTip("分析当前选中图的普通已收盘K线，生成现货/永续做单参考；非期权，不会下单。")
        self._trade_recommendation_button.clicked.connect(self._show_trade_recommendation)

        load_btn = QPushButton("加载")
        load_btn.setObjectName("Primary")
        load_btn.clicked.connect(self._load_data)

        self._auto_refresh_btn = QPushButton("自动刷新:开" if _AUTO_REFRESH_DEFAULT_ENABLED else "自动刷新:关")
        self._auto_refresh_btn.setCheckable(True)
        self._auto_refresh_btn.setChecked(_AUTO_REFRESH_DEFAULT_ENABLED)
        self._auto_refresh_btn.toggled.connect(self._toggle_auto_refresh)

        self._chart_range_mode_btn = QPushButton("全量视图")
        self._chart_range_mode_btn.clicked.connect(self._toggle_chart_view_range_mode)
        workspace_row = QHBoxLayout()
        workspace_row.setSpacing(8)
        workspace_row.addWidget(ma_group, 0)
        workspace_row.addWidget(self._best_parameter_indicators_check, 0)
        workspace_row.addWidget(self._history_trade_group, 0)
        workspace_row.addWidget(shape_group, 0)
        workspace_row.addWidget(self._shape_settings_button, 0)
        workspace_row.addWidget(kline_display_group, 0)
        for chart_check in (
            self._secondary_chart_check,
            self._tertiary_chart_check,
            self._quaternary_chart_check,
        ):
            linkage_row.removeWidget(chart_check)
            workspace_row.addWidget(chart_check, 0)
        workspace_row.addStretch(1)
        workspace_row.addWidget(self._trade_recommendation_button, 0)
        self._account_drawer_button = QPushButton("委托/持仓")
        self._account_drawer_button.setToolTip("打开下方委托和持仓面板")
        self._account_drawer_button.clicked.connect(self._toggle_account_drawer)
        workspace_row.addWidget(self._account_drawer_button, 0)
        workspace_row.addWidget(view_options_button, 0)
        workspace_row.addWidget(load_btn, 0)
        workspace_row.addWidget(self._auto_refresh_btn, 0)
        workspace_row.addWidget(self._chart_range_mode_btn, 0)

        self._layout_context_panel.setVisible(False)
        header_layout.addLayout(top_row)
        header_layout.addLayout(workspace_row)
        header_layout.addWidget(self._layout_context_panel)

        self._refresh_api_profiles()
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
        control_scroll = QScrollArea()
        self._control_scroll = control_scroll
        control_scroll.setWidgetResizable(True)
        control_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        control_scroll.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        control_scroll.setFrameShape(QFrame.Shape.NoFrame)
        control_scroll.setWidget(control)

        control_layout = QVBoxLayout(control)
        control_layout.setContentsMargins(8, 8, 8, 8)
        control_layout.setSpacing(6)

        control_tabs = QTabWidget()
        self._control_tabs = control_tabs
        control_tabs.setDocumentMode(True)
        control_tabs.setTabPosition(QTabWidget.TabPosition.North)
        control_tabs.tabBar().setUsesScrollButtons(True)
        control_layout.addWidget(control_tabs)

        analysis_page = QWidget()
        analysis_layout = QVBoxLayout(analysis_page)
        analysis_layout.setContentsMargins(4, 6, 4, 4)
        analysis_layout.setSpacing(6)
        control_tabs.addTab(analysis_page, "分析")

        drawing_page = QWidget()
        drawing_layout = QVBoxLayout(drawing_page)
        drawing_layout.setContentsMargins(4, 6, 4, 4)
        drawing_layout.setSpacing(6)
        control_tabs.addTab(drawing_page, "画线告警")

        rr_page = QWidget()
        rr_layout = QVBoxLayout(rr_page)
        rr_layout.setContentsMargins(4, 6, 4, 4)
        rr_layout.setSpacing(6)
        control_tabs.addTab(rr_page, "RR交易")
        self._control_rr_tab_index = control_tabs.indexOf(rr_page)

        # Keep an internal mode value for existing workflow actions; users now
        # choose the workspace directly with the top-level tabs.
        self._rr_workspace_mode_combo = QComboBox(self)
        self._rr_workspace_mode_combo.addItem("直接入场", "direct")
        self._rr_workspace_mode_combo.addItem("区域形态入场", "area")
        self._rr_workspace_mode_combo.currentIndexChanged.connect(self._on_rr_workspace_mode_changed)
        self._rr_workspace_mode_combo.hide()
        self._rr_workspace_other_status = _WrappedStatusLabel("")
        self._rr_workspace_other_status.setObjectName("Subtle")
        self._rr_workspace_other_status.hide()
        rr_layout.addWidget(self._rr_workspace_other_status)

        area_page = QWidget()
        area_layout = QVBoxLayout(area_page)
        area_layout.setContentsMargins(4, 6, 4, 4)
        area_layout.setSpacing(6)
        control_tabs.addTab(area_page, "智能辅助交易")
        self._control_area_tab_index = control_tabs.indexOf(area_page)
        self._area_workspace_other_status = _WrappedStatusLabel("")
        self._area_workspace_other_status.setObjectName("Subtle")
        self._area_workspace_other_status.hide()
        area_layout.addWidget(self._area_workspace_other_status)

        area_notice = QLabel("支持纸面演练、OKX 模拟盘和实盘。")
        area_notice.setObjectName("Subtle")
        area_notice.setWordWrap(True)
        area_notice.setSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Preferred)
        area_layout.addWidget(area_notice)
        self._area_shape_status = QLabel("区域形态：尚未创建计划")
        self._area_shape_status.setObjectName("Subtle")
        self._area_shape_status.setWordWrap(True)
        self._area_shape_status.setSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Preferred)
        area_layout.addWidget(self._area_shape_status)
        self._area_shape_lines = QLabel("下方支撑：待生成\n上方压力：待生成")
        self._area_shape_lines.setWordWrap(True)
        self._area_shape_lines.setSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Preferred)
        area_layout.addWidget(self._area_shape_lines)
        self._area_shape_price_button = QPushButton("手动调整支撑/压力")
        self._area_shape_price_button.setToolTip("为当前草稿直接输入支撑和压力价；会同步移动图上的两条射线。监控计划不可修改。")
        self._area_shape_price_button.clicked.connect(self._edit_area_shape_prices)
        self._area_shape_price_button.setSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Fixed)
        area_layout.addWidget(self._area_shape_price_button)
        area_layout.addWidget(QLabel("执行模式"))
        self._area_shape_mode_combo = QComboBox()
        self._area_shape_mode_combo.addItem("纸面演练", "paper")
        self._area_shape_mode_combo.addItem("OKX模拟盘", "demo")
        self._area_shape_mode_combo.addItem("OKX实盘", "live")
        self._area_shape_mode_combo.setToolTip("价格触发模式：做多下穿支撑线、做空上穿压力线时按画线价模拟入场；实时流不可用时按1H OHLC检查同方向穿越。OKX实盘/模拟盘在对应入场线挂GTC限价单。形态模式仍等待1H收盘确认。")
        self._area_shape_mode_combo.setSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Fixed)
        self._area_shape_mode_combo.setMinimumContentsLength(8)
        self._area_shape_mode_combo.setSizeAdjustPolicy(QComboBox.SizeAdjustPolicy.AdjustToMinimumContentsLengthWithIcon)
        self._area_shape_mode_combo.currentIndexChanged.connect(self._on_area_shape_mode_changed)
        area_layout.addWidget(self._area_shape_mode_combo)
        self._area_shape_create_button = QPushButton("一键两线")
        self._area_shape_create_button.setToolTip("快速取最近120根1H K线的最低点和最高点生成两条线；生成后可在图上手工调整。")
        self._area_shape_create_button.clicked.connect(self._create_area_shape_plan)
        self._area_shape_create_button.setSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Fixed)
        area_layout.addWidget(self._area_shape_create_button)
        self._area_shape_auto_button = QPushButton("自动找区域")
        self._area_shape_auto_button.setToolTip("从最近240根已收盘1H K线中，聚合至少2个确认摆动点识别当前价格下方支撑、上方压力，生成可手调的两条射线；不自动下单，不随行情移动。已有两线时不会覆盖，请先删除草稿和两线。")
        self._area_shape_auto_button.clicked.connect(self._auto_create_area_shape_plan)
        self._area_shape_auto_button.setSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Fixed)
        area_layout.addWidget(self._area_shape_auto_button)
        self._area_shape_edit_button = QPushButton("调整参数")
        self._area_shape_edit_button.setToolTip("设置方向、单笔风险、最低可用空间、离场模板及高级ATR/成本参数。监控启动后参数冻结。")
        self._area_shape_edit_button.clicked.connect(self._edit_area_shape_plan)
        self._area_shape_edit_button.setSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Fixed)
        area_layout.addWidget(self._area_shape_edit_button)
        self._area_shape_replay_button = QPushButton("复盘交易")
        self._area_shape_replay_button.setToolTip(
            "打开当前账户、币种和周期已保存的多空复盘与潜在机会；首次没有结果时才计算。"
            "调整参数、射线或行情刷新不会自动重算；要更新结果，请在窗口中点‘重新复盘’。"
        )
        self._area_shape_replay_button.clicked.connect(self._run_area_shape_replay)
        self._area_shape_replay_button.setSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Fixed)
        area_layout.addWidget(self._area_shape_replay_button)
        self._area_shape_projection_button = QPushButton("未来推演")
        self._area_shape_projection_button.setToolTip("选择一种1H形态，在图表右侧绘制未来情景K线以及入场、止损和目标线；只作示意，不会下单。")
        self._area_shape_projection_button.clicked.connect(self._toggle_area_shape_future_projection)
        self._area_shape_projection_button.setSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Fixed)
        area_layout.addWidget(self._area_shape_projection_button)
        self._area_shape_start_button = QPushButton("启用计划")
        self._area_shape_start_button.setToolTip("核对方向、入场线和另一条线的盈亏比后启用。每个计划只运行一个方向；OKX实盘/模拟盘启用前会显示对应环境的订单确认。")
        self._area_shape_start_button.clicked.connect(self._start_area_shape_monitoring)
        self._area_shape_start_button.setSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Fixed)
        area_layout.addWidget(self._area_shape_start_button)
        self._area_shape_pause_button = QPushButton("暂停新入场")
        self._area_shape_pause_button.setToolTip("停止新入场；OKX实盘/模拟盘会请求撤销未成交入场单，已有仓位的交易所保护继续运行。")
        self._area_shape_pause_button.clicked.connect(self._pause_area_shape_monitoring)
        self._area_shape_pause_button.setSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Fixed)
        area_layout.addWidget(self._area_shape_pause_button)
        self._area_shape_cancel_button = QPushButton("取消计划")
        self._area_shape_cancel_button.setToolTip("结束当前方向的计划。不会平掉已有仓位，也不会撤销其交易所止盈止损保护。")
        self._area_shape_cancel_button.clicked.connect(self._cancel_area_shape_plan)
        self._area_shape_cancel_button.setSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Fixed)
        area_layout.addWidget(self._area_shape_cancel_button)
        demo_actions = QHBoxLayout()
        self._area_shape_reconcile_button = QPushButton("只读对账")
        self._area_shape_reconcile_button.setEnabled(False)
        self._area_shape_reconcile_button.setSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Fixed)
        self._area_shape_reconcile_button.clicked.connect(self._reconcile_area_shape_demo_session)
        demo_actions.addWidget(self._area_shape_reconcile_button)
        self._area_shape_rearm_button = QPushButton("继续保护")
        self._area_shape_rearm_button.setEnabled(False)
        self._area_shape_rearm_button.setToolTip("只读对账成功后，显式恢复原持仓保护管理；不会复制旧信号或新开仓。")
        self._area_shape_rearm_button.setSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Fixed)
        self._area_shape_rearm_button.clicked.connect(self._rearm_area_shape_demo_session)
        demo_actions.addWidget(self._area_shape_rearm_button)
        area_layout.addLayout(demo_actions)
        self._area_shape_demo_status = QLabel("OKX执行：未启用")
        self._area_shape_demo_status.setObjectName("Subtle")
        self._area_shape_demo_status.setWordWrap(True)
        self._area_shape_demo_status.setSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Preferred)
        area_layout.addWidget(self._area_shape_demo_status)
        self._area_shape_summary = QLabel("直接价格触发：做多下穿支撑、做空上穿压力后按画线价模拟入场；形态确认模式按下一根1H开盘价推演。纸面不发送订单。")
        self._area_shape_summary.setObjectName("Subtle")
        self._area_shape_summary.setWordWrap(True)
        self._area_shape_summary.setSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Preferred)
        self._area_shape_summary.setSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Preferred)
        area_layout.addWidget(self._area_shape_summary)
        self._area_shape_plan_details = QLabel("计划参数：请先创建计划并保存参数。")
        self._area_shape_plan_details.setObjectName("Subtle")
        self._area_shape_plan_details.setWordWrap(True)
        self._area_shape_plan_details.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        self._area_shape_plan_details.setSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Preferred)
        area_layout.addWidget(self._area_shape_plan_details)
        area_layout.addStretch(1)

        self._control_width_policy_snapshot = (
            control.sizePolicy(), control.minimumWidth(),
            control_tabs.sizePolicy(), control_tabs.minimumWidth(),
        )
        control_tabs.currentChanged.connect(self._on_control_tab_changed)
        self._on_control_tab_changed(control_tabs.currentIndex())
        self._refresh_rr_workspace_other_status("direct")

        self._backend_hint = QLabel("")
        self._backend_hint.setObjectName("Subtle")
        self._backend_hint.setWordWrap(True)
        analysis_layout.addWidget(self._backend_hint)

        self._rr_trade_hint = QLabel("")
        self._rr_trade_hint.setObjectName("Subtle")
        self._rr_trade_hint.setWordWrap(True)
        rr_layout.addWidget(self._rr_trade_hint)

        analysis_layout.addWidget(QLabel("告警引擎"))
        self._ma_cross_alert_check = QCheckBox("EMA 15 与 SMA 50 交叉")
        self._ma_cross_alert_check.toggled.connect(self._save_workspace_settings)
        analysis_layout.addWidget(self._ma_cross_alert_check)

        self._box_breakout_alert_check = QCheckBox("自动箱体突破")
        self._box_breakout_alert_check.toggled.connect(self._save_workspace_settings)
        analysis_layout.addWidget(self._box_breakout_alert_check)

        analysis_layout.addWidget(QLabel("自动通道"))
        self._auto_box_check = QCheckBox("显示自动箱体")
        self._auto_box_check.setChecked(False)
        self._auto_box_check.toggled.connect(self._on_auto_box_visibility_changed)
        analysis_layout.addWidget(self._auto_box_check)

        self._history_box_check = QCheckBox("显示历史箱体")
        self._history_box_check.setChecked(False)
        self._history_box_check.toggled.connect(self._on_auto_box_visibility_changed)
        analysis_layout.addWidget(self._history_box_check)
        self._live_box_check = self._history_box_check

        self._auto_channel_check = QCheckBox("显示通道")
        self._auto_channel_check.setChecked(False)
        self._auto_channel_check.toggled.connect(self._on_auto_channel_visibility_changed)
        analysis_layout.addWidget(self._auto_channel_check)

        self._auto_channel_settings_button = QPushButton("通道参数")
        auto_channel_menu = QMenu(self._auto_channel_settings_button)
        auto_channel_form = QWidget(auto_channel_menu)
        auto_channel_layout = QFormLayout(auto_channel_form)
        auto_channel_layout.setContentsMargins(8, 8, 8, 8)
        self._auto_channel_anchor_spin = QSpinBox(auto_channel_form)
        self._auto_channel_anchor_spin.setRange(1, 120)
        self._auto_channel_anchor_spin.setValue(8)
        self._auto_channel_min_bars_spin = QSpinBox(auto_channel_form)
        self._auto_channel_min_bars_spin.setRange(2, 500)
        self._auto_channel_min_bars_spin.setValue(18)
        self._auto_channel_violations_spin = QSpinBox(auto_channel_form)
        self._auto_channel_violations_spin.setRange(0, 120)
        self._auto_channel_violations_spin.setValue(8)
        auto_channel_layout.addRow("锚点间距", self._auto_channel_anchor_spin)
        auto_channel_layout.addRow("最少 K", self._auto_channel_min_bars_spin)
        auto_channel_layout.addRow("最大违规", self._auto_channel_violations_spin)
        auto_channel_action = QWidgetAction(auto_channel_menu)
        auto_channel_action.setDefaultWidget(auto_channel_form)
        auto_channel_menu.addAction(auto_channel_action)
        self._auto_channel_settings_button.setMenu(auto_channel_menu)
        for spin in (
            self._auto_channel_anchor_spin,
            self._auto_channel_min_bars_spin,
            self._auto_channel_violations_spin,
        ):
            spin.valueChanged.connect(self._on_auto_channel_parameters_changed)
        analysis_layout.addWidget(self._auto_channel_settings_button)

        self._structure_hint = QLabel("")
        self._structure_hint.setObjectName("Subtle")
        self._structure_hint.setWordWrap(True)
        analysis_layout.addWidget(self._structure_hint)

        drawing_layout.addWidget(QLabel("画线预警"))
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
        rr_long_btn = QPushButton("RR多")
        rr_long_btn.clicked.connect(lambda: self._set_draw_tool("rr_long"))
        line_toolbar.addWidget(rr_long_btn)
        rr_short_btn = QPushButton("RR空")
        rr_short_btn.clicked.connect(lambda: self._set_draw_tool("rr_short"))
        line_toolbar.addWidget(rr_short_btn)
        drawing_layout.addLayout(line_toolbar)

        self._line_label_edit = QLineEdit()
        self._line_label_edit.setPlaceholderText("线条名称")
        drawing_layout.addWidget(self._line_label_edit)

        line_price_row = QHBoxLayout()
        self._line_price_a_label = QLabel("价格")
        line_price_row.addWidget(self._line_price_a_label)
        self._line_price_a_edit = QLineEdit()
        self._line_price_a_edit.setPlaceholderText("价格")
        line_price_row.addWidget(self._line_price_a_edit, 1)
        self._line_price_b_label = QLabel("终点价")
        line_price_row.addWidget(self._line_price_b_label)
        self._line_price_b_edit = QLineEdit()
        self._line_price_b_edit.setPlaceholderText("终点价")
        line_price_row.addWidget(self._line_price_b_edit, 1)
        drawing_layout.addLayout(line_price_row)
        self._refresh_line_price_controls(None)

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
        self._line_action_combo.currentIndexChanged.connect(lambda _index: self._refresh_line_email_controls())
        line_rule_row.addWidget(self._line_action_combo, 1)
        drawing_layout.addLayout(line_rule_row)

        line_email_row = QHBoxLayout()
        self._line_email_enabled_check = QCheckBox("邮件提醒")
        self._line_email_enabled_check.setToolTip("触发“提醒”时按系统邮箱配置发送邮件。")
        self._line_email_enabled_check.toggled.connect(lambda _checked: self._refresh_line_email_controls())
        line_email_row.addWidget(self._line_email_enabled_check)
        self._line_email_delivery_mode_combo = QComboBox()
        self._line_email_delivery_mode_combo.addItem("仅一次", "once")
        self._line_email_delivery_mode_combo.addItem("每次触发", "repeat")
        self._line_email_delivery_mode_combo.setToolTip("“仅一次”在该线首次触发并提交邮件后不再重复发送。")
        line_email_row.addWidget(self._line_email_delivery_mode_combo, 1)
        drawing_layout.addLayout(line_email_row)

        self._line_enabled_check = QCheckBox("启用当前线条")
        drawing_layout.addWidget(self._line_enabled_check)

        line_trade_row = QHBoxLayout()
        self._line_trade_enabled_check = QCheckBox("启用线条交易")
        self._line_trade_enabled_check.setToolTip("仅允许该线条在触发时创建交易计划；默认关闭。")
        line_trade_row.addWidget(self._line_trade_enabled_check)
        self._line_trade_execution_mode_combo = QComboBox()
        self._line_trade_execution_mode_combo.addItem("限价", "limit")
        self._line_trade_execution_mode_combo.addItem("市价", "market")
        self._line_trade_execution_mode_combo.addItem("盘口追单", "chase_best_quote")
        self._line_trade_execution_mode_combo.setToolTip("线条触发后的开仓方式。")
        line_trade_row.addWidget(self._line_trade_execution_mode_combo, 1)
        line_trade_config_btn = QPushButton("交易参数")
        line_trade_config_btn.clicked.connect(self._open_line_trade_card_for_selected)
        line_trade_row.addWidget(line_trade_config_btn)
        drawing_layout.addLayout(line_trade_row)
        self._line_trade_armed_check = QCheckBox("全局线条交易")
        self._line_trade_armed_check.setToolTip("总开关默认关闭。开启后，满足条件的线条触发才会自动提交订单。")
        self._line_trade_armed_check.toggled.connect(self._on_line_trade_armed_toggled)
        drawing_layout.addWidget(self._line_trade_armed_check)
        self._line_trade_hint = QLabel("线条交易默认关闭：需同时启用当前线条、启用线条交易和全局线条交易。")
        self._line_trade_hint.setObjectName("Subtle")
        self._line_trade_hint.setWordWrap(True)
        drawing_layout.addWidget(self._line_trade_hint)

        line_manage_row = QHBoxLayout()
        update_line_btn = QPushButton("更新")
        update_line_btn.clicked.connect(self._update_selected_line)
        line_manage_row.addWidget(update_line_btn)
        delete_line_btn = QPushButton("删除")
        delete_line_btn.clicked.connect(self._delete_selected_line)
        line_manage_row.addWidget(delete_line_btn)
        drawing_layout.addLayout(line_manage_row)

        self._line_table = QTableWidget(0, 6)
        self._line_table.setHorizontalHeaderLabels(["标签", "类型", "价格", "触发", "操作", "状态"])
        self._line_table.verticalHeader().setVisible(False)
        self._line_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self._line_table.itemSelectionChanged.connect(self._on_line_selected)
        self._line_table.setMinimumHeight(96)
        self._line_table.setMaximumHeight(112)
        drawing_layout.addWidget(self._line_table)

        rr_layout.addWidget(QLabel("RR 工作区"))
        rr_draw_row = QHBoxLayout()
        for text, tool in (("RR多", "rr_long"), ("RR空", "rr_short"), ("光标", "none")):
            button = QPushButton(text)
            button.clicked.connect(lambda _checked=False, tool=tool: self._set_draw_tool(tool))
            rr_draw_row.addWidget(button)
        rr_layout.addLayout(rr_draw_row)
        rr_workflow_hint = _WrappedStatusLabel("先选 RR多/空，在图上确定入场和止损；选中 RR 后点“交易参数”，保存后再启用交易。")
        rr_workflow_hint.setObjectName("Subtle")
        rr_layout.addWidget(rr_workflow_hint)
        rr_manage_row = QVBoxLayout()
        rr_manage_top = QHBoxLayout()
        save_rr_btn = QPushButton("保存 RR")
        save_rr_btn.clicked.connect(self._save_rr_item)
        rr_manage_top.addWidget(save_rr_btn)
        edit_rr_btn = QPushButton("交易参数")
        edit_rr_btn.clicked.connect(self._open_rr_card_for_selected)
        rr_manage_top.addWidget(edit_rr_btn)
        rr_manage_row.addLayout(rr_manage_top)
        rr_manage_bottom = QHBoxLayout()
        remove_rr_btn = QPushButton("删除 RR 方案")
        remove_rr_btn.setToolTip(
            "删除本地 RR 图形和方案；若入场挂单仍未成交，会先撤单。"
            "已成交仓位不会平仓，已有止盈止损保护会保留。"
        )
        remove_rr_btn.clicked.connect(self._remove_rr_item)
        rr_manage_bottom.addWidget(remove_rr_btn)
        monitor_rr_btn = QPushButton("RR 监控")
        monitor_rr_btn.clicked.connect(self.open_rr_monitor_dialog)
        rr_manage_bottom.addWidget(monitor_rr_btn)
        rr_manage_row.addLayout(rr_manage_bottom)
        rr_layout.addLayout(rr_manage_row)

        rr_risk_row = QHBoxLayout()
        rr_risk_row.addWidget(QLabel("风险金(U)"))
        self._rr_risk_edit = QLineEdit("100")
        self._rr_risk_edit.setPlaceholderText("默认 100")
        self._rr_risk_edit.setToolTip(
            "每笔交易最多按此金额承担止损风险。实际下单风险会受合约最小张数取整影响。"
        )
        rr_risk_row.addWidget(self._rr_risk_edit, 1)
        rr_layout.addLayout(rr_risk_row)
        rr_risk_note = _WrappedStatusLabel("默认 100 U；保存 RR 后生效。")
        rr_risk_note.setObjectName("Subtle")
        rr_layout.addWidget(rr_risk_note)

        self._rr_table = QTableWidget(0, 9)
        self._rr_table.setHorizontalHeaderLabels(["方向", "入场", "止损", "止盈", "管理", "R", "K线", "锁定", "风险金"])
        self._rr_table.verticalHeader().setVisible(False)
        self._rr_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self._rr_table.itemSelectionChanged.connect(self._on_rr_selected)
        self._rr_table.cellClicked.connect(self._on_rr_table_cell_clicked)
        self._rr_table.cellDoubleClicked.connect(self._on_rr_table_cell_double_clicked)
        self._rr_table.setWordWrap(False)
        self._rr_table.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        rr_header = self._rr_table.horizontalHeader()
        rr_header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        rr_header.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        rr_header.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        rr_header.setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
        rr_header.setSectionResizeMode(4, QHeaderView.ResizeMode.ResizeToContents)
        rr_header.setSectionResizeMode(5, QHeaderView.ResizeMode.ResizeToContents)
        rr_header.setSectionResizeMode(6, QHeaderView.ResizeMode.ResizeToContents)
        rr_header.setSectionResizeMode(7, QHeaderView.ResizeMode.ResizeToContents)
        rr_header.setSectionResizeMode(8, QHeaderView.ResizeMode.ResizeToContents)
        rr_header.setStretchLastSection(False)
        self._rr_table.setMinimumHeight(96)
        self._rr_table.setMaximumHeight(116)
        rr_layout.addWidget(self._rr_table)

        rr_form = QWidget()
        self._rr_form = rr_form
        rr_form_layout = QFormLayout(rr_form)
        rr_form_layout.setContentsMargins(0, 0, 0, 0)
        rr_form_layout.setSpacing(6)
        rr_form_layout.setLabelAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        rr_form_layout.setFormAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop)
        rr_form_layout.setFieldGrowthPolicy(QFormLayout.FieldGrowthPolicy.AllNonFixedFieldsGrow)
        rr_form_layout.setRowWrapPolicy(QFormLayout.RowWrapPolicy.DontWrapRows)
        self._rr_side_combo = QComboBox()
        self._rr_side_combo.addItem("多头", "long")
        self._rr_side_combo.addItem("空头", "short")
        self._rr_management_mode_combo = QComboBox()
        self._rr_management_mode_combo.addItem("固定止盈", "fixed_tp")
        self._rr_management_mode_combo.addItem("1:1 到保本", "trail_after_1r")
        self._rr_management_mode_combo.addItem("1:2 到保本", "trail_after_2r")
        self._rr_management_mode_combo.addItem("1:3 到保本", "trail_after_3r")
        self._rr_entry_edit = QLineEdit()
        self._rr_stop_edit = QLineEdit()
        self._rr_r_edit = RMultipleSpinBox()
        self._rr_bar_edit = QLineEdit("0")
        self._rr_fee_offset_check = QCheckBox("2倍手续费偏移")
        self._rr_locked_check = QCheckBox("锁定")
        self._rr_execution_mode_combo = QComboBox()
        self._rr_execution_mode_combo.addItem("限价", "limit")
        self._rr_execution_mode_combo.addItem("市价", "market")
        self._rr_execution_mode_combo.addItem("最优盘口限价（单次）", "chase_best_quote")
        self._rr_execution_mode_combo.setToolTip("提交时读取一次最优盘口并按限价提交，不会持续追价。")
        self._rr_management_mode_combo.setToolTip("监控观察到所选 R 档后先移至保本；之后每跨整数 R 锁入前一整数 R，RR 目标止盈保留。")
        self._rr_preview = QLabel("止盈会按入场、止损和 R 倍数自动计算。")
        self._rr_preview.setObjectName("Subtle")
        self._rr_preview.setWordWrap(True)
        rr_form_layout.addRow("方向", self._rr_side_combo)
        rr_form_layout.addRow("管理方式", self._rr_management_mode_combo)
        rr_form_layout.addRow("入场价", self._rr_entry_edit)
        rr_form_layout.addRow("止损价", self._rr_stop_edit)
        rr_form_layout.addRow("R 倍数", self._rr_r_edit)
        rr_form_layout.addRow("K线序号", self._rr_bar_edit)
        rr_form_layout.addRow("入场方式", self._rr_execution_mode_combo)
        rr_form_layout.addRow(self._rr_fee_offset_check)
        rr_form_layout.addRow(self._rr_locked_check)
        rr_form_layout.addRow(self._rr_preview)
        rr_layout.addWidget(rr_form)
        rr_form.hide()

        rr_layout.addWidget(QLabel("RR 跟踪"))
        self._rr_tracking_summary = _WrappedStatusLabel("选中 RR 后显示入场、止损、止盈和跟踪状态。")
        self._rr_tracking_summary.setObjectName("Subtle")
        rr_layout.addWidget(self._rr_tracking_summary)
        rr_execution_row = QHBoxLayout()
        self._rr_enable_trade_btn = QPushButton("启用交易")
        self._rr_enable_trade_btn.setObjectName("Primary")
        self._rr_enable_trade_btn.clicked.connect(self._enable_selected_rr_trade)
        rr_execution_row.addWidget(self._rr_enable_trade_btn)
        self._rr_cancel_trade_btn = QPushButton("仅撤销入场挂单")
        self._rr_cancel_trade_btn.setToolTip(
            "只撤销此 RR 未成交的入场部分，RR 图形和方案保留；"
            "不会平仓，已成交仓位和止盈止损保护保留。"
        )
        self._rr_cancel_trade_btn.clicked.connect(self._cancel_selected_rr_trade)
        rr_execution_row.addWidget(self._rr_cancel_trade_btn)
        rr_layout.addLayout(rr_execution_row)
        self._rr_action_status = _WrappedStatusLabel("")
        self._rr_action_status.setObjectName("Subtle")
        self._rr_action_status.hide()
        rr_layout.addWidget(self._rr_action_status)
        self._rr_condition_status = _WrappedStatusLabel("条件单：未启用交易")
        self._rr_condition_status.setObjectName("Subtle")
        rr_layout.addWidget(self._rr_condition_status)

        rr_layout.addWidget(QLabel("事件日志"))
        self._event_log = QTextEdit()
        self._event_log.setReadOnly(True)
        self._event_log.setMinimumHeight(72)
        self._event_log.setMaximumHeight(84)
        rr_layout.addWidget(self._event_log)

        analysis_layout.addStretch(1)
        drawing_layout.addStretch(1)
        rr_layout.addStretch(1)

        chart_host = QFrame()
        self._chart_host = chart_host
        chart_host.setObjectName("Panel")
        chart_layout = QVBoxLayout(chart_host)
        self._chart_host_layout = chart_layout
        chart_layout.setContentsMargins(8, 8, 8, 8)
        chart_layout.setSpacing(0)

        chart_frame = QFrame()
        self._chart_frame = chart_frame
        chart_frame_layout = QVBoxLayout(chart_frame)
        chart_frame_layout.setContentsMargins(0, 0, 0, 0)
        chart_frame_layout.setSpacing(0)

        if self._use_native_chart:
            self._create_native_chart(chart_frame_layout)
        elif QWebEngineView is None:
            fallback = QLabel("当前环境未检测到QWebEngine")
            fallback.setObjectName("Subtle")
            fallback.setAlignment(Qt.AlignmentFlag.AlignCenter)
            chart_frame_layout.addWidget(fallback, 1)
        else:
            self._web = QWebEngineView()
            self._web.setHtml(self._chart_html())
            self._web.loadFinished.connect(self._on_chart_ready)
            chart_frame_layout.addWidget(self._web, 1)

        self._chart_account_splitter = QSplitter(Qt.Orientation.Vertical)
        self._chart_account_splitter.setChildrenCollapsible(False)
        self._chart_account_splitter.addWidget(chart_frame)
        self._account_drawer = KlineAccountDrawer()
        self._account_drawer.collapseRequested.connect(self._collapse_account_drawer)
        self._chart_account_splitter.addWidget(self._account_drawer)
        self._account_drawer.hide()
        self._chart_account_splitter.setStretchFactor(0, 5)
        self._chart_account_splitter.setStretchFactor(1, 2)
        chart_layout.addWidget(self._chart_account_splitter, 1)

        symbol_tab_row = QHBoxLayout()
        symbol_tab_row.setContentsMargins(0, 4, 0, 0)
        symbol_tab_row.setSpacing(8)
        symbol_tab_row.addWidget(QLabel("交易对"))
        self._symbol_tab_bar = QTabBar()
        self._symbol_tab_bar.setShape(QTabBar.Shape.RoundedSouth)
        self._symbol_tab_bar.setExpanding(False)
        self._symbol_tab_bar.setUsesScrollButtons(True)
        self._symbol_tab_bar.setDrawBase(False)
        self._symbol_tab_bar.setToolTip("点击切换交易对；BTC-DVOL/ETH-DVOL 标签切换到波动率副图")
        for symbol in KLINE_SYMBOL_OPTIONS:
            tab_index = self._symbol_tab_bar.addTab(symbol)
            self._symbol_tab_bar.setTabData(tab_index, symbol)
        for label, tab_value in VOLATILITY_TAB_OPTIONS:
            tab_index = self._symbol_tab_bar.addTab(label)
            self._symbol_tab_bar.setTabData(tab_index, tab_value)
        self._symbol_tab_bar.currentChanged.connect(self._on_symbol_tab_changed)
        symbol_tab_row.addWidget(self._symbol_tab_bar, 1)
        self._symbol_link_all_check = QCheckBox("全部K线窗口联动")
        self._symbol_link_all_check.setToolTip("开启后，点击底部交易对标签会同时切换主图、副图、第三图和第四图")
        symbol_tab_row.addWidget(self._symbol_link_all_check)
        chart_layout.addLayout(symbol_tab_row)

        splitter.addWidget(control_scroll)
        splitter.addWidget(chart_host)
        splitter.setStretchFactor(0, 1)
        splitter.setStretchFactor(1, 5)
        parent_layout.addWidget(splitter, 1)

        self._refresh_timer = QTimer(self)
        self._refresh_timer.timeout.connect(self._load_data)

    def _on_control_tab_changed(self, index: int) -> None:
        panel = getattr(self, "_control_panel", None)
        tabs = getattr(self, "_control_tabs", None)
        snapshot = getattr(self, "_control_width_policy_snapshot", None)
        if panel is None or tabs is None or snapshot is None:
            return
        rr_tab_index = int(getattr(self, "_control_rr_tab_index", -1))
        area_tab_index = int(getattr(self, "_control_area_tab_index", -1))
        if int(index) in {rr_tab_index, area_tab_index}:
            mode = "area" if int(index) == area_tab_index else "direct"
            combo = getattr(self, "_rr_workspace_mode_combo", None)
            if combo is not None and str(combo.currentData() or "direct") != mode:
                target = combo.findData(mode)
                if target >= 0:
                    combo.blockSignals(True)
                    combo.setCurrentIndex(target)
                    combo.blockSignals(False)
            self._refresh_rr_workspace_other_status(mode)
            panel_policy = panel.sizePolicy()
            panel_policy.setHorizontalPolicy(QSizePolicy.Policy.Ignored)
            panel.setSizePolicy(panel_policy)
            panel.setMinimumWidth(0)
            tabs_policy = tabs.sizePolicy()
            tabs_policy.setHorizontalPolicy(QSizePolicy.Policy.Ignored)
            tabs.setSizePolicy(tabs_policy)
            tabs.setMinimumWidth(0)
            return
        panel_policy, panel_minimum, tabs_policy, tabs_minimum = snapshot
        panel.setSizePolicy(panel_policy)
        panel.setMinimumWidth(panel_minimum)
        tabs.setSizePolicy(tabs_policy)
        tabs.setMinimumWidth(tabs_minimum)

    def _select_rr_workspace_mode(self, mode: str) -> None:
        combo = getattr(self, "_rr_workspace_mode_combo", None)
        if combo is None:
            return
        target = combo.findData(str(mode))
        if target >= 0:
            if combo.currentIndex() == target:
                self._on_rr_workspace_mode_changed(target)
            else:
                combo.setCurrentIndex(target)

    @Slot(int)
    def _on_rr_workspace_mode_changed(self, index: int) -> None:
        combo = getattr(self, "_rr_workspace_mode_combo", None)
        tabs = getattr(self, "_control_tabs", None)
        if tabs is None or combo is None:
            return
        mode = str(combo.itemData(index) or "direct")
        target_tab = int(getattr(self, "_control_area_tab_index", -1)) if mode == "area" else int(getattr(self, "_control_rr_tab_index", -1))
        if target_tab >= 0 and tabs.currentIndex() != target_tab:
            tabs.setCurrentIndex(target_tab)
        self._refresh_rr_workspace_other_status(mode)

    def _refresh_rr_workspace_other_status(self, mode: str | None = None) -> None:
        direct_label = getattr(self, "_rr_workspace_other_status", None)
        area_label = getattr(self, "_area_workspace_other_status", None)
        if direct_label is None or area_label is None:
            return
        selected_mode = mode or str(self._rr_workspace_mode_combo.currentData() or "direct")
        label = area_label if selected_mode == "area" else direct_label
        other_label = direct_label if selected_mode == "area" else area_label
        other_label.hide()
        message = ""
        if selected_mode == "direct":
            plan = self._current_area_plan()
            plan_status = str(getattr(plan, "status", "") or "") if plan is not None else ""
            if plan_status and plan_status not in {"draft", "closed", "cancelled", "expired", "paper_closed"}:
                message = f"区域形态计划：{plan.status}（切换只改变显示）。"
            terminal_session_statuses = {"closed", "cancelled", "expired"}
            sessions = self._area_shape_demo_sessions()
            active_sessions = [
                status
                for item in sessions
                if (status := str(item.get("status", "") or "")) and status not in terminal_session_statuses
            ]
            if active_sessions:
                session_text = ", ".join(active_sessions[-2:])
                message = f"{message} {('；' if message else '')}智能辅助交易会话：{session_text}（切换只改变显示）。"
        else:
            terminal_rr_statuses = {"closed", "cancelled", "expired"}
            active_entries = [
                entry
                for entry in self._matching_rr_trade_ledger_entries()
                if str(getattr(entry, "status", "") or "")
                and str(getattr(entry, "status", "") or "") not in terminal_rr_statuses
            ]
            if active_entries:
                message = f"RR 交易状态：{active_entries[-1].status}（切换只改变显示）。"
        label.setText(message)
        label.setVisible(bool(message))

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
            "1W": 30 * 60_000,
        }
        return mapping.get(period, 60_000)

    def _build_refresh_timer(self) -> None:
        self._refresh_timer.setInterval(self._auto_refresh_interval_ms(self._period_combo.currentText()))

    def _schedule_chart_layout_refresh(self, delay_ms: int = 80) -> None:
        timer = getattr(self, "_layout_refresh_timer", None)
        if not isinstance(timer, QTimer):
            return
        timer.start(max(0, int(delay_ms)))

    @Slot()
    def _refresh_chart_layout_after_window_change(self) -> None:
        self._apply_default_splitter_sizes()
        self._apply_secondary_chart_layout()
        if self._pending_payload is not None and self._page_ready:
            self._render_to_chart(self._pending_payload)
        if (
            self._secondary_chart_check.isChecked()
            and self._secondary_pending_payload is not None
            and self._page_ready
        ):
            self._render_secondary_chart(self._secondary_pending_payload)
        if self._triple_chart_enabled() and self._tertiary_pending_payload is not None and self._page_ready:
            self._render_tertiary_chart(self._tertiary_pending_payload)
        if self._quad_chart_enabled() and self._quaternary_pending_payload is not None and self._page_ready:
            self._render_quaternary_chart(self._quaternary_pending_payload)
        if isinstance(self._native_chart_view, InteractiveKlineChartView):
            self._native_chart_view.update()
        if isinstance(self._secondary_native_chart_view, InteractiveKlineChartView):
            self._secondary_native_chart_view.update()
        if isinstance(self._tertiary_native_chart_view, InteractiveKlineChartView):
            self._tertiary_native_chart_view.update()
        if isinstance(self._quaternary_native_chart_view, InteractiveKlineChartView):
            self._quaternary_native_chart_view.update()
        self._keep_window_within_current_screen()

    def _keep_window_within_current_screen(self) -> None:
        """Prevent a layout recalculation from pushing a normal window off-screen."""
        if self._clamping_window_to_screen:
            return
        window_target = self.window()
        if (
            window_target is None
            or window_target.isMaximized()
            or window_target.isFullScreen()
            or window_target.isMinimized()
        ):
            return
        screen = QGuiApplication.screenAt(window_target.frameGeometry().center())
        if screen is None:
            screen = window_target.screen() or QGuiApplication.primaryScreen()
        if screen is None:
            return
        available = screen.availableGeometry()
        frame = window_target.frameGeometry()
        frame_extra_width = max(0, frame.width() - window_target.width())
        frame_extra_height = max(0, frame.height() - window_target.height())
        target_width = min(window_target.width(), max(320, available.width() - frame_extra_width))
        target_height = min(window_target.height(), max(240, available.height() - frame_extra_height))

        self._clamping_window_to_screen = True
        try:
            if target_width != window_target.width() or target_height != window_target.height():
                window_target.resize(target_width, target_height)
            frame = window_target.frameGeometry()
            target_x = min(max(frame.x(), available.left()), available.right() - frame.width() + 1)
            target_y = min(max(frame.y(), available.top()), available.bottom() - frame.height() + 1)
            if target_x != frame.x() or target_y != frame.y():
                window_target.move(target_x, target_y)
        finally:
            self._clamping_window_to_screen = False

    def _apply_default_splitter_sizes(self) -> None:
        splitter = self._body_splitter
        if splitter is None or self._splitter_default_applied:
            return
        available_width = splitter.width() or self.width() or 1680
        if self._left_panel_hidden:
            control = self._control_scroll or self._control_panel
            if control is not None:
                control.setVisible(False)
            splitter.setSizes([0, available_width])
            self._splitter_default_applied = True
            return
        left_width, right_width = _default_kline_splitter_sizes(available_width)
        splitter.setSizes([left_width, right_width])
        self._splitter_default_applied = True

    def _secondary_chart_kind(self) -> str:
        value = str(self._secondary_chart_kind_mode or "kline").strip().lower()
        return value if value in {"kline", "volatility"} else "kline"

    def _triple_chart_enabled(self) -> bool:
        return bool(self._tertiary_chart_check.isChecked())

    def _quad_chart_enabled(self) -> bool:
        return bool(self._quaternary_chart_check.isChecked())

    def _all_charts_volatility_enabled(self) -> bool:
        """Whether an explicit all-window link may switch every chart to DVOL."""
        return bool(
            self._secondary_chart_kind() == "volatility"
            and self._current_volatility_currency()
            and self._symbol_link_all_check is not None
            and self._symbol_link_all_check.isChecked()
        )

    def _secondary_layout_mode(self) -> str:
        value = str(self._secondary_layout_mode_value or "horizontal").strip().lower()
        return value if value in {"vertical", "horizontal"} else "horizontal"

    def _refresh_secondary_layout_button(self) -> None:
        if self._secondary_layout_cycle_btn is None:
            return
        self._secondary_layout_cycle_btn.setText(
            _next_secondary_layout_button_text(self._secondary_layout_mode())
        )

    def _refresh_secondary_chart_kind_button(self) -> None:
        if self._secondary_chart_kind_btn is None:
            return
        currency = self._current_volatility_currency()
        if currency is None:
            self._secondary_chart_kind_btn.setText("暂无波动率")
            return
        self._secondary_chart_kind_btn.setText(
            _next_secondary_chart_kind_button_text(self._secondary_chart_kind(), currency)
        )

    def _refresh_secondary_sync_period_button(self) -> None:
        if self._secondary_sync_period_btn is None:
            return
        if self._secondary_chart_kind() == "volatility":
            self._secondary_sync_period_btn.setText("同周期切换")
            self._secondary_sync_period_btn.setToolTip("副图为波动率时：主图和副图同步切换周期（1H/4H/1D/1W）")
            return
        self._secondary_sync_period_btn.setText("1D+4H")
        self._secondary_sync_period_btn.setToolTip("副图为K线时：主图设为1D、副图设为4H，并切换到最近视图")

    def _shape_signal_size_metric_value(self) -> str:
        value = str(self._shape_signal_size_metric or "body").strip().lower()
        return value if value in {"body", "range"} else "body"

    def _refresh_shape_signal_size_metric_button(self) -> None:
        button = getattr(self, "_shape_signal_size_metric_btn", None)
        if button is None:
            return
        metric = self._shape_signal_size_metric_value()
        button.setText("实体" if metric == "body" else "振幅")
        button.setToolTip("切换核心K排名口径：实体=abs(close-open)，振幅=high-low")

    @Slot()
    def _toggle_shape_signal_size_metric(self) -> None:
        self._shape_signal_size_metric = "range" if self._shape_signal_size_metric_value() == "body" else "body"
        self._refresh_shape_signal_size_metric_button()
        self._sync_chart_options()

    def _secondary_display_symbol(self) -> str:
        if self._secondary_chart_kind() == "volatility":
            return f"{self._current_volatility_currency() or '-'} DVOL"
        return self._selected_secondary_symbol()

    def _selected_symbol(self) -> str:
        return self._symbol_combo.currentText().strip().upper()

    def _selected_secondary_symbol(self) -> str:
        return self._secondary_symbol_combo.currentText().strip().upper()

    def _selected_tertiary_symbol(self) -> str:
        return self._tertiary_symbol_combo.currentText().strip().upper()

    def _selected_quaternary_symbol(self) -> str:
        return self._quaternary_symbol_combo.currentText().strip().upper()

    def _current_volatility_currency(self) -> str | None:
        return self._active_volatility_currency or _volatility_currency_for_symbol(self._selected_symbol())

    def _volatility_available_for_current_symbol(self) -> bool:
        return _volatility_currency_for_symbol(self._selected_symbol()) is not None

    @staticmethod
    def _kline_venue_label(period: str) -> str:
        normalized = str(period or "").strip().upper()
        if normalized == "1D":
            return "OKX UTC+8"
        if normalized == "1DUTC":
            return "OKX UTC"
        return "OKX"

    def _secondary_chart_venue_label(self) -> str:
        if self._secondary_chart_kind() == "volatility":
            return "DERIBIT"
        return self._kline_venue_label(self._secondary_period_combo.currentText())

    def _secondary_chart_note_lines(self, payload: KlineChartPayload, *, period: str) -> list[str]:
        if self._secondary_chart_kind() != "volatility":
            return []
        normalized_period = period.strip().upper()
        if normalized_period == "1H":
            aggregation_text = "1H直连"
        elif normalized_period == "1D":
            aggregation_text = "1D本地聚合（UTC+8日界）"
        elif normalized_period == "1W":
            aggregation_text = "1W本地聚合（UTC+8周一开周）"
        else:
            aggregation_text = f"{normalized_period}本地聚合"
        average_text = "开" if self._secondary_average_kline_enabled() else "关"
        currency = self._current_volatility_currency() or "-"
        source_text = str(payload.stats.get("source", f"Deribit {currency} DVOL") or f"Deribit {currency} DVOL")
        source_text = source_text.replace("（本地聚合）", "")
        return [f"{currency}波动率 | 平均K线 {average_text} | {aggregation_text} | 来源 {source_text}"]

    def _chart_period_for_target(self, target: str) -> str:
        controls = {
            "primary": self._period_combo,
            "secondary": self._secondary_period_combo,
            "tertiary": self._tertiary_period_combo,
            "quaternary": self._quaternary_period_combo,
        }
        control = controls.get(target)
        return control.currentText().strip().upper() if control is not None else "1H"

    def _linked_range_for_target(
        self,
        *,
        target: str,
        start_x: float,
        end_x: float,
        source: str | None,
    ) -> tuple[float, float]:
        if not source or source == target:
            return float(start_x), float(end_x)
        source_view = {
            "primary": self._native_chart_view,
            "secondary": self._secondary_native_chart_view,
            "tertiary": self._tertiary_native_chart_view,
            "quaternary": self._quaternary_native_chart_view,
        }.get(source)
        target_view = {
            "primary": self._native_chart_view,
            "secondary": self._secondary_native_chart_view,
            "tertiary": self._tertiary_native_chart_view,
            "quaternary": self._quaternary_native_chart_view,
        }.get(target)
        source_step = max(1.0, float(getattr(source_view, "_display_step_ms", 0.0) or 0.0))
        target_step = max(1.0, float(getattr(target_view, "_display_step_ms", 0.0) or 0.0))
        source_visible_bars = max(1, int(round(max(1.0, float(end_x) - float(start_x)) / source_step)))
        target_period = self._chart_period_for_target(target)
        target_visible_bars = max(source_visible_bars, _LINKED_MIN_VISIBLE_BARS.get(target_period, _RECENT_VIEW_BARS))
        target_span = float(target_visible_bars) * target_step
        return float(end_x) - target_span, float(end_x)

    def _sync_chart_range_to_other(
        self,
        *,
        target: str,
        start_x: float,
        end_x: float,
        source: str | None = None,
    ) -> None:
        if self._syncing_chart_range or not self._secondary_chart_check.isChecked() or not self._use_native_chart:
            return
        target_views = {
            "primary": self._native_chart_view,
            "secondary": self._secondary_native_chart_view,
            "tertiary": self._tertiary_native_chart_view,
            "quaternary": self._quaternary_native_chart_view,
        }
        target_view = target_views.get(target)
        if not isinstance(target_view, InteractiveKlineChartView):
            return
        self._syncing_chart_range = True
        try:
            linked_start, linked_end = self._linked_range_for_target(
                target=target,
                start_x=start_x,
                end_x=end_x,
                source=source if (self._triple_chart_enabled() or self._quad_chart_enabled()) else None,
            )
            target_view.set_external_x_range(linked_start, linked_end)
        finally:
            self._syncing_chart_range = False

    def _sync_chart_range_from(self, *, source: str, start_x: float, end_x: float) -> None:
        targets = ["primary", "secondary"]
        if self._triple_chart_enabled():
            targets.append("tertiary")
        if self._quad_chart_enabled():
            targets.append("quaternary")
        for target in targets:
            if target != source:
                self._sync_chart_range_to_other(
                    target=target,
                    start_x=start_x,
                    end_x=end_x,
                    source=source,
                )

    def _sync_hover_time_from(self, *, source: str, candle_time: object) -> None:
        targets: dict[str, object] = {
            "primary": self._native_chart_view,
            "secondary": self._secondary_native_chart_view,
        }
        if self._triple_chart_enabled():
            targets["tertiary"] = self._tertiary_native_chart_view
        if self._quad_chart_enabled():
            targets["quaternary"] = self._quaternary_native_chart_view
        value = None if candle_time is None else int(candle_time)
        source_view = targets.get(source)
        price = (
            source_view.hover_price_for_link()
            if value is not None and isinstance(source_view, InteractiveKlineChartView)
            else None
        )
        for target, view in targets.items():
            if not isinstance(view, InteractiveKlineChartView):
                continue
            if target == source:
                view.set_external_hover_time(None)
            else:
                same_symbol = (
                    isinstance(source_view, InteractiveKlineChartView)
                    and bool(source_view._symbol)
                    and source_view._symbol.upper() == view._symbol.upper()
                )
                view.set_external_hover_time(value, price if same_symbol else None)

    def _sync_secondary_chart_range_from_primary(self) -> None:
        if not self._secondary_chart_check.isChecked():
            return
        if not isinstance(self._native_chart_view, InteractiveKlineChartView):
            return
        if self._triple_chart_enabled() or self._quad_chart_enabled():
            self._syncing_chart_range = True
            try:
                set_range = (
                    InteractiveKlineChartView.set_full_view_range
                    if self._chart_view_range_is_full()
                    else InteractiveKlineChartView.set_recent_view_range
                )
                for view in (
                    self._native_chart_view,
                    self._secondary_native_chart_view,
                    self._tertiary_native_chart_view if self._triple_chart_enabled() else None,
                    self._quaternary_native_chart_view if self._quad_chart_enabled() else None,
                ):
                    if isinstance(view, InteractiveKlineChartView):
                        set_range(view)
            finally:
                self._syncing_chart_range = False
            return
        start_x, end_x = self._native_chart_view.current_x_range()
        self._sync_chart_range_from(source="primary", start_x=start_x, end_x=end_x)

    def _apply_recent_view_range_for_linked_charts(self) -> bool:
        if (
            not self._secondary_chart_check.isChecked()
            or not isinstance(self._native_chart_view, InteractiveKlineChartView)
            or not isinstance(self._secondary_native_chart_view, InteractiveKlineChartView)
        ):
            return False
        if self._triple_chart_enabled():
            self._syncing_chart_range = True
            try:
                for view in (
                    self._native_chart_view,
                    self._secondary_native_chart_view,
                    self._tertiary_native_chart_view,
                    self._quaternary_native_chart_view if self._quad_chart_enabled() else None,
                ):
                    if isinstance(view, InteractiveKlineChartView):
                        view.set_recent_view_range()
            finally:
                self._syncing_chart_range = False
            return True
        primary_period_ms = _bar_to_ms(self._period_combo.currentText().strip())
        secondary_period_ms = _bar_to_ms(self._secondary_period_combo.currentText().strip())
        if secondary_period_ms < primary_period_ms:
            self._secondary_native_chart_view.set_recent_view_range()
            start_x, end_x = self._secondary_native_chart_view.current_x_range()
            self._sync_chart_range_to_other(target="primary", start_x=start_x, end_x=end_x)
            return True
        self._native_chart_view.set_recent_view_range()
        start_x, end_x = self._native_chart_view.current_x_range()
        self._sync_chart_range_to_other(target="secondary", start_x=start_x, end_x=end_x)
        return True

    def _toggle_left_panel(self, hidden: bool) -> None:
        self._left_panel_hidden = bool(hidden)
        if self._toggle_left_panel_btn is not None:
            self._toggle_left_panel_btn.setText("显示左栏" if hidden else "隐藏左栏")
        splitter = self._body_splitter
        control = self._control_scroll or self._control_panel
        if splitter is None or control is None:
            return
        control.setVisible(not hidden)
        available_width = splitter.width() or self.width() or 1680
        if hidden:
            splitter.setSizes([0, available_width])
            return
        left_width, right_width = _default_kline_splitter_sizes(available_width)
        splitter.setSizes([left_width, right_width])

    @Slot(bool)
    def _toggle_chart_visibility(self, hidden: bool) -> None:
        chart_frame = self._chart_frame
        if chart_frame is not None:
            chart_frame.setVisible(not hidden)
        splitter = self._chart_account_splitter
        drawer = self._account_drawer
        if hidden and splitter is not None and drawer is not None:
            drawer.show()
            self._sync_account_drawer_context(refresh_if_visible=False)
            drawer.refresh_data()
            splitter.setSizes([0, max(splitter.size().height(), 1)])
        elif not hidden and splitter is not None and drawer is not None and not drawer.isHidden():
            total_height = max(splitter.size().height(), 1)
            drawer_height = max(int(total_height * 0.28), 180)
            splitter.setSizes([max(total_height - drawer_height, 240), drawer_height])
        self._hide_chart_btn.setText("显示图表" if hidden else "隐藏图表")

    @Slot()
    def _toggle_chart_fullscreen(self) -> None:
        self._set_chart_fullscreen(not self._chart_fullscreen_enabled)

    @Slot()
    def _copy_chart_screenshot_to_clipboard(self) -> None:
        chart_host = self._chart_host
        if chart_host is None or not chart_host.isVisible():
            self._set_status("当前没有可截图的图表区域。")
            return
        screenshot = chart_host.grab()
        if screenshot.isNull():
            self._set_status("图表截图失败，请稍后重试。")
            return
        clipboard = QGuiApplication.clipboard()
        if clipboard is None:
            self._set_status("系统剪贴板不可用，无法复制截图。")
            return
        clipboard.setPixmap(screenshot)
        self._set_status("当前 K 线图表已复制到剪贴板。")

    def _set_chart_fullscreen(self, enabled: bool) -> None:
        enabled = bool(enabled)
        if enabled == self._chart_fullscreen_enabled:
            return

        if enabled:
            self._enter_chart_fullscreen()
        else:
            self._exit_chart_fullscreen()

    def _enter_chart_fullscreen(self) -> None:
        header = self._header_panel
        control = self._control_scroll or self._control_panel
        drawer = self._account_drawer
        body_splitter = self._body_splitter
        account_splitter = self._chart_account_splitter
        chart_layout = self._chart_host_layout

        self._chart_fullscreen_restore_header_visible = bool(header is not None and not header.isHidden())
        self._chart_fullscreen_restore_control_visible = bool(control is not None and not control.isHidden())
        self._chart_fullscreen_restore_drawer_visible = bool(drawer is not None and not drawer.isHidden())
        self._chart_fullscreen_restore_body_sizes = list(body_splitter.sizes()) if body_splitter is not None else []
        self._chart_fullscreen_restore_account_sizes = (
            list(account_splitter.sizes()) if account_splitter is not None else []
        )
        if chart_layout is not None:
            margins = chart_layout.contentsMargins()
            self._chart_fullscreen_restore_main_margins = (
                margins.left(),
                margins.top(),
                margins.right(),
                margins.bottom(),
            )
            self._chart_fullscreen_restore_main_spacing = chart_layout.spacing()
            chart_layout.setContentsMargins(0, 0, 0, 0)
            chart_layout.setSpacing(0)

        main_layout = getattr(self, "_main_layout", None)
        if isinstance(main_layout, QVBoxLayout):
            margins = main_layout.contentsMargins()
            self._chart_fullscreen_restore_outer_margins = (
                margins.left(),
                margins.top(),
                margins.right(),
                margins.bottom(),
            )
            self._chart_fullscreen_restore_outer_spacing = main_layout.spacing()
            main_layout.setContentsMargins(0, 0, 0, 0)
            main_layout.setSpacing(0)

        window_target = self.window()
        self._chart_fullscreen_window_target = window_target
        self._chart_fullscreen_restore_window_maximized = bool(window_target.isMaximized())
        self._chart_fullscreen_owns_window_state = not (
            window_target.isMaximized() or window_target.isFullScreen()
        )

        workspace_header = getattr(window_target, "_workspace_header", None)
        if isinstance(workspace_header, QWidget):
            self._chart_fullscreen_restore_workspace_header = workspace_header
            self._chart_fullscreen_restore_workspace_header_visible = not workspace_header.isHidden()
            workspace_header.hide()
        else:
            self._chart_fullscreen_restore_workspace_header = None
            self._chart_fullscreen_restore_workspace_header_visible = False

        if header is not None:
            header.hide()
        if control is not None:
            control.hide()
        if drawer is not None:
            drawer.hide()
        if body_splitter is not None:
            body_splitter.setSizes([0, max(body_splitter.width(), self.width(), 1)])
        if account_splitter is not None:
            account_splitter.setSizes([max(account_splitter.height(), self.height(), 1), 0])

        self._chart_fullscreen_enabled = True
        if self._chart_fullscreen_button is not None:
            self._chart_fullscreen_button.setText("退出全屏")
        if self._chart_fullscreen_owns_window_state:
            # 保留 Windows 标题栏，方便最小化、最大化和关闭。
            window_target.showMaximized()
        self._schedule_chart_layout_refresh(0)

    def _exit_chart_fullscreen(self) -> None:
        header = self._header_panel
        control = self._control_scroll or self._control_panel
        drawer = self._account_drawer
        body_splitter = self._body_splitter
        account_splitter = self._chart_account_splitter
        chart_layout = self._chart_host_layout

        if header is not None:
            header.setVisible(self._chart_fullscreen_restore_header_visible)
        if control is not None:
            control.setVisible(self._chart_fullscreen_restore_control_visible)
        if drawer is not None:
            drawer.setVisible(self._chart_fullscreen_restore_drawer_visible)
        if body_splitter is not None and self._chart_fullscreen_restore_body_sizes:
            body_splitter.setSizes(self._chart_fullscreen_restore_body_sizes)
        if account_splitter is not None and self._chart_fullscreen_restore_account_sizes:
            account_splitter.setSizes(self._chart_fullscreen_restore_account_sizes)
        if chart_layout is not None:
            chart_layout.setContentsMargins(*self._chart_fullscreen_restore_main_margins)
            chart_layout.setSpacing(self._chart_fullscreen_restore_main_spacing)

        main_layout = getattr(self, "_main_layout", None)
        if isinstance(main_layout, QVBoxLayout):
            outer_margins = getattr(self, "_chart_fullscreen_restore_outer_margins", (10, 10, 10, 10))
            main_layout.setContentsMargins(*outer_margins)
            main_layout.setSpacing(getattr(self, "_chart_fullscreen_restore_outer_spacing", 10))

        workspace_header = self._chart_fullscreen_restore_workspace_header
        if workspace_header is not None:
            workspace_header.setVisible(self._chart_fullscreen_restore_workspace_header_visible)
        window_target = self._chart_fullscreen_window_target
        if window_target is not None and self._chart_fullscreen_owns_window_state:
            if self._chart_fullscreen_restore_window_maximized:
                window_target.showMaximized()
            else:
                window_target.showNormal()

        self._chart_fullscreen_enabled = False
        self._chart_fullscreen_owns_window_state = False
        self._chart_fullscreen_window_target = None
        if self._chart_fullscreen_button is not None:
            self._chart_fullscreen_button.setText("全屏图表")
        self._schedule_chart_layout_refresh(0)

    def _update_secondary_controls_state(self) -> None:
        enabled = bool(self._secondary_chart_check.isChecked())
        if self._layout_context_panel is not None:
            self._layout_context_panel.setVisible(
                enabled
                or self._tertiary_chart_check.isChecked()
                or self._quaternary_chart_check.isChecked()
            )
        secondary_symbol_available = enabled and self._secondary_chart_kind() == "kline"
        self._secondary_symbol_label.setVisible(secondary_symbol_available)
        self._secondary_symbol_combo.setVisible(secondary_symbol_available)
        self._secondary_symbol_combo.setEnabled(secondary_symbol_available)
        tertiary_available = self._triple_chart_enabled()
        self._tertiary_symbol_label.setVisible(tertiary_available)
        self._tertiary_symbol_combo.setVisible(tertiary_available)
        self._tertiary_symbol_combo.setEnabled(tertiary_available)
        self._tertiary_period_label.setVisible(tertiary_available)
        self._tertiary_period_combo.setVisible(tertiary_available)
        self._tertiary_period_combo.setEnabled(tertiary_available)
        quaternary_available = self._quad_chart_enabled()
        self._quaternary_symbol_label.setVisible(quaternary_available)
        self._quaternary_symbol_combo.setVisible(quaternary_available)
        self._quaternary_symbol_combo.setEnabled(quaternary_available)
        self._quaternary_period_label.setVisible(quaternary_available)
        self._quaternary_period_combo.setVisible(quaternary_available)
        self._quaternary_period_combo.setEnabled(quaternary_available)
        self._secondary_period_label.setVisible(enabled)
        self._secondary_period_combo.setVisible(enabled)
        self._secondary_period_combo.setEnabled(enabled)
        if self._secondary_layout_cycle_btn is not None:
            self._secondary_layout_cycle_btn.setEnabled(enabled and not tertiary_available and not quaternary_available)
        if self._secondary_chart_kind_btn is not None:
            self._secondary_chart_kind_btn.setEnabled(
                enabled and not tertiary_available and not quaternary_available and self._volatility_available_for_current_symbol()
            )
        if self._secondary_sync_period_btn is not None:
            self._secondary_sync_period_btn.setEnabled(enabled)
        self._secondary_average_kline_check.setEnabled(True)
        self._secondary_average_kline_check.setVisible(True)
        self._sync_primary_average_secondary_normal_control_state()
        self._refresh_secondary_layout_button()
        self._refresh_secondary_chart_kind_button()
        self._refresh_secondary_sync_period_button()

    def _primary_average_secondary_normal_available(self) -> bool:
        return bool(
            self._secondary_chart_check.isChecked()
            and self._secondary_chart_kind() == "kline"
        )

    def _primary_average_secondary_normal_enabled(self) -> bool:
        return bool(
            self._primary_average_secondary_normal_available()
            and self._primary_average_secondary_normal_check.isChecked()
        )

    def _primary_average_kline_enabled(self) -> bool:
        return bool(
            self._secondary_average_kline_check.isChecked()
            or self._primary_average_secondary_normal_enabled()
        )

    def _secondary_average_kline_enabled(self) -> bool:
        return bool(self._secondary_average_kline_check.isChecked())

    def _sync_primary_average_secondary_normal_control_state(self) -> None:
        available = self._primary_average_secondary_normal_available()
        self._primary_average_secondary_normal_check.setEnabled(available)
        if not available and self._primary_average_secondary_normal_check.isChecked():
            self._primary_average_secondary_normal_check.blockSignals(True)
            self._primary_average_secondary_normal_check.setChecked(False)
            self._primary_average_secondary_normal_check.blockSignals(False)

    @Slot(bool)
    def _on_secondary_average_kline_toggled(self, enabled: bool) -> None:
        if enabled and self._primary_average_secondary_normal_check.isChecked():
            self._primary_average_secondary_normal_check.blockSignals(True)
            self._primary_average_secondary_normal_check.setChecked(False)
            self._primary_average_secondary_normal_check.blockSignals(False)
        self._load_data()

    @Slot(bool)
    def _on_primary_average_secondary_normal_toggled(self, enabled: bool) -> None:
        if enabled and self._secondary_average_kline_check.isChecked():
            self._secondary_average_kline_check.blockSignals(True)
            self._secondary_average_kline_check.setChecked(False)
            self._secondary_average_kline_check.blockSignals(False)
        self._load_data()

    def _refresh_chart_mode_cycle_button(self) -> None:
        if self._chart_mode_cycle_btn is None:
            return
        self._chart_mode_cycle_btn.setText("涓婁笅鍒嗗睆" if not self._secondary_chart_check.isChecked() else "缈婚噷K绾?")

    @Slot()
    def _on_chart_mode_cycle_clicked(self) -> None:
        next_enabled = not self._secondary_chart_check.isChecked()
        if next_enabled:
            self._secondary_layout_mode_value = "horizontal"
            self._refresh_secondary_layout_button()
        self._secondary_chart_check.setChecked(next_enabled)

    @Slot()
    def _on_secondary_layout_cycle_clicked(self) -> None:
        if self._triple_chart_enabled():
            return
        self._secondary_layout_mode_value = (
            "horizontal" if self._secondary_layout_mode() == "vertical" else "vertical"
        )
        self._refresh_secondary_layout_button()
        self._sync_primary_average_secondary_normal_control_state()
        self._apply_secondary_chart_layout()

    @Slot()
    def _on_secondary_chart_kind_cycle_clicked(self) -> None:
        if self._triple_chart_enabled():
            return
        previous_kind = self._secondary_chart_kind()
        if previous_kind == "kline" and not self._volatility_available_for_current_symbol():
            self._set_status("波动率视图仅支持 BTC / ETH；请先选择 BTC-USDT-SWAP 或 ETH-USDT-SWAP。")
            return
        self._secondary_chart_kind_mode = ("volatility" if previous_kind == "kline" else "kline")
        if previous_kind == "volatility":
            self._retire_loader("_secondary_volatility_loader")
            if isinstance(self._loader, SecondaryVolatilityDataLoader):
                self._retire_loader("_loader")
        if self._secondary_chart_kind() == "volatility":
            self._active_volatility_currency = self._current_volatility_currency()
        else:
            self._active_volatility_currency = None
        self._refresh_secondary_chart_kind_button()
        self._refresh_secondary_sync_period_button()
        self._update_secondary_controls_state()
        self._sync_primary_average_secondary_normal_control_state()
        if previous_kind == "kline" and self._secondary_chart_kind() == "volatility":
            self._period_combo.blockSignals(True)
            self._secondary_period_combo.blockSignals(True)
            self._period_combo.setCurrentText("1H")
            self._secondary_period_combo.setCurrentText("1H")
            self._period_combo.blockSignals(False)
            self._secondary_period_combo.blockSignals(False)
            self._sync_primary_period_buttons()
            self._set_chart_view_range_mode("recent")
            self._apply_chart_view_range()
            self._refresh_timer.setInterval(self._auto_refresh_interval_ms("1H"))
        elif self._secondary_chart_kind() == "volatility":
            current_period = self._secondary_period_combo.currentText().strip().upper()
            if current_period not in {"1H", "4H", "1D", "1W"}:
                self._secondary_period_combo.blockSignals(True)
                self._secondary_period_combo.setCurrentText("1H")
                self._secondary_period_combo.blockSignals(False)
        if self._secondary_chart_check.isChecked():
            self._load_data()

    @Slot()
    def _on_secondary_sync_period_clicked(self) -> None:
        if not self._secondary_chart_check.isChecked():
            return
        if self._secondary_chart_kind() == "volatility":
            sequence = ("1H", "4H", "1D", "1W")
            current = self._period_combo.currentText().strip().upper()
            if current not in sequence:
                current = self._secondary_period_combo.currentText().strip().upper()
                if current not in sequence:
                    current = sequence[0]
            primary_next = sequence[(sequence.index(current) + 1) % len(sequence)]
            secondary_next = primary_next
        else:
            primary_next = "1D"
            secondary_next = "4H"

        self._period_combo.blockSignals(True)
        self._secondary_period_combo.blockSignals(True)
        self._period_combo.setCurrentText(primary_next)
        self._secondary_period_combo.setCurrentText(secondary_next)
        self._period_combo.blockSignals(False)
        self._secondary_period_combo.blockSignals(False)
        self._sync_primary_period_buttons()
        self._set_chart_view_range_mode("recent")
        self._apply_chart_view_range()
        self._refresh_timer.setInterval(self._auto_refresh_interval_ms(primary_next))
        self._load_data()

    @Slot()
    def _on_daily_timezone_compare_clicked(self) -> None:
        symbol = self._selected_symbol()
        if not symbol:
            self._set_status("请先选择交易对")
            return
        self._secondary_chart_kind_mode = "kline"
        self._secondary_layout_mode_value = "horizontal"
        controls = (
            self._period_combo,
            self._secondary_period_combo,
            self._secondary_symbol_combo,
            self._secondary_chart_check,
            self._tertiary_chart_check,
            self._quaternary_chart_check,
        )
        for control in controls:
            control.blockSignals(True)
        try:
            self._period_combo.setCurrentText("1D")
            self._secondary_period_combo.setCurrentText("1Dutc")
            self._secondary_symbol_combo.setCurrentText(symbol)
            self._tertiary_chart_check.setChecked(False)
            self._quaternary_chart_check.setChecked(False)
            self._secondary_chart_check.setChecked(True)
        finally:
            for control in controls:
                control.blockSignals(False)
        self._apply_secondary_chart_visibility()
        self._set_active_chart_target("primary")
        self._sync_primary_period_buttons()
        self._set_chart_view_range_mode("recent")
        self._apply_chart_view_range()
        self._refresh_timer.setInterval(self._auto_refresh_interval_ms("1D"))
        self._load_data()

    def _set_quad_layout_active(self, active: bool) -> None:
        splitter = self._chart_stack_splitter
        grid_host = self._quad_grid_host
        grid_layout = self._quad_grid_layout
        if splitter is None or grid_host is None or grid_layout is None:
            return
        frames = (
            self._primary_chart_frame,
            self._secondary_chart_frame,
            self._tertiary_chart_frame,
            self._quaternary_chart_frame,
        )
        if active:
            if not grid_host.isVisible():
                for index, frame in enumerate(frames):
                    if frame is None:
                        continue
                    frame.setParent(grid_host)
                    row, column = divmod(index, 2)
                    grid_layout.addWidget(frame, row, column)
                grid_layout.setRowStretch(0, 1)
                grid_layout.setRowStretch(1, 1)
                grid_layout.setColumnStretch(0, 1)
                grid_layout.setColumnStretch(1, 1)
            splitter.hide()
            grid_host.show()
            return
        if grid_host.isVisible():
            for frame in frames:
                if frame is None:
                    continue
                grid_layout.removeWidget(frame)
                frame.setParent(splitter)
                splitter.addWidget(frame)
            grid_host.hide()
        splitter.show()

    def _apply_secondary_chart_layout(self) -> None:
        splitter = self._chart_stack_splitter
        if splitter is None:
            return
        enabled = bool(self._secondary_chart_check.isChecked())
        if self._quad_chart_enabled():
            self._set_quad_layout_active(True)
            if self._quad_grid_host is not None:
                self._quad_grid_host.setVisible(enabled)
            return
        self._set_quad_layout_active(False)
        if self._triple_chart_enabled():
            splitter.setOrientation(Qt.Orientation.Horizontal)
            available_width = splitter.width() or self.width() or 1800
            third_width = max(1, available_width // 3)
            splitter.setSizes([third_width, third_width, third_width, 0])
            return
        layout_mode = self._secondary_layout_mode()
        splitter.setOrientation(Qt.Orientation.Horizontal if layout_mode == "horizontal" else Qt.Orientation.Vertical)
        if not enabled:
            splitter.setSizes([1, 0, 0, 0])
            return
        if layout_mode == "horizontal":
            available_width = splitter.width() or self.width() or 1600
            left_width, right_width = _default_chart_stack_horizontal_sizes(available_width)
            splitter.setSizes([left_width, right_width, 0, 0])
            return
        available_height = splitter.height() or self.height() or 920
        top_height, bottom_height = _default_chart_stack_splitter_sizes(available_height)
        splitter.setSizes([top_height, bottom_height, 0, 0])

    def _apply_secondary_chart_visibility(self) -> None:
        enabled = bool(self._secondary_chart_check.isChecked())
        if self._secondary_chart_frame is not None:
            self._secondary_chart_frame.setVisible(enabled)
        if self._tertiary_chart_frame is not None:
            self._tertiary_chart_frame.setVisible(self._triple_chart_enabled())
        if self._quaternary_chart_frame is not None:
            self._quaternary_chart_frame.setVisible(self._quad_chart_enabled())
        self._update_secondary_controls_state()
        self._apply_secondary_chart_layout()
        self._refresh_chart_mode_cycle_button()
        self._schedule_chart_layout_refresh(0)

    def _active_period_value(self) -> str:
        if self._active_chart_target == "secondary" and self._secondary_chart_check.isChecked():
            return self._secondary_period_combo.currentText().strip()
        if self._active_chart_target == "tertiary" and self._triple_chart_enabled():
            return self._tertiary_period_combo.currentText().strip()
        if self._active_chart_target == "quaternary" and self._quad_chart_enabled():
            return self._quaternary_period_combo.currentText().strip()
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
        if self._tertiary_chart_frame is not None:
            self._tertiary_chart_frame.setStyleSheet(
                self._chart_frame_style(active=self._active_chart_target == "tertiary" and self._triple_chart_enabled())
            )
        if self._quaternary_chart_frame is not None:
            self._quaternary_chart_frame.setStyleSheet(
                self._chart_frame_style(active=self._active_chart_target == "quaternary" and self._quad_chart_enabled())
            )

    def _set_active_chart_target(self, target: str) -> None:
        if target == "quaternary" and self._quad_chart_enabled():
            resolved = "quaternary"
        elif target == "tertiary" and self._triple_chart_enabled():
            resolved = "tertiary"
        elif target == "secondary" and self._secondary_chart_check.isChecked():
            resolved = "secondary"
        else:
            resolved = "primary"
        if self._active_chart_target == resolved:
            self._refresh_chart_selection_visuals()
            self._sync_primary_period_buttons()
            self._refresh_symbol_tab_selection()
            return
        self._active_chart_target = resolved
        self._refresh_chart_selection_visuals()
        self._sync_primary_period_buttons()
        self._refresh_symbol_tab_selection()

    def _symbol_for_chart_target(self, target: str | None = None) -> str:
        resolved = target or self._active_chart_target
        if resolved == "quaternary" and self._quad_chart_enabled():
            return self._selected_quaternary_symbol()
        if resolved == "tertiary" and self._triple_chart_enabled():
            return self._selected_tertiary_symbol()
        if resolved == "secondary" and self._secondary_chart_check.isChecked():
            return self._selected_secondary_symbol()
        return self._selected_symbol()

    @Slot()
    def _show_trade_recommendation(self) -> None:
        if bool(getattr(self, "_shutdown_requested", False)):
            return
        symbol = self._symbol_for_chart_target()
        period = self._active_period_value()
        now_ms = int(time.time() * 1000)
        volatility_view = self._all_charts_volatility_enabled() or (
            self._active_chart_target == "secondary" and self._secondary_chart_check.isChecked()
            and self._secondary_chart_kind() == "volatility"
        )
        try:
            if volatility_view:
                idea = KlineTradeIdea(symbol, period, now_ms, reasons=("当前选中的是波动率指数图，请切回现货或永续价格K线后查看做单参考。",))
            else:
                # Cache is keyed by the selected symbol and period. Never use a
                # previous chart's payload or its transformed/average candles.
                candles = load_candle_cache(symbol, period, limit=250)
                idea = build_kline_trade_idea(symbol=symbol, period=period, candles=candles, now_ms=now_ms)
        except Exception as exc:
            idea = KlineTradeIdea(symbol, period, now_ms, reasons=(f"本次参考生成失败（{type(exc).__name__}）；请刷新K线后再试。",))
        if self._trade_recommendation_dialog is None:
            self._trade_recommendation_dialog = KlineTradeRecommendationDialog(self)
        self._trade_recommendation_dialog.set_result(idea)
        self._trade_recommendation_dialog.show()
        self._trade_recommendation_dialog.raise_()
        self._trade_recommendation_dialog.activateWindow()

    def _refresh_symbol_tab_selection(self) -> None:
        tab_bar = self._symbol_tab_bar
        if tab_bar is None:
            return
        symbol = self._symbol_for_chart_target()
        if self._secondary_chart_kind() == "volatility":
            currency = self._active_volatility_currency or self._current_volatility_currency()
            symbol = f"__DVOL_{currency}__" if currency else symbol
        target_index = next(
            (
                index
                for index in range(tab_bar.count())
                if str(tab_bar.tabData(index) or "").strip().upper() == symbol
            ),
            -1,
        )
        if target_index < 0 or tab_bar.currentIndex() == target_index:
            return
        self._updating_symbol_tabs = True
        try:
            tab_bar.blockSignals(True)
            tab_bar.setCurrentIndex(target_index)
        finally:
            tab_bar.blockSignals(False)
            self._updating_symbol_tabs = False

    @Slot(int)
    def _on_symbol_tab_changed(self, index: int) -> None:
        if self._updating_symbol_tabs or self._symbol_tab_bar is None:
            return
        symbol = str(self._symbol_tab_bar.tabData(index) or "").strip().upper()
        if not symbol:
            return
        volatility_currency = None
        for _label, tab_value in VOLATILITY_TAB_OPTIONS:
            if symbol == tab_value:
                volatility_currency = tab_value.removeprefix("__DVOL_").removesuffix("__")
                break
        if volatility_currency:
            base_symbol = f"{volatility_currency}-USDT-SWAP"
            link_all = bool(self._symbol_link_all_check is not None and self._symbol_link_all_check.isChecked())
            linked_combos = (
                self._symbol_combo,
                self._secondary_symbol_combo,
                self._tertiary_symbol_combo,
                self._quaternary_symbol_combo,
            ) if link_all else (self._secondary_symbol_combo,)
            self._invalidate_pending_chart_results(
                targets=("primary", "secondary", "tertiary", "quaternary") if link_all else ("secondary",)
            )
            for combo in linked_combos:
                combo.blockSignals(True)
            try:
                for combo in linked_combos:
                    combo.setCurrentText(base_symbol)
                self._secondary_chart_kind_mode = "volatility"
                self._active_volatility_currency = volatility_currency
            finally:
                for combo in linked_combos:
                    combo.blockSignals(False)
            if not self._secondary_chart_check.isChecked():
                self._secondary_chart_check.blockSignals(True)
                self._secondary_chart_check.setChecked(True)
                self._secondary_chart_check.blockSignals(False)
                self._apply_secondary_chart_visibility()
            self._set_active_chart_target("secondary")
            self._refresh_symbol_tab_selection()
            self._update_secondary_controls_state()
            if link_all:
                for target in ("primary", "secondary", "tertiary", "quaternary"):
                    if target == "primary" or (
                        target == "secondary" and self._secondary_chart_check.isChecked()
                    ) or (
                        target == "tertiary" and self._triple_chart_enabled()
                    ) or (
                        target == "quaternary" and self._quad_chart_enabled()
                    ):
                        self._prepare_chart_for_symbol_reload(target)
                self._load_data()
            else:
                self._prepare_chart_for_symbol_reload("secondary")
                self._load_secondary_data(symbol=base_symbol)
            return
        # 普通交易对和 DVOL 标签使用同一套切换规则。离开 DVOL 时先
        # 恢复普通 K 线模式，后续分支再按“全部联动”或当前图处理品种。
        target = self._active_chart_target
        if self._secondary_chart_kind() == "volatility":
            self._invalidate_pending_chart_results(targets=("secondary",))
            self._retire_loader("_secondary_volatility_loader")
            if isinstance(self._loader, SecondaryVolatilityDataLoader):
                self._retire_loader("_loader")
            self._secondary_chart_kind_mode = "kline"
            self._active_volatility_currency = None
            self._secondary_pending_payload = None
            self._loaded_secondary_request_key = None
            self._refresh_secondary_chart_kind_button()
            self._refresh_secondary_sync_period_button()
            self._update_secondary_controls_state()
            self._apply_secondary_chart_visibility()
        if self._symbol_link_all_check is not None and self._symbol_link_all_check.isChecked():
            self._switch_all_chart_symbols(symbol)
            return
        self._invalidate_pending_chart_results(targets=(target,))
        if target == "secondary" and self._secondary_chart_check.isChecked():
            if self._selected_secondary_symbol() != symbol:
                self._secondary_symbol_combo.setCurrentText(symbol)
            else:
                self._on_secondary_symbol_changed(symbol)
            return
        if target == "tertiary" and self._triple_chart_enabled():
            if self._selected_tertiary_symbol() != symbol:
                self._tertiary_symbol_combo.setCurrentText(symbol)
            else:
                self._on_tertiary_symbol_changed(symbol)
            return
        if target == "quaternary" and self._quad_chart_enabled():
            if self._selected_quaternary_symbol() != symbol:
                self._quaternary_symbol_combo.setCurrentText(symbol)
            else:
                self._on_quaternary_symbol_changed(symbol)
            return
        if self._selected_symbol() != symbol:
            self._symbol_combo.setCurrentText(symbol)
        else:
            self._on_symbol_confirmed()

    def _switch_all_chart_symbols(self, symbol: str) -> None:
        combos = (
            self._symbol_combo,
            self._secondary_symbol_combo,
            self._tertiary_symbol_combo,
            self._quaternary_symbol_combo,
        )
        for combo in combos:
            combo.blockSignals(True)
        try:
            for combo in combos:
                combo.setCurrentText(symbol)
        finally:
            for combo in combos:
                combo.blockSignals(False)
        self._invalidate_pending_chart_results(targets=("primary", "secondary", "tertiary", "quaternary"))
        self._on_symbol_confirmed(refresh_linked_charts=True)

    def _invalidate_pending_chart_results(self, *, targets: tuple[str, ...]) -> None:
        """Make only the changed chart's in-flight callbacks stale.

        Loaders are intentionally allowed to finish in the background.  The
        request counters prevent an older symbol (for example SOL) from being
        painted after the user has already selected another tab (for example
        ETH), without discarding a still-valid load for another visible chart.
        """
        if "primary" in targets:
            self._request_id += 1
            self._active_request_id = self._request_id
            self._active_primary_request_key = None
        if "secondary" in targets:
            self._secondary_request_id += 1
            self._active_secondary_request_id = self._secondary_request_id
            self._active_secondary_request_key = None
        if "tertiary" in targets:
            self._tertiary_request_id += 1
            self._active_tertiary_request_id = self._tertiary_request_id
            self._active_tertiary_request_key = None
        if "quaternary" in targets:
            self._quaternary_request_id += 1
            self._active_quaternary_request_id = self._quaternary_request_id
            self._active_quaternary_request_key = None

    def _apply_chart_mode_period_defaults(self, *, dual_enabled: bool) -> None:
        primary_period = _DEFAULT_DUAL_PRIMARY_PERIOD if dual_enabled else _DEFAULT_SINGLE_CHART_PERIOD
        secondary_period = _DEFAULT_DUAL_SECONDARY_PERIOD
        self._period_combo.blockSignals(True)
        self._secondary_period_combo.blockSignals(True)
        self._tertiary_period_combo.blockSignals(True)
        self._quaternary_period_combo.blockSignals(True)
        self._period_combo.setCurrentText(primary_period)
        self._secondary_period_combo.setCurrentText(secondary_period)
        self._tertiary_period_combo.setCurrentText(secondary_period)
        self._quaternary_period_combo.setCurrentText("1W")
        self._period_combo.blockSignals(False)
        self._secondary_period_combo.blockSignals(False)
        self._tertiary_period_combo.blockSignals(False)
        self._quaternary_period_combo.blockSignals(False)
        self._sync_primary_period_buttons()
        self._refresh_timer.setInterval(self._auto_refresh_interval_ms(primary_period))
        self._reload_workspace_view()

    def _apply_quad_chart_period_defaults(self) -> None:
        """Use the fixed four-chart comparison order: 1W / 1D / 4H / 1H."""
        controls = (
            self._period_combo,
            self._secondary_period_combo,
            self._tertiary_period_combo,
            self._quaternary_period_combo,
        )
        for control in controls:
            control.blockSignals(True)
        try:
            self._period_combo.setCurrentText("1W")
            self._secondary_period_combo.setCurrentText("1D")
            self._tertiary_period_combo.setCurrentText("4H")
            self._quaternary_period_combo.setCurrentText("1H")
        finally:
            for control in controls:
                control.blockSignals(False)
        self._sync_primary_period_buttons()
        self._refresh_timer.setInterval(self._auto_refresh_interval_ms("1W"))
        self._reload_workspace_view()

    @Slot(str)
    def _on_period_button_clicked(self, period_value: str) -> None:
        if self._active_chart_target == "quaternary" and self._quad_chart_enabled():
            if self._quaternary_period_combo.currentText().strip() != period_value:
                self._quaternary_period_combo.setCurrentText(period_value)
            else:
                self._load_quaternary_data()
            return
        if self._active_chart_target == "tertiary" and self._triple_chart_enabled():
            if self._tertiary_period_combo.currentText().strip() != period_value:
                self._tertiary_period_combo.setCurrentText(period_value)
            else:
                self._load_tertiary_data()
            return
        if self._active_chart_target == "secondary" and self._secondary_chart_check.isChecked():
            if self._secondary_chart_kind() == "volatility" and period_value not in {"1H", "4H", "1D", "1W"}:
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
        if not bool(getattr(self, "_shutdown_complete", False)):
            event.ignore()
            if not bool(getattr(self, "_close_pending", False)):
                self._close_pending = True
                self.begin_shutdown(lambda: QTimer.singleShot(0, self.close))
            return
        super().closeEvent(event)

    def keyPressEvent(self, event) -> None:  # noqa: ANN001
        if event.key() == Qt.Key.Key_F11:
            self._toggle_chart_fullscreen()
            event.accept()
            return
        if event.key() == Qt.Key.Key_Escape and self._chart_fullscreen_enabled:
            self._set_chart_fullscreen(False)
            event.accept()
            return
        if event.key() == Qt.Key.Key_Escape and self._draw_tool != "none":
            self._set_draw_tool("none")
            event.accept()
            return
        if event.key() in (Qt.Key.Key_Delete, Qt.Key.Key_Backspace):
            if self._selected_line_index >= 0:
                self._delete_selected_line()
                event.accept()
                return
            if self._selected_rr_index >= 0:
                self._remove_rr_item()
                event.accept()
                return
        super().keyPressEvent(event)

    def _has_active_loaders(self) -> bool:
        # Keep a slot occupied until the queued ``finished`` callback clears
        # the reference, even when QThread.isRunning() has already flipped false.
        return any(
            loader is not None
            for loader in (
                self._loader,
                self._secondary_loader,
                self._tertiary_loader,
                self._quaternary_loader,
                self._secondary_volatility_loader,
            )
        )

    def _refresh_api_profiles(self) -> None:
        self._profile_snapshots, selected_profile = load_profile_snapshots()
        self._suppress_api_profile_change = True
        try:
            runtime = self._runtime if self._runtime is not None else load_runtime("159") or load_runtime()
            runtime_profile = ""
            if runtime is not None:
                self._runtime = runtime
                runtime_profile = str(getattr(runtime, "credential_profile_name", "") or "").strip()
                if not runtime_profile:
                    credentials = getattr(runtime, "credentials", None)
                    runtime_profile = str(getattr(credentials, "profile_name", "") or "").strip()
            names: list[str] = []
            for candidate in (
                *list(self._profile_snapshots),
                *(str(item).strip() for item in profile_names()),
                selected_profile,
                runtime_profile,
            ):
                normalized = str(candidate or "").strip()
                if normalized and normalized not in names:
                    names.append(normalized)
            self._unlocked_profiles.intersection_update(set(names))
            if runtime_profile:
                self._unlocked_profiles.add(runtime_profile)
            self._api_profile_combo.clear()
            if names:
                self._api_profile_combo.addItems(names)
            selected = runtime_profile
            if not selected:
                selected = selected_profile if selected_profile in names else ""
            if not selected:
                selected = "moni" if "moni" in names else (names[0] if names else "")
            if selected:
                index = self._api_profile_combo.findText(selected)
                if index >= 0:
                    self._api_profile_combo.setCurrentIndex(index)
                if runtime is None or selected != runtime_profile:
                    runtime = load_runtime(selected)
                    if runtime is not None:
                        self._runtime = runtime
                        self._unlocked_profiles.add(selected)
            elif self._api_profile_combo.count() > 0:
                self._api_profile_combo.setCurrentIndex(0)
                selected = self._api_profile_combo.currentText().strip()
                if selected:
                    runtime = load_runtime(selected)
                    if runtime is not None:
                        self._runtime = runtime
                        self._unlocked_profiles.add(selected)
        finally:
            self._suppress_api_profile_change = False
        self._last_profile_name = self._api_profile_combo.currentText().strip() if hasattr(self, "_api_profile_combo") else ""
        self._sync_account_context()

    def _ensure_profile_access(self, profile_name: str) -> bool:
        if profile_name.strip() not in self._profile_snapshots:
            self._profile_snapshots, _selected_profile = load_profile_snapshots()
            self._unlocked_profiles.intersection_update(set(self._profile_snapshots))
        return ensure_profile_unlocked(self, profile_name, self._profile_snapshots, self._unlocked_profiles)

    def _restore_api_profile_selection(self) -> None:
        if not self._last_profile_name:
            return
        index = self._api_profile_combo.findText(self._last_profile_name)
        if index < 0:
            return
        self._suppress_api_profile_change = True
        try:
            self._api_profile_combo.setCurrentIndex(index)
        finally:
            self._suppress_api_profile_change = False

    def _active_profile_name(self) -> str:
        runtime_profile = ""
        if self._runtime is not None:
            runtime_profile = str(getattr(self._runtime, "credential_profile_name", "") or "").strip()
            if not runtime_profile:
                credentials = getattr(self._runtime, "credentials", None)
                runtime_profile = str(getattr(credentials, "profile_name", "") or "").strip()
        combo_profile = self._api_profile_combo.currentText().strip() if hasattr(self, "_api_profile_combo") else ""
        return runtime_profile or combo_profile

    def _active_environment(self) -> str:
        if self._runtime is None:
            return ""
        return str(getattr(self._runtime, "environment", "") or "").strip()

    def _history_trade_context_key(self) -> tuple[str, str, str]:
        return (
            self._active_profile_name().strip(),
            self._active_environment().strip(),
            self._selected_symbol().strip().upper(),
        )

    def _visible_history_trade_markers(self) -> list[KlineHistoryTradeMarker]:
        if not self._history_trades_supported():
            return []
        if not hasattr(self, "_show_history_trades_check") or not self._show_history_trades_check.isChecked():
            return []
        markers = self._history_trade_markers_by_context.get(self._history_trade_context_key(), ())
        return _filter_kline_history_trade_markers(markers, direction_filter=self._history_trade_direction_filter())

    def _history_trades_supported(self) -> bool:
        return self._period_combo.currentText().strip().upper() == "1H"

    def _refresh_history_trade_availability(self) -> None:
        if not hasattr(self, "_history_trade_group"):
            return
        self._history_trade_group.setVisible(self._history_trades_supported())

    def _history_trade_direction_filter(self) -> str:
        if not hasattr(self, "_history_trade_direction_combo"):
            return "all"
        return str(self._history_trade_direction_combo.currentData() or "all").strip().lower()

    def _load_history_trades(self, *, force: bool = False) -> None:
        if self._preview_mode or bool(getattr(self, "_shutdown_requested", False)):
            return
        if not self._history_trades_supported():
            return
        if not hasattr(self, "_show_history_trades_check") or not self._show_history_trades_check.isChecked():
            return
        context_key = self._history_trade_context_key()
        if not all(context_key) or self._runtime is None:
            return
        if not force and context_key in self._history_trade_markers_by_context:
            return
        if self._history_trade_loader is not None:
            self._pending_history_trade_reload = True
            if force:
                self._set_status("历史交易仍在同步，请稍候...")
            return
        self._history_trade_request_id += 1
        request_id = self._history_trade_request_id
        self._active_history_trade_request_id = request_id
        self._history_trade_context_by_request[request_id] = context_key
        loader = KlineHistoryTradeLoader(
            request_id=request_id,
            runtime=self._runtime,
            symbol=context_key[2],
        )
        self._history_trade_loader = loader
        loader.loaded.connect(self._on_history_trades_loaded)
        loader.failed.connect(self._on_history_trades_failed)
        loader.finished.connect(self._on_history_trade_loader_finished)
        loader.start()

    @Slot(int, object)
    def _on_history_trades_loaded(self, request_id: int, markers: object) -> None:
        if request_id != self._active_history_trade_request_id:
            return
        parsed = tuple(item for item in markers if isinstance(item, KlineHistoryTradeMarker)) if isinstance(markers, (tuple, list)) else ()
        context_key = self._history_trade_context_by_request.pop(request_id, self._history_trade_context_key())
        self._history_trade_markers_by_context[context_key] = parsed
        self._history_trade_sync_error_by_context.pop(context_key, None)
        self._sync_chart_options()

    @Slot(int, str)
    def _on_history_trades_failed(self, request_id: int, message: str) -> None:
        if request_id != self._active_history_trade_request_id:
            return
        context_key = self._history_trade_context_by_request.pop(request_id, self._history_trade_context_key())
        self._history_trade_sync_error_by_context[context_key] = str(message)
        self._set_status(f"历史交易同步失败：{message}")

    @Slot()
    def _on_history_trade_loader_finished(self) -> None:
        self._release_finished_loader("_history_trade_loader")
        if self._pending_history_trade_reload and not bool(getattr(self, "_shutdown_requested", False)):
            self._pending_history_trade_reload = False
            QTimer.singleShot(0, self._load_history_trades)

    @Slot(bool)
    def _on_history_trade_visibility_changed(self, enabled: bool) -> None:
        self._history_trade_direction_combo.setEnabled(enabled)
        self._sync_history_trades_button.setEnabled(enabled)
        if enabled and self._history_trades_supported():
            self._load_history_trades()
        self._sync_chart_options()

    @Slot(bool)
    def _on_best_parameter_indicators_changed(self, enabled: bool) -> None:
        self._ema9.setEnabled(not enabled)
        self._ema21.setEnabled(not enabled)
        self._ema21_55_check.setEnabled(not enabled)
        self._sync_chart_options()

    def _runtime_for_task_profile(self, profile_name: str):
        target = profile_name.strip()
        if not target:
            return None
        runtime = load_runtime(target)
        if runtime is None:
            return None
        runtime_profile = str(getattr(runtime, "credential_profile_name", "") or "").strip()
        if not runtime_profile:
            credentials = getattr(runtime, "credentials", None)
            runtime_profile = str(getattr(credentials, "profile_name", "") or "").strip()
        return runtime if runtime_profile == target else None

    def _sync_account_context(self) -> None:
        profile_name = self._active_profile_name() or "-"
        environment = self._active_environment() or "-"
        if hasattr(self, "_account_context"):
            self._account_context.setText(f"账户 {profile_name} | {environment}")
        self._refresh_rr_trade_hint()
        self._sync_account_drawer_context()

    def _sync_account_drawer_context(self, *, refresh_if_visible: bool = True) -> None:
        if self._account_drawer is None:
            return
        self._account_drawer.set_context(
            runtime=self._runtime,
            profile_name=self._active_profile_name(),
            environment=self._active_environment(),
            symbol=self._selected_symbol(),
            refresh_if_visible=refresh_if_visible,
        )

    def _schedule_account_drawer_prefetch_after_chart_render(self) -> None:
        if self._account_drawer is None or self._runtime is None:
            return
        context = (self._active_profile_name(), self._active_environment())
        if not all(context) or context == self._account_prefetch_context or self._account_prefetch_timer.isActive():
            return
        # Give the first K-line paint priority.  The account request itself is
        # performed by RealtimeAccountStore on a background thread.
        self._account_prefetch_timer.start(350)

    @Slot()
    def _prefetch_account_drawer_after_chart_ready(self) -> None:
        if bool(getattr(self, "_preview_mode", False)) or bool(getattr(self, "_shutdown_requested", False)):
            return
        if self._account_drawer is None or self._runtime is None:
            return
        context = (self._active_profile_name(), self._active_environment())
        if not all(context) or context == self._account_prefetch_context:
            return
        self._account_prefetch_context = context
        self._sync_account_drawer_context(refresh_if_visible=False)
        self._account_drawer._realtime_store.request_reconcile("kline-prefetch")

    def _instrument_for_symbol(self, symbol: str | None = None) -> object | None:
        normalized = (symbol or self._selected_symbol()).strip().upper()
        if not normalized:
            return None
        if normalized in self._instrument_cache:
            return self._instrument_cache[normalized]
        try:
            instrument = self._market_client.get_instrument(normalized, prefer_cached=True)
        except Exception:
            instrument = None
        self._instrument_cache[normalized] = instrument
        return instrument

    def _format_rr_size_with_coin(self, inst_id: str, size: object) -> str:
        """Show RR filled/remaining size as coin amount followed by contracts."""
        try:
            contracts = Decimal(str(size or "0"))
        except Exception:
            return "-"
        contract_text = format_decimal(contracts)
        normalized_id = str(inst_id or "").strip().upper()
        inst_type = infer_inst_type(normalized_id)
        instrument = self._instrument_for_symbol(normalized_id)
        instruments = {normalized_id: instrument} if instrument is not None else {}
        amount, currency = _history_display_amount(
            inst_id=normalized_id,
            inst_type=inst_type,
            size=abs(contracts),
            reference_price=None,
            instruments=instruments,
            use_swap_contract_fallback=True,
        )
        coin_text = _format_history_size_amount(amount, currency)
        if inst_type not in {"SWAP", "FUTURES", "OPTION"}:
            return coin_text or contract_text
        if contracts == 0:
            return f"0（{contract_text} 张）"
        if not coin_text or coin_text == "-" or coin_text.endswith(" 张"):
            return f"{contract_text} 张"
        return f"{coin_text}（{contract_text} 张）"

    def _current_rr_price_increment(self, item: dict[str, object] | None = None) -> Decimal | None:
        symbol = ""
        if isinstance(item, dict):
            symbol = str(item.get("inst_id", "") or "").strip().upper()
        instrument = self._instrument_for_symbol(symbol or None)
        tick_size = getattr(instrument, "tick_size", None)
        if isinstance(tick_size, Decimal) and tick_size > 0:
            return tick_size
        return None

    @Slot()
    def _on_api_profile_changed(self) -> None:
        if self._suppress_api_profile_change:
            return
        selected = self._api_profile_combo.currentText().strip()
        if not selected:
            self._runtime = None
            self._last_profile_name = ""
            self._sync_account_context()
            return
        self._profile_snapshots, _selected_profile = load_profile_snapshots()
        self._unlocked_profiles.intersection_update(set(self._profile_snapshots))
        if selected != self._last_profile_name and not self._ensure_profile_access(selected):
            self._restore_api_profile_selection()
            return
        runtime = load_runtime(selected)
        if runtime is None:
            self._sync_account_context()
            return
        self._runtime = runtime
        self._last_profile_name = selected
        self._sync_account_context()
        self._load_data()

    def _matching_rr_trade_ledger_entries(self, *, symbol: str | None = None) -> list[RRTradeLedgerEntry]:
        target_symbol = (symbol or self._selected_symbol()).strip().upper()
        target_profile = self._active_profile_name().strip()
        target_environment = self._active_environment().strip()
        matched: list[RRTradeLedgerEntry] = []
        for entry in self._all_rr_trade_ledger_entries():
            plan = entry.plan
            if target_profile and plan.profile_name.strip() != target_profile:
                continue
            if target_environment and plan.environment.strip() != target_environment:
                continue
            if target_symbol and plan.inst_id.strip().upper() != target_symbol:
                continue
            matched.append(entry)
        return matched

    def _all_rr_trade_ledger_entries(self) -> list[RRTradeLedgerEntry]:
        snapshot = self._rr_trade_ledger_snapshot if isinstance(self._rr_trade_ledger_snapshot, dict) else {}
        raw_entries = snapshot.get("entries", [])
        if not isinstance(raw_entries, list):
            return []
        entries: list[RRTradeLedgerEntry] = []
        for item in raw_entries:
            if not isinstance(item, dict):
                continue
            try:
                entries.append(RRTradeLedgerEntry.from_dict(item))
            except Exception:
                continue
        return entries

    def _monitorable_rr_trade_ledger_entries(self) -> list[RRTradeLedgerEntry]:
        return [
            entry
            for entry in self._all_rr_trade_ledger_entries()
            if self._rr_trade_execution_service.should_monitor_status(entry.status)
        ]

    def _next_monitorable_rr_entry(self) -> RRTradeLedgerEntry | None:
        entries = self._monitorable_rr_trade_ledger_entries()
        if not entries:
            self._rr_monitor_cursor = 0
            return None
        index = self._rr_monitor_cursor % len(entries)
        self._rr_monitor_cursor = (index + 1) % len(entries)
        return entries[index]

    def open_rr_monitor_dialog(self) -> None:
        entries = self._monitorable_rr_trade_ledger_entries()
        dialog = QDialog(self)
        dialog.setWindowTitle("RR 监控")
        dialog.setMinimumWidth(880)

        layout = QVBoxLayout(dialog)
        hint = QLabel(
            "这里列出本程序仍在轮询的 RR 账本记录。清空仅停止本程序监控，"
            "不会平仓，也不会撤销 OKX 的止盈止损单。",
            dialog,
        )
        hint.setWordWrap(True)
        layout.addWidget(hint)

        table = QTableWidget(len(entries), 9, dialog)
        table.setHorizontalHeaderLabels(
            ["账户", "环境", "合约", "方向", "状态", "已成交（币/张）", "剩余（币/张）", "止损/止盈", "最后更新"]
        )
        table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        table.setSelectionMode(QTableWidget.SelectionMode.SingleSelection)
        table.verticalHeader().setVisible(False)
        header = table.horizontalHeader()
        for column in range(9):
            header.setSectionResizeMode(column, QHeaderView.ResizeMode.ResizeToContents)
        header.setStretchLastSection(True)
        for row, entry in enumerate(entries):
            plan = entry.plan
            protection_text = (
                f"止损 {getattr(entry.stop_loss_order, 'state', '-') or '-'} / "
                f"止盈 {getattr(entry.take_profit_order, 'state', '-') or '-'}"
            )
            values = (
                plan.profile_name or "-",
                plan.environment or "-",
                plan.inst_id or "-",
                "多" if plan.direction == "long" else "空",
                entry.status or "-",
                self._format_rr_size_with_coin(plan.inst_id, entry.filled_size),
                self._format_rr_size_with_coin(plan.inst_id, entry.remaining_size),
                protection_text,
                entry.updated_at or entry.created_at or "-",
            )
            for column, value in enumerate(values):
                table.setItem(row, column, QTableWidgetItem(str(value)))
        layout.addWidget(table)

        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Close, parent=dialog)
        clear_button = buttons.addButton("清空本地监控", QDialogButtonBox.ButtonRole.DestructiveRole)
        buttons.rejected.connect(dialog.reject)
        buttons.accepted.connect(dialog.accept)
        layout.addWidget(buttons)

        def _clear_monitoring() -> None:
            if not entries:
                return
            confirmed = QMessageBox.question(
                dialog,
                "清空本地 RR 监控",
                f"将删除 {len(entries)} 条本地 RR 监控账本记录，顶部 RR 计数会归零。\n\n"
                "不会平仓，也不会撤销 OKX 的止盈止损单；清空后本程序不再跟踪这些记录。是否继续？",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                QMessageBox.StandardButton.No,
            )
            if confirmed != QMessageBox.StandardButton.Yes:
                return
            removed = self._clear_rr_monitor_entries(entries)
            self._set_status(f"已清空 {removed} 条本地 RR 监控；未对 OKX 仓位或保护单执行操作。")
            dialog.accept()

        clear_button.clicked.connect(_clear_monitoring)
        dialog.exec()

    def _clear_rr_monitor_entries(self, entries: list[RRTradeLedgerEntry]) -> int:
        plan_ids = {str(entry.plan.plan_id or "").strip() for entry in entries}
        plan_ids.discard("")
        if not plan_ids:
            return 0
        snapshot = self._rr_trade_ledger_snapshot if isinstance(self._rr_trade_ledger_snapshot, dict) else {}
        raw_entries = list(snapshot.get("entries", [])) if isinstance(snapshot.get("entries"), list) else []
        retained_entries: list[dict[str, object]] = []
        removed = 0
        for raw in raw_entries:
            if not isinstance(raw, dict):
                continue
            try:
                ledger_entry = RRTradeLedgerEntry.from_dict(raw)
            except Exception:
                retained_entries.append(raw)
                continue
            if str(ledger_entry.plan.plan_id or "").strip() in plan_ids:
                removed += 1
                continue
            retained_entries.append(raw)
        if removed:
            self._rr_trade_ledger_snapshot = {"entries": retained_entries}
            self._rr_monitor_cursor = 0
            save_kline_rr_trade_ledger_snapshot(retained_entries)
            self._refresh_rr_trade_hint()
        return removed

    def _refresh_rr_trade_hint(self, *, symbol: str | None = None) -> None:
        if not hasattr(self, "_rr_trade_hint"):
            return
        matched = self._matching_rr_trade_ledger_entries(symbol=symbol)
        profile_name = self._active_profile_name() or "-"
        environment = self._active_environment() or "-"
        display_symbol = (symbol or self._selected_symbol()).strip().upper() or "-"
        self._rr_trade_hint.setText(
            f"RR账本：{profile_name} | {environment} | {display_symbol} | 记录 {len(matched)}"
        )

    def _current_primary_request_key(self) -> tuple[Any, ...]:
        if self._all_charts_volatility_enabled():
            return (
                "primary",
                "volatility",
                self._current_volatility_currency(),
                self._period_combo.currentText().strip().upper(),
                max(50, self._limit_spin.value()),
                self._secondary_average_kline_enabled(),
            )
        return (
            "primary",
            self._selected_symbol(),
            self._period_combo.currentText().strip().upper(),
            max(50, self._limit_spin.value()),
            bool(self._prefer_local_checkbox.isChecked()),
            self._primary_average_kline_enabled(),
            bool(getattr(self, "_auto_channel_check", None) and self._auto_channel_check.isChecked()),
            bool(getattr(self, "_auto_box_check", None) and self._auto_box_check.isChecked()),
            bool(getattr(self, "_history_box_check", None) and self._history_box_check.isChecked()),
            self.pattern_signals_enabled(),
            bool(self._reverse_kline_check.isChecked()),
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
                self._current_volatility_currency(),
                secondary_period,
                requested_limit,
                self._secondary_average_kline_enabled(),
                False,
            )
        return (
            "secondary",
            chart_kind,
            (symbol or self._selected_secondary_symbol()),
            secondary_period,
            requested_limit,
            bool(self._prefer_local_checkbox.isChecked()),
            self._secondary_average_kline_enabled(),
            bool(self._reverse_kline_check.isChecked()),
        )

    def _current_tertiary_request_key(self) -> tuple[Any, ...] | None:
        if not self._triple_chart_enabled() or not self._use_native_chart:
            return None
        if self._all_charts_volatility_enabled():
            return (
                "tertiary",
                "volatility",
                self._current_volatility_currency(),
                self._tertiary_period_combo.currentText().strip().upper(),
                max(50, self._limit_spin.value()),
                self._secondary_average_kline_enabled(),
            )
        return (
            "tertiary",
            "kline",
            self._selected_tertiary_symbol(),
            self._tertiary_period_combo.currentText().strip().upper(),
            max(50, self._limit_spin.value()),
            bool(self._prefer_local_checkbox.isChecked()),
            self._secondary_average_kline_enabled(),
            bool(self._reverse_kline_check.isChecked()),
        )

    def _current_quaternary_request_key(self) -> tuple[Any, ...] | None:
        if not self._quad_chart_enabled() or not self._use_native_chart:
            return None
        if self._all_charts_volatility_enabled():
            return (
                "quaternary",
                "volatility",
                self._current_volatility_currency(),
                self._quaternary_period_combo.currentText().strip().upper(),
                max(50, self._limit_spin.value()),
                self._secondary_average_kline_enabled(),
            )
        return (
            "quaternary",
            "kline",
            self._selected_quaternary_symbol(),
            self._quaternary_period_combo.currentText().strip().upper(),
            max(50, self._limit_spin.value()),
            bool(self._prefer_local_checkbox.isChecked()),
            self._secondary_average_kline_enabled(),
            bool(self._reverse_kline_check.isChecked()),
        )

    def _remember_payload_cache(
        self,
        cache: dict[tuple[Any, ...], KlineChartPayload],
        key: tuple[Any, ...] | None,
        payload: KlineChartPayload,
    ) -> None:
        if key is None:
            return
        cache[key] = payload
        while len(cache) > _KLINE_PAYLOAD_CACHE_LIMIT:
            oldest_key = next(iter(cache))
            cache.pop(oldest_key, None)

    def _preview_cached_primary_payload(self, request_key: tuple[Any, ...]) -> bool:
        payload = self._primary_payload_cache.get(request_key)
        if payload is None or not self._page_ready:
            return False
        self._pending_payload = payload
        self._loaded_primary_request_key = request_key
        self._render_loaded_payload(payload)
        return True

    def _preview_cached_secondary_payload(self, request_key: tuple[Any, ...] | None) -> bool:
        if request_key is None:
            return False
        payload = self._secondary_payload_cache.get(request_key)
        if payload is None or not self._page_ready:
            return False
        self._secondary_pending_payload = payload
        self._loaded_secondary_request_key = request_key
        self._render_secondary_chart(payload)
        self._sync_secondary_chart_range_from_primary()
        return True

    def _schedule_pending_reload_if_ready(self) -> None:
        if bool(getattr(self, "_shutdown_requested", False)):
            return
        if not self._pending_reload_after_load and not (
            self._pending_secondary_reload
            or self._pending_tertiary_reload
            or self._pending_quaternary_reload
        ):
            return
        if not self._page_active:
            return
        primary_busy = self._loader is not None and self._loader.isRunning()
        if self._pending_reload_after_load and not primary_busy:
            self._pending_reload_after_load = False
            self._schedule_load_data(10)
        if self._pending_secondary_reload and self._secondary_loader is None and self._secondary_volatility_loader is None:
            self._pending_secondary_reload = False
            self._load_secondary_data(symbol=self._selected_secondary_symbol())
        if self._pending_tertiary_reload and not (
            self._tertiary_loader is not None and self._tertiary_loader.isRunning()
        ):
            self._pending_tertiary_reload = False
            self._load_tertiary_data()
        if self._pending_quaternary_reload and not (
            self._quaternary_loader is not None and self._quaternary_loader.isRunning()
        ):
            self._pending_quaternary_reload = False
            self._load_quaternary_data()
        still_waiting = (
            (self._pending_reload_after_load and primary_busy)
            or (
                self._pending_secondary_reload
                and (self._secondary_loader is not None or self._secondary_volatility_loader is not None)
            )
            or (
                self._pending_tertiary_reload
                and self._tertiary_loader is not None
                and self._tertiary_loader.isRunning()
            )
            or (
                self._pending_quaternary_reload
                and self._quaternary_loader is not None
                and self._quaternary_loader.isRunning()
            )
        )
        retry_timer = getattr(self, "_pending_reload_retry_timer", None)
        if retry_timer is not None:
            if still_waiting and not retry_timer.isActive():
                retry_timer.start(120)
            elif not still_waiting and retry_timer.isActive():
                retry_timer.stop()

    @Slot()
    def _load_data(self, *, refresh_linked_charts: bool = True) -> None:
        if bool(getattr(self, "_shutdown_requested", False)) or not self._page_active:
            return
        symbol = self._selected_symbol()
        period = self._period_combo.currentText()
        self._refresh_history_trade_availability()
        _debug_log(f"[kline] _load_data begin | symbol={symbol or '-'} | period={period or '-'}")
        if not symbol:
            self._set_status("请输入交易对")
            return
        all_charts_volatility = self._all_charts_volatility_enabled()
        if self._loader is not None and self._loader.isRunning():
            running_volatility_loader = isinstance(self._loader, SecondaryVolatilityDataLoader)
            if running_volatility_loader != all_charts_volatility:
                self._retire_loader("_loader")
        if self._loader is not None and self._loader.isRunning():
            self._pending_reload_after_load = True
            self._set_status("当前仍在加载，已排队刷新最新选择，请稍候...")
            self._schedule_pending_reload_if_ready()
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
        previewed_cached_payload = self._preview_cached_primary_payload(self._active_primary_request_key)
        if all_charts_volatility and self._realtime_candle_unsubscribe is not None:
            try:
                self._realtime_candle_unsubscribe()
            except Exception:
                pass
            self._realtime_candle_unsubscribe = None
            self._realtime_candle_key = None
        self._set_status(
            f"正在加载 {'波动率' if all_charts_volatility else 'K线'} {symbol} {period} | 数量={requested_limit} | "
            f"{'本地缓存' if self._prefer_local_checkbox.isChecked() else '本地优先'}"
            f"{' | 已先显示内存缓存' if previewed_cached_payload else ''}"
        )
        if self._page_active and self._use_native_chart and self._native_chart is not None:
            self._native_chart.setTitle(f"正在加载 {'波动率' if all_charts_volatility else 'K线'}：{symbol} {period}")

        if all_charts_volatility:
            self._loader = SecondaryVolatilityDataLoader(
                request_id=self._request_id,
                currency=self._current_volatility_currency() or "",
                period=period,
                limit=requested_limit,
                average_kline=self._secondary_average_kline_enabled(),
                local_only=self._preview_mode or self._prefer_local_checkbox.isChecked(),
            )
        else:
            self._loader = KlineDataLoader(
                request_id=self._request_id,
                symbol=symbol,
                period=period,
                limit=requested_limit,
                local_only=self._preview_mode or self._prefer_local_checkbox.isChecked(),
                average_kline=self._primary_average_kline_enabled(),
                workspace_entry=workspace_entry,
                enable_alerts=not self._preview_mode,
                enable_shape_signals=self.pattern_signals_enabled(),
            )
        self._loader.loaded.connect(self._on_data_loaded)
        self._loader.failed.connect(self._on_data_failed)
        self._loader.finished.connect(self._on_loader_finished)
        self._loader.start()
        if not self._preview_mode:
            self._load_history_trades()
        if self._secondary_chart_check.isChecked() and self._use_native_chart:
            if refresh_linked_charts:
                self._load_secondary_data(symbol=self._selected_secondary_symbol())
                if self._triple_chart_enabled():
                    self._load_tertiary_data()
                if self._quad_chart_enabled():
                    self._load_quaternary_data()
        else:
            self._active_secondary_request_key = None
            self._loaded_secondary_request_key = None
            self._secondary_pending_payload = None
            if isinstance(self._secondary_native_chart_view, InteractiveKlineChartView):
                self._secondary_native_chart_view.set_external_hover_time(None)
            if self._secondary_native_chart is not None:
                self._secondary_native_chart.setTitle("副图")
            self._tertiary_pending_payload = None
            self._quaternary_pending_payload = None

    @Slot()
    def _on_loader_finished(self) -> None:
        self._release_finished_loader("_loader")
        self._schedule_pending_reload_if_ready()

    def _retire_loader(self, attribute: str) -> None:
        """Keep an obsolete worker owned until its completion signal is delivered."""
        loader = getattr(self, attribute, None)
        if loader is None:
            return
        if not loader.isRunning():
            # A completed thread can still have its queued finished callback
            # pending; keep the slot occupied until that callback releases it.
            return
        loader.requestInterruption()
        # The request slot can be reused immediately, while the retired set
        # keeps the QThread alive until its queued ``finished`` callback.
        self._retired_loaders.add(loader)
        if getattr(self, attribute, None) is loader:
            setattr(self, attribute, None)

    def _release_finished_loader(self, attribute: str) -> None:
        loader = self.sender()
        if loader is None:
            loader = getattr(self, attribute, None)
        if loader is None or loader.isRunning():
            return
        self._retired_loaders.discard(loader)
        if getattr(self, attribute, None) is loader:
            setattr(self, attribute, None)
        loader.deleteLater()

    def _prepare_chart_for_symbol_reload(self, target: str) -> None:
        """Remove the previous series while a newly selected symbol loads."""
        chart_map = {
            "primary": (self._native_chart, self._native_chart_view),
            "secondary": (self._secondary_native_chart, self._secondary_native_chart_view),
            "tertiary": (self._tertiary_native_chart, self._tertiary_native_chart_view),
            "quaternary": (self._quaternary_native_chart, self._quaternary_native_chart_view),
        }
        pending_names = {
            "primary": ("_pending_payload", "_loaded_primary_request_key"),
            "secondary": ("_secondary_pending_payload", "_loaded_secondary_request_key"),
            "tertiary": ("_tertiary_pending_payload", "_loaded_tertiary_request_key"),
            "quaternary": ("_quaternary_pending_payload", "_loaded_quaternary_request_key"),
        }
        chart, chart_view = chart_map.get(target, (None, None))
        for name in pending_names.get(target, ()):
            setattr(self, name, None)
        if chart is None:
            return
        if isinstance(chart_view, InteractiveKlineChartView):
            chart_view.clear_chart_context()
        chart.removeAllSeries()
        for axis in list(chart.axes()):
            chart.removeAxis(axis)
        chart.setTitle("正在加载最新交易对...")

    def _load_secondary_data(self, *, symbol: str) -> None:
        if bool(getattr(self, "_shutdown_requested", False)):
            return
        self._pending_secondary_reload = False
        if self._secondary_chart_kind() == "volatility":
            self._retire_loader("_secondary_loader")
        else:
            self._retire_loader("_secondary_volatility_loader")
        if self._secondary_loader is not None or self._secondary_volatility_loader is not None:
            self._pending_secondary_reload = True
            self._schedule_pending_reload_if_ready()
            return
        secondary_period = self._secondary_period_combo.currentText().strip()
        requested_limit = max(50, self._limit_spin.value())
        secondary_symbol = symbol.strip().upper() or self._selected_secondary_symbol()
        self._secondary_request_id += 1
        self._active_secondary_request_id = self._secondary_request_id
        self._active_secondary_request_key = self._current_secondary_request_key(symbol=secondary_symbol)
        self._preview_cached_secondary_payload(self._active_secondary_request_key)
        if self._secondary_chart_kind() == "volatility":
            currency = self._current_volatility_currency()
            if currency is None:
                self._secondary_chart_kind_mode = "kline"
                self._update_secondary_controls_state()
                self._load_secondary_data(symbol=secondary_symbol)
                return
            self._secondary_volatility_loader = SecondaryVolatilityDataLoader(
                request_id=self._secondary_request_id,
                currency=currency,
                period=secondary_period,
                limit=requested_limit,
                average_kline=self._secondary_average_kline_enabled(),
                local_only=self._preview_mode,
            )
            self._secondary_volatility_loader.loaded.connect(self._on_secondary_data_loaded)
            self._secondary_volatility_loader.failed.connect(self._on_secondary_data_failed)
            self._secondary_volatility_loader.finished.connect(self._on_secondary_volatility_loader_finished)
            self._secondary_volatility_loader.start()
            return
        self._secondary_loader = KlineDataLoader(
            request_id=self._secondary_request_id,
            symbol=secondary_symbol,
            period=secondary_period,
            limit=requested_limit,
            local_only=self._preview_mode or self._prefer_local_checkbox.isChecked(),
            average_kline=self._secondary_average_kline_enabled(),
            workspace_entry={},
            enable_alerts=False,
            enable_shape_signals=False,
        )
        self._secondary_loader.loaded.connect(self._on_secondary_data_loaded)
        self._secondary_loader.failed.connect(self._on_secondary_data_failed)
        self._secondary_loader.finished.connect(self._on_secondary_loader_finished)
        self._secondary_loader.start()

    def _load_tertiary_data(self) -> None:
        if bool(getattr(self, "_shutdown_requested", False)):
            return
        if self._tertiary_loader is not None and self._tertiary_loader.isRunning():
            self._pending_tertiary_reload = True
            self._schedule_pending_reload_if_ready()
            return
        request_key = self._current_tertiary_request_key()
        if request_key is None:
            return
        self._tertiary_request_id += 1
        self._active_tertiary_request_id = self._tertiary_request_id
        self._active_tertiary_request_key = request_key
        cached_payload = self._tertiary_payload_cache.get(request_key)
        if cached_payload is not None and self._page_ready:
            self._tertiary_pending_payload = cached_payload
            self._loaded_tertiary_request_key = request_key
            self._render_tertiary_chart(cached_payload)
        if self._all_charts_volatility_enabled():
            self._tertiary_loader = SecondaryVolatilityDataLoader(
                request_id=self._tertiary_request_id,
                currency=self._current_volatility_currency() or "",
                period=self._tertiary_period_combo.currentText().strip(),
                limit=max(50, self._limit_spin.value()),
                average_kline=self._secondary_average_kline_enabled(),
                local_only=self._preview_mode or self._prefer_local_checkbox.isChecked(),
            )
        else:
            self._tertiary_loader = KlineDataLoader(
                request_id=self._tertiary_request_id,
                symbol=self._selected_tertiary_symbol(),
                period=self._tertiary_period_combo.currentText().strip(),
                limit=max(50, self._limit_spin.value()),
                local_only=self._preview_mode or self._prefer_local_checkbox.isChecked(),
                average_kline=self._secondary_average_kline_enabled(),
                workspace_entry={},
                enable_alerts=False,
                enable_shape_signals=False,
            )
        self._tertiary_loader.loaded.connect(self._on_tertiary_data_loaded)
        self._tertiary_loader.failed.connect(self._on_tertiary_data_failed)
        self._tertiary_loader.finished.connect(self._on_tertiary_loader_finished)
        self._tertiary_loader.start()

    def _load_quaternary_data(self) -> None:
        if bool(getattr(self, "_shutdown_requested", False)):
            return
        if self._quaternary_loader is not None and self._quaternary_loader.isRunning():
            self._pending_quaternary_reload = True
            self._schedule_pending_reload_if_ready()
            return
        request_key = self._current_quaternary_request_key()
        if request_key is None:
            return
        self._quaternary_request_id += 1
        self._active_quaternary_request_id = self._quaternary_request_id
        self._active_quaternary_request_key = request_key
        cached_payload = self._quaternary_payload_cache.get(request_key)
        if cached_payload is not None and self._page_ready:
            self._quaternary_pending_payload = cached_payload
            self._loaded_quaternary_request_key = request_key
            self._render_quaternary_chart(cached_payload)
        if self._all_charts_volatility_enabled():
            self._quaternary_loader = SecondaryVolatilityDataLoader(
                request_id=self._quaternary_request_id,
                currency=self._current_volatility_currency() or "",
                period=self._quaternary_period_combo.currentText().strip(),
                limit=max(50, self._limit_spin.value()),
                average_kline=self._secondary_average_kline_enabled(),
                local_only=self._preview_mode or self._prefer_local_checkbox.isChecked(),
            )
        else:
            self._quaternary_loader = KlineDataLoader(
                request_id=self._quaternary_request_id,
                symbol=self._selected_quaternary_symbol(),
                period=self._quaternary_period_combo.currentText().strip(),
                limit=max(50, self._limit_spin.value()),
                local_only=self._preview_mode or self._prefer_local_checkbox.isChecked(),
                average_kline=self._secondary_average_kline_enabled(),
                workspace_entry={},
                enable_alerts=False,
                enable_shape_signals=False,
            )
        self._quaternary_loader.loaded.connect(self._on_quaternary_data_loaded)
        self._quaternary_loader.failed.connect(self._on_quaternary_data_failed)
        self._quaternary_loader.finished.connect(self._on_quaternary_loader_finished)
        self._quaternary_loader.start()

    @Slot()
    def _on_secondary_loader_finished(self) -> None:
        self._release_finished_loader("_secondary_loader")
        self._schedule_pending_reload_if_ready()

    @Slot()
    def _on_tertiary_loader_finished(self) -> None:
        self._release_finished_loader("_tertiary_loader")
        self._schedule_pending_reload_if_ready()

    @Slot()
    def _on_quaternary_loader_finished(self) -> None:
        self._release_finished_loader("_quaternary_loader")
        self._schedule_pending_reload_if_ready()

    @Slot()
    def _on_secondary_volatility_loader_finished(self) -> None:
        self._release_finished_loader("_secondary_volatility_loader")
        self._schedule_pending_reload_if_ready()

    @Slot()
    def _on_symbol_confirmed(self, *, refresh_linked_charts: bool = False) -> None:
        reload_secondary_as_kline = False
        if self._secondary_chart_kind() == "volatility":
            selected_symbol = self._selected_symbol()
            self._retire_loader("_secondary_volatility_loader")
            if isinstance(self._loader, SecondaryVolatilityDataLoader):
                self._retire_loader("_loader")
            self._secondary_chart_kind_mode = "kline"
            self._active_volatility_currency = None
            self._secondary_pending_payload = None
            self._loaded_secondary_request_key = None
            reload_secondary_as_kline = bool(
                self._secondary_chart_check.isChecked()
                and self._use_native_chart
                and not refresh_linked_charts
            )
            if self._symbol_link_all_check is not None and self._symbol_link_all_check.isChecked():
                linked_combos = (
                    self._secondary_symbol_combo,
                    self._tertiary_symbol_combo,
                    self._quaternary_symbol_combo,
                )
                for combo in linked_combos:
                    combo.blockSignals(True)
                try:
                    for combo in linked_combos:
                        combo.setCurrentText(selected_symbol)
                finally:
                    for combo in linked_combos:
                        combo.blockSignals(False)
            self._refresh_secondary_chart_kind_button()
            self._refresh_secondary_sync_period_button()
            self._update_secondary_controls_state()
            self._apply_secondary_chart_visibility()
        self._refresh_symbol_tab_selection()
        self._update_secondary_controls_state()
        self._reload_workspace_view()
        self._refresh_rr_trade_hint()
        self._sync_account_drawer_context()
        self._prepare_chart_for_symbol_reload("primary")
        self._load_data(refresh_linked_charts=refresh_linked_charts)
        if reload_secondary_as_kline:
            self._prepare_chart_for_symbol_reload("secondary")
            self._load_secondary_data(symbol=self._selected_secondary_symbol())

    @Slot(str)
    def _on_secondary_symbol_changed(self, _value: str) -> None:
        self._refresh_symbol_tab_selection()
        if (
            not self._secondary_chart_check.isChecked()
            or not self._use_native_chart
            or self._secondary_chart_kind() != "kline"
        ):
            return
        self._prepare_chart_for_symbol_reload("secondary")
        self._load_secondary_data(symbol=self._selected_secondary_symbol())

    @Slot(str)
    def _on_tertiary_symbol_changed(self, _value: str) -> None:
        self._refresh_symbol_tab_selection()
        if not self._triple_chart_enabled() or not self._use_native_chart:
            return
        self._prepare_chart_for_symbol_reload("tertiary")
        self._load_tertiary_data()

    @Slot(str)
    def _on_tertiary_period_changed(self, _value: str) -> None:
        if self._active_chart_target == "tertiary":
            self._sync_primary_period_buttons()
        if self._triple_chart_enabled():
            self._on_tertiary_symbol_changed(self._selected_tertiary_symbol())

    @Slot(str)
    def _on_quaternary_symbol_changed(self, _value: str) -> None:
        self._refresh_symbol_tab_selection()
        if not self._quad_chart_enabled() or not self._use_native_chart:
            return
        self._prepare_chart_for_symbol_reload("quaternary")
        self._load_quaternary_data()

    @Slot(str)
    def _on_quaternary_period_changed(self, _value: str) -> None:
        if self._active_chart_target == "quaternary":
            self._sync_primary_period_buttons()
        if self._quad_chart_enabled():
            self._on_quaternary_symbol_changed(self._selected_quaternary_symbol())

    def _show_account_drawer(self, tab_name: str) -> None:
        if self._account_drawer is None or self._chart_account_splitter is None:
            return
        target_tab = "orders" if tab_name == "orders" else "positions"
        current_tab = "orders" if self._account_drawer._tabs.currentIndex() == 0 else "positions"
        if not self._account_drawer.isHidden() and current_tab == target_tab:
            self._collapse_account_drawer()
            return
        # Swap in the current cached account snapshot while hidden.  Rendering
        # the wide data grid inside this click handler delays the drawer show.
        self._sync_account_drawer_context(refresh_if_visible=False)
        self._account_drawer.show_tab(target_tab)
        self._account_drawer.show()
        total_height = max(self._chart_account_splitter.size().height(), 1)
        drawer_height = max(int(total_height * 0.28), 180)
        chart_height = max(total_height - drawer_height, 240)
        self._chart_account_splitter.setSizes([chart_height, drawer_height])
        # First return to the event loop so the drawer and splitter can paint.
        # The cached active tab then renders, while the latest account check
        # continues in the existing background realtime store.
        self._account_drawer._schedule_active_table_refresh()
        self._account_drawer.schedule_refresh_data()

    def _toggle_account_drawer(self) -> None:
        if self._account_drawer is None:
            return
        if not self._account_drawer.isHidden():
            self._collapse_account_drawer()
            return
        self._show_account_drawer("orders")

    def _collapse_account_drawer(self) -> None:
        if self._account_drawer is None or self._chart_account_splitter is None:
            return
        total_height = max(self._chart_account_splitter.size().height(), 1)
        self._account_drawer.hide()
        self._chart_account_splitter.setSizes([total_height, 0])

    @Slot(str)
    def _on_period_changed(self, _value: str) -> None:
        self._refresh_history_trade_availability()
        self._sync_primary_period_buttons()
        self._refresh_timer.setInterval(self._auto_refresh_interval_ms(_value))
        self._reload_workspace_view()
        self._sync_chart_options()
        if self._auto_refresh_btn.isChecked():
            self._load_data()

    @Slot(bool)
    def _on_secondary_chart_toggled(self, enabled: bool) -> None:
        if not enabled and self._quad_chart_enabled():
            self._quaternary_chart_check.blockSignals(True)
            self._quaternary_chart_check.setChecked(False)
            self._quaternary_chart_check.blockSignals(False)
        if not enabled and self._triple_chart_enabled():
            self._tertiary_chart_check.blockSignals(True)
            self._tertiary_chart_check.setChecked(False)
            self._tertiary_chart_check.blockSignals(False)
        if enabled and not self._triple_chart_enabled():
            self._secondary_layout_mode_value = "horizontal"
        primary_average_secondary_normal_was_enabled = self._primary_average_secondary_normal_check.isChecked()
        self._apply_secondary_chart_visibility()
        self._apply_chart_mode_period_defaults(dual_enabled=enabled)
        if not enabled:
            self._set_active_chart_target("primary")
        else:
            self._refresh_chart_selection_visuals()
            self._sync_primary_period_buttons()
        self._refresh_chart_mode_cycle_button()
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
            if primary_average_secondary_normal_was_enabled:
                self._load_data()

    @Slot(bool)
    def _on_tertiary_chart_toggled(self, enabled: bool) -> None:
        if not enabled and self._quad_chart_enabled():
            self._quaternary_chart_check.blockSignals(True)
            self._quaternary_chart_check.setChecked(False)
            self._quaternary_chart_check.blockSignals(False)
        if enabled:
            if self._secondary_chart_kind() != "kline":
                self._secondary_chart_kind_mode = "kline"
            if not self._secondary_chart_check.isChecked():
                self._secondary_chart_check.blockSignals(True)
                self._secondary_chart_check.setChecked(True)
                self._secondary_chart_check.blockSignals(False)
            self._set_active_chart_target("tertiary")
        elif self._active_chart_target == "tertiary":
            self._set_active_chart_target("primary")
        self._apply_secondary_chart_visibility()
        if enabled:
            self._load_data()

    @Slot(bool)
    def _on_quaternary_chart_toggled(self, enabled: bool) -> None:
        if enabled:
            current_symbol = self._selected_symbol()
            if current_symbol:
                self._quaternary_symbol_combo.blockSignals(True)
                self._quaternary_symbol_combo.setCurrentText(current_symbol)
                self._quaternary_symbol_combo.blockSignals(False)
            if not self._triple_chart_enabled():
                self._tertiary_chart_check.blockSignals(True)
                self._tertiary_chart_check.setChecked(True)
                self._tertiary_chart_check.blockSignals(False)
            if not self._secondary_chart_check.isChecked():
                self._secondary_chart_check.blockSignals(True)
                self._secondary_chart_check.setChecked(True)
                self._secondary_chart_check.blockSignals(False)
            # 保留用户当前选择的副图类型；如果此前选的是 BTC/ETH DVOL，
            # 四图布局应把波动率作为其中一张图，而不是强制改回普通 K 线。
            self._apply_quad_chart_period_defaults()
            self._set_active_chart_target("quaternary")
        elif self._active_chart_target == "quaternary":
            self._set_active_chart_target("primary")
        self._apply_secondary_chart_visibility()
        if enabled:
            self._load_data()

    @Slot()
    def _on_secondary_layout_changed(self) -> None:
        self._on_secondary_layout_cycle_clicked()

    @Slot()
    def _on_secondary_chart_kind_changed(self) -> None:
        self._on_secondary_chart_kind_cycle_clicked()

    @Slot(str)
    def _on_secondary_period_changed(self, _value: str) -> None:
        if self._secondary_chart_kind() == "volatility" and _value.strip().upper() not in {"1H", "4H", "1D", "1W"}:
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
            if bool(getattr(self, "_shutdown_requested", False)):
                return
            if request_id != self._active_request_id:
                return
            if self._active_primary_request_key != self._current_primary_request_key():
                self._set_status("主图结果已过期，正在刷新最新选择...")
                return
            payload = self._mark_preview_signal(payload)
            self._pending_payload = payload
            self._loaded_primary_request_key = self._active_primary_request_key
            self._remember_payload_cache(self._primary_payload_cache, self._loaded_primary_request_key, payload)
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
            if bool(payload.stats.get("weekly_cache_generated", False)):
                source_bar = str(payload.stats.get("weekly_cache_source_bar", "") or "")
                self._primary_chart_status_text += f" | 周线本地生成（来源{source_bar or '本地'}）"
            if sync_text:
                self._primary_chart_status_text = f"{self._primary_chart_status_text}{sync_text}"
            combined_status_parts = [f"主图：{self._primary_chart_status_text}"]
            if self._secondary_chart_status_text:
                combined_status_parts.append(f"副图：{self._secondary_chart_status_text}")
            self._set_status(" | ".join(combined_status_parts))
            if self._page_ready and self._page_active:
                self._render_loaded_payload(payload)
                if self._secondary_chart_check.isChecked() and self._secondary_pending_payload is not None and self._use_native_chart:
                    self._sync_secondary_chart_range_from_primary()
            if self._page_active:
                self._apply_alert_snapshot(payload.alert_snapshot)
                if not self._preview_mode and not self._all_charts_volatility_enabled():
                    self._subscribe_realtime_candle()
                self._update_refresh_hint()
        except Exception as exc:
            self._set_status(f"数据处理异常：{exc}")

    def _mark_preview_signal(self, payload: KlineChartPayload) -> KlineChartPayload:
        target_ts = int(getattr(self, "_preview_signal_ts", 0) or 0)
        if not self._preview_mode or target_ts <= 0:
            return payload
        target_seconds = target_ts // 1000
        markers: list[dict[str, Any]] = []
        for marker in payload.signal_markers:
            item = dict(marker)
            marker_seconds = int(item.get("time", 0) or 0)
            if marker_seconds == target_seconds:
                item["color"] = "#f59e0b"
                item["label"] = f"★ {item.get('label', '')}".strip()
                item["text"] = f"★ {self._preview_signal_text or item.get('text', '')}".strip()
            markers.append(item)
        return replace(payload, signal_markers=markers)

    @Slot(object)
    def _apply_realtime_candle(self, candle: object) -> None:
        if not isinstance(candle, Candle) or self._pending_payload is None:
            return
        payload = _merge_realtime_candle_payload(self._pending_payload, candle)
        self._pending_payload = payload
        self._remember_payload_cache(self._primary_payload_cache, self._loaded_primary_request_key, payload)
        if self._page_active and not bool(getattr(self, "_shutdown_requested", False)):
            self._apply_realtime_candle_to_chart(payload)

    def _apply_realtime_candle_to_chart(self, payload: KlineChartPayload) -> None:
        if self._use_native_chart:
            self._apply_realtime_candle_to_native_chart(payload)
            return
        if not payload.candles:
            return
        latest = payload.candles[-1]
        ema9 = payload.ema_9[-1] if payload.ema_9 else None
        ema21 = payload.ema_21[-1] if payload.ema_21 else None
        display_candles = self._display_payload_for_chart(payload, is_secondary=False).candles
        ema21_fast_points = _ema_chart_points(display_candles, 21)
        ema55_points = _ema_chart_points(display_candles, 55)
        realtime_payload = {
            "candle": latest,
            "ema9": ema9,
            "ema21": ema21,
            "ema21Fast": ema21_fast_points[-1] if ema21_fast_points else None,
            "ema55": ema55_points[-1] if ema55_points else None,
            "atr10": (
                {"time": int(display_candles[-1]["time"]), "value": _atr_chart_values(display_candles, 10)[-1]}
                if self._atr10_check.isChecked() and display_candles
                else None
            ),
        }
        self._run_js(
            f"window.updateRealtimeCandle({json.dumps(realtime_payload)});"
        )

    def _apply_realtime_candle_to_native_chart(self, payload: KlineChartPayload) -> None:
        if self._native_chart is None or not payload.candles or QCandlestickSeries is None or QCandlestickSet is None:
            return
        display_payload = self._display_payload_for_chart(payload, is_secondary=False)
        if not display_payload.candles:
            return
        candle_values = display_payload.candles[-1]
        display_times = _build_display_times_ms(display_payload.candles, self._period_combo.currentText().strip())
        if not display_times:
            return
        candle_series = next((item for item in self._native_chart.series() if isinstance(item, QCandlestickSeries)), None)
        if candle_series is None:
            return
        sets = candle_series.sets()
        timestamp = float(display_times[-1])
        if len(sets) == len(display_payload.candles):
            candle_set = sets[-1]
            candle_set.setOpen(float(candle_values["open"]))
            candle_set.setHigh(float(candle_values["high"]))
            candle_set.setLow(float(candle_values["low"]))
            candle_set.setClose(float(candle_values["close"]))
            candle_set.setTimestamp(timestamp)
        elif len(sets) + 1 == len(display_payload.candles):
            candle_series.append(QCandlestickSet(float(candle_values["open"]), float(candle_values["high"]), float(candle_values["low"]), float(candle_values["close"]), timestamp))
        else:
            return
        best_parameter_indicators = bool(
            getattr(self, "_best_parameter_indicators_check", None)
            and self._best_parameter_indicators_check.isChecked()
        )
        if best_parameter_indicators:
            closes = [float(item["close"]) for item in display_payload.candles]
            latest_points = {
                label: {"value": (values[-1] if values else None)}
                for label, ma_type, line_period, _color in _best_parameter_indicator_specs(
                    self._selected_symbol(),
                    self._period_combo.currentText().strip(),
                    direction_filter=self._history_trade_direction_filter(),
                )
                for values in [
                    _to_ema(closes, line_period) if ma_type == "ema" else _to_sma(closes, line_period)
                ]
            }
        else:
            latest_points = {
                "EMA 15": display_payload.ema_9[-1] if display_payload.ema_9 else None,
                "SMA 50": display_payload.ema_21[-1] if display_payload.ema_21 else None,
            }
        for series in self._native_chart.series():
            if not isinstance(series, QLineSeries):
                continue
            point = latest_points.get(series.name())
            if not isinstance(point, dict):
                continue
            value = float(point.get("value") or 0.0)
            if series.count() == len(display_payload.candles):
                series.replace(series.count() - 1, QPointF(timestamp, value))
            elif series.count() + 1 == len(display_payload.candles):
                series.append(timestamp, value)
        if self._native_chart_view is not None:
            # Reuse the existing candle stream for the right-axis latest-price
            # marker; no extra ticker request or high-frequency timer is added.
            if isinstance(self._native_chart_view, InteractiveKlineChartView):
                if self._atr10_check.isChecked():
                    self._native_chart_view._atr_values = _atr_chart_values(display_payload.candles, 10)
                self._native_chart_view.set_latest_price(float(candle_values["close"]))
            self._native_chart_view.update()

    def _subscribe_realtime_candle(self) -> None:
        if self._pending_payload is None:
            return
        symbol = self._selected_symbol()
        period = self._period_combo.currentText().strip()
        environment = str(getattr(self._runtime, "environment", "demo") or "demo")
        key = CandleStreamKey(symbol, period, environment)
        if key == self._realtime_candle_key:
            return
        if self._realtime_candle_unsubscribe is not None:
            try:
                self._realtime_candle_unsubscribe()
            except Exception:
                pass
        self._realtime_candle_key = key
        self._realtime_candle_unsubscribe = self._market_client.watch_candle(
            key,
            lambda candle, _confirmed: self._realtime_candle_received.emit(candle),
        )

    @Slot(int, KlineChartPayload)
    def _on_secondary_data_loaded(self, request_id: int, payload: KlineChartPayload) -> None:
        if bool(getattr(self, "_shutdown_requested", False)):
            return
        if request_id != self._active_secondary_request_id:
            return
        if self._active_secondary_request_key != self._current_secondary_request_key():
            self._set_status("副图结果已过期，正在刷新最新选择...")
            return
        self._secondary_pending_payload = payload
        self._loaded_secondary_request_key = self._active_secondary_request_key
        self._remember_payload_cache(self._secondary_payload_cache, self._loaded_secondary_request_key, payload)
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
        else:
            loaded = int(payload.stats.get("returned", 0) or 0)
            source = _source_status_text(str(payload.stats.get("source", "")))
            local_count = int(payload.stats.get("local_count", 0) or 0)
            api_count = int(payload.stats.get("remote_added_count", 0) or 0)
            self._secondary_chart_status_text = (
                f"{loaded}条 | {source} | 本地{local_count} | 新增{api_count}"
            )
            combined_status_parts = []
            if self._primary_chart_status_text:
                combined_status_parts.append(f"主图：{self._primary_chart_status_text}")
            combined_status_parts.append(f"副图：{self._secondary_chart_status_text}")
            self._set_status(" | ".join(combined_status_parts))
        if self._page_active and self._secondary_chart_check.isChecked() and self._use_native_chart:
            self._render_secondary_chart(payload)
            self._sync_secondary_chart_range_from_primary()
        if (
            self._secondary_chart_check.isChecked()
            and self._period_combo.currentText().strip().upper() == "4H"
            and self._secondary_period_combo.currentText().strip().upper() == "1D"
            and self._pending_payload is not None
            and self._page_ready
            and self._page_active
            and self._secondary_pending_payload is not None
            and self._use_native_chart
        ):
            self._sync_chart_options()

    @Slot(int, KlineChartPayload)
    def _on_tertiary_data_loaded(self, request_id: int, payload: KlineChartPayload) -> None:
        if bool(getattr(self, "_shutdown_requested", False)):
            return
        if request_id != self._active_tertiary_request_id:
            return
        if self._active_tertiary_request_key != self._current_tertiary_request_key():
            self._set_status("第三图结果已过期，正在刷新最新选择...")
            return
        self._tertiary_pending_payload = payload
        self._loaded_tertiary_request_key = self._active_tertiary_request_key
        self._remember_payload_cache(self._tertiary_payload_cache, self._loaded_tertiary_request_key, payload)
        if self._page_active and self._triple_chart_enabled() and self._use_native_chart:
            self._render_tertiary_chart(payload)
            self._sync_secondary_chart_range_from_primary()

    @Slot(int, KlineChartPayload)
    def _on_quaternary_data_loaded(self, request_id: int, payload: KlineChartPayload) -> None:
        if bool(getattr(self, "_shutdown_requested", False)):
            return
        if request_id != self._active_quaternary_request_id:
            return
        if self._active_quaternary_request_key != self._current_quaternary_request_key():
            self._set_status("第四图结果已过期，正在刷新最新选择...")
            return
        self._quaternary_pending_payload = payload
        self._loaded_quaternary_request_key = self._active_quaternary_request_key
        self._remember_payload_cache(self._quaternary_payload_cache, self._loaded_quaternary_request_key, payload)
        if self._page_active and self._quad_chart_enabled() and self._use_native_chart:
            self._render_quaternary_chart(payload)
            self._sync_secondary_chart_range_from_primary()

    @Slot(int, str)
    def _on_data_failed(self, request_id: int, message: str) -> None:
        if bool(getattr(self, "_shutdown_requested", False)):
            return
        if request_id != self._active_request_id:
            return
        self._set_status(f"加载失败：{message}")
        if self._page_active and self._use_native_chart and self._native_chart is not None:
            self._native_chart.setTitle(f"加载失败：{message}")
        elif self._page_active:
            self._run_js("window.handleChartWarning(%s);" % json.dumps(message))

    @Slot(int, str)
    def _on_secondary_data_failed(self, request_id: int, message: str) -> None:
        if bool(getattr(self, "_shutdown_requested", False)):
            return
        if request_id != self._active_secondary_request_id:
            return
        if self._page_active and self._secondary_native_chart is not None:
            self._secondary_native_chart.setTitle(f"副图加载失败：{message}")

    @Slot(int, str)
    def _on_tertiary_data_failed(self, request_id: int, message: str) -> None:
        if bool(getattr(self, "_shutdown_requested", False)):
            return
        if request_id != self._active_tertiary_request_id:
            return
        if self._page_active and self._tertiary_native_chart is not None:
            self._tertiary_native_chart.setTitle(f"第三图加载失败：{message}")

    @Slot(int, str)
    def _on_quaternary_data_failed(self, request_id: int, message: str) -> None:
        if bool(getattr(self, "_shutdown_requested", False)):
            return
        if request_id != self._active_quaternary_request_id:
            return
        if self._page_active and self._quaternary_native_chart is not None:
            self._quaternary_native_chart.setTitle(f"第四图加载失败：{message}")

    @Slot(object)
    def _on_primary_hover_time_changed(self, candle_time: object) -> None:
        self._sync_hover_time_from(source="primary", candle_time=candle_time)

    @Slot(object)
    def _on_secondary_hover_time_changed(self, candle_time: object) -> None:
        self._sync_hover_time_from(source="secondary", candle_time=candle_time)

    @Slot(object)
    def _on_tertiary_hover_time_changed(self, candle_time: object) -> None:
        self._sync_hover_time_from(source="tertiary", candle_time=candle_time)

    @Slot(object)
    def _on_quaternary_hover_time_changed(self, candle_time: object) -> None:
        self._sync_hover_time_from(source="quaternary", candle_time=candle_time)

    @Slot(float, float)
    def _on_primary_x_range_changed(self, start_x: float, end_x: float) -> None:
        self._sync_chart_range_from(source="primary", start_x=start_x, end_x=end_x)

    @Slot(float, float)
    def _on_secondary_x_range_changed(self, start_x: float, end_x: float) -> None:
        self._sync_chart_range_from(source="secondary", start_x=start_x, end_x=end_x)

    @Slot(float, float)
    def _on_tertiary_x_range_changed(self, start_x: float, end_x: float) -> None:
        self._sync_chart_range_from(source="tertiary", start_x=start_x, end_x=end_x)

    @Slot(float, float)
    def _on_quaternary_x_range_changed(self, start_x: float, end_x: float) -> None:
        self._sync_chart_range_from(source="quaternary", start_x=start_x, end_x=end_x)

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
        self._apply_chart_view_range()

    def _chart_view_range_is_full(self) -> bool:
        return self._chart_view_range_mode == "full"

    def _set_chart_view_range_mode(self, mode: str) -> None:
        normalized = str(mode).strip().lower()
        self._chart_view_range_mode = "full" if normalized == "full" else "recent"
        self._refresh_chart_view_range_button()

    def _refresh_chart_view_range_button(self) -> None:
        if self._chart_range_mode_btn is None:
            return
        self._chart_range_mode_btn.setText("最近视图" if self._chart_view_range_is_full() else "全量视图")
        self._chart_range_mode_btn.setToolTip("切换图表显示范围（近期视图/全量视图）")

    @Slot()
    def _toggle_chart_view_range_mode(self) -> None:
        self._set_chart_view_range_mode("full" if self._chart_view_range_mode == "recent" else "recent")
        self._apply_chart_view_range()

    def _apply_chart_view_range(self) -> None:
        if self._use_native_chart:
            if self._chart_view_range_is_full():
                if isinstance(self._native_chart_view, InteractiveKlineChartView):
                    self._native_chart_view.set_full_view_range()
                if (
                    self._secondary_chart_check.isChecked()
                    and isinstance(self._secondary_native_chart_view, InteractiveKlineChartView)
                ):
                    self._sync_secondary_chart_range_from_primary()
            else:
                if not self._apply_recent_view_range_for_linked_charts():
                    if isinstance(self._native_chart_view, InteractiveKlineChartView):
                        self._native_chart_view.set_recent_view_range()
                    if (
                        self._secondary_chart_check.isChecked()
                        and isinstance(self._secondary_native_chart_view, InteractiveKlineChartView)
                    ):
                        self._sync_secondary_chart_range_from_primary()
            return
        self._run_js(
            f"if (typeof window.applyChartViewMode === 'function') {{ window.applyChartViewMode({json.dumps(self._chart_view_range_mode)}); }}"
        )

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
        if (
            self._tertiary_pending_payload is not None
            and self._triple_chart_enabled()
            and self._loaded_tertiary_request_key == self._current_tertiary_request_key()
        ):
            self._render_tertiary_chart(self._tertiary_pending_payload)
        if (
            self._quaternary_pending_payload is not None
            and self._quad_chart_enabled()
            and self._loaded_quaternary_request_key == self._current_quaternary_request_key()
        ):
            self._render_quaternary_chart(self._quaternary_pending_payload)

    def _reverse_kline_enabled_for_chart(self, *, is_secondary: bool) -> bool:
        if not bool(self._reverse_kline_check.isChecked()):
            return False
        if not is_secondary:
            return True
        return self._secondary_chart_kind() == "kline"

    def _display_payload_for_chart(self, payload: KlineChartPayload, *, is_secondary: bool) -> KlineChartPayload:
        if not self._reverse_kline_enabled_for_chart(is_secondary=is_secondary):
            return payload
        return _reverse_kline_chart_payload(payload)

    def _display_price_from_logical(self, payload: KlineChartPayload, value: float, *, is_secondary: bool) -> float:
        if not self._reverse_kline_enabled_for_chart(is_secondary=is_secondary):
            return float(value)
        return _reverse_kline_price(float(value), _reverse_kline_anchor_price(payload))

    def _logical_price_from_display(self, payload: KlineChartPayload, value: float, *, is_secondary: bool) -> float:
        if not self._reverse_kline_enabled_for_chart(is_secondary=is_secondary):
            return float(value)
        return _reverse_kline_price(float(value), _reverse_kline_anchor_price(payload))

    def _render_to_chart(self, payload: KlineChartPayload) -> None:
        if self._use_native_chart:
            self._render_to_native_chart(payload)
            return
        display_payload = self._display_payload_for_chart(payload, is_secondary=False)
        period = self._period_combo.currentText().strip()
        signal_markers = self._filter_replay_signal_markers_for_chart(
            display_payload.signal_markers,
            period=period,
            is_secondary=False,
        )
        trend_payload = display_payload.trend_indicator if _supports_trend_indicator(period) else []
        if not isinstance(trend_payload, list):
            trend_payload = []
        payload_map: dict[str, Any] = {
            "candles": display_payload.candles,
            "ema": {
                "ema9": display_payload.ema_9,
                "ema21": display_payload.ema_21,
                "ema21Fast": _ema_chart_points(display_payload.candles, 21),
                "ema55": _ema_chart_points(display_payload.candles, 55),
            },
            "show": {
                "ema9": self._ema9.isChecked(),
                "ema21": self._ema21.isChecked(),
                "emaPair": self._ema21_55_check.isChecked() and not self._best_parameter_indicators_check.isChecked(),
                "atr10": self._atr10_check.isChecked(),
            },
            "atr10": [
                {"time": int(display_payload.candles[index]["time"]), "value": value}
                for index, value in enumerate(_atr_chart_values(display_payload.candles, 10))
                if value is not None
            ] if self._atr10_check.isChecked() else [],
            "period": period,
            "trend": trend_payload,
            "signals": signal_markers,
            "boxes": self._visible_box_overlays(display_payload),
            "channels": self._visible_channel_overlays(display_payload),
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
        elif normalized_period == "1W":
            signal_check = getattr(self, "_show_1w_shape_signal_check", None)
        else:
            return []
        if signal_check is None or not signal_check.isChecked():
            return []

        metric = self._shape_signal_size_metric_value()
        rank_key = "core_range_rank" if metric == "range" else "core_body_rank"
        display_rank_key = "core_amplitude_rank"
        metric_filtered: list[dict[str, Any]] = []
        for marker in signal_markers:
            if not isinstance(marker, dict):
                continue
            pattern_id = str(marker.get("pattern_id", "") or "").strip().lower()
            if pattern_id in {"long_upper_shadow", "long_lower_shadow"}:
                shadow_rank_value = marker.get("shadow_rank")
                try:
                    shadow_rank = int(shadow_rank_value)
                except (TypeError, ValueError):
                    continue
                if shadow_rank > 4:
                    continue
                metric_filtered.append(dict(marker))
                continue
            rank_value = marker.get(rank_key)
            # 订阅监控在当前指标没有排名时，会使用另一种有效的核心K排名
            # （例如实体排名为空、区间排名为“核心K前3”）。图表不能因此
            # 把历史中已经确认的信号过滤掉，否则会出现“历史有、K线无”。
            if rank_value is None:
                rank_value = marker.get(display_rank_key)
            try:
                rank = int(rank_value)
            except (TypeError, ValueError):
                continue
            if rank > 4:
                continue
            item = dict(marker)
            item[display_rank_key] = rank
            text = str(item.get("text", ""))
            if "核心K前" in text:
                prefix, _, rest = text.partition("核心K前")
                suffix = rest
                for pos, char in enumerate(rest):
                    if not char.isdigit():
                        suffix = rest[pos:]
                        break
                else:
                    suffix = ""
                item["text"] = f"{prefix}核心K前{rank}{suffix}"
            metric_filtered.append(item)
        signal_markers = metric_filtered

        ma_touch_check = getattr(self, "_shape_signal_ma_touch_check", None)
        require_core_ma_touch = bool(ma_touch_check is not None and ma_touch_check.isChecked())
        if require_core_ma_touch:
            signal_markers = [
                marker
                for marker in signal_markers
                if isinstance(marker, dict) and bool(marker.get("core_ma_touch", False))
            ]

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
        show_current = bool(getattr(self, "_auto_box_check", None) and self._auto_box_check.isChecked())
        show_history = bool(getattr(self, "_history_box_check", None) and self._history_box_check.isChecked())
        if not show_current and not show_history:
            return []
        visible: list[dict[str, Any]] = []
        for item in payload.box_overlays:
            if not isinstance(item, dict):
                continue
            mode = str(item.get("mode", "history")).strip().lower()
            if mode == "history" and not show_history:
                continue
            if mode != "history" and not show_current:
                continue
            visible.append(dict(item))
        return visible

    def _visible_channel_overlays(self, payload: KlineChartPayload) -> list[dict[str, Any]]:
        if not bool(getattr(self, "_auto_channel_check", None) and self._auto_channel_check.isChecked()):
            return []
        return [dict(item) for item in payload.channel_overlays if isinstance(item, dict)]

    def _render_loaded_payload(self, payload: KlineChartPayload) -> None:
        if not self._use_native_chart:
            self._render_to_chart(payload)
            self._schedule_account_drawer_prefetch_after_chart_render()
            return
        if self._should_stage_native_bootstrap(payload):
            preview_payload = _slice_chart_payload_tail(payload, _NATIVE_BOOTSTRAP_RENDER_BARS)
            self._render_to_native_chart(preview_payload)
            self._deferred_chart_payload = payload
            self._deferred_chart_request_id = self._active_request_id
            self._deferred_chart_render_timer.start(_NATIVE_BOOTSTRAP_RENDER_DELAY_MS)
            self._schedule_account_drawer_prefetch_after_chart_render()
            return
        if self._deferred_chart_render_timer.isActive():
            self._deferred_chart_render_timer.stop()
        self._deferred_chart_payload = None
        self._deferred_chart_request_id = 0
        self._render_to_native_chart(payload)
        self._native_chart_bootstrap_complete = True
        self._schedule_account_drawer_prefetch_after_chart_render()

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
        tertiary_frame = QFrame()
        tertiary_frame.setStyleSheet(self._chart_frame_style(active=False))
        tertiary_layout = QVBoxLayout(tertiary_frame)
        tertiary_layout.setContentsMargins(0, 0, 0, 0)
        self._tertiary_native_chart, self._tertiary_native_chart_view = self._create_native_chart_view(
            title="正在准备第三图...",
            allow_draw_clicks=False,
        )
        tertiary_layout.addWidget(self._tertiary_native_chart_view, 1)
        self._tertiary_chart_frame = tertiary_frame
        chart_splitter.addWidget(tertiary_frame)
        quaternary_frame = QFrame()
        quaternary_frame.setStyleSheet(self._chart_frame_style(active=False))
        quaternary_layout = QVBoxLayout(quaternary_frame)
        quaternary_layout.setContentsMargins(0, 0, 0, 0)
        self._quaternary_native_chart, self._quaternary_native_chart_view = self._create_native_chart_view(
            title="正在准备第四图...",
            allow_draw_clicks=False,
        )
        quaternary_layout.addWidget(self._quaternary_native_chart_view, 1)
        self._quaternary_chart_frame = quaternary_frame
        chart_splitter.addWidget(quaternary_frame)
        chart_splitter.setStretchFactor(0, 5)
        chart_splitter.setStretchFactor(1, 2)
        chart_splitter.setStretchFactor(2, 2)
        chart_splitter.setStretchFactor(3, 2)

        quad_grid_host = QFrame()
        quad_grid_host.setObjectName("QuadChartGrid")
        quad_grid_layout = QGridLayout(quad_grid_host)
        quad_grid_layout.setContentsMargins(0, 0, 0, 0)
        quad_grid_layout.setSpacing(4)
        self._quad_grid_host = quad_grid_host
        self._quad_grid_layout = quad_grid_layout
        quad_grid_host.hide()

        if isinstance(self._native_chart_view, InteractiveKlineChartView) and isinstance(self._secondary_native_chart_view, InteractiveKlineChartView):
            self._native_chart_view.hoverTimeChanged.connect(self._on_primary_hover_time_changed)
            self._secondary_native_chart_view.hoverTimeChanged.connect(self._on_secondary_hover_time_changed)
            self._native_chart_view.chartActivated.connect(lambda: self._set_active_chart_target("primary"))
            self._secondary_native_chart_view.chartActivated.connect(lambda: self._set_active_chart_target("secondary"))
            if isinstance(self._tertiary_native_chart_view, InteractiveKlineChartView):
                self._tertiary_native_chart_view.hoverTimeChanged.connect(self._on_tertiary_hover_time_changed)
                self._tertiary_native_chart_view.chartActivated.connect(lambda: self._set_active_chart_target("tertiary"))
            if isinstance(self._quaternary_native_chart_view, InteractiveKlineChartView):
                self._quaternary_native_chart_view.hoverTimeChanged.connect(self._on_quaternary_hover_time_changed)
                self._quaternary_native_chart_view.chartActivated.connect(lambda: self._set_active_chart_target("quaternary"))
        elif isinstance(self._native_chart_view, InteractiveKlineChartView):
            self._native_chart_view.chartActivated.connect(lambda: self._set_active_chart_target("primary"))

        chart_layout.addWidget(chart_splitter, 1)
        chart_layout.addWidget(quad_grid_host, 1)
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
                view.chartDoubleClicked.connect(self._on_native_chart_double_clicked)  # type: ignore[attr-defined]
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
        all_charts_volatility = self._all_charts_volatility_enabled()
        display_payload = self._display_payload_for_chart(payload, is_secondary=all_charts_volatility)
        self._render_native_chart_target(
            chart=self._native_chart,
            chart_view=self._native_chart_view,
            payload=display_payload,
            period=self._period_combo.currentText().strip(),
            title_suffix="波动率主图" if all_charts_volatility else "主图",
            include_workspace_lines=not all_charts_volatility,
            is_secondary=all_charts_volatility,
            display_symbol=self._secondary_display_symbol() if all_charts_volatility else None,
            venue_label="DERIBIT" if all_charts_volatility else self._kline_venue_label(self._period_combo.currentText()),
            chart_note_lines=(
                self._secondary_chart_note_lines(payload, period=self._period_combo.currentText().strip())
                if all_charts_volatility
                else None
            ),
            source_payload=payload,
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
        display_payload = self._display_payload_for_chart(payload, is_secondary=True)
        self._render_native_chart_target(
            chart=self._secondary_native_chart,
            chart_view=self._secondary_native_chart_view,
            payload=display_payload,
            period=self._secondary_period_combo.currentText().strip(),
            title_suffix="波动率副图" if self._secondary_chart_kind() == "volatility" else "副图",
            include_workspace_lines=False,
            is_secondary=True,
            display_symbol=self._secondary_display_symbol(),
            venue_label=self._secondary_chart_venue_label(),
            chart_note_lines=self._secondary_chart_note_lines(
                display_payload,
                period=self._secondary_period_combo.currentText().strip(),
            ),
            source_payload=payload,
        )

    def _render_tertiary_chart(self, payload: KlineChartPayload) -> None:
        if (
            self._tertiary_native_chart is None
            or self._tertiary_native_chart_view is None
            or QCandlestickSeries is None
            or QCandlestickSet is None
            or QDateTimeAxis is None
            or QLineSeries is None
            or QValueAxis is None
        ):
            return
        display_payload = self._display_payload_for_chart(payload, is_secondary=True)
        self._render_native_chart_target(
            chart=self._tertiary_native_chart,
            chart_view=self._tertiary_native_chart_view,
            payload=display_payload,
            period=self._tertiary_period_combo.currentText().strip(),
            title_suffix="波动率第三图" if self._all_charts_volatility_enabled() else "第三图",
            include_workspace_lines=False,
            is_secondary=True,
            display_symbol=self._secondary_display_symbol() if self._all_charts_volatility_enabled() else self._selected_tertiary_symbol(),
            venue_label="DERIBIT" if self._all_charts_volatility_enabled() else self._kline_venue_label(self._tertiary_period_combo.currentText()),
            chart_note_lines=(
                self._secondary_chart_note_lines(payload, period=self._tertiary_period_combo.currentText().strip())
                if self._all_charts_volatility_enabled()
                else None
            ),
            source_payload=payload,
        )

    def _render_quaternary_chart(self, payload: KlineChartPayload) -> None:
        if (
            self._quaternary_native_chart is None
            or self._quaternary_native_chart_view is None
            or QCandlestickSeries is None
            or QCandlestickSet is None
            or QDateTimeAxis is None
            or QLineSeries is None
            or QValueAxis is None
        ):
            return
        display_payload = self._display_payload_for_chart(payload, is_secondary=True)
        self._render_native_chart_target(
            chart=self._quaternary_native_chart,
            chart_view=self._quaternary_native_chart_view,
            payload=display_payload,
            period=self._quaternary_period_combo.currentText().strip(),
            title_suffix="波动率第四图" if self._all_charts_volatility_enabled() else "第四图",
            include_workspace_lines=False,
            is_secondary=True,
            display_symbol=self._secondary_display_symbol() if self._all_charts_volatility_enabled() else self._selected_quaternary_symbol(),
            venue_label="DERIBIT" if self._all_charts_volatility_enabled() else self._kline_venue_label(self._quaternary_period_combo.currentText()),
            chart_note_lines=(
                self._secondary_chart_note_lines(payload, period=self._quaternary_period_combo.currentText().strip())
                if self._all_charts_volatility_enabled()
                else None
            ),
            source_payload=payload,
        )

    def _render_native_chart_target(self, **kwargs) -> None:  # noqa: ANN003
        payload = kwargs["payload"]
        with measure_ui_step("kline_full_render", candles=len(payload.candles)):
            self._render_native_chart_target_impl(**kwargs)

    def _render_native_chart_target_impl(
        self,
        *,
        chart: QChart,
        chart_view: QChartView,
        payload: KlineChartPayload,
        period: str,
        title_suffix: str,
        include_workspace_lines: bool,
        is_secondary: bool,
        source_payload: KlineChartPayload | None = None,
        display_symbol: str | None = None,
        venue_label: str = "OKX",
        chart_note_lines: list[str] | None = None,
    ) -> None:
        restore_state = None
        if isinstance(chart_view, InteractiveKlineChartView):
            restore_state = chart_view.capture_view_state()
            chart_view.clear_chart_context()

        chart.removeAllSeries()
        for axis in list(chart.axes()):
            chart.removeAxis(axis)

        candles = payload.candles
        logical_payload = source_payload or payload
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
        candle_series.setBodyOutlineVisible(True)
        candle_series.setCapsVisible(False)
        if hasattr(candle_series, "setBodyWidth"):
            candle_series.setBodyWidth(0.72)

        min_price = min(float(item["low"]) for item in candles)
        max_price = max(float(item["high"]) for item in candles)
        candle_price_span = max_price - min_price
        show_atr = self._atr10_check.isChecked() and not (is_secondary and self._all_charts_volatility_enabled())
        atr_values = _atr_chart_values(candles, 10) if show_atr else []
        overlay_values: list[list[float]] = []
        indicator_series: list[dict[str, Any]] = []
        workspace_lines: list[dict[str, object]] = []
        workspace_rr_items: list[dict[str, object]] = []
        trend_indicators = payload.trend_indicator if _supports_trend_indicator(period) else []
        history_trade_markers: list[dict[str, Any]] = []
        if not is_secondary:
            history_trade_markers = _history_trade_markers_for_candles(
                self._visible_history_trade_markers(),
                payload.candles,
            )
            logical_payload = source_payload or payload
            for marker in history_trade_markers:
                logical_price = float(marker.get("price", 0.0) or 0.0)
                marker["logical_price"] = logical_price
                marker["price"] = self._display_price_from_logical(
                    logical_payload,
                    logical_price,
                    is_secondary=False,
                )

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
            candle_pen.setWidth(
                _candle_body_pen_width(
                    float(item["open"]),
                    float(item["close"]),
                    price_span=candle_price_span,
                )
            )
            candle_set.setPen(candle_pen)
            candle_set.setBrush(candle_color)
            candle_series.append(candle_set)

        chart.addSeries(candle_series)

        if not is_secondary and self._best_parameter_indicators_check.isChecked():
            best_closes = [float(item["close"]) for item in candles]
            line_specs = tuple(
                (
                    label,
                    [
                        {"time": int(candles[index]["time"]), "value": value}
                        for index, value in enumerate(
                            _to_ema(best_closes, line_period)
                            if ma_type == "ema"
                            else _to_sma(best_closes, line_period)
                        )
                    ],
                    True,
                    color,
                    2,
                )
                for label, ma_type, line_period, color in _best_parameter_indicator_specs(
                    display_symbol or self._selected_symbol(),
                    period,
                    direction_filter=self._history_trade_direction_filter(),
                )
            )
        else:
            line_specs = (
                ("EMA 15", payload.ema_9, self._ema9.isChecked(), _CHART_EMA15_COLOR, _EMA15_LINE_WIDTH),
                ("SMA 50", payload.ema_21, self._ema21.isChecked(), _CHART_SMA50_COLOR, _SMA50_LINE_WIDTH),
                (
                    "EMA 21",
                    _ema_chart_points(candles, 21),
                    self._ema21_55_check.isChecked(),
                    _CHART_EMA21_COLOR,
                    _EMA15_LINE_WIDTH,
                ),
                (
                    "EMA 55",
                    _ema_chart_points(candles, 55),
                    self._ema21_55_check.isChecked(),
                    _CHART_EMA55_COLOR,
                    _SMA50_LINE_WIDTH,
                ),
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

        for marker in history_trade_markers:
            try:
                trade_price = float(marker.get("price", 0.0) or 0.0)
            except (TypeError, ValueError):
                continue
            if trade_price > 0.0:
                min_price = min(min_price, trade_price)
                max_price = max(max_price, trade_price)

        if include_workspace_lines:
            entry = self._workspace_entry()
            line_rules = entry.get("lines", [])
            records = list(line_rules) if isinstance(line_rules, list) else []
            workspace_lines = [dict(item) for item in records if isinstance(item, dict)]
            rr_rules = entry.get("rr", [])
            raw_rr_items = list(rr_rules) if isinstance(rr_rules, list) else []
            workspace_rr_items = [dict(item) for item in raw_rr_items if isinstance(item, dict)]
            instrument = self._instrument_for_symbol(display_symbol or self._selected_symbol())
            price_increment = self._current_rr_price_increment()
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
                kind = str(item.get("kind", "horizontal") or "horizontal").strip().lower()
                ray_start = int(item.get("time_a", 0) or 0) if kind == "ray" else 0
                if kind == "ray":
                    start_x = _display_x_for_candle_time(
                        candles, display_times_ms, candle_time=ray_start, display_step_ms=display_step_ms,
                    )
                    start_value = self._display_price_from_logical(
                        logical_payload, float(item.get("price_a", 0.0) or 0.0), is_secondary=is_secondary,
                    )
                    series.append(float(start_x), start_value)
                    line_values.append(start_value)
                    min_price = min(min_price, start_value)
                    max_price = max(max_price, start_value)
                for candle_index, candle in enumerate(candles):
                    if kind == "ray" and int(candle["time"]) < ray_start:
                        continue
                    projected_value = self._display_price_from_logical(
                        logical_payload,
                        float(line_value_at(item, int(candle["time"]))),
                        is_secondary=is_secondary,
                    )
                    series.append(float(display_times_ms[candle_index]), projected_value)
                    line_values.append(projected_value)
                    min_price = min(min_price, projected_value)
                    max_price = max(max_price, projected_value)
                future_line_time = 0
                if kind == "horizontal":
                    right_edge_x = float(display_times_ms[-1]) + _native_right_padding_ms(display_step_ms)
                    projected_value = self._display_price_from_logical(
                        logical_payload,
                        float(line_value_at(item, last_candle_time)),
                        is_secondary=is_secondary,
                    )
                    series.append(right_edge_x, projected_value)
                    line_values.append(projected_value)
                    min_price = min(min_price, projected_value)
                    max_price = max(max_price, projected_value)
                elif kind == "ray":
                    right_edge_x = float(display_times_ms[-1]) + _native_right_padding_ms(display_step_ms)
                    projected_value = self._display_price_from_logical(
                        logical_payload, float(item.get("price_a", 0.0) or 0.0), is_secondary=is_secondary,
                    )
                    series.append(right_edge_x, projected_value)
                    line_values.append(projected_value)
                else:
                    future_line_time = max(int(item.get("time_a", 0) or 0), int(item.get("time_b", 0) or 0))
                    if future_line_time <= last_candle_time:
                        future_line_time = 0
                if future_line_time > last_candle_time:
                    future_display_x = _display_x_for_candle_time(
                        candles,
                        display_times_ms,
                        candle_time=future_line_time,
                        display_step_ms=display_step_ms,
                    )
                    future_value = self._display_price_from_logical(
                        logical_payload,
                        float(line_value_at(item, future_line_time)),
                        is_secondary=is_secondary,
                    )
                    series.append(future_display_x, future_value)
                    line_values.append(future_value)
                    min_price = min(min_price, future_value)
                    max_price = max(max_price, future_value)
                chart.addSeries(series)
                overlay_values.append(line_values)
            if self._reverse_kline_enabled_for_chart(is_secondary=is_secondary):
                workspace_lines = [
                    dict(
                        item,
                        price_a=self._display_price_from_logical(
                            logical_payload,
                            float(item.get("price_a", 0.0) or 0.0),
                            is_secondary=is_secondary,
                        ),
                        price_b=self._display_price_from_logical(
                            logical_payload,
                            float(item.get("price_b", 0.0) or 0.0),
                            is_secondary=is_secondary,
                        ),
                    )
                    for item in workspace_lines
                ]
                workspace_rr_items = [
                    dict(
                        item,
                        price_entry=self._display_price_from_logical(
                            logical_payload,
                            float(item.get("price_entry", 0.0) or 0.0),
                            is_secondary=is_secondary,
                        ),
                        price_stop=self._display_price_from_logical(
                            logical_payload,
                            float(item.get("price_stop", 0.0) or 0.0),
                            is_secondary=is_secondary,
                        ),
                        price_tp=self._display_price_from_logical(
                            logical_payload,
                            float(item.get("price_tp", 0.0) or 0.0),
                            is_secondary=is_secondary,
                        ),
                    )
                    for item in workspace_rr_items
                ]
            workspace_rr_items = [
                dict(
                    item,
                    **_build_rr_overlay_snapshot(
                        item,
                        instrument=instrument,
                        price_increment=price_increment,
                    ),
                )
                for item in workspace_rr_items
            ]
            for item in workspace_rr_items:
                try:
                    min_price = min(
                        min_price,
                        float(item.get("price_entry", 0.0) or 0.0),
                        float(item.get("price_stop", 0.0) or 0.0),
                        float(item.get("price_tp", 0.0) or 0.0),
                    )
                    max_price = max(
                        max_price,
                        float(item.get("price_entry", 0.0) or 0.0),
                        float(item.get("price_stop", 0.0) or 0.0),
                        float(item.get("price_tp", 0.0) or 0.0),
                    )
                except (TypeError, ValueError):
                    continue

        axis_x = QDateTimeAxis()
        axis_x.setFormat(
            "yyyy-MM-dd ddd" if _bar_to_ms(period) >= 86_400_000 else "yyyy-MM-dd ddd HH:mm"
        )
        axis_x.setTickCount(min(8, max(3, len(candles) // 180 + 3)))
        axis_x.setLabelsColor(QColor(_CHART_AXIS_TEXT_COLOR))
        axis_x.setGridLineColor(QColor(_CHART_GRID_COLOR))
        axis_x.setLinePenColor(QColor(_CHART_AXIS_LINE_COLOR))
        axis_x.setGridLineVisible(False)

        axis_y = QValueAxis()
        top_padding, bottom_padding = _compute_axis_y_padding(min_price, max_price, show_atr=show_atr)
        axis_y.setRange(min_price - bottom_padding, max_price + top_padding)
        axis_y.setLabelFormat(_axis_y_label_format(min_price, max_price))
        axis_y.setTickCount(8)
        axis_y.setLabelsColor(QColor(_CHART_AXIS_TEXT_COLOR))
        axis_y.setGridLineColor(QColor(_CHART_GRID_COLOR))
        axis_y.setLinePenColor(QColor(_CHART_AXIS_LINE_COLOR))

        chart.addAxis(axis_x, Qt.AlignmentFlag.AlignBottom)
        chart.addAxis(axis_y, Qt.AlignmentFlag.AlignRight)
        for series in chart.series():
            series.attachAxis(axis_x)
            series.attachAxis(axis_y)

        symbol = (display_symbol or self._selected_symbol()).strip().upper()
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
                atr_values=atr_values,
                show_atr=show_atr,
                chart_note_lines=chart_note_lines,
                trend_indicators=trend_indicators,
                signal_markers=self._filter_replay_signal_markers_for_chart(
                    payload.signal_markers,
                    period=period,
                    is_secondary=is_secondary,
                ),
                history_trade_markers=history_trade_markers,
                box_overlays=self._visible_box_overlays(payload),
                channel_overlays=self._visible_channel_overlays(payload),
                workspace_lines=workspace_lines if include_workspace_lines else [],
                workspace_rr_items=workspace_rr_items if include_workspace_lines else [],
                selected_workspace_line_index=self._selected_line_index if include_workspace_lines else -1,
                selected_workspace_rr_index=self._selected_rr_index if include_workspace_lines else -1,
                hovered_workspace_line_index=self._hovered_line_index if include_workspace_lines else -1,
                hovered_workspace_drag_mode=self._hovered_line_drag_mode if include_workspace_lines else None,
                latest_price=float(candles[-1]["close"]) if candles else None,
                restore_state=restore_state,
            )
            if chart_view is self._native_chart_view:
                if self._best_parameter_indicators_check.isChecked():
                    future_ma_specs = [
                        (ma_type, line_period, color, 2)
                        for _label, ma_type, line_period, color in _best_parameter_indicator_specs(
                            symbol,
                            period,
                            direction_filter=self._history_trade_direction_filter(),
                        )
                    ]
                else:
                    future_ma_specs = []
                    if self._ema9.isChecked():
                        future_ma_specs.append(("ema", 15, _CHART_EMA15_COLOR, _EMA15_LINE_WIDTH))
                    if self._ema21.isChecked():
                        future_ma_specs.append(("sma", 50, _CHART_SMA50_COLOR, _SMA50_LINE_WIDTH))
                    if self._ema21_55_check.isChecked():
                        future_ma_specs.append(("ema", 21, _CHART_EMA21_COLOR, _EMA15_LINE_WIDTH))
                        future_ma_specs.append(("ema", 55, _CHART_EMA55_COLOR, _SMA50_LINE_WIDTH))
                chart_view.set_future_ma_specs(future_ma_specs)
                self._restore_area_shape_projection(chart_view, symbol=symbol, period=period)
                self._restore_area_shape_replay(chart_view, symbol=symbol, period=period)
        else:
            axis_x.setRange(
                QDateTime.fromMSecsSinceEpoch(display_times_ms[0]),
                QDateTime.fromMSecsSinceEpoch(display_times_ms[-1]),
            )

    def _current_workspace_key(self, *, symbol: str | None = None, period: str | None = None) -> str:
        resolved_symbol = (symbol or self._selected_symbol()).strip().upper()
        resolved_period = (period or self._period_combo.currentText()).strip()
        return build_workspace_key(resolved_symbol, resolved_period)

    def _workspace_entry(self, *, symbol: str | None = None, period: str | None = None) -> dict[str, object]:
        key = self._current_workspace_key(symbol=symbol, period=period)
        entry = normalize_workspace_entry(self._workspace_entries.get(key))
        self._workspace_entries[key] = entry
        return entry

    def _save_workspace_snapshot(self) -> None:
        save_kline_analysis_workspace_entries(self._workspace_entries)

    def _area_shape_plans(self) -> list[AreaShapePlan]:
        monitor = getattr(self, "_area_shape_monitor", None)
        getter = getattr(monitor, "plans", None)
        if not callable(getter):
            return []
        try:
            return list(getter())
        except Exception:
            return []

    def _current_area_plan(self, *, draft_only: bool = False) -> AreaShapePlan | None:
        symbol = self._selected_symbol().strip().upper()
        profile = self._active_profile_name().strip()
        environment = self._active_environment().strip().lower()
        candidates = [
            plan for plan in self._area_shape_plans()
            if plan.symbol == symbol and plan.period == "1H" and plan.profile_name == profile and plan.environment == environment
            and (not draft_only or plan.status == "draft")
        ]
        return candidates[-1] if candidates else None

    def _frozen_area_plans_for_line(self, line_id: str) -> list[AreaShapePlan]:
        return [
            plan
            for plan in self._area_shape_plans()
            if (
                line_id in {plan.support_line_id, plan.pressure_line_id}
                and plan.status in {
                    "monitoring", "touched", "paper_pending", "paper_position",
                    "paper_break_even", "paper_locked", "paused", "manual_review",
                }
            )
        ]

    def _area_line_is_frozen(self, line_id: str) -> bool:
        return bool(self._frozen_area_plans_for_line(line_id))

    def _area_line_price(self, line_id: str) -> Decimal:
        entry = self._workspace_entry(symbol=self._selected_symbol(), period="1H")
        for item in entry.get("lines", []) if isinstance(entry.get("lines"), list) else []:
            if isinstance(item, dict) and str(item.get("id", "")) == line_id:
                return Decimal(str(item.get("price_a", "0")))
        raise ValueError("区域计划关联线已丢失，请重新创建两线草稿")

    def _apply_area_shape_prices(self, plan: AreaShapePlan, support: Decimal, pressure: Decimal) -> None:
        if plan.status != "draft":
            raise ValueError("仅草稿计划可以手动调整支撑和压力价")
        if (
            not support.is_finite() or not pressure.is_finite()
            or not math.isfinite(float(support)) or not math.isfinite(float(pressure))
            or float(support) <= 0 or float(pressure) <= float(support)
        ):
            raise ValueError("请输入有效价格，并确保下方支撑低于上方压力")
        if self._area_line_is_frozen(plan.support_line_id) or self._area_line_is_frozen(plan.pressure_line_id):
            raise ValueError("监控中的区域线与冻结参数不能修改")

        entry = self._workspace_entry(symbol=plan.symbol, period=plan.period)
        lines = entry.get("lines")
        if not isinstance(lines, list):
            raise ValueError("找不到区域射线，请重新创建两线草稿")
        prices_by_id = {plan.support_line_id: support, plan.pressure_line_id: pressure}
        found: set[str] = set()
        previous_lines: dict[str, tuple[object, object]] = {}
        previous_prices = plan.support_price, plan.pressure_price
        for record in lines:
            if not isinstance(record, dict):
                continue
            line_id = str(record.get("id", ""))
            if line_id not in prices_by_id:
                continue
            found.add(line_id)
            previous_lines[line_id] = record.get("price_a"), record.get("price_b")
            price = float(prices_by_id[line_id])
            record["price_a"] = price
            record["price_b"] = price
        if found != set(prices_by_id):
            for record in lines:
                if isinstance(record, dict) and str(record.get("id", "")) in previous_lines:
                    old_a, old_b = previous_lines[str(record["id"])]
                    record["price_a"], record["price_b"] = old_a, old_b
            raise ValueError("区域计划关联线已丢失，请重新创建两线草稿")

        plan.support_price = support
        plan.pressure_price = pressure
        monitor_saved = False
        try:
            monitor = getattr(self, "_area_shape_monitor", None)
            if monitor is None:
                raise ValueError("区域形态后台服务未连接")
            monitor.save_draft(plan)
            monitor_saved = True
            self._save_workspace_snapshot()
        except Exception as exc:
            for record in lines:
                if isinstance(record, dict) and str(record.get("id", "")) in previous_lines:
                    old_a, old_b = previous_lines[str(record["id"])]
                    record["price_a"], record["price_b"] = old_a, old_b
            plan.support_price, plan.pressure_price = previous_prices
            if monitor_saved:
                try:
                    monitor.save_draft(plan)
                except Exception as rollback_exc:
                    raise RuntimeError(f"保存价位失败：{exc}；草稿回退失败，请核对计划价位：{rollback_exc}") from exc
            raise

        self._populate_line_table()
        if self._pending_payload is not None:
            self._render_to_chart(self._pending_payload)
        self._refresh_area_shape_status(plan)

    def _edit_area_shape_prices(self) -> None:
        plan = self._current_area_plan(draft_only=True)
        if plan is None:
            self._set_area_shape_status("请先创建草稿；监控中的计划不能手动改价。")
            return
        if not self._area_pair_lines_exist(plan):
            self._set_area_shape_status("区域计划关联线已丢失，请重新创建两线草稿。")
            return

        dialog = QDialog(self)
        dialog.setWindowTitle("手动调整支撑 / 压力")
        layout = QVBoxLayout(dialog)
        form = QFormLayout()
        support_edit = QLineEdit(self._format_area_shape_price(plan.support_price, plan.symbol), dialog)
        pressure_edit = QLineEdit(self._format_area_shape_price(plan.pressure_price, plan.symbol), dialog)
        support_edit.setPlaceholderText("例如 80799.5")
        pressure_edit.setPlaceholderText("例如 84000")
        form.addRow("下方支撑", support_edit)
        form.addRow("上方压力", pressure_edit)
        layout.addLayout(form)
        layout.addWidget(QLabel("应用后会同步更新图上线条和当前草稿。"))
        buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel,
            parent=dialog,
        )
        buttons.button(QDialogButtonBox.StandardButton.Ok).setText("应用")
        buttons.button(QDialogButtonBox.StandardButton.Cancel).setText("取消")
        layout.addWidget(buttons)

        def _accept_prices() -> None:
            try:
                support = Decimal(support_edit.text().strip())
                pressure = Decimal(pressure_edit.text().strip())
            except InvalidOperation:
                QMessageBox.warning(dialog, "价格无效", "请输入有效数字，例如 84000 或 84000.5。")
                return
            try:
                self._apply_area_shape_prices(plan, support, pressure)
            except Exception as exc:
                QMessageBox.warning(dialog, "调整失败", str(exc))
                return
            self._set_area_shape_status("支撑和压力价已更新；图上射线与草稿计划已同步。")
            dialog.accept()

        buttons.accepted.connect(_accept_prices)
        buttons.rejected.connect(dialog.reject)
        try:
            dialog.exec()
        finally:
            dialog.deleteLater()

    def _area_pair_lines_exist(self, plan: AreaShapePlan | None = None) -> bool:
        target = plan or self._current_area_plan()
        if target is None:
            return False
        records = self._workspace_entry(symbol=target.symbol, period=target.period).get("lines", [])
        ids = {str(item.get("id", "")) for item in records if isinstance(item, dict)} if isinstance(records, list) else set()
        return target.support_line_id in ids and target.pressure_line_id in ids

    def _area_shape_initial_ray_time(self) -> int:
        candles = self._pending_payload.candles if self._pending_payload else []
        if not candles:
            return 0
        times = _build_display_times_ms(candles, "1H")
        view = self._native_chart_view
        if (isinstance(view, InteractiveKlineChartView) and view._axis_x is not None
                and view._period.upper() == "1H" and view._symbol == self._selected_symbol().upper()):
            left, right = view.current_x_range()
        else:
            left, right = _default_native_x_range_with_right_padding(
                times, display_step_ms=_display_step_ms("1H", candles),
                right_padding_bars=_one_hour_right_padding_bars(len(candles)),
            )
        midpoint = (left + right) / 2
        index = min(range(len(times)), key=lambda item: abs(times[item] - midpoint))
        return int(candles[index]["time"])

    def _create_area_shape_plan(self) -> None:
        self._select_rr_workspace_mode("area")
        if self._pending_payload is None or not self._pending_payload.candles:
            self._set_area_shape_status("请先加载1H K线，再创建区域计划。")
            return
        if self._period_combo.currentText().strip().upper() != "1H" or not self._selected_symbol().strip().upper().endswith("-SWAP"):
            self._set_area_shape_status("区域计划仅开放在1H SWAP合约图上。")
            return
        monitor = getattr(self, "_area_shape_monitor", None)
        if monitor is None:
            self._set_area_shape_status("区域形态后台服务未连接；计划创建已禁用。")
            return
        prior = self._current_area_plan(draft_only=True)
        current_plan = self._current_area_plan()
        existing_pair_plan = prior or current_plan
        reusable_terminal_plan = (
            current_plan
            if prior is None and current_plan is not None
            and current_plan.status in {"cancelled", "paper_closed", "expired"}
            and current_plan.paper_position is None
            and self._area_pair_lines_exist(current_plan)
            else None
        )
        if self._area_pair_lines_exist(existing_pair_plan):
            if prior is not None:
                try:
                    prior.support_price = self._area_line_price(prior.support_line_id)
                    prior.pressure_price = self._area_line_price(prior.pressure_line_id)
                    monitor.save_draft(prior)
                    self._refresh_area_shape_status(prior)
                except Exception as exc:
                    self._set_area_shape_status(f"读取已有两线失败：{exc}")
                    return
                self._set_area_shape_status("当前计划的一键两线已在图上；拖动射线调整即可。")
                self._sync_area_shape_workflow_actions()
                return
            if reusable_terminal_plan is None:
                self._set_area_shape_status("当前计划的一键两线已在图上；请先结束计划，再新建草稿。")
                self._sync_area_shape_workflow_actions()
                return
        context_plans = [
            item for item in self._area_shape_plans()
            if item.symbol == self._selected_symbol().strip().upper()
            and item.period == "1H"
            and item.profile_name == self._active_profile_name().strip()
            and item.environment == self._active_environment().strip().lower()
        ]
        try:
            config = prior.config if prior is not None else (context_plans[-1].config if context_plans else AreaShapeConfig())
            config.validate()
            candles = self._pending_payload.candles
            recent_candles = candles[-120:]
            lows = [Decimal(str(item["low"])) for item in recent_candles]
            highs = [Decimal(str(item["high"])) for item in recent_candles]
            low, high = min(lows), max(highs)
            if low <= 0 or high <= low:
                raise ValueError("最近K线高低点无效，无法生成两条线")
            support = low
            pressure = high
            workspace = self._workspace_entry(symbol=self._selected_symbol(), period="1H")
            lines = workspace.get("lines")
            if not isinstance(lines, list):
                lines = []
                workspace["lines"] = lines
            now_time = self._area_shape_initial_ray_time()
            if prior is not None:
                support_id, pressure_id = prior.support_line_id, prior.pressure_line_id
                found_support = found_pressure = False
                for item in lines:
                    if not isinstance(item, dict):
                        continue
                    if item.get("id") == support_id:
                        support = Decimal(str(item.get("price_a", support)))
                        found_support = True
                    elif item.get("id") == pressure_id:
                        pressure = Decimal(str(item.get("price_a", pressure)))
                        found_pressure = True
                if not found_support or not found_pressure:
                    raise ValueError("草稿关联线缺失；请先取消损坏草稿，再重新生成两线")
                if support <= 0 or pressure <= support:
                    raise ValueError("草稿支撑/压力线顺序无效，请先校准两线")
                prior.support_price, prior.pressure_price, prior.config = support, pressure, config
                plan = prior
            elif reusable_terminal_plan is not None:
                support_id = reusable_terminal_plan.support_line_id
                pressure_id = reusable_terminal_plan.pressure_line_id
                support = self._area_line_price(support_id)
                pressure = self._area_line_price(pressure_id)
                if support <= 0 or pressure <= support:
                    raise ValueError("原计划两线顺序无效，请先校准两线")
                plan = AreaShapePlan.create(
                    profile_name=self._active_profile_name(), environment=self._active_environment(),
                    symbol=self._selected_symbol(), support_price=support, pressure_price=pressure,
                    config=config, support_line_id=support_id, pressure_line_id=pressure_id,
                )
            else:
                support_id, pressure_id = uuid4().hex[:10], uuid4().hex[:10]
                plan = AreaShapePlan.create(
                    profile_name=self._active_profile_name(), environment=self._active_environment(),
                    symbol=self._selected_symbol(), support_price=support, pressure_price=pressure,
                    config=config, support_line_id=support_id, pressure_line_id=pressure_id,
                )
                lines.extend([
                    {"id": support_id, "kind": "ray", "label": f"区域支撑 · {plan.profile_name}/{plan.environment} · {plan.plan_id[:6]}", "trigger": "touch", "action": "notify", "time_a": now_time, "price_a": float(support), "time_b": now_time, "price_b": float(support), "enabled": False, "color": "#2563eb"},
                    {"id": pressure_id, "kind": "ray", "label": f"区域压力 · {plan.profile_name}/{plan.environment} · {plan.plan_id[:6]}", "trigger": "touch", "action": "notify", "time_a": now_time, "price_a": float(pressure), "time_b": now_time, "price_b": float(pressure), "enabled": False, "color": "#ea580c"},
                ])
            monitor.save_draft(plan)
            self._save_workspace_snapshot()
            self._populate_line_table()
            # Workspace line bodies are QLineSeries, not paint-only handles.
            # Rebuild them now instead of waiting for a timeframe reload.
            self._render_to_chart(self._pending_payload)
            self._refresh_area_shape_status()
            if prior is not None:
                self._set_status("已定位当前账户草稿的两根区域线；保留原位置，可继续拖动校准。")
            else:
                self._set_status(f"已生成一组成对区域线；初始位置取最近 {len(recent_candles)} 根 1H K 线的最低点和最高点，请手工校准。")
        except Exception as exc:
            self._set_area_shape_status(f"无法创建区域计划：{exc}")

    def _auto_create_area_shape_plan(self) -> None:
        """Create a normal area plan from confirmed, clustered swing zones."""
        self._select_rr_workspace_mode("area")
        if self._pending_payload is None or not self._pending_payload.raw_candles:
            self._set_area_shape_status("请先加载1H K线，再自动找区域。")
            return
        if self._period_combo.currentText().strip().upper() != "1H" or not self._selected_symbol().strip().upper().endswith("-SWAP"):
            self._set_area_shape_status("自动区域计划仅开放在1H SWAP合约图上。")
            return
        monitor = getattr(self, "_area_shape_monitor", None)
        if monitor is None:
            self._set_area_shape_status("区域形态后台服务未连接；计划创建已禁用。")
            return
        current = self._current_area_plan()
        if current is not None and current.status != "draft":
            self._set_area_shape_status("当前已有非草稿区域计划；请先结束该计划，再创建自动区域。")
            return
        if current is not None and self._area_pair_lines_exist(current):
            self._set_area_shape_status("当前草稿已有区域线；请先删除当前草稿和两线，再重新自动识别。")
            return
        try:
            candles = [item for item in self._pending_payload.raw_candles if item.confirmed]
            zones = find_auto_zone_pair(candles)
            config = current.config if current is not None else AreaShapeConfig()
            config.validate()
            if config.direction == "long" and zones.support.price >= zones.resistance.price:
                raise ValueError("自动支撑压力顺序无效")
            if zones.support.price <= 0 or zones.resistance.price <= zones.support.price:
                raise ValueError("自动区域价格关系无效")
            workspace = self._workspace_entry(symbol=self._selected_symbol(), period="1H")
            lines = workspace.get("lines")
            if not isinstance(lines, list):
                lines = []
                workspace["lines"] = lines
            support_id = current.support_line_id if current is not None else uuid4().hex[:10]
            pressure_id = current.pressure_line_id if current is not None else uuid4().hex[:10]
            if current is not None:
                plan = current
                plan.support_price = zones.support.price
                plan.pressure_price = zones.resistance.price
                plan.config = config
                plan._event(
                    "auto_zones_detected",
                    f"自动区域由最近{zones.lookback_bars}根已收盘1H K线识别；支撑簇{zones.support.touches}点，压力簇{zones.resistance.touches}点。",
                )
            else:
                plan = AreaShapePlan.create(
                    profile_name=self._active_profile_name(), environment=self._active_environment(),
                    symbol=self._selected_symbol(), support_price=zones.support.price,
                    pressure_price=zones.resistance.price, config=config,
                    support_line_id=support_id, pressure_line_id=pressure_id,
                )
            now_time = self._area_shape_initial_ray_time()
            lines.extend([
                {"id": support_id, "kind": "ray", "label": f"自动支撑({zones.support.touches}) · {plan.plan_id[:6]}", "trigger": "touch", "action": "notify", "time_a": now_time, "price_a": float(zones.support.price), "time_b": now_time, "price_b": float(zones.support.price), "enabled": False, "color": "#2563eb"},
                {"id": pressure_id, "kind": "ray", "label": f"自动压力({zones.resistance.touches}) · {plan.plan_id[:6]}", "trigger": "touch", "action": "notify", "time_a": now_time, "price_a": float(zones.resistance.price), "time_b": now_time, "price_b": float(zones.resistance.price), "enabled": False, "color": "#ea580c"},
            ])
            monitor.save_draft(plan)
            self._save_workspace_snapshot()
            self._populate_line_table()
            self._render_to_chart(self._pending_payload)
            self._refresh_area_shape_status(plan)
            message = (
                f"自动区域已生成：支撑 {zones.support.price}（{zones.support.touches}个确认摆动点），"
                f"压力 {zones.resistance.price}（{zones.resistance.touches}个确认摆动点）；基于最近 {zones.lookback_bars} 根已收盘1H K线。"
            )
            self._set_area_shape_status(message)
            self._set_status(message)
        except Exception as exc:
            self._set_area_shape_status(f"自动识别区域失败：{exc}")

    def _area_shape_replay_signature(self, plan: AreaShapePlan, start_seconds: int) -> str:
        payload = {
            "version": 9,
            "plan_id": plan.plan_id,
            "symbol": plan.symbol.strip().upper(),
            "period": plan.period.strip().upper(),
            "support_line_id": plan.support_line_id,
            "pressure_line_id": plan.pressure_line_id,
            "support_price": str(plan.support_price),
            "pressure_price": str(plan.pressure_price),
            "start_seconds": int(start_seconds),
            "config": plan.config.to_dict(),
        }
        return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    def _area_shape_replay_record(
        self, plan_id: str, *, symbol: str, period: str,
    ) -> dict[str, Any] | None:
        workspace = self._workspace_entry(symbol=symbol, period=period)
        records = workspace.get("area_replays")
        if not isinstance(records, dict):
            return None
        record = records.get(str(plan_id))
        return dict(record) if isinstance(record, dict) else None

    def _saved_area_shape_replay(self, *, symbol: str, period: str) -> tuple[AreaShapePlan, dict[str, Any]] | None:
        """Saved reports outlive their editable plan, rays and calculation version."""
        current = self._current_area_plan()
        candidates = []
        for plan_id, record in self._workspace_entry(symbol=symbol, period=period)["area_replays"].items():
            snapshot = record.get("plan_snapshot")
            try:
                if isinstance(snapshot, dict):
                    plan = AreaShapePlan.from_dict(snapshot)
                elif current is not None and current.plan_id == plan_id:
                    # Compatibility with older saved reports without a plan snapshot.
                    plan = copy.deepcopy(current)
                else:
                    continue
                if (
                    plan.symbol.strip().upper() != symbol.strip().upper()
                    or plan.period.strip().upper() != period.strip().upper()
                    or plan.profile_name.strip() != self._active_profile_name().strip()
                    or plan.environment.strip().lower() != self._active_environment().strip().lower()
                ):
                    continue
                candidates.append((plan, dict(record)))
            except (TypeError, ValueError, KeyError):
                continue
        return max(reversed(candidates), key=lambda item: str(item[1].get("saved_at", "")), default=None)

    def _area_shape_saved_overlay(self, plan: AreaShapePlan, record: dict[str, Any]) -> dict[str, Any]:
        return {
            "symbol": plan.symbol, "period": plan.period,
            "profile_name": plan.profile_name, "environment": plan.environment,
            "input_signature": record.get("input_signature", ""),
            "rows": list(record.get("rows", [])),
            "filters": dict(record.get("filters", {})),
        }

    def _open_saved_area_shape_replay(self, plan: AreaShapePlan, record: dict[str, Any]) -> None:
        self._area_shape_replay_overlay = self._area_shape_saved_overlay(plan, record)
        self._restore_area_shape_replay(
            self._native_chart_view, symbol=self._selected_symbol(),
            period=self._period_combo.currentText(), focus=True,
        )
        self._show_area_shape_replay_results(
            plan, int(record.get("start_seconds", 0)), list(record.get("rows", [])),
            sides=("long", "short"), replay_record={**record, "_cache_hit": True},
        )
        self._set_area_shape_status("已打开保存的复盘结果；行情、参数和射线变化不会自动更新，仅点击“重新复盘”后替换。")

    def _run_area_shape_replay(self, _checked: bool = False, *, force: bool = False) -> None:
        if self._area_shape_replay_thread is not None and self._area_shape_replay_thread.isRunning():
            self._set_area_shape_status("区域复盘正在运行，请等待当前结果。")
            return
        if not force:
            saved = self._saved_area_shape_replay(
                symbol=self._selected_symbol(), period=self._period_combo.currentText(),
            )
            if saved is not None:
                self._open_saved_area_shape_replay(*saved)
                return
        plan = self._current_area_plan()
        if plan is None or self._pending_payload is None:
            self._set_area_shape_status("请先生成两条射线和区域计划，再运行复盘。")
            return
        current_period = self._period_combo.currentText().strip().upper()
        if current_period != plan.period.strip().upper():
            self._set_area_shape_status(f"当前图表是 {current_period}，此区域计划按 {plan.period} K 线复盘；请切换到 {plan.period} 后再运行。")
            return
        records = self._workspace_entry(symbol=plan.symbol, period=plan.period).get("lines", [])
        lines = [item for item in records if isinstance(item, dict)] if isinstance(records, list) else []
        support = next((item for item in lines if item.get("id") == plan.support_line_id), None)
        pressure = next((item for item in lines if item.get("id") == plan.pressure_line_id), None)
        if support is None or pressure is None:
            self._set_area_shape_status("复盘需要两条关联射线都在图上。")
            return
        start_seconds = int(support.get("time_a", 0) or 0)
        if start_seconds != int(pressure.get("time_a", 0) or 0):
            self._set_area_shape_status("两条射线起点时间不一致；请拖动任一射线起点使它们对齐。")
            return
        replay_plan = copy.deepcopy(plan)
        replay_plan.support_price = Decimal(str(support.get("price_a", plan.support_price)))
        replay_plan.pressure_price = Decimal(str(pressure.get("price_a", plan.pressure_price)))
        if replay_plan.support_price <= 0 or replay_plan.pressure_price <= replay_plan.support_price:
            self._set_area_shape_status("关联射线当前价格无效：支撑价必须低于压力价；请检查两条射线。")
            return
        signature = self._area_shape_replay_signature(replay_plan, start_seconds)
        cached = self._area_shape_replay_record(replay_plan.plan_id, symbol=plan.symbol, period=plan.period)
        overlay = self._area_shape_replay_overlay
        if (
            (cached is None or str(cached.get("input_signature", "")) != signature)
            and isinstance(overlay, dict)
            and str(overlay.get("symbol", "")).strip().upper() == replay_plan.symbol.strip().upper()
            and str(overlay.get("period", "")).strip().upper() == replay_plan.period.strip().upper()
            and str(overlay.get("input_signature", "")) == signature
        ):
            cached = {
                "version": 1,
                "input_signature": signature,
                "plan_snapshot": replay_plan.to_dict(),
                "start_seconds": start_seconds,
                "rows": list(overlay.get("rows", [])),
                "filters": dict(overlay.get("filters", {})) if isinstance(overlay.get("filters"), dict) else {},
            }
            self._workspace_entry(symbol=plan.symbol, period=plan.period)["area_replays"][plan.plan_id] = cached
            self._save_workspace_snapshot()
        if not force and cached is not None:
            self._open_saved_area_shape_replay(replay_plan, cached)
            return

        candles = [item for item in self._pending_payload.raw_candles if item.confirmed]
        sides = ("long", "short")
        range_snapshot = {
            "first_candle_ts": int(candles[0].ts) if candles else 0,
            "last_candle_ts": int(candles[-1].ts) if candles else 0,
            "candle_count": len(candles),
        }
        self._area_shape_replay_button.setEnabled(False)
        if force:
            replay_status = "正在按当前行情重新复盘"
        else:
            replay_status = "当前账户、币种和周期尚无保存结果，正在首次计算并保存"
        self._set_area_shape_status(
            f"{replay_status}：多空两侧 {len(candles)} 根已收盘K线，并扫描持仓期间的潜在机会；"
            f"本次射线价：支撑 {replay_plan.support_price}，压力 {replay_plan.pressure_price}。"
        )
        action = lambda: self._simulate_area_shape_replay(
            replay_plan, candles, start_ms=start_seconds * 1000, sides=sides,
        )
        worker = AreaShapeDemoActionThread(action, self)
        self._area_shape_replay_thread = worker
        self._area_shape_replay_context = (replay_plan, start_seconds, sides, signature, range_snapshot)
        self._retired_loaders.add(worker)
        worker.completed.connect(self._on_area_shape_replay_worker_completed)
        worker.failed.connect(self._area_shape_replay_failed)
        worker.finished.connect(self._on_area_shape_replay_worker_finished)
        worker.start()

    @Slot()
    def _on_area_shape_replay_worker_finished(self) -> None:
        worker = self.sender()
        self._retired_loaders.discard(worker)
        if self._area_shape_replay_thread is worker:
            self._area_shape_replay_thread = None
        self._area_shape_replay_context = None
        if not bool(getattr(self, "_shutdown_requested", False)):
            self._area_shape_replay_button.setEnabled(True)
        if worker is not None:
            worker.deleteLater()

    @Slot(str)
    def _area_shape_replay_failed(self, error: str) -> None:
        if not bool(getattr(self, "_shutdown_requested", False)):
            self._set_area_shape_status(f"区域复盘失败，已有保存结果保持不变：{error}")

    @Slot(object)
    def _on_area_shape_replay_worker_completed(self, rows: object) -> None:
        if bool(getattr(self, "_shutdown_requested", False)):
            return
        if not isinstance(rows, list):
            self._set_area_shape_status("区域复盘失败：后台结果格式无效。")
            return
        context = self._area_shape_replay_context
        if context is None:
            self._set_area_shape_status("区域复盘失败：复盘上下文已失效。")
            return
        plan, start_seconds, sides, signature, range_snapshot = context
        replay_rows = [dict(item) for item in rows if isinstance(item, dict)]
        previous = self._area_shape_replay_record(plan.plan_id, symbol=plan.symbol, period=plan.period)
        filters = dict(previous.get("filters", {})) if previous and isinstance(previous.get("filters"), dict) else {"kind": "all", "side": "all"}
        stored_rows = [
            {key: str(value) if isinstance(value, Decimal) else value for key, value in item.items()}
            for item in replay_rows
        ]
        workspace = self._workspace_entry(symbol=plan.symbol, period=plan.period)
        replay_records = workspace.get("area_replays")
        if not isinstance(replay_records, dict):
            replay_records = {}
        replay_records[plan.plan_id] = {
            "version": 1,
            "input_signature": signature,
            "plan_snapshot": plan.to_dict(),
            "support_price": str(plan.support_price),
            "pressure_price": str(plan.pressure_price),
            "start_seconds": int(start_seconds),
            "data_range": dict(range_snapshot),
            "sides": list(sides),
            "filters": filters,
            "rows": stored_rows,
            "saved_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        }
        workspace["area_replays"] = replay_records
        self._save_workspace_snapshot()
        if (
            self._selected_symbol().strip().upper() != plan.symbol.strip().upper()
            or self._period_combo.currentText().strip().upper() != plan.period.strip().upper()
            or self._active_profile_name().strip() != plan.profile_name.strip()
            or self._active_environment().strip().lower() != plan.environment.strip().lower()
        ):
            return  # Saved in its own context; never replace another account's chart.
        self._area_shape_replay_overlay = self._area_shape_saved_overlay(plan, replay_records[plan.plan_id])
        self._restore_area_shape_replay(
            self._native_chart_view,
            symbol=self._selected_symbol(),
            period=self._period_combo.currentText(),
            focus=True,
        )
        trade_count = sum(1 for item in rows if str(item.get("record_type", "trade")) == "trade")
        opportunity_count = sum(1 for item in rows if str(item.get("record_type", "trade")) == "opportunity")
        self._set_area_shape_status(
            f"区域复盘完成：回测交易 {trade_count} 笔，潜在机会 {opportunity_count} 个。"
        )
        self._show_area_shape_replay_results(
            plan, start_seconds, stored_rows, sides=sides, replay_record=replay_records[plan.plan_id],
        )

    def _restore_area_shape_replay(
        self, chart_view: object, *, symbol: str, period: str, focus: bool = False,
    ) -> None:
        overlay = self._area_shape_replay_overlay
        setter = getattr(chart_view, "set_area_shape_replay_trades", None)
        if not callable(setter):
            return
        if overlay is not None and (
            str(overlay.get("symbol", "")).strip().upper() != str(symbol).strip().upper()
            or str(overlay.get("period", "")).strip().upper() != str(period).strip().upper()
            or str(overlay.get("profile_name", self._active_profile_name())).strip() != self._active_profile_name().strip()
            or str(overlay.get("environment", self._active_environment())).strip().lower() != self._active_environment().strip().lower()
        ):
            overlay = None
        if overlay is None:
            saved = self._saved_area_shape_replay(symbol=symbol, period=period)
            if saved is not None:
                overlay = self._area_shape_saved_overlay(*saved)
        self._area_shape_replay_overlay = overlay
        if overlay is None:
            setter(None)
            return
        visible = self._area_shape_replay_filtered_rows(
            list(overlay.get("rows", [])), overlay.get("filters", {}),
        )
        focused_row = overlay.get("focused_row")
        if isinstance(focused_row, dict) and focused_row in visible:
            visible = [focused_row]
        setter(visible, focus=focus)

    @staticmethod
    def _area_shape_replay_filtered_rows(rows: list[dict[str, Any]], filters: object) -> list[dict[str, Any]]:
        filters = filters if isinstance(filters, dict) else {}
        kind = str(filters.get("kind", "all") or "all")
        side = str(filters.get("side", "all") or "all")
        return [
            item for item in rows
            if isinstance(item, dict)
            and (kind == "all" or str(item.get("record_type", "trade")) == kind)
            and (side == "all" or str(item.get("side", "")) == side)
        ]

    def _simulate_area_shape_replay(
        self, plan: AreaShapePlan, candles: list[Candle], *, start_ms: int, sides: tuple[str, ...],
    ) -> list[dict[str, object]]:
        results: list[dict[str, object]] = []
        if not candles:
            return results
        from okx_quant.area_shape_trade import AreaShapePlan as ReplayPlan

        def first_touch_after_origin(price: Decimal) -> int:
            previous_close: Decimal | None = None
            for candle in candles:
                if candle.ts <= start_ms:
                    previous_close = candle.close
                    continue
                touched = candle.low <= price <= candle.high
                crossed_gap = previous_close is not None and (
                    (previous_close > price and candle.high < price)
                    or (previous_close < price and candle.low > price)
                )
                if touched or crossed_gap:
                    return candle.ts
                previous_close = candle.close
            return 0

        reference_touch_by_side = {
            "long": first_touch_after_origin(plan.support_price),
            "short": first_touch_after_origin(plan.pressure_price),
        }

        for side in sides:
            def make_replay(replay_side: str = side) -> AreaShapePlan:
                replay_plan = ReplayPlan.from_dict(copy.deepcopy(plan.to_dict()))
                replay_plan.config = replace(
                    plan.config, direction=replay_side,
                    signal_patterns=self._area_shape_replay_patterns(plan.config.signal_patterns, replay_side),
                    entry_mode=("next_open" if plan.config.entry_trigger_mode == "pattern" else plan.config.entry_mode),
                    exit_template="fixed_1r", fixed_tp_r=Decimal("2"),
                    break_even_enabled=False, time_break_even_enabled=False,
                )
                replay_plan.status = "draft"
                replay_plan.last_candle_ts = 0
                replay_plan.last_signal_key = ""
                replay_plan.events = []
                replay_plan.paper_position = None
                replay_plan.execution_mode = "paper"
                replay_start_ms = max(start_ms, reference_touch_by_side.get(replay_side, 0))
                replay_plan.activate(
                    last_closed_candle_ts=replay_start_ms, profile_name=plan.profile_name,
                    environment=plan.environment,
                )
                replay_plan.expires_at_candle_ts = candles[-1].ts + 3_600_000
                return replay_plan

            replay = make_replay()
            current_trade: dict[str, object] | None = None
            scanners: dict[str, AreaShapePlan] = {}
            scanner_pending: dict[str, dict[str, object]] = {}
            main_pending: dict[str, object] = {}
            opportunities: list[dict[str, object]] = []
            for index, candle in enumerate(candles):
                if candle.ts <= start_ms:
                    continue
                occupied_before_bar = current_trade is not None
                if occupied_before_bar:
                    for scanner_side, scanner in scanners.items():
                        scan_event = scanner.observe(candles[: index + 1])
                        if scan_event and scan_event.get("kind") == "paper_pending":
                            scanner_pending[scanner_side] = dict(scanner.paper_position or {})
                        elif scan_event and scan_event.get("kind") == "signal_skipped" and scan_event.get("space_r") is not None:
                            opportunities.append(self._area_shape_replay_rejected_opportunity(scan_event, scanner_side))
                        elif scan_event and scan_event.get("kind") == "paper_entry":
                            position = dict(scanner.paper_position or scanner_pending.get(scanner_side, {}))
                            signal_ts = int(position.get("signal_candle_ts", candle.ts) or candle.ts)
                            opportunities.append({
                                "record_type": "opportunity", "side": scanner_side,
                                "pattern": str(scan_event.get("pattern", position.get("pattern", ""))),
                                "pattern_score": int(scan_event.get("pattern_score", position.get("pattern_score", 0)) or 0),
                                "signal_ts": signal_ts,
                                "entry_ts": int(scan_event.get("candle_ts", candle.ts) or candle.ts),
                                "entry": Decimal(str(scan_event.get("entry", "0"))),
                                "stop": Decimal(str(scan_event.get("stop", position.get("stop", "0")))),
                                "target": Decimal(str(position.get(
                                    "target", plan.pressure_price if scanner_side == "long" else plan.support_price,
                                ))),
                                "risk": Decimal(str(scan_event.get("risk_distance", "0"))),
                                "space_r": Decimal(str(scan_event.get("space_r", position.get("space_r", "0")))),
                                "r": None, "pnl": None, "exit": None, "exit_ts": 0,
                                "outcome": "模拟持仓期间出现，顺序回放未重复开仓",
                            })
                            scanner.status = "monitoring"
                            scanner.paper_position = None
                            scanner.last_signal_key = ""
                            scanner.events.clear()
                            scanner_pending.pop(scanner_side, None)

                event: dict[str, object] | None = None
                if replay.status in {"paper_position", "paper_break_even", "paper_locked"}:
                    event = replay.observe_paper_candle(candle)
                elif replay.status in {"monitoring", "touched", "paper_pending"}:
                    event = replay.observe(candles[: index + 1])
                if event and event.get("kind") == "paper_pending":
                    pending_position = replay.paper_position or {}
                    main_pending = dict(pending_position)
                if event and event.get("kind") == "signal_skipped" and event.get("space_r") is not None:
                    opportunities.append(self._area_shape_replay_rejected_opportunity(event, side))
                if event and event.get("kind") == "paper_entry":
                    position = replay.paper_position or main_pending
                    current_trade = {
                        "side": side, "pattern": str(event.get("pattern", "")), "entry": Decimal(str(event["entry"])),
                        "pattern_score": int(event.get("pattern_score", position.get("pattern_score", 0)) or 0),
                        "stop": Decimal(str(event.get("stop", "0"))), "risk": Decimal(str(event.get("risk_distance", "0"))),
                        "entry_ts": int(event.get("candle_ts", candle.ts)),
                        "signal_ts": int(position.get("signal_candle_ts", candle.ts) or candle.ts),
                        "record_type": "trade",
                    }
                    scanners = {candidate_side: make_replay(candidate_side) for candidate_side in ("long", "short")}
                    for scanner in scanners.values():
                        scanner.last_candle_ts = candle.ts
                    scanner_pending.clear()
                    main_pending = {}
                if replay.status == "paper_closed" and current_trade is not None:
                    exit_event = next((item for item in replay.events if str(item.get("kind", "")).startswith("paper_") and item.get("exit_price") and int(item.get("candle_ts", 0) or 0) == candle.ts), {})
                    if exit_event:
                        results.append(self._area_shape_replay_result(plan, current_trade, exit_event))
                    current_trade = None
                    replay.status = "monitoring"
                    replay.paper_position = None
                    replay.last_candle_ts = candle.ts
                    replay.last_signal_key = ""
                    scanners.clear()
                    scanner_pending.clear()
            if current_trade is not None:
                current_trade["exit"] = Decimal(str(candles[-1].close))
                current_trade["exit_ts"] = candles[-1].ts
                current_trade["outcome"] = "持仓中"
                results.append(self._area_shape_replay_result(plan, current_trade, None))
            trade_entries = {
                (str(item.get("side", "")), int(item.get("entry_ts", 0) or 0))
                for item in results
            }
            results.extend(
                item for item in opportunities
                if (str(item.get("side", "")), int(item.get("entry_ts", 0) or 0)) not in trade_entries
            )
        results.sort(key=lambda item: (int(item.get("signal_ts", item.get("entry_ts", 0)) or 0), str(item.get("side", ""))))
        return results

    @staticmethod
    def _area_shape_replay_rejected_opportunity(event: dict[str, object], side: str) -> dict[str, object]:
        """Keep a valid setup visible when its next-open risk/reward gate rejects entry."""
        entry = Decimal(str(event.get("entry", "0") or "0"))
        stop = Decimal(str(event.get("stop", "0") or "0"))
        target = Decimal(str(event.get("target", "0") or "0"))
        space_r = Decimal(str(event.get("space_r", "0") or "0"))
        minimum = Decimal(str(event.get("min_space_r", "0") or "0"))
        return {
            "record_type": "opportunity", "side": str(event.get("side") or side),
            "pattern": str(event.get("pattern", "")),
            "pattern_score": int(event.get("pattern_score", 0) or 0),
            "signal_ts": int(event.get("signal_candle_ts", event.get("candle_ts", 0)) or 0),
            "entry_ts": int(event.get("candle_ts", 0) or 0), "entry": entry,
            "stop": stop, "target": target, "risk": abs(entry - stop), "space_r": space_r,
            "r": None, "pnl": None, "exit": None, "exit_ts": 0,
            "outcome": f"盈亏比不足：{space_r:.2f}R < 最低 {minimum}R，不符合入场设置",
        }

    @staticmethod
    def _area_shape_replay_patterns(patterns: tuple[str, ...], side: str) -> tuple[str, ...]:
        """Mirror selected directional pattern families for the requested side."""
        pairs = (
            ("big_bullish", "big_bearish"),
            ("long_lower_shadow", "long_upper_shadow"),
            ("double_reversal_up", "double_reversal_down"),
            ("bottom_fractal", "top_fractal"),
            ("hammer", "shooting_star"),
            ("inverted_hammer", "hanging_man"),
            ("dragonfly_doji", "gravestone_doji"),
        )
        counterpart = {name: other for long_name, short_name in pairs for name, other in ((long_name, short_name), (short_name, long_name))}
        expected = "long" if side == "long" else "short"
        mapped: list[str] = []
        for pattern in patterns:
            if pattern == "inside_bar":
                candidate = pattern
            else:
                candidate = pattern
                if pattern in counterpart:
                    is_bullish = pattern in {long_name for long_name, _ in pairs}
                    if (expected == "long") != is_bullish:
                        candidate = counterpart[pattern]
            if candidate not in mapped:
                mapped.append(candidate)
        return tuple(mapped)

    def _area_shape_replay_result(
        self, plan: AreaShapePlan, trade: dict[str, object], exit_event: dict[str, object] | None,
    ) -> dict[str, object]:
        entry = Decimal(str(trade["entry"]))
        stop = Decimal(str(trade["stop"]))
        risk = Decimal(str(trade["risk"]))
        side = str(trade["side"])
        exit_kind = str(exit_event.get("kind", "")) if exit_event else ""
        if exit_kind == "paper_stop":
            # A stop is executed at the protective stop level, never at the
            # triggering candle's close. Older events have no stop_price field;
            # their exit_price was also recorded at the stop level.
            exit_price = Decimal(str(
                exit_event.get("stop_price") or exit_event.get("exit_price") or stop
            ))
        else:
            exit_price = Decimal(str(exit_event.get("exit_price"))) if exit_event and exit_event.get("exit_price") else Decimal(str(trade.get("exit", entry)))
        sign = Decimal("1") if side == "long" else Decimal("-1")
        gross_r = ((exit_price - entry) * sign / risk) if risk > 0 else Decimal("0")
        cost_rate = plan.config.round_trip_fee_rate
        if exit_kind != "paper_stop":
            cost_rate += plan.config.slippage_rate
        cost_r = ((entry + exit_price) * cost_rate / risk) if risk > 0 else Decimal("0")
        net_r = gross_r - cost_r
        pnl = net_r * plan.config.risk_amount
        return {
            "side": side, "pattern": str(trade.get("pattern", "")), "entry": entry, "stop": stop,
            "pattern_score": int(trade.get("pattern_score", 0) or 0),
            "exit": exit_price, "r": net_r, "pnl": pnl, "entry_ts": int(trade.get("entry_ts", 0) or 0),
            "signal_ts": int(trade.get("signal_ts", trade.get("entry_ts", 0)) or 0),
            "record_type": "trade",
            "exit_ts": int(exit_event.get("candle_ts", 0) if exit_event else trade.get("exit_ts", 0) or 0),
            "outcome": str(exit_event.get("kind", "持仓中")) if exit_event else str(trade.get("outcome", "持仓中")),
        }

    def _show_area_shape_replay_results(
        self, plan: AreaShapePlan, start_seconds: int, rows: list[dict[str, object]], *,
        sides: tuple[str, ...] = (), replay_record: dict[str, Any] | None = None,
    ) -> None:
        previous_dialog = self._area_shape_replay_dialog
        if previous_dialog is not None:
            previous_dialog.close()
        self._set_draw_tool("none")
        dialog = QDialog(self)
        self._area_shape_replay_dialog = dialog
        dialog.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose)
        dialog.setWindowModality(Qt.WindowModality.NonModal)
        dialog.setWindowFlag(Qt.WindowType.WindowMinMaxButtonsHint, True)

        def release_dialog(_result: int) -> None:
            if self._area_shape_replay_dialog is dialog:
                self._area_shape_replay_dialog = None
        dialog.finished.connect(release_dialog)
        dialog.setWindowTitle("区域计划复盘结果")
        dialog.resize(1220, 620)
        layout = QVBoxLayout(dialog)
        trade_rows = [item for item in rows if str(item.get("record_type", "trade")) == "trade"]
        opportunity_rows = [item for item in rows if str(item.get("record_type", "trade")) == "opportunity"]
        pnl_total = sum((Decimal(str(item.get("pnl", "0") or "0")) for item in trade_rows), Decimal("0"))
        net_r_total = sum((Decimal(str(item.get("r", "0") or "0")) for item in trade_rows), Decimal("0"))
        summary = QLabel(
            f"{plan.symbol} | 起点 {_format_bar_time(start_seconds)} | "
            f"<span style='color:#15803d'>回测交易 {len(trade_rows)} 笔</span> | "
            f"<span style='color:#6d28d9'>潜在机会 {len(opportunity_rows)} 个</span> | "
            f"支撑 {self._format_area_shape_price(plan.support_price, plan.symbol)} / "
            f"压力 {self._format_area_shape_price(plan.pressure_price, plan.symbol)} | "
            f"估算净盈亏 {format_decimal_fixed(pnl_total, places=2)} U / "
            f"净R {format_decimal_fixed(net_r_total, places=2)}R（按单笔风险预算折算；含配置手续费/滑点，不含资金费） | "
            f"{'复用已保存结果，未重新计算' if isinstance(replay_record, dict) and replay_record.get('_cache_hit') else '本次复盘结果'}",
            dialog,
        )
        summary.setWordWrap(True)
        layout.addWidget(summary)

        filter_layout = QHBoxLayout()
        kind_filter = QComboBox(dialog)
        kind_filter.addItem("全部记录", "all")
        kind_filter.addItem("回测交易", "trade")
        kind_filter.addItem("潜在机会", "opportunity")
        side_filter = QComboBox(dialog)
        side_filter.addItem("多空一起", "all")
        side_filter.addItem("只看做多", "long")
        side_filter.addItem("只看做空", "short")
        saved_filters = replay_record.get("filters", {}) if isinstance(replay_record, dict) else {}
        saved_filters = saved_filters if isinstance(saved_filters, dict) else {}
        for combo, key in ((kind_filter, "kind"), (side_filter, "side")):
            index = combo.findData(str(saved_filters.get(key, "all")))
            combo.setCurrentIndex(max(0, index))
        filter_layout.addWidget(QLabel("类型", dialog))
        filter_layout.addWidget(kind_filter)
        filter_layout.addWidget(QLabel("方向", dialog))
        filter_layout.addWidget(side_filter)
        filter_layout.addWidget(QLabel(
            "图上和表格颜色：<span style='color:#15803d'>多头回测</span> / "
            "<span style='color:#b91c1c'>空头回测</span> / "
            "<span style='color:#0369a1'>潜在多头</span> / "
            "<span style='color:#6d28d9'>潜在空头</span>；菱形为回测，三角为潜在机会。筛选同步控制图上显示。",
            dialog,
        ), 1)
        layout.addLayout(filter_layout)

        chart_controls = QHBoxLayout()
        focus_note = QLabel("复盘查看时区域线锁定，主图可拖动缩放；双击记录单独定位。", dialog)
        focus_note.setWordWrap(True)
        show_all_button = QPushButton("显示全部标记", dialog)
        show_all_button.setObjectName("areaReplayShowAll")
        show_all_button.setToolTip("恢复当前类型和方向筛选范围内的全部复盘标记")
        show_all_button.setEnabled(False)
        chart_controls.addWidget(focus_note, 1)
        chart_controls.addWidget(show_all_button)
        layout.addLayout(chart_controls)

        table = QTableWidget(0, 11, dialog)
        table.setHorizontalHeaderLabels(["记录", "方向", "形态", "评分", "信号时间", "入场时间", "入场价", "止损", "退出/目标参考", "净R/空间R", "估算U/说明"])
        table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        table.setSelectionMode(QTableWidget.SelectionMode.SingleSelection)
        table.verticalHeader().setVisible(False)
        header = table.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        for column, width in enumerate((88, 52, 100, 80, 140, 140, 90, 90, 120, 100, 220)):
            table.setColumnWidth(column, width)
        table.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(table)

        display_overlay = self._area_shape_replay_overlay
        visible_rows: list[dict[str, object]] = []

        def chart_overlay() -> dict[str, Any] | None:
            if (
                display_overlay is not None
                and self._selected_symbol().strip().upper() == plan.symbol.strip().upper()
                and self._period_combo.currentText().strip().upper() == plan.period.strip().upper()
                and self._active_profile_name().strip() == plan.profile_name.strip()
                and self._active_environment().strip().lower() == plan.environment.strip().lower()
            ):
                if self._area_shape_replay_overlay is None:
                    self._restore_area_shape_replay(self._native_chart_view, symbol=plan.symbol, period=plan.period)
                current_overlay = self._area_shape_replay_overlay
                if current_overlay is not None and current_overlay.get("input_signature") == display_overlay.get("input_signature"):
                    return current_overlay
            focus_note.setText("图表账户、币种、周期或复盘内容已切换；请返回对应图表后重新打开复盘结果。")
            return None

        def populate_table() -> list[dict[str, object]]:
            nonlocal visible_rows
            visible = self._area_shape_replay_filtered_rows(
                rows, {"kind": kind_filter.currentData(), "side": side_filter.currentData()},
            )
            visible_rows = visible
            table.setRowCount(len(visible))
            for row_index, item in enumerate(visible):
                opportunity = str(item.get("record_type", "trade")) == "opportunity"
                try:
                    reference_price = item.get("target") if opportunity else item.get("exit")
                    reference_text = self._format_area_shape_price(Decimal(str(reference_price)), plan.symbol) if reference_price is not None else "—"
                    net_or_space = Decimal(str(item.get("space_r", "0") if opportunity else item.get("r", "0")))
                    net_or_space_text = f"空间 {format_decimal_fixed(net_or_space, places=2)}R" if opportunity else f"{format_decimal_fixed(net_or_space, places=2)}R"
                    pnl_value = item.get("pnl")
                    amount_text = (
                        str(item.get("outcome", "潜在机会")) if opportunity else
                        f"持仓中 · 浮动 {format_decimal_fixed(Decimal(str(pnl_value or '0')), places=2)} U"
                        if str(item.get("outcome", "")) == "持仓中" else
                        format_decimal_fixed(Decimal(str(pnl_value or "0")), places=2)
                    )
                    values = (
                        "潜在机会" if opportunity else "回测交易",
                        "多" if item.get("side") == "long" else "空",
                        _REPLAY_SIGNAL_LABELS.get(str(item.get("pattern", "")), str(item.get("pattern", ""))),
                        (
                            f"{int(item.get('pattern_score', 0) or 0)} · "
                            f"{'重点' if int(item.get('pattern_score', 0) or 0) >= 80 else '普通' if int(item.get('pattern_score', 0) or 0) >= 65 else '弱'}"
                            if int(item.get("pattern_score", 0) or 0) > 0 else "—"
                        ),
                        _format_bar_time(int(item.get("signal_ts", item.get("entry_ts", 0)) or 0) // 1000),
                        _format_bar_time(int(item.get("entry_ts", 0) or 0) // 1000),
                        self._format_area_shape_price(Decimal(str(item.get("entry", "0"))), plan.symbol),
                        self._format_area_shape_price(Decimal(str(item.get("stop", "0"))), plan.symbol),
                        reference_text,
                        net_or_space_text,
                        amount_text,
                    )
                    palette = (
                        ("#0369a1", "#effaff") if opportunity and item.get("side") == "long" else
                        ("#6d28d9", "#f5f0ff") if opportunity else
                        ("#15803d", "#eef9f1") if item.get("side") == "long" else
                        ("#b91c1c", "#fff1f0")
                    )
                    for column, value in enumerate(values):
                        cell = QTableWidgetItem(str(value))
                        cell.setForeground(QColor(palette[0]))
                        cell.setBackground(QColor(palette[1]))
                        table.setItem(row_index, column, cell)
                except (TypeError, ValueError):
                    continue
            return visible

        def show_all_markers(*_args: object) -> None:
            overlay = chart_overlay()
            if overlay is None:
                return
            overlay.pop("focused_row", None)
            setter = getattr(self._native_chart_view, "set_area_shape_replay_trades", None)
            if callable(setter):
                setter(visible_rows, focus=True, fit=True)
            show_all_button.setEnabled(False)
            focus_note.setText(
                f"已显示并定位当前筛选内 {len(visible_rows)} 条记录；查看时区域线锁定。"
                if visible_rows else f"当前筛选为 0 条（共 {len(rows)} 条记录），请调整类型或方向筛选。"
            )

        def focus_record(row_index: int, _column: int) -> None:
            if not 0 <= row_index < len(visible_rows):
                return
            overlay = chart_overlay()
            if overlay is None:
                return
            record = visible_rows[row_index]
            overlay["focused_row"] = record
            setter = getattr(self._native_chart_view, "set_area_shape_replay_trades", None)
            if callable(setter):
                setter([record], focus=True, fit=True)
            show_all_button.setEnabled(True)
            focus_note.setText(f"图上仅显示第 {row_index + 1} 条记录及其关联标记；列表保留全部记录。")

        def apply_filters(*_args: object) -> None:
            filters = {"kind": str(kind_filter.currentData()), "side": str(side_filter.currentData())}
            overlay = chart_overlay()
            if overlay is not None:
                overlay["filters"] = filters
            populate_table()
            show_all_markers()
            cache = self._area_shape_replay_record(plan.plan_id, symbol=plan.symbol, period=plan.period)
            if cache is not None:
                cache["filters"] = filters
                self._workspace_entry(symbol=plan.symbol, period=plan.period).setdefault("area_replays", {})[plan.plan_id] = cache
                self._save_workspace_snapshot()

        populate_table()
        kind_filter.currentIndexChanged.connect(apply_filters)
        side_filter.currentIndexChanged.connect(apply_filters)
        table.cellDoubleClicked.connect(focus_record)
        show_all_button.clicked.connect(show_all_markers)
        if not rows:
            empty_state = QLabel(
                "起点之后没有符合条件的回测交易或潜在机会；触线、所选形态、止损关系与最低目标空间均按当前计划检查。数据共 "
                f"{sum(1 for candle in self._pending_payload.raw_candles if candle.confirmed) if self._pending_payload else 0} 根已收盘K线。"
                "可以调整两条射线或计划参数后重新复盘。",
                dialog,
            )
            empty_state.setWordWrap(True)
            layout.addWidget(empty_state)
        data_range = replay_record.get("data_range", {}) if isinstance(replay_record, dict) else {}
        data_range = data_range if isinstance(data_range, dict) else {}
        first_data = int(data_range.get("first_candle_ts", 0) or 0)
        last_data = int(data_range.get("last_candle_ts", 0) or 0)
        range_note = (
            f"数据范围 {_format_bar_time(first_data // 1000) if first_data else '—'} 至 {_format_bar_time(last_data // 1000) if last_data else '—'}；"
            "射线起点后的首次触线仅作参考、不计入回测；之后只统计已收盘K线。同一根K线同时触及止盈止损时按止损先发生。结果为纸面估算，不代表真实成交。"
        )
        layout.addWidget(QLabel(range_note, dialog))
        saved_at = str(replay_record.get("saved_at", "")) if isinstance(replay_record, dict) else ""
        report_note = QLabel(
            (f"保存时间 {saved_at}；" if saved_at else "")
            + "本报告保留复盘时的参数和价格；调整参数、射线或行情刷新不会自动更新，仅“重新复盘”成功后替换。",
            dialog,
        )
        report_note.setWordWrap(True)
        layout.addWidget(report_note)
        buttons = QHBoxLayout()
        rerun_button = QPushButton("重新复盘", dialog)
        close_button = QPushButton("关闭", dialog)
        # Enter in a table/filter must not implicitly activate recalculation.
        for button in (rerun_button, close_button, show_all_button):
            button.setAutoDefault(False)
            button.setDefault(False)

        def rerun() -> None:
            if chart_overlay() is None:
                return
            self._run_area_shape_replay(force=True)

        rerun_button.clicked.connect(rerun)
        close_button.clicked.connect(dialog.accept)
        buttons.addStretch(1)
        buttons.addWidget(rerun_button)
        buttons.addWidget(close_button)
        layout.addLayout(buttons)
        dialog.show()

    def _toggle_area_shape_future_projection(self) -> None:
        chart = self._native_chart_view
        if not isinstance(chart, InteractiveKlineChartView) or self._pending_payload is None:
            self._set_area_shape_status("当前图表不支持未来推演绘制。")
            return
        if self._area_shape_projection is not None:
            self._area_shape_projection = None
            chart.set_future_projection(None)
            self._area_shape_projection_button.setText("未来推演")
            return
        plan = self._current_area_plan()
        if plan is None:
            self._set_area_shape_status("请先创建区域计划并保存参数，再选择形态进行未来推演。")
            return
        dialog = QDialog(self)
        dialog.setWindowTitle("未来形态推演")
        form = QFormLayout(dialog)
        direction_combo = QComboBox(dialog)
        direction_combo.addItem("做多（下方支撑为入场线）", "long")
        direction_combo.addItem("做空（上方压力为入场线）", "short")
        direction_combo.setCurrentIndex(direction_combo.findData(plan.config.direction))
        pattern_combo = QComboBox(dialog)
        patterns_by_side = {
            "long": ("big_bullish", "long_lower_shadow", "double_reversal_up", "inside_bar", "bottom_fractal"),
            "short": ("big_bearish", "long_upper_shadow", "double_reversal_down", "inside_bar", "top_fractal"),
        }

        def refresh_projection_patterns() -> None:
            side = str(direction_combo.currentData())
            previous = pattern_combo.currentData()
            pattern_combo.clear()
            for pattern_id in patterns_by_side[side]:
                pattern_combo.addItem(_REPLAY_SIGNAL_LABELS[pattern_id], pattern_id)
            index = pattern_combo.findData(previous)
            if index >= 0:
                pattern_combo.setCurrentIndex(index)

        direction_combo.currentIndexChanged.connect(refresh_projection_patterns)
        refresh_projection_patterns()
        horizon = QSpinBox(dialog)
        horizon.setRange(4, 40)
        horizon.setValue(12)
        form.addRow("推演方向", direction_combo)
        form.addRow("形态", pattern_combo)
        form.addRow("推演K线根数", horizon)
        form.addRow(QLabel("推演参考近期真实K线的波动、实体与影线；关键线较远时会延长根数。仅为情景示意，不是预测。", dialog))
        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel, parent=dialog)
        buttons.accepted.connect(dialog.accept)
        buttons.rejected.connect(dialog.reject)
        form.addRow(buttons)
        if dialog.exec() != QDialog.DialogCode.Accepted or pattern_combo.currentData() is None:
            return
        projection = self._build_area_shape_projection(
            plan, str(pattern_combo.currentData()), horizon.value(), str(direction_combo.currentData()),
        )
        projection["_context_symbol"] = self._selected_symbol().strip().upper()
        projection["_context_period"] = self._period_combo.currentText().strip().upper()
        self._area_shape_projection = projection
        chart.set_future_projection(projection)
        self._area_shape_projection_button.setText("清除推演")
        if not projection.get("setup_reached", True):
            self._set_area_shape_status("关键线距当前价较远，已按近期波动推演 96 根接近行情，尚未到达所选形态位置。")
        elif len(projection.get("bars", [])) > horizon.value():
            self._set_area_shape_status(f"按近期波动将推演延长到 {len(projection['bars'])} 根，所选形态放在关键线附近。")

    def _restore_area_shape_projection(self, chart_view: object, *, symbol: str, period: str) -> None:
        projection = self._area_shape_projection
        if projection is None:
            return
        if (
            str(projection.get("_context_symbol", "")).strip().upper() != str(symbol).strip().upper()
            or str(projection.get("_context_period", "")).strip().upper() != str(period).strip().upper()
        ):
            self._area_shape_projection = None
            if self._area_shape_projection_button is not None:
                self._area_shape_projection_button.setText("未来推演")
            return
        setter = getattr(chart_view, "set_future_projection", None)
        if callable(setter):
            setter(projection)

    def _build_area_shape_projection(
        self, plan: AreaShapePlan, pattern: str, horizon: int, direction: str | None = None,
    ) -> dict[str, object]:
        candles = self._pending_payload.candles if self._pending_payload else []
        if not candles:
            return {"label": _REPLAY_SIGNAL_LABELS.get(pattern, pattern), "bars": []}
        side = direction if direction in {"long", "short"} else plan.config.direction
        latest = candles[-1]
        source = candles[-96:]
        true_ranges: list[float] = []
        previous_close = float(source[0]["open"])
        for item in source:
            high, low = float(item["high"]), float(item["low"])
            true_ranges.append(max(high - low, abs(high - previous_close), abs(low - previous_close)))
            previous_close = float(item["close"])
        recent_ranges = true_ranges[-max(3, plan.config.atr_period):]
        atr_value = max(sum(recent_ranges) / len(recent_ranges), float(latest["close"]) * 0.000001)
        entry = float(plan.support_price if side == "long" else plan.pressure_price)
        target = float(plan.pressure_price if side == "long" else plan.support_price)
        step_seconds = _display_step_ms("1H", candles) // 1000
        latest_time = int(latest["time"])
        start_price = float(latest["close"])
        bars: list[dict[str, float]] = []
        shape_bars = 3 if pattern in {"bottom_fractal", "top_fractal"} else 2 if pattern in {
            "double_reversal_up", "double_reversal_down", "false_breakdown", "false_breakout", "inside_bar",
        } else 1
        rng = random.Random(str((
            plan.symbol, side, pattern, horizon,
            [(item["time"], item["open"], item["high"], item["low"], item["close"]) for item in source],
        )))
        source_atr = max(sum(true_ranges) / len(true_ranges), 1e-12)
        scale = min(2.0, max(0.35, atr_value / source_atr))
        shape_atr = min(atr_value * rng.uniform(0.9, 1.1), entry * 0.25)
        side_sign = 1 if side == "long" else -1
        if pattern in {"bottom_fractal", "top_fractal"}:
            first_open_offset = 0.26
        elif pattern in {"double_reversal_up", "double_reversal_down", "false_breakdown", "false_breakout"}:
            first_open_offset = 0.32
        elif pattern in {"long_lower_shadow", "long_upper_shadow"}:
            first_open_offset = 0.18
        else:
            first_open_offset = 0.22 if pattern == "inside_bar" else 0.12
        approach_end = entry + side_sign * shape_atr * first_open_offset
        distance = abs(approach_end - start_price)
        required_approach = max(1, math.ceil(distance / (atr_value * 0.75)))
        setup_reached = required_approach <= 96
        approach_bars = min(96, max(1, int(horizon) - shape_bars, required_approach))
        if not setup_reached:
            approach_end = start_price + (approach_end - start_price) * approach_bars / required_approach

        # Short historical blocks preserve changes in volatility and wick
        # proportions. The seeded bridge removes the fixed pullback rhythm
        # while joining the selected setup continuously at the scenario level.
        samples: list[dict[str, Any]] = []
        while len(samples) < approach_bars:
            start_index = rng.randrange(len(source))
            samples.extend(source[(start_index + offset) % len(source)] for offset in range(rng.randint(2, 5)))
        samples = samples[:approach_bars]
        source_moves = [float(item["close"]) - float(item["open"]) for item in source]
        mean_move = sum(source_moves) / len(source_moves)
        noise_scale = max(median(abs(value) for value in source_moves) * scale, atr_value * 0.28)
        cumulative_noise = [0.0]
        last_noise = 0.0
        for item in samples:
            sampled_move = (float(item["close"]) - float(item["open"]) - mean_move) * scale
            last_noise = 0.22 * last_noise + 0.65 * sampled_move + rng.gauss(0, noise_scale) * 0.45
            last_noise = max(-atr_value * 1.4, min(atr_value * 1.4, last_noise))
            cumulative_noise.append(cumulative_noise[-1] + last_noise)
        previous_close = start_price
        for index, source_bar in enumerate(samples):
            open_price = previous_close
            progress = (index + 1) / approach_bars
            baseline = start_price + (approach_end - start_price) * progress
            noise = cumulative_noise[index + 1] - progress * cumulative_noise[-1]
            gap = min(atr_value * 0.025, shape_atr * first_open_offset * 0.2)
            if (start_price - entry) * side_sign > 0:
                clearance = max((baseline - entry) * side_sign - gap, atr_value * 1e-6)
                close = baseline + side_sign * clearance * math.tanh(noise * side_sign / clearance)
            else:
                close = max(start_price * 1e-6, baseline + noise)
            source_high, source_low = float(source_bar["high"]), float(source_bar["low"])
            source_open, source_close = float(source_bar["open"]), float(source_bar["close"])
            high_wick = max(atr_value * rng.uniform(0.015, 0.065),
                            (source_high - max(source_open, source_close)) * scale * rng.uniform(0.75, 1.25))
            low_wick = max(atr_value * rng.uniform(0.015, 0.065),
                           (min(source_open, source_close) - source_low) * scale * rng.uniform(0.75, 1.25))
            high_wick = min(high_wick, atr_value * 1.3)
            low_wick = min(low_wick, atr_value * 1.3)
            if side == "long" and start_price > entry:
                clearance = max(min(open_price, close) - entry - gap, 0.0)
                low_wick = low_wick * clearance / max(low_wick + clearance, 1e-12)
            elif side == "short" and start_price < entry:
                clearance = max(entry - gap - max(open_price, close), 0.0)
                high_wick = high_wick * clearance / max(high_wick + clearance, 1e-12)
            high = max(open_price, close) + high_wick
            low = max(min(open_price, close) - low_wick, start_price * 1e-9)
            bars.append({"time_ms": float((latest_time + step_seconds * (index + 1)) * 1000),
                         "open": open_price, "high": high, "low": low, "close": close})
            previous_close = close
        if not setup_reached:
            return {"label": f"{_REPLAY_SIGNAL_LABELS.get(pattern, pattern)} · 尚未到关键线", "bars": bars,
                    "entry": entry, "target": target, "direction": side,
                    "support": float(plan.support_price), "pressure": float(plan.pressure_price),
                    "approach_bars": approach_bars, "setup_reached": False}
        atr_value = shape_atr
        bullish = side == "long"
        direction_sign = 1.0 if bullish else -1.0

        def append_bar(open_price: float, close: float, high: float, low: float) -> None:
            index = len(bars) + 1
            bars.append({"time_ms": float((latest_time + step_seconds * index) * 1000),
                         "open": open_price, "high": max(high, open_price, close),
                         "low": min(low, open_price, close), "close": close})

        if pattern in {"bottom_fractal", "top_fractal"}:
            if bullish:
                append_bar(entry + atr_value * 0.26, entry + atr_value * 0.08, entry + atr_value * 0.36, entry - atr_value * 0.18)
                append_bar(entry + atr_value * 0.08, entry + atr_value * 0.16, entry + atr_value * 0.24, entry - atr_value * 0.65)
                append_bar(entry + atr_value * 0.16, entry + atr_value * 0.48, entry + atr_value * 0.58, entry - atr_value * 0.14)
            else:
                append_bar(entry - atr_value * 0.26, entry - atr_value * 0.08, entry + atr_value * 0.18, entry - atr_value * 0.36)
                append_bar(entry - atr_value * 0.08, entry - atr_value * 0.16, entry + atr_value * 0.65, entry - atr_value * 0.24)
                append_bar(entry - atr_value * 0.16, entry - atr_value * 0.48, entry + atr_value * 0.14, entry - atr_value * 0.58)
        elif pattern in {"double_reversal_up", "double_reversal_down", "false_breakdown", "false_breakout"}:
            if bullish:
                append_bar(entry + atr_value * 0.32, entry - atr_value * 0.20, entry + atr_value * 0.44, entry - atr_value * 0.56)
                append_bar(entry - atr_value * 0.20, entry + atr_value * 0.42, entry + atr_value * 0.56, entry - atr_value * 0.28)
            else:
                append_bar(entry - atr_value * 0.32, entry + atr_value * 0.20, entry + atr_value * 0.56, entry - atr_value * 0.44)
                append_bar(entry + atr_value * 0.20, entry - atr_value * 0.42, entry + atr_value * 0.28, entry - atr_value * 0.56)
        elif pattern in {"long_lower_shadow", "long_upper_shadow"}:
            if bullish:
                append_bar(entry + atr_value * 0.18, entry + atr_value * 0.34, entry + atr_value * 0.48, entry - atr_value * 0.78)
            else:
                append_bar(entry - atr_value * 0.18, entry - atr_value * 0.34, entry + atr_value * 0.78, entry - atr_value * 0.48)
        elif pattern == "inside_bar":
            if bullish:
                append_bar(entry + atr_value * 0.22, entry - atr_value * 0.12, entry + atr_value * 0.52, entry - atr_value * 0.46)
                append_bar(entry - atr_value * 0.12, entry + atr_value * 0.20, entry + atr_value * 0.32, entry - atr_value * 0.28)
            else:
                append_bar(entry - atr_value * 0.22, entry + atr_value * 0.12, entry + atr_value * 0.46, entry - atr_value * 0.52)
                append_bar(entry + atr_value * 0.12, entry - atr_value * 0.20, entry + atr_value * 0.28, entry - atr_value * 0.32)
        else:  # Big directional candle
            open_price = entry + direction_sign * atr_value * 0.12
            close = entry + direction_sign * atr_value * 0.98
            append_bar(open_price, close, max(open_price, close) + atr_value * 0.12, min(open_price, close) - atr_value * 0.26)
        signal_lows = [item["low"] for item in bars[-3:]]
        signal_highs = [item["high"] for item in bars[-3:]]
        stop_base = min(signal_lows) if side_sign > 0 else max(signal_highs)
        stop = stop_base - atr_value * float(plan.config.stop_offset_atr) if side_sign > 0 else stop_base + atr_value * float(plan.config.stop_offset_atr)
        return {"label": _REPLAY_SIGNAL_LABELS.get(pattern, pattern), "bars": bars,
                "entry": entry, "stop": stop, "target": target, "direction": side,
                "support": float(plan.support_price), "pressure": float(plan.pressure_price),
                "approach_bars": approach_bars}

    def _edit_area_shape_plan(self) -> None:
        self._select_rr_workspace_mode("area")
        plan = self._current_area_plan()
        if plan is None:
            self._set_area_shape_status("请先一键生成两线草稿。")
            return
        terminal_plan = plan.status in {"cancelled", "paper_closed", "expired"} and plan.paper_position is None
        if plan.status != "draft" and not terminal_plan:
            self._set_area_shape_status("计划参数已随监控冻结；已有持仓保护结束后，可新建草稿调整参数。")
            return
        dialog = AreaShapeConfigDialog(
            plan.config, self, support_price=plan.support_price, pressure_price=plan.pressure_price,
        )
        if dialog.exec() != QDialog.DialogCode.Accepted:
            return
        try:
            config = dialog.config()
            support = self._area_line_price(plan.support_line_id)
            pressure = self._area_line_price(plan.pressure_line_id)
            if terminal_plan:
                plan = AreaShapePlan.create(
                    profile_name=plan.profile_name, environment=plan.environment, symbol=plan.symbol,
                    support_price=support, pressure_price=pressure, config=config,
                    support_line_id=plan.support_line_id, pressure_line_id=plan.pressure_line_id,
                )
            else:
                plan.config = config
                plan.support_price = support
                plan.pressure_price = pressure
            self._area_shape_monitor.save_draft(plan)
            self._refresh_area_shape_status(plan)
            if terminal_plan:
                self._set_area_shape_status("已基于原区域线创建新草稿；可继续调整射线并启用计划。")
        except Exception as exc:
            self._set_area_shape_status(f"无法保存区域参数：{exc}")

    def _start_area_shape_monitoring(self) -> None:
        self._select_rr_workspace_mode("area")
        mode = str(self._area_shape_mode_combo.currentData() or "paper")
        plan = self._current_area_plan(draft_only=True)
        if plan is None:
            candidate = self._current_area_plan()
            if candidate is not None and (
                candidate.status == "paused"
                or (mode in {"demo", "live"} and candidate.execution_mode == f"{mode}_exchange" and candidate.status in {"monitoring", "touched"})
            ):
                plan = candidate
        if plan is None or self._area_shape_monitor is None:
            self._set_area_shape_status("当前账户和合约没有可启动的区域草稿。")
            return
        try:
            if self._period_combo.currentText().strip().upper() != "1H":
                raise ValueError("请切换到1H图启动监控")
            if mode in {"demo", "live"}:
                active_environment = self._active_environment().strip().lower()
                runtime_environment = str(getattr(self._runtime, "environment", "") or "").strip().lower()
                if plan.environment != mode or active_environment != mode or runtime_environment != mode:
                    if mode == "demo":
                        raise PermissionError("区域形态订单只允许OKX模拟盘；计划、顶部环境与运行时环境必须同时匹配")
                    raise PermissionError("计划、顶部环境与运行时环境必须同时匹配所选执行环境")
            retry_demo_arm = mode in {"demo", "live"} and plan.execution_mode == f"{mode}_exchange" and plan.status in {"monitoring", "touched"}
            if plan.status not in {"draft", "paused"} and not retry_demo_arm:
                paused = [
                    item for item in self._area_shape_plans()
                    if item.status == "paused" and item.symbol == self._selected_symbol().strip().upper()
                    and item.profile_name == self._active_profile_name().strip()
                    and item.environment == self._active_environment().strip().lower()
                ]
                if not paused:
                    raise ValueError("当前计划状态不能启动或恢复")
                plan = paused[-1]
            confirmed = [item for item in self._pending_payload.raw_candles if isinstance(item, Candle) and item.confirmed] if self._pending_payload is not None else []
            if not confirmed:
                raise ValueError("缺少已收盘1H K线")
            support = self._area_line_price(plan.support_line_id)
            pressure = self._area_line_price(plan.pressure_line_id)
            plan.config.validate()
            direction_text = "做多" if plan.config.direction == "long" else "做空"
            entry_line = f"下方支撑线 {support}" if plan.config.direction == "long" else f"上方压力线 {pressure}"
            target_line = f"上方压力线 {pressure}" if plan.config.direction == "long" else f"下方支撑线 {support}"
            mode_text = ("纸面演练，不发送订单" if mode == "paper" else
                         "OKX 模拟盘，可能提交模拟委托" if mode == "demo" else
                         "OKX 实盘，可能提交真实委托")
            profile_text = self._active_profile_name().strip() or "未选账户"
            if plan.config.entry_trigger_mode == "direct_touch":
                execution_text = "入场线触发 GTC 限价单"
            else:
                signal_entry = "形态确认 FOK" if plan.config.entry_mode == "next_open" else "形态确认条件限价单"
                execution_text = (f"入场线 GTC + {signal_entry}" if plan.config.resting_entry_enabled
                                 else signal_entry)
            confirmation_text = (
                f"账户：{profile_text} / {mode if mode in {'demo', 'live'} else plan.environment}\n方向：{direction_text}\n"
                f"风险预算：{plan.config.risk_amount} USDT\n执行方式：{execution_text}\n"
                f"入场参考：{entry_line}\n"
                f"盈亏比校验：{target_line}（最低 {plan.config.min_space_r}R）\n"
                f"执行：{mode_text}\n\n确认启用此计划？"
            )
            if mode in {"demo", "live"} and plan.config.resting_entry_enabled:
                confirmation_text += f"\n\n{mode_text}会同时挂入场线 GTC；它可能在 1H 形态确认前成交。"
            accepted = QMessageBox.question(
                self, "启用区域计划", confirmation_text,
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                QMessageBox.StandardButton.No,
            )
            if accepted != QMessageBox.StandardButton.Yes:
                self._set_area_shape_status("已取消启用；计划保持原状态。")
                return
            if plan.status == "draft":
                plan.support_price, plan.pressure_price = support, pressure
                self._area_shape_monitor.save_draft(plan)
            last_closed_ts = int(confirmed[-1].ts)
            if mode in {"demo", "live"}:
                runtime, profile_name = self._require_area_shape_demo_access(environment=mode)
                execution_monitor = self._area_shape_execution_monitor
                if execution_monitor is None:
                    raise RuntimeError(f"{mode_text}执行服务未连接")
                session = self._area_shape_demo_session_for_plan(plan)
                if session is not None:
                    if session.get("status") == "paused":
                        def _resume_demo_plan():
                            resumed = execution_monitor.resume_session(
                                str(session["session_id"]), runtime=runtime, unlocked=True,
                            )
                            self._area_shape_monitor.start_plan(
                                plan.plan_id, last_closed_candle_ts=last_closed_ts,
                                profile_name=profile_name, environment=plan.environment,
                                execution_mode=f"{mode}_exchange",
                            )
                            return resumed

                        self._run_area_shape_demo_action(
                            f"恢复{mode}监控",
                            _resume_demo_plan,
                        )
                        return
                    if session.get("status") in {"needs_manual_rearm", "manual_review"}:
                        raise RuntimeError("此会话重启后处于只读状态；请先执行只读对账，再显式继续保护")
                    raise RuntimeError(f"该计划已有{mode}会话（{session.get('status', 'unknown')}），请使用现有会话操作")
            started_plan = plan if retry_demo_arm else self._area_shape_monitor.start_plan(
                plan.plan_id, last_closed_candle_ts=last_closed_ts,
                profile_name=self._active_profile_name(), environment=self._active_environment(),
                execution_mode=f"{mode}_exchange" if mode in {"demo", "live"} else "paper",
            )
            self._refresh_area_shape_status()
            if mode in {"demo", "live"}:
                self._area_shape_demo_arm_pending = True
                execution_monitor.request_arm(
                    plan=started_plan, runtime=runtime, unlocked=True, paper_monitor=self._area_shape_monitor,
                    environment=mode,
                )
                self._set_area_shape_status(f"{mode}启用预检中：完成账户UID、合约与保护条件核对后才会启用新信号执行。")
                self._set_status(f"区域形态{mode}启用预检中；等待线触碰和1H形态确认，再提交价格受限FOK。")
            else:
                self._set_status("区域形态纸面监控已启动；next_open按下一根已确认1H K线开盘价假设入场，不发送订单。")
            self._refresh_area_shape_execution_status()
        except Exception as exc:
            self._area_shape_demo_arm_pending = False
            self._set_area_shape_status(f"未启动区域监控：{exc}")
            if str(self._area_shape_mode_combo.currentData() or "paper") in {"demo", "live"}:
                environment_label = "实盘" if str(self._area_shape_mode_combo.currentData()) == "live" else "模拟盘"
                self._set_area_shape_demo_error(f"{environment_label}未启用：{exc}")
                self._sync_area_shape_demo_actions()

    def _pause_area_shape_monitoring(self) -> None:
        self._select_rr_workspace_mode("area")
        plan = self._current_area_plan()
        if plan is None or self._area_shape_monitor is None:
            self._set_area_shape_status("当前没有区域形态计划。")
            return
        confirmed = QMessageBox.question(
            self, "暂停区域计划",
            "暂停后不再接受新的入场信号；OKX实盘/模拟盘会撤销并核实未成交入场单，已有仓位的交易所保护继续运行。确认暂停？",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )
        if confirmed != QMessageBox.StandardButton.Yes:
            self._set_area_shape_status("已取消暂停；计划保持原状态。")
            return
        try:
            if str(self._area_shape_mode_combo.currentData() or "paper") in {"demo", "live"}:
                session = self._area_shape_demo_session_for_plan(plan)
                if session is not None:
                    self._area_shape_execution_monitor.pause_session(str(session["session_id"]))
                    self._area_shape_monitor.pause_plan(plan.plan_id)
                    self._refresh_area_shape_execution_status(session)
                    self._set_area_shape_status("已暂停新入场；未成交入场单正在请求撤销并核实，已有仓位保护继续运行。")
                    self._refresh_area_shape_status()
                    return
            self._area_shape_monitor.pause_plan(plan.plan_id)
            self._refresh_area_shape_status()
        except Exception as exc:
            self._set_area_shape_status(f"暂停失败：{exc}")

    def _cancel_area_shape_plan(self) -> None:
        self._select_rr_workspace_mode("area")
        plan = self._current_area_plan()
        if plan is None or self._area_shape_monitor is None:
            self._set_area_shape_status("当前没有区域形态计划。")
            return
        direction_text = "做多" if plan.config.direction == "long" else "做空"
        confirmed = QMessageBox.question(
            self, "取消区域计划",
            f"取消 {direction_text} 计划后将停止后续入场。OKX实盘/模拟盘未成交入场单会请求撤销并等待 OKX 确认；已有仓位不会平仓，交易所保护继续运行。确认取消？",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )
        if confirmed != QMessageBox.StandardButton.Yes:
            self._set_area_shape_status("已取消本次操作；计划保持原状态。")
            return
        try:
            if str(self._area_shape_mode_combo.currentData() or "paper") in {"demo", "live"}:
                session = self._area_shape_demo_session_for_plan(plan)
                if session is not None:
                    if isinstance(session.get("position"), dict):
                        self._area_shape_execution_monitor.pause_session(str(session["session_id"]))
                    else:
                        self._area_shape_execution_monitor.cancel_session(str(session["session_id"]))
            self._area_shape_monitor.cancel_plan(plan.plan_id)
            self._refresh_area_shape_status()
            self._refresh_area_shape_execution_status()
            updated_plan = self._current_area_plan()
            if updated_plan is not None and updated_plan.cancel_after_position:
                self._set_area_shape_status("已提交持仓后取消；当前纸面持仓的止损/止盈继续运行，平仓后计划结束。")
                return
            session = self._area_shape_demo_session_for_plan(plan)
            if session is not None and isinstance(session.get("position"), dict):
                self._set_area_shape_status("计划已取消；已有OKX仓位未平仓，保护继续运行。")
            elif str(self._area_shape_mode_combo.currentData() or "paper") == "paper":
                self._set_area_shape_status("计划已取消；不会再生成纸面入场信号。")
            elif session is None:
                self._set_area_shape_status("计划已取消；没有关联的OKX执行会话。")
            else:
                self._set_area_shape_status("计划已取消；OKX入场撤单请求已提交，最终状态以 OKX 对账为准。")
        except Exception as exc:
            self._set_area_shape_status(f"取消失败：{exc}")

    @Slot(object)
    def _on_area_plan_updated(self, payload: object) -> None:
        if not isinstance(payload, dict):
            return
        try:
            plan = AreaShapePlan.from_dict(payload)
        except (TypeError, ValueError, KeyError):
            return
        if plan.symbol == self._selected_symbol().strip().upper() and plan.profile_name == self._active_profile_name() and plan.environment == self._active_environment():
            self._refresh_area_shape_status(plan)

    @Slot(str)
    def _on_area_monitor_status(self, status: str) -> None:
        if status:
            if self._area_shape_mode_combo.currentData() in {"demo", "live"}:
                environment_text = "实盘" if self._area_shape_mode_combo.currentData() == "live" else "模拟盘"
                self._area_shape_summary.setText(f"后台：{status}。{environment_text}形态触发由本地识别并提交价格受限FOK；本地对账成交与管理仓位保护。")
            else:
                self._area_shape_summary.setText(f"后台：{status}。纸面按下一根1H开盘价假设入场，不发送订单。")

    def _refresh_area_shape_status(self, plan: AreaShapePlan | None = None) -> None:
        target = plan or self._current_area_plan()
        if target is None:
            self._set_area_shape_status("区域形态：尚未创建计划")
            self._sync_area_shape_workflow_actions()
            details = getattr(self, "_area_shape_plan_details", None)
            if details is not None:
                details.setText("计划参数：请先创建计划并保存参数。")
            self._refresh_rr_workspace_other_status()
            lines = getattr(self, "_area_shape_lines", None)
            if lines is not None:
                lines.setText("下方支撑：待生成\n上方压力：待生成")
            self._refresh_area_shape_execution_status()
            return
        latest = target.events[0] if target.events else {}
        message = str(latest.get("message", "等待监控")) if isinstance(latest, dict) else "等待监控"
        self._set_area_shape_status(f"{target.symbol} | {target.profile_name}/{target.environment} | {target.config.direction} | {target.status}\n{message}")
        self._sync_area_shape_workflow_actions(target)
        self._refresh_rr_workspace_other_status()
        lines = getattr(self, "_area_shape_lines", None)
        if lines is not None:
            support_text = self._format_area_shape_price(target.support_price, target.symbol)
            pressure_text = self._format_area_shape_price(target.pressure_price, target.symbol)
            lines.setText(f"下方支撑：{support_text}\n上方压力：{pressure_text}")
        details = getattr(self, "_area_shape_plan_details", None)
        if details is not None:
            details.setText(self._area_shape_plan_details_text(target))
        self._refresh_area_shape_execution_status()

    def _area_shape_plan_details_text(self, plan: AreaShapePlan) -> str:
        config = plan.config
        long_side = config.direction == "long"
        direction = "做多" if long_side else "做空"
        entry_name, entry_price = (
            ("下方支撑", plan.support_price) if long_side else ("上方压力", plan.pressure_price)
        )
        target_name, target_price = (
            ("上方压力", plan.pressure_price) if long_side else ("下方支撑", plan.support_price)
        )
        patterns = "不使用形态（直接触线）" if config.entry_trigger_mode == "direct_touch" else "、".join(
            _REPLAY_SIGNAL_LABELS.get(item, item) for item in config.signal_patterns
        )
        if config.exit_template == "fixed_1r":
            exit_text = f"固定止盈 {format_decimal(config.fixed_tp_r)}R"
        else:
            exit_text = (
                f"锁盈从 {format_decimal(config.lock_trigger_r)}R 开始，先锁 {format_decimal(config.lock_profit_r)}R；"
                f"之后每增加 {format_decimal(config.lock_trail_every_r)}R 浮盈，止损上移 {format_decimal(config.lock_trail_add_r)}R"
                f"；远端{AREA_DYNAMIC_TAKE_PROFIT_R}R止盈固定保留"
            )
        if config.break_even_enabled:
            exit_text += f"；价格保本 {format_decimal(config.be_trigger_r)}R"
            if config.time_break_even_enabled:
                exit_text += f"；时间保本：持仓满{config.time_break_even_bars}根1H且当前价允许保本"
        else:
            exit_text += "；保本关闭"
        entry_mode = "做多下穿支撑/做空上穿压力即触发（纸面按线价模拟；OKX实盘/模拟盘预挂入场线GTC）" if config.entry_trigger_mode == "direct_touch" else (
            "触线+形态确认后入场（纸面按下一根1H开盘；OKX实盘/模拟盘价格受限FOK）"
            if config.entry_mode == "next_open" else "形态后等待价格突破（OKX条件单）"
        )
        paper_fee = format_decimal(config.round_trip_fee_rate * Decimal("100"))
        slippage = format_decimal(config.slippage_rate * Decimal("100"))
        target_buffer = format_decimal(config.target_buffer_atr)
        entry_offset = format_decimal(config.entry_offset_atr)
        stop_offset = format_decimal(config.stop_offset_atr)
        if config.stop_basis == "fixed_distance":
            stop_text = f"固定价差 {format_decimal(config.fixed_stop_distance)} U（相对入场线）"
        else:
            reference = "最近三根" if config.stop_basis == "three_candle" else (
                "最近已收盘" if config.entry_trigger_mode == "direct_touch" else "信号"
            )
            stop_text = f"{reference}K线高低点外 {stop_offset} ATR"
        stop_text += f" | ATR周期 {config.atr_period}" if config.requires_atr() else " | 当前组合不需ATR"
        if config.entry_trigger_mode == "pattern" and config.entry_mode == "breakout":
            stop_text += f" | 突破入场偏移 {entry_offset} ATR"
        gtc_text = "直接入场自动挂单" if config.entry_trigger_mode == "direct_touch" else (
            "开" if config.resting_entry_enabled and config.entry_mode == "next_open" else "关"
        )
        return "\n".join((
            "当前计划参数",
            f"方向：{direction} | 入场参考：{entry_name} {self._format_area_shape_price(entry_price, plan.symbol)}",
            f"盈亏比校验：{target_name} {self._format_area_shape_price(target_price, plan.symbol)} | 最低目标空间 {format_decimal(config.min_space_r)}R",
            f"1H触发形态：{patterns}",
            f"单笔风险预算：{format_decimal(config.risk_amount)} U",
            f"入场：{entry_mode} | GTC：{gtc_text}",
            f"止损：{stop_text}",
            f"离场管理：{exit_text}",
            f"成本参数：纸面往返手续费估算 {paper_fee}% | 滑点上限 {slippage}% | 目标缓冲 {target_buffer} ATR",
            f"有效期：{config.valid_for_bars} 根1H K线 | 触发价类型：{config.trigger_price_type}",
        ))

    def _format_area_shape_price(self, value: Decimal, symbol: str) -> str:
        instrument = self._instrument_for_symbol(symbol)
        tick_size = getattr(instrument, "tick_size", None)
        if isinstance(tick_size, Decimal) and tick_size > 0:
            return format_decimal_by_increment(value, tick_size)
        return format_decimal(Decimal(format_decimal_fixed(value, places=8)))

    def _set_area_shape_status(self, message: str) -> None:
        label = getattr(self, "_area_shape_status", None)
        if label is not None:
            label.setText(message)

    def _sync_area_shape_workflow_actions(self, plan: AreaShapePlan | None = None) -> None:
        plan = plan or self._current_area_plan()
        create_button = getattr(self, "_area_shape_create_button", None)
        if create_button is not None:
            pair_exists = self._area_pair_lines_exist(plan)
            can_start_new_draft = bool(
                plan is not None and plan.status in {"cancelled", "paper_closed", "expired"}
                and plan.paper_position is None and pair_exists
            )
            create_button.setText("新建草稿" if can_start_new_draft else ("两线已生成" if pair_exists else "一键两线"))
            if can_start_new_draft:
                create_button.setToolTip("沿用当前两条区域线新建草稿；旧计划记录保留，可再调整参数。")
            elif pair_exists:
                create_button.setToolTip("当前计划的两条射线已在图上，可直接拖动调整。")
            else:
                create_button.setToolTip("快速取最近120根1H K线的最低点和最高点生成两条射线；生成后可在图上手工调整。")
            create_button.setEnabled(not pair_exists or can_start_new_draft)
        price_button = getattr(self, "_area_shape_price_button", None)
        if price_button is not None:
            can_edit_prices = bool(
                plan is not None
                and plan.status == "draft"
                and self._area_pair_lines_exist(plan)
                and plan.symbol == self._selected_symbol().strip().upper()
                and self._period_combo.currentText().strip().upper() == "1H"
                and not self._area_shape_demo_arm_pending
                and not self._area_shape_demo_action_pending
            )
            price_button.setEnabled(can_edit_prices)
        start = getattr(self, "_area_shape_start_button", None)
        pause = getattr(self, "_area_shape_pause_button", None)
        cancel = getattr(self, "_area_shape_cancel_button", None)
        if start is None or pause is None or cancel is None:
            return
        busy = bool(self._area_shape_demo_arm_pending or self._area_shape_demo_action_pending)
        if plan is None:
            start.setText("启用计划")
            start.setEnabled(not busy)
            pause.setEnabled(False)
            cancel.setEnabled(False)
            return

        status = str(plan.status)
        demo_mode = self._area_shape_mode_combo.currentData() in {"demo", "live"}
        session = self._area_shape_demo_session_for_plan(plan) if demo_mode else None
        session_status = str(session.get("status", "")) if session else ""
        session_paused = bool(session.get("paused")) if session else False
        can_resume_demo = session_status == "paused" or (
            session_paused and session_status in {"protected", "position_unprotected", "tp_pending"}
        )
        start.setText("恢复计划" if status == "paused" else ("监控中" if status in {"monitoring", "touched", "paper_pending", "paper_position", "paper_break_even", "paper_locked"} else "启用计划"))
        can_start = status in {"draft", "paused"}
        if demo_mode and status == "paused" and session is not None and not can_resume_demo:
            can_start = False
        if demo_mode and status in {"monitoring", "touched"} and session_status in {"arming", "monitoring", "armed", "signal_pending", "entry_intent", "position_unprotected", "tp_pending", "protected", "closing_pending", "manual_control", "needs_manual_rearm", "manual_review"}:
            can_start = False
        start.setEnabled(can_start and not busy)
        pause.setEnabled(status in {"monitoring", "touched", "paper_pending"} and not busy)
        cancelable_statuses = {"draft", "monitoring", "touched", "paper_pending", "paused", "paper_closed", "expired"}
        paper_position_statuses = {"paper_position", "paper_break_even", "paper_locked"}
        paper_position_cancelable = (
            status in paper_position_statuses and plan.execution_mode == "paper"
            and plan.paper_position is not None and not plan.cancel_after_position
        )
        cancel.setText("持仓后取消" if status in paper_position_statuses else "取消计划")
        cancel.setEnabled(
            (status in cancelable_statuses and plan.paper_position is None or paper_position_cancelable)
            and not busy
        )
        if status in paper_position_statuses and plan.execution_mode == "paper":
            cancel.setToolTip(
                "提交持仓后取消请求；当前纸面持仓继续接受止损/止盈检查，平仓后结束计划。"
                if not plan.cancel_after_position else "已提交持仓后取消；当前纸面持仓保护仍在运行。"
            )
        elif busy:
            cancel.setToolTip("OKX执行操作正在处理中；完成后即可取消计划。")
        else:
            cancel.setToolTip("结束当前计划。不会平掉已有仓位，也不会撤销其交易所止盈止损保护。")

    def _area_shape_demo_sessions(self) -> list[dict[str, object]]:
        monitor = getattr(self, "_area_shape_execution_monitor", None)
        getter = getattr(monitor, "sessions", None)
        if not callable(getter):
            return []
        try:
            profile = self._active_profile_name().strip()
            environment = self._active_environment().strip().lower()
            symbol = self._selected_symbol().strip().upper()
            return [
                item for item in getter()
                if isinstance(item, dict)
                and str(item.get("profile_name", "")).strip() == profile
                and str(item.get("environment", "")).strip().lower() == environment
                and str(item.get("symbol", "")).strip().upper() == symbol
            ]
        except Exception as exc:
            self._set_area_shape_demo_error(f"读取OKX执行会话失败：{exc}")
            return []

    def _area_shape_demo_session_for_plan(self, plan: AreaShapePlan) -> dict[str, object] | None:
        matches = [item for item in self._area_shape_demo_sessions() if str(item.get("plan_id", "")) == plan.plan_id]
        return matches[-1] if matches else None

    def _require_area_shape_demo_access(self, *, environment: str | None = None):
        profile_name = self._active_profile_name().strip()
        active_environment = self._active_environment().strip().lower()
        expected_environment = str(environment or active_environment).strip().lower()
        runtime = self._runtime
        runtime_profile = str(getattr(runtime, "credential_profile_name", "") or "").strip() if runtime is not None else ""
        if not runtime_profile and runtime is not None:
            runtime_profile = str(getattr(getattr(runtime, "credentials", None), "profile_name", "") or "").strip()
        if not profile_name or runtime is None or runtime_profile != profile_name:
            raise RuntimeError("当前API profile与运行时不一致；请重新选择并解锁目标账户")
        selected_profile = self._api_profile_combo.currentText().strip() if hasattr(self, "_api_profile_combo") else profile_name
        if selected_profile and selected_profile != profile_name:
            raise RuntimeError("顶部选择的API profile尚未切换完成；请等待账户状态更新后重试")
        runtime_environment = str(getattr(runtime, "environment", "") or "").strip().lower()
        if (expected_environment not in {"demo", "live"} or active_environment != expected_environment
                or runtime_environment != expected_environment):
            if expected_environment == "demo":
                raise RuntimeError("区域形态订单只允许OKX模拟盘；当前运行环境不是demo")
            raise RuntimeError("当前profile运行环境与所选区域执行环境不一致")
        if not self._ensure_profile_access(profile_name):
            raise PermissionError("当前API profile尚未解锁")
        return runtime, profile_name

    def _run_area_shape_demo_action(self, label: str, action: Callable[[], object]) -> None:
        if self._area_shape_demo_action_pending:
            self._set_area_shape_demo_error("OKX执行对账/恢复仍在处理中，请等待完成。")
            return
        self._area_shape_demo_action_pending = True
        self._sync_area_shape_demo_actions()
        self._set_area_shape_demo_error(f"{label}进行中…")
        worker = AreaShapeDemoActionThread(action, self)
        self._area_shape_demo_workers.add(worker)
        worker.completed.connect(self._on_area_shape_demo_action_completed)
        worker.failed.connect(self._on_area_shape_demo_action_failed)

        def _finished() -> None:
            self._area_shape_demo_workers.discard(worker)
            worker.deleteLater()

        worker.finished.connect(_finished)
        worker.start()

    @Slot(object)
    def _on_area_shape_demo_action_completed(self, session: object) -> None:
        if bool(getattr(self, "_shutdown_requested", False)):
            return
        self._area_shape_demo_action_pending = False
        self._area_shape_demo_ui_error = ""
        if isinstance(session, dict):
            self._refresh_area_shape_execution_status(session)
        else:
            self._refresh_area_shape_execution_status()
        self._sync_area_shape_demo_actions()

    @Slot(str)
    def _on_area_shape_demo_action_failed(self, message: str) -> None:
        if bool(getattr(self, "_shutdown_requested", False)):
            return
        self._area_shape_demo_action_pending = False
        self._refresh_area_shape_execution_status()
        self._set_area_shape_demo_error(f"OKX执行操作失败：{message}")
        self._sync_area_shape_demo_actions()

    @Slot(object)
    def _on_area_demo_session_updated(self, payload: object) -> None:
        if isinstance(payload, dict):
            symbol = str(payload.get("symbol", "")).strip().upper()
            profile = str(payload.get("profile_name", "")).strip()
            environment = str(payload.get("environment", "")).strip().lower()
            if (symbol, profile, environment) == (
                self._selected_symbol().strip().upper(), self._active_profile_name().strip(),
                self._active_environment().strip().lower(),
            ):
                self._area_shape_demo_arm_pending = False
                self._area_shape_demo_ui_error = ""
                self._refresh_area_shape_execution_status(payload)
        self._sync_area_shape_demo_actions()

    @Slot(str)
    def _on_area_demo_monitor_status(self, status: str) -> None:
        if not status:
            return
        symbol = self._selected_symbol().strip().upper()
        is_arm_error = self._area_shape_demo_arm_pending and "执行启用失败" in status
        if symbol not in status.upper() and not is_arm_error:
            return
        if symbol in status.upper() or is_arm_error:
            self._area_shape_demo_arm_pending = False
        self._set_area_shape_demo_error(status)
        self._sync_area_shape_demo_actions()
        self._refresh_rr_workspace_other_status()

    @Slot(int)
    def _on_area_shape_mode_changed(self, _index: int) -> None:
        if not hasattr(self, "_area_shape_mode_combo"):
            return
        mode = self._area_shape_mode_combo.currentData()
        if mode == "paper":
            summary = "直接价格触发按方向穿越触发：做多下穿支撑、做空上穿压力，纸面按画线价模拟入场；形态确认模式按下一根1H开盘价推演。纸面不发送订单。"
        else:
            environment_label = "OKX实盘" if mode == "live" else "OKX模拟盘"
            summary = f"{environment_label}按确认框显示的入场方式执行；入场成交后先核验并保护真实成交仓位。"
        self._area_shape_summary.setText(summary)
        self._refresh_area_shape_execution_status()

    def _set_area_shape_demo_error(self, message: str) -> None:
        self._area_shape_demo_ui_error = str(message)
        label = getattr(self, "_area_shape_demo_status", None)
        if label is not None:
            label.setText(str(message))

    def _refresh_area_shape_execution_status(self, session: dict[str, object] | None = None) -> None:
        label = getattr(self, "_area_shape_demo_status", None)
        if label is None:
            return
        sessions = self._area_shape_demo_sessions()
        target = session if isinstance(session, dict) else (sessions[-1] if sessions else None)
        if target is None:
            label.setText(self._area_shape_demo_ui_error or "OKX执行：未启用")
            self._sync_area_shape_demo_actions()
            self._sync_area_shape_workflow_actions()
            self._refresh_rr_workspace_other_status()
            return
        mode_combo = getattr(self, "_area_shape_mode_combo", None)
        if mode_combo is not None and str(target.get("status", "")) not in {"closed", "cancelled", "expired"}:
            session_mode = str(target.get("environment", "demo")).lower()
            demo_index = mode_combo.findData(session_mode)
            if demo_index >= 0 and mode_combo.currentIndex() != demo_index:
                mode_combo.blockSignals(True)
                mode_combo.setCurrentIndex(demo_index)
                mode_combo.blockSignals(False)
                environment_text = "OKX实盘" if session_mode == "live" else "OKX模拟盘"
                self._area_shape_summary.setText(f"{environment_text}按冻结计划的入场方式执行；交易所执行已挂止盈止损，本程序持续管理动态保护及成交对账。")
        intent = target.get("pending_intent") if isinstance(target.get("pending_intent"), dict) else {}
        position = target.get("position") if isinstance(target.get("position"), dict) else {}
        resting = target.get("resting_entry_intent") if isinstance(target.get("resting_entry_intent"), dict) else {}
        entry = (f"OKX GTC线价单 {resting.get('price', resting.get('cl_ord_id', '待核对'))}"
                 if resting else "等待新信号") if not intent else (
            f"OKX FOK委托上限 {intent.get('price', intent.get('cl_ord_id', '待核对'))}"
            if str(intent.get("entry_kind") or "trigger") == "fok"
            else f"OKX条件委托 {intent.get('trigger_px', intent.get('cl_ord_id', '待核对'))}"
        )
        if position:
            entry = f"仓位 {position.get('size', '-')} @ {position.get('entry', '-')}"
        currently_protected = str(target.get("status", "")) == "protected"
        stop_verified = int(position.get("stop_verified_at_ms", 0) or 0) if position else 0
        tp_verified = int(position.get("take_profit_verified_at_ms", 0) or 0) if position else 0
        waiting_for_entry_protection = not position and not intent and not resting
        stop_state = (
            f"最近核验 {datetime.fromtimestamp(stop_verified / 1000).strftime('%H:%M:%S')}"
            if currently_protected and stop_verified else (
                "等待入场成交后生成" if waiting_for_entry_protection else "当前未确认/需对账"
            )
        )
        tp_state = (
            f"最近核验 {datetime.fromtimestamp(tp_verified / 1000).strftime('%H:%M:%S')}"
            if currently_protected and tp_verified else (
                "请求待核验" if position and position.get("take_profit_intent") else (
                    "等待入场成交后生成" if waiting_for_entry_protection else "未设置/当前未确认"
                )
            )
        )
        stop = f"{position.get('stop', '-')}（{stop_state}）" if position else f"-（{stop_state}）"
        take_profit = f"{position.get('take_profit', '-')}（{tp_state}）" if position else f"-（{tp_state}）"
        event_message = ""
        events = target.get("events")
        if isinstance(events, list) and events and isinstance(events[-1], dict):
            event_message = str(events[-1].get("message", "") or "")
        error = str(target.get("last_error", "") or "")
        environment_label = "实盘" if str(target.get("environment", "")).lower() == "live" else "模拟盘"
        status_text = "平仓对账中" if target.get("status") == "closing_pending" else str(target.get("status", "unknown"))
        pieces = [
            f"{environment_label} {status_text} | {target.get('profile_name', '-')}/{target.get('environment', '-')} | UID {target.get('account_uid', '-')}",
            f"{entry} | SL {stop} | TP {take_profit}",
        ]
        if position.get("pending_amend_intent"):
            pieces.append(f"动态止损待核验：目标 {position.get('pending_stop_price', '-')}；以上SL仍为上次确认价")
        if error:
            pieces.append(f"预检/执行错误：{error}")
        if event_message:
            pieces.append(event_message)
        report = target.get("readonly_reconciliation")
        if isinstance(report, dict):
            pieces.append(f"只读对账：entry={report.get('entry')} GTC={report.get('resting_entry')} position={report.get('position')} SL={report.get('stop')} TP={report.get('take_profit')}")
        label.setText("\n".join(pieces))
        self._sync_area_shape_demo_actions(target)
        self._sync_area_shape_workflow_actions()
        self._refresh_rr_workspace_other_status()

    def _sync_area_shape_demo_actions(self, session: dict[str, object] | None = None) -> None:
        reconcile = getattr(self, "_area_shape_reconcile_button", None)
        rearm = getattr(self, "_area_shape_rearm_button", None)
        if reconcile is None or rearm is None:
            return
        target = session
        if target is None:
            sessions = self._area_shape_demo_sessions()
            target = sessions[-1] if sessions else None
        status = str(target.get("status", "")) if isinstance(target, dict) else ""
        selected_demo = getattr(self, "_area_shape_mode_combo", None) is not None and self._area_shape_mode_combo.currentData() in {"demo", "live"}
        available = selected_demo and not self._area_shape_demo_action_pending
        combo = getattr(self, "_area_shape_mode_combo", None)
        if combo is not None:
            busy_statuses = {"arming", "armed", "signal_pending", "entry_intent", "position_unprotected", "tp_pending", "protected", "closing_pending", "manual_control", "needs_manual_rearm", "manual_review", "paused"}
            combo.setEnabled(not self._area_shape_demo_arm_pending and status not in busy_statuses)
        start_button = getattr(self, "_area_shape_start_button", None)
        if start_button is not None:
            start_button.setEnabled(
                not self._area_shape_demo_arm_pending
                and not self._area_shape_demo_action_pending
                and status not in {"arming", "armed", "signal_pending", "entry_intent", "position_unprotected", "tp_pending", "protected", "closing_pending", "manual_control", "needs_manual_rearm", "manual_review"}
            )
        self._sync_area_shape_workflow_actions()
        reconcile.setEnabled(available and status in {"needs_manual_rearm", "manual_review"})
        has_reconciliation = isinstance(target, dict) and bool(target.get("readonly_reconciled_at_ms"))
        rearm.setEnabled(available and status in {"needs_manual_rearm", "manual_review"} and has_reconciliation)

    def _reconcile_area_shape_demo_session(self) -> None:
        self._select_rr_workspace_mode("area")
        session = self._area_shape_demo_session_for_current_context()
        if session is None:
            self._set_area_shape_demo_error("当前账户/合约没有待对账的OKX执行会话。")
            return
        try:
            runtime, _profile = self._require_area_shape_demo_access()
            monitor = self._area_shape_execution_monitor
            self._run_area_shape_demo_action(
                "只读交易所对账",
                lambda: monitor.reconcile_readonly(str(session["session_id"]), runtime=runtime, unlocked=True),
            )
        except Exception as exc:
            self._set_area_shape_demo_error(f"不能开始只读对账：{exc}")

    def _rearm_area_shape_demo_session(self) -> None:
        self._select_rr_workspace_mode("area")
        session = self._area_shape_demo_session_for_current_context()
        if session is None:
            self._set_area_shape_demo_error("当前账户/合约没有可继续的OKX执行会话。")
            return
        try:
            runtime, _profile = self._require_area_shape_demo_access()
            monitor = self._area_shape_execution_monitor
            self._run_area_shape_demo_action(
                "恢复OKX持仓保护",
                lambda: monitor.rearm_session(str(session["session_id"]), runtime=runtime, unlocked=True),
            )
        except Exception as exc:
            self._set_area_shape_demo_error(f"不能恢复OKX持仓保护：{exc}")

    def _area_shape_demo_session_for_current_context(self) -> dict[str, object] | None:
        sessions = self._area_shape_demo_sessions()
        if not sessions:
            return None
        return sessions[-1]

    def _sync_area_shape_draft_lines(self) -> None:
        if self._area_shape_monitor is None:
            return
        profile = self._active_profile_name().strip()
        environment = self._active_environment().strip().lower()
        for plan in self._area_shape_plans():
            if plan.status != "draft" or plan.symbol != self._selected_symbol().strip().upper() or plan.period != self._period_combo.currentText().strip().upper() or plan.profile_name != profile or plan.environment != environment:
                continue
            try:
                plan.support_price = self._area_line_price(plan.support_line_id)
                plan.pressure_price = self._area_line_price(plan.pressure_line_id)
                self._area_shape_monitor.save_draft(plan)
            except Exception:
                continue

    def _reload_workspace_view(self, symbol: str | None = None, period: str | None = None) -> None:
        entry = self._workspace_entry(symbol=symbol, period=period)
        alerts = entry.get("alerts", {})
        visuals = entry.get("visuals", {})
        auto_channel = entry.get("auto_channel", {})
        ma_cross = alerts.get("ma_cross", {}) if isinstance(alerts, dict) else {}
        box_breakout = alerts.get("box_breakout", {}) if isinstance(alerts, dict) else {}
        visuals = visuals if isinstance(visuals, dict) else {}
        auto_channel = auto_channel if isinstance(auto_channel, dict) else {}
        self._ma_cross_alert_check.blockSignals(True)
        self._box_breakout_alert_check.blockSignals(True)
        self._auto_box_check.blockSignals(True)
        self._history_box_check.blockSignals(True)
        self._auto_channel_check.blockSignals(True)
        self._auto_channel_anchor_spin.blockSignals(True)
        self._auto_channel_min_bars_spin.blockSignals(True)
        self._auto_channel_violations_spin.blockSignals(True)
        self._ma_cross_alert_check.setChecked(bool(ma_cross.get("enabled", True)))
        self._box_breakout_alert_check.setChecked(bool(box_breakout.get("enabled", False)))
        self._auto_box_check.setChecked(bool(visuals.get("auto_box_visible", False)))
        self._history_box_check.setChecked(bool(visuals.get("history_box_visible", False)))
        self._auto_channel_check.setChecked(bool(visuals.get("auto_channel_visible", False)))
        self._auto_channel_anchor_spin.setValue(int(auto_channel.get("anchor_distance", 8) or 8))
        self._auto_channel_min_bars_spin.setValue(int(auto_channel.get("min_bars", 18) or 18))
        self._auto_channel_violations_spin.setValue(int(auto_channel.get("max_violations", 8) or 8))
        self._ma_cross_alert_check.blockSignals(False)
        self._box_breakout_alert_check.blockSignals(False)
        self._auto_box_check.blockSignals(False)
        self._history_box_check.blockSignals(False)
        self._auto_channel_check.blockSignals(False)
        self._auto_channel_anchor_spin.blockSignals(False)
        self._auto_channel_min_bars_spin.blockSignals(False)
        self._auto_channel_violations_spin.blockSignals(False)
        self._backend_hint.setText(
            "当前采用Qt绘图。支持：K线 | 成交量 | 形态显示 | 画线功能 | 历史箱体 | 历史仓位"
            if self._use_native_chart
            else "当前采用Web版绘图。支持：K线 | 成交量 | 形态显示 | 画线功能 | 历史箱体 | 历史仓位"
        )
        self._refresh_rr_trade_hint(symbol=symbol)
        self._populate_line_table()
        self._populate_rr_table()
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
        visuals = entry.get("visuals", {})
        if not isinstance(visuals, dict):
            visuals = {}
            entry["visuals"] = visuals
        auto_channel = entry.get("auto_channel", {})
        if not isinstance(auto_channel, dict):
            auto_channel = {}
            entry["auto_channel"] = auto_channel
        ma_cross["enabled"] = self._ma_cross_alert_check.isChecked()
        box_breakout["enabled"] = self._box_breakout_alert_check.isChecked()
        visuals["auto_box_visible"] = self._auto_box_check.isChecked()
        visuals["history_box_visible"] = self._history_box_check.isChecked()
        visuals["auto_channel_visible"] = self._auto_channel_check.isChecked()
        auto_channel["anchor_distance"] = self._auto_channel_anchor_spin.value()
        auto_channel["min_bars"] = self._auto_channel_min_bars_spin.value()
        auto_channel["max_violations"] = self._auto_channel_violations_spin.value()
        alerts["ma_cross"] = ma_cross
        alerts["box_breakout"] = box_breakout
        self._save_workspace_snapshot()

    @Slot(bool)
    def _on_auto_channel_visibility_changed(self, _enabled: bool) -> None:
        self._save_workspace_settings()
        self._sync_chart_options()
        self._load_data()

    @Slot(bool)
    def _on_auto_box_visibility_changed(self, _enabled: bool) -> None:
        self._save_workspace_settings()
        self._sync_chart_options()
        self._load_data()

    @Slot(int)
    def _on_auto_channel_parameters_changed(self, _value: int) -> None:
        self._save_workspace_settings()
        if self._auto_channel_check.isChecked():
            self._load_data()

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
            price = _line_price_table_text(item, self._current_rr_price_increment(item))
            trigger = _line_trigger_text(str(item.get("trigger", "") or ""))
            action = _line_action_text(str(item.get("action", "") or ""))
            state = _line_state_text(bool(item.get("enabled", True)))
            for column, value in enumerate((label, kind, price, trigger, action, state)):
                self._line_table.setItem(row, column, QTableWidgetItem(value))
        self._line_table.blockSignals(False)
        if target_index >= 0:
            self._line_table.setCurrentCell(target_index, 0)
            self._apply_line_record_to_form(target_index, records[target_index])
            return
        self._selected_line_index = -1
        self._line_table.clearSelection()
        self._line_price_a_edit.clear()
        self._line_price_b_edit.clear()
        self._refresh_line_price_controls(None)
        self._line_enabled_check.setChecked(True)
        self._line_email_enabled_check.setChecked(False)
        self._line_email_delivery_mode_combo.setCurrentIndex(0)
        self._refresh_line_email_controls()
        self._line_trade_enabled_check.setChecked(False)
        self._line_trade_execution_mode_combo.setCurrentIndex(0)
        self._refresh_line_trade_hint(None)

    def _populate_rr_table(self, selected_index: int | None = None) -> None:
        entry = self._workspace_entry()
        raw_rr = entry.get("rr", [])
        records = list(raw_rr) if isinstance(raw_rr, list) else []
        price_increment = self._current_rr_price_increment()
        target_index = self._selected_rr_index if selected_index is None else selected_index
        if target_index < 0 or target_index >= len(records):
            target_index = -1
        self._rr_table.blockSignals(True)
        self._rr_table.setRowCount(len(records))
        for row, item in enumerate(records):
            record = item if isinstance(item, dict) else {}
            side = str(record.get("side", "") or "").strip().lower()
            values = (
                "空头" if side == "short" else "多头",
                _format_rr_table_price(record.get("price_entry", ""), price_increment),
                _format_rr_table_price(record.get("price_stop", ""), price_increment),
                _format_rr_table_price(record.get("price_tp", ""), price_increment),
                _rr_management_mode_text(record.get("management_mode")),
                str(record.get("r_multiple", "") or ""),
                str(record.get("bar_entry", "") or ""),
                "是" if bool(record.get("locked", False)) else "否",
                _format_rr_table_price(record.get("risk_amount", "100")) or "100",
            )
            for column, value in enumerate(values):
                item_widget = QTableWidgetItem(value)
                if column in {1, 2, 3, 5, 6, 8}:
                    item_widget.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
                elif column == 7:
                    item_widget.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self._rr_table.setItem(row, column, item_widget)
        self._rr_table.blockSignals(False)
        if target_index >= 0:
            self._rr_table.setCurrentCell(target_index, 0)
            record = records[target_index] if isinstance(records[target_index], dict) else {}
            self._apply_rr_record_to_form(target_index, record)
            return
        self._selected_rr_index = -1
        self._rr_table.clearSelection()
        self._rr_side_combo.setCurrentIndex(0)
        self._rr_management_mode_combo.setCurrentIndex(0)
        self._rr_entry_edit.clear()
        self._rr_stop_edit.clear()
        self._rr_r_edit.setValue(2.0)
        self._rr_risk_edit.setText("100")
        self._rr_bar_edit.setText("0")
        self._rr_fee_offset_check.setChecked(False)
        self._rr_locked_check.setChecked(False)
        self._rr_preview.setText("止盈会按入场、止损和 R 倍数自动计算。")
        self._refresh_rr_tracking_summary(None)

    @Slot(int, int)
    def _on_rr_table_cell_clicked(self, row: int, _column: int) -> None:
        if row < 0:
            return
        self._select_rr_workspace_mode("direct")
        self._populate_rr_table(selected_index=row)

    @Slot(int, int)
    def _on_rr_table_cell_double_clicked(self, row: int, _column: int) -> None:
        if row < 0:
            return
        self._select_rr_workspace_mode("direct")
        self._populate_rr_table(selected_index=row)
        self._open_rr_card_for_selected()

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

    @Slot()
    def _on_rr_selected(self) -> None:
        row = self._rr_table.currentRow()
        entry = self._workspace_entry()
        raw_rr = entry.get("rr", [])
        records = list(raw_rr) if isinstance(raw_rr, list) else []
        if row < 0 or row >= len(records):
            self._selected_rr_index = -1
            if self._pending_payload is not None:
                self._render_to_chart(self._pending_payload)
            return
        item = records[row] if isinstance(records[row], dict) else {}
        self._apply_rr_record_to_form(row, item)
        if self._pending_payload is not None:
            self._render_to_chart(self._pending_payload)

    def _apply_line_record_to_form(self, row: int, item: dict[str, object]) -> None:
        self._selected_line_index = row
        self._line_label_edit.setText(str(item.get("label", "") or ""))
        price_increment = self._current_rr_price_increment(item)
        self._line_price_a_edit.setText(_format_rr_table_price(item.get("price_a", ""), price_increment))
        self._line_price_b_edit.setText(_format_rr_table_price(item.get("price_b", ""), price_increment))
        self._refresh_line_price_controls(item)
        self._line_trigger_combo.setCurrentIndex(max(0, self._line_trigger_combo.findData(item.get("trigger"))))
        self._line_action_combo.setCurrentIndex(max(0, self._line_action_combo.findData(item.get("action"))))
        self._line_enabled_check.setChecked(bool(item.get("enabled", True)))
        self._line_email_enabled_check.setChecked(bool(item.get("email_enabled", False)))
        self._line_email_delivery_mode_combo.setCurrentIndex(
            max(0, self._line_email_delivery_mode_combo.findData(str(item.get("email_delivery_mode", "once") or "once")))
        )
        self._refresh_line_email_controls()
        self._line_trade_enabled_check.setChecked(_rr_fee_offset_enabled(item.get("trade_enabled", False)))
        self._line_trade_execution_mode_combo.setCurrentIndex(
            max(0, self._line_trade_execution_mode_combo.findData(str(item.get("entry_execution_mode", "limit") or "limit")))
        )
        self._refresh_line_trade_hint(item)

    def _refresh_line_price_controls(self, item: dict[str, object] | None) -> None:
        kind = str(item.get("kind", "horizontal") or "horizontal").strip().lower() if isinstance(item, dict) else "horizontal"
        is_trend = kind == "trend"
        is_ray = kind == "ray"
        self._line_price_a_label.setText("起点价" if is_trend else ("射线价格" if is_ray else "价格"))
        self._line_price_a_edit.setPlaceholderText("起点价" if is_trend else ("射线价格" if is_ray else "价格"))
        self._line_price_b_label.setVisible(is_trend)
        self._line_price_b_edit.setVisible(is_trend)

    def _refresh_line_email_controls(self) -> None:
        is_notify = str(self._line_action_combo.currentData() or "notify") == "notify"
        self._line_email_enabled_check.setEnabled(is_notify)
        self._line_email_delivery_mode_combo.setEnabled(is_notify and self._line_email_enabled_check.isChecked())

    def _refresh_line_trade_hint(self, item: dict[str, object] | None) -> None:
        if not hasattr(self, "_line_trade_hint"):
            return
        if not isinstance(item, dict):
            self._line_trade_hint.setText("线条交易默认关闭：需同时启用当前线条、启用线条交易和全局线条交易。")
            return
        action = str(item.get("action", "notify") or "notify").strip().lower()
        if action not in {"long", "short"}:
            self._line_trade_hint.setText("当前线条为提醒模式，不会下单。请选择做多或做空后再配置交易参数。")
            return
        enabled = _rr_fee_offset_enabled(item.get("trade_enabled", False))
        risk = _format_rr_table_price(item.get("risk_amount", "100")) or "100"
        stop = _format_rr_table_price(item.get("stop_loss_price", "")) or "未设置"
        r_multiple = _format_rr_table_price(item.get("direct_take_profit_r", "2")) or "2"
        armed = "已开启" if self._line_trade_armed_check.isChecked() else "未开启"
        self._line_trade_hint.setText(
            f"线条交易：{'已启用' if enabled else '未启用'} | 风险 {risk} | 止损 {stop} | R {r_multiple} | 全局开关：{armed}"
        )

    @Slot(bool)
    def _on_line_trade_armed_toggled(self, enabled: bool) -> None:
        if not enabled:
            self._refresh_line_trade_hint(self._selected_line_payload_or_none())
            return
        confirmed = QMessageBox.question(
            self,
            "启用全局线条交易",
            "开启后，满足线条条件的做多/做空预警会自动向当前 API 提交订单。\n\n"
            "仍需线条本身启用且勾选“启用线条交易”。是否确认开启？",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )
        if confirmed != QMessageBox.StandardButton.Yes:
            self._line_trade_armed_check.blockSignals(True)
            self._line_trade_armed_check.setChecked(False)
            self._line_trade_armed_check.blockSignals(False)
            self._set_status("已取消开启全局线条交易。")
        else:
            self._set_status("全局线条交易已开启，请确认当前 API 与环境。")
        self._refresh_line_trade_hint(self._selected_line_payload_or_none())

    def _selected_line_payload_or_none(self) -> dict[str, object] | None:
        entry = self._workspace_entry()
        lines = entry.get("lines", [])
        if not isinstance(lines, list) or not (0 <= self._selected_line_index < len(lines)):
            return None
        item = lines[self._selected_line_index]
        return dict(item) if isinstance(item, dict) else None

    def _open_line_trade_card_for_selected(self) -> None:
        line = self._selected_line_payload_or_none()
        if line is None:
            self._set_status("请先选中一条线，再配置交易参数。")
            return
        side = str(line.get("action", "notify") or "notify").strip().lower()
        if side not in {"long", "short"}:
            self._set_status("线条动作需选择做多或做空，提醒模式不能配置交易。")
            return
        candle_time = int(line.get("time_b", 0) or line.get("time_a", 0) or 0)
        if self._pending_payload is not None and self._pending_payload.candles:
            candle_time = int(self._pending_payload.candles[-1].get("time", candle_time) or candle_time)
        entry_price = Decimal(str(line_value_at(line, candle_time)))
        stop_price = _parse_rr_optional_decimal(line.get("stop_loss_price"))
        if stop_price is None or stop_price <= 0:
            stop_price = entry_price * (Decimal("0.99") if side == "long" else Decimal("1.01"))
        item = {
            "rr_id": f"line-{str(line.get('id', '') or '')}",
            "side": side,
            "management_mode": line.get("management_mode", "fixed_tp"),
            "price_entry": decimal_to_text(entry_price),
            "price_stop": decimal_to_text(stop_price),
            "r_multiple": line.get("direct_take_profit_r", "2"),
            "risk_amount": line.get("risk_amount", "100"),
            "fee_offset_enabled": line.get("fee_offset_enabled", False),
            "locked": True,
        }
        dialog = RRCardDialog(
            parent=self,
            item=item,
            instrument=self._instrument_for_symbol(),
            symbol=self._selected_symbol(),
            period=self._period_combo.currentText().strip(),
            price_increment=self._current_rr_price_increment(),
        )
        dialog.setWindowTitle(f"线条交易参数 | {str(line.get('label', '') or '线条')}")
        dialog._side_combo.setEnabled(False)
        dialog._entry_edit.setReadOnly(True)
        dialog._locked_check.setVisible(False)
        if dialog.exec() != int(QDialog.DialogCode.Accepted):
            return
        result = dialog.result_payload()
        if not isinstance(result, dict):
            return
        entry = self._workspace_entry()
        lines = entry.get("lines", [])
        if not isinstance(lines, list) or not (0 <= self._selected_line_index < len(lines)):
            return
        updated = dict(lines[self._selected_line_index])
        updated.update(
            {
                "risk_amount": result.get("risk_amount", "100"),
                "stop_loss_price": result.get("price_stop", ""),
                "direct_take_profit_r": result.get("direct_take_profit_r", "2"),
                "management_mode": result.get("management_mode", "fixed_tp"),
                "entry_execution_mode": str(self._line_trade_execution_mode_combo.currentData() or "limit"),
                "fee_offset_enabled": result.get("fee_offset_enabled", False),
            }
        )
        lines[self._selected_line_index] = updated
        self._save_workspace_snapshot()
        self._populate_line_table(selected_index=self._selected_line_index)
        self._set_status(f"已更新线条交易参数：{updated.get('label', '')}")

    def _apply_rr_record_to_form(self, row: int, item: dict[str, object]) -> None:
        self._selected_rr_index = row
        side = str(item.get("side", "long") or "long").strip().lower()
        price_increment = self._current_rr_price_increment(item)
        self._rr_side_combo.setCurrentIndex(1 if side == "short" else 0)
        self._rr_management_mode_combo.setCurrentIndex(
            max(0, self._rr_management_mode_combo.findData(_normalize_rr_management_mode(item.get("management_mode"))))
        )
        self._rr_entry_edit.setText(_format_rr_table_price(item.get("price_entry", ""), price_increment))
        self._rr_stop_edit.setText(_format_rr_table_price(item.get("price_stop", ""), price_increment))
        self._rr_r_edit.setValue(float(str(item.get("r_multiple", "") or "2")))
        self._rr_risk_edit.setText(str(item.get("risk_amount", "100") or "100"))
        self._rr_bar_edit.setText(str(item.get("bar_entry", "") or "0"))
        self._rr_execution_mode_combo.setCurrentIndex(
            max(0, self._rr_execution_mode_combo.findData(str(item.get("entry_execution_mode", "limit") or "limit")))
        )
        self._rr_fee_offset_check.setChecked(_rr_fee_offset_enabled(item.get("fee_offset_enabled", False)))
        self._rr_locked_check.setChecked(bool(item.get("locked", False)))
        price_tp_text = _format_rr_table_price(item.get("price_tp", ""), price_increment) or "-"
        self._rr_preview.setText(f"自动止盈：{price_tp_text}")
        self._refresh_rr_tracking_summary(item)

    def _selected_rr_payload(self) -> dict[str, object]:
        entry = self._workspace_entry()
        records = entry.get("rr", [])
        if not isinstance(records, list) or not (0 <= self._selected_rr_index < len(records)):
            raise RuntimeError("请先选中一个 RR 区块。")
        payload = records[self._selected_rr_index]
        if not isinstance(payload, dict):
            raise RuntimeError("当前 RR 数据无效。")
        return dict(payload)

    def _build_selected_rr_trade_plan(self):
        runtime = self._runtime
        if runtime is None:
            raise RuntimeError("未加载可用 API，请先选择账户。")
        payload = self._selected_rr_payload()
        instrument = self._instrument_for_symbol()
        if instrument is None or str(getattr(instrument, "inst_type", "") or "").upper() != "SWAP":
            raise RuntimeError("第一阶段 RR 交易只支持永续合约（SWAP）。")
        profile_name = self._active_profile_name()
        environment = self._active_environment()
        if not profile_name or not environment:
            raise RuntimeError("当前 API 账户或环境无效。")
        rr_id = str(payload.get("rr_id", "") or "").strip()
        trade_ref = str(payload.get("trade_ref", "") or "").strip()
        if not rr_id:
            raise RuntimeError("RR 缺少标识，请先保存该 RR。")
        risk_amount = _parse_rr_optional_decimal(payload.get("risk_amount")) or Decimal("100")
        entry_price = self._parse_rr_decimal(str(payload.get("price_entry", "") or ""), "入场价")
        stop_price = self._parse_rr_decimal(str(payload.get("price_stop", "") or ""), "止损价")
        take_profit = self._parse_rr_decimal(str(payload.get("price_tp", "") or ""), "止盈价")
        r_multiple = self._parse_rr_decimal(str(payload.get("r_multiple", "") or ""), "R 倍数")
        round_trip_fee_rate = Decimal("0")
        if _rr_fee_offset_enabled(payload.get("fee_offset_enabled", False)):
            round_trip_fee_rate = _dynamic_two_taker_fee_offset_live(entry_price, enabled=True) / entry_price
        plan = build_rr_trade_plan(
            plan_id=f"{profile_name}:{str(getattr(instrument, 'inst_id', '') or '')}:{trade_ref or rr_id}",
            profile_name=profile_name,
            environment=environment,
            instrument=instrument,
            direction="short" if str(payload.get("side", "") or "").lower() == "short" else "long",
            entry_execution_mode=str(self._rr_execution_mode_combo.currentData() or "limit"),
            management_mode=_normalize_rr_management_mode(payload.get("management_mode")),
            trigger_price_type="last",
            risk_amount=risk_amount,
            entry_price=entry_price,
            stop_loss_price=stop_price,
            direct_take_profit_r=r_multiple,
            round_trip_fee_rate=round_trip_fee_rate,
        )
        return replace(plan, take_profit_price=take_profit)

    def _build_line_trade_plan_from_event(self, event: dict[str, object]):
        if str(event.get("kind", "") or "") != "line_alert":
            raise RuntimeError("当前事件不是线条触发事件。")
        line_id = str(event.get("line_id", "") or "").strip()
        if not line_id:
            raise RuntimeError("线条触发事件缺少线条标识。")
        entry = self._workspace_entry()
        lines = entry.get("lines", [])
        line_record = next(
            (
                item
                for item in lines
                if isinstance(item, dict) and str(item.get("id", "") or "").strip() == line_id
            ),
            None,
        )
        if not isinstance(line_record, dict):
            raise RuntimeError("触发线条已不存在，未提交交易。")
        line = dict(line_record)
        action = str(line.get("action", "notify") or "notify").strip().lower()
        if action not in {"long", "short"}:
            raise RuntimeError("线条动作不是做多或做空，未提交交易。")
        if not bool(line.get("enabled", True)) or not _rr_fee_offset_enabled(line.get("trade_enabled", False)):
            raise RuntimeError("线条交易未启用，未提交交易。")
        instrument = self._instrument_for_symbol()
        if instrument is None or str(getattr(instrument, "inst_type", "") or "").upper() != "SWAP":
            raise RuntimeError("第一阶段线条交易只支持永续合约（SWAP）。")
        profile_name = str(line.get("trade_profile_name", "") or "").strip() or self._active_profile_name()
        environment = str(line.get("trade_environment", "") or "").strip() or self._active_environment()
        if not profile_name or not environment:
            raise RuntimeError("当前 API 账户或环境无效。")
        if not str(line.get("trade_profile_name", "") or "").strip():
            line_record["trade_profile_name"] = profile_name
            line_record["trade_environment"] = environment
            self._save_workspace_snapshot()
        candle_time = int(event.get("candle_time", 0) or 0)
        if candle_time <= 0:
            raise RuntimeError("线条触发事件缺少 K 线时间。")
        entry_price = Decimal(str(line_value_at(line, candle_time)))
        stop_price = self._parse_rr_decimal(str(line.get("stop_loss_price", "") or ""), "线条止损价")
        risk_amount = self._parse_rr_decimal(str(line.get("risk_amount", "") or ""), "风险金额")
        direct_take_profit_r = self._parse_rr_decimal(str(line.get("direct_take_profit_r", "") or ""), "止盈 R 倍数")
        entry_execution_mode = str(line.get("entry_execution_mode", "limit") or "limit").strip().lower()
        if entry_execution_mode not in {"limit", "market", "chase_best_quote"}:
            entry_execution_mode = "limit"
        round_trip_fee_rate = Decimal("0")
        if _rr_fee_offset_enabled(line.get("fee_offset_enabled", False)):
            round_trip_fee_rate = _dynamic_two_taker_fee_offset_live(entry_price, enabled=True) / entry_price
        return build_rr_trade_plan(
            plan_id=f"{profile_name}:{str(getattr(instrument, 'inst_id', '') or '')}:{line_id}:{candle_time}",
            profile_name=profile_name,
            environment=environment,
            instrument=instrument,
            direction=action,
            entry_execution_mode=entry_execution_mode,
            management_mode=_normalize_rr_management_mode(line.get("management_mode")),
            trigger_price_type="last",
            risk_amount=risk_amount,
            entry_price=entry_price,
            stop_loss_price=stop_price,
            direct_take_profit_r=direct_take_profit_r,
            round_trip_fee_rate=round_trip_fee_rate,
        )

    def _build_armed_line_trade_plans(self, events: list[dict[str, object]]) -> list[RRTradePlan]:
        if not self._line_trade_armed_check.isChecked():
            return []
        existing_ids = {
            str(item.plan.plan_id or "")
            for item in self._matching_rr_trade_ledger_entries()
        }
        queued_ids = {str(item.plan_id or "") for item in self._line_trade_execution_queue}
        plans: list[RRTradePlan] = []
        for event in events:
            if not isinstance(event, dict):
                continue
            if str(event.get("kind", "") or "") != "line_alert":
                continue
            if not _rr_fee_offset_enabled(event.get("trade_enabled", False)):
                continue
            if str(event.get("trade_action", "notify") or "notify").strip().lower() not in {"long", "short"}:
                continue
            try:
                plan = self._build_line_trade_plan_from_event(event)
            except Exception as exc:  # noqa: BLE001
                self._set_status(f"线条交易未提交：{exc}")
                continue
            if plan.plan_id in existing_ids or plan.plan_id in queued_ids:
                continue
            plans.append(plan)
            queued_ids.add(plan.plan_id)
        return plans

    def _find_rr_trade_ledger_entry(self, plan_id: str) -> RRTradeLedgerEntry | None:
        for item in self._all_rr_trade_ledger_entries():
            if item.plan.plan_id == plan_id:
                return item
        return None

    def _rr_trade_binding_plan_id(self, item: dict[str, object] | None) -> str:
        if not isinstance(item, dict):
            return ""
        binding = item.get("trade_binding")
        if not isinstance(binding, dict):
            return ""
        return str(binding.get("plan_id", "") or "").strip()

    def _find_rr_item_index_for_entry(self, entry: RRTradeLedgerEntry) -> int:
        plan_id = str(entry.plan.plan_id or "").strip()
        entry_trade_ref = plan_id.rsplit(":", 1)[-1] if ":" in plan_id else plan_id
        records = self._workspace_entry().get("rr", [])
        if not isinstance(records, list):
            return -1
        for index, item in enumerate(records):
            if not isinstance(item, dict):
                continue
            if self._rr_trade_binding_plan_id(item) == plan_id:
                return index
        for index, item in enumerate(records):
            if not isinstance(item, dict):
                continue
            trade_ref = str(item.get("trade_ref", "") or "").strip()
            if trade_ref and trade_ref == entry_trade_ref:
                return index
        for index, item in enumerate(records):
            if not isinstance(item, dict):
                continue
            trade_ref = str(item.get("trade_ref", "") or "").strip()
            if trade_ref:
                continue
            rr_id = str(item.get("rr_id", "") or "").strip()
            if rr_id and plan_id.endswith(f":{rr_id}") and self._legacy_rr_plan_matches_item(entry, item):
                return index
        return -1

    def _bind_rr_trade_entry(self, entry: RRTradeLedgerEntry) -> None:
        index = self._find_rr_item_index_for_entry(entry)
        records = self._workspace_entry().get("rr", [])
        if index < 0 or not isinstance(records, list) or index >= len(records):
            return
        existing = records[index]
        if not isinstance(existing, dict):
            return
        plan_id = str(entry.plan.plan_id or "").strip()
        trade_ref = str(existing.get("trade_ref", "") or "").strip()
        if not trade_ref and ":" in plan_id:
            trade_ref = plan_id.rsplit(":", 1)[-1]
        binding_payload = {
            "plan_id": plan_id,
            "status": str(entry.status or "").strip(),
            "entry_order_id": str(getattr(entry.entry_order, "order_id", "") or "").strip(),
            "entry_client_id": str(getattr(entry.entry_order, "client_id", "") or "").strip(),
            "stop_loss_algo_id": str(getattr(entry.stop_loss_order, "algo_id", "") or "").strip(),
            "stop_loss_client_id": str(getattr(entry.stop_loss_order, "client_id", "") or "").strip(),
            "take_profit_algo_id": str(getattr(entry.take_profit_order, "algo_id", "") or "").strip(),
            "take_profit_client_id": str(getattr(entry.take_profit_order, "client_id", "") or "").strip(),
        }
        updated = dict(existing)
        updated["trade_ref"] = trade_ref
        updated["trade_binding"] = binding_payload
        if updated == existing:
            return
        records[index] = updated
        self._save_workspace_snapshot()

    def _save_rr_trade_ledger_entry(self, entry: RRTradeLedgerEntry) -> None:
        snapshot = self._rr_trade_ledger_snapshot if isinstance(self._rr_trade_ledger_snapshot, dict) else {}
        records = list(snapshot.get("entries", [])) if isinstance(snapshot.get("entries"), list) else []
        saved = False
        normalized_records: list[dict[str, object]] = []
        for raw in records:
            if not isinstance(raw, dict):
                continue
            try:
                existing = RRTradeLedgerEntry.from_dict(raw)
            except Exception:
                continue
            if existing.entry_id == entry.entry_id:
                normalized_records.append(entry.to_dict())
                saved = True
            else:
                normalized_records.append(raw)
        if not saved:
            normalized_records.append(entry.to_dict())
        self._rr_trade_ledger_snapshot = {"entries": normalized_records}
        save_kline_rr_trade_ledger_snapshot(normalized_records)
        self._bind_rr_trade_entry(entry)
        self._refresh_rr_trade_hint()
        records = self._workspace_entry().get("rr", [])
        if isinstance(records, list) and 0 <= self._selected_rr_index < len(records):
            selected = records[self._selected_rr_index]
            if isinstance(selected, dict):
                self._refresh_rr_tracking_summary(selected)

    def _drop_rr_trade_ledger_entry(self, plan_id: str) -> bool:
        target_plan_id = str(plan_id or "").strip()
        if not target_plan_id:
            return False
        snapshot = self._rr_trade_ledger_snapshot if isinstance(self._rr_trade_ledger_snapshot, dict) else {}
        records = list(snapshot.get("entries", [])) if isinstance(snapshot.get("entries"), list) else []
        normalized_records: list[dict[str, object]] = []
        removed = False
        for raw in records:
            if not isinstance(raw, dict):
                continue
            try:
                existing = RRTradeLedgerEntry.from_dict(raw)
            except Exception:
                continue
            if str(existing.plan.plan_id or "").strip() == target_plan_id:
                removed = True
                continue
            normalized_records.append(raw)
        if not removed:
            return False
        self._rr_trade_ledger_snapshot = {"entries": normalized_records}
        save_kline_rr_trade_ledger_snapshot(normalized_records)
        return True

    def _start_rr_execution_action(
        self,
        *,
        action: Callable[[], RRTradeLedgerEntry],
        on_success: Callable[[RRTradeLedgerEntry], None],
        on_failure: Callable[[str], None] | None = None,
        queue_if_busy: bool = False,
        queued_status_text: str | None = None,
    ) -> bool:
        if self._rr_execution_in_flight:
            if queue_if_busy:
                self._pending_rr_execution_requests.append(
                    {
                        "action": action,
                        "on_success": on_success,
                        "on_failure": on_failure,
                    }
                )
                self._set_status(queued_status_text or "当前 RR 请求已排队，等待前一个任务完成后自动执行。")
                return False
            self._set_status("RR 交易请求正在执行，请等待当前请求完成。")
            return False
        self._rr_execution_in_flight = True
        thread = RRTradeExecutionThread(action, self)
        self._rr_execution_thread = thread

        def _completed(entry: object) -> None:
            if not isinstance(entry, RRTradeLedgerEntry):
                self._set_status("RR 交易返回了无效结果。")
                if on_failure is not None:
                    on_failure("RR 交易返回了无效结果。")
                return
            if self._find_rr_trade_ledger_entry(entry.plan.plan_id) != entry:
                self._save_rr_trade_ledger_entry(entry)
            on_success(entry)

        def _failed(message: str) -> None:
            self._set_status(f"RR 交易失败：{message}")
            if on_failure is not None:
                on_failure(message)

        def _finished() -> None:
            self._rr_execution_in_flight = False
            if self._rr_execution_thread is thread:
                self._rr_execution_thread = None
            thread.deleteLater()
            self._start_next_pending_rr_execution_request()
            if not self._rr_execution_in_flight:
                self._start_next_line_trade_execution()

        thread.completed.connect(_completed)
        thread.failed.connect(_failed)
        thread.finished.connect(_finished)
        thread.start()
        return True

    def _start_next_pending_rr_execution_request(self) -> None:
        if self._rr_execution_in_flight:
            return
        if not self._pending_rr_execution_requests:
            return
        request = self._pending_rr_execution_requests.pop(0)
        action = request.get("action")
        on_success = request.get("on_success")
        on_failure = request.get("on_failure")
        if not callable(action) or not callable(on_success):
            self._start_next_pending_rr_execution_request()
            return
        self._start_rr_execution_action(
            action=action,
            on_success=on_success,
            on_failure=on_failure if callable(on_failure) else None,
        )

    def _enqueue_line_trade_events(self, events: list[dict[str, object]]) -> None:
        plans = self._build_armed_line_trade_plans(events)
        if not plans:
            return
        self._line_trade_execution_queue.extend(plans)
        self._start_next_line_trade_execution()

    def _start_next_line_trade_execution(self) -> None:
        if self._rr_execution_in_flight:
            return
        if not self._line_trade_armed_check.isChecked():
            self._line_trade_execution_queue.clear()
            return
        if not self._line_trade_execution_queue:
            return
        plan = self._line_trade_execution_queue.pop(0)
        runtime = self._runtime_for_task_profile(plan.profile_name)
        if runtime is None:
            self._set_status(f"线条交易未提交：API {plan.profile_name} 不可用。")
            self._start_next_line_trade_execution()
            return
        if self._find_rr_trade_ledger_entry(plan.plan_id) is not None:
            self._start_next_line_trade_execution()
            return
        self._set_status(f"线条触发交易提交中：{plan.inst_id} | {plan.direction}")
        self._start_rr_execution_action(
            action=lambda: self._rr_trade_execution_service.activate(
                client=OkxRestClient(),
                credentials=runtime.credentials,
                config=_build_strategy_config(plan.inst_id, runtime),
                plan=plan,
            ),
            on_success=lambda entry: self._handle_line_trade_execution_result(entry),
            on_failure=lambda _message: self._start_next_line_trade_execution(),
        )

    def _handle_line_trade_execution_result(self, entry: RRTradeLedgerEntry) -> None:
        self._set_status(f"线条交易已启用：{entry.plan.inst_id} | {entry.status}")
        self._start_next_line_trade_execution()

    @Slot()
    def _monitor_active_rr_trades(self) -> None:
        if self._rr_execution_in_flight or self._pending_rr_execution_requests:
            return
        entry = self._next_monitorable_rr_entry()
        if entry is None:
            return
        runtime = self._runtime_for_task_profile(entry.plan.profile_name)
        if runtime is None:
            self._set_status(f"RR 监控等待：API {entry.plan.profile_name} 不可用。")
            return
        self._start_rr_execution_action(
            action=lambda: self._rr_trade_execution_service.reconcile(
                client=self._rr_client(runtime),
                credentials=runtime.credentials,
                config=_build_strategy_config(entry.plan.inst_id, runtime),
                entry=entry,
            ),
            on_success=lambda _updated: None,
        )

    @Slot()
    def _enable_selected_rr_trade(self) -> None:
        try:
            plan = self._build_selected_rr_trade_plan()
            if plan.plan_id in self._pending_rr_activation_plan_ids:
                raise RuntimeError("该 RR 的启用请求正在处理，请勿重复提交。")
            existing = self._find_rr_trade_ledger_entry(plan.plan_id)
            if existing is not None and existing.status not in {"cancelled", "manual_review"}:
                raise RuntimeError(f"该 RR 已有交易记录，当前状态：{existing.status}。")
            confirmed = QMessageBox.question(
                self,
                "启用 RR 交易",
                (
                    f"合约：{plan.inst_id}\n方向：{'多头' if plan.direction == 'long' else '空头'}\n"
                    f"入场方式：{plan.entry_execution_mode}\n数量：{_rr_plan_position_text(plan)}\n"
                    f"止损：{format_decimal(plan.stop_loss_price)}\n止盈：{format_decimal(plan.take_profit_price)}\n\n"
                    "确认后才会向当前 API 提交订单。"
                ),
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                QMessageBox.StandardButton.No,
            )
            if confirmed != QMessageBox.StandardButton.Yes:
                self._set_status("已取消启用 RR 交易。")
                return
            plan_profile = str(getattr(plan, "profile_name", "") or "").strip()
            runtime = self._runtime_for_task_profile(plan_profile) if plan_profile else self._runtime
            if runtime is None:
                raise RuntimeError(f"API {plan.profile_name} 不可用。")
        except Exception as exc:  # noqa: BLE001
            self._set_status(f"无法启用 RR 交易：{exc}")
            return

        requested_at = time.perf_counter()
        self._pending_rr_activation_plan_ids.add(plan.plan_id)
        self._set_rr_activation_status("正在提交 RR 交易…", pending=True)
        self._start_rr_execution_action(
            action=lambda: self._activate_rr_trade(plan, runtime, requested_at),
            on_success=lambda entry: self._handle_rr_activation_success(entry, runtime, requested_at),
            on_failure=lambda message: self._handle_rr_activation_failure(plan.plan_id, message),
            queue_if_busy=True,
            queued_status_text="当前正在同步已报 RR，新的启用请求已排队，稍后自动提交。",
        )
        if self._rr_execution_in_flight and self._pending_rr_execution_requests:
            self._set_rr_activation_status("RR 启用请求已排队，当前任务结束后优先提交。", pending=True)

    def _rr_client(self, runtime) -> OkxRestClient:
        credentials = runtime.credentials
        key = (
            str(getattr(credentials, "profile_name", "") or ""), str(runtime.environment),
            str(getattr(credentials, "api_key", "") or id(credentials)),
        )
        if key not in self._rr_execution_clients:
            self._rr_execution_clients[key] = OkxRestClient()
        return self._rr_execution_clients[key]

    def _set_rr_activation_status(self, text: str, *, pending: bool = False) -> None:
        self._rr_enable_trade_btn.setEnabled(not pending)
        self._rr_action_status.setText(text)
        self._rr_action_status.show()
        self._set_status(text)
        self._refresh_rr_workspace_other_status()

    def _activate_rr_trade(self, plan: RRTradePlan, runtime, requested_at: float) -> RRTradeLedgerEntry:
        append_log_line(f"[rr_latency] start plan={plan.plan_id} queue_ms={(time.perf_counter() - requested_at) * 1000:.1f}")
        with measure_ui_step("rr.submit", profile=plan.profile_name, environment=plan.environment, plan=plan.plan_id):
            return self._rr_trade_execution_service.activate(
                client=self._rr_client(runtime), credentials=runtime.credentials,
                config=_build_strategy_config(plan.inst_id, runtime), plan=plan,
            )

    def _handle_rr_activation_success(self, entry: RRTradeLedgerEntry, runtime, requested_at: float) -> None:
        self._pending_rr_activation_plan_ids.discard(entry.plan.plan_id)
        order = entry.entry_order
        if order is not None and order.order_id:
            plan = entry.plan
            view = OrderStatusView(
                inst_id=plan.inst_id, inst_type="SWAP", ord_id=order.order_id,
                side="buy" if plan.direction == "long" else "sell", pos_side="",
                td_mode=str(getattr(runtime, "trade_mode", "")),
                ord_type="market" if plan.entry_execution_mode == "market" else "limit",
                state="pending_confirmation", price=order.price, avg_price=None,
                size=order.size, filled_size=None, created_time=int(time.time() * 1000), update_time=None,
                client_order_id=order.client_id or "", reduce_only=False,
                raw={"_feed_source": "submit_ack", "_source_kind": "normal", "_ack_received_at": time.perf_counter(),
                     "tpTriggerPx": str(plan.take_profit_price), "tpOrdPx": "-1",
                     "slTriggerPx": str(plan.stop_loss_price), "slOrdPx": "-1"},
            )
            store = get_shared_order_store()
            with measure_ui_step("rr.publish_ack", profile=plan.profile_name, environment=plan.environment, order=order.order_id):
                store.publish_order_updates(profile_name=plan.profile_name, environment=plan.environment, orders=[view])
            store.confirm_submitted_order(runtime=runtime, profile_name=plan.profile_name, order=view)
        self._set_rr_activation_status("RR 下单已收到回执，委托已发布；确认状态见下方委托列表。")
        append_log_line(f"[rr_latency] ack_published plan={entry.plan.plan_id} total_ms={(time.perf_counter() - requested_at) * 1000:.1f}")

    def _handle_rr_activation_failure(self, plan_id: str, message: str) -> None:
        self._pending_rr_activation_plan_ids.discard(plan_id)
        self._set_rr_activation_status(f"RR 提交未确认：{message}；请核对订单状态，勿连续重复提交。")

    @Slot()
    def _cancel_selected_rr_trade(self) -> None:
        self._set_rr_cancel_status("正在核对 RR 入场挂单…", pending=True)
        try:
            payload = self._selected_rr_payload()
            if self._purge_placeholder_rr_entry(payload, delete_item=False):
                self._set_rr_cancel_status(self._status.text())
                return
            entry = self._rr_ledger_entry_for_item(payload)
            if entry is None:
                raise RuntimeError("当前 RR 没有可取消的交易记录。")
            plan = getattr(entry, "plan", None)
            plan_profile = str(getattr(plan, "profile_name", "") or "").strip()
            runtime = self._runtime_for_task_profile(plan_profile) if plan_profile else self._runtime
            if runtime is None:
                raise RuntimeError(f"API {plan_profile or '-'} 不可用。")
        except Exception as exc:  # noqa: BLE001
            self._set_rr_cancel_status(f"无法撤销 RR 入场挂单：{exc}")
            return

        self._submit_rr_trade_cancel(
            entry=entry,
            runtime=runtime,
            confirmed_for_filled=False,
            delete_after_cancel=False,
        )

    def _set_rr_cancel_status(self, text: str, *, pending: bool = False) -> None:
        self._rr_action_status.setText(text)
        self._rr_action_status.show()
        self._rr_cancel_trade_btn.setEnabled(not pending)
        self._set_status(text)
        self._refresh_rr_workspace_other_status()

    def _submit_rr_trade_cancel(
        self,
        *,
        entry: RRTradeLedgerEntry,
        runtime,
        confirmed_for_filled: bool,
        delete_after_cancel: bool,
    ) -> None:
        progress_text = (
            "正在撤销 RR 入场挂单；成功后会删除本地 RR 方案…"
            if delete_after_cancel
            else "正在提交 RR 入场挂单撤单请求…"
        )
        queued_text = (
            "撤单请求已排队；完成后会删除本地 RR 方案。"
            if delete_after_cancel
            else "当前正在同步 RR 状态，撤单请求已排队，稍后自动执行。"
        )
        self._set_rr_cancel_status(progress_text, pending=True)
        started = self._start_rr_execution_action(
            action=lambda: self._rr_trade_execution_service.cancel(
                client=OkxRestClient(),
                credentials=runtime.credentials,
                config=_build_strategy_config(entry.plan.inst_id, runtime),
                entry=entry,
                confirmed_for_filled=confirmed_for_filled,
            ),
            on_success=lambda updated: self._handle_rr_cancel_result(
                updated,
                runtime=runtime,
                delete_after_cancel=delete_after_cancel,
            ),
            on_failure=lambda message: self._set_rr_cancel_status(f"RR 入场挂单撤单失败：{message}"),
            queue_if_busy=True,
            queued_status_text=queued_text,
        )
        if started is False:
            self._set_rr_cancel_status(queued_text, pending=True)

    def _delete_rr_item_at_index(self, index: int) -> None:
        entry = self._workspace_entry()
        rr_items = entry.get("rr")
        if not isinstance(rr_items, list) or index < 0 or index >= len(rr_items):
            return
        del rr_items[index]
        next_index = min(index, len(rr_items) - 1)
        self._selected_rr_index = next_index
        self._save_workspace_snapshot()
        self._populate_rr_table(selected_index=next_index)
        if self._pending_payload is not None:
            self._render_to_chart(self._pending_payload)

    def _delete_rr_visual_keep_protection(self, entry: RRTradeLedgerEntry) -> None:
        index = self._find_rr_item_index_for_entry(entry)
        if index < 0:
            index = self._selected_rr_index
        if index >= 0:
            self._delete_rr_item_at_index(index)
        self._set_status("已删除本地 RR 图形；已成交仓位和交易所止损/止盈保护继续保留。")

    def _purge_placeholder_rr_entry(self, payload: dict[str, object], *, delete_item: bool) -> bool:
        ledger_entry = self._rr_ledger_entry_for_item(payload)
        if not _rr_entry_looks_local_placeholder(ledger_entry):
            return False
        plan_id = ""
        if isinstance(ledger_entry, RRTradeLedgerEntry):
            plan_id = str(ledger_entry.plan.plan_id or "").strip()
        if not plan_id:
            binding = payload.get("trade_binding")
            if isinstance(binding, dict):
                plan_id = str(binding.get("plan_id", "") or "").strip()
        if plan_id:
            self._drop_rr_trade_ledger_entry(plan_id)
        if delete_item:
            self._delete_rr_item_at_index(self._selected_rr_index)
            self._set_status("已清理本地测试 RR 记录。")
            return True
        refreshed = self._workspace_entry().get("rr", [])
        if isinstance(refreshed, list) and 0 <= self._selected_rr_index < len(refreshed):
            item = refreshed[self._selected_rr_index]
            if isinstance(item, dict):
                item.pop("trade_binding", None)
                self._save_workspace_snapshot()
                self._refresh_rr_tracking_summary(item)
        self._set_status("已清理本地测试 RR 绑定，可继续正常操作。")
        return True

    def _handle_rr_cancel_result(self, entry: RRTradeLedgerEntry, *, runtime, delete_after_cancel: bool) -> None:
        self._rr_cancel_trade_btn.setEnabled(True)
        if entry.status == "cancel_confirmation_required":
            confirmation_text = "该 RR 已有成交。确认后只撤销未成交部分，已成交仓位及止损/止盈保护会保留。是否继续？"
            if delete_after_cancel:
                confirmation_text += "\n\n同时会删除本地 RR 图形和列表记录；不会平仓，也不会撤销交易所保护单。"
            confirmed = QMessageBox.question(
                self,
                "删除 RR 方案并撤销剩余挂单" if delete_after_cancel else "确认撤销剩余入场挂单",
                confirmation_text,
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                QMessageBox.StandardButton.No,
            )
            if confirmed == QMessageBox.StandardButton.Yes:
                self._submit_rr_trade_cancel(
                    entry=entry,
                    runtime=runtime,
                    confirmed_for_filled=True,
                    delete_after_cancel=delete_after_cancel,
                )
                return
            self._set_rr_cancel_status("已取消撤单确认，原订单保持不变。")
            return
        if entry.status in {"cancelled", "protected_cancelled_remainder"}:
            get_shared_order_store().request_refresh(runtime=runtime, profile_name=entry.plan.profile_name)
        if delete_after_cancel:
            if entry.status == "cancelled":
                index = self._find_rr_item_index_for_entry(entry)
                if index >= 0:
                    self._delete_rr_item_at_index(index)
                self._set_rr_cancel_status("RR 撤单请求已处理，图形已删除；请以 OKX 最终订单状态为准。")
                return
            if entry.status in {
                "protected",
                "protected_break_even",
                "protected_trailing",
                "protected_cancelled_remainder",
            }:
                self._delete_rr_visual_keep_protection(entry)
                self._set_rr_cancel_status(self._status.text())
                return
            self._set_rr_cancel_status(f"RR 取消结果：{entry.status}，已有成交或保护单，保留图形。")
            return
        if entry.status == "cancelled":
            message = "RR 入场挂单撤单请求已处理，请以 OKX 最终订单状态为准。"
        elif entry.status == "protected_cancelled_remainder":
            message = "剩余入场挂单撤单请求已处理；已成交仓位和止盈止损保护保留。请以 OKX 最终订单状态为准。"
        else:
            detail = entry.events[-1].message if entry.events else ""
            message = f"RR 撤单返回状态：{entry.status}" + (f"；{detail}" if detail else "")
        self._set_rr_cancel_status(message)

    def _rr_ledger_entry_for_item(self, item: dict[str, object]) -> RRTradeLedgerEntry | None:
        bound_plan_id = self._rr_trade_binding_plan_id(item)
        if bound_plan_id:
            bound_entry = self._find_rr_trade_ledger_entry(bound_plan_id)
            if bound_entry is not None:
                return bound_entry
        rr_id = str(item.get("rr_id", "") or "").strip()
        trade_ref = str(item.get("trade_ref", "") or "").strip()
        if not rr_id and not trade_ref:
            return None
        suffix = f":{trade_ref or rr_id}"
        for entry in self._matching_rr_trade_ledger_entries():
            if not str(entry.plan.plan_id or "").endswith(suffix):
                continue
            if trade_ref or self._legacy_rr_plan_matches_item(entry, item):
                return entry
        return None

    def _legacy_rr_plan_matches_item(self, entry: RRTradeLedgerEntry, item: dict[str, object]) -> bool:
        plan = entry.plan
        side = str(item.get("side", "") or "").strip().lower()
        plan_direction = str(getattr(plan, "direction", "") or "").strip().lower()
        if side and plan_direction and plan_direction != side:
            return False
        for item_key, plan_key in (
            ("price_entry", "entry_price"),
            ("price_stop", "stop_loss_price"),
            ("price_tp", "take_profit_price"),
        ):
            item_price = _parse_rr_optional_decimal(item.get(item_key))
            plan_price = _parse_rr_optional_decimal(getattr(plan, plan_key, None))
            if item_price is None or plan_price is None:
                continue
            if item_price != plan_price:
                return False
        return True

    def _refresh_rr_tracking_summary(self, item: dict[str, object] | None) -> None:
        if not hasattr(self, "_rr_tracking_summary"):
            return
        if not isinstance(item, dict) or not item:
            summary_text = "选中 RR 后显示入场、止损、止盈和跟踪状态。"
            self._rr_tracking_summary.setText(summary_text)
            self._rr_tracking_summary.setToolTip(summary_text)
            if hasattr(self, "_rr_condition_status"):
                condition_text = _rr_condition_status_text(None)
                self._rr_condition_status.setText(condition_text)
                self._rr_condition_status.setToolTip(condition_text)
            self._refresh_rr_workspace_other_status()
            return
        price_increment = self._current_rr_price_increment(item)
        rr_id = str(item.get("rr_id", "") or f"rr-{self._selected_rr_index + 1}")
        side = "空头" if str(item.get("side", "")).strip().lower() == "short" else "多头"
        entry_text = _format_rr_table_price(item.get("price_entry", ""), price_increment) or "-"
        stop_text = _format_rr_table_price(item.get("price_stop", ""), price_increment) or "-"
        tp_text = _format_rr_table_price(item.get("price_tp", ""), price_increment) or "-"
        r_text = str(item.get("r_multiple", "") or "-")
        risk_text = _format_rr_table_price(item.get("risk_amount", "100")) or "100"
        locked_text = "已锁定" if bool(item.get("locked", False)) else "未锁定"
        fee_text = "开启" if _rr_fee_offset_enabled(item.get("fee_offset_enabled", False)) else "关闭"
        management_text = _rr_management_mode_text(item.get("management_mode"))
        ledger_entry = self._rr_ledger_entry_for_item(item)
        if hasattr(self, "_rr_condition_status"):
            condition_text = _rr_condition_status_text(ledger_entry)
            self._rr_condition_status.setText(condition_text)
            self._rr_condition_status.setToolTip(condition_text)
        ledger_text = "交易：未启用"
        if ledger_entry is not None:
            status_text = {
                "entry_working": "追单中",
                "entry_partially_filled": "部分成交",
                "protected": "已保护",
                "protected_break_even": "已保本",
                "protected_trailing": "锁盈中",
                "protected_cancelled_remainder": "已撤剩余单，保护中",
                "cancelled": "已取消",
                "manual_review": "需人工处理",
            }.get(str(ledger_entry.status or ""), str(ledger_entry.status or "未知"))
            filled_text = self._format_rr_size_with_coin(
                ledger_entry.plan.inst_id,
                ledger_entry.filled_size or Decimal("0"),
            )
            remaining_text = self._format_rr_size_with_coin(
                ledger_entry.plan.inst_id,
                ledger_entry.remaining_size or Decimal("0"),
            )
            current_stop = getattr(getattr(ledger_entry, "stop_loss_order", None), "trigger_price", None)
            current_stop_text = _format_rr_table_price(current_stop, price_increment) or "-"
            last_event = ledger_entry.events[-1].message if ledger_entry.events else "-"
            ledger_text = f"交易：{status_text} | 成交 {filled_text} | 剩余 {remaining_text} | 当前止损 {current_stop_text}"
        summary_text = (
            f"{rr_id} | {side} | 入场 {entry_text} | 止损 {stop_text} | 止盈 {tp_text} | 风险金 {risk_text} U | {management_text} | R 1:{r_text}\n"
            f"状态：{locked_text} | 手续费偏移：{fee_text}\n{ledger_text}"
        )
        tooltip_text = summary_text if ledger_entry is None else f"{summary_text}\n最后事件：{last_event}"
        self._rr_tracking_summary.setText(summary_text)
        self._rr_tracking_summary.setToolTip(tooltip_text)
        self._refresh_rr_workspace_other_status()

    def _open_rr_card_for_selected(self) -> None:
        self._select_rr_workspace_mode("direct")
        entry = self._workspace_entry()
        raw_rr = entry.get("rr", [])
        if not isinstance(raw_rr, list) or not (0 <= self._selected_rr_index < len(raw_rr)):
            return
        item = raw_rr[self._selected_rr_index]
        if not isinstance(item, dict):
            return
        instrument = self._instrument_for_symbol()
        price_increment = self._current_rr_price_increment(item)
        dialog = RRCardDialog(
            parent=self,
            item=item,
            instrument=instrument,
            symbol=self._selected_symbol(),
            period=self._period_combo.currentText().strip(),
            price_increment=price_increment,
        )
        if dialog.exec() != int(QDialog.DialogCode.Accepted):
            return
        payload = dialog.result_payload()
        if not isinstance(payload, dict):
            return
        self._rr_side_combo.setCurrentIndex(1 if str(payload.get("side", "long")) == "short" else 0)
        self._rr_management_mode_combo.setCurrentIndex(
            max(0, self._rr_management_mode_combo.findData(_normalize_rr_management_mode(payload.get("management_mode"))))
        )
        self._rr_entry_edit.setText(str(payload.get("price_entry", "") or ""))
        self._rr_stop_edit.setText(str(payload.get("price_stop", "") or ""))
        self._rr_r_edit.setValue(float(str(payload.get("r_multiple", "") or "2")))
        self._rr_fee_offset_check.setChecked(_rr_fee_offset_enabled(payload.get("fee_offset_enabled", False)))
        self._rr_locked_check.setChecked(bool(payload.get("locked", False)))
        self._rr_execution_mode_combo.setCurrentIndex(
            max(0, self._rr_execution_mode_combo.findData(str(payload.get("entry_execution_mode", "limit") or "limit")))
        )
        self._save_rr_item(extra_payload=payload)

    def _set_draw_tool(self, tool: str) -> None:
        if tool in {"rr_long", "rr_short"}:
            self._select_rr_workspace_mode("direct")
        self._draw_tool = tool
        self._pending_line_start = None
        self._pending_rr_start = None
        self._clear_line_drag_state(unlock_view=True)
        self._set_hovered_line_interaction()
        if isinstance(self._native_chart_view, InteractiveKlineChartView):
            self._native_chart_view.set_draw_mode_enabled(tool != "none")
            if tool == "none":
                self._native_chart_view.set_preview_line(None)
                self._native_chart_view.set_preview_rr_item(None)
        label = {
            "none": "光标模式",
            "horizontal": "点击开始绘制水平线",
            "trend": "点击开始绘制趋势线",
            "rr_long": "RR多：先点入场，再点止损",
            "rr_short": "RR空：先点入场，再点止损",
        }.get(tool, "光标模式")
        self._set_status(label)

    def _clear_line_drag_state(self, *, unlock_view: bool) -> None:
        self._line_drag_state = None
        self._rr_drag_state = None
        if unlock_view and isinstance(self._native_chart_view, InteractiveKlineChartView):
            self._native_chart_view.set_interaction_locked(False)

    def _current_chart_pointer_scene_pos(self) -> QPointF | None:
        if isinstance(self._native_chart_view, InteractiveKlineChartView):
            return self._native_chart_view.last_pointer_scene_pos()
        return None

    def _chart_drag_threshold_crossed(self, state: dict[str, object]) -> bool:
        press_x = state.get("press_scene_x")
        press_y = state.get("press_scene_y")
        current_pos = self._current_chart_pointer_scene_pos()
        if current_pos is None or press_x is None or press_y is None:
            return True
        delta_x = float(current_pos.x()) - float(press_x)
        delta_y = float(current_pos.y()) - float(press_y)
        return math.hypot(delta_x, delta_y) >= _RR_DRAG_ACTIVATION_DISTANCE_PX

    def _area_shape_replay_inspection_active(self) -> bool:
        dialog = getattr(self, "_area_shape_replay_dialog", None)
        return dialog is not None and dialog.isVisible()

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
        if self._area_shape_replay_inspection_active():
            return
        if self._draw_tool in {"rr_long", "rr_short"} and self._pending_payload is not None:
            if self._pending_rr_start is None:
                resolved = self._resolve_primary_chart_click(x_value=x_value, y_value=y_value)
                if resolved is None:
                    return
                candle_time, price = resolved
                side = "long" if self._draw_tool == "rr_long" else "short"
                self._pending_rr_start = (side, candle_time, price)
                self._rr_side_combo.setCurrentIndex(0 if side == "long" else 1)
                self._rr_entry_edit.setText(decimal_to_text(Decimal(str(price))))
                self._rr_bar_edit.setText(str(self._bar_index_for_candle_time(candle_time)))
                self._set_status(f"RR {'多头' if side == 'long' else '空头'}入场已记录 | {_format_bar_time(candle_time)} | {price:.2f}")
            return
        if self._draw_tool != "none" or self._pending_payload is None:
            return
        resolved = self._resolve_primary_chart_click(x_value=x_value, y_value=y_value)
        if resolved is None:
            return
        candle_time, price = resolved
        hit = self._line_hit_test(candle_time=candle_time, price=price)
        if hit is not None:
            index = int(hit["index"])
            self._populate_line_table(selected_index=index)
            self._line_drag_state = {
                "index": index,
                "active": False,
                "drag_mode": str(hit["drag_mode"]),
                "anchor_candle_time": candle_time,
                "anchor_price": price,
                "anchor_line": dict(hit["line"]),
            }
            press_scene_pos = self._current_chart_pointer_scene_pos()
            if press_scene_pos is not None:
                self._line_drag_state["press_scene_x"] = float(press_scene_pos.x())
                self._line_drag_state["press_scene_y"] = float(press_scene_pos.y())
            self._set_hovered_line_interaction(index, str(hit["drag_mode"]))
        else:
            rr_hit = self._rr_hit_test(candle_time=candle_time, price=price)
            if rr_hit is None:
                return
            index = int(rr_hit["index"])
            was_selected = self._selected_rr_index == index
            self._clear_line_selection_for_rr_focus()
            self._populate_rr_table(selected_index=index)
            if not was_selected:
                if self._pending_payload is not None:
                    self._render_to_chart(self._pending_payload)
                return
            rr_drag_state: dict[str, object] = {
                "index": index,
                "drag_mode": str(rr_hit["drag_mode"]),
                "active": False,
                "anchor_candle_time": candle_time,
                "anchor_price": price,
                "anchor_rr": dict(self._workspace_entry().get("rr", [])[index]),
            }
            press_scene_pos = self._current_chart_pointer_scene_pos()
            if press_scene_pos is not None:
                rr_drag_state["press_scene_x"] = float(press_scene_pos.x())
                rr_drag_state["press_scene_y"] = float(press_scene_pos.y())
            self._rr_drag_state = rr_drag_state
        if isinstance(self._native_chart_view, InteractiveKlineChartView):
            self._native_chart_view.set_interaction_locked(True)
            self._native_chart_view.set_interaction_cursor_mode("dragging")
        if self._pending_payload is not None:
            self._render_to_chart(self._pending_payload)

    @Slot(float, float)
    def _on_chart_pointer_moved(self, x_value: float, y_value: float) -> None:
        if self._area_shape_replay_inspection_active():
            self._set_hovered_line_interaction()
            return
        if self._draw_tool != "none":
            self._set_hovered_line_interaction()
            resolved_preview = self._resolve_primary_chart_click(x_value=x_value, y_value=y_value)
            if resolved_preview is not None:
                preview_time, preview_price = resolved_preview
                self._update_draw_preview(candle_time=preview_time, price=preview_price)
            return
        if self._line_drag_state is None and self._rr_drag_state is None:
            resolved_hover = self._resolve_primary_chart_click(x_value=x_value, y_value=y_value)
            if resolved_hover is not None:
                hover_time, hover_price = resolved_hover
                hit = self._line_hit_test(candle_time=hover_time, price=hover_price)
                rr_hit = None
                if hit is None:
                    self._set_hovered_line_interaction()
                    rr_hit = self._rr_hit_test(candle_time=hover_time, price=hover_price)
                else:
                    self._set_hovered_line_interaction(int(hit["index"]), str(hit.get("drag_mode", "")))
                if isinstance(self._native_chart_view, InteractiveKlineChartView):
                    if hit is not None:
                        if str(hit.get("drag_mode", "")) in {"endpoint_a", "endpoint_b"}:
                            self._native_chart_view.set_interaction_cursor_mode("endpoint")
                        else:
                            self._native_chart_view.set_interaction_cursor_mode("move")
                    elif rr_hit is not None:
                        self._native_chart_view.set_interaction_cursor_mode("move")
                    else:
                        self._native_chart_view.set_interaction_cursor_mode("default")
            else:
                self._set_hovered_line_interaction()
            return
        resolved = self._resolve_primary_chart_click(x_value=x_value, y_value=y_value)
        if resolved is None:
            return
        candle_time, price = resolved
        updated = False
        if self._line_drag_state is not None:
            if not bool(self._line_drag_state.get("active", False)):
                if not self._chart_drag_threshold_crossed(self._line_drag_state):
                    return
                self._line_drag_state["active"] = True
            updated = self._apply_line_drag_update(candle_time=candle_time, price=price)
        elif self._rr_drag_state is not None:
            if not bool(self._rr_drag_state.get("active", False)):
                if not self._chart_drag_threshold_crossed(self._rr_drag_state):
                    return
                self._rr_drag_state["active"] = True
            updated = self._apply_rr_drag_update(candle_time=candle_time, price=price)
        if updated and self._pending_payload is not None:
            self._render_to_chart(self._pending_payload)

    @Slot(float, float)
    def _on_chart_pointer_released(self, x_value: float, y_value: float) -> None:
        if self._draw_tool in {"rr_long", "rr_short"} and self._pending_rr_start is not None:
            resolved = self._resolve_primary_chart_click(x_value=x_value, y_value=y_value)
            if resolved is not None:
                candle_time, price = resolved
                pending_side, entry_time, entry_price = self._pending_rr_start
                if self._append_rr_rule_from_chart(
                    side=pending_side,
                    entry_candle_time=entry_time,
                    entry_price=entry_price,
                    stop_candle_time=candle_time,
                    stop_price=price,
                ):
                    self._pending_rr_start = None
                    self._set_draw_tool("none")
            self._suppress_next_chart_click = True
            return
        if self._line_drag_state is None and self._rr_drag_state is None:
            return
        line_drag_active = bool(isinstance(self._line_drag_state, dict) and self._line_drag_state.get("active", False))
        rr_drag_active = bool(isinstance(self._rr_drag_state, dict) and self._rr_drag_state.get("active", False))
        resolved = self._resolve_primary_chart_click(x_value=x_value, y_value=y_value)
        if resolved is not None:
            candle_time, price = resolved
            if line_drag_active:
                self._apply_line_drag_update(candle_time=candle_time, price=price)
            elif rr_drag_active:
                self._apply_rr_drag_update(candle_time=candle_time, price=price)
        if line_drag_active or rr_drag_active:
            self._save_workspace_snapshot()
            if line_drag_active:
                self._sync_area_shape_draft_lines()
                self._refresh_area_shape_status()
                if self._line_drag_state is not None and self._line_drag_state.get("area_price_error"):
                    self._set_area_shape_status(str(self._line_drag_state["area_price_error"]))
            if self._pending_payload is not None:
                self._render_to_chart(self._pending_payload)
        entry = self._workspace_entry()
        label = ""
        rr_id = ""
        selected_index = int(self._line_drag_state.get("index", -1)) if self._line_drag_state is not None else -1
        selected_rr_index = int(self._rr_drag_state.get("index", -1)) if self._rr_drag_state is not None else -1
        lines = entry.get("lines", [])
        if isinstance(lines, list) and 0 <= selected_index < len(lines) and isinstance(lines[selected_index], dict):
            label = str(lines[selected_index].get("label", "") or "")
        raw_rr = entry.get("rr", [])
        if isinstance(raw_rr, list) and 0 <= selected_rr_index < len(raw_rr) and isinstance(raw_rr[selected_rr_index], dict):
            rr_id = str(raw_rr[selected_rr_index].get("rr_id", "") or "")
        self._clear_line_drag_state(unlock_view=True)
        self._set_hovered_line_interaction(selected_index, "move" if selected_index >= 0 else None)
        if label:
            self._set_status(f"已选中标注: {label}")
        elif rr_id:
            self._set_status(f"RR 已更新：{rr_id}")

    @Slot(float, float)
    def _on_native_chart_clicked(self, x_value: float, y_value: float) -> None:
        if self._area_shape_replay_inspection_active():
            return
        if self._suppress_next_chart_click:
            self._suppress_next_chart_click = False
            return
        if self._pending_payload is None:
            return
        resolved = self._resolve_primary_chart_click(x_value=x_value, y_value=y_value)
        if resolved is None:
            return
        candle_time, price = resolved
        if self._draw_tool == "none":
            if self._select_nearest_rr_from_chart(candle_time=candle_time, price=price):
                return
            self._select_nearest_line_from_chart(candle_time=candle_time, price=price)
            return
        if self._draw_tool in {"rr_long", "rr_short"}:
            side = "long" if self._draw_tool == "rr_long" else "short"
            if self._pending_rr_start is None:
                self._pending_rr_start = (side, candle_time, price)
                self._rr_side_combo.setCurrentIndex(0 if side == "long" else 1)
                self._rr_entry_edit.setText(decimal_to_text(Decimal(str(price))))
                self._rr_bar_edit.setText(str(self._bar_index_for_candle_time(candle_time)))
                self._set_status(f"RR {'多头' if side == 'long' else '空头'}入场已记录 | {_format_bar_time(candle_time)} | {price:.2f}")
                return
            pending_side, entry_time, entry_price = self._pending_rr_start
            if self._append_rr_rule_from_chart(
                side=pending_side,
                entry_candle_time=entry_time,
                entry_price=entry_price,
                stop_candle_time=candle_time,
                stop_price=price,
            ):
                self._pending_rr_start = None
                self._set_draw_tool("none")
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
                email_enabled=(
                    self._line_email_enabled_check.isChecked()
                    and str(self._line_action_combo.currentData() or "notify") == "notify"
                ),
                email_delivery_mode=str(self._line_email_delivery_mode_combo.currentData() or "once"),
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
            email_enabled=(
                self._line_email_enabled_check.isChecked()
                and str(self._line_action_combo.currentData() or "notify") == "notify"
            ),
            email_delivery_mode=str(self._line_email_delivery_mode_combo.currentData() or "once"),
        )
        self._append_line_rule(line)
        self._pending_line_start = None
        if isinstance(self._native_chart_view, InteractiveKlineChartView):
            self._native_chart_view.set_preview_line(None)
        self._set_draw_tool("none")

    @Slot(float, float)
    def _on_native_chart_double_clicked(self, x_value: float, y_value: float) -> None:
        chart = self._native_chart_view
        if self._draw_tool == "none" and isinstance(chart, InteractiveKlineChartView) and chart._area_shape_replay_trades:
            self._clear_line_drag_state(unlock_view=True)
            chart.set_area_shape_replay_trades(chart._area_shape_replay_trades, focus=True, fit=True)
            self._set_status("已定位当前复盘标记；点击标记文字不会修改交易或区域线。")
            return
        if self._pending_payload is None or self._draw_tool != "none":
            if isinstance(self._native_chart_view, InteractiveKlineChartView):
                self._native_chart_view.reset_view()
            return
        resolved = self._resolve_primary_chart_click(x_value=x_value, y_value=y_value)
        if resolved is None:
            if isinstance(self._native_chart_view, InteractiveKlineChartView):
                self._native_chart_view.reset_view()
            return
        candle_time, price = resolved
        if self._select_nearest_rr_from_chart(candle_time=candle_time, price=price, open_dialog=True):
            return
        if isinstance(self._native_chart_view, InteractiveKlineChartView):
            self._native_chart_view.reset_view()

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
        return candle_time, self._logical_price_from_display(self._pending_payload, float(y_value), is_secondary=False)

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
            if self._area_line_is_frozen(str(item.get("id", ""))):
                continue
            kind = str(item.get("kind", "horizontal") or "horizontal").strip().lower()
            if kind == "ray":
                endpoint_time = int(item.get("time_a", 0) or 0)
                endpoint_price = float(item.get("price_a", 0.0) or 0.0)
                time_score = abs(endpoint_time - candle_time) / max(float(endpoint_time_tolerance), 1.0)
                price_score = abs(endpoint_price - price) / max(endpoint_price_tolerance, 1.0)
                score = max(time_score, price_score)
                if score <= 1.0 and score < best_score:
                    best_score = score
                    best_hit = {"index": index, "line": item, "drag_mode": "endpoint_a"}
                if candle_time >= endpoint_time:
                    line_distance = abs(endpoint_price - float(price))
                    score = line_distance / max(body_price_tolerance, 1.0)
                    if score <= 1.0 and score < best_score:
                        best_score = score
                        best_hit = {"index": index, "line": item, "drag_mode": "move"}
                continue
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
        index = int(state.get("index", -1))
        if not isinstance(lines, list) or index < 0 or index >= len(lines) or not isinstance(lines[index], dict):
            return False
        if self._area_line_is_frozen(str(lines[index].get("id", ""))):
            self._line_drag_state = None
            self._set_area_shape_status("监控中的区域线与冻结参数不能修改。")
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
        line_id = str(updated.get("id", ""))
        plan = next((item for item in self._area_shape_plans() if line_id in {item.support_line_id, item.pressure_line_id} and item.status == "draft"), None)
        if plan is not None:
            candidates = {str(item.get("id", "")): item for item in lines if isinstance(item, dict)}
            candidates[line_id] = updated
            support = float(candidates.get(plan.support_line_id, {}).get("price_a", 0) or 0)
            pressure = float(candidates.get(plan.pressure_line_id, {}).get("price_a", 0) or 0)
            if not math.isfinite(support) or not math.isfinite(pressure) or support <= 0 or pressure <= support:
                message = "支撑必须大于 0 且低于压力，已保留上一次有效价位。"
                state["area_price_error"] = message
                self._set_area_shape_status(message)
                return False
            state.pop("area_price_error", None)
        lines[index] = updated
        if plan is not None and str(updated.get("kind", "")) == "ray":
            partner_id = plan.pressure_line_id if line_id == plan.support_line_id else plan.support_line_id
            for partner in lines:
                if isinstance(partner, dict) and str(partner.get("id", "")) == partner_id:
                    partner["time_a"] = int(updated.get("time_a", candle_time) or candle_time)
                    partner["time_b"] = int(updated.get("time_a", candle_time) or candle_time)
                    break
        self._selected_line_index = index
        self._apply_line_record_to_form(index, updated)
        return True

    def _apply_rr_drag_update(self, *, candle_time: int, price: float) -> bool:
        state = self._rr_drag_state
        if state is None:
            return False
        entry = self._workspace_entry()
        raw_rr = entry.get("rr")
        index = int(state.get("index", -1))
        if not isinstance(raw_rr, list) or index < 0 or index >= len(raw_rr) or not isinstance(raw_rr[index], dict):
            return False
        existing_payload = dict(raw_rr[index])
        drag_mode = str(state.get("drag_mode", "rr_stop") or "rr_stop")
        anchor_payload = state.get("anchor_rr")
        annotation_payload = dict(anchor_payload) if drag_mode == "rr_move" and isinstance(anchor_payload, dict) else existing_payload
        annotation = rr_annotation_from_payload(annotation_payload)
        if drag_mode == "rr_move":
            anchor_candle_time = int(state.get("anchor_candle_time", candle_time) or candle_time)
            anchor_price = Decimal(str(state.get("anchor_price", price) or price))
            bar_delta = float(self._bar_index_for_candle_time(candle_time) - self._bar_index_for_candle_time(anchor_candle_time))
            updated = drag_rr_annotation(
                annotation,
                drag_mode,
                annotation.price_entry + (Decimal(str(price)) - anchor_price),
                bar_delta=bar_delta,
            )
        else:
            updated = drag_rr_annotation(annotation, drag_mode, Decimal(str(price)))
        payload = {**existing_payload, **rr_annotation_to_payload(updated)}
        payload = self._normalized_rr_payload(payload, derive_r_from_take_profit=(drag_mode == "rr_tp"))
        raw_rr[index] = payload
        self._selected_rr_index = index
        self._apply_rr_record_to_form(index, payload)
        return True

    def _update_draw_preview(self, *, candle_time: int, price: float) -> None:
        if not isinstance(self._native_chart_view, InteractiveKlineChartView):
            return
        display_price = (
            self._display_price_from_logical(self._pending_payload, price, is_secondary=False)
            if self._pending_payload is not None
            else float(price)
        )
        if self._draw_tool == "horizontal":
            self._native_chart_view.set_preview_line(
                {
                    "kind": "horizontal",
                    "time_a": candle_time,
                    "price_a": display_price,
                    "time_b": candle_time,
                    "price_b": display_price,
                }
            )
            return
        if self._draw_tool == "trend" and self._pending_line_start is not None:
            start_time, start_price = self._pending_line_start
            display_start_price = (
                self._display_price_from_logical(self._pending_payload, start_price, is_secondary=False)
                if self._pending_payload is not None
                else float(start_price)
            )
            line_time_a, line_price_a, line_time_b, line_price_b = _ordered_trend_endpoints(
                start_time,
                display_start_price,
                candle_time,
                display_price,
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
        if self._draw_tool in {"rr_long", "rr_short"} and self._pending_rr_start is not None:
            side, start_time, start_price = self._pending_rr_start
            try:
                (normalized_entry_time, price_entry), (_, price_stop) = self._normalize_rr_points(
                    side=side,
                    first_candle_time=start_time,
                    first_price=start_price,
                    second_candle_time=candle_time,
                    second_price=price,
                )
                r_multiple = self._parse_rr_decimal(self._rr_r_edit.text(), "R 倍数")
                price_tp = _compute_rr_take_profit(
                    side,
                    price_entry,
                    price_stop,
                    r_multiple,
                    fee_offset_enabled=self._rr_fee_offset_check.isChecked(),
                    price_increment=self._current_rr_price_increment(),
                )
            except Exception:
                self._native_chart_view.set_preview_rr_item(None)
                self._native_chart_view.set_preview_line(None)
                return
            self._native_chart_view.set_preview_rr_item(
                {
                    "side": side,
                    "bar_entry": self._bar_index_for_candle_time(normalized_entry_time),
                    "price_entry": float(price_entry),
                    "price_stop": float(price_stop),
                    "price_tp": float(price_tp),
                    "r_multiple": decimal_to_text(_normalize_rr_multiple_step(r_multiple)),
                    "fee_offset_enabled": self._rr_fee_offset_check.isChecked(),
                }
            )
            self._native_chart_view.set_preview_line(None)
            return
        self._native_chart_view.set_preview_rr_item(None)
        self._native_chart_view.set_preview_line(None)

    def _select_nearest_rr_from_chart(self, *, candle_time: int, price: float, open_dialog: bool = False) -> bool:
        entry = self._workspace_entry()
        raw_rr = entry.get("rr", [])
        records = list(raw_rr) if isinstance(raw_rr, list) else []
        if not records:
            return False
        nearest_index = self._nearest_rr_index(candle_time=candle_time, price=price, records=records)
        if nearest_index < 0:
            return False
        self._select_rr_workspace_mode("direct")
        self._clear_line_selection_for_rr_focus()
        self._populate_rr_table(selected_index=nearest_index)
        if self._pending_payload is not None:
            self._render_to_chart(self._pending_payload)
        rr_id = str(records[nearest_index].get("rr_id", "") or f"RR {nearest_index + 1}")
        self._set_status(f"选择 RR：{rr_id}")
        if open_dialog:
            self._open_rr_card_for_selected()
        return True

    def _clear_line_selection_for_rr_focus(self) -> None:
        self._selected_line_index = -1
        self._line_table.blockSignals(True)
        self._line_table.clearSelection()
        self._line_table.setCurrentItem(None)
        self._line_table.blockSignals(False)

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
        was_selected = self._line_table.currentRow() == nearest_index and self._selected_line_index == nearest_index
        self._populate_line_table(selected_index=nearest_index)
        if was_selected and self._pending_payload is not None:
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

    def _bar_index_for_candle_time(self, candle_time: int) -> int:
        if self._pending_payload is None or not self._pending_payload.candles:
            return 0
        candles = self._pending_payload.candles
        for index, candle in enumerate(candles):
            if int(candle["time"]) == int(candle_time):
                return index
        period = self._period_combo.currentText().strip()
        step_ms = _display_step_ms(period, candles)
        step_seconds = max(1, step_ms // 1000)
        first_time = int(candles[0]["time"])
        last_time = int(candles[-1]["time"])
        if int(candle_time) > last_time:
            future_steps = max(1, math.ceil((int(candle_time) - last_time) / float(step_seconds)))
            return (len(candles) - 1) + future_steps
        if int(candle_time) < first_time:
            past_steps = max(1, math.ceil((first_time - int(candle_time)) / float(step_seconds)))
            return -past_steps
        nearest_index = min(range(len(candles)), key=lambda idx: abs(int(candles[idx]["time"]) - int(candle_time)))
        return int(nearest_index)

    def _normalize_rr_points(
        self,
        *,
        side: str,
        first_candle_time: int,
        first_price: float,
        second_candle_time: int,
        second_price: float,
    ) -> tuple[tuple[int, Decimal], tuple[int, Decimal]]:
        first = (int(first_candle_time), Decimal(str(first_price)))
        second = (int(second_candle_time), Decimal(str(second_price)))
        normalized_side = side.strip().lower()
        if normalized_side == "long":
            return (first, second) if first[1] >= second[1] else (second, first)
        if normalized_side == "short":
            return (first, second) if first[1] <= second[1] else (second, first)
        raise ValueError(f"unsupported side: {side!r}")

    def _current_workspace_display_step_ms(self) -> int:
        if hasattr(self, "_display_step_ms"):
            try:
                value = int(getattr(self, "_display_step_ms"))
                if value > 0:
                    return value
            except Exception:
                pass
        if self._pending_payload is not None and self._pending_payload.candles:
            return _display_step_ms(self._period_combo.currentText().strip(), self._pending_payload.candles)
        return 60_000

    def _append_rr_rule_from_chart(
        self,
        *,
        side: str,
        entry_candle_time: int,
        entry_price: float,
        stop_candle_time: int,
        stop_price: float,
    ) -> bool:
        try:
            (normalized_entry_time, price_entry), (normalized_stop_time, price_stop) = self._normalize_rr_points(
                side=side,
                first_candle_time=entry_candle_time,
                first_price=entry_price,
                second_candle_time=stop_candle_time,
                second_price=stop_price,
            )
            r_multiple = self._parse_rr_decimal(self._rr_r_edit.text(), "R 倍数")
            payload = self._normalized_rr_payload(
                {
                "rr_id": self._next_rr_id(),
                "trade_ref": uuid4().hex,
                "side": side,
                "bar_entry": self._bar_index_for_candle_time(normalized_entry_time),
                "bar_stop": self._bar_index_for_candle_time(normalized_stop_time),
                "price_entry": decimal_to_text(price_entry),
                "price_stop": decimal_to_text(price_stop),
                "r_multiple": decimal_to_text(r_multiple),
                "fee_offset_enabled": self._rr_fee_offset_check.isChecked(),
                "locked": False,
                }
            )
        except Exception as exc:  # noqa: BLE001
            self._set_status(f"图上 RR 创建失败：{exc}")
            return False
        entry = self._workspace_entry()
        rr_items = entry.get("rr")
        if not isinstance(rr_items, list):
            rr_items = []
            entry["rr"] = rr_items
        rr_items.append(payload)
        new_index = len(rr_items) - 1
        self._selected_line_index = -1
        self._save_workspace_snapshot()
        self._populate_rr_table(selected_index=new_index)
        self._select_rr_workspace_mode("direct")
        if self._pending_payload is not None:
            self._render_to_chart(self._pending_payload)
        self._set_status(f"已添加 RR：{payload['rr_id']}")
        return True

    def _nearest_rr_index(self, *, candle_time: int, price: float, records: list[object]) -> int:
        if self._pending_payload is None or not self._pending_payload.candles:
            return -1
        lows = [float(item["low"]) for item in self._pending_payload.candles]
        highs = [float(item["high"]) for item in self._pending_payload.candles]
        price_span = max(max(highs) - min(lows), 1.0)
        tolerance = max(price_span * 0.012, abs(price) * 0.0015, 1.0)
        candles = self._pending_payload.candles
        display_step_ms = self._current_workspace_display_step_ms()
        nearest_index = -1
        nearest_distance = float("inf")
        for index, item in enumerate(records):
            if not isinstance(item, dict):
                continue
            try:
                entry_price = float(item.get("price_entry", 0.0) or 0.0)
                stop_price = float(item.get("price_stop", 0.0) or 0.0)
                take_profit = float(item.get("price_tp", 0.0) or 0.0)
                bar_entry = int(round(float(item.get("bar_entry", 0.0) or 0.0)))
            except (TypeError, ValueError):
                continue
            if entry_price <= 0.0 or stop_price <= 0.0 or take_profit <= 0.0:
                continue
            rr_start_time = _candle_time_for_bar_index(
                candles,
                bar_index=bar_entry,
                display_step_ms=display_step_ms,
            )
            if candle_time < rr_start_time:
                continue
            lower = min(entry_price, stop_price, take_profit) - tolerance
            upper = max(entry_price, stop_price, take_profit) + tolerance
            if not lower <= price <= upper:
                continue
            distance = min(
                abs(entry_price - float(price)),
                abs(stop_price - float(price)),
                abs(take_profit - float(price)),
            )
            if distance < nearest_distance:
                nearest_distance = distance
                nearest_index = index
        return nearest_index if nearest_distance <= tolerance else -1

    def _rr_hit_test(self, *, candle_time: int, price: float) -> dict[str, object] | None:
        entry = self._workspace_entry()
        raw_rr = entry.get("rr", [])
        records = list(raw_rr) if isinstance(raw_rr, list) else []
        if not records or self._pending_payload is None or not self._pending_payload.candles:
            return None
        lows = [float(item["low"]) for item in self._pending_payload.candles]
        highs = [float(item["high"]) for item in self._pending_payload.candles]
        price_span = max(max(highs) - min(lows), 1.0)
        tolerance = max(price_span * 0.012, abs(price) * 0.0015, 1.0)
        display_step_ms = self._current_workspace_display_step_ms()
        candidate_indexes = list(range(len(records)))
        if 0 <= self._selected_rr_index < len(records):
            candidate_indexes = [self._selected_rr_index, *[index for index in candidate_indexes if index != self._selected_rr_index]]
        best_hit: dict[str, object] | None = None
        best_distance = float("inf")
        for index in candidate_indexes:
            item = records[index]
            if (
                not isinstance(item, dict)
                or bool(item.get("locked", False))
                or _rr_ledger_blocks_editing(self._rr_ledger_entry_for_item(item))
            ):
                continue
            try:
                entry_price = float(item.get("price_entry", 0.0) or 0.0)
                stop_price = float(item.get("price_stop", 0.0) or 0.0)
                take_profit = float(item.get("price_tp", 0.0) or 0.0)
                bar_entry = int(round(float(item.get("bar_entry", 0.0) or 0.0)))
            except (TypeError, ValueError):
                continue
            rr_start_time = _candle_time_for_bar_index(
                self._pending_payload.candles,
                bar_index=bar_entry,
                display_step_ms=display_step_ms,
            )
            if candle_time < rr_start_time:
                continue
            for drag_mode, target_price in (("rr_move", entry_price), ("rr_stop", stop_price), ("rr_tp", take_profit)):
                distance = abs(target_price - float(price))
                if distance <= tolerance and distance < best_distance:
                    best_distance = distance
                    best_hit = {"index": index, "drag_mode": drag_mode}
            if best_hit is not None:
                continue
            current_bar = self._bar_index_for_candle_time(candle_time)
            box_start = int(round(float(item.get("bar_entry", 0.0) or 0.0)))
            box_end = box_start + _RR_BOX_WIDTH_BARS
            box_low = min(stop_price, take_profit)
            box_high = max(stop_price, take_profit)
            if box_start <= current_bar <= box_end and box_low < float(price) < box_high:
                best_hit = {"index": index, "drag_mode": "rr_move"}
        return best_hit

    def _next_line_label(self, kind: str) -> str:
        explicit = self._line_label_edit.text().strip()
        if explicit:
            return explicit
        entry = self._workspace_entry()
        lines = entry.get("lines", [])
        count = len(lines) + 1 if isinstance(lines, list) else 1
        prefix = "趋势线" if kind == "trend" else ("射线" if kind == "ray" else "水平线")
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
        if self._area_line_is_frozen(str(item.get("id", ""))):
            self._set_area_shape_status("监控中的区域线与冻结参数不能修改。")
            return
        item["label"] = self._line_label_edit.text().strip() or str(item.get("label", "") or "")
        price_a = _parse_rr_optional_decimal(self._line_price_a_edit.text())
        kind = str(item.get("kind", "horizontal") or "horizontal").strip().lower()
        if price_a is None or price_a <= 0:
            self._set_status("请填写有效的线条价格。")
            return
        item["price_a"] = float(price_a)
        if kind == "trend":
            price_b = _parse_rr_optional_decimal(self._line_price_b_edit.text())
            if price_b is None or price_b <= 0:
                self._set_status("请填写有效的趋势线终点价。")
                return
            item["price_b"] = float(price_b)
        else:
            item["price_b"] = float(price_a)
        item["trigger"] = str(self._line_trigger_combo.currentData())
        item["action"] = str(self._line_action_combo.currentData())
        item["enabled"] = self._line_enabled_check.isChecked()
        previous_email_enabled = bool(item.get("email_enabled", False))
        previous_email_delivery_mode = str(item.get("email_delivery_mode", "once") or "once")
        item["email_enabled"] = (
            self._line_email_enabled_check.isChecked() and item["action"] == "notify"
        )
        item["email_delivery_mode"] = str(self._line_email_delivery_mode_combo.currentData() or "once")
        if (
            not item["email_enabled"]
            or not previous_email_enabled
            or previous_email_delivery_mode != item["email_delivery_mode"]
        ):
            item["email_sent_once"] = False
        was_trade_enabled = _rr_fee_offset_enabled(item.get("trade_enabled", False))
        trade_enabled = self._line_trade_enabled_check.isChecked()
        item["trade_enabled"] = trade_enabled
        if trade_enabled and (not was_trade_enabled or not str(item.get("trade_profile_name", "") or "").strip()):
            item["trade_profile_name"] = self._active_profile_name()
            item["trade_environment"] = self._active_environment()
        elif not trade_enabled:
            item.pop("trade_profile_name", None)
            item.pop("trade_environment", None)
        item["entry_execution_mode"] = str(self._line_trade_execution_mode_combo.currentData() or "limit")
        lines[self._selected_line_index] = item
        selected_index = self._selected_line_index
        self._save_workspace_snapshot()
        self._sync_area_shape_draft_lines()
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
        selected_line = lines[self._selected_line_index]
        frozen_plans = (
            self._frozen_area_plans_for_line(str(selected_line.get("id", "")))
            if isinstance(selected_line, dict) else []
        )
        if frozen_plans:
            plan = frozen_plans[-1]
            status_names = {
                "monitoring": "监控中", "touched": "已触区", "paper_pending": "等待入场",
                "paper_position": "纸面持仓中", "paper_break_even": "纸面保本中",
                "paper_locked": "纸面锁盈中", "paused": "已暂停（仍可恢复）", "manual_review": "待人工核对",
            }
            status_text = status_names.get(plan.status, plan.status)
            self._set_area_shape_status(
                f"无法删除：该线仍绑定 {plan.profile_name}/{plan.environment} 区域计划（{status_text}）。"
                "切换账户不会解除绑定。请先在智能辅助交易中取消计划并核对 OKX 未成交委托；"
                "已有实盘持仓不会因取消计划而平仓，交易所保护会继续运行。计划结束后再删除线条。"
            )
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

    def _existing_rr_id(self, entry: dict[str, object] | None = None) -> str:
        entry = entry if isinstance(entry, dict) else self._workspace_entry()
        raw_rr = entry.get("rr", [])
        if isinstance(raw_rr, list) and 0 <= self._selected_rr_index < len(raw_rr):
            payload = raw_rr[self._selected_rr_index]
            if isinstance(payload, dict):
                existing = str(payload.get("rr_id", "") or "").strip()
                if existing:
                    return existing
        count = len(raw_rr) + 1 if isinstance(raw_rr, list) else 1
        return f"rr-{count}"

    def _next_rr_id(self, entry: dict[str, object] | None = None) -> str:
        entry = entry if isinstance(entry, dict) else self._workspace_entry()
        raw_rr = entry.get("rr", [])
        records = list(raw_rr) if isinstance(raw_rr, list) else []
        existing_ids = {
            str(item.get("rr_id", "") or "").strip()
            for item in records
            if isinstance(item, dict)
        }
        sequence = max(1, len(records) + 1)
        candidate = f"rr-{sequence}"
        while candidate in existing_ids:
            sequence += 1
            candidate = f"rr-{sequence}"
        return candidate

    def _parse_rr_decimal(self, text: str, field_name: str) -> Decimal:
        value = Decimal(str(text or "").strip())
        if value <= 0:
            raise RuntimeError(f"{field_name}必须大于 0。")
        return value

    def _rr_fee_offset_active(self, item: dict[str, object] | None = None) -> bool:
        if isinstance(item, dict) and "fee_offset_enabled" in item:
            return _rr_fee_offset_enabled(item.get("fee_offset_enabled"))
        return bool(self._rr_fee_offset_check.isChecked())

    def _normalized_rr_payload(
        self,
        payload: dict[str, object],
        *,
        derive_r_from_take_profit: bool = False,
    ) -> dict[str, object]:
        side = str(payload.get("side", "long") or "long")
        management_mode = _normalize_rr_management_mode(payload.get("management_mode"))
        price_entry = self._parse_rr_decimal(str(payload.get("price_entry", "") or ""), "入场价")
        price_stop = self._parse_rr_decimal(str(payload.get("price_stop", "") or ""), "止损价")
        price_increment = self._current_rr_price_increment(payload)
        fee_offset_enabled = self._rr_fee_offset_active(payload)
        if derive_r_from_take_profit:
            take_profit = self._parse_rr_decimal(str(payload.get("price_tp", "") or ""), "止盈价")
            r_multiple = _compute_rr_multiple_from_take_profit(
                side,
                price_entry,
                price_stop,
                take_profit,
                fee_offset_enabled=fee_offset_enabled,
            )
        else:
            r_multiple = self._parse_rr_decimal(str(payload.get("r_multiple", "") or ""), "R 倍数")
            r_multiple = _normalize_rr_multiple_step(r_multiple)
        price_tp = _compute_rr_take_profit(
            side,
            price_entry,
            price_stop,
            r_multiple,
            fee_offset_enabled=fee_offset_enabled,
            price_increment=price_increment,
        )
        normalized = dict(payload)
        normalized["price_entry"] = decimal_to_text(price_entry)
        normalized["price_stop"] = decimal_to_text(price_stop)
        normalized["price_tp"] = decimal_to_text(price_tp)
        normalized["r_multiple"] = decimal_to_text(r_multiple)
        normalized["management_mode"] = management_mode
        normalized["direct_take_profit_r"] = decimal_to_text(r_multiple)
        management_trigger_r = _rr_management_trigger_r(management_mode)
        if management_trigger_r is None:
            normalized["management_trigger_price"] = ""
        else:
            management_trigger_price = _compute_rr_take_profit(
                side,
                price_entry,
                price_stop,
                management_trigger_r,
                fee_offset_enabled=False,
                price_increment=price_increment,
            )
            normalized["management_trigger_price"] = decimal_to_text(management_trigger_price)
        normalized["fee_offset_enabled"] = fee_offset_enabled
        return normalized

    @Slot()
    def _save_rr_item(self, extra_payload: dict[str, object] | None = None) -> None:
        try:
            entry = self._workspace_entry()
            side = str(self._rr_side_combo.currentData() or "long")
            price_entry = self._parse_rr_decimal(self._rr_entry_edit.text(), "入场价")
            price_stop = self._parse_rr_decimal(self._rr_stop_edit.text(), "止损价")
            r_multiple = self._parse_rr_decimal(self._rr_r_edit.text(), "R 倍数")
            risk_amount = self._parse_rr_decimal(self._rr_risk_edit.text(), "风险金")
            bar_entry = float(str(self._rr_bar_edit.text() or "").strip())
            existing_payload: dict[str, object] = {}
            rr_items = entry.get("rr")
            if isinstance(rr_items, list) and 0 <= self._selected_rr_index < len(rr_items):
                existing_item = rr_items[self._selected_rr_index]
                if isinstance(existing_item, dict):
                    existing_payload = dict(existing_item)
            payload = {
                **existing_payload,
                "rr_id": self._existing_rr_id(entry),
                "trade_ref": str(existing_payload.get("trade_ref", "") or uuid4().hex),
                "side": side,
                "management_mode": str(self._rr_management_mode_combo.currentData() or "fixed_tp"),
                "bar_entry": bar_entry,
                "bar_stop": bar_entry,
                "price_entry": decimal_to_text(price_entry),
                "price_stop": decimal_to_text(price_stop),
                "r_multiple": decimal_to_text(r_multiple),
                "risk_amount": decimal_to_text(risk_amount),
                "fee_offset_enabled": self._rr_fee_offset_check.isChecked(),
                "locked": self._rr_locked_check.isChecked(),
            }
            if isinstance(extra_payload, dict):
                payload.update(extra_payload)
            payload = self._normalized_rr_payload(payload)
            rr_items = entry.get("rr")
            if not isinstance(rr_items, list):
                rr_items = []
                entry["rr"] = rr_items
            if 0 <= self._selected_rr_index < len(rr_items):
                rr_items[self._selected_rr_index] = payload
                selected_index = self._selected_rr_index
            else:
                rr_items.append(payload)
                selected_index = len(rr_items) - 1
            self._save_workspace_snapshot()
            self._populate_rr_table(selected_index=selected_index)
            self._rr_preview.setText(f"自动止盈：{payload['price_tp']}")
            if self._pending_payload is not None:
                self._render_to_chart(self._pending_payload)
            self._set_status("RR 区块已保存。")
        except Exception as exc:  # noqa: BLE001
            self._set_status(f"保存 RR 失败：{exc}")

    @Slot()
    def _remove_rr_item(self) -> None:
        if self._selected_rr_index < 0:
            return
        try:
            payload = self._selected_rr_payload()
        except Exception as exc:  # noqa: BLE001
            self._set_status(f"删除 RR 失败：{exc}")
            return
        if self._purge_placeholder_rr_entry(payload, delete_item=True):
            return
        ledger_entry = self._rr_ledger_entry_for_item(payload)
        if ledger_entry is not None:
            active_statuses = {
                "entry_working",
                "entry_partially_filled",
                "cancel_confirmation_required",
            }
            protected_statuses = {
                "protected",
                "protected_break_even",
                "protected_trailing",
                "protected_cancelled_remainder",
            }
            status = str(ledger_entry.status or "").strip().lower()
            if status in protected_statuses:
                confirmed = QMessageBox.question(
                    self,
                    "删除已成交 RR 图形",
                    "该 RR 已有关联仓位或止损/止盈保护。\n\n"
                    "确认后只删除本地 RR 图形和列表记录；不会平仓，也不会撤销交易所保护单。是否继续？",
                    QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                    QMessageBox.StandardButton.No,
                )
                if confirmed == QMessageBox.StandardButton.Yes:
                    self._delete_rr_visual_keep_protection(ledger_entry)
                else:
                    self._set_status("已取消删除 RR 图形，仓位和保护保持不变。")
                return
            if status in active_statuses:
                runtime = self._runtime
                if runtime is None:
                    self._set_status("当前 RR 有挂单，但 API 不可用，无法先撤单再删除。")
                    return
                self._submit_rr_trade_cancel(
                    entry=ledger_entry,
                    runtime=runtime,
                    confirmed_for_filled=False,
                    delete_after_cancel=True,
                )
                return
        self._delete_rr_item_at_index(self._selected_rr_index)
        self._set_status("RR 区块已删除。")

    def _apply_alert_snapshot(self, snapshot: KlineAlertSnapshot | None) -> None:
        if snapshot is None:
            return
        current_entry = self._workspace_entry()
        updated_entry = normalize_workspace_entry(snapshot.workspace_entry)
        updated_entry["rr"] = [dict(item) for item in current_entry.get("rr", []) if isinstance(item, dict)]
        # The worker snapshot predates foreground replay runs and filter changes.
        # Alert evaluation must not roll back these UI-owned saved results.
        updated_entry["area_replays"].update(copy.deepcopy(current_entry.get("area_replays", {})))
        new_events = list(snapshot.new_events)
        structure = dict(snapshot.structure)
        self._workspace_entries[self._current_workspace_key()] = updated_entry
        email_sent_count = self._dispatch_line_alert_emails(new_events, updated_entry) if new_events else 0
        self._save_workspace_snapshot()
        self._structure_hint.setText(str(structure.get("note", "") or ""))
        self._refresh_event_log()
        self._populate_line_table()
        self._populate_rr_table(selected_index=self._selected_rr_index)
        if new_events:
            self._enqueue_line_trade_events(new_events)
            latest = str(new_events[0].get("message", "") or "")
            email_status = f" | 邮件提醒已提交 {email_sent_count} 封" if email_sent_count else ""
            self._set_status(f"{self._status.text()} | 事件：{latest}{email_status}")

    def _dispatch_line_alert_emails(
        self,
        events: list[dict[str, object]],
        workspace_entry: dict[str, object],
    ) -> int:
        return _deliver_line_alert_emails(
            workspace_entry=workspace_entry,
            events=events,
            symbol=self._symbol_combo.currentText().strip().upper(),
            period=self._period_combo.currentText().strip().upper(),
            notifier=_build_kline_line_email_notifier(),
        )

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
        html = """
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
                const lineSeries = { ema9: null, ema21: null, ema21Fast: null, ema55: null };
                let channelSeries = [];
                let trendSeries = null;
                let volumeSeries = null;
                let atrSeries = null;
                let latestPriceLine = null;
                let trendByTime = {};
                let signalByTime = {};
                let currentCandles = [];
                const chartRecentVisibleBars = __RECENT_VIEW_BARS__;

                function formatChartTime(time) {
                  const date = new Date(Number(time) * 1000);
                  if (!Number.isFinite(date.getTime())) return '-';
                  const weekdays = ['周日', '周一', '周二', '周三', '周四', '周五', '周六'];
                  const pad = (value) => String(value).padStart(2, '0');
                  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} ${weekdays[date.getDay()]} ${pad(date.getHours())}:${pad(date.getMinutes())}`;
                }

                function toTooltipText(time, payload, trendPayload, atrPayload) {
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
                  const dateText = formatChartTime(time);
                  const trendTitle = trendPayload?.title || trendTitleForPeriod(payload?.period);
                  const atrText = atrPayload ? `\nATR 10 ${formatNumber(atrPayload.value)}` : '';
                  return `时间 ${dateText}\n开 ${formatNumber(o)}  高 ${formatNumber(h)}\n低 ${formatNumber(l)}  收 ${formatNumber(c)}\n量 ${formatNumber(v)}${atrText}\n${trendTitle} ${trendText}`;
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
                    localization: {
                      timeFormatter: formatChartTime,
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

                function syncLatestPriceLine(price, previousClose) {
                  if (!candlestickSeries) return;
                  if (latestPriceLine) {
                    candlestickSeries.removePriceLine(latestPriceLine);
                    latestPriceLine = null;
                  }
                  const value = Number(price);
                  if (!Number.isFinite(value) || value <= 0) return;
                  const previous = Number(previousClose);
                  const color = Number.isFinite(previous) && value < previous ? '#e04f84' : '#22c55e';
                  latestPriceLine = candlestickSeries.createPriceLine({
                    price: value,
                    color,
                    lineWidth: 1,
                    lineStyle: 2,
                    lineVisible: false,
                    axisLabelVisible: true,
                    title: '最新',
                  });
                }

                function syncChannelSeries(channels, candles) {
                  for (const series of channelSeries) {
                    chart.removeSeries(series);
                  }
                  channelSeries = [];
                  if (!Array.isArray(channels) || !Array.isArray(candles)) return;
                  for (const item of channels) {
                    const startIndex = Number(item?.start_index);
                    const endIndex = Number(item?.end_index);
                    const start = candles[startIndex];
                    const end = candles[endIndex];
                    if (!start || !end) continue;
                    const color = typeof item?.outline === 'string' ? item.outline : '#2563eb';
                    for (const side of ['upper', 'lower']) {
                      const startValue = Number(item?.[`${side}_start`]);
                      const endValue = Number(item?.[`${side}_end`]);
                      if (!Number.isFinite(startValue) || !Number.isFinite(endValue)) continue;
                      const series = chart.addLineSeries({
                        color,
                        lineWidth: 2,
                        title: side === 'upper' ? '自动通道上轨' : '自动通道下轨',
                        priceLineVisible: false,
                        lastValueVisible: false,
                      });
                      series.setData([
                        { time: Number(start.time), value: startValue },
                        { time: Number(end.time), value: endValue },
                      ]);
                      channelSeries.push(series);
                    }
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

                function _resolveRecentRange(candles) {
                  if (!Array.isArray(candles) || candles.length === 0) return null;
                  const safeBars = Number(chartRecentVisibleBars);
                  const visibleBars = Number.isFinite(safeBars) ? Math.max(1, Math.floor(safeBars)) : __RECENT_VIEW_BARS__;
                  const span = Math.min(visibleBars, candles.length);
                  const to = Number(candles[candles.length - 1]?.time);
                  const from = Number(candles[Math.max(0, candles.length - span)]?.time);
                  if (!Number.isFinite(to) || !Number.isFinite(from)) return null;
                  return { from, to };
                }

                function applyChartViewMode(mode) {
                  if (!chart || !candlestickSeries) return;
                  const normalized = String(mode || window.__chartViewMode || "recent").trim().toLowerCase();
                  window.__chartViewMode = normalized === "full" ? "full" : "recent";
                  if (window.__chartViewMode === "full") {
                    chart.timeScale().fitContent();
                    return;
                  }
                  const range = _resolveRecentRange(currentCandles);
                  if (!range) {
                    chart.timeScale().fitContent();
                    return;
                  }
                  chart.timeScale().setVisibleRange({ from: range.from, to: range.to });
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
                    currentCandles = candles;
                    candlestickSeries.setData(candles);
                    syncLatestPriceLine(
                      candles.length ? candles[candles.length - 1]?.close : null,
                      candles.length > 1 ? candles[candles.length - 2]?.close : null,
                    );
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
                    if (safePayload.show?.atr10) {
                      if (!atrSeries) {
                        atrSeries = chart.addLineSeries({
                          priceScaleId: 'atr10',
                          color: '#f59e0b',
                          lineWidth: 2,
                          title: 'ATR 10',
                          priceLineVisible: false,
                          lastValueVisible: true,
                        });
                        chart.priceScale('atr10').applyOptions({
                          scaleMargins: { top: 0.66, bottom: 0.20 },
                          borderVisible: false,
                        });
                      }
                      atrSeries.setData(Array.isArray(safePayload.atr10) ? safePayload.atr10 : []);
                    } else if (atrSeries) {
                      chart.removeSeries(atrSeries);
                      atrSeries = null;
                    }
                    syncTrendSeries(
                      Array.isArray(safePayload.trend) ? safePayload.trend : [],
                      safePayload.period === '1D' || safePayload.period === '1W'
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
                    syncLineSeries(
                      'ema21Fast',
                      emaData.ema21Fast,
                      safePayload.show?.emaPair,
                      { color: '#38bdf8', lineWidth: 2, title: 'EMA 21', priceLineVisible: false, lastValueVisible: false }
                    );
                    syncLineSeries(
                      'ema55',
                      emaData.ema55,
                      safePayload.show?.emaPair,
                      { color: '#a78bfa', lineWidth: 3, title: 'EMA 55', priceLineVisible: false, lastValueVisible: false }
                    );
                    syncChannelSeries(Array.isArray(safePayload.channels) ? safePayload.channels : [], candles);
                    applyChartViewMode(window.__chartViewMode || 'recent');
                  } catch (error) {
                    if (window.console && window.console.error) {
                      window.console.error('[applyChartData]', error);
                    }
                    handleChartWarning(`璧板娍鍥炬覆鏌撳紓甯革細${String(error)}`);
                  }
                }

                function updateRealtimeCandle(payload) {
                  try {
                    if (!chart || !candlestickSeries || !payload || !payload.candle) return;
                    const candle = payload.candle;
                    const time = Number(candle.time) || 0;
                    if (!time) return;
                    const index = currentCandles.findIndex((item) => Number(item?.time) === time);
                    if (index >= 0) currentCandles[index] = candle;
                    else currentCandles.push(candle);
                    candlestickSeries.update(candle);
                    const previousClose = currentCandles.length > 1
                      ? currentCandles[currentCandles.length - 2]?.close
                      : null;
                    syncLatestPriceLine(candle.close, previousClose);
                    if (volumeSeries) {
                      const color = candle.close >= candle.open ? 'rgba(34, 197, 94, 0.85)' : 'rgba(224, 79, 132, 0.85)';
                      volumeSeries.update({ time, value: Number(candle.volume) || 0, color });
                    }
                    if (payload.atr10 && atrSeries && Number.isFinite(Number(payload.atr10.value))) {
                      atrSeries.update(payload.atr10);
                    }
                    if (payload.ema9 && lineSeries.ema9) lineSeries.ema9.update(payload.ema9);
                    if (payload.ema21 && lineSeries.ema21) lineSeries.ema21.update(payload.ema21);
                    if (payload.ema21Fast && lineSeries.ema21Fast) lineSeries.ema21Fast.update(payload.ema21Fast);
                    if (payload.ema55 && lineSeries.ema55) lineSeries.ema55.update(payload.ema55);
                    if (window.__chartViewMode === 'recent') applyChartViewMode('recent');
                  } catch (error) {
                    if (window.console && window.console.error) window.console.error('[updateRealtimeCandle]', error);
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
                    const atrPayload = atrSeries ? param.seriesData.get(atrSeries) : null;
                    const text = toTooltipText(param.time, candleData, trendPayload, atrPayload);
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
        return html.replace("__RECENT_VIEW_BARS__", str(_RECENT_VIEW_BARS))
