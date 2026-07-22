from __future__ import annotations

import json
import math
import os
import re
import sys
import time
from dataclasses import dataclass, field, replace
from datetime import datetime
from decimal import Decimal
from typing import Any, Callable
from uuid import uuid4

from PySide6.QtCore import QDateTime, QMargins, QObject, QPointF, QRectF, QTimer, Qt, QThread, Signal, Slot
from PySide6.QtGui import QAction, QColor, QPainter, QPen
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
    QSpinBox,
    QTabWidget,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QVBoxLayout,
    QWidget,
    QWidgetAction,
)

from okx_quant.candle_cache import load_candle_cache
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
    _hourly_fetch_start_ts,
    _hourly_history_limit,
    _merge_deribit_candles,
    _merge_price_candles,
    _to_average_price_candles,
    _to_average_volatility_candles,
)
from okx_quant.kline_rr_execution import RRTradeExecutionService
from okx_quant.kline_rr_trade import RRTradeLedgerEntry, RRTradePlan, build_rr_trade_plan
from okx_quant.okx_client import OkxRestClient
from okx_quant.engine import _dynamic_two_taker_fee_offset_live
from okx_quant.pricing import format_decimal, format_decimal_by_increment, format_decimal_fixed, snap_to_increment
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
from roll_terminal_qt.line_trading_core import (
    compute_rr_target,
    decimal_to_text,
    drag_rr_annotation,
    rr_annotation_from_payload,
    rr_annotation_to_payload,
)
from roll_terminal_qt.kline_box_rules import AUTO_BOX_MAX_CANDIDATES, is_auto_box_candidate_valid
from roll_terminal_qt.perf_metrics import measure_ui_step
from roll_terminal_qt.kline_alerts import (
    build_workspace_key,
    evaluate_workspace_alerts,
    line_value_at,
    make_line_rule,
    normalize_workspace_entry,
)
from roll_terminal_qt.kline_account_drawer import KlineAccountDrawer
from roll_terminal_qt.profile_access import ensure_profile_unlocked, load_profile_snapshots
from roll_terminal_qt.runtime import load_runtime, profile_names
from roll_terminal_qt.workspace_shell import LocalTaskCount, merge_local_task_counts


_INITIAL_WINDOW_LOAD_DELAY_MS = 80
_NATIVE_BOOTSTRAP_RENDER_BARS = 360
_NATIVE_BOOTSTRAP_RENDER_DELAY_MS = 90
_AUTO_REFRESH_DEFAULT_ENABLED = True
_NATIVE_RIGHT_PADDING_BARS = 24
_RECENT_VIEW_BARS = 240
_KLINE_PAYLOAD_CACHE_LIMIT = 8
_KLINE_SPLITTER_LEFT_RATIO = 0.11
_VOLUME_OVERLAY_HEIGHT_RATIO = 0.18
_EMA15_LINE_WIDTH = 2
_SMA50_LINE_WIDTH = 3
_SECONDARY_CHART_TOP_RATIO = 0.31
_SECONDARY_CHART_SIDE_RATIO = 0.56
_SECONDARY_CHART_SPLITTER_HANDLE_WIDTH = 10
_HEADER_SYMBOL_INPUT_MIN_WIDTH = 220
_HEADER_SYMBOL_INPUT_MAX_WIDTH = 360
_RR_BOX_WIDTH_BARS = 6
_RR_MULTIPLE_STEP = Decimal("0.1")
_RR_DRAG_ACTIVATION_DISTANCE_PX = 6.0
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
    item = _load_deribit_volatility_cache_payload().get(_deribit_hourly_cache_key(currency))
    if not isinstance(item, dict):
        return None
    try:
        volatility_candles = [
            DeribitVolatilityCandle(
                ts=int(candle["ts"]),
                open=Decimal(str(candle["open"])),
                high=Decimal(str(candle["high"])),
                low=Decimal(str(candle["low"])),
                close=Decimal(str(candle["close"])),
            )
            for candle in item.get("volatility_hourly", [])
        ]
        spot_candles = [
            Candle(
                ts=int(candle["ts"]),
                open=Decimal(str(candle["open"])),
                high=Decimal(str(candle["high"])),
                low=Decimal(str(candle["low"])),
                close=Decimal(str(candle["close"])),
                volume=Decimal(str(candle.get("volume", "0"))),
                confirmed=bool(candle.get("confirmed", True)),
            )
            for candle in item.get("spot_hourly", [])
        ]
        if not volatility_candles or not spot_candles:
            return None
        return (
            str(item.get("spot_inst_id", OKX_SPOT_SYMBOLS[currency])),
            volatility_candles,
            [candle for candle in spot_candles if candle.confirmed],
            datetime.fromisoformat(str(item["fetched_at"])),
        )
    except Exception:
        return None


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


def _compute_axis_y_padding(min_price: float, max_price: float) -> tuple[float, float]:
    price_span = max(float(max_price) - float(min_price), 0.0)
    reference_price = max(abs(float(min_price)), abs(float(max_price)))
    baseline = max(price_span, reference_price * 0.002, 1e-8)
    top_padding = max(price_span * 0.08, reference_price * 0.002, 1e-8)
    reserved_ratio = min(0.42, max(_VOLUME_OVERLAY_HEIGHT_RATIO + 0.03, 0.12))
    visible_ratio = max(0.18, 1.0 - reserved_ratio)
    bottom_padding = max((price_span + top_padding) * (reserved_ratio / visible_ratio), baseline * 0.12)
    return top_padding, bottom_padding


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
        parameter_layout.addRow("风险金额", self._risk_edit)
        parameter_layout.addRow("入场价", self._entry_edit)
        parameter_layout.addRow("止损价", self._stop_edit)
        parameter_layout.addRow("R 倍数", self._r_edit)
        parameter_layout.addRow("杠杆", self._leverage_edit)
        parameter_layout.addRow(self._fee_offset_check)
        parameter_layout.addRow(self._locked_check)
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

        for widget in (self._side_combo, self._management_mode_combo, self._risk_edit, self._entry_edit, self._stop_edit, self._r_edit, self._leverage_edit):
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
        try:
            side = str(self._side_combo.currentData() or "long")
            management_mode = str(self._management_mode_combo.currentData() or "fixed_tp")
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
                        f"实际风险：{snapshot['card_actual_risk_text']}",
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
) -> int | None:
    if index < 0 or index >= len(candles):
        return None
    start = max(0, index - max(int(lookback), 1) + 1)
    metric_func = _candle_amplitude if str(metric).strip().lower() == "range" else _candle_body_size
    target = metric_func(candles[index])
    if target <= 0:
        return None
    larger_count = 0
    for candle in candles[start : index + 1]:
        if metric_func(candle) > target:
            larger_count += 1
    return larger_count + 1


def _shadow_rank_in_recent_window(
    candles: list[Any],
    index: int,
    *,
    lookback: int = 10,
    side: str,
) -> int | None:
    if index < 0 or index >= len(candles):
        return None
    start = max(0, index - max(int(lookback), 1) + 1)
    target = _candle_shadow_size(candles[index], side=side)
    if target <= 0:
        return None
    larger_count = 0
    for candle in candles[start : index + 1]:
        if _candle_shadow_size(candle, side=side) > target:
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
) -> tuple[int, int] | None:
    best: tuple[int, int] | None = None
    for index in _signal_pattern_index_range(signal_index=signal_index, candle_count=candle_count):
        if index < 0 or index >= len(candles):
            continue
        rank = _rank_in_recent_window(candles, index, lookback=lookback, metric=metric)
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


def _double_reversal_reclaims_first_body(
    *,
    first: Any,
    second: Any,
    direction: str,
) -> bool:
    first_open = _to_float(first.open)
    first_close = _to_float(first.close)
    second_close = _to_float(second.close)
    body_midpoint = (first_open + first_close) / 2.0
    if direction == "long":
        return second_close >= body_midpoint
    return second_close <= body_midpoint


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
        if signal.pattern_id in {"false_breakdown", "false_breakout"}:
            continue
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
        core_body_candle = _signal_core_amplitude_candle(
            candles=candles,
            signal_index=index,
            candle_count=int(signal.candle_count),
            lookback=10,
            top_n=4,
            metric="body",
        )
        core_range_candle = _signal_core_amplitude_candle(
            candles=candles,
            signal_index=index,
            candle_count=int(signal.candle_count),
            lookback=10,
            top_n=4,
            metric="range",
        )
        if core_body_candle is None and core_range_candle is None:
            continue
        core_index, core_rank = core_body_candle or core_range_candle  # default display metric is body.
        core_item = candles[core_index]
        core_ma_touch = _candle_touches_enabled_ma(
            candle=core_item,
            index=core_index,
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
        key = (index, signal.pattern_id)
        if key in seen:
            continue
        seen.add(key)
        direction = str(signal.direction or "neutral").strip().lower()
        color = _REPLAY_SIGNAL_LONG_COLOR if direction == "long" else _REPLAY_SIGNAL_SHORT_COLOR
        label = _REPLAY_SIGNAL_LABELS.get(signal.pattern_id, signal.pattern_name)
        text_suffix = f"核心K前{core_rank}"
        if nearest_name != "-" and nearest_distance is not None and nearest_distance <= _REPLAY_SIGNAL_NEAR_MA_MAX_PCT:
            text_suffix = f"{text_suffix} @{nearest_name}"
        shadow_rank = None
        if signal.pattern_id == "long_upper_shadow":
            shadow_rank = _shadow_rank_in_recent_window(candles, index, lookback=10, side="upper")
        elif signal.pattern_id == "long_lower_shadow":
            shadow_rank = _shadow_rank_in_recent_window(candles, index, lookback=10, side="lower")
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
                "core_range_index": core_range_candle[0] if core_range_candle is not None else None,
                "core_range_rank": core_range_candle[1] if core_range_candle is not None else None,
                "shadow_rank": shadow_rank,
                "core_ma_touch": core_ma_touch,
                "score": int(signal.score),
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
        core_body_candle = _signal_core_amplitude_candle(
            candles=candles,
            signal_index=index,
            candle_count=2,
            lookback=10,
            top_n=4,
            metric="body",
        )
        core_range_candle = _signal_core_amplitude_candle(
            candles=candles,
            signal_index=index,
            candle_count=2,
            lookback=10,
            top_n=4,
            metric="range",
        )
        if core_body_candle is None and core_range_candle is None:
            continue
        key = (index, pattern_id)
        if key in seen:
            continue
        seen.add(key)
        core_index, core_rank = core_body_candle or core_range_candle  # default display metric is body.
        core_item = candles[core_index]
        core_ma_touch = _candle_touches_enabled_ma(
            candle=core_item,
            index=core_index,
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
        text_suffix = f"核心K前{core_rank}"
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
                "core_range_index": core_range_candle[0] if core_range_candle is not None else None,
                "core_range_rank": core_range_candle[1] if core_range_candle is not None else None,
                "core_ma_touch": core_ma_touch,
                "score": 62 + max(0, 4 - core_rank),
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
        candidates: list[tuple[str, str, str, int | None]] = []
        if upper_shadow >= (body * 2.0) and upper_shadow >= lower_shadow:
            candidates.append(("long_upper_shadow", "short", "upper", _shadow_rank_in_recent_window(candles, index, lookback=10, side="upper")))
        if lower_shadow >= (body * 2.0) and lower_shadow >= upper_shadow:
            candidates.append(("long_lower_shadow", "long", "lower", _shadow_rank_in_recent_window(candles, index, lookback=10, side="lower")))
        if not candidates:
            continue
        for pattern_id, direction, side, shadow_rank in candidates:
            if shadow_rank is None or shadow_rank > 4:
                continue
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
            text_suffix = f"影线前{shadow_rank}"
            if nearest_name != "-" and nearest_distance is not None and nearest_distance <= _REPLAY_SIGNAL_NEAR_MA_MAX_PCT:
                text_suffix = f"{text_suffix} @{nearest_name}"
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
                    "core_amplitude_rank": None,
                    "core_body_index": None,
                    "core_body_rank": None,
                    "core_range_index": None,
                    "core_range_rank": None,
                    "shadow_rank": shadow_rank,
                    "core_ma_touch": core_ma_touch,
                    "score": 62 + max(0, 4 - shadow_rank),
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
    snapshot = build_auto_channel_live_chart_snapshot(
        session_id="kline-auto-channel",
        candles=list(candles),
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
                "start_index": start_index,
                "end_index": end_index,
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
            self._workspace_rr_items: list[dict[str, object]] = []
            self._signal_markers: list[dict[str, Any]] = []
            self._box_overlays: list[dict[str, Any]] = []
            self._channel_overlays: list[dict[str, Any]] = []
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
            workspace_rr_items: list[dict[str, object]] | None = None,
            trend_indicators: list[dict[str, Any]] | None = None,
            signal_markers: list[dict[str, Any]] | None = None,
            box_overlays: list[dict[str, Any]] | None = None,
            channel_overlays: list[dict[str, Any]] | None = None,
            selected_workspace_line_index: int = -1,
            selected_workspace_rr_index: int = -1,
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
            self._workspace_rr_items = [dict(item) for item in (workspace_rr_items or []) if isinstance(item, dict)]
            self._signal_markers = [dict(item) for item in (signal_markers or []) if isinstance(item, dict)]
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

        def clear_chart_context(self) -> None:
            self._axis_x = None
            self._axis_y = None
            self._candles = []
            self._overlay_values = []
            self._trend_indicators = []
            self._display_times_ms = []
            self._indicator_series = []
            self._chart_note_lines = []
            self._workspace_lines = []
            self._workspace_rr_items = []
            self._signal_markers = []
            self._box_overlays = []
            self._channel_overlays = []
            self._selected_workspace_rr_index = -1
            self._preview_rr_item = None
            self._hover_pos = None
            self._press_pos = None
            self._last_pointer_pos = None
            self._pan_anchor_x = None
            self._dragging = False
            self._external_hover_time = None
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

        def set_recent_view_range(self) -> None:
            self.reset_view()

        def set_full_view_range(self) -> None:
            if self._axis_x is None:
                return
            start_x, end_x = self._full_x_min, self._full_x_max
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
                self._press_pos = QPointF(event.position())
                self._last_pointer_pos = QPointF(event.position())
                self._pan_anchor_x = float(event.position().x())
                self._dragging = False
                point = self.chart().mapToValue(event.position())
                self.chartPointerPressed.emit(float(point.x()), float(point.y()))
            super().mousePressEvent(event)

        def mouseMoveEvent(self, event) -> None:  # noqa: ANN001
            plot_area = self.chart().plotArea()
            if plot_area.contains(event.position()):
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
                self._last_pointer_pos = QPointF(event.position())
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
                self._draw_volume_overlay(painter, plot_area)
                self._draw_channel_overlays(painter, plot_area)
                self._draw_box_overlays(painter, plot_area)
                self._draw_workspace_rr_items(painter, plot_area)
                self._draw_preview_rr_item(painter, plot_area)
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
            top_padding, bottom_padding = _compute_axis_y_padding(min_price, max_price)
            axis_y.setRange(min_price - bottom_padding, max_price + top_padding)

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
                lines.append(f"信号 {label} | 均线距离 {distance:.2f}% | 评分 {score}")
            return tuple(lines)

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
            volume_reserved_height = max(42.0, float(bounds.height()) * _VOLUME_OVERLAY_HEIGHT_RATIO)
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
        currency: str,
        period: str,
        limit: int,
        average_kline: bool,
    ) -> None:
        super().__init__()
        self._request_id = request_id
        self._currency = currency.strip().upper()
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


class KlineAnalysisWindow(QMainWindow):
    _realtime_candle_received = Signal(object)

    def __init__(self, *, embedded: bool = False) -> None:
        super().__init__()
        _debug_log("[kline] __init__ begin")
        self._embedded = bool(embedded)
        if self._embedded:
            self.setWindowFlags(Qt.WindowType.Widget)
        else:
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
        self._tertiary_request_id = 0
        self._active_tertiary_request_id = 0
        self._active_tertiary_request_key: tuple[Any, ...] | None = None
        self._loaded_tertiary_request_key: tuple[Any, ...] | None = None
        self._tertiary_loader: KlineDataLoader | None = None
        self._use_native_chart = bool(
            QChartView is not None and (_prefer_native_chart_backend() or QWebEngineView is None)
        )
        self._page_ready = self._use_native_chart
        self._pending_payload: KlineChartPayload | None = None
        self._secondary_pending_payload: KlineChartPayload | None = None
        self._tertiary_pending_payload: KlineChartPayload | None = None
        self._primary_payload_cache: dict[tuple[Any, ...], KlineChartPayload] = {}
        self._secondary_payload_cache: dict[tuple[Any, ...], KlineChartPayload] = {}
        self._tertiary_payload_cache: dict[tuple[Any, ...], KlineChartPayload] = {}
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
        self._primary_chart_frame = None
        self._secondary_native_chart = None
        self._secondary_native_chart_view = None
        self._secondary_chart_frame = None
        self._tertiary_native_chart = None
        self._tertiary_native_chart_view = None
        self._tertiary_chart_frame = None
        self._chart_stack_splitter = None
        self._primary_period_buttons: dict[str, QPushButton] = {}
        self._active_chart_target = "primary"
        self._chart_mode_cycle_btn: QPushButton | None = None
        self._chart_range_mode_btn: QPushButton | None = None
        self._chart_view_range_mode = "recent"
        self._secondary_layout_cycle_btn: QPushButton | None = None
        self._secondary_chart_kind_btn: QPushButton | None = None
        self._secondary_sync_period_btn: QPushButton | None = None
        self._secondary_layout_mode_value = "vertical"
        self._secondary_chart_kind_mode = "kline"
        self._shape_signal_size_metric = "body"
        self._initial_load_requested = False
        self._splitter_default_applied = False
        self._native_chart_bootstrap_complete = False
        self._deferred_chart_payload: KlineChartPayload | None = None
        self._deferred_chart_request_id = 0
        self._body_splitter: QSplitter | None = None
        self._chart_account_splitter: QSplitter | None = None
        self._chart_host: QWidget | None = None
        self._control_panel: QFrame | None = None
        self._control_scroll: QScrollArea | None = None
        self._account_drawer: KlineAccountDrawer | None = None
        self._orders_drawer_button: QPushButton | None = None
        self._positions_drawer_button: QPushButton | None = None
        self._left_panel_hidden = False
        self._secondary_volatility_loader: SecondaryVolatilityDataLoader | None = None
        self._syncing_chart_range = False
        self._pending_reload_after_load = False
        self._primary_chart_status_text = ""
        self._secondary_chart_status_text = ""
        self._runtime = load_runtime("moni") or load_runtime()
        self._market_client = OkxRestClient()
        self._realtime_candle_key: CandleStreamKey | None = None
        self._realtime_candle_unsubscribe: Callable[[], None] | None = None
        self._realtime_candle_received.connect(self._apply_realtime_candle)
        self._instrument_cache: dict[str, object | None] = {}
        self._rr_trade_ledger_snapshot = load_kline_rr_trade_ledger_snapshot()
        self._rr_trade_execution_service = RRTradeExecutionService()
        self._rr_execution_thread: QThread | None = None
        self._rr_execution_in_flight = False
        self._rr_monitor_cursor = 0
        self._pending_rr_execution_requests: list[dict[str, object]] = []
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

        self._build_header(main_layout)
        self._build_body(main_layout)
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
        self._rr_monitor_timer.start()
        self._deferred_chart_render_timer = QTimer(self)
        self._deferred_chart_render_timer.setSingleShot(True)
        self._deferred_chart_render_timer.timeout.connect(self._render_deferred_full_chart)
        self._layout_refresh_timer = QTimer(self)
        self._layout_refresh_timer.setSingleShot(True)
        self._layout_refresh_timer.timeout.connect(self._refresh_chart_layout_after_window_change)
        _debug_log("[kline] __init__ ready")

    def showEvent(self, event) -> None:  # noqa: ANN001
        super().showEvent(event)
        _debug_log("[kline] showEvent")
        self._apply_default_splitter_sizes()
        self._schedule_chart_layout_refresh(80)
        if self._initial_load_requested:
            return
        self._initial_load_requested = True
        if self._auto_refresh_btn.isChecked() and not self._refresh_timer.isActive():
            self._refresh_timer.start()
        self._set_status("窗口已就绪，正在加载首屏图表...")
        QTimer.singleShot(_INITIAL_WINDOW_LOAD_DELAY_MS, self._load_data)

    def set_page_active(self, active: bool) -> None:
        if not active:
            self._refresh_timer.stop()
            return
        if self._auto_refresh_btn.isChecked():
            self._refresh_timer.start()
        if self._pending_payload is not None and self._page_ready:
            self._render_loaded_payload(self._pending_payload)

    def workspace_profile_name(self) -> str:
        return self._active_profile_name()

    def apply_workspace_profile(self, profile_name: str) -> None:
        target = profile_name.strip()
        if not target or target == self._last_profile_name:
            return
        runtime = load_runtime(target)
        if runtime is None:
            return
        self._runtime = runtime
        self._last_profile_name = target
        self._sync_account_context()
        self._sync_account_drawer_context()
        self._load_data()

    def pattern_signals_enabled(self) -> bool:
        return any(
            checkbox.isChecked()
            for checkbox in (
                self._show_1h_shape_signal_check,
                self._show_4h_shape_signal_check,
                self._show_1d_shape_signal_check,
            )
        )

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
            return
        self._shutdown_requested = True
        self._refresh_timer.stop()
        self._rr_monitor_timer.stop()
        if self._realtime_candle_unsubscribe is not None:
            try:
                self._realtime_candle_unsubscribe()
            except Exception:
                pass
            self._realtime_candle_unsubscribe = None
        for loader in (
            getattr(self, "_loader", None),
            getattr(self, "_secondary_loader", None),
            getattr(self, "_tertiary_loader", None),
            getattr(self, "_secondary_volatility_loader", None),
        ):
            if loader is not None and loader.isRunning():
                loader.requestInterruption()
        KlineAnalysisWindow._poll_shutdown_loaders(self)

    def _poll_shutdown_loaders(self) -> None:
        active = any(
            loader is not None and loader.isRunning()
            for loader in (
                getattr(self, "_loader", None),
                getattr(self, "_secondary_loader", None),
                getattr(self, "_tertiary_loader", None),
                getattr(self, "_secondary_volatility_loader", None),
            )
        )
        if active:
            QTimer.singleShot(50, lambda: KlineAnalysisWindow._poll_shutdown_loaders(self))
            return
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

        self._toggle_left_panel_btn = QPushButton("隐藏左栏")
        self._toggle_left_panel_btn.setCheckable(True)
        self._toggle_left_panel_btn.toggled.connect(self._toggle_left_panel)
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
        top_row.addWidget(ma_group, 0)

        shape_signal_tooltip = (
            "形态说明：1H/4H/1D 显示核心标志K触发的形态信号。\n"
            "核心K按振幅排序，需在含自身最近10根K线中排前4。\n"
            "勾选碰均线后，核心K还需触碰对应均线；1H 只参考 SMA50，4H/1D 参考 EMA15 或 MA50。"
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

        self._shape_signal_size_metric_btn = QPushButton("")
        self._shape_signal_size_metric_btn.setToolTip(shape_signal_tooltip)
        self._shape_signal_size_metric_btn.clicked.connect(self._toggle_shape_signal_size_metric)
        shape_group_layout.addWidget(self._shape_signal_size_metric_btn)

        self._shape_signal_ma_touch_check = QCheckBox("碰均线")
        self._shape_signal_ma_touch_check.setChecked(False)
        self._shape_signal_ma_touch_check.setToolTip(shape_signal_tooltip)
        self._shape_signal_ma_touch_check.toggled.connect(self._sync_chart_options)
        shape_group_layout.addWidget(self._shape_signal_ma_touch_check)
        top_row.addWidget(shape_group, 0)

        self._shape_settings_button = QPushButton("形态：关")
        self._shape_settings_button.setToolTip(shape_signal_tooltip)
        shape_menu = QMenu(self._shape_settings_button)
        self._shape_setting_actions: dict[str, QAction] = {}
        for label, checkbox in (
            ("1H", self._show_1h_shape_signal_check),
            ("4H", self._show_4h_shape_signal_check),
            ("1D", self._show_1d_shape_signal_check),
            ("碰均线", self._shape_signal_ma_touch_check),
        ):
            action = shape_menu.addAction(label)
            action.setCheckable(True)
            action.setChecked(checkbox.isChecked())
            action.toggled.connect(checkbox.setChecked)
            checkbox.toggled.connect(action.setChecked)
            checkbox.toggled.connect(self._refresh_compact_shape_button)
            self._shape_setting_actions[label] = action
        self._shape_settings_button.setMenu(shape_menu)
        top_row.addWidget(self._shape_settings_button, 0)
        if self._embedded:
            self._shape_signal_group.hide()
        else:
            self._shape_settings_button.hide()

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

        self._secondary_symbol_label = QLabel("副图交易对")
        action_row.addWidget(self._secondary_symbol_label)
        self._secondary_symbol_combo = QComboBox()
        self._secondary_symbol_combo.addItems(KLINE_SYMBOL_OPTIONS)
        self._secondary_symbol_combo.setMinimumWidth(_HEADER_SYMBOL_INPUT_MIN_WIDTH)
        self._secondary_symbol_combo.setMaximumWidth(_HEADER_SYMBOL_INPUT_MAX_WIDTH)
        self._secondary_symbol_combo.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Fixed)
        self._secondary_symbol_combo.setEnabled(False)
        self._secondary_symbol_combo.hide()
        self._secondary_symbol_combo.currentTextChanged.connect(self._on_secondary_symbol_changed)
        action_row.addWidget(self._secondary_symbol_combo, 0)
        self._secondary_symbol_label.hide()

        self._tertiary_chart_check = QCheckBox("三图联动")
        self._tertiary_chart_check.toggled.connect(self._on_tertiary_chart_toggled)
        action_row.addWidget(self._tertiary_chart_check)
        self._tertiary_symbol_label = QLabel("第三图交易对")
        action_row.addWidget(self._tertiary_symbol_label)
        self._tertiary_symbol_combo = QComboBox()
        self._tertiary_symbol_combo.addItems(KLINE_SYMBOL_OPTIONS)
        self._tertiary_symbol_combo.setMinimumWidth(_HEADER_SYMBOL_INPUT_MIN_WIDTH)
        self._tertiary_symbol_combo.setMaximumWidth(_HEADER_SYMBOL_INPUT_MAX_WIDTH)
        self._tertiary_symbol_combo.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Fixed)
        self._tertiary_symbol_combo.setEnabled(False)
        self._tertiary_symbol_combo.hide()
        self._tertiary_symbol_combo.currentTextChanged.connect(self._on_tertiary_symbol_changed)
        action_row.addWidget(self._tertiary_symbol_combo, 0)
        self._tertiary_symbol_label.hide()
        self._tertiary_period_label = QLabel("第三图周期")
        action_row.addWidget(self._tertiary_period_label)
        self._tertiary_period_combo = QComboBox()
        self._tertiary_period_combo.addItems([period for _, period in _PRIMARY_PERIOD_OPTIONS])
        self._tertiary_period_combo.setCurrentText(_DEFAULT_DUAL_SECONDARY_PERIOD)
        self._tertiary_period_combo.setEnabled(False)
        self._tertiary_period_combo.hide()
        self._tertiary_period_combo.currentTextChanged.connect(self._on_tertiary_period_changed)
        action_row.addWidget(self._tertiary_period_combo, 0)
        self._tertiary_period_label.hide()

        self._secondary_period_label = QLabel("副图周期")
        action_row.addWidget(self._secondary_period_label)
        self._secondary_period_combo = QComboBox()
        self._secondary_period_combo.addItems([period for _, period in _PRIMARY_PERIOD_OPTIONS])
        self._secondary_period_combo.addItem("1Dutc")
        self._secondary_period_combo.setCurrentText(_DEFAULT_DUAL_SECONDARY_PERIOD)
        self._secondary_period_combo.setEnabled(False)
        self._secondary_period_combo.hide()
        self._secondary_period_combo.currentTextChanged.connect(self._on_secondary_period_changed)
        action_row.addWidget(self._secondary_period_combo, 0)
        self._secondary_period_label.hide()

        self._secondary_layout_cycle_btn = QPushButton("")
        self._secondary_layout_cycle_btn.setEnabled(False)
        self._secondary_layout_cycle_btn.clicked.connect(self._on_secondary_layout_cycle_clicked)
        action_row.addWidget(self._secondary_layout_cycle_btn)

        self._secondary_chart_kind_btn = QPushButton("")
        self._secondary_chart_kind_btn.setEnabled(False)
        self._secondary_chart_kind_btn.clicked.connect(self._on_secondary_chart_kind_cycle_clicked)
        action_row.addWidget(self._secondary_chart_kind_btn)

        self._secondary_sync_period_btn = QPushButton("")
        self._secondary_sync_period_btn.setEnabled(False)
        self._secondary_sync_period_btn.setToolTip("副图为K线时：主图1D、副图4H并切换到最近视图")
        self._secondary_sync_period_btn.clicked.connect(self._on_secondary_sync_period_clicked)
        action_row.addWidget(self._secondary_sync_period_btn)

        self._daily_timezone_compare_btn = QPushButton("UTC+8/UTC日线")
        self._daily_timezone_compare_btn.setToolTip("同一交易对左右比较：左图 UTC+8 日线，右图 UTC 日线")
        self._daily_timezone_compare_btn.clicked.connect(self._on_daily_timezone_compare_clicked)
        action_row.addWidget(self._daily_timezone_compare_btn)

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
        self._secondary_average_kline_check.setToolTip("开启后，主图和副图都使用平均K线算法显示K线。")
        self._secondary_average_kline_check.toggled.connect(self._on_secondary_average_kline_toggled)
        action_row.addWidget(self._secondary_average_kline_check)

        self._primary_average_secondary_normal_check = QCheckBox("主均副普")
        self._primary_average_secondary_normal_check.setToolTip(
            "仅在双图且副图为K线时生效：左右分屏为左图平均、右图正常；上下分屏为上图平均、下图正常。"
        )
        self._primary_average_secondary_normal_check.setEnabled(False)
        self._primary_average_secondary_normal_check.toggled.connect(
            self._on_primary_average_secondary_normal_toggled
        )
        action_row.addWidget(self._primary_average_secondary_normal_check)

        self._reverse_kline_check = QCheckBox("K线反转")
        self._reverse_kline_check.setToolTip("开启后，将当前主图及副图K线按价格镜像反转显示；波动率副图不参与反转。")
        self._reverse_kline_check.toggled.connect(self._load_data)
        action_row.addWidget(self._reverse_kline_check)

        self._hide_chart_btn = QPushButton("隐藏图表")
        self._hide_chart_btn.setCheckable(True)
        self._hide_chart_btn.toggled.connect(self._toggle_chart_visibility)
        action_row.addWidget(self._hide_chart_btn)

        self._orders_drawer_button = QPushButton("委托")
        self._orders_drawer_button.clicked.connect(lambda: self._show_account_drawer("orders"))
        action_row.addWidget(self._orders_drawer_button)

        self._positions_drawer_button = QPushButton("持仓")
        self._positions_drawer_button.clicked.connect(lambda: self._show_account_drawer("positions"))
        action_row.addWidget(self._positions_drawer_button)

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

        self._chart_range_mode_btn = QPushButton("全量视图")
        self._chart_range_mode_btn.clicked.connect(self._toggle_chart_view_range_mode)
        action_row.addWidget(self._chart_range_mode_btn)
        header_layout.addLayout(action_row)

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

        self._backend_hint = QLabel("")
        self._backend_hint.setObjectName("Subtle")
        self._backend_hint.setWordWrap(True)
        control_layout.addWidget(self._backend_hint)

        self._rr_trade_hint = QLabel("")
        self._rr_trade_hint.setObjectName("Subtle")
        self._rr_trade_hint.setWordWrap(True)
        control_layout.addWidget(self._rr_trade_hint)

        control_layout.addWidget(QLabel("告警引擎"))
        self._ma_cross_alert_check = QCheckBox("EMA 15 与 SMA 50 交叉")
        self._ma_cross_alert_check.toggled.connect(self._save_workspace_settings)
        control_layout.addWidget(self._ma_cross_alert_check)

        self._box_breakout_alert_check = QCheckBox("自动箱体突破")
        self._box_breakout_alert_check.toggled.connect(self._save_workspace_settings)
        control_layout.addWidget(self._box_breakout_alert_check)

        control_layout.addWidget(QLabel("自动通道"))
        self._auto_box_check = QCheckBox("显示自动箱体")
        self._auto_box_check.setChecked(False)
        self._auto_box_check.toggled.connect(self._on_auto_box_visibility_changed)
        control_layout.addWidget(self._auto_box_check)

        self._history_box_check = QCheckBox("显示历史箱体")
        self._history_box_check.setChecked(False)
        self._history_box_check.toggled.connect(self._on_auto_box_visibility_changed)
        control_layout.addWidget(self._history_box_check)
        self._live_box_check = self._history_box_check

        self._auto_channel_check = QCheckBox("显示通道")
        self._auto_channel_check.setChecked(False)
        self._auto_channel_check.toggled.connect(self._on_auto_channel_visibility_changed)
        control_layout.addWidget(self._auto_channel_check)

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
        control_layout.addWidget(self._auto_channel_settings_button)

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
        rr_long_btn = QPushButton("RR多")
        rr_long_btn.clicked.connect(lambda: self._set_draw_tool("rr_long"))
        line_toolbar.addWidget(rr_long_btn)
        rr_short_btn = QPushButton("RR空")
        rr_short_btn.clicked.connect(lambda: self._set_draw_tool("rr_short"))
        line_toolbar.addWidget(rr_short_btn)
        control_layout.addLayout(line_toolbar)

        self._line_label_edit = QLineEdit()
        self._line_label_edit.setPlaceholderText("线条名称")
        control_layout.addWidget(self._line_label_edit)

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
        control_layout.addLayout(line_price_row)
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
        control_layout.addLayout(line_rule_row)

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
        control_layout.addLayout(line_email_row)

        self._line_enabled_check = QCheckBox("启用当前线条")
        control_layout.addWidget(self._line_enabled_check)

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
        control_layout.addLayout(line_trade_row)
        self._line_trade_armed_check = QCheckBox("全局线条交易")
        self._line_trade_armed_check.setToolTip("总开关默认关闭。开启后，满足条件的线条触发才会自动提交订单。")
        self._line_trade_armed_check.toggled.connect(self._on_line_trade_armed_toggled)
        control_layout.addWidget(self._line_trade_armed_check)
        self._line_trade_hint = QLabel("线条交易默认关闭：需同时启用当前线条、启用线条交易和全局线条交易。")
        self._line_trade_hint.setObjectName("Subtle")
        self._line_trade_hint.setWordWrap(True)
        control_layout.addWidget(self._line_trade_hint)

        line_manage_row = QHBoxLayout()
        update_line_btn = QPushButton("更新")
        update_line_btn.clicked.connect(self._update_selected_line)
        line_manage_row.addWidget(update_line_btn)
        delete_line_btn = QPushButton("删除")
        delete_line_btn.clicked.connect(self._delete_selected_line)
        line_manage_row.addWidget(delete_line_btn)
        control_layout.addLayout(line_manage_row)

        self._line_table = QTableWidget(0, 6)
        self._line_table.setHorizontalHeaderLabels(["标签", "类型", "价格", "触发", "操作", "状态"])
        self._line_table.verticalHeader().setVisible(False)
        self._line_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self._line_table.itemSelectionChanged.connect(self._on_line_selected)
        self._line_table.setMinimumHeight(96)
        self._line_table.setMaximumHeight(112)
        control_layout.addWidget(self._line_table)

        control_layout.addWidget(QLabel("RR 工作区"))
        rr_manage_row = QHBoxLayout()
        save_rr_btn = QPushButton("新增/保存 RR")
        save_rr_btn.clicked.connect(self._save_rr_item)
        rr_manage_row.addWidget(save_rr_btn)
        remove_rr_btn = QPushButton("删除 RR")
        remove_rr_btn.clicked.connect(self._remove_rr_item)
        rr_manage_row.addWidget(remove_rr_btn)
        control_layout.addLayout(rr_manage_row)

        self._rr_table = QTableWidget(0, 8)
        self._rr_table.setHorizontalHeaderLabels(["方向", "入场", "止损", "止盈", "管理", "R", "K线", "锁定"])
        self._rr_table.verticalHeader().setVisible(False)
        self._rr_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self._rr_table.itemSelectionChanged.connect(self._on_rr_selected)
        self._rr_table.cellClicked.connect(self._on_rr_table_cell_clicked)
        self._rr_table.cellDoubleClicked.connect(self._on_rr_table_cell_double_clicked)
        self._rr_table.setWordWrap(False)
        self._rr_table.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        rr_header = self._rr_table.horizontalHeader()
        rr_header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        rr_header.setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        rr_header.setSectionResizeMode(2, QHeaderView.ResizeMode.Stretch)
        rr_header.setSectionResizeMode(3, QHeaderView.ResizeMode.Stretch)
        rr_header.setSectionResizeMode(4, QHeaderView.ResizeMode.ResizeToContents)
        rr_header.setSectionResizeMode(5, QHeaderView.ResizeMode.ResizeToContents)
        rr_header.setSectionResizeMode(6, QHeaderView.ResizeMode.ResizeToContents)
        rr_header.setSectionResizeMode(7, QHeaderView.ResizeMode.ResizeToContents)
        rr_header.setStretchLastSection(False)
        self._rr_table.setMinimumHeight(96)
        self._rr_table.setMaximumHeight(116)
        control_layout.addWidget(self._rr_table)

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
        self._rr_execution_mode_combo.addItem("盘口追单", "chase_best_quote")
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
        control_layout.addWidget(rr_form)
        rr_form.hide()

        control_layout.addWidget(QLabel("RR 跟踪"))
        self._rr_tracking_summary = QLabel("选中 RR 后显示入场、止损、止盈和跟踪状态。")
        self._rr_tracking_summary.setObjectName("Subtle")
        self._rr_tracking_summary.setWordWrap(True)
        self._rr_tracking_summary.setFixedHeight(60)
        control_layout.addWidget(self._rr_tracking_summary)
        rr_execution_row = QHBoxLayout()
        self._rr_enable_trade_btn = QPushButton("启用交易")
        self._rr_enable_trade_btn.setObjectName("Primary")
        self._rr_enable_trade_btn.clicked.connect(self._enable_selected_rr_trade)
        rr_execution_row.addWidget(self._rr_enable_trade_btn)
        self._rr_cancel_trade_btn = QPushButton("取消交易")
        self._rr_cancel_trade_btn.clicked.connect(self._cancel_selected_rr_trade)
        rr_execution_row.addWidget(self._rr_cancel_trade_btn)
        control_layout.addLayout(rr_execution_row)
        self._rr_condition_status = QLabel("条件单：未启用交易")
        self._rr_condition_status.setObjectName("Subtle")
        self._rr_condition_status.setWordWrap(False)
        self._rr_condition_status.setFixedHeight(24)
        control_layout.addWidget(self._rr_condition_status)

        control_layout.addWidget(QLabel("事件日志"))
        self._event_log = QTextEdit()
        self._event_log.setReadOnly(True)
        self._event_log.setMinimumHeight(72)
        self._event_log.setMaximumHeight(84)
        control_layout.addWidget(self._event_log)

        control_layout.addStretch(1)

        chart_host = QFrame()
        self._chart_host = chart_host
        chart_host.setObjectName("Panel")
        chart_layout = QVBoxLayout(chart_host)
        chart_layout.setContentsMargins(8, 8, 8, 8)
        chart_layout.setSpacing(0)

        chart_frame = QFrame()
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

        splitter.addWidget(control_scroll)
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
        if isinstance(self._native_chart_view, InteractiveKlineChartView):
            self._native_chart_view.update()
        if isinstance(self._secondary_native_chart_view, InteractiveKlineChartView):
            self._secondary_native_chart_view.update()
        if isinstance(self._tertiary_native_chart_view, InteractiveKlineChartView):
            self._tertiary_native_chart_view.update()

    def _apply_default_splitter_sizes(self) -> None:
        splitter = self._body_splitter
        if splitter is None or self._splitter_default_applied:
            return
        available_width = splitter.width() or self.width() or 1680
        left_width, right_width = _default_kline_splitter_sizes(available_width)
        splitter.setSizes([left_width, right_width])
        self._splitter_default_applied = True

    def _secondary_chart_kind(self) -> str:
        value = str(self._secondary_chart_kind_mode or "kline").strip().lower()
        return value if value in {"kline", "volatility"} else "kline"

    def _triple_chart_enabled(self) -> bool:
        return bool(self._tertiary_chart_check.isChecked())

    def _secondary_layout_mode(self) -> str:
        value = str(self._secondary_layout_mode_value or "vertical").strip().lower()
        return value if value in {"vertical", "horizontal"} else "vertical"

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
            self._secondary_sync_period_btn.setToolTip("副图为波动率时：主图和副图同步切换周期（1H/4H/1D）")
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

    def _current_volatility_currency(self) -> str | None:
        return _volatility_currency_for_symbol(self._selected_symbol())

    def _volatility_available_for_current_symbol(self) -> bool:
        return self._current_volatility_currency() is not None

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
        aggregation_text = "1H直连" if normalized_period == "1H" else f"{normalized_period}本地聚合"
        average_text = "开" if self._secondary_average_kline_enabled() else "关"
        currency = self._current_volatility_currency() or "-"
        source_text = str(payload.stats.get("source", f"Deribit {currency} DVOL") or f"Deribit {currency} DVOL")
        source_text = source_text.replace("（本地聚合）", "")
        return [f"{currency}波动率 | 平均K线 {average_text} | {aggregation_text} | 来源 {source_text}"]

    def _sync_chart_range_to_other(self, *, target: str, start_x: float, end_x: float) -> None:
        if self._syncing_chart_range or not self._secondary_chart_check.isChecked() or not self._use_native_chart:
            return
        target_views = {
            "primary": self._native_chart_view,
            "secondary": self._secondary_native_chart_view,
            "tertiary": self._tertiary_native_chart_view,
        }
        target_view = target_views.get(target)
        if not isinstance(target_view, InteractiveKlineChartView):
            return
        self._syncing_chart_range = True
        try:
            target_view.set_external_x_range(float(start_x), float(end_x))
        finally:
            self._syncing_chart_range = False

    def _sync_chart_range_from(self, *, source: str, start_x: float, end_x: float) -> None:
        targets = ["primary", "secondary"]
        if self._triple_chart_enabled():
            targets.append("tertiary")
        for target in targets:
            if target != source:
                self._sync_chart_range_to_other(target=target, start_x=start_x, end_x=end_x)

    def _sync_hover_time_from(self, *, source: str, candle_time: object) -> None:
        targets: dict[str, object] = {
            "primary": self._native_chart_view,
            "secondary": self._secondary_native_chart_view,
        }
        if self._triple_chart_enabled():
            targets["tertiary"] = self._tertiary_native_chart_view
        value = None if candle_time is None else int(candle_time)
        for target, view in targets.items():
            if target != source and isinstance(view, InteractiveKlineChartView):
                view.set_external_hover_time(value)

    def _sync_secondary_chart_range_from_primary(self) -> None:
        if not self._secondary_chart_check.isChecked():
            return
        if not isinstance(self._native_chart_view, InteractiveKlineChartView):
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
            self._native_chart_view.set_recent_view_range()
            start_x, end_x = self._native_chart_view.current_x_range()
            self._sync_chart_range_from(source="primary", start_x=start_x, end_x=end_x)
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
        chart_host = self._chart_host
        if chart_host is not None:
            chart_host.setVisible(not hidden)
        self._hide_chart_btn.setText("显示图表" if hidden else "隐藏图表")

    def _update_secondary_controls_state(self) -> None:
        enabled = bool(self._secondary_chart_check.isChecked())
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
        self._secondary_period_label.setVisible(enabled)
        self._secondary_period_combo.setVisible(enabled)
        self._secondary_period_combo.setEnabled(enabled)
        if self._secondary_layout_cycle_btn is not None:
            self._secondary_layout_cycle_btn.setEnabled(enabled and not tertiary_available)
        if self._secondary_chart_kind_btn is not None:
            self._secondary_chart_kind_btn.setEnabled(
                enabled and not tertiary_available and self._volatility_available_for_current_symbol()
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
            self._secondary_layout_mode_value = "vertical"
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
        self._secondary_chart_kind_mode = ("volatility" if previous_kind == "kline" else "kline")
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
            if current_period not in {"1H", "4H", "1D"}:
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
            sequence = ("1H", "4H", "1D")
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
        )
        for control in controls:
            control.blockSignals(True)
        try:
            self._period_combo.setCurrentText("1D")
            self._secondary_period_combo.setCurrentText("1Dutc")
            self._secondary_symbol_combo.setCurrentText(symbol)
            self._tertiary_chart_check.setChecked(False)
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

    def _apply_secondary_chart_layout(self) -> None:
        splitter = self._chart_stack_splitter
        if splitter is None:
            return
        enabled = bool(self._secondary_chart_check.isChecked())
        if self._triple_chart_enabled():
            splitter.setOrientation(Qt.Orientation.Horizontal)
            available_width = splitter.width() or self.width() or 1800
            third_width = max(1, available_width // 3)
            splitter.setSizes([third_width, third_width, third_width])
            return
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
        enabled = bool(self._secondary_chart_check.isChecked())
        if self._secondary_chart_frame is not None:
            self._secondary_chart_frame.setVisible(enabled)
        if self._tertiary_chart_frame is not None:
            self._tertiary_chart_frame.setVisible(self._triple_chart_enabled())
        self._update_secondary_controls_state()
        self._apply_secondary_chart_layout()
        self._refresh_chart_mode_cycle_button()

    def _active_period_value(self) -> str:
        if self._active_chart_target == "secondary" and self._secondary_chart_check.isChecked():
            return self._secondary_period_combo.currentText().strip()
        if self._active_chart_target == "tertiary" and self._triple_chart_enabled():
            return self._tertiary_period_combo.currentText().strip()
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

    def _set_active_chart_target(self, target: str) -> None:
        if target == "tertiary" and self._triple_chart_enabled():
            resolved = "tertiary"
        elif target == "secondary" and self._secondary_chart_check.isChecked():
            resolved = "secondary"
        else:
            resolved = "primary"
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
        self._tertiary_period_combo.blockSignals(True)
        self._period_combo.setCurrentText(primary_period)
        self._secondary_period_combo.setCurrentText(secondary_period)
        self._tertiary_period_combo.setCurrentText(secondary_period)
        self._period_combo.blockSignals(False)
        self._secondary_period_combo.blockSignals(False)
        self._tertiary_period_combo.blockSignals(False)
        self._sync_primary_period_buttons()
        self._refresh_timer.setInterval(self._auto_refresh_interval_ms(primary_period))
        self._reload_workspace_view()

    @Slot(str)
    def _on_period_button_clicked(self, period_value: str) -> None:
        if self._active_chart_target == "tertiary" and self._triple_chart_enabled():
            if self._tertiary_period_combo.currentText().strip() != period_value:
                self._tertiary_period_combo.setCurrentText(period_value)
            else:
                self._load_tertiary_data()
            return
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
        monitor = getattr(self, "_rr_monitor_timer", None)
        if monitor is not None:
            monitor.stop()
        if self._account_drawer is not None and not self._account_drawer.shutdown():
            self._set_status("账户抽屉请求仍在完成中，窗口将在请求结束后关闭。")
            event.ignore()
            QTimer.singleShot(250, self.close)
            return
        thread = self._rr_execution_thread
        if thread is not None and thread.isRunning() and not thread.wait(1500):
            self._set_status("RR 请求仍在完成中，窗口将在请求结束后关闭。")
            event.ignore()
            QTimer.singleShot(250, self.close)
            return
        if self._loader is not None and self._loader.isRunning():
            self._loader.requestInterruption()
            self._loader.wait(1000)
        if self._secondary_loader is not None and self._secondary_loader.isRunning():
            self._secondary_loader.requestInterruption()
            self._secondary_loader.wait(1000)
        if self._tertiary_loader is not None and self._tertiary_loader.isRunning():
            self._tertiary_loader.requestInterruption()
            self._tertiary_loader.wait(1000)
        if self._secondary_volatility_loader is not None and self._secondary_volatility_loader.isRunning():
            self._secondary_volatility_loader.requestInterruption()
            self._secondary_volatility_loader.wait(1000)
        if self._deferred_chart_render_timer.isActive():
            self._deferred_chart_render_timer.stop()
        if self._layout_refresh_timer.isActive():
            self._layout_refresh_timer.stop()
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
            or (self._tertiary_loader is not None and self._tertiary_loader.isRunning())
            or (
                self._secondary_volatility_loader is not None
                and self._secondary_volatility_loader.isRunning()
            )
        )

    def _refresh_api_profiles(self) -> None:
        self._profile_snapshots, selected_profile = load_profile_snapshots()
        self._suppress_api_profile_change = True
        try:
            runtime = self._runtime if self._runtime is not None else load_runtime("moni") or load_runtime()
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
        if not self._pending_reload_after_load or self._has_active_loaders():
            return
        self._pending_reload_after_load = False
        QTimer.singleShot(10, self._load_data)

    @Slot()
    def _load_data(self) -> None:
        symbol = self._selected_symbol()
        period = self._period_combo.currentText()
        _debug_log(f"[kline] _load_data begin | symbol={symbol or '-'} | period={period or '-'}")
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
        previewed_cached_payload = self._preview_cached_primary_payload(self._active_primary_request_key)
        self._set_status(
            f"正在加载 {symbol} {period} | K线={requested_limit} | "
            f"{'本地缓存' if self._prefer_local_checkbox.isChecked() else '本地优先'}"
            f"{' | 已先显示内存缓存' if previewed_cached_payload else ''}"
        )

        self._loader = KlineDataLoader(
            request_id=self._request_id,
            symbol=symbol,
            period=period,
            limit=requested_limit,
            local_only=self._prefer_local_checkbox.isChecked(),
            average_kline=self._primary_average_kline_enabled(),
            workspace_entry=workspace_entry,
            enable_shape_signals=self.pattern_signals_enabled(),
        )
        self._loader.loaded.connect(self._on_data_loaded)
        self._loader.failed.connect(self._on_data_failed)
        self._loader.finished.connect(self._on_loader_finished)
        self._loader.start()
        if self._secondary_chart_check.isChecked() and self._use_native_chart:
            self._load_secondary_data(symbol=self._selected_secondary_symbol())
            if self._triple_chart_enabled():
                self._load_tertiary_data()
        else:
            self._active_secondary_request_key = None
            self._loaded_secondary_request_key = None
            self._secondary_pending_payload = None
            if isinstance(self._secondary_native_chart_view, InteractiveKlineChartView):
                self._secondary_native_chart_view.set_external_hover_time(None)
            if self._secondary_native_chart is not None:
                self._secondary_native_chart.setTitle("副图")
            self._tertiary_pending_payload = None

    @Slot()
    def _on_loader_finished(self) -> None:
        if self._loader is not None:
            self._loader.deleteLater()
            self._loader = None
        self._schedule_pending_reload_if_ready()

    def _load_secondary_data(self, *, symbol: str) -> None:
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
            local_only=self._prefer_local_checkbox.isChecked(),
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
        self._tertiary_loader = KlineDataLoader(
            request_id=self._tertiary_request_id,
            symbol=self._selected_tertiary_symbol(),
            period=self._tertiary_period_combo.currentText().strip(),
            limit=max(50, self._limit_spin.value()),
            local_only=self._prefer_local_checkbox.isChecked(),
            average_kline=self._secondary_average_kline_enabled(),
            workspace_entry={},
            enable_alerts=False,
            enable_shape_signals=False,
        )
        self._tertiary_loader.loaded.connect(self._on_tertiary_data_loaded)
        self._tertiary_loader.failed.connect(self._on_tertiary_data_failed)
        self._tertiary_loader.finished.connect(self._on_tertiary_loader_finished)
        self._tertiary_loader.start()

    @Slot()
    def _on_secondary_loader_finished(self) -> None:
        if self._secondary_loader is not None:
            self._secondary_loader.deleteLater()
            self._secondary_loader = None
        self._schedule_pending_reload_if_ready()

    @Slot()
    def _on_tertiary_loader_finished(self) -> None:
        if self._tertiary_loader is not None:
            self._tertiary_loader.deleteLater()
            self._tertiary_loader = None
        self._schedule_pending_reload_if_ready()

    @Slot()
    def _on_secondary_volatility_loader_finished(self) -> None:
        if self._secondary_volatility_loader is not None:
            self._secondary_volatility_loader.deleteLater()
            self._secondary_volatility_loader = None
        self._schedule_pending_reload_if_ready()

    @Slot()
    def _on_symbol_confirmed(self) -> None:
        if not self._volatility_available_for_current_symbol() and self._secondary_chart_kind() == "volatility":
            self._secondary_chart_kind_mode = "kline"
            self._secondary_pending_payload = None
            self._loaded_secondary_request_key = None
        self._update_secondary_controls_state()
        self._reload_workspace_view()
        self._refresh_rr_trade_hint()
        self._sync_account_drawer_context()
        self._load_data()

    @Slot(str)
    def _on_secondary_symbol_changed(self, _value: str) -> None:
        if (
            not self._secondary_chart_check.isChecked()
            or not self._use_native_chart
            or self._secondary_chart_kind() != "kline"
        ):
            return
        if self._has_active_loaders():
            self._pending_reload_after_load = True
            self._set_status("当前仍在加载，已排队刷新最新副图交易对，请稍候...")
            return
        self._load_secondary_data(symbol=self._selected_secondary_symbol())

    @Slot(str)
    def _on_tertiary_symbol_changed(self, _value: str) -> None:
        if not self._triple_chart_enabled() or not self._use_native_chart:
            return
        if self._has_active_loaders():
            self._pending_reload_after_load = True
            self._set_status("当前仍在加载，已排队刷新最新第三图交易对，请稍候...")
            return
        self._load_tertiary_data()

    @Slot(str)
    def _on_tertiary_period_changed(self, _value: str) -> None:
        if self._active_chart_target == "tertiary":
            self._sync_primary_period_buttons()
        if self._triple_chart_enabled():
            self._on_tertiary_symbol_changed(self._selected_tertiary_symbol())

    def _show_account_drawer(self, tab_name: str) -> None:
        if self._account_drawer is None or self._chart_account_splitter is None:
            return
        target_tab = "orders" if tab_name == "orders" else "positions"
        current_tab = "orders" if self._account_drawer._tabs.currentIndex() == 0 else "positions"
        if not self._account_drawer.isHidden() and current_tab == target_tab:
            self._collapse_account_drawer()
            return
        self._account_drawer.show()
        self._account_drawer.show_tab(target_tab)
        self._sync_account_drawer_context(refresh_if_visible=False)
        total_height = max(self._chart_account_splitter.size().height(), 1)
        drawer_height = max(int(total_height * 0.28), 180)
        chart_height = max(total_height - drawer_height, 240)
        self._chart_account_splitter.setSizes([chart_height, drawer_height])
        self._account_drawer.refresh_data()

    def _collapse_account_drawer(self) -> None:
        if self._account_drawer is None or self._chart_account_splitter is None:
            return
        total_height = max(self._chart_account_splitter.size().height(), 1)
        self._account_drawer.hide()
        self._chart_account_splitter.setSizes([total_height, 0])

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
        if not enabled and self._triple_chart_enabled():
            self._tertiary_chart_check.blockSignals(True)
            self._tertiary_chart_check.setChecked(False)
            self._tertiary_chart_check.blockSignals(False)
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

    @Slot()
    def _on_secondary_layout_changed(self) -> None:
        self._on_secondary_layout_cycle_clicked()

    @Slot()
    def _on_secondary_chart_kind_changed(self) -> None:
        self._on_secondary_chart_kind_cycle_clicked()

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
            self._subscribe_realtime_candle()
            self._update_refresh_hint()
        except Exception as exc:
            self._set_status(f"数据处理异常：{exc}")

    @Slot(object)
    def _apply_realtime_candle(self, candle: object) -> None:
        if not isinstance(candle, Candle) or self._pending_payload is None:
            return
        payload = _merge_realtime_candle_payload(self._pending_payload, candle)
        self._pending_payload = payload
        self._remember_payload_cache(self._primary_payload_cache, self._loaded_primary_request_key, payload)
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
        self._run_js(f"window.updateRealtimeCandle({json.dumps({'candle': latest, 'ema9': ema9, 'ema21': ema21})});")

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

    @Slot(int, KlineChartPayload)
    def _on_tertiary_data_loaded(self, request_id: int, payload: KlineChartPayload) -> None:
        if request_id != self._active_tertiary_request_id:
            return
        if self._active_tertiary_request_key != self._current_tertiary_request_key():
            self._set_status("第三图结果已过期，正在刷新最新选择...")
            return
        self._tertiary_pending_payload = payload
        self._loaded_tertiary_request_key = self._active_tertiary_request_key
        self._remember_payload_cache(self._tertiary_payload_cache, self._loaded_tertiary_request_key, payload)
        if self._triple_chart_enabled() and self._use_native_chart:
            self._render_tertiary_chart(payload)
            self._sync_secondary_chart_range_from_primary()

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

    @Slot(int, str)
    def _on_tertiary_data_failed(self, request_id: int, message: str) -> None:
        if request_id != self._active_tertiary_request_id:
            return
        if self._tertiary_native_chart is not None:
            self._tertiary_native_chart.setTitle(f"第三图加载失败：{message}")

    @Slot(object)
    def _on_primary_hover_time_changed(self, candle_time: object) -> None:
        self._sync_hover_time_from(source="primary", candle_time=candle_time)

    @Slot(object)
    def _on_secondary_hover_time_changed(self, candle_time: object) -> None:
        self._sync_hover_time_from(source="secondary", candle_time=candle_time)

    @Slot(object)
    def _on_tertiary_hover_time_changed(self, candle_time: object) -> None:
        self._sync_hover_time_from(source="tertiary", candle_time=candle_time)

    @Slot(float, float)
    def _on_primary_x_range_changed(self, start_x: float, end_x: float) -> None:
        self._sync_chart_range_from(source="primary", start_x=start_x, end_x=end_x)

    @Slot(float, float)
    def _on_secondary_x_range_changed(self, start_x: float, end_x: float) -> None:
        self._sync_chart_range_from(source="secondary", start_x=start_x, end_x=end_x)

    @Slot(float, float)
    def _on_tertiary_x_range_changed(self, start_x: float, end_x: float) -> None:
        self._sync_chart_range_from(source="tertiary", start_x=start_x, end_x=end_x)

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
            },
            "show": {
                "ema9": self._ema9.isChecked(),
                "ema21": self._ema21.isChecked(),
            },
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
        chart_splitter.setStretchFactor(0, 5)
        chart_splitter.setStretchFactor(1, 2)
        chart_splitter.setStretchFactor(2, 2)

        if isinstance(self._native_chart_view, InteractiveKlineChartView) and isinstance(self._secondary_native_chart_view, InteractiveKlineChartView):
            self._native_chart_view.hoverTimeChanged.connect(self._on_primary_hover_time_changed)
            self._secondary_native_chart_view.hoverTimeChanged.connect(self._on_secondary_hover_time_changed)
            self._native_chart_view.xRangeChanged.connect(self._on_primary_x_range_changed)
            self._secondary_native_chart_view.xRangeChanged.connect(self._on_secondary_x_range_changed)
            self._native_chart_view.chartActivated.connect(lambda: self._set_active_chart_target("primary"))
            self._secondary_native_chart_view.chartActivated.connect(lambda: self._set_active_chart_target("secondary"))
            if isinstance(self._tertiary_native_chart_view, InteractiveKlineChartView):
                self._tertiary_native_chart_view.hoverTimeChanged.connect(self._on_tertiary_hover_time_changed)
                self._tertiary_native_chart_view.xRangeChanged.connect(self._on_tertiary_x_range_changed)
                self._tertiary_native_chart_view.chartActivated.connect(lambda: self._set_active_chart_target("tertiary"))
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
        display_payload = self._display_payload_for_chart(payload, is_secondary=False)
        self._render_native_chart_target(
            chart=self._native_chart,
            chart_view=self._native_chart_view,
            payload=display_payload,
            period=self._period_combo.currentText().strip(),
            title_suffix="主图",
            include_workspace_lines=True,
            is_secondary=False,
            venue_label=self._kline_venue_label(self._period_combo.currentText()),
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
            title_suffix="副图",
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
            title_suffix="第三图",
            include_workspace_lines=False,
            is_secondary=True,
            display_symbol=self._selected_tertiary_symbol(),
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
        overlay_values: list[list[float]] = []
        indicator_series: list[dict[str, Any]] = []
        workspace_lines: list[dict[str, object]] = []
        workspace_rr_items: list[dict[str, object]] = []
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
                for candle_index, candle in enumerate(candles):
                    projected_value = self._display_price_from_logical(
                        logical_payload,
                        float(line_value_at(item, int(candle["time"]))),
                        is_secondary=is_secondary,
                    )
                    series.append(float(display_times_ms[candle_index]), projected_value)
                    line_values.append(projected_value)
                    min_price = min(min_price, projected_value)
                    max_price = max(max_price, projected_value)
                kind = str(item.get("kind", "horizontal") or "horizontal").strip().lower()
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
        axis_x.setFormat("MM-dd" if _bar_to_ms(period) >= 86_400_000 else "MM-dd HH:mm")
        axis_x.setTickCount(min(8, max(3, len(candles) // 180 + 3)))
        axis_x.setLabelsColor(QColor(_CHART_AXIS_TEXT_COLOR))
        axis_x.setGridLineColor(QColor(_CHART_GRID_COLOR))
        axis_x.setLinePenColor(QColor(_CHART_AXIS_LINE_COLOR))
        axis_x.setGridLineVisible(False)

        axis_y = QValueAxis()
        top_padding, bottom_padding = _compute_axis_y_padding(min_price, max_price)
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
                chart_note_lines=chart_note_lines,
                trend_indicators=trend_indicators,
                signal_markers=self._filter_replay_signal_markers_for_chart(
                    payload.signal_markers,
                    period=period,
                    is_secondary=is_secondary,
                ),
                box_overlays=self._visible_box_overlays(payload),
                channel_overlays=self._visible_channel_overlays(payload),
                workspace_lines=workspace_lines if include_workspace_lines else [],
                workspace_rr_items=workspace_rr_items if include_workspace_lines else [],
                selected_workspace_line_index=self._selected_line_index if include_workspace_lines else -1,
                selected_workspace_rr_index=self._selected_rr_index if include_workspace_lines else -1,
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
            )
            for column, value in enumerate(values):
                item_widget = QTableWidgetItem(value)
                if column in {1, 2, 3, 5, 6}:
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
        self._rr_bar_edit.setText("0")
        self._rr_fee_offset_check.setChecked(False)
        self._rr_locked_check.setChecked(False)
        self._rr_preview.setText("止盈会按入场、止损和 R 倍数自动计算。")
        self._refresh_rr_tracking_summary(None)

    @Slot(int, int)
    def _on_rr_table_cell_clicked(self, row: int, _column: int) -> None:
        if row < 0:
            return
        self._populate_rr_table(selected_index=row)

    @Slot(int, int)
    def _on_rr_table_cell_double_clicked(self, row: int, _column: int) -> None:
        if row < 0:
            return
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
        self._line_price_a_label.setText("起点价" if is_trend else "价格")
        self._line_price_a_edit.setPlaceholderText("起点价" if is_trend else "价格")
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
            self._rr_execution_in_flight = False
            if not isinstance(entry, RRTradeLedgerEntry):
                self._set_status("RR 交易返回了无效结果。")
                return
            if self._find_rr_trade_ledger_entry(entry.plan.plan_id) != entry:
                self._save_rr_trade_ledger_entry(entry)
            on_success(entry)

        def _failed(message: str) -> None:
            self._rr_execution_in_flight = False
            self._set_status(f"RR 交易失败：{message}")
            if on_failure is not None:
                on_failure(message)

        def _finished() -> None:
            if self._rr_execution_thread is thread:
                self._rr_execution_thread = None
            thread.deleteLater()
            self._start_next_pending_rr_execution_request()

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
        if self._rr_execution_in_flight:
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
                client=OkxRestClient(),
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

        self._set_status("正在提交 RR 交易...")
        self._start_rr_execution_action(
            action=lambda: self._rr_trade_execution_service.activate(
                client=OkxRestClient(),
                credentials=runtime.credentials,
                config=_build_strategy_config(plan.inst_id, runtime),
                plan=plan,
            ),
            on_success=lambda entry: self._set_status(f"RR 交易已启用：{entry.status}"),
            queue_if_busy=True,
            queued_status_text="当前正在同步已报 RR，新的启用请求已排队，稍后自动提交。",
        )

    @Slot()
    def _cancel_selected_rr_trade(self) -> None:
        try:
            payload = self._selected_rr_payload()
            if self._purge_placeholder_rr_entry(payload, delete_item=False):
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
            self._set_status(f"无法取消 RR 交易：{exc}")
            return

        self._submit_rr_trade_cancel(
            entry=entry,
            runtime=runtime,
            confirmed_for_filled=False,
            delete_after_cancel=False,
        )

    def _submit_rr_trade_cancel(
        self,
        *,
        entry: RRTradeLedgerEntry,
        runtime,
        confirmed_for_filled: bool,
        delete_after_cancel: bool,
    ) -> None:
        self._start_rr_execution_action(
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
            queue_if_busy=True,
            queued_status_text="当前正在同步 RR 状态，撤单请求已排队，稍后自动执行。",
        )

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
        if entry.status == "cancel_confirmation_required":
            confirmation_text = "该 RR 已有成交。确认后只撤销未成交部分，已成交仓位及止损/止盈保护会保留。是否继续？"
            if delete_after_cancel:
                confirmation_text += "\n\n同时会删除本地 RR 图形和列表记录；不会平仓，也不会撤销交易所保护单。"
            confirmed = QMessageBox.question(
                self,
                "确认取消已成交 RR",
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
            self._set_status("已取消撤单确认，原订单保持不变。")
            return
        if delete_after_cancel:
            if entry.status == "cancelled":
                index = self._find_rr_item_index_for_entry(entry)
                if index >= 0:
                    self._delete_rr_item_at_index(index)
                self._set_status("RR 挂单已撤销，图形已删除。")
                return
            if entry.status in {
                "protected",
                "protected_break_even",
                "protected_trailing",
                "protected_cancelled_remainder",
            }:
                self._delete_rr_visual_keep_protection(entry)
                return
            self._set_status(f"RR 取消结果：{entry.status}，已有成交或保护单，保留图形。")
            return
        self._set_status(f"RR 取消结果：{entry.status}")

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
            return
        price_increment = self._current_rr_price_increment(item)
        rr_id = str(item.get("rr_id", "") or f"rr-{self._selected_rr_index + 1}")
        side = "空头" if str(item.get("side", "")).strip().lower() == "short" else "多头"
        entry_text = _format_rr_table_price(item.get("price_entry", ""), price_increment) or "-"
        stop_text = _format_rr_table_price(item.get("price_stop", ""), price_increment) or "-"
        tp_text = _format_rr_table_price(item.get("price_tp", ""), price_increment) or "-"
        r_text = str(item.get("r_multiple", "") or "-")
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
            filled_text = format_decimal(ledger_entry.filled_size or Decimal("0"))
            remaining_text = format_decimal(ledger_entry.remaining_size or Decimal("0"))
            current_stop = getattr(getattr(ledger_entry, "stop_loss_order", None), "trigger_price", None)
            current_stop_text = _format_rr_table_price(current_stop, price_increment) or "-"
            last_event = ledger_entry.events[-1].message if ledger_entry.events else "-"
            ledger_text = f"交易：{status_text} | 成交 {filled_text}张 | 剩余 {remaining_text}张 | 当前止损 {current_stop_text}"
        summary_text = (
            f"{rr_id} | {side} | 入场 {entry_text} | 止损 {stop_text} | 止盈 {tp_text} | {management_text} | R 1:{r_text}\n"
            f"状态：{locked_text} | 手续费偏移：{fee_text}\n{ledger_text}"
        )
        tooltip_text = summary_text if ledger_entry is None else f"{summary_text}\n最后事件：{last_event}"
        self._rr_tracking_summary.setText(summary_text)
        self._rr_tracking_summary.setToolTip(tooltip_text)

    def _open_rr_card_for_selected(self) -> None:
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
        self._save_rr_item(extra_payload=payload)

    def _set_draw_tool(self, tool: str) -> None:
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

    def _rr_drag_threshold_crossed(self, state: dict[str, object]) -> bool:
        press_x = state.get("press_scene_x")
        press_y = state.get("press_scene_y")
        current_pos = self._current_chart_pointer_scene_pos()
        if current_pos is None or press_x is None or press_y is None:
            return True
        delta_x = float(current_pos.x()) - float(press_x)
        delta_y = float(current_pos.y()) - float(press_y)
        return math.hypot(delta_x, delta_y) >= _RR_DRAG_ACTIVATION_DISTANCE_PX

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
                "drag_mode": str(hit["drag_mode"]),
                "anchor_candle_time": candle_time,
                "anchor_price": price,
                "anchor_line": dict(hit["line"]),
            }
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
            updated = self._apply_line_drag_update(candle_time=candle_time, price=price)
        elif self._rr_drag_state is not None:
            if not bool(self._rr_drag_state.get("active", False)):
                if not self._rr_drag_threshold_crossed(self._rr_drag_state):
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
        line_drag_in_progress = self._line_drag_state is not None
        rr_drag_active = bool(isinstance(self._rr_drag_state, dict) and self._rr_drag_state.get("active", False))
        resolved = self._resolve_primary_chart_click(x_value=x_value, y_value=y_value)
        if resolved is not None:
            candle_time, price = resolved
            if line_drag_in_progress:
                self._apply_line_drag_update(candle_time=candle_time, price=price)
            elif rr_drag_active:
                self._apply_rr_drag_update(candle_time=candle_time, price=price)
        if line_drag_in_progress or rr_drag_active:
            self._save_workspace_snapshot()
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
        index = int(state.get("index", -1))
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
                const lineSeries = { ema9: null, ema21: null };
                let channelSeries = [];
                let trendSeries = null;
                let volumeSeries = null;
                let trendByTime = {};
                let signalByTime = {};
                let currentCandles = [];
                const chartRecentVisibleBars = __RECENT_VIEW_BARS__;

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
                    if (volumeSeries) {
                      const color = candle.close >= candle.open ? 'rgba(34, 197, 94, 0.85)' : 'rgba(224, 79, 132, 0.85)';
                      volumeSeries.update({ time, value: Number(candle.volume) || 0, color });
                    }
                    if (payload.ema9 && lineSeries.ema9) lineSeries.ema9.update(payload.ema9);
                    if (payload.ema21 && lineSeries.ema21) lineSeries.ema21.update(payload.ema21);
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
        return html.replace("__RECENT_VIEW_BARS__", str(_RECENT_VIEW_BARS))
