from __future__ import annotations

import json
import os
import queue
import subprocess
import sys
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
import uuid
from dataclasses import MISSING, asdict, dataclass, field, fields as dataclass_fields, replace
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from types import FunctionType
import tkinter.font as tkfont
from tkinter import BooleanVar, Canvas, END, Label, Listbox, Menu, StringVar, Text, TclError, Tk, Toplevel, filedialog, simpledialog
from tkinter import messagebox, ttk

from okx_quant.app_meta import APP_VERSION, build_app_title, build_version_info_text
from okx_quant.analysis import (
    BoxDetectionConfig,
    ChannelDetectionConfig,
    PivotDetectionConfig,
    TrendlineDetectionConfig,
    TriangleDetectionConfig,
)
from okx_quant.auto_channel_preview import sample_auto_channel_candles
from okx_quant.auto_channel_storage import (
    build_auto_channel_snapshot_record,
    deserialize_strategy_live_chart_snapshot,
    load_auto_channel_snapshots,
    save_auto_channel_snapshots,
)
from okx_quant.backtest_ui import BacktestCompareOverviewWindow, BacktestLaunchState, BacktestWindow
from okx_quant.btc_market_analysis_ui import BtcMarketAnalysisWindow
from okx_quant.btc_research_workbench_ui import BtcResearchWorkbenchWindow
from okx_quant.deribit_client import DeribitRestClient
from okx_quant.deribit_volatility_monitor_ui import DeribitVolatilityMonitorWindow
from okx_quant.deribit_volatility_ui import DeribitVolatilityWindow
from okx_quant.engine import (
    DEFAULT_DEBUG_ATR_PERIOD,
    FilledPosition,
    StrategyEngine,
    live_exchange_dynamic_take_profit_template_enabled,
    _dynamic_two_taker_fee_offset_live,
    _format_notify_size_with_unit,
    _format_size_with_contract_equivalent,
    build_protection_plan,
    determine_order_size,
    fetch_hourly_ema_debug,
    fixed_entry_side_mode_support_reason,
    format_hourly_debug,
    recommended_indicator_lookback,
    resolve_open_pos_side,
    supports_fixed_entry_side_mode,
)
from okx_quant.indicators import atr
from okx_quant.journal_ui import JournalWindow
from okx_quant.log_utils import (
    append_line_desk_log_line,
    append_log_line,
    append_preformatted_log_line,
    current_log_timestamp,
    daily_log_file_path,
    ensure_log_timestamp,
    logs_dir,
    read_daily_log_tail,
    strategy_session_log_file_path,
)
from okx_quant.models import (
    Candle,
    Credentials,
    EmailNotificationConfig,
    Instrument,
    OrderPlan,
    ProtectionPlan,
    StrategyConfig,
)
from okx_quant.duration_input import (
    format_duration_cn_compact,
    parse_nonnegative_duration_seconds,
    try_parse_nonnegative_duration_seconds,
)
from okx_quant.notifications import EmailNotifier
from okx_quant.option_roll import is_short_option_position
from okx_quant.option_roll_ui import OptionRollSuggestionWindow
from okx_quant.option_strategy_ui import OptionStrategyCalculatorWindow, _build_option_quote
from okx_quant.okx_client import (
    OkxAccountBillItem,
    OkxAccountAssetItem,
    OkxAccountConfig,
    OkxAccountOverview,
    OkxApiError,
    OkxFillHistoryItem,
    OkxOrderResult,
    OkxPosition,
    OkxPositionHistoryItem,
    OkxRestClient,
    OkxTicker,
    OkxTradeOrderItem,
    infer_inst_type,
    infer_option_family,
)
from okx_quant.persistence import (
    credentials_file_path,
    DEFAULT_CREDENTIAL_PROFILE_NAME,
    load_history_cache_records,
    load_position_history_view_prefs,
    load_recoverable_strategy_sessions_snapshot,
    load_credentials_profiles_snapshot,
    load_notification_snapshot,
    load_line_trading_desk_annotations_entries,
    load_position_notes_snapshot,
    load_strategy_parameter_drafts,
    load_strategy_history_snapshot,
    load_strategy_trade_ledger_snapshot,
    save_recoverable_strategy_sessions_snapshot,
    save_credentials_profiles_snapshot,
    save_notification_snapshot,
    save_history_cache_records,
    save_position_history_view_prefs,
    save_line_trading_desk_annotations_entries,
    save_position_notes_snapshot,
    save_strategy_parameter_drafts,
    save_strategy_history_snapshot,
    settings_file_path,
    strategy_history_file_path,
    strategy_trade_ledger_file_path,
    save_strategy_trade_ledger_snapshot,
)
from okx_quant.position_protection import (
    OptionProtectionConfig,
    PositionProtectionManager,
    build_close_order_price_from_mark,
    describe_protection_price_logic,
    derive_position_direction,
    infer_protection_profit_on_rise,
    infer_default_spot_inst_id,
    normalize_spot_inst_id,
    validate_live_protection_order_price_guard,
)
from okx_quant.protection_replay_ui import ProtectionReplayLaunchState, ProtectionReplayWindow
from okx_quant.pricing import format_decimal, format_decimal_by_increment, format_decimal_fixed, snap_to_increment
from okx_quant.signal_monitor_ui import SignalMonitorWindow
from okx_quant.signal_replay_mock_ui import SignalReplayMockWindow
from okx_quant.trader_desk import (
    TraderDeskSnapshot,
    TraderDraftRecord,
    TraderEventRecord,
    TraderRunState,
    TraderSlotRecord,
    load_trader_desk_snapshot,
    normalize_trader_draft_inputs,
    save_trader_desk_snapshot,
    trader_gate_allows_price,
    trader_has_watching_slot,
    trader_open_position_summary,
    trader_realized_close_counts,
    trader_realized_net_pnl,
    trader_remaining_quota_steps,
    trader_slots_for,
    trader_used_quota_steps,
)
from okx_quant.trader_desk_ui import TraderDeskWindow
from okx_quant.smart_order import SmartOrderRuntimeConfig
from okx_quant.smart_order_ui import SmartOrderWindow
from okx_quant.strategy_live_chart import (
    DEFAULT_STRATEGY_LIVE_CHART_CANDLE_LIMIT,
    DEFAULT_STRATEGY_LIVE_CHART_REFRESH_MS,
    LINE_TRADING_DESK_CANDLE_TARGET,
    LINE_TRADING_DESK_POLL_MS,
    StrategyLiveChartLayout,
    StrategyLiveChartSnapshot,
    StrategyLiveChartTimeMarker,
    append_candles_to_snapshot,
    build_auto_channel_live_chart_snapshot,
    build_strategy_live_chart_snapshot,
    compute_strategy_live_chart_layout,
    layout_bar_index_to_x_center,
    layout_pixel_to_bar_index,
    layout_price_to_y_clamped,
    layout_price_to_y_unclamped,
    layout_y_to_price,
    line_price_through_anchors,
    line_trading_desk_max_view_start,
    line_trading_desk_visible_bar_count,
    render_strategy_live_chart,
    slice_strategy_live_chart_snapshot_with_desk_right_pad,
    strategy_live_chart_canvas_layout,
    strategy_live_chart_price_bounds,
)
from okx_quant.strategies.ema_atr import EmaAtrStrategy
from okx_quant.strategies.ema_cross_ema_stop import EmaCrossEmaStopStrategy
from okx_quant.strategies.ema_dynamic import EmaDynamicOrderStrategy
from okx_quant.strategy_catalog import (
    BACKTEST_STRATEGY_DEFINITIONS,
    STRATEGY_DEFINITIONS,
    STRATEGY_DYNAMIC_ID,
    STRATEGY_DYNAMIC_LONG_ID,
    STRATEGY_DYNAMIC_SHORT_ID,
    STRATEGY_EMA5_EMA8_ID,
    STRATEGY_EMA_BREAKOUT_LONG_ID,
    STRATEGY_EMA_BREAKDOWN_SHORT_ID,
    StrategyDefinition,
    get_strategy_definition,
    is_dynamic_strategy_id,
    is_ema_atr_breakout_strategy,
    resolve_dynamic_signal_mode,
    supports_signal_only,
)
from okx_quant.strategy_parameters import (
    iter_strategy_parameter_keys,
    strategy_fixed_value,
    strategy_is_parameter_editable,
    strategy_parameter_default_value,
    strategy_uses_parameter,
)
from okx_quant.window_layout import (
    apply_adaptive_window_geometry,
    apply_fill_window_geometry,
    apply_window_icon,
)
from okx_quant.ui_backtest_entry import UiBacktestEntryMixin
from okx_quant.ui_positions import UiPositionsMixin
from okx_quant.ui_protection import UiProtectionMixin
from okx_quant.ui_strategy_sessions import UiStrategySessionsMixin


def _bind_mixin_to_shell_globals(mixin_cls):
    for name, value in tuple(mixin_cls.__dict__.items()):
        if isinstance(value, staticmethod):
            func = value.__func__
            rebound = FunctionType(
                func.__code__,
                globals(),
                func.__name__,
                func.__defaults__,
                func.__closure__,
            )
            rebound.__kwdefaults__ = func.__kwdefaults__
            rebound.__annotations__ = dict(getattr(func, "__annotations__", {}))
            rebound.__dict__.update(getattr(func, "__dict__", {}))
            setattr(mixin_cls, name, staticmethod(rebound))
        elif isinstance(value, FunctionType):
            rebound = FunctionType(
                value.__code__,
                globals(),
                value.__name__,
                value.__defaults__,
                value.__closure__,
            )
            rebound.__kwdefaults__ = value.__kwdefaults__
            rebound.__annotations__ = dict(getattr(value, "__annotations__", {}))
            rebound.__dict__.update(getattr(value, "__dict__", {}))
            setattr(mixin_cls, name, rebound)
    return mixin_cls


UiBacktestEntryMixin = _bind_mixin_to_shell_globals(UiBacktestEntryMixin)
UiPositionsMixin = _bind_mixin_to_shell_globals(UiPositionsMixin)
UiProtectionMixin = _bind_mixin_to_shell_globals(UiProtectionMixin)
UiStrategySessionsMixin = _bind_mixin_to_shell_globals(UiStrategySessionsMixin)


BAR_OPTIONS = ["1m", "3m", "5m", "15m", "1H", "4H"]
DEFAULT_LAUNCH_SYMBOLS = ("BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP", "BNB-USDT-SWAP", "DOGE-USDT-SWAP")
PREFERRED_STARTUP_CREDENTIAL_PROFILE_NAME = "moni"
STRATEGY_TEMPLATE_SCHEMA_VERSION = 1
POSITIONS_ZOOM_DEFAULT_VISIBLE_COLUMNS = {
    "positions": (
        "inst_type",
        "time_value",
        "time_value_usdt",
        "intrinsic_value",
        "intrinsic_usdt",
        "bid_price",
        "bid_usdt",
        "ask_price",
        "ask_usdt",
        "mark",
        "mark_usdt",
        "avg",
        "avg_usdt",
        "open_value_usdt",
        "pos",
        "option_side",
        "upl",
        "upl_usdt",
        "realized",
        "realized_usdt",
        "market_value",
        "mgn_ratio",
        "note",
        "delta",
        "gamma",
        "vega",
        "theta",
        "theta_usdt",
    ),
    "pending_orders": (
        "time",
        "source",
        "inst_type",
        "inst_id",
        "state",
        "side",
        "ord_type",
        "price",
        "size",
        "filled",
        "tp_sl",
        "order_id",
        "cl_ord_id",
    ),
    "order_history": (
        "time",
        "source",
        "inst_type",
        "inst_id",
        "state",
        "side",
        "ord_type",
        "price",
        "size",
        "filled",
        "fee",
        "tp_sl",
        "order_id",
        "cl_ord_id",
    ),
    "fills": (
        "time",
        "inst_type",
        "inst_id",
        "side",
        "price",
        "size",
        "fee",
        "pnl",
        "exec_type",
    ),
    "position_history": (
        "time",
        "inst_type",
        "inst_id",
        "mgn_mode",
        "side",
        "open_avg",
        "close_avg",
        "close_size",
        "fee",
        "pnl",
        "realized",
        "note",
    ),
}
SIGNAL_LABEL_TO_VALUE = {
    "双向": "both",
    "只做多": "long_only",
    "只做空": "short_only",
}
POSITION_MODE_OPTIONS = {
    "净持仓 net": "net",
    "双向持仓 long/short": "long_short",
}
TRADE_MODE_OPTIONS = {
    "全仓 cross": "cross",
    "逐仓 isolated": "isolated",
}
ENV_OPTIONS = {
    "模拟盘 demo": "demo",
    "实盘 live": "live",
}


def _blank_credential_profile_snapshot(*, environment: str = "") -> dict[str, str]:
    snapshot = {
        "api_key": "",
        "secret_key": "",
        "passphrase": "",
    }
    if environment in {"demo", "live"}:
        snapshot["environment"] = environment
    return snapshot


TRIGGER_TYPE_OPTIONS = {
    "标记价格 mark": "mark",
    "最新成交价 last": "last",
    "指数价格 index": "index",
}
TP_SL_MODE_OPTIONS = {
    "OKX 托管（仅同标的永续）": "exchange",
    "按交易标的价格（本地）": "local_trade",
    "按下单标的价格（本地）": "local_trade",
    "按信号标的价格（本地）": "local_signal",
    "按自定义标的价格（本地）": "local_custom",
}
LAUNCHER_TP_SL_MODE_LABELS = (
    "OKX 托管（仅同标的永续）",
    "按交易标的价格（本地）",
    "按自定义标的价格（本地）",
)
ENTRY_SIDE_MODE_OPTIONS = {
    "跟随信号": "follow_signal",
    "固定买入": "fixed_buy",
    "固定卖出": "fixed_sell",
}
TAKE_PROFIT_MODE_OPTIONS = {
    "固定止盈": "fixed",
    "动态止盈": "dynamic",
}
RUN_MODE_OPTIONS = {
    "交易并下单": "trade",
    "只发信号邮件": "signal_only",
}
RUNNING_SESSION_FILTER_OPTIONS = ("全部", "普通量化", "交易员策略", "信号观察台")
STRATEGY_BOOK_FILTER_ALL_API = "全部API"
STRATEGY_BOOK_FILTER_ALL_TRADER = "全部交易员"
STRATEGY_BOOK_FILTER_ALL_STRATEGY = "全部策略"
STRATEGY_BOOK_FILTER_ALL_SYMBOL = "全部标的"
STRATEGY_BOOK_FILTER_ALL_BAR = "全部周期"
STRATEGY_BOOK_FILTER_ALL_DIRECTION = "全部方向"
STRATEGY_BOOK_FILTER_ALL_STATUS = "全部状态"
STRATEGY_HISTORY_FILTER_ALL_MODE = "全部模式"
STRATEGY_HISTORY_FILTER_ALL_PNL = "全部净盈亏"
STRATEGY_HISTORY_FILTER_PNL_PROFIT = "盈利"
STRATEGY_HISTORY_FILTER_PNL_LOSS = "亏损"
STRATEGY_HISTORY_FILTER_PNL_FLAT = "持平"
POSITION_TYPE_OPTIONS = {
    "全部类型": "",
    "永续 SWAP": "SWAP",
    "交割 FUTURES": "FUTURES",
    "期权 OPTION": "OPTION",
    "现货 SPOT": "SPOT",
}
HISTORY_MARGIN_MODE_FILTER_OPTIONS = {
    "全部模式": "",
    "全仓 cross": "cross",
    "逐仓 isolated": "isolated",
}
HISTORY_FILL_SIDE_FILTER_OPTIONS = {
    "全部方向": "",
    "买入 buy": "buy",
    "卖出 sell": "sell",
}
ORDER_SOURCE_FILTER_OPTIONS = {
    "全部来源": "",
    "普通委托": "normal",
    "算法委托": "algo",
}
ORDER_STATE_FILTER_OPTIONS = {
    "全部状态": "",
    "生效 live": "live",
    "部分成交 partially_filled": "partially_filled",
    "已成交 filled": "filled",
    "已撤销 canceled": "canceled",
    "算法生效 effective": "effective",
    "失败 order_failed": "order_failed",
}
ENGINE_CL_ORD_ID_PATTERN = re.compile(r"^[a-z0-9]{4,8}(ent|exi)[0-9]{15}$")
PROTECTION_CL_ORD_ID_PATTERN = re.compile(r"^ppp\d{2,}\d{4}$")
SMART_ORDER_CL_ORD_ID_PATTERN = re.compile(r"^so[a-z0-9]{4}\d{6}$")
SESSION_BAR_TIME_PATTERN = re.compile(r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) \|")
FUNDING_FEE_BILL_SUBTYPES = {"173", "174"}
FUNDING_FEE_BILL_MARKERS = ("funding", "资金费")
POSITION_REFRESH_INTERVAL_OPTIONS = {
    "10秒": 10_000,
    "15秒": 15_000,
    "30秒": 30_000,
    "60秒": 60_000,
}
REFRESH_STALE_FAILURE_THRESHOLD = 3
REFRESH_BADGE_PALETTES = {
    "idle": {"bg": "#f3f4f6", "fg": "#4b5563"},
    "normal": {"bg": "#e8f7ee", "fg": "#137333"},
    "warning": {"bg": "#fff4e5", "fg": "#9a6700"},
    "stale": {"bg": "#fde8e8", "fg": "#b42318"},
}
PROTECTION_TRIGGER_SOURCE_OPTIONS = {
    "期权标记价格": "option_mark",
    "现货最新价": "spot_last",
}
PROTECTION_ORDER_MODE_OPTIONS = {
    "设定价格": "fixed_price",
    "标记价格加减滑点": "mark_with_slippage",
}


def _format_network_error_message(message: str) -> str:
    raw = (message or "").strip()
    if not raw:
        return "网络请求失败，请稍后重试。"
    html_summary = _summarize_http_error_message(raw)
    if html_summary:
        return html_summary
    lowered = raw.lower()
    if "handshake operation timed out" in lowered:
        return "网络握手超时，请稍后重试。"
    if "read operation timed out" in lowered or "read timed out" in lowered:
        return "网络读取超时，请稍后重试。"
    if "remote end closed connection without response" in lowered or "remotedisconnected" in lowered:
        return "交易所提前断开连接，请稍后重试。"
    if "timed out" in lowered:
        return "网络连接超时，请稍后重试。"
    collapsed = _collapse_error_message(raw)
    if len(collapsed) > 220:
        return f"{collapsed[:217]}..."
    return collapsed


@dataclass
class RefreshHealthState:
    label: str
    last_success_at: datetime | None = None
    consecutive_failures: int = 0
    stale_since: datetime | None = None
    last_error_summary: str | None = None


def _collapse_error_message(message: str) -> str:
    return re.sub(r"\s+", " ", (message or "")).strip()


def _extract_http_status_code(message: str) -> str | None:
    raw = message or ""
    patterns = (
        r"\bHTTP\s*(\d{3})\b",
        r"\berror code\s*(\d{3})\b",
        r"\b(\d{3})\s*:\s*Bad gateway\b",
        r"\b(\d{3})\s*:\s*Service unavailable\b",
    )
    for pattern in patterns:
        match = re.search(pattern, raw, flags=re.IGNORECASE)
        if match:
            return match.group(1)
    return None


def _extract_cloudflare_ray_id(message: str) -> str | None:
    match = re.search(
        r"Cloudflare Ray ID:\s*(?:<strong[^>]*>)?([A-Za-z0-9]+)",
        message or "",
        flags=re.IGNORECASE | re.DOTALL,
    )
    if match:
        return match.group(1)
    return None


def _summarize_http_error_message(message: str) -> str | None:
    raw = (message or "").strip()
    lowered = raw.lower()
    if "<html" not in lowered and "<!doctype html" not in lowered:
        return None
    status_code = _extract_http_status_code(raw)
    parts: list[str] = []
    if status_code:
        parts.append(f"HTTP {status_code}")
    if "cloudflare" in lowered and ("bad gateway" in lowered or ("host" in lowered and "error" in lowered)):
        parts.append("OKX源站异常")
    elif status_code and status_code.startswith("5"):
        parts.append("交易所服务暂时不可用")
    elif "cloudflare" in lowered:
        parts.append("Cloudflare 错误页")
    else:
        parts.append("交易所返回HTML错误页")
    ray_id = _extract_cloudflare_ray_id(raw)
    if ray_id:
        parts.append(f"RayID={ray_id}")
    return " | ".join(parts)


def _mark_refresh_health_success(state: RefreshHealthState, *, at: datetime | None = None) -> None:
    state.last_success_at = at or datetime.now()
    state.consecutive_failures = 0
    state.stale_since = None
    state.last_error_summary = None


def _reset_refresh_health(state: RefreshHealthState) -> None:
    state.last_success_at = None
    state.consecutive_failures = 0
    state.stale_since = None
    state.last_error_summary = None


def _mark_refresh_health_failure(
    state: RefreshHealthState,
    summary: str,
    *,
    at: datetime | None = None,
    stale_after_failures: int = REFRESH_STALE_FAILURE_THRESHOLD,
) -> None:
    failure_at = at or datetime.now()
    state.consecutive_failures += 1
    state.last_error_summary = summary
    if (
        state.last_success_at is not None
        and state.consecutive_failures >= stale_after_failures
        and state.stale_since is None
    ):
        state.stale_since = failure_at


def _format_elapsed_compact(total_seconds: int) -> str:
    seconds = max(int(total_seconds), 0)
    if seconds < 60:
        return f"{seconds}秒"
    if seconds < 3600:
        minutes, remain = divmod(seconds, 60)
        return f"{minutes}分{remain}秒" if remain else f"{minutes}分"
    hours, remain = divmod(seconds, 3600)
    minutes = remain // 60
    return f"{hours}小时{minutes}分" if minutes else f"{hours}小时"


def _format_refresh_health_suffix(state: RefreshHealthState, *, now: datetime | None = None) -> str:
    if state.consecutive_failures <= 0:
        return ""
    current_at = now or datetime.now()
    parts = [f"连续失败：{state.consecutive_failures}次"]
    if state.last_success_at is not None:
        parts.append(f"上次成功：{state.last_success_at.strftime('%H:%M:%S')}")
        if state.stale_since is not None:
            age_seconds = int((current_at - state.last_success_at).total_seconds())
            parts.append(f"缓存年龄≈{_format_elapsed_compact(age_seconds)}")
            parts.append("数据可能已过期")
    return " | " + " | ".join(parts)


def _refresh_health_is_stale(state: RefreshHealthState) -> bool:
    return state.stale_since is not None


def _refresh_indicator_level(state: RefreshHealthState) -> str:
    if _refresh_health_is_stale(state):
        return "stale"
    if state.consecutive_failures > 0:
        return "warning"
    if state.last_success_at is None:
        return "idle"
    return "normal"


def _refresh_indicator_badge_text(state: RefreshHealthState) -> str:
    level = _refresh_indicator_level(state)
    if level == "stale":
        return f"过期 x{state.consecutive_failures}"
    if level == "warning":
        return f"告警 x{state.consecutive_failures}"
    if level == "normal":
        return "正常"
    return "未读"


def _describe_refresh_health(state: RefreshHealthState, *, now: datetime | None = None) -> str:
    current_at = now or datetime.now()
    lines = [f"连续失败：{state.consecutive_failures}次"]
    if state.last_success_at is not None:
        lines.append(f"上次成功：{state.last_success_at.strftime('%Y-%m-%d %H:%M:%S')}")
        age_seconds = int((current_at - state.last_success_at).total_seconds())
        lines.append(f"缓存年龄：{_format_elapsed_compact(age_seconds)}")
    if state.last_error_summary:
        lines.append(f"最近原因：{state.last_error_summary}")
    if state.stale_since is not None:
        lines.append("当前数据可能已过期。")
    return "\n".join(lines)


@dataclass
class ProfilePositionSnapshot:
    api_name: str
    effective_environment: str | None
    positions: list[OkxPosition]
    upl_usdt_prices: dict[str, Decimal]
    refreshed_at: datetime
    position_instruments: dict[str, Instrument] = field(default_factory=dict)


@dataclass
class StrategyTradeRuntimeState:
    round_id: str
    signal_bar_at: datetime | None = None
    opened_logged_at: datetime | None = None
    entry_order_id: str = ""
    entry_client_order_id: str = ""
    entry_price: Decimal | None = None
    size: Decimal | None = None
    pending_entry_reference: Decimal | None = None
    pending_stop_price: Decimal | None = None
    pending_take_profit: Decimal | None = None
    pending_side: str = ""
    pending_signal: str = ""
    protective_algo_id: str = ""
    protective_algo_cl_ord_id: str = ""
    initial_stop_price: Decimal | None = None
    current_stop_price: Decimal | None = None
    reconciliation_started: bool = False


@dataclass
class StrategyTradeLedgerRecord:
    record_id: str
    history_record_id: str
    session_id: str
    api_name: str
    strategy_id: str
    strategy_name: str
    symbol: str
    direction_label: str
    run_mode_label: str
    environment: str
    closed_at: datetime
    signal_bar_at: datetime | None = None
    opened_at: datetime | None = None
    entry_order_id: str = ""
    entry_client_order_id: str = ""
    exit_order_id: str = ""
    protective_algo_id: str = ""
    protective_algo_cl_ord_id: str = ""
    entry_price: Decimal | None = None
    exit_price: Decimal | None = None
    size: Decimal | None = None
    entry_fee: Decimal | None = None
    exit_fee: Decimal | None = None
    funding_fee: Decimal | None = None
    gross_pnl: Decimal | None = None
    net_pnl: Decimal | None = None
    close_reason: str = ""
    reason_confidence: str = "low"
    summary_note: str = ""
    updated_at: datetime | None = None


@dataclass
class StrategyTradeReconciliationSnapshot:
    effective_environment: str
    order_history: list[OkxTradeOrderItem]
    fills: list[OkxFillHistoryItem]
    position_history: list[OkxPositionHistoryItem]
    account_bills: list[OkxAccountBillItem]
    environment_note: str | None = None


@dataclass
class StrategyTradeReconciliationResult:
    session_id: str
    round_id: str
    ledger_record: StrategyTradeLedgerRecord | None = None
    environment_note: str | None = None
    attribution_summary: str = ""
    cumulative_summary: str = ""
    error_message: str = ""


@dataclass
class StrategySession:
    session_id: str
    api_name: str
    strategy_id: str
    strategy_name: str
    symbol: str
    direction_label: str
    run_mode_label: str
    engine: StrategyEngine
    config: StrategyConfig
    started_at: datetime
    status: str = "运行中"
    history_record_id: str | None = None
    stopped_at: datetime | None = None
    ended_reason: str = ""
    log_file_path: Path | None = None
    stop_cleanup_in_progress: bool = False
    runtime_status: str = "启动中"
    last_message: str = ""
    recovery_root_dir: Path | None = None
    recovery_supported: bool = False
    active_trade: StrategyTradeRuntimeState | None = None
    trade_count: int = 0
    win_count: int = 0
    gross_pnl_total: Decimal = Decimal("0")
    fee_total: Decimal = Decimal("0")
    funding_total: Decimal = Decimal("0")
    net_pnl_total: Decimal = Decimal("0")
    last_close_reason: str = ""
    trader_id: str = ""
    trader_slot_id: str = ""
    email_notifications_enabled: bool = True

    @property
    def log_prefix(self) -> str:
        if self.api_name:
            return f"[{self.api_name}] [{self.session_id} {self.strategy_name} {self.symbol}]"
        return f"[{self.session_id} {self.strategy_name} {self.symbol}]"

    @property
    def display_status(self) -> str:
        if self.status == "运行中" and self.runtime_status:
            return self.runtime_status
        return self.status


@dataclass
class StrategyLiveChartWindowState:
    session_id: str
    window: Toplevel
    canvas: Canvas
    headline_text: StringVar
    status_text: StringVar
    footer_text: StringVar
    refresh_job: str | None = None
    refresh_inflight: bool = False
    last_snapshot: StrategyLiveChartSnapshot | None = None
    line_annotations: list["LiveChartLineAnnotation"] = field(default_factory=list)
    active_tool: str = "none"
    draft_line_start: tuple[float, float] | None = None
    draft_line_current: tuple[float, float] | None = None
    trade_price_basis: StringVar | None = None
    trade_stop_basis: StringVar | None = None
    trade_order_mode: StringVar | None = None
    trade_risk_mode: StringVar | None = None
    trade_risk_amount: StringVar | None = None
    trade_fixed_size: StringVar | None = None
    trade_status_text: StringVar | None = None


@dataclass
class LiveChartLineAnnotation:
    kind: str
    x1: float
    y1: float
    x2: float
    y2: float
    color: str
    label: str = ""
    bar_a: float | None = None
    price_a: Decimal | None = None
    bar_b: float | None = None
    price_b: Decimal | None = None
    desk_ray_action: str = "notify"
    desk_ray_triggered: bool = False
    desk_ray_submit_pending: bool = False
    desk_ray_last_side: int | None = None
    locked: bool = False


@dataclass
class DeskRiskRewardAnnotation:
    rr_id: str
    side: str
    bar_entry: float
    price_entry: Decimal
    bar_stop: float
    price_stop: Decimal
    price_tp: Decimal
    r_multiple: Decimal = field(default_factory=lambda: Decimal("2"))
    locked: bool = False


@dataclass
class LineTradingDeskWindowState:
    window: Toplevel
    canvas: Canvas
    symbol_var: StringVar
    bar_var: StringVar
    status_text: StringVar
    position_tree: ttk.Treeview
    pending_orders_tree: ttk.Treeview
    order_history_tree: ttk.Treeview
    api_profile_var: StringVar
    param_atr_period: StringVar
    param_atr_mult: StringVar
    param_risk_amount: StringVar
    param_cross_mode: StringVar
    param_ray_action: StringVar
    param_order_mode: StringVar
    param_rr_r: StringVar
    param_rr_side: StringVar
    param_rr_fee_offset: BooleanVar
    param_close_qty: StringVar
    ray_tree: ttk.Treeview
    rr_manage_tree: ttk.Treeview
    refresh_job: str | None = None
    last_snapshot: StrategyLiveChartSnapshot | None = None
    latest_positions: list[OkxPosition] = field(default_factory=list)
    latest_pending_orders: list[OkxTradeOrderItem] = field(default_factory=list)
    latest_order_history: list[OkxTradeOrderItem] = field(default_factory=list)
    line_annotations: list[LiveChartLineAnnotation] = field(default_factory=list)
    rr_annotations: list[DeskRiskRewardAnnotation] = field(default_factory=list)
    active_tool: str = "none"
    draft_line_start: tuple[float, float] | None = None
    draft_line_current: tuple[float, float] | None = None
    desk_view_start: int = 0
    desk_visible_bars: int = 200
    desk_pan_origin: tuple[float, int] | None = None
    last_desk_api_profile: str = ""
    desk_initial_scroll_done: bool = False
    rr_selected_id: str | None = None
    rr_drag: tuple[str, str] | None = None
    rr_pick: tuple[str, str, float, float] | None = None
    desk_chart_paint_job: str | None = None
    desk_last_chart_paint_t: float = 0.0
    desk_range_zoom_active: bool = False
    desk_price_tick: Decimal | None = None
    desk_tick_symbol: str | None = None
    desk_canvas_configure_job: str | None = None
    desk_canvas_last_wh: tuple[int, int] | None = None
    desk_refresh_generation: int = 0
    desk_instrument_cache: dict[str, tuple[Instrument | None, float]] = field(default_factory=dict)
    desk_submit_inflight: bool = False
    desk_log_text: Text | None = None
    desk_chart_throttle_job: str | None = None
    desk_last_chart_render_t: float = 0.0
    desk_layout_retry_job: str | None = None
    desk_layout_bootstrap_gen: int = 0
    desk_map_render_job: str | None = None
    desk_annotation_store_key: str | None = None
    desk_annotation_save_job: str | None = None


_LINE_DESK_INSTRUMENT_CACHE_TTL_S = 120.0
# 划线交易台主图全量重绘最小间隔（秒），合并高频 pan/滚轮触发的 delete+重画，减轻卡顿与闪屏。
_LINE_DESK_CHART_MIN_INTERVAL_S = 1.0 / 12.0
# 射线/盈亏比等标注落盘防抖（毫秒）；关窗、切标的会立即 flush。
_LINE_DESK_ANNOTATION_PERSIST_MS = 450


# 划线交易台「新射线默认」下拉与内部 desk_ray_action（notify/long/short）对照
# notify：只记日志/界面提示，不自动下单（不单独触发邮件；邮件仍看全局「启用邮件通知」等设置）
_LINE_DESK_RAY_ACTION_LABEL_ZH = {"notify": "通知", "long": "开多", "short": "开空"}
_LINE_DESK_RAY_ACTION_FROM_ZH = {v: k for k, v in _LINE_DESK_RAY_ACTION_LABEL_ZH.items()}


def _line_trading_desk_normalize_ray_action(raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        return "notify"
    low = s.lower()
    if low in ("notify", "long", "short"):
        return low
    if s == "仅站内":
        return "notify"
    return _LINE_DESK_RAY_ACTION_FROM_ZH.get(s, "notify")


@dataclass
class StrategyStopCleanupSnapshot:
    effective_environment: str
    pending_orders: list[OkxTradeOrderItem]
    order_history: list[OkxTradeOrderItem]
    positions: list[OkxPosition]
    environment_note: str | None = None


@dataclass
class StrategyStopCleanupResult:
    session_id: str
    effective_environment: str
    environment_note: str | None = None
    cancel_requested_summaries: tuple[str, ...] = ()
    cancel_failed_summaries: tuple[str, ...] = ()
    remaining_pending_summaries: tuple[str, ...] = ()
    protective_pending_summaries: tuple[str, ...] = ()
    filled_order_summaries: tuple[str, ...] = ()
    open_position_summaries: tuple[str, ...] = ()
    needs_manual_review: bool = False
    final_reason: str = "用户手动停止"


@dataclass
class StrategyHistoryRecord:
    record_id: str
    session_id: str
    api_name: str
    strategy_id: str
    strategy_name: str
    symbol: str
    direction_label: str
    run_mode_label: str
    status: str
    started_at: datetime
    stopped_at: datetime | None = None
    ended_reason: str = ""
    log_file_path: str = ""
    updated_at: datetime | None = None
    config_snapshot: dict[str, object] = field(default_factory=dict)
    trade_count: int = 0
    win_count: int = 0
    gross_pnl_total: Decimal = Decimal("0")
    fee_total: Decimal = Decimal("0")
    funding_total: Decimal = Decimal("0")
    net_pnl_total: Decimal = Decimal("0")
    last_close_reason: str = ""


@dataclass
class NormalStrategyBookSummary:
    strategy_count: int = 0
    history_count: int = 0
    trade_count: int = 0
    win_count: int = 0
    loss_count: int = 0
    api_count: int = 0
    gross_pnl_total: Decimal = field(default_factory=lambda: Decimal("0"))
    fee_total: Decimal = field(default_factory=lambda: Decimal("0"))
    funding_total: Decimal = field(default_factory=lambda: Decimal("0"))
    net_pnl_total: Decimal = field(default_factory=lambda: Decimal("0"))


@dataclass(frozen=True)
class NormalStrategyBookFilters:
    api_name: str = ""
    trader_label: str = ""
    strategy_name: str = ""
    symbol: str = ""
    bar: str = ""
    direction_label: str = ""
    status: str = ""


@dataclass(frozen=True)
class StrategyHistoryFilters:
    api_name: str = ""
    strategy_name: str = ""
    symbol: str = ""
    direction_label: str = ""
    run_mode_label: str = ""
    pnl_bucket: str = ""
    status: str = ""


@dataclass
class RecoverableStrategySessionRecord:
    session_id: str
    api_name: str
    strategy_id: str
    strategy_name: str
    symbol: str
    direction_label: str
    run_mode_label: str
    started_at: datetime
    history_record_id: str = ""
    log_file_path: Path | None = None
    recovery_root_dir: Path | None = None
    config_snapshot: dict[str, object] = field(default_factory=dict)
    updated_at: datetime | None = None


@dataclass(frozen=True)
class StrategyTemplateRecord:
    strategy_id: str
    strategy_name: str
    api_name: str
    direction_label: str
    run_mode_label: str
    symbol: str
    config: StrategyConfig
    exported_at: datetime | None = None


class PositionNoteEditorDialog(simpledialog.Dialog):
    def __init__(self, parent: Tk | Toplevel, *, title: str, prompt: str, initial_value: str = "") -> None:
        self._prompt = prompt
        self._initial_value = initial_value
        self._note_text_widget: Text | None = None
        self.result_text: str | None = None
        super().__init__(parent, title)

    def body(self, master: ttk.Frame) -> Text:
        master.columnconfigure(0, weight=1)
        master.rowconfigure(1, weight=1)
        ttk.Label(master, text=self._prompt, justify="left", wraplength=520).grid(row=0, column=0, sticky="w", pady=(0, 8))
        note_text = Text(master, width=68, height=8, wrap="word", font=("Microsoft YaHei UI", 10))
        note_text.grid(row=1, column=0, sticky="nsew")
        note_text.insert("1.0", self._initial_value)
        note_text.focus_set()
        self._note_text_widget = note_text
        return note_text

    def apply(self) -> None:
        if self._note_text_widget is None:
            self.result_text = ""
            return
        self.result_text = _normalize_position_note_text(self._note_text_widget.get("1.0", END))


def _serialize_strategy_config_snapshot(config: StrategyConfig) -> dict[str, object]:
    snapshot: dict[str, object] = {}
    for item in dataclass_fields(StrategyConfig):
        value = getattr(config, item.name)
        if isinstance(value, Decimal):
            snapshot[item.name] = format(value, "f")
        else:
            snapshot[item.name] = value
    return snapshot


def _strategy_config_default(field_name: str) -> object:
    for item in dataclass_fields(StrategyConfig):
        if item.name != field_name:
            continue
        if item.default is not MISSING:
            return item.default
        if item.default_factory is not MISSING:
            return item.default_factory()
        break
    raise KeyError(field_name)


def _coerce_snapshot_decimal(value: object, default: Decimal) -> Decimal:
    raw = str(value or "").strip()
    if not raw:
        return default
    try:
        return Decimal(raw)
    except (InvalidOperation, ValueError):
        return default


def _coerce_snapshot_optional_decimal(value: object) -> Decimal | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return Decimal(raw)
    except (InvalidOperation, ValueError):
        return None


def _coerce_snapshot_int(value: object, default: int, *, minimum: int | None = None) -> int:
    try:
        parsed = int(float(str(value)))
    except (TypeError, ValueError):
        parsed = default
    if minimum is not None:
        parsed = max(parsed, minimum)
    return parsed


def _coerce_snapshot_float(value: object, default: float, *, minimum: float | None = None) -> float:
    try:
        parsed = float(str(value))
    except (TypeError, ValueError):
        parsed = default
    if minimum is not None:
        parsed = max(parsed, minimum)
    return parsed


def _coerce_snapshot_bool(value: object, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    raw = str(value).strip().lower()
    if raw in {"1", "true", "yes", "on", "enabled"}:
        return True
    if raw in {"0", "false", "no", "off", "disabled"}:
        return False
    return default


def _coerce_snapshot_text(value: object, default: str = "") -> str:
    raw = str(value or "").strip()
    return raw or default


def _coerce_snapshot_optional_text(value: object) -> str | None:
    raw = str(value or "").strip()
    return raw or None


def _reverse_lookup_label(mapping: dict[str, str], value: str, default: str) -> str:
    normalized = str(value or "").strip()
    for label, candidate in mapping.items():
        if candidate == normalized:
            return label
    return default


def _deserialize_strategy_config_snapshot(payload: object) -> StrategyConfig | None:
    if not isinstance(payload, dict):
        return None
    inst_id = _coerce_snapshot_text(payload.get("inst_id"))
    bar = _coerce_snapshot_text(payload.get("bar"))
    if not inst_id or not bar:
        return None
    return StrategyConfig(
        inst_id=inst_id,
        bar=bar,
        ema_period=_coerce_snapshot_int(payload.get("ema_period"), 21, minimum=1),
        atr_period=_coerce_snapshot_int(payload.get("atr_period"), 10, minimum=1),
        atr_stop_multiplier=_coerce_snapshot_decimal(payload.get("atr_stop_multiplier"), Decimal("2")),
        atr_take_multiplier=_coerce_snapshot_decimal(payload.get("atr_take_multiplier"), Decimal("4")),
        order_size=_coerce_snapshot_decimal(payload.get("order_size"), Decimal("1")),
        trade_mode=_coerce_snapshot_text(payload.get("trade_mode"), "cross"),
        signal_mode=_coerce_snapshot_text(payload.get("signal_mode"), "both"),
        position_mode=_coerce_snapshot_text(payload.get("position_mode"), "net"),
        environment=_coerce_snapshot_text(payload.get("environment"), "demo"),
        tp_sl_trigger_type=_coerce_snapshot_text(payload.get("tp_sl_trigger_type"), "mark"),
        trend_ema_period=_coerce_snapshot_int(
            payload.get("trend_ema_period"),
            int(_strategy_config_default("trend_ema_period")),
            minimum=1,
        ),
        big_ema_period=_coerce_snapshot_int(
            payload.get("big_ema_period"),
            int(_strategy_config_default("big_ema_period")),
            minimum=1,
        ),
        strategy_id=_coerce_snapshot_text(
            payload.get("strategy_id"),
            str(_strategy_config_default("strategy_id")),
        ),
        poll_seconds=_coerce_snapshot_float(
            payload.get("poll_seconds"),
            float(_strategy_config_default("poll_seconds")),
            minimum=0.2,
        ),
        risk_amount=_coerce_snapshot_optional_decimal(payload.get("risk_amount")),
        trade_inst_id=_coerce_snapshot_optional_text(payload.get("trade_inst_id")),
        tp_sl_mode=_coerce_snapshot_text(
            payload.get("tp_sl_mode"),
            str(_strategy_config_default("tp_sl_mode")),
        ),
        local_tp_sl_inst_id=_coerce_snapshot_optional_text(payload.get("local_tp_sl_inst_id")),
        entry_side_mode=_coerce_snapshot_text(
            payload.get("entry_side_mode"),
            str(_strategy_config_default("entry_side_mode")),
        ),
        run_mode=_coerce_snapshot_text(
            payload.get("run_mode"),
            str(_strategy_config_default("run_mode")),
        ),
        backtest_initial_capital=_coerce_snapshot_decimal(
            payload.get("backtest_initial_capital"),
            Decimal(str(_strategy_config_default("backtest_initial_capital"))),
        ),
        backtest_sizing_mode=_coerce_snapshot_text(
            payload.get("backtest_sizing_mode"),
            str(_strategy_config_default("backtest_sizing_mode")),
        ),
        backtest_risk_percent=_coerce_snapshot_optional_decimal(payload.get("backtest_risk_percent")),
        backtest_compounding=_coerce_snapshot_bool(
            payload.get("backtest_compounding"),
            bool(_strategy_config_default("backtest_compounding")),
        ),
        backtest_entry_slippage_rate=_coerce_snapshot_decimal(
            payload.get("backtest_entry_slippage_rate"),
            Decimal(str(_strategy_config_default("backtest_entry_slippage_rate"))),
        ),
        backtest_exit_slippage_rate=_coerce_snapshot_decimal(
            payload.get("backtest_exit_slippage_rate"),
            Decimal(str(_strategy_config_default("backtest_exit_slippage_rate"))),
        ),
        backtest_slippage_rate=_coerce_snapshot_decimal(
            payload.get("backtest_slippage_rate"),
            Decimal(str(_strategy_config_default("backtest_slippage_rate"))),
        ),
        backtest_funding_rate=_coerce_snapshot_decimal(
            payload.get("backtest_funding_rate"),
            Decimal(str(_strategy_config_default("backtest_funding_rate"))),
        ),
        take_profit_mode=_coerce_snapshot_text(
            payload.get("take_profit_mode"),
            str(_strategy_config_default("take_profit_mode")),
        ),
        max_entries_per_trend=_coerce_snapshot_int(
            payload.get("max_entries_per_trend"),
            int(_strategy_config_default("max_entries_per_trend")),
            minimum=1,
        ),
        entry_reference_ema_period=_coerce_snapshot_int(
            payload.get("entry_reference_ema_period"),
            int(_strategy_config_default("entry_reference_ema_period")),
            minimum=1,
        ),
        dynamic_two_r_break_even=_coerce_snapshot_bool(
            payload.get("dynamic_two_r_break_even"),
            bool(_strategy_config_default("dynamic_two_r_break_even")),
        ),
        dynamic_fee_offset_enabled=_coerce_snapshot_bool(
            payload.get("dynamic_fee_offset_enabled"),
            bool(_strategy_config_default("dynamic_fee_offset_enabled")),
        ),
        startup_chase_window_seconds=_coerce_snapshot_int(
            payload.get("startup_chase_window_seconds"),
            int(_strategy_config_default("startup_chase_window_seconds")),
            minimum=0,
        ),
        time_stop_break_even_enabled=_coerce_snapshot_bool(
            payload.get("time_stop_break_even_enabled"),
            bool(_strategy_config_default("time_stop_break_even_enabled")),
        ),
        time_stop_break_even_bars=_coerce_snapshot_int(
            payload.get("time_stop_break_even_bars"),
            int(_strategy_config_default("time_stop_break_even_bars")),
            minimum=0,
        ),
        trader_virtual_stop_loss=_coerce_snapshot_bool(
            payload.get("trader_virtual_stop_loss"),
            bool(_strategy_config_default("trader_virtual_stop_loss")),
        ),
        backtest_profile_id=_coerce_snapshot_text(
            payload.get("backtest_profile_id"),
            str(_strategy_config_default("backtest_profile_id")),
        ),
        backtest_profile_name=_coerce_snapshot_text(
            payload.get("backtest_profile_name"),
            str(_strategy_config_default("backtest_profile_name")),
        ),
        backtest_profile_summary=_coerce_snapshot_text(
            payload.get("backtest_profile_summary"),
            str(_strategy_config_default("backtest_profile_summary")),
        ),
    )


def _strategy_template_direction_label(strategy_id: str, config: StrategyConfig, fallback: str = "") -> str:
    effective_signal_mode = resolve_dynamic_signal_mode(strategy_id, config.signal_mode)
    default_label = fallback or _reverse_lookup_label(SIGNAL_LABEL_TO_VALUE, effective_signal_mode, "双向")
    label = _reverse_lookup_label(SIGNAL_LABEL_TO_VALUE, effective_signal_mode, default_label)
    try:
        definition = get_strategy_definition(strategy_id)
    except KeyError:
        return label
    if label in definition.allowed_signal_labels:
        return label
    return definition.default_signal_label


def _launcher_symbol_from_strategy_config(strategy_id: str, config: StrategyConfig) -> str:
    del strategy_id
    return (config.trade_inst_id or config.inst_id or "").strip().upper()


def _launcher_tp_sl_mode_label(tp_sl_mode: str) -> str:
    if tp_sl_mode == "exchange":
        return "OKX 托管（仅同标的永续）"
    if tp_sl_mode == "local_custom":
        return "按自定义标的价格（本地）"
    return "按交易标的价格（本地）"


def _format_entry_decimal(value: Decimal | None) -> str:
    if value is None:
        return ""
    return format(value, "f")


def _format_entry_float(value: float) -> str:
    text = format(float(value), "g")
    return text if text != "-0" else "0"


def _build_strategy_template_payload(session: StrategySession) -> dict[str, object]:
    return {
        "schema_version": STRATEGY_TEMPLATE_SCHEMA_VERSION,
        "exported_at": datetime.now().isoformat(timespec="seconds"),
        "app_version": APP_VERSION,
        "api_name": session.api_name,
        "strategy_id": session.strategy_id,
        "strategy_name": session.strategy_name,
        "direction_label": session.direction_label,
        "run_mode_label": session.run_mode_label,
        "symbol": session.symbol,
        "includes_credentials": False,
        "config_snapshot": _serialize_strategy_config_snapshot(session.config),
    }


def _build_strategy_template_payload_from_record(record: StrategyTemplateRecord) -> dict[str, object]:
    return {
        "schema_version": STRATEGY_TEMPLATE_SCHEMA_VERSION,
        "exported_at": datetime.now().isoformat(timespec="seconds"),
        "app_version": APP_VERSION,
        "api_name": record.api_name,
        "strategy_id": record.strategy_id,
        "strategy_name": record.strategy_name,
        "direction_label": record.direction_label,
        "run_mode_label": record.run_mode_label,
        "symbol": record.symbol,
        "includes_credentials": False,
        "config_snapshot": _serialize_strategy_config_snapshot(record.config),
    }


def _strategy_template_record_from_payload(payload: object) -> StrategyTemplateRecord | None:
    if not isinstance(payload, dict):
        return None
    raw_snapshot = payload.get("config_snapshot")
    config = _deserialize_strategy_config_snapshot(raw_snapshot)
    if config is None:
        return None
    strategy_id = str(payload.get("strategy_id") or config.strategy_id).strip()
    if not strategy_id:
        return None
    strategy_name = str(payload.get("strategy_name") or "").strip()
    api_name = str(payload.get("api_name") or "").strip()
    direction_label = str(payload.get("direction_label") or "").strip()
    run_mode_label = str(payload.get("run_mode_label") or "").strip()
    symbol = str(payload.get("symbol") or "").strip()
    exported_at = _parse_datetime_snapshot(payload.get("exported_at"))
    try:
        definition = get_strategy_definition(strategy_id)
    except KeyError:
        definition = None
    if not strategy_name and definition is not None:
        strategy_name = definition.name
    if not run_mode_label:
        run_mode_label = _reverse_lookup_label(RUN_MODE_OPTIONS, config.run_mode, "交易并下单")
    direction_label = _strategy_template_direction_label(
        strategy_id,
        config,
        fallback=direction_label or (definition.default_signal_label if definition is not None else ""),
    )
    if not symbol:
        symbol = _launcher_symbol_from_strategy_config(strategy_id, config)
    return StrategyTemplateRecord(
        strategy_id=strategy_id,
        strategy_name=strategy_name,
        api_name=api_name,
        direction_label=direction_label,
        run_mode_label=run_mode_label,
        symbol=symbol,
        config=config,
        exported_at=exported_at,
    )


def _resolve_import_api_profile(source_api_name: str, current_api_name: str, available_profiles: set[str]) -> tuple[str, str]:
    imported = source_api_name.strip()
    current = current_api_name.strip()
    if imported and imported in available_profiles:
        return imported, f"已自动切换到导出文件里的 API：{imported}"
    if imported and imported != current:
        return current, f"导出文件里的 API：{imported} 在本机不存在，保留当前 API：{current}"
    if current:
        return current, f"继续使用当前 API：{current}"
    return "", "当前未选择 API，请先确认本机 API 配置。"


def _parse_datetime_snapshot(value: object) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    normalized = raw.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _parse_decimal_snapshot(value: object, *, default: Decimal | None = Decimal("0")) -> Decimal | None:
    raw = str(value or "").strip()
    if not raw:
        return default
    try:
        return Decimal(raw)
    except (InvalidOperation, ValueError):
        return default


def _format_history_datetime(value: datetime | None) -> str:
    if value is None:
        return "-"
    try:
        return value.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return "-"


def _coerce_log_file_path(value: object) -> Path | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return Path(raw).expanduser()
    except Exception:
        return None


def _history_record_source_type(record: StrategyHistoryRecord) -> str:
    snapshot = dict(record.config_snapshot or {})
    if bool(snapshot.get("trader_virtual_stop_loss")):
        return "交易员策略"
    run_mode = str(snapshot.get("run_mode") or "").strip().lower()
    if run_mode == "signal_only":
        return "信号观察台"
    return "普通量化"


def _history_record_bar_label(record: StrategyHistoryRecord | None) -> str:
    if record is None:
        return "-"
    snapshot = dict(record.config_snapshot or {})
    return str(snapshot.get("bar") or "").strip() or "-"


def _history_record_trader_label(record: StrategyHistoryRecord | None) -> str:
    if record is None:
        return "-"
    snapshot = dict(record.config_snapshot or {})
    trader_id = str(snapshot.get("trader_id") or "").strip()
    return trader_id or "-"


def _history_record_status_label(record: StrategyHistoryRecord | None) -> str:
    if record is None:
        return "-"
    return str(record.status or "").strip() or "-"


def _strategy_book_filter_normalized(value: str, all_label: str) -> str:
    normalized = str(value or "").strip()
    if not normalized or normalized == all_label:
        return ""
    return normalized


def _strategy_book_filter_match(actual: str, expected: str) -> bool:
    return not expected or actual == expected


def _strategy_history_pnl_bucket(record: StrategyHistoryRecord) -> str:
    pnl = record.net_pnl_total or Decimal("0")
    if pnl > 0:
        return STRATEGY_HISTORY_FILTER_PNL_PROFIT
    if pnl < 0:
        return STRATEGY_HISTORY_FILTER_PNL_LOSS
    return STRATEGY_HISTORY_FILTER_PNL_FLAT


def _strategy_history_record_matches(record: StrategyHistoryRecord, filters: StrategyHistoryFilters) -> bool:
    return (
        _strategy_book_filter_match(record.api_name or "-", filters.api_name)
        and _strategy_book_filter_match(record.strategy_name or "-", filters.strategy_name)
        and _strategy_book_filter_match(record.symbol or "-", filters.symbol)
        and _strategy_book_filter_match(record.direction_label or "-", filters.direction_label)
        and _strategy_book_filter_match(record.run_mode_label or "-", filters.run_mode_label)
        and _strategy_book_filter_match(_strategy_history_pnl_bucket(record), filters.pnl_bucket)
        and _strategy_book_filter_match(record.status or "-", filters.status)
    )


def _build_strategy_history_filter_options(records: list[StrategyHistoryRecord]) -> dict[str, tuple[str, ...]]:
    ordered = sorted(records, key=lambda item: (item.started_at, item.record_id), reverse=True)

    def _sorted_with_all(all_label: str, values: set[str]) -> tuple[str, ...]:
        cleaned = sorted(value for value in values if str(value or "").strip())
        return (all_label, *cleaned)

    return {
        "api_name": _sorted_with_all(STRATEGY_BOOK_FILTER_ALL_API, {record.api_name or "-" for record in ordered}),
        "strategy_name": _sorted_with_all(
            STRATEGY_BOOK_FILTER_ALL_STRATEGY,
            {record.strategy_name or "-" for record in ordered},
        ),
        "symbol": _sorted_with_all(STRATEGY_BOOK_FILTER_ALL_SYMBOL, {record.symbol or "-" for record in ordered}),
        "direction_label": _sorted_with_all(
            STRATEGY_BOOK_FILTER_ALL_DIRECTION,
            {record.direction_label or "-" for record in ordered},
        ),
        "run_mode_label": _sorted_with_all(
            STRATEGY_HISTORY_FILTER_ALL_MODE,
            {record.run_mode_label or "-" for record in ordered},
        ),
        "pnl_bucket": (
            STRATEGY_HISTORY_FILTER_ALL_PNL,
            STRATEGY_HISTORY_FILTER_PNL_PROFIT,
            STRATEGY_HISTORY_FILTER_PNL_LOSS,
            STRATEGY_HISTORY_FILTER_PNL_FLAT,
        ),
        "status": _sorted_with_all(STRATEGY_BOOK_FILTER_ALL_STATUS, {record.status or "-" for record in ordered}),
    }


def _normal_strategy_history_records(records: list[StrategyHistoryRecord]) -> list[StrategyHistoryRecord]:
    ordered = [record for record in records if _history_record_source_type(record) == "普通量化"]
    ordered.sort(key=lambda item: (item.started_at, item.record_id), reverse=True)
    return ordered


def _normal_strategy_history_record_matches(
    record: StrategyHistoryRecord,
    filters: NormalStrategyBookFilters,
) -> bool:
    return (
        _strategy_book_filter_match(record.api_name or "-", filters.api_name)
        and _strategy_book_filter_match(_history_record_trader_label(record), filters.trader_label)
        and _strategy_book_filter_match(record.strategy_name or "-", filters.strategy_name)
        and _strategy_book_filter_match(record.symbol or "-", filters.symbol)
        and _strategy_book_filter_match(_history_record_bar_label(record), filters.bar)
        and _strategy_book_filter_match(record.direction_label or "-", filters.direction_label)
        and _strategy_book_filter_match(_history_record_status_label(record), filters.status)
    )


def _normal_strategy_trade_ledger_records(
    ledger_records: list[StrategyTradeLedgerRecord],
    history_records: list[StrategyHistoryRecord],
    *,
    filters: NormalStrategyBookFilters | None = None,
) -> list[StrategyTradeLedgerRecord]:
    active_filters = filters or NormalStrategyBookFilters()
    history_by_id = {record.record_id: record for record in history_records}
    ordered: list[StrategyTradeLedgerRecord] = []
    for record in ledger_records:
        history_record = history_by_id.get(record.history_record_id)
        if history_record is not None:
            if _history_record_source_type(history_record) != "普通量化":
                continue
            if not _normal_strategy_history_record_matches(history_record, active_filters):
                continue
        elif "信号" in str(record.run_mode_label or ""):
            continue
        else:
            if not (
                _strategy_book_filter_match(record.api_name or "-", active_filters.api_name)
                and _strategy_book_filter_match("-", active_filters.trader_label)
                and _strategy_book_filter_match(record.strategy_name or "-", active_filters.strategy_name)
                and _strategy_book_filter_match(record.symbol or "-", active_filters.symbol)
                and _strategy_book_filter_match("-", active_filters.bar)
                and _strategy_book_filter_match(record.direction_label or "-", active_filters.direction_label)
                and _strategy_book_filter_match("-", active_filters.status)
            ):
                continue
        ordered.append(record)
    ordered.sort(key=lambda item: (item.closed_at, item.record_id), reverse=True)
    return ordered


def _build_normal_strategy_book_filter_options(
    ledger_records: list[StrategyTradeLedgerRecord],
    history_records: list[StrategyHistoryRecord],
) -> dict[str, tuple[str, ...]]:
    normal_history = _normal_strategy_history_records(history_records)
    normal_ledgers = _normal_strategy_trade_ledger_records(ledger_records, history_records)
    history_by_id = {record.record_id: record for record in normal_history}

    def _sorted_with_all(all_label: str, values: set[str]) -> tuple[str, ...]:
        cleaned = sorted(value for value in values if str(value or "").strip())
        return (all_label, *cleaned)

    api_names = {record.api_name or "-" for record in normal_ledgers}
    trader_labels = {_history_record_trader_label(history_by_id.get(record.history_record_id)) for record in normal_ledgers}
    strategy_names = {record.strategy_name or "-" for record in normal_ledgers}
    symbols = {record.symbol or "-" for record in normal_ledgers}
    bars = {_history_record_bar_label(history_by_id.get(record.history_record_id)) for record in normal_ledgers}
    directions = {record.direction_label or "-" for record in normal_ledgers}
    statuses = {_history_record_status_label(history_by_id.get(record.history_record_id)) for record in normal_ledgers}

    return {
        "api_name": _sorted_with_all(STRATEGY_BOOK_FILTER_ALL_API, api_names),
        "trader_label": _sorted_with_all(STRATEGY_BOOK_FILTER_ALL_TRADER, trader_labels),
        "strategy_name": _sorted_with_all(STRATEGY_BOOK_FILTER_ALL_STRATEGY, strategy_names),
        "symbol": _sorted_with_all(STRATEGY_BOOK_FILTER_ALL_SYMBOL, symbols),
        "bar": _sorted_with_all(STRATEGY_BOOK_FILTER_ALL_BAR, bars),
        "direction_label": _sorted_with_all(STRATEGY_BOOK_FILTER_ALL_DIRECTION, directions),
        "status": _sorted_with_all(STRATEGY_BOOK_FILTER_ALL_STATUS, statuses),
    }


def _build_normal_strategy_book_summary(
    ledger_records: list[StrategyTradeLedgerRecord],
    history_records: list[StrategyHistoryRecord],
    *,
    filters: NormalStrategyBookFilters | None = None,
) -> NormalStrategyBookSummary:
    active_filters = filters or NormalStrategyBookFilters()
    normal_history = [
        record for record in _normal_strategy_history_records(history_records)
        if _normal_strategy_history_record_matches(record, active_filters)
    ]
    normal_ledgers = _normal_strategy_trade_ledger_records(ledger_records, history_records, filters=active_filters)
    history_by_id = {record.record_id: record for record in normal_history}
    strategy_keys: set[tuple[str, str, str, str, str, str]] = set()
    api_names: set[str] = set()
    for record in normal_ledgers:
        history_record = history_by_id.get(record.history_record_id)
        strategy_keys.add(
            (
                record.api_name or "-",
                _history_record_trader_label(history_record),
                record.strategy_name or "-",
                record.symbol or "-",
                _history_record_bar_label(history_record),
                record.direction_label or "-",
            )
        )
        if record.api_name:
            api_names.add(record.api_name)
    return NormalStrategyBookSummary(
        strategy_count=len(strategy_keys),
        history_count=len(normal_history),
        trade_count=len(normal_ledgers),
        win_count=sum(1 for record in normal_ledgers if (record.net_pnl or Decimal("0")) > 0),
        loss_count=sum(1 for record in normal_ledgers if (record.net_pnl or Decimal("0")) <= 0),
        api_count=len(api_names),
        gross_pnl_total=sum(((record.gross_pnl or Decimal("0")) for record in normal_ledgers), Decimal("0")),
        fee_total=sum(
            (((record.entry_fee or Decimal("0")) + (record.exit_fee or Decimal("0"))) for record in normal_ledgers),
            Decimal("0"),
        ),
        funding_total=sum(((record.funding_fee or Decimal("0")) for record in normal_ledgers), Decimal("0")),
        net_pnl_total=sum(((record.net_pnl or Decimal("0")) for record in normal_ledgers), Decimal("0")),
    )


def _normal_strategy_book_summary_text(summary: NormalStrategyBookSummary) -> str:
    return (
        f"普通量化策略 {summary.strategy_count} 组"
        f" | 历史会话 {summary.history_count} 条"
        f" | 平仓 {summary.trade_count} 单"
        f" | 盈利 {summary.win_count}"
        f" | 亏损 {summary.loss_count}"
        f" | API {summary.api_count} 个"
        f" | 毛盈亏 {_format_optional_usdt_precise(summary.gross_pnl_total, places=2)}"
        f" | 手续费 {_format_optional_usdt_precise(summary.fee_total, places=2)}"
        f" | 资金费 {_format_optional_usdt_precise(summary.funding_total, places=2)}"
        f" | 总净盈亏 {_format_optional_usdt_precise(summary.net_pnl_total, places=2)}"
    )


def _build_normal_strategy_book_group_rows(
    ledger_records: list[StrategyTradeLedgerRecord],
    history_records: list[StrategyHistoryRecord],
    *,
    filters: NormalStrategyBookFilters | None = None,
) -> list[tuple[str, tuple[object, ...]]]:
    normal_ledgers = _normal_strategy_trade_ledger_records(ledger_records, history_records, filters=filters)
    history_by_id = {record.record_id: record for record in history_records}
    grouped: dict[tuple[str, str, str, str, str, str], list[StrategyTradeLedgerRecord]] = {}
    for record in normal_ledgers:
        history_record = history_by_id.get(record.history_record_id)
        key = (
            record.api_name or "-",
            _history_record_trader_label(history_record),
            record.strategy_name or "-",
            record.symbol or "-",
            _history_record_bar_label(history_record),
            record.direction_label or "-",
        )
        grouped.setdefault(key, []).append(record)

    rows: list[tuple[str, tuple[object, ...]]] = []
    ordered_items = sorted(
        grouped.items(),
        key=lambda item: max(row.closed_at for row in item[1]),
        reverse=True,
    )
    for key, records in ordered_items:
        api_name, trader_label, strategy_name, symbol, bar, direction_label = key
        trade_count = len(records)
        win_count = sum(1 for record in records if (record.net_pnl or Decimal("0")) > 0)
        loss_count = trade_count - win_count
        gross_total = sum(((record.gross_pnl or Decimal("0")) for record in records), Decimal("0"))
        fee_total = sum(
            (((record.entry_fee or Decimal("0")) + (record.exit_fee or Decimal("0"))) for record in records),
            Decimal("0"),
        )
        funding_total = sum(((record.funding_fee or Decimal("0")) for record in records), Decimal("0"))
        net_total = sum(((record.net_pnl or Decimal("0")) for record in records), Decimal("0"))
        win_rate = _format_ratio(Decimal(win_count) / Decimal(trade_count), places=0) if trade_count > 0 else "-"
        latest_history_record = max(
            (history_by_id.get(record.history_record_id) for record in records),
            key=lambda item: item.updated_at or item.stopped_at or item.started_at if item is not None else datetime.min,
            default=None,
        )
        row_id = "||".join(key)
        rows.append(
            (
                row_id,
                (
                    api_name,
                    trader_label,
                    strategy_name,
                    symbol,
                    bar,
                    direction_label,
                    _history_record_status_label(latest_history_record),
                    trade_count,
                    win_count,
                    loss_count,
                    win_rate,
                    _format_optional_usdt_precise(gross_total, places=2),
                    _format_optional_usdt_precise(fee_total, places=2),
                    _format_optional_usdt_precise(funding_total, places=2),
                    _format_optional_usdt_precise(net_total, places=2),
                    _format_history_datetime(max(record.closed_at for record in records)),
                ),
            )
        )
    return rows


def _build_normal_strategy_book_ledger_rows(
    ledger_records: list[StrategyTradeLedgerRecord],
    history_records: list[StrategyHistoryRecord],
    *,
    filters: NormalStrategyBookFilters | None = None,
) -> list[tuple[str, tuple[object, ...]]]:
    history_by_id = {record.record_id: record for record in history_records}
    rows: list[tuple[str, tuple[object, ...]]] = []
    for record in _normal_strategy_trade_ledger_records(ledger_records, history_records, filters=filters):
        history_record = history_by_id.get(record.history_record_id)
        rows.append(
            (
                record.record_id,
                (
                    _format_history_datetime(record.closed_at),
                    record.api_name or "-",
                    _history_record_trader_label(history_record),
                    record.strategy_name or "-",
                    record.symbol or "-",
                    _history_record_bar_label(history_record),
                    record.direction_label or "-",
                    _history_record_status_label(history_record),
                    record.session_id or "-",
                    _format_history_datetime(record.opened_at),
                    _format_optional_decimal(record.entry_price),
                    _format_optional_decimal(record.exit_price),
                    _format_optional_decimal(record.size),
                    _format_optional_usdt_precise(record.gross_pnl or Decimal("0"), places=2),
                    _format_optional_usdt_precise(
                        (record.entry_fee or Decimal("0")) + (record.exit_fee or Decimal("0")),
                        places=2,
                    ),
                    _format_optional_usdt_precise(record.funding_fee or Decimal("0"), places=2),
                    _format_optional_usdt_precise(record.net_pnl or Decimal("0"), places=2),
                    record.close_reason or "-",
                ),
            )
        )
    return rows


def _infer_session_runtime_status(message: str, current_status: str = "") -> str | None:
    text = str(message or "").strip()
    if not text:
        return None
    if (
        "OKX 读取异常，准备重试" in text
        or "OKX 读取异常，进入重试" in text
        or "OKX 读取失败" in text
    ):
        return current_status or "网络重试中"
    if (
        "开始监控 OKX 动态止损" in text
        or "开始本地止盈止损监控" in text
        or "交易员虚拟止损监控启动" in text
    ):
        return "持仓监控中"
    if (
        "挂单已成交" in text
        or "市价单成交" in text
        or "初始 OKX 止损已提交" in text
        or "动态止盈上移" in text
        or "OKX 动态止损已上移" in text
        or "OKX 动态止损候选价已过期" in text
        or "OKX 动态止损委托暂未出现在挂单列表" in text
        or "OKX 动态止损上移失败，稍后重试" in text
        or "交易员动态止盈保护价已上移" in text
        or "交易员虚拟止损已触发（不平仓）" in text
        or "交易员固定止盈已触发" in text
        or "交易员动态止盈保护价触发" in text
    ):
        return "持仓监控中"
    if (
        "准备挂单" in text
        or "准备市价单" in text
        or "挂单已提交到 OKX" in text
        or "订单已提交到 OKX" in text
        or "委托追踪" in text
        or "检测到挂单状态已变更" in text
        or "挂单部分成交" in text
        or "启动追单窗口内接管当前波段" in text
    ):
        return "开仓监控中"
    if "已提交启动请求" in text:
        return "启动中"
    if (
        "当前无信号" in text
        or
        "当前无法生成挂单" in text
        or "当前无法生成动态开仓价" in text
        or "当前无法生成本地开仓价" in text
        or "已收盘K线数量不足" in text
        or "启动默认不追老信号" in text
        or "启动追单窗口已过期，当前不追单" in text
        or "开仓次数已达上限" in text
        or "信号次数已达上限" in text
        or "本轮持仓已结束，继续监控下一次信号" in text
    ):
        return "等待信号"
    return current_status or None


def _extract_session_bar_time(message: str) -> datetime | None:
    match = SESSION_BAR_TIME_PATTERN.match(str(message or "").strip())
    if match is None:
        return None
    try:
        return datetime.strptime(match.group("ts"), "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def _extract_log_field(message: str, field_name: str) -> str | None:
    text = str(message or "")
    marker = f"{field_name}="
    start = text.find(marker)
    if start < 0:
        return None
    start += len(marker)
    end = text.find("|", start)
    if end < 0:
        end = len(text)
    value = text[start:end].strip()
    return value or None


def _extract_log_field_decimal(message: str, field_name: str) -> Decimal | None:
    raw = _extract_log_field(message, field_name)
    if raw is None:
        return None
    try:
        return Decimal(raw)
    except (InvalidOperation, ValueError):
        match = re.search(r"[-+]?\d+(?:\.\d+)?", raw)
        if match is None:
            return None
        try:
            return Decimal(match.group(0))
        except (InvalidOperation, ValueError):
            return None


def _trade_order_event_time(item: OkxTradeOrderItem) -> int:
    return item.update_time or item.created_time or 0


def _weighted_average_fill_price(fills: list[OkxFillHistoryItem]) -> Decimal | None:
    weighted_total = Decimal("0")
    size_total = Decimal("0")
    for item in fills:
        if item.fill_price is None or item.fill_size is None:
            continue
        weighted_total += item.fill_price * item.fill_size
        size_total += item.fill_size
    if size_total <= 0:
        return None
    return weighted_total / size_total


def _sum_fill_size(fills: list[OkxFillHistoryItem]) -> Decimal | None:
    total = Decimal("0")
    seen = False
    for item in fills:
        if item.fill_size is None:
            continue
        total += item.fill_size
        seen = True
    return total if seen else None


def _sum_fill_fee(fills: list[OkxFillHistoryItem]) -> Decimal | None:
    total = Decimal("0")
    seen = False
    for item in fills:
        if item.fill_fee is None:
            continue
        total += item.fill_fee
        seen = True
    return total if seen else None


def _sum_fill_pnl(fills: list[OkxFillHistoryItem]) -> Decimal | None:
    total = Decimal("0")
    seen = False
    for item in fills:
        if item.pnl is None:
            continue
        total += item.pnl
        seen = True
    return total if seen else None


_HISTORY_CACHE_FIELD_ALIASES: dict[str, tuple[str, ...]] = {
    "inst_id": ("inst_id", "instId"),
    "fill_time": ("fill_time", "fillTs", "fillTime", "ts"),
    "order_id": ("order_id", "ordId"),
    "trade_id": ("trade_id", "tradeId"),
    "created_time": ("created_time", "cTime"),
    "update_time": ("update_time", "uTime", "fillTime"),
    "source_kind": ("source_kind",),
    "client_order_id": ("client_order_id", "clOrdId"),
    "algo_id": ("algo_id", "algoId"),
    "algo_client_order_id": ("algo_client_order_id", "algoClOrdId"),
    "side": ("side",),
    "fill_size": ("fill_size", "fillSz", "sz"),
    "fill_price": ("fill_price", "fillPx", "px"),
    "pos_side": ("pos_side", "posSide"),
    "direction": ("direction",),
    "close_size": ("close_size", "closeSz", "sz"),
    "close_avg_price": ("close_avg_price", "closeAvgPx", "avgPx"),
}


def _history_cache_field_str(record: dict[str, object], field: str) -> str:
    if field == "source_kind":
        for key in ("source_kind",):
            value = record.get(key)
            if value is not None and str(value).strip() != "":
                return str(value)
        raw = record.get("raw") if isinstance(record.get("raw"), dict) else {}
        if record.get("algo_id") or record.get("algoId") or raw.get("algoId"):
            return "algo"
        return "normal"
    for key in _HISTORY_CACHE_FIELD_ALIASES.get(field, (field,)):
        value = record.get(key)
        if value is None or value == "":
            continue
        return str(value)
    return ""


def _history_cache_key(record: dict[str, object], fields: tuple[str, ...]) -> str:
    return "|".join(_history_cache_field_str(record, name) for name in fields)


def _record_coalesce_int(record: dict[str, object], *keys: str) -> int | None:
    for key in keys:
        parsed = _parse_int_or_none(record.get(key))
        if parsed is not None:
            return parsed
    return None


def _safe_build_history_instrument_map(client: OkxRestClient, inst_ids: list[str]) -> dict[str, Instrument]:
    try:
        return _build_history_instrument_map(client, inst_ids)
    except Exception:
        return {}


def _merge_history_cache_records(
    local_records: list[dict[str, object]],
    remote_records: list[dict[str, object]],
    dedup_fields: tuple[str, ...],
) -> list[dict[str, object]]:
    merged: dict[str, dict[str, object]] = {}
    for record in local_records:
        key = _history_cache_key(record, dedup_fields)
        if key:
            merged[key] = record
    for record in remote_records:
        key = _history_cache_key(record, dedup_fields)
        if key:
            merged[key] = record
    return list(merged.values())


def _serialize_history_item(item: object) -> dict[str, object]:
    payload: dict[str, object] = {}
    for key, value in asdict(item).items():
        if isinstance(value, Decimal):
            payload[key] = str(value)
            continue
        payload[key] = value
    return payload


def _parse_decimal_or_none(value: object) -> Decimal | None:
    if value in {None, ""}:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _parse_int_or_none(value: object) -> int | None:
    if value in {None, ""}:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _order_item_from_cache(record: dict[str, object]) -> OkxTradeOrderItem | None:
    inst_id = str(record.get("inst_id") or record.get("instId") or "").strip().upper()
    if not inst_id:
        return None
    created_time = _record_coalesce_int(record, "created_time", "cTime", "ts")
    update_time = _record_coalesce_int(record, "update_time", "uTime", "fillTime")
    if created_time is None and update_time is None:
        return None
    return OkxTradeOrderItem(
        source_kind=str(record.get("source_kind", "")),
        source_label=str(record.get("source_label", "")),
        created_time=created_time,
        update_time=update_time,
        inst_id=inst_id,
        inst_type=str(record.get("inst_type") or record.get("instType") or ""),
        side=str(record.get("side", "")) or None,
        pos_side=str(record.get("pos_side") or record.get("posSide") or "") or None,
        td_mode=str(record.get("td_mode") or record.get("tdMode") or "") or None,
        ord_type=str(record.get("ord_type") or record.get("ordType") or "") or None,
        state=str(record.get("state", "")) or None,
        price=_parse_decimal_or_none(record.get("price")) or _parse_decimal_or_none(record.get("px")),
        size=_parse_decimal_or_none(record.get("size")) or _parse_decimal_or_none(record.get("sz")),
        filled_size=_parse_decimal_or_none(record.get("filled_size")) or _parse_decimal_or_none(record.get("accFillSz")),
        avg_price=_parse_decimal_or_none(record.get("avg_price")) or _parse_decimal_or_none(record.get("avgPx")),
        order_id=str(record.get("order_id") or record.get("ordId") or "") or None,
        algo_id=str(record.get("algo_id") or record.get("algoId") or "") or None,
        client_order_id=str(record.get("client_order_id") or record.get("clOrdId") or "") or None,
        algo_client_order_id=str(record.get("algo_client_order_id") or record.get("algoClOrdId") or "") or None,
        pnl=_parse_decimal_or_none(record.get("pnl")),
        fee=_parse_decimal_or_none(record.get("fee")) or _parse_decimal_or_none(record.get("fillFee")),
        fee_currency=str(record.get("fee_currency") or record.get("feeCcy") or record.get("fillFeeCcy") or "") or None,
        reduce_only=bool(record.get("reduce_only")) if record.get("reduce_only") is not None else None,
        trigger_price=_parse_decimal_or_none(record.get("trigger_price")),
        trigger_price_type=str(record.get("trigger_price_type", "")) or None,
        order_price=_parse_decimal_or_none(record.get("order_price")),
        actual_price=_parse_decimal_or_none(record.get("actual_price")),
        actual_size=_parse_decimal_or_none(record.get("actual_size")),
        actual_side=str(record.get("actual_side", "")) or None,
        take_profit_trigger_price=_parse_decimal_or_none(record.get("take_profit_trigger_price")),
        take_profit_order_price=_parse_decimal_or_none(record.get("take_profit_order_price")),
        take_profit_trigger_price_type=str(record.get("take_profit_trigger_price_type", "")) or None,
        stop_loss_trigger_price=_parse_decimal_or_none(record.get("stop_loss_trigger_price")),
        stop_loss_order_price=_parse_decimal_or_none(record.get("stop_loss_order_price")),
        stop_loss_trigger_price_type=str(record.get("stop_loss_trigger_price_type", "")) or None,
        raw=record.get("raw") if isinstance(record.get("raw"), dict) else {},
    )


def _fill_item_from_cache(record: dict[str, object]) -> OkxFillHistoryItem | None:
    inst_id = str(record.get("inst_id") or record.get("instId") or "").strip()
    fill_time = _record_coalesce_int(record, "fill_time", "fillTs", "fillTime", "ts")
    if not inst_id or fill_time is None:
        return None
    return OkxFillHistoryItem(
        fill_time=fill_time,
        inst_id=inst_id,
        inst_type=str(record.get("inst_type") or record.get("instType") or ""),
        side=str(record.get("side", "")) or None,
        pos_side=str(record.get("pos_side") or record.get("posSide") or "") or None,
        fill_price=_parse_decimal_or_none(record.get("fill_price")) or _parse_decimal_or_none(record.get("fillPx")),
        fill_size=_parse_decimal_or_none(record.get("fill_size")) or _parse_decimal_or_none(record.get("fillSz")),
        fill_fee=_parse_decimal_or_none(record.get("fill_fee"))
        or _parse_decimal_or_none(record.get("fillFee"))
        or _parse_decimal_or_none(record.get("fee")),
        fee_currency=str(record.get("fee_currency") or record.get("feeCcy") or record.get("fillFeeCcy") or "") or None,
        pnl=_parse_decimal_or_none(record.get("pnl")),
        order_id=str(record.get("order_id") or record.get("ordId") or "") or None,
        trade_id=str(record.get("trade_id") or record.get("tradeId") or "") or None,
        exec_type=str(record.get("exec_type") or record.get("execType") or "") or None,
        raw=record.get("raw") if isinstance(record.get("raw"), dict) else {},
    )


def _position_history_item_from_cache(record: dict[str, object]) -> OkxPositionHistoryItem | None:
    inst_id = str(record.get("inst_id") or record.get("instId") or "").strip()
    update_time = _record_coalesce_int(record, "update_time", "uTime", "ts")
    if not inst_id or update_time is None:
        return None
    return OkxPositionHistoryItem(
        update_time=update_time,
        inst_id=inst_id,
        inst_type=str(record.get("inst_type", "")),
        mgn_mode=str(record.get("mgn_mode", "")) or None,
        pos_side=str(record.get("pos_side", "")) or None,
        direction=str(record.get("direction", "")) or None,
        open_avg_price=_parse_decimal_or_none(record.get("open_avg_price")),
        close_avg_price=_parse_decimal_or_none(record.get("close_avg_price")),
        close_size=_parse_decimal_or_none(record.get("close_size")),
        pnl=_parse_decimal_or_none(record.get("pnl")),
        realized_pnl=_parse_decimal_or_none(record.get("realized_pnl")),
        settle_pnl=_parse_decimal_or_none(record.get("settle_pnl")),
        raw=record.get("raw") if isinstance(record.get("raw"), dict) else {},
        fee=_parse_decimal_or_none(record.get("fee")) or _parse_decimal_or_none(record.get("fillFee")),
        fee_currency=str(record.get("fee_currency") or record.get("feeCcy") or record.get("ccy") or "") or None,
    )


class QuantApp(UiPositionsMixin, UiProtectionMixin, UiBacktestEntryMixin, UiStrategySessionsMixin):
    def __init__(self) -> None:
        self.root = Tk()
        apply_window_icon(self.root)
        self.root.title(build_app_title())
        screen_width = self.root.winfo_screenwidth()
        screen_height = self.root.winfo_screenheight()
        default_width = min(max(int(screen_width * 0.9), 1560), max(screen_width - 80, 1560))
        default_height = min(max(int(screen_height * 0.88), 980), max(screen_height - 80, 980))
        offset_x = max((screen_width - default_width) // 2, 20)
        offset_y = max((screen_height - default_height) // 2 - 12, 20)
        self.root.geometry(f"{default_width}x{default_height}+{offset_x}+{offset_y}")
        self.root.minsize(1420, 900)

        self.client = OkxRestClient()
        self.deribit_client = DeribitRestClient()
        self.log_queue: queue.Queue[str] = queue.Queue()
        self.instruments: list[Instrument] = []
        self._fixed_order_size_hint_instrument_cache: dict[str, Instrument] = {}
        self._fixed_order_size_hint_fetching_inst_ids: set[str] = set()
        self._minimum_order_risk_hint_after_id: str | None = None
        self._minimum_order_risk_hint_request_serial = 0
        self._minimum_order_risk_hint_active_request_serial = 0
        self.sessions: dict[str, StrategySession] = {}
        self._strategy_history_records: list[StrategyHistoryRecord] = []
        self._strategy_history_by_id: dict[str, StrategyHistoryRecord] = {}
        self._strategy_trade_ledger_records: list[StrategyTradeLedgerRecord] = []
        self._strategy_trade_ledger_by_id: dict[str, StrategyTradeLedgerRecord] = {}
        self._recoverable_strategy_sessions: dict[str, RecoverableStrategySessionRecord] = {}
        self._trader_desk_drafts: list[TraderDraftRecord] = []
        self._trader_desk_runs: list[TraderRunState] = []
        self._trader_desk_slots: list[TraderSlotRecord] = []
        self._trader_desk_events: list[TraderEventRecord] = []
        self._trader_gate_price_cache: dict[str, tuple[datetime, OkxTicker]] = {}
        self._strategy_log_write_failures: set[str] = set()
        self._session_counter = 0
        self._settings_window: Toplevel | None = None
        self._backtest_window: BacktestWindow | None = None
        self._backtest_compare_window: BacktestCompareOverviewWindow | None = None
        self._btc_market_analysis_window: BtcMarketAnalysisWindow | None = None
        self._btc_research_workbench_window: BtcResearchWorkbenchWindow | None = None
        self._signal_replay_mock_window: SignalReplayMockWindow | None = None
        self._journal_window: JournalWindow | None = None
        self._signal_monitor_window: SignalMonitorWindow | None = None
        self._trader_desk_window: TraderDeskWindow | None = None
        self._line_trading_desk_window: LineTradingDeskWindowState | None = None
        self._smart_order_window: SmartOrderWindow | None = None
        self._deribit_volatility_monitor_window: DeribitVolatilityMonitorWindow | None = None
        self._deribit_volatility_window: DeribitVolatilityWindow | None = None
        self._option_strategy_window: OptionStrategyCalculatorWindow | None = None
        self._option_roll_window: OptionRollSuggestionWindow | None = None
        self._positions_zoom_window: Toplevel | None = None
        self._strategy_history_window: Toplevel | None = None
        self._protection_window: Toplevel | None = None
        self._protection_replay_window: ProtectionReplayWindow | None = None
        self._positions_refreshing = False
        self._positions_history_refreshing = False
        self._positions_zoom_takeover_status_text = StringVar(
            value="动态止盈接管：当前无运行中任务；请在大窗底部「动态止盈接管」选项卡查看列表与说明。"
        )
        # session_id -> {thread, engine, summary, log_prefix, inst_id, algo_id, algo_cl}
        self._position_takeover_sessions: dict[str, dict[str, object]] = {}
        self._takeover_prefetch_request_id = 0
        self._takeover_prefetch_context: dict[str, object] | None = None
        self._takeover_open_flow_busy = False
        self._takeover_instrument_pending_slots: set[str] = set()
        self._positions_zoom_takeover_tree: object | None = None
        self._position_takeover_registry: list[dict[str, object]] = []
        self._takeover_last_running_session_id: str | None = None
        self._default_symbol_values = list(DEFAULT_LAUNCH_SYMBOLS)
        self._custom_trigger_symbol_values = ["", *self._default_symbol_values]
        self._default_launch_symbol = (
            "ETH-USDT-SWAP"
            if "ETH-USDT-SWAP" in self._default_symbol_values
            else (self._default_symbol_values[0] if self._default_symbol_values else "")
        )
        self._latest_positions: list[OkxPosition] = []
        self._latest_pending_orders: list[OkxTradeOrderItem] = []
        self._latest_order_history: list[OkxTradeOrderItem] = []
        self._latest_fill_history: list[OkxFillHistoryItem] = []
        self._latest_position_history: list[OkxPositionHistoryItem] = []
        self._position_current_notes: dict[str, dict[str, object]] = {}
        self._position_history_notes: dict[str, dict[str, object]] = {}
        self._positions_context_note: str | None = None
        self._positions_context_profile_name: str | None = None
        self._positions_last_refresh_at: datetime | None = None
        self._positions_history_last_refresh_at: datetime | None = None
        self._positions_effective_environment: str | None = None
        self._position_history_profile_name: str | None = None
        self._position_history_effective_environment: str | None = None
        self._positions_refresh_health = RefreshHealthState("持仓")
        self._pending_orders_refresh_health = RefreshHealthState("当前委托")
        self._order_history_refresh_health = RefreshHealthState("历史委托")
        self._account_info_refresh_health = RefreshHealthState("账户信息")
        self._upl_usdt_prices: dict[str, Decimal] = {}
        self._position_history_usdt_prices: dict[str, Decimal] = {}
        self._order_history_usdt_prices: dict[str, Decimal] = {}
        self._fill_history_usdt_prices: dict[str, Decimal] = {}
        self._position_instruments: dict[str, Instrument] = {}
        self._pending_order_instruments: dict[str, Instrument] = {}
        self._order_history_instruments: dict[str, Instrument] = {}
        self._fill_history_instruments: dict[str, Instrument] = {}
        self._position_history_instruments: dict[str, Instrument] = {}
        self._position_tickers: dict[str, OkxTicker] = {}
        self._position_row_payloads: dict[str, dict[str, object]] = {}
        self._positions_view_rendering = False
        self._selected_session_detail: Text | None = None
        self._position_detail_panel: Text | None = None
        self._positions_zoom_tree: ttk.Treeview | None = None
        self._positions_zoom_detail: Text | None = None
        self._positions_zoom_notebook: ttk.Notebook | None = None
        self._positions_zoom_pending_orders_tree: ttk.Treeview | None = None
        self._positions_zoom_pending_orders_detail: Text | None = None
        self._positions_zoom_order_history_tree: ttk.Treeview | None = None
        self._positions_zoom_order_history_detail: Text | None = None
        self._positions_zoom_fills_tree: ttk.Treeview | None = None
        self._positions_zoom_fills_detail: Text | None = None
        self._positions_zoom_position_history_tree: ttk.Treeview | None = None
        self._positions_zoom_position_history_detail: Text | None = None
        self._account_info_window: Toplevel | None = None
        self._account_info_tree: ttk.Treeview | None = None
        self._account_info_detail_panel: Text | None = None
        self._account_info_config_panel: Text | None = None
        self._account_info_pending_orders_tree: ttk.Treeview | None = None
        self._account_info_pending_orders_detail: Text | None = None
        self._account_info_order_history_tree: ttk.Treeview | None = None
        self._account_info_order_history_detail: Text | None = None
        self._account_info_refreshing = False
        self._latest_account_overview: OkxAccountOverview | None = None
        self._latest_account_config: OkxAccountConfig | None = None
        self._positions_zoom_column_window: Toplevel | None = None
        self._positions_zoom_credential_profile_combo: ttk.Combobox | None = None
        self._positions_zoom_detail_frame: ttk.LabelFrame | None = None
        self._positions_zoom_pending_orders_detail_frame: ttk.LabelFrame | None = None
        self._positions_zoom_order_history_detail_frame: ttk.LabelFrame | None = None
        self._positions_zoom_fills_detail_frame: ttk.LabelFrame | None = None
        self._positions_zoom_position_history_detail_frame: ttk.LabelFrame | None = None
        self._position_selection_syncing = False
        self._position_selection_suppressed_item_id: str | None = None
        self._positions_zoom_selection_suppressed_item_id: str | None = None
        self._positions_zoom_sync_job: str | None = None
        self._positions_zoom_selected_item_id: str | None = None
        self._positions_refresh_badges: list[Label] = []
        self._account_info_refresh_badges: list[Label] = []
        self._pending_orders_refresh_badges: list[Label] = []
        self._order_history_refresh_badges: list[Label] = []
        self._fills_history_refreshing = False
        self._position_history_refreshing = False
        self._pending_orders_refreshing = False
        self._pending_orders_refresh_queue: tuple[Credentials, str] | None = None
        self._pending_order_canceling = False
        self._order_history_refreshing = False
        self._pending_orders_last_refresh_at: datetime | None = None
        self._order_history_last_refresh_at: datetime | None = None
        self._fills_history_last_refresh_at: datetime | None = None
        self._fills_history_from_local_only = False
        self._fills_history_refresh_request: tuple[Credentials, str, str] | None = None
        self._order_history_refresh_request: tuple[Credentials, str, str] | None = None
        self._position_history_last_refresh_at: datetime | None = None
        self._positions_zoom_column_groups: dict[str, dict[str, object]] = {}
        self._positions_zoom_column_vars: dict[str, dict[str, BooleanVar]] = {}
        self._main_positions_pane: ttk.Panedwindow | None = None
        self._main_position_detail_frame: ttk.LabelFrame | None = None
        self._main_position_detail_collapsed = True
        self._main_position_detail_toggle_text = StringVar(value="展开持仓详情")
        self._positions_zoom_detail_collapsed = False
        self._positions_zoom_history_collapsed = False
        self._positions_zoom_pending_orders_detail_collapsed = False
        self._positions_zoom_order_history_detail_collapsed = False
        self._positions_zoom_fills_detail_collapsed = False
        self._positions_zoom_position_history_detail_collapsed = False
        self._positions_zoom_detail_toggle_text = StringVar(value="折叠持仓详情")
        self._positions_zoom_history_toggle_text = StringVar(value="折叠历史区域")
        self._positions_zoom_pending_orders_detail_toggle_text = StringVar(value="折叠委托详情")
        self._positions_zoom_order_history_detail_toggle_text = StringVar(value="折叠委托详情")
        self._positions_zoom_fills_detail_toggle_text = StringVar(value="折叠成交详情")
        self._positions_zoom_position_history_detail_toggle_text = StringVar(value="折叠仓位详情")
        self._positions_zoom_pending_orders_summary_text = StringVar(value="当前委托尚未读取。")
        self._positions_zoom_order_history_summary_text = StringVar(value="历史委托尚未读取。")
        self._positions_zoom_pending_orders_base_summary = "当前委托尚未读取。"
        self._positions_zoom_order_history_base_summary = "历史委托尚未读取。"
        self._positions_zoom_fills_summary_text = StringVar(value="历史成交尚未读取。")
        self._positions_zoom_fills_load_more_text = StringVar(value="增加100条")
        self._positions_zoom_position_history_summary_text = StringVar(value="历史仓位尚未读取。")
        self._positions_zoom_position_history_load_more_text = StringVar(value="增加100条")
        self._positions_zoom_position_history_base_summary = "历史仓位尚未读取。"
        self._fill_history_fetch_limit = 100
        self._fill_history_load_more_clicks = 0
        self._position_history_fetch_limit = 300
        self._position_history_load_more_clicks = 0
        self._positions_zoom_summary_text = StringVar(value="当前尚未获取持仓。")
        self._positions_zoom_option_search_hint_text = StringVar(
            value="\u9009\u4e2d\u671f\u6743\u540e\uff0c\u53ef\u4e00\u952e\u5e26\u5165\u5408\u7ea6\u6216\u5230\u671f\u524d\u7f00\u3002"
        )
        self._positions_zoom_fill_history_search_hint_text = StringVar(
            value="\u9009\u4e2d\u5386\u53f2\u671f\u6743\u6210\u4ea4\u540e\uff0c\u53ef\u4e00\u952e\u5e26\u5165\u5408\u7ea6\u6216\u5230\u671f\u524d\u7f00\u3002"
        )
        self._positions_zoom_position_history_search_hint_text = StringVar(
            value="\u9009\u4e2d\u5386\u53f2\u671f\u6743\u540e\uff0c\u53ef\u4e00\u952e\u5e26\u5165\u5408\u7ea6\u6216\u5230\u671f\u524d\u7f00\u3002"
        )
        self._positions_zoom_apply_contract_button: ttk.Button | None = None
        self._positions_zoom_apply_expiry_prefix_button: ttk.Button | None = None
        self._positions_zoom_fills_apply_contract_button: ttk.Button | None = None
        self._positions_zoom_fills_apply_expiry_prefix_button: ttk.Button | None = None
        self._positions_zoom_position_history_apply_contract_button: ttk.Button | None = None
        self._positions_zoom_position_history_apply_expiry_prefix_button: ttk.Button | None = None
        self._main_body_pane: ttk.Panedwindow | None = None
        self._sessions_pane: ttk.Panedwindow | None = None
        self._protection_sessions_tree: ttk.Treeview | None = None
        self._protection_detail_text: Text | None = None
        self._protection_form_title_text = StringVar(value="请选择一个期权持仓后，再设置保护。")
        self._protection_logic_hint_text = StringVar(value="请先选择一条期权持仓，系统会显示当前组合下的止盈止损方向。")
        self._protection_status_text = StringVar(value="当前没有运行中的期权持仓保护任务。")
        self._protection_selected_session_id: str | None = None
        self._protection_form_position_id: str | None = None
        self._protection_form_position_key: str | None = None
        self._protection_take_profit_order_price_entry: ttk.Entry | None = None
        self._protection_stop_loss_order_price_entry: ttk.Entry | None = None
        self._protection_take_profit_slippage_entry: ttk.Entry | None = None
        self._protection_stop_loss_slippage_entry: ttk.Entry | None = None
        self._protection_take_profit_fixed_price_memory = ""
        self._protection_stop_loss_fixed_price_memory = ""
        self._protection_order_mode_job: str | None = None

        self._protection_manager = PositionProtectionManager(
            self.client,
            self._make_system_logger("持仓保护"),
            notifier=None,
            on_change=self._schedule_protection_window_refresh,
        )

        self._strategy_name_to_id = {item.name: item.strategy_id for item in STRATEGY_DEFINITIONS}
        self.strategy_name = StringVar(value=STRATEGY_DEFINITIONS[0].name)

        self.api_key = StringVar()
        self.secret_key = StringVar()
        self.passphrase = StringVar()
        self.api_profile_name = StringVar(value=DEFAULT_CREDENTIAL_PROFILE_NAME)
        self.environment_label = StringVar(value="模拟盘 demo")

        self.symbol = StringVar(value=self._default_launch_symbol)
        self.trade_symbol = StringVar(value=self._default_launch_symbol)
        self.local_tp_sl_symbol = StringVar(value="")
        self.bar = StringVar(value="15m")
        self.ema_period = StringVar(value="21")
        self.trend_ema_period = StringVar(value="55")
        self.big_ema_period = StringVar(value="233")
        self.entry_reference_ema_period = StringVar(value="55")
        self.atr_period = StringVar(value="10")
        self.stop_atr = StringVar(value="2")
        self.take_atr = StringVar(value="4")
        self.risk_amount = StringVar(value="10")
        self.order_size = StringVar(value="1")
        self.fixed_order_size_hint_text = StringVar(
            value="固定数量=OKX下单数量(sz)，不是USDT；若填写风险金，则优先按风险金计算。"
        )
        self.minimum_order_risk_hint_text = StringVar(value="下单门槛：请先选择下单标的。")
        self.launch_parameter_hint_text = StringVar(value="")
        self.trend_parameter_hint_text = StringVar(value="")
        self.dynamic_protection_hint_text = StringVar(value="")
        self.poll_seconds = StringVar(value="10")
        self.signal_mode_label = StringVar(value=STRATEGY_DEFINITIONS[0].default_signal_label)
        self.take_profit_mode_label = StringVar(value="动态止盈")
        self.max_entries_per_trend = StringVar(value="1")
        self.startup_chase_window_seconds = StringVar(value="0")
        self.dynamic_two_r_break_even = BooleanVar(value=True)
        self.dynamic_fee_offset_enabled = BooleanVar(value=True)
        self.time_stop_break_even_enabled = BooleanVar(value=False)
        self.time_stop_break_even_bars = StringVar(value="10")
        self.run_mode_label = StringVar(value="交易并下单")
        self.trade_mode_label = StringVar(value="全仓 cross")
        self.position_mode_label = StringVar(value="净持仓 net")
        self.trigger_type_label = StringVar(value="标记价格 mark")
        self.tp_sl_mode_label = StringVar(value="OKX 托管（仅同标的永续）")
        self.entry_side_mode_label = StringVar(value="跟随信号")
        self.entry_side_mode_hint_text = StringVar(value="")
        self.symbol.trace_add("write", self._sync_trade_symbol_to_symbol)
        self.trade_symbol.trace_add("write", self._on_fixed_order_size_symbol_changed)
        self.trade_symbol.trace_add("write", self._schedule_minimum_order_risk_hint_update)
        self.symbol.trace_add("write", self._schedule_minimum_order_risk_hint_update)
        self.strategy_name.trace_add("write", self._schedule_minimum_order_risk_hint_update)
        self.bar.trace_add("write", self._schedule_minimum_order_risk_hint_update)
        self.signal_mode_label.trace_add("write", self._schedule_minimum_order_risk_hint_update)
        self.ema_period.trace_add("write", self._schedule_minimum_order_risk_hint_update)
        self.trend_ema_period.trace_add("write", self._schedule_minimum_order_risk_hint_update)
        self.big_ema_period.trace_add("write", self._schedule_minimum_order_risk_hint_update)
        self.entry_reference_ema_period.trace_add("write", self._schedule_minimum_order_risk_hint_update)
        self.atr_period.trace_add("write", self._schedule_minimum_order_risk_hint_update)
        self.stop_atr.trace_add("write", self._schedule_minimum_order_risk_hint_update)
        self.risk_amount.trace_add("write", self._update_fixed_order_size_hint)
        self.risk_amount.trace_add("write", self._schedule_minimum_order_risk_hint_update)
        self.order_size.trace_add("write", self._update_fixed_order_size_hint)
        self.stop_atr.trace_add("write", self._update_launch_parameter_hint)
        self.take_atr.trace_add("write", self._update_launch_parameter_hint)
        self.take_profit_mode_label.trace_add("write", self._update_launch_parameter_hint)
        self.take_profit_mode_label.trace_add("write", self._update_dynamic_protection_hint)
        self.max_entries_per_trend.trace_add("write", self._update_launch_parameter_hint)
        self.startup_chase_window_seconds.trace_add("write", self._update_launch_parameter_hint)
        self.ema_period.trace_add("write", self._update_trend_parameter_hint)
        self.trend_ema_period.trace_add("write", self._update_trend_parameter_hint)
        self.big_ema_period.trace_add("write", self._update_trend_parameter_hint)
        self.entry_reference_ema_period.trace_add("write", self._update_trend_parameter_hint)
        self.dynamic_two_r_break_even.trace_add("write", self._update_dynamic_protection_hint)
        self.dynamic_fee_offset_enabled.trace_add("write", self._update_dynamic_protection_hint)
        self.time_stop_break_even_enabled.trace_add("write", self._update_dynamic_protection_hint)
        self.time_stop_break_even_bars.trace_add("write", self._update_dynamic_protection_hint)
        self.time_stop_break_even_enabled.trace_add("write", lambda *_: self._sync_dynamic_take_profit_controls())
        self.run_mode_label.trace_add("write", lambda *_: self._sync_entry_side_mode_controls())
        self.run_mode_label.trace_add("write", self._schedule_minimum_order_risk_hint_update)
        self.tp_sl_mode_label.trace_add("write", lambda *_: self._sync_entry_side_mode_controls())
        self.tp_sl_mode_label.trace_add("write", self._schedule_minimum_order_risk_hint_update)

        self.notify_enabled = BooleanVar(value=False)
        self.smtp_host = StringVar()
        self.smtp_port = StringVar(value="465")
        self.smtp_username = StringVar()
        self.smtp_password = StringVar()
        self.sender_email = StringVar()
        self.recipient_emails = StringVar()
        self.use_ssl = BooleanVar(value=True)
        self.notify_trade_fills = BooleanVar(value=True)
        self.notify_signals = BooleanVar(value=True)
        self.notify_errors = BooleanVar(value=True)
        self.running_session_filter = StringVar(value="全部")
        self.positions_zoom_type_filter = StringVar(value="全部类型")
        self.positions_zoom_keyword = StringVar()
        self.pending_order_type_filter = StringVar(value="全部类型")
        self.pending_order_source_filter = StringVar(value="全部来源")
        self.pending_order_state_filter = StringVar(value="全部状态")
        self.pending_order_asset_filter = StringVar()
        self.pending_order_expiry_prefix_filter = StringVar()
        self.pending_order_keyword = StringVar()
        self.order_history_type_filter = StringVar(value="全部类型")
        self.order_history_source_filter = StringVar(value="全部来源")
        self.order_history_state_filter = StringVar(value="全部状态")
        self.order_history_asset_filter = StringVar()
        self.order_history_expiry_prefix_filter = StringVar()
        self.order_history_keyword = StringVar()
        self.fill_history_type_filter = StringVar(value="全部类型")
        self.fill_history_side_filter = StringVar(value="全部方向")
        self.fill_history_asset_filter = StringVar()
        self.fill_history_expiry_prefix_filter = StringVar()
        self.fill_history_keyword = StringVar()
        self.position_history_type_filter = StringVar(value="全部类型")
        self.position_history_margin_filter = StringVar(value="全部模式")
        self.position_history_asset_filter = StringVar()
        self.position_history_expiry_prefix_filter = StringVar()
        self.position_history_keyword = StringVar()
        self.position_history_range_start = StringVar(value="")
        self.position_history_range_end = StringVar(value="")
        self.position_refresh_interval_label = StringVar(value="15秒")
        self.position_auto_refresh_button_text = StringVar(value="暂停自动刷新")
        self.position_auto_refresh_enabled = True
        self.protection_trigger_source_label = StringVar(value="期权标记价格")
        self.protection_spot_symbol = StringVar()
        self.protection_take_profit_trigger = StringVar()
        self.protection_stop_loss_trigger = StringVar()
        self.protection_take_profit_order_mode_label = StringVar(value="设定价格")
        self.protection_take_profit_order_price = StringVar()
        self.protection_take_profit_slippage = StringVar(value="0")
        self.protection_stop_loss_order_mode_label = StringVar(value="设定价格")
        self.protection_stop_loss_order_price = StringVar()
        self.protection_stop_loss_slippage = StringVar(value="0")
        self.protection_poll_seconds = StringVar(value="2")

        self.status_text = StringVar(value="运行中策略：0")
        self.session_summary_text = StringVar(value="多策略合计：当前没有运行中的策略。")
        self.session_quick_actions_text = StringVar(
            value="快捷操作：会话=双击日志 | 交易员=双击打开管理台 | 邮件=双击切换 | 标的=双击K线"
        )
        self.global_email_toggle_text = StringVar(value="发邮件：开")
        self.settings_summary_text = StringVar()
        self.strategy_summary_text = StringVar()
        self.strategy_rule_text = StringVar()
        self.strategy_hint_text = StringVar()
        self.selected_session_text = StringVar(value=self._default_selected_session_text())
        self._selected_session_detail_session_id: str | None = None
        self.strategy_history_text = StringVar(value=self._default_strategy_history_text())
        self.strategy_history_api_filter = StringVar(value=STRATEGY_BOOK_FILTER_ALL_API)
        self.strategy_history_strategy_filter = StringVar(value=STRATEGY_BOOK_FILTER_ALL_STRATEGY)
        self.strategy_history_symbol_filter = StringVar(value=STRATEGY_BOOK_FILTER_ALL_SYMBOL)
        self.strategy_history_direction_filter = StringVar(value=STRATEGY_BOOK_FILTER_ALL_DIRECTION)
        self.strategy_history_mode_filter = StringVar(value=STRATEGY_HISTORY_FILTER_ALL_MODE)
        self.strategy_history_pnl_filter = StringVar(value=STRATEGY_HISTORY_FILTER_ALL_PNL)
        self.strategy_history_status_filter = StringVar(value=STRATEGY_BOOK_FILTER_ALL_STATUS)
        self.strategy_book_summary_text = StringVar(value="普通量化策略总账本尚未打开。")
        self.strategy_book_api_filter = StringVar(value=STRATEGY_BOOK_FILTER_ALL_API)
        self.strategy_book_trader_filter = StringVar(value=STRATEGY_BOOK_FILTER_ALL_TRADER)
        self.strategy_book_strategy_filter = StringVar(value=STRATEGY_BOOK_FILTER_ALL_STRATEGY)
        self.strategy_book_symbol_filter = StringVar(value=STRATEGY_BOOK_FILTER_ALL_SYMBOL)
        self.strategy_book_bar_filter = StringVar(value=STRATEGY_BOOK_FILTER_ALL_BAR)
        self.strategy_book_direction_filter = StringVar(value=STRATEGY_BOOK_FILTER_ALL_DIRECTION)
        self.strategy_book_status_filter = StringVar(value=STRATEGY_BOOK_FILTER_ALL_STATUS)
        self.positions_summary_text = StringVar(value="当前尚未获取持仓。")
        self._positions_refresh_badge_text = StringVar(value="未读")
        self.position_total_text = StringVar(value="-")
        self.position_upl_text = StringVar(value="-")
        self.position_realized_text = StringVar(value="-")
        self.position_margin_text = StringVar(value="-")
        self.position_delta_text = StringVar(value="-")
        self.position_short_call_text = StringVar(value="-")
        self.position_short_put_text = StringVar(value="-")
        self.position_long_call_text = StringVar(value="-")
        self.position_long_put_text = StringVar(value="-")
        self.position_detail_text = StringVar(value=self._default_position_detail_text())
        self.account_info_summary_text = StringVar(value="尚未读取账户信息。")
        self._account_info_refresh_badge_text = StringVar(value="未读")
        self._pending_orders_refresh_badge_text = StringVar(value="未读")
        self._order_history_refresh_badge_text = StringVar(value="未读")
        self.account_total_equity_text = StringVar(value="-")
        self.account_adjusted_equity_text = StringVar(value="-")
        self.account_available_equity_text = StringVar(value="-")
        self.account_upl_text = StringVar(value="-")
        self.account_imr_text = StringVar(value="-")
        self.account_mmr_text = StringVar(value="-")
        self._main_position_detail_toggle_text.set("\u5c55\u5f00\u6301\u4ed3\u8be6\u60c5")
        self._positions_zoom_detail_toggle_text.set("\u5c55\u5f00\u6301\u4ed3\u8be6\u60c5")
        self._positions_zoom_fills_detail_toggle_text.set("\u5c55\u5f00\u6210\u4ea4\u8be6\u60c5")
        self._positions_zoom_position_history_detail_toggle_text.set("\u5c55\u5f00\u4ed3\u4f4d\u8be6\u60c5")

        self._credential_watch_enabled = False
        self._credential_save_job: str | None = None
        self._last_saved_credentials: tuple[str, str, str, str, str] | None = None
        self._auto_save_notice_shown = False
        self._credential_profiles: dict[str, dict[str, str]] = {}
        self._header_credential_profile_combo: ttk.Combobox | None = None
        self._credential_profile_combo: ttk.Combobox | None = None
        self._loaded_credential_profile_name = DEFAULT_CREDENTIAL_PROFILE_NAME
        self._default_environment_label = self.environment_label.get()
        self._strategy_history_tree: ttk.Treeview | None = None
        self._strategy_history_detail: Text | None = None
        self._strategy_history_selected_record_id: str | None = None
        self._strategy_history_api_combo: ttk.Combobox | None = None
        self._strategy_history_strategy_combo: ttk.Combobox | None = None
        self._strategy_history_symbol_combo: ttk.Combobox | None = None
        self._strategy_history_direction_combo: ttk.Combobox | None = None
        self._strategy_history_mode_combo: ttk.Combobox | None = None
        self._strategy_history_pnl_combo: ttk.Combobox | None = None
        self._strategy_history_status_combo: ttk.Combobox | None = None
        self._strategy_history_sort_column = "started"
        self._strategy_history_sort_descending = True
        self._auto_channel_preview_window: Toplevel | None = None
        self._strategy_book_window: Toplevel | None = None
        self._strategy_live_chart_windows: dict[str, StrategyLiveChartWindowState] = {}
        self._strategy_book_group_tree: ttk.Treeview | None = None
        self._strategy_book_ledger_tree: ttk.Treeview | None = None
        self._strategy_book_api_combo: ttk.Combobox | None = None
        self._strategy_book_trader_combo: ttk.Combobox | None = None
        self._strategy_book_strategy_combo: ttk.Combobox | None = None
        self._strategy_book_symbol_combo: ttk.Combobox | None = None
        self._strategy_book_bar_combo: ttk.Combobox | None = None
        self._strategy_book_direction_combo: ttk.Combobox | None = None
        self._strategy_book_status_combo: ttk.Combobox | None = None
        self._session_tree_hover_tip_window: Toplevel | None = None
        self._session_tree_hover_tip_label: ttk.Label | None = None
        self._session_tree_hover_tip_column = ""
        self._history_tree_hover_tip_window: Toplevel | None = None
        self._history_tree_hover_tip_label: ttk.Label | None = None
        self._history_tree_hover_tip_column = ""
        self._strategy_book_tree_hover_tip_window: Toplevel | None = None
        self._strategy_book_tree_hover_tip_label: ttk.Label | None = None
        self._strategy_book_tree_hover_tip_column = ""
        self._positions_snapshot_by_profile: dict[str, ProfilePositionSnapshot] = {}
        self._session_live_pnl_cache: dict[str, tuple[Decimal | None, datetime | None]] = {}

        self._settings_watch_enabled = False
        self._settings_save_job: str | None = None
        self._last_saved_notification_state: tuple[object, ...] | None = None
        self._position_history_view_prefs_save_job: str | None = None
        self._last_saved_position_history_view_prefs: tuple[str, str] | None = None

        self._load_saved_credentials()
        self._load_saved_notification_settings()
        self._load_position_notes()
        self._load_position_history_view_prefs()
        self._line_trading_desk_annotation_entries: dict[str, dict[str, object]] = {}
        try:
            self._line_trading_desk_annotation_entries = load_line_trading_desk_annotations_entries()
        except Exception as exc:
            self._enqueue_log(f"读取划线台画线持久化失败：{exc}")
        self._load_recoverable_strategy_sessions_registry()
        self._load_strategy_history()
        self._load_strategy_trade_ledger()
        self._strategy_parameter_drafts = load_strategy_parameter_drafts()
        self._strategy_parameter_scope = "launcher"
        self._last_strategy_parameter_strategy_id: str | None = None
        self._load_trader_desk_snapshot()
        self._build_menu()
        self._build_layout()
        self._hydrate_recoverable_strategy_sessions()
        self._refresh_all_refresh_badges()
        self._apply_initial_detail_visibility()
        self._bind_auto_save()
        self._apply_selected_strategy_definition()
        self._update_settings_summary()
        self.root.after_idle(self._apply_initial_pane_layout)
        self.root.after(250, self._drain_log_queue)
        self.root.after(500, self._refresh_status)
        self.root.after(900, self._attempt_auto_restore_recoverable_sessions)
        self.root.after(1200, self._refresh_positions_periodic)
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

    @staticmethod
    def _strategy_uses_big_ema(strategy_id: str) -> bool:
        return strategy_uses_parameter(strategy_id, "big_ema_period")

    @staticmethod
    def _set_field_state(widget: object, *, editable: bool) -> None:
        if isinstance(widget, ttk.Combobox):
            widget.configure(state="readonly" if editable else "disabled")
            return
        try:
            widget.configure(state="normal" if editable else "readonly")
        except Exception:
            try:
                widget.configure(state="normal" if editable else "disabled")
            except Exception:
                pass

    def _strategy_parameter_scope_drafts(self) -> dict[str, object]:
        drafts = self._strategy_parameter_drafts.get(self._strategy_parameter_scope)
        if not isinstance(drafts, dict):
            drafts = {}
            self._strategy_parameter_drafts[self._strategy_parameter_scope] = drafts
        return drafts

    def _strategy_parameter_bindings(self) -> dict[str, object]:
        return {
            "bar": self.bar,
            "signal_mode": self.signal_mode_label,
            "ema_period": self.ema_period,
            "trend_ema_period": self.trend_ema_period,
            "big_ema_period": self.big_ema_period,
            "atr_period": self.atr_period,
            "atr_stop_multiplier": self.stop_atr,
            "atr_take_multiplier": self.take_atr,
            "entry_reference_ema_period": self.entry_reference_ema_period,
            "take_profit_mode": self.take_profit_mode_label,
            "max_entries_per_trend": self.max_entries_per_trend,
            "dynamic_two_r_break_even": self.dynamic_two_r_break_even,
            "dynamic_fee_offset_enabled": self.dynamic_fee_offset_enabled,
            "time_stop_break_even_enabled": self.time_stop_break_even_enabled,
            "time_stop_break_even_bars": self.time_stop_break_even_bars,
            "startup_chase_window_seconds": self.startup_chase_window_seconds,
        }

    def _capture_strategy_parameter_draft(self, strategy_id: str) -> dict[str, object]:
        values: dict[str, object] = {}
        bindings = self._strategy_parameter_bindings()
        for key in iter_strategy_parameter_keys(strategy_id):
            variable = bindings.get(key)
            if variable is None:
                continue
            values[key] = variable.get()
        return values

    def _save_strategy_parameter_draft(self, strategy_id: str | None = None) -> None:
        target_strategy_id = strategy_id or self._last_strategy_parameter_strategy_id
        if not target_strategy_id:
            return
        scope_drafts = self._strategy_parameter_scope_drafts()
        scope_drafts[target_strategy_id] = self._capture_strategy_parameter_draft(target_strategy_id)
        save_strategy_parameter_drafts(self._strategy_parameter_drafts)

    def _restore_strategy_parameter_draft(self, strategy_id: str) -> None:
        bindings = self._strategy_parameter_bindings()
        draft_payload = self._strategy_parameter_scope_drafts().get(strategy_id)
        draft = draft_payload if isinstance(draft_payload, dict) else {}
        definition = get_strategy_definition(strategy_id)
        for key in iter_strategy_parameter_keys(strategy_id):
            variable = bindings.get(key)
            if variable is None:
                continue
            if key in draft:
                variable.set(draft[key])
                continue
            default_value = strategy_parameter_default_value(key)
            if default_value is None:
                continue
            if key == "signal_mode":
                variable.set(_reverse_lookup_label(SIGNAL_LABEL_TO_VALUE, str(default_value), definition.default_signal_label))
            elif key == "take_profit_mode":
                variable.set(_reverse_lookup_label(TAKE_PROFIT_MODE_OPTIONS, str(default_value), self.take_profit_mode_label.get()))
            else:
                variable.set(default_value)
        for key in iter_strategy_parameter_keys(strategy_id):
            fixed_value = strategy_fixed_value(strategy_id, key)
            if fixed_value is None:
                continue
            variable = bindings.get(key)
            if variable is None:
                continue
            if key == "signal_mode":
                variable.set(_reverse_lookup_label(SIGNAL_LABEL_TO_VALUE, str(fixed_value), definition.default_signal_label))
            else:
                variable.set(fixed_value)

    def _resolve_strategy_parameter_value(self, strategy_id: str, key: str, current_value: object) -> object:
        fixed_value = strategy_fixed_value(strategy_id, key)
        if fixed_value is not None:
            return fixed_value
        return current_value

    def _apply_strategy_parameter_fixed_labels(self, strategy_id: str) -> None:
        fixed_suffix = "（本策略固定）"
        label_map = {
            "bar": (self._bar_label, "K线周期"),
            "signal_mode": (self._signal_label, "信号方向"),
            "ema_period": (self._ema_label, "EMA小周期"),
            "trend_ema_period": (self._trend_ema_label, "EMA中周期"),
            "big_ema_period": (self._big_ema_label, "EMA大周期"),
        }
        for key, (widget, base_text) in label_map.items():
            text = f"{base_text}{fixed_suffix}" if strategy_fixed_value(strategy_id, key) is not None else base_text
            widget.configure(text=text)

    def _build_menu(self) -> None:
        menu_bar = Menu(self.root)

        settings_menu = Menu(menu_bar, tearoff=False)
        settings_menu.add_command(label="API 与通知设置", command=self.open_settings_window)
        menu_bar.add_cascade(label="设置", menu=settings_menu)

        tools_menu = Menu(menu_bar, tearoff=False)
        tools_menu.add_command(label="打开无限下单", command=self.open_smart_order_window)
        tools_menu.add_command(label="打开回测窗口", command=self.open_backtest_window)
        tools_menu.add_command(label="打开回测对比总览", command=self.open_backtest_compare_window)
        tools_menu.add_command(label="打开BTC行情分析", command=self.open_btc_market_analysis_window)
        tools_menu.add_command(label="打开BTC研究工作台", command=self.open_btc_research_workbench_window)
        tools_menu.add_command(label="打开信号复盘实验室", command=self.open_signal_replay_mock_window)
        tools_menu.add_command(label="打开行情日记", command=self.open_journal_window)
        tools_menu.add_command(label="打开划线交易台", command=self.open_line_trading_desk_window)
        tools_menu.add_command(label="打开信号观察台", command=self.open_signal_monitor_window)
        tools_menu.add_command(label="打开自动通道预览", command=self.open_auto_channel_preview_window)
        tools_menu.add_command(label="打开交易员管理台", command=self.open_trader_desk_window)
        tools_menu.add_command(label="打开波动率监控", command=self.open_deribit_volatility_monitor_window)
        tools_menu.add_command(label="打开Deribit波动率指数", command=self.open_deribit_volatility_window)
        tools_menu.add_command(label="打开期权策略计算器", command=self.open_option_strategy_window)
        tools_menu.add_command(label="打开运行日志目录", command=self._open_run_logs_directory)
        menu_bar.add_cascade(label="工具", menu=tools_menu)

        system_menu = Menu(menu_bar, tearoff=False)
        system_menu.add_command(label=f"版本信息 (v{APP_VERSION})", command=self.show_version_info)
        system_menu.add_separator()
        system_menu.add_command(label="退出", command=self._on_close)
        menu_bar.add_cascade(label="系统", menu=system_menu)

        self.root.config(menu=menu_bar)

    def show_version_info(self) -> None:
        messagebox.showinfo("版本信息", build_version_info_text(), parent=self.root)

    def _on_strategy_launch_form_canvas_configure(self, event) -> None:
        canvas = self._strategy_launch_form_canvas
        if event.width > 1:
            canvas.itemconfigure(self._strategy_launch_form_window, width=event.width)

    def _on_strategy_launch_form_inner_configure(self, _event=None) -> None:
        canvas = self._strategy_launch_form_canvas
        bbox = canvas.bbox("all")
        if bbox:
            canvas.configure(scrollregion=bbox)

    def _on_strategy_launch_form_mousewheel(self, event) -> None:
        canvas = self._strategy_launch_form_canvas
        if getattr(event, "num", None) == 5:
            canvas.yview_scroll(1, "units")
            return "break"
        if getattr(event, "num", None) == 4:
            canvas.yview_scroll(-1, "units")
            return "break"
        if event.delta:
            canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
        return "break"

    def _bind_strategy_launch_form_mousewheel(self, _event=None) -> None:
        self._strategy_launch_form_canvas.bind_all("<MouseWheel>", self._on_strategy_launch_form_mousewheel)

    def _unbind_strategy_launch_form_mousewheel(self, _event=None) -> None:
        self._strategy_launch_form_canvas.unbind_all("<MouseWheel>")

    def _build_layout(self) -> None:
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(1, weight=4)
        self.root.rowconfigure(2, weight=1)

        header = ttk.Frame(self.root, padding=(16, 16, 16, 8))
        header.grid(row=0, column=0, sticky="ew")
        header.columnconfigure(0, weight=1)
        header.columnconfigure(1, weight=1)

        ttk.Label(
            header,
            text="OKX 多策略工作台",
            font=("Microsoft YaHei UI", 20, "bold"),
        ).grid(row=0, column=0, sticky="w")
        ttk.Label(
            header,
            textvariable=self.status_text,
            font=("Microsoft YaHei UI", 11, "bold"),
        ).grid(row=0, column=1, sticky="e")
        summary_row = ttk.Frame(header)
        summary_row.grid(row=1, column=1, sticky="e", pady=(6, 0))
        ttk.Label(summary_row, text="API").grid(row=0, column=0, sticky="e")
        self._header_credential_profile_combo = ttk.Combobox(
            summary_row,
            textvariable=self.api_profile_name,
            values=self._credential_profile_names(),
            state="readonly",
            width=10,
        )
        self._header_credential_profile_combo.grid(row=0, column=1, sticky="e", padx=(4, 8))
        self._header_credential_profile_combo.bind("<<ComboboxSelected>>", self._on_api_profile_selected)
        ttk.Label(
            summary_row,
            textvariable=self.settings_summary_text,
            justify="right",
            wraplength=540,
        ).grid(row=0, column=2, sticky="e")

        body = ttk.Panedwindow(self.root, orient="horizontal")
        body.grid(row=1, column=0, sticky="nsew", padx=16, pady=(0, 10))
        self._main_body_pane = body

        launcher_frame = ttk.Frame(body, padding=12)
        sessions_frame = ttk.Frame(body, padding=12)
        body.add(launcher_frame, weight=2)
        body.add(sessions_frame, weight=3)

        launcher_frame.columnconfigure(0, weight=1)
        launcher_frame.rowconfigure(0, weight=1)
        launcher_frame.rowconfigure(1, weight=0)
        sessions_frame.columnconfigure(0, weight=1)
        sessions_frame.rowconfigure(0, weight=1)

        _lp = (6, 0)
        _lp_tight = (4, 0)
        _ix = (0, 10)
        _hint_wrap = 520

        start_frame = ttk.LabelFrame(launcher_frame, text="策略启动", padding=8)
        start_frame.grid(row=0, column=0, sticky="nsew")
        start_frame.columnconfigure(0, weight=1)
        start_frame.rowconfigure(0, weight=1)
        start_frame.rowconfigure(1, weight=0)

        scroll_host = ttk.Frame(start_frame)
        scroll_host.grid(row=0, column=0, sticky="nsew")
        scroll_host.columnconfigure(0, weight=1)
        scroll_host.rowconfigure(0, weight=1)
        scroll_host.bind("<Enter>", self._bind_strategy_launch_form_mousewheel)
        scroll_host.bind("<Leave>", self._unbind_strategy_launch_form_mousewheel)

        launch_canvas = Canvas(scroll_host, highlightthickness=0, borderwidth=0)
        launch_vsb = ttk.Scrollbar(scroll_host, orient="vertical", command=launch_canvas.yview)
        launch_canvas.configure(yscrollcommand=launch_vsb.set)
        launch_canvas.grid(row=0, column=0, sticky="nsew")
        launch_vsb.grid(row=0, column=1, sticky="ns")

        launch_form = ttk.Frame(launch_canvas, padding=(0, 0, 4, 0))
        for column in range(4):
            launch_form.columnconfigure(column, weight=1)
        launch_window = launch_canvas.create_window((0, 0), window=launch_form, anchor="nw")
        self._strategy_launch_form_canvas = launch_canvas
        self._strategy_launch_form_window = launch_window
        launch_canvas.bind("<Configure>", self._on_strategy_launch_form_canvas_configure)
        launch_form.bind("<Configure>", self._on_strategy_launch_form_inner_configure)
        launch_form.bind("<Button-4>", self._on_strategy_launch_form_mousewheel)
        launch_form.bind("<Button-5>", self._on_strategy_launch_form_mousewheel)

        row = 0
        ttk.Label(launch_form, text="选择策略").grid(row=row, column=0, sticky="w")
        self.strategy_combo = ttk.Combobox(
            launch_form,
            textvariable=self.strategy_name,
            values=[item.name for item in STRATEGY_DEFINITIONS],
            state="readonly",
        )
        self.strategy_combo.grid(row=row, column=1, sticky="ew", padx=_ix)
        self.strategy_combo.bind("<<ComboboxSelected>>", self._on_strategy_selected)
        ttk.Label(launch_form, text="交易标的").grid(row=row, column=2, sticky="w")
        self.symbol_combo = ttk.Combobox(
            launch_form,
            textvariable=self.symbol,
            values=self._default_symbol_values,
            state="readonly",
        )
        self.symbol_combo.grid(row=row, column=3, sticky="ew")

        row += 1
        self._bar_label = ttk.Label(launch_form, text="K线周期")
        self._bar_label.grid(row=row, column=0, sticky="w", pady=_lp)
        self._bar_combo = ttk.Combobox(launch_form, textvariable=self.bar, values=BAR_OPTIONS, state="readonly")
        self._bar_combo.grid(row=row, column=1, sticky="ew", padx=_ix, pady=_lp)
        self._signal_label = ttk.Label(launch_form, text="信号方向")
        self._signal_label.grid(row=row, column=2, sticky="w", pady=_lp)
        self.signal_combo = ttk.Combobox(launch_form, textvariable=self.signal_mode_label, state="readonly")
        self.signal_combo.grid(row=row, column=3, sticky="ew", pady=_lp)

        row += 1
        self._take_profit_mode_label = ttk.Label(launch_form, text="止盈方式")
        self._take_profit_mode_label.grid(row=row, column=0, sticky="w", pady=_lp)
        self._take_profit_mode_combo = ttk.Combobox(
            launch_form,
            textvariable=self.take_profit_mode_label,
            values=list(TAKE_PROFIT_MODE_OPTIONS.keys()),
            state="readonly",
        )
        self._take_profit_mode_combo.grid(row=row, column=1, sticky="ew", padx=_ix, pady=_lp)
        self._take_profit_mode_combo.bind("<<ComboboxSelected>>", lambda *_: self._sync_dynamic_take_profit_controls())
        self._max_entries_per_trend_label = ttk.Label(launch_form, text="每波最多开仓次数")
        self._max_entries_per_trend_label.grid(row=row, column=2, sticky="w", pady=_lp)
        self._max_entries_per_trend_entry = ttk.Entry(launch_form, textvariable=self.max_entries_per_trend)
        self._max_entries_per_trend_entry.grid(row=row, column=3, sticky="ew", pady=_lp)

        row += 1
        self._startup_chase_window_label = ttk.Label(launch_form, text="启动追单窗口")
        self._startup_chase_window_label.grid(row=row, column=0, sticky="w", pady=_lp)
        self._startup_chase_window_entry = ttk.Entry(launch_form, textvariable=self.startup_chase_window_seconds)
        self._startup_chase_window_entry.grid(row=row, column=1, sticky="ew", padx=_ix, pady=_lp)
        self._startup_chase_window_hint_label = ttk.Label(
            launch_form,
            text="0=不追单；可填秒数(如300)或时长写法(如5m、2h30m、1天)。",
        )
        self._startup_chase_window_hint_label.grid(row=row, column=2, columnspan=2, sticky="w", pady=_lp)

        row += 1
        self._dynamic_two_r_break_even_check = ttk.Checkbutton(
            launch_form,
            text="启用2R保本（2R时先移到保本位）",
            variable=self.dynamic_two_r_break_even,
        )
        self._dynamic_two_r_break_even_check.grid(row=row, column=0, columnspan=4, sticky="w", pady=_lp)

        row += 1
        self._dynamic_fee_offset_check = ttk.Checkbutton(
            launch_form,
            text="启用手续费偏移（按2倍Taker手续费留缓冲）",
            variable=self.dynamic_fee_offset_enabled,
        )
        self._dynamic_fee_offset_check.grid(row=row, column=0, columnspan=4, sticky="w", pady=_lp_tight)

        row += 1
        self._dynamic_fee_offset_hint_label = ttk.Label(
            launch_form,
            text="提示：保本位是否叠加手续费偏移，由下方开关决定；大部分组合开启更优，默认建议开启。",
        )
        self._dynamic_fee_offset_hint_label.grid(row=row, column=0, columnspan=4, sticky="w", pady=(2, 0))

        row += 1
        self._time_stop_break_even_check = ttk.Checkbutton(
            launch_form,
            text="启用时间保本（持仓满指定K线且已达到净保本时，上移到保本位）",
            variable=self.time_stop_break_even_enabled,
        )
        self._time_stop_break_even_check.grid(row=row, column=0, columnspan=2, sticky="w", pady=_lp_tight)
        self._time_stop_break_even_bars_label = ttk.Label(launch_form, text="时间保本K线数")
        self._time_stop_break_even_bars_label.grid(row=row, column=2, sticky="e", pady=_lp_tight)
        self._time_stop_break_even_bars_entry = ttk.Entry(launch_form, textvariable=self.time_stop_break_even_bars)
        self._time_stop_break_even_bars_entry.grid(row=row, column=3, sticky="ew", pady=_lp_tight)

        row += 1
        ttk.Label(
            launch_form,
            textvariable=self.dynamic_protection_hint_text,
            justify="left",
            wraplength=_hint_wrap,
        ).grid(row=row, column=0, columnspan=4, sticky="w", pady=(2, 0))

        row += 1
        ttk.Label(launch_form, text="运行模式").grid(row=row, column=0, sticky="w", pady=_lp)
        ttk.Combobox(
            launch_form,
            textvariable=self.run_mode_label,
            values=list(RUN_MODE_OPTIONS.keys()),
            state="readonly",
        ).grid(row=row, column=1, sticky="ew", padx=_ix, pady=_lp)
        ttk.Label(launch_form, text="轮询秒数").grid(row=row, column=2, sticky="w", pady=_lp)
        ttk.Entry(launch_form, textvariable=self.poll_seconds).grid(row=row, column=3, sticky="ew", pady=_lp)

        row += 1
        self._ema_label = ttk.Label(launch_form, text="EMA小周期")
        self._ema_label.grid(row=row, column=0, sticky="w", pady=_lp)
        self._ema_entry = ttk.Entry(launch_form, textvariable=self.ema_period)
        self._ema_entry.grid(row=row, column=1, sticky="ew", padx=_ix, pady=_lp)
        self._trend_ema_label = ttk.Label(launch_form, text="EMA中周期")
        self._trend_ema_label.grid(row=row, column=2, sticky="w", pady=_lp)
        self._trend_ema_entry = ttk.Entry(launch_form, textvariable=self.trend_ema_period)
        self._trend_ema_entry.grid(row=row, column=3, sticky="ew", pady=_lp)

        row += 1
        self._big_ema_label = ttk.Label(launch_form, text="EMA大周期")
        self._big_ema_label.grid(row=row, column=0, sticky="w", pady=_lp)
        self._big_ema_entry = ttk.Entry(launch_form, textvariable=self.big_ema_period)
        self._big_ema_entry.grid(row=row, column=1, sticky="ew", padx=_ix, pady=_lp)
        self._atr_label = ttk.Label(launch_form, text="ATR 周期")
        self._atr_label.grid(row=row, column=2, sticky="w", pady=_lp)
        self._atr_entry = ttk.Entry(launch_form, textvariable=self.atr_period)
        self._atr_entry.grid(row=row, column=3, sticky="ew", pady=_lp)

        row += 1
        self._entry_reference_ema_label = ttk.Label(launch_form, text="参考EMA周期")
        self._entry_reference_ema_label.grid(row=row, column=0, sticky="w", pady=_lp)
        self._entry_reference_ema_entry = ttk.Entry(launch_form, textvariable=self.entry_reference_ema_period)
        self._entry_reference_ema_entry.grid(row=row, column=1, sticky="ew", padx=_ix, pady=_lp)

        row += 1
        ttk.Label(
            launch_form,
            textvariable=self.trend_parameter_hint_text,
            justify="left",
            wraplength=_hint_wrap,
        ).grid(row=row, column=0, columnspan=4, sticky="w", pady=(2, 0))

        row += 1
        self._stop_atr_label = ttk.Label(launch_form, text="止损 ATR 倍数")
        self._stop_atr_label.grid(row=row, column=0, sticky="w", pady=_lp)
        self._stop_atr_entry = ttk.Entry(launch_form, textvariable=self.stop_atr)
        self._stop_atr_entry.grid(row=row, column=1, sticky="ew", padx=_ix, pady=_lp)
        self._take_atr_label = ttk.Label(launch_form, text="止盈 ATR 倍数")
        self._take_atr_label.grid(row=row, column=2, sticky="w", pady=_lp)
        self._take_atr_entry = ttk.Entry(launch_form, textvariable=self.take_atr)
        self._take_atr_entry.grid(row=row, column=3, sticky="ew", pady=_lp)

        row += 1
        ttk.Label(
            launch_form,
            textvariable=self.launch_parameter_hint_text,
            justify="left",
            wraplength=_hint_wrap,
        ).grid(row=row, column=0, columnspan=4, sticky="w", pady=(2, 0))

        row += 1
        ttk.Label(launch_form, text="风险金").grid(row=row, column=0, sticky="w", pady=_lp)
        ttk.Entry(launch_form, textvariable=self.risk_amount).grid(
            row=row, column=1, sticky="ew", padx=_ix, pady=_lp
        )
        ttk.Label(launch_form, text="固定数量").grid(row=row, column=2, sticky="w", pady=_lp)
        ttk.Entry(launch_form, textvariable=self.order_size).grid(row=row, column=3, sticky="ew", pady=_lp)

        row += 1
        ttk.Label(
            launch_form,
            textvariable=self.fixed_order_size_hint_text,
            justify="left",
            wraplength=_hint_wrap,
        ).grid(row=row, column=0, columnspan=4, sticky="w", pady=(2, 0))

        row += 1
        ttk.Label(
            launch_form,
            textvariable=self.minimum_order_risk_hint_text,
            justify="left",
            wraplength=_hint_wrap,
        ).grid(row=row, column=0, columnspan=4, sticky="w", pady=(2, 0))

        row += 1
        ttk.Label(launch_form, text="下单方向模式").grid(row=row, column=0, sticky="w", pady=_lp)
        self._entry_side_mode_combo = ttk.Combobox(
            launch_form,
            textvariable=self.entry_side_mode_label,
            values=list(ENTRY_SIDE_MODE_OPTIONS.keys()),
            state="readonly",
        )
        self._entry_side_mode_combo.grid(row=row, column=1, sticky="ew", padx=_ix, pady=_lp)
        ttk.Label(launch_form, text="止盈止损模式").grid(row=row, column=2, sticky="w", pady=_lp)
        ttk.Combobox(
            launch_form,
            textvariable=self.tp_sl_mode_label,
            values=LAUNCHER_TP_SL_MODE_LABELS,
            state="readonly",
        ).grid(row=row, column=3, sticky="ew", pady=_lp)

        row += 1
        ttk.Label(
            launch_form,
            textvariable=self.entry_side_mode_hint_text,
            justify="left",
            wraplength=_hint_wrap,
        ).grid(row=row, column=0, columnspan=4, sticky="w", pady=(2, 0))

        row += 1
        ttk.Label(launch_form, text="自定义触发标的").grid(row=row, column=0, sticky="w", pady=_lp)
        self.local_tp_sl_symbol_combo = ttk.Combobox(
            launch_form,
            textvariable=self.local_tp_sl_symbol,
            values=self._custom_trigger_symbol_values,
            state="readonly",
        )
        self.local_tp_sl_symbol_combo.grid(
            row=row, column=1, sticky="ew", padx=_ix, pady=_lp
        )

        launch_footer = ttk.Frame(start_frame)
        launch_footer.grid(row=1, column=0, sticky="ew", pady=(6, 0))
        button_row = ttk.Frame(launch_footer)
        button_row.grid(row=0, column=0, sticky="w")
        ttk.Button(button_row, text="启动", command=self.start).grid(row=0, column=0, padx=(0, 8))
        ttk.Button(button_row, text="加载 OKX SWAP", command=self.load_symbols).grid(row=0, column=1, padx=(0, 8))
        ttk.Button(button_row, text="导出 1小时调试", command=self.debug_hourly_values).grid(row=0, column=2)
        ttk.Label(
            launch_footer,
            text="API、交易模式、持仓模式和邮件通知都已移动到菜单：设置 > API 与通知设置",
            wraplength=520,
            justify="left",
        ).grid(row=1, column=0, sticky="w", pady=(6, 0))

        strategy_info = ttk.LabelFrame(launcher_frame, text="策略说明", padding=12)
        strategy_info.grid(row=1, column=0, sticky="ew", pady=(12, 0))
        strategy_info.columnconfigure(0, weight=1)

        ttk.Label(strategy_info, text="策略简介", font=("Microsoft YaHei UI", 11, "bold")).grid(
            row=0, column=0, sticky="w"
        )
        ttk.Label(
            strategy_info,
            textvariable=self.strategy_summary_text,
            wraplength=820,
            justify="left",
        ).grid(row=1, column=0, sticky="w", pady=(6, 12))

        ttk.Label(strategy_info, text="规则说明", font=("Microsoft YaHei UI", 11, "bold")).grid(
            row=2, column=0, sticky="w"
        )
        ttk.Label(
            strategy_info,
            textvariable=self.strategy_rule_text,
            wraplength=820,
            justify="left",
        ).grid(row=3, column=0, sticky="w", pady=(6, 12))

        ttk.Label(strategy_info, text="参数提示", font=("Microsoft YaHei UI", 11, "bold")).grid(
            row=4, column=0, sticky="w"
        )
        ttk.Label(
            strategy_info,
            textvariable=self.strategy_hint_text,
            wraplength=820,
            justify="left",
        ).grid(row=5, column=0, sticky="w", pady=(6, 0))

        sessions_pane = ttk.Panedwindow(sessions_frame, orient="vertical")
        sessions_pane.grid(row=0, column=0, sticky="nsew")
        self._sessions_pane = sessions_pane

        session_top_frame = ttk.Frame(sessions_pane)
        session_top_frame.columnconfigure(0, weight=1)
        session_top_frame.rowconfigure(0, weight=4)
        session_top_frame.rowconfigure(1, weight=1)
        sessions_pane.add(session_top_frame, weight=2)

        running_frame = ttk.LabelFrame(session_top_frame, text="运行中策略", padding=12)
        running_frame.grid(row=0, column=0, sticky="nsew")
        running_frame.columnconfigure(0, weight=1)
        running_frame.rowconfigure(1, weight=1)

        running_header = ttk.Frame(running_frame)
        running_header.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 8))
        running_header.columnconfigure(0, weight=1)
        ttk.Label(running_header, textvariable=self.session_summary_text, justify="left").grid(
            row=0,
            column=0,
            sticky="w",
        )
        ttk.Label(
            running_header,
            textvariable=self.session_quick_actions_text,
            justify="left",
            foreground="#556070",
        ).grid(row=1, column=0, sticky="w", pady=(4, 0))
        ttk.Label(running_header, text="筛选").grid(row=0, column=1, sticky="e", padx=(12, 6))
        running_session_filter_combo = ttk.Combobox(
            running_header,
            textvariable=self.running_session_filter,
            values=RUNNING_SESSION_FILTER_OPTIONS,
            state="readonly",
            width=14,
        )
        running_session_filter_combo.grid(row=0, column=2, sticky="e")
        running_session_filter_combo.bind("<<ComboboxSelected>>", self._on_running_session_filter_changed)

        self.session_tree = ttk.Treeview(
            running_frame,
            columns=(
                "session",
                "trader",
                "email",
                "api",
                "source_type",
                "strategy",
                "mode",
                "symbol",
                "bar",
                "direction",
                "open_qty",
                "live_pnl",
                "pnl",
                "status",
                "started",
            ),
            show="headings",
            selectmode="browse",
        )
        self.session_tree.heading("session", text="会话(双击日志)")
        self.session_tree.heading("trader", text="交易员(双击打开)")
        self.session_tree.heading("email", text="邮件(双击切换)")
        self.session_tree.heading("api", text="API")
        self.session_tree.heading("source_type", text="来源类型")
        self.session_tree.heading("strategy", text="策略")
        self.session_tree.heading("mode", text="模式")
        self.session_tree.heading("symbol", text="标的(双击K线)")
        self.session_tree.heading("bar", text="周期")
        self.session_tree.heading("direction", text="方向")
        self.session_tree.heading("open_qty", text="开位数量")
        self.session_tree.heading("live_pnl", text="实时浮盈亏")
        self.session_tree.heading("pnl", text="净盈亏")
        self.session_tree.heading("status", text="状态")
        self.session_tree.heading("started", text="启动时间")
        self.session_tree.column("session", width=56, anchor="center")
        self.session_tree.column("trader", width=72, anchor="center")
        self.session_tree.column("email", width=56, anchor="center")
        self.session_tree.column("api", width=80, anchor="center")
        self.session_tree.column("source_type", width=98, anchor="center")
        self.session_tree.column("strategy", width=120, anchor="w")
        self.session_tree.column("mode", width=96, anchor="center")
        self.session_tree.column("symbol", width=156, anchor="w")
        self.session_tree.column("bar", width=68, anchor="center")
        self.session_tree.column("direction", width=82, anchor="center")
        self.session_tree.column("open_qty", width=112, anchor="e")
        self.session_tree.column("live_pnl", width=112, anchor="e")
        self.session_tree.column("pnl", width=104, anchor="e")
        self.session_tree.column("status", width=108, anchor="center")
        self.session_tree.column("started", width=96, anchor="center")
        self.session_tree.grid(row=1, column=0, sticky="nsew")
        self.session_tree.bind("<<TreeviewSelect>>", self._on_session_selected)
        self.session_tree.bind("<Double-1>", self._on_session_tree_double_click)
        self.session_tree.bind("<Motion>", self._on_session_tree_hover)
        self.session_tree.bind("<Leave>", self._on_session_tree_hover_leave)
        self.session_tree.tag_configure("duplicate_conflict", background="#fff4e5", foreground="#a85a00")

        tree_scroll = ttk.Scrollbar(running_frame, orient="vertical", command=self.session_tree.yview)
        tree_scroll.grid(row=1, column=1, sticky="ns")
        self.session_tree.configure(yscrollcommand=tree_scroll.set)

        control_row = ttk.Frame(running_frame)
        control_row.grid(row=2, column=0, columnspan=2, sticky="w", pady=(10, 0))
        ttk.Button(control_row, text="\u505c\u6b62\u9009\u4e2d\u7b56\u7565", command=self.stop_selected_session).grid(row=0, column=0)
        ttk.Button(control_row, textvariable=self.global_email_toggle_text, command=self.toggle_global_email_notifications).grid(
            row=0, column=1, padx=(8, 0)
        )
        ttk.Button(control_row, text="\u5f00\u542f\u90ae\u4ef6", command=self.enable_selected_session_email_notifications).grid(
            row=0,
            column=2,
            padx=(8, 0),
        )
        ttk.Button(control_row, text="\u5173\u95ed\u90ae\u4ef6", command=self.disable_selected_session_email_notifications).grid(
            row=0,
            column=3,
            padx=(8, 0),
        )
        ttk.Button(control_row, text="\u5b9e\u65f6K\u7ebf\u56fe", command=self.open_selected_strategy_live_chart).grid(
            row=0, column=4, padx=(8, 0)
        )
        ttk.Button(control_row, text="BTC行情分析", command=self.open_btc_market_analysis_window).grid(
            row=0, column=5, padx=(8, 0)
        )
        ttk.Button(control_row, text="\u4fe1\u53f7\u89c2\u5bdf\u53f0", command=self.open_signal_monitor_window).grid(
            row=0, column=6, padx=(8, 0)
        )
        ttk.Button(control_row, text="\u4ea4\u6613\u5458\u7ba1\u7406\u53f0", command=self.open_trader_desk_window).grid(
            row=0, column=7, padx=(8, 0)
        )
        ttk.Button(control_row, text="\u6e05\u7a7a\u5df2\u505c\u6b62", command=self.clear_stopped_sessions).grid(
            row=0, column=8, padx=(8, 0)
        )
        ttk.Button(control_row, text="\u5386\u53f2\u7b56\u7565", command=self.open_strategy_history_window).grid(
            row=0, column=9, padx=(8, 0)
        )
        ttk.Button(control_row, text="\u7b56\u7565\u603b\u8d26\u672c", command=self.open_strategy_book_window).grid(
            row=0, column=10, padx=(8, 0)
        )
        ttk.Button(control_row, text="\u5bfc\u51fa\u9009\u4e2d\u53c2\u6570", command=self.export_selected_session_template).grid(
            row=0, column=11, padx=(8, 0)
        )
        ttk.Button(control_row, text="\u5bfc\u5165\u7b56\u7565\u53c2\u6570", command=self.import_strategy_template).grid(
            row=0, column=12, padx=(8, 0)
        )
        ttk.Button(control_row, text="恢复选中策略", command=self.recover_selected_session).grid(
            row=0, column=13, padx=(8, 0)
        )

        detail_frame = ttk.LabelFrame(session_top_frame, text="选中策略详情", padding=16)
        detail_frame.grid(row=1, column=0, sticky="nsew", pady=(12, 0))
        detail_frame.columnconfigure(0, weight=1)
        detail_frame.rowconfigure(0, weight=1)
        self._selected_session_detail = Text(
            detail_frame,
            height=9,
            wrap="word",
            font=("Microsoft YaHei UI", 10),
            relief="flat",
        )
        self._selected_session_detail.grid(row=0, column=0, sticky="nsew")
        detail_scroll = ttk.Scrollbar(detail_frame, orient="vertical", command=self._selected_session_detail.yview)
        detail_scroll.grid(row=0, column=1, sticky="ns")
        self._selected_session_detail.configure(yscrollcommand=detail_scroll.set)
        self._set_readonly_text(self._selected_session_detail, self.selected_session_text.get())

        positions_frame = ttk.LabelFrame(sessions_pane, text="账户持仓（仿 OKX 客户端风格）", padding=(6, 5))
        positions_frame.columnconfigure(0, weight=1)
        positions_frame.rowconfigure(2, weight=1)
        sessions_pane.add(positions_frame, weight=7)

        header_row = ttk.Frame(positions_frame)
        header_row.grid(row=0, column=0, sticky="ew", pady=(0, 4))
        header_row.columnconfigure(2, weight=1)
        positions_badge = self._create_refresh_badge(
            header_row,
            self._positions_refresh_badge_text,
            self._positions_refresh_badges,
        )
        positions_badge.grid(row=0, column=0, sticky="w", padx=(0, 6))
        filter_compact = ttk.Frame(header_row)
        ttk.Label(filter_compact, text="类型").pack(side="left")
        position_type_combo = ttk.Combobox(
            filter_compact,
            textvariable=self.positions_zoom_type_filter,
            values=list(POSITION_TYPE_OPTIONS.keys()),
            state="readonly",
            width=11,
        )
        position_type_combo.pack(side="left", padx=(4, 0))
        position_type_combo.bind("<<ComboboxSelected>>", self._on_position_filter_changed)
        ttk.Label(filter_compact, text="搜索").pack(side="left", padx=(10, 0))
        position_keyword_entry = ttk.Entry(filter_compact, textvariable=self.positions_zoom_keyword, width=24)
        position_keyword_entry.pack(side="left", padx=(4, 0))
        position_keyword_entry.bind("<KeyRelease>", self._on_position_filter_changed)
        filter_compact.grid(row=0, column=1, sticky="w")
        ttk.Label(
            header_row,
            textvariable=self.positions_summary_text,
            font=("Microsoft YaHei UI", 9),
        ).grid(row=0, column=2, sticky="ew", padx=(6, 6))
        action_row = ttk.Frame(header_row)
        action_row.grid(row=0, column=3, sticky="e")
        ttk.Button(action_row, text="刷新", command=self.refresh_positions).grid(row=0, column=0, padx=(0, 4))
        ttk.Button(action_row, text="持仓大窗", command=self.open_positions_zoom_window).grid(row=0, column=1, padx=(0, 4))
        ttk.Button(action_row, text="复制合约", command=self.copy_selected_position_symbol).grid(row=0, column=2)

        overview_row = ttk.Frame(positions_frame)
        overview_row.grid(row=1, column=0, sticky="ew", pady=(0, 4))
        for column in range(9):
            overview_row.columnconfigure(column, weight=1)
        self._build_metric_card(overview_row, 0, "持仓笔数", self.position_total_text, compact=True)
        self._build_metric_card(overview_row, 1, "浮动盈亏(USDT)", self.position_upl_text, compact=True)
        self._build_metric_card(overview_row, 2, "已实现盈亏", self.position_realized_text, compact=True)
        self._build_metric_card(overview_row, 3, "初始保证金(IMR)", self.position_margin_text, compact=True)
        self._build_metric_card(overview_row, 4, "Delta 合计", self.position_delta_text, compact=True)
        self._build_metric_card(overview_row, 5, "买购数量", self.position_long_call_text, compact=True)
        self._build_metric_card(overview_row, 6, "卖购数量", self.position_short_call_text, compact=True)
        self._build_metric_card(overview_row, 7, "买沽数量", self.position_long_put_text, compact=True)
        self._build_metric_card(overview_row, 8, "卖沽数量", self.position_short_put_text, compact=True)

        position_table = ttk.Frame(positions_frame)
        position_table.grid(row=2, column=0, sticky="nsew")
        position_table.columnconfigure(0, weight=1)
        position_table.rowconfigure(0, weight=1)

        self.position_tree = ttk.Treeview(
            position_table,
            columns=(
                "inst_type",
                "mgn_mode",
                "time_value",
                "time_value_usdt",
                "intrinsic_value",
                "intrinsic_usdt",
                "bid_price",
                "bid_usdt",
                "ask_price",
                "ask_usdt",
                "mark",
                "mark_usdt",
                "avg",
                "avg_usdt",
                "open_value_usdt",
                "pos",
                "option_side",
                "upl",
                "upl_usdt",
                "realized",
                "realized_usdt",
                "market_value",
                "liq",
                "mgn_ratio",
                "imr",
                "mmr",
                "delta",
                "gamma",
                "vega",
                "theta",
                "theta_usdt",
                "note",
            ),
            show="tree headings",
            selectmode="browse",
        )
        self.position_tree.heading("#0", text="合约 / 分组")
        self.position_tree.heading("inst_type", text="类型")
        self.position_tree.heading("mgn_mode", text="保证金模式")
        self.position_tree.heading("time_value", text="时间价值")
        self.position_tree.heading("time_value_usdt", text="时间≈USDT")
        self.position_tree.heading("intrinsic_value", text="内在价值")
        self.position_tree.heading("intrinsic_usdt", text="内在≈USDT")
        self.position_tree.heading("bid_price", text="买一价")
        self.position_tree.heading("bid_usdt", text="买一≈USDT")
        self.position_tree.heading("ask_price", text="卖一价")
        self.position_tree.heading("ask_usdt", text="卖一≈USDT")
        self.position_tree.heading("mark", text="标记价")
        self.position_tree.heading("mark_usdt", text="标记≈USDT")
        self.position_tree.heading("avg", text="开仓价")
        self.position_tree.heading("avg_usdt", text="开仓≈USDT")
        self.position_tree.heading("open_value_usdt", text="开仓价值≈USDT")
        self.position_tree.heading("pos", text="持仓量")
        self.position_tree.heading("option_side", text="买购:卖购 | 买沽:卖沽")
        self.position_tree.heading("upl", text="浮盈亏")
        self.position_tree.heading("upl_usdt", text="浮盈≈USDT")
        self.position_tree.heading("realized", text="已实现盈亏")
        self.position_tree.heading("realized_usdt", text="已实现≈USDT")
        self.position_tree.heading("market_value", text="市值")
        self.position_tree.heading("liq", text="强平价")
        self.position_tree.heading("mgn_ratio", text="保证金率")
        self.position_tree.heading("imr", text="初始保证金")
        self.position_tree.heading("mmr", text="维持保证金")
        self.position_tree.heading("delta", text="Delta(PA)")
        self.position_tree.heading("gamma", text="Gamma(PA)")
        self.position_tree.heading("vega", text="Vega(PA)")
        self.position_tree.heading("theta", text="Theta(PA)")
        self.position_tree.heading("theta_usdt", text="Theta≈USDT")
        self.position_tree.heading("note", text="备注")
        self.position_tree.column("#0", width=240, anchor="w", stretch=True)
        self.position_tree.column("inst_type", width=72, anchor="center")
        self.position_tree.column("mgn_mode", width=92, anchor="center")
        self.position_tree.column("time_value", width=88, anchor="e")
        self.position_tree.column("time_value_usdt", width=44, anchor="e")
        self.position_tree.column("intrinsic_value", width=88, anchor="e")
        self.position_tree.column("intrinsic_usdt", width=44, anchor="e")
        self.position_tree.column("bid_price", width=78, anchor="e")
        self.position_tree.column("bid_usdt", width=50, anchor="e")
        self.position_tree.column("ask_price", width=78, anchor="e")
        self.position_tree.column("ask_usdt", width=50, anchor="e")
        self.position_tree.column("mark", width=108, anchor="e")
        self.position_tree.column("mark_usdt", width=54, anchor="e")
        self.position_tree.column("avg", width=108, anchor="e")
        self.position_tree.column("avg_usdt", width=54, anchor="e")
        self.position_tree.column("open_value_usdt", width=96, anchor="e")
        self.position_tree.column("pos", width=110, anchor="e")
        self.position_tree.column("option_side", width=170, anchor="center")
        self.position_tree.column("upl", width=210, anchor="e")
        self.position_tree.column("upl_usdt", width=105, anchor="e")
        self.position_tree.column("realized", width=118, anchor="e")
        self.position_tree.column("realized_usdt", width=105, anchor="e")
        self.position_tree.column("market_value", width=160, anchor="e")
        self.position_tree.column("liq", width=92, anchor="e")
        self.position_tree.column("mgn_ratio", width=88, anchor="e")
        self.position_tree.column("imr", width=100, anchor="e")
        self.position_tree.column("mmr", width=100, anchor="e")
        self.position_tree.column("delta", width=82, anchor="e")
        self.position_tree.column("gamma", width=82, anchor="e")
        self.position_tree.column("vega", width=82, anchor="e")
        self.position_tree.column("theta", width=108, anchor="e")
        self.position_tree.column("theta_usdt", width=54, anchor="e")
        self.position_tree.column("note", width=180, anchor="w")
        self.position_tree.configure(
            displaycolumns=(
                "inst_type",
                "mgn_mode",
                "mark",
                "mark_usdt",
                "avg",
                "avg_usdt",
                "open_value_usdt",
                "pos",
                "option_side",
                "upl",
                "upl_usdt",
                "realized",
                "market_value",
                "liq",
                "mgn_ratio",
                "imr",
                "mmr",
                "delta",
                "gamma",
                "vega",
                "theta",
                "theta_usdt",
                "note",
            )
        )
        self.position_tree.grid(row=0, column=0, sticky="nsew")
        self.position_tree.bind("<<TreeviewSelect>>", self._on_position_selected)
        self.position_tree.tag_configure("profit", foreground="#13803d")
        self.position_tree.tag_configure("loss", foreground="#c23b3b")
        self.position_tree.tag_configure("group", foreground="#2f3a4a")
        self.position_tree.tag_configure("isolated_mode", background="#fff4e5")
        self.position_tree.tag_configure("cross_mode", background="#f4f8ff")

        position_scroll_y = ttk.Scrollbar(position_table, orient="vertical", command=self.position_tree.yview)
        position_scroll_y.grid(row=0, column=1, sticky="ns")
        position_scroll_x = ttk.Scrollbar(position_table, orient="horizontal", command=self.position_tree.xview)
        position_scroll_x.grid(row=1, column=0, sticky="ew")
        self.position_tree.configure(yscrollcommand=position_scroll_y.set, xscrollcommand=position_scroll_x.set)

        self._main_positions_pane = None
        self._main_position_detail_frame = None

        log_frame = ttk.LabelFrame(self.root, text="运行日志", padding=12)
        log_frame.grid(row=2, column=0, sticky="nsew", padx=16, pady=(0, 16))
        log_frame.columnconfigure(0, weight=1)
        log_frame.rowconfigure(0, weight=1)

        self.log_text = Text(log_frame, height=18, wrap="word", font=("Consolas", 10))
        self.log_text.grid(row=0, column=0, sticky="nsew")
        scrollbar = ttk.Scrollbar(log_frame, orient="vertical", command=self.log_text.yview)
        scrollbar.grid(row=0, column=1, sticky="ns")
        self.log_text.configure(yscrollcommand=scrollbar.set)
        self._hydrate_run_log_widget_from_disk()
        try:
            self._enqueue_log(f"运行日志已按日保存至：{daily_log_file_path().resolve()}")
        except Exception:
            self._enqueue_log(f"运行日志已按日保存至：{daily_log_file_path()}")

    def _hydrate_run_log_widget_from_disk(self) -> None:
        """Load the tail of today's log file into the widget; merge with any lines already queued (see append_log_line)."""
        pending: list[str] = []
        while True:
            try:
                pending.append(self.log_queue.get_nowait())
            except queue.Empty:
                break
        lines = read_daily_log_tail(500)
        if pending:
            n = len(pending)
            if len(lines) >= n and lines[-n:] == pending:
                show = lines
            else:
                show = lines + pending
        else:
            show = lines
        if show:
            self.log_text.insert(END, "\n".join(show) + "\n")
            self.log_text.see(END)

    def _open_run_logs_directory(self) -> None:
        target = logs_dir()
        try:
            target.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            messagebox.showerror("打开日志目录", f"无法创建日志目录：{exc}", parent=self.root)
            return
        try:
            if sys.platform == "win32":
                os.startfile(target)  # type: ignore[attr-defined]
            elif sys.platform == "darwin":
                subprocess.run(["open", str(target)], check=False)
            else:
                subprocess.run(["xdg-open", str(target)], check=False)
        except Exception as exc:
            messagebox.showerror("打开日志目录", str(exc), parent=self.root)

    def _apply_initial_pane_layout(self) -> None:
        try:
            if self._main_body_pane is not None and self._main_body_pane.winfo_exists():
                total_width = self._main_body_pane.winfo_width()
                if total_width > 1200:
                    self._main_body_pane.sashpos(0, int(total_width * Decimal("0.39")))
            if self._sessions_pane is not None and self._sessions_pane.winfo_exists():
                total_height = self._sessions_pane.winfo_height()
                if total_height > 600:
                    self._sessions_pane.sashpos(0, int(total_height * Decimal("0.15")))
        except Exception:
            return

    def _apply_initial_detail_visibility(self) -> None:
        return

    def toggle_main_position_detail(self) -> None:
        if self._main_positions_pane is None or self._main_position_detail_frame is None:
            return
        try:
            panes = tuple(str(pane) for pane in self._main_positions_pane.panes())
            frame_id = str(self._main_position_detail_frame)
            if self._main_position_detail_collapsed:
                if frame_id not in panes:
                    self._main_positions_pane.add(self._main_position_detail_frame, weight=2)
                self._main_position_detail_toggle_text.set("\u6298\u53e0\u6301\u4ed3\u8be6\u60c5")
            else:
                if frame_id in panes:
                    self._main_positions_pane.forget(self._main_position_detail_frame)
                self._main_position_detail_toggle_text.set("\u5c55\u5f00\u6301\u4ed3\u8be6\u60c5")
            self._main_position_detail_collapsed = not self._main_position_detail_collapsed
        except Exception:
            return

    def _default_selected_session_text(self) -> str:
        return (
            "启动后，这里会显示选中策略的完整详情。\n"
            "左侧选择策略并点击“启动”后，右侧列表会出现会话；选中某个会话，就能在这里查看规则、参数和运行状态。"
        )

    def _default_strategy_history_text(self) -> str:
        return (
            "这里会显示历史策略记录。\n"
            "每次启动、停止、异常结束，都会同步写入本地策略历史文件，方便后续溯源。"
        )

    def _default_position_detail_text(self) -> str:
        return (
            "这里会显示选中持仓或风险分组的详细信息。\n"
            "你可以先刷新持仓，再用上面的类型筛选、搜索、展开/折叠，按 OKX 客户端那种方式查看账户结构。"
        )

    def _default_account_info_detail_text(self) -> str:
        return (
            "这里会显示账户概览、账户配置和选中资产详情。\n"
            "点击“账户信息”后，程序会读取 OKX 账户余额与账户配置接口；下方标签页也可以继续查看当前委托和历史委托。"
        )

    def _set_readonly_text(self, widget: Text | None, content: str, *, preserve_scroll: bool = False) -> None:
        if widget is None or not _widget_exists(widget):
            return
        try:
            yview = widget.yview() if preserve_scroll else None
            widget.configure(state="normal")
            widget.delete("1.0", END)
            widget.insert("1.0", content)
            if yview:
                widget.yview_moveto(yview[0])
            widget.configure(state="disabled")
        except TclError:
            return

    def _build_metric_card(
        self,
        parent: ttk.Frame,
        column: int,
        title: str,
        value_var: StringVar,
        *,
        compact: bool = False,
    ) -> None:
        if compact:
            card = ttk.LabelFrame(parent, text=title, padding=(5, 3))
            card.grid(row=0, column=column, sticky="nsew", padx=(0 if column == 0 else 3, 0))
            value_font = ("Microsoft YaHei UI", 10, "bold")
        else:
            card = ttk.LabelFrame(parent, text=title, padding=(10, 8))
            card.grid(row=0, column=column, sticky="nsew", padx=(0 if column == 0 else 6, 0))
            value_font = ("Microsoft YaHei UI", 11, "bold")
        card.columnconfigure(0, weight=1)
        ttk.Label(card, textvariable=value_var, font=value_font).grid(row=0, column=0, sticky="w")

    def _create_trade_order_tree(
        self,
        parent: ttk.Frame,
        *,
        on_select,
        column_group_key: str | None = None,
        title: str | None = None,
    ) -> ttk.Treeview:
        columns = ("time", "source", "inst_type", "inst_id", "state", "side", "ord_type", "price", "size", "filled", "fee", "tp_sl", "order_id", "cl_ord_id")
        tree = ttk.Treeview(parent, columns=columns, show="headings", selectmode="browse")
        headings = {
            "time": "时间",
            "source": "来源",
            "inst_type": "类型",
            "inst_id": "合约",
            "state": "状态",
            "side": "方向",
            "ord_type": "委托类型",
            "price": "委托价",
            "size": "委托量",
            "filled": "已成交",
            "fee": "手续费",
            "tp_sl": "TP/SL",
            "order_id": "订单ID",
            "cl_ord_id": "clOrdId",
        }
        for column_id, width in (
            ("time", 150),
            ("source", 82),
            ("inst_type", 72),
            ("inst_id", 240),
            ("state", 120),
            ("side", 96),
            ("ord_type", 110),
            ("price", 100),
            ("size", 100),
            ("filled", 100),
            ("fee", 220),
            ("tp_sl", 180),
            ("order_id", 120),
            ("cl_ord_id", 150),
        ):
            tree.heading(column_id, text=headings[column_id])
            tree.column(column_id, width=width, anchor="e" if column_id in {"price", "size", "filled", "fee"} else "center")
        tree.column("inst_id", anchor="w")
        tree.column("tp_sl", anchor="w")
        tree.column("cl_ord_id", anchor="w")
        tree.grid(row=0, column=0, sticky="nsew")
        tree.bind("<<TreeviewSelect>>", on_select)
        tree.tag_configure("profit", foreground="#13803d")
        tree.tag_configure("loss", foreground="#c23b3b")
        scroll_y = ttk.Scrollbar(parent, orient="vertical", command=tree.yview)
        scroll_y.grid(row=0, column=1, sticky="ns")
        scroll_x = ttk.Scrollbar(parent, orient="horizontal", command=tree.xview)
        scroll_x.grid(row=1, column=0, sticky="ew")
        tree.configure(yscrollcommand=scroll_y.set, xscrollcommand=scroll_x.set)
        if column_group_key and title:
            self._register_positions_zoom_columns(column_group_key, title, tree, columns)
        return tree

    def refresh_account_dashboard(self) -> None:
        self.refresh_account_info()
        self.refresh_order_views()

    def open_account_info_window(self) -> None:
        if self._account_info_window is not None and self._account_info_window.winfo_exists():
            self._account_info_window.focus_force()
            self.refresh_account_dashboard()
            return

        window = Toplevel(self.root)
        window.title("账户信息")
        apply_adaptive_window_geometry(
            window,
            width_ratio=0.78,
            height_ratio=0.76,
            min_width=1120,
            min_height=760,
            max_width=1600,
            max_height=1080,
        )
        self._account_info_window = window
        window.protocol("WM_DELETE_WINDOW", self._close_account_info_window)

        container = ttk.Frame(window, padding=12)
        container.pack(fill="both", expand=True)
        container.columnconfigure(0, weight=1)
        container.rowconfigure(3, weight=1)

        header = ttk.Frame(container)
        header.grid(row=0, column=0, sticky="ew", pady=(0, 10))
        header.columnconfigure(1, weight=1)
        account_badge = self._create_refresh_badge(
            header,
            self._account_info_refresh_badge_text,
            self._account_info_refresh_badges,
        )
        account_badge.grid(row=0, column=0, sticky="w", padx=(0, 8))
        ttk.Label(header, textvariable=self.account_info_summary_text, justify="left").grid(row=0, column=1, sticky="w")
        action_row = ttk.Frame(header)
        action_row.grid(row=0, column=2, sticky="e")
        ttk.Button(action_row, text="刷新全部", command=self.refresh_account_dashboard).grid(row=0, column=0, padx=(0, 6))
        ttk.Button(action_row, text="关闭", command=self._close_account_info_window).grid(row=0, column=1)

        overview_row = ttk.Frame(container)
        overview_row.grid(row=1, column=0, sticky="ew", pady=(0, 10))
        for column in range(6):
            overview_row.columnconfigure(column, weight=1)
        self._build_metric_card(overview_row, 0, "总权益", self.account_total_equity_text)
        self._build_metric_card(overview_row, 1, "调整后权益", self.account_adjusted_equity_text)
        self._build_metric_card(overview_row, 2, "可用权益", self.account_available_equity_text)
        self._build_metric_card(overview_row, 3, "未实现盈亏", self.account_upl_text)
        self._build_metric_card(overview_row, 4, "初始保证金(IMR)", self.account_imr_text)
        self._build_metric_card(overview_row, 5, "维持保证金(MMR)", self.account_mmr_text)

        config_frame = ttk.LabelFrame(container, text="账户配置", padding=10)
        config_frame.grid(row=2, column=0, sticky="nsew", pady=(0, 10))
        config_frame.columnconfigure(0, weight=1)
        config_frame.rowconfigure(0, weight=1)
        self._account_info_config_panel = Text(
            config_frame,
            height=5,
            wrap="word",
            font=("Microsoft YaHei UI", 10),
            relief="flat",
        )
        self._account_info_config_panel.grid(row=0, column=0, sticky="nsew")
        config_scroll = ttk.Scrollbar(config_frame, orient="vertical", command=self._account_info_config_panel.yview)
        config_scroll.grid(row=0, column=1, sticky="ns")
        self._account_info_config_panel.configure(yscrollcommand=config_scroll.set)
        self._set_readonly_text(self._account_info_config_panel, self._default_account_info_detail_text())

        notebook = ttk.Notebook(container)
        notebook.grid(row=3, column=0, sticky="nsew")

        asset_tab = ttk.Frame(notebook, padding=10)
        asset_tab.columnconfigure(0, weight=1)
        asset_tab.rowconfigure(0, weight=1)
        asset_tab.rowconfigure(1, weight=1)
        notebook.add(asset_tab, text="资产明细")

        asset_frame = ttk.LabelFrame(asset_tab, text="资产明细", padding=10)
        asset_frame.grid(row=0, column=0, sticky="nsew", pady=(0, 10))
        asset_frame.columnconfigure(0, weight=1)
        asset_frame.rowconfigure(0, weight=1)

        self._account_info_tree = ttk.Treeview(
            asset_frame,
            columns=("ccy", "eq", "eq_usd", "cash", "avail_bal", "avail_eq", "upl", "frozen", "liab"),
            show="headings",
            selectmode="browse",
        )
        tree = self._account_info_tree
        tree.heading("ccy", text="币种")
        tree.heading("eq", text="权益")
        tree.heading("eq_usd", text="折合USD")
        tree.heading("cash", text="现金余额")
        tree.heading("avail_bal", text="可用余额")
        tree.heading("avail_eq", text="可用权益")
        tree.heading("upl", text="未实现盈亏")
        tree.heading("frozen", text="冻结")
        tree.heading("liab", text="负债")
        tree.column("ccy", width=90, anchor="center")
        tree.column("eq", width=110, anchor="e")
        tree.column("eq_usd", width=110, anchor="e")
        tree.column("cash", width=110, anchor="e")
        tree.column("avail_bal", width=110, anchor="e")
        tree.column("avail_eq", width=110, anchor="e")
        tree.column("upl", width=110, anchor="e")
        tree.column("frozen", width=100, anchor="e")
        tree.column("liab", width=100, anchor="e")
        tree.grid(row=0, column=0, sticky="nsew")
        tree.bind("<<TreeviewSelect>>", self._refresh_account_info_detail)
        tree.tag_configure("profit", foreground="#13803d")
        tree.tag_configure("loss", foreground="#c23b3b")
        asset_scroll_y = ttk.Scrollbar(asset_frame, orient="vertical", command=tree.yview)
        asset_scroll_y.grid(row=0, column=1, sticky="ns")
        asset_scroll_x = ttk.Scrollbar(asset_frame, orient="horizontal", command=tree.xview)
        asset_scroll_x.grid(row=1, column=0, sticky="ew")
        tree.configure(yscrollcommand=asset_scroll_y.set, xscrollcommand=asset_scroll_x.set)

        detail_frame = ttk.LabelFrame(asset_tab, text="选中资产详情", padding=10)
        detail_frame.grid(row=1, column=0, sticky="nsew")
        detail_frame.columnconfigure(0, weight=1)
        detail_frame.rowconfigure(0, weight=1)
        self._account_info_detail_panel = Text(
            detail_frame,
            height=8,
            wrap="word",
            font=("Microsoft YaHei UI", 10),
            relief="flat",
        )
        self._account_info_detail_panel.grid(row=0, column=0, sticky="nsew")
        detail_scroll = ttk.Scrollbar(detail_frame, orient="vertical", command=self._account_info_detail_panel.yview)
        detail_scroll.grid(row=0, column=1, sticky="ns")
        self._account_info_detail_panel.configure(yscrollcommand=detail_scroll.set)
        self._set_readonly_text(self._account_info_detail_panel, self._default_account_info_detail_text())

        pending_orders_tab = ttk.Frame(notebook, padding=10)
        pending_orders_tab.columnconfigure(0, weight=1)
        pending_orders_tab.rowconfigure(2, weight=1)
        pending_orders_tab.rowconfigure(3, weight=1)
        notebook.add(pending_orders_tab, text="当前委托")
        self._build_account_info_pending_orders_tab(pending_orders_tab)

        order_history_tab = ttk.Frame(notebook, padding=10)
        order_history_tab.columnconfigure(0, weight=1)
        order_history_tab.rowconfigure(2, weight=1)
        order_history_tab.rowconfigure(3, weight=1)
        notebook.add(order_history_tab, text="历史委托")
        self._build_account_info_order_history_tab(order_history_tab)

        self._expand_to_screen(window, margin=30)
        self._refresh_all_refresh_badges()
        self.refresh_account_dashboard()

    def _close_account_info_window(self) -> None:
        if self._account_info_window is not None and self._account_info_window.winfo_exists():
            self._account_info_window.destroy()
        self._account_info_window = None
        self._account_info_tree = None
        self._account_info_detail_panel = None
        self._account_info_config_panel = None
        self._account_info_pending_orders_tree = None
        self._account_info_pending_orders_detail = None
        self._account_info_order_history_tree = None
        self._account_info_order_history_detail = None

    def refresh_account_info(self) -> None:
        if self._account_info_refreshing:
            return
        credentials = self._current_credentials_or_none()
        if credentials is None:
            _reset_refresh_health(self._account_info_refresh_health)
            self.account_info_summary_text.set("未配置 API 凭证，无法读取账户信息。")
            self._set_readonly_text(self._account_info_config_panel, "未配置 API 凭证，无法读取账户配置。")
            self._set_readonly_text(self._account_info_detail_panel, self._default_account_info_detail_text())
            if self._account_info_tree is not None:
                self._account_info_tree.delete(*self._account_info_tree.get_children())
            self._refresh_all_refresh_badges()
            return
        self._account_info_refreshing = True
        self.account_info_summary_text.set("正在刷新账户信息...")
        environment = self._positions_effective_environment or ENV_OPTIONS[self.environment_label.get()]
        threading.Thread(
            target=self._refresh_account_info_worker,
            args=(credentials, environment),
            daemon=True,
        ).start()

    def _refresh_account_info_worker(self, credentials: Credentials, environment: str) -> None:
        try:
            overview = self.client.get_account_overview(credentials, environment=environment)
            config = self.client.get_account_config(credentials, environment=environment)
        except Exception as exc:
            message = str(exc)
            if "50101" in message and "current environment" in message:
                alternate = "live" if environment == "demo" else "demo"
                try:
                    overview = self.client.get_account_overview(credentials, environment=alternate)
                    config = self.client.get_account_config(credentials, environment=alternate)
                except Exception:
                    self.root.after(0, lambda: self._apply_account_info_error(message))
                    return
                summary = (
                    f"当前 API Key 与 {alternate} 环境匹配，已自动按 "
                    f"{'实盘' if alternate == 'live' else '模拟盘'} 读取账户信息。"
                )
                self.root.after(0, lambda: self._apply_account_info(overview, config, summary, alternate))
                return
            self.root.after(0, lambda: self._apply_account_info_error(message))
            return
        self.root.after(0, lambda: self._apply_account_info(overview, config, None, environment))

    def _apply_account_info(
        self,
        overview: OkxAccountOverview,
        config: OkxAccountConfig,
        summary_note: str | None,
        effective_environment: str,
    ) -> None:
        self._account_info_refreshing = False
        self._latest_account_overview = overview
        self._latest_account_config = config
        _mark_refresh_health_success(self._account_info_refresh_health)
        self._refresh_all_refresh_badges()
        environment_label = "实盘 live" if effective_environment == "live" else "模拟盘 demo"
        summary_parts = []
        if summary_note:
            summary_parts.append(summary_note)
        summary_parts.append(f"API配置：{self._current_credential_profile()}")
        summary_parts.append(f"环境：{environment_label}")
        summary_parts.append(f"账户模式：{_format_account_level(config.account_level)}")
        summary_parts.append(f"持仓模式：{_format_account_position_mode(config.position_mode)}")
        summary_parts.append(f"Greeks：{_format_greeks_type(config.greeks_type)}")
        self.account_info_summary_text.set(" | ".join(summary_parts))
        self.account_total_equity_text.set(_format_optional_usdt_precise(overview.total_equity, places=2, with_sign=False))
        self.account_adjusted_equity_text.set(_format_optional_usdt_precise(overview.adjusted_equity, places=2, with_sign=False))
        self.account_available_equity_text.set(_format_optional_usdt_precise(overview.available_equity, places=2, with_sign=False))
        self.account_upl_text.set(_format_optional_usdt_precise(overview.unrealized_pnl, places=2))
        self.account_imr_text.set(_format_optional_usdt_precise(overview.initial_margin, places=2, with_sign=False))
        self.account_mmr_text.set(_format_optional_usdt_precise(overview.maintenance_margin, places=2, with_sign=False))
        self._set_readonly_text(
            self._account_info_config_panel,
            _build_account_config_detail_text(
                config,
                overview,
                profile_name=self._current_credential_profile(),
                environment=effective_environment,
            ),
        )
        if self._account_info_tree is not None:
            selected_before = self._account_info_tree.selection()[0] if self._account_info_tree.selection() else None
            self._account_info_tree.delete(*self._account_info_tree.get_children())
            for index, asset in enumerate(overview.details):
                tags: tuple[str, ...] = ()
                if asset.unrealized_pnl is not None:
                    tags = (_pnl_tag(asset.unrealized_pnl),)
                self._account_info_tree.insert(
                    "",
                    END,
                    iid=f"acct-{index}",
                    values=(
                        asset.ccy or "-",
                        _format_optional_decimal(asset.equity),
                        _format_optional_usdt_precise(asset.equity_usd, places=2, with_sign=False),
                        _format_optional_decimal(asset.cash_balance),
                        _format_optional_decimal(asset.available_balance),
                        _format_optional_decimal(asset.available_equity),
                        _format_optional_decimal(asset.unrealized_pnl, with_sign=True),
                        _format_optional_decimal(asset.frozen_balance),
                        _format_optional_decimal(asset.liability),
                    ),
                    tags=tags,
                )
            if selected_before and self._account_info_tree.exists(selected_before):
                self._account_info_tree.selection_set(selected_before)
            elif overview.details:
                self._account_info_tree.selection_set("acct-0")
            self._refresh_account_info_detail()

    def _apply_account_info_error(self, message: str) -> None:
        self._account_info_refreshing = False
        friendly_message = _format_network_error_message(message)
        _mark_refresh_health_failure(self._account_info_refresh_health, friendly_message)
        self._refresh_all_refresh_badges()
        suffix = _format_refresh_health_suffix(self._account_info_refresh_health)
        has_previous_data = self._latest_account_overview is not None or self._latest_account_config is not None
        if has_previous_data:
            summary = f"账户信息刷新失败，继续显示上一份缓存：{friendly_message}{suffix}"
            self.account_info_summary_text.set(summary)
            self._enqueue_log(summary)
            return
        summary = f"账户信息读取失败：{friendly_message}{suffix}"
        self.account_info_summary_text.set(summary)
        self._set_readonly_text(self._account_info_config_panel, f"账户配置读取失败：{friendly_message}")
        self._set_readonly_text(self._account_info_detail_panel, self._default_account_info_detail_text())
        self._enqueue_log(summary)

    def _refresh_account_info_detail(self, *_: object) -> None:
        tree = self._account_info_tree
        overview = self._latest_account_overview
        if tree is None or overview is None:
            self._set_readonly_text(self._account_info_detail_panel, self._default_account_info_detail_text())
            return
        selection = tree.selection()
        if not selection:
            self._set_readonly_text(self._account_info_detail_panel, self._default_account_info_detail_text())
            return
        try:
            index = int(selection[0].split("-", 1)[1])
        except Exception:
            self._set_readonly_text(self._account_info_detail_panel, self._default_account_info_detail_text())
            return
        if index < 0 or index >= len(overview.details):
            self._set_readonly_text(self._account_info_detail_panel, self._default_account_info_detail_text())
            return
        self._set_readonly_text(self._account_info_detail_panel, _build_account_asset_detail_text(overview.details[index]))

    def _build_account_info_pending_orders_tab(self, parent: ttk.Frame) -> None:
        header = ttk.Frame(parent)
        header.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        header.columnconfigure(1, weight=1)
        pending_badge = self._create_refresh_badge(
            header,
            self._pending_orders_refresh_badge_text,
            self._pending_orders_refresh_badges,
        )
        pending_badge.grid(row=0, column=0, sticky="w", padx=(0, 8))
        ttk.Label(header, textvariable=self._positions_zoom_pending_orders_summary_text).grid(row=0, column=1, sticky="w")
        ttk.Button(header, text="刷新", command=self.refresh_pending_orders).grid(row=0, column=2, sticky="e", padx=(0, 6))
        ttk.Button(
            header,
            text="撤单选中",
            command=lambda: self.cancel_selected_pending_order("account_info"),
        ).grid(row=0, column=3, sticky="e", padx=(0, 6))
        ttk.Button(
            header,
            text="批量撤当前筛选",
            command=lambda: self.cancel_filtered_pending_orders("account_info"),
        ).grid(row=0, column=4, sticky="e")

        filter_row = ttk.Frame(parent)
        filter_row.grid(row=1, column=0, sticky="ew", pady=(0, 8))
        filter_row.columnconfigure(11, weight=1)
        ttk.Label(filter_row, text="类型").grid(row=0, column=0, sticky="w")
        type_combo = ttk.Combobox(
            filter_row,
            textvariable=self.pending_order_type_filter,
            values=list(POSITION_TYPE_OPTIONS.keys()),
            state="readonly",
            width=16,
        )
        type_combo.grid(row=0, column=1, sticky="w", padx=(6, 12))
        type_combo.bind("<<ComboboxSelected>>", self._on_pending_order_filter_changed)
        ttk.Label(filter_row, text="来源").grid(row=0, column=2, sticky="w")
        source_combo = ttk.Combobox(
            filter_row,
            textvariable=self.pending_order_source_filter,
            values=list(ORDER_SOURCE_FILTER_OPTIONS.keys()),
            state="readonly",
            width=16,
        )
        source_combo.grid(row=0, column=3, sticky="w", padx=(6, 12))
        source_combo.bind("<<ComboboxSelected>>", self._on_pending_order_filter_changed)
        ttk.Label(filter_row, text="状态").grid(row=0, column=4, sticky="w")
        state_combo = ttk.Combobox(
            filter_row,
            textvariable=self.pending_order_state_filter,
            values=list(ORDER_STATE_FILTER_OPTIONS.keys()),
            state="readonly",
            width=20,
        )
        state_combo.grid(row=0, column=5, sticky="w", padx=(6, 12))
        state_combo.bind("<<ComboboxSelected>>", self._on_pending_order_filter_changed)
        ttk.Label(filter_row, text="标的").grid(row=0, column=6, sticky="w")
        asset_entry = ttk.Entry(filter_row, textvariable=self.pending_order_asset_filter, width=10)
        asset_entry.grid(row=0, column=7, sticky="w", padx=(6, 12))
        asset_entry.bind("<KeyRelease>", self._on_pending_order_filter_changed)
        ttk.Label(filter_row, text="到期前缀").grid(row=0, column=8, sticky="w")
        expiry_entry = ttk.Entry(filter_row, textvariable=self.pending_order_expiry_prefix_filter, width=14)
        expiry_entry.grid(row=0, column=9, sticky="w", padx=(6, 12))
        expiry_entry.bind("<KeyRelease>", self._on_pending_order_filter_changed)
        ttk.Label(filter_row, text="搜索").grid(row=0, column=10, sticky="w")
        keyword_entry = ttk.Entry(filter_row, textvariable=self.pending_order_keyword)
        keyword_entry.grid(row=0, column=11, sticky="ew", padx=(6, 12))
        keyword_entry.bind("<KeyRelease>", self._on_pending_order_filter_changed)
        ttk.Button(filter_row, text="应用筛选", command=self._render_pending_orders_view).grid(row=0, column=12, padx=(0, 6))
        ttk.Button(filter_row, text="清空筛选", command=self.reset_pending_order_filters).grid(row=0, column=13)

        tree_frame = ttk.Frame(parent)
        tree_frame.grid(row=2, column=0, sticky="nsew")
        tree_frame.columnconfigure(0, weight=1)
        tree_frame.rowconfigure(0, weight=1)
        self._account_info_pending_orders_tree = self._create_trade_order_tree(
            tree_frame,
            on_select=self._on_pending_orders_selected,
        )

        detail_frame = ttk.LabelFrame(parent, text="委托详情", padding=12)
        detail_frame.grid(row=3, column=0, sticky="nsew", pady=(10, 0))
        detail_frame.columnconfigure(0, weight=1)
        detail_frame.rowconfigure(0, weight=1)
        self._account_info_pending_orders_detail = Text(
            detail_frame,
            height=8,
            wrap="word",
            font=("Microsoft YaHei UI", 10),
            relief="flat",
        )
        self._account_info_pending_orders_detail.grid(row=0, column=0, sticky="nsew")
        detail_scroll = ttk.Scrollbar(detail_frame, orient="vertical", command=self._account_info_pending_orders_detail.yview)
        detail_scroll.grid(row=0, column=1, sticky="ns")
        self._account_info_pending_orders_detail.configure(yscrollcommand=detail_scroll.set)
        self._set_readonly_text(self._account_info_pending_orders_detail, "这里会显示选中当前委托的详情。")

    def _build_account_info_order_history_tab(self, parent: ttk.Frame) -> None:
        header = ttk.Frame(parent)
        header.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        header.columnconfigure(1, weight=1)
        order_history_badge = self._create_refresh_badge(
            header,
            self._order_history_refresh_badge_text,
            self._order_history_refresh_badges,
        )
        order_history_badge.grid(row=0, column=0, sticky="w", padx=(0, 8))
        ttk.Label(header, textvariable=self._positions_zoom_order_history_summary_text).grid(row=0, column=1, sticky="w")
        ttk.Button(header, text="同步", command=self.refresh_order_history).grid(row=0, column=2, sticky="e")

        filter_row = ttk.Frame(parent)
        filter_row.grid(row=1, column=0, sticky="ew", pady=(0, 8))
        filter_row.columnconfigure(11, weight=1)
        ttk.Label(filter_row, text="类型").grid(row=0, column=0, sticky="w")
        type_combo = ttk.Combobox(
            filter_row,
            textvariable=self.order_history_type_filter,
            values=list(POSITION_TYPE_OPTIONS.keys()),
            state="readonly",
            width=16,
        )
        type_combo.grid(row=0, column=1, sticky="w", padx=(6, 12))
        type_combo.bind("<<ComboboxSelected>>", self._on_order_history_filter_changed)
        ttk.Label(filter_row, text="来源").grid(row=0, column=2, sticky="w")
        source_combo = ttk.Combobox(
            filter_row,
            textvariable=self.order_history_source_filter,
            values=list(ORDER_SOURCE_FILTER_OPTIONS.keys()),
            state="readonly",
            width=16,
        )
        source_combo.grid(row=0, column=3, sticky="w", padx=(6, 12))
        source_combo.bind("<<ComboboxSelected>>", self._on_order_history_filter_changed)
        ttk.Label(filter_row, text="状态").grid(row=0, column=4, sticky="w")
        state_combo = ttk.Combobox(
            filter_row,
            textvariable=self.order_history_state_filter,
            values=list(ORDER_STATE_FILTER_OPTIONS.keys()),
            state="readonly",
            width=20,
        )
        state_combo.grid(row=0, column=5, sticky="w", padx=(6, 12))
        state_combo.bind("<<ComboboxSelected>>", self._on_order_history_filter_changed)
        ttk.Label(filter_row, text="标的").grid(row=0, column=6, sticky="w")
        asset_entry = ttk.Entry(filter_row, textvariable=self.order_history_asset_filter, width=10)
        asset_entry.grid(row=0, column=7, sticky="w", padx=(6, 12))
        asset_entry.bind("<KeyRelease>", self._on_order_history_filter_changed)
        ttk.Label(filter_row, text="到期前缀").grid(row=0, column=8, sticky="w")
        expiry_entry = ttk.Entry(filter_row, textvariable=self.order_history_expiry_prefix_filter, width=14)
        expiry_entry.grid(row=0, column=9, sticky="w", padx=(6, 12))
        expiry_entry.bind("<KeyRelease>", self._on_order_history_filter_changed)
        ttk.Label(filter_row, text="搜索").grid(row=0, column=10, sticky="w")
        keyword_entry = ttk.Entry(filter_row, textvariable=self.order_history_keyword)
        keyword_entry.grid(row=0, column=11, sticky="ew", padx=(6, 12))
        keyword_entry.bind("<KeyRelease>", self._on_order_history_filter_changed)
        ttk.Button(filter_row, text="应用筛选", command=self._render_order_history_view).grid(row=0, column=12, padx=(0, 6))
        ttk.Button(filter_row, text="清空筛选", command=self.reset_order_history_filters).grid(row=0, column=13)

        tree_frame = ttk.Frame(parent)
        tree_frame.grid(row=2, column=0, sticky="nsew")
        tree_frame.columnconfigure(0, weight=1)
        tree_frame.rowconfigure(0, weight=1)
        self._account_info_order_history_tree = self._create_trade_order_tree(
            tree_frame,
            on_select=self._on_order_history_selected,
        )

        detail_frame = ttk.LabelFrame(parent, text="委托详情", padding=12)
        detail_frame.grid(row=3, column=0, sticky="nsew", pady=(10, 0))
        detail_frame.columnconfigure(0, weight=1)
        detail_frame.rowconfigure(0, weight=1)
        self._account_info_order_history_detail = Text(
            detail_frame,
            height=8,
            wrap="word",
            font=("Microsoft YaHei UI", 10),
            relief="flat",
        )
        self._account_info_order_history_detail.grid(row=0, column=0, sticky="nsew")
        detail_scroll = ttk.Scrollbar(detail_frame, orient="vertical", command=self._account_info_order_history_detail.yview)
        detail_scroll.grid(row=0, column=1, sticky="ns")
        self._account_info_order_history_detail.configure(yscrollcommand=detail_scroll.set)
        self._set_readonly_text(self._account_info_order_history_detail, "这里会显示选中历史委托的详情。")
























































































































































    def open_settings_window(self) -> None:
        if self._settings_window is not None and self._settings_window.winfo_exists():
            self._settings_window.focus_force()
            return

        window = Toplevel(self.root)
        window.title("API 与通知设置")
        apply_adaptive_window_geometry(
            window,
            width_ratio=0.58,
            height_ratio=0.66,
            min_width=760,
            min_height=620,
            max_width=1080,
            max_height=900,
        )
        window.transient(self.root)
        self._settings_window = window
        window.protocol("WM_DELETE_WINDOW", self._close_settings_window)

        container = ttk.Frame(window, padding=16)
        container.pack(fill="both", expand=True)
        container.columnconfigure(0, weight=1)
        container.rowconfigure(1, weight=1)

        account_frame = ttk.LabelFrame(container, text="API 与交易设置", padding=16)
        account_frame.grid(row=0, column=0, sticky="ew")
        for column in range(4):
            account_frame.columnconfigure(column, weight=1)

        row = 0
        ttk.Label(account_frame, text="API 配置").grid(row=row, column=0, sticky="w")
        self._credential_profile_combo = ttk.Combobox(
            account_frame,
            textvariable=self.api_profile_name,
            state="readonly",
        )
        self._credential_profile_combo.grid(row=row, column=1, sticky="ew", padx=(0, 16))
        self._credential_profile_combo.bind("<<ComboboxSelected>>", self._on_api_profile_selected)
        profile_buttons = ttk.Frame(account_frame)
        profile_buttons.grid(row=row, column=2, columnspan=2, sticky="e")
        ttk.Button(profile_buttons, text="新建配置", command=self._create_api_profile).grid(row=0, column=0, padx=(0, 8))
        ttk.Button(profile_buttons, text="重命名", command=self._rename_current_api_profile).grid(row=0, column=1, padx=(0, 8))
        ttk.Button(profile_buttons, text="删除当前", command=self._delete_current_api_profile).grid(row=0, column=2)
        self._sync_credential_profile_combo()

        row += 1
        ttk.Label(account_frame, text="API Key").grid(row=row, column=0, sticky="w")
        ttk.Entry(account_frame, textvariable=self.api_key).grid(row=row, column=1, sticky="ew", padx=(0, 16))
        ttk.Label(account_frame, text="Passphrase").grid(row=row, column=2, sticky="w")
        ttk.Entry(account_frame, textvariable=self.passphrase, show="*").grid(row=row, column=3, sticky="ew")

        row += 1
        ttk.Label(account_frame, text="Secret Key").grid(row=row, column=0, sticky="w", pady=(12, 0))
        ttk.Entry(account_frame, textvariable=self.secret_key, show="*").grid(
            row=row, column=1, sticky="ew", padx=(0, 16), pady=(12, 0)
        )
        ttk.Label(account_frame, text="环境").grid(row=row, column=2, sticky="w", pady=(12, 0))
        ttk.Combobox(
            account_frame,
            textvariable=self.environment_label,
            values=list(ENV_OPTIONS.keys()),
            state="readonly",
        ).grid(row=row, column=3, sticky="ew", pady=(12, 0))

        row += 1
        ttk.Label(account_frame, text="交易模式").grid(row=row, column=0, sticky="w", pady=(12, 0))
        ttk.Combobox(
            account_frame,
            textvariable=self.trade_mode_label,
            values=list(TRADE_MODE_OPTIONS.keys()),
            state="readonly",
        ).grid(row=row, column=1, sticky="ew", padx=(0, 16), pady=(12, 0))
        ttk.Label(account_frame, text="持仓模式").grid(row=row, column=2, sticky="w", pady=(12, 0))
        ttk.Combobox(
            account_frame,
            textvariable=self.position_mode_label,
            values=list(POSITION_MODE_OPTIONS.keys()),
            state="readonly",
        ).grid(row=row, column=3, sticky="ew", pady=(12, 0))

        row += 1
        ttk.Label(account_frame, text="TP/SL 触发价格类型").grid(row=row, column=0, sticky="w", pady=(12, 0))
        ttk.Combobox(
            account_frame,
            textvariable=self.trigger_type_label,
            values=list(TRIGGER_TYPE_OPTIONS.keys()),
            state="readonly",
        ).grid(row=row, column=1, sticky="ew", padx=(0, 16), pady=(12, 0))
        ttk.Label(
            account_frame,
            text=f"凭证自动保存到：{credentials_file_path().name}（支持多个 API 配置）",
            justify="left",
        ).grid(row=row, column=2, columnspan=2, sticky="w", pady=(12, 0))

        mail_frame = ttk.LabelFrame(container, text="邮件通知设置", padding=16)
        mail_frame.grid(row=1, column=0, sticky="nsew", pady=(14, 0))
        for column in range(4):
            mail_frame.columnconfigure(column, weight=1)

        row = 0
        ttk.Checkbutton(mail_frame, text="启用邮件通知", variable=self.notify_enabled).grid(
            row=row, column=0, sticky="w"
        )
        ttk.Checkbutton(mail_frame, text="使用 SSL", variable=self.use_ssl).grid(
            row=row, column=1, sticky="w"
        )
        ttk.Checkbutton(mail_frame, text="成交邮件", variable=self.notify_trade_fills).grid(
            row=row, column=2, sticky="w"
        )
        ttk.Checkbutton(mail_frame, text="信号邮件", variable=self.notify_signals).grid(
            row=row, column=3, sticky="w"
        )

        row += 1
        ttk.Label(mail_frame, text="SMTP 主机").grid(row=row, column=0, sticky="w", pady=(12, 0))
        ttk.Entry(mail_frame, textvariable=self.smtp_host).grid(
            row=row, column=1, sticky="ew", padx=(0, 16), pady=(12, 0)
        )
        ttk.Label(mail_frame, text="SMTP 端口").grid(row=row, column=2, sticky="w", pady=(12, 0))
        ttk.Entry(mail_frame, textvariable=self.smtp_port).grid(row=row, column=3, sticky="ew", pady=(12, 0))

        row += 1
        ttk.Label(mail_frame, text="SMTP 用户名").grid(row=row, column=0, sticky="w", pady=(12, 0))
        ttk.Entry(mail_frame, textvariable=self.smtp_username).grid(
            row=row, column=1, sticky="ew", padx=(0, 16), pady=(12, 0)
        )
        ttk.Label(mail_frame, text="SMTP 密码").grid(row=row, column=2, sticky="w", pady=(12, 0))
        ttk.Entry(mail_frame, textvariable=self.smtp_password, show="*").grid(
            row=row, column=3, sticky="ew", pady=(12, 0)
        )

        row += 1
        ttk.Label(mail_frame, text="发件邮箱").grid(row=row, column=0, sticky="w", pady=(12, 0))
        ttk.Entry(mail_frame, textvariable=self.sender_email).grid(
            row=row, column=1, sticky="ew", padx=(0, 16), pady=(12, 0)
        )
        ttk.Label(mail_frame, text="收件邮箱").grid(row=row, column=2, sticky="w", pady=(12, 0))
        ttk.Entry(mail_frame, textvariable=self.recipient_emails).grid(row=row, column=3, sticky="ew", pady=(12, 0))

        row += 1
        ttk.Checkbutton(mail_frame, text="异常邮件", variable=self.notify_errors).grid(
            row=row, column=0, sticky="w", pady=(12, 0)
        )
        ttk.Label(
            mail_frame,
            text="多个收件人可用逗号、分号或换行分隔。",
            justify="left",
        ).grid(row=row, column=1, columnspan=3, sticky="w", pady=(12, 0))

        row += 1
        ttk.Label(
            mail_frame,
            text=f"通知设置会保存到：{settings_file_path().name}",
            justify="left",
        ).grid(row=row, column=0, columnspan=4, sticky="w", pady=(16, 0))

        footer = ttk.Frame(container)
        footer.grid(row=2, column=0, sticky="e", pady=(16, 0))
        ttk.Button(footer, text="发送测试邮件", command=self.send_test_email).grid(row=0, column=0, padx=(0, 8))
        ttk.Button(footer, text="关闭", command=self._close_settings_window).grid(row=0, column=1)





    def open_line_trading_desk_window(self) -> None:
        existing = self._line_trading_desk_window
        if existing is not None and _widget_exists(existing.window):
            existing.window.deiconify()
            existing.window.lift()
            existing.window.focus_force()
            self._request_line_trading_desk_refresh(immediate=True)
            return

        window = Toplevel(self.root)
        apply_window_icon(window)
        window.title("划线交易台")
        apply_adaptive_window_geometry(
            window,
            width_ratio=0.84,
            height_ratio=0.74,
            min_width=1220,
            min_height=760,
            max_width=1880,
            max_height=1240,
        )
        # 不设 transient，以便 Windows 显示系统标题栏上的最小化/最大化(方块)/关闭，与原生窗口一致。

        container = ttk.Frame(window, padding=10)
        container.pack(fill="both", expand=True)
        container.columnconfigure(0, weight=3)
        container.columnconfigure(1, weight=2)
        container.rowconfigure(2, weight=1)

        top_bar = ttk.Frame(container)
        top_bar.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 8))
        top_bar.columnconfigure(16, weight=1)
        ttk.Label(top_bar, text="API").grid(row=0, column=0, sticky="w")
        api_profile_var = StringVar(value=self._current_credential_profile())
        api_combo = ttk.Combobox(top_bar, width=14, textvariable=api_profile_var, values=self._credential_profile_names())
        api_combo.grid(row=0, column=1, padx=(4, 10))
        ttk.Label(top_bar, text="标的").grid(row=0, column=2, sticky="w")
        symbol_var = StringVar(value=self._default_line_trading_symbol())
        symbol_box = ttk.Combobox(top_bar, width=18, textvariable=symbol_var)
        symbol_box["values"] = self._line_trading_symbol_values()
        symbol_box.grid(row=0, column=3, padx=(4, 8))
        ttk.Label(top_bar, text="周期").grid(row=0, column=4, sticky="w")
        bar_var = StringVar(value="1H")
        ttk.Combobox(top_bar, width=8, state="readonly", values=BAR_OPTIONS, textvariable=bar_var).grid(
            row=0, column=5, padx=(4, 8)
        )
        ttk.Button(top_bar, text="刷新", command=lambda: self._request_line_trading_desk_refresh(immediate=True)).grid(
            row=0, column=6, padx=(0, 6)
        )
        ttk.Button(top_bar, text="重置视图", command=self._line_trading_desk_reset_chart_view).grid(row=0, column=7, padx=(0, 6))
        ttk.Button(top_bar, text="区间放大", command=lambda: self._set_line_trading_desk_tool("zoom_range")).grid(
            row=0, column=8, padx=(0, 4)
        )
        ttk.Button(top_bar, text="趋势线(射线)", command=lambda: self._set_line_trading_desk_tool("line")).grid(
            row=0, column=9, padx=(0, 4)
        )
        ttk.Button(top_bar, text="水平射线", command=lambda: self._set_line_trading_desk_tool("horizontal")).grid(
            row=0, column=10, padx=(0, 4)
        )
        ttk.Button(top_bar, text="止损线", command=lambda: self._set_line_trading_desk_tool("stop")).grid(
            row=0, column=11, padx=(0, 4)
        )
        ttk.Button(top_bar, text="盈亏比·多", command=lambda: self._set_line_trading_desk_rr_draw("long")).grid(
            row=0, column=12, padx=(0, 2)
        )
        ttk.Button(top_bar, text="盈亏比·空", command=lambda: self._set_line_trading_desk_rr_draw("short")).grid(
            row=0, column=13, padx=(0, 4)
        )
        ttk.Button(top_bar, text="清空线", command=self._clear_line_trading_desk_lines).grid(row=0, column=14, padx=(0, 4))
        ttk.Button(top_bar, text="开多", command=lambda: self._submit_line_trading_desk_order("long")).grid(
            row=0, column=15, padx=(8, 4)
        )
        ttk.Button(top_bar, text="开空", command=lambda: self._submit_line_trading_desk_order("short")).grid(
            row=0, column=16, sticky="w"
        )

        param_card = ttk.LabelFrame(container, text="集中参数（开多/开空、射线触发、盈亏比预览共用）", padding=8)
        param_card.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(0, 8))
        for col in range(0, 12):
            param_card.columnconfigure(col, weight=0)
        param_atr_period = StringVar(value="10")
        param_atr_mult = StringVar(value="1")
        param_risk_amount = StringVar(value="100")
        param_cross_mode = StringVar(value="tick_last")
        param_ray_action = StringVar(value=_LINE_DESK_RAY_ACTION_LABEL_ZH["notify"])
        param_rr_r = StringVar(value="2")
        param_rr_side = StringVar(value="long")
        pr = 0
        ttk.Label(param_card, text="ATR周期").grid(row=pr, column=0, sticky="w")
        ttk.Entry(param_card, width=6, textvariable=param_atr_period).grid(row=pr, column=1, padx=(4, 12))
        ttk.Label(param_card, text="止损×ATR").grid(row=pr, column=2, sticky="w")
        ttk.Entry(param_card, width=6, textvariable=param_atr_mult).grid(row=pr, column=3, padx=(4, 12))
        ttk.Label(param_card, text="风险金(USDT)").grid(row=pr, column=4, sticky="w")
        ttk.Entry(param_card, width=8, textvariable=param_risk_amount).grid(row=pr, column=5, padx=(4, 12))
        ttk.Label(param_card, text="穿越判定").grid(row=pr, column=6, sticky="w")
        ttk.Combobox(
            param_card,
            width=14,
            state="readonly",
            textvariable=param_cross_mode,
            values=("tick_last", "close"),
        ).grid(row=pr, column=7, padx=(4, 12))
        ttk.Label(param_card, text="新射线默认").grid(row=pr, column=8, sticky="w")
        ttk.Combobox(
            param_card,
            width=10,
            state="readonly",
            textvariable=param_ray_action,
            values=tuple(_LINE_DESK_RAY_ACTION_LABEL_ZH[k] for k in ("notify", "long", "short")),
        ).grid(row=pr, column=9, padx=(4, 12))
        ttk.Label(param_card, text="止盈×止损距(R)").grid(row=pr, column=10, sticky="w")
        ttk.Entry(param_card, width=5, textvariable=param_rr_r).grid(row=pr, column=11, padx=(4, 0))
        param_order_mode = StringVar(value="限价挂单")
        desk_pr2 = 1
        ttk.Label(param_card, text="开仓方式").grid(row=desk_pr2, column=0, sticky="w", pady=(6, 0))
        ttk.Combobox(
            param_card,
            width=14,
            state="readonly",
            textvariable=param_order_mode,
            values=("限价挂单", "对手价"),
        ).grid(row=desk_pr2, column=1, padx=(4, 12), sticky="w", pady=(6, 0))
        hint = ttk.Label(
            param_card,
            text=(
                "标的默认可选5个主流永续；周期默认1H。右侧「账户」可切换当前持仓 / 当前委托 / 历史委托（均按当前标的过滤）。"
                "K线纵轴价、盈亏比与持仓价等按 tick 显示。滚轮缩放；Shift+拖或区间放大后在空白处左键拖平移。"
                "盈亏比列表有选中行时，开多/开空将按该行提交「限价+交易所止盈止损」（非单笔裸限价）。"
                "开仓方式「对手价」为 IOC 吃单。"
                "趋势线/水平射线在「新射线默认」为开多/开空时：触发后以穿越价为参考开仓价，止损=上一根K线参照+ATR周期×止损×ATR，"
                "止盈距离=|开仓-止损|×上方「止盈×止损距(R)」；成交后本地监控用同一 R。"
                "持仓表下方可填平仓数量（空=全平），点「按数量市价平仓」。"
            ),
            foreground="#555",
        )
        hint.grid(row=2, column=0, columnspan=14, sticky="w", pady=(6, 0))

        chart_frame = ttk.Frame(container)
        chart_frame.grid(row=2, column=0, sticky="nsew", padx=(0, 8))
        chart_frame.columnconfigure(0, weight=1)
        chart_frame.rowconfigure(0, weight=1)
        canvas = Canvas(chart_frame, background="#ffffff", highlightthickness=0, width=980, height=620)
        canvas.grid(row=0, column=0, sticky="nsew")

        right = ttk.Frame(container)
        right.grid(row=2, column=1, sticky="nsew")
        right.columnconfigure(0, weight=1)
        right.rowconfigure(4, weight=1)
        status_text = StringVar(value="划线交易台已就绪。")
        _desk_status_bg = ttk.Style(self.root).lookup("TFrame", "background") or "#ececec"
        Label(
            right,
            textvariable=status_text,
            wraplength=460,
            justify="left",
            anchor="nw",
            height=3,
            background=_desk_status_bg,
            font=("Microsoft YaHei UI", 9),
        ).grid(row=0, column=0, sticky="ew", pady=(0, 6))

        desk_log_frame = ttk.LabelFrame(
            right,
            text="工作台日志（画线 / 射线触发 / 盈亏比；另存 logs/line_desk/日期.log）",
            padding=4,
        )
        desk_log_frame.grid(row=1, column=0, sticky="nsew", pady=(0, 6))
        desk_log_frame.columnconfigure(0, weight=1)
        desk_log_frame.rowconfigure(0, weight=1)
        desk_log_text = Text(
            desk_log_frame,
            height=7,
            wrap="word",
            font=("Consolas", 9),
            relief="flat",
            state="disabled",
        )
        desk_log_text.grid(row=0, column=0, sticky="nsew")
        desk_log_scroll = ttk.Scrollbar(desk_log_frame, orient="vertical", command=desk_log_text.yview)
        desk_log_scroll.grid(row=0, column=1, sticky="ns")
        desk_log_text.configure(yscrollcommand=desk_log_scroll.set)

        ray_box = ttk.LabelFrame(right, text="射线触发（一次性）", padding=6)
        ray_box.grid(row=2, column=0, sticky="ew", pady=(0, 6))
        ray_box.columnconfigure(0, weight=1)
        ray_tree = ttk.Treeview(
            ray_box,
            columns=("lock", "kind", "hprice", "action", "state"),
            show="headings",
            height=5,
        )
        for col, text, width in (
            ("lock", "锁", 36),
            ("kind", "类型", 80),
            ("hprice", "水平价（双击改）", 112),
            ("action", "动作", 70),
            ("state", "状态", 80),
        ):
            ray_tree.heading(col, text=text)
            ray_tree.column(col, width=width, anchor="center")
        ray_tree.grid(row=0, column=0, sticky="ew")
        ray_tree.bind("<Double-1>", self._on_line_trading_desk_ray_tree_double_click)
        ray_tree.bind("<<TreeviewSelect>>", lambda _e: self._line_trading_desk_on_ray_tree_select())

        ray_btn_row = ttk.Frame(ray_box)
        ray_btn_row.grid(row=1, column=0, sticky="ew", pady=(4, 0))
        ttk.Button(ray_btn_row, text="切换选中锁定", command=self._line_trading_desk_toggle_selected_ray_lock).grid(
            row=0, column=0, padx=(0, 6)
        )
        ttk.Button(ray_btn_row, text="删除选中", command=self._line_trading_desk_delete_selected_ray).grid(row=0, column=1, padx=(0, 0))

        rr_box = ttk.LabelFrame(right, text="盈亏比（拖入场/止损/止盈线或风险区；锁后不可拖）", padding=6)
        rr_box.grid(row=3, column=0, sticky="ew", pady=(0, 6))
        rr_box.columnconfigure(0, weight=1)
        rr_manage_tree = ttk.Treeview(
            rr_box,
            columns=("lock", "entry", "sl", "tp", "r"),
            show="headings",
            height=4,
        )
        for col, text, width in (
            ("lock", "锁", 36),
            ("entry", "入场", 72),
            ("sl", "止损", 72),
            ("tp", "止盈", 72),
            ("r", "R", 36),
        ):
            rr_manage_tree.heading(col, text=text)
            rr_manage_tree.column(col, width=width, anchor="center")
        rr_manage_tree.grid(row=0, column=0, sticky="ew")
        rr_btn_row = ttk.Frame(rr_box)
        rr_btn_row.grid(row=1, column=0, sticky="ew", pady=(4, 0))
        rr_btn_row.columnconfigure(4, weight=1)
        ttk.Button(rr_btn_row, text="切换选中锁定", command=self._line_trading_desk_toggle_selected_rr_lock).grid(
            row=0, column=0, padx=(0, 4)
        )
        ttk.Button(rr_btn_row, text="限价委托单", command=self._line_trading_desk_submit_rr_limit_order_from_tree).grid(
            row=0, column=1, padx=(0, 4)
        )
        ttk.Button(rr_btn_row, text="触发价委托", command=self._line_trading_desk_submit_rr_trigger_order_from_tree).grid(
            row=0, column=2, padx=(0, 4)
        )
        ttk.Button(rr_btn_row, text="删除选中", command=self._line_trading_desk_delete_selected_rr).grid(row=0, column=3, padx=(0, 8))
        param_rr_fee_offset = BooleanVar(value=True)
        ttk.Checkbutton(
            rr_btn_row,
            text="启用手续费偏移（按2倍Taker手续费留缓冲）",
            variable=param_rr_fee_offset,
        ).grid(row=0, column=4, padx=(10, 0), sticky="w")

        positions_box = ttk.LabelFrame(right, text="账户（持仓与委托）", padding=8)
        positions_box.grid(row=4, column=0, sticky="nsew")
        positions_box.columnconfigure(0, weight=1)
        positions_box.rowconfigure(0, weight=1)
        desk_orders_nb = ttk.Notebook(positions_box)
        desk_orders_nb.grid(row=0, column=0, sticky="nsew")

        tab_pos = ttk.Frame(desk_orders_nb)
        tab_pos.columnconfigure(0, weight=1)
        tab_pos.rowconfigure(0, weight=1)
        position_tree = ttk.Treeview(
            tab_pos,
            columns=("inst_id", "pos_side", "size", "avg_price", "mark_price", "upl"),
            show="headings",
            height=12,
        )
        for col, text, width in (
            ("inst_id", "合约", 160),
            ("pos_side", "方向", 70),
            ("size", "数量(币)", 80),
            ("avg_price", "开仓均价", 90),
            ("mark_price", "标记价", 90),
            ("upl", "浮盈亏", 90),
        ):
            position_tree.heading(col, text=text)
            position_tree.column(col, width=width, anchor="center")
        position_tree.grid(row=0, column=0, sticky="nsew")
        pos_y_scroll = ttk.Scrollbar(tab_pos, orient="vertical", command=position_tree.yview)
        pos_y_scroll.grid(row=0, column=1, sticky="ns")
        position_tree.configure(yscrollcommand=pos_y_scroll.set)
        param_close_qty = StringVar(value="")
        pos_close_bar = ttk.Frame(tab_pos)
        pos_close_bar.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(4, 0))
        ttk.Label(pos_close_bar, text="平仓数量(空=全平)").pack(side="left", padx=(0, 6))
        ttk.Entry(pos_close_bar, width=14, textvariable=param_close_qty).pack(side="left", padx=(0, 8))
        ttk.Button(
            pos_close_bar,
            text="按数量市价平仓",
            command=lambda: self._line_trading_desk_flatten_selected("market", use_qty_field=True),
        ).pack(side="left")
        desk_orders_nb.add(tab_pos, text="当前持仓")

        order_headings = (
            ("inst_id", "合约", 124),
            ("side", "方向", 40),
            ("ord_type", "类型", 56),
            ("px", "价格", 74),
            ("sz", "数量(币)", 92),
            ("filled", "已成交", 88),
            ("tp", "止盈", 128),
            ("sl", "止损", 128),
            ("state", "状态", 64),
            ("src", "来源", 44),
            ("ut", "更新", 86),
        )
        tab_pend = ttk.Frame(desk_orders_nb)
        tab_pend.columnconfigure(0, weight=1)
        tab_pend.rowconfigure(0, weight=1)
        pending_orders_tree = ttk.Treeview(tab_pend, columns=tuple(c[0] for c in order_headings), show="headings", height=10)
        for col, text, width in order_headings:
            pending_orders_tree.heading(col, text=text)
            pending_orders_tree.column(col, width=width, anchor="center")
        pending_orders_tree.grid(row=0, column=0, sticky="nsew")
        pend_y_scroll = ttk.Scrollbar(tab_pend, orient="vertical", command=pending_orders_tree.yview)
        pend_y_scroll.grid(row=0, column=1, sticky="ns")
        pending_orders_tree.configure(yscrollcommand=pend_y_scroll.set)
        pend_toolbar = ttk.Frame(tab_pend)
        pend_toolbar.grid(row=1, column=0, columnspan=2, sticky="w", pady=(4, 0))
        ttk.Button(pend_toolbar, text="刷新", command=self._line_trading_desk_refresh_pending_tab).pack(side="left")
        desk_orders_nb.add(tab_pend, text="当前委托")

        tab_hist = ttk.Frame(desk_orders_nb)
        tab_hist.columnconfigure(0, weight=1)
        tab_hist.rowconfigure(0, weight=1)
        order_history_tree = ttk.Treeview(tab_hist, columns=tuple(c[0] for c in order_headings), show="headings", height=10)
        for col, text, width in order_headings:
            order_history_tree.heading(col, text=text)
            order_history_tree.column(col, width=width, anchor="center")
        order_history_tree.grid(row=0, column=0, sticky="nsew")
        hist_y_scroll = ttk.Scrollbar(tab_hist, orient="vertical", command=order_history_tree.yview)
        hist_y_scroll.grid(row=0, column=1, sticky="ns")
        order_history_tree.configure(yscrollcommand=hist_y_scroll.set)
        hist_toolbar = ttk.Frame(tab_hist)
        hist_toolbar.grid(row=1, column=0, columnspan=2, sticky="w", pady=(4, 0))
        ttk.Button(hist_toolbar, text="刷新", command=self._line_trading_desk_refresh_order_history_tab).pack(side="left")
        desk_orders_nb.add(tab_hist, text="历史委托")

        action_row = ttk.Frame(right)
        action_row.grid(row=5, column=0, sticky="ew", pady=(6, 0))
        ttk.Button(action_row, text="市价平仓选中", command=lambda: self._line_trading_desk_flatten_selected("market")).grid(
            row=0, column=0, padx=(0, 4)
        )
        ttk.Button(action_row, text="挂买一/卖一平仓", command=lambda: self._line_trading_desk_flatten_selected("best_quote")).grid(
            row=0, column=1, padx=(0, 4)
        )
        ttk.Button(action_row, text="撤销委托", command=self._line_trading_desk_cancel_selected_pending_order).grid(
            row=0, column=2, padx=(0, 4)
        )
        ttk.Button(action_row, text="关闭", command=self._close_line_trading_desk_window).grid(row=0, column=3)

        state = LineTradingDeskWindowState(
            window=window,
            canvas=canvas,
            symbol_var=symbol_var,
            bar_var=bar_var,
            status_text=status_text,
            position_tree=position_tree,
            pending_orders_tree=pending_orders_tree,
            order_history_tree=order_history_tree,
            api_profile_var=api_profile_var,
            param_atr_period=param_atr_period,
            param_atr_mult=param_atr_mult,
            param_risk_amount=param_risk_amount,
            param_cross_mode=param_cross_mode,
            param_ray_action=param_ray_action,
            param_order_mode=param_order_mode,
            param_rr_r=param_rr_r,
            param_rr_side=param_rr_side,
            param_rr_fee_offset=param_rr_fee_offset,
            param_close_qty=param_close_qty,
            ray_tree=ray_tree,
            rr_manage_tree=rr_manage_tree,
            desk_visible_bars=200,
            desk_view_start=0,
            last_desk_api_profile=api_profile_var.get().strip(),
            desk_log_text=desk_log_text,
        )
        self._line_trading_desk_window = state
        self._line_trading_desk_local_log(
            state,
            "工作台已就绪；画线、区间放大、射线触发、盈亏比调整及本台下单会记录在此。",
        )
        self._line_trading_desk_update_price_tick(state)
        rr_manage_tree.bind("<<TreeviewSelect>>", lambda _e: self._line_trading_desk_on_rr_tree_select())
        window.protocol("WM_DELETE_WINDOW", self._close_line_trading_desk_window)
        canvas.bind("<Configure>", self._on_line_trading_desk_canvas_configure)
        canvas.bind("<ButtonPress-1>", self._on_line_trading_desk_button_press)
        canvas.bind("<B1-Motion>", self._on_line_trading_desk_mouse_move)
        canvas.bind("<ButtonRelease-1>", self._on_line_trading_desk_button_release)
        canvas.bind("<MouseWheel>", self._on_line_trading_desk_mouse_wheel)
        canvas.bind("<Button-4>", self._on_line_trading_desk_mouse_wheel)
        canvas.bind("<Button-5>", self._on_line_trading_desk_mouse_wheel)
        canvas.bind("<Enter>", lambda e: e.widget.focus_set())
        canvas.bind("<Map>", self._on_line_trading_desk_canvas_map_event, add="+")
        symbol_var.trace_add("write", lambda *_: self._on_line_trading_desk_symbol_or_bar_changed())
        bar_var.trace_add("write", lambda *_: self._on_line_trading_desk_symbol_or_bar_changed())
        api_profile_var.trace_add("write", lambda *_: self._on_line_trading_desk_api_profile_changed())
        for _ in range(4):
            try:
                window.update_idletasks()
            except Exception:
                break
        try:
            window.wait_visibility()
        except Exception:
            pass
        # 先让窗口完成布局与首帧绘制，再拉数据，避免打开瞬间主线程与网络叠加大卡顿。
        self.root.after(16, lambda: self._request_line_trading_desk_refresh(immediate=True))

    def open_signal_monitor_window(self) -> None:
        if self._signal_monitor_window is not None and self._signal_monitor_window.window.winfo_exists():
            self._signal_monitor_window.show()
            return

        self._signal_monitor_window = SignalMonitorWindow(
            self.root,
            logger=self._enqueue_log,
            current_template_factory=lambda: self._template_record_from_launcher(force_run_mode="signal_only"),
            template_serializer=_build_strategy_template_payload_from_record,
            template_deserializer=_strategy_template_record_from_payload,
            template_symbol_cloner=self._clone_template_record_for_symbol,
            template_launcher=lambda record, source_label: self._launch_strategy_template_record(
                record,
                source_label=f"信号观察台[{source_label}]",
                ask_confirm=False,
            ),
            session_provider=self._signal_observer_session_rows,
            session_stopper=self._stop_sessions_by_id,
            session_deleter=self._delete_signal_observer_sessions_by_id,
            session_log_opener=self.open_strategy_session_log,
            session_chart_opener=self.open_strategy_live_chart_window,
        )

    def open_auto_channel_preview_window(self) -> None:
        existing = self._auto_channel_preview_window
        if existing is not None and _widget_exists(existing):
            existing.deiconify()
            existing.lift()
            existing.focus_force()
            return

        window = Toplevel(self.root)
        self._auto_channel_preview_window = window
        apply_window_icon(window)
        window.title("自动通道预览")
        apply_adaptive_window_geometry(
            window,
            width_ratio=0.78,
            height_ratio=0.7,
            min_width=1120,
            min_height=720,
            max_width=1800,
            max_height=1160,
        )
        container = ttk.Frame(window, padding=10)
        container.pack(fill="both", expand=True)
        container.columnconfigure(0, weight=1)
        container.rowconfigure(3, weight=1)

        top_bar = ttk.Frame(container)
        top_bar.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        ttk.Label(top_bar, text="样例").grid(row=0, column=0, sticky="w")
        sample_var = StringVar(value="通道样例")
        sample_box = ttk.Combobox(
            top_bar,
            width=12,
            state="readonly",
            textvariable=sample_var,
            values=("通道样例", "箱体样例"),
        )
        sample_box.grid(row=0, column=1, padx=(6, 10))
        ttk.Label(top_bar, text="标的").grid(row=0, column=2, sticky="w")
        symbol_var = StringVar(value=self._default_line_trading_symbol())
        ttk.Combobox(top_bar, width=18, textvariable=symbol_var, values=self._line_trading_symbol_values()).grid(
            row=0, column=3, padx=(6, 10)
        )
        ttk.Label(top_bar, text="周期").grid(row=0, column=4, sticky="w")
        bar_var = StringVar(value="1H")
        ttk.Combobox(top_bar, width=8, state="readonly", values=BAR_OPTIONS, textvariable=bar_var).grid(
            row=0, column=5, padx=(6, 10)
        )
        ttk.Label(top_bar, text="K线").grid(row=0, column=6, sticky="w")
        limit_var = StringVar(value="240")
        ttk.Entry(top_bar, width=7, textvariable=limit_var).grid(row=0, column=7, padx=(6, 10))
        status_text = StringVar(value="自动通道预览：仅用于看图验证，不会下单。")
        top_bar.columnconfigure(10, weight=1)

        param_bar = ttk.LabelFrame(container, text="识别参数", padding=(10, 8))
        param_bar.grid(row=1, column=0, sticky="ew", pady=(0, 8))
        pivot_left_var = StringVar(value="1")
        pivot_right_var = StringVar(value="1")
        pivot_gap_var = StringVar(value="1")
        atr_period_var = StringVar(value="3")
        atr_multiplier_var = StringVar(value="0")
        min_anchor_var = StringVar(value="6")
        min_span_var = StringVar(value="18")
        max_violations_var = StringVar(value="8")
        show_channels_var = BooleanVar(value=True)
        show_boxes_var = BooleanVar(value=True)
        show_trendlines_var = BooleanVar(value=True)
        show_triangles_var = BooleanVar(value=True)
        show_pivots_var = BooleanVar(value=True)

        ttk.Label(param_bar, text="Pivot左").grid(row=0, column=0, sticky="w")
        ttk.Entry(param_bar, width=5, textvariable=pivot_left_var).grid(row=0, column=1, padx=(6, 10))
        ttk.Label(param_bar, text="Pivot右").grid(row=0, column=2, sticky="w")
        ttk.Entry(param_bar, width=5, textvariable=pivot_right_var).grid(row=0, column=3, padx=(6, 10))
        ttk.Label(param_bar, text="最小间距").grid(row=0, column=4, sticky="w")
        ttk.Entry(param_bar, width=5, textvariable=pivot_gap_var).grid(row=0, column=5, padx=(6, 10))
        ttk.Label(param_bar, text="ATR周期").grid(row=0, column=6, sticky="w")
        ttk.Entry(param_bar, width=6, textvariable=atr_period_var).grid(row=0, column=7, padx=(6, 10))
        ttk.Label(param_bar, text="ATR过滤").grid(row=0, column=8, sticky="w")
        ttk.Entry(param_bar, width=7, textvariable=atr_multiplier_var).grid(row=0, column=9, padx=(6, 10))
        ttk.Label(param_bar, text="最小跨度").grid(row=0, column=10, sticky="w")
        ttk.Entry(param_bar, width=6, textvariable=min_span_var).grid(row=0, column=11, padx=(6, 10))

        ttk.Label(param_bar, text="锚点间距").grid(row=1, column=0, sticky="w", pady=(8, 0))
        ttk.Entry(param_bar, width=5, textvariable=min_anchor_var).grid(row=1, column=1, padx=(6, 10), pady=(8, 0))
        ttk.Label(param_bar, text="最大穿越").grid(row=1, column=2, sticky="w", pady=(8, 0))
        ttk.Entry(param_bar, width=5, textvariable=max_violations_var).grid(
            row=1,
            column=3,
            padx=(6, 10),
            pady=(8, 0),
        )
        ttk.Checkbutton(param_bar, text="通道", variable=show_channels_var).grid(row=1, column=4, sticky="w", pady=(8, 0))
        ttk.Checkbutton(param_bar, text="箱体", variable=show_boxes_var).grid(row=1, column=5, sticky="w", pady=(8, 0))
        ttk.Checkbutton(param_bar, text="趋势线", variable=show_trendlines_var).grid(
            row=1,
            column=6,
            sticky="w",
            pady=(8, 0),
        )
        ttk.Checkbutton(param_bar, text="三角形", variable=show_triangles_var).grid(
            row=1,
            column=7,
            sticky="w",
            pady=(8, 0),
        )
        ttk.Checkbutton(param_bar, text="枢轴点", variable=show_pivots_var).grid(row=1, column=8, sticky="w", pady=(8, 0))
        param_bar.columnconfigure(12, weight=1)

        history_bar = ttk.Frame(container)
        history_bar.grid(row=2, column=0, sticky="ew", pady=(0, 8))
        ttk.Label(history_bar, text="历史划线").grid(row=0, column=0, sticky="w")
        saved_var = StringVar(value="")
        saved_combo = ttk.Combobox(history_bar, width=42, textvariable=saved_var, state="readonly")
        saved_combo.grid(row=0, column=1, padx=(6, 8))
        history_bar.columnconfigure(4, weight=1)

        canvas = Canvas(container, background="#ffffff", highlightthickness=0, width=1120, height=640)
        canvas.grid(row=3, column=0, sticky="nsew")
        snapshot_box: dict[str, StrategyLiveChartSnapshot | None] = {"snapshot": None}
        load_generation_box = {"value": 0}
        append_generation_box = {"value": 0}
        append_after_box: dict[str, str | None] = {"job": None}
        saved_records_box = {"records": load_auto_channel_snapshots()}
        source_state: dict[str, object] = {
            "mode": "sample",
            "symbol": "",
            "bar": "",
            "limit": 0,
            "api_profile": "",
            "label": "",
        }

        def sample_key() -> str:
            return "box" if sample_var.get() == "箱体样例" else "channel"

        def parse_positive_int(raw: str, label: str) -> int:
            try:
                value = int(raw.strip())
            except ValueError as exc:
                raise ValueError(f"{label}必须是整数。") from exc
            if value <= 0:
                raise ValueError(f"{label}必须大于 0。")
            return value

        def parse_nonnegative_int(raw: str, label: str) -> int:
            try:
                value = int(raw.strip())
            except ValueError as exc:
                raise ValueError(f"{label}必须是整数。") from exc
            if value < 0:
                raise ValueError(f"{label}不能小于 0。")
            return value

        def parse_nonnegative_decimal(raw: str, label: str) -> Decimal:
            try:
                value = Decimal(raw.strip())
            except (InvalidOperation, ValueError) as exc:
                raise ValueError(f"{label}必须是数字。") from exc
            if value < 0:
                raise ValueError(f"{label}不能小于 0。")
            return value

        def build_detection_settings() -> dict[str, object]:
            pivot = PivotDetectionConfig(
                left_bars=parse_positive_int(pivot_left_var.get(), "Pivot左"),
                right_bars=parse_positive_int(pivot_right_var.get(), "Pivot右"),
                atr_period=parse_positive_int(atr_period_var.get(), "ATR周期"),
                atr_multiplier=parse_nonnegative_decimal(atr_multiplier_var.get(), "ATR过滤"),
                min_index_distance=parse_nonnegative_int(pivot_gap_var.get(), "最小间距"),
            )
            min_anchor_distance = parse_positive_int(min_anchor_var.get(), "锚点间距")
            min_span = parse_positive_int(min_span_var.get(), "最小跨度")
            max_violations = parse_nonnegative_int(max_violations_var.get(), "最大穿越")
            return {
                "channel_config": ChannelDetectionConfig(
                    pivot=pivot,
                    min_anchor_distance=min_anchor_distance,
                    min_channel_bars=max(3, min_span),
                    max_violations=max_violations,
                ),
                "box_config": BoxDetectionConfig(
                    pivot=pivot,
                    min_box_bars=max(3, min_span),
                    min_touches_per_side=2,
                    max_violations=max_violations,
                ),
                "trendline_config": TrendlineDetectionConfig(
                    pivot=pivot,
                    min_anchor_distance=min_anchor_distance,
                    min_line_bars=max(3, min_span),
                    max_violations=max_violations,
                ),
                "triangle_config": TriangleDetectionConfig(
                    pivot=pivot,
                    min_anchor_distance=max(2, min_anchor_distance),
                    min_triangle_bars=max(3, min_span),
                    max_violations=max_violations,
                ),
                "show_channels": bool(show_channels_var.get()),
                "show_boxes": bool(show_boxes_var.get()),
                "show_trendlines": bool(show_trendlines_var.get()),
                "show_triangles": bool(show_triangles_var.get()),
                "show_pivots": bool(show_pivots_var.get()),
            }

        def apply_parameter_preset(preset: str) -> None:
            if preset == "conservative":
                pivot_left_var.set("3")
                pivot_right_var.set("3")
                pivot_gap_var.set("2")
                atr_period_var.set("14")
                atr_multiplier_var.set("0.5")
                min_anchor_var.set("8")
                min_span_var.set("28")
                max_violations_var.set("4")
                label = "保守"
            elif preset == "aggressive":
                pivot_left_var.set("1")
                pivot_right_var.set("1")
                pivot_gap_var.set("1")
                atr_period_var.set("7")
                atr_multiplier_var.set("0")
                min_anchor_var.set("4")
                min_span_var.set("14")
                max_violations_var.set("10")
                label = "激进"
            else:
                pivot_left_var.set("2")
                pivot_right_var.set("2")
                pivot_gap_var.set("2")
                atr_period_var.set("14")
                atr_multiplier_var.set("0.25")
                min_anchor_var.set("6")
                min_span_var.set("20")
                max_violations_var.set("6")
                label = "平衡"
            show_channels_var.set(True)
            show_boxes_var.set(True)
            show_trendlines_var.set(True)
            show_triangles_var.set(True)
            show_pivots_var.set(True)
            status_text.set(f"已切换到{label}预设。图形暂不变化，点击“按当前参数重算”后生效。")

        def build_snapshot_from_candles(
            *,
            session_id: str,
            candles: list[Candle] | tuple[Candle, ...],
            note: str,
            settings: dict[str, object] | None = None,
        ) -> StrategyLiveChartSnapshot:
            resolved_settings = settings or build_detection_settings()
            candle_items = list(candles)
            return build_auto_channel_live_chart_snapshot(
                session_id=session_id,
                candles=candle_items,
                channel_config=resolved_settings["channel_config"],
                box_config=resolved_settings["box_config"],
                trendline_config=resolved_settings["trendline_config"],
                triangle_config=resolved_settings["triangle_config"],
                max_channels=1 if resolved_settings["show_channels"] else 0,
                max_boxes=1 if resolved_settings["show_boxes"] else 0,
                max_trendlines=2 if resolved_settings["show_trendlines"] else 0,
                max_triangles=1 if resolved_settings["show_triangles"] else 0,
                show_pivots=bool(resolved_settings["show_pivots"]),
                right_pad_bars=50,
                channel_extend_bars=50,
                latest_price=candle_items[-1].close if candle_items else None,
                note=note,
            )

        def format_candle_ts(value: object) -> str:
            try:
                normalized = int(value)
            except (TypeError, ValueError):
                return "-" if value in (None, "") else str(value)
            try:
                return datetime.fromtimestamp(normalized / 1000).strftime("%Y-%m-%d %H:%M")
            except Exception:
                return str(normalized)

        def format_saved_at(value: object) -> str:
            raw = str(value or "").strip()
            if not raw:
                return ""
            try:
                normalized = raw.replace("Z", "+00:00")
                return datetime.fromisoformat(normalized).astimezone().strftime("%m-%d %H:%M")
            except Exception:
                return raw

        def record_option_text(record: dict[str, object]) -> str:
            label = str(record.get("label", "") or "").strip() or "未命名"
            symbol = str(record.get("symbol", "") or "").strip()
            bar = str(record.get("bar", "") or "").strip()
            last_candle_text = format_candle_ts(record.get("last_candle_ts"))
            saved_at = format_saved_at(record.get("saved_at"))
            if symbol and bar:
                base = f"{symbol} | {bar} | {last_candle_text}"
            else:
                base = label
            if saved_at:
                return f"{base} | 保存 {saved_at}"
            return base

        def refresh_saved_options(select_record_id: str | None = None) -> None:
            records = saved_records_box["records"]
            values = [record_option_text(item) for item in records]
            saved_combo.configure(values=values)
            if not values:
                saved_var.set("")
                return
            if select_record_id:
                for item, text in zip(records, values):
                    if str(item.get("record_id", "")) == select_record_id:
                        saved_var.set(text)
                        return
            if saved_var.get() not in values:
                saved_var.set(values[0])

        def redraw(_event=None) -> None:
            snapshot = snapshot_box.get("snapshot")
            if snapshot is not None:
                render_strategy_live_chart(canvas, snapshot)

        def cancel_auto_append() -> None:
            append_generation_box["value"] += 1
            job = append_after_box.get("job")
            if job is not None:
                try:
                    self.root.after_cancel(job)
                except Exception:
                    pass
            append_after_box["job"] = None

        def schedule_auto_append() -> None:
            if source_state.get("mode") != "real":
                return
            generation = append_generation_box["value"]

            def kick() -> None:
                run_auto_append_poll(generation)

            append_after_box["job"] = self.root.after(15_000, kick)

        def run_auto_append_poll(generation: int) -> None:
            if generation != append_generation_box["value"] or source_state.get("mode") != "real":
                return
            symbol = str(source_state.get("symbol", "") or "")
            bar = str(source_state.get("bar", "") or "")
            limit = int(source_state.get("limit", 0) or 0)
            if not symbol or not bar or limit <= 0:
                return

            def worker() -> None:
                try:
                    candles = self.client.get_candles_history(symbol, bar, limit=limit)
                    error = None
                except Exception as exc:
                    candles = None
                    error = str(exc)

                def apply_result() -> None:
                    if generation != append_generation_box["value"] or not _widget_exists(window):
                        return
                    append_after_box["job"] = None
                    if error is not None:
                        status_text.set(f"追加K线失败：{error}")
                        schedule_auto_append()
                        return
                    current = snapshot_box.get("snapshot")
                    if current is not None and candles:
                        snapshot_box["snapshot"] = append_candles_to_snapshot(current, candles)
                        redraw()
                    schedule_auto_append()

                self.root.after(0, apply_result)

            threading.Thread(target=worker, daemon=True, name="auto-channel-preview-append").start()

        def refresh_sample() -> None:
            load_generation_box["value"] += 1
            cancel_auto_append()
            try:
                snapshot = build_snapshot_from_candles(
                    session_id=f"auto-{sample_key()}",
                    candles=sample_auto_channel_candles(sample_key()),
                    note="自动通道预览",
                )
            except ValueError as exc:
                messagebox.showerror("参数错误", str(exc), parent=window)
                return
            snapshot_box["snapshot"] = snapshot
            source_state.update(
                {
                    "mode": "sample",
                    "symbol": "",
                    "bar": "",
                    "limit": 0,
                    "api_profile": "",
                    "label": sample_var.get(),
                }
            )
            status_text.set(snapshot.note or "自动通道预览已刷新")
            redraw()

        def save_current_snapshot() -> None:
            snapshot = snapshot_box.get("snapshot")
            if snapshot is None or not snapshot.candles:
                messagebox.showwarning("无法保存", "当前没有可保存的结构图。", parent=window)
                return
            symbol = str(source_state.get("symbol", "") or symbol_var.get().strip().upper() or sample_key())
            bar = str(source_state.get("bar", "") or bar_var.get().strip() or "-")
            last_ts = snapshot.candles[-1].ts
            default_label = f"{symbol} | {bar} | {format_candle_ts(last_ts)}"
            label = simpledialog.askstring("保存结构", "请输入保存名称：", initialvalue=default_label, parent=window)
            if label is None:
                return
            record = build_auto_channel_snapshot_record(
                snapshot=snapshot,
                source_mode=str(source_state.get("mode", "sample") or "sample"),
                symbol=symbol,
                bar=bar,
                label=label,
                api_profile=str(source_state.get("api_profile", "") or ""),
                candle_limit=int(source_state.get("limit", 0) or 0),
            )
            saved_records_box["records"].insert(0, record)
            save_auto_channel_snapshots(saved_records_box["records"])
            refresh_saved_options(str(record.get("record_id", "")))
            status_text.set(f"已保存结构 | {label} | 最后K线={format_candle_ts(last_ts)}")

        def load_saved_snapshot() -> None:
            selected = saved_var.get().strip()
            if not selected:
                messagebox.showwarning("无法加载", "请先选择一条历史划线。", parent=window)
                return
            record = next((item for item in saved_records_box["records"] if record_option_text(item) == selected), None)
            if record is None:
                messagebox.showwarning("无法加载", "没有找到对应的历史划线记录。", parent=window)
                return
            payload = record.get("snapshot")
            if not isinstance(payload, dict):
                messagebox.showwarning("无法加载", "该记录缺少图表快照。", parent=window)
                return
            load_generation_box["value"] += 1
            cancel_auto_append()
            snapshot = deserialize_strategy_live_chart_snapshot(payload)
            snapshot_box["snapshot"] = snapshot
            source_state.update(
                {
                    "mode": "saved",
                    "symbol": str(record.get("symbol", "") or ""),
                    "bar": str(record.get("bar", "") or ""),
                    "limit": int(record.get("candle_limit", 0) or 0),
                    "api_profile": str(record.get("api_profile", "") or ""),
                    "label": str(record.get("label", "") or ""),
                }
            )
            if source_state["symbol"]:
                symbol_var.set(str(source_state["symbol"]))
            if source_state["bar"]:
                bar_var.set(str(source_state["bar"]))
            if source_state["limit"]:
                limit_var.set(str(source_state["limit"]))
            status_text.set(
                f"已加载历史结构 | {record.get('label', '')} | 最后K线={format_candle_ts(record.get('last_candle_ts'))}"
            )
            redraw()

        def rebuild_current_snapshot() -> None:
            mode = str(source_state.get("mode", "") or "")
            if mode == "real":
                load_real_candles()
                return
            if mode == "sample":
                refresh_sample()
                return
            snapshot = snapshot_box.get("snapshot")
            if snapshot is None or not snapshot.candles:
                messagebox.showwarning("无法重算", "当前没有可重算的K线数据。", parent=window)
                return
            load_generation_box["value"] += 1
            cancel_auto_append()
            try:
                rebuilt = build_snapshot_from_candles(
                    session_id=f"auto-saved:{source_state.get('label', 'snapshot')}",
                    candles=snapshot.candles,
                    note=str(source_state.get("label", "历史结构") or "历史结构"),
                )
            except ValueError as exc:
                messagebox.showerror("参数错误", str(exc), parent=window)
                return
            snapshot_box["snapshot"] = rebuilt
            status_text.set(f"已按当前参数重算 | {source_state.get('label', '历史结构')} | 图形仍保持冻结。")
            redraw()

        def load_real_candles() -> None:
            load_generation_box["value"] += 1
            cancel_auto_append()
            generation = load_generation_box["value"]
            symbol = symbol_var.get().strip().upper()
            bar = bar_var.get().strip()
            try:
                limit = max(50, min(1000, int(limit_var.get().strip())))
            except ValueError:
                messagebox.showerror("参数错误", "K线数量必须是整数。", parent=window)
                return
            if not symbol:
                messagebox.showerror("参数错误", "请填写标的。", parent=window)
                return
            try:
                detection_settings = build_detection_settings()
            except ValueError as exc:
                messagebox.showerror("参数错误", str(exc), parent=window)
                return
            limit_var.set(str(limit))
            status_text.set(f"正在加载真实K线 | {symbol} | {bar} | {limit} 根...")

            def worker() -> None:
                try:
                    candles = self.client.get_candles_history(symbol, bar, limit=limit)
                    snapshot = build_snapshot_from_candles(
                        session_id=f"auto-live:{symbol}:{bar}",
                        candles=candles,
                        note=f"{symbol} | {bar} | 真实K线",
                        settings=detection_settings,
                    )
                    error = None
                except Exception as exc:
                    snapshot = None
                    error = str(exc)

                def apply_result() -> None:
                    if load_generation_box["value"] != generation or not _widget_exists(window):
                        return
                    if error is not None:
                        status_text.set(f"真实K线加载失败：{error}")
                        self._enqueue_log(f"自动通道预览加载失败 | {symbol} | {bar} | {error}")
                        return
                    snapshot_box["snapshot"] = snapshot
                    source_state.update(
                        {
                            "mode": "real",
                            "symbol": symbol,
                            "bar": bar,
                            "limit": limit,
                            "api_profile": self._current_credential_profile(),
                            "label": f"{symbol} | {bar}",
                        }
                    )
                    if snapshot is None:
                        status_text.set("真实K线加载完成，但没有可绘制数据。")
                    else:
                        status_text.set((snapshot.note or f"已加载真实K线 | {symbol} | {bar}") + " | 结构冻结，后续仅追加K线。")
                        schedule_auto_append()
                    redraw()

                self.root.after(0, apply_result)

            threading.Thread(target=worker, daemon=True, name="auto-channel-preview-load").start()

        ttk.Button(top_bar, text="刷新样例", command=refresh_sample).grid(row=0, column=8, padx=(0, 8))
        ttk.Button(top_bar, text="加载真实K线", command=load_real_candles).grid(row=0, column=9, padx=(0, 8))
        ttk.Label(top_bar, textvariable=status_text, foreground="#555").grid(row=0, column=10, sticky="w")
        ttk.Button(param_bar, text="保守", command=lambda: apply_parameter_preset("conservative")).grid(
            row=2,
            column=0,
            padx=(0, 8),
            pady=(10, 0),
            sticky="w",
        )
        ttk.Button(param_bar, text="平衡", command=lambda: apply_parameter_preset("balanced")).grid(
            row=2,
            column=1,
            padx=(0, 8),
            pady=(10, 0),
            sticky="w",
        )
        ttk.Button(param_bar, text="激进", command=lambda: apply_parameter_preset("aggressive")).grid(
            row=2,
            column=2,
            padx=(0, 12),
            pady=(10, 0),
            sticky="w",
        )
        ttk.Label(param_bar, text="预设只改参数，不会自动改图。", foreground="#666").grid(
            row=2,
            column=3,
            columnspan=4,
            sticky="w",
            pady=(10, 0),
        )
        ttk.Button(param_bar, text="按当前参数重算", command=rebuild_current_snapshot).grid(
            row=1,
            column=9,
            padx=(12, 8),
            pady=(8, 0),
            sticky="w",
        )
        ttk.Label(param_bar, text="参数变更后只有手动重算才会改图。", foreground="#666").grid(
            row=1,
            column=10,
            columnspan=3,
            sticky="w",
            pady=(8, 0),
        )
        ttk.Button(history_bar, text="保存当前结构", command=save_current_snapshot).grid(row=0, column=2, padx=(0, 8))
        ttk.Button(history_bar, text="加载历史结构", command=load_saved_snapshot).grid(row=0, column=3, padx=(0, 8))
        ttk.Label(history_bar, text="保存会带上最后一根K线时间。", foreground="#666").grid(row=0, column=4, sticky="w")

        def close() -> None:
            load_generation_box["value"] += 1
            cancel_auto_append()
            self._auto_channel_preview_window = None
            window.destroy()

        sample_box.bind("<<ComboboxSelected>>", lambda _event: refresh_sample())
        canvas.bind("<Configure>", redraw)
        window.protocol("WM_DELETE_WINDOW", close)
        refresh_saved_options()
        self.root.after(50, refresh_sample)

    def open_btc_research_workbench_window(self) -> None:
        if (
            self._btc_research_workbench_window is not None
            and self._btc_research_workbench_window.window.winfo_exists()
        ):
            self._btc_research_workbench_window.show()
            return

        self._btc_research_workbench_window = BtcResearchWorkbenchWindow(
            self.root,
            client=self.client,
            deribit_client=self.deribit_client,
            logger=self._enqueue_log,
        )

    def open_journal_window(self) -> None:
        if self._journal_window is not None and self._journal_window.window.winfo_exists():
            self._journal_window.show()
            return

        self._journal_window = JournalWindow(
            self.root,
            logger=self._enqueue_log,
        )

    def open_signal_replay_mock_window(self) -> None:
        if self._signal_replay_mock_window is not None and self._signal_replay_mock_window.window.winfo_exists():
            self._signal_replay_mock_window.show()
            return

        self._signal_replay_mock_window = SignalReplayMockWindow(
            self.root,
            client=self.client,
            logger=self._enqueue_log,
        )

    def open_trader_desk_window(self) -> None:
        if self._trader_desk_window is not None and self._trader_desk_window.window.winfo_exists():
            self._trader_desk_window.show()
            return

        self._trader_desk_window = TraderDeskWindow(
            self.root,
            logger=self._enqueue_log,
            current_template_factory=self._template_record_from_launcher,
            template_serializer=_build_strategy_template_payload_from_record,
            template_deserializer=_strategy_template_record_from_payload,
            template_target_cloner=self._clone_template_record_for_targets,
            snapshot_provider=self._trader_desk_snapshot_for_ui,
            draft_saver=self._save_trader_desk_draft,
            draft_deleter=self._delete_trader_desk_draft,
            trader_starter=self.start_trader_draft,
            trader_pauser=self.pause_trader_draft,
            trader_resumer=self.resume_trader_draft,
            trader_flattener=lambda trader_id, flatten_mode="market": self.flatten_trader_draft(
                trader_id,
                flatten_mode=flatten_mode,
            ),
            trader_force_cleaner=self.force_clear_trader_draft,
            symbol_provider=self._trader_desk_symbol_choices,
            runtime_snapshot_provider=self._trader_runtime_snapshot_for_ui,
            session_log_opener=self.open_strategy_session_log,
            session_chart_opener=self.open_strategy_live_chart_window,
        )

    def open_trader_desk_window_for_trader(self, trader_id: str) -> None:
        normalized = str(trader_id or "").strip()
        if not normalized:
            self.open_trader_desk_window()
            return
        self.open_trader_desk_window()
        window = self._trader_desk_window
        if window is None:
            return
        try:
            window._refresh_views(select_id=normalized)
            window._focus_trader_row(normalized)
        except Exception:
            pass

    def open_deribit_volatility_monitor_window(self) -> None:
        if (
            self._deribit_volatility_monitor_window is not None
            and self._deribit_volatility_monitor_window.window.winfo_exists()
        ):
            self._deribit_volatility_monitor_window.show()
            return

        self._deribit_volatility_monitor_window = DeribitVolatilityMonitorWindow(
            self.root,
            self.deribit_client,
            notifier_factory=self._build_signal_monitor_notifier,
            api_name_provider=self._current_credential_profile,
            logger=self._enqueue_log,
        )

    def open_deribit_volatility_window(self) -> None:
        if self._deribit_volatility_window is not None and self._deribit_volatility_window.window.winfo_exists():
            self._deribit_volatility_window.show()
            return

        self._deribit_volatility_window = DeribitVolatilityWindow(
            self.root,
            self.deribit_client,
            market_client=self.client,
            logger=self._enqueue_log,
        )

    def open_option_strategy_window(self) -> None:
        if self._option_strategy_window is not None and self._option_strategy_window.window.winfo_exists():
            self._option_strategy_window.show()
            return

        self._option_strategy_window = OptionStrategyCalculatorWindow(
            self.root,
            self.client,
            runtime_provider=self._build_option_strategy_runtime_or_none,
            logger=self._enqueue_log,
        )

    def open_option_roll_window(self) -> None:
        position = self._selected_option_position()
        if position is None:
            messagebox.showinfo("提示", "请先在账户持仓中选中一条期权持仓。", parent=self.root)
            return
        if position.inst_type != "OPTION":
            messagebox.showinfo("提示", "展期建议目前只支持期权持仓。", parent=self.root)
            return
        if not is_short_option_position(position):
            messagebox.showinfo("提示", "展期建议第一版只支持期权卖出方持仓。", parent=self.root)
            return

        instrument = self._position_instruments.get(position.inst_id)
        if instrument is None:
            try:
                instrument = self.client.get_instrument(position.inst_id)
            except Exception as exc:
                messagebox.showerror("打开失败", f"读取合约信息失败：{exc}", parent=self.root)
                return

        ticker = self._position_tickers.get(position.inst_id)
        if ticker is None:
            try:
                ticker = self.client.get_ticker(position.inst_id)
            except Exception as exc:
                messagebox.showerror("打开失败", f"读取行情失败：{exc}", parent=self.root)
                return

        quote = _build_option_quote(instrument, ticker)
        api_name = self._current_credential_profile()

        def _send_to_option_strategy(payload):
            if self._option_strategy_window is None or not self._option_strategy_window.window.winfo_exists():
                self._option_strategy_window = OptionStrategyCalculatorWindow(
                    self.root,
                    self.client,
                    runtime_provider=self._build_option_strategy_runtime_or_none,
                    logger=self._enqueue_log,
                )
            self._option_strategy_window.load_roll_transfer_payload(payload)

        if self._option_roll_window is not None and self._option_roll_window.window.winfo_exists():
            self._option_roll_window.load_position(
                position=position,
                instrument=instrument,
                quote=quote,
                api_name=api_name,
                auto_scan=True,
            )
            self._option_roll_window.show()
            return

        self._option_roll_window = OptionRollSuggestionWindow(
            self.root,
            self.client,
            position=position,
            instrument=instrument,
            quote=quote,
            api_name=api_name,
            send_to_strategy_callback=_send_to_option_strategy,
            logger=self._enqueue_log,
        )

    def _close_settings_window(self) -> None:
        self._save_credentials_now(silent=True)
        self._save_notification_settings_now(silent=True)
        if self._settings_window is not None and self._settings_window.winfo_exists():
            self._settings_window.destroy()
        self._settings_window = None
        self._credential_profile_combo = None

    def _credential_profile_names(self) -> list[str]:
        if not self._credential_profiles:
            return [DEFAULT_CREDENTIAL_PROFILE_NAME]
        return sorted(self._credential_profiles.keys())

    def _startup_credential_profile_name(self, selected_profile: str) -> str:
        preferred = PREFERRED_STARTUP_CREDENTIAL_PROFILE_NAME.strip()
        if preferred and preferred in self._credential_profiles:
            return preferred
        target = selected_profile.strip()
        if target in self._credential_profiles:
            return target
        return self._credential_profile_names()[0]

    def _current_credential_profile(self) -> str:
        profile_name = self.api_profile_name.get().strip()
        if profile_name:
            return profile_name
        return self._credential_profile_names()[0]

    def _editing_credential_profile(self) -> str:
        profile_name = self._loaded_credential_profile_name.strip()
        if profile_name:
            return profile_name
        return self._current_credential_profile()

    def _normalized_environment_label(self, label: str | None, *, fallback: str | None = None) -> str:
        candidate = (label or "").strip()
        if candidate in ENV_OPTIONS:
            return candidate
        fallback_candidate = (fallback or "").strip()
        if fallback_candidate in ENV_OPTIONS:
            return fallback_candidate
        default_candidate = getattr(self, "_default_environment_label", "").strip()
        if default_candidate in ENV_OPTIONS:
            return default_candidate
        return next(iter(ENV_OPTIONS))

    def _environment_value_from_label(self, label: str | None) -> str:
        return ENV_OPTIONS[self._normalized_environment_label(label)]

    def _environment_label_for_profile(self, profile_name: str) -> str:
        snapshot = self._credential_profiles.get(profile_name, {})
        environment = str(snapshot.get("environment", "")).strip().lower()
        if environment == "live":
            return "实盘 live"
        if environment == "demo":
            return "模拟盘 demo"
        return self._normalized_environment_label(None)

    def _apply_profile_environment(self, profile_name: str) -> None:
        self._positions_effective_environment = None
        self.environment_label.set(self._environment_label_for_profile(profile_name))

    def _current_credentials_state(self) -> tuple[str, str, str, str, str]:
        return (
            self._editing_credential_profile(),
            self.api_key.get().strip(),
            self.secret_key.get().strip(),
            self.passphrase.get().strip(),
            self._environment_value_from_label(self.environment_label.get()),
        )

    def _set_credentials_fields(self, snapshot: dict[str, str]) -> None:
        was_enabled = self._credential_watch_enabled
        self._credential_watch_enabled = False
        self.api_key.set(snapshot["api_key"])
        self.secret_key.set(snapshot["secret_key"])
        self.passphrase.set(snapshot["passphrase"])
        self._credential_watch_enabled = was_enabled

    def _sync_credential_profile_combo(self) -> None:
        values = self._credential_profile_names()
        header_width = max(8, min(14, max((len(item) for item in values), default=8) + 1))
        if self._header_credential_profile_combo is not None:
            self._header_credential_profile_combo.configure(values=values, width=header_width)
        if self._positions_zoom_credential_profile_combo is not None:
            self._positions_zoom_credential_profile_combo.configure(values=values, width=header_width)
        if self._credential_profile_combo is not None:
            self._credential_profile_combo.configure(values=values)
        current = self._current_credential_profile()
        if current not in values:
            current = values[0]
        if self.api_profile_name.get() != current:
            self.api_profile_name.set(current)

    def _apply_credentials_profile(self, profile_name: str, *, log_change: bool = False) -> None:
        target = profile_name.strip() or DEFAULT_CREDENTIAL_PROFILE_NAME
        snapshot = self._credential_profiles.get(target, _blank_credential_profile_snapshot())
        stored_environment = str(snapshot.get("environment", "")).strip().lower()
        if stored_environment not in {"demo", "live"}:
            stored_environment = ""
        self._loaded_credential_profile_name = target
        self.api_profile_name.set(target)
        self._set_credentials_fields(snapshot)
        self._last_saved_credentials = (
            target,
            snapshot["api_key"],
            snapshot["secret_key"],
            snapshot["passphrase"],
            stored_environment,
        )
        self._apply_profile_environment(target)
        self._sync_credential_profile_combo()
        self._update_settings_summary()
        if log_change:
            self._enqueue_log(f"已切换 API 配置：{target}")
        UiPositionsMixin._refresh_account_views_after_credential_profile_switch(self)

    def _load_saved_credentials(self) -> None:
        try:
            snapshot = load_credentials_profiles_snapshot()
        except Exception as exc:
            self._enqueue_log(f"读取本地凭证文件失败：{exc}")
            return

        profiles = snapshot.get("profiles", {})
        self._credential_profiles = profiles if isinstance(profiles, dict) else {}
        self._sync_credential_profile_combo()
        startup_profile = self._startup_credential_profile_name(
            str(snapshot.get("selected_profile", DEFAULT_CREDENTIAL_PROFILE_NAME))
        )
        self._apply_credentials_profile(startup_profile)
        if any(self._current_credentials_state()[1:4]):
            self._enqueue_log(f"已自动读取本地凭证文件：{credentials_file_path().name}")

    def _load_saved_notification_settings(self) -> None:
        try:
            snapshot = load_notification_snapshot()
        except Exception as exc:
            self._enqueue_log(f"读取通知设置失败：{exc}")
            return

        self.trade_mode_label.set(str(snapshot["trade_mode_label"]))
        self.position_mode_label.set(str(snapshot["position_mode_label"]))
        self.trigger_type_label.set(str(snapshot["trigger_type_label"]))
        self.notify_enabled.set(bool(snapshot["enabled"]))
        self.smtp_host.set(str(snapshot["smtp_host"]))
        self.smtp_port.set(str(snapshot["smtp_port"]))
        self.smtp_username.set(str(snapshot["smtp_username"]))
        self.smtp_password.set(str(snapshot["smtp_password"]))
        self.sender_email.set(str(snapshot["sender_email"]))
        self.recipient_emails.set(str(snapshot["recipient_emails"]))
        self.use_ssl.set(bool(snapshot["use_ssl"]))
        self.notify_trade_fills.set(bool(snapshot["notify_trade_fills"]))
        self.notify_signals.set(bool(snapshot["notify_signals"]))
        self.notify_errors.set(bool(snapshot["notify_errors"]))
        self._refresh_global_email_toggle_text()
        self._default_environment_label = self._normalized_environment_label(str(snapshot["environment_label"]))
        self.environment_label.set(self._default_environment_label)
        self._apply_profile_environment(self._current_credential_profile())
        self._last_saved_notification_state = self._current_notification_state()

    def _load_position_history_view_prefs(self) -> None:
        try:
            snapshot = load_position_history_view_prefs()
        except Exception as exc:
            self._enqueue_log(f"读取历史仓位日期筛选配置失败：{exc}")
            snapshot = {"local_range_start": "", "local_range_end": ""}
        start = str(snapshot.get("local_range_start", "") or "")
        end = str(snapshot.get("local_range_end", "") or "")
        if not start.strip() and not end.strip():
            start, end = _default_position_history_local_year_range_strings()
        self.position_history_range_start.set(start)
        self.position_history_range_end.set(end)
        self._last_saved_position_history_view_prefs = (start, end)

    def _schedule_save_position_history_view_prefs(self, *_: str) -> None:
        if self._position_history_view_prefs_save_job is not None:
            try:
                self.root.after_cancel(self._position_history_view_prefs_save_job)
            except Exception:
                pass
            self._position_history_view_prefs_save_job = None
        self._position_history_view_prefs_save_job = self.root.after(800, self._save_position_history_view_prefs_now)

    def _save_position_history_view_prefs_now(self) -> None:
        if self._position_history_view_prefs_save_job is not None:
            try:
                self.root.after_cancel(self._position_history_view_prefs_save_job)
            except Exception:
                pass
            self._position_history_view_prefs_save_job = None
        current = (self.position_history_range_start.get(), self.position_history_range_end.get())
        if current == self._last_saved_position_history_view_prefs:
            return
        try:
            save_position_history_view_prefs(
                local_range_start=current[0],
                local_range_end=current[1],
            )
        except Exception as exc:
            self._enqueue_log(f"保存历史仓位日期筛选失败：{exc}")
            return
        self._last_saved_position_history_view_prefs = current

    def _load_position_notes(self) -> None:
        try:
            snapshot = load_position_notes_snapshot()
        except Exception as exc:
            self._enqueue_log(f"读取持仓备注失败：{exc}")
            return
        raw_current_notes = snapshot.get("current_notes", [])
        raw_history_notes = snapshot.get("history_notes", [])
        if isinstance(raw_current_notes, list):
            self._position_current_notes = {
                str(item["record_key"]): dict(item)
                for item in raw_current_notes
                if isinstance(item, dict) and str(item.get("record_key", "")).strip()
            }
        if isinstance(raw_history_notes, list):
            self._position_history_notes = {
                str(item["record_key"]): dict(item)
                for item in raw_history_notes
                if isinstance(item, dict) and str(item.get("record_key", "")).strip()
            }

    def _save_position_notes(self) -> None:
        try:
            save_position_notes_snapshot(
                current_notes=list(self._position_current_notes.values()),
                history_notes=list(self._position_history_notes.values()),
            )
        except Exception as exc:
            self._enqueue_log(f"保存持仓备注失败：{exc}")

    def _bind_auto_save(self) -> None:
        self.api_key.trace_add("write", self._on_credentials_changed)
        self.secret_key.trace_add("write", self._on_credentials_changed)
        self.passphrase.trace_add("write", self._on_credentials_changed)
        self.environment_label.trace_add("write", self._on_environment_label_changed)
        self.trade_mode_label.trace_add("write", self._on_settings_changed)
        self.position_mode_label.trace_add("write", self._on_settings_changed)
        self.trigger_type_label.trace_add("write", self._on_settings_changed)
        self._credential_watch_enabled = True

        for variable in (
            self.notify_enabled,
            self.smtp_host,
            self.smtp_port,
            self.smtp_username,
            self.smtp_password,
            self.sender_email,
            self.recipient_emails,
            self.use_ssl,
            self.notify_trade_fills,
            self.notify_signals,
            self.notify_errors,
        ):
            variable.trace_add("write", self._on_notification_settings_changed)
        self.position_history_range_start.trace_add("write", self._schedule_save_position_history_view_prefs)
        self.position_history_range_end.trace_add("write", self._schedule_save_position_history_view_prefs)
        self._settings_watch_enabled = True

    def _on_credentials_changed(self, *_: str) -> None:
        if not self._credential_watch_enabled:
            return
        if self._credential_save_job is not None:
            self.root.after_cancel(self._credential_save_job)
        self._credential_save_job = self.root.after(600, self._save_credentials_now)

    def _on_notification_settings_changed(self, *_: str) -> None:
        if not self._settings_watch_enabled:
            return
        self._refresh_global_email_toggle_text()
        self._refresh_running_session_tree()
        self._refresh_selected_session_details()
        self._on_settings_changed()
        if self._settings_save_job is not None:
            self.root.after_cancel(self._settings_save_job)
        self._settings_save_job = self.root.after(600, self._save_notification_settings_now)

    def _on_settings_changed(self, *_: str) -> None:
        self._update_settings_summary()

    def _on_environment_label_changed(self, *_: str) -> None:
        self._default_environment_label = self._normalized_environment_label(self.environment_label.get())
        self._positions_effective_environment = None
        self._update_settings_summary()
        if self._credential_watch_enabled:
            if self._credential_save_job is not None:
                self.root.after_cancel(self._credential_save_job)
            self._credential_save_job = self.root.after(600, self._save_credentials_now)
        if self._settings_watch_enabled:
            if self._settings_save_job is not None:
                self.root.after_cancel(self._settings_save_job)
            self._settings_save_job = self.root.after(600, self._save_notification_settings_now)

    def _save_credentials_now(self, silent: bool = False) -> None:
        if self._credential_save_job is not None:
            try:
                self.root.after_cancel(self._credential_save_job)
            except Exception:
                pass
            self._credential_save_job = None

        current = self._current_credentials_state()
        if current == self._last_saved_credentials:
            return

        try:
            profile_name, api_key, secret_key, passphrase, environment = current
            self._credential_profiles[profile_name] = {
                "api_key": api_key,
                "secret_key": secret_key,
                "passphrase": passphrase,
                "environment": environment,
            }
            save_credentials_profiles_snapshot(
                selected_profile=profile_name,
                profiles=self._credential_profiles,
            )
        except Exception as exc:
            if not silent:
                self._enqueue_log(f"自动保存凭证失败：{exc}")
            return

        self._last_saved_credentials = current
        if not silent and any(current[1:4]) and not self._auto_save_notice_shown:
            self._enqueue_log(f"已自动保存 API 凭证到：{credentials_file_path().name}")
            self._auto_save_notice_shown = True
        self._sync_credential_profile_combo()
        self._update_settings_summary()

    def _next_api_profile_name(self) -> str:
        used = set(self._credential_profile_names())
        index = 1
        while True:
            candidate = f"api{index}"
            if candidate not in used:
                return candidate
            index += 1

    def _on_api_profile_selected(self, *_: object) -> None:
        selected = self.api_profile_name.get().strip()
        if not selected:
            return
        if selected == self._loaded_credential_profile_name:
            return
        self._save_credentials_now(silent=True)
        if selected not in self._credential_profiles:
            self._credential_profiles[selected] = _blank_credential_profile_snapshot(
                environment=self._environment_value_from_label(self.environment_label.get())
            )
            save_credentials_profiles_snapshot(
                selected_profile=selected,
                profiles=self._credential_profiles,
            )
        self._apply_credentials_profile(selected, log_change=True)

    def _create_api_profile(self) -> None:
        self._save_credentials_now(silent=True)
        profile_name = self._next_api_profile_name()
        self._credential_profiles[profile_name] = _blank_credential_profile_snapshot(
            environment=self._environment_value_from_label(self.environment_label.get())
        )
        save_credentials_profiles_snapshot(
            selected_profile=profile_name,
            profiles=self._credential_profiles,
        )
        self._apply_credentials_profile(profile_name, log_change=True)
        self._enqueue_log(f"已新增 API 配置：{profile_name}")

    def _rename_current_api_profile(self) -> None:
        current_name = self._editing_credential_profile()
        new_name = simpledialog.askstring(
            "重命名 API 配置",
            "请输入新的 API 配置名称：",
            initialvalue=current_name,
            parent=self._settings_window or self.root,
        )
        if new_name is None:
            return

        target_name = new_name.strip()
        if not target_name:
            messagebox.showerror("重命名失败", "API 配置名称不能为空。", parent=self._settings_window or self.root)
            return
        if target_name == current_name:
            return
        if target_name in self._credential_profiles:
            messagebox.showerror(
                "重命名失败",
                f"API 配置 {target_name} 已存在，请换一个名字。",
                parent=self._settings_window or self.root,
            )
            return

        self._save_credentials_now(silent=True)
        profiles = dict(self._credential_profiles)
        profile_payload = profiles.pop(current_name, _blank_credential_profile_snapshot())
        profiles[target_name] = profile_payload
        self._credential_profiles = profiles
        save_credentials_profiles_snapshot(
            selected_profile=target_name,
            profiles=self._credential_profiles,
        )
        self._apply_credentials_profile(target_name, log_change=True)
        self._enqueue_log(f"已将 API 配置 {current_name} 重命名为：{target_name}")

    def _delete_current_api_profile(self) -> None:
        profile_name = self._editing_credential_profile()
        if not messagebox.askyesno(
            "删除确认",
            f"确认删除 API 配置 {profile_name} 吗？",
            parent=self._settings_window or self.root,
        ):
            return

        profiles = dict(self._credential_profiles)
        profiles.pop(profile_name, None)
        if not profiles:
            next_profile = DEFAULT_CREDENTIAL_PROFILE_NAME
            profiles[next_profile] = _blank_credential_profile_snapshot(
                environment=self._environment_value_from_label(self.environment_label.get())
            )
        else:
            next_profile = sorted(profiles.keys())[0]

        self._credential_profiles = profiles
        save_credentials_profiles_snapshot(
            selected_profile=next_profile,
            profiles=self._credential_profiles,
        )
        self._apply_credentials_profile(next_profile, log_change=True)
        self._enqueue_log(f"已删除 API 配置：{profile_name}")

    def _save_notification_settings_now(self, silent: bool = False) -> None:
        if self._settings_save_job is not None:
            try:
                self.root.after_cancel(self._settings_save_job)
            except Exception:
                pass
            self._settings_save_job = None

        current = self._current_notification_state()
        if current == self._last_saved_notification_state:
            return

        try:
            port = self._parse_optional_port(self.smtp_port.get())
            save_notification_snapshot(
                environment_label=self.environment_label.get(),
                trade_mode_label=self.trade_mode_label.get(),
                position_mode_label=self.position_mode_label.get(),
                trigger_type_label=self.trigger_type_label.get(),
                enabled=self.notify_enabled.get(),
                smtp_host=self.smtp_host.get(),
                smtp_port=port,
                smtp_username=self.smtp_username.get(),
                smtp_password=self.smtp_password.get(),
                sender_email=self.sender_email.get(),
                recipient_emails=self.recipient_emails.get(),
                use_ssl=self.use_ssl.get(),
                notify_trade_fills=self.notify_trade_fills.get(),
                notify_signals=self.notify_signals.get(),
                notify_errors=self.notify_errors.get(),
            )
        except Exception as exc:
            if not silent:
                self._enqueue_log(f"保存通知设置失败：{exc}")
            return

        self._last_saved_notification_state = current

    def load_symbols(self) -> None:
        self._enqueue_log("正在从 OKX 加载永续合约列表...")
        threading.Thread(target=self._load_symbols_worker, daemon=True).start()

    def _load_symbols_worker(self) -> None:
        try:
            instruments = [item for item in self.client.get_swap_instruments() if item.state.lower() == "live"]
            symbols = [item.inst_id for item in instruments]
            self.root.after(0, lambda: self._apply_symbols(instruments, symbols))
        except Exception as exc:
            self._enqueue_log(f"加载交易对失败：{exc}")

    def _apply_symbols(self, instruments: list[Instrument], symbols: list[str]) -> None:
        self.instruments = instruments
        self._fixed_order_size_hint_instrument_cache.update(
            {item.inst_id.strip().upper(): item for item in instruments if item.inst_id.strip()}
        )
        merged = list(dict.fromkeys(self._default_symbol_values + symbols))
        custom_trigger_values = ["", *merged]
        preferred_symbol = self._default_launch_symbol if self._default_launch_symbol in merged else (merged[0] if merged else "")
        self.symbol_combo["values"] = merged
        self.local_tp_sl_symbol_combo["values"] = custom_trigger_values
        if self.symbol.get() not in merged and merged:
            self.symbol.set(preferred_symbol)
        elif merged and self.trade_symbol.get() not in merged:
            self.trade_symbol.set(preferred_symbol)
        if self.local_tp_sl_symbol.get() not in custom_trigger_values:
            self.local_tp_sl_symbol.set("")
        self._enqueue_log(f"已加载 {len(symbols)} 个可交易永续合约。")
        self._update_fixed_order_size_hint()

    def _sync_trade_symbol_to_symbol(self, *_: str) -> None:
        symbol = self.symbol.get().strip().upper()
        if self.trade_symbol.get() != symbol:
            self.trade_symbol.set(symbol)

    def _on_fixed_order_size_symbol_changed(self, *_: str) -> None:
        self._update_fixed_order_size_hint(fetch_instrument_if_missing=True)

    def _update_launch_parameter_hint(self, *_: str) -> None:
        self.launch_parameter_hint_text.set(
            _build_launch_parameter_hint_text(
                stop_atr_raw=self.stop_atr.get(),
                take_atr_raw=self.take_atr.get(),
                take_profit_mode_label=self.take_profit_mode_label.get(),
                max_entries_raw=self.max_entries_per_trend.get(),
                startup_chase_window_raw=self.startup_chase_window_seconds.get(),
            )
        )

    def _update_trend_parameter_hint(self, *_: str) -> None:
        definition = self._selected_strategy_definition()
        self.trend_parameter_hint_text.set(
            _build_trend_parameter_hint_text(
                strategy_id=definition.strategy_id,
                ema_period_raw=self.ema_period.get(),
                trend_ema_period_raw=self.trend_ema_period.get(),
                big_ema_period_raw=self.big_ema_period.get(),
                entry_reference_ema_period_raw=self.entry_reference_ema_period.get(),
            )
        )

    def _update_dynamic_protection_hint(self, *_: str) -> None:
        self.dynamic_protection_hint_text.set(
            _build_dynamic_protection_hint_text(
                take_profit_mode_label=self.take_profit_mode_label.get(),
                dynamic_two_r_break_even_enabled=self.dynamic_two_r_break_even.get(),
                dynamic_fee_offset_enabled=self.dynamic_fee_offset_enabled.get(),
                time_stop_break_even_enabled=self.time_stop_break_even_enabled.get(),
                time_stop_break_even_bars_raw=self.time_stop_break_even_bars.get(),
            )
        )

    def _update_fixed_order_size_hint(self, *_: str, fetch_instrument_if_missing: bool = False) -> None:
        symbol = _normalize_symbol_input(self.trade_symbol.get()) or _normalize_symbol_input(self.symbol.get())
        instrument = self._find_instrument_for_fixed_order_size_hint(
            symbol,
            fetch_if_missing=fetch_instrument_if_missing,
        )
        base_hint = _build_fixed_order_size_hint_text(symbol, instrument)
        mode_hint = _build_order_size_mode_hint_text(self.risk_amount.get(), self.order_size.get())
        self.fixed_order_size_hint_text.set(f"{base_hint} {mode_hint}".strip())

    def _schedule_minimum_order_risk_hint_update(self, *_: str) -> None:
        if not hasattr(self, "root"):
            return
        if self._minimum_order_risk_hint_after_id is not None:
            try:
                self.root.after_cancel(self._minimum_order_risk_hint_after_id)
            except Exception:
                pass
        self._minimum_order_risk_hint_after_id = self.root.after(250, self._update_minimum_order_risk_hint)

    def _update_minimum_order_risk_hint(self) -> None:
        self._minimum_order_risk_hint_after_id = None
        signal_symbol = _normalize_symbol_input(self.symbol.get())
        trade_symbol = _normalize_symbol_input(self.trade_symbol.get()) or signal_symbol
        instrument = self._find_instrument_for_fixed_order_size_hint(trade_symbol, fetch_if_missing=True)
        if not trade_symbol:
            self.minimum_order_risk_hint_text.set("下单门槛：请先选择下单标的。")
            return
        request = self._build_minimum_order_risk_hint_request(signal_symbol, trade_symbol, instrument)
        if request is None:
            return
        if instrument is None:
            self.minimum_order_risk_hint_text.set(
                _build_minimum_order_risk_hint_text(
                    inst_id=trade_symbol,
                    instrument=None,
                    risk_amount_raw=self.risk_amount.get(),
                    note=request["note"],
                )
            )
            return
        self.minimum_order_risk_hint_text.set(
            _build_minimum_order_risk_hint_text(
                inst_id=trade_symbol,
                instrument=instrument,
                risk_amount_raw=self.risk_amount.get(),
                note=request["note"],
                pending=request["pending"],
            )
        )
        if not request["should_estimate"]:
            return
        self._minimum_order_risk_hint_request_serial += 1
        request_serial = self._minimum_order_risk_hint_request_serial
        self._minimum_order_risk_hint_active_request_serial = request_serial
        threading.Thread(
            target=self._estimate_minimum_order_risk_hint_worker,
            args=(request_serial, request),
            daemon=True,
        ).start()

    def _build_minimum_order_risk_hint_request(
        self,
        signal_symbol: str,
        trade_symbol: str,
        instrument: Instrument | None,
    ) -> dict[str, object] | None:
        run_mode = RUN_MODE_OPTIONS.get(self.run_mode_label.get(), "trade")
        if run_mode != "trade":
            return {
                "note": "当前运行模式不下单，不需要最小下单门槛。",
                "pending": False,
                "should_estimate": False,
            }
        definition = self._selected_strategy_definition()
        if instrument is None:
            return {
                "note": "正在读取该标的最小下单规格。",
                "pending": False,
                "should_estimate": False,
            }
        try:
            config = StrategyConfig(
                inst_id=signal_symbol or trade_symbol,
                bar=self.bar.get().strip(),
                ema_period=self._parse_nonnegative_int(self.ema_period.get(), "EMA小周期"),
                trend_ema_period=self._parse_nonnegative_int(self.trend_ema_period.get(), "EMA中周期"),
                big_ema_period=self._parse_nonnegative_int(self.big_ema_period.get(), "EMA大周期"),
                atr_period=self._parse_nonnegative_int(self.atr_period.get(), "ATR周期"),
                atr_stop_multiplier=self._parse_positive_decimal(self.stop_atr.get(), "止损 ATR 倍数"),
                atr_take_multiplier=self._parse_positive_decimal(self.take_atr.get(), "止盈 ATR 倍数"),
                order_size=_parse_positive_decimal_hint(self.order_size.get()) or Decimal("1"),
                trade_mode=TRADE_MODE_OPTIONS.get(self.trade_mode_label.get(), "cross"),
                signal_mode=SIGNAL_LABEL_TO_VALUE.get(self.signal_mode_label.get(), definition.default_signal_mode),
                position_mode=POSITION_MODE_OPTIONS.get(self.position_mode_label.get(), "net"),
                environment=ENV_OPTIONS.get(self.environment_label.get(), "demo"),
                tp_sl_trigger_type=TRIGGER_TYPE_OPTIONS.get(self.trigger_type_label.get(), "mark"),
                strategy_id=definition.strategy_id,
                risk_amount=_parse_positive_decimal_hint(self.risk_amount.get()),
                trade_inst_id=trade_symbol,
                tp_sl_mode=TP_SL_MODE_OPTIONS.get(self.tp_sl_mode_label.get(), "exchange"),
                local_tp_sl_inst_id=_normalize_symbol_input(self.local_tp_sl_symbol.get()) or None,
                run_mode=run_mode,
                take_profit_mode=TAKE_PROFIT_MODE_OPTIONS.get(self.take_profit_mode_label.get(), "dynamic"),
                max_entries_per_trend=max(self._parse_nonnegative_int(self.max_entries_per_trend.get(), "每波最多开仓次数"), 0),
                entry_reference_ema_period=max(
                    self._parse_nonnegative_int(
                        self.entry_reference_ema_period.get(),
                        _entry_reference_ema_caption(definition.strategy_id),
                    ),
                    0,
                ),
            )
        except Exception as exc:
            self.minimum_order_risk_hint_text.set(
                _build_minimum_order_risk_hint_text(
                    inst_id=trade_symbol,
                    instrument=instrument,
                    risk_amount_raw=self.risk_amount.get(),
                    note=f"参数未填完整：{exc}",
                )
            )
            return None
        return {
            "signal_symbol": signal_symbol or trade_symbol,
            "trade_symbol": trade_symbol,
            "instrument": instrument,
            "config": config,
            "risk_amount_raw": self.risk_amount.get(),
            "note": "",
            "pending": True,
            "should_estimate": True,
        }
    def _estimate_minimum_order_risk_hint_worker(self, request_serial: int, request: dict[str, object]) -> None:
        instrument = request["instrument"]
        config = request["config"]
        signal_symbol = str(request["signal_symbol"])
        risk_amount_raw = str(request["risk_amount_raw"])
        note = ""
        minimum_risk_amount: Decimal | None = None
        try:
            minimum_risk_amount, note = _estimate_launcher_minimum_risk_amount(
                client=self.client,
                signal_inst_id=signal_symbol,
                trade_instrument=instrument,
                config=config,
            )
        except Exception as exc:
            detail = str(exc).strip()
            note = _format_network_error_message(detail) if detail else "读取下单门槛失败。"
        try:
            self.root.after(
                0,
                lambda: self._apply_minimum_order_risk_hint_result(
                    request_serial=request_serial,
                    trade_symbol=str(request["trade_symbol"]),
                    instrument=instrument,
                    risk_amount_raw=risk_amount_raw,
                    minimum_risk_amount=minimum_risk_amount,
                    note=note,
                ),
            )
        except Exception:
            pass
    def _apply_minimum_order_risk_hint_result(
        self,
        *,
        request_serial: int,
        trade_symbol: str,
        instrument: Instrument,
        risk_amount_raw: str,
        minimum_risk_amount: Decimal | None,
        note: str,
    ) -> None:
        if request_serial != self._minimum_order_risk_hint_active_request_serial:
            return
        current_trade_symbol = _normalize_symbol_input(self.trade_symbol.get()) or _normalize_symbol_input(self.symbol.get())
        if current_trade_symbol != trade_symbol:
            return
        self.minimum_order_risk_hint_text.set(
            _build_minimum_order_risk_hint_text(
                inst_id=trade_symbol,
                instrument=instrument,
                risk_amount_raw=risk_amount_raw,
                minimum_risk_amount=minimum_risk_amount,
                note=note,
            )
        )

    def _find_instrument_for_fixed_order_size_hint(
        self,
        inst_id: str,
        *,
        fetch_if_missing: bool = False,
    ) -> Instrument | None:
        normalized = _normalize_symbol_input(inst_id)
        if not normalized:
            return None
        instrument = self._fixed_order_size_hint_instrument_cache.get(normalized)
        if instrument is not None:
            return instrument
        for instrument in self.instruments:
            if instrument.inst_id.strip().upper() == normalized:
                self._fixed_order_size_hint_instrument_cache[normalized] = instrument
                return instrument
        instrument = self._position_instruments.get(normalized)
        if instrument is not None:
            self._fixed_order_size_hint_instrument_cache[normalized] = instrument
            return instrument
        if fetch_if_missing:
            self._ensure_fixed_order_size_hint_instrument_async(normalized)
        return None

    def _ensure_fixed_order_size_hint_instrument_async(self, inst_id: str) -> None:
        normalized = _normalize_symbol_input(inst_id)
        if not normalized:
            return
        if normalized in self._fixed_order_size_hint_instrument_cache:
            return
        if normalized in self._fixed_order_size_hint_fetching_inst_ids:
            return
        self._fixed_order_size_hint_fetching_inst_ids.add(normalized)
        threading.Thread(
            target=self._fetch_fixed_order_size_hint_instrument_worker,
            args=(normalized,),
            daemon=True,
        ).start()

    def _fetch_fixed_order_size_hint_instrument_worker(self, inst_id: str) -> None:
        instrument: Instrument | None
        try:
            instrument = self.client.get_instrument(inst_id)
        except Exception:
            instrument = None
        try:
            self.root.after(0, lambda: self._apply_fixed_order_size_hint_instrument(inst_id, instrument))
        except Exception:
            pass

    def _apply_fixed_order_size_hint_instrument(self, inst_id: str, instrument: Instrument | None) -> None:
        normalized = _normalize_symbol_input(inst_id)
        if normalized:
            self._fixed_order_size_hint_fetching_inst_ids.discard(normalized)
            if instrument is not None:
                self._fixed_order_size_hint_instrument_cache[normalized] = instrument
                if all(item.inst_id.strip().upper() != normalized for item in self.instruments):
                    self.instruments.append(instrument)
        current_symbol = _normalize_symbol_input(self.trade_symbol.get()) or _normalize_symbol_input(self.symbol.get())
        if normalized and current_symbol == normalized:
            self._update_fixed_order_size_hint()
            self._schedule_minimum_order_risk_hint_update()






















































































































































































































































































    def _parse_positive_int(self, raw: str, field_name: str) -> int:
        try:
            value = int(raw)
        except ValueError as exc:
            raise ValueError(f"{field_name} 不是有效整数") from exc
        if value <= 0:
            raise ValueError(f"{field_name} 必须大于 0")
        return value

    def _parse_nonnegative_int(self, raw: str, field_name: str) -> int:
        try:
            value = int(raw)
        except ValueError as exc:
            raise ValueError(f"{field_name} 不是有效整数") from exc
        if value < 0:
            raise ValueError(f"{field_name} 不能小于 0")
        return value

    def _parse_positive_decimal(self, raw: str, field_name: str) -> Decimal:
        try:
            value = Decimal(raw)
        except InvalidOperation as exc:
            raise ValueError(f"{field_name} 不是有效数字") from exc
        if value <= 0:
            raise ValueError(f"{field_name} 必须大于 0")
        return value

    def _parse_optional_positive_decimal(self, raw: str, field_name: str) -> Decimal | None:
        cleaned = raw.strip()
        if not cleaned:
            return None
        return self._parse_positive_decimal(cleaned, field_name)

    def _parse_optional_port(self, raw: str) -> int:
        cleaned = raw.strip()
        if not cleaned:
            return 465
        value = int(cleaned)
        if value <= 0:
            raise ValueError("SMTP 端口必须大于 0")
        return value

    def _split_recipients(self, raw: str) -> list[str]:
        return [item.strip() for item in re.split(r"[,\n;]+", raw) if item.strip()]

    def _current_credentials_or_none(self) -> Credentials | None:
        api_key = self.api_key.get().strip()
        secret_key = self.secret_key.get().strip()
        passphrase = self.passphrase.get().strip()
        if not api_key or not secret_key or not passphrase:
            return None
        return Credentials(
            api_key=api_key,
            secret_key=secret_key,
            passphrase=passphrase,
            profile_name=self._current_credential_profile(),
        )

    def _build_smart_order_runtime_config_or_none(self) -> SmartOrderRuntimeConfig | None:
        credentials = self._current_credentials_or_none()
        if credentials is None:
            return None
        return SmartOrderRuntimeConfig(
            credentials=credentials,
            environment=ENV_OPTIONS[self.environment_label.get()],
            trade_mode=TRADE_MODE_OPTIONS[self.trade_mode_label.get()],
            position_mode=POSITION_MODE_OPTIONS[self.position_mode_label.get()],
            credential_profile_name=self._loaded_credential_profile_name,
        )

    def _build_option_strategy_runtime_or_none(self) -> tuple[Credentials, str] | None:
        credentials = self._current_credentials_or_none()
        if credentials is None:
            return None
        environment = self._positions_effective_environment or ENV_OPTIONS[self.environment_label.get()]
        return credentials, environment

    def _current_notification_state(self) -> tuple[object, ...]:
        return (
            self.environment_label.get(),
            self.trade_mode_label.get(),
            self.position_mode_label.get(),
            self.trigger_type_label.get(),
            self.notify_enabled.get(),
            self.smtp_host.get().strip(),
            self.smtp_port.get().strip(),
            self.smtp_username.get().strip(),
            self.smtp_password.get(),
            self.sender_email.get().strip(),
            self.recipient_emails.get().strip(),
            self.use_ssl.get(),
            self.notify_trade_fills.get(),
            self.notify_signals.get(),
            self.notify_errors.get(),
        )

    def _enqueue_log(self, message: str) -> None:
        self.log_queue.put(append_log_line(message))

    def _drain_log_queue(self) -> None:
        while not self.log_queue.empty():
            line = self.log_queue.get_nowait()
            self.log_text.insert(END, line + "\n")
            self.log_text.see(END)
        self.root.after(250, self._drain_log_queue)

    def _trader_desk_handle_stopped_session(self, session: StrategySession) -> None:
        trader_id = getattr(session, "trader_id", "").strip()
        trader_slot_id = getattr(session, "trader_slot_id", "").strip()
        if not trader_id or not trader_slot_id:
            return
        slot = self._trader_desk_slot_for_session(session.session_id, trader_slot_id)
        run = self._trader_desk_run_by_id(trader_id, create=True)
        draft = self._trader_desk_draft_by_id(trader_id)
        if slot is None or run is None:
            return
        if slot.status in {"closed_profit", "closed_loss", "closed_manual", "stopped", "failed"}:
            return
        stopped_at = session.stopped_at or datetime.now()
        stop_reason = self._session_stop_reason_text(session)
        expected_stop = self._expected_trader_stop_reason(stop_reason)
        if slot.status == "watching":
            slot.status = "stopped"
            slot.closed_at = stopped_at
            slot.released_at = stopped_at
            slot.close_reason = stop_reason
            run.armed_session_id = ""
            run.last_event_at = datetime.now()
            run.updated_at = datetime.now()
            if not expected_stop and run.status not in {"paused_manual", "paused_loss", "stopped"}:
                run.status = "stopped"
                run.paused_reason = stop_reason
                if draft is not None:
                    draft.status = "paused"
                    draft.updated_at = datetime.now()
                self._trader_desk_add_event(
                    trader_id,
                    f"watcher 异常结束，已暂停交易员 | 会话={session.session_id} | 原因={stop_reason}",
                    level="error",
                )
                self._save_trader_desk_snapshot()
                return
            self._trader_desk_add_event(
                trader_id,
                f"watcher 已停止 | 会话={session.session_id} | 原因={slot.close_reason}",
                level="warning" if run.status.startswith("paused") else "info",
            )
            self._save_trader_desk_snapshot()
            return
        run.armed_session_id = ""
        run.last_event_at = datetime.now()
        run.updated_at = datetime.now()
        if not expected_stop and run.status not in {"paused_manual", "paused_loss", "stopped"}:
            slot.status = "failed"
            slot.closed_at = stopped_at
            slot.close_reason = stop_reason
            slot.history_record_id = session.history_record_id or slot.history_record_id
            run.status = "stopped"
            run.paused_reason = f"活动额度格异常结束：{stop_reason}"
            if draft is not None:
                draft.status = "paused"
                draft.updated_at = datetime.now()
            self._trader_desk_add_event(
                trader_id,
                f"活动额度格异常结束，已暂停交易员 | 会话={session.session_id} | 原因={stop_reason} | 请人工核对持仓/委托",
                level="error",
            )
            self._save_trader_desk_snapshot()
            return
        if slot.pending_manual_exit_order_id or slot.pending_manual_exit_cl_ord_id:
            slot.close_reason = stop_reason or slot.close_reason or "人工平仓后停止策略线程"
            slot.note = (
                f"策略线程已停止，等待{self._trader_manual_flatten_mode_label(slot.pending_manual_exit_mode)}成交回查"
            )
            self._trader_desk_add_event(
                trader_id,
                f"活动额度格策略已停止，等待人工平仓单成交 | 会话={session.session_id} | 方式={self._trader_manual_flatten_mode_label(slot.pending_manual_exit_mode)}",
                level="warning",
            )
            self._save_trader_desk_snapshot()
            return
        slot.status = "closed_manual"
        slot.closed_at = stopped_at
        slot.released_at = stopped_at
        slot.quota_occupied = False
        slot.close_reason = stop_reason
        slot.history_record_id = session.history_record_id or slot.history_record_id
        self._trader_desk_add_event(
            trader_id,
            f"活动额度格已停止 | 会话={session.session_id} | 原因={slot.close_reason}",
            level="warning",
        )
        self._save_trader_desk_snapshot()
        self._ensure_trader_watcher(trader_id)

    def _refresh_trader_pending_manual_flatten_orders(self, trader_id: str) -> None:
        pending_slots = [
            slot
            for slot in trader_slots_for(self._trader_desk_slots, trader_id)
            if slot.status == "open" and (slot.pending_manual_exit_order_id or slot.pending_manual_exit_cl_ord_id)
        ]
        if not pending_slots:
            return
        draft = self._trader_desk_draft_by_id(trader_id)
        if draft is None:
            return
        config = _deserialize_strategy_config_snapshot(draft.template_payload.get("config_snapshot"))
        if config is None:
            return
        credentials = self._credentials_for_profile_or_none(str(draft.template_payload.get("api_name") or ""))
        if credentials is None:
            return
        changed = False
        now = datetime.now()
        for slot in sorted(pending_slots, key=lambda item: (item.created_at, item.slot_id)):
            inst_id = (
                str(slot.pending_manual_exit_inst_id or "").strip().upper()
                or (config.trade_inst_id or config.inst_id or str(draft.template_payload.get("symbol") or "")).strip().upper()
            )
            if not inst_id:
                continue
            try:
                status = self.client.get_order(
                    credentials,
                    config,
                    inst_id=inst_id,
                    ord_id=(slot.pending_manual_exit_order_id or "").strip() or None,
                    cl_ord_id=(slot.pending_manual_exit_cl_ord_id or "").strip() or None,
                )
            except Exception:
                continue
            latest_state = str(status.state or "").strip().lower()
            if latest_state == "filled":
                self._mark_trader_slot_manual_flatten_closed(
                    slot,
                    now=now,
                    exit_price=status.avg_price or status.price,
                    flatten_mode=slot.pending_manual_exit_mode,
                )
                slot.note = (
                    f"人工平仓已成交 | 方式={self._trader_manual_flatten_mode_label(slot.pending_manual_exit_mode)} | "
                    f"ordId={(status.ord_id or '-').strip() or '-'}"
                )
                self._trader_desk_add_event(
                    trader_id,
                    f"人工平仓单已成交 | 会话={slot.session_id} | 方式={self._trader_manual_flatten_mode_label(slot.pending_manual_exit_mode)} | 平仓价={_format_optional_decimal(slot.exit_price)}",
                )
                changed = True
                continue
            if latest_state in {"canceled", "mmp_canceled", "order_failed", "partially_failed"}:
                slot.note = (
                    f"人工平仓挂单未成交 | 方式={self._trader_manual_flatten_mode_label(slot.pending_manual_exit_mode)} | "
                    f"状态={latest_state or '-'} | ordId={(status.ord_id or '-').strip() or '-'}"
                )
                slot.close_reason = f"人工{self._trader_manual_flatten_mode_label(slot.pending_manual_exit_mode)}未成交"
                self._clear_trader_manual_flatten_pending(slot)
                self._trader_desk_add_event(
                    trader_id,
                    f"人工平仓挂单未成交 | 会话={slot.session_id} | 方式={self._trader_manual_flatten_mode_label(slot.pending_manual_exit_mode)} | 状态={latest_state or '-'}",
                    level="warning",
                )
                changed = True
                continue
            if latest_state == "partially_filled":
                slot.note = (
                    f"人工平仓挂单部分成交 | 方式={self._trader_manual_flatten_mode_label(slot.pending_manual_exit_mode)} | "
                    f"已成交={_format_optional_decimal(status.filled_size)} / {_format_optional_decimal(status.size)}"
                )
                changed = True
        if changed:
            self._save_trader_desk_snapshot()

    def _refresh_trader_desk_runtime(self) -> None:
        for run in list(self._trader_desk_runs):
            self._refresh_trader_pending_manual_flatten_orders(run.trader_id)
            self._cleanup_stale_trader_watchers(run.trader_id)
            if run.status not in {"running", "quota_exhausted"}:
                continue
            self._ensure_trader_watcher(run.trader_id)

    def _refresh_status(self) -> None:
        running_count = 0
        self._refresh_session_live_pnl_cache()
        for session in self.sessions.values():
            if session.engine.is_running:
                if session.status != "停止中":
                    session.status = "运行中"
                    if session.runtime_status in {"待恢复", "恢复中"} and not session.last_message:
                        session.runtime_status = "运行中"
                    if session.ended_reason in {"应用关闭", "应用关闭后待恢复接管", "恢复中", "恢复启动失败"}:
                        session.ended_reason = ""
                running_count += 1
            elif session.stop_cleanup_in_progress:
                session.status = "停止中"
            elif session.status == "恢复中":
                session.status = "待恢复"
                session.runtime_status = "待恢复"
                if session.stopped_at is None:
                    session.stopped_at = datetime.now()
                if not session.ended_reason or session.ended_reason == "恢复中":
                    session.ended_reason = "恢复启动失败"
            elif session.status in {"运行中", "停止中"}:
                session.status = "已停止"
                if session.stopped_at is None:
                    session.stopped_at = datetime.now()
                session.ended_reason = self._session_stop_reason_text(session)
                self._remove_recoverable_strategy_session(session.session_id)
                self._trader_desk_handle_stopped_session(session)
            self._upsert_session_row(session)
            self._sync_strategy_history_from_session(session)

        self.status_text.set(f"运行中策略：{running_count}")
        self._refresh_trader_desk_runtime()
        self._refresh_running_session_summary()
        self._update_settings_summary()
        self._refresh_selected_session_details()
        self._refresh_strategy_book_window()
        self.root.after(500, self._refresh_status)

    def _default_line_trading_symbol(self) -> str:
        return DEFAULT_LAUNCH_SYMBOLS[0]

    def _line_trading_symbol_values(self) -> tuple[str, ...]:
        return tuple(DEFAULT_LAUNCH_SYMBOLS)

    def _line_trading_desk_annotation_storage_key_for(self, state: LineTradingDeskWindowState) -> str:
        api = (state.api_profile_var.get().strip() if state.api_profile_var is not None else "") or DEFAULT_CREDENTIAL_PROFILE_NAME
        sym = state.symbol_var.get().strip().upper()
        bar = state.bar_var.get().strip()
        return f"{api}|{sym}|{bar}"

    def _line_trading_desk_line_annotation_to_payload(self, ann: LiveChartLineAnnotation) -> dict[str, object]:
        d: dict[str, object] = {
            "kind": ann.kind,
            "x1": float(ann.x1),
            "y1": float(ann.y1),
            "x2": float(ann.x2),
            "y2": float(ann.y2),
            "color": str(ann.color or ""),
            "label": str(ann.label or ""),
            "desk_ray_action": str(ann.desk_ray_action or "notify"),
            "desk_ray_triggered": bool(ann.desk_ray_triggered),
            "desk_ray_submit_pending": False,
            "desk_ray_last_side": ann.desk_ray_last_side,
            "locked": bool(getattr(ann, "locked", False)),
        }
        if ann.bar_a is not None:
            d["bar_a"] = float(ann.bar_a)
        if ann.bar_b is not None:
            d["bar_b"] = float(ann.bar_b)
        if ann.price_a is not None:
            d["price_a"] = str(ann.price_a)
        if ann.price_b is not None:
            d["price_b"] = str(ann.price_b)
        return d

    def _line_trading_desk_rr_annotation_to_payload(self, box: DeskRiskRewardAnnotation) -> dict[str, object]:
        return {
            "rr_id": str(box.rr_id),
            "side": str(box.side),
            "bar_entry": float(box.bar_entry),
            "bar_stop": float(box.bar_stop),
            "price_entry": str(box.price_entry),
            "price_stop": str(box.price_stop),
            "price_tp": str(box.price_tp),
            "r_multiple": str(box.r_multiple),
            "locked": bool(box.locked),
        }

    def _line_trading_desk_parse_line_annotation_payload(self, raw: object) -> LiveChartLineAnnotation | None:
        if not isinstance(raw, dict):
            return None
        kind = str(raw.get("kind") or "").strip()
        if kind not in ("line", "horizontal", "stop"):
            return None
        try:
            x1 = float(raw.get("x1", 0.0))
            y1 = float(raw.get("y1", 0.0))
            x2 = float(raw.get("x2", 0.0))
            y2 = float(raw.get("y2", 0.0))
        except (TypeError, ValueError):
            return None
        color = str(raw.get("color") or "#1d4ed8")
        label = str(raw.get("label") or "")
        bar_a = raw.get("bar_a")
        bar_b = raw.get("bar_b")
        try:
            bar_af = float(bar_a) if bar_a is not None else None
            bar_bf = float(bar_b) if bar_b is not None else None
        except (TypeError, ValueError):
            return None
        if kind in ("line", "horizontal") and (bar_af is None or bar_bf is None):
            return None
        if kind == "stop" and (bar_af is None or bar_bf is None):
            return None

        def _pd(key: str) -> Decimal | None:
            v = raw.get(key)
            if v is None or v == "":
                return None
            try:
                return Decimal(str(v).strip().replace(",", ""))
            except InvalidOperation:
                return None

        pa, pb = _pd("price_a"), _pd("price_b")
        if kind in ("line", "horizontal") and (pa is None or pb is None):
            return None
        if kind == "stop" and pa is None:
            return None
        if kind == "stop" and pb is None:
            pb = pa
        ray_action = _line_trading_desk_normalize_ray_action(str(raw.get("desk_ray_action") or "notify"))
        last_side = raw.get("desk_ray_last_side")
        if last_side is not None and last_side != "":
            try:
                last_side_i = int(last_side)
            except (TypeError, ValueError):
                last_side_i = None
        else:
            last_side_i = None
        return LiveChartLineAnnotation(
            kind=kind,
            x1=x1,
            y1=y1,
            x2=x2,
            y2=y2,
            color=color,
            label=label,
            bar_a=bar_af,
            price_a=pa,
            bar_b=bar_bf,
            price_b=pb if pb is not None else pa,
            desk_ray_action=ray_action,
            desk_ray_triggered=bool(raw.get("desk_ray_triggered", False)),
            desk_ray_submit_pending=False,
            desk_ray_last_side=last_side_i,
            locked=bool(raw.get("locked", False)),
        )

    def _line_trading_desk_parse_rr_annotation_payload(self, raw: object) -> DeskRiskRewardAnnotation | None:
        if not isinstance(raw, dict):
            return None
        rid = str(raw.get("rr_id") or "").strip() or uuid.uuid4().hex[:12]
        side = str(raw.get("side") or "long").strip().lower()
        if side not in ("long", "short"):
            side = "long"
        try:
            bar_entry = float(raw.get("bar_entry", 0.0))
            bar_stop = float(raw.get("bar_stop", bar_entry))
        except (TypeError, ValueError):
            return None
        try:
            price_entry = Decimal(str(raw.get("price_entry")).strip().replace(",", ""))
            price_stop = Decimal(str(raw.get("price_stop")).strip().replace(",", ""))
            price_tp = Decimal(str(raw.get("price_tp")).strip().replace(",", ""))
        except (InvalidOperation, TypeError, AttributeError):
            return None
        try:
            r_multiple = Decimal(str(raw.get("r_multiple", "2")).strip() or "2")
        except InvalidOperation:
            r_multiple = Decimal("2")
        if r_multiple <= 0:
            r_multiple = Decimal("2")
        return DeskRiskRewardAnnotation(
            rr_id=rid,
            side=side,
            bar_entry=bar_entry,
            price_entry=price_entry,
            bar_stop=bar_stop,
            price_stop=price_stop,
            price_tp=price_tp,
            r_multiple=r_multiple,
            locked=bool(raw.get("locked", False)),
        )

    def _line_trading_desk_capture_state_into_annotation_entries(self, state: LineTradingDeskWindowState, key: str) -> None:
        if not key:
            return
        lines = [self._line_trading_desk_line_annotation_to_payload(a) for a in state.line_annotations]
        rr = [self._line_trading_desk_rr_annotation_to_payload(b) for b in state.rr_annotations]
        self._line_trading_desk_annotation_entries[key] = {"lines": lines, "rr": rr}

    def _line_trading_desk_apply_annotation_entry_to_state(self, state: LineTradingDeskWindowState, key: str) -> None:
        state.line_annotations.clear()
        state.rr_annotations.clear()
        entry = self._line_trading_desk_annotation_entries.get(key)
        if not isinstance(entry, dict):
            return
        raw_lines = entry.get("lines")
        if isinstance(raw_lines, list):
            for item in raw_lines:
                ann = self._line_trading_desk_parse_line_annotation_payload(item)
                if ann is not None:
                    state.line_annotations.append(ann)
        raw_rr = entry.get("rr")
        if isinstance(raw_rr, list):
            for item in raw_rr:
                box = self._line_trading_desk_parse_rr_annotation_payload(item)
                if box is not None:
                    state.rr_annotations.append(box)

    def _line_trading_desk_cancel_annotation_persist_job(self, state: LineTradingDeskWindowState) -> None:
        jid = state.desk_annotation_save_job
        if jid is not None:
            try:
                self.root.after_cancel(jid)
            except Exception:
                pass
            state.desk_annotation_save_job = None

    def _line_trading_desk_flush_annotation_persist_job(self, state: LineTradingDeskWindowState) -> None:
        if self._line_trading_desk_window is not state:
            return
        state.desk_annotation_save_job = None
        if not state.desk_annotation_store_key:
            state.desk_annotation_store_key = self._line_trading_desk_annotation_storage_key_for(state)
        self._line_trading_desk_capture_state_into_annotation_entries(state, state.desk_annotation_store_key)
        try:
            save_line_trading_desk_annotations_entries(self._line_trading_desk_annotation_entries)
        except Exception as exc:
            self._enqueue_log(f"保存划线台画线失败：{exc}")

    def _line_trading_desk_schedule_annotation_persist(self, state: LineTradingDeskWindowState) -> None:
        if state is None or self._line_trading_desk_window is not state:
            return
        self._line_trading_desk_cancel_annotation_persist_job(state)
        desk_ref = state

        def _fire() -> None:
            st = self._line_trading_desk_window
            if st is not desk_ref or not _widget_exists(st.window):
                return
            self._line_trading_desk_flush_annotation_persist_job(st)

        state.desk_annotation_save_job = self.root.after(_LINE_DESK_ANNOTATION_PERSIST_MS, _fire)

    def _line_trading_desk_switch_symbol_or_bar_annotation_context(self, state: LineTradingDeskWindowState) -> None:
        """标的或周期变化：先落盘当前键，再载入新键下的射线/盈亏比。"""
        self._line_trading_desk_cancel_annotation_persist_job(state)
        if state.desk_annotation_store_key:
            self._line_trading_desk_capture_state_into_annotation_entries(state, state.desk_annotation_store_key)
        new_key = self._line_trading_desk_annotation_storage_key_for(state)
        self._line_trading_desk_apply_annotation_entry_to_state(state, new_key)
        state.desk_annotation_store_key = new_key
        if state.rr_selected_id and not any(b.rr_id == state.rr_selected_id for b in state.rr_annotations):
            state.rr_selected_id = state.rr_annotations[0].rr_id if state.rr_annotations else None
        self._line_trading_desk_refresh_ray_tree(state)
        self._line_trading_desk_refresh_rr_tree(state)
        try:
            save_line_trading_desk_annotations_entries(self._line_trading_desk_annotation_entries)
        except Exception as exc:
            self._enqueue_log(f"保存划线台画线失败：{exc}")

    def _line_trading_desk_hydrate_annotation_bundle_on_desk_open(self, state: LineTradingDeskWindowState) -> None:
        self._line_trading_desk_cancel_annotation_persist_job(state)
        new_key = self._line_trading_desk_annotation_storage_key_for(state)
        self._line_trading_desk_apply_annotation_entry_to_state(state, new_key)
        state.desk_annotation_store_key = new_key
        if state.rr_selected_id and not any(b.rr_id == state.rr_selected_id for b in state.rr_annotations):
            state.rr_selected_id = state.rr_annotations[0].rr_id if state.rr_annotations else None
        self._line_trading_desk_refresh_ray_tree(state)
        self._line_trading_desk_refresh_rr_tree(state)

    def _line_trading_desk_invalidate_price_tick(self, state: LineTradingDeskWindowState) -> None:
        state.desk_tick_symbol = None

    def _line_trading_desk_update_price_tick(self, state: LineTradingDeskWindowState, *, force: bool = False) -> None:
        sym = state.symbol_var.get().strip().upper()
        if not sym:
            state.desk_price_tick = None
            state.desk_tick_symbol = None
            return
        if (
            not force
            and state.desk_tick_symbol == sym
            and state.desk_price_tick is not None
        ):
            return
        inst = self._line_trading_desk_get_cached_instrument(state, sym)
        if inst is None:
            self._line_trading_desk_prefetch_instrument_async(state, sym)
            state.desk_price_tick = None
            state.desk_tick_symbol = sym
            return
        ts = inst.tick_size
        state.desk_price_tick = ts if ts is not None and ts > 0 else None
        state.desk_tick_symbol = sym

    def _line_trading_desk_format_price(self, state: LineTradingDeskWindowState, value: Decimal | None) -> str:
        if value is None:
            return "-"
        if state.desk_price_tick is not None:
            return format_decimal_by_increment(value, state.desk_price_tick)
        return format_decimal(value)

    def _line_trading_desk_order_ts_cell(self, ts: int | None) -> str:
        if ts is None or ts <= 0:
            return "-"
        try:
            return datetime.fromtimestamp(ts / 1000).strftime("%m-%d %H:%M")
        except (OverflowError, OSError, ValueError):
            return "-"

    def _line_trading_desk_order_price_cell(self, item: OkxTradeOrderItem) -> str:
        for candidate in (item.price, item.order_price, item.trigger_price):
            if candidate is not None and candidate > 0:
                return format_decimal(candidate)
        return "-"

    def _line_trading_desk_order_src_cell(self, item: OkxTradeOrderItem) -> str:
        if item.source_kind == "algo":
            return "算法"
        return "普通"

    def _line_trading_desk_get_cached_instrument(
        self,
        state: LineTradingDeskWindowState,
        inst_id: str,
    ) -> Instrument | None:
        key = inst_id.strip().upper()
        if not key:
            return None
        now = time.monotonic()
        hit = state.desk_instrument_cache.get(key)
        if hit is None:
            return None
        inst, ts = hit
        if now - float(ts) <= _LINE_DESK_INSTRUMENT_CACHE_TTL_S:
            return inst
        return None

    def _line_trading_desk_prefetch_instrument_async(self, state: LineTradingDeskWindowState, inst_id: str) -> None:
        key = inst_id.strip().upper()
        if not key:
            return
        if self._line_trading_desk_get_cached_instrument(state, key) is not None:
            return

        def work() -> None:
            inst: Instrument | None
            try:
                inst = self.client.get_instrument(key)
            except Exception:
                inst = None
            now = time.monotonic()

            def apply() -> None:
                st = self._line_trading_desk_window
                if st is not state or not _widget_exists(st.window):
                    return
                st.desk_instrument_cache[key] = (inst, now)
                if st.symbol_var.get().strip().upper() == key:
                    self._line_trading_desk_update_price_tick(st, force=True)

            self.root.after(0, apply)

        threading.Thread(target=work, daemon=True).start()

    def _line_trading_desk_require_instrument_for_submit(
        self,
        state: LineTradingDeskWindowState,
        symbol: str,
    ) -> Instrument | None:
        inst = self._line_trading_desk_get_cached_instrument(state, symbol)
        if inst is not None:
            return inst
        self._line_trading_desk_prefetch_instrument_async(state, symbol)
        state.status_text.set(f"正在加载 {symbol} 合约信息，请稍后再点一次。")
        return None

    def _line_trading_desk_instrument_for_order_row(
        self,
        state: LineTradingDeskWindowState,
        inst_id: str,
        cache: dict[str, Instrument | None],
    ) -> Instrument | None:
        key = inst_id.strip().upper()
        if key in cache:
            return cache[key]
        inst = self._line_trading_desk_get_cached_instrument(state, key)
        if inst is None:
            # 表格渲染不走同步网络，避免点击下单后刷新时 UI 卡死；后台预热后下一轮自动补全单位显示。
            self._line_trading_desk_prefetch_instrument_async(state, key)
            cache[key] = None
            return None
        cache[key] = inst
        return inst

    def _line_trading_desk_order_qty_cell(
        self,
        state: LineTradingDeskWindowState,
        item: OkxTradeOrderItem,
        *,
        qty: Decimal | None,
        inst_cache: dict[str, Instrument | None],
    ) -> str:
        if qty is None:
            return "-"
        inst = self._line_trading_desk_instrument_for_order_row(state, item.inst_id, inst_cache)
        if inst is None:
            return format_decimal(qty)
        return _format_notify_size_with_unit(inst, qty)

    def _line_trading_desk_position_qty_cell(self, state: LineTradingDeskWindowState, pos: OkxPosition) -> str:
        if pos.position is None:
            return "-"
        inst = self._line_trading_desk_get_cached_instrument(state, pos.inst_id)
        if inst is None:
            self._line_trading_desk_prefetch_instrument_async(state, pos.inst_id)
            return format_decimal(pos.position)
        return _format_notify_size_with_unit(inst, pos.position)

    def _line_trading_desk_confirm_qty_line(self, state: LineTradingDeskWindowState, symbol: str, size: Decimal) -> str:
        inst = self._line_trading_desk_get_cached_instrument(state, symbol.strip().upper())
        if inst is not None:
            return _format_notify_size_with_unit(inst, size)
        return f"{format_decimal(size)} 张（合约）"

    def _line_trading_desk_order_bracket_leg_cell(
        self,
        state: LineTradingDeskWindowState,
        trigger_px: Decimal | None,
        trigger_type: str | None,
        order_px: Decimal | None,
    ) -> str:
        if trigger_px is None:
            return "-"
        line = self._line_trading_desk_format_price(state, trigger_px)
        t = (trigger_type or "").strip().lower()
        if t:
            line = f"{line}·{t}"
        if order_px is not None:
            if order_px <= 0:
                line += "→触发后市价"
            else:
                line += f"→{self._line_trading_desk_format_price(state, order_px)}"
        return line

    def _line_trading_desk_sync_tree_rows(
        self,
        tree: ttk.Treeview,
        rows: list[tuple[str, tuple[object, ...]]],
        *,
        preserve_selected_iid: str | None = None,
    ) -> None:
        bases = [iid for iid, _vals in rows]
        finals = _desk_tree_final_iids_from_bases(bases)
        deduped_rows = [(fin, vals) for fin, (_b, vals) in zip(finals, rows)]
        rows = deduped_rows
        existing = list(tree.get_children())
        existing_set = set(existing)
        wanted = [iid for iid, _vals in rows]
        wanted_set = set(wanted)
        for iid in existing:
            if iid not in wanted_set:
                tree.delete(iid)
        for iid, values in rows:
            if iid in existing_set:
                tree.item(iid, values=values)
            else:
                tree.insert("", END, iid=iid, values=values)
        if preserve_selected_iid and tree.exists(preserve_selected_iid):
            tree.selection_set(preserve_selected_iid)
        elif wanted:
            first = wanted[0]
            if tree.exists(first):
                tree.selection_set(first)

    def _line_trading_desk_order_row_iid(self, item: OkxTradeOrderItem, index: int, *, prefix: str) -> str:
        ident = (
            str(item.order_id or "").strip()
            or str(item.algo_id or "").strip()
            or str(item.client_order_id or "").strip()
            or f"{item.inst_id}|{item.side}|{item.ord_type}|{index}"
        )
        safe = "".join(ch if ch.isalnum() or ch in {"-", "_", ":", "|"} else "_" for ch in ident)
        return f"{prefix}-{safe}"

    def _line_trading_desk_position_row_iid(self, item: OkxPosition, index: int) -> str:
        ident = (
            f"{item.inst_id}|{item.pos_side or '-'}|"
            f"{format_decimal(item.position) if item.position is not None else '-'}|{index}"
        )
        safe = "".join(ch if ch.isalnum() or ch in {"-", "_", ":", "|"} else "_" for ch in ident)
        return f"desk-pos-{safe}"

    def _line_trading_desk_refresh_pending_orders_tree(self, state: LineTradingDeskWindowState) -> None:
        tree = state.pending_orders_tree
        selected = tree.selection()[0] if tree.selection() else None
        inst_cache: dict[str, Instrument | None] = {}
        rows: list[tuple[str, tuple[object, ...]]] = []
        for index, item in enumerate(state.latest_pending_orders):
            iid = self._line_trading_desk_order_row_iid(item, index, prefix="desk-pend")
            rows.append(
                (
                    iid,
                    (
                    item.inst_id,
                    (item.side or "-").upper(),
                    (item.ord_type or "-").lower(),
                    self._line_trading_desk_order_price_cell(item),
                    self._line_trading_desk_order_qty_cell(state, item, qty=item.size, inst_cache=inst_cache),
                    self._line_trading_desk_order_qty_cell(state, item, qty=item.filled_size, inst_cache=inst_cache),
                    self._line_trading_desk_order_bracket_leg_cell(
                        state,
                        item.take_profit_trigger_price,
                        item.take_profit_trigger_price_type,
                        item.take_profit_order_price,
                    ),
                    self._line_trading_desk_order_bracket_leg_cell(
                        state,
                        item.stop_loss_trigger_price,
                        item.stop_loss_trigger_price_type,
                        item.stop_loss_order_price,
                    ),
                    item.state or "-",
                    self._line_trading_desk_order_src_cell(item),
                    self._line_trading_desk_order_ts_cell(item.update_time or item.created_time),
                ),
                )
            )
        self._line_trading_desk_sync_tree_rows(tree, rows, preserve_selected_iid=selected)

    def _line_trading_desk_refresh_order_history_tree(self, state: LineTradingDeskWindowState) -> None:
        tree = state.order_history_tree
        selected = tree.selection()[0] if tree.selection() else None
        inst_cache: dict[str, Instrument | None] = {}
        rows: list[tuple[str, tuple[object, ...]]] = []
        for index, item in enumerate(state.latest_order_history):
            iid = self._line_trading_desk_order_row_iid(item, index, prefix="desk-hist")
            rows.append(
                (
                    iid,
                    (
                    item.inst_id,
                    (item.side or "-").upper(),
                    (item.ord_type or "-").lower(),
                    self._line_trading_desk_order_price_cell(item),
                    self._line_trading_desk_order_qty_cell(state, item, qty=item.size, inst_cache=inst_cache),
                    self._line_trading_desk_order_qty_cell(state, item, qty=item.filled_size, inst_cache=inst_cache),
                    self._line_trading_desk_order_bracket_leg_cell(
                        state,
                        item.take_profit_trigger_price,
                        item.take_profit_trigger_price_type,
                        item.take_profit_order_price,
                    ),
                    self._line_trading_desk_order_bracket_leg_cell(
                        state,
                        item.stop_loss_trigger_price,
                        item.stop_loss_trigger_price_type,
                        item.stop_loss_order_price,
                    ),
                    item.state or "-",
                    self._line_trading_desk_order_src_cell(item),
                    self._line_trading_desk_order_ts_cell(item.update_time or item.created_time),
                ),
                )
            )
        self._line_trading_desk_sync_tree_rows(tree, rows, preserve_selected_iid=selected)

    def _line_trading_desk_refresh_pending_tab(self) -> None:
        """仅重新拉取当前委托（不整图刷新 K 线），减轻请求量。"""
        state = self._line_trading_desk_window
        if state is None or not _widget_exists(state.window):
            return
        symbol = state.symbol_var.get().strip().upper()
        if not symbol:
            state.status_text.set("请先选择标的。")
            return
        profile = state.api_profile_var.get().strip()
        credentials = self._credentials_for_profile_or_none(profile)
        if credentials is None:
            state.status_text.set("未配置所选 API 凭证，无法刷新委托。")
            return
        env_label = self._environment_label_for_profile(profile or self._current_credential_profile())
        environment = ENV_OPTIONS[self._normalized_environment_label(env_label)]
        desk_ref = state
        state.status_text.set(f"正在刷新当前委托：{symbol}…")

        def work() -> None:
            try:
                pending = self.client.get_pending_orders(credentials, environment=environment, limit=100)
            except Exception as exc:
                self.root.after(0, lambda msg=str(exc): self._line_trading_desk_apply_pending_only(desk_ref, None, msg))
                return
            self.root.after(0, lambda rows=pending: self._line_trading_desk_apply_pending_only(desk_ref, rows, None))

        threading.Thread(target=work, daemon=True).start()

    def _line_trading_desk_refresh_order_history_tab(self) -> None:
        """仅重新拉取历史委托（不整图刷新 K 线），减轻请求量。"""
        state = self._line_trading_desk_window
        if state is None or not _widget_exists(state.window):
            return
        symbol = state.symbol_var.get().strip().upper()
        if not symbol:
            state.status_text.set("请先选择标的。")
            return
        profile = state.api_profile_var.get().strip()
        credentials = self._credentials_for_profile_or_none(profile)
        if credentials is None:
            state.status_text.set("未配置所选 API 凭证，无法刷新历史委托。")
            return
        env_label = self._environment_label_for_profile(profile or self._current_credential_profile())
        environment = ENV_OPTIONS[self._normalized_environment_label(env_label)]
        desk_ref = state
        state.status_text.set(f"正在刷新历史委托：{symbol}…")

        def work() -> None:
            try:
                history = self.client.get_order_history(credentials, environment=environment, limit=100)
            except Exception as exc:
                self.root.after(0, lambda msg=str(exc): self._line_trading_desk_apply_order_history_only(desk_ref, None, msg))
                return
            self.root.after(0, lambda rows=history: self._line_trading_desk_apply_order_history_only(desk_ref, rows, None))

        threading.Thread(target=work, daemon=True).start()

    def _line_trading_desk_apply_pending_only(
        self,
        desk_ref: LineTradingDeskWindowState,
        pending: list[OkxTradeOrderItem] | None,
        err: str | None,
    ) -> None:
        st = self._line_trading_desk_window
        if st is not desk_ref or not _widget_exists(st.window):
            return
        sym = st.symbol_var.get().strip().upper()
        if err is not None:
            st.status_text.set(f"刷新当前委托失败：{err}")
            self._line_trading_desk_dual_log(st, f"刷新当前委托失败 | {sym} | {err}")
            return
        filtered = [o for o in (pending or []) if o.inst_id.strip().upper() == sym]
        st.latest_pending_orders = filtered
        self._line_trading_desk_refresh_pending_orders_tree(st)
        st.status_text.set(f"已刷新当前委托 | {sym} | {len(filtered)} 条")
        self._line_trading_desk_dual_log(st, f"已刷新当前委托 | {sym} | {len(filtered)} 条")

    def _line_trading_desk_apply_order_history_only(
        self,
        desk_ref: LineTradingDeskWindowState,
        history: list[OkxTradeOrderItem] | None,
        err: str | None,
    ) -> None:
        st = self._line_trading_desk_window
        if st is not desk_ref or not _widget_exists(st.window):
            return
        sym = st.symbol_var.get().strip().upper()
        if err is not None:
            st.status_text.set(f"刷新历史委托失败：{err}")
            self._line_trading_desk_dual_log(st, f"刷新历史委托失败 | {sym} | {err}")
            return
        filtered = [o for o in (history or []) if o.inst_id.strip().upper() == sym][:80]
        st.latest_order_history = filtered
        self._line_trading_desk_refresh_order_history_tree(st)
        st.status_text.set(f"已刷新历史委托 | {sym} | {len(filtered)} 条")
        self._line_trading_desk_dual_log(st, f"已刷新历史委托 | {sym} | {len(filtered)} 条")

    def _line_trading_desk_log_prefix(self, state: LineTradingDeskWindowState) -> str:
        p = state.api_profile_var.get().strip()
        if p:
            return f"[{p}] [划线交易台]"
        return "[划线交易台]"

    def _line_trading_desk_local_log(self, state: LineTradingDeskWindowState, detail: str) -> None:
        """Append one line to the desk-only log panel and ``logs/line_desk/YYYY-MM-DD.log``."""
        raw = (detail or "").strip()
        if not raw:
            return
        sym = state.symbol_var.get().strip() or "-"
        body = f"{sym} | {raw}"
        line = ensure_log_timestamp(body, timestamp=current_log_timestamp())
        append_line_desk_log_line(line)

        def append_widget() -> None:
            w = state.desk_log_text
            if w is None or not _widget_exists(w):
                return
            try:
                w.configure(state="normal")
                w.insert(END, line + "\n")
                total_lines = int(float(w.index("end-1c").split(".")[0]))
                if total_lines > 3000:
                    w.delete("1.0", "501.0")
                w.see(END)
                w.configure(state="disabled")
            except TclError:
                return

        try:
            state.window.after(0, append_widget)
        except TclError:
            return

    def _line_trading_desk_dual_log(self, state: LineTradingDeskWindowState, detail: str) -> None:
        """Same line to main run log and to the line-trading desk panel."""
        d = (detail or "").strip()
        if not d:
            return
        pr = self._line_trading_desk_log_prefix(state)
        self._enqueue_log(f"{pr} {d}")
        self._line_trading_desk_local_log(state, d)

    def _line_trading_desk_sz_log_segment(self, inst_id: str, contracts: Decimal) -> str:
        try:
            inst = self.client.get_instrument(inst_id)
        except Exception:
            return f"sz={format_decimal(contracts)}"
        return f"sz={_format_size_with_contract_equivalent(inst, contracts)}"

    def _line_trading_desk_spawn_rr_limit_network(
        self,
        state: LineTradingDeskWindowState,
        credentials: Credentials,
        config: StrategyConfig,
        plan: OrderPlan,
        size: Decimal,
        symbol: str,
        entry: Decimal,
        sl: Decimal,
        tp: Decimal,
        direction: str,
        *,
        desk_ref: LineTradingDeskWindowState,
        busy_message: str,
        error_prefix: str,
        log_title: str,
        id_label: str,
        use_trigger_algo: bool,
        success_status_for_rid: Callable[[str], str],
    ) -> None:
        if state.desk_submit_inflight:
            state.status_text.set("已有委托正在提交，请稍候…")
            return
        state.desk_submit_inflight = True
        state.status_text.set(busy_message)

        def work() -> None:
            plain_limit_fallback = False
            retried_plain_limit = [False]
            try:
                if use_trigger_algo:
                    result = self.client.place_trigger_limit_algo_order(
                        credentials,
                        config,
                        plan,
                        include_take_profit=True,
                        include_attached_protection=True,
                    )
                else:
                    try:
                        result = self.client.place_limit_order(
                            credentials,
                            config,
                            plan,
                            include_take_profit=True,
                            include_attached_protection=True,
                        )
                    except OkxApiError as bulk_exc:
                        # OKX 常对「限价 + 交易所附带 TP/SL」整笔给 sCode=1 / 操作全部失败且无子项说明
                        if "操作全部失败" in str(bulk_exc):
                            retried_plain_limit[0] = True
                            result = self.client.place_limit_order(
                                credentials,
                                config,
                                plan,
                                include_take_profit=False,
                                include_attached_protection=False,
                            )
                            plain_limit_fallback = True
                        else:
                            raise
            except Exception as exc:
                err = str(exc).strip() or exc.__class__.__name__
                if isinstance(exc, OkxApiError):
                    code = getattr(exc, "code", None)
                    if code not in (None, "", "-"):
                        if f"code={code}" not in err:
                            err = f"{err} | code={code}"
                if retried_plain_limit[0]:
                    err = f"{err} | 已去掉附带TP/SL重试仍失败"

                def fail() -> None:
                    st = self._line_trading_desk_window
                    if st is not desk_ref or not _widget_exists(st.window):
                        return
                    st.desk_submit_inflight = False
                    st.status_text.set(f"{error_prefix}：{err}")
                    self._line_trading_desk_dual_log(
                        st,
                        f"{log_title}失败 | {direction.upper()} | {symbol} | "
                        f"入={self._line_trading_desk_format_price(st, entry)} | "
                        f"损={self._line_trading_desk_format_price(st, sl)} | "
                        f"盈={self._line_trading_desk_format_price(st, tp)} | {err}",
                    )

                self.root.after(0, fail)
                return

            rid = (result.ord_id or "-").strip() or "-"

            def done() -> None:
                st = self._line_trading_desk_window
                if st is not desk_ref or not _widget_exists(st.window):
                    return
                st.desk_submit_inflight = False
                if plain_limit_fallback:
                    st.status_text.set(
                        f"已提交限价开仓（未附带交易所TP/SL）| {symbol} | sz={format_decimal(size)} | ordId={rid}"
                    )
                else:
                    st.status_text.set(success_status_for_rid(rid))
                sz_seg = self._line_trading_desk_sz_log_segment(symbol, size)
                self._line_trading_desk_dual_log(
                    st,
                    f"{log_title} | {direction.upper()} | {symbol} | "
                    f"入={self._line_trading_desk_format_price(st, entry)} | "
                    f"损={self._line_trading_desk_format_price(st, sl)} | 盈={self._line_trading_desk_format_price(st, tp)} | "
                    f"{sz_seg} | {id_label}={rid}",
                )
                if plain_limit_fallback:
                    self._line_trading_desk_dual_log(
                        st,
                        "说明：交易所拒绝了「限价+附带TP/SL」整笔，已自动改为**仅限价开仓**；"
                        "请在成交后于网页/APP 或「触发价委托」单独挂止盈止损。",
                    )
                self._request_line_trading_desk_refresh(immediate=False)

            self.root.after(0, done)
            self.root.after(
                0,
                lambda: (
                    setattr(desk_ref, "desk_submit_inflight", False)
                    if self._line_trading_desk_window is desk_ref and _widget_exists(desk_ref.window)
                    else None
                ),
            )

        threading.Thread(target=work, daemon=True).start()

    def _on_line_trading_desk_api_profile_changed(self) -> None:
        state = self._line_trading_desk_window
        if state is None or not _widget_exists(state.window):
            return
        current = state.api_profile_var.get().strip()
        prev = (state.last_desk_api_profile or "").strip()
        # Combobox 可能短暂写成空串再写回；仅当新旧均为非空且不同时才清空画线，避免误清盈亏比。
        if prev and current and current != prev:
            self._line_trading_desk_cancel_annotation_persist_job(state)
            if state.desk_annotation_store_key:
                self._line_trading_desk_capture_state_into_annotation_entries(state, state.desk_annotation_store_key)
                try:
                    save_line_trading_desk_annotations_entries(self._line_trading_desk_annotation_entries)
                except Exception as exc:
                    self._enqueue_log(f"保存划线台画线失败：{exc}")
            self._line_trading_desk_cancel_motion_chart_paint(state)
            self._line_trading_desk_cancel_chart_throttle(state)
            state.line_annotations.clear()
            locked_rr = [b for b in state.rr_annotations if b.locked]
            state.rr_annotations.clear()
            state.rr_annotations.extend(locked_rr)
            state.draft_line_start = None
            state.draft_line_current = None
            state.rr_drag = None
            state.rr_pick = None
            if state.rr_selected_id and not any(b.rr_id == state.rr_selected_id for b in state.rr_annotations):
                state.rr_selected_id = state.rr_annotations[0].rr_id if state.rr_annotations else None
            state.desk_range_zoom_active = False
            self._line_trading_desk_invalidate_price_tick(state)
        state.last_desk_api_profile = current
        state.desk_annotation_store_key = self._line_trading_desk_annotation_storage_key_for(state)
        self._request_line_trading_desk_refresh(immediate=True)

    def _line_trading_desk_reset_chart_view(self) -> None:
        state = self._line_trading_desk_window
        if state is None:
            return
        state.desk_visible_bars = 200
        state.desk_range_zoom_active = False
        state.desk_initial_scroll_done = False
        snap = state.last_snapshot
        if snap and snap.candles:
            n = len(snap.candles)
            state.desk_view_start = max(0, n - min(state.desk_visible_bars, n))
            state.desk_initial_scroll_done = True
        else:
            state.desk_view_start = 0
        self._render_line_trading_desk_chart(force=True)

    def _on_line_trading_desk_symbol_or_bar_changed(self) -> None:
        state = self._line_trading_desk_window
        if state is not None:
            state.desk_initial_scroll_done = False
            state.desk_range_zoom_active = False
            self._line_trading_desk_invalidate_price_tick(state)
            self._line_trading_desk_update_price_tick(state)
            symbol = state.symbol_var.get().strip().upper()
            if symbol:
                self._line_trading_desk_prefetch_instrument_async(state, symbol)
        self._request_line_trading_desk_refresh(immediate=True)

    def _line_trading_desk_desk_anchor_prices(
        self, state: LineTradingDeskWindowState, *, sliced: StrategyLiveChartSnapshot | None = None
    ) -> tuple[Decimal, ...]:
        """纳入 desk 纵轴计算。用户射线与盈亏比锚点价一律并入纵轴范围。

        若按可见 OHLC 加宽边带过滤锚点价，新 K 线推动边带后锚点会反复进出过滤结果，
        ``strategy_live_chart_price_bounds`` 的上下界随之跳变，同一组 (bar, price) 映射到像素会漂移，
        表现为趋势线「自己挪动」。远端锚点宁可略压缩蜡烛视觉占比，也应保持几何稳定。
        """
        _ = sliced  # 保留参数供调用方按「当前切片」语义传入，纵轴不再按切片过滤锚点价。
        line_prices: list[Decimal] = []
        for ann in state.line_annotations:
            if ann.price_a is not None:
                line_prices.append(ann.price_a)
            if ann.price_b is not None:
                line_prices.append(ann.price_b)
        rr_prices: list[Decimal] = []
        for box in state.rr_annotations:
            rr_prices.append(box.price_entry)
            rr_prices.append(box.price_stop)
            rr_prices.append(box.price_tp)
        return tuple(line_prices + rr_prices)

    def _line_trading_desk_visible_snapshot(
        self, state: LineTradingDeskWindowState
    ) -> tuple[StrategyLiveChartSnapshot, int, int] | None:
        snap = state.last_snapshot
        if snap is None or not snap.candles:
            return None
        n = len(snap.candles)
        min_vis = 5 if state.desk_range_zoom_active else 30
        vb = line_trading_desk_visible_bar_count(n, state.desk_visible_bars, min_bars=min_vis)
        state.desk_visible_bars = vb
        vs_max = line_trading_desk_max_view_start(n, vb, min_visible_bars=min_vis)
        vs = max(0, min(int(state.desk_view_start), vs_max))
        state.desk_view_start = vs
        sliced = slice_strategy_live_chart_snapshot_with_desk_right_pad(snap, vs, vb, min_visible_bars=min_vis)
        return sliced, vs, vb

    def _line_trading_desk_pixel_to_bar_price(
        self, state: LineTradingDeskWindowState, x: float, y: float
    ) -> tuple[float, Decimal] | None:
        """与当前画布一致：按「可见切片」K 线换算 bar 与价格（全量布局会导致错位、线飞到图外）。"""
        tup = self._line_trading_desk_visible_snapshot(state)
        if tup is None or state.last_snapshot is None:
            return None
        sliced, vs, _vb = tup
        if not sliced.candles:
            return None
        to_draw = replace(sliced, price_display_tick=state.desk_price_tick)
        anchors = self._line_trading_desk_desk_anchor_prices(state, sliced=sliced)
        lay = compute_strategy_live_chart_layout(
            state.canvas, to_draw, bounds_policy="desk", desk_anchor_prices=anchors
        )
        if lay is None:
            return None
        rel_bar = layout_pixel_to_bar_index(lay, x)
        n_vis = len(sliced.candles)
        rel_bar = min(max(rel_bar, 0.0), float(max(0, n_vis - 1)))
        abs_bar = float(vs) + rel_bar
        price = layout_y_to_price(lay, y)
        return abs_bar, price

    def _line_trading_desk_slice_layout(self, state: LineTradingDeskWindowState) -> StrategyLiveChartLayout | None:
        tup = self._line_trading_desk_visible_snapshot(state)
        if tup is None:
            return None
        sliced, _vs, _vb = tup
        to_draw = replace(sliced, price_display_tick=state.desk_price_tick)
        anchors = self._line_trading_desk_desk_anchor_prices(state, sliced=sliced)
        return compute_strategy_live_chart_layout(
            state.canvas, to_draw, bounds_policy="desk", desk_anchor_prices=anchors
        )

    def _on_line_trading_desk_mouse_wheel(self, event) -> None:
        state = self._line_trading_desk_window
        if state is None or not _widget_exists(state.canvas):
            return
        snap = state.last_snapshot
        if not snap or not snap.candles:
            return
        n = len(snap.candles)
        delta = getattr(event, "delta", 0)
        num = getattr(event, "num", 0)
        zoom_in = delta > 0 or num == 4
        floor_b = 5 if state.desk_range_zoom_active else 40
        if zoom_in:
            state.desk_visible_bars = max(floor_b, state.desk_visible_bars - max(5, state.desk_visible_bars // 12))
        else:
            state.desk_visible_bars = min(n, int(state.desk_visible_bars * 1.15) + 10)
        self._line_trading_desk_clamp_view(state)
        self._render_line_trading_desk_chart()

    def _line_trading_desk_clamp_view(self, state: LineTradingDeskWindowState) -> None:
        snap = state.last_snapshot
        if not snap or not snap.candles:
            state.desk_view_start = 0
            return
        n = len(snap.candles)
        min_vis = 5 if state.desk_range_zoom_active else 30
        vb = line_trading_desk_visible_bar_count(n, state.desk_visible_bars, min_bars=min_vis)
        state.desk_visible_bars = vb
        vs_max = line_trading_desk_max_view_start(n, vb, min_visible_bars=min_vis)
        state.desk_view_start = max(0, min(int(state.desk_view_start), vs_max))

    def _line_trading_desk_closest_bar_index_for_ts(self, candles: tuple[Candle, ...], ts: int) -> int:
        if not candles:
            return 0
        best_i = 0
        best_d = abs(int(candles[0].ts) - int(ts))
        for i, c in enumerate(candles):
            d = abs(int(c.ts) - int(ts))
            if d < best_d:
                best_d = d
                best_i = i
        return best_i

    def _line_trading_desk_remap_bar_coordinate(
        self,
        old_candles: tuple[Candle, ...],
        new_candles: tuple[Candle, ...],
        bar_f: float,
    ) -> float:
        if not new_candles:
            return 0.0
        hi_new = float(len(new_candles) - 1)
        if not old_candles:
            return min(max(float(bar_f), 0.0), hi_new)
        max_old = float(len(old_candles) - 1)
        clamped = min(max(float(bar_f), 0.0), max_old)
        idx = int(round(clamped))
        idx = max(0, min(idx, len(old_candles) - 1))
        target_ts = int(old_candles[idx].ts)
        j = self._line_trading_desk_closest_bar_index_for_ts(new_candles, target_ts)
        return float(min(max(j, 0), len(new_candles) - 1))

    def _line_trading_desk_should_remap_annotations_for_candle_change(
        self,
        state: LineTradingDeskWindowState,
        old_candles: tuple[Candle, ...],
        new_candles: tuple[Candle, ...],
    ) -> bool:
        """K 线根数变化（如先 280 再补到 500）时，绝对 bar 索引会错位，需按时间戳重绑。"""
        if len(old_candles) != len(new_candles):
            return True
        n = len(new_candles)
        if n == 0:
            return False
        hi = float(n - 1)
        for ann in state.line_annotations:
            for b in (ann.bar_a, ann.bar_b):
                if b is None:
                    continue
                bf = float(b)
                if bf < 0.0 or bf > hi:
                    return True
        for box in state.rr_annotations:
            if float(box.bar_entry) < 0.0 or float(box.bar_entry) > hi or float(box.bar_stop) < 0.0 or float(box.bar_stop) > hi:
                return True
        return False

    def _line_trading_desk_remap_annotations_bars_after_candle_reload(
        self,
        state: LineTradingDeskWindowState,
        old_candles: tuple[Candle, ...],
        new_candles: tuple[Candle, ...],
    ) -> None:
        for ann in state.line_annotations:
            if ann.bar_a is not None:
                ann.bar_a = self._line_trading_desk_remap_bar_coordinate(old_candles, new_candles, float(ann.bar_a))
            if ann.bar_b is not None:
                ann.bar_b = self._line_trading_desk_remap_bar_coordinate(old_candles, new_candles, float(ann.bar_b))
        for box in state.rr_annotations:
            box.bar_entry = self._line_trading_desk_remap_bar_coordinate(old_candles, new_candles, float(box.bar_entry))
            box.bar_stop = self._line_trading_desk_remap_bar_coordinate(old_candles, new_candles, float(box.bar_stop))

    def _on_line_trading_desk_button_press(self, event) -> None:
        state = self._line_trading_desk_window
        if state is None:
            return
        if getattr(event, "state", 0) & 0x0001:
            self._line_trading_desk_clamp_view(state)
            state.desk_pan_origin = (float(event.x), int(state.desk_view_start))
            return
        if state.active_tool != "none":
            state.draft_line_start = (float(event.x), float(event.y))
            state.draft_line_current = (float(event.x), float(event.y))
            self._render_line_trading_desk_chart(force=True)
            return
        hit = self._line_trading_desk_rr_hit_test(state, float(event.x), float(event.y))
        if hit is not None:
            box, handle = hit
            state.rr_selected_id = box.rr_id
            if box.locked:
                state.rr_pick = None
            else:
                state.rr_pick = (box.rr_id, handle, float(event.x), float(event.y))
            self._line_trading_desk_refresh_rr_tree(state)
            self._render_line_trading_desk_chart(force=True)
            return
        if state.desk_range_zoom_active:
            self._line_trading_desk_clamp_view(state)
            state.desk_pan_origin = (float(event.x), int(state.desk_view_start))
            self._render_line_trading_desk_chart(force=True)
            return
        state.rr_selected_id = None
        state.rr_pick = None
        self._line_trading_desk_refresh_rr_tree(state)
        self._render_line_trading_desk_chart(force=True)

    def _on_line_trading_desk_button_release(self, event) -> None:
        state = self._line_trading_desk_window
        if state is None:
            return
        if state.desk_pan_origin is not None:
            state.desk_pan_origin = None
            return
        if state.rr_pick is not None:
            state.rr_pick = None
            self._line_trading_desk_refresh_rr_tree(state)
            self._render_line_trading_desk_chart(force=True)
            return
        if state.rr_drag is not None:
            rid, _handle = state.rr_drag
            state.rr_drag = None
            box = next((b for b in state.rr_annotations if b.rr_id == rid), None)
            if box is not None:
                self._line_trading_desk_rr_snap_prices(state, box)
                self._line_trading_desk_dual_log(
                    state,
                    f"盈亏比 | 拖动调整 | id={rid} | 入={self._line_trading_desk_format_price(state, box.price_entry)} | "
                    f"损={self._line_trading_desk_format_price(state, box.price_stop)} | "
                    f"盈={self._line_trading_desk_format_price(state, box.price_tp)}",
                )
            self._line_trading_desk_refresh_rr_tree(state)
            self._render_line_trading_desk_chart(force=True)
            return
        if state.draft_line_start is None:
            return
        self._line_trading_desk_complete_draw(state, float(event.x), float(event.y))
        state.draft_line_start = None
        state.draft_line_current = None
        self._render_line_trading_desk_chart(force=True)

    def _line_trading_desk_complete_draw(self, state: LineTradingDeskWindowState, x: float, y: float) -> None:
        tool = state.active_tool
        start = state.draft_line_start
        if start is None or state.last_snapshot is None or not state.last_snapshot.candles:
            return
        end = (x, y)
        if tool in ("horizontal", "zoom_range"):
            end = (x, start[1])
        color = "#1d4ed8"
        label = "趋势线"
        if tool == "horizontal":
            color = "#7c3aed"
            label = "水平射线"
        elif tool == "zoom_range":
            color = "#0f766e"
            label = "区间放大"
        elif tool == "stop":
            color = "#cf222e"
            label = "止损线"
        elif tool == "rr":
            color = "#0b7285"
            label = "盈亏比"
        p0 = self._line_trading_desk_pixel_to_bar_price(state, start[0], start[1])
        p1 = self._line_trading_desk_pixel_to_bar_price(state, end[0], end[1])
        if p0 is None or p1 is None:
            state.status_text.set("无法在图上解析价格，请重试。")
            return
        bar_a, price_a = p0
        bar_b, price_b = p1
        if tool == "horizontal":
            price_b = price_a = (price_a + price_b) / Decimal("2")
        if tool == "zoom_range":
            n_c = len(state.last_snapshot.candles)
            ba = float(bar_a)
            bb = float(bar_b)
            a = max(0, min(int(round(min(ba, bb))), n_c - 1))
            b = max(0, min(int(round(max(ba, bb))), n_c - 1))
            if a > b:
                a, b = b, a
            span = b - a + 1
            if span < 3:
                state.status_text.set("请拖选稍长一些的区间（至少约 3 根K）。")
                return
            state.desk_range_zoom_active = True
            state.desk_visible_bars = span
            state.desk_view_start = a
            state.active_tool = "none"
            self._line_trading_desk_clamp_view(state)
            state.status_text.set(
                f"已局部放大：仅显示约第 {a + 1}～{b + 1} 根K（共 {span} 根），区间外已隐藏。点「重置视图」恢复。"
            )
            self._line_trading_desk_dual_log(state, f"区间放大 | bars {a}..{b} | span={span}")
            return
        if tool == "stop":
            avg_p = (price_a + price_b) / Decimal("2")
            state.line_annotations.append(
                LiveChartLineAnnotation(
                    kind="stop",
                    x1=start[0],
                    y1=start[1],
                    x2=end[0],
                    y2=end[1],
                    color=color,
                    label=label,
                    bar_a=bar_a,
                    price_a=avg_p,
                    bar_b=bar_b,
                    price_b=avg_p,
                )
            )
            state.status_text.set("已添加止损线。")
            self._line_trading_desk_dual_log(
                state,
                f"线 | 止损 | 参考价={self._line_trading_desk_format_price(state, avg_p)}",
            )
            self._line_trading_desk_schedule_annotation_persist(state)
            return
        if tool == "rr":
            try:
                r_mult = Decimal(str(state.param_rr_r.get().strip() or "2"))
            except InvalidOperation:
                r_mult = Decimal("2")
            if r_mult <= 0:
                r_mult = Decimal("2")
            side = state.param_rr_side.get().strip().lower() or "long"
            if side not in ("long", "short"):
                side = "long"
            if side == "long":
                entry_p = max(price_a, price_b)
                stop_p = min(price_a, price_b)
                risk = entry_p - stop_p
                if risk <= 0:
                    state.status_text.set("多头盈亏比：止损价须低于入场价。")
                    return
                tp_p = entry_p + r_mult * risk
            else:
                entry_p = min(price_a, price_b)
                stop_p = max(price_a, price_b)
                risk = stop_p - entry_p
                if risk <= 0:
                    state.status_text.set("空头盈亏比：止损价须高于入场价。")
                    return
                tp_p = entry_p - r_mult * risk
            bar_mid = (bar_a + bar_b) / 2.0
            rid = uuid.uuid4().hex[:12]
            state.rr_annotations.append(
                DeskRiskRewardAnnotation(
                    rr_id=rid,
                    side=side,
                    bar_entry=bar_mid,
                    price_entry=entry_p,
                    bar_stop=bar_mid,
                    price_stop=stop_p,
                    price_tp=tp_p,
                    r_multiple=r_mult,
                    locked=False,
                )
            )
            state.rr_selected_id = rid
            self._line_trading_desk_refresh_rr_tree(state)
            state.active_tool = "none"
            state.status_text.set(
                f"已添加{('多' if side == 'long' else '空')}头盈亏框。工具已关闭；点框选中后可拖动；需要再画请再点「盈亏比·多/空」。"
            )
            self._line_trading_desk_dual_log(
                state,
                f"盈亏比 | {side} | 入={self._line_trading_desk_format_price(state, entry_p)} | "
                f"损={self._line_trading_desk_format_price(state, stop_p)} | 盈={self._line_trading_desk_format_price(state, tp_p)} | R={format_decimal(r_mult)}",
            )
            self._line_trading_desk_schedule_annotation_persist(state)
            return
        if tool == "line":
            # 趋势线需明显拖拽，单击易误出一条短线；水平射线仍可「点两下」极小位移即成。
            dx = float(end[0]) - float(start[0])
            dy = float(end[1]) - float(start[1])
            if dx * dx + dy * dy < 144.0:  # 约 12 像素半径
                state.status_text.set("趋势线：请按住拖开一点再松开；误触太短已忽略。再画请再点「趋势线」。")
                return
        ray_action = _line_trading_desk_normalize_ray_action(state.param_ray_action.get())
        state.line_annotations.append(
            LiveChartLineAnnotation(
                kind=tool,
                x1=start[0],
                y1=start[1],
                x2=end[0],
                y2=end[1],
                color=color,
                label=label,
                bar_a=bar_a,
                price_a=price_a,
                bar_b=bar_b,
                price_b=price_b,
                desk_ray_action=ray_action,
                desk_ray_triggered=False,
                desk_ray_submit_pending=False,
                desk_ray_last_side=None,
                locked=False,
            )
        )
        # 每笔只画一条，画完退回「无」工具，避免连点画布叠多条或与盈亏比拖拽冲突。
        state.active_tool = "none"
        state.status_text.set(
            f"已添加{label}（工具已收起），射线默认动作={_LINE_DESK_RAY_ACTION_LABEL_ZH.get(ray_action, ray_action)}；"
            "再画请重新点按钮。"
        )
        self._line_trading_desk_dual_log(
            state,
            f"线 | {label} | 默认动作={_LINE_DESK_RAY_ACTION_LABEL_ZH.get(ray_action, ray_action)} | "
            f"端点价={self._line_trading_desk_format_price(state, price_a)}→{self._line_trading_desk_format_price(state, price_b)}",
        )
        self._line_trading_desk_refresh_ray_tree(state)
        self._line_trading_desk_schedule_annotation_persist(state)

    def _line_trading_desk_ray_horizontal_price_text(
        self, state: LineTradingDeskWindowState, ann: LiveChartLineAnnotation
    ) -> str:
        pa, pb = ann.price_a, ann.price_b
        if pa is None and pb is None:
            return "-"
        if ann.kind == "horizontal" or (pa is not None and pb is not None and pa == pb):
            p = pa if pa is not None else pb
            return self._line_trading_desk_format_price(state, p)
        if pa is not None and pb is not None:
            return (
                f"{self._line_trading_desk_format_price(state, pa)}→"
                f"{self._line_trading_desk_format_price(state, pb)}"
            )
        return self._line_trading_desk_format_price(state, pa if pa is not None else pb)

    def _line_trading_desk_refresh_ray_tree(self, state: LineTradingDeskWindowState) -> None:
        tree = state.ray_tree
        tree.delete(*tree.get_children())
        for index, ann in enumerate(state.line_annotations):
            if ann.kind not in ("line", "horizontal"):
                continue
            if ann.bar_a is None:
                continue
            if ann.desk_ray_triggered:
                st = "已触发"
            elif ann.desk_ray_submit_pending:
                st = "提交中"
            else:
                st = "待命"
            tree.insert(
                "",
                END,
                iid=f"ray-{index}",
                values=(
                    "锁" if getattr(ann, "locked", False) else "",
                    ann.label,
                    self._line_trading_desk_ray_horizontal_price_text(state, ann),
                    _LINE_DESK_RAY_ACTION_LABEL_ZH.get(ann.desk_ray_action, ann.desk_ray_action),
                    st,
                ),
            )

    def _on_line_trading_desk_ray_tree_double_click(self, event) -> None:
        """未触发/未在提交中的射线：双击「水平价」列可改价（水平线改两端同价；趋势线改末端价）。"""
        state = self._line_trading_desk_window
        tree = event.widget
        if state is None or tree is not state.ray_tree or not _widget_exists(state.window):
            return
        if tree.identify_column(event.x) != "#3":
            return
        row = tree.identify_row(event.y)
        if not row or not str(row).startswith("ray-"):
            return
        try:
            idx = int(str(row)[4:])
        except ValueError:
            return
        if idx < 0 or idx >= len(state.line_annotations):
            return
        ann = state.line_annotations[idx]
        if ann.kind not in ("line", "horizontal") or ann.bar_a is None:
            return
        if getattr(ann, "locked", False):
            messagebox.showinfo("改价", "已锁定的射线不能改价；请先在列表中「切换选中锁定」解锁。", parent=state.window)
            return
        if ann.desk_ray_triggered or ann.desk_ray_submit_pending:
            messagebox.showinfo("改价", "已触发或提交中的射线不能修改价格。", parent=state.window)
            return
        sym = state.symbol_var.get().strip().upper()
        self._line_trading_desk_update_price_tick(state)
        inst = self._line_trading_desk_get_cached_instrument(state, sym)
        tick_inc: Decimal | None = None
        if inst is not None and inst.tick_size is not None and inst.tick_size > 0:
            tick_inc = inst.tick_size
        elif state.desk_price_tick is not None and state.desk_tick_symbol == sym:
            tick_inc = state.desk_price_tick

        def _snap(p: Decimal) -> Decimal:
            if tick_inc is None or tick_inc <= 0:
                return p
            return snap_to_increment(p, tick_inc, "nearest")

        def _fmt_px(p: Decimal) -> str:
            return format_decimal_by_increment(p, tick_inc) if tick_inc is not None else format_decimal(p)

        tick_hint = f"最小报价单位 {format_decimal(tick_inc)}；" if tick_inc is not None else ""

        if ann.kind == "horizontal" or (
            ann.price_a is not None and ann.price_b is not None and ann.price_a == ann.price_b
        ):
            cur = ann.price_a if ann.price_a is not None else ann.price_b
            if cur is None:
                return
            cur_snapped = _snap(cur)
            raw = simpledialog.askstring(
                "修改水平价",
                f"标的 {sym or '-'}，请输入新价格（{tick_hint}保存时按该单位对齐）。",
                initialvalue=_fmt_px(cur_snapped),
                parent=state.window,
            )
            if raw is None:
                return
            try:
                new_p = Decimal(str(raw).strip().replace(",", ""))
            except InvalidOperation:
                messagebox.showerror("改价", "价格格式无效。", parent=state.window)
                return
            if new_p <= 0:
                messagebox.showerror("改价", "价格须大于 0。", parent=state.window)
                return
            snapped = _snap(new_p)
            old = cur
            ann.price_a = ann.price_b = snapped
            ann.desk_ray_last_side = None
            state.status_text.set(
                f"已改射线 #{idx + 1} 水平价 {_fmt_px(old)} → {_fmt_px(snapped)}（穿越侧已重置）"
            )
            self._line_trading_desk_dual_log(
                state,
                f"射线改价 | {sym or '-'} | #{idx + 1} 水平 | {_fmt_px(old)}→{_fmt_px(snapped)}",
            )
        else:
            if ann.price_b is None:
                return
            b0 = _snap(ann.price_b)
            raw = simpledialog.askstring(
                "修改射线末端价",
                f"标的 {sym or '-'}，修改趋势线末端价（K线索引不变，仅竖直方向；{tick_hint}保存时对齐）。",
                initialvalue=_fmt_px(b0),
                parent=state.window,
            )
            if raw is None:
                return
            try:
                new_p = Decimal(str(raw).strip().replace(",", ""))
            except InvalidOperation:
                messagebox.showerror("改价", "价格格式无效。", parent=state.window)
                return
            if new_p <= 0:
                messagebox.showerror("改价", "价格须大于 0。", parent=state.window)
                return
            snapped = _snap(new_p)
            old_b = ann.price_b
            ann.price_b = snapped
            ann.desk_ray_last_side = None
            state.status_text.set(
                f"已改射线 #{idx + 1} 末端价 {_fmt_px(old_b)} → {_fmt_px(snapped)}（穿越侧已重置）"
            )
            self._line_trading_desk_dual_log(
                state,
                f"射线改价 | {sym or '-'} | #{idx + 1} 末端 | {_fmt_px(old_b)}→{_fmt_px(snapped)}",
            )
        self._line_trading_desk_refresh_ray_tree(state)
        self._render_line_trading_desk_chart(force=True)
        self._line_trading_desk_schedule_annotation_persist(state)

    def _line_trading_desk_on_ray_tree_select(self) -> None:
        pass

    def _line_trading_desk_toggle_selected_ray_lock(self) -> None:
        state = self._line_trading_desk_window
        if state is None or not _widget_exists(state.window):
            return
        sel = state.ray_tree.selection()
        if not sel:
            messagebox.showinfo("射线", "请先在射线列表中选择一行。", parent=state.window)
            return
        row = sel[0]
        if not str(row).startswith("ray-"):
            return
        try:
            idx = int(str(row)[4:])
        except ValueError:
            return
        if idx < 0 or idx >= len(state.line_annotations):
            return
        ann = state.line_annotations[idx]
        if ann.kind not in ("line", "horizontal"):
            return
        ann.locked = not getattr(ann, "locked", False)
        self._line_trading_desk_dual_log(
            state,
            f"射线 | 锁定切换 | #{idx + 1} {ann.label} | 现在={'已锁定' if ann.locked else '未锁定'}",
        )
        self._line_trading_desk_refresh_ray_tree(state)
        self._render_line_trading_desk_chart(force=True)
        self._line_trading_desk_schedule_annotation_persist(state)

    def _line_trading_desk_delete_selected_ray(self) -> None:
        state = self._line_trading_desk_window
        if state is None or not _widget_exists(state.window):
            return
        sel = state.ray_tree.selection()
        if not sel:
            messagebox.showinfo("射线", "请先在射线列表中选择一行。", parent=state.window)
            return
        row = sel[0]
        if not str(row).startswith("ray-"):
            return
        try:
            idx = int(str(row)[4:])
        except ValueError:
            return
        if idx < 0 or idx >= len(state.line_annotations):
            return
        ann = state.line_annotations[idx]
        if ann.kind not in ("line", "horizontal"):
            messagebox.showinfo("射线", "所选行不是趋势线/水平射线。", parent=state.window)
            return
        del state.line_annotations[idx]
        state.ray_tree.selection_remove(sel)
        self._line_trading_desk_dual_log(state, f"射线 | 已删除列表项 #{idx + 1}（{ann.label}）")
        self._line_trading_desk_refresh_ray_tree(state)
        self._render_line_trading_desk_chart(force=True)
        self._line_trading_desk_schedule_annotation_persist(state)

    def _line_trading_desk_delete_selected_rr(self) -> None:
        state = self._line_trading_desk_window
        if state is None or not _widget_exists(state.window):
            return
        sel = state.rr_manage_tree.selection()
        if not sel:
            messagebox.showinfo("盈亏比", "请先在盈亏比列表中选择一行。", parent=state.window)
            return
        rid = sel[0]
        before = len(state.rr_annotations)
        state.rr_annotations[:] = [b for b in state.rr_annotations if b.rr_id != rid]
        if len(state.rr_annotations) == before:
            messagebox.showinfo("盈亏比", "未找到选中计划。", parent=state.window)
            return
        if state.rr_selected_id == rid:
            state.rr_selected_id = state.rr_annotations[0].rr_id if state.rr_annotations else None
        state.rr_pick = None
        state.rr_drag = None
        self._line_trading_desk_dual_log(state, f"盈亏比 | 已删除 id={rid}")
        self._line_trading_desk_refresh_rr_tree(state)
        self._render_line_trading_desk_chart(force=True)
        self._line_trading_desk_schedule_annotation_persist(state)

    def _line_trading_desk_refresh_rr_tree(self, state: LineTradingDeskWindowState) -> None:
        tree = state.rr_manage_tree
        tree.delete(*tree.get_children())
        for box in state.rr_annotations:
            tree.insert(
                "",
                END,
                iid=box.rr_id,
                values=(
                    "锁" if box.locked else "",
                    self._line_trading_desk_format_price(state, box.price_entry),
                    self._line_trading_desk_format_price(state, box.price_stop),
                    self._line_trading_desk_format_price(state, box.price_tp),
                    format_decimal(box.r_multiple),
                ),
            )
        if state.rr_selected_id and tree.exists(state.rr_selected_id):
            tree.selection_set(state.rr_selected_id)

    def _line_trading_desk_on_rr_tree_select(self) -> None:
        state = self._line_trading_desk_window
        if state is None:
            return
        sel = state.rr_manage_tree.selection()
        state.rr_selected_id = sel[0] if sel else None
        self._render_line_trading_desk_chart(force=True)

    def _line_trading_desk_toggle_selected_rr_lock(self) -> None:
        state = self._line_trading_desk_window
        if state is None:
            return
        sel = state.rr_manage_tree.selection()
        if not sel:
            messagebox.showinfo("盈亏比", "请先在列表中选择一行。", parent=state.window)
            return
        rid = sel[0]
        for box in state.rr_annotations:
            if box.rr_id == rid:
                box.locked = not box.locked
                self._line_trading_desk_dual_log(
                    state,
                    f"盈亏比 | 锁定切换 | id={rid} | 现在={'已锁定' if box.locked else '未锁定'}",
                )
                break
        self._line_trading_desk_refresh_rr_tree(state)
        self._render_line_trading_desk_chart(force=True)
        self._line_trading_desk_schedule_annotation_persist(state)

    def _line_trading_desk_rr_selected_box(self, state: LineTradingDeskWindowState) -> DeskRiskRewardAnnotation | None:
        sel = state.rr_manage_tree.selection()
        if not sel:
            messagebox.showinfo("盈亏比", "请先在列表中选择一行。", parent=state.window)
            return None
        rid = sel[0]
        for box in state.rr_annotations:
            if box.rr_id == rid:
                return box
        messagebox.showinfo("盈亏比", "选中的计划不存在。", parent=state.window)
        return None

    def _line_trading_desk_strategy_config(self, symbol: str, profile: str) -> StrategyConfig:
        """组装划线台下单用配置：标的对齐主界面收集逻辑，环境与所选 API profile 一致（避免 simulated 错位）。"""
        normalized = _normalize_symbol_input(symbol)
        old_symbol = self.symbol.get()
        try:
            self.symbol.set(normalized)
            record = self._template_record_from_launcher()
        finally:
            self.symbol.set(old_symbol)
        env_label = self._environment_label_for_profile(profile or self._current_credential_profile())
        desk_env = ENV_OPTIONS[self._normalized_environment_label(env_label)]
        return replace(
            record.config,
            inst_id=normalized,
            trade_inst_id=normalized,
            environment=desk_env,
        )

    def _line_trading_desk_collect_rr_exchange_order(
        self,
        state: LineTradingDeskWindowState,
        box: DeskRiskRewardAnnotation,
        direction: str,
    ) -> tuple[Credentials, StrategyConfig, OrderPlan, Decimal, str] | None:
        symbol = state.symbol_var.get().strip().upper()
        profile = state.api_profile_var.get().strip()
        credentials = self._credentials_for_profile_or_none(profile)
        if credentials is None:
            state.status_text.set("未配置API，无法下单。")
            return None
        instrument = self._line_trading_desk_require_instrument_for_submit(state, symbol)
        if instrument is None:
            return None
        state.desk_instrument_cache[symbol.strip().upper()] = (instrument, time.monotonic())
        self._line_trading_desk_rr_snap_prices(state, box)
        entry = box.price_entry
        sl = box.price_stop
        tp = box.price_tp
        if direction == "long" and not (sl < entry < tp):
            state.status_text.set("多头：须满足 止损 < 入场 < 止盈。")
            return None
        if direction == "short" and not (tp < entry < sl):
            state.status_text.set("空头：须满足 止盈 < 入场 < 止损。")
            return None
        try:
            risk_raw = str(state.param_risk_amount.get()).strip() or "0"
            risk_amount = Decimal(risk_raw)
        except InvalidOperation:
            state.status_text.set("风险金格式无效。")
            return None
        if risk_amount <= 0:
            state.status_text.set("风险金必须大于0。")
            return None
        try:
            config = self._line_trading_desk_strategy_config(symbol, profile)
        except Exception as exc:
            state.status_text.set(f"无法组装下单配置：{exc}")
            return None
        try:
            size = determine_order_size(
                instrument=instrument,
                config=replace(config, risk_amount=risk_amount, order_size=None),
                entry_price=entry,
                stop_loss=sl,
                risk_price_compatible=True,
            )
        except Exception as exc:
            state.status_text.set(f"定量失败：{exc}")
            return None
        tp_for_plan = tp
        if bool(state.param_rr_fee_offset.get()):
            off = _dynamic_two_taker_fee_offset_live(entry, enabled=True)
            if direction == "long":
                tp_for_plan = tp + off
            else:
                tp_for_plan = tp - off
            tick = instrument.tick_size
            if tick is not None and tick > 0:
                tp_for_plan = snap_to_increment(tp_for_plan, tick, "nearest")
            if direction == "long" and not (sl < entry < tp_for_plan):
                tp_for_plan = tp
            elif direction == "short" and not (tp_for_plan < entry < sl):
                tp_for_plan = tp
        side = "buy" if direction == "long" else "sell"
        pos_side = resolve_open_pos_side(config, side)
        atr_period = max(1, int(str(state.param_atr_period.get()).strip() or "10"))
        atr_vals = atr(list(state.last_snapshot.candles), atr_period) if state.last_snapshot else []
        atr_val = atr_vals[-1] if atr_vals and atr_vals[-1] is not None else Decimal("1")
        if atr_val <= 0:
            atr_val = Decimal("1")
        last_ts = state.last_snapshot.candles[-1].ts if state.last_snapshot and state.last_snapshot.candles else 0
        plan = OrderPlan(
            inst_id=symbol,
            side=side,
            pos_side=pos_side,
            size=size,
            take_profit=tp_for_plan,
            stop_loss=sl,
            entry_reference=entry,
            atr_value=atr_val,
            signal="long" if direction == "long" else "short",
            candle_ts=last_ts,
            tp_sl_inst_id=symbol,
            tp_sl_mode="exchange",
        )
        return credentials, config, plan, size, symbol

    def _line_trading_desk_rr_snap_prices(self, state: LineTradingDeskWindowState, box: DeskRiskRewardAnnotation) -> None:
        symbol = state.symbol_var.get().strip().upper()
        inst = self._line_trading_desk_get_cached_instrument(state, symbol)
        if inst is None:
            self._line_trading_desk_prefetch_instrument_async(state, symbol)
            return
        tick = inst.tick_size
        if tick and tick > 0:
            box.price_entry = snap_to_increment(box.price_entry, tick, "nearest")
            box.price_stop = snap_to_increment(box.price_stop, tick, "nearest")
            box.price_tp = snap_to_increment(box.price_tp, tick, "nearest")

    def _line_trading_desk_cancel_canvas_configure_job(self, state: LineTradingDeskWindowState) -> None:
        jid = state.desk_canvas_configure_job
        if jid is not None:
            try:
                self.root.after_cancel(jid)
            except Exception:
                pass
            state.desk_canvas_configure_job = None

    def _on_line_trading_desk_canvas_configure(self, event) -> None:
        state = self._line_trading_desk_window
        if state is None or getattr(event, "widget", None) is not state.canvas:
            return
        try:
            w = max(int(event.width), 1)
            h = max(int(event.height), 1)
        except (TclError, TypeError, ValueError):
            return
        # 布局过程中宽高可能短暂为 1，全量重绘会导致闪白或错位，等尺寸稳定后再画。
        if w < 24 or h < 24:
            return
        last = state.desk_canvas_last_wh
        if last is not None and abs(last[0] - w) < 8 and abs(last[1] - h) < 8:
            return
        state.desk_canvas_last_wh = (w, h)
        self._line_trading_desk_cancel_canvas_configure_job(state)

        def _fire() -> None:
            state.desk_canvas_configure_job = None
            if self._line_trading_desk_window is not state or not _widget_exists(state.canvas):
                return
            self._render_line_trading_desk_chart(force=True)

        state.desk_canvas_configure_job = self.root.after(200, _fire)

    def _line_trading_desk_cancel_motion_chart_paint(self, state: LineTradingDeskWindowState) -> None:
        jid = state.desk_chart_paint_job
        if jid is not None:
            try:
                self.root.after_cancel(jid)
            except Exception:
                pass
            state.desk_chart_paint_job = None

    def _request_line_trading_desk_motion_chart_paint(self, state: LineTradingDeskWindowState) -> None:
        """Pan / 草稿线 / 盈亏拖动时限制全图重绘频率，避免卡顿。"""
        min_gap = 1.0 / 14.0
        now = time.monotonic()
        since = now - float(state.desk_last_chart_paint_t or 0.0)
        if since >= min_gap:
            self._line_trading_desk_cancel_motion_chart_paint(state)
            state.desk_last_chart_paint_t = time.monotonic()
            self._render_line_trading_desk_chart()
            return
        if state.desk_chart_paint_job is not None:
            return
        delay_ms = max(1, int((min_gap - since) * 1000))

        def _fire() -> None:
            state.desk_chart_paint_job = None
            if self._line_trading_desk_window is not state:
                return
            state.desk_last_chart_paint_t = time.monotonic()
            self._render_line_trading_desk_chart()

        state.desk_chart_paint_job = self.root.after(delay_ms, _fire)

    def _line_trading_desk_rr_hit_test(
        self, state: LineTradingDeskWindowState, cx: float, cy: float, *, tol: float = 18.0
    ) -> tuple[DeskRiskRewardAnnotation, str] | None:
        lay = self._line_trading_desk_slice_layout(state)
        if lay is None:
            return None
        tup = self._line_trading_desk_visible_snapshot(state)
        if tup is None:
            return None
        sliced0, vs, _vb = tup
        x_outer = 92.0
        body_half_w = 58.0
        body_y_pad = 12.0
        for box in reversed(state.rr_annotations):
            rel_mid = self._line_trading_desk_rr_layout_rel_mid_clamped(box, vs, sliced0, lay)
            x_mid = layout_bar_index_to_x_center(lay, rel_mid)
            if abs(cx - x_mid) > x_outer:
                continue
            ytp = float(layout_price_to_y_clamped(lay, box.price_tp))
            yey = float(layout_price_to_y_clamped(lay, box.price_entry))
            ysy = float(layout_price_to_y_clamped(lay, box.price_stop))
            for name, yy in (("tp", ytp), ("entry", yey), ("sl", ysy)):
                if abs(cy - yy) < tol:
                    return box, name
            y_top = min(ytp, yey, ysy)
            y_bot = max(ytp, yey, ysy)
            if abs(cx - x_mid) <= body_half_w and (y_top - body_y_pad) <= cy <= (y_bot + body_y_pad):
                return box, "body"
        return None

    def _line_trading_desk_rr_drag_update(self, state: LineTradingDeskWindowState, cy: float) -> None:
        if state.rr_drag is None:
            return
        rid, handle = state.rr_drag
        box = next((b for b in state.rr_annotations if b.rr_id == rid), None)
        if box is None:
            return
        lay = self._line_trading_desk_slice_layout(state)
        if lay is None:
            return
        new_p = layout_y_to_price(lay, cy)
        if box.side == "long":
            if handle in ("body", "entry"):
                delta = new_p - box.price_entry
                box.price_entry += delta
                box.price_stop += delta
                box.price_tp += delta
            elif handle == "sl":
                stop_p = min(new_p, box.price_entry - Decimal("0.0001"))
                if stop_p >= box.price_entry:
                    return
                box.price_stop = stop_p
                risk = box.price_entry - box.price_stop
                if risk > 0:
                    box.price_tp = box.price_entry + box.r_multiple * risk
            elif handle == "tp":
                if new_p <= box.price_entry:
                    return
                box.price_tp = new_p
                risk = box.price_entry - box.price_stop
                if risk > 0:
                    box.r_multiple = (box.price_tp - box.price_entry) / risk
        else:
            if handle in ("body", "entry"):
                delta = new_p - box.price_entry
                box.price_entry += delta
                box.price_stop += delta
                box.price_tp += delta
            elif handle == "sl":
                stop_p = max(new_p, box.price_entry + Decimal("0.0001"))
                if stop_p <= box.price_entry:
                    return
                box.price_stop = stop_p
                risk = box.price_stop - box.price_entry
                if risk > 0:
                    box.price_tp = box.price_entry - box.r_multiple * risk
            elif handle == "tp":
                if new_p >= box.price_entry:
                    return
                box.price_tp = new_p
                risk = box.price_stop - box.price_entry
                if risk > 0:
                    box.r_multiple = (box.price_entry - box.price_tp) / risk

    def _line_trading_desk_submit_rr_bracket_order(
        self,
        state: LineTradingDeskWindowState,
        box: DeskRiskRewardAnnotation,
        direction: str,
    ) -> None:
        if box.side != direction:
            state.status_text.set(f"当前框为「{'多' if box.side == 'long' else '空'}头」计划，请用对应按钮。")
            return
        state.status_text.set("正在准备附带 TP/SL 的限价单…")
        ctx = self._line_trading_desk_collect_rr_exchange_order(state, box, direction)
        if ctx is None:
            return
        credentials, config, plan, size, symbol = ctx
        entry = box.price_entry
        sl = box.price_stop
        tp = plan.take_profit
        self._line_trading_desk_spawn_rr_limit_network(
            state,
            credentials,
            config,
            plan,
            size,
            symbol,
            entry,
            sl,
            tp,
            direction,
            desk_ref=state,
            busy_message="正在提交限价+TP/SL…",
            error_prefix="条件限价单失败",
            log_title="盈亏比限价+附带TP/SL",
            id_label="ordId",
            use_trigger_algo=False,
            success_status_for_rid=lambda r: f"已提交限价+TP/SL | {symbol} | sz={format_decimal(size)} | ordId={r}",
        )

    def _line_trading_desk_submit_rr_limit_order_from_tree(self) -> None:
        state = self._line_trading_desk_window
        if state is None:
            return
        if state.desk_submit_inflight:
            state.status_text.set("已有委托正在提交，请稍候…")
            return
        box = self._line_trading_desk_rr_selected_box(state)
        if box is None:
            return
        state.status_text.set("正在准备限价委托…")
        ctx = self._line_trading_desk_collect_rr_exchange_order(state, box, box.side)
        if ctx is None:
            return
        credentials, config, plan, size, symbol = ctx
        entry = box.price_entry
        sl = box.price_stop
        tp = plan.take_profit
        direction = box.side
        qty_line = self._line_trading_desk_confirm_qty_line(state, symbol, size)
        if not messagebox.askyesno(
            "确认限价委托",
            (
                f"方向：{direction.upper()}\n"
                f"标的：{symbol}\n"
                f"入场：{self._line_trading_desk_format_price(state, entry)}\n"
                f"止损：{self._line_trading_desk_format_price(state, sl)}\n"
                f"止盈：{self._line_trading_desk_format_price(state, tp)}\n"
                f"数量：{qty_line}\n\n"
                "确认提交「限价委托单」吗？"
            ),
            parent=state.window,
        ):
            state.status_text.set("已取消限价委托提交。")
            return
        self._line_trading_desk_spawn_rr_limit_network(
            state,
            credentials,
            config,
            plan,
            size,
            symbol,
            entry,
            sl,
            tp,
            direction,
            desk_ref=state,
            busy_message="正在提交限价委托…",
            error_prefix="限价委托失败",
            log_title="限价委托单",
            id_label="ordId",
            use_trigger_algo=False,
            success_status_for_rid=lambda r: f"已提交限价委托+TP/SL | {symbol} | sz={format_decimal(size)} | ordId={r}",
        )

    def _line_trading_desk_submit_rr_trigger_order_from_tree(self) -> None:
        state = self._line_trading_desk_window
        if state is None:
            return
        if state.desk_submit_inflight:
            state.status_text.set("已有委托正在提交，请稍候…")
            return
        box = self._line_trading_desk_rr_selected_box(state)
        if box is None:
            return
        state.status_text.set("正在准备触发价委托…")
        ctx = self._line_trading_desk_collect_rr_exchange_order(state, box, box.side)
        if ctx is None:
            return
        credentials, config, plan, size, symbol = ctx
        entry = box.price_entry
        sl = box.price_stop
        tp = plan.take_profit
        direction = box.side
        qty_line = self._line_trading_desk_confirm_qty_line(state, symbol, size)
        if not messagebox.askyesno(
            "确认触发价委托",
            (
                f"方向：{direction.upper()}\n"
                f"标的：{symbol}\n"
                f"入场：{self._line_trading_desk_format_price(state, entry)}\n"
                f"止损：{self._line_trading_desk_format_price(state, sl)}\n"
                f"止盈：{self._line_trading_desk_format_price(state, tp)}\n"
                f"数量：{qty_line}\n\n"
                "确认提交「触发价委托」吗？"
            ),
            parent=state.window,
        ):
            state.status_text.set("已取消触发价委托提交。")
            return
        self._line_trading_desk_spawn_rr_limit_network(
            state,
            credentials,
            config,
            plan,
            size,
            symbol,
            entry,
            sl,
            tp,
            direction,
            desk_ref=state,
            busy_message="正在提交触发价委托…",
            error_prefix="触发价委托失败",
            log_title="触发价委托",
            id_label="algoId",
            use_trigger_algo=True,
            success_status_for_rid=lambda r: f"已提交触发限价+TP/SL（算法单）| {symbol} | sz={format_decimal(size)} | algoId={r}",
        )

    def _line_trading_desk_draw_ray_on_canvas(
        self,
        state: LineTradingDeskWindowState,
        ann: LiveChartLineAnnotation,
        layout: StrategyLiveChartLayout,
        view_start: int,
    ) -> None:
        if ann.bar_a is None or ann.price_a is None or ann.bar_b is None or ann.price_b is None:
            canvas = state.canvas
            canvas.create_line(ann.x1, ann.y1, ann.x2, ann.y2, fill=ann.color, width=2, tags=("desk_line",))
            if ann.label:
                canvas.create_text(
                    ann.x2 + 4,
                    ann.y2 - 4,
                    anchor="sw",
                    text=ann.label,
                    fill=ann.color,
                    font=("Microsoft YaHei UI", 9),
                    tags=("desk_line",),
                )
            return
        bar_a = ann.bar_a
        bar_b = ann.bar_b
        rel_a = bar_a - float(view_start)
        rel_b = bar_b - float(view_start)
        x_a = layout_bar_index_to_x_center(layout, rel_a)
        x_b = layout_bar_index_to_x_center(layout, rel_b)
        # 趋势线用未钳价→像素，避免两端价都在界外时被钳到同一条边 dy=0 变成「折线+水平虚线」。
        if ann.kind == "line":
            y_a = float(layout_price_to_y_unclamped(layout, ann.price_a))
            y_b = float(layout_price_to_y_unclamped(layout, ann.price_b))
        else:
            y_a = float(layout_price_to_y_clamped(layout, ann.price_a))
            y_b = float(layout_price_to_y_clamped(layout, ann.price_b))
        dx = x_b - x_a
        dy = y_b - y_a
        if abs(dx) < 1e-6 and abs(dy) < 1e-6:
            return
        big = float(max(layout.right - layout.left, layout.bottom - layout.top, 1.0)) * 4.0
        if dx >= 0:
            x_far = layout.right + big
        else:
            x_far = layout.left - big
        t_top = float(layout.top) + 0.5
        t_bot = float(layout.bottom) - 0.5
        if abs(dx) < 1e-9:
            x2 = x_a
            stretch = (y_b - y_a) * big
            if ann.kind == "line":
                y2 = float(y_b + stretch)
            else:
                y2 = float(min(max(y_b + stretch, t_top), t_bot))
        else:
            t = (x_far - x_b) / dx
            x2 = x_far
            if ann.kind == "line":
                y2 = float(y_b + t * dy)
            else:
                y2 = float(min(max(y_b + t * dy, t_top), t_bot))
        canvas = state.canvas
        canvas.create_line(x_a, y_a, x_b, y_b, fill=ann.color, width=2, tags=("desk_line",))
        canvas.create_line(x_b, y_b, x2, y2, fill=ann.color, width=2, dash=(6, 4), tags=("desk_line",))
        canvas.create_text(
            min(x_b + 6, layout.right - 4),
            y_b - 6,
            anchor="sw",
            text=ann.label,
            fill=ann.color,
            font=("Microsoft YaHei UI", 9),
            tags=("desk_line",),
        )

    def _line_trading_desk_slice_last_real_rel_hi(self, sliced: StrategyLiveChartSnapshot) -> float:
        """当前可见切片里最后一根「真实」K 的相对下标上限（不含右侧平盘占位 ghost）。"""
        n = len(sliced.candles)
        if n <= 0:
            return 0.0
        pe = sliced.series_plot_end_index
        if pe is None:
            return float(n - 1)
        cap = max(0, min(int(pe), n))
        return float(max(0, cap - 1))

    def _line_trading_desk_rr_layout_rel_mid_clamped(
        self,
        box: DeskRiskRewardAnnotation,
        view_start: int,
        sliced: StrategyLiveChartSnapshot,
        layout: StrategyLiveChartLayout,
    ) -> float:
        rel_e = box.bar_entry - float(view_start)
        rel_s = box.bar_stop - float(view_start)
        rel_mid = (rel_e + rel_s) / 2.0
        n_vis = max(1, int(layout.candle_count))
        # 不画在右侧 ghost 空白里；索引越界时钳到真实 K 区间。
        rel_hi = min(float(n_vis - 1), self._line_trading_desk_slice_last_real_rel_hi(sliced))
        return min(max(rel_mid, 0.0), rel_hi)

    def _line_trading_desk_draw_rr_zones(
        self,
        state: LineTradingDeskWindowState,
        layout: StrategyLiveChartLayout,
        view_start: int,
        sliced: StrategyLiveChartSnapshot,
    ) -> None:
        canvas = state.canvas
        for box in state.rr_annotations:
            rel_mid = self._line_trading_desk_rr_layout_rel_mid_clamped(box, view_start, sliced, layout)
            x_mid = layout_bar_index_to_x_center(layout, rel_mid)
            entry_y = layout_price_to_y_clamped(layout, box.price_entry)
            stop_y = layout_price_to_y_clamped(layout, box.price_stop)
            profit_y = layout_price_to_y_clamped(layout, box.price_tp)
            risk = abs(box.price_entry - box.price_stop)
            if risk <= 0:
                continue
            sel = state.rr_selected_id == box.rr_id
            ow = 3 if sel else 1
            if box.side == "long":
                top_y = min(entry_y, profit_y)
                bot_y = max(stop_y, entry_y)
                canvas.create_rectangle(
                    x_mid - 36,
                    top_y,
                    x_mid + 36,
                    entry_y,
                    outline="#137333",
                    width=ow,
                    tags=("desk_rr",),
                )
                canvas.create_rectangle(
                    x_mid - 36,
                    entry_y,
                    x_mid + 36,
                    bot_y,
                    outline="#cf222e",
                    width=ow,
                    tags=("desk_rr",),
                )
            else:
                top_y = min(stop_y, entry_y)
                bot_y = max(entry_y, profit_y)
                canvas.create_rectangle(
                    x_mid - 36,
                    top_y,
                    x_mid + 36,
                    entry_y,
                    outline="#cf222e",
                    width=ow,
                    tags=("desk_rr",),
                )
                canvas.create_rectangle(
                    x_mid - 36,
                    entry_y,
                    x_mid + 36,
                    bot_y,
                    outline="#137333",
                    width=ow,
                    tags=("desk_rr",),
                )
            for y, dash in ((entry_y, (4, 2)), (stop_y, (2, 2)), (profit_y, (4, 4))):
                canvas.create_line(
                    x_mid - 40, y, x_mid + 40, y, fill="#374151", width=1, dash=dash, tags=("desk_rr",)
                )
            label_x = x_mid + 44
            line_labels = (
                (entry_y, box.price_entry, "开仓", "#1864ab"),
                (stop_y, box.price_stop, "止损", "#c92a2a"),
                (profit_y, box.price_tp, "止盈", "#2b8a3e"),
            )
            for ly, price, title, color in line_labels:
                canvas.create_text(
                    label_x,
                    ly,
                    anchor="w",
                    text=f"{title} {self._line_trading_desk_format_price(state, price)}",
                    fill=color,
                    font=("Microsoft YaHei UI", 8),
                    tags=("desk_rr",),
                )
            rr_txt = f"RR×{format_decimal(box.r_multiple)}{' 锁' if box.locked else ''}"
            canvas.create_text(
                x_mid, min(entry_y, stop_y, profit_y) - 12, text=rr_txt, fill="#0b7285", tags=("desk_rr",)
            )

    def _line_trading_desk_eval_ray_triggers(self, state: LineTradingDeskWindowState) -> bool:
        """若射线穿越状态或触发标记有变化则返回 True，用于持久化防抖。"""
        snap = state.last_snapshot
        if not snap or len(snap.candles) < 2:
            return False
        dirty = False
        mode = (state.param_cross_mode.get() or "tick_last").strip()
        if mode == "close":
            px = snap.candles[-1].close
        else:
            px = snap.latest_price or snap.candles[-1].close
        bar_now = float(len(snap.candles) - 1)
        for ann in state.line_annotations:
            if ann.kind not in ("line", "horizontal") or getattr(ann, "locked", False):
                continue
            if ann.desk_ray_triggered or ann.desk_ray_submit_pending:
                continue
            if ann.bar_a is None or ann.price_a is None or ann.bar_b is None or ann.price_b is None:
                continue
            line_p = line_price_through_anchors(ann.bar_a, ann.price_a, ann.bar_b, ann.price_b, bar_now)
            diff = px - line_p
            side = 0 if diff == 0 else (1 if diff > 0 else -1)
            if ann.desk_ray_last_side is None:
                ann.desk_ray_last_side = side
                dirty = True
                continue
            if side == 0:
                continue
            if side == ann.desk_ray_last_side:
                continue
            self._line_trading_desk_fire_ray_trigger(state, ann, px, line_p)
            dirty = True
        return dirty

    def _line_trading_desk_fire_ray_trigger(
        self,
        state: LineTradingDeskWindowState,
        ann: LiveChartLineAnnotation,
        px: Decimal,
        line_p: Decimal,
    ) -> None:
        sym = state.symbol_var.get().strip().upper()
        action = _line_trading_desk_normalize_ray_action(ann.desk_ray_action or "notify")
        action_zh = _LINE_DESK_RAY_ACTION_LABEL_ZH.get(action, action)
        self._line_trading_desk_dual_log(
            state,
            f"射线触发 | {sym} | {ann.label} | 价={self._line_trading_desk_format_price(state, px)} | "
            f"线≈{self._line_trading_desk_format_price(state, line_p)} | 动作={action_zh}",
        )
        if action not in ("long", "short"):
            ann.desk_ray_triggered = True
            return
        snap = state.last_snapshot
        if snap is None or not snap.candles:
            msg = "射线自动开仓跳过：无K线快照。"
            state.status_text.set(msg)
            self._line_trading_desk_dual_log(state, msg)
            return
        stop_p = self._line_trading_desk_atr_stop_from_params(state, action, candles=list(snap.candles))
        if stop_p is None:
            msg = "射线自动开仓：ATR/上一根不可用，已跳过。"
            state.status_text.set(msg)
            self._line_trading_desk_dual_log(state, msg)
            return
        if action == "long" and stop_p >= px:
            fs, fp = self._line_trading_desk_format_price(state, stop_p), self._line_trading_desk_format_price(state, px)
            msg = (
                f"射线开多已跳过：多头要求 ATR 止损严格低于参考价（最新价/收盘等）。"
                f"当前 ATR 止损={fs}，参考价={fp}（须 止损 < 参考价），不满足故未报单。"
            )
            state.status_text.set(msg)
            self._line_trading_desk_dual_log(state, msg)
            return
        if action == "short" and stop_p <= px:
            fs, fp = self._line_trading_desk_format_price(state, stop_p), self._line_trading_desk_format_price(state, px)
            msg = (
                f"射线开空已跳过：空头要求 ATR 止损严格高于参考价。"
                f"当前 ATR 止损={fs}，参考价={fp}（须 止损 > 参考价），不满足故未报单。"
            )
            state.status_text.set(msg)
            self._line_trading_desk_dual_log(state, msg)
            return
        tp_r = self._line_trading_desk_parse_tp_r(state)
        if not self._submit_line_trading_desk_order(
            action,
            entry_price_override=px,
            stop_price_override=stop_p,
            tp_r_multiple=tp_r,
            ray_annotation=ann,
        ):
            self._line_trading_desk_dual_log(state, "射线自动开仓未送出委托，请查看状态栏具体原因。")

    def _line_trading_desk_cancel_layout_retry_job(self, state: LineTradingDeskWindowState) -> None:
        jid = state.desk_layout_retry_job
        if jid is not None:
            try:
                self.root.after_cancel(jid)
            except Exception:
                pass
            state.desk_layout_retry_job = None

    def _line_trading_desk_cancel_desk_map_render_job(self, state: LineTradingDeskWindowState) -> None:
        jid = state.desk_map_render_job
        if jid is not None:
            try:
                self.root.after_cancel(jid)
            except Exception:
                pass
            state.desk_map_render_job = None

    def _line_trading_desk_invalidate_bootstrap_repaints(self, state: LineTradingDeskWindowState) -> None:
        state.desk_layout_bootstrap_gen += 1
        self._line_trading_desk_cancel_layout_retry_job(state)
        self._line_trading_desk_cancel_desk_map_render_job(state)

    def _line_trading_desk_start_bootstrap_repaints(self, state: LineTradingDeskWindowState) -> None:
        """画布尚未映射出有效 winfo 时 render 会跳过；用短间隔多帧重试直到尺寸稳定。"""
        self._line_trading_desk_cancel_layout_retry_job(state)
        state.desk_layout_bootstrap_gen += 1
        gen = state.desk_layout_bootstrap_gen
        self._line_trading_desk_run_bootstrap_repaints(state, 0, gen)

    def _line_trading_desk_run_bootstrap_repaints(self, state: LineTradingDeskWindowState, step: int, gen: int) -> None:
        if self._line_trading_desk_window is not state or not _widget_exists(state.canvas):
            state.desk_layout_retry_job = None
            return
        if state.desk_layout_bootstrap_gen != gen:
            return
        self._render_line_trading_desk_chart(force=True)
        _w, _h, ok_now = strategy_live_chart_canvas_layout(state.canvas)
        if ok_now:
            state.desk_layout_retry_job = None
            return
        if step >= 5:
            state.desk_layout_retry_job = None
            return
        delays_ms = (40, 80, 130, 210, 340)

        def _next() -> None:
            if self._line_trading_desk_window is not state or state.desk_layout_bootstrap_gen != gen:
                return
            self._line_trading_desk_run_bootstrap_repaints(state, step + 1, gen)

        state.desk_layout_retry_job = self.root.after(delays_ms[step], _next)

    def _on_line_trading_desk_canvas_map_event(self, event) -> None:
        state = self._line_trading_desk_window
        if state is None or getattr(event, "widget", None) is not state.canvas:
            return
        self._line_trading_desk_cancel_desk_map_render_job(state)
        desk_ref = state

        def _deferred() -> None:
            st = self._line_trading_desk_window
            if st is not desk_ref or not _widget_exists(desk_ref.canvas):
                return
            st.desk_map_render_job = None
            self._line_trading_desk_start_bootstrap_repaints(st)

        state.desk_map_render_job = self.root.after(24, _deferred)

    def _close_line_trading_desk_window(self) -> None:
        state = self._line_trading_desk_window
        if state is None:
            return
        self._line_trading_desk_invalidate_bootstrap_repaints(state)
        self._line_trading_desk_cancel_motion_chart_paint(state)
        self._line_trading_desk_cancel_chart_throttle(state)
        self._line_trading_desk_cancel_canvas_configure_job(state)
        if state.refresh_job is not None:
            try:
                self.root.after_cancel(state.refresh_job)
            except Exception:
                pass
            state.refresh_job = None
        if _widget_exists(state.window):
            state.window.destroy()
        self._line_trading_desk_window = None

    def _request_line_trading_desk_refresh(self, *, immediate: bool = False) -> None:
        state = self._line_trading_desk_window
        if state is None or not _widget_exists(state.window):
            return
        if state.refresh_job is not None:
            try:
                self.root.after_cancel(state.refresh_job)
            except Exception:
                pass
            state.refresh_job = None
        delay = 10 if immediate else LINE_TRADING_DESK_POLL_MS
        state.refresh_job = self.root.after(delay, self._run_line_trading_desk_refresh)

    def _run_line_trading_desk_refresh(self) -> None:
        state = self._line_trading_desk_window
        if state is None or not _widget_exists(state.window):
            return
        state.refresh_job = None
        symbol = state.symbol_var.get().strip().upper()
        bar = state.bar_var.get().strip()
        if not symbol or not bar:
            state.status_text.set("请先选择标的和周期。")
            return
        profile = state.api_profile_var.get().strip()
        credentials = self._credentials_for_profile_or_none(profile)
        if credentials is None:
            state.status_text.set("未配置所选 API 凭证。")
            return
        env_label = self._environment_label_for_profile(profile or self._current_credential_profile())
        environment = ENV_OPTIONS[self._normalized_environment_label(env_label)]
        self._line_trading_desk_prefetch_instrument_async(state, symbol)
        state.status_text.set(f"正在刷新：{symbol} / {bar} | API={profile or '-'}（保留当前画面）")
        state.desk_refresh_generation += 1
        gen = state.desk_refresh_generation
        threading.Thread(
            target=self._line_trading_desk_refresh_worker,
            args=(symbol, bar, credentials, environment, profile, gen),
            daemon=True,
        ).start()

    def _line_trading_desk_refresh_worker(
        self,
        symbol: str,
        bar: str,
        credentials: Credentials,
        environment: str,
        profile_name: str,
        generation: int,
    ) -> None:
        try:
            # 单次拉满目标根数，避免「先 280 再 500」两帧切换导致画线锚点与纵轴短暂错位、闪动。
            candles = self.client.get_candles_history(symbol, bar, limit=LINE_TRADING_DESK_CANDLE_TARGET)
            latest_price = candles[-1].close if candles else None
            try:
                ticker = self.client.get_ticker(symbol)
                if ticker.last is not None:
                    latest_price = ticker.last
            except Exception:
                pass
            snapshot = build_strategy_live_chart_snapshot(
                session_id=f"desk:{symbol}",
                candles=candles,
                ema_period=21,
                trend_ema_period=55,
                reference_ema_period=21,
                latest_price=latest_price,
                note=f"划线交易台 | {symbol} | {bar} | API={profile_name or '-'} | K={len(candles)}",
            )
        except Exception as exc:
            self.root.after(
                0,
                lambda msg=str(exc), g=generation: self._apply_line_trading_desk_error(msg, generation=g),
            )
            return

        self.root.after(
            0,
            lambda s=snapshot, g=generation: self._apply_line_trading_desk_chart_only(s, g, loading_account=True),
        )

        account_error: str | None = None
        positions: list[OkxPosition] = []
        pending_orders: list[OkxTradeOrderItem] = []
        order_history: list[OkxTradeOrderItem] = []
        try:
            with ThreadPoolExecutor(max_workers=3) as pool:
                fut_pos = pool.submit(self.client.get_positions, credentials, environment=environment)
                fut_pend = pool.submit(
                    self.client.get_pending_orders, credentials, environment=environment, limit=100
                )
                fut_hist = pool.submit(self.client.get_order_history, credentials, environment=environment, limit=100)
                positions = fut_pos.result()
                pending_orders = fut_pend.result()
                order_history = fut_hist.result()
        except Exception as exc:
            account_error = str(exc).strip() or exc.__class__.__name__

        self.root.after(
            0,
            lambda p=positions, po=pending_orders, oh=order_history, g=generation, err=account_error: self._apply_line_trading_desk_account_only(
                p, po, oh, g, account_error=err
            ),
        )

    def _apply_line_trading_desk_error(self, message: str, *, generation: int | None = None) -> None:
        state = self._line_trading_desk_window
        if state is None:
            return
        if generation is not None and state.desk_refresh_generation != generation:
            return
        state.status_text.set(f"刷新失败：{message}")
        self._request_line_trading_desk_refresh(immediate=False)

    def _apply_line_trading_desk_chart_only(
        self,
        snapshot: StrategyLiveChartSnapshot,
        generation: int,
        *,
        loading_account: bool = False,
    ) -> None:
        state = self._line_trading_desk_window
        if state is None or not _widget_exists(state.window):
            return
        if state.desk_refresh_generation != generation:
            return
        chart_snapshot = snapshot
        chart_note = ""
        if not snapshot.candles and state.last_snapshot is not None and state.last_snapshot.candles:
            chart_snapshot = state.last_snapshot
            chart_note = " | 本次K线返回空数据，已保留上一画面"
        prev_snap = state.last_snapshot
        if (
            prev_snap is not None
            and prev_snap.candles
            and chart_snapshot.candles
            and self._line_trading_desk_should_remap_annotations_for_candle_change(
                state, prev_snap.candles, chart_snapshot.candles
            )
        ):
            self._line_trading_desk_remap_annotations_bars_after_candle_reload(
                state, prev_snap.candles, chart_snapshot.candles
            )
            self._line_trading_desk_schedule_annotation_persist(state)
        state.last_snapshot = chart_snapshot
        self._line_trading_desk_update_price_tick(state)
        n = len(chart_snapshot.candles)
        if n > 0 and not state.desk_initial_scroll_done:
            state.desk_view_start = max(0, n - min(state.desk_visible_bars, n))
            state.desk_initial_scroll_done = True
        self._line_trading_desk_clamp_view(state)
        if self._line_trading_desk_eval_ray_triggers(state):
            self._line_trading_desk_schedule_annotation_persist(state)
        self._render_line_trading_desk_chart(force=True)
        self._line_trading_desk_refresh_ray_tree(state)
        self._line_trading_desk_refresh_rr_tree(state)
        sym = state.symbol_var.get().strip().upper()
        if loading_account:
            state.status_text.set(
                f"已加载K线 {len(chart_snapshot.candles)} 根 | {sym}{chart_note} | 正在同步持仓/委托…"
            )
        # 后台补足 K 线根数时不再改写状态栏，避免覆盖「已刷新」摘要。

    def _apply_line_trading_desk_account_only(
        self,
        positions: list[OkxPosition],
        pending_orders: list[OkxTradeOrderItem],
        order_history: list[OkxTradeOrderItem],
        generation: int,
        *,
        account_error: str | None = None,
    ) -> None:
        state = self._line_trading_desk_window
        if state is None or not _widget_exists(state.window):
            return
        if state.desk_refresh_generation != generation:
            return
        sym = state.symbol_var.get().strip().upper()
        state.latest_positions = [item for item in positions if item.inst_id == sym]
        for item in state.latest_positions:
            self._line_trading_desk_prefetch_instrument_async(state, item.inst_id)
        state.latest_pending_orders = [o for o in pending_orders if o.inst_id.strip().upper() == sym]
        state.latest_order_history = [o for o in order_history if o.inst_id.strip().upper() == sym][:80]
        tree = state.position_tree
        selected = tree.selection()[0] if tree.selection() else None
        rows: list[tuple[str, tuple[object, ...]]] = []
        for index, item in enumerate(state.latest_positions):
            iid = self._line_trading_desk_position_row_iid(item, index)
            rows.append(
                (
                    iid,
                    (
                        item.inst_id,
                        item.pos_side or "-",
                        self._line_trading_desk_position_qty_cell(state, item),
                        self._line_trading_desk_format_price(state, item.avg_price),
                        self._line_trading_desk_format_price(state, item.mark_price),
                        _format_optional_decimal(item.unrealized_pnl),
                    ),
                )
            )
        self._line_trading_desk_sync_tree_rows(tree, rows, preserve_selected_iid=selected)
        self._line_trading_desk_refresh_pending_orders_tree(state)
        self._line_trading_desk_refresh_order_history_tree(state)
        chart_n = len(state.last_snapshot.candles) if state.last_snapshot and state.last_snapshot.candles else 0
        err_seg = f" | 持仓/委托：{account_error}" if account_error else ""
        state.status_text.set(
            f"已刷新 {sym} | K线 {chart_n} 根 | 持仓 {len(state.latest_positions)} 条 | "
            f"当前委托 {len(state.latest_pending_orders)} 条 | 历史委托 {len(state.latest_order_history)} 条（标的过滤）{err_seg}"
        )
        self._request_line_trading_desk_refresh(immediate=False)

    def _line_trading_desk_cancel_chart_throttle(self, state: LineTradingDeskWindowState) -> None:
        jid = state.desk_chart_throttle_job
        if jid is not None:
            try:
                self.root.after_cancel(jid)
            except Exception:
                pass
            state.desk_chart_throttle_job = None

    def _render_line_trading_desk_chart(self, *, force: bool = False) -> None:
        state = self._line_trading_desk_window
        if state is None or not _widget_exists(state.canvas):
            return
        self._line_trading_desk_cancel_motion_chart_paint(state)
        if force:
            self._line_trading_desk_cancel_chart_throttle(state)
            self._render_line_trading_desk_chart_impl(state)
            state.desk_last_chart_render_t = time.monotonic()
            return
        now = time.monotonic()
        last = float(state.desk_last_chart_render_t or 0.0)
        elapsed = now - last
        if elapsed < _LINE_DESK_CHART_MIN_INTERVAL_S:
            self._line_trading_desk_cancel_chart_throttle(state)
            delay_ms = max(1, int((_LINE_DESK_CHART_MIN_INTERVAL_S - elapsed) * 1000))
            desk_ref = state

            def _delayed() -> None:
                st = self._line_trading_desk_window
                if st is not desk_ref or not _widget_exists(st.canvas):
                    return
                st.desk_chart_throttle_job = None
                self._render_line_trading_desk_chart(force=True)

            state.desk_chart_throttle_job = self.root.after(delay_ms, _delayed)
            return
        self._line_trading_desk_cancel_chart_throttle(state)
        state.desk_last_chart_render_t = time.monotonic()
        self._render_line_trading_desk_chart_impl(state)

    def _render_line_trading_desk_chart_impl(self, state: LineTradingDeskWindowState) -> None:
        try:
            snapshot = state.last_snapshot
            if snapshot is None:
                snapshot = StrategyLiveChartSnapshot(
                    session_id="desk:empty",
                    candles=(),
                    note="等待加载K线...",
                )
                render_strategy_live_chart(state.canvas, snapshot)
                return
            if not snapshot.candles:
                render_strategy_live_chart(state.canvas, snapshot)
                return
            tup = self._line_trading_desk_visible_snapshot(state)
            if tup is None:
                render_strategy_live_chart(state.canvas, snapshot)
                return
            sliced, vs, _vb = tup
            to_draw = replace(sliced, price_display_tick=state.desk_price_tick)
            anchors = self._line_trading_desk_desk_anchor_prices(state, sliced=sliced)
            if not render_strategy_live_chart(
                state.canvas, to_draw, bounds_policy="desk", desk_anchor_prices=anchors
            ):
                if to_draw.candles:
                    self._line_trading_desk_start_bootstrap_repaints(state)
                return
            layout = compute_strategy_live_chart_layout(
                state.canvas, to_draw, bounds_policy="desk", desk_anchor_prices=anchors
            )
            if layout is not None:
                for ann in state.line_annotations:
                    if ann.kind in ("line", "horizontal"):
                        self._line_trading_desk_draw_ray_on_canvas(state, ann, layout, vs)
                    elif ann.kind == "stop":
                        self._line_trading_desk_draw_stop_line(state, ann, layout, vs)
                self._line_trading_desk_draw_rr_zones(state, layout, vs, to_draw)
            self._draw_line_annotations(state.canvas, [], state.draft_line_start, state.draft_line_current)
        except Exception as exc:
            # 防止绘图链路偶发异常导致画布被清空后“整页消失”。
            fallback = StrategyLiveChartSnapshot(
                session_id="desk:render-error",
                candles=(),
                note=f"绘图异常：{exc}",
            )
            try:
                render_strategy_live_chart(state.canvas, fallback)
            except Exception:
                pass
            state.status_text.set(f"图表重绘异常：{exc}（可继续操作，系统会自动重试）")

    def _line_trading_desk_draw_stop_line(
        self,
        state: LineTradingDeskWindowState,
        ann: LiveChartLineAnnotation,
        layout: StrategyLiveChartLayout,
        view_start: int,
    ) -> None:
        canvas = state.canvas
        if ann.price_a is not None:
            y = layout_price_to_y_clamped(layout, ann.price_a)
            canvas.create_line(layout.left, y, layout.right, y, fill=ann.color, width=2, dash=(5, 3), tags=("desk_line",))
            canvas.create_text(
                layout.right - 6,
                y - 6,
                anchor="e",
                text=ann.label or "止损",
                fill=ann.color,
                font=("Microsoft YaHei UI", 9),
                tags=("desk_line",),
            )
            return
        canvas.create_line(ann.x1, ann.y1, ann.x2, ann.y2, fill=ann.color, width=2, tags=("desk_line",))

    def _set_line_trading_desk_tool(self, tool: str) -> None:
        state = self._line_trading_desk_window
        if state is None:
            return
        state.active_tool = tool
        state.rr_pick = None
        labels = {
            "line": "趋势线(射线)",
            "horizontal": "水平射线",
            "stop": "止损线",
            "zoom_range": "区间放大（横拖选定K线，远端隐藏）",
            "none": "无（可选中拖动盈亏比框）",
        }
        base = f"当前画线工具：{labels.get(tool, tool)}"
        if tool == "line":
            base += " | 按住拖拽后松手画一条，画完自动退出；再画请重新点本按钮。"
        elif tool == "horizontal":
            base += " | 点按或横拖后松手画一条水平射线，画完自动退出；再画请重新点本按钮。"
        state.status_text.set(base)
        self._render_line_trading_desk_chart(force=True)

    def _set_line_trading_desk_rr_draw(self, side: str) -> None:
        state = self._line_trading_desk_window
        if state is None:
            return
        side = side.strip().lower()
        if side not in ("long", "short"):
            side = "long"
        state.param_rr_side.set(side)
        state.active_tool = "rr"
        state.rr_pick = None
        state.draft_line_start = None
        state.draft_line_current = None
        lab = "多头" if side == "long" else "空头"
        state.status_text.set(
            f"{lab}盈亏比：拖一次定两点画好一张框（画完自动退出工具）；无工具时点框可选中，再按住拖动。"
        )
        self._render_line_trading_desk_chart(force=True)

    def _clear_line_trading_desk_lines(self) -> None:
        state = self._line_trading_desk_window
        if state is None:
            return
        self._line_trading_desk_cancel_motion_chart_paint(state)
        locked_rays = [
            a
            for a in state.line_annotations
            if a.kind in ("line", "horizontal") and getattr(a, "locked", False)
        ]
        state.line_annotations.clear()
        state.line_annotations.extend(locked_rays)
        locked_rr = [b for b in state.rr_annotations if b.locked]
        state.rr_annotations.clear()
        state.rr_annotations.extend(locked_rr)
        state.draft_line_start = None
        state.draft_line_current = None
        state.desk_pan_origin = None
        state.rr_drag = None
        state.rr_pick = None
        if state.rr_selected_id and not any(b.rr_id == state.rr_selected_id for b in state.rr_annotations):
            state.rr_selected_id = state.rr_annotations[0].rr_id if state.rr_annotations else None
        self._line_trading_desk_refresh_ray_tree(state)
        self._line_trading_desk_refresh_rr_tree(state)
        self._render_line_trading_desk_chart(force=True)
        self._line_trading_desk_schedule_annotation_persist(state)
        if locked_rays or locked_rr:
            parts = []
            if locked_rays:
                parts.append(f"{len(locked_rays)} 条锁定射线")
            if locked_rr:
                parts.append(f"{len(locked_rr)} 个锁定盈亏比")
            state.status_text.set(f"已清空未锁定画线；保留 {'、'.join(parts)}。")
            self._line_trading_desk_dual_log(
                state,
                f"清空线 | 未锁定项已清除 | 保留锁定射线={len(locked_rays)} | 保留锁定盈亏比={len(locked_rr)}",
            )
        else:
            state.status_text.set("已清空画线。")
            self._line_trading_desk_dual_log(state, "清空线 | 已全部清除")

    def _on_line_trading_desk_mouse_move(self, event) -> None:
        state = self._line_trading_desk_window
        if state is None:
            return
        if state.desk_pan_origin is not None:
            lay = self._line_trading_desk_slice_layout(state)
            if lay is None:
                return
            x0, v0 = state.desk_pan_origin
            delta_x = float(event.x) - x0
            step = max(lay.candle_step, 1e-9)
            dv = int(round(-delta_x / step))
            state.desk_view_start = max(0, v0 + dv)
            self._line_trading_desk_clamp_view(state)
            self._request_line_trading_desk_motion_chart_paint(state)
            return
        if state.rr_pick is not None:
            rid, handle, px, py = state.rr_pick
            dx = float(event.x) - px
            dy = float(event.y) - py
            if dx * dx + dy * dy >= 36.0:
                state.rr_drag = (rid, handle)
                state.rr_pick = None
                self._line_trading_desk_rr_drag_update(state, float(event.y))
                self._request_line_trading_desk_motion_chart_paint(state)
            return
        if state.rr_drag is not None:
            self._line_trading_desk_rr_drag_update(state, float(event.y))
            self._request_line_trading_desk_motion_chart_paint(state)
            return
        if state.draft_line_start is None:
            return
        if state.active_tool == "zoom_range":
            state.draft_line_current = (float(event.x), state.draft_line_start[1])
        else:
            state.draft_line_current = (float(event.x), float(event.y))
        self._request_line_trading_desk_motion_chart_paint(state)

    def _line_trading_desk_stop_price(self) -> Decimal | None:
        state = self._line_trading_desk_window
        if state is None or state.last_snapshot is None or not state.last_snapshot.candles:
            return None
        stop_lines = [item for item in state.line_annotations if item.kind == "stop"]
        if not stop_lines:
            return None
        last = stop_lines[-1]
        if last.price_a is not None:
            if last.price_b is not None:
                return (last.price_a + last.price_b) / Decimal("2")
            return last.price_a
        low, high = strategy_live_chart_price_bounds(state.last_snapshot)
        canvas_height = max(state.canvas.winfo_height(), 1)
        top = 26
        bottom = max(canvas_height - 44, top + 1)
        ratio = min(max((last.y1 - top) / max(bottom - top, 1), 0.0), 1.0)
        return high - (high - low) * Decimal(str(ratio))

    def _line_trading_desk_parse_tp_r(self, state: LineTradingDeskWindowState) -> Decimal:
        try:
            r = Decimal(str(state.param_rr_r.get()).strip() or "2")
        except InvalidOperation:
            r = Decimal("2")
        return r if r > 0 else Decimal("2")

    def _line_trading_desk_atr_stop_from_params(
        self, state: LineTradingDeskWindowState, direction: str, *, candles: list | None = None
    ) -> Decimal | None:
        snap = state.last_snapshot
        if snap is None or len(snap.candles) < 2:
            return None
        seq = list(candles) if candles is not None else list(snap.candles)
        if len(seq) < 2:
            return None
        try:
            atr_period = max(1, int(str(state.param_atr_period.get()).strip() or "10"))
        except ValueError:
            atr_period = 10
        try:
            atr_mult = Decimal(str(state.param_atr_mult.get()).strip() or "1")
        except InvalidOperation:
            atr_mult = Decimal("1")
        atr_values = atr(seq, atr_period)
        if not atr_values or atr_values[-1] is None or atr_values[-1] <= 0:
            return None
        atr_value = atr_values[-1] * atr_mult
        prev = seq[-2]
        if direction == "long":
            return prev.low - atr_value
        return prev.high + atr_value

    def _submit_line_trading_desk_order(
        self,
        direction: str,
        *,
        entry_price_override: Decimal | None = None,
        stop_price_override: Decimal | None = None,
        tp_r_multiple: Decimal | None = None,
        ray_annotation: LiveChartLineAnnotation | None = None,
    ) -> bool:
        """返回是否已排队执行网络下单。射线单须在交易所受理成功后才标记已触发（见 done/fail）。"""
        state = self._line_trading_desk_window
        if state is None or state.last_snapshot is None or not state.last_snapshot.candles:
            return False
        profile = state.api_profile_var.get().strip()
        credentials = self._credentials_for_profile_or_none(profile)
        if credentials is None:
            state.status_text.set("未配置API，无法下单。")
            return False
        symbol = state.symbol_var.get().strip().upper()
        instrument = self._line_trading_desk_require_instrument_for_submit(state, symbol)
        if instrument is None:
            return False
        # 射线自动开仓会传入参考价/止损覆盖；此时不得走「选中盈亏比框」分支，否则会静默失败且无提交日志。
        desk_price_override = entry_price_override is not None or stop_price_override is not None
        if not desk_price_override and state.rr_selected_id:
            rr_box = next((b for b in state.rr_annotations if b.rr_id == state.rr_selected_id), None)
            if rr_box is not None:
                self._line_trading_desk_submit_rr_bracket_order(state, rr_box, direction)
                return True
        entry_price = (
            entry_price_override
            if entry_price_override is not None
            else (state.last_snapshot.latest_price or state.last_snapshot.candles[-1].close)
        )
        if stop_price_override is not None:
            stop_price = stop_price_override
        else:
            stop_price = self._line_trading_desk_stop_price()
            if stop_price is None:
                stop_price = self._line_trading_desk_atr_stop_from_params(state, direction)
                if stop_price is None:
                    state.status_text.set("缺少止损线且ATR不可用。")
                    return False
        if direction == "long" and stop_price >= entry_price:
            state.status_text.set("多头止损价须低于开仓参考价。")
            return False
        if direction == "short" and stop_price <= entry_price:
            state.status_text.set("空头止损价须高于开仓参考价。")
            return False
        try:
            config = self._line_trading_desk_strategy_config(symbol, profile)
        except Exception as exc:
            state.status_text.set(f"无法组装下单配置：{exc}")
            return False
        try:
            risk_raw = str(state.param_risk_amount.get()).strip() or "0"
            risk_amount = Decimal(risk_raw)
        except InvalidOperation:
            state.status_text.set("风险金格式无效。")
            return False
        if risk_amount <= 0:
            state.status_text.set("风险金必须大于0。")
            return False
        try:
            size = determine_order_size(
                instrument=instrument,
                config=replace(config, risk_amount=risk_amount, order_size=None),
                entry_price=entry_price,
                stop_loss=stop_price,
                risk_price_compatible=True,
            )
        except Exception as exc:
            state.status_text.set(f"定量失败：{exc}")
            return False
        if state.desk_submit_inflight:
            state.status_text.set("已有委托正在提交，请稍候…")
            return False
        tp_r_use = tp_r_multiple if tp_r_multiple is not None else self._line_trading_desk_parse_tp_r(state)
        side = "buy" if direction == "long" else "sell"
        pos_side = resolve_open_pos_side(config, side)
        order_mode = state.param_order_mode.get() if state.param_order_mode is not None else "限价挂单"
        session_log_tag = f"desk:{_normalize_symbol_input(symbol)}"
        api_profile = profile
        cl_ord_id = f"ld{datetime.utcnow().strftime('%y%m%d%H%M%S%f')}"[:32]
        desk_ref = state
        ann_ref = ray_annotation
        if ann_ref is not None:
            ann_ref.desk_ray_submit_pending = True
            self._line_trading_desk_refresh_ray_tree(state)
        state.desk_submit_inflight = True
        state.status_text.set("正在提交委托…")

        def work() -> None:
            try:
                if order_mode == "对手价":
                    result = self.client.place_aggressive_limit_order(
                        credentials,
                        config,
                        instrument,
                        side=side,
                        size=size,
                        pos_side=pos_side,
                        cl_ord_id=cl_ord_id,
                    )
                else:
                    result = self.client.place_simple_order(
                        credentials,
                        config,
                        inst_id=symbol,
                        side=side,
                        size=size,
                        ord_type="limit",
                        pos_side=pos_side,
                        price=entry_price,
                        cl_ord_id=cl_ord_id,
                    )
            except Exception as exc:
                err = str(exc)

                def fail() -> None:
                    st = self._line_trading_desk_window
                    if st is not desk_ref or not _widget_exists(st.window):
                        return
                    st.desk_submit_inflight = False
                    if ann_ref is not None:
                        ann_ref.desk_ray_submit_pending = False
                        self._line_trading_desk_dual_log(
                            st,
                            f"射线自动开仓下单失败：{err}",
                        )
                        self._line_trading_desk_refresh_ray_tree(st)
                    st.status_text.set(f"下单失败：{err}")

                self.root.after(0, fail)
                return

            rid = (result.ord_id or "-").strip() or "-"
            track_ord = (result.ord_id or "").strip()
            track_cl = (result.cl_ord_id or cl_ord_id or "").strip()

            def done() -> None:
                st = self._line_trading_desk_window
                if st is not desk_ref or not _widget_exists(st.window):
                    return
                st.desk_submit_inflight = False
                if ann_ref is not None:
                    ann_ref.desk_ray_submit_pending = False
                    ann_ref.desk_ray_triggered = True
                    self._line_trading_desk_refresh_ray_tree(st)
                mode_note = "对手价IOC" if order_mode == "对手价" else "限价"
                st.status_text.set(
                    f"已提交{('多' if direction == 'long' else '空')}单（{mode_note}）| {symbol} | "
                    f"size={format_decimal(size)} | ordId={rid} | clOrdId={track_cl or '-'}"
                )
                sz_seg = self._line_trading_desk_sz_log_segment(symbol, size)
                self._line_trading_desk_dual_log(
                    st,
                    f"提交{direction.upper()}单 | 标的={symbol} | 方式={mode_note} | "
                    f"开仓价={self._line_trading_desk_format_price(st, entry_price)} | "
                    f"止损={self._line_trading_desk_format_price(st, stop_price)} | {sz_seg} | "
                    f"ordId={rid} | clOrdId={track_cl or '-'}",
                )
                threading.Thread(
                    target=self._line_desk_post_entry_worker,
                    args=(
                        desk_ref,
                        credentials,
                        config,
                        instrument,
                        track_ord,
                        track_cl,
                        side,
                        pos_side,
                        size,
                        entry_price,
                        stop_price,
                        session_log_tag,
                        api_profile,
                        tp_r_use,
                    ),
                    daemon=True,
                ).start()

            self.root.after(0, done)

        threading.Thread(target=work, daemon=True).start()
        return True

    def _line_trading_desk_selected_position(self) -> OkxPosition | None:
        state = self._line_trading_desk_window
        if state is None:
            return None
        selection = state.position_tree.selection()
        if not selection:
            return None
        sel = selection[0]
        items = state.latest_positions
        bases = [self._line_trading_desk_position_row_iid(it, i) for i, it in enumerate(items)]
        for final_iid, it in zip(_desk_tree_final_iids_from_bases(bases), items):
            if final_iid == sel:
                return it
        idx = _history_tree_index(sel, "desk-pos")
        if idx is not None and 0 <= idx < len(items):
            return items[idx]
        return None

    def _line_trading_desk_selected_pending_order(self) -> OkxTradeOrderItem | None:
        state = self._line_trading_desk_window
        if state is None:
            return None
        selection = state.pending_orders_tree.selection()
        if not selection:
            return None
        sel = selection[0]
        items = state.latest_pending_orders
        bases = [self._line_trading_desk_order_row_iid(it, i, prefix="desk-pend") for i, it in enumerate(items)]
        for final_iid, it in zip(_desk_tree_final_iids_from_bases(bases), items):
            if final_iid == sel:
                return it
        idx = _history_tree_index(sel, "desk-pend")
        if idx is not None and 0 <= idx < len(items):
            return items[idx]
        return None

    def _line_trading_desk_cancel_selected_pending_order(self) -> None:
        state = self._line_trading_desk_window
        if state is None:
            return
        item = self._line_trading_desk_selected_pending_order()
        if item is None:
            messagebox.showinfo("撤单", "请先在「当前委托」列表中选中一条记录。", parent=state.window)
            return
        profile = state.api_profile_var.get().strip()
        credentials = self._credentials_for_profile_or_none(profile)
        if credentials is None:
            state.status_text.set("未配置API，无法撤单。")
            return
        env_label = self._environment_label_for_profile(profile or self._current_credential_profile())
        environment = ENV_OPTIONS[self._normalized_environment_label(env_label)]
        desk_ref = state
        inst = item.inst_id
        src = item.source_kind
        state.status_text.set("正在撤销委托…")

        def work() -> None:
            try:
                if item.source_kind == "algo":
                    algo_id = (item.algo_id or "").strip()
                    algo_cl = (item.algo_client_order_id or item.client_order_id or "").strip()
                    if algo_id:
                        result = self.client.cancel_algo_order(
                            credentials,
                            environment=environment,
                            inst_id=item.inst_id,
                            algo_id=algo_id,
                        )
                    elif algo_cl:
                        result = self.client.cancel_algo_order(
                            credentials,
                            environment=environment,
                            inst_id=item.inst_id,
                            algo_cl_ord_id=algo_cl,
                        )
                    else:
                        raise OkxApiError("算法委托缺少 algoId / algoClOrdId")
                else:
                    oid = (item.order_id or "").strip()
                    cl = (item.client_order_id or "").strip()
                    if oid:
                        result = self.client.cancel_order_by_id(
                            credentials,
                            environment=environment,
                            inst_id=item.inst_id,
                            ord_id=oid,
                        )
                    elif cl:
                        result = self.client.cancel_order_by_id(
                            credentials,
                            environment=environment,
                            inst_id=item.inst_id,
                            cl_ord_id=cl,
                        )
                    else:
                        raise OkxApiError("普通委托缺少 ordId / clOrdId")
            except Exception as exc:
                err = str(exc)

                def fail() -> None:
                    st = self._line_trading_desk_window
                    if st is not desk_ref or not _widget_exists(st.window):
                        return
                    st.status_text.set(f"撤单失败：{err}")

                self.root.after(0, fail)
                return

            rid = (result.ord_id or "-").strip() or "-"

            def ok() -> None:
                st = self._line_trading_desk_window
                if st is not desk_ref or not _widget_exists(st.window):
                    return
                st.status_text.set(f"已提交撤单 | {inst} | id={rid}")
                self._line_trading_desk_dual_log(
                    st,
                    f"撤销委托 | {inst} | 来源={src} | ordType={item.ord_type or '-'} | id={rid}",
                )
                self._request_line_trading_desk_refresh(immediate=True)

            self.root.after(0, ok)

        threading.Thread(target=work, daemon=True).start()

    def _line_trading_desk_flatten_selected(self, flatten_mode: str, *, use_qty_field: bool = False) -> None:
        state = self._line_trading_desk_window
        if state is None:
            return
        position = self._line_trading_desk_selected_position()
        if position is None:
            messagebox.showinfo("平仓", "请先在持仓列表里选中一条记录。", parent=state.window)
            return
        qty_override: Decimal | None = None
        if use_qty_field:
            raw = (state.param_close_qty.get() or "").strip()
            if raw:
                try:
                    qty_override = Decimal(raw)
                except InvalidOperation:
                    messagebox.showinfo("平仓", "平仓数量格式无效。", parent=state.window)
                    return
                if qty_override <= 0:
                    messagebox.showinfo("平仓", "平仓数量须大于 0。", parent=state.window)
                    return
        prev_ctx = self._positions_context_profile_name
        try:
            self._positions_context_profile_name = state.api_profile_var.get().strip()
            result, price, normalized_flatten_mode = self._submit_selected_position_manual_flatten(
                position, flatten_mode, close_size=qty_override
            )
        except Exception as exc:
            messagebox.showerror("平仓失败", str(exc), parent=state.window)
            return
        finally:
            self._positions_context_profile_name = prev_ctx
        mode_label = self._position_manual_flatten_mode_label(normalized_flatten_mode)
        state.status_text.set(
            f"已提交平仓 | {position.inst_id} | 方式={mode_label} | ordId={result.ord_id or '-'}"
        )
        self._line_trading_desk_dual_log(
            state,
            f"提交平仓 | 合约={position.inst_id} | 方式={mode_label} | ordId={result.ord_id or '-'}",
        )
        if normalized_flatten_mode == "best_quote" and price is not None:
            self._line_trading_desk_dual_log(
                state,
                f"平仓挂单价={self._line_trading_desk_format_price(state, price)}",
            )
        self._request_line_trading_desk_refresh(immediate=True)

    def _on_close(self) -> None:
        confirmed = messagebox.askyesno(
            "确认关闭",
            "是否要关闭主界面？",
            parent=self.root,
        )
        if not confirmed:
            return
        self._save_strategy_parameter_draft()
        self._save_credentials_now(silent=True)
        self._save_notification_settings_now(silent=True)
        closed_at = datetime.now()
        for session in self.sessions.values():
            if session.status in {"运行中", "停止中"} or session.engine.is_running:
                if session.recovery_supported:
                    session.status = "待恢复"
                    session.runtime_status = "待恢复"
                    session.ended_reason = "应用关闭后待恢复接管"
                    session.stopped_at = closed_at
                    self._upsert_recoverable_strategy_session(session)
                else:
                    session.status = "已停止"
                    if not session.ended_reason:
                        session.ended_reason = "应用关闭"
                    self._remove_recoverable_strategy_session(session.session_id)
                    if session.stopped_at is None:
                        session.stopped_at = closed_at
                if session.stopped_at is None:
                    session.stopped_at = closed_at
                self._sync_strategy_history_from_session(session)
            session.engine.stop()
            session.engine.wait_stopped(timeout=1.5)
        self._protection_manager.stop_all()
        self._close_strategy_history_window()
        self._close_strategy_book_window()
        self._close_all_strategy_live_chart_windows()
        self._close_line_trading_desk_window()
        self._close_settings_window()
        if self._backtest_window is not None and self._backtest_window.window.winfo_exists():
            self._backtest_window.window.destroy()
        if self._backtest_compare_window is not None and self._backtest_compare_window.window.winfo_exists():
            self._backtest_compare_window.window.destroy()
        if (
            self._btc_market_analysis_window is not None
            and self._btc_market_analysis_window.window.winfo_exists()
        ):
            self._btc_market_analysis_window.destroy()
        if (
            self._btc_research_workbench_window is not None
            and self._btc_research_workbench_window.window.winfo_exists()
        ):
            self._btc_research_workbench_window.destroy()
        if self._signal_replay_mock_window is not None and self._signal_replay_mock_window.window.winfo_exists():
            self._signal_replay_mock_window.destroy()
        if self._journal_window is not None and self._journal_window.window.winfo_exists():
            self._journal_window.destroy()
        if self._signal_monitor_window is not None and self._signal_monitor_window.window.winfo_exists():
            self._signal_monitor_window.destroy()
        if self._trader_desk_window is not None and self._trader_desk_window.window.winfo_exists():
            self._trader_desk_window.destroy()
        if self._smart_order_window is not None and self._smart_order_window.window.winfo_exists():
            self._smart_order_window.destroy()
        if (
            self._deribit_volatility_monitor_window is not None
            and self._deribit_volatility_monitor_window.window.winfo_exists()
        ):
            self._deribit_volatility_monitor_window.destroy()
        if self._deribit_volatility_window is not None and self._deribit_volatility_window.window.winfo_exists():
            self._deribit_volatility_window.window.destroy()
        if self._option_strategy_window is not None and self._option_strategy_window.window.winfo_exists():
            self._option_strategy_window.window.destroy()
        if self._option_roll_window is not None and self._option_roll_window.window.winfo_exists():
            self._option_roll_window.window.destroy()
        if self._protection_replay_window is not None and self._protection_replay_window.window.winfo_exists():
            self._protection_replay_window.window.destroy()
        self._close_positions_zoom_window()
        self._close_position_protection_window()
        self.root.destroy()

def _format_optional_decimal(value: Decimal | None, *, with_sign: bool = False) -> str:
    if value is None:
        return "-"
    text = format_decimal(value)
    if with_sign and value > 0:
        return f"+{text}"
    return text


def _format_optional_decimal_fixed(value: Decimal | None, *, places: int, with_sign: bool = False) -> str:
    if value is None:
        return "-"
    text = format_decimal_fixed(value, places)
    if with_sign and value > 0:
        return f"+{text}"
    return text


def _format_summary_delta(value: Decimal | None) -> str:
    if value is None:
        return "-"
    magnitude = abs(value)
    if magnitude >= Decimal("1000"):
        places = 2
    elif magnitude >= Decimal("1"):
        places = 4
    else:
        places = 5
    return _format_optional_decimal_fixed(value, places=places)


def _format_optional_integer(value: Decimal | None, *, with_sign: bool = False) -> str:
    if value is None:
        return "-"
    text = format_decimal_fixed(value, 0)
    if with_sign and value > 0:
        return f"+{text}"
    return text


def _format_optional_approx_usdt(value: Decimal | None) -> str:
    if value is None:
        return "-"
    return f"≈{_format_optional_usdt(value, with_sign=False)} USDT"


def _format_optional_usdt(value: Decimal | int | float | None, *, with_sign: bool = True) -> str:
    if value is None:
        return "-"
    if not isinstance(value, Decimal):
        value = Decimal(str(value))
    text = format_decimal_fixed(value, 0)
    if with_sign and value > 0:
        return f"+{text}"
    return text


def _format_optional_usdt_precise(
    value: Decimal | int | float | None,
    *,
    places: int = 2,
    with_sign: bool = True,
) -> str:
    if value is None:
        return "-"
    if not isinstance(value, Decimal):
        value = Decimal(str(value))
    text = format_decimal_fixed(value, places)
    if with_sign and value > 0:
        return f"+{text}"
    return text


def _instrument_contract_value_snapshot(instrument: Instrument) -> tuple[Decimal | None, str | None]:
    contract_value = (instrument.ct_val or Decimal("0")) * (instrument.ct_mult or Decimal("1"))
    contract_ccy = (instrument.ct_val_ccy or "").strip().upper()
    if not contract_ccy:
        normalized_inst_id = instrument.inst_id.strip().upper()
        if "-" in normalized_inst_id:
            contract_ccy = normalized_inst_id.split("-", 1)[0]
    if contract_value > 0:
        return contract_value, contract_ccy or None
    return None, contract_ccy or None


def _format_contract_size_with_equivalent(instrument: Instrument, size: Decimal) -> str:
    contract_value, contract_ccy = _instrument_contract_value_snapshot(instrument)
    if instrument.inst_type in {"SWAP", "FUTURES", "OPTION"} and contract_value is not None and contract_ccy:
        amount = abs(size) * contract_value
        amount_text = format_decimal(amount)
        if size < 0:
            amount_text = f"-{amount_text}"
        return f"{format_decimal(size)}张（折合{amount_text} {contract_ccy}）"
    return format_decimal(size)


def _parse_positive_decimal_hint(raw: str) -> Decimal | None:
    cleaned = raw.strip()
    if not cleaned:
        return None
    try:
        value = Decimal(cleaned)
    except InvalidOperation:
        return None
    if value <= 0:
        return None
    return value


def _build_minimum_order_risk_hint_text(
    *,
    inst_id: str,
    instrument: Instrument | None,
    risk_amount_raw: str,
    minimum_risk_amount: Decimal | None = None,
    note: str = "",
    pending: bool = False,
) -> str:
    normalized_inst_id = inst_id.strip().upper()
    if not normalized_inst_id:
        return "下单门槛：请先选择下单标的。"
    if instrument is None:
        return f"下单门槛：正在读取 {normalized_inst_id} 的最小下单规格。"
    min_order_text = _format_contract_size_with_equivalent(instrument, instrument.min_size)
    parts = [f"下单门槛：最小下单量 {min_order_text}。"]
    if minimum_risk_amount is not None and minimum_risk_amount > 0:
        current_risk_amount = _parse_positive_decimal_hint(risk_amount_raw)
        threshold_text = format_decimal(minimum_risk_amount)
        if current_risk_amount is None:
            parts.append(f"按当前止损估算，至少需要风险金 {threshold_text}。")
        elif current_risk_amount >= minimum_risk_amount:
            parts.append(
                f"按当前止损估算，至少需要风险金 {threshold_text}；你当前填写 {format_decimal(current_risk_amount)}，可以下最小一笔。"
            )
        else:
            parts.append(
                f"按当前止损估算，至少需要风险金 {threshold_text}；你当前填写 {format_decimal(current_risk_amount)}，还不够下最小一笔。"
            )
    elif pending:
        parts.append("正在结合当前止损估算最低风险金...")
    if note:
        parts.append(note)
    return " ".join(parts)


def _estimate_launcher_minimum_risk_amount(
    *,
    client: OkxRestClient,
    signal_inst_id: str,
    trade_instrument: Instrument,
    config: StrategyConfig,
) -> tuple[Decimal | None, str]:
    if config.run_mode != "trade":
        return None, "当前运行模式不下单，不需要估算最低风险金。"
    if trade_instrument.inst_type == "SPOT":
        return None, f"现货按币数量下单，最小步长={format_decimal(trade_instrument.lot_size)}。"
    contract_value, _ = _instrument_contract_value_snapshot(trade_instrument)
    if contract_value is None or trade_instrument.min_size <= 0:
        return None, "当前标的缺少合约面值或最小下单量信息，暂时无法估算。"

    trigger_inst_id = (config.trade_inst_id or signal_inst_id).strip().upper()
    if config.tp_sl_mode == "local_signal":
        trigger_inst_id = signal_inst_id.strip().upper()
    elif config.tp_sl_mode == "local_custom":
        trigger_inst_id = (config.local_tp_sl_inst_id or "").strip().upper()
    if trigger_inst_id and trigger_inst_id != trade_instrument.inst_id.strip().upper():
        return None, "当前止盈止损参考的是其他标的，最低风险金暂不做联动估算。"

    if config.strategy_id == STRATEGY_EMA5_EMA8_ID:
        lookback = recommended_indicator_lookback(config.ema_period, config.trend_ema_period)
        strategy = EmaCrossEmaStopStrategy()
    elif is_dynamic_strategy_id(config.strategy_id):
        lookback = recommended_indicator_lookback(
            config.ema_period,
            config.trend_ema_period,
            config.atr_period,
            config.resolved_entry_reference_ema_period(),
        )
        strategy = EmaDynamicOrderStrategy()
    else:
        lookback = recommended_indicator_lookback(
            config.ema_period + 2,
            config.trend_ema_period + 2,
            config.big_ema_period + 2,
            config.atr_period + 2,
        )
        strategy = EmaAtrStrategy()

    candles = client.get_candles(signal_inst_id, config.bar, limit=lookback)
    confirmed = [candle for candle in candles if candle.confirmed]
    decision = strategy.evaluate(confirmed, config, price_increment=trade_instrument.tick_size)
    if decision.signal is None or decision.entry_reference is None or decision.candle_ts is None:
        return None, "当前还没有有效信号，等出现可挂单的一波后再给出估算。"

    if config.strategy_id == STRATEGY_EMA5_EMA8_ID:
        _, stop_line = strategy.latest_stop_line(confirmed, config)
        stop_loss = snap_to_increment(stop_line, trade_instrument.tick_size, "nearest")
        minimum_risk_amount = abs(decision.entry_reference - stop_loss) * contract_value * trade_instrument.min_size
        if minimum_risk_amount <= 0:
            return None, "当前止损距离过小，暂时无法估算最低风险金。"
        return minimum_risk_amount, "按最新 EMA 止损位估算。"

    if decision.atr_value is None:
        return None, "ATR 还没准备好，暂时无法估算最低风险金。"

    protection = build_protection_plan(
        instrument=trade_instrument,
        config=config,
        direction=decision.signal,
        entry_reference=decision.entry_reference,
        atr_value=decision.atr_value,
        candle_ts=decision.candle_ts,
        trigger_inst_id=trade_instrument.inst_id,
                use_signal_extrema=is_ema_atr_breakout_strategy(config.strategy_id),
        signal_candle_high=decision.signal_candle_high,
        signal_candle_low=decision.signal_candle_low,
    )
    minimum_risk_amount = (
        abs(protection.entry_reference - protection.stop_loss) * contract_value * trade_instrument.min_size
    )
    if minimum_risk_amount <= 0:
        return None, "当前止损距离过小，暂时无法估算最低风险金。"
    return minimum_risk_amount, "按当前止损距离估算。"
def _build_fixed_order_size_hint_text(inst_id: str, instrument: Instrument | None) -> str:
    prefix = "固定数量=OKX下单数量(sz)，不是USDT；若填写风险金，则优先按风险金计算。"
    normalized_inst_id = inst_id.strip().upper()
    if not normalized_inst_id:
        return prefix
    if instrument is None:
        return f"{prefix} 当前标的：{normalized_inst_id}。"
    if instrument.inst_type == "SPOT":
        return f"{prefix} 当前 {normalized_inst_id} 按币数量填写；最小步长={format_decimal(instrument.lot_size)}。"

    contract_value = (instrument.ct_val or Decimal("0")) * (instrument.ct_mult or Decimal("1"))
    contract_ccy = (instrument.ct_val_ccy or "").strip().upper()
    if not contract_ccy and "-" in normalized_inst_id:
        contract_ccy = normalized_inst_id.split("-", 1)[0]
    if contract_value > 0 and contract_ccy:
        step_value = contract_value * instrument.lot_size
        return (
            f"{prefix} 当前 {normalized_inst_id}：1={format_decimal(contract_value)} {contract_ccy}，"
            f"10={format_decimal(contract_value * Decimal('10'))} {contract_ccy}，"
            f"最小步长={format_decimal(instrument.lot_size)}（约{format_decimal(step_value)} {contract_ccy}）。"
        )
    return f"{prefix} 当前 {normalized_inst_id}：最小步长={format_decimal(instrument.lot_size)}。"


def _build_order_size_mode_hint_text(risk_amount_raw: str, order_size_raw: str) -> str:
    def _parse_positive(raw: str) -> Decimal | None:
        cleaned = raw.strip()
        if not cleaned:
            return None
        try:
            value = Decimal(cleaned)
        except InvalidOperation:
            return None
        if value <= 0:
            return None
        return value

    risk_amount = _parse_positive(risk_amount_raw)
    order_size = _parse_positive(order_size_raw)
    if risk_amount is not None:
        return "当前模式：风险金优先，固定数量仅作备用。"
    if order_size is not None:
        return "当前模式：若风险金留空，将按固定数量下单。"
    return "当前模式：请填写风险金或固定数量其一。"


def _build_launch_parameter_hint_text(
    *,
    stop_atr_raw: str,
    take_atr_raw: str,
    take_profit_mode_label: str,
    max_entries_raw: str,
    startup_chase_window_raw: str,
) -> str:
    stop_atr = stop_atr_raw.strip() or "?"
    take_atr = take_atr_raw.strip() or "?"
    max_entries = max_entries_raw.strip() or "?"
    startup_chase_window = startup_chase_window_raw.strip() or "0"
    parts = [
        f"止损ATR倍数：{stop_atr}=止损距离是 {stop_atr}×ATR。",
    ]
    if take_profit_mode_label == "动态止盈":
        parts.append(
            f"止盈ATR倍数：{take_atr} 在动态止盈下不用于初始挂止盈，系统会先挂止损，后续靠上移止损锁盈。"
        )
    else:
        parts.append(f"止盈ATR倍数：{take_atr}=止盈距离是 {take_atr}×ATR。")
    if max_entries == "0":
        parts.append("每波最多开仓次数：0=不限，同一波行情可重复开仓。")
    else:
        parts.append(f"每波最多开仓次数：{max_entries}=同一波最多开 {max_entries} 次。")
    if startup_chase_window in {"", "0"}:
        parts.append("启动追单窗口：0=启动不追老信号，只等启动后的新波。")
    else:
        resolved = try_parse_nonnegative_duration_seconds(startup_chase_window)
        if resolved is None:
            parts.append(f"启动追单窗口：{startup_chase_window}（无法换算，请检查写法）。")
        elif resolved == 0:
            parts.append("启动追单窗口：0=启动不追老信号，只等启动后的新波。")
        else:
            human = format_duration_cn_compact(resolved)
            parts.append(
                f"启动追单窗口：输入「{startup_chase_window}」等价 {resolved} 秒（{human}），"
                f"只接管启动前该时长内刚确认的波。"
            )
    return "参数速记： " + " ".join(parts)


def _build_trend_parameter_hint_text(
    *,
    strategy_id: str,
    ema_period_raw: str,
    trend_ema_period_raw: str,
    big_ema_period_raw: str,
    entry_reference_ema_period_raw: str,
) -> str:
    ema_period = ema_period_raw.strip() or "?"
    trend_ema_period = trend_ema_period_raw.strip() or "?"
    big_ema_period = big_ema_period_raw.strip() or "?"
    entry_reference_ema_period = entry_reference_ema_period_raw.strip() or "0"
    parts = [
        f"EMA小周期：{ema_period}=快线，负责捕捉最近节奏。",
        f"EMA中周期：{trend_ema_period}=趋势过滤线，用来判断当前方向是否还有效。",
    ]
    if strategy_id == STRATEGY_EMA_BREAKDOWN_SHORT_ID:
        ref = entry_reference_ema_period if entry_reference_ema_period not in {"", "0"} else ema_period
        parts.append(
            f"参考EMA({ref})：收盘向下跌破该线触发做空，且须 EMA{ema_period}<EMA{trend_ema_period}。"
        )
    elif is_ema_atr_breakout_strategy(strategy_id):
        ref = entry_reference_ema_period if entry_reference_ema_period not in {"", "0"} else ema_period
        parts.append(
            f"参考EMA({ref})：收盘向上突破该线触发做多，且须 EMA{ema_period}>EMA{trend_ema_period}。"
        )
    elif strategy_id == STRATEGY_EMA5_EMA8_ID:
        parts.append(f"EMA大周期：{big_ema_period}=4H 大趋势过滤线。")
    if is_dynamic_strategy_id(strategy_id):
        if entry_reference_ema_period in {"", "0"}:
            parts.append(f"挂单参考EMA：0=跟随EMA小周期，当前按 EMA{ema_period} 作为挂单价格锚点。")
        else:
            parts.append(f"挂单参考EMA：{entry_reference_ema_period}=挂单价格锚点，价格会围绕 EMA{entry_reference_ema_period} 重挂。")
    return "趋势参数： " + " ".join(parts)


def _entry_reference_ema_caption(strategy_id: str) -> str:
    if is_dynamic_strategy_id(strategy_id):
        return "挂单参考EMA"
    if strategy_id == STRATEGY_EMA_BREAKDOWN_SHORT_ID or is_ema_atr_breakout_strategy(strategy_id):
        return "突破参考EMA"
    return "参考EMA周期"


def _build_dynamic_protection_hint_text(
    *,
    take_profit_mode_label: str,
    dynamic_two_r_break_even_enabled: bool,
    dynamic_fee_offset_enabled: bool,
    time_stop_break_even_enabled: bool,
    time_stop_break_even_bars_raw: str,
) -> str:
    if take_profit_mode_label != "动态止盈":
        return "动态保护：当前为固定止盈，2R保本 / 手续费偏移 / 时间保本都不生效。"
    time_stop_bars = time_stop_break_even_bars_raw.strip() or "0"
    parts = [
        (
            "2R保本：开启，浮盈达到 2R 后先把止损抬到保本位。"
            if dynamic_two_r_break_even_enabled
            else "2R保本：关闭，浮盈达到 2R 也不会自动抬到保本位。"
        ),
        (
            "手续费偏移：开启，保本位会额外预留双边手续费缓冲。"
            if dynamic_fee_offset_enabled
            else "手续费偏移：关闭，保本位不额外预留手续费缓冲。"
        ),
        (
            f"时间保本：开启，持仓满 {time_stop_bars} 根K线且达到净保本后，再把止损抬到保本位。"
            if time_stop_break_even_enabled
            else f"时间保本：关闭（当前设定 {time_stop_bars} 根，仅保存参数，不会启用）。"
        ),
    ]
    return "动态保护： " + " ".join(parts)


def _build_strategy_start_confirmation_message(
    *,
    strategy_name: str,
    rule_description: str,
    strategy_symbol: str,
    config: StrategyConfig,
    run_mode_label: str,
    environment_label: str,
    trade_mode_label: str,
    position_mode_label: str,
    signal_mode_label: str,
    entry_side_mode_label: str,
    tp_sl_mode_label: str,
    trigger_type_label: str,
    take_profit_mode_label: str,
    risk_value: str,
    fixed_size: str,
    custom_trigger_symbol: str,
    instrument: Instrument | None = None,
    api_label: str = "",
) -> str:
    def _signal_mode_text(label: str) -> str:
        description = {
            "双向": "多空信号都接收",
            "只做多": "只接收多头信号",
            "只做空": "只接收空头信号",
        }.get(label, "")
        return f"{label}（{description}）" if description else label

    def _entry_side_mode_text(label: str) -> str:
        description = {
            "跟随信号": "多头买入，空头卖出",
            "固定买入": "忽略信号方向，统一按买入开仓",
            "固定卖出": "忽略信号方向，统一按卖出开仓",
        }.get(label, "")
        return f"{label}（{description}）" if description else label

    def _tp_sl_mode_text() -> str:
        if config.tp_sl_mode == "exchange":
            if is_dynamic_strategy_id(config.strategy_id) and config.take_profit_mode == "dynamic":
                return f"{tp_sl_mode_label}（开仓后由 OKX 托管初始止损，后续本地动态上移保护价）"
            return f"{tp_sl_mode_label}（开仓后由 OKX 托管止损/止盈）"
        if config.tp_sl_mode == "local_trade":
            return f"{tp_sl_mode_label}（本地监控下单标的价格，触发后再执行平仓）"
        if config.tp_sl_mode == "local_signal":
            return f"{tp_sl_mode_label}（本地监控信号标的价格，触发后再执行平仓）"
        if config.tp_sl_mode == "local_custom":
            return f"{tp_sl_mode_label}（本地监控自定义标的价格，触发后再执行平仓）"
        return tp_sl_mode_label

    def _trigger_type_text(label: str) -> str:
        description = {
            "标记价格 mark": "止损/止盈按标记价触发",
            "最新成交价 last": "止损/止盈按最新成交价触发",
            "指数价格 index": "止损/止盈按指数价格触发",
        }.get(label, "")
        return f"{label}（{description}）" if description else label

    def _startup_chase_text() -> str:
        seconds = config.resolved_startup_chase_window_seconds()
        if seconds <= 0:
            return "关闭（启动不追老信号，只等新波）"
        return f"{seconds}秒（只接管启动前窗口内刚确认的波）"

    def _risk_amount_text() -> str:
        if config.run_mode == "signal_only":
            return "-（当前仅发信号，不下单）"
        if config.risk_amount is not None and config.risk_amount > 0:
            return f"{risk_value}（按止损距离反推仓位）"
        return "-（当前不按风险金反推仓位）"

    def _fixed_size_example_text() -> str:
        if instrument is None:
            return ""
        if instrument.inst_type == "SPOT":
            return f"；{strategy_symbol} 按币数量填写"
        contract_value = (instrument.ct_val or Decimal("0")) * (instrument.ct_mult or Decimal("1"))
        contract_ccy = (instrument.ct_val_ccy or "").strip().upper()
        if not contract_ccy:
            inst_text = (instrument.inst_id or strategy_symbol).strip().upper()
            if "-" in inst_text:
                contract_ccy = inst_text.split("-", 1)[0]
        if contract_value > 0 and contract_ccy:
            return f"；{strategy_symbol} 下 1={format_decimal(contract_value)} {contract_ccy}"
        return ""

    def _fixed_size_text() -> str:
        if config.run_mode == "signal_only":
            return "-（当前仅发信号，不下单）"
        if config.risk_amount is not None and config.risk_amount > 0:
            if fixed_size and fixed_size != "-":
                return f"{fixed_size}（OKX 下单数量 sz；当前已填写风险金，仅作备用{_fixed_size_example_text()}）"
            return "-（当前按风险金反推仓位）"
        if fixed_size and fixed_size != "-":
            return f"{fixed_size}（OKX 下单数量 sz；当前按固定数量下单{_fixed_size_example_text()}）"
        return "-"

    def _custom_trigger_text() -> str:
        if config.tp_sl_mode != "local_custom":
            return "-（当前模式未使用）"
        return f"{custom_trigger_symbol or '-'}（本地止盈止损按这个标的触发）"

    stop_atr_text = format_decimal(config.atr_stop_multiplier)
    take_atr_text = format_decimal(config.atr_take_multiplier)
    take_profit_mode_text = (
        f"{take_profit_mode_label}（初始不挂止盈，后续通过上移止损锁盈）"
        if config.take_profit_mode == "dynamic"
        else f"{take_profit_mode_label}（止盈距离 = {take_atr_text} × ATR）"
    )
    take_profit_atr_text = (
        f"{take_atr_text}（当前为动态止盈，初始不直接挂止盈）"
        if config.take_profit_mode == "dynamic"
        else f"{take_atr_text}（止盈距离 = {take_atr_text} × ATR）"
    )

    api_text = (api_label or "").strip() or "-"

    lines = [
        f"策略：{strategy_name}",
        "",
        "基础信息：",
        f"运行模式：{run_mode_label}",
        f"交易环境：{environment_label}",
        f"API：{api_text}",
        f"交易模式：{trade_mode_label}",
        f"持仓模式：{position_mode_label}",
        f"交易标的：{strategy_symbol}",
        f"K线周期：{config.bar}",
        "",
        "执行口径：",
        f"信号方向：{_signal_mode_text(signal_mode_label)}",
        f"下单方向模式：{_entry_side_mode_text(entry_side_mode_label)}",
        f"止盈止损模式：{_tp_sl_mode_text()}",
        f"触发价格类型：{_trigger_type_text(trigger_type_label)}",
        f"自定义触发标的：{_custom_trigger_text()}",
        "",
        "参数说明：",
        f"EMA小周期：{config.ema_period}（快线）",
        f"EMA中周期：{config.trend_ema_period}（趋势过滤线）",
    ]
    if config.strategy_id == STRATEGY_EMA5_EMA8_ID:
        lines.append(f"EMA大周期：{config.big_ema_period}（大趋势过滤线）")
    if config.strategy_id == STRATEGY_EMA_BREAKDOWN_SHORT_ID or is_ema_atr_breakout_strategy(config.strategy_id):
        lines.append(f"突破参考EMA：EMA{config.resolved_entry_reference_ema_period()}（已收盘K线的突破/跌破判断基准）")
    if is_dynamic_strategy_id(config.strategy_id) or config.strategy_id == STRATEGY_EMA_BREAKDOWN_SHORT_ID or is_ema_atr_breakout_strategy(config.strategy_id):
        if is_dynamic_strategy_id(config.strategy_id):
            lines.extend(
                [
                    f"挂单参考EMA：{config.entry_reference_ema_label()}（挂单价格锚点）",
                    f"止盈方式：{take_profit_mode_text}",
                    f"每波最多开仓次数：{config.max_entries_per_trend if config.max_entries_per_trend > 0 else '不限'}（同一波最多允许开仓的次数）",
                    f"启动追单窗口：{_startup_chase_text()}",
                ]
            )
        else:
            # 突破参考 EMA 已在上方「参数说明」首段追加，此处勿重复
            lines.extend(
                [
                    f"止盈方式：{take_profit_mode_text}",
                    f"每波最多开仓次数：{config.max_entries_per_trend if config.max_entries_per_trend > 0 else '不限'}（同一波最多允许开仓的次数）",
                    f"启动追单窗口：{_startup_chase_text()}",
                ]
            )
        if config.take_profit_mode == "dynamic":
            lines.extend(
                [
                    f"2R保本开关：{config.dynamic_two_r_break_even_label()}（浮盈达到 2R 后止损抬到保本）",
                    f"手续费偏移开关：{config.dynamic_fee_offset_enabled_label()}（保本位预留双边手续费）",
                    (
                        f"时间保本：{config.time_stop_break_even_enabled_label()} / "
                        f"{config.resolved_time_stop_break_even_bars()}根（持仓满指定K线且达到净保本时再抬止损）"
                    ),
                ]
            )
    lines.extend(
        [
            f"ATR周期：{config.atr_period}（波动计算周期）",
            f"止损 ATR 倍数：{stop_atr_text}（止损距离 = {stop_atr_text} × ATR）",
            f"止盈 ATR 倍数：{take_profit_atr_text}",
            f"风险金：{_risk_amount_text()}",
            f"固定数量：{_fixed_size_text()}",
            "",
            "策略规则：",
            rule_description,
            "",
            "确认启动这个策略吗？",
        ]
    )
    return "\n".join(lines)


def _format_ratio(value: Decimal | None, *, places: int = 2) -> str:
    if value is None:
        return "-"
    return f"{format_decimal_fixed(value * Decimal('100'), places)}%"


def _format_margin_mode(value: str | None) -> str:
    text = (value or "").strip().lower()
    if not text:
        return "-"
    if text == "isolated":
        return "逐仓 isolated"
    if text == "cross":
        return "全仓 cross"
    return text


def _format_account_level(value: str | None) -> str:
    mapping = {
        "1": "简单交易",
        "2": "单币种保证金",
        "3": "跨币种保证金",
        "4": "组合保证金",
    }
    text = (value or "").strip()
    if not text:
        return "-"
    return mapping.get(text, text)


def _format_account_position_mode(value: str | None) -> str:
    text = (value or "").strip().lower()
    if not text:
        return "-"
    if text == "net":
        return "净持仓 net"
    if text in {"long_short", "long/short"}:
        return "双向持仓 long/short"
    return text


def _format_greeks_type(value: str | None) -> str:
    text = (value or "").strip().upper()
    if not text:
        return "-"
    if text in {"PA", "BS"}:
        return text
    return text


def _margin_mode_tag(value: str | None) -> str | None:
    text = (value or "").strip().lower()
    if text == "isolated":
        return "isolated_mode"
    if text == "cross":
        return "cross_mode"
    return None


def _format_pos_side(pos_side: str, position: Decimal) -> str:
    if pos_side and pos_side.lower() != "net":
        return pos_side
    if position > 0:
        return "long"
    if position < 0:
        return "short"
    return pos_side or "-"


def _normalize_symbol_input(raw: str) -> str:
    cleaned = raw.strip().upper()
    if not cleaned:
        return ""
    if "-" in cleaned:
        return cleaned
    if cleaned.endswith("USDT") and len(cleaned) > 4:
        return f"{cleaned[:-4]}-USDT-SWAP"
    return cleaned


def _extract_asset_key(inst_id: str) -> str:
    return inst_id.split("-")[0] if inst_id else "UNKNOWN"


def _extract_quote_key(inst_id: str) -> str | None:
    parts = inst_id.split("-")
    if len(parts) >= 2 and parts[1]:
        return parts[1].upper()
    return None


def _extract_history_expiry_prefix(inst_id: str) -> str:
    parts = inst_id.strip().upper().split("-")
    if len(parts) >= 3 and re.fullmatch(r"\d{6,8}", parts[2] or ""):
        return parts[2]
    return ""


def _extract_history_family(inst_id: str, inst_type: str) -> str | None:
    normalized = inst_id.strip().upper()
    if not normalized:
        return None
    if inst_type == "OPTION":
        return infer_option_family(normalized)
    parts = normalized.split("-")
    if inst_type in {"FUTURES", "SWAP"} and len(parts) >= 2:
        return f"{parts[0]}-{parts[1]}"
    return None


def _infer_history_inst_type(inst_id: str) -> str:
    normalized = inst_id.strip().upper()
    if not normalized:
        return "SPOT"
    if normalized.endswith("-SWAP"):
        return "SWAP"
    if infer_option_family(normalized):
        return "OPTION"
    parts = normalized.split("-")
    if len(parts) == 3 and re.fullmatch(r"\d{6,8}", parts[2] or ""):
        return "FUTURES"
    return infer_inst_type(normalized)


def _extract_bucket_key(position: OkxPosition) -> str:
    parts = position.inst_id.split("-")
    if position.inst_type == "OPTION" and len(parts) >= 3:
        return parts[2]
    if position.inst_type == "FUTURES" and len(parts) >= 3:
        return parts[2]
    return "__DIRECT__"


def _group_positions_for_tree(positions: list[OkxPosition]) -> dict[str, dict[str, list[OkxPosition]]]:
    grouped: dict[str, dict[str, list[OkxPosition]]] = {}
    for position in positions:
        asset_key = _extract_asset_key(position.inst_id)
        bucket_key = _extract_bucket_key(position)
        grouped.setdefault(asset_key, {}).setdefault(bucket_key, []).append(position)
    ordered: dict[str, dict[str, list[OkxPosition]]] = {}
    for asset_key, buckets in sorted(grouped.items(), key=lambda item: item[0]):
        ordered[asset_key] = dict(
            sorted(
                (
                    (
                        bucket_key,
                        sorted(bucket_positions, key=_position_bucket_sort_key),
                    )
                    for bucket_key, bucket_positions in buckets.items()
                ),
                key=lambda item: _bucket_sort_key(item[0]),
            )
        )
    return ordered


def _bucket_sort_key(bucket_key: str) -> tuple[int, int | str]:
    if bucket_key.isdigit():
        return (0, int(bucket_key))
    if bucket_key == "__DIRECT__":
        return (2, bucket_key)
    return (1, bucket_key)


def _position_bucket_sort_key(position: OkxPosition) -> tuple[int, int, int, str]:
    if position.inst_type == "OPTION":
        strike, option_side = _extract_option_sort_components(position.inst_id)
        option_side_rank = 0 if option_side == "C" else 1 if option_side == "P" else 2
        return (0, strike, option_side_rank, position.inst_id)
    if position.inst_type == "FUTURES":
        return (2, 0, 0, position.inst_id)
    return (1, 0, 0, position.inst_id)


def _extract_option_sort_components(inst_id: str) -> tuple[int, str]:
    parts = inst_id.split("-")
    if len(parts) >= 5:
        try:
            strike = int(parts[3])
        except ValueError:
            strike = 10**9
        return strike, parts[4].upper()
    return 10**9, ""


def _option_search_shortcuts(inst_id: str) -> tuple[str, str]:
    normalized = inst_id.strip().upper()
    if not normalized or infer_option_family(normalized) is None:
        return "", ""
    parts = normalized.split("-")
    if len(parts) < 3:
        return normalized, normalized
    return normalized, f"{parts[0]}-{parts[1]}-{parts[2]}-"


def _advance_fill_history_limit(current_limit: int, load_more_clicks: int) -> tuple[int, int, str]:
    increment = 100 if load_more_clicks == 0 else 200
    return current_limit + increment, load_more_clicks + 1, "增加200条"


def _aggregate_position_metrics(
    positions: list[OkxPosition],
    upl_usdt_prices: dict[str, Decimal],
    position_instruments: dict[str, Instrument],
) -> dict[str, Decimal | int | None]:
    def _sum_decimal(values: list[Decimal | None]) -> Decimal | None:
        decimals = [value for value in values if value is not None]
        if not decimals:
            return None
        return sum(decimals, Decimal("0"))

    pnl_currencies = sorted(
        {
            _infer_upl_currency(item)
            for item in positions
            if item.unrealized_pnl is not None or item.realized_pnl is not None
        }
    )
    pnl_currency: str | None = pnl_currencies[0] if len(pnl_currencies) == 1 else None
    return {
        "count": len(positions),
        "size_display": _format_group_position_size(positions, position_instruments),
        "option_side_display": _format_group_option_trade_side(positions, position_instruments),
        "upl": _sum_decimal([item.unrealized_pnl for item in positions]),
        "upl_usdt": _sum_decimal([_position_unrealized_pnl_usdt(item, upl_usdt_prices) for item in positions]),
        "market_value_usdt": _sum_decimal(
            [_position_market_value_usdt(item, position_instruments, upl_usdt_prices) for item in positions]
        ),
        "realized": _sum_decimal([item.realized_pnl for item in positions]),
        "realized_usdt": _sum_decimal([_position_realized_pnl_usdt(item, upl_usdt_prices) for item in positions]),
        "pnl_currency": pnl_currency,
        "imr": _sum_decimal([item.initial_margin for item in positions]),
        "mmr": _sum_decimal([item.maintenance_margin for item in positions]),
        "delta": _sum_decimal([_position_delta_value(item, position_instruments) for item in positions]),
        "gamma": _sum_decimal([item.gamma for item in positions]),
        "vega": _sum_decimal([item.vega for item in positions]),
        "theta": _sum_decimal([item.theta for item in positions]),
        "theta_usdt": _sum_decimal([_position_theta_usdt(item, upl_usdt_prices) for item in positions]),
        "open_value_usdt": _sum_decimal(
            [_position_signed_open_value_approx_usdt(item, position_instruments, upl_usdt_prices) for item in positions]
        ),
    }


def _build_group_row_values(group_type: str, metrics: dict[str, Decimal | int | None]) -> tuple[str, ...]:
    count = metrics["count"]
    pnl_places = _group_pnl_places(metrics.get("pnl_currency"))
    size_display = metrics.get("size_display")
    option_side_display = metrics.get("option_side_display")
    return (
        group_type,
        "--",
        "--",
        "--",
        "--",
        "--",
        "--",
        "--",
        "--",
        "--",
        "--",
        "--",
        "--",
        "--",
        _format_optional_approx_usdt(
            metrics["open_value_usdt"] if isinstance(metrics.get("open_value_usdt"), Decimal) else None
        ),
        (
            f"{count} 个持仓 | {size_display}"
            if isinstance(count, int) and isinstance(size_display, str) and size_display
            else (f"{count} 个持仓" if isinstance(count, int) else "--")
        ),
        option_side_display if isinstance(option_side_display, str) and option_side_display else "--",
        _format_optional_decimal_fixed(
            metrics["upl"] if isinstance(metrics["upl"], Decimal) else None,
            places=pnl_places,
            with_sign=True,
        ),
        _format_optional_usdt(metrics["upl_usdt"] if isinstance(metrics["upl_usdt"], Decimal) else None),
        _format_optional_decimal_fixed(
            metrics["realized"] if isinstance(metrics["realized"], Decimal) else None,
            places=pnl_places,
            with_sign=True,
        ),
        _format_optional_usdt(
            metrics.get("realized_usdt") if isinstance(metrics.get("realized_usdt"), Decimal) else None
        ),
        _format_optional_approx_usdt(
            metrics["market_value_usdt"] if isinstance(metrics["market_value_usdt"], Decimal) else None
        ),
        "--",
        "--",
        _format_optional_integer(metrics["imr"] if isinstance(metrics["imr"], Decimal) else None),
        _format_optional_integer(metrics["mmr"] if isinstance(metrics["mmr"], Decimal) else None),
        _format_optional_decimal_fixed(metrics["delta"] if isinstance(metrics["delta"], Decimal) else None, places=5),
        _format_optional_decimal_fixed(metrics["gamma"] if isinstance(metrics["gamma"], Decimal) else None, places=5),
        _format_optional_decimal_fixed(metrics["vega"] if isinstance(metrics["vega"], Decimal) else None, places=5),
        _format_optional_decimal_fixed(metrics["theta"] if isinstance(metrics["theta"], Decimal) else None, places=5),
        _format_optional_usdt_precise(
            metrics["theta_usdt"] if isinstance(metrics["theta_usdt"], Decimal) else None,
            places=2,
        ),
        "--",
    )


def _position_signed_display_amount(
    position: OkxPosition,
    position_instruments: dict[str, Instrument],
) -> tuple[Decimal | None, str | None]:
    if position.position == 0:
        return None, None

    direction = _format_pos_side(position.pos_side, position.position)
    sign = Decimal("-1") if direction == "short" else Decimal("1")
    instrument = position_instruments.get(position.inst_id)

    if instrument is not None and instrument.ct_val is not None and instrument.ct_val > 0 and instrument.ct_val_ccy:
        multiplier = instrument.ct_mult if instrument.ct_mult is not None and instrument.ct_mult > 0 else Decimal("1")
        quote_currency = instrument.ct_val_ccy.upper()
        if quote_currency in {"USD", "USDT", "USDC"} and position.inst_type in {"FUTURES", "SWAP"}:
            reference_price = position.mark_price or position.last_price or position.avg_price
            base_currency = _extract_asset_key(position.inst_id).upper()
            if reference_price is not None and reference_price > 0 and base_currency:
                amount = (abs(position.position) * instrument.ct_val * multiplier / reference_price) * sign
                return amount, base_currency
        amount = abs(position.position) * instrument.ct_val * multiplier * sign
        return amount, quote_currency

    asset_currency = _extract_asset_key(position.inst_id).upper()
    return abs(position.position) * sign, asset_currency if asset_currency else None


def _format_group_position_size(
    positions: list[OkxPosition],
    position_instruments: dict[str, Instrument],
) -> str:
    totals: dict[str, Decimal] = {}
    for position in positions:
        amount, currency = _position_signed_display_amount(position, position_instruments)
        if amount is None or not currency:
            continue
        totals[currency] = totals.get(currency, Decimal("0")) + amount

    if not totals:
        return ""

    parts: list[str] = []
    for currency in sorted(totals.keys()):
        amount = totals[currency]
        parts.append(f"{format_decimal_fixed(amount, 2)} {currency}")
    return " / ".join(parts)


def _extract_option_kind(inst_id: str) -> str | None:
    parts = inst_id.split("-")
    if not parts:
        return None
    suffix = parts[-1].strip().upper()
    return suffix if suffix in {"C", "P"} else None


def _format_option_trade_side(position: OkxPosition) -> str:
    if position.inst_type != "OPTION":
        return "-"
    option_kind = _extract_option_kind(position.inst_id)
    direction = _format_pos_side(position.pos_side, position.position)
    if option_kind == "C":
        if direction == "long":
            return "买购"
        if direction == "short":
            return "卖购"
    if option_kind == "P":
        if direction == "long":
            return "买沽"
        if direction == "short":
            return "卖沽"
    return "-"


def _format_option_trade_side_display(position: OkxPosition) -> str:
    return _format_option_trade_side(position)


def _format_group_option_trade_side(
    positions: list[OkxPosition],
    position_instruments: dict[str, Instrument],
) -> str:
    totals: dict[str, Decimal] = {}
    ordered_labels = ("买购", "卖购", "买沽", "卖沽")
    for position in positions:
        label = _format_option_trade_side(position)
        if label == "-":
            continue
        amount, currency = _position_signed_display_amount(position, position_instruments)
        if amount is None or not currency:
            continue
        totals[label] = totals.get(label, Decimal("0")) + abs(amount)

    def _slot_text(label: str) -> str:
        if label not in totals:
            return "-"
        amount = totals[label]
        return format_decimal_fixed(amount, 2)

    return (
        f"{_slot_text('买购')} : {_slot_text('卖购')} | "
        f"{_slot_text('买沽')} : {_slot_text('卖沽')}"
    )


def _format_filtered_option_position_size(
    positions: list[OkxPosition],
    position_instruments: dict[str, Instrument],
    *,
    option_kind: str,
    direction: str,
) -> str:
    totals: dict[str, Decimal] = {}
    for position in positions:
        if position.inst_type != "OPTION":
            continue
        if _extract_option_kind(position.inst_id) != option_kind:
            continue
        current_direction = _format_pos_side(position.pos_side, position.position)
        if current_direction != direction:
            continue
        amount, currency = _position_signed_display_amount(position, position_instruments)
        if amount is None or not currency:
            continue
        totals[currency] = totals.get(currency, Decimal("0")) + abs(amount)

    if not totals:
        return "-"

    parts: list[str] = []
    for currency in sorted(totals.keys()):
        parts.append(f"{format_decimal_fixed(totals[currency], 2)} {currency}")
    return " / ".join(parts)


def _format_position_size(position: OkxPosition, position_instruments: dict[str, Instrument]) -> str:
    if position.position == 0:
        return "-"

    direction = _format_pos_side(position.pos_side, position.position)
    amount, currency = _position_signed_display_amount(position, position_instruments)
    if amount is None:
        return "-"
    if currency and currency not in {"USD", "USDT", "USDC"} and position.inst_type in {"FUTURES", "SWAP"}:
        return f"{format_decimal_fixed(amount, 4)} {currency} ({direction})"
    if currency:
        return f"{format_decimal(amount)} {currency} ({direction})"
    return f"{format_decimal(amount)} ({direction})"


def _position_delta_value(
    position: OkxPosition,
    position_instruments: dict[str, Instrument],
) -> Decimal | None:
    if position.inst_type == "OPTION":
        return position.delta

    amount, amount_currency = _position_contract_amount(position, position_instruments)
    direction = _format_pos_side(position.pos_side, position.position)
    sign = Decimal("-1") if direction == "short" else Decimal("1")
    if amount is not None and amount_currency:
        if amount_currency in {"USD", "USDT", "USDC"}:
            reference_price = position.mark_price or position.last_price or position.avg_price
            base_currency = _extract_asset_key(position.inst_id).upper()
            if reference_price is not None and reference_price > 0 and base_currency:
                return (amount / reference_price) * sign
        return amount * sign

    if position.inst_type == "SPOT":
        return abs(position.position) * sign
    return position.delta


def _position_theta_usdt(position: OkxPosition, upl_usdt_prices: dict[str, Decimal]) -> Decimal | None:
    if position.theta is None:
        return None
    currency = _infer_upl_currency(position)
    if currency in {"USDT", "USD", "USDC"}:
        return position.theta
    price = upl_usdt_prices.get(currency)
    if price is None:
        return None
    return position.theta * price


def _position_option_intrinsic_value(position: OkxPosition, upl_usdt_prices: dict[str, Decimal]) -> Decimal | None:
    if position.inst_type != "OPTION":
        return None
    asset_currency = _extract_asset_key(position.inst_id).upper()
    underlying_price = upl_usdt_prices.get(asset_currency)
    if underlying_price is None or underlying_price <= 0:
        return None
    strike, option_kind = _extract_option_sort_components(position.inst_id)
    strike_price = Decimal(str(strike))
    if option_kind == "C":
        intrinsic_usdt = max(underlying_price - strike_price, Decimal("0"))
    elif option_kind == "P":
        intrinsic_usdt = max(strike_price - underlying_price, Decimal("0"))
    else:
        return None

    payout_currency = _infer_upl_currency(position)
    if payout_currency in {"USDT", "USD", "USDC"}:
        return intrinsic_usdt
    return intrinsic_usdt / underlying_price


def _position_option_intrinsic_value_usdt(position: OkxPosition, upl_usdt_prices: dict[str, Decimal]) -> Decimal | None:
    intrinsic_value = _position_option_intrinsic_value(position, upl_usdt_prices)
    if intrinsic_value is None:
        return None
    payout_currency = _infer_upl_currency(position)
    if payout_currency in {"USDT", "USD", "USDC"}:
        return intrinsic_value
    asset_currency = _extract_asset_key(position.inst_id).upper()
    underlying_price = upl_usdt_prices.get(asset_currency)
    if underlying_price is None:
        return None
    return intrinsic_value * underlying_price


def _position_option_time_value(position: OkxPosition, upl_usdt_prices: dict[str, Decimal]) -> Decimal | None:
    if position.inst_type != "OPTION":
        return None
    mark_price = position.mark_price or position.last_price
    intrinsic_value = _position_option_intrinsic_value(position, upl_usdt_prices)
    if mark_price is None or intrinsic_value is None:
        return None
    return mark_price - intrinsic_value


def _position_option_time_value_usdt(position: OkxPosition, upl_usdt_prices: dict[str, Decimal]) -> Decimal | None:
    time_value = _position_option_time_value(position, upl_usdt_prices)
    if time_value is None:
        return None
    payout_currency = _infer_upl_currency(position)
    if payout_currency in {"USDT", "USD", "USDC"}:
        return time_value
    asset_currency = _extract_asset_key(position.inst_id).upper()
    underlying_price = upl_usdt_prices.get(asset_currency)
    if underlying_price is None:
        return None
    return time_value * underlying_price


def _format_option_price_component(value: Decimal | None, position: OkxPosition) -> str:
    if value is None:
        return "-"
    prefix = _mark_price_prefix(position)
    text = format_decimal_fixed(value, 4)
    return f"{prefix} {text}" if prefix else text


def _format_position_option_price_component(
    position: OkxPosition,
    upl_usdt_prices: dict[str, Decimal],
    *,
    component: str,
) -> str:
    if component == "time_value":
        return _format_option_price_component(_position_option_time_value(position, upl_usdt_prices), position)
    if component == "intrinsic_value":
        return _format_option_price_component(_position_option_intrinsic_value(position, upl_usdt_prices), position)
    return "-"


def _format_position_option_component_usdt(
    position: OkxPosition,
    upl_usdt_prices: dict[str, Decimal],
    *,
    component: str,
) -> str:
    if component == "time_value":
        value = _position_option_time_value_usdt(position, upl_usdt_prices)
    elif component == "intrinsic_value":
        value = _position_option_intrinsic_value_usdt(position, upl_usdt_prices)
    else:
        value = None
    return _format_optional_usdt_precise(value, places=2, with_sign=False)


def _position_mark_price_usdt(position: OkxPosition, upl_usdt_prices: dict[str, Decimal]) -> Decimal | None:
    mark_price = position.mark_price or position.last_price
    if mark_price is None:
        return None
    if position.inst_type == "OPTION":
        currency = _infer_upl_currency(position)
    else:
        currency = (_extract_quote_key(position.inst_id) or "").upper()
    if currency in {"USDT", "USD", "USDC"}:
        return mark_price
    price = upl_usdt_prices.get(currency)
    if price is None:
        return None
    return mark_price * price


def _format_mark_price(position: OkxPosition) -> str:
    mark_price = position.mark_price or position.last_price
    if mark_price is None:
        return "-"
    if position.inst_type == "OPTION":
        amount_text = format_decimal_fixed(mark_price, 4)
    else:
        amount_text = format_decimal(mark_price)
    prefix = _mark_price_prefix(position)
    return f"{prefix} {amount_text}" if prefix else amount_text


def _format_position_quote_price(
    position: OkxPosition,
    position_instruments: dict[str, Instrument],
    position_tickers: dict[str, OkxTicker],
    *,
    side: str,
) -> str:
    ticker = position_tickers.get(position.inst_id)
    if ticker is None:
        return "-"
    if side == "bid":
        quote_price = ticker.bid
    elif side == "ask":
        quote_price = ticker.ask
    else:
        quote_price = None
    if quote_price is None:
        return "-"
    prefix = _mark_price_prefix(position)
    instrument = position_instruments.get(position.inst_id)
    places = _tick_size_places(instrument.tick_size) if instrument is not None else None
    if position.inst_type == "OPTION":
        if places is None:
            places = 4
        amount_text = format_decimal_fixed(quote_price, places)
    elif position.inst_type not in {"FUTURES", "SWAP"}:
        amount_text = _format_optional_decimal(quote_price)
    elif places is None:
        amount_text = _format_optional_decimal(quote_price)
    else:
        amount_text = format_decimal_fixed(quote_price, places)
    return f"{prefix} {amount_text}" if prefix and amount_text != "-" else amount_text


def _position_quote_price_usdt(
    position: OkxPosition,
    position_tickers: dict[str, OkxTicker],
    upl_usdt_prices: dict[str, Decimal],
    *,
    side: str,
) -> Decimal | None:
    ticker = position_tickers.get(position.inst_id)
    if ticker is None:
        return None
    if side == "bid":
        quote_price = ticker.bid
    elif side == "ask":
        quote_price = ticker.ask
    else:
        quote_price = None
    if quote_price is None:
        return None
    if position.inst_type == "OPTION":
        currency = _infer_upl_currency(position)
    else:
        currency = (_extract_quote_key(position.inst_id) or "").upper()
    if currency in {"USDT", "USD", "USDC"}:
        return quote_price
    price = upl_usdt_prices.get(currency)
    if price is None:
        return None
    return quote_price * price


def _format_position_quote_price_usdt(
    position: OkxPosition,
    position_tickers: dict[str, OkxTicker],
    upl_usdt_prices: dict[str, Decimal],
    *,
    side: str,
) -> str:
    return _format_optional_usdt(_position_quote_price_usdt(position, position_tickers, upl_usdt_prices, side=side), with_sign=False)


def _mark_price_prefix(position: OkxPosition) -> str:
    if position.inst_type == "OPTION":
        currency = _extract_asset_key(position.inst_id).upper()
    else:
        currency = (_extract_quote_key(position.inst_id) or "").upper()
    if currency in {"USD", "USDT", "USDC"}:
        return "$"
    if currency == "BTC":
        return "B"
    if currency == "ETH":
        return "E"
    return currency[:1] if currency else ""


def _tick_size_places(tick_size: Decimal | None) -> int | None:
    if tick_size is None or tick_size <= 0:
        return None
    normalized = tick_size.normalize()
    exponent = normalized.as_tuple().exponent
    return max(0, -exponent)


def _format_position_avg_price(position: OkxPosition, position_instruments: dict[str, Instrument]) -> str:
    if position.avg_price is None:
        return "-"
    prefix = _mark_price_prefix(position)
    instrument = position_instruments.get(position.inst_id)
    places = _tick_size_places(instrument.tick_size) if instrument is not None else None
    if position.inst_type == "OPTION":
        if places is None:
            places = 4
        amount_text = format_decimal_fixed(position.avg_price, places)
        return f"{prefix} {amount_text}" if prefix and amount_text != "-" else amount_text
    if position.inst_type not in {"FUTURES", "SWAP"}:
        amount_text = _format_optional_decimal(position.avg_price)
        return f"{prefix} {amount_text}" if prefix and amount_text != "-" else amount_text
    if places is None:
        amount_text = _format_optional_decimal(position.avg_price)
    else:
        amount_text = format_decimal_fixed(position.avg_price, places)
    return f"{prefix} {amount_text}" if prefix and amount_text != "-" else amount_text


def _position_avg_price_usdt(position: OkxPosition, upl_usdt_prices: dict[str, Decimal]) -> Decimal | None:
    if position.inst_type != "OPTION" or position.avg_price is None:
        return None
    currency = _infer_upl_currency(position)
    if currency in {"USDT", "USD", "USDC"}:
        return position.avg_price
    price = upl_usdt_prices.get(currency)
    if price is None:
        return None
    return position.avg_price * price


def _format_position_avg_price_usdt(position: OkxPosition, upl_usdt_prices: dict[str, Decimal]) -> str:
    return _format_optional_usdt(_position_avg_price_usdt(position, upl_usdt_prices), with_sign=False)


def _format_position_mark_price_usdt(position: OkxPosition, upl_usdt_prices: dict[str, Decimal]) -> str:
    return _format_optional_usdt(_position_mark_price_usdt(position, upl_usdt_prices), with_sign=False)


def _infer_upl_currency(position: OkxPosition) -> str:
    if position.margin_ccy:
        return position.margin_ccy.strip().upper()
    return _extract_asset_key(position.inst_id).upper()


def _group_pnl_places(currency: object) -> int:
    text = str(currency).strip().upper() if currency is not None else ""
    if text in {"USDT", "USD", "USDC"}:
        return 2
    return 5


def _position_contract_amount(
    position: OkxPosition,
    position_instruments: dict[str, Instrument],
) -> tuple[Decimal | None, str | None]:
    instrument = position_instruments.get(position.inst_id)
    if instrument is not None and instrument.ct_val is not None and instrument.ct_val > 0 and instrument.ct_val_ccy:
        multiplier = instrument.ct_mult if instrument.ct_mult is not None and instrument.ct_mult > 0 else Decimal("1")
        amount = abs(position.position) * instrument.ct_val * multiplier
        return amount, instrument.ct_val_ccy.upper()
    if position.inst_type == "SPOT":
        return abs(position.position), _extract_asset_key(position.inst_id).upper()
    return None, None


def _position_market_value_native(
    position: OkxPosition,
    position_instruments: dict[str, Instrument],
) -> tuple[Decimal | None, str | None]:
    mark_price = position.mark_price or position.last_price
    amount, amount_currency = _position_contract_amount(position, position_instruments)
    if amount is None or amount_currency is None:
        return None, None

    if position.inst_type == "OPTION":
        if mark_price is None:
            return None, None
        return amount * mark_price, amount_currency

    if amount_currency in {"USD", "USDT", "USDC"}:
        return amount, amount_currency

    if mark_price is None:
        return None, None

    quote_currency = _extract_quote_key(position.inst_id)
    if quote_currency is None:
        return None, None
    return amount * mark_price, quote_currency


def _position_signed_open_value_approx_usdt(
    position: OkxPosition,
    position_instruments: dict[str, Instrument],
    upl_usdt_prices: dict[str, Decimal],
) -> Decimal | None:
    """期权、永续/期货：开仓价 × 持仓量（与持仓列同源的 signed 数量，空为负）；折 USDT 供 ≈USDT 列展示。"""
    if position.position == 0:
        return None
    if position.inst_type not in {"OPTION", "SWAP", "FUTURES"}:
        return None
    if position.avg_price is None:
        return None
    signed_amt, _ = _position_signed_display_amount(position, position_instruments)
    if signed_amt is None:
        return None
    raw = position.avg_price * signed_amt

    if position.inst_type in {"SWAP", "FUTURES"}:
        quote = (_extract_quote_key(position.inst_id) or "").upper()
        if quote in {"USDT", "USD", "USDC"}:
            return raw
        px = upl_usdt_prices.get(quote)
        if px is None:
            return None
        return raw * px

    ccy = _infer_upl_currency(position)
    if ccy in {"USDT", "USD", "USDC"}:
        return raw
    px = upl_usdt_prices.get(ccy)
    if px is None:
        return None
    return raw * px


def _position_market_value_usdt(
    position: OkxPosition,
    position_instruments: dict[str, Instrument],
    upl_usdt_prices: dict[str, Decimal],
) -> Decimal | None:
    native_value, native_currency = _position_market_value_native(position, position_instruments)
    if native_value is None or native_currency is None:
        return None
    if native_currency in {"USDT", "USD", "USDC"}:
        return native_value
    conversion = upl_usdt_prices.get(native_currency)
    if conversion is None:
        return None
    return native_value * conversion


def _format_position_market_value(
    position: OkxPosition,
    position_instruments: dict[str, Instrument],
    upl_usdt_prices: dict[str, Decimal],
) -> str:
    native_value, native_currency = _position_market_value_native(position, position_instruments)
    if native_value is None or native_currency is None:
        return "-"
    native_text = f"{format_decimal_fixed(native_value, 5)} {native_currency}"
    if native_currency in {"USDT", "USD", "USDC"}:
        return native_text
    usdt_value = _position_market_value_usdt(position, position_instruments, upl_usdt_prices)
    if usdt_value is None:
        return native_text
    return f"{native_text} ({_format_optional_approx_usdt(usdt_value)})"


def _format_position_unrealized_pnl(position: OkxPosition) -> str:
    if position.unrealized_pnl is None:
        return "-"
    places = 2 if position.inst_type in {"FUTURES", "SWAP"} else 8
    amount_text = _format_optional_decimal_fixed(position.unrealized_pnl, places=places, with_sign=True)
    currency = _infer_upl_currency(position)
    if currency:
        amount_text = f"{amount_text} {currency}"
    if position.unrealized_pnl_ratio is not None:
        amount_text = f"{amount_text}（{_format_ratio(position.unrealized_pnl_ratio, places=2)}）"
    return amount_text


def _position_unrealized_pnl_usdt(position: OkxPosition, upl_usdt_prices: dict[str, Decimal]) -> Decimal | None:
    if position.unrealized_pnl is None:
        return None
    currency = _infer_upl_currency(position)
    price = upl_usdt_prices.get(currency)
    if price is None:
        return None
    return position.unrealized_pnl * price


def _position_realized_pnl_usdt(position: OkxPosition, upl_usdt_prices: dict[str, Decimal]) -> Decimal | None:
    if position.realized_pnl is None:
        return None
    currency = _infer_upl_currency(position)
    price = upl_usdt_prices.get(currency)
    if price is None:
        return None
    return position.realized_pnl * price


def _session_trade_inst_id(session: StrategySession) -> str:
    config = getattr(session, "config", None)
    trade_inst_id = getattr(config, "trade_inst_id", None) or getattr(config, "inst_id", None) or getattr(session, "symbol", "")
    return str(trade_inst_id or "").strip().upper()


def _session_expected_position_sides(session: StrategySession) -> tuple[str, ...]:
    signal_mode = str(getattr(getattr(session, "config", None), "signal_mode", "") or "").strip().lower()
    if signal_mode == "long_only":
        return ("long",)
    if signal_mode == "short_only":
        return ("short",)
    return ("long", "short")


def _position_matches_session_live_pnl(
    position: OkxPosition,
    *,
    trade_inst_id: str,
    expected_sides: tuple[str, ...],
) -> bool:
    if position.inst_id.strip().upper() != trade_inst_id:
        return False
    derived_side = _format_pos_side(position.pos_side, position.position).strip().lower()
    return derived_side in expected_sides


def _build_upl_usdt_price_map(client: OkxRestClient, positions: list[OkxPosition]) -> dict[str, Decimal]:
    currencies = {_infer_upl_currency(position) for position in positions if position.unrealized_pnl is not None}
    return _build_usdt_price_snapshot(client, currencies)


def _build_position_history_usdt_price_map(
    client: OkxRestClient,
    items: list[OkxPositionHistoryItem],
) -> dict[str, Decimal]:
    currencies: set[str] = set()
    for item in items:
        if item.realized_pnl is not None:
            currencies.add(_infer_position_history_pnl_currency(item))
        if item.pnl is not None:
            currencies.add(_infer_position_history_pnl_currency(item))
        if item.fee is not None and item.fee_currency:
            currencies.add(item.fee_currency.strip().upper())
    return _build_usdt_price_snapshot(client, currencies)


def _build_usdt_price_snapshot(client: OkxRestClient, currencies: set[str]) -> dict[str, Decimal]:
    prices: dict[str, Decimal] = {}
    for currency in sorted(currencies):
        if currency in {"USDT", "USD"}:
            prices[currency] = Decimal("1")
            continue
        if currency == "USDC":
            prices[currency] = Decimal("1")
            continue
        inst_id = f"{currency}-USDT"
        try:
            ticker = client.get_ticker(inst_id)
        except Exception:
            continue
        spot_price = ticker.last or ticker.bid or ticker.ask or ticker.mark or ticker.index
        if spot_price is not None and spot_price > 0:
            prices[currency] = spot_price
    return prices


def _build_position_instrument_map(client: OkxRestClient, positions: list[OkxPosition]) -> dict[str, Instrument]:
    needed_ids = {position.inst_id for position in positions}
    result: dict[str, Instrument] = {}

    option_families = sorted(
        {
            infer_option_family(position.inst_id)
            for position in positions
            if position.inst_type == "OPTION" and infer_option_family(position.inst_id)
        }
    )
    for family in option_families:
        for instrument in client.get_option_instruments(inst_family=family):
            if instrument.inst_id in needed_ids:
                result[instrument.inst_id] = instrument

    swap_ids = {position.inst_id for position in positions if position.inst_type == "SWAP"}
    if swap_ids:
        for instrument in client.get_swap_instruments():
            if instrument.inst_id in swap_ids:
                result[instrument.inst_id] = instrument

    futures_ids = {position.inst_id for position in positions if position.inst_type == "FUTURES"}
    if futures_ids:
        for instrument in client.get_instruments("FUTURES"):
            if instrument.inst_id in futures_ids:
                result[instrument.inst_id] = instrument

    return result


def _build_position_ticker_map(client: OkxRestClient, positions: list[OkxPosition]) -> dict[str, OkxTicker]:
    result: dict[str, OkxTicker] = {}
    for inst_id in sorted({position.inst_id for position in positions}):
        try:
            result[inst_id] = client.get_ticker(inst_id)
        except Exception:
            continue
    return result


def _now_epoch_ms() -> int:
    return int(datetime.now().timestamp() * 1000)


def _normalize_position_note_text(value: object) -> str:
    if value is None:
        return ""
    lines = str(value).replace("\r\n", "\n").replace("\r", "\n").split("\n")
    while lines and not lines[0].strip():
        lines.pop(0)
    while lines and not lines[-1].strip():
        lines.pop()
    return "\n".join(line.rstrip() for line in lines)


def _format_position_note_summary(note: str, *, limit: int = 24) -> str:
    normalized_note = _normalize_position_note_text(note)
    if not normalized_note:
        return "-"
    single_line = " | ".join(part.strip() for part in normalized_note.splitlines() if part.strip())
    if len(single_line) <= limit:
        return single_line
    return f"{single_line[: max(limit - 1, 1)].rstrip()}…"


def _format_position_note_detail(note: str) -> str:
    normalized_note = _normalize_position_note_text(note)
    if not normalized_note:
        return ""
    lines = normalized_note.splitlines()
    if len(lines) == 1:
        return f"备注：{lines[0]}\n"
    formatted_lines = [f"备注：{lines[0]}"]
    formatted_lines.extend(f"      {line}" for line in lines[1:])
    return "\n".join(formatted_lines) + "\n"


def _normalize_position_note_side(value: str | None) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"buy", "long"}:
        return "long"
    if normalized in {"sell", "short"}:
        return "short"
    if not normalized or normalized == "net":
        return "net"
    return normalized


def _normalize_position_note_margin_mode(value: str | None) -> str:
    normalized = str(value or "").strip().lower()
    return normalized or "unknown"


def _position_note_current_record_key(
    profile_name: str,
    environment: str,
    inst_id: str,
    pos_side: str | None,
    mgn_mode: str | None,
) -> str:
    return "|".join(
        (
            profile_name.strip(),
            environment.strip().lower(),
            inst_id.strip().upper(),
            _normalize_position_note_side(pos_side),
            _normalize_position_note_margin_mode(mgn_mode),
        )
    )


def _position_note_current_key(profile_name: str, environment: str, position: OkxPosition) -> str:
    return _position_note_current_record_key(
        profile_name,
        environment,
        position.inst_id,
        position.pos_side,
        position.mgn_mode,
    )


def _position_history_note_key(profile_name: str, environment: str, item: OkxPositionHistoryItem) -> str:
    side = _normalize_position_note_side(item.pos_side or item.direction)
    return "|".join(
        (
            profile_name.strip(),
            environment.strip().lower(),
            str(item.update_time or 0),
            item.inst_id.strip().upper(),
            _normalize_position_note_margin_mode(item.mgn_mode),
            side,
            str(item.close_size) if item.close_size is not None else "",
            str(item.close_avg_price) if item.close_avg_price is not None else "",
        )
    )


def _build_current_position_note_record(
    *,
    profile_name: str,
    environment: str,
    position: OkxPosition,
    note: str,
    now_ms: int,
    previous: dict[str, object] | None = None,
) -> dict[str, object] | None:
    normalized_note = _normalize_position_note_text(note)
    if not normalized_note:
        return None
    linked_history_keys = previous.get("linked_history_keys", []) if isinstance(previous, dict) else []
    linked_history_values = (
        [str(value).strip() for value in linked_history_keys if str(value).strip()]
        if isinstance(linked_history_keys, list)
        else []
    )
    return {
        "record_key": _position_note_current_key(profile_name, environment, position),
        "profile_name": profile_name.strip(),
        "environment": environment.strip().lower(),
        "inst_id": position.inst_id.strip().upper(),
        "pos_side": _normalize_position_note_side(position.pos_side),
        "mgn_mode": _normalize_position_note_margin_mode(position.mgn_mode),
        "note": normalized_note,
        "activated_at_ms": now_ms,
        "updated_at_ms": now_ms,
        "missing_success_count": 0,
        "missing_started_at_ms": None,
        "linked_history_keys": linked_history_values,
    }


def _build_history_position_note_record(
    *,
    profile_name: str,
    environment: str,
    item: OkxPositionHistoryItem,
    note: str,
    now_ms: int,
    source_current_key: str = "",
    previous: dict[str, object] | None = None,
) -> dict[str, object] | None:
    normalized_note = _normalize_position_note_text(note)
    if not normalized_note:
        return None
    record = {
        "record_key": _position_history_note_key(profile_name, environment, item),
        "profile_name": profile_name.strip(),
        "environment": environment.strip().lower(),
        "inst_id": item.inst_id.strip().upper(),
        "update_time": item.update_time,
        "mgn_mode": _normalize_position_note_margin_mode(item.mgn_mode),
        "pos_side": _normalize_position_note_side(item.pos_side),
        "direction": _normalize_position_note_side(item.direction),
        "close_size": str(item.close_size) if item.close_size is not None else "",
        "close_avg_price": str(item.close_avg_price) if item.close_avg_price is not None else "",
        "note": normalized_note,
        "source_current_key": source_current_key.strip(),
        "updated_at_ms": now_ms,
    }
    if isinstance(previous, dict):
        previous_source = str(previous.get("source_current_key", "")).strip()
        if previous_source and not record["source_current_key"]:
            record["source_current_key"] = previous_source
    return record


def _history_note_record_time_ms(record: dict[str, object]) -> int | None:
    update_time = record.get("update_time")
    if isinstance(update_time, int):
        return update_time
    if isinstance(update_time, str) and update_time.strip().isdigit():
        return int(update_time.strip())
    updated_at_ms = record.get("updated_at_ms")
    if isinstance(updated_at_ms, int):
        return updated_at_ms
    if isinstance(updated_at_ms, str) and updated_at_ms.strip().isdigit():
        return int(updated_at_ms.strip())
    return None


def _find_current_position_note_for_history_item(
    current_notes: dict[str, dict[str, object]],
    *,
    profile_name: str,
    environment: str,
    item: OkxPositionHistoryItem,
) -> dict[str, object] | None:
    exact_key = _position_note_current_record_key(
        profile_name,
        environment,
        item.inst_id,
        item.pos_side or item.direction,
        item.mgn_mode,
    )
    candidate = current_notes.get(exact_key)
    if candidate is not None:
        activated_at_ms = candidate.get("activated_at_ms")
        if not isinstance(activated_at_ms, int) or item.update_time is None or item.update_time >= activated_at_ms:
            return candidate
    if item.mgn_mode:
        return None
    matching_candidates = [
        record
        for record in current_notes.values()
        if str(record.get("profile_name", "")).strip() == profile_name.strip()
        and str(record.get("environment", "")).strip().lower() == environment.strip().lower()
        and str(record.get("inst_id", "")).strip().upper() == item.inst_id.strip().upper()
        and _normalize_position_note_side(str(record.get("pos_side", "")))
        == _normalize_position_note_side(item.pos_side or item.direction)
    ]
    if len(matching_candidates) != 1:
        return None
    candidate = matching_candidates[0]
    activated_at_ms = candidate.get("activated_at_ms")
    if not isinstance(activated_at_ms, int) or item.update_time is None or item.update_time >= activated_at_ms:
        return candidate
    return None


def _reconcile_current_position_note_records(
    current_notes: dict[str, dict[str, object]],
    *,
    profile_name: str,
    environment: str,
    positions: list[OkxPosition],
    now_ms: int,
) -> bool:
    visible_keys = {
        _position_note_current_key(profile_name, environment, position)
        for position in positions
    }
    changed = False
    for record in current_notes.values():
        if str(record.get("profile_name", "")).strip() != profile_name.strip():
            continue
        if str(record.get("environment", "")).strip().lower() != environment.strip().lower():
            continue
        record_key = str(record.get("record_key", "")).strip()
        if not record_key:
            continue
        if record_key in visible_keys:
            if int(record.get("missing_success_count", 0) or 0) != 0 or record.get("missing_started_at_ms") is not None:
                record["missing_success_count"] = 0
                record["missing_started_at_ms"] = None
                record["updated_at_ms"] = now_ms
                changed = True
            continue
        previous_count = int(record.get("missing_success_count", 0) or 0)
        record["missing_success_count"] = previous_count + 1
        if previous_count == 0 or record.get("missing_started_at_ms") is None:
            record["missing_started_at_ms"] = now_ms
        record["updated_at_ms"] = now_ms
        changed = True
    return changed


def _inherit_position_history_notes(
    current_notes: dict[str, dict[str, object]],
    history_notes: dict[str, dict[str, object]],
    *,
    profile_name: str,
    environment: str,
    position_history: list[OkxPositionHistoryItem],
    now_ms: int,
) -> bool:
    changed = False
    for item in position_history:
        history_key = _position_history_note_key(profile_name, environment, item)
        if history_key in history_notes:
            continue
        current_record = _find_current_position_note_for_history_item(
            current_notes,
            profile_name=profile_name,
            environment=environment,
            item=item,
        )
        if current_record is None:
            continue
        history_record = _build_history_position_note_record(
            profile_name=profile_name,
            environment=environment,
            item=item,
            note=str(current_record.get("note", "")),
            now_ms=now_ms,
            source_current_key=str(current_record.get("record_key", "")),
        )
        if history_record is None:
            continue
        history_notes[history_key] = history_record
        linked_history_keys = current_record.get("linked_history_keys", [])
        if not isinstance(linked_history_keys, list):
            linked_history_keys = []
        if history_key not in linked_history_keys:
            linked_history_keys.append(history_key)
            current_record["linked_history_keys"] = linked_history_keys
        current_record["updated_at_ms"] = now_ms
        changed = True
    return changed


def _prune_closed_current_position_notes(
    current_notes: dict[str, dict[str, object]],
    history_notes: dict[str, dict[str, object]],
    *,
    profile_name: str,
    environment: str,
) -> bool:
    changed = False
    for record_key, record in list(current_notes.items()):
        if str(record.get("profile_name", "")).strip() != profile_name.strip():
            continue
        if str(record.get("environment", "")).strip().lower() != environment.strip().lower():
            continue
        if int(record.get("missing_success_count", 0) or 0) < 2:
            continue
        missing_started_at_ms = record.get("missing_started_at_ms")
        if not isinstance(missing_started_at_ms, int):
            continue
        linked_history_keys = record.get("linked_history_keys", [])
        if not isinstance(linked_history_keys, list):
            continue
        has_history_after_missing = False
        for history_key in linked_history_keys:
            history_record = history_notes.get(str(history_key))
            if history_record is None:
                continue
            history_time_ms = _history_note_record_time_ms(history_record)
            if history_time_ms is not None and history_time_ms >= missing_started_at_ms:
                has_history_after_missing = True
                break
        if not has_history_after_missing:
            continue
        del current_notes[record_key]
        changed = True
    return changed


def _filter_positions(
    positions: list[OkxPosition],
    *,
    inst_type: str,
    keyword: str,
    note_texts: dict[str, str] | None = None,
) -> list[OkxPosition]:
    normalized_keyword = keyword.strip().lower()
    results: list[OkxPosition] = []
    for position in positions:
        if inst_type and position.inst_type != inst_type:
            continue
        if normalized_keyword:
            haystack = " ".join(
                part.lower()
                for part in (
                    position.inst_id,
                    position.inst_type,
                    position.pos_side,
                    position.mgn_mode,
                    _extract_asset_key(position.inst_id),
                    note_texts.get(_position_tree_row_id(position), "") if note_texts else "",
                )
                if part
            )
            if normalized_keyword not in haystack:
                continue
        results.append(position)
    return results


def _format_position_filter_summary(type_label: str, keyword: str) -> str:
    parts: list[str] = []
    if type_label and type_label != "全部类型":
        parts.append(type_label)
    if keyword.strip():
        parts.append(keyword.strip().upper())
    return " + ".join(parts)


def _build_position_detail_text(
    position: OkxPosition,
    upl_usdt_prices: dict[str, Decimal],
    position_instruments: dict[str, Instrument],
    note: str = "",
) -> str:
    delta_value = _position_delta_value(position, position_instruments)
    note_line = _format_position_note_detail(note)
    return (
        f"合约：{position.inst_id}\n"
        f"类型：{position.inst_type}\n"
        f"方向：{_format_pos_side(position.pos_side, position.position)}\n"
        f"持仓量：{_format_position_size(position, position_instruments)}\n"
        f"{note_line}"
        f"可平数量：{_format_optional_decimal(position.avail_position)}\n"
        f"保证金模式：{position.mgn_mode or '-'}\n"
        f"杠杆：{_format_optional_decimal(position.leverage)}\n"
        f"开仓价 / 开仓≈USDT：{_format_position_avg_price(position, position_instruments)} / "
        f"{_format_position_avg_price_usdt(position, upl_usdt_prices)}\n"
        f"标记价 / 标记≈USDT：{_format_mark_price(position)} / "
        f"{_format_position_mark_price_usdt(position, upl_usdt_prices)}\n"
        f"时间价值 / 时间≈USDT："
        f"{_format_position_option_price_component(position, upl_usdt_prices, component='time_value')} / "
        f"{_format_position_option_component_usdt(position, upl_usdt_prices, component='time_value')}\n"
        f"内在价值 / 内在≈USDT："
        f"{_format_position_option_price_component(position, upl_usdt_prices, component='intrinsic_value')} / "
        f"{_format_position_option_component_usdt(position, upl_usdt_prices, component='intrinsic_value')}\n"
        f"市值：{_format_position_market_value(position, position_instruments, upl_usdt_prices)}\n"
        f"最新价：{_format_optional_decimal(position.last_price)}\n"
        f"浮盈亏 / 浮盈≈USDT：{_format_position_unrealized_pnl(position)} / "
        f"{_format_optional_usdt(_position_unrealized_pnl_usdt(position, upl_usdt_prices))}\n"
        f"已实现盈亏 / 已实现≈USDT："
        f"{_format_optional_decimal_fixed(position.realized_pnl, places=5, with_sign=True)} / "
        f"{_format_optional_usdt(_position_realized_pnl_usdt(position, upl_usdt_prices))}\n"
        f"强平价：{_format_optional_decimal(position.liquidation_price)}\n"
        f"保证金币种：{position.margin_ccy or '-'}\n"
        f"保证金率：{_format_ratio(position.margin_ratio, places=2)}\n"
        f"初始保证金(IMR)：{_format_optional_integer(position.initial_margin)}\n"
        f"维持保证金：{_format_optional_integer(position.maintenance_margin)}\n"
        f"Delta / Gamma(PA) / Vega(PA) / Theta(PA) / Theta≈USDT："
        f"{_format_optional_decimal_fixed(delta_value, places=5)} / "
        f"{_format_optional_decimal_fixed(position.gamma, places=5)} / "
        f"{_format_optional_decimal_fixed(position.vega, places=5)} / "
        f"{_format_optional_decimal_fixed(position.theta, places=5)} / "
        f"{_format_optional_usdt_precise(_position_theta_usdt(position, upl_usdt_prices), places=2)}"
    )


def _build_group_detail_text(
    label: str,
    positions: list[OkxPosition],
    metrics: dict[str, Decimal | int | None],
    upl_usdt_prices: dict[str, Decimal],
    position_instruments: dict[str, Instrument],
) -> str:
    pnl_places = _group_pnl_places(metrics.get("pnl_currency"))
    lines = [
        f"分组：{label}",
        f"持仓笔数：{metrics['count']}",
        f"浮动盈亏：{_format_optional_decimal_fixed(metrics['upl'] if isinstance(metrics['upl'], Decimal) else None, places=pnl_places, with_sign=True)}",
        f"折合USDT：{_format_optional_usdt(metrics['upl_usdt'] if isinstance(metrics['upl_usdt'], Decimal) else None)}",
        f"市值：{_format_optional_approx_usdt(metrics['market_value_usdt'] if isinstance(metrics['market_value_usdt'], Decimal) else None)}",
        f"已实现盈亏：{_format_optional_decimal_fixed(metrics['realized'] if isinstance(metrics['realized'], Decimal) else None, places=pnl_places, with_sign=True)}",
        f"已实现≈USDT：{_format_optional_usdt(metrics['realized_usdt'] if isinstance(metrics['realized_usdt'], Decimal) else None)}",
        f"初始保证金(IMR)：{_format_optional_integer(metrics['imr'] if isinstance(metrics['imr'], Decimal) else None)}",
        f"维持保证金：{_format_optional_integer(metrics['mmr'] if isinstance(metrics['mmr'], Decimal) else None)}",
        f"Greeks 汇总(PA)：Δ {_format_optional_decimal(metrics['delta'] if isinstance(metrics['delta'], Decimal) else None)}"
        f" / Γ {_format_optional_decimal(metrics['gamma'] if isinstance(metrics['gamma'], Decimal) else None)}"
        f" / V {_format_optional_decimal(metrics['vega'] if isinstance(metrics['vega'], Decimal) else None)}"
        f" / Θ {_format_optional_decimal(metrics['theta'] if isinstance(metrics['theta'], Decimal) else None)}"
        f" / Θ≈USDT {_format_optional_usdt_precise(metrics['theta_usdt'] if isinstance(metrics['theta_usdt'], Decimal) else None, places=2)}",
        "",
        "包含持仓：",
    ]
    preview = positions[:8]
    lines.extend(
        f"- {item.inst_id} | {_format_position_size(item, position_instruments)} | 浮盈 {_format_position_unrealized_pnl(item)}"
        f" | 折合USDT {_format_optional_usdt(_position_unrealized_pnl_usdt(item, upl_usdt_prices))}"
        for item in preview
    )
    if len(positions) > len(preview):
        lines.append(f"- ... 还有 {len(positions) - len(preview)} 笔")
    return "\n".join(lines)


def _validate_protection_price_relationship(
    *,
    option_inst_id: str,
    direction: str,
    trigger_inst_id: str,
    trigger_price_type: str,
    take_profit: Decimal | None,
    stop_loss: Decimal | None,
) -> None:
    if take_profit is None or stop_loss is None:
        return
    profit_on_rise = infer_protection_profit_on_rise(
        option_inst_id=option_inst_id,
        direction="long" if direction == "long" else "short",
        trigger_inst_id=trigger_inst_id,
        trigger_price_type=trigger_price_type,  # type: ignore[arg-type]
    )
    logic_hint = describe_protection_price_logic(
        option_inst_id=option_inst_id,
        direction="long" if direction == "long" else "short",
        trigger_inst_id=trigger_inst_id,
        trigger_price_type=trigger_price_type,  # type: ignore[arg-type]
    )
    if profit_on_rise and take_profit <= stop_loss:
        raise ValueError(logic_hint)
    if not profit_on_rise and take_profit >= stop_loss:
        raise ValueError(logic_hint)


def _validate_protection_live_price_availability(
    client: OkxRestClient,
    protection: OptionProtectionConfig,
    position: OkxPosition,
) -> None:
    if protection.trigger_price_type == "mark":
        try:
            client.get_trigger_price(protection.trigger_inst_id, "mark")
        except OkxApiError as exc:
            raise ValueError(
                f"{protection.option_inst_id} 当前拿不到标记价格，不能用“期权标记价格”触发。"
                "请改用“现货最新价”，或者等 OKX 返回 markPx 后再启动。"
            ) from exc

    requires_mark_for_order = (
        protection.take_profit_order_mode == "mark_with_slippage"
        or protection.stop_loss_order_mode == "mark_with_slippage"
    )
    if not requires_mark_for_order:
        return

    try:
        client.get_trigger_price(protection.option_inst_id, "mark")
    except OkxApiError as exc:
        raise ValueError(
            f"{protection.option_inst_id} 当前拿不到标记价格，不能用“标记价格加减滑点”报单。"
            "请把止盈/止损报单方式改成“设定价格”，或者等 OKX 返回 markPx 后再启动。"
        ) from exc

    instrument = client.get_instrument(protection.option_inst_id)
    close_side = "sell" if derive_position_direction(position) == "long" else "buy"
    open_avg_price = position.avg_price

    if protection.take_profit_trigger is not None:
        if protection.take_profit_order_mode == "mark_with_slippage":
            mark_price = client.get_trigger_price(protection.option_inst_id, "mark")
            preview_price = build_close_order_price_from_mark(
                mark_price=mark_price,
                close_side=close_side,
                tick_size=instrument.tick_size,
                mode=protection.take_profit_order_mode,
                fixed_price=protection.take_profit_order_price,
                slippage=protection.take_profit_slippage,
            )
        else:
            preview_price = protection.take_profit_order_price
        if preview_price is not None:
            try:
                validate_live_protection_order_price_guard(
                    client=client,
                    option_inst_id=protection.option_inst_id,
                    close_side=close_side,
                    order_price=preview_price,
                    tick_size=instrument.tick_size,
                    open_avg_price=open_avg_price,
                )
            except RuntimeError as exc:
                raise ValueError(str(exc)) from exc

    if protection.stop_loss_trigger is not None:
        if protection.stop_loss_order_mode == "mark_with_slippage":
            mark_price = client.get_trigger_price(protection.option_inst_id, "mark")
            preview_price = build_close_order_price_from_mark(
                mark_price=mark_price,
                close_side=close_side,
                tick_size=instrument.tick_size,
                mode=protection.stop_loss_order_mode,
                fixed_price=protection.stop_loss_order_price,
                slippage=protection.stop_loss_slippage,
            )
        else:
            preview_price = protection.stop_loss_order_price
        if preview_price is not None:
            try:
                validate_live_protection_order_price_guard(
                    client=client,
                    option_inst_id=protection.option_inst_id,
                    close_side=close_side,
                    order_price=preview_price,
                    tick_size=instrument.tick_size,
                    open_avg_price=open_avg_price,
                )
            except RuntimeError as exc:
                raise ValueError(str(exc)) from exc


def _resolve_protection_order_mode_value(mode_label: str) -> str:
    if mode_label in PROTECTION_ORDER_MODE_OPTIONS:
        return PROTECTION_ORDER_MODE_OPTIONS[mode_label]
    normalized = mode_label.strip().lower()
    if "mark" in normalized or "滑点" in mode_label or "slippage" in normalized:
        return "mark_with_slippage"
    return "fixed_price"


def _format_protection_order_mode_label(mode: str) -> str:
    return "设定价格" if mode == "fixed_price" else "标记价格加减滑点"


def _format_protection_order_price_detail(mode: str, price: Decimal | None) -> str:
    if mode == "fixed_price":
        return _format_optional_decimal(price)
    return "自动按标记价与滑点计算"


def _format_protection_trigger_price_type(trigger_price_type: str) -> str:
    return "标记价" if trigger_price_type == "mark" else "最新价"


def _normalize_okx_timestamp_ms(timestamp_value: int | None) -> int | None:
    if timestamp_value is None or timestamp_value <= 0:
        return None
    normalized = int(timestamp_value)
    if normalized >= 1_000_000_000_000_000:
        normalized //= 1_000_000
    elif normalized >= 1_000_000_000_000:
        normalized //= 1_000
    if normalized < 100_000_000_000:
        normalized *= 1000
    if normalized < 1_262_304_000_000:
        return None
    return normalized


def _format_okx_ms_timestamp(timestamp_ms: int | None) -> str:
    normalized = _normalize_okx_timestamp_ms(timestamp_ms)
    if normalized is None:
        return "-"
    try:
        return datetime.fromtimestamp(normalized / 1000).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return "-"


def _format_history_side(side: str | None, pos_side: str | None) -> str:
    parts = [part for part in (side, pos_side) if part and part.lower() != "net"]
    return " / ".join(parts) if parts else (side or pos_side or "-")


def _desk_tree_final_iids_from_bases(base_iids: list[str]) -> list[str]:
    """与 `_line_trading_desk_sync_tree_rows` 一致：重复 base iid 时追加 __2、__3…"""
    iid_seen: dict[str, int] = {}
    out: list[str] = []
    for base in base_iids:
        cnt = iid_seen.get(base, 0)
        iid_seen[base] = cnt + 1
        if cnt > 0:
            out.append(f"{base}__{cnt + 1}")
        else:
            out.append(base)
    return out


def _history_tree_index(item_id: str, prefix: str) -> int | None:
    marker = f"{prefix}-"
    if not item_id.startswith(marker):
        return None
    try:
        return int(item_id[len(marker) :])
    except ValueError:
        return None


def _default_position_history_local_year_range_strings() -> tuple[str, str]:
    today = datetime.now().date()
    start = date(today.year, 1, 1)
    end = date(today.year, 12, 31)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def _parse_position_history_local_date(raw: str) -> date | None:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError:
        return None


def _position_history_local_date(update_time: int | None) -> date | None:
    normalized = _normalize_okx_timestamp_ms(update_time)
    if normalized is None:
        return None
    try:
        return datetime.fromtimestamp(normalized / 1000).date()
    except (OSError, OverflowError, ValueError):
        return None


def _position_history_in_local_date_range(
    item: OkxPositionHistoryItem,
    *,
    range_start_local: date | None,
    range_end_local: date | None,
) -> bool:
    if range_start_local is None and range_end_local is None:
        return True
    local_d = _position_history_local_date(getattr(item, "update_time", None))
    if local_d is None:
        return True
    if range_start_local is not None and local_d < range_start_local:
        return False
    if range_end_local is not None and local_d > range_end_local:
        return False
    return True


def _filter_position_history_items(
    items: list[OkxPositionHistoryItem],
    *,
    inst_type: str = "",
    margin_mode: str = "",
    asset: str = "",
    expiry_prefix: str = "",
    keyword: str = "",
    note_texts_by_index: dict[int, str] | None = None,
    range_start_local: date | None = None,
    range_end_local: date | None = None,
) -> list[tuple[int, OkxPositionHistoryItem]]:
    normalized_inst_type = inst_type.strip().upper()
    normalized_margin_mode = margin_mode.strip().lower()
    normalized_asset = asset.strip().upper()
    normalized_expiry_prefix = expiry_prefix.strip().upper()
    normalized_keyword = keyword.strip().lower()
    filtered: list[tuple[int, OkxPositionHistoryItem]] = []

    for index, item in enumerate(items):
        if normalized_inst_type and (item.inst_type or "").upper() != normalized_inst_type:
            continue
        if normalized_margin_mode and (item.mgn_mode or "").lower() != normalized_margin_mode:
            continue
        if normalized_asset and _extract_asset_key(item.inst_id).upper() != normalized_asset:
            continue
        if normalized_expiry_prefix and not _extract_history_expiry_prefix(item.inst_id).startswith(normalized_expiry_prefix):
            continue
        if normalized_keyword:
            haystack = " ".join(
                part
                for part in (
                    item.inst_id,
                    item.inst_type,
                    item.mgn_mode,
                    item.pos_side,
                    item.direction,
                    note_texts_by_index.get(index, "") if note_texts_by_index else "",
                )
                if part
            ).lower()
            if normalized_keyword not in haystack:
                continue
        if not _position_history_in_local_date_range(
            item,
            range_start_local=range_start_local,
            range_end_local=range_end_local,
        ):
            continue
        filtered.append((index, item))
    return filtered


def _filter_fill_history_items(
    items: list[OkxFillHistoryItem],
    *,
    inst_type: str = "",
    side: str = "",
    asset: str = "",
    expiry_prefix: str = "",
    keyword: str = "",
) -> list[tuple[int, OkxFillHistoryItem]]:
    normalized_inst_type = inst_type.strip().upper()
    normalized_side = side.strip().lower()
    normalized_asset = asset.strip().upper()
    normalized_expiry_prefix = expiry_prefix.strip().upper()
    normalized_keyword = keyword.strip().lower()
    filtered: list[tuple[int, OkxFillHistoryItem]] = []

    for index, item in enumerate(items):
        if normalized_inst_type and (item.inst_type or "").upper() != normalized_inst_type:
            continue
        if normalized_side and (item.side or "").lower() != normalized_side:
            continue
        if normalized_asset and _extract_asset_key(item.inst_id).upper() != normalized_asset:
            continue
        if normalized_expiry_prefix and not _extract_history_expiry_prefix(item.inst_id).startswith(normalized_expiry_prefix):
            continue
        if normalized_keyword:
            haystack = " ".join(
                part
                for part in (
                    item.inst_id,
                    item.inst_type,
                    item.side,
                    item.pos_side,
                    item.exec_type,
                    item.fee_currency,
                )
                if part
            ).lower()
            if normalized_keyword not in haystack:
                continue
        filtered.append((index, item))
    return filtered


def _filter_trade_order_items(
    items: list[OkxTradeOrderItem],
    *,
    inst_type: str = "",
    source: str = "",
    state: str = "",
    asset: str = "",
    expiry_prefix: str = "",
    keyword: str = "",
) -> list[tuple[int, OkxTradeOrderItem]]:
    normalized_inst_type = inst_type.strip().upper()
    normalized_source = source.strip().lower()
    normalized_state = state.strip().lower()
    normalized_asset = asset.strip().upper()
    normalized_expiry_prefix = expiry_prefix.strip().upper()
    normalized_keyword = keyword.strip().lower()
    filtered: list[tuple[int, OkxTradeOrderItem]] = []

    for index, item in enumerate(items):
        if normalized_inst_type and (item.inst_type or "").upper() != normalized_inst_type:
            continue
        if normalized_source and (item.source_kind or "").lower() != normalized_source:
            continue
        if normalized_state and (item.state or "").lower() != normalized_state:
            continue
        if normalized_asset and _extract_asset_key(item.inst_id).upper() != normalized_asset:
            continue
        if normalized_expiry_prefix and not _extract_history_expiry_prefix(item.inst_id).startswith(normalized_expiry_prefix):
            continue
        if normalized_keyword:
            haystack = " ".join(
                part
                for part in (
                    item.source_label,
                    item.source_kind,
                    item.inst_id,
                    item.inst_type,
                    item.state,
                    item.side,
                    item.pos_side,
                    item.td_mode,
                    item.ord_type,
                    item.order_id,
                    item.algo_id,
                    item.client_order_id,
                    item.algo_client_order_id,
                )
                if part
            ).lower()
            if normalized_keyword not in haystack:
                continue
        filtered.append((index, item))
    return filtered


def _trade_order_filter_enabled(
    inst_type: str,
    source: str,
    state: str,
    asset: str,
    expiry_prefix: str,
    keyword: str,
) -> bool:
    return any(
        str(value or "").strip()
        for value in (inst_type, source, state, asset, expiry_prefix, keyword)
    )


def _format_trade_order_timestamp(item: OkxTradeOrderItem) -> str:
    return _format_okx_ms_timestamp(item.update_time or item.created_time)


def _format_trade_order_state(state: str | None) -> str:
    normalized = (state or "").strip().lower()
    if not normalized:
        return "-"
    mapping = {
        "live": "生效中 live",
        "partially_filled": "部分成交 partially_filled",
        "filled": "已成交 filled",
        "canceled": "已撤销 canceled",
        "mmp_canceled": "风控撤销 mmp_canceled",
        "effective": "算法生效 effective",
        "triggered": "已触发 triggered",
        "pause": "暂停 pause",
        "order_failed": "失败 order_failed",
        "partially_failed": "部分失败 partially_failed",
    }
    return mapping.get(normalized, normalized)


def _format_trade_order_trigger_price_type(trigger_price_type: str | None) -> str:
    normalized = (trigger_price_type or "").strip().lower()
    if not normalized:
        return "-"
    mapping = {
        "mark": "标记价 mark",
        "last": "最新价 last",
        "index": "指数价 index",
    }
    return mapping.get(normalized, normalized)


def _format_trade_order_price(value: Decimal | None, inst_id: str, inst_type: str) -> str:
    if value is None:
        return "-"
    if value == Decimal("-1"):
        return "市价"
    normalized_inst_type = (inst_type or "").upper()
    if normalized_inst_type == "OPTION":
        return _format_optional_decimal(value)
    quote_currency = _extract_quote_key(inst_id)
    if quote_currency in {"USDT", "USD", "USDC"}:
        return _format_optional_decimal_fixed(value, places=2)
    return _format_optional_decimal(value)


def _format_trade_order_size(value: Decimal | None) -> str:
    return _format_optional_decimal(value)


def _format_trade_order_coin_size(item: OkxTradeOrderItem, instruments: dict[str, Instrument]) -> str:
    amount, currency = _history_display_amount(
        inst_id=item.inst_id,
        inst_type=item.inst_type,
        size=item.size,
        reference_price=item.price if item.price and item.price > 0 else item.avg_price,
        instruments=instruments,
    )
    return _format_history_size_amount(amount, currency)


def _format_trade_order_coin_filled_size(item: OkxTradeOrderItem, instruments: dict[str, Instrument]) -> str:
    amount, currency = _history_display_amount(
        inst_id=item.inst_id,
        inst_type=item.inst_type,
        size=item.filled_size,
        reference_price=item.avg_price or item.price,
        instruments=instruments,
    )
    return _format_history_size_amount(amount, currency)


def _format_trade_order_tp_sl(item: OkxTradeOrderItem) -> str:
    def _build_tp_sl_segment(label: str, trigger_price: Decimal | None, order_price: Decimal | None) -> str | None:
        if trigger_price is None:
            return None
        trigger_text = _format_trade_order_price(trigger_price, item.inst_id, item.inst_type)
        if order_price is None or order_price == trigger_price:
            return f"{label} {trigger_text}"
        order_text = _format_trade_order_price(order_price, item.inst_id, item.inst_type)
        return f"{label} {trigger_text}=>{order_text}"

    parts = [
        segment
        for segment in (
            _build_tp_sl_segment("TP", item.take_profit_trigger_price, item.take_profit_order_price),
            _build_tp_sl_segment("SL", item.stop_loss_trigger_price, item.stop_loss_order_price),
        )
        if segment
    ]
    if parts:
        return " / ".join(parts)
    if item.trigger_price is not None:
        trigger_text = _format_trade_order_price(item.trigger_price, item.inst_id, item.inst_type)
        if item.order_price is None or item.order_price == item.trigger_price:
            return f"触发 {trigger_text}"
        order_text = _format_trade_order_price(item.order_price, item.inst_id, item.inst_type)
        return f"触发 {trigger_text}=>{order_text}"
    return "-"


def _trade_order_cancel_reference(item: OkxTradeOrderItem) -> str:
    if item.source_kind == "algo":
        return item.algo_id or item.algo_client_order_id or item.client_order_id or ""
    return item.order_id or item.client_order_id or ""


def _trade_order_program_owner_label(item: OkxTradeOrderItem) -> str | None:
    candidates = [
        (item.client_order_id or "").strip().lower(),
        (item.algo_client_order_id or "").strip().lower(),
    ]
    for candidate in candidates:
        if not candidate:
            continue
        if PROTECTION_CL_ORD_ID_PATTERN.fullmatch(candidate):
            return "风控保护"
        if SMART_ORDER_CL_ORD_ID_PATTERN.fullmatch(candidate):
            return "智能下单"
        if ENGINE_CL_ORD_ID_PATTERN.fullmatch(candidate):
            return "策略引擎"
    return None


def _session_order_prefixes(session: StrategySession) -> tuple[str, ...]:
    session_token = "".join(ch for ch in session.session_id.lower() if ch.isascii() and ch.isalnum())[:4] or "sess"
    strategy_token = "".join(ch for ch in session.strategy_name.lower() if ch.isascii() and ch.isalnum())[:4] or "stg"
    return (f"{session_token}{strategy_token}",)


def _trade_order_session_role(item: OkxTradeOrderItem, session: StrategySession) -> str | None:
    prefixes = _session_order_prefixes(session)
    candidates = [
        (item.client_order_id or "").strip().lower(),
        (item.algo_client_order_id or "").strip().lower(),
    ]
    for candidate in candidates:
        if not candidate:
            continue
        for prefix in prefixes:
            if not candidate.startswith(prefix):
                continue
            role = candidate[len(prefix): len(prefix) + 3]
            if role in {"ent", "exi", "slg"}:
                return role
    return None


def _trade_order_belongs_to_session(item: OkxTradeOrderItem, session: StrategySession) -> bool:
    return _trade_order_session_role(item, session) is not None


def _trade_order_cancel_summary(item: OkxTradeOrderItem) -> str:
    cancel_id = _trade_order_cancel_reference(item) or "-"
    return (
        f"{item.source_label or '委托'} | 合约={item.inst_id or '-'} | "
        f"方向={_format_history_side(item.side, item.pos_side)} | 标识={cancel_id}"
    )


def _trade_order_fill_summary(item: OkxTradeOrderItem) -> str:
    return (
        f"{item.source_label or '委托'} | 合约={item.inst_id or '-'} | "
        f"状态={_format_trade_order_state(item.state)} | "
        f"方向={_format_history_side(item.side, item.pos_side)} | "
        f"成交量={_format_trade_order_size(item.filled_size)} | "
        f"均价={_format_trade_order_price(item.avg_price or item.price, item.inst_id, item.inst_type)}"
    )


def _position_manual_review_summary(position: OkxPosition) -> str:
    side = position.pos_side or "net"
    avg_price = _format_optional_decimal(position.avg_price)
    return f"{position.inst_id} [{side}] | 持仓量={format_decimal(position.position)} | 开仓均价={avg_price}"


def _format_history_cell_with_approx_usdt(
    native_display: str,
    value: Decimal | None,
    currency: str | None,
    usdt_prices: dict[str, Decimal],
) -> str:
    """Append `(≈N USDT)` for non USDT-like fee/pnl cells when a spot index price exists."""
    if not usdt_prices or native_display == "-" or value is None:
        return native_display
    ccy = (currency or "").strip().upper()
    if not ccy or ccy in {"USDT", "USD", "USDC"}:
        return native_display
    price = usdt_prices.get(ccy)
    if price is None:
        return native_display
    usdt_val = value * price
    rounded = usdt_val.quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    approx_num = format_decimal_fixed(rounded, 0)
    return f"{native_display} (≈{approx_num} USDT)"


def _format_trade_order_fee_cell(item: OkxTradeOrderItem, usdt_prices: dict[str, Decimal] | None = None) -> str:
    text = _format_optional_decimal(item.fee, with_sign=True)
    ccy = (item.fee_currency or "").strip().upper()
    if ccy and text != "-":
        base = f"{text} {ccy}"
    else:
        base = text
    if usdt_prices:
        return _format_history_cell_with_approx_usdt(base, item.fee, ccy, usdt_prices)
    return base


def _format_position_history_fee_cell(
    item: OkxPositionHistoryItem,
    usdt_prices: dict[str, Decimal] | None = None,
) -> str:
    text = _format_optional_decimal(item.fee, with_sign=True)
    ccy = (item.fee_currency or "").strip().upper()
    if ccy and text != "-":
        base = f"{text} {ccy}"
    else:
        base = text
    if usdt_prices:
        return _format_history_cell_with_approx_usdt(base, item.fee, ccy, usdt_prices)
    return base


def _format_fill_history_fee_cell(item: OkxFillHistoryItem, usdt_prices: dict[str, Decimal] | None = None) -> str:
    text = _format_optional_decimal(item.fill_fee, with_sign=True)
    ccy = (item.fee_currency or "").strip().upper()
    if ccy and text != "-":
        base = f"{text} {ccy}"
    else:
        base = text
    if usdt_prices:
        return _format_history_cell_with_approx_usdt(base, item.fill_fee, ccy, usdt_prices)
    return base


def _build_trade_order_detail_text(item: OkxTradeOrderItem) -> str:
    def _format_flag(value: bool | None) -> str:
        if value is True:
            return "是"
        if value is False:
            return "否"
        return "-"

    def _format_amount(value: Decimal | None, currency: str | None, *, with_sign: bool = False) -> str:
        amount_text = _format_optional_decimal(value, with_sign=with_sign)
        normalized_currency = (currency or "").strip().upper()
        if normalized_currency and amount_text != "-":
            return f"{amount_text} {normalized_currency}"
        return amount_text

    owner_label = _trade_order_program_owner_label(item)
    lines = [
        f"来源：{item.source_label or '-'}",
        f"程序来源：{owner_label or '非本程序委托'}",
        f"创建时间：{_format_okx_ms_timestamp(item.created_time)}",
        f"更新时间：{_format_okx_ms_timestamp(item.update_time)}",
        f"合约：{item.inst_id or '-'}",
        f"类型：{item.inst_type or '-'}",
        f"状态：{_format_trade_order_state(item.state)}",
        f"方向：{_format_history_side(item.side, item.pos_side)}",
        f"保证金模式：{_format_margin_mode(item.td_mode)}",
        f"委托类型：{item.ord_type or '-'}",
        f"委托价：{_format_trade_order_price(item.price, item.inst_id, item.inst_type)}",
        f"委托量：{_format_trade_order_size(item.size)}",
        f"已成交：{_format_trade_order_size(item.filled_size)}",
        f"成交均价：{_format_trade_order_price(item.avg_price, item.inst_id, item.inst_type)}",
        f"reduceOnly：{_format_flag(item.reduce_only)}",
        f"TP/SL：{_format_trade_order_tp_sl(item)}",
        f"订单ID：{item.order_id or '-'}",
        f"算法ID：{item.algo_id or '-'}",
        f"clOrdId：{item.client_order_id or '-'}",
        f"algoClOrdId：{item.algo_client_order_id or '-'}",
    ]

    if item.trigger_price is not None:
        lines.append(f"触发价：{_format_trade_order_price(item.trigger_price, item.inst_id, item.inst_type)}")
        lines.append(f"触发价类型：{_format_trade_order_trigger_price_type(item.trigger_price_type)}")
    if item.order_price is not None and item.order_price != item.price:
        lines.append(f"算法委托价：{_format_trade_order_price(item.order_price, item.inst_id, item.inst_type)}")
    if item.actual_price is not None and item.actual_price != item.avg_price:
        lines.append(f"实际价格：{_format_trade_order_price(item.actual_price, item.inst_id, item.inst_type)}")
    if item.actual_size is not None and item.actual_size != item.filled_size:
        lines.append(f"实际数量：{_format_trade_order_size(item.actual_size)}")
    if item.actual_side and item.actual_side != item.side:
        lines.append(f"实际方向：{item.actual_side}")
    if item.take_profit_trigger_price is not None:
        tp_line = f"止盈触发价：{_format_trade_order_price(item.take_profit_trigger_price, item.inst_id, item.inst_type)}"
        if item.take_profit_trigger_price_type:
            tp_line = f"{tp_line}（{_format_trade_order_trigger_price_type(item.take_profit_trigger_price_type)}）"
        lines.append(tp_line)
    if item.take_profit_order_price is not None:
        lines.append(f"止盈委托价：{_format_trade_order_price(item.take_profit_order_price, item.inst_id, item.inst_type)}")
    if item.stop_loss_trigger_price is not None:
        sl_line = f"止损触发价：{_format_trade_order_price(item.stop_loss_trigger_price, item.inst_id, item.inst_type)}"
        if item.stop_loss_trigger_price_type:
            sl_line = f"{sl_line}（{_format_trade_order_trigger_price_type(item.stop_loss_trigger_price_type)}）"
        lines.append(sl_line)
    if item.stop_loss_order_price is not None:
        lines.append(f"止损委托价：{_format_trade_order_price(item.stop_loss_order_price, item.inst_id, item.inst_type)}")
    if item.pnl is not None:
        lines.append(f"盈亏：{_format_optional_decimal(item.pnl, with_sign=True)}")
    if item.fee is not None or item.fee_currency:
        lines.append(f"手续费：{_format_amount(item.fee, item.fee_currency, with_sign=True)}")

    raw_text = json.dumps(item.raw, ensure_ascii=False, indent=2, sort_keys=True)
    return "\n".join(lines) + f"\n\n原始响应：\n{raw_text}"


def _format_position_history_currency_totals(totals: dict[str, Decimal]) -> str:
    if not totals:
        return "-"
    parts: list[str] = []
    for currency, value in totals.items():
        normalized_currency = (currency or "-").upper()
        parts.append(f"{normalized_currency} {_format_history_amount(value, normalized_currency, with_sign=True)}")
    return " / ".join(parts)


def _format_position_history_filter_stats(
    filtered_items: list[tuple[int, OkxPositionHistoryItem]],
    usdt_prices: dict[str, Decimal],
) -> str:
    pnl_totals: dict[str, Decimal] = {}
    realized_totals: dict[str, Decimal] = {}
    realized_usdt_total = Decimal("0")
    realized_usdt_count = 0

    for _, item in filtered_items:
        currency = _infer_position_history_pnl_currency(item)
        if item.pnl is not None:
            pnl_totals[currency] = pnl_totals.get(currency, Decimal("0")) + item.pnl
        if item.realized_pnl is not None:
            realized_totals[currency] = realized_totals.get(currency, Decimal("0")) + item.realized_pnl
        realized_usdt = _position_history_realized_pnl_usdt(item, usdt_prices)
        if realized_usdt is not None:
            realized_usdt_total += realized_usdt
            realized_usdt_count += 1

    realized_usdt_text = (
        _format_optional_usdt(realized_usdt_total)
        if realized_usdt_count
        else "-"
    )
    return (
        f"\u76c8\u4e8f\u5408\u8ba1 { _format_position_history_currency_totals(pnl_totals) } | "
        f"\u5df2\u5b9e\u73b0\u5408\u8ba1 { _format_position_history_currency_totals(realized_totals) } | "
        f"\u6298\u5408USDT\u5408\u8ba1 {realized_usdt_text}"
    )


def _infer_position_history_pnl_currency(item: OkxPositionHistoryItem) -> str:
    quote_currency = _extract_quote_key(item.inst_id)
    if item.inst_type in {"SWAP", "SPOT", "FUTURES"} and quote_currency in {"USDT", "USD", "USDC"}:
        return quote_currency
    return _extract_asset_key(item.inst_id).upper()


def _position_history_realized_pnl_usdt(
    item: OkxPositionHistoryItem,
    upl_usdt_prices: dict[str, Decimal],
) -> Decimal | None:
    if item.inst_type != "OPTION":
        return None
    if item.realized_pnl is None:
        return None
    currency = _infer_position_history_pnl_currency(item)
    if currency in {"USDT", "USD", "USDC"}:
        return item.realized_pnl
    price = upl_usdt_prices.get(currency)
    if price is None:
        return None
    return item.realized_pnl * price


def _build_history_instrument_map(
    client: OkxRestClient,
    inst_ids: list[str],
) -> dict[str, Instrument]:
    option_families = sorted(
        {
            family
            for inst_id in inst_ids
            if _infer_history_inst_type(inst_id) == "OPTION"
            for family in [infer_option_family(inst_id)]
            if family
        }
    )
    instruments: dict[str, Instrument] = {}
    for family in option_families:
        try:
            option_instruments = client.get_option_instruments(inst_family=family)
        except Exception:
            continue
        for instrument in option_instruments:
            instruments[instrument.inst_id] = instrument

    futures_ids = {inst_id for inst_id in inst_ids if _infer_history_inst_type(inst_id) == "FUTURES"}
    futures_families = {
        family
        for inst_id in futures_ids
        for family in [_extract_history_family(inst_id, "FUTURES")]
        if family
    }
    if futures_ids:
        try:
            for instrument in client.get_instruments("FUTURES"):
                instrument_family = (
                    instrument.inst_family.upper()
                    if instrument.inst_family
                    else _extract_history_family(instrument.inst_id, instrument.inst_type)
                )
                if instrument.inst_id in futures_ids or instrument_family in futures_families:
                    instruments[instrument.inst_id] = instrument
        except Exception:
            pass

    swap_ids = {inst_id for inst_id in inst_ids if _infer_history_inst_type(inst_id) == "SWAP"}
    if swap_ids:
        try:
            for instrument in client.get_swap_instruments():
                if instrument.inst_id in swap_ids:
                    instruments[instrument.inst_id] = instrument
        except Exception:
            pass
    return instruments


def _resolve_history_instrument(
    *,
    inst_id: str,
    inst_type: str,
    instruments: dict[str, Instrument],
) -> Instrument | None:
    instrument = instruments.get(inst_id)
    if instrument is not None:
        return instrument

    family = _extract_history_family(inst_id, inst_type)
    if not family:
        return None

    for candidate in instruments.values():
        candidate_family = (
            candidate.inst_family.upper()
            if candidate.inst_family
            else _extract_history_family(candidate.inst_id, candidate.inst_type)
        )
        if candidate.inst_type != inst_type or candidate_family != family:
            continue
        if candidate.ct_val is not None and candidate.ct_val > 0 and candidate.ct_val_ccy:
            return candidate
    return None


def _history_display_amount(
    *,
    inst_id: str,
    inst_type: str,
    size: Decimal | None,
    reference_price: Decimal | None,
    instruments: dict[str, Instrument],
) -> tuple[Decimal | None, str | None]:
    if size is None:
        return None, None

    instrument = _resolve_history_instrument(inst_id=inst_id, inst_type=inst_type, instruments=instruments)
    if instrument is None or instrument.ct_val is None or instrument.ct_val <= 0 or not instrument.ct_val_ccy:
        normalized_type = (inst_type or "").upper()
        quote_currency = (_extract_quote_key(inst_id) or "").upper()
        base_currency = _extract_asset_key(inst_id).upper()
        if normalized_type == "FUTURES" and quote_currency in {"USD", "USDT", "USDC"} and reference_price is not None and reference_price > 0 and base_currency:
            # Expired futures may not be returned by public instrument list.
            # For USD-like quoted futures, use notional/price fallback to show coin amount.
            return abs(size) / reference_price, base_currency
        if normalized_type in {"SWAP", "FUTURES", "OPTION"}:
            return abs(size), "张"
        return size, base_currency if base_currency else None

    multiplier = instrument.ct_mult if instrument.ct_mult is not None and instrument.ct_mult > 0 else Decimal("1")
    payout_currency = instrument.ct_val_ccy.upper()
    if payout_currency in {"USD", "USDT", "USDC"} and inst_type in {"FUTURES", "SWAP"}:
        base_currency = _extract_asset_key(inst_id).upper()
        if reference_price is not None and reference_price > 0 and base_currency:
            amount = abs(size) * instrument.ct_val * multiplier / reference_price
            return amount, base_currency
    amount = abs(size) * instrument.ct_val * multiplier
    return amount, payout_currency


def _format_history_size_amount(value: Decimal | None, currency: str | None) -> str:
    if value is None:
        return "-"
    text = _format_optional_decimal_capped(value, places=4)
    if currency:
        return f"{text} {currency}"
    return text


def _format_fill_history_size(item: OkxFillHistoryItem, instruments: dict[str, Instrument]) -> str:
    amount, currency = _history_fill_display_amount(item, instruments)
    return _format_history_size_amount(amount, currency)


def _history_fill_display_amount(
    item: OkxFillHistoryItem,
    instruments: dict[str, Instrument],
) -> tuple[Decimal | None, str | None]:
    raw = item.raw if isinstance(item.raw, dict) else {}
    bill_id = str(raw.get("billId") or "").strip()
    bill_sub_type = str(raw.get("subType") or "").strip()

    if bill_id and item.inst_type == "FUTURES" and bill_sub_type in {"112", "113"}:
        instrument = _resolve_history_instrument(inst_id=item.inst_id, inst_type=item.inst_type, instruments=instruments)
        if instrument is not None and item.fill_price is not None and item.fill_price > 0:
            raw_fill_sz = raw.get("fillSz")
            if raw_fill_sz not in {None, ""}:
                try:
                    contract_size = Decimal(str(raw_fill_sz))
                except (InvalidOperation, ValueError):
                    contract_size = item.fill_size
                else:
                    amount, currency = _history_display_amount(
                        inst_id=item.inst_id,
                        inst_type=item.inst_type,
                        size=contract_size,
                        reference_price=item.fill_price,
                        instruments=instruments,
                    )
                    if amount is not None:
                        return amount, currency

            payout_currency = (instrument.ct_val_ccy or "").upper()
            base_currency = _extract_asset_key(item.inst_id).upper()
            if payout_currency in {"USD", "USDT", "USDC"} and base_currency and item.fill_size is not None:
                return abs(item.fill_size) / item.fill_price, base_currency

    amount, currency = _history_display_amount(
        inst_id=item.inst_id,
        inst_type=item.inst_type,
        size=item.fill_size,
        reference_price=item.fill_price,
        instruments=instruments,
    )
    return amount, currency


def _format_position_history_size(item: OkxPositionHistoryItem, instruments: dict[str, Instrument]) -> str:
    amount, currency = _history_display_amount(
        inst_id=item.inst_id,
        inst_type=item.inst_type,
        size=item.close_size,
        reference_price=item.close_avg_price,
        instruments=instruments,
    )
    return _format_history_size_amount(amount, currency)


def _infer_fill_history_pnl_currency(item: OkxFillHistoryItem) -> str:
    if item.inst_type == "OPTION":
        return _extract_asset_key(item.inst_id).upper()
    quote_currency = _extract_quote_key(item.inst_id)
    if quote_currency in {"USDT", "USD", "USDC"}:
        return quote_currency
    return _extract_asset_key(item.inst_id).upper()


def _format_optional_decimal_capped(value: Decimal | None, *, places: int, with_sign: bool = False) -> str:
    if value is None:
        return "-"
    quant = Decimal("1").scaleb(-places)
    rounded = value.quantize(quant, rounding=ROUND_HALF_UP)
    text = format_decimal(rounded)
    if with_sign and value > 0:
        return f"+{text}"
    return text


def _format_history_amount(value: Decimal | None, currency: str | None, *, with_sign: bool = False) -> str:
    normalized = (currency or "").upper()
    if normalized in {"USDT", "USD", "USDC"}:
        return _format_optional_decimal_fixed(value, places=2, with_sign=with_sign)
    return _format_optional_decimal_capped(value, places=8, with_sign=with_sign)


def _format_position_history_price(value: Decimal | None, inst_id: str, inst_type: str) -> str:
    if inst_type == "OPTION":
        return _format_optional_decimal_capped(value, places=8)
    quote_currency = _extract_quote_key(inst_id)
    if quote_currency in {"USDT", "USD", "USDC"}:
        return _format_optional_decimal_fixed(value, places=2)
    return _format_optional_decimal_capped(value, places=8)


def _format_position_history_pnl(
    value: Decimal | None,
    item: OkxPositionHistoryItem,
    *,
    with_sign: bool = False,
    usdt_prices: dict[str, Decimal] | None = None,
) -> str:
    currency = _infer_position_history_pnl_currency(item)
    base = _format_history_amount(value, currency, with_sign=with_sign)
    if usdt_prices:
        return _format_history_cell_with_approx_usdt(base, value, currency, usdt_prices)
    return base


def _format_fill_history_pnl(item: OkxFillHistoryItem, usdt_prices: dict[str, Decimal] | None = None) -> str:
    if item.pnl is not None and item.pnl == 0:
        return ""
    currency = _infer_fill_history_pnl_currency(item)
    base = _format_history_amount(item.pnl, currency, with_sign=True)
    if usdt_prices:
        return _format_history_cell_with_approx_usdt(base, item.pnl, currency, usdt_prices)
    return base


def _normalize_fill_history_exec_type(value: object) -> str:
    text = str(value or "").strip()
    lowered = text.lower()
    if lowered in {"t", "m", "exercise", "delivery"}:
        return lowered
    if any(marker in text for marker in ("行权", "琛屾潈")) and any(
        marker in text for marker in ("交割", "浜ゅ壊")
    ):
        return "exercise_delivery"
    if "exercise" in lowered or any(marker in text for marker in ("行权", "琛屾潈")):
        return "exercise"
    if any(marker in lowered for marker in ("delivery", "expire", "expiration")) or any(
        marker in text for marker in ("交割", "浜ゅ壊")
    ):
        return "delivery"
    return text


def _format_fill_history_exec_type(value: object) -> str:
    normalized = _normalize_fill_history_exec_type(value)
    if normalized == "t":
        return "T"
    if normalized == "m":
        return "M"
    if normalized == "exercise":
        return "行权"
    if normalized == "delivery":
        return "交割"
    if normalized == "exercise_delivery":
        return "行权/交割"
    return str(value or "").strip() or "-"


def _format_fill_history_price(item: OkxFillHistoryItem) -> str:
    if _normalize_fill_history_exec_type(item.exec_type) in {"exercise", "delivery", "exercise_delivery"}:
        return _format_optional_decimal_fixed(item.fill_price, places=2)
    if item.inst_type == "OPTION":
        return _format_optional_decimal(item.fill_price)
    quote_currency = _extract_quote_key(item.inst_id)
    if quote_currency in {"USDT", "USD", "USDC"}:
        return _format_optional_decimal_fixed(item.fill_price, places=2)
    return _format_optional_decimal(item.fill_price)


def _build_account_config_detail_text(
    config: OkxAccountConfig,
    overview: OkxAccountOverview,
    *,
    profile_name: str,
    environment: str,
) -> str:
    environment_label = "实盘 live" if environment == "live" else "模拟盘 demo"
    if config.auto_loan is True:
        auto_loan_text = "开启"
    elif config.auto_loan is False:
        auto_loan_text = "关闭"
    else:
        auto_loan_text = "-"
    return (
        f"API配置：{profile_name}\n"
        f"环境：{environment_label}\n"
        f"账户模式：{_format_account_level(config.account_level)}\n"
        f"持仓模式：{_format_account_position_mode(config.position_mode)}\n"
        f"Greeks类型：{_format_greeks_type(config.greeks_type)}\n"
        f"自动借币：{auto_loan_text}\n"
        f"总权益：{_format_optional_usdt_precise(overview.total_equity, places=2, with_sign=False)}\n"
        f"调整后权益：{_format_optional_usdt_precise(overview.adjusted_equity, places=2, with_sign=False)}\n"
        f"总名义价值(USD)：{_format_optional_usdt_precise(overview.notional_usd, places=2, with_sign=False)}\n"
        f"订单冻结：{_format_optional_usdt_precise(overview.order_frozen, places=2, with_sign=False)}"
    )


def _build_account_asset_detail_text(item: OkxAccountAssetItem) -> str:
    return (
        f"币种：{item.ccy or '-'}\n"
        f"权益：{_format_optional_decimal(item.equity)}\n"
        f"折合USD：{_format_optional_usdt_precise(item.equity_usd, places=2, with_sign=False)}\n"
        f"现金余额：{_format_optional_decimal(item.cash_balance)}\n"
        f"可用余额：{_format_optional_decimal(item.available_balance)}\n"
        f"可用权益：{_format_optional_decimal(item.available_equity)}\n"
        f"未实现盈亏：{_format_optional_decimal(item.unrealized_pnl, with_sign=True)}\n"
        f"折后权益：{_format_optional_decimal(item.discount_equity)}\n"
        f"冻结：{_format_optional_decimal(item.frozen_balance)}\n"
        f"负债：{_format_optional_decimal(item.liability)}\n"
        f"全仓负债：{_format_optional_decimal(item.cross_liability)}\n"
        f"利息：{_format_optional_decimal(item.interest)}"
    )


def _build_fill_history_detail_text(item: OkxFillHistoryItem, instruments: dict[str, Instrument]) -> str:
    return (
        f"时间：{_format_okx_ms_timestamp(item.fill_time)}\n"
        f"合约：{item.inst_id or '-'}\n"
        f"类型：{item.inst_type or '-'}\n"
        f"方向：{_format_history_side(item.side, item.pos_side)}\n"
        f"成交价：{_format_fill_history_price(item)}\n"
        f"成交量：{_format_fill_history_size(item, instruments)}\n"
        f"手续费：{_format_fill_history_fee_cell(item)}\n"
        f"已实现盈亏：{_format_fill_history_pnl(item)}\n"
        f"成交类型：{_format_fill_history_exec_type(item.exec_type)}\n"
        f"订单ID：{item.order_id or '-'}\n"
        f"成交ID：{item.trade_id or '-'}"
    )


def _build_position_history_detail_text(
    item: OkxPositionHistoryItem,
    upl_usdt_prices: dict[str, Decimal],
    instruments: dict[str, Instrument],
    note: str = "",
) -> str:
    note_line = _format_position_note_detail(note)
    return (
        f"更新时间：{_format_okx_ms_timestamp(item.update_time)}\n"
        f"合约：{item.inst_id or '-'}\n"
        f"类型：{item.inst_type or '-'}\n"
        f"{note_line}"
        f"保证金模式：{_format_margin_mode(item.mgn_mode or '')}\n"
        f"方向：{_format_history_side(None, item.pos_side or item.direction)}\n"
        f"开仓均价：{_format_position_history_price(item.open_avg_price, item.inst_id, item.inst_type)}\n"
        f"平仓均价：{_format_position_history_price(item.close_avg_price, item.inst_id, item.inst_type)}\n"
        f"平仓数量：{_format_position_history_size(item, instruments)}\n"
        f"手续费：{_format_position_history_fee_cell(item, upl_usdt_prices)}\n"
        f"盈亏：{_format_position_history_pnl(item.pnl, item, usdt_prices=upl_usdt_prices)}\n"
        f"已实现盈亏：{_format_position_history_pnl(item.realized_pnl, item, with_sign=True, usdt_prices=upl_usdt_prices)}\n"
        f"结算盈亏：{_format_optional_decimal(item.settle_pnl)}"
    )


def _tree_display_columns(tree: ttk.Treeview, columns: tuple[str, ...]) -> tuple[str, ...]:
    display_columns = tree.cget("displaycolumns")
    if display_columns in ("#all", ("#all",), "", None):
        return columns
    if isinstance(display_columns, str):
        return tuple(part for part in display_columns.split() if part)
    return tuple(display_columns)


def _widget_exists(widget: object) -> bool:
    if widget is None:
        return False
    winfo_exists = getattr(widget, "winfo_exists", None)
    if not callable(winfo_exists):
        return False
    try:
        return bool(winfo_exists())
    except TclError:
        return False


def _tree_safe_token(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.:-]+", "_", value.strip())
    return cleaned or "item"


def _asset_group_row_id(asset_label: str) -> str:
    return f"asset:{_tree_safe_token(asset_label)}"


def _bucket_group_row_id(asset_label: str, bucket_label: str) -> str:
    return f"bucket:{_tree_safe_token(asset_label)}:{_tree_safe_token(bucket_label)}"


def _position_tree_row_id(position: OkxPosition) -> str:
    pos_side = (position.pos_side or "net").lower()
    mgn_mode = (position.mgn_mode or "unknown").lower()
    return f"pos:{_tree_safe_token(position.inst_id)}:{_tree_safe_token(pos_side)}:{_tree_safe_token(mgn_mode)}"


def _find_position_by_key(positions: list[OkxPosition], key: str) -> OkxPosition | None:
    for position in positions:
        if _position_tree_row_id(position) == key:
            return position
    return None


def _resolve_position_selection_target(
    *,
    existing_ids: set[str],
    selected_position_key: str | None,
    protection_position_key: str | None,
    selected_before: str | None,
    top_items: tuple[str, ...],
) -> str | None:
    for candidate in (selected_position_key, protection_position_key, selected_before):
        if candidate and candidate in existing_ids:
            return candidate
    return top_items[0] if top_items else None


def _pnl_tag(value: Decimal | None) -> str:
    if value is None:
        return "group"
    if value > 0:
        return "profit"
    if value < 0:
        return "loss"
    return "group"


def _refresh_status_with_recovery_support(self: QuantApp) -> None:
    running_count = 0
    self._refresh_session_live_pnl_cache()
    for session in self.sessions.values():
        if session.engine.is_running:
            if session.status != "停止中":
                session.status = "运行中"
                if session.runtime_status in {"待恢复", "恢复中"} and not session.last_message:
                    session.runtime_status = "运行中"
                if session.ended_reason in {"应用关闭", "应用关闭后待恢复接管", "恢复中", "恢复启动失败"}:
                    session.ended_reason = ""
            running_count += 1
        elif session.stop_cleanup_in_progress:
            session.status = "停止中"
        elif session.status == "恢复中":
            session.status = "待恢复"
            session.runtime_status = "待恢复"
            if session.stopped_at is None:
                session.stopped_at = datetime.now()
            if not session.ended_reason or session.ended_reason == "恢复中":
                session.ended_reason = "恢复启动失败"
        elif session.status in {"运行中", "停止中"}:
            if self._session_should_transition_to_recoverable(session):
                session.status = "待恢复"
                session.runtime_status = "待恢复"
                if session.stopped_at is None:
                    session.stopped_at = datetime.now()
                session.ended_reason = self._session_stop_reason_text(session) or "策略异常停止，待恢复接管"
                self._upsert_recoverable_strategy_session(session)
            else:
                session.status = "已停止"
                if session.stopped_at is None:
                    session.stopped_at = datetime.now()
                session.ended_reason = self._session_stop_reason_text(session)
                self._remove_recoverable_strategy_session(session.session_id)
                self._trader_desk_handle_stopped_session(session)
        self._upsert_session_row(session)
        self._sync_strategy_history_from_session(session)

    self.status_text.set(f"运行中策略：{running_count}")
    self._refresh_trader_desk_runtime()
    self._refresh_running_session_summary()
    self._update_settings_summary()
    self._refresh_selected_session_details()
    self._refresh_strategy_book_window()
    self.root.after(500, self._refresh_status)


QuantApp._refresh_status = _refresh_status_with_recovery_support


def run_app() -> None:
    app = QuantApp()
    app.root.mainloop()
