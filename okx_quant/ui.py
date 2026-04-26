from __future__ import annotations

import json
import os
import queue
import re
import threading
from dataclasses import MISSING, dataclass, field, fields as dataclass_fields, replace
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
import tkinter.font as tkfont
from tkinter import BooleanVar, Canvas, END, Label, Menu, StringVar, Text, TclError, Tk, Toplevel, filedialog, simpledialog
from tkinter import messagebox, ttk

from okx_quant.app_meta import APP_VERSION, build_app_title, build_version_info_text
from okx_quant.backtest_ui import BacktestCompareOverviewWindow, BacktestLaunchState, BacktestWindow
from okx_quant.deribit_client import DeribitRestClient
from okx_quant.deribit_volatility_monitor_ui import DeribitVolatilityMonitorWindow
from okx_quant.deribit_volatility_ui import DeribitVolatilityWindow
from okx_quant.engine import (
    StrategyEngine,
    fetch_hourly_ema_debug,
    fixed_entry_side_mode_support_reason,
    format_hourly_debug,
    supports_fixed_entry_side_mode,
)
from okx_quant.log_utils import append_log_line, append_preformatted_log_line, strategy_session_log_file_path
from okx_quant.models import Credentials, EmailNotificationConfig, Instrument, StrategyConfig
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
    load_recoverable_strategy_sessions_snapshot,
    load_credentials_profiles_snapshot,
    load_notification_snapshot,
    load_position_notes_snapshot,
    load_strategy_history_snapshot,
    load_strategy_trade_ledger_snapshot,
    save_recoverable_strategy_sessions_snapshot,
    save_credentials_profiles_snapshot,
    save_notification_snapshot,
    save_position_notes_snapshot,
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
from okx_quant.pricing import format_decimal, format_decimal_fixed, snap_to_increment
from okx_quant.signal_monitor_ui import SignalMonitorWindow
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
    StrategyLiveChartSnapshot,
    build_strategy_live_chart_snapshot,
    render_strategy_live_chart,
)
from okx_quant.strategy_catalog import (
    BACKTEST_STRATEGY_DEFINITIONS,
    STRATEGY_DEFINITIONS,
    STRATEGY_DYNAMIC_ID,
    STRATEGY_DYNAMIC_LONG_ID,
    STRATEGY_DYNAMIC_SHORT_ID,
    STRATEGY_EMA5_EMA8_ID,
    StrategyDefinition,
    get_strategy_definition,
    is_dynamic_strategy_id,
    resolve_dynamic_signal_mode,
    supports_signal_only,
)
from okx_quant.window_layout import (
    apply_adaptive_window_geometry,
    apply_fill_window_geometry,
    apply_window_icon,
)


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
        "pnl",
        "realized",
        "realized_usdt",
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


@dataclass
class StrategyTradeRuntimeState:
    round_id: str
    signal_bar_at: datetime | None = None
    opened_logged_at: datetime | None = None
    entry_order_id: str = ""
    entry_client_order_id: str = ""
    entry_price: Decimal | None = None
    size: Decimal | None = None
    protective_algo_id: str = ""
    protective_algo_cl_ord_id: str = ""
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
    if "OKX 读取异常，准备重试" in text or "OKX 读取失败" in text:
        return current_status or "网络重试中"
    if (
        "开始监控 OKX 动态止损" in text
        or "开始本地止盈止损监控" in text
        or "交易员虚拟止损监控启动" in text
    ):
        return "持仓监控中"
    if (
        "挂单已成交" in text
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
        or "挂单已提交到 OKX" in text
        or "委托追踪" in text
        or "检测到挂单状态已变更" in text
        or "挂单部分成交" in text
        or "启动追单窗口内接管当前波段" in text
    ):
        return "开仓监控中"
    if "已提交启动请求" in text:
        return "启动中"
    if (
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


class QuantApp:
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
        self._signal_monitor_window: SignalMonitorWindow | None = None
        self._trader_desk_window: TraderDeskWindow | None = None
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
        self._position_instruments: dict[str, Instrument] = {}
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
        self._pending_order_canceling = False
        self._order_history_refreshing = False
        self._pending_orders_last_refresh_at: datetime | None = None
        self._order_history_last_refresh_at: datetime | None = None
        self._fills_history_last_refresh_at: datetime | None = None
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
        self._position_history_fetch_limit = 100
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
        self.trade_symbol.trace_add("write", self._update_fixed_order_size_hint)
        self.risk_amount.trace_add("write", self._update_fixed_order_size_hint)
        self.order_size.trace_add("write", self._update_fixed_order_size_hint)
        self.time_stop_break_even_enabled.trace_add("write", lambda *_: self._sync_dynamic_take_profit_controls())
        self.run_mode_label.trace_add("write", lambda *_: self._sync_entry_side_mode_controls())
        self.tp_sl_mode_label.trace_add("write", lambda *_: self._sync_entry_side_mode_controls())

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
        self.position_type_filter = StringVar(value="全部类型")
        self.position_keyword = StringVar()
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
        self.settings_summary_text = StringVar()
        self.strategy_summary_text = StringVar()
        self.strategy_rule_text = StringVar()
        self.strategy_hint_text = StringVar()
        self.selected_session_text = StringVar(value=self._default_selected_session_text())
        self._selected_session_detail_session_id: str | None = None
        self.strategy_history_text = StringVar(value=self._default_strategy_history_text())
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
        self._positions_snapshot_by_profile: dict[str, ProfilePositionSnapshot] = {}
        self._session_live_pnl_cache: dict[str, tuple[Decimal | None, datetime | None]] = {}

        self._settings_watch_enabled = False
        self._settings_save_job: str | None = None
        self._last_saved_notification_state: tuple[object, ...] | None = None

        self._load_saved_credentials()
        self._load_saved_notification_settings()
        self._load_position_notes()
        self._load_recoverable_strategy_sessions_registry()
        self._load_strategy_history()
        self._load_strategy_trade_ledger()
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
        return strategy_id == STRATEGY_EMA5_EMA8_ID

    def _build_menu(self) -> None:
        menu_bar = Menu(self.root)

        settings_menu = Menu(menu_bar, tearoff=False)
        settings_menu.add_command(label="API 与通知设置", command=self.open_settings_window)
        menu_bar.add_cascade(label="设置", menu=settings_menu)

        tools_menu = Menu(menu_bar, tearoff=False)
        tools_menu.add_command(label="打开无限下单", command=self.open_smart_order_window)
        tools_menu.add_command(label="打开回测窗口", command=self.open_backtest_window)
        tools_menu.add_command(label="打开回测对比总览", command=self.open_backtest_compare_window)
        tools_menu.add_command(label="打开信号观察台", command=self.open_signal_monitor_window)
        tools_menu.add_command(label="打开交易员管理台", command=self.open_trader_desk_window)
        tools_menu.add_command(label="打开波动率监控", command=self.open_deribit_volatility_monitor_window)
        tools_menu.add_command(label="打开Deribit波动率指数", command=self.open_deribit_volatility_window)
        tools_menu.add_command(label="打开期权策略计算器", command=self.open_option_strategy_window)
        tools_menu.add_command(label="刷新账户持仓", command=self.refresh_positions)
        menu_bar.add_cascade(label="工具", menu=tools_menu)

        system_menu = Menu(menu_bar, tearoff=False)
        system_menu.add_command(label=f"版本信息 (v{APP_VERSION})", command=self.show_version_info)
        system_menu.add_separator()
        system_menu.add_command(label="退出", command=self._on_close)
        menu_bar.add_cascade(label="系统", menu=system_menu)

        self.root.config(menu=menu_bar)

    def show_version_info(self) -> None:
        messagebox.showinfo("版本信息", build_version_info_text(), parent=self.root)

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
        launcher_frame.rowconfigure(1, weight=0)
        sessions_frame.columnconfigure(0, weight=1)
        sessions_frame.rowconfigure(0, weight=1)

        start_frame = ttk.LabelFrame(launcher_frame, text="策略启动", padding=16)
        start_frame.grid(row=0, column=0, sticky="ew")
        for column in range(4):
            start_frame.columnconfigure(column, weight=1)

        row = 0
        ttk.Label(start_frame, text="选择策略").grid(row=row, column=0, sticky="w")
        self.strategy_combo = ttk.Combobox(
            start_frame,
            textvariable=self.strategy_name,
            values=[item.name for item in STRATEGY_DEFINITIONS],
            state="readonly",
        )
        self.strategy_combo.grid(row=row, column=1, sticky="ew", padx=(0, 16))
        self.strategy_combo.bind("<<ComboboxSelected>>", self._on_strategy_selected)
        ttk.Label(start_frame, text="交易标的").grid(row=row, column=2, sticky="w")
        self.symbol_combo = ttk.Combobox(
            start_frame,
            textvariable=self.symbol,
            values=self._default_symbol_values,
            state="readonly",
        )
        self.symbol_combo.grid(row=row, column=3, sticky="ew")

        row += 1
        ttk.Label(start_frame, text="K线周期").grid(row=row, column=0, sticky="w", pady=(12, 0))
        ttk.Combobox(start_frame, textvariable=self.bar, values=BAR_OPTIONS, state="readonly").grid(
            row=row, column=1, sticky="ew", padx=(0, 16), pady=(12, 0)
        )
        ttk.Label(start_frame, text="信号方向").grid(row=row, column=2, sticky="w", pady=(12, 0))
        self.signal_combo = ttk.Combobox(start_frame, textvariable=self.signal_mode_label, state="readonly")
        self.signal_combo.grid(row=row, column=3, sticky="ew", pady=(12, 0))

        row += 1
        self._take_profit_mode_label = ttk.Label(start_frame, text="止盈方式")
        self._take_profit_mode_label.grid(row=row, column=0, sticky="w", pady=(12, 0))
        self._take_profit_mode_combo = ttk.Combobox(
            start_frame,
            textvariable=self.take_profit_mode_label,
            values=list(TAKE_PROFIT_MODE_OPTIONS.keys()),
            state="readonly",
        )
        self._take_profit_mode_combo.grid(row=row, column=1, sticky="ew", padx=(0, 16), pady=(12, 0))
        self._take_profit_mode_combo.bind("<<ComboboxSelected>>", lambda *_: self._sync_dynamic_take_profit_controls())
        self._max_entries_per_trend_label = ttk.Label(start_frame, text="每波最多开仓次数")
        self._max_entries_per_trend_label.grid(row=row, column=2, sticky="w", pady=(12, 0))
        self._max_entries_per_trend_entry = ttk.Entry(start_frame, textvariable=self.max_entries_per_trend)
        self._max_entries_per_trend_entry.grid(row=row, column=3, sticky="ew", pady=(12, 0))

        row += 1
        self._startup_chase_window_label = ttk.Label(start_frame, text="启动追单窗口(秒)")
        self._startup_chase_window_label.grid(row=row, column=0, sticky="w", pady=(12, 0))
        self._startup_chase_window_entry = ttk.Entry(start_frame, textvariable=self.startup_chase_window_seconds)
        self._startup_chase_window_entry.grid(row=row, column=1, sticky="ew", padx=(0, 16), pady=(12, 0))
        self._startup_chase_window_hint_label = ttk.Label(
            start_frame,
            text="0=不追单；300=只追启动前5分钟内刚确认的信号。",
        )
        self._startup_chase_window_hint_label.grid(row=row, column=2, columnspan=2, sticky="w", pady=(12, 0))

        row += 1
        self._dynamic_two_r_break_even_check = ttk.Checkbutton(
            start_frame,
            text="启用2R保本（2R时先移到保本位）",
            variable=self.dynamic_two_r_break_even,
        )
        self._dynamic_two_r_break_even_check.grid(row=row, column=0, columnspan=4, sticky="w", pady=(12, 0))

        row += 1
        self._dynamic_fee_offset_check = ttk.Checkbutton(
            start_frame,
            text="启用手续费偏移（按2倍Taker手续费留缓冲）",
            variable=self.dynamic_fee_offset_enabled,
        )
        self._dynamic_fee_offset_check.grid(row=row, column=0, columnspan=4, sticky="w", pady=(8, 0))

        row += 1
        self._dynamic_fee_offset_hint_label = ttk.Label(
            start_frame,
            text="提示：保本位是否叠加手续费偏移，由下方开关决定；大部分组合开启更优，默认建议开启。",
        )
        self._dynamic_fee_offset_hint_label.grid(row=row, column=0, columnspan=4, sticky="w", pady=(2, 0))

        row += 1
        self._time_stop_break_even_check = ttk.Checkbutton(
            start_frame,
            text="启用时间保本（持仓满指定K线且已达到净保本时，上移到保本位）",
            variable=self.time_stop_break_even_enabled,
        )
        self._time_stop_break_even_check.grid(row=row, column=0, columnspan=2, sticky="w", pady=(8, 0))
        self._time_stop_break_even_bars_label = ttk.Label(start_frame, text="时间保本K线数")
        self._time_stop_break_even_bars_label.grid(row=row, column=2, sticky="e", pady=(8, 0))
        self._time_stop_break_even_bars_entry = ttk.Entry(start_frame, textvariable=self.time_stop_break_even_bars)
        self._time_stop_break_even_bars_entry.grid(row=row, column=3, sticky="ew", pady=(8, 0))

        row += 1
        ttk.Label(start_frame, text="运行模式").grid(row=row, column=0, sticky="w", pady=(12, 0))
        ttk.Combobox(
            start_frame,
            textvariable=self.run_mode_label,
            values=list(RUN_MODE_OPTIONS.keys()),
            state="readonly",
        ).grid(row=row, column=1, sticky="ew", padx=(0, 16), pady=(12, 0))
        ttk.Label(start_frame, text="轮询秒数").grid(row=row, column=2, sticky="w", pady=(12, 0))
        ttk.Entry(start_frame, textvariable=self.poll_seconds).grid(row=row, column=3, sticky="ew", pady=(12, 0))

        row += 1
        ttk.Label(start_frame, text="EMA小周期").grid(row=row, column=0, sticky="w", pady=(12, 0))
        ttk.Entry(start_frame, textvariable=self.ema_period).grid(
            row=row, column=1, sticky="ew", padx=(0, 16), pady=(12, 0)
        )
        ttk.Label(start_frame, text="EMA中周期").grid(row=row, column=2, sticky="w", pady=(12, 0))
        ttk.Entry(start_frame, textvariable=self.trend_ema_period).grid(
            row=row, column=3, sticky="ew", pady=(12, 0)
        )

        row += 1
        self._big_ema_label = ttk.Label(start_frame, text="EMA大周期")
        self._big_ema_label.grid(row=row, column=0, sticky="w", pady=(12, 0))
        self._big_ema_entry = ttk.Entry(start_frame, textvariable=self.big_ema_period)
        self._big_ema_entry.grid(row=row, column=1, sticky="ew", padx=(0, 16), pady=(12, 0))
        self._entry_reference_ema_label = ttk.Label(start_frame, text="挂单参考EMA")
        self._entry_reference_ema_label.grid(row=row, column=0, sticky="w", pady=(12, 0))
        self._entry_reference_ema_entry = ttk.Entry(start_frame, textvariable=self.entry_reference_ema_period)
        self._entry_reference_ema_entry.grid(row=row, column=1, sticky="ew", padx=(0, 16), pady=(12, 0))
        ttk.Label(start_frame, text="ATR 周期").grid(row=row, column=2, sticky="w", pady=(12, 0))
        ttk.Entry(start_frame, textvariable=self.atr_period).grid(
            row=row, column=3, sticky="ew", pady=(12, 0)
        )

        row += 1
        ttk.Label(start_frame, text="止损 ATR 倍数").grid(row=row, column=0, sticky="w", pady=(12, 0))
        ttk.Entry(start_frame, textvariable=self.stop_atr).grid(
            row=row, column=1, sticky="ew", padx=(0, 16), pady=(12, 0)
        )
        ttk.Label(start_frame, text="止盈 ATR 倍数").grid(row=row, column=2, sticky="w", pady=(12, 0))
        ttk.Entry(start_frame, textvariable=self.take_atr).grid(row=row, column=3, sticky="ew", pady=(12, 0))

        row += 1
        ttk.Label(start_frame, text="风险金").grid(row=row, column=0, sticky="w", pady=(12, 0))
        ttk.Entry(start_frame, textvariable=self.risk_amount).grid(
            row=row, column=1, sticky="ew", padx=(0, 16), pady=(12, 0)
        )
        ttk.Label(start_frame, text="固定数量").grid(row=row, column=2, sticky="w", pady=(12, 0))
        ttk.Entry(start_frame, textvariable=self.order_size).grid(row=row, column=3, sticky="ew", pady=(12, 0))

        row += 1
        ttk.Label(
            start_frame,
            textvariable=self.fixed_order_size_hint_text,
            justify="left",
            wraplength=760,
        ).grid(row=row, column=0, columnspan=4, sticky="w", pady=(4, 0))

        row += 1
        ttk.Label(start_frame, text="下单方向模式").grid(row=row, column=0, sticky="w", pady=(12, 0))
        self._entry_side_mode_combo = ttk.Combobox(
            start_frame,
            textvariable=self.entry_side_mode_label,
            values=list(ENTRY_SIDE_MODE_OPTIONS.keys()),
            state="readonly",
        )
        self._entry_side_mode_combo.grid(row=row, column=1, sticky="ew", padx=(0, 16), pady=(12, 0))
        ttk.Label(start_frame, text="止盈止损模式").grid(row=row, column=2, sticky="w", pady=(12, 0))
        ttk.Combobox(
            start_frame,
            textvariable=self.tp_sl_mode_label,
            values=LAUNCHER_TP_SL_MODE_LABELS,
            state="readonly",
        ).grid(row=row, column=3, sticky="ew", pady=(12, 0))

        row += 1
        ttk.Label(
            start_frame,
            textvariable=self.entry_side_mode_hint_text,
            justify="left",
            wraplength=760,
        ).grid(row=row, column=0, columnspan=4, sticky="w", pady=(4, 0))

        row += 1
        ttk.Label(start_frame, text="自定义触发标的").grid(row=row, column=0, sticky="w", pady=(12, 0))
        self.local_tp_sl_symbol_combo = ttk.Combobox(
            start_frame,
            textvariable=self.local_tp_sl_symbol,
            values=self._custom_trigger_symbol_values,
            state="readonly",
        )
        self.local_tp_sl_symbol_combo.grid(
            row=row, column=1, sticky="ew", padx=(0, 16), pady=(12, 0)
        )

        row += 1
        button_row = ttk.Frame(start_frame)
        button_row.grid(row=row, column=0, columnspan=4, sticky="w", pady=(16, 0))
        ttk.Button(button_row, text="启动", command=self.start).grid(row=0, column=0, padx=(0, 8))
        ttk.Button(button_row, text="加载 OKX SWAP", command=self.load_symbols).grid(row=0, column=1, padx=(0, 8))
        ttk.Button(button_row, text="导出 1小时调试", command=self.debug_hourly_values).grid(row=0, column=2)

        row += 1
        ttk.Label(
            start_frame,
            text="API、交易模式、持仓模式和邮件通知都已移动到菜单：设置 > API 与通知设置",
            wraplength=820,
            justify="left",
        ).grid(row=row, column=0, columnspan=4, sticky="w", pady=(14, 0))

        strategy_info = ttk.LabelFrame(launcher_frame, text="策略说明", padding=16)
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
                "api",
                "source_type",
                "strategy",
                "symbol",
                "bar",
                "direction",
                "mode",
                "live_pnl",
                "pnl",
                "status",
                "started",
            ),
            show="headings",
            selectmode="browse",
        )
        self.session_tree.heading("session", text="会话")
        self.session_tree.heading("trader", text="交易员")
        self.session_tree.heading("api", text="API")
        self.session_tree.heading("source_type", text="来源类型")
        self.session_tree.heading("strategy", text="策略")
        self.session_tree.heading("symbol", text="标的")
        self.session_tree.heading("bar", text="周期")
        self.session_tree.heading("direction", text="方向")
        self.session_tree.heading("mode", text="模式")
        self.session_tree.heading("live_pnl", text="实时浮盈亏")
        self.session_tree.heading("pnl", text="净盈亏")
        self.session_tree.heading("status", text="状态")
        self.session_tree.heading("started", text="启动时间")
        self.session_tree.column("session", width=56, anchor="center")
        self.session_tree.column("trader", width=72, anchor="center")
        self.session_tree.column("api", width=80, anchor="center")
        self.session_tree.column("source_type", width=98, anchor="center")
        self.session_tree.column("strategy", width=120, anchor="w")
        self.session_tree.column("symbol", width=156, anchor="w")
        self.session_tree.column("bar", width=68, anchor="center")
        self.session_tree.column("direction", width=82, anchor="center")
        self.session_tree.column("mode", width=96, anchor="center")
        self.session_tree.column("live_pnl", width=112, anchor="e")
        self.session_tree.column("pnl", width=104, anchor="e")
        self.session_tree.column("status", width=108, anchor="center")
        self.session_tree.column("started", width=96, anchor="center")
        self.session_tree.grid(row=1, column=0, sticky="nsew")
        self.session_tree.bind("<<TreeviewSelect>>", self._on_session_selected)
        self.session_tree.tag_configure("duplicate_conflict", background="#fff4e5", foreground="#a85a00")

        tree_scroll = ttk.Scrollbar(running_frame, orient="vertical", command=self.session_tree.yview)
        tree_scroll.grid(row=1, column=1, sticky="ns")
        self.session_tree.configure(yscrollcommand=tree_scroll.set)

        control_row = ttk.Frame(running_frame)
        control_row.grid(row=2, column=0, columnspan=2, sticky="w", pady=(10, 0))
        ttk.Button(control_row, text="\u505c\u6b62\u9009\u4e2d\u7b56\u7565", command=self.stop_selected_session).grid(row=0, column=0)
        ttk.Button(control_row, text="\u5b9e\u65f6K\u7ebf\u56fe", command=self.open_selected_strategy_live_chart).grid(
            row=0, column=1, padx=(8, 0)
        )
        ttk.Button(control_row, text="\u4fe1\u53f7\u89c2\u5bdf\u53f0", command=self.open_signal_monitor_window).grid(
            row=0,
            column=2,
            padx=(8, 0),
        )
        ttk.Button(control_row, text="\u4ea4\u6613\u5458\u7ba1\u7406\u53f0", command=self.open_trader_desk_window).grid(
            row=0,
            column=3,
            padx=(8, 0),
        )
        ttk.Button(control_row, text="\u6e05\u7a7a\u5df2\u505c\u6b62", command=self.clear_stopped_sessions).grid(
            row=0, column=4, padx=(8, 0)
        )
        ttk.Button(control_row, text="\u5386\u53f2\u7b56\u7565", command=self.open_strategy_history_window).grid(
            row=0, column=5, padx=(8, 0)
        )
        ttk.Button(control_row, text="\u7b56\u7565\u603b\u8d26\u672c", command=self.open_strategy_book_window).grid(
            row=0, column=6, padx=(8, 0)
        )
        ttk.Button(control_row, text="\u5bfc\u51fa\u9009\u4e2d\u53c2\u6570", command=self.export_selected_session_template).grid(
            row=0, column=7, padx=(8, 0)
        )
        ttk.Button(control_row, text="\u5bfc\u5165\u7b56\u7565\u53c2\u6570", command=self.import_strategy_template).grid(
            row=0, column=8, padx=(8, 0)
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

        positions_frame = ttk.LabelFrame(sessions_pane, text="账户持仓（仿 OKX 客户端风格）", padding=12)
        positions_frame.columnconfigure(0, weight=1)
        positions_frame.rowconfigure(3, weight=1)
        sessions_pane.add(positions_frame, weight=7)

        header_row = ttk.Frame(positions_frame)
        header_row.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        header_row.columnconfigure(1, weight=1)
        positions_badge = self._create_refresh_badge(
            header_row,
            self._positions_refresh_badge_text,
            self._positions_refresh_badges,
        )
        positions_badge.grid(row=0, column=0, sticky="w", padx=(0, 8))
        ttk.Label(header_row, textvariable=self.positions_summary_text).grid(row=0, column=1, sticky="w")
        action_row = ttk.Frame(header_row)
        action_row.grid(row=0, column=2, sticky="e")
        ttk.Button(action_row, text="刷新", command=self.refresh_positions).grid(row=0, column=0, padx=(0, 6))
        ttk.Label(action_row, text="自动刷新").grid(row=0, column=1, padx=(0, 6))
        position_refresh_interval_combo = ttk.Combobox(
            action_row,
            textvariable=self.position_refresh_interval_label,
            values=list(POSITION_REFRESH_INTERVAL_OPTIONS.keys()),
            state="readonly",
            width=8,
        )
        position_refresh_interval_combo.grid(row=0, column=2, padx=(0, 6))
        position_refresh_interval_combo.bind("<<ComboboxSelected>>", self._on_position_refresh_interval_changed)
        ttk.Button(
            action_row,
            textvariable=self.position_auto_refresh_button_text,
            command=self.toggle_position_auto_refresh,
        ).grid(row=0, column=3, padx=(0, 6))
        ttk.Button(action_row, text="账户信息", command=self.open_account_info_window).grid(row=0, column=4, padx=(0, 6))
        ttk.Button(action_row, text="持仓大窗", command=self.open_positions_zoom_window).grid(row=0, column=5, padx=(0, 6))
        ttk.Button(action_row, text="设置期权保护", command=self.open_position_protection_window).grid(
            row=0, column=6, padx=(0, 6)
        )
        ttk.Button(action_row, text="展期建议", command=self.open_option_roll_window).grid(
            row=0, column=7, padx=(0, 6)
        )
        ttk.Button(action_row, text="全部展开", command=self.expand_all_position_groups).grid(
            row=0, column=8, padx=(0, 6)
        )
        ttk.Button(action_row, text="折叠分组", command=self.collapse_position_groups).grid(
            row=0, column=9, padx=(0, 6)
        )
        ttk.Button(action_row, text="编辑备注", command=self.edit_selected_position_note).grid(row=0, column=10, padx=(0, 6))
        ttk.Button(action_row, text="清空备注", command=self.clear_selected_position_note).grid(row=0, column=11, padx=(0, 6))
        ttk.Button(action_row, text="复制合约", command=self.copy_selected_position_symbol).grid(row=0, column=12)

        filter_row = ttk.Frame(positions_frame)
        filter_row.grid(row=1, column=0, sticky="ew", pady=(0, 8))
        filter_row.columnconfigure(3, weight=1)
        ttk.Label(filter_row, text="类型").grid(row=0, column=0, sticky="w")
        position_type_combo = ttk.Combobox(
            filter_row,
            textvariable=self.position_type_filter,
            values=list(POSITION_TYPE_OPTIONS.keys()),
            state="readonly",
            width=16,
        )
        position_type_combo.grid(row=0, column=1, sticky="w", padx=(6, 12))
        position_type_combo.bind("<<ComboboxSelected>>", self._on_position_filter_changed)
        ttk.Label(filter_row, text="搜索").grid(row=0, column=2, sticky="w")
        position_keyword_entry = ttk.Entry(filter_row, textvariable=self.position_keyword)
        position_keyword_entry.grid(row=0, column=3, sticky="ew", padx=(6, 12))
        position_keyword_entry.bind("<KeyRelease>", self._on_position_filter_changed)
        ttk.Button(filter_row, text="应用筛选", command=self._render_positions_view).grid(
            row=0, column=4, padx=(0, 6)
        )
        ttk.Button(filter_row, text="清空筛选", command=self.reset_position_filters).grid(row=0, column=5)

        overview_row = ttk.Frame(positions_frame)
        overview_row.grid(row=2, column=0, sticky="ew", pady=(0, 10))
        for column in range(9):
            overview_row.columnconfigure(column, weight=1)
        self._build_metric_card(overview_row, 0, "持仓笔数", self.position_total_text)
        self._build_metric_card(overview_row, 1, "浮动盈亏(USDT)", self.position_upl_text)
        self._build_metric_card(overview_row, 2, "已实现盈亏", self.position_realized_text)
        self._build_metric_card(overview_row, 3, "初始保证金(IMR)", self.position_margin_text)
        self._build_metric_card(overview_row, 4, "Delta 合计", self.position_delta_text)
        self._build_metric_card(overview_row, 5, "买购数量", self.position_long_call_text)
        self._build_metric_card(overview_row, 6, "卖购数量", self.position_short_call_text)
        self._build_metric_card(overview_row, 7, "买沽数量", self.position_long_put_text)
        self._build_metric_card(overview_row, 8, "卖沽数量", self.position_short_put_text)

        position_table = ttk.Frame(positions_frame)
        position_table.grid(row=3, column=0, sticky="nsew")
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

    def _build_metric_card(self, parent: ttk.Frame, column: int, title: str, value_var: StringVar) -> None:
        card = ttk.LabelFrame(parent, text=title, padding=(10, 8))
        card.grid(row=0, column=column, sticky="nsew", padx=(0 if column == 0 else 6, 0))
        card.columnconfigure(0, weight=1)
        ttk.Label(card, textvariable=value_var, font=("Microsoft YaHei UI", 11, "bold")).grid(
            row=0, column=0, sticky="w"
        )

    def _create_trade_order_tree(
        self,
        parent: ttk.Frame,
        *,
        on_select,
        column_group_key: str | None = None,
        title: str | None = None,
    ) -> ttk.Treeview:
        columns = ("time", "source", "inst_type", "inst_id", "state", "side", "ord_type", "price", "size", "filled", "tp_sl", "order_id", "cl_ord_id")
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
            ("tp_sl", 180),
            ("order_id", 120),
            ("cl_ord_id", 150),
        ):
            tree.heading(column_id, text=headings[column_id])
            tree.column(column_id, width=width, anchor="e" if column_id in {"price", "size", "filled"} else "center")
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
        ttk.Button(header, text="刷新", command=self.refresh_order_history).grid(row=0, column=2, sticky="e")

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

    def _schedule_protection_window_refresh(self) -> None:
        try:
            if self.root.winfo_exists():
                self.root.after(0, self._refresh_protection_window_view)
        except Exception:
            return

    def _position_refresh_interval_ms(self) -> int:
        return POSITION_REFRESH_INTERVAL_OPTIONS.get(self.position_refresh_interval_label.get(), 15_000)

    def toggle_position_auto_refresh(self) -> None:
        self.position_auto_refresh_enabled = not self.position_auto_refresh_enabled
        if self.position_auto_refresh_enabled:
            self.position_auto_refresh_button_text.set("暂停自动刷新")
            self._enqueue_log(f"账户持仓已恢复自动刷新，当前间隔：{self.position_refresh_interval_label.get()}")
            self.refresh_positions()
        else:
            self.position_auto_refresh_button_text.set("恢复自动刷新")
            self._enqueue_log("账户持仓已暂停自动刷新。需要更新时可以手动点“刷新”。")
            self._update_position_summary(_filter_positions(
                self._latest_positions,
                inst_type=POSITION_TYPE_OPTIONS[self.position_type_filter.get()],
                keyword=self.position_keyword.get(),
                note_texts=self._current_position_note_text_map(),
            ))

    def _on_position_refresh_interval_changed(self, *_: object) -> None:
        visible_positions = _filter_positions(
            self._latest_positions,
            inst_type=POSITION_TYPE_OPTIONS[self.position_type_filter.get()],
            keyword=self.position_keyword.get(),
            note_texts=self._current_position_note_text_map(),
        )
        self._update_position_summary(visible_positions)
        self._enqueue_log(f"账户持仓自动刷新间隔已切换为：{self.position_refresh_interval_label.get()}")

    def _on_position_filter_changed(self, *_: object) -> None:
        self._render_positions_view()

    def reset_position_filters(self) -> None:
        self.position_type_filter.set("全部类型")
        self.position_keyword.set("")
        self._render_positions_view()

    def expand_all_position_groups(self) -> None:
        for item_id in self.position_tree.get_children():
            self.position_tree.item(item_id, open=True)
            for child_id in self.position_tree.get_children(item_id):
                self.position_tree.item(child_id, open=True)

    def collapse_position_groups(self) -> None:
        for item_id in self.position_tree.get_children():
            for child_id in self.position_tree.get_children(item_id):
                self.position_tree.item(child_id, open=False)
            self.position_tree.item(item_id, open=False)

    def copy_selected_position_symbol(self) -> None:
        payload = self._selected_position_payload()
        if payload is None or payload["kind"] != "position":
            messagebox.showinfo("提示", "请先在持仓列表中选中一条具体持仓。")
            return
        position = payload["item"]
        if not isinstance(position, OkxPosition):
            return
        self.root.clipboard_clear()
        self.root.clipboard_append(position.inst_id)
        self._enqueue_log(f"已复制合约代码：{position.inst_id}")

    def _current_position_note_context(self) -> tuple[str, str]:
        profile_name = (self._positions_context_profile_name or self._current_credential_profile()).strip()
        environment = self._positions_effective_environment or ENV_OPTIONS[self.environment_label.get()]
        return profile_name, environment

    def _position_history_note_context(self) -> tuple[str, str]:
        profile_name = (self._position_history_profile_name or self._current_credential_profile()).strip()
        environment = self._position_history_effective_environment or ENV_OPTIONS[self.environment_label.get()]
        return profile_name, environment

    def _current_position_note_text(self, position: OkxPosition) -> str:
        profile_name, environment = self._current_position_note_context()
        record = self._position_current_notes.get(_position_note_current_key(profile_name, environment, position))
        return _normalize_position_note_text(record.get("note", "")) if isinstance(record, dict) else ""

    def _current_position_note_summary(self, position: OkxPosition) -> str:
        return _format_position_note_summary(self._current_position_note_text(position))

    def _position_history_note_text(self, item: OkxPositionHistoryItem) -> str:
        profile_name, environment = self._position_history_note_context()
        record = self._position_history_notes.get(_position_history_note_key(profile_name, environment, item))
        return _normalize_position_note_text(record.get("note", "")) if isinstance(record, dict) else ""

    def _position_history_note_summary(self, item: OkxPositionHistoryItem) -> str:
        return _format_position_note_summary(self._position_history_note_text(item))

    def _current_position_note_text_map(self) -> dict[str, str]:
        return {
            _position_tree_row_id(position): self._current_position_note_text(position)
            for position in self._latest_positions
        }

    def _position_history_note_text_map_by_index(self) -> dict[int, str]:
        return {
            index: self._position_history_note_text(item)
            for index, item in enumerate(self._latest_position_history)
        }

    def _selected_position_history_item(self) -> OkxPositionHistoryItem | None:
        if self._positions_zoom_position_history_tree is None:
            return None
        selection = self._positions_zoom_position_history_tree.selection()
        if not selection:
            return None
        index = _history_tree_index(selection[0], "ph")
        if index is None or index >= len(self._latest_position_history):
            return None
        return self._latest_position_history[index]

    def _sync_position_note_state_for_positions(
        self,
        *,
        profile_name: str,
        environment: str,
        positions: list[OkxPosition],
    ) -> None:
        now_ms = _now_epoch_ms()
        changed = _reconcile_current_position_note_records(
            self._position_current_notes,
            profile_name=profile_name,
            environment=environment,
            positions=positions,
            now_ms=now_ms,
        )
        if (
            self._position_history_profile_name == profile_name
            and self._position_history_effective_environment == environment
            and self._latest_position_history
        ):
            changed = _inherit_position_history_notes(
                self._position_current_notes,
                self._position_history_notes,
                profile_name=profile_name,
                environment=environment,
                position_history=self._latest_position_history,
                now_ms=now_ms,
            ) or changed
            changed = _prune_closed_current_position_notes(
                self._position_current_notes,
                self._position_history_notes,
                profile_name=profile_name,
                environment=environment,
            ) or changed
        if changed:
            self._save_position_notes()

    def _sync_position_note_state_for_history(
        self,
        *,
        profile_name: str,
        environment: str,
        position_history: list[OkxPositionHistoryItem],
    ) -> None:
        now_ms = _now_epoch_ms()
        changed = _inherit_position_history_notes(
            self._position_current_notes,
            self._position_history_notes,
            profile_name=profile_name,
            environment=environment,
            position_history=position_history,
            now_ms=now_ms,
        )
        changed = _prune_closed_current_position_notes(
            self._position_current_notes,
            self._position_history_notes,
            profile_name=profile_name,
            environment=environment,
        ) or changed
        if changed:
            self._save_position_notes()

    def edit_selected_position_note(self) -> None:
        payload = self._selected_position_payload()
        if payload is None or payload.get("kind") != "position":
            messagebox.showinfo("备注", "请先在当前持仓里选中一条具体持仓。", parent=self._positions_zoom_window or self.root)
            return
        position = payload.get("item")
        if not isinstance(position, OkxPosition):
            return
        dialog = PositionNoteEditorDialog(
            self._positions_zoom_window or self.root,
            title="编辑持仓备注",
            prompt=f"为 {position.inst_id} 填写备注。留空后保存会清空当前持仓备注。",
            initial_value=self._current_position_note_text(position),
        )
        if dialog.result_text is None:
            return
        profile_name, environment = self._current_position_note_context()
        record_key = _position_note_current_key(profile_name, environment, position)
        if dialog.result_text:
            previous = self._position_current_notes.get(record_key)
            record = _build_current_position_note_record(
                profile_name=profile_name,
                environment=environment,
                position=position,
                note=dialog.result_text,
                now_ms=_now_epoch_ms(),
                previous=previous,
            )
            if record is not None:
                self._position_current_notes[record_key] = record
                self._save_position_notes()
                self._render_positions_view()
                self._enqueue_log(f"已更新持仓备注：{position.inst_id}")
            return
        if record_key in self._position_current_notes:
            del self._position_current_notes[record_key]
            self._save_position_notes()
            self._render_positions_view()
            self._enqueue_log(f"已清空持仓备注：{position.inst_id}")

    def clear_selected_position_note(self) -> None:
        payload = self._selected_position_payload()
        if payload is None or payload.get("kind") != "position":
            messagebox.showinfo("备注", "请先在当前持仓里选中一条具体持仓。", parent=self._positions_zoom_window or self.root)
            return
        position = payload.get("item")
        if not isinstance(position, OkxPosition):
            return
        profile_name, environment = self._current_position_note_context()
        record_key = _position_note_current_key(profile_name, environment, position)
        if record_key not in self._position_current_notes:
            messagebox.showinfo("备注", "当前持仓还没有备注。", parent=self._positions_zoom_window or self.root)
            return
        del self._position_current_notes[record_key]
        self._save_position_notes()
        self._render_positions_view()
        self._enqueue_log(f"已清空持仓备注：{position.inst_id}")

    def edit_selected_position_history_note(self) -> None:
        item = self._selected_position_history_item()
        if item is None:
            messagebox.showinfo("备注", "请先在历史仓位里选中一条记录。", parent=self._positions_zoom_window or self.root)
            return
        dialog = PositionNoteEditorDialog(
            self._positions_zoom_window or self.root,
            title="编辑历史仓位备注",
            prompt=f"为 {item.inst_id} 的这条历史仓位填写备注。留空后保存会清空历史仓位备注。",
            initial_value=self._position_history_note_text(item),
        )
        if dialog.result_text is None:
            return
        profile_name, environment = self._position_history_note_context()
        record_key = _position_history_note_key(profile_name, environment, item)
        if dialog.result_text:
            previous = self._position_history_notes.get(record_key)
            record = _build_history_position_note_record(
                profile_name=profile_name,
                environment=environment,
                item=item,
                note=dialog.result_text,
                now_ms=_now_epoch_ms(),
                source_current_key=(
                    str(previous.get("source_current_key", ""))
                    if isinstance(previous, dict)
                    else ""
                ),
                previous=previous,
            )
            if record is not None:
                self._position_history_notes[record_key] = record
                self._save_position_notes()
                self._render_positions_zoom_position_history_view()
                self._enqueue_log(f"已更新历史仓位备注：{item.inst_id}")
            return
        if record_key in self._position_history_notes:
            del self._position_history_notes[record_key]
            self._save_position_notes()
            self._render_positions_zoom_position_history_view()
            self._enqueue_log(f"已清空历史仓位备注：{item.inst_id}")

    def clear_selected_position_history_note(self) -> None:
        item = self._selected_position_history_item()
        if item is None:
            messagebox.showinfo("备注", "请先在历史仓位里选中一条记录。", parent=self._positions_zoom_window or self.root)
            return
        profile_name, environment = self._position_history_note_context()
        record_key = _position_history_note_key(profile_name, environment, item)
        if record_key not in self._position_history_notes:
            messagebox.showinfo("备注", "当前历史仓位还没有备注。", parent=self._positions_zoom_window or self.root)
            return
        del self._position_history_notes[record_key]
        self._save_position_notes()
        self._render_positions_zoom_position_history_view()
        self._enqueue_log(f"已清空历史仓位备注：{item.inst_id}")

    def _expand_to_screen(self, window: Toplevel, *, margin: int = 20) -> None:
        try:
            apply_fill_window_geometry(window, min_width=1200, min_height=800, margin=margin)
        except Exception:
            return

    def _schedule_positions_zoom_sync(self, delay_ms: int = 10) -> None:
        if self._positions_zoom_sync_job is not None:
            try:
                self.root.after_cancel(self._positions_zoom_sync_job)
            except Exception:
                pass
            self._positions_zoom_sync_job = None
        self._positions_zoom_sync_job = self.root.after(delay_ms, self._sync_positions_zoom_window)

    def open_positions_zoom_window(self) -> None:
        if self._positions_zoom_window is not None and self._positions_zoom_window.winfo_exists():
            self._positions_zoom_window.focus_force()
            self._schedule_positions_zoom_sync()
            self.refresh_order_views()
            self.refresh_position_histories()
            return

        window = Toplevel(self.root)
        window.title("账户持仓大窗")
        apply_adaptive_window_geometry(
            window,
            width_ratio=0.84,
            height_ratio=0.82,
            min_width=1280,
            min_height=860,
            max_width=1800,
            max_height=1120,
        )
        self._positions_zoom_window = window
        window.protocol("WM_DELETE_WINDOW", self._close_positions_zoom_window)

        container = ttk.Frame(window, padding=12)
        container.pack(fill="both", expand=True)
        container.columnconfigure(0, weight=1)
        container.rowconfigure(2, weight=3)
        container.rowconfigure(3, weight=1)
        container.rowconfigure(4, weight=2)

        header = ttk.Frame(container)
        header.grid(row=0, column=0, sticky="ew", pady=(0, 10))
        header.columnconfigure(1, weight=1)
        zoom_positions_badge = self._create_refresh_badge(
            header,
            self._positions_refresh_badge_text,
            self._positions_refresh_badges,
        )
        zoom_positions_badge.grid(row=0, column=0, sticky="w", padx=(0, 8))
        ttk.Label(header, textvariable=self._positions_zoom_summary_text).grid(row=0, column=1, sticky="w")
        zoom_actions = ttk.Frame(header)
        zoom_actions.grid(row=0, column=2, sticky="e")
        ttk.Button(zoom_actions, text="刷新", command=self.refresh_positions).grid(row=0, column=0, padx=(0, 6))
        ttk.Button(zoom_actions, text="刷新历史", command=self.refresh_position_histories).grid(row=0, column=1, padx=(0, 6))
        ttk.Button(zoom_actions, text="刷新历史成交", command=self.refresh_fill_history).grid(row=0, column=2, padx=(0, 6))
        ttk.Button(zoom_actions, text="账户信息", command=self.open_account_info_window).grid(row=0, column=3, padx=(0, 6))
        ttk.Button(zoom_actions, text="编辑备注", command=self.edit_selected_position_note).grid(row=0, column=4, padx=(0, 6))
        ttk.Button(zoom_actions, text="清空备注", command=self.clear_selected_position_note).grid(row=0, column=5, padx=(0, 6))
        ttk.Button(zoom_actions, text="设置期权保护", command=self.open_position_protection_window).grid(
            row=0, column=6, padx=(0, 6)
        )
        ttk.Button(zoom_actions, text="展期建议", command=self.open_option_roll_window).grid(
            row=0, column=7, padx=(0, 6)
        )
        ttk.Button(zoom_actions, text="关闭", command=self._close_positions_zoom_window).grid(row=0, column=8)
        ttk.Button(zoom_actions, text="列设置", command=self.open_positions_zoom_column_window).grid(
            row=0, column=9, padx=(0, 6)
        )

        ttk.Button(zoom_actions, textvariable=self._positions_zoom_detail_toggle_text, command=self.toggle_positions_zoom_detail).grid(
            row=0, column=10, padx=(0, 6)
        )
        ttk.Button(zoom_actions, textvariable=self._positions_zoom_history_toggle_text, command=self.toggle_positions_zoom_history).grid(
            row=0, column=11
        )
        for column_index, child in enumerate(zoom_actions.winfo_children()):
            child.grid_configure(column=column_index)

        filter_row = ttk.Frame(container)
        filter_row.grid(row=1, column=0, sticky="ew", pady=(0, 10))
        filter_row.columnconfigure(3, weight=1)
        ttk.Label(filter_row, text="类型").grid(row=0, column=0, sticky="w")
        zoom_position_type_combo = ttk.Combobox(
            filter_row,
            textvariable=self.position_type_filter,
            values=list(POSITION_TYPE_OPTIONS.keys()),
            state="readonly",
            width=16,
        )
        zoom_position_type_combo.grid(row=0, column=1, sticky="w", padx=(6, 12))
        zoom_position_type_combo.bind("<<ComboboxSelected>>", self._on_position_filter_changed)
        ttk.Label(filter_row, text="搜索").grid(row=0, column=2, sticky="w")
        zoom_position_keyword_entry = ttk.Entry(filter_row, textvariable=self.position_keyword)
        zoom_position_keyword_entry.grid(row=0, column=3, sticky="ew", padx=(6, 12))
        zoom_position_keyword_entry.bind("<KeyRelease>", self._on_position_filter_changed)
        self._positions_zoom_apply_contract_button = ttk.Button(
            filter_row,
            text="\u5e26\u5165\u5408\u7ea6",
            command=self.apply_selected_option_to_position_search,
        )
        self._positions_zoom_apply_contract_button.grid(row=0, column=4, padx=(0, 6))
        self._positions_zoom_apply_expiry_prefix_button = ttk.Button(
            filter_row,
            text="\u5e26\u5165\u5230\u671f\u524d\u7f00",
            command=self.apply_selected_option_expiry_prefix_to_position_search,
        )
        self._positions_zoom_apply_expiry_prefix_button.grid(row=0, column=5, padx=(0, 6))
        ttk.Button(filter_row, text="应用筛选", command=self._render_positions_view).grid(
            row=0, column=6, padx=(0, 6)
        )
        ttk.Button(filter_row, text="清空筛选", command=self.reset_position_filters).grid(row=0, column=7)
        ttk.Label(
            filter_row,
            textvariable=self._positions_zoom_option_search_hint_text,
            foreground="#6b7280",
        ).grid(row=1, column=2, columnspan=6, sticky="w", pady=(6, 0))

        tree_frame = ttk.Frame(container)
        tree_frame.grid(row=2, column=0, sticky="nsew")
        tree_frame.columnconfigure(0, weight=1)
        tree_frame.rowconfigure(0, weight=1)

        columns = tuple(self.position_tree["columns"])
        zoom_tree = ttk.Treeview(tree_frame, columns=columns, show="tree headings", selectmode="browse")
        self._positions_zoom_tree = zoom_tree
        self._sync_positions_zoom_columns_from_main()
        zoom_tree.grid(row=0, column=0, sticky="nsew")
        zoom_tree.bind("<<TreeviewSelect>>", self._on_positions_zoom_selected)
        zoom_tree.tag_configure("profit", foreground="#13803d")
        zoom_tree.tag_configure("loss", foreground="#c23b3b")
        zoom_tree.tag_configure("group", foreground="#2f3a4a")
        zoom_tree.tag_configure("isolated_mode", background="#fff4e5")
        zoom_tree.tag_configure("cross_mode", background="#f4f8ff")
        zoom_scroll_y = ttk.Scrollbar(tree_frame, orient="vertical", command=zoom_tree.yview)
        zoom_scroll_y.grid(row=0, column=1, sticky="ns")
        zoom_scroll_x = ttk.Scrollbar(tree_frame, orient="horizontal", command=zoom_tree.xview)
        zoom_scroll_x.grid(row=1, column=0, sticky="ew")
        zoom_tree.configure(yscrollcommand=zoom_scroll_y.set, xscrollcommand=zoom_scroll_x.set)
        self._register_positions_zoom_columns("positions", "当前持仓", zoom_tree, columns)

        detail_frame = ttk.LabelFrame(container, text="大窗持仓详情", padding=12)
        detail_frame.grid(row=3, column=0, sticky="nsew")
        self._positions_zoom_detail_frame = detail_frame
        detail_frame.columnconfigure(0, weight=1)
        detail_frame.rowconfigure(0, weight=1)
        self._positions_zoom_detail = Text(
            detail_frame,
            height=10,
            wrap="word",
            font=("Microsoft YaHei UI", 10),
            relief="flat",
        )
        self._positions_zoom_detail.grid(row=0, column=0, sticky="nsew")
        detail_scroll = ttk.Scrollbar(detail_frame, orient="vertical", command=self._positions_zoom_detail.yview)
        detail_scroll.grid(row=0, column=1, sticky="ns")
        self._positions_zoom_detail.configure(yscrollcommand=detail_scroll.set)
        self._set_readonly_text(self._positions_zoom_detail, self.position_detail_text.get())
        self._positions_zoom_summary_text.set("正在打开持仓大窗...")
        self._set_readonly_text(
            self._positions_zoom_detail,
            "大窗已经创建，正在同步当前持仓视图。若你的持仓较多，会在一瞬间完成填充。",
        )
        history_notebook = ttk.Notebook(container)
        history_notebook.grid(row=4, column=0, sticky="nsew", pady=(10, 0))
        self._positions_zoom_notebook = history_notebook

        pending_orders_tab = ttk.Frame(history_notebook, padding=10)
        pending_orders_tab.columnconfigure(0, weight=1)
        pending_orders_tab.rowconfigure(2, weight=1)
        pending_orders_tab.rowconfigure(3, weight=1)
        history_notebook.add(pending_orders_tab, text="当前委托")
        self._build_positions_zoom_pending_orders_tab(pending_orders_tab)

        order_history_tab = ttk.Frame(history_notebook, padding=10)
        order_history_tab.columnconfigure(0, weight=1)
        order_history_tab.rowconfigure(2, weight=1)
        order_history_tab.rowconfigure(3, weight=1)
        history_notebook.add(order_history_tab, text="历史委托")
        self._build_positions_zoom_order_history_tab(order_history_tab)

        fills_tab = ttk.Frame(history_notebook, padding=10)
        fills_tab.columnconfigure(0, weight=1)
        fills_tab.rowconfigure(1, weight=1)
        fills_tab.rowconfigure(2, weight=1)
        history_notebook.add(fills_tab, text="历史成交")
        self._build_positions_zoom_fills_tab(fills_tab)

        position_history_tab = ttk.Frame(history_notebook, padding=10)
        position_history_tab.columnconfigure(0, weight=1)
        position_history_tab.rowconfigure(2, weight=1)
        position_history_tab.rowconfigure(3, weight=1)
        history_notebook.add(position_history_tab, text="历史仓位")
        self._build_positions_zoom_position_history_tab(position_history_tab)
        if not self._positions_zoom_detail_collapsed:
            self.toggle_positions_zoom_detail()
        if not self._positions_zoom_pending_orders_detail_collapsed:
            self.toggle_positions_zoom_pending_orders_detail()
        if not self._positions_zoom_order_history_detail_collapsed:
            self.toggle_positions_zoom_order_history_detail()
        if not self._positions_zoom_fills_detail_collapsed:
            self.toggle_positions_zoom_fills_detail()
        if not self._positions_zoom_position_history_detail_collapsed:
            self.toggle_positions_zoom_position_history_detail()
        self.refresh_order_views()
        self.refresh_position_histories()
        self._expand_to_screen(window)
        self._refresh_all_refresh_badges()
        self._update_positions_zoom_search_shortcuts()
        self._update_position_history_search_shortcuts()
        self._schedule_positions_zoom_sync(30)

    def _build_positions_zoom_pending_orders_tab(self, parent: ttk.Frame) -> None:
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
        ttk.Button(header, text="撤单选中", command=lambda: self.cancel_selected_pending_order("positions_zoom")).grid(
            row=0, column=3, sticky="e", padx=(0, 6)
        )
        ttk.Button(header, text="批量撤当前筛选", command=lambda: self.cancel_filtered_pending_orders("positions_zoom")).grid(
            row=0, column=4, sticky="e", padx=(0, 6)
        )
        ttk.Button(
            header,
            textvariable=self._positions_zoom_pending_orders_detail_toggle_text,
            command=self.toggle_positions_zoom_pending_orders_detail,
        ).grid(row=0, column=5, sticky="e")

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

        columns = ("time", "source", "inst_type", "inst_id", "state", "side", "ord_type", "price", "size", "filled", "tp_sl", "order_id", "cl_ord_id")
        tree = ttk.Treeview(tree_frame, columns=columns, show="headings", selectmode="browse")
        self._positions_zoom_pending_orders_tree = tree
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
            ("tp_sl", 180),
            ("order_id", 120),
            ("cl_ord_id", 150),
        ):
            tree.heading(column_id, text=headings[column_id])
            tree.column(column_id, width=width, anchor="e" if column_id in {"price", "size", "filled"} else "center")
        tree.column("inst_id", anchor="w")
        tree.column("tp_sl", anchor="w")
        tree.column("cl_ord_id", anchor="w")
        tree.grid(row=0, column=0, sticky="nsew")
        tree.bind("<<TreeviewSelect>>", self._on_pending_orders_selected)
        tree.tag_configure("profit", foreground="#13803d")
        tree.tag_configure("loss", foreground="#c23b3b")
        scroll_y = ttk.Scrollbar(tree_frame, orient="vertical", command=tree.yview)
        scroll_y.grid(row=0, column=1, sticky="ns")
        scroll_x = ttk.Scrollbar(tree_frame, orient="horizontal", command=tree.xview)
        scroll_x.grid(row=1, column=0, sticky="ew")
        tree.configure(yscrollcommand=scroll_y.set, xscrollcommand=scroll_x.set)
        self._register_positions_zoom_columns("pending_orders", "当前委托", tree, columns)

        detail_frame = ttk.LabelFrame(parent, text="委托详情", padding=12)
        detail_frame.grid(row=3, column=0, sticky="nsew", pady=(10, 0))
        self._positions_zoom_pending_orders_detail_frame = detail_frame
        detail_frame.columnconfigure(0, weight=1)
        detail_frame.rowconfigure(0, weight=1)
        self._positions_zoom_pending_orders_detail = Text(detail_frame, height=8, wrap="word", font=("Microsoft YaHei UI", 10), relief="flat")
        self._positions_zoom_pending_orders_detail.grid(row=0, column=0, sticky="nsew")
        detail_scroll = ttk.Scrollbar(detail_frame, orient="vertical", command=self._positions_zoom_pending_orders_detail.yview)
        detail_scroll.grid(row=0, column=1, sticky="ns")
        self._positions_zoom_pending_orders_detail.configure(yscrollcommand=detail_scroll.set)
        self._set_readonly_text(self._positions_zoom_pending_orders_detail, "这里会显示选中当前委托的详情。")

    def _build_positions_zoom_order_history_tab(self, parent: ttk.Frame) -> None:
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
        ttk.Button(header, text="刷新", command=self.refresh_order_history).grid(row=0, column=2, sticky="e", padx=(0, 6))
        ttk.Button(
            header,
            textvariable=self._positions_zoom_order_history_detail_toggle_text,
            command=self.toggle_positions_zoom_order_history_detail,
        ).grid(row=0, column=3, sticky="e")

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

        columns = ("time", "source", "inst_type", "inst_id", "state", "side", "ord_type", "price", "size", "filled", "tp_sl", "order_id", "cl_ord_id")
        tree = ttk.Treeview(tree_frame, columns=columns, show="headings", selectmode="browse")
        self._positions_zoom_order_history_tree = tree
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
            ("tp_sl", 180),
            ("order_id", 120),
            ("cl_ord_id", 150),
        ):
            tree.heading(column_id, text=headings[column_id])
            tree.column(column_id, width=width, anchor="e" if column_id in {"price", "size", "filled"} else "center")
        tree.column("inst_id", anchor="w")
        tree.column("tp_sl", anchor="w")
        tree.column("cl_ord_id", anchor="w")
        tree.grid(row=0, column=0, sticky="nsew")
        tree.bind("<<TreeviewSelect>>", self._on_order_history_selected)
        tree.tag_configure("profit", foreground="#13803d")
        tree.tag_configure("loss", foreground="#c23b3b")
        scroll_y = ttk.Scrollbar(tree_frame, orient="vertical", command=tree.yview)
        scroll_y.grid(row=0, column=1, sticky="ns")
        scroll_x = ttk.Scrollbar(tree_frame, orient="horizontal", command=tree.xview)
        scroll_x.grid(row=1, column=0, sticky="ew")
        tree.configure(yscrollcommand=scroll_y.set, xscrollcommand=scroll_x.set)
        self._register_positions_zoom_columns("order_history", "历史委托", tree, columns)

        detail_frame = ttk.LabelFrame(parent, text="委托详情", padding=12)
        detail_frame.grid(row=3, column=0, sticky="nsew", pady=(10, 0))
        self._positions_zoom_order_history_detail_frame = detail_frame
        detail_frame.columnconfigure(0, weight=1)
        detail_frame.rowconfigure(0, weight=1)
        self._positions_zoom_order_history_detail = Text(detail_frame, height=8, wrap="word", font=("Microsoft YaHei UI", 10), relief="flat")
        self._positions_zoom_order_history_detail.grid(row=0, column=0, sticky="nsew")
        detail_scroll = ttk.Scrollbar(detail_frame, orient="vertical", command=self._positions_zoom_order_history_detail.yview)
        detail_scroll.grid(row=0, column=1, sticky="ns")
        self._positions_zoom_order_history_detail.configure(yscrollcommand=detail_scroll.set)
        self._set_readonly_text(self._positions_zoom_order_history_detail, "这里会显示选中历史委托的详情。")

    def _build_positions_zoom_fills_tab(self, parent: ttk.Frame) -> None:
        header = ttk.Frame(parent)
        header.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        header.columnconfigure(0, weight=1)
        ttk.Label(header, textvariable=self._positions_zoom_fills_summary_text).grid(row=0, column=0, sticky="w")
        ttk.Button(
            header,
            textvariable=self._positions_zoom_fills_load_more_text,
            command=self.expand_fill_history_limit,
        ).grid(row=0, column=1, sticky="e", padx=(0, 6))
        ttk.Button(header, textvariable=self._positions_zoom_fills_detail_toggle_text, command=self.toggle_positions_zoom_fills_detail).grid(
            row=0, column=2, sticky="e"
        )
        filter_row = ttk.Frame(parent)
        filter_row.grid(row=1, column=0, sticky="ew", pady=(0, 8))
        filter_row.columnconfigure(9, weight=1)
        ttk.Label(filter_row, text="类型").grid(row=0, column=0, sticky="w")
        type_combo = ttk.Combobox(
            filter_row,
            textvariable=self.fill_history_type_filter,
            values=list(POSITION_TYPE_OPTIONS.keys()),
            state="readonly",
            width=16,
        )
        type_combo.grid(row=0, column=1, sticky="w", padx=(6, 12))
        type_combo.bind("<<ComboboxSelected>>", self._on_fill_history_filter_changed)
        ttk.Label(filter_row, text="方向").grid(row=0, column=2, sticky="w")
        side_combo = ttk.Combobox(
            filter_row,
            textvariable=self.fill_history_side_filter,
            values=list(HISTORY_FILL_SIDE_FILTER_OPTIONS.keys()),
            state="readonly",
            width=16,
        )
        side_combo.grid(row=0, column=3, sticky="w", padx=(6, 12))
        side_combo.bind("<<ComboboxSelected>>", self._on_fill_history_filter_changed)
        ttk.Label(filter_row, text="标的").grid(row=0, column=4, sticky="w")
        asset_entry = ttk.Entry(filter_row, textvariable=self.fill_history_asset_filter, width=10)
        asset_entry.grid(row=0, column=5, sticky="w", padx=(6, 12))
        asset_entry.bind("<KeyRelease>", self._on_fill_history_filter_changed)
        ttk.Label(filter_row, text="到期前缀").grid(row=0, column=6, sticky="w")
        expiry_entry = ttk.Entry(filter_row, textvariable=self.fill_history_expiry_prefix_filter, width=14)
        expiry_entry.grid(row=0, column=7, sticky="w", padx=(6, 12))
        expiry_entry.bind("<KeyRelease>", self._on_fill_history_filter_changed)
        ttk.Label(filter_row, text="搜索").grid(row=0, column=8, sticky="w")
        keyword_entry = ttk.Entry(filter_row, textvariable=self.fill_history_keyword)
        keyword_entry.grid(row=0, column=9, sticky="ew", padx=(6, 12))
        keyword_entry.bind("<KeyRelease>", self._on_fill_history_filter_changed)
        self._positions_zoom_fills_apply_contract_button = ttk.Button(
            filter_row,
            text="\u5e26\u5165\u5408\u7ea6",
            command=self.apply_selected_option_to_fill_history_search,
        )
        self._positions_zoom_fills_apply_contract_button.grid(row=0, column=10, padx=(0, 6))
        self._positions_zoom_fills_apply_expiry_prefix_button = ttk.Button(
            filter_row,
            text="\u5e26\u5165\u5230\u671f\u524d\u7f00",
            command=self.apply_selected_option_expiry_prefix_to_fill_history_search,
        )
        self._positions_zoom_fills_apply_expiry_prefix_button.grid(row=0, column=11, padx=(0, 6))
        ttk.Button(filter_row, text="应用筛选", command=self._render_positions_zoom_fills_view).grid(
            row=0, column=12, padx=(0, 6)
        )
        ttk.Button(filter_row, text="清空筛选", command=self.reset_fill_history_filters).grid(row=0, column=13)
        ttk.Label(
            filter_row,
            textvariable=self._positions_zoom_fill_history_search_hint_text,
            foreground="#6b7280",
        ).grid(row=1, column=8, columnspan=6, sticky="w", pady=(6, 0))
        tree_frame = ttk.Frame(parent)
        tree_frame.grid(row=2, column=0, sticky="nsew")
        tree_frame.columnconfigure(0, weight=1)
        tree_frame.rowconfigure(0, weight=1)

        columns = ("time", "inst_type", "inst_id", "side", "price", "size", "fee", "pnl", "exec_type")
        tree = ttk.Treeview(tree_frame, columns=columns, show="headings", selectmode="browse")
        self._positions_zoom_fills_tree = tree
        headings = {
            "time": "时间",
            "inst_type": "类型",
            "inst_id": "合约",
            "side": "方向",
            "price": "成交价",
            "size": "成交量",
            "fee": "手续费",
            "pnl": "已实现盈亏",
            "exec_type": "成交类型",
            "realized_usdt": "鎶樺悎USDT",
        }
        for column_id, width in (
            ("time", 150),
            ("inst_type", 72),
            ("inst_id", 240),
            ("side", 96),
            ("price", 100),
            ("size", 100),
            ("fee", 100),
            ("pnl", 110),
            ("exec_type", 108),
        ):
            tree.heading(column_id, text=headings[column_id])
            tree.column(column_id, width=width, anchor="e" if column_id in {"price", "size", "fee", "pnl"} else "center")
        tree.column("inst_id", anchor="w")
        tree.grid(row=0, column=0, sticky="nsew")
        tree.bind("<<TreeviewSelect>>", self._on_positions_zoom_fills_selected)
        tree.tag_configure("profit", foreground="#13803d")
        tree.tag_configure("loss", foreground="#c23b3b")
        scroll_y = ttk.Scrollbar(tree_frame, orient="vertical", command=tree.yview)
        scroll_y.grid(row=0, column=1, sticky="ns")
        scroll_x = ttk.Scrollbar(tree_frame, orient="horizontal", command=tree.xview)
        scroll_x.grid(row=1, column=0, sticky="ew")
        tree.configure(yscrollcommand=scroll_y.set, xscrollcommand=scroll_x.set)
        self._register_positions_zoom_columns("fills", "历史成交", tree, columns)

        detail_frame = ttk.LabelFrame(parent, text="成交详情", padding=12)
        detail_frame.grid(row=3, column=0, sticky="nsew", pady=(10, 0))
        self._positions_zoom_fills_detail_frame = detail_frame
        detail_frame.columnconfigure(0, weight=1)
        detail_frame.rowconfigure(0, weight=1)
        self._positions_zoom_fills_detail = Text(detail_frame, height=8, wrap="word", font=("Microsoft YaHei UI", 10), relief="flat")
        self._positions_zoom_fills_detail.grid(row=0, column=0, sticky="nsew")
        detail_scroll = ttk.Scrollbar(detail_frame, orient="vertical", command=self._positions_zoom_fills_detail.yview)
        detail_scroll.grid(row=0, column=1, sticky="ns")
        self._positions_zoom_fills_detail.configure(yscrollcommand=detail_scroll.set)
        self._set_readonly_text(self._positions_zoom_fills_detail, "这里会显示选中历史成交单的详情。")

    def _build_positions_zoom_position_history_tab(self, parent: ttk.Frame) -> None:
        header = ttk.Frame(parent)
        header.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        header.columnconfigure(0, weight=1)
        ttk.Label(header, textvariable=self._positions_zoom_position_history_summary_text).grid(row=0, column=0, sticky="w")
        ttk.Button(
            header,
            textvariable=self._positions_zoom_position_history_load_more_text,
            command=self.expand_position_history_limit,
        ).grid(row=0, column=1, sticky="e", padx=(0, 6))
        ttk.Button(header, text="编辑备注", command=self.edit_selected_position_history_note).grid(
            row=0, column=2, sticky="e", padx=(0, 6)
        )
        ttk.Button(header, text="清空备注", command=self.clear_selected_position_history_note).grid(
            row=0, column=3, sticky="e", padx=(0, 6)
        )
        ttk.Button(
            header,
            textvariable=self._positions_zoom_position_history_detail_toggle_text,
            command=self.toggle_positions_zoom_position_history_detail,
        ).grid(row=0, column=4, sticky="e")
        filter_row = ttk.Frame(parent)
        filter_row.grid(row=1, column=0, sticky="ew", pady=(0, 8))
        filter_row.columnconfigure(9, weight=1)
        ttk.Label(filter_row, text="类型").grid(row=0, column=0, sticky="w")
        type_combo = ttk.Combobox(
            filter_row,
            textvariable=self.position_history_type_filter,
            values=list(POSITION_TYPE_OPTIONS.keys()),
            state="readonly",
            width=16,
        )
        type_combo.grid(row=0, column=1, sticky="w", padx=(6, 12))
        type_combo.bind("<<ComboboxSelected>>", self._on_position_history_filter_changed)
        ttk.Label(filter_row, text="保证金模式").grid(row=0, column=2, sticky="w")
        margin_combo = ttk.Combobox(
            filter_row,
            textvariable=self.position_history_margin_filter,
            values=list(HISTORY_MARGIN_MODE_FILTER_OPTIONS.keys()),
            state="readonly",
            width=16,
        )
        margin_combo.grid(row=0, column=3, sticky="w", padx=(6, 12))
        margin_combo.bind("<<ComboboxSelected>>", self._on_position_history_filter_changed)
        ttk.Label(filter_row, text="标的").grid(row=0, column=4, sticky="w")
        asset_entry = ttk.Entry(filter_row, textvariable=self.position_history_asset_filter, width=10)
        asset_entry.grid(row=0, column=5, sticky="w", padx=(6, 12))
        asset_entry.bind("<KeyRelease>", self._on_position_history_filter_changed)
        ttk.Label(filter_row, text="到期前缀").grid(row=0, column=6, sticky="w")
        expiry_entry = ttk.Entry(filter_row, textvariable=self.position_history_expiry_prefix_filter, width=14)
        expiry_entry.grid(row=0, column=7, sticky="w", padx=(6, 12))
        expiry_entry.bind("<KeyRelease>", self._on_position_history_filter_changed)
        ttk.Label(filter_row, text="搜索").grid(row=0, column=8, sticky="w")
        keyword_entry = ttk.Entry(filter_row, textvariable=self.position_history_keyword)
        keyword_entry.grid(row=0, column=9, sticky="ew", padx=(6, 12))
        keyword_entry.bind("<KeyRelease>", self._on_position_history_filter_changed)
        self._positions_zoom_position_history_apply_contract_button = ttk.Button(
            filter_row,
            text="\u5e26\u5165\u5408\u7ea6",
            command=self.apply_selected_option_to_position_history_search,
        )
        self._positions_zoom_position_history_apply_contract_button.grid(row=0, column=10, padx=(0, 6))
        self._positions_zoom_position_history_apply_expiry_prefix_button = ttk.Button(
            filter_row,
            text="\u5e26\u5165\u5230\u671f\u524d\u7f00",
            command=self.apply_selected_option_expiry_prefix_to_position_history_search,
        )
        self._positions_zoom_position_history_apply_expiry_prefix_button.grid(row=0, column=11, padx=(0, 6))
        ttk.Button(filter_row, text="应用筛选", command=self._render_positions_zoom_position_history_view).grid(
            row=0, column=12, padx=(0, 6)
        )
        ttk.Button(filter_row, text="清空筛选", command=self.reset_position_history_filters).grid(row=0, column=13)
        ttk.Label(
            filter_row,
            textvariable=self._positions_zoom_position_history_search_hint_text,
            foreground="#6b7280",
        ).grid(row=1, column=8, columnspan=6, sticky="w", pady=(6, 0))
        tree_frame = ttk.Frame(parent)
        tree_frame.grid(row=2, column=0, sticky="nsew")
        tree_frame.columnconfigure(0, weight=1)
        tree_frame.rowconfigure(0, weight=1)

        columns = (
            "time",
            "inst_type",
            "inst_id",
            "mgn_mode",
            "side",
            "open_avg",
            "close_avg",
            "close_size",
            "pnl",
            "realized",
            "realized_usdt",
            "note",
        )
        tree = ttk.Treeview(tree_frame, columns=columns, show="headings", selectmode="browse")
        self._positions_zoom_position_history_tree = tree
        headings = {
            "time": "更新时间",
            "inst_type": "类型",
            "inst_id": "合约",
            "mgn_mode": "保证金模式",
            "side": "方向",
            "open_avg": "开仓均价",
            "close_avg": "平仓均价",
            "close_size": "平仓数量",
            "pnl": "盈亏",
            "realized": "已实现盈亏",
            "note": "备注",
        }
        headings["realized_usdt"] = "折合USDT"
        for column_id, width in (
            ("time", 150),
            ("inst_type", 72),
            ("inst_id", 240),
            ("mgn_mode", 96),
            ("side", 96),
            ("open_avg", 100),
            ("close_avg", 100),
            ("close_size", 100),
            ("pnl", 100),
            ("realized", 110),
            ("realized_usdt", 110),
            ("note", 220),
        ):
            tree.heading(column_id, text=headings[column_id])
            tree.column(
                column_id,
                width=width,
                anchor="e" if column_id in {"open_avg", "close_avg", "close_size", "pnl", "realized", "realized_usdt"} else "center",
            )
        tree.column("inst_id", anchor="w")
        tree.column("note", anchor="w")
        tree.grid(row=0, column=0, sticky="nsew")
        tree.bind("<<TreeviewSelect>>", self._on_positions_zoom_position_history_selected)
        tree.tag_configure("profit", foreground="#13803d")
        tree.tag_configure("loss", foreground="#c23b3b")
        scroll_y = ttk.Scrollbar(tree_frame, orient="vertical", command=tree.yview)
        scroll_y.grid(row=0, column=1, sticky="ns")
        scroll_x = ttk.Scrollbar(tree_frame, orient="horizontal", command=tree.xview)
        scroll_x.grid(row=1, column=0, sticky="ew")
        tree.configure(yscrollcommand=scroll_y.set, xscrollcommand=scroll_x.set)
        self._register_positions_zoom_columns("position_history", "历史仓位", tree, columns)

        detail_frame = ttk.LabelFrame(parent, text="历史仓位详情", padding=12)
        detail_frame.grid(row=3, column=0, sticky="nsew", pady=(10, 0))
        self._positions_zoom_position_history_detail_frame = detail_frame
        detail_frame.columnconfigure(0, weight=1)
        detail_frame.rowconfigure(0, weight=1)
        self._positions_zoom_position_history_detail = Text(
            detail_frame,
            height=8,
            wrap="word",
            font=("Microsoft YaHei UI", 10),
            relief="flat",
        )
        self._positions_zoom_position_history_detail.grid(row=0, column=0, sticky="nsew")
        detail_scroll = ttk.Scrollbar(detail_frame, orient="vertical", command=self._positions_zoom_position_history_detail.yview)
        detail_scroll.grid(row=0, column=1, sticky="ns")
        self._positions_zoom_position_history_detail.configure(yscrollcommand=detail_scroll.set)
        self._set_readonly_text(self._positions_zoom_position_history_detail, "这里会显示选中历史仓位的详情。")

    def _close_positions_zoom_window(self) -> None:
        if self._positions_zoom_sync_job is not None:
            try:
                self.root.after_cancel(self._positions_zoom_sync_job)
            except Exception:
                pass
        self._positions_zoom_sync_job = None
        if self._positions_zoom_column_window is not None and self._positions_zoom_column_window.winfo_exists():
            self._positions_zoom_column_window.destroy()
        self._positions_zoom_column_window = None
        if self._positions_zoom_window is not None and self._positions_zoom_window.winfo_exists():
            self._positions_zoom_window.destroy()
        self._positions_zoom_window = None
        self._positions_zoom_tree = None
        self._positions_zoom_detail = None
        self._positions_zoom_notebook = None
        self._positions_zoom_selection_suppressed_item_id = None
        self._positions_zoom_pending_orders_tree = None
        self._positions_zoom_pending_orders_detail = None
        self._positions_zoom_order_history_tree = None
        self._positions_zoom_order_history_detail = None
        self._positions_zoom_fills_tree = None
        self._positions_zoom_fills_detail = None
        self._positions_zoom_position_history_tree = None
        self._positions_zoom_position_history_detail = None
        self._positions_zoom_column_groups = {}
        self._positions_zoom_column_vars = {}
        self._position_history_usdt_prices = {}
        self._positions_zoom_detail_frame = None
        self._positions_zoom_pending_orders_detail_frame = None
        self._positions_zoom_order_history_detail_frame = None
        self._positions_zoom_fills_detail_frame = None
        self._positions_zoom_position_history_detail_frame = None
        self._positions_zoom_selected_item_id = None
        self._positions_zoom_apply_contract_button = None
        self._positions_zoom_apply_expiry_prefix_button = None
        self._positions_zoom_fills_apply_contract_button = None
        self._positions_zoom_fills_apply_expiry_prefix_button = None
        self._positions_zoom_position_history_apply_contract_button = None
        self._positions_zoom_position_history_apply_expiry_prefix_button = None
        self._pending_order_canceling = False
        self._positions_zoom_detail_collapsed = False
        self._positions_zoom_history_collapsed = False
        self._positions_zoom_pending_orders_detail_collapsed = False
        self._positions_zoom_order_history_detail_collapsed = False
        self._positions_zoom_fills_detail_collapsed = False
        self._positions_zoom_position_history_detail_collapsed = False
        self._positions_zoom_detail_toggle_text.set("\u5c55\u5f00\u6301\u4ed3\u8be6\u60c5")
        self._positions_zoom_pending_orders_detail_toggle_text.set("\u5c55\u5f00\u59d4\u6258\u8be6\u60c5")
        self._positions_zoom_order_history_detail_toggle_text.set("\u5c55\u5f00\u59d4\u6258\u8be6\u60c5")
        self._positions_zoom_fills_detail_toggle_text.set("\u5c55\u5f00\u6210\u4ea4\u8be6\u60c5")
        self._positions_zoom_position_history_detail_toggle_text.set("\u5c55\u5f00\u4ed3\u4f4d\u8be6\u60c5")
        self._positions_zoom_detail_toggle_text.set("折叠持仓详情")
        self._positions_zoom_history_toggle_text.set("折叠历史区域")
        self._positions_zoom_pending_orders_detail_toggle_text.set("折叠委托详情")
        self._positions_zoom_order_history_detail_toggle_text.set("折叠委托详情")
        self._positions_zoom_fills_detail_toggle_text.set("折叠成交详情")
        self._positions_zoom_position_history_detail_toggle_text.set("折叠仓位详情")
        self._positions_zoom_fills_load_more_text.set("增加100条")
        self._positions_zoom_position_history_load_more_text.set("增加100条")
        self._fill_history_fetch_limit = 100
        self._fill_history_load_more_clicks = 0
        self._position_history_fetch_limit = 100
        self._position_history_load_more_clicks = 0
        self._positions_zoom_option_search_hint_text.set("选中期权后，可一键带入合约或到期前缀。")
        self._positions_zoom_position_history_search_hint_text.set("选中历史期权后，可一键带入合约或到期前缀。")

    def _register_positions_zoom_columns(
        self,
        group_key: str,
        title: str,
        tree: ttk.Treeview,
        columns: tuple[str, ...],
    ) -> None:
        default_visible_columns = POSITIONS_ZOOM_DEFAULT_VISIBLE_COLUMNS.get(group_key)
        if default_visible_columns:
            tree.configure(
                displaycolumns=tuple(column_id for column_id in columns if column_id in default_visible_columns)
            )
        self._positions_zoom_column_groups[group_key] = {
            "title": title,
            "tree": tree,
            "columns": tuple(columns),
            "headings": {column_id: tree.heading(column_id).get("text", column_id) for column_id in columns},
        }

    def open_positions_zoom_column_window(self) -> None:
        if self._positions_zoom_window is None or not self._positions_zoom_window.winfo_exists():
            return
        if self._positions_zoom_column_window is not None and self._positions_zoom_column_window.winfo_exists():
            self._positions_zoom_column_window.focus_force()
            return

        window = Toplevel(self._positions_zoom_window)
        window.title("持仓大窗列设置")
        apply_adaptive_window_geometry(
            window,
            width_ratio=0.34,
            height_ratio=0.48,
            min_width=480,
            min_height=420,
            max_width=700,
            max_height=760,
        )
        window.transient(self._positions_zoom_window)
        self._positions_zoom_column_window = window
        window.protocol("WM_DELETE_WINDOW", self._close_positions_zoom_column_window)

        container = ttk.Frame(window, padding=12)
        container.pack(fill="both", expand=True)
        container.columnconfigure(0, weight=1)
        container.rowconfigure(1, weight=1)

        ttk.Label(
            container,
            text="可按区域勾选显示/隐藏列。'合约/分组' 为结构列，当前固定显示。",
        ).grid(row=0, column=0, sticky="w", pady=(0, 10))

        notebook = ttk.Notebook(container)
        notebook.grid(row=1, column=0, sticky="nsew")

        self._positions_zoom_column_vars = {}
        group_order = ("positions", "pending_orders", "order_history", "fills", "position_history")
        for group_key in group_order:
            group = self._positions_zoom_column_groups.get(group_key)
            if not group:
                continue
            title = str(group["title"])
            columns = tuple(group["columns"])
            headings = dict(group["headings"])
            tree = group["tree"]
            if not isinstance(tree, ttk.Treeview):
                continue
            visible_columns = set(_tree_display_columns(tree, columns))
            tab = ttk.Frame(notebook, padding=12)
            tab.columnconfigure(0, weight=1)
            notebook.add(tab, text=title)

            actions = ttk.Frame(tab)
            actions.grid(row=0, column=0, sticky="ew", pady=(0, 8))
            actions.columnconfigure(0, weight=1)
            ttk.Label(actions, text=f"{title} 列").grid(row=0, column=0, sticky="w")
            ttk.Button(
                actions,
                text="全部显示",
                command=lambda key=group_key: self._set_positions_zoom_columns_visible(key, True),
            ).grid(row=0, column=1, padx=(0, 6))
            ttk.Button(
                actions,
                text="恢复默认",
                command=lambda key=group_key: self._reset_positions_zoom_columns(key),
            ).grid(row=0, column=2)

            checks = ttk.Frame(tab)
            checks.grid(row=1, column=0, sticky="nsew")
            for column_index in range(2):
                checks.columnconfigure(column_index, weight=1)

            group_vars: dict[str, BooleanVar] = {}
            for index, column_id in enumerate(columns):
                var = BooleanVar(value=column_id in visible_columns)
                group_vars[column_id] = var
                ttk.Checkbutton(
                    checks,
                    text=headings.get(column_id, column_id),
                    variable=var,
                    command=lambda key=group_key: self._apply_positions_zoom_column_visibility(key),
                ).grid(row=index // 2, column=index % 2, sticky="w", padx=(0, 12), pady=4)
            self._positions_zoom_column_vars[group_key] = group_vars

    def _close_positions_zoom_column_window(self) -> None:
        if self._positions_zoom_column_window is not None and self._positions_zoom_column_window.winfo_exists():
            self._positions_zoom_column_window.destroy()
        self._positions_zoom_column_window = None

    def _apply_positions_zoom_column_visibility(self, group_key: str) -> None:
        group = self._positions_zoom_column_groups.get(group_key)
        variables = self._positions_zoom_column_vars.get(group_key)
        if not group or not variables:
            return
        tree = group["tree"]
        columns = tuple(group["columns"])
        if not isinstance(tree, ttk.Treeview):
            return
        visible_columns = [column_id for column_id in columns if variables[column_id].get()]
        if not visible_columns:
            fallback = columns[0]
            variables[fallback].set(True)
            visible_columns = [fallback]
        tree.configure(displaycolumns=tuple(visible_columns))

    def _set_positions_zoom_columns_visible(self, group_key: str, visible: bool) -> None:
        variables = self._positions_zoom_column_vars.get(group_key)
        group = self._positions_zoom_column_groups.get(group_key)
        if not variables or not group:
            return
        columns = tuple(group["columns"])
        if not columns:
            return
        for column_id in columns:
            variables[column_id].set(visible)
        if not visible:
            variables[columns[0]].set(True)
        self._apply_positions_zoom_column_visibility(group_key)

    def _reset_positions_zoom_columns(self, group_key: str) -> None:
        variables = self._positions_zoom_column_vars.get(group_key)
        group = self._positions_zoom_column_groups.get(group_key)
        if not variables or not group:
            return
        columns = tuple(group["columns"])
        default_visible_columns = POSITIONS_ZOOM_DEFAULT_VISIBLE_COLUMNS.get(group_key)
        if default_visible_columns:
            default_set = set(default_visible_columns)
            for column_id in columns:
                variables[column_id].set(column_id in default_set)
            self._apply_positions_zoom_column_visibility(group_key)
            return
        self._set_positions_zoom_columns_visible(group_key, True)

    def toggle_positions_zoom_detail(self) -> None:
        if self._positions_zoom_detail_frame is None:
            return
        if self._positions_zoom_detail_collapsed:
            self._positions_zoom_detail_frame.grid()
            self._positions_zoom_detail_toggle_text.set("折叠持仓详情")
        else:
            self._positions_zoom_detail_frame.grid_remove()
            self._positions_zoom_detail_toggle_text.set("展开持仓详情")
        self._positions_zoom_detail_collapsed = not self._positions_zoom_detail_collapsed

    def toggle_positions_zoom_history(self) -> None:
        if self._positions_zoom_notebook is None:
            return
        if self._positions_zoom_history_collapsed:
            self._positions_zoom_notebook.grid()
            self._positions_zoom_history_toggle_text.set("折叠历史区域")
        else:
            self._positions_zoom_notebook.grid_remove()
            self._positions_zoom_history_toggle_text.set("展开历史区域")
        self._positions_zoom_history_collapsed = not self._positions_zoom_history_collapsed

    def toggle_positions_zoom_pending_orders_detail(self) -> None:
        if self._positions_zoom_pending_orders_detail_frame is None:
            return
        if self._positions_zoom_pending_orders_detail_collapsed:
            self._positions_zoom_pending_orders_detail_frame.grid()
            self._positions_zoom_pending_orders_detail_toggle_text.set("折叠委托详情")
        else:
            self._positions_zoom_pending_orders_detail_frame.grid_remove()
            self._positions_zoom_pending_orders_detail_toggle_text.set("展开委托详情")
        self._positions_zoom_pending_orders_detail_collapsed = not self._positions_zoom_pending_orders_detail_collapsed

    def toggle_positions_zoom_order_history_detail(self) -> None:
        if self._positions_zoom_order_history_detail_frame is None:
            return
        if self._positions_zoom_order_history_detail_collapsed:
            self._positions_zoom_order_history_detail_frame.grid()
            self._positions_zoom_order_history_detail_toggle_text.set("折叠委托详情")
        else:
            self._positions_zoom_order_history_detail_frame.grid_remove()
            self._positions_zoom_order_history_detail_toggle_text.set("展开委托详情")
        self._positions_zoom_order_history_detail_collapsed = not self._positions_zoom_order_history_detail_collapsed

    def toggle_positions_zoom_fills_detail(self) -> None:
        if self._positions_zoom_fills_detail_frame is None:
            return
        if self._positions_zoom_fills_detail_collapsed:
            self._positions_zoom_fills_detail_frame.grid()
            self._positions_zoom_fills_detail_toggle_text.set("折叠成交详情")
        else:
            self._positions_zoom_fills_detail_frame.grid_remove()
            self._positions_zoom_fills_detail_toggle_text.set("展开成交详情")
        self._positions_zoom_fills_detail_collapsed = not self._positions_zoom_fills_detail_collapsed

    def toggle_positions_zoom_position_history_detail(self) -> None:
        if self._positions_zoom_position_history_detail_frame is None:
            return
        if self._positions_zoom_position_history_detail_collapsed:
            self._positions_zoom_position_history_detail_frame.grid()
            self._positions_zoom_position_history_detail_toggle_text.set("折叠仓位详情")
        else:
            self._positions_zoom_position_history_detail_frame.grid_remove()
            self._positions_zoom_position_history_detail_toggle_text.set("展开仓位详情")
        self._positions_zoom_position_history_detail_collapsed = not self._positions_zoom_position_history_detail_collapsed

    def _sync_positions_zoom_window(self) -> None:
        self._positions_zoom_sync_job = None
        if (
            self._positions_zoom_window is None
            or not self._positions_zoom_window.winfo_exists()
            or self._positions_zoom_tree is None
        ):
            return

        self._positions_zoom_summary_text.set(self.positions_summary_text.get())
        zoom_tree = self._positions_zoom_tree
        self._sync_positions_zoom_columns_from_main()
        zoom_tree.delete(*zoom_tree.get_children())

        def _copy_branch(source_parent: str, target_parent: str) -> None:
            for item_id in self.position_tree.get_children(source_parent):
                item = self.position_tree.item(item_id)
                zoom_tree.insert(
                    target_parent,
                    END,
                    iid=item_id,
                    text=item.get("text", ""),
                    values=item.get("values", ()),
                    open=bool(item.get("open")),
                    tags=item.get("tags", ()),
                )
                _copy_branch(item_id, item_id)

        _copy_branch("", "")
        selected = self.position_tree.selection()
        if selected and zoom_tree.exists(selected[0]):
            if zoom_tree.selection() != (selected[0],):
                self._positions_zoom_selection_suppressed_item_id = selected[0]
                self._position_selection_syncing = True
                try:
                    zoom_tree.selection_set(selected[0])
                finally:
                    self._position_selection_syncing = False
            try:
                zoom_tree.see(selected[0])
            except Exception:
                pass
            self._positions_zoom_selected_item_id = selected[0]
        else:
            self._positions_zoom_selected_item_id = None
        self._refresh_positions_zoom_detail()
        self._update_positions_zoom_search_shortcuts()

    def _sync_position_tree_selection(self, item_id: str) -> None:
        if self.position_tree is None or not self.position_tree.exists(item_id):
            return
        if self.position_tree.selection() == (item_id,):
            return
        self._position_selection_suppressed_item_id = item_id
        self._position_selection_syncing = True
        try:
            self.position_tree.selection_set(item_id)
        finally:
            self._position_selection_syncing = False
        try:
            self.position_tree.see(item_id)
        except Exception:
            pass

    def _sync_positions_zoom_selection(self, item_id: str) -> None:
        if self._positions_zoom_tree is None or not self._positions_zoom_tree.exists(item_id):
            return
        if self._positions_zoom_tree.selection() == (item_id,):
            return
        self._positions_zoom_selection_suppressed_item_id = item_id
        self._position_selection_syncing = True
        try:
            self._positions_zoom_tree.selection_set(item_id)
        finally:
            self._position_selection_syncing = False
        try:
            self._positions_zoom_tree.see(item_id)
        except Exception:
            pass

    def _on_positions_zoom_selected(self, *_: object) -> None:
        if self._positions_zoom_tree is None or self._positions_view_rendering or self._position_selection_syncing:
            return
        selection = self._positions_zoom_tree.selection()
        if not selection:
            self._positions_zoom_selection_suppressed_item_id = None
            self._positions_zoom_selected_item_id = None
            self._refresh_positions_zoom_detail()
            self._update_positions_zoom_search_shortcuts()
            return
        selected_item_id = selection[0]
        if self._positions_zoom_selection_suppressed_item_id == selected_item_id:
            self._positions_zoom_selection_suppressed_item_id = None
            self._positions_zoom_selected_item_id = selected_item_id
            return
        self._positions_zoom_selection_suppressed_item_id = None
        self._positions_zoom_selected_item_id = selected_item_id
        if self.position_tree is not None and self.position_tree.exists(selected_item_id):
            self._sync_position_tree_selection(selected_item_id)
            self._refresh_position_detail_panel()
        else:
            self._refresh_positions_zoom_detail()
        self._update_positions_zoom_search_shortcuts()

    def _selected_positions_zoom_option_for_search(self) -> OkxPosition | None:
        payload = None
        if self._positions_zoom_selected_item_id:
            payload = self._position_row_payloads.get(self._positions_zoom_selected_item_id)
        if payload is None:
            payload = self._selected_position_payload()
        if payload is None or payload.get("kind") != "position":
            return None
        position = payload.get("item")
        if isinstance(position, OkxPosition) and position.inst_type == "OPTION":
            return position
        return None

    def _sync_positions_zoom_columns_from_main(self) -> None:
        if self.position_tree is None or self._positions_zoom_tree is None:
            return
        zoom_tree = self._positions_zoom_tree
        columns = tuple(self.position_tree["columns"])
        approx_heading_columns = {
            "bid_usdt",
            "ask_usdt",
            "mark_usdt",
            "avg_usdt",
            "upl_usdt",
            "realized_usdt",
            "theta_usdt",
        }
        compact_zoom_columns = {
            "time_value": 88,
            "time_value_usdt": 72,
            "intrinsic_value": 88,
            "intrinsic_usdt": 72,
            "bid_price": 72,
            "bid_usdt": 78,
            "ask_price": 72,
            "ask_usdt": 78,
        }
        heading_font_name = ttk.Style().lookup("Treeview.Heading", "font") or "TkDefaultFont"
        try:
            heading_font = tkfont.nametofont(heading_font_name)
        except Exception:
            heading_font = tkfont.nametofont("TkDefaultFont")
        for column_id in ("#0", *columns):
            heading = self.position_tree.heading(column_id)
            column = self.position_tree.column(column_id)
            width = column.get("width")
            stretch = column.get("stretch")
            if column_id in compact_zoom_columns:
                width = compact_zoom_columns[column_id]
                stretch = False
            elif column_id in approx_heading_columns:
                heading_text = str(heading.get("text", ""))
                width = max(heading_font.measure(heading_text) + 20, 84)
                stretch = False
            zoom_tree.heading(column_id, text=heading.get("text", ""))
            zoom_tree.column(
                column_id,
                width=width,
                anchor=column.get("anchor"),
                stretch=stretch,
            )

    def _selected_position_history_option_for_search(self) -> OkxPositionHistoryItem | None:
        if self._positions_zoom_position_history_tree is None:
            return None
        selection = self._positions_zoom_position_history_tree.selection()
        if not selection:
            return None
        index = _history_tree_index(selection[0], "ph")
        if index is None or index >= len(self._latest_position_history):
            return None
        item = self._latest_position_history[index]
        if item.inst_type == "OPTION":
            return item
        return None

    def _set_optional_button_enabled(self, button: ttk.Button | None, enabled: bool) -> None:
        if button is None:
            return
        if enabled:
            button.state(["!disabled"])
        else:
            button.state(["disabled"])

    def _update_positions_zoom_search_shortcuts(self) -> None:
        position = self._selected_positions_zoom_option_for_search()
        contract, expiry_prefix = _option_search_shortcuts(position.inst_id if position else "")
        enabled = bool(contract)
        self._set_optional_button_enabled(self._positions_zoom_apply_contract_button, enabled)
        self._set_optional_button_enabled(self._positions_zoom_apply_expiry_prefix_button, enabled)
        if not enabled:
            self._positions_zoom_option_search_hint_text.set("选中期权后，可一键带入合约或到期前缀。")
            return
        self._positions_zoom_option_search_hint_text.set(
            f"已选期权：{contract} | 快捷筛选：合约={contract} | 到期前缀={expiry_prefix}"
        )

    def _update_position_history_search_shortcuts(self) -> None:
        item = self._selected_position_history_option_for_search()
        contract, expiry_prefix = _option_search_shortcuts(item.inst_id if item else "")
        enabled = bool(contract)
        self._set_optional_button_enabled(self._positions_zoom_position_history_apply_contract_button, enabled)
        self._set_optional_button_enabled(self._positions_zoom_position_history_apply_expiry_prefix_button, enabled)
        if not enabled:
            self._positions_zoom_position_history_search_hint_text.set("选中历史期权后，可一键带入合约或到期前缀。")
            return
        self._positions_zoom_position_history_search_hint_text.set(
            f"已选历史期权：{contract} | 快捷筛选：合约={contract} | 到期前缀={expiry_prefix}"
        )

    def _selected_fill_history_option_for_search(self) -> OkxFillHistoryItem | None:
        if self._positions_zoom_fills_tree is None:
            return None
        selection = self._positions_zoom_fills_tree.selection()
        if not selection:
            return None
        index = _history_tree_index(selection[0], "fill")
        if index is None or index >= len(self._latest_fill_history):
            return None
        item = self._latest_fill_history[index]
        if item.inst_type == "OPTION":
            return item
        return None

    def _update_fill_history_search_shortcuts(self) -> None:
        item = self._selected_fill_history_option_for_search()
        contract, expiry_prefix = _option_search_shortcuts(item.inst_id if item else "")
        enabled = bool(contract)
        self._set_optional_button_enabled(self._positions_zoom_fills_apply_contract_button, enabled)
        self._set_optional_button_enabled(self._positions_zoom_fills_apply_expiry_prefix_button, enabled)
        if not enabled:
            self._positions_zoom_fill_history_search_hint_text.set("选中历史期权成交后，可一键带入合约或到期前缀。")
            return
        self._positions_zoom_fill_history_search_hint_text.set(
            f"已选历史期权成交：{contract} | 快捷筛选：合约={contract} | 到期前缀={expiry_prefix}"
        )

    def apply_selected_option_to_position_search(self) -> None:
        position = self._selected_positions_zoom_option_for_search()
        contract, _ = _option_search_shortcuts(position.inst_id if position else "")
        if not contract:
            messagebox.showinfo("快捷筛选", "请先在当前持仓里选中一条期权合约。")
            return
        self.position_keyword.set(contract)
        self._render_positions_view()

    def apply_selected_option_expiry_prefix_to_position_search(self) -> None:
        position = self._selected_positions_zoom_option_for_search()
        _, expiry_prefix = _option_search_shortcuts(position.inst_id if position else "")
        if not expiry_prefix:
            messagebox.showinfo("快捷筛选", "请先在当前持仓里选中一条期权合约。")
            return
        self.position_keyword.set(expiry_prefix)
        self._render_positions_view()

    def apply_selected_option_to_position_history_search(self) -> None:
        item = self._selected_position_history_option_for_search()
        contract, _ = _option_search_shortcuts(item.inst_id if item else "")
        if not contract:
            messagebox.showinfo("快捷筛选", "请先在历史仓位里选中一条期权合约。")
            return
        self.position_history_keyword.set(contract)
        self._render_positions_zoom_position_history_view()

    def apply_selected_option_expiry_prefix_to_position_history_search(self) -> None:
        item = self._selected_position_history_option_for_search()
        _, expiry_prefix = _option_search_shortcuts(item.inst_id if item else "")
        if not expiry_prefix:
            messagebox.showinfo("快捷筛选", "请先在历史仓位里选中一条期权合约。")
            return
        self.position_history_expiry_prefix_filter.set(expiry_prefix.rstrip("-").split("-")[-1])
        self._render_positions_zoom_position_history_view()

    def apply_selected_option_to_fill_history_search(self) -> None:
        item = self._selected_fill_history_option_for_search()
        contract, _ = _option_search_shortcuts(item.inst_id if item else "")
        if not contract:
            messagebox.showinfo("快捷筛选", "请先在历史成交里选中一条期权合约。")
            return
        self.fill_history_keyword.set(contract)
        self._render_positions_zoom_fills_view()

    def apply_selected_option_expiry_prefix_to_fill_history_search(self) -> None:
        item = self._selected_fill_history_option_for_search()
        _, expiry_prefix = _option_search_shortcuts(item.inst_id if item else "")
        if not expiry_prefix:
            messagebox.showinfo("快捷筛选", "请先在历史成交里选中一条期权合约。")
            return
        self.fill_history_expiry_prefix_filter.set(expiry_prefix.rstrip("-").split("-")[-1])
        self._render_positions_zoom_fills_view()

    def _refresh_positions_zoom_detail(self) -> None:
        if self._positions_zoom_detail is None:
            return
        payload = None
        if self._positions_zoom_selected_item_id:
            payload = self._position_row_payloads.get(self._positions_zoom_selected_item_id)
        if payload is None:
            self._set_readonly_text(self._positions_zoom_detail, self.position_detail_text.get())
            return
        if payload["kind"] == "position":
            position = payload["item"]
            if isinstance(position, OkxPosition):
                self._set_readonly_text(
                    self._positions_zoom_detail,
                    _build_position_detail_text(
                        position,
                        self._upl_usdt_prices,
                        self._position_instruments,
                        note=self._current_position_note_text(position),
                    ),
                )
                return
        label = payload["label"]
        positions = payload["item"]
        metrics = payload["metrics"]
        if isinstance(label, str) and isinstance(positions, list) and isinstance(metrics, dict):
            self._set_readonly_text(
                self._positions_zoom_detail,
                _build_group_detail_text(
                    label,
                    positions,
                    metrics,
                    self._upl_usdt_prices,
                    self._position_instruments,
                ),
            )
            return
        self._set_readonly_text(self._positions_zoom_detail, self.position_detail_text.get())

    def refresh_order_views(self) -> None:
        credentials = self._current_credentials_or_none()
        if credentials is None:
            _reset_refresh_health(self._pending_orders_refresh_health)
            _reset_refresh_health(self._order_history_refresh_health)
            self._positions_zoom_pending_orders_base_summary = "未配置 API 凭证，无法读取当前委托。"
            self._positions_zoom_pending_orders_summary_text.set(self._positions_zoom_pending_orders_base_summary)
            self._positions_zoom_order_history_base_summary = "未配置 API 凭证，无法读取历史委托。"
            self._positions_zoom_order_history_summary_text.set(self._positions_zoom_order_history_base_summary)
            self._latest_pending_orders = []
            self._latest_order_history = []
            self._render_pending_orders_view()
            self._render_order_history_view()
            self._refresh_all_refresh_badges()
            return
        environment = self._positions_effective_environment or ENV_OPTIONS[self.environment_label.get()]
        self._start_pending_orders_refresh(credentials, environment)
        self._start_order_history_refresh(credentials, environment)

    def refresh_pending_orders(self) -> None:
        credentials = self._current_credentials_or_none()
        if credentials is None:
            _reset_refresh_health(self._pending_orders_refresh_health)
            self._positions_zoom_pending_orders_base_summary = "未配置 API 凭证，无法读取当前委托。"
            self._positions_zoom_pending_orders_summary_text.set(self._positions_zoom_pending_orders_base_summary)
            self._latest_pending_orders = []
            self._render_pending_orders_view()
            self._refresh_all_refresh_badges()
            return
        environment = self._positions_effective_environment or ENV_OPTIONS[self.environment_label.get()]
        self._start_pending_orders_refresh(credentials, environment)

    def refresh_order_history(self) -> None:
        credentials = self._current_credentials_or_none()
        if credentials is None:
            _reset_refresh_health(self._order_history_refresh_health)
            self._positions_zoom_order_history_base_summary = "未配置 API 凭证，无法读取历史委托。"
            self._positions_zoom_order_history_summary_text.set(self._positions_zoom_order_history_base_summary)
            self._latest_order_history = []
            self._render_order_history_view()
            self._refresh_all_refresh_badges()
            return
        environment = self._positions_effective_environment or ENV_OPTIONS[self.environment_label.get()]
        self._start_order_history_refresh(credentials, environment)

    def _pending_order_tree_for_view(self, view_name: str | None = None) -> ttk.Treeview | None:
        if view_name == "account_info":
            return self._account_info_pending_orders_tree
        if view_name == "positions_zoom":
            return self._positions_zoom_pending_orders_tree
        if self._positions_zoom_pending_orders_tree is not None and _widget_exists(self._positions_zoom_pending_orders_tree):
            return self._positions_zoom_pending_orders_tree
        if self._account_info_pending_orders_tree is not None and _widget_exists(self._account_info_pending_orders_tree):
            return self._account_info_pending_orders_tree
        return None

    def _pending_order_parent_for_view(self, view_name: str | None = None):
        if view_name == "account_info" and _widget_exists(self._account_info_window):
            return self._account_info_window
        if view_name == "positions_zoom" and _widget_exists(self._positions_zoom_window):
            return self._positions_zoom_window
        if _widget_exists(self._positions_zoom_window):
            return self._positions_zoom_window
        if _widget_exists(self._account_info_window):
            return self._account_info_window
        return self.root

    def _create_refresh_badge(self, parent, textvariable: StringVar, store: list[Label]) -> Label:
        badge = Label(
            parent,
            textvariable=textvariable,
            font=("Microsoft YaHei UI", 9, "bold"),
            padx=8,
            pady=2,
            bd=0,
            relief="flat",
        )
        store.append(badge)
        return badge

    def _apply_refresh_badge_state(
        self,
        store: list[Label],
        textvariable: StringVar,
        state: RefreshHealthState,
    ) -> None:
        textvariable.set(_refresh_indicator_badge_text(state))
        palette = REFRESH_BADGE_PALETTES[_refresh_indicator_level(state)]
        alive: list[Label] = []
        for badge in store:
            if not _widget_exists(badge):
                continue
            badge.configure(
                bg=palette["bg"],
                fg=palette["fg"],
                activebackground=palette["bg"],
                activeforeground=palette["fg"],
            )
            alive.append(badge)
        store[:] = alive

    def _refresh_all_refresh_badges(self) -> None:
        self._apply_refresh_badge_state(
            self._positions_refresh_badges,
            self._positions_refresh_badge_text,
            self._positions_refresh_health,
        )
        self._apply_refresh_badge_state(
            self._account_info_refresh_badges,
            self._account_info_refresh_badge_text,
            self._account_info_refresh_health,
        )
        self._apply_refresh_badge_state(
            self._pending_orders_refresh_badges,
            self._pending_orders_refresh_badge_text,
            self._pending_orders_refresh_health,
        )
        self._apply_refresh_badge_state(
            self._order_history_refresh_badges,
            self._order_history_refresh_badge_text,
            self._order_history_refresh_health,
        )

    def _guard_live_action_against_stale_cache(
        self,
        state: RefreshHealthState,
        *,
        action_label: str,
        data_label: str,
        parent,
        refresh_callback=None,
    ) -> bool:
        if not _refresh_health_is_stale(state):
            return True
        if callable(refresh_callback):
            try:
                refresh_callback()
            except Exception:
                pass
        messagebox.showwarning(
            "数据可能已过期",
            (
                f"当前{data_label}已经连续刷新失败，为避免基于旧缓存执行{action_label}，本次操作已拦截。\n\n"
                f"{_describe_refresh_health(state)}\n\n"
                "系统已尝试重新发起刷新，请等待下一次刷新成功后再重试。"
            ),
            parent=parent,
        )
        return False

    def cancel_selected_pending_order(self, view_name: str | None = None) -> None:
        parent = self._pending_order_parent_for_view(view_name)
        if self._pending_order_canceling:
            messagebox.showinfo("撤单中", "当前已有一笔撤单请求在处理中，请稍等。", parent=parent)
            return
        item = self._selected_pending_order_item(view_name)
        if item is None:
            messagebox.showinfo("撤单", "请先在当前委托里选中一条要撤销的委托。", parent=parent)
            return
        owner_label = _trade_order_program_owner_label(item)
        if owner_label is None:
            messagebox.showinfo(
                "撤单限制",
                "当前只允许撤销本程序发出的委托。\n这条委托没有识别到本程序 clOrdId 规则，已拦截。",
                parent=parent,
            )
            return
        credentials = self._current_credentials_or_none()
        if credentials is None:
            messagebox.showinfo("撤单", "当前未配置 API 凭证，无法发起撤单。", parent=parent)
            return
        if not self._guard_live_action_against_stale_cache(
            self._pending_orders_refresh_health,
            action_label="撤单",
            data_label="当前委托",
            parent=parent,
            refresh_callback=self.refresh_pending_orders,
        ):
            return
        cancel_id = _trade_order_cancel_reference(item)
        if not cancel_id:
            messagebox.showinfo("撤单", "这条委托缺少可用订单 ID，暂时无法撤单。", parent=parent)
            return
        confirm_message = (
            f"确认撤销这条{item.source_label or '委托'}吗？\n\n"
            f"程序来源：{owner_label}\n"
            f"合约：{item.inst_id or '-'}\n"
            f"方向：{_format_history_side(item.side, item.pos_side)}\n"
            f"状态：{_format_trade_order_state(item.state)}\n"
            f"标识：{cancel_id}"
        )
        if not messagebox.askyesno("撤单确认", confirm_message, parent=parent):
            return
        environment = self._positions_effective_environment or ENV_OPTIONS[self.environment_label.get()]
        self._pending_order_canceling = True
        self._positions_zoom_pending_orders_summary_text.set(
            f"正在撤单：{item.source_label or '委托'} | {item.inst_id or '-'} | {cancel_id}"
        )
        threading.Thread(
            target=self._cancel_selected_pending_order_worker,
            args=(credentials, environment, item, owner_label, view_name),
            daemon=True,
        ).start()

    def cancel_filtered_pending_orders(self, view_name: str | None = None) -> None:
        parent = self._pending_order_parent_for_view(view_name)
        if self._pending_order_canceling:
            messagebox.showinfo("撤单中", "当前已有撤单请求在处理中，请稍等。", parent=parent)
            return
        filtered_items = [item for _, item in self._filtered_pending_order_items()]
        if not filtered_items:
            messagebox.showinfo("批量撤单", "当前筛选结果为空，没有可撤的委托。", parent=parent)
            return
        cancelable_items = [
            item for item in filtered_items if _trade_order_program_owner_label(item) is not None and _trade_order_cancel_reference(item)
        ]
        skipped_manual = sum(1 for item in filtered_items if _trade_order_program_owner_label(item) is None)
        skipped_missing_id = sum(
            1 for item in filtered_items if _trade_order_program_owner_label(item) is not None and not _trade_order_cancel_reference(item)
        )
        if not cancelable_items:
            messagebox.showinfo(
                "批量撤单",
                "当前筛选结果里没有可识别为本程序发出的可撤委托。\n不会撤销手工单或来源不明的委托。",
                parent=parent,
            )
            return
        filter_warning = ""
        if not _trade_order_filter_enabled(
            POSITION_TYPE_OPTIONS.get(self.pending_order_type_filter.get(), ""),
            ORDER_SOURCE_FILTER_OPTIONS.get(self.pending_order_source_filter.get(), ""),
            ORDER_STATE_FILTER_OPTIONS.get(self.pending_order_state_filter.get(), ""),
            self.pending_order_asset_filter.get(),
            self.pending_order_expiry_prefix_filter.get(),
            self.pending_order_keyword.get(),
        ):
            filter_warning = "当前未启用任何筛选，本次会按当前页全部可识别程序单执行。\n\n"
        confirm_message = (
            f"{filter_warning}"
            f"确认批量撤销当前筛选结果中的程序委托吗？\n\n"
            f"筛选结果总数：{len(filtered_items)}\n"
            f"将尝试撤销：{len(cancelable_items)}\n"
            f"跳过非程序单：{skipped_manual}\n"
            f"跳过缺少ID：{skipped_missing_id}"
        )
        if not messagebox.askyesno("批量撤单确认", confirm_message, parent=parent):
            return
        credentials = self._current_credentials_or_none()
        if credentials is None:
            messagebox.showinfo("批量撤单", "当前未配置 API 凭证，无法发起批量撤单。", parent=parent)
            return
        if not self._guard_live_action_against_stale_cache(
            self._pending_orders_refresh_health,
            action_label="批量撤单",
            data_label="当前委托",
            parent=parent,
            refresh_callback=self.refresh_pending_orders,
        ):
            return
        environment = self._positions_effective_environment or ENV_OPTIONS[self.environment_label.get()]
        self._pending_order_canceling = True
        self._positions_zoom_pending_orders_summary_text.set(
            f"正在批量撤单：准备处理 {len(cancelable_items)} / {len(filtered_items)} 条"
        )
        threading.Thread(
            target=self._cancel_filtered_pending_orders_worker,
            args=(credentials, environment, cancelable_items, skipped_manual, skipped_missing_id, view_name),
            daemon=True,
        ).start()

    def _cancel_selected_pending_order_worker(
        self,
        credentials: Credentials,
        environment: str,
        item: OkxTradeOrderItem,
        owner_label: str,
        view_name: str | None,
    ) -> None:
        try:
            result = self._cancel_pending_order_request(credentials, environment=environment, item=item)
            note = None
            effective_environment = environment
        except Exception as exc:
            message = str(exc)
            if "50101" in message and "current environment" in message:
                alternate = "live" if environment == "demo" else "demo"
                try:
                    result = self._cancel_pending_order_request(credentials, environment=alternate, item=item)
                    note = f"撤单自动切换到 {'实盘' if alternate == 'live' else '模拟'} 环境执行。"
                    effective_environment = alternate
                except Exception:
                    self.root.after(
                        0,
                        lambda: self._apply_pending_order_cancel_error(item, message, owner_label, environment, view_name),
                    )
                    return
            else:
                self.root.after(
                    0,
                    lambda: self._apply_pending_order_cancel_error(item, message, owner_label, environment, view_name),
                )
                return
        self.root.after(
            0,
            lambda: self._apply_pending_order_cancel_result(item, result, owner_label, note, effective_environment, view_name),
        )

    def _cancel_filtered_pending_orders_worker(
        self,
        credentials: Credentials,
        environment: str,
        items: list[OkxTradeOrderItem],
        skipped_manual: int,
        skipped_missing_id: int,
        view_name: str | None,
    ) -> None:
        success_items: list[tuple[OkxTradeOrderItem, OkxOrderResult, str]] = []
        failed_items: list[tuple[OkxTradeOrderItem, str, str]] = []
        active_environment = environment
        note: str | None = None
        switched = False
        for item in items:
            owner_label = _trade_order_program_owner_label(item) or "本程序委托"
            try:
                result = self._cancel_pending_order_request(credentials, environment=active_environment, item=item)
            except Exception as exc:
                message = str(exc)
                if not switched and "50101" in message and "current environment" in message:
                    alternate = "live" if active_environment == "demo" else "demo"
                    try:
                        result = self._cancel_pending_order_request(credentials, environment=alternate, item=item)
                        active_environment = alternate
                        note = f"批量撤单自动切换到 {'实盘' if alternate == 'live' else '模拟'} 环境执行。"
                        switched = True
                    except Exception as retry_exc:
                        failed_items.append((item, owner_label, str(retry_exc)))
                        continue
                else:
                    failed_items.append((item, owner_label, message))
                    continue
            success_items.append((item, result, owner_label))
        self.root.after(
            0,
            lambda: self._apply_bulk_pending_order_cancel_result(
                success_items,
                failed_items,
                skipped_manual,
                skipped_missing_id,
                note,
                active_environment,
                view_name,
            ),
        )

    def _cancel_pending_order_request(
        self,
        credentials: Credentials,
        *,
        environment: str,
        item: OkxTradeOrderItem,
    ) -> OkxOrderResult:
        if item.source_kind == "algo":
            return self.client.cancel_algo_order(
                credentials,
                environment=environment,
                inst_id=item.inst_id,
                algo_id=item.algo_id or None,
                algo_cl_ord_id=item.algo_client_order_id or item.client_order_id or None,
            )
        return self.client.cancel_order_by_id(
            credentials,
            environment=environment,
            inst_id=item.inst_id,
            ord_id=item.order_id or None,
            cl_ord_id=item.client_order_id or None,
        )

    def _apply_pending_order_cancel_result(
        self,
        item: OkxTradeOrderItem,
        result: OkxOrderResult,
        owner_label: str,
        note: str | None = None,
        effective_environment: str | None = None,
        view_name: str | None = None,
    ) -> None:
        self._pending_order_canceling = False
        if effective_environment:
            self._positions_effective_environment = effective_environment
        cancel_id = _trade_order_cancel_reference(item) or result.ord_id or result.cl_ord_id or "-"
        summary = f"撤单请求已提交：{item.inst_id or '-'} | {cancel_id}"
        if note:
            summary = f"{summary} | {note}"
        self._positions_zoom_pending_orders_base_summary = summary
        self._positions_zoom_pending_orders_summary_text.set(summary)
        self._enqueue_log(
            "实盘撤单"
            f" | 模式=单笔"
            f" | 环境={effective_environment or '-'}"
            f" | 程序来源={owner_label}"
            f" | 来源={item.source_label or '-'}"
            f" | 合约={item.inst_id or '-'}"
            f" | 标识={cancel_id}"
            f" | 结果=已提交"
            f" | sCode={result.s_code}"
            f" | sMsg={result.s_msg or 'accepted'}"
        )
        parent = self._pending_order_parent_for_view(view_name)
        messagebox.showinfo(
            "撤单结果",
            (
                "撤单请求已提交。\n\n"
                f"程序来源：{owner_label}\n"
                f"来源：{item.source_label or '-'}\n"
                f"合约：{item.inst_id or '-'}\n"
                f"标识：{cancel_id}\n"
                f"返回：sCode={result.s_code} | sMsg={result.s_msg or 'accepted'}"
            ),
            parent=parent,
        )
        self.refresh_pending_orders()
        self.refresh_order_history()

    def _apply_pending_order_cancel_error(
        self,
        item: OkxTradeOrderItem,
        message: str,
        owner_label: str,
        environment: str,
        view_name: str | None = None,
    ) -> None:
        self._pending_order_canceling = False
        friendly_message = _format_network_error_message(message)
        self._positions_zoom_pending_orders_base_summary = f"撤单失败：{friendly_message}"
        self._positions_zoom_pending_orders_summary_text.set(self._positions_zoom_pending_orders_base_summary)
        self._enqueue_log(
            "实盘撤单"
            f" | 模式=单笔"
            f" | 环境={environment}"
            f" | 程序来源={owner_label}"
            f" | 来源={item.source_label or '-'}"
            f" | 合约={item.inst_id or '-'}"
            f" | 标识={_trade_order_cancel_reference(item) or '-'}"
            f" | 结果=失败"
            f" | 原因={friendly_message}"
        )
        parent = self._pending_order_parent_for_view(view_name)
        messagebox.showerror(
            "撤单失败",
            (
                f"{item.source_label or '委托'} 撤单失败。\n\n"
                f"程序来源：{owner_label}\n"
                f"合约：{item.inst_id or '-'}\n"
                f"原因：{friendly_message}"
            ),
            parent=parent,
        )

    def _apply_bulk_pending_order_cancel_result(
        self,
        success_items: list[tuple[OkxTradeOrderItem, OkxOrderResult, str]],
        failed_items: list[tuple[OkxTradeOrderItem, str, str]],
        skipped_manual: int,
        skipped_missing_id: int,
        note: str | None = None,
        effective_environment: str | None = None,
        view_name: str | None = None,
    ) -> None:
        self._pending_order_canceling = False
        if effective_environment:
            self._positions_effective_environment = effective_environment
        success_count = len(success_items)
        failed_count = len(failed_items)
        summary = (
            f"批量撤单完成：提交 {success_count} 条"
            f" | 失败 {failed_count} 条"
            f" | 跳过非程序单 {skipped_manual} 条"
            f" | 跳过缺少ID {skipped_missing_id} 条"
        )
        if note:
            summary = f"{summary} | {note}"
        self._positions_zoom_pending_orders_base_summary = summary
        self._positions_zoom_pending_orders_summary_text.set(summary)
        self._enqueue_log(
            "实盘撤单"
            f" | 模式=批量"
            f" | 环境={effective_environment or '-'}"
            f" | 提交={success_count}"
            f" | 失败={failed_count}"
            f" | 跳过非程序单={skipped_manual}"
            f" | 跳过缺少ID={skipped_missing_id}"
            f"{' | 备注=' + note if note else ''}"
        )
        for item, result, owner_label in success_items:
            self._enqueue_log(
                "实盘撤单明细"
                f" | 结果=已提交"
                f" | 程序来源={owner_label}"
                f" | 来源={item.source_label or '-'}"
                f" | 合约={item.inst_id or '-'}"
                f" | 标识={_trade_order_cancel_reference(item) or result.ord_id or result.cl_ord_id or '-'}"
                f" | sCode={result.s_code}"
                f" | sMsg={result.s_msg or 'accepted'}"
            )
        for item, owner_label, message in failed_items:
            self._enqueue_log(
                "实盘撤单明细"
                f" | 结果=失败"
                f" | 程序来源={owner_label}"
                f" | 来源={item.source_label or '-'}"
                f" | 合约={item.inst_id or '-'}"
                f" | 标识={_trade_order_cancel_reference(item) or '-'}"
                f" | 原因={_format_network_error_message(message)}"
            )
        parent = self._pending_order_parent_for_view(view_name)
        messagebox.showinfo(
            "批量撤单结果",
            (
                f"{summary}\n\n"
                f"已提交：{success_count}\n"
                f"失败：{failed_count}\n"
                f"跳过非程序单：{skipped_manual}\n"
                f"跳过缺少ID：{skipped_missing_id}"
            ),
            parent=parent,
        )
        self.refresh_pending_orders()
        self.refresh_order_history()

    def _start_pending_orders_refresh(self, credentials: Credentials, environment: str) -> None:
        if self._pending_orders_refreshing:
            return
        self._pending_orders_refreshing = True
        self._positions_zoom_pending_orders_summary_text.set("正在刷新当前委托...")
        threading.Thread(
            target=self._refresh_pending_orders_worker,
            args=(credentials, environment),
            daemon=True,
        ).start()

    def _start_order_history_refresh(self, credentials: Credentials, environment: str) -> None:
        if self._order_history_refreshing:
            return
        self._order_history_refreshing = True
        self._positions_zoom_order_history_summary_text.set("正在刷新历史委托...")
        threading.Thread(
            target=self._refresh_order_history_worker,
            args=(credentials, environment),
            daemon=True,
        ).start()

    def _refresh_pending_orders_worker(self, credentials: Credentials, environment: str) -> None:
        try:
            items = self.client.get_pending_orders(credentials, environment=environment, limit=200)
            note = None
            effective_environment = environment
        except Exception as exc:
            message = str(exc)
            if "50101" in message and "current environment" in message:
                alternate = "live" if environment == "demo" else "demo"
                try:
                    items = self.client.get_pending_orders(credentials, environment=alternate, limit=200)
                    note = f"委托数据自动切换到 {'实盘' if alternate == 'live' else '模拟'} 环境读取。"
                    effective_environment = alternate
                except Exception:
                    self.root.after(0, lambda: self._apply_pending_orders_error(message))
                    return
            else:
                self.root.after(0, lambda: self._apply_pending_orders_error(message))
                return
        self.root.after(0, lambda: self._apply_pending_orders(items, note, effective_environment))

    def _refresh_order_history_worker(self, credentials: Credentials, environment: str) -> None:
        try:
            items = self.client.get_order_history(credentials, environment=environment, limit=200)
            note = None
            effective_environment = environment
        except Exception as exc:
            message = str(exc)
            if "50101" in message and "current environment" in message:
                alternate = "live" if environment == "demo" else "demo"
                try:
                    items = self.client.get_order_history(credentials, environment=alternate, limit=200)
                    note = f"委托数据自动切换到 {'实盘' if alternate == 'live' else '模拟'} 环境读取。"
                    effective_environment = alternate
                except Exception:
                    self.root.after(0, lambda: self._apply_order_history_error(message))
                    return
            else:
                self.root.after(0, lambda: self._apply_order_history_error(message))
                return
        self.root.after(0, lambda: self._apply_order_history(items, note, effective_environment))

    def _apply_pending_orders(
        self,
        items: list[OkxTradeOrderItem],
        note: str | None = None,
        effective_environment: str | None = None,
    ) -> None:
        self._pending_orders_refreshing = False
        self._latest_pending_orders = list(items)
        self._pending_orders_last_refresh_at = datetime.now()
        _mark_refresh_health_success(self._pending_orders_refresh_health, at=self._pending_orders_last_refresh_at)
        self._refresh_all_refresh_badges()
        if effective_environment:
            self._positions_effective_environment = effective_environment
        timestamp = self._pending_orders_last_refresh_at.strftime("%H:%M:%S")
        summary = f"当前委托：{len(items)} 条 | 最近刷新：{timestamp}"
        if note:
            summary = f"{summary} | {note}"
        self._positions_zoom_pending_orders_base_summary = summary
        self._positions_zoom_pending_orders_summary_text.set(summary)
        self._render_pending_orders_view()

    def _apply_order_history(
        self,
        items: list[OkxTradeOrderItem],
        note: str | None = None,
        effective_environment: str | None = None,
    ) -> None:
        self._order_history_refreshing = False
        self._latest_order_history = list(items)
        self._order_history_last_refresh_at = datetime.now()
        _mark_refresh_health_success(self._order_history_refresh_health, at=self._order_history_last_refresh_at)
        self._refresh_all_refresh_badges()
        if effective_environment:
            self._positions_effective_environment = effective_environment
        timestamp = self._order_history_last_refresh_at.strftime("%H:%M:%S")
        summary = f"历史委托：{len(items)} 条 | 最近刷新：{timestamp}"
        if note:
            summary = f"{summary} | {note}"
        self._positions_zoom_order_history_base_summary = summary
        self._positions_zoom_order_history_summary_text.set(summary)
        self._render_order_history_view()

    def _apply_pending_orders_error(self, message: str) -> None:
        self._pending_orders_refreshing = False
        friendly_message = _format_network_error_message(message)
        _mark_refresh_health_failure(self._pending_orders_refresh_health, friendly_message)
        self._refresh_all_refresh_badges()
        suffix = _format_refresh_health_suffix(self._pending_orders_refresh_health)
        has_previous_items = bool(self._latest_pending_orders) or self._pending_orders_last_refresh_at is not None
        if has_previous_items:
            summary = f"当前委托刷新失败，继续显示上一份缓存：{friendly_message}{suffix}"
        else:
            summary = f"当前委托读取失败：{friendly_message}{suffix}"
        self._positions_zoom_pending_orders_base_summary = summary
        self._positions_zoom_pending_orders_summary_text.set(self._positions_zoom_pending_orders_base_summary)
        self._enqueue_log(summary)

    def _apply_order_history_error(self, message: str) -> None:
        self._order_history_refreshing = False
        friendly_message = _format_network_error_message(message)
        _mark_refresh_health_failure(self._order_history_refresh_health, friendly_message)
        self._refresh_all_refresh_badges()
        suffix = _format_refresh_health_suffix(self._order_history_refresh_health)
        has_previous_items = bool(self._latest_order_history) or self._order_history_last_refresh_at is not None
        if has_previous_items:
            summary = f"历史委托刷新失败，继续显示上一份缓存：{friendly_message}{suffix}"
        else:
            summary = f"历史委托读取失败：{friendly_message}{suffix}"
        self._positions_zoom_order_history_base_summary = summary
        self._positions_zoom_order_history_summary_text.set(self._positions_zoom_order_history_base_summary)
        self._enqueue_log(summary)

    def _on_pending_order_filter_changed(self, *_: object) -> None:
        self._render_pending_orders_view()

    def reset_pending_order_filters(self) -> None:
        self.pending_order_type_filter.set("全部类型")
        self.pending_order_source_filter.set("全部来源")
        self.pending_order_state_filter.set("全部状态")
        self.pending_order_asset_filter.set("")
        self.pending_order_expiry_prefix_filter.set("")
        self.pending_order_keyword.set("")
        self._render_pending_orders_view()

    def _on_order_history_filter_changed(self, *_: object) -> None:
        self._render_order_history_view()

    def reset_order_history_filters(self) -> None:
        self.order_history_type_filter.set("全部类型")
        self.order_history_source_filter.set("全部来源")
        self.order_history_state_filter.set("全部状态")
        self.order_history_asset_filter.set("")
        self.order_history_expiry_prefix_filter.set("")
        self.order_history_keyword.set("")
        self._render_order_history_view()

    def _trade_order_views(self, *pairs: tuple[str, str]) -> list[tuple[str, str, ttk.Treeview, Text | None]]:
        views: list[tuple[str, str, ttk.Treeview, Text | None]] = []
        for tree_attr, detail_attr in pairs:
            tree = getattr(self, tree_attr)
            detail = getattr(self, detail_attr)
            if tree is not None and not _widget_exists(tree):
                setattr(self, tree_attr, None)
                tree = None
            if detail is not None and not _widget_exists(detail):
                setattr(self, detail_attr, None)
                detail = None
            if tree is not None:
                views.append((tree_attr, detail_attr, tree, detail))
        return views

    def _pending_order_views(self) -> list[tuple[str, str, ttk.Treeview, Text | None]]:
        return self._trade_order_views(
            ("_positions_zoom_pending_orders_tree", "_positions_zoom_pending_orders_detail"),
            ("_account_info_pending_orders_tree", "_account_info_pending_orders_detail"),
        )

    def _order_history_views(self) -> list[tuple[str, str, ttk.Treeview, Text | None]]:
        return self._trade_order_views(
            ("_positions_zoom_order_history_tree", "_positions_zoom_order_history_detail"),
            ("_account_info_order_history_tree", "_account_info_order_history_detail"),
        )

    def _render_pending_orders_view(self) -> None:
        filtered_items = self._filtered_pending_order_items()
        summary = self._positions_zoom_pending_orders_base_summary
        cancelable_count = sum(
            1 for _, item in filtered_items if _trade_order_program_owner_label(item) is not None and _trade_order_cancel_reference(item)
        )
        if _trade_order_filter_enabled(
            POSITION_TYPE_OPTIONS.get(self.pending_order_type_filter.get(), ""),
            ORDER_SOURCE_FILTER_OPTIONS.get(self.pending_order_source_filter.get(), ""),
            ORDER_STATE_FILTER_OPTIONS.get(self.pending_order_state_filter.get(), ""),
            self.pending_order_asset_filter.get(),
            self.pending_order_expiry_prefix_filter.get(),
            self.pending_order_keyword.get(),
        ):
            summary = f"{summary} | 当前显示：{len(filtered_items)}/{len(self._latest_pending_orders)}"
        if filtered_items:
            summary = f"{summary} | 可撤程序单：{cancelable_count}/{len(filtered_items)}"
        self._positions_zoom_pending_orders_summary_text.set(summary)
        for tree_attr, _, tree, _ in self._pending_order_views():
            try:
                selection = tree.selection()
                selected_before = selection[0] if selection else None
                tree.delete(*tree.get_children())
                for index, item in filtered_items:
                    iid = f"po-{index}"
                    tree.insert(
                        "",
                        END,
                        iid=iid,
                        values=(
                            _format_trade_order_timestamp(item),
                            item.source_label,
                            item.inst_type or "-",
                            item.inst_id or "-",
                            _format_trade_order_state(item.state),
                            _format_history_side(item.side, item.pos_side),
                            item.ord_type or "-",
                            _format_trade_order_price(item.price, item.inst_id, item.inst_type),
                            _format_trade_order_size(item.size),
                            _format_trade_order_size(item.filled_size),
                            _format_trade_order_tp_sl(item),
                            item.order_id or item.algo_id or "-",
                            item.client_order_id or item.algo_client_order_id or "-",
                        ),
                        tags=tuple(tag for tag in (_pnl_tag(item.pnl),) if tag),
                    )
                if selected_before and tree.exists(selected_before):
                    tree.selection_set(selected_before)
                    tree.focus(selected_before)
                elif tree.get_children():
                    first = tree.get_children()[0]
                    tree.selection_set(first)
                    tree.focus(first)
            except TclError:
                setattr(self, tree_attr, None)
        self._refresh_pending_orders_detail()

    def _render_order_history_view(self) -> None:
        filtered_items = _filter_trade_order_items(
            self._latest_order_history,
            inst_type=POSITION_TYPE_OPTIONS.get(self.order_history_type_filter.get(), ""),
            source=ORDER_SOURCE_FILTER_OPTIONS.get(self.order_history_source_filter.get(), ""),
            state=ORDER_STATE_FILTER_OPTIONS.get(self.order_history_state_filter.get(), ""),
            asset=self.order_history_asset_filter.get(),
            expiry_prefix=self.order_history_expiry_prefix_filter.get(),
            keyword=self.order_history_keyword.get(),
        )
        summary = self._positions_zoom_order_history_base_summary
        if _trade_order_filter_enabled(
            POSITION_TYPE_OPTIONS.get(self.order_history_type_filter.get(), ""),
            ORDER_SOURCE_FILTER_OPTIONS.get(self.order_history_source_filter.get(), ""),
            ORDER_STATE_FILTER_OPTIONS.get(self.order_history_state_filter.get(), ""),
            self.order_history_asset_filter.get(),
            self.order_history_expiry_prefix_filter.get(),
            self.order_history_keyword.get(),
        ):
            summary = f"{summary} | 当前显示：{len(filtered_items)}/{len(self._latest_order_history)}"
        self._positions_zoom_order_history_summary_text.set(summary)
        for tree_attr, _, tree, _ in self._order_history_views():
            try:
                selection = tree.selection()
                selected_before = selection[0] if selection else None
                tree.delete(*tree.get_children())
                for index, item in filtered_items:
                    iid = f"oh-{index}"
                    tree.insert(
                        "",
                        END,
                        iid=iid,
                        values=(
                            _format_trade_order_timestamp(item),
                            item.source_label,
                            item.inst_type or "-",
                            item.inst_id or "-",
                            _format_trade_order_state(item.state),
                            _format_history_side(item.side, item.pos_side),
                            item.ord_type or "-",
                            _format_trade_order_price(item.price, item.inst_id, item.inst_type),
                            _format_trade_order_size(item.size),
                            _format_trade_order_size(item.filled_size),
                            _format_trade_order_tp_sl(item),
                            item.order_id or item.algo_id or "-",
                            item.client_order_id or item.algo_client_order_id or "-",
                        ),
                        tags=tuple(tag for tag in (_pnl_tag(item.pnl),) if tag),
                    )
                if selected_before and tree.exists(selected_before):
                    tree.selection_set(selected_before)
                    tree.focus(selected_before)
                elif tree.get_children():
                    first = tree.get_children()[0]
                    tree.selection_set(first)
                    tree.focus(first)
            except TclError:
                setattr(self, tree_attr, None)
        self._refresh_order_history_detail()

    def _on_pending_orders_selected(self, *_: object) -> None:
        self._refresh_pending_orders_detail()

    def _on_order_history_selected(self, *_: object) -> None:
        self._refresh_order_history_detail()

    def _refresh_pending_orders_detail(self) -> None:
        for tree_attr, detail_attr, tree, detail in self._pending_order_views():
            if detail is None:
                continue
            try:
                selection = tree.selection()
            except TclError:
                setattr(self, tree_attr, None)
                setattr(self, detail_attr, None)
                continue
            if not selection:
                self._set_readonly_text(detail, "这里会显示选中当前委托的详情。")
                continue
            index = _history_tree_index(selection[0], "po")
            if index is None or index >= len(self._latest_pending_orders):
                self._set_readonly_text(detail, "这里会显示选中当前委托的详情。")
                continue
            self._set_readonly_text(detail, _build_trade_order_detail_text(self._latest_pending_orders[index]))

    def _refresh_order_history_detail(self) -> None:
        for tree_attr, detail_attr, tree, detail in self._order_history_views():
            if detail is None:
                continue
            try:
                selection = tree.selection()
            except TclError:
                setattr(self, tree_attr, None)
                setattr(self, detail_attr, None)
                continue
            if not selection:
                self._set_readonly_text(detail, "这里会显示选中历史委托的详情。")
                continue
            index = _history_tree_index(selection[0], "oh")
            if index is None or index >= len(self._latest_order_history):
                self._set_readonly_text(detail, "这里会显示选中历史委托的详情。")
                continue
            self._set_readonly_text(detail, _build_trade_order_detail_text(self._latest_order_history[index]))

    def _selected_pending_order_item(self, view_name: str | None = None) -> OkxTradeOrderItem | None:
        candidate_trees: list[ttk.Treeview] = []
        tree = self._pending_order_tree_for_view(view_name)
        if tree is not None and _widget_exists(tree):
            candidate_trees.append(tree)
        for fallback_tree in (self._positions_zoom_pending_orders_tree, self._account_info_pending_orders_tree):
            if fallback_tree is not None and fallback_tree not in candidate_trees and _widget_exists(fallback_tree):
                candidate_trees.append(fallback_tree)
        for active_tree in candidate_trees:
            try:
                selection = active_tree.selection()
            except TclError:
                continue
            if not selection:
                continue
            index = _history_tree_index(selection[0], "po")
            if index is None or index >= len(self._latest_pending_orders):
                continue
            return self._latest_pending_orders[index]
        return None

    def _filtered_pending_order_items(self) -> list[tuple[int, OkxTradeOrderItem]]:
        return _filter_trade_order_items(
            self._latest_pending_orders,
            inst_type=POSITION_TYPE_OPTIONS.get(self.pending_order_type_filter.get(), ""),
            source=ORDER_SOURCE_FILTER_OPTIONS.get(self.pending_order_source_filter.get(), ""),
            state=ORDER_STATE_FILTER_OPTIONS.get(self.pending_order_state_filter.get(), ""),
            asset=self.pending_order_asset_filter.get(),
            expiry_prefix=self.pending_order_expiry_prefix_filter.get(),
            keyword=self.pending_order_keyword.get(),
        )

    def refresh_position_histories(self) -> None:
        credentials = self._current_credentials_or_none()
        if credentials is None:
            self._positions_zoom_fills_summary_text.set("未配置 API 凭证，无法读取历史成交。")
            self._positions_zoom_position_history_base_summary = "未配置 API 凭证，无法读取历史仓位。"
            self._positions_zoom_position_history_summary_text.set(self._positions_zoom_position_history_base_summary)
            return
        environment = self._positions_effective_environment or ENV_OPTIONS[self.environment_label.get()]
        profile_name = credentials.profile_name or self._current_credential_profile()
        self._start_position_history_refresh(credentials, environment, profile_name)
        self._start_fill_history_refresh(credentials, environment)

    def refresh_fill_history(self) -> None:
        credentials = self._current_credentials_or_none()
        if credentials is None:
            self._positions_zoom_fills_summary_text.set("未配置 API 凭证，无法读取历史成交。")
            return
        environment = self._positions_effective_environment or ENV_OPTIONS[self.environment_label.get()]
        self._start_fill_history_refresh(credentials, environment)

    def expand_fill_history_limit(self) -> None:
        self._fill_history_fetch_limit, self._fill_history_load_more_clicks, next_label = _advance_fill_history_limit(
            self._fill_history_fetch_limit,
            self._fill_history_load_more_clicks,
        )
        self._positions_zoom_fills_load_more_text.set(next_label)
        self.refresh_fill_history()

    def expand_position_history_limit(self) -> None:
        self._position_history_fetch_limit, self._position_history_load_more_clicks, next_label = _advance_fill_history_limit(
            self._position_history_fetch_limit,
            self._position_history_load_more_clicks,
        )
        self._positions_zoom_position_history_load_more_text.set(next_label)
        credentials = self._current_credentials_or_none()
        if credentials is None:
            self._positions_zoom_position_history_summary_text.set("未配置 API 凭证，无法读取历史仓位。")
            return
        environment = self._positions_effective_environment or ENV_OPTIONS[self.environment_label.get()]
        profile_name = credentials.profile_name or self._current_credential_profile()
        self._start_position_history_refresh(credentials, environment, profile_name)

    def _start_fill_history_refresh(self, credentials: Credentials, environment: str) -> None:
        if self._fills_history_refreshing:
            return
        self._fills_history_refreshing = True
        self._positions_zoom_fills_summary_text.set("正在刷新历史成交...")
        threading.Thread(
            target=self._refresh_fill_history_worker,
            args=(credentials, environment),
            daemon=True,
        ).start()

    def _start_position_history_refresh(self, credentials: Credentials, environment: str, profile_name: str) -> None:
        if self._position_history_refreshing:
            return
        self._position_history_refreshing = True
        self._positions_zoom_position_history_summary_text.set("正在刷新历史仓位...")
        threading.Thread(
            target=self._refresh_position_history_worker,
            args=(credentials, environment, profile_name),
            daemon=True,
        ).start()

    def _refresh_fill_history_worker(self, credentials: Credentials, environment: str) -> None:
        try:
            fills = self.client.get_fills_history(credentials, environment=environment, limit=self._fill_history_fetch_limit)
            instruments = _build_history_instrument_map(self.client, [item.inst_id for item in fills])
            note = None
            effective_environment = environment
        except Exception as exc:
            message = str(exc)
            if "50101" in message and "current environment" in message:
                alternate = "live" if environment == "demo" else "demo"
                try:
                    fills = self.client.get_fills_history(credentials, environment=alternate, limit=self._fill_history_fetch_limit)
                    instruments = _build_history_instrument_map(self.client, [item.inst_id for item in fills])
                    note = f"历史数据自动切换到 {'实盘' if alternate == 'live' else '模拟'} 环境读取。"
                    effective_environment = alternate
                except Exception:
                    self.root.after(0, lambda: self._apply_fill_history_error(message))
                    return
            else:
                self.root.after(0, lambda: self._apply_fill_history_error(message))
                return
        self.root.after(0, lambda: self._apply_fill_history(fills, instruments, note, effective_environment))

    def _refresh_position_history_worker(self, credentials: Credentials, environment: str, profile_name: str) -> None:
        try:
            position_history = self.client.get_positions_history(credentials, environment=environment, limit=self._position_history_fetch_limit)
            usdt_prices = _build_position_history_usdt_price_map(self.client, position_history)
            instruments = _build_history_instrument_map(self.client, [item.inst_id for item in position_history])
            note = None
            effective_environment = environment
        except Exception as exc:
            message = str(exc)
            if "50101" in message and "current environment" in message:
                alternate = "live" if environment == "demo" else "demo"
                try:
                    position_history = self.client.get_positions_history(credentials, environment=alternate, limit=self._position_history_fetch_limit)
                    usdt_prices = _build_position_history_usdt_price_map(self.client, position_history)
                    instruments = _build_history_instrument_map(self.client, [item.inst_id for item in position_history])
                    note = f"历史数据自动切换到 {'实盘' if alternate == 'live' else '模拟'} 环境读取。"
                    effective_environment = alternate
                except Exception:
                    self.root.after(0, lambda: self._apply_position_history_error(message))
                    return
            else:
                self.root.after(0, lambda: self._apply_position_history_error(message))
                return
        self.root.after(
            0,
            lambda: self._apply_position_history(
                position_history,
                usdt_prices,
                instruments,
                note,
                effective_environment,
                profile_name,
            ),
        )

    def _apply_fill_history(
        self,
        fills: list[OkxFillHistoryItem],
        instruments: dict[str, Instrument],
        note: str | None = None,
        effective_environment: str | None = None,
    ) -> None:
        self._fills_history_refreshing = False
        self._latest_fill_history = list(fills)
        self._fill_history_instruments = dict(instruments)
        self._fills_history_last_refresh_at = datetime.now()
        self._positions_history_last_refresh_at = self._fills_history_last_refresh_at
        if effective_environment:
            self._positions_effective_environment = effective_environment
        timestamp = self._fills_history_last_refresh_at.strftime("%H:%M:%S")
        fill_summary = f"历史成交：{len(fills)} 条 | 最近刷新：{timestamp}"
        if note:
            fill_summary = f"{fill_summary} | {note}"
        self._positions_zoom_fills_summary_text.set(fill_summary)
        self._render_positions_zoom_fills_view()

    def _apply_position_history(
        self,
        position_history: list[OkxPositionHistoryItem],
        usdt_prices: dict[str, Decimal],
        instruments: dict[str, Instrument],
        note: str | None = None,
        effective_environment: str | None = None,
        credential_profile_name: str | None = None,
    ) -> None:
        self._position_history_refreshing = False
        self._latest_position_history = list(position_history)
        self._position_history_usdt_prices = dict(usdt_prices)
        self._position_history_instruments = dict(instruments)
        self._position_history_last_refresh_at = datetime.now()
        self._positions_history_last_refresh_at = self._position_history_last_refresh_at
        self._position_history_profile_name = (credential_profile_name or self._current_credential_profile()).strip()
        self._position_history_effective_environment = effective_environment
        if effective_environment:
            self._positions_effective_environment = effective_environment
        timestamp = self._position_history_last_refresh_at.strftime("%H:%M:%S")
        history_summary = f"历史仓位：{len(position_history)} 条 | 最近刷新：{timestamp}"
        if note:
            history_summary = f"{history_summary} | {note}"
        self._positions_zoom_position_history_base_summary = history_summary
        self._positions_zoom_position_history_summary_text.set(history_summary)
        if self._position_history_profile_name and self._position_history_effective_environment:
            self._sync_position_note_state_for_history(
                profile_name=self._position_history_profile_name,
                environment=self._position_history_effective_environment,
                position_history=position_history,
            )
        self._render_positions_zoom_position_history_view()

    def _apply_fill_history_error(self, message: str) -> None:
        self._fills_history_refreshing = False
        self._positions_zoom_fills_summary_text.set(f"历史成交读取失败：{message}")

    def _apply_position_history_error(self, message: str) -> None:
        self._position_history_refreshing = False
        self._positions_zoom_position_history_base_summary = f"历史仓位读取失败：{message}"
        self._positions_zoom_position_history_summary_text.set(self._positions_zoom_position_history_base_summary)

    def _render_positions_zoom_fills_view(self) -> None:
        if self._positions_zoom_fills_tree is None:
            return
        tree = self._positions_zoom_fills_tree
        selected_before = tree.selection()[0] if tree.selection() else None
        tree.delete(*tree.get_children())
        filtered_items = _filter_fill_history_items(
            self._latest_fill_history,
            inst_type=POSITION_TYPE_OPTIONS.get(self.fill_history_type_filter.get(), ""),
            side=HISTORY_FILL_SIDE_FILTER_OPTIONS.get(self.fill_history_side_filter.get(), ""),
            asset=self.fill_history_asset_filter.get(),
            expiry_prefix=self.fill_history_expiry_prefix_filter.get(),
            keyword=self.fill_history_keyword.get(),
        )
        for index, item in filtered_items:
            iid = f"fill-{index}"
            tree.insert(
                "",
                END,
                iid=iid,
                values=(
                    _format_okx_ms_timestamp(item.fill_time),
                    item.inst_type or "-",
                    item.inst_id or "-",
                    _format_history_side(item.side, item.pos_side),
                    _format_fill_history_price(item),
                    _format_fill_history_size(item, self._fill_history_instruments),
                    _format_optional_decimal(item.fill_fee),
                    _format_fill_history_pnl(item),
                    _format_fill_history_exec_type(item.exec_type),
                  ),
                  tags=tuple(tag for tag in (_pnl_tag(item.pnl),) if tag),
              )
        if self._fills_history_last_refresh_at is not None:
            timestamp = self._fills_history_last_refresh_at.strftime("%H:%M:%S")
            summary = f"历史成交：{len(self._latest_fill_history)} 条 | 最近刷新：{timestamp}"
        else:
            summary = f"历史成交：{len(self._latest_fill_history)} 条"
        if (
            POSITION_TYPE_OPTIONS.get(self.fill_history_type_filter.get(), "")
            or HISTORY_FILL_SIDE_FILTER_OPTIONS.get(self.fill_history_side_filter.get(), "")
            or self.fill_history_asset_filter.get().strip()
            or self.fill_history_expiry_prefix_filter.get().strip()
            or self.fill_history_keyword.get().strip()
        ):
            summary = f"{summary} | 当前显示：{len(filtered_items)}/{len(self._latest_fill_history)}"
        self._positions_zoom_fills_summary_text.set(summary)
        if selected_before and tree.exists(selected_before):
            tree.selection_set(selected_before)
            tree.focus(selected_before)
        elif tree.get_children():
            first = tree.get_children()[0]
            tree.selection_set(first)
            tree.focus(first)
        self._refresh_positions_zoom_fills_detail()
        self._update_fill_history_search_shortcuts()

    def _on_fill_history_filter_changed(self, *_: object) -> None:
        self._render_positions_zoom_fills_view()

    def reset_fill_history_filters(self) -> None:
        self.fill_history_type_filter.set("全部类型")
        self.fill_history_side_filter.set("全部方向")
        self.fill_history_asset_filter.set("")
        self.fill_history_expiry_prefix_filter.set("")
        self.fill_history_keyword.set("")
        self._render_positions_zoom_fills_view()

    def _render_positions_zoom_position_history_view(self) -> None:
        if self._positions_zoom_position_history_tree is None:
            return
        tree = self._positions_zoom_position_history_tree
        selected_before = tree.selection()[0] if tree.selection() else None
        tree.delete(*tree.get_children())
        filtered_items = _filter_position_history_items(
            self._latest_position_history,
            inst_type=POSITION_TYPE_OPTIONS.get(self.position_history_type_filter.get(), ""),
            margin_mode=HISTORY_MARGIN_MODE_FILTER_OPTIONS.get(self.position_history_margin_filter.get(), ""),
            asset=self.position_history_asset_filter.get(),
            expiry_prefix=self.position_history_expiry_prefix_filter.get(),
            keyword=self.position_history_keyword.get(),
            note_texts_by_index=self._position_history_note_text_map_by_index(),
        )
        for index, item in filtered_items:
            iid = f"ph-{index}"
            tree.insert(
                "",
                END,
                iid=iid,
                values=(
                    _format_okx_ms_timestamp(item.update_time),
                    item.inst_type or "-",
                    item.inst_id or "-",
                    _format_margin_mode(item.mgn_mode or ""),
                    _format_history_side(None, item.pos_side or item.direction),
                    _format_position_history_price(item.open_avg_price, item.inst_id, item.inst_type),
                    _format_position_history_price(item.close_avg_price, item.inst_id, item.inst_type),
                    _format_position_history_size(item, self._position_history_instruments),
                    _format_position_history_pnl(item.pnl, item),
                    _format_position_history_pnl(item.realized_pnl, item, with_sign=True),
                    _format_optional_usdt(
                        _position_history_realized_pnl_usdt(item, self._position_history_usdt_prices),
                        with_sign=True,
                    ),
                    self._position_history_note_summary(item),
                ),
                tags=tuple(tag for tag in (_pnl_tag(item.pnl),) if tag),
            )
        summary = self._positions_zoom_position_history_base_summary
        if (
            POSITION_TYPE_OPTIONS.get(self.position_history_type_filter.get(), "")
            or HISTORY_MARGIN_MODE_FILTER_OPTIONS.get(self.position_history_margin_filter.get(), "")
            or self.position_history_asset_filter.get().strip()
            or self.position_history_expiry_prefix_filter.get().strip()
            or self.position_history_keyword.get().strip()
        ):
            summary = f"{summary} | 当前显示：{len(filtered_items)}/{len(self._latest_position_history)}"
        if (
            POSITION_TYPE_OPTIONS.get(self.position_history_type_filter.get(), "")
            or HISTORY_MARGIN_MODE_FILTER_OPTIONS.get(self.position_history_margin_filter.get(), "")
            or self.position_history_asset_filter.get().strip()
            or self.position_history_expiry_prefix_filter.get().strip()
            or self.position_history_keyword.get().strip()
        ):
            summary = (
                f"{self._positions_zoom_position_history_base_summary} | \u5f53\u524d\u663e\u793a\uff1a{len(filtered_items)}/{len(self._latest_position_history)}"
                f"\n\u7b5b\u9009\u7edf\u8ba1\uff1a"
                f"{_format_position_history_filter_stats(filtered_items, self._position_history_usdt_prices)}"
            )
        self._positions_zoom_position_history_summary_text.set(summary)
        if selected_before and tree.exists(selected_before):
            tree.selection_set(selected_before)
            tree.focus(selected_before)
        elif tree.get_children():
            first = tree.get_children()[0]
            tree.selection_set(first)
            tree.focus(first)
        self._refresh_positions_zoom_position_history_detail()
        self._update_position_history_search_shortcuts()

    def _on_positions_zoom_fills_selected(self, *_: object) -> None:
        self._refresh_positions_zoom_fills_detail()
        self._update_fill_history_search_shortcuts()

    def _on_position_history_filter_changed(self, *_: object) -> None:
        self._render_positions_zoom_position_history_view()

    def reset_position_history_filters(self) -> None:
        self.position_history_type_filter.set("全部类型")
        self.position_history_margin_filter.set("全部模式")
        self.position_history_asset_filter.set("")
        self.position_history_expiry_prefix_filter.set("")
        self.position_history_keyword.set("")
        self._render_positions_zoom_position_history_view()

    def _on_positions_zoom_position_history_selected(self, *_: object) -> None:
        self._refresh_positions_zoom_position_history_detail()
        self._update_position_history_search_shortcuts()

    def _refresh_positions_zoom_fills_detail(self) -> None:
        if self._positions_zoom_fills_tree is None or self._positions_zoom_fills_detail is None:
            return
        selection = self._positions_zoom_fills_tree.selection()
        if not selection:
            self._set_readonly_text(self._positions_zoom_fills_detail, "这里会显示选中历史成交单的详情。")
            return
        index = _history_tree_index(selection[0], "fill")
        if index is None or index >= len(self._latest_fill_history):
            self._set_readonly_text(self._positions_zoom_fills_detail, "这里会显示选中历史成交单的详情。")
            return
        self._set_readonly_text(
            self._positions_zoom_fills_detail,
            _build_fill_history_detail_text(self._latest_fill_history[index], self._fill_history_instruments),
        )

    def _refresh_positions_zoom_position_history_detail(self) -> None:
        if self._positions_zoom_position_history_tree is None or self._positions_zoom_position_history_detail is None:
            return
        selection = self._positions_zoom_position_history_tree.selection()
        if not selection:
            self._set_readonly_text(self._positions_zoom_position_history_detail, "这里会显示选中历史仓位的详情。")
            return
        index = _history_tree_index(selection[0], "ph")
        if index is None or index >= len(self._latest_position_history):
            self._set_readonly_text(self._positions_zoom_position_history_detail, "这里会显示选中历史仓位的详情。")
            return
        self._set_readonly_text(
            self._positions_zoom_position_history_detail,
            _build_position_history_detail_text(
                self._latest_position_history[index],
                self._position_history_usdt_prices,
                self._position_history_instruments,
                note=self._position_history_note_text(self._latest_position_history[index]),
            ),
        )

    def _selected_option_position(self, *, prefer_protection_form: bool = False) -> OkxPosition | None:
        if prefer_protection_form and self._protection_form_position_key:
            fallback = _find_position_by_key(self._latest_positions, self._protection_form_position_key)
            if fallback is not None and fallback.inst_type == "OPTION":
                return fallback
        payload = None
        if self._positions_zoom_window is not None and self._positions_zoom_window.winfo_exists():
            if self._positions_zoom_selected_item_id:
                payload = self._position_row_payloads.get(self._positions_zoom_selected_item_id)
        if payload is None:
            payload = self._selected_position_payload()
        if payload is not None and payload["kind"] == "position":
            position = payload["item"]
            if isinstance(position, OkxPosition) and position.inst_type == "OPTION":
                return position
        if self._protection_form_position_key:
            fallback = _find_position_by_key(self._latest_positions, self._protection_form_position_key)
            if fallback is not None and fallback.inst_type == "OPTION":
                return fallback
        return None

    def open_position_protection_window(self) -> None:
        if self._protection_window is not None and self._protection_window.winfo_exists():
            self._populate_protection_form_from_selection(force=True)
            self._refresh_protection_window_view()
            self._protection_window.focus_force()
            return

        window = Toplevel(self.root)
        window.title("期权持仓保护")
        apply_adaptive_window_geometry(
            window,
            width_ratio=0.76,
            height_ratio=0.78,
            min_width=980,
            min_height=760,
            max_width=1520,
            max_height=1060,
        )
        self._protection_window = window
        window.protocol("WM_DELETE_WINDOW", self._close_position_protection_window)

        container = ttk.Frame(window, padding=16)
        container.pack(fill="both", expand=True)
        container.columnconfigure(0, weight=1)
        container.rowconfigure(1, weight=1)
        container.rowconfigure(2, weight=1)

        form_frame = ttk.LabelFrame(container, text="选中期权持仓保护", padding=16)
        form_frame.grid(row=0, column=0, sticky="ew")
        for column in range(4):
            form_frame.columnconfigure(column, weight=1)

        ttk.Label(
            form_frame,
            textvariable=self._protection_form_title_text,
            justify="left",
            wraplength=960,
            font=("Microsoft YaHei UI", 10, "bold"),
        ).grid(row=0, column=0, columnspan=4, sticky="w", pady=(0, 12))
        ttk.Label(
            form_frame,
            textvariable=self._protection_logic_hint_text,
            justify="left",
            wraplength=960,
            foreground="#8a4600",
        ).grid(row=1, column=0, columnspan=4, sticky="w", pady=(0, 8))

        row = 2
        ttk.Label(form_frame, text="触发条件").grid(row=row, column=0, sticky="w")
        protection_trigger_combo = ttk.Combobox(
            form_frame,
            textvariable=self.protection_trigger_source_label,
            values=list(PROTECTION_TRIGGER_SOURCE_OPTIONS.keys()),
            state="readonly",
        )
        protection_trigger_combo.grid(row=row, column=1, sticky="ew", padx=(0, 16))
        protection_trigger_combo.bind("<<ComboboxSelected>>", self._on_protection_trigger_source_changed)
        ttk.Label(form_frame, text="现货标的").grid(row=row, column=2, sticky="w")
        self._protection_spot_symbol_entry = ttk.Entry(form_frame, textvariable=self.protection_spot_symbol)
        self._protection_spot_symbol_entry.grid(row=row, column=3, sticky="ew")

        row += 1
        ttk.Label(form_frame, text="止盈触发价").grid(row=row, column=0, sticky="w", pady=(12, 0))
        ttk.Entry(form_frame, textvariable=self.protection_take_profit_trigger).grid(
            row=row, column=1, sticky="ew", padx=(0, 16), pady=(12, 0)
        )
        ttk.Label(form_frame, text="止损触发价").grid(row=row, column=2, sticky="w", pady=(12, 0))
        ttk.Entry(form_frame, textvariable=self.protection_stop_loss_trigger).grid(
            row=row, column=3, sticky="ew", pady=(12, 0)
        )

        row += 1
        ttk.Label(form_frame, text="止盈报单方式").grid(row=row, column=0, sticky="w", pady=(12, 0))
        take_profit_mode_combo = ttk.Combobox(
            form_frame,
            textvariable=self.protection_take_profit_order_mode_label,
            values=list(PROTECTION_ORDER_MODE_OPTIONS.keys()),
            state="readonly",
        )
        take_profit_mode_combo.grid(row=row, column=1, sticky="ew", padx=(0, 16), pady=(12, 0))
        take_profit_mode_combo.bind("<<ComboboxSelected>>", self._on_protection_order_mode_changed)
        ttk.Label(form_frame, text="止盈报单价格").grid(row=row, column=2, sticky="w", pady=(12, 0))
        self._protection_take_profit_order_price_entry = ttk.Entry(
            form_frame,
            textvariable=self.protection_take_profit_order_price,
        )
        self._protection_take_profit_order_price_entry.grid(row=row, column=3, sticky="ew", pady=(12, 0))

        row += 1
        ttk.Label(form_frame, text="止盈滑点").grid(row=row, column=0, sticky="w", pady=(12, 0))
        self._protection_take_profit_slippage_entry = ttk.Entry(
            form_frame,
            textvariable=self.protection_take_profit_slippage,
        )
        self._protection_take_profit_slippage_entry.grid(row=row, column=1, sticky="ew", padx=(0, 16), pady=(12, 0))
        ttk.Label(form_frame, text="轮询秒数").grid(row=row, column=2, sticky="w", pady=(12, 0))
        ttk.Entry(form_frame, textvariable=self.protection_poll_seconds).grid(row=row, column=3, sticky="ew", pady=(12, 0))

        row += 1
        ttk.Label(form_frame, text="止损报单方式").grid(row=row, column=0, sticky="w", pady=(12, 0))
        stop_loss_mode_combo = ttk.Combobox(
            form_frame,
            textvariable=self.protection_stop_loss_order_mode_label,
            values=list(PROTECTION_ORDER_MODE_OPTIONS.keys()),
            state="readonly",
        )
        stop_loss_mode_combo.grid(row=row, column=1, sticky="ew", padx=(0, 16), pady=(12, 0))
        stop_loss_mode_combo.bind("<<ComboboxSelected>>", self._on_protection_order_mode_changed)
        ttk.Label(form_frame, text="止损报单价格").grid(row=row, column=2, sticky="w", pady=(12, 0))
        self._protection_stop_loss_order_price_entry = ttk.Entry(
            form_frame,
            textvariable=self.protection_stop_loss_order_price,
        )
        self._protection_stop_loss_order_price_entry.grid(row=row, column=3, sticky="ew", pady=(12, 0))

        row += 1
        ttk.Label(form_frame, text="止损滑点").grid(row=row, column=0, sticky="w", pady=(12, 0))
        self._protection_stop_loss_slippage_entry = ttk.Entry(
            form_frame,
            textvariable=self.protection_stop_loss_slippage,
        )
        self._protection_stop_loss_slippage_entry.grid(row=row, column=1, sticky="ew", padx=(0, 16), pady=(12, 0))
        ttk.Label(
            form_frame,
            text="说明：触发条件可用“期权标记价格”或“现货最新价”；报单价格可用“设定价格”或“标记价格加减滑点”。",
            justify="left",
            wraplength=520,
        ).grid(row=row, column=2, columnspan=2, sticky="w", pady=(12, 0))

        action_frame = ttk.Frame(form_frame)
        action_frame.grid(row=row + 1, column=0, columnspan=4, sticky="e", pady=(16, 0))
        ttk.Button(action_frame, text="启动保护", command=self.start_selected_position_protection).grid(
            row=0, column=0, padx=(0, 8)
        )
        ttk.Button(action_frame, text="回放模拟", command=self.open_position_protection_replay_window).grid(
            row=0, column=1, padx=(0, 8)
        )
        ttk.Button(action_frame, text="停止选中任务", command=self.stop_selected_position_protection).grid(
            row=0, column=2, padx=(0, 8)
        )
        ttk.Button(action_frame, text="关闭", command=self._close_position_protection_window).grid(row=0, column=3)

        sessions_frame = ttk.LabelFrame(container, text="运行中的期权保护任务", padding=12)
        sessions_frame.grid(row=1, column=0, sticky="nsew", pady=(12, 0))
        sessions_frame.columnconfigure(0, weight=1)
        sessions_frame.rowconfigure(1, weight=1)
        sessions_header = ttk.Frame(sessions_frame)
        sessions_header.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 8))
        sessions_header.columnconfigure(0, weight=1)
        ttk.Label(sessions_header, textvariable=self._protection_status_text).grid(row=0, column=0, sticky="w")
        ttk.Button(sessions_header, text="清除已结束", command=self.clear_finished_position_protections).grid(
            row=0, column=1, sticky="e"
        )
        task_tree = ttk.Treeview(
            sessions_frame,
            columns=("api", "option", "trigger", "direction", "status", "started"),
            show="headings",
            selectmode="browse",
        )
        task_tree.heading("api", text="API")
        task_tree.heading("option", text="期权合约")
        task_tree.heading("trigger", text="触发条件")
        task_tree.heading("direction", text="方向")
        task_tree.heading("status", text="状态")
        task_tree.heading("started", text="启动时间")
        task_tree.column("api", width=96, anchor="center")
        task_tree.column("option", width=250, anchor="w")
        task_tree.column("trigger", width=180, anchor="w")
        task_tree.column("direction", width=80, anchor="center")
        task_tree.column("status", width=100, anchor="center")
        task_tree.column("started", width=120, anchor="center")
        task_tree.grid(row=1, column=0, sticky="nsew")
        task_tree.bind("<<TreeviewSelect>>", self._on_protection_session_selected)
        task_scroll = ttk.Scrollbar(sessions_frame, orient="vertical", command=task_tree.yview)
        task_scroll.grid(row=1, column=1, sticky="ns")
        task_tree.configure(yscrollcommand=task_scroll.set)
        self._protection_sessions_tree = task_tree

        detail_frame = ttk.LabelFrame(container, text="保护任务详情", padding=12)
        detail_frame.grid(row=2, column=0, sticky="nsew", pady=(12, 0))
        detail_frame.columnconfigure(0, weight=1)
        detail_frame.rowconfigure(0, weight=1)
        self._protection_detail_text = Text(
            detail_frame,
            height=8,
            wrap="word",
            font=("Microsoft YaHei UI", 10),
            relief="flat",
        )
        self._protection_detail_text.grid(row=0, column=0, sticky="nsew")
        detail_scroll = ttk.Scrollbar(detail_frame, orient="vertical", command=self._protection_detail_text.yview)
        detail_scroll.grid(row=0, column=1, sticky="ns")
        self._protection_detail_text.configure(yscrollcommand=detail_scroll.set)
        self._set_readonly_text(self._protection_detail_text, "请选择一个保护任务查看详情。")

        self._populate_protection_form_from_selection(force=True)
        self._refresh_protection_window_view()

    def _close_position_protection_window(self) -> None:
        if self._protection_order_mode_job is not None:
            try:
                self.root.after_cancel(self._protection_order_mode_job)
            except Exception:
                pass
            self._protection_order_mode_job = None
        if self._protection_window is not None and self._protection_window.winfo_exists():
            self._protection_window.destroy()
        self._protection_window = None
        self._protection_sessions_tree = None
        self._protection_detail_text = None
        self._protection_selected_session_id = None
        self._protection_form_position_id = None
        self._protection_form_position_key = None

    def _populate_protection_form_from_selection(self, *, force: bool = False) -> None:
        position = self._selected_option_position(prefer_protection_form=not force)
        if position is None:
            if self._positions_view_rendering and self._protection_form_position_key:
                return
            self._protection_form_position_id = None
            self._protection_form_position_key = None
            self._protection_form_title_text.set("当前没有选中期权持仓。请先在账户持仓里选中一条期权仓位。")
            self._protection_logic_hint_text.set("请先选择一条期权持仓，系统会显示当前组合下的止盈止损方向。")
            return

        direction = derive_position_direction(position)
        self._protection_form_title_text.set(
            f"当前选中：{position.inst_id} | 方向={direction.upper()} | 持仓量={format_decimal(position.position)} | "
            f"开仓均价={_format_optional_decimal(position.avg_price)}"
        )
        position_key = _position_tree_row_id(position)
        if force or self._protection_form_position_key != position_key:
            self._protection_form_position_id = position.inst_id
            self._protection_form_position_key = position_key
            self.protection_trigger_source_label.set("期权标记价格")
            self.protection_spot_symbol.set(infer_default_spot_inst_id(position.inst_id))
            self.protection_take_profit_trigger.set("")
            self.protection_stop_loss_trigger.set("")
            self.protection_take_profit_order_mode_label.set("标记价格加减滑点")
            self.protection_take_profit_order_price.set("")
            self.protection_take_profit_slippage.set("0")
            self.protection_stop_loss_order_mode_label.set("标记价格加减滑点")
            self.protection_stop_loss_order_price.set("")
            self.protection_stop_loss_slippage.set("0")
            self.protection_poll_seconds.set("2")
        self._on_protection_trigger_source_changed()
        self._update_protection_order_mode_widgets()
        self._update_protection_logic_hint()

    def _on_protection_trigger_source_changed(self, *_: object) -> None:
        if hasattr(self, "_protection_spot_symbol_entry"):
            state = "normal" if self.protection_trigger_source_label.get() == "现货最新价" else "disabled"
            self._protection_spot_symbol_entry.configure(state=state)
        self._update_protection_logic_hint()

    def _update_protection_logic_hint(self) -> None:
        position = self._selected_option_position(prefer_protection_form=True)
        if position is None:
            self._protection_logic_hint_text.set("请先选择一条期权持仓，系统会显示当前组合下的止盈止损方向。")
            return
        trigger_source = PROTECTION_TRIGGER_SOURCE_OPTIONS.get(self.protection_trigger_source_label.get(), "option_mark")
        if trigger_source == "option_mark":
            trigger_inst_id = position.inst_id
            trigger_price_type = "mark"
        else:
            trigger_inst_id = normalize_spot_inst_id(self.protection_spot_symbol.get()) or infer_default_spot_inst_id(position.inst_id)
            trigger_price_type = "last"
        self._protection_logic_hint_text.set(
            describe_protection_price_logic(
                option_inst_id=position.inst_id,
                direction=derive_position_direction(position),
                trigger_inst_id=trigger_inst_id,
                trigger_price_type=trigger_price_type,  # type: ignore[arg-type]
            )
        )

    def _on_protection_order_mode_changed(self, *_: object) -> None:
        if self._protection_order_mode_job is not None:
            try:
                self.root.after_cancel(self._protection_order_mode_job)
            except Exception:
                pass
        self._protection_order_mode_job = self.root.after(1, self._apply_protection_order_mode_widgets)

    def _apply_protection_order_mode_widgets(self) -> None:
        self._protection_order_mode_job = None
        self._update_protection_order_mode_widgets()

    def _update_protection_order_mode_widgets(self) -> None:
        self._sync_protection_order_mode_widget(
            mode_label=self.protection_take_profit_order_mode_label.get(),
            price_var=self.protection_take_profit_order_price,
            price_entry=self._protection_take_profit_order_price_entry,
            slippage_entry=self._protection_take_profit_slippage_entry,
            memory_attr="_protection_take_profit_fixed_price_memory",
        )
        self._sync_protection_order_mode_widget(
            mode_label=self.protection_stop_loss_order_mode_label.get(),
            price_var=self.protection_stop_loss_order_price,
            price_entry=self._protection_stop_loss_order_price_entry,
            slippage_entry=self._protection_stop_loss_slippage_entry,
            memory_attr="_protection_stop_loss_fixed_price_memory",
        )

    def _sync_protection_order_mode_widget(
        self,
        *,
        mode_label: str,
        price_var: StringVar,
        price_entry: ttk.Entry | None,
        slippage_entry: ttk.Entry | None,
        memory_attr: str,
    ) -> None:
        placeholder = "自动按标记价与滑点计算"
        fixed_mode = _resolve_protection_order_mode_value(mode_label) == "fixed_price"
        if fixed_mode:
            if price_var.get() == placeholder:
                price_var.set(getattr(self, memory_attr, ""))
            if price_entry is not None:
                price_entry.configure(state="normal")
            if slippage_entry is not None:
                slippage_entry.configure(state="disabled")
        else:
            current = price_var.get().strip()
            if current and current != placeholder:
                setattr(self, memory_attr, current)
            price_var.set(placeholder)
            if price_entry is not None:
                price_entry.configure(state="readonly")
            if slippage_entry is not None:
                slippage_entry.configure(state="normal")

    def _refresh_protection_window_view(self) -> None:
        if self._protection_window is None or not self._protection_window.winfo_exists():
            return
        self._populate_protection_form_from_selection(force=False)
        sessions = self._protection_manager.list_sessions()
        self._protection_status_text.set(
            f"当前保护任务：{len(sessions)}"
            if sessions
            else "当前没有运行中的期权持仓保护任务。"
        )
        if self._protection_sessions_tree is not None:
            selected_before = self._protection_sessions_tree.selection()
            self._protection_sessions_tree.delete(*self._protection_sessions_tree.get_children())
            for session in sessions:
                self._protection_sessions_tree.insert(
                    "",
                    END,
                    iid=session.session_id,
                    values=(
                        session.api_name or "-",
                        session.option_inst_id,
                        session.trigger_label,
                        session.direction,
                        session.status,
                        session.started_at.strftime("%H:%M:%S"),
                    ),
                )
            if selected_before and self._protection_sessions_tree.exists(selected_before[0]):
                target = selected_before[0]
            else:
                target = sessions[0].session_id if sessions else None
            if target is not None:
                self._protection_sessions_tree.selection_set(target)
                self._protection_sessions_tree.focus(target)
                self._protection_selected_session_id = target
            else:
                self._protection_selected_session_id = None
        self._refresh_protection_detail_panel()

    def _on_protection_session_selected(self, *_: object) -> None:
        if self._protection_sessions_tree is None:
            return
        selection = self._protection_sessions_tree.selection()
        self._protection_selected_session_id = selection[0] if selection else None
        self._refresh_protection_detail_panel()

    def _refresh_protection_detail_panel(self) -> None:
        if self._protection_detail_text is None:
            return
        sessions = {item.session_id: item for item in self._protection_manager.list_sessions()}
        session = sessions.get(self._protection_selected_session_id or "")
        if session is None:
            self._set_readonly_text(self._protection_detail_text, "请选择一个保护任务查看详情。")
            return
        self._set_readonly_text(
            self._protection_detail_text,
            "\n".join(
                [
                    f"任务：{session.session_id}",
                    f"API配置：{session.api_name or '-'}",
                    f"期权合约：{session.option_inst_id}",
                    f"触发条件：{session.trigger_label}",
                    f"触发标的：{session.trigger_inst_id}",
                    f"触发价格类型：{_format_protection_trigger_price_type(session.trigger_price_type)}",
                    f"方向：{session.direction}",
                    f"持仓方向：{session.pos_side or '-'}",
                    f"止盈触发：{_format_optional_decimal(session.take_profit_trigger)}",
                    f"止盈报单方式：{_format_protection_order_mode_label(session.take_profit_order_mode)}",
                    f"止盈报单价格：{_format_protection_order_price_detail(session.take_profit_order_mode, session.take_profit_order_price)}",
                    f"止盈滑点：{_format_optional_decimal(session.take_profit_slippage)}",
                    f"止损触发：{_format_optional_decimal(session.stop_loss_trigger)}",
                    f"止损报单方式：{_format_protection_order_mode_label(session.stop_loss_order_mode)}",
                    f"止损报单价格：{_format_protection_order_price_detail(session.stop_loss_order_mode, session.stop_loss_order_price)}",
                    f"止损滑点：{_format_optional_decimal(session.stop_loss_slippage)}",
                    f"轮询秒数：{session.poll_seconds:g}",
                    f"状态：{session.status}",
                    f"启动时间：{session.started_at.strftime('%Y-%m-%d %H:%M:%S')}",
                    "",
                    f"最新状态：{session.last_message}",
                ]
            ),
        )

    def start_selected_position_protection(self) -> None:
        position = self._selected_option_position(prefer_protection_form=True)
        if position is None:
            messagebox.showinfo("提示", "请先在账户持仓中选中一条期权仓位。", parent=self._protection_window or self.root)
            return
        credentials = self._current_credentials_or_none()
        if credentials is None:
            messagebox.showerror("启动失败", "请先在设置里配置 API 凭证。", parent=self._protection_window or self.root)
            return
        if not self._guard_live_action_against_stale_cache(
            self._positions_refresh_health,
            action_label="启动持仓保护",
            data_label="持仓",
            parent=self._protection_window or self.root,
            refresh_callback=self.refresh_positions,
        ):
            return

        try:
            notifier = self._build_optional_protection_notifier()
            self._protection_manager.set_notifier(notifier)
            protection = self._build_selected_position_protection(position)
            _validate_protection_live_price_availability(self.client, protection, position)
            config = self._build_manual_protection_strategy_config(position, protection)
            session_id = self._protection_manager.start(credentials, config, protection)
            if credentials.profile_name:
                self._enqueue_log(f"[持仓保护 {credentials.profile_name} {session_id}] 已启动 {position.inst_id} 的期权保护任务。")
            else:
                self._enqueue_log(f"[持仓保护 {session_id}] 已启动 {position.inst_id} 的期权保护任务。")
            self._refresh_protection_window_view()
        except Exception as exc:
            messagebox.showerror("启动保护失败", str(exc), parent=self._protection_window or self.root)

    def stop_selected_position_protection(self) -> None:
        if not self._protection_selected_session_id:
            messagebox.showinfo("提示", "请先在下方列表中选中一个保护任务。", parent=self._protection_window or self.root)
            return
        try:
            self._protection_manager.stop(self._protection_selected_session_id)
            self._refresh_protection_window_view()
        except Exception as exc:
            messagebox.showerror("停止保护失败", str(exc), parent=self._protection_window or self.root)

    def open_position_protection_replay_window(self) -> None:
        position = self._selected_option_position(prefer_protection_form=True)
        if position is None:
            messagebox.showinfo("提示", "请先在账户持仓中选中一条期权仓位。", parent=self._protection_window or self.root)
            return
        try:
            protection = self._build_selected_position_protection(position)
        except Exception as exc:
            messagebox.showerror("回放参数错误", str(exc), parent=self._protection_window or self.root)
            return

        if self._protection_replay_window is not None and self._protection_replay_window.window.winfo_exists():
            self._protection_replay_window.window.destroy()

        self._protection_replay_window = ProtectionReplayWindow(
            self.root,
            self.client,
            position,
            protection,
            initial_state=ProtectionReplayLaunchState(bar=self.bar.get(), candle_limit="120"),
        )

    def clear_finished_position_protections(self) -> None:
        cleared = self._protection_manager.clear_finished()
        self._refresh_protection_window_view()
        if cleared <= 0:
            messagebox.showinfo("提示", "当前没有可清除的已结束任务。", parent=self._protection_window or self.root)

    def _build_selected_position_protection(self, position: OkxPosition) -> OptionProtectionConfig:
        trigger_source = PROTECTION_TRIGGER_SOURCE_OPTIONS[self.protection_trigger_source_label.get()]
        if trigger_source == "option_mark":
            trigger_inst_id = position.inst_id
            trigger_price_type = "mark"
            trigger_label = f"{position.inst_id} 标记价"
        else:
            trigger_inst_id = normalize_spot_inst_id(self.protection_spot_symbol.get())
            if not trigger_inst_id:
                raise ValueError("现货触发模式下，请填写现货标的。")
            trigger_instrument = self.client.get_instrument(trigger_inst_id)
            if trigger_instrument.inst_type != "SPOT":
                raise ValueError("现货触发模式下，请填写现货交易对，例如 BTC-USDT。")
            trigger_price_type = "last"
            trigger_label = f"{trigger_inst_id} 最新价"

        take_profit_trigger = self._parse_optional_positive_decimal(self.protection_take_profit_trigger.get(), "止盈触发价")
        stop_loss_trigger = self._parse_optional_positive_decimal(self.protection_stop_loss_trigger.get(), "止损触发价")
        if take_profit_trigger is None and stop_loss_trigger is None:
            raise ValueError("止盈触发价和止损触发价至少要填写一个。")

        direction = derive_position_direction(position)
        _validate_protection_price_relationship(
            option_inst_id=position.inst_id,
            direction=direction,
            trigger_inst_id=trigger_inst_id,
            trigger_price_type=trigger_price_type,
            take_profit=take_profit_trigger,
            stop_loss=stop_loss_trigger,
        )

        take_profit_order_mode = PROTECTION_ORDER_MODE_OPTIONS[self.protection_take_profit_order_mode_label.get()]
        stop_loss_order_mode = PROTECTION_ORDER_MODE_OPTIONS[self.protection_stop_loss_order_mode_label.get()]
        take_profit_order_price = self._parse_protection_order_price(
            self.protection_take_profit_order_price.get(),
            "止盈报单价格",
            take_profit_order_mode,
        )
        stop_loss_order_price = self._parse_protection_order_price(
            self.protection_stop_loss_order_price.get(),
            "止损报单价格",
            stop_loss_order_mode,
        )
        return OptionProtectionConfig(
            option_inst_id=position.inst_id,
            trigger_inst_id=trigger_inst_id,
            trigger_price_type=trigger_price_type,
            direction=direction,
            pos_side=position.pos_side if position.pos_side and position.pos_side.lower() != "net" else None,
            take_profit_trigger=take_profit_trigger,
            stop_loss_trigger=stop_loss_trigger,
            take_profit_order_mode=take_profit_order_mode,
            take_profit_order_price=take_profit_order_price,
            take_profit_slippage=self._parse_nonnegative_decimal(self.protection_take_profit_slippage.get(), "止盈滑点"),
            stop_loss_order_mode=stop_loss_order_mode,
            stop_loss_order_price=stop_loss_order_price,
            stop_loss_slippage=self._parse_nonnegative_decimal(self.protection_stop_loss_slippage.get(), "止损滑点"),
            poll_seconds=float(self._parse_positive_decimal(self.protection_poll_seconds.get(), "轮询秒数")),
            trigger_label=trigger_label,
        )

    def _parse_protection_order_price(self, raw: str, field_name: str, order_mode: str) -> Decimal | None:
        if order_mode != "fixed_price":
            return None
        return self._parse_positive_decimal(raw, field_name)

    def _parse_nonnegative_decimal(self, raw: str, field_name: str) -> Decimal:
        cleaned = raw.strip()
        if not cleaned:
            return Decimal("0")
        try:
            value = Decimal(cleaned)
        except InvalidOperation as exc:
            raise ValueError(f"{field_name} 不是有效数字") from exc
        if value < 0:
            raise ValueError(f"{field_name} 不能小于 0")
        return value

    def _build_manual_protection_strategy_config(
        self,
        position: OkxPosition,
        protection: OptionProtectionConfig,
    ) -> StrategyConfig:
        environment = self._positions_effective_environment or ENV_OPTIONS[self.environment_label.get()]
        trade_mode = position.mgn_mode if position.mgn_mode in {"cross", "isolated"} else TRADE_MODE_OPTIONS[self.trade_mode_label.get()]
        position_mode = "long_short" if position.pos_side and position.pos_side.lower() != "net" else "net"
        return StrategyConfig(
            inst_id=protection.trigger_inst_id,
            bar=self.bar.get(),
            ema_period=1,
            atr_period=1,
            atr_stop_multiplier=Decimal("1"),
            atr_take_multiplier=Decimal("1"),
            order_size=abs(position.position),
            trade_mode=trade_mode,
            signal_mode="long_only" if protection.direction == "long" else "short_only",
            position_mode=position_mode,
            environment=environment,
            tp_sl_trigger_type=protection.trigger_price_type,
            strategy_id="manual_option_protection",
            poll_seconds=protection.poll_seconds,
            risk_amount=None,
            trade_inst_id=position.inst_id,
            tp_sl_mode="local_trade",
            local_tp_sl_inst_id=protection.trigger_inst_id,
            entry_side_mode="follow_signal",
            run_mode="trade",
        )

    def _build_optional_protection_notifier(self) -> EmailNotifier | None:
        notification_config = self._collect_notification_config(validate_if_enabled=False)
        if not notification_config.enabled:
            return None
        return EmailNotifier(notification_config, logger=self._make_system_logger("邮件 持仓保护"))

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

    def open_backtest_window(self) -> None:
        if self._backtest_window is not None and self._backtest_window.window.winfo_exists():
            self._backtest_window.window.focus_force()
            return

        backtest_strategy_name = self.strategy_name.get()
        backtest_strategy_names = {item.name for item in BACKTEST_STRATEGY_DEFINITIONS}
        if backtest_strategy_name not in backtest_strategy_names:
            backtest_strategy_name = BACKTEST_STRATEGY_DEFINITIONS[0].name

        self._backtest_window = BacktestWindow(
            self.root,
            self.client,
            BacktestLaunchState(
                strategy_name=backtest_strategy_name,
                symbol=_normalize_symbol_input(self.symbol.get()),
                bar=self.bar.get(),
                ema_period=self.ema_period.get(),
                trend_ema_period=self.trend_ema_period.get(),
                big_ema_period=self.big_ema_period.get(),
                entry_reference_ema_period=self.entry_reference_ema_period.get(),
                atr_period=self.atr_period.get(),
                stop_atr=self.stop_atr.get(),
                take_atr=self.take_atr.get(),
                risk_amount=self.risk_amount.get(),
                take_profit_mode_label=self.take_profit_mode_label.get(),
                max_entries_per_trend=self.max_entries_per_trend.get(),
                dynamic_two_r_break_even=self.dynamic_two_r_break_even.get(),
                dynamic_fee_offset_enabled=self.dynamic_fee_offset_enabled.get(),
                time_stop_break_even_enabled=self.time_stop_break_even_enabled.get(),
                time_stop_break_even_bars=self.time_stop_break_even_bars.get(),
                signal_mode_label=self.signal_mode_label.get(),
                trade_mode_label=self.trade_mode_label.get(),
                position_mode_label=self.position_mode_label.get(),
                trigger_type_label=self.trigger_type_label.get(),
                environment_label=self.environment_label.get(),
                maker_fee_percent="0.015",
                taker_fee_percent="0.036",
                initial_capital="10000",
                sizing_mode_label="固定风险金",
                risk_percent="1",
                compounding_enabled=False,
                entry_slippage_percent="0",
                exit_slippage_percent="0",
                funding_rate_percent="0",
                candle_limit="10000",
            ),
        )

    def open_smart_order_window(self) -> None:
        if self._smart_order_window is not None and self._smart_order_window.window.winfo_exists():
            self._smart_order_window.show()
            return
        self._smart_order_window = SmartOrderWindow(
            self.root,
            self.client,
            runtime_config_provider=self._build_smart_order_runtime_config_or_none,
            logger=self._enqueue_log,
        )

    def open_backtest_compare_window(self) -> None:
        if self._backtest_compare_window is not None and self._backtest_compare_window.window.winfo_exists():
            self._backtest_compare_window.show()
            return
        self._backtest_compare_window = BacktestCompareOverviewWindow(self.root)

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
            trader_flattener=self.flatten_trader_draft,
            trader_force_cleaner=self.force_clear_trader_draft,
            symbol_provider=self._trader_desk_symbol_choices,
            runtime_snapshot_provider=self._trader_runtime_snapshot_for_ui,
        )

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
        self._default_environment_label = self._normalized_environment_label(str(snapshot["environment_label"]))
        self.environment_label.set(self._default_environment_label)
        self._apply_profile_environment(self._current_credential_profile())
        self._last_saved_notification_state = self._current_notification_state()

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

    def _update_fixed_order_size_hint(self, *_: str) -> None:
        symbol = _normalize_symbol_input(self.trade_symbol.get()) or _normalize_symbol_input(self.symbol.get())
        instrument = self._find_instrument_for_fixed_order_size_hint(symbol)
        base_hint = _build_fixed_order_size_hint_text(symbol, instrument)
        mode_hint = _build_order_size_mode_hint_text(self.risk_amount.get(), self.order_size.get())
        self.fixed_order_size_hint_text.set(f"{base_hint} {mode_hint}".strip())

    def _find_instrument_for_fixed_order_size_hint(self, inst_id: str) -> Instrument | None:
        normalized = _normalize_symbol_input(inst_id)
        if not normalized:
            return None
        for instrument in self.instruments:
            if instrument.inst_id.strip().upper() == normalized:
                return instrument
        instrument = self._position_instruments.get(normalized)
        if instrument is not None:
            return instrument
        try:
            return self.client.get_instrument(normalized)
        except Exception:
            return None

    @staticmethod
    def _format_strategy_symbol_display(signal_symbol: str, trade_symbol: str | None) -> str:
        normalized_signal = signal_symbol.strip().upper()
        normalized_trade = (trade_symbol or normalized_signal).strip().upper()
        if not normalized_signal:
            return normalized_trade
        if normalized_trade == normalized_signal:
            return normalized_signal
        return f"{normalized_signal} -> {normalized_trade}"

    @staticmethod
    def _default_strategy_template_filename(record: StrategyTemplateRecord) -> str:
        raw = f"{record.strategy_name or record.strategy_id}_{record.symbol or 'strategy'}"
        sanitized = re.sub(r'[\\/:*?"<>|]+', "_", raw).strip(" ._")
        return f"{sanitized or 'strategy_template'}.json"

    def _resolve_strategy_template_definition(self, record: StrategyTemplateRecord) -> StrategyDefinition:
        try:
            return get_strategy_definition(record.strategy_id)
        except KeyError:
            if record.strategy_name and record.strategy_name in self._strategy_name_to_id:
                return get_strategy_definition(self._strategy_name_to_id[record.strategy_name])
        raise ValueError(f"当前版本不认识这个策略：{record.strategy_id}")

    def _ensure_importable_strategy_symbols(self, symbol: str, local_tp_sl_symbol: str | None = None) -> None:
        normalized_symbol = _normalize_symbol_input(symbol)
        if normalized_symbol and normalized_symbol not in self._default_symbol_values:
            self._default_symbol_values.append(normalized_symbol)
        merged = list(dict.fromkeys(self._default_symbol_values))
        custom_values = ["", *merged]
        normalized_local = _normalize_symbol_input(local_tp_sl_symbol or "")
        if normalized_local and normalized_local not in custom_values:
            custom_values.append(normalized_local)
        self._default_symbol_values = merged
        self._custom_trigger_symbol_values = custom_values
        if hasattr(self, "symbol_combo") and self.symbol_combo is not None:
            self.symbol_combo["values"] = merged
        if hasattr(self, "local_tp_sl_symbol_combo") and self.local_tp_sl_symbol_combo is not None:
            self.local_tp_sl_symbol_combo["values"] = custom_values

    def _apply_strategy_template_record(self, record: StrategyTemplateRecord) -> tuple[StrategyDefinition, str, str]:
        definition = self._resolve_strategy_template_definition(record)
        resolved_api_name, api_note = _resolve_import_api_profile(
            record.api_name,
            self._current_credential_profile(),
            set(self._credential_profiles.keys()),
        )
        if (
            resolved_api_name
            and resolved_api_name in self._credential_profiles
            and resolved_api_name != self._current_credential_profile()
        ):
            self._apply_credentials_profile(resolved_api_name, log_change=False)

        self.strategy_name.set(definition.name)
        self._on_strategy_selected()

        launch_symbol = _launcher_symbol_from_strategy_config(definition.strategy_id, record.config)
        custom_symbol = (record.config.local_tp_sl_inst_id or "").strip().upper()
        self._ensure_importable_strategy_symbols(launch_symbol, custom_symbol)

        self.symbol.set(launch_symbol)
        self.trade_symbol.set(launch_symbol)
        self.local_tp_sl_symbol.set(custom_symbol)
        self.bar.set(record.config.bar)
        self.ema_period.set(str(record.config.ema_period))
        self.trend_ema_period.set(str(record.config.trend_ema_period))
        self.big_ema_period.set(str(record.config.big_ema_period))
        self.entry_reference_ema_period.set(str(record.config.entry_reference_ema_period))
        self.atr_period.set(str(record.config.atr_period))
        self.stop_atr.set(_format_entry_decimal(record.config.atr_stop_multiplier))
        self.take_atr.set(_format_entry_decimal(record.config.atr_take_multiplier))
        self.risk_amount.set(_format_entry_decimal(record.config.risk_amount))
        self.order_size.set(_format_entry_decimal(record.config.order_size))
        self.poll_seconds.set(_format_entry_float(record.config.poll_seconds))
        self.signal_mode_label.set(
            _strategy_template_direction_label(
                definition.strategy_id,
                record.config,
                fallback=record.direction_label or definition.default_signal_label,
            )
        )
        self.take_profit_mode_label.set(
            _reverse_lookup_label(TAKE_PROFIT_MODE_OPTIONS, record.config.take_profit_mode, "动态止盈")
        )
        self.max_entries_per_trend.set(str(record.config.max_entries_per_trend))
        self.startup_chase_window_seconds.set(str(record.config.startup_chase_window_seconds))
        self.dynamic_two_r_break_even.set(record.config.dynamic_two_r_break_even)
        self.dynamic_fee_offset_enabled.set(record.config.dynamic_fee_offset_enabled)
        self.time_stop_break_even_enabled.set(record.config.time_stop_break_even_enabled)
        self.time_stop_break_even_bars.set(str(record.config.time_stop_break_even_bars))
        self.run_mode_label.set(_reverse_lookup_label(RUN_MODE_OPTIONS, record.config.run_mode, record.run_mode_label))
        self.trade_mode_label.set(_reverse_lookup_label(TRADE_MODE_OPTIONS, record.config.trade_mode, "全仓 cross"))
        self.position_mode_label.set(
            _reverse_lookup_label(POSITION_MODE_OPTIONS, record.config.position_mode, "净持仓 net")
        )
        self.trigger_type_label.set(
            _reverse_lookup_label(TRIGGER_TYPE_OPTIONS, record.config.tp_sl_trigger_type, "标记价格 mark")
        )
        self.tp_sl_mode_label.set(_launcher_tp_sl_mode_label(record.config.tp_sl_mode))
        self.entry_side_mode_label.set(
            _reverse_lookup_label(ENTRY_SIDE_MODE_OPTIONS, record.config.entry_side_mode, "跟随信号")
        )
        self.environment_label.set(_reverse_lookup_label(ENV_OPTIONS, record.config.environment, "模拟盘 demo"))
        self._sync_dynamic_take_profit_controls()
        QuantApp._sync_entry_side_mode_controls(self)
        return definition, resolved_api_name, api_note

    def export_selected_session_template(self) -> None:
        session = self._selected_session()
        if session is None:
            messagebox.showinfo("提示", "请先在运行中策略列表中选中一条策略。")
            return
        record = _strategy_template_record_from_payload(_build_strategy_template_payload(session))
        if record is None:
            messagebox.showerror("导出失败", "当前选中策略缺少可导出的有效配置。")
            return
        target = filedialog.asksaveasfilename(
            parent=self.root,
            title="导出策略参数",
            defaultextension=".json",
            filetypes=(("JSON 文件", "*.json"), ("所有文件", "*.*")),
            initialfile=self._default_strategy_template_filename(record),
        )
        if not target:
            return
        payload = _build_strategy_template_payload(session)
        try:
            Path(target).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception as exc:
            messagebox.showerror("导出失败", f"写入策略参数文件失败：{exc}")
            return
        self._enqueue_log(f"[{session.session_id}] 已导出策略参数：{target}")
        messagebox.showinfo(
            "导出完成",
            "已导出策略参数文件。\n\n文件不包含 API 密钥，可在其他机器导入后复用当前参数。",
        )

    def import_strategy_template(self) -> None:
        source = filedialog.askopenfilename(
            parent=self.root,
            title="导入策略参数",
            filetypes=(("JSON 文件", "*.json"), ("所有文件", "*.*")),
        )
        if not source:
            return
        try:
            payload = json.loads(Path(source).read_text(encoding="utf-8"))
            record = _strategy_template_record_from_payload(payload)
            if record is None:
                raise ValueError("文件缺少有效的策略配置快照。")
            definition, resolved_api_name, api_note = self._apply_strategy_template_record(record)
        except Exception as exc:
            messagebox.showerror("导入失败", str(exc))
            return

        applied_api = resolved_api_name or self._current_credential_profile()
        self._finish_strategy_template_import(
            source=source,
            record=record,
            definition=definition,
            applied_api=applied_api,
            api_note=api_note,
        )

    @staticmethod
    def _session_blocks_duplicate_launch(session: StrategySession) -> bool:
        return session.engine.is_running or session.status in {"运行中", "停止中"}

    @staticmethod
    def _format_duplicate_launch_block_message(session: StrategySession, *, imported: bool) -> str:
        headline = "已导入参数，但检测到重复策略：" if imported else "检测到重复策略启动："
        guidance = (
            "当前参数已经回填到启动区。\n如需复制参数开新策略，请先修改标的或切换 API 后再启动。"
            if imported
            else "当前已经存在同 API、同参数、同标的的策略会话。\n请先停止、恢复或清理原会话；如需复制参数开新策略，请先修改标的或切换 API 后再启动。"
        )
        return (
            f"{headline}\n\n"
            f"API：{session.api_name or '-'}\n"
            f"会话：{session.session_id}\n"
            f"状态：{session.display_status}\n"
            f"启动时间：{session.started_at.strftime('%H:%M:%S')}\n"
            f"标的：{session.symbol or '-'}\n\n"
            f"{guidance}"
        )

    def _find_duplicate_strategy_session(self, *, api_name: str, config: StrategyConfig) -> StrategySession | None:
        target_api_name = api_name.strip()
        for session in self.sessions.values():
            if session.api_name.strip() != target_api_name:
                continue
            if session.config != config:
                continue
            if not self._session_blocks_duplicate_launch(session):
                continue
            return session
        return None

    def _focus_session_row(self, session_id: str) -> None:
        if not self.session_tree.exists(session_id):
            return
        self.session_tree.selection_set(session_id)
        self.session_tree.focus(session_id)
        self._refresh_selected_session_details()

    def open_selected_strategy_live_chart(self) -> None:
        session = self._selected_session()
        if session is None:
            messagebox.showinfo("\u63d0\u793a", "\u8bf7\u5148\u5728\u8fd0\u884c\u4e2d\u7b56\u7565\u5217\u8868\u4e2d\u9009\u4e2d\u4e00\u6761\u7b56\u7565\u3002")
            return
        self.open_strategy_live_chart_window(session.session_id)

    def open_strategy_live_chart_window(self, session_id: str) -> None:
        session = self.sessions.get(session_id)
        if session is None:
            messagebox.showwarning("\u63d0\u793a", "\u5f53\u524d\u4f1a\u8bdd\u5df2\u4e0d\u5b58\u5728\uff0c\u8bf7\u91cd\u65b0\u9009\u62e9\u3002")
            return
        existing = self._strategy_live_chart_windows.get(session_id)
        if existing is not None and _widget_exists(existing.window):
            existing.window.focus_force()
            self._request_strategy_live_chart_refresh(session_id, immediate=True)
            return

        window = Toplevel(self.root)
        apply_window_icon(window)
        window.title(self._strategy_live_chart_window_title(session))
        apply_adaptive_window_geometry(
            window,
            width_ratio=0.76,
            height_ratio=0.7,
            min_width=1080,
            min_height=700,
            max_width=1760,
            max_height=1180,
        )
        window.transient(self.root)

        container = ttk.Frame(window, padding=12)
        container.pack(fill="both", expand=True)
        container.columnconfigure(0, weight=1)
        container.rowconfigure(1, weight=1)

        headline_text = StringVar(value=self._strategy_live_chart_headline(session))
        status_text = StringVar(value="\u6b63\u5728\u51c6\u5907\u5b9e\u65f6K\u7ebf\u56fe...")
        footer_text = StringVar(
            value=(
                f"\u53ea\u8bfb\u76d1\u63a7\u7a97\uff1a\u9ed8\u8ba4\u6bcf {DEFAULT_STRATEGY_LIVE_CHART_REFRESH_MS // 1000} \u79d2\u5237\u65b0\u4e00\u6b21\uff0c"
                "\u7a97\u53e3\u5173\u95ed\u540e\u81ea\u52a8\u505c\u6b62\u3002"
            )
        )

        header = ttk.Frame(container)
        header.grid(row=0, column=0, sticky="ew", pady=(0, 10))
        header.columnconfigure(0, weight=1)
        ttk.Label(
            header,
            textvariable=headline_text,
            font=("Microsoft YaHei UI", 11, "bold"),
            justify="left",
        ).grid(row=0, column=0, sticky="w")
        ttk.Label(
            header,
            textvariable=status_text,
            justify="left",
            wraplength=980,
        ).grid(row=1, column=0, sticky="w", pady=(6, 0))

        chart_frame = ttk.Frame(container)
        chart_frame.grid(row=1, column=0, sticky="nsew")
        chart_frame.columnconfigure(0, weight=1)
        chart_frame.rowconfigure(0, weight=1)
        canvas = Canvas(chart_frame, background="#ffffff", highlightthickness=0, width=1120, height=620)
        canvas.grid(row=0, column=0, sticky="nsew")

        footer = ttk.Frame(container)
        footer.grid(row=2, column=0, sticky="ew", pady=(10, 0))
        footer.columnconfigure(0, weight=1)
        ttk.Label(footer, textvariable=footer_text, justify="left", wraplength=980).grid(row=0, column=0, sticky="w")
        action_row = ttk.Frame(footer)
        action_row.grid(row=0, column=1, sticky="e")
        ttk.Button(
            action_row,
            text="\u7acb\u5373\u5237\u65b0",
            command=lambda target_session_id=session_id: self._request_strategy_live_chart_refresh(
                target_session_id, immediate=True
            ),
        ).grid(row=0, column=0, padx=(0, 6))
        ttk.Button(
            action_row,
            text="\u5173\u95ed",
            command=lambda target_session_id=session_id: self._close_strategy_live_chart_window(target_session_id),
        ).grid(row=0, column=1)

        state = StrategyLiveChartWindowState(
            session_id=session_id,
            window=window,
            canvas=canvas,
            headline_text=headline_text,
            status_text=status_text,
            footer_text=footer_text,
        )
        self._strategy_live_chart_windows[session_id] = state
        window.protocol("WM_DELETE_WINDOW", lambda target_session_id=session_id: self._close_strategy_live_chart_window(target_session_id))
        canvas.bind("<Configure>", lambda *_args, target_session_id=session_id: self._render_strategy_live_chart_window(target_session_id))
        self._render_strategy_live_chart_window(session_id)
        self._request_strategy_live_chart_refresh(session_id, immediate=True)

    def _strategy_live_chart_window_title(self, session: StrategySession) -> str:
        trade_inst_id = _session_trade_inst_id(session) or session.symbol
        return f"\u5b9e\u65f6K\u7ebf\u56fe - {session.session_id} {trade_inst_id}"

    def _strategy_live_chart_headline(self, session: StrategySession) -> str:
        trade_inst_id = _session_trade_inst_id(session) or session.symbol
        return (
            f"{session.session_id} | {session.strategy_name} | {trade_inst_id} | "
            f"\u5468\u671f {session.config.bar} | API {session.api_name} | \u6a21\u5f0f {session.run_mode_label}"
        )

    def _close_strategy_live_chart_window(self, session_id: str) -> None:
        state = self._strategy_live_chart_windows.pop(session_id, None)
        if state is None:
            return
        if state.refresh_job is not None:
            try:
                self.root.after_cancel(state.refresh_job)
            except TclError:
                pass
            state.refresh_job = None
        if _widget_exists(state.window):
            state.window.destroy()

    def _close_all_strategy_live_chart_windows(self) -> None:
        for session_id in tuple(self._strategy_live_chart_windows):
            self._close_strategy_live_chart_window(session_id)

    def _request_strategy_live_chart_refresh(self, session_id: str, *, immediate: bool = False) -> None:
        state = self._strategy_live_chart_windows.get(session_id)
        if state is None or not _widget_exists(state.window):
            return
        if state.refresh_job is not None:
            try:
                self.root.after_cancel(state.refresh_job)
            except TclError:
                pass
            state.refresh_job = None
        delay = 0 if immediate else DEFAULT_STRATEGY_LIVE_CHART_REFRESH_MS
        state.refresh_job = self.root.after(delay, lambda target_session_id=session_id: self._run_strategy_live_chart_refresh(target_session_id))

    def _run_strategy_live_chart_refresh(self, session_id: str) -> None:
        state = self._strategy_live_chart_windows.get(session_id)
        if state is None or not _widget_exists(state.window):
            return
        state.refresh_job = None
        if state.refresh_inflight:
            self._request_strategy_live_chart_refresh(session_id, immediate=False)
            return
        session = self.sessions.get(session_id)
        if session is not None:
            state.headline_text.set(self._strategy_live_chart_headline(session))
            state.status_text.set(f"\u72b6\u6001 {session.display_status} | \u6b63\u5728\u5237\u65b0\u5b9e\u65f6K\u7ebf\u56fe...")
        state.refresh_inflight = True
        threading.Thread(target=self._refresh_strategy_live_chart_worker, args=(session_id,), daemon=True).start()

    def _refresh_strategy_live_chart_worker(self, session_id: str) -> None:
        session = self.sessions.get(session_id)
        if session is None:
            self.root.after(0, lambda target_session_id=session_id: self._apply_strategy_live_chart_missing_session(target_session_id))
            return

        trade_inst_id = _session_trade_inst_id(session) or session.symbol.strip().upper()
        if not trade_inst_id:
            self.root.after(
                0,
                lambda target_session_id=session_id: self._apply_strategy_live_chart_error(
                    target_session_id, "\u7f3a\u5c11\u53ef\u67e5\u8be2\u7684\u4ea4\u6613\u6807\u7684\u3002"
                ),
            )
            return

        try:
            candles = self.client.get_candles(
                trade_inst_id,
                session.config.bar,
                limit=DEFAULT_STRATEGY_LIVE_CHART_CANDLE_LIMIT,
            )
        except Exception as exc:
            self.root.after(
                0,
                lambda target_session_id=session_id, message=str(exc): self._apply_strategy_live_chart_error(
                    target_session_id, message
                ),
            )
            return

        pending_entry_prices = self._strategy_live_chart_pending_entry_prices(session)
        position_avg_price, position_refreshed_at = self._strategy_live_chart_position_avg_price(session)
        live_pnl, live_pnl_refreshed_at = self._session_live_pnl_snapshot(session)
        stop_price = self._strategy_live_chart_stop_price(session)
        entry_price = session.active_trade.entry_price if session.active_trade is not None else None
        chart_refreshed_at = datetime.now()
        snapshot = build_strategy_live_chart_snapshot(
            session_id=session.session_id,
            candles=candles,
            ema_period=session.config.ema_period,
            trend_ema_period=session.config.trend_ema_period,
            reference_ema_period=session.config.resolved_entry_reference_ema_period(),
            pending_entry_prices=pending_entry_prices,
            entry_price=entry_price,
            position_avg_price=position_avg_price,
            stop_price=stop_price,
            latest_price=candles[-1].close if candles else None,
            note=self._strategy_live_chart_canvas_note(
                pending_entry_count=len(pending_entry_prices),
                position_refreshed_at=position_refreshed_at,
                live_pnl_refreshed_at=live_pnl_refreshed_at,
                stop_price=stop_price,
            ),
        )
        status_text = self._strategy_live_chart_status_text(
            session,
            live_pnl=live_pnl,
            pending_entry_count=len(pending_entry_prices),
            has_position=position_avg_price is not None or entry_price is not None,
            stop_price=stop_price,
        )
        footer_text = self._strategy_live_chart_footer_text(
            session=session,
            trade_inst_id=trade_inst_id,
            chart_refreshed_at=chart_refreshed_at,
            position_refreshed_at=position_refreshed_at,
            live_pnl_refreshed_at=live_pnl_refreshed_at,
            candle_count=len(candles),
            latest_candle_confirmed=bool(candles[-1].confirmed) if candles else True,
        )
        self.root.after(
            0,
            lambda target_session_id=session_id, chart_snapshot=snapshot, status_line=status_text, footer_line=footer_text: self._apply_strategy_live_chart_snapshot(
                target_session_id,
                chart_snapshot,
                status_line,
                footer_line,
            ),
        )

    def _apply_strategy_live_chart_snapshot(
        self,
        session_id: str,
        snapshot: StrategyLiveChartSnapshot,
        status_text: str,
        footer_text: str,
    ) -> None:
        state = self._strategy_live_chart_windows.get(session_id)
        if state is None or not _widget_exists(state.window):
            return
        session = self.sessions.get(session_id)
        state.refresh_inflight = False
        state.last_snapshot = snapshot
        if session is not None:
            state.window.title(self._strategy_live_chart_window_title(session))
            state.headline_text.set(self._strategy_live_chart_headline(session))
        state.status_text.set(status_text)
        state.footer_text.set(footer_text)
        self._render_strategy_live_chart_window(session_id)
        if session is not None:
            self._request_strategy_live_chart_refresh(session_id, immediate=False)

    def _apply_strategy_live_chart_missing_session(self, session_id: str) -> None:
        state = self._strategy_live_chart_windows.get(session_id)
        if state is None or not _widget_exists(state.window):
            return
        state.refresh_inflight = False
        state.status_text.set("\u4f1a\u8bdd\u5df2\u4ece\u8fd0\u884c\u5217\u8868\u79fb\u9664\uff0c\u56fe\u7a97\u505c\u6b62\u81ea\u52a8\u5237\u65b0\u3002")
        state.footer_text.set("\u5982\u5df2\u6e05\u7a7a\u505c\u6b62\u7b56\u7565\uff0c\u53ef\u76f4\u63a5\u5173\u95ed\u8fd9\u4e2a\u56fe\u7a97\u3002")

    def _apply_strategy_live_chart_error(self, session_id: str, message: str) -> None:
        state = self._strategy_live_chart_windows.get(session_id)
        if state is None or not _widget_exists(state.window):
            return
        state.refresh_inflight = False
        friendly_message = _format_network_error_message(message)
        if state.last_snapshot is None:
            state.last_snapshot = StrategyLiveChartSnapshot(
                session_id=session_id,
                candles=(),
                note=friendly_message,
            )
            self._render_strategy_live_chart_window(session_id)
            state.status_text.set(f"\u5b9e\u65f6K\u7ebf\u56fe\u8bfb\u53d6\u5931\u8d25\uff1a{friendly_message}")
        else:
            state.status_text.set(f"\u5b9e\u65f6K\u7ebf\u56fe\u5237\u65b0\u5931\u8d25\uff0c\u7ee7\u7eed\u663e\u793a\u4e0a\u4e00\u5f20\u56fe\uff1a{friendly_message}")
        state.footer_text.set(
            f"\u5c06\u4e8e {DEFAULT_STRATEGY_LIVE_CHART_REFRESH_MS // 1000} \u79d2\u540e\u81ea\u52a8\u91cd\u8bd5\u3002"
        )
        self._request_strategy_live_chart_refresh(session_id, immediate=False)

    def _render_strategy_live_chart_window(self, session_id: str) -> None:
        state = self._strategy_live_chart_windows.get(session_id)
        if state is None or not _widget_exists(state.canvas):
            return
        snapshot = state.last_snapshot
        if snapshot is None:
            snapshot = StrategyLiveChartSnapshot(
                session_id=session_id,
                candles=(),
                note="\u6b63\u5728\u52a0\u8f7dK\u7ebf\u6570\u636e...",
            )
        render_strategy_live_chart(state.canvas, snapshot)

    def _strategy_live_chart_pending_entry_prices(self, session: StrategySession) -> tuple[Decimal, ...]:
        prices: list[Decimal] = []
        seen: set[Decimal] = set()
        for item in self._latest_pending_orders:
            if _trade_order_session_role(item, session) != "ent":
                continue
            for candidate in (item.price, item.order_price, item.trigger_price):
                if candidate is None or candidate in seen:
                    continue
                seen.add(candidate)
                prices.append(candidate)
                break
        return tuple(prices)

    def _strategy_live_chart_stop_price(self, session: StrategySession) -> Decimal | None:
        active_trade = session.active_trade
        if active_trade is not None and active_trade.current_stop_price is not None:
            return active_trade.current_stop_price
        for item in self._latest_pending_orders:
            if _trade_order_session_role(item, session) != "slg":
                continue
            for candidate in (
                item.stop_loss_trigger_price,
                item.trigger_price,
                item.stop_loss_order_price,
                item.order_price,
            ):
                if candidate is not None:
                    return candidate
        return None

    def _strategy_live_chart_position_avg_price(self, session: StrategySession) -> tuple[Decimal | None, datetime | None]:
        snapshot = self._positions_snapshot_for_session(session)
        if snapshot is None:
            return None, None
        positions = [
            position
            for position in snapshot.positions
            if (
                position.position != 0
                and position.avg_price is not None
                and _position_matches_session_live_pnl(
                    position,
                    trade_inst_id=_session_trade_inst_id(session),
                    expected_sides=_session_expected_position_sides(session),
                )
            )
        ]
        if not positions:
            return None, snapshot.refreshed_at
        if len(positions) == 1:
            return positions[0].avg_price, snapshot.refreshed_at
        total_size = sum((abs(position.position) for position in positions), Decimal("0"))
        if total_size <= 0:
            return positions[0].avg_price, snapshot.refreshed_at
        weighted_value = sum((abs(position.position) * (position.avg_price or Decimal("0")) for position in positions), Decimal("0"))
        return weighted_value / total_size, snapshot.refreshed_at

    @staticmethod
    def _strategy_live_chart_canvas_note(
        *,
        pending_entry_count: int,
        position_refreshed_at: datetime | None,
        live_pnl_refreshed_at: datetime | None,
        stop_price: Decimal | None,
    ) -> str:
        parts: list[str] = []
        if pending_entry_count > 0:
            parts.append(f"\u6302\u5355 {pending_entry_count} \u6761")
        if stop_price is not None:
            parts.append("\u6b62\u635f\u5df2\u540c\u6b65")
        if position_refreshed_at is not None:
            parts.append(f"\u6301\u4ed3\u7f13\u5b58 {position_refreshed_at.strftime('%H:%M:%S')}")
        if live_pnl_refreshed_at is not None and live_pnl_refreshed_at != position_refreshed_at:
            parts.append(f"\u6d6e\u76c8\u7f13\u5b58 {live_pnl_refreshed_at.strftime('%H:%M:%S')}")
        return " | ".join(parts)

    def _strategy_live_chart_status_text(
        self,
        session: StrategySession,
        *,
        live_pnl: Decimal | None,
        pending_entry_count: int,
        has_position: bool,
        stop_price: Decimal | None,
    ) -> str:
        parts = [
            f"\u72b6\u6001 {session.display_status}",
            f"\u5b9e\u65f6\u6d6e\u76c8\u4e8f {_format_optional_usdt_precise(live_pnl, places=2)} USDT",
            f"\u51c0\u76c8\u4e8f {_format_optional_usdt_precise(session.net_pnl_total, places=2)} USDT",
        ]
        if has_position:
            parts.append("\u5f53\u524d\u6709\u6301\u4ed3")
        elif pending_entry_count > 0:
            parts.append(f"\u5f53\u524d\u6709\u6302\u5355 x{pending_entry_count}")
        else:
            parts.append("\u5f53\u524d\u65e0\u6301\u4ed3/\u6302\u5355")
        if stop_price is not None:
            parts.append(f"\u6b62\u635f {format_decimal(stop_price)}")
        last_message = (session.last_message or "").strip()
        if last_message:
            if len(last_message) > 72:
                last_message = f"{last_message[:72]}..."
            parts.append(f"\u6700\u8fd1\u6d88\u606f {last_message}")
        return " | ".join(parts)

    @staticmethod
    def _strategy_live_chart_footer_text(
        *,
        session: StrategySession,
        trade_inst_id: str,
        chart_refreshed_at: datetime,
        position_refreshed_at: datetime | None,
        live_pnl_refreshed_at: datetime | None,
        candle_count: int,
        latest_candle_confirmed: bool,
    ) -> str:
        candle_state = "\u5df2\u6536\u76d8" if latest_candle_confirmed else "\u672a\u6536\u76d8"
        parts = [
            f"\u5408\u7ea6 {trade_inst_id}",
            f"\u5468\u671f {session.config.bar}",
            f"\u6700\u8fd1\u5237\u65b0 {chart_refreshed_at.strftime('%H:%M:%S')}",
            f"K\u7ebf {candle_count} \u6839",
            f"\u5f53\u524dK\u7ebf {candle_state}",
            f"\u81ea\u52a8\u5237\u65b0 {DEFAULT_STRATEGY_LIVE_CHART_REFRESH_MS // 1000} \u79d2",
        ]
        if position_refreshed_at is not None:
            parts.append(f"\u6301\u4ed3\u7f13\u5b58 {position_refreshed_at.strftime('%H:%M:%S')}")
        if live_pnl_refreshed_at is not None and live_pnl_refreshed_at != position_refreshed_at:
            parts.append(f"\u6d6e\u76c8\u7f13\u5b58 {live_pnl_refreshed_at.strftime('%H:%M:%S')}")
        return " | ".join(parts)

    def _finish_strategy_template_import(
        self,
        *,
        source: str,
        record: StrategyTemplateRecord,
        definition: StrategyDefinition,
        applied_api: str,
        api_note: str,
    ) -> None:
        self._enqueue_log(
            f"已导入策略参数：{source} | 策略={definition.name} | API={applied_api or '-'} | {api_note}"
        )
        duplicate_session = self._find_duplicate_strategy_session(api_name=applied_api, config=record.config) if applied_api else None
        if duplicate_session is not None:
            self._focus_session_row(duplicate_session.session_id)
            messagebox.showwarning(
                "导入完成",
                self._format_duplicate_launch_block_message(duplicate_session, imported=True),
            )
            return

        summary = "\n".join(
            [
                f"已导入：{definition.name}",
                f"交易标的：{self.symbol.get()}",
                f"API：{applied_api or '-'}",
                api_note,
                "导入文件不包含 API 密钥，只会复用本机当前或同名 API 配置。",
                "如需复制参数开新策略，请先改标的或切换 API，再启动。",
                "",
                "是否现在就按这套参数启动？",
            ]
        )
        if messagebox.askyesno("导入完成", summary):
            self.start()

    def _template_record_from_launcher(self, *, force_run_mode: str | None = None) -> StrategyTemplateRecord:
        definition = self._selected_strategy_definition()
        original_run_mode_label = self.run_mode_label.get()
        if force_run_mode is not None:
            self.run_mode_label.set(_reverse_lookup_label(RUN_MODE_OPTIONS, force_run_mode, original_run_mode_label))
        try:
            _, config = self._collect_inputs(definition)
        finally:
            if force_run_mode is not None:
                self.run_mode_label.set(original_run_mode_label)
        if force_run_mode == "signal_only" and not supports_signal_only(definition.strategy_id):
            raise ValueError(f"{definition.name} 当前不支持只发信号邮件模式。")
        return StrategyTemplateRecord(
            strategy_id=definition.strategy_id,
            strategy_name=definition.name,
            api_name=self._current_credential_profile(),
            direction_label=_strategy_template_direction_label(
                definition.strategy_id,
                config,
                fallback=self.signal_mode_label.get() or definition.default_signal_label,
            ),
            run_mode_label=_reverse_lookup_label(RUN_MODE_OPTIONS, config.run_mode, self.run_mode_label.get()),
            symbol=_launcher_symbol_from_strategy_config(definition.strategy_id, config),
            config=config,
        )

    def _clone_template_record_for_symbol(self, record: StrategyTemplateRecord, symbol: str) -> StrategyTemplateRecord:
        normalized_symbol = _normalize_symbol_input(symbol)
        if not normalized_symbol:
            raise ValueError("复制草稿时缺少有效标的。")
        cloned_config = replace(
            record.config,
            inst_id=normalized_symbol,
            trade_inst_id=normalized_symbol if record.config.trade_inst_id is not None else None,
            local_tp_sl_inst_id=None,
        )
        return StrategyTemplateRecord(
            strategy_id=record.strategy_id,
            strategy_name=record.strategy_name,
            api_name=record.api_name,
            direction_label=record.direction_label,
            run_mode_label=record.run_mode_label,
            symbol=normalized_symbol,
            config=cloned_config,
            exported_at=record.exported_at,
        )

    def _clone_template_record_for_targets(
        self,
        record: StrategyTemplateRecord,
        trade_symbol: str,
        trigger_symbol: str = "",
    ) -> StrategyTemplateRecord:
        normalized_trade = _normalize_symbol_input(trade_symbol)
        if not normalized_trade:
            raise ValueError("复制草稿时缺少有效的交易标的。")
        normalized_trigger = _normalize_symbol_input(trigger_symbol) or normalized_trade
        trigger_matches_trade = normalized_trigger == normalized_trade
        cloned_config = replace(
            record.config,
            inst_id=normalized_trigger,
            trade_inst_id=normalized_trade if (not trigger_matches_trade or record.config.trade_inst_id is not None) else None,
            local_tp_sl_inst_id=None if trigger_matches_trade else normalized_trigger,
            tp_sl_mode="exchange" if trigger_matches_trade else "local_custom",
        )
        return StrategyTemplateRecord(
            strategy_id=record.strategy_id,
            strategy_name=record.strategy_name,
            api_name=record.api_name,
            direction_label=record.direction_label,
            run_mode_label=record.run_mode_label,
            symbol=self._format_strategy_symbol_display(normalized_trigger, normalized_trade),
            config=cloned_config,
            exported_at=record.exported_at,
        )

    def _load_trader_desk_snapshot(self) -> None:
        try:
            snapshot = load_trader_desk_snapshot()
        except Exception as exc:
            self._enqueue_log(f"读取交易员管理台数据失败：{exc}")
            return
        self._trader_desk_drafts = list(snapshot.drafts)
        self._trader_desk_runs = list(snapshot.runs)
        self._trader_desk_slots = list(snapshot.slots)
        self._trader_desk_events = list(snapshot.events)

    def _save_trader_desk_snapshot(self) -> None:
        try:
            save_trader_desk_snapshot(self._trader_desk_snapshot_for_ui())
        except Exception as exc:
            self._enqueue_log(f"保存交易员管理台数据失败：{exc}")

    def _trader_desk_snapshot_for_ui(self) -> TraderDeskSnapshot:
        return TraderDeskSnapshot(
            drafts=list(self._trader_desk_drafts),
            runs=list(self._trader_desk_runs),
            slots=list(self._trader_desk_slots),
            events=list(self._trader_desk_events),
        )

    @staticmethod
    def _session_runtime_snapshot_for_ui(session: StrategySession) -> dict[str, object]:
        return {
            "session_id": session.session_id,
            "runtime_status": session.display_status or session.status,
            "last_message": session.last_message,
            "started_at": session.started_at,
            "ended_reason": session.ended_reason,
            "is_running": bool(session.engine.is_running or session.stop_cleanup_in_progress),
            "log_file_path": str(session.log_file_path) if session.log_file_path is not None else "",
        }

    def _trader_runtime_snapshot_for_ui(self, trader_id: str) -> dict[str, object] | None:
        normalized = trader_id.strip()
        if not normalized:
            return None
        run = self._trader_desk_run_by_id(normalized)
        preferred_session_id = run.armed_session_id if run is not None else ""
        sessions = [session for session in self.sessions.values() if session.trader_id == normalized]
        if preferred_session_id:
            for session in sessions:
                if session.session_id == preferred_session_id:
                    return self._session_runtime_snapshot_for_ui(session)
        if sessions:
            sessions.sort(
                key=lambda item: (
                    1 if (item.engine.is_running or item.stop_cleanup_in_progress) else 0,
                    item.started_at,
                    item.session_id,
                ),
                reverse=True,
            )
            return self._session_runtime_snapshot_for_ui(sessions[0])
        if preferred_session_id:
            return {
                "session_id": preferred_session_id,
                "runtime_status": "未找到活动会话",
                "last_message": "",
                "started_at": None,
                "ended_reason": "当前交易员记录里保留了 watcher 会话号，但主界面里已经找不到这条会话。",
                "is_running": False,
                "log_file_path": "",
            }
        return None

    def _trader_desk_draft_by_id(self, trader_id: str) -> TraderDraftRecord | None:
        normalized = trader_id.strip()
        if not normalized:
            return None
        for draft in self._trader_desk_drafts:
            if draft.trader_id == normalized:
                return draft
        return None

    def _trader_desk_run_by_id(self, trader_id: str, *, create: bool = False) -> TraderRunState | None:
        normalized = trader_id.strip()
        if not normalized:
            return None
        for run in self._trader_desk_runs:
            if run.trader_id == normalized:
                return run
        if not create:
            return None
        run = TraderRunState(trader_id=normalized, updated_at=datetime.now())
        self._trader_desk_runs.append(run)
        return run

    def _trader_desk_slot_for_session(self, session_id: str) -> TraderSlotRecord | None:
        normalized = session_id.strip()
        if not normalized:
            return None
        for slot in self._trader_desk_slots:
            if slot.session_id == normalized:
                return slot
        return None

    def _trader_desk_slots_for_statuses(self, trader_id: str, statuses: set[str]) -> list[TraderSlotRecord]:
        return [slot for slot in trader_slots_for(self._trader_desk_slots, trader_id) if slot.status in statuses]

    def _trader_desk_next_slot_id(self, trader_id: str) -> str:
        base = f"{trader_id}-{datetime.now():%Y%m%d%H%M%S%f}"
        slot_id = base
        suffix = 2
        known_ids = {slot.slot_id for slot in self._trader_desk_slots}
        while slot_id in known_ids:
            slot_id = f"{base}-{suffix}"
            suffix += 1
        return slot_id

    def _trader_desk_next_event_id(self, trader_id: str) -> str:
        base = f"{trader_id}-{datetime.now():%Y%m%d%H%M%S%f}"
        event_id = base
        suffix = 2
        known_ids = {event.event_id for event in self._trader_desk_events}
        while event_id in known_ids:
            event_id = f"{base}-{suffix}"
            suffix += 1
        return event_id

    def _trader_desk_add_event(self, trader_id: str, message: str, *, level: str = "info") -> None:
        text = str(message or "").strip()
        normalized = trader_id.strip()
        if not normalized or not text:
            return
        self._trader_desk_events.append(
            TraderEventRecord(
                event_id=self._trader_desk_next_event_id(normalized),
                trader_id=normalized,
                created_at=datetime.now(),
                level=level,
                message=text,
            )
        )
        self._trader_desk_events.sort(key=lambda item: (item.created_at, item.event_id), reverse=True)
        self._trader_desk_events = self._trader_desk_events[:400]
        self._enqueue_log(f"[交易员管理台] [{normalized}] {text}")

    @staticmethod
    def _expected_trader_stop_reason(reason: str) -> bool:
        normalized = str(reason or "").strip()
        if not normalized:
            return False
        markers = (
            "人工暂停",
            "手动平仓",
            "用户手动停止",
            "信号观察台手动停止",
            "应用关闭",
            "待恢复",
            "恢复启动失败",
            "停止清理失败",
        )
        return any(marker in normalized for marker in markers)

    @staticmethod
    def _session_stop_reason_text(session: StrategySession) -> str:
        ended_reason = str(getattr(session, "ended_reason", "") or "").strip()
        if ended_reason:
            return ended_reason
        last_message = str(getattr(session, "last_message", "") or "").strip()
        if last_message.startswith("策略停止，原因："):
            detail = last_message.partition("：")[2].strip()
            return detail or last_message
        return "策略线程结束"

    def _save_trader_desk_draft(self, draft: TraderDraftRecord) -> None:
        for index, current in enumerate(self._trader_desk_drafts):
            if current.trader_id == draft.trader_id:
                self._trader_desk_drafts[index] = draft
                break
        else:
            self._trader_desk_drafts.append(draft)
        self._save_trader_desk_snapshot()

    def _delete_trader_desk_draft(self, trader_id: str) -> None:
        draft = self._trader_desk_draft_by_id(trader_id)
        if draft is None:
            raise ValueError("未找到对应的交易员草稿。")
        self._cleanup_stale_trader_watchers(trader_id)
        active_sessions = [
            session
            for session in self.sessions.values()
            if session.trader_id == trader_id and (session.engine.is_running or session.stop_cleanup_in_progress)
        ]
        if active_sessions:
            raise ValueError("该交易员仍有关联会话在运行，请先暂停或平仓。")
        active_slots = self._trader_desk_slots_for_statuses(trader_id, {"watching", "open"})
        if active_slots:
            raise ValueError("该交易员仍有活动中的额度格，请先暂停或平仓。")
        self._trader_desk_drafts = [item for item in self._trader_desk_drafts if item.trader_id != trader_id]
        self._trader_desk_runs = [item for item in self._trader_desk_runs if item.trader_id != trader_id]
        self._trader_desk_slots = [item for item in self._trader_desk_slots if item.trader_id != trader_id]
        self._trader_desk_events = [item for item in self._trader_desk_events if item.trader_id != trader_id]
        self._save_trader_desk_snapshot()
        self._enqueue_log(f"[交易员管理台] [{trader_id}] 已删除。")

    def _trader_desk_symbol_choices(self) -> list[str]:
        return list(dict.fromkeys(self._custom_trigger_symbol_values + self._default_symbol_values))

    def _trader_desk_market_price(self, inst_id: str, price_type: str) -> Decimal | None:
        normalized_inst_id = inst_id.strip().upper()
        if not normalized_inst_id:
            return None
        cached = self._trader_gate_price_cache.get(normalized_inst_id)
        now = datetime.now()
        ticker: OkxTicker
        if cached is not None and (now - cached[0]).total_seconds() < 3:
            ticker = cached[1]
        else:
            ticker = self.client.get_ticker(normalized_inst_id)
            self._trader_gate_price_cache[normalized_inst_id] = (now, ticker)
        normalized_type = str(price_type or "mark").strip().lower()
        candidate = (
            ticker.last
            if normalized_type == "last"
            else ticker.index
            if normalized_type == "index"
            else ticker.mark
        )
        return candidate or ticker.last or ticker.mark or ticker.index

    def _trader_desk_gate_passes(self, draft: TraderDraftRecord) -> bool:
        if not draft.gate.enabled:
            return True
        current_price = self._trader_desk_market_price(draft.gate.trigger_inst_id, draft.gate.trigger_price_type)
        if current_price is None:
            return False
        return trader_gate_allows_price(draft.gate, current_price)

    def _trader_desk_start_slot(self, trader_id: str) -> bool:
        draft = self._trader_desk_draft_by_id(trader_id)
        run = self._trader_desk_run_by_id(trader_id, create=True)
        if draft is None or run is None:
            raise ValueError("未找到对应的交易员草稿。")
        if run.status not in {"running", "quota_exhausted"}:
            return False
        if trader_has_watching_slot(self._trader_desk_slots, trader_id):
            return False
        remaining = trader_remaining_quota_steps(draft, self._trader_desk_slots)
        if remaining <= 0:
            if run.status != "quota_exhausted":
                run.status = "quota_exhausted"
                run.paused_reason = "额度已满，等待释放。"
                run.updated_at = datetime.now()
                self._trader_desk_add_event(trader_id, "额度已满，暂停补位等待释放。", level="warning")
                self._save_trader_desk_snapshot()
            return False
        if not self._trader_desk_gate_passes(draft):
            if run.paused_reason != "价格开关未满足。":
                run.paused_reason = "价格开关未满足。"
                run.updated_at = datetime.now()
                self._save_trader_desk_snapshot()
            return False

        record = _strategy_template_record_from_payload(draft.template_payload)
        if record is None:
            run.status = "stopped"
            run.paused_reason = "草稿缺少有效模板。"
            run.updated_at = datetime.now()
            self._trader_desk_add_event(trader_id, "启动 watcher 失败：草稿缺少有效模板。", level="error")
            self._save_trader_desk_snapshot()
            return False
        definition = self._resolve_strategy_template_definition(record)
        resolved_api_name, _ = _resolve_import_api_profile(
            record.api_name,
            self._current_credential_profile(),
            set(self._credential_profiles.keys()),
        )
        target_api_name = resolved_api_name or self._current_credential_profile()
        credentials = self._credentials_for_profile_or_none(target_api_name)
        if credentials is None:
            run.status = "stopped"
            run.paused_reason = f"未找到 API：{target_api_name}"
            run.updated_at = datetime.now()
            self._trader_desk_add_event(trader_id, f"启动 watcher 失败：未找到 API {target_api_name}。", level="error")
            self._save_trader_desk_snapshot()
            return False

        slot_id = self._trader_desk_next_slot_id(trader_id)
        config = replace(
            record.config,
            run_mode="trade",
            risk_amount=None,
            order_size=draft.unit_quota,
            trader_virtual_stop_loss=True,
        )
        notifier = self._build_notifier(config)
        try:
            session_id = self._start_strategy_session(
                definition=definition,
                credentials=credentials,
                config=config,
                notifier=notifier,
                api_name=target_api_name,
                direction_label=_strategy_template_direction_label(
                    definition.strategy_id,
                    config,
                    fallback=record.direction_label or definition.default_signal_label,
                ),
                run_mode_label=_reverse_lookup_label(RUN_MODE_OPTIONS, config.run_mode, record.run_mode_label),
                source_label=f"交易员 {trader_id}",
                select_session=False,
                allow_duplicate_launch=True,
                trader_id=trader_id,
                trader_slot_id=slot_id,
            )
        except Exception as exc:
            run.status = "stopped"
            run.paused_reason = str(exc)
            run.updated_at = datetime.now()
            self._trader_desk_add_event(trader_id, f"启动 watcher 失败：{exc}", level="error")
            self._save_trader_desk_snapshot()
            return False

        now = datetime.now()
        self._trader_desk_slots.append(
            TraderSlotRecord(
                slot_id=slot_id,
                trader_id=trader_id,
                session_id=session_id,
                api_name=target_api_name,
                strategy_name=definition.name,
                symbol=self._format_strategy_symbol_display(config.inst_id, config.trade_inst_id),
                bar=str(config.bar or "").strip(),
                direction_label=record.direction_label or definition.default_signal_label,
                status="watching",
                quota_occupied=False,
                created_at=now,
            )
        )
        run.status = "running"
        run.paused_reason = ""
        run.armed_session_id = session_id
        run.last_started_at = now
        run.last_event_at = now
        run.updated_at = now
        draft.updated_at = now
        self._trader_desk_add_event(
            trader_id,
            "已启动 watcher"
            f" | 会话={session_id}"
            f" | 周期={config.bar}"
            f" | 固定数量={format_decimal(draft.unit_quota)}"
            f" | 剩余额度格={trader_remaining_quota_steps(draft, self._trader_desk_slots)}",
        )
        self._save_trader_desk_snapshot()
        return True

    def _ensure_trader_watcher(self, trader_id: str) -> None:
        draft = self._trader_desk_draft_by_id(trader_id)
        run = self._trader_desk_run_by_id(trader_id)
        if draft is None or run is None:
            return
        if run.status not in {"running", "quota_exhausted"}:
            return
        self._cleanup_stale_trader_watchers(trader_id)
        if trader_has_watching_slot(self._trader_desk_slots, trader_id):
            return
        remaining = trader_remaining_quota_steps(draft, self._trader_desk_slots)
        if remaining <= 0:
            if run.status != "quota_exhausted":
                run.status = "quota_exhausted"
                run.paused_reason = "额度已满，等待释放。"
                run.updated_at = datetime.now()
                self._trader_desk_add_event(trader_id, "额度已满，暂停补位等待释放。", level="warning")
                self._save_trader_desk_snapshot()
            return
        if run.status == "quota_exhausted":
            run.status = "running"
            run.paused_reason = ""
            run.updated_at = datetime.now()
        self._trader_desk_start_slot(trader_id)

    def _cleanup_stale_trader_watchers(self, trader_id: str) -> None:
        run = self._trader_desk_run_by_id(trader_id, create=True)
        if run is None:
            return
        changed = False
        now = datetime.now()
        for slot in self._trader_desk_slots_for_statuses(trader_id, {"watching"}):
            session = self.sessions.get(slot.session_id)
            if session is not None and (session.engine.is_running or session.stop_cleanup_in_progress or session.status in {"运行中", "停止中", "恢复中"}):
                continue
            slot.status = "stopped"
            slot.closed_at = slot.closed_at or now
            slot.released_at = slot.released_at or now
            slot.close_reason = slot.close_reason or "watcher 会话不存在或已停止"
            if run.armed_session_id == slot.session_id:
                run.armed_session_id = ""
                run.last_event_at = now
                run.updated_at = now
            self._trader_desk_add_event(
                trader_id,
                f"检测到失效 watcher，已清理 | 会话={slot.session_id} | 原因={slot.close_reason}",
                level="warning",
            )
            changed = True
        if changed:
            self._save_trader_desk_snapshot()

    def start_trader_draft(self, trader_id: str) -> None:
        draft = self._trader_desk_draft_by_id(trader_id)
        run = self._trader_desk_run_by_id(trader_id, create=True)
        if draft is None or run is None:
            raise ValueError("未找到对应的交易员草稿。")
        now = datetime.now()
        draft.status = "ready"
        draft.updated_at = now
        run.status = "running"
        run.paused_reason = ""
        run.updated_at = now
        run.last_started_at = now
        self._trader_desk_add_event(trader_id, "已启动交易员。")
        self._save_trader_desk_snapshot()
        self._ensure_trader_watcher(trader_id)

    def pause_trader_draft(self, trader_id: str) -> None:
        draft = self._trader_desk_draft_by_id(trader_id)
        run = self._trader_desk_run_by_id(trader_id, create=True)
        if draft is None or run is None:
            raise ValueError("未找到对应的交易员草稿。")
        now = datetime.now()
        draft.status = "paused"
        draft.updated_at = now
        run.status = "paused_manual"
        run.paused_reason = "人工暂停。"
        run.updated_at = now
        session_ids = [slot.session_id for slot in self._trader_desk_slots_for_statuses(trader_id, {"watching"})]
        for session_id in session_ids:
            self._request_stop_strategy_session(
                session_id,
                ended_reason="交易员人工暂停",
                source_label=f"交易员 {trader_id} 人工暂停",
                show_dialog=False,
            )
        self._trader_desk_add_event(trader_id, f"已人工暂停交易员，停止 watcher {len(session_ids)} 个。", level="warning")
        self._save_trader_desk_snapshot()

    def resume_trader_draft(self, trader_id: str) -> None:
        draft = self._trader_desk_draft_by_id(trader_id)
        run = self._trader_desk_run_by_id(trader_id, create=True)
        if draft is None or run is None:
            raise ValueError("未找到对应的交易员草稿。")
        now = datetime.now()
        draft.status = "ready"
        draft.updated_at = now
        run.status = "running"
        run.paused_reason = ""
        run.updated_at = now
        self._trader_desk_add_event(trader_id, "已恢复交易员。")
        self._save_trader_desk_snapshot()
        self._ensure_trader_watcher(trader_id)

    def flatten_trader_draft(self, trader_id: str) -> None:
        draft = self._trader_desk_draft_by_id(trader_id)
        run = self._trader_desk_run_by_id(trader_id, create=True)
        if draft is None or run is None:
            raise ValueError("未找到对应的交易员草稿。")
        now = datetime.now()
        draft.status = "paused"
        draft.updated_at = now
        run.status = "paused_manual"
        run.paused_reason = "人工平仓。"
        run.armed_session_id = ""
        run.updated_at = now
        active_slots = self._trader_desk_slots_for_statuses(trader_id, {"watching", "open"})
        session_ids = sorted({slot.session_id for slot in active_slots if slot.session_id})
        for session_id in session_ids:
            self._request_stop_strategy_session(
                session_id,
                ended_reason="交易员手动平仓",
                source_label=f"交易员 {trader_id} 手动平仓",
                show_dialog=False,
            )
        watching_slots = [slot for slot in active_slots if slot.status == "watching"]
        for slot in watching_slots:
            slot.status = "stopped"
            slot.quota_occupied = False
            slot.closed_at = slot.closed_at or now
            slot.released_at = slot.released_at or now
            slot.close_reason = slot.close_reason or "人工平仓停止 watcher"
        open_slots = [slot for slot in active_slots if slot.status == "open"]
        submitted_count, stale_count, failed_count = self._submit_trader_manual_flatten_orders(
            draft,
            open_slots,
            now,
        )
        if submitted_count or stale_count or failed_count:
            self._trader_desk_add_event(
                trader_id,
                "手动平仓结果 | "
                f"已提交平仓单 {submitted_count} 个 | "
                f"已清理无真实持仓槽位 {stale_count} 个 | "
                f"提交失败 {failed_count} 个",
                level="warning",
            )
        self._trader_desk_add_event(trader_id, f"已请求手动平仓/停止 {len(session_ids)} 个额度格。", level="warning")
        self._save_trader_desk_snapshot()

    @staticmethod
    def _trader_manual_flatten_open_side(
        config: StrategyConfig,
    ) -> tuple[str, str, str | None]:
        long_pos_side = "long" if config.position_mode == "long_short" else None
        short_pos_side = "short" if config.position_mode == "long_short" else None
        signal_mode = str(config.signal_mode or "").strip().lower()
        if signal_mode == "long_only":
            return ("sell", "long", long_pos_side)
        if signal_mode == "short_only":
            return ("buy", "short", short_pos_side)
        raise ValueError("交易员手动平仓仅支持只做多或只做空策略。")

    @staticmethod
    def _trader_position_closeable_size(position: OkxPosition) -> Decimal:
        base = position.avail_position
        if base is None or base == 0:
            base = position.position
        return abs(base)

    @staticmethod
    def _trader_slot_flatten_size(slot: TraderSlotRecord, draft: TraderDraftRecord) -> Decimal:
        if slot.size is not None and slot.size > 0:
            return slot.size
        return draft.unit_quota

    @staticmethod
    def _build_trader_manual_flatten_cl_ord_id(slot: TraderSlotRecord) -> str:
        session_token = "".join(ch for ch in slot.session_id.lower() if ch.isascii() and ch.isalnum())[:4] or "sess"
        strategy_token = "".join(ch for ch in slot.strategy_name.lower() if ch.isascii() and ch.isalnum())[:4] or "trdr"
        suffix = datetime.now().strftime("%m%d%H%M%S%f")[-15:]
        return f"{session_token}{strategy_token}exi{suffix}"[:32]

    def _lookup_trader_manual_flatten_exit_price(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        *,
        inst_id: str,
        result: OkxOrderResult,
    ) -> Decimal | None:
        order_id = (result.ord_id or "").strip()
        client_order_id = (result.cl_ord_id or "").strip()
        if not order_id and not client_order_id:
            return None
        for _ in range(3):
            try:
                status = self.client.get_order(
                    credentials,
                    config,
                    inst_id=inst_id,
                    ord_id=order_id or None,
                    cl_ord_id=client_order_id or None,
                )
            except Exception:
                threading.Event().wait(0.2)
                continue
            return status.avg_price or status.price
        return None

    def _submit_trader_manual_flatten_orders(
        self,
        draft: TraderDraftRecord,
        open_slots: list[TraderSlotRecord],
        now: datetime,
    ) -> tuple[int, int, int]:
        if not open_slots:
            return (0, 0, 0)
        config = _deserialize_strategy_config_snapshot(draft.template_payload.get("config_snapshot"))
        if config is None:
            raise ValueError("交易员草稿缺少可用的策略配置快照，无法执行手动平仓。")
        credentials = self._credentials_for_profile_or_none(str(draft.template_payload.get("api_name") or ""))
        if credentials is None:
            raise ValueError("当前找不到该交易员对应的 API 凭证，无法执行手动平仓。")
        trade_inst_id = (
            config.trade_inst_id
            or config.inst_id
            or str(draft.template_payload.get("symbol") or "")
        ).strip().upper()
        if not trade_inst_id:
            raise ValueError("交易员草稿缺少交易标的，无法执行手动平仓。")
        close_side, expected_position_side, pos_side = self._trader_manual_flatten_open_side(config)
        instrument = self.client.get_instrument(trade_inst_id)
        positions = self.client.get_positions(credentials, environment=config.environment)
        remaining_live_size = sum(
            self._trader_position_closeable_size(position)
            for position in positions
            if position.inst_id.strip().upper() == trade_inst_id
            and _format_pos_side(position.pos_side, position.position).strip().lower() == expected_position_side
            and ((position.mgn_mode or "").strip().lower() or config.trade_mode) == config.trade_mode
        )

        submitted_count = 0
        stale_count = 0
        failed_count = 0
        for slot in sorted(open_slots, key=lambda item: (item.created_at, item.slot_id)):
            requested_size = self._trader_slot_flatten_size(slot, draft)
            if remaining_live_size <= 0:
                slot.status = "stopped"
                slot.quota_occupied = False
                slot.closed_at = slot.closed_at or now
                slot.released_at = slot.released_at or now
                slot.close_reason = "人工平仓时未检测到交易所持仓"
                stale_count += 1
                continue

            close_size = snap_to_increment(min(requested_size, remaining_live_size), instrument.lot_size, "down")
            if close_size < instrument.min_size:
                slot.status = "stopped"
                slot.quota_occupied = False
                slot.closed_at = slot.closed_at or now
                slot.released_at = slot.released_at or now
                slot.close_reason = "人工平仓时剩余交易所持仓不足最小下单量"
                stale_count += 1
                continue

            try:
                result = self.client.place_simple_order(
                    credentials,
                    config,
                    inst_id=trade_inst_id,
                    side=close_side,
                    size=close_size,
                    ord_type="market",
                    pos_side=pos_side,
                    cl_ord_id=self._build_trader_manual_flatten_cl_ord_id(slot),
                )
            except Exception as exc:
                failed_count += 1
                slot.note = _format_network_error_message(str(exc))
                self._trader_desk_add_event(
                    draft.trader_id,
                    f"手动平仓提交失败 | 会话={slot.session_id} | 合约={trade_inst_id} | 原因={slot.note}",
                    level="warning",
                )
                continue

            slot.status = "closed_manual"
            slot.quota_occupied = False
            slot.closed_at = slot.closed_at or now
            slot.released_at = slot.released_at or now
            slot.close_reason = "人工手动平仓"
            slot.exit_price = self._lookup_trader_manual_flatten_exit_price(
                credentials,
                config,
                inst_id=trade_inst_id,
                result=result,
            )
            slot.note = (
                f"人工平仓单已提交 | ordId={(result.ord_id or '-').strip() or '-'} | "
                f"clOrdId={(result.cl_ord_id or '-').strip() or '-'}"
            )
            remaining_live_size = max(remaining_live_size - close_size, Decimal("0"))
            submitted_count += 1

        return (submitted_count, stale_count, failed_count)

    def force_clear_trader_draft(self, trader_id: str) -> None:
        draft = self._trader_desk_draft_by_id(trader_id)
        run = self._trader_desk_run_by_id(trader_id, create=True)
        if draft is None or run is None:
            raise ValueError("未找到对应的交易员草稿。")
        now = datetime.now()
        draft.status = "paused"
        draft.updated_at = now
        run.status = "paused_manual"
        run.paused_reason = "人工强制清格。"
        run.armed_session_id = ""
        run.last_event_at = now
        run.updated_at = now

        stop_requested = 0
        for session in [item for item in self.sessions.values() if item.trader_id == trader_id]:
            if session.stop_cleanup_in_progress or not session.engine.is_running:
                continue
            if self._request_stop_strategy_session(
                session.session_id,
                ended_reason="交易员强制清格",
                source_label=f"交易员 {trader_id} 强制清格",
                show_dialog=False,
            ):
                stop_requested += 1

        cleared_slots = 0
        for slot in trader_slots_for(self._trader_desk_slots, trader_id):
            if slot.status in {"closed_profit", "closed_loss", "closed_manual"}:
                continue
            previous_status = slot.status
            slot.status = "stopped"
            slot.quota_occupied = False
            slot.closed_at = slot.closed_at or now
            slot.released_at = slot.released_at or now
            if previous_status == "open":
                slot.close_reason = "人工强制清格（未同步平仓结果）"
            else:
                slot.close_reason = slot.close_reason or "人工强制清格"
            cleared_slots += 1

        self._trader_desk_add_event(
            trader_id,
            f"已强制清理 {cleared_slots} 个额度格 | 已请求停止 {stop_requested} 个关联会话 | "
            "本地额度占用已释放，请人工确认交易所真实仓位/委托。",
            level="warning",
        )
        self._save_trader_desk_snapshot()

    def _signal_observer_session_rows(self) -> list[dict[str, str]]:
        rows: list[dict[str, str]] = []
        ordered = sorted(self.sessions.values(), key=lambda item: (item.started_at, item.session_id), reverse=True)
        for session in ordered:
            if session.config.run_mode != "signal_only":
                continue
            rows.append(
                {
                    "session_id": session.session_id,
                    "api_name": session.api_name or "-",
                    "strategy_name": session.strategy_name,
                    "symbol": session.symbol,
                    "status": session.display_status,
                    "last_message": session.last_message or "-",
                }
            )
        return rows

    def _stop_sessions_by_id(self, session_ids: list[str]) -> None:
        for session_id in session_ids:
            session = self.sessions.get(session_id)
            if session is None:
                continue
            if session.config.run_mode != "signal_only":
                self._enqueue_log(f"[{session.session_id}] 仅支持由信号观察台停止 signal_only 会话。")
                continue
            if session.stop_cleanup_in_progress:
                continue
            if not session.engine.is_running:
                continue
            session.stop_cleanup_in_progress = True
            session.status = "停止中"
            session.runtime_status = "停止中"
            session.ended_reason = "信号观察台手动停止"
            session.engine.stop()
            session.engine.wait_stopped(timeout=1.5)
            session.stop_cleanup_in_progress = False
            session.status = "已停止"
            session.runtime_status = "已停止"
            session.stopped_at = datetime.now()
            self._upsert_session_row(session)
            self._refresh_selected_session_details()
            self._sync_strategy_history_from_session(session)
            self._log_session_message(session, "信号观察台已停止该会话。")

    def _delete_signal_observer_sessions_by_id(self, session_ids: list[str]) -> tuple[int, list[str]]:
        tree = getattr(self, "session_tree", None)
        tree_exists = tree is not None and tree.winfo_exists()
        selected_before = tree.selection()[0] if tree_exists and tree.selection() else None
        deleted_ids: list[str] = []
        blocked_ids: list[str] = []
        for session_id in session_ids:
            session = self.sessions.get(session_id)
            if session is None:
                continue
            if session.config.run_mode != "signal_only" or not QuantApp._session_can_be_cleared(session):
                blocked_ids.append(session_id)
                continue
            self.sessions.pop(session_id, None)
            self._remove_recoverable_strategy_session(session_id)
            if tree_exists and tree.exists(session_id):
                tree.delete(session_id)
            deleted_ids.append(session_id)
        if deleted_ids:
            if tree_exists:
                remaining = tuple(tree.get_children())
                next_selection = QuantApp._next_session_selection_after_clear(selected_before, remaining)
                if next_selection is not None:
                    tree.selection_set(next_selection)
                    tree.focus(next_selection)
                    tree.see(next_selection)
            self._refresh_selected_session_details()
            self._refresh_running_session_summary()
        return len(deleted_ids), blocked_ids

    def _launch_strategy_template_record(
        self,
        record: StrategyTemplateRecord,
        *,
        source_label: str,
        ask_confirm: bool,
    ) -> str:
        definition = self._resolve_strategy_template_definition(record)
        resolved_api_name, _ = _resolve_import_api_profile(
            record.api_name,
            self._current_credential_profile(),
            set(self._credential_profiles.keys()),
        )
        target_api_name = resolved_api_name or self._current_credential_profile()
        credentials = self._credentials_for_profile_or_none(target_api_name)
        if credentials is None:
            raise ValueError(f"未找到 API 配置：{target_api_name}")
        config = record.config
        if config.run_mode == "signal_only" and not supports_signal_only(definition.strategy_id):
            raise ValueError(f"{definition.name} 当前不支持只发信号邮件模式。")
        if ask_confirm and not self._confirm_start(definition, config):
            raise ValueError("已取消启动。")
        notifier = self._build_notifier(config)
        self._save_credentials_now(silent=True)
        self._save_notification_settings_now(silent=True)
        return self._start_strategy_session(
            definition=definition,
            credentials=credentials,
            config=config,
            notifier=notifier,
            api_name=target_api_name,
            direction_label=_strategy_template_direction_label(
                definition.strategy_id,
                config,
                fallback=record.direction_label or definition.default_signal_label,
            ),
            run_mode_label=_reverse_lookup_label(RUN_MODE_OPTIONS, config.run_mode, record.run_mode_label),
            source_label=source_label,
            select_session=ask_confirm,
        )

    def _start_strategy_session(
        self,
        *,
        definition: StrategyDefinition,
        credentials: Credentials,
        config: StrategyConfig,
        notifier: EmailNotifier | None,
        api_name: str,
        direction_label: str,
        run_mode_label: str,
        source_label: str = "",
        select_session: bool = True,
        allow_duplicate_launch: bool = False,
        trader_id: str = "",
        trader_slot_id: str = "",
    ) -> str:
        if not allow_duplicate_launch:
            duplicate_session = self._find_duplicate_strategy_session(api_name=api_name, config=config)
            if duplicate_session is not None:
                if select_session:
                    self._focus_session_row(duplicate_session.session_id)
                raise ValueError(self._format_duplicate_launch_block_message(duplicate_session, imported=False))

        session_id = self._next_session_id()
        session_symbol = self._format_strategy_symbol_display(config.inst_id, config.trade_inst_id)
        session_started_at = datetime.now()
        session_log_path = strategy_session_log_file_path(
            started_at=session_started_at,
            session_id=session_id,
            strategy_name=definition.name,
            symbol=session_symbol,
            api_name=api_name,
        ).resolve()
        engine = self._create_session_engine(
            strategy_id=definition.strategy_id,
            strategy_name=definition.name,
            session_id=session_id,
            symbol=session_symbol,
            api_name=api_name,
            log_file_path=session_log_path,
            notifier=notifier,
            direction_label=direction_label,
            run_mode_label=run_mode_label,
            trader_id=trader_id.strip(),
        )
        session = StrategySession(
            session_id=session_id,
            api_name=api_name,
            strategy_id=definition.strategy_id,
            strategy_name=definition.name,
            symbol=session_symbol,
            direction_label=direction_label,
            run_mode_label=run_mode_label,
            engine=engine,
            config=config,
            started_at=session_started_at,
            log_file_path=session_log_path,
            trader_id=trader_id.strip(),
            trader_slot_id=trader_slot_id.strip(),
        )

        self.sessions[session_id] = session
        self._upsert_session_row(session)
        try:
            engine.start(credentials, config)
        except Exception:
            self.sessions.pop(session_id, None)
            if self.session_tree.exists(session_id):
                self.session_tree.delete(session_id)
            raise
        self._record_strategy_session_started(session)
        if select_session:
            self.session_tree.selection_set(session_id)
            self.session_tree.focus(session_id)
        self._refresh_selected_session_details()
        if source_label:
            self._log_session_message(session, f"{source_label} 已提交启动请求。")
        else:
            self._log_session_message(session, "已提交启动请求。")
        return session_id

    def start(self) -> None:
        try:
            definition = self._selected_strategy_definition()
            credentials, config = self._collect_inputs(definition)
            notifier = self._build_notifier(config)
            if not self._confirm_start(definition, config):
                return

            self._save_credentials_now(silent=True)
            self._save_notification_settings_now(silent=True)
            api_name = credentials.profile_name or self._current_credential_profile()
            self._start_strategy_session(
                definition=definition,
                credentials=credentials,
                config=config,
                notifier=notifier,
                api_name=api_name,
                direction_label=self.signal_mode_label.get(),
                run_mode_label=self.run_mode_label.get(),
                select_session=True,
            )
        except Exception as exc:
            messagebox.showerror("启动失败", str(exc))

    def stop_selected_session(self) -> None:
        session = self._selected_session()
        if session is None:
            messagebox.showinfo("提示", "请先在右侧选择一个策略会话。")
            return
        self._request_stop_strategy_session(
            session.session_id,
            ended_reason="用户手动停止",
            source_label="用户手动停止",
            show_dialog=True,
        )

    def _request_stop_strategy_session(
        self,
        session_id: str,
        *,
        ended_reason: str,
        source_label: str,
        show_dialog: bool,
    ) -> bool:
        session = self.sessions.get(session_id)
        if session is None:
            return False
        if session.stop_cleanup_in_progress:
            if show_dialog:
                messagebox.showinfo("提示", "这个策略正在执行停止清理，请稍等。")
            return False
        if not session.engine.is_running:
            if show_dialog:
                messagebox.showinfo("提示", "这个策略已经停止了。")
            return False
        if session.config.run_mode == "signal_only":
            self._stop_sessions_by_id([session.session_id])
            return True

        credentials = self._credentials_for_profile_or_none(session.api_name)
        session.status = "停止中"
        session.stop_cleanup_in_progress = True
        session.ended_reason = ended_reason
        session.engine.stop()
        self._upsert_session_row(session)
        self._refresh_selected_session_details()
        self._sync_strategy_history_from_session(session)
        self._log_session_message(session, f"{source_label}，正在检查本策略委托与持仓。")
        if credentials is None:
            session.stop_cleanup_in_progress = False
            session.status = "已停止"
            session.stopped_at = datetime.now()
            session.ended_reason = f"{ended_reason}（未找到对应API凭证，未执行撤单检查）"
            self._remove_recoverable_strategy_session(session.session_id)
            self._upsert_session_row(session)
            self._refresh_selected_session_details()
            self._sync_strategy_history_from_session(session)
            self._log_session_message(session, "停止清理失败：未找到该会话对应的 API 凭证，请人工检查委托与仓位。")
            if show_dialog:
                messagebox.showwarning(
                    "停止提醒",
                    "策略线程已收到停止请求，但当前找不到该会话对应的 API 凭证。\n\n请人工检查：\n- 当前委托是否还有残留\n- 是否已经成交并留下仓位",
                )
            return False
        threading.Thread(
            target=self._stop_session_cleanup_worker,
            args=(session.session_id, credentials),
            daemon=True,
        ).start()
        return True

    def _credentials_for_profile_or_none(self, profile_name: str) -> Credentials | None:
        target = profile_name.strip() or self._current_credential_profile()
        current_profile = self._current_credential_profile()
        if target == current_profile:
            current_credentials = self._current_credentials_or_none()
            if current_credentials is not None:
                return Credentials(
                    api_key=current_credentials.api_key,
                    secret_key=current_credentials.secret_key,
                    passphrase=current_credentials.passphrase,
                    profile_name=target,
                )
        snapshot = self._credential_profiles.get(target)
        if not snapshot:
            return None
        api_key = str(snapshot.get("api_key", "")).strip()
        secret_key = str(snapshot.get("secret_key", "")).strip()
        passphrase = str(snapshot.get("passphrase", "")).strip()
        if not api_key or not secret_key or not passphrase:
            return None
        return Credentials(
            api_key=api_key,
            secret_key=secret_key,
            passphrase=passphrase,
            profile_name=target,
        )

    def _load_strategy_stop_cleanup_snapshot(
        self,
        session: StrategySession,
        credentials: Credentials,
        environment: str,
    ) -> StrategyStopCleanupSnapshot:
        pending_orders = self.client.get_pending_orders(credentials, environment=environment, limit=200)
        order_history = self.client.get_order_history(credentials, environment=environment, limit=200)
        positions = self.client.get_positions(credentials, environment=environment)
        return StrategyStopCleanupSnapshot(
            effective_environment=environment,
            pending_orders=pending_orders,
            order_history=order_history,
            positions=positions,
        )

    def _load_strategy_stop_cleanup_snapshot_with_fallback(
        self,
        session: StrategySession,
        credentials: Credentials,
    ) -> StrategyStopCleanupSnapshot:
        environment = session.config.environment
        try:
            return self._load_strategy_stop_cleanup_snapshot(session, credentials, environment)
        except Exception as exc:
            message = str(exc)
            if "50101" not in message or "current environment" not in message:
                raise
            alternate = "live" if environment == "demo" else "demo"
            snapshot = self._load_strategy_stop_cleanup_snapshot(session, credentials, alternate)
            snapshot.environment_note = f"停止检查自动切换到 {'实盘' if alternate == 'live' else '模拟'} 环境执行。"
            return snapshot

    def _perform_stop_session_cleanup(
        self,
        session: StrategySession,
        credentials: Credentials,
    ) -> StrategyStopCleanupResult:
        initial_snapshot = self._load_strategy_stop_cleanup_snapshot_with_fallback(session, credentials)
        effective_environment = initial_snapshot.effective_environment
        initial_pending = [item for item in initial_snapshot.pending_orders if _trade_order_belongs_to_session(item, session)]
        cancelable_pending = [
            item for item in initial_pending if _trade_order_session_role(item, session) in {"ent", "exi"}
        ]

        cancel_requested_summaries: list[str] = []
        cancel_failed_summaries: list[str] = []
        for item in cancelable_pending:
            try:
                result = self._cancel_pending_order_request(credentials, environment=effective_environment, item=item)
                cancel_requested_summaries.append(
                    f"{_trade_order_cancel_summary(item)} | sCode={result.s_code} | sMsg={result.s_msg or 'accepted'}"
                )
            except Exception as exc:
                cancel_failed_summaries.append(
                    f"{_trade_order_cancel_summary(item)} | 原因={_format_network_error_message(str(exc))}"
                )

        final_snapshot = initial_snapshot
        wait_rounds = 3 if cancelable_pending else 1
        for attempt in range(wait_rounds):
            if attempt > 0:
                threading.Event().wait(0.6)
            final_snapshot = self._load_strategy_stop_cleanup_snapshot(session, credentials, effective_environment)
            remaining_cancelable = [
                item
                for item in final_snapshot.pending_orders
                if _trade_order_session_role(item, session) in {"ent", "exi"}
            ]
            if not remaining_cancelable:
                break

        for _ in range(20):
            if not session.engine.is_running:
                break
            threading.Event().wait(0.25)

        remaining_cancelable = [
            item
            for item in final_snapshot.pending_orders
            if _trade_order_session_role(item, session) in {"ent", "exi"}
        ]
        protective_pending = [
            item
            for item in final_snapshot.pending_orders
            if _trade_order_session_role(item, session) == "slg"
        ]
        session_history = [item for item in final_snapshot.order_history if _trade_order_belongs_to_session(item, session)]
        filled_orders = [
            item
            for item in session_history
            if (item.filled_size or Decimal("0")) > 0 or (item.state or "").strip().lower() in {"filled", "partially_filled"}
        ]
        trade_inst_id = (session.config.trade_inst_id or session.config.inst_id or "").strip().upper()
        open_positions = [
            position
            for position in final_snapshot.positions
            if position.inst_id.strip().upper() == trade_inst_id and position.position != 0
        ]

        needs_manual_review = bool(cancel_failed_summaries or remaining_cancelable)
        if open_positions and (filled_orders or protective_pending):
            needs_manual_review = True
        if protective_pending and not open_positions:
            needs_manual_review = True

        final_reason_parts: list[str] = []
        if cancel_failed_summaries or remaining_cancelable:
            final_reason_parts.append("撤单未完全确认，需人工检查")
        if open_positions and filled_orders:
            final_reason_parts.append("检测到已成交仓位，需人工判断")
        elif protective_pending:
            final_reason_parts.append("检测到保护委托，需人工确认")
        final_reason = "用户手动停止"
        if final_reason_parts:
            final_reason = f"用户手动停止（{'；'.join(final_reason_parts)}）"

        return StrategyStopCleanupResult(
            session_id=session.session_id,
            effective_environment=effective_environment,
            environment_note=initial_snapshot.environment_note,
            cancel_requested_summaries=tuple(cancel_requested_summaries),
            cancel_failed_summaries=tuple(cancel_failed_summaries),
            remaining_pending_summaries=tuple(_trade_order_cancel_summary(item) for item in remaining_cancelable),
            protective_pending_summaries=tuple(_trade_order_cancel_summary(item) for item in protective_pending),
            filled_order_summaries=tuple(_trade_order_fill_summary(item) for item in filled_orders),
            open_position_summaries=tuple(_position_manual_review_summary(item) for item in open_positions),
            needs_manual_review=needs_manual_review,
            final_reason=final_reason,
        )

    def _stop_session_cleanup_worker(self, session_id: str, credentials: Credentials) -> None:
        session = self.sessions.get(session_id)
        if session is None:
            return
        try:
            result = self._perform_stop_session_cleanup(session, credentials)
        except Exception as exc:
            self.root.after(0, lambda: self._apply_stop_session_cleanup_error(session_id, str(exc)))
            return
        self.root.after(0, lambda: self._apply_stop_session_cleanup_result(result))

    def _apply_stop_session_cleanup_result(self, result: StrategyStopCleanupResult) -> None:
        session = self.sessions.get(result.session_id)
        if session is None:
            return
        session.stop_cleanup_in_progress = False
        session.status = "已停止"
        if session.stopped_at is None:
            session.stopped_at = datetime.now()
        session.ended_reason = result.final_reason
        self._remove_recoverable_strategy_session(session.session_id)
        self._upsert_session_row(session)
        self._refresh_selected_session_details()
        self._sync_strategy_history_from_session(session)

        if result.environment_note:
            self._log_session_message(session, result.environment_note)
        if result.cancel_requested_summaries:
            self._log_session_message(session, f"停止清理：已提交撤单 {len(result.cancel_requested_summaries)} 条。")
            for summary in result.cancel_requested_summaries:
                self._log_session_message(session, f"停止清理 | 已提交撤单 | {summary}")
        else:
            self._log_session_message(session, "停止清理：未发现需要自动撤销的程序挂单。")
        for summary in result.cancel_failed_summaries:
            self._log_session_message(session, f"停止清理 | 撤单失败 | {summary}")
        for summary in result.remaining_pending_summaries:
            self._log_session_message(session, f"停止清理 | 残留未撤委托 | {summary}")
        for summary in result.filled_order_summaries:
            self._log_session_message(session, f"停止清理 | 检测到已成交委托 | {summary}")
        for summary in result.open_position_summaries:
            self._log_session_message(session, f"停止清理 | 检测到仍有仓位 | {summary}")
        for summary in result.protective_pending_summaries:
            self._log_session_message(session, f"停止清理 | 检测到保护委托 | {summary}")
        self._log_session_message(session, f"停止流程结束 | 结论={result.final_reason}")

        if session.api_name.strip() == self._current_credential_profile():
            self.refresh_positions()
            self.refresh_order_views()

        if result.needs_manual_review:
            details: list[str] = ["策略已停止，但检测到需要人工接管的情况。"]
            if result.cancel_failed_summaries or result.remaining_pending_summaries:
                details.append("")
                details.append("委托检查：")
                details.append(
                    f"- 撤单失败 {len(result.cancel_failed_summaries)} 条，残留未确认撤销 {len(result.remaining_pending_summaries)} 条"
                )
            if result.filled_order_summaries and result.open_position_summaries:
                details.append("")
                details.append("已成交且仍有仓位：")
                for summary in result.open_position_summaries[:3]:
                    details.append(f"- {summary}")
            if result.protective_pending_summaries:
                details.append("")
                details.append("保护委托仍在交易所：")
                for summary in result.protective_pending_summaries[:3]:
                    details.append(f"- {summary}")
            details.append("")
            details.append("请人工检查“当前委托 / 账户持仓 / OKX 托管止损”，再决定是否保留、撤单或平仓。")
            messagebox.showwarning("停止提醒", "\n".join(details))
            return

        messagebox.showinfo(
            "停止结果",
            (
                "策略已停止。\n\n"
                f"自动撤单：{len(result.cancel_requested_summaries)} 条\n"
                "未发现残留仓位或需人工接管的问题。"
            ),
        )

    def _apply_stop_session_cleanup_error(self, session_id: str, message: str) -> None:
        session = self.sessions.get(session_id)
        if session is None:
            return
        session.stop_cleanup_in_progress = False
        session.status = "已停止"
        if session.stopped_at is None:
            session.stopped_at = datetime.now()
        friendly_message = _format_network_error_message(message)
        session.ended_reason = "用户手动停止（停止清理失败，需人工检查）"
        self._remove_recoverable_strategy_session(session.session_id)
        self._upsert_session_row(session)
        self._refresh_selected_session_details()
        self._sync_strategy_history_from_session(session)
        self._log_session_message(session, f"停止清理失败：{friendly_message}")
        self._log_session_message(session, "请人工检查当前委托、历史委托与账户持仓。")
        messagebox.showwarning(
            "停止提醒",
            "策略线程已收到停止请求，但停止清理阶段失败。\n\n"
            f"原因：{friendly_message}\n\n"
            "请人工检查：\n- 当前委托是否仍有残留\n- 是否已经成交并留下仓位\n- OKX 托管止损是否仍在",
        )

    @staticmethod
    def _session_can_be_cleared(session: StrategySession) -> bool:
        return session.status == "已停止" and not session.engine.is_running

    @staticmethod
    def _next_session_selection_after_clear(
        selected_before: str | None,
        remaining_session_ids: tuple[str, ...] | list[str],
    ) -> str | None:
        if selected_before and selected_before in remaining_session_ids:
            return selected_before
        return remaining_session_ids[0] if remaining_session_ids else None

    @staticmethod
    def _next_history_selection_after_mutation(
        selected_before: str | None,
        remaining_record_ids: tuple[str, ...] | list[str],
    ) -> str | None:
        if selected_before and selected_before in remaining_record_ids:
            return selected_before
        return remaining_record_ids[0] if remaining_record_ids else None

    def clear_stopped_sessions(self) -> None:
        stopped_ids = [
            session_id
            for session_id, session in self.sessions.items()
            if self._session_can_be_cleared(session)
        ]
        if not stopped_ids:
            messagebox.showinfo("提示", "当前没有可清空的已停止策略。")
            return

        confirmed = messagebox.askyesno(
            "确认清空",
            f"确认从运行中策略列表清空 {len(stopped_ids)} 条已停止会话吗？\n\n历史策略记录和独立日志会保留。",
            parent=self.root,
        )
        if not confirmed:
            return

        tree = self.session_tree
        selected_before = tree.selection()[0] if tree.selection() else None
        for session_id in stopped_ids:
            self.sessions.pop(session_id, None)
            if tree.exists(session_id):
                tree.delete(session_id)

        remaining = tuple(tree.get_children())
        next_selection = self._next_session_selection_after_clear(selected_before, remaining)
        if next_selection is not None:
            tree.selection_set(next_selection)
            tree.focus(next_selection)
            tree.see(next_selection)
        self._refresh_selected_session_details()
        self._enqueue_log(f"已从运行中策略列表清空 {len(stopped_ids)} 条已停止会话；历史策略记录保留。")

    def debug_hourly_values(self) -> None:
        symbol = _normalize_symbol_input(self.symbol.get())
        if not symbol:
            messagebox.showerror("提示", "请先选择交易标的")
            return
        ema_period = self._parse_positive_int(self.ema_period.get(), "EMA小周期")
        trend_ema_period = self._parse_positive_int(self.trend_ema_period.get(), "EMA中周期")
        entry_reference_ema_period = 0
        if is_dynamic_strategy_id(self._selected_strategy_definition().strategy_id):
            entry_reference_ema_period = self._parse_nonnegative_int(self.entry_reference_ema_period.get(), "挂单参考EMA")
        if entry_reference_ema_period <= 0:
            entry_reference_ema_period = ema_period
        self._enqueue_log(
            f"正在获取 {symbol} 的 1 小时调试值，EMA小周期={ema_period}，趋势EMA={trend_ema_period}，挂单参考EMA={entry_reference_ema_period} ..."
        )
        threading.Thread(
            target=self._debug_hourly_values_worker,
            args=(symbol, ema_period, trend_ema_period, entry_reference_ema_period),
            daemon=True,
        ).start()

    def _debug_hourly_values_worker(
        self,
        symbol: str,
        ema_period: int,
        trend_ema_period: int,
        entry_reference_ema_period: int,
    ) -> None:
        try:
            snapshot = fetch_hourly_ema_debug(
                self.client,
                symbol,
                ema_period=ema_period,
                trend_ema_period=trend_ema_period,
                entry_reference_ema_period=entry_reference_ema_period,
            )
            self._enqueue_log(format_hourly_debug(symbol, snapshot))
        except Exception as exc:
            self._enqueue_log(f"获取 1 小时调试值失败：{exc}")

    def refresh_positions(self) -> None:
        if self._positions_refreshing:
            return

        credentials = self._current_credentials_or_none()
        if credentials is None:
            self._apply_positions([], "未配置 API 凭证，无法读取持仓。")
            _reset_refresh_health(self._positions_refresh_health)
            self._refresh_all_refresh_badges()
            return

        self._positions_refreshing = True
        self.positions_summary_text.set("正在刷新账户持仓...")
        environment = ENV_OPTIONS[self.environment_label.get()]
        profile_name = credentials.profile_name or self._current_credential_profile()
        threading.Thread(
            target=self._refresh_positions_worker,
            args=(credentials, environment, profile_name),
            daemon=True,
        ).start()

    def _refresh_positions_worker(self, credentials: Credentials, environment: str, profile_name: str) -> None:
        try:
            positions = self.client.get_positions(credentials, environment=environment)
            upl_usdt_prices = _build_upl_usdt_price_map(self.client, positions)
            position_instruments = _build_position_instrument_map(self.client, positions)
            position_tickers = _build_position_ticker_map(self.client, positions)
        except Exception as exc:
            message = str(exc)
            if "50101" in message and "current environment" in message:
                alternate = "live" if environment == "demo" else "demo"
                try:
                    positions = self.client.get_positions(credentials, environment=alternate)
                    upl_usdt_prices = _build_upl_usdt_price_map(self.client, positions)
                    position_instruments = _build_position_instrument_map(self.client, positions)
                    position_tickers = _build_position_ticker_map(self.client, positions)
                except Exception:
                    self.root.after(0, lambda: self._apply_positions_error(message))
                    return
                summary = (
                    f"当前 API Key 与 {alternate} 环境匹配，已自动按 "
                    f"{'实盘' if alternate == 'live' else '模拟盘'} 读取持仓。"
                )
                self.root.after(
                    0,
                    lambda: self._apply_positions(
                        positions=positions,
                        summary=summary,
                        effective_environment=alternate,
                        credential_profile_name=profile_name,
                        upl_usdt_prices=upl_usdt_prices,
                        position_instruments=position_instruments,
                        position_tickers=position_tickers,
                    ),
                )
                return
            self.root.after(0, lambda: self._apply_positions_error(message))
            return
        self.root.after(
            0,
            lambda: self._apply_positions(
                positions=positions,
                summary=None,
                effective_environment=environment,
                credential_profile_name=profile_name,
                upl_usdt_prices=upl_usdt_prices,
                position_instruments=position_instruments,
                position_tickers=position_tickers,
            ),
        )

    def _apply_positions(
        self,
        positions: list[OkxPosition],
        summary: str | None = None,
        effective_environment: str | None = None,
        credential_profile_name: str | None = None,
        upl_usdt_prices: dict[str, Decimal] | None = None,
        position_instruments: dict[str, Instrument] | None = None,
        position_tickers: dict[str, OkxTicker] | None = None,
    ) -> None:
        self._positions_refreshing = False
        self._latest_positions = list(positions)
        self._positions_context_note = summary
        self._positions_last_refresh_at = datetime.now()
        self._positions_effective_environment = effective_environment
        self._positions_context_profile_name = (credential_profile_name or self._current_credential_profile()).strip()
        _mark_refresh_health_success(self._positions_refresh_health, at=self._positions_last_refresh_at)
        self._refresh_all_refresh_badges()
        self._upl_usdt_prices = dict(upl_usdt_prices or {})
        self._position_instruments = dict(position_instruments or {})
        self._position_tickers = dict(position_tickers or {})
        profile_name = self._positions_context_profile_name
        if profile_name:
            self._positions_snapshot_by_profile[profile_name] = ProfilePositionSnapshot(
                api_name=profile_name,
                effective_environment=effective_environment,
                positions=list(positions),
                upl_usdt_prices=dict(upl_usdt_prices or {}),
                refreshed_at=self._positions_last_refresh_at,
            )
        if profile_name and effective_environment:
            self._sync_position_note_state_for_positions(
                profile_name=profile_name,
                environment=effective_environment,
                positions=positions,
            )
        self._render_positions_view()
        self._refresh_session_live_pnl_cache()
        for session in self.sessions.values():
            self._upsert_session_row(session)
        self._refresh_running_session_summary()
        self._refresh_selected_session_details()

    @staticmethod
    def _session_counts_toward_running_summary(session: StrategySession) -> bool:
        return bool(
            session.engine.is_running
            or session.stop_cleanup_in_progress
            or session.status in {"运行中", "停止中", "待恢复", "恢复中"}
        )

    def _positions_snapshot_for_session(self, session: StrategySession) -> ProfilePositionSnapshot | None:
        profile_name = (session.api_name or "").strip()
        if not profile_name:
            return None
        snapshot = self._positions_snapshot_by_profile.get(profile_name)
        if snapshot is None:
            return None
        expected_environment = str(getattr(session.config, "environment", "") or "").strip().lower()
        effective_environment = str(snapshot.effective_environment or "").strip().lower()
        if expected_environment and effective_environment and expected_environment != effective_environment:
            return None
        return snapshot

    def _refresh_session_live_pnl_cache(self) -> None:
        cache: dict[str, tuple[Decimal | None, datetime | None]] = {
            session.session_id: (None, None) for session in self.sessions.values()
        }
        sessions_by_snapshot_key: dict[tuple[str, str], tuple[ProfilePositionSnapshot, list[StrategySession]]] = {}
        for session in self.sessions.values():
            if session.active_trade is None:
                continue
            snapshot = self._positions_snapshot_for_session(session)
            if snapshot is None:
                continue
            snapshot_key = (
                snapshot.api_name,
                str(snapshot.effective_environment or "").strip().lower(),
            )
            bucket = sessions_by_snapshot_key.get(snapshot_key)
            if bucket is None:
                sessions_by_snapshot_key[snapshot_key] = (snapshot, [session])
            else:
                bucket[1].append(session)

        for snapshot, sessions in sessions_by_snapshot_key.values():
            for position in snapshot.positions:
                candidate_sessions = [
                    session
                    for session in sessions
                    if _position_matches_session_live_pnl(
                        position,
                        trade_inst_id=_session_trade_inst_id(session),
                        expected_sides=_session_expected_position_sides(session),
                    )
                ]
                if not candidate_sessions:
                    continue
                pnl_value = _position_unrealized_pnl_usdt(position, snapshot.upl_usdt_prices)
                if pnl_value is None and _infer_upl_currency(position) in {"USDT", "USD", "USDC"}:
                    pnl_value = position.unrealized_pnl
                if pnl_value is None:
                    for session in candidate_sessions:
                        cache[session.session_id] = (cache[session.session_id][0], snapshot.refreshed_at)
                    continue

                allocations: dict[str, Decimal] = {}
                weighted_sizes: list[Decimal] = []
                for session in candidate_sessions:
                    trade_size = session.active_trade.size if session.active_trade is not None else None
                    if trade_size is None or trade_size <= 0:
                        weighted_sizes = []
                        break
                    weighted_sizes.append(trade_size)

                if len(candidate_sessions) == 1:
                    allocations[candidate_sessions[0].session_id] = pnl_value
                elif weighted_sizes:
                    total_size = sum(weighted_sizes, Decimal("0"))
                    if total_size > 0:
                        for session, trade_size in zip(candidate_sessions, weighted_sizes):
                            allocations[session.session_id] = pnl_value * trade_size / total_size
                if not allocations:
                    shared_value = pnl_value / Decimal(len(candidate_sessions))
                    for session in candidate_sessions:
                        allocations[session.session_id] = shared_value

                for session in candidate_sessions:
                    previous_value, _ = cache[session.session_id]
                    allocated = allocations.get(session.session_id)
                    if allocated is None:
                        cache[session.session_id] = (previous_value, snapshot.refreshed_at)
                        continue
                    cache[session.session_id] = (
                        (previous_value or Decimal("0")) + allocated,
                        snapshot.refreshed_at,
                    )

        self._session_live_pnl_cache = cache

    def _session_live_pnl_snapshot(self, session: StrategySession) -> tuple[Decimal | None, datetime | None]:
        return self._session_live_pnl_cache.get(session.session_id, (None, None))

    def _active_duplicate_strategy_groups(self) -> dict[tuple[str, StrategyConfig], list[StrategySession]]:
        groups: dict[tuple[str, StrategyConfig], list[StrategySession]] = {}
        sessions = getattr(self, "sessions", {})
        items = sessions.values() if isinstance(sessions, dict) else ()
        for session in items:
            api_name = str(getattr(session, "api_name", "") or "").strip()
            config = getattr(session, "config", None)
            if not api_name or not isinstance(config, StrategyConfig):
                continue
            if not QuantApp._session_blocks_duplicate_launch(session):
                continue
            key = (api_name, config)
            groups.setdefault(key, []).append(session)
        return {key: items for key, items in groups.items() if len(items) > 1}

    def _duplicate_launch_conflicts_for(self, session: StrategySession) -> list[StrategySession]:
        key = (session.api_name.strip(), session.config)
        groups = QuantApp._active_duplicate_strategy_groups(self)
        items = groups.get(key, [])
        return [item for item in items if item.session_id != session.session_id]

    def _session_has_duplicate_launch_conflict(self, session: StrategySession) -> bool:
        return bool(QuantApp._duplicate_launch_conflicts_for(self, session))

    @staticmethod
    def _session_category_label(session: StrategySession) -> str:
        trader_id = str(getattr(session, "trader_id", "") or "").strip()
        if trader_id:
            return "交易员策略"
        config = getattr(session, "config", None)
        run_mode = str(getattr(config, "run_mode", "") or "").strip().lower()
        if run_mode == "signal_only":
            return "信号观察台"
        return "普通量化"

    def _session_trader_label(self, session: StrategySession) -> str:
        trader_id = str(getattr(session, "trader_id", "") or "").strip()
        if not trader_id:
            return "-"
        draft = self._trader_desk_draft_by_id(trader_id)
        if draft is None:
            return trader_id
        return str(getattr(draft, "trader_id", "") or "").strip() or trader_id

    def _current_running_session_filter_label(self) -> str:
        selected_filter: object = getattr(self, "running_session_filter", "全部")
        if hasattr(selected_filter, "get"):
            label = str(selected_filter.get() or "").strip()
        else:
            label = str(selected_filter or "").strip()
        if label in RUNNING_SESSION_FILTER_OPTIONS:
            return label
        return "全部"

    def _session_matches_running_filter(self, session: StrategySession) -> bool:
        selected_filter = QuantApp._current_running_session_filter_label(self)
        if selected_filter == "全部":
            return True
        return QuantApp._session_category_label(session) == selected_filter

    @staticmethod
    def _build_duplicate_launch_conflict_warning(
        session: StrategySession,
        conflicts: list[StrategySession],
    ) -> str:
        if not conflicts:
            return ""
        ordered = sorted(conflicts, key=lambda item: (item.started_at, item.session_id))
        session_refs = ", ".join(item.session_id for item in ordered)
        return (
            f"重复风险：与 {session_refs} 参数完全相同（同 API）。"
            " 如需复制参数开新策略，请先修改标的或切换 API 后再启动。"
        )

    def _refresh_running_session_summary(self) -> None:
        self._refresh_session_live_pnl_cache()
        active_sessions = [
            session for session in self.sessions.values() if self._session_counts_toward_running_summary(session)
        ]
        if not active_sessions:
            self.session_summary_text.set("多策略合计：当前没有运行中的策略。")
            return

        net_total = Decimal("0")
        live_total = Decimal("0")
        live_covered = 0
        latest_refresh_at: datetime | None = None
        for session in active_sessions:
            net_total += session.net_pnl_total or Decimal("0")
            live_pnl, refreshed_at = self._session_live_pnl_snapshot(session)
            if refreshed_at is not None and (latest_refresh_at is None or refreshed_at > latest_refresh_at):
                latest_refresh_at = refreshed_at
            if live_pnl is None:
                continue
            live_total += live_pnl
            live_covered += 1

        parts = [
            f"多策略合计：{len(active_sessions)} 个策略",
            f"实时浮盈亏={_format_optional_usdt_precise(live_total, places=2) if live_covered else '-'}",
            f"净盈亏={_format_optional_usdt_precise(net_total, places=2)}",
        ]
        duplicate_groups = QuantApp._active_duplicate_strategy_groups(self)
        if duplicate_groups:
            duplicate_sessions = sum(len(items) for items in duplicate_groups.values())
            parts.append(f"重复风险 {len(duplicate_groups)}组/{duplicate_sessions}条")
        if live_covered < len(active_sessions):
            parts.append(f"浮盈覆盖 {live_covered}/{len(active_sessions)}")
        selected_filter = QuantApp._current_running_session_filter_label(self)
        if selected_filter != "全部":
            visible_count = sum(1 for session in active_sessions if QuantApp._session_matches_running_filter(self, session))
            parts.append(f"当前筛选 {selected_filter} {visible_count}条")
        if latest_refresh_at is not None:
            parts.append(f"参考持仓 {latest_refresh_at.strftime('%H:%M:%S')}")
        else:
            parts.append("实时浮盈待持仓刷新")
        self.session_summary_text.set(" | ".join(parts))

    def _refresh_running_session_tree(self) -> None:
        tree = self.session_tree
        selected_before = tree.selection()[0] if tree.selection() else None
        for session in self.sessions.values():
            self._upsert_session_row(session)

        remaining = tuple(tree.get_children())
        next_selection = None
        if selected_before and tree.exists(selected_before):
            next_selection = selected_before
        elif remaining:
            next_selection = remaining[0]
        if next_selection is not None:
            tree.selection_set(next_selection)
            tree.focus(next_selection)
            tree.see(next_selection)

    def _on_running_session_filter_changed(self, *_: object) -> None:
        self._refresh_running_session_summary()
        self._refresh_running_session_tree()
        self._refresh_selected_session_details()

    def _render_positions_view(self) -> None:
        selected_before = self.position_tree.selection()[0] if self.position_tree.selection() else None
        selected_payload = self._selected_position_payload()
        selected_position_key = None
        if selected_payload is not None and selected_payload["kind"] == "position":
            item = selected_payload["item"]
            if isinstance(item, OkxPosition):
                selected_position_key = _position_tree_row_id(item)

        self._positions_view_rendering = True
        try:
            self.position_tree.delete(*self.position_tree.get_children())
            self._position_row_payloads.clear()
            visible_positions = _filter_positions(
                self._latest_positions,
                inst_type=POSITION_TYPE_OPTIONS[self.position_type_filter.get()],
                keyword=self.position_keyword.get(),
                note_texts=self._current_position_note_text_map(),
            )
            groups = _group_positions_for_tree(visible_positions)
            for asset_label, buckets in groups.items():
                asset_id = _asset_group_row_id(asset_label)
                asset_positions = [item for bucket in buckets.values() for item in bucket]
                asset_metrics = _aggregate_position_metrics(asset_positions, self._upl_usdt_prices, self._position_instruments)
                asset_label_text = f"{asset_label} 风险单元"
                self.position_tree.insert(
                    "",
                    END,
                    iid=asset_id,
                    text=asset_label_text,
                    values=_build_group_row_values("组合", asset_metrics),
                    open=True,
                    tags=("group", _pnl_tag(asset_metrics["upl"])),
                )
                self._position_row_payloads[asset_id] = {
                    "kind": "group",
                    "label": asset_label_text,
                    "item": asset_positions,
                    "metrics": asset_metrics,
                }

                for bucket_label, bucket_positions in buckets.items():
                    if bucket_label == "__DIRECT__":
                        for position in bucket_positions:
                            self._insert_position_row(asset_id, position, _position_tree_row_id(position))
                        continue

                    bucket_id = _bucket_group_row_id(asset_label, bucket_label)
                    bucket_metrics = _aggregate_position_metrics(
                        bucket_positions,
                        self._upl_usdt_prices,
                        self._position_instruments,
                    )
                    self.position_tree.insert(
                        asset_id,
                        END,
                        iid=bucket_id,
                        text=bucket_label,
                        values=_build_group_row_values("分组", bucket_metrics),
                        open=True,
                        tags=("group", _pnl_tag(bucket_metrics["upl"])),
                    )
                    self._position_row_payloads[bucket_id] = {
                        "kind": "group",
                        "label": bucket_label,
                        "item": bucket_positions,
                        "metrics": bucket_metrics,
                    }
                    for position in bucket_positions:
                        self._insert_position_row(bucket_id, position, _position_tree_row_id(position))

            self._update_position_summary(visible_positions)
            self._update_position_metrics(visible_positions)

            target = _resolve_position_selection_target(
                existing_ids=set(self._position_row_payloads.keys()),
                selected_position_key=selected_position_key,
                protection_position_key=self._protection_form_position_key,
                selected_before=selected_before,
                top_items=self.position_tree.get_children(),
            )

            if target is not None:
                self.position_tree.selection_set(target)
                self.position_tree.focus(target)
        finally:
            self._positions_view_rendering = False
        self._refresh_position_detail_panel()
        self._sync_positions_zoom_window()
        self._refresh_protection_window_view()

    def _insert_position_row(self, parent_id: str, position: OkxPosition, row_id: str) -> None:
        label = position.inst_id
        if position.pos_side and position.pos_side.lower() != "net":
            label = f"{label} [{position.pos_side}]"
        tags = [tag for tag in (_pnl_tag(position.unrealized_pnl), _margin_mode_tag(position.mgn_mode)) if tag]
        self.position_tree.insert(
            parent_id,
            END,
            iid=row_id,
            text=label,
            values=(
                position.inst_type,
                _format_margin_mode(position.mgn_mode),
                _format_position_option_price_component(position, self._upl_usdt_prices, component="time_value"),
                _format_position_option_component_usdt(position, self._upl_usdt_prices, component="time_value"),
                _format_position_option_price_component(position, self._upl_usdt_prices, component="intrinsic_value"),
                _format_position_option_component_usdt(position, self._upl_usdt_prices, component="intrinsic_value"),
                _format_position_quote_price(
                    position,
                    self._position_instruments,
                    self._position_tickers,
                    side="bid",
                ),
                _format_position_quote_price_usdt(
                    position,
                    self._position_tickers,
                    self._upl_usdt_prices,
                    side="bid",
                ),
                _format_position_quote_price(
                    position,
                    self._position_instruments,
                    self._position_tickers,
                    side="ask",
                ),
                _format_position_quote_price_usdt(
                    position,
                    self._position_tickers,
                    self._upl_usdt_prices,
                    side="ask",
                ),
                _format_mark_price(position),
                _format_position_mark_price_usdt(position, self._upl_usdt_prices),
                _format_position_avg_price(position, self._position_instruments),
                _format_position_avg_price_usdt(position, self._upl_usdt_prices),
                _format_position_size(position, self._position_instruments),
                _format_option_trade_side_display(position),
                _format_position_unrealized_pnl(position),
                _format_optional_usdt(_position_unrealized_pnl_usdt(position, self._upl_usdt_prices)),
                _format_optional_decimal_fixed(position.realized_pnl, places=5, with_sign=True),
                _format_optional_usdt(_position_realized_pnl_usdt(position, self._upl_usdt_prices)),
                _format_position_market_value(position, self._position_instruments, self._upl_usdt_prices),
                _format_optional_decimal(position.liquidation_price),
                _format_ratio(position.margin_ratio, places=2),
                _format_optional_integer(position.initial_margin),
                _format_optional_integer(position.maintenance_margin),
                _format_optional_decimal_fixed(_position_delta_value(position, self._position_instruments), places=5),
                _format_optional_decimal_fixed(position.gamma, places=5),
                _format_optional_decimal_fixed(position.vega, places=5),
                _format_optional_decimal_fixed(position.theta, places=5),
                _format_optional_usdt_precise(_position_theta_usdt(position, self._upl_usdt_prices), places=2),
                self._current_position_note_summary(position),
            ),
            tags=tuple(tags),
        )
        self._position_row_payloads[row_id] = {
            "kind": "position",
            "label": label,
            "item": position,
            "metrics": None,
        }

    def _update_position_summary(self, visible_positions: list[OkxPosition]) -> None:
        timestamp = self._positions_last_refresh_at.strftime("%H:%M:%S") if self._positions_last_refresh_at else "--:--:--"
        parts: list[str] = []
        if self._positions_context_note:
            parts.append(self._positions_context_note)
        parts.append(f"API配置：{self._current_credential_profile()}")

        total_count = len(self._latest_positions)
        visible_count = len(visible_positions)
        if total_count:
            summary = f"当前仓位（{total_count}）"
            if visible_count != total_count:
                summary += f"，当前显示 {visible_count}"
            parts.append(summary)
        else:
            parts.append("当前没有持仓")

        filter_text = _format_position_filter_summary(
            self.position_type_filter.get(),
            self.position_keyword.get(),
        )
        if filter_text:
            parts.append(f"筛选：{filter_text}")
        if self.position_auto_refresh_enabled:
            parts.append(f"自动刷新：{self.position_refresh_interval_label.get()}")
        else:
            parts.append("自动刷新：已暂停")
        parts.append(f"最近刷新：{timestamp}")
        self.positions_summary_text.set(" | ".join(parts))

    def _update_position_metrics(self, visible_positions: list[OkxPosition]) -> None:
        metrics = _aggregate_position_metrics(visible_positions, self._upl_usdt_prices, self._position_instruments)
        visible_count = len(visible_positions)
        total_count = len(self._latest_positions)
        self.position_total_text.set(
            f"{visible_count} 笔" if visible_count == total_count else f"{visible_count} / {total_count} 笔"
        )
        self.position_upl_text.set(
            _format_optional_usdt(metrics["upl_usdt"] if isinstance(metrics["upl_usdt"], Decimal) else None)
        )
        self.position_realized_text.set(
            _format_optional_decimal_fixed(
                metrics["realized"] if isinstance(metrics["realized"], Decimal) else None,
                places=2,
                with_sign=True,
            )
        )
        self.position_margin_text.set(
            _format_optional_integer(metrics["imr"] if isinstance(metrics["imr"], Decimal) else None)
        )
        self.position_delta_text.set(
            _format_summary_delta(metrics["delta"] if isinstance(metrics["delta"], Decimal) else None)
        )
        self.position_short_call_text.set(
            _format_filtered_option_position_size(
                visible_positions,
                self._position_instruments,
                option_kind="C",
                direction="short",
            )
        )
        self.position_short_put_text.set(
            _format_filtered_option_position_size(
                visible_positions,
                self._position_instruments,
                option_kind="P",
                direction="short",
            )
        )
        self.position_long_call_text.set(
            _format_filtered_option_position_size(
                visible_positions,
                self._position_instruments,
                option_kind="C",
                direction="long",
            )
        )
        self.position_long_put_text.set(
            _format_filtered_option_position_size(
                visible_positions,
                self._position_instruments,
                option_kind="P",
                direction="long",
            )
        )

    def _selected_position_payload(self) -> dict[str, object] | None:
        selection = self.position_tree.selection()
        if not selection:
            return None
        return self._position_row_payloads.get(selection[0])

    def _on_position_selected(self, *_: object) -> None:
        if self._position_selection_syncing or self._positions_view_rendering:
            return
        selection = self.position_tree.selection()
        if selection and self._position_selection_suppressed_item_id == selection[0]:
            self._position_selection_suppressed_item_id = None
            return
        self._position_selection_suppressed_item_id = None
        self._refresh_position_detail_panel()

    def _refresh_position_detail_panel(self) -> None:
        payload = self._selected_position_payload()
        if payload is None:
            self.position_detail_text.set(self._default_position_detail_text())
        elif payload["kind"] == "position":
            position = payload["item"]
            if isinstance(position, OkxPosition):
                self.position_detail_text.set(
                    _build_position_detail_text(
                        position,
                        self._upl_usdt_prices,
                        self._position_instruments,
                        note=self._current_position_note_text(position),
                    )
                )
        else:
            label = payload["label"]
            positions = payload["item"]
            metrics = payload["metrics"]
            if isinstance(label, str) and isinstance(positions, list) and isinstance(metrics, dict):
                self.position_detail_text.set(
                    _build_group_detail_text(
                        label,
                        positions,
                        metrics,
                        self._upl_usdt_prices,
                        self._position_instruments,
                    )
                )
        self._set_readonly_text(self._position_detail_panel, self.position_detail_text.get())
        if self._positions_zoom_tree is not None:
            selection = self.position_tree.selection()
            if selection and self._positions_zoom_tree.exists(selection[0]):
                self._sync_positions_zoom_selection(selection[0])
        self._refresh_positions_zoom_detail()
        self._refresh_protection_window_view()

    def _apply_positions_error(self, message: str) -> None:
        friendly_message = _format_network_error_message(message)
        _mark_refresh_health_failure(self._positions_refresh_health, friendly_message)
        self._refresh_all_refresh_badges()
        suffix = _format_refresh_health_suffix(self._positions_refresh_health)
        has_previous_positions = (
            bool(self._latest_positions)
            or bool(self._position_row_payloads)
            or self._positions_last_refresh_at is not None
        )
        self._positions_refreshing = False
        if has_previous_positions:
            summary = f"持仓刷新失败，继续显示上一份缓存：{friendly_message}{suffix}"
            self.positions_summary_text.set(summary)
            self._sync_positions_zoom_window()
            self._refresh_positions_zoom_detail()
            self._enqueue_log(summary)
            return
        self._latest_positions = []
        self._positions_context_note = None
        self._positions_context_profile_name = None
        self._positions_last_refresh_at = None
        self._positions_effective_environment = None
        self._upl_usdt_prices = {}
        self._position_instruments = {}
        self._position_tickers = {}
        self.position_tree.delete(*self.position_tree.get_children())
        self._position_row_payloads.clear()
        self.position_total_text.set("-")
        self.position_upl_text.set("-")
        self.position_realized_text.set("-")
        self.position_margin_text.set("-")
        self.position_delta_text.set("-")
        self.position_short_call_text.set("-")
        self.position_short_put_text.set("-")
        self.position_long_call_text.set("-")
        self.position_long_put_text.set("-")
        self.position_detail_text.set(self._default_position_detail_text())
        self._set_readonly_text(self._position_detail_panel, self.position_detail_text.get())
        self._sync_positions_zoom_window()
        self._refresh_positions_zoom_detail()
        summary = f"持仓读取失败：{friendly_message}{suffix}"
        self.positions_summary_text.set(summary)
        self._enqueue_log(summary)

    def _refresh_positions_periodic(self) -> None:
        if self.position_auto_refresh_enabled:
            self.refresh_positions()
        self.root.after(self._position_refresh_interval_ms(), self._refresh_positions_periodic)

    def _on_strategy_selected(self, *_: object) -> None:
        self._apply_selected_strategy_definition()

    def _apply_selected_strategy_definition(self) -> None:
        definition = self._selected_strategy_definition()
        self.signal_combo["values"] = definition.allowed_signal_labels
        if self.signal_mode_label.get() not in definition.allowed_signal_labels:
            self.signal_mode_label.set(definition.default_signal_label)
        if is_dynamic_strategy_id(definition.strategy_id):
            self._take_profit_mode_label.grid()
            self._take_profit_mode_combo.grid()
            self._max_entries_per_trend_label.grid()
            self._max_entries_per_trend_entry.grid()
            self._startup_chase_window_label.grid()
            self._startup_chase_window_entry.grid()
            self._startup_chase_window_hint_label.grid()
            self._dynamic_two_r_break_even_check.grid()
            self._dynamic_fee_offset_check.grid()
            self._dynamic_fee_offset_hint_label.grid()
            self._time_stop_break_even_check.grid()
            self._time_stop_break_even_bars_label.grid()
            self._time_stop_break_even_bars_entry.grid()
            self._entry_reference_ema_label.grid()
            self._entry_reference_ema_entry.grid()
        else:
            self._take_profit_mode_label.grid_remove()
            self._take_profit_mode_combo.grid_remove()
            self._max_entries_per_trend_label.grid_remove()
            self._max_entries_per_trend_entry.grid_remove()
            self._startup_chase_window_label.grid_remove()
            self._startup_chase_window_entry.grid_remove()
            self._startup_chase_window_hint_label.grid_remove()
            self._dynamic_two_r_break_even_check.grid_remove()
            self._dynamic_fee_offset_check.grid_remove()
            self._dynamic_fee_offset_hint_label.grid_remove()
            self._time_stop_break_even_check.grid_remove()
            self._time_stop_break_even_bars_label.grid_remove()
            self._time_stop_break_even_bars_entry.grid_remove()
            self._entry_reference_ema_label.grid_remove()
            self._entry_reference_ema_entry.grid_remove()
        if definition.strategy_id == STRATEGY_EMA5_EMA8_ID:
            self.bar.set("4H")
            self.ema_period.set("5")
            self.trend_ema_period.set("8")
            self.big_ema_period.set("233")
            self.entry_reference_ema_period.set("0")
            self.risk_amount.set("10")
            self.take_profit_mode_label.set("固定止盈")
            self.max_entries_per_trend.set("0")
            self.entry_side_mode_label.set("跟随信号")
            self.tp_sl_mode_label.set("按交易标的价格（本地）")
        if self._strategy_uses_big_ema(definition.strategy_id):
            self._big_ema_label.grid()
            self._big_ema_entry.grid()
        else:
            self._big_ema_label.grid_remove()
            self._big_ema_entry.grid_remove()
        if is_dynamic_strategy_id(definition.strategy_id) and not self.entry_reference_ema_period.get().strip():
            self.entry_reference_ema_period.set("55")
        if is_dynamic_strategy_id(definition.strategy_id) and not self.startup_chase_window_seconds.get().strip():
            self.startup_chase_window_seconds.set("0")
        self._sync_dynamic_take_profit_controls()
        QuantApp._sync_entry_side_mode_controls(self)
        self.strategy_summary_text.set(definition.summary)
        self.strategy_rule_text.set(definition.rule_description)
        self.strategy_hint_text.set(definition.parameter_hint)
        self._update_fixed_order_size_hint()

    def _sync_dynamic_take_profit_controls(self) -> None:
        if not hasattr(self, "_dynamic_two_r_break_even_check"):
            return
        definition = self._selected_strategy_definition()
        dynamic_strategy = is_dynamic_strategy_id(definition.strategy_id)
        dynamic_take_profit = (
            dynamic_strategy and TAKE_PROFIT_MODE_OPTIONS.get(self.take_profit_mode_label.get(), "fixed") == "dynamic"
        )
        self._dynamic_two_r_break_even_check.configure(state="normal" if dynamic_take_profit else "disabled")
        self._dynamic_fee_offset_check.configure(state="normal" if dynamic_take_profit else "disabled")
        self._time_stop_break_even_check.configure(state="normal" if dynamic_take_profit else "disabled")
        self._time_stop_break_even_bars_label.configure(state="normal" if dynamic_take_profit else "disabled")
        self._time_stop_break_even_bars_entry.configure(
            state="normal" if dynamic_take_profit and self.time_stop_break_even_enabled.get() else "disabled"
        )

    def _sync_entry_side_mode_controls(self) -> None:
        if not hasattr(self, "_entry_side_mode_combo"):
            return
        definition = self._selected_strategy_definition()
        run_mode = RUN_MODE_OPTIONS.get(self.run_mode_label.get(), "trade")
        tp_sl_mode = TP_SL_MODE_OPTIONS.get(self.tp_sl_mode_label.get(), "exchange")
        if supports_fixed_entry_side_mode(definition.strategy_id, run_mode, tp_sl_mode):
            self._entry_side_mode_combo.configure(values=list(ENTRY_SIDE_MODE_OPTIONS.keys()), state="readonly")
            self.entry_side_mode_hint_text.set("当前模式支持跟随信号、固定买入、固定卖出。")
            if self.entry_side_mode_label.get() not in ENTRY_SIDE_MODE_OPTIONS:
                self.entry_side_mode_label.set("跟随信号")
            return
        self._entry_side_mode_combo.configure(values=("跟随信号",), state="disabled")
        if self.entry_side_mode_label.get() != "跟随信号":
            self.entry_side_mode_label.set("跟随信号")
        self.entry_side_mode_hint_text.set(
            fixed_entry_side_mode_support_reason(definition.strategy_id, run_mode, tp_sl_mode) or "当前模式仅支持跟随信号。"
        )

    def _selected_strategy_definition(self) -> StrategyDefinition:
        strategy_id = self._strategy_name_to_id[self.strategy_name.get()]
        return get_strategy_definition(strategy_id)

    def _confirm_start(self, definition: StrategyDefinition, config: StrategyConfig) -> bool:
        strategy_symbol = self._format_strategy_symbol_display(config.inst_id, config.trade_inst_id)
        risk_value = self.risk_amount.get().strip() or "-"
        fixed_size = self.order_size.get().strip() or "-"
        lines = [
            f"策略：{definition.name}",
            f"运行模式：{self.run_mode_label.get()}",
            f"交易标的：{strategy_symbol}",
            f"K线周期：{config.bar}",
            f"信号方向：{self.signal_mode_label.get()}",
            f"下单方向模式：{self.entry_side_mode_label.get()}",
            f"止盈止损模式：{self.tp_sl_mode_label.get()}",
            f"自定义触发标的：{self.local_tp_sl_symbol.get().strip().upper() or '-'}",
            f"EMA小周期：{config.ema_period}",
            f"EMA中周期：{config.trend_ema_period}",
        ]
        if is_dynamic_strategy_id(definition.strategy_id):
            lines.extend(
                [
                    f"挂单参考EMA：{config.entry_reference_ema_label()}",
                    f"止盈方式：{self.take_profit_mode_label.get()}",
                    f"每波最多开仓次数：{config.max_entries_per_trend if config.max_entries_per_trend > 0 else '不限'}",
                    f"启动追单窗口：{config.startup_chase_window_label()}",
                ]
            )
            if config.take_profit_mode == "dynamic":
                lines.append(f"2R保本开关：{config.dynamic_two_r_break_even_label()}")
                lines.append(f"手续费偏移开关：{config.dynamic_fee_offset_enabled_label()}")
                lines.append(
                    f"时间保本：{config.time_stop_break_even_enabled_label()} / {config.resolved_time_stop_break_even_bars()}根"
                )
        if self._strategy_uses_big_ema(definition.strategy_id):
            lines.append(f"EMA大周期：{config.big_ema_period}")
        lines.extend(
            [
                f"ATR 周期：{config.atr_period}",
                f"风险金：{risk_value}",
                f"固定数量：{fixed_size}",
                "",
                definition.rule_description,
                "",
                "确认启动这个策略吗？",
            ]
        )
        message = "\n".join(lines)
        return messagebox.askokcancel(f"确认启动 {definition.name}", message)

    def _collect_inputs(self, definition: StrategyDefinition) -> tuple[Credentials, StrategyConfig]:
        api_key = self.api_key.get().strip()
        secret_key = self.secret_key.get().strip()
        passphrase = self.passphrase.get().strip()
        symbol = _normalize_symbol_input(self.symbol.get())
        trade_symbol = symbol
        local_tp_sl_symbol = _normalize_symbol_input(self.local_tp_sl_symbol.get()) or None
        tp_sl_mode = TP_SL_MODE_OPTIONS[self.tp_sl_mode_label.get()]
        run_mode = RUN_MODE_OPTIONS[self.run_mode_label.get()]
        entry_side_mode = ENTRY_SIDE_MODE_OPTIONS[self.entry_side_mode_label.get()]
        if not supports_fixed_entry_side_mode(definition.strategy_id, run_mode, tp_sl_mode):
            entry_side_mode = "follow_signal"
        effective_signal_mode = resolve_dynamic_signal_mode(
            definition.strategy_id,
            SIGNAL_LABEL_TO_VALUE[self.signal_mode_label.get()],
        )
        risk_amount = self._parse_optional_positive_decimal(self.risk_amount.get(), "风险金")
        order_size = self._parse_optional_positive_decimal(self.order_size.get(), "固定数量") or Decimal("0")
        max_entries_per_trend = self._parse_nonnegative_int(self.max_entries_per_trend.get(), "每波最多开仓次数")
        startup_chase_window_seconds = 0
        entry_reference_ema_period = 0
        if is_dynamic_strategy_id(definition.strategy_id):
            entry_reference_ema_period = self._parse_nonnegative_int(self.entry_reference_ema_period.get(), "挂单参考EMA")
            startup_chase_window_seconds = self._parse_nonnegative_int(
                self.startup_chase_window_seconds.get(),
                "启动追单窗口(秒)",
            )

        if not api_key or not secret_key or not passphrase:
            raise ValueError("请先在 菜单 > 设置 > API 与通知设置 中填写 API 凭证")
        if not symbol:
            raise ValueError("请选择交易标的")
        if run_mode == "trade":
            if tp_sl_mode == "exchange":
                if trade_symbol != symbol:
                    raise ValueError("OKX 托管止盈止损只支持同一交易标的")
                if infer_inst_type(trade_symbol) != "SWAP":
                    raise ValueError("OKX 托管止盈止损当前只支持永续合约")
            if tp_sl_mode == "local_custom" and not local_tp_sl_symbol:
                raise ValueError("已选择自定义本地止盈止损，请填写触发标的")
            if risk_amount is None and order_size <= 0:
                raise ValueError("交易并下单模式下，风险金和固定数量至少填写一个")

        notification_config = self._collect_notification_config(validate_if_enabled=True)
        if run_mode == "signal_only":
            if not notification_config.enabled:
                raise ValueError("只发信号邮件模式需要先在设置里启用邮件通知")
            if not notification_config.notify_signals:
                raise ValueError("只发信号邮件模式需要勾选“信号邮件”")

        if definition.strategy_id == STRATEGY_EMA5_EMA8_ID:
            trade_symbol = symbol
            local_tp_sl_symbol = symbol
            tp_sl_mode = "local_trade"
            risk_amount = Decimal("10")
            order_size = Decimal("0")

        credentials = Credentials(api_key=api_key, secret_key=secret_key, passphrase=passphrase)
        config = StrategyConfig(
            inst_id=symbol,
            bar="4H" if definition.strategy_id == STRATEGY_EMA5_EMA8_ID else self.bar.get(),
            ema_period=5 if definition.strategy_id == STRATEGY_EMA5_EMA8_ID else self._parse_positive_int(self.ema_period.get(), "EMA小周期"),
            trend_ema_period=8 if definition.strategy_id == STRATEGY_EMA5_EMA8_ID else self._parse_positive_int(self.trend_ema_period.get(), "EMA中周期"),
            big_ema_period=233 if definition.strategy_id == STRATEGY_EMA5_EMA8_ID else self._parse_positive_int(self.big_ema_period.get(), "EMA大周期"),
            entry_reference_ema_period=entry_reference_ema_period,
            atr_period=self._parse_positive_int(self.atr_period.get(), "ATR 周期"),
            atr_stop_multiplier=self._parse_positive_decimal(self.stop_atr.get(), "止损 ATR 倍数"),
            atr_take_multiplier=self._parse_positive_decimal(self.take_atr.get(), "止盈 ATR 倍数"),
            order_size=order_size,
            trade_mode=TRADE_MODE_OPTIONS[self.trade_mode_label.get()],
            signal_mode=effective_signal_mode,
            position_mode=POSITION_MODE_OPTIONS[self.position_mode_label.get()],
            environment=ENV_OPTIONS[self.environment_label.get()],
            tp_sl_trigger_type=TRIGGER_TYPE_OPTIONS[self.trigger_type_label.get()],
            strategy_id=definition.strategy_id,
            poll_seconds=float(self._parse_positive_decimal(self.poll_seconds.get(), "轮询秒数")),
            risk_amount=risk_amount,
            trade_inst_id=trade_symbol,
            tp_sl_mode=tp_sl_mode,
            local_tp_sl_inst_id=local_tp_sl_symbol,
            entry_side_mode=entry_side_mode,
            run_mode=run_mode,
            take_profit_mode=TAKE_PROFIT_MODE_OPTIONS[self.take_profit_mode_label.get()],
            max_entries_per_trend=max_entries_per_trend,
            startup_chase_window_seconds=startup_chase_window_seconds if is_dynamic_strategy_id(definition.strategy_id) else 0,
            dynamic_two_r_break_even=self.dynamic_two_r_break_even.get()
            if is_dynamic_strategy_id(definition.strategy_id)
            else False,
            dynamic_fee_offset_enabled=self.dynamic_fee_offset_enabled.get()
            if is_dynamic_strategy_id(definition.strategy_id)
            else False,
            time_stop_break_even_enabled=self.time_stop_break_even_enabled.get()
            if is_dynamic_strategy_id(definition.strategy_id)
            else False,
            time_stop_break_even_bars=(
                self._parse_positive_int(self.time_stop_break_even_bars.get(), "时间保本K线数")
                if is_dynamic_strategy_id(definition.strategy_id) and self.time_stop_break_even_enabled.get()
                else 0
            ),
        )
        return credentials, config

    def _collect_notification_config(self, *, validate_if_enabled: bool) -> EmailNotificationConfig:
        smtp_port = self._parse_optional_port(self.smtp_port.get())
        recipients = tuple(self._split_recipients(self.recipient_emails.get()))
        config = EmailNotificationConfig(
            enabled=self.notify_enabled.get(),
            smtp_host=self.smtp_host.get().strip(),
            smtp_port=smtp_port,
            smtp_username=self.smtp_username.get().strip(),
            smtp_password=self.smtp_password.get(),
            sender_email=self.sender_email.get().strip(),
            recipient_emails=recipients,
            use_ssl=self.use_ssl.get(),
            notify_trade_fills=self.notify_trade_fills.get(),
            notify_signals=self.notify_signals.get(),
            notify_errors=self.notify_errors.get(),
        )
        if validate_if_enabled and config.enabled:
            if not config.smtp_host:
                raise ValueError("已启用邮件通知，请填写 SMTP 主机")
            if not recipients:
                raise ValueError("已启用邮件通知，请填写至少一个收件邮箱")
            if not (config.sender_email or config.smtp_username):
                raise ValueError("已启用邮件通知，请填写发件邮箱或 SMTP 用户名")
        return config

    def _build_notifier(self, config: StrategyConfig) -> EmailNotifier | None:
        notification_config = self._collect_notification_config(validate_if_enabled=True)
        if not notification_config.enabled:
            return None
        return EmailNotifier(
            notification_config,
            logger=self._make_system_logger(f"邮件 {config.strategy_id}"),
        )

    def _build_signal_monitor_notifier(self) -> EmailNotifier | None:
        notification_config = self._collect_notification_config(validate_if_enabled=True)
        if not notification_config.enabled:
            return None
        return EmailNotifier(notification_config, logger=self._make_system_logger("邮件 信号监控"))

    def send_test_email(self) -> None:
        try:
            notifier = self._build_signal_monitor_notifier()
        except Exception as exc:
            messagebox.showerror("测试邮件失败", str(exc), parent=self._settings_window or self.root)
            return

        if notifier is None:
            messagebox.showinfo("提示", "当前未启用邮件通知。", parent=self._settings_window or self.root)
            return

        subject = f"[QQOKX] 测试邮件 | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        body = "\n".join(
            [
                "这是一封来自 QQOKX 的测试邮件。",
                f"当前环境：{self.environment_label.get()}",
                f"发送时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            ]
        )
        notifier.notify_async(subject, body)
        self._enqueue_log("已提交测试邮件发送请求。")
        messagebox.showinfo("提示", "测试邮件已提交，请检查收件箱。", parent=self._settings_window or self.root)

    def _update_settings_summary(self) -> None:
        api_status = "已配置" if all(
            [self.api_key.get().strip(), self.secret_key.get().strip(), self.passphrase.get().strip()]
        ) else "未配置"
        mail_status = "邮件已启用" if self.notify_enabled.get() else "邮件未启用"
        self.settings_summary_text.set(
            f"{api_status} | {mail_status} | {self.environment_label.get()} | {self.trade_mode_label.get()} | "
            f"{self.position_mode_label.get()}"
        )

    def _append_logged_message(
        self,
        message: str,
        *,
        extra_log_path: Path | None = None,
        extra_log_owner: str = "",
    ) -> None:
        line = append_log_line(message)
        if extra_log_path is not None:
            try:
                append_preformatted_log_line(line, path=extra_log_path)
            except Exception as exc:
                failure_key = str(extra_log_path)
                if failure_key not in self._strategy_log_write_failures:
                    self._strategy_log_write_failures.add(failure_key)
                    owner_prefix = f"{extra_log_owner} " if extra_log_owner else ""
                    self._enqueue_log(f"{owner_prefix}独立日志写入失败：{exc}")
        self.log_queue.put(line)

    def _log_session_message(self, session: StrategySession, message: str) -> None:
        self._record_session_runtime_message(session.session_id, message)
        self._append_logged_message(
            f"{session.log_prefix} {message}",
            extra_log_path=session.log_file_path,
            extra_log_owner=session.log_prefix,
        )

    def _make_session_logger(
        self,
        session_id: str,
        strategy_name: str,
        symbol: str,
        api_name: str = "",
        log_file_path: Path | None = None,
    ):
        prefix = f"[{api_name}] [{session_id} {strategy_name} {symbol}]" if api_name else f"[{session_id} {strategy_name} {symbol}]"

        def _logger(message: str) -> None:
            self._record_session_runtime_message(session_id, message)
            self._append_logged_message(
                f"{prefix} {message}",
                extra_log_path=log_file_path,
                extra_log_owner=prefix,
            )

        return _logger

    def _make_system_logger(self, name: str):
        prefix = f"[{name}]"

        def _logger(message: str) -> None:
            self._enqueue_log(f"{prefix} {message}")

        return _logger

    def _create_session_engine(
        self,
        *,
        strategy_id: str,
        strategy_name: str,
        session_id: str,
        symbol: str,
        api_name: str,
        log_file_path: Path | None,
        notifier: EmailNotifier | None,
        direction_label: str,
        run_mode_label: str,
        trader_id: str = "",
    ) -> StrategyEngine:
        session_logger = self._make_session_logger(
            session_id,
            strategy_name,
            symbol,
            api_name,
            log_file_path,
        )
        return StrategyEngine(
            self.client,
            session_logger,
            notifier=notifier,
            strategy_name=strategy_name,
            session_id=session_id,
            direction_label=direction_label,
            run_mode_label=run_mode_label,
            trader_id=trader_id,
        )

    def _next_session_id(self) -> str:
        while True:
            self._session_counter += 1
            session_id = f"S{self._session_counter:02d}"
            if session_id not in self.sessions:
                return session_id

    def _record_session_runtime_message(self, session_id: str, message: str) -> None:
        session = self.sessions.get(session_id)
        if session is None:
            return
        text = str(message or "").strip()
        if not text:
            return
        session.last_message = text
        inferred = _infer_session_runtime_status(text, session.runtime_status)
        if inferred:
            session.runtime_status = inferred
        self._track_session_trade_runtime(session, text)

    def _ensure_session_trade_runtime(
        self,
        session: StrategySession,
        *,
        observed_at: datetime,
        signal_bar_at: datetime | None = None,
    ) -> StrategyTradeRuntimeState:
        current = session.active_trade
        if current is None or current.reconciliation_started:
            current = StrategyTradeRuntimeState(
                round_id=f"{session.session_id}-{observed_at.strftime('%Y%m%d%H%M%S%f')}",
                signal_bar_at=signal_bar_at,
            )
            session.active_trade = current
        elif signal_bar_at is not None and current.signal_bar_at is None:
            current.signal_bar_at = signal_bar_at
        return current

    def _track_session_trade_runtime(self, session: StrategySession, message: str) -> None:
        observed_at = datetime.now()
        signal_bar_at = _extract_session_bar_time(message)
        if "挂单已提交到 OKX" in message:
            trade = self._ensure_session_trade_runtime(session, observed_at=observed_at, signal_bar_at=signal_bar_at)
            entry_order_id = _extract_log_field(message, "ordId")
            if entry_order_id:
                trade.entry_order_id = entry_order_id
            return
        if "委托追踪" in message:
            trade = self._ensure_session_trade_runtime(session, observed_at=observed_at, signal_bar_at=signal_bar_at)
            client_order_id = _extract_log_field(message, "clOrdId")
            if client_order_id:
                trade.entry_client_order_id = client_order_id
            return
        if "挂单已成交" in message:
            trade = self._ensure_session_trade_runtime(session, observed_at=observed_at, signal_bar_at=signal_bar_at)
            trade.opened_logged_at = observed_at
            entry_order_id = _extract_log_field(message, "ordId")
            if entry_order_id:
                trade.entry_order_id = entry_order_id
            entry_price = _extract_log_field_decimal(message, "开仓价")
            if entry_price is not None:
                trade.entry_price = entry_price
            size = _extract_log_field_decimal(message, "数量")
            if size is not None:
                trade.size = size
            QuantApp._trader_desk_sync_open_trade_state(self, session)
            return
        if "交易员虚拟止损监控启动" in message:
            trade = session.active_trade
            if trade is None:
                return
            stop_price = _extract_log_field_decimal(message, "策略止损")
            if stop_price is not None:
                trade.current_stop_price = stop_price
            return
        if "交易员动态止盈保护价已上移" in message:
            trade = session.active_trade
            if trade is None:
                return
            new_stop = _extract_log_field_decimal(message, "新保护价") or _extract_log_field_decimal(message, "保护价")
            if new_stop is not None:
                trade.current_stop_price = new_stop
            return
        if "交易员虚拟止损已触发（不平仓）" in message:
            trade = session.active_trade
            if trade is None:
                return
            stop_price = _extract_log_field_decimal(message, "策略止损")
            if stop_price is not None:
                trade.current_stop_price = stop_price
            return
        if "初始 OKX 止损已提交" in message:
            trade = session.active_trade
            if trade is None:
                return
            algo_cl_ord_id = _extract_log_field(message, "algoClOrdId")
            if algo_cl_ord_id:
                trade.protective_algo_cl_ord_id = algo_cl_ord_id
            stop_price = _extract_log_field_decimal(message, "止损")
            if stop_price is not None:
                trade.current_stop_price = stop_price
            return
        if "OKX 动态止损已上移" in message:
            trade = session.active_trade
            if trade is None:
                return
            new_stop = _extract_log_field_decimal(message, "新止损") or _extract_log_field_decimal(message, "止损")
            if new_stop is not None:
                trade.current_stop_price = new_stop
            return
        if "本轮持仓已结束，继续监控下一次信号" in message:
            trade = session.active_trade
            if trade is None or trade.reconciliation_started:
                return
            trade.reconciliation_started = True
            self._start_session_trade_reconciliation(session, trade)

    def _trader_desk_sync_open_trade_state(self, session: StrategySession) -> None:
        trader_id = getattr(session, "trader_id", "").strip()
        trader_slot_id = getattr(session, "trader_slot_id", "").strip()
        if not trader_id or not trader_slot_id:
            return
        slot = self._trader_desk_slot_for_session(session.session_id)
        trade = session.active_trade
        if slot is None or trade is None or trade.opened_logged_at is None:
            return
        changed = False
        if slot.status == "watching":
            slot.status = "open"
            slot.quota_occupied = True
            slot.opened_at = trade.opened_logged_at or datetime.now()
            changed = True
        if slot.entry_price is None and trade.entry_price is not None:
            slot.entry_price = trade.entry_price
            changed = True
        if slot.size is None and trade.size is not None:
            slot.size = trade.size
            changed = True
        if not changed:
            return
        run = self._trader_desk_run_by_id(trader_id, create=True)
        if run is not None and run.armed_session_id == session.session_id:
            run.armed_session_id = ""
            run.last_event_at = datetime.now()
            run.updated_at = datetime.now()
        self._trader_desk_add_event(
            trader_id,
            f"额度格已开仓 | 会话={session.session_id} | 开仓价={_format_optional_decimal(slot.entry_price)} | 数量={_format_optional_decimal(slot.size)}",
        )
        self._save_trader_desk_snapshot()
        self._ensure_trader_watcher(trader_id)

    def _start_session_trade_reconciliation(
        self,
        session: StrategySession,
        trade: StrategyTradeRuntimeState,
    ) -> None:
        credentials = self._credentials_for_profile_or_none(session.api_name)
        if credentials is None:
            self._log_session_message(
                session,
                "检测到仓位已关闭，但未找到该会话对应的 API 凭证，无法自动归因与结算。",
            )
            if session.active_trade is not None and session.active_trade.round_id == trade.round_id:
                session.active_trade = None
            return
        self._log_session_message(
            session,
            f"检测到仓位已关闭，开始归因 | 最近保护单={trade.protective_algo_cl_ord_id or '-'}",
        )
        trade_snapshot = StrategyTradeRuntimeState(
            round_id=trade.round_id,
            signal_bar_at=trade.signal_bar_at,
            opened_logged_at=trade.opened_logged_at,
            entry_order_id=trade.entry_order_id,
            entry_client_order_id=trade.entry_client_order_id,
            entry_price=trade.entry_price,
            size=trade.size,
            protective_algo_id=trade.protective_algo_id,
            protective_algo_cl_ord_id=trade.protective_algo_cl_ord_id,
            current_stop_price=trade.current_stop_price,
            reconciliation_started=True,
        )
        threading.Thread(
            target=self._reconcile_session_trade_worker,
            args=(session.session_id, trade_snapshot, credentials),
            daemon=True,
        ).start()

    def _load_strategy_trade_reconciliation_snapshot(
        self,
        session: StrategySession,
        credentials: Credentials,
        environment: str,
    ) -> StrategyTradeReconciliationSnapshot:
        trade_inst_id = (session.config.trade_inst_id or session.config.inst_id or session.symbol).strip().upper()
        inst_type = infer_inst_type(trade_inst_id) if trade_inst_id else "SWAP"
        inst_types = (inst_type,)
        return StrategyTradeReconciliationSnapshot(
            effective_environment=environment,
            order_history=self.client.get_order_history(credentials, environment=environment, inst_types=inst_types, limit=400),
            fills=self.client.get_fills_history(credentials, environment=environment, inst_types=inst_types, limit=400),
            position_history=self.client.get_positions_history(
                credentials,
                environment=environment,
                inst_types=inst_types if inst_type != "SPOT" else ("SPOT",),
                limit=200,
            ),
            account_bills=self.client.get_account_bills_history(
                credentials,
                environment=environment,
                inst_types=inst_types,
                limit=300,
            ),
        )

    def _load_strategy_trade_reconciliation_snapshot_with_fallback(
        self,
        session: StrategySession,
        credentials: Credentials,
    ) -> StrategyTradeReconciliationSnapshot:
        environment = session.config.environment
        try:
            return self._load_strategy_trade_reconciliation_snapshot(session, credentials, environment)
        except Exception as exc:
            message = str(exc)
            if "50101" not in message or "current environment" not in message:
                raise
            alternate = "live" if environment == "demo" else "demo"
            snapshot = self._load_strategy_trade_reconciliation_snapshot(session, credentials, alternate)
            snapshot.environment_note = f"本轮归因自动切换到 {'实盘' if alternate == 'live' else '模拟'} 环境读取。"
            return snapshot

    @staticmethod
    def _is_funding_fee_bill(item: OkxAccountBillItem) -> bool:
        if (item.bill_sub_type or "").strip() in FUNDING_FEE_BILL_SUBTYPES:
            return True
        text = " ".join(
            value.strip().lower()
            for value in (item.bill_type or "", item.bill_sub_type or "", item.business_type or "", item.event_type or "")
            if value
        )
        return any(marker in text for marker in FUNDING_FEE_BILL_MARKERS)

    def _build_strategy_trade_reconciliation_result(
        self,
        session: StrategySession,
        trade: StrategyTradeRuntimeState,
        snapshot: StrategyTradeReconciliationSnapshot,
    ) -> StrategyTradeReconciliationResult:
        trade_inst_id = (session.config.trade_inst_id or session.config.inst_id or session.symbol).strip().upper()
        open_anchor = trade.opened_logged_at or datetime.now()
        open_ms = int((open_anchor - timedelta(minutes=2)).timestamp() * 1000)

        session_orders = [
            item
            for item in snapshot.order_history
            if _trade_order_belongs_to_session(item, session)
            and (not trade_inst_id or item.inst_id.strip().upper() == trade_inst_id)
        ]
        recent_orders = [item for item in session_orders if _trade_order_event_time(item) >= open_ms]
        if not recent_orders:
            recent_orders = session_orders

        entry_order = next(
            (
                item
                for item in recent_orders
                if trade.entry_order_id and (item.order_id or "").strip() == trade.entry_order_id
            ),
            None,
        )
        if entry_order is None:
            entry_order = next(
                (
                    item
                    for item in recent_orders
                    if trade.entry_client_order_id and (item.client_order_id or "").strip() == trade.entry_client_order_id
                ),
                None,
            )
        if entry_order is None:
            entry_candidates = [item for item in recent_orders if _trade_order_session_role(item, session) == "ent"]
            entry_candidates.sort(key=_trade_order_event_time)
            entry_order = next(
                (
                    item
                    for item in entry_candidates
                    if (item.filled_size or Decimal("0")) > 0 or (item.state or "").strip().lower() == "filled"
                ),
                entry_candidates[0] if entry_candidates else None,
            )

        protective_orders = [item for item in recent_orders if _trade_order_session_role(item, session) == "slg"]
        protective_orders.sort(
            key=lambda item: (
                0
                if trade.protective_algo_cl_ord_id
                and (item.algo_client_order_id or "").strip() == trade.protective_algo_cl_ord_id
                else 1,
                -_trade_order_event_time(item),
            )
        )
        protective_order = protective_orders[0] if protective_orders else None

        exit_orders = [item for item in recent_orders if _trade_order_session_role(item, session) == "exi"]
        exit_orders.sort(key=_trade_order_event_time, reverse=True)
        filled_exit_order = next(
            (
                item
                for item in exit_orders
                if (item.filled_size or Decimal("0")) > 0
                or (item.actual_size or Decimal("0")) > 0
                or (item.state or "").strip().lower() == "filled"
            ),
            None,
        )

        relevant_fills = [
            item
            for item in snapshot.fills
            if (not trade_inst_id or item.inst_id.strip().upper() == trade_inst_id)
            and (item.fill_time or 0) >= open_ms
        ]
        entry_order_ids = {
            value
            for value in (
                trade.entry_order_id,
                entry_order.order_id if entry_order is not None else "",
            )
            if value
        }
        close_order_ids = {
            value
            for value in (
                filled_exit_order.order_id if filled_exit_order is not None else "",
                protective_order.order_id if protective_order is not None else "",
            )
            if value
        }
        entry_fills = [item for item in relevant_fills if (item.order_id or "") in entry_order_ids]
        close_fills = [item for item in relevant_fills if (item.order_id or "") in close_order_ids]

        relevant_position_history = [
            item
            for item in snapshot.position_history
            if (not trade_inst_id or item.inst_id.strip().upper() == trade_inst_id)
            and (item.update_time or 0) >= open_ms
        ]
        relevant_position_history.sort(key=lambda item: item.update_time or 0, reverse=True)
        matched_position_history = relevant_position_history[0] if relevant_position_history else None

        close_reason = "持仓已关闭（原因待确认）"
        reason_confidence = "low"
        close_order = filled_exit_order
        protective_executed = False
        if protective_order is not None:
            protective_executed = (
                (protective_order.actual_size or Decimal("0")) > 0
                or (protective_order.filled_size or Decimal("0")) > 0
                or protective_order.actual_price is not None
                or (
                    protective_order.order_id is not None
                    and any((item.order_id or "") == protective_order.order_id for item in close_fills)
                )
            )
        if protective_executed:
            close_reason = "OKX止损触发"
            reason_confidence = "high"
            close_order = protective_order
        elif filled_exit_order is not None:
            close_reason = "策略主动平仓"
            reason_confidence = "high"
        elif close_fills:
            close_reason = "外部成交平仓"
            reason_confidence = "medium"
        elif matched_position_history is not None:
            close_reason = "持仓已关闭（原因待确认）"
            reason_confidence = "medium"

        entry_price = (
            _weighted_average_fill_price(entry_fills)
            or trade.entry_price
            or (entry_order.avg_price if entry_order is not None else None)
            or (entry_order.actual_price if entry_order is not None else None)
            or (entry_order.price if entry_order is not None else None)
        )
        size = (
            _sum_fill_size(entry_fills)
            or trade.size
            or (entry_order.filled_size if entry_order is not None else None)
            or (entry_order.actual_size if entry_order is not None else None)
            or (entry_order.size if entry_order is not None else None)
        )
        exit_price = (
            _weighted_average_fill_price(close_fills)
            or (close_order.actual_price if close_order is not None else None)
            or (close_order.avg_price if close_order is not None else None)
            or (close_order.price if close_order is not None else None)
            or (matched_position_history.close_avg_price if matched_position_history is not None else None)
            or trade.current_stop_price
        )
        entry_fee = _sum_fill_fee(entry_fills)
        if entry_fee is None and entry_order is not None:
            entry_fee = entry_order.fee
        exit_fee = _sum_fill_fee(close_fills)
        if exit_fee is None and close_order is not None:
            exit_fee = close_order.fee
        gross_pnl = _sum_fill_pnl(close_fills)
        if gross_pnl is None and close_order is not None:
            gross_pnl = close_order.pnl
        if gross_pnl is None and matched_position_history is not None:
            gross_pnl = matched_position_history.pnl

        close_time_ms = max(
            [item.fill_time or 0 for item in close_fills]
            + [
                _trade_order_event_time(close_order) if close_order is not None else 0,
                matched_position_history.update_time if matched_position_history is not None else 0,
            ]
        )
        if close_time_ms > 0:
            closed_at = datetime.fromtimestamp(close_time_ms / 1000)
        else:
            closed_at = datetime.now()

        funding_fee = None
        if snapshot.account_bills:
            funding_total = Decimal("0")
            funding_seen = False
            close_window_ms = int((closed_at + timedelta(minutes=2)).timestamp() * 1000)
            for bill in snapshot.account_bills:
                if trade_inst_id and bill.inst_id.strip().upper() != trade_inst_id:
                    continue
                if (bill.bill_time or 0) < open_ms or (bill.bill_time or 0) > close_window_ms:
                    continue
                if not self._is_funding_fee_bill(bill):
                    continue
                amount = bill.amount
                if amount is None:
                    amount = bill.pnl if bill.pnl is not None else bill.balance_change
                if amount is None:
                    continue
                funding_total += amount
                funding_seen = True
            if funding_seen:
                funding_fee = funding_total

        net_pnl = None
        if gross_pnl is not None:
            net_pnl = gross_pnl + (entry_fee or Decimal("0")) + (exit_fee or Decimal("0")) + (funding_fee or Decimal("0"))
        elif matched_position_history is not None and matched_position_history.realized_pnl is not None:
            net_pnl = matched_position_history.realized_pnl

        ledger_record = StrategyTradeLedgerRecord(
            record_id=self._next_strategy_trade_ledger_record_id(session, closed_at),
            history_record_id=session.history_record_id or "",
            session_id=session.session_id,
            api_name=session.api_name,
            strategy_id=session.strategy_id,
            strategy_name=session.strategy_name,
            symbol=trade_inst_id or session.symbol,
            direction_label=session.direction_label,
            run_mode_label=session.run_mode_label,
            environment=snapshot.effective_environment,
            signal_bar_at=trade.signal_bar_at,
            opened_at=trade.opened_logged_at,
            closed_at=closed_at,
            entry_order_id=trade.entry_order_id or (entry_order.order_id if entry_order is not None else ""),
            entry_client_order_id=trade.entry_client_order_id,
            exit_order_id=close_order.order_id if close_order is not None and close_order.order_id is not None else "",
            protective_algo_id=protective_order.algo_id if protective_order is not None and protective_order.algo_id is not None else "",
            protective_algo_cl_ord_id=trade.protective_algo_cl_ord_id or (
                protective_order.algo_client_order_id if protective_order is not None else ""
            ),
            entry_price=entry_price,
            exit_price=exit_price,
            size=size,
            entry_fee=entry_fee,
            exit_fee=exit_fee,
            funding_fee=funding_fee,
            gross_pnl=gross_pnl,
            net_pnl=net_pnl,
            close_reason=close_reason,
            reason_confidence=reason_confidence,
            summary_note=snapshot.environment_note or "",
            updated_at=datetime.now(),
        )

        projected_trade_count = session.trade_count + 1
        projected_win_count = session.win_count + (1 if (net_pnl or Decimal("0")) > 0 else 0)
        projected_net_pnl = session.net_pnl_total + (net_pnl or Decimal("0"))
        win_rate_text = (
            _format_ratio(Decimal(projected_win_count) / Decimal(projected_trade_count), places=2)
            if projected_trade_count
            else "-"
        )
        attribution_summary = (
            f"本轮结束 | 原因={close_reason} | 开仓均价={_format_optional_decimal(entry_price)} | "
            f"平仓均价={_format_optional_decimal(exit_price)} | 数量={_format_optional_decimal(size)} | "
            f"开仓手续费={_format_optional_usdt_precise(entry_fee, places=2)} | "
            f"平仓手续费={_format_optional_usdt_precise(exit_fee, places=2)} | "
            f"资金费={_format_optional_usdt_precise(funding_fee, places=2)} | "
            f"毛盈亏={_format_optional_usdt_precise(gross_pnl, places=2)} | "
            f"净盈亏={_format_optional_usdt_precise(net_pnl, places=2)}"
        )
        cumulative_summary = (
            f"会话累计 | 交易次数={projected_trade_count} | 胜率={win_rate_text} | "
            f"累计净盈亏={_format_optional_usdt_precise(projected_net_pnl, places=2)}"
        )
        return StrategyTradeReconciliationResult(
            session_id=session.session_id,
            round_id=trade.round_id,
            ledger_record=ledger_record,
            environment_note=snapshot.environment_note,
            attribution_summary=attribution_summary,
            cumulative_summary=cumulative_summary,
        )

    def _reconcile_session_trade_worker(
        self,
        session_id: str,
        trade: StrategyTradeRuntimeState,
        credentials: Credentials,
    ) -> None:
        session = self.sessions.get(session_id)
        if session is None:
            return
        try:
            snapshot = self._load_strategy_trade_reconciliation_snapshot_with_fallback(session, credentials)
            result = self._build_strategy_trade_reconciliation_result(session, trade, snapshot)
        except Exception as exc:
            result = StrategyTradeReconciliationResult(
                session_id=session_id,
                round_id=trade.round_id,
                error_message=_format_network_error_message(str(exc)),
            )
        self.root.after(0, lambda: self._apply_strategy_trade_reconciliation_result(result))

    def _apply_strategy_trade_reconciliation_result(self, result: StrategyTradeReconciliationResult) -> None:
        session = self.sessions.get(result.session_id)
        if session is None:
            return
        if session.active_trade is not None and session.active_trade.round_id == result.round_id:
            session.active_trade = None
        if result.environment_note:
            self._log_session_message(session, result.environment_note)
        if result.error_message:
            self._log_session_message(session, f"本轮结束归因失败：{result.error_message}")
            return
        if result.ledger_record is None:
            return
        self._upsert_strategy_trade_ledger_record(result.ledger_record)
        self._refresh_session_financials_from_trade_ledger(session)
        session.last_close_reason = result.ledger_record.close_reason
        self._refresh_running_session_summary()
        self._log_session_message(session, result.attribution_summary)
        self._log_session_message(session, result.cumulative_summary)
        self._apply_trader_desk_reconciliation(session, result.ledger_record)

    def _apply_trader_desk_reconciliation(
        self,
        session: StrategySession,
        ledger_record: StrategyTradeLedgerRecord,
    ) -> None:
        trader_id = getattr(session, "trader_id", "").strip()
        trader_slot_id = getattr(session, "trader_slot_id", "").strip()
        if not trader_id or not trader_slot_id:
            return
        slot = self._trader_desk_slot_for_session(session.session_id)
        draft = self._trader_desk_draft_by_id(trader_id)
        run = self._trader_desk_run_by_id(trader_id, create=True)
        if slot is None or draft is None or run is None:
            return
        now = datetime.now()
        close_reason = ledger_record.close_reason or session.ended_reason or ""
        net_pnl = ledger_record.net_pnl or Decimal("0")
        is_profit = net_pnl > 0
        if "手动" in close_reason:
            slot.status = "closed_manual"
        else:
            slot.status = "closed_profit" if is_profit else "closed_loss"
        slot.quota_occupied = False
        slot.opened_at = slot.opened_at or ledger_record.opened_at
        slot.closed_at = ledger_record.closed_at
        slot.released_at = ledger_record.closed_at
        slot.entry_price = slot.entry_price if slot.entry_price is not None else ledger_record.entry_price
        slot.exit_price = ledger_record.exit_price
        slot.size = slot.size if slot.size is not None else ledger_record.size
        slot.net_pnl = net_pnl
        slot.close_reason = close_reason
        slot.history_record_id = ledger_record.history_record_id or session.history_record_id or ""
        run.armed_session_id = ""
        run.last_event_at = now
        run.updated_at = now
        if slot.status == "closed_loss" and draft.pause_on_stop_loss:
            run.status = "paused_loss"
            run.paused_reason = close_reason or "亏损后暂停"
            self._trader_desk_add_event(
                trader_id,
                f"亏损单已记录并暂停 | 会话={session.session_id} | 净盈亏={_format_optional_usdt_precise(net_pnl, places=2)} | 原因={close_reason or '-'}",
                level="warning",
            )
            self._save_trader_desk_snapshot()
            return
        if slot.status == "closed_profit":
            self._trader_desk_add_event(
                trader_id,
                f"盈利单已释放额度 | 会话={session.session_id} | 净盈亏={_format_optional_usdt_precise(net_pnl, places=2)}",
            )
            if not draft.auto_restart_on_profit:
                run.status = "idle"
                run.paused_reason = "盈利后等待人工继续。"
                self._save_trader_desk_snapshot()
                return
        else:
            self._trader_desk_add_event(
                trader_id,
                f"额度格已结束 | 会话={session.session_id} | 状态={slot.status} | 原因={close_reason or '-'}",
                level="warning" if slot.status == "closed_loss" else "info",
            )
        if run.status not in {"paused_manual", "paused_loss", "stopped"}:
            run.status = "running"
            run.paused_reason = ""
        self._save_trader_desk_snapshot()
        self._ensure_trader_watcher(trader_id)

    def _upsert_session_row(self, session: StrategySession) -> None:
        if not QuantApp._session_matches_running_filter(self, session):
            if self.session_tree.exists(session.session_id):
                self.session_tree.delete(session.session_id)
            return
        live_pnl, _ = self._session_live_pnl_snapshot(session)
        source_type = QuantApp._session_category_label(session)
        trader_label = QuantApp._session_trader_label(self, session)
        bar_label = str(getattr(getattr(session, "config", None), "bar", "") or "").strip() or "-"
        tags = ("duplicate_conflict",) if QuantApp._session_has_duplicate_launch_conflict(self, session) else ()
        values = (
            session.session_id,
            trader_label,
            session.api_name or "-",
            source_type,
            session.strategy_name,
            session.symbol,
            bar_label,
            session.direction_label,
            session.run_mode_label,
            _format_optional_usdt_precise(live_pnl, places=2),
            _format_optional_usdt_precise(session.net_pnl_total, places=2),
            session.display_status,
            session.started_at.strftime("%H:%M:%S"),
        )
        if self.session_tree.exists(session.session_id):
            self.session_tree.item(session.session_id, values=values, tags=tags)
        else:
            self.session_tree.insert("", END, iid=session.session_id, values=values, tags=tags)

    def _selected_session(self) -> StrategySession | None:
        selected = self.session_tree.selection()
        if not selected:
            return None
        return self.sessions.get(selected[0])

    def _on_session_selected(self, *_: object) -> None:
        self._refresh_selected_session_details()

    def _refresh_selected_session_details(self) -> None:
        session = self._selected_session()
        if session is None:
            self.selected_session_text.set(self._default_selected_session_text())
            self._set_readonly_text(self._selected_session_detail, self.selected_session_text.get())
            self._selected_session_detail_session_id = None
            return

        preserve_scroll = session.session_id == self._selected_session_detail_session_id
        live_pnl, live_pnl_refreshed_at = self._session_live_pnl_snapshot(session)
        duplicate_warning = QuantApp._build_duplicate_launch_conflict_warning(
            session,
            QuantApp._duplicate_launch_conflicts_for(self, session),
        )
        self.selected_session_text.set(
            self._build_strategy_detail_text(
                session_id=session.session_id,
                api_name=session.api_name,
                status=session.status,
                runtime_status=session.runtime_status,
                strategy_id=session.strategy_id,
                strategy_name=session.strategy_name,
                symbol=session.symbol,
                direction_label=session.direction_label,
                run_mode_label=session.run_mode_label,
                started_at=session.started_at,
                stopped_at=session.stopped_at,
                ended_reason=session.ended_reason,
                config_snapshot=_serialize_strategy_config_snapshot(session.config),
                log_file_path=session.log_file_path,
                last_message=session.last_message,
                trade_count=session.trade_count,
                win_count=session.win_count,
                gross_pnl_total=session.gross_pnl_total,
                fee_total=session.fee_total,
                funding_total=session.funding_total,
                net_pnl_total=session.net_pnl_total,
                last_close_reason=session.last_close_reason,
                live_pnl=live_pnl,
                live_pnl_refreshed_at=live_pnl_refreshed_at,
                duplicate_warning=duplicate_warning,
            )
        )
        self._set_readonly_text(
            self._selected_session_detail,
            self.selected_session_text.get(),
            preserve_scroll=preserve_scroll,
        )
        self._selected_session_detail_session_id = session.session_id

    @staticmethod
    def _snapshot_optional_text(snapshot: dict[str, object], key: str) -> str | None:
        value = snapshot.get(key)
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    @staticmethod
    def _snapshot_text(snapshot: dict[str, object], key: str, default: str = "-") -> str:
        value = QuantApp._snapshot_optional_text(snapshot, key)
        return value if value is not None else default

    @staticmethod
    def _snapshot_int(snapshot: dict[str, object], key: str, default: int = 0) -> int:
        value = snapshot.get(key, default)
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _bool_label(value: object) -> str:
        if isinstance(value, str):
            normalized = value.strip().lower()
            return "开启" if normalized in {"1", "true", "yes", "on", "开启"} else "关闭"
        return "开启" if bool(value) else "关闭"

    def _build_strategy_detail_text(
        self,
        *,
        session_id: str,
        api_name: str,
        status: str,
        strategy_id: str,
        strategy_name: str,
        symbol: str,
        direction_label: str,
        run_mode_label: str,
        started_at: datetime,
        stopped_at: datetime | None,
        ended_reason: str,
        config_snapshot: dict[str, object],
        log_file_path: str | Path | None = None,
        record_id: str | None = None,
        updated_at: datetime | None = None,
        runtime_status: str | None = None,
        last_message: str = "",
        trade_count: int = 0,
        win_count: int = 0,
        gross_pnl_total: Decimal = Decimal("0"),
        fee_total: Decimal = Decimal("0"),
        funding_total: Decimal = Decimal("0"),
        net_pnl_total: Decimal = Decimal("0"),
        last_close_reason: str = "",
        live_pnl: Decimal | None = None,
        live_pnl_refreshed_at: datetime | None = None,
        duplicate_warning: str = "",
    ) -> str:
        try:
            definition = get_strategy_definition(strategy_id)
            summary = definition.summary
            rule_description = definition.rule_description
            parameter_hint = definition.parameter_hint
        except KeyError:
            summary = "历史记录中的策略定义已不存在，保留原始参数供溯源。"
            rule_description = "-"
            parameter_hint = "-"
        snapshot = dict(config_snapshot or {})
        signal_inst_id = self._snapshot_optional_text(snapshot, "inst_id") or ""
        trade_inst_id = self._snapshot_optional_text(snapshot, "trade_inst_id")
        display_symbol = symbol or self._format_strategy_symbol_display(signal_inst_id, trade_inst_id)
        ema_period = self._snapshot_int(snapshot, "ema_period")
        trend_ema_period = self._snapshot_int(snapshot, "trend_ema_period")
        entry_reference_ema_period = self._snapshot_int(snapshot, "entry_reference_ema_period")
        lines = []
        if record_id:
            lines.append(f"记录ID：{record_id}")
        lines.extend(
            [
                f"会话：{session_id or '-'}",
                f"API配置：{api_name or '-'}",
                f"状态：{status or '-'}",
                f"独立日志：{_coerce_log_file_path(log_file_path) or '-'}",
            ]
        )
        if runtime_status and status == "运行中":
            lines.append(f"最近运行状态：{runtime_status}")
        if duplicate_warning:
            lines.append(duplicate_warning)
        if last_message:
            lines.append(f"最近日志：{last_message}")
        if ended_reason:
            lines.append(f"结束原因：{ended_reason}")
        lines.extend(
            [
                f"交易次数：{trade_count}",
                f"胜率：{_format_ratio(Decimal(win_count) / Decimal(trade_count), places=2) if trade_count else '-'}",
            ]
        )
        if record_id is None:
            live_pnl_text = _format_optional_usdt_precise(live_pnl, places=2)
            if live_pnl_refreshed_at is not None:
                live_pnl_text += f"（参考持仓 {live_pnl_refreshed_at.strftime('%H:%M:%S')}）"
            lines.append(f"实时浮盈亏：{live_pnl_text}")
        lines.extend(
            [
                f"毛盈亏：{_format_optional_usdt_precise(gross_pnl_total, places=2)}",
                f"手续费：{_format_optional_usdt_precise(fee_total, places=2)}",
                f"资金费：{_format_optional_usdt_precise(funding_total, places=2)}",
                f"净盈亏：{_format_optional_usdt_precise(net_pnl_total, places=2)}",
            ]
        )
        if last_close_reason:
            lines.append(f"最近结论：{last_close_reason}")
        lines.extend(
            [
                f"策略：{strategy_name}",
                f"运行模式：{run_mode_label or '-'}",
                f"交易标的：{display_symbol or '-'}",
                f"方向：{direction_label or '-'}",
                f"K线周期：{self._snapshot_text(snapshot, 'bar')}",
                f"EMA小周期：{ema_period or '-'}",
                f"EMA中周期：{trend_ema_period or '-'}",
            ]
        )
        if is_dynamic_strategy_id(strategy_id):
            if entry_reference_ema_period > 0:
                entry_reference_label = f"EMA{entry_reference_ema_period}"
            else:
                entry_reference_label = f"跟随EMA小周期(EMA{ema_period or '-'})"
            lines.append(f"挂单参考EMA：{entry_reference_label}")
            startup_window_seconds = self._snapshot_int(snapshot, "startup_chase_window_seconds") or 0
            lines.append(
                "启动追单窗口："
                + ("关闭（启动不追老信号）" if startup_window_seconds <= 0 else f"{startup_window_seconds}秒")
            )
            if self._snapshot_text(snapshot, "take_profit_mode", "dynamic") == "dynamic":
                lines.append(f"2R保本开关：{self._bool_label(snapshot.get('dynamic_two_r_break_even', True))}")
                lines.append(f"手续费偏移开关：{self._bool_label(snapshot.get('dynamic_fee_offset_enabled', True))}")
                lines.append(
                    "时间保本："
                    f"{self._bool_label(snapshot.get('time_stop_break_even_enabled', False))} / "
                    f"{self._snapshot_text(snapshot, 'time_stop_break_even_bars', '10')}根"
                )
        if self._strategy_uses_big_ema(strategy_id):
            lines.append(f"EMA大周期：{self._snapshot_int(snapshot, 'big_ema_period') or '-'}")
        lines.extend(
            [
                f"ATR 周期：{self._snapshot_text(snapshot, 'atr_period')}",
                f"止损 ATR 倍数：{self._snapshot_text(snapshot, 'atr_stop_multiplier')}",
                f"止盈 ATR 倍数：{self._snapshot_text(snapshot, 'atr_take_multiplier')}",
                f"风险金：{self._snapshot_text(snapshot, 'risk_amount')}",
                f"固定数量：{self._snapshot_text(snapshot, 'order_size')}",
                f"下单方向模式：{self._snapshot_text(snapshot, 'entry_side_mode')}",
                f"止盈止损模式：{self._snapshot_text(snapshot, 'tp_sl_mode')}",
                f"自定义触发标的：{self._snapshot_text(snapshot, 'local_tp_sl_inst_id')}",
                f"轮询秒数：{self._snapshot_text(snapshot, 'poll_seconds')}",
                f"启动时间：{_format_history_datetime(started_at)}",
            ]
        )
        if stopped_at is not None:
            lines.append(f"停止时间：{_format_history_datetime(stopped_at)}")
        if updated_at is not None:
            lines.append(f"最近更新：{_format_history_datetime(updated_at)}")
        lines.extend(
            [
                "",
                f"策略简介：{summary}",
                "",
                f"规则说明：{rule_description}",
                "",
                f"参数提示：{parameter_hint}",
            ]
        )
        return "\n".join(lines)

    def _recoverable_strategy_record_from_payload(
        self,
        payload: dict[str, object],
    ) -> RecoverableStrategySessionRecord | None:
        started_at = _parse_datetime_snapshot(payload.get("started_at"))
        if started_at is None:
            return None
        recovery_root_dir = _coerce_log_file_path(payload.get("recovery_root_dir"))
        if recovery_root_dir is None:
            return None
        config_snapshot = payload.get("config_snapshot")
        return RecoverableStrategySessionRecord(
            session_id=str(payload.get("session_id", "")).strip(),
            api_name=str(payload.get("api_name", "")).strip(),
            strategy_id=str(payload.get("strategy_id", "")).strip(),
            strategy_name=str(payload.get("strategy_name", "")).strip(),
            symbol=str(payload.get("symbol", "")).strip(),
            direction_label=str(payload.get("direction_label", "")).strip(),
            run_mode_label=str(payload.get("run_mode_label", "")).strip(),
            started_at=started_at,
            history_record_id=str(payload.get("history_record_id", "")).strip(),
            log_file_path=_coerce_log_file_path(payload.get("log_file_path")),
            recovery_root_dir=recovery_root_dir,
            config_snapshot=dict(config_snapshot) if isinstance(config_snapshot, dict) else {},
            updated_at=_parse_datetime_snapshot(payload.get("updated_at")),
        )

    @staticmethod
    def _recoverable_strategy_record_payload(record: RecoverableStrategySessionRecord) -> dict[str, object]:
        return {
            "session_id": record.session_id,
            "api_name": record.api_name,
            "strategy_id": record.strategy_id,
            "strategy_name": record.strategy_name,
            "symbol": record.symbol,
            "direction_label": record.direction_label,
            "run_mode_label": record.run_mode_label,
            "started_at": record.started_at.isoformat(timespec="seconds"),
            "history_record_id": record.history_record_id,
            "log_file_path": str(record.log_file_path) if record.log_file_path is not None else "",
            "recovery_root_dir": str(record.recovery_root_dir) if record.recovery_root_dir is not None else "",
            "config_snapshot": dict(record.config_snapshot),
            "updated_at": record.updated_at.isoformat(timespec="seconds") if record.updated_at is not None else None,
        }

    def _load_recoverable_strategy_sessions_registry(self) -> None:
        self._recoverable_strategy_sessions = {}

    def _save_recoverable_strategy_sessions_registry(self) -> None:
        return

    def _build_recoverable_strategy_session_record(
        self,
        session: StrategySession,
    ) -> RecoverableStrategySessionRecord | None:
        return None

    def _upsert_recoverable_strategy_session(self, session: StrategySession) -> None:
        return

    def _remove_recoverable_strategy_session(self, session_id: str) -> None:
        self._recoverable_strategy_sessions.pop(session_id, None)

    def _hydrate_recoverable_strategy_sessions(self) -> None:
        return

    def _attempt_auto_restore_recoverable_sessions(self) -> None:
        return

    def recover_selected_session(self) -> None:
        messagebox.showinfo("提示", "旧版恢复接管逻辑已下线。请使用信号观察台或交易员管理台的新流程。")

    def _recover_session(self, session_id: str, *, auto: bool) -> bool:
        if not auto:
            self._enqueue_log(f"[{session_id}] 旧版恢复接管逻辑已下线。")
        return False

    def _history_record_from_payload(self, payload: dict[str, object]) -> StrategyHistoryRecord | None:
        started_at = _parse_datetime_snapshot(payload.get("started_at"))
        if started_at is None:
            return None
        config_snapshot = payload.get("config_snapshot")
        return StrategyHistoryRecord(
            record_id=str(payload.get("record_id", "")).strip(),
            session_id=str(payload.get("session_id", "")).strip(),
            api_name=str(payload.get("api_name", "")).strip(),
            strategy_id=str(payload.get("strategy_id", "")).strip(),
            strategy_name=str(payload.get("strategy_name", "")).strip(),
            symbol=str(payload.get("symbol", "")).strip(),
            direction_label=str(payload.get("direction_label", "")).strip(),
            run_mode_label=str(payload.get("run_mode_label", "")).strip(),
            status=str(payload.get("status", "")).strip() or "已停止",
            started_at=started_at,
            stopped_at=_parse_datetime_snapshot(payload.get("stopped_at")),
            ended_reason=str(payload.get("ended_reason", "")).strip(),
            log_file_path=str(payload.get("log_file_path", "")).strip(),
            updated_at=_parse_datetime_snapshot(payload.get("updated_at")),
            config_snapshot=dict(config_snapshot) if isinstance(config_snapshot, dict) else {},
            trade_count=max(0, int(payload.get("trade_count", 0) or 0)),
            win_count=max(0, int(payload.get("win_count", 0) or 0)),
            gross_pnl_total=_parse_decimal_snapshot(payload.get("gross_pnl_total")),
            fee_total=_parse_decimal_snapshot(payload.get("fee_total")),
            funding_total=_parse_decimal_snapshot(payload.get("funding_total")),
            net_pnl_total=_parse_decimal_snapshot(payload.get("net_pnl_total")),
            last_close_reason=str(payload.get("last_close_reason", "")).strip(),
        )

    @staticmethod
    def _history_record_payload(record: StrategyHistoryRecord) -> dict[str, object]:
        return {
            "record_id": record.record_id,
            "session_id": record.session_id,
            "api_name": record.api_name,
            "strategy_id": record.strategy_id,
            "strategy_name": record.strategy_name,
            "symbol": record.symbol,
            "direction_label": record.direction_label,
            "run_mode_label": record.run_mode_label,
            "status": record.status,
            "started_at": record.started_at.isoformat(timespec="seconds"),
            "stopped_at": record.stopped_at.isoformat(timespec="seconds") if record.stopped_at is not None else None,
            "ended_reason": record.ended_reason,
            "log_file_path": record.log_file_path,
            "updated_at": record.updated_at.isoformat(timespec="seconds") if record.updated_at is not None else None,
            "config_snapshot": dict(record.config_snapshot),
            "trade_count": record.trade_count,
            "win_count": record.win_count,
            "gross_pnl_total": format(record.gross_pnl_total, "f"),
            "fee_total": format(record.fee_total, "f"),
            "funding_total": format(record.funding_total, "f"),
            "net_pnl_total": format(record.net_pnl_total, "f"),
            "last_close_reason": record.last_close_reason,
        }

    def _sort_strategy_history_records(self) -> None:
        self._strategy_history_records.sort(
            key=lambda item: (item.started_at.isoformat(timespec="seconds"), item.record_id),
            reverse=True,
        )

    def _save_strategy_history_records(self) -> None:
        self._sort_strategy_history_records()
        try:
            save_strategy_history_snapshot(
                [self._history_record_payload(record) for record in self._strategy_history_records]
            )
        except Exception as exc:
            self._enqueue_log(f"保存策略历史失败：{exc}")

    def _trade_ledger_record_from_payload(self, payload: dict[str, object]) -> StrategyTradeLedgerRecord | None:
        closed_at = _parse_datetime_snapshot(payload.get("closed_at"))
        if closed_at is None:
            return None
        return StrategyTradeLedgerRecord(
            record_id=str(payload.get("record_id", "")).strip(),
            history_record_id=str(payload.get("history_record_id", "")).strip(),
            session_id=str(payload.get("session_id", "")).strip(),
            api_name=str(payload.get("api_name", "")).strip(),
            strategy_id=str(payload.get("strategy_id", "")).strip(),
            strategy_name=str(payload.get("strategy_name", "")).strip(),
            symbol=str(payload.get("symbol", "")).strip(),
            direction_label=str(payload.get("direction_label", "")).strip(),
            run_mode_label=str(payload.get("run_mode_label", "")).strip(),
            environment=str(payload.get("environment", "")).strip(),
            signal_bar_at=_parse_datetime_snapshot(payload.get("signal_bar_at")),
            opened_at=_parse_datetime_snapshot(payload.get("opened_at")),
            closed_at=closed_at,
            entry_order_id=str(payload.get("entry_order_id", "")).strip(),
            entry_client_order_id=str(payload.get("entry_client_order_id", "")).strip(),
            exit_order_id=str(payload.get("exit_order_id", "")).strip(),
            protective_algo_id=str(payload.get("protective_algo_id", "")).strip(),
            protective_algo_cl_ord_id=str(payload.get("protective_algo_cl_ord_id", "")).strip(),
            entry_price=_parse_decimal_snapshot(payload.get("entry_price"), default=None) if payload.get("entry_price") else None,
            exit_price=_parse_decimal_snapshot(payload.get("exit_price"), default=None) if payload.get("exit_price") else None,
            size=_parse_decimal_snapshot(payload.get("size"), default=None) if payload.get("size") else None,
            entry_fee=_parse_decimal_snapshot(payload.get("entry_fee"), default=None) if payload.get("entry_fee") else None,
            exit_fee=_parse_decimal_snapshot(payload.get("exit_fee"), default=None) if payload.get("exit_fee") else None,
            funding_fee=_parse_decimal_snapshot(payload.get("funding_fee"), default=None) if payload.get("funding_fee") else None,
            gross_pnl=_parse_decimal_snapshot(payload.get("gross_pnl"), default=None) if payload.get("gross_pnl") else None,
            net_pnl=_parse_decimal_snapshot(payload.get("net_pnl"), default=None) if payload.get("net_pnl") else None,
            close_reason=str(payload.get("close_reason", "")).strip(),
            reason_confidence=str(payload.get("reason_confidence", "")).strip() or "low",
            summary_note=str(payload.get("summary_note", "")).strip(),
            updated_at=_parse_datetime_snapshot(payload.get("updated_at")),
        )

    @staticmethod
    def _trade_ledger_payload(record: StrategyTradeLedgerRecord) -> dict[str, object]:
        return {
            "record_id": record.record_id,
            "history_record_id": record.history_record_id,
            "session_id": record.session_id,
            "api_name": record.api_name,
            "strategy_id": record.strategy_id,
            "strategy_name": record.strategy_name,
            "symbol": record.symbol,
            "direction_label": record.direction_label,
            "run_mode_label": record.run_mode_label,
            "environment": record.environment,
            "signal_bar_at": record.signal_bar_at.isoformat(timespec="seconds") if record.signal_bar_at is not None else None,
            "opened_at": record.opened_at.isoformat(timespec="seconds") if record.opened_at is not None else None,
            "closed_at": record.closed_at.isoformat(timespec="seconds"),
            "entry_order_id": record.entry_order_id,
            "entry_client_order_id": record.entry_client_order_id,
            "exit_order_id": record.exit_order_id,
            "protective_algo_id": record.protective_algo_id,
            "protective_algo_cl_ord_id": record.protective_algo_cl_ord_id,
            "entry_price": format(record.entry_price, "f") if record.entry_price is not None else None,
            "exit_price": format(record.exit_price, "f") if record.exit_price is not None else None,
            "size": format(record.size, "f") if record.size is not None else None,
            "entry_fee": format(record.entry_fee, "f") if record.entry_fee is not None else None,
            "exit_fee": format(record.exit_fee, "f") if record.exit_fee is not None else None,
            "funding_fee": format(record.funding_fee, "f") if record.funding_fee is not None else None,
            "gross_pnl": format(record.gross_pnl, "f") if record.gross_pnl is not None else None,
            "net_pnl": format(record.net_pnl, "f") if record.net_pnl is not None else None,
            "close_reason": record.close_reason,
            "reason_confidence": record.reason_confidence,
            "summary_note": record.summary_note,
            "updated_at": record.updated_at.isoformat(timespec="seconds") if record.updated_at is not None else None,
        }

    def _sort_strategy_trade_ledger_records(self) -> None:
        self._strategy_trade_ledger_records.sort(
            key=lambda item: (item.closed_at.isoformat(timespec="seconds"), item.record_id),
            reverse=True,
        )

    def _save_strategy_trade_ledger_records(self) -> None:
        self._sort_strategy_trade_ledger_records()
        try:
            save_strategy_trade_ledger_snapshot(
                [self._trade_ledger_payload(record) for record in self._strategy_trade_ledger_records]
            )
        except Exception as exc:
            self._enqueue_log(f"保存策略交易账本失败：{exc}")

    def _load_strategy_trade_ledger(self) -> None:
        try:
            snapshot = load_strategy_trade_ledger_snapshot()
        except Exception as exc:
            self._enqueue_log(f"读取策略交易账本失败：{exc}")
            return
        records: list[StrategyTradeLedgerRecord] = []
        raw_records = snapshot.get("records", [])
        if isinstance(raw_records, list):
            for item in raw_records:
                if not isinstance(item, dict):
                    continue
                record = self._trade_ledger_record_from_payload(item)
                if record is not None:
                    records.append(record)
        self._strategy_trade_ledger_records = records
        self._strategy_trade_ledger_by_id = {record.record_id: record for record in records}
        self._rebuild_history_financials_from_trade_ledger()

    def _next_strategy_trade_ledger_record_id(self, session: StrategySession, closed_at: datetime) -> str:
        base = f"{closed_at.strftime('%Y%m%d%H%M%S%f')}-{session.session_id}"
        record_id = base
        suffix = 2
        while record_id in self._strategy_trade_ledger_by_id:
            record_id = f"{base}-{suffix}"
            suffix += 1
        return record_id

    def _upsert_strategy_trade_ledger_record(self, record: StrategyTradeLedgerRecord) -> None:
        existing = self._strategy_trade_ledger_by_id.get(record.record_id)
        self._strategy_trade_ledger_by_id[record.record_id] = record
        if existing is None:
            self._strategy_trade_ledger_records.append(record)
        else:
            for index, item in enumerate(self._strategy_trade_ledger_records):
                if item.record_id == record.record_id:
                    self._strategy_trade_ledger_records[index] = record
                    break
        self._save_strategy_trade_ledger_records()

    def _apply_financial_totals(self, target: StrategySession | StrategyHistoryRecord, records: list[StrategyTradeLedgerRecord]) -> None:
        target.trade_count = len(records)
        target.win_count = sum(1 for item in records if (item.net_pnl or Decimal("0")) > 0)
        target.gross_pnl_total = sum(((item.gross_pnl or Decimal("0")) for item in records), Decimal("0"))
        target.fee_total = sum(
            (((item.entry_fee or Decimal("0")) + (item.exit_fee or Decimal("0"))) for item in records),
            Decimal("0"),
        )
        target.funding_total = sum(((item.funding_fee or Decimal("0")) for item in records), Decimal("0"))
        target.net_pnl_total = sum(((item.net_pnl or Decimal("0")) for item in records), Decimal("0"))
        target.last_close_reason = records[0].close_reason if records else ""

    def _rebuild_history_financials_from_trade_ledger(self) -> None:
        grouped: dict[str, list[StrategyTradeLedgerRecord]] = {}
        for record in self._strategy_trade_ledger_records:
            key = record.history_record_id.strip()
            if not key:
                continue
            grouped.setdefault(key, []).append(record)
        changed = False
        for record in self._strategy_history_records:
            matched = grouped.get(record.record_id, [])
            previous = (
                record.trade_count,
                record.win_count,
                record.gross_pnl_total,
                record.fee_total,
                record.funding_total,
                record.net_pnl_total,
                record.last_close_reason,
            )
            self._apply_financial_totals(record, matched)
            current = (
                record.trade_count,
                record.win_count,
                record.gross_pnl_total,
                record.fee_total,
                record.funding_total,
                record.net_pnl_total,
                record.last_close_reason,
            )
            if current != previous:
                record.updated_at = datetime.now()
                changed = True
        if changed:
            self._save_strategy_history_records()

    def _refresh_session_financials_from_trade_ledger(self, session: StrategySession) -> None:
        if session.history_record_id:
            matched = [
                record
                for record in self._strategy_trade_ledger_records
                if record.history_record_id == session.history_record_id
            ]
        else:
            matched = [record for record in self._strategy_trade_ledger_records if record.session_id == session.session_id]
        self._apply_financial_totals(session, matched)
        self._upsert_session_row(session)
        self._refresh_selected_session_details()
        self._sync_strategy_history_from_session(session)

    def _load_strategy_history(self) -> None:
        try:
            snapshot = load_strategy_history_snapshot()
        except Exception as exc:
            self._enqueue_log(f"读取策略历史失败：{exc}")
            return
        records: list[StrategyHistoryRecord] = []
        raw_records = snapshot.get("records", [])
        if isinstance(raw_records, list):
            for item in raw_records:
                if not isinstance(item, dict):
                    continue
                record = self._history_record_from_payload(item)
                if record is not None:
                    records.append(record)
        self._strategy_history_records = records
        self._strategy_history_by_id = {record.record_id: record for record in records}
        recoverable_count, abnormal_count = self._mark_unfinished_strategy_history_records()
        if recoverable_count:
            self._enqueue_log(f"检测到 {recoverable_count} 条可恢复历史策略，已标记为待恢复。")
        if abnormal_count:
            self._enqueue_log(f"检测到 {abnormal_count} 条未正常结束的历史策略，已自动标记为异常结束。")

    def _mark_unfinished_strategy_history_records(self) -> tuple[int, int]:
        recovered_at = datetime.now()
        recoverable_count = 0
        abnormal_count = 0
        for record in self._strategy_history_records:
            if record.status not in {"运行中", "停止中"}:
                continue
            if record.session_id in self._recoverable_strategy_sessions:
                record.status = "待恢复"
                if not record.ended_reason:
                    record.ended_reason = "应用关闭后待恢复接管"
                recoverable_count += 1
            else:
                record.status = "异常结束"
                if not record.ended_reason:
                    record.ended_reason = "应用异常退出"
                abnormal_count += 1
            if record.stopped_at is None:
                record.stopped_at = recovered_at
            record.updated_at = recovered_at
        if recoverable_count or abnormal_count:
            self._save_strategy_history_records()
        return recoverable_count, abnormal_count

    def _next_strategy_history_record_id(self, session: StrategySession) -> str:
        base = f"{session.started_at.strftime('%Y%m%d%H%M%S%f')}-{session.session_id}"
        record_id = base
        suffix = 2
        while record_id in self._strategy_history_by_id:
            record_id = f"{base}-{suffix}"
            suffix += 1
        return record_id

    def _build_strategy_history_record(self, session: StrategySession) -> StrategyHistoryRecord:
        record_id = session.history_record_id or self._next_strategy_history_record_id(session)
        return StrategyHistoryRecord(
            record_id=record_id,
            session_id=session.session_id,
            api_name=session.api_name,
            strategy_id=session.strategy_id,
            strategy_name=session.strategy_name,
            symbol=session.symbol,
            direction_label=session.direction_label,
            run_mode_label=session.run_mode_label,
            status=session.status,
            started_at=session.started_at,
            stopped_at=session.stopped_at,
            ended_reason=session.ended_reason,
            log_file_path=str(session.log_file_path) if session.log_file_path is not None else "",
            updated_at=datetime.now(),
            config_snapshot=_serialize_strategy_config_snapshot(session.config),
            trade_count=session.trade_count,
            win_count=session.win_count,
            gross_pnl_total=session.gross_pnl_total,
            fee_total=session.fee_total,
            funding_total=session.funding_total,
            net_pnl_total=session.net_pnl_total,
            last_close_reason=session.last_close_reason,
        )

    def _upsert_strategy_history_record(self, record: StrategyHistoryRecord) -> None:
        existing = self._strategy_history_by_id.get(record.record_id)
        self._strategy_history_by_id[record.record_id] = record
        if existing is None:
            self._strategy_history_records.append(record)
        else:
            for index, item in enumerate(self._strategy_history_records):
                if item.record_id == record.record_id:
                    self._strategy_history_records[index] = record
                    break
        self._save_strategy_history_records()
        self._render_strategy_history_view()

    def _record_strategy_session_started(self, session: StrategySession) -> None:
        record = self._build_strategy_history_record(session)
        session.history_record_id = record.record_id
        self._upsert_strategy_history_record(record)

    def _sync_strategy_history_from_session(self, session: StrategySession) -> None:
        if not session.history_record_id:
            return
        record = self._strategy_history_by_id.get(session.history_record_id)
        if record is None:
            self._record_strategy_session_started(session)
            return
        desired_snapshot = _serialize_strategy_config_snapshot(session.config)
        changed = False
        for attr, desired in (
            ("session_id", session.session_id),
            ("api_name", session.api_name),
            ("strategy_id", session.strategy_id),
            ("strategy_name", session.strategy_name),
            ("symbol", session.symbol),
            ("direction_label", session.direction_label),
            ("run_mode_label", session.run_mode_label),
            ("status", session.status),
            ("started_at", session.started_at),
            ("stopped_at", session.stopped_at),
            ("ended_reason", session.ended_reason),
            ("log_file_path", str(session.log_file_path) if session.log_file_path is not None else ""),
            ("trade_count", session.trade_count),
            ("win_count", session.win_count),
            ("gross_pnl_total", session.gross_pnl_total),
            ("fee_total", session.fee_total),
            ("funding_total", session.funding_total),
            ("net_pnl_total", session.net_pnl_total),
            ("last_close_reason", session.last_close_reason),
        ):
            if getattr(record, attr) != desired:
                setattr(record, attr, desired)
                changed = True
        if record.config_snapshot != desired_snapshot:
            record.config_snapshot = desired_snapshot
            changed = True
        if not changed:
            return
        record.updated_at = datetime.now()
        self._upsert_strategy_history_record(record)

    def open_strategy_history_window(self) -> None:
        if self._strategy_history_window is not None and _widget_exists(self._strategy_history_window):
            self._strategy_history_window.focus_force()
            self._render_strategy_history_view()
            return

        window = Toplevel(self.root)
        apply_window_icon(window)
        window.title("历史策略")
        apply_adaptive_window_geometry(
            window,
            width_ratio=0.76,
            height_ratio=0.72,
            min_width=1080,
            min_height=640,
            max_width=1680,
            max_height=1080,
        )
        self._strategy_history_window = window
        window.protocol("WM_DELETE_WINDOW", self._close_strategy_history_window)

        container = ttk.Frame(window, padding=12)
        container.pack(fill="both", expand=True)
        container.columnconfigure(0, weight=1)
        container.rowconfigure(1, weight=1)
        container.rowconfigure(2, weight=1)

        header = ttk.Frame(container)
        header.grid(row=0, column=0, sticky="ew", pady=(0, 10))
        header.columnconfigure(0, weight=1)
        ttk.Label(
            header,
            text=f"历史策略会永久保存到：{strategy_history_file_path()}",
            justify="left",
        ).grid(row=0, column=0, sticky="w")
        action_row = ttk.Frame(header)
        action_row.grid(row=0, column=1, sticky="e")
        ttk.Button(action_row, text="删除选中", command=self.delete_selected_strategy_history_record).grid(
            row=0, column=0, padx=(0, 6)
        )
        ttk.Button(action_row, text="清空历史", command=self.clear_strategy_history_records).grid(
            row=0, column=1, padx=(0, 6)
        )
        ttk.Button(action_row, text="打开日志", command=self.open_selected_strategy_history_log).grid(
            row=0, column=2, padx=(0, 6)
        )
        ttk.Button(action_row, text="复制日志路径", command=self.copy_selected_strategy_history_log_path).grid(
            row=0, column=3, padx=(0, 6)
        )
        ttk.Button(action_row, text="刷新", command=self._render_strategy_history_view).grid(
            row=0, column=4, padx=(0, 6)
        )
        ttk.Button(action_row, text="关闭", command=self._close_strategy_history_window).grid(row=0, column=5)

        list_frame = ttk.LabelFrame(container, text="历史策略列表", padding=12)
        list_frame.grid(row=1, column=0, sticky="nsew")
        list_frame.columnconfigure(0, weight=1)
        list_frame.rowconfigure(0, weight=1)

        self._strategy_history_tree = ttk.Treeview(
            list_frame,
            columns=("session", "api", "strategy", "symbol", "direction", "mode", "pnl", "status", "started", "stopped"),
            show="headings",
            selectmode="browse",
        )
        tree = self._strategy_history_tree
        tree.heading("session", text="会话")
        tree.heading("api", text="API")
        tree.heading("strategy", text="策略")
        tree.heading("symbol", text="标的")
        tree.heading("direction", text="方向")
        tree.heading("mode", text="模式")
        tree.heading("pnl", text="净盈亏")
        tree.heading("status", text="状态")
        tree.heading("started", text="启动时间")
        tree.heading("stopped", text="停止时间")
        tree.column("session", width=76, anchor="center")
        tree.column("api", width=88, anchor="center")
        tree.column("strategy", width=138, anchor="w")
        tree.column("symbol", width=178, anchor="w")
        tree.column("direction", width=82, anchor="center")
        tree.column("mode", width=102, anchor="center")
        tree.column("pnl", width=110, anchor="e")
        tree.column("status", width=92, anchor="center")
        tree.column("started", width=150, anchor="center")
        tree.column("stopped", width=150, anchor="center")
        tree.grid(row=0, column=0, sticky="nsew")
        tree.bind("<<TreeviewSelect>>", self._on_strategy_history_selected)
        history_scroll = ttk.Scrollbar(list_frame, orient="vertical", command=tree.yview)
        history_scroll.grid(row=0, column=1, sticky="ns")
        tree.configure(yscrollcommand=history_scroll.set)

        detail_frame = ttk.LabelFrame(container, text="选中历史记录详情", padding=12)
        detail_frame.grid(row=2, column=0, sticky="nsew", pady=(10, 0))
        detail_frame.columnconfigure(0, weight=1)
        detail_frame.rowconfigure(0, weight=1)

        self._strategy_history_detail = Text(
            detail_frame,
            height=12,
            wrap="word",
            font=("Microsoft YaHei UI", 10),
            relief="flat",
        )
        self._strategy_history_detail.grid(row=0, column=0, sticky="nsew")
        detail_scroll = ttk.Scrollbar(detail_frame, orient="vertical", command=self._strategy_history_detail.yview)
        detail_scroll.grid(row=0, column=1, sticky="ns")
        self._strategy_history_detail.configure(yscrollcommand=detail_scroll.set)
        self._set_readonly_text(self._strategy_history_detail, self.strategy_history_text.get())
        self._render_strategy_history_view()

    def _close_strategy_history_window(self) -> None:
        if self._strategy_history_window is not None and _widget_exists(self._strategy_history_window):
            self._strategy_history_window.destroy()
        self._strategy_history_window = None
        self._strategy_history_tree = None
        self._strategy_history_detail = None
        self._strategy_history_selected_record_id = None

    def _selected_strategy_history_record(self) -> StrategyHistoryRecord | None:
        tree = self._strategy_history_tree
        if tree is None or not _widget_exists(tree):
            if self._strategy_history_selected_record_id is None:
                return None
            return self._strategy_history_by_id.get(self._strategy_history_selected_record_id)
        try:
            selection = tree.selection()
        except TclError:
            selection = ()
        if selection:
            self._strategy_history_selected_record_id = selection[0]
        if self._strategy_history_selected_record_id is None:
            return None
        return self._strategy_history_by_id.get(self._strategy_history_selected_record_id)

    def _session_by_history_record_id(self, record_id: str) -> StrategySession | None:
        for session in self.sessions.values():
            if session.history_record_id == record_id:
                return session
        return None

    def open_strategy_book_window(self) -> None:
        if self._strategy_book_window is not None and _widget_exists(self._strategy_book_window):
            self._strategy_book_window.focus_force()
            self._refresh_strategy_book_window()
            return

        window = Toplevel(self.root)
        apply_window_icon(window)
        window.title("普通量化策略总账本")
        apply_adaptive_window_geometry(
            window,
            width_ratio=0.82,
            height_ratio=0.76,
            min_width=1240,
            min_height=720,
            max_width=1780,
            max_height=1160,
        )
        self._strategy_book_window = window
        window.protocol("WM_DELETE_WINDOW", self._close_strategy_book_window)

        container = ttk.Frame(window, padding=12)
        container.pack(fill="both", expand=True)
        container.columnconfigure(0, weight=1)
        container.rowconfigure(2, weight=1)
        container.rowconfigure(3, weight=2)

        header = ttk.Frame(container)
        header.grid(row=0, column=0, sticky="ew", pady=(0, 10))
        header.columnconfigure(0, weight=1)
        ttk.Label(
            header,
            text=f"普通量化总账本来源：{strategy_trade_ledger_file_path()}",
            justify="left",
        ).grid(row=0, column=0, sticky="w")
        ttk.Label(header, textvariable=self.strategy_book_summary_text, justify="left").grid(
            row=1,
            column=0,
            sticky="w",
            pady=(6, 0),
        )
        action_row = ttk.Frame(header)
        action_row.grid(row=0, column=1, rowspan=2, sticky="e")
        ttk.Button(action_row, text="刷新", command=self._refresh_strategy_book_window).grid(row=0, column=0, padx=(0, 6))
        ttk.Button(action_row, text="关闭", command=self._close_strategy_book_window).grid(row=0, column=1)

        filter_frame = ttk.LabelFrame(container, text="筛选条件", padding=12)
        filter_frame.grid(row=1, column=0, sticky="ew", pady=(0, 10))
        for column in range(7):
            filter_frame.columnconfigure(column, weight=1 if column in {1, 3, 5} else 0)

        ttk.Label(filter_frame, text="API").grid(row=0, column=0, sticky="w")
        self._strategy_book_api_combo = ttk.Combobox(
            filter_frame,
            textvariable=self.strategy_book_api_filter,
            state="readonly",
            width=14,
        )
        self._strategy_book_api_combo.grid(row=0, column=1, sticky="ew", padx=(6, 12))

        ttk.Label(filter_frame, text="交易员").grid(row=0, column=2, sticky="w")
        self._strategy_book_trader_combo = ttk.Combobox(
            filter_frame,
            textvariable=self.strategy_book_trader_filter,
            state="readonly",
            width=14,
        )
        self._strategy_book_trader_combo.grid(row=0, column=3, sticky="ew", padx=(6, 12))

        ttk.Label(filter_frame, text="策略").grid(row=0, column=4, sticky="w")
        self._strategy_book_strategy_combo = ttk.Combobox(
            filter_frame,
            textvariable=self.strategy_book_strategy_filter,
            state="readonly",
            width=20,
        )
        self._strategy_book_strategy_combo.grid(row=0, column=5, sticky="ew", padx=(6, 12))

        ttk.Label(filter_frame, text="标的").grid(row=0, column=6, sticky="w")
        self._strategy_book_symbol_combo = ttk.Combobox(
            filter_frame,
            textvariable=self.strategy_book_symbol_filter,
            state="readonly",
            width=18,
        )
        self._strategy_book_symbol_combo.grid(row=0, column=7, sticky="ew", padx=(6, 0))

        filter_frame.columnconfigure(7, weight=1)
        ttk.Label(filter_frame, text="周期").grid(row=1, column=0, sticky="w", pady=(8, 0))
        self._strategy_book_bar_combo = ttk.Combobox(
            filter_frame,
            textvariable=self.strategy_book_bar_filter,
            state="readonly",
            width=14,
        )
        self._strategy_book_bar_combo.grid(row=1, column=1, sticky="ew", padx=(6, 12), pady=(8, 0))

        ttk.Label(filter_frame, text="方向").grid(row=1, column=2, sticky="w", pady=(8, 0))
        self._strategy_book_direction_combo = ttk.Combobox(
            filter_frame,
            textvariable=self.strategy_book_direction_filter,
            state="readonly",
            width=14,
        )
        self._strategy_book_direction_combo.grid(row=1, column=3, sticky="ew", padx=(6, 12), pady=(8, 0))

        ttk.Label(filter_frame, text="状态").grid(row=1, column=4, sticky="w", pady=(8, 0))
        self._strategy_book_status_combo = ttk.Combobox(
            filter_frame,
            textvariable=self.strategy_book_status_filter,
            state="readonly",
            width=20,
        )
        self._strategy_book_status_combo.grid(row=1, column=5, sticky="ew", padx=(6, 12), pady=(8, 0))

        ttk.Button(filter_frame, text="重置筛选", command=self._reset_strategy_book_filters).grid(
            row=1,
            column=7,
            sticky="e",
            pady=(8, 0),
        )

        for combo in (
            self._strategy_book_api_combo,
            self._strategy_book_trader_combo,
            self._strategy_book_strategy_combo,
            self._strategy_book_symbol_combo,
            self._strategy_book_bar_combo,
            self._strategy_book_direction_combo,
            self._strategy_book_status_combo,
        ):
            combo.bind("<<ComboboxSelected>>", self._on_strategy_book_filter_changed)

        summary_frame = ttk.LabelFrame(container, text="策略汇总", padding=12)
        summary_frame.grid(row=2, column=0, sticky="nsew")
        summary_frame.columnconfigure(0, weight=1)
        summary_frame.rowconfigure(0, weight=1)
        self._strategy_book_group_tree = ttk.Treeview(
            summary_frame,
            columns=(
                "api",
                "trader",
                "strategy",
                "symbol",
                "bar",
                "direction",
                "status",
                "trades",
                "wins",
                "losses",
                "rate",
                "gross",
                "fee",
                "funding",
                "net",
                "closed",
            ),
            show="headings",
            selectmode="browse",
        )
        group_tree = self._strategy_book_group_tree
        group_tree.heading("api", text="API")
        group_tree.heading("trader", text="交易员")
        group_tree.heading("strategy", text="策略")
        group_tree.heading("symbol", text="标的")
        group_tree.heading("bar", text="周期")
        group_tree.heading("direction", text="方向")
        group_tree.heading("status", text="状态")
        group_tree.heading("trades", text="平仓单")
        group_tree.heading("wins", text="盈利")
        group_tree.heading("losses", text="亏损")
        group_tree.heading("rate", text="胜率")
        group_tree.heading("gross", text="毛盈亏")
        group_tree.heading("fee", text="手续费")
        group_tree.heading("funding", text="资金费")
        group_tree.heading("net", text="净盈亏")
        group_tree.heading("closed", text="最近平仓")
        group_tree.column("api", width=84, anchor="center")
        group_tree.column("trader", width=92, anchor="center")
        group_tree.column("strategy", width=150, anchor="w")
        group_tree.column("symbol", width=170, anchor="w")
        group_tree.column("bar", width=68, anchor="center")
        group_tree.column("direction", width=82, anchor="center")
        group_tree.column("status", width=92, anchor="center")
        group_tree.column("trades", width=72, anchor="center")
        group_tree.column("wins", width=66, anchor="center")
        group_tree.column("losses", width=66, anchor="center")
        group_tree.column("rate", width=72, anchor="center")
        group_tree.column("gross", width=98, anchor="e")
        group_tree.column("fee", width=98, anchor="e")
        group_tree.column("funding", width=98, anchor="e")
        group_tree.column("net", width=98, anchor="e")
        group_tree.column("closed", width=150, anchor="center")
        group_tree.grid(row=0, column=0, sticky="nsew")
        group_scroll = ttk.Scrollbar(summary_frame, orient="vertical", command=group_tree.yview)
        group_scroll.grid(row=0, column=1, sticky="ns")
        group_tree.configure(yscrollcommand=group_scroll.set)

        ledger_frame = ttk.LabelFrame(container, text="账本流水", padding=12)
        ledger_frame.grid(row=3, column=0, sticky="nsew", pady=(10, 0))
        ledger_frame.columnconfigure(0, weight=1)
        ledger_frame.rowconfigure(0, weight=1)
        self._strategy_book_ledger_tree = ttk.Treeview(
            ledger_frame,
            columns=(
                "closed",
                "api",
                "trader",
                "strategy",
                "symbol",
                "bar",
                "direction",
                "status",
                "session",
                "opened",
                "entry",
                "exit",
                "size",
                "gross",
                "fee",
                "funding",
                "net",
                "reason",
            ),
            show="headings",
            selectmode="browse",
        )
        ledger_tree = self._strategy_book_ledger_tree
        ledger_tree.heading("closed", text="平仓时间")
        ledger_tree.heading("api", text="API")
        ledger_tree.heading("trader", text="交易员")
        ledger_tree.heading("strategy", text="策略")
        ledger_tree.heading("symbol", text="标的")
        ledger_tree.heading("bar", text="周期")
        ledger_tree.heading("direction", text="方向")
        ledger_tree.heading("status", text="状态")
        ledger_tree.heading("session", text="会话")
        ledger_tree.heading("opened", text="开仓时间")
        ledger_tree.heading("entry", text="开仓价")
        ledger_tree.heading("exit", text="平仓价")
        ledger_tree.heading("size", text="数量")
        ledger_tree.heading("gross", text="毛盈亏")
        ledger_tree.heading("fee", text="手续费")
        ledger_tree.heading("funding", text="资金费")
        ledger_tree.heading("net", text="净盈亏")
        ledger_tree.heading("reason", text="原因")
        ledger_tree.column("closed", width=150, anchor="center")
        ledger_tree.column("api", width=84, anchor="center")
        ledger_tree.column("trader", width=92, anchor="center")
        ledger_tree.column("strategy", width=150, anchor="w")
        ledger_tree.column("symbol", width=170, anchor="w")
        ledger_tree.column("bar", width=68, anchor="center")
        ledger_tree.column("direction", width=82, anchor="center")
        ledger_tree.column("status", width=92, anchor="center")
        ledger_tree.column("session", width=72, anchor="center")
        ledger_tree.column("opened", width=150, anchor="center")
        ledger_tree.column("entry", width=90, anchor="e")
        ledger_tree.column("exit", width=90, anchor="e")
        ledger_tree.column("size", width=82, anchor="e")
        ledger_tree.column("gross", width=98, anchor="e")
        ledger_tree.column("fee", width=98, anchor="e")
        ledger_tree.column("funding", width=98, anchor="e")
        ledger_tree.column("net", width=98, anchor="e")
        ledger_tree.column("reason", width=220, anchor="w")
        ledger_tree.grid(row=0, column=0, sticky="nsew")
        ledger_tree.bind("<<TreeviewSelect>>", self._on_strategy_book_ledger_selected)
        ledger_v_scroll = ttk.Scrollbar(ledger_frame, orient="vertical", command=ledger_tree.yview)
        ledger_v_scroll.grid(row=0, column=1, sticky="ns")
        ledger_x_scroll = ttk.Scrollbar(ledger_frame, orient="horizontal", command=ledger_tree.xview)
        ledger_x_scroll.grid(row=1, column=0, sticky="ew", pady=(6, 0))
        ledger_tree.configure(yscrollcommand=ledger_v_scroll.set, xscrollcommand=ledger_x_scroll.set)

        self._refresh_strategy_book_window()

    def _close_strategy_book_window(self) -> None:
        if self._strategy_book_window is not None and _widget_exists(self._strategy_book_window):
            self._strategy_book_window.destroy()
        self._strategy_book_window = None
        self._strategy_book_group_tree = None
        self._strategy_book_ledger_tree = None
        self._strategy_book_api_combo = None
        self._strategy_book_trader_combo = None
        self._strategy_book_strategy_combo = None
        self._strategy_book_symbol_combo = None
        self._strategy_book_bar_combo = None
        self._strategy_book_direction_combo = None
        self._strategy_book_status_combo = None

    def _current_strategy_book_filters(self) -> NormalStrategyBookFilters:
        return NormalStrategyBookFilters(
            api_name=_strategy_book_filter_normalized(
                self.strategy_book_api_filter.get(),
                STRATEGY_BOOK_FILTER_ALL_API,
            ),
            trader_label=_strategy_book_filter_normalized(
                self.strategy_book_trader_filter.get(),
                STRATEGY_BOOK_FILTER_ALL_TRADER,
            ),
            strategy_name=_strategy_book_filter_normalized(
                self.strategy_book_strategy_filter.get(),
                STRATEGY_BOOK_FILTER_ALL_STRATEGY,
            ),
            symbol=_strategy_book_filter_normalized(
                self.strategy_book_symbol_filter.get(),
                STRATEGY_BOOK_FILTER_ALL_SYMBOL,
            ),
            bar=_strategy_book_filter_normalized(
                self.strategy_book_bar_filter.get(),
                STRATEGY_BOOK_FILTER_ALL_BAR,
            ),
            direction_label=_strategy_book_filter_normalized(
                self.strategy_book_direction_filter.get(),
                STRATEGY_BOOK_FILTER_ALL_DIRECTION,
            ),
            status=_strategy_book_filter_normalized(
                self.strategy_book_status_filter.get(),
                STRATEGY_BOOK_FILTER_ALL_STATUS,
            ),
        )

    def _reset_strategy_book_filters(self) -> None:
        self.strategy_book_api_filter.set(STRATEGY_BOOK_FILTER_ALL_API)
        self.strategy_book_trader_filter.set(STRATEGY_BOOK_FILTER_ALL_TRADER)
        self.strategy_book_strategy_filter.set(STRATEGY_BOOK_FILTER_ALL_STRATEGY)
        self.strategy_book_symbol_filter.set(STRATEGY_BOOK_FILTER_ALL_SYMBOL)
        self.strategy_book_bar_filter.set(STRATEGY_BOOK_FILTER_ALL_BAR)
        self.strategy_book_direction_filter.set(STRATEGY_BOOK_FILTER_ALL_DIRECTION)
        self.strategy_book_status_filter.set(STRATEGY_BOOK_FILTER_ALL_STATUS)
        self._refresh_strategy_book_window()

    def _on_strategy_book_filter_changed(self, *_: object) -> None:
        self._refresh_strategy_book_window()

    def _refresh_strategy_book_filter_controls(self) -> None:
        options = _build_normal_strategy_book_filter_options(
            self._strategy_trade_ledger_records,
            self._strategy_history_records,
        )
        control_specs = (
            (
                self._strategy_book_api_combo,
                self.strategy_book_api_filter,
                options["api_name"],
                STRATEGY_BOOK_FILTER_ALL_API,
            ),
            (
                self._strategy_book_trader_combo,
                self.strategy_book_trader_filter,
                options["trader_label"],
                STRATEGY_BOOK_FILTER_ALL_TRADER,
            ),
            (
                self._strategy_book_strategy_combo,
                self.strategy_book_strategy_filter,
                options["strategy_name"],
                STRATEGY_BOOK_FILTER_ALL_STRATEGY,
            ),
            (
                self._strategy_book_symbol_combo,
                self.strategy_book_symbol_filter,
                options["symbol"],
                STRATEGY_BOOK_FILTER_ALL_SYMBOL,
            ),
            (
                self._strategy_book_bar_combo,
                self.strategy_book_bar_filter,
                options["bar"],
                STRATEGY_BOOK_FILTER_ALL_BAR,
            ),
            (
                self._strategy_book_direction_combo,
                self.strategy_book_direction_filter,
                options["direction_label"],
                STRATEGY_BOOK_FILTER_ALL_DIRECTION,
            ),
            (
                self._strategy_book_status_combo,
                self.strategy_book_status_filter,
                options["status"],
                STRATEGY_BOOK_FILTER_ALL_STATUS,
            ),
        )
        for combo, variable, values, default_value in control_specs:
            if combo is None or not _widget_exists(combo):
                continue
            combo.configure(values=values)
            current_value = variable.get()
            variable.set(current_value if current_value in values else default_value)

    def _refresh_strategy_book_window(self) -> None:
        if self._strategy_book_window is None or not _widget_exists(self._strategy_book_window):
            return
        self._refresh_strategy_book_filter_controls()
        filters = self._current_strategy_book_filters()
        summary = _build_normal_strategy_book_summary(
            self._strategy_trade_ledger_records,
            self._strategy_history_records,
            filters=filters,
        )
        self.strategy_book_summary_text.set(_normal_strategy_book_summary_text(summary))
        self._refresh_strategy_book_group_tree(filters=filters)
        self._refresh_strategy_book_ledger_tree(filters=filters)

    def _refresh_strategy_book_group_tree(self, *, filters: NormalStrategyBookFilters | None = None) -> None:
        tree = self._strategy_book_group_tree
        if tree is None or not _widget_exists(tree):
            return
        try:
            selected = tree.selection()
        except TclError:
            selected = ()
        selected_id = selected[0] if selected else None
        for item_id in tree.get_children():
            tree.delete(item_id)
        for row_id, values in _build_normal_strategy_book_group_rows(
            self._strategy_trade_ledger_records,
            self._strategy_history_records,
            filters=filters,
        ):
            tree.insert("", END, iid=row_id, values=values)
        if selected_id is not None and tree.exists(selected_id):
            tree.selection_set(selected_id)
            tree.focus(selected_id)
            tree.see(selected_id)

    def _refresh_strategy_book_ledger_tree(self, *, filters: NormalStrategyBookFilters | None = None) -> None:
        tree = self._strategy_book_ledger_tree
        if tree is None or not _widget_exists(tree):
            return
        try:
            selected = tree.selection()
        except TclError:
            selected = ()
        selected_id = selected[0] if selected else None
        for item_id in tree.get_children():
            tree.delete(item_id)
        for row_id, values in _build_normal_strategy_book_ledger_rows(
            self._strategy_trade_ledger_records,
            self._strategy_history_records,
            filters=filters,
        ):
            session_id = str(values[8] or "").strip()
            tree.insert("", END, iid=row_id, values=values, tags=(session_id,))
        if selected_id is not None and tree.exists(selected_id):
            tree.selection_set(selected_id)
            tree.focus(selected_id)
            tree.see(selected_id)

    def _on_strategy_book_ledger_selected(self, *_: object) -> None:
        tree = self._strategy_book_ledger_tree
        if tree is None or not _widget_exists(tree):
            return
        try:
            selection = tree.selection()
        except TclError:
            selection = ()
        if not selection:
            return
        tags = tree.item(selection[0], "tags")
        session_id = tags[0] if tags else ""
        if session_id:
            self._focus_session_row(session_id)

    @staticmethod
    def _session_blocks_history_deletion(session: StrategySession) -> bool:
        return session.engine.is_running or session.status in {"运行中", "停止中", "待恢复", "恢复中"}

    def _remove_strategy_history_records(self, record_ids: list[str], *, selected_before: str | None = None) -> tuple[int, int]:
        removed_count = 0
        blocked_count = 0
        blocked_ids: set[str] = set()
        removed_ids: set[str] = set()
        for record_id in record_ids:
            record = self._strategy_history_by_id.get(record_id)
            if record is None:
                continue
            session = self._session_by_history_record_id(record_id)
            if session is not None and self._session_blocks_history_deletion(session):
                blocked_count += 1
                blocked_ids.add(record_id)
                continue
            if session is not None:
                session.history_record_id = None
            self._strategy_history_by_id.pop(record_id, None)
            removed_count += 1
            removed_ids.add(record_id)
        if removed_count:
            self._strategy_history_records = [
                record for record in self._strategy_history_records if record.record_id not in set(record_ids) - blocked_ids
            ]
            if removed_ids:
                self._strategy_trade_ledger_records = [
                    record
                    for record in self._strategy_trade_ledger_records
                    if record.history_record_id not in removed_ids
                ]
                self._strategy_trade_ledger_by_id = {
                    record.record_id: record for record in self._strategy_trade_ledger_records
                }
                self._save_strategy_trade_ledger_records()
            self._save_strategy_history_records()
        self._render_strategy_history_view(selected_before=selected_before)
        return removed_count, blocked_count

    def delete_selected_strategy_history_record(self) -> None:
        parent = self._strategy_history_window if _widget_exists(self._strategy_history_window) else self.root
        record = self._selected_strategy_history_record()
        if record is None:
            messagebox.showinfo("提示", "请先在历史策略列表中选中一条记录。", parent=parent)
            return
        session = self._session_by_history_record_id(record.record_id)
        if session is not None and self._session_blocks_history_deletion(session):
            messagebox.showinfo(
                "提示",
                "这条历史记录对应的策略仍在运行或停止中，暂时不能删除。\n请先在运行中策略列表处理完成后再删。",
                parent=parent,
            )
            return
        confirmed = messagebox.askyesno(
            "确认删除",
            "确认删除当前选中的历史策略记录吗？\n\n只删除历史记录，不删除独立日志文件。",
            parent=parent,
        )
        if not confirmed:
            return
        removed_count, _ = self._remove_strategy_history_records([record.record_id], selected_before=record.record_id)
        if removed_count:
            self._enqueue_log(f"[历史策略 {record.record_id}] 已删除选中历史记录；独立日志文件保留。")

    def clear_strategy_history_records(self) -> None:
        parent = self._strategy_history_window if _widget_exists(self._strategy_history_window) else self.root
        if not self._strategy_history_records:
            messagebox.showinfo("提示", "当前没有可清空的历史策略记录。", parent=parent)
            return
        deletable_count = 0
        for record in self._strategy_history_records:
            session = self._session_by_history_record_id(record.record_id)
            if session is None or not self._session_blocks_history_deletion(session):
                deletable_count += 1
        if deletable_count <= 0:
            messagebox.showinfo(
                "提示",
                "当前历史列表里的记录都仍被运行中的策略占用，暂时不能清空。",
                parent=parent,
            )
            return
        confirmed = messagebox.askyesno(
            "确认清空",
            f"确认清空 {deletable_count} 条历史策略记录吗？\n\n运行中的策略记录会保留，独立日志文件不会删除。",
            parent=parent,
        )
        if not confirmed:
            return
        selected_before = self._strategy_history_selected_record_id
        record_ids = [record.record_id for record in self._strategy_history_records]
        removed_count, blocked_count = self._remove_strategy_history_records(record_ids, selected_before=selected_before)
        if removed_count:
            message = f"已清空 {removed_count} 条历史策略记录；独立日志文件保留。"
            if blocked_count:
                message += f" 另有 {blocked_count} 条运行中的记录已保留。"
            self._enqueue_log(message)

    def _selected_strategy_history_log_path(self) -> Path | None:
        record = self._selected_strategy_history_record()
        if record is None:
            return None
        return _coerce_log_file_path(record.log_file_path)

    def copy_selected_strategy_history_log_path(self) -> None:
        parent = self._strategy_history_window if _widget_exists(self._strategy_history_window) else self.root
        record = self._selected_strategy_history_record()
        if record is None:
            messagebox.showinfo("提示", "请先在历史策略列表中选中一条记录。", parent=parent)
            return
        log_path = self._selected_strategy_history_log_path()
        if log_path is None:
            messagebox.showinfo("提示", "这条历史策略还没有记录独立日志路径。", parent=parent)
            return
        self.root.clipboard_clear()
        self.root.clipboard_append(str(log_path))
        self._enqueue_log(f"[历史策略 {record.record_id}] 已复制独立日志路径：{log_path}")

    def open_selected_strategy_history_log(self) -> None:
        parent = self._strategy_history_window if _widget_exists(self._strategy_history_window) else self.root
        record = self._selected_strategy_history_record()
        if record is None:
            messagebox.showinfo("提示", "请先在历史策略列表中选中一条记录。", parent=parent)
            return
        log_path = self._selected_strategy_history_log_path()
        if log_path is None:
            messagebox.showinfo("提示", "这条历史策略还没有记录独立日志路径。", parent=parent)
            return
        if not log_path.exists():
            messagebox.showerror("打开失败", f"日志文件不存在：\n{log_path}", parent=parent)
            return
        startfile = getattr(os, "startfile", None)
        if not callable(startfile):
            messagebox.showerror("打开失败", "当前系统不支持直接打开日志文件。", parent=parent)
            return
        startfile(str(log_path))
        self._enqueue_log(f"[历史策略 {record.record_id}] 已打开独立日志：{log_path}")

    def _on_strategy_history_selected(self, *_: object) -> None:
        record = self._selected_strategy_history_record()
        self._strategy_history_selected_record_id = record.record_id if record is not None else None
        self._refresh_selected_strategy_history_details()

    def _refresh_selected_strategy_history_details(self) -> None:
        record = self._selected_strategy_history_record()
        if record is None:
            self.strategy_history_text.set(self._default_strategy_history_text())
            self._set_readonly_text(self._strategy_history_detail, self.strategy_history_text.get())
            return
        self.strategy_history_text.set(
            self._build_strategy_detail_text(
                record_id=record.record_id,
                session_id=record.session_id,
                api_name=record.api_name,
                status=record.status,
                strategy_id=record.strategy_id,
                strategy_name=record.strategy_name,
                symbol=record.symbol,
                direction_label=record.direction_label,
                run_mode_label=record.run_mode_label,
                started_at=record.started_at,
                stopped_at=record.stopped_at,
                ended_reason=record.ended_reason,
                updated_at=record.updated_at,
                config_snapshot=record.config_snapshot,
                log_file_path=record.log_file_path,
                trade_count=record.trade_count,
                win_count=record.win_count,
                gross_pnl_total=record.gross_pnl_total,
                fee_total=record.fee_total,
                funding_total=record.funding_total,
                net_pnl_total=record.net_pnl_total,
                last_close_reason=record.last_close_reason,
            )
        )
        self._set_readonly_text(self._strategy_history_detail, self.strategy_history_text.get())

    def _render_strategy_history_view(self, *, selected_before: str | None = None) -> None:
        tree = self._strategy_history_tree
        if tree is None or not _widget_exists(tree):
            return
        if selected_before is None:
            try:
                current_selection = tree.selection()
                selected_before = current_selection[0] if current_selection else self._strategy_history_selected_record_id
            except TclError:
                selected_before = self._strategy_history_selected_record_id
        for item_id in tree.get_children():
            tree.delete(item_id)
        for record in self._strategy_history_records:
            tree.insert(
                "",
                END,
                iid=record.record_id,
                values=(
                    record.session_id or "-",
                    record.api_name or "-",
                    record.strategy_name,
                    record.symbol,
                    record.direction_label,
                    record.run_mode_label,
                    _format_optional_usdt_precise(record.net_pnl_total, places=2),
                    record.status,
                    _format_history_datetime(record.started_at),
                    _format_history_datetime(record.stopped_at),
                ),
            )
        remaining_ids = tuple(record.record_id for record in self._strategy_history_records)
        target = self._next_history_selection_after_mutation(selected_before, remaining_ids)
        if target is not None and tree.exists(target):
            tree.selection_set(target)
            tree.focus(target)
            tree.see(target)
            self._strategy_history_selected_record_id = target
        else:
            self._strategy_history_selected_record_id = None
        self._refresh_selected_strategy_history_details()

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
        slot = self._trader_desk_slot_for_session(session.session_id)
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

    def _refresh_trader_desk_runtime(self) -> None:
        for run in list(self._trader_desk_runs):
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

    def _on_close(self) -> None:
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
        self._close_settings_window()
        if self._backtest_window is not None and self._backtest_window.window.winfo_exists():
            self._backtest_window.window.destroy()
        if self._backtest_compare_window is not None and self._backtest_compare_window.window.winfo_exists():
            self._backtest_compare_window.window.destroy()
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
        _format_optional_usdt(metrics["realized_usdt"] if isinstance(metrics["realized_usdt"], Decimal) else None),
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
    currencies = {_infer_position_history_pnl_currency(item) for item in items if item.realized_pnl is not None}
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


def _format_okx_ms_timestamp(timestamp_ms: int | None) -> str:
    if timestamp_ms is None or timestamp_ms <= 0:
        return "-"
    try:
        return datetime.fromtimestamp(timestamp_ms / 1000).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return "-"


def _format_history_side(side: str | None, pos_side: str | None) -> str:
    parts = [part for part in (side, pos_side) if part and part.lower() != "net"]
    return " / ".join(parts) if parts else (side or pos_side or "-")


def _history_tree_index(item_id: str, prefix: str) -> int | None:
    marker = f"{prefix}-"
    if not item_id.startswith(marker):
        return None
    try:
        return int(item_id[len(marker) :])
    except ValueError:
        return None


def _filter_position_history_items(
    items: list[OkxPositionHistoryItem],
    *,
    inst_type: str = "",
    margin_mode: str = "",
    asset: str = "",
    expiry_prefix: str = "",
    keyword: str = "",
    note_texts_by_index: dict[int, str] | None = None,
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
        asset_currency = _extract_asset_key(inst_id).upper()
        return size, asset_currency if asset_currency else None

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
) -> str:
    return _format_history_amount(value, _infer_position_history_pnl_currency(item), with_sign=with_sign)


def _format_fill_history_pnl(item: OkxFillHistoryItem) -> str:
    return _format_history_amount(item.pnl, _infer_fill_history_pnl_currency(item), with_sign=True)


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
        f"手续费：{_format_optional_decimal(item.fill_fee)} {item.fee_currency or ''}\n"
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
        f"盈亏：{_format_position_history_pnl(item.pnl, item)}\n"
        f"已实现盈亏：{_format_position_history_pnl(item.realized_pnl, item, with_sign=True)}\n"
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


def run_app() -> None:
    app = QuantApp()
    app.root.mainloop()
