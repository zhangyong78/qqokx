from __future__ import annotations

import json
import hashlib
import os
import queue
import subprocess
import sys
import re
import threading
import time
import tempfile
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
import uuid
from dataclasses import MISSING, asdict, dataclass, field, fields as dataclass_fields, replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from types import FunctionType
import tkinter.font as tkfont
from tkinter import BooleanVar, Canvas, END, Label, Listbox, Menu, StringVar, Text, TclError, Tk, Toplevel, filedialog, simpledialog
from tkinter import messagebox, ttk

from okx_quant.app_paths import configured_data_root, data_root
from okx_quant.client_order_id import CUSTOM_ORDER_ID_PREFIX, strategy_order_identity, with_custom_order_id_prefix
from okx_quant.app_meta import APP_VERSION, build_app_title, build_version_info_text
from okx_quant.analysis import (
    BoxDetectionConfig,
    ChannelDetectionConfig,
    PivotDetectionConfig,
    TrendlineDetectionConfig,
    TriangleDetectionConfig,
)
from okx_quant.backtest_ui import BacktestCompareOverviewWindow, BacktestLaunchState, BacktestWindow
from okx_quant.btc_market_analysis_ui import BtcMarketAnalysisWindow
from okx_quant.btc_research_workbench_ui import BtcResearchWorkbenchWindow
from okx_quant.deribit_client import DeribitRestClient
from okx_quant.deribit_volatility_monitor_ui import DeribitVolatilityMonitorWindow
from okx_quant.deribit_volatility_ui import DeribitVolatilityWindow
from okx_quant.email_schedule_manager_ui import EmailScheduleManagerWindow
from okx_quant.daily_filters import daily_boundary_anchor_offset_ms
from okx_quant.daily_trade_report import (
    DailyTrade,
    build_daily_trade_report,
    daily_trade_from_strategy_ledger,
    format_report_decimal,
    report_to_csv,
    report_to_html,
)
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
    append_log_line,
    append_preformatted_log_line,
    current_log_timestamp,
    daily_log_file_path,
    ensure_log_timestamp,
    logs_dir,
    read_daily_log_tail,
    strategy_session_log_file_path,
)
from okx_quant.market_data_hub import MarketDataHub
from okx_quant.models import (
    Candle,
    Credentials,
    DynamicProtectionRule,
    EmailNotificationConfig,
    Instrument,
    OrderPlan,
    ProtectionPlan,
    StrategyConfig,
    build_legacy_dynamic_protection_rules,
    describe_dynamic_protection_rule_overlap_warnings,
    describe_dynamic_protection_rules,
    dynamic_protection_rules_to_payload,
    merge_dynamic_protection_rules,
    normalize_dynamic_protection_rules,
)
from okx_quant.minimum_risk_recommendations import format_risk_recommendation, recommended_minimum_risk_amount_for_strategy
from okx_quant.duration_input import (
    format_duration_cn_compact,
    parse_nonnegative_duration_seconds,
    try_parse_nonnegative_duration_seconds,
)
from okx_quant.notifications import EmailNotifier
from okx_quant.option_roll import is_short_option_position
from okx_quant.option_roll_ui import OptionRollSuggestionWindow, _build_option_quote
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
    build_profile_switch_password_snapshot,
    credential_profile_has_switch_password,
    credentials_file_path,
    DEFAULT_CREDENTIAL_PROFILE_NAME,
    load_account_equity_curve_records,
    load_history_cache_records,
    load_position_history_view_prefs,
    load_recoverable_strategy_sessions_snapshot,
    load_credentials_profiles_snapshot,
    load_notification_snapshot,
    load_position_notes_snapshot,
    load_strategy_parameter_drafts,
    load_strategy_history_snapshot,
    load_strategy_trade_ledger_snapshot,
    save_recoverable_strategy_sessions_snapshot,
    save_credentials_profiles_snapshot,
    save_account_equity_curve_records,
    save_notification_snapshot,
    save_history_cache_records,
    save_position_history_view_prefs,
    save_position_notes_snapshot,
    save_strategy_parameter_drafts,
    save_strategy_history_snapshot,
    settings_file_path,
    strategy_history_file_path,
    strategy_trade_ledger_file_path,
    save_strategy_trade_ledger_snapshot,
    verify_profile_switch_password,
)
from okx_quant.position_protection import (
    OptionProtectionConfig,
    PositionProtectionManager,
    build_close_order_price_from_mark,
    describe_protection_price_logic,
    derive_position_direction,
    evaluate_protection_trigger,
    infer_protection_profit_on_rise,
    infer_default_spot_inst_id,
    normalize_spot_inst_id,
    validate_live_protection_order_price_guard,
)
from okx_quant.protection_replay_ui import ProtectionReplayLaunchState, ProtectionReplayWindow
from okx_quant.pricing import format_decimal, format_decimal_by_increment, format_decimal_fixed, snap_to_increment
from okx_quant.stop_execution import assess_stop_execution
from okx_quant.strategy_profiles import StrategyProfile, build_strategy_profile_from_config
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
from okx_quant.semi_auto_desk import (
    SemiAutoDeskSnapshot,
    SemiAutoPoolRecord,
    SemiAutoTaskRecord,
    build_semi_auto_pool_summary,
    load_semi_auto_desk_snapshot,
    save_semi_auto_desk_snapshot,
)
from okx_quant.semi_auto_desk_ui import SemiAutoDeskWindow, build_semi_auto_pool_replay_time_markers
from okx_quant.semi_auto_strategy_library_ui import SemiAutoStrategyLibraryDialog
from okx_quant.strategy_live_chart import (
    DEFAULT_STRATEGY_LIVE_CHART_CANDLE_LIMIT,
    DEFAULT_STRATEGY_LIVE_CHART_REFRESH_MS,
    StrategyLiveChartLayout,
    StrategyLiveChartSnapshot,
    StrategyLiveChartTimeMarker,
    append_candles_to_snapshot,
    build_strategy_live_chart_snapshot,
    compute_strategy_live_chart_layout,
    layout_bar_index_to_x_center,
    layout_pixel_to_bar_index,
    layout_price_to_y_clamped,
    layout_price_to_y_unclamped,
    layout_y_to_price,
    render_strategy_live_chart,
)
from okx_quant.strategies.ema_atr import EmaAtrStrategy
from okx_quant.strategies.ema_cross_ema_stop import EmaCrossEmaStopStrategy
from okx_quant.strategies.ema_dynamic import EmaDynamicOrderStrategy
from okx_quant.strategies.ema_dynamic_multi_timeframe import EmaDynamicMultiTimeframeStrategy
from okx_quant.strategies.ema55_slope_short import evaluate_ema55_slope_short_signal
from okx_quant.upgrade_launch import (
    UPGRADE_CUSTOM_EXECUTABLE_NAME,
    UPGRADE_LAUNCH_MODE_AUTO,
    UPGRADE_LAUNCH_MODE_CUSTOM,
    UPGRADE_LAUNCH_MODE_NONE,
    UpgradeLaunchManager,
    UpgradeLaunchPlan,
)
from okx_quant.strategy_catalog import (
    BACKTEST_STRATEGY_DEFINITIONS,
    STRATEGY_BTC_EMA55_SLOPE_SHORT_ID,
    STRATEGY_DEFINITIONS,
    STRATEGY_DYNAMIC_ID,
    STRATEGY_EMA55_SLOPE_SHORT_ID,
    StrategyDefinition,
    get_strategy_definition,
    is_ema55_slope_short_strategy,
    resolve_dynamic_signal_mode,
    supports_startup_chase_current_signal,
    supports_trader_desk,
    supports_signal_only,
)
from okx_quant.strategy_parameters import (
    iter_strategy_parameter_keys,
    strategy_fixed_value,
    strategy_is_parameter_editable,
    strategy_uses_parameter,
)
from okx_quant.strategy_symbol_defaults import get_strategy_symbol_parameter_defaults
from okx_quant.strategy_ui_schema import (
    build_strategy_widget_visibility,
    strategy_forces_follow_signal,
    strategy_forces_local_trade,
    strategy_parameter_default_for_scope,
    strategy_supports_dynamic_take_profit,
    strategy_ui_extra_defaults,
    strategy_ui_fixed_extra_value,
    strategy_uses_startup_chase_window,
)
from okx_quant.strategy_runtime_registry import (
    get_strategy_runtime_profile,
    strategy_entry_reference_caption,
    strategy_entry_reference_period_caption,
    strategy_preferred_direction,
    strategy_uses_mtf_filter,
    strategy_uses_signal_extrema,
)
from okx_quant.strategy_status_email import (
    StrategyStatusEmailRow,
    build_strategy_status_email,
    claim_status_email_slot,
    latest_due_status_email_slot,
)
from okx_quant.window_layout import (
    apply_adaptive_window_geometry,
    apply_fill_window_geometry,
    apply_window_icon,
)
from okx_quant.ui_backtest_entry import UiBacktestEntryMixin
from okx_quant.ui_positions import UiPositionsMixin
from okx_quant.ui_protection import UiProtectionMixin
from okx_quant.ui_strategy_sessions import (
    UiStrategySessionsMixin,
    _SESSION_RUNTIME_HEARTBEAT_PREFIX,
    _SESSION_RUNTIME_HEARTBEAT_TIMEOUT_MIN_SECONDS,
    _SESSION_RUNTIME_HEARTBEAT_TIMEOUT_POLLS,
)


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


def _strategy_fast_line_caption(strategy_id: str) -> str:
    if strategy_id in {STRATEGY_BTC_EMA55_SLOPE_SHORT_ID, STRATEGY_EMA55_SLOPE_SHORT_ID}:
        return "信号均线（斜率开平仓）"
    return "快线均线"


def _build_app_restart_command(
    *,
    executable: str | None = None,
    argv0: str | None = None,
    frozen: bool | None = None,
    data_dir: str | Path | None = None,
) -> list[str]:
    resolved_executable = str(Path(executable or sys.executable).resolve())
    is_frozen = bool(getattr(sys, "frozen", False) if frozen is None else frozen)
    command = [resolved_executable]
    if not is_frozen:
        resolved_argv0 = str(Path(argv0 or sys.argv[0]).resolve())
        command.append(resolved_argv0)
    resolved_data_dir = Path(data_dir).resolve() if data_dir is not None else (configured_data_root() or data_root())
    command.extend(["--data-dir", str(resolved_data_dir)])
    return command


def _app_restart_workdir(*, argv0: str | None = None, frozen: bool | None = None) -> str:
    is_frozen = bool(getattr(sys, "frozen", False) if frozen is None else frozen)
    target = Path(sys.executable if is_frozen else (argv0 or sys.argv[0])).resolve()
    return str(target.parent)



def _ps_quote(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _build_daily_filter_boundary_time_markers(
    candles: list[Candle] | tuple[Candle, ...],
    *,
    boundary: str,
    key_prefix: str,
    max_markers: int = 16,
) -> tuple[StrategyLiveChartTimeMarker, ...]:
    normalized_boundary = str(boundary or "exchange").strip().lower()
    if not candles:
        return ()
    labels = {
        "exchange": "1D",
        "bjt_00": "BJT 00",
        "bjt_08": "BJT 08",
    }
    offset_ms = daily_boundary_anchor_offset_ms(normalized_boundary)  # type: ignore[arg-type]
    day_ms = 86_400_000
    markers: list[StrategyLiveChartTimeMarker] = []
    previous_bucket: int | None = None
    for candle in candles:
        candle_ts = int(candle.ts)
        bucket_open_ts = ((candle_ts - offset_ms) // day_ms) * day_ms + offset_ms
        if previous_bucket is None or bucket_open_ts != previous_bucket:
            marker_at = datetime.fromtimestamp(candle_ts / 1000)
            markers.append(
                StrategyLiveChartTimeMarker(
                    key=f"{key_prefix}:{bucket_open_ts}",
                    label=labels.get(normalized_boundary, normalized_boundary),
                    at=marker_at,
                    color="#7c3aed",
                    dash=(2, 6),
                    width=1,
                )
            )
            previous_bucket = bucket_open_ts
    return tuple(markers[-max_markers:])


def _ps_array(values: list[str]) -> str:
    return "@(" + ", ".join(_ps_quote(value) for value in values) + ")"


def _build_upgrade_launch_log_message(plan: UpgradeLaunchPlan) -> str:
    if plan.mode == UPGRADE_LAUNCH_MODE_NONE:
        return "升级完成后不启动任何程序。"
    if plan.mode == UPGRADE_LAUNCH_MODE_CUSTOM:
        return f"升级完成后启动指定版本：{plan.resolved_executable or '-'}"
    return "升级完成后自动启动当前版本。"


def _build_upgrade_launch_worker_script(
    *,
    wait_pid: int,
    plan: UpgradeLaunchPlan,
    log_file_path: Path,
) -> str:
    launch_command = list(plan.command or [])
    working_directory = plan.working_directory or ""
    return f"""$ErrorActionPreference = 'Stop'
$waitPid = {int(wait_pid)}
$launchMode = {_ps_quote(plan.mode)}
$launchCommand = {_ps_array(launch_command)}
$launchWorkingDirectory = {_ps_quote(working_directory)}
$logFile = {_ps_quote(str(log_file_path.resolve()))}

function Write-UpgradeLaunchLog([string]$message) {{
    $timestamp = Get-Date -Format 'MM-dd HH:mm:ss'
    $line = "[$timestamp] $message"
    $logDir = Split-Path -Parent $logFile
    if ($logDir) {{
        New-Item -ItemType Directory -Force -Path $logDir | Out-Null
    }}
    Add-Content -LiteralPath $logFile -Value $line -Encoding UTF8
}}

for ($attempt = 0; $attempt -lt 240; $attempt++) {{
    if (-not (Get-Process -Id $waitPid -ErrorAction SilentlyContinue)) {{
        break
    }}
    Start-Sleep -Milliseconds 500
}}
if (Get-Process -Id $waitPid -ErrorAction SilentlyContinue) {{
    throw "等待旧进程退出超时：PID=$waitPid"
}}

if ($launchMode -eq 'none') {{
    Write-UpgradeLaunchLog '升级完成后按配置不启动程序。'
    exit 0
}}

if (-not $launchCommand -or $launchCommand.Count -eq 0) {{
    throw '升级启动命令为空。'
}}

$exe = $launchCommand[0]
$args = @()
if ($launchCommand.Count -gt 1) {{
    $args = $launchCommand[1..($launchCommand.Count - 1)]
}}

try {{
    Start-Process -FilePath $exe -ArgumentList $args -WorkingDirectory $launchWorkingDirectory -WindowStyle Hidden
    Write-UpgradeLaunchLog ("升级完成后启动成功：模式=" + $launchMode + " | 目标=" + $exe)
}} catch {{
    Write-UpgradeLaunchLog ("升级完成后启动失败：模式=" + $launchMode + " | 目标=" + $exe + " | 原因=" + $_.Exception.Message)
    throw
}}
"""


def _build_upgrade_launch_worker_command(
    *,
    plan: UpgradeLaunchPlan,
    log_file_path: Path,
) -> list[str] | None:
    import tempfile as _tempfile

    if not plan.should_launch and plan.mode != UPGRADE_LAUNCH_MODE_NONE:
        raise ValueError("升级启动计划无效。")
    worker_dir = Path(_tempfile.mkdtemp(prefix="qqokx-launch-worker-"))
    script_path = worker_dir / "run_upgrade_launch.ps1"
    script_path.write_text(
        _build_upgrade_launch_worker_script(
            wait_pid=os.getpid(),
            plan=plan,
            log_file_path=log_file_path,
        ),
        encoding="utf-8",
    )
    return [
        "powershell",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str(script_path),
    ]


def _build_upgrade_confirmation_message(
    *,
    running_count: int,
    migratable_count: int,
    unsupported_count: int,
    data_dir: str | Path,
) -> str:
    resolved_data_dir = str(Path(data_dir).resolve())
    lines = [
        "程序会关闭当前窗口并自动拉起新进程。",
        "",
        "升级前请先确认：新版本代码已经覆盖到当前程序目录。",
        "如果代码还没更新，现在取消即可；否则重新打开的仍会是旧版本。",
        "",
        f"共享数据目录：{resolved_data_dir}",
    ]
    if running_count > 0:
        lines.extend(
            [
                "",
                f"当前检测到 {running_count} 条运行中策略。",
                f"- 可自动迁移：{migratable_count} 条",
                f"- 不支持自动迁移：{unsupported_count} 条",
            ]
        )
        if migratable_count > 0:
            lines.append("可迁移策略里，有持仓/挂单的会继续接管，纯等待信号的会自动恢复监听。")
        if unsupported_count > 0:
            lines.append("不支持自动迁移的策略会先停止，需要升级后手工重新启动。")
    lines.extend(["", "现在开始程序升级吗？"])
    return "\n".join(lines)


UiBacktestEntryMixin = _bind_mixin_to_shell_globals(UiBacktestEntryMixin)
UiPositionsMixin = _bind_mixin_to_shell_globals(UiPositionsMixin)
UiProtectionMixin = _bind_mixin_to_shell_globals(UiProtectionMixin)
UiStrategySessionsMixin = _bind_mixin_to_shell_globals(UiStrategySessionsMixin)


BAR_OPTIONS = ["1m", "3m", "5m", "15m", "1H", "4H"]
DEFAULT_LAUNCH_SYMBOLS = ("BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP", "BNB-USDT-SWAP", "DOGE-USDT-SWAP")
PREFERRED_STARTUP_CREDENTIAL_PROFILE_NAME = "159"
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
        "trade_side",
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
        "spot_maker_fee_rate": "",
        "spot_taker_fee_rate": "",
        "futures_maker_fee_rate": "",
        "futures_taker_fee_rate": "",
        "option_maker_fee_rate": "",
        "option_taker_fee_rate": "",
        "switch_password_hash": "",
        "switch_password_salt": "",
        "switch_password_iterations": "",
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
MOVING_AVERAGE_TYPE_OPTIONS = ("EMA", "MA")
MTF_REVERSAL_MODE_OPTIONS = {
    "只过滤新开仓": "block_new_entries",
}
MTF_REVERSAL_MODE_VALUE_TO_LABEL = {value: label for label, value in MTF_REVERSAL_MODE_OPTIONS.items()}
DAILY_FILTER_BOUNDARY_LABEL_TO_VALUE = {
    "Exchange 1D": "exchange",
    "BJT 00:00": "bjt_00",
    "BJT 08:00": "bjt_08",
}
DAILY_FILTER_BOUNDARY_VALUE_TO_LABEL = {
    value: label for label, value in DAILY_FILTER_BOUNDARY_LABEL_TO_VALUE.items()
}
DAILY_FILTER_MODE_LABEL_TO_VALUE = {
    "Disabled": "disabled",
    "close vs MA/EMA": "close_vs_ma",
    "Weak Day": "weak_day",
}
DAILY_FILTER_MODE_VALUE_TO_LABEL = {value: label for label, value in DAILY_FILTER_MODE_LABEL_TO_VALUE.items()}
DAILY_FILTER_SCOPE_LABEL_TO_VALUE = {
    "Both": "both",
    "Long Only": "long_only",
    "Short Only": "short_only",
}
DAILY_FILTER_SCOPE_VALUE_TO_LABEL = {
    value: label for label, value in DAILY_FILTER_SCOPE_LABEL_TO_VALUE.items()
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
ENGINE_CL_ORD_ID_PATTERN = re.compile(
    rf"^(?:{CUSTOM_ORDER_ID_PREFIX.lower()})?[a-z0-9]{{4,8}}(ent|exi|slg)[a-z0-9]{{5,15}}$"
)
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
# 非前台 API 只承担运行中策略的仓位展示核对。保持低频，避免把
# 500ms 的界面状态刷新循环放大成账户 REST 轮询。
RUNNING_SESSION_POSITION_SNAPSHOT_MAX_AGE_SECONDS = 60
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
    ws_cache_note: str = ""


@dataclass
class AccountOverviewCacheEntry:
    profile_name: str
    environment: str
    overview: OkxAccountOverview
    config: OkxAccountConfig | None = None
    refreshed_at: datetime | None = None
    source: str = "rest"


@dataclass
class AccountEquityCurveWindowState:
    profile_name: str
    environment: str
    window: Toplevel
    canvas: Canvas
    summary_text: StringVar
    range_var: StringVar
    event_mode_var: StringVar
    detail_text: StringVar
    selected_trade_record_ids: tuple[str, ...] = ()
    symbol_colors: dict[str, str] = field(default_factory=dict)


@dataclass
class StrategyTradeRuntimeState:
    round_id: str
    signal_bar_at: datetime | None = None
    opened_logged_at: datetime | None = None
    closed_logged_at: datetime | None = None
    entry_order_id: str = ""
    entry_client_order_id: str = ""
    exit_order_id: str = ""
    entry_price: Decimal | None = None
    exit_price: Decimal | None = None
    size: Decimal | None = None
    exit_size: Decimal | None = None
    pending_entry_reference: Decimal | None = None
    pending_stop_price: Decimal | None = None
    pending_take_profit: Decimal | None = None
    pending_side: str = ""
    pending_signal: str = ""
    protective_algo_id: str = ""
    protective_algo_cl_ord_id: str = ""
    initial_stop_price: Decimal | None = None
    current_stop_price: Decimal | None = None
    management_mode: str = "auto"
    manual_reason: str = ""
    manual_override_stop_price: Decimal | None = None
    manual_override_at: datetime | None = None
    close_reason_hint: str = ""
    reconciliation_started: bool = False
    stop_execution_status: str = ""
    planned_risk_usdt: Decimal | None = None
    actual_price_loss_usdt: Decimal | None = None
    effective_stop_price_at_exit: Decimal | None = None
    stop_slippage_price: Decimal | None = None
    stop_slippage_usdt: Decimal | None = None
    stop_overrun_usdt: Decimal | None = None
    stop_overrun_pct: Decimal | None = None


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
    round_id: str = ""
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
    planned_initial_risk_usdt: Decimal | None = None
    actual_price_loss_usdt: Decimal | None = None
    effective_stop_price_at_exit: Decimal | None = None
    stop_slippage_price: Decimal | None = None
    stop_slippage_usdt: Decimal | None = None
    stop_overrun_usdt: Decimal | None = None
    stop_overrun_pct: Decimal | None = None
    stop_execution_status: str = ""
    updated_at: datetime | None = None
    semi_auto_pool_id: str = ""
    semi_auto_task_id: str = ""
    strategy_group_id: str = ""


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
    last_message_at: datetime | None = None
    last_runtime_heartbeat_at: datetime | None = None
    last_runtime_heartbeat_label: str = ""
    runtime_alert: str = ""
    runtime_heartbeat_timeout_logged_at: datetime | None = None
    recovery_root_dir: Path | None = None
    recovery_supported: bool = False
    active_trade: StrategyTradeRuntimeState | None = None
    trade_count: int = 0
    win_count: int = 0
    gross_pnl_total: Decimal = Decimal("0")
    fee_total: Decimal = Decimal("0")
    funding_total: Decimal = Decimal("0")
    net_pnl_total: Decimal = Decimal("0")
    last_net_pnl: Decimal | None = None
    last_close_reason: str = ""
    trader_id: str = ""
    trader_slot_id: str = ""
    semi_auto_pool_id: str = ""
    semi_auto_task_id: str = ""
    semi_auto_mode: str = ""
    email_notifications_enabled: bool = True
    strategy_group_id: str = ""
    pause_after_cleanup: bool = False
    market_condition_paused: bool = False
    market_condition_last_allowed: bool | None = None
    market_condition_last_checked_at: datetime | None = None
    market_condition_last_note: str = ""

    @property
    def log_prefix(self) -> str:
        if self.api_name:
            return f"[{self.api_name}] [{self.session_id} {self.strategy_name} {self.symbol}]"
        return f"[{self.session_id} {self.strategy_name} {self.symbol}]"

    @property
    def display_status(self) -> str:
        if self.status == "运行中" and self.runtime_alert:
            return self.runtime_alert
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
    hover_tip_window: Toplevel | None = None
    hover_tip_label: ttk.Label | None = None
    hover_tip_marker_key: str = ""


@dataclass
class LiveChartLineAnnotation:
    kind: str
    x1: float
    y1: float
    x2: float
    y2: float
    color: str
    label: str = ""




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
    ignored_opposite_position_summaries: tuple[str, ...] = ()
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
    last_net_pnl: Decimal | None = None
    last_close_reason: str = ""
    strategy_group_id: str = ""


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


@dataclass(frozen=True)
class StrategyTemplateBundleItem:
    record: StrategyTemplateRecord
    total_quota: Decimal = Decimal("1")
    unit_quota: Decimal = Decimal("0.1")
    quota_steps: int = 10
    status: str = "draft"
    auto_restart_on_profit: bool = True
    pause_on_stop_loss: bool = True
    notes: str = ""


@dataclass(frozen=True)
class StrategyTemplateBundleImport:
    package_name: str
    items: tuple[StrategyTemplateBundleItem, ...]
    auto_start_on_import: bool = False


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
        elif item.name == "dynamic_protection_rules":
            snapshot[item.name] = dynamic_protection_rules_to_payload(value)
        else:
            snapshot[item.name] = value
    return snapshot


def build_strategy_group_id(
    *,
    api_name: str,
    strategy_id: str,
    strategy_name: str,
    symbol: str,
    direction_label: str,
    run_mode_label: str,
    config_snapshot: dict[str, object] | None = None,
    trader_id: str = "",
) -> str:
    """Return a stable accounting identity for repeated runs of one deployment.

    The API profile is intentionally part of the identity.  Two otherwise
    identical strategies on different accounts must never share positions or
    PnL totals.  The full config snapshot also separates parameter versions.
    """
    identity = {
        "api_name": str(api_name or "").strip(),
        "strategy_id": str(strategy_id or "").strip(),
        "strategy_name": str(strategy_name or "").strip(),
        "symbol": str(symbol or "").strip().upper(),
        "direction_label": str(direction_label or "").strip(),
        "run_mode_label": str(run_mode_label or "").strip(),
        "trader_id": str(trader_id or "").strip(),
        "config_snapshot": dict(config_snapshot or {}),
    }
    canonical = json.dumps(identity, ensure_ascii=False, sort_keys=True, default=str, separators=(",", ":"))
    return f"sg-{hashlib.sha256(canonical.encode('utf-8')).hexdigest()[:20]}"


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
        ema_type=_coerce_snapshot_text(payload.get("ema_type"), "ema"),
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
        trend_ema_type=_coerce_snapshot_text(payload.get("trend_ema_type"), "ema"),
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
            minimum=0,
        ),
        reentry_confirmation_enabled=_coerce_snapshot_bool(
            payload.get("reentry_confirmation_enabled"),
            bool(_strategy_config_default("reentry_confirmation_enabled")),
        ),
        reentry_confirmation_min_sequence=_coerce_snapshot_int(
            payload.get("reentry_confirmation_min_sequence"),
            int(_strategy_config_default("reentry_confirmation_min_sequence")),
            minimum=0,
        ),
        reentry_confirmation_ma_type=_coerce_snapshot_text(
            payload.get("reentry_confirmation_ma_type"),
            str(_strategy_config_default("reentry_confirmation_ma_type")),
        ),
        reentry_confirmation_ma_period=_coerce_snapshot_int(
            payload.get("reentry_confirmation_ma_period"),
            int(_strategy_config_default("reentry_confirmation_ma_period")),
            minimum=1,
        ),
        entry_reference_ema_period=_coerce_snapshot_int(
            payload.get("entry_reference_ema_period"),
            int(_strategy_config_default("entry_reference_ema_period")),
            minimum=0,
        ),
        entry_reference_ema_type=_coerce_snapshot_text(
            payload.get("entry_reference_ema_type"),
            _coerce_snapshot_text(payload.get("ema_type"), "ema"),
        ),
        dynamic_two_r_break_even=_coerce_snapshot_bool(
            payload.get("dynamic_two_r_break_even"),
            bool(_strategy_config_default("dynamic_two_r_break_even")),
        ),
        dynamic_break_even_trigger_r=_coerce_snapshot_int(
            payload.get("dynamic_break_even_trigger_r"),
            int(_strategy_config_default("dynamic_break_even_trigger_r")),
            minimum=1,
        ),
        ema55_slope_exit_enabled=_coerce_snapshot_bool(
            payload.get("ema55_slope_exit_enabled"),
            bool(_strategy_config_default("ema55_slope_exit_enabled")),
        ),
        ema55_slope_lock_profit_enabled=_coerce_snapshot_bool(
            payload.get("ema55_slope_lock_profit_enabled"),
            bool(_strategy_config_default("ema55_slope_lock_profit_enabled")),
        ),
        ema55_slope_lock_profit_trigger_r=_coerce_snapshot_int(
            payload.get("ema55_slope_lock_profit_trigger_r"),
            int(_strategy_config_default("ema55_slope_lock_profit_trigger_r")),
            minimum=2,
        ),
        ema55_slope_negative_entry_bars=_coerce_snapshot_int(
            payload.get("ema55_slope_negative_entry_bars"),
            int(_strategy_config_default("ema55_slope_negative_entry_bars")),
            minimum=1,
        ),
        dynamic_first_lock_r=_coerce_snapshot_int(
            payload.get("dynamic_first_lock_r"),
            int(_strategy_config_default("dynamic_first_lock_r")),
            minimum=0,
        ),
        dynamic_trailing_step_r=_coerce_snapshot_int(
            payload.get("dynamic_trailing_step_r"),
            int(_strategy_config_default("dynamic_trailing_step_r")),
            minimum=1,
        ),
        dynamic_protection_rules=normalize_dynamic_protection_rules(payload.get("dynamic_protection_rules")),
        dynamic_fee_offset_enabled=_coerce_snapshot_bool(
            payload.get("dynamic_fee_offset_enabled"),
            bool(_strategy_config_default("dynamic_fee_offset_enabled")),
        ),
        trend_ema_slope_filter_enabled=_coerce_snapshot_bool(
            payload.get("trend_ema_slope_filter_enabled"),
            bool(_strategy_config_default("trend_ema_slope_filter_enabled")),
        ),
        trend_ema_slope_filter_lookback_bars=_coerce_snapshot_int(
            payload.get("trend_ema_slope_filter_lookback_bars"),
            int(_strategy_config_default("trend_ema_slope_filter_lookback_bars")),
            minimum=2,
        ),
        trend_ema_slope_filter_min_ratio=_coerce_snapshot_decimal(
            payload.get("trend_ema_slope_filter_min_ratio"),
            Decimal(str(_strategy_config_default("trend_ema_slope_filter_min_ratio"))),
        ),
        atr_percentile_filter_max=_coerce_snapshot_decimal(
            payload.get("atr_percentile_filter_max"),
            Decimal(str(_strategy_config_default("atr_percentile_filter_max"))),
        ),
        body_retest_breakdown_atr_multiplier=_coerce_snapshot_decimal(
            payload.get("body_retest_breakdown_atr_multiplier"),
            Decimal(str(_strategy_config_default("body_retest_breakdown_atr_multiplier"))),
        ),
        body_retest_retest_atr_multiplier=_coerce_snapshot_decimal(
            payload.get("body_retest_retest_atr_multiplier"),
            Decimal(str(_strategy_config_default("body_retest_retest_atr_multiplier"))),
        ),
        body_retest_stop_buffer_atr_multiplier=_coerce_snapshot_decimal(
            payload.get("body_retest_stop_buffer_atr_multiplier"),
            Decimal(str(_strategy_config_default("body_retest_stop_buffer_atr_multiplier"))),
        ),
        body_retest_body_atr_limit=_coerce_snapshot_decimal(
            payload.get("body_retest_body_atr_limit"),
            Decimal(str(_strategy_config_default("body_retest_body_atr_limit"))),
        ),
        body_retest_watch_bars=_coerce_snapshot_int(
            payload.get("body_retest_watch_bars"),
            int(_strategy_config_default("body_retest_watch_bars")),
            minimum=0,
        ),
        startup_chase_current_signal=_coerce_snapshot_bool(
            payload.get("startup_chase_current_signal"),
            bool(_strategy_config_default("startup_chase_current_signal")),
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
        trend_ema_close_exit_after_trigger_r_enabled=_coerce_snapshot_bool(
            payload.get("trend_ema_close_exit_after_trigger_r_enabled"),
            bool(_strategy_config_default("trend_ema_close_exit_after_trigger_r_enabled")),
        ),
        trend_ema_close_exit_after_trigger_r=_coerce_snapshot_int(
            payload.get("trend_ema_close_exit_after_trigger_r"),
            int(_strategy_config_default("trend_ema_close_exit_after_trigger_r")),
            minimum=1,
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
        mtf_filter_inst_id=_coerce_snapshot_optional_text(payload.get("mtf_filter_inst_id")),
        mtf_filter_bar=_coerce_snapshot_optional_text(payload.get("mtf_filter_bar")),
        mtf_filter_fast_ema_period=_coerce_snapshot_int(
            payload.get("mtf_filter_fast_ema_period"),
            int(_strategy_config_default("mtf_filter_fast_ema_period")),
            minimum=1,
        ),
        mtf_filter_slow_ema_period=_coerce_snapshot_int(
            payload.get("mtf_filter_slow_ema_period"),
            int(_strategy_config_default("mtf_filter_slow_ema_period")),
            minimum=1,
        ),
        mtf_reversal_mode=_coerce_snapshot_text(
            payload.get("mtf_reversal_mode"),
            str(_strategy_config_default("mtf_reversal_mode")),
        ),
        daily_filter_inst_id=_coerce_snapshot_optional_text(payload.get("daily_filter_inst_id")),
        daily_filter_bar=_coerce_snapshot_optional_text(payload.get("daily_filter_bar")),
        daily_filter_boundary=_coerce_snapshot_text(
            payload.get("daily_filter_boundary"),
            str(_strategy_config_default("daily_filter_boundary")),
        ),
        daily_filter_enabled=_coerce_snapshot_bool(
            payload.get("daily_filter_enabled"),
            bool(_strategy_config_default("daily_filter_enabled")),
        ),
        daily_filter_mode=_coerce_snapshot_text(
            payload.get("daily_filter_mode"),
            str(_strategy_config_default("daily_filter_mode")),
        ),
        daily_filter_scope=_coerce_snapshot_text(
            payload.get("daily_filter_scope"),
            str(_strategy_config_default("daily_filter_scope")),
        ),
        daily_filter_ma_type=_coerce_snapshot_text(
            payload.get("daily_filter_ma_type"),
            str(_strategy_config_default("daily_filter_ma_type")),
        ),
        daily_filter_period=_coerce_snapshot_int(
            payload.get("daily_filter_period"),
            int(_strategy_config_default("daily_filter_period")),
            minimum=1,
        ),
        runtime_gate_enabled=_coerce_snapshot_bool(
            payload.get("runtime_gate_enabled"),
            bool(_strategy_config_default("runtime_gate_enabled")),
        ),
        runtime_gate_inst_id=_coerce_snapshot_optional_text(payload.get("runtime_gate_inst_id")),
        runtime_gate_bar=_coerce_snapshot_text(
            payload.get("runtime_gate_bar"),
            str(_strategy_config_default("runtime_gate_bar")),
        ),
        runtime_gate_ma_type=_coerce_snapshot_text(
            payload.get("runtime_gate_ma_type"),
            str(_strategy_config_default("runtime_gate_ma_type")),
        ),
        runtime_gate_period=_coerce_snapshot_int(
            payload.get("runtime_gate_period"),
            int(_strategy_config_default("runtime_gate_period")),
            minimum=0,
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


def _normalize_strategy_direction_label(strategy_id: str, config: StrategyConfig | None, fallback: str = "") -> str:
    if config is None:
        return str(fallback or "").strip()
    return _strategy_template_direction_label(strategy_id, config, fallback=fallback)


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
    strategy_profile = build_strategy_profile_from_config(
        profile_id=f"{session.strategy_id}:{session.symbol}:{session.api_name or '-'}",
        profile_name=f"{session.strategy_name} | {session.symbol}",
        strategy_id=session.strategy_id,
        symbol=session.symbol,
        config=session.config,
        api_name=session.api_name,
        direction_label=session.direction_label,
        run_mode_label=session.run_mode_label,
    )
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
        "strategy_profile": strategy_profile.to_payload(),
    }


def _build_strategy_template_payload_from_record(record: StrategyTemplateRecord) -> dict[str, object]:
    strategy_profile = build_strategy_profile_from_config(
        profile_id=f"{record.strategy_id}:{record.symbol}:{record.api_name or '-'}",
        profile_name=f"{record.strategy_name} | {record.symbol}",
        strategy_id=record.strategy_id,
        symbol=record.symbol,
        config=record.config,
        api_name=record.api_name,
        direction_label=record.direction_label,
        run_mode_label=record.run_mode_label,
    )
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
        "strategy_profile": strategy_profile.to_payload(),
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


def _strategy_template_payload_label(payload: dict[str, object]) -> str:
    strategy_name = str(payload.get("strategy_name") or "").strip()
    strategy_id = str(payload.get("strategy_id") or "").strip()
    symbol = str(payload.get("symbol") or "").strip().upper()
    core = strategy_name or strategy_id or "未命名策略"
    return f"{core} {symbol}".strip()


def _validate_trader_template_payload(payload: object) -> dict[str, object]:
    if not isinstance(payload, dict):
        raise ValueError("当前参数缺少可保存的策略模板。")
    strategy_id = str(payload.get("strategy_id") or "").strip()
    if not strategy_id:
        raise ValueError("当前参数缺少策略标识，无法加入交易员管理台。")
    try:
        if not supports_trader_desk(strategy_id):
            raise ValueError(f"{_strategy_template_payload_label(payload)} 暂不支持加入交易员管理台。")
    except KeyError as exc:
        raise ValueError(f"当前版本不认识这个策略：{strategy_id}") from exc
    return payload


def _strategy_template_payload_identity(payload: dict[str, object]) -> str:
    identity_payload = {
        "api_name": str(payload.get("api_name") or "").strip(),
        "strategy_id": str(payload.get("strategy_id") or "").strip(),
        "direction_label": str(payload.get("direction_label") or "").strip(),
        "run_mode_label": str(payload.get("run_mode_label") or "").strip(),
        "symbol": str(payload.get("symbol") or "").strip().upper(),
        "config_snapshot": payload.get("config_snapshot"),
    }
    return json.dumps(identity_payload, ensure_ascii=False, sort_keys=True)


def _strategy_template_bundle_item_from_payload(
    raw_item: object,
    *,
    default_total_quota: str,
    default_unit_quota: str,
    default_quota_steps: str,
    default_status: str,
    default_auto_restart_on_profit: bool,
    default_pause_on_stop_loss: bool,
    default_notes: str,
    item_index: int,
) -> StrategyTemplateBundleItem:
    if not isinstance(raw_item, dict):
        raise ValueError(f"第 {item_index} 条策略不是有效对象。")
    raw_template = raw_item.get("template_payload")
    template_payload = dict(raw_template) if isinstance(raw_template, dict) else dict(raw_item)
    for key in ("api_name", "strategy_id", "strategy_name", "direction_label", "run_mode_label", "symbol"):
        value = raw_item.get(key)
        if value not in (None, "") and key not in template_payload:
            template_payload[key] = value
    record = _strategy_template_record_from_payload(template_payload)
    if record is None:
        raise ValueError(f"第 {item_index} 条策略缺少有效的 config_snapshot。")
    normalized = normalize_trader_draft_inputs(
        total_quota=str(raw_item.get("total_quota", default_total_quota) or default_total_quota),
        unit_quota=str(raw_item.get("unit_quota", default_unit_quota) or default_unit_quota),
        quota_steps=str(raw_item.get("quota_steps", default_quota_steps) or default_quota_steps),
        status=str(raw_item.get("status", default_status) or default_status),
        gate_enabled=False,
        gate_condition="always",
        gate_trigger_inst_id="",
        gate_trigger_price_type="mark",
        gate_lower_price="",
        gate_upper_price="",
    )
    return StrategyTemplateBundleItem(
        record=record,
        total_quota=normalized["total_quota"],
        unit_quota=normalized["unit_quota"],
        quota_steps=normalized["quota_steps"],
        status=str(normalized["status"]),
        auto_restart_on_profit=_coerce_snapshot_bool(
            raw_item.get("auto_restart_on_profit"),
            default_auto_restart_on_profit,
        ),
        pause_on_stop_loss=_coerce_snapshot_bool(
            raw_item.get("pause_on_stop_loss"),
            default_pause_on_stop_loss,
        ),
        notes=str(raw_item.get("notes", default_notes) or default_notes).strip(),
    )


def _strategy_template_bundle_from_payload(
    payload: object,
    *,
    default_package_name: str = "",
) -> StrategyTemplateBundleImport | None:
    if not isinstance(payload, dict):
        return None
    raw_items = payload.get("strategies")
    if not isinstance(raw_items, list) or not raw_items:
        raw_profiles = payload.get("profiles")
        if isinstance(raw_profiles, list) and raw_profiles:
            converted_items: list[dict[str, object]] = []
            for raw_profile in raw_profiles:
                profile = StrategyProfile.from_payload(raw_profile)
                converted_items.append(
                    {
                        "api_name": profile.api_name,
                        "strategy_id": profile.strategy_id,
                        "strategy_name": profile.profile_name,
                        "direction_label": profile.direction_label,
                        "run_mode_label": profile.run_mode_label,
                        "symbol": profile.symbol,
                        "config_snapshot": dict(profile.config_snapshot),
                        "strategy_profile": profile.to_payload(),
                    }
                )
            raw_items = converted_items
        else:
            return None
    defaults = payload.get("defaults")
    default_block = defaults if isinstance(defaults, dict) else {}
    default_total_quota = str(default_block.get("total_quota", "1") or "1")
    default_unit_quota = str(default_block.get("unit_quota", "0.1") or "0.1")
    default_quota_steps = str(default_block.get("quota_steps", "10") or "10")
    default_status = str(default_block.get("status", "draft") or "draft")
    default_auto_restart_on_profit = _coerce_snapshot_bool(
        default_block.get("auto_restart_on_profit"),
        True,
    )
    default_pause_on_stop_loss = _coerce_snapshot_bool(
        default_block.get("pause_on_stop_loss"),
        True,
    )
    default_notes = str(default_block.get("notes", "") or "").strip()
    items = tuple(
        _strategy_template_bundle_item_from_payload(
            raw_item,
            default_total_quota=default_total_quota,
            default_unit_quota=default_unit_quota,
            default_quota_steps=default_quota_steps,
            default_status=default_status,
            default_auto_restart_on_profit=default_auto_restart_on_profit,
            default_pause_on_stop_loss=default_pause_on_stop_loss,
            default_notes=default_notes,
            item_index=index,
        )
        for index, raw_item in enumerate(raw_items, start=1)
    )
    package_name = str(
        payload.get("package_name")
        or payload.get("name")
        or default_package_name
        or "策略组合包"
    ).strip()
    auto_start_on_import = _coerce_snapshot_bool(payload.get("auto_start_on_import"), False)
    return StrategyTemplateBundleImport(
        package_name=package_name,
        items=items,
        auto_start_on_import=auto_start_on_import,
    )


class StrategyBundleImportDialog(simpledialog.Dialog):
    def __init__(
        self,
        parent: Tk | Toplevel,
        *,
        package_name: str,
        items: tuple[StrategyTemplateBundleItem, ...],
        source_apis: tuple[str, ...],
        available_profiles: tuple[str, ...],
        current_api_name: str,
        auto_start_default: bool = False,
    ) -> None:
        self._package_name = package_name
        self._items = items
        self._source_apis = source_apis
        self._available_profiles = available_profiles
        self._current_api_name = current_api_name
        initial_mode = "preserve" if len(source_apis) <= 1 else "selected"
        initial_api = current_api_name or (available_profiles[0] if available_profiles else "")
        self._initial_api = initial_api
        self.mode_var = StringVar(value=initial_mode)
        self.api_var = StringVar(value=initial_api)
        self.auto_start_var = BooleanVar(value=auto_start_default)
        self.result_payload: dict[str, str] | None = None
        self._api_combo: ttk.Combobox | None = None
        self._item_vars: list[BooleanVar] = []
        self._item_api_vars: list[StringVar] = []
        self._item_api_combos: list[ttk.Combobox] = []
        self._item_risk_vars: list[StringVar] = []
        self._item_chase_vars: list[BooleanVar] = []
        super().__init__(parent, "导入策略组合")

    def body(self, master: ttk.Frame):
        master.columnconfigure(0, weight=1)
        summary_lines = [
            f"组合包：{self._package_name}",
            f"文件里的 API：{', '.join(self._source_apis) if self._source_apis else '未填写'}",
            f"本机可用 API：{', '.join(self._available_profiles) if self._available_profiles else '无'}",
        ]
        ttk.Label(master, text="\n".join(summary_lines), justify="left", wraplength=520).grid(
            row=0,
            column=0,
            sticky="w",
            pady=(0, 10),
        )
        options = ttk.LabelFrame(master, text="导入时 API 处理", padding=10)
        options.grid(row=1, column=0, sticky="ew")
        options.columnconfigure(0, weight=1)
        ttk.Radiobutton(
            options,
            text="按文件里的 API 分配（本机不存在时回退到当前 API）",
            value="preserve",
            variable=self.mode_var,
            command=self._refresh_api_combo_state,
        ).grid(row=0, column=0, sticky="w")
        ttk.Radiobutton(
            options,
            text="全部改成当前 API",
            value="current",
            variable=self.mode_var,
            command=self._refresh_api_combo_state,
        ).grid(row=1, column=0, sticky="w", pady=(8, 0))
        ttk.Radiobutton(
            options,
            text="全部改成我指定的 API",
            value="selected",
            variable=self.mode_var,
            command=self._refresh_api_combo_state,
        ).grid(row=2, column=0, sticky="w", pady=(8, 0))
        ttk.Radiobutton(
            options,
            text="逐条指定 API（只对勾选且导入的条目生效）",
            value="per_item",
            variable=self.mode_var,
            command=self._refresh_api_combo_state,
        ).grid(row=3, column=0, sticky="w", pady=(8, 0))
        combo = ttk.Combobox(
            options,
            state="readonly",
            textvariable=self.api_var,
            values=self._available_profiles,
        )
        combo.grid(row=4, column=0, sticky="ew", pady=(8, 0))
        self._api_combo = combo
        self._refresh_api_combo_state()
        ttk.Checkbutton(
            master,
            text="导入后自动启动策略（启动后会进入运行中策略）",
            variable=self.auto_start_var,
        ).grid(row=2, column=0, sticky="w", pady=(10, 0))
        item_box = ttk.LabelFrame(master, text=f"导入条目（共 {len(self._items)} 条）", padding=10)
        item_box.grid(row=3, column=0, sticky="ew", pady=(10, 0))
        item_box.columnconfigure(0, weight=1)
        item_box.columnconfigure(1, weight=0)
        item_box.columnconfigure(2, weight=0)
        item_box.columnconfigure(3, weight=0)
        ttk.Label(item_box, text="Strategy").grid(row=0, column=0, sticky="w")
        ttk.Label(item_box, text="API").grid(row=0, column=1, sticky="w", padx=(12, 0))
        ttk.Label(item_box, text="Risk").grid(row=0, column=2, sticky="w", padx=(12, 0))
        self._item_vars = []
        self._item_api_vars = []
        self._item_api_combos = []
        self._item_risk_vars = []
        self._item_chase_vars = []
        for index, item in enumerate(self._items, start=1):
            var = BooleanVar(value=True)
            self._item_vars.append(var)
            default_api = item.record.api_name.strip()
            if not default_api or default_api not in self._available_profiles:
                default_api = self._initial_api
            api_var = StringVar(value=default_api)
            self._item_api_vars.append(api_var)
            risk_var = StringVar(value=self._bundle_item_initial_risk_text(item))
            self._item_risk_vars.append(risk_var)
            preview_text = self._bundle_item_preview_text(index, item)
            ttk.Checkbutton(
                item_box,
                text=preview_text,
                variable=var,
            ).grid(row=index, column=0, sticky="w", pady=(0 if index == 1 else 6, 0))
            api_combo = ttk.Combobox(
                item_box,
                state="disabled",
                textvariable=api_var,
                values=self._available_profiles,
                width=16,
            )
            api_combo.grid(row=index, column=1, sticky="e", padx=(12, 0), pady=(0 if index == 1 else 6, 0))
            self._item_api_combos.append(api_combo)
            ttk.Entry(item_box, textvariable=risk_var, width=10).grid(
                row=index,
                column=2,
                sticky="e",
                padx=(12, 0),
                pady=(0 if index == 1 else 6, 0),
            )
            chase_var = BooleanVar(value=bool(getattr(item.record.config, "startup_chase_current_signal", False)))
            self._item_chase_vars.append(chase_var)
            if supports_startup_chase_current_signal(item.record.strategy_id):
                ttk.Checkbutton(
                    item_box,
                    text="追当前信号",
                    variable=chase_var,
                ).grid(row=index, column=3, sticky="e", padx=(12, 0), pady=(0 if index == 1 else 6, 0))
        return combo

    @staticmethod
    def _bundle_item_preview_text(index: int, item: StrategyTemplateBundleItem) -> str:
        record = item.record
        strategy_name = record.strategy_name or record.strategy_id
        daily_summary = record.config.daily_filter_summary()
        return (
            f"{index}. {strategy_name} | {record.symbol} | {record.direction_label or '-'} | "
            f"API={record.api_name or '-'} | {daily_summary}"
        )

    @staticmethod
    def _bundle_item_initial_risk_text(item: StrategyTemplateBundleItem) -> str:
        risk_amount = getattr(item.record.config, "risk_amount", None)
        if isinstance(risk_amount, Decimal) and risk_amount > 0:
            return format_decimal(risk_amount)
        return ""

    def _refresh_api_combo_state(self) -> None:
        if self._api_combo is None:
            return
        mode = self.mode_var.get().strip()
        state = "readonly" if mode == "selected" else "disabled"
        self._api_combo.configure(state=state)
        item_state = "readonly" if mode == "per_item" else "disabled"
        for combo in self._item_api_combos:
            combo.configure(state=item_state)

    def validate(self) -> bool:
        mode = self.mode_var.get().strip()
        api_name = self.api_var.get().strip()
        if mode == "selected" and not api_name:
            messagebox.showerror("提示", "请选择一个目标 API。", parent=self)
            return False
        if mode == "per_item":
            for index, variable in enumerate(self._item_vars):
                if not variable.get():
                    continue
                selected_api = self._item_api_vars[index].get().strip()
                if not selected_api:
                    messagebox.showerror("提示", f"请为第 {index + 1} 条策略选择 API。", parent=self)
                    return False
        for index, variable in enumerate(self._item_vars):
            if not variable.get():
                continue
            risk_text = self._item_risk_vars[index].get().strip()
            if not risk_text:
                continue
            try:
                risk_amount = Decimal(risk_text)
            except InvalidOperation:
                messagebox.showerror("提示", f"Strategy #{index + 1} has an invalid risk amount.", parent=self)
                return False
            if risk_amount <= 0:
                messagebox.showerror("提示", f"Strategy #{index + 1} risk amount must be > 0.", parent=self)
                return False
        if mode == "current" and not self._current_api_name:
            messagebox.showerror("提示", "当前没有选中 API，不能使用“全部改成当前 API”。", parent=self)
            return False
        selected_indices = [str(index) for index, variable in enumerate(self._item_vars) if variable.get()]
        if not selected_indices:
            messagebox.showerror("提示", "请至少勾选一条策略。", parent=self)
            return False
        per_item_api_map = ";".join(
            f"{index}={self._item_api_vars[index].get().strip()}"
            for index, variable in enumerate(self._item_vars)
            if variable.get() and self._item_api_vars[index].get().strip()
        )
        per_item_chase_map = ";".join(
            f"{index}={'1' if self._item_chase_vars[index].get() else '0'}"
            for index, variable in enumerate(self._item_vars)
            if variable.get() and supports_startup_chase_current_signal(self._items[index].record.strategy_id)
        )
        per_item_risk_map = ";".join(
            f"{index}={self._item_risk_vars[index].get().strip()}"
            for index, variable in enumerate(self._item_vars)
            if variable.get() and self._item_risk_vars[index].get().strip()
        )
        self.result_payload = {
            "mode": mode,
            "api_name": api_name,
            "auto_start": "1" if self.auto_start_var.get() else "0",
            "selected_indices": ",".join(selected_indices),
            "per_item_api_map": per_item_api_map,
            "per_item_chase_map": per_item_chase_map,
            "per_item_risk_map": per_item_risk_map,
        }
        return True


def _show_strategy_bundle_import_result_dialog(
    parent: Tk | Toplevel,
    *,
    title: str,
    summary_lines: list[str] | tuple[str, ...],
    detail_lines: list[str] | tuple[str, ...],
    warning: bool = False,
) -> None:
    window = Toplevel(parent)
    apply_window_icon(window)
    window.title(title)
    apply_adaptive_window_geometry(
        window,
        width_ratio=0.54,
        height_ratio=0.56,
        min_width=760,
        min_height=520,
        max_width=1280,
        max_height=960,
    )
    window.transient(parent)
    window.grab_set()

    container = ttk.Frame(window, padding=12)
    container.pack(fill="both", expand=True)
    container.columnconfigure(0, weight=1)
    container.rowconfigure(2, weight=1)

    headline = "组合包处理完成，请核对下面的摘要和明细。" if not warning else "组合包处理完成，但存在需要留意的跳过或失败项。"
    ttk.Label(
        container,
        text=headline,
        justify="left",
        wraplength=880,
        font=("Microsoft YaHei UI", 10, "bold"),
    ).grid(row=0, column=0, sticky="w")
    ttk.Label(
        container,
        text="\n".join(str(line) for line in summary_lines if str(line).strip()),
        justify="left",
        wraplength=880,
    ).grid(row=1, column=0, sticky="ew", pady=(8, 10))

    detail_frame = ttk.LabelFrame(container, text="详细结果", padding=8)
    detail_frame.grid(row=2, column=0, sticky="nsew")
    detail_frame.columnconfigure(0, weight=1)
    detail_frame.rowconfigure(0, weight=1)

    detail_text = Text(
        detail_frame,
        wrap="word",
        height=18,
        font=("Consolas", 10),
        undo=False,
    )
    detail_text.grid(row=0, column=0, sticky="nsew")
    detail_scroll = ttk.Scrollbar(detail_frame, orient="vertical", command=detail_text.yview)
    detail_scroll.grid(row=0, column=1, sticky="ns")
    detail_text.configure(yscrollcommand=detail_scroll.set)
    detail_payload = "\n".join(str(line) for line in detail_lines if str(line).strip()) or "无更多明细。"
    detail_text.insert("1.0", detail_payload)
    detail_text.configure(state="disabled")

    action_row = ttk.Frame(container)
    action_row.grid(row=3, column=0, sticky="e", pady=(10, 0))

    def _copy_details() -> None:
        try:
            window.clipboard_clear()
            window.clipboard_append(detail_payload)
            window.update_idletasks()
        except TclError:
            return

    ttk.Button(action_row, text="复制明细", command=_copy_details).grid(row=0, column=0, padx=(0, 8))
    ttk.Button(action_row, text="关闭", command=window.destroy).grid(row=0, column=1)
    window.protocol("WM_DELETE_WINDOW", window.destroy)
    window.wait_window()


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
    strategy_keys: set[tuple[str, str, str, str, str, str, str]] = set()
    api_names: set[str] = set()
    for record in normal_ledgers:
        history_record = history_by_id.get(record.history_record_id)
        strategy_keys.add(_normal_strategy_book_group_key(record, history_record))
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


def _normal_strategy_book_group_key(
    record: StrategyTradeLedgerRecord,
    history_record: StrategyHistoryRecord | None,
) -> tuple[str, str, str, str, str, str, str]:
    """Group repeated runs without mixing API accounts or parameter versions."""
    group_id = str(
        record.strategy_group_id
        or (history_record.strategy_group_id if history_record is not None else "")
        or ""
    ).strip()
    return (
        group_id,
        record.api_name or "-",
        _history_record_trader_label(history_record),
        record.strategy_name or "-",
        record.symbol or "-",
        _history_record_bar_label(history_record),
        record.direction_label or "-",
    )


def _build_normal_strategy_book_group_rows(
    ledger_records: list[StrategyTradeLedgerRecord],
    history_records: list[StrategyHistoryRecord],
    *,
    filters: NormalStrategyBookFilters | None = None,
) -> list[tuple[str, tuple[object, ...]]]:
    normal_ledgers = _normal_strategy_trade_ledger_records(ledger_records, history_records, filters=filters)
    history_by_id = {record.record_id: record for record in history_records}
    grouped: dict[tuple[str, str, str, str, str, str, str], list[StrategyTradeLedgerRecord]] = {}
    for record in normal_ledgers:
        history_record = history_by_id.get(record.history_record_id)
        key = _normal_strategy_book_group_key(record, history_record)
        grouped.setdefault(key, []).append(record)

    rows: list[tuple[str, tuple[object, ...]]] = []
    ordered_items = sorted(
        grouped.items(),
        key=lambda item: max(row.closed_at for row in item[1]),
        reverse=True,
    )
    for key, records in ordered_items:
        group_id, api_name, trader_label, strategy_name, symbol, bar, direction_label = key
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
        row_id = "||".join(key if group_id else key[1:])
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
        or "启动追当前信号，本次接管当前波段" in text
    ):
        return "开仓监控中"
    if "已提交启动请求" in text:
        return "启动中"
    if (
        "当前无信号" in text
        or "当前无开多信号" in text
        or "当前无开空信号" in text
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
        or "仓位关闭已确认" in text
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
        funding_fee=_parse_decimal_or_none(record.get("funding_fee"))
        or _parse_decimal_or_none((record.get("raw") or {}).get("fundingFee") if isinstance(record.get("raw"), dict) else None),
    )


def _position_history_lifecycle_key(item: OkxPositionHistoryItem) -> str:
    raw = item.raw if isinstance(item.raw, dict) else {}
    inst_id = item.inst_id.strip().upper()
    pos_id = str(raw.get("posId") or "").strip()
    created_time = _record_coalesce_int(raw, "cTime")
    side = str(item.pos_side or item.direction or "").strip().lower()
    margin_mode = str(item.mgn_mode or "").strip().lower()
    if pos_id and created_time is not None:
        return "|".join((inst_id, pos_id, str(created_time)))
    if created_time is not None:
        return "|".join((inst_id, str(created_time), side, margin_mode))
    update_time = _normalize_okx_timestamp_ms(item.update_time) or 0
    close_size = str(item.close_size) if item.close_size is not None else ""
    close_avg_price = str(item.close_avg_price) if item.close_avg_price is not None else ""
    return "|".join((inst_id, str(update_time), side, margin_mode, close_size, close_avg_price))


def _position_history_snapshot_close_total(item: OkxPositionHistoryItem) -> Decimal | None:
    raw = item.raw if isinstance(item.raw, dict) else {}
    return (
        _parse_decimal_or_none(raw.get("closeTotalPos"))
        or _parse_decimal_or_none(raw.get("closePos"))
        or _parse_decimal_or_none(raw.get("closeSz"))
        or item.close_size
    )


def _position_history_snapshot_open_total(item: OkxPositionHistoryItem) -> Decimal | None:
    raw = item.raw if isinstance(item.raw, dict) else {}
    return _parse_decimal_or_none(raw.get("openMaxPos"))


def _decorate_position_history_records(records: list[dict[str, object]]) -> list[dict[str, object]]:
    decorated_records = [dict(record) for record in records if isinstance(record, dict)]
    parsed_items: list[tuple[int, OkxPositionHistoryItem]] = []
    for index, record in enumerate(decorated_records):
        item = _position_history_item_from_cache(record)
        if item is not None:
            parsed_items.append((index, item))
    groups: dict[str, list[tuple[int, OkxPositionHistoryItem]]] = {}
    for index, item in parsed_items:
        groups.setdefault(_position_history_lifecycle_key(item), []).append((index, item))
    for lifecycle_key, entries in groups.items():
        entries.sort(key=lambda pair: _normalize_okx_timestamp_ms(pair[1].update_time) or 0)
        previous_close_total: Decimal | None = None
        group_count = len(entries)
        for seq, (record_index, item) in enumerate(entries, start=1):
            record = decorated_records[record_index]
            raw = dict(record.get("raw")) if isinstance(record.get("raw"), dict) else {}
            close_total = _position_history_snapshot_close_total(item)
            open_total = _position_history_snapshot_open_total(item)
            has_later_snapshot = seq < group_count
            is_partial_close = False
            if open_total is not None and close_total is not None and close_total < open_total:
                is_partial_close = True
            elif has_later_snapshot:
                is_partial_close = True
            incremental_close_total: Decimal | None = None
            if close_total is not None:
                incremental_close_total = close_total
                if previous_close_total is not None:
                    delta = close_total - previous_close_total
                    if delta >= 0:
                        incremental_close_total = delta
            meta = {
                "lifecycleKey": lifecycle_key,
                "snapshotSeq": seq,
                "snapshotCount": group_count,
                "isLatestSnapshot": seq == group_count,
                "isPartialClose": is_partial_close,
                "prevCloseTotal": str(previous_close_total) if previous_close_total is not None else "",
                "incrementalCloseTotal": str(incremental_close_total) if incremental_close_total is not None else "",
                "openTotal": str(open_total) if open_total is not None else "",
                "closeTotal": str(close_total) if close_total is not None else "",
            }
            raw["qqokxHistoryMeta"] = meta
            record["raw"] = raw
            record["qqokx_history_meta"] = meta
            decorated_records[record_index] = record
            if close_total is not None:
                previous_close_total = close_total
    return decorated_records


def _collapse_position_history_records(records: list[dict[str, object]]) -> list[dict[str, object]]:
    decorated_records = _decorate_position_history_records(records)
    latest_by_lifecycle: dict[str, tuple[int, dict[str, object]]] = {}
    for index, record in enumerate(decorated_records):
        item = _position_history_item_from_cache(record)
        if item is None:
            continue
        lifecycle_key = _position_history_lifecycle_key(item)
        existing = latest_by_lifecycle.get(lifecycle_key)
        current_time = _normalize_okx_timestamp_ms(item.update_time) or 0
        if existing is None:
            latest_by_lifecycle[lifecycle_key] = (current_time, record)
            continue
        existing_time = existing[0]
        if current_time >= existing_time:
            latest_by_lifecycle[lifecycle_key] = (current_time, record)
    collapsed_records = [record for _, record in latest_by_lifecycle.values()]
    collapsed_records.sort(
        key=lambda record: _record_coalesce_int(record, "update_time", "uTime", "ts") or 0,
        reverse=True,
    )
    return collapsed_records


def _position_history_snapshot_meta(item: OkxPositionHistoryItem) -> dict[str, object]:
    raw = item.raw if isinstance(item.raw, dict) else {}
    meta = raw.get("qqokxHistoryMeta")
    return meta if isinstance(meta, dict) else {}


def _position_history_is_partial_close(item: OkxPositionHistoryItem) -> bool:
    meta = _position_history_snapshot_meta(item)
    return bool(meta.get("isPartialClose"))


def _position_history_incremental_close_total(item: OkxPositionHistoryItem) -> Decimal | None:
    meta = _position_history_snapshot_meta(item)
    return _parse_decimal_or_none(meta.get("incrementalCloseTotal"))


def _position_history_auto_summary(item: OkxPositionHistoryItem) -> str:
    if not _position_history_is_partial_close(item):
        return ""
    return "部分平仓"


def _position_history_note_summary_text(item: OkxPositionHistoryItem, note: str) -> str:
    parts = [part for part in (_position_history_auto_summary(item), _format_position_note_summary(note)) if part and part != "-"]
    return " | ".join(parts) if parts else "-"


@dataclass
class _DynamicProtectionRuleEditorRow:
    frame: ttk.Frame
    trigger_r: StringVar
    action: StringVar
    lock_r: StringVar
    trail_mode: StringVar
    trail_every_r: StringVar
    trail_add_r: StringVar
    trigger_entry: ttk.Entry
    action_combo: ttk.Combobox
    lock_entry: ttk.Entry
    trail_mode_combo: ttk.Combobox
    trail_every_entry: ttk.Entry
    trail_add_entry: ttk.Entry
    delete_button: ttk.Button


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
        self._configure_compact_ui_style()

        self.client = OkxRestClient()
        self.market_data_hub = MarketDataHub(self.client, logger=self._enqueue_log)
        self.deribit_client = DeribitRestClient()
        self.log_queue: queue.Queue[str] = queue.Queue()
        self.instruments: list[Instrument] = []
        self._fixed_order_size_hint_instrument_cache: dict[str, Instrument] = {}
        self._fixed_order_size_hint_fetching_inst_ids: set[str] = set()
        self._minimum_order_risk_hint_after_id: str | None = None
        self._minimum_order_risk_hint_request_serial = 0
        self._minimum_order_risk_hint_active_request_serial = 0
        self.sessions: dict[str, StrategySession] = {}
        # Strategy workers must not read Tkinter variables directly. Keep a
        # UI-thread-updated snapshot for email delivery policy checks.
        self._email_runtime_policy_lock = threading.Lock()
        self._email_runtime_policy_by_session: dict[str, tuple[bool, bool, bool, bool, bool]] = {}
        self._strategy_history_records: list[StrategyHistoryRecord] = []
        self._strategy_history_by_id: dict[str, StrategyHistoryRecord] = {}
        self._strategy_trade_ledger_records: list[StrategyTradeLedgerRecord] = []
        self._strategy_trade_ledger_by_id: dict[str, StrategyTradeLedgerRecord] = {}
        self._recoverable_strategy_sessions: dict[str, RecoverableStrategySessionRecord] = {}
        self._close_confirmation_required = True
        self._restart_command_on_close: list[str] | None = None
        self._upgrade_worker_command_on_close: list[str] | None = None
        self._trader_desk_drafts: list[TraderDraftRecord] = []
        self._trader_desk_runs: list[TraderRunState] = []
        self._trader_desk_slots: list[TraderSlotRecord] = []
        self._trader_desk_events: list[TraderEventRecord] = []
        self._semi_auto_desk_pools: list[SemiAutoPoolRecord] = []
        self._semi_auto_desk_tasks: list = []
        self._semi_auto_desk_window: SemiAutoDeskWindow | None = None
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
        self._email_schedule_manager_window: EmailScheduleManagerWindow | None = None
        self._deribit_volatility_monitor_window: DeribitVolatilityMonitorWindow | None = None
        self._deribit_volatility_window: DeribitVolatilityWindow | None = None
        self._option_roll_window: OptionRollSuggestionWindow | None = None
        self._positions_zoom_window: Toplevel | None = None
        self._strategy_history_window: Toplevel | None = None
        self._protection_window: Toplevel | None = None
        self._protection_replay_window: ProtectionReplayWindow | None = None
        self._positions_refreshing = False
        self._selected_position_manual_flatten_running = False
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
        self._positions_ws_cache_note = ""
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
        self._positions_refresh_generation = 0
        self._positions_active_generation = 0
        self._positions_refresh_request: tuple[Credentials, str, str] | None = None
        self._positions_enrichment_refreshing = False
        self._positions_enrichment_request: tuple[int, list[OkxPosition], str, str] | None = None
        self._session_positions_snapshot_refreshing = False
        self._session_positions_snapshot_refresh_requested = False
        self._session_position_snapshot_force_refresh_keys: set[tuple[str, str]] = set()
        self._session_position_snapshot_last_attempt_at_by_key: dict[tuple[str, str], datetime] = {}
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
        self._account_info_last_refresh_at: datetime | None = None
        self._latest_account_info_profile_name = ""
        self._latest_account_info_environment = ""
        self._account_overview_cache_by_key: dict[tuple[str, str], AccountOverviewCacheEntry] = {}
        self._account_overview_refreshing_keys: set[tuple[str, str]] = set()
        self._account_overview_last_error_by_key: dict[tuple[str, str], str] = {}
        self._account_equity_curve_records_by_key: dict[tuple[str, str], list[dict[str, object]]] = {}
        self._account_equity_curve_windows: dict[tuple[str, str], AccountEquityCurveWindowState] = {}
        self._running_session_account_equity_last_scan_at: datetime | None = None
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
        self._positions_zoom_api_switch_badge_text = StringVar(value="")
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
        self._launcher_frame: ttk.Frame | None = None
        self._launcher_compact_frame: ttk.LabelFrame | None = None
        self._launcher_watch_rail_frame: ttk.Frame | None = None
        self._launcher_start_frame: ttk.LabelFrame | None = None
        self._launcher_strategy_info_frame: ttk.LabelFrame | None = None
        self._sessions_frame: ttk.Frame | None = None
        self._sessions_pane: ttk.Panedwindow | None = None
        self._session_detail_frame: ttk.LabelFrame | None = None
        self._positions_frame: ttk.LabelFrame | None = None
        self._positions_table_frame: ttk.Frame | None = None
        self._log_frame: ttk.LabelFrame | None = None
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
        self.spot_maker_fee_rate = StringVar(value="0.0600")
        self.spot_taker_fee_rate = StringVar(value="0.0700")
        self.futures_maker_fee_rate = StringVar(value="0.0150")
        self.futures_taker_fee_rate = StringVar(value="0.0360")
        self.option_maker_fee_rate = StringVar(value="0.0250")
        self.option_taker_fee_rate = StringVar(value="0.0300")
        self.api_profile_name = StringVar(value=DEFAULT_CREDENTIAL_PROFILE_NAME)
        self.credential_profile_password_status_text = StringVar(value="切换密码：未设置")
        self.environment_label = StringVar(value="模拟盘 demo")

        self.symbol = StringVar(value=self._default_launch_symbol)
        self.trade_symbol = StringVar(value=self._default_launch_symbol)
        self.local_tp_sl_symbol = StringVar(value="")
        self.bar = StringVar(value="15m")
        self.ema_type = StringVar(value="EMA")
        self.ema_period = StringVar(value="21")
        self.trend_ema_type = StringVar(value="EMA")
        self.trend_ema_period = StringVar(value="55")
        self.big_ema_period = StringVar(value="233")
        self.entry_reference_ema_type = StringVar(value="EMA")
        self.entry_reference_ema_period = StringVar(value="55")
        self.mtf_filter_bar = StringVar(value="1H")
        self.mtf_filter_fast_ema_period = StringVar(value="21")
        self.mtf_filter_slow_ema_period = StringVar(value="55")
        self.mtf_reversal_mode_label = StringVar(value=MTF_REVERSAL_MODE_VALUE_TO_LABEL["block_new_entries"])
        self.daily_filter_enabled = BooleanVar(value=False)
        self.daily_filter_boundary_label = StringVar(value=DAILY_FILTER_BOUNDARY_VALUE_TO_LABEL["exchange"])
        self.daily_filter_mode_label = StringVar(value=DAILY_FILTER_MODE_VALUE_TO_LABEL["disabled"])
        self.daily_filter_scope_label = StringVar(value=DAILY_FILTER_SCOPE_VALUE_TO_LABEL["both"])
        self.daily_filter_ma_type = StringVar(value="EMA")
        self.daily_filter_period = StringVar(value="5")
        self.runtime_gate_enabled = BooleanVar(value=False)
        self.runtime_gate_bar = StringVar(value="4H")
        self.runtime_gate_ma_type = StringVar(value="EMA")
        self.runtime_gate_period = StringVar(value="0")
        self.trend_ema_slope_filter_enabled = BooleanVar(value=True)
        self.trend_ema_slope_filter_min_ratio = StringVar(value="0")
        self.atr_percentile_filter_max = StringVar(value="0")
        self.body_retest_breakdown_atr_multiplier = StringVar(value="0.2")
        self.body_retest_retest_atr_multiplier = StringVar(value="0.3")
        self.body_retest_stop_buffer_atr_multiplier = StringVar(value="0.3")
        self.body_retest_body_atr_limit = StringVar(value="1.0")
        self.body_retest_watch_bars = StringVar(value="6")
        self.atr_period = StringVar(value="10")
        self.stop_atr = StringVar(value="2")
        self.take_atr = StringVar(value="4")
        self.risk_amount = StringVar(value="10")
        self.order_size = StringVar(value="1")
        self.fixed_order_size_hint_text = StringVar(
            value="固定数量=OKX下单数量(sz)，不是USDT；若填写风险金，则优先按风险金计算。"
        )
        self.minimum_order_risk_hint_text = StringVar(value="回测参考：请先选择标的。")
        self.launch_parameter_hint_text = StringVar(value="")
        self.trend_parameter_hint_text = StringVar(value="")
        self.dynamic_protection_hint_text = StringVar(value="")
        self.poll_seconds = StringVar(value="10")
        self.signal_mode_label = StringVar(value=STRATEGY_DEFINITIONS[0].default_signal_label)
        self.take_profit_mode_label = StringVar(value="动态止盈")
        self.max_entries_per_trend = StringVar(value="1")
        self.reentry_confirmation_enabled = BooleanVar(value=False)
        self.reentry_confirmation_min_sequence = StringVar(value="0")
        self.reentry_confirmation_ma_type = StringVar(value="EMA")
        self.reentry_confirmation_ma_period = StringVar(value="21")
        self.startup_chase_current_signal = BooleanVar(value=False)
        self.startup_chase_window_seconds = StringVar(value="0")
        self.ema55_slope_exit_enabled = BooleanVar(value=True)
        self.ema55_slope_lock_profit_enabled = BooleanVar(value=False)
        self.dynamic_two_r_break_even = BooleanVar(value=True)
        self.dynamic_break_even_trigger_r = StringVar(value="2")
        self.ema55_slope_lock_profit_trigger_r = StringVar(value="5")
        self.ema55_slope_negative_entry_bars = StringVar(value="1")
        self.dynamic_first_lock_r = StringVar(value="0")
        self.dynamic_trailing_step_r = StringVar(value="1")
        self.dynamic_protection_rules_json = StringVar(value="")
        self._dynamic_protection_rule_rows: list[_DynamicProtectionRuleEditorRow] = []
        self._dynamic_protection_rules_frame: ttk.Frame | None = None
        self.dynamic_fee_offset_enabled = BooleanVar(value=True)
        self.time_stop_break_even_enabled = BooleanVar(value=False)
        self.time_stop_break_even_bars = StringVar(value="10")
        self.trend_ema_close_exit_after_trigger_r_enabled = BooleanVar(value=False)
        self.trend_ema_close_exit_after_trigger_r = StringVar(value="5")
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
        self.symbol.trace_add("write", self._update_launcher_selection_summary)
        self.strategy_name.trace_add("write", self._schedule_minimum_order_risk_hint_update)
        self.strategy_name.trace_add("write", self._update_launcher_selection_summary)
        self.bar.trace_add("write", self._schedule_minimum_order_risk_hint_update)
        self.bar.trace_add("write", self._update_launcher_selection_summary)
        self.signal_mode_label.trace_add("write", self._schedule_minimum_order_risk_hint_update)
        self.signal_mode_label.trace_add("write", self._update_launcher_selection_summary)
        self.ema_type.trace_add("write", self._schedule_minimum_order_risk_hint_update)
        self.ema_period.trace_add("write", self._schedule_minimum_order_risk_hint_update)
        self.trend_ema_type.trace_add("write", self._schedule_minimum_order_risk_hint_update)
        self.trend_ema_period.trace_add("write", self._schedule_minimum_order_risk_hint_update)
        self.big_ema_period.trace_add("write", self._schedule_minimum_order_risk_hint_update)
        self.entry_reference_ema_type.trace_add("write", self._schedule_minimum_order_risk_hint_update)
        self.entry_reference_ema_period.trace_add("write", self._schedule_minimum_order_risk_hint_update)
        self.mtf_filter_bar.trace_add("write", self._schedule_minimum_order_risk_hint_update)
        self.mtf_filter_fast_ema_period.trace_add("write", self._schedule_minimum_order_risk_hint_update)
        self.mtf_filter_slow_ema_period.trace_add("write", self._schedule_minimum_order_risk_hint_update)
        self.daily_filter_enabled.trace_add("write", self._schedule_minimum_order_risk_hint_update)
        self.daily_filter_boundary_label.trace_add("write", self._schedule_minimum_order_risk_hint_update)
        self.daily_filter_mode_label.trace_add("write", self._schedule_minimum_order_risk_hint_update)
        self.daily_filter_scope_label.trace_add("write", self._schedule_minimum_order_risk_hint_update)
        self.daily_filter_ma_type.trace_add("write", self._schedule_minimum_order_risk_hint_update)
        self.daily_filter_period.trace_add("write", self._schedule_minimum_order_risk_hint_update)
        self.trend_ema_slope_filter_enabled.trace_add("write", self._schedule_minimum_order_risk_hint_update)
        self.trend_ema_slope_filter_min_ratio.trace_add("write", self._schedule_minimum_order_risk_hint_update)
        self.atr_percentile_filter_max.trace_add("write", self._schedule_minimum_order_risk_hint_update)
        self.body_retest_breakdown_atr_multiplier.trace_add("write", self._schedule_minimum_order_risk_hint_update)
        self.body_retest_retest_atr_multiplier.trace_add("write", self._schedule_minimum_order_risk_hint_update)
        self.body_retest_stop_buffer_atr_multiplier.trace_add("write", self._schedule_minimum_order_risk_hint_update)
        self.body_retest_body_atr_limit.trace_add("write", self._schedule_minimum_order_risk_hint_update)
        self.body_retest_watch_bars.trace_add("write", self._schedule_minimum_order_risk_hint_update)
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
        self.reentry_confirmation_enabled.trace_add("write", self._update_launch_parameter_hint)
        self.reentry_confirmation_min_sequence.trace_add("write", self._update_launch_parameter_hint)
        self.reentry_confirmation_ma_type.trace_add("write", self._update_launch_parameter_hint)
        self.reentry_confirmation_ma_period.trace_add("write", self._update_launch_parameter_hint)
        self.reentry_confirmation_enabled.trace_add("write", lambda *_: self._sync_dynamic_take_profit_controls())
        self.startup_chase_window_seconds.trace_add("write", self._update_launch_parameter_hint)
        self.ema_type.trace_add("write", self._update_trend_parameter_hint)
        self.ema_period.trace_add("write", self._update_trend_parameter_hint)
        self.trend_ema_type.trace_add("write", self._update_trend_parameter_hint)
        self.trend_ema_period.trace_add("write", self._update_trend_parameter_hint)
        self.big_ema_period.trace_add("write", self._update_trend_parameter_hint)
        self.entry_reference_ema_type.trace_add("write", self._update_trend_parameter_hint)
        self.entry_reference_ema_period.trace_add("write", self._update_trend_parameter_hint)
        self.mtf_filter_bar.trace_add("write", self._update_trend_parameter_hint)
        self.mtf_filter_fast_ema_period.trace_add("write", self._update_trend_parameter_hint)
        self.mtf_filter_slow_ema_period.trace_add("write", self._update_trend_parameter_hint)
        self.daily_filter_enabled.trace_add("write", self._update_trend_parameter_hint)
        self.daily_filter_boundary_label.trace_add("write", self._update_trend_parameter_hint)
        self.daily_filter_mode_label.trace_add("write", self._update_trend_parameter_hint)
        self.daily_filter_scope_label.trace_add("write", self._update_trend_parameter_hint)
        self.daily_filter_ma_type.trace_add("write", self._update_trend_parameter_hint)
        self.daily_filter_period.trace_add("write", self._update_trend_parameter_hint)
        self.trend_ema_slope_filter_enabled.trace_add("write", self._update_trend_parameter_hint)
        self.trend_ema_slope_filter_enabled.trace_add("write", lambda *_: self._sync_trend_slope_filter_controls())
        self.trend_ema_slope_filter_min_ratio.trace_add("write", self._update_trend_parameter_hint)
        self.ema55_slope_lock_profit_enabled.trace_add(
            "write",
            lambda *_: self._sync_btc_ema55_slope_short_controls(),
        )
        self.dynamic_two_r_break_even.trace_add("write", self._update_dynamic_protection_hint)
        self.dynamic_break_even_trigger_r.trace_add("write", self._update_dynamic_protection_hint)
        self.ema55_slope_lock_profit_trigger_r.trace_add("write", self._update_dynamic_protection_hint)
        self.dynamic_first_lock_r.trace_add("write", self._update_dynamic_protection_hint)
        self.dynamic_trailing_step_r.trace_add("write", self._update_dynamic_protection_hint)
        self.dynamic_protection_rules_json.trace_add("write", self._update_dynamic_protection_hint)
        self.dynamic_fee_offset_enabled.trace_add("write", self._update_dynamic_protection_hint)
        self.time_stop_break_even_enabled.trace_add("write", self._update_dynamic_protection_hint)
        self.time_stop_break_even_bars.trace_add("write", self._update_dynamic_protection_hint)
        self.trend_ema_close_exit_after_trigger_r_enabled.trace_add("write", self._update_dynamic_protection_hint)
        self.trend_ema_close_exit_after_trigger_r.trace_add("write", self._update_dynamic_protection_hint)
        self.time_stop_break_even_enabled.trace_add("write", lambda *_: self._sync_dynamic_take_profit_controls())
        self.trend_ema_close_exit_after_trigger_r_enabled.trace_add(
            "write",
            lambda *_: self._sync_dynamic_take_profit_controls(),
        )
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
        self.api_sender_email_override = StringVar()
        self.recipient_emails = StringVar()
        self.use_ssl = BooleanVar(value=True)
        self.notify_trade_fills = BooleanVar(value=True)
        self.notify_signals = BooleanVar(value=True)
        self.notify_errors = BooleanVar(value=True)
        self._upgrade_launch_mode_setting = UpgradeLaunchManager.default_mode()
        self._upgrade_custom_launch_path_setting = ""
        self._pending_upgrade_launch_settings: tuple[str, str] | None = None
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
        self._watch_mode_button_text = StringVar(value="退出盯盘")
        self._launcher_manual_toggle_text = StringVar(value="展开手动参数")
        self._launcher_panel_toggle_text = StringVar(value="显示左栏")
        self.session_summary_text = StringVar(value="多策略合计：当前没有运行中的策略。")
        self.session_quick_actions_text = StringVar(
            value="快捷操作：会话=双击日志 | 交易员=双击打开管理台 | 邮件=双击切换 | 标的=双击K线"
        )
        self.global_email_toggle_text = StringVar(value="发邮件：开")
        self.settings_summary_text = StringVar()
        self.strategy_summary_text = StringVar()
        self.launcher_status_summary_text = StringVar(value="")
        self.launcher_mode_summary_text = StringVar(value="")
        self.launcher_strategy_title_text = StringVar(value="未选择策略")
        self.launcher_selection_summary_text = StringVar(value="未选择标的 | - | -")
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
        self._positions_api_switch_badge_text = StringVar(value="")
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
        self._watch_mode_enabled = True
        self._launcher_manual_visible = False
        self._launcher_panel_visible = False
        self.account_info_summary_text = StringVar(value="尚未读取账户信息。")
        self._account_info_api_switch_badge_text = StringVar(value="")
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
        self._locked_credential_profiles: set[str] = set()
        self._credential_access_granted_profiles: set[str] = set()
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
        self.running_session_api_filter = StringVar(value=STRATEGY_BOOK_FILTER_ALL_API)
        self._running_session_display_columns = UiStrategySessionsMixin._running_session_default_display_columns()
        self._running_session_column_vars: dict[str, BooleanVar] = {}
        self._running_session_column_menu: Menu | None = None
        self._running_session_columns_button: ttk.Button | None = None
        self._running_session_sort_column = "started"
        self._running_session_sort_descending = True
        self._strategy_history_sort_column = "started"
        self._strategy_history_sort_descending = True
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
        self._market_condition_scheduler_job: str | None = None
        self._market_condition_scheduler_inflight: set[str] = set()

        self._settings_watch_enabled = False
        self._api_sender_override_watch_enabled = False
        self._settings_save_job: str | None = None
        self._last_saved_notification_state: tuple[object, ...] | None = None
        self._api_sender_email_overrides: dict[str, str] = {}
        self._position_history_view_prefs_save_job: str | None = None
        self._last_saved_position_history_view_prefs: tuple[str, str] | None = None

        self._load_saved_credentials()
        self._load_saved_notification_settings()
        self._load_position_notes()
        self._load_position_history_view_prefs()
        self._load_recoverable_strategy_sessions_registry()
        self._load_strategy_history()
        self._load_strategy_trade_ledger()
        self._load_semi_auto_desk_snapshot()
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
        self._update_launcher_selection_summary()
        self.root.after_idle(self._apply_initial_pane_layout)
        self.root.after(250, self._drain_log_queue)
        self.root.after(500, self._refresh_status)
        self.root.after(900, self._attempt_auto_restore_recoverable_sessions)
        self.root.after(5000, self._run_market_condition_scheduler)
        self.root.after(1200, self._refresh_positions_periodic)
        self._start_strategy_status_email_scheduler()
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

    def _configure_compact_ui_style(self) -> None:
        def _shrink_named_font(name: str, *, minimum: int = 8) -> None:
            try:
                named_font = tkfont.nametofont(name)
            except Exception:
                return
            try:
                current_size = int(named_font.cget("size"))
            except Exception:
                return
            if current_size <= 0:
                return
            named_font.configure(size=max(current_size - 1, minimum))

        for font_name in (
            "TkDefaultFont",
            "TkTextFont",
            "TkMenuFont",
            "TkHeadingFont",
            "TkCaptionFont",
            "TkSmallCaptionFont",
            "TkTooltipFont",
            "TkIconFont",
            "TkFixedFont",
        ):
            _shrink_named_font(font_name)

        style = ttk.Style(self.root)
        default_font = tkfont.nametofont("TkDefaultFont")
        heading_font = tkfont.nametofont("TkHeadingFont")
        text_font = tkfont.nametofont("TkTextFont")
        style.configure(".", font=default_font)
        style.configure("TButton", padding=(6, 2))
        style.configure("TCombobox", padding=(4, 1))
        style.configure("TEntry", padding=(4, 1))
        style.configure("TLabelframe.Label", font=heading_font)
        style.configure("Treeview", font=text_font, rowheight=22)
        style.configure("Treeview.Heading", font=heading_font)
        style.configure("Vertical.TScrollbar", arrowsize=12)
        style.configure("Horizontal.TScrollbar", arrowsize=12)
        self.root.option_add("*Font", default_font)
        self.root.option_add("*Text.Font", text_font)
        self.root.option_add("*Text.spacing1", 0)
        self.root.option_add("*Text.spacing2", 1)
        self.root.option_add("*Text.spacing3", 0)
        self.root.option_add("*Menu.Font", default_font)
        self.root.option_add("*Listbox.Font", text_font)
        self.root.option_add("*TCombobox*Listbox.font", default_font)
        self.root.option_add("*tearOff", False)

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
            "ema_type": self.ema_type,
            "ema_period": self.ema_period,
            "trend_ema_type": self.trend_ema_type,
            "trend_ema_period": self.trend_ema_period,
            "big_ema_period": self.big_ema_period,
            "atr_period": self.atr_period,
            "atr_stop_multiplier": self.stop_atr,
            "atr_take_multiplier": self.take_atr,
            "entry_reference_ema_type": self.entry_reference_ema_type,
            "entry_reference_ema_period": self.entry_reference_ema_period,
            "mtf_filter_bar": self.mtf_filter_bar,
            "mtf_filter_fast_ema_period": self.mtf_filter_fast_ema_period,
            "mtf_filter_slow_ema_period": self.mtf_filter_slow_ema_period,
            "mtf_reversal_mode": self.mtf_reversal_mode_label,
            "daily_filter_enabled": self.daily_filter_enabled,
            "daily_filter_bar": None,
            "daily_filter_boundary": self.daily_filter_boundary_label,
            "daily_filter_mode": self.daily_filter_mode_label,
            "daily_filter_scope": self.daily_filter_scope_label,
            "daily_filter_ma_type": self.daily_filter_ma_type,
            "daily_filter_period": self.daily_filter_period,
            "runtime_gate_enabled": getattr(self, "runtime_gate_enabled", None),
            "runtime_gate_inst_id": None,
            "runtime_gate_bar": getattr(self, "runtime_gate_bar", None),
            "runtime_gate_ma_type": getattr(self, "runtime_gate_ma_type", None),
            "runtime_gate_period": getattr(self, "runtime_gate_period", None),
            "trend_ema_slope_filter_enabled": self.trend_ema_slope_filter_enabled,
            "trend_ema_slope_filter_min_ratio": self.trend_ema_slope_filter_min_ratio,
            "atr_percentile_filter_max": self.atr_percentile_filter_max,
            "body_retest_breakdown_atr_multiplier": self.body_retest_breakdown_atr_multiplier,
            "body_retest_retest_atr_multiplier": self.body_retest_retest_atr_multiplier,
            "body_retest_stop_buffer_atr_multiplier": self.body_retest_stop_buffer_atr_multiplier,
            "body_retest_body_atr_limit": self.body_retest_body_atr_limit,
            "body_retest_watch_bars": self.body_retest_watch_bars,
            "take_profit_mode": self.take_profit_mode_label,
            "max_entries_per_trend": self.max_entries_per_trend,
            "reentry_confirmation_enabled": getattr(self, "reentry_confirmation_enabled", None),
            "reentry_confirmation_min_sequence": getattr(self, "reentry_confirmation_min_sequence", None),
            "reentry_confirmation_ma_type": getattr(self, "reentry_confirmation_ma_type", None),
            "reentry_confirmation_ma_period": getattr(self, "reentry_confirmation_ma_period", None),
            "ema55_slope_exit_enabled": self.ema55_slope_exit_enabled,
            "ema55_slope_lock_profit_enabled": self.ema55_slope_lock_profit_enabled,
            "dynamic_two_r_break_even": self.dynamic_two_r_break_even,
            "dynamic_break_even_trigger_r": self.dynamic_break_even_trigger_r,
            "ema55_slope_lock_profit_trigger_r": self.ema55_slope_lock_profit_trigger_r,
            "dynamic_first_lock_r": self.dynamic_first_lock_r,
            "dynamic_trailing_step_r": self.dynamic_trailing_step_r,
            "ema55_slope_negative_entry_bars": self.ema55_slope_negative_entry_bars,
            "dynamic_protection_rules": self.dynamic_protection_rules_json,
            "dynamic_fee_offset_enabled": self.dynamic_fee_offset_enabled,
            "time_stop_break_even_enabled": self.time_stop_break_even_enabled,
            "time_stop_break_even_bars": self.time_stop_break_even_bars,
            "trend_ema_close_exit_after_trigger_r_enabled": self.trend_ema_close_exit_after_trigger_r_enabled,
            "trend_ema_close_exit_after_trigger_r": self.trend_ema_close_exit_after_trigger_r,
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
        if supports_startup_chase_current_signal(strategy_id):
            values["startup_chase_current_signal"] = bool(self.startup_chase_current_signal.get())
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
            default_value = strategy_parameter_default_for_scope(strategy_id, key, self._strategy_parameter_scope)
            if default_value is None:
                continue
            if key == "signal_mode":
                variable.set(_reverse_lookup_label(SIGNAL_LABEL_TO_VALUE, str(default_value), definition.default_signal_label))
            elif key == "take_profit_mode":
                variable.set(_reverse_lookup_label(TAKE_PROFIT_MODE_OPTIONS, str(default_value), self.take_profit_mode_label.get()))
            elif key == "mtf_reversal_mode":
                variable.set(_reverse_lookup_label(MTF_REVERSAL_MODE_OPTIONS, str(default_value), self.mtf_reversal_mode_label.get()))
            elif key.endswith("_type"):
                variable.set(str(default_value).upper())
            else:
                variable.set(default_value)
        self.startup_chase_current_signal.set(
            bool(draft.get("startup_chase_current_signal", False))
            if supports_startup_chase_current_signal(strategy_id)
            else False
        )
        self._apply_strategy_parameter_fixed_values(strategy_id, definition=definition)

    def _apply_strategy_parameter_fixed_values(
        self,
        strategy_id: str,
        *,
        definition: StrategyDefinition | None = None,
    ) -> None:
        bindings = self._strategy_parameter_bindings()
        resolved_definition = definition or get_strategy_definition(strategy_id)
        for key in iter_strategy_parameter_keys(strategy_id):
            fixed_value = strategy_fixed_value(strategy_id, key)
            if fixed_value is None:
                continue
            variable = bindings.get(key)
            if variable is None:
                continue
            if key == "signal_mode":
                variable.set(
                    _reverse_lookup_label(
                        SIGNAL_LABEL_TO_VALUE,
                        str(fixed_value),
                        resolved_definition.default_signal_label,
                    )
                )
            elif key == "mtf_reversal_mode":
                variable.set(_reverse_lookup_label(MTF_REVERSAL_MODE_OPTIONS, str(fixed_value), self.mtf_reversal_mode_label.get()))
            elif key.endswith("_type"):
                variable.set(str(fixed_value).upper())
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
            "ema_period": (self._ema_label, _strategy_fast_line_caption(strategy_id)),
            "trend_ema_period": (self._trend_ema_label, "趋势均线"),
            "big_ema_period": (self._big_ema_label, "大周期均线"),
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
        tools_menu.add_command(label="打开回测窗口", command=self.open_backtest_window)
        tools_menu.add_command(label="打开回测对比总览", command=self.open_backtest_compare_window)
        tools_menu.add_command(label="打开BTC行情分析", command=self.open_btc_market_analysis_window)
        tools_menu.add_command(label="打开邮件任务管理器", command=self.open_email_schedule_manager_window)
        tools_menu.add_command(label="打开BTC研究工作台", command=self.open_btc_research_workbench_window)
        tools_menu.add_command(label="打开信号复盘实验室", command=self.open_signal_replay_mock_window)
        tools_menu.add_command(label="打开行情日记", command=self.open_journal_window)
        tools_menu.add_command(label="打开信号观察台", command=self.open_signal_monitor_window)
        tools_menu.add_command(label="打开交易员管理台", command=self.open_trader_desk_window)
        tools_menu.add_command(label="打开半自动操盘台", command=self.open_semi_auto_desk_window)
        menu_bar.add_cascade(label="工具", menu=tools_menu)


        volatility_menu = Menu(menu_bar, tearoff=False)
        volatility_menu.add_command(label="打开波动率监控", command=self.open_deribit_volatility_monitor_window)
        volatility_menu.add_command(label="打开Deribit波动率指数", command=self.open_deribit_volatility_window)
        volatility_menu.add_command(label="打开运行日志目录", command=self._open_run_logs_directory)
        menu_bar.add_cascade(label="波动率", menu=volatility_menu)

        run_logs_menu_label = volatility_menu.entrycget(2, "label")
        volatility_menu.delete(2)

        system_menu = Menu(menu_bar, tearoff=False)
        system_menu.add_command(label=f"版本信息 (v{APP_VERSION})", command=self.show_version_info)
        system_menu.add_command(label="程序升级", command=self.upgrade_program)
        system_menu.add_command(label=run_logs_menu_label, command=self._open_run_logs_directory)
        system_menu.add_separator()
        system_menu.add_command(label="退出", command=self._on_close)
        menu_bar.add_cascade(label="系统", menu=system_menu)

        self.root.config(menu=menu_bar)

    def show_version_info(self) -> None:
        messagebox.showinfo("版本信息", build_version_info_text(), parent=self.root)

    def _save_upgrade_launch_settings(self, *, mode: str, custom_launch_path: str) -> None:
        self._upgrade_launch_mode_setting = UpgradeLaunchManager.normalize_mode(mode)
        self._upgrade_custom_launch_path_setting = UpgradeLaunchManager.normalize_custom_launch_path(custom_launch_path)
        self._save_notification_settings_now(silent=True)

    def _commit_pending_upgrade_launch_settings(self) -> None:
        pending = self._pending_upgrade_launch_settings
        self._pending_upgrade_launch_settings = None
        if pending is None:
            return
        self._save_upgrade_launch_settings(mode=pending[0], custom_launch_path=pending[1])

    def _prompt_upgrade_launch_settings(self) -> tuple[str, str, bool] | None:
        window = Toplevel(self.root)
        window.title("升级完成后的处理")
        window.transient(self.root)
        window.resizable(False, False)
        window.grab_set()

        mode_var = StringVar(value=self._upgrade_launch_mode_setting)
        path_var = StringVar(value=self._upgrade_custom_launch_path_setting)
        remember_var = BooleanVar(value=False)

        body = ttk.Frame(window, padding=16)
        body.grid(row=0, column=0, sticky="nsew")
        body.columnconfigure(0, weight=1)

        ttk.Label(body, text="升级完成后的处理：").grid(row=0, column=0, sticky="w")
        ttk.Radiobutton(
            body,
            text="自动启动当前版本",
            variable=mode_var,
            value=UPGRADE_LAUNCH_MODE_AUTO,
        ).grid(row=1, column=0, sticky="w", pady=(8, 0))
        ttk.Radiobutton(
            body,
            text="启动指定目录版本",
            variable=mode_var,
            value=UPGRADE_LAUNCH_MODE_CUSTOM,
        ).grid(row=2, column=0, sticky="w", pady=(8, 0))

        custom_row = ttk.Frame(body)
        custom_row.grid(row=3, column=0, sticky="ew", pady=(6, 0))
        custom_row.columnconfigure(0, weight=1)
        custom_path_entry = ttk.Entry(custom_row, textvariable=path_var, width=56)
        custom_path_entry.grid(row=0, column=0, sticky="ew")

        def browse_custom_dir() -> None:
            current_text = path_var.get().strip()
            initial_dir = ""
            if current_text:
                current_path = Path(current_text).expanduser()
                if current_path.exists():
                    initial_dir = str(current_path if current_path.is_dir() else current_path.parent)
            selected = filedialog.askdirectory(
                parent=window,
                title="请选择启动目录",
                initialdir=initial_dir or _app_restart_workdir(),
                mustexist=True,
            )
            if not selected:
                return
            selected_path = Path(selected).resolve()
            candidate = selected_path / UPGRADE_CUSTOM_EXECUTABLE_NAME
            path_var.set(str(candidate if candidate.exists() else selected_path))

        browse_button = ttk.Button(custom_row, text="选择目录", command=browse_custom_dir)
        browse_button.grid(row=0, column=1, padx=(8, 0))
        ttk.Label(
            body,
            text=f"会自动查找 {UPGRADE_CUSTOM_EXECUTABLE_NAME}，也可以直接填写 exe 路径。",
        ).grid(row=4, column=0, sticky="w", pady=(4, 0))
        ttk.Radiobutton(
            body,
            text="升级完成后不启动",
            variable=mode_var,
            value=UPGRADE_LAUNCH_MODE_NONE,
        ).grid(row=5, column=0, sticky="w", pady=(10, 0))
        ttk.Checkbutton(body, text="记住本次选择", variable=remember_var).grid(row=6, column=0, sticky="w", pady=(12, 0))

        result: dict[str, object] = {"value": None}

        def refresh_custom_state(*_args: object) -> None:
            is_custom = mode_var.get() == UPGRADE_LAUNCH_MODE_CUSTOM
            entry_state = "normal" if is_custom else "disabled"
            button_state = "normal" if is_custom else "disabled"
            custom_path_entry.configure(state=entry_state)
            browse_button.configure(state=button_state)

        def confirm() -> None:
            mode = UpgradeLaunchManager.normalize_mode(mode_var.get())
            custom_path = path_var.get().strip()
            if mode == UPGRADE_LAUNCH_MODE_CUSTOM:
                try:
                    resolved = UpgradeLaunchManager.resolve_custom_launch_path(custom_path)
                except Exception as exc:
                    messagebox.showerror("启动目录无效", str(exc), parent=window)
                    return
                custom_path = str(resolved)
            result["value"] = (mode, custom_path, remember_var.get())
            window.destroy()

        def cancel() -> None:
            window.destroy()

        button_row = ttk.Frame(body)
        button_row.grid(row=7, column=0, sticky="e", pady=(16, 0))
        ttk.Button(button_row, text="取消", command=cancel).grid(row=0, column=0, padx=(0, 8))
        ttk.Button(button_row, text="确定", command=confirm).grid(row=0, column=1)

        mode_var.trace_add("write", refresh_custom_state)
        refresh_custom_state()
        window.protocol("WM_DELETE_WINDOW", cancel)
        self.root.wait_window(window)
        value = result["value"]
        if not isinstance(value, tuple):
            return None
        return value  # type: ignore[return-value]

    def _resolve_upgrade_launch_plan(self) -> UpgradeLaunchPlan | None:
        self._pending_upgrade_launch_settings = None
        selection = self._prompt_upgrade_launch_settings()
        if selection is None:
            return None
        mode, custom_launch_path, remember_selection = selection
        plan = UpgradeLaunchManager.build_plan(
            mode=mode,
            current_version_command=_build_app_restart_command(),
            current_version_workdir=_app_restart_workdir(),
            custom_launch_path=custom_launch_path,
        )
        if remember_selection:
            self._pending_upgrade_launch_settings = (mode, custom_launch_path)
        self._enqueue_log(_build_upgrade_launch_log_message(plan))
        return plan

    def upgrade_program(self) -> None:
        launch_plan = self._resolve_upgrade_launch_plan()
        if launch_plan is None:
            return
        running_sessions = [session for session in self.sessions.values() if session.engine.is_running]
        running_count = len(running_sessions)
        migratable_count = sum(1 for session in running_sessions if self._session_can_auto_migrate_on_close(session))
        unsupported_count = max(0, running_count - migratable_count)
        message = _build_upgrade_confirmation_message(
            running_count=running_count,
            migratable_count=migratable_count,
            unsupported_count=unsupported_count,
            data_dir=configured_data_root() or data_root(),
        )
        confirmed = messagebox.askyesno("程序升级", message, parent=self.root)
        if not confirmed:
            self._pending_upgrade_launch_settings = None
            return
        self._commit_pending_upgrade_launch_settings()
        if migratable_count > 0:
            session_labels = ", ".join(
                session.session_id for session in running_sessions if self._session_can_auto_migrate_on_close(session)
            )
            self._enqueue_log(f"程序升级开始：准备迁移 {migratable_count} 条策略 -> {session_labels}")
        if unsupported_count > 0:
            session_labels = ", ".join(
                session.session_id
                for session in running_sessions
                if not self._session_can_auto_migrate_on_close(session)
            )
            self._enqueue_log(f"程序升级提示：有 {unsupported_count} 条策略不支持自动迁移，升级后需要手工重启 -> {session_labels}")
        self._upgrade_worker_command_on_close = None
        self._restart_command_on_close = _build_upgrade_launch_worker_command(
            plan=launch_plan,
            log_file_path=daily_log_file_path(base_dir=configured_data_root() or data_root()),
        )
        self._close_confirmation_required = False
        self._on_close()

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
        self.root.rowconfigure(1, weight=6)
        self.root.rowconfigure(2, weight=1)

        header = ttk.Frame(self.root, padding=(12, 12, 12, 6))
        header.grid(row=0, column=0, sticky="ew")
        header.columnconfigure(0, weight=1)
        header.columnconfigure(1, weight=0)

        ttk.Label(
            header,
            text="OKX 多策略工作台",
            font=("Microsoft YaHei UI", 12, "bold"),
        ).grid(row=0, column=0, sticky="w")
        summary_row = ttk.Frame(header)
        summary_row.grid(row=0, column=1, sticky="e")
        ttk.Label(summary_row, text="API").grid(row=0, column=0, sticky="e")
        self._header_credential_profile_combo = ttk.Combobox(
            summary_row,
            textvariable=self.api_profile_name,
            values=self._credential_profile_names(),
            state="readonly",
            width=8,
        )
        self._header_credential_profile_combo.grid(row=0, column=1, sticky="e", padx=(4, 8))
        self._header_credential_profile_combo.bind("<<ComboboxSelected>>", self._on_api_profile_selected)
        ttk.Label(
            summary_row,
            textvariable=self.settings_summary_text,
            justify="right",
            wraplength=460,
            font=("Microsoft YaHei UI", 8),
        ).grid(row=0, column=2, sticky="e")
        ttk.Label(
            summary_row,
            textvariable=self.status_text,
            font=("Microsoft YaHei UI", 10, "bold"),
        ).grid(row=0, column=3, sticky="e", padx=(12, 0))
        ttk.Button(
            summary_row,
            textvariable=self._launcher_panel_toggle_text,
            command=self.toggle_launcher_panel,
        ).grid(row=0, column=4, sticky="e", padx=(12, 0))

        body = ttk.Panedwindow(self.root, orient="horizontal")
        body.grid(row=1, column=0, sticky="nsew", padx=16, pady=(0, 10))
        self._main_body_pane = body

        launcher_frame = ttk.Frame(body, padding=10)
        sessions_frame = ttk.Frame(body, padding=10)
        body.add(launcher_frame, weight=1)
        body.add(sessions_frame, weight=5)
        self._launcher_frame = launcher_frame
        self._sessions_frame = sessions_frame

        launcher_frame.columnconfigure(0, weight=1)
        launcher_frame.rowconfigure(0, weight=0)
        launcher_frame.rowconfigure(1, weight=1)
        launcher_frame.rowconfigure(2, weight=0)
        sessions_frame.columnconfigure(0, weight=1)
        sessions_frame.rowconfigure(0, weight=1)

        launcher_compact_frame = ttk.LabelFrame(launcher_frame, text="启动概览", padding=10)
        launcher_compact_frame.grid(row=0, column=0, sticky="ew")
        launcher_compact_frame.columnconfigure(0, weight=1)
        self._launcher_compact_frame = launcher_compact_frame

        launcher_summary = ttk.Frame(launcher_compact_frame)
        launcher_summary.grid(row=0, column=0, sticky="ew")
        launcher_summary.columnconfigure(0, weight=1)
        launcher_summary.columnconfigure(1, weight=1)
        ttk.Label(
            launcher_summary,
            text="当前账户",
            font=("Microsoft YaHei UI", 10, "bold"),
        ).grid(row=0, column=0, sticky="w")
        ttk.Label(launcher_summary, text="当前 API").grid(row=1, column=0, sticky="w", pady=(10, 0))
        ttk.Label(launcher_summary, textvariable=self.api_profile_name, font=("Microsoft YaHei UI", 11, "bold")).grid(
            row=1, column=1, sticky="w", padx=(8, 0), pady=(10, 0)
        )
        ttk.Label(launcher_summary, text="环境").grid(row=2, column=0, sticky="w", pady=(4, 0))
        ttk.Label(launcher_summary, textvariable=self.environment_label, font=("Microsoft YaHei UI", 9, "bold")).grid(
            row=2, column=1, sticky="w", padx=(8, 0), pady=(6, 0)
        )
        ttk.Label(
            launcher_summary,
            textvariable=self.launcher_status_summary_text,
            foreground="#374151",
            wraplength=248,
            justify="left",
        ).grid(row=3, column=0, columnspan=2, sticky="w", pady=(8, 0))
        ttk.Label(
            launcher_summary,
            textvariable=self.launcher_mode_summary_text,
            foreground="#6b7280",
            wraplength=248,
            justify="left",
        ).grid(row=4, column=0, columnspan=2, sticky="w", pady=(4, 0))

        ttk.Separator(launcher_compact_frame, orient="horizontal").grid(row=1, column=0, sticky="ew", pady=(10, 8))

        launcher_target = ttk.Frame(launcher_compact_frame)
        launcher_target.grid(row=2, column=0, sticky="ew")
        launcher_target.columnconfigure(0, weight=1)
        ttk.Label(launcher_target, text="待启动策略", font=("Microsoft YaHei UI", 10, "bold")).grid(
            row=0, column=0, sticky="w"
        )
        ttk.Label(
            launcher_target,
            textvariable=self.launcher_strategy_title_text,
            font=("Microsoft YaHei UI", 11, "bold"),
        ).grid(row=1, column=0, sticky="w", pady=(8, 0))
        ttk.Label(
            launcher_target,
            textvariable=self.launcher_selection_summary_text,
            justify="left",
            wraplength=248,
            foreground="#374151",
        ).grid(row=2, column=0, sticky="w", pady=(4, 0))

        action_row = ttk.Frame(launcher_compact_frame)
        action_row.grid(row=3, column=0, sticky="ew", pady=(10, 0))
        action_row.columnconfigure(0, weight=1)
        action_row.columnconfigure(1, weight=1)
        ttk.Button(
            action_row,
            textvariable=self._launcher_manual_toggle_text,
            command=self.toggle_launcher_manual_controls,
        ).grid(row=0, column=0, sticky="ew", padx=(0, 4))
        ttk.Button(action_row, textvariable=self._watch_mode_button_text, command=self.toggle_watch_mode).grid(
            row=0, column=1, sticky="ew", padx=(4, 0)
        )

        ttk.Separator(launcher_compact_frame, orient="horizontal").grid(row=4, column=0, sticky="ew", pady=(10, 8))

        launcher_checklist = ttk.Frame(launcher_compact_frame)
        launcher_checklist.grid(row=5, column=0, sticky="ew")
        launcher_checklist.columnconfigure(0, weight=1)
        ttk.Label(launcher_checklist, text="启动前确认", font=("Microsoft YaHei UI", 10, "bold")).grid(
            row=0, column=0, sticky="w"
        )
        ttk.Label(
            launcher_checklist,
            text="API 环境一致 · 组合包切换跟随 API · 越损接管会自动跳过",
            justify="left",
            wraplength=248,
            foreground="#6b7280",
        ).grid(row=1, column=0, sticky="w", pady=(8, 0))

        launcher_watch_rail = ttk.Frame(launcher_frame, padding=(0, 12, 0, 12))
        launcher_watch_rail.grid(row=0, column=0, sticky="nsw")
        launcher_watch_rail.columnconfigure(0, weight=1)
        ttk.Label(launcher_watch_rail, text="左栏", font=("Microsoft YaHei UI", 11, "bold")).grid(row=0, column=0, sticky="n")
        ttk.Button(launcher_watch_rail, text=">", command=self.toggle_watch_mode).grid(row=1, column=0, sticky="ew", pady=(12, 0))
        launcher_watch_rail.grid_remove()
        self._launcher_watch_rail_frame = launcher_watch_rail

        _lp = (6, 0)
        _lp_tight = (4, 0)
        _ix = (0, 10)
        _hint_wrap = 520

        start_frame = ttk.LabelFrame(launcher_frame, text="策略启动", padding=6)
        start_frame.grid(row=1, column=0, sticky="nsew", pady=(12, 0))
        start_frame.columnconfigure(0, weight=1)
        start_frame.rowconfigure(0, weight=1)
        start_frame.rowconfigure(1, weight=0)
        self._launcher_start_frame = start_frame

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
        self.symbol_combo.bind("<<ComboboxSelected>>", self._on_strategy_symbol_selected)

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
        self._reentry_confirmation_enabled_check = ttk.Checkbutton(
            launch_form,
            text="再开仓确认",
            variable=self.reentry_confirmation_enabled,
            command=self._sync_dynamic_take_profit_controls,
        )
        self._reentry_confirmation_enabled_check.grid(row=row, column=0, sticky="w", pady=_lp)
        self._reentry_confirmation_min_sequence_label = ttk.Label(launch_form, text="从第")
        self._reentry_confirmation_min_sequence_label.grid(row=row, column=1, sticky="w", padx=_ix, pady=_lp)
        self._reentry_confirmation_min_sequence_entry = ttk.Entry(
            launch_form,
            textvariable=self.reentry_confirmation_min_sequence,
            width=6,
        )
        self._reentry_confirmation_min_sequence_entry.grid(row=row, column=2, sticky="w", pady=_lp)
        self._reentry_confirmation_ma_frame = ttk.Frame(launch_form)
        self._reentry_confirmation_ma_frame.grid(row=row, column=3, sticky="ew", pady=_lp)
        self._reentry_confirmation_rule_prefix = ttk.Label(
            self._reentry_confirmation_ma_frame,
            text="次起，确认K收盘站上",
        )
        self._reentry_confirmation_rule_prefix.grid(row=0, column=0, sticky="w", padx=(0, 6))
        self._reentry_confirmation_ma_type_combo = ttk.Combobox(
            self._reentry_confirmation_ma_frame,
            textvariable=self.reentry_confirmation_ma_type,
            values=MOVING_AVERAGE_TYPE_OPTIONS,
            state="readonly",
            width=6,
        )
        self._reentry_confirmation_ma_type_combo.grid(row=0, column=1, sticky="w", padx=(0, 6))
        self._reentry_confirmation_ma_period_entry = ttk.Entry(
            self._reentry_confirmation_ma_frame,
            textvariable=self.reentry_confirmation_ma_period,
            width=6,
        )
        self._reentry_confirmation_ma_period_entry.grid(row=0, column=2, sticky="w", padx=(0, 6))
        self._reentry_confirmation_rule_suffix = ttk.Label(
            self._reentry_confirmation_ma_frame,
            text="后才允许再次挂单",
        )
        self._reentry_confirmation_rule_suffix.grid(row=0, column=3, sticky="w")

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
        self._ema55_slope_exit_conditions_caption = ttk.Label(launch_form, text="平仓条件")
        self._ema55_slope_exit_conditions_caption.grid(row=row, column=0, sticky="w", pady=_lp)
        self._ema55_slope_exit_enabled_check = ttk.Checkbutton(
            launch_form,
            text="信号均线斜率重新转正时，按收盘价平仓",
            variable=self.ema55_slope_exit_enabled,
        )
        self._ema55_slope_exit_enabled_check.grid(row=row, column=1, columnspan=3, sticky="w", pady=_lp)

        row += 1
        self._ema55_slope_negative_entry_bars_label = ttk.Label(launch_form, text="连续负斜率根数")
        self._ema55_slope_negative_entry_bars_label.grid(row=row, column=0, sticky="w", pady=_lp)
        self._ema55_slope_negative_entry_bars_entry = ttk.Entry(
            launch_form,
            textvariable=self.ema55_slope_negative_entry_bars,
        )
        self._ema55_slope_negative_entry_bars_entry.grid(row=row, column=1, sticky="ew", padx=_ix, pady=_lp)
        self._ema55_slope_negative_entry_bars_hint_label = ttk.Label(
            launch_form,
            text="BTC 斜率做空专用。1=当前 K 线斜率转负即可开空；2/3=要求连续负斜率确认。",
            foreground="#57606a",
        )
        self._ema55_slope_negative_entry_bars_hint_label.grid(row=row, column=2, columnspan=2, sticky="w", pady=_lp)

        row += 1
        self._ema55_slope_lock_profit_enabled_check = ttk.Checkbutton(
            launch_form,
            text="启用 N R 锁盈利 + 双向手续费",
            variable=self.ema55_slope_lock_profit_enabled,
            command=self._sync_btc_ema55_slope_short_controls,
        )
        self._ema55_slope_lock_profit_enabled_check.grid(row=row, column=0, columnspan=2, sticky="w", pady=_lp)

        row += 1
        self._dynamic_two_r_break_even_check = ttk.Checkbutton(
            launch_form,
            text="启用保本（达到保本触发R时先移到保本位）",
            variable=self.dynamic_two_r_break_even,
        )
        self._dynamic_two_r_break_even_check.grid(row=row, column=0, columnspan=2, sticky="w", pady=_lp)
        self._dynamic_break_even_trigger_r_label = ttk.Label(launch_form, text="保本触发R")
        self._dynamic_break_even_trigger_r_label.grid(row=row, column=2, sticky="e", pady=_lp)
        self._dynamic_break_even_trigger_r_entry = ttk.Entry(
            launch_form,
            textvariable=self.dynamic_break_even_trigger_r,
        )
        self._dynamic_break_even_trigger_r_entry.grid(row=row, column=3, sticky="ew", pady=_lp)

        row += 1
        self._ema55_slope_lock_profit_trigger_r_label = ttk.Label(launch_form, text="移动止盈触发R")
        self._ema55_slope_lock_profit_trigger_r_label.grid(row=row, column=0, sticky="w", pady=_lp)
        self._ema55_slope_lock_profit_trigger_r_entry = ttk.Entry(
            launch_form,
            textvariable=self.ema55_slope_lock_profit_trigger_r,
        )
        self._ema55_slope_lock_profit_trigger_r_entry.grid(row=row, column=1, sticky="ew", padx=_ix, pady=_lp)
        self._dynamic_first_lock_r_label = ttk.Label(launch_form, text="首档锁盈R")
        self._dynamic_first_lock_r_label.grid(row=row, column=2, sticky="e", pady=_lp)
        self._dynamic_first_lock_r_entry = ttk.Entry(
            launch_form,
            textvariable=self.dynamic_first_lock_r,
        )
        self._dynamic_first_lock_r_entry.grid(row=row, column=3, sticky="ew", pady=_lp)

        row += 1
        self._dynamic_trailing_step_r_label = ttk.Label(launch_form, text="移动步长R")
        self._dynamic_trailing_step_r_label.grid(row=row, column=2, sticky="e", pady=_lp)
        self._dynamic_trailing_step_r_entry = ttk.Entry(
            launch_form,
            textvariable=self.dynamic_trailing_step_r,
        )
        self._dynamic_trailing_step_r_entry.grid(row=row, column=3, sticky="ew", pady=_lp)

        for widget in (
            self._dynamic_two_r_break_even_check,
            self._dynamic_break_even_trigger_r_label,
            self._dynamic_break_even_trigger_r_entry,
            self._ema55_slope_negative_entry_bars_label,
            self._ema55_slope_negative_entry_bars_entry,
            self._ema55_slope_negative_entry_bars_hint_label,
            self._ema55_slope_lock_profit_enabled_check,
            self._ema55_slope_lock_profit_trigger_r_label,
            self._ema55_slope_lock_profit_trigger_r_entry,
            self._dynamic_first_lock_r_label,
            self._dynamic_first_lock_r_entry,
            self._dynamic_trailing_step_r_label,
            self._dynamic_trailing_step_r_entry,
        ):
            widget.grid_remove()

        self._dynamic_protection_rules_card = ttk.LabelFrame(launch_form, text="动态保护规则", padding=(10, 8))
        self._dynamic_protection_rules_card.grid(row=row - 2, column=0, columnspan=4, sticky="ew", pady=(0, 6))
        self._dynamic_protection_rules_card.columnconfigure(0, weight=1)
        header_frame = ttk.Frame(self._dynamic_protection_rules_card)
        header_frame.grid(row=0, column=0, sticky="ew")
        for column, text in enumerate(("触发R", "动作", "锁到R", "递进", "每隔R", "每次加R", "操作")):
            ttk.Label(header_frame, text=text).grid(row=0, column=column, sticky="w", padx=(0, 6))
        self._dynamic_protection_rules_frame = ttk.Frame(self._dynamic_protection_rules_card)
        self._dynamic_protection_rules_frame.grid(row=1, column=0, sticky="ew", pady=(4, 0))
        footer_frame = ttk.Frame(self._dynamic_protection_rules_card)
        footer_frame.grid(row=2, column=0, sticky="w", pady=(8, 0))
        ttk.Button(
            footer_frame,
            text="新增规则",
            command=lambda: (self._append_dynamic_protection_rule_row(), self._sync_dynamic_protection_rules_json_from_editor()),
        ).grid(row=0, column=0, sticky="w")
        ttk.Button(
            footer_frame,
            text="恢复默认",
            command=self._rebuild_dynamic_protection_rule_editor,
        ).grid(row=0, column=1, sticky="w", padx=(6, 0))
        self._rebuild_dynamic_protection_rule_editor()

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
        self._trend_ema_close_exit_after_trigger_r_enabled_check = ttk.Checkbutton(
            launch_form,
            text="达到 nR 后，收盘跌破趋势 EMA 平仓",
            variable=self.trend_ema_close_exit_after_trigger_r_enabled,
            command=self._sync_dynamic_take_profit_controls,
        )
        self._trend_ema_close_exit_after_trigger_r_enabled_check.grid(row=row, column=0, columnspan=2, sticky="w", pady=_lp_tight)
        self._trend_ema_close_exit_after_trigger_r_label = ttk.Label(launch_form, text="趋势EMA平仓触发R")
        self._trend_ema_close_exit_after_trigger_r_label.grid(row=row, column=2, sticky="e", pady=_lp_tight)
        self._trend_ema_close_exit_after_trigger_r_entry = ttk.Entry(
            launch_form,
            textvariable=self.trend_ema_close_exit_after_trigger_r,
        )
        self._trend_ema_close_exit_after_trigger_r_entry.grid(row=row, column=3, sticky="ew", pady=_lp_tight)

        row += 1
        self._trend_ema_close_exit_after_trigger_r_hint_label = ttk.Label(
            launch_form,
            text="趋势 EMA 随上方趋势均线同步",
            foreground="#57606a",
        )
        self._trend_ema_close_exit_after_trigger_r_hint_label.grid(row=row, column=0, columnspan=4, sticky="w", pady=(2, 0))

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
        self._ema_label = ttk.Label(launch_form, text="快线均线")
        self._ema_label.grid(row=row, column=0, sticky="w", pady=_lp)
        self._ema_frame = ttk.Frame(launch_form)
        self._ema_frame.grid(row=row, column=1, sticky="ew", padx=_ix, pady=_lp)
        self._ema_frame.columnconfigure(1, weight=1)
        self._ema_type_combo = ttk.Combobox(
            self._ema_frame,
            textvariable=self.ema_type,
            values=MOVING_AVERAGE_TYPE_OPTIONS,
            state="readonly",
            width=6,
        )
        self._ema_type_combo.grid(row=0, column=0, sticky="w", padx=(0, 6))
        self._ema_entry = ttk.Entry(self._ema_frame, textvariable=self.ema_period)
        self._ema_entry.grid(row=0, column=1, sticky="ew")
        self._trend_ema_label = ttk.Label(launch_form, text="趋势均线")
        self._trend_ema_label.grid(row=row, column=2, sticky="w", pady=_lp)
        self._trend_ema_frame = ttk.Frame(launch_form)
        self._trend_ema_frame.grid(row=row, column=3, sticky="ew", pady=_lp)
        self._trend_ema_frame.columnconfigure(1, weight=1)
        self._trend_ema_type_combo = ttk.Combobox(
            self._trend_ema_frame,
            textvariable=self.trend_ema_type,
            values=MOVING_AVERAGE_TYPE_OPTIONS,
            state="readonly",
            width=6,
        )
        self._trend_ema_type_combo.grid(row=0, column=0, sticky="w", padx=(0, 6))
        self._trend_ema_entry = ttk.Entry(self._trend_ema_frame, textvariable=self.trend_ema_period)
        self._trend_ema_entry.grid(row=0, column=1, sticky="ew")

        row += 1
        self._big_ema_label = ttk.Label(launch_form, text="大周期均线")
        self._big_ema_label.grid(row=row, column=0, sticky="w", pady=_lp)
        self._big_ema_entry = ttk.Entry(launch_form, textvariable=self.big_ema_period)
        self._big_ema_entry.grid(row=row, column=1, sticky="ew", padx=_ix, pady=_lp)
        self._atr_label = ttk.Label(launch_form, text="ATR 周期")
        self._atr_label.grid(row=row, column=2, sticky="w", pady=_lp)
        self._atr_entry = ttk.Entry(launch_form, textvariable=self.atr_period)
        self._atr_entry.grid(row=row, column=3, sticky="ew", pady=_lp)

        row += 1
        self._entry_reference_ema_label = ttk.Label(launch_form, text="参考线周期")
        self._entry_reference_ema_label.grid(row=row, column=0, sticky="w", pady=_lp)
        self._entry_reference_ema_frame = ttk.Frame(launch_form)
        self._entry_reference_ema_frame.grid(row=row, column=1, sticky="ew", padx=_ix, pady=_lp)
        self._entry_reference_ema_frame.columnconfigure(1, weight=1)
        self._entry_reference_ema_type_combo = ttk.Combobox(
            self._entry_reference_ema_frame,
            textvariable=self.entry_reference_ema_type,
            values=MOVING_AVERAGE_TYPE_OPTIONS,
            state="readonly",
            width=6,
        )
        self._entry_reference_ema_type_combo.grid(row=0, column=0, sticky="w", padx=(0, 6))
        self._entry_reference_ema_entry = ttk.Entry(self._entry_reference_ema_frame, textvariable=self.entry_reference_ema_period)
        self._entry_reference_ema_entry.grid(row=0, column=1, sticky="ew")

        row += 1
        self._trend_slope_filter_enabled_check = ttk.Checkbutton(
            launch_form,
            text="启用趋势线斜率过滤",
            variable=self.trend_ema_slope_filter_enabled,
            command=self._sync_trend_slope_filter_controls,
        )
        self._trend_slope_filter_enabled_check.grid(row=row, column=0, columnspan=4, sticky="w", pady=_lp)

        row += 1
        self._slope_threshold_label = ttk.Label(launch_form, text="趋势线斜率阈值")
        self._slope_threshold_label.grid(row=row, column=0, sticky="w", pady=_lp)
        self._slope_threshold_entry = ttk.Entry(launch_form, textvariable=self.trend_ema_slope_filter_min_ratio)
        self._slope_threshold_entry.grid(row=row, column=1, sticky="ew", padx=_ix, pady=_lp)
        self._slope_threshold_hint_label = ttk.Label(
            launch_form,
            text="按最近 5 根趋势线回归斜率 / 当前趋势线值得出；填 0 表示斜率一转负就拦截，填 -0.0005 表示允许轻微下拐。",
        )
        self._slope_threshold_hint_label.grid(row=row, column=2, columnspan=2, sticky="w", pady=_lp)

        row += 1
        self._mtf_filter_bar_label = ttk.Label(launch_form, text="高周期K线")
        self._mtf_filter_bar_label.grid(row=row, column=0, sticky="w", pady=_lp)
        self._atr_percentile_filter_label = ttk.Label(launch_form, text="ATR percentile max")
        self._atr_percentile_filter_label.grid(row=row, column=0, sticky="w", pady=_lp)
        self._atr_percentile_filter_entry = ttk.Entry(launch_form, textvariable=self.atr_percentile_filter_max)
        self._atr_percentile_filter_entry.grid(row=row, column=1, sticky="ew", padx=_ix, pady=_lp)
        self._body_retest_breakdown_label = ttk.Label(launch_form, text="Breakdown ATR")
        self._body_retest_breakdown_label.grid(row=row, column=2, sticky="w", pady=_lp)
        self._body_retest_breakdown_entry = ttk.Entry(
            launch_form,
            textvariable=self.body_retest_breakdown_atr_multiplier,
        )
        self._body_retest_breakdown_entry.grid(row=row, column=3, sticky="ew", pady=_lp)

        row += 1
        self._body_retest_retest_label = ttk.Label(launch_form, text="Retest ATR")
        self._body_retest_retest_label.grid(row=row, column=0, sticky="w", pady=_lp)
        self._body_retest_retest_entry = ttk.Entry(
            launch_form,
            textvariable=self.body_retest_retest_atr_multiplier,
        )
        self._body_retest_retest_entry.grid(row=row, column=1, sticky="ew", padx=_ix, pady=_lp)
        self._body_retest_stop_buffer_label = ttk.Label(launch_form, text="Stop buffer ATR")
        self._body_retest_stop_buffer_label.grid(row=row, column=2, sticky="w", pady=_lp)
        self._body_retest_stop_buffer_entry = ttk.Entry(
            launch_form,
            textvariable=self.body_retest_stop_buffer_atr_multiplier,
        )
        self._body_retest_stop_buffer_entry.grid(row=row, column=3, sticky="ew", pady=_lp)

        row += 1
        self._body_retest_body_limit_label = ttk.Label(launch_form, text="Body ATR limit")
        self._body_retest_body_limit_label.grid(row=row, column=0, sticky="w", pady=_lp)
        self._body_retest_body_limit_entry = ttk.Entry(launch_form, textvariable=self.body_retest_body_atr_limit)
        self._body_retest_body_limit_entry.grid(row=row, column=1, sticky="ew", padx=_ix, pady=_lp)
        self._body_retest_watch_bars_label = ttk.Label(launch_form, text="Watch bars")
        self._body_retest_watch_bars_label.grid(row=row, column=2, sticky="w", pady=_lp)
        self._body_retest_watch_bars_entry = ttk.Entry(launch_form, textvariable=self.body_retest_watch_bars)
        self._body_retest_watch_bars_entry.grid(row=row, column=3, sticky="ew", pady=_lp)

        row += 1
        self._mtf_filter_bar_label_row = ttk.Label(launch_form, text="Higher TF bar")
        self._mtf_filter_bar_label_row.grid(row=row, column=0, sticky="w", pady=_lp)
        self._mtf_filter_bar_combo = ttk.Combobox(
            launch_form,
            textvariable=self.mtf_filter_bar,
            values=BAR_OPTIONS,
            state="readonly",
        )
        self._mtf_filter_bar_combo.grid(row=row, column=1, sticky="ew", padx=_ix, pady=_lp)
        self._mtf_filter_fast_ema_label = ttk.Label(launch_form, text="高周期快EMA")
        self._mtf_filter_fast_ema_label.grid(row=row, column=2, sticky="w", pady=_lp)
        self._mtf_filter_fast_ema_entry = ttk.Entry(launch_form, textvariable=self.mtf_filter_fast_ema_period)
        self._mtf_filter_fast_ema_entry.grid(row=row, column=3, sticky="ew", pady=_lp)

        row += 1
        self._mtf_filter_slow_ema_label = ttk.Label(launch_form, text="高周期慢EMA")
        self._mtf_filter_slow_ema_label.grid(row=row, column=0, sticky="w", pady=_lp)
        self._mtf_filter_slow_ema_entry = ttk.Entry(launch_form, textvariable=self.mtf_filter_slow_ema_period)
        self._mtf_filter_slow_ema_entry.grid(row=row, column=1, sticky="ew", padx=_ix, pady=_lp)
        self._mtf_reversal_mode_label = ttk.Label(launch_form, text="高周期反向处理")
        self._mtf_reversal_mode_label.grid(row=row, column=2, sticky="w", pady=_lp)
        self._mtf_reversal_mode_combo = ttk.Combobox(
            launch_form,
            textvariable=self.mtf_reversal_mode_label,
            values=list(MTF_REVERSAL_MODE_OPTIONS.keys()),
            state="readonly",
        )
        self._mtf_reversal_mode_combo.grid(row=row, column=3, sticky="ew", pady=_lp)

        row += 1
        self._runtime_gate_enabled_check = ttk.Checkbutton(
            launch_form,
            text="启用独立行情运行条件（不等于日线过滤）",
            variable=self.runtime_gate_enabled,
        )
        self._runtime_gate_enabled_check.grid(row=row, column=0, columnspan=4, sticky="w", pady=_lp)

        row += 1
        ttk.Label(launch_form, text="运行条件周期").grid(row=row, column=0, sticky="w", pady=_lp)
        self._runtime_gate_bar_combo = ttk.Combobox(
            launch_form,
            textvariable=self.runtime_gate_bar,
            values=["1H", "4H", "1D"],
            state="readonly",
        )
        self._runtime_gate_bar_combo.grid(row=row, column=1, sticky="ew", padx=_ix, pady=_lp)
        ttk.Label(launch_form, text="运行条件均线").grid(row=row, column=2, sticky="w", pady=_lp)
        self._runtime_gate_ma_combo = ttk.Combobox(
            launch_form,
            textvariable=self.runtime_gate_ma_type,
            values=MOVING_AVERAGE_TYPE_OPTIONS,
            state="readonly",
        )
        self._runtime_gate_ma_combo.grid(row=row, column=3, sticky="ew", pady=_lp)

        row += 1
        ttk.Label(launch_form, text="运行条件均线周期").grid(row=row, column=0, sticky="w", pady=_lp)
        self._runtime_gate_period_entry = ttk.Entry(launch_form, textvariable=self.runtime_gate_period)
        self._runtime_gate_period_entry.grid(row=row, column=1, sticky="ew", padx=_ix, pady=_lp)
        ttk.Label(
            launch_form,
            text="填0表示使用当前策略趋势EMA；不满足时空闲策略暂停，有持仓继续管理。",
            justify="left",
            wraplength=_hint_wrap,
        ).grid(row=row, column=2, columnspan=2, sticky="w", pady=_lp)

        row += 1
        self._daily_filter_enabled_check = ttk.Checkbutton(
            launch_form,
            text="鍚敤鏃ョ嚎杩囨护锛堜粎浣跨敤褰撴椂宸叉敹鐩樼殑涓婁竴鏍规棩绾匡級",
            variable=self.daily_filter_enabled,
            command=self._sync_daily_filter_controls,
        )
        self._daily_filter_enabled_check.grid(row=row, column=0, columnspan=4, sticky="w", pady=_lp)

        row += 1
        self._daily_filter_boundary_label = ttk.Label(launch_form, text="鏃ョ嚎鏍囧噯")
        self._daily_filter_boundary_label.grid(row=row, column=0, sticky="w", pady=_lp)
        self._daily_filter_boundary_combo = ttk.Combobox(
            launch_form,
            textvariable=self.daily_filter_boundary_label,
            values=list(DAILY_FILTER_BOUNDARY_LABEL_TO_VALUE.keys()),
            state="readonly",
        )
        self._daily_filter_boundary_combo.grid(row=row, column=1, sticky="ew", padx=_ix, pady=_lp)
        self._daily_filter_scope_label = ttk.Label(launch_form, text="杩囨护鏂瑰悜")
        self._daily_filter_scope_label.grid(row=row, column=2, sticky="w", pady=_lp)
        self._daily_filter_scope_combo = ttk.Combobox(
            launch_form,
            textvariable=self.daily_filter_scope_label,
            values=list(DAILY_FILTER_SCOPE_LABEL_TO_VALUE.keys()),
            state="readonly",
        )
        self._daily_filter_scope_combo.grid(row=row, column=3, sticky="ew", pady=_lp)

        row += 1
        self._daily_filter_mode_caption = ttk.Label(launch_form, text="杩囨护瑙勫垯")
        self._daily_filter_mode_caption.grid(row=row, column=0, sticky="w", pady=_lp)
        self._daily_filter_mode_combo = ttk.Combobox(
            launch_form,
            textvariable=self.daily_filter_mode_label,
            values=list(DAILY_FILTER_MODE_LABEL_TO_VALUE.keys()),
            state="readonly",
        )
        self._daily_filter_mode_combo.grid(row=row, column=1, sticky="ew", padx=_ix, pady=_lp)
        self._daily_filter_mode_combo.bind("<<ComboboxSelected>>", lambda *_: self._sync_daily_filter_controls())
        self._daily_filter_ma_label = ttk.Label(launch_form, text="鏃ョ嚎鍧囩嚎绫诲瀷")
        self._daily_filter_ma_label.grid(row=row, column=2, sticky="w", pady=_lp)
        self._daily_filter_ma_combo = ttk.Combobox(
            launch_form,
            textvariable=self.daily_filter_ma_type,
            values=MOVING_AVERAGE_TYPE_OPTIONS,
            state="readonly",
        )
        self._daily_filter_ma_combo.grid(row=row, column=3, sticky="ew", pady=_lp)

        row += 1
        self._daily_filter_period_label = ttk.Label(launch_form, text="鏃ョ嚎鍧囩嚎鍛ㄦ湡")
        self._daily_filter_period_label.grid(row=row, column=0, sticky="w", pady=_lp)
        self._daily_filter_period_entry = ttk.Entry(launch_form, textvariable=self.daily_filter_period)
        self._daily_filter_period_entry.grid(row=row, column=1, sticky="ew", padx=_ix, pady=_lp)
        self._daily_filter_hint_label = ttk.Label(
            launch_form,
            text="鍖椾含鏃堕棿0鐐?8鐐规棩绾块兘浠庢湰鍦?1H 宸茬‘璁绾块噸閲囨牱锛屼笉鐩存帴璇诲彇浜ゆ槗鎵€褰撳ぉ鏈敹鐩樼殑1D銆?",
        )
        self._daily_filter_hint_label.grid(row=row, column=2, columnspan=2, sticky="w", pady=_lp)

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

        strategy_info = ttk.LabelFrame(launcher_frame, text="策略说明", padding=10)
        strategy_info.grid(row=2, column=0, sticky="ew", pady=(12, 0))
        strategy_info.columnconfigure(0, weight=1)
        self._launcher_strategy_info_frame = strategy_info

        ttk.Label(strategy_info, text="策略简介", font=("Microsoft YaHei UI", 10, "bold")).grid(
            row=0, column=0, sticky="w"
        )
        ttk.Label(
            strategy_info,
            textvariable=self.strategy_summary_text,
            wraplength=820,
            justify="left",
        ).grid(row=1, column=0, sticky="w", pady=(6, 12))

        ttk.Label(strategy_info, text="规则说明", font=("Microsoft YaHei UI", 10, "bold")).grid(
            row=2, column=0, sticky="w"
        )
        ttk.Label(
            strategy_info,
            textvariable=self.strategy_rule_text,
            wraplength=820,
            justify="left",
        ).grid(row=3, column=0, sticky="w", pady=(6, 12))

        ttk.Label(strategy_info, text="参数提示", font=("Microsoft YaHei UI", 10, "bold")).grid(
            row=4, column=0, sticky="w"
        )
        ttk.Label(
            strategy_info,
            textvariable=self.strategy_hint_text,
            wraplength=820,
            justify="left",
        ).grid(row=5, column=0, sticky="w", pady=(6, 0))
        start_frame.grid_remove()
        strategy_info.grid_remove()

        sessions_pane = ttk.Panedwindow(sessions_frame, orient="vertical")
        sessions_pane.grid(row=0, column=0, sticky="nsew")
        self._sessions_pane = sessions_pane

        session_top_frame = ttk.Frame(sessions_pane)
        session_top_frame.columnconfigure(0, weight=1)
        session_top_frame.rowconfigure(0, weight=6)
        session_top_frame.rowconfigure(1, weight=1)
        sessions_pane.add(session_top_frame, weight=2)

        running_frame = ttk.LabelFrame(session_top_frame, text="运行中策略", padding=10)
        running_frame.grid(row=0, column=0, sticky="nsew")
        running_frame.columnconfigure(0, weight=1)
        running_frame.rowconfigure(1, weight=1)
        self._running_frame = running_frame

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
        ttk.Label(running_header, text="API").grid(row=0, column=3, sticky="e", padx=(12, 6))
        self._running_session_api_filter_combo = ttk.Combobox(
            running_header,
            textvariable=self.running_session_api_filter,
            values=(STRATEGY_BOOK_FILTER_ALL_API,),
            state="readonly",
            width=14,
        )
        self._running_session_api_filter_combo.grid(row=0, column=4, sticky="e")
        self._running_session_api_filter_combo.bind("<<ComboboxSelected>>", self._on_running_session_filter_changed)
        self._running_session_columns_button = ttk.Button(running_header, text="列设置")
        self._running_session_columns_button.grid(row=0, column=5, sticky="e", padx=(12, 0))

        self.session_tree = ttk.Treeview(
            running_frame,
            columns=(
                "session",
                "trader",
                "email",
                "api",
                "account_equity",
                "source_type",
                "strategy",
                "mode",
                "symbol",
                "bar",
                "direction",
                "risk_amount",
                "open_qty",
                "entry_price",
                "stop_price",
                "take_profit",
                "live_pnl",
                "pnl",
                "last_pnl",
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
        self.session_tree.heading("account_equity", text="账户总权益")
        self.session_tree.heading("source_type", text="来源类型")
        self.session_tree.heading("strategy", text="策略")
        self.session_tree.heading("mode", text="模式")
        self.session_tree.heading("symbol", text="标的(双击K线)")
        self.session_tree.heading("bar", text="周期")
        self.session_tree.heading("direction", text="方向")
        self.session_tree.heading("risk_amount", text="风险金")
        self.session_tree.heading("open_qty", text="开仓数量")
        self.session_tree.heading("entry_price", text="开仓价")
        self.session_tree.heading("stop_price", text="止损价")
        self.session_tree.heading("take_profit", text="止盈价")
        self.session_tree.heading("live_pnl", text="实时浮盈亏")
        self.session_tree.heading("pnl", text="净盈亏")
        self.session_tree.heading("last_pnl", text="上次净盈亏")
        self.session_tree.heading("status", text="状态")
        self.session_tree.heading("started", text="启动时间")
        self._refresh_running_session_tree_headings()
        self.session_tree.column("session", width=52, anchor="center")
        self.session_tree.column("trader", width=64, anchor="center")
        self.session_tree.column("email", width=48, anchor="center")
        self.session_tree.column("api", width=74, anchor="center")
        self.session_tree.column("account_equity", width=96, anchor="e")
        self.session_tree.column("source_type", width=86, anchor="center")
        self.session_tree.column("strategy", width=108, anchor="w")
        self.session_tree.column("mode", width=88, anchor="center")
        self.session_tree.column("symbol", width=132, anchor="w")
        self.session_tree.column("bar", width=54, anchor="center")
        self.session_tree.column("direction", width=64, anchor="center")
        self.session_tree.column("risk_amount", width=72, anchor="e")
        self.session_tree.column("open_qty", width=92, anchor="e")
        self.session_tree.column("entry_price", width=76, anchor="e")
        self.session_tree.column("stop_price", width=76, anchor="e")
        self.session_tree.column("take_profit", width=76, anchor="e")
        self.session_tree.column("live_pnl", width=96, anchor="e")
        self.session_tree.column("pnl", width=88, anchor="e")
        self.session_tree.column("last_pnl", width=88, anchor="e")
        self.session_tree.column("status", width=120, anchor="center")
        self.session_tree.column("started", width=108, anchor="center")
        self.session_tree.configure(
            displaycolumns=UiStrategySessionsMixin._normalize_running_session_display_columns(
                self._running_session_display_columns
            )
        )
        self.session_tree.grid(row=1, column=0, sticky="nsew")
        self.session_tree.bind("<<TreeviewSelect>>", self._on_session_selected)
        self.session_tree.bind("<Double-1>", self._on_session_tree_double_click)
        self.session_tree.bind("<Motion>", self._on_session_tree_hover)
        self.session_tree.bind("<Leave>", self._on_session_tree_hover_leave)
        self.session_tree.tag_configure("duplicate_conflict", background="#fff4e5", foreground="#a85a00")
        self._running_session_column_menu = Menu(self.root, tearoff=0)
        for column_id in UiStrategySessionsMixin._running_session_all_columns():
            variable = BooleanVar(value=column_id in self._running_session_display_columns)
            self._running_session_column_vars[column_id] = variable
            self._running_session_column_menu.add_checkbutton(
                label=UiStrategySessionsMixin._running_session_column_heading_text(column_id),
                variable=variable,
                command=lambda current=column_id: self._toggle_running_session_column(current),
            )
        self._running_session_columns_button.configure(command=self._show_running_session_column_menu)
        self._apply_running_session_display_columns()

        tree_scroll = ttk.Scrollbar(running_frame, orient="vertical", command=self.session_tree.yview)
        tree_scroll.grid(row=1, column=1, sticky="ns")
        self.session_tree.configure(yscrollcommand=tree_scroll.set)

        control_row = ttk.Frame(running_frame)
        control_row.grid(row=2, column=0, columnspan=2, sticky="w", pady=(8, 0))
        session_group = ttk.LabelFrame(control_row, text="会话控制", padding=(8, 6))
        session_group.grid(row=0, column=0, sticky="w")
        ttk.Button(session_group, text="停止选中策略", command=self.stop_selected_session).grid(row=0, column=0)
        ttk.Button(session_group, text="暂停选中策略", command=self.pause_selected_session).grid(
            row=0, column=1, padx=(8, 0)
        )
        ttk.Button(session_group, text="恢复选中策略", command=self.recover_selected_session).grid(
            row=0, column=2, padx=(8, 0)
        )

        trade_group = ttk.LabelFrame(control_row, text="持仓处理", padding=(8, 6))
        trade_group.grid(row=0, column=1, sticky="w", padx=(10, 0))
        ttk.Button(trade_group, text="提前平仓", command=self.manual_flatten_selected_session).grid(row=0, column=0)
        ttk.Button(trade_group, text="修改止损", command=self.manual_adjust_selected_session_stop_loss).grid(
            row=0, column=1, padx=(8, 0)
        )
        ttk.Button(trade_group, text="恢复自动止盈", command=self.resume_selected_session_auto_management).grid(
            row=0, column=2, padx=(8, 0)
        )

        book_group = ttk.LabelFrame(control_row, text="历史账本", padding=(8, 6))
        book_group.grid(row=0, column=2, sticky="w", padx=(10, 0))
        ttk.Button(book_group, text="历史策略", command=self.open_strategy_history_window).grid(row=0, column=0)
        ttk.Button(book_group, text="策略总账本", command=self.open_strategy_book_window).grid(
            row=0, column=1, padx=(8, 0)
        )
        ttk.Button(book_group, text="API每日报表", command=self.open_daily_trade_report_window).grid(
            row=0, column=2, padx=(8, 0)
        )
        ttk.Button(book_group, text="导入最佳参数组合包", command=self.import_strategy_template_bundle).grid(
            row=0, column=3, padx=(8, 0)
        )

        utility_group = ttk.LabelFrame(control_row, text="清理与模式", padding=(8, 6))
        utility_group.grid(row=0, column=3, sticky="w", padx=(10, 0))
        ttk.Button(utility_group, text="清空已停止", command=self.clear_stopped_sessions).grid(row=0, column=0)
        ttk.Button(utility_group, textvariable=self._watch_mode_button_text, command=self.toggle_watch_mode).grid(
            row=0, column=1, padx=(8, 0)
        )

        detail_frame = ttk.LabelFrame(session_top_frame, text="选中策略详情", padding=12)
        detail_frame.grid(row=1, column=0, sticky="nsew", pady=(12, 0))
        detail_frame.columnconfigure(0, weight=1)
        detail_frame.rowconfigure(0, weight=1)
        self._session_detail_frame = detail_frame
        self._selected_session_detail = Text(
            detail_frame,
            height=5,
            wrap="word",
            font=("Microsoft YaHei UI", 9),
            relief="flat",
        )
        self._selected_session_detail.configure(spacing1=0, spacing2=1, spacing3=0)
        self._selected_session_detail.grid(row=0, column=0, sticky="nsew")
        detail_scroll = ttk.Scrollbar(detail_frame, orient="vertical", command=self._selected_session_detail.yview)
        detail_scroll.grid(row=0, column=1, sticky="ns")
        self._selected_session_detail.configure(yscrollcommand=detail_scroll.set)
        self._set_readonly_text(self._selected_session_detail, self.selected_session_text.get())

        positions_frame = ttk.LabelFrame(sessions_pane, text="账户持仓", padding=(5, 4))
        positions_frame.columnconfigure(0, weight=1)
        positions_frame.rowconfigure(2, weight=1)
        sessions_pane.add(positions_frame, weight=1)
        self._positions_frame = positions_frame

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
        self._positions_api_switch_badge_label = Label(
            header_row,
            textvariable=self._positions_api_switch_badge_text,
            font=("Microsoft YaHei UI", 8, "bold"),
            padx=10,
            pady=2,
            bd=0,
            relief="flat",
            bg="#e5e7eb",
            fg="#111827",
        )
        self._positions_api_switch_badge_label.grid(row=0, column=2, sticky="w", padx=(6, 0))
        self._positions_summary_label = Label(
            header_row,
            textvariable=self.positions_summary_text,
            font=("Microsoft YaHei UI", 8),
            anchor="w",
            justify="left",
            fg="#111827",
        )
        self._positions_summary_label.grid(row=0, column=3, sticky="ew", padx=(6, 6))
        action_row = ttk.Frame(header_row)
        action_row.grid(row=0, column=4, sticky="e")
        ttk.Button(action_row, text="刷新", command=self.refresh_positions).grid(row=0, column=0, padx=(0, 4))
        ttk.Button(action_row, text="持仓大窗", command=self.open_positions_zoom_window).grid(row=0, column=1, padx=(0, 4))
        ttk.Button(action_row, text="复制合约", command=self.copy_selected_position_symbol).grid(row=0, column=2)

        overview_row = ttk.Frame(positions_frame)
        overview_row.grid(row=1, column=0, sticky="ew", pady=(0, 4))
        for column in range(8):
            overview_row.columnconfigure(column, weight=1)
        self._build_metric_card(overview_row, 0, "总权益", self.account_total_equity_text, compact=True)
        self._build_metric_card(overview_row, 1, "调整后权益", self.account_adjusted_equity_text, compact=True)
        self._build_metric_card(overview_row, 2, "可用权益", self.account_available_equity_text, compact=True)
        self._build_metric_card(overview_row, 3, "未实现盈亏", self.account_upl_text, compact=True)
        self._build_metric_card(overview_row, 4, "持仓笔数", self.position_total_text, compact=True)
        self._build_metric_card(overview_row, 5, "浮动盈亏(USDT)", self.position_upl_text, compact=True)
        self._build_metric_card(overview_row, 6, "已实现盈亏", self.position_realized_text, compact=True)
        self._build_metric_card(overview_row, 7, "初始保证金(IMR)", self.position_margin_text, compact=True)

        position_table = ttk.Frame(positions_frame)
        position_table.grid(row=2, column=0, sticky="nsew")
        position_table.columnconfigure(0, weight=1)
        position_table.rowconfigure(0, weight=1)
        self._positions_table_frame = position_table

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
        self.position_tree.column("pos", width=170, anchor="e")
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

        log_frame = ttk.LabelFrame(self.root, text="运行日志", padding=10)
        log_frame.grid(row=2, column=0, sticky="nsew", padx=16, pady=(0, 16))
        log_frame.columnconfigure(0, weight=1)
        log_frame.rowconfigure(0, weight=1)
        self._log_frame = log_frame

        self.log_text = Text(log_frame, height=18, wrap="word", font=("Consolas", 9))
        self.log_text.configure(spacing1=0, spacing2=1, spacing3=0)
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
        self._apply_layout_sash_positions()

    def _update_launcher_selection_summary(self, *_args: object) -> None:
        strategy_name = str(self.strategy_name.get() or "").strip() or "未选择策略"
        symbol = str(self.symbol.get() or "").strip().upper() or "未选择标的"
        bar = str(self.bar.get() or "").strip() or "-"
        direction = str(self.signal_mode_label.get() or "").strip() or "-"
        self.launcher_strategy_title_text.set(strategy_name)
        self.launcher_selection_summary_text.set(f"{symbol} | 周期 {bar} | 方向 {direction}")

    def _apply_initial_detail_visibility(self) -> None:
        self._apply_launcher_manual_visibility()
        self._apply_watch_mode_layout()
        self._apply_launcher_panel_visibility()

    def toggle_launcher_panel(self) -> None:
        self._launcher_panel_visible = not self._launcher_panel_visible
        self._apply_launcher_panel_visibility()
        self._apply_layout_sash_positions()

    def _apply_launcher_panel_visibility(self) -> None:
        pane = self._main_body_pane
        launcher_frame = self._launcher_frame
        if pane is None or launcher_frame is None:
            return
        try:
            panes = tuple(str(item) for item in pane.panes())
            launcher_id = str(launcher_frame)
            if self._launcher_panel_visible:
                if launcher_id not in panes:
                    pane.insert(0, launcher_frame, weight=1)
                self._launcher_panel_toggle_text.set("隐藏左栏")
            else:
                if launcher_id in panes:
                    pane.forget(launcher_frame)
                self._launcher_panel_toggle_text.set("显示左栏")
        except Exception:
            return

    def toggle_launcher_manual_controls(self) -> None:
        self._launcher_manual_visible = not self._launcher_manual_visible
        self._apply_launcher_manual_visibility()
        self._apply_layout_sash_positions()

    def toggle_watch_mode(self) -> None:
        self._watch_mode_enabled = not self._watch_mode_enabled
        self._apply_watch_mode_layout()
        self._apply_layout_sash_positions()

    def _apply_launcher_manual_visibility(self) -> None:
        launcher_frame = self._launcher_frame
        start_frame = self._launcher_start_frame
        info_frame = self._launcher_strategy_info_frame
        if start_frame is None or info_frame is None:
            return
        if self._watch_mode_enabled:
            start_frame.grid_remove()
            info_frame.grid_remove()
            if launcher_frame is not None:
                launcher_frame.rowconfigure(1, weight=0)
                launcher_frame.rowconfigure(2, weight=0)
            self._launcher_manual_toggle_text.set("展开手动参数")
            return
        if self._launcher_manual_visible:
            start_frame.grid()
            info_frame.grid_remove()
            if launcher_frame is not None:
                launcher_frame.rowconfigure(1, weight=1)
                launcher_frame.rowconfigure(2, weight=0)
            self._launcher_manual_toggle_text.set("收起手动参数")
        else:
            start_frame.grid_remove()
            info_frame.grid_remove()
            if launcher_frame is not None:
                launcher_frame.rowconfigure(1, weight=0)
                launcher_frame.rowconfigure(2, weight=0)
            self._launcher_manual_toggle_text.set("展开手动参数")

    def _apply_watch_mode_layout(self) -> None:
        self._watch_mode_button_text.set("退出盯盘" if self._watch_mode_enabled else "盯盘模式")
        if self._launcher_compact_frame is not None:
            if self._watch_mode_enabled:
                self._launcher_compact_frame.grid_remove()
            else:
                self._launcher_compact_frame.grid()
        if self._launcher_watch_rail_frame is not None:
            if self._watch_mode_enabled:
                self._launcher_watch_rail_frame.grid()
            else:
                self._launcher_watch_rail_frame.grid_remove()
        self._apply_launcher_manual_visibility()
        if self._session_detail_frame is not None:
            if self._watch_mode_enabled:
                self._session_detail_frame.grid_remove()
            else:
                self._session_detail_frame.grid()
        if self._positions_table_frame is not None:
            self._positions_table_frame.grid()
        if getattr(self, "log_text", None) is not None:
            try:
                self.log_text.configure(height=5 if self._watch_mode_enabled else 10)
            except TclError:
                pass

    def _apply_layout_sash_positions(self) -> None:
        try:
            self.root.update_idletasks()
        except Exception:
            pass
        try:
            if (
                getattr(self, "_launcher_panel_visible", True)
                and self._main_body_pane is not None
                and self._main_body_pane.winfo_exists()
            ):
                total_width = self._main_body_pane.winfo_width()
                if total_width > 1200:
                    if self._watch_mode_enabled:
                        self._main_body_pane.sashpos(0, 84)
                    elif self._launcher_manual_visible:
                        self._main_body_pane.sashpos(0, int(total_width * Decimal("0.30")))
                    else:
                        self._main_body_pane.sashpos(0, int(total_width * Decimal("0.16")))
            if self._sessions_pane is not None and self._sessions_pane.winfo_exists():
                total_height = self._sessions_pane.winfo_height()
                if total_height > 600:
                    self._sessions_pane.sashpos(0, int(total_height * Decimal("0.67")))
        except Exception:
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

    def _refresh_home_account_snapshot_if_needed(self, *, force: bool = False, max_age_seconds: int = 30) -> None:
        credentials = self._current_credentials_or_none()
        if credentials is None:
            return
        active_profile = (credentials.profile_name or self._current_credential_profile()).strip()
        active_environment = self._positions_effective_environment or ENV_OPTIONS[self.environment_label.get()]
        profile_mismatch = active_profile != self._latest_account_info_profile_name
        environment_mismatch = active_environment != self._latest_account_info_environment
        stale = True
        if self._account_info_last_refresh_at is not None:
            stale = (datetime.now() - self._account_info_last_refresh_at).total_seconds() >= max_age_seconds
        should_refresh = force or self._latest_account_overview is None or profile_mismatch or environment_mismatch or stale
        if should_refresh:
            self.refresh_account_info()

    @staticmethod
    def _account_overview_cache_key(profile_name: str, environment: str) -> tuple[str, str]:
        normalized_profile = str(profile_name or "").strip() or DEFAULT_CREDENTIAL_PROFILE_NAME
        normalized_environment = str(environment or "").strip().lower()
        if normalized_environment not in {"demo", "live"}:
            normalized_environment = "demo"
        return normalized_profile, normalized_environment

    def _session_account_overview_key(self, session: StrategySession) -> tuple[str, str]:
        profile_name = str(getattr(session, "api_name", "") or "").strip() or self._current_credential_profile().strip()
        fallback_environment = str(getattr(getattr(session, "config", None), "environment", "") or "").strip().lower()
        environment = self._credential_profile_environment_value(profile_name, fallback=fallback_environment)
        return self._account_overview_cache_key(profile_name, environment or fallback_environment or "demo")

    def _running_session_account_overview_keys(self) -> tuple[tuple[str, str], ...]:
        keys: list[tuple[str, str]] = []
        seen: set[tuple[str, str]] = set()
        for session in self.sessions.values():
            if not self._session_counts_toward_running_summary(session):
                continue
            key = self._session_account_overview_key(session)
            if key in seen:
                continue
            seen.add(key)
            keys.append(key)
        return tuple(keys)

    def _account_overview_cache_entry_for_session(self, session: StrategySession) -> AccountOverviewCacheEntry | None:
        return self._account_overview_cache_by_key.get(self._session_account_overview_key(session))

    def _session_account_total_equity_text(self, session: StrategySession) -> str:
        entry = self._account_overview_cache_entry_for_session(session)
        if entry is None:
            return "-"
        return _format_optional_usdt_precise(entry.overview.total_equity, places=2, with_sign=False)

    def _load_account_equity_curve_records_cached(self, profile_name: str, environment: str) -> list[dict[str, object]]:
        key = self._account_overview_cache_key(profile_name, environment)
        cached = self._account_equity_curve_records_by_key.get(key)
        if cached is None:
            cached = list(load_account_equity_curve_records(key[0], key[1]))
            self._account_equity_curve_records_by_key[key] = cached
        return cached

    @staticmethod
    def _account_equity_curve_recorded_at(record: dict[str, object]) -> datetime | None:
        text = str(record.get("time", "") or "").strip()
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    @staticmethod
    def _account_equity_event_time_utc(value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            return value.astimezone().astimezone(timezone.utc)
        return value.astimezone(timezone.utc)

    def _account_equity_curve_trade_records(
        self,
        key: tuple[str, str],
        *,
        start_time: datetime,
        end_time: datetime,
    ) -> list[StrategyTradeLedgerRecord]:
        profile_name, environment = key
        matched: list[StrategyTradeLedgerRecord] = []
        for record in self._strategy_trade_ledger_records:
            if str(record.api_name or "").strip().casefold() != profile_name.casefold():
                continue
            if str(record.environment or "").strip().lower() != environment:
                continue
            opened_at = self._account_equity_event_time_utc(record.opened_at)
            closed_at = self._account_equity_event_time_utc(record.closed_at)
            if closed_at is None:
                continue
            interval_start = opened_at or closed_at
            if closed_at < start_time or interval_start > end_time:
                continue
            matched.append(record)
        matched.sort(
            key=lambda item: (
                (self._account_equity_event_time_utc(item.closed_at) or datetime.min.replace(tzinfo=timezone.utc)).timestamp(),
                item.record_id,
            )
        )
        return matched

    @staticmethod
    def _account_equity_curve_nearest_point(
        points: list[tuple[datetime, Decimal]],
        event_time: datetime,
    ) -> tuple[datetime, Decimal] | None:
        if not points:
            return None
        return min(points, key=lambda item: abs((item[0] - event_time).total_seconds()))

    @staticmethod
    def _account_equity_curve_symbol_palette() -> tuple[str, ...]:
        return (
            "#2563eb",
            "#d97706",
            "#16803a",
            "#7c3aed",
            "#db2777",
            "#0891b2",
            "#b45309",
            "#4f46e5",
            "#be123c",
            "#0f766e",
            "#9333ea",
            "#4d7c0f",
        )

    @classmethod
    def _account_equity_curve_symbol_color(cls, symbol: str) -> str:
        """Return the preferred stable color for one instrument."""

        palette = cls._account_equity_curve_symbol_palette()
        normalized = str(symbol or "-").strip().upper()
        index = sum((position + 1) * ord(char) for position, char in enumerate(normalized)) % len(palette)
        return palette[index]

    def _account_equity_curve_symbol_colors(
        self,
        state: AccountEquityCurveWindowState,
        symbols: list[str],
    ) -> dict[str, str]:
        """Give simultaneous instruments distinct colors while retaining earlier assignments."""

        palette = self._account_equity_curve_symbol_palette()
        active_symbols = sorted({str(symbol or "-").strip() or "-" for symbol in symbols})
        used_colors = set(state.symbol_colors.values())
        for symbol in active_symbols:
            if symbol in state.symbol_colors:
                continue
            preferred = self._account_equity_curve_symbol_color(symbol)
            start_index = palette.index(preferred)
            color = next((palette[(start_index + offset) % len(palette)] for offset in range(len(palette)) if palette[(start_index + offset) % len(palette)] not in used_colors), preferred)
            state.symbol_colors[symbol] = color
            used_colors.add(color)
        return {symbol: state.symbol_colors[symbol] for symbol in active_symbols}

    def _set_account_equity_curve_event_mode(self, key: tuple[str, str]) -> None:
        state = self._account_equity_curve_windows.get(key)
        if state is None:
            return
        state.selected_trade_record_ids = ()
        self._render_account_equity_curve_window(key)

    def _select_account_equity_curve_trades(self, key: tuple[str, str], record_ids: tuple[str, ...]) -> None:
        state = self._account_equity_curve_windows.get(key)
        if state is None:
            return
        state.selected_trade_record_ids = tuple(dict.fromkeys(record_ids))
        self._render_account_equity_curve_window(key)

    def _account_equity_curve_selected_detail_text(
        self,
        records: list[StrategyTradeLedgerRecord],
        points: list[tuple[datetime, Decimal]],
        selected_record_ids: tuple[str, ...],
    ) -> str:
        if not selected_record_ids:
            return "提示：点击平仓点查看策略、品种、本轮盈亏和持仓期间的账户权益变化。"
        by_id = {record.record_id: record for record in records}
        selected = [by_id[record_id] for record_id in selected_record_ids if record_id in by_id]
        if not selected:
            return "提示：点击平仓点查看策略、品种、本轮盈亏和持仓期间的账户权益变化。"
        if len(selected) > 1:
            brief = "；".join(f"{record.symbol} {record.session_id}" for record in selected[:4])
            suffix = "……" if len(selected) > 4 else ""
            return f"已选 {len(selected)} 笔相邻平仓：{brief}{suffix}。可切换到较短时间范围查看单笔详情。"

        record = selected[0]
        opened_at = self._account_equity_event_time_utc(record.opened_at)
        closed_at = self._account_equity_event_time_utc(record.closed_at)
        open_point = self._account_equity_curve_nearest_point(points, opened_at) if opened_at is not None else None
        close_point = self._account_equity_curve_nearest_point(points, closed_at) if closed_at is not None else None
        pnl_text = "本轮盈亏 -"
        if record.net_pnl is not None:
            pnl = record.net_pnl.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            pnl_text = f"本轮盈亏 {('+' if pnl > 0 else '')}{pnl:.2f}U"
        stage_text = "同期权益 -"
        if open_point is not None and close_point is not None:
            delta = (close_point[1] - open_point[1]).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            stage_text = f"同期权益 {('+' if delta > 0 else '')}{delta:.2f}U"
        closed_text = closed_at.astimezone().strftime("%m-%d %H:%M") if closed_at is not None else "-"
        strategy_name = str(record.strategy_name or "-").strip() or "-"
        return f"已选 {record.symbol} | {record.session_id} | {strategy_name} | 平仓 {closed_text} | {pnl_text} | {stage_text}"

    def _draw_account_equity_curve_trade_impacts(
        self,
        canvas: Canvas,
        key: tuple[str, str],
        points: list[tuple[datetime, Decimal]],
        *,
        start_time: datetime,
        end_time: datetime,
        x_for_time: Callable[[datetime], float],
        y_for_value: Callable[[Decimal], float],
        left: float,
        top: float,
        right: float,
        bottom: float,
        event_mode: str,
        selected_record_ids: tuple[str, ...],
        symbol_colors: dict[str, str],
    ) -> None:
        """Draw local strategy events compactly; details appear only after a click."""

        if not points:
            return
        records = self._account_equity_curve_trade_records(key, start_time=start_time, end_time=end_time)
        selected_ids = set(selected_record_ids)

        def _color_for_symbol(symbol: str) -> str:
            normalized = str(symbol or "-").strip() or "-"
            return symbol_colors.get(normalized, self._account_equity_curve_symbol_color(normalized))

        def _bind_select(item_ids: tuple[int, ...], record_ids: tuple[str, ...]) -> None:
            tag = f"account-equity-trade:{'|'.join(record_ids)}"
            for item_id in item_ids:
                canvas.addtag_withtag(tag, item_id)
            canvas.tag_bind(
                tag,
                "<Button-1>",
                lambda _event, target_key=key, target_ids=record_ids: self._select_account_equity_curve_trades(target_key, target_ids),
            )

        def _draw_close_marker(x: float, y: float, *, color: str, result: Decimal | None, selected: bool) -> tuple[int, ...]:
            radius = 7 if selected else 5
            fill = color if selected else "#ffffff"
            if result is not None and result > 0:
                marker = canvas.create_polygon(x, y - radius, x + radius, y + radius, x - radius, y + radius, outline=color, fill=fill, width=2)
            elif result is not None and result < 0:
                marker = canvas.create_polygon(x - radius, y - radius, x + radius, y - radius, x, y + radius, outline=color, fill=fill, width=2)
            else:
                marker = canvas.create_polygon(x, y - radius, x + radius, y, x, y + radius, x - radius, y, outline=color, fill=fill, width=2)
            return (marker,)

        close_events: list[dict[str, object]] = []
        for record in records:
            closed_at = self._account_equity_event_time_utc(record.closed_at)
            if closed_at is None or not (start_time <= closed_at <= end_time):
                continue
            close_point = self._account_equity_curve_nearest_point(points, closed_at)
            if close_point is None:
                continue
            close_events.append({"record": record, "x": float(x_for_time(closed_at)), "y": float(y_for_value(close_point[1]))})

        # Group only points that would otherwise overlap on the current canvas.
        clusters: list[list[dict[str, object]]] = []
        for event in sorted(close_events, key=lambda item: (float(item["x"]), float(item["y"]))):
            if clusters:
                last_cluster = clusters[-1]
                center_x = sum(float(item["x"]) for item in last_cluster) / len(last_cluster)
                center_y = sum(float(item["y"]) for item in last_cluster) / len(last_cluster)
                if abs(float(event["x"]) - center_x) <= 14 and abs(float(event["y"]) - center_y) <= 14:
                    last_cluster.append(event)
                    continue
            clusters.append([event])

        # A selected trade alone reveals its holding segment and opening point.
        for record in records:
            if record.record_id not in selected_ids:
                continue
            opened_at = self._account_equity_event_time_utc(record.opened_at)
            closed_at = self._account_equity_event_time_utc(record.closed_at)
            if opened_at is None or closed_at is None:
                continue
            segment_start = max(opened_at, start_time)
            segment_end = min(closed_at, end_time)
            if segment_start > segment_end:
                continue
            open_point = self._account_equity_curve_nearest_point(points, segment_start)
            close_point = self._account_equity_curve_nearest_point(points, segment_end)
            if open_point is None or close_point is None:
                continue
            color = _color_for_symbol(record.symbol)
            canvas.create_line(
                float(x_for_time(segment_start)),
                float(y_for_value(open_point[1])),
                float(x_for_time(segment_end)),
                float(y_for_value(close_point[1])),
                fill=color,
                width=2,
                dash=(5, 3),
            )
            if start_time <= opened_at <= end_time:
                x = float(x_for_time(opened_at))
                y = float(y_for_value(open_point[1]))
                canvas.create_oval(x - 5, y - 5, x + 5, y + 5, outline=color, fill="#ffffff", width=2)

        if event_mode == "both":
            for record in records:
                opened_at = self._account_equity_event_time_utc(record.opened_at)
                if opened_at is None or not (start_time <= opened_at <= end_time):
                    continue
                point = self._account_equity_curve_nearest_point(points, opened_at)
                if point is None:
                    continue
                x = float(x_for_time(opened_at))
                y = float(y_for_value(point[1]))
                color = _color_for_symbol(record.symbol)
                marker = canvas.create_oval(x - 4, y - 4, x + 4, y + 4, outline=color, fill="#ffffff", width=2)
                _bind_select((marker,), (record.record_id,))

        if event_mode == "off":
            return
        for cluster in clusters:
            cluster_records = [item["record"] for item in cluster]
            record_ids = tuple(record.record_id for record in cluster_records)
            x = sum(float(item["x"]) for item in cluster) / len(cluster)
            y = sum(float(item["y"]) for item in cluster) / len(cluster)
            if len(cluster) == 1:
                record = cluster_records[0]
                items = _draw_close_marker(
                    x,
                    y,
                    color=_color_for_symbol(record.symbol),
                    result=record.net_pnl,
                    selected=record.record_id in selected_ids,
                )
                _bind_select(items, record_ids)
                continue

            selected = bool(selected_ids.intersection(record_ids))
            radius = 10 if selected else 9
            fill = "#374151" if selected else "#ffffff"
            marker = canvas.create_oval(x - radius, y - radius, x + radius, y + radius, outline="#374151", fill=fill, width=2)
            count = canvas.create_text(x, y, text=str(len(cluster)), fill="#ffffff" if selected else "#374151", font=("Microsoft YaHei UI", 8, "bold"))
            _bind_select((marker, count), record_ids)


    def _maybe_record_account_equity_curve_sample(
        self,
        profile_name: str,
        environment: str,
        overview: OkxAccountOverview,
        *,
        sampled_at: datetime | None = None,
        min_interval_seconds: int = 3600,
    ) -> bool:
        key = self._account_overview_cache_key(profile_name, environment)
        records = list(self._load_account_equity_curve_records_cached(key[0], key[1]))
        sample_time = sampled_at or datetime.now(timezone.utc)
        if sample_time.tzinfo is None:
            sample_time = sample_time.replace(tzinfo=timezone.utc)
        else:
            sample_time = sample_time.astimezone(timezone.utc)
        if records:
            latest_at = self._account_equity_curve_recorded_at(records[-1])
            if latest_at is not None and (sample_time - latest_at).total_seconds() < min_interval_seconds:
                return False
        records.append(
            {
                "time": sample_time.isoformat(timespec="seconds").replace("+00:00", "Z"),
                "total_equity": str(overview.total_equity) if overview.total_equity is not None else None,
                "adjusted_equity": str(overview.adjusted_equity) if overview.adjusted_equity is not None else None,
                "available_equity": str(overview.available_equity) if overview.available_equity is not None else None,
                "upl": str(overview.unrealized_pnl) if overview.unrealized_pnl is not None else None,
            }
        )
        self._account_equity_curve_records_by_key[key] = records
        save_account_equity_curve_records(key[0], key[1], records)
        return True

    def _store_account_overview_cache_entry(
        self,
        profile_name: str,
        environment: str,
        overview: OkxAccountOverview,
        *,
        config: OkxAccountConfig | None = None,
        refreshed_at: datetime | None = None,
        source: str = "rest",
    ) -> None:
        key = self._account_overview_cache_key(profile_name, environment)
        existing = self._account_overview_cache_by_key.get(key)
        self._account_overview_cache_by_key[key] = AccountOverviewCacheEntry(
            profile_name=key[0],
            environment=key[1],
            overview=overview,
            config=config if config is not None else (existing.config if existing is not None else None),
            refreshed_at=refreshed_at or datetime.now(),
            source=source,
        )
        self._account_overview_last_error_by_key.pop(key, None)
        self._maybe_record_account_equity_curve_sample(key[0], key[1], overview, sampled_at=refreshed_at)
        matched_session = False
        for session in self.sessions.values():
            if self._session_account_overview_key(session) != key:
                continue
            matched_session = True
            self._upsert_session_row(session, reorder=False)
        sort_column = str(getattr(self, "_running_session_sort_column", "") or "")
        if matched_session and sort_column == "account_equity":
            self._refresh_running_session_tree()
        elif matched_session and hasattr(self, "_apply_running_session_tree_sort_order"):
            self._apply_running_session_tree_sort_order()
        window_state = self._account_equity_curve_windows.get(key)
        if window_state is not None and _widget_exists(window_state.window):
            self._refresh_account_equity_curve_window(key)

    def _apply_session_account_overview_cache_entry(
        self,
        profile_name: str,
        environment: str,
        overview: OkxAccountOverview,
        *,
        source: str,
    ) -> None:
        key = self._account_overview_cache_key(profile_name, environment)
        self._account_overview_refreshing_keys.discard(key)
        self._store_account_overview_cache_entry(
            key[0],
            key[1],
            overview,
            refreshed_at=datetime.now(),
            source=source,
        )

    def _apply_session_account_overview_cache_error(self, profile_name: str, environment: str, message: str) -> None:
        key = self._account_overview_cache_key(profile_name, environment)
        self._account_overview_refreshing_keys.discard(key)
        self._account_overview_last_error_by_key[key] = _format_network_error_message(message)
        window_state = self._account_equity_curve_windows.get(key)
        if window_state is not None and _widget_exists(window_state.window):
            self._refresh_account_equity_curve_window(key)

    def _refresh_session_account_overview_worker(
        self,
        credentials: Credentials,
        profile_name: str,
        environment: str,
    ) -> None:
        source = "rest"
        try:
            cached_overview = self.client.get_cached_private_account_overview(credentials, environment=environment)
            if cached_overview is not None:
                _version, overview = cached_overview
                source = "ws"
            else:
                overview = self.client.get_account_overview(
                    credentials,
                    environment=environment,
                    prefer_cache=False,
                )
        except Exception as exc:
            message = str(exc)
            if "50101" in message and "current environment" in message:
                alternate = "live" if environment == "demo" else "demo"
                try:
                    cached_overview = self.client.get_cached_private_account_overview(credentials, environment=alternate)
                    if cached_overview is not None:
                        _version, overview = cached_overview
                        source = "ws"
                    else:
                        overview = self.client.get_account_overview(
                            credentials,
                            environment=alternate,
                            prefer_cache=False,
                        )
                except Exception:
                    self.root.after(
                        0,
                        lambda target_profile=profile_name, target_environment=environment, error_text=message: self._apply_session_account_overview_cache_error(
                            target_profile,
                            target_environment,
                            error_text,
                        ),
                    )
                    return
                self.root.after(
                    0,
                    lambda target_profile=profile_name, target_environment=environment, target_overview=overview, target_source=source: self._apply_session_account_overview_cache_entry(
                        target_profile,
                        target_environment,
                        target_overview,
                        source=target_source,
                    ),
                )
                return
            self.root.after(
                0,
                lambda target_profile=profile_name, target_environment=environment, error_text=message: self._apply_session_account_overview_cache_error(
                    target_profile,
                    target_environment,
                    error_text,
                ),
            )
            return
        self.root.after(
            0,
            lambda target_profile=profile_name, target_environment=environment, target_overview=overview, target_source=source: self._apply_session_account_overview_cache_entry(
                target_profile,
                target_environment,
                target_overview,
                source=target_source,
            ),
        )

    def _refresh_running_session_account_equities_if_needed(
        self,
        *,
        force: bool = False,
        target_keys: tuple[tuple[str, str], ...] | None = None,
        max_age_seconds: int = 30,
        min_scan_interval_seconds: int = 5,
    ) -> None:
        now = datetime.now()
        if not force and self._running_session_account_equity_last_scan_at is not None:
            since = (now - self._running_session_account_equity_last_scan_at).total_seconds()
            if since < min_scan_interval_seconds:
                return
        self._running_session_account_equity_last_scan_at = now
        keys = target_keys if target_keys is not None else self._running_session_account_overview_keys()
        for key in keys:
            if key in self._account_overview_refreshing_keys:
                continue
            cache_entry = self._account_overview_cache_by_key.get(key)
            if not force and cache_entry is not None and cache_entry.refreshed_at is not None:
                age = (now - cache_entry.refreshed_at).total_seconds()
                if age < max_age_seconds:
                    continue
            credentials = self._credentials_for_profile_or_none(key[0])
            if credentials is None:
                continue
            self._account_overview_refreshing_keys.add(key)
            threading.Thread(
                target=self._refresh_session_account_overview_worker,
                args=(credentials, key[0], key[1]),
                daemon=True,
            ).start()

    def open_account_equity_curve_window_for_session(self, session: StrategySession) -> None:
        key = self._session_account_overview_key(session)
        self.open_account_equity_curve_window(key[0], key[1])

    def open_account_equity_curve_window(self, profile_name: str, environment: str) -> None:
        key = self._account_overview_cache_key(profile_name, environment)
        existing = self._account_equity_curve_windows.get(key)
        if existing is not None and _widget_exists(existing.window):
            existing.window.focus_force()
            self._refresh_account_equity_curve_window(key)
            self._refresh_running_session_account_equities_if_needed(force=True, target_keys=(key,))
            return

        window = Toplevel(self.root)
        window.title(f"账户权益曲线 - {key[0]} - {'实盘 live' if key[1] == 'live' else '模拟盘 demo'}")
        apply_adaptive_window_geometry(
            window,
            width_ratio=0.64,
            height_ratio=0.56,
            min_width=920,
            min_height=520,
            max_width=1440,
            max_height=920,
        )
        container = ttk.Frame(window, padding=12)
        container.pack(fill="both", expand=True)
        container.columnconfigure(0, weight=1)
        container.rowconfigure(1, weight=1)

        header = ttk.Frame(container)
        header.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        header.columnconfigure(0, weight=1)
        summary_text = StringVar(value="正在获取账户权益...")
        ttk.Label(header, textvariable=summary_text, anchor="w", justify="left").grid(row=0, column=0, sticky="w")
        tools = ttk.Frame(header)
        tools.grid(row=0, column=1, sticky="e")
        range_var = StringVar(value="24h")
        for index, (label, value) in enumerate((("24H", "24h"), ("7D", "7d"), ("30D", "30d"), ("全部", "all"))):
            ttk.Radiobutton(
                tools,
                text=label,
                value=value,
                variable=range_var,
                command=lambda target_key=key: self._render_account_equity_curve_window(target_key),
            ).grid(row=0, column=index, padx=(0, 6) if index < 3 else (0, 0))
        event_mode_var = StringVar(value="close")
        ttk.Label(tools, text="标记").grid(row=0, column=4, padx=(12, 3))
        for index, (label, value) in enumerate((("仅平仓", "close"), ("开平仓", "both"), ("关闭", "off"))):
            ttk.Radiobutton(
                tools,
                text=label,
                value=value,
                variable=event_mode_var,
                command=lambda target_key=key: self._set_account_equity_curve_event_mode(target_key),
            ).grid(row=0, column=5 + index, padx=(0, 5) if index < 2 else (0, 0))
        ttk.Button(
            tools,
            text="刷新",
            command=lambda target_key=key: self._refresh_running_session_account_equities_if_needed(
                force=True,
                target_keys=(target_key,),
            ),
        ).grid(row=0, column=8, padx=(12, 6))
        ttk.Button(tools, text="关闭", command=lambda target_key=key: self._close_account_equity_curve_window(target_key)).grid(
            row=0,
            column=9,
        )
        detail_text = StringVar(value="提示：点击平仓点查看策略、品种、本轮盈亏和持仓期间的账户权益变化。")
        ttk.Label(header, textvariable=detail_text, anchor="w", justify="left", foreground="#4b5563").grid(
            row=1,
            column=0,
            columnspan=2,
            sticky="ew",
            pady=(4, 0),
        )

        canvas = Canvas(container, background="#ffffff", highlightthickness=0, width=980, height=460)
        canvas.grid(row=1, column=0, sticky="nsew")
        state = AccountEquityCurveWindowState(
            profile_name=key[0],
            environment=key[1],
            window=window,
            canvas=canvas,
            summary_text=summary_text,
            range_var=range_var,
            event_mode_var=event_mode_var,
            detail_text=detail_text,
        )
        self._account_equity_curve_windows[key] = state
        window.protocol("WM_DELETE_WINDOW", lambda target_key=key: self._close_account_equity_curve_window(target_key))
        canvas.bind("<Configure>", lambda *_args, target_key=key: self._render_account_equity_curve_window(target_key))
        self._refresh_account_equity_curve_window(key)
        self._refresh_running_session_account_equities_if_needed(force=True, target_keys=(key,))

    def _close_account_equity_curve_window(self, key: tuple[str, str]) -> None:
        state = self._account_equity_curve_windows.pop(key, None)
        if state is not None and _widget_exists(state.window):
            state.window.destroy()

    def _refresh_account_equity_curve_window(self, key: tuple[str, str]) -> None:
        state = self._account_equity_curve_windows.get(key)
        if state is None or not _widget_exists(state.window):
            return
        cache_entry = self._account_overview_cache_by_key.get(key)
        if cache_entry is not None:
            self._maybe_record_account_equity_curve_sample(
                key[0],
                key[1],
                cache_entry.overview,
                sampled_at=cache_entry.refreshed_at,
            )
        self._render_account_equity_curve_window(key)

    def _render_account_equity_curve_window(self, key: tuple[str, str]) -> None:
        state = self._account_equity_curve_windows.get(key)
        if state is None or not _widget_exists(state.canvas):
            return
        cache_entry = self._account_overview_cache_by_key.get(key)
        error_text = self._account_overview_last_error_by_key.get(key, "")
        if cache_entry is not None:
            refreshed_text = cache_entry.refreshed_at.strftime("%m-%d %H:%M:%S") if cache_entry.refreshed_at else "-"
            source_label = "WS缓存" if cache_entry.source == "ws" else "REST"
            state.summary_text.set(
                " | ".join(
                    (
                        f"总权益 {_format_optional_usdt_precise(cache_entry.overview.total_equity, places=2, with_sign=False)}",
                        f"调整后 {_format_optional_usdt_precise(cache_entry.overview.adjusted_equity, places=2, with_sign=False)}",
                        f"可用 {_format_optional_usdt_precise(cache_entry.overview.available_equity, places=2, with_sign=False)}",
                        f"未实现 {_format_optional_usdt_precise(cache_entry.overview.unrealized_pnl, places=2)}",
                        f"最近刷新 {refreshed_text}",
                        f"来源 {source_label}",
                    )
                )
            )
        elif error_text:
            state.summary_text.set(f"账户权益读取失败：{error_text}")
        else:
            state.summary_text.set("正在获取账户权益...")

        all_records = list(self._load_account_equity_curve_records_cached(key[0], key[1]))
        now_utc = datetime.now(timezone.utc)
        range_value = str(state.range_var.get() or "24h").strip().lower()
        range_windows = {
            "24h": timedelta(hours=24),
            "7d": timedelta(days=7),
            "30d": timedelta(days=30),
        }
        cutoff = now_utc - range_windows[range_value] if range_value in range_windows else None
        points: list[tuple[datetime, Decimal]] = []
        for record in all_records:
            recorded_at = self._account_equity_curve_recorded_at(record)
            if recorded_at is None:
                continue
            if cutoff is not None and recorded_at < cutoff:
                continue
            raw_value = record.get("total_equity")
            if raw_value in {None, ""}:
                continue
            try:
                equity_value = Decimal(str(raw_value))
            except (InvalidOperation, TypeError, ValueError):
                continue
            points.append((recorded_at, equity_value))

        canvas = state.canvas
        canvas.delete("all")
        width = max(int(canvas.winfo_width() or 0), int(float(canvas.cget("width") or 0) or 980))
        height = max(int(canvas.winfo_height() or 0), int(float(canvas.cget("height") or 0) or 460))
        if width <= 40 or height <= 40:
            return
        if not points:
            empty_text = "当前区间暂无权益采样点。"
            if not all_records:
                empty_text = "还没有账户权益采样点，等待刷新后会自动开始记录。"
            canvas.create_text(width / 2, height / 2, text=empty_text, fill="#6b7280", font=("Microsoft YaHei UI", 12))
            return

        left = 72
        top = 46
        right = max(left + 120, width - 24)
        bottom = max(top + 120, height - 52)
        min_value = min(value for _, value in points)
        max_value = max(value for _, value in points)
        if min_value == max_value:
            padding = max(abs(float(max_value)) * 0.02, 1.0)
            min_plot = float(min_value) - padding
            max_plot = float(max_value) + padding
        else:
            value_span = float(max_value - min_value)
            padding = max(value_span * 0.08, 1.0)
            min_plot = float(min_value) - padding
            max_plot = float(max_value) + padding
        start_time = points[0][0]
        end_time = points[-1][0]
        span_seconds = max((end_time - start_time).total_seconds(), 1.0)

        def _x(recorded_at: datetime) -> float:
            if len(points) == 1:
                return (left + right) / 2
            return left + ((recorded_at - start_time).total_seconds() / span_seconds) * (right - left)

        def _y(value: Decimal) -> float:
            ratio = 0.5 if max_plot == min_plot else (float(value) - min_plot) / (max_plot - min_plot)
            return bottom - ratio * (bottom - top)

        canvas.create_rectangle(left, top, right, bottom, outline="#d1d5db", width=1)
        for step in range(1, 4):
            y = top + ((bottom - top) * step / 4.0)
            canvas.create_line(left, y, right, y, fill="#eef2f7")

        line_points: list[float] = []
        for recorded_at, value in points:
            line_points.extend((_x(recorded_at), _y(value)))
        if len(line_points) >= 4:
            canvas.create_line(*line_points, fill="#2563eb", width=2, smooth=False)
        for recorded_at, value in (points[0], points[-1]) if len(points) > 1 else (points[0],):
            x = _x(recorded_at)
            y = _y(value)
            canvas.create_oval(x - 3, y - 3, x + 3, y + 3, fill="#2563eb", outline="")

        visible_records = self._account_equity_curve_trade_records(key, start_time=start_time, end_time=end_time)
        visible_record_ids = {record.record_id for record in visible_records}
        selected_record_ids = tuple(record_id for record_id in state.selected_trade_record_ids if record_id in visible_record_ids)
        if selected_record_ids != state.selected_trade_record_ids:
            state.selected_trade_record_ids = selected_record_ids
        state.detail_text.set(
            self._account_equity_curve_selected_detail_text(visible_records, points, state.selected_trade_record_ids)
        )
        visible_symbols = sorted({str(record.symbol or "-").strip() or "-" for record in visible_records})
        symbol_colors = self._account_equity_curve_symbol_colors(state, visible_symbols)
        self._draw_account_equity_curve_trade_impacts(
            canvas,
            key,
            points,
            start_time=start_time,
            end_time=end_time,
            x_for_time=_x,
            y_for_value=_y,
            left=left,
            top=top,
            right=right,
            bottom=bottom,
            event_mode=str(state.event_mode_var.get() or "close").strip().lower(),
            selected_record_ids=state.selected_trade_record_ids,
            symbol_colors=symbol_colors,
        )

        legend_x = left
        legend_y = 16
        result_legend_width = 116
        canvas.create_text(legend_x, legend_y, text="品种", anchor="w", fill="#6b7280", font=("Microsoft YaHei UI", 8))
        legend_x += 30
        max_legend_items = 7
        for index, symbol in enumerate(visible_symbols[:max_legend_items]):
            symbol_label = str(symbol).split("-", 1)[0] or symbol
            item_width = 18 + len(symbol_label) * 7
            if legend_x + item_width > right - result_legend_width:
                remaining = len(visible_symbols) - index
                canvas.create_text(legend_x, legend_y, text=f"+{remaining}种", anchor="w", fill="#6b7280", font=("Microsoft YaHei UI", 8))
                break
            color = symbol_colors[symbol]
            canvas.create_oval(legend_x, legend_y - 4, legend_x + 8, legend_y + 4, outline=color, fill=color)
            canvas.create_text(legend_x + 12, legend_y, text=symbol_label, anchor="w", fill=color, font=("Microsoft YaHei UI", 8))
            legend_x += item_width
        canvas.create_text(right, legend_y, text="▲盈利  ▼亏损  ◇未知", anchor="e", fill="#6b7280", font=("Microsoft YaHei UI", 8))

        canvas.create_text(left, top - 10, text=_format_optional_usdt_precise(Decimal(str(max_plot)), places=2, with_sign=False), anchor="w", fill="#374151", font=("Microsoft YaHei UI", 9))
        canvas.create_text(left, bottom + 10, text=_format_optional_usdt_precise(Decimal(str(min_plot)), places=2, with_sign=False), anchor="w", fill="#374151", font=("Microsoft YaHei UI", 9))
        canvas.create_text(left, bottom + 28, text=start_time.astimezone().strftime("%m-%d %H:%M"), anchor="w", fill="#6b7280", font=("Microsoft YaHei UI", 9))
        canvas.create_text(right, bottom + 28, text=end_time.astimezone().strftime("%m-%d %H:%M"), anchor="e", fill="#6b7280", font=("Microsoft YaHei UI", 9))
        latest_value = points[-1][1]
        canvas.create_text(
            right,
            top - 10,
            text=f"{len(points)} 点 | 最新 {_format_optional_usdt_precise(latest_value, places=2, with_sign=False)}",
            anchor="e",
            fill="#2563eb",
            font=("Microsoft YaHei UI", 9, "bold"),
        )

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
        self._account_info_api_switch_badge_label = Label(
            header,
            textvariable=self._account_info_api_switch_badge_text,
            font=("Microsoft YaHei UI", 9, "bold"),
            padx=10,
            pady=2,
            bd=0,
            relief="flat",
            bg="#e5e7eb",
            fg="#111827",
        )
        self._account_info_api_switch_badge_label.grid(row=0, column=1, sticky="w", padx=(0, 8))
        self._account_info_summary_label = Label(
            header,
            textvariable=self.account_info_summary_text,
            justify="left",
            anchor="w",
            fg="#111827",
        )
        self._account_info_summary_label.grid(row=0, column=2, sticky="w")
        self._refresh_api_switch_status_colors()
        action_row = ttk.Frame(header)
        action_row.grid(row=0, column=3, sticky="e")
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
        self._account_info_api_switch_badge_label = None
        self._account_info_summary_label = None
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
            self._account_info_last_refresh_at = None
            self._latest_account_info_profile_name = ""
            self._latest_account_info_environment = ""
            self._refresh_all_refresh_badges()
            return
        self._account_info_refreshing = True
        self._mark_api_switch_refresh_step("account_info", "running")
        self.account_info_summary_text.set("正在刷新账户信息...")
        environment = self._positions_effective_environment or ENV_OPTIONS[self.environment_label.get()]
        threading.Thread(
            target=self._refresh_account_info_worker,
            args=(credentials, environment),
            daemon=True,
        ).start()

    def _refresh_account_info_worker(self, credentials: Credentials, environment: str) -> None:
        source = "rest"
        try:
            cached_overview = self.client.get_cached_private_account_overview(credentials, environment=environment)
            if cached_overview is not None:
                _version, overview = cached_overview
                source = "ws"
            else:
                overview = self.client.get_account_overview(credentials, environment=environment, prefer_cache=False)
            config = self.client.get_account_config(credentials, environment=environment)
        except Exception as exc:
            message = str(exc)
            if "50101" in message and "current environment" in message:
                alternate = "live" if environment == "demo" else "demo"
                try:
                    cached_overview = self.client.get_cached_private_account_overview(credentials, environment=alternate)
                    if cached_overview is not None:
                        _version, overview = cached_overview
                        source = "ws"
                    else:
                        overview = self.client.get_account_overview(credentials, environment=alternate, prefer_cache=False)
                    config = self.client.get_account_config(credentials, environment=alternate)
                except Exception:
                    self.root.after(0, lambda: self._apply_account_info_error(message))
                    return
                summary = (
                    f"当前 API Key 与 {alternate} 环境匹配，已自动按 "
                    f"{'实盘' if alternate == 'live' else '模拟盘'} 读取账户信息。"
                )
                self.root.after(0, lambda: self._apply_account_info(overview, config, summary, alternate, source=source))
                return
            self.root.after(0, lambda: self._apply_account_info_error(message))
            return
        self.root.after(0, lambda: self._apply_account_info(overview, config, None, environment, source=source))

    def _apply_account_info(
        self,
        overview: OkxAccountOverview,
        config: OkxAccountConfig,
        summary_note: str | None,
        effective_environment: str,
        *,
        source: str = "rest",
    ) -> None:
        self._account_info_refreshing = False
        self._mark_api_switch_refresh_step("account_info", "done")
        self._latest_account_overview = overview
        self._latest_account_config = config
        self._account_info_last_refresh_at = datetime.now()
        self._latest_account_info_profile_name = self._current_credential_profile().strip()
        self._latest_account_info_environment = effective_environment
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
        self._store_account_overview_cache_entry(
            self._latest_account_info_profile_name,
            effective_environment,
            overview,
            config=config,
            refreshed_at=self._account_info_last_refresh_at,
            source=source,
        )
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
        self._mark_api_switch_refresh_step("account_info", "failed")
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
        ttk.Label(account_frame, text="切换密码").grid(row=row, column=0, sticky="w", pady=(12, 0))
        ttk.Label(
            account_frame,
            textvariable=self.credential_profile_password_status_text,
        ).grid(row=row, column=1, sticky="w", padx=(0, 16), pady=(12, 0))
        password_buttons = ttk.Frame(account_frame)
        password_buttons.grid(row=row, column=2, columnspan=2, sticky="e", pady=(12, 0))
        ttk.Button(password_buttons, text="解锁当前", command=self._unlock_current_api_profile).grid(
            row=0, column=0, padx=(0, 8)
        )
        ttk.Button(password_buttons, text="设置/更新密码", command=self._set_current_api_profile_switch_password).grid(
            row=0, column=1, padx=(0, 8)
        )
        ttk.Button(password_buttons, text="清除密码", command=self._clear_current_api_profile_switch_password).grid(
            row=0, column=2
        )

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
        ttk.Label(account_frame, text="Trade Mode").grid(row=row, column=0, sticky="w", pady=(12, 0))
        ttk.Combobox(
            account_frame,
            textvariable=self.trade_mode_label,
            values=list(TRADE_MODE_OPTIONS.keys()),
            state="readonly",
        ).grid(row=row, column=1, sticky="ew", padx=(0, 16), pady=(12, 0))
        ttk.Label(account_frame, text="Position Mode").grid(row=row, column=2, sticky="w", pady=(12, 0))
        ttk.Combobox(
            account_frame,
            textvariable=self.position_mode_label,
            values=list(POSITION_MODE_OPTIONS.keys()),
            state="readonly",
        ).grid(row=row, column=3, sticky="ew", pady=(12, 0))

        row += 1
        fee_frame = ttk.LabelFrame(account_frame, text="Custom Fee (%)", padding=12)
        fee_frame.grid(row=row, column=0, columnspan=4, sticky="ew", pady=(12, 0))
        for fee_column in range(3):
            fee_frame.columnconfigure(fee_column, weight=1)
        ttk.Label(fee_frame, text="Type").grid(row=0, column=0, sticky="w")
        ttk.Label(fee_frame, text="Maker").grid(row=0, column=1, sticky="w")
        ttk.Label(fee_frame, text="Taker").grid(row=0, column=2, sticky="w")
        ttk.Label(fee_frame, text="Spot").grid(row=1, column=0, sticky="w", pady=(8, 0))
        ttk.Entry(fee_frame, textvariable=self.spot_maker_fee_rate).grid(row=1, column=1, sticky="ew", padx=(0, 12), pady=(8, 0))
        ttk.Entry(fee_frame, textvariable=self.spot_taker_fee_rate).grid(row=1, column=2, sticky="ew", pady=(8, 0))
        ttk.Label(fee_frame, text="Futures").grid(row=2, column=0, sticky="w", pady=(8, 0))
        ttk.Entry(fee_frame, textvariable=self.futures_maker_fee_rate).grid(row=2, column=1, sticky="ew", padx=(0, 12), pady=(8, 0))
        ttk.Entry(fee_frame, textvariable=self.futures_taker_fee_rate).grid(row=2, column=2, sticky="ew", pady=(8, 0))
        ttk.Label(fee_frame, text="Option").grid(row=3, column=0, sticky="w", pady=(8, 0))
        ttk.Entry(fee_frame, textvariable=self.option_maker_fee_rate).grid(row=3, column=1, sticky="ew", padx=(0, 12), pady=(8, 0))
        ttk.Entry(fee_frame, textvariable=self.option_taker_fee_rate).grid(row=3, column=2, sticky="ew", pady=(8, 0))


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
            text=f"凭证会加密保存到：{credentials_file_path().name}（支持多个 API 配置）",
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
        ttk.Label(mail_frame, text="当前 API 专属收件邮箱").grid(row=row, column=0, sticky="w", pady=(12, 0))
        ttk.Entry(mail_frame, textvariable=self.api_sender_email_override).grid(
            row=row, column=1, sticky="ew", padx=(0, 16), pady=(12, 0)
        )
        ttk.Label(mail_frame, text="留空则只发送到全局收件邮箱", justify="left").grid(
            row=row, column=2, columnspan=2, sticky="w", pady=(12, 0)
        )

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
        ttk.Button(
            footer,
            text="发送策略状态测试邮件",
            command=self.send_strategy_status_test_email,
        ).grid(row=0, column=1, padx=(0, 8))
        ttk.Button(footer, text="关闭", command=self._close_settings_window).grid(row=0, column=2)






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

    def open_email_schedule_manager_window(self) -> None:
        if self._email_schedule_manager_window is not None and self._email_schedule_manager_window.window.winfo_exists():
            self._email_schedule_manager_window.show()
            return

        self._email_schedule_manager_window = EmailScheduleManagerWindow(
            self.root,
            on_close=lambda: setattr(self, "_email_schedule_manager_window", None),
        )
        self._email_schedule_manager_window.show()

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

    def open_semi_auto_desk_window(self) -> None:
        if self._semi_auto_desk_window is not None and self._semi_auto_desk_window.window.winfo_exists():
            self._semi_auto_desk_window.show()
            return

        self._semi_auto_desk_window = SemiAutoDeskWindow(
            self.root,
            snapshot_provider=self._semi_auto_desk_snapshot_for_ui,
            strategy_library_opener=self.open_semi_auto_strategy_library,
            pool_creator=self.create_semi_auto_pool,
            task_adder=self.add_semi_auto_task,
            task_starter=self.start_semi_auto_task,
            task_canceller=self.cancel_semi_auto_task,
            replay_opener=self.open_semi_auto_pool_replay,
            ledger_provider=lambda: list(self._strategy_trade_ledger_records),
            summary_provider=lambda pool: build_semi_auto_pool_summary(
                pool,
                [task for task in self._semi_auto_desk_tasks if task.pool_id == pool.pool_id],
                self._strategy_trade_ledger_records,
            ),
            default_api_name=self._current_credential_profile(),
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

        self._open_option_roll_dialog(
            position=position,
            instrument=instrument,
            ticker=ticker,
            api_name=self._current_credential_profile(),
        )

    def _open_option_roll_dialog(
        self,
        *,
        position: OkxPosition,
        instrument: Instrument,
        ticker: OkxTicker,
        api_name: str,
    ) -> None:
        quote = _build_option_quote(instrument, ticker)

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
            logger=self._enqueue_log,
        )

    def _close_settings_window(self) -> None:
        self._save_credentials_now(silent=True)
        self._save_notification_settings_now(silent=True)
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

    def _credential_profile_environment_value(self, profile_name: str, *, fallback: str = "") -> str:
        snapshot = self._credential_profiles.get(profile_name, {})
        environment = str(snapshot.get("environment", "")).strip().lower()
        if environment in {"demo", "live"}:
            return environment
        fallback_value = str(fallback or "").strip().lower()
        return fallback_value if fallback_value in {"demo", "live"} else ""

    def _environment_label_for_profile(self, profile_name: str) -> str:
        environment = self._credential_profile_environment_value(profile_name)
        if environment == "live":
            return "实盘 live"
        if environment == "demo":
            return "模拟盘 demo"
        return self._normalized_environment_label(None)

    def _apply_profile_environment(self, profile_name: str) -> None:
        self._positions_effective_environment = None
        self.environment_label.set(self._environment_label_for_profile(profile_name))

    def _credential_field_text(self, field_name: str) -> str:
        variable = getattr(self, field_name, None)
        if hasattr(variable, "get"):
            return str(variable.get() or "").strip()
        return ""

    def _current_credentials_state(self) -> tuple[str, str, str, str, str, str, str, str, str, str, str]:
        return (
            self._editing_credential_profile(),
            self.api_key.get().strip(),
            self.secret_key.get().strip(),
            self.passphrase.get().strip(),
            self._environment_value_from_label(self.environment_label.get()),
            QuantApp._credential_field_text(self, "spot_maker_fee_rate"),
            QuantApp._credential_field_text(self, "spot_taker_fee_rate"),
            QuantApp._credential_field_text(self, "futures_maker_fee_rate"),
            QuantApp._credential_field_text(self, "futures_taker_fee_rate"),
            QuantApp._credential_field_text(self, "option_maker_fee_rate"),
            QuantApp._credential_field_text(self, "option_taker_fee_rate"),
        )

    def _set_credentials_fields(self, snapshot: dict[str, str]) -> None:
        was_enabled = self._credential_watch_enabled
        self._credential_watch_enabled = False
        self.api_key.set(snapshot["api_key"])
        self.secret_key.set(snapshot["secret_key"])
        self.passphrase.set(snapshot["passphrase"])
        self.spot_maker_fee_rate.set(str(snapshot.get("spot_maker_fee_rate", "") or "0.0600"))
        self.spot_taker_fee_rate.set(str(snapshot.get("spot_taker_fee_rate", "") or "0.0700"))
        self.futures_maker_fee_rate.set(str(snapshot.get("futures_maker_fee_rate", "") or "0.0150"))
        self.futures_taker_fee_rate.set(str(snapshot.get("futures_taker_fee_rate", "") or "0.0360"))
        self.option_maker_fee_rate.set(str(snapshot.get("option_maker_fee_rate", "") or "0.0250"))
        self.option_taker_fee_rate.set(str(snapshot.get("option_taker_fee_rate", "") or "0.0300"))
        self._credential_watch_enabled = was_enabled

    def _credential_profile_requires_switch_password(self, profile_name: str) -> bool:
        snapshot = self._credential_profiles.get(profile_name.strip(), {})
        return credential_profile_has_switch_password(snapshot)

    def _credential_profile_is_locked(self, profile_name: str) -> bool:
        return profile_name.strip() in self._locked_credential_profiles

    def _credential_profile_access_granted(self, profile_name: str) -> bool:
        return profile_name.strip() in getattr(self, "_credential_access_granted_profiles", set())

    def _grant_credential_profile_access(self, profile_name: str) -> None:
        target = profile_name.strip()
        if not target:
            return
        if not hasattr(self, "_credential_access_granted_profiles"):
            self._credential_access_granted_profiles = set()
        self._credential_access_granted_profiles.add(target)

    def _revoke_credential_profile_access(self, profile_name: str) -> None:
        target = profile_name.strip()
        if not target:
            return
        getattr(self, "_credential_access_granted_profiles", set()).discard(target)

    def _ensure_credential_profile_access(self, profile_name: str, *, prompt: bool = True) -> bool:
        target = profile_name.strip() or DEFAULT_CREDENTIAL_PROFILE_NAME
        if not target:
            return False
        if not self._credential_profile_requires_switch_password(target):
            return True
        if self._credential_profile_access_granted(target):
            return True
        current = self._current_credential_profile().strip()
        if (
            target == current
            and not self._credential_profile_is_locked(target)
            and self._loaded_credential_profile_name.strip() == target
        ):
            self._grant_credential_profile_access(target)
            return True
        if not prompt:
            return False
        return self._confirm_credential_profile_switch(target)

    def _update_current_api_profile_password_status(self, profile_name: str | None = None) -> None:
        target = (profile_name or self._current_credential_profile()).strip()
        if self._credential_profile_is_locked(target):
            status_text = "切换密码：已设置（未解锁）"
        elif self._credential_profile_requires_switch_password(target):
            status_text = "切换密码：已设置"
        else:
            status_text = "切换密码：未设置"
        self.credential_profile_password_status_text.set(status_text)

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
        self._locked_credential_profiles.discard(target)
        if not self._credential_profile_requires_switch_password(target):
            granted_profiles = getattr(self, "_credential_access_granted_profiles", None)
            if granted_profiles is None:
                granted_profiles = set()
                self._credential_access_granted_profiles = granted_profiles
            granted_profiles.add(target)
        self._loaded_credential_profile_name = target
        self.api_profile_name.set(target)
        self._set_credentials_fields(snapshot)
        self._sync_current_api_sender_email_override(target)
        self._update_current_api_profile_password_status(target)
        self._last_saved_credentials = (
                target,
                snapshot["api_key"],
                snapshot["secret_key"],
                snapshot["passphrase"],
                stored_environment,
                str(snapshot.get("spot_maker_fee_rate", "") or ""),
                str(snapshot.get("spot_taker_fee_rate", "") or ""),
                str(snapshot.get("futures_maker_fee_rate", "") or ""),
                str(snapshot.get("futures_taker_fee_rate", "") or ""),
                str(snapshot.get("option_maker_fee_rate", "") or ""),
                str(snapshot.get("option_taker_fee_rate", "") or ""),
            )
        self._apply_profile_environment(target)
        self._sync_credential_profile_combo()
        self._update_settings_summary()
        if log_change:
            self._enqueue_log(f"API切换 | 配置={target}")
        UiPositionsMixin._refresh_account_views_after_credential_profile_switch(self)

    def _apply_locked_credentials_profile(self, profile_name: str) -> None:
        target = profile_name.strip() or DEFAULT_CREDENTIAL_PROFILE_NAME
        snapshot = self._credential_profiles.get(target, _blank_credential_profile_snapshot())
        self._locked_credential_profiles.add(target)
        getattr(self, "_credential_access_granted_profiles", set()).discard(target)
        self._loaded_credential_profile_name = ""
        self.api_profile_name.set(target)
        self._set_credentials_fields(_blank_credential_profile_snapshot())
        self._sync_current_api_sender_email_override(target)
        self._last_saved_credentials = None
        self._apply_profile_environment(target)
        self._sync_credential_profile_combo()
        self._update_settings_summary()
        self._update_current_api_profile_password_status(target)

    def _normalized_api_sender_email_overrides(self) -> dict[str, str]:
        return {
            str(key).strip(): str(value).strip()
            for key, value in self._api_sender_email_overrides.items()
            if str(key).strip() and str(value).strip()
        }

    def _resolved_api_sender_email_override(self, profile_name: str | None = None) -> str:
        target = (profile_name or self._current_credential_profile()).strip()
        if not target:
            return ""
        return self._normalized_api_sender_email_overrides().get(target, "")

    def _sync_current_api_sender_email_override(self, profile_name: str | None = None) -> None:
        target = (profile_name or self._current_credential_profile()).strip()
        value = self._resolved_api_sender_email_override(target)
        was_enabled = self._api_sender_override_watch_enabled
        self._api_sender_override_watch_enabled = False
        self.api_sender_email_override.set(value)
        self._api_sender_override_watch_enabled = was_enabled

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
        if self._credential_profile_requires_switch_password(startup_profile):
            if self._confirm_credential_profile_switch(startup_profile):
                self._apply_credentials_profile(startup_profile)
            else:
                self._apply_locked_credentials_profile(startup_profile)
                self._enqueue_log(f"启动时未解锁 API 配置：{startup_profile}")
                return
        else:
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
        self._api_sender_email_overrides = {
            str(key).strip(): str(value).strip()
            for key, value in dict(snapshot.get("api_sender_email_overrides", {})).items()
            if str(key).strip() and str(value).strip()
        }
        self.recipient_emails.set(str(snapshot["recipient_emails"]))
        self.use_ssl.set(bool(snapshot["use_ssl"]))
        self.notify_trade_fills.set(bool(snapshot["notify_trade_fills"]))
        self.notify_signals.set(bool(snapshot["notify_signals"]))
        self.notify_errors.set(bool(snapshot["notify_errors"]))
        self._upgrade_launch_mode_setting = UpgradeLaunchManager.normalize_mode(snapshot.get("upgrade_launch_mode"))
        self._upgrade_custom_launch_path_setting = UpgradeLaunchManager.normalize_custom_launch_path(
            snapshot.get("upgrade_custom_launch_path", "")
        )
        self._running_session_display_columns = UiStrategySessionsMixin._normalize_running_session_display_columns(
            snapshot.get("running_session_display_columns", ())
        )
        self._sync_current_api_sender_email_override(self._current_credential_profile())
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
        self.spot_maker_fee_rate.trace_add("write", self._on_credentials_changed)
        self.spot_taker_fee_rate.trace_add("write", self._on_credentials_changed)
        self.futures_maker_fee_rate.trace_add("write", self._on_credentials_changed)
        self.futures_taker_fee_rate.trace_add("write", self._on_credentials_changed)
        self.option_maker_fee_rate.trace_add("write", self._on_credentials_changed)
        self.option_taker_fee_rate.trace_add("write", self._on_credentials_changed)
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
        self.api_sender_email_override.trace_add("write", self._on_api_sender_email_override_changed)
        self.position_history_range_start.trace_add("write", self._schedule_save_position_history_view_prefs)
        self.position_history_range_end.trace_add("write", self._schedule_save_position_history_view_prefs)
        self._settings_watch_enabled = True
        self._api_sender_override_watch_enabled = True

    def _on_credentials_changed(self, *_: str) -> None:
        if not self._credential_watch_enabled:
            return
        if self._credential_save_job is not None:
            self.root.after_cancel(self._credential_save_job)
        self._credential_save_job = self.root.after(600, self._save_credentials_now)

    def _on_notification_settings_changed(self, *_: str) -> None:
        if not self._settings_watch_enabled:
            return
        self._refresh_email_runtime_policy_cache()
        self._refresh_running_session_email_notifiers()
        self._refresh_global_email_toggle_text()
        self._refresh_running_session_tree()
        self._refresh_selected_session_details()
        self._on_settings_changed()
        if self._settings_save_job is not None:
            self.root.after_cancel(self._settings_save_job)
        self._settings_save_job = self.root.after(600, self._save_notification_settings_now)

    def _on_api_sender_email_override_changed(self, *_: str) -> None:
        if not self._api_sender_override_watch_enabled:
            return
        profile_name = self._current_credential_profile()
        override = self.api_sender_email_override.get().strip()
        if override:
            self._api_sender_email_overrides[profile_name] = override
        else:
            self._api_sender_email_overrides.pop(profile_name, None)
        self._on_notification_settings_changed()

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
        if len(current) == 5:
            current = (*current, "", "", "", "", "", "")
        if current == self._last_saved_credentials:
            return
        if self._credential_profile_is_locked(current[0]):
            return

        try:
            (
                profile_name,
                api_key,
                secret_key,
                passphrase,
                environment,
                spot_maker_fee_rate,
                spot_taker_fee_rate,
                futures_maker_fee_rate,
                futures_taker_fee_rate,
                option_maker_fee_rate,
                option_taker_fee_rate,
            ) = current
            existing_snapshot = self._credential_profiles.get(profile_name, {})
            self._credential_profiles[profile_name] = {
                "api_key": api_key,
                "secret_key": secret_key,
                "passphrase": passphrase,
                "environment": environment,
                "spot_maker_fee_rate": spot_maker_fee_rate,
                "spot_taker_fee_rate": spot_taker_fee_rate,
                "futures_maker_fee_rate": futures_maker_fee_rate,
                "futures_taker_fee_rate": futures_taker_fee_rate,
                "option_maker_fee_rate": option_maker_fee_rate,
                "option_taker_fee_rate": option_taker_fee_rate,
                "switch_password_hash": str(existing_snapshot.get("switch_password_hash", "") or ""),
                "switch_password_salt": str(existing_snapshot.get("switch_password_salt", "") or ""),
                "switch_password_iterations": str(existing_snapshot.get("switch_password_iterations", "") or ""),
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

    def _active_settings_parent(self) -> object:
        if self._settings_window is not None and _widget_exists(self._settings_window):
            return self._settings_window
        return self.root

    def _restore_loaded_credential_profile_selection(self) -> None:
        loaded = self._loaded_credential_profile_name.strip() or DEFAULT_CREDENTIAL_PROFILE_NAME
        if self.api_profile_name.get().strip() != loaded:
            self.api_profile_name.set(loaded)

    def _restore_profile_selection(self, profile_name: str) -> None:
        target = profile_name.strip() or DEFAULT_CREDENTIAL_PROFILE_NAME
        if self.api_profile_name.get().strip() != target:
            self.api_profile_name.set(target)

    def _confirm_credential_profile_switch(self, profile_name: str) -> bool:
        target = profile_name.strip()
        if not target or not self._credential_profile_requires_switch_password(target):
            return True
        parent = self._active_settings_parent()
        password = simpledialog.askstring(
            "输入 API 切换密码",
            f"API 配置 {target} 已设置切换密码，请输入后继续：",
            show="*",
            parent=parent,
        )
        if password is None:
            return False
        if verify_profile_switch_password(self._credential_profiles.get(target, {}), password):
            granted_profiles = getattr(self, "_credential_access_granted_profiles", None)
            if granted_profiles is None:
                granted_profiles = set()
                self._credential_access_granted_profiles = granted_profiles
            granted_profiles.add(target)
            return True
        messagebox.showerror("密码错误", f"API 配置 {target} 的切换密码不正确。", parent=parent)
        return False

    def _confirm_existing_api_profile_password(self, profile_name: str, *, action_label: str) -> bool:
        target = profile_name.strip()
        if not target or not self._credential_profile_requires_switch_password(target):
            return True
        parent = self._active_settings_parent()
        password = simpledialog.askstring(
            "验证旧密码",
            f"API 配置 {target} 已设置切换密码。\n请先输入旧密码，再{action_label}：",
            show="*",
            parent=parent,
        )
        if password is None:
            return False
        if verify_profile_switch_password(self._credential_profiles.get(target, {}), password):
            return True
        messagebox.showerror("密码错误", f"API 配置 {target} 的旧密码不正确。", parent=parent)
        return False

    def _unlock_current_api_profile(self) -> None:
        profile_name = self._current_credential_profile()
        if not profile_name:
            return
        if self._credential_profile_is_locked(profile_name):
            if not self._confirm_credential_profile_switch(profile_name):
                self._restore_profile_selection(profile_name)
                return
            self._apply_credentials_profile(profile_name, log_change=False)
            self._enqueue_log(f"已解锁 API 配置：{profile_name}")
            return
        if (
            profile_name == self._loaded_credential_profile_name
            and not self._credential_profile_requires_switch_password(profile_name)
        ):
            return
        self._apply_credentials_profile(profile_name, log_change=True)

    def _set_current_api_profile_switch_password(self) -> None:
        profile_name = self._editing_credential_profile()
        parent = self._active_settings_parent()
        if not self._confirm_existing_api_profile_password(profile_name, action_label="更新切换密码"):
            return
        password = simpledialog.askstring(
            "设置 API 切换密码",
            f"给 API 配置 {profile_name} 设置切换密码：",
            show="*",
            parent=parent,
        )
        if password is None:
            return
        if not str(password).strip():
            messagebox.showerror("设置失败", "API 切换密码不能为空。", parent=parent)
            return
        confirm = simpledialog.askstring(
            "确认 API 切换密码",
            f"请再次输入 API 配置 {profile_name} 的切换密码：",
            show="*",
            parent=parent,
        )
        if confirm is None:
            return
        if password != confirm:
            messagebox.showerror("设置失败", "两次输入的密码不一致。", parent=parent)
            return
        snapshot = dict(self._credential_profiles.get(profile_name, _blank_credential_profile_snapshot()))
        snapshot.update(build_profile_switch_password_snapshot(password))
        self._credential_profiles[profile_name] = snapshot
        getattr(self, "_credential_access_granted_profiles", set()).discard(profile_name)
        save_credentials_profiles_snapshot(
            selected_profile=self._current_credential_profile(),
            profiles=self._credential_profiles,
        )
        self._update_current_api_profile_password_status(profile_name)
        self._enqueue_log(f"已为 API 配置 {profile_name} 设置切换密码")

    def _clear_current_api_profile_switch_password(self) -> None:
        profile_name = self._editing_credential_profile()
        if not self._credential_profile_requires_switch_password(profile_name):
            return
        parent = self._active_settings_parent()
        if not self._confirm_existing_api_profile_password(profile_name, action_label="清除切换密码"):
            return
        if not messagebox.askyesno(
            "清除 API 切换密码",
            f"确认清除 API 配置 {profile_name} 的切换密码吗？",
            parent=parent,
        ):
            return
        snapshot = dict(self._credential_profiles.get(profile_name, _blank_credential_profile_snapshot()))
        snapshot.update(
            {
                "switch_password_hash": "",
                "switch_password_salt": "",
                "switch_password_iterations": "",
            }
        )
        self._locked_credential_profiles.discard(profile_name)
        getattr(self, "_credential_access_granted_profiles", set()).discard(profile_name)
        self._credential_profiles[profile_name] = snapshot
        save_credentials_profiles_snapshot(
            selected_profile=self._current_credential_profile(),
            profiles=self._credential_profiles,
        )
        self._update_current_api_profile_password_status(profile_name)
        self._enqueue_log(f"已清除 API 配置 {profile_name} 的切换密码")

    def _on_api_profile_selected(self, *_: object) -> None:
        selected = self.api_profile_name.get().strip()
        if not selected:
            return
        if selected == self._loaded_credential_profile_name:
            return
        if not self._confirm_credential_profile_switch(selected):
            self._restore_loaded_credential_profile_selection()
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
        self._save_notification_settings_now(silent=True)
        profiles = dict(self._credential_profiles)
        profile_payload = profiles.pop(current_name, _blank_credential_profile_snapshot())
        profiles[target_name] = profile_payload
        self._credential_profiles = profiles
        override = self._api_sender_email_overrides.pop(current_name, "").strip()
        if override:
            self._api_sender_email_overrides[target_name] = override
        granted_profiles = getattr(self, "_credential_access_granted_profiles", set())
        if current_name in granted_profiles:
            granted_profiles.discard(current_name)
            granted_profiles.add(target_name)
        save_credentials_profiles_snapshot(
            selected_profile=target_name,
            profiles=self._credential_profiles,
        )
        self._apply_credentials_profile(target_name, log_change=True)
        self._save_notification_settings_now(silent=True)
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
            if strategy_uses_mtf_filter(config.strategy_id):
                lines.append(
                    f"高周期过滤：{config.resolved_mtf_filter_bar()} EMA{config.mtf_filter_fast_ema_period}/"
                    f"EMA{config.mtf_filter_slow_ema_period}（只过滤新开仓）"
                )
        else:
            next_profile = sorted(profiles.keys())[0]

        self._credential_profiles = profiles
        self._api_sender_email_overrides.pop(profile_name, None)
        self._locked_credential_profiles.discard(profile_name)
        getattr(self, "_credential_access_granted_profiles", set()).discard(profile_name)
        save_credentials_profiles_snapshot(
            selected_profile=next_profile,
            profiles=self._credential_profiles,
        )
        self._apply_credentials_profile(next_profile, log_change=True)
        self._save_notification_settings_now(silent=True)
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
                api_sender_email_overrides=self._api_sender_email_overrides,
                use_ssl=self.use_ssl.get(),
                notify_trade_fills=self.notify_trade_fills.get(),
                notify_signals=self.notify_signals.get(),
                notify_errors=self.notify_errors.get(),
                upgrade_launch_mode=self._upgrade_launch_mode_setting,
                upgrade_custom_launch_path=self._upgrade_custom_launch_path_setting,
                running_session_display_columns=list(
                    UiStrategySessionsMixin._normalize_running_session_display_columns(
                        getattr(self, "_running_session_display_columns", ())
                    )
                ),
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
                ema_type_raw=self.ema_type.get(),
                ema_period_raw=self.ema_period.get(),
                trend_ema_type_raw=self.trend_ema_type.get(),
                trend_ema_period_raw=self.trend_ema_period.get(),
                big_ema_period_raw=self.big_ema_period.get(),
                entry_reference_ema_type_raw=self.entry_reference_ema_type.get(),
                entry_reference_ema_period_raw=self.entry_reference_ema_period.get(),
                mtf_filter_bar_raw=self.mtf_filter_bar.get(),
                mtf_filter_fast_ema_period_raw=self.mtf_filter_fast_ema_period.get(),
                mtf_filter_slow_ema_period_raw=self.mtf_filter_slow_ema_period.get(),
                daily_filter_enabled=self.daily_filter_enabled.get(),
                daily_filter_boundary_label=self.daily_filter_boundary_label.get(),
                daily_filter_mode_label=self.daily_filter_mode_label.get(),
                daily_filter_scope_label=self.daily_filter_scope_label.get(),
                daily_filter_ma_type_raw=self.daily_filter_ma_type.get(),
                daily_filter_period_raw=self.daily_filter_period.get(),
            )
        )

    def _build_legacy_dynamic_protection_rules_from_current_inputs(self) -> tuple[DynamicProtectionRule, ...]:
        try:
            break_even_trigger_r = max(int((self.dynamic_break_even_trigger_r.get() or "2").strip()), 1)
        except ValueError:
            break_even_trigger_r = 2
        try:
            trailing_start_r = max(int((self.ema55_slope_lock_profit_trigger_r.get() or "5").strip()), 2)
        except ValueError:
            trailing_start_r = 5
        try:
            first_lock_r = max(int((self.dynamic_first_lock_r.get() or "0").strip()), 0)
        except ValueError:
            first_lock_r = 0
        try:
            trailing_step_r = max(int((self.dynamic_trailing_step_r.get() or "1").strip()), 1)
        except ValueError:
            trailing_step_r = 1
        return build_legacy_dynamic_protection_rules(
            break_even_enabled=bool(self.dynamic_two_r_break_even.get()),
            break_even_trigger_r=break_even_trigger_r,
            trailing_start_r=trailing_start_r,
            first_lock_r=first_lock_r,
            trailing_step_r=trailing_step_r,
        )

    def _current_dynamic_protection_rules(self) -> tuple[DynamicProtectionRule, ...]:
        raw = self.dynamic_protection_rules_json.get().strip()
        if raw:
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                return ()
            return merge_dynamic_protection_rules(
                self._build_legacy_dynamic_protection_rules_from_current_inputs(),
                payload,
            )
        return self._build_legacy_dynamic_protection_rules_from_current_inputs()

    def _sync_dynamic_protection_rules_json_from_editor(self) -> None:
        rules_payload: list[dict[str, object]] = []
        for row in self._dynamic_protection_rule_rows:
            action = "break_even" if row.action.get() == "保本" else "lock_profit"
            trail_mode = "step" if row.trail_mode.get() == "阶梯" else "none"
            trigger_raw = row.trigger_r.get().strip()
            if not trigger_raw:
                continue
            try:
                trigger_r = max(int(trigger_raw), 1)
            except ValueError:
                continue
            item: dict[str, object] = {
                "trigger_r": trigger_r,
                "action": action,
                "lock_r": None if action == "break_even" else max(int(row.lock_r.get().strip() or "0"), 0),
                "trail_mode": "none" if action == "break_even" else trail_mode,
                "trail_every_r": None if action == "break_even" or trail_mode == "none" else max(int(row.trail_every_r.get().strip() or "1"), 1),
                "trail_add_r": None if action == "break_even" or trail_mode == "none" else max(int(row.trail_add_r.get().strip() or "1"), 1),
            }
            rules_payload.append(item)
        normalized = dynamic_protection_rules_to_payload(rules_payload)
        serialized = json.dumps(normalized, ensure_ascii=False)
        if self.dynamic_protection_rules_json.get() != serialized:
            self.dynamic_protection_rules_json.set(serialized)

    def _update_dynamic_protection_rule_row_state(self, row: _DynamicProtectionRuleEditorRow) -> None:
        action_is_break_even = row.action.get() == "保本"
        trail_enabled = (not action_is_break_even) and row.trail_mode.get() == "阶梯"
        row.lock_entry.configure(state="normal" if not action_is_break_even else "disabled")
        row.trail_mode_combo.configure(state="readonly" if not action_is_break_even else "disabled")
        row.trail_every_entry.configure(state="normal" if trail_enabled else "disabled")
        row.trail_add_entry.configure(state="normal" if trail_enabled else "disabled")

    def _remove_dynamic_protection_rule_row(self, row: _DynamicProtectionRuleEditorRow) -> None:
        if row not in self._dynamic_protection_rule_rows:
            return
        self._dynamic_protection_rule_rows.remove(row)
        row.frame.destroy()
        self._sync_dynamic_protection_rules_json_from_editor()

    def _append_dynamic_protection_rule_row(self, rule: DynamicProtectionRule | None = None) -> None:
        if self._dynamic_protection_rules_frame is None:
            return
        normalized = rule.normalized() if rule is not None else None
        row_frame = ttk.Frame(self._dynamic_protection_rules_frame)
        row_frame.grid(column=0, row=len(self._dynamic_protection_rule_rows) + 1, sticky="ew", pady=(4, 0))
        for column, width in ((0, 10), (1, 12), (2, 10), (3, 10), (4, 10), (5, 10), (6, 10)):
            row_frame.columnconfigure(column, minsize=width)
        trigger_r = StringVar(value=str(normalized.trigger_r if normalized else len(self._dynamic_protection_rule_rows) + 2))
        action = StringVar(value="保本" if normalized and normalized.action == "break_even" else "锁盈")
        lock_r = StringVar(value="" if normalized is None or normalized.action == "break_even" else str(normalized.lock_r or 0))
        trail_mode = StringVar(value="阶梯" if normalized and normalized.trailing_enabled() else "无")
        trail_every_r = StringVar(value="" if normalized is None or not normalized.trailing_enabled() else str(normalized.trail_every_r or 1))
        trail_add_r = StringVar(value="" if normalized is None or not normalized.trailing_enabled() else str(normalized.trail_add_r or 1))
        trigger_entry = ttk.Entry(row_frame, textvariable=trigger_r, width=8)
        trigger_entry.grid(row=0, column=0, sticky="ew", padx=(0, 6))
        action_combo = ttk.Combobox(row_frame, textvariable=action, values=("保本", "锁盈"), state="readonly", width=8)
        action_combo.grid(row=0, column=1, sticky="ew", padx=(0, 6))
        lock_entry = ttk.Entry(row_frame, textvariable=lock_r, width=8)
        lock_entry.grid(row=0, column=2, sticky="ew", padx=(0, 6))
        trail_mode_combo = ttk.Combobox(row_frame, textvariable=trail_mode, values=("无", "阶梯"), state="readonly", width=8)
        trail_mode_combo.grid(row=0, column=3, sticky="ew", padx=(0, 6))
        trail_every_entry = ttk.Entry(row_frame, textvariable=trail_every_r, width=8)
        trail_every_entry.grid(row=0, column=4, sticky="ew", padx=(0, 6))
        trail_add_entry = ttk.Entry(row_frame, textvariable=trail_add_r, width=8)
        trail_add_entry.grid(row=0, column=5, sticky="ew", padx=(0, 6))
        delete_button = ttk.Button(row_frame, text="删除")
        delete_button.grid(row=0, column=6, sticky="ew")
        editor_row = _DynamicProtectionRuleEditorRow(
            frame=row_frame,
            trigger_r=trigger_r,
            action=action,
            lock_r=lock_r,
            trail_mode=trail_mode,
            trail_every_r=trail_every_r,
            trail_add_r=trail_add_r,
            trigger_entry=trigger_entry,
            action_combo=action_combo,
            lock_entry=lock_entry,
            trail_mode_combo=trail_mode_combo,
            trail_every_entry=trail_every_entry,
            trail_add_entry=trail_add_entry,
            delete_button=delete_button,
        )
        delete_button.configure(command=lambda current=editor_row: self._remove_dynamic_protection_rule_row(current))
        for variable in (trigger_r, action, lock_r, trail_mode, trail_every_r, trail_add_r):
            variable.trace_add("write", lambda *_args, current=editor_row: (self._update_dynamic_protection_rule_row_state(current), self._sync_dynamic_protection_rules_json_from_editor()))
        self._dynamic_protection_rule_rows.append(editor_row)
        self._update_dynamic_protection_rule_row_state(editor_row)

    def _rebuild_dynamic_protection_rule_editor(self) -> None:
        rules = self._current_dynamic_protection_rules()
        for row in list(self._dynamic_protection_rule_rows):
            row.frame.destroy()
        self._dynamic_protection_rule_rows.clear()
        if not rules:
            rules = self._build_legacy_dynamic_protection_rules_from_current_inputs()
        for rule in rules:
            self._append_dynamic_protection_rule_row(rule)
        if not self._dynamic_protection_rule_rows:
            self._append_dynamic_protection_rule_row()
        self._sync_dynamic_protection_rules_json_from_editor()

    def _update_dynamic_protection_hint_legacy(self, *_: str) -> None:
        strategy_id = self._strategy_name_to_id.get(self.strategy_name.get(), "")
        rules = self._current_dynamic_protection_rules()
        if self.take_profit_mode_label.get() == "鍔ㄦ€佹鐩?" and rules:
            preview_lines = describe_dynamic_protection_rules(
                rules,
                fee_offset_enabled=self.dynamic_fee_offset_enabled.get(),
            )
            time_stop_bars = self.time_stop_break_even_bars.get().strip() or "0"
            time_stop_text = (
                f"时间保本：开启，持仓满 {time_stop_bars} 根K线且已达到净保本后，再把止损抬到保本位。"
                if self.time_stop_break_even_enabled.get()
                else f"时间保本：关闭（当前设定 {time_stop_bars} 根，仅保存参数，不会启用）。"
            )
            self.dynamic_protection_hint_text.set("动态保护： " + " / ".join(preview_lines + (time_stop_text,)))
            return
        self.dynamic_protection_hint_text.set(
            _build_dynamic_protection_hint_text(
                take_profit_mode_label=self.take_profit_mode_label.get(),
                dynamic_two_r_break_even_enabled=self.dynamic_two_r_break_even.get(),
                break_even_trigger_r_raw=self.dynamic_break_even_trigger_r.get(),
                trailing_start_r_raw=self.ema55_slope_lock_profit_trigger_r.get(),
                first_lock_r_raw=self.dynamic_first_lock_r.get(),
                trailing_step_r_raw=self.dynamic_trailing_step_r.get(),
                break_even_trigger_r_configurable=bool(
                    strategy_id and strategy_uses_parameter(strategy_id, "dynamic_break_even_trigger_r")
                ),
                dynamic_fee_offset_enabled=self.dynamic_fee_offset_enabled.get(),
                time_stop_break_even_enabled=self.time_stop_break_even_enabled.get(),
                time_stop_break_even_bars_raw=self.time_stop_break_even_bars.get(),
                trend_ema_close_exit_after_trigger_r_enabled=self.trend_ema_close_exit_after_trigger_r_enabled.get(),
                trend_ema_close_exit_after_trigger_r_raw=self.trend_ema_close_exit_after_trigger_r.get(),
            )
        )

    def _update_dynamic_protection_hint(self, *_: str) -> None:
        strategy_id = self._strategy_name_to_id.get(self.strategy_name.get(), "")
        rules = self._current_dynamic_protection_rules()
        if self.take_profit_mode_label.get() == "动态止盈" and rules:
            preview_lines = describe_dynamic_protection_rules(
                rules,
                fee_offset_enabled=self.dynamic_fee_offset_enabled.get(),
            )
            overlap_warnings = describe_dynamic_protection_rule_overlap_warnings(rules)
            time_stop_bars = self.time_stop_break_even_bars.get().strip() or "0"
            time_stop_text = (
                f"时间保本：开启，持仓满 {time_stop_bars} 根K线且已达到净保本后，再把止损抬到保本位。"
                if self.time_stop_break_even_enabled.get()
                else f"时间保本：关闭（当前设定 {time_stop_bars} 根，仅保存参数，不会启用）。"
            )
            hint_parts = preview_lines + (time_stop_text,)
            if self.trend_ema_close_exit_after_trigger_r_enabled.get():
                trend_exit_r = self.trend_ema_close_exit_after_trigger_r.get().strip() or "5"
                hint_parts = hint_parts + (
                    f"趋势EMA离场：达到 {trend_exit_r}R 后，若收盘跌破趋势EMA则平仓。",
                )
            if overlap_warnings:
                hint_parts = hint_parts + ("规则提示：" + "；".join(overlap_warnings),)
            self.dynamic_protection_hint_text.set("动态保护： " + " / ".join(hint_parts))
            return
        self.dynamic_protection_hint_text.set(
            _build_dynamic_protection_hint_text(
                take_profit_mode_label=self.take_profit_mode_label.get(),
                dynamic_two_r_break_even_enabled=self.dynamic_two_r_break_even.get(),
                break_even_trigger_r_raw=self.dynamic_break_even_trigger_r.get(),
                trailing_start_r_raw=self.ema55_slope_lock_profit_trigger_r.get(),
                first_lock_r_raw=self.dynamic_first_lock_r.get(),
                trailing_step_r_raw=self.dynamic_trailing_step_r.get(),
                break_even_trigger_r_configurable=bool(
                    strategy_id and strategy_uses_parameter(strategy_id, "dynamic_break_even_trigger_r")
                ),
                dynamic_fee_offset_enabled=self.dynamic_fee_offset_enabled.get(),
                time_stop_break_even_enabled=self.time_stop_break_even_enabled.get(),
                time_stop_break_even_bars_raw=self.time_stop_break_even_bars.get(),
                trend_ema_close_exit_after_trigger_r_enabled=self.trend_ema_close_exit_after_trigger_r_enabled.get(),
                trend_ema_close_exit_after_trigger_r_raw=self.trend_ema_close_exit_after_trigger_r.get(),
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
        definition = self._selected_strategy_definition()
        signal_mode = SIGNAL_LABEL_TO_VALUE.get(self.signal_mode_label.get(), definition.default_signal_mode)
        if not trade_symbol:
            self.minimum_order_risk_hint_text.set("回测参考：请先选择标的。")
            return
        self.minimum_order_risk_hint_text.set(
            _build_minimum_order_risk_hint_text(
                inst_id=trade_symbol,
                instrument=None,
                risk_amount_raw=self.risk_amount.get(),
                strategy_id=definition.strategy_id,
                signal_mode=signal_mode,
            )
        )

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
            trend_ema_slope_filter_enabled = (
                bool(self.trend_ema_slope_filter_enabled.get())
                if strategy_uses_parameter(definition.strategy_id, "trend_ema_slope_filter_enabled")
                else True
            )
            trend_ema_slope_filter_min_ratio = Decimal(str(self.trend_ema_slope_filter_min_ratio.get()).strip() or "0")
            if trend_ema_slope_filter_min_ratio > 0:
                raise ValueError("趋势线斜率阈值必须小于或等于 0")
            atr_percentile_filter_max = Decimal(str(self.atr_percentile_filter_max.get()).strip() or "0")
            body_retest_breakdown_atr_multiplier = Decimal(
                str(self.body_retest_breakdown_atr_multiplier.get()).strip() or "0.2"
            )
            body_retest_retest_atr_multiplier = Decimal(
                str(self.body_retest_retest_atr_multiplier.get()).strip() or "0.3"
            )
            body_retest_stop_buffer_atr_multiplier = Decimal(
                str(self.body_retest_stop_buffer_atr_multiplier.get()).strip() or "0.3"
            )
            body_retest_body_atr_limit = Decimal(str(self.body_retest_body_atr_limit.get()).strip() or "1.0")
            body_retest_watch_bars = self._parse_nonnegative_int(self.body_retest_watch_bars.get(), "Watch bars")
            ema55_slope_exit_enabled = (
                bool(self.ema55_slope_exit_enabled.get())
                if strategy_uses_parameter(definition.strategy_id, "ema55_slope_exit_enabled")
                else True
            )
            ema55_slope_lock_profit_trigger_r = (
                self._parse_nonnegative_int(self.ema55_slope_lock_profit_trigger_r.get(), "移动止盈触发R")
                if strategy_uses_parameter(definition.strategy_id, "ema55_slope_lock_profit_trigger_r")
                else 2
            )
            dynamic_break_even_trigger_r = (
                self._parse_nonnegative_int(self.dynamic_break_even_trigger_r.get(), "保本触发R")
                if strategy_uses_parameter(definition.strategy_id, "dynamic_break_even_trigger_r")
                else 2
            )
            dynamic_trailing_step_r = (
                self._parse_nonnegative_int(self.dynamic_trailing_step_r.get(), "移动步长R")
                if strategy_uses_parameter(definition.strategy_id, "dynamic_trailing_step_r")
                else 1
            )
            dynamic_first_lock_r = (
                self._parse_nonnegative_int(self.dynamic_first_lock_r.get(), "首档锁盈R")
                if strategy_uses_parameter(definition.strategy_id, "dynamic_first_lock_r")
                else 0
            )
            trend_ema_close_exit_after_trigger_r_enabled = (
                bool(self.trend_ema_close_exit_after_trigger_r_enabled.get())
                if strategy_uses_parameter(definition.strategy_id, "trend_ema_close_exit_after_trigger_r_enabled")
                else False
            )
            trend_ema_close_exit_after_trigger_r = (
                self._parse_positive_int(self.trend_ema_close_exit_after_trigger_r.get(), "趋势EMA平仓触发R")
                if strategy_uses_parameter(definition.strategy_id, "trend_ema_close_exit_after_trigger_r")
                else 5
            )
            reentry_confirmation_enabled = (
                bool(self.reentry_confirmation_enabled.get())
                if strategy_uses_parameter(definition.strategy_id, "reentry_confirmation_enabled")
                else False
            )
            reentry_confirmation_min_sequence = (
                self._parse_positive_int(self.reentry_confirmation_min_sequence.get(), "再开仓确认起始次数")
                if reentry_confirmation_enabled
                else 0
            )
            reentry_confirmation_ma_type = (
                self.reentry_confirmation_ma_type.get().strip().lower()
                if strategy_uses_parameter(definition.strategy_id, "reentry_confirmation_ma_type")
                else "ema"
            )
            reentry_confirmation_ma_period = (
                self._parse_positive_int(self.reentry_confirmation_ma_period.get(), "再开仓确认均线周期")
                if strategy_uses_parameter(definition.strategy_id, "reentry_confirmation_ma_period")
                else 21
            )
            if ema55_slope_lock_profit_trigger_r < 2:
                raise ValueError("移动止盈触发R 不能小于 2")
            if dynamic_break_even_trigger_r < 1:
                raise ValueError("保本触发R 不能小于 1")
            if dynamic_trailing_step_r < 1:
                raise ValueError("移动步长R 不能小于 1")
            if trend_ema_close_exit_after_trigger_r < 1:
                raise ValueError("趋势EMA平仓触发R 不能小于 1")
            dynamic_protection_rules = (
                self._current_dynamic_protection_rules()
                if TAKE_PROFIT_MODE_OPTIONS.get(self.take_profit_mode_label.get(), "dynamic") == "dynamic"
                else ()
            )
            if TAKE_PROFIT_MODE_OPTIONS.get(self.take_profit_mode_label.get(), "dynamic") == "dynamic" and not dynamic_protection_rules:
                raise ValueError("动态止盈至少需要一条动态保护规则")
            config = StrategyConfig(
                inst_id=signal_symbol or trade_symbol,
                bar=self.bar.get().strip(),
                ema_type=self.ema_type.get().strip().lower(),
                ema_period=self._parse_nonnegative_int(self.ema_period.get(), "快线均线周期"),
                trend_ema_type=self.trend_ema_type.get().strip().lower(),
                trend_ema_period=self._parse_nonnegative_int(self.trend_ema_period.get(), "趋势均线周期"),
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
                ema55_slope_exit_enabled=ema55_slope_exit_enabled,
                dynamic_two_r_break_even=bool(self.dynamic_two_r_break_even.get()),
                dynamic_break_even_trigger_r=dynamic_break_even_trigger_r,
                ema55_slope_lock_profit_trigger_r=ema55_slope_lock_profit_trigger_r,
                dynamic_first_lock_r=dynamic_first_lock_r,
                dynamic_trailing_step_r=dynamic_trailing_step_r,
                dynamic_protection_rules=dynamic_protection_rules,
                dynamic_fee_offset_enabled=bool(self.dynamic_fee_offset_enabled.get()),
                trend_ema_slope_filter_enabled=trend_ema_slope_filter_enabled,
                trend_ema_slope_filter_min_ratio=trend_ema_slope_filter_min_ratio,
                atr_percentile_filter_max=atr_percentile_filter_max,
                body_retest_breakdown_atr_multiplier=body_retest_breakdown_atr_multiplier,
                body_retest_retest_atr_multiplier=body_retest_retest_atr_multiplier,
                body_retest_stop_buffer_atr_multiplier=body_retest_stop_buffer_atr_multiplier,
                body_retest_body_atr_limit=body_retest_body_atr_limit,
                body_retest_watch_bars=body_retest_watch_bars,
                time_stop_break_even_enabled=bool(self.time_stop_break_even_enabled.get()),
                time_stop_break_even_bars=(
                    self._parse_nonnegative_int(self.time_stop_break_even_bars.get(), "鏃堕棿淇濇湰K绾挎暟")
                    if self.time_stop_break_even_enabled.get()
                    else 0
                ),
                trend_ema_close_exit_after_trigger_r_enabled=trend_ema_close_exit_after_trigger_r_enabled,
                trend_ema_close_exit_after_trigger_r=trend_ema_close_exit_after_trigger_r,
                max_entries_per_trend=max(self._parse_nonnegative_int(self.max_entries_per_trend.get(), "每波最多开仓次数"), 0),
                entry_reference_ema_type=self.entry_reference_ema_type.get().strip().lower(),
                entry_reference_ema_period=max(
                    self._parse_nonnegative_int(
                        self.entry_reference_ema_period.get(),
                        _entry_reference_ema_caption(definition.strategy_id),
                    ),
                    0,
                ),
                reentry_confirmation_enabled=reentry_confirmation_enabled,
                reentry_confirmation_min_sequence=reentry_confirmation_min_sequence,
                reentry_confirmation_ma_type=reentry_confirmation_ma_type,
                reentry_confirmation_ma_period=reentry_confirmation_ma_period,
            )
        except Exception as exc:
            self.minimum_order_risk_hint_text.set(
                _build_minimum_order_risk_hint_text(
                    inst_id=trade_symbol,
                    instrument=instrument,
                    risk_amount_raw=self.risk_amount.get(),
                    strategy_id=definition.strategy_id,
                    signal_mode=SIGNAL_LABEL_TO_VALUE.get(self.signal_mode_label.get(), definition.default_signal_mode),
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
                strategy_id=config.strategy_id,
                signal_mode=config.signal_mode,
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
        prompt = threading.current_thread() is threading.main_thread()
        ensure_access = getattr(self, "_ensure_credential_profile_access", None)
        if callable(ensure_access):
            if not ensure_access(self._current_credential_profile(), prompt=prompt):
                return None
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
            tuple(sorted(self._normalized_api_sender_email_overrides().items())),
            self.recipient_emails.get().strip(),
            self.use_ssl.get(),
            self.notify_trade_fills.get(),
            self.notify_signals.get(),
            self.notify_errors.get(),
            self._upgrade_launch_mode_setting,
            self._upgrade_custom_launch_path_setting,
            tuple(
                UiStrategySessionsMixin._normalize_running_session_display_columns(
                    getattr(self, "_running_session_display_columns", ())
                )
            ),
        )

    def _enqueue_log(self, message: str) -> None:
        self.log_queue.put(append_log_line(message))

    def _drain_log_queue(self) -> None:
        while not self.log_queue.empty():
            line = self.log_queue.get_nowait()
            self.log_text.insert(END, line + "\n")
            self.log_text.see(END)
        self._drain_pending_runtime_session_updates()
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
                session.status = "已停止"
                session.runtime_status = "已停止"
                if session.stopped_at is None:
                    session.stopped_at = datetime.now()
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
            self._update_session_runtime_heartbeat_state(session)
            self._upsert_session_row(session)
            self._sync_strategy_history_from_session(session)

        self.status_text.set(f"运行中策略：{running_count}")
        self._refresh_trader_desk_runtime()
        self._refresh_running_session_summary()
        self._update_settings_summary()
        self._refresh_selected_session_details()
        self._refresh_strategy_book_window()
        self.root.after(500, self._refresh_status)

































































































        # 后台补足 K 线根数时不再改写状态栏，避免覆盖「已刷新」摘要。


















    def _on_close(self) -> None:
        if self._close_confirmation_required:
            confirmed = messagebox.askyesno(
                "确认关闭",
                "是否要关闭主界面？",
                parent=self.root,
            )
            if not confirmed:
                return
        self._close_confirmation_required = True
        self._stop_strategy_status_email_scheduler()
        upgrade_worker_command = self._upgrade_worker_command_on_close
        self._upgrade_worker_command_on_close = None
        restart_command = self._restart_command_on_close
        self._restart_command_on_close = None
        self._save_strategy_parameter_draft()
        self._save_credentials_now(silent=True)
        self._save_notification_settings_now(silent=True)
        closed_at = datetime.now()
        for session in self.sessions.values():
            if session.status in {"运行中", "停止中"} or session.engine.is_running:
                if self._session_can_auto_migrate_on_close(session):
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
        self.market_data_hub.stop()
        self._protection_manager.stop_all()
        self._close_strategy_history_window()
        self._close_strategy_book_window()
        self._close_all_strategy_live_chart_windows()
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
        if (
            self._email_schedule_manager_window is not None
            and self._email_schedule_manager_window.window.winfo_exists()
        ):
            self._email_schedule_manager_window.destroy()
        if self._signal_monitor_window is not None and self._signal_monitor_window.window.winfo_exists():
            self._signal_monitor_window.destroy()
        if self._trader_desk_window is not None and self._trader_desk_window.window.winfo_exists():
            self._trader_desk_window.destroy()
        if (
            self._deribit_volatility_monitor_window is not None
            and self._deribit_volatility_monitor_window.window.winfo_exists()
        ):
            self._deribit_volatility_monitor_window.destroy()
        if self._deribit_volatility_window is not None and self._deribit_volatility_window.window.winfo_exists():
            self._deribit_volatility_window.window.destroy()
        if self._option_roll_window is not None and self._option_roll_window.window.winfo_exists():
            self._option_roll_window.window.destroy()
        if self._protection_replay_window is not None and self._protection_replay_window.window.winfo_exists():
            self._protection_replay_window.window.destroy()
        self._close_positions_zoom_window()
        self._close_position_protection_window()
        post_close_command = upgrade_worker_command or restart_command
        post_close_error_title = "一键升级失败" if upgrade_worker_command is not None else "程序升级失败"
        post_close_error_message = (
            "旧程序已经关闭运行线程，但自动升级助手启动失败了。\n\n请手动重新打开程序。"
            if upgrade_worker_command is not None
            else "旧程序已经关闭运行线程，但自动拉起新程序失败了。\n\n请手动重新打开程序。"
        )
        if post_close_command is not None:
            try:
                creationflags = 0
                if os.name == "nt":
                    creationflags |= int(getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0))
                    creationflags |= int(getattr(subprocess, "DETACHED_PROCESS", 0))
                popen_kwargs: dict[str, object] = {"cwd": _app_restart_workdir()}
                if creationflags:
                    popen_kwargs["creationflags"] = creationflags
                subprocess.Popen(post_close_command, **popen_kwargs)
            except Exception as exc:
                messagebox.showerror(
                    post_close_error_title,
                    post_close_error_message + "\n\n"
                    f"请手动重新打开程序。\n\n错误信息：{exc}",
                    parent=self.root,
                )
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
    strategy_id: str = "",
    signal_mode: str = "both",
    minimum_risk_amount: Decimal | None = None,
    note: str = "",
    pending: bool = False,
) -> str:
    normalized_inst_id = inst_id.strip().upper()
    if not normalized_inst_id:
        return "回测参考：请先选择标的。"
    recommendation = recommended_minimum_risk_amount_for_strategy(strategy_id, normalized_inst_id, signal_mode)
    if recommendation is not None:
        return f"回测参考：{normalized_inst_id} {format_risk_recommendation(recommendation)}。"
    return f"回测参考：{normalized_inst_id} 暂无推荐值。"


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

    profile = get_strategy_runtime_profile(config.strategy_id)
    if profile.family == "ema5_ema8":
        lookback = recommended_indicator_lookback(config.ema_period, config.trend_ema_period)
        strategy = EmaCrossEmaStopStrategy()
    elif profile.family == "ema55_slope_short":
        lookback = recommended_indicator_lookback(
            config.ema_period,
            config.trend_ema_period,
            config.atr_period,
            DEFAULT_DEBUG_ATR_PERIOD,
        ) + 1
        strategy = None
    elif profile.uses_dynamic_orders:
        lookback = recommended_indicator_lookback(
            config.ema_period,
            config.trend_ema_period,
            config.atr_period,
            config.resolved_entry_reference_ema_period(),
        )
        strategy = EmaDynamicMultiTimeframeStrategy() if profile.uses_mtf_filter else EmaDynamicOrderStrategy()
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
    if profile.uses_mtf_filter:
        filter_lookback = recommended_indicator_lookback(
            config.mtf_filter_fast_ema_period,
            config.mtf_filter_slow_ema_period,
        )
        filter_candles = client.get_candles(
            config.resolved_mtf_filter_inst_id(),
            config.resolved_mtf_filter_bar(),
            limit=filter_lookback,
        )
        decision = strategy.evaluate(
            confirmed,
            [candle for candle in filter_candles if candle.confirmed],
            config,
            price_increment=trade_instrument.tick_size,
        )
    elif profile.family == "ema55_slope_short":
        decision = evaluate_ema55_slope_short_signal(
            confirmed,
            config,
            price_increment=trade_instrument.tick_size,
        )
    else:
        decision = strategy.evaluate(confirmed, config, price_increment=trade_instrument.tick_size)
    if decision.signal is None or decision.entry_reference is None or decision.candle_ts is None:
        return None, "当前还没有有效信号，等出现可挂单的一波后再给出估算。"

    if profile.family == "ema5_ema8":
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
                use_signal_extrema=strategy_uses_signal_extrema(config.strategy_id),
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
    ema_type_raw: str,
    ema_period_raw: str,
    trend_ema_type_raw: str,
    trend_ema_period_raw: str,
    big_ema_period_raw: str,
    entry_reference_ema_type_raw: str,
    entry_reference_ema_period_raw: str,
    mtf_filter_bar_raw: str = "",
    mtf_filter_fast_ema_period_raw: str = "",
    mtf_filter_slow_ema_period_raw: str = "",
    daily_filter_enabled: bool = False,
    daily_filter_boundary_label: str = "",
    daily_filter_mode_label: str = "",
    daily_filter_scope_label: str = "",
    daily_filter_ma_type_raw: str = "",
    daily_filter_period_raw: str = "",
) -> str:
    profile = get_strategy_runtime_profile(strategy_id)
    ema_type = (ema_type_raw or "EMA").strip().upper()
    ema_period = ema_period_raw.strip() or "?"
    trend_ema_type = (trend_ema_type_raw or "EMA").strip().upper()
    trend_ema_period = trend_ema_period_raw.strip() or "?"
    big_ema_period = big_ema_period_raw.strip() or "?"
    entry_reference_ema_type = (entry_reference_ema_type_raw or ema_type).strip().upper()
    entry_reference_ema_period = entry_reference_ema_period_raw.strip() or "0"
    fast_label = f"{ema_type}{ema_period}"
    trend_label = f"{trend_ema_type}{trend_ema_period}"
    reference_label = (
        f"{entry_reference_ema_type}{entry_reference_ema_period}"
        if entry_reference_ema_period not in {"", "0"}
        else f"跟随快线({fast_label})"
    )
    if profile.family == "ema55_slope_short":
        parts = [
            f"信号均线：{fast_label}，负责斜率开仓与斜率转正平仓。",
            f"趋势均线：{trend_label}，当前作为同组趋势参数展示，不负责这条斜率平仓信号。",
        ]
    else:
        parts = [
            f"快线均线：{fast_label}，负责捕捉最近节奏。",
            f"趋势均线：{trend_label}，用来判断当前方向是否仍然有效。",
        ]
    if profile.family == "cross_breakdown_short":
        ref = reference_label if entry_reference_ema_period not in {"", "0"} else fast_label
        parts.append(f"突破参考线：{ref}，收盘向下跌破该线触发做空，且需 {fast_label}<{trend_label}。")
    elif profile.family == "cross_breakout_long":
        ref = reference_label if entry_reference_ema_period not in {"", "0"} else fast_label
        parts.append(f"突破参考线：{ref}，收盘向上突破该线触发做多，且需 {fast_label}>{trend_label}。")
    elif profile.family == "ema5_ema8":
        parts.append(f"大周期均线：EMA{big_ema_period}，用于 4H 大趋势过滤。")
    if profile.uses_dynamic_orders:
        parts.append(f"挂单参考线：{reference_label}，价格会围绕这条线动态重挂。")
    if profile.uses_mtf_filter:
        mtf_bar = mtf_filter_bar_raw.strip() or "?"
        mtf_fast = mtf_filter_fast_ema_period_raw.strip() or "?"
        mtf_slow = mtf_filter_slow_ema_period_raw.strip() or "?"
        parts.append(f"高周期过滤：{mtf_bar} EMA{mtf_fast}/EMA{mtf_slow} 只决定是否允许低周期新开仓。")
    daily_mode = DAILY_FILTER_MODE_LABEL_TO_VALUE.get(daily_filter_mode_label, "disabled")
    if daily_filter_enabled and daily_mode != "disabled":
        boundary = daily_filter_boundary_label.strip() or "交易所1D"
        scope = daily_filter_scope_label.strip() or "多空都过滤"
        if daily_mode == "weak_day":
            parts.append(f"日线过滤：{boundary} 弱日规则，只使用当时已收盘的上一根日线，{scope}。")
        else:
            daily_ma_type = (daily_filter_ma_type_raw or "EMA").strip().upper()
            daily_period = daily_filter_period_raw.strip() or "?"
            parts.append(
                f"日线过滤：{boundary} close vs {daily_ma_type}{daily_period}，只使用当时已收盘的上一根日线，{scope}。"
            )
    return "趋势参数：" + " ".join(parts)


def _entry_reference_ema_caption(strategy_id: str) -> str:
    return strategy_entry_reference_caption(strategy_id)


def _build_dynamic_protection_hint_text_legacy(
    *,
    take_profit_mode_label: str,
    dynamic_two_r_break_even_enabled: bool,
    break_even_trigger_r_raw: str = "2",
    trailing_start_r_raw: str = "2",
    first_lock_r_raw: str = "0",
    trailing_step_r_raw: str = "1",
    break_even_trigger_r_configurable: bool = False,
    dynamic_fee_offset_enabled: bool,
    time_stop_break_even_enabled: bool,
    time_stop_break_even_bars_raw: str,
) -> str:
    if take_profit_mode_label != "动态止盈":
        return "动态保护：当前为固定止盈，首档触发R / nR保本 / 手续费偏移 / 时间保本都不生效。"
    trigger_r_raw = trailing_start_r_raw.strip() or "2"
    break_even_raw = break_even_trigger_r_raw.strip() or "2"
    trailing_step_raw = trailing_step_r_raw.strip() or "1"
    try:
        trigger_r = max(int(trigger_r_raw), 2)
    except ValueError:
        trigger_r = 2
    try:
        break_even_trigger_r = max(int(break_even_raw), 1)
    except ValueError:
        break_even_trigger_r = 2
    try:
        trailing_step_r = max(int(trailing_step_raw), 1)
    except ValueError:
        trailing_step_r = 1
    time_stop_bars = time_stop_break_even_bars_raw.strip() or "0"
    locked_r = max(trigger_r - trailing_step_r, 0)
    if dynamic_two_r_break_even_enabled:
        first_leg_text = (
            f"保本：开启，价格先到 {break_even_trigger_r}R 时抬到保本位；"
            f"到 {trigger_r}R 后开始移动止盈，先锁 {locked_r}R，之后每 {trailing_step_r}R 推进一次。"
        )
    else:
        first_leg_text = (
            f"保本：关闭，价格达到 {trigger_r}R 后才开始移动止盈，先锁 {locked_r}R，"
            f"之后每 {trailing_step_r}R 推进一次。"
        )
    parts = [
        (
            f"保本触发R：{break_even_trigger_r}；移动止盈触发R：{trigger_r}；移动步长R：{trailing_step_r}。"
            if break_even_trigger_r_configurable
            else "移动止盈触发R：固定为 2。"
        ),
        first_leg_text,
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
    if not break_even_trigger_r_configurable:
        parts[0] = "首档触发R：固定为 2。 " + parts[0]
        first_leg_prefix = "2R保本：开启，" if dynamic_two_r_break_even_enabled else "2R保本：关闭，"
        parts[1] = first_leg_prefix + parts[1]
    return "动态保护： " + " ".join(parts)


def _build_dynamic_protection_hint_text(
    *,
    take_profit_mode_label: str,
    dynamic_two_r_break_even_enabled: bool,
    break_even_trigger_r_raw: str = "2",
    trailing_start_r_raw: str = "2",
    first_lock_r_raw: str = "0",
    trailing_step_r_raw: str = "1",
    break_even_trigger_r_configurable: bool = False,
    dynamic_fee_offset_enabled: bool,
    time_stop_break_even_enabled: bool,
    time_stop_break_even_bars_raw: str,
    trend_ema_close_exit_after_trigger_r_enabled: bool = False,
    trend_ema_close_exit_after_trigger_r_raw: str = "5",
) -> str:
    if take_profit_mode_label != "动态止盈":
        return "动态保护：当前为固定止盈，保本触发R / 移动止盈触发R / 首档锁盈R / 手续费偏移 / 时间保本都不生效。"
    trigger_r_raw = trailing_start_r_raw.strip() or "2"
    break_even_raw = break_even_trigger_r_raw.strip() or "2"
    first_lock_raw = first_lock_r_raw.strip() or "0"
    trailing_step_raw = trailing_step_r_raw.strip() or "1"
    try:
        trigger_r = max(int(trigger_r_raw), 2)
    except ValueError:
        trigger_r = 2
    try:
        break_even_trigger_r = max(int(break_even_raw), 1)
    except ValueError:
        break_even_trigger_r = 2
    try:
        first_lock_r = max(int(first_lock_raw), 0)
    except ValueError:
        first_lock_r = 0
    try:
        trailing_step_r = max(int(trailing_step_raw), 1)
    except ValueError:
        trailing_step_r = 1
    time_stop_bars = time_stop_break_even_bars_raw.strip() or "0"
    trend_exit_r_raw = trend_ema_close_exit_after_trigger_r_raw.strip() or "5"
    try:
        trend_exit_r = max(int(trend_exit_r_raw), 1)
    except ValueError:
        trend_exit_r = 5
    auto_first_lock_r = max(trigger_r - trailing_step_r, 0)
    effective_first_lock_r = first_lock_r if first_lock_r > 0 else auto_first_lock_r
    custom_first_lock_enabled = first_lock_r > 0 and first_lock_r != auto_first_lock_r
    if dynamic_two_r_break_even_enabled:
        if custom_first_lock_enabled:
            next_trigger_r = trigger_r + trailing_step_r
            next_lock_r = effective_first_lock_r + trailing_step_r
            first_leg_text = (
                f"保本：开启，价格先到 {break_even_trigger_r}R 时先移到保本位；"
                f"到 {trigger_r}R 后先锁 {effective_first_lock_r}R，"
                f"到 {next_trigger_r}R 后上移到 {next_lock_r}R，之后每 {trailing_step_r}R 再上移 {trailing_step_r}R。"
            )
        else:
            next_trigger_r = trigger_r + trailing_step_r
            next_lock_r = effective_first_lock_r + trailing_step_r
            first_leg_text = (
                f"保本：开启，价格先到 {break_even_trigger_r}R 时先移到保本位；"
                f"到 {trigger_r}R 后开始移动止盈，先锁 {effective_first_lock_r}R；"
                f"到 {next_trigger_r}R 后上移到 {next_lock_r}R，之后每 {trailing_step_r}R 再上移 {trailing_step_r}R。"
            )
    else:
        if custom_first_lock_enabled:
            next_trigger_r = trigger_r + trailing_step_r
            next_lock_r = effective_first_lock_r + trailing_step_r
            first_leg_text = (
                f"保本：关闭，价格到 {trigger_r}R 后才开始移动止盈，先锁 {effective_first_lock_r}R；"
                f"到 {next_trigger_r}R 后上移到 {next_lock_r}R，之后每 {trailing_step_r}R 再上移 {trailing_step_r}R。"
            )
        else:
            next_trigger_r = trigger_r + trailing_step_r
            next_lock_r = effective_first_lock_r + trailing_step_r
            first_leg_text = (
                f"保本：关闭，价格到 {trigger_r}R 后才开始移动止盈，先锁 {effective_first_lock_r}R；"
                f"到 {next_trigger_r}R 后上移到 {next_lock_r}R，之后每 {trailing_step_r}R 再上移 {trailing_step_r}R。"
            )
    parts = [
        (
            f"保本触发R：{break_even_trigger_r}；移动止盈触发R：{trigger_r}；首档锁盈R："
            f"{first_lock_r if first_lock_r > 0 else '自动'}；移动步长R：{trailing_step_r}。"
            if break_even_trigger_r_configurable
            else "首档触发R：固定为 2。"
        ),
        first_leg_text,
        (
            "手续费偏移：开启，保本位会额外预留双向手续费缓冲。"
            if dynamic_fee_offset_enabled
            else "手续费偏移：关闭，保本位不额外预留手续费缓冲。"
        ),
        (
            f"时间保本：开启，持仓满 {time_stop_bars} 根K线且达到净保本后，再把止损抬到保本位。"
            if time_stop_break_even_enabled
            else f"时间保本：关闭（当前设定 {time_stop_bars} 根，仅保存参数，不会启用）。"
        ),
    ]
    if trend_ema_close_exit_after_trigger_r_enabled:
        parts.append(f"趋势EMA离场：达到 {trend_exit_r}R 后，若收盘跌破趋势EMA则平仓。")
    else:
        parts.append(f"趋势EMA离场：关闭（当前触发R 设为 {trend_exit_r}）。")
    if not break_even_trigger_r_configurable:
        parts[0] = "首档触发R：固定为 2。" + parts[0]
        first_leg_prefix = "2R保本：开启，" if dynamic_two_r_break_even_enabled else "2R保本：关闭，"
        parts[1] = first_leg_prefix + parts[1]
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
    api_environment_label: str = "",
) -> str:
    def _environment_value_from_label(label: str) -> str:
        normalized = str(label or "").strip().lower()
        if normalized.endswith("live") or normalized == "live":
            return "live"
        if normalized.endswith("demo") or normalized == "demo":
            return "demo"
        return ""

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
            if strategy_supports_dynamic_take_profit(config.strategy_id) and config.take_profit_mode == "dynamic":
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

    def _startup_chase_current_signal_text() -> str:
        if not supports_startup_chase_current_signal(config.strategy_id):
            return "-"
        if config.startup_chase_current_signal:
            return "开启（导入启动时若当前信号仍有效，本次直接接管）"
        return "关闭（导入启动时不接当前波，只等下一次新信号）"

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

    def _minimum_order_text() -> str:
        recommendation = recommended_minimum_risk_amount_for_strategy(
            config.strategy_id,
            strategy_symbol or config.inst_id,
            config.signal_mode,
        )
        if recommendation is not None:
            return format_risk_recommendation(recommendation)
        if instrument is None:
            return "-（当前未读取到合约最小下单规格）"
        min_order_text = _format_contract_size_with_equivalent(instrument, instrument.min_size)
        if config.run_mode == "signal_only":
            return f"{min_order_text}（当前仅发信号，不下单）"
        current_risk_amount = _parse_positive_decimal_hint(risk_value)
        current_fixed_size = _parse_positive_decimal_hint(fixed_size)
        if current_risk_amount is not None:
            return (
                f"{min_order_text}（当前风险金 {format_decimal(current_risk_amount)}；"
                "若止损过宽，仍可能被最小单量顶到最小仓位）"
            )
        if current_fixed_size is not None and current_fixed_size < instrument.min_size:
            return f"{min_order_text}（当前固定数量 {format_decimal(current_fixed_size)} 低于最小下单量）"
        return min_order_text

    def _custom_trigger_text() -> str:
        if config.tp_sl_mode != "local_custom":
            return "-（当前模式未使用）"
        return f"{custom_trigger_symbol or '-'}（本地止盈止损按这个标的触发）"

    def _time_stop_break_even_text() -> str:
        if config.time_stop_break_even_enabled:
            return (
                f"开启 / {config.resolved_time_stop_break_even_bars()}根"
                "（持仓满指定K线且达到净保本时再抬止损）"
            )
        return f"关闭 / {config.resolved_time_stop_break_even_bars()}根（当前仅保存参数，不启用）"

    def _trend_ema_exit_text() -> str:
        trigger_r = config.resolved_trend_ema_close_exit_after_trigger_r()
        if config.trend_ema_close_exit_after_trigger_r_enabled:
            return f"开启（达到 {trigger_r}R 后，若收盘跌破趋势EMA则平仓）"
        return f"关闭（当前触发R 设为 {trigger_r}）"

    def _dynamic_protection_rule_lines(*, include_trend_exit: bool) -> list[str]:
        if config.take_profit_mode != "dynamic":
            return []
        rules = config.resolved_dynamic_protection_rules()
        if rules:
            rule_summary = " / ".join(
                describe_dynamic_protection_rules(
                    rules,
                    fee_offset_enabled=bool(config.dynamic_fee_offset_enabled),
                )
            )
            lines = [f"动态保护规则：{rule_summary}"]
        else:
            lines = [
                _build_dynamic_protection_hint_text(
                    take_profit_mode_label=take_profit_mode_label,
                    dynamic_two_r_break_even_enabled=bool(config.dynamic_two_r_break_even),
                    break_even_trigger_r_raw=str(config.dynamic_break_even_trigger_r),
                    trailing_start_r_raw=str(config.ema55_slope_lock_profit_trigger_r),
                    first_lock_r_raw=str(config.dynamic_first_lock_r),
                    trailing_step_r_raw=str(config.dynamic_trailing_step_r),
                    break_even_trigger_r_configurable=strategy_uses_parameter(
                        config.strategy_id,
                        "dynamic_break_even_trigger_r",
                    ),
                    dynamic_fee_offset_enabled=bool(config.dynamic_fee_offset_enabled),
                    time_stop_break_even_enabled=bool(config.time_stop_break_even_enabled),
                    time_stop_break_even_bars_raw=str(config.time_stop_break_even_bars),
                    trend_ema_close_exit_after_trigger_r_enabled=bool(
                        config.trend_ema_close_exit_after_trigger_r_enabled
                    ),
                    trend_ema_close_exit_after_trigger_r_raw=str(
                        config.trend_ema_close_exit_after_trigger_r
                    ),
                )
            ]
        lines.append(
            "手续费偏移："
            + (
                "开启（保本/锁盈位额外预留双边手续费）"
                if config.dynamic_fee_offset_enabled
                else "关闭（保本/锁盈位不额外预留手续费）"
            )
        )
        lines.append(f"时间保本：{_time_stop_break_even_text()}")
        if include_trend_exit:
            lines.append(f"趋势EMA离场：{_trend_ema_exit_text()}")
        return lines

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

    profile = get_strategy_runtime_profile(config.strategy_id)
    visibility = build_strategy_widget_visibility(config.strategy_id, "launcher")
    api_text = (api_label or "").strip() or "-"
    api_environment_text = (api_environment_label or "").strip() or "-"
    strategy_environment_text = (environment_label or "").strip() or "-"
    api_environment_value = _environment_value_from_label(api_environment_text)
    strategy_environment_value = _environment_value_from_label(strategy_environment_text)
    if api_environment_value and strategy_environment_value:
        environment_status = "一致" if api_environment_value == strategy_environment_value else "不一致（将阻止启动）"
    else:
        environment_status = "未校验"

    lines = [
        f"策略：{strategy_name}",
        "",
        "基础信息：",
        f"运行模式：{run_mode_label}",
        f"API：{api_text}",
        f"API环境：{api_environment_text}",
        f"策略环境：{strategy_environment_text}",
        f"环境状态：{environment_status}",
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
        (
            f"信号均线：{config.ema_label()}"
            if profile.family == "ema55_slope_short"
            else f"快线均线：{config.ema_label()}"
        ),
        f"趋势均线：{config.trend_ema_label()}",
    ]
    if config.uses_daily_filter():
        lines.append(config.daily_filter_summary())
    if config.uses_runtime_gate():
        lines.append(config.runtime_gate_summary())
    if visibility.show_big_ema:
        lines.append(f"EMA大周期：{config.big_ema_period}（大趋势过滤线）")
    if profile.family in {"cross_breakout_long", "cross_breakdown_short"}:
        lines.append(f"突破参考线：{config.entry_reference_line_label()}（已收盘K线的突破/跌破判断基准）")
    if profile.uses_dynamic_orders or profile.family in {"cross_breakout_long", "cross_breakdown_short"}:
        if profile.uses_dynamic_orders:
            lines.extend(
                [
                    f"挂单参考线：{config.entry_reference_line_label()}（挂单价格锚点）",
                    f"止盈方式：{take_profit_mode_text}",
                    f"每波最多开仓次数：{config.max_entries_per_trend if config.max_entries_per_trend > 0 else '不限'}（同一波最多允许开仓的次数）",
                    f"启动追单窗口：{_startup_chase_text()}",
                    f"追当前信号：{_startup_chase_current_signal_text()}",
                ]
            )
        else:
            # 突破参考 EMA 已在上方「参数说明」首段追加，此处勿重复
            lines.extend(
                [
                    f"止盈方式：{take_profit_mode_text}",
                    f"每波最多开仓次数：{config.max_entries_per_trend if config.max_entries_per_trend > 0 else '不限'}（同一波最多允许开仓的次数）",
                    f"启动追单窗口：{_startup_chase_text()}",
                    f"追当前信号：{_startup_chase_current_signal_text()}",
                ]
            )
        if config.take_profit_mode == "dynamic":
            lines.extend(_dynamic_protection_rule_lines(include_trend_exit=True))
    if profile.family == "ema55_slope_short":
        """
        lines.extend(
            [
                (
                    "寮€绌烘枩鐜囬槇鍊硷細"
                    f"{format_decimal_fixed(Decimal(str(config.trend_ema_slope_filter_min_ratio)), 6)}"
                    "锛圗MA55 鍗曟牴鏂滅巼 / 褰撳墠 EMA55锛屽皬浜庣瓑浜庤璐熷€兼墠寮€绌猴級"
                ),
                f"姝㈢泩鏂瑰紡锛歿take_profit_mode_text}",
                "骞充粨淇″彿锛欵MA55 鏂滅巼杞鍚庯紝鏈湴鎸夋敹鐩樼‘璁ゅ钩浠?),
            ]
        )
        if config.take_profit_mode == "dynamic":
            lines.extend(
                [
                    f"2R淇濇湰寮€鍏筹細{config.dynamic_two_r_break_even_label()}锛堟诞鐩堣揪鍒?2R 鍚庢鎹熸姮鍒颁繚鏈級",
                    f"鎵嬬画璐瑰亸绉诲紑鍏筹細{config.dynamic_fee_offset_enabled_label()}锛堜繚鏈綅棰勭暀鍙岃竟鎵嬬画璐癸級",
                    (
                        f"鏃堕棿淇濇湰锛歿config.time_stop_break_even_enabled_label()} / "
                        f"{config.resolved_time_stop_break_even_bars()}鏍癸紙鎸佷粨婊℃寚瀹欿绾夸笖杈惧埌鍑€淇濇湰鏃跺啀鎶鎹燂級"
                    ),
                ]
            )
        """
        line_label = config.ema_label()
        lines.extend(
            [
                (
                    "开空斜率阈值："
                    f"{format_decimal_fixed(Decimal(str(config.trend_ema_slope_filter_min_ratio)), 6)}"
                    f"（{line_label} 单根斜率 / 当前 {line_label}，小于等于该负值才开空）"
                ),
                f"止盈方式：{take_profit_mode_text}",
                f"启动追单窗口：{_startup_chase_text()}",
                f"追当前信号：{_startup_chase_current_signal_text()}",
                (
                    f"平仓信号：{line_label} 斜率转正后，本地按收盘确认平仓"
                    if config.ema55_slope_exit_enabled
                    else "平仓信号：已关闭斜率转正平仓，仅保留 ATR 止损与动态保护离场"
                ),
            ]
        )
        if config.take_profit_mode == "dynamic":
            lines.extend(_dynamic_protection_rule_lines(include_trend_exit=False))
    lines.extend(
        [
            f"ATR周期：{config.atr_period}（波动计算周期）",
            f"止损 ATR 倍数：{stop_atr_text}（止损距离 = {stop_atr_text} × ATR）",
            f"止盈 ATR 倍数：{take_profit_atr_text}",
            f"风险金：{_risk_amount_text()}",
            f"固定数量：{_fixed_size_text()}",
            "",
            "策略规则：",
            f"回测参考：{_minimum_order_text()}",
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
    market_value_usdt_values = [
        _position_market_value_usdt(item, position_instruments, upl_usdt_prices)
        for item in positions
    ]
    btc_price_usdt = upl_usdt_prices.get("BTC")
    btc_market_value = (
        sum(
            (
                value / btc_price_usdt
                for item, value in zip(positions, market_value_usdt_values)
                if value is not None
                and _extract_asset_key(item.inst_id).strip().upper() == "BTC"
            ),
            Decimal("0"),
        )
        if btc_price_usdt is not None and btc_price_usdt > 0
        else None
    )
    return {
        "count": len(positions),
        "size_display": _format_group_position_size(positions, position_instruments),
        "option_side_display": _format_group_option_trade_side(positions, position_instruments),
        "upl": _sum_decimal([item.unrealized_pnl for item in positions]),
        "upl_usdt": _sum_decimal([_position_unrealized_pnl_usdt(item, upl_usdt_prices) for item in positions]),
        "market_value_usdt": _sum_decimal(market_value_usdt_values),
        "market_value_native": btc_market_value,
        "market_value_currency": "BTC" if btc_market_value is not None else None,
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
    market_value_native = metrics.get("market_value_native")
    market_value_currency = metrics.get("market_value_currency")
    if isinstance(market_value_native, Decimal) and isinstance(market_value_currency, str) and market_value_currency:
        market_value_text = (
            f"{format_decimal_fixed(market_value_native, 2)} {market_value_currency}"
            f"（{_format_optional_approx_usdt(metrics['market_value_usdt'] if isinstance(metrics.get('market_value_usdt'), Decimal) else None)}）"
        )
    else:
        market_value_text = _format_optional_approx_usdt(
            metrics["market_value_usdt"] if isinstance(metrics.get("market_value_usdt"), Decimal) else None
        )
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
        market_value_text,
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

    contract_value, contract_currency = _position_contract_value_snapshot(position, instrument)
    if contract_value is not None and contract_value > 0 and contract_currency:
        multiplier = (
            instrument.ct_mult
            if instrument is not None and instrument.ct_mult is not None and instrument.ct_mult > 0
            else Decimal("1")
        )
        quote_currency = contract_currency.upper()
        if quote_currency in {"USD", "USDT", "USDC"} and position.inst_type in {"FUTURES", "SWAP"}:
            reference_price = position.mark_price or position.last_price or position.avg_price
            base_currency = _extract_asset_key(position.inst_id).upper()
            if reference_price is not None and reference_price > 0 and base_currency:
                amount = (abs(position.position) * contract_value * multiplier / reference_price) * sign
                return amount, base_currency
        amount = abs(position.position) * contract_value * multiplier * sign
        return amount, quote_currency

    if position.inst_type in {"OPTION", "SWAP", "FUTURES"}:
        # Without contract specs, raw derivative size is contracts, not base coin.
        return abs(position.position) * sign, "张"

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


def _position_contract_value_snapshot(
    position: OkxPosition,
    instrument: Instrument | None,
) -> tuple[Decimal | None, str | None]:
    if instrument is not None and instrument.ct_val is not None and instrument.ct_val > 0 and instrument.ct_val_ccy:
        return instrument.ct_val, instrument.ct_val_ccy.upper()
    return _fallback_position_contract_value(position.inst_id, position.inst_type)


def _fallback_position_contract_value(inst_id: str, inst_type: str) -> tuple[Decimal | None, str | None]:
    normalized_type = str(inst_type or "").strip().upper()
    asset = _extract_asset_key(inst_id).upper()
    quote = (_extract_quote_key(inst_id) or "").upper()
    if not asset:
        return None, None

    if normalized_type == "OPTION" and quote == "USD":
        option_contract_values = {
            "BTC": Decimal("0.01"),
            "ETH": Decimal("0.1"),
        }
        contract_value = option_contract_values.get(asset)
        if contract_value is not None:
            return contract_value, asset

    if normalized_type in {"SWAP", "FUTURES"} and quote == "USD":
        inverse_contract_values = {
            "BTC": Decimal("100"),
        }
        contract_value = inverse_contract_values.get(asset)
        if contract_value is not None:
            return contract_value, "USD"

    if normalized_type in {"SWAP", "FUTURES"} and quote in {"USDT", "USDC"}:
        linear_contract_values = {
            "BTC": Decimal("0.01"),
            "ETH": Decimal("0.1"),
            "BNB": Decimal("0.01"),
            "OKB": Decimal("0.01"),
            "SOL": Decimal("1"),
            "DOGE": Decimal("1000"),
            "XRP": Decimal("100"),
        }
        contract_value = linear_contract_values.get(asset)
        if contract_value is not None:
            return contract_value, asset

    return None, None


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


def _pnl_display_places(currency: object) -> int:
    text = str(currency).strip().upper() if currency is not None else ""
    if text in {"USDT", "USD", "USDC"}:
        return 2
    return 8


def _group_pnl_places(currency: object) -> int:
    return _pnl_display_places(currency)


def _position_contract_amount(
    position: OkxPosition,
    position_instruments: dict[str, Instrument],
) -> tuple[Decimal | None, str | None]:
    instrument = position_instruments.get(position.inst_id)
    contract_value, contract_currency = _position_contract_value_snapshot(position, instrument)
    if contract_value is not None and contract_value > 0 and contract_currency:
        multiplier = (
            instrument.ct_mult
            if instrument is not None and instrument.ct_mult is not None and instrument.ct_mult > 0
            else Decimal("1")
        )
        amount = abs(position.position) * contract_value * multiplier
        return amount, contract_currency.upper()
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
    currency = _infer_upl_currency(position)
    places = _pnl_display_places(currency)
    amount_text = _format_optional_decimal_fixed(position.unrealized_pnl, places=places, with_sign=True)
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
    for position in positions:
        inferred_currency = _infer_upl_currency(position)
        if inferred_currency:
            currencies.add(inferred_currency)
        if position.inst_type == "OPTION":
            asset_currency = _extract_asset_key(position.inst_id).upper()
            if asset_currency:
                currencies.add(asset_currency)
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
    needed_ids = {position.inst_id for position in positions if position.inst_id}
    result: dict[str, Instrument] = {}

    option_families = sorted(
        {
            infer_option_family(position.inst_id)
            for position in positions
            if position.inst_type == "OPTION" and infer_option_family(position.inst_id)
        }
    )
    for family in option_families:
        family_needed_ids = {
            position.inst_id
            for position in positions
            if position.inst_type == "OPTION" and infer_option_family(position.inst_id) == family
        }
        try:
            family_instruments = client.get_option_instruments(inst_family=family)
        except Exception:
            family_instruments = []
        for instrument in family_instruments:
            if instrument.inst_id in needed_ids:
                result[instrument.inst_id] = instrument
        if family_needed_ids.issubset(result.keys()):
            continue
        try:
            uly_instruments = client.get_option_instruments(uly=family)
        except Exception:
            uly_instruments = []
        for instrument in uly_instruments:
            if instrument.inst_id in needed_ids:
                result[instrument.inst_id] = instrument

    swap_ids = {position.inst_id for position in positions if position.inst_type == "SWAP"}
    if swap_ids:
        try:
            for instrument in client.get_swap_instruments():
                if instrument.inst_id in swap_ids:
                    result[instrument.inst_id] = instrument
        except Exception:
            pass

    futures_ids = {position.inst_id for position in positions if position.inst_type == "FUTURES"}
    if futures_ids:
        try:
            for instrument in client.get_instruments("FUTURES"):
                if instrument.inst_id in futures_ids:
                    result[instrument.inst_id] = instrument
        except Exception:
            pass

    missing_ids = needed_ids.difference(result.keys())
    for inst_id in sorted(missing_ids):
        try:
            instrument = client.get_instrument(inst_id)
        except Exception:
            continue
        if instrument.inst_id in needed_ids:
            result[instrument.inst_id] = instrument

    return result


def _build_position_ticker_map(client: OkxRestClient, positions: list[OkxPosition]) -> dict[str, OkxTicker]:
    result: dict[str, OkxTicker] = {}
    needed_ids = {position.inst_id for position in positions if position.inst_id}
    if not needed_ids:
        return result

    option_families = sorted(
        {
            infer_option_family(position.inst_id)
            for position in positions
            if position.inst_type == "OPTION" and infer_option_family(position.inst_id)
        }
    )
    for family in option_families:
        family_needed_ids = {
            position.inst_id
            for position in positions
            if position.inst_type == "OPTION" and infer_option_family(position.inst_id) == family
        }
        try:
            uly_tickers = client.get_tickers("OPTION", uly=family)
        except Exception:
            uly_tickers = []
        for ticker in uly_tickers:
            if ticker.inst_id in needed_ids:
                result[ticker.inst_id] = ticker
        if family_needed_ids.issubset(result.keys()):
            continue
        try:
            family_tickers = client.get_tickers("OPTION", inst_family=family)
        except Exception:
            family_tickers = []
        for ticker in family_tickers:
            if ticker.inst_id in needed_ids:
                result[ticker.inst_id] = ticker

    for inst_type in ("SWAP", "FUTURES", "SPOT", "MARGIN"):
        type_needed_ids = {
            position.inst_id
            for position in positions
            if position.inst_id and str(position.inst_type or "").upper() == inst_type
        }
        if not type_needed_ids:
            continue
        try:
            for ticker in client.get_tickers(inst_type):
                if ticker.inst_id in type_needed_ids:
                    result[ticker.inst_id] = ticker
        except Exception:
            continue
    missing_ids = needed_ids.difference(result.keys())
    for inst_id in sorted(missing_ids):
        try:
            result[inst_id] = client.get_ticker(inst_id)
        except Exception:
            continue
    for inst_id in sorted(needed_ids):
        ticker = result.get(inst_id)
        ticker_bid = getattr(ticker, "bid", None)
        ticker_ask = getattr(ticker, "ask", None)
        if ticker is not None and ticker_bid is not None and ticker_ask is not None:
            continue
        try:
            order_book = client.get_order_book(inst_id, depth=1)
        except Exception:
            continue
        bid_price = order_book.bids[0][0] if order_book.bids else None
        ask_price = order_book.asks[0][0] if order_book.asks else None
        if bid_price is None and ask_price is None:
            continue
        result[inst_id] = _merge_ticker_order_book_quotes(ticker, inst_id, bid_price=bid_price, ask_price=ask_price)
    return result


def _merge_ticker_order_book_quotes(
    ticker: OkxTicker | None,
    inst_id: str,
    *,
    bid_price: Decimal | None,
    ask_price: Decimal | None,
) -> OkxTicker:
    raw = dict(ticker.raw) if ticker is not None and isinstance(ticker.raw, dict) else {}
    raw["orderBookBidPx"] = str(bid_price) if bid_price is not None else ""
    raw["orderBookAskPx"] = str(ask_price) if ask_price is not None else ""
    return OkxTicker(
        inst_id=getattr(ticker, "inst_id", inst_id) if ticker is not None else inst_id,
        last=getattr(ticker, "last", None) if ticker is not None else None,
        bid=getattr(ticker, "bid", None) if ticker is not None and getattr(ticker, "bid", None) is not None else bid_price,
        ask=getattr(ticker, "ask", None) if ticker is not None and getattr(ticker, "ask", None) is not None else ask_price,
        mark=getattr(ticker, "mark", None) if ticker is not None else None,
        index=getattr(ticker, "index", None) if ticker is not None else None,
        raw=raw,
    )


def _augment_upl_usdt_prices_from_positions(
    prices: dict[str, Decimal],
    positions: list[OkxPosition],
    position_tickers: dict[str, OkxTicker],
) -> dict[str, Decimal]:
    augmented = dict(prices)
    augmented.setdefault("USD", Decimal("1"))
    augmented.setdefault("USDT", Decimal("1"))
    augmented.setdefault("USDC", Decimal("1"))

    def _set_price(currency: str, value: Decimal | None) -> None:
        normalized = currency.strip().upper()
        if not normalized or normalized in augmented:
            return
        if value is not None and value > 0:
            augmented[normalized] = value

    def _ticker_reference_price(ticker: OkxTicker | None, *, allow_mark: bool) -> Decimal | None:
        if ticker is None:
            return None
        candidates = (ticker.index, ticker.last, ticker.mark, ticker.bid, ticker.ask) if allow_mark else (ticker.index, ticker.last)
        for candidate in candidates:
            if candidate is not None and candidate > 0:
                return candidate
        return None

    for position in positions:
        asset_currency = _extract_asset_key(position.inst_id).upper()
        quote_currency = (_extract_quote_key(position.inst_id) or "").upper()
        ticker = position_tickers.get(position.inst_id)
        if not asset_currency:
            continue

        if position.inst_type in {"SWAP", "FUTURES", "SPOT"} and quote_currency in {"USD", "USDT", "USDC"}:
            position_price = position.mark_price or position.last_price or position.avg_price
            _set_price(asset_currency, position_price)
            _set_price(asset_currency, _ticker_reference_price(ticker, allow_mark=True))
            continue

        if position.inst_type == "OPTION":
            raw = position.raw if isinstance(position.raw, dict) else {}
            for key in ("idxPx", "indexPx", "underlyingPx", "ulyPx"):
                raw_price = _parse_decimal_or_none(raw.get(key))
                if raw_price is not None and raw_price > 0:
                    _set_price(asset_currency, raw_price)
                    break
            _set_price(asset_currency, _ticker_reference_price(ticker, allow_mark=False))

    return augmented


def _apply_position_ticker_prices(
    positions: list[OkxPosition],
    position_tickers: dict[str, OkxTicker],
) -> list[OkxPosition]:
    enriched: list[OkxPosition] = []
    for position in positions:
        ticker = position_tickers.get(position.inst_id)
        if ticker is None:
            enriched.append(position)
            continue
        mark_price = position.mark_price or ticker.mark
        last_price = position.last_price or ticker.last
        if mark_price is None:
            mark_price = last_price
        if mark_price == position.mark_price and last_price == position.last_price:
            enriched.append(position)
            continue
        enriched.append(
            OkxPosition(
                **{
                    **position.__dict__,
                    "mark_price": mark_price,
                    "last_price": last_price,
                }
            )
        )
    return enriched


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
        f"{_format_position_realized_pnl(position)} / "
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

    try:
        current_trigger_price = client.get_trigger_price(protection.trigger_inst_id, protection.trigger_price_type)
    except OkxApiError:
        current_trigger_price = None
    if current_trigger_price is not None:
        stop_hit, take_hit = evaluate_protection_trigger(
            direction=protection.direction,
            current_price=current_trigger_price,
            stop_loss=protection.stop_loss_trigger,
            take_profit=protection.take_profit_trigger,
            option_inst_id=protection.option_inst_id,
            uses_underlying_trigger=(
                protection.trigger_inst_id.strip().upper() != protection.option_inst_id.strip().upper()
                or protection.trigger_price_type != "mark"
            ),
        )
        if stop_hit or take_hit:
            trigger_kind = "止损触发" if stop_hit else "止盈触发"
            trigger_value = protection.stop_loss_trigger if stop_hit else protection.take_profit_trigger
            if trigger_value is None:
                guidance = "请调整触发价后再启动。"
            elif trigger_value < current_trigger_price:
                guidance = (
                    f"{format_decimal(trigger_value)} 低于当前价 {format_decimal(current_trigger_price)}，"
                    "属于下跌触发；如果你想等跌破后再触发，请填写在止损触发。"
                )
            elif trigger_value > current_trigger_price:
                guidance = (
                    f"{format_decimal(trigger_value)} 高于当前价 {format_decimal(current_trigger_price)}，"
                    "属于上涨触发；如果你想等涨破后再触发，请填写在止盈触发。"
                )
            else:
                guidance = (
                    f"{format_decimal(trigger_value)} 与当前价 {format_decimal(current_trigger_price)} 相等，"
                    "保护任务会在启动后立刻触发，请调整触发价。"
                )
            raise ValueError(
                f"{protection.trigger_label or protection.trigger_inst_id} 当前价 {format_decimal(current_trigger_price)} "
                f"已满足{trigger_kind}。为避免启动后立刻触发，本次已阻止启动。{guidance}"
            )

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


def _decimal_sign(value: Decimal | None) -> int:
    if value is None:
        return 0
    if value > 0:
        return 1
    if value < 0:
        return -1
    return 0


def _normalize_position_history_trade_side(value: str | None) -> str | None:
    token = str(value or "").strip().lower()
    if token in {"buy", "long"}:
        return "buy"
    if token in {"sell", "short"}:
        return "sell"
    return None


def _infer_position_history_trade_side(item: OkxPositionHistoryItem) -> tuple[str | None, bool]:
    explicit_side = _normalize_position_history_trade_side(item.pos_side) or _normalize_position_history_trade_side(item.direction)
    if explicit_side is not None:
        return explicit_side, False
    if item.open_avg_price is None or item.close_avg_price is None:
        return None, False
    price_delta = item.close_avg_price - item.open_avg_price
    price_sign = _decimal_sign(price_delta)
    if price_sign == 0:
        return None, False
    pnl_source: Decimal | None = None
    for candidate in (item.realized_pnl, item.pnl, item.settle_pnl):
        if _decimal_sign(candidate) != 0:
            pnl_source = candidate
            break
    if pnl_source is None:
        return None, False
    effective_pnl = pnl_source
    if item.fee is not None:
        effective_pnl -= item.fee
    if item.funding_fee is not None:
        effective_pnl -= item.funding_fee
    pnl_sign = _decimal_sign(effective_pnl)
    if pnl_sign == 0:
        pnl_sign = _decimal_sign(pnl_source)
    if pnl_sign == 0:
        return None, False
    return ("buy", True) if price_sign == pnl_sign else ("sell", True)


def _format_position_history_trade_side(item: OkxPositionHistoryItem) -> str:
    side, inferred = _infer_position_history_trade_side(item)
    if side == "buy":
        return "买入开仓（推断）" if inferred else "买入开仓"
    if side == "sell":
        return "卖出开仓（推断）" if inferred else "卖出开仓"
    return "无法判断"


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
                    _position_history_auto_summary(item),
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
        has_broker_prefix = candidate.startswith(CUSTOM_ORDER_ID_PREFIX.lower())
        suffix = candidate[len(CUSTOM_ORDER_ID_PREFIX):] if has_broker_prefix else candidate
        if PROTECTION_CL_ORD_ID_PATTERN.fullmatch(suffix) or (has_broker_prefix and suffix.startswith("pp")):
            return "风控保护"
        if SMART_ORDER_CL_ORD_ID_PATTERN.fullmatch(suffix) or (has_broker_prefix and suffix.startswith("so")):
            return "智能下单"
        if ENGINE_CL_ORD_ID_PATTERN.fullmatch(candidate) or ENGINE_CL_ORD_ID_PATTERN.fullmatch(suffix):
            return "策略引擎"
        if has_broker_prefix and suffix.startswith(("arb", "pair")):
            return "套利执行"
        if has_broker_prefix:
            return "本程序委托"
    return None


def _session_order_prefixes(session: StrategySession) -> tuple[str, ...]:
    session_id = str(getattr(session, "session_id", "") or "")
    strategy_name = str(getattr(session, "strategy_name", "") or getattr(session, "strategy_id", "") or "")
    session_token = "".join(ch for ch in session_id.lower() if ch.isascii() and ch.isalnum())[:4] or "sess"
    strategy_token = "".join(ch for ch in strategy_name.lower() if ch.isascii() and ch.isalnum())[:4] or "stg"
    return (
        f"{session_token}{strategy_token}",
        strategy_order_identity(session_id, strategy_name),
    )


def _strategy_live_chart_fill_session_role(item: OkxFillHistoryItem, session: StrategySession) -> str | None:
    raw_value = getattr(item, "raw", None)
    raw = raw_value if isinstance(raw_value, dict) else {}
    prefixes = _session_order_prefixes(session)
    candidates = [
        str(raw.get("clOrdId") or "").strip().lower(),
        str(raw.get("algoClOrdId") or "").strip().lower(),
    ]
    for candidate in candidates:
        if not candidate:
            continue
        if candidate.startswith(CUSTOM_ORDER_ID_PREFIX.lower()):
            candidate = candidate[len(CUSTOM_ORDER_ID_PREFIX):]
        for prefix in prefixes:
            if candidate.startswith(prefix):
                role = candidate[len(prefix): len(prefix) + 3]
                if role in {"ent", "exi", "slg"}:
                    return role
    return None


def _strategy_live_chart_fill_belongs_to_session(item: OkxFillHistoryItem, session: StrategySession) -> bool:
    return _strategy_live_chart_fill_session_role(item, session) is not None


def _trade_order_session_role(item: OkxTradeOrderItem, session: StrategySession) -> str | None:
    prefixes = _session_order_prefixes(session)
    candidates = [
        (item.client_order_id or "").strip().lower(),
        (item.algo_client_order_id or "").strip().lower(),
    ]
    for candidate in candidates:
        if not candidate:
            continue
        if candidate.startswith(CUSTOM_ORDER_ID_PREFIX.lower()):
            candidate = candidate[len(CUSTOM_ORDER_ID_PREFIX):]
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
    realized_totals: dict[str, Decimal] = {}
    profit_totals: dict[str, Decimal] = {}
    loss_totals: dict[str, Decimal] = {}
    realized_usdt_total = Decimal("0")
    realized_usdt_count = 0
    profit_usdt_total = Decimal("0")
    loss_usdt_total = Decimal("0")
    profit_count = 0
    loss_count = 0
    flat_count = 0
    valid_count = 0

    for _, item in filtered_items:
        currency = _infer_position_history_pnl_currency(item)
        realized_pnl = item.realized_pnl
        if realized_pnl is not None:
            valid_count += 1
            realized_totals[currency] = realized_totals.get(currency, Decimal("0")) + realized_pnl
            if realized_pnl > 0:
                profit_count += 1
                profit_totals[currency] = profit_totals.get(currency, Decimal("0")) + realized_pnl
            elif realized_pnl < 0:
                loss_count += 1
                loss_totals[currency] = loss_totals.get(currency, Decimal("0")) + realized_pnl
            else:
                flat_count += 1
        realized_usdt = _position_history_realized_pnl_usdt(item, usdt_prices)
        if realized_usdt is not None:
            realized_usdt_total += realized_usdt
            realized_usdt_count += 1
            if realized_usdt > 0:
                profit_usdt_total += realized_usdt
            elif realized_usdt < 0:
                loss_usdt_total += realized_usdt

    realized_usdt_text = (
        _format_optional_usdt(realized_usdt_total)
        if realized_usdt_count
        else "-"
    )
    amounts_are_usdt = valid_count > 0 and realized_usdt_count == valid_count
    if amounts_are_usdt:
        profit_amount_text = f"{_format_optional_usdt(profit_usdt_total, with_sign=True)} USDT"
        loss_amount_text = f"{_format_optional_usdt(loss_usdt_total, with_sign=True)} USDT"
        profit_for_ratio = profit_usdt_total
        loss_for_ratio = abs(loss_usdt_total)
    else:
        profit_amount_text = _format_position_history_currency_totals(profit_totals)
        loss_amount_text = _format_position_history_currency_totals(loss_totals)
        if len(realized_totals) == 1:
            profit_for_ratio = sum(profit_totals.values(), Decimal("0"))
            loss_for_ratio = abs(sum(loss_totals.values(), Decimal("0")))
        else:
            profit_for_ratio = None
            loss_for_ratio = None

    if profit_for_ratio is None or loss_for_ratio is None:
        profit_loss_ratio_text = "-"
    elif loss_for_ratio > 0:
        profit_loss_ratio_text = f"{format_decimal_fixed(profit_for_ratio / loss_for_ratio, 2)}:1"
    elif profit_for_ratio > 0:
        profit_loss_ratio_text = "∞:1"
    else:
        profit_loss_ratio_text = "-"
    win_rate_text = (
        f"{format_decimal_fixed(Decimal(profit_count) * Decimal(100) / Decimal(valid_count), 2)}%"
        if valid_count
        else "-"
    )
    return (
        f"\u5df2\u5b9e\u73b0\u6536\u76ca\u5408\u8ba1 { _format_position_history_currency_totals(realized_totals) } | "
        f"\u6298\u5408USDT\u5408\u8ba1 {realized_usdt_text} | "
        f"\u76c8\u5229 {profit_count}\u7b14/{profit_amount_text} | "
        f"\u4e8f\u635f {loss_count}\u7b14/{loss_amount_text} | "
        f"\u6301\u5e73 {flat_count}\u7b14 | \u76c8\u4e8f\u6bd4 {profit_loss_ratio_text} | \u80dc\u7387 {win_rate_text}"
    )


def _infer_position_history_pnl_currency(item: OkxPositionHistoryItem) -> str:
    raw = item.raw if isinstance(item.raw, dict) else {}
    raw_currency = str(raw.get("ccy") or raw.get("pnlCcy") or raw.get("feeCcy") or "").strip().upper()
    if raw_currency:
        return raw_currency
    quote_currency = _extract_quote_key(item.inst_id)
    if item.inst_type in {"SWAP", "SPOT", "FUTURES"} and quote_currency in {"USDT", "USD", "USDC"}:
        return quote_currency
    return _extract_asset_key(item.inst_id).upper()


def _format_position_realized_pnl(position: OkxPosition) -> str:
    currency = _infer_upl_currency(position)
    return _format_optional_decimal_fixed(
        position.realized_pnl,
        places=_pnl_display_places(currency),
        with_sign=True,
    )


def _position_history_realized_pnl_usdt(
    item: OkxPositionHistoryItem,
    upl_usdt_prices: dict[str, Decimal],
) -> Decimal | None:
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
    use_swap_contract_fallback: bool = False,
) -> tuple[Decimal | None, str | None]:
    if size is None:
        return None, None

    instrument = _resolve_history_instrument(inst_id=inst_id, inst_type=inst_type, instruments=instruments)
    if instrument is None or instrument.ct_val is None or instrument.ct_val <= 0 or not instrument.ct_val_ccy:
        normalized_type = (inst_type or "").upper()
        quote_currency = (_extract_quote_key(inst_id) or "").upper()
        base_currency = _extract_asset_key(inst_id).upper()
        if use_swap_contract_fallback and normalized_type == "SWAP" and quote_currency in {"USDT", "USDC"}:
            # Keep historic quantity readable while public instrument metadata
            # is unavailable.  This only affects presentation; live metadata
            # takes precedence whenever it is cached.
            linear_contract_values = {
                "BTC": Decimal("0.01"),
                "ETH": Decimal("0.1"),
                "BNB": Decimal("0.01"),
                "OKB": Decimal("0.01"),
                "SOL": Decimal("1"),
                "DOGE": Decimal("1000"),
                "XRP": Decimal("100"),
            }
            contract_value = linear_contract_values.get(base_currency)
            if contract_value is not None:
                return abs(size) * contract_value, base_currency
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
    status_text = _position_history_auto_summary(item) or "完整平仓/最终快照"
    incremental_close_total = _position_history_incremental_close_total(item)
    incremental_close_line = ""
    if incremental_close_total is not None:
        incremental_item = OkxPositionHistoryItem(
            **{
                **item.__dict__,
                "close_size": incremental_close_total,
            }
        )
        incremental_close_line = (
            f"本次增量平仓：{_format_position_history_size(incremental_item, instruments)}\n"
        )
    return (
        f"更新时间：{_format_okx_ms_timestamp(item.update_time)}\n"
        f"合约：{item.inst_id or '-'}\n"
        f"类型：{item.inst_type or '-'}\n"
        f"{note_line}"
        f"状态：{status_text}\n"
        f"保证金模式：{_format_margin_mode(item.mgn_mode or '')}\n"
        f"方向：{_format_history_side(None, item.pos_side or item.direction)}\n"
        f"开仓均价：{_format_position_history_price(item.open_avg_price, item.inst_id, item.inst_type)}\n"
        f"平仓均价：{_format_position_history_price(item.close_avg_price, item.inst_id, item.inst_type)}\n"
        f"平仓数量：{_format_position_history_size(item, instruments)}\n"
        f"{incremental_close_line}"
        f"手续费：{_format_position_history_fee_cell(item, upl_usdt_prices)}\n"
        f"\u5df2\u5b9e\u73b0\u6536\u76ca?{_format_position_history_pnl(item.realized_pnl, item, with_sign=True, usdt_prices=upl_usdt_prices)}\n"
        f"结算盈亏：{_format_optional_decimal(item.settle_pnl)}"
    )

def _build_position_history_detail_text(
    item: OkxPositionHistoryItem,
    upl_usdt_prices: dict[str, Decimal],
    instruments: dict[str, Instrument],
    note: str = "",
) -> str:
    note_line = _format_position_note_detail(note)
    status_text = _position_history_auto_summary(item) or "完整平仓/最终快照"
    incremental_close_total = _position_history_incremental_close_total(item)
    incremental_close_line = ""
    if incremental_close_total is not None:
        incremental_item = OkxPositionHistoryItem(
            **{
                **item.__dict__,
                "close_size": incremental_close_total,
            }
        )
        incremental_close_line = f"本次增量平仓：{_format_position_history_size(incremental_item, instruments)}\n"
    return (
        f"更新时间：{_format_okx_ms_timestamp(item.update_time)}\n"
        f"合约：{item.inst_id or '-'}\n"
        f"类型：{item.inst_type or '-'}\n"
        f"{note_line}"
        f"状态：{status_text}\n"
        f"保证金模式：{_format_margin_mode(item.mgn_mode or '')}\n"
        f"持仓模式：{_format_history_side(None, item.pos_side or item.direction)}\n"
        f"交易方向：{_format_position_history_trade_side(item)}\n"
        f"开仓均价：{_format_position_history_price(item.open_avg_price, item.inst_id, item.inst_type)}\n"
        f"平仓均价：{_format_position_history_price(item.close_avg_price, item.inst_id, item.inst_type)}\n"
        f"平仓数量：{_format_position_history_size(item, instruments)}\n"
        f"{incremental_close_line}"
        f"手续费：{_format_position_history_fee_cell(item, upl_usdt_prices)}\n"
        f"\u5df2\u5b9e\u73b0\u6536\u76ca?{_format_position_history_pnl(item.realized_pnl, item, with_sign=True, usdt_prices=upl_usdt_prices)}\n"
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
            session.status = "已停止"
            session.runtime_status = "已停止"
            if session.stopped_at is None:
                session.stopped_at = datetime.now()
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
        self._update_session_runtime_heartbeat_state(session)
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
