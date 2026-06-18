from __future__ import annotations

import json
import math
import threading
import time
import uuid
from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from tkinter import BOTH, END, LEFT, RIGHT, BooleanVar, Button, Canvas, PanedWindow, StringVar, Text, Toplevel, X, Y
from tkinter import filedialog, messagebox, ttk

from okx_quant.backtest import (
    ATR_BATCH_MULTIPLIERS,
    ATR_BATCH_TAKE_RATIOS,
    BATCH_MAX_ENTRIES_OPTIONS,
    BacktestManualPosition,
    BacktestReport,
    BacktestResult,
    BacktestTrade,
    MAX_BACKTEST_CANDLES,
    _backtest_uses_daily_filter,
    _backtest_uses_mtf_filter,
    _load_backtest_candles,
    _load_daily_filter_candles,
    _required_backtest_preload_candles,
    _required_mtf_filter_preload_candles,
    build_parameter_batch_configs,
    format_backtest_report,
    format_trade_exit_reason,
    is_stop_exit_reason,
    run_backtest,
    run_backtest_batch,
)
from okx_quant.backtest_audit import describe_backtest_export_artifacts, single_backtest_artifact_paths
from okx_quant.backtest_export import (
    build_backtest_focus_lines,
    export_batch_backtest_report,
    export_single_backtest_report,
)
from okx_quant.backtest_strategy_pool import is_strategy_pool_config, strategy_pool_profile_name
from okx_quant.candle_continuity import bar_step_ms, find_candle_gaps_half_open_range, find_candle_gaps_in_window
from okx_quant.candle_store import get_candle_count, get_candle_time_bounds
from okx_quant.candle_cache_verify import CacheVerifyOutcome, verify_and_repair_cached_candles
from okx_quant.models import (
    DynamicProtectionRule,
    Instrument,
    StrategyConfig,
    build_legacy_dynamic_protection_rules,
    describe_dynamic_protection_rule_overlap_warnings,
    describe_dynamic_protection_rules,
    dynamic_protection_rules_to_payload,
    merge_dynamic_protection_rules,
    moving_average_display_label,
    normalize_dynamic_protection_rules,
)
from okx_quant.minimum_risk_recommendations import (
    format_risk_recommendation,
    recommended_minimum_risk_amount_for_config,
    recommended_minimum_risk_amount_for_strategy,
)
from okx_quant.okx_client import OkxRestClient
from okx_quant.persistence import backtest_history_file_path, load_strategy_parameter_drafts, save_strategy_parameter_drafts
from okx_quant.pricing import format_decimal, format_decimal_fixed
from okx_quant.strategy_profiles import StrategyProfile, read_strategy_bundle
from okx_quant.strategy_catalog import (
    ALL_STRATEGY_DEFINITIONS,
    BACKTEST_STRATEGY_DEFINITIONS,
    STRATEGY_BTC_EMA55_SLOPE_SHORT_ID,
    STRATEGY_BODY_RETEST_SHORT_ID,
    STRATEGY_DYNAMIC_ID,
    STRATEGY_EMA55_SLOPE_SHORT_ID,
    StrategyDefinition,
    get_strategy_definition,
    resolve_dynamic_signal_mode,
)
from okx_quant.strategy_runtime_registry import (
    get_strategy_runtime_profile,
    strategy_entry_reference_caption,
    strategy_entry_reference_period_caption,
    strategy_is_cross_family,
    strategy_uses_dynamic_orders,
)
from okx_quant.strategy_symbol_defaults import get_strategy_symbol_parameter_defaults
from okx_quant.strategy_parameters import (
    iter_strategy_parameter_keys,
    strategy_fixed_value,
    strategy_is_parameter_editable,
    strategy_uses_parameter,
)
from okx_quant.strategy_ui_schema import (
    build_strategy_widget_visibility,
    strategy_parameter_default_for_scope,
    strategy_supports_dynamic_take_profit,
    strategy_ui_extra_defaults,
    strategy_ui_fixed_extra_value,
)
from okx_quant.window_layout import apply_adaptive_window_geometry


SIGNAL_LABEL_TO_VALUE = {
    "双向": "both",
    "只做多": "long_only",
    "只做空": "short_only",
}
def _strategy_fast_line_caption(strategy_id: str) -> str:
    if strategy_id in {STRATEGY_BTC_EMA55_SLOPE_SHORT_ID, STRATEGY_EMA55_SLOPE_SHORT_ID}:
        return "信号均线（斜率开平仓）"
    return "快线均线"


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
TRIGGER_TYPE_OPTIONS = {
    "标记价格 mark": "mark",
    "最新成交价 last": "last",
    "指数价格 index": "index",
}
BACKTEST_BAR_LABEL_TO_VALUE = {
    "5分钟": "5m",
    "15分钟": "15m",
    "1小时": "1H",
    "4小时": "4H",
}
BACKTEST_BAR_VALUE_TO_LABEL = {value: label for label, value in BACKTEST_BAR_LABEL_TO_VALUE.items()}
DEFAULT_BACKTEST_BAR_LABEL = "15分钟"
STRATEGY_ID_TO_NAME = {item.strategy_id: item.name for item in ALL_STRATEGY_DEFINITIONS}
SIGNAL_VALUE_TO_LABEL = {value: label for label, value in SIGNAL_LABEL_TO_VALUE.items()}
BACKTEST_SYMBOL_OPTIONS = (
    "BTC-USDT-SWAP",
    "ETH-USDT-SWAP",
    "SOL-USDT-SWAP",
    "BNB-USDT-SWAP",
    "DOGE-USDT-SWAP",
)
BACKTEST_HISTORY_SYNC_BARS = ("5m", "15m", "1H", "4H")
DEFAULT_HISTORY_SYNC_BAR_FLAGS = {
    "5m": False,
    "15m": False,
    "1H": True,
    "4H": True,
}
DEFAULT_MAKER_FEE_PERCENT = "0.015"
DEFAULT_TAKER_FEE_PERCENT = "0.036"
CHART_ACTION_BUTTON_BG = "#d8f3dc"
CHART_ACTION_BUTTON_ACTIVE_BG = "#b7e4c7"
CHART_ACTION_BUTTON_FG = "#1b4332"
BACKTEST_SIZING_OPTIONS = {
    "固定风险金": "fixed_risk",
    "固定数量": "fixed_size",
    "风险百分比": "risk_percent",
}
BACKTEST_SIZING_VALUE_TO_LABEL = {value: label for label, value in BACKTEST_SIZING_OPTIONS.items()}
TAKE_PROFIT_MODE_OPTIONS = {
    "固定止盈": "fixed",
    "动态止盈": "dynamic",
}
MTF_REVERSAL_MODE_OPTIONS = {
    "只过滤新开仓": "block_new_entries",
}
MTF_REVERSAL_MODE_VALUE_TO_LABEL = {value: label for label, value in MTF_REVERSAL_MODE_OPTIONS.items()}
MANUAL_NEAR_BREAK_EVEN_THRESHOLD_PCT = Decimal("0.50")
DEFAULT_BACKTEST_CHART_VISIBLE_CANDLES = 900
BACKTEST_CHART_FAST_RENDER_CANDLES = 800
BACKTEST_CHART_FULL_RENDER_CANDLES = 2200
TAKE_PROFIT_MODE_VALUE_TO_LABEL = {value: label for label, value in TAKE_PROFIT_MODE_OPTIONS.items()}
MOVING_AVERAGE_TYPE_OPTIONS = ("EMA", "MA")
DAILY_FILTER_BOUNDARY_LABEL_TO_VALUE = {
    "交易所1D": "exchange",
    "北京时间0点": "bjt_00",
    "北京时间8点": "bjt_08",
}
DAILY_FILTER_BOUNDARY_VALUE_TO_LABEL = {
    value: label for label, value in DAILY_FILTER_BOUNDARY_LABEL_TO_VALUE.items()
}
DAILY_FILTER_MODE_LABEL_TO_VALUE = {
    "关闭": "disabled",
    "close vs MA/EMA": "close_vs_ma",
    "弱日规则": "weak_day",
}
DAILY_FILTER_MODE_VALUE_TO_LABEL = {value: label for label, value in DAILY_FILTER_MODE_LABEL_TO_VALUE.items()}
DAILY_FILTER_SCOPE_LABEL_TO_VALUE = {
    "多空都过滤": "both",
    "只过滤多头": "long_only",
    "只过滤空头": "short_only",
}
DAILY_FILTER_SCOPE_VALUE_TO_LABEL = {value: label for label, value in DAILY_FILTER_SCOPE_LABEL_TO_VALUE.items()}
MANUAL_FILTER_OPTIONS = {
    "全部": "all",
    "仅接近保本": "near_break_even",
    "仅亏损仓": "loss_only",
    "仅做多": "long_only",
    "仅做空": "short_only",
}
MANUAL_SORT_OPTIONS = {
    "方向+距保本": "direction_gap",
    "距保本最近": "break_even_gap",
    "入池最久": "oldest_handoff",
    "浮亏最大": "largest_loss",
    "风险值最大": "largest_risk",
}
MANUAL_DEFAULT_SORT_LABEL = "方向+距保本"


@dataclass(frozen=True)
class BacktestLaunchState:
    strategy_name: str
    symbol: str
    bar: str
    ema_type: str
    ema_period: str
    trend_ema_type: str
    trend_ema_period: str
    big_ema_period: str
    entry_reference_ema_type: str
    entry_reference_ema_period: str
    mtf_filter_bar: str
    mtf_filter_fast_ema_period: str
    mtf_filter_slow_ema_period: str
    mtf_reversal_mode_label: str
    atr_period: str
    stop_atr: str
    take_atr: str
    risk_amount: str
    take_profit_mode_label: str
    max_entries_per_trend: str
    reentry_confirmation_enabled: bool
    reentry_confirmation_min_sequence: str
    reentry_confirmation_ma_type: str
    reentry_confirmation_ma_period: str
    dynamic_two_r_break_even: bool
    dynamic_break_even_trigger_r: str
    dynamic_fee_offset_enabled: bool
    time_stop_break_even_enabled: bool
    time_stop_break_even_bars: str
    signal_mode_label: str
    trade_mode_label: str
    position_mode_label: str
    trigger_type_label: str
    environment_label: str
    trend_ema_close_exit_after_trigger_r_enabled: bool = False
    trend_ema_close_exit_after_trigger_r: str = "5"
    hold_close_exit_bars: str = "0"
    trend_ema_slope_filter_min_ratio: str = "0"
    atr_percentile_filter_max: str = "0"
    body_retest_breakdown_atr_multiplier: str = "0.2"
    body_retest_retest_atr_multiplier: str = "0.3"
    body_retest_stop_buffer_atr_multiplier: str = "0.3"
    body_retest_body_atr_limit: str = "1.0"
    body_retest_watch_bars: str = "6"
    ema55_slope_exit_enabled: bool = True
    ema55_slope_lock_profit_enabled: bool = True
    ema55_slope_lock_profit_trigger_r: str = "5"
    dynamic_first_lock_r: str = "0"
    dynamic_trailing_step_r: str = "1"
    ema55_slope_negative_entry_bars: str = "1"
    maker_fee_percent: str = DEFAULT_MAKER_FEE_PERCENT
    taker_fee_percent: str = DEFAULT_TAKER_FEE_PERCENT
    initial_capital: str = "10000"
    sizing_mode_label: str = "固定风险金"
    risk_percent: str = "1"
    compounding_enabled: bool = False
    entry_slippage_percent: str = "0"
    exit_slippage_percent: str = "0"
    funding_rate_percent: str = "0"
    start_time_text: str = ""
    end_time_text: str = ""
    candle_limit: str = "10000"
    daily_filter_enabled: bool = False
    daily_filter_boundary_label: str = "交易所1D"
    daily_filter_mode_label: str = "关闭"
    daily_filter_scope_label: str = "多空都过滤"
    daily_filter_ma_type: str = "EMA"
    daily_filter_period: str = "5"
    backtest_profile_id: str = ""
    backtest_profile_name: str = ""
    backtest_profile_summary: str = ""
    dynamic_protection_rules_json: str = ""


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


@dataclass
class _ChartViewport:
    start_index: int = 0
    visible_count: int | None = None
    pan_anchor_x: int | None = None
    pan_anchor_start: int = 0


@dataclass(frozen=True)
class _ChartRenderState:
    left: int
    right: int
    top: int
    bottom: int
    price_bottom: int
    net_top: int
    net_bottom: int
    drawdown_top: int
    drawdown_bottom: int
    width: int
    height: int
    start_index: int
    end_index: int
    candle_step: float


@dataclass(frozen=True)
class _BacktestSnapshot:
    snapshot_id: str
    created_at: datetime
    config: StrategyConfig
    candle_limit: int
    candle_count: int
    report: BacktestReport
    report_text: str
    start_ts: int | None = None
    end_ts: int | None = None
    result: BacktestResult | None = None
    maker_fee_rate: Decimal = Decimal("0")
    taker_fee_rate: Decimal = Decimal("0")
    export_path: str | None = None
    runtime_id: str | None = None
    archive_id: str | None = None


class _BacktestSnapshotStore:
    def __init__(self) -> None:
        self._snapshots: dict[str, _BacktestSnapshot] = {}
        self._order: list[str] = []
        self._sequence = 0
        self._listeners: dict[int, callable] = {}
        self._listener_sequence = 0
        self._load_from_disk()

    def add_snapshot(
        self,
        result: BacktestResult,
        config: StrategyConfig,
        candle_limit: int,
        *,
        runtime_snapshot_id: str | None = None,
        export_path: str | None = None,
    ) -> _BacktestSnapshot:
        self._sequence += 1
        snapshot = _BacktestSnapshot(
            snapshot_id=f"S{self._sequence:03d}",
            created_at=datetime.now(),
            config=config,
            candle_limit=candle_limit,
            candle_count=len(result.candles),
            start_ts=result.candles[0].ts if result.candles else None,
            end_ts=result.candles[-1].ts if result.candles else None,
            report=result.report,
            report_text=format_backtest_report(result),
            result=None,
            maker_fee_rate=result.maker_fee_rate,
            taker_fee_rate=result.taker_fee_rate,
            export_path=export_path,
            runtime_id=runtime_snapshot_id,
            archive_id=f"S{self._sequence:03d}",
        )
        self._snapshots[snapshot.snapshot_id] = snapshot
        self._order.append(snapshot.snapshot_id)
        self._save_to_disk()
        self._notify()
        return snapshot

    def list_snapshots(self) -> list[_BacktestSnapshot]:
        return [self._snapshots[snapshot_id] for snapshot_id in self._order]

    def get_snapshot(self, snapshot_id: str) -> _BacktestSnapshot | None:
        return self._snapshots.get(snapshot_id)

    def clear(self) -> None:
        self._snapshots.clear()
        self._order.clear()
        self._save_to_disk()
        self._notify()

    def subscribe(self, callback) -> int:
        self._listener_sequence += 1
        token = self._listener_sequence
        self._listeners[token] = callback
        return token

    def unsubscribe(self, token: int | None) -> None:
        if token is None:
            return
        self._listeners.pop(token, None)

    def _notify(self) -> None:
        for callback in list(self._listeners.values()):
            callback()

    def _load_from_disk(self) -> None:
        path = backtest_history_file_path()
        if not path.exists():
            return
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return
        records = payload.get("records") if isinstance(payload, dict) else None
        if not isinstance(records, list):
            return
        snapshots: list[_BacktestSnapshot] = []
        for item in records:
            snapshot = _deserialize_backtest_snapshot(item)
            if snapshot is not None:
                snapshots.append(snapshot)
        snapshots.sort(key=lambda item: item.created_at)
        self._snapshots = {snapshot.snapshot_id: snapshot for snapshot in snapshots}
        self._order = [snapshot.snapshot_id for snapshot in snapshots]
        self._sequence = max((self._extract_sequence(snapshot.snapshot_id) for snapshot in snapshots), default=0)

    def _save_to_disk(self) -> None:
        path = backtest_history_file_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        # 流式写入避免在 32 位 Python 里为整个历史文件额外构造一份超大 JSON 字符串。
        # 固定名 .tmp 多实例会互踩；replace 在 Windows 上遇杀软/占用易 PermissionError，故用随机临时名 + 短重试。
        temp_path = path.with_name(f"{path.stem}.{uuid.uuid4().hex}.tmp")
        try:
            with temp_path.open("w", encoding="utf-8", newline="\n") as handle:
                handle.write('{"records":[')
                for index, snapshot_id in enumerate(self._order):
                    if index > 0:
                        handle.write(",")
                    json.dump(
                        _serialize_backtest_snapshot(self._snapshots[snapshot_id]),
                        handle,
                        ensure_ascii=False,
                        separators=(",", ":"),
                    )
                handle.write('],"updated_at":')
                json.dump(datetime.now().isoformat(timespec="seconds"), handle, ensure_ascii=False)
                handle.write("}\n")
            last_err: PermissionError | None = None
            for attempt in range(8):
                try:
                    temp_path.replace(path)
                    return
                except PermissionError as exc:
                    last_err = exc
                    time.sleep(0.05 * (attempt + 1))
            if last_err is not None:
                raise last_err
        finally:
            if temp_path.exists():
                try:
                    temp_path.unlink()
                except OSError:
                    pass

    @staticmethod
    def _extract_sequence(snapshot_id: str) -> int:
        digits = "".join(ch for ch in snapshot_id if ch.isdigit())
        return int(digits) if digits else 0


_BACKTEST_SNAPSHOT_STORE: _BacktestSnapshotStore | None = None


def get_backtest_snapshot_store() -> _BacktestSnapshotStore:
    global _BACKTEST_SNAPSHOT_STORE
    if _BACKTEST_SNAPSHOT_STORE is None:
        _BACKTEST_SNAPSHOT_STORE = _BacktestSnapshotStore()
    return _BACKTEST_SNAPSHOT_STORE


def _runtime_snapshot_id(snapshot: _BacktestSnapshot) -> str | None:
    if snapshot.runtime_id:
        return snapshot.runtime_id
    if snapshot.snapshot_id.startswith("R"):
        return snapshot.snapshot_id
    return None


def _archive_snapshot_id(snapshot: _BacktestSnapshot) -> str | None:
    if snapshot.archive_id:
        return snapshot.archive_id
    if snapshot.snapshot_id.startswith("S"):
        return snapshot.snapshot_id
    return None


def _build_backtest_identity_parts(snapshot: _BacktestSnapshot, *, prefer_archive: bool) -> list[str]:
    runtime_id = _runtime_snapshot_id(snapshot)
    archive_id = _archive_snapshot_id(snapshot)
    if prefer_archive:
        parts: list[str] = []
        if archive_id:
            parts.append(f"归档编号：{archive_id}")
        else:
            parts.append(f"归档编号：{snapshot.snapshot_id}")
        if runtime_id:
            parts.append(f"当前会话编号：{runtime_id}")
        return parts

    parts = [f"运行编号：{runtime_id or snapshot.snapshot_id}"]
    if archive_id:
        parts.append(f"已归档：{archive_id}")
    return parts


def _normalize_backtest_bar_label(value: str) -> str:
    normalized = value.strip()
    if normalized in BACKTEST_BAR_LABEL_TO_VALUE:
        return normalized
    if normalized in BACKTEST_BAR_VALUE_TO_LABEL:
        return BACKTEST_BAR_VALUE_TO_LABEL[normalized]
    return DEFAULT_BACKTEST_BAR_LABEL


def _backtest_bar_value_from_label(label: str) -> str:
    return BACKTEST_BAR_LABEL_TO_VALUE[_normalize_backtest_bar_label(label)]


def _parse_positive_decimal_hint(raw: str) -> Decimal | None:
    cleaned = str(raw or "").strip()
    if not cleaned:
        return None
    try:
        value = Decimal(cleaned)
    except InvalidOperation:
        return None
    if value <= 0:
        return None
    return value


def _backtest_instrument_contract_value_snapshot(instrument: Instrument) -> tuple[Decimal | None, str | None]:
    ct_val = instrument.ct_val if instrument.ct_val is not None and instrument.ct_val > 0 else None
    if ct_val is None:
        return None, None
    ct_mult = instrument.ct_mult if instrument.ct_mult is not None and instrument.ct_mult > 0 else Decimal("1")
    contract_ccy = (instrument.ct_val_ccy or "").strip().upper()
    if not contract_ccy and "-" in instrument.inst_id:
        contract_ccy = instrument.inst_id.split("-", 1)[0]
    return ct_val * ct_mult, contract_ccy or None


def _format_backtest_contract_size_with_equivalent(instrument: Instrument, size: Decimal) -> str:
    contract_value, contract_ccy = _backtest_instrument_contract_value_snapshot(instrument)
    if contract_value is not None and contract_value > 0 and contract_ccy:
        amount = size * contract_value
        return f"{format_decimal(size)}张（折合{format_decimal(amount)} {contract_ccy}）"
    return format_decimal(size)


def _build_backtest_minimum_order_hint_text(
    *,
    inst_id: str,
    strategy_id: str,
    signal_mode: str,
    instrument: Instrument | None,
    sizing_mode_label: str,
    size_or_risk_raw: str,
) -> str:
    normalized_inst_id = inst_id.strip().upper()
    if not normalized_inst_id:
        return "回测参考：请先选择标的。"
    recommendation = recommended_minimum_risk_amount_for_strategy(strategy_id, normalized_inst_id, signal_mode)
    if recommendation is None:
        return f"回测参考：{normalized_inst_id} 暂无推荐值。"
    return f"回测参考：{normalized_inst_id} {format_risk_recommendation(recommendation)}。"


def _required_minimum_risk_amount_for_trade(trade: BacktestTrade, min_size: Decimal) -> Decimal | None:
    trade_size = abs(trade.size)
    if trade_size <= 0 or min_size <= 0:
        return None
    return (trade.risk_value / trade_size) * min_size


def _percentile_decimal(values: list[Decimal], percentile: float) -> Decimal:
    if not values:
        return Decimal("0")
    ordered = sorted(values)
    index = max(0, min(int(len(ordered) * percentile) - 1, len(ordered) - 1))
    return ordered[index]


def _build_backtest_minimum_order_sample_summary(result: BacktestResult, config: StrategyConfig) -> str:
    recommendation = recommended_minimum_risk_amount_for_config(config)
    if recommendation is not None:
        return f"历史推荐：{config.inst_id} {format_risk_recommendation(recommendation)}。"
    if not result.trades or result.instrument.min_size <= 0:
        return ""
    required_risk_amounts = [
        amount
        for amount in (_required_minimum_risk_amount_for_trade(trade, result.instrument.min_size) for trade in result.trades)
        if amount is not None and amount > 0
    ]
    if not required_risk_amounts:
        return ""
    return f"历史推荐：{config.inst_id} 样本内最低风险金 P95={format_decimal(_percentile_decimal(required_risk_amounts, 0.95))}U。"


def _reverse_lookup_label(mapping: dict[str, str], value: str, default: str) -> str:
    normalized = str(value or "").strip()
    for label, candidate in mapping.items():
        if candidate == normalized:
            return label
    return default


def _format_backtest_candle_limit(candle_limit: int) -> str:
    return "全量" if candle_limit <= 0 else str(candle_limit)


def _format_local_cache_range_text(bounds: tuple[int, int] | None) -> str:
    if bounds is None:
        return "暂无缓存"
    start_ts, end_ts = bounds
    return f"{_format_chart_timestamp(start_ts)} -> {_format_chart_timestamp(end_ts)}"


def _format_gap_window(start_ts: int, end_exclusive_ts: int, step_ms: int) -> str:
    end_ts = max(start_ts, end_exclusive_ts - step_ms)
    return f"{_format_chart_timestamp(start_ts)} -> {_format_chart_timestamp(end_ts)}"


def _first_gap_summary(gaps: list[tuple[int, int, int]], step_ms: int) -> str:
    if not gaps:
        return ""
    gap_start, gap_end_exclusive, missing_count = gaps[0]
    return f"{_format_gap_window(gap_start, gap_end_exclusive, step_ms)}（缺 {missing_count} 根）"


def _build_backtest_symbol_options(current_symbol: str) -> tuple[str, ...]:
    normalized = current_symbol.strip().upper()
    if normalized and normalized not in BACKTEST_SYMBOL_OPTIONS:
        return (normalized,) + BACKTEST_SYMBOL_OPTIONS
    return BACKTEST_SYMBOL_OPTIONS


def _strategy_display_name(config: StrategyConfig) -> str:
    base_name = STRATEGY_ID_TO_NAME.get(config.strategy_id, config.strategy_id)
    if config.backtest_profile_name:
        return f"{base_name} / {config.backtest_profile_name}"
    return base_name


def _extract_report_line_value(report_text: str, *prefixes: str) -> str | None:
    for raw_line in report_text.splitlines():
        line = raw_line.strip()
        for prefix in prefixes:
            if line.startswith(prefix):
                value = line[len(prefix) :].strip()
                return value or None
    return None


def _backtest_snapshot_range_text(snapshot: _BacktestSnapshot) -> tuple[str, str]:
    start_text = (
        _format_chart_timestamp(snapshot.start_ts)
        if snapshot.start_ts is not None
        else _extract_report_line_value(
            snapshot.report_text,
            "\u5f00\u59cb\u65f6\u95f4\uff1a",
            "寮€濮嬫椂闂达細",
        )
        or "-"
    )
    end_text = (
        _format_chart_timestamp(snapshot.end_ts)
        if snapshot.end_ts is not None
        else _extract_report_line_value(
            snapshot.report_text,
            "\u7ed3\u675f\u65f6\u95f4\uff1a",
            "缁撴潫鏃堕棿锛歿",
        )
        or "-"
    )
    return start_text, end_text


def _build_backtest_candle_scope_text(snapshot: _BacktestSnapshot) -> str:
    candle_count_text = f"{snapshot.candle_count:,}根"
    if snapshot.candle_limit <= 0:
        return f"{candle_count_text}（全量）"
    if snapshot.candle_count == snapshot.candle_limit:
        return candle_count_text
    return f"{candle_count_text}（上限 {snapshot.candle_limit:,}）"


def _build_backtest_period_summary(snapshot: _BacktestSnapshot) -> str:
    start_text, end_text = _backtest_snapshot_range_text(snapshot)
    return (
        f"{start_text} -> {end_text} | "
        f"{_normalize_backtest_bar_label(snapshot.config.bar)} | "
        f"{_build_backtest_candle_scope_text(snapshot)}"
    )


def _build_backtest_header_summary(snapshot: _BacktestSnapshot) -> str:
    report = snapshot.report
    start_text, end_text = _backtest_snapshot_range_text(snapshot)
    signal_label = SIGNAL_VALUE_TO_LABEL.get(snapshot.config.signal_mode, snapshot.config.signal_mode)
    identity_text = " | ".join(_build_backtest_identity_parts(snapshot, prefer_archive=False))
    return (
        f"{identity_text} | 时间：{snapshot.created_at.strftime('%Y-%m-%d %H:%M:%S')} | "
        f"策略：{_strategy_display_name(snapshot.config)} | 交易对：{snapshot.config.inst_id} | "
        f"区间：{start_text} -> {end_text} | K线：{_normalize_backtest_bar_label(snapshot.config.bar)} | "
        f"样本：{_build_backtest_candle_scope_text(snapshot)} | 方向：{signal_label} | "
        f"交易次数：{report.total_trades} | 胜率：{format_decimal_fixed(report.win_rate, 2)}% | "
        f"总盈亏：{format_decimal_fixed(report.total_pnl, 4)} | "
        f"最大回撤：{format_decimal_fixed(report.max_drawdown, 4)}"
    )


def _backtest_snapshot_matches_keyword(snapshot: _BacktestSnapshot, keyword: str) -> bool:
    normalized = " ".join(str(keyword or "").strip().lower().split())
    if not normalized:
        return True
    tokens = normalized.split(" ")
    values = [
        snapshot.snapshot_id,
        _runtime_snapshot_id(snapshot) or "",
        _archive_snapshot_id(snapshot) or "",
        snapshot.created_at.strftime("%Y-%m-%d %H:%M:%S"),
        snapshot.config.strategy_id,
        _strategy_display_name(snapshot.config),
        snapshot.config.inst_id,
        *_build_backtest_compare_row(snapshot),
        *_build_backtest_compare_row(snapshot, prefer_archive=True),
    ]
    haystack = " ".join(str(value) for value in values if value).lower()
    return all(token in haystack for token in tokens)


def _filter_backtest_snapshots(snapshots: list[_BacktestSnapshot], keyword: str) -> list[_BacktestSnapshot]:
    return [snapshot for snapshot in snapshots if _backtest_snapshot_matches_keyword(snapshot, keyword)]


def _build_backtest_compare_row(snapshot: _BacktestSnapshot, *, prefer_archive: bool = False) -> tuple[str, ...]:
    report = snapshot.report
    config = snapshot.config
    if prefer_archive:
        display_id = _archive_snapshot_id(snapshot) or snapshot.snapshot_id
        return (
            display_id,
            snapshot.created_at.strftime("%m-%d %H:%M"),
            _strategy_display_name(config),
            config.inst_id,
            _build_backtest_period_summary(snapshot),
            _build_backtest_param_summary(
                config,
                maker_fee_rate=snapshot.maker_fee_rate,
                taker_fee_rate=snapshot.taker_fee_rate,
            ),
            str(report.total_trades),
            f"{format_decimal_fixed(report.win_rate, 2)}%",
            format_decimal_fixed(report.total_pnl, 4),
            format_decimal_fixed(report.max_drawdown, 4),
        )

    runtime_id = _runtime_snapshot_id(snapshot) or snapshot.snapshot_id
    archive_id = _archive_snapshot_id(snapshot) or "-"
    return (
        runtime_id,
        archive_id,
        snapshot.created_at.strftime("%m-%d %H:%M"),
        _strategy_display_name(config),
        config.inst_id,
        _build_backtest_period_summary(snapshot),
        _build_backtest_param_summary(
            config,
            maker_fee_rate=snapshot.maker_fee_rate,
            taker_fee_rate=snapshot.taker_fee_rate,
        ),
        str(report.total_trades),
        f"{format_decimal_fixed(report.win_rate, 2)}%",
        format_decimal_fixed(report.total_pnl, 4),
        format_decimal_fixed(report.max_drawdown, 4),
    )


def _configure_backtest_compare_tree(tree: ttk.Treeview, *, id_heading: str, include_archive_column: bool = False) -> None:
    if include_archive_column:
        columns = (
            ("id", id_heading, 92, "center", False),
            ("archive_id", "已归档", 92, "center", False),
            ("time", "回测时间", 112, "center", False),
            ("strategy", "策略", 150, "w", False),
            ("symbol", "交易对", 122, "center", False),
            ("period", "回测区间 / K线", 320, "w", True),
            ("params", "参数摘要", 520, "w", True),
            ("trades", "交易数", 68, "e", False),
            ("win_rate", "胜率", 80, "e", False),
            ("pnl", "总盈亏", 100, "e", False),
            ("drawdown", "最大回撤", 100, "e", False),
        )
    else:
        columns = (
            ("id", id_heading, 132, "center", False),
            ("time", "回测时间", 112, "center", False),
            ("strategy", "策略", 150, "w", False),
            ("symbol", "交易对", 122, "center", False),
            ("period", "回测区间 / K线", 320, "w", True),
            ("params", "参数摘要", 520, "w", True),
            ("trades", "交易数", 68, "e", False),
            ("win_rate", "胜率", 80, "e", False),
            ("pnl", "总盈亏", 100, "e", False),
            ("drawdown", "最大回撤", 100, "e", False),
        )
    for column_id, heading, width, anchor, stretch in columns:
        tree.heading(column_id, text=heading)
        tree.column(column_id, width=width, anchor=anchor, stretch=stretch)


def _btc_ema55_slope_exit_summary(config: StrategyConfig) -> str:
    trigger_r = max(int(config.ema55_slope_lock_profit_trigger_r), 2)
    slope_exit = "开" if config.ema55_slope_exit_enabled else "关"
    lock_profit = "nR保本开启(含双向手续费)" if config.ema55_slope_lock_profit_enabled else "nR保本关闭"
    return f"斜率转正平仓={slope_exit} / 首档触发R={trigger_r}R / {lock_profit}"


def _btc_ema55_slope_reentry_summary(config: StrategyConfig) -> str:
    parts: list[str] = []
    if config.ema55_slope_same_bar_reentry_block:
        parts.append("同K线禁重开")
    if config.ema55_slope_dynamic_exit_requires_bear_reentry:
        parts.append("动态保护后等新阴线")
    if config.ema55_slope_dynamic_exit_bear_reentry_break_prev_low:
        parts.append("新阴线需破前低")
    if config.ema55_slope_dynamic_exit_requires_ema_reclaim:
        parts.append("先回抽EMA再跌回")
    if config.ema55_slope_locked_reentry_requires_ema21_near:
        parts.append("锁盈后需接近EMA21")
    if config.ema55_slope_dynamic_exit_bull_bar_requires_bear_reentry:
        parts.append("阳线保护后等新阴线")
    if not parts:
        return "默认"
    return "+".join(parts)


def _backtest_exit_mode_label(config: StrategyConfig) -> str:
    if config.strategy_id == STRATEGY_BTC_EMA55_SLOPE_SHORT_ID:
        return _btc_ema55_slope_exit_summary(config)
    if config.strategy_id in {STRATEGY_EMA55_SLOPE_SHORT_ID, STRATEGY_BODY_RETEST_SHORT_ID} and config.take_profit_mode == "dynamic":
        trigger_r = max(int(config.ema55_slope_lock_profit_trigger_r), 2)
        slope_exit = "开" if config.ema55_slope_exit_enabled else "关"
        return f"动态止盈 / 斜率转正平仓={slope_exit} / 首档触发R={trigger_r}R"
    if config.strategy_id in {STRATEGY_EMA55_SLOPE_SHORT_ID, STRATEGY_BODY_RETEST_SHORT_ID}:
        slope_exit = "开" if config.ema55_slope_exit_enabled else "关"
        return f"固定止盈 / 斜率转正平仓={slope_exit}"
    return "动态止盈" if config.take_profit_mode == "dynamic" else "固定止盈"


def _uses_dynamic_break_even_trigger_r(config: StrategyConfig) -> bool:
    return config.take_profit_mode == "dynamic" and strategy_uses_parameter(
        config.strategy_id,
        "dynamic_break_even_trigger_r",
    )


def _dynamic_protection_summary_parts(config: StrategyConfig) -> tuple[str, ...]:
    rules = config.resolved_dynamic_protection_rules()
    extra_parts: list[str] = []
    if bool(config.trend_ema_close_exit_after_trigger_r_enabled):
        extra_parts.append(f"{config.resolved_trend_ema_close_exit_after_trigger_r()}R破趋势EMA平仓")
    if rules:
        return tuple(
            describe_dynamic_protection_rules(
                rules,
                fee_offset_enabled=bool(config.dynamic_fee_offset_enabled),
            )
        ) + tuple(extra_parts)
    if _uses_dynamic_break_even_trigger_r(config):
        first_lock_r = max(int(config.dynamic_first_lock_r), 0)
        trailing_start_r = max(int(config.ema55_slope_lock_profit_trigger_r), 2)
        trailing_step_r = max(int(config.dynamic_trailing_step_r), 1)
        effective_first_lock_r = first_lock_r if first_lock_r > 0 else max(trailing_start_r - trailing_step_r, 0)
        return (
            f"{max(int(config.dynamic_break_even_trigger_r), 1)}R保本",
            f"nR保本{config.dynamic_two_r_break_even_label()}",
            f"{trailing_start_r}R锁{effective_first_lock_r}R后每{trailing_step_r}R移{trailing_step_r}R",
        ) + tuple(extra_parts)
    return (f"2R保本{config.dynamic_two_r_break_even_label()}",) + tuple(extra_parts)


def _dynamic_protection_metric_parts(config: StrategyConfig) -> tuple[str, ...]:
    rules = config.resolved_dynamic_protection_rules()
    if rules:
        parts = [
            "动态保护规则："
            + " / ".join(
                describe_dynamic_protection_rules(
                    rules,
                    fee_offset_enabled=bool(config.dynamic_fee_offset_enabled),
                )
            )
        ]
        if bool(config.trend_ema_close_exit_after_trigger_r_enabled):
            parts.append(
                f"趋势EMA离场：达到 {config.resolved_trend_ema_close_exit_after_trigger_r()}R 后，若收盘跌破趋势EMA则平仓"
            )
        return tuple(parts)
    if _uses_dynamic_break_even_trigger_r(config):
        return ("动态保护规则：" + " / ".join(_dynamic_protection_summary_parts(config)),)
    return (f"动态保护规则：2R保本：{config.dynamic_two_r_break_even_label()}",)


def _build_backtest_param_summary(
    config: StrategyConfig,
    *,
    maker_fee_rate: Decimal = Decimal("0"),
    taker_fee_rate: Decimal = Decimal("0"),
) -> str:
    fast_label = config.ema_label()
    trend_label = config.trend_ema_label()
    reference_label = config.entry_reference_line_label()
    profile = get_strategy_runtime_profile(config.strategy_id)

    if config.strategy_id == STRATEGY_BTC_EMA55_SLOPE_SHORT_ID:
        return (
            f"{fast_label}/{trend_label} / ATR{config.atr_period} / "
            f"SLx{format_decimal(config.atr_stop_multiplier)} / "
            f"开仓连续负斜率{max(int(config.ema55_slope_negative_entry_bars), 1)}根 / "
            f"平仓规则{_btc_ema55_slope_exit_summary(config)} / "
            f"再入场{_btc_ema55_slope_reentry_summary(config)}"
        )

    if config.strategy_id in {STRATEGY_EMA55_SLOPE_SHORT_ID, STRATEGY_BODY_RETEST_SHORT_ID}:
        extra_parts = [_backtest_exit_mode_label(config)]
        if config.take_profit_mode == "dynamic":
            extra_parts.extend(_dynamic_protection_summary_parts(config))
            if config.time_stop_break_even_enabled and config.resolved_time_stop_break_even_bars() > 0:
                extra_parts.append(f"时间保本{config.resolved_time_stop_break_even_bars()}根")
        else:
            extra_parts.append(f"TPx{format_decimal(config.atr_take_multiplier)}")
        extra_text = " / ".join(extra_parts)
        return (
            f"{fast_label}/{trend_label} / ATR{config.atr_period} / "
            f"SLx{format_decimal(config.atr_stop_multiplier)} / {extra_text}"
        )

    if profile.uses_dynamic_orders or strategy_is_cross_family(config.strategy_id):
        extra_parts = [_backtest_exit_mode_label(config)]
        if config.take_profit_mode == "dynamic":
            extra_parts.extend(_dynamic_protection_summary_parts(config))
            if config.time_stop_break_even_enabled and config.resolved_time_stop_break_even_bars() > 0:
                extra_parts.append(f"时间保本{config.resolved_time_stop_break_even_bars()}根")
        else:
            extra_parts.append(f"TPx{format_decimal(config.atr_take_multiplier)}")
        max_entries_text = "每波不限" if config.max_entries_per_trend <= 0 else f"每波{config.max_entries_per_trend}次"
        extra_parts.append(max_entries_text)
        if strategy_is_cross_family(config.strategy_id) and int(config.hold_close_exit_bars) > 0:
            extra_parts.append(f"{int(config.hold_close_exit_bars)}根收盘平仓")
        extra_text = " / ".join(extra_parts)
        ref_line_label = strategy_entry_reference_caption(config.strategy_id)
        return (
            f"{fast_label}/{trend_label} / ATR{config.atr_period} / "
            f"{ref_line_label}{reference_label} / "
            f"SLx{format_decimal(config.atr_stop_multiplier)} / {extra_text}"
        )

    if profile.family == "ema5_ema8":
        return (
            f"4H固定 / {fast_label}/{trend_label}/EMA{config.big_ema_period} / "
            f"{trend_label}动态止损 / 方向{SIGNAL_VALUE_TO_LABEL.get(config.signal_mode, config.signal_mode)}"
        )

    return (
        f"{fast_label}/{trend_label}/EMA{config.big_ema_period} / ATR{config.atr_period} / "
        f"SLx{format_decimal(config.atr_stop_multiplier)} / TPx{format_decimal(config.atr_take_multiplier)} / "
        f"方向{SIGNAL_VALUE_TO_LABEL.get(config.signal_mode, config.signal_mode)}"
    )
def _backtest_export_detail_lines(export_path: str | None) -> list[str]:
    if not export_path:
        return []
    try:
        return describe_backtest_export_artifacts(export_path)
    except Exception:
        return [f"报告文件：{export_path}"]


def _build_backtest_compare_detail(snapshot: _BacktestSnapshot, *, prefer_archive: bool = False) -> str:
    config = snapshot.config
    strategy_name = _strategy_display_name(config)
    start_text, end_text = _backtest_snapshot_range_text(snapshot)
    lines = [
        *_build_backtest_identity_parts(snapshot, prefer_archive=prefer_archive),
        f"回测时间：{snapshot.created_at.strftime('%Y-%m-%d %H:%M:%S')}",
        f"策略：{strategy_name}",
        f"交易对：{config.inst_id}",
        f"回测区间：{start_text} -> {end_text}",
        f"K线周期：{_normalize_backtest_bar_label(config.bar)}",
        f"K线根数：{_build_backtest_candle_scope_text(snapshot)}",
        f"方向{SIGNAL_VALUE_TO_LABEL.get(config.signal_mode, config.signal_mode)}",
        f"参数：{_build_backtest_param_summary(config, maker_fee_rate=snapshot.maker_fee_rate, taker_fee_rate=snapshot.taker_fee_rate)}",
    ]
    lines.extend(_backtest_export_detail_lines(snapshot.export_path))
    lines.extend([
        "",
        snapshot.report_text,
    ])
    return "\n".join(lines)


def _build_backtest_report_copy_text(snapshot: _BacktestSnapshot) -> str:
    if snapshot.result is None:
        return snapshot.report_text
    report_path = Path(snapshot.export_path) if snapshot.export_path else None
    artifact_paths = None
    if report_path is not None:
        try:
            artifact_paths = single_backtest_artifact_paths(report_path)
        except Exception:
            artifact_paths = None
    lines = [
        "交易员速览",
        "=" * 72,
        *build_backtest_focus_lines(
            snapshot.result,
            snapshot.config,
            snapshot.candle_limit,
            exported_at=snapshot.created_at,
            snapshot_id=snapshot.snapshot_id,
            report_path=report_path,
            artifact_paths=artifact_paths,
        ),
        "",
        "完整明细",
        "-" * 72,
        format_backtest_report(snapshot.result),
    ]
    return "\n".join(lines)


def _format_trade_exit_reason(exit_reason: str) -> str:
    return format_trade_exit_reason(exit_reason)


def _build_trade_tree_rows(result: BacktestResult) -> list[tuple[str, tuple[str | int, ...]]]:
    rows: list[tuple[str, tuple[str | int, ...]]] = []
    for index, trade in enumerate(result.trades, start=1):
        rows.append(
            (
                f"T{index:03d}",
                (
                    index,
                    "做多" if trade.signal == "long" else "做空",
                    _format_chart_timestamp(trade.entry_ts),
                    format_decimal_fixed(trade.entry_price, 4),
                    format_decimal_fixed(trade.stop_loss, 4),
                    format_decimal_fixed(trade.atr_value, 4),
                    format_decimal_fixed(trade.size, 4),
                    _format_chart_timestamp(trade.exit_ts),
                    format_decimal_fixed(trade.exit_price, 4),
                    format_decimal_fixed(trade.total_fee, 4),
                    _format_trade_exit_reason(trade.exit_reason),
                    format_decimal_fixed(trade.pnl, 4),
                    format_decimal_fixed(trade.r_multiple, 4),
                ),
            )
        )
    if result.open_position is not None:
        open_position = result.open_position
        rows.append(
            (
                "OPEN",
                (
                    "OPEN",
                    "做多" if open_position.signal in ("buy", "long") else "做空",
                    _format_chart_timestamp(open_position.entry_ts),
                    format_decimal_fixed(open_position.entry_price, 4),
                    format_decimal_fixed(open_position.stop_loss, 4),
                    "-",
                    format_decimal_fixed(open_position.size, 4),
                    _format_chart_timestamp(open_position.current_ts),
                    format_decimal_fixed(open_position.current_price, 4),
                    format_decimal_fixed(open_position.entry_fee, 4),
                    "未平仓快照",
                    format_decimal_fixed(open_position.pnl, 4),
                    format_decimal_fixed(open_position.r_multiple, 4),
                ),
            )
        )
    return rows


def _manual_position_break_even_gap_pct(manual_position: BacktestManualPosition) -> Decimal:
    gap_value = abs(manual_position.current_price - manual_position.break_even_price)
    base_price = abs(manual_position.break_even_price)
    if base_price <= 0:
        base_price = abs(manual_position.entry_price)
    if base_price <= 0:
        return Decimal("0")
    return (gap_value / base_price) * Decimal("100")


def _format_manual_gap_pct(gap_pct: Decimal) -> str:
    return f"{format_decimal_fixed(gap_pct, 2)}%"


def _manual_direction_order(signal: str) -> int:
    return 0 if signal == "long" else 1


def _manual_sort_description(sort_value: str) -> str:
    if sort_value == "break_even_gap":
        return "全池按距保本从近到远排序"
    if sort_value == "oldest_handoff":
        return "全池按入池时间从久到新排序"
    if sort_value == "largest_loss":
        return "全池按浮亏从大到小排序"
    if sort_value == "largest_risk":
        return "全池按风险值从大到小排序"
    return "同方向内按距保本从近到远排序"


def _sorted_manual_positions(
    manual_positions: list[BacktestManualPosition],
    sort_value: str = "direction_gap",
) -> list[BacktestManualPosition]:
    if sort_value == "break_even_gap":
        return sorted(
            manual_positions,
            key=lambda item: (
                _manual_position_break_even_gap_pct(item),
                _manual_direction_order(item.signal),
                item.handoff_ts,
                item.entry_ts,
            ),
        )
    if sort_value == "oldest_handoff":
        return sorted(
            manual_positions,
            key=lambda item: (
                item.handoff_ts,
                item.entry_ts,
                _manual_direction_order(item.signal),
                _manual_position_break_even_gap_pct(item),
            ),
        )
    if sort_value == "largest_loss":
        return sorted(
            manual_positions,
            key=lambda item: (
                item.pnl,
                _manual_position_break_even_gap_pct(item),
                -item.risk_value,
                item.handoff_ts,
                item.entry_ts,
            ),
        )
    if sort_value == "largest_risk":
        return sorted(
            manual_positions,
            key=lambda item: (
                -item.risk_value,
                item.pnl,
                _manual_position_break_even_gap_pct(item),
                item.handoff_ts,
                item.entry_ts,
            ),
        )
    return sorted(
        manual_positions,
        key=lambda item: (
            _manual_direction_order(item.signal),
            _manual_position_break_even_gap_pct(item),
            item.handoff_ts,
            item.entry_ts,
        ),
    )


def _manual_direction_breakdown_text(manual_positions: list[BacktestManualPosition]) -> str:
    direction_parts: list[str] = []
    for signal, label in (("long", "做多"), ("short", "做空")):
        positions = [item for item in manual_positions if item.signal == signal]
        if not positions:
            continue
        total_size = sum((item.size for item in positions), Decimal("0"))
        total_pnl = sum((item.pnl for item in positions), Decimal("0"))
        nearest_gap = min((_manual_position_break_even_gap_pct(item) for item in positions), default=Decimal("0"))
        direction_parts.append(
            f"{label} {len(positions)} 笔 / {format_decimal_fixed(total_size, 4)} / "
            f"浮盈亏 {format_decimal_fixed(total_pnl, 4)} / 最近保本 {format_decimal_fixed(nearest_gap, 2)}%"
        )
    return " | ".join(direction_parts) if direction_parts else "当前无待人工处理仓位。"


def _manual_row_tag(manual_position: BacktestManualPosition) -> str:
    near_break_even = _manual_position_break_even_gap_pct(manual_position) <= MANUAL_NEAR_BREAK_EVEN_THRESHOLD_PCT
    if manual_position.pnl > 0:
        return "manual_profit_near" if near_break_even else "manual_profit"
    if manual_position.pnl < 0:
        return "manual_loss_near" if near_break_even else "manual_loss"
    return "manual_flat_near" if near_break_even else "manual_flat"


def _manual_position_matches_filter(manual_position: BacktestManualPosition, filter_value: str) -> bool:
    if filter_value == "near_break_even":
        return _manual_position_break_even_gap_pct(manual_position) <= MANUAL_NEAR_BREAK_EVEN_THRESHOLD_PCT
    if filter_value == "loss_only":
        return manual_position.pnl < 0
    if filter_value == "long_only":
        return manual_position.signal == "long"
    if filter_value == "short_only":
        return manual_position.signal == "short"
    return True


def _filter_manual_positions(
    manual_positions: list[BacktestManualPosition],
    filter_value: str,
) -> list[BacktestManualPosition]:
    return [item for item in manual_positions if _manual_position_matches_filter(item, filter_value)]


def _manual_signed_gap_value(manual_position: BacktestManualPosition) -> Decimal:
    if manual_position.signal == "long":
        return manual_position.current_price - manual_position.break_even_price
    return manual_position.break_even_price - manual_position.current_price


def _format_signed_price_gap(value: Decimal) -> str:
    if value > 0:
        return f"+{format_decimal_fixed(value, 4)}"
    return format_decimal_fixed(value, 4)


def _format_manual_age(manual_position: BacktestManualPosition) -> str:
    raw_delta = max(manual_position.current_ts - manual_position.handoff_ts, 0)
    if manual_position.current_ts >= 10**12 or manual_position.handoff_ts >= 10**12:
        total_minutes = raw_delta // 60000
    else:
        total_minutes = raw_delta // 60
    days, remaining_minutes = divmod(int(total_minutes), 1440)
    hours, minutes = divmod(remaining_minutes, 60)
    if days > 0:
        return f"{days}d{hours:02d}h"
    if hours > 0:
        return f"{hours}h{minutes:02d}m"
    return f"{minutes}m"


def _manual_focus_window(
    manual_position: BacktestManualPosition,
    total_count: int,
    *,
    min_visible: int = 40,
    leading_padding: int = 12,
    trailing_padding: int = 18,
) -> tuple[int, int, int]:
    if total_count <= 0:
        return 0, 0, 0
    anchor_index = max(0, min(manual_position.handoff_index, total_count - 1))
    left_index = max(min(manual_position.entry_index, manual_position.handoff_index) - leading_padding, 0)
    right_index = min(max(manual_position.entry_index, manual_position.handoff_index) + trailing_padding, total_count - 1)
    visible_count = max(min_visible, right_index - left_index + 1)
    visible_count = min(visible_count, total_count)
    target_start = max(left_index, anchor_index - (visible_count // 2))
    start_index, visible_count = _normalize_chart_viewport(target_start, visible_count, total_count, min_visible=min_visible)
    return start_index, visible_count, anchor_index


def _has_extension_stats(result: BacktestResult | None) -> bool:
    if result is None:
        return False
    report = result.report
    return bool(
        result.manual_positions
        or report.manual_handoffs
        or report.manual_open_positions
        or report.manual_open_size != 0
        or report.manual_open_pnl != 0
        or report.max_manual_positions
        or report.max_total_occupied_slots
    )


def _build_open_position_summary(result: BacktestResult) -> str:
    open_position = result.open_position
    if open_position is None:
        return ""
    direction_text = "做多" if open_position.signal in ("buy", "long") else "做空"
    return (
        "期末未平仓："
        f"{direction_text} | 开仓={_format_chart_timestamp(open_position.entry_ts)} | "
        f"当前={format_decimal_fixed(open_position.current_price, 4)} | "
        f"浮盈亏={format_decimal_fixed(open_position.pnl, 4)} | "
        f"R={format_decimal_fixed(open_position.r_multiple, 4)}"
    )


def _build_manual_pool_summary(
    result: BacktestResult,
    config: StrategyConfig,
    *,
    visible_positions: list[BacktestManualPosition] | None = None,
    filter_label: str | None = None,
    sort_label: str | None = None,
) -> str:
    if not result.manual_positions:
        return "当前策略没有额外托管仓位统计。"

    report = result.report
    current_ts = result.manual_positions[0].current_ts if result.manual_positions else (result.candles[-1].ts if result.candles else None)
    current_time_text = _format_chart_timestamp(current_ts) if current_ts is not None else "-"
    slot_limit_text = (
        f"{report.max_total_occupied_slots}/{config.max_entries_per_trend}"
        if config.max_entries_per_trend > 0
        else str(report.max_total_occupied_slots)
    )
    total_entry_fee = sum((item.entry_fee for item in result.manual_positions), Decimal("0"))
    total_funding = sum((item.funding_cost for item in result.manual_positions), Decimal("0"))
    base_text = (
        f"当前时间：{current_time_text} | 托管仓位：{report.manual_open_positions} 笔 / "
        f"{format_decimal_fixed(report.manual_open_size, 4)} | 浮盈亏：{format_decimal_fixed(report.manual_open_pnl, 4)} | "
        f"累计转托管：{report.manual_handoffs} | 峰值托管仓位：{report.max_manual_positions} | "
        f"峰值占槽：{slot_limit_text} | 开仓手续费：{format_decimal_fixed(total_entry_fee, 4)} | "
        f"资金费：{format_decimal_fixed(total_funding, 4)}"
    )
    resolved_sort_label = sort_label if sort_label in MANUAL_SORT_OPTIONS else MANUAL_DEFAULT_SORT_LABEL
    sort_value = MANUAL_SORT_OPTIONS.get(resolved_sort_label, "direction_gap")
    base_text = f"{base_text} | 当前排序：{resolved_sort_label}"
    display_positions = result.manual_positions if visible_positions is None else visible_positions
    if filter_label and filter_label in MANUAL_FILTER_OPTIONS and filter_label != "全部":
        base_text = f"{base_text} | 当前筛选：{filter_label} ({len(display_positions)}/{len(result.manual_positions)})"
    if not result.manual_positions:
        return f"{base_text}\n当前无待人工处理仓位。"
    if not display_positions:
        return f"{base_text}\n当前筛选下暂无仓位。"

    sorted_positions = _sorted_manual_positions(display_positions, sort_value)
    nearest_position = min(display_positions, key=_manual_position_break_even_gap_pct)
    nearest_gap_text = _format_manual_gap_pct(_manual_position_break_even_gap_pct(nearest_position))
    return (
        f"{base_text}\n"
        f"方向分组：{_manual_direction_breakdown_text(sorted_positions)} | "
        f"最接近保本：{nearest_gap_text} | "
        f"{_manual_sort_description(sort_value)}，黄色底表示距保本 ≤ {_format_manual_gap_pct(MANUAL_NEAR_BREAK_EVEN_THRESHOLD_PCT)}"
    )


def _build_manual_position_row(index: int, manual_position: BacktestManualPosition) -> tuple[str, ...]:
    return (
        str(index),
        "做多" if manual_position.signal == "long" else "做空",
        _format_chart_timestamp(manual_position.entry_ts),
        _format_chart_timestamp(manual_position.handoff_ts),
        _format_manual_age(manual_position),
        format_decimal_fixed(manual_position.entry_price, 4),
        format_decimal_fixed(manual_position.handoff_price, 4),
        format_decimal_fixed(manual_position.current_price, 4),
        format_decimal_fixed(manual_position.break_even_price, 4),
        _format_signed_price_gap(_manual_signed_gap_value(manual_position)),
        _format_manual_gap_pct(_manual_position_break_even_gap_pct(manual_position)),
        format_decimal_fixed(manual_position.size, 4),
        format_decimal_fixed(manual_position.pnl, 4),
        format_decimal_fixed(manual_position.entry_fee, 4),
        format_decimal_fixed(manual_position.funding_cost, 4),
        manual_position.handoff_reason,
    )


def _format_fee_rate_percent(rate: Decimal) -> str:
    return f"{format_decimal_fixed(rate * Decimal('100'), 4)}%"


def _format_backtest_slippage_summary(config: StrategyConfig) -> str:
    return (
        f"开滑{_format_fee_rate_percent(config.resolved_backtest_entry_slippage_rate())} / "
        f"平滑{_format_fee_rate_percent(config.resolved_backtest_exit_slippage_rate())}"
    )


def _batch_entries_label(value: int) -> str:
    return "不限(0)" if value <= 0 else f"{value}次"


def _batch_entries_value_from_label(label: str) -> int:
    if label.startswith("不限"):
        return 0
    digits = "".join(ch for ch in label if ch.isdigit())
    return int(digits) if digits else 0


def _batch_mode_for_snapshots(snapshots: list[_BacktestSnapshot]) -> str:
    if not snapshots:
        return "none"
    config = snapshots[0].config
    if is_strategy_pool_config(config):
        return "strategy_pool"
    if get_strategy_runtime_profile(config.strategy_id).family == "ema55_slope_short":
        return "atr_period_matrix"
    if strategy_uses_dynamic_orders(config.strategy_id):
        if config.take_profit_mode == "dynamic":
            return "dynamic_entries"
        return "fixed_entries"
    return "atr_matrix"


def _batch_entry_levels(snapshots: list[_BacktestSnapshot]) -> list[int]:
    levels = sorted({snapshot.config.max_entries_per_trend for snapshot in snapshots})
    if levels:
        return levels
    return list(BATCH_MAX_ENTRIES_OPTIONS)


def _snapshot_sort_key(snapshot: _BacktestSnapshot, batch_mode: str) -> tuple[object, ...]:
    config = snapshot.config
    if batch_mode == "strategy_pool":
        return (
            config.backtest_profile_id or config.backtest_profile_name,
            config.ema_period,
            config.trend_ema_period,
            config.atr_period,
            config.atr_stop_multiplier,
            config.atr_take_multiplier,
        )
    if batch_mode == "dynamic_entries":
        return (config.atr_stop_multiplier, config.max_entries_per_trend)
    if batch_mode == "fixed_entries":
        return (
            config.max_entries_per_trend,
            config.atr_stop_multiplier,
            config.atr_take_multiplier,
        )
    if batch_mode == "atr_period_matrix":
        return (config.atr_stop_multiplier, config.atr_period)
    return (config.atr_stop_multiplier, config.atr_take_multiplier)


def _serialize_strategy_config(config: StrategyConfig) -> dict[str, object]:
    return {
        "inst_id": config.inst_id,
        "bar": config.bar,
        "ema_type": config.resolved_ema_type(),
        "ema_period": config.ema_period,
        "trend_ema_type": config.resolved_trend_ema_type(),
        "trend_ema_period": config.trend_ema_period,
        "big_ema_period": config.big_ema_period,
        "entry_reference_ema_type": config.resolved_entry_reference_ema_type(),
        "entry_reference_ema_period": config.entry_reference_ema_period,
        "atr_period": config.atr_period,
        "atr_stop_multiplier": str(config.atr_stop_multiplier),
        "atr_take_multiplier": str(config.atr_take_multiplier),
        "order_size": str(config.order_size),
        "trade_mode": config.trade_mode,
        "signal_mode": config.signal_mode,
        "position_mode": config.position_mode,
        "environment": config.environment,
        "tp_sl_trigger_type": config.tp_sl_trigger_type,
        "strategy_id": config.strategy_id,
        "poll_seconds": config.poll_seconds,
        "risk_amount": None if config.risk_amount is None else str(config.risk_amount),
        "trade_inst_id": config.trade_inst_id,
        "tp_sl_mode": config.tp_sl_mode,
        "local_tp_sl_inst_id": config.local_tp_sl_inst_id,
        "entry_side_mode": config.entry_side_mode,
        "run_mode": config.run_mode,
        "take_profit_mode": config.take_profit_mode,
        "max_entries_per_trend": config.max_entries_per_trend,
        "reentry_confirmation_enabled": config.reentry_confirmation_enabled,
        "reentry_confirmation_min_sequence": config.resolved_reentry_confirmation_min_sequence(),
        "reentry_confirmation_ma_type": config.resolved_reentry_confirmation_ma_type(),
        "reentry_confirmation_ma_period": config.resolved_reentry_confirmation_ma_period(),
        "dynamic_two_r_break_even": config.dynamic_two_r_break_even,
        "dynamic_break_even_trigger_r": int(config.dynamic_break_even_trigger_r),
        "dynamic_fee_offset_enabled": config.dynamic_fee_offset_enabled,
        "dynamic_protection_rules": list(dynamic_protection_rules_to_payload(config.resolved_dynamic_protection_rules())),
        "ema55_slope_exit_enabled": config.ema55_slope_exit_enabled,
        "ema55_slope_lock_profit_enabled": config.ema55_slope_lock_profit_enabled,
        "ema55_slope_lock_profit_trigger_r": int(config.ema55_slope_lock_profit_trigger_r),
        "dynamic_first_lock_r": int(config.dynamic_first_lock_r),
        "dynamic_trailing_step_r": int(config.dynamic_trailing_step_r),
        "ema55_slope_negative_entry_bars": int(config.ema55_slope_negative_entry_bars),
        "trend_ema_slope_filter_enabled": config.trend_ema_slope_filter_enabled,
        "trend_ema_slope_filter_lookback_bars": config.trend_ema_slope_filter_lookback_bars,
        "trend_ema_slope_filter_min_ratio": str(config.trend_ema_slope_filter_min_ratio),
        "atr_percentile_filter_max": str(config.atr_percentile_filter_max),
        "body_retest_breakdown_atr_multiplier": str(config.body_retest_breakdown_atr_multiplier),
        "body_retest_retest_atr_multiplier": str(config.body_retest_retest_atr_multiplier),
        "body_retest_stop_buffer_atr_multiplier": str(config.body_retest_stop_buffer_atr_multiplier),
        "body_retest_body_atr_limit": str(config.body_retest_body_atr_limit),
        "body_retest_watch_bars": int(config.body_retest_watch_bars),
        "time_stop_break_even_enabled": config.time_stop_break_even_enabled,
        "time_stop_break_even_bars": config.resolved_time_stop_break_even_bars(),
        "trend_ema_close_exit_after_trigger_r_enabled": config.trend_ema_close_exit_after_trigger_r_enabled,
        "trend_ema_close_exit_after_trigger_r": config.resolved_trend_ema_close_exit_after_trigger_r(),
        "hold_close_exit_bars": int(config.hold_close_exit_bars),
        "mtf_filter_inst_id": config.mtf_filter_inst_id,
        "mtf_filter_bar": config.mtf_filter_bar,
        "mtf_filter_fast_ema_period": config.mtf_filter_fast_ema_period,
        "mtf_filter_slow_ema_period": config.mtf_filter_slow_ema_period,
        "mtf_reversal_mode": config.mtf_reversal_mode,
        "daily_filter_inst_id": config.daily_filter_inst_id,
        "daily_filter_bar": config.daily_filter_bar,
        "daily_filter_boundary": config.daily_filter_boundary,
        "daily_filter_enabled": config.daily_filter_enabled,
        "daily_filter_mode": config.daily_filter_mode,
        "daily_filter_scope": config.daily_filter_scope,
        "daily_filter_ma_type": config.daily_filter_ma_type,
        "daily_filter_period": config.daily_filter_period,
        "rail_candidate_ema_periods": list(config.rail_candidate_ema_periods),
        "rail_touch_atr_ratio": str(config.rail_touch_atr_ratio),
        "rail_bounce_atr_ratio": str(config.rail_bounce_atr_ratio),
        "rail_bounce_confirm_bars": config.rail_bounce_confirm_bars,
        "rail_break_atr_ratio": str(config.rail_break_atr_ratio),
        "rail_reclaim_bars": config.rail_reclaim_bars,
        "rail_score_lookback_bars": config.rail_score_lookback_bars,
        "rail_switch_min_score_delta": str(config.rail_switch_min_score_delta),
        "rail_min_touches": config.rail_min_touches,
        "rail_min_bounces": config.rail_min_bounces,
        "rail_fast_gate_enabled": config.rail_fast_gate_enabled,
        "rail_fast_gate_period": config.rail_fast_gate_period,
        "rail_fast_min_gap_ema200_atr": str(config.rail_fast_min_gap_ema200_atr),
        "rail_fast_min_spread_trend_atr": str(config.rail_fast_min_spread_trend_atr),
        "rail_fast_max_recent_range_atr": str(config.rail_fast_max_recent_range_atr),
        "rail_fast_recent_range_bars": config.rail_fast_recent_range_bars,
        "backtest_profile_id": config.backtest_profile_id,
        "backtest_profile_name": config.backtest_profile_name,
        "backtest_profile_summary": config.backtest_profile_summary,
        "backtest_initial_capital": str(config.backtest_initial_capital),
        "backtest_sizing_mode": config.backtest_sizing_mode,
        "backtest_risk_percent": None
        if config.backtest_risk_percent is None
        else str(config.backtest_risk_percent),
        "backtest_compounding": config.backtest_compounding,
        "backtest_entry_slippage_rate": str(config.resolved_backtest_entry_slippage_rate()),
        "backtest_exit_slippage_rate": str(config.resolved_backtest_exit_slippage_rate()),
        "backtest_slippage_rate": str(config.backtest_slippage_rate),
        "backtest_funding_rate": str(config.backtest_funding_rate),
    }


def _deserialize_strategy_config(payload: dict[str, object]) -> StrategyConfig:
    legacy_slippage_rate = Decimal(str(payload.get("backtest_slippage_rate", "0")))
    entry_slippage_rate = (
        legacy_slippage_rate
        if payload.get("backtest_entry_slippage_rate") in (None, "")
        else Decimal(str(payload.get("backtest_entry_slippage_rate")))
    )
    exit_slippage_rate = (
        legacy_slippage_rate
        if payload.get("backtest_exit_slippage_rate") in (None, "")
        else Decimal(str(payload.get("backtest_exit_slippage_rate")))
    )
    def coerce_bool(value: object, default: bool) -> bool:
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

    rail_candidate_periods_raw = payload.get("rail_candidate_ema_periods", (21, 34, 55, 89))
    rail_candidate_periods: tuple[int, ...]
    if isinstance(rail_candidate_periods_raw, (list, tuple)):
        rail_candidate_periods = tuple(
            int(item) for item in rail_candidate_periods_raw if str(item).strip() and int(item) > 0
        )
    else:
        rail_candidate_periods = (21, 34, 55, 89)

    return StrategyConfig(
        inst_id=str(payload.get("inst_id", "")),
        bar=str(payload.get("bar", "15m")),
        ema_type=str(payload.get("ema_type", "ema")),
        ema_period=int(payload.get("ema_period", 21)),
        trend_ema_type=str(payload.get("trend_ema_type", "ema")),
        trend_ema_period=int(payload.get("trend_ema_period", 55)),
        big_ema_period=int(payload.get("big_ema_period", 233)),
        entry_reference_ema_type=str(payload.get("entry_reference_ema_type", payload.get("ema_type", "ema"))),
        entry_reference_ema_period=int(payload.get("entry_reference_ema_period", 55)),
        atr_period=int(payload.get("atr_period", 14)),
        atr_stop_multiplier=Decimal(str(payload.get("atr_stop_multiplier", "2"))),
        atr_take_multiplier=Decimal(str(payload.get("atr_take_multiplier", "4"))),
        order_size=Decimal(str(payload.get("order_size", "0"))),
        trade_mode=str(payload.get("trade_mode", "cross")),
        signal_mode=str(payload.get("signal_mode", "both")),
        position_mode=str(payload.get("position_mode", "net")),
        environment=str(payload.get("environment", "demo")),
        tp_sl_trigger_type=str(payload.get("tp_sl_trigger_type", "mark")),
        strategy_id=str(payload.get("strategy_id", STRATEGY_DYNAMIC_ID)),
        poll_seconds=float(payload.get("poll_seconds", 10.0)),
        risk_amount=None
        if payload.get("risk_amount") in (None, "")
        else Decimal(str(payload.get("risk_amount"))),
        trade_inst_id=None if payload.get("trade_inst_id") in (None, "") else str(payload.get("trade_inst_id")),
        tp_sl_mode=str(payload.get("tp_sl_mode", "exchange")),
        local_tp_sl_inst_id=None
        if payload.get("local_tp_sl_inst_id") in (None, "")
        else str(payload.get("local_tp_sl_inst_id")),
        entry_side_mode=str(payload.get("entry_side_mode", "follow_signal")),
        run_mode=str(payload.get("run_mode", "trade")),
        take_profit_mode=str(payload.get("take_profit_mode", "dynamic")),
        max_entries_per_trend=int(payload.get("max_entries_per_trend", 1)),
        reentry_confirmation_enabled=coerce_bool(payload.get("reentry_confirmation_enabled"), False),
        reentry_confirmation_min_sequence=int(payload.get("reentry_confirmation_min_sequence", 0)),
        reentry_confirmation_ma_type=str(payload.get("reentry_confirmation_ma_type", "ema")),
        reentry_confirmation_ma_period=int(payload.get("reentry_confirmation_ma_period", 21)),
        dynamic_two_r_break_even=bool(payload.get("dynamic_two_r_break_even", True)),
        dynamic_break_even_trigger_r=int(payload.get("dynamic_break_even_trigger_r", 2)),
        dynamic_fee_offset_enabled=bool(payload.get("dynamic_fee_offset_enabled", True)),
        dynamic_protection_rules=normalize_dynamic_protection_rules(payload.get("dynamic_protection_rules")),
        ema55_slope_exit_enabled=coerce_bool(payload.get("ema55_slope_exit_enabled"), True),
        ema55_slope_lock_profit_enabled=coerce_bool(payload.get("ema55_slope_lock_profit_enabled"), False),
        ema55_slope_lock_profit_trigger_r=int(payload.get("ema55_slope_lock_profit_trigger_r", 5)),
        dynamic_first_lock_r=int(payload.get("dynamic_first_lock_r", 0)),
        dynamic_trailing_step_r=int(payload.get("dynamic_trailing_step_r", 1)),
        ema55_slope_negative_entry_bars=int(payload.get("ema55_slope_negative_entry_bars", 1)),
        trend_ema_slope_filter_enabled=coerce_bool(payload.get("trend_ema_slope_filter_enabled"), True),
        trend_ema_slope_filter_lookback_bars=int(payload.get("trend_ema_slope_filter_lookback_bars", 5)),
        trend_ema_slope_filter_min_ratio=Decimal(str(payload.get("trend_ema_slope_filter_min_ratio", "0"))),
        atr_percentile_filter_max=Decimal(str(payload.get("atr_percentile_filter_max", "0"))),
        body_retest_breakdown_atr_multiplier=Decimal(str(payload.get("body_retest_breakdown_atr_multiplier", "0.2"))),
        body_retest_retest_atr_multiplier=Decimal(str(payload.get("body_retest_retest_atr_multiplier", "0.3"))),
        body_retest_stop_buffer_atr_multiplier=Decimal(
            str(payload.get("body_retest_stop_buffer_atr_multiplier", "0.3"))
        ),
        body_retest_body_atr_limit=Decimal(str(payload.get("body_retest_body_atr_limit", "1.0"))),
        body_retest_watch_bars=int(payload.get("body_retest_watch_bars", 6)),
        time_stop_break_even_enabled=bool(payload.get("time_stop_break_even_enabled", False)),
        time_stop_break_even_bars=int(payload.get("time_stop_break_even_bars", 10)),
        trend_ema_close_exit_after_trigger_r_enabled=bool(
            payload.get("trend_ema_close_exit_after_trigger_r_enabled", False)
        ),
        trend_ema_close_exit_after_trigger_r=str(payload.get("trend_ema_close_exit_after_trigger_r", 5)),
        hold_close_exit_bars=int(payload.get("hold_close_exit_bars", 0)),
        mtf_filter_inst_id=None
        if payload.get("mtf_filter_inst_id") in (None, "")
        else str(payload.get("mtf_filter_inst_id")),
        mtf_filter_bar=None if payload.get("mtf_filter_bar") in (None, "") else str(payload.get("mtf_filter_bar")),
        mtf_filter_fast_ema_period=int(payload.get("mtf_filter_fast_ema_period", 21)),
        mtf_filter_slow_ema_period=int(payload.get("mtf_filter_slow_ema_period", 55)),
        mtf_reversal_mode=str(payload.get("mtf_reversal_mode", "block_new_entries")),
        daily_filter_inst_id=None
        if payload.get("daily_filter_inst_id") in (None, "")
        else str(payload.get("daily_filter_inst_id")),
        daily_filter_bar=None if payload.get("daily_filter_bar") in (None, "") else str(payload.get("daily_filter_bar")),
        daily_filter_boundary=str(payload.get("daily_filter_boundary", "exchange")),
        daily_filter_enabled=coerce_bool(payload.get("daily_filter_enabled"), False),
        daily_filter_mode=str(payload.get("daily_filter_mode", "disabled")),
        daily_filter_scope=str(payload.get("daily_filter_scope", "both")),
        daily_filter_ma_type=str(payload.get("daily_filter_ma_type", "ema")),
        daily_filter_period=int(payload.get("daily_filter_period", 5)),
        rail_candidate_ema_periods=rail_candidate_periods,
        rail_touch_atr_ratio=Decimal(str(payload.get("rail_touch_atr_ratio", "0.2"))),
        rail_bounce_atr_ratio=Decimal(str(payload.get("rail_bounce_atr_ratio", "0.6"))),
        rail_bounce_confirm_bars=int(payload.get("rail_bounce_confirm_bars", 3)),
        rail_break_atr_ratio=Decimal(str(payload.get("rail_break_atr_ratio", "1.0"))),
        rail_reclaim_bars=int(payload.get("rail_reclaim_bars", 2)),
        rail_score_lookback_bars=int(payload.get("rail_score_lookback_bars", 60)),
        rail_switch_min_score_delta=Decimal(str(payload.get("rail_switch_min_score_delta", "8"))),
        rail_min_touches=int(payload.get("rail_min_touches", 2)),
        rail_min_bounces=int(payload.get("rail_min_bounces", 1)),
        rail_fast_gate_enabled=coerce_bool(payload.get("rail_fast_gate_enabled"), True),
        rail_fast_gate_period=int(payload.get("rail_fast_gate_period", 21)),
        rail_fast_min_gap_ema200_atr=Decimal(str(payload.get("rail_fast_min_gap_ema200_atr", "5.0"))),
        rail_fast_min_spread_trend_atr=Decimal(str(payload.get("rail_fast_min_spread_trend_atr", "1.5"))),
        rail_fast_max_recent_range_atr=Decimal(str(payload.get("rail_fast_max_recent_range_atr", "3.0"))),
        rail_fast_recent_range_bars=int(payload.get("rail_fast_recent_range_bars", 8)),
        backtest_profile_id=str(payload.get("backtest_profile_id", "")),
        backtest_profile_name=str(payload.get("backtest_profile_name", "")),
        backtest_profile_summary=str(payload.get("backtest_profile_summary", "")),
        backtest_initial_capital=Decimal(str(payload.get("backtest_initial_capital", "10000"))),
        backtest_sizing_mode=str(payload.get("backtest_sizing_mode", "fixed_risk")),
        backtest_risk_percent=None
        if payload.get("backtest_risk_percent") in (None, "")
        else Decimal(str(payload.get("backtest_risk_percent"))),
        backtest_compounding=bool(payload.get("backtest_compounding", False)),
        backtest_entry_slippage_rate=entry_slippage_rate,
        backtest_exit_slippage_rate=exit_slippage_rate,
        backtest_slippage_rate=legacy_slippage_rate,
        backtest_funding_rate=Decimal(str(payload.get("backtest_funding_rate", "0"))),
    )


def _serialize_backtest_report(report: BacktestReport) -> dict[str, object]:
    return {
        "total_trades": report.total_trades,
        "win_trades": report.win_trades,
        "loss_trades": report.loss_trades,
        "breakeven_trades": report.breakeven_trades,
        "win_rate": str(report.win_rate),
        "total_pnl": str(report.total_pnl),
        "average_pnl": str(report.average_pnl),
        "gross_profit": str(report.gross_profit),
        "gross_loss": str(report.gross_loss),
        "profit_factor": None if report.profit_factor is None else str(report.profit_factor),
        "average_win": str(report.average_win),
        "average_loss": str(report.average_loss),
        "profit_loss_ratio": None if report.profit_loss_ratio is None else str(report.profit_loss_ratio),
        "average_r_multiple": str(report.average_r_multiple),
        "max_drawdown": str(report.max_drawdown),
        "max_drawdown_pct": str(report.max_drawdown_pct),
        "take_profit_hits": report.take_profit_hits,
        "stop_loss_hits": report.stop_loss_hits,
        "ending_equity": str(report.ending_equity),
        "total_return_pct": str(report.total_return_pct),
        "maker_fees": str(report.maker_fees),
        "taker_fees": str(report.taker_fees),
        "total_fees": str(report.total_fees),
        "slippage_costs": str(report.slippage_costs),
        "funding_costs": str(report.funding_costs),
        "manual_handoffs": report.manual_handoffs,
        "manual_open_positions": report.manual_open_positions,
        "manual_open_size": str(report.manual_open_size),
        "manual_open_pnl": str(report.manual_open_pnl),
        "max_manual_positions": report.max_manual_positions,
        "max_total_occupied_slots": report.max_total_occupied_slots,
    }


def _deserialize_backtest_report(payload: dict[str, object]) -> BacktestReport:
    return BacktestReport(
        total_trades=int(payload.get("total_trades", 0)),
        win_trades=int(payload.get("win_trades", 0)),
        loss_trades=int(payload.get("loss_trades", 0)),
        breakeven_trades=int(payload.get("breakeven_trades", 0)),
        win_rate=Decimal(str(payload.get("win_rate", "0"))),
        total_pnl=Decimal(str(payload.get("total_pnl", "0"))),
        average_pnl=Decimal(str(payload.get("average_pnl", "0"))),
        gross_profit=Decimal(str(payload.get("gross_profit", "0"))),
        gross_loss=Decimal(str(payload.get("gross_loss", "0"))),
        profit_factor=None if payload.get("profit_factor") in (None, "") else Decimal(str(payload.get("profit_factor"))),
        average_win=Decimal(str(payload.get("average_win", "0"))),
        average_loss=Decimal(str(payload.get("average_loss", "0"))),
        profit_loss_ratio=None
        if payload.get("profit_loss_ratio") in (None, "")
        else Decimal(str(payload.get("profit_loss_ratio"))),
        average_r_multiple=Decimal(str(payload.get("average_r_multiple", "0"))),
        max_drawdown=Decimal(str(payload.get("max_drawdown", "0"))),
        max_drawdown_pct=Decimal(str(payload.get("max_drawdown_pct", "0"))),
        take_profit_hits=int(payload.get("take_profit_hits", 0)),
        stop_loss_hits=int(payload.get("stop_loss_hits", 0)),
        ending_equity=Decimal(str(payload.get("ending_equity", "0"))),
        total_return_pct=Decimal(str(payload.get("total_return_pct", "0"))),
        maker_fees=Decimal(str(payload.get("maker_fees", "0"))),
        taker_fees=Decimal(str(payload.get("taker_fees", "0"))),
        total_fees=Decimal(str(payload.get("total_fees", "0"))),
        slippage_costs=Decimal(str(payload.get("slippage_costs", "0"))),
        funding_costs=Decimal(str(payload.get("funding_costs", "0"))),
        manual_handoffs=int(payload.get("manual_handoffs", 0)),
        manual_open_positions=int(payload.get("manual_open_positions", 0)),
        manual_open_size=Decimal(str(payload.get("manual_open_size", "0"))),
        manual_open_pnl=Decimal(str(payload.get("manual_open_pnl", "0"))),
        max_manual_positions=int(payload.get("max_manual_positions", 0)),
        max_total_occupied_slots=int(payload.get("max_total_occupied_slots", 0)),
    )


def _serialize_backtest_snapshot(snapshot: _BacktestSnapshot) -> dict[str, object]:
    return {
        "snapshot_id": snapshot.snapshot_id,
        "created_at": snapshot.created_at.isoformat(timespec="seconds"),
        "candle_limit": snapshot.candle_limit,
        "candle_count": snapshot.candle_count,
        "start_ts": snapshot.start_ts,
        "end_ts": snapshot.end_ts,
        "maker_fee_rate": str(snapshot.maker_fee_rate),
        "taker_fee_rate": str(snapshot.taker_fee_rate),
        "export_path": snapshot.export_path or "",
        "config": _serialize_strategy_config(snapshot.config),
        "report": _serialize_backtest_report(snapshot.report),
        "report_text": snapshot.report_text,
        "archive_id": snapshot.archive_id or "",
    }


def _deserialize_backtest_snapshot(payload: object) -> _BacktestSnapshot | None:
    if not isinstance(payload, dict):
        return None
    try:
        created_raw = str(payload.get("created_at", "")).strip()
        created_at = datetime.fromisoformat(created_raw) if created_raw else datetime.now()
        config_payload = payload.get("config")
        report_payload = payload.get("report")
        if not isinstance(config_payload, dict) or not isinstance(report_payload, dict):
            return None
        return _BacktestSnapshot(
            snapshot_id=str(payload.get("snapshot_id", "")).strip() or "S000",
            created_at=created_at,
            config=_deserialize_strategy_config(config_payload),
            candle_limit=int(payload.get("candle_limit", 0)),
            candle_count=int(payload.get("candle_count", 0)),
            start_ts=int(payload["start_ts"]) if payload.get("start_ts") not in (None, "") else None,
            end_ts=int(payload["end_ts"]) if payload.get("end_ts") not in (None, "") else None,
            report=_deserialize_backtest_report(report_payload),
            report_text=str(payload.get("report_text", "")),
            result=None,
            maker_fee_rate=Decimal(str(payload.get("maker_fee_rate", "0"))),
            taker_fee_rate=Decimal(str(payload.get("taker_fee_rate", "0"))),
            export_path=str(payload.get("export_path", "")).strip() or None,
            runtime_id=None,
            archive_id=str(payload.get("archive_id", "")).strip() or None,
        )
    except Exception:
        return None


class BacktestCompareOverviewWindow:
    def __init__(self, parent) -> None:
        self.window = Toplevel(parent)
        self.window.title("策略回测对比总览")
        apply_adaptive_window_geometry(
            self.window,
            width_ratio=0.82,
            height_ratio=0.78,
            min_width=1180,
            min_height=720,
            max_width=1640,
            max_height=980,
        )
        self.window.columnconfigure(0, weight=1)
        self.window.rowconfigure(1, weight=1)
        self.window.rowconfigure(2, weight=1)

        self._store = get_backtest_snapshot_store()
        self._subscription_token = self._store.subscribe(self._refresh)
        self.summary_text = StringVar(value="正在加载历史回测记录...")
        self.filter_keyword = StringVar(value="")

        self._build_layout()
        self._refresh()
        self.window.protocol("WM_DELETE_WINDOW", self._close)

    def show(self) -> None:
        self.window.deiconify()
        self.window.lift()
        self.window.focus_force()
        self._refresh()

    @staticmethod
    def _strategy_uses_big_ema(strategy_id: str) -> bool:
        return strategy_uses_parameter(strategy_id, "big_ema_period")

    def _build_layout(self) -> None:
        header = ttk.LabelFrame(self.window, text="回测总览", padding=12)
        header.grid(row=0, column=0, sticky="ew", padx=16, pady=(16, 8))
        header.columnconfigure(0, weight=1)
        ttk.Label(header, textvariable=self.summary_text, justify="left").grid(row=0, column=0, sticky="w")
        ttk.Button(header, text="刷新", command=self._refresh).grid(row=0, column=1, sticky="e", padx=(8, 8))
        ttk.Button(header, text="清空全部历史", command=self._clear_all).grid(row=0, column=2, sticky="e")
        filter_bar = ttk.Frame(header)
        filter_bar.grid(row=1, column=0, columnspan=3, sticky="ew", pady=(10, 0))
        filter_bar.columnconfigure(1, weight=1)
        ttk.Label(filter_bar, text="筛选").grid(row=0, column=0, sticky="w")
        filter_entry = ttk.Entry(filter_bar, textvariable=self.filter_keyword)
        filter_entry.grid(row=0, column=1, sticky="ew", padx=(8, 8))
        filter_entry.bind("<Return>", lambda *_: self._refresh())
        ttk.Button(filter_bar, text="应用筛选", command=self._refresh).grid(row=0, column=2, padx=(0, 6))
        ttk.Button(filter_bar, text="清空筛选", command=self._clear_filter).grid(row=0, column=3)

        tree_frame = ttk.Frame(self.window)
        tree_frame.grid(row=1, column=0, sticky="nsew", padx=16, pady=(0, 8))
        tree_frame.columnconfigure(0, weight=1)
        tree_frame.rowconfigure(0, weight=1)

        self.tree = ttk.Treeview(
            tree_frame,
            columns=("id", "time", "strategy", "symbol", "period", "params", "trades", "win_rate", "pnl", "drawdown"),
            show="headings",
            selectmode="browse",
        )
        _configure_backtest_compare_tree(self.tree, id_heading="归档编号")
        tree_scroll_y = ttk.Scrollbar(tree_frame, orient="vertical", command=self.tree.yview)
        tree_scroll_x = ttk.Scrollbar(tree_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(
            yscrollcommand=tree_scroll_y.set,
            xscrollcommand=tree_scroll_x.set,
        )
        self.tree.grid(row=0, column=0, sticky="nsew")
        tree_scroll_y.grid(row=0, column=1, sticky="ns")
        tree_scroll_x.grid(row=1, column=0, sticky="ew")
        self.tree.bind("<<TreeviewSelect>>", self._on_tree_selected)

        detail_frame = ttk.LabelFrame(self.window, text="记录详情", padding=12)
        detail_frame.grid(row=2, column=0, sticky="nsew", padx=16, pady=(0, 16))
        detail_frame.columnconfigure(0, weight=1)
        detail_frame.rowconfigure(0, weight=1)
        self.detail_text = Text(detail_frame, wrap="word", font=("Consolas", 10))
        self.detail_text.grid(row=0, column=0, sticky="nsew")

    def _refresh(self) -> None:
        all_snapshots = self._store.list_snapshots()
        keyword = self.filter_keyword.get()
        snapshots = _filter_backtest_snapshots(all_snapshots, keyword)
        previous_selection = self.tree.selection()
        self.tree.delete(*self.tree.get_children())
        for snapshot in snapshots:
            self.tree.insert(
                "",
                END,
                iid=snapshot.snapshot_id,
                values=_build_backtest_compare_row(snapshot, prefer_archive=True),
            )
        if snapshots:
            summary = f"已保存 {len(all_snapshots)} 组历史回测结果。归档编号用于历史保存。"
            if str(keyword).strip():
                summary = f"{summary} 当前筛选：{len(snapshots)}/{len(all_snapshots)}。"
            self.summary_text.set(summary)
        elif all_snapshots and str(keyword).strip():
            self.summary_text.set(f"已保存 {len(all_snapshots)} 组历史回测结果。当前筛选无结果。")
        else:
            self.summary_text.set("暂无历史回测记录。新的回测结果会自动保存到总览页。")

        target_selection = previous_selection[0] if previous_selection and previous_selection[0] in {item.snapshot_id for item in snapshots} else None
        if target_selection is None and snapshots:
            target_selection = snapshots[-1].snapshot_id
        if target_selection is not None:
            self.tree.selection_set(target_selection)
            self.tree.focus(target_selection)
            self.tree.see(target_selection)
            self._show_snapshot_detail(target_selection)
        else:
            self.detail_text.delete("1.0", END)

    def _clear_filter(self) -> None:
        self.filter_keyword.set("")
        self._refresh()

    def _on_tree_selected(self, *_: object) -> None:
        selection = self.tree.selection()
        if not selection:
            return
        self._show_snapshot_detail(selection[0])

    def _show_snapshot_detail(self, snapshot_id: str) -> None:
        snapshot = self._store.get_snapshot(snapshot_id)
        self.detail_text.delete("1.0", END)
        if snapshot is None:
            return
        self.detail_text.insert("1.0", _build_backtest_compare_detail(snapshot, prefer_archive=True))

    def _clear_all(self) -> None:
        if not messagebox.askyesno("清空历史", "确定要清空全部历史回测记录吗？该操作会同步保存到本地文件。", parent=self.window):
            return
        self._store.clear()

    def _close(self) -> None:
        self._store.unsubscribe(self._subscription_token)
        self.window.destroy()


class BacktestWindow:
    def __init__(self, parent, client: OkxRestClient, initial_state: BacktestLaunchState) -> None:
        self.client = client
        self.window = Toplevel(parent)
        self.window.title("策略回测")
        self._closed = False
        self.window.protocol("WM_DELETE_WINDOW", self._close)
        apply_adaptive_window_geometry(
            self.window,
            width_ratio=0.86,
            height_ratio=0.88,
            min_width=1100,
            min_height=900,
            max_width=1720,
            max_height=1220,
        )

        self._strategy_name_to_id = {item.name: item.strategy_id for item in BACKTEST_STRATEGY_DEFINITIONS}

        self.strategy_name = StringVar(value=initial_state.strategy_name)
        self.symbol = StringVar(value=initial_state.symbol)
        self.bar_label = StringVar(value=_normalize_backtest_bar_label(initial_state.bar))
        self.ema_type = StringVar(value=(initial_state.ema_type or "EMA").upper())
        self.ema_period = StringVar(value=initial_state.ema_period)
        self.trend_ema_type = StringVar(value=(initial_state.trend_ema_type or "EMA").upper())
        self.trend_ema_period = StringVar(value=initial_state.trend_ema_period)
        self.big_ema_period = StringVar(value=initial_state.big_ema_period)
        self.entry_reference_ema_type = StringVar(value=(initial_state.entry_reference_ema_type or "EMA").upper())
        self.entry_reference_ema_period = StringVar(value=initial_state.entry_reference_ema_period)
        self.mtf_filter_bar = StringVar(value=initial_state.mtf_filter_bar)
        self.mtf_filter_fast_ema_period = StringVar(value=initial_state.mtf_filter_fast_ema_period)
        self.mtf_filter_slow_ema_period = StringVar(value=initial_state.mtf_filter_slow_ema_period)
        self.mtf_reversal_mode_label = StringVar(value=initial_state.mtf_reversal_mode_label)
        self.daily_filter_enabled = BooleanVar(value=initial_state.daily_filter_enabled)
        self.daily_filter_boundary_label = StringVar(value=initial_state.daily_filter_boundary_label)
        self.daily_filter_mode_label = StringVar(value=initial_state.daily_filter_mode_label)
        self.daily_filter_scope_label = StringVar(value=initial_state.daily_filter_scope_label)
        self.daily_filter_ma_type = StringVar(value=(initial_state.daily_filter_ma_type or "EMA").upper())
        self.daily_filter_period = StringVar(value=initial_state.daily_filter_period)
        self.atr_period = StringVar(value=initial_state.atr_period)
        self.stop_atr = StringVar(value=initial_state.stop_atr)
        self.take_atr = StringVar(value=initial_state.take_atr)
        self.risk_amount = StringVar(value=initial_state.risk_amount)
        self.maker_fee_percent = StringVar(value=initial_state.maker_fee_percent)
        self.taker_fee_percent = StringVar(value=initial_state.taker_fee_percent)
        self.initial_capital = StringVar(value=initial_state.initial_capital)
        self.sizing_mode_label = StringVar(value=initial_state.sizing_mode_label)
        self.risk_percent = StringVar(value=initial_state.risk_percent)
        self.compounding_enabled = BooleanVar(value=initial_state.compounding_enabled)
        self.entry_slippage_percent = StringVar(value=initial_state.entry_slippage_percent)
        self.exit_slippage_percent = StringVar(value=initial_state.exit_slippage_percent)
        self.funding_rate_percent = StringVar(value=initial_state.funding_rate_percent)
        self.start_time_text = StringVar(value=initial_state.start_time_text)
        self.end_time_text = StringVar(value=initial_state.end_time_text)
        self.take_profit_mode_label = StringVar(value=initial_state.take_profit_mode_label)
        self.max_entries_per_trend = StringVar(value=initial_state.max_entries_per_trend)
        self.reentry_confirmation_enabled = BooleanVar(value=initial_state.reentry_confirmation_enabled)
        self.reentry_confirmation_min_sequence = StringVar(value=initial_state.reentry_confirmation_min_sequence)
        self.reentry_confirmation_ma_type = StringVar(value=(initial_state.reentry_confirmation_ma_type or "EMA").upper())
        self.reentry_confirmation_ma_period = StringVar(value=initial_state.reentry_confirmation_ma_period)
        self.dynamic_two_r_break_even = BooleanVar(value=initial_state.dynamic_two_r_break_even)
        self.dynamic_break_even_trigger_r = StringVar(value=initial_state.dynamic_break_even_trigger_r)
        self.dynamic_protection_rules_json = StringVar(value=initial_state.dynamic_protection_rules_json)
        self.dynamic_fee_offset_enabled = BooleanVar(value=initial_state.dynamic_fee_offset_enabled)
        self.time_stop_break_even_enabled = BooleanVar(value=initial_state.time_stop_break_even_enabled)
        self.time_stop_break_even_bars = StringVar(value=initial_state.time_stop_break_even_bars)
        self.trend_ema_close_exit_after_trigger_r_enabled = BooleanVar(
            value=initial_state.trend_ema_close_exit_after_trigger_r_enabled
        )
        self.trend_ema_close_exit_after_trigger_r = StringVar(
            value=initial_state.trend_ema_close_exit_after_trigger_r
        )
        self.hold_close_exit_bars = StringVar(value=initial_state.hold_close_exit_bars)
        self.trend_ema_slope_filter_min_ratio = StringVar(value=initial_state.trend_ema_slope_filter_min_ratio)
        self.atr_percentile_filter_max = StringVar(value=initial_state.atr_percentile_filter_max)
        self.body_retest_breakdown_atr_multiplier = StringVar(value=initial_state.body_retest_breakdown_atr_multiplier)
        self.body_retest_retest_atr_multiplier = StringVar(value=initial_state.body_retest_retest_atr_multiplier)
        self.body_retest_stop_buffer_atr_multiplier = StringVar(value=initial_state.body_retest_stop_buffer_atr_multiplier)
        self.body_retest_body_atr_limit = StringVar(value=initial_state.body_retest_body_atr_limit)
        self.body_retest_watch_bars = StringVar(value=initial_state.body_retest_watch_bars)
        self.ema55_slope_exit_enabled = BooleanVar(value=initial_state.ema55_slope_exit_enabled)
        self.ema55_slope_lock_profit_enabled = BooleanVar(value=initial_state.ema55_slope_lock_profit_enabled)
        self.ema55_slope_lock_profit_trigger_r = StringVar(value=initial_state.ema55_slope_lock_profit_trigger_r)
        self.dynamic_first_lock_r = StringVar(value=initial_state.dynamic_first_lock_r)
        self.dynamic_trailing_step_r = StringVar(value=initial_state.dynamic_trailing_step_r)
        self.ema55_slope_negative_entry_bars = StringVar(value=initial_state.ema55_slope_negative_entry_bars)
        self.signal_mode_label = StringVar(value=initial_state.signal_mode_label)
        self.trade_mode_label = StringVar(value=initial_state.trade_mode_label)
        self.position_mode_label = StringVar(value=initial_state.position_mode_label)
        self.trigger_type_label = StringVar(value=initial_state.trigger_type_label)
        self.environment_label = StringVar(value=initial_state.environment_label)
        self.candle_limit = StringVar(value=initial_state.candle_limit)
        self.pure_local_backtest = BooleanVar(value=True)
        self.sync_history_bar_vars = {
            bar: BooleanVar(value=DEFAULT_HISTORY_SYNC_BAR_FLAGS.get(bar, False)) for bar in BACKTEST_HISTORY_SYNC_BARS
        }
        self.backtest_profile_id = StringVar(value=initial_state.backtest_profile_id)
        self.backtest_profile_name = StringVar(value=initial_state.backtest_profile_name)
        self.backtest_profile_summary = StringVar(value=initial_state.backtest_profile_summary)
        self.profile_summary_text = StringVar(value=self._build_profile_summary_text())
        self.minimum_order_hint_text = StringVar(value="回测参考：请先选择标的。")
        self._dynamic_protection_rule_rows: list[_DynamicProtectionRuleEditorRow] = []
        self._dynamic_protection_rules_frame: ttk.Frame | None = None
        self._dynamic_protection_rules_card: ttk.LabelFrame | None = None
        self._dynamic_protection_add_rule_button: ttk.Button | None = None
        self._dynamic_protection_restore_button: ttk.Button | None = None
        self._strategy_parameter_drafts = load_strategy_parameter_drafts()
        self._strategy_parameter_scope = "backtest"
        self._last_strategy_parameter_strategy_id: str | None = None
        self.local_data_status = StringVar(value="本地数据：等待选择标的与周期。")
        self.history_sync_status = StringVar(
            value="填 0 = 全量；填 10000 = 最新往前 10000 根。可先点“同步历史数据”下载 5 个币种的 5m / 15m / 1H / 4H 全量缓存；点“校验数据”检查连续性并尝试补洞。"
        )
        self.report_summary = StringVar(value="点击“开始回测”后，会在这里显示报告摘要。")
        self.manual_summary = StringVar(value="当前策略没有额外扩展统计。")
        self.manual_filter_label = StringVar(value="全部")
        self.manual_sort_label = StringVar(value=MANUAL_DEFAULT_SORT_LABEL)
        self.compare_summary = StringVar(value="暂无回测对比记录。")
        self.compare_filter_keyword = StringVar(value="")
        self.matrix_summary = StringVar(value="\u6682\u65e0 ATR \u6279\u91cf\u56de\u6d4b\u77e9\u9635\u3002")
        self.heatmap_summary = StringVar(
            value="\u53c2\u6570\u70ed\u529b\u56fe\u4f1a\u5728\u8fd9\u91cc\u663e\u793a\uff0c\u53ef\u5207\u6362\u6307\u6807\u5e76\u5355\u51fb\u5355\u5143\u683c\u8054\u52a8\u56de\u6d4b\u89c6\u56fe\u3002"
        )
        self.heatmap_metric = StringVar(value="总盈亏")
        self.batch_entries_layer_label = StringVar(value=_batch_entries_label(BATCH_MAX_ENTRIES_OPTIONS[0]))
        self._latest_result: BacktestResult | None = None
        self.chart_frame: ttk.LabelFrame | None = None
        self.chart_canvas: Canvas | None = None
        self._chart_zoom_window: Toplevel | None = None
        self._chart_zoom_canvas: Canvas | None = None
        self._chart_zoom_intro_label: ttk.Label | None = None
        self._chart_zoom_context_label: ttk.Label | None = None
        self._chart_zoom_metrics_label: ttk.Label | None = None
        self._chart_redraw_job: str | None = None
        self._chart_canvas_redraw_jobs: dict[int, str] = {}
        self._chart_canvas_finalize_jobs: dict[int, str] = {}
        self._main_chart_view = _ChartViewport()
        self._zoom_chart_view = _ChartViewport()
        self._chart_render_states: dict[int, _ChartRenderState] = {}
        self._chart_hover_indices: dict[int, int | None] = {}
        self._backtest_snapshots: dict[str, _BacktestSnapshot] = {}
        self._backtest_snapshot_order: list[str] = []
        self._backtest_snapshot_sequence = 0
        self._manual_tree_position_map: dict[str, BacktestManualPosition] = {}
        self._trades_notebook: ttk.Notebook | None = None
        self._trade_tab: ttk.Frame | None = None
        self._extension_stats_tab: ttk.Frame | None = None
        self._current_snapshot_id: str | None = None
        self._backtest_running = False
        self._history_sync_running = False
        self._history_verify_running = False
        self._batch_sequence = 0
        self._batch_snapshot_groups: dict[str, list[str]] = {}
        self._snapshot_batch_labels: dict[str, str] = {}
        self._current_matrix_batch_label: str | None = None
        self._content_pane_initialized = False
        self._backtest_hint_instrument_cache: dict[str, Instrument] = {}
        self._backtest_hint_fetching_symbols: set[str] = set()
        self._backtest_hint_after_id: str | None = None
        self._local_data_status_after_id: str | None = None

        self.strategy_name.trace_add("write", self._schedule_backtest_minimum_order_hint_update)
        self.symbol.trace_add("write", self._schedule_backtest_minimum_order_hint_update)
        self.signal_mode_label.trace_add("write", self._schedule_backtest_minimum_order_hint_update)
        self.risk_amount.trace_add("write", self._schedule_backtest_minimum_order_hint_update)
        self.sizing_mode_label.trace_add("write", self._schedule_backtest_minimum_order_hint_update)
        self.symbol.trace_add("write", self._schedule_local_data_status_update)
        self.bar_label.trace_add("write", self._schedule_local_data_status_update)
        self.start_time_text.trace_add("write", self._schedule_local_data_status_update)
        self.end_time_text.trace_add("write", self._schedule_local_data_status_update)
        self.candle_limit.trace_add("write", self._schedule_local_data_status_update)
        self.pure_local_backtest.trace_add("write", self._schedule_local_data_status_update)
        self.local_data_status.trace_add("write", self._refresh_backtest_action_summary)
        self.history_sync_status.trace_add("write", self._refresh_backtest_action_summary)

        self._build_layout()
        self._update_batch_layer_controls("none", [])
        self._apply_selected_strategy_definition()
        self._update_sizing_mode_widgets()
        self._update_backtest_minimum_order_hint()
        self.history_sync_status.set(
            "先勾选要同步的周期。默认只同步 1H / 4H；打开“纯本地回测”后只使用本地缓存，缺数据会直接提示。"
        )
        self._update_local_data_status()

    @staticmethod
    def _widget_exists(widget: object) -> bool:
        try:
            return widget is not None and bool(widget.winfo_exists())
        except Exception:
            return False

    def _ui_alive(self) -> bool:
        return (not self._closed) and self._widget_exists(self.window)

    def _close(self) -> None:
        self._save_strategy_parameter_draft()
        self._closed = True
        if self._chart_redraw_job is not None and self._widget_exists(self.window):
            try:
                self.window.after_cancel(self._chart_redraw_job)
            except Exception:
                pass
            self._chart_redraw_job = None
        if self._local_data_status_after_id is not None and self._widget_exists(self.window):
            try:
                self.window.after_cancel(self._local_data_status_after_id)
            except Exception:
                pass
            self._local_data_status_after_id = None
        if self._widget_exists(self.window):
            for job_map in (self._chart_canvas_redraw_jobs, self._chart_canvas_finalize_jobs):
                for job in list(job_map.values()):
                    try:
                        self.window.after_cancel(job)
                    except Exception:
                        pass
                job_map.clear()
        if self._widget_exists(getattr(self, "_chart_zoom_window", None)):
            try:
                self._chart_zoom_window.destroy()
            except Exception:
                pass
        if self._widget_exists(self.window):
            self.window.destroy()

    @staticmethod
    def _strategy_uses_big_ema(strategy_id: str) -> bool:
        return strategy_uses_parameter(strategy_id, "big_ema_period")

    @staticmethod
    def _strategy_supports_dynamic_take_profit(strategy_id: str) -> bool:
        return strategy_supports_dynamic_take_profit(strategy_id)

    @staticmethod
    def _build_backtest_panedwindow(parent: object, *, orient: str) -> PanedWindow:
        # 关闭实时重排，拖动 sash 时只显示预览，避免 Treeview 和图表在拖动期间连续重布局。
        return PanedWindow(
            parent,
            orient=orient,
            opaqueresize=False,
            sashwidth=10,
            sashrelief="flat",
            bd=0,
            relief="flat",
            bg="#d8dee4",
        )

    def _bind_responsive_wrap(
        self,
        label: ttk.Label,
        container: object,
        *,
        padding: int = 40,
        min_wrap: int = 220,
    ) -> None:
        def _apply_wrap(_event: object | None = None) -> None:
            if not self._widget_exists(label):
                return
            try:
                width = int(container.winfo_width())
            except Exception:
                return
            if width <= 1:
                return
            label.configure(wraplength=max(width - padding, min_wrap))

        try:
            container.bind("<Configure>", _apply_wrap, add="+")
        except Exception:
            pass
        self.window.after_idle(_apply_wrap)

    @staticmethod
    def _set_field_state(widget: object, *, editable: bool) -> None:
        state = "normal" if editable else "readonly"
        if isinstance(widget, ttk.Combobox):
            widget.configure(state="readonly" if editable else "disabled")
            return
        try:
            widget.configure(state=state)
        except Exception:
            try:
                widget.configure(state="normal" if editable else "disabled")
            except Exception:
                pass

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
            rules_payload.append(
                {
                    "trigger_r": trigger_r,
                    "action": action,
                    "lock_r": None if action == "break_even" else max(int(row.lock_r.get().strip() or "0"), 0),
                    "trail_mode": "none" if action == "break_even" else trail_mode,
                    "trail_every_r": None
                    if action == "break_even" or trail_mode == "none"
                    else max(int(row.trail_every_r.get().strip() or "1"), 1),
                    "trail_add_r": None
                    if action == "break_even" or trail_mode == "none"
                    else max(int(row.trail_add_r.get().strip() or "1"), 1),
                }
            )
        serialized = json.dumps(dynamic_protection_rules_to_payload(rules_payload), ensure_ascii=False)
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
        trail_every_r = StringVar(
            value="" if normalized is None or not normalized.trailing_enabled() else str(normalized.trail_every_r or 1)
        )
        trail_add_r = StringVar(
            value="" if normalized is None or not normalized.trailing_enabled() else str(normalized.trail_add_r or 1)
        )
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
            variable.trace_add(
                "write",
                lambda *_args, current=editor_row: (
                    self._update_dynamic_protection_rule_row_state(current),
                    self._sync_dynamic_protection_rules_json_from_editor(),
                ),
            )
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

    def _build_profile_summary_text(self) -> str:
        name = self.backtest_profile_name.get().strip()
        summary = self.backtest_profile_summary.get().strip()
        if not name and not summary:
            return "当前未加载参数模板。可直接手填参数，或先选择策略和币种自动带出默认参数，也可导入参数模板覆盖当前设置。"
        if name and summary:
            return f"当前 Profile：{name} | {summary}"
        return f"当前 Profile：{name or summary}"

    def _refresh_profile_summary_text(self) -> None:
        self.profile_summary_text.set(self._build_profile_summary_text())

    def _strategy_parameter_scope_drafts(self) -> dict[str, object]:
        drafts = self._strategy_parameter_drafts.get(self._strategy_parameter_scope)
        if not isinstance(drafts, dict):
            drafts = {}
            self._strategy_parameter_drafts[self._strategy_parameter_scope] = drafts
        return drafts

    def _strategy_parameter_bindings(self) -> dict[str, object]:
        return {
            "bar": self.bar_label,
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
            "take_profit_mode": self.take_profit_mode_label,
            "max_entries_per_trend": self.max_entries_per_trend,
            "reentry_confirmation_enabled": self.reentry_confirmation_enabled,
            "reentry_confirmation_min_sequence": self.reentry_confirmation_min_sequence,
            "reentry_confirmation_ma_type": self.reentry_confirmation_ma_type,
            "reentry_confirmation_ma_period": self.reentry_confirmation_ma_period,
            "dynamic_two_r_break_even": self.dynamic_two_r_break_even,
            "dynamic_break_even_trigger_r": self.dynamic_break_even_trigger_r,
            "dynamic_protection_rules": self.dynamic_protection_rules_json,
            "dynamic_fee_offset_enabled": self.dynamic_fee_offset_enabled,
            "trend_ema_close_exit_after_trigger_r_enabled": self.trend_ema_close_exit_after_trigger_r_enabled,
            "trend_ema_close_exit_after_trigger_r": self.trend_ema_close_exit_after_trigger_r,
            "trend_ema_slope_filter_min_ratio": self.trend_ema_slope_filter_min_ratio,
            "atr_percentile_filter_max": self.atr_percentile_filter_max,
            "body_retest_breakdown_atr_multiplier": self.body_retest_breakdown_atr_multiplier,
            "body_retest_retest_atr_multiplier": self.body_retest_retest_atr_multiplier,
            "body_retest_stop_buffer_atr_multiplier": self.body_retest_stop_buffer_atr_multiplier,
            "body_retest_body_atr_limit": self.body_retest_body_atr_limit,
            "body_retest_watch_bars": self.body_retest_watch_bars,
            "ema55_slope_exit_enabled": self.ema55_slope_exit_enabled,
            "ema55_slope_lock_profit_enabled": self.ema55_slope_lock_profit_enabled,
            "ema55_slope_lock_profit_trigger_r": self.ema55_slope_lock_profit_trigger_r,
            "dynamic_first_lock_r": self.dynamic_first_lock_r,
            "dynamic_trailing_step_r": self.dynamic_trailing_step_r,
            "ema55_slope_negative_entry_bars": self.ema55_slope_negative_entry_bars,
            "time_stop_break_even_enabled": self.time_stop_break_even_enabled,
            "time_stop_break_even_bars": self.time_stop_break_even_bars,
            "hold_close_exit_bars": self.hold_close_exit_bars,
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
            default_value = strategy_parameter_default_for_scope(strategy_id, key, self._strategy_parameter_scope)
            if default_value is None:
                continue
            if key == "bar":
                variable.set(_normalize_backtest_bar_label(str(default_value)))
            elif key == "signal_mode":
                variable.set(SIGNAL_VALUE_TO_LABEL.get(str(default_value), definition.default_signal_label))
            elif key == "take_profit_mode":
                variable.set(TAKE_PROFIT_MODE_VALUE_TO_LABEL.get(str(default_value), self.take_profit_mode_label.get()))
            elif key == "mtf_reversal_mode":
                variable.set(MTF_REVERSAL_MODE_VALUE_TO_LABEL.get(str(default_value), self.mtf_reversal_mode_label.get()))
            elif key.endswith("_type"):
                variable.set(str(default_value).upper())
            else:
                variable.set(default_value)
        self._apply_strategy_parameter_fixed_values(strategy_id, definition=definition)
        self._rebuild_dynamic_protection_rule_editor()

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
            if variable is not None:
                if key == "bar":
                    variable.set(_normalize_backtest_bar_label(str(fixed_value)))
                elif key == "signal_mode":
                    variable.set(SIGNAL_VALUE_TO_LABEL.get(str(fixed_value), resolved_definition.default_signal_label))
                elif key == "mtf_reversal_mode":
                    variable.set(MTF_REVERSAL_MODE_VALUE_TO_LABEL.get(str(fixed_value), self.mtf_reversal_mode_label.get()))
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
        self.entry_reference_ema_caption.configure(text=strategy_entry_reference_period_caption(strategy_id))
        label_map = {
            "bar": (self.bar_caption, "K线周期"),
            "signal_mode": (self.signal_caption, "信号方向"),
            "ema_period": (self.ema_period_caption, _strategy_fast_line_caption(strategy_id)),
            "trend_ema_period": (self.trend_ema_period_caption, "趋势均线"),
            "big_ema_period": (self.big_ema_caption, "大周期均线"),
        }
        for key, (widget, base_text) in label_map.items():
            text = f"{base_text}{fixed_suffix}" if strategy_fixed_value(strategy_id, key) is not None else base_text
            widget.configure(text=text)

    def _sync_backtest_params_viewport(self, event: object | None = None) -> None:
        canvas = getattr(self, "_params_canvas", None)
        scroll = getattr(self, "_params_scroll", None)
        inner = getattr(self, "_params_inner", None)
        inner_id = getattr(self, "_params_inner_window_id", None)
        viewport = getattr(self, "_params_viewport", None)
        if canvas is None or scroll is None or inner is None or inner_id is None:
            return
        if not self._widget_exists(canvas):
            return
        try:
            self.window.update_idletasks()
            canvas.update_idletasks()
            if viewport is not None and self._widget_exists(viewport):
                viewport.update_idletasks()
        except Exception:
            pass
        try:
            inner_w = int(canvas.winfo_width())
        except Exception:
            inner_w = 0
        if event is not None and getattr(event, "widget", None) is canvas:
            try:
                inner_w = max(inner_w, int(getattr(event, "width", 0)))
            except Exception:
                pass
        if inner_w <= 2 and viewport is not None and self._widget_exists(viewport):
            try:
                vw = max(int(viewport.winfo_width()), inner_w)
            except Exception:
                vw = inner_w
            inner_w = vw
            try:
                if scroll.winfo_ismapped():
                    inner_w -= int(scroll.winfo_width())
            except Exception:
                pass
        inner_w = max(inner_w - 2, 0)
        if inner_w > 2:
            try:
                canvas.itemconfigure(inner_id, width=inner_w)
            except Exception:
                pass
        try:
            inner_h = inner.winfo_reqheight()
        except Exception:
            return
        viewport_h = 0
        if viewport is not None and self._widget_exists(viewport):
            try:
                viewport_h = int(viewport.winfo_height())
            except Exception:
                viewport_h = 0
        try:
            screen_h = max(int(self.window.winfo_screenheight()), 600)
        except Exception:
            screen_h = 800
        cap = max(360, min(int(screen_h * 0.48), 760))
        available_h = max(viewport_h - 2, 0)
        preferred_h = max(cap, available_h) if available_h > 0 else cap
        view_h = max(1, min(inner_h + 10, preferred_h))
        try:
            canvas.configure(height=view_h)
            bbox = canvas.bbox("all")
            if bbox is not None:
                canvas.configure(scrollregion=bbox)
            else:
                canvas.configure(scrollregion=(0, 0, int(canvas.cget("width") or 1), view_h))
        except Exception:
            return
        shown = False
        try:
            content_bottom = float(bbox[3]) if bbox is not None else float(inner_h)
            shown = content_bottom > float(view_h) + 2.0
        except Exception:
            shown = inner_h > view_h + 2
        try:
            if shown:
                scroll.grid(row=0, column=1, sticky="ns")
            else:
                scroll.grid_remove()
                canvas.yview_moveto(0.0)
        except Exception:
            pass

    def _initialize_backtest_content_pane(self) -> None:
        if self._content_pane_initialized:
            return
        content_pane = getattr(self, "_content_pane", None)
        if content_pane is None or not self._widget_exists(content_pane):
            return
        try:
            self.window.update_idletasks()
            total_h = max(int(content_pane.winfo_height()), int(content_pane.winfo_reqheight()))
        except Exception:
            return
        if total_h <= 1:
            return
        controls = getattr(self, "_controls_frame", None)
        try:
            controls_h = int(controls.winfo_reqheight()) if controls is not None and self._widget_exists(controls) else 0
        except Exception:
            controls_h = 0
        desired_upper = controls_h + 24 if controls_h > 0 else int(total_h * 0.46)
        upper_height = max(300, min(desired_upper, total_h - 260))
        try:
            content_pane.sashpos(0, upper_height)
        except Exception:
            return
        self._content_pane_initialized = True

    def _params_canvas_mousewheel(self, event: object) -> None:
        canvas = getattr(self, "_params_canvas", None)
        if canvas is None or not self._widget_exists(canvas):
            return
        try:
            delta = int(getattr(event, "delta", 0) / 120)
        except Exception:
            delta = 0
        if delta:
            canvas.yview_scroll(-delta, "units")
            return
        num = getattr(event, "num", 0)
        if num == 4:
            canvas.yview_scroll(-3, "units")
        elif num == 5:
            canvas.yview_scroll(3, "units")

    def _bind_params_canvas_mousewheel(self) -> None:
        canvas = getattr(self, "_params_canvas", None)
        if canvas is None:
            return

        def _enter(_e: object) -> None:
            canvas.bind_all("<MouseWheel>", self._params_canvas_mousewheel)
            canvas.bind_all("<Button-4>", self._params_canvas_mousewheel)
            canvas.bind_all("<Button-5>", self._params_canvas_mousewheel)

        def _leave(_e: object) -> None:
            canvas.unbind_all("<MouseWheel>")
            canvas.unbind_all("<Button-4>")
            canvas.unbind_all("<Button-5>")

        canvas.bind("<Enter>", _enter)
        canvas.bind("<Leave>", _leave)

    def _sync_matrix_grid_viewport(self, event: object | None = None) -> None:
        canvas = getattr(self, "matrix_canvas", None)
        inner = getattr(self, "matrix_grid_frame", None)
        inner_id = getattr(self, "_matrix_inner_window_id", None)
        if canvas is None or inner is None or inner_id is None:
            return
        if not self._widget_exists(canvas):
            return
        try:
            self.window.update_idletasks()
            canvas.update_idletasks()
        except Exception:
            pass
        try:
            inner_w = int(canvas.winfo_width())
        except Exception:
            inner_w = 0
        if event is not None and getattr(event, "widget", None) is canvas:
            try:
                inner_w = max(inner_w, int(getattr(event, "width", 0)))
            except Exception:
                pass
        inner_w = max(inner_w - 2, 0)
        if inner_w > 2:
            try:
                canvas.itemconfigure(inner_id, width=inner_w)
            except Exception:
                pass
        try:
            bbox = canvas.bbox("all")
            if bbox is None:
                bbox = (0, 0, max(inner_w, 1), max(int(inner.winfo_reqheight()), 1))
            canvas.configure(scrollregion=bbox)
        except Exception:
            return

    def _sync_heatmap_canvas_scrollregion(self, fallback_width: int = 640, fallback_height: int = 360) -> None:
        canvas = getattr(self, "heatmap_canvas", None)
        if canvas is None or not self._widget_exists(canvas):
            return
        try:
            bbox = canvas.bbox("all")
            if bbox is None:
                bbox = (0, 0, fallback_width, fallback_height)
            canvas.configure(scrollregion=bbox)
        except Exception:
            return

    def _build_layout(self) -> None:
        self.window.columnconfigure(0, weight=1)
        self.window.rowconfigure(0, weight=1)

        main_pane = self._build_backtest_panedwindow(self.window, orient="vertical")
        main_pane.grid(row=0, column=0, sticky="nsew", padx=16, pady=16)
        self._content_pane = main_pane

        params_viewport = ttk.Frame(main_pane)
        params_viewport.columnconfigure(0, weight=1)
        params_viewport.rowconfigure(0, weight=1)
        params_viewport.bind("<Configure>", lambda _e: self._sync_backtest_params_viewport())
        self._params_viewport = params_viewport

        try:
            _params_bg = self.window.cget("background")
        except Exception:
            _params_bg = ""

        params_canvas = Canvas(
            params_viewport,
            highlightthickness=0,
            bd=0,
            background=_params_bg or "#f0f0f0",
        )
        params_canvas.grid(row=0, column=0, sticky="nsew")
        params_scroll = ttk.Scrollbar(params_viewport, orient="vertical", command=params_canvas.yview)
        params_canvas.configure(yscrollcommand=params_scroll.set)
        self._params_canvas = params_canvas
        self._params_scroll = params_scroll

        params_inner = ttk.Frame(params_canvas)
        self._params_inner_window_id = params_canvas.create_window((0, 0), window=params_inner, anchor="nw")
        self._params_inner = params_inner
        params_inner.columnconfigure(0, weight=1)
        params_inner.bind("<Configure>", lambda _e: self._sync_backtest_params_viewport())
        params_canvas.bind("<Configure>", self._sync_backtest_params_viewport)

        controls = ttk.LabelFrame(params_inner, text="回测参数", padding=12)
        controls.grid(row=0, column=0, sticky="ew")
        self._controls_frame = controls
        self._bind_params_canvas_mousewheel()
        self.window.after_idle(self._sync_backtest_params_viewport)
        self.window.after(120, self._sync_backtest_params_viewport)

        controls.columnconfigure(0, weight=1)

        def _configure_pair_columns(frame: ttk.Frame, count: int) -> None:
            for column in range(count):
                frame.columnconfigure(column, weight=1 if column % 2 == 1 else 0)

        top_sections = ttk.Frame(controls)
        top_sections.grid(row=0, column=0, sticky="ew")
        top_sections.columnconfigure(0, weight=4)
        top_sections.columnconfigure(1, weight=4)
        top_sections.columnconfigure(2, weight=3)

        strategy_section = ttk.LabelFrame(top_sections, text="基础参数", padding=(10, 8))
        strategy_section.grid(row=0, column=0, sticky="nsew", padx=(0, 8))
        _configure_pair_columns(strategy_section, 4)

        signal_section = ttk.LabelFrame(top_sections, text="信号与止盈", padding=(10, 8))
        signal_section.grid(row=0, column=1, sticky="nsew", padx=(0, 8))
        _configure_pair_columns(signal_section, 4)

        advanced_section = ttk.LabelFrame(top_sections, text="扩展条件", padding=(10, 8))
        advanced_section.grid(row=0, column=2, sticky="nsew")
        _configure_pair_columns(advanced_section, 4)

        dynamic_section = ttk.LabelFrame(controls, text="动态止盈与退出条件", padding=(10, 8))
        dynamic_section.grid(row=1, column=0, sticky="ew", pady=(10, 0))
        _configure_pair_columns(dynamic_section, 6)

        backtest_section = ttk.LabelFrame(controls, text="回测参数", padding=(10, 8))
        backtest_section.grid(row=2, column=0, sticky="ew", pady=(10, 0))
        backtest_section.columnconfigure(0, weight=1)

        row = 0
        ttk.Label(strategy_section, text="策略").grid(row=row, column=0, sticky="w")
        strategy_combo = ttk.Combobox(
            strategy_section,
            textvariable=self.strategy_name,
            values=[item.name for item in BACKTEST_STRATEGY_DEFINITIONS],
            state="readonly",
        )
        strategy_combo.grid(row=row, column=1, columnspan=3, sticky="ew")
        strategy_combo.bind("<<ComboboxSelected>>", self._on_strategy_selected)

        row += 1
        ttk.Label(strategy_section, text="交易对").grid(row=row, column=0, sticky="w", pady=(8, 0))
        self.symbol_combo = ttk.Combobox(
            strategy_section,
            textvariable=self.symbol,
            values=_build_backtest_symbol_options(self.symbol.get()),
            state="readonly",
        )
        self.symbol_combo.grid(row=row, column=1, sticky="ew", padx=(0, 8), pady=(8, 0))
        self.symbol_combo.bind("<<ComboboxSelected>>", self._on_symbol_selected)
        self.bar_caption = ttk.Label(strategy_section, text="K线周期")
        self.bar_caption.grid(row=row, column=2, sticky="w", pady=(8, 0))
        self.bar_combo = ttk.Combobox(
            strategy_section,
            textvariable=self.bar_label,
            values=list(BACKTEST_BAR_LABEL_TO_VALUE.keys()),
            state="readonly",
        )
        self.bar_combo.grid(row=row, column=3, sticky="ew", pady=(8, 0))

        row += 1
        self.profile_summary_caption = ttk.Label(strategy_section, text="Profile")
        self.profile_summary_caption.grid(row=row, column=0, sticky="w", pady=(8, 0))
        self.profile_summary_label = ttk.Label(
            strategy_section,
            textvariable=self.profile_summary_text,
            justify="left",
            foreground="#57606a",
        )
        self.profile_summary_label.grid(row=row, column=1, columnspan=3, sticky="ew", pady=(8, 0))
        self._bind_responsive_wrap(self.profile_summary_label, strategy_section, padding=48, min_wrap=240)

        row += 1
        profile_button_frame = ttk.Frame(strategy_section)
        profile_button_frame.grid(row=row, column=1, columnspan=3, sticky="e", pady=(8, 0))
        self.import_profile_button = ttk.Button(
            profile_button_frame,
            text="导入参数模板",
            command=self.import_backtest_profile_bundle,
        )
        self.import_profile_button.pack(side=LEFT, padx=(0, 6))
        self.clear_profile_button = ttk.Button(
            profile_button_frame,
            text="清除模板来源",
            command=self.clear_backtest_profile_origin,
        )
        self.clear_profile_button.pack(side=LEFT)

        row = 0
        self.ema_period_caption = ttk.Label(signal_section, text="快线均线")
        self.ema_period_caption.grid(row=row, column=0, sticky="w")
        self.ema_period_frame = ttk.Frame(signal_section)
        self.ema_period_frame.grid(row=row, column=1, sticky="ew", padx=(0, 8))
        self.ema_period_frame.columnconfigure(1, weight=1)
        self.ema_type_combo = ttk.Combobox(
            self.ema_period_frame,
            textvariable=self.ema_type,
            values=MOVING_AVERAGE_TYPE_OPTIONS,
            state="readonly",
            width=6,
        )
        self.ema_type_combo.grid(row=0, column=0, sticky="w", padx=(0, 6))
        self.ema_period_entry = ttk.Entry(self.ema_period_frame, textvariable=self.ema_period)
        self.ema_period_entry.grid(row=0, column=1, sticky="ew")
        self.trend_ema_period_caption = ttk.Label(signal_section, text="趋势均线")
        self.trend_ema_period_caption.grid(row=row, column=2, sticky="w")
        self.trend_ema_period_frame = ttk.Frame(signal_section)
        self.trend_ema_period_frame.grid(row=row, column=3, sticky="ew")
        self.trend_ema_period_frame.columnconfigure(1, weight=1)
        self.trend_ema_type_combo = ttk.Combobox(
            self.trend_ema_period_frame,
            textvariable=self.trend_ema_type,
            values=MOVING_AVERAGE_TYPE_OPTIONS,
            state="readonly",
            width=6,
        )
        self.trend_ema_type_combo.grid(row=0, column=0, sticky="w", padx=(0, 6))
        self.trend_ema_period_entry = ttk.Entry(self.trend_ema_period_frame, textvariable=self.trend_ema_period)
        self.trend_ema_period_entry.grid(row=0, column=1, sticky="ew")

        row += 1
        self.big_ema_caption = ttk.Label(signal_section, text="大周期均线")
        self.big_ema_caption.grid(row=row, column=0, sticky="w", pady=(8, 0))
        self.big_ema_entry = ttk.Entry(signal_section, textvariable=self.big_ema_period)
        self.big_ema_entry.grid(row=row, column=1, sticky="ew", padx=(0, 8), pady=(8, 0))
        self.atr_period_caption = ttk.Label(signal_section, text="ATR 周期")
        self.atr_period_caption.grid(row=row, column=2, sticky="w", pady=(8, 0))
        self.atr_period_entry = ttk.Entry(signal_section, textvariable=self.atr_period)
        self.atr_period_entry.grid(row=row, column=3, sticky="ew", pady=(8, 0))

        row += 1
        self.stop_atr_caption = ttk.Label(signal_section, text="止损 ATR 倍数")
        self.stop_atr_caption.grid(row=row, column=0, sticky="w", pady=(8, 0))
        self.stop_atr_entry = ttk.Entry(signal_section, textvariable=self.stop_atr)
        self.stop_atr_entry.grid(row=row, column=1, sticky="ew", padx=(0, 8), pady=(8, 0))
        self.take_atr_caption = ttk.Label(signal_section, text="止盈 ATR 倍数")
        self.take_atr_caption.grid(row=row, column=2, sticky="w", pady=(8, 0))
        self.take_atr_entry = ttk.Entry(signal_section, textvariable=self.take_atr)
        self.take_atr_entry.grid(row=row, column=3, sticky="ew", pady=(8, 0))

        row += 1
        self.signal_caption = ttk.Label(signal_section, text="信号方向")
        self.signal_caption.grid(row=row, column=0, sticky="w", pady=(8, 0))
        self.signal_combo = ttk.Combobox(signal_section, textvariable=self.signal_mode_label, state="readonly")
        self.signal_combo.grid(row=row, column=1, sticky="ew", padx=(0, 8), pady=(8, 0))
        self.entry_reference_ema_caption = ttk.Label(signal_section, text="挂单参考线")
        self.entry_reference_ema_caption.grid(row=row, column=2, sticky="w", pady=(8, 0))
        self.entry_reference_ema_frame = ttk.Frame(signal_section)
        self.entry_reference_ema_frame.grid(row=row, column=3, sticky="ew", pady=(8, 0))
        self.entry_reference_ema_frame.columnconfigure(1, weight=1)
        self.entry_reference_ema_type_combo = ttk.Combobox(
            self.entry_reference_ema_frame,
            textvariable=self.entry_reference_ema_type,
            values=MOVING_AVERAGE_TYPE_OPTIONS,
            state="readonly",
            width=6,
        )
        self.entry_reference_ema_type_combo.grid(row=0, column=0, sticky="w", padx=(0, 6))
        self.entry_reference_ema_entry = ttk.Entry(
            self.entry_reference_ema_frame,
            textvariable=self.entry_reference_ema_period,
        )
        self.entry_reference_ema_entry.grid(row=0, column=1, sticky="ew")

        row += 1
        self.take_profit_mode_caption = ttk.Label(signal_section, text="止盈方式")
        self.take_profit_mode_caption.grid(row=row, column=0, sticky="w", pady=(8, 0))
        self.take_profit_mode_combo = ttk.Combobox(
            signal_section,
            textvariable=self.take_profit_mode_label,
            values=list(TAKE_PROFIT_MODE_OPTIONS.keys()),
            state="readonly",
        )
        self.take_profit_mode_combo.grid(row=row, column=1, sticky="ew", padx=(0, 8), pady=(8, 0))
        self.take_profit_mode_combo.bind("<<ComboboxSelected>>", lambda *_: self._sync_dynamic_take_profit_controls())
        self.max_entries_caption = ttk.Label(signal_section, text="每波最多开仓次数")
        self.max_entries_caption.grid(row=row, column=2, sticky="w", pady=(8, 0))
        self.max_entries_entry = ttk.Entry(signal_section, textvariable=self.max_entries_per_trend)
        self.max_entries_entry.grid(row=row, column=3, sticky="ew", pady=(8, 0))

        row += 1
        self.reentry_confirmation_check = ttk.Checkbutton(
            signal_section,
            text="再开仓确认",
            variable=self.reentry_confirmation_enabled,
            command=self._sync_dynamic_take_profit_controls,
        )
        self.reentry_confirmation_check.grid(row=row, column=0, sticky="w", pady=(8, 0))
        self.reentry_confirmation_min_sequence_caption = ttk.Label(signal_section, text="从第")
        self.reentry_confirmation_min_sequence_caption.grid(row=row, column=1, sticky="w", padx=(0, 8), pady=(8, 0))
        self.reentry_confirmation_min_sequence_entry = ttk.Entry(
            signal_section,
            textvariable=self.reentry_confirmation_min_sequence,
            width=6,
        )
        self.reentry_confirmation_min_sequence_entry.grid(row=row, column=2, sticky="w", pady=(8, 0))
        self.reentry_confirmation_ma_frame = ttk.Frame(signal_section)
        self.reentry_confirmation_ma_frame.grid(row=row, column=3, sticky="ew", pady=(8, 0))
        self.reentry_confirmation_rule_prefix = ttk.Label(
            self.reentry_confirmation_ma_frame,
            text="次起，确认K收盘站上",
        )
        self.reentry_confirmation_rule_prefix.grid(row=0, column=0, sticky="w", padx=(0, 6))
        self.reentry_confirmation_ma_type_combo = ttk.Combobox(
            self.reentry_confirmation_ma_frame,
            textvariable=self.reentry_confirmation_ma_type,
            values=MOVING_AVERAGE_TYPE_OPTIONS,
            state="readonly",
            width=6,
        )
        self.reentry_confirmation_ma_type_combo.grid(row=0, column=1, sticky="w", padx=(0, 6))
        self.reentry_confirmation_ma_period_entry = ttk.Entry(
            self.reentry_confirmation_ma_frame,
            textvariable=self.reentry_confirmation_ma_period,
            width=6,
        )
        self.reentry_confirmation_ma_period_entry.grid(row=0, column=2, sticky="w", padx=(0, 6))
        self.reentry_confirmation_rule_suffix = ttk.Label(
            self.reentry_confirmation_ma_frame,
            text="后才允许再次挂单",
        )
        self.reentry_confirmation_rule_suffix.grid(row=0, column=3, sticky="w")

        row = 0
        self.slope_threshold_caption = ttk.Label(advanced_section, text="开空斜率阈值(负数)")
        self.slope_threshold_caption.grid(row=row, column=0, sticky="w")
        self.slope_threshold_entry = ttk.Entry(advanced_section, textvariable=self.trend_ema_slope_filter_min_ratio)
        self.slope_threshold_entry.grid(row=row, column=1, sticky="ew", padx=(0, 8))
        self.atr_percentile_filter_caption = ttk.Label(advanced_section, text="ATR percentile max")
        self.atr_percentile_filter_caption.grid(row=row, column=2, sticky="w")
        self.atr_percentile_filter_entry = ttk.Entry(advanced_section, textvariable=self.atr_percentile_filter_max)
        self.atr_percentile_filter_entry.grid(row=row, column=3, sticky="ew")

        row += 1
        self.slope_threshold_hint = ttk.Label(
            advanced_section,
            text="填 0 表示只要均线斜率转负就开空；例如填 -0.0005 表示需要更陡的负斜率才开空。",
            foreground="#57606a",
        )
        self.slope_threshold_hint.grid(row=row, column=0, columnspan=4, sticky="w", pady=(4, 0))
        self._bind_responsive_wrap(self.slope_threshold_hint, advanced_section, padding=36, min_wrap=220)

        row += 1
        self.ema55_slope_negative_entry_bars_caption = ttk.Label(
            advanced_section,
            text="连续负斜率根数",
        )
        self.ema55_slope_negative_entry_bars_caption.grid(row=row, column=0, sticky="w", pady=(8, 0))
        self.ema55_slope_negative_entry_bars_entry = ttk.Entry(
            advanced_section,
            textvariable=self.ema55_slope_negative_entry_bars,
        )
        self.ema55_slope_negative_entry_bars_entry.grid(row=row, column=1, sticky="ew", padx=(0, 8), pady=(8, 0))

        row += 1
        self.ema55_slope_negative_entry_bars_hint = ttk.Label(
            advanced_section,
            text="仅 BTC EMA55 斜率做空使用；只有当 EMA55 先为非负斜率，再连续 N 根负斜率同时满足阈值时才开空。",
            foreground="#57606a",
        )
        self.ema55_slope_negative_entry_bars_hint.grid(row=row, column=0, columnspan=4, sticky="w", pady=(4, 0))
        self._bind_responsive_wrap(self.ema55_slope_negative_entry_bars_hint, advanced_section, padding=36, min_wrap=220)

        row += 1
        self.mtf_filter_bar_caption = ttk.Label(advanced_section, text="高周期K线")
        self.mtf_filter_bar_caption.grid(row=row, column=0, sticky="w", pady=(8, 0))
        self.body_retest_breakdown_caption = ttk.Label(advanced_section, text="Breakdown ATR")
        self.body_retest_breakdown_caption.grid(row=row, column=2, sticky="w", pady=(8, 0))
        self.body_retest_breakdown_entry = ttk.Entry(
            advanced_section,
            textvariable=self.body_retest_breakdown_atr_multiplier,
        )
        self.body_retest_breakdown_entry.grid(row=row, column=3, sticky="ew", pady=(8, 0))

        row += 1
        self.body_retest_retest_caption = ttk.Label(advanced_section, text="Retest ATR")
        self.body_retest_retest_caption.grid(row=row, column=0, sticky="w", pady=(8, 0))
        self.body_retest_retest_entry = ttk.Entry(advanced_section, textvariable=self.body_retest_retest_atr_multiplier)
        self.body_retest_retest_entry.grid(row=row, column=1, sticky="ew", padx=(0, 8), pady=(8, 0))
        self.body_retest_stop_buffer_caption = ttk.Label(advanced_section, text="Stop buffer ATR")
        self.body_retest_stop_buffer_caption.grid(row=row, column=2, sticky="w", pady=(8, 0))
        self.body_retest_stop_buffer_entry = ttk.Entry(
            advanced_section,
            textvariable=self.body_retest_stop_buffer_atr_multiplier,
        )
        self.body_retest_stop_buffer_entry.grid(row=row, column=3, sticky="ew", pady=(8, 0))

        row += 1
        self.body_retest_body_limit_caption = ttk.Label(advanced_section, text="Body ATR limit")
        self.body_retest_body_limit_caption.grid(row=row, column=0, sticky="w", pady=(8, 0))
        self.body_retest_body_limit_entry = ttk.Entry(advanced_section, textvariable=self.body_retest_body_atr_limit)
        self.body_retest_body_limit_entry.grid(row=row, column=1, sticky="ew", padx=(0, 8), pady=(8, 0))
        self.body_retest_watch_bars_caption = ttk.Label(advanced_section, text="Watch bars")
        self.body_retest_watch_bars_caption.grid(row=row, column=2, sticky="w", pady=(8, 0))
        self.body_retest_watch_bars_entry = ttk.Entry(advanced_section, textvariable=self.body_retest_watch_bars)
        self.body_retest_watch_bars_entry.grid(row=row, column=3, sticky="ew", pady=(8, 0))

        row += 1
        self.mtf_filter_bar_caption_row = ttk.Label(advanced_section, text="Higher TF bar")
        self.mtf_filter_bar_caption_row.grid(row=row, column=0, sticky="w", pady=(8, 0))
        self.mtf_filter_bar_combo = ttk.Combobox(
            advanced_section,
            textvariable=self.mtf_filter_bar,
            values=list(BACKTEST_BAR_LABEL_TO_VALUE.keys()),
            state="readonly",
        )
        self.mtf_filter_bar_combo.grid(row=row, column=1, sticky="ew", padx=(0, 8), pady=(8, 0))
        self.mtf_filter_fast_ema_caption = ttk.Label(advanced_section, text="高周期快EMA")
        self.mtf_filter_fast_ema_caption.grid(row=row, column=2, sticky="w", pady=(8, 0))
        self.mtf_filter_fast_ema_entry = ttk.Entry(advanced_section, textvariable=self.mtf_filter_fast_ema_period)
        self.mtf_filter_fast_ema_entry.grid(row=row, column=3, sticky="ew", pady=(8, 0))

        row += 1
        self.mtf_filter_slow_ema_caption = ttk.Label(advanced_section, text="高周期慢EMA")
        self.mtf_filter_slow_ema_caption.grid(row=row, column=0, sticky="w", pady=(8, 0))
        self.mtf_filter_slow_ema_entry = ttk.Entry(advanced_section, textvariable=self.mtf_filter_slow_ema_period)
        self.mtf_filter_slow_ema_entry.grid(row=row, column=1, sticky="ew", padx=(0, 8), pady=(8, 0))
        self.mtf_reversal_mode_caption = ttk.Label(advanced_section, text="高周期反向处理")
        self.mtf_reversal_mode_caption.grid(row=row, column=2, sticky="w", pady=(8, 0))
        self.mtf_reversal_mode_combo = ttk.Combobox(
            advanced_section,
            textvariable=self.mtf_reversal_mode_label,
            values=list(MTF_REVERSAL_MODE_OPTIONS.keys()),
            state="readonly",
        )
        self.mtf_reversal_mode_combo.grid(row=row, column=3, sticky="ew", pady=(8, 0))

        row += 1
        self.daily_filter_enabled_check = ttk.Checkbutton(
            advanced_section,
            text="启用日线过滤（仅使用当时已收盘的上一根日线）",
            variable=self.daily_filter_enabled,
            command=self._sync_daily_filter_controls,
        )
        self.daily_filter_enabled_check.grid(row=row, column=0, columnspan=4, sticky="w", pady=(8, 0))

        row += 1
        self.daily_filter_boundary_caption = ttk.Label(advanced_section, text="日线标准")
        self.daily_filter_boundary_caption.grid(row=row, column=0, sticky="w", pady=(8, 0))
        self.daily_filter_boundary_combo = ttk.Combobox(
            advanced_section,
            textvariable=self.daily_filter_boundary_label,
            values=list(DAILY_FILTER_BOUNDARY_LABEL_TO_VALUE.keys()),
            state="readonly",
        )
        self.daily_filter_boundary_combo.grid(row=row, column=1, sticky="ew", padx=(0, 8), pady=(8, 0))
        self.daily_filter_scope_caption = ttk.Label(advanced_section, text="过滤方向")
        self.daily_filter_scope_caption.grid(row=row, column=2, sticky="w", pady=(8, 0))
        self.daily_filter_scope_combo = ttk.Combobox(
            advanced_section,
            textvariable=self.daily_filter_scope_label,
            values=list(DAILY_FILTER_SCOPE_LABEL_TO_VALUE.keys()),
            state="readonly",
        )
        self.daily_filter_scope_combo.grid(row=row, column=3, sticky="ew", pady=(8, 0))

        row += 1
        self.daily_filter_mode_caption = ttk.Label(advanced_section, text="过滤规则")
        self.daily_filter_mode_caption.grid(row=row, column=0, sticky="w", pady=(8, 0))
        self.daily_filter_mode_combo = ttk.Combobox(
            advanced_section,
            textvariable=self.daily_filter_mode_label,
            values=list(DAILY_FILTER_MODE_LABEL_TO_VALUE.keys()),
            state="readonly",
        )
        self.daily_filter_mode_combo.grid(row=row, column=1, sticky="ew", padx=(0, 8), pady=(8, 0))
        self.daily_filter_mode_combo.bind("<<ComboboxSelected>>", lambda *_: self._sync_daily_filter_controls())
        self.daily_filter_ma_caption = ttk.Label(advanced_section, text="均线类型")
        self.daily_filter_ma_caption.grid(row=row, column=2, sticky="w", pady=(8, 0))
        self.daily_filter_ma_combo = ttk.Combobox(
            advanced_section,
            textvariable=self.daily_filter_ma_type,
            values=MOVING_AVERAGE_TYPE_OPTIONS,
            state="readonly",
            width=6,
        )
        self.daily_filter_ma_combo.grid(row=row, column=3, sticky="ew", pady=(8, 0))

        row += 1
        self.daily_filter_period_caption = ttk.Label(advanced_section, text="均线周期")
        self.daily_filter_period_caption.grid(row=row, column=0, sticky="w", pady=(8, 0))
        self.daily_filter_period_entry = ttk.Entry(advanced_section, textvariable=self.daily_filter_period)
        self.daily_filter_period_entry.grid(row=row, column=1, sticky="ew", padx=(0, 8), pady=(8, 0))

        row += 1
        self.daily_filter_hint = ttk.Label(
            advanced_section,
            text="北京时间0点/8点日线会从 1H 已收盘K线重采样；交易所1D 直接使用 OKX 已收盘日线。",
            foreground="#57606a",
        )
        self.daily_filter_hint.grid(row=row, column=0, columnspan=4, sticky="w", pady=(4, 0))
        self._bind_responsive_wrap(self.daily_filter_hint, advanced_section, padding=36, min_wrap=220)

        row = 0
        self.dynamic_two_r_break_even_check = ttk.Checkbutton(
            dynamic_section,
            text="启用保本（达到保本触发R时先移到保本位）",
            variable=self.dynamic_two_r_break_even,
        )
        self.dynamic_two_r_break_even_check.grid(row=row, column=0, columnspan=2, sticky="w")
        self.dynamic_break_even_trigger_r_label = ttk.Label(dynamic_section, text="保本触发R")
        self.dynamic_break_even_trigger_r_label.grid(row=row, column=2, sticky="e")
        self.dynamic_break_even_trigger_r_entry = ttk.Entry(
            dynamic_section,
            textvariable=self.dynamic_break_even_trigger_r,
        )
        self.dynamic_break_even_trigger_r_entry.grid(row=row, column=3, sticky="ew", padx=(0, 10))
        self.dynamic_fee_offset_check = ttk.Checkbutton(
            dynamic_section,
            text="启用手续费偏移（按2倍Taker手续费留缓冲）",
            variable=self.dynamic_fee_offset_enabled,
        )
        self.dynamic_fee_offset_check.grid(row=row, column=4, columnspan=2, sticky="w")

        row += 1
        self.dynamic_trailing_start_r_label = ttk.Label(dynamic_section, text="移动止盈触发R")
        self.dynamic_trailing_start_r_label.grid(row=row, column=0, sticky="w", pady=(8, 0))
        self.dynamic_trailing_start_r_entry = ttk.Entry(
            dynamic_section,
            textvariable=self.ema55_slope_lock_profit_trigger_r,
        )
        self.dynamic_trailing_start_r_entry.grid(row=row, column=1, sticky="ew", padx=(0, 10), pady=(8, 0))
        self.dynamic_first_lock_r_label = ttk.Label(dynamic_section, text="首档锁盈R")
        self.dynamic_first_lock_r_label.grid(row=row, column=2, sticky="e", pady=(8, 0))
        self.dynamic_first_lock_r_entry = ttk.Entry(
            dynamic_section,
            textvariable=self.dynamic_first_lock_r,
        )
        self.dynamic_first_lock_r_entry.grid(row=row, column=3, sticky="ew", padx=(0, 10), pady=(8, 0))
        self.dynamic_trailing_step_r_label = ttk.Label(dynamic_section, text="移动步长R")
        self.dynamic_trailing_step_r_label.grid(row=row, column=4, sticky="e", pady=(8, 0))
        self.dynamic_trailing_step_r_entry = ttk.Entry(
            dynamic_section,
            textvariable=self.dynamic_trailing_step_r,
        )
        self.dynamic_trailing_step_r_entry.grid(row=row, column=5, sticky="ew", pady=(8, 0))

        row += 1
        self.dynamic_fee_offset_hint_label = ttk.Label(
            dynamic_section,
            text="提示：保本位是否叠加手续费偏移，由下方开关决定；大部分组合开启更优，默认建议开启。",
        )
        self.dynamic_fee_offset_hint_label.grid(row=row, column=0, columnspan=6, sticky="w", pady=(4, 0))
        self._bind_responsive_wrap(self.dynamic_fee_offset_hint_label, dynamic_section, padding=36, min_wrap=360)

        row += 1
        self.time_stop_break_even_check = ttk.Checkbutton(
            dynamic_section,
            text="启用时间保本（持仓满指定K线且已达到净保本时，上移到保本位）",
            variable=self.time_stop_break_even_enabled,
            command=self._sync_dynamic_take_profit_controls,
        )
        self.time_stop_break_even_check.grid(row=row, column=0, columnspan=2, sticky="w", pady=(8, 0))
        self.time_stop_break_even_bars_label = ttk.Label(dynamic_section, text="时间保本K线数")
        self.time_stop_break_even_bars_label.grid(row=row, column=2, sticky="e", pady=(8, 0))
        self.time_stop_break_even_bars_entry = ttk.Entry(dynamic_section, textvariable=self.time_stop_break_even_bars)
        self.time_stop_break_even_bars_entry.grid(row=row, column=3, sticky="ew", padx=(0, 10), pady=(8, 0))
        self.trend_ema_close_exit_after_trigger_r_enabled_check = ttk.Checkbutton(
            dynamic_section,
            text="达到 nR 后，收盘跌破趋势 EMA 平仓",
            variable=self.trend_ema_close_exit_after_trigger_r_enabled,
            command=self._sync_dynamic_take_profit_controls,
        )
        self.trend_ema_close_exit_after_trigger_r_enabled_check.grid(row=row, column=4, sticky="w", pady=(8, 0))
        self.trend_ema_close_exit_after_trigger_r_label = ttk.Label(dynamic_section, text="趋势EMA平仓触发R")
        self.trend_ema_close_exit_after_trigger_r_label.grid(row=row, column=5, sticky="e", pady=(8, 0))
        self.trend_ema_close_exit_after_trigger_r_entry = ttk.Entry(
            dynamic_section,
            textvariable=self.trend_ema_close_exit_after_trigger_r,
        )
        self.trend_ema_close_exit_after_trigger_r_entry.grid(row=row, column=6, sticky="ew", padx=(0, 10), pady=(8, 0))
        self.ema55_slope_exit_conditions_caption = ttk.Label(dynamic_section, text="平仓条件")
        self.ema55_slope_exit_conditions_caption.grid(row=row, column=7, sticky="e", pady=(8, 0))
        self.ema55_slope_exit_enabled_check = ttk.Checkbutton(
            dynamic_section,
            text="信号均线斜率重新转正时，按收盘价平仓",
            variable=self.ema55_slope_exit_enabled,
        )
        self.ema55_slope_exit_enabled_check.grid(row=row, column=8, sticky="w", pady=(8, 0))

        row += 1
        self.trend_ema_close_exit_after_trigger_r_hint_label = ttk.Label(
            dynamic_section,
            text="趋势 EMA 随上方趋势均线同步",
            foreground="#57606a",
        )
        self.trend_ema_close_exit_after_trigger_r_hint_label.grid(
            row=row,
            column=4,
            columnspan=3,
            sticky="w",
            pady=(2, 0),
        )

        row += 1
        self.ema55_slope_lock_profit_enabled_check = ttk.Checkbutton(
            dynamic_section,
            text="启用 N R 锁盈利 + 双向手续费",
            variable=self.ema55_slope_lock_profit_enabled,
            command=self._sync_ema55_slope_exit_condition_controls,
        )
        self.ema55_slope_lock_profit_enabled_check.grid(row=row, column=0, columnspan=2, sticky="w", pady=(8, 0))
        self.ema55_slope_lock_profit_trigger_r_label = ttk.Label(dynamic_section, text="锁盈利触发R")
        self.ema55_slope_lock_profit_trigger_r_label.grid(row=row, column=2, sticky="e", pady=(8, 0))
        self.ema55_slope_lock_profit_trigger_r_entry = ttk.Entry(
            dynamic_section,
            textvariable=self.ema55_slope_lock_profit_trigger_r,
        )
        self.ema55_slope_lock_profit_trigger_r_entry.grid(row=row, column=3, sticky="ew", padx=(0, 10), pady=(8, 0))
        self.hold_close_exit_bars_caption = ttk.Label(dynamic_section, text="满N根K线收盘价平仓")
        self.hold_close_exit_bars_caption.grid(row=row, column=4, sticky="e", pady=(8, 0))
        self.hold_close_exit_bars_entry = ttk.Entry(dynamic_section, textvariable=self.hold_close_exit_bars)
        self.hold_close_exit_bars_entry.grid(row=row, column=5, sticky="ew", pady=(8, 0))

        row += 1
        self.ema55_slope_exit_conditions_hint = ttk.Label(
            dynamic_section,
            text="锁盈利规则：价格先到 N R，再把止损上移到 (N-1)R + 双向 Taker 手续费；后续每新增 1R，继续逐级上移。",
            foreground="#57606a",
        )
        self.ema55_slope_exit_conditions_hint.grid(row=row, column=0, columnspan=4, sticky="w", pady=(4, 0))
        self.hold_close_exit_hint = ttk.Label(
            dynamic_section,
            text="填0关闭；从开仓K线索引起计满N根已收盘K线后，当根按收盘价平仓。",
            foreground="#57606a",
        )
        self.hold_close_exit_hint.grid(row=row, column=4, columnspan=2, sticky="w", pady=(4, 0))
        self._bind_responsive_wrap(self.ema55_slope_exit_conditions_hint, dynamic_section, padding=36, min_wrap=280)
        self._bind_responsive_wrap(self.hold_close_exit_hint, dynamic_section, padding=36, min_wrap=220)

        row += 1
        self._dynamic_protection_rules_card = ttk.LabelFrame(dynamic_section, text="动态保护规则", padding=(10, 8))
        self._dynamic_protection_rules_card.grid(row=row, column=0, columnspan=6, sticky="ew", pady=(8, 0))
        self._dynamic_protection_rules_card.columnconfigure(0, weight=1)
        header_frame = ttk.Frame(self._dynamic_protection_rules_card)
        header_frame.grid(row=0, column=0, sticky="ew")
        for column, text in enumerate(("触发R", "动作", "锁到R", "递进", "每隔R", "每次加R", "操作")):
            ttk.Label(header_frame, text=text).grid(row=0, column=column, sticky="w", padx=(0, 6))
        self._dynamic_protection_rules_frame = ttk.Frame(self._dynamic_protection_rules_card)
        self._dynamic_protection_rules_frame.grid(row=1, column=0, sticky="ew", pady=(4, 0))
        footer_frame = ttk.Frame(self._dynamic_protection_rules_card)
        footer_frame.grid(row=2, column=0, sticky="w", pady=(8, 0))
        self._dynamic_protection_add_rule_button = ttk.Button(
            footer_frame,
            text="新增规则",
            command=lambda: (self._append_dynamic_protection_rule_row(), self._sync_dynamic_protection_rules_json_from_editor()),
        )
        self._dynamic_protection_add_rule_button.grid(row=0, column=0, sticky="w")
        self._dynamic_protection_restore_button = ttk.Button(
            footer_frame,
            text="按当前4字段生成",
            command=self._rebuild_dynamic_protection_rule_editor,
        )
        self._dynamic_protection_restore_button.grid(row=0, column=1, sticky="w", padx=(6, 0))
        self._rebuild_dynamic_protection_rule_editor()

        backtest_boxes = ttk.Frame(backtest_section)
        backtest_boxes.grid(row=0, column=0, sticky="ew")
        backtest_boxes.columnconfigure(0, weight=5)
        backtest_boxes.columnconfigure(1, weight=4)
        backtest_boxes.columnconfigure(2, weight=4)

        funds_box = ttk.LabelFrame(backtest_boxes, text="资金与成本", padding=(10, 8))
        funds_box.grid(row=0, column=0, sticky="nsew", padx=(0, 8))
        _configure_pair_columns(funds_box, 4)

        sample_box = ttk.LabelFrame(backtest_boxes, text="时间与样本", padding=(10, 8))
        sample_box.grid(row=0, column=1, sticky="nsew", padx=(0, 8))
        _configure_pair_columns(sample_box, 4)

        data_box = ttk.LabelFrame(backtest_boxes, text="运行与数据", padding=(10, 8))
        data_box.grid(row=0, column=2, sticky="nsew")
        data_box.columnconfigure(0, weight=1)

        row = 0
        self.size_or_risk_label = ttk.Label(funds_box, text="固定风险金/数量")
        self.size_or_risk_label.grid(row=row, column=0, sticky="w")
        self.size_or_risk_entry = ttk.Entry(funds_box, textvariable=self.risk_amount)
        self.size_or_risk_entry.grid(row=row, column=1, sticky="ew", padx=(0, 8))
        ttk.Label(funds_box, text="Maker手续费(%)").grid(row=row, column=2, sticky="w")
        ttk.Entry(funds_box, textvariable=self.maker_fee_percent).grid(row=row, column=3, sticky="ew")

        row += 1
        ttk.Label(funds_box, text="初始资金").grid(row=row, column=0, sticky="w", pady=(8, 0))
        ttk.Entry(funds_box, textvariable=self.initial_capital).grid(row=row, column=1, sticky="ew", padx=(0, 8), pady=(8, 0))
        ttk.Label(funds_box, text="Taker手续费(%)").grid(row=row, column=2, sticky="w", pady=(8, 0))
        ttk.Entry(funds_box, textvariable=self.taker_fee_percent).grid(row=row, column=3, sticky="ew", pady=(8, 0))

        row += 1
        ttk.Label(funds_box, text="仓位模式").grid(row=row, column=0, sticky="w", pady=(8, 0))
        self.sizing_mode_combo = ttk.Combobox(
            funds_box,
            textvariable=self.sizing_mode_label,
            values=list(BACKTEST_SIZING_OPTIONS.keys()),
            state="readonly",
        )
        self.sizing_mode_combo.grid(row=row, column=1, sticky="ew", padx=(0, 8), pady=(8, 0))
        self.sizing_mode_combo.bind("<<ComboboxSelected>>", lambda *_: self._update_sizing_mode_widgets())
        ttk.Label(funds_box, text="风险百分比(%)").grid(row=row, column=2, sticky="w", pady=(8, 0))
        self.risk_percent_entry = ttk.Entry(funds_box, textvariable=self.risk_percent)
        self.risk_percent_entry.grid(row=row, column=3, sticky="ew", pady=(8, 0))

        row += 1
        self._minimum_order_hint_label = ttk.Label(
            funds_box,
            textvariable=self.minimum_order_hint_text,
            justify="left",
            foreground="#57606a",
        )
        self._minimum_order_hint_label.grid(row=row, column=0, columnspan=4, sticky="w", pady=(4, 0))
        self._bind_responsive_wrap(self._minimum_order_hint_label, funds_box, padding=36, min_wrap=260)

        row += 1
        ttk.Label(funds_box, text="开仓滑点(%)").grid(row=row, column=0, sticky="w", pady=(8, 0))
        ttk.Entry(funds_box, textvariable=self.entry_slippage_percent).grid(
            row=row, column=1, sticky="ew", padx=(0, 8), pady=(8, 0)
        )
        ttk.Label(funds_box, text="平仓滑点(%)").grid(row=row, column=2, sticky="w", pady=(8, 0))
        ttk.Entry(funds_box, textvariable=self.exit_slippage_percent).grid(
            row=row, column=3, sticky="ew", pady=(8, 0)
        )

        row += 1
        ttk.Label(funds_box, text="资金费率/8h(%)").grid(row=row, column=0, sticky="w", pady=(8, 0))
        ttk.Entry(funds_box, textvariable=self.funding_rate_percent).grid(
            row=row, column=1, sticky="ew", padx=(0, 8), pady=(8, 0)
        )
        ttk.Checkbutton(funds_box, text="启用复利", variable=self.compounding_enabled).grid(
            row=row, column=2, columnspan=2, sticky="w", pady=(8, 0)
        )

        row = 0
        ttk.Label(sample_box, text="开始时间").grid(row=row, column=0, sticky="w")
        ttk.Entry(sample_box, textvariable=self.start_time_text).grid(row=row, column=1, sticky="ew", padx=(0, 8))
        ttk.Label(sample_box, text="结束时间").grid(row=row, column=2, sticky="w")
        ttk.Entry(sample_box, textvariable=self.end_time_text).grid(row=row, column=3, sticky="ew")

        row += 1
        ttk.Label(sample_box, text="回测K线数").grid(row=row, column=0, sticky="w", pady=(8, 0))
        ttk.Entry(sample_box, textvariable=self.candle_limit).grid(row=row, column=1, sticky="ew", padx=(0, 8), pady=(8, 0))

        row += 1
        ttk.Label(sample_box, text="时间格式").grid(row=row, column=0, sticky="w", pady=(8, 0))
        ttk.Label(
            sample_box,
            text="支持 YYYYMMDD 或 YYYYMMDD HH:MM",
            foreground="#57606a",
        ).grid(row=row, column=1, columnspan=3, sticky="w", pady=(8, 0))

        row += 1
        self._candle_limit_hint_label = ttk.Label(
            sample_box,
            text="填 0 = 全量；填 10000 = 最新往前 10000 根；正数上限 10000。",
            justify="left",
        )
        self._candle_limit_hint_label.grid(row=row, column=0, columnspan=4, sticky="w", pady=(4, 0))
        self._bind_responsive_wrap(self._candle_limit_hint_label, sample_box, padding=36, min_wrap=240)

        row = 0
        ttk.Checkbutton(data_box, text="纯本地回测（不补拉）", variable=self.pure_local_backtest).grid(
            row=row, column=0, sticky="w"
        )

        row += 1
        history_btn_frame = ttk.Frame(data_box)
        history_btn_frame.grid(row=row, column=0, sticky="w", pady=(8, 0))
        ttk.Label(history_btn_frame, text="同步周期").pack(side=LEFT)
        for bar in BACKTEST_HISTORY_SYNC_BARS:
            ttk.Checkbutton(history_btn_frame, text=bar, variable=self.sync_history_bar_vars[bar]).pack(
                side=LEFT, padx=(6, 0)
            )

        row += 1
        data_actions = ttk.Frame(data_box)
        data_actions.grid(row=row, column=0, sticky="w", pady=(8, 0))
        self.sync_history_button = ttk.Button(data_actions, text="同步历史数据", command=self.sync_history_data)
        self.sync_history_button.pack(side=LEFT, padx=(0, 6))
        self.sync_metadata_button = ttk.Button(
            data_actions,
            text="同步价格精度/下单规则",
            command=self.sync_instrument_metadata,
        )
        self.sync_metadata_button.pack(side=LEFT, padx=(0, 6))
        self.verify_cache_button = ttk.Button(data_actions, text="校验数据", command=self.verify_history_cache_data)
        self.verify_cache_button.pack(side=LEFT)

        actions_bar = ttk.Frame(backtest_section)
        actions_bar.grid(row=1, column=0, sticky="ew", pady=(10, 0))
        actions_bar.columnconfigure(0, weight=1)
        self._backtest_action_summary = StringVar(value="")
        self._backtest_action_summary_label = ttk.Label(
            actions_bar,
            textvariable=self._backtest_action_summary,
            justify="left",
            anchor="w",
        )
        self._backtest_action_summary_label.grid(row=0, column=0, sticky="ew", padx=(0, 12))
        run_btn_frame = ttk.Frame(actions_bar)
        run_btn_frame.grid(row=0, column=1, sticky="e")
        self.single_backtest_button = ttk.Button(
            run_btn_frame,
            text="\u5f53\u524d\u53c2\u6570\u5355\u7ec4\u56de\u6d4b",
            command=self.start_single_backtest,
        )
        self.single_backtest_button.pack(side=LEFT, padx=(0, 8))
        self.batch_backtest_button = ttk.Button(run_btn_frame, text="开始回测", command=self.start_backtest)
        self.batch_backtest_button.pack(side=LEFT)
        self._refresh_backtest_action_summary()

        content_frame = ttk.Frame(main_pane)
        content_frame.columnconfigure(0, weight=1)
        content_frame.rowconfigure(0, weight=1)

        report_frame = self._build_backtest_panedwindow(content_frame, orient="horizontal")

        summary_frame = ttk.LabelFrame(report_frame, text="回测报告", padding=12)
        trades_frame = ttk.LabelFrame(report_frame, text="交易明细", padding=12)
        report_frame.add(summary_frame, stretch="always")
        report_frame.add(trades_frame, stretch="always")
        report_frame.grid(row=0, column=0, sticky="nsew")

        summary_frame.columnconfigure(0, weight=1)
        summary_frame.rowconfigure(1, weight=1)
        trades_frame.columnconfigure(0, weight=1)
        trades_frame.rowconfigure(0, weight=1)

        self._report_summary_label = ttk.Label(summary_frame, textvariable=self.report_summary, justify="left")
        self._report_summary_label.grid(row=0, column=0, sticky="ew", pady=(0, 10))
        self._bind_responsive_wrap(self._report_summary_label, summary_frame, padding=28, min_wrap=280)
        self.chart_zoom_button = Button(
            summary_frame,
            text="回测K线图",
            command=self.open_chart_zoom_window,
            bg=CHART_ACTION_BUTTON_BG,
            activebackground=CHART_ACTION_BUTTON_ACTIVE_BG,
            fg=CHART_ACTION_BUTTON_FG,
            activeforeground=CHART_ACTION_BUTTON_FG,
            relief="raised",
            bd=1,
            cursor="hand2",
            padx=8,
            pady=1,
        )
        self.chart_zoom_button.grid(row=0, column=1, sticky="e", padx=(8, 0), pady=(0, 10))

        report_notebook = ttk.Notebook(summary_frame)
        report_notebook.grid(row=1, column=0, columnspan=2, sticky="nsew")

        report_tab = ttk.Frame(report_notebook, padding=8)
        report_tab.columnconfigure(0, weight=1)
        report_tab.rowconfigure(0, weight=1)
        report_scroll_y = ttk.Scrollbar(report_tab, orient="vertical")
        self.report_text = Text(report_tab, height=11, wrap="word", font=("Consolas", 10))
        self.report_text.configure(yscrollcommand=report_scroll_y.set)
        self.report_text.grid(row=0, column=0, sticky="nsew")
        report_scroll_y.configure(command=self.report_text.yview)
        report_scroll_y.grid(row=0, column=1, sticky="ns")
        report_notebook.add(report_tab, text="当前报告")

        compare_tab = ttk.Frame(report_notebook, padding=8)
        compare_tab.columnconfigure(0, weight=1)
        compare_tab.rowconfigure(1, weight=1)
        compare_tab.rowconfigure(2, weight=1)

        compare_toolbar = ttk.Frame(compare_tab)
        compare_toolbar.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        compare_toolbar.columnconfigure(2, weight=1)
        ttk.Label(compare_toolbar, textvariable=self.compare_summary, justify="left").grid(row=0, column=0, sticky="w")
        ttk.Label(compare_toolbar, text="筛选").grid(row=0, column=1, sticky="e", padx=(12, 6))
        compare_filter_entry = ttk.Entry(compare_toolbar, textvariable=self.compare_filter_keyword, width=30)
        compare_filter_entry.grid(row=0, column=2, sticky="ew")
        compare_filter_entry.bind("<Return>", lambda *_: self._render_compare_tree())
        ttk.Button(compare_toolbar, text="应用筛选", command=self._render_compare_tree).grid(row=0, column=3, sticky="e", padx=(8, 6))
        ttk.Button(compare_toolbar, text="清空筛选", command=self._clear_compare_filter).grid(row=0, column=4, sticky="e", padx=(0, 8))
        ttk.Button(compare_toolbar, text="加载所选", command=self.load_selected_snapshot).grid(row=0, column=5, sticky="e", padx=(0, 8))
        ttk.Button(compare_toolbar, text="清空记录", command=self.clear_backtest_snapshots).grid(row=0, column=6, sticky="e")

        compare_tree_frame = ttk.Frame(compare_tab)
        compare_tree_frame.grid(row=1, column=0, sticky="nsew")
        compare_tree_frame.columnconfigure(0, weight=1)
        compare_tree_frame.rowconfigure(0, weight=1)

        self.compare_tree = ttk.Treeview(
            compare_tree_frame,
            columns=("id", "archive_id", "time", "strategy", "symbol", "period", "params", "trades", "win_rate", "pnl", "drawdown"),
            show="headings",
            selectmode="browse",
        )
        _configure_backtest_compare_tree(self.compare_tree, id_heading="运行编号", include_archive_column=True)
        compare_tree_yscroll = ttk.Scrollbar(compare_tree_frame, orient="vertical", command=self.compare_tree.yview)
        compare_tree_xscroll = ttk.Scrollbar(compare_tree_frame, orient="horizontal", command=self.compare_tree.xview)
        self.compare_tree.configure(yscrollcommand=compare_tree_yscroll.set, xscrollcommand=compare_tree_xscroll.set)
        self.compare_tree.grid(row=0, column=0, sticky="nsew")
        compare_tree_yscroll.grid(row=0, column=1, sticky="ns")
        compare_tree_xscroll.grid(row=1, column=0, sticky="ew")
        self.compare_tree.bind("<<TreeviewSelect>>", self._on_compare_tree_selected)
        self.compare_tree.bind("<Double-Button-1>", lambda *_: self.open_chart_zoom_window())

        compare_detail_scroll_y = ttk.Scrollbar(compare_tab, orient="vertical")
        self.compare_detail_text = Text(compare_tab, height=6, wrap="word", font=("Consolas", 10))
        self.compare_detail_text.configure(yscrollcommand=compare_detail_scroll_y.set)
        self.compare_detail_text.grid(row=2, column=0, sticky="nsew", pady=(8, 0))
        compare_detail_scroll_y.configure(command=self.compare_detail_text.yview)
        compare_detail_scroll_y.grid(row=2, column=1, sticky="ns", pady=(8, 0))
        report_notebook.add(compare_tab, text="回测对比")

        matrix_tab = ttk.Frame(report_notebook, padding=8)
        matrix_tab.columnconfigure(0, weight=1)
        matrix_tab.rowconfigure(2, weight=1)
        matrix_toolbar = ttk.Frame(matrix_tab)
        matrix_toolbar.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        matrix_toolbar.columnconfigure(4, weight=1)
        self.matrix_layer_caption = ttk.Label(matrix_toolbar, text="开仓次数层")
        self.matrix_layer_caption.grid(row=0, column=0, sticky="w")
        self.matrix_layer_combo = ttk.Combobox(
            matrix_toolbar,
            textvariable=self.batch_entries_layer_label,
            values=[_batch_entries_label(value) for value in BATCH_MAX_ENTRIES_OPTIONS],
            state="readonly",
            width=12,
        )
        self.matrix_layer_combo.grid(row=0, column=1, sticky="w", padx=(8, 0))
        self.matrix_layer_combo.bind("<<ComboboxSelected>>", lambda *_: self._refresh_current_batch_views())
        ttk.Label(
            matrix_toolbar,
            text="固定止盈按开仓次数分层查看，动态止盈自动切到 SL x 开仓次数矩阵。",
            foreground="#57606a",
        ).grid(row=0, column=4, sticky="e", padx=(12, 0))
        ttk.Label(
            matrix_tab,
            textvariable=self.matrix_summary,
            wraplength=480,
            justify="left",
        ).grid(row=1, column=0, sticky="w", pady=(0, 10))
        matrix_viewport = ttk.Frame(matrix_tab)
        matrix_viewport.grid(row=2, column=0, sticky="nsew")
        matrix_viewport.columnconfigure(0, weight=1)
        matrix_viewport.rowconfigure(0, weight=1)
        self.matrix_canvas = Canvas(matrix_viewport, highlightthickness=0, bd=0, background="#f0f0f0")
        self.matrix_canvas.grid(row=0, column=0, sticky="nsew")
        matrix_scroll_y = ttk.Scrollbar(matrix_viewport, orient="vertical", command=self.matrix_canvas.yview)
        self.matrix_canvas.configure(yscrollcommand=matrix_scroll_y.set)
        matrix_scroll_y.grid(row=0, column=1, sticky="ns")
        self.matrix_grid_frame = ttk.Frame(self.matrix_canvas)
        self._matrix_inner_window_id = self.matrix_canvas.create_window((0, 0), window=self.matrix_grid_frame, anchor="nw")
        self.matrix_grid_frame.bind("<Configure>", lambda _e: self._sync_matrix_grid_viewport())
        self.matrix_canvas.bind("<Configure>", self._sync_matrix_grid_viewport)
        self.window.after_idle(self._sync_matrix_grid_viewport)
        report_notebook.add(matrix_tab, text="\u77e9\u9635\u5bf9\u6bd4")

        heatmap_tab = ttk.Frame(report_notebook, padding=8)
        heatmap_tab.columnconfigure(0, weight=1)
        heatmap_tab.rowconfigure(2, weight=1)
        heatmap_toolbar = ttk.Frame(heatmap_tab)
        heatmap_toolbar.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        heatmap_toolbar.columnconfigure(4, weight=1)
        ttk.Label(heatmap_toolbar, text="热力指标").grid(row=0, column=0, sticky="w")
        heatmap_metric_combo = ttk.Combobox(
            heatmap_toolbar,
            textvariable=self.heatmap_metric,
            values=("总盈亏", "盈亏回撤比", "胜率", "交易数"),
            state="readonly",
            width=16,
        )
        heatmap_metric_combo.grid(row=0, column=1, sticky="w", padx=(8, 0))
        heatmap_metric_combo.bind("<<ComboboxSelected>>", lambda *_: self._show_batch_heatmap(self._current_matrix_batch_label))
        self.heatmap_layer_caption = ttk.Label(heatmap_toolbar, text="开仓次数层")
        self.heatmap_layer_caption.grid(row=0, column=2, sticky="w", padx=(16, 0))
        self.heatmap_layer_combo = ttk.Combobox(
            heatmap_toolbar,
            textvariable=self.batch_entries_layer_label,
            values=[_batch_entries_label(value) for value in BATCH_MAX_ENTRIES_OPTIONS],
            state="readonly",
            width=12,
        )
        self.heatmap_layer_combo.grid(row=0, column=3, sticky="w", padx=(8, 0))
        self.heatmap_layer_combo.bind("<<ComboboxSelected>>", lambda *_: self._refresh_current_batch_views())
        ttk.Label(
            heatmap_toolbar,
            text="单击单元格可切换到对应回测。",
            foreground="#57606a",
        ).grid(row=0, column=4, sticky="e", padx=(12, 0))
        ttk.Label(
            heatmap_tab,
            textvariable=self.heatmap_summary,
            justify="left",
            wraplength=860,
        ).grid(row=1, column=0, sticky="ew", pady=(0, 8))
        heatmap_scroll_y = ttk.Scrollbar(heatmap_tab, orient="vertical")
        self.heatmap_canvas = Canvas(heatmap_tab, background="#ffffff", highlightthickness=0)
        self.heatmap_canvas.configure(yscrollcommand=heatmap_scroll_y.set)
        self.heatmap_canvas.grid(row=2, column=0, sticky="nsew")
        heatmap_scroll_y.configure(command=self.heatmap_canvas.yview)
        heatmap_scroll_y.grid(row=2, column=1, sticky="ns")
        self.heatmap_canvas.bind("<Configure>", lambda *_: self._show_batch_heatmap(self._current_matrix_batch_label))
        report_notebook.add(heatmap_tab, text="参数热力图")

        stats_tab = ttk.Frame(report_notebook, padding=8)
        stats_tab.columnconfigure(0, weight=1)
        stats_tab.rowconfigure(0, weight=1)
        stats_notebook = ttk.Notebook(stats_tab)
        stats_notebook.grid(row=0, column=0, sticky="nsew")

        monthly_tab = ttk.Frame(stats_notebook, padding=8)
        monthly_tab.columnconfigure(0, weight=1)
        monthly_tab.rowconfigure(0, weight=1)
        monthly_scroll_y = ttk.Scrollbar(monthly_tab, orient="vertical")
        self.monthly_stats_tree = ttk.Treeview(
            monthly_tab,
            columns=("period", "trades", "win_rate", "pnl", "return_pct", "drawdown", "drawdown_pct", "end_equity"),
            show="headings",
            selectmode="browse",
        )
        for column, label, width in (
            ("period", "月份", 90),
            ("trades", "交易数", 70),
            ("win_rate", "胜率", 80),
            ("pnl", "总盈亏", 100),
            ("return_pct", "收益率", 90),
            ("drawdown", "最大回撤", 100),
            ("drawdown_pct", "回撤比例", 90),
            ("end_equity", "期末权益", 110),
        ):
            self.monthly_stats_tree.heading(column, text=label)
            self.monthly_stats_tree.column(column, width=width, anchor="e" if column != "period" else "center")
        self.monthly_stats_tree.configure(yscrollcommand=monthly_scroll_y.set)
        self.monthly_stats_tree.grid(row=0, column=0, sticky="nsew")
        monthly_scroll_y.configure(command=self.monthly_stats_tree.yview)
        monthly_scroll_y.grid(row=0, column=1, sticky="ns")
        stats_notebook.add(monthly_tab, text="月度统计")

        yearly_tab = ttk.Frame(stats_notebook, padding=8)
        yearly_tab.columnconfigure(0, weight=1)
        yearly_tab.rowconfigure(0, weight=1)
        yearly_scroll_y = ttk.Scrollbar(yearly_tab, orient="vertical")
        self.yearly_stats_tree = ttk.Treeview(
            yearly_tab,
            columns=("period", "trades", "win_rate", "pnl", "return_pct", "drawdown", "drawdown_pct", "end_equity"),
            show="headings",
            selectmode="browse",
        )
        for column, label, width in (
            ("period", "年份", 90),
            ("trades", "交易数", 70),
            ("win_rate", "胜率", 80),
            ("pnl", "总盈亏", 100),
            ("return_pct", "收益率", 90),
            ("drawdown", "最大回撤", 100),
            ("drawdown_pct", "回撤比例", 90),
            ("end_equity", "期末权益", 110),
        ):
            self.yearly_stats_tree.heading(column, text=label)
            self.yearly_stats_tree.column(column, width=width, anchor="e" if column != "period" else "center")
        self.yearly_stats_tree.configure(yscrollcommand=yearly_scroll_y.set)
        self.yearly_stats_tree.grid(row=0, column=0, sticky="nsew")
        yearly_scroll_y.configure(command=self.yearly_stats_tree.yview)
        yearly_scroll_y.grid(row=0, column=1, sticky="ns")
        stats_notebook.add(yearly_tab, text="年度统计")
        report_notebook.add(stats_tab, text="周期统计")

        trades_notebook = ttk.Notebook(trades_frame)
        trades_notebook.grid(row=0, column=0, sticky="nsew")
        self._trades_notebook = trades_notebook

        trade_tab = ttk.Frame(trades_notebook, padding=8)
        trade_tab.columnconfigure(0, weight=1)
        trade_tab.rowconfigure(0, weight=1)
        self._trade_tab = trade_tab
        manual_tab = ttk.Frame(trades_notebook, padding=8)
        manual_tab.columnconfigure(0, weight=1)
        manual_tab.rowconfigure(2, weight=1)
        self._extension_stats_tab = manual_tab

        trade_tree_frame = ttk.Frame(trade_tab)
        trade_tree_frame.grid(row=0, column=0, sticky="nsew")
        trade_tree_frame.columnconfigure(0, weight=1)
        trade_tree_frame.rowconfigure(0, weight=1)

        self.trade_tree = ttk.Treeview(
            trade_tree_frame,
            columns=("seq", "signal", "entry_time", "entry", "stop", "atr", "size", "exit_time", "exit", "fee", "reason", "pnl", "r"),
            show="headings",
            selectmode="browse",
        )
        self.trade_tree.heading("seq", text="序号")
        self.trade_tree.heading("signal", text="方向")
        self.trade_tree.heading("entry_time", text="进场时间")
        self.trade_tree.heading("entry", text="进场价格")
        self.trade_tree.heading("stop", text="止损值")
        self.trade_tree.heading("atr", text="ATR值")
        self.trade_tree.heading("size", text="开仓数量")
        self.trade_tree.heading("exit_time", text="出场时间")
        self.trade_tree.heading("exit", text="出场价格")
        self.trade_tree.heading("fee", text="手续费")
        self.trade_tree.heading("reason", text="原因")
        self.trade_tree.heading("pnl", text="盈亏")
        self.trade_tree.heading("r", text="R倍数")
        self.trade_tree.column("seq", width=60, anchor="center")
        self.trade_tree.column("signal", width=70, anchor="center")
        self.trade_tree.column("entry_time", width=140, anchor="center")
        self.trade_tree.column("entry", width=110, anchor="e")
        self.trade_tree.column("stop", width=110, anchor="e")
        self.trade_tree.column("atr", width=100, anchor="e")
        self.trade_tree.column("size", width=100, anchor="e")
        self.trade_tree.column("exit_time", width=140, anchor="center")
        self.trade_tree.column("exit", width=110, anchor="e")
        self.trade_tree.column("fee", width=100, anchor="e")
        self.trade_tree.column("reason", width=90, anchor="center")
        self.trade_tree.column("pnl", width=110, anchor="e")
        self.trade_tree.column("r", width=90, anchor="e")
        self.trade_tree.grid(row=0, column=0, sticky="nsew")
        trade_tree_scroll_y = ttk.Scrollbar(trade_tree_frame, orient="vertical", command=self.trade_tree.yview)
        trade_tree_scroll_x = ttk.Scrollbar(trade_tree_frame, orient="horizontal", command=self.trade_tree.xview)
        self.trade_tree.configure(
            yscrollcommand=trade_tree_scroll_y.set,
            xscrollcommand=trade_tree_scroll_x.set,
        )
        trade_tree_scroll_y.grid(row=0, column=1, sticky="ns")
        trade_tree_scroll_x.grid(row=1, column=0, sticky="ew")

        self._manual_summary_label = ttk.Label(manual_tab, textvariable=self.manual_summary, justify="left")
        self._manual_summary_label.grid(row=0, column=0, sticky="w", pady=(0, 10))
        self._bind_responsive_wrap(self._manual_summary_label, manual_tab, padding=28, min_wrap=300)

        manual_toolbar = ttk.Frame(manual_tab)
        manual_toolbar.grid(row=1, column=0, sticky="ew", pady=(0, 8))
        manual_toolbar.columnconfigure(5, weight=1)
        ttk.Label(manual_toolbar, text="筛选").grid(row=0, column=0, sticky="w")
        manual_filter_combo = ttk.Combobox(
            manual_toolbar,
            textvariable=self.manual_filter_label,
            values=list(MANUAL_FILTER_OPTIONS.keys()),
            state="readonly",
            width=12,
        )
        manual_filter_combo.grid(row=0, column=1, sticky="w", padx=(8, 12))
        manual_filter_combo.bind("<<ComboboxSelected>>", lambda *_: self.refresh_manual_tree_view())
        ttk.Label(manual_toolbar, text="排序").grid(row=0, column=2, sticky="w")
        manual_sort_combo = ttk.Combobox(
            manual_toolbar,
            textvariable=self.manual_sort_label,
            values=list(MANUAL_SORT_OPTIONS.keys()),
            state="readonly",
            width=14,
        )
        manual_sort_combo.grid(row=0, column=3, sticky="w", padx=(8, 12))
        manual_sort_combo.bind("<<ComboboxSelected>>", lambda *_: self.refresh_manual_tree_view())
        ttk.Button(manual_toolbar, text="定位选中", command=self.focus_selected_manual_position_on_chart).grid(
            row=0, column=4, sticky="w"
        )
        ttk.Label(
            manual_toolbar,
            text="双击仓位可直接跳到图表对应开仓/移交区间。",
            justify="left",
        ).grid(row=0, column=5, sticky="w", padx=(12, 0))

        manual_tree_frame = ttk.Frame(manual_tab)
        manual_tree_frame.grid(row=2, column=0, sticky="nsew")
        manual_tree_frame.columnconfigure(0, weight=1)
        manual_tree_frame.rowconfigure(0, weight=1)

        self.manual_tree = ttk.Treeview(
            manual_tree_frame,
            columns=(
                "seq",
                "signal",
                "entry_time",
                "handoff_time",
                "age",
                "entry",
                "handoff",
                "current",
                "break_even",
                "gap_value",
                "gap_pct",
                "size",
                "pnl",
                "entry_fee",
                "funding",
                "reason",
            ),
            show="headings",
            selectmode="browse",
        )
        self.manual_tree.heading("seq", text="序号")
        self.manual_tree.heading("signal", text="方向")
        self.manual_tree.heading("entry_time", text="开仓时间")
        self.manual_tree.heading("handoff_time", text="移交时间")
        self.manual_tree.heading("age", text="入池时长")
        self.manual_tree.heading("entry", text="开仓价")
        self.manual_tree.heading("handoff", text="移交价")
        self.manual_tree.heading("current", text="当前价")
        self.manual_tree.heading("break_even", text="保本价")
        self.manual_tree.heading("gap_value", text="距保本价差")
        self.manual_tree.heading("gap_pct", text="距保本")
        self.manual_tree.heading("size", text="数量")
        self.manual_tree.heading("pnl", text="浮盈亏")
        self.manual_tree.heading("entry_fee", text="开仓手续费")
        self.manual_tree.heading("funding", text="资金费")
        self.manual_tree.heading("reason", text="移交原因")
        self.manual_tree.column("seq", width=60, anchor="center")
        self.manual_tree.column("signal", width=70, anchor="center")
        self.manual_tree.column("entry_time", width=140, anchor="center")
        self.manual_tree.column("handoff_time", width=140, anchor="center")
        self.manual_tree.column("age", width=90, anchor="center")
        self.manual_tree.column("entry", width=100, anchor="e")
        self.manual_tree.column("handoff", width=100, anchor="e")
        self.manual_tree.column("current", width=100, anchor="e")
        self.manual_tree.column("break_even", width=100, anchor="e")
        self.manual_tree.column("gap_value", width=100, anchor="e")
        self.manual_tree.column("gap_pct", width=90, anchor="e")
        self.manual_tree.column("size", width=90, anchor="e")
        self.manual_tree.column("pnl", width=110, anchor="e")
        self.manual_tree.column("entry_fee", width=110, anchor="e")
        self.manual_tree.column("funding", width=110, anchor="e")
        self.manual_tree.column("reason", width=220, anchor="w")
        self.manual_tree.tag_configure("manual_profit", foreground="#1a7f37")
        self.manual_tree.tag_configure("manual_profit_near", foreground="#1a7f37", background="#fff3bf")
        self.manual_tree.tag_configure("manual_loss", foreground="#d1242f")
        self.manual_tree.tag_configure("manual_loss_near", foreground="#d1242f", background="#fff3bf")
        self.manual_tree.tag_configure("manual_flat", foreground="#9a6700")
        self.manual_tree.tag_configure("manual_flat_near", foreground="#9a6700", background="#fff3bf")
        self.manual_tree.grid(row=0, column=0, sticky="nsew")
        self.manual_tree.bind("<Double-Button-1>", lambda *_: self.focus_selected_manual_position_on_chart())
        manual_tree_scroll_y = ttk.Scrollbar(manual_tree_frame, orient="vertical", command=self.manual_tree.yview)
        manual_tree_scroll_x = ttk.Scrollbar(manual_tree_frame, orient="horizontal", command=self.manual_tree.xview)
        self.manual_tree.configure(
            yscrollcommand=manual_tree_scroll_y.set,
            xscrollcommand=manual_tree_scroll_x.set,
        )
        manual_tree_scroll_y.grid(row=0, column=1, sticky="ns")
        manual_tree_scroll_x.grid(row=1, column=0, sticky="ew")

        trades_notebook.add(trade_tab, text="交易流水")
        main_pane.add(params_viewport, stretch="never")
        main_pane.add(content_frame, stretch="always")
        self.window.after_idle(self._initialize_backtest_content_pane)

    def _update_sizing_mode_widgets(self) -> None:
        self.sizing_mode_combo.configure(state="readonly")
        mode = BACKTEST_SIZING_OPTIONS.get(self.sizing_mode_label.get(), "fixed_risk")
        if mode == "fixed_size":
            self.size_or_risk_label.configure(text="固定数量")
            self.size_or_risk_entry.configure(state="normal")
            self.risk_percent_entry.configure(state="disabled")
        elif mode == "risk_percent":
            self.size_or_risk_label.configure(text="固定风险金/数量")
            self.size_or_risk_entry.configure(state="disabled")
            self.risk_percent_entry.configure(state="normal")
        else:
            self.size_or_risk_label.configure(text="固定风险金")
            self.size_or_risk_entry.configure(state="normal")
            self.risk_percent_entry.configure(state="disabled")
        self._schedule_backtest_minimum_order_hint_update()

    def _schedule_backtest_minimum_order_hint_update(self, *_: str) -> None:
        if not self._ui_alive():
            return
        if self._backtest_hint_after_id is not None:
            try:
                self.window.after_cancel(self._backtest_hint_after_id)
            except Exception:
                pass
        self._backtest_hint_after_id = self.window.after(120, self._update_backtest_minimum_order_hint)

    def _find_instrument_for_backtest_hint(
        self,
        inst_id: str,
        *,
        fetch_if_missing: bool = False,
    ) -> Instrument | None:
        normalized_inst_id = inst_id.strip().upper()
        if not normalized_inst_id:
            return None
        cached = self._backtest_hint_instrument_cache.get(normalized_inst_id)
        if cached is not None:
            return cached
        if fetch_if_missing:
            self._ensure_backtest_hint_instrument_async(normalized_inst_id)
        return None

    def _ensure_backtest_hint_instrument_async(self, inst_id: str) -> None:
        normalized_inst_id = inst_id.strip().upper()
        if not normalized_inst_id or normalized_inst_id in self._backtest_hint_fetching_symbols:
            return
        self._backtest_hint_fetching_symbols.add(normalized_inst_id)

        def worker() -> None:
            instrument: Instrument | None = None
            try:
                instrument = self.client.get_instrument(normalized_inst_id)
            except Exception:
                instrument = None

            def apply() -> None:
                self._backtest_hint_fetching_symbols.discard(normalized_inst_id)
                if instrument is not None:
                    self._backtest_hint_instrument_cache[normalized_inst_id] = instrument
                self._update_backtest_minimum_order_hint()

            if self._ui_alive():
                self.window.after(0, apply)

        threading.Thread(target=worker, daemon=True).start()

    def _update_backtest_minimum_order_hint(self) -> None:
        self._backtest_hint_after_id = None
        inst_id = self.symbol.get().strip().upper()
        definition = self._selected_strategy_definition()
        signal_mode = SIGNAL_LABEL_TO_VALUE.get(self.signal_mode_label.get(), definition.default_signal_mode)
        self.minimum_order_hint_text.set(
            _build_backtest_minimum_order_hint_text(
                inst_id=inst_id,
                strategy_id=definition.strategy_id,
                signal_mode=signal_mode,
                instrument=None,
                sizing_mode_label=self.sizing_mode_label.get(),
                size_or_risk_raw=self.risk_amount.get(),
            )
        )

    def _selected_history_sync_bars(self) -> list[str]:
        return [bar for bar in BACKTEST_HISTORY_SYNC_BARS if self.sync_history_bar_vars[bar].get()]

    @staticmethod
    def _compact_status_text(text: str) -> str:
        return text.strip().replace("\n", " ").rstrip("。")

    def _compact_local_data_status(self) -> str:
        text = self._compact_status_text(self.local_data_status.get())
        if text.startswith("本地数据："):
            text = f"本地：{text.split('：', 1)[1]}"
        if text.startswith("纯本地数据："):
            text = f"纯本地：{text.split('：', 1)[1]}"
        if text.startswith("本地优先数据："):
            text = f"本地优先：{text.split('：', 1)[1]}"
        text = text.replace(" 共 ", " ").replace(" 根，覆盖 ", "根，")
        if "。可先" in text:
            text = text.split("。", 1)[0]
        return text

    def _compact_history_sync_status(self) -> str:
        text = self._compact_status_text(self.history_sync_status.get())
        if text.startswith("先勾选要同步的周期"):
            return "同步：勾选周期后同步，纯本地仅用缓存"
        if text.startswith("正在同步历史数据"):
            return text.replace("正在同步历史数据", "同步中", 1)
        if text.startswith("正在校验本地缓存"):
            return text.replace("正在校验本地缓存", "校验中", 1)
        if text.startswith("正在同步价格精度/下单规则"):
            return text.replace("正在同步价格精度/下单规则", "规则同步中", 1)
        return text

    def _refresh_backtest_action_summary(self, *_: str) -> None:
        if not hasattr(self, "_backtest_action_summary"):
            return
        parts = [
            self._compact_local_data_status(),
            "批量：SL×1/1.5/2，TP×1/2/3，共9组",
            self._compact_history_sync_status(),
        ]
        self._backtest_action_summary.set("；".join(part for part in parts if part))

    def _schedule_local_data_status_update(self, *_: str) -> None:
        if not self._ui_alive():
            return
        if self._local_data_status_after_id is not None:
            try:
                self.window.after_cancel(self._local_data_status_after_id)
            except Exception:
                pass
        self._local_data_status_after_id = self.window.after(120, self._update_local_data_status)

    def _update_local_data_status(self) -> None:
        self._local_data_status_after_id = None
        inst_id = self.symbol.get().strip().upper()
        bar = _backtest_bar_value_from_label(self.bar_label.get())
        if not inst_id:
            self.local_data_status.set("本地数据：等待选择标的与周期。")
            return
        count = get_candle_count(inst_id, bar)
        bounds = get_candle_time_bounds(inst_id, bar)
        if count <= 0 or bounds is None:
            self.local_data_status.set(
                f"本地数据：{inst_id} {bar} 暂无缓存。可先勾选周期后点“同步历史数据”，需要离线回测时再点“同步价格精度/下单规则”。"
            )
            return
        prefix = "纯本地" if self.pure_local_backtest.get() else "本地优先"
        self.local_data_status.set(
            f"{prefix}数据：{inst_id} {bar} 共 {count} 根，覆盖 {_format_local_cache_range_text(bounds)}。"
        )

    def _validate_local_candle_dataset(
        self,
        *,
        candles: list[object],
        inst_id: str,
        bar: str,
        purpose: str,
        candle_limit: int,
        start_ts: int | None,
        end_ts: int | None,
    ) -> None:
        if not candles:
            raise ValueError(f"{purpose}缺少本地缓存：{inst_id} {bar}。请先同步对应周期数据。")
        step_ms = bar_step_ms(bar)
        if start_ts is not None or end_ts is not None:
            window_start = start_ts if start_ts is not None else candles[0].ts
            window_end = end_ts if end_ts is not None else candles[-1].ts
            in_range = [candle for candle in candles if window_start <= candle.ts <= window_end]
            if not in_range:
                raise ValueError(
                    f"{purpose}本地缓存未覆盖请求区间：{inst_id} {bar}，当前仅有 {_format_local_cache_range_text((candles[0].ts, candles[-1].ts))}。"
                )
            gaps, _warnings = find_candle_gaps_in_window(
                in_range,
                window_start_ms=window_start,
                window_end_ms_inclusive=window_end,
                step_ms=step_ms,
            )
            if gaps:
                raise ValueError(
                    f"{purpose}存在本地缺口：{inst_id} {bar}，首段缺口 {_first_gap_summary(gaps, step_ms)}。"
                )
            return
        if candle_limit > 0 and len(candles) < candle_limit:
            raise ValueError(
                f"{purpose}本地缓存不足：{inst_id} {bar} 仅有 {len(candles)} 根，少于请求的 {candle_limit} 根；"
                f"当前覆盖 {_format_local_cache_range_text((candles[0].ts, candles[-1].ts))}。"
            )
        gaps, _warnings = find_candle_gaps_half_open_range(
            candles,
            start_ms=candles[0].ts,
            end_exclusive_ms=candles[-1].ts + step_ms,
            step_ms=step_ms,
        )
        if gaps:
            raise ValueError(f"{purpose}存在本地缺口：{inst_id} {bar}，首段缺口 {_first_gap_summary(gaps, step_ms)}。")

    def _preflight_pure_local_backtest(
        self,
        config: StrategyConfig,
        candle_limit: int,
        start_ts: int | None,
        end_ts: int | None,
    ) -> None:
        cached_instrument_getter = getattr(self.client, "get_cached_instrument", None)
        if callable(cached_instrument_getter) and cached_instrument_getter(config.inst_id) is None:
            raise ValueError(f"缺少合约元数据缓存：{config.inst_id}。请先点“同步价格精度/下单规则”。")

        preload_count = _required_backtest_preload_candles(config)
        primary_candles = _load_backtest_candles(
            self.client,
            config.inst_id,
            config.bar,
            candle_limit,
            start_ts=start_ts,
            end_ts=end_ts,
            preload_count=preload_count,
            local_only=True,
        )
        self._validate_local_candle_dataset(
            candles=primary_candles,
            inst_id=config.inst_id,
            bar=config.bar,
            purpose="主回测K线",
            candle_limit=candle_limit,
            start_ts=start_ts,
            end_ts=end_ts,
        )

        if _backtest_uses_mtf_filter(config.strategy_id):
            filter_limit = min(MAX_BACKTEST_CANDLES, max(800, candle_limit if candle_limit > 0 else len(primary_candles)))
            filter_candles = _load_backtest_candles(
                self.client,
                config.resolved_mtf_filter_inst_id(),
                config.resolved_mtf_filter_bar(),
                filter_limit,
                start_ts=start_ts,
                end_ts=end_ts,
                preload_count=_required_mtf_filter_preload_candles(config),
                local_only=True,
            )
            self._validate_local_candle_dataset(
                candles=filter_candles,
                inst_id=config.resolved_mtf_filter_inst_id(),
                bar=config.resolved_mtf_filter_bar(),
                purpose="多周期过滤K线",
                candle_limit=filter_limit,
                start_ts=start_ts,
                end_ts=end_ts,
            )

        if (
            strategy_is_cross_family(config.strategy_id)
            and int(config.cross_higher_tf_ref_ema_period) > 0
            and (config.cross_higher_tf_inst_id or "").strip()
            and (config.cross_higher_tf_bar or "").strip()
        ):
            hi_inst_id = (config.cross_higher_tf_inst_id or config.inst_id).strip()
            hi_bar = (config.cross_higher_tf_bar or "").strip()
            hi_limit = min(MAX_BACKTEST_CANDLES, max(800, len(primary_candles) // 4 + 400))
            hi_candles = _load_backtest_candles(
                self.client,
                hi_inst_id,
                hi_bar,
                hi_limit,
                start_ts=start_ts,
                end_ts=end_ts,
                preload_count=max(0, int(config.cross_higher_tf_ref_ema_period) + 5),
                local_only=True,
            )
            self._validate_local_candle_dataset(
                candles=hi_candles,
                inst_id=hi_inst_id,
                bar=hi_bar,
                purpose="高周期方向过滤K线",
                candle_limit=hi_limit,
                start_ts=start_ts,
                end_ts=end_ts,
            )

        if _backtest_uses_daily_filter(config) and primary_candles:
            daily_candles = _load_daily_filter_candles(
                self.client,
                config,
                entry_candles=primary_candles,
                local_only=True,
            )
            if len(daily_candles) < max(int(config.daily_filter_period), 1):
                raise ValueError(
                    f"日线过滤本地缓存不足：{config.resolved_daily_filter_inst_id()} {config.resolved_daily_filter_bar()}。"
                )

    def start_backtest(self) -> None:
        try:
            config = self._build_config()
            candle_limit = self._parse_positive_int(self.candle_limit.get(), "回测K线数")
        except Exception as exc:
            messagebox.showerror("回测参数错误", str(exc), parent=self.window)
            return

        self.report_summary.set(
            f"快照编号：{snapshot.snapshot_id} | 时间：{snapshot.created_at.strftime('%Y-%m-%d %H:%M:%S')} | "
            f"策略：{STRATEGY_ID_TO_NAME.get(snapshot.config.strategy_id, snapshot.config.strategy_id)} | "
            f"交易对：{snapshot.config.inst_id} | K线：{_normalize_backtest_bar_label(snapshot.config.bar)} | "
            f"交易次数：{result.report.total_trades}"
        )
        self.report_text.delete("1.0", END)
        self.trade_tree.delete(*self.trade_tree.get_children())
        self._reset_chart_views()
        self._clear_chart_canvas(self.chart_canvas)
        if self._chart_zoom_canvas is not None and self._chart_zoom_canvas.winfo_exists():
            self._clear_chart_canvas(self._chart_zoom_canvas)

        threading.Thread(
            target=self._run_backtest_worker,
            args=(config, candle_limit),
            daemon=True,
        ).start()

    def _run_backtest_worker(self, config: StrategyConfig, candle_limit: int) -> None:
        try:
            result = run_backtest(self.client, config, candle_limit=candle_limit)
            self.window.after(0, lambda: self._apply_backtest_result(result, config, candle_limit))
        except Exception as exc:
            self.window.after(0, lambda error=exc: self._show_backtest_error(error))

    def _apply_backtest_result(self, result: BacktestResult, config: StrategyConfig, candle_limit: int) -> None:
        snapshot = self._append_backtest_snapshot(result, config, candle_limit)
        self._load_snapshot(snapshot.snapshot_id)

    def _load_snapshot(self, snapshot_id: str) -> None:
        if not self._ui_alive():
            return
        snapshot = self._backtest_snapshots[snapshot_id]
        result = snapshot.result
        self._current_snapshot_id = snapshot_id
        self._latest_result = result
        self._reset_chart_views()
        self.report_summary.set(
            f"快照编号：{snapshot.snapshot_id} | 时间：{snapshot.created_at.strftime('%Y-%m-%d %H:%M:%S')} | "
            f"策略：{STRATEGY_ID_TO_NAME.get(snapshot.config.strategy_id, snapshot.config.strategy_id)} | "
            f"交易对：{snapshot.config.inst_id} | K线：{_normalize_backtest_bar_label(snapshot.config.bar)} | "
            f"交易次数：{result.report.total_trades}"
        )
        self.report_text.delete("1.0", END)
        self.report_text.insert("1.0", _build_backtest_report_copy_text(snapshot))
        self.trade_tree.delete(*self.trade_tree.get_children())
        for index, trade in enumerate(result.trades, start=1):
            exit_reason = _format_trade_exit_reason(trade.exit_reason)
            self.trade_tree.insert(
                "",
                END,
                iid=f"T{index:03d}",
                values=(
                    index,
                    "做多" if trade.signal == "long" else "做空",
                    _format_chart_timestamp(trade.entry_ts),
                    format_decimal_fixed(trade.entry_price, 4),
                    format_decimal_fixed(trade.stop_loss, 4),
                    format_decimal_fixed(trade.atr_value, 4),
                    format_decimal_fixed(trade.size, 4),
                    _format_chart_timestamp(trade.exit_ts),
                    format_decimal_fixed(trade.exit_price, 4),
                    exit_reason,
                    format_decimal_fixed(trade.pnl, 4),
                    format_decimal_fixed(trade.r_multiple, 4),
                ),
            )
        if self.compare_tree.exists(snapshot.snapshot_id):
            self.compare_tree.selection_set(snapshot.snapshot_id)
            self.compare_tree.focus(snapshot.snapshot_id)
            self.compare_tree.see(snapshot.snapshot_id)
        self._update_compare_detail(snapshot)
        self._show_batch_matrix_for_snapshot(snapshot.snapshot_id)
        self._populate_period_stats(self.monthly_stats_tree, result.monthly_stats)
        self._populate_period_stats(self.yearly_stats_tree, result.yearly_stats)
        self._redraw_all_charts()

    def _show_backtest_error(self, exc: Exception) -> None:
        self.report_summary.set("回测失败")
        self._set_chart_title("K线图、资金曲线与止盈止损触发位置 | 回测失败")
        messagebox.showerror("回测失败", str(exc), parent=self.window)

    def start_backtest(self) -> None:
        if self._backtest_running or self._history_ui_busy():
            return
        try:
            config, candle_limit, maker_fee_rate, taker_fee_rate, start_ts, end_ts, local_only = self._build_backtest_request()
            if local_only:
                self._preflight_pure_local_backtest(config, candle_limit, start_ts, end_ts)
            raw_batch_count = len(build_parameter_batch_configs(config))
        except Exception as exc:
            messagebox.showerror("回测参数错误", str(exc), parent=self.window)
            return
        if raw_batch_count <= 1:
            self._prepare_backtest_output("正在单组回测，请稍候..." if not local_only else "正在纯本地单组回测，请稍候...")
            self._set_backtest_running(True)
            threading.Thread(
                target=self._run_backtest_worker,
                args=(config, candle_limit, maker_fee_rate, taker_fee_rate, start_ts, end_ts, local_only),
                daemon=True,
            ).start()
            return

        summary_text = (
            f"正在准备批量回测：原始矩阵 {raw_batch_count} 组，正在排除无效组合，请稍候..."
            if not local_only
            else f"正在准备纯本地批量回测：原始矩阵 {raw_batch_count} 组，正在排除无效组合，请稍候..."
        )
        self._prepare_backtest_output(summary_text)
        self._set_backtest_running(True)
        batch_label = self._next_batch_label()
        threading.Thread(
            target=self._run_batch_backtest_worker,
            args=(
                config,
                candle_limit,
                batch_label,
                raw_batch_count,
                maker_fee_rate,
                taker_fee_rate,
                start_ts,
                end_ts,
                local_only,
            ),
            daemon=True,
        ).start()

    def start_single_backtest(self) -> None:
        if self._backtest_running or self._history_ui_busy():
            return
        try:
            config, candle_limit, maker_fee_rate, taker_fee_rate, start_ts, end_ts, local_only = self._build_backtest_request()
            if local_only:
                self._preflight_pure_local_backtest(config, candle_limit, start_ts, end_ts)
        except Exception as exc:
            messagebox.showerror("回测参数错误", str(exc), parent=self.window)
            return

        self._prepare_backtest_output("正在单组回测，请稍候..." if not local_only else "正在纯本地单组回测，请稍候...")
        self._set_backtest_running(True)
        threading.Thread(
            target=self._run_backtest_worker,
            args=(config, candle_limit, maker_fee_rate, taker_fee_rate, start_ts, end_ts, local_only),
            daemon=True,
        ).start()

    def _build_backtest_request(
        self,
    ) -> tuple[StrategyConfig, int, Decimal, Decimal, int | None, int | None, bool]:
        start_ts = self._parse_optional_datetime(self.start_time_text.get(), "开始时间", end_of_day=False)
        end_ts = self._parse_optional_datetime(self.end_time_text.get(), "结束时间", end_of_day=True)
        if start_ts is not None and end_ts is not None and start_ts > end_ts:
            raise ValueError("开始时间不能晚于结束时间")
        config = self._build_config()
        return (
            config,
            self._parse_backtest_candle_limit(self.candle_limit.get()),
            self._parse_fee_percent(self.maker_fee_percent.get(), "Maker手续费"),
            self._parse_fee_percent(self.taker_fee_percent.get(), "Taker手续费"),
            start_ts,
            end_ts,
            bool(self.pure_local_backtest.get()),
        )

    def _prepare_backtest_output(self, summary_text: str) -> None:
        self.report_summary.set(summary_text)
        self.report_text.delete("1.0", END)
        self._clear_detail_tables(
            manual_summary_text="当前策略没有额外扩展统计。"
        )
        self._current_snapshot_id = None
        self._reset_chart_views()
        self._set_chart_title("K线图、资金曲线与止盈止损触发位置 | 正在准备回测")
        if self._chart_zoom_canvas is not None and self._chart_zoom_canvas.winfo_exists():
            self._clear_chart_canvas(self._chart_zoom_canvas)
        self._refresh_zoom_chart_header()

    def _set_chart_title(self, text: str) -> None:
        if getattr(self, "chart_frame", None) is not None:
            self.chart_frame.configure(text=text)

    def _build_chart_title_for_snapshot(self, snapshot: _BacktestSnapshot) -> str:
        signal_label = SIGNAL_VALUE_TO_LABEL.get(snapshot.config.signal_mode, snapshot.config.signal_mode)
        return (
            "K线图、资金曲线与止盈止损触发位置 | "
            f"运行结果 {_runtime_snapshot_id(snapshot) or snapshot.snapshot_id} | "
            f"{_strategy_display_name(snapshot.config)} | "
            f"{snapshot.config.inst_id} | "
            f"{_normalize_backtest_bar_label(snapshot.config.bar)} | "
            f"{signal_label} | M费{_format_fee_rate_percent(snapshot.maker_fee_rate)} | "
            f"T费{_format_fee_rate_percent(snapshot.taker_fee_rate)}"
        )

    def _manual_filter_value(self) -> str:
        return MANUAL_FILTER_OPTIONS.get(self.manual_filter_label.get(), "all")

    def _manual_sort_value(self) -> str:
        return MANUAL_SORT_OPTIONS.get(self.manual_sort_label.get(), "direction_gap")

    def _selected_manual_position(self) -> BacktestManualPosition | None:
        selection = self.manual_tree.selection()
        if not selection:
            return None
        return self._manual_tree_position_map.get(selection[0])

    def _focus_chart_on_manual_position(self, manual_position: BacktestManualPosition) -> None:
        if self._latest_result is None or not self._latest_result.candles:
            return
        start_index, visible_count, hover_index = _manual_focus_window(
            manual_position,
            len(self._latest_result.candles),
        )
        for viewport in (self._main_chart_view, self._zoom_chart_view):
            viewport.start_index = start_index
            viewport.visible_count = visible_count
        if self._widget_exists(getattr(self, "chart_canvas", None)):
            self._chart_hover_indices[id(self.chart_canvas)] = hover_index
        if self._chart_zoom_canvas is not None and self._widget_exists(self._chart_zoom_canvas):
            self._chart_hover_indices[id(self._chart_zoom_canvas)] = hover_index
        self._redraw_all_charts()

    def focus_selected_manual_position_on_chart(self) -> None:
        manual_position = self._selected_manual_position()
        if manual_position is None:
            messagebox.showinfo("扩展统计", "请先在扩展统计里选中一条记录。", parent=self.window)
            return
        if self._chart_zoom_window is None or not self._chart_zoom_window.winfo_exists():
            self.open_chart_zoom_window()
        self._focus_chart_on_manual_position(manual_position)

    def _sync_extension_stats_tab(self, result: BacktestResult | None) -> None:
        notebook = self._trades_notebook
        trade_tab = self._trade_tab
        extension_tab = self._extension_stats_tab
        if notebook is None or trade_tab is None or extension_tab is None:
            return
        should_show = _has_extension_stats(result)
        extension_tab_id = str(extension_tab)
        visible_tab_ids = notebook.tabs()
        is_visible = extension_tab_id in visible_tab_ids
        if should_show and not is_visible:
            notebook.add(extension_tab, text="扩展统计")
            return
        if not should_show and is_visible:
            if notebook.select() == extension_tab_id:
                notebook.select(trade_tab)
            notebook.forget(extension_tab)

    def refresh_manual_tree_view(self) -> None:
        if self._current_snapshot_id is None:
            if self._latest_result is None:
                return
            self.manual_summary.set("当前策略没有额外扩展统计。")
            return
        snapshot = self._backtest_snapshots.get(self._current_snapshot_id)
        if snapshot is None or snapshot.result is None:
            return
        self._populate_manual_tree(snapshot.result, snapshot.config)

    def _clear_detail_tables(self, *, manual_summary_text: str | None = None) -> None:
        self.trade_tree.delete(*self.trade_tree.get_children())
        self.manual_tree.delete(*self.manual_tree.get_children())
        self._manual_tree_position_map.clear()
        self.manual_summary.set(manual_summary_text or "当前策略没有额外扩展统计。")
        self._sync_extension_stats_tab(None)

    def _populate_trade_tree(self, trades: list[BacktestTrade]) -> None:
        self.trade_tree.delete(*self.trade_tree.get_children())
        for index, trade in enumerate(trades, start=1):
            self.trade_tree.insert(
                "",
                END,
                iid=f"T{index:03d}",
                values=(
                    index,
                    "做多" if trade.signal == "long" else "做空",
                    _format_chart_timestamp(trade.entry_ts),
                    format_decimal_fixed(trade.entry_price, 4),
                    format_decimal_fixed(trade.stop_loss, 4),
                    format_decimal_fixed(trade.atr_value, 4),
                    format_decimal_fixed(trade.size, 4),
                    _format_chart_timestamp(trade.exit_ts),
                    format_decimal_fixed(trade.exit_price, 4),
                    format_decimal_fixed(trade.total_fee, 4),
                    _format_trade_exit_reason(trade.exit_reason),
                    format_decimal_fixed(trade.pnl, 4),
                    format_decimal_fixed(trade.r_multiple, 4),
                ),
            )

    def _populate_trade_tree_with_result(self, result: BacktestResult) -> None:
        self.trade_tree.delete(*self.trade_tree.get_children())
        for item_id, values in _build_trade_tree_rows(result):
            self.trade_tree.insert("", END, iid=item_id, values=values)

    def _populate_manual_tree(self, result: BacktestResult, config: StrategyConfig) -> None:
        previous_selected = self._selected_manual_position()
        filter_label = self.manual_filter_label.get()
        sort_label = self.manual_sort_label.get()
        filtered_positions = _filter_manual_positions(result.manual_positions, self._manual_filter_value())
        sorted_positions = _sorted_manual_positions(filtered_positions, self._manual_sort_value())
        self.manual_tree.delete(*self.manual_tree.get_children())
        self._manual_tree_position_map.clear()
        self.manual_summary.set(
            _build_manual_pool_summary(
                result,
                config,
                visible_positions=filtered_positions,
                filter_label=filter_label,
                sort_label=sort_label,
            )
        )
        target_iid = None
        for index, manual_position in enumerate(sorted_positions, start=1):
            iid = f"M{index:03d}"
            self.manual_tree.insert(
                "",
                END,
                iid=iid,
                values=_build_manual_position_row(index, manual_position),
                tags=(_manual_row_tag(manual_position),),
            )
            self._manual_tree_position_map[iid] = manual_position
            if previous_selected == manual_position:
                target_iid = iid
        if target_iid is not None:
            self.manual_tree.selection_set(target_iid)
            self.manual_tree.focus(target_iid)
            self.manual_tree.see(target_iid)

    def _set_backtest_running(self, running: bool) -> None:
        self._backtest_running = running
        self._refresh_action_button_states()

    def _set_history_sync_running(self, running: bool) -> None:
        self._history_sync_running = running
        self._refresh_action_button_states()

    def _set_history_verify_running(self, running: bool) -> None:
        self._history_verify_running = running
        self._refresh_action_button_states()

    def _history_ui_busy(self) -> bool:
        return self._history_sync_running or self._history_verify_running

    def _refresh_action_button_states(self) -> None:
        state = "disabled" if (self._backtest_running or self._history_ui_busy()) else "normal"
        if self._widget_exists(getattr(self, "single_backtest_button", None)):
            self.single_backtest_button.configure(state=state)
        if self._widget_exists(getattr(self, "batch_backtest_button", None)):
            self.batch_backtest_button.configure(state=state)
        if self._widget_exists(getattr(self, "sync_history_button", None)):
            self.sync_history_button.configure(state=state)
        if self._widget_exists(getattr(self, "sync_metadata_button", None)):
            self.sync_metadata_button.configure(state=state)
        if self._widget_exists(getattr(self, "verify_cache_button", None)):
            self.verify_cache_button.configure(state=state)

    def sync_history_data(self) -> None:
        if self._backtest_running or self._history_ui_busy():
            return
        selected_bars = self._selected_history_sync_bars()
        if not selected_bars:
            messagebox.showinfo("同步历史数据", "请至少勾选一个要同步的周期。", parent=self.window)
            return
        tasks = [(symbol, bar) for symbol in BACKTEST_SYMBOL_OPTIONS for bar in selected_bars]
        self.history_sync_status.set(
            f"正在同步历史数据（0/{len(tasks)}）：5 个币种 × {len(selected_bars)} 个周期（{' / '.join(selected_bars)}），全量缓存下载中，请稍候..."
        )
        self._set_history_sync_running(True)
        threading.Thread(
            target=self._run_history_sync_worker,
            args=(tasks,),
            daemon=True,
        ).start()

    def verify_history_cache_data(self) -> None:
        if self._backtest_running or self._history_ui_busy():
            return
        selected_bars = self._selected_history_sync_bars()
        if not selected_bars:
            messagebox.showinfo("校验数据", "请至少勾选一个要校验的周期。", parent=self.window)
            return
        tasks = [(symbol, bar) for symbol in BACKTEST_SYMBOL_OPTIONS for bar in selected_bars]
        self.history_sync_status.set(
            f"正在校验本地缓存（0/{len(tasks)}）：5 个币种 × {len(selected_bars)} 个周期（{' / '.join(selected_bars)}），检查连续性并尝试补洞…"
        )
        self._set_history_verify_running(True)
        threading.Thread(
            target=self._run_history_verify_worker,
            args=(tasks,),
            daemon=True,
        ).start()

    def sync_instrument_metadata(self) -> None:
        if self._backtest_running or self._history_ui_busy():
            return
        symbols = list(BACKTEST_SYMBOL_OPTIONS)
        self.history_sync_status.set(
            f"正在同步价格精度/下单规则（0/{len(symbols)}）：5 个回测币种的价格精度与最小下单单位缓存中，请稍候..."
        )
        self._set_history_sync_running(True)
        threading.Thread(target=self._run_instrument_metadata_sync_worker, args=(symbols,), daemon=True).start()

    def _run_instrument_metadata_sync_worker(self, symbols: list[str]) -> None:
        results: list[tuple[str, str | None]] = []
        try:
            total = len(symbols)
            for index, symbol in enumerate(symbols, start=1):
                if self._ui_alive():
                    progress_text = f"正在同步价格精度/下单规则（{index}/{total}）：{symbol}"
                    self.window.after(0, lambda text=progress_text: self.history_sync_status.set(text))
                try:
                    self.client.get_instrument(symbol)
                    results.append((symbol, None))
                except Exception as exc:
                    results.append((symbol, str(exc)))
            if self._ui_alive():
                self.window.after(0, lambda: self._apply_instrument_metadata_sync_results(results))
        except Exception as exc:
            if self._ui_alive():
                self.window.after(0, lambda error=exc: self._show_history_sync_error(error))

    def _apply_instrument_metadata_sync_results(self, results: list[tuple[str, str | None]]) -> None:
        if not self._ui_alive():
            return
        self._set_history_sync_running(False)
        success = [symbol for symbol, error in results if error is None]
        failed = [(symbol, error) for symbol, error in results if error is not None]
        self.history_sync_status.set(f"价格精度/下单规则同步完成：成功 {len(success)} 个，失败 {len(failed)} 个。")
        self._update_local_data_status()
        lines = [f"本次共同步 {len(results)} 个回测币种的合约信息：成功 {len(success)} 个，失败 {len(failed)} 个。", ""]
        lines.extend([f"{symbol} | 成功" for symbol in success])
        lines.extend([f"{symbol} | 失败：{error}" for symbol, error in failed])
        if failed:
            messagebox.showwarning("同步价格精度/下单规则", "\n".join(lines), parent=self.window)
        else:
            messagebox.showinfo("同步价格精度/下单规则", "\n".join(lines), parent=self.window)

    def _run_history_verify_worker(self, tasks: list[tuple[str, str]]) -> None:
        results: list[CacheVerifyOutcome] = []
        try:
            total = len(tasks)
            for index, (symbol, bar) in enumerate(tasks, start=1):
                if self._ui_alive():
                    progress_text = (
                        f"正在校验本地缓存（{index}/{total}）："
                        f"{symbol} | {_normalize_backtest_bar_label(bar)}"
                    )
                    self.window.after(0, lambda text=progress_text: self.history_sync_status.set(text))
                try:
                    outcome = verify_and_repair_cached_candles(self.client, symbol, bar)
                    results.append(outcome)
                except Exception as exc:
                    results.append(
                        CacheVerifyOutcome(
                            inst_id=symbol,
                            bar=bar,
                            ok=False,
                            candle_count_before=0,
                            candle_count_after=0,
                            missing_bars_before=0,
                            missing_bars_after=0,
                            gap_segments_before=0,
                            gap_segments_after=0,
                            range_fetch_calls=0,
                            did_full_history_fetch=False,
                            warnings=(),
                            error=str(exc),
                        )
                    )
            if self._ui_alive():
                self.window.after(0, lambda: self._apply_history_verify_results(results))
        except Exception as exc:
            if self._ui_alive():
                self.window.after(0, lambda error=exc: self._show_history_verify_error(error))

    def _apply_history_verify_results(self, results: list[CacheVerifyOutcome]) -> None:
        if not self._ui_alive():
            return
        self._set_history_verify_running(False)
        self._update_local_data_status()
        ok_n = sum(1 for item in results if item.ok and not item.error)
        bad_n = len(results) - ok_n
        total_fetched = sum(item.range_fetch_calls for item in results)
        full_n = sum(1 for item in results if item.did_full_history_fetch)
        full_tail = f" 其中 {full_n} 组曾从空缓存全量拉取。" if full_n else ""
        if bad_n:
            self.history_sync_status.set(
                f"校验完成：{ok_n} 组通过，{bad_n} 组仍有问题；累计区间拉取 {total_fetched} 次。{full_tail}"
            )
        else:
            self.history_sync_status.set(
                f"校验完成：{len(results)} 组均连续；区间拉取 {total_fetched} 次。{full_tail}"
            )
        lines = [
            f"本次校验共 {len(results)} 组（与「同步历史数据」相同标的与周期）：通过 {ok_n} 组，未完全通过 {bad_n} 组。",
            "",
        ]
        for item in results:
            label = f"{item.inst_id} | {_normalize_backtest_bar_label(item.bar)}"
            if item.error and not item.ok:
                lines.append(f"{label} | 失败：{item.error}")
            elif not item.ok:
                lines.append(
                    f"{label} | 仍有缺口 约 {item.missing_bars_after} 根（{item.gap_segments_after} 段）"
                    f" | 缓存 {item.candle_count_before}→{item.candle_count_after} 根"
                    f" | 区间拉取 {item.range_fetch_calls} 次"
                )
            else:
                extra = ""
                if item.missing_bars_before > 0:
                    extra = f" | 曾缺约 {item.missing_bars_before} 根，已尝试补齐"
                if item.did_full_history_fetch:
                    extra += " | 空缓存已全量拉取"
                lines.append(
                    f"{label} | 通过{extra} | 缓存 {item.candle_count_before}→{item.candle_count_after} 根"
                    f" | 区间拉取 {item.range_fetch_calls} 次"
                )
            if item.warnings:
                for w in item.warnings[:5]:
                    lines.append(f"    警告：{w}")
                if len(item.warnings) > 5:
                    lines.append(f"    … 另有 {len(item.warnings) - 5} 条警告未显示")
        if bad_n:
            messagebox.showwarning("校验数据", "\n".join(lines), parent=self.window)
        else:
            messagebox.showinfo("校验数据", "\n".join(lines), parent=self.window)

    def _show_history_verify_error(self, exc: Exception) -> None:
        if not self._ui_alive():
            return
        self._set_history_verify_running(False)
        self.history_sync_status.set(f"校验数据失败：{exc}")
        messagebox.showerror("校验数据失败", str(exc), parent=self.window)

    def _run_history_sync_worker(self, tasks: list[tuple[str, str]]) -> None:
        results: list[tuple[str, str, int, str | None]] = []
        try:
            total = len(tasks)
            for index, (symbol, bar) in enumerate(tasks, start=1):
                if self._ui_alive():
                    progress_text = (
                        f"正在同步历史数据（{index}/{total}）："
                        f"{symbol} | {_normalize_backtest_bar_label(bar)} | 全量历史"
                    )
                    self.window.after(0, lambda text=progress_text: self.history_sync_status.set(text))
                try:
                    progress_state = {"last_page": 0}

                    def on_progress(payload: dict[str, object]) -> None:
                        if not self._ui_alive():
                            return
                        page_count = int(payload.get("page_count", 0) or 0)
                        if page_count > 1 and page_count % 5 != 0:
                            return
                        if page_count <= progress_state["last_page"]:
                            return
                        progress_state["last_page"] = page_count
                        total_count = int(payload.get("total_count", 0) or 0)
                        oldest_ts = payload.get("oldest_ts")
                        range_text = (
                            _format_chart_timestamp(int(oldest_ts))
                            if isinstance(oldest_ts, int)
                            else "-"
                        )
                        progress_text = (
                            f"正在同步历史数据（{index}/{total}）：{symbol} | "
                            f"{_normalize_backtest_bar_label(bar)} | 第 {page_count} 页 | "
                            f"已累计 {total_count} 根 | 最早到 {range_text}"
                        )
                        self.window.after(0, lambda text=progress_text: self.history_sync_status.set(text))

                    candles = self.client.get_candles_history(symbol, bar, limit=0, progress_callback=on_progress)
                    results.append((symbol, bar, len(candles), None))
                except Exception as exc:
                    results.append((symbol, bar, 0, str(exc)))
            if self._ui_alive():
                self.window.after(0, lambda: self._apply_history_sync_results(results))
        except Exception as exc:
            if self._ui_alive():
                self.window.after(0, lambda error=exc: self._show_history_sync_error(error))

    def _apply_history_sync_results(self, results: list[tuple[str, str, int, str | None]]) -> None:
        if not self._ui_alive():
            return
        self._set_history_sync_running(False)
        self._update_local_data_status()
        success = [(symbol, bar, count) for symbol, bar, count, error in results if not error]
        failed = [(symbol, bar, error or "未知错误") for symbol, bar, _, error in results if error]
        total_candles = sum(count for _, _, count in success)
        if failed:
            self.history_sync_status.set(f"历史数据同步完成：成功 {len(success)} 组，失败 {len(failed)} 组。")
        else:
            self.history_sync_status.set(
                f"历史数据同步完成：{len(success)} 组全量缓存已更新，累计缓存 {total_candles} 根。"
            )
        lines = [f"本次同步共 {len(results)} 组：成功 {len(success)} 组，失败 {len(failed)} 组。"]
        if success:
            lines.append("")
            lines.append("已同步：")
            lines.extend(
                f"{symbol} | {_normalize_backtest_bar_label(bar)} | {count} 根"
                for symbol, bar, count in success
            )
        if failed:
            lines.append("")
            lines.append("失败：")
            lines.extend(
                f"{symbol} | {_normalize_backtest_bar_label(bar)} | {error}"
                for symbol, bar, error in failed
            )
        if failed:
            messagebox.showwarning("同步历史数据部分失败", "\n".join(lines), parent=self.window)
            return
        messagebox.showinfo("同步历史数据完成", "\n".join(lines), parent=self.window)

    def _run_batch_backtest_worker(
        self,
        config: StrategyConfig,
        candle_limit: int,
        batch_label: str,
        raw_batch_count: int,
        maker_fee_rate: Decimal,
        taker_fee_rate: Decimal,
        start_ts: int | None,
        end_ts: int | None,
        local_only: bool,
    ) -> None:
        try:
            results = run_backtest_batch(
                self.client,
                config,
                candle_limit=candle_limit,
                start_ts=start_ts,
                end_ts=end_ts,
                maker_fee_rate=maker_fee_rate,
                taker_fee_rate=taker_fee_rate,
                local_only=local_only,
            )
            if self._ui_alive():
                self.window.after(
                    0,
                    lambda: self._apply_batch_backtest_results(
                        results,
                        candle_limit,
                        batch_label,
                        raw_batch_count=raw_batch_count,
                    ),
                )
        except Exception as exc:
            if self._ui_alive():
                self.window.after(0, lambda error=exc: self._show_backtest_error(error))

    def _run_backtest_worker(
        self,
        config: StrategyConfig,
        candle_limit: int,
        maker_fee_rate: Decimal,
        taker_fee_rate: Decimal,
        start_ts: int | None,
        end_ts: int | None,
        local_only: bool,
    ) -> None:
        try:
            result = run_backtest(
                self.client,
                config,
                candle_limit=candle_limit,
                start_ts=start_ts,
                end_ts=end_ts,
                maker_fee_rate=maker_fee_rate,
                taker_fee_rate=taker_fee_rate,
                local_only=local_only,
            )
            if self._ui_alive():
                self.window.after(0, lambda: self._apply_backtest_result(result, config, candle_limit))
        except Exception as exc:
            if self._ui_alive():
                self.window.after(0, lambda error=exc: self._show_backtest_error(error))

    def _apply_backtest_result(self, result: BacktestResult, config: StrategyConfig, candle_limit: int) -> None:
        if not self._ui_alive():
            return
        export_path = None
        try:
            export_path = str(export_single_backtest_report(result, config, candle_limit))
        except Exception as exc:
            messagebox.showwarning("回测报告导出失败", f"回测已完成，但报告导出失败：{exc}", parent=self.window)
        snapshot = self._append_backtest_snapshot(result, config, candle_limit, export_path=export_path)
        self._load_snapshot(snapshot.snapshot_id)
        self._set_backtest_running(False)

    def _apply_batch_backtest_results(
        self,
        results: list[tuple[StrategyConfig, BacktestResult]],
        candle_limit: int,
        batch_label: str,
        *,
        raw_batch_count: int,
    ) -> None:
        if not self._ui_alive():
            return
        valid_batch_count = len(results)
        skipped_batch_count = max(raw_batch_count - valid_batch_count, 0)
        export_path = None
        try:
            export_path = str(
                export_batch_backtest_report(
                    results,
                    candle_limit,
                    batch_label=batch_label,
                )
            )
        except Exception as exc:
            messagebox.showwarning("批量回测报告导出失败", f"批量回测已完成，但报告导出失败：{exc}", parent=self.window)
        last_snapshot: _BacktestSnapshot | None = None
        for config, result in results:
            last_snapshot = self._append_backtest_snapshot(
                result,
                config,
                candle_limit,
                batch_label=batch_label,
                export_path=export_path,
            )
        batch_summary_line = (
            f"批量回测完成：原始矩阵 {raw_batch_count} 组 | "
            f"有效组合 {valid_batch_count} 组 | 已排除 {skipped_batch_count} 组无效组合"
        )
        if last_snapshot is None:
            self.report_summary.set(f"{batch_summary_line}\n当前样本内没有可执行的有效组合。")
            self.report_text.delete("1.0", END)
            self._clear_detail_tables()
            self._populate_period_stats(self.monthly_stats_tree, [])
            self._populate_period_stats(self.yearly_stats_tree, [])
            self._show_batch_matrix(None)
            self._set_chart_title("K线图、资金曲线与止盈止损触发位置 | 当前没有可执行的有效组合")
            self._clear_chart_canvas(self.chart_canvas)
            if self._chart_zoom_canvas is not None and self._chart_zoom_canvas.winfo_exists():
                self._clear_chart_canvas(self._chart_zoom_canvas)
            self._refresh_zoom_chart_header()
            self._set_backtest_running(False)
            return
        if last_snapshot is not None:
            self._load_snapshot(last_snapshot.snapshot_id)
            self.report_summary.set(f"{batch_summary_line}\n{self.report_summary.get()}")
            self._show_batch_matrix(batch_label)
        self._set_backtest_running(False)

    def _show_backtest_error(self, exc: Exception) -> None:
        if not self._ui_alive():
            return
        self._current_snapshot_id = None
        self._latest_result = None
        self.report_summary.set("回测失败")
        self._clear_detail_tables()
        self._set_backtest_running(False)
        self._refresh_zoom_chart_header()
        messagebox.showerror("回测失败", str(exc), parent=self.window)

    def _show_history_sync_error(self, exc: Exception) -> None:
        if not self._ui_alive():
            return
        self._set_history_sync_running(False)
        self.history_sync_status.set(f"历史数据同步失败：{exc}")
        messagebox.showerror("同步历史数据失败", str(exc), parent=self.window)

    def _build_config(self) -> StrategyConfig:
        definition = self._selected_strategy_definition()
        dynamic_strategy = strategy_uses_dynamic_orders(definition.strategy_id)
        dynamic_tp_strategy = self._strategy_supports_dynamic_take_profit(definition.strategy_id)
        strategy_id = definition.strategy_id
        sizing_mode = BACKTEST_SIZING_OPTIONS[self.sizing_mode_label.get()]
        size_or_risk = self._parse_positive_decimal(self.risk_amount.get(), "固定风险金/数量")
        risk_percent = None
        order_size = Decimal("0")
        risk_amount = None
        if sizing_mode == "fixed_size":
            order_size = size_or_risk
        elif sizing_mode == "risk_percent":
            risk_percent = self._parse_positive_decimal(self.risk_percent.get(), "风险百分比")
        else:
            risk_amount = size_or_risk
        fixed_risk_amount = strategy_ui_fixed_extra_value(strategy_id, "risk_amount", "backtest")
        if fixed_risk_amount is not None:
            risk_amount = Decimal(str(fixed_risk_amount))
        fixed_order_size = strategy_ui_fixed_extra_value(strategy_id, "order_size", "backtest")
        if fixed_order_size is not None:
            order_size = Decimal(str(fixed_order_size))
        signal_mode = SIGNAL_LABEL_TO_VALUE[self.signal_mode_label.get()]
        if dynamic_strategy:
            signal_mode = resolve_dynamic_signal_mode(definition.strategy_id, signal_mode)
        take_profit_mode = "fixed"
        max_entries_per_trend = 0
        entry_reference_ema_period = 0
        mtf_filter_bar = None
        mtf_filter_fast_ema_period = 21
        mtf_filter_slow_ema_period = 55
        mtf_reversal_mode = "block_new_entries"
        daily_filter_enabled = False
        daily_filter_boundary = "exchange"
        daily_filter_mode = "disabled"
        daily_filter_scope = "both"
        daily_filter_ma_type = "ema"
        daily_filter_period = 5
        reentry_confirmation_enabled = False
        reentry_confirmation_min_sequence = 0
        reentry_confirmation_ma_type = "ema"
        reentry_confirmation_ma_period = 21
        dynamic_two_r_break_even = False
        dynamic_fee_offset_enabled = False
        dynamic_protection_rules: tuple[DynamicProtectionRule, ...] = ()
        trend_ema_slope_filter_min_ratio = Decimal("0")
        atr_percentile_filter_max = Decimal("0")
        body_retest_breakdown_atr_multiplier = Decimal("0.2")
        body_retest_retest_atr_multiplier = Decimal("0.3")
        body_retest_stop_buffer_atr_multiplier = Decimal("0.3")
        body_retest_body_atr_limit = Decimal("1.0")
        body_retest_watch_bars = 6
        time_stop_break_even_enabled = False
        time_stop_break_even_bars = 0
        trend_ema_close_exit_after_trigger_r_enabled = False
        hold_close_exit_bars = 0
        ema55_slope_exit_enabled = True
        ema55_slope_lock_profit_enabled = False
        ema55_slope_lock_profit_trigger_r = 5
        ema55_slope_negative_entry_bars = 1
        ema_type = str(self._resolve_strategy_parameter_value(strategy_id, "ema_type", self.ema_type.get().strip().lower()))
        trend_ema_type = str(
            self._resolve_strategy_parameter_value(strategy_id, "trend_ema_type", self.trend_ema_type.get().strip().lower())
        )
        entry_reference_ema_type = str(
            self._resolve_strategy_parameter_value(
                strategy_id,
                "entry_reference_ema_type",
                self.entry_reference_ema_type.get().strip().lower(),
            )
        )
        if strategy_uses_parameter(definition.strategy_id, "entry_reference_ema_period"):
            entry_reference_ema_period = self._parse_nonnegative_int(
                self.entry_reference_ema_period.get(),
                strategy_entry_reference_period_caption(definition.strategy_id),
            )
        if strategy_uses_parameter(definition.strategy_id, "trend_ema_slope_filter_min_ratio"):
            trend_ema_slope_filter_min_ratio = self._parse_decimal(
                self.trend_ema_slope_filter_min_ratio.get(),
                "开空斜率阈值",
            )
            if trend_ema_slope_filter_min_ratio > 0:
                raise ValueError("开空斜率阈值必须小于或等于 0")
        if strategy_id == STRATEGY_BTC_EMA55_SLOPE_SHORT_ID:
            ema55_slope_negative_entry_bars = self._parse_positive_int(
                self.ema55_slope_negative_entry_bars.get(),
                "连续负斜率根数",
            )
        if strategy_uses_parameter(definition.strategy_id, "atr_percentile_filter_max"):
            atr_percentile_filter_max = self._parse_nonnegative_decimal(
                self.atr_percentile_filter_max.get(),
                "ATR percentile max",
            )
        if strategy_uses_parameter(definition.strategy_id, "body_retest_breakdown_atr_multiplier"):
            body_retest_breakdown_atr_multiplier = self._parse_nonnegative_decimal(
                self.body_retest_breakdown_atr_multiplier.get(),
                "Breakdown ATR",
            )
        if strategy_uses_parameter(definition.strategy_id, "body_retest_retest_atr_multiplier"):
            body_retest_retest_atr_multiplier = self._parse_nonnegative_decimal(
                self.body_retest_retest_atr_multiplier.get(),
                "Retest ATR",
            )
        if strategy_uses_parameter(definition.strategy_id, "body_retest_stop_buffer_atr_multiplier"):
            body_retest_stop_buffer_atr_multiplier = self._parse_nonnegative_decimal(
                self.body_retest_stop_buffer_atr_multiplier.get(),
                "Stop buffer ATR",
            )
        if strategy_uses_parameter(definition.strategy_id, "body_retest_body_atr_limit"):
            body_retest_body_atr_limit = self._parse_nonnegative_decimal(
                self.body_retest_body_atr_limit.get(),
                "Body ATR limit",
            )
        if strategy_uses_parameter(definition.strategy_id, "body_retest_watch_bars"):
            body_retest_watch_bars = self._parse_positive_int(
                self.body_retest_watch_bars.get(),
                "Watch bars",
            )
        if strategy_uses_parameter(definition.strategy_id, "mtf_filter_bar"):
            mtf_filter_bar = str(
                self._resolve_strategy_parameter_value(
                    strategy_id,
                    "mtf_filter_bar",
                    _backtest_bar_value_from_label(self.mtf_filter_bar.get()),
                )
            )
        if strategy_uses_parameter(definition.strategy_id, "mtf_filter_fast_ema_period"):
            mtf_filter_fast_ema_period = int(
                self._resolve_strategy_parameter_value(
                    strategy_id,
                    "mtf_filter_fast_ema_period",
                    self._parse_positive_int(self.mtf_filter_fast_ema_period.get(), "高周期快EMA"),
                )
            )
        if strategy_uses_parameter(definition.strategy_id, "mtf_filter_slow_ema_period"):
            mtf_filter_slow_ema_period = int(
                self._resolve_strategy_parameter_value(
                    strategy_id,
                    "mtf_filter_slow_ema_period",
                    self._parse_positive_int(self.mtf_filter_slow_ema_period.get(), "高周期慢EMA"),
                )
            )
        if strategy_uses_parameter(definition.strategy_id, "mtf_reversal_mode"):
            mtf_reversal_mode = str(
                self._resolve_strategy_parameter_value(
                    strategy_id,
                    "mtf_reversal_mode",
                    MTF_REVERSAL_MODE_OPTIONS.get(self.mtf_reversal_mode_label.get(), "block_new_entries"),
                )
            )
        if strategy_uses_parameter(definition.strategy_id, "daily_filter_enabled"):
            daily_filter_enabled = bool(self.daily_filter_enabled.get())
            daily_filter_boundary = str(
                self._resolve_strategy_parameter_value(
                    strategy_id,
                    "daily_filter_boundary",
                    DAILY_FILTER_BOUNDARY_LABEL_TO_VALUE.get(self.daily_filter_boundary_label.get(), "exchange"),
                )
            )
            daily_filter_mode = str(
                self._resolve_strategy_parameter_value(
                    strategy_id,
                    "daily_filter_mode",
                    DAILY_FILTER_MODE_LABEL_TO_VALUE.get(self.daily_filter_mode_label.get(), "disabled"),
                )
            )
            daily_filter_scope = str(
                self._resolve_strategy_parameter_value(
                    strategy_id,
                    "daily_filter_scope",
                    DAILY_FILTER_SCOPE_LABEL_TO_VALUE.get(self.daily_filter_scope_label.get(), "both"),
                )
            )
            daily_filter_ma_type = str(
                self._resolve_strategy_parameter_value(
                    strategy_id,
                    "daily_filter_ma_type",
                    self.daily_filter_ma_type.get().strip().lower(),
                )
            )
            if daily_filter_mode == "close_vs_ma":
                daily_filter_period = int(
                    self._resolve_strategy_parameter_value(
                        strategy_id,
                        "daily_filter_period",
                        self._parse_positive_int(self.daily_filter_period.get(), "日线均线周期"),
                    )
                )
        if strategy_uses_parameter(definition.strategy_id, "reentry_confirmation_enabled"):
            reentry_confirmation_enabled = bool(self.reentry_confirmation_enabled.get())
            reentry_confirmation_ma_type = str(
                self._resolve_strategy_parameter_value(
                    strategy_id,
                    "reentry_confirmation_ma_type",
                    self.reentry_confirmation_ma_type.get().strip().lower(),
                )
            )
            reentry_confirmation_ma_period = int(
                self._resolve_strategy_parameter_value(
                    strategy_id,
                    "reentry_confirmation_ma_period",
                    self._parse_positive_int(self.reentry_confirmation_ma_period.get(), "再开仓确认均线周期"),
                )
            )
            if reentry_confirmation_enabled:
                reentry_confirmation_min_sequence = int(
                    self._resolve_strategy_parameter_value(
                        strategy_id,
                        "reentry_confirmation_min_sequence",
                        self._parse_positive_int(self.reentry_confirmation_min_sequence.get(), "再开仓确认起始次数"),
                    )
                )
        if dynamic_tp_strategy:
            take_profit_mode = TAKE_PROFIT_MODE_OPTIONS[self.take_profit_mode_label.get()]
            if strategy_uses_parameter(definition.strategy_id, "max_entries_per_trend"):
                max_entries_per_trend = self._parse_nonnegative_int(self.max_entries_per_trend.get(), "每波最多开仓次数")
            dynamic_two_r_break_even = bool(self.dynamic_two_r_break_even.get())
            dynamic_fee_offset_enabled = bool(self.dynamic_fee_offset_enabled.get())
            if take_profit_mode == "dynamic":
                dynamic_protection_rules = self._current_dynamic_protection_rules()
            time_stop_break_even_enabled = bool(self.time_stop_break_even_enabled.get())
            time_stop_break_even_bars = (
                self._parse_positive_int(self.time_stop_break_even_bars.get(), "时间保本K线数")
                if time_stop_break_even_enabled
                else 0
            )
        if strategy_uses_parameter(definition.strategy_id, "trend_ema_close_exit_after_trigger_r_enabled"):
            trend_ema_close_exit_after_trigger_r_enabled = bool(
                self.trend_ema_close_exit_after_trigger_r_enabled.get()
            )
        trend_ema_close_exit_after_trigger_r = 5
        if strategy_uses_parameter(definition.strategy_id, "trend_ema_close_exit_after_trigger_r"):
            trend_ema_close_exit_after_trigger_r = self._parse_positive_int(
                self.trend_ema_close_exit_after_trigger_r.get(),
                "趋势EMA平仓触发R",
            )
            if trend_ema_close_exit_after_trigger_r < 1:
                raise ValueError("趋势EMA平仓触发R 不能小于 1")
        if strategy_uses_parameter(definition.strategy_id, "hold_close_exit_bars"):
            hold_close_exit_bars = self._parse_nonnegative_int(self.hold_close_exit_bars.get(), "满N根K线收盘价平仓")
        if strategy_uses_parameter(definition.strategy_id, "ema55_slope_exit_enabled"):
            ema55_slope_exit_enabled = bool(self.ema55_slope_exit_enabled.get())
        if strategy_uses_parameter(definition.strategy_id, "ema55_slope_lock_profit_trigger_r"):
            ema55_slope_lock_profit_trigger_r = self._parse_positive_int(
                self.ema55_slope_lock_profit_trigger_r.get(),
                "移动止盈触发R",
            )
            if ema55_slope_lock_profit_trigger_r < 2:
                raise ValueError("移动止盈触发R 不能小于 2")
        dynamic_break_even_trigger_r = 2
        if strategy_uses_parameter(definition.strategy_id, "dynamic_break_even_trigger_r"):
            dynamic_break_even_trigger_r = self._parse_positive_int(
                self.dynamic_break_even_trigger_r.get(),
                "保本触发R",
            )
            if dynamic_break_even_trigger_r < 1:
                raise ValueError("保本触发R 不能小于 1")
        dynamic_first_lock_r = 0
        if strategy_uses_parameter(definition.strategy_id, "dynamic_first_lock_r"):
            dynamic_first_lock_r = self._parse_nonnegative_int(
                self.dynamic_first_lock_r.get() or "0",
                "首档锁盈R",
            )
        dynamic_trailing_step_r = 1
        if strategy_uses_parameter(definition.strategy_id, "dynamic_trailing_step_r"):
            dynamic_trailing_step_r = self._parse_positive_int(
                self.dynamic_trailing_step_r.get(),
                "移动步长R",
            )
            if dynamic_trailing_step_r < 1:
                raise ValueError("移动步长R 不能小于 1")
        if take_profit_mode == "dynamic":
            overlap_warnings = describe_dynamic_protection_rule_overlap_warnings(dynamic_protection_rules)
            if overlap_warnings:
                raise ValueError(
                    "动态保护规则有冲突：\n"
                    + "\n".join(overlap_warnings)
                    + "\n请删除或修改较弱规则后再回测。"
                )
        if strategy_id == STRATEGY_BTC_EMA55_SLOPE_SHORT_ID:
            ema55_slope_lock_profit_enabled = bool(self.ema55_slope_lock_profit_enabled.get())
        entry_slippage_rate = self._parse_nonnegative_decimal(self.entry_slippage_percent.get(), "开仓滑点") / Decimal("100")
        exit_slippage_rate = self._parse_nonnegative_decimal(self.exit_slippage_percent.get(), "平仓滑点") / Decimal("100")
        return StrategyConfig(
            inst_id=self.symbol.get().strip().upper(),
            bar=str(self._resolve_strategy_parameter_value(strategy_id, "bar", _backtest_bar_value_from_label(self.bar_label.get()))),
            ema_type=ema_type,
            ema_period=int(
                self._resolve_strategy_parameter_value(
                    strategy_id,
                    "ema_period",
                    self._parse_positive_int(self.ema_period.get(), "快线均线周期"),
                )
            ),
            trend_ema_period=int(
                self._resolve_strategy_parameter_value(
                    strategy_id,
                    "trend_ema_period",
                    self._parse_positive_int(self.trend_ema_period.get(), "趋势均线周期"),
                )
            ),
            trend_ema_type=trend_ema_type,
            big_ema_period=(
                int(
                    self._resolve_strategy_parameter_value(
                        strategy_id,
                        "big_ema_period",
                        self._parse_positive_int(self.big_ema_period.get(), "EMA大周期"),
                    )
                )
                if self._strategy_uses_big_ema(strategy_id)
                else 0
            ),
            entry_reference_ema_type=entry_reference_ema_type,
            entry_reference_ema_period=entry_reference_ema_period,
            reentry_confirmation_enabled=reentry_confirmation_enabled,
            reentry_confirmation_min_sequence=reentry_confirmation_min_sequence,
            reentry_confirmation_ma_type=reentry_confirmation_ma_type,
            reentry_confirmation_ma_period=reentry_confirmation_ma_period,
            atr_period=self._parse_positive_int(self.atr_period.get(), "ATR 周期"),
            atr_stop_multiplier=self._parse_positive_decimal(self.stop_atr.get(), "止损 ATR 倍数"),
            atr_take_multiplier=self._parse_positive_decimal(self.take_atr.get(), "止盈 ATR 倍数"),
            order_size=order_size,
            trade_mode=TRADE_MODE_OPTIONS[self.trade_mode_label.get()],
            signal_mode=signal_mode,
            position_mode=POSITION_MODE_OPTIONS[self.position_mode_label.get()],
            environment=ENV_OPTIONS[self.environment_label.get()],
            tp_sl_trigger_type=TRIGGER_TYPE_OPTIONS[self.trigger_type_label.get()],
            strategy_id=definition.strategy_id,
            risk_amount=risk_amount,
            take_profit_mode=take_profit_mode,
            max_entries_per_trend=max_entries_per_trend,
            dynamic_two_r_break_even=dynamic_two_r_break_even,
            dynamic_break_even_trigger_r=dynamic_break_even_trigger_r,
            dynamic_fee_offset_enabled=dynamic_fee_offset_enabled,
            dynamic_protection_rules=dynamic_protection_rules,
            ema55_slope_exit_enabled=ema55_slope_exit_enabled,
            ema55_slope_lock_profit_enabled=ema55_slope_lock_profit_enabled,
            ema55_slope_lock_profit_trigger_r=ema55_slope_lock_profit_trigger_r,
            dynamic_first_lock_r=dynamic_first_lock_r,
            dynamic_trailing_step_r=dynamic_trailing_step_r,
            ema55_slope_negative_entry_bars=ema55_slope_negative_entry_bars,
            trend_ema_slope_filter_min_ratio=trend_ema_slope_filter_min_ratio,
            atr_percentile_filter_max=atr_percentile_filter_max,
            body_retest_breakdown_atr_multiplier=body_retest_breakdown_atr_multiplier,
            body_retest_retest_atr_multiplier=body_retest_retest_atr_multiplier,
            body_retest_stop_buffer_atr_multiplier=body_retest_stop_buffer_atr_multiplier,
            body_retest_body_atr_limit=body_retest_body_atr_limit,
            body_retest_watch_bars=body_retest_watch_bars,
            time_stop_break_even_enabled=time_stop_break_even_enabled,
            time_stop_break_even_bars=time_stop_break_even_bars,
            trend_ema_close_exit_after_trigger_r_enabled=trend_ema_close_exit_after_trigger_r_enabled,
            trend_ema_close_exit_after_trigger_r=trend_ema_close_exit_after_trigger_r,
            hold_close_exit_bars=hold_close_exit_bars,
            mtf_filter_bar=mtf_filter_bar,
            mtf_filter_fast_ema_period=mtf_filter_fast_ema_period,
            mtf_filter_slow_ema_period=mtf_filter_slow_ema_period,
            mtf_reversal_mode=mtf_reversal_mode,
            daily_filter_enabled=daily_filter_enabled,
            daily_filter_boundary=daily_filter_boundary,
            daily_filter_mode=daily_filter_mode,
            daily_filter_scope=daily_filter_scope,
            daily_filter_ma_type=daily_filter_ma_type,
            daily_filter_period=daily_filter_period,
            backtest_initial_capital=self._parse_positive_decimal(self.initial_capital.get(), "初始资金"),
            backtest_sizing_mode=sizing_mode,
            backtest_risk_percent=risk_percent,
            backtest_compounding=bool(self.compounding_enabled.get()),
            backtest_entry_slippage_rate=entry_slippage_rate,
            backtest_exit_slippage_rate=exit_slippage_rate,
            backtest_slippage_rate=exit_slippage_rate,
            backtest_funding_rate=self._parse_nonnegative_decimal(self.funding_rate_percent.get(), "资金费率/8h")
            / Decimal("100"),
            backtest_profile_id=self.backtest_profile_id.get().strip(),
            backtest_profile_name=self.backtest_profile_name.get().strip(),
            backtest_profile_summary=self.backtest_profile_summary.get().strip(),
        )

    def _selected_strategy_definition(self) -> StrategyDefinition:
        strategy_id = self._strategy_name_to_id[self.strategy_name.get()]
        return get_strategy_definition(strategy_id)

    def _on_strategy_selected(self, *_: object) -> None:
        self._apply_selected_strategy_definition()

    def _on_symbol_selected(self, *_: object) -> None:
        self._apply_symbol_specific_defaults_if_needed(clear_profile_origin=True)

    def _apply_selected_strategy_definition(self) -> None:
        definition = self._selected_strategy_definition()
        strategy_id = definition.strategy_id
        has_saved_draft = strategy_id in self._strategy_parameter_scope_drafts()
        previous_strategy_id = self._last_strategy_parameter_strategy_id
        if previous_strategy_id and previous_strategy_id != strategy_id:
            self._save_strategy_parameter_draft(previous_strategy_id)
        self._restore_strategy_parameter_draft(strategy_id)
        visibility = build_strategy_widget_visibility(strategy_id, "backtest")
        self.signal_combo["values"] = definition.allowed_signal_labels
        fixed_signal_mode = strategy_fixed_value(strategy_id, "signal_mode")
        if fixed_signal_mode is not None:
            self.signal_mode_label.set(SIGNAL_VALUE_TO_LABEL.get(str(fixed_signal_mode), definition.default_signal_label))
        elif self.signal_mode_label.get() not in definition.allowed_signal_labels:
            self.signal_mode_label.set(definition.default_signal_label)
        if not has_saved_draft:
            extra_defaults = strategy_ui_extra_defaults(strategy_id, "backtest")
            sizing_mode = extra_defaults.get("sizing_mode")
            if sizing_mode is not None:
                self.sizing_mode_label.set(BACKTEST_SIZING_VALUE_TO_LABEL.get(str(sizing_mode), str(sizing_mode)))
            risk_amount = extra_defaults.get("risk_amount")
            if risk_amount is not None:
                self.risk_amount.set(str(risk_amount))
        self._apply_symbol_specific_defaults_if_needed()
        if hasattr(self, "_controls_frame"):
            big_ema_widgets = (self.big_ema_caption, self.big_ema_entry)
            for widget in big_ema_widgets:
                if visibility.show_big_ema:
                    widget.grid()
                else:
                    widget.grid_remove()
            entry_reference_widgets = (
                self.entry_reference_ema_caption,
                self.entry_reference_ema_entry,
            )
            for widget in entry_reference_widgets:
                if visibility.show_entry_reference:
                    widget.grid()
                else:
                    widget.grid_remove()
            slope_threshold_widgets = (
                self.slope_threshold_caption,
                self.slope_threshold_entry,
                self.slope_threshold_hint,
            )
            for widget in slope_threshold_widgets:
                if visibility.show_slope_threshold:
                    widget.grid()
                else:
                    widget.grid_remove()
            btc_slope_entry_widgets = (
                self.ema55_slope_negative_entry_bars_caption,
                self.ema55_slope_negative_entry_bars_entry,
                self.ema55_slope_negative_entry_bars_hint,
            )
            show_btc_slope_entry_widgets = strategy_id == STRATEGY_BTC_EMA55_SLOPE_SHORT_ID
            for widget in btc_slope_entry_widgets:
                if show_btc_slope_entry_widgets:
                    widget.grid()
                else:
                    widget.grid_remove()
            mtf_widgets = (
                self.mtf_filter_bar_caption,
                self.mtf_filter_bar_caption_row,
                self.mtf_filter_bar_combo,
                self.mtf_filter_fast_ema_caption,
                self.mtf_filter_fast_ema_entry,
                self.mtf_filter_slow_ema_caption,
                self.mtf_filter_slow_ema_entry,
                self.mtf_reversal_mode_caption,
                self.mtf_reversal_mode_combo,
            )
            for widget in mtf_widgets:
                if visibility.show_mtf_controls:
                    widget.grid()
                else:
                    widget.grid_remove()
            daily_filter_widgets = (
                self.daily_filter_enabled_check,
                self.daily_filter_boundary_caption,
                self.daily_filter_boundary_combo,
                self.daily_filter_scope_caption,
                self.daily_filter_scope_combo,
                self.daily_filter_mode_caption,
                self.daily_filter_mode_combo,
                self.daily_filter_ma_caption,
                self.daily_filter_ma_combo,
                self.daily_filter_period_caption,
                self.daily_filter_period_entry,
                self.daily_filter_hint,
            )
            for widget in daily_filter_widgets:
                if visibility.show_daily_filter_controls:
                    widget.grid()
                else:
                    widget.grid_remove()
            atr_percentile_widgets = (
                self.atr_percentile_filter_caption,
                self.atr_percentile_filter_entry,
            )
            show_atr_percentile = strategy_uses_parameter(strategy_id, "atr_percentile_filter_max")
            for widget in atr_percentile_widgets:
                if show_atr_percentile:
                    widget.grid()
                else:
                    widget.grid_remove()
            body_retest_widgets = (
                self.body_retest_breakdown_caption,
                self.body_retest_breakdown_entry,
                self.body_retest_retest_caption,
                self.body_retest_retest_entry,
                self.body_retest_stop_buffer_caption,
                self.body_retest_stop_buffer_entry,
                self.body_retest_body_limit_caption,
                self.body_retest_body_limit_entry,
                self.body_retest_watch_bars_caption,
                self.body_retest_watch_bars_entry,
            )
            show_body_retest = strategy_uses_parameter(strategy_id, "body_retest_breakdown_atr_multiplier")
            for widget in body_retest_widgets:
                if show_body_retest:
                    widget.grid()
                else:
                    widget.grid_remove()
            dynamic_widgets = (
                self.take_profit_mode_caption,
                self.take_profit_mode_combo,
                self.dynamic_two_r_break_even_check,
                self.dynamic_break_even_trigger_r_label,
                self.dynamic_break_even_trigger_r_entry,
                self.dynamic_trailing_start_r_label,
                self.dynamic_trailing_start_r_entry,
                self.dynamic_first_lock_r_label,
                self.dynamic_first_lock_r_entry,
                self.dynamic_trailing_step_r_label,
                self.dynamic_trailing_step_r_entry,
                self._dynamic_protection_rules_card,
                self.dynamic_fee_offset_check,
                self.dynamic_fee_offset_hint_label,
                self.time_stop_break_even_check,
                self.time_stop_break_even_bars_label,
                self.time_stop_break_even_bars_entry,
                self.trend_ema_close_exit_after_trigger_r_enabled_check,
                self.trend_ema_close_exit_after_trigger_r_label,
                self.trend_ema_close_exit_after_trigger_r_entry,
                self.trend_ema_close_exit_after_trigger_r_hint_label,
            )
            for widget in dynamic_widgets:
                if visibility.show_dynamic_take_profit:
                    widget.grid()
                else:
                    widget.grid_remove()
            generic_slope_trigger_widgets = (
                self.dynamic_break_even_trigger_r_label,
                self.dynamic_break_even_trigger_r_entry,
            )
            show_generic_slope_trigger_widgets = strategy_uses_parameter(
                strategy_id,
                "ema55_slope_lock_profit_trigger_r",
            )
            for widget in generic_slope_trigger_widgets:
                if visibility.show_dynamic_take_profit and show_generic_slope_trigger_widgets:
                    widget.grid()
                else:
                    widget.grid_remove()
            slope_exit_widgets = (
                self.ema55_slope_exit_conditions_caption,
                self.ema55_slope_exit_enabled_check,
            )
            show_slope_exit_widgets = strategy_id in {
                STRATEGY_EMA55_SLOPE_SHORT_ID,
                STRATEGY_BTC_EMA55_SLOPE_SHORT_ID,
                STRATEGY_BODY_RETEST_SHORT_ID,
            }
            for widget in slope_exit_widgets:
                if show_slope_exit_widgets:
                    widget.grid()
                else:
                    widget.grid_remove()
            if strategy_uses_parameter(strategy_id, "trend_ema_close_exit_after_trigger_r_enabled"):
                self.trend_ema_close_exit_after_trigger_r_enabled_check.grid()
                self.trend_ema_close_exit_after_trigger_r_label.grid()
                self.trend_ema_close_exit_after_trigger_r_entry.grid()
                self.trend_ema_close_exit_after_trigger_r_hint_label.grid()
            else:
                self.trend_ema_close_exit_after_trigger_r_enabled_check.grid_remove()
                self.trend_ema_close_exit_after_trigger_r_label.grid_remove()
                self.trend_ema_close_exit_after_trigger_r_entry.grid_remove()
                self.trend_ema_close_exit_after_trigger_r_hint_label.grid_remove()
            btc_slope_exit_widgets = (
                self.ema55_slope_lock_profit_enabled_check,
                self.ema55_slope_lock_profit_trigger_r_label,
                self.ema55_slope_lock_profit_trigger_r_entry,
                self.ema55_slope_exit_conditions_hint,
            )
            show_btc_slope_exit_widgets = strategy_id == STRATEGY_BTC_EMA55_SLOPE_SHORT_ID
            for widget in btc_slope_exit_widgets:
                if show_btc_slope_exit_widgets:
                    widget.grid()
                else:
                    widget.grid_remove()
            max_entries_widgets = (self.max_entries_caption, self.max_entries_entry)
            for widget in max_entries_widgets:
                if visibility.show_max_entries:
                    widget.grid()
                else:
                    widget.grid_remove()
            if not visibility.show_max_entries:
                self.max_entries_caption.configure(text="每波最多开仓次数")
            reentry_widgets = (
                self.reentry_confirmation_check,
                self.reentry_confirmation_min_sequence_caption,
                self.reentry_confirmation_min_sequence_entry,
                self.reentry_confirmation_ma_frame,
            )
            for widget in reentry_widgets:
                if visibility.show_reentry_confirmation:
                    widget.grid()
                else:
                    widget.grid_remove()
            hold_close_widgets = (
                self.hold_close_exit_bars_caption,
                self.hold_close_exit_bars_entry,
                self.hold_close_exit_hint,
            )
            for widget in hold_close_widgets:
                if visibility.show_hold_close_exit:
                    widget.grid()
                else:
                    widget.grid_remove()
            self._set_field_state(self.bar_combo, editable=strategy_is_parameter_editable(strategy_id, "bar", "backtest"))
            self._set_field_state(self.ema_type_combo, editable=strategy_is_parameter_editable(strategy_id, "ema_type", "backtest"))
            self._set_field_state(self.ema_period_entry, editable=strategy_is_parameter_editable(strategy_id, "ema_period", "backtest"))
            self._set_field_state(
                self.trend_ema_type_combo,
                editable=strategy_is_parameter_editable(strategy_id, "trend_ema_type", "backtest"),
            )
            self._set_field_state(self.trend_ema_period_entry, editable=strategy_is_parameter_editable(strategy_id, "trend_ema_period", "backtest"))
            self._set_field_state(self.big_ema_entry, editable=strategy_is_parameter_editable(strategy_id, "big_ema_period", "backtest"))
            self._set_field_state(self.signal_combo, editable=strategy_is_parameter_editable(strategy_id, "signal_mode", "backtest"))
            self._set_field_state(self.mtf_filter_bar_combo, editable=strategy_is_parameter_editable(strategy_id, "mtf_filter_bar", "backtest"))
            self._set_field_state(self.mtf_filter_fast_ema_entry, editable=strategy_is_parameter_editable(strategy_id, "mtf_filter_fast_ema_period", "backtest"))
            self._set_field_state(self.mtf_filter_slow_ema_entry, editable=strategy_is_parameter_editable(strategy_id, "mtf_filter_slow_ema_period", "backtest"))
            self._set_field_state(self.mtf_reversal_mode_combo, editable=strategy_is_parameter_editable(strategy_id, "mtf_reversal_mode", "backtest"))
            if visibility.show_daily_filter_controls:
                self.daily_filter_enabled_check.configure(
                    state=(
                        "normal"
                        if strategy_is_parameter_editable(strategy_id, "daily_filter_enabled", "backtest")
                        else "disabled"
                    )
                )
            self._set_field_state(
                self.entry_reference_ema_type_combo,
                editable=strategy_is_parameter_editable(strategy_id, "entry_reference_ema_type", "backtest"),
            )
            self._set_field_state(
                self.slope_threshold_entry,
                editable=strategy_is_parameter_editable(strategy_id, "trend_ema_slope_filter_min_ratio", "backtest"),
            )
            self._set_field_state(
                self.atr_percentile_filter_entry,
                editable=strategy_is_parameter_editable(strategy_id, "atr_percentile_filter_max", "backtest"),
            )
            self._set_field_state(
                self.body_retest_breakdown_entry,
                editable=strategy_is_parameter_editable(
                    strategy_id,
                    "body_retest_breakdown_atr_multiplier",
                    "backtest",
                ),
            )
            self._set_field_state(
                self.body_retest_retest_entry,
                editable=strategy_is_parameter_editable(
                    strategy_id,
                    "body_retest_retest_atr_multiplier",
                    "backtest",
                ),
            )
            self._set_field_state(
                self.body_retest_stop_buffer_entry,
                editable=strategy_is_parameter_editable(
                    strategy_id,
                    "body_retest_stop_buffer_atr_multiplier",
                    "backtest",
                ),
            )
            self._set_field_state(
                self.body_retest_body_limit_entry,
                editable=strategy_is_parameter_editable(
                    strategy_id,
                    "body_retest_body_atr_limit",
                    "backtest",
                ),
            )
            self._set_field_state(
                self.body_retest_watch_bars_entry,
                editable=strategy_is_parameter_editable(
                    strategy_id,
                    "body_retest_watch_bars",
                    "backtest",
                ),
            )
            self._set_field_state(
                self.hold_close_exit_bars_entry,
                editable=strategy_is_parameter_editable(strategy_id, "hold_close_exit_bars", "backtest"),
            )
            self._apply_strategy_parameter_fixed_labels(strategy_id)
        if visibility.show_entry_reference and not self.entry_reference_ema_period.get().strip():
            self.entry_reference_ema_period.set("55")
        if visibility.show_reentry_confirmation:
            if not self.reentry_confirmation_min_sequence.get().strip():
                self.reentry_confirmation_min_sequence.set("0")
            if not self.reentry_confirmation_ma_type.get().strip():
                self.reentry_confirmation_ma_type.set("EMA")
            if not self.reentry_confirmation_ma_period.get().strip():
                self.reentry_confirmation_ma_period.set("21")
        self._last_strategy_parameter_strategy_id = strategy_id
        self._sync_dynamic_take_profit_controls()
        self._sync_ema55_slope_exit_condition_controls()
        self._sync_daily_filter_controls()
        self._refresh_profile_summary_text()
        self._update_sizing_mode_widgets()
        if self._latest_result is None:
            self.manual_summary.set("当前策略没有额外扩展统计。")
        if self._ui_alive():
            self.window.after_idle(self._sync_backtest_params_viewport)

    def _apply_symbol_specific_defaults_if_needed(self, *, clear_profile_origin: bool = False) -> None:
        definition = self._selected_strategy_definition()
        defaults = get_strategy_symbol_parameter_defaults(
            definition.strategy_id,
            self.symbol.get(),
            "backtest",
        )
        if not defaults:
            return
        bindings = self._strategy_parameter_bindings()
        for key, value in defaults.items():
            variable = bindings.get(key)
            if variable is None:
                continue
            if key == "bar":
                variable.set(_normalize_backtest_bar_label(str(value)))
            elif key == "take_profit_mode":
                variable.set(TAKE_PROFIT_MODE_VALUE_TO_LABEL.get(str(value), self.take_profit_mode_label.get()))
            elif key == "mtf_reversal_mode":
                variable.set(MTF_REVERSAL_MODE_VALUE_TO_LABEL.get(str(value), self.mtf_reversal_mode_label.get()))
            elif key == "dynamic_protection_rules":
                variable.set(json.dumps(dynamic_protection_rules_to_payload(value), ensure_ascii=False))
            elif key.endswith("_type"):
                variable.set(str(value).upper())
            else:
                variable.set(value)
        if clear_profile_origin:
            self.backtest_profile_id.set("")
            self.backtest_profile_name.set("")
            self.backtest_profile_summary.set("")
        self._rebuild_dynamic_protection_rule_editor()
        self._sync_dynamic_take_profit_controls()
        self._sync_ema55_slope_exit_condition_controls()
        self._sync_daily_filter_controls()
        self._refresh_profile_summary_text()

    def _sync_dynamic_take_profit_controls(self) -> None:
        if not hasattr(self, "dynamic_two_r_break_even_check"):
            return
        definition = self._selected_strategy_definition()
        dynamic_strategy = self._strategy_supports_dynamic_take_profit(definition.strategy_id)
        dynamic_take_profit = (
            dynamic_strategy and TAKE_PROFIT_MODE_OPTIONS.get(self.take_profit_mode_label.get(), "fixed") == "dynamic"
        )
        uses_dynamic_trigger_r = strategy_uses_parameter(
            definition.strategy_id,
            "ema55_slope_lock_profit_trigger_r",
        )
        uses_break_even_trigger_r = strategy_uses_parameter(
            definition.strategy_id,
            "dynamic_break_even_trigger_r",
        )
        uses_trailing_step_r = strategy_uses_parameter(
            definition.strategy_id,
            "dynamic_trailing_step_r",
        )
        uses_first_lock_r = strategy_uses_parameter(
            definition.strategy_id,
            "dynamic_first_lock_r",
        )
        self.dynamic_two_r_break_even_check.configure(state="normal" if dynamic_take_profit else "disabled")
        self.dynamic_break_even_trigger_r_label.configure(
            state="normal" if dynamic_take_profit and uses_break_even_trigger_r else "disabled"
        )
        self.dynamic_break_even_trigger_r_entry.configure(
            state="normal" if dynamic_take_profit and uses_break_even_trigger_r else "disabled"
        )
        self.dynamic_trailing_start_r_label.configure(
            state="normal" if dynamic_take_profit and uses_dynamic_trigger_r else "disabled"
        )
        self.dynamic_trailing_start_r_entry.configure(
            state="normal" if dynamic_take_profit and uses_dynamic_trigger_r else "disabled"
        )
        self.dynamic_first_lock_r_label.configure(
            state="normal" if dynamic_take_profit and uses_first_lock_r else "disabled"
        )
        self.dynamic_first_lock_r_entry.configure(
            state="normal" if dynamic_take_profit and uses_first_lock_r else "disabled"
        )
        self.dynamic_trailing_step_r_label.configure(
            state="normal" if dynamic_take_profit and uses_trailing_step_r else "disabled"
        )
        self.dynamic_trailing_step_r_entry.configure(
            state="normal" if dynamic_take_profit and uses_trailing_step_r else "disabled"
        )
        if self._dynamic_protection_add_rule_button is not None:
            self._dynamic_protection_add_rule_button.configure(state="normal" if dynamic_take_profit else "disabled")
        if self._dynamic_protection_restore_button is not None:
            self._dynamic_protection_restore_button.configure(state="normal" if dynamic_take_profit else "disabled")
        for row in self._dynamic_protection_rule_rows:
            row.trigger_entry.configure(state="normal" if dynamic_take_profit else "disabled")
            row.action_combo.configure(state="readonly" if dynamic_take_profit else "disabled")
            row.delete_button.configure(state="normal" if dynamic_take_profit else "disabled")
            if dynamic_take_profit:
                self._update_dynamic_protection_rule_row_state(row)
            else:
                row.lock_entry.configure(state="disabled")
                row.trail_mode_combo.configure(state="disabled")
                row.trail_every_entry.configure(state="disabled")
                row.trail_add_entry.configure(state="disabled")
        self.dynamic_fee_offset_check.configure(state="normal" if dynamic_take_profit else "disabled")
        self.time_stop_break_even_check.configure(state="normal" if dynamic_take_profit else "disabled")
        self.time_stop_break_even_bars_label.configure(state="normal" if dynamic_take_profit else "disabled")
        self.time_stop_break_even_bars_entry.configure(
            state="normal" if dynamic_take_profit and self.time_stop_break_even_enabled.get() else "disabled"
        )
        self.trend_ema_close_exit_after_trigger_r_enabled_check.configure(
            state=(
                "normal"
                if dynamic_take_profit
                and strategy_uses_parameter(definition.strategy_id, "trend_ema_close_exit_after_trigger_r_enabled")
                else "disabled"
            )
        )
        trend_ema_close_exit_enabled = (
            dynamic_take_profit
            and self.trend_ema_close_exit_after_trigger_r_enabled.get()
            and strategy_uses_parameter(definition.strategy_id, "trend_ema_close_exit_after_trigger_r")
        )
        self.trend_ema_close_exit_after_trigger_r_label.configure(
            state="normal" if trend_ema_close_exit_enabled else "disabled"
        )
        self.trend_ema_close_exit_after_trigger_r_entry.configure(
            state="normal" if trend_ema_close_exit_enabled else "disabled"
        )
        reentry_supported = strategy_uses_parameter(definition.strategy_id, "reentry_confirmation_enabled")
        reentry_enabled = reentry_supported and bool(self.reentry_confirmation_enabled.get())
        if hasattr(self, "reentry_confirmation_check"):
            self.reentry_confirmation_check.configure(state="normal" if reentry_supported else "disabled")
            self.reentry_confirmation_min_sequence_caption.configure(
                state="normal" if reentry_enabled else "disabled"
            )
            self.reentry_confirmation_min_sequence_entry.configure(
                state="normal" if reentry_enabled else "disabled"
            )
            self.reentry_confirmation_ma_type_combo.configure(
                state="readonly" if reentry_enabled else "disabled"
            )
            self.reentry_confirmation_ma_period_entry.configure(
                state="normal" if reentry_enabled else "disabled"
            )
            self.reentry_confirmation_rule_prefix.configure(
                state="normal" if reentry_enabled else "disabled"
            )
            self.reentry_confirmation_rule_suffix.configure(
                state="normal" if reentry_enabled else "disabled"
            )

    def _sync_ema55_slope_exit_condition_controls(self) -> None:
        if not hasattr(self, "ema55_slope_lock_profit_trigger_r_entry"):
            return
        definition = self._selected_strategy_definition()
        enabled = (
            definition.strategy_id == STRATEGY_BTC_EMA55_SLOPE_SHORT_ID
            and bool(self.ema55_slope_lock_profit_enabled.get())
        )
        state = "normal" if enabled else "disabled"
        self.ema55_slope_lock_profit_trigger_r_label.configure(state=state)
        self.ema55_slope_lock_profit_trigger_r_entry.configure(state=state)

    def _sync_daily_filter_controls(self) -> None:
        if not hasattr(self, "daily_filter_enabled_check"):
            return
        enabled = bool(self.daily_filter_enabled.get())
        mode = DAILY_FILTER_MODE_LABEL_TO_VALUE.get(self.daily_filter_mode_label.get(), "disabled")
        mode_active = enabled and mode != "disabled"
        ma_active = mode_active and mode == "close_vs_ma"
        self._set_field_state(self.daily_filter_boundary_combo, editable=mode_active)
        self._set_field_state(self.daily_filter_scope_combo, editable=mode_active)
        self._set_field_state(self.daily_filter_mode_combo, editable=enabled)
        self._set_field_state(self.daily_filter_ma_combo, editable=ma_active)
        self._set_field_state(self.daily_filter_period_entry, editable=ma_active)

    def clear_backtest_profile_origin(self) -> None:
        self.backtest_profile_id.set("")
        self.backtest_profile_name.set("")
        self.backtest_profile_summary.set("")
        self._refresh_profile_summary_text()

    def import_backtest_profile_bundle(self) -> None:
        source = filedialog.askopenfilename(
            parent=self.window,
            title="导入 Backtest Profile / Bundle",
            filetypes=(("JSON 文件", "*.json"), ("所有文件", "*.*")),
        )
        if not source:
            return
        source_path = Path(source)
        try:
            payload = json.loads(source_path.read_text(encoding="utf-8"))
            profiles: list[StrategyProfile] = []
            if isinstance(payload, dict) and isinstance(payload.get("profiles"), list):
                bundle = read_strategy_bundle(source_path)
                profiles = list(bundle.profiles)
            elif isinstance(payload, dict) and "profile_id" in payload and "config_snapshot" in payload:
                profiles = [StrategyProfile.from_payload(payload)]
            elif isinstance(payload, dict) and isinstance(payload.get("strategy_profile"), dict):
                profiles = [StrategyProfile.from_payload(payload.get("strategy_profile"))]
            else:
                config = _deserialize_strategy_config(payload if isinstance(payload, dict) else {})
                if config is None:
                    raise ValueError("文件里没有可识别的 Profile / Bundle / 回测参数快照。")
                definition = get_strategy_definition(str(config.strategy_id or "").strip())
                profiles = [
                    StrategyProfile(
                        profile_id=f"{config.strategy_id}:{config.inst_id}",
                        profile_name=f"{definition.name} | {config.inst_id}",
                        strategy_id=str(config.strategy_id or "").strip(),
                        symbol=str(config.inst_id or "").strip().upper(),
                        config_snapshot=_serialize_strategy_config(config),
                    )
                ]
            if not profiles:
                raise ValueError("导入文件里没有可用的 Profile。")
            self._apply_backtest_profile(self._select_profile_for_backtest_import(profiles))
        except Exception as exc:
            messagebox.showerror("导入失败", str(exc), parent=self.window)
            return
        self.report_summary.set(
            f"已导入 Profile：{self.backtest_profile_name.get().strip() or '-'} | 来源：{source_path.name}"
        )

    def _select_profile_for_backtest_import(self, profiles: list[StrategyProfile]) -> StrategyProfile:
        current_strategy_id = self._selected_strategy_definition().strategy_id
        current_symbol = self.symbol.get().strip().upper()
        exact = [item for item in profiles if item.strategy_id == current_strategy_id and item.symbol == current_symbol]
        if exact:
            return exact[0]
        same_strategy = [item for item in profiles if item.strategy_id == current_strategy_id]
        if len(same_strategy) == 1:
            return same_strategy[0]
        same_symbol = [item for item in profiles if item.symbol == current_symbol]
        if len(same_symbol) == 1:
            return same_symbol[0]
        return profiles[0]

    def _apply_backtest_profile(self, profile: StrategyProfile) -> None:
        config = _deserialize_strategy_config(dict(profile.config_snapshot))
        if config is None:
            raise ValueError("Profile 缺少有效的回测配置快照。")
        definition = get_strategy_definition(profile.strategy_id)
        self.strategy_name.set(definition.name)
        self._apply_selected_strategy_definition()
        self.symbol.set(str(profile.symbol or config.inst_id).strip().upper())
        self.symbol_combo.configure(values=_build_backtest_symbol_options(self.symbol.get()))
        self.bar_label.set(_normalize_backtest_bar_label(config.bar))
        self.ema_type.set(str(config.resolved_ema_type()).upper())
        self.ema_period.set(str(config.ema_period))
        self.trend_ema_type.set(str(config.resolved_trend_ema_type()).upper())
        self.trend_ema_period.set(str(config.trend_ema_period))
        self.big_ema_period.set(str(config.big_ema_period))
        self.entry_reference_ema_type.set(str(config.resolved_entry_reference_ema_type()).upper())
        self.entry_reference_ema_period.set(str(config.entry_reference_ema_period))
        self.reentry_confirmation_enabled.set(bool(config.reentry_confirmation_enabled))
        self.reentry_confirmation_min_sequence.set(str(config.reentry_confirmation_min_sequence))
        self.reentry_confirmation_ma_type.set(str(config.resolved_reentry_confirmation_ma_type()).upper())
        self.reentry_confirmation_ma_period.set(str(config.reentry_confirmation_ma_period))
        self.mtf_filter_bar.set(_normalize_backtest_bar_label(config.resolved_mtf_filter_bar()))
        self.mtf_filter_fast_ema_period.set(str(config.mtf_filter_fast_ema_period))
        self.mtf_filter_slow_ema_period.set(str(config.mtf_filter_slow_ema_period))
        self.daily_filter_enabled.set(bool(config.daily_filter_enabled))
        self.daily_filter_boundary_label.set(
            DAILY_FILTER_BOUNDARY_VALUE_TO_LABEL.get(str(config.daily_filter_boundary), "交易所1D")
        )
        self.daily_filter_mode_label.set(
            DAILY_FILTER_MODE_VALUE_TO_LABEL.get(str(config.daily_filter_mode), "关闭")
        )
        self.daily_filter_scope_label.set(
            DAILY_FILTER_SCOPE_VALUE_TO_LABEL.get(str(config.daily_filter_scope), "多空都过滤")
        )
        self.daily_filter_ma_type.set(str(config.daily_filter_ma_type or "ema").upper())
        self.daily_filter_period.set(str(config.daily_filter_period))
        self.atr_period.set(str(config.atr_period))
        self.stop_atr.set(format_decimal(config.atr_stop_multiplier))
        self.take_atr.set(format_decimal(config.atr_take_multiplier))
        self.risk_amount.set("" if config.risk_amount is None else format_decimal(config.risk_amount))
        self.take_profit_mode_label.set(
            TAKE_PROFIT_MODE_VALUE_TO_LABEL.get(str(config.take_profit_mode), self.take_profit_mode_label.get())
        )
        self.max_entries_per_trend.set(str(config.max_entries_per_trend))
        self.dynamic_two_r_break_even.set(bool(config.dynamic_two_r_break_even))
        self.dynamic_break_even_trigger_r.set(str(max(int(config.dynamic_break_even_trigger_r), 1)))
        self.dynamic_protection_rules_json.set(
            json.dumps(dynamic_protection_rules_to_payload(config.resolved_dynamic_protection_rules()), ensure_ascii=False)
        )
        self.dynamic_fee_offset_enabled.set(bool(config.dynamic_fee_offset_enabled))
        self.time_stop_break_even_enabled.set(bool(config.time_stop_break_even_enabled))
        self.time_stop_break_even_bars.set(str(config.time_stop_break_even_bars))
        self.trend_ema_close_exit_after_trigger_r_enabled.set(
            bool(config.trend_ema_close_exit_after_trigger_r_enabled)
        )
        self.trend_ema_close_exit_after_trigger_r.set(str(config.resolved_trend_ema_close_exit_after_trigger_r()))
        self.hold_close_exit_bars.set(str(config.hold_close_exit_bars))
        self.trend_ema_slope_filter_min_ratio.set(format_decimal(config.trend_ema_slope_filter_min_ratio))
        self.atr_percentile_filter_max.set(format_decimal(config.atr_percentile_filter_max))
        self.body_retest_breakdown_atr_multiplier.set(format_decimal(config.body_retest_breakdown_atr_multiplier))
        self.body_retest_retest_atr_multiplier.set(format_decimal(config.body_retest_retest_atr_multiplier))
        self.body_retest_stop_buffer_atr_multiplier.set(format_decimal(config.body_retest_stop_buffer_atr_multiplier))
        self.body_retest_body_atr_limit.set(format_decimal(config.body_retest_body_atr_limit))
        self.body_retest_watch_bars.set(str(config.body_retest_watch_bars))
        self.ema55_slope_exit_enabled.set(bool(config.ema55_slope_exit_enabled))
        self.ema55_slope_lock_profit_enabled.set(bool(config.ema55_slope_lock_profit_enabled))
        self.ema55_slope_lock_profit_trigger_r.set(str(max(int(config.ema55_slope_lock_profit_trigger_r), 2)))
        self.dynamic_first_lock_r.set(str(max(int(config.dynamic_first_lock_r), 0)))
        self.dynamic_trailing_step_r.set(str(max(int(config.dynamic_trailing_step_r), 1)))
        self.ema55_slope_negative_entry_bars.set(str(max(int(config.ema55_slope_negative_entry_bars), 1)))
        self.signal_mode_label.set(SIGNAL_VALUE_TO_LABEL.get(str(config.signal_mode), self.signal_mode_label.get()))
        self.trade_mode_label.set(_reverse_lookup_label(TRADE_MODE_OPTIONS, config.trade_mode, self.trade_mode_label.get()))
        self.position_mode_label.set(
            _reverse_lookup_label(POSITION_MODE_OPTIONS, config.position_mode, self.position_mode_label.get())
        )
        self.trigger_type_label.set(
            _reverse_lookup_label(TRIGGER_TYPE_OPTIONS, config.tp_sl_trigger_type, self.trigger_type_label.get())
        )
        self.environment_label.set(_reverse_lookup_label(ENV_OPTIONS, config.environment, self.environment_label.get()))
        self.initial_capital.set(format_decimal(config.backtest_initial_capital))
        self.sizing_mode_label.set(
            BACKTEST_SIZING_VALUE_TO_LABEL.get(str(config.backtest_sizing_mode), self.sizing_mode_label.get())
        )
        self.risk_percent.set("" if config.backtest_risk_percent is None else format_decimal(config.backtest_risk_percent))
        self.compounding_enabled.set(bool(config.backtest_compounding))
        self.entry_slippage_percent.set(format_decimal(config.resolved_backtest_entry_slippage_rate() * Decimal("100")))
        self.exit_slippage_percent.set(format_decimal(config.resolved_backtest_exit_slippage_rate() * Decimal("100")))
        self.funding_rate_percent.set(format_decimal(config.backtest_funding_rate * Decimal("100")))
        self.backtest_profile_id.set(profile.profile_id)
        self.backtest_profile_name.set(profile.profile_name)
        self.backtest_profile_summary.set(config.daily_filter_summary())
        self._apply_strategy_parameter_fixed_values(definition.strategy_id, definition=definition)
        self._rebuild_dynamic_protection_rule_editor()
        self._refresh_profile_summary_text()
        self._sync_daily_filter_controls()
        self._sync_dynamic_take_profit_controls()
        self._sync_ema55_slope_exit_condition_controls()
        self._update_sizing_mode_widgets()

    def _append_backtest_snapshot(
        self,
        result: BacktestResult,
        config: StrategyConfig,
        candle_limit: int,
        *,
        batch_label: str | None = None,
        export_path: str | None = None,
    ) -> _BacktestSnapshot:
        self._backtest_snapshot_sequence += 1
        runtime_snapshot_id = f"R{self._backtest_snapshot_sequence:03d}"
        snapshot = _BacktestSnapshot(
            snapshot_id=runtime_snapshot_id,
            created_at=datetime.now(),
            config=config,
            candle_limit=candle_limit,
            candle_count=len(result.candles),
            start_ts=result.candles[0].ts if result.candles else None,
            end_ts=result.candles[-1].ts if result.candles else None,
            report=result.report,
            report_text=format_backtest_report(result),
            result=result,
            maker_fee_rate=result.maker_fee_rate,
            taker_fee_rate=result.taker_fee_rate,
            export_path=export_path,
            runtime_id=runtime_snapshot_id,
        )
        self._backtest_snapshots[snapshot.snapshot_id] = snapshot
        self._backtest_snapshot_order.append(snapshot.snapshot_id)
        if batch_label:
            self._batch_snapshot_groups.setdefault(batch_label, []).append(snapshot.snapshot_id)
            self._snapshot_batch_labels[snapshot.snapshot_id] = batch_label
        if self._ui_alive() and self._widget_exists(getattr(self, "compare_tree", None)):
            self._render_compare_tree(target_snapshot_id=snapshot.snapshot_id)
        archive_snapshot = get_backtest_snapshot_store().add_snapshot(
            result,
            config,
            candle_limit,
            runtime_snapshot_id=runtime_snapshot_id,
            export_path=export_path,
        )
        snapshot = replace(snapshot, archive_id=_archive_snapshot_id(archive_snapshot))
        self._backtest_snapshots[snapshot.snapshot_id] = snapshot
        if self._ui_alive() and self._widget_exists(getattr(self, "compare_tree", None)):
            self._render_compare_tree(target_snapshot_id=snapshot.snapshot_id)
        return snapshot

    def _update_compare_summary(self) -> None:
        total_count = len(self._backtest_snapshot_order)
        if total_count == 0:
            self.compare_summary.set("暂无回测对比记录。")
            return
        visible_count = len(self.compare_tree.get_children()) if self._widget_exists(getattr(self, "compare_tree", None)) else total_count
        summary = f"已保存 {total_count} 组回测结果。单击任一运行编号即可联动切换当前结果，详情里可查看对应归档编号。"
        if str(self.compare_filter_keyword.get()).strip():
            summary = f"{summary} 当前筛选：{visible_count}/{total_count}。"
        self.compare_summary.set(summary)
        return

    def _ordered_compare_snapshots(self) -> list[_BacktestSnapshot]:
        return [self._backtest_snapshots[snapshot_id] for snapshot_id in self._backtest_snapshot_order if snapshot_id in self._backtest_snapshots]

    def _filtered_compare_snapshots(self) -> list[_BacktestSnapshot]:
        return _filter_backtest_snapshots(self._ordered_compare_snapshots(), self.compare_filter_keyword.get())

    def _render_compare_tree(self, target_snapshot_id: str | None = None) -> None:
        if not self._widget_exists(getattr(self, "compare_tree", None)):
            return
        previous_selection = self.compare_tree.selection()
        snapshots = self._filtered_compare_snapshots()
        visible_ids = {snapshot.snapshot_id for snapshot in snapshots}
        self.compare_tree.delete(*self.compare_tree.get_children())
        for snapshot in snapshots:
            self.compare_tree.insert("", END, iid=snapshot.snapshot_id, values=_build_backtest_compare_row(snapshot))
        self._update_compare_summary()
        selected_id = None
        if target_snapshot_id and target_snapshot_id in visible_ids:
            selected_id = target_snapshot_id
        elif previous_selection and previous_selection[0] in visible_ids:
            selected_id = previous_selection[0]
        elif self._current_snapshot_id and self._current_snapshot_id in visible_ids:
            selected_id = self._current_snapshot_id
        elif snapshots:
            selected_id = snapshots[-1].snapshot_id
        if selected_id is not None:
            self.compare_tree.selection_set(selected_id)
            self.compare_tree.focus(selected_id)
            self.compare_tree.see(selected_id)
            self._update_compare_detail(self._backtest_snapshots[selected_id])
            self._show_batch_matrix_for_snapshot(selected_id)
            return
        self.compare_detail_text.delete("1.0", END)
        self._show_batch_matrix(None)

    def _clear_compare_filter(self) -> None:
        self.compare_filter_keyword.set("")
        self._render_compare_tree()

    def _on_compare_tree_selected(self, *_: object) -> None:
        snapshot = self._selected_compare_snapshot()
        if snapshot is None:
            self.compare_detail_text.delete("1.0", END)
            self._show_batch_matrix(None)
            self._populate_period_stats(self.monthly_stats_tree, [])
            self._populate_period_stats(self.yearly_stats_tree, [])
            return
        if snapshot.snapshot_id != self._current_snapshot_id:
            self._load_snapshot(snapshot.snapshot_id)
            return
        self._update_compare_detail(snapshot)
        self._show_batch_matrix_for_snapshot(snapshot.snapshot_id)

    def _update_compare_detail(self, snapshot: _BacktestSnapshot) -> None:
        self.compare_detail_text.delete("1.0", END)
        self.compare_detail_text.insert("1.0", _build_backtest_compare_detail(snapshot))

    def _next_batch_label(self) -> str:
        self._batch_sequence += 1
        return f"B{self._batch_sequence:03d}"

    def _show_batch_matrix_for_snapshot(self, snapshot_id: str | None) -> None:
        if snapshot_id is None:
            self._show_batch_matrix(None)
            return
        snapshot = self._backtest_snapshots.get(snapshot_id)
        if snapshot is not None and strategy_uses_dynamic_orders(snapshot.config.strategy_id) and snapshot.config.take_profit_mode != "dynamic":
            self.batch_entries_layer_label.set(_batch_entries_label(snapshot.config.max_entries_per_trend))
        self._show_batch_matrix(self._snapshot_batch_labels.get(snapshot_id))

    def _show_batch_matrix(self, batch_label: str | None) -> None:
        return self._show_batch_matrix_v2(batch_label)
        self._current_matrix_batch_label = batch_label
        for child in self.matrix_grid_frame.winfo_children():
            child.destroy()
        self._show_batch_heatmap(batch_label)

        if not batch_label:
            self.matrix_summary.set("\u5f53\u524d\u6240\u9009\u56de\u6d4b\u4e0d\u5c5e\u4e8e ATR \u6279\u91cf\u77e9\u9635\u3002")
            return

        snapshot_ids = self._batch_snapshot_groups.get(batch_label, [])
        snapshots = [self._backtest_snapshots[snapshot_id] for snapshot_id in snapshot_ids if snapshot_id in self._backtest_snapshots]
        if not snapshots:
            self.matrix_summary.set("\u5f53\u524d ATR \u6279\u91cf\u77e9\u9635\u6682\u65e0\u53ef\u7528\u6570\u636e\u3002")
            return

        signal_label = SIGNAL_VALUE_TO_LABEL.get(snapshots[0].config.signal_mode, snapshots[0].config.signal_mode)
        symbol_text = snapshots[0].config.inst_id
        bar_text = _normalize_backtest_bar_label(snapshots[0].config.bar)
        param_text = _build_backtest_param_summary(
            snapshots[0].config,
            maker_fee_rate=snapshots[0].maker_fee_rate,
            taker_fee_rate=snapshots[0].taker_fee_rate,
        )
        start_text, end_text = _backtest_snapshot_range_text(snapshots[0])
        self.matrix_summary.set(
            f"ATR \u77e9\u9635\u6279\u6b21\uff1a{batch_label} \uff5c \u4ea4\u6613\u5bf9\uff1a{symbol_text} \uff5c \u5468\u671f\uff1a{bar_text} \uff5c "
            f"\u53c2\u6570\u6458\u8981\uff1a{param_text} \uff5c \u4fe1\u53f7\u65b9\u5411\uff1a{signal_label} \uff5c "
            f"\u5f00\u59cb\u65f6\u95f4\uff1a{start_text} \uff5c \u7ed3\u675f\u65f6\u95f4\uff1a{end_text} \uff5c "
            f"\u5171 {len(snapshots)} \u7ec4\u7ed3\u679c\uff0c"
            "\u884c\u4e3a SL x1/1.5/2\uff0c\u5217\u4e3a TP = SL x1/2/3\u3002\u5355\u5143\u683c\u663e\u793a\u201c\u603b\u76c8\u4e8f | \u80dc\u7387 | \u4ea4\u6613\u6570\u201d\uff0c\u70b9\u51fb\u53ef\u52a0\u8f7d\u5bf9\u5e94\u56de\u6d4b\u3002"
        )

        ttk.Label(self.matrix_grid_frame, text="SL \\\\ TP", anchor="center").grid(
            row=0, column=0, sticky="nsew", padx=4, pady=4
        )
        for column, take_ratio in enumerate(ATR_BATCH_TAKE_RATIOS, start=1):
            ttk.Label(
                self.matrix_grid_frame,
                text=f"TP = SL x{format_decimal(take_ratio)}",
                anchor="center",
            ).grid(row=0, column=column, sticky="nsew", padx=4, pady=4)
            self.matrix_grid_frame.columnconfigure(column, weight=1)
        self.matrix_grid_frame.columnconfigure(0, weight=0)

        snapshot_map = {
            (snapshot.config.atr_stop_multiplier, snapshot.config.atr_take_multiplier): snapshot
            for snapshot in snapshots
        }
        for row, stop_multiplier in enumerate(ATR_BATCH_MULTIPLIERS, start=1):
            ttk.Label(
                self.matrix_grid_frame,
                text=f"SL x{format_decimal(stop_multiplier)}",
                anchor="center",
            ).grid(row=row, column=0, sticky="nsew", padx=4, pady=4)
            self.matrix_grid_frame.rowconfigure(row, weight=1)
            for column, take_ratio in enumerate(ATR_BATCH_TAKE_RATIOS, start=1):
                take_multiplier = stop_multiplier * take_ratio
                snapshot = snapshot_map.get((stop_multiplier, take_multiplier))
                if snapshot is None:
                    ttk.Label(
                        self.matrix_grid_frame,
                        text="--",
                        anchor="center",
                        relief="groove",
                        padding=8,
                    ).grid(row=row, column=column, sticky="nsew", padx=4, pady=4)
                    continue
                cell_text = (
                    f"{format_decimal_fixed(snapshot.report.total_pnl, 4)} | "
                    f"{format_decimal_fixed(snapshot.report.win_rate, 2)}% | "
                    f"{snapshot.report.total_trades}\u7b14"
                )
                ttk.Button(
                    self.matrix_grid_frame,
                    text=cell_text,
                    command=lambda sid=snapshot.snapshot_id: self._load_snapshot(sid),
                ).grid(row=row, column=column, sticky="nsew", padx=4, pady=4)

    def _show_batch_heatmap(self, batch_label: str | None) -> None:
        return self._show_batch_heatmap_v2(batch_label)
        canvas = getattr(self, "heatmap_canvas", None)
        if canvas is None:
            return
        canvas.delete("all")
        width = max(canvas.winfo_width(), 640)
        height = max(canvas.winfo_height(), 340)
        if not batch_label:
            self.heatmap_summary.set("参数热力图会在这里显示，可切换指标并单击单元格联动回测视图。")
            canvas.create_text(
                width / 2,
                height / 2,
                text="当前没有可显示的参数热力图。",
                fill="#6e7781",
                font=("Microsoft YaHei UI", 11),
            )
            return

        snapshot_ids = self._batch_snapshot_groups.get(batch_label, [])
        snapshots = [self._backtest_snapshots[snapshot_id] for snapshot_id in snapshot_ids if snapshot_id in self._backtest_snapshots]
        if not snapshots:
            self.heatmap_summary.set("当前批次暂无热力图数据。")
            canvas.create_text(width / 2, height / 2, text="当前批次暂无热力图数据。", fill="#6e7781")
            return

        metric_label = self.heatmap_metric.get()
        signal_label = SIGNAL_VALUE_TO_LABEL.get(snapshots[0].config.signal_mode, snapshots[0].config.signal_mode)
        symbol_text = snapshots[0].config.inst_id
        bar_text = _normalize_backtest_bar_label(snapshots[0].config.bar)
        self.heatmap_summary.set(
            f"批次：{batch_label} | 交易对：{symbol_text} | 周期：{bar_text} | 信号方向：{signal_label} | 指标：{metric_label}"
        )
        values = [_heatmap_metric_value(snapshot, metric_label) for snapshot in snapshots]
        min_value = min(values) if values else Decimal("0")
        max_value = max(values) if values else Decimal("0")
        left = 92
        top = 60
        right = 24
        bottom = 20
        grid_width = width - left - right
        grid_height = height - top - bottom
        cell_width = grid_width / max(len(ATR_BATCH_TAKE_RATIOS), 1)
        cell_height = grid_height / max(len(ATR_BATCH_MULTIPLIERS), 1)

        snapshot_map = {
            (snapshot.config.atr_stop_multiplier, snapshot.config.atr_take_multiplier): snapshot
            for snapshot in snapshots
        }
        canvas.create_rectangle(left, top, left + grid_width, top + grid_height, outline="#d0d7de", width=1)
        for column, take_ratio in enumerate(ATR_BATCH_TAKE_RATIOS):
            x1 = left + (column * cell_width)
            x2 = x1 + cell_width
            canvas.create_text(
                (x1 + x2) / 2,
                top - 22,
                text=f"TP = SL x{format_decimal(take_ratio)}",
                fill="#57606a",
                font=("Microsoft YaHei UI", 10, "bold"),
            )
        for row, stop_multiplier in enumerate(ATR_BATCH_MULTIPLIERS):
            y1 = top + (row * cell_height)
            y2 = y1 + cell_height
            canvas.create_text(
                left - 12,
                (y1 + y2) / 2,
                text=f"SL x{format_decimal(stop_multiplier)}",
                anchor="e",
                fill="#57606a",
                font=("Microsoft YaHei UI", 10, "bold"),
            )
            for column, take_ratio in enumerate(ATR_BATCH_TAKE_RATIOS):
                take_multiplier = stop_multiplier * take_ratio
                snapshot = snapshot_map.get((stop_multiplier, take_multiplier))
                x1 = left + (column * cell_width)
                y1 = top + (row * cell_height)
                x2 = x1 + cell_width
                y2 = y1 + cell_height
                fill = "#f3f4f6"
                text = "--"
                if snapshot is not None:
                    value = _heatmap_metric_value(snapshot, metric_label)
                    fill = _heatmap_fill_color(value, min_value, max_value)
                    text = _heatmap_metric_text(snapshot, metric_label)
                item_id = canvas.create_rectangle(x1, y1, x2, y2, fill=fill, outline="#d0d7de")
                text_id = canvas.create_text(
                    (x1 + x2) / 2,
                    (y1 + y2) / 2,
                    text=text,
                    width=cell_width - 14,
                    fill="#24292f",
                    font=("Microsoft YaHei UI", 11),
                )
                if snapshot is not None:
                    canvas.tag_bind(item_id, "<Button-1>", lambda _e, sid=snapshot.snapshot_id: self._load_snapshot(sid))
                    canvas.tag_bind(text_id, "<Button-1>", lambda _e, sid=snapshot.snapshot_id: self._load_snapshot(sid))

    def _refresh_current_batch_views(self) -> None:
        self._show_batch_matrix(self._current_matrix_batch_label)

    def _update_batch_layer_controls(self, batch_mode: str, levels: list[int]) -> None:
        labels = [_batch_entries_label(value) for value in levels]
        for combo in (getattr(self, "matrix_layer_combo", None), getattr(self, "heatmap_layer_combo", None)):
            if self._widget_exists(combo):
                combo.configure(values=labels)

        widgets = (
            getattr(self, "matrix_layer_caption", None),
            getattr(self, "matrix_layer_combo", None),
            getattr(self, "heatmap_layer_caption", None),
            getattr(self, "heatmap_layer_combo", None),
        )
        if batch_mode == "fixed_entries":
            if labels and self.batch_entries_layer_label.get() not in labels:
                self.batch_entries_layer_label.set(labels[0])
            for widget in widgets:
                if self._widget_exists(widget):
                    widget.grid()
            return

        if labels and self.batch_entries_layer_label.get() not in labels:
            self.batch_entries_layer_label.set(labels[0])
        for widget in widgets:
            if self._widget_exists(widget):
                widget.grid_remove()

    def _render_strategy_pool_matrix(self, snapshots: list[_BacktestSnapshot]) -> None:
        columns = min(3, max(len(snapshots), 1))
        for column in range(columns):
            self.matrix_grid_frame.columnconfigure(column, weight=1)
        rows = max((len(snapshots) + columns - 1) // columns, 1)
        for row in range(rows):
            self.matrix_grid_frame.rowconfigure(row, weight=1)

        for index, snapshot in enumerate(snapshots):
            row = index // columns
            column = index % columns
            frame = ttk.Frame(self.matrix_grid_frame, padding=12, relief="groove")
            frame.grid(row=row, column=column, sticky="nsew", padx=4, pady=4)
            frame.columnconfigure(0, weight=1)
            profile_name = strategy_pool_profile_name(snapshot.config)
            metrics_text = (
                f"总盈亏：{format_decimal_fixed(snapshot.report.total_pnl, 4)}\n"
                f"胜率：{format_decimal_fixed(snapshot.report.win_rate, 2)}%\n"
                f"交易数：{snapshot.report.total_trades}笔\n"
                f"PF：{format_decimal_fixed(snapshot.report.profit_factor or Decimal('0'), 2)}\n"
                f"平均R：{format_decimal_fixed(snapshot.report.average_r_multiple, 2)}"
            )
            ttk.Label(
                frame,
                text=profile_name,
                anchor="center",
                justify="center",
            ).grid(row=0, column=0, sticky="ew")
            ttk.Label(
                frame,
                text=(
                    f"{snapshot.config.backtest_profile_summary}\n"
                    f"{snapshot.config.ema_label()}/{snapshot.config.trend_ema_label()} | "
                    f"ATR{snapshot.config.atr_period} | "
                    f"SL x{format_decimal(snapshot.config.atr_stop_multiplier)} | "
                    f"TP x{format_decimal(snapshot.config.atr_take_multiplier)}"
                ),
                anchor="w",
                justify="left",
                wraplength=300,
            ).grid(row=1, column=0, sticky="ew", pady=(8, 8))
            ttk.Label(
                frame,
                text=metrics_text,
                anchor="w",
                justify="left",
            ).grid(row=2, column=0, sticky="ew")
            ttk.Button(
                frame,
                text="加载该候选",
                command=lambda sid=snapshot.snapshot_id: self._load_snapshot(sid),
            ).grid(row=3, column=0, sticky="ew", pady=(10, 0))

    def _show_batch_matrix_v2(self, batch_label: str | None) -> None:
        self._current_matrix_batch_label = batch_label
        for child in self.matrix_grid_frame.winfo_children():
            child.destroy()

        if not batch_label:
            self._update_batch_layer_controls("none", [])
            current_snapshot = self._backtest_snapshots.get(self._current_snapshot_id) if self._current_snapshot_id else None
            if current_snapshot is None:
                self.matrix_summary.set("当前没有可展示的参数矩阵。执行单组或批量回测后，这里会显示参数对比摘要。")
                self._show_batch_heatmap_v2(batch_label)
                return

            signal_label = SIGNAL_VALUE_TO_LABEL.get(
                current_snapshot.config.signal_mode,
                current_snapshot.config.signal_mode,
            )
            symbol_text = current_snapshot.config.inst_id
            bar_text = _normalize_backtest_bar_label(current_snapshot.config.bar)
            strategy_name = _strategy_display_name(current_snapshot.config)
            entry_reference_ema = current_snapshot.config.resolved_entry_reference_ema_period()
            take_profit_label = _backtest_exit_mode_label(current_snapshot.config)
            max_entries_label = (
                "不限(0)"
                if current_snapshot.config.max_entries_per_trend <= 0
                else str(current_snapshot.config.max_entries_per_trend)
            )
            param_text = _build_backtest_param_summary(
                current_snapshot.config,
                maker_fee_rate=current_snapshot.maker_fee_rate,
                taker_fee_rate=current_snapshot.taker_fee_rate,
            )
            start_text, end_text = _backtest_snapshot_range_text(current_snapshot)
            self.matrix_summary.set(
                "当前只保留 1 组结果，因此这里展示单组参数卡；"
                "批量回测时会自动生成参数矩阵。"
            )
            self.matrix_grid_frame.columnconfigure(0, weight=1)
            self.matrix_grid_frame.columnconfigure(1, weight=1)
            self.matrix_grid_frame.rowconfigure(1, weight=1)
            ttk.Label(self.matrix_grid_frame, text="当前参数", anchor="center").grid(
                row=0, column=0, sticky="nsew", padx=4, pady=4
            )
            ttk.Label(self.matrix_grid_frame, text="当前回测结果", anchor="center").grid(
                row=0, column=1, sticky="nsew", padx=4, pady=4
            )
            param_frame = ttk.Frame(self.matrix_grid_frame, padding=12, relief="groove")
            param_frame.grid(row=1, column=0, sticky="nsew", padx=4, pady=4)
            param_frame.columnconfigure(0, weight=1)
            param_frame.columnconfigure(1, weight=1)
            param_frame.columnconfigure(2, weight=1)
            ttk.Label(
                param_frame,
                text=f"策略：{strategy_name} ｜ 方向：{signal_label}",
                anchor="center",
                justify="center",
            ).grid(row=0, column=0, columnspan=3, sticky="ew", pady=(0, 8))
            ttk.Label(
                param_frame,
                text=f"挂单参考线\n{current_snapshot.config.entry_reference_line_label()}",
                anchor="center",
                justify="center",
                relief="ridge",
                padding=(8, 8),
            ).grid(row=1, column=0, sticky="nsew", padx=(0, 4))
            ttk.Label(
                param_frame,
                text=f"止盈模式\n{take_profit_label}",
                anchor="center",
                justify="center",
                relief="ridge",
                padding=(8, 8),
            ).grid(row=1, column=1, sticky="nsew", padx=4)
            ttk.Label(
                param_frame,
                text=f"每波开仓\n{max_entries_label}",
                anchor="center",
                justify="center",
                relief="ridge",
                padding=(8, 8),
            ).grid(row=1, column=2, sticky="nsew", padx=(4, 0))
            ttk.Label(
                param_frame,
                text=f"时间范围：{start_text} ~ {end_text}\n{param_text}",
                anchor="w",
                justify="left",
                wraplength=360,
            ).grid(row=2, column=0, columnspan=3, sticky="ew", pady=(10, 0))

            result_frame = ttk.Frame(self.matrix_grid_frame, padding=12, relief="groove")
            result_frame.grid(row=1, column=1, sticky="nsew", padx=4, pady=4)
            result_frame.columnconfigure(0, weight=1)
            ttk.Label(
                result_frame,
                text=" | ".join(_build_backtest_identity_parts(current_snapshot, prefer_archive=False)),
                anchor="center",
                justify="center",
            ).grid(row=0, column=0, sticky="ew", pady=(0, 8))
            ttk.Label(
                result_frame,
                text=(
                    f"总盈亏\n{format_decimal_fixed(current_snapshot.report.total_pnl, 4)}\n\n"
                    f"胜率\n{format_decimal_fixed(current_snapshot.report.win_rate, 2)}%\n\n"
                    f"交易数\n{current_snapshot.report.total_trades}笔\n\n"
                    f"最大回撤\n{format_decimal_fixed(current_snapshot.report.max_drawdown, 4)}"
                ),
                anchor="center",
                justify="center",
            ).grid(row=1, column=0, sticky="nsew")
            ttk.Button(
                result_frame,
                text="重新加载当前回测",
                command=lambda sid=current_snapshot.snapshot_id: self._load_snapshot(sid),
            ).grid(row=2, column=0, sticky="ew", pady=(10, 0))
            self._show_batch_heatmap_v2(batch_label)
            return

        snapshot_ids = self._batch_snapshot_groups.get(batch_label, [])
        snapshots = [self._backtest_snapshots[snapshot_id] for snapshot_id in snapshot_ids if snapshot_id in self._backtest_snapshots]
        if not snapshots:
            self._update_batch_layer_controls("none", [])
            self.matrix_summary.set("当前所选回测不属于批量参数对比。")
            self._show_batch_heatmap_v2(batch_label)
            return

        batch_mode = _batch_mode_for_snapshots(snapshots)
        ordered_snapshots = sorted(snapshots, key=lambda item: _snapshot_sort_key(item, batch_mode))
        levels = _batch_entry_levels(ordered_snapshots)
        self._update_batch_layer_controls(batch_mode, levels)

        signal_label = SIGNAL_VALUE_TO_LABEL.get(ordered_snapshots[0].config.signal_mode, ordered_snapshots[0].config.signal_mode)
        symbol_text = ordered_snapshots[0].config.inst_id
        bar_text = _normalize_backtest_bar_label(ordered_snapshots[0].config.bar)
        param_text = _build_backtest_param_summary(
            ordered_snapshots[0].config,
            maker_fee_rate=ordered_snapshots[0].maker_fee_rate,
            taker_fee_rate=ordered_snapshots[0].taker_fee_rate,
        )
        start_text, end_text = _backtest_snapshot_range_text(ordered_snapshots[0])

        if batch_mode == "strategy_pool":
            self.matrix_summary.set(
                f"5m 策略池批次：{batch_label} ｜ 交易对：{symbol_text} ｜ 周期：{bar_text} ｜ "
                f"方向：{signal_label} ｜ 开始时间：{start_text} ｜ 结束时间：{end_text} ｜ "
                f"共 {len(ordered_snapshots)} 组候选，批量深测固定使用 5m 候选参数，仅保留你的方向、槽位和费用设定。"
            )
            self._render_strategy_pool_matrix(ordered_snapshots)
            self._show_batch_heatmap_v2(batch_label)
            return

        if batch_mode == "atr_period_matrix":
            atr_periods = sorted({snapshot.config.atr_period for snapshot in ordered_snapshots})
            strategy_name = _strategy_display_name(ordered_snapshots[0].config)
            self.matrix_summary.set(
                f"{strategy_name}矩阵：{batch_label} | 交易对：{symbol_text} | 周期：{bar_text} | 参数摘要：{param_text} | "
                f"信号方向：{signal_label} | 开始时间：{start_text} | 结束时间：{end_text} | 共 {len(ordered_snapshots)} 组结果，"
                "行为 ATR 止损倍数 SL x1/1.5/2，列为 ATR 周期 ATR10/ATR14。单元格显示“总盈亏 | 胜率 | 交易数”，点击可加载对应回测。"
            )
            self.matrix_grid_frame.columnconfigure(0, weight=0)
            ttk.Label(self.matrix_grid_frame, text="SL \\\\ ATR", anchor="center").grid(
                row=0, column=0, sticky="nsew", padx=4, pady=4
            )
            for column, atr_period in enumerate(atr_periods, start=1):
                self.matrix_grid_frame.columnconfigure(column, weight=1)
                ttk.Label(
                    self.matrix_grid_frame,
                    text=f"ATR{atr_period}",
                    anchor="center",
                ).grid(row=0, column=column, sticky="nsew", padx=4, pady=4)
            snapshot_map = {
                (snapshot.config.atr_stop_multiplier, snapshot.config.atr_period): snapshot
                for snapshot in ordered_snapshots
            }
            for row, stop_multiplier in enumerate(ATR_BATCH_MULTIPLIERS, start=1):
                ttk.Label(
                    self.matrix_grid_frame,
                    text=f"SL x{format_decimal(stop_multiplier)}",
                    anchor="center",
                ).grid(row=row, column=0, sticky="nsew", padx=4, pady=4)
                self.matrix_grid_frame.rowconfigure(row, weight=1)
                for column, atr_period in enumerate(atr_periods, start=1):
                    snapshot = snapshot_map.get((stop_multiplier, atr_period))
                    if snapshot is None:
                        ttk.Label(
                            self.matrix_grid_frame,
                            text="--",
                            anchor="center",
                            relief="groove",
                            padding=8,
                        ).grid(row=row, column=column, sticky="nsew", padx=4, pady=4)
                        continue
                    cell_text = (
                        f"{format_decimal_fixed(snapshot.report.total_pnl, 4)} | "
                        f"{format_decimal_fixed(snapshot.report.win_rate, 2)}% | "
                        f"{snapshot.report.total_trades}笔"
                    )
                    ttk.Button(
                        self.matrix_grid_frame,
                        text=cell_text,
                        command=lambda sid=snapshot.snapshot_id: self._load_snapshot(sid),
                    ).grid(row=row, column=column, sticky="nsew", padx=4, pady=4)
            self._show_batch_heatmap_v2(batch_label)
            return

        if batch_mode == "dynamic_entries":
            self.matrix_summary.set(
                f"动态止盈批次：{batch_label} ｜ 交易对：{symbol_text} ｜ 周期：{bar_text} ｜ 参数摘要：{param_text} ｜ "
                f"信号方向：{signal_label} ｜ 开始时间：{start_text} ｜ 结束时间：{end_text} ｜ 共 {len(ordered_snapshots)} 组结果，"
                "行为止损倍数 SL x1/1.5/2，列为每波最多开仓次数 0/1/2/3。单元格显示“总盈亏 | 胜率 | 交易数”，点击可加载对应回测。"
            )
            self.matrix_grid_frame.columnconfigure(0, weight=0)
            ttk.Label(self.matrix_grid_frame, text="SL \\\\ 开仓次数", anchor="center").grid(
                row=0, column=0, sticky="nsew", padx=4, pady=4
            )
            for column, entry_limit in enumerate(levels, start=1):
                self.matrix_grid_frame.columnconfigure(column, weight=1)
                ttk.Label(
                    self.matrix_grid_frame,
                    text=_batch_entries_label(entry_limit),
                    anchor="center",
                ).grid(row=0, column=column, sticky="nsew", padx=4, pady=4)
            snapshot_map = {
                (snapshot.config.atr_stop_multiplier, snapshot.config.max_entries_per_trend): snapshot
                for snapshot in ordered_snapshots
            }
            for row, stop_multiplier in enumerate(ATR_BATCH_MULTIPLIERS, start=1):
                ttk.Label(
                    self.matrix_grid_frame,
                    text=f"SL x{format_decimal(stop_multiplier)}",
                    anchor="center",
                ).grid(row=row, column=0, sticky="nsew", padx=4, pady=4)
                self.matrix_grid_frame.rowconfigure(row, weight=1)
                for column, entry_limit in enumerate(levels, start=1):
                    snapshot = snapshot_map.get((stop_multiplier, entry_limit))
                    if snapshot is None:
                        ttk.Label(
                            self.matrix_grid_frame,
                            text="--",
                            anchor="center",
                            relief="groove",
                            padding=8,
                        ).grid(row=row, column=column, sticky="nsew", padx=4, pady=4)
                        continue
                    cell_text = (
                        f"{format_decimal_fixed(snapshot.report.total_pnl, 4)} | "
                        f"{format_decimal_fixed(snapshot.report.win_rate, 2)}% | "
                        f"{snapshot.report.total_trades}笔"
                    )
                    ttk.Button(
                        self.matrix_grid_frame,
                        text=cell_text,
                        command=lambda sid=snapshot.snapshot_id: self._load_snapshot(sid),
                    ).grid(row=row, column=column, sticky="nsew", padx=4, pady=4)
        else:
            selected_limit = _batch_entries_value_from_label(self.batch_entries_layer_label.get())
            filtered = (
                [snapshot for snapshot in ordered_snapshots if snapshot.config.max_entries_per_trend == selected_limit]
                if batch_mode == "fixed_entries"
                else ordered_snapshots
            )
            if batch_mode == "fixed_entries":
                self.matrix_summary.set(
                    f"ATR 矩阵批次：{batch_label} ｜ 交易对：{symbol_text} ｜ 周期：{bar_text} ｜ 参数摘要：{param_text} ｜ "
                    f"信号方向：{signal_label} ｜ 开始时间：{start_text} ｜ 结束时间：{end_text} ｜ 共 {len(ordered_snapshots)} 组结果，"
                    f"当前展示“每波最多开仓次数 = {_batch_entries_label(selected_limit)}”这一层的 3x3 SL/TP 矩阵。"
                )
            else:
                self.matrix_summary.set(
                    f"ATR 矩阵批次：{batch_label} ｜ 交易对：{symbol_text} ｜ 周期：{bar_text} ｜ 参数摘要：{param_text} ｜ "
                    f"信号方向：{signal_label} ｜ 开始时间：{start_text} ｜ 结束时间：{end_text} ｜ 共 {len(ordered_snapshots)} 组结果，"
                    "行为 SL x1/1.5/2，列为 TP = SL x1/2/3。单元格显示“总盈亏 | 胜率 | 交易数”，点击可加载对应回测。"
                )

            ttk.Label(self.matrix_grid_frame, text="SL \\\\ TP", anchor="center").grid(
                row=0, column=0, sticky="nsew", padx=4, pady=4
            )
            for column, take_ratio in enumerate(ATR_BATCH_TAKE_RATIOS, start=1):
                ttk.Label(
                    self.matrix_grid_frame,
                    text=f"TP = SL x{format_decimal(take_ratio)}",
                    anchor="center",
                ).grid(row=0, column=column, sticky="nsew", padx=4, pady=4)
                self.matrix_grid_frame.columnconfigure(column, weight=1)
            self.matrix_grid_frame.columnconfigure(0, weight=0)

            snapshot_map = {
                (snapshot.config.atr_stop_multiplier, snapshot.config.atr_take_multiplier): snapshot
                for snapshot in filtered
            }
            for row, stop_multiplier in enumerate(ATR_BATCH_MULTIPLIERS, start=1):
                ttk.Label(
                    self.matrix_grid_frame,
                    text=f"SL x{format_decimal(stop_multiplier)}",
                    anchor="center",
                ).grid(row=row, column=0, sticky="nsew", padx=4, pady=4)
                self.matrix_grid_frame.rowconfigure(row, weight=1)
                for column, take_ratio in enumerate(ATR_BATCH_TAKE_RATIOS, start=1):
                    take_multiplier = stop_multiplier * take_ratio
                    snapshot = snapshot_map.get((stop_multiplier, take_multiplier))
                    if snapshot is None:
                        ttk.Label(
                            self.matrix_grid_frame,
                            text="--",
                            anchor="center",
                            relief="groove",
                            padding=8,
                        ).grid(row=row, column=column, sticky="nsew", padx=4, pady=4)
                        continue
                    cell_text = (
                        f"{format_decimal_fixed(snapshot.report.total_pnl, 4)} | "
                        f"{format_decimal_fixed(snapshot.report.win_rate, 2)}% | "
                        f"{snapshot.report.total_trades}笔"
                    )
                    ttk.Button(
                        self.matrix_grid_frame,
                        text=cell_text,
                        command=lambda sid=snapshot.snapshot_id: self._load_snapshot(sid),
                    ).grid(row=row, column=column, sticky="nsew", padx=4, pady=4)

        self._sync_matrix_grid_viewport()
        self._show_batch_heatmap_v2(batch_label)

    def _show_batch_heatmap_v2(self, batch_label: str | None) -> None:
        canvas = getattr(self, "heatmap_canvas", None)
        if canvas is None:
            return
        canvas.delete("all")
        width = max(canvas.winfo_width(), 640)
        height = max(canvas.winfo_height(), 340)
        if not batch_label:
            current_snapshot = self._backtest_snapshots.get(self._current_snapshot_id) if self._current_snapshot_id else None
            if current_snapshot is None:
                self.heatmap_summary.set("参数热力图会在这里显示，可切换指标并单击单元格联动回测视图。")
                canvas.create_text(
                    width / 2,
                    height / 2,
                    text="当前没有可显示的参数热力图。",
                    fill="#6e7781",
                    font=("Microsoft YaHei UI", 11),
                )
                self._sync_heatmap_canvas_scrollregion(width, height)
                return

            self.heatmap_summary.set(
                f"当前参数单组回测：{current_snapshot.config.inst_id} | {_normalize_backtest_bar_label(current_snapshot.config.bar)} | "
                "热力图仅在批量参数对比时生成。"
            )
            canvas.create_text(
                width / 2,
                height / 2,
                text="当前为单组回测，暂无参数热力图。\n如需热力图，请执行批量参数回测。",
                fill="#6e7781",
                font=("Microsoft YaHei UI", 11),
            )
            self._sync_heatmap_canvas_scrollregion(width, height)
            return

        snapshot_ids = self._batch_snapshot_groups.get(batch_label, [])
        snapshots = [self._backtest_snapshots[snapshot_id] for snapshot_id in snapshot_ids if snapshot_id in self._backtest_snapshots]
        if not snapshots:
            self.heatmap_summary.set("当前批次暂无热力图数据。")
            canvas.create_text(width / 2, height / 2, text="当前批次暂无热力图数据。", fill="#6e7781")
            return

        batch_mode = _batch_mode_for_snapshots(snapshots)
        ordered_snapshots = sorted(snapshots, key=lambda item: _snapshot_sort_key(item, batch_mode))
        levels = _batch_entry_levels(ordered_snapshots)
        metric_label = self.heatmap_metric.get()
        signal_label = SIGNAL_VALUE_TO_LABEL.get(ordered_snapshots[0].config.signal_mode, ordered_snapshots[0].config.signal_mode)
        symbol_text = ordered_snapshots[0].config.inst_id
        bar_text = _normalize_backtest_bar_label(ordered_snapshots[0].config.bar)
        if batch_mode == "strategy_pool":
            render_snapshots = ordered_snapshots
            self.heatmap_summary.set(
                f"批次：{batch_label} | 交易对：{symbol_text} | 周期：{bar_text} | 方向：{signal_label} | "
                f"指标：{metric_label} | 当前为 5m 候选策略池。"
            )
        elif batch_mode == "atr_period_matrix":
            render_snapshots = ordered_snapshots
            strategy_name = _strategy_display_name(ordered_snapshots[0].config)
            self.heatmap_summary.set(
                f"批次：{batch_label} | 交易对：{symbol_text} | 周期：{bar_text} | 信号方向：{signal_label} | "
                f"指标：{metric_label} | 当前为 {strategy_name} 矩阵，按 ATR 周期 x ATR 止损倍数显示 2 x 3 对比。"
            )
        elif batch_mode == "dynamic_entries":
            render_snapshots = ordered_snapshots
            self.heatmap_summary.set(
                f"批次：{batch_label} | 交易对：{symbol_text} | 周期：{bar_text} | 信号方向：{signal_label} | "
                f"指标：{metric_label} | 当前为动态止盈模式，按止损倍数 x 开仓次数显示 3 x 4 对比。"
            )
        elif batch_mode == "fixed_entries":
            selected_limit = _batch_entries_value_from_label(self.batch_entries_layer_label.get())
            render_snapshots = [snapshot for snapshot in ordered_snapshots if snapshot.config.max_entries_per_trend == selected_limit]
            self.heatmap_summary.set(
                f"批次：{batch_label} | 交易对：{symbol_text} | 周期：{bar_text} | 信号方向：{signal_label} | "
                f"指标：{metric_label} | 当前热力图层：每波最多开仓次数 = {_batch_entries_label(selected_limit)}。"
            )
        else:
            render_snapshots = ordered_snapshots
            self.heatmap_summary.set(
                f"批次：{batch_label} | 交易对：{symbol_text} | 周期：{bar_text} | 信号方向：{signal_label} | 指标：{metric_label}"
            )

        values = [_heatmap_metric_value(snapshot, metric_label) for snapshot in render_snapshots]
        min_value = min(values) if values else Decimal("0")
        max_value = max(values) if values else Decimal("0")
        left = 92
        top = 60
        right = 24
        bottom = 20
        grid_width = width - left - right
        grid_height = height - top - bottom
        canvas.create_rectangle(left, top, left + grid_width, top + grid_height, outline="#d0d7de", width=1)

        if batch_mode == "strategy_pool":
            columns = min(3, max(len(render_snapshots), 1))
            rows = max((len(render_snapshots) + columns - 1) // columns, 1)
            cell_width = grid_width / max(columns, 1)
            cell_height = grid_height / max(rows, 1)
            for index, snapshot in enumerate(render_snapshots):
                row = index // columns
                column = index % columns
                x1 = left + (column * cell_width)
                y1 = top + (row * cell_height)
                x2 = x1 + cell_width
                y2 = y1 + cell_height
                value = _heatmap_metric_value(snapshot, metric_label)
                fill = _heatmap_fill_color(value, min_value, max_value)
                item_id = canvas.create_rectangle(x1, y1, x2, y2, fill=fill, outline="#d0d7de")
                text_id = canvas.create_text(
                    (x1 + x2) / 2,
                    (y1 + y2) / 2,
                    text=f"{strategy_pool_profile_name(snapshot.config)}\n{_heatmap_metric_text(snapshot, metric_label)}",
                    width=cell_width - 18,
                    fill="#24292f",
                    font=("Microsoft YaHei UI", 11),
                )
                canvas.tag_bind(item_id, "<Button-1>", lambda _e, sid=snapshot.snapshot_id: self._load_snapshot(sid))
                canvas.tag_bind(text_id, "<Button-1>", lambda _e, sid=snapshot.snapshot_id: self._load_snapshot(sid))
            return

        if batch_mode == "atr_period_matrix":
            atr_periods = sorted({snapshot.config.atr_period for snapshot in render_snapshots})
            cell_width = grid_width / max(len(atr_periods), 1)
            cell_height = grid_height / max(len(ATR_BATCH_MULTIPLIERS), 1)
            snapshot_map = {
                (snapshot.config.atr_stop_multiplier, snapshot.config.atr_period): snapshot
                for snapshot in render_snapshots
            }
            for column, atr_period in enumerate(atr_periods):
                x1 = left + (column * cell_width)
                x2 = x1 + cell_width
                canvas.create_text(
                    (x1 + x2) / 2,
                    top - 22,
                    text=f"ATR{atr_period}",
                    fill="#57606a",
                    font=("Microsoft YaHei UI", 10, "bold"),
                )
            for row, stop_multiplier in enumerate(ATR_BATCH_MULTIPLIERS):
                y1 = top + (row * cell_height)
                y2 = y1 + cell_height
                canvas.create_text(
                    left - 12,
                    (y1 + y2) / 2,
                    text=f"SL x{format_decimal(stop_multiplier)}",
                    anchor="e",
                    fill="#57606a",
                    font=("Microsoft YaHei UI", 10, "bold"),
                )
                for column, atr_period in enumerate(atr_periods):
                    x1 = left + (column * cell_width)
                    x2 = x1 + cell_width
                    snapshot = snapshot_map.get((stop_multiplier, atr_period))
                    fill = "#f3f4f6"
                    text = "--"
                    if snapshot is not None:
                        value = _heatmap_metric_value(snapshot, metric_label)
                        fill = _heatmap_fill_color(value, min_value, max_value)
                        text = _heatmap_metric_text(snapshot, metric_label)
                    item_id = canvas.create_rectangle(x1, y1, x2, y2, fill=fill, outline="#d0d7de")
                    text_id = canvas.create_text(
                        (x1 + x2) / 2,
                        (y1 + y2) / 2,
                        text=text,
                        width=cell_width - 14,
                        fill="#24292f",
                        font=("Microsoft YaHei UI", 11),
                    )
                    if snapshot is not None:
                        canvas.tag_bind(item_id, "<Button-1>", lambda _e, sid=snapshot.snapshot_id: self._load_snapshot(sid))
                        canvas.tag_bind(text_id, "<Button-1>", lambda _e, sid=snapshot.snapshot_id: self._load_snapshot(sid))
            return

        if batch_mode == "dynamic_entries":
            cell_width = grid_width / max(len(levels), 1)
            cell_height = grid_height / max(len(ATR_BATCH_MULTIPLIERS), 1)
            snapshot_map = {
                (snapshot.config.atr_stop_multiplier, snapshot.config.max_entries_per_trend): snapshot
                for snapshot in render_snapshots
            }
            for column, entry_limit in enumerate(levels):
                x1 = left + (column * cell_width)
                x2 = x1 + cell_width
                canvas.create_text(
                    (x1 + x2) / 2,
                    top - 22,
                    text=_batch_entries_label(entry_limit),
                    fill="#57606a",
                    font=("Microsoft YaHei UI", 10, "bold"),
                )
            for row, stop_multiplier in enumerate(ATR_BATCH_MULTIPLIERS):
                y1 = top + (row * cell_height)
                y2 = y1 + cell_height
                canvas.create_text(
                    left - 12,
                    (y1 + y2) / 2,
                    text=f"SL x{format_decimal(stop_multiplier)}",
                    anchor="e",
                    fill="#57606a",
                    font=("Microsoft YaHei UI", 10, "bold"),
                )
                for column, entry_limit in enumerate(levels):
                    x1 = left + (column * cell_width)
                    x2 = x1 + cell_width
                    snapshot = snapshot_map.get((stop_multiplier, entry_limit))
                    fill = "#f3f4f6"
                    text = "--"
                    if snapshot is not None:
                        value = _heatmap_metric_value(snapshot, metric_label)
                        fill = _heatmap_fill_color(value, min_value, max_value)
                        text = _heatmap_metric_text(snapshot, metric_label)
                    item_id = canvas.create_rectangle(x1, y1, x2, y2, fill=fill, outline="#d0d7de")
                    text_id = canvas.create_text(
                        (x1 + x2) / 2,
                        (y1 + y2) / 2,
                        text=text,
                        width=cell_width - 14,
                        fill="#24292f",
                        font=("Microsoft YaHei UI", 11),
                    )
                    if snapshot is not None:
                        canvas.tag_bind(item_id, "<Button-1>", lambda _e, sid=snapshot.snapshot_id: self._load_snapshot(sid))
                        canvas.tag_bind(text_id, "<Button-1>", lambda _e, sid=snapshot.snapshot_id: self._load_snapshot(sid))
            self._sync_heatmap_canvas_scrollregion(width, height)
            return

        cell_width = grid_width / max(len(ATR_BATCH_TAKE_RATIOS), 1)
        cell_height = grid_height / max(len(ATR_BATCH_MULTIPLIERS), 1)
        snapshot_map = {
            (snapshot.config.atr_stop_multiplier, snapshot.config.atr_take_multiplier): snapshot
            for snapshot in render_snapshots
        }
        for column, take_ratio in enumerate(ATR_BATCH_TAKE_RATIOS):
            x1 = left + (column * cell_width)
            x2 = x1 + cell_width
            canvas.create_text(
                (x1 + x2) / 2,
                top - 22,
                text=f"TP = SL x{format_decimal(take_ratio)}",
                fill="#57606a",
                font=("Microsoft YaHei UI", 10, "bold"),
            )
        for row, stop_multiplier in enumerate(ATR_BATCH_MULTIPLIERS):
            y1 = top + (row * cell_height)
            y2 = y1 + cell_height
            canvas.create_text(
                left - 12,
                (y1 + y2) / 2,
                text=f"SL x{format_decimal(stop_multiplier)}",
                anchor="e",
                fill="#57606a",
                font=("Microsoft YaHei UI", 10, "bold"),
            )
            for column, take_ratio in enumerate(ATR_BATCH_TAKE_RATIOS):
                take_multiplier = stop_multiplier * take_ratio
                snapshot = snapshot_map.get((stop_multiplier, take_multiplier))
                x1 = left + (column * cell_width)
                y1 = top + (row * cell_height)
                x2 = x1 + cell_width
                y2 = y1 + cell_height
                fill = "#f3f4f6"
                text = "--"
                if snapshot is not None:
                    value = _heatmap_metric_value(snapshot, metric_label)
                    fill = _heatmap_fill_color(value, min_value, max_value)
                    text = _heatmap_metric_text(snapshot, metric_label)
                item_id = canvas.create_rectangle(x1, y1, x2, y2, fill=fill, outline="#d0d7de")
                text_id = canvas.create_text(
                    (x1 + x2) / 2,
                    (y1 + y2) / 2,
                    text=text,
                    width=cell_width - 14,
                    fill="#24292f",
                    font=("Microsoft YaHei UI", 11),
                )
                if snapshot is not None:
                    canvas.tag_bind(item_id, "<Button-1>", lambda _e, sid=snapshot.snapshot_id: self._load_snapshot(sid))
                    canvas.tag_bind(text_id, "<Button-1>", lambda _e, sid=snapshot.snapshot_id: self._load_snapshot(sid))
        self._sync_heatmap_canvas_scrollregion(width, height)

    def _selected_compare_snapshot(self) -> _BacktestSnapshot | None:
        selection = self.compare_tree.selection()
        if not selection:
            return None
        return self._backtest_snapshots.get(selection[0])

    def load_selected_snapshot(self) -> None:
        snapshot = self._selected_compare_snapshot()
        if snapshot is None:
            messagebox.showinfo("回测对比", "请先在“回测对比”里选中一条回测记录。", parent=self.window)
            return
        self._load_snapshot(snapshot.snapshot_id)

    def clear_backtest_snapshots(self) -> None:
        if not self._backtest_snapshot_order:
            return
        if not messagebox.askyesno("清空记录", "确定要清空当前窗口里的全部回测对比记录吗？", parent=self.window):
            return
        self._backtest_snapshots.clear()
        self._backtest_snapshot_order.clear()
        self._batch_snapshot_groups.clear()
        self._snapshot_batch_labels.clear()
        self._current_matrix_batch_label = None
        self._current_snapshot_id = None
        self._latest_result = None
        self.compare_tree.delete(*self.compare_tree.get_children())
        self.compare_detail_text.delete("1.0", END)
        self._update_compare_summary()
        self._show_batch_matrix(None)
        self.report_summary.set("暂无选中回测。")
        self.report_text.delete("1.0", END)
        self._clear_detail_tables()
        self._populate_period_stats(self.monthly_stats_tree, [])
        self._populate_period_stats(self.yearly_stats_tree, [])
        self._set_chart_title("K线图、资金曲线与止盈止损触发位置 | 暂无选中回测")
        if self._chart_zoom_canvas is not None and self._chart_zoom_canvas.winfo_exists():
            self._clear_chart_canvas(self._chart_zoom_canvas)
        self._refresh_zoom_chart_header()

    def _load_snapshot(self, snapshot_id: str) -> None:
        snapshot = self._backtest_snapshots[snapshot_id]
        result = snapshot.result
        self._current_snapshot_id = snapshot_id
        self._latest_result = result
        self._sync_extension_stats_tab(result)
        self._reset_chart_views()
        self._set_chart_title(self._build_chart_title_for_snapshot(snapshot))
        self._refresh_zoom_chart_header()
        self.report_summary.set(_build_backtest_header_summary(snapshot))
        self.report_text.delete("1.0", END)
        self.report_text.insert("1.0", _build_backtest_report_copy_text(snapshot))
        self._populate_trade_tree_with_result(result)
        self._populate_manual_tree(result, snapshot.config)
        if self.compare_tree.exists(snapshot.snapshot_id):
            self.compare_tree.selection_set(snapshot.snapshot_id)
            self.compare_tree.focus(snapshot.snapshot_id)
            self.compare_tree.see(snapshot.snapshot_id)
        self._update_compare_detail(snapshot)
        self._show_batch_matrix_for_snapshot(snapshot.snapshot_id)
        self._populate_period_stats(self.monthly_stats_tree, result.monthly_stats)
        self._populate_period_stats(self.yearly_stats_tree, result.yearly_stats)
        self._redraw_all_charts()

    def _bind_chart_interactions(self, canvas: Canvas) -> None:
        canvas.bind("<MouseWheel>", lambda event, target=canvas: self._on_chart_mousewheel(target, event))
        canvas.bind("<ButtonPress-1>", lambda event, target=canvas: self._on_chart_press(target, event))
        canvas.bind("<B1-Motion>", lambda event, target=canvas: self._on_chart_drag(target, event))
        canvas.bind("<ButtonRelease-1>", lambda event, target=canvas: self._on_chart_release(target))
        canvas.bind("<Motion>", lambda event, target=canvas: self._on_chart_motion(target, event))
        canvas.bind("<Leave>", lambda _event, target=canvas: self._clear_chart_hover(target))

    def _viewport_for_canvas(self, canvas: Canvas) -> _ChartViewport:
        if self._chart_zoom_canvas is not None and canvas is self._chart_zoom_canvas:
            return self._zoom_chart_view
        return self._main_chart_view

    def _reset_chart_views(self) -> None:
        self._main_chart_view = _ChartViewport()
        self._zoom_chart_view = _ChartViewport()

    def reset_main_chart_view(self) -> None:
        self._main_chart_view = _ChartViewport()
        self._redraw_all_charts()

    def reset_zoom_chart_view(self) -> None:
        self._zoom_chart_view = _ChartViewport()
        self._redraw_all_charts()

    def _on_chart_mousewheel(self, canvas: Canvas, event: object) -> None:
        if self._latest_result is None:
            return
        delta = getattr(event, "delta", 0)
        if delta == 0:
            return

        candles = self._latest_result.candles
        viewport = self._viewport_for_canvas(canvas)
        width = max(canvas.winfo_width(), 960)
        left = 56
        right = 20
        inner_width = max(width - left - right, 1)
        cursor_x = getattr(event, "x", left + inner_width / 2)
        anchor_ratio = min(max((cursor_x - left) / inner_width, 0.0), 1.0)
        next_start_index, visible_count = _zoom_chart_viewport(
            start_index=viewport.start_index,
            visible_count=viewport.visible_count,
            total_count=len(candles),
            anchor_ratio=anchor_ratio,
            zoom_in=delta > 0,
        )
        if next_start_index == viewport.start_index and visible_count == viewport.visible_count:
            return
        viewport.start_index = next_start_index
        viewport.visible_count = visible_count
        self._schedule_canvas_redraw(canvas)

    def _on_chart_press(self, canvas: Canvas, event: object) -> None:
        if self._latest_result is None:
            return
        viewport = self._viewport_for_canvas(canvas)
        viewport.pan_anchor_x = int(getattr(event, "x", 0))
        viewport.pan_anchor_start = viewport.start_index
        self._clear_chart_hover(canvas)

    def _on_chart_drag(self, canvas: Canvas, event: object) -> None:
        if self._latest_result is None:
            return
        viewport = self._viewport_for_canvas(canvas)
        if viewport.pan_anchor_x is None:
            return
        width = max(canvas.winfo_width(), 960)
        left = 56
        right = 20
        inner_width = max(width - left - right, 1)
        _, visible_count = _normalize_chart_viewport(
            viewport.start_index,
            viewport.visible_count,
            len(self._latest_result.candles),
        )
        candle_step = inner_width / max(visible_count, 1)
        current_x = int(getattr(event, "x", viewport.pan_anchor_x))
        shift = int(round((viewport.pan_anchor_x - current_x) / max(candle_step, 1)))
        next_start_index = _pan_chart_viewport(
            viewport.pan_anchor_start,
            visible_count,
            len(self._latest_result.candles),
            shift,
        )
        if next_start_index == viewport.start_index:
            return
        viewport.start_index = next_start_index
        self._schedule_canvas_redraw(canvas, delay_ms=24, fast_mode=True)

    def _on_chart_release(self, canvas: Canvas) -> None:
        viewport = self._viewport_for_canvas(canvas)
        viewport.pan_anchor_x = None
        self._schedule_canvas_redraw(canvas, delay_ms=0)

    def _on_chart_motion(self, canvas: Canvas, event: object) -> None:
        if self._latest_result is None:
            return
        state = self._chart_render_states.get(id(canvas))
        if state is None:
            return
        index = _chart_hover_index_for_x(
            x=float(getattr(event, "x", -1)),
            left=state.left,
            width=state.width - state.left - state.right,
            start_index=state.start_index,
            end_index=state.end_index,
            candle_step=state.candle_step,
        )
        current = self._chart_hover_indices.get(id(canvas))
        if current == index:
            return
        self._chart_hover_indices[id(canvas)] = index
        self._render_chart_hover(canvas)

    def _clear_chart_hover(self, canvas: Canvas) -> None:
        self._chart_hover_indices[id(canvas)] = None
        canvas.delete("chart-hover")

    def _on_chart_canvas_configure(self, canvas: Canvas) -> None:
        if self._latest_result is None or not self._widget_exists(canvas):
            return
        self._clear_chart_hover(canvas)
        self._schedule_canvas_redraw(canvas, delay_ms=48, fast_mode=True)
        self._schedule_canvas_finalize_redraw(canvas, delay_ms=180)

    def _draw_chart(self, result: BacktestResult, canvas: Canvas, *, fast_mode: bool = False) -> None:
        canvas.delete("all")
        candles = result.candles
        if not candles:
            return

        width = max(canvas.winfo_width(), 960)
        height = max(canvas.winfo_height(), 420)
        left = 56
        right = 20
        top = 20
        bottom = 30
        inner_width = width - left - right
        inner_height = height - top - bottom
        if inner_width <= 0 or inner_height <= 0:
            return

        panel_gap = 14
        drawdown_panel_height = max(72, min(110, int(inner_height * 0.16)))
        net_panel_height = max(84, min(150, int(inner_height * 0.22)))
        reserved_height = net_panel_height + drawdown_panel_height + (panel_gap * 2)
        if inner_height - reserved_height < 140:
            drawdown_panel_height = max(64, min(90, int(inner_height * 0.14)))
            net_panel_height = max(76, min(120, int(inner_height * 0.2)))
            panel_gap = 10
            reserved_height = net_panel_height + drawdown_panel_height + (panel_gap * 2)
        price_panel_height = max(inner_height - reserved_height, 120)
        price_bottom = top + price_panel_height
        net_top = price_bottom + panel_gap
        net_bottom = net_top + net_panel_height
        drawdown_top = net_bottom + panel_gap
        drawdown_bottom = height - bottom
        if drawdown_bottom <= drawdown_top:
            drawdown_top = net_bottom + 8
            drawdown_bottom = height - bottom

        viewport = self._viewport_for_canvas(canvas)
        if viewport.visible_count is None:
            start_index, visible_count = _default_chart_viewport(
                len(candles),
                DEFAULT_BACKTEST_CHART_VISIBLE_CANDLES,
            )
        else:
            start_index, visible_count = _normalize_chart_viewport(
                viewport.start_index,
                viewport.visible_count,
                len(candles),
            )
        viewport.start_index = start_index
        viewport.visible_count = visible_count
        end_index = start_index + visible_count
        self._chart_render_states[id(canvas)] = _ChartRenderState(
            left=left,
            right=right,
            top=top,
            bottom=bottom,
            price_bottom=price_bottom,
            net_top=net_top,
            net_bottom=net_bottom,
            drawdown_top=drawdown_top,
            drawdown_bottom=drawdown_bottom,
            width=width,
            height=height,
            start_index=start_index,
            end_index=end_index,
            candle_step=inner_width / max(visible_count, 1),
        )

        visible_candles = candles[start_index:end_index]
        visible_ema = result.ema_values[start_index:end_index]
        visible_trend_ema = result.trend_ema_values[start_index:end_index]
        visible_reference_ema = result.entry_reference_ema_values[start_index:end_index]
        visible_big_ema = (
            result.big_ema_values[start_index:end_index]
            if self._strategy_uses_big_ema(result.strategy_id)
            else []
        )
        visible_net_value = (
            result.net_value_curve[start_index:end_index]
            if result.net_value_curve
            else [Decimal("0") for _ in visible_candles]
        )
        visible_drawdown = (
            [Decimal("0") - value for value in result.drawdown_pct_curve[start_index:end_index]]
            if result.drawdown_pct_curve
            else [Decimal("0") for _ in visible_candles]
        )
        visible_trades = [
            trade
            for trade in result.trades
            if not (trade.exit_index < start_index or trade.entry_index >= end_index)
        ]
        visible_manual_positions = [
            manual_position
            for manual_position in result.manual_positions
            if not (manual_position.handoff_index < start_index or manual_position.entry_index >= end_index)
        ]
        max_render_candles = BACKTEST_CHART_FAST_RENDER_CANDLES if fast_mode else BACKTEST_CHART_FULL_RENDER_CANDLES
        render_stride = max(1, int(math.ceil(visible_count / max_render_candles)))
        dense_fast_mode = fast_mode and (visible_count >= 220 or len(visible_trades) >= 80)
        dense_display_mode = dense_fast_mode or render_stride > 1

        plotted_prices = [float(candle.high) for candle in visible_candles] + [float(candle.low) for candle in visible_candles]
        plotted_prices.extend(float(value) for value in visible_ema if value is not None)
        plotted_prices.extend(float(value) for value in visible_trend_ema if value is not None)
        plotted_prices.extend(float(value) for value in visible_reference_ema if value is not None)
        plotted_prices.extend(float(value) for value in visible_big_ema if value is not None)
        for trade in visible_trades:
            plotted_prices.extend(
                [
                    float(trade.entry_price),
                    float(trade.exit_price),
                    float(trade.stop_loss),
                    float(trade.take_profit),
                ]
            )
        for manual_position in visible_manual_positions:
            plotted_prices.extend(
                [
                    float(manual_position.entry_price),
                    float(manual_position.handoff_price),
                    float(manual_position.current_price),
                    float(manual_position.break_even_price),
                ]
            )
        price_max = max(plotted_prices)
        price_min = min(plotted_prices)
        if price_max == price_min:
            price_max += 1
            price_min -= 1

        def y_for(price: Decimal) -> float:
            ratio = (price_max - float(price)) / (price_max - price_min)
            return top + (ratio * price_panel_height)

        net_floor = min((float(value) for value in visible_net_value), default=0.0)
        net_ceiling = max((float(value) for value in visible_net_value), default=0.0)
        net_min = min(net_floor, 0.0)
        net_max = max(net_ceiling, 0.0)
        if net_max == net_min:
            padding = max(abs(net_max) * 0.1, 1.0)
            net_max += padding
            net_min -= padding

        def y_for_net_value(value: Decimal) -> float:
            ratio = (net_max - float(value)) / (net_max - net_min)
            return net_top + (ratio * max(net_bottom - net_top, 1))

        drawdown_floor = min((float(value) for value in visible_drawdown), default=0.0)
        drawdown_min = min(drawdown_floor, -0.01)
        drawdown_max = 0.0
        if drawdown_max == drawdown_min:
            drawdown_min -= 1.0

        def y_for_drawdown(value: Decimal) -> float:
            ratio = (drawdown_max - float(value)) / (drawdown_max - drawdown_min)
            return drawdown_top + (ratio * max(drawdown_bottom - drawdown_top, 1))

        candle_step = inner_width / max(visible_count, 1)

        def x_for(global_index: int) -> float:
            return left + ((global_index - start_index) * candle_step) + (candle_step / 2)

        body_width = max(2.0, candle_step * 0.6)

        canvas.create_rectangle(left, top, width - right, price_bottom, outline="#d0d7de")
        canvas.create_rectangle(left, net_top, width - right, net_bottom, outline="#d0d7de")
        canvas.create_rectangle(left, drawdown_top, width - right, drawdown_bottom, outline="#d0d7de")
        canvas.create_text(
            left,
            top - 6,
            text=f"显示 {start_index + 1}-{min(end_index, len(candles))} / {len(candles)} | 滚轮缩放 | 左键拖动 | 双击打开大窗",
            anchor="sw",
            fill="#57606a",
            font=("Microsoft YaHei UI", 9),
        )
        if result.daily_filter_enabled:
            boundary_labels = {
                "exchange": "Exchange 1D",
                "bjt_00": "BJT 00:00",
                "bjt_08": "BJT 08:00",
            }
            scope_labels = {
                "both": "Both",
                "long_only": "Long Only",
                "short_only": "Short Only",
            }
            if result.daily_filter_mode == "weak_day":
                filter_text = (
                    f"Daily Filter: {boundary_labels.get(result.daily_filter_boundary, result.daily_filter_boundary)} | "
                    f"Weak Day | {scope_labels.get(result.daily_filter_scope, result.daily_filter_scope)}"
                )
            else:
                filter_text = (
                    f"Daily Filter: {boundary_labels.get(result.daily_filter_boundary, result.daily_filter_boundary)} | "
                    f"{str(result.daily_filter_ma_type).upper()}{result.daily_filter_period} close-vs-MA | "
                    f"{scope_labels.get(result.daily_filter_scope, result.daily_filter_scope)}"
                )
            visible_bias = result.direction_filter_bias[start_index:end_index] if result.direction_filter_bias else []
            if visible_bias:
                allowed_long = sum(1 for item in visible_bias if item in {"long", "both"})
                allowed_short = sum(1 for item in visible_bias if item in {"short", "both"})
                filter_text = f"{filter_text} | L {allowed_long} / S {allowed_short}"
            canvas.create_text(
                left,
                top + 8,
                text=filter_text,
                anchor="nw",
                fill="#7c3aed",
                font=("Microsoft YaHei UI", 9, "bold"),
            )
        axis_steps = 1 if dense_fast_mode else (2 if fast_mode else 4)
        for price_value in _chart_price_axis_values(Decimal(str(price_min)), Decimal(str(price_max)), steps=axis_steps):
            y = y_for(price_value)
            canvas.create_line(left, y, width - right, y, fill="#eaeef2", dash=(2, 4))
            canvas.create_text(
                left - 8,
                y,
                text=_format_chart_axis_price(price_value),
                anchor="e",
                fill="#57606a",
                font=("Microsoft YaHei UI", 9),
            )

        net_axis_steps = 1 if fast_mode else 3
        for net_value in _chart_price_axis_values(Decimal(str(net_min)), Decimal(str(net_max)), steps=net_axis_steps):
            y = y_for_net_value(net_value)
            canvas.create_line(left, y, width - right, y, fill="#eef2f7", dash=(2, 4))
            canvas.create_text(
                left - 8,
                y,
                text=format_decimal_fixed(net_value, 2),
                anchor="e",
                fill="#57606a",
                font=("Microsoft YaHei UI", 9),
            )

        if net_min < 0 < net_max:
            zero_y = y_for_net_value(Decimal("0"))
            canvas.create_line(left, zero_y, width - right, zero_y, fill="#8c959f", dash=(4, 3))

        for drawdown_value in _chart_price_axis_values(
            Decimal(str(drawdown_min)),
            Decimal("0"),
            steps=1 if dense_fast_mode else (2 if fast_mode else 3),
        ):
            y = y_for_drawdown(drawdown_value)
            canvas.create_line(left, y, width - right, y, fill="#eef2f7", dash=(2, 4))
            canvas.create_text(
                left - 8,
                y,
                text=f"{format_decimal_fixed(abs(drawdown_value), 2)}%",
                anchor="e",
                fill="#57606a",
                font=("Microsoft YaHei UI", 9),
            )

        ema_points: list[float] = []
        if visible_ema:
            for index, ema_value in enumerate(visible_ema, start=start_index):
                if (index - start_index) % render_stride != 0 and index != end_index - 1:
                    continue
                if ema_value is None:
                    continue
                x = x_for(index)
                ema_points.extend((x, y_for(ema_value)))

        trend_ema_points: list[float] = []
        if visible_trend_ema:
            for index, trend_ema_value in enumerate(visible_trend_ema, start=start_index):
                if (index - start_index) % render_stride != 0 and index != end_index - 1:
                    continue
                if trend_ema_value is None:
                    continue
                x = x_for(index)
                trend_ema_points.extend((x, y_for(trend_ema_value)))

        reference_ema_points: list[float] = []
        if visible_reference_ema:
            for index, reference_ema_value in enumerate(visible_reference_ema, start=start_index):
                if (index - start_index) % render_stride != 0 and index != end_index - 1:
                    continue
                if reference_ema_value is None:
                    continue
                x = x_for(index)
                reference_ema_points.extend((x, y_for(reference_ema_value)))

        big_ema_points: list[float] = []
        if visible_big_ema:
            for index, big_ema_value in enumerate(visible_big_ema, start=start_index):
                if (index - start_index) % render_stride != 0 and index != end_index - 1:
                    continue
                if big_ema_value is None:
                    continue
                x = x_for(index)
                big_ema_points.extend((x, y_for(big_ema_value)))

        net_value_points: list[float] = []
        if visible_net_value:
            for index, net_value in enumerate(visible_net_value, start=start_index):
                if (index - start_index) % render_stride != 0 and index != end_index - 1:
                    continue
                x = x_for(index)
                net_value_points.extend((x, y_for_net_value(net_value)))

        drawdown_points: list[float] = []
        if visible_drawdown:
            for index, drawdown_value in enumerate(visible_drawdown, start=start_index):
                if (index - start_index) % render_stride != 0 and index != end_index - 1:
                    continue
                x = x_for(index)
                drawdown_points.extend((x, y_for_drawdown(drawdown_value)))

        for index, candle in enumerate(visible_candles, start=start_index):
            if (index - start_index) % render_stride != 0 and index != end_index - 1:
                continue
            x = x_for(index)
            open_y = y_for(candle.open)
            close_y = y_for(candle.close)
            high_y = y_for(candle.high)
            low_y = y_for(candle.low)
            color = _backtest_candle_color(candle.open, candle.close)
            if dense_display_mode:
                canvas.create_line(x, high_y, x, low_y, fill=color, width=max(1, int(round(min(body_width, 3.0)))))
                continue
            canvas.create_line(x, high_y, x, low_y, fill=color, width=1)
            body_top = min(open_y, close_y)
            body_bottom = max(open_y, close_y)
            if abs(body_bottom - body_top) < 1:
                body_bottom = body_top + 1
            canvas.create_rectangle(
                x - body_width / 2,
                body_top,
                x + body_width / 2,
                body_bottom,
                outline=color,
                fill=color,
            )

        for trade in visible_trades:
            entry_x = x_for(trade.entry_index)
            exit_x = x_for(trade.exit_index)
            entry_y = y_for(trade.entry_price)
            exit_y = y_for(trade.exit_price)
            stop_y = y_for(trade.stop_loss)
            take_y = y_for(trade.take_profit)
            trade_color = "#0969da" if trade.signal == "long" else "#8250df"
            exit_color = "#1a7f37" if not is_stop_exit_reason(trade.exit_reason) else "#d1242f"

            canvas.create_line(entry_x, entry_y, exit_x, exit_y, fill=trade_color, width=2)
            if fast_mode:
                if not dense_fast_mode:
                    canvas.create_oval(entry_x - 3, entry_y - 3, entry_x + 3, entry_y + 3, fill=trade_color, outline="")
                    canvas.create_oval(exit_x - 4, exit_y - 4, exit_x + 4, exit_y + 4, fill=exit_color, outline="")
                continue
            canvas.create_line(entry_x, stop_y, exit_x, stop_y, fill="#d1242f", dash=(4, 2))
            canvas.create_line(entry_x, take_y, exit_x, take_y, fill="#1a7f37", dash=(4, 2))
            canvas.create_oval(entry_x - 4, entry_y - 4, entry_x + 4, entry_y + 4, fill=trade_color, outline="")
            canvas.create_oval(exit_x - 5, exit_y - 5, exit_x + 5, exit_y + 5, fill=exit_color, outline="")
            exit_label = _format_backtest_trade_exit_label(trade)
            label_anchor = "e" if exit_x > width - right - 52 else "w"
            label_x = exit_x - 8 if label_anchor == "e" else exit_x + 8
            label_id = canvas.create_text(
                label_x,
                exit_y,
                text=exit_label,
                anchor=label_anchor,
                fill="#ffffff",
                font=("Microsoft YaHei UI", 10, "bold"),
            )
            label_bbox = canvas.bbox(label_id)
            if label_bbox is not None:
                pad_x = 4
                pad_y = 2
                badge_id = canvas.create_rectangle(
                    label_bbox[0] - pad_x,
                    label_bbox[1] - pad_y,
                    label_bbox[2] + pad_x,
                    label_bbox[3] + pad_y,
                    fill=exit_color,
                    outline="#ffffff",
                    width=1,
                )
                canvas.tag_lower(badge_id, label_id)

        if result.open_position is not None and result.candles:
            open_position = result.open_position
            current_index = len(result.candles) - 1
            entry_visible = start_index <= open_position.entry_index < end_index
            current_visible = start_index <= current_index < end_index
            if entry_visible or current_visible:
                entry_x = x_for(open_position.entry_index) if entry_visible else left
                current_x = x_for(current_index) if current_visible else (width - right)
                entry_y = y_for(open_position.entry_price)
                current_y = y_for(open_position.current_price)
                open_color = "#bc4c00" if open_position.signal == "short" else "#0b7285"
                pnl_color = "#1a7f37" if open_position.pnl >= 0 else "#d1242f"
                if entry_visible:
                    canvas.create_oval(
                        entry_x - 5,
                        entry_y - 5,
                        entry_x + 5,
                        entry_y + 5,
                        fill=open_color,
                        outline="",
                    )
                canvas.create_line(entry_x, entry_y, current_x, current_y, fill=open_color, width=3, dash=(6, 3))
                canvas.create_polygon(
                    current_x,
                    current_y - 8,
                    current_x - 7,
                    current_y,
                    current_x,
                    current_y + 8,
                    current_x + 7,
                    current_y,
                    fill=open_color,
                    outline="",
                )
                label_anchor = "w" if current_x < (width - right - 42) else "e"
                label_x = current_x + 10 if label_anchor == "w" else current_x - 10
                canvas.create_text(
                    label_x,
                    current_y - 12,
                    text=f"OPEN {format_decimal_fixed(open_position.pnl, 2)}",
                    anchor=label_anchor,
                    fill=pnl_color,
                    font=("Microsoft YaHei UI", 10, "bold"),
                )

        selected_manual_position = None if fast_mode else self._selected_manual_position()
        for manual_position in visible_manual_positions:
            entry_visible = start_index <= manual_position.entry_index < end_index
            handoff_visible = start_index <= manual_position.handoff_index < end_index
            if not entry_visible and not handoff_visible:
                continue
            manual_color = "#9a6700" if manual_position.signal == "long" else "#bc4c00"
            highlight_width = 3 if manual_position == selected_manual_position else 2
            if fast_mode:
                if entry_visible and handoff_visible:
                    canvas.create_line(
                        x_for(manual_position.entry_index),
                        y_for(manual_position.entry_price),
                        x_for(manual_position.handoff_index),
                        y_for(manual_position.handoff_price),
                        fill=manual_color,
                        width=highlight_width,
                        dash=(4, 3),
                    )
                continue
            if entry_visible:
                entry_x = x_for(manual_position.entry_index)
                entry_y = y_for(manual_position.entry_price)
                canvas.create_polygon(
                    entry_x,
                    entry_y - 7,
                    entry_x - 6,
                    entry_y + 5,
                    entry_x + 6,
                    entry_y + 5,
                    fill=manual_color,
                    outline="",
                )
            else:
                entry_x = None
                entry_y = None
            if handoff_visible:
                handoff_x = x_for(manual_position.handoff_index)
                handoff_y = y_for(manual_position.handoff_price)
                canvas.create_polygon(
                    handoff_x,
                    handoff_y - 6,
                    handoff_x - 6,
                    handoff_y,
                    handoff_x,
                    handoff_y + 6,
                    handoff_x + 6,
                    handoff_y,
                    fill="#f59e0b",
                    outline="",
                )
                break_even_y = y_for(manual_position.break_even_price)
                canvas.create_line(
                    max(left, handoff_x - 28),
                    break_even_y,
                    min(width - right, handoff_x + 28),
                    break_even_y,
                    fill="#f59e0b",
                    dash=(3, 2),
                    width=1,
                )
                if not fast_mode:
                    canvas.create_text(
                        handoff_x + 8,
                        handoff_y - 8,
                        text="转人工",
                        anchor="sw",
                        fill="#9a6700",
                        font=("Microsoft YaHei UI", 9, "bold"),
                    )
            else:
                handoff_x = None
                handoff_y = None
            if entry_visible and handoff_visible and entry_x is not None and entry_y is not None and handoff_x is not None and handoff_y is not None:
                canvas.create_line(
                    entry_x,
                    entry_y,
                    handoff_x,
                    handoff_y,
                    fill=manual_color,
                    width=highlight_width,
                    dash=(4, 3),
                )

        time_label_target = 3 if dense_fast_mode else (4 if fast_mode else 6)
        for time_index in _chart_time_label_indices(start_index, end_index, target_labels=time_label_target):
            x = x_for(time_index)
            canvas.create_line(x, top, x, drawdown_bottom, fill="#f3f4f6", dash=(2, 4))
            canvas.create_line(x, height - bottom, x, height - bottom + 5, fill="#8c959f")
            canvas.create_text(
                x,
                height - bottom + 8,
                text=_format_chart_timestamp(candles[time_index].ts),
                anchor="n",
                fill="#57606a",
                font=("Microsoft YaHei UI", 9),
            )

        if len(ema_points) >= 4:
            canvas.create_line(*ema_points, fill="#ff8c00", width=2, smooth=not fast_mode)
            canvas.create_text(
                width - right,
                top + 12,
                text=f"{result.ema_type.upper()}({result.ema_period})",
                anchor="ne",
                fill="#ff8c00",
                font=("Microsoft YaHei UI", 10, "bold"),
            )
        if len(trend_ema_points) >= 4:
            canvas.create_line(*trend_ema_points, fill="#0a7f5a", width=2, smooth=not fast_mode)
            canvas.create_text(
                width - right,
                top + 30,
                text=f"{result.trend_ema_type.upper()}({result.trend_ema_period})",
                anchor="ne",
                fill="#0a7f5a",
                font=("Microsoft YaHei UI", 10, "bold"),
            )
        if (
            len(reference_ema_points) >= 4
            and not (
                result.entry_reference_ema_period == result.ema_period
                and str(result.entry_reference_ema_type).lower() == str(result.ema_type).lower()
            )
        ):
            canvas.create_line(*reference_ema_points, fill="#7c3aed", width=2, dash=(6, 3), smooth=not fast_mode)
            canvas.create_text(
                width - right,
                top + 48,
                text=f"{result.entry_reference_ema_type.upper()}({result.entry_reference_ema_period})",
                anchor="ne",
                fill="#7c3aed",
                font=("Microsoft YaHei UI", 10, "bold"),
            )
        if len(big_ema_points) >= 4:
            canvas.create_line(*big_ema_points, fill="#8b5cf6", width=2, smooth=not fast_mode)
            canvas.create_text(
                width - right,
                top + (
                    66
                    if not (
                        result.entry_reference_ema_period == result.ema_period
                        and str(result.entry_reference_ema_type).lower() == str(result.ema_type).lower()
                    )
                    else 48
                ),
                text=f"EMA({result.big_ema_period})",
                anchor="ne",
                fill="#8b5cf6",
                font=("Microsoft YaHei UI", 10, "bold"),
            )
        if len(net_value_points) >= 4:
            canvas.create_line(*net_value_points, fill="#0969da", width=2, smooth=not fast_mode)
        if len(drawdown_points) >= 4:
            canvas.create_line(*drawdown_points, fill="#d1242f", width=2, smooth=not fast_mode)
        canvas.create_text(
            width - right,
            net_top + 12,
            text="净值曲线",
            anchor="ne",
            fill="#0969da",
            font=("Microsoft YaHei UI", 10, "bold"),
        )
        canvas.create_text(
            width - right,
            drawdown_top + 12,
            text="回撤曲线(%)",
            anchor="ne",
            fill="#d1242f",
            font=("Microsoft YaHei UI", 10, "bold"),
        )
        if not fast_mode:
            self._render_chart_hover(canvas)

    def _render_chart_hover(self, canvas: Canvas) -> None:
        canvas.delete("chart-hover")
        if self._latest_result is None:
            return
        state = self._chart_render_states.get(id(canvas))
        hover_index = self._chart_hover_indices.get(id(canvas))
        if state is None or hover_index is None:
            return
        if not (state.start_index <= hover_index < state.end_index):
            return

        candle = self._latest_result.candles[hover_index]
        ema_value = (
            self._latest_result.ema_values[hover_index]
            if hover_index < len(self._latest_result.ema_values)
            else None
        )
        trend_ema_value = (
            self._latest_result.trend_ema_values[hover_index]
            if hover_index < len(self._latest_result.trend_ema_values)
            else None
        )
        reference_ema_value = (
            self._latest_result.entry_reference_ema_values[hover_index]
            if hover_index < len(self._latest_result.entry_reference_ema_values)
            else None
        )
        atr_value = (
            self._latest_result.atr_values[hover_index]
            if hover_index < len(self._latest_result.atr_values)
            else None
        )
        if self._strategy_uses_big_ema(self._latest_result.strategy_id) and hover_index < len(self._latest_result.big_ema_values):
            big_ema_value: Decimal | None = self._latest_result.big_ema_values[hover_index]
            big_ema_period: str | None = str(self._latest_result.big_ema_period)
        else:
            big_ema_value = None
            big_ema_period = None
        equity_value = self._latest_result.net_value_curve[hover_index] if self._latest_result.net_value_curve else Decimal("0")
        drawdown_pct_value = (
            Decimal("0") - self._latest_result.drawdown_pct_curve[hover_index]
            if self._latest_result.drawdown_pct_curve
            else Decimal("0")
        )
        direction_bias = (
            self._latest_result.direction_filter_bias[hover_index]
            if hover_index < len(self._latest_result.direction_filter_bias)
            else None
        )
        x = state.left + ((hover_index - state.start_index) * state.candle_step) + (state.candle_step / 2)
        canvas.create_line(
            x,
            state.top,
            x,
            state.drawdown_bottom,
            fill="#8b949e",
            dash=(4, 4),
            tags=("chart-hover",),
        )
        lines = _format_chart_hover_lines(
            candle=candle,
            ema_value=ema_value,
            trend_ema_value=trend_ema_value,
            reference_ema_value=reference_ema_value,
            big_ema_value=big_ema_value,
            atr_value=atr_value,
            equity_value=equity_value,
            drawdown_pct_value=drawdown_pct_value,
            ema_type=str(self._latest_result.ema_type),
            ema_period=str(self._latest_result.ema_period),
            trend_ema_type=str(self._latest_result.trend_ema_type),
            trend_ema_period=str(self._latest_result.trend_ema_period),
            reference_ema_type=str(self._latest_result.entry_reference_ema_type),
            reference_ema_period=str(self._latest_result.entry_reference_ema_period),
            big_ema_period=big_ema_period,
            atr_period=str(self._latest_result.atr_period),
            tick_size=self._latest_result.instrument.tick_size,
            direction_filter_bias=direction_bias,
        )
        text_item = canvas.create_text(
            state.left + 10,
            state.top + 10,
            text="\n".join(lines),
            anchor="nw",
            fill="#24292f",
            font=("Microsoft YaHei UI", 9),
            tags=("chart-hover",),
        )
        x1, y1, x2, y2 = canvas.bbox(text_item)
        background = canvas.create_rectangle(
            x1 - 8,
            y1 - 6,
            x2 + 8,
            y2 + 6,
            fill="#ffffff",
            outline="#d0d7de",
            tags=("chart-hover",),
        )
        canvas.tag_lower(background, text_item)

    def _clear_chart_canvas(self, canvas: Canvas | None) -> None:
        if canvas is None or not self._widget_exists(canvas):
            return
        canvas.delete("all")
        width = max(canvas.winfo_width(), 960)
        height = max(canvas.winfo_height(), 420)
        canvas.create_rectangle(0, 0, width, height, outline="", fill="#ffffff")
        canvas.create_text(
            width / 2,
            height / 2,
            text="\u8fd0\u884c\u56de\u6d4b\u540e\uff0c\u8fd9\u91cc\u4f1a\u663e\u793a K \u7ebf\u3001EMA\u3001\u51c0\u503c\u66f2\u7ebf\u3001\u56de\u64a4\u66f2\u7ebf\u548c\u4ea4\u6613\u8def\u5f84\u3002",
            fill="#6e7781",
            font=("Microsoft YaHei UI", 11),
        )

    def _populate_period_stats(self, tree: ttk.Treeview, stats: list) -> None:
        tree.delete(*tree.get_children())
        for index, stat in enumerate(stats, start=1):
            tree.insert(
                "",
                END,
                iid=f"S{index:03d}",
                values=(
                    stat.period_label,
                    stat.trades,
                    f"{format_decimal_fixed(stat.win_rate, 2)}%",
                    format_decimal_fixed(stat.total_pnl, 4),
                    f"{format_decimal_fixed(stat.return_pct, 2)}%",
                    format_decimal_fixed(stat.max_drawdown, 4),
                    f"{format_decimal_fixed(stat.max_drawdown_pct, 2)}%",
                    format_decimal_fixed(stat.end_equity, 2),
                ),
            )

    def _schedule_chart_redraw(self, *_: object, delay_ms: int = 16) -> None:
        if self._latest_result is None:
            return
        if self._chart_redraw_job is not None:
            self.window.after_cancel(self._chart_redraw_job)
        self._chart_redraw_job = self.window.after(delay_ms, self._redraw_all_charts)

    def _schedule_canvas_redraw(self, canvas: Canvas, *, delay_ms: int = 16, fast_mode: bool = False) -> None:
        if self._latest_result is None:
            return
        canvas_id = id(canvas)
        existing_job = self._chart_canvas_redraw_jobs.get(canvas_id)
        if existing_job is not None:
            self.window.after_cancel(existing_job)
        self._chart_canvas_redraw_jobs[canvas_id] = self.window.after(
            delay_ms,
            lambda target=canvas, target_id=canvas_id, fast=fast_mode: self._run_canvas_redraw(target, target_id, fast),
        )

    def _schedule_canvas_finalize_redraw(self, canvas: Canvas, *, delay_ms: int = 160) -> None:
        if self._latest_result is None:
            return
        canvas_id = id(canvas)
        existing_job = self._chart_canvas_finalize_jobs.get(canvas_id)
        if existing_job is not None:
            self.window.after_cancel(existing_job)
        self._chart_canvas_finalize_jobs[canvas_id] = self.window.after(
            delay_ms,
            lambda target=canvas, target_id=canvas_id: self._run_canvas_finalize_redraw(target, target_id),
        )

    def _run_canvas_redraw(self, canvas: Canvas, canvas_id: int, fast_mode: bool) -> None:
        self._chart_canvas_redraw_jobs.pop(canvas_id, None)
        if self._latest_result is None or not canvas.winfo_exists():
            return
        self._draw_chart(self._latest_result, canvas, fast_mode=fast_mode)

    def _run_canvas_finalize_redraw(self, canvas: Canvas, canvas_id: int) -> None:
        self._chart_canvas_finalize_jobs.pop(canvas_id, None)
        if self._latest_result is None or not canvas.winfo_exists():
            return
        self._draw_chart(self._latest_result, canvas, fast_mode=False)

    def _redraw_all_charts(self) -> None:
        self._chart_redraw_job = None
        if self._latest_result is None:
            return
        if self._widget_exists(getattr(self, "chart_canvas", None)):
            self._draw_chart(self._latest_result, self.chart_canvas)
        if self._chart_zoom_canvas is not None and self._chart_zoom_canvas.winfo_exists():
            self._draw_chart(self._latest_result, self._chart_zoom_canvas)

    def _build_zoom_chart_header_lines(self, snapshot: _BacktestSnapshot | None) -> tuple[str, str]:
        if snapshot is None or snapshot.result is None:
            return (
                "暂无回测",
                "执行回测后，这里会显示策略、参数与结果摘要。",
            )

        config = snapshot.config
        result = snapshot.result
        report = result.report
        strategy_name = _strategy_display_name(config)
        signal_label = SIGNAL_VALUE_TO_LABEL.get(config.signal_mode, config.signal_mode)
        exit_label = _backtest_exit_mode_label(config)
        exit_metric_label = "平仓" if config.strategy_id == STRATEGY_BTC_EMA55_SLOPE_SHORT_ID else "止盈"
        max_entries_label = "不限(0)" if config.max_entries_per_trend <= 0 else str(config.max_entries_per_trend)
        identity_text = " | ".join(_build_backtest_identity_parts(snapshot, prefer_archive=False))
        context_line = (
            f"{identity_text} | 策略：{strategy_name} | 交易对：{config.inst_id} | "
            f"K线：{_normalize_backtest_bar_label(config.bar)} | 方向：{signal_label}"
        )
        metrics_parts = [
            f"挂单参考线：{config.entry_reference_line_label()}",
            f"指标：{moving_average_display_label(result.ema_type, result.ema_period)} / {moving_average_display_label(result.trend_ema_type, result.trend_ema_period)} / ATR{result.atr_period}",
            f"止损：{format_decimal(config.atr_stop_multiplier)} ATR",
            f"{exit_metric_label}：{exit_label}",
        ]
        if config.uses_daily_filter():
            metrics_parts.append(config.daily_filter_summary())
        if config.strategy_id != STRATEGY_BTC_EMA55_SLOPE_SHORT_ID and config.take_profit_mode == "dynamic":
            metrics_parts.extend(_dynamic_protection_metric_parts(config))
            metrics_parts.append(
                f"时间保本：{config.time_stop_break_even_enabled_label()}/{config.resolved_time_stop_break_even_bars()}根"
            )
        metrics_parts.extend(
            [
                f"每波开仓：{max_entries_label}",
                f"交易数：{report.total_trades}",
                f"胜率：{format_decimal_fixed(report.win_rate, 2)}%",
                f"总盈亏：{format_decimal_fixed(report.total_pnl, 4)}",
                f"最大回撤：{format_decimal_fixed(report.max_drawdown, 4)}",
            ]
        )
        metrics_line = " | ".join(metrics_parts)
        return context_line, metrics_line
    def _refresh_zoom_chart_header(self) -> None:
        snapshot = self._backtest_snapshots.get(self._current_snapshot_id) if self._current_snapshot_id else None
        context_line, metrics_line = self._build_zoom_chart_header_lines(snapshot)
        if self._chart_zoom_context_label is not None and self._widget_exists(self._chart_zoom_context_label):
            self._chart_zoom_context_label.configure(text=context_line)
        if self._chart_zoom_metrics_label is not None and self._widget_exists(self._chart_zoom_metrics_label):
            self._chart_zoom_metrics_label.configure(text=metrics_line)
        if self._chart_zoom_window is not None and self._chart_zoom_window.winfo_exists():
            desired_title = (
                "回测K线图"
                if snapshot is None
                else f"回测K线图 | {snapshot.config.inst_id} | {_normalize_backtest_bar_label(snapshot.config.bar)}"
            )
            self._chart_zoom_window.title(desired_title)
            return

    def open_chart_zoom_window(self) -> None:
        if self._chart_zoom_window is not None and self._chart_zoom_window.winfo_exists():
            self._chart_zoom_window.deiconify()
            self._chart_zoom_window.lift()
            self._chart_zoom_window.focus_force()
            self._refresh_zoom_chart_header()
            self._redraw_all_charts()
            return

        zoom_window = Toplevel(self.window)
        zoom_window.title("\u56de\u6d4b\u56fe\u8868\u5927\u7a97")
        zoom_window.title("回测K线图")
        apply_adaptive_window_geometry(
            zoom_window,
            width_ratio=0.9,
            height_ratio=0.88,
            min_width=1200,
            min_height=720,
            max_width=1880,
            max_height=1160,
        )
        zoom_window.columnconfigure(0, weight=1)
        zoom_window.rowconfigure(1, weight=1)
        zoom_window.protocol("WM_DELETE_WINDOW", self._close_chart_zoom_window)
        try:
            zoom_window.state("zoomed")
        except Exception:
            pass

        toolbar = ttk.Frame(zoom_window, padding=(12, 12, 12, 0))
        toolbar.grid(row=0, column=0, sticky="ew")
        toolbar.columnconfigure(0, weight=1)
        self._chart_zoom_intro_label = ttk.Label(
            toolbar,
            text="回测K线图：用于查看 K 线结构、EMA 轨迹、资金曲线、回撤曲线和 TP/SL 触发位置，支持滚轮缩放和拖动平移。",
            justify="left",
        )
        self._chart_zoom_intro_label.grid(row=0, column=0, sticky="w")
        ttk.Button(toolbar, text="重置视图", command=self.reset_zoom_chart_view).grid(row=0, column=1, sticky="e", padx=(0, 8))
        ttk.Button(toolbar, text="关闭", command=self._close_chart_zoom_window).grid(row=0, column=2, sticky="e")
        self._chart_zoom_context_label = ttk.Label(toolbar, justify="left")
        self._chart_zoom_context_label.grid(row=1, column=0, columnspan=3, sticky="w", pady=(8, 0))
        self._chart_zoom_metrics_label = ttk.Label(toolbar, justify="left")
        self._chart_zoom_metrics_label.grid(row=2, column=0, columnspan=3, sticky="w", pady=(2, 0))

        zoom_canvas = Canvas(zoom_window, background="#ffffff", highlightthickness=0)
        zoom_canvas.grid(row=1, column=0, sticky="nsew", padx=12, pady=12)
        zoom_canvas.bind("<Configure>", lambda _event, target=zoom_canvas: self._on_chart_canvas_configure(target))
        self._bind_chart_interactions(zoom_canvas)

        self._chart_zoom_window = zoom_window
        self._chart_zoom_canvas = zoom_canvas
        self._refresh_zoom_chart_header()
        if self._latest_result is not None:
            self._redraw_all_charts()
        else:
            self._clear_chart_canvas(zoom_canvas)

    def _close_chart_zoom_window(self) -> None:
        zoom_canvas = self._chart_zoom_canvas
        if self._chart_zoom_window is not None and self._chart_zoom_window.winfo_exists():
            self._chart_zoom_window.destroy()
        self._chart_zoom_window = None
        self._chart_zoom_canvas = None
        self._chart_zoom_intro_label = None
        self._chart_zoom_context_label = None
        self._chart_zoom_metrics_label = None
        self._zoom_chart_view = _ChartViewport()
        if zoom_canvas is not None:
            self._chart_render_states.pop(id(zoom_canvas), None)
            self._chart_hover_indices.pop(id(zoom_canvas), None)
            scheduled_job = self._chart_canvas_redraw_jobs.pop(id(zoom_canvas), None)
            if scheduled_job is not None:
                self.window.after_cancel(scheduled_job)
            finalize_job = self._chart_canvas_finalize_jobs.pop(id(zoom_canvas), None)
            if finalize_job is not None:
                self.window.after_cancel(finalize_job)

    def _parse_positive_int(self, raw: str, field_name: str) -> int:
        value = int(raw)
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

    def _parse_backtest_candle_limit(self, raw: str) -> int:
        value = self._parse_nonnegative_int(raw, "回测K线数")
        if value > MAX_BACKTEST_CANDLES:
            raise ValueError(f"回测K线数最多支持 {MAX_BACKTEST_CANDLES}")
        return value

    def _parse_positive_decimal(self, raw: str, field_name: str) -> Decimal:
        try:
            value = Decimal(raw)
        except InvalidOperation as exc:
            raise ValueError(f"{field_name} 不是有效数字") from exc
        if value <= 0:
            raise ValueError(f"{field_name} 必须大于 0")
        return value

    def _parse_decimal(self, raw: str, field_name: str) -> Decimal:
        try:
            return Decimal(raw)
        except InvalidOperation as exc:
            raise ValueError(f"{field_name} 不是有效数字") from exc

    def _parse_nonnegative_decimal(self, raw: str, field_name: str) -> Decimal:
        try:
            value = Decimal(raw)
        except InvalidOperation as exc:
            raise ValueError(f"{field_name} 不是有效数字") from exc
        if value < 0:
            raise ValueError(f"{field_name} 不能小于 0")
        return value

    def _parse_fee_percent(self, raw: str, field_name: str) -> Decimal:
        try:
            value = Decimal(raw)
        except InvalidOperation as exc:
            raise ValueError(f"{field_name} 不是有效数字") from exc
        if value < 0:
            raise ValueError(f"{field_name} 不能小于 0")
        return value / Decimal("100")

    def _parse_optional_datetime(self, raw: str, field_name: str, *, end_of_day: bool) -> int | None:
        text = raw.strip()
        if not text:
            return None
        formats = (
            ("%Y%m%d %H:%M:%S", False),
            ("%Y%m%d %H:%M", False),
            ("%Y%m%d", True),
            ("%Y-%m-%d %H:%M:%S", False),
            ("%Y-%m-%d %H:%M", False),
            ("%Y-%m-%d", True),
        )
        for fmt, date_only in formats:
            try:
                parsed = datetime.strptime(text, fmt)
            except ValueError:
                continue
            if date_only:
                parsed = parsed.replace(hour=23 if end_of_day else 0, minute=59 if end_of_day else 0, second=59 if end_of_day else 0)
            return int(parsed.timestamp() * 1000)
        raise ValueError(f"{field_name} 格式不正确，支持 YYYYMMDD 或 YYYYMMDD HH:MM")


def _normalize_chart_viewport(
    start_index: int,
    visible_count: int | None,
    total_count: int,
    *,
    min_visible: int = 20,
) -> tuple[int, int]:
    if total_count <= 0:
        return 0, 0
    normalized_min_visible = max(1, min(min_visible, total_count))
    normalized_visible = total_count if visible_count is None else max(normalized_min_visible, min(visible_count, total_count))
    max_start = max(total_count - normalized_visible, 0)
    normalized_start = max(0, min(start_index, max_start))
    return normalized_start, normalized_visible


def _default_chart_viewport(
    total_count: int,
    requested_visible: int = DEFAULT_BACKTEST_CHART_VISIBLE_CANDLES,
    *,
    min_visible: int = 20,
) -> tuple[int, int]:
    if total_count <= 0:
        return 0, 0
    _, visible_count = _normalize_chart_viewport(
        0,
        requested_visible,
        total_count,
        min_visible=min_visible,
    )
    return max(total_count - visible_count, 0), visible_count


def _zoom_chart_viewport(
    *,
    start_index: int,
    visible_count: int | None,
    total_count: int,
    anchor_ratio: float,
    zoom_in: bool,
    min_visible: int = 20,
) -> tuple[int, int]:
    normalized_start, normalized_visible = _normalize_chart_viewport(
        start_index,
        visible_count,
        total_count,
        min_visible=min_visible,
    )
    if total_count <= 0:
        return 0, 0

    factor = 0.8 if zoom_in else 1.25
    target_visible = int(round(normalized_visible * factor))
    min_count = max(1, min(min_visible, total_count))
    target_visible = max(min_count, min(target_visible, total_count))
    if target_visible == normalized_visible:
        return normalized_start, normalized_visible

    clamped_ratio = min(max(anchor_ratio, 0.0), 1.0)
    anchor_index = normalized_start + (normalized_visible * clamped_ratio)
    target_start = int(round(anchor_index - (target_visible * clamped_ratio)))
    return _normalize_chart_viewport(target_start, target_visible, total_count, min_visible=min_visible)


def _pan_chart_viewport(
    start_index: int,
    visible_count: int | None,
    total_count: int,
    shift: int,
    *,
    min_visible: int = 20,
) -> int:
    normalized_start, normalized_visible = _normalize_chart_viewport(
        start_index,
        visible_count,
        total_count,
        min_visible=min_visible,
    )
    target_start, _ = _normalize_chart_viewport(
        normalized_start + shift,
        normalized_visible,
        total_count,
        min_visible=min_visible,
    )
    return target_start


def _chart_price_axis_values(price_min: Decimal, price_max: Decimal, *, steps: int = 4) -> list[Decimal]:
    if steps <= 0:
        return [price_min, price_max]
    if price_max <= price_min:
        return [price_min]
    step = (price_max - price_min) / Decimal(steps)
    return [price_min + (step * Decimal(index)) for index in range(steps + 1)]


def _format_chart_axis_price(value: Decimal) -> str:
    absolute = abs(value)
    if absolute >= Decimal("1000"):
        places = 1
    elif absolute >= Decimal("1"):
        places = 2
    elif absolute >= Decimal("0.1"):
        places = 4
    else:
        places = 5
    return format_decimal_fixed(value, places)


def _format_backtest_stop_r_label(r_multiple: Decimal) -> str:
    if r_multiple <= 0:
        return "SL"
    places = 1 if abs(r_multiple) >= Decimal("1") else 2
    text = format_decimal_fixed(r_multiple, places).rstrip("0").rstrip(".")
    return f"{text}R"


def _format_backtest_trade_exit_label(trade: BacktestTrade) -> str:
    if trade.exit_reason == "take_profit":
        return "TP"
    if trade.exit_reason == "break_even_stop":
        return "BE"
    if is_stop_exit_reason(trade.exit_reason):
        return _format_backtest_stop_r_label(trade.r_multiple)
    return "SL"


def _backtest_candle_color(open_price: Decimal, close_price: Decimal) -> str:
    return "#1a7f37" if close_price >= open_price else "#d1242f"


def _format_chart_timestamp(ts: int) -> str:
    if ts >= 10**12:
        dt = datetime.fromtimestamp(ts / 1000)
        return dt.strftime("%Y-%m-%d %H:%M")
    if ts >= 10**9:
        dt = datetime.fromtimestamp(ts)
        return dt.strftime("%Y-%m-%d %H:%M")
    return str(ts)


def _chart_time_label_indices(start_index: int, end_index: int, *, target_labels: int = 6) -> list[int]:
    visible_count = max(end_index - start_index, 0)
    if visible_count <= 0:
        return []
    if visible_count <= target_labels:
        return list(range(start_index, end_index))
    span = visible_count - 1
    indices = {
        start_index + int(round(span * label_index / max(target_labels - 1, 1)))
        for label_index in range(target_labels)
    }
    return sorted(index for index in indices if start_index <= index < end_index)


def _chart_hover_index_for_x(
    *,
    x: float,
    left: int,
    width: int,
    start_index: int,
    end_index: int,
    candle_step: float,
) -> int | None:
    if width <= 0 or candle_step <= 0:
        return None
    if x < left or x > left + width:
        return None
    relative = x - left - (candle_step / 2)
    offset = int(round(relative / candle_step))
    index = start_index + offset
    if index < start_index or index >= end_index:
        return None
    return index


def _format_chart_hover_lines(
    *,
    candle,
    ema_value: Decimal | None,
    trend_ema_value: Decimal | None,
    reference_ema_value: Decimal | None = None,
    big_ema_value: Decimal | None,
    atr_value: Decimal | None,
    equity_value: Decimal,
    drawdown_pct_value: Decimal,
    ema_type: str = "ema",
    ema_period: str,
    trend_ema_type: str = "ema",
    trend_ema_period: str,
    reference_ema_type: str = "ema",
    reference_ema_period: str | None = None,
    big_ema_period: str | None,
    atr_period: str,
    tick_size: Decimal,
    direction_filter_bias: str | None = None,
) -> list[str]:
    reference_period = ema_period if reference_ema_period is None else str(reference_ema_period)
    lines = [
        f"时间: {_format_chart_timestamp(candle.ts)}",
        (
            "开/高/低/收: "
            f"{format_decimal(candle.open)} / {format_decimal(candle.high)} / "
            f"{format_decimal(candle.low)} / {format_decimal(candle.close)}"
        ),
    ]
    if ema_value is not None:
        lines.append(f"{str(ema_type).upper()}({ema_period}): {_format_price_by_tick_size(ema_value, tick_size)}")
    if trend_ema_value is not None:
        lines.append(
            f"{str(trend_ema_type).upper()}({trend_ema_period}): {_format_price_by_tick_size(trend_ema_value, tick_size)}"
        )
    if (
        reference_ema_value is not None
        and not (
            str(reference_ema_type).lower() == str(ema_type).lower()
            and reference_period == str(ema_period)
        )
    ):
        lines.append(
            f"{str(reference_ema_type).upper()}({reference_period}): "
            f"{_format_price_by_tick_size(reference_ema_value, tick_size)}"
        )
    if atr_value is not None:
        lines.append(f"ATR({atr_period}): {_format_price_by_tick_size(atr_value, tick_size)}")
    if big_ema_value is not None and big_ema_period:
        lines.append(f"EMA({big_ema_period}): {_format_price_by_tick_size(big_ema_value, tick_size)}")
    if direction_filter_bias:
        bias_labels = {
            "long": "只允许做多",
            "short": "只允许做空",
            "both": "多空都允许",
            "neutral": "当前都不允许",
        }
        lines.append(f"日线过滤: {bias_labels.get(direction_filter_bias, direction_filter_bias)}")
    lines.extend(
        [
            f"净值曲线: {format_decimal_fixed(equity_value, 2)}",
            f"当前回撤: {format_decimal_fixed(drawdown_pct_value, 2)}%",
        ]
    )
    return lines


def _format_price_by_tick_size(value: Decimal, tick_size: Decimal) -> str:
    places = _decimal_places_for_tick_size(tick_size)
    return format_decimal_fixed(value, places)


def _decimal_places_for_tick_size(tick_size: Decimal) -> int:
    normalized = tick_size.normalize()
    exponent = normalized.as_tuple().exponent
    return max(-exponent, 0)


def _heatmap_metric_value(snapshot: _BacktestSnapshot, metric_label: str) -> Decimal:
    report = snapshot.report
    if metric_label == "胜率":
        return report.win_rate
    if metric_label == "交易数":
        return Decimal(report.total_trades)
    if metric_label == "盈亏回撤比":
        if report.max_drawdown <= 0:
            return Decimal("0")
        return report.total_pnl / report.max_drawdown
    return report.total_pnl


def _heatmap_metric_text(snapshot: _BacktestSnapshot, metric_label: str) -> str:
    value = _heatmap_metric_value(snapshot, metric_label)
    if metric_label == "胜率":
        return f"{format_decimal_fixed(value, 2)}%"
    if metric_label == "交易数":
        return f"{snapshot.report.total_trades}笔"
    if metric_label == "盈亏回撤比":
        return format_decimal_fixed(value, 2)
    return format_decimal_fixed(value, 4)


def _heatmap_fill_color(value: Decimal, min_value: Decimal, max_value: Decimal) -> str:
    if max_value == min_value:
        return "#eef2f7"
    if min_value < 0 < max_value:
        span = max(abs(min_value), abs(max_value))
        if span <= 0:
            return "#eef2f7"
        intensity = float(min(abs(value) / span, Decimal("1")))
        if value > 0:
            red = int(235 - (55 * intensity))
            green = int(248 - (40 * intensity))
            blue = int(235 - (145 * intensity))
            return f"#{red:02x}{green:02x}{blue:02x}"
        if value < 0:
            red = int(248 - (12 * intensity))
            green = int(236 - (120 * intensity))
            blue = int(236 - (120 * intensity))
            return f"#{red:02x}{green:02x}{blue:02x}"
        return "#eef2f7"
    ratio = float((value - min_value) / (max_value - min_value))
    start = (238, 242, 247)
    end = (18, 133, 63)
    red = int(start[0] + ((end[0] - start[0]) * ratio))
    green = int(start[1] + ((end[1] - start[1]) * ratio))
    blue = int(start[2] + ((end[2] - start[2]) * ratio))
    return f"#{red:02x}{green:02x}{blue:02x}"
