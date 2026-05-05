from __future__ import annotations

import json
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from tkinter import BOTH, END, LEFT, RIGHT, BooleanVar, Canvas, StringVar, Text, Toplevel, X, Y
from tkinter import messagebox, ttk

from okx_quant.backtest import (
    ATR_BATCH_MULTIPLIERS,
    ATR_BATCH_TAKE_RATIOS,
    BATCH_MAX_ENTRIES_OPTIONS,
    BacktestManualPosition,
    BacktestReport,
    BacktestResult,
    BacktestTrade,
    MAX_BACKTEST_CANDLES,
    build_parameter_batch_configs,
    format_backtest_report,
    run_backtest,
    run_backtest_batch,
)
from okx_quant.backtest_audit import describe_backtest_export_artifacts
from okx_quant.backtest_export import export_batch_backtest_report, export_single_backtest_report
from okx_quant.backtest_strategy_pool import is_strategy_pool_config, strategy_pool_profile_name
from okx_quant.models import StrategyConfig
from okx_quant.okx_client import OkxRestClient
from okx_quant.persistence import backtest_history_file_path, load_strategy_parameter_drafts, save_strategy_parameter_drafts
from okx_quant.pricing import format_decimal, format_decimal_fixed
from okx_quant.strategy_catalog import (
    ALL_STRATEGY_DEFINITIONS,
    BACKTEST_STRATEGY_DEFINITIONS,
    STRATEGY_DYNAMIC_ID,
    STRATEGY_EMA5_EMA8_ID,
    STRATEGY_EMA_BREAKDOWN_SHORT_ID,
    StrategyDefinition,
    get_strategy_definition,
    is_dynamic_strategy_id,
    is_ema_atr_breakout_strategy,
    resolve_dynamic_signal_mode,
)
from okx_quant.strategy_parameters import (
    iter_strategy_parameter_keys,
    strategy_fixed_value,
    strategy_is_parameter_editable,
    strategy_parameter_default_value,
    strategy_uses_parameter,
)
from okx_quant.window_layout import apply_adaptive_window_geometry


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
DEFAULT_MAKER_FEE_PERCENT = "0.015"
DEFAULT_TAKER_FEE_PERCENT = "0.036"
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
MANUAL_NEAR_BREAK_EVEN_THRESHOLD_PCT = Decimal("0.50")
TAKE_PROFIT_MODE_VALUE_TO_LABEL = {value: label for label, value in TAKE_PROFIT_MODE_OPTIONS.items()}
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
    ema_period: str
    trend_ema_period: str
    big_ema_period: str
    entry_reference_ema_period: str
    atr_period: str
    stop_atr: str
    take_atr: str
    risk_amount: str
    take_profit_mode_label: str
    max_entries_per_trend: str
    dynamic_two_r_break_even: bool
    dynamic_fee_offset_enabled: bool
    time_stop_break_even_enabled: bool
    time_stop_break_even_bars: str
    signal_mode_label: str
    trade_mode_label: str
    position_mode_label: str
    trigger_type_label: str
    environment_label: str
    hold_close_exit_bars: str = "0"
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
        payload = {
            "records": [_serialize_backtest_snapshot(self._snapshots[snapshot_id]) for snapshot_id in self._order],
            "updated_at": datetime.now().isoformat(timespec="seconds"),
        }
        body = json.dumps(payload, ensure_ascii=False, indent=2)
        # 固定名 .tmp 多实例会互踩；replace 在 Windows 上遇杀软/占用易 PermissionError，故用随机临时名 + 短重试。
        temp_path = path.with_name(f"{path.stem}.{uuid.uuid4().hex}.tmp")
        try:
            temp_path.write_text(body, encoding="utf-8")
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


def _normalize_backtest_bar_label(value: str) -> str:
    normalized = value.strip()
    if normalized in BACKTEST_BAR_LABEL_TO_VALUE:
        return normalized
    if normalized in BACKTEST_BAR_VALUE_TO_LABEL:
        return BACKTEST_BAR_VALUE_TO_LABEL[normalized]
    return DEFAULT_BACKTEST_BAR_LABEL


def _backtest_bar_value_from_label(label: str) -> str:
    return BACKTEST_BAR_LABEL_TO_VALUE[_normalize_backtest_bar_label(label)]


def _format_backtest_candle_limit(candle_limit: int) -> str:
    return "全量" if candle_limit <= 0 else str(candle_limit)


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


def _build_backtest_compare_row(snapshot: _BacktestSnapshot) -> tuple[str, ...]:
    report = snapshot.report
    config = snapshot.config
    start_text, end_text = _backtest_snapshot_range_text(snapshot)
    return (
        snapshot.snapshot_id,
        snapshot.created_at.strftime("%m-%d %H:%M:%S"),
        start_text,
        end_text,
        _strategy_display_name(config),
        config.inst_id,
        _normalize_backtest_bar_label(config.bar),
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


def _build_backtest_param_summary(
    config: StrategyConfig,
    *,
    maker_fee_rate: Decimal = Decimal("0"),
    taker_fee_rate: Decimal = Decimal("0"),
) -> str:
    if is_dynamic_strategy_id(config.strategy_id) or is_ema_atr_breakout_strategy(config.strategy_id):
        risk_text = "-" if config.risk_amount is None else format_decimal(config.risk_amount)
        sizing_label = BACKTEST_SIZING_VALUE_TO_LABEL.get(config.backtest_sizing_mode, config.backtest_sizing_mode)
        if config.backtest_sizing_mode == "risk_percent":
            sizing_text = f"{sizing_label}{format_decimal(config.backtest_risk_percent or Decimal('0'))}%"
        elif config.backtest_sizing_mode == "fixed_size":
            sizing_text = f"{sizing_label}{format_decimal(config.order_size)}"
        else:
            sizing_text = f"{sizing_label}{risk_text}"
        take_profit_label = "动态止盈" if config.take_profit_mode == "dynamic" else "固定止盈"
        extra_parts = [take_profit_label]
        if config.take_profit_mode == "dynamic":
            extra_parts.append(f"2R保本{config.dynamic_two_r_break_even_label()}")
            extra_parts.append(f"手续费偏移{config.dynamic_fee_offset_enabled_label()}")
            extra_parts.append(
                f"时间保本{config.time_stop_break_even_enabled_label()}/{config.resolved_time_stop_break_even_bars()}根"
            )
        if is_dynamic_strategy_id(config.strategy_id) or is_ema_atr_breakout_strategy(config.strategy_id):
            max_entries_text = "不限" if config.max_entries_per_trend <= 0 else f"每波前{config.max_entries_per_trend}次"
            extra_parts.append(max_entries_text)
        if is_ema_atr_breakout_strategy(config.strategy_id) and int(config.hold_close_exit_bars) > 0:
            extra_parts.append(f"满{int(config.hold_close_exit_bars)}根收盘平仓")
        extra_text = " / ".join(extra_parts)
        ref_ema_label = (
            "跌破参考EMA"
            if config.strategy_id == STRATEGY_EMA_BREAKDOWN_SHORT_ID
            else ("突破参考EMA" if is_ema_atr_breakout_strategy(config.strategy_id) else "挂单EMA")
        )
        return (
            f"EMA{config.ema_period}/{config.trend_ema_period} / ATR{config.atr_period} / "
            f"{ref_ema_label}{config.resolved_entry_reference_ema_period()} / "
            f"SLx{format_decimal(config.atr_stop_multiplier)} / TPx{format_decimal(config.atr_take_multiplier)} / "
            f"{extra_text} / "
            f"方向{SIGNAL_VALUE_TO_LABEL.get(config.signal_mode, config.signal_mode)} / 仓位{sizing_text} / "
            f"本金{format_decimal_fixed(config.backtest_initial_capital, 2)} / "
            f"{'复利' if config.backtest_compounding else '不复利'} / "
            f"M费{_format_fee_rate_percent(maker_fee_rate)} / T费{_format_fee_rate_percent(taker_fee_rate)} / "
            f"{_format_backtest_slippage_summary(config)} / "
            f"资金费{_format_fee_rate_percent(config.backtest_funding_rate)}"
        )

    if config.strategy_id == STRATEGY_EMA5_EMA8_ID:
        sizing_label = BACKTEST_SIZING_VALUE_TO_LABEL.get(config.backtest_sizing_mode, config.backtest_sizing_mode)
        if config.backtest_sizing_mode == "risk_percent":
            sizing_text = f"{sizing_label}{format_decimal(config.backtest_risk_percent or Decimal('0'))}%"
        elif config.backtest_sizing_mode == "fixed_size":
            sizing_text = f"{sizing_label}{format_decimal(config.order_size)}"
        else:
            sizing_text = f"{sizing_label}{format_decimal(config.risk_amount or Decimal('0'))}"
        return (
            f"4H固定 / EMA{config.ema_period}/{config.trend_ema_period}/{config.big_ema_period} / "
            f"EMA{config.trend_ema_period}动态止损 / 方向{SIGNAL_VALUE_TO_LABEL.get(config.signal_mode, config.signal_mode)} / "
            f"仓位{sizing_text} / 本金{format_decimal_fixed(config.backtest_initial_capital, 2)} / "
            f"{'复利' if config.backtest_compounding else '不复利'} / "
            f"M费{_format_fee_rate_percent(maker_fee_rate)} / T费{_format_fee_rate_percent(taker_fee_rate)} / "
            f"{_format_backtest_slippage_summary(config)} / "
            f"资金费{_format_fee_rate_percent(config.backtest_funding_rate)}"
        )

    risk_text = "-" if config.risk_amount is None else format_decimal(config.risk_amount)
    sizing_label = BACKTEST_SIZING_VALUE_TO_LABEL.get(config.backtest_sizing_mode, config.backtest_sizing_mode)
    if config.backtest_sizing_mode == "risk_percent":
        sizing_text = f"{sizing_label}{format_decimal(config.backtest_risk_percent or Decimal('0'))}%"
    elif config.backtest_sizing_mode == "fixed_size":
        sizing_text = f"{sizing_label}{format_decimal(config.order_size)}"
    else:
        sizing_text = f"{sizing_label}{risk_text}"
    return (
        f"EMA{config.ema_period}/{config.trend_ema_period}/{config.big_ema_period} / ATR{config.atr_period} / "
        f"SLx{format_decimal(config.atr_stop_multiplier)} / TPx{format_decimal(config.atr_take_multiplier)} / "
        f"方向{SIGNAL_VALUE_TO_LABEL.get(config.signal_mode, config.signal_mode)} / 仓位{sizing_text} / "
        f"本金{format_decimal_fixed(config.backtest_initial_capital, 2)} / "
        f"{'复利' if config.backtest_compounding else '不复利'} / "
        f"M费{_format_fee_rate_percent(maker_fee_rate)} / T费{_format_fee_rate_percent(taker_fee_rate)} / "
        f"{_format_backtest_slippage_summary(config)} / "
        f"资金费{_format_fee_rate_percent(config.backtest_funding_rate)}"
    )

def _backtest_export_detail_lines(export_path: str | None) -> list[str]:
    if not export_path:
        return []
    try:
        return describe_backtest_export_artifacts(export_path)
    except Exception:
        return [f"报告文件：{export_path}"]


def _build_backtest_compare_detail(snapshot: _BacktestSnapshot) -> str:
    config = snapshot.config
    strategy_name = _strategy_display_name(config)
    start_text, end_text = _backtest_snapshot_range_text(snapshot)
    lines = [
        f"编号：{snapshot.snapshot_id}",
        f"回测时间：{snapshot.created_at.strftime('%Y-%m-%d %H:%M:%S')}",
        f"策略：{strategy_name}",
        f"交易对：{config.inst_id}",
        f"K线周期：{_normalize_backtest_bar_label(config.bar)}",
        f"回测K线数：{_format_backtest_candle_limit(snapshot.candle_limit)}",
        f"参数：{_build_backtest_param_summary(config, maker_fee_rate=snapshot.maker_fee_rate, taker_fee_rate=snapshot.taker_fee_rate)}",
        f"开始时间：{start_text}",
        f"结束时间：{end_text}",
    ]
    lines.extend(_backtest_export_detail_lines(snapshot.export_path))
    lines.extend([
        "",
        snapshot.report_text,
    ])
    return "\n".join(lines)


def _format_trade_exit_reason(exit_reason: str) -> str:
    return {
        "take_profit": "止盈",
        "stop_loss": "止损",
        "signal_profit_exit": "信号失效盈利平仓",
    }.get(exit_reason, exit_reason)


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
    if is_dynamic_strategy_id(config.strategy_id):
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
    return (config.atr_stop_multiplier, config.atr_take_multiplier)


def _serialize_strategy_config(config: StrategyConfig) -> dict[str, object]:
    return {
        "inst_id": config.inst_id,
        "bar": config.bar,
        "ema_period": config.ema_period,
        "trend_ema_period": config.trend_ema_period,
        "big_ema_period": config.big_ema_period,
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
        "dynamic_two_r_break_even": config.dynamic_two_r_break_even,
        "dynamic_fee_offset_enabled": config.dynamic_fee_offset_enabled,
        "time_stop_break_even_enabled": config.time_stop_break_even_enabled,
        "time_stop_break_even_bars": config.resolved_time_stop_break_even_bars(),
        "hold_close_exit_bars": int(config.hold_close_exit_bars),
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
    return StrategyConfig(
        inst_id=str(payload.get("inst_id", "")),
        bar=str(payload.get("bar", "15m")),
        ema_period=int(payload.get("ema_period", 21)),
        trend_ema_period=int(payload.get("trend_ema_period", 55)),
        big_ema_period=int(payload.get("big_ema_period", 233)),
        entry_reference_ema_period=int(payload.get("entry_reference_ema_period", 55)),
        atr_period=int(payload.get("atr_period", 14)),
        atr_stop_multiplier=Decimal(str(payload.get("atr_stop_multiplier", "1.5"))),
        atr_take_multiplier=Decimal(str(payload.get("atr_take_multiplier", "4"))),
        order_size=Decimal(str(payload.get("order_size", "0"))),
        trade_mode=str(payload.get("trade_mode", "cross")),
        signal_mode=str(payload.get("signal_mode", "both")),
        position_mode=str(payload.get("position_mode", "net")),
        environment=str(payload.get("environment", "demo")),
        tp_sl_trigger_type=str(payload.get("tp_sl_trigger_type", "mark")),
        strategy_id=str(payload.get("strategy_id", STRATEGY_DYNAMIC_ID)),
        poll_seconds=float(payload.get("poll_seconds", 3.0)),
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
        dynamic_two_r_break_even=bool(payload.get("dynamic_two_r_break_even", True)),
        dynamic_fee_offset_enabled=bool(payload.get("dynamic_fee_offset_enabled", True)),
        time_stop_break_even_enabled=bool(payload.get("time_stop_break_even_enabled", False)),
        time_stop_break_even_bars=int(payload.get("time_stop_break_even_bars", 10)),
        hold_close_exit_bars=int(payload.get("hold_close_exit_bars", 0)),
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

        self.tree = ttk.Treeview(
            self.window,
            columns=("id", "time", "start", "end", "strategy", "symbol", "bar", "params", "trades", "win_rate", "pnl", "drawdown"),
            show="headings",
            selectmode="browse",
        )
        self.tree.heading("id", text="编号")
        self.tree.heading("time", text="回测时间")
        self.tree.heading("start", text="开始时间")
        self.tree.heading("end", text="结束时间")
        self.tree.heading("strategy", text="策略")
        self.tree.heading("symbol", text="交易对")
        self.tree.heading("bar", text="周期")
        self.tree.heading("params", text="参数摘要")
        self.tree.heading("trades", text="交易数")
        self.tree.heading("win_rate", text="胜率")
        self.tree.heading("pnl", text="总盈亏")
        self.tree.heading("drawdown", text="最大回撤")
        self.tree.column("id", width=70, anchor="center")
        self.tree.column("time", width=150, anchor="center")
        self.tree.column("start", width=145, anchor="center")
        self.tree.column("end", width=145, anchor="center")
        self.tree.column("strategy", width=120, anchor="center")
        self.tree.column("symbol", width=140, anchor="center")
        self.tree.column("bar", width=70, anchor="center")
        self.tree.column("params", width=280, anchor="w")
        self.tree.column("trades", width=70, anchor="e")
        self.tree.column("win_rate", width=80, anchor="e")
        self.tree.column("pnl", width=110, anchor="e")
        self.tree.column("drawdown", width=110, anchor="e")
        self.tree.grid(row=1, column=0, sticky="nsew", padx=16, pady=(0, 8))
        self.tree.bind("<<TreeviewSelect>>", self._on_tree_selected)

        detail_frame = ttk.LabelFrame(self.window, text="记录详情", padding=12)
        detail_frame.grid(row=2, column=0, sticky="nsew", padx=16, pady=(0, 16))
        detail_frame.columnconfigure(0, weight=1)
        detail_frame.rowconfigure(0, weight=1)
        self.detail_text = Text(detail_frame, wrap="word", font=("Consolas", 10))
        self.detail_text.grid(row=0, column=0, sticky="nsew")

    def _refresh(self) -> None:
        snapshots = self._store.list_snapshots()
        previous_selection = self.tree.selection()
        self.tree.delete(*self.tree.get_children())
        for snapshot in snapshots:
            self.tree.insert("", END, iid=snapshot.snapshot_id, values=_build_backtest_compare_row(snapshot))
        if snapshots:
            self.summary_text.set(f"已保存 {len(snapshots)} 组历史回测结果。关闭程序后仍会保留。")
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
        self.detail_text.insert("1.0", _build_backtest_compare_detail(snapshot))

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
            width_ratio=0.8,
            height_ratio=0.88,
            min_width=1100,
            min_height=900,
            max_width=1580,
            max_height=1220,
        )

        self._strategy_name_to_id = {item.name: item.strategy_id for item in BACKTEST_STRATEGY_DEFINITIONS}

        self.strategy_name = StringVar(value=initial_state.strategy_name)
        self.symbol = StringVar(value=initial_state.symbol)
        self.bar_label = StringVar(value=_normalize_backtest_bar_label(initial_state.bar))
        self.ema_period = StringVar(value=initial_state.ema_period)
        self.trend_ema_period = StringVar(value=initial_state.trend_ema_period)
        self.big_ema_period = StringVar(value=initial_state.big_ema_period)
        self.entry_reference_ema_period = StringVar(value=initial_state.entry_reference_ema_period)
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
        self.dynamic_two_r_break_even = BooleanVar(value=initial_state.dynamic_two_r_break_even)
        self.dynamic_fee_offset_enabled = BooleanVar(value=initial_state.dynamic_fee_offset_enabled)
        self.time_stop_break_even_enabled = BooleanVar(value=initial_state.time_stop_break_even_enabled)
        self.time_stop_break_even_bars = StringVar(value=initial_state.time_stop_break_even_bars)
        self.hold_close_exit_bars = StringVar(value=initial_state.hold_close_exit_bars)
        self.signal_mode_label = StringVar(value=initial_state.signal_mode_label)
        self.trade_mode_label = StringVar(value=initial_state.trade_mode_label)
        self.position_mode_label = StringVar(value=initial_state.position_mode_label)
        self.trigger_type_label = StringVar(value=initial_state.trigger_type_label)
        self.environment_label = StringVar(value=initial_state.environment_label)
        self.candle_limit = StringVar(value=initial_state.candle_limit)
        self._strategy_parameter_drafts = load_strategy_parameter_drafts()
        self._strategy_parameter_scope = "backtest"
        self._last_strategy_parameter_strategy_id: str | None = None
        self.history_sync_status = StringVar(
            value="填 0 = 全量；填 10000 = 最新往前 10000 根。可先点“同步历史数据”下载 5 个币种的 5m / 15m / 1H / 4H 全量缓存。"
        )
        self.report_summary = StringVar(value="点击“开始回测”后，会在这里显示报告摘要。")
        self.manual_summary = StringVar(value="当前策略没有额外扩展统计。")
        self.manual_filter_label = StringVar(value="全部")
        self.manual_sort_label = StringVar(value=MANUAL_DEFAULT_SORT_LABEL)
        self.compare_summary = StringVar(value="暂无回测对比记录。")
        self.matrix_summary = StringVar(value="\u6682\u65e0 ATR \u6279\u91cf\u56de\u6d4b\u77e9\u9635\u3002")
        self.heatmap_summary = StringVar(
            value="\u53c2\u6570\u70ed\u529b\u56fe\u4f1a\u5728\u8fd9\u91cc\u663e\u793a\uff0c\u53ef\u5207\u6362\u6307\u6807\u5e76\u5355\u51fb\u5355\u5143\u683c\u8054\u52a8\u56de\u6d4b\u89c6\u56fe\u3002"
        )
        self.heatmap_metric = StringVar(value="总盈亏")
        self.batch_entries_layer_label = StringVar(value=_batch_entries_label(BATCH_MAX_ENTRIES_OPTIONS[0]))
        self._latest_result: BacktestResult | None = None
        self._chart_zoom_window: Toplevel | None = None
        self._chart_zoom_canvas: Canvas | None = None
        self._chart_zoom_intro_label: ttk.Label | None = None
        self._chart_zoom_context_label: ttk.Label | None = None
        self._chart_zoom_metrics_label: ttk.Label | None = None
        self._chart_redraw_job: str | None = None
        self._chart_canvas_redraw_jobs: dict[int, str] = {}
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
        self._batch_sequence = 0
        self._batch_snapshot_groups: dict[str, list[str]] = {}
        self._snapshot_batch_labels: dict[str, str] = {}
        self._current_matrix_batch_label: str | None = None

        self._build_layout()
        self._update_batch_layer_controls("none", [])
        self._apply_selected_strategy_definition()
        self._update_sizing_mode_widgets()

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
        return is_dynamic_strategy_id(strategy_id) or is_ema_atr_breakout_strategy(strategy_id)

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
            default_value = strategy_parameter_default_value(key)
            if default_value is None:
                continue
            if key == "bar":
                variable.set(_normalize_backtest_bar_label(str(default_value)))
            elif key == "signal_mode":
                variable.set(SIGNAL_VALUE_TO_LABEL.get(str(default_value), definition.default_signal_label))
            elif key == "take_profit_mode":
                variable.set(TAKE_PROFIT_MODE_VALUE_TO_LABEL.get(str(default_value), self.take_profit_mode_label.get()))
            else:
                variable.set(default_value)
        for key in iter_strategy_parameter_keys(strategy_id):
            fixed_value = strategy_fixed_value(strategy_id, key)
            if fixed_value is None:
                continue
            variable = bindings.get(key)
            if variable is not None:
                if key == "bar":
                    variable.set(_normalize_backtest_bar_label(str(fixed_value)))
                elif key == "signal_mode":
                    variable.set(SIGNAL_VALUE_TO_LABEL.get(str(fixed_value), definition.default_signal_label))
                else:
                    variable.set(fixed_value)

    def _resolve_strategy_parameter_value(self, strategy_id: str, key: str, current_value: object) -> object:
        fixed_value = strategy_fixed_value(strategy_id, key)
        if fixed_value is not None:
            return fixed_value
        return current_value

    def _apply_strategy_parameter_fixed_labels(self, strategy_id: str) -> None:
        fixed_suffix = "（本策略固定）"
        if is_ema_atr_breakout_strategy(strategy_id):
            self.entry_reference_ema_caption.configure(
                text="跌破参考EMA周期" if strategy_id == STRATEGY_EMA_BREAKDOWN_SHORT_ID else "突破参考EMA周期"
            )
        else:
            self.entry_reference_ema_caption.configure(text="挂单参考EMA")
        label_map = {
            "bar": (self.bar_caption, "K线周期"),
            "signal_mode": (self.signal_caption, "信号方向"),
            "ema_period": (self.ema_period_caption, "EMA小周期"),
            "trend_ema_period": (self.trend_ema_period_caption, "EMA中周期"),
            "big_ema_period": (self.big_ema_caption, "EMA大周期"),
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
        try:
            screen_h = max(int(self.window.winfo_screenheight()), 600)
        except Exception:
            screen_h = 800
        cap = max(320, min(int(screen_h * 0.46), 920))
        view_h = max(1, min(inner_h + 10, cap))
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

    def _build_layout(self) -> None:
        self.window.columnconfigure(0, weight=1)
        self.window.rowconfigure(1, weight=1)
        self.window.rowconfigure(2, weight=3)

        params_viewport = ttk.Frame(self.window)
        params_viewport.grid(row=0, column=0, sticky="ew", padx=16, pady=(16, 8))
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

        controls = ttk.LabelFrame(params_inner, text="回测参数", padding=16)
        controls.grid(row=0, column=0, sticky="ew")
        self._controls_frame = controls
        self._bind_params_canvas_mousewheel()
        self.window.after_idle(self._sync_backtest_params_viewport)
        self.window.after(120, self._sync_backtest_params_viewport)

        for column in range(6):
            controls.columnconfigure(column, weight=1)

        row = 0
        ttk.Label(controls, text="策略").grid(row=row, column=0, sticky="w")
        strategy_combo = ttk.Combobox(
            controls,
            textvariable=self.strategy_name,
            values=[item.name for item in BACKTEST_STRATEGY_DEFINITIONS],
            state="readonly",
        )
        strategy_combo.grid(row=row, column=1, sticky="ew", padx=(0, 12))
        strategy_combo.bind("<<ComboboxSelected>>", self._on_strategy_selected)
        ttk.Label(controls, text="交易对").grid(row=row, column=2, sticky="w")
        self.symbol_combo = ttk.Combobox(
            controls,
            textvariable=self.symbol,
            values=_build_backtest_symbol_options(self.symbol.get()),
            state="readonly",
        )
        self.symbol_combo.grid(row=row, column=3, sticky="ew", padx=(0, 12))
        self.bar_caption = ttk.Label(controls, text="K线周期")
        self.bar_caption.grid(row=row, column=4, sticky="w")
        self.bar_combo = ttk.Combobox(
            controls,
            textvariable=self.bar_label,
            values=list(BACKTEST_BAR_LABEL_TO_VALUE.keys()),
            state="readonly",
        )
        self.bar_combo.grid(row=row, column=5, sticky="ew")
        row += 1
        self.ema_period_caption = ttk.Label(controls, text="EMA小周期")
        self.ema_period_caption.grid(row=row, column=0, sticky="w", pady=(12, 0))
        self.ema_period_entry = ttk.Entry(controls, textvariable=self.ema_period)
        self.ema_period_entry.grid(row=row, column=1, sticky="ew", padx=(0, 12), pady=(12, 0))
        self.trend_ema_period_caption = ttk.Label(controls, text="EMA中周期")
        self.trend_ema_period_caption.grid(row=row, column=2, sticky="w", pady=(12, 0))
        self.trend_ema_period_entry = ttk.Entry(controls, textvariable=self.trend_ema_period)
        self.trend_ema_period_entry.grid(row=row, column=3, sticky="ew", padx=(0, 12), pady=(12, 0))
        self.big_ema_caption = ttk.Label(controls, text="EMA大周期")
        self.big_ema_caption.grid(row=row, column=4, sticky="w", pady=(12, 0))
        self.big_ema_entry = ttk.Entry(controls, textvariable=self.big_ema_period)
        self.big_ema_entry.grid(row=row, column=5, sticky="ew", pady=(12, 0))

        row += 1
        self.atr_period_caption = ttk.Label(controls, text="ATR 周期")
        self.atr_period_caption.grid(row=row, column=0, sticky="w", pady=(12, 0))
        self.atr_period_entry = ttk.Entry(controls, textvariable=self.atr_period)
        self.atr_period_entry.grid(row=row, column=1, sticky="ew", padx=(0, 12), pady=(12, 0))
        self.stop_atr_caption = ttk.Label(controls, text="止损 ATR 倍数")
        self.stop_atr_caption.grid(row=row, column=2, sticky="w", pady=(12, 0))
        self.stop_atr_entry = ttk.Entry(controls, textvariable=self.stop_atr)
        self.stop_atr_entry.grid(row=row, column=3, sticky="ew", padx=(0, 12), pady=(12, 0))
        self.take_atr_caption = ttk.Label(controls, text="止盈 ATR 倍数")
        self.take_atr_caption.grid(row=row, column=4, sticky="w", pady=(12, 0))
        self.take_atr_entry = ttk.Entry(controls, textvariable=self.take_atr)
        self.take_atr_entry.grid(row=row, column=5, sticky="ew", pady=(12, 0))

        row += 1
        self.signal_caption = ttk.Label(controls, text="信号方向")
        self.signal_caption.grid(row=row, column=0, sticky="w", pady=(12, 0))
        self.signal_combo = ttk.Combobox(controls, textvariable=self.signal_mode_label, state="readonly")
        self.signal_combo.grid(row=row, column=1, sticky="ew", padx=(0, 12), pady=(12, 0))
        self.entry_reference_ema_caption = ttk.Label(controls, text="挂单参考EMA")
        self.entry_reference_ema_caption.grid(row=row, column=2, sticky="w", pady=(12, 0))
        self.entry_reference_ema_entry = ttk.Entry(controls, textvariable=self.entry_reference_ema_period)
        self.entry_reference_ema_entry.grid(row=row, column=3, sticky="ew", padx=(0, 12), pady=(12, 0))

        row += 1
        self.take_profit_mode_caption = ttk.Label(controls, text="止盈方式")
        self.take_profit_mode_caption.grid(row=row, column=0, sticky="w", pady=(12, 0))
        self.take_profit_mode_combo = ttk.Combobox(
            controls,
            textvariable=self.take_profit_mode_label,
            values=list(TAKE_PROFIT_MODE_OPTIONS.keys()),
            state="readonly",
        )
        self.take_profit_mode_combo.grid(row=row, column=1, sticky="ew", padx=(0, 12), pady=(12, 0))
        self.take_profit_mode_combo.bind("<<ComboboxSelected>>", lambda *_: self._sync_dynamic_take_profit_controls())
        self.max_entries_caption = ttk.Label(controls, text="每波最多开仓次数")
        self.max_entries_caption.grid(row=row, column=2, sticky="w", pady=(12, 0))
        self.max_entries_entry = ttk.Entry(controls, textvariable=self.max_entries_per_trend)
        self.max_entries_entry.grid(row=row, column=3, sticky="ew", padx=(0, 12), pady=(12, 0))

        row += 1
        self.dynamic_two_r_break_even_check = ttk.Checkbutton(
            controls,
            text="启用2R保本（2R时先移到保本位）",
            variable=self.dynamic_two_r_break_even,
        )
        self.dynamic_two_r_break_even_check.grid(row=row, column=0, columnspan=4, sticky="w", pady=(12, 0))

        row += 1
        self.dynamic_fee_offset_check = ttk.Checkbutton(
            controls,
            text="启用手续费偏移（按2倍Taker手续费留缓冲）",
            variable=self.dynamic_fee_offset_enabled,
        )
        self.dynamic_fee_offset_check.grid(row=row, column=0, columnspan=4, sticky="w", pady=(8, 0))

        row += 1
        self.dynamic_fee_offset_hint_label = ttk.Label(
            controls,
            text="提示：保本位是否叠加手续费偏移，由下方开关决定；大部分组合开启更优，默认建议开启。",
        )
        self.dynamic_fee_offset_hint_label.grid(row=row, column=0, columnspan=4, sticky="w", pady=(2, 0))

        row += 1
        self.time_stop_break_even_check = ttk.Checkbutton(
            controls,
            text="启用时间保本（持仓满指定K线且已达到净保本时，上移到保本位）",
            variable=self.time_stop_break_even_enabled,
            command=self._sync_dynamic_take_profit_controls,
        )
        self.time_stop_break_even_check.grid(row=row, column=0, columnspan=2, sticky="w", pady=(8, 0))
        self.time_stop_break_even_bars_label = ttk.Label(controls, text="时间保本K线数")
        self.time_stop_break_even_bars_label.grid(row=row, column=2, sticky="e", pady=(8, 0))
        self.time_stop_break_even_bars_entry = ttk.Entry(controls, textvariable=self.time_stop_break_even_bars)
        self.time_stop_break_even_bars_entry.grid(row=row, column=3, sticky="ew", padx=(0, 12), pady=(8, 0))

        row += 1
        self.hold_close_exit_bars_caption = ttk.Label(controls, text="满N根K线收盘价平仓")
        self.hold_close_exit_bars_caption.grid(row=row, column=0, sticky="w", pady=(8, 0))
        self.hold_close_exit_bars_entry = ttk.Entry(controls, textvariable=self.hold_close_exit_bars)
        self.hold_close_exit_bars_entry.grid(row=row, column=1, sticky="ew", padx=(0, 12), pady=(8, 0))
        self.hold_close_exit_hint = ttk.Label(
            controls,
            text="填0关闭；从开仓K线索引起计满N根已收盘K线后，当根按收盘价平仓。",
            foreground="#57606a",
        )
        self.hold_close_exit_hint.grid(row=row, column=2, columnspan=4, sticky="w", pady=(8, 0))

        row += 1
        self.size_or_risk_label = ttk.Label(controls, text="固定风险金/数量")
        self.size_or_risk_label.grid(row=row, column=0, sticky="w", pady=(12, 0))
        self.size_or_risk_entry = ttk.Entry(controls, textvariable=self.risk_amount)
        self.size_or_risk_entry.grid(row=row, column=1, sticky="ew", padx=(0, 12), pady=(12, 0))
        ttk.Label(controls, text="Maker手续费(%)").grid(row=row, column=2, sticky="w", pady=(12, 0))
        ttk.Entry(controls, textvariable=self.maker_fee_percent).grid(
            row=row, column=3, sticky="ew", padx=(0, 12), pady=(12, 0)
        )
        ttk.Label(controls, text="Taker手续费(%)").grid(row=row, column=4, sticky="w", pady=(12, 0))
        ttk.Entry(controls, textvariable=self.taker_fee_percent).grid(row=row, column=5, sticky="ew", pady=(12, 0))

        row += 1
        ttk.Label(controls, text="初始资金").grid(row=row, column=0, sticky="w", pady=(12, 0))
        ttk.Entry(controls, textvariable=self.initial_capital).grid(row=row, column=1, sticky="ew", padx=(0, 12), pady=(12, 0))
        ttk.Label(controls, text="仓位模式").grid(row=row, column=2, sticky="w", pady=(12, 0))
        self.sizing_mode_combo = ttk.Combobox(
            controls,
            textvariable=self.sizing_mode_label,
            values=list(BACKTEST_SIZING_OPTIONS.keys()),
            state="readonly",
        )
        self.sizing_mode_combo.grid(row=row, column=3, sticky="ew", padx=(0, 12), pady=(12, 0))
        self.sizing_mode_combo.bind("<<ComboboxSelected>>", lambda *_: self._update_sizing_mode_widgets())
        ttk.Label(controls, text="风险百分比(%)").grid(row=row, column=4, sticky="w", pady=(12, 0))
        self.risk_percent_entry = ttk.Entry(controls, textvariable=self.risk_percent)
        self.risk_percent_entry.grid(row=row, column=5, sticky="ew", pady=(12, 0))

        row += 1
        ttk.Label(controls, text="开仓滑点(%)").grid(row=row, column=0, sticky="w", pady=(12, 0))
        ttk.Entry(controls, textvariable=self.entry_slippage_percent).grid(
            row=row, column=1, sticky="ew", padx=(0, 12), pady=(12, 0)
        )
        ttk.Label(controls, text="平仓滑点(%)").grid(row=row, column=2, sticky="w", pady=(12, 0))
        ttk.Entry(controls, textvariable=self.exit_slippage_percent).grid(
            row=row, column=3, sticky="ew", padx=(0, 12), pady=(12, 0)
        )
        ttk.Label(controls, text="资金费率/8h(%)").grid(row=row, column=4, sticky="w", pady=(12, 0))
        ttk.Entry(controls, textvariable=self.funding_rate_percent).grid(row=row, column=5, sticky="ew", pady=(12, 0))

        row += 1
        ttk.Checkbutton(controls, text="启用复利", variable=self.compounding_enabled).grid(
            row=row, column=0, columnspan=2, sticky="w", pady=(12, 0)
        )

        row += 1
        ttk.Label(controls, text="开始时间").grid(row=row, column=0, sticky="w", pady=(12, 0))
        ttk.Entry(controls, textvariable=self.start_time_text).grid(
            row=row, column=1, sticky="ew", padx=(0, 12), pady=(12, 0)
        )
        ttk.Label(controls, text="结束时间").grid(row=row, column=2, sticky="w", pady=(12, 0))
        ttk.Entry(controls, textvariable=self.end_time_text).grid(
            row=row, column=3, sticky="ew", padx=(0, 12), pady=(12, 0)
        )
        ttk.Label(
            controls,
            text="支持 YYYYMMDD 或 YYYYMMDD HH:MM",
        ).grid(row=row, column=4, columnspan=2, sticky="w", pady=(12, 0))

        row += 1
        ttk.Label(controls, text="回测K线数").grid(row=row, column=0, sticky="w", pady=(12, 0))
        ttk.Entry(controls, textvariable=self.candle_limit).grid(row=row, column=1, sticky="ew", padx=(0, 12), pady=(12, 0))
        ttk.Label(
            controls,
            text="填 0 = 全量；填 10000 = 最新往前 10000 根；正数上限 10000。",
        ).grid(row=row, column=2, columnspan=2, sticky="w", pady=(12, 0))
        self.sync_history_button = ttk.Button(controls, text="同步历史数据", command=self.sync_history_data)
        self.sync_history_button.grid(row=row, column=4, sticky="e", pady=(12, 0), padx=(0, 8))
        self.batch_backtest_button = ttk.Button(controls, text="开始回测", command=self.start_backtest)
        self.batch_backtest_button.grid(row=row, column=5, sticky="e", pady=(12, 0))

        row += 1
        batch_note = ttk.Frame(controls)
        batch_note.grid(row=row, column=0, columnspan=6, sticky="ew", pady=(8, 0))
        batch_note.columnconfigure(0, weight=1)
        ttk.Label(
            batch_note,
            text="\u201c\u5f00\u59cb\u56de\u6d4b\u201d\u4f1a\u56fa\u5b9a\u8fd0\u884c SL x 1/1.5/2\uff0c\u6bcf\u4e00\u884c\u518d\u6309 TP = SL x 1/2/3 \u751f\u6210 9 \u7ec4\u6279\u91cf\u56de\u6d4b\u3002",
        ).grid(row=0, column=0, sticky="w")
        self.single_backtest_button = ttk.Button(
            batch_note,
            text="\u5f53\u524d\u53c2\u6570\u5355\u7ec4\u56de\u6d4b",
            command=self.start_single_backtest,
        )
        self.single_backtest_button.grid(row=0, column=1, sticky="e", padx=(8, 0))
        ttk.Label(
            batch_note,
            textvariable=self.history_sync_status,
        ).grid(row=1, column=0, columnspan=2, sticky="w", pady=(6, 0))

        report_frame = ttk.Panedwindow(self.window, orient="horizontal")
        report_frame.grid(row=1, column=0, sticky="nsew", padx=16, pady=(0, 8))

        summary_frame = ttk.LabelFrame(report_frame, text="回测报告", padding=12)
        trades_frame = ttk.LabelFrame(report_frame, text="交易明细", padding=12)
        report_frame.add(summary_frame, weight=1)
        report_frame.add(trades_frame, weight=1)

        summary_frame.columnconfigure(0, weight=1)
        summary_frame.rowconfigure(0, weight=1)
        trades_frame.columnconfigure(0, weight=1)
        trades_frame.rowconfigure(0, weight=1)

        report_notebook = ttk.Notebook(summary_frame)
        report_notebook.grid(row=0, column=0, sticky="nsew")

        report_tab = ttk.Frame(report_notebook, padding=8)
        report_tab.columnconfigure(0, weight=1)
        report_tab.rowconfigure(1, weight=1)
        ttk.Label(report_tab, textvariable=self.report_summary, wraplength=480, justify="left").grid(
            row=0, column=0, sticky="w", pady=(0, 10)
        )
        self.report_text = Text(report_tab, height=16, wrap="word", font=("Consolas", 10))
        self.report_text.grid(row=1, column=0, sticky="nsew")
        report_notebook.add(report_tab, text="当前报告")

        compare_tab = ttk.Frame(report_notebook, padding=8)
        compare_tab.columnconfigure(0, weight=1)
        compare_tab.rowconfigure(1, weight=1)
        compare_tab.rowconfigure(2, weight=1)

        compare_toolbar = ttk.Frame(compare_tab)
        compare_toolbar.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        compare_toolbar.columnconfigure(0, weight=1)
        ttk.Label(compare_toolbar, textvariable=self.compare_summary, justify="left").grid(row=0, column=0, sticky="w")
        ttk.Button(compare_toolbar, text="加载所选", command=self.load_selected_snapshot).grid(row=0, column=1, sticky="e", padx=(8, 8))
        ttk.Button(compare_toolbar, text="清空记录", command=self.clear_backtest_snapshots).grid(row=0, column=2, sticky="e")

        compare_tree_frame = ttk.Frame(compare_tab)
        compare_tree_frame.grid(row=1, column=0, sticky="nsew")
        compare_tree_frame.columnconfigure(0, weight=1)
        compare_tree_frame.rowconfigure(0, weight=1)

        self.compare_tree = ttk.Treeview(
            compare_tree_frame,
            columns=("id", "time", "start", "end", "strategy", "symbol", "bar", "params", "trades", "win_rate", "pnl", "drawdown"),
            show="headings",
            selectmode="browse",
        )
        self.compare_tree.heading("id", text="编号")
        self.compare_tree.heading("time", text="回测时间")
        self.compare_tree.heading("start", text="开始时间")
        self.compare_tree.heading("end", text="结束时间")
        self.compare_tree.heading("strategy", text="策略")
        self.compare_tree.heading("symbol", text="交易对")
        self.compare_tree.heading("bar", text="周期")
        self.compare_tree.heading("params", text="参数摘要")
        self.compare_tree.heading("trades", text="交易数")
        self.compare_tree.heading("win_rate", text="胜率")
        self.compare_tree.heading("pnl", text="总盈亏")
        self.compare_tree.heading("drawdown", text="最大回撤")
        self.compare_tree.column("id", width=60, anchor="center")
        self.compare_tree.column("time", width=140, anchor="center")
        self.compare_tree.column("start", width=135, anchor="center")
        self.compare_tree.column("end", width=135, anchor="center")
        self.compare_tree.column("strategy", width=110, anchor="center")
        self.compare_tree.column("symbol", width=130, anchor="center")
        self.compare_tree.column("bar", width=70, anchor="center")
        self.compare_tree.column("params", width=220, anchor="w")
        self.compare_tree.column("trades", width=70, anchor="e")
        self.compare_tree.column("win_rate", width=80, anchor="e")
        self.compare_tree.column("pnl", width=100, anchor="e")
        self.compare_tree.column("drawdown", width=100, anchor="e")
        compare_tree_xscroll = ttk.Scrollbar(compare_tree_frame, orient="horizontal", command=self.compare_tree.xview)
        self.compare_tree.configure(xscrollcommand=compare_tree_xscroll.set)
        self.compare_tree.grid(row=0, column=0, sticky="nsew")
        compare_tree_xscroll.grid(row=1, column=0, sticky="ew")
        self.compare_tree.bind("<<TreeviewSelect>>", self._on_compare_tree_selected)
        self.compare_tree.bind("<Double-Button-1>", lambda *_: self.load_selected_snapshot())

        self.compare_detail_text = Text(compare_tab, height=8, wrap="word", font=("Consolas", 10))
        self.compare_detail_text.grid(row=2, column=0, sticky="nsew", pady=(8, 0))
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
        self.matrix_grid_frame = ttk.Frame(matrix_tab)
        self.matrix_grid_frame.grid(row=2, column=0, sticky="nsew")
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
        self.heatmap_canvas = Canvas(heatmap_tab, background="#ffffff", highlightthickness=0)
        self.heatmap_canvas.grid(row=2, column=0, sticky="nsew")
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
        self.monthly_stats_tree.grid(row=0, column=0, sticky="nsew")
        stats_notebook.add(monthly_tab, text="月度统计")

        yearly_tab = ttk.Frame(stats_notebook, padding=8)
        yearly_tab.columnconfigure(0, weight=1)
        yearly_tab.rowconfigure(0, weight=1)
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
        self.yearly_stats_tree.grid(row=0, column=0, sticky="nsew")
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

        ttk.Label(manual_tab, textvariable=self.manual_summary, wraplength=520, justify="left").grid(
            row=0, column=0, sticky="w", pady=(0, 10)
        )

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

        trades_notebook.add(trade_tab, text="已平仓")

        self.chart_frame = ttk.LabelFrame(self.window, text="K线图、资金曲线与止盈止损触发位置 | 暂无选中回测", padding=12)
        self.chart_frame.grid(row=2, column=0, sticky="nsew", padx=16, pady=(0, 16))
        self.chart_frame.columnconfigure(0, weight=1)
        self.chart_frame.rowconfigure(1, weight=1)

        chart_toolbar = ttk.Frame(self.chart_frame)
        chart_toolbar.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        chart_toolbar.columnconfigure(0, weight=1)
        ttk.Label(
            chart_toolbar,
            text="支持滚轮缩放、按住左键拖动平移；上方显示K线与EMA，下方显示资金曲线，可使用“图表大窗”或双击主图放大观察。",
        ).grid(row=0, column=0, sticky="w")
        ttk.Button(chart_toolbar, text="重置视图", command=self.reset_main_chart_view).grid(row=0, column=1, sticky="e", padx=(0, 8))
        ttk.Button(chart_toolbar, text="图表大窗", command=self.open_chart_zoom_window).grid(row=0, column=2, sticky="e")

        self.chart_canvas = Canvas(self.chart_frame, background="#ffffff", highlightthickness=0)
        self.chart_canvas.grid(row=1, column=0, sticky="nsew")
        self.chart_canvas.bind("<Double-Button-1>", lambda *_: self.open_chart_zoom_window())
        self.chart_canvas.bind("<Configure>", self._schedule_chart_redraw)
        self._bind_chart_interactions(self.chart_canvas)

    def _update_sizing_mode_widgets(self) -> None:
        definition = self._selected_strategy_definition()
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

    def start_backtest(self) -> None:
        try:
            config = self._build_config()
            candle_limit = self._parse_positive_int(self.candle_limit.get(), "回测K线数")
        except Exception as exc:
            messagebox.showerror("回测参数错误", str(exc), parent=self.window)
            return

        self.report_summary.set(
            f"编号：{snapshot.snapshot_id} | 时间：{snapshot.created_at.strftime('%Y-%m-%d %H:%M:%S')} | "
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
            f"编号：{snapshot.snapshot_id} | 时间：{snapshot.created_at.strftime('%Y-%m-%d %H:%M:%S')} | "
            f"策略：{STRATEGY_ID_TO_NAME.get(snapshot.config.strategy_id, snapshot.config.strategy_id)} | "
            f"交易对：{snapshot.config.inst_id} | K线：{_normalize_backtest_bar_label(snapshot.config.bar)} | "
            f"交易次数：{result.report.total_trades}"
        )
        self.report_text.delete("1.0", END)
        self.report_text.insert("1.0", format_backtest_report(result))
        self.trade_tree.delete(*self.trade_tree.get_children())
        for index, trade in enumerate(result.trades, start=1):
            exit_reason = {
                "take_profit": "止盈",
                "stop_loss": "止损",
            }.get(trade.exit_reason, trade.exit_reason)
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
        if self._backtest_running or self._history_sync_running:
            return
        if self._selected_strategy_definition().strategy_id == STRATEGY_EMA5_EMA8_ID:
            self.start_single_backtest()
            return
        try:
            config, candle_limit, maker_fee_rate, taker_fee_rate, start_ts, end_ts = self._build_backtest_request()
            batch_count = len(build_parameter_batch_configs(config))
        except Exception as exc:
            messagebox.showerror("回测参数错误", str(exc), parent=self.window)
            return

        summary_text = f"正在批量回测 {batch_count} 组参数组合，请稍候..."
        self._prepare_backtest_output(summary_text)
        self._set_backtest_running(True)
        batch_label = self._next_batch_label()
        threading.Thread(
            target=self._run_batch_backtest_worker,
            args=(config, candle_limit, batch_label, maker_fee_rate, taker_fee_rate, start_ts, end_ts),
            daemon=True,
        ).start()

    def start_single_backtest(self) -> None:
        if self._backtest_running or self._history_sync_running:
            return
        try:
            config, candle_limit, maker_fee_rate, taker_fee_rate, start_ts, end_ts = self._build_backtest_request()
        except Exception as exc:
            messagebox.showerror("回测参数错误", str(exc), parent=self.window)
            return

        self._prepare_backtest_output("\u6b63\u5728\u5355\u7ec4\u56de\u6d4b\uff0c\u8bf7\u7a0d\u5019...")
        self._set_backtest_running(True)
        threading.Thread(
            target=self._run_backtest_worker,
            args=(config, candle_limit, maker_fee_rate, taker_fee_rate, start_ts, end_ts),
            daemon=True,
        ).start()

    def _build_backtest_request(self) -> tuple[StrategyConfig, int, Decimal, Decimal, int | None, int | None]:
        start_ts = self._parse_optional_datetime(self.start_time_text.get(), "开始时间", end_of_day=False)
        end_ts = self._parse_optional_datetime(self.end_time_text.get(), "结束时间", end_of_day=True)
        if start_ts is not None and end_ts is not None and start_ts > end_ts:
            raise ValueError("开始时间不能晚于结束时间")
        return (
            self._build_config(),
            self._parse_backtest_candle_limit(self.candle_limit.get()),
            self._parse_fee_percent(self.maker_fee_percent.get(), "Maker手续费"),
            self._parse_fee_percent(self.taker_fee_percent.get(), "Taker手续费"),
            start_ts,
            end_ts,
        )

    def _prepare_backtest_output(self, summary_text: str) -> None:
        self.report_summary.set(summary_text)
        self.report_text.delete("1.0", END)
        self._clear_detail_tables(
            manual_summary_text="当前策略没有额外扩展统计。"
        )
        self._current_snapshot_id = None
        self._reset_chart_views()
        self._clear_chart_canvas(self.chart_canvas)
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
            f"{snapshot.snapshot_id} | "
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

    def _refresh_action_button_states(self) -> None:
        state = "disabled" if (self._backtest_running or self._history_sync_running) else "normal"
        if self._widget_exists(getattr(self, "single_backtest_button", None)):
            self.single_backtest_button.configure(state=state)
        if self._widget_exists(getattr(self, "batch_backtest_button", None)):
            self.batch_backtest_button.configure(state=state)
        if self._widget_exists(getattr(self, "sync_history_button", None)):
            self.sync_history_button.configure(state=state)

    def sync_history_data(self) -> None:
        if self._backtest_running or self._history_sync_running:
            return
        tasks = [(symbol, bar) for symbol in BACKTEST_SYMBOL_OPTIONS for bar in BACKTEST_HISTORY_SYNC_BARS]
        self.history_sync_status.set(
            f"正在同步历史数据（0/{len(tasks)}）：5 个币种 x 4 个周期，全量缓存下载中，请稍候..."
        )
        self._set_history_sync_running(True)
        threading.Thread(
            target=self._run_history_sync_worker,
            args=(tasks,),
            daemon=True,
        ).start()

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
        maker_fee_rate: Decimal,
        taker_fee_rate: Decimal,
        start_ts: int | None,
        end_ts: int | None,
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
            )
            if self._ui_alive():
                self.window.after(0, lambda: self._apply_batch_backtest_results(results, candle_limit, batch_label))
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
    ) -> None:
        if not self._ui_alive():
            return
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
        if last_snapshot is not None:
            self._load_snapshot(last_snapshot.snapshot_id)
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
        dynamic_strategy = is_dynamic_strategy_id(definition.strategy_id)
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
        if definition.strategy_id == STRATEGY_EMA5_EMA8_ID:
            risk_amount = Decimal("100")
            order_size = Decimal("0")
        signal_mode = SIGNAL_LABEL_TO_VALUE[self.signal_mode_label.get()]
        if dynamic_strategy:
            signal_mode = resolve_dynamic_signal_mode(definition.strategy_id, signal_mode)
        take_profit_mode = "fixed"
        max_entries_per_trend = 0
        entry_reference_ema_period = 0
        dynamic_two_r_break_even = False
        dynamic_fee_offset_enabled = False
        time_stop_break_even_enabled = False
        time_stop_break_even_bars = 0
        hold_close_exit_bars = 0
        if strategy_uses_parameter(definition.strategy_id, "entry_reference_ema_period"):
            entry_reference_ema_period = self._parse_nonnegative_int(
                self.entry_reference_ema_period.get(),
                (
                    "跌破参考EMA周期"
                    if definition.strategy_id == STRATEGY_EMA_BREAKDOWN_SHORT_ID
                    else ("突破参考EMA周期" if is_ema_atr_breakout_strategy(definition.strategy_id) else "挂单参考EMA")
                ),
            )
        if dynamic_tp_strategy:
            take_profit_mode = TAKE_PROFIT_MODE_OPTIONS[self.take_profit_mode_label.get()]
            if strategy_uses_parameter(definition.strategy_id, "max_entries_per_trend"):
                max_entries_per_trend = self._parse_nonnegative_int(self.max_entries_per_trend.get(), "每波最多开仓次数")
            dynamic_two_r_break_even = bool(self.dynamic_two_r_break_even.get())
            dynamic_fee_offset_enabled = bool(self.dynamic_fee_offset_enabled.get())
            time_stop_break_even_enabled = bool(self.time_stop_break_even_enabled.get())
            time_stop_break_even_bars = (
                self._parse_positive_int(self.time_stop_break_even_bars.get(), "时间保本K线数")
                if time_stop_break_even_enabled
                else 0
            )
        if strategy_uses_parameter(definition.strategy_id, "hold_close_exit_bars"):
            hold_close_exit_bars = self._parse_nonnegative_int(self.hold_close_exit_bars.get(), "满N根K线收盘价平仓")
        entry_slippage_rate = self._parse_nonnegative_decimal(self.entry_slippage_percent.get(), "开仓滑点") / Decimal("100")
        exit_slippage_rate = self._parse_nonnegative_decimal(self.exit_slippage_percent.get(), "平仓滑点") / Decimal("100")
        return StrategyConfig(
            inst_id=self.symbol.get().strip().upper(),
            bar=str(self._resolve_strategy_parameter_value(strategy_id, "bar", _backtest_bar_value_from_label(self.bar_label.get()))),
            ema_period=int(
                self._resolve_strategy_parameter_value(
                    strategy_id,
                    "ema_period",
                    self._parse_positive_int(self.ema_period.get(), "EMA小周期"),
                )
            ),
            trend_ema_period=int(
                self._resolve_strategy_parameter_value(
                    strategy_id,
                    "trend_ema_period",
                    self._parse_positive_int(self.trend_ema_period.get(), "EMA中周期"),
                )
            ),
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
            entry_reference_ema_period=entry_reference_ema_period,
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
            dynamic_fee_offset_enabled=dynamic_fee_offset_enabled,
            time_stop_break_even_enabled=time_stop_break_even_enabled,
            time_stop_break_even_bars=time_stop_break_even_bars,
            hold_close_exit_bars=hold_close_exit_bars,
            backtest_initial_capital=self._parse_positive_decimal(self.initial_capital.get(), "初始资金"),
            backtest_sizing_mode=sizing_mode,
            backtest_risk_percent=risk_percent,
            backtest_compounding=bool(self.compounding_enabled.get()),
            backtest_entry_slippage_rate=entry_slippage_rate,
            backtest_exit_slippage_rate=exit_slippage_rate,
            backtest_slippage_rate=exit_slippage_rate,
            backtest_funding_rate=self._parse_nonnegative_decimal(self.funding_rate_percent.get(), "资金费率/8h")
            / Decimal("100"),
        )

    def _selected_strategy_definition(self) -> StrategyDefinition:
        strategy_id = self._strategy_name_to_id[self.strategy_name.get()]
        return get_strategy_definition(strategy_id)

    def _on_strategy_selected(self, *_: object) -> None:
        self._apply_selected_strategy_definition()

    def _apply_selected_strategy_definition(self) -> None:
        definition = self._selected_strategy_definition()
        strategy_id = definition.strategy_id
        previous_strategy_id = self._last_strategy_parameter_strategy_id
        if previous_strategy_id and previous_strategy_id != strategy_id:
            self._save_strategy_parameter_draft(previous_strategy_id)
        self._restore_strategy_parameter_draft(strategy_id)
        dynamic_strategy = is_dynamic_strategy_id(strategy_id)
        dynamic_tp_strategy = self._strategy_supports_dynamic_take_profit(strategy_id)
        self.signal_combo["values"] = definition.allowed_signal_labels
        fixed_signal_mode = strategy_fixed_value(strategy_id, "signal_mode")
        if fixed_signal_mode is not None:
            self.signal_mode_label.set(SIGNAL_VALUE_TO_LABEL.get(str(fixed_signal_mode), definition.default_signal_label))
        elif self.signal_mode_label.get() not in definition.allowed_signal_labels:
            self.signal_mode_label.set(definition.default_signal_label)
        if strategy_id == STRATEGY_EMA5_EMA8_ID:
            self.entry_reference_ema_period.set("0")
            self.risk_amount.set("100")
        if hasattr(self, "_controls_frame"):
            big_ema_widgets = (self.big_ema_caption, self.big_ema_entry)
            for widget in big_ema_widgets:
                if self._strategy_uses_big_ema(strategy_id):
                    widget.grid()
                else:
                    widget.grid_remove()
            entry_reference_widgets = (
                self.entry_reference_ema_caption,
                self.entry_reference_ema_entry,
            )
            for widget in entry_reference_widgets:
                if strategy_uses_parameter(strategy_id, "entry_reference_ema_period"):
                    widget.grid()
                else:
                    widget.grid_remove()
            dynamic_widgets = (
                self.take_profit_mode_caption,
                self.take_profit_mode_combo,
                self.dynamic_two_r_break_even_check,
                self.dynamic_fee_offset_check,
                self.dynamic_fee_offset_hint_label,
                self.time_stop_break_even_check,
                self.time_stop_break_even_bars_label,
                self.time_stop_break_even_bars_entry,
            )
            for widget in dynamic_widgets:
                if dynamic_tp_strategy:
                    widget.grid()
                else:
                    widget.grid_remove()
            max_entries_widgets = (self.max_entries_caption, self.max_entries_entry)
            for widget in max_entries_widgets:
                if strategy_uses_parameter(strategy_id, "max_entries_per_trend"):
                    widget.grid()
                else:
                    widget.grid_remove()
            if not strategy_uses_parameter(strategy_id, "max_entries_per_trend"):
                self.max_entries_caption.configure(text="每波最多开仓次数")
            hold_close_widgets = (
                self.hold_close_exit_bars_caption,
                self.hold_close_exit_bars_entry,
                self.hold_close_exit_hint,
            )
            for widget in hold_close_widgets:
                if strategy_uses_parameter(strategy_id, "hold_close_exit_bars"):
                    widget.grid()
                else:
                    widget.grid_remove()
            self._set_field_state(self.bar_combo, editable=strategy_is_parameter_editable(strategy_id, "bar", "backtest"))
            self._set_field_state(self.ema_period_entry, editable=strategy_is_parameter_editable(strategy_id, "ema_period", "backtest"))
            self._set_field_state(self.trend_ema_period_entry, editable=strategy_is_parameter_editable(strategy_id, "trend_ema_period", "backtest"))
            self._set_field_state(self.big_ema_entry, editable=strategy_is_parameter_editable(strategy_id, "big_ema_period", "backtest"))
            self._set_field_state(self.signal_combo, editable=strategy_is_parameter_editable(strategy_id, "signal_mode", "backtest"))
            self._set_field_state(
                self.hold_close_exit_bars_entry,
                editable=strategy_is_parameter_editable(strategy_id, "hold_close_exit_bars", "backtest"),
            )
            self._apply_strategy_parameter_fixed_labels(strategy_id)
        if dynamic_tp_strategy and not self.entry_reference_ema_period.get().strip():
            self.entry_reference_ema_period.set("55")
        self._last_strategy_parameter_strategy_id = strategy_id
        self._sync_dynamic_take_profit_controls()
        self._update_sizing_mode_widgets()
        if self._latest_result is None:
            self.manual_summary.set("当前策略没有额外扩展统计。")
        if self._latest_result is None:
            self.manual_summary.set("当前策略没有额外扩展统计。")
        if self._ui_alive():
            self.window.after_idle(self._sync_backtest_params_viewport)

    def _sync_dynamic_take_profit_controls(self) -> None:
        if not hasattr(self, "dynamic_two_r_break_even_check"):
            return
        definition = self._selected_strategy_definition()
        dynamic_strategy = self._strategy_supports_dynamic_take_profit(definition.strategy_id)
        dynamic_take_profit = (
            dynamic_strategy and TAKE_PROFIT_MODE_OPTIONS.get(self.take_profit_mode_label.get(), "fixed") == "dynamic"
        )
        self.dynamic_two_r_break_even_check.configure(state="normal" if dynamic_take_profit else "disabled")
        self.dynamic_fee_offset_check.configure(state="normal" if dynamic_take_profit else "disabled")
        self.time_stop_break_even_check.configure(state="normal" if dynamic_take_profit else "disabled")
        self.time_stop_break_even_bars_label.configure(state="normal" if dynamic_take_profit else "disabled")
        self.time_stop_break_even_bars_entry.configure(
            state="normal" if dynamic_take_profit and self.time_stop_break_even_enabled.get() else "disabled"
        )

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
        snapshot = _BacktestSnapshot(
            snapshot_id=f"R{self._backtest_snapshot_sequence:03d}",
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
        )
        self._backtest_snapshots[snapshot.snapshot_id] = snapshot
        self._backtest_snapshot_order.append(snapshot.snapshot_id)
        if batch_label:
            self._batch_snapshot_groups.setdefault(batch_label, []).append(snapshot.snapshot_id)
            self._snapshot_batch_labels[snapshot.snapshot_id] = batch_label
        if self._ui_alive() and self._widget_exists(getattr(self, "compare_tree", None)):
            self.compare_tree.insert("", END, iid=snapshot.snapshot_id, values=_build_backtest_compare_row(snapshot))
            self._update_compare_summary()
        get_backtest_snapshot_store().add_snapshot(result, config, candle_limit, export_path=export_path)
        return snapshot

    def _update_compare_summary(self) -> None:
        count = len(self._backtest_snapshot_order)
        if count == 0:
            self.compare_summary.set("暂无回测对比记录。")
            return
        self.compare_summary.set(f"已保留 {count} 组回测结果。单击任一编号即可联动切换主视图，双击或点“加载所选”也可恢复到主视图。")

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
        if snapshot is not None and is_dynamic_strategy_id(snapshot.config.strategy_id) and snapshot.config.take_profit_mode != "dynamic":
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
                    f"EMA{snapshot.config.ema_period}/{snapshot.config.trend_ema_period} | "
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
            take_profit_label = (
                "动态止盈"
                if current_snapshot.config.take_profit_mode == "dynamic"
                else f"固定止盈 TP x{format_decimal(current_snapshot.config.atr_take_multiplier)}"
            )
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
                f"当前参数单组回测：交易对：{symbol_text} ｜ 周期：{bar_text} ｜ 参数摘要：{param_text} ｜ "
                f"信号方向：{signal_label} ｜ 开始时间：{start_text} ｜ 结束时间：{end_text}。"
                "当前只保留 1 组结果，因此这里展示单组摘要卡；批量回测时会自动生成参数矩阵。"
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
                text=f"挂单EMA\nEMA{entry_reference_ema}",
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
                text=f"编号：{current_snapshot.snapshot_id}",
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
        self._clear_chart_canvas(self.chart_canvas)
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
        signal_label = SIGNAL_VALUE_TO_LABEL.get(snapshot.config.signal_mode, snapshot.config.signal_mode)
        start_text, end_text = _backtest_snapshot_range_text(snapshot)
        summary_text = (
            f"\u7f16\u53f7\uff1a{snapshot.snapshot_id} | \u65f6\u95f4\uff1a{snapshot.created_at.strftime('%Y-%m-%d %H:%M:%S')} | "
            f"\u7b56\u7565\uff1a{_strategy_display_name(snapshot.config)} | "
            f"\u4ea4\u6613\u5bf9\uff1a{snapshot.config.inst_id} | K\u7ebf\uff1a{_normalize_backtest_bar_label(snapshot.config.bar)} | "
            f"M费\uff1a{_format_fee_rate_percent(snapshot.maker_fee_rate)} | T费\uff1a{_format_fee_rate_percent(snapshot.taker_fee_rate)} | "
            f"\u5f00\u59cb\uff1a{start_text} | \u7ed3\u675f\uff1a{end_text} | "
            f"\u4fe1\u53f7\u65b9\u5411\uff1a{signal_label} | \u4ea4\u6613\u6b21\u6570\uff1a{result.report.total_trades}"
        )
        if result.data_source_note:
            summary_text = f"{summary_text}\n{result.data_source_note}"
        export_lines = _backtest_export_detail_lines(snapshot.export_path)
        if export_lines:
            summary_text = f"{summary_text}\n" + "\n".join(export_lines)
        self.report_summary.set(summary_text)
        self.report_text.delete("1.0", END)
        self.report_text.insert("1.0", format_backtest_report(result))
        self._populate_trade_tree(result.trades)
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
        self._schedule_canvas_redraw(canvas, delay_ms=8, fast_mode=True)

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

        plotted_prices = [float(candle.high) for candle in visible_candles] + [float(candle.low) for candle in visible_candles]
        plotted_prices.extend(float(value) for value in visible_ema)
        plotted_prices.extend(float(value) for value in visible_trend_ema)
        plotted_prices.extend(float(value) for value in visible_big_ema)
        for trade in result.trades:
            if trade.exit_index < start_index or trade.entry_index >= end_index:
                continue
            plotted_prices.extend(
                [
                    float(trade.entry_price),
                    float(trade.exit_price),
                    float(trade.stop_loss),
                    float(trade.take_profit),
                ]
            )
        for manual_position in result.manual_positions:
            if manual_position.handoff_index < start_index or manual_position.entry_index >= end_index:
                continue
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
        axis_steps = 2 if fast_mode else 4
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

        for drawdown_value in _chart_price_axis_values(Decimal(str(drawdown_min)), Decimal("0"), steps=2 if fast_mode else 3):
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
                x = x_for(index)
                ema_points.extend((x, y_for(ema_value)))

        trend_ema_points: list[float] = []
        if visible_trend_ema:
            for index, trend_ema_value in enumerate(visible_trend_ema, start=start_index):
                x = x_for(index)
                trend_ema_points.extend((x, y_for(trend_ema_value)))

        big_ema_points: list[float] = []
        if visible_big_ema:
            for index, big_ema_value in enumerate(visible_big_ema, start=start_index):
                x = x_for(index)
                big_ema_points.extend((x, y_for(big_ema_value)))

        net_value_points: list[float] = []
        if visible_net_value:
            for index, net_value in enumerate(visible_net_value, start=start_index):
                x = x_for(index)
                net_value_points.extend((x, y_for_net_value(net_value)))

        drawdown_points: list[float] = []
        if visible_drawdown:
            for index, drawdown_value in enumerate(visible_drawdown, start=start_index):
                x = x_for(index)
                drawdown_points.extend((x, y_for_drawdown(drawdown_value)))

        for index, candle in enumerate(visible_candles, start=start_index):
            x = x_for(index)
            open_y = y_for(candle.open)
            close_y = y_for(candle.close)
            high_y = y_for(candle.high)
            low_y = y_for(candle.low)
            color = _backtest_candle_color(candle.open, candle.close)
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

        for trade in result.trades:
            if trade.exit_index < start_index or trade.entry_index >= end_index:
                continue
            entry_x = x_for(trade.entry_index)
            exit_x = x_for(trade.exit_index)
            entry_y = y_for(trade.entry_price)
            exit_y = y_for(trade.exit_price)
            stop_y = y_for(trade.stop_loss)
            take_y = y_for(trade.take_profit)
            trade_color = "#0969da" if trade.signal == "long" else "#8250df"
            exit_color = "#1a7f37" if trade.exit_reason == "take_profit" else "#d1242f"

            canvas.create_line(entry_x, entry_y, exit_x, exit_y, fill=trade_color, width=2)
            canvas.create_line(entry_x, stop_y, exit_x, stop_y, fill="#d1242f", dash=(4, 2))
            canvas.create_line(entry_x, take_y, exit_x, take_y, fill="#1a7f37", dash=(4, 2))
            canvas.create_oval(entry_x - 4, entry_y - 4, entry_x + 4, entry_y + 4, fill=trade_color, outline="")
            canvas.create_oval(exit_x - 5, exit_y - 5, exit_x + 5, exit_y + 5, fill=exit_color, outline="")
            if not fast_mode:
                canvas.create_text(
                    exit_x + 8,
                    exit_y,
                    text="TP" if trade.exit_reason == "take_profit" else "SL",
                    anchor="w",
                    fill=exit_color,
                )

        selected_manual_position = self._selected_manual_position()
        for manual_position in result.manual_positions:
            if manual_position.handoff_index < start_index or manual_position.entry_index >= end_index:
                continue
            entry_visible = start_index <= manual_position.entry_index < end_index
            handoff_visible = start_index <= manual_position.handoff_index < end_index
            if not entry_visible and not handoff_visible:
                continue
            manual_color = "#9a6700" if manual_position.signal == "long" else "#bc4c00"
            highlight_width = 3 if manual_position == selected_manual_position else 2
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

        time_label_target = 4 if fast_mode else 6
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
                text=f"EMA({result.ema_period})",
                anchor="ne",
                fill="#ff8c00",
                font=("Microsoft YaHei UI", 10, "bold"),
            )
        if len(trend_ema_points) >= 4:
            canvas.create_line(*trend_ema_points, fill="#0a7f5a", width=2, smooth=not fast_mode)
            canvas.create_text(
                width - right,
                top + 30,
                text=f"EMA({result.trend_ema_period})",
                anchor="ne",
                fill="#0a7f5a",
                font=("Microsoft YaHei UI", 10, "bold"),
            )
        if len(big_ema_points) >= 4:
            canvas.create_line(*big_ema_points, fill="#8b5cf6", width=2, smooth=not fast_mode)
            canvas.create_text(
                width - right,
                top + 48,
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
            else Decimal("0")
        )
        trend_ema_value = (
            self._latest_result.trend_ema_values[hover_index]
            if hover_index < len(self._latest_result.trend_ema_values)
            else Decimal("0")
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
            big_ema_value=big_ema_value,
            atr_value=atr_value,
            equity_value=equity_value,
            drawdown_pct_value=drawdown_pct_value,
            ema_period=str(self._latest_result.ema_period),
            trend_ema_period=str(self._latest_result.trend_ema_period),
            big_ema_period=big_ema_period,
            atr_period=str(self._latest_result.atr_period),
            tick_size=self._latest_result.instrument.tick_size,
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

    def _clear_chart_canvas(self, canvas: Canvas) -> None:
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

    def _run_canvas_redraw(self, canvas: Canvas, canvas_id: int, fast_mode: bool) -> None:
        self._chart_canvas_redraw_jobs.pop(canvas_id, None)
        if self._latest_result is None or not canvas.winfo_exists():
            return
        self._draw_chart(self._latest_result, canvas, fast_mode=fast_mode)

    def _redraw_all_charts(self) -> None:
        self._chart_redraw_job = None
        if self._latest_result is None:
            return
        self._draw_chart(self._latest_result, self.chart_canvas)
        if self._chart_zoom_canvas is not None and self._chart_zoom_canvas.winfo_exists():
            self._draw_chart(self._latest_result, self._chart_zoom_canvas)

    def _build_zoom_chart_header_lines(self, snapshot: _BacktestSnapshot | None) -> tuple[str, str]:
        if snapshot is None or snapshot.result is None:
            return (
                "暂无选中回测",
                "运行或切换回测后，这里会显示策略、方向、挂单 EMA、止损、止盈方式、每波开仓次数和关键绩效。",
            )

        config = snapshot.config
        result = snapshot.result
        report = result.report
        strategy_name = _strategy_display_name(config)
        signal_label = SIGNAL_VALUE_TO_LABEL.get(config.signal_mode, config.signal_mode)
        entry_reference_ema = config.resolved_entry_reference_ema_period()
        take_profit_label = "动态止盈" if config.take_profit_mode == "dynamic" else f"固定止盈(TP x{format_decimal(config.atr_take_multiplier)})"
        max_entries_label = "不限(0)" if config.max_entries_per_trend <= 0 else str(config.max_entries_per_trend)
        context_line = (
            f"编号：{snapshot.snapshot_id} | 策略：{strategy_name} | 交易对：{config.inst_id} | "
            f"K线：{_normalize_backtest_bar_label(config.bar)} | 方向：{signal_label}"
        )
        metrics_parts = [
            f"挂单EMA：EMA{entry_reference_ema}",
            f"指标：EMA{result.ema_period} / 趋势EMA{result.trend_ema_period} / ATR{result.atr_period}",
            f"止损：{format_decimal(config.atr_stop_multiplier)} ATR",
            f"止盈：{take_profit_label}",
        ]
        if config.take_profit_mode == "dynamic":
            metrics_parts.append(f"2R保本：{config.dynamic_two_r_break_even_label()}")
            metrics_parts.append(f"手续费偏移：{config.dynamic_fee_offset_enabled_label()}")
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
            if snapshot is None:
                self._chart_zoom_window.title("回测图表大窗")
            else:
                self._chart_zoom_window.title(
                    f"回测图表大窗 | {snapshot.snapshot_id} | {snapshot.config.inst_id} | {_normalize_backtest_bar_label(snapshot.config.bar)}"
                )

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
            text="放大图表：适合全面观察 K 线结构、EMA 轨迹、资金曲线和 TP/SL 触发位置，支持滚轮缩放和拖动平移。",
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
        zoom_canvas.bind("<Configure>", self._schedule_chart_redraw)
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
    ema_value: Decimal,
    trend_ema_value: Decimal,
    big_ema_value: Decimal,
    equity_value: Decimal,
    drawdown_pct_value: Decimal,
    ema_period: str,
    trend_ema_period: str,
    big_ema_period: str,
    tick_size: Decimal,
) -> list[str]:
    return [
        f"时间: {_format_chart_timestamp(candle.ts)}",
        (
            "开/高/低/收: "
            f"{format_decimal(candle.open)} / {format_decimal(candle.high)} / "
            f"{format_decimal(candle.low)} / {format_decimal(candle.close)}"
        ),
        f"EMA({ema_period}): {_format_price_by_tick_size(ema_value, tick_size)}",
        f"EMA({trend_ema_period}): {_format_price_by_tick_size(trend_ema_value, tick_size)}",
        f"EMA({big_ema_period}): {_format_price_by_tick_size(big_ema_value, tick_size)}",
        f"净值曲线: {format_decimal_fixed(equity_value, 2)}",
        f"当前回撤: {format_decimal_fixed(drawdown_pct_value, 2)}%",
    ]


def _format_chart_hover_lines(
    *,
    candle,
    ema_value: Decimal,
    trend_ema_value: Decimal,
    big_ema_value: Decimal | None,
    atr_value: Decimal | None,
    equity_value: Decimal,
    drawdown_pct_value: Decimal,
    ema_period: str,
    trend_ema_period: str,
    big_ema_period: str | None,
    atr_period: str,
    tick_size: Decimal,
) -> list[str]:
    lines = [
        f"时间: {_format_chart_timestamp(candle.ts)}",
        (
            "开/高/低/收: "
            f"{format_decimal(candle.open)} / {format_decimal(candle.high)} / "
            f"{format_decimal(candle.low)} / {format_decimal(candle.close)}"
        ),
        f"EMA({ema_period}): {_format_price_by_tick_size(ema_value, tick_size)}",
        f"EMA({trend_ema_period}): {_format_price_by_tick_size(trend_ema_value, tick_size)}",
    ]
    if atr_value is not None:
        lines.append(f"ATR({atr_period}): {_format_price_by_tick_size(atr_value, tick_size)}")
    if big_ema_value is not None and big_ema_period:
        lines.append(f"EMA({big_ema_period}): {_format_price_by_tick_size(big_ema_value, tick_size)}")
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
