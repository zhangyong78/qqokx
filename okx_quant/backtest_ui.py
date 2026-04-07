from __future__ import annotations

import json
import threading
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from tkinter import BOTH, END, LEFT, RIGHT, BooleanVar, Canvas, StringVar, Text, Toplevel, X, Y
from tkinter import messagebox, ttk

from okx_quant.backtest import (
    ATR_BATCH_MULTIPLIERS,
    ATR_BATCH_TAKE_RATIOS,
    BacktestReport,
    BacktestResult,
    BacktestTrade,
    format_backtest_report,
    run_backtest,
    run_backtest_batch,
)
from okx_quant.backtest_export import export_batch_backtest_report, export_single_backtest_report
from okx_quant.models import StrategyConfig
from okx_quant.okx_client import OkxRestClient
from okx_quant.persistence import backtest_history_file_path
from okx_quant.pricing import format_decimal, format_decimal_fixed
from okx_quant.strategy_catalog import (
    STRATEGY_DEFINITIONS,
    STRATEGY_DYNAMIC_ID,
    STRATEGY_EMA5_EMA8_ID,
    StrategyDefinition,
    get_strategy_definition,
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
STRATEGY_ID_TO_NAME = {item.strategy_id: item.name for item in STRATEGY_DEFINITIONS}
SIGNAL_VALUE_TO_LABEL = {value: label for label, value in SIGNAL_LABEL_TO_VALUE.items()}
BACKTEST_SYMBOL_OPTIONS = (
    "BTC-USDT-SWAP",
    "ETH-USDT-SWAP",
    "SOL-USDT-SWAP",
    "BNB-USDT-SWAP",
    "DOGE-USDT-SWAP",
)
DEFAULT_MAKER_FEE_PERCENT = "0.01"
DEFAULT_TAKER_FEE_PERCENT = "0.028"
BACKTEST_SIZING_OPTIONS = {
    "固定风险金": "fixed_risk",
    "固定数量": "fixed_size",
    "风险百分比": "risk_percent",
}
BACKTEST_SIZING_VALUE_TO_LABEL = {value: label for label, value in BACKTEST_SIZING_OPTIONS.items()}


@dataclass(frozen=True)
class BacktestLaunchState:
    strategy_name: str
    symbol: str
    bar: str
    ema_period: str
    trend_ema_period: str
    big_ema_period: str
    atr_period: str
    stop_atr: str
    take_atr: str
    risk_amount: str
    signal_mode_label: str
    trade_mode_label: str
    position_mode_label: str
    trigger_type_label: str
    environment_label: str
    maker_fee_percent: str = DEFAULT_MAKER_FEE_PERCENT
    taker_fee_percent: str = DEFAULT_TAKER_FEE_PERCENT
    initial_capital: str = "10000"
    sizing_mode_label: str = "固定风险金"
    risk_percent: str = "1"
    compounding_enabled: bool = False
    slippage_percent: str = "0"
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
        temp_path = path.with_suffix(path.suffix + ".tmp")
        temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        temp_path.replace(path)

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


def _build_backtest_symbol_options(current_symbol: str) -> tuple[str, ...]:
    normalized = current_symbol.strip().upper()
    if normalized and normalized not in BACKTEST_SYMBOL_OPTIONS:
        return (normalized,) + BACKTEST_SYMBOL_OPTIONS
    return BACKTEST_SYMBOL_OPTIONS


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
        STRATEGY_ID_TO_NAME.get(config.strategy_id, config.strategy_id),
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
            f"滑点{_format_fee_rate_percent(config.backtest_slippage_rate)} / "
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
        f"滑点{_format_fee_rate_percent(config.backtest_slippage_rate)} / "
        f"资金费{_format_fee_rate_percent(config.backtest_funding_rate)}"
    )


def _build_backtest_compare_detail(snapshot: _BacktestSnapshot) -> str:
    config = snapshot.config
    strategy_name = STRATEGY_ID_TO_NAME.get(config.strategy_id, config.strategy_id)
    start_text, end_text = _backtest_snapshot_range_text(snapshot)
    lines = [
        f"编号：{snapshot.snapshot_id}",
        f"回测时间：{snapshot.created_at.strftime('%Y-%m-%d %H:%M:%S')}",
        f"策略：{strategy_name}",
        f"交易对：{config.inst_id}",
        f"K线周期：{_normalize_backtest_bar_label(config.bar)}",
        f"回测K线数：{snapshot.candle_limit}",
        f"参数：{_build_backtest_param_summary(config, maker_fee_rate=snapshot.maker_fee_rate, taker_fee_rate=snapshot.taker_fee_rate)}",
        f"\u5f00\u59cb\u65f6\u95f4\uff1a{start_text}",
        f"\u7ed3\u675f\u65f6\u95f4\uff1a{end_text}",
        "",
        snapshot.report_text,
    ]
    if snapshot.export_path:
        lines.insert(-2, f"\u62a5\u544a\u6587\u4ef6\uff1a{snapshot.export_path}")
    return "\n".join(lines)


def _format_fee_rate_percent(rate: Decimal) -> str:
    return f"{format_decimal_fixed(rate * Decimal('100'), 4)}%"


def _serialize_strategy_config(config: StrategyConfig) -> dict[str, object]:
    return {
        "inst_id": config.inst_id,
        "bar": config.bar,
        "ema_period": config.ema_period,
        "trend_ema_period": config.trend_ema_period,
        "big_ema_period": config.big_ema_period,
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
        "backtest_initial_capital": str(config.backtest_initial_capital),
        "backtest_sizing_mode": config.backtest_sizing_mode,
        "backtest_risk_percent": None
        if config.backtest_risk_percent is None
        else str(config.backtest_risk_percent),
        "backtest_compounding": config.backtest_compounding,
        "backtest_slippage_rate": str(config.backtest_slippage_rate),
        "backtest_funding_rate": str(config.backtest_funding_rate),
    }


def _deserialize_strategy_config(payload: dict[str, object]) -> StrategyConfig:
    return StrategyConfig(
        inst_id=str(payload.get("inst_id", "")),
        bar=str(payload.get("bar", "15m")),
        ema_period=int(payload.get("ema_period", 21)),
        trend_ema_period=int(payload.get("trend_ema_period", 55)),
        big_ema_period=int(payload.get("big_ema_period", 233)),
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
        backtest_initial_capital=Decimal(str(payload.get("backtest_initial_capital", "10000"))),
        backtest_sizing_mode=str(payload.get("backtest_sizing_mode", "fixed_risk")),
        backtest_risk_percent=None
        if payload.get("backtest_risk_percent") in (None, "")
        else Decimal(str(payload.get("backtest_risk_percent"))),
        backtest_compounding=bool(payload.get("backtest_compounding", False)),
        backtest_slippage_rate=Decimal(str(payload.get("backtest_slippage_rate", "0"))),
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
        apply_adaptive_window_geometry(
            self.window,
            width_ratio=0.8,
            height_ratio=0.8,
            min_width=1100,
            min_height=760,
            max_width=1580,
            max_height=1080,
        )

        self._strategy_name_to_id = {item.name: item.strategy_id for item in STRATEGY_DEFINITIONS}

        self.strategy_name = StringVar(value=initial_state.strategy_name)
        self.symbol = StringVar(value=initial_state.symbol)
        self.bar_label = StringVar(value=_normalize_backtest_bar_label(initial_state.bar))
        self.ema_period = StringVar(value=initial_state.ema_period)
        self.trend_ema_period = StringVar(value=initial_state.trend_ema_period)
        self.big_ema_period = StringVar(value=initial_state.big_ema_period)
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
        self.slippage_percent = StringVar(value=initial_state.slippage_percent)
        self.funding_rate_percent = StringVar(value=initial_state.funding_rate_percent)
        self.start_time_text = StringVar(value=initial_state.start_time_text)
        self.end_time_text = StringVar(value=initial_state.end_time_text)
        self.signal_mode_label = StringVar(value=initial_state.signal_mode_label)
        self.trade_mode_label = StringVar(value=initial_state.trade_mode_label)
        self.position_mode_label = StringVar(value=initial_state.position_mode_label)
        self.trigger_type_label = StringVar(value=initial_state.trigger_type_label)
        self.environment_label = StringVar(value=initial_state.environment_label)
        self.candle_limit = StringVar(value=initial_state.candle_limit)
        self.report_summary = StringVar(value="点击“开始回测”后，会在这里显示报告摘要。")
        self.compare_summary = StringVar(value="暂无回测对比记录。")
        self.matrix_summary = StringVar(value="\u6682\u65e0 ATR \u6279\u91cf\u56de\u6d4b\u77e9\u9635\u3002")
        self.heatmap_summary = StringVar(
            value="\u53c2\u6570\u70ed\u529b\u56fe\u4f1a\u5728\u8fd9\u91cc\u663e\u793a\uff0c\u53ef\u5207\u6362\u6307\u6807\u5e76\u5355\u51fb\u5355\u5143\u683c\u8054\u52a8\u56de\u6d4b\u89c6\u56fe\u3002"
        )
        self.heatmap_metric = StringVar(value="总盈亏")
        self._latest_result: BacktestResult | None = None
        self._chart_zoom_window: Toplevel | None = None
        self._chart_zoom_canvas: Canvas | None = None
        self._chart_redraw_job: str | None = None
        self._chart_canvas_redraw_jobs: dict[int, str] = {}
        self._main_chart_view = _ChartViewport()
        self._zoom_chart_view = _ChartViewport()
        self._chart_render_states: dict[int, _ChartRenderState] = {}
        self._chart_hover_indices: dict[int, int | None] = {}
        self._backtest_snapshots: dict[str, _BacktestSnapshot] = {}
        self._backtest_snapshot_order: list[str] = []
        self._backtest_snapshot_sequence = 0
        self._current_snapshot_id: str | None = None
        self._backtest_running = False
        self._batch_sequence = 0
        self._batch_snapshot_groups: dict[str, list[str]] = {}
        self._snapshot_batch_labels: dict[str, str] = {}
        self._current_matrix_batch_label: str | None = None
        self._content_pane: ttk.Panedwindow | None = None
        self._report_pane: ttk.Panedwindow | None = None

        self._build_layout()
        self._apply_selected_strategy_definition()
        self._update_sizing_mode_widgets()
        self.window.after_idle(self._apply_initial_layout_preferences)

    def _build_layout(self) -> None:
        self.window.columnconfigure(0, weight=1)
        self.window.rowconfigure(1, weight=1)

        controls = ttk.LabelFrame(self.window, text="回测参数", padding=16)
        controls.grid(row=0, column=0, sticky="ew", padx=16, pady=(16, 8))
        for column in range(6):
            controls.columnconfigure(column, weight=1)

        row = 0
        ttk.Label(controls, text="策略").grid(row=row, column=0, sticky="w")
        strategy_combo = ttk.Combobox(
            controls,
            textvariable=self.strategy_name,
            values=[item.name for item in STRATEGY_DEFINITIONS],
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
        ttk.Label(controls, text="K线周期").grid(row=row, column=4, sticky="w")
        ttk.Combobox(
            controls,
            textvariable=self.bar_label,
            values=list(BACKTEST_BAR_LABEL_TO_VALUE.keys()),
            state="readonly",
        ).grid(row=row, column=5, sticky="ew")

        row += 1
        ttk.Label(controls, text="EMA小周期").grid(row=row, column=0, sticky="w", pady=(12, 0))
        ttk.Entry(controls, textvariable=self.ema_period).grid(
            row=row, column=1, sticky="ew", padx=(0, 12), pady=(12, 0)
        )
        ttk.Label(controls, text="EMA中周期").grid(row=row, column=2, sticky="w", pady=(12, 0))
        ttk.Entry(controls, textvariable=self.trend_ema_period).grid(
            row=row, column=3, sticky="ew", padx=(0, 12), pady=(12, 0)
        )
        ttk.Label(controls, text="EMA大周期").grid(row=row, column=4, sticky="w", pady=(12, 0))
        ttk.Entry(controls, textvariable=self.big_ema_period).grid(row=row, column=5, sticky="ew", pady=(12, 0))

        row += 1
        ttk.Label(controls, text="ATR 周期").grid(row=row, column=0, sticky="w", pady=(12, 0))
        ttk.Entry(controls, textvariable=self.atr_period).grid(
            row=row, column=1, sticky="ew", padx=(0, 12), pady=(12, 0)
        )
        ttk.Label(controls, text="止损 ATR 倍数").grid(row=row, column=2, sticky="w", pady=(12, 0))
        ttk.Entry(controls, textvariable=self.stop_atr).grid(
            row=row, column=3, sticky="ew", padx=(0, 12), pady=(12, 0)
        )
        ttk.Label(controls, text="止盈 ATR 倍数").grid(row=row, column=4, sticky="w", pady=(12, 0))
        ttk.Entry(controls, textvariable=self.take_atr).grid(row=row, column=5, sticky="ew", pady=(12, 0))

        row += 1
        ttk.Label(controls, text="信号方向").grid(row=row, column=0, sticky="w", pady=(12, 0))
        self.signal_combo = ttk.Combobox(controls, textvariable=self.signal_mode_label, state="readonly")
        self.signal_combo.grid(row=row, column=1, sticky="ew", padx=(0, 12), pady=(12, 0))

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
        ttk.Label(controls, text="滑点(%)").grid(row=row, column=0, sticky="w", pady=(12, 0))
        ttk.Entry(controls, textvariable=self.slippage_percent).grid(row=row, column=1, sticky="ew", padx=(0, 12), pady=(12, 0))
        ttk.Label(controls, text="资金费率/8h(%)").grid(row=row, column=2, sticky="w", pady=(12, 0))
        ttk.Entry(controls, textvariable=self.funding_rate_percent).grid(
            row=row, column=3, sticky="ew", padx=(0, 12), pady=(12, 0)
        )
        ttk.Checkbutton(controls, text="启用复利", variable=self.compounding_enabled).grid(
            row=row, column=4, columnspan=2, sticky="w", pady=(12, 0)
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
            text="当前回测区间最多支持 10000 根已收盘 K 线",
        ).grid(row=row, column=2, columnspan=2, sticky="w", pady=(12, 0))
        ttk.Button(controls, text="开始回测", command=self.start_backtest).grid(row=row, column=4, columnspan=2, sticky="e", pady=(12, 0))

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
            text="回测取数已支持 10000 根区间 K 线，并会自动补足前置预热数据、优先使用本地历史缓存。",
        ).grid(row=1, column=0, columnspan=2, sticky="w", pady=(6, 0))
        self.batch_backtest_button = None

        content_pane = ttk.Panedwindow(self.window, orient="vertical")
        content_pane.grid(row=1, column=0, sticky="nsew", padx=16, pady=(0, 16))
        self._content_pane = content_pane

        report_frame = ttk.Panedwindow(content_pane, orient="horizontal")
        self._report_pane = report_frame

        summary_frame = ttk.LabelFrame(report_frame, text="回测报告", padding=12)
        trades_frame = ttk.LabelFrame(report_frame, text="交易明细", padding=12)
        report_frame.add(summary_frame, weight=1)
        report_frame.add(trades_frame, weight=1)
        content_pane.add(report_frame, weight=2)

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
        matrix_tab.rowconfigure(1, weight=1)
        ttk.Label(
            matrix_tab,
            textvariable=self.matrix_summary,
            wraplength=480,
            justify="left",
        ).grid(row=0, column=0, sticky="w", pady=(0, 10))
        self.matrix_grid_frame = ttk.Frame(matrix_tab)
        self.matrix_grid_frame.grid(row=1, column=0, sticky="nsew")
        report_notebook.add(matrix_tab, text="\u77e9\u9635\u5bf9\u6bd4")

        heatmap_tab = ttk.Frame(report_notebook, padding=8)
        heatmap_tab.columnconfigure(0, weight=1)
        heatmap_tab.rowconfigure(2, weight=1)
        heatmap_toolbar = ttk.Frame(heatmap_tab)
        heatmap_toolbar.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        heatmap_toolbar.columnconfigure(2, weight=1)
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
        ttk.Label(
            heatmap_toolbar,
            text="单击单元格可切换到对应回测。",
            foreground="#57606a",
        ).grid(row=0, column=2, sticky="e", padx=(12, 0))
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

        trade_tree_frame = ttk.Frame(trades_frame)
        trade_tree_frame.grid(row=0, column=0, sticky="nsew")
        trade_tree_frame.columnconfigure(0, weight=1)
        trade_tree_frame.rowconfigure(0, weight=1)

        self.trade_tree = ttk.Treeview(
            trade_tree_frame,
            columns=("signal", "entry_time", "entry", "exit_time", "exit", "reason", "pnl", "r"),
            show="headings",
            selectmode="browse",
        )
        self.trade_tree.heading("signal", text="方向")
        self.trade_tree.heading("entry_time", text="进场时间")
        self.trade_tree.heading("entry", text="进场价格")
        self.trade_tree.heading("exit_time", text="出场时间")
        self.trade_tree.heading("exit", text="出场价格")
        self.trade_tree.heading("reason", text="原因")
        self.trade_tree.heading("pnl", text="盈亏")
        self.trade_tree.heading("r", text="R倍数")
        self.trade_tree.column("signal", width=70, anchor="center")
        self.trade_tree.column("entry_time", width=140, anchor="center")
        self.trade_tree.column("entry", width=110, anchor="e")
        self.trade_tree.column("exit_time", width=140, anchor="center")
        self.trade_tree.column("exit", width=110, anchor="e")
        self.trade_tree.column("reason", width=90, anchor="center")
        self.trade_tree.column("pnl", width=110, anchor="e")
        self.trade_tree.column("r", width=90, anchor="e")
        self.trade_tree.grid(row=0, column=0, sticky="nsew")
        trade_tree_scroll_y = ttk.Scrollbar(trade_tree_frame, orient="vertical", command=self.trade_tree.yview)
        self.trade_tree.configure(yscrollcommand=trade_tree_scroll_y.set)
        trade_tree_scroll_y.grid(row=0, column=1, sticky="ns")

        self.chart_frame = ttk.LabelFrame(content_pane, text="K线图、资金曲线与止盈止损触发位置 | 暂无选中回测", padding=12)
        self.chart_frame.columnconfigure(0, weight=1)
        self.chart_frame.rowconfigure(1, weight=1)
        content_pane.add(self.chart_frame, weight=3)

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

    def _apply_initial_layout_preferences(self) -> None:
        self.window.update_idletasks()
        window_height = max(self.window.winfo_height(), self.window.winfo_reqheight())
        window_width = max(self.window.winfo_width(), self.window.winfo_reqwidth())

        if self._content_pane is not None and len(self._content_pane.panes()) >= 2:
            content_height = max(self._content_pane.winfo_height(), window_height - 260)
            top_ratio = 0.34 if window_height < 920 else 0.38
            report_height = max(240, min(int(content_height * top_ratio), content_height - 300))
            try:
                self._content_pane.sashpos(0, report_height)
            except Exception:
                pass

        if self._report_pane is not None and len(self._report_pane.panes()) >= 2:
            report_width = max(self._report_pane.winfo_width(), window_width - 80)
            left_ratio = 0.42 if window_width < 1400 else 0.45
            summary_width = max(420, min(int(report_width * left_ratio), report_width - 520))
            try:
                self._report_pane.sashpos(0, summary_width)
            except Exception:
                pass

    def _update_sizing_mode_widgets(self) -> None:
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

        self.report_summary.set("正在回测中，请稍候...")
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
            self.trade_tree.insert(
                "",
                END,
                iid=f"T{index:03d}",
                values=(
                    "做多" if trade.signal == "long" else "做空",
                    _format_chart_timestamp(trade.entry_ts),
                    format_decimal_fixed(trade.entry_price, 4),
                    _format_chart_timestamp(trade.exit_ts),
                    format_decimal_fixed(trade.exit_price, 4),
                    "止盈" if trade.exit_reason == "take_profit" else "止损",
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
        if self._backtest_running:
            return
        if self._selected_strategy_definition().strategy_id == STRATEGY_EMA5_EMA8_ID:
            self.start_single_backtest()
            return
        try:
            config, candle_limit, maker_fee_rate, taker_fee_rate, start_ts, end_ts = self._build_backtest_request()
        except Exception as exc:
            messagebox.showerror("回测参数错误", str(exc), parent=self.window)
            return

        self._prepare_backtest_output("\u6b63\u5728\u6279\u91cf\u56de\u6d4b 9 \u7ec4 ATR \u53c2\u6570\uff0c\u8bf7\u7a0d\u5019...")
        self._set_backtest_running(True)
        batch_label = self._next_batch_label()
        threading.Thread(
            target=self._run_batch_backtest_worker,
            args=(config, candle_limit, batch_label, maker_fee_rate, taker_fee_rate, start_ts, end_ts),
            daemon=True,
        ).start()

    def start_single_backtest(self) -> None:
        if self._backtest_running:
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
            self._parse_positive_int(self.candle_limit.get(), "回测K线数"),
            self._parse_fee_percent(self.maker_fee_percent.get(), "Maker手续费"),
            self._parse_fee_percent(self.taker_fee_percent.get(), "Taker手续费"),
            start_ts,
            end_ts,
        )

    def _prepare_backtest_output(self, summary_text: str) -> None:
        self.report_summary.set(summary_text)
        self.report_text.delete("1.0", END)
        self.trade_tree.delete(*self.trade_tree.get_children())
        self._reset_chart_views()
        self._clear_chart_canvas(self.chart_canvas)
        self._set_chart_title("K线图、资金曲线与止盈止损触发位置 | 正在准备回测")
        if self._chart_zoom_canvas is not None and self._chart_zoom_canvas.winfo_exists():
            self._clear_chart_canvas(self._chart_zoom_canvas)

    def _set_chart_title(self, text: str) -> None:
        if getattr(self, "chart_frame", None) is not None:
            self.chart_frame.configure(text=text)

    def _build_chart_title_for_snapshot(self, snapshot: _BacktestSnapshot) -> str:
        signal_label = SIGNAL_VALUE_TO_LABEL.get(snapshot.config.signal_mode, snapshot.config.signal_mode)
        return (
            "K线图、资金曲线与止盈止损触发位置 | "
            f"{snapshot.snapshot_id} | "
            f"{STRATEGY_ID_TO_NAME.get(snapshot.config.strategy_id, snapshot.config.strategy_id)} | "
            f"{snapshot.config.inst_id} | "
            f"{_normalize_backtest_bar_label(snapshot.config.bar)} | "
            f"{signal_label} | M费{_format_fee_rate_percent(snapshot.maker_fee_rate)} | "
            f"T费{_format_fee_rate_percent(snapshot.taker_fee_rate)}"
        )

    def _set_backtest_running(self, running: bool) -> None:
        self._backtest_running = running
        state = "disabled" if running else "normal"
        if getattr(self, "single_backtest_button", None) is not None:
            self.single_backtest_button.configure(state=state)
        if getattr(self, "batch_backtest_button", None) is not None:
            self.batch_backtest_button.configure(state=state)

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
            self.window.after(0, lambda: self._apply_batch_backtest_results(results, candle_limit, batch_label))
        except Exception as exc:
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
            self.window.after(0, lambda: self._apply_backtest_result(result, config, candle_limit))
        except Exception as exc:
            self.window.after(0, lambda error=exc: self._show_backtest_error(error))

    def _apply_backtest_result(self, result: BacktestResult, config: StrategyConfig, candle_limit: int) -> None:
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
        self.report_summary.set("回测失败")
        self._set_backtest_running(False)
        messagebox.showerror("回测失败", str(exc), parent=self.window)

    def _build_config(self) -> StrategyConfig:
        definition = self._selected_strategy_definition()
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
        return StrategyConfig(
            inst_id=self.symbol.get().strip().upper(),
            bar="4H" if definition.strategy_id == STRATEGY_EMA5_EMA8_ID else _backtest_bar_value_from_label(self.bar_label.get()),
            ema_period=5 if definition.strategy_id == STRATEGY_EMA5_EMA8_ID else self._parse_positive_int(self.ema_period.get(), "EMA小周期"),
            trend_ema_period=8 if definition.strategy_id == STRATEGY_EMA5_EMA8_ID else self._parse_positive_int(self.trend_ema_period.get(), "EMA中周期"),
            big_ema_period=233 if definition.strategy_id == STRATEGY_EMA5_EMA8_ID else self._parse_positive_int(self.big_ema_period.get(), "EMA大周期"),
            atr_period=self._parse_positive_int(self.atr_period.get(), "ATR 周期"),
            atr_stop_multiplier=self._parse_positive_decimal(self.stop_atr.get(), "止损 ATR 倍数"),
            atr_take_multiplier=self._parse_positive_decimal(self.take_atr.get(), "止盈 ATR 倍数"),
            order_size=order_size,
            trade_mode=TRADE_MODE_OPTIONS[self.trade_mode_label.get()],
            signal_mode=SIGNAL_LABEL_TO_VALUE[self.signal_mode_label.get()],
            position_mode=POSITION_MODE_OPTIONS[self.position_mode_label.get()],
            environment=ENV_OPTIONS[self.environment_label.get()],
            tp_sl_trigger_type=TRIGGER_TYPE_OPTIONS[self.trigger_type_label.get()],
            strategy_id=definition.strategy_id,
            risk_amount=risk_amount,
            backtest_initial_capital=self._parse_positive_decimal(self.initial_capital.get(), "初始资金"),
            backtest_sizing_mode=sizing_mode,
            backtest_risk_percent=risk_percent,
            backtest_compounding=bool(self.compounding_enabled.get()),
            backtest_slippage_rate=self._parse_nonnegative_decimal(self.slippage_percent.get(), "滑点") / Decimal("100"),
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
        self.signal_combo["values"] = definition.allowed_signal_labels
        if self.signal_mode_label.get() not in definition.allowed_signal_labels:
            self.signal_mode_label.set(definition.default_signal_label)
        if definition.strategy_id == STRATEGY_EMA5_EMA8_ID:
            self.bar_label.set("4小时")
            self.ema_period.set("5")
            self.trend_ema_period.set("8")
            self.big_ema_period.set("233")
            self.risk_amount.set("100")

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
        self._show_batch_matrix(self._snapshot_batch_labels.get(snapshot_id))

    def _show_batch_matrix(self, batch_label: str | None) -> None:
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
        self.compare_tree.delete(*self.compare_tree.get_children())
        self.compare_detail_text.delete("1.0", END)
        self._update_compare_summary()
        self._show_batch_matrix(None)
        self._populate_period_stats(self.monthly_stats_tree, [])
        self._populate_period_stats(self.yearly_stats_tree, [])
        self._set_chart_title("K线图、资金曲线与止盈止损触发位置 | 暂无选中回测")

    def _load_snapshot(self, snapshot_id: str) -> None:
        snapshot = self._backtest_snapshots[snapshot_id]
        result = snapshot.result
        self._current_snapshot_id = snapshot_id
        self._latest_result = result
        self._reset_chart_views()
        self._set_chart_title(self._build_chart_title_for_snapshot(snapshot))
        signal_label = SIGNAL_VALUE_TO_LABEL.get(snapshot.config.signal_mode, snapshot.config.signal_mode)
        start_text, end_text = _backtest_snapshot_range_text(snapshot)
        summary_text = (
            f"\u7f16\u53f7\uff1a{snapshot.snapshot_id} | \u65f6\u95f4\uff1a{snapshot.created_at.strftime('%Y-%m-%d %H:%M:%S')} | "
            f"\u7b56\u7565\uff1a{STRATEGY_ID_TO_NAME.get(snapshot.config.strategy_id, snapshot.config.strategy_id)} | "
            f"\u4ea4\u6613\u5bf9\uff1a{snapshot.config.inst_id} | K\u7ebf\uff1a{_normalize_backtest_bar_label(snapshot.config.bar)} | "
            f"M费\uff1a{_format_fee_rate_percent(snapshot.maker_fee_rate)} | T费\uff1a{_format_fee_rate_percent(snapshot.taker_fee_rate)} | "
            f"\u5f00\u59cb\uff1a{start_text} | \u7ed3\u675f\uff1a{end_text} | "
            f"\u4fe1\u53f7\u65b9\u5411\uff1a{signal_label} | \u4ea4\u6613\u6b21\u6570\uff1a{result.report.total_trades}"
        )
        if result.data_source_note:
            summary_text = f"{summary_text}\n{result.data_source_note}"
        if snapshot.export_path:
            summary_text = f"{summary_text}\n报告文件：{snapshot.export_path}"
        self.report_summary.set(summary_text)
        self.report_text.delete("1.0", END)
        self.report_text.insert("1.0", format_backtest_report(result))
        self.trade_tree.delete(*self.trade_tree.get_children())
        for index, trade in enumerate(result.trades, start=1):
            self.trade_tree.insert(
                "",
                END,
                iid=f"T{index:03d}",
                values=(
                    "\u505a\u591a" if trade.signal == "long" else "\u505a\u7a7a",
                    _format_chart_timestamp(trade.entry_ts),
                    format_decimal_fixed(trade.entry_price, 4),
                    _format_chart_timestamp(trade.exit_ts),
                    format_decimal_fixed(trade.exit_price, 4),
                    "\u6b62\u76c8" if trade.exit_reason == "take_profit" else "\u6b62\u635f",
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
        visible_big_ema = result.big_ema_values[start_index:end_index]
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
        big_ema_value = (
            self._latest_result.big_ema_values[hover_index]
            if hover_index < len(self._latest_result.big_ema_values)
            else Decimal("0")
        )
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
            equity_value=equity_value,
            drawdown_pct_value=drawdown_pct_value,
            ema_period=str(self._latest_result.ema_period),
            trend_ema_period=str(self._latest_result.trend_ema_period),
            big_ema_period=str(self._latest_result.big_ema_period),
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

    def open_chart_zoom_window(self) -> None:
        if self._chart_zoom_window is not None and self._chart_zoom_window.winfo_exists():
            self._chart_zoom_window.deiconify()
            self._chart_zoom_window.lift()
            self._chart_zoom_window.focus_force()
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
        ttk.Label(
            toolbar,
            text="放大图表：适合全面观察 K 线结构、EMA 轨迹、资金曲线和 TP/SL 触发位置，支持滚轮缩放和拖动平移。",
        ).grid(row=0, column=0, sticky="w")
        ttk.Button(toolbar, text="重置视图", command=self.reset_zoom_chart_view).grid(row=0, column=1, sticky="e", padx=(0, 8))
        ttk.Button(toolbar, text="关闭", command=self._close_chart_zoom_window).grid(row=0, column=2, sticky="e")

        zoom_canvas = Canvas(zoom_window, background="#ffffff", highlightthickness=0)
        zoom_canvas.grid(row=1, column=0, sticky="nsew", padx=12, pady=12)
        zoom_canvas.bind("<Configure>", self._schedule_chart_redraw)
        self._bind_chart_interactions(zoom_canvas)

        self._chart_zoom_window = zoom_window
        self._chart_zoom_canvas = zoom_canvas
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
