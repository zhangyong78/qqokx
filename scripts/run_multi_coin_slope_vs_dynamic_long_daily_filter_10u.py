from __future__ import annotations

import base64
import html
import io
import json
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from shutil import copyfile

import matplotlib.pyplot as plt
import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from okx_quant.backtest import (
    BacktestTrade,
    _apply_slippage_price,
    _backtest_trade_start_index,
    _build_closed_trade,
    _create_open_position,
    _determine_backtest_order_size,
    _direction_filter_allows_signal,
    _run_backtest_with_loaded_data,
    _try_close_position,
    build_protection_plan,
)
from okx_quant.candle_cache import load_candle_cache
from okx_quant.indicators import atr, moving_average
from okx_quant.models import StrategyConfig
from okx_quant.okx_client import OkxRestClient
from okx_quant.persistence import analysis_report_dir_path
from okx_quant.pricing import format_decimal_fixed, snap_to_increment
from okx_quant.strategy_catalog import STRATEGY_DYNAMIC_LONG_ID
from scripts.run_btc_daily_ma_direction_filter_research import (
    ENTRY_BAR,
    FILTER_BAR,
    INITIAL_CAPITAL,
    LONG_MAKER_FEE_RATE,
    LONG_TAKER_FEE_RATE,
    RISK_AMOUNT,
    SplitMetrics,
    build_daily_direction_bias,
    build_metrics,
    build_split_bounds,
    filter_split_trades,
    format_ts,
)
from scripts.run_multi_coin_best_long_daily_gate_report import GATES, LONG_PROFILES, SYMBOL_LABELS


plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "Noto Sans CJK SC", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False


REPORT_DIR = analysis_report_dir_path()
REPORT_DIR.mkdir(parents=True, exist_ok=True)
PROJECT_REPORT_DIR = ROOT / "reports"
PROJECT_REPORT_DIR.mkdir(parents=True, exist_ok=True)

STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
REPORT_BASENAME = f"multi_coin_slope_vs_dynamic_long_daily_filter_full_10u_{STAMP}"
HTML_PATH = REPORT_DIR / f"{REPORT_BASENAME}.html"
CSV_PATH = REPORT_DIR / f"{REPORT_BASENAME}.csv"
JSON_PATH = REPORT_DIR / f"{REPORT_BASENAME}.json"
PROJECT_HTML_PATH = PROJECT_REPORT_DIR / "multi_coin_slope_vs_dynamic_long_daily_filter_full_10u.html"

SYMBOLS = tuple(LONG_PROFILES.keys())
ENTRY_LIMIT: int | None = None


@dataclass(frozen=True)
class GateRun:
    gate_key: str
    gate_label: str
    trades: list[BacktestTrade]
    test_trades: list[BacktestTrade]
    all_metrics: SplitMetrics
    test_metrics: SplitMetrics


@dataclass(frozen=True)
class CoinStudy:
    symbol: str
    label: str
    entry_count: int
    filter_count: int
    start_ts: int
    end_ts: int
    gate_runs: dict[str, GateRun]


@dataclass(frozen=True)
class ScenarioResult:
    key: str
    label: str
    description: str
    selected_gates: dict[str, str]
    trades: list[BacktestTrade]
    test_trades: list[BacktestTrade]
    all_metrics: SplitMetrics
    test_metrics: SplitMetrics


@dataclass(frozen=True)
class StrategyStudy:
    key: str
    label: str
    config_note: str
    studies: list[CoinStudy]
    scenarios: list[ScenarioResult]
    gate_frame: pd.DataFrame


def build_dynamic_long_config(symbol: str) -> StrategyConfig:
    profile = LONG_PROFILES[symbol]
    return StrategyConfig(
        inst_id=symbol,
        bar=ENTRY_BAR,
        ema_period=profile.ema_period,
        trend_ema_period=profile.trend_ema_period,
        big_ema_period=233,
        atr_period=10,
        atr_stop_multiplier=profile.atr_stop_multiplier,
        atr_take_multiplier=profile.atr_stop_multiplier * Decimal("2"),
        order_size=Decimal("0"),
        trade_mode="cross",
        signal_mode="long_only",
        position_mode="net",
        environment="demo",
        tp_sl_trigger_type="mark",
        strategy_id=STRATEGY_DYNAMIC_LONG_ID,
        risk_amount=RISK_AMOUNT,
        backtest_initial_capital=INITIAL_CAPITAL,
        backtest_sizing_mode="fixed_risk",
        entry_reference_ema_period=profile.entry_reference_ema_period,
        take_profit_mode="dynamic",
        max_entries_per_trend=1,
        dynamic_two_r_break_even=True,
        dynamic_fee_offset_enabled=True,
        trend_ema_slope_filter_enabled=False,
        time_stop_break_even_enabled=False,
        time_stop_break_even_bars=0,
    )


def build_slope_long_config(symbol: str) -> StrategyConfig:
    return StrategyConfig(
        inst_id=symbol,
        bar=ENTRY_BAR,
        ema_period=55,
        ema_type="ema",
        trend_ema_period=55,
        trend_ema_type="ema",
        big_ema_period=233,
        atr_period=14,
        atr_stop_multiplier=Decimal("2"),
        atr_take_multiplier=Decimal("4"),
        order_size=Decimal("0"),
        trade_mode="cross",
        signal_mode="long_only",
        position_mode="net",
        environment="demo",
        tp_sl_trigger_type="mark",
        strategy_id="ema55_slope_long_mirror_research",
        risk_amount=RISK_AMOUNT,
        take_profit_mode="dynamic",
        max_entries_per_trend=1,
        entry_reference_ema_period=55,
        entry_reference_ema_type="ema",
        dynamic_two_r_break_even=True,
        dynamic_fee_offset_enabled=True,
        ema55_slope_exit_enabled=True,
        trend_ema_slope_filter_enabled=True,
        trend_ema_slope_filter_lookback_bars=1,
        trend_ema_slope_filter_min_ratio=Decimal("0.0005"),
        time_stop_break_even_enabled=False,
        time_stop_break_even_bars=10,
        backtest_initial_capital=INITIAL_CAPITAL,
        backtest_sizing_mode="fixed_risk",
        backtest_compounding=False,
        backtest_entry_slippage_rate=Decimal("0"),
        backtest_exit_slippage_rate=Decimal("0"),
        backtest_funding_rate=Decimal("0"),
    )


def build_data_note(symbol: str, entry_count: int, filter_count: int) -> str:
    return (
        f"local candle_cache full history | {symbol} {ENTRY_BAR} candles={entry_count} | "
        f"{FILTER_BAR} candles={filter_count}"
    )


def entry_limit_label() -> str:
    if ENTRY_LIMIT is None:
        return "全历史"
    return f"最多 {ENTRY_LIMIT} 根"


def run_slope_long_with_bias(
    candles: list,
    instrument: object,
    config: StrategyConfig,
    direction_filter_bias: list[str] | None = None,
) -> list[BacktestTrade]:
    minimum = max(int(config.ema_period), int(config.trend_ema_period), int(config.atr_period), 2) + 1
    if len(candles) < minimum:
        raise RuntimeError(f"not enough candles for {config.inst_id} slope long")

    trade_start_index = _backtest_trade_start_index(minimum)
    closes = [candle.close for candle in candles]
    ema_values = moving_average(closes, int(config.ema_period), "ema")
    atr_values = atr(candles, int(config.atr_period))
    trades: list[BacktestTrade] = []
    open_position = None
    entry_threshold_ratio = Decimal(str(config.trend_ema_slope_filter_min_ratio))
    dynamic_take_profit_enabled = config.take_profit_mode == "dynamic"

    for index in range(trade_start_index, len(candles)):
        candle = candles[index]
        current_ema = ema_values[index]
        previous_ema = ema_values[index - 1] if index > 0 else None
        atr_value = atr_values[index] if index < len(atr_values) else None
        if current_ema is None or previous_ema is None or atr_value is None or atr_value <= 0:
            continue

        slope = current_ema - previous_ema
        slope_ratio = slope / current_ema if current_ema != 0 else None

        if open_position is not None:
            closed_trade = _try_close_position(
                open_position,
                candle,
                index,
                exit_fee_rate=LONG_TAKER_FEE_RATE,
                exit_fee_type="taker",
            )
            if closed_trade is not None:
                trades.append(closed_trade)
                open_position = None

        if open_position is not None and slope < 0:
            exit_price_raw = snap_to_increment(candle.close, instrument.tick_size, "nearest")
            exit_price = _apply_slippage_price(
                exit_price_raw,
                signal=open_position.signal,
                tick_size=open_position.tick_size,
                slippage_rate=open_position.exit_slippage_rate,
                is_entry=False,
            )
            trades.append(
                _build_closed_trade(
                    open_position,
                    candle,
                    index,
                    exit_price_raw=exit_price_raw,
                    exit_price=exit_price,
                    exit_reason="slope_turn_negative",
                    exit_fee_rate=LONG_TAKER_FEE_RATE,
                    exit_fee_type="taker",
                )
            )
            open_position = None

        if open_position is not None or slope_ratio is None or slope_ratio < entry_threshold_ratio:
            continue

        if direction_filter_bias is not None and index < len(direction_filter_bias):
            if not _direction_filter_allows_signal(direction_filter_bias[index], "long"):
                continue

        protection = build_protection_plan(
            instrument=instrument,
            config=config,
            direction="long",
            entry_reference=candle.close,
            atr_value=atr_value,
            candle_ts=candle.ts,
            trigger_inst_id=instrument.inst_id,
        )
        size = _determine_backtest_order_size(
            instrument=instrument,
            config=config,
            entry_price=protection.entry_reference,
            stop_loss=protection.stop_loss,
            risk_price_compatible=bool(config.risk_amount is not None and config.risk_amount > 0),
        )
        open_position = _create_open_position(
            instrument=instrument,
            signal="long",
            entry_index=index,
            entry_ts=candle.ts,
            entry_price_raw=protection.entry_reference,
            stop_loss=protection.stop_loss,
            take_profit=protection.take_profit,
            atr_value=protection.atr_value,
            size=size,
            entry_fee_rate=LONG_TAKER_FEE_RATE,
            exit_fee_rate=LONG_TAKER_FEE_RATE,
            entry_fee_type="taker",
            entry_slippage_rate=config.resolved_backtest_entry_slippage_rate(),
            exit_slippage_rate=config.resolved_backtest_exit_slippage_rate(),
            funding_rate=config.backtest_funding_rate,
            dynamic_take_profit_enabled=dynamic_take_profit_enabled,
            dynamic_exit_fee_rate=LONG_TAKER_FEE_RATE,
            dynamic_two_r_break_even=config.dynamic_two_r_break_even,
            dynamic_fee_offset_enabled=config.dynamic_fee_offset_enabled,
            time_stop_break_even_enabled=config.time_stop_break_even_enabled,
            time_stop_break_even_bars=config.resolved_time_stop_break_even_bars(),
            apply_entry_slippage=True,
        )

    return trades


def run_coin_study(client: OkxRestClient, symbol: str, strategy_key: str) -> CoinStudy:
    entry_candles = [c for c in load_candle_cache(symbol, ENTRY_BAR, limit=ENTRY_LIMIT) if c.confirmed]
    filter_candles = [c for c in load_candle_cache(symbol, FILTER_BAR, limit=None) if c.confirmed]
    if not entry_candles or not filter_candles:
        raise RuntimeError(f"missing local candles for {symbol} {ENTRY_BAR}/{FILTER_BAR}")

    instrument = client.get_instrument(symbol)
    test_bounds = build_split_bounds(len(entry_candles))["test"]

    bias_map: dict[str, list[str] | None] = {"none": None}
    for gate in GATES:
        if gate.key == "none":
            continue
        bias_map[gate.key] = build_daily_direction_bias(entry_candles, filter_candles, gate)

    gate_runs: dict[str, GateRun] = {}
    for gate in GATES:
        print(f"run {SYMBOL_LABELS[symbol]} {strategy_key} gate {gate.label}")
        if strategy_key == "dynamic_long":
            result = _run_backtest_with_loaded_data(
                entry_candles,
                instrument,
                build_dynamic_long_config(symbol),
                data_source_note=build_data_note(symbol, len(entry_candles), len(filter_candles)),
                maker_fee_rate=LONG_MAKER_FEE_RATE,
                taker_fee_rate=LONG_TAKER_FEE_RATE,
                direction_filter_bias=bias_map[gate.key],
            )
            trades = list(result.trades)
        elif strategy_key == "slope_long":
            trades = run_slope_long_with_bias(
                entry_candles,
                instrument,
                build_slope_long_config(symbol),
                bias_map[gate.key],
            )
        else:
            raise ValueError(f"unsupported strategy_key: {strategy_key}")

        test_trades = filter_split_trades(trades, test_bounds)
        gate_runs[gate.key] = GateRun(
            gate_key=gate.key,
            gate_label=gate.label,
            trades=trades,
            test_trades=test_trades,
            all_metrics=build_metrics(trades),
            test_metrics=build_metrics(test_trades),
        )

    return CoinStudy(
        symbol=symbol,
        label=SYMBOL_LABELS[symbol],
        entry_count=len(entry_candles),
        filter_count=len(filter_candles),
        start_ts=entry_candles[0].ts,
        end_ts=entry_candles[-1].ts,
        gate_runs=gate_runs,
    )


def combine_runs(selected_runs: list[GateRun]) -> tuple[list[BacktestTrade], list[BacktestTrade], SplitMetrics, SplitMetrics]:
    trades: list[BacktestTrade] = []
    test_trades: list[BacktestTrade] = []
    for run in selected_runs:
        trades.extend(run.trades)
        test_trades.extend(run.test_trades)
    trades.sort(key=lambda trade: (trade.exit_ts, trade.entry_ts, trade.signal))
    test_trades.sort(key=lambda trade: (trade.exit_ts, trade.entry_ts, trade.signal))
    return trades, test_trades, build_metrics(trades), build_metrics(test_trades)


def build_scenarios(studies: list[CoinStudy], strategy_label: str) -> list[ScenarioResult]:
    filtered_gates = [gate for gate in GATES if gate.key != "none"]

    baseline_runs = [study.gate_runs["none"] for study in studies]
    baseline_trades, baseline_test_trades, baseline_all, baseline_test = combine_runs(baseline_runs)
    baseline = ScenarioResult(
        key="baseline",
        label="不过滤",
        description=f"{strategy_label} 直接运行，不加日线过滤。",
        selected_gates={study.symbol: "none" for study in studies},
        trades=baseline_trades,
        test_trades=baseline_test_trades,
        all_metrics=baseline_all,
        test_metrics=baseline_test,
    )

    common_candidates: list[ScenarioResult] = []
    for gate in filtered_gates:
        selected_runs = [study.gate_runs[gate.key] for study in studies]
        trades, test_trades, all_metrics, test_metrics = combine_runs(selected_runs)
        common_candidates.append(
            ScenarioResult(
                key=f"common_{gate.key}",
                label=f"统一过滤: {gate.label}",
                description=f"5 个币种统一使用 {gate.label} 作为日线过滤。",
                selected_gates={study.symbol: gate.key for study in studies},
                trades=trades,
                test_trades=test_trades,
                all_metrics=all_metrics,
                test_metrics=test_metrics,
            )
        )
    best_common = max(common_candidates, key=lambda item: (item.test_metrics.pnl, item.all_metrics.pnl))

    best_per_coin_runs: list[GateRun] = []
    best_per_coin_keys: dict[str, str] = {}
    for study in studies:
        best_run = max(
            [study.gate_runs[gate.key] for gate in filtered_gates],
            key=lambda item: (item.test_metrics.pnl, item.all_metrics.pnl),
        )
        best_per_coin_runs.append(best_run)
        best_per_coin_keys[study.symbol] = best_run.gate_key
    trades, test_trades, all_metrics, test_metrics = combine_runs(best_per_coin_runs)
    best_per_coin = ScenarioResult(
        key="best_per_coin_filtered",
        label="各币种最佳过滤",
        description=f"每个币种单独选择对 {strategy_label} 测试段最优的日线过滤。",
        selected_gates=best_per_coin_keys,
        trades=trades,
        test_trades=test_trades,
        all_metrics=all_metrics,
        test_metrics=test_metrics,
    )

    return [baseline, best_common, best_per_coin]


def aggregate_gate_metrics(studies: list[CoinStudy], gate_key: str) -> GateRun:
    selected_runs = [study.gate_runs[gate_key] for study in studies]
    trades, test_trades, all_metrics, test_metrics = combine_runs(selected_runs)
    gate_label = next(gate.label for gate in GATES if gate.key == gate_key)
    return GateRun(
        gate_key=gate_key,
        gate_label=gate_label,
        trades=trades,
        test_trades=test_trades,
        all_metrics=all_metrics,
        test_metrics=test_metrics,
    )


def build_gate_frame(studies: list[CoinStudy], strategy_key: str, strategy_label: str) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for study in studies:
        baseline = study.gate_runs["none"]
        for gate in GATES:
            run = study.gate_runs[gate.key]
            rows.append(
                {
                    "strategy_key": strategy_key,
                    "strategy_label": strategy_label,
                    "scope": study.label,
                    "symbol": study.symbol,
                    "gate_key": gate.key,
                    "gate_label": gate.label,
                    "all_pnl": float(run.all_metrics.pnl),
                    "all_trades": run.all_metrics.trades,
                    "all_win_rate": float(run.all_metrics.win_rate),
                    "all_avg_r": float(run.all_metrics.avg_r),
                    "all_profit_factor": None if run.all_metrics.profit_factor is None else float(run.all_metrics.profit_factor),
                    "all_drawdown": float(run.all_metrics.max_drawdown),
                    "test_pnl": float(run.test_metrics.pnl),
                    "test_trades": run.test_metrics.trades,
                    "test_win_rate": float(run.test_metrics.win_rate),
                    "test_avg_r": float(run.test_metrics.avg_r),
                    "test_profit_factor": None if run.test_metrics.profit_factor is None else float(run.test_metrics.profit_factor),
                    "test_drawdown": float(run.test_metrics.max_drawdown),
                    "test_delta_vs_baseline": float(run.test_metrics.pnl - baseline.test_metrics.pnl),
                }
            )

    aggregate_baseline = aggregate_gate_metrics(studies, "none")
    for gate in GATES:
        aggregate = aggregate_gate_metrics(studies, gate.key)
        rows.append(
            {
                "strategy_key": strategy_key,
                "strategy_label": strategy_label,
                "scope": "ALL",
                "symbol": "ALL",
                "gate_key": gate.key,
                "gate_label": gate.label,
                "all_pnl": float(aggregate.all_metrics.pnl),
                "all_trades": aggregate.all_metrics.trades,
                "all_win_rate": float(aggregate.all_metrics.win_rate),
                "all_avg_r": float(aggregate.all_metrics.avg_r),
                "all_profit_factor": None if aggregate.all_metrics.profit_factor is None else float(aggregate.all_metrics.profit_factor),
                "all_drawdown": float(aggregate.all_metrics.max_drawdown),
                "test_pnl": float(aggregate.test_metrics.pnl),
                "test_trades": aggregate.test_metrics.trades,
                "test_win_rate": float(aggregate.test_metrics.win_rate),
                "test_avg_r": float(aggregate.test_metrics.avg_r),
                "test_profit_factor": None if aggregate.test_metrics.profit_factor is None else float(aggregate.test_metrics.profit_factor),
                "test_drawdown": float(aggregate.test_metrics.max_drawdown),
                "test_delta_vs_baseline": float(aggregate.test_metrics.pnl - aggregate_baseline.test_metrics.pnl),
            }
        )
    return pd.DataFrame(rows)


def build_strategy_study(client: OkxRestClient, strategy_key: str, label: str, config_note: str) -> StrategyStudy:
    studies = [run_coin_study(client, symbol, strategy_key) for symbol in SYMBOLS]
    scenarios = build_scenarios(studies, label)
    gate_frame = build_gate_frame(studies, strategy_key, label)
    return StrategyStudy(
        key=strategy_key,
        label=label,
        config_note=config_note,
        studies=studies,
        scenarios=scenarios,
        gate_frame=gate_frame,
    )


def fmt(value: Decimal | float | int, digits: int = 4) -> str:
    return format_decimal_fixed(Decimal(str(value)), digits)


def fmt_pf(value: Decimal | None) -> str:
    if value is None:
        return "-"
    return format_decimal_fixed(value, 4)


def pct(value: Decimal) -> str:
    return f"{format_decimal_fixed(value, 2)}%"


def split_payload(metrics: SplitMetrics) -> dict[str, object]:
    return {
        "pnl": str(metrics.pnl),
        "trades": metrics.trades,
        "win_rate": str(metrics.win_rate),
        "profit_factor": None if metrics.profit_factor is None else str(metrics.profit_factor),
        "avg_r": str(metrics.avg_r),
        "max_drawdown": str(metrics.max_drawdown),
        "return_pct": str(metrics.return_pct),
    }


def scenario_payload(scenario: ScenarioResult, baseline: ScenarioResult) -> dict[str, object]:
    return {
        "key": scenario.key,
        "label": scenario.label,
        "description": scenario.description,
        "selected_gates": scenario.selected_gates,
        "all_metrics": split_payload(scenario.all_metrics),
        "test_metrics": split_payload(scenario.test_metrics),
        "all_delta_vs_baseline": str(scenario.all_metrics.pnl - baseline.all_metrics.pnl),
        "test_delta_vs_baseline": str(scenario.test_metrics.pnl - baseline.test_metrics.pnl),
    }


def build_summary_frame(strategy_studies: list[StrategyStudy]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for strategy in strategy_studies:
        baseline = next(item for item in strategy.scenarios if item.key == "baseline")
        for scenario in strategy.scenarios:
            rows.append(
                {
                    "strategy_key": strategy.key,
                    "strategy_label": strategy.label,
                    "scenario_key": scenario.key,
                    "scenario_label": scenario.label,
                    "all_pnl": float(scenario.all_metrics.pnl),
                    "all_trades": scenario.all_metrics.trades,
                    "all_win_rate": float(scenario.all_metrics.win_rate),
                    "all_avg_r": float(scenario.all_metrics.avg_r),
                    "all_profit_factor": None if scenario.all_metrics.profit_factor is None else float(scenario.all_metrics.profit_factor),
                    "all_drawdown": float(scenario.all_metrics.max_drawdown),
                    "test_pnl": float(scenario.test_metrics.pnl),
                    "test_trades": scenario.test_metrics.trades,
                    "test_win_rate": float(scenario.test_metrics.win_rate),
                    "test_avg_r": float(scenario.test_metrics.avg_r),
                    "test_profit_factor": None if scenario.test_metrics.profit_factor is None else float(scenario.test_metrics.profit_factor),
                    "test_drawdown": float(scenario.test_metrics.max_drawdown),
                    "test_delta_vs_baseline": float(scenario.test_metrics.pnl - baseline.test_metrics.pnl),
                    "all_delta_vs_baseline": float(scenario.all_metrics.pnl - baseline.all_metrics.pnl),
                }
            )
    return pd.DataFrame(rows)


def build_yearly_frame(strategy_studies: list[StrategyStudy]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for strategy in strategy_studies:
        best_filtered = next(item for item in strategy.scenarios if item.key == "best_per_coin_filtered")
        for trade in best_filtered.trades:
            year = datetime.fromtimestamp(int(trade.exit_ts) / 1000, timezone.utc).strftime("%Y")
            rows.append(
                {
                    "strategy_label": strategy.label,
                    "year": year,
                    "pnl": float(trade.pnl),
                }
            )
    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(columns=["strategy_label", "year", "trades", "total_pnl"])
    return frame.groupby(["strategy_label", "year"], as_index=False).agg(
        trades=("pnl", "size"),
        total_pnl=("pnl", "sum"),
    )


def fig_to_base64(fig) -> str:
    buffer = io.BytesIO()
    fig.savefig(buffer, format="png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    return base64.b64encode(buffer.getvalue()).decode("ascii")


def build_scenario_chart(summary_frame: pd.DataFrame):
    plot_frame = summary_frame.copy()
    plot_frame["label"] = plot_frame["strategy_label"] + " / " + plot_frame["scenario_label"]
    x = range(len(plot_frame))
    width = 0.35
    fig, ax = plt.subplots(figsize=(11.5, 5.8))
    ax.bar([item - width / 2 for item in x], plot_frame["test_pnl"], width=width, label="测试段 PnL", color="#1d4ed8")
    ax.bar([item + width / 2 for item in x], plot_frame["all_pnl"], width=width, label="全样本 PnL", color="#0f766e")
    ax.set_title("两条做多线在不同日线过滤方案下的总盈亏对比", fontsize=14, pad=12)
    ax.set_xticks(list(x))
    ax.set_xticklabels(plot_frame["label"], rotation=12, ha="right")
    ax.set_ylabel("PnL (U)")
    ax.axhline(0, color="#475467", linewidth=1)
    ax.grid(axis="y", alpha=0.2)
    ax.legend()
    fig.tight_layout()
    return fig


def build_best_filtered_chart(summary_frame: pd.DataFrame):
    plot_frame = summary_frame[summary_frame["scenario_key"] == "best_per_coin_filtered"].copy()
    fig, ax = plt.subplots(figsize=(8, 5.2))
    width = 0.34
    x = range(len(plot_frame))
    ax.bar([item - width / 2 for item in x], plot_frame["test_pnl"], width=width, label="测试段 PnL", color="#d97706")
    ax.bar([item + width / 2 for item in x], plot_frame["all_pnl"], width=width, label="全样本 PnL", color="#059669")
    ax.set_title("各币种最佳过滤后的策略正面对比", fontsize=14, pad=12)
    ax.set_xticks(list(x))
    ax.set_xticklabels(plot_frame["strategy_label"])
    ax.set_ylabel("PnL (U)")
    ax.axhline(0, color="#475467", linewidth=1)
    ax.grid(axis="y", alpha=0.2)
    ax.legend()
    fig.tight_layout()
    return fig


def build_yearly_chart(yearly_frame: pd.DataFrame):
    fig, ax = plt.subplots(figsize=(10, 5.2))
    if yearly_frame.empty:
        ax.text(0.5, 0.5, "No yearly trades", ha="center", va="center", transform=ax.transAxes)
        ax.axis("off")
        fig.tight_layout()
        return fig
    pivot = yearly_frame.pivot(index="year", columns="strategy_label", values="total_pnl").fillna(0).sort_index()
    pivot.plot(kind="bar", ax=ax, color=["#b45309", "#0f766e"], width=0.75)
    ax.set_title("各币种最佳过滤后的年度总盈亏", fontsize=14, pad=12)
    ax.set_xlabel("")
    ax.set_ylabel("PnL (U)")
    ax.axhline(0, color="#475467", linewidth=1)
    ax.grid(axis="y", alpha=0.2)
    plt.xticks(rotation=0)
    fig.tight_layout()
    return fig


def build_payload(strategy_studies: list[StrategyStudy], summary_frame: pd.DataFrame) -> dict[str, object]:
    payload: dict[str, object] = {
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "entry_bar": ENTRY_BAR,
        "filter_bar": FILTER_BAR,
        "risk_amount": str(RISK_AMOUNT),
        "entry_limit": ENTRY_LIMIT,
        "coins": [SYMBOL_LABELS[symbol] for symbol in SYMBOLS],
        "strategies": {},
        "summary_rows": summary_frame.to_dict("records"),
    }
    for strategy in strategy_studies:
        baseline = next(item for item in strategy.scenarios if item.key == "baseline")
        best_common = next(item for item in strategy.scenarios if item.key.startswith("common_"))
        best_per_coin = next(item for item in strategy.scenarios if item.key == "best_per_coin_filtered")
        payload["strategies"][strategy.key] = {
            "label": strategy.label,
            "config_note": strategy.config_note,
            "scenarios": [scenario_payload(item, baseline) for item in strategy.scenarios],
            "best_common_gate": best_common.label,
            "best_per_coin_gates": {
                study.label: study.gate_runs[best_per_coin.selected_gates[study.symbol]].gate_label
                for study in strategy.studies
            },
            "gate_metrics": strategy.gate_frame.to_dict("records"),
            "data_ranges": {
                study.label: {
                    "entry_candles": study.entry_count,
                    "filter_candles": study.filter_count,
                    "start_utc": format_ts(study.start_ts),
                    "end_utc": format_ts(study.end_ts),
                }
                for study in strategy.studies
            },
        }
    return payload


def scenario_table(summary_frame: pd.DataFrame) -> str:
    rows = []
    for row in summary_frame.sort_values(["strategy_label", "scenario_key"]).to_dict("records"):
        test_delta = Decimal(str(row["test_delta_vs_baseline"]))
        all_delta = Decimal(str(row["all_delta_vs_baseline"]))
        rows.append(
            "<tr>"
            f"<td>{html.escape(str(row['strategy_label']))}</td>"
            f"<td>{html.escape(str(row['scenario_label']))}</td>"
            f"<td>{fmt(row['test_pnl'])}</td>"
            f"<td class=\"{'good' if test_delta >= 0 else 'bad'}\">{fmt(test_delta)}</td>"
            f"<td>{int(row['test_trades'])}</td>"
            f"<td>{fmt(row['test_avg_r'])}</td>"
            f"<td>{fmt_pf(None if row['test_profit_factor'] is None else Decimal(str(row['test_profit_factor'])))}</td>"
            f"<td>{fmt(row['all_pnl'])}</td>"
            f"<td class=\"{'good' if all_delta >= 0 else 'bad'}\">{fmt(all_delta)}</td>"
            f"<td>{int(row['all_trades'])}</td>"
            "</tr>"
        )
    return (
        "<table><thead><tr>"
        "<th>策略</th><th>方案</th><th>测试段PnL</th><th>测试段vs不过滤</th><th>测试段笔数</th>"
        "<th>测试段Avg R</th><th>测试段PF</th><th>全样本PnL</th><th>全样本vs不过滤</th><th>全样本笔数</th>"
        "</tr></thead><tbody>"
        + "".join(rows)
        + "</tbody></table>"
    )


def gate_table(strategy: StrategyStudy) -> str:
    frame = strategy.gate_frame[strategy.gate_frame["scope"] == "ALL"].copy()
    frame = frame.sort_values(["test_pnl", "all_pnl"], ascending=False)
    baseline_test_pnl = Decimal(str(float(frame[frame["gate_key"] == "none"]["test_pnl"].iloc[0])))
    rows = []
    for row in frame.to_dict("records"):
        delta = Decimal(str(row["test_pnl"])) - baseline_test_pnl
        rows.append(
            "<tr>"
            f"<td>{html.escape(str(row['gate_label']))}</td>"
            f"<td>{fmt(row['test_pnl'])}</td>"
            f"<td class=\"{'good' if delta >= 0 else 'bad'}\">{fmt(delta)}</td>"
            f"<td>{int(row['test_trades'])}</td>"
            f"<td>{fmt(row['test_avg_r'])}</td>"
            f"<td>{fmt_pf(None if row['test_profit_factor'] is None else Decimal(str(row['test_profit_factor'])))}</td>"
            f"<td>{fmt(row['all_pnl'])}</td>"
            f"<td>{int(row['all_trades'])}</td>"
            "</tr>"
        )
    return (
        "<table><thead><tr>"
        "<th>日线过滤</th><th>测试段PnL</th><th>测试段vs不过滤</th><th>测试段笔数</th>"
        "<th>测试段Avg R</th><th>测试段PF</th><th>全样本PnL</th><th>全样本笔数</th>"
        "</tr></thead><tbody>"
        + "".join(rows)
        + "</tbody></table>"
    )


def per_coin_best_gate_table(strategy: StrategyStudy) -> str:
    scenario = next(item for item in strategy.scenarios if item.key == "best_per_coin_filtered")
    rows = []
    for study in strategy.studies:
        run = study.gate_runs[scenario.selected_gates[study.symbol]]
        rows.append(
            "<tr>"
            f"<td>{html.escape(study.label)}</td>"
            f"<td>{html.escape(run.gate_label)}</td>"
            f"<td>{fmt(run.test_metrics.pnl)}</td>"
            f"<td>{fmt(run.all_metrics.pnl)}</td>"
            f"<td>{run.test_metrics.trades}</td>"
            f"<td>{run.all_metrics.trades}</td>"
            "</tr>"
        )
    return (
        "<table><thead><tr>"
        "<th>币种</th><th>最佳日线过滤</th><th>测试段PnL</th><th>全样本PnL</th><th>测试段笔数</th><th>全样本笔数</th>"
        "</tr></thead><tbody>"
        + "".join(rows)
        + "</tbody></table>"
    )


def build_html(strategy_studies: list[StrategyStudy], summary_frame: pd.DataFrame, yearly_frame: pd.DataFrame) -> str:
    slope = next(item for item in strategy_studies if item.key == "slope_long")
    dynamic = next(item for item in strategy_studies if item.key == "dynamic_long")
    slope_best = next(item for item in slope.scenarios if item.key == "best_per_coin_filtered")
    dynamic_best = next(item for item in dynamic.scenarios if item.key == "best_per_coin_filtered")
    best_test_winner = slope if slope_best.test_metrics.pnl >= dynamic_best.test_metrics.pnl else dynamic
    best_all_winner = slope if slope_best.all_metrics.pnl >= dynamic_best.all_metrics.pnl else dynamic

    scenario_chart = fig_to_base64(build_scenario_chart(summary_frame))
    best_filtered_chart = fig_to_base64(build_best_filtered_chart(summary_frame))
    yearly_chart = fig_to_base64(build_yearly_chart(yearly_frame))

    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <title>5币 10U 斜率做多 vs 动态委托做多 日线过滤对比</title>
  <style>
    :root {{
      --bg: #f7f4ee;
      --card: #fffdf8;
      --ink: #1f2937;
      --muted: #667085;
      --line: #e7dccd;
      --accent-a: #b45309;
      --accent-b: #0f766e;
      --good: #047857;
      --bad: #b42318;
      --shadow: 0 18px 44px rgba(89, 65, 44, 0.08);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Microsoft YaHei", "Segoe UI", sans-serif;
      background:
        radial-gradient(circle at top left, rgba(180,83,9,0.10), transparent 28%),
        radial-gradient(circle at top right, rgba(15,118,110,0.10), transparent 26%),
        linear-gradient(180deg, #fcfaf6 0%, var(--bg) 100%);
      color: var(--ink);
    }}
    .wrap {{ max-width: 1220px; margin: 0 auto; padding: 32px 24px 56px; }}
    .hero {{
      background: linear-gradient(135deg, rgba(180,83,9,0.92), rgba(15,118,110,0.92));
      color: white;
      border-radius: 24px;
      padding: 28px 30px;
      box-shadow: var(--shadow);
    }}
    .hero h1 {{ margin: 0 0 10px; font-size: 34px; }}
    .hero p {{ margin: 6px 0; line-height: 1.65; }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 16px;
      margin: 24px 0 6px;
    }}
    .card {{
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 18px 18px 16px;
      box-shadow: var(--shadow);
    }}
    .kpi .label {{ color: var(--muted); font-size: 13px; }}
    .kpi .value {{ font-size: 28px; font-weight: 700; margin: 8px 0 4px; }}
    .kpi .sub {{ color: var(--muted); font-size: 13px; line-height: 1.5; }}
    h2 {{ margin: 30px 0 12px; font-size: 22px; }}
    h3 {{ margin: 22px 0 10px; font-size: 18px; }}
    p, li {{ line-height: 1.7; }}
    .section {{ margin-top: 22px; }}
    .chart img {{ width: 100%; border-radius: 18px; border: 1px solid var(--line); background: white; }}
    table {{
      width: 100%;
      border-collapse: collapse;
      background: white;
      border-radius: 16px;
      overflow: hidden;
      box-shadow: var(--shadow);
    }}
    th, td {{
      padding: 10px 12px;
      border-bottom: 1px solid #efe7db;
      text-align: right;
      font-size: 14px;
      white-space: nowrap;
    }}
    th:first-child, td:first-child,
    th:nth-child(2), td:nth-child(2) {{ text-align: left; }}
    th {{
      background: #f8f1e7;
      color: #5b4633;
      position: sticky;
      top: 0;
    }}
    tr:hover td {{ background: #fffcf7; }}
    .good {{ color: var(--good); font-weight: 700; }}
    .bad {{ color: var(--bad); font-weight: 700; }}
    .muted {{ color: var(--muted); }}
    .split {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
      gap: 18px;
    }}
    .note {{
      background: rgba(255,255,255,0.82);
      border: 1px dashed #d8c5af;
      border-radius: 16px;
      padding: 14px 16px;
    }}
    @media (max-width: 720px) {{
      .wrap {{ padding: 18px 14px 36px; }}
      .hero h1 {{ font-size: 26px; }}
      .kpi .value {{ font-size: 24px; }}
      th, td {{ font-size: 13px; padding: 9px 10px; }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <h1>5币 10U 做多线对比</h1>
      <p>对象：<strong>EMA55 斜率镜像做多</strong> vs <strong>EMA 动态委托做多</strong>。两条线都额外接上 <strong>日线方向过滤</strong>，并在 5 个币种上全历史回测。</p>
      <p>口径：{ENTRY_BAR} 入场，{FILTER_BAR} 过滤，固定风险金 <strong>{RISK_AMOUNT}U</strong>，初始资金 <strong>{INITIAL_CAPITAL}U</strong>，本地缓存读取 <strong>{entry_limit_label()}</strong> {ENTRY_BAR} K 线。</p>
    </section>

    <div class="grid">
      <div class="card kpi">
        <div class="label">测试段最强策略</div>
        <div class="value">{html.escape(best_test_winner.label)}</div>
        <div class="sub">按“各币种最佳过滤”口径，测试段 PnL {fmt(max(slope_best.test_metrics.pnl, dynamic_best.test_metrics.pnl))}U</div>
      </div>
      <div class="card kpi">
        <div class="label">全样本最强策略</div>
        <div class="value">{html.escape(best_all_winner.label)}</div>
        <div class="sub">按“各币种最佳过滤”口径，全样本 PnL {fmt(max(slope_best.all_metrics.pnl, dynamic_best.all_metrics.pnl))}U</div>
      </div>
      <div class="card kpi">
        <div class="label">斜率做多最佳过滤</div>
        <div class="value">{fmt(slope_best.test_metrics.pnl)}U</div>
        <div class="sub">测试段 {slope_best.test_metrics.trades} 笔，全样本 {fmt(slope_best.all_metrics.pnl)}U</div>
      </div>
      <div class="card kpi">
        <div class="label">动态做多最佳过滤</div>
        <div class="value">{fmt(dynamic_best.test_metrics.pnl)}U</div>
        <div class="sub">测试段 {dynamic_best.test_metrics.trades} 笔，全样本 {fmt(dynamic_best.all_metrics.pnl)}U</div>
      </div>
    </div>

    <section class="section">
      <h2>结论</h2>
      <div class="split">
        <div class="card">
          <h3>测试段</h3>
          <p>{html.escape(best_test_winner.label)} 在“各币种最佳过滤”口径下更强。斜率做多测试段为 <strong>{fmt(slope_best.test_metrics.pnl)}U</strong>，动态做多为 <strong>{fmt(dynamic_best.test_metrics.pnl)}U</strong>，差值 <strong>{fmt(slope_best.test_metrics.pnl - dynamic_best.test_metrics.pnl)}</strong>U。</p>
        </div>
        <div class="card">
          <h3>全样本</h3>
          <p>{html.escape(best_all_winner.label)} 在“各币种最佳过滤”口径下更强。斜率做多全样本为 <strong>{fmt(slope_best.all_metrics.pnl)}U</strong>，动态做多为 <strong>{fmt(dynamic_best.all_metrics.pnl)}U</strong>，差值 <strong>{fmt(slope_best.all_metrics.pnl - dynamic_best.all_metrics.pnl)}</strong>U。</p>
        </div>
      </div>
      <div class="note">
        <strong>当前斜率做多参数假设：</strong>{html.escape(slope.config_note)}<br>
        <strong>当前动态做多参数假设：</strong>{html.escape(dynamic.config_note)}
      </div>
    </section>

    <section class="section chart">
      <h2>总览图</h2>
      <img alt="scenario_chart" src="data:image/png;base64,{scenario_chart}">
    </section>

    <section class="section chart">
      <h2>最佳过滤正面对比</h2>
      <img alt="best_filtered_chart" src="data:image/png;base64,{best_filtered_chart}">
    </section>

    <section class="section">
      <h2>策略方案表</h2>
      {scenario_table(summary_frame)}
    </section>

    <section class="section">
      <h2>日线过滤扫描</h2>
      <div class="split">
        <div>
          <h3>{html.escape(slope.label)}</h3>
          {gate_table(slope)}
        </div>
        <div>
          <h3>{html.escape(dynamic.label)}</h3>
          {gate_table(dynamic)}
        </div>
      </div>
    </section>

    <section class="section">
      <h2>各币种最佳过滤</h2>
      <div class="split">
        <div>
          <h3>{html.escape(slope.label)}</h3>
          {per_coin_best_gate_table(slope)}
        </div>
        <div>
          <h3>{html.escape(dynamic.label)}</h3>
          {per_coin_best_gate_table(dynamic)}
        </div>
      </div>
    </section>

    <section class="section chart">
      <h2>最佳过滤年度表现</h2>
      <img alt="yearly_chart" src="data:image/png;base64,{yearly_chart}">
    </section>

    <section class="section">
      <h2>样本说明</h2>
      <div class="card">
        <p>5 个币种：{", ".join(SYMBOL_LABELS[symbol] for symbol in SYMBOLS)}。</p>
        <p>日线过滤候选：{", ".join(gate.label for gate in GATES if gate.key != "none")}。</p>
        <p>生成时间：{datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")}。</p>
      </div>
    </section>
  </div>
</body>
</html>
"""


def main() -> None:
    client = OkxRestClient()
    strategy_studies = [
        build_strategy_study(
            client,
            "slope_long",
            "EMA55 斜率镜像做多",
            "EMA55 单根斜率比率 >= +0.0005 入场，斜率转负出场，ATR14 止损 2 倍，动态止盈 4 倍。",
        ),
        build_strategy_study(
            client,
            "dynamic_long",
            "EMA 动态委托做多",
            "沿用 5 币种既有最佳参数：EMA/趋势线/挂单参考线/ATR 止损按币种分别取最优组合。",
        ),
    ]

    summary_frame = build_summary_frame(strategy_studies)
    gate_frame = pd.concat([study.gate_frame for study in strategy_studies], ignore_index=True)
    yearly_frame = build_yearly_frame(strategy_studies)

    summary_frame.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")
    payload = build_payload(strategy_studies, summary_frame)
    payload["gate_rows"] = gate_frame.to_dict("records")
    payload["yearly_rows"] = yearly_frame.to_dict("records")
    JSON_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    HTML_PATH.write_text(build_html(strategy_studies, summary_frame, yearly_frame), encoding="utf-8")
    copyfile(HTML_PATH, PROJECT_HTML_PATH)
    print(HTML_PATH)
    print(PROJECT_HTML_PATH)


if __name__ == "__main__":
    main()
