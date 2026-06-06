from __future__ import annotations

import bisect
import json
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from okx_quant.backtest import (
    _build_drawdown_curves,
    _build_equity_curve,
    _build_period_stats,
    _build_report,
    _evaluate_dynamic_signal_precomputed,
    _run_dynamic_backtest,
)
from okx_quant.candle_cache import load_candle_cache
from okx_quant.indicators import atr, moving_average
from okx_quant.models import Candle, StrategyConfig
from okx_quant.okx_client import OkxRestClient
from okx_quant.persistence import analysis_report_dir_path
from okx_quant.pricing import format_decimal_fixed
from okx_quant.strategies.adaptive_ema_rail import (
    ADAPTIVE_RAIL_STATE_BROKEN,
    ADAPTIVE_RAIL_STATE_CONFIRMED,
    adaptive_rail_candidate_periods,
    evaluate_adaptive_rail_signal,
)
from okx_quant.strategy_catalog import STRATEGY_DYNAMIC_LONG_ID
from okx_quant.timeframe import closed_candle_available_timestamps


SYMBOL = "BTC-USDT-SWAP"
INITIAL_CAPITAL = Decimal("10000")
RISK_AMOUNT = Decimal("10")
MAKER_FEE = Decimal("0.00015")
TAKER_FEE = Decimal("0.00036")


@dataclass(frozen=True)
class Window:
    key: str
    label: str
    start_ts: int


@dataclass(frozen=True)
class Scenario:
    key: str
    label: str
    entry_bar: str
    filter_bar: str
    ema_period: int
    trend_ema_period: int
    entry_reference_ema_period: int
    atr_stop_multiplier: Decimal
    atr_take_multiplier: Decimal


@dataclass(frozen=True)
class ResultRow:
    scenario_key: str
    scenario_label: str
    filter_mode: str
    window_key: str
    window_label: str
    entry_bar: str
    filter_bar: str
    candle_count: int
    total_trades: int
    win_rate: str
    total_pnl: str
    total_return_pct: str
    max_drawdown_pct: str
    profit_factor: str
    average_r_multiple: str
    filter_long_coverage_pct: str
    raw_long_signals: int
    blocked_long_signals: int
    blocked_signal_pct: str


WINDOWS: tuple[Window, ...] = (
    Window(key="full", label="Full History", start_ts=0),
    Window(
        key="since_2024",
        label="Since 2024-01-01",
        start_ts=int(datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp() * 1000),
    ),
    Window(
        key="since_2025",
        label="Since 2025-01-01",
        start_ts=int(datetime(2025, 1, 1, tzinfo=timezone.utc).timestamp() * 1000),
    ),
)


SCENARIOS: tuple[Scenario, ...] = (
    Scenario(
        key="dynamic_5_13_1h",
        label="BTC EMA Dynamic 5/13 1H",
        entry_bar="1H",
        filter_bar="4H",
        ema_period=5,
        trend_ema_period=13,
        entry_reference_ema_period=0,
        atr_stop_multiplier=Decimal("1"),
        atr_take_multiplier=Decimal("4"),
    ),
    Scenario(
        key="dynamic_5_13_4h",
        label="BTC EMA Dynamic 5/13 4H",
        entry_bar="4H",
        filter_bar="4H",
        ema_period=5,
        trend_ema_period=13,
        entry_reference_ema_period=0,
        atr_stop_multiplier=Decimal("1"),
        atr_take_multiplier=Decimal("4"),
    ),
)


FILTER_MODE_LABELS: dict[str, str] = {
    "none": "No Filter",
    "confirmed_state": "Adaptive Rail Confirmed",
    "entry_ready": "Adaptive Rail Entry Ready",
}


def _fmt(value: Decimal | None, digits: int = 4) -> str:
    if value is None:
        return "-"
    return format_decimal_fixed(value, digits)


def _dynamic_config(scenario: Scenario) -> StrategyConfig:
    return StrategyConfig(
        inst_id=SYMBOL,
        bar=scenario.entry_bar,
        ema_period=scenario.ema_period,
        trend_ema_period=scenario.trend_ema_period,
        big_ema_period=233,
        atr_period=10,
        atr_stop_multiplier=scenario.atr_stop_multiplier,
        atr_take_multiplier=scenario.atr_take_multiplier,
        order_size=Decimal("0"),
        trade_mode="cross",
        signal_mode="long_only",
        position_mode="net",
        environment="demo",
        tp_sl_trigger_type="mark",
        strategy_id=STRATEGY_DYNAMIC_LONG_ID,
        risk_amount=RISK_AMOUNT,
        entry_reference_ema_period=scenario.entry_reference_ema_period,
        take_profit_mode="dynamic",
        max_entries_per_trend=1,
        dynamic_two_r_break_even=True,
        dynamic_fee_offset_enabled=True,
        time_stop_break_even_enabled=False,
        time_stop_break_even_bars=0,
        hold_close_exit_bars=0,
        backtest_initial_capital=INITIAL_CAPITAL,
        backtest_sizing_mode="fixed_risk",
        backtest_compounding=False,
        backtest_entry_slippage_rate=Decimal("0"),
        backtest_exit_slippage_rate=Decimal("0"),
        backtest_slippage_rate=Decimal("0"),
        backtest_funding_rate=Decimal("0"),
    )


def _adaptive_filter_config(filter_bar: str) -> StrategyConfig:
    return StrategyConfig(
        inst_id=SYMBOL,
        bar=filter_bar,
        ema_period=21,
        trend_ema_period=55,
        big_ema_period=233,
        atr_period=10,
        atr_stop_multiplier=Decimal("1.5"),
        atr_take_multiplier=Decimal("4"),
        order_size=Decimal("0"),
        trade_mode="cross",
        signal_mode="long_only",
        position_mode="net",
        environment="demo",
        tp_sl_trigger_type="mark",
        strategy_id="adaptive_ema_rail_long",
        risk_amount=RISK_AMOUNT,
        entry_reference_ema_period=55,
        take_profit_mode="dynamic",
        max_entries_per_trend=1,
        dynamic_two_r_break_even=True,
        dynamic_fee_offset_enabled=True,
        time_stop_break_even_enabled=False,
        time_stop_break_even_bars=0,
        hold_close_exit_bars=0,
        backtest_initial_capital=INITIAL_CAPITAL,
        backtest_sizing_mode="fixed_risk",
        backtest_compounding=False,
        backtest_entry_slippage_rate=Decimal("0"),
        backtest_exit_slippage_rate=Decimal("0"),
        backtest_slippage_rate=Decimal("0"),
        backtest_funding_rate=Decimal("0"),
        rail_break_atr_ratio=Decimal("1.5"),
        rail_reclaim_bars=2,
        rail_switch_min_score_delta=Decimal("12"),
        rail_candidate_ema_periods=(21, 34, 55, 89),
        rail_fast_gate_enabled=True,
        rail_fast_gate_period=21,
        rail_fast_min_gap_ema200_atr=Decimal("5.0"),
        rail_fast_min_spread_trend_atr=Decimal("1.5"),
        rail_fast_max_recent_range_atr=Decimal("3.0"),
        rail_fast_recent_range_bars=8,
    )


def _build_adaptive_rail_bias(
    entry_candles: list[Candle],
    filter_candles: list[Candle],
    *,
    config: StrategyConfig,
    mode: str,
) -> tuple[list[str], Decimal]:
    confirmed_filter_candles = [candle for candle in filter_candles if candle.confirmed]
    if not entry_candles or not confirmed_filter_candles:
        return ["neutral"] * len(entry_candles), Decimal("0")

    closes = [candle.close for candle in confirmed_filter_candles]
    candidate_periods = adaptive_rail_candidate_periods(config)
    ema_periods = {period for period in candidate_periods if period > 0}
    ema_periods.add(int(config.trend_ema_period))
    if bool(config.rail_fast_gate_enabled) and int(config.rail_fast_gate_period) > 0:
        ema_periods.add(int(config.rail_fast_gate_period))
    ema_by_period = {period: moving_average(closes, period, "ema") for period in sorted(ema_periods)}
    ema200_values = ema_by_period.get(200) or moving_average(closes, 200, "ema")
    atr_values = atr(confirmed_filter_candles, config.atr_period)

    per_filter_bias: list[str] = []
    current_period: int | None = None
    long_bars = 0
    for index in range(len(confirmed_filter_candles)):
        snapshot = evaluate_adaptive_rail_signal(
            confirmed_filter_candles,
            index,
            ema_by_period=ema_by_period,
            ema200_values=ema200_values,
            atr_values=atr_values,
            config=config,
            current_period=current_period,
        )
        if snapshot.state == ADAPTIVE_RAIL_STATE_BROKEN:
            current_period = None
        elif snapshot.dominant_period is not None:
            current_period = snapshot.dominant_period

        if mode == "confirmed_state":
            allow_long = snapshot.state == ADAPTIVE_RAIL_STATE_CONFIRMED and snapshot.dominant_period is not None
        elif mode == "entry_ready":
            allow_long = snapshot.decision.signal == "long"
        else:
            allow_long = False
        per_filter_bias.append("long" if allow_long else "neutral")
        if allow_long:
            long_bars += 1

    filter_coverage_pct = (
        Decimal("0")
        if not confirmed_filter_candles
        else (Decimal(long_bars) / Decimal(len(confirmed_filter_candles))) * Decimal("100")
    )

    filter_ts = closed_candle_available_timestamps(confirmed_filter_candles)
    entry_bias: list[str] = []
    for entry_candle in entry_candles:
        j = bisect.bisect_right(filter_ts, entry_candle.ts) - 1
        entry_bias.append(per_filter_bias[j] if j >= 0 else "neutral")
    return entry_bias, filter_coverage_pct


def _count_dynamic_long_signals(
    candles: list[Candle],
    *,
    config: StrategyConfig,
    filter_bias: list[str] | None,
) -> tuple[int, int]:
    entry_reference_period = config.resolved_entry_reference_ema_period()
    minimum = max(
        config.ema_period,
        config.trend_ema_period,
        config.atr_period,
        entry_reference_period,
    )
    if len(candles) < minimum + 1:
        return 0, 0

    closes = [candle.close for candle in candles]
    ema_values = moving_average(closes, config.ema_period, config.resolved_ema_type())
    entry_reference_values = (
        ema_values
        if (
            entry_reference_period == config.ema_period
            and config.resolved_entry_reference_ema_type() == config.resolved_ema_type()
        )
        else moving_average(closes, entry_reference_period, config.resolved_entry_reference_ema_type())
    )
    trend_values = moving_average(closes, config.trend_ema_period, config.resolved_trend_ema_type())
    atr_values = atr(candles, config.atr_period)

    raw_signals = 0
    blocked_signals = 0
    trade_start_index = max(max(minimum - 1, 0), 200)
    for index in range(trade_start_index, len(candles) - 1):
        decision = _evaluate_dynamic_signal_precomputed(
            candles,
            index,
            ema_values,
            entry_reference_values,
            trend_values,
            atr_values,
            config,
        )
        if decision.signal != "long":
            continue
        raw_signals += 1
        if filter_bias is not None and index < len(filter_bias) and filter_bias[index] != "long":
            blocked_signals += 1
    return raw_signals, blocked_signals


def _run_row(
    instrument,
    entry_candles: list[Candle],
    filter_candles: list[Candle],
    *,
    scenario: Scenario,
    window: Window,
    filter_mode: str,
) -> ResultRow:
    config = _dynamic_config(scenario)
    bias = None
    filter_coverage_pct = Decimal("0")
    if filter_mode != "none":
        bias, filter_coverage_pct = _build_adaptive_rail_bias(
            entry_candles,
            filter_candles,
            config=_adaptive_filter_config(scenario.filter_bar),
            mode=filter_mode,
        )

    trades, _ = _run_dynamic_backtest(
        entry_candles,
        instrument,
        config,
        maker_fee_rate=MAKER_FEE,
        taker_fee_rate=TAKER_FEE,
        mtf_filter_bias=bias,
    )
    report = _build_report(trades, initial_capital=INITIAL_CAPITAL)
    raw_signals, blocked_signals = _count_dynamic_long_signals(
        entry_candles,
        config=config,
        filter_bias=bias,
    )
    blocked_signal_pct = (
        Decimal("0")
        if raw_signals <= 0
        else (Decimal(blocked_signals) / Decimal(raw_signals)) * Decimal("100")
    )

    return ResultRow(
        scenario_key=scenario.key,
        scenario_label=scenario.label,
        filter_mode=filter_mode,
        window_key=window.key,
        window_label=window.label,
        entry_bar=scenario.entry_bar,
        filter_bar=scenario.filter_bar,
        candle_count=len(entry_candles),
        total_trades=report.total_trades,
        win_rate=_fmt(report.win_rate, 2),
        total_pnl=_fmt(report.total_pnl, 4),
        total_return_pct=_fmt(report.total_return_pct, 2),
        max_drawdown_pct=_fmt(report.max_drawdown_pct, 2),
        profit_factor=_fmt(report.profit_factor, 4),
        average_r_multiple=_fmt(report.average_r_multiple, 4),
        filter_long_coverage_pct=_fmt(filter_coverage_pct, 2),
        raw_long_signals=raw_signals,
        blocked_long_signals=blocked_signals,
        blocked_signal_pct=_fmt(blocked_signal_pct, 2),
    )


def _build_markdown(rows: list[ResultRow]) -> str:
    lines = [
        "# EMA Dynamic Long + Adaptive Rail Filter Study",
        "",
        f"- Symbol: `{SYMBOL}`",
        "- Goal: decide whether Adaptive Rail still has value as a structure filter instead of a standalone strategy.",
        "- Dynamic baseline: BTC EMA Dynamic 5/13 on `1H` and `4H`.",
        "- Adaptive filter baseline: `4H Balanced + 21/34/55/89 + EMA21 Gate`.",
        "",
    ]

    for scenario in SCENARIOS:
        lines.extend(
            [
                f"## {scenario.label}",
                "",
                "| Filter | Window | Trades | Win Rate | Return | Max DD | PF | Avg R | Filter Coverage | Raw Long Signals | Blocked Signals | Blocked % |",
                "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        scenario_rows = [row for row in rows if row.scenario_key == scenario.key]
        for row in scenario_rows:
            lines.append(
                f"| {FILTER_MODE_LABELS[row.filter_mode]} | {row.window_label} | {row.total_trades} | "
                f"{row.win_rate}% | {row.total_return_pct}% | {row.max_drawdown_pct}% | {row.profit_factor} | "
                f"{row.average_r_multiple} | {row.filter_long_coverage_pct}% | {row.raw_long_signals} | "
                f"{row.blocked_long_signals} | {row.blocked_signal_pct}% |"
            )
        lines.append("")

    lines.extend(
        [
            "## Reading Guide",
            "",
            "1. `Confirmed` means only the rail structure must already be confirmed; Dynamic Long keeps its own entry timing.",
            "2. `Entry Ready` is stricter: Adaptive Rail itself must already be in long-entry-ready state.",
            "3. If filtering reduces trades but does not improve 2024+/2025+ return quality, the direction can be safely downgraded.",
        ]
    )
    return "\n".join(lines)


def main() -> None:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = analysis_report_dir_path()
    out_dir.mkdir(parents=True, exist_ok=True)
    md_path = out_dir / f"dynamic_long_with_adaptive_rail_filter_study_{stamp}.md"
    json_path = out_dir / f"dynamic_long_with_adaptive_rail_filter_study_{stamp}.json"

    client = OkxRestClient()
    instrument = client.get_instrument(SYMBOL)
    cache: dict[str, list[Candle]] = {}

    rows: list[ResultRow] = []
    for scenario in SCENARIOS:
        entry_all = cache.setdefault(
            scenario.entry_bar,
            [candle for candle in load_candle_cache(SYMBOL, scenario.entry_bar, limit=None) if candle.confirmed],
        )
        filter_all = cache.setdefault(
            scenario.filter_bar,
            [candle for candle in load_candle_cache(SYMBOL, scenario.filter_bar, limit=None) if candle.confirmed],
        )
        for window in WINDOWS:
            entry_candles = [candle for candle in entry_all if candle.ts >= window.start_ts]
            filter_candles = [candle for candle in filter_all if candle.ts >= window.start_ts]
            for filter_mode in ("none", "confirmed_state", "entry_ready"):
                print(f"run {scenario.key} {window.key} {filter_mode}", flush=True)
                rows.append(
                    _run_row(
                        instrument,
                        entry_candles,
                        filter_candles,
                        scenario=scenario,
                        window=window,
                        filter_mode=filter_mode,
                    )
                )

    md_path.write_text(_build_markdown(rows), encoding="utf-8")
    payload = {
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "symbol": SYMBOL,
        "rows": [asdict(row) for row in rows],
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(md_path)
    print(json_path)


if __name__ == "__main__":
    main()
