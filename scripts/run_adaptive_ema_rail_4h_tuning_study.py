from __future__ import annotations

import json
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from okx_quant.backtest import _run_backtest_with_loaded_data
from okx_quant.candle_cache import load_candle_cache
from okx_quant.models import StrategyConfig
from okx_quant.okx_client import OkxRestClient
from okx_quant.persistence import analysis_report_dir_path
from okx_quant.pricing import format_decimal_fixed
from okx_quant.strategy_catalog import (
    STRATEGY_ADAPTIVE_EMA_RAIL_LONG_ID,
    STRATEGY_DYNAMIC_LONG_ID,
    STRATEGY_EMA_BREAKOUT_LONG_ID,
)


SYMBOL = "BTC-USDT-SWAP"
BAR = "4H"
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
class Variant:
    key: str
    label: str
    strategy_id: str
    ema_period: int
    trend_ema_period: int
    entry_reference_ema_period: int
    atr_stop_multiplier: Decimal
    atr_take_multiplier: Decimal
    rail_break_atr_ratio: Decimal = Decimal("1.0")
    rail_reclaim_bars: int = 2
    rail_switch_min_score_delta: Decimal = Decimal("8")


@dataclass(frozen=True)
class ResultRow:
    variant_key: str
    variant_label: str
    window_key: str
    window_label: str
    candle_count: int
    total_trades: int
    win_rate: str
    total_return_pct: str
    max_drawdown_pct: str
    profit_factor: str
    average_r_multiple: str
    confirmed_coverage_pct: str
    rail_switches: str
    average_hold_bars: str
    rail_broken_exit_pct: str
    dominant_period_mix: str


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


VARIANTS: tuple[Variant, ...] = (
    Variant(
        key="dynamic_std_4h",
        label="EMA Dynamic Std 4H",
        strategy_id=STRATEGY_DYNAMIC_LONG_ID,
        ema_period=21,
        trend_ema_period=55,
        entry_reference_ema_period=55,
        atr_stop_multiplier=Decimal("2.0"),
        atr_take_multiplier=Decimal("4"),
    ),
    Variant(
        key="breakout_21_4h",
        label="EMA Breakout 4H",
        strategy_id=STRATEGY_EMA_BREAKOUT_LONG_ID,
        ema_period=21,
        trend_ema_period=55,
        entry_reference_ema_period=21,
        atr_stop_multiplier=Decimal("2.0"),
        atr_take_multiplier=Decimal("4"),
    ),
    Variant(
        key="adaptive_default_4h",
        label="Adaptive Rail Current 4H",
        strategy_id=STRATEGY_ADAPTIVE_EMA_RAIL_LONG_ID,
        ema_period=21,
        trend_ema_period=55,
        entry_reference_ema_period=55,
        atr_stop_multiplier=Decimal("2.0"),
        atr_take_multiplier=Decimal("4"),
        rail_break_atr_ratio=Decimal("1.0"),
        rail_reclaim_bars=2,
        rail_switch_min_score_delta=Decimal("8"),
    ),
    Variant(
        key="adaptive_best_return_4h",
        label="Adaptive Rail Return 4H",
        strategy_id=STRATEGY_ADAPTIVE_EMA_RAIL_LONG_ID,
        ema_period=21,
        trend_ema_period=55,
        entry_reference_ema_period=55,
        atr_stop_multiplier=Decimal("2.0"),
        atr_take_multiplier=Decimal("4"),
        rail_break_atr_ratio=Decimal("1.0"),
        rail_reclaim_bars=1,
        rail_switch_min_score_delta=Decimal("4"),
    ),
    Variant(
        key="adaptive_balanced_4h",
        label="Adaptive Rail Balanced 4H",
        strategy_id=STRATEGY_ADAPTIVE_EMA_RAIL_LONG_ID,
        ema_period=21,
        trend_ema_period=55,
        entry_reference_ema_period=55,
        atr_stop_multiplier=Decimal("1.5"),
        atr_take_multiplier=Decimal("4"),
        rail_break_atr_ratio=Decimal("1.5"),
        rail_reclaim_bars=2,
        rail_switch_min_score_delta=Decimal("12"),
    ),
)


def _fmt(value: Decimal | None, digits: int = 4) -> str:
    if value is None:
        return "-"
    return format_decimal_fixed(value, digits)


def _fmt_pct_cell(value: str) -> str:
    return value if value == "-" else f"{value}%"


def _build_config(variant: Variant) -> StrategyConfig:
    return StrategyConfig(
        inst_id=SYMBOL,
        bar=BAR,
        ema_period=variant.ema_period,
        trend_ema_period=variant.trend_ema_period,
        big_ema_period=233,
        atr_period=10,
        atr_stop_multiplier=variant.atr_stop_multiplier,
        atr_take_multiplier=variant.atr_take_multiplier,
        order_size=Decimal("0"),
        trade_mode="cross",
        signal_mode="long_only",
        position_mode="net",
        environment="demo",
        tp_sl_trigger_type="mark",
        strategy_id=variant.strategy_id,
        risk_amount=RISK_AMOUNT,
        entry_reference_ema_period=variant.entry_reference_ema_period,
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
        rail_break_atr_ratio=variant.rail_break_atr_ratio,
        rail_reclaim_bars=variant.rail_reclaim_bars,
        rail_switch_min_score_delta=variant.rail_switch_min_score_delta,
    )


def _run_variant(
    instrument,
    all_candles: list,
    variant: Variant,
    window: Window,
) -> ResultRow:
    candles = [candle for candle in all_candles if candle.ts >= window.start_ts]
    result = _run_backtest_with_loaded_data(
        candles,
        instrument,
        _build_config(variant),
        data_source_note=f"local candle_cache full history | {SYMBOL} {BAR} | candles={len(candles)}",
        maker_fee_rate=MAKER_FEE,
        taker_fee_rate=TAKER_FEE,
    )
    report = result.report
    adaptive_stats = result.adaptive_rail_stats
    return ResultRow(
        variant_key=variant.key,
        variant_label=variant.label,
        window_key=window.key,
        window_label=window.label,
        candle_count=len(candles),
        total_trades=report.total_trades,
        win_rate=_fmt(report.win_rate, 2),
        total_return_pct=_fmt(report.total_return_pct, 2),
        max_drawdown_pct=_fmt(report.max_drawdown_pct, 2),
        profit_factor=_fmt(report.profit_factor, 4),
        average_r_multiple=_fmt(report.average_r_multiple, 4),
        confirmed_coverage_pct=(
            _fmt(adaptive_stats.confirmed_coverage_pct, 2) if adaptive_stats is not None else "-"
        ),
        rail_switches=str(adaptive_stats.dominant_rail_switches) if adaptive_stats is not None else "-",
        average_hold_bars=(
            _fmt(adaptive_stats.average_dominant_rail_hold_bars, 2) if adaptive_stats is not None else "-"
        ),
        rail_broken_exit_pct=(
            _fmt(adaptive_stats.rail_broken_exit_pct, 2) if adaptive_stats is not None else "-"
        ),
        dominant_period_mix=(
            ", ".join(
                f"EMA{item.period} {_fmt(item.share_pct, 2)}%"
                for item in adaptive_stats.dominant_period_frequencies[:3]
            )
            if adaptive_stats is not None and adaptive_stats.dominant_period_frequencies
            else "-"
        ),
    )


def _build_markdown(rows: list[ResultRow]) -> str:
    lines = [
        "# Adaptive EMA Rail 4H Tuning Study",
        "",
        f"- Symbol: `{SYMBOL}`",
        f"- Bar: `{BAR}`",
        "- Data: local `candle_cache` confirmed full history",
        f"- Fees: maker `{MAKER_FEE}` / taker `{TAKER_FEE}`",
        "",
        "## Summary",
        "",
        "| Variant | Window | Candles | Trades | Win Rate | Return | Max DD | PF | Avg R | Confirmed | Switches | Avg Hold | rail_broken Exit | Dominant Mix |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for row in rows:
        lines.append(
            f"| {row.variant_label} | {row.window_label} | {row.candle_count} | {row.total_trades} | "
            f"{row.win_rate}% | {row.total_return_pct}% | {row.max_drawdown_pct}% | {row.profit_factor} | {row.average_r_multiple} | "
            f"{_fmt_pct_cell(row.confirmed_coverage_pct)} | {row.rail_switches} | {row.average_hold_bars} | {_fmt_pct_cell(row.rail_broken_exit_pct)} | {row.dominant_period_mix} |"
        )

    lines.extend(
        [
            "",
            "## Notes",
            "",
            "1. `Adaptive Rail Current 4H` already behaves like a low-drawdown structure-following system.",
            "Its edge is not maximum total return. The useful part is trade quality and drawdown control.",
            "",
            "2. `Adaptive Rail Return 4H` is the strongest full-history return variant in this study.",
            "The main changes are `rail_reclaim_bars=1` and `rail_switch_min_score_delta=4`, which make the rail switch and failure logic more aggressive.",
            "",
            "3. `Adaptive Rail Balanced 4H` looks like the most promising refinement candidate.",
            "It does not win every metric, but its post-2024 shape is smoother and easier to keep iterating on.",
            "",
            "4. The new structure stats help explain why.",
            "The better 4H variants are not just profitable. They also keep switch counts moderate, dominant rails alive for longer, and `rail_broken` exits under control.",
            "",
            "5. The useful tuning axis is coordination between rail failure and stop width.",
            "The best improvements did not come from chasing more trades. They came from making rail invalidation and stop placement fit each other better.",
        ]
    )
    return "\n".join(lines)


def _variant_payload(variant: Variant) -> dict[str, object]:
    payload = asdict(variant)
    payload["atr_stop_multiplier"] = str(variant.atr_stop_multiplier)
    payload["atr_take_multiplier"] = str(variant.atr_take_multiplier)
    payload["rail_break_atr_ratio"] = str(variant.rail_break_atr_ratio)
    payload["rail_switch_min_score_delta"] = str(variant.rail_switch_min_score_delta)
    return payload


def main() -> None:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = analysis_report_dir_path()
    out_dir.mkdir(parents=True, exist_ok=True)
    md_path = out_dir / f"adaptive_ema_rail_4h_tuning_study_{stamp}.md"
    json_path = out_dir / f"adaptive_ema_rail_4h_tuning_study_{stamp}.json"

    client = OkxRestClient()
    instrument = client.get_instrument(SYMBOL)
    all_candles = [candle for candle in load_candle_cache(SYMBOL, BAR, limit=None) if candle.confirmed]

    rows: list[ResultRow] = []
    for variant in VARIANTS:
        for window in WINDOWS:
            print(f"run {variant.key} {window.key}", flush=True)
            rows.append(_run_variant(instrument, all_candles, variant, window))

    md_path.write_text(_build_markdown(rows), encoding="utf-8")
    json_path.write_text(
        json.dumps(
            {
                "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
                "symbol": SYMBOL,
                "bar": BAR,
                "variants": [_variant_payload(variant) for variant in VARIANTS],
                "rows": [asdict(row) for row in rows],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    print(md_path)
    print(json_path)


if __name__ == "__main__":
    main()
