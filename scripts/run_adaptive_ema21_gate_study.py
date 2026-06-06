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
from okx_quant.strategy_catalog import STRATEGY_ADAPTIVE_EMA_RAIL_LONG_ID


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
    candidate_periods: tuple[int, ...]
    fast_gate_enabled: bool = False
    fast_min_gap_ema200_atr: Decimal = Decimal("0")
    fast_min_spread_trend_atr: Decimal = Decimal("0")
    fast_max_recent_range_atr: Decimal = Decimal("0")
    fast_recent_range_bars: int = 8


@dataclass(frozen=True)
class ResultRow:
    variant_key: str
    variant_label: str
    window_key: str
    window_label: str
    total_trades: int
    ema21_trades: int
    total_return_pct: str
    max_drawdown_pct: str
    profit_factor: str
    average_r_multiple: str
    ema21_contribution_pct: str
    confirmed_coverage_pct: str
    rail_switches: str


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
        key="core_baseline",
        label="Core 21/34/55/89",
        candidate_periods=(21, 34, 55, 89),
    ),
    Variant(
        key="core_ema21_gated",
        label="Core 21/34/55/89 + EMA21 Gate",
        candidate_periods=(21, 34, 55, 89),
        fast_gate_enabled=True,
        fast_min_gap_ema200_atr=Decimal("5.0"),
        fast_min_spread_trend_atr=Decimal("1.5"),
        fast_max_recent_range_atr=Decimal("3.0"),
        fast_recent_range_bars=8,
    ),
    Variant(
        key="mid_baseline",
        label="Mid 34/55/89",
        candidate_periods=(34, 55, 89),
    ),
)


def _fmt(value: Decimal | None, digits: int = 4) -> str:
    if value is None:
        return "-"
    return format_decimal_fixed(value, digits)


def _build_config(variant: Variant) -> StrategyConfig:
    return StrategyConfig(
        inst_id=SYMBOL,
        bar=BAR,
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
        strategy_id=STRATEGY_ADAPTIVE_EMA_RAIL_LONG_ID,
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
        rail_candidate_ema_periods=variant.candidate_periods,
        rail_fast_gate_enabled=variant.fast_gate_enabled,
        rail_fast_gate_period=21,
        rail_fast_min_gap_ema200_atr=variant.fast_min_gap_ema200_atr,
        rail_fast_min_spread_trend_atr=variant.fast_min_spread_trend_atr,
        rail_fast_max_recent_range_atr=variant.fast_max_recent_range_atr,
        rail_fast_recent_range_bars=variant.fast_recent_range_bars,
    )


def _run_variant(instrument, all_candles: list, window: Window, variant: Variant) -> ResultRow:
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
    ema21_trades = [trade for trade in result.trades if trade.adaptive_rail_period == 21]
    ema21_contribution = sum((trade.pnl for trade in ema21_trades), Decimal("0"))
    ema21_contribution_pct = (
        Decimal("0") if INITIAL_CAPITAL <= 0 else (ema21_contribution / INITIAL_CAPITAL) * Decimal("100")
    )
    return ResultRow(
        variant_key=variant.key,
        variant_label=variant.label,
        window_key=window.key,
        window_label=window.label,
        total_trades=report.total_trades,
        ema21_trades=len(ema21_trades),
        total_return_pct=_fmt(report.total_return_pct, 2),
        max_drawdown_pct=_fmt(report.max_drawdown_pct, 2),
        profit_factor=_fmt(report.profit_factor, 4),
        average_r_multiple=_fmt(report.average_r_multiple, 4),
        ema21_contribution_pct=_fmt(ema21_contribution_pct, 2),
        confirmed_coverage_pct=(
            _fmt(adaptive_stats.confirmed_coverage_pct, 2) if adaptive_stats is not None else "-"
        ),
        rail_switches=str(adaptive_stats.dominant_rail_switches) if adaptive_stats is not None else "-",
    )


def _build_markdown(rows: list[ResultRow]) -> str:
    lines = [
        "# Adaptive EMA21 Gate Study",
        "",
        f"- Symbol: `{SYMBOL}`",
        f"- Bar: `{BAR}`",
        "- Base template: Balanced 4H",
        "- Gate heuristic: `close-EMA200 >= 5 ATR`, `EMA21-EMA55 >= 1.5 ATR`, `recent 8-bar range <= 3 ATR`",
        "",
        "## Summary",
        "",
        "| Variant | Window | Trades | EMA21 Trades | Return | Max DD | PF | Avg R | EMA21 Contribution | Confirmed | Switches |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in rows:
        lines.append(
            f"| {row.variant_label} | {row.window_label} | {row.total_trades} | {row.ema21_trades} | "
            f"{row.total_return_pct}% | {row.max_drawdown_pct}% | {row.profit_factor} | {row.average_r_multiple} | "
            f"{row.ema21_contribution_pct}% | {row.confirmed_coverage_pct}% | {row.rail_switches} |"
        )

    lines.extend(
        [
            "",
            "## Reading Guide",
            "",
            "1. Compare `Core` vs `Core + EMA21 Gate` first.",
            "If return improves while EMA21 trade count falls, the gate is removing low-quality fast-rail entries rather than killing the entire edge.",
            "",
            "2. Compare `Core + EMA21 Gate` vs `Mid 34/55/89` next.",
            "If gated Core still beats Mid, EMA21 remains additive after filtering.",
        ]
    )
    return "\n".join(lines)


def _variant_payload(variant: Variant) -> dict[str, object]:
    return {
        "key": variant.key,
        "label": variant.label,
        "candidate_periods": list(variant.candidate_periods),
        "fast_gate_enabled": variant.fast_gate_enabled,
        "fast_min_gap_ema200_atr": str(variant.fast_min_gap_ema200_atr),
        "fast_min_spread_trend_atr": str(variant.fast_min_spread_trend_atr),
        "fast_max_recent_range_atr": str(variant.fast_max_recent_range_atr),
        "fast_recent_range_bars": variant.fast_recent_range_bars,
    }


def main() -> None:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = analysis_report_dir_path()
    out_dir.mkdir(parents=True, exist_ok=True)
    md_path = out_dir / f"adaptive_ema21_gate_study_{stamp}.md"
    json_path = out_dir / f"adaptive_ema21_gate_study_{stamp}.json"

    client = OkxRestClient()
    instrument = client.get_instrument(SYMBOL)
    all_candles = [candle for candle in load_candle_cache(SYMBOL, BAR, limit=None) if candle.confirmed]

    rows: list[ResultRow] = []
    for variant in VARIANTS:
        for window in WINDOWS:
            print(f"run {variant.key} {window.key}", flush=True)
            rows.append(_run_variant(instrument, all_candles, window, variant))

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
