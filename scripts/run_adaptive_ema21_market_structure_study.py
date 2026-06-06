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

from okx_quant.backtest import BacktestResult, BacktestTrade, _run_backtest_with_loaded_data
from okx_quant.candle_cache import load_candle_cache
from okx_quant.indicators import atr, ema
from okx_quant.models import Candle, StrategyConfig
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
SLOPE_LOOKBACK = 10
ATR_PERCENTILE_LOOKBACK = 100


@dataclass(frozen=True)
class Window:
    key: str
    label: str
    start_ts: int


@dataclass(frozen=True)
class FeatureSummaryRow:
    window_key: str
    window_label: str
    scope: str
    trades: int
    win_rate: str
    avg_r: str
    avg_gap_ema200_atr: str
    avg_ema55_slope_atr: str
    avg_ema200_slope_atr: str
    avg_ema21_ema55_spread_atr: str
    avg_recent_range_atr: str
    avg_atr_percentile_100: str


@dataclass(frozen=True)
class Ema21TradeDetail:
    window_key: str
    ts: str
    pnl: str
    r_multiple: str
    exit_reason: str
    gap_ema200_atr: str
    ema55_slope_atr: str
    ema200_slope_atr: str
    ema21_ema55_spread_atr: str
    recent_range_atr: str
    atr_percentile_100: str


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


def _fmt(value: Decimal | None, digits: int = 4) -> str:
    if value is None:
        return "-"
    return format_decimal_fixed(value, digits)


def _average(values: list[Decimal]) -> Decimal | None:
    if not values:
        return None
    return sum(values, Decimal("0")) / Decimal(len(values))


def _build_config() -> StrategyConfig:
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
        rail_candidate_ema_periods=(21, 34, 55, 89),
    )


def _timestamp_label(ts: int) -> str:
    return datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


def _atr_percentile(atr_values: list[Decimal | None], index: int, lookback: int) -> Decimal | None:
    current = atr_values[index]
    if current is None:
        return None
    start = max(0, index - lookback + 1)
    window = [item for item in atr_values[start : index + 1] if item is not None]
    if not window:
        return None
    less_equal = sum(1 for item in window if item <= current)
    return (Decimal(less_equal) / Decimal(len(window))) * Decimal("100")


def _recent_range_atr(candles: list[Candle], index: int, atr_value: Decimal | None, bars: int = 8) -> Decimal | None:
    if atr_value is None or atr_value <= 0:
        return None
    start = max(0, index - bars + 1)
    window = candles[start : index + 1]
    if not window:
        return None
    value = max(candle.high for candle in window) - min(candle.low for candle in window)
    return value / atr_value


def _slope_atr(ma_values: list[Decimal], index: int, atr_value: Decimal | None, lookback: int = SLOPE_LOOKBACK) -> Decimal | None:
    if atr_value is None or atr_value <= 0 or index < lookback:
        return None
    return (ma_values[index] - ma_values[index - lookback]) / atr_value


def _trade_features(
    result: BacktestResult,
    trade: BacktestTrade,
    ema21_values: list[Decimal],
    ema55_values: list[Decimal],
    ema200_values: list[Decimal],
    atr_values: list[Decimal | None],
) -> dict[str, Decimal]:
    index = trade.entry_index
    candle = result.candles[index]
    atr_value = atr_values[index]
    if atr_value is None or atr_value <= 0:
        return {}

    features: dict[str, Decimal] = {}
    features["gap_ema200_atr"] = (candle.close - ema200_values[index]) / atr_value
    ema55_slope = _slope_atr(ema55_values, index, atr_value)
    ema200_slope = _slope_atr(ema200_values, index, atr_value)
    recent_range = _recent_range_atr(result.candles, index, atr_value)
    atr_pct = _atr_percentile(atr_values, index, ATR_PERCENTILE_LOOKBACK)
    if ema55_slope is not None:
        features["ema55_slope_atr"] = ema55_slope
    if ema200_slope is not None:
        features["ema200_slope_atr"] = ema200_slope
    features["ema21_ema55_spread_atr"] = (ema21_values[index] - ema55_values[index]) / atr_value
    if recent_range is not None:
        features["recent_range_atr"] = recent_range
    if atr_pct is not None:
        features["atr_percentile_100"] = atr_pct
    return features


def _summarize_scope(
    window: Window,
    scope: str,
    trades: list[BacktestTrade],
    result: BacktestResult,
    ema21_values: list[Decimal],
    ema55_values: list[Decimal],
    ema200_values: list[Decimal],
    atr_values: list[Decimal | None],
) -> FeatureSummaryRow:
    wins = [trade for trade in trades if trade.pnl > 0]
    avg_r = _average([trade.r_multiple for trade in trades])
    win_rate = None if not trades else (Decimal(len(wins)) / Decimal(len(trades))) * Decimal("100")

    feature_map: dict[str, list[Decimal]] = {
        "gap_ema200_atr": [],
        "ema55_slope_atr": [],
        "ema200_slope_atr": [],
        "ema21_ema55_spread_atr": [],
        "recent_range_atr": [],
        "atr_percentile_100": [],
    }
    for trade in trades:
        features = _trade_features(result, trade, ema21_values, ema55_values, ema200_values, atr_values)
        for key, value in features.items():
            feature_map[key].append(value)

    return FeatureSummaryRow(
        window_key=window.key,
        window_label=window.label,
        scope=scope,
        trades=len(trades),
        win_rate=_fmt(win_rate, 2),
        avg_r=_fmt(avg_r, 4),
        avg_gap_ema200_atr=_fmt(_average(feature_map["gap_ema200_atr"]), 2),
        avg_ema55_slope_atr=_fmt(_average(feature_map["ema55_slope_atr"]), 2),
        avg_ema200_slope_atr=_fmt(_average(feature_map["ema200_slope_atr"]), 2),
        avg_ema21_ema55_spread_atr=_fmt(_average(feature_map["ema21_ema55_spread_atr"]), 2),
        avg_recent_range_atr=_fmt(_average(feature_map["recent_range_atr"]), 2),
        avg_atr_percentile_100=_fmt(_average(feature_map["atr_percentile_100"]), 2),
    )


def _run_window(instrument, all_candles: list[Candle], window: Window) -> tuple[BacktestResult, list[Decimal], list[Decimal], list[Decimal], list[Decimal | None]]:
    candles = [candle for candle in all_candles if candle.ts >= window.start_ts]
    result = _run_backtest_with_loaded_data(
        candles,
        instrument,
        _build_config(),
        data_source_note=f"local candle_cache full history | {SYMBOL} {BAR} | candles={len(candles)}",
        maker_fee_rate=MAKER_FEE,
        taker_fee_rate=TAKER_FEE,
    )
    closes = [candle.close for candle in candles]
    ema21_values = ema(closes, 21)
    ema55_values = ema(closes, 55)
    ema200_values = ema(closes, 200)
    atr_values = atr(candles, 10)
    return result, ema21_values, ema55_values, ema200_values, atr_values


def _build_markdown(summary_rows: list[FeatureSummaryRow], details: list[Ema21TradeDetail]) -> str:
    lines = [
        "# Adaptive EMA21 Market Structure Study",
        "",
        f"- Symbol: `{SYMBOL}`",
        f"- Bar: `{BAR}`",
        "- Variant: Balanced 4H with candidate pool `21/34/55/89`",
        "- Goal: identify the entry-time market structure where `EMA21` becomes useful",
        "",
        "## Feature Summary",
        "",
        "| Window | Scope | Trades | Win Rate | Avg R | Gap vs EMA200 (ATR) | EMA55 Slope (ATR/10 bars) | EMA200 Slope (ATR/10 bars) | EMA21-EMA55 Spread (ATR) | Recent 8-Bar Range (ATR) | ATR Percentile 100 |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in summary_rows:
        lines.append(
            f"| {row.window_label} | {row.scope} | {row.trades} | {row.win_rate}% | {row.avg_r} | "
            f"{row.avg_gap_ema200_atr} | {row.avg_ema55_slope_atr} | {row.avg_ema200_slope_atr} | "
            f"{row.avg_ema21_ema55_spread_atr} | {row.avg_recent_range_atr} | {row.avg_atr_percentile_100}% |"
        )

    lines.extend(
        [
            "",
            "## EMA21 Trade Details",
            "",
            "| Window | Date | PnL | Avg R | Exit | Gap vs EMA200 | EMA55 Slope | EMA200 Slope | EMA21-EMA55 Spread | Recent Range | ATR Percentile 100 |",
            "| --- | --- | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in details:
        lines.append(
            f"| {row.window_key} | {row.ts} | {row.pnl} | {row.r_multiple} | {row.exit_reason} | "
            f"{row.gap_ema200_atr} | {row.ema55_slope_atr} | {row.ema200_slope_atr} | "
            f"{row.ema21_ema55_spread_atr} | {row.recent_range_atr} | {row.atr_percentile_100}% |"
        )

    lines.extend(
        [
            "",
            "## Reading Guide",
            "",
            "1. `Gap vs EMA200` larger means price is already stretched farther above the long trend anchor.",
            "2. `EMA21-EMA55 Spread` larger means the fast rail has clearly separated from the medium rail.",
            "3. `Recent 8-Bar Range` and `ATR Percentile 100` together show whether EMA21 trades appear during expansion phases rather than quiet drifts.",
        ]
    )
    return "\n".join(lines)


def main() -> None:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = analysis_report_dir_path()
    out_dir.mkdir(parents=True, exist_ok=True)
    md_path = out_dir / f"adaptive_ema21_market_structure_study_{stamp}.md"
    json_path = out_dir / f"adaptive_ema21_market_structure_study_{stamp}.json"

    client = OkxRestClient()
    instrument = client.get_instrument(SYMBOL)
    all_candles = [candle for candle in load_candle_cache(SYMBOL, BAR, limit=None) if candle.confirmed]

    summary_rows: list[FeatureSummaryRow] = []
    detail_rows: list[Ema21TradeDetail] = []

    for window in WINDOWS:
        print(f"run {window.key}", flush=True)
        result, ema21_values, ema55_values, ema200_values, atr_values = _run_window(instrument, all_candles, window)
        ema21_trades = [trade for trade in result.trades if trade.adaptive_rail_period == 21]
        non_ema21_trades = [trade for trade in result.trades if trade.adaptive_rail_period != 21]

        summary_rows.append(
            _summarize_scope(
                window,
                "EMA21 Trades",
                ema21_trades,
                result,
                ema21_values,
                ema55_values,
                ema200_values,
                atr_values,
            )
        )
        summary_rows.append(
            _summarize_scope(
                window,
                "Non-EMA21 Trades",
                non_ema21_trades,
                result,
                ema21_values,
                ema55_values,
                ema200_values,
                atr_values,
            )
        )

        for trade in ema21_trades:
            features = _trade_features(result, trade, ema21_values, ema55_values, ema200_values, atr_values)
            detail_rows.append(
                Ema21TradeDetail(
                    window_key=window.label,
                    ts=_timestamp_label(trade.entry_ts),
                    pnl=_fmt(trade.pnl, 4),
                    r_multiple=_fmt(trade.r_multiple, 4),
                    exit_reason=trade.exit_reason,
                    gap_ema200_atr=_fmt(features.get("gap_ema200_atr"), 2),
                    ema55_slope_atr=_fmt(features.get("ema55_slope_atr"), 2),
                    ema200_slope_atr=_fmt(features.get("ema200_slope_atr"), 2),
                    ema21_ema55_spread_atr=_fmt(features.get("ema21_ema55_spread_atr"), 2),
                    recent_range_atr=_fmt(features.get("recent_range_atr"), 2),
                    atr_percentile_100=_fmt(features.get("atr_percentile_100"), 2),
                )
            )

    md_path.write_text(_build_markdown(summary_rows, detail_rows), encoding="utf-8")
    json_path.write_text(
        json.dumps(
            {
                "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
                "symbol": SYMBOL,
                "bar": BAR,
                "summary_rows": [asdict(row) for row in summary_rows],
                "detail_rows": [asdict(row) for row in detail_rows],
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
