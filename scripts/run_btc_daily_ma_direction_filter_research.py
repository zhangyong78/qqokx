from __future__ import annotations

import csv
import json
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from okx_quant.backtest import BacktestTrade, _build_report, _run_backtest_with_loaded_data
from okx_quant.candle_cache import load_candle_cache
from okx_quant.indicators import moving_average
from okx_quant.models import Candle, StrategyConfig
from okx_quant.okx_client import OkxRestClient
from okx_quant.persistence import analysis_report_dir_path
from okx_quant.pricing import format_decimal_fixed
from okx_quant.strategy_catalog import STRATEGY_DYNAMIC_LONG_ID, STRATEGY_EMA55_SLOPE_SHORT_ID
from okx_quant.timeframe import latest_closed_candle_index


REPORT_DIR = analysis_report_dir_path()
REPORT_DIR.mkdir(parents=True, exist_ok=True)

SYMBOL = "BTC-USDT-SWAP"
ENTRY_BAR = "1H"
FILTER_BAR = "1D"
INITIAL_CAPITAL = Decimal("10000")

LONG_MAKER_FEE_RATE = Decimal("0.0001")
LONG_TAKER_FEE_RATE = Decimal("0.00028")
SHORT_TAKER_FEE_RATE = Decimal("0.00036")
RISK_AMOUNT = Decimal("10")

STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
CSV_PATH = REPORT_DIR / f"btc_daily_ma_direction_filter_research_{STAMP}.csv"
JSON_PATH = REPORT_DIR / f"btc_daily_ma_direction_filter_research_{STAMP}.json"
MD_PATH = REPORT_DIR / f"btc_daily_ma_direction_filter_research_{STAMP}.md"


@dataclass(frozen=True)
class Variant:
    key: str
    label: str
    ma_type: str | None = None
    period: int | None = None


@dataclass(frozen=True)
class SplitBounds:
    name: str
    start_index: int
    end_index: int


@dataclass(frozen=True)
class SplitMetrics:
    pnl: Decimal
    trades: int
    win_rate: Decimal
    profit_factor: Decimal | None
    avg_r: Decimal
    max_drawdown: Decimal
    return_pct: Decimal


@dataclass(frozen=True)
class BiasStats:
    long_bars: int
    short_bars: int
    neutral_bars: int

    @property
    def total(self) -> int:
        return self.long_bars + self.short_bars + self.neutral_bars


@dataclass(frozen=True)
class VariantResult:
    variant: Variant
    bias_stats: BiasStats
    long_all: SplitMetrics
    long_test: SplitMetrics
    short_all: SplitMetrics
    short_test: SplitMetrics
    combined_all: SplitMetrics
    combined_test: SplitMetrics


VARIANTS = (
    Variant(key="baseline", label="无日线过滤"),
    Variant(key="ema_5", label="日线 EMA5", ma_type="ema", period=5),
    Variant(key="ema_8", label="日线 EMA8", ma_type="ema", period=8),
    Variant(key="ema_13", label="日线 EMA13", ma_type="ema", period=13),
    Variant(key="ema_21", label="日线 EMA21", ma_type="ema", period=21),
    Variant(key="ema_55", label="日线 EMA55", ma_type="ema", period=55),
    Variant(key="ma_5", label="日线 MA5", ma_type="ma", period=5),
    Variant(key="ma_8", label="日线 MA8", ma_type="ma", period=8),
    Variant(key="ma_13", label="日线 MA13", ma_type="ma", period=13),
    Variant(key="ma_21", label="日线 MA21", ma_type="ma", period=21),
    Variant(key="ma_55", label="日线 MA55", ma_type="ma", period=55),
)


def main() -> None:
    entry_candles = [candle for candle in load_candle_cache(SYMBOL, ENTRY_BAR, limit=None) if candle.confirmed]
    filter_candles = [candle for candle in load_candle_cache(SYMBOL, FILTER_BAR, limit=None) if candle.confirmed]
    if not entry_candles or not filter_candles:
        raise RuntimeError(f"missing local candles for {SYMBOL} {ENTRY_BAR}/{FILTER_BAR}")

    client = OkxRestClient()
    instrument = client.get_instrument(SYMBOL)
    bounds = build_split_bounds(len(entry_candles))

    results: list[VariantResult] = []
    for variant in VARIANTS:
        print(f"run {variant.label}")
        bias = build_daily_direction_bias(entry_candles, filter_candles, variant) if variant.period else None
        bias_stats = summarize_bias(bias, len(entry_candles))

        long_result = _run_backtest_with_loaded_data(
            entry_candles,
            instrument,
            build_long_config(),
            data_source_note=build_data_note(entry_candles, filter_candles),
            maker_fee_rate=LONG_MAKER_FEE_RATE,
            taker_fee_rate=LONG_TAKER_FEE_RATE,
            direction_filter_bias=bias,
        )
        short_result = _run_backtest_with_loaded_data(
            entry_candles,
            instrument,
            build_short_config(),
            data_source_note=build_data_note(entry_candles, filter_candles),
            taker_fee_rate=SHORT_TAKER_FEE_RATE,
            direction_filter_bias=bias,
        )

        combined_trades = sorted(
            [*long_result.trades, *short_result.trades],
            key=lambda trade: (trade.exit_ts, trade.entry_ts, trade.signal),
        )

        results.append(
            VariantResult(
                variant=variant,
                bias_stats=bias_stats,
                long_all=build_metrics(long_result.trades),
                long_test=build_metrics(filter_split_trades(long_result.trades, bounds["test"])),
                short_all=build_metrics(short_result.trades),
                short_test=build_metrics(filter_split_trades(short_result.trades, bounds["test"])),
                combined_all=build_metrics(combined_trades),
                combined_test=build_metrics(filter_split_trades(combined_trades, bounds["test"])),
            )
        )

    export_csv(CSV_PATH, results)
    summary = build_summary_payload(entry_candles, filter_candles, results)
    JSON_PATH.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    MD_PATH.write_text(build_markdown(entry_candles, filter_candles, results), encoding="utf-8")
    print(MD_PATH)


def build_long_config() -> StrategyConfig:
    return StrategyConfig(
        inst_id=SYMBOL,
        bar=ENTRY_BAR,
        ema_period=21,
        ema_type="ema",
        trend_ema_period=50,
        trend_ema_type="ma",
        entry_reference_ema_period=50,
        entry_reference_ema_type="ma",
        big_ema_period=233,
        atr_period=10,
        atr_stop_multiplier=Decimal("2"),
        atr_take_multiplier=Decimal("2"),
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
        take_profit_mode="dynamic",
        max_entries_per_trend=1,
        dynamic_two_r_break_even=True,
        dynamic_fee_offset_enabled=True,
        trend_ema_slope_filter_enabled=False,
        time_stop_break_even_enabled=False,
        time_stop_break_even_bars=0,
    )


def build_short_config() -> StrategyConfig:
    return StrategyConfig(
        inst_id=SYMBOL,
        bar=ENTRY_BAR,
        ema_period=55,
        ema_type="ema",
        trend_ema_period=55,
        trend_ema_type="ema",
        atr_period=14,
        atr_stop_multiplier=Decimal("2"),
        atr_take_multiplier=Decimal("4"),
        order_size=Decimal("0"),
        trade_mode="cross",
        signal_mode="short_only",
        position_mode="net",
        environment="demo",
        tp_sl_trigger_type="mark",
        strategy_id=STRATEGY_EMA55_SLOPE_SHORT_ID,
        risk_amount=RISK_AMOUNT,
        backtest_initial_capital=INITIAL_CAPITAL,
        backtest_sizing_mode="fixed_risk",
        take_profit_mode="dynamic",
        dynamic_two_r_break_even=True,
        dynamic_fee_offset_enabled=True,
        ema55_slope_exit_enabled=False,
        atr_percentile_filter_max=Decimal("0.5"),
        trend_ema_slope_filter_min_ratio=Decimal("-0.0005"),
        time_stop_break_even_enabled=False,
        time_stop_break_even_bars=10,
    )


def build_daily_direction_bias(
    entry_candles: list[Candle],
    filter_candles: list[Candle],
    variant: Variant,
) -> list[str]:
    if not variant.period or not variant.ma_type:
        return ["neutral"] * len(entry_candles)

    closes = [candle.close for candle in filter_candles]
    line_values = moving_average(closes, int(variant.period), variant.ma_type)
    out: list[str] = []
    for entry_candle in entry_candles:
        idx = latest_closed_candle_index(filter_candles, entry_candle.ts)
        if idx < 0 or idx >= len(line_values):
            out.append("neutral")
            continue
        line = line_values[idx]
        if line is None:
            out.append("neutral")
            continue
        close = filter_candles[idx].close
        if close > line:
            out.append("long")
        elif close < line:
            out.append("short")
        else:
            out.append("neutral")
    return out


def summarize_bias(bias: list[str] | None, total_bars: int) -> BiasStats:
    if bias is None:
        return BiasStats(long_bars=total_bars, short_bars=total_bars, neutral_bars=0)
    long_bars = sum(1 for item in bias if item == "long")
    short_bars = sum(1 for item in bias if item == "short")
    neutral_bars = len(bias) - long_bars - short_bars
    return BiasStats(long_bars=long_bars, short_bars=short_bars, neutral_bars=neutral_bars)


def build_split_bounds(length: int) -> dict[str, SplitBounds]:
    train_end = int(length * 0.6) - 1
    validation_end = int(length * 0.8) - 1
    return {
        "train": SplitBounds("train", 0, train_end),
        "validation": SplitBounds("validation", train_end + 1, validation_end),
        "test": SplitBounds("test", validation_end + 1, length - 1),
    }


def filter_split_trades(trades: list[BacktestTrade], bounds: SplitBounds) -> list[BacktestTrade]:
    return [trade for trade in trades if bounds.start_index <= trade.exit_index <= bounds.end_index]


def build_metrics(trades: list[BacktestTrade]) -> SplitMetrics:
    report = _build_report(trades, initial_capital=INITIAL_CAPITAL)
    return SplitMetrics(
        pnl=report.total_pnl,
        trades=report.total_trades,
        win_rate=report.win_rate,
        profit_factor=report.profit_factor,
        avg_r=report.average_r_multiple,
        max_drawdown=report.max_drawdown,
        return_pct=report.total_return_pct,
    )


def build_data_note(entry_candles: list[Candle], filter_candles: list[Candle]) -> str:
    return (
        f"local candle_cache full history | {SYMBOL} {ENTRY_BAR} candles={len(entry_candles)} | "
        f"{FILTER_BAR} candles={len(filter_candles)}"
    )


def format_ts(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def export_csv(path: Path, results: list[VariantResult]) -> None:
    baseline = results[0]
    with path.open("w", newline="", encoding="utf-8-sig") as fp:
        writer = csv.writer(fp)
        writer.writerow(
            [
                "variant_key",
                "variant_label",
                "daily_ma_type",
                "daily_period",
                "bias_long_bars",
                "bias_short_bars",
                "bias_neutral_bars",
                "long_all_pnl",
                "long_all_trades",
                "long_all_win_rate",
                "long_all_pf",
                "long_all_avg_r",
                "long_all_drawdown",
                "long_test_pnl",
                "long_test_trades",
                "short_all_pnl",
                "short_all_trades",
                "short_all_win_rate",
                "short_all_pf",
                "short_all_avg_r",
                "short_all_drawdown",
                "short_test_pnl",
                "short_test_trades",
                "combined_all_pnl",
                "combined_all_trades",
                "combined_all_win_rate",
                "combined_all_pf",
                "combined_all_avg_r",
                "combined_all_drawdown",
                "combined_all_delta_vs_baseline",
                "combined_test_pnl",
                "combined_test_trades",
                "combined_test_win_rate",
                "combined_test_pf",
                "combined_test_avg_r",
                "combined_test_drawdown",
                "combined_test_delta_vs_baseline",
            ]
        )
        for item in results:
            writer.writerow(
                [
                    item.variant.key,
                    item.variant.label,
                    item.variant.ma_type or "",
                    item.variant.period or "",
                    item.bias_stats.long_bars,
                    item.bias_stats.short_bars,
                    item.bias_stats.neutral_bars,
                    fmt(item.long_all.pnl),
                    item.long_all.trades,
                    fmt(item.long_all.win_rate),
                    fmt_or_blank(item.long_all.profit_factor),
                    fmt(item.long_all.avg_r),
                    fmt(item.long_all.max_drawdown),
                    fmt(item.long_test.pnl),
                    item.long_test.trades,
                    fmt(item.short_all.pnl),
                    item.short_all.trades,
                    fmt(item.short_all.win_rate),
                    fmt_or_blank(item.short_all.profit_factor),
                    fmt(item.short_all.avg_r),
                    fmt(item.short_all.max_drawdown),
                    fmt(item.short_test.pnl),
                    item.short_test.trades,
                    fmt(item.combined_all.pnl),
                    item.combined_all.trades,
                    fmt(item.combined_all.win_rate),
                    fmt_or_blank(item.combined_all.profit_factor),
                    fmt(item.combined_all.avg_r),
                    fmt(item.combined_all.max_drawdown),
                    fmt(item.combined_all.pnl - baseline.combined_all.pnl),
                    fmt(item.combined_test.pnl),
                    item.combined_test.trades,
                    fmt(item.combined_test.win_rate),
                    fmt_or_blank(item.combined_test.profit_factor),
                    fmt(item.combined_test.avg_r),
                    fmt(item.combined_test.max_drawdown),
                    fmt(item.combined_test.pnl - baseline.combined_test.pnl),
                ]
            )


def build_summary_payload(
    entry_candles: list[Candle],
    filter_candles: list[Candle],
    results: list[VariantResult],
) -> dict[str, object]:
    baseline = results[0]
    non_baseline = results[1:]
    best_all = max(non_baseline, key=lambda item: item.combined_all.pnl)
    best_test = max(non_baseline, key=lambda item: item.combined_test.pnl)
    return {
        "symbol": SYMBOL,
        "entry_bar": ENTRY_BAR,
        "filter_bar": FILTER_BAR,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "sample": {
            "entry_candles": len(entry_candles),
            "filter_candles": len(filter_candles),
            "start_utc": format_ts(entry_candles[0].ts),
            "end_utc": format_ts(entry_candles[-1].ts),
        },
        "assumption": {
            "rule": "日线收盘 > 日线均线只允许做多；日线收盘 < 日线均线只允许做空；相等/均线未就绪则不开新仓",
            "long_strategy": "1H EMA动态委托做多，EMA21 + MA50，挂单参考MA50，2ATR止损，动态止盈，2R保本，10U风险",
            "short_strategy": "1H EMA55斜率做空，阈值-0.0005，ATR14，2ATR止损，动态止盈，2R保本，10U风险",
        },
        "baseline": variant_payload(baseline, baseline),
        "best_combined_all": variant_payload(best_all, baseline),
        "best_combined_test": variant_payload(best_test, baseline),
        "variants": [variant_payload(item, baseline) for item in results],
    }


def variant_payload(item: VariantResult, baseline: VariantResult) -> dict[str, object]:
    return {
        "variant": asdict(item.variant),
        "bias_stats": asdict(item.bias_stats),
        "long_all": split_payload(item.long_all),
        "long_test": split_payload(item.long_test),
        "short_all": split_payload(item.short_all),
        "short_test": split_payload(item.short_test),
        "combined_all": split_payload(item.combined_all),
        "combined_test": split_payload(item.combined_test),
        "delta_vs_baseline": {
            "combined_all_pnl": str(item.combined_all.pnl - baseline.combined_all.pnl),
            "combined_test_pnl": str(item.combined_test.pnl - baseline.combined_test.pnl),
            "combined_all_trades": item.combined_all.trades - baseline.combined_all.trades,
            "combined_test_trades": item.combined_test.trades - baseline.combined_test.trades,
        },
    }


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


def build_markdown(
    entry_candles: list[Candle],
    filter_candles: list[Candle],
    results: list[VariantResult],
) -> str:
    baseline = results[0]
    ranked_test = sorted(results[1:], key=lambda item: item.combined_test.pnl, reverse=True)
    ranked_all = sorted(results[1:], key=lambda item: item.combined_all.pnl, reverse=True)
    lines = [
        "# BTC 1H 多空组合 + 日线均线方向闸门研究",
        "",
        f"- 标的：`{SYMBOL}`",
        f"- 低周期：`{ENTRY_BAR}`，高周期过滤：`{FILTER_BAR}`",
        f"- 样本区间：{format_ts(entry_candles[0].ts)} -> {format_ts(entry_candles[-1].ts)}",
        f"- 1H 样本数：{len(entry_candles)}，1D 样本数：{len(filter_candles)}",
        "- 过滤假设：日线收盘高于日线均线时只开多，低于日线均线时只开空，相等或均线未就绪时不新开仓。",
        "- 做多基线：1H EMA动态委托做多，EMA21 + MA50，挂单参考MA50，2ATR止损，动态止盈，2R保本，10U风险。",
        "- 做空基线：1H EMA55斜率做空，阈值=-0.0005，ATR14，2ATR止损，动态止盈，2R保本，10U风险。",
        f"- 结果文件：[CSV]({CSV_PATH}) | [JSON]({JSON_PATH})",
        "",
        "## 基线",
        "",
        (
            f"- 组合全样本：PnL {fmt(baseline.combined_all.pnl)} | Trades {baseline.combined_all.trades} | "
            f"WinRate {fmt(baseline.combined_all.win_rate)}% | PF {fmt_or_dash(baseline.combined_all.profit_factor)} | "
            f"AvgR {fmt(baseline.combined_all.avg_r)} | DD {fmt(baseline.combined_all.max_drawdown)}"
        ),
        (
            f"- 组合测试段：PnL {fmt(baseline.combined_test.pnl)} | Trades {baseline.combined_test.trades} | "
            f"WinRate {fmt(baseline.combined_test.win_rate)}% | PF {fmt_or_dash(baseline.combined_test.profit_factor)} | "
            f"AvgR {fmt(baseline.combined_test.avg_r)} | DD {fmt(baseline.combined_test.max_drawdown)}"
        ),
        "",
        "## 组合测试段排名",
        "",
        "| 方案 | 测试PnL | 相对基线 | 测试交易数 | 全样本PnL | 相对基线 | 日线Long/Short/Neutral |",
        "| --- | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for item in ranked_test:
        lines.append(
            f"| {item.variant.label} | {fmt(item.combined_test.pnl)} | {fmt(item.combined_test.pnl - baseline.combined_test.pnl)} | "
            f"{item.combined_test.trades} | {fmt(item.combined_all.pnl)} | {fmt(item.combined_all.pnl - baseline.combined_all.pnl)} | "
            f"{item.bias_stats.long_bars}/{item.bias_stats.short_bars}/{item.bias_stats.neutral_bars} |"
        )
    lines.extend(
        [
            "",
            "## 最优观察",
            "",
            (
                f"- 全样本最强：{ranked_all[0].variant.label}，组合PnL {fmt(ranked_all[0].combined_all.pnl)}，"
                f"较基线 {fmt(ranked_all[0].combined_all.pnl - baseline.combined_all.pnl)}。"
            ),
            (
                f"- 测试段最强：{ranked_test[0].variant.label}，组合PnL {fmt(ranked_test[0].combined_test.pnl)}，"
                f"较基线 {fmt(ranked_test[0].combined_test.pnl - baseline.combined_test.pnl)}。"
            ),
            (
                f"- 做多最佳测试段：{max(results[1:], key=lambda item: item.long_test.pnl).variant.label} | "
                f"PnL {fmt(max(results[1:], key=lambda item: item.long_test.pnl).long_test.pnl)}"
            ),
            (
                f"- 做空最佳测试段：{max(results[1:], key=lambda item: item.short_test.pnl).variant.label} | "
                f"PnL {fmt(max(results[1:], key=lambda item: item.short_test.pnl).short_test.pnl)}"
            ),
            "",
            "## 结论提示",
            "",
            "- 这份研究只回答“日线 close vs 均线 方向闸门”是否改善，不涉及重新调参低周期入场或出场。",
            "- 如果你认可其中某个日线闸门方向，我们下一步可以继续拆：只作用于做多、只作用于做空、或改成日线双均线排列过滤。",
            "",
        ]
    )
    return "\n".join(lines)


def fmt(value: Decimal) -> str:
    return format_decimal_fixed(value, 4)


def fmt_or_blank(value: Decimal | None) -> str:
    if value is None:
        return ""
    return fmt(value)


def fmt_or_dash(value: Decimal | None) -> str:
    if value is None:
        return "-"
    return fmt(value)


if __name__ == "__main__":
    main()
