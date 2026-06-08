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

from okx_quant.backtest import BacktestTrade, _run_backtest_with_loaded_data
from okx_quant.candle_cache import load_candle_cache
from okx_quant.models import StrategyConfig
from okx_quant.okx_client import OkxRestClient
from okx_quant.persistence import analysis_report_dir_path
from okx_quant.strategy_catalog import STRATEGY_EMA55_SLOPE_SHORT_ID
from scripts.run_btc_daily_ma_direction_filter_research import (
    ENTRY_BAR,
    INITIAL_CAPITAL,
    LONG_MAKER_FEE_RATE,
    LONG_TAKER_FEE_RATE,
    RISK_AMOUNT,
    SHORT_TAKER_FEE_RATE,
    SplitMetrics,
    build_metrics,
    build_split_bounds,
    filter_split_trades,
    format_ts,
)
from scripts.run_multi_coin_best_long_daily_gate_report import (
    LONG_PROFILES,
    SYMBOLS,
    SYMBOL_LABELS,
    build_long_config,
)


plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "Noto Sans CJK SC", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False


REPORT_DIR = analysis_report_dir_path()
REPORT_DIR.mkdir(parents=True, exist_ok=True)
PROJECT_REPORT_DIR = ROOT / "reports"
PROJECT_REPORT_DIR.mkdir(parents=True, exist_ok=True)

STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
BASENAME = f"multi_coin_long_plus_short_reentry_color_compare_5coins_10u_{STAMP}"
HTML_PATH = REPORT_DIR / f"{BASENAME}.html"
JSON_PATH = REPORT_DIR / f"{BASENAME}.json"
SUMMARY_CSV_PATH = REPORT_DIR / f"{BASENAME}.csv"
COIN_CSV_PATH = REPORT_DIR / f"{BASENAME}_by_coin.csv"
MONTHLY_CSV_PATH = REPORT_DIR / f"{BASENAME}_monthly.csv"
YEARLY_CSV_PATH = REPORT_DIR / f"{BASENAME}_yearly.csv"
COIN_MONTHLY_CSV_PATH = REPORT_DIR / f"{BASENAME}_monthly_by_coin.csv"
COIN_YEARLY_CSV_PATH = REPORT_DIR / f"{BASENAME}_yearly_by_coin.csv"
PROJECT_HTML_PATH = PROJECT_REPORT_DIR / "multi_coin_long_plus_short_reentry_color_compare_5coins_10u.html"


@dataclass(frozen=True)
class ShortVariant:
    key: str
    label: str
    note: str
    locked_reentry_ema21_near: bool = False
    locked_reentry_min_r: int = 0
    locked_reentry_max_r: int = 0
    dynamic_exit_bull_bar_requires_bear_reentry: bool = False
    dynamic_exit_bull_bar_reentry_min_r: int = 0
    dynamic_exit_bull_bar_reentry_max_r: int = 0


@dataclass(frozen=True)
class SideRun:
    symbol: str
    label: str
    trades: list[BacktestTrade]
    test_trades: list[BacktestTrade]
    all_metrics: SplitMetrics
    test_metrics: SplitMetrics


@dataclass(frozen=True)
class OverlapStats:
    pair_count: int
    long_trade_count: int
    short_trade_count: int


@dataclass(frozen=True)
class ComboRun:
    variant: ShortVariant
    symbol: str
    label: str
    trades: list[BacktestTrade]
    test_trades: list[BacktestTrade]
    all_metrics: SplitMetrics
    test_metrics: SplitMetrics
    overlap_all: OverlapStats
    overlap_test: OverlapStats


@dataclass(frozen=True)
class SymbolStudy:
    symbol: str
    label: str
    candle_count: int
    start_ts: int
    end_ts: int
    long_run: SideRun
    short_runs: dict[str, SideRun]
    combo_runs: dict[str, ComboRun]


@dataclass(frozen=True)
class AggregateRun:
    label: str
    trades: list[BacktestTrade]
    test_trades: list[BacktestTrade]
    all_metrics: SplitMetrics
    test_metrics: SplitMetrics


SHORT_VARIANTS = (
    ShortVariant(
        key="original",
        label="原做空",
        note="原始 EMA55 斜率做空：动态保护出场后，只要信号仍成立，就允许继续做空。",
    ),
    ShortVariant(
        key="ema21_reset",
        label="EMA21 重置做空",
        note="仅当 locked_2r_stop 出场时，必须先反抽接近 EMA21（<= 0.3 ATR），再跌回 EMA21 下方才允许再次做空。",
        locked_reentry_ema21_near=True,
        locked_reentry_min_r=2,
        locked_reentry_max_r=2,
    ),
    ShortVariant(
        key="bull_wait_bear",
        label="阳线等阴线做空",
        note="仅对保本/锁盈类动态保护出场生效：若平仓当根收阳，则必须等后续新的阴线且做空条件仍成立才允许再空；若平仓当根收阴，则按原逻辑继续。",
        dynamic_exit_bull_bar_requires_bear_reentry=True,
    ),
    ShortVariant(
        key="bull_wait_bear_locked_2r",
        label="2R阳线等阴线做空",
        note="仅当 locked_2r_stop 出场且平仓当根收阳时，后续必须等新的阴线且做空条件仍成立才允许再空；其他动态保护出场仍按原逻辑处理。",
        dynamic_exit_bull_bar_requires_bear_reentry=True,
        dynamic_exit_bull_bar_reentry_min_r=2,
        dynamic_exit_bull_bar_reentry_max_r=2,
    ),
)


def main() -> None:
    client = OkxRestClient()
    studies = [run_symbol_study(client, symbol) for symbol in SYMBOLS]

    aggregate_long = aggregate_side([study.long_run for study in studies], label="做多")
    aggregate_short = {
        variant.key: aggregate_side([study.short_runs[variant.key] for study in studies], label=variant.label)
        for variant in SHORT_VARIANTS
    }
    aggregate_combo = {
        variant.key: aggregate_combo_side([study.combo_runs[variant.key] for study in studies], label=f"做多 + {variant.label}")
        for variant in SHORT_VARIANTS
    }

    summary_frame = build_summary_frame(aggregate_long, aggregate_short, aggregate_combo)
    coin_frame = build_coin_frame(studies)
    monthly_frame = build_period_frame(aggregate_long, aggregate_short, aggregate_combo, period="month")
    yearly_frame = build_period_frame(aggregate_long, aggregate_short, aggregate_combo, period="year")
    coin_monthly_frame = build_coin_period_frame(studies, period="month")
    coin_yearly_frame = build_coin_period_frame(studies, period="year")

    summary_frame.to_csv(SUMMARY_CSV_PATH, index=False, encoding="utf-8-sig")
    coin_frame.to_csv(COIN_CSV_PATH, index=False, encoding="utf-8-sig")
    monthly_frame.to_csv(MONTHLY_CSV_PATH, index=False, encoding="utf-8-sig")
    yearly_frame.to_csv(YEARLY_CSV_PATH, index=False, encoding="utf-8-sig")
    coin_monthly_frame.to_csv(COIN_MONTHLY_CSV_PATH, index=False, encoding="utf-8-sig")
    coin_yearly_frame.to_csv(COIN_YEARLY_CSV_PATH, index=False, encoding="utf-8-sig")

    payload = build_payload(studies, aggregate_long, aggregate_short, aggregate_combo, summary_frame, coin_frame)
    JSON_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    HTML_PATH.write_text(
        build_html(
            studies,
            aggregate_long,
            aggregate_short,
            aggregate_combo,
            summary_frame,
            coin_frame,
            monthly_frame,
            yearly_frame,
            coin_monthly_frame,
            coin_yearly_frame,
        ),
        encoding="utf-8",
    )
    copyfile(HTML_PATH, PROJECT_HTML_PATH)
    print(HTML_PATH)
    print(PROJECT_HTML_PATH)


def run_symbol_study(client: OkxRestClient, symbol: str) -> SymbolStudy:
    candles = [candle for candle in load_candle_cache(symbol, ENTRY_BAR, limit=None) if candle.confirmed]
    if not candles:
        raise RuntimeError(f"missing local candles for {symbol} {ENTRY_BAR}")
    instrument = client.get_instrument(symbol)
    test_bounds = build_split_bounds(len(candles))["test"]

    long_result = _run_backtest_with_loaded_data(
        candles,
        instrument,
        build_long_config(symbol),
        data_source_note=build_data_note(symbol, len(candles)),
        maker_fee_rate=LONG_MAKER_FEE_RATE,
        taker_fee_rate=LONG_TAKER_FEE_RATE,
    )
    long_run = build_side_run(symbol, list(long_result.trades), test_bounds)

    short_runs: dict[str, SideRun] = {}
    combo_runs: dict[str, ComboRun] = {}
    for variant in SHORT_VARIANTS:
        short_result = _run_backtest_with_loaded_data(
            candles,
            instrument,
            build_short_config(symbol, variant),
            data_source_note=build_data_note(symbol, len(candles)),
            taker_fee_rate=SHORT_TAKER_FEE_RATE,
        )
        short_run = build_side_run(symbol, list(short_result.trades), test_bounds)
        short_runs[variant.key] = short_run
        combo_runs[variant.key] = build_combo_run(long_run, short_run, variant)

    return SymbolStudy(
        symbol=symbol,
        label=SYMBOL_LABELS[symbol],
        candle_count=len(candles),
        start_ts=candles[0].ts,
        end_ts=candles[-1].ts,
        long_run=long_run,
        short_runs=short_runs,
        combo_runs=combo_runs,
    )


def build_short_config(symbol: str, variant: ShortVariant) -> StrategyConfig:
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
        atr_take_multiplier=Decimal("0"),
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
        ema55_slope_same_bar_reentry_block=False,
        ema55_slope_dynamic_exit_requires_ema_reclaim=False,
        ema55_slope_locked_reentry_requires_ema21_near=variant.locked_reentry_ema21_near,
        ema55_slope_locked_reentry_min_r=variant.locked_reentry_min_r,
        ema55_slope_locked_reentry_max_r=variant.locked_reentry_max_r,
        ema55_slope_dynamic_exit_bull_bar_requires_bear_reentry=variant.dynamic_exit_bull_bar_requires_bear_reentry,
        ema55_slope_dynamic_exit_bull_bar_reentry_min_r=variant.dynamic_exit_bull_bar_reentry_min_r,
        ema55_slope_dynamic_exit_bull_bar_reentry_max_r=variant.dynamic_exit_bull_bar_reentry_max_r,
        atr_percentile_filter_max=Decimal("0.5"),
        trend_ema_slope_filter_min_ratio=Decimal("-0.0005"),
        time_stop_break_even_enabled=False,
        time_stop_break_even_bars=10,
    )


def build_data_note(symbol: str, candle_count: int) -> str:
    return f"local candle_cache full history | {symbol} {ENTRY_BAR} candles={candle_count}"


def build_side_run(symbol: str, trades: list[BacktestTrade], test_bounds) -> SideRun:
    test_trades = filter_split_trades(trades, test_bounds)
    return SideRun(
        symbol=symbol,
        label=SYMBOL_LABELS[symbol],
        trades=trades,
        test_trades=test_trades,
        all_metrics=build_metrics(trades),
        test_metrics=build_metrics(test_trades),
    )


def build_combo_run(long_run: SideRun, short_run: SideRun, variant: ShortVariant) -> ComboRun:
    trades = merge_trades(long_run.trades, short_run.trades)
    test_trades = merge_trades(long_run.test_trades, short_run.test_trades)
    return ComboRun(
        variant=variant,
        symbol=long_run.symbol,
        label=long_run.label,
        trades=trades,
        test_trades=test_trades,
        all_metrics=build_metrics(trades),
        test_metrics=build_metrics(test_trades),
        overlap_all=compute_overlap_stats(long_run.trades, short_run.trades),
        overlap_test=compute_overlap_stats(long_run.test_trades, short_run.test_trades),
    )


def merge_trades(left: list[BacktestTrade], right: list[BacktestTrade]) -> list[BacktestTrade]:
    return sorted([*left, *right], key=lambda trade: (trade.exit_ts, trade.entry_ts, trade.signal))


def aggregate_side(runs: list[SideRun], *, label: str) -> AggregateRun:
    trades: list[BacktestTrade] = []
    test_trades: list[BacktestTrade] = []
    for run in runs:
        trades.extend(run.trades)
        test_trades.extend(run.test_trades)
    trades.sort(key=lambda trade: (trade.exit_ts, trade.entry_ts, trade.signal))
    test_trades.sort(key=lambda trade: (trade.exit_ts, trade.entry_ts, trade.signal))
    return AggregateRun(label=label, trades=trades, test_trades=test_trades, all_metrics=build_metrics(trades), test_metrics=build_metrics(test_trades))


def aggregate_combo_side(runs: list[ComboRun], *, label: str) -> AggregateRun:
    trades: list[BacktestTrade] = []
    test_trades: list[BacktestTrade] = []
    for run in runs:
        trades.extend(run.trades)
        test_trades.extend(run.test_trades)
    trades.sort(key=lambda trade: (trade.exit_ts, trade.entry_ts, trade.signal))
    test_trades.sort(key=lambda trade: (trade.exit_ts, trade.entry_ts, trade.signal))
    return AggregateRun(label=label, trades=trades, test_trades=test_trades, all_metrics=build_metrics(trades), test_metrics=build_metrics(test_trades))


def compute_overlap_stats(long_trades: list[BacktestTrade], short_trades: list[BacktestTrade]) -> OverlapStats:
    pair_count = 0
    long_hits: set[int] = set()
    short_hits: set[int] = set()
    for long_index, long_trade in enumerate(long_trades):
        for short_index, short_trade in enumerate(short_trades):
            if intervals_overlap(long_trade.entry_ts, long_trade.exit_ts, short_trade.entry_ts, short_trade.exit_ts):
                pair_count += 1
                long_hits.add(long_index)
                short_hits.add(short_index)
    return OverlapStats(pair_count=pair_count, long_trade_count=len(long_hits), short_trade_count=len(short_hits))


def intervals_overlap(a_start: int, a_end: int, b_start: int, b_end: int) -> bool:
    return a_start < b_end and b_start < a_end


def build_summary_frame(
    aggregate_long: AggregateRun,
    aggregate_short: dict[str, AggregateRun],
    aggregate_combo: dict[str, AggregateRun],
) -> pd.DataFrame:
    rows = [summary_row("long_only", "做多", aggregate_long, None)]
    for variant in SHORT_VARIANTS:
        baseline = aggregate_short["original"] if variant.key != "original" else None
        rows.append(summary_row("short_only", variant.label, aggregate_short[variant.key], baseline))
    for variant in SHORT_VARIANTS:
        baseline = aggregate_combo["original"] if variant.key != "original" else None
        rows.append(summary_row("combo", f"做多 + {variant.label}", aggregate_combo[variant.key], baseline))
    return pd.DataFrame(rows)


def summary_row(scope: str, label: str, run: AggregateRun, baseline: AggregateRun | None) -> dict[str, object]:
    row = {
        "scope": scope,
        "label": label,
        "all_pnl_u": float(run.all_metrics.pnl),
        "all_trades": run.all_metrics.trades,
        "all_win_rate_pct": float(run.all_metrics.win_rate),
        "all_profit_factor": none_or_float(run.all_metrics.profit_factor),
        "all_avg_r": float(run.all_metrics.avg_r),
        "all_drawdown_u": float(run.all_metrics.max_drawdown),
        "all_return_pct": float(run.all_metrics.return_pct),
        "test_pnl_u": float(run.test_metrics.pnl),
        "test_trades": run.test_metrics.trades,
        "test_win_rate_pct": float(run.test_metrics.win_rate),
        "test_profit_factor": none_or_float(run.test_metrics.profit_factor),
        "test_avg_r": float(run.test_metrics.avg_r),
        "test_drawdown_u": float(run.test_metrics.max_drawdown),
        "test_return_pct": float(run.test_metrics.return_pct),
    }
    if baseline is None:
        row["all_delta_vs_original_u"] = None
        row["test_delta_vs_original_u"] = None
        row["all_drawdown_delta_u"] = None
        row["test_drawdown_delta_u"] = None
    else:
        row["all_delta_vs_original_u"] = float(run.all_metrics.pnl - baseline.all_metrics.pnl)
        row["test_delta_vs_original_u"] = float(run.test_metrics.pnl - baseline.test_metrics.pnl)
        row["all_drawdown_delta_u"] = float(run.all_metrics.max_drawdown - baseline.all_metrics.max_drawdown)
        row["test_drawdown_delta_u"] = float(run.test_metrics.max_drawdown - baseline.test_metrics.max_drawdown)
    return row


def build_coin_frame(studies: list[SymbolStudy]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for study in studies:
        original_short = study.short_runs["original"]
        ema21_short = study.short_runs["ema21_reset"]
        color_short = study.short_runs["bull_wait_bear"]
        color_2r_short = study.short_runs["bull_wait_bear_locked_2r"]
        original_combo = study.combo_runs["original"]
        ema21_combo = study.combo_runs["ema21_reset"]
        color_combo = study.combo_runs["bull_wait_bear"]
        color_2r_combo = study.combo_runs["bull_wait_bear_locked_2r"]
        rows.append(
            {
                "coin": study.label,
                "start": format_ts(study.start_ts),
                "end": format_ts(study.end_ts),
                "candles": study.candle_count,
                "long_test_pnl_u": float(study.long_run.test_metrics.pnl),
                "short_original_test_pnl_u": float(original_short.test_metrics.pnl),
                "short_ema21_test_pnl_u": float(ema21_short.test_metrics.pnl),
                "short_color_test_pnl_u": float(color_short.test_metrics.pnl),
                "short_color_2r_test_pnl_u": float(color_2r_short.test_metrics.pnl),
                "combo_original_test_pnl_u": float(original_combo.test_metrics.pnl),
                "combo_ema21_test_pnl_u": float(ema21_combo.test_metrics.pnl),
                "combo_color_test_pnl_u": float(color_combo.test_metrics.pnl),
                "combo_color_2r_test_pnl_u": float(color_2r_combo.test_metrics.pnl),
                "combo_ema21_delta_test_u": float(ema21_combo.test_metrics.pnl - original_combo.test_metrics.pnl),
                "combo_color_delta_test_u": float(color_combo.test_metrics.pnl - original_combo.test_metrics.pnl),
                "combo_color_2r_delta_test_u": float(color_2r_combo.test_metrics.pnl - original_combo.test_metrics.pnl),
                "combo_original_test_drawdown_u": float(original_combo.test_metrics.max_drawdown),
                "combo_ema21_test_drawdown_u": float(ema21_combo.test_metrics.max_drawdown),
                "combo_color_test_drawdown_u": float(color_combo.test_metrics.max_drawdown),
                "combo_color_2r_test_drawdown_u": float(color_2r_combo.test_metrics.max_drawdown),
                "combo_ema21_drawdown_delta_test_u": float(ema21_combo.test_metrics.max_drawdown - original_combo.test_metrics.max_drawdown),
                "combo_color_drawdown_delta_test_u": float(color_combo.test_metrics.max_drawdown - original_combo.test_metrics.max_drawdown),
                "combo_color_2r_drawdown_delta_test_u": float(color_2r_combo.test_metrics.max_drawdown - original_combo.test_metrics.max_drawdown),
            }
        )
    return pd.DataFrame(rows)


def build_period_frame(
    aggregate_long: AggregateRun,
    aggregate_short: dict[str, AggregateRun],
    aggregate_combo: dict[str, AggregateRun],
    *,
    period: str,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    datasets = [("做多", aggregate_long.trades)]
    for variant in SHORT_VARIANTS:
        datasets.append((variant.label, aggregate_short[variant.key].trades))
    for variant in SHORT_VARIANTS:
        datasets.append((f"做多 + {variant.label}", aggregate_combo[variant.key].trades))
    for label, trades in datasets:
        rows.extend(period_rows(label, trades, period))
    return pd.DataFrame(rows)


def build_coin_period_frame(studies: list[SymbolStudy], *, period: str) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for study in studies:
        datasets = [("做多", study.long_run.trades)]
        for variant in SHORT_VARIANTS:
            datasets.append((variant.label, study.short_runs[variant.key].trades))
        for variant in SHORT_VARIANTS:
            datasets.append((f"做多 + {variant.label}", study.combo_runs[variant.key].trades))
        for label, trades in datasets:
            for row in period_rows(label, trades, period):
                rows.append(
                    {
                        "coin": study.label,
                        "period": row["period"],
                        "label": row["label"],
                        "pnl_u": row["pnl_u"],
                        "trades": row["trades"],
                    }
                )
    return pd.DataFrame(rows)


def period_rows(label: str, trades: list[BacktestTrade], period: str) -> list[dict[str, object]]:
    bucket: dict[str, dict[str, Decimal | int]] = {}
    for trade in trades:
        key = period_key(trade.exit_ts, period)
        item = bucket.setdefault(key, {"pnl": Decimal("0"), "trades": 0})
        item["pnl"] = item["pnl"] + trade.pnl
        item["trades"] = int(item["trades"]) + 1
    return [{"period": key, "label": label, "pnl_u": float(bucket[key]["pnl"]), "trades": int(bucket[key]["trades"])} for key in sorted(bucket)]


def period_key(exit_ts: int, period: str) -> str:
    dt = timestamp_to_datetime(exit_ts)
    if period == "year":
        return dt.strftime("%Y")
    return dt.strftime("%Y-%m")


def build_payload(
    studies: list[SymbolStudy],
    aggregate_long: AggregateRun,
    aggregate_short: dict[str, AggregateRun],
    aggregate_combo: dict[str, AggregateRun],
    summary_frame: pd.DataFrame,
    coin_frame: pd.DataFrame,
) -> dict[str, object]:
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "html_path": str(HTML_PATH),
        "project_html_path": str(PROJECT_HTML_PATH),
        "risk_amount_u": str(RISK_AMOUNT),
        "entry_bar": ENTRY_BAR,
        "symbols": list(SYMBOLS),
        "summary": summary_frame.to_dict(orient="records"),
        "by_coin": coin_frame.to_dict(orient="records"),
        "study_range": {
            "start": min(format_ts(study.start_ts) for study in studies),
            "end": max(format_ts(study.end_ts) for study in studies),
        },
        "short_notes": {variant.key: variant.note for variant in SHORT_VARIANTS},
        "all_sample": {variant.key: str(aggregate_combo[variant.key].all_metrics.pnl) for variant in SHORT_VARIANTS},
        "test_sample": {variant.key: str(aggregate_combo[variant.key].test_metrics.pnl) for variant in SHORT_VARIANTS},
        "long_only_test_pnl_u": str(aggregate_long.test_metrics.pnl),
    }


def build_html(
    studies: list[SymbolStudy],
    aggregate_long: AggregateRun,
    aggregate_short: dict[str, AggregateRun],
    aggregate_combo: dict[str, AggregateRun],
    summary_frame: pd.DataFrame,
    coin_frame: pd.DataFrame,
    monthly_frame: pd.DataFrame,
    yearly_frame: pd.DataFrame,
    coin_monthly_frame: pd.DataFrame,
    coin_yearly_frame: pd.DataFrame,
) -> str:
    base_combo = aggregate_combo["original"]
    ema21_combo = aggregate_combo["ema21_reset"]
    color_combo = aggregate_combo["bull_wait_bear"]
    color_2r_combo = aggregate_combo["bull_wait_bear_locked_2r"]

    all_curve = build_equity_curve_image(
        {
            "做多": aggregate_long.trades,
            "做多 + 原做空": base_combo.trades,
            "做多 + EMA21 重置做空": ema21_combo.trades,
            "做多 + 阳线等阴线做空": color_combo.trades,
            "做多 + 2R阳线等阴线做空": color_2r_combo.trades,
        },
        "全样本累计净利润",
    )
    test_curve = build_equity_curve_image(
        {
            "做多": aggregate_long.test_trades,
            "做多 + 原做空": base_combo.test_trades,
            "做多 + EMA21 重置做空": ema21_combo.test_trades,
            "做多 + 阳线等阴线做空": color_combo.test_trades,
            "做多 + 2R阳线等阴线做空": color_2r_combo.test_trades,
        },
        "测试段累计净利润",
    )

    monthly_pivot = build_period_pivot_html(monthly_frame, "月度汇总")
    yearly_pivot = build_period_pivot_html(yearly_frame, "年度汇总")
    coin_yearly_sections = build_coin_period_sections(coin_yearly_frame, "年度")
    coin_monthly_sections = build_coin_period_sections(coin_monthly_frame, "月度")

    hero_rows = [
        ("原组合全样本", base_combo.all_metrics.pnl),
        ("EMA21 组合全样本", ema21_combo.all_metrics.pnl),
        ("颜色规则组合全样本", color_combo.all_metrics.pnl),
        ("2R颜色规则组合全样本", color_2r_combo.all_metrics.pnl),
        ("原组合测试段", base_combo.test_metrics.pnl),
        ("EMA21 组合测试段", ema21_combo.test_metrics.pnl),
        ("颜色规则组合测试段", color_combo.test_metrics.pnl),
        ("2R颜色规则组合测试段", color_2r_combo.test_metrics.pnl),
    ]

    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <title>做多 + 做空再入场规则对比报告</title>
  <style>
    :root {{
      --bg: #f5f1e8;
      --panel: #fffdf8;
      --ink: #1f1b16;
      --muted: #6c6257;
      --line: #d9cfc1;
      --accent: #9a3412;
      --good: #166534;
      --bad: #b91c1c;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      background:
        radial-gradient(circle at top left, rgba(154,52,18,0.08), transparent 32%),
        linear-gradient(180deg, #f8f3ea 0%, var(--bg) 100%);
      color: var(--ink);
      font-family: "Segoe UI", "Microsoft YaHei", sans-serif;
      line-height: 1.55;
    }}
    .wrap {{ width: min(1440px, calc(100vw - 48px)); margin: 0 auto; padding: 28px 0 40px; }}
    h1, h2, h3 {{ margin: 0 0 12px; }}
    h1 {{ font-size: 34px; }}
    h2 {{ font-size: 22px; margin-top: 32px; }}
    h3 {{ font-size: 18px; margin-top: 20px; }}
    p {{ margin: 8px 0; color: var(--muted); }}
    .hero {{
      background: linear-gradient(135deg, rgba(154,52,18,0.14), rgba(255,255,255,0.92));
      border: 1px solid rgba(154,52,18,0.15);
      border-radius: 24px;
      padding: 24px 28px;
      box-shadow: 0 20px 40px rgba(31,27,22,0.08);
    }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 14px; margin-top: 18px; }}
    .card {{ background: var(--panel); border: 1px solid var(--line); border-radius: 18px; padding: 16px 18px; }}
    .label {{ color: var(--muted); font-size: 13px; letter-spacing: 0.03em; }}
    .value {{ font-size: 28px; font-weight: 700; margin-top: 6px; }}
    .good {{ color: var(--good); }}
    .bad {{ color: var(--bad); }}
    .section {{ background: rgba(255,255,255,0.72); border: 1px solid rgba(217,207,193,0.85); border-radius: 22px; padding: 22px 24px; margin-top: 24px; }}
    table {{ width: 100%; border-collapse: collapse; margin-top: 14px; background: var(--panel); border-radius: 14px; overflow: hidden; }}
    th, td {{ padding: 10px 12px; border-bottom: 1px solid var(--line); text-align: right; white-space: nowrap; font-size: 13px; }}
    th:first-child, td:first-child {{ text-align: left; }}
    thead th {{ background: #efe5d8; color: #3a2d22; position: sticky; top: 0; }}
    tr:last-child td {{ border-bottom: none; }}
    .note {{ padding: 12px 14px; border-left: 4px solid var(--accent); background: rgba(154,52,18,0.06); border-radius: 12px; color: #4a3829; }}
    .img-box {{ margin-top: 16px; background: var(--panel); border: 1px solid var(--line); border-radius: 18px; padding: 10px; }}
    .two-col {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(360px, 1fr)); gap: 16px; }}
    img {{ display: block; width: 100%; height: auto; border-radius: 12px; }}
    ul {{ margin: 10px 0 0; padding-left: 18px; color: var(--muted); }}
    code {{ background: rgba(31,27,22,0.06); padding: 1px 6px; border-radius: 6px; color: #5c2411; }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <h1>做多 + 做空再入场规则对比报告</h1>
      <p>口径：5 币种全样本、1H、固定风险金 10U、正式主回测接口。做多侧保持当前正式参数不变；做空侧比较三种版本：原做空、EMA21 重置做空、阳线等阴线做空。</p>
      <div class="grid">
        {''.join(f'<div class="card"><div class="label">{html.escape(label)}</div><div class="value">{fmt_u(value)}</div></div>' for label, value in hero_rows)}
      </div>
    </section>

    <section class="section">
      <h2>核心结论</h2>
      <div class="note">
        <strong>这次新增的“阳线等阴线做空”规则</strong>只对保本/锁盈类动态保护出场生效。也就是说，它不是改整个做空系统，而是专门拦“动态保护平仓后，阳线里马上又追空”的那一段再入场逻辑。
      </div>
      <ul>
        <li>原做空：最接近当前强势基线。</li>
        <li>EMA21 重置做空：更强调结构重置，逻辑最干净，但一般会损失一部分利润空间。</li>
        <li>阳线等阴线做空：更贴近你这次的新思路，只拦“阳线平仓后”的再入场，不拦“阴线延续中的再空”。</li>
      </ul>
    </section>

    <section class="section">
      <h2>总览对比</h2>
      {dataframe_to_html(summary_frame)}
    </section>

    <section class="section">
      <h2>累计净利润曲线</h2>
      <div class="two-col">
        <div class="img-box">
          <h3>全样本</h3>
          <img alt="全样本累计净利润曲线" src="data:image/png;base64,{all_curve}" />
        </div>
        <div class="img-box">
          <h3>测试段</h3>
          <img alt="测试段累计净利润曲线" src="data:image/png;base64,{test_curve}" />
        </div>
      </div>
    </section>

    <section class="section">
      <h2>分币种测试段拆解</h2>
      <p>下面这张表只保留最关键的测试段指标，方便直接看每个币换规则后是增益还是拖累。</p>
      {dataframe_to_html(coin_frame)}
    </section>

    <section class="section">
      <h2>月度与年度汇总</h2>
      <div class="two-col">
        <div>{monthly_pivot}</div>
        <div>{yearly_pivot}</div>
      </div>
    </section>

    <section class="section">
      <h2>分币种年度明细</h2>
      {coin_yearly_sections}
    </section>

    <section class="section">
      <h2>分币种月度明细</h2>
      {coin_monthly_sections}
    </section>

    <section class="section">
      <h2>规则说明</h2>
      <div class="two-col">
        <div>
          <h3>做空侧</h3>
          <ul>
            <li><code>EMA55</code> 斜率做空，<code>ATR14</code>，2ATR 止损，动态止盈。</li>
            {''.join(f'<li><strong>{html.escape(variant.label)}</strong>：{html.escape(variant.note)}</li>' for variant in SHORT_VARIANTS)}
          </ul>
        </div>
        <div>
          <h3>做多侧</h3>
          <ul>
            <li>做多沿用当前正式推荐参数，按币种使用不同的快线、趋势线和入场参考线。</li>
            <li>统一为动态止盈、2R 保本、手续费偏移开启、固定风险金 10U。</li>
          </ul>
          {long_profile_table()}
        </div>
      </div>
    </section>

    <section class="section">
      <h2>样本范围</h2>
      <p>币种：{", ".join(SYMBOL_LABELS[symbol] for symbol in SYMBOLS)}。</p>
      <p>时间覆盖：{html.escape(min(format_ts(study.start_ts) for study in studies))} 至 {html.escape(max(format_ts(study.end_ts) for study in studies))}。</p>
      <p>项目内固定报告：<code>{html.escape(str(PROJECT_HTML_PATH))}</code></p>
      <p>分析目录原始报告：<code>{html.escape(str(HTML_PATH))}</code></p>
    </section>
  </div>
</body>
</html>"""


def long_profile_table() -> str:
    rows = []
    for symbol in SYMBOLS:
        profile = LONG_PROFILES[symbol]
        entry_ref = "跟随快线" if profile.entry_reference_ema_period <= 0 else f"EMA{profile.entry_reference_ema_period}"
        rows.append(
            f"<tr><td>{html.escape(SYMBOL_LABELS[symbol])}</td><td>EMA{profile.ema_period}</td><td>EMA{profile.trend_ema_period}</td><td>{html.escape(entry_ref)}</td><td>{html.escape(str(profile.atr_stop_multiplier))}</td></tr>"
        )
    return "<table><thead><tr><th>币种</th><th>快线</th><th>趋势线</th><th>入场参考</th><th>ATR 止损倍数</th></tr></thead><tbody>" + "".join(rows) + "</tbody></table>"


def build_equity_curve_image(series_map: dict[str, list[BacktestTrade]], title: str) -> str:
    fig, ax = plt.subplots(figsize=(11, 4.8))
    for label, trades in series_map.items():
        points = equity_points(trades)
        if not points:
            continue
        xs, ys = zip(*points)
        ax.plot(xs, ys, label=label, linewidth=2)
    ax.set_title(title)
    ax.set_ylabel("PnL (U)")
    ax.grid(alpha=0.2)
    ax.legend()
    fig.autofmt_xdate()
    buffer = io.BytesIO()
    fig.savefig(buffer, format="png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    return base64.b64encode(buffer.getvalue()).decode("ascii")


def equity_points(trades: list[BacktestTrade]) -> list[tuple[datetime, float]]:
    ordered = sorted(trades, key=lambda trade: (trade.exit_ts, trade.entry_ts, trade.signal))
    equity = Decimal("0")
    points: list[tuple[datetime, float]] = []
    for trade in ordered:
        equity += trade.pnl
        points.append((timestamp_to_datetime(trade.exit_ts), float(equity)))
    return points


def build_period_pivot_html(frame: pd.DataFrame, title: str) -> str:
    pivot = frame.pivot(index="period", columns="label", values="pnl_u").fillna(0.0).reset_index()
    ordered_columns = ["period", "做多"] + [variant.label for variant in SHORT_VARIANTS] + [f"做多 + {variant.label}" for variant in SHORT_VARIANTS]
    pivot = pivot[[column for column in ordered_columns if column in pivot.columns]]
    return f"<h3>{html.escape(title)}</h3>{dataframe_to_html(pivot)}"


def build_coin_period_sections(frame: pd.DataFrame, title: str) -> str:
    blocks: list[str] = []
    for coin in [SYMBOL_LABELS[symbol] for symbol in SYMBOLS]:
        coin_frame = frame[frame["coin"] == coin].copy()
        if coin_frame.empty:
            continue
        pivot = coin_frame.pivot(index="period", columns="label", values="pnl_u").fillna(0.0).reset_index()
        ordered_columns = ["period", "做多"] + [f"做多 + {variant.label}" for variant in SHORT_VARIANTS] + [variant.label for variant in SHORT_VARIANTS]
        pivot = pivot[[column for column in ordered_columns if column in pivot.columns]]
        blocks.append(f'<div class="img-box"><h3>{html.escape(coin)} {html.escape(title)}</h3>{dataframe_to_html(pivot)}</div>')
    return "".join(blocks)


def dataframe_to_html(frame: pd.DataFrame) -> str:
    display_frame = frame.copy()
    for column in display_frame.columns:
        if pd.api.types.is_float_dtype(display_frame[column]):
            display_frame[column] = display_frame[column].map(format_float_value)
    return display_frame.to_html(index=False, escape=False, classes="report-table")


def format_float_value(value: float) -> str:
    if pd.isna(value):
        return ""
    return f"{value:,.2f}"


def timestamp_to_datetime(ts: int) -> datetime:
    if ts >= 10**12:
        return datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
    return datetime.fromtimestamp(ts, tz=timezone.utc)


def none_or_float(value: Decimal | None) -> float | None:
    return None if value is None else float(value)


def fmt_u(value: Decimal) -> str:
    return f"{float(value):,.2f}U"


if __name__ == "__main__":
    main()
