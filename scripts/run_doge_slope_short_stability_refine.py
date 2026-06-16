from __future__ import annotations

import csv
import json
import sys
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from okx_quant.backtest import BacktestTrade, _build_report, _run_backtest_with_loaded_data
from okx_quant.candle_cache import load_candle_cache
from okx_quant.models import StrategyConfig
from okx_quant.okx_client import OkxRestClient
from okx_quant.persistence import analysis_report_dir_path
from okx_quant.pricing import format_decimal_fixed
from scripts.build_best_parameter_bundle import build_specs


REPORT_DIR = analysis_report_dir_path()
REPORT_DIR.mkdir(parents=True, exist_ok=True)
PROJECT_REPORT_DIR = ROOT / "reports"
PROJECT_REPORT_DIR.mkdir(parents=True, exist_ok=True)

SYMBOL = "DOGE-USDT-SWAP"
BAR = "1H"
INITIAL_CAPITAL = Decimal("10000")
TAKER_FEE_RATE = Decimal("0.00036")
OOS_START = datetime(2022, 1, 1, tzinfo=timezone.utc)
TRAIN_MONTHS = 18
TEST_MONTHS = 6
STEP_MONTHS = 6

STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
BASE_NAME = f"doge_slope_short_stability_refine_{STAMP}"
SUMMARY_CSV_PATH = REPORT_DIR / f"{BASE_NAME}_summary.csv"
WINDOW_CSV_PATH = REPORT_DIR / f"{BASE_NAME}_windows.csv"
JSON_PATH = REPORT_DIR / f"{BASE_NAME}.json"
MD_PATH = REPORT_DIR / f"{BASE_NAME}.md"
LATEST_MD_PATH = PROJECT_REPORT_DIR / "doge_slope_short_stability_refine_latest.md"
LATEST_CSV_PATH = PROJECT_REPORT_DIR / "doge_slope_short_stability_refine_latest.csv"
LATEST_JSON_PATH = PROJECT_REPORT_DIR / "doge_slope_short_stability_refine_latest.json"


@dataclass(frozen=True)
class Candidate:
    candidate_id: str
    label: str
    config: StrategyConfig
    notes: str


@dataclass(frozen=True)
class Metrics:
    pnl: Decimal
    trades: int
    win_rate: Decimal
    profit_factor: Decimal | None
    avg_r: Decimal
    max_drawdown: Decimal
    return_pct: Decimal


@dataclass(frozen=True)
class StreakStats:
    max_loss_streak: int
    max_loss_start: str
    max_loss_end: str
    max_stop_streak: int
    max_stop_start: str
    max_stop_end: str
    stop_loss_count: int
    slope_turn_count: int
    breakeven_count: int


@dataclass(frozen=True)
class CandidateRun:
    candidate: Candidate
    trades: tuple[BacktestTrade, ...]
    full_metrics: Metrics
    oos_metrics: Metrics
    full_streaks: StreakStats
    oos_streaks: StreakStats


@dataclass(frozen=True)
class Window:
    index: int
    train_start: datetime
    train_end: datetime
    test_start: datetime
    test_end: datetime


@dataclass(frozen=True)
class WindowResult:
    window: Window
    candidate_id: str
    label: str
    train_metrics: Metrics
    test_metrics: Metrics


def _fmt(value: Decimal | None, digits: int = 4) -> str:
    if value is None:
        return "-"
    return format_decimal_fixed(value, digits)


def _dt_text(value: datetime) -> str:
    return value.strftime("%Y-%m-%d")


def _trade_text(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _add_months(dt: datetime, months: int) -> datetime:
    year = dt.year + (dt.month - 1 + months) // 12
    month = (dt.month - 1 + months) % 12 + 1
    return datetime(year, month, 1, tzinfo=dt.tzinfo)


def _make_rules(
    *,
    break_even_trigger_r: int,
    lock_trigger_r: int,
    first_lock_r: int,
    trailing_step_r: int,
) -> tuple[dict[str, object], ...]:
    return (
        {
            "trigger_r": break_even_trigger_r,
            "action": "break_even",
            "lock_r": None,
            "trail_mode": "none",
            "trail_every_r": None,
            "trail_add_r": None,
        },
        {
            "trigger_r": lock_trigger_r,
            "action": "lock_profit",
            "lock_r": first_lock_r,
            "trail_mode": "step",
            "trail_every_r": trailing_step_r,
            "trail_add_r": trailing_step_r,
        },
    )


def _with_protection(
    config: StrategyConfig,
    *,
    atr_period: int,
    stop_atr: Decimal,
    break_even_trigger_r: int,
    lock_trigger_r: int,
    first_lock_r: int,
    trailing_step_r: int,
) -> StrategyConfig:
    return replace(
        config,
        atr_period=atr_period,
        atr_stop_multiplier=stop_atr,
        atr_take_multiplier=stop_atr * Decimal("2"),
        dynamic_break_even_trigger_r=break_even_trigger_r,
        dynamic_first_lock_r=first_lock_r,
        dynamic_trailing_step_r=trailing_step_r,
        dynamic_protection_rules=_make_rules(
            break_even_trigger_r=break_even_trigger_r,
            lock_trigger_r=lock_trigger_r,
            first_lock_r=first_lock_r,
            trailing_step_r=trailing_step_r,
        ),
        ema55_slope_lock_profit_trigger_r=lock_trigger_r,
    )


def _with_bear_reentry(config: StrategyConfig) -> StrategyConfig:
    return replace(
        config,
        ema55_slope_dynamic_exit_requires_bear_reentry=True,
        ema55_slope_dynamic_exit_bear_reentry_break_prev_low=True,
    )


def build_candidates() -> tuple[Candidate, ...]:
    final_spec = next(item for item in build_specs() if item.profile_id == "slope_short_best_doge_v2")
    final_config = final_spec.config
    ma55_context = _with_protection(
        replace(
            final_config,
            ema_period=55,
            ema_type="ma",
            trend_ema_period=55,
            trend_ema_type="ma",
        ),
        atr_period=14,
        stop_atr=Decimal("2"),
        break_even_trigger_r=2,
        lock_trigger_r=5,
        first_lock_r=4,
        trailing_step_r=1,
    )
    sl2_bear = _with_bear_reentry(final_config)
    sl25 = _with_protection(
        final_config,
        atr_period=13,
        stop_atr=Decimal("2.5"),
        break_even_trigger_r=2,
        lock_trigger_r=6,
        first_lock_r=5,
        trailing_step_r=1,
    )
    sl25_bear = _with_bear_reentry(sl25)
    sl3 = _with_protection(
        final_config,
        atr_period=13,
        stop_atr=Decimal("3"),
        break_even_trigger_r=2,
        lock_trigger_r=6,
        first_lock_r=5,
        trailing_step_r=1,
    )
    sl3_bear = _with_bear_reentry(sl3)
    return (
        Candidate(
            "final_best",
            "定稿 MA21 / ATR13 / SL2 / 2R保本 / 6R锁5R / step1",
            final_config,
            "当前最佳参数包定稿，用作收益上限与基准体感对照。",
        ),
        Candidate(
            "sl2_bear_reentry",
            "稳健化 MA21 / ATR13 / SL2 / Bear-Reentry",
            sl2_bear,
            "不改止损，只加动态离场后的再次转弱确认，观察能否减少噪音重开仓。",
        ),
        Candidate(
            "sl25",
            "稳健化 MA21 / ATR13 / SL2.5 / 2R保本 / 6R锁5R",
            sl25,
            "轻度放宽止损，优先看回撤和连亏是否变顺。",
        ),
        Candidate(
            "sl25_bear_reentry",
            "稳健化 MA21 / ATR13 / SL2.5 / Bear-Reentry",
            sl25_bear,
            "止损稍放宽，同时加再转弱确认，目标是降低追空反复。",
        ),
        Candidate(
            "sl3",
            "稳健化 MA21 / ATR13 / SL3 / 2R保本 / 6R锁5R",
            sl3,
            "进一步放宽止损，测试是否只是少止损还是会吞掉效率。",
        ),
        Candidate(
            "sl3_bear_reentry",
            "稳健化 MA21 / ATR13 / SL3 / Bear-Reentry",
            sl3_bear,
            "最保守候选，看是否能明显压缩回撤和连续止损。",
        ),
        Candidate(
            "ma55_context",
            "对照 MA55 / ATR14 / SL2 / 2R保本 / 5R锁4R",
            ma55_context,
            "保留早前 MA55 对照，方便看少交易是否真的更稳。",
        ),
    )


def build_metrics(trades: tuple[BacktestTrade, ...]) -> Metrics:
    report = _build_report(list(trades), initial_capital=INITIAL_CAPITAL)
    return Metrics(
        pnl=report.total_pnl,
        trades=report.total_trades,
        win_rate=report.win_rate,
        profit_factor=report.profit_factor,
        avg_r=report.average_r_multiple,
        max_drawdown=report.max_drawdown,
        return_pct=report.total_return_pct,
    )


def slice_trades(
    trades: tuple[BacktestTrade, ...],
    *,
    start: datetime,
    end: datetime,
) -> tuple[BacktestTrade, ...]:
    start_ts = int(start.timestamp() * 1000)
    end_ts = int(end.timestamp() * 1000)
    return tuple(trade for trade in trades if start_ts <= trade.exit_ts < end_ts)


def score_metrics(metrics: Metrics) -> tuple[Decimal, Decimal, Decimal, Decimal]:
    return (
        metrics.pnl,
        metrics.profit_factor or Decimal("0"),
        -metrics.max_drawdown,
        metrics.avg_r,
    )


def build_windows(sample_end: datetime) -> tuple[Window, ...]:
    windows: list[Window] = []
    test_start = OOS_START
    index = 1
    while test_start < sample_end:
        train_start = _add_months(test_start, -TRAIN_MONTHS)
        test_end = min(_add_months(test_start, TEST_MONTHS), sample_end)
        windows.append(
            Window(
                index=index,
                train_start=train_start,
                train_end=test_start,
                test_start=test_start,
                test_end=test_end,
            )
        )
        index += 1
        next_start = _add_months(test_start, STEP_MONTHS)
        if next_start <= test_start:
            break
        test_start = next_start
    return tuple(windows)


def compute_streak_stats(trades: tuple[BacktestTrade, ...]) -> StreakStats:
    max_loss_streak = 0
    max_stop_streak = 0
    current_loss = 0
    current_stop = 0
    loss_start = ""
    stop_start = ""
    max_loss_start = ""
    max_loss_end = ""
    max_stop_start = ""
    max_stop_end = ""
    stop_loss_count = 0
    slope_turn_count = 0
    breakeven_count = 0

    for trade in sorted(trades, key=lambda item: (item.exit_ts, item.entry_ts)):
        exit_text = _trade_text(trade.exit_ts)
        if trade.exit_reason == "stop_loss":
            stop_loss_count += 1
        elif trade.exit_reason == "slope_turn_positive":
            slope_turn_count += 1
        elif trade.exit_reason == "break_even_stop":
            breakeven_count += 1

        if trade.pnl < 0:
            if current_loss == 0:
                loss_start = exit_text
            current_loss += 1
            if current_loss > max_loss_streak:
                max_loss_streak = current_loss
                max_loss_start = loss_start
                max_loss_end = exit_text
        else:
            current_loss = 0
            loss_start = ""

        if trade.exit_reason == "stop_loss":
            if current_stop == 0:
                stop_start = exit_text
            current_stop += 1
            if current_stop > max_stop_streak:
                max_stop_streak = current_stop
                max_stop_start = stop_start
                max_stop_end = exit_text
        else:
            current_stop = 0
            stop_start = ""

    return StreakStats(
        max_loss_streak=max_loss_streak,
        max_loss_start=max_loss_start,
        max_loss_end=max_loss_end,
        max_stop_streak=max_stop_streak,
        max_stop_start=max_stop_start,
        max_stop_end=max_stop_end,
        stop_loss_count=stop_loss_count,
        slope_turn_count=slope_turn_count,
        breakeven_count=breakeven_count,
    )


def run_candidates() -> tuple[CandidateRun, ...]:
    client = OkxRestClient()
    instrument = client.get_instrument(SYMBOL)
    candles = [candle for candle in load_candle_cache(SYMBOL, BAR, limit=None) if candle.confirmed]
    if not candles:
        raise RuntimeError(f"missing candles for {SYMBOL} {BAR}")

    runs: list[CandidateRun] = []
    for candidate in build_candidates():
        print(f"[candidate] {candidate.label}")
        result = _run_backtest_with_loaded_data(
            candles,
            instrument,
            candidate.config,
            data_source_note=f"local candle_cache full history | {SYMBOL} | {BAR} | stability refine",
            maker_fee_rate=Decimal("0"),
            taker_fee_rate=TAKER_FEE_RATE,
        )
        trades = tuple(result.trades)
        oos_trades = slice_trades(trades, start=OOS_START, end=datetime.max.replace(tzinfo=timezone.utc))
        runs.append(
            CandidateRun(
                candidate=candidate,
                trades=trades,
                full_metrics=build_metrics(trades),
                oos_metrics=build_metrics(oos_trades),
                full_streaks=compute_streak_stats(trades),
                oos_streaks=compute_streak_stats(oos_trades),
            )
        )
    return tuple(runs)


def build_window_results(runs: tuple[CandidateRun, ...], sample_end: datetime) -> tuple[WindowResult, ...]:
    rows: list[WindowResult] = []
    for window in build_windows(sample_end):
        for run in runs:
            train_trades = slice_trades(run.trades, start=window.train_start, end=window.train_end)
            test_trades = slice_trades(run.trades, start=window.test_start, end=window.test_end)
            rows.append(
                WindowResult(
                    window=window,
                    candidate_id=run.candidate.candidate_id,
                    label=run.candidate.label,
                    train_metrics=build_metrics(train_trades),
                    test_metrics=build_metrics(test_trades),
                )
            )
    return tuple(rows)


def summarize_window_outcomes(window_rows: tuple[WindowResult, ...]) -> dict[str, dict[str, int]]:
    summary: dict[str, dict[str, int]] = {}
    for candidate_id in {row.candidate_id for row in window_rows}:
        rows = tuple(row for row in window_rows if row.candidate_id == candidate_id)
        summary[candidate_id] = {
            "positive_test_windows": sum(1 for row in rows if row.test_metrics.pnl > 0),
            "selected_windows": 0,
            "selected_positive_windows": 0,
        }
    for window_index in sorted({row.window.index for row in window_rows}):
        bucket = [row for row in window_rows if row.window.index == window_index]
        chosen = max(bucket, key=lambda item: score_metrics(item.train_metrics))
        summary[chosen.candidate_id]["selected_windows"] += 1
        if chosen.test_metrics.pnl > 0:
            summary[chosen.candidate_id]["selected_positive_windows"] += 1
    return summary


def write_summary_csv(runs: tuple[CandidateRun, ...], window_summary: dict[str, dict[str, int]]) -> None:
    baseline = next(run for run in runs if run.candidate.candidate_id == "final_best")
    fieldnames = [
        "candidate_id",
        "label",
        "oos_pnl",
        "oos_max_drawdown",
        "oos_profit_factor",
        "oos_avg_r",
        "oos_trades",
        "oos_loss_streak",
        "oos_stop_streak",
        "positive_test_windows",
        "selected_windows",
        "selected_positive_windows",
        "trade_delta_vs_final",
        "pnl_delta_vs_final",
        "dd_delta_vs_final",
        "notes",
    ]
    with SUMMARY_CSV_PATH.open("w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for run in sorted(runs, key=lambda item: score_metrics(item.oos_metrics), reverse=True):
            window_stats = window_summary[run.candidate.candidate_id]
            writer.writerow(
                {
                    "candidate_id": run.candidate.candidate_id,
                    "label": run.candidate.label,
                    "oos_pnl": _fmt(run.oos_metrics.pnl),
                    "oos_max_drawdown": _fmt(run.oos_metrics.max_drawdown),
                    "oos_profit_factor": _fmt(run.oos_metrics.profit_factor),
                    "oos_avg_r": _fmt(run.oos_metrics.avg_r),
                    "oos_trades": run.oos_metrics.trades,
                    "oos_loss_streak": run.oos_streaks.max_loss_streak,
                    "oos_stop_streak": run.oos_streaks.max_stop_streak,
                    "positive_test_windows": window_stats["positive_test_windows"],
                    "selected_windows": window_stats["selected_windows"],
                    "selected_positive_windows": window_stats["selected_positive_windows"],
                    "trade_delta_vs_final": run.oos_metrics.trades - baseline.oos_metrics.trades,
                    "pnl_delta_vs_final": _fmt(run.oos_metrics.pnl - baseline.oos_metrics.pnl),
                    "dd_delta_vs_final": _fmt(run.oos_metrics.max_drawdown - baseline.oos_metrics.max_drawdown),
                    "notes": run.candidate.notes,
                }
            )


def write_window_csv(window_rows: tuple[WindowResult, ...]) -> None:
    fieldnames = [
        "window_index",
        "train_start",
        "train_end",
        "test_start",
        "test_end",
        "candidate_id",
        "label",
        "train_pnl",
        "train_pf",
        "test_pnl",
        "test_pf",
        "test_max_drawdown",
        "test_trades",
    ]
    with WINDOW_CSV_PATH.open("w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in window_rows:
            writer.writerow(
                {
                    "window_index": row.window.index,
                    "train_start": _dt_text(row.window.train_start),
                    "train_end": _dt_text(row.window.train_end),
                    "test_start": _dt_text(row.window.test_start),
                    "test_end": _dt_text(row.window.test_end),
                    "candidate_id": row.candidate_id,
                    "label": row.label,
                    "train_pnl": _fmt(row.train_metrics.pnl),
                    "train_pf": _fmt(row.train_metrics.profit_factor),
                    "test_pnl": _fmt(row.test_metrics.pnl),
                    "test_pf": _fmt(row.test_metrics.profit_factor),
                    "test_max_drawdown": _fmt(row.test_metrics.max_drawdown),
                    "test_trades": row.test_metrics.trades,
                }
            )


def json_ready(value: object) -> object:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, tuple):
        return [json_ready(item) for item in value]
    if isinstance(value, list):
        return [json_ready(item) for item in value]
    if isinstance(value, dict):
        return {str(key): json_ready(item) for key, item in value.items()}
    if hasattr(value, "__dataclass_fields__"):
        return {key: json_ready(getattr(value, key)) for key in value.__dataclass_fields__}
    return value


def build_markdown(
    *,
    runs: tuple[CandidateRun, ...],
    window_rows: tuple[WindowResult, ...],
    sample_start: datetime,
    sample_end: datetime,
) -> str:
    baseline = next(run for run in runs if run.candidate.candidate_id == "final_best")
    window_summary = summarize_window_outcomes(window_rows)
    window_count = len(build_windows(sample_end))
    best_tweak = max(
        (
            run
            for run in runs
            if run.candidate.candidate_id not in {"final_best", "ma55_context"}
        ),
        key=lambda item: (
            window_summary[item.candidate.candidate_id]["positive_test_windows"],
            item.oos_metrics.profit_factor or Decimal("0"),
            item.oos_metrics.pnl,
            -item.oos_metrics.max_drawdown,
        ),
    )
    stable_candidates = sorted(
        (
            run
            for run in runs
            if run.candidate.candidate_id != "final_best"
            and run.oos_metrics.max_drawdown <= baseline.oos_metrics.max_drawdown
        ),
        key=lambda item: (
            window_summary[item.candidate.candidate_id]["positive_test_windows"],
            item.oos_metrics.profit_factor or Decimal("0"),
            -item.oos_metrics.max_drawdown,
            item.oos_metrics.pnl,
        ),
        reverse=True,
    )
    stable_pick = stable_candidates[0] if stable_candidates else None

    lines = [
        "# DOGE 斜率做空稳健性打磨",
        "",
        "## 锁死口径",
        "",
        f"- 标的：`{SYMBOL}`",
        f"- 周期：`{BAR}`",
        f"- 样本范围：`{sample_start.strftime('%Y-%m-%d %H:%M:%S UTC')}` -> `{sample_end.strftime('%Y-%m-%d %H:%M:%S UTC')}`",
        f"- 样本外起点：`{OOS_START.strftime('%Y-%m-%d %H:%M:%S UTC')}`",
        "- 固定风险金：`100U` / 笔；初始资金 `10000U`；非复利",
        f"- 手续费：Taker `{format_decimal_fixed(TAKER_FEE_RATE * Decimal('100'), 4)}%`",
        "- 本轮只测现定稿附近的直觉邻域：宽一点止损、加再次转弱确认、保留 MA55 慢线对照。",
        "",
        "## 样本外主表",
        "",
        "| 方案 | OOS PnL | OOS DD | OOS PF | OOS AvgR | OOS Trades | 正收益窗 | 最长连亏 | 最长连止损 | 相比定稿交易数 | 相比定稿PnL |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for run in sorted(
        runs,
        key=lambda item: (
            window_summary[item.candidate.candidate_id]["positive_test_windows"],
            item.oos_metrics.profit_factor or Decimal("0"),
            -item.oos_metrics.max_drawdown,
            item.oos_metrics.pnl,
        ),
        reverse=True,
    ):
        window_stats = window_summary[run.candidate.candidate_id]
        lines.append(
            f"| {run.candidate.label} | {_fmt(run.oos_metrics.pnl)} | {_fmt(run.oos_metrics.max_drawdown)} | "
            f"{_fmt(run.oos_metrics.profit_factor)} | {_fmt(run.oos_metrics.avg_r)} | {run.oos_metrics.trades} | "
            f"{window_stats['positive_test_windows']}/{window_count} | {run.oos_streaks.max_loss_streak} | "
            f"{run.oos_streaks.max_stop_streak} | {run.oos_metrics.trades - baseline.oos_metrics.trades} | "
            f"{_fmt(run.oos_metrics.pnl - baseline.oos_metrics.pnl)} |"
        )

    lines.extend(
        [
            "",
            "## 全样本 vs 样本外",
            "",
            "| 方案 | Full PnL | Full DD | Full PF | Full Trades | OOS PnL | OOS DD | OOS PF | OOS Trades |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for run in sorted(runs, key=lambda item: score_metrics(item.oos_metrics), reverse=True):
        lines.append(
            f"| {run.candidate.label} | {_fmt(run.full_metrics.pnl)} | {_fmt(run.full_metrics.max_drawdown)} | "
            f"{_fmt(run.full_metrics.profit_factor)} | {run.full_metrics.trades} | {_fmt(run.oos_metrics.pnl)} | "
            f"{_fmt(run.oos_metrics.max_drawdown)} | {_fmt(run.oos_metrics.profit_factor)} | {run.oos_metrics.trades} |"
        )

    lines.extend(
        [
            "",
            "## Walk-Forward 摘要",
            "",
            "| 方案 | 测试窗正收益 | 被训练窗选中 | 被选中后正收益 |",
            "| --- | ---: | ---: | ---: |",
        ]
    )
    for run in sorted(
        runs,
        key=lambda item: (
            window_summary[item.candidate.candidate_id]["positive_test_windows"],
            window_summary[item.candidate.candidate_id]["selected_positive_windows"],
            item.oos_metrics.profit_factor or Decimal("0"),
        ),
        reverse=True,
    ):
        window_stats = window_summary[run.candidate.candidate_id]
        selected_positive_text = (
            "-"
            if window_stats["selected_windows"] == 0
            else f"{window_stats['selected_positive_windows']}/{window_stats['selected_windows']}"
        )
        lines.append(
            f"| {run.candidate.label} | {window_stats['positive_test_windows']}/{window_count} | "
            f"{window_stats['selected_windows']}/{window_count} | "
            f"{selected_positive_text} |"
        )

    lines.extend(
        [
            "",
            "## 观察",
            "",
            f"- 定稿基准：OOS `PnL {_fmt(baseline.oos_metrics.pnl)}` / `DD {_fmt(baseline.oos_metrics.max_drawdown)}` / `PF {_fmt(baseline.oos_metrics.profit_factor)}` / `Trades {baseline.oos_metrics.trades}` / `最长连亏 {baseline.oos_streaks.max_loss_streak}`。",
        ]
    )
    best_stats = window_summary[best_tweak.candidate.candidate_id]
    lines.append(
        f"- 整体最值得继续看的微调，是 `{best_tweak.candidate.label}`：OOS `PnL {_fmt(best_tweak.oos_metrics.pnl)}` / "
        f"`DD {_fmt(best_tweak.oos_metrics.max_drawdown)}` / `PF {_fmt(best_tweak.oos_metrics.profit_factor)}` / "
        f"`Trades {best_tweak.oos_metrics.trades}` / `测试窗正收益 {best_stats['positive_test_windows']}/{window_count}`。"
    )
    if stable_pick is not None:
        stable_stats = window_summary[stable_pick.candidate.candidate_id]
        lines.append(
            f"- 如果硬性要求 `DD 不高于定稿`，稳健备选里最值得看的，是 `{stable_pick.candidate.label}`："
            f"OOS `PnL {_fmt(stable_pick.oos_metrics.pnl)}` / `DD {_fmt(stable_pick.oos_metrics.max_drawdown)}` / "
            f"`PF {_fmt(stable_pick.oos_metrics.profit_factor)}` / `Trades {stable_pick.oos_metrics.trades}` / "
            f"`测试窗正收益 {stable_stats['positive_test_windows']}/{window_count}`。"
        )
    lines.extend(
        [
            "- 如果某组只是交易数变少，但 PF、测试窗胜率和回撤没有同步变好，就视为“单纯降频”，不算真正稳健化。",
            "- 这轮重点不是找更高收益上限，而是判断 DOGE 的强连续止损感，究竟来自止损太紧，还是来自动态离场后的反复重开仓。",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> None:
    runs = run_candidates()
    sample_end_ts = max(trade.exit_ts for run in runs for trade in run.trades)
    sample_start_ts = min(trade.entry_ts for run in runs for trade in run.trades)
    sample_start = datetime.fromtimestamp(sample_start_ts / 1000, tz=timezone.utc)
    sample_end = datetime.fromtimestamp(sample_end_ts / 1000, tz=timezone.utc)
    window_rows = build_window_results(runs, sample_end)
    window_summary = summarize_window_outcomes(window_rows)

    write_summary_csv(runs, window_summary)
    write_window_csv(window_rows)

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "symbol": SYMBOL,
        "bar": BAR,
        "sample_start_utc": sample_start.strftime("%Y-%m-%d %H:%M:%S UTC"),
        "sample_end_utc": sample_end.strftime("%Y-%m-%d %H:%M:%S UTC"),
        "oos_start_utc": OOS_START.strftime("%Y-%m-%d %H:%M:%S UTC"),
        "train_months": TRAIN_MONTHS,
        "test_months": TEST_MONTHS,
        "step_months": STEP_MONTHS,
        "candidates": json_ready(runs),
        "windows": json_ready(window_rows),
        "window_summary": json_ready(window_summary),
    }
    JSON_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    markdown = build_markdown(runs=runs, window_rows=window_rows, sample_start=sample_start, sample_end=sample_end)
    MD_PATH.write_text(markdown, encoding="utf-8")
    LATEST_MD_PATH.write_text(markdown, encoding="utf-8")
    LATEST_CSV_PATH.write_text(SUMMARY_CSV_PATH.read_text(encoding="utf-8-sig"), encoding="utf-8-sig")
    LATEST_JSON_PATH.write_text(JSON_PATH.read_text(encoding="utf-8"), encoding="utf-8")
    print(MD_PATH)
    print(SUMMARY_CSV_PATH)
    print(WINDOW_CSV_PATH)
    print(JSON_PATH)


if __name__ == "__main__":
    main()
