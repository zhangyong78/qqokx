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
OOS_START = datetime(2022, 1, 1, tzinfo=timezone.utc)
TRAIN_MONTHS = 18
TEST_MONTHS = 6
STEP_MONTHS = 6

STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
BASE_NAME = f"doge_slope_short_overfit_validation_{STAMP}"
SUMMARY_CSV_PATH = REPORT_DIR / f"{BASE_NAME}_summary.csv"
WINDOW_CSV_PATH = REPORT_DIR / f"{BASE_NAME}_windows.csv"
JSON_PATH = REPORT_DIR / f"{BASE_NAME}.json"
MD_PATH = REPORT_DIR / f"{BASE_NAME}.md"
LATEST_MD_PATH = PROJECT_REPORT_DIR / "doge_slope_short_overfit_validation_latest.md"
LATEST_CSV_PATH = PROJECT_REPORT_DIR / "doge_slope_short_overfit_validation_latest.csv"
LATEST_JSON_PATH = PROJECT_REPORT_DIR / "doge_slope_short_overfit_validation_latest.json"


@dataclass(frozen=True)
class Candidate:
    candidate_id: str
    label: str
    config: StrategyConfig


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
class CandidateRun:
    candidate: Candidate
    trades: tuple[BacktestTrade, ...]
    full_metrics: Metrics
    oos_metrics: Metrics
    yearly_pnl: tuple[tuple[str, Decimal], ...]
    oos_yearly_pnl: tuple[tuple[str, Decimal], ...]


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


def _fmt(value: Decimal | None, digits: int = 4) -> str:
    if value is None:
        return "-"
    return format_decimal_fixed(value, digits)


def _fmt_pct(value: Decimal) -> str:
    return f"{format_decimal_fixed(value, 2)}%"


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
    break_even_trigger_r: int,
    lock_trigger_r: int,
    first_lock_r: int,
    trailing_step_r: int,
) -> StrategyConfig:
    return replace(
        config,
        atr_period=atr_period,
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


def build_candidates() -> tuple[Candidate, ...]:
    final_spec = next(item for item in build_specs() if item.profile_id == "slope_short_best_doge_v2")
    final_config = final_spec.config
    ma55_baseline = _with_protection(
        replace(
            final_config,
            ema_period=55,
            ema_type="ma",
            trend_ema_period=55,
            trend_ema_type="ma",
        ),
        atr_period=14,
        break_even_trigger_r=2,
        lock_trigger_r=5,
        first_lock_r=4,
        trailing_step_r=1,
    )
    atr14_neighbor = _with_protection(
        final_config,
        atr_period=14,
        break_even_trigger_r=2,
        lock_trigger_r=5,
        first_lock_r=4,
        trailing_step_r=1,
    )
    be3_neighbor = _with_protection(
        final_config,
        atr_period=13,
        break_even_trigger_r=3,
        lock_trigger_r=6,
        first_lock_r=5,
        trailing_step_r=1,
    )
    step2_neighbor = _with_protection(
        final_config,
        atr_period=13,
        break_even_trigger_r=3,
        lock_trigger_r=6,
        first_lock_r=4,
        trailing_step_r=2,
    )
    return (
        Candidate("final_best", "定稿 MA21 / ATR13 / 2R保本 / 6R锁5R / step1", final_config),
        Candidate("ma55_baseline", "对照 MA55 / ATR14 / 2R保本 / 5R锁4R / step1", ma55_baseline),
        Candidate("atr14_neighbor", "邻近 MA21 / ATR14 / 2R保本 / 5R锁4R / step1", atr14_neighbor),
        Candidate("be3_neighbor", "邻近 MA21 / ATR13 / 3R保本 / 6R锁5R / step1", be3_neighbor),
        Candidate("step2_neighbor", "邻近 MA21 / ATR13 / 3R保本 / 6R锁4R / step2", step2_neighbor),
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


def summarize_yearly_pnl(trades: tuple[BacktestTrade, ...], *, min_year: int | None = None) -> tuple[tuple[str, Decimal], ...]:
    buckets: dict[str, Decimal] = {}
    for trade in trades:
        dt = datetime.fromtimestamp(trade.exit_ts / 1000, tz=timezone.utc)
        if min_year is not None and dt.year < min_year:
            continue
        year = dt.strftime("%Y")
        buckets[year] = buckets.get(year, Decimal("0")) + trade.pnl
    return tuple(sorted(buckets.items(), key=lambda item: item[0]))


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
            data_source_note=f"local candle_cache full history | {SYMBOL} | {BAR} | overfit validation",
            maker_fee_rate=Decimal("0"),
            taker_fee_rate=Decimal("0.00036"),
        )
        trades = tuple(result.trades)
        oos_trades = slice_trades(trades, start=OOS_START, end=datetime.max.replace(tzinfo=timezone.utc))
        runs.append(
            CandidateRun(
                candidate=candidate,
                trades=trades,
                full_metrics=build_metrics(trades),
                oos_metrics=build_metrics(oos_trades),
                yearly_pnl=summarize_yearly_pnl(trades),
                oos_yearly_pnl=summarize_yearly_pnl(trades, min_year=2022),
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


def stability_verdict(
    final_run: CandidateRun,
    final_window_rows: tuple[WindowResult, ...],
    selected_window_rows: tuple[WindowResult, ...],
    selected_count: int,
) -> str:
    positive_test_windows = sum(1 for row in final_window_rows if row.test_metrics.pnl > 0)
    total_test_windows = len(final_window_rows)
    selected_positive = sum(1 for row in selected_window_rows if row.test_metrics.pnl > 0)
    if (
        final_run.oos_metrics.profit_factor is not None
        and final_run.oos_metrics.profit_factor > Decimal("1")
        and positive_test_windows >= max(total_test_windows - 2, 1)
        and selected_count >= max(total_test_windows // 2, 1)
    ):
        return "未见明显过拟合：样本外 PF 仍大于 1，walk-forward 测试窗多数为正，且定稿组在训练窗里经常仍是优选。"
    if (
        final_run.oos_metrics.profit_factor is not None
        and final_run.oos_metrics.profit_factor > Decimal("1")
        and positive_test_windows >= max(total_test_windows // 2, 1)
        and selected_positive >= max(total_test_windows // 2, 1)
    ):
        return "没有看到强过拟合证据，但存在中等稳定性压力：样本外仍赚钱，不过测试窗并非全程顺滑，连续亏损段需要靠仓位和停机纪律承受。"
    return "存在较明显的过拟合或稳定性不足风险：样本外和 walk-forward 测试窗没有形成足够一致的正收益。"


def write_summary_csv(runs: tuple[CandidateRun, ...]) -> None:
    fieldnames = [
        "candidate_id",
        "label",
        "full_pnl",
        "full_trades",
        "full_win_rate",
        "full_pf",
        "full_avg_r",
        "full_max_drawdown",
        "oos_pnl",
        "oos_trades",
        "oos_win_rate",
        "oos_pf",
        "oos_avg_r",
        "oos_max_drawdown",
    ]
    with SUMMARY_CSV_PATH.open("w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for run in runs:
            writer.writerow(
                {
                    "candidate_id": run.candidate.candidate_id,
                    "label": run.candidate.label,
                    "full_pnl": _fmt(run.full_metrics.pnl),
                    "full_trades": run.full_metrics.trades,
                    "full_win_rate": _fmt(run.full_metrics.win_rate),
                    "full_pf": _fmt(run.full_metrics.profit_factor),
                    "full_avg_r": _fmt(run.full_metrics.avg_r),
                    "full_max_drawdown": _fmt(run.full_metrics.max_drawdown),
                    "oos_pnl": _fmt(run.oos_metrics.pnl),
                    "oos_trades": run.oos_metrics.trades,
                    "oos_win_rate": _fmt(run.oos_metrics.win_rate),
                    "oos_pf": _fmt(run.oos_metrics.profit_factor),
                    "oos_avg_r": _fmt(run.oos_metrics.avg_r),
                    "oos_max_drawdown": _fmt(run.oos_metrics.max_drawdown),
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
        "train_max_drawdown",
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
                    "train_max_drawdown": _fmt(row.train_metrics.max_drawdown),
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
    final_run = next(run for run in runs if run.candidate.candidate_id == "final_best")
    final_window_rows = tuple(row for row in window_rows if row.candidate_id == "final_best")
    selected_rows: list[WindowResult] = []
    selected_final_count = 0
    for window_index in sorted({row.window.index for row in window_rows}):
        bucket = [row for row in window_rows if row.window.index == window_index]
        chosen = max(bucket, key=lambda item: score_metrics(item.train_metrics))
        selected_rows.append(chosen)
        if chosen.candidate_id == "final_best":
            selected_final_count += 1
    streaks_full = compute_streak_stats(final_run.trades)
    streaks_oos = compute_streak_stats(tuple(trade for trade in final_run.trades if trade.exit_ts >= int(OOS_START.timestamp() * 1000)))

    lines = [
        "# DOGE 斜率做空过拟合验证",
        "",
        "## 锁死口径",
        "",
        f"- 标的：`{SYMBOL}`",
        f"- 周期：`{BAR}`",
        f"- 样本范围：`{sample_start.strftime('%Y-%m-%d %H:%M:%S UTC')}` -> `{sample_end.strftime('%Y-%m-%d %H:%M:%S UTC')}`",
        f"- 样本外起点：`{OOS_START.strftime('%Y-%m-%d %H:%M:%S UTC')}`",
        f"- 固定风险金：`100U` / 笔，初始资金 `10000U`，非复利",
        "- 手续费：Taker `0.0360%`，其余执行口径保持不变",
        "- 候选集：定稿参数 + 4 个邻近组合，只做稳定性验证，不扩展冷门参数搜索",
        "",
        "## 全样本 / 样本外对照",
        "",
        "| 方案 | 全样本PnL | 全样本DD | 全样本PF | 全样本AvgR | 全样本交易数 | 样本外PnL | 样本外DD | 样本外PF | 样本外AvgR | 样本外交易数 |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for run in sorted(runs, key=lambda item: score_metrics(item.oos_metrics), reverse=True):
        lines.append(
            f"| {run.candidate.label} | {_fmt(run.full_metrics.pnl)} | {_fmt(run.full_metrics.max_drawdown)} | "
            f"{_fmt(run.full_metrics.profit_factor)} | {_fmt(run.full_metrics.avg_r)} | {run.full_metrics.trades} | "
            f"{_fmt(run.oos_metrics.pnl)} | {_fmt(run.oos_metrics.max_drawdown)} | {_fmt(run.oos_metrics.profit_factor)} | "
            f"{_fmt(run.oos_metrics.avg_r)} | {run.oos_metrics.trades} |"
        )

    lines.extend(
        [
            "",
            "## Walk-Forward（18个月训练 / 6个月测试 / 6个月步进）",
            "",
            "| 窗口 | 训练冠军 | 训练PnL | 测试PnL | 测试PF | 定稿测试PnL | 定稿测试PF |",
            "| --- | --- | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for selected in selected_rows:
        final_row = next(row for row in final_window_rows if row.window.index == selected.window.index)
        lines.append(
            f"| W{selected.window.index} `{_dt_text(selected.window.test_start)}->{_dt_text(selected.window.test_end)}` | "
            f"{selected.label} | {_fmt(selected.train_metrics.pnl)} | {_fmt(selected.test_metrics.pnl)} | {_fmt(selected.test_metrics.profit_factor)} | "
            f"{_fmt(final_row.test_metrics.pnl)} | {_fmt(final_row.test_metrics.profit_factor)} |"
        )

    final_positive_windows = sum(1 for row in final_window_rows if row.test_metrics.pnl > 0)
    selected_positive_windows = sum(1 for row in selected_rows if row.test_metrics.pnl > 0)
    lines.extend(
        [
            "",
            "## 连续亏损 / 连续止损",
            "",
            f"- 全样本最长连续亏损：`{streaks_full.max_loss_streak}` 笔，区间 `{streaks_full.max_loss_start}` -> `{streaks_full.max_loss_end}`",
            f"- 全样本最长连续止损：`{streaks_full.max_stop_streak}` 笔，区间 `{streaks_full.max_stop_start}` -> `{streaks_full.max_stop_end}`",
            f"- 全样本退出原因：止损 `{streaks_full.stop_loss_count}` / 斜率转正平仓 `{streaks_full.slope_turn_count}` / 保本 `{streaks_full.breakeven_count}`",
            f"- 样本外最长连续亏损：`{streaks_oos.max_loss_streak}` 笔，区间 `{streaks_oos.max_loss_start}` -> `{streaks_oos.max_loss_end}`",
            f"- 样本外最长连续止损：`{streaks_oos.max_stop_streak}` 笔，区间 `{streaks_oos.max_stop_start}` -> `{streaks_oos.max_stop_end}`",
            "",
            "## 定稿年度样本外PnL",
            "",
            "| 年份 | PnL |",
            "| --- | ---: |",
        ]
    )
    for year, pnl in final_run.oos_yearly_pnl:
        lines.append(f"| {year} | {_fmt(pnl)} |")

    verdict = stability_verdict(final_run, final_window_rows, tuple(selected_rows), selected_final_count)
    lines.extend(
        [
            "",
            "## 结论",
            "",
            f"- 定稿全样本：PnL `{_fmt(final_run.full_metrics.pnl)}` / PF `{_fmt(final_run.full_metrics.profit_factor)}` / DD `{_fmt(final_run.full_metrics.max_drawdown)}` / Trades `{final_run.full_metrics.trades}`",
            f"- 定稿样本外：PnL `{_fmt(final_run.oos_metrics.pnl)}` / PF `{_fmt(final_run.oos_metrics.profit_factor)}` / DD `{_fmt(final_run.oos_metrics.max_drawdown)}` / Trades `{final_run.oos_metrics.trades}`",
            f"- 定稿测试窗正收益：`{final_positive_windows}/{len(final_window_rows)}`；训练冠军测试窗正收益：`{selected_positive_windows}/{len(selected_rows)}`；定稿在训练窗被选为冠军：`{selected_final_count}/{len(selected_rows)}`",
            f"- 过拟合判断：{verdict}",
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

    write_summary_csv(runs)
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
