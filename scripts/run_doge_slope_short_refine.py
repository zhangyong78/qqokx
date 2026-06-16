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
from okx_quant.models import StrategyConfig
from okx_quant.okx_client import OkxRestClient
from okx_quant.persistence import analysis_report_dir_path
from okx_quant.pricing import format_decimal_fixed
from scripts.build_best_parameter_bundle import build_slope_short_config


REPORT_DIR = analysis_report_dir_path()
REPORT_DIR.mkdir(parents=True, exist_ok=True)
PROJECT_REPORT_DIR = ROOT / "reports"
PROJECT_REPORT_DIR.mkdir(parents=True, exist_ok=True)

SYMBOL = "DOGE-USDT-SWAP"
BAR = "1H"
INITIAL_CAPITAL = Decimal("10000")
RISK_AMOUNT = Decimal("100")
MAKER_FEE_RATE = Decimal("0")
TAKER_FEE_RATE = Decimal("0.00036")
OOS_START_TS = int(datetime(2022, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)

STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
BASE_NAME = f"doge_slope_short_refine_{STAMP}"
CSV_PATH = REPORT_DIR / f"{BASE_NAME}.csv"
JSON_PATH = REPORT_DIR / f"{BASE_NAME}.json"
MD_PATH = REPORT_DIR / f"{BASE_NAME}.md"
LATEST_MD_PATH = PROJECT_REPORT_DIR / "doge_slope_short_refine_latest.md"
LATEST_CSV_PATH = PROJECT_REPORT_DIR / "doge_slope_short_refine_latest.csv"
LATEST_JSON_PATH = PROJECT_REPORT_DIR / "doge_slope_short_refine_latest.json"


@dataclass(frozen=True)
class PhaseConfig:
    phase: str
    candidate_id: str
    label: str
    ema_type: str
    ema_period: int
    trend_ema_type: str
    trend_ema_period: int
    atr_period: int
    break_even_trigger_r: int
    lock_trigger_r: int
    first_lock_r: int
    trailing_step_r: int
    time_stop_break_even_bars: int = 10


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
class PhaseResult:
    config: PhaseConfig
    full_metrics: Metrics
    oos_metrics: Metrics
    years_positive_full: int
    years_positive_oos: int
    yearly_pnl_full: tuple[tuple[str, Decimal], ...]
    yearly_pnl_oos: tuple[tuple[str, Decimal], ...]


def build_metrics(trades: list[BacktestTrade]) -> Metrics:
    report = _build_report(trades, initial_capital=INITIAL_CAPITAL)
    return Metrics(
        pnl=report.total_pnl,
        trades=report.total_trades,
        win_rate=report.win_rate,
        profit_factor=report.profit_factor,
        avg_r=report.average_r_multiple,
        max_drawdown=report.max_drawdown,
        return_pct=report.total_return_pct,
    )


def filter_oos_trades(trades: list[BacktestTrade]) -> list[BacktestTrade]:
    return [trade for trade in trades if trade.exit_ts >= OOS_START_TS]


def summarize_yearly_pnl(trades: list[BacktestTrade], *, min_year: int | None = None) -> tuple[tuple[str, Decimal], ...]:
    buckets: dict[str, Decimal] = {}
    for trade in trades:
        year = datetime.fromtimestamp(trade.exit_ts / 1000, tz=timezone.utc).strftime("%Y")
        if min_year is not None and int(year) < min_year:
            continue
        buckets[year] = buckets.get(year, Decimal("0")) + trade.pnl
    return tuple(sorted(buckets.items(), key=lambda item: item[0]))


def count_positive_years(summary: tuple[tuple[str, Decimal], ...]) -> int:
    return sum(1 for _, pnl in summary if pnl > 0)


def make_dynamic_rules(
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


def build_config(phase_config: PhaseConfig) -> StrategyConfig:
    return build_slope_short_config(
        symbol=SYMBOL,
        ema_period=phase_config.ema_period,
        ema_type=phase_config.ema_type,
        trend_ema_period=phase_config.trend_ema_period,
        trend_ema_type=phase_config.trend_ema_type,
        atr_period=phase_config.atr_period,
        dynamic_break_even_trigger_r=phase_config.break_even_trigger_r,
        dynamic_first_lock_r=phase_config.first_lock_r,
        dynamic_trailing_step_r=phase_config.trailing_step_r,
        dynamic_protection_rules=make_dynamic_rules(
            break_even_trigger_r=phase_config.break_even_trigger_r,
            lock_trigger_r=phase_config.lock_trigger_r,
            first_lock_r=phase_config.first_lock_r,
            trailing_step_r=phase_config.trailing_step_r,
        ),
        ema55_slope_lock_profit_trigger_r=phase_config.lock_trigger_r,
        time_stop_break_even_bars=phase_config.time_stop_break_even_bars,
    )


def phase1_candidates() -> tuple[PhaseConfig, ...]:
    return (
        PhaseConfig(
            phase="line",
            candidate_id="line_ma21",
            label="MA21 / ATR14 / 2R保本 / 5R锁4R / step1",
            ema_type="ma",
            ema_period=21,
            trend_ema_type="ma",
            trend_ema_period=21,
            atr_period=14,
            break_even_trigger_r=2,
            lock_trigger_r=5,
            first_lock_r=4,
            trailing_step_r=1,
        ),
        PhaseConfig(
            phase="line",
            candidate_id="line_ma55",
            label="MA55 / ATR14 / 2R保本 / 5R锁4R / step1",
            ema_type="ma",
            ema_period=55,
            trend_ema_type="ma",
            trend_ema_period=55,
            atr_period=14,
            break_even_trigger_r=2,
            lock_trigger_r=5,
            first_lock_r=4,
            trailing_step_r=1,
        ),
    )


def phase2_candidates(base: PhaseConfig) -> tuple[PhaseConfig, ...]:
    candidates: list[PhaseConfig] = []
    for atr_period in (12, 13, 14, 15, 16):
        candidates.append(
            PhaseConfig(
                phase="atr",
                candidate_id=f"atr_{atr_period}",
                label=(
                    f"{base.ema_type.upper()}{base.ema_period} / ATR{atr_period} / "
                    f"{base.break_even_trigger_r}R保本 / {base.lock_trigger_r}R锁{base.first_lock_r}R / step{base.trailing_step_r}"
                ),
                ema_type=base.ema_type,
                ema_period=base.ema_period,
                trend_ema_type=base.trend_ema_type,
                trend_ema_period=base.trend_ema_period,
                atr_period=atr_period,
                break_even_trigger_r=base.break_even_trigger_r,
                lock_trigger_r=base.lock_trigger_r,
                first_lock_r=base.first_lock_r,
                trailing_step_r=base.trailing_step_r,
                time_stop_break_even_bars=base.time_stop_break_even_bars,
            )
        )
    return tuple(candidates)


def phase3_candidates(base: PhaseConfig) -> tuple[PhaseConfig, ...]:
    specs = (
        ("r_2_5_4_1", 2, 5, 4, 1),
        ("r_2_6_5_1", 2, 6, 5, 1),
        ("r_3_6_5_1", 3, 6, 5, 1),
        ("r_3_7_6_1", 3, 7, 6, 1),
        ("r_4_7_6_1", 4, 7, 6, 1),
        ("r_3_6_4_2", 3, 6, 4, 2),
        ("r_3_7_5_2", 3, 7, 5, 2),
        ("r_4_8_6_2", 4, 8, 6, 2),
    )
    candidates: list[PhaseConfig] = []
    for candidate_id, break_even_trigger_r, lock_trigger_r, first_lock_r, trailing_step_r in specs:
        candidates.append(
            PhaseConfig(
                phase="r",
                candidate_id=candidate_id,
                label=(
                    f"{base.ema_type.upper()}{base.ema_period} / ATR{base.atr_period} / "
                    f"{break_even_trigger_r}R保本 / {lock_trigger_r}R锁{first_lock_r}R / step{trailing_step_r}"
                ),
                ema_type=base.ema_type,
                ema_period=base.ema_period,
                trend_ema_type=base.trend_ema_type,
                trend_ema_period=base.trend_ema_period,
                atr_period=base.atr_period,
                break_even_trigger_r=break_even_trigger_r,
                lock_trigger_r=lock_trigger_r,
                first_lock_r=first_lock_r,
                trailing_step_r=trailing_step_r,
                time_stop_break_even_bars=base.time_stop_break_even_bars,
            )
        )
    return tuple(candidates)


def score_result(result: PhaseResult) -> tuple[Decimal, Decimal, Decimal, Decimal]:
    oos_pf = result.oos_metrics.profit_factor or Decimal("0")
    dd_penalty = -result.oos_metrics.max_drawdown
    return (result.oos_metrics.pnl, oos_pf, dd_penalty, result.full_metrics.pnl)


def select_best(results: tuple[PhaseResult, ...]) -> PhaseResult:
    return max(results, key=score_result)


def run_phase(*, candles, instrument, candidates: tuple[PhaseConfig, ...]) -> tuple[PhaseResult, ...]:
    phase_results: list[PhaseResult] = []
    total = len(candidates)
    for index, candidate in enumerate(candidates, start=1):
        print(f"[{candidate.phase} {index}/{total}] {candidate.label}")
        result = _run_backtest_with_loaded_data(
            candles,
            instrument,
            build_config(candidate),
            data_source_note=f"local candle_cache full history | {SYMBOL} | {BAR}",
            maker_fee_rate=MAKER_FEE_RATE,
            taker_fee_rate=TAKER_FEE_RATE,
        )
        full_trades = list(result.trades)
        oos_trades = filter_oos_trades(full_trades)
        yearly_full = summarize_yearly_pnl(full_trades)
        yearly_oos = summarize_yearly_pnl(full_trades, min_year=2022)
        phase_results.append(
            PhaseResult(
                config=candidate,
                full_metrics=build_metrics(full_trades),
                oos_metrics=build_metrics(oos_trades),
                years_positive_full=count_positive_years(yearly_full),
                years_positive_oos=count_positive_years(yearly_oos),
                yearly_pnl_full=yearly_full,
                yearly_pnl_oos=yearly_oos,
            )
        )
    return tuple(phase_results)


def phase_rows(results: tuple[PhaseResult, ...]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for item in results:
        rows.append(
            {
                "phase": item.config.phase,
                "candidate_id": item.config.candidate_id,
                "label": item.config.label,
                "ema_type": item.config.ema_type,
                "ema_period": item.config.ema_period,
                "trend_ema_type": item.config.trend_ema_type,
                "trend_ema_period": item.config.trend_ema_period,
                "atr_period": item.config.atr_period,
                "break_even_trigger_r": item.config.break_even_trigger_r,
                "lock_trigger_r": item.config.lock_trigger_r,
                "first_lock_r": item.config.first_lock_r,
                "trailing_step_r": item.config.trailing_step_r,
                "full_pnl": str(item.full_metrics.pnl),
                "full_trades": item.full_metrics.trades,
                "full_win_rate": str(item.full_metrics.win_rate),
                "full_pf": None if item.full_metrics.profit_factor is None else str(item.full_metrics.profit_factor),
                "full_avg_r": str(item.full_metrics.avg_r),
                "full_max_drawdown": str(item.full_metrics.max_drawdown),
                "oos_pnl": str(item.oos_metrics.pnl),
                "oos_trades": item.oos_metrics.trades,
                "oos_win_rate": str(item.oos_metrics.win_rate),
                "oos_pf": None if item.oos_metrics.profit_factor is None else str(item.oos_metrics.profit_factor),
                "oos_avg_r": str(item.oos_metrics.avg_r),
                "oos_max_drawdown": str(item.oos_metrics.max_drawdown),
                "positive_years_full": item.years_positive_full,
                "positive_years_oos": item.years_positive_oos,
            }
        )
    return rows


def write_csv(rows: list[dict[str, object]]) -> None:
    fieldnames = [
        "phase",
        "candidate_id",
        "label",
        "ema_type",
        "ema_period",
        "trend_ema_type",
        "trend_ema_period",
        "atr_period",
        "break_even_trigger_r",
        "lock_trigger_r",
        "first_lock_r",
        "trailing_step_r",
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
        "positive_years_full",
        "positive_years_oos",
    ]
    with CSV_PATH.open("w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def fmt(value: Decimal | None, digits: int = 4) -> str:
    if value is None:
        return "-"
    return format_decimal_fixed(value, digits)


def json_ready(value: object) -> object:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, tuple):
        return [json_ready(item) for item in value]
    if isinstance(value, list):
        return [json_ready(item) for item in value]
    if isinstance(value, dict):
        return {str(key): json_ready(item) for key, item in value.items()}
    return value


def overfit_risk_text(final_result: PhaseResult) -> str:
    risk_flags: list[str] = []
    if final_result.oos_metrics.pnl <= 0:
        risk_flags.append("样本外总盈亏不为正")
    if final_result.oos_metrics.profit_factor is None or final_result.oos_metrics.profit_factor <= Decimal("1"):
        risk_flags.append("样本外 PF <= 1")
    if final_result.full_metrics.pnl > 0 and final_result.oos_metrics.pnl < final_result.full_metrics.pnl * Decimal("0.35"):
        risk_flags.append("样本外收益显著低于全样本")
    if not risk_flags:
        return "未见明显过拟合信号：胜出参数在全样本与 2022-01-01 之后样本外都保持正收益，且样本外 PF 仍大于 1。"
    return "存在一定过拟合风险：" + "；".join(risk_flags) + "。"


def format_ts(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def build_markdown(
    *,
    sample_start_ts: int,
    sample_end_ts: int,
    phase1_results: tuple[PhaseResult, ...],
    phase2_results: tuple[PhaseResult, ...],
    phase3_results: tuple[PhaseResult, ...],
    phase1_best: PhaseResult,
    phase2_best: PhaseResult,
    final_best: PhaseResult,
) -> str:
    lines = [
        "# DOGE 斜率做空打磨",
        "",
        "## 锁死口径",
        "",
        f"- 标的：`{SYMBOL}`",
        f"- 周期：`{BAR}`",
        f"- 样本范围：`{format_ts(sample_start_ts)}` -> `{format_ts(sample_end_ts)}`",
        "- 样本外：`2022-01-01 00:00:00 UTC` 之后按平仓时间统计",
        "- K线来源：本地 `candle_cache` 全历史已收盘K线",
        f"- 固定风险金：`{fmt(RISK_AMOUNT, 0)}`U / 笔",
        f"- 初始资金：`{fmt(INITIAL_CAPITAL, 0)}`U",
        f"- 手续费：Maker `{fmt(MAKER_FEE_RATE * Decimal('100'), 4)}%` / Taker `{fmt(TAKER_FEE_RATE * Decimal('100'), 4)}%`",
        "- 其他执行口径不变，只比较参数组合",
        "",
        "## 第一层：主导均线",
        "",
        "| 方案 | 全样本PnL | 全样本DD | 全样本PF | 全样本AvgR | 交易笔数 | 样本外PnL | 样本外DD | 样本外PF | 样本外AvgR | 样本外交易笔数 |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for item in sorted(phase1_results, key=score_result, reverse=True):
        lines.append(
            f"| {item.config.label} | {fmt(item.full_metrics.pnl)} | {fmt(item.full_metrics.max_drawdown)} | "
            f"{fmt(item.full_metrics.profit_factor)} | {fmt(item.full_metrics.avg_r)} | {item.full_metrics.trades} | "
            f"{fmt(item.oos_metrics.pnl)} | {fmt(item.oos_metrics.max_drawdown)} | {fmt(item.oos_metrics.profit_factor)} | "
            f"{fmt(item.oos_metrics.avg_r)} | {item.oos_metrics.trades} |"
        )
    lines.extend(
        [
            "",
            f"- 第一层胜出：`{phase1_best.config.label}`",
            "",
            "## 第二层：ATR",
            "",
            "| 方案 | 全样本PnL | 全样本DD | 全样本PF | AvgR | 交易笔数 | 样本外PnL | 样本外DD | 样本外PF | 样本外AvgR | 样本外交易笔数 |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for item in sorted(phase2_results, key=score_result, reverse=True):
        lines.append(
            f"| {item.config.label} | {fmt(item.full_metrics.pnl)} | {fmt(item.full_metrics.max_drawdown)} | "
            f"{fmt(item.full_metrics.profit_factor)} | {fmt(item.full_metrics.avg_r)} | {item.full_metrics.trades} | "
            f"{fmt(item.oos_metrics.pnl)} | {fmt(item.oos_metrics.max_drawdown)} | {fmt(item.oos_metrics.profit_factor)} | "
            f"{fmt(item.oos_metrics.avg_r)} | {item.oos_metrics.trades} |"
        )
    lines.extend(
        [
            "",
            f"- 第二层胜出：`{phase2_best.config.label}`",
            "",
            "## 第三层：R参数",
            "",
            "| 方案 | 全样本PnL | 全样本DD | 全样本PF | AvgR | 交易笔数 | 样本外PnL | 样本外DD | 样本外PF | 样本外AvgR | 样本外交易笔数 |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for item in sorted(phase3_results, key=score_result, reverse=True):
        lines.append(
            f"| {item.config.label} | {fmt(item.full_metrics.pnl)} | {fmt(item.full_metrics.max_drawdown)} | "
            f"{fmt(item.full_metrics.profit_factor)} | {fmt(item.full_metrics.avg_r)} | {item.full_metrics.trades} | "
            f"{fmt(item.oos_metrics.pnl)} | {fmt(item.oos_metrics.max_drawdown)} | {fmt(item.oos_metrics.profit_factor)} | "
            f"{fmt(item.oos_metrics.avg_r)} | {item.oos_metrics.trades} |"
        )
    lines.extend(
        [
            "",
            "## 定稿",
            "",
            f"- 主导均线：`{final_best.config.ema_type.upper()}{final_best.config.ema_period}`",
            f"- ATR：`{final_best.config.atr_period}`",
            (
                f"- R结构：`{final_best.config.break_even_trigger_r}R` 保本，"
                f"`{final_best.config.lock_trigger_r}R` 锁 `"
                f"{final_best.config.first_lock_r}R`，之后每 `"
                f"{final_best.config.trailing_step_r}R` 再上移 `"
                f"{final_best.config.trailing_step_r}R`"
            ),
            f"- 全样本：PnL `{fmt(final_best.full_metrics.pnl)}` / DD `{fmt(final_best.full_metrics.max_drawdown)}` / PF `{fmt(final_best.full_metrics.profit_factor)}` / AvgR `{fmt(final_best.full_metrics.avg_r)}` / Trades `{final_best.full_metrics.trades}`",
            f"- 样本外：PnL `{fmt(final_best.oos_metrics.pnl)}` / DD `{fmt(final_best.oos_metrics.max_drawdown)}` / PF `{fmt(final_best.oos_metrics.profit_factor)}` / AvgR `{fmt(final_best.oos_metrics.avg_r)}` / Trades `{final_best.oos_metrics.trades}`",
            f"- 正收益年份：全样本 `{final_best.years_positive_full}` 年，样本外 `{final_best.years_positive_oos}` 年",
            f"- 过拟合判断：{overfit_risk_text(final_best)}",
            "",
            "## 年度样本外PnL",
            "",
            "| 年份 | PnL |",
            "| --- | ---: |",
        ]
    )
    for year, pnl in final_best.yearly_pnl_oos:
        lines.append(f"| {year} | {fmt(pnl)} |")
    lines.extend(["", f"- 明细文件：`{CSV_PATH}` / `{JSON_PATH}`", ""])
    return "\n".join(lines)


def main() -> None:
    candles = [candle for candle in load_candle_cache(SYMBOL, BAR, limit=None) if candle.confirmed]
    if not candles:
        raise RuntimeError(f"missing local candles for {SYMBOL} {BAR}")

    client = OkxRestClient()
    instrument = client.get_instrument(SYMBOL)

    phase1_results = run_phase(candles=candles, instrument=instrument, candidates=phase1_candidates())
    phase1_best = select_best(phase1_results)

    phase2_results = run_phase(candles=candles, instrument=instrument, candidates=phase2_candidates(phase1_best.config))
    phase2_best = select_best(phase2_results)

    phase3_results = run_phase(candles=candles, instrument=instrument, candidates=phase3_candidates(phase2_best.config))
    final_best = select_best(phase3_results)

    rows = phase_rows(phase1_results) + phase_rows(phase2_results) + phase_rows(phase3_results)
    write_csv(rows)

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "symbol": SYMBOL,
        "bar": BAR,
        "sample_start_utc": format_ts(candles[0].ts),
        "sample_end_utc": format_ts(candles[-1].ts),
        "oos_start_utc": format_ts(OOS_START_TS),
        "risk_amount": str(RISK_AMOUNT),
        "initial_capital": str(INITIAL_CAPITAL),
        "maker_fee_rate": str(MAKER_FEE_RATE),
        "taker_fee_rate": str(TAKER_FEE_RATE),
        "phase1_best": asdict(phase1_best),
        "phase2_best": asdict(phase2_best),
        "final_best": asdict(final_best),
        "rows": rows,
    }
    JSON_PATH.write_text(json.dumps(json_ready(payload), ensure_ascii=False, indent=2), encoding="utf-8")

    markdown = build_markdown(
        sample_start_ts=candles[0].ts,
        sample_end_ts=candles[-1].ts,
        phase1_results=phase1_results,
        phase2_results=phase2_results,
        phase3_results=phase3_results,
        phase1_best=phase1_best,
        phase2_best=phase2_best,
        final_best=final_best,
    )
    MD_PATH.write_text(markdown, encoding="utf-8")
    LATEST_MD_PATH.write_text(markdown, encoding="utf-8")
    LATEST_CSV_PATH.write_text(CSV_PATH.read_text(encoding="utf-8-sig"), encoding="utf-8-sig")
    LATEST_JSON_PATH.write_text(JSON_PATH.read_text(encoding="utf-8"), encoding="utf-8")

    print(MD_PATH)
    print(CSV_PATH)
    print(JSON_PATH)


if __name__ == "__main__":
    main()
