from __future__ import annotations

import csv
import json
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
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
from okx_quant.strategy_catalog import STRATEGY_DYNAMIC_LONG_ID


REPORT_DIR = analysis_report_dir_path()
REPORT_DIR.mkdir(parents=True, exist_ok=True)
PROJECT_REPORT_DIR = ROOT / "reports"
PROJECT_REPORT_DIR.mkdir(parents=True, exist_ok=True)

SYMBOL = "SOL-USDT-SWAP"
BAR = "1H"
INITIAL_CAPITAL = Decimal("10000")
RISK_AMOUNT = Decimal("100")
MAKER_FEE_RATE = Decimal("0")
TAKER_FEE_RATE = Decimal("0.00036")
OOS_START = datetime(2024, 1, 1, tzinfo=timezone.utc)

STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
BASE_NAME = f"sol_dynamic_long_lock5_validation_{STAMP}"
SUMMARY_CSV_PATH = REPORT_DIR / f"{BASE_NAME}_summary.csv"
YEARLY_CSV_PATH = REPORT_DIR / f"{BASE_NAME}_yearly.csv"
QUARTERLY_CSV_PATH = REPORT_DIR / f"{BASE_NAME}_quarterly.csv"
ROLLING_CSV_PATH = REPORT_DIR / f"{BASE_NAME}_rolling_6m.csv"
JSON_PATH = REPORT_DIR / f"{BASE_NAME}.json"
MD_PATH = REPORT_DIR / f"{BASE_NAME}.md"
LATEST_MD_PATH = PROJECT_REPORT_DIR / "sol_dynamic_long_lock5_validation_latest.md"
LATEST_CSV_PATH = PROJECT_REPORT_DIR / "sol_dynamic_long_lock5_validation_latest.csv"
LATEST_JSON_PATH = PROJECT_REPORT_DIR / "sol_dynamic_long_lock5_validation_latest.json"


@dataclass(frozen=True)
class Candidate:
    candidate_id: str
    label: str
    first_lock_trigger_r: int
    max_entries_per_trend: int = 2


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
class TailStats:
    top5_share_pct: Decimal
    top10_share_pct: Decimal
    top20_share_pct: Decimal
    pnl_without_top20: Decimal
    positive_months: int
    negative_months: int
    total_months: int
    max_loss_streak: int


@dataclass(frozen=True)
class PeriodRow:
    period: str
    pnl: Decimal
    trades: int
    win_rate: Decimal
    profit_factor: Decimal | None
    avg_r: Decimal


@dataclass(frozen=True)
class CandidateRun:
    candidate: Candidate
    full_metrics: Metrics
    oos_metrics: Metrics
    oos_tail: TailStats
    oos_yearly: tuple[PeriodRow, ...]
    oos_quarterly: tuple[PeriodRow, ...]
    rolling_6m: tuple[PeriodRow, ...]


BASELINE = Candidate(
    candidate_id="baseline_s656",
    label="基线 | SL1 / 3R保本 / 7R锁1R / 11R锁10R / 每波2次",
    first_lock_trigger_r=7,
)

CHALLENGER = Candidate(
    candidate_id="candidate_lock5",
    label="候选 | SL1 / 3R保本 / 5R锁1R / 11R锁10R / 每波2次",
    first_lock_trigger_r=5,
)


def fmt(value: Decimal | None, digits: int = 4) -> str:
    if value is None:
        return "-"
    return format_decimal_fixed(value, digits)


def fmt_pct(value: Decimal) -> str:
    return f"{format_decimal_fixed(value, 2)}%"


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


def _trade_dt(trade: BacktestTrade) -> datetime:
    return datetime.fromtimestamp(trade.exit_ts / 1000, tz=timezone.utc)


def _period_key(dt: datetime, mode: str) -> str:
    if mode == "year":
        return dt.strftime("%Y")
    if mode == "quarter":
        quarter = (dt.month - 1) // 3 + 1
        return f"{dt.year}-Q{quarter}"
    if mode == "month":
        return dt.strftime("%Y-%m")
    raise ValueError(f"unsupported mode: {mode}")


def _add_months(dt: datetime, months: int) -> datetime:
    year = dt.year + (dt.month - 1 + months) // 12
    month = (dt.month - 1 + months) % 12 + 1
    return datetime(year, month, 1, tzinfo=dt.tzinfo)


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
    return [trade for trade in trades if _trade_dt(trade) >= OOS_START]


def make_rules(first_lock_trigger_r: int) -> tuple[dict[str, object], ...]:
    return (
        {
            "trigger_r": 3,
            "action": "break_even",
            "lock_r": None,
            "trail_mode": "none",
            "trail_every_r": None,
            "trail_add_r": None,
        },
        {
            "trigger_r": first_lock_trigger_r,
            "action": "lock_profit",
            "lock_r": 1,
            "trail_mode": "step",
            "trail_every_r": 1,
            "trail_add_r": 1,
        },
        {
            "trigger_r": 11,
            "action": "lock_profit",
            "lock_r": 10,
            "trail_mode": "step",
            "trail_every_r": 1,
            "trail_add_r": 1,
        },
    )


def build_config(candidate: Candidate) -> StrategyConfig:
    return StrategyConfig(
        inst_id=SYMBOL,
        bar=BAR,
        ema_type="ema",
        ema_period=21,
        trend_ema_type="ema",
        trend_ema_period=55,
        big_ema_period=0,
        entry_reference_ema_type="ema",
        entry_reference_ema_period=13,
        atr_period=10,
        atr_stop_multiplier=Decimal("1"),
        atr_take_multiplier=Decimal("1"),
        order_size=Decimal("0"),
        trade_mode="cross",
        signal_mode="long_only",
        position_mode="net",
        environment="live",
        tp_sl_trigger_type="mark",
        strategy_id=STRATEGY_DYNAMIC_LONG_ID,
        poll_seconds=10.0,
        risk_amount=RISK_AMOUNT,
        tp_sl_mode="exchange",
        entry_side_mode="follow_signal",
        run_mode="trade",
        take_profit_mode="dynamic",
        max_entries_per_trend=candidate.max_entries_per_trend,
        dynamic_two_r_break_even=True,
        dynamic_break_even_trigger_r=3,
        dynamic_fee_offset_enabled=True,
        dynamic_protection_rules=make_rules(candidate.first_lock_trigger_r),
        ema55_slope_exit_enabled=True,
        ema55_slope_lock_profit_enabled=False,
        ema55_slope_lock_profit_trigger_r=candidate.first_lock_trigger_r,
        dynamic_first_lock_r=1,
        dynamic_trailing_step_r=1,
        ema55_slope_negative_entry_bars=1,
        trend_ema_slope_filter_enabled=True,
        trend_ema_slope_filter_lookback_bars=5,
        trend_ema_slope_filter_min_ratio=Decimal("0"),
        atr_percentile_filter_max=Decimal("0"),
        body_retest_breakdown_atr_multiplier=Decimal("0.2"),
        body_retest_retest_atr_multiplier=Decimal("0.3"),
        body_retest_stop_buffer_atr_multiplier=Decimal("0.3"),
        body_retest_body_atr_limit=Decimal("1.0"),
        body_retest_watch_bars=6,
        time_stop_break_even_enabled=False,
        time_stop_break_even_bars=0,
        trend_ema_close_exit_after_trigger_r_enabled=False,
        trend_ema_close_exit_after_trigger_r=5,
        hold_close_exit_bars=0,
        daily_filter_enabled=False,
        daily_filter_boundary="exchange",
        daily_filter_mode="disabled",
        daily_filter_scope="both",
        daily_filter_ma_type="ema",
        daily_filter_period=5,
        backtest_initial_capital=INITIAL_CAPITAL,
        backtest_sizing_mode="fixed_risk",
        backtest_compounding=False,
        backtest_entry_slippage_rate=Decimal("0"),
        backtest_exit_slippage_rate=Decimal("0"),
        backtest_slippage_rate=Decimal("0"),
        backtest_funding_rate=Decimal("0"),
    )


def build_tail_stats(trades: list[BacktestTrade]) -> TailStats:
    pnls = [trade.pnl for trade in trades]
    total_pnl = sum(pnls, Decimal("0"))
    winners = sorted((pnl for pnl in pnls if pnl > 0), reverse=True)
    monthly: dict[str, Decimal] = {}
    loss_streak = 0
    max_loss_streak = 0

    for trade in trades:
        pnl = trade.pnl
        month = _period_key(_trade_dt(trade), "month")
        monthly[month] = monthly.get(month, Decimal("0")) + pnl
        if pnl < 0:
            loss_streak += 1
            if loss_streak > max_loss_streak:
                max_loss_streak = loss_streak
        else:
            loss_streak = 0

    def share(top_n: int) -> Decimal:
        if total_pnl == 0:
            return Decimal("0")
        return sum(winners[:top_n], Decimal("0")) / total_pnl * Decimal("100")

    return TailStats(
        top5_share_pct=share(5),
        top10_share_pct=share(10),
        top20_share_pct=share(20),
        pnl_without_top20=total_pnl - sum(winners[:20], Decimal("0")),
        positive_months=sum(1 for value in monthly.values() if value > 0),
        negative_months=sum(1 for value in monthly.values() if value < 0),
        total_months=len(monthly),
        max_loss_streak=max_loss_streak,
    )


def build_period_rows(trades: list[BacktestTrade], mode: str) -> tuple[PeriodRow, ...]:
    buckets: dict[str, list[BacktestTrade]] = {}
    for trade in trades:
        key = _period_key(_trade_dt(trade), mode)
        buckets.setdefault(key, []).append(trade)
    rows: list[PeriodRow] = []
    for key in sorted(buckets):
        metrics = build_metrics(buckets[key])
        rows.append(
            PeriodRow(
                period=key,
                pnl=metrics.pnl,
                trades=metrics.trades,
                win_rate=metrics.win_rate,
                profit_factor=metrics.profit_factor,
                avg_r=metrics.avg_r,
            )
        )
    return tuple(rows)


def build_rolling_6m_rows(trades: list[BacktestTrade]) -> tuple[PeriodRow, ...]:
    if not trades:
        return ()
    rows: list[PeriodRow] = []
    start = datetime(OOS_START.year, OOS_START.month, 1, tzinfo=timezone.utc)
    last_dt = _trade_dt(trades[-1])
    while start <= last_dt:
        end = _add_months(start, 6)
        window_trades = [trade for trade in trades if start <= _trade_dt(trade) < end]
        if window_trades:
            metrics = build_metrics(window_trades)
            rows.append(
                PeriodRow(
                    period=f"{start.strftime('%Y-%m')} -> {(end - timedelta(hours=1)).strftime('%Y-%m')}",
                    pnl=metrics.pnl,
                    trades=metrics.trades,
                    win_rate=metrics.win_rate,
                    profit_factor=metrics.profit_factor,
                    avg_r=metrics.avg_r,
                )
            )
        start = _add_months(start, 3)
    return tuple(rows)


def run_candidate(candidate: Candidate) -> CandidateRun:
    client = OkxRestClient()
    candles = [candle for candle in load_candle_cache(SYMBOL, BAR, limit=None) if candle.confirmed]
    if not candles:
        raise RuntimeError(f"missing local candles for {SYMBOL} {BAR}")
    result = _run_backtest_with_loaded_data(
        candles,
        client.get_instrument(SYMBOL),
        build_config(candidate),
        data_source_note=f"local candle_cache full history | {SYMBOL} | {BAR} | {candidate.label}",
        maker_fee_rate=MAKER_FEE_RATE,
        taker_fee_rate=TAKER_FEE_RATE,
    )
    full_trades = list(result.trades)
    oos_trades = filter_oos_trades(full_trades)
    return CandidateRun(
        candidate=candidate,
        full_metrics=build_metrics(full_trades),
        oos_metrics=build_metrics(oos_trades),
        oos_tail=build_tail_stats(oos_trades),
        oos_yearly=build_period_rows(oos_trades, "year"),
        oos_quarterly=build_period_rows(oos_trades, "quarter"),
        rolling_6m=build_rolling_6m_rows(oos_trades),
    )


def diff_decimal(lhs: Decimal | None, rhs: Decimal | None) -> Decimal | None:
    if lhs is None or rhs is None:
        return None
    return lhs - rhs


def positive_periods(rows: tuple[PeriodRow, ...]) -> int:
    return sum(1 for row in rows if row.pnl > 0)


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    if not rows:
        return
    with path.open("w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def build_markdown(baseline: CandidateRun, challenger: CandidateRun) -> str:
    pnl_delta = challenger.oos_metrics.pnl - baseline.oos_metrics.pnl
    pf_delta = diff_decimal(challenger.oos_metrics.profit_factor, baseline.oos_metrics.profit_factor)
    dd_delta = challenger.oos_metrics.max_drawdown - baseline.oos_metrics.max_drawdown
    top20_delta = challenger.oos_tail.top20_share_pct - baseline.oos_tail.top20_share_pct
    positive_quarter_delta = positive_periods(challenger.oos_quarterly) - positive_periods(baseline.oos_quarterly)

    lines = [
        "# SOL 动态做多 5R 锁盈验证",
        "",
        "## 验证口径",
        "",
        f"- 标的：`{SYMBOL}`",
        f"- 周期：`{BAR}`",
        "- 固定风险：`100U` / 笔，初始资金 `10000U`，非复利",
        "- K 线来源：本地 `candle_cache` 全量确认收盘数据",
        "- 对照仅比较两组：`每波2次 / 7R锁1R` 与 `每波2次 / 5R锁1R`",
        "- 样本外定义：`2024-01-01 00:00:00 UTC` 之后按平仓时间统计",
        "",
        "## 结论",
        "",
        f"- 基线样本外：PnL `{fmt(baseline.oos_metrics.pnl)}` / PF `{fmt(baseline.oos_metrics.profit_factor)}` / DD `{fmt(baseline.oos_metrics.max_drawdown)}` / AvgR `{fmt(baseline.oos_metrics.avg_r)}` / Trades `{baseline.oos_metrics.trades}`",
        f"- 候选样本外：PnL `{fmt(challenger.oos_metrics.pnl)}` / PF `{fmt(challenger.oos_metrics.profit_factor)}` / DD `{fmt(challenger.oos_metrics.max_drawdown)}` / AvgR `{fmt(challenger.oos_metrics.avg_r)}` / Trades `{challenger.oos_metrics.trades}`",
        f"- 样本外 PnL 变化：`{fmt(pnl_delta)}`",
        f"- 样本外 PF 变化：`{fmt(pf_delta)}`",
        f"- 样本外 DD 变化：`{fmt(dd_delta)}`",
        f"- 样本外正季度变化：`{positive_quarter_delta}`",
        f"- 样本外 Top20 依赖变化：`{fmt(top20_delta)}` pct",
        "",
        "## 读法",
        "",
        "- 如果更关心近期样本外收益和回撤，`5R锁1R` 确实比基线更优。",
        "- 如果更关心结构纯度，`5R锁1R` 并没有消灭头部盈利依赖，只是把样本外结果做得更顺了一些。",
        "- 所以它更像“可升级候选”，还不是“无争议定稿”。",
        "",
        "## OOS 年度",
        "",
        "| 方案 | 年份 | PnL | PF | AvgR | Trades |",
        "| --- | --- | ---: | ---: | ---: | ---: |",
    ]
    for run in (baseline, challenger):
        yearly_map = {row.period: row for row in run.oos_yearly}
        for period in sorted(yearly_map):
            row = yearly_map[period]
            lines.append(
                f"| {run.candidate.label} | {period} | {fmt(row.pnl)} | {fmt(row.profit_factor)} | {fmt(row.avg_r)} | {row.trades} |"
            )

    lines.extend(
        [
            "",
            "## OOS 季度",
            "",
            "| 方案 | 季度 | PnL | PF | AvgR | Trades |",
            "| --- | --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for run in (baseline, challenger):
        for row in run.oos_quarterly:
            lines.append(
                f"| {run.candidate.candidate_id} | {row.period} | {fmt(row.pnl)} | {fmt(row.profit_factor)} | {fmt(row.avg_r)} | {row.trades} |"
            )

    lines.extend(
        [
            "",
            "## OOS 6个月滚动窗口",
            "",
            "| 方案 | 窗口 | PnL | PF | AvgR | Trades |",
            "| --- | --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for run in (baseline, challenger):
        for row in run.rolling_6m:
            lines.append(
                f"| {run.candidate.candidate_id} | {row.period} | {fmt(row.pnl)} | {fmt(row.profit_factor)} | {fmt(row.avg_r)} | {row.trades} |"
            )

    lines.extend(
        [
            "",
            "## OOS 结构摘要",
            "",
            "| 方案 | Top5占比 | Top10占比 | Top20占比 | 去掉Top20后PnL | 正月数 | 负月数 | 最大连亏 |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for run in (baseline, challenger):
        tail = run.oos_tail
        lines.append(
            f"| {run.candidate.label} | {fmt(tail.top5_share_pct)}% | {fmt(tail.top10_share_pct)}% | "
            f"{fmt(tail.top20_share_pct)}% | {fmt(tail.pnl_without_top20)} | {tail.positive_months} | "
            f"{tail.negative_months} | {tail.max_loss_streak} |"
        )

    lines.extend(
        [
            "",
            f"- 明细文件：`{SUMMARY_CSV_PATH}` / `{YEARLY_CSV_PATH}` / `{QUARTERLY_CSV_PATH}` / `{ROLLING_CSV_PATH}` / `{JSON_PATH}`",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> None:
    baseline = run_candidate(BASELINE)
    challenger = run_candidate(CHALLENGER)

    summary_rows = [
        {
            "candidate_id": run.candidate.candidate_id,
            "label": run.candidate.label,
            "full_pnl": str(run.full_metrics.pnl),
            "full_pf": None if run.full_metrics.profit_factor is None else str(run.full_metrics.profit_factor),
            "full_avg_r": str(run.full_metrics.avg_r),
            "full_max_drawdown": str(run.full_metrics.max_drawdown),
            "full_trades": run.full_metrics.trades,
            "oos_pnl": str(run.oos_metrics.pnl),
            "oos_pf": None if run.oos_metrics.profit_factor is None else str(run.oos_metrics.profit_factor),
            "oos_avg_r": str(run.oos_metrics.avg_r),
            "oos_max_drawdown": str(run.oos_metrics.max_drawdown),
            "oos_trades": run.oos_metrics.trades,
            "oos_top5_share_pct": str(run.oos_tail.top5_share_pct),
            "oos_top10_share_pct": str(run.oos_tail.top10_share_pct),
            "oos_top20_share_pct": str(run.oos_tail.top20_share_pct),
            "oos_pnl_without_top20": str(run.oos_tail.pnl_without_top20),
            "oos_positive_months": run.oos_tail.positive_months,
            "oos_negative_months": run.oos_tail.negative_months,
            "oos_max_loss_streak": run.oos_tail.max_loss_streak,
        }
        for run in (baseline, challenger)
    ]
    write_csv(SUMMARY_CSV_PATH, summary_rows)

    yearly_rows: list[dict[str, object]] = []
    for run in (baseline, challenger):
        for row in run.oos_yearly:
            yearly_rows.append(
                {
                    "candidate_id": run.candidate.candidate_id,
                    "label": run.candidate.label,
                    "period": row.period,
                    "pnl": str(row.pnl),
                    "profit_factor": None if row.profit_factor is None else str(row.profit_factor),
                    "avg_r": str(row.avg_r),
                    "trades": row.trades,
                    "win_rate": str(row.win_rate),
                }
            )
    write_csv(YEARLY_CSV_PATH, yearly_rows)

    quarterly_rows: list[dict[str, object]] = []
    for run in (baseline, challenger):
        for row in run.oos_quarterly:
            quarterly_rows.append(
                {
                    "candidate_id": run.candidate.candidate_id,
                    "label": run.candidate.label,
                    "period": row.period,
                    "pnl": str(row.pnl),
                    "profit_factor": None if row.profit_factor is None else str(row.profit_factor),
                    "avg_r": str(row.avg_r),
                    "trades": row.trades,
                    "win_rate": str(row.win_rate),
                }
            )
    write_csv(QUARTERLY_CSV_PATH, quarterly_rows)

    rolling_rows: list[dict[str, object]] = []
    for run in (baseline, challenger):
        for row in run.rolling_6m:
            rolling_rows.append(
                {
                    "candidate_id": run.candidate.candidate_id,
                    "label": run.candidate.label,
                    "period": row.period,
                    "pnl": str(row.pnl),
                    "profit_factor": None if row.profit_factor is None else str(row.profit_factor),
                    "avg_r": str(row.avg_r),
                    "trades": row.trades,
                    "win_rate": str(row.win_rate),
                }
            )
    write_csv(ROLLING_CSV_PATH, rolling_rows)

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "symbol": SYMBOL,
        "bar": BAR,
        "oos_start_utc": OOS_START.strftime("%Y-%m-%d %H:%M:%S UTC"),
        "baseline": asdict(baseline),
        "challenger": asdict(challenger),
    }
    JSON_PATH.write_text(json.dumps(json_ready(payload), ensure_ascii=False, indent=2), encoding="utf-8")

    markdown = build_markdown(baseline, challenger)
    MD_PATH.write_text(markdown, encoding="utf-8")
    LATEST_MD_PATH.write_text(markdown, encoding="utf-8")
    LATEST_CSV_PATH.write_text(SUMMARY_CSV_PATH.read_text(encoding="utf-8-sig"), encoding="utf-8-sig")
    LATEST_JSON_PATH.write_text(JSON_PATH.read_text(encoding="utf-8"), encoding="utf-8")

    print(MD_PATH)
    print(SUMMARY_CSV_PATH)
    print(JSON_PATH)


if __name__ == "__main__":
    main()
