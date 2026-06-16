from __future__ import annotations

import csv
import json
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
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
from okx_quant.strategy_catalog import STRATEGY_DYNAMIC_LONG_ID


REPORT_DIR = analysis_report_dir_path()
REPORT_DIR.mkdir(parents=True, exist_ok=True)
PROJECT_REPORT_DIR = ROOT / "reports"
PROJECT_REPORT_DIR.mkdir(parents=True, exist_ok=True)

BAR = "1H"
SYMBOL = "SOL-USDT-SWAP"
LABEL = "SOL"
INITIAL_CAPITAL = Decimal("10000")
RISK_AMOUNT = Decimal("100")
MAKER_FEE_RATE = Decimal("0")
TAKER_FEE_RATE = Decimal("0.00036")
OOS_START_TS = int(datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)

STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
BASE_NAME = f"sol_dynamic_long_s656_followup_{STAMP}"
CSV_PATH = REPORT_DIR / f"{BASE_NAME}.csv"
JSON_PATH = REPORT_DIR / f"{BASE_NAME}.json"
MD_PATH = REPORT_DIR / f"{BASE_NAME}.md"
LATEST_MD_PATH = PROJECT_REPORT_DIR / "sol_dynamic_long_s656_followup_latest.md"
LATEST_CSV_PATH = PROJECT_REPORT_DIR / "sol_dynamic_long_s656_followup_latest.csv"
LATEST_JSON_PATH = PROJECT_REPORT_DIR / "sol_dynamic_long_s656_followup_latest.json"


@dataclass(frozen=True)
class Candidate:
    candidate_id: str
    max_entries_per_trend: int
    first_lock_trigger_r: int

    @property
    def label(self) -> str:
        return (
            f"SL1 / 3R保本 / {self.first_lock_trigger_r}R锁1R / "
            f"11R锁10R / 每波{self.max_entries_per_trend}次"
        )


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
    max_loss_streak: int


@dataclass(frozen=True)
class YearMetrics:
    year: int
    pnl: Decimal
    trades: int
    win_rate: Decimal


@dataclass(frozen=True)
class CandidateResult:
    candidate: Candidate
    full_metrics: Metrics
    oos_metrics: Metrics
    tail_stats: TailStats
    yearly_oos: tuple[YearMetrics, ...]


def fmt(value: Decimal | None, digits: int = 4) -> str:
    if value is None:
        return "-"
    return format_decimal_fixed(value, digits)


def format_ts(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


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


def build_tail_stats(trades: list[BacktestTrade]) -> TailStats:
    pnls = [trade.pnl for trade in trades]
    total_pnl = sum(pnls, Decimal("0"))
    wins = sorted((pnl for pnl in pnls if pnl > 0), reverse=True)

    def _share(count: int) -> Decimal:
        if total_pnl == 0:
            return Decimal("0")
        return sum(wins[:count], Decimal("0")) / total_pnl * Decimal("100")

    loss_streak = 0
    max_loss_streak = 0
    for pnl in pnls:
        if pnl < 0:
            loss_streak += 1
            if loss_streak > max_loss_streak:
                max_loss_streak = loss_streak
        else:
            loss_streak = 0

    return TailStats(
        top5_share_pct=_share(5),
        top10_share_pct=_share(10),
        top20_share_pct=_share(20),
        pnl_without_top20=total_pnl - sum(wins[:20], Decimal("0")),
        max_loss_streak=max_loss_streak,
    )


def build_yearly_oos(trades: list[BacktestTrade]) -> tuple[YearMetrics, ...]:
    buckets: dict[int, list[BacktestTrade]] = {}
    for trade in trades:
        year = datetime.fromtimestamp(trade.exit_ts / 1000, tz=timezone.utc).year
        buckets.setdefault(year, []).append(trade)
    rows: list[YearMetrics] = []
    for year in sorted(buckets):
        metrics = build_metrics(buckets[year])
        rows.append(
            YearMetrics(
                year=year,
                pnl=metrics.pnl,
                trades=metrics.trades,
                win_rate=metrics.win_rate,
            )
        )
    return tuple(rows)


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


def score_result(result: CandidateResult) -> tuple[Decimal, Decimal, Decimal, Decimal]:
    oos_pf = result.oos_metrics.profit_factor or Decimal("0")
    return (
        result.oos_metrics.pnl,
        oos_pf,
        -result.oos_metrics.max_drawdown,
        result.full_metrics.pnl,
    )


def run_candidate(candidate: Candidate) -> dict[str, object]:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    from okx_quant.backtest import _run_backtest_with_loaded_data
    from okx_quant.candle_cache import load_candle_cache
    from okx_quant.okx_client import OkxRestClient

    candles = [candle for candle in load_candle_cache(SYMBOL, BAR, limit=None) if candle.confirmed]
    if not candles:
        raise RuntimeError(f"missing local candles for {SYMBOL} {BAR}")
    client = OkxRestClient()
    result = _run_backtest_with_loaded_data(
        candles,
        client.get_instrument(SYMBOL),
        build_config(candidate),
        data_source_note=f"local candle_cache full history | {SYMBOL} | {BAR} | {candidate.label}",
        maker_fee_rate=MAKER_FEE_RATE,
        taker_fee_rate=TAKER_FEE_RATE,
    )
    trades = list(result.trades)
    oos_trades = filter_oos_trades(trades)
    row = CandidateResult(
        candidate=candidate,
        full_metrics=build_metrics(trades),
        oos_metrics=build_metrics(oos_trades),
        tail_stats=build_tail_stats(trades),
        yearly_oos=build_yearly_oos(oos_trades),
    )
    return json_ready(asdict(row))


def candidate_grid() -> tuple[Candidate, ...]:
    rows: list[Candidate] = []
    for max_entries in (1, 2, 3):
        for first_lock_trigger_r in (5, 6, 7, 8):
            rows.append(
                Candidate(
                    candidate_id=f"entries_{max_entries}_lock_{first_lock_trigger_r}",
                    max_entries_per_trend=max_entries,
                    first_lock_trigger_r=first_lock_trigger_r,
                )
            )
    return tuple(rows)


def parse_result(payload: dict[str, object]) -> CandidateResult:
    candidate_data = payload["candidate"]
    full_data = payload["full_metrics"]
    oos_data = payload["oos_metrics"]
    tail_data = payload["tail_stats"]
    yearly_data = payload["yearly_oos"]

    return CandidateResult(
        candidate=Candidate(
            candidate_id=str(candidate_data["candidate_id"]),
            max_entries_per_trend=int(candidate_data["max_entries_per_trend"]),
            first_lock_trigger_r=int(candidate_data["first_lock_trigger_r"]),
        ),
        full_metrics=Metrics(
            pnl=Decimal(str(full_data["pnl"])),
            trades=int(full_data["trades"]),
            win_rate=Decimal(str(full_data["win_rate"])),
            profit_factor=None if full_data["profit_factor"] is None else Decimal(str(full_data["profit_factor"])),
            avg_r=Decimal(str(full_data["avg_r"])),
            max_drawdown=Decimal(str(full_data["max_drawdown"])),
            return_pct=Decimal(str(full_data["return_pct"])),
        ),
        oos_metrics=Metrics(
            pnl=Decimal(str(oos_data["pnl"])),
            trades=int(oos_data["trades"]),
            win_rate=Decimal(str(oos_data["win_rate"])),
            profit_factor=None if oos_data["profit_factor"] is None else Decimal(str(oos_data["profit_factor"])),
            avg_r=Decimal(str(oos_data["avg_r"])),
            max_drawdown=Decimal(str(oos_data["max_drawdown"])),
            return_pct=Decimal(str(oos_data["return_pct"])),
        ),
        tail_stats=TailStats(
            top5_share_pct=Decimal(str(tail_data["top5_share_pct"])),
            top10_share_pct=Decimal(str(tail_data["top10_share_pct"])),
            top20_share_pct=Decimal(str(tail_data["top20_share_pct"])),
            pnl_without_top20=Decimal(str(tail_data["pnl_without_top20"])),
            max_loss_streak=int(tail_data["max_loss_streak"]),
        ),
        yearly_oos=tuple(
            YearMetrics(
                year=int(item["year"]),
                pnl=Decimal(str(item["pnl"])),
                trades=int(item["trades"]),
                win_rate=Decimal(str(item["win_rate"])),
            )
            for item in yearly_data
        ),
    )


def build_rows(results: tuple[CandidateResult, ...]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for item in results:
        rows.append(
            {
                "candidate_id": item.candidate.candidate_id,
                "label": item.candidate.label,
                "max_entries_per_trend": item.candidate.max_entries_per_trend,
                "first_lock_trigger_r": item.candidate.first_lock_trigger_r,
                "full_pnl": str(item.full_metrics.pnl),
                "full_max_drawdown": str(item.full_metrics.max_drawdown),
                "full_pf": None if item.full_metrics.profit_factor is None else str(item.full_metrics.profit_factor),
                "full_avg_r": str(item.full_metrics.avg_r),
                "full_trades": item.full_metrics.trades,
                "oos_pnl": str(item.oos_metrics.pnl),
                "oos_max_drawdown": str(item.oos_metrics.max_drawdown),
                "oos_pf": None if item.oos_metrics.profit_factor is None else str(item.oos_metrics.profit_factor),
                "oos_avg_r": str(item.oos_metrics.avg_r),
                "oos_trades": item.oos_metrics.trades,
                "top10_share_pct": str(item.tail_stats.top10_share_pct),
                "top20_share_pct": str(item.tail_stats.top20_share_pct),
                "pnl_without_top20": str(item.tail_stats.pnl_without_top20),
                "max_loss_streak": item.tail_stats.max_loss_streak,
            }
        )
    return rows


def compare_text(best: CandidateResult, baseline: CandidateResult) -> list[str]:
    pnl_delta = best.oos_metrics.pnl - baseline.oos_metrics.pnl
    pf_delta = (best.oos_metrics.profit_factor or Decimal("0")) - (baseline.oos_metrics.profit_factor or Decimal("0"))
    dd_delta = best.oos_metrics.max_drawdown - baseline.oos_metrics.max_drawdown
    top20_delta = best.tail_stats.top20_share_pct - baseline.tail_stats.top20_share_pct
    lines = [
        f"- 相对基线样本外 PnL 变化：`{fmt(pnl_delta)}`",
        f"- 相对基线样本外 PF 变化：`{fmt(pf_delta)}`",
        f"- 相对基线样本外 DD 变化：`{fmt(dd_delta)}`",
        f"- 相对基线头部依赖变化：Top20 贡献占比 `{fmt(top20_delta)}` pct",
    ]
    return lines


def build_markdown(
    *,
    sample_start_ts: int,
    sample_end_ts: int,
    baseline: CandidateResult,
    best: CandidateResult,
    results: tuple[CandidateResult, ...],
) -> str:
    lines = [
        "# SOL 动态做多 S656 跟进实验",
        "",
        "## 回测口径",
        "",
        f"- 标的：`{SYMBOL}`",
        f"- 周期：`{BAR}`",
        f"- 样本范围：`{format_ts(sample_start_ts)}` -> `{format_ts(sample_end_ts)}`",
        "- K线来源：本地 `candle_cache` 已确认收盘 K 线",
        f"- 固定风险金：`{fmt(RISK_AMOUNT, 0)}`U / 笔",
        f"- 初始资金：`{fmt(INITIAL_CAPITAL, 0)}`U",
        f"- 手续费：Maker `{fmt(MAKER_FEE_RATE * Decimal('100'), 4)}%` / Taker `{fmt(TAKER_FEE_RATE * Decimal('100'), 4)}%`",
        "- 复利：关闭",
        f"- 样本外起点：`{format_ts(OOS_START_TS)}`",
        "- 固定底座：`EMA21 / EMA55 / 挂单 EMA13 / ATR10 / SL1 / 3R保本 / 11R锁10R`",
        "- 本轮只扫两层：`每波 1/2/3 次` 与 `首档锁盈 5/6/7/8R`",
        "",
        "## 结论",
        "",
        f"- 当前基线：`{baseline.candidate.label}`",
        f"- 本轮样本外最佳：`{best.candidate.label}`",
        f"- 基线样本外：PnL `{fmt(baseline.oos_metrics.pnl)}` / PF `{fmt(baseline.oos_metrics.profit_factor)}` / DD `{fmt(baseline.oos_metrics.max_drawdown)}` / AvgR `{fmt(baseline.oos_metrics.avg_r)}` / Trades `{baseline.oos_metrics.trades}`",
        f"- 最佳样本外：PnL `{fmt(best.oos_metrics.pnl)}` / PF `{fmt(best.oos_metrics.profit_factor)}` / DD `{fmt(best.oos_metrics.max_drawdown)}` / AvgR `{fmt(best.oos_metrics.avg_r)}` / Trades `{best.oos_metrics.trades}`",
    ]
    lines.extend(compare_text(best, baseline))
    lines.extend(
        [
            "",
            "## 候选矩阵",
            "",
            "| 方案 | 全样本PnL | 全样本DD | 全样本PF | 全样本AvgR | 交易数 | 样本外PnL | 样本外DD | 样本外PF | 样本外AvgR | 样本外交易数 | Top10占比 | Top20占比 | 去掉Top20后PnL |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for item in sorted(results, key=score_result, reverse=True):
        lines.append(
            f"| {item.candidate.label} | {fmt(item.full_metrics.pnl)} | {fmt(item.full_metrics.max_drawdown)} | "
            f"{fmt(item.full_metrics.profit_factor)} | {fmt(item.full_metrics.avg_r)} | {item.full_metrics.trades} | "
            f"{fmt(item.oos_metrics.pnl)} | {fmt(item.oos_metrics.max_drawdown)} | {fmt(item.oos_metrics.profit_factor)} | "
            f"{fmt(item.oos_metrics.avg_r)} | {item.oos_metrics.trades} | {fmt(item.tail_stats.top10_share_pct)}% | "
            f"{fmt(item.tail_stats.top20_share_pct)}% | {fmt(item.tail_stats.pnl_without_top20)} |"
        )

    lines.extend(
        [
            "",
            "## 样本外年度",
            "",
            "| 方案 | 2024 PnL/笔数 | 2025 PnL/笔数 | 2026 PnL/笔数 |",
            "| --- | ---: | ---: | ---: |",
        ]
    )
    for item in sorted(results, key=score_result, reverse=True):
        bucket = {year_item.year: year_item for year_item in item.yearly_oos}
        year_cells: list[str] = []
        for year in (2024, 2025, 2026):
            year_item = bucket.get(year)
            if year_item is None:
                year_cells.append("-")
            else:
                year_cells.append(f"{fmt(year_item.pnl)} / {year_item.trades}")
        lines.append(f"| {item.candidate.label} | {year_cells[0]} | {year_cells[1]} | {year_cells[2]} |")

    lines.extend(
        [
            "",
            "## 观察",
            "",
            "- 这组方案整体仍有明显头部盈利依赖；若 Top20 大单被拿掉，多数组合都会转负。",
            "- `每波3次` 在 SOL 上没有带来更好的样本外质量，更多是在增加交易笔数与费用暴露。",
            "- 若较早把首档锁盈提前到 `5R/6R`，通常能减轻一点尾部依赖，但容易牺牲长尾利润。",
            "- 若仍坚持当前骨架，优先关注样本外 PnL / PF 是否真的改善，再看胜率和体感。",
            "",
            f"- 明细文件：`{CSV_PATH}` / `{JSON_PATH}`",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> None:
    grid = candidate_grid()
    rows: list[CandidateResult] = []

    candles = [candle for candle in load_candle_cache(SYMBOL, BAR, limit=None) if candle.confirmed]
    if not candles:
        raise RuntimeError(f"missing local candles for {SYMBOL} {BAR}")
    sample_start_ts = candles[0].ts
    sample_end_ts = candles[-1].ts

    with ProcessPoolExecutor(max_workers=min(6, len(grid))) as executor:
        future_map = {executor.submit(run_candidate, candidate): candidate for candidate in grid}
        for future in as_completed(future_map):
            candidate = future_map[future]
            print(f"[done] {candidate.label}")
            rows.append(parse_result(future.result()))

    results = tuple(sorted(rows, key=score_result, reverse=True))
    baseline = next(item for item in results if item.candidate.candidate_id == "entries_2_lock_7")
    best = results[0]

    csv_rows = build_rows(results)
    with CSV_PATH.open("w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(csv_rows[0].keys()))
        writer.writeheader()
        writer.writerows(csv_rows)

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "symbol": SYMBOL,
        "bar": BAR,
        "risk_amount": str(RISK_AMOUNT),
        "initial_capital": str(INITIAL_CAPITAL),
        "maker_fee_rate": str(MAKER_FEE_RATE),
        "taker_fee_rate": str(TAKER_FEE_RATE),
        "sample_start_utc": format_ts(sample_start_ts),
        "sample_end_utc": format_ts(sample_end_ts),
        "oos_start_utc": format_ts(OOS_START_TS),
        "baseline_candidate_id": baseline.candidate.candidate_id,
        "best_candidate_id": best.candidate.candidate_id,
        "results": [asdict(item) for item in results],
    }
    JSON_PATH.write_text(json.dumps(json_ready(payload), ensure_ascii=False, indent=2), encoding="utf-8")

    markdown = build_markdown(
        sample_start_ts=sample_start_ts,
        sample_end_ts=sample_end_ts,
        baseline=baseline,
        best=best,
        results=results,
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
