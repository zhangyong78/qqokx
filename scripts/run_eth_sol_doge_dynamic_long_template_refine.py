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
from scripts.build_best_parameter_bundle import build_dynamic_long_config


REPORT_DIR = analysis_report_dir_path()
REPORT_DIR.mkdir(parents=True, exist_ok=True)
PROJECT_REPORT_DIR = ROOT / "reports"
PROJECT_REPORT_DIR.mkdir(parents=True, exist_ok=True)

BAR = "1H"
INITIAL_CAPITAL = Decimal("10000")
RISK_AMOUNT = Decimal("100")
MAKER_FEE_RATE = Decimal("0")
TAKER_FEE_RATE = Decimal("0.00036")
OOS_START_TS = int(datetime(2022, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)

STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
BASE_NAME = f"eth_sol_doge_dynamic_long_template_refine_{STAMP}"
CSV_PATH = REPORT_DIR / f"{BASE_NAME}.csv"
JSON_PATH = REPORT_DIR / f"{BASE_NAME}.json"
MD_PATH = REPORT_DIR / f"{BASE_NAME}.md"
LATEST_MD_PATH = PROJECT_REPORT_DIR / "eth_sol_doge_dynamic_long_template_refine_latest.md"
LATEST_CSV_PATH = PROJECT_REPORT_DIR / "eth_sol_doge_dynamic_long_template_refine_latest.csv"
LATEST_JSON_PATH = PROJECT_REPORT_DIR / "eth_sol_doge_dynamic_long_template_refine_latest.json"


@dataclass(frozen=True)
class CoinProfile:
    symbol: str
    label: str
    ema_period: int
    trend_ema_period: int
    entry_reference_ema_period: int
    current_stop_multiplier: Decimal
    current_trigger_r: int
    current_max_entries: int

    @property
    def core_label(self) -> str:
        entry_label = (
            f"EMA{self.entry_reference_ema_period}"
            if self.entry_reference_ema_period > 0
            else f"跟随EMA{self.ema_period}"
        )
        return f"EMA{self.ema_period} / EMA{self.trend_ema_period} / 入场 {entry_label}"


@dataclass(frozen=True)
class PhaseConfig:
    phase: str
    candidate_id: str
    label: str
    atr_stop_multiplier: Decimal
    break_even_trigger_r: int
    lock_trigger_r: int
    max_entries_per_trend: int


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
    coin: CoinProfile
    config: PhaseConfig
    full_metrics: Metrics
    oos_metrics: Metrics


COIN_PROFILES: tuple[CoinProfile, ...] = (
    CoinProfile(
        symbol="ETH-USDT-SWAP",
        label="ETH",
        ema_period=21,
        trend_ema_period=55,
        entry_reference_ema_period=34,
        current_stop_multiplier=Decimal("1.5"),
        current_trigger_r=3,
        current_max_entries=3,
    ),
    CoinProfile(
        symbol="SOL-USDT-SWAP",
        label="SOL",
        ema_period=21,
        trend_ema_period=55,
        entry_reference_ema_period=13,
        current_stop_multiplier=Decimal("1"),
        current_trigger_r=3,
        current_max_entries=1,
    ),
    CoinProfile(
        symbol="DOGE-USDT-SWAP",
        label="DOGE",
        ema_period=5,
        trend_ema_period=13,
        entry_reference_ema_period=0,
        current_stop_multiplier=Decimal("1.5"),
        current_trigger_r=6,
        current_max_entries=1,
    ),
)


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


def format_ts(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def make_btc_style_rules(break_even_trigger_r: int, lock_trigger_r: int) -> tuple[dict[str, object], ...]:
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


def has_strict_protection_order(*, break_even_trigger_r: int, lock_trigger_r: int) -> bool:
    return int(lock_trigger_r) > int(break_even_trigger_r)


def build_template_config(profile: CoinProfile, phase_config: PhaseConfig) -> StrategyConfig:
    return build_dynamic_long_config(
        symbol=profile.symbol,
        ema_period=profile.ema_period,
        trend_ema_period=profile.trend_ema_period,
        entry_reference_ema_period=profile.entry_reference_ema_period,
        atr_stop_multiplier=phase_config.atr_stop_multiplier,
        atr_take_multiplier=phase_config.atr_stop_multiplier,
        trigger_r=phase_config.lock_trigger_r,
        max_entries_per_trend=phase_config.max_entries_per_trend,
        dynamic_break_even_trigger_r=phase_config.break_even_trigger_r,
        dynamic_first_lock_r=1,
        dynamic_trailing_step_r=1,
        dynamic_protection_rules=make_btc_style_rules(
            phase_config.break_even_trigger_r,
            phase_config.lock_trigger_r,
        ),
        trend_ema_close_exit_after_trigger_r_enabled=False,
        trend_ema_close_exit_after_trigger_r=5,
    )


def build_current_best_config(profile: CoinProfile) -> StrategyConfig:
    return build_dynamic_long_config(
        symbol=profile.symbol,
        ema_period=profile.ema_period,
        trend_ema_period=profile.trend_ema_period,
        entry_reference_ema_period=profile.entry_reference_ema_period,
        atr_stop_multiplier=profile.current_stop_multiplier,
        trigger_r=profile.current_trigger_r,
        max_entries_per_trend=profile.current_max_entries,
    )


def phase1_candidates() -> tuple[PhaseConfig, ...]:
    candidates: list[PhaseConfig] = []
    for atr_stop_multiplier in (Decimal("1"), Decimal("1.5"), Decimal("2")):
        for max_entries_per_trend in (1, 2, 3):
            label = (
                f"SL{atr_stop_multiplier} / 1R保本 / 4R锁1R / 11R锁10R / 每波 {max_entries_per_trend} 次"
            )
            candidates.append(
                PhaseConfig(
                    phase="atr_entries",
                    candidate_id=f"sl_{str(atr_stop_multiplier).replace('.', '_')}_entries_{max_entries_per_trend}",
                    label=label,
                    atr_stop_multiplier=atr_stop_multiplier,
                    break_even_trigger_r=1,
                    lock_trigger_r=4,
                    max_entries_per_trend=max_entries_per_trend,
                )
            )
    return tuple(candidates)


def phase2_candidates(base: PhaseConfig) -> tuple[PhaseConfig, ...]:
    return tuple(
        PhaseConfig(
            phase="break_even",
            candidate_id=f"be_{break_even_trigger_r}",
            label=(
                f"SL{base.atr_stop_multiplier} / {break_even_trigger_r}R保本 / "
                f"4R锁1R / 11R锁10R / 每波 {base.max_entries_per_trend} 次"
            ),
            atr_stop_multiplier=base.atr_stop_multiplier,
            break_even_trigger_r=break_even_trigger_r,
            lock_trigger_r=4,
            max_entries_per_trend=base.max_entries_per_trend,
        )
        for break_even_trigger_r in (1, 2, 3, 4, 5)
        if has_strict_protection_order(break_even_trigger_r=break_even_trigger_r, lock_trigger_r=4)
    )


def phase3_candidates(base: PhaseConfig) -> tuple[PhaseConfig, ...]:
    return tuple(
        PhaseConfig(
            phase="lock_r",
            candidate_id=f"lock_{lock_trigger_r}",
            label=(
                f"SL{base.atr_stop_multiplier} / {base.break_even_trigger_r}R保本 / "
                f"{lock_trigger_r}R锁1R / 11R锁10R / 每波 {base.max_entries_per_trend} 次"
            ),
            atr_stop_multiplier=base.atr_stop_multiplier,
            break_even_trigger_r=base.break_even_trigger_r,
            lock_trigger_r=lock_trigger_r,
            max_entries_per_trend=base.max_entries_per_trend,
        )
        for lock_trigger_r in (3, 4, 5, 6, 7, 8)
        if has_strict_protection_order(
            break_even_trigger_r=base.break_even_trigger_r,
            lock_trigger_r=lock_trigger_r,
        )
    )


def score_result(result: PhaseResult) -> tuple[Decimal, Decimal, Decimal, Decimal]:
    oos_pf = result.oos_metrics.profit_factor or Decimal("0")
    dd_penalty = -result.oos_metrics.max_drawdown
    return (result.oos_metrics.pnl, oos_pf, dd_penalty, result.full_metrics.pnl)


def select_best(results: tuple[PhaseResult, ...]) -> PhaseResult:
    return max(results, key=score_result)


def run_candidates(
    *,
    profile: CoinProfile,
    candles,
    instrument,
    candidates: tuple[PhaseConfig, ...],
) -> tuple[PhaseResult, ...]:
    phase_results: list[PhaseResult] = []
    total = len(candidates)
    for index, candidate in enumerate(candidates, start=1):
        print(f"[{profile.label}][{candidate.phase} {index}/{total}] {candidate.label}")
        result = _run_backtest_with_loaded_data(
            candles,
            instrument,
            build_template_config(profile, candidate),
            data_source_note=f"local candle_cache full history | {profile.symbol} | {BAR}",
            maker_fee_rate=MAKER_FEE_RATE,
            taker_fee_rate=TAKER_FEE_RATE,
        )
        full_trades = list(result.trades)
        oos_trades = filter_oos_trades(full_trades)
        phase_results.append(
            PhaseResult(
                coin=profile,
                config=candidate,
                full_metrics=build_metrics(full_trades),
                oos_metrics=build_metrics(oos_trades),
            )
        )
    return tuple(phase_results)


def run_current_benchmark(profile: CoinProfile, candles, instrument) -> PhaseResult:
    result = _run_backtest_with_loaded_data(
        candles,
        instrument,
        build_current_best_config(profile),
        data_source_note=f"current best package baseline | {profile.symbol} | {BAR}",
        maker_fee_rate=MAKER_FEE_RATE,
        taker_fee_rate=TAKER_FEE_RATE,
    )
    full_trades = list(result.trades)
    oos_trades = filter_oos_trades(full_trades)
    return PhaseResult(
        coin=profile,
        config=PhaseConfig(
            phase="current",
            candidate_id="current_best_package",
            label=(
                f"当前最佳参数包 / SL{profile.current_stop_multiplier} / "
                f"{profile.current_trigger_r}R 触发动态保护 / 每波 {profile.current_max_entries} 次"
            ),
            atr_stop_multiplier=profile.current_stop_multiplier,
            break_even_trigger_r=2,
            lock_trigger_r=profile.current_trigger_r,
            max_entries_per_trend=profile.current_max_entries,
        ),
        full_metrics=build_metrics(full_trades),
        oos_metrics=build_metrics(oos_trades),
    )


def rows_for_results(results: tuple[PhaseResult, ...]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for item in results:
        rows.append(
            {
                "coin": item.coin.label,
                "symbol": item.coin.symbol,
                "phase": item.config.phase,
                "candidate_id": item.config.candidate_id,
                "label": item.config.label,
                "atr_stop_multiplier": str(item.config.atr_stop_multiplier),
                "break_even_trigger_r": item.config.break_even_trigger_r,
                "lock_trigger_r": item.config.lock_trigger_r,
                "max_entries_per_trend": item.config.max_entries_per_trend,
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
            }
        )
    return rows


def build_markdown(
    *,
    sample_ranges: dict[str, tuple[int, int]],
    current_results: dict[str, PhaseResult],
    phase1_results: dict[str, tuple[PhaseResult, ...]],
    phase2_results: dict[str, tuple[PhaseResult, ...]],
    phase3_results: dict[str, tuple[PhaseResult, ...]],
    final_results: dict[str, PhaseResult],
) -> str:
    lines = [
        "# ETH / SOL / DOGE 动态委托做多打磨",
        "",
        "## 锁死口径",
        "",
        "- 标的：`ETH-USDT-SWAP / SOL-USDT-SWAP / DOGE-USDT-SWAP`",
        f"- 周期：`{BAR}`",
        "- K线来源：本地 `candle_cache` 全历史已收盘K线",
        f"- 固定风险金：`{fmt(RISK_AMOUNT, 0)}`U / 笔",
        f"- 初始资金：`{fmt(INITIAL_CAPITAL, 0)}`U",
        f"- 手续费：Maker `{fmt(MAKER_FEE_RATE * Decimal('100'), 4)}%` / Taker `{fmt(TAKER_FEE_RATE * Decimal('100'), 4)}%`",
        "- 样本外：`2022-01-01 00:00:00 UTC` 之后按平仓时间统计",
        "- 本轮只做研究，不同步默认参数、UI 或实盘口径，待审核后再落地",
        "- 动态保护锚点参考 BTC 当前模板：`1R保本 / 4R锁1R / 11R锁10R / 每波1次`",
        "",
        "## 简表",
        "",
        "| 币种 | 当前最佳参数包 | 研究候选定稿 | 当前样本外PnL | 候选样本外PnL | 当前样本外PF | 候选样本外PF | 当前样本外交易 | 候选样本外交易 |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for profile in COIN_PROFILES:
        current = current_results[profile.symbol]
        final = final_results[profile.symbol]
        lines.append(
            f"| {profile.label} | {current.config.label} | {final.config.label} | "
            f"{fmt(current.oos_metrics.pnl)} | {fmt(final.oos_metrics.pnl)} | "
            f"{fmt(current.oos_metrics.profit_factor)} | {fmt(final.oos_metrics.profit_factor)} | "
            f"{current.oos_metrics.trades} | {final.oos_metrics.trades} |"
        )

    for profile in COIN_PROFILES:
        sample_start_ts, sample_end_ts = sample_ranges[profile.symbol]
        current = current_results[profile.symbol]
        final = final_results[profile.symbol]
        lines.extend(
            [
                "",
                f"## {profile.label}",
                "",
                f"- 当前长线骨架：`{profile.core_label}`",
                f"- 样本范围：`{format_ts(sample_start_ts)}` -> `{format_ts(sample_end_ts)}`",
                f"- 当前最佳参数包：`{current.config.label}`",
                f"- 当前样本外：PnL `{fmt(current.oos_metrics.pnl)}` / PF `{fmt(current.oos_metrics.profit_factor)}` / AvgR `{fmt(current.oos_metrics.avg_r)}` / Trades `{current.oos_metrics.trades}`",
                f"- 研究候选定稿：`{final.config.label}`",
                f"- 候选全样本：PnL `{fmt(final.full_metrics.pnl)}` / DD `{fmt(final.full_metrics.max_drawdown)}` / PF `{fmt(final.full_metrics.profit_factor)}` / AvgR `{fmt(final.full_metrics.avg_r)}` / Trades `{final.full_metrics.trades}`",
                f"- 候选样本外：PnL `{fmt(final.oos_metrics.pnl)}` / DD `{fmt(final.oos_metrics.max_drawdown)}` / PF `{fmt(final.oos_metrics.profit_factor)}` / AvgR `{fmt(final.oos_metrics.avg_r)}` / Trades `{final.oos_metrics.trades}`",
                "",
                "### 第一层：SL倍数 + 每波次数",
                "",
                "| 方案 | 全样本PnL | 全样本DD | 全样本PF | 全样本AvgR | 交易笔数 | 样本外PnL | 样本外DD | 样本外PF | 样本外AvgR | 样本外交易笔数 |",
                "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for item in sorted(phase1_results[profile.symbol], key=score_result, reverse=True):
            lines.append(
                f"| {item.config.label} | {fmt(item.full_metrics.pnl)} | {fmt(item.full_metrics.max_drawdown)} | "
                f"{fmt(item.full_metrics.profit_factor)} | {fmt(item.full_metrics.avg_r)} | {item.full_metrics.trades} | "
                f"{fmt(item.oos_metrics.pnl)} | {fmt(item.oos_metrics.max_drawdown)} | {fmt(item.oos_metrics.profit_factor)} | "
                f"{fmt(item.oos_metrics.avg_r)} | {item.oos_metrics.trades} |"
            )
        lines.extend(
            [
                "",
                "### 第二层：保本R",
                "",
                "| 方案 | 全样本PnL | 全样本DD | 全样本PF | 全样本AvgR | 交易笔数 | 样本外PnL | 样本外DD | 样本外PF | 样本外AvgR | 样本外交易笔数 |",
                "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for item in sorted(phase2_results[profile.symbol], key=score_result, reverse=True):
            lines.append(
                f"| {item.config.label} | {fmt(item.full_metrics.pnl)} | {fmt(item.full_metrics.max_drawdown)} | "
                f"{fmt(item.full_metrics.profit_factor)} | {fmt(item.full_metrics.avg_r)} | {item.full_metrics.trades} | "
                f"{fmt(item.oos_metrics.pnl)} | {fmt(item.oos_metrics.max_drawdown)} | {fmt(item.oos_metrics.profit_factor)} | "
                f"{fmt(item.oos_metrics.avg_r)} | {item.oos_metrics.trades} |"
            )
        lines.extend(
            [
                "",
                "### 第三层：首档锁盈R",
                "",
                "| 方案 | 全样本PnL | 全样本DD | 全样本PF | 全样本AvgR | 交易笔数 | 样本外PnL | 样本外DD | 样本外PF | 样本外AvgR | 样本外交易笔数 |",
                "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for item in sorted(phase3_results[profile.symbol], key=score_result, reverse=True):
            lines.append(
                f"| {item.config.label} | {fmt(item.full_metrics.pnl)} | {fmt(item.full_metrics.max_drawdown)} | "
                f"{fmt(item.full_metrics.profit_factor)} | {fmt(item.full_metrics.avg_r)} | {item.full_metrics.trades} | "
                f"{fmt(item.oos_metrics.pnl)} | {fmt(item.oos_metrics.max_drawdown)} | {fmt(item.oos_metrics.profit_factor)} | "
                f"{fmt(item.oos_metrics.avg_r)} | {item.oos_metrics.trades} |"
            )

    lines.extend(["", f"- 明细文件：`{CSV_PATH}` / `{JSON_PATH}`", ""])
    return "\n".join(lines)


def write_csv(rows: list[dict[str, object]]) -> None:
    fieldnames = [
        "coin",
        "symbol",
        "phase",
        "candidate_id",
        "label",
        "atr_stop_multiplier",
        "break_even_trigger_r",
        "lock_trigger_r",
        "max_entries_per_trend",
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
    with CSV_PATH.open("w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main() -> None:
    client = OkxRestClient()

    sample_ranges: dict[str, tuple[int, int]] = {}
    current_results: dict[str, PhaseResult] = {}
    phase1_results: dict[str, tuple[PhaseResult, ...]] = {}
    phase2_results: dict[str, tuple[PhaseResult, ...]] = {}
    phase3_results: dict[str, tuple[PhaseResult, ...]] = {}
    final_results: dict[str, PhaseResult] = {}
    all_rows: list[dict[str, object]] = []

    for profile in COIN_PROFILES:
        candles = [candle for candle in load_candle_cache(profile.symbol, BAR, limit=None) if candle.confirmed]
        if not candles:
            raise RuntimeError(f"missing local candles for {profile.symbol} {BAR}")
        instrument = client.get_instrument(profile.symbol)
        sample_ranges[profile.symbol] = (candles[0].ts, candles[-1].ts)

        current = run_current_benchmark(profile, candles, instrument)
        current_results[profile.symbol] = current
        all_rows.extend(rows_for_results((current,)))

        phase1 = run_candidates(
            profile=profile,
            candles=candles,
            instrument=instrument,
            candidates=phase1_candidates(),
        )
        phase1_results[profile.symbol] = phase1
        phase1_best = select_best(phase1)
        all_rows.extend(rows_for_results(phase1))

        phase2 = run_candidates(
            profile=profile,
            candles=candles,
            instrument=instrument,
            candidates=phase2_candidates(phase1_best.config),
        )
        phase2_results[profile.symbol] = phase2
        phase2_best = select_best(phase2)
        all_rows.extend(rows_for_results(phase2))

        phase3 = run_candidates(
            profile=profile,
            candles=candles,
            instrument=instrument,
            candidates=phase3_candidates(phase2_best.config),
        )
        phase3_results[profile.symbol] = phase3
        final_best = select_best(phase3)
        final_results[profile.symbol] = final_best
        all_rows.extend(rows_for_results(phase3))

    write_csv(all_rows)

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "bar": BAR,
        "oos_start_utc": format_ts(OOS_START_TS),
        "risk_amount": str(RISK_AMOUNT),
        "initial_capital": str(INITIAL_CAPITAL),
        "maker_fee_rate": str(MAKER_FEE_RATE),
        "taker_fee_rate": str(TAKER_FEE_RATE),
        "coins": [asdict(profile) for profile in COIN_PROFILES],
        "sample_ranges": sample_ranges,
        "current_results": {key: asdict(value) for key, value in current_results.items()},
        "phase1_results": {key: [asdict(item) for item in value] for key, value in phase1_results.items()},
        "phase2_results": {key: [asdict(item) for item in value] for key, value in phase2_results.items()},
        "phase3_results": {key: [asdict(item) for item in value] for key, value in phase3_results.items()},
        "final_results": {key: asdict(value) for key, value in final_results.items()},
        "rows": all_rows,
    }
    JSON_PATH.write_text(json.dumps(json_ready(payload), ensure_ascii=False, indent=2), encoding="utf-8")

    markdown = build_markdown(
        sample_ranges=sample_ranges,
        current_results=current_results,
        phase1_results=phase1_results,
        phase2_results=phase2_results,
        phase3_results=phase3_results,
        final_results=final_results,
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
