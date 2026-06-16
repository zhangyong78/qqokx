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
BASE_NAME = f"eth_dynamic_long_template_refine_ema55_{STAMP}"
CSV_PATH = REPORT_DIR / f"{BASE_NAME}.csv"
JSON_PATH = REPORT_DIR / f"{BASE_NAME}.json"
MD_PATH = REPORT_DIR / f"{BASE_NAME}.md"
LATEST_MD_PATH = PROJECT_REPORT_DIR / "eth_dynamic_long_template_refine_ema55_latest.md"
LATEST_CSV_PATH = PROJECT_REPORT_DIR / "eth_dynamic_long_template_refine_ema55_latest.csv"
LATEST_JSON_PATH = PROJECT_REPORT_DIR / "eth_dynamic_long_template_refine_ema55_latest.json"


@dataclass(frozen=True)
class EthProfile:
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
        return (
            f"EMA{self.ema_period} / EMA{self.trend_ema_period} / "
            f"挂单 EMA{self.entry_reference_ema_period}"
        )


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
    config: PhaseConfig
    full_metrics: Metrics
    oos_metrics: Metrics


@dataclass(frozen=True)
class EntryEmaCompareRow:
    entry_reference_ema_period: int
    label: str
    full_metrics: Metrics
    oos_metrics: Metrics


CURRENT_PROFILE = EthProfile(
    symbol="ETH-USDT-SWAP",
    label="ETH",
    ema_period=21,
    trend_ema_period=55,
    entry_reference_ema_period=34,
    current_stop_multiplier=Decimal("1.5"),
    current_trigger_r=3,
    current_max_entries=3,
)

EMA55_PROFILE = EthProfile(
    symbol="ETH-USDT-SWAP",
    label="ETH",
    ema_period=21,
    trend_ema_period=55,
    entry_reference_ema_period=55,
    current_stop_multiplier=Decimal("1.5"),
    current_trigger_r=3,
    current_max_entries=3,
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


def build_template_config(profile: EthProfile, phase_config: PhaseConfig) -> StrategyConfig:
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


def build_current_best_config(profile: EthProfile) -> StrategyConfig:
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
    rows: list[PhaseConfig] = []
    for atr_stop_multiplier in (Decimal("1"), Decimal("1.5"), Decimal("2")):
        for max_entries_per_trend in (1, 2, 3):
            rows.append(
                PhaseConfig(
                    phase="atr_entries",
                    candidate_id=f"sl_{str(atr_stop_multiplier).replace('.', '_')}_entries_{max_entries_per_trend}",
                    label=(
                        f"SL{atr_stop_multiplier} / 1R保本 / 4R锁1R / "
                        f"11R锁10R / 每波 {max_entries_per_trend} 次"
                    ),
                    atr_stop_multiplier=atr_stop_multiplier,
                    break_even_trigger_r=1,
                    lock_trigger_r=4,
                    max_entries_per_trend=max_entries_per_trend,
                )
            )
    return tuple(rows)


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
        for break_even_trigger_r in (1, 2, 3)
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
        for lock_trigger_r in (4, 5, 6, 7, 8)
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


def run_backtest(candles, instrument, config: StrategyConfig) -> tuple[Metrics, Metrics]:
    result = _run_backtest_with_loaded_data(
        candles,
        instrument,
        config,
        data_source_note=f"local candle_cache full history | {config.inst_id} | {config.bar}",
        maker_fee_rate=MAKER_FEE_RATE,
        taker_fee_rate=TAKER_FEE_RATE,
    )
    full_trades = list(result.trades)
    oos_trades = filter_oos_trades(full_trades)
    return build_metrics(full_trades), build_metrics(oos_trades)


def run_candidates(
    *,
    profile: EthProfile,
    candles,
    instrument,
    candidates: tuple[PhaseConfig, ...],
) -> tuple[PhaseResult, ...]:
    rows: list[PhaseResult] = []
    total = len(candidates)
    for index, candidate in enumerate(candidates, start=1):
        print(f"[{profile.label}][{candidate.phase} {index}/{total}] {candidate.label}")
        full_metrics, oos_metrics = run_backtest(candles, instrument, build_template_config(profile, candidate))
        rows.append(PhaseResult(config=candidate, full_metrics=full_metrics, oos_metrics=oos_metrics))
    return tuple(rows)


def run_previous_final_ema34(candles, instrument) -> PhaseResult:
    config = PhaseConfig(
        phase="previous_final_ema34",
        candidate_id="ema34_previous_final",
        label="SL1.5 / 3R保本 / 4R锁1R / 11R锁10R / 每波 3 次",
        atr_stop_multiplier=Decimal("1.5"),
        break_even_trigger_r=3,
        lock_trigger_r=4,
        max_entries_per_trend=3,
    )
    full_metrics, oos_metrics = run_backtest(candles, instrument, build_template_config(CURRENT_PROFILE, config))
    return PhaseResult(config=config, full_metrics=full_metrics, oos_metrics=oos_metrics)


def run_entry_ema_focus(
    *,
    candles,
    instrument,
    final_config: PhaseConfig,
) -> tuple[EntryEmaCompareRow, ...]:
    rows: list[EntryEmaCompareRow] = []
    for entry_period in (21, 34, 55):
        profile = EthProfile(
            symbol="ETH-USDT-SWAP",
            label="ETH",
            ema_period=21,
            trend_ema_period=55,
            entry_reference_ema_period=entry_period,
            current_stop_multiplier=Decimal("1.5"),
            current_trigger_r=3,
            current_max_entries=3,
        )
        label = f"EMA21 / EMA55 / 挂单 EMA{entry_period}"
        print(f"[ETH][entry_ema] {label}")
        full_metrics, oos_metrics = run_backtest(candles, instrument, build_template_config(profile, final_config))
        rows.append(
            EntryEmaCompareRow(
                entry_reference_ema_period=entry_period,
                label=label,
                full_metrics=full_metrics,
                oos_metrics=oos_metrics,
            )
        )
    return tuple(rows)


def build_markdown(
    *,
    sample_range: tuple[int, int],
    current_result: PhaseResult,
    previous_final_ema34: PhaseResult,
    phase1_results: tuple[PhaseResult, ...],
    phase2_results: tuple[PhaseResult, ...],
    phase3_results: tuple[PhaseResult, ...],
    final_result: PhaseResult,
    entry_ema_focus: tuple[EntryEmaCompareRow, ...],
) -> str:
    sample_start_ts, sample_end_ts = sample_range
    lines = [
        "# ETH 动态委托做多 EMA55 跟进",
        "",
        "## 锁死口径",
        "",
        "- 标的：`ETH-USDT-SWAP`",
        f"- 周期：`{BAR}`",
        "- K线来源：本地 `candle_cache` 全历史已收盘 K 线",
        f"- 固定风险金：`{fmt(RISK_AMOUNT, 0)}`U / 笔",
        f"- 初始资金：`{fmt(INITIAL_CAPITAL, 0)}`U",
        f"- 手续费：Maker `{fmt(MAKER_FEE_RATE * Decimal('100'), 4)}%` / Taker `{fmt(TAKER_FEE_RATE * Decimal('100'), 4)}%`",
        "- 样本外：`2022-01-01 00:00:00 UTC` 之后按平仓时间统计",
        "- 结构约束：`锁盈触发R > 保本触发R` 才算有效组合",
        "- 本轮只做研究，不同步默认参数、UI 或实盘口径",
        "",
        "## 结论摘要",
        "",
        f"- 当前样本范围：`{format_ts(sample_start_ts)}` -> `{format_ts(sample_end_ts)}`",
        f"- 当前最佳参数包（EMA34骨架）样本外：PnL `{fmt(current_result.oos_metrics.pnl)}` / PF `{fmt(current_result.oos_metrics.profit_factor)}` / AvgR `{fmt(current_result.oos_metrics.avg_r)}` / DD `{fmt(current_result.oos_metrics.max_drawdown)}` / Trades `{current_result.oos_metrics.trades}`",
        f"- 上一版研究定稿（EMA34骨架）样本外：PnL `{fmt(previous_final_ema34.oos_metrics.pnl)}` / PF `{fmt(previous_final_ema34.oos_metrics.profit_factor)}` / AvgR `{fmt(previous_final_ema34.oos_metrics.avg_r)}` / DD `{fmt(previous_final_ema34.oos_metrics.max_drawdown)}` / Trades `{previous_final_ema34.oos_metrics.trades}`",
        f"- 本轮 EMA55 定稿样本外：PnL `{fmt(final_result.oos_metrics.pnl)}` / PF `{fmt(final_result.oos_metrics.profit_factor)}` / AvgR `{fmt(final_result.oos_metrics.avg_r)}` / DD `{fmt(final_result.oos_metrics.max_drawdown)}` / Trades `{final_result.oos_metrics.trades}`",
        f"- 本轮 EMA55 定稿参数：`{final_result.config.label}`",
        "",
        "## 分阶段结果",
        "",
        "### 第一层：SL 倍数 + 每波次数",
        "",
        "| 方案 | 全样本PnL | 全样本DD | 全样本PF | 全样本AvgR | 交易笔数 | 样本外PnL | 样本外DD | 样本外PF | 样本外AvgR | 样本外交易 |",
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
            "### 第二层：保本 R",
            "",
            "| 方案 | 全样本PnL | 全样本DD | 全样本PF | 全样本AvgR | 交易笔数 | 样本外PnL | 样本外DD | 样本外PF | 样本外AvgR | 样本外交易 |",
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
            "### 第三层：首档锁盈 R",
            "",
            "| 方案 | 全样本PnL | 全样本DD | 全样本PF | 全样本AvgR | 交易笔数 | 样本外PnL | 样本外DD | 样本外PF | 样本外AvgR | 样本外交易 |",
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
            "## 挂单均线邻近对比",
            "",
            "- 控制变量：固定本轮最终保护参数，只换挂单均线",
            "",
            "| 挂单均线 | 全样本PnL | 全样本DD | 全样本PF | 全样本AvgR | 交易笔数 | 样本外PnL | 样本外DD | 样本外PF | 样本外AvgR | 样本外交易 |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for item in sorted(entry_ema_focus, key=lambda row: (row.oos_metrics.pnl, row.oos_metrics.profit_factor or Decimal("0")), reverse=True):
        lines.append(
            f"| {item.label} | {fmt(item.full_metrics.pnl)} | {fmt(item.full_metrics.max_drawdown)} | "
            f"{fmt(item.full_metrics.profit_factor)} | {fmt(item.full_metrics.avg_r)} | {item.full_metrics.trades} | "
            f"{fmt(item.oos_metrics.pnl)} | {fmt(item.oos_metrics.max_drawdown)} | {fmt(item.oos_metrics.profit_factor)} | "
            f"{fmt(item.oos_metrics.avg_r)} | {item.oos_metrics.trades} |"
        )
    lines.extend(["", f"- 明细文件：`{CSV_PATH}` / `{JSON_PATH}`", ""])
    return "\n".join(lines)


def write_csv(
    *,
    current_result: PhaseResult,
    previous_final_ema34: PhaseResult,
    phase1_results: tuple[PhaseResult, ...],
    phase2_results: tuple[PhaseResult, ...],
    phase3_results: tuple[PhaseResult, ...],
    entry_ema_focus: tuple[EntryEmaCompareRow, ...],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []

    def add_phase_row(group: str, result: PhaseResult) -> None:
        rows.append(
            {
                "group": group,
                "candidate_id": result.config.candidate_id,
                "label": result.config.label,
                "entry_reference_ema_period": EMA55_PROFILE.entry_reference_ema_period,
                "atr_stop_multiplier": str(result.config.atr_stop_multiplier),
                "break_even_trigger_r": result.config.break_even_trigger_r,
                "lock_trigger_r": result.config.lock_trigger_r,
                "max_entries_per_trend": result.config.max_entries_per_trend,
                "full_pnl": str(result.full_metrics.pnl),
                "full_trades": result.full_metrics.trades,
                "full_pf": None if result.full_metrics.profit_factor is None else str(result.full_metrics.profit_factor),
                "full_avg_r": str(result.full_metrics.avg_r),
                "full_max_drawdown": str(result.full_metrics.max_drawdown),
                "oos_pnl": str(result.oos_metrics.pnl),
                "oos_trades": result.oos_metrics.trades,
                "oos_pf": None if result.oos_metrics.profit_factor is None else str(result.oos_metrics.profit_factor),
                "oos_avg_r": str(result.oos_metrics.avg_r),
                "oos_max_drawdown": str(result.oos_metrics.max_drawdown),
            }
        )

    add_phase_row("current_best_package_ema34", current_result)
    add_phase_row("previous_final_ema34", previous_final_ema34)
    for item in phase1_results:
        add_phase_row("phase1", item)
    for item in phase2_results:
        add_phase_row("phase2", item)
    for item in phase3_results:
        add_phase_row("phase3", item)
    for item in entry_ema_focus:
        rows.append(
            {
                "group": "entry_ema_focus",
                "candidate_id": f"entry_ema_{item.entry_reference_ema_period}",
                "label": item.label,
                "entry_reference_ema_period": item.entry_reference_ema_period,
                "atr_stop_multiplier": "",
                "break_even_trigger_r": "",
                "lock_trigger_r": "",
                "max_entries_per_trend": "",
                "full_pnl": str(item.full_metrics.pnl),
                "full_trades": item.full_metrics.trades,
                "full_pf": None if item.full_metrics.profit_factor is None else str(item.full_metrics.profit_factor),
                "full_avg_r": str(item.full_metrics.avg_r),
                "full_max_drawdown": str(item.full_metrics.max_drawdown),
                "oos_pnl": str(item.oos_metrics.pnl),
                "oos_trades": item.oos_metrics.trades,
                "oos_pf": None if item.oos_metrics.profit_factor is None else str(item.oos_metrics.profit_factor),
                "oos_avg_r": str(item.oos_metrics.avg_r),
                "oos_max_drawdown": str(item.oos_metrics.max_drawdown),
            }
        )

    fieldnames = [
        "group",
        "candidate_id",
        "label",
        "entry_reference_ema_period",
        "atr_stop_multiplier",
        "break_even_trigger_r",
        "lock_trigger_r",
        "max_entries_per_trend",
        "full_pnl",
        "full_trades",
        "full_pf",
        "full_avg_r",
        "full_max_drawdown",
        "oos_pnl",
        "oos_trades",
        "oos_pf",
        "oos_avg_r",
        "oos_max_drawdown",
    ]
    with CSV_PATH.open("w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return rows


def main() -> None:
    client = OkxRestClient()
    candles = [candle for candle in load_candle_cache(EMA55_PROFILE.symbol, BAR, limit=None) if candle.confirmed]
    if not candles:
        raise RuntimeError(f"missing local candles for {EMA55_PROFILE.symbol} {BAR}")
    instrument = client.get_instrument(EMA55_PROFILE.symbol)
    sample_range = (candles[0].ts, candles[-1].ts)

    current_full, current_oos = run_backtest(candles, instrument, build_current_best_config(CURRENT_PROFILE))
    current_result = PhaseResult(
        config=PhaseConfig(
            phase="current",
            candidate_id="current_best_package",
            label="当前最佳参数包 / EMA21 / EMA55 / 挂单 EMA34 / SL1.5 / 3R触发动态保护 / 每波 3 次",
            atr_stop_multiplier=CURRENT_PROFILE.current_stop_multiplier,
            break_even_trigger_r=2,
            lock_trigger_r=CURRENT_PROFILE.current_trigger_r,
            max_entries_per_trend=CURRENT_PROFILE.current_max_entries,
        ),
        full_metrics=current_full,
        oos_metrics=current_oos,
    )

    previous_final_ema34 = run_previous_final_ema34(candles, instrument)

    phase1_results = run_candidates(
        profile=EMA55_PROFILE,
        candles=candles,
        instrument=instrument,
        candidates=phase1_candidates(),
    )
    phase1_best = select_best(phase1_results)

    phase2_results = run_candidates(
        profile=EMA55_PROFILE,
        candles=candles,
        instrument=instrument,
        candidates=phase2_candidates(phase1_best.config),
    )
    phase2_best = select_best(phase2_results)

    phase3_results = run_candidates(
        profile=EMA55_PROFILE,
        candles=candles,
        instrument=instrument,
        candidates=phase3_candidates(phase2_best.config),
    )
    final_result = select_best(phase3_results)

    entry_ema_focus = run_entry_ema_focus(
        candles=candles,
        instrument=instrument,
        final_config=final_result.config,
    )

    rows = write_csv(
        current_result=current_result,
        previous_final_ema34=previous_final_ema34,
        phase1_results=phase1_results,
        phase2_results=phase2_results,
        phase3_results=phase3_results,
        entry_ema_focus=entry_ema_focus,
    )

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "symbol": EMA55_PROFILE.symbol,
        "bar": BAR,
        "oos_start_utc": format_ts(OOS_START_TS),
        "risk_amount": str(RISK_AMOUNT),
        "initial_capital": str(INITIAL_CAPITAL),
        "maker_fee_rate": str(MAKER_FEE_RATE),
        "taker_fee_rate": str(TAKER_FEE_RATE),
        "sample_range": sample_range,
        "current_profile": asdict(CURRENT_PROFILE),
        "ema55_profile": asdict(EMA55_PROFILE),
        "current_result": asdict(current_result),
        "previous_final_ema34": asdict(previous_final_ema34),
        "phase1_results": [asdict(item) for item in phase1_results],
        "phase2_results": [asdict(item) for item in phase2_results],
        "phase3_results": [asdict(item) for item in phase3_results],
        "final_result": asdict(final_result),
        "entry_ema_focus": [asdict(item) for item in entry_ema_focus],
        "rows": rows,
    }
    JSON_PATH.write_text(json.dumps(json_ready(payload), ensure_ascii=False, indent=2), encoding="utf-8")

    markdown = build_markdown(
        sample_range=sample_range,
        current_result=current_result,
        previous_final_ema34=previous_final_ema34,
        phase1_results=phase1_results,
        phase2_results=phase2_results,
        phase3_results=phase3_results,
        final_result=final_result,
        entry_ema_focus=entry_ema_focus,
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
