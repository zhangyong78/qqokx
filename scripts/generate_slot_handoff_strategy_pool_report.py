from __future__ import annotations

import csv
import json
import sys
from dataclasses import asdict, dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Iterable

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from okx_quant.backtest import BacktestManualPosition, BacktestResult, run_backtest_batch
from okx_quant.models import Instrument, StrategyConfig
from okx_quant.okx_client import OkxRestClient
from okx_quant.pricing import snap_to_increment
from okx_quant.strategy_catalog import STRATEGY_SLOT_LONG_ID, STRATEGY_SLOT_SHORT_ID

REPORTS_DIR = ROOT / "reports" / "analysis"
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

SYMBOLS = (
    "BTC-USDT-SWAP",
    "ETH-USDT-SWAP",
    "SOL-USDT-SWAP",
    "BNB-USDT-SWAP",
    "DOGE-USDT-SWAP",
)
DIRECTIONS = (
    (STRATEGY_SLOT_LONG_ID, "long_only", "做多"),
    (STRATEGY_SLOT_SHORT_ID, "short_only", "做空"),
)
CANDLE_LIMIT = 10000
BAR = "5m"
TARGET_SLOT_NOTIONAL = Decimal("1000")
MAX_ENTRIES_PER_TREND = 10
INITIAL_CAPITAL = TARGET_SLOT_NOTIONAL * Decimal(str(MAX_ENTRIES_PER_TREND))
MAKER_FEE_RATE = Decimal("0.00015")
TAKER_FEE_RATE = Decimal("0.00036")
ENTRY_SLIPPAGE_RATE = Decimal("0.0001")
EXIT_SLIPPAGE_RATE = Decimal("0.0001")
FUNDING_RATE = Decimal("0.0001")
NEAR_BREAK_EVEN_THRESHOLD_PCT = Decimal("0.50")


@dataclass(frozen=True)
class RunRecord:
    symbol: str
    direction: str
    strategy_id: str
    profile_id: str
    profile_name: str
    profile_summary: str
    order_size: Decimal
    slot_notional: Decimal
    candle_count: int
    window_start: str
    window_end: str
    total_trades: int
    win_rate: Decimal
    total_pnl: Decimal
    mtm_pnl: Decimal
    total_return_pct: Decimal
    mtm_return_pct: Decimal
    average_r_multiple: Decimal
    max_drawdown: Decimal
    max_drawdown_pct: Decimal
    profit_factor: Decimal | None
    profit_loss_ratio: Decimal | None
    ending_equity: Decimal
    total_fees: Decimal
    slippage_costs: Decimal
    funding_costs: Decimal
    take_profit_hits: int
    stop_loss_hits: int
    manual_handoffs: int
    manual_open_positions: int
    manual_open_size: Decimal
    manual_open_pnl: Decimal
    max_manual_positions: int
    max_total_occupied_slots: int
    slot_pressure_pct: Decimal
    slot_full_at: str
    days_to_slot_full: Decimal | None
    manual_handoff_ratio_pct: Decimal
    near_break_even_positions: int
    near_break_even_ratio_pct: Decimal
    data_source_note: str

    @property
    def group_key(self) -> tuple[str, str]:
        return (self.symbol, self.direction)


@dataclass(frozen=True)
class ProfileSummary:
    profile_id: str
    profile_name: str
    profile_summary: str
    runs: int
    profitable_runs: int
    mtm_profitable_runs: int
    top1_count: int
    avg_total_pnl: Decimal
    avg_mtm_pnl: Decimal
    avg_total_return_pct: Decimal
    avg_mtm_return_pct: Decimal
    avg_average_r_multiple: Decimal
    avg_max_drawdown: Decimal
    avg_max_drawdown_pct: Decimal
    avg_slot_pressure_pct: Decimal
    avg_days_to_slot_full: Decimal | None
    avg_manual_handoff_ratio_pct: Decimal
    avg_near_break_even_ratio_pct: Decimal
    avg_total_trades: Decimal
    avg_win_rate: Decimal
    avg_manual_open_positions: Decimal
    avg_manual_open_pnl: Decimal


@dataclass(frozen=True)
class ExportBundle:
    all_runs_csv: Path
    best_by_market_csv: Path
    profile_summary_csv: Path
    report_md: Path
    summary_json: Path
    run_log_txt: Path


def format_decimal(value: Decimal | None) -> str:
    if value is None:
        return "-"
    text = format(value.normalize(), "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def format_decimal_fixed(value: Decimal | None, digits: int = 4) -> str:
    if value is None:
        return "-"
    return f"{value:.{digits}f}"


def format_percent(value: Decimal | None, digits: int = 2) -> str:
    if value is None:
        return "-"
    return f"{value:.{digits}f}%"


def format_ts(ts: int | None) -> str:
    if ts is None:
        return "-"
    scale = 1000 if ts >= 10**12 else 1
    return datetime.fromtimestamp(ts / scale).strftime("%Y-%m-%d %H:%M")


def timestamp_scale(ts: int | None) -> int:
    if ts is None:
        return 1000
    return 1000 if ts >= 10**12 else 1


def decimal_average(values: Iterable[Decimal]) -> Decimal:
    items = list(values)
    if not items:
        return Decimal("0")
    return sum(items, Decimal("0")) / Decimal(str(len(items)))


def manual_break_even_gap_pct(position: BacktestManualPosition) -> Decimal:
    gap_value = abs(position.current_price - position.break_even_price)
    base_price = abs(position.break_even_price)
    if base_price <= 0:
        base_price = abs(position.entry_price)
    if base_price <= 0:
        return Decimal("0")
    return (gap_value / base_price) * Decimal("100")


def resolve_slot_order_size(
    instrument: Instrument,
    reference_price: Decimal,
    *,
    target_notional: Decimal,
) -> tuple[Decimal, Decimal]:
    lot_size = instrument.lot_size if instrument.lot_size > 0 else instrument.min_size
    if lot_size <= 0:
        lot_size = Decimal("0.00000001")
    min_size = instrument.min_size if instrument.min_size > 0 else lot_size
    contract_value = instrument.ct_val if instrument.ct_val and instrument.ct_val > 0 else Decimal("1")
    if reference_price <= 0:
        size = min_size
    else:
        raw_size = target_notional / (reference_price * contract_value)
        size = snap_to_increment(raw_size, lot_size, direction="up")
        if size < min_size:
            size = min_size
    size = snap_to_increment(size, lot_size, direction="up")
    slot_notional = size * contract_value * reference_price
    return size, slot_notional


def build_base_config(
    *,
    symbol: str,
    strategy_id: str,
    signal_mode: str,
    order_size: Decimal,
) -> StrategyConfig:
    return StrategyConfig(
        inst_id=symbol,
        bar=BAR,
        ema_period=21,
        trend_ema_period=55,
        big_ema_period=0,
        atr_period=10,
        atr_stop_multiplier=Decimal("1"),
        atr_take_multiplier=Decimal("2"),
        order_size=order_size,
        trade_mode="cross",
        signal_mode=signal_mode,
        position_mode="net",
        environment="demo",
        tp_sl_trigger_type="mark",
        strategy_id=strategy_id,
        risk_amount=None,
        backtest_initial_capital=INITIAL_CAPITAL,
        backtest_sizing_mode="fixed_size",
        take_profit_mode="fixed",
        max_entries_per_trend=MAX_ENTRIES_PER_TREND,
        entry_reference_ema_period=21,
        backtest_entry_slippage_rate=ENTRY_SLIPPAGE_RATE,
        backtest_exit_slippage_rate=EXIT_SLIPPAGE_RATE,
        backtest_funding_rate=FUNDING_RATE,
    )


def best_record_sort_key(record: RunRecord) -> tuple[Decimal, Decimal, Decimal, Decimal, Decimal]:
    return (
        record.total_pnl,
        record.mtm_pnl,
        record.average_r_multiple,
        -record.max_drawdown,
        Decimal("-999999") if record.days_to_slot_full is None else record.days_to_slot_full,
    )


def pick_best_by_market(records: Iterable[RunRecord]) -> list[RunRecord]:
    best_map: dict[tuple[str, str], RunRecord] = {}
    for record in records:
        current = best_map.get(record.group_key)
        if current is None or best_record_sort_key(record) > best_record_sort_key(current):
            best_map[record.group_key] = record
    return sorted(best_map.values(), key=lambda item: (item.symbol, item.direction))


def summarize_profiles(records: list[RunRecord], best_by_market: list[RunRecord]) -> list[ProfileSummary]:
    top1_counter: dict[str, int] = {}
    for record in best_by_market:
        top1_counter[record.profile_id] = top1_counter.get(record.profile_id, 0) + 1

    grouped: dict[str, list[RunRecord]] = {}
    for record in records:
        grouped.setdefault(record.profile_id, []).append(record)

    summaries: list[ProfileSummary] = []
    for profile_id, items in grouped.items():
        sample = items[0]
        summaries.append(
            ProfileSummary(
                profile_id=profile_id,
                profile_name=sample.profile_name,
                profile_summary=sample.profile_summary,
                runs=len(items),
                profitable_runs=sum(1 for item in items if item.total_pnl > 0),
                mtm_profitable_runs=sum(1 for item in items if item.mtm_pnl > 0),
                top1_count=top1_counter.get(profile_id, 0),
                avg_total_pnl=decimal_average(item.total_pnl for item in items),
                avg_mtm_pnl=decimal_average(item.mtm_pnl for item in items),
                avg_total_return_pct=decimal_average(item.total_return_pct for item in items),
                avg_mtm_return_pct=decimal_average(item.mtm_return_pct for item in items),
                avg_average_r_multiple=decimal_average(item.average_r_multiple for item in items),
                avg_max_drawdown=decimal_average(item.max_drawdown for item in items),
                avg_max_drawdown_pct=decimal_average(item.max_drawdown_pct for item in items),
                avg_slot_pressure_pct=decimal_average(item.slot_pressure_pct for item in items),
                avg_days_to_slot_full=(
                    decimal_average(item.days_to_slot_full for item in items if item.days_to_slot_full is not None)
                    if any(item.days_to_slot_full is not None for item in items)
                    else None
                ),
                avg_manual_handoff_ratio_pct=decimal_average(item.manual_handoff_ratio_pct for item in items),
                avg_near_break_even_ratio_pct=decimal_average(item.near_break_even_ratio_pct for item in items),
                avg_total_trades=decimal_average(Decimal(item.total_trades) for item in items),
                avg_win_rate=decimal_average(item.win_rate for item in items),
                avg_manual_open_positions=decimal_average(Decimal(item.manual_open_positions) for item in items),
                avg_manual_open_pnl=decimal_average(item.manual_open_pnl for item in items),
            )
        )
    return sorted(
        summaries,
        key=lambda item: (
            item.top1_count,
            item.profitable_runs,
            item.avg_total_pnl,
            item.avg_mtm_pnl,
            item.avg_average_r_multiple,
            Decimal("-999999") if item.avg_days_to_slot_full is None else item.avg_days_to_slot_full,
            -item.avg_slot_pressure_pct,
        ),
        reverse=True,
    )


def csv_ready_row(payload: dict[str, object]) -> dict[str, object]:
    row: dict[str, object] = {}
    for key, value in payload.items():
        if isinstance(value, Decimal):
            row[key] = format_decimal(value)
        else:
            row[key] = value
    return row


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def build_exports(stamp: str) -> ExportBundle:
    return ExportBundle(
        all_runs_csv=REPORTS_DIR / f"slot_handoff_strategy_pool_all_runs_{stamp}.csv",
        best_by_market_csv=REPORTS_DIR / f"slot_handoff_strategy_pool_best_by_market_{stamp}.csv",
        profile_summary_csv=REPORTS_DIR / f"slot_handoff_strategy_pool_profile_summary_{stamp}.csv",
        report_md=REPORTS_DIR / f"slot_handoff_strategy_pool_report_{stamp}.md",
        summary_json=REPORTS_DIR / f"slot_handoff_strategy_pool_summary_{stamp}.json",
        run_log_txt=REPORTS_DIR / f"slot_handoff_strategy_pool_run_log_{stamp}.txt",
    )


def build_report(
    *,
    generated_at: str,
    records: list[RunRecord],
    best_by_market: list[RunRecord],
    profile_summaries: list[ProfileSummary],
    log_lines: list[str],
) -> str:
    primary_profile = profile_summaries[0] if profile_summaries else None
    aggressive_profile = max(records, key=lambda item: (item.total_pnl, item.slot_pressure_pct), default=None)
    lines = [
        "# 5m 槽位接管策略池深测",
        "",
        f"- 生成时间：{generated_at}",
        f"- 样本交易对：{', '.join(SYMBOLS)}",
        f"- 方向：{', '.join(direction for _, _, direction in DIRECTIONS)}",
        f"- 回测根数：每组 {CANDLE_LIMIT} 根 5m K 线",
        f"- 槽位上限：{MAX_ENTRIES_PER_TREND}",
        f"- 单槽目标名义：{format_decimal(TARGET_SLOT_NOTIONAL)} USDT",
        f"- 初始资金：{format_decimal(INITIAL_CAPITAL)} USDT",
        "- MTM 口径说明：仅统计期货腿，不叠加现货持仓盈亏。",
        (
            "- 成本口径："
            f"maker {format_percent(MAKER_FEE_RATE * Decimal('100'))} / "
            f"taker {format_percent(TAKER_FEE_RATE * Decimal('100'))} / "
            f"开仓滑点 {format_percent(ENTRY_SLIPPAGE_RATE * Decimal('100'))} / "
            f"平仓滑点 {format_percent(EXIT_SLIPPAGE_RATE * Decimal('100'))} / "
            f"资金费 {format_percent(FUNDING_RATE * Decimal('100'))} 每 8h"
        ),
        "",
        "## 结论",
        "",
    ]
    if primary_profile is not None:
        lines.extend(
            [
                (
                    f"- 主推荐：`{primary_profile.profile_name}`，"
                    f"Top1 次数 {primary_profile.top1_count}/10，"
                    f"盈利场景 {primary_profile.profitable_runs}/{primary_profile.runs}，"
                    f"平均总盈亏 {format_decimal_fixed(primary_profile.avg_total_pnl, 4)}，"
                    f"平均 MTM {format_decimal_fixed(primary_profile.avg_mtm_pnl, 4)}，"
                    f"平均峰值占槽 {format_percent(primary_profile.avg_slot_pressure_pct)}，"
                    f"平均打满天数 {format_decimal_fixed(primary_profile.avg_days_to_slot_full, 2) if primary_profile.avg_days_to_slot_full is not None else '-'}。"
                ),
                f"- 主推荐说明：{primary_profile.profile_summary}",
            ]
        )
    if aggressive_profile is not None:
        lines.append(
            (
                f"- 进攻型观察：单组最高总盈亏来自 `{aggressive_profile.profile_name}` "
                f"@ {aggressive_profile.symbol} {aggressive_profile.direction}，"
                f"总盈亏 {format_decimal_fixed(aggressive_profile.total_pnl, 4)}，"
                f"峰值占槽 {format_percent(aggressive_profile.slot_pressure_pct)}。"
            )
        )
    if all(item.mtm_pnl <= 0 for item in records):
        lines.append(
            "- 风险结论：若按“仅期货腿”的 MTM 口径，把期末人工池浮盈亏一并计入，本次 60 组回测全部为负。这说明它目前不能直接视为无人值守自动盈利系统，必须结合你的现货对冲和人工释放槽位来理解。"
        )
    lines.extend(
        [
            "- 解释口径：本报告优先看“总盈亏 + 平均 R + 峰值占槽 + 人工移交比例”，不只看胜率，避免把高移交压力误判成优策略。",
            "- 注意：`MTM盈亏` 是期货腿单独口径；如果你的现货腿与之对应，组合后的真实风险暴露会比这里更低。",
            "- 胜率口径说明：这里的胜率只统计系统自动平掉的已实现交易；被移交到人工池的亏损仓，不会在已实现胜率里体现。",
            "",
            "## 各市场优胜候选",
            "",
            "| 交易对 | 方向 | 候选 | 已实现盈亏 | MTM盈亏 | 平均R | 打满天数 | 峰值占槽 | 人工移交 | 期末人工池 | 最接近保本仓位 |",
            "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for item in best_by_market:
        lines.append(
            "| "
            + " | ".join(
                [
                    item.symbol,
                    item.direction,
                    item.profile_name,
                    format_decimal_fixed(item.total_pnl, 4),
                    format_decimal_fixed(item.mtm_pnl, 4),
                    format_decimal_fixed(item.average_r_multiple, 4),
                    format_decimal_fixed(item.days_to_slot_full, 2) if item.days_to_slot_full is not None else "-",
                    format_percent(item.slot_pressure_pct),
                    f"{item.manual_handoffs}",
                    f"{item.manual_open_positions}",
                    f"{item.near_break_even_positions}",
                ]
            )
            + " |"
        )

    lines.extend(
        [
            "",
            "## 候选总表",
            "",
            "| 候选 | Top1 | 已实现盈利组数 | MTM盈利组数 | 平均已实现盈亏 | 平均MTM盈亏 | 平均收益率 | 平均MTM收益率 | 平均R | 平均回撤 | 平均打满天数 | 平均占槽 | 平均人工移交率 | 平均近保本比率 |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for item in profile_summaries:
        lines.append(
            "| "
            + " | ".join(
                [
                    item.profile_name,
                    str(item.top1_count),
                    f"{item.profitable_runs}/{item.runs}",
                    f"{item.mtm_profitable_runs}/{item.runs}",
                    format_decimal_fixed(item.avg_total_pnl, 4),
                    format_decimal_fixed(item.avg_mtm_pnl, 4),
                    format_percent(item.avg_total_return_pct),
                    format_percent(item.avg_mtm_return_pct),
                    format_decimal_fixed(item.avg_average_r_multiple, 4),
                    format_decimal_fixed(item.avg_max_drawdown, 4),
                    format_decimal_fixed(item.avg_days_to_slot_full, 2) if item.avg_days_to_slot_full is not None else "-",
                    format_percent(item.avg_slot_pressure_pct),
                    format_percent(item.avg_manual_handoff_ratio_pct),
                    format_percent(item.avg_near_break_even_ratio_pct),
                ]
            )
            + " |"
        )

    lines.extend(
        [
            "",
            "## 复现说明",
            "",
            "- 所有回测均通过 `scripts/generate_slot_handoff_strategy_pool_report.py` 生成。",
            "- 该脚本会固化样本交易对、5m 周期、10 槽、成本参数，并输出 CSV / Markdown / JSON / 运行日志。",
            "- 若后续要改“最大槽位”或“单槽目标名义”，建议复制本脚本后单独留档，不要覆盖本次基准报告。",
            "",
            "## 运行摘要",
            "",
        ]
    )
    lines.extend(f"- {line}" for line in log_lines[-12:])
    return "\n".join(lines) + "\n"


def main() -> None:
    client = OkxRestClient()
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    exports = build_exports(stamp)
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    records: list[RunRecord] = []
    log_lines: list[str] = []

    total = len(SYMBOLS) * len(DIRECTIONS)
    sequence = 0
    for symbol in SYMBOLS:
        instrument = client.get_instrument(symbol)
        latest_candle = client.get_candles(symbol, BAR, limit=1)
        if not latest_candle:
            raise RuntimeError(f"无法获取 {symbol} 最新 {BAR} K 线，无法计算固定槽位数量。")
        reference_price = latest_candle[-1].close
        order_size, slot_notional = resolve_slot_order_size(
            instrument,
            reference_price,
            target_notional=TARGET_SLOT_NOTIONAL,
        )
        for strategy_id, signal_mode, direction_label in DIRECTIONS:
            sequence += 1
            header = (
                f"[{sequence}/{total}] {symbol} {direction_label} | "
                f"order_size={format_decimal(order_size)} | slot_notional≈{format_decimal_fixed(slot_notional, 2)}"
            )
            print(header)
            log_lines.append(header)
            base_config = build_base_config(
                symbol=symbol,
                strategy_id=strategy_id,
                signal_mode=signal_mode,
                order_size=order_size,
            )
            batch_results = run_backtest_batch(
                client,
                base_config,
                candle_limit=CANDLE_LIMIT,
                maker_fee_rate=MAKER_FEE_RATE,
                taker_fee_rate=TAKER_FEE_RATE,
            )
            if not batch_results:
                raise RuntimeError(f"{symbol} {direction_label} 未返回策略池结果。")
            for config, result in batch_results:
                report = result.report
                slot_pressure_pct = (
                    Decimal("0")
                    if config.max_entries_per_trend <= 0
                    else (Decimal(report.max_total_occupied_slots) / Decimal(str(config.max_entries_per_trend))) * Decimal("100")
                )
                slot_full_ts = None
                days_to_slot_full = None
                if report.manual_open_positions >= config.max_entries_per_trend and result.manual_positions:
                    slot_full_ts = max(position.handoff_ts for position in result.manual_positions)
                    if result.candles:
                        start_ts = result.candles[0].ts
                        scale = timestamp_scale(start_ts)
                        elapsed_days = Decimal(str((slot_full_ts - start_ts) / (scale * 60 * 60 * 24)))
                        days_to_slot_full = elapsed_days if elapsed_days >= 0 else Decimal("0")
                denominator = report.total_trades + report.manual_handoffs
                manual_handoff_ratio_pct = (
                    Decimal("0")
                    if denominator <= 0
                    else (Decimal(report.manual_handoffs) / Decimal(str(denominator))) * Decimal("100")
                )
                near_break_even_positions = sum(
                    1
                    for position in result.manual_positions
                    if manual_break_even_gap_pct(position) <= NEAR_BREAK_EVEN_THRESHOLD_PCT
                )
                near_break_even_ratio_pct = (
                    Decimal("0")
                    if report.manual_open_positions <= 0
                    else (Decimal(near_break_even_positions) / Decimal(str(report.manual_open_positions))) * Decimal("100")
                )
                record = RunRecord(
                    symbol=symbol,
                    direction=direction_label,
                    strategy_id=strategy_id,
                    profile_id=config.backtest_profile_id,
                    profile_name=config.backtest_profile_name or config.backtest_profile_id,
                    profile_summary=config.backtest_profile_summary,
                    order_size=order_size,
                    slot_notional=slot_notional,
                    candle_count=len(result.candles),
                    window_start=format_ts(result.candles[0].ts if result.candles else None),
                    window_end=format_ts(result.candles[-1].ts if result.candles else None),
                    total_trades=report.total_trades,
                    win_rate=report.win_rate,
                    total_pnl=report.total_pnl,
                    mtm_pnl=report.total_pnl + report.manual_open_pnl,
                    total_return_pct=report.total_return_pct,
                    mtm_return_pct=((report.total_pnl + report.manual_open_pnl) / INITIAL_CAPITAL) * Decimal("100"),
                    average_r_multiple=report.average_r_multiple,
                    max_drawdown=report.max_drawdown,
                    max_drawdown_pct=report.max_drawdown_pct,
                    profit_factor=report.profit_factor,
                    profit_loss_ratio=report.profit_loss_ratio,
                    ending_equity=report.ending_equity,
                    total_fees=report.total_fees,
                    slippage_costs=report.slippage_costs,
                    funding_costs=report.funding_costs,
                    take_profit_hits=report.take_profit_hits,
                    stop_loss_hits=report.stop_loss_hits,
                    manual_handoffs=report.manual_handoffs,
                    manual_open_positions=report.manual_open_positions,
                    manual_open_size=report.manual_open_size,
                    manual_open_pnl=report.manual_open_pnl,
                    max_manual_positions=report.max_manual_positions,
                    max_total_occupied_slots=report.max_total_occupied_slots,
                    slot_pressure_pct=slot_pressure_pct,
                    slot_full_at=format_ts(slot_full_ts),
                    days_to_slot_full=days_to_slot_full,
                    manual_handoff_ratio_pct=manual_handoff_ratio_pct,
                    near_break_even_positions=near_break_even_positions,
                    near_break_even_ratio_pct=near_break_even_ratio_pct,
                    data_source_note=result.data_source_note,
                )
                records.append(record)
                detail_line = (
                    f"  - {record.profile_name}: pnl={format_decimal_fixed(record.total_pnl, 4)} | "
                    f"mtm={format_decimal_fixed(record.mtm_pnl, 4)} | "
                    f"R={format_decimal_fixed(record.average_r_multiple, 4)} | "
                    f"DD={format_decimal_fixed(record.max_drawdown, 4)} | "
                    f"full_days={format_decimal_fixed(record.days_to_slot_full, 2) if record.days_to_slot_full is not None else '-'} | "
                    f"pressure={format_percent(record.slot_pressure_pct)} | "
                    f"handoff={record.manual_handoffs} | manual_open={record.manual_open_positions}"
                )
                print(detail_line)
                log_lines.append(detail_line)

    best_by_market = pick_best_by_market(records)
    profile_summaries = summarize_profiles(records, best_by_market)

    all_run_rows = [csv_ready_row(asdict(item)) for item in records]
    best_rows = [csv_ready_row(asdict(item)) for item in best_by_market]
    summary_rows = [csv_ready_row(asdict(item)) for item in profile_summaries]
    write_csv(exports.all_runs_csv, all_run_rows)
    write_csv(exports.best_by_market_csv, best_rows)
    write_csv(exports.profile_summary_csv, summary_rows)

    exports.report_md.write_text(
        build_report(
            generated_at=generated_at,
            records=records,
            best_by_market=best_by_market,
            profile_summaries=profile_summaries,
            log_lines=log_lines,
        ),
        encoding="utf-8",
    )

    summary_payload = {
        "generated_at": generated_at,
        "scope": {
            "symbols": list(SYMBOLS),
            "directions": [label for _, _, label in DIRECTIONS],
            "bar": BAR,
            "candle_limit": CANDLE_LIMIT,
            "target_slot_notional": format_decimal(TARGET_SLOT_NOTIONAL),
            "max_entries_per_trend": MAX_ENTRIES_PER_TREND,
            "initial_capital": format_decimal(INITIAL_CAPITAL),
            "maker_fee_rate": format_decimal(MAKER_FEE_RATE),
            "taker_fee_rate": format_decimal(TAKER_FEE_RATE),
            "entry_slippage_rate": format_decimal(ENTRY_SLIPPAGE_RATE),
            "exit_slippage_rate": format_decimal(EXIT_SLIPPAGE_RATE),
            "funding_rate": format_decimal(FUNDING_RATE),
        },
        "best_by_market": best_rows,
        "profile_summary": summary_rows,
        "exports": {
            "all_runs_csv": str(exports.all_runs_csv),
            "best_by_market_csv": str(exports.best_by_market_csv),
            "profile_summary_csv": str(exports.profile_summary_csv),
            "report_md": str(exports.report_md),
            "run_log_txt": str(exports.run_log_txt),
        },
    }
    exports.summary_json.write_text(json.dumps(summary_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    exports.run_log_txt.write_text("\n".join(log_lines) + "\n", encoding="utf-8")

    print(f"all runs -> {exports.all_runs_csv}")
    print(f"best by market -> {exports.best_by_market_csv}")
    print(f"profile summary -> {exports.profile_summary_csv}")
    print(f"report -> {exports.report_md}")
    print(f"summary json -> {exports.summary_json}")
    print(f"run log -> {exports.run_log_txt}")


if __name__ == "__main__":
    main()
