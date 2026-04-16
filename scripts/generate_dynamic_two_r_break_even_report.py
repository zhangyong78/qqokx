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

from okx_quant.backtest import _build_backtest_data_source_note, _load_backtest_candles, _run_backtest_with_loaded_data
from okx_quant.models import Instrument, StrategyConfig
from okx_quant.okx_client import OkxRestClient
from okx_quant.strategy_catalog import STRATEGY_DYNAMIC_LONG_ID, STRATEGY_DYNAMIC_SHORT_ID

REPORTS_DIR = ROOT / "reports" / "analysis"
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

SYMBOLS = (
    "BTC-USDT-SWAP",
    "ETH-USDT-SWAP",
    "BNB-USDT-SWAP",
    "SOL-USDT-SWAP",
    "DOGE-USDT-SWAP",
)
TIMEFRAMES = (
    ("5m", "5分钟"),
    ("15m", "15分钟"),
    ("1H", "1小时"),
    ("4H", "4小时"),
)
DIRECTIONS = (
    (STRATEGY_DYNAMIC_LONG_ID, "long_only", "做多"),
    (STRATEGY_DYNAMIC_SHORT_ID, "short_only", "做空"),
)
ENTRY_REFERENCE_EMAS = (21, 55)
ATR_STOP_MULTIPLIERS = (Decimal("1"), Decimal("1.5"), Decimal("2"))
MAX_ENTRIES_OPTIONS = (0, 1, 2, 3)
CANDLE_LIMIT = 10000
EMA_PERIOD = 21
TREND_EMA_PERIOD = 55
ATR_PERIOD = 10
RISK_AMOUNT = Decimal("100")
INITIAL_CAPITAL = Decimal("10000")
MAKER_FEE_RATE = Decimal("0.00015")
TAKER_FEE_RATE = Decimal("0.00036")
RULES = (
    (False, "break_even_off", "2R直锁1R+2倍Taker手续费"),
    (True, "break_even_on", "2R保本+2倍Taker手续费"),
)


@dataclass(frozen=True)
class RunRecord:
    rule_code: str
    rule_label: str
    dynamic_two_r_break_even: bool
    symbol: str
    bar: str
    bar_label: str
    direction: str
    entry_reference_ema: int
    max_entries_per_trend: int
    stop_atr: Decimal
    total_trades: int
    win_rate: Decimal
    total_pnl: Decimal
    total_return_pct: Decimal
    average_r_multiple: Decimal
    max_drawdown: Decimal
    max_drawdown_pct: Decimal
    profit_factor: Decimal | None
    profit_loss_ratio: Decimal | None
    ending_equity: Decimal
    total_fees: Decimal
    maker_fees: Decimal
    taker_fees: Decimal
    take_profit_hits: int
    stop_loss_hits: int
    data_source_note: str

    @property
    def param_label(self) -> str:
        return (
            f"EMA{self.entry_reference_ema}挂单 | SL x{format_decimal(self.stop_atr)} | "
            f"每波开仓{format_max_entries_label(self.max_entries_per_trend)} | "
            f"{'2R保本' if self.dynamic_two_r_break_even else '2R直锁1R'}"
        )

    @property
    def run_key(self) -> tuple[object, ...]:
        return (
            self.symbol,
            self.bar,
            self.direction,
            self.entry_reference_ema,
            self.max_entries_per_trend,
            self.stop_atr,
        )

    @property
    def best_key(self) -> tuple[str, str, str]:
        return (self.symbol, self.bar, self.direction)


@dataclass(frozen=True)
class ExportBundle:
    all_runs_csv: Path
    off_best_csv: Path
    on_best_csv: Path
    all_compare_csv: Path
    best_compare_csv: Path
    symbol_summary_csv: Path
    timeframe_summary_csv: Path
    report_md: Path
    summary_md: Path
    summary_json: Path


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


def format_max_entries_label(value: int) -> str:
    return "不限(0)" if value <= 0 else str(value)


def average_decimal(values: Iterable[Decimal]) -> Decimal | None:
    items = list(values)
    if not items:
        return None
    return sum(items, Decimal("0")) / Decimal(str(len(items)))


def timeframe_order(bar: str) -> int:
    order = {"5m": 0, "15m": 1, "1H": 2, "4H": 3}
    return order.get(bar, 99)


def max_entries_order(value: int) -> int:
    return 999 if value <= 0 else value


def sort_record(record: RunRecord) -> tuple[object, ...]:
    direction_order = {"做多": 0, "做空": 1}
    rule_order = {"break_even_off": 0, "break_even_on": 1}
    return (
        rule_order.get(record.rule_code, 99),
        record.symbol,
        timeframe_order(record.bar),
        direction_order.get(record.direction, 99),
        record.entry_reference_ema,
        record.stop_atr,
        max_entries_order(record.max_entries_per_trend),
    )


def build_base_config(
    *,
    symbol: str,
    bar: str,
    strategy_id: str,
    signal_mode: str,
    entry_reference_ema_period: int,
    stop_atr: Decimal,
    max_entries_per_trend: int,
    dynamic_two_r_break_even: bool,
) -> StrategyConfig:
    return StrategyConfig(
        inst_id=symbol,
        bar=bar,
        ema_period=EMA_PERIOD,
        trend_ema_period=TREND_EMA_PERIOD,
        big_ema_period=233,
        atr_period=ATR_PERIOD,
        atr_stop_multiplier=stop_atr,
        atr_take_multiplier=stop_atr * Decimal("2"),
        order_size=Decimal("0"),
        trade_mode="cross",
        signal_mode=signal_mode,
        position_mode="net",
        environment="demo",
        tp_sl_trigger_type="mark",
        strategy_id=strategy_id,
        risk_amount=RISK_AMOUNT,
        backtest_initial_capital=INITIAL_CAPITAL,
        backtest_sizing_mode="fixed_risk",
        take_profit_mode="dynamic",
        max_entries_per_trend=max_entries_per_trend,
        entry_reference_ema_period=entry_reference_ema_period,
        dynamic_two_r_break_even=dynamic_two_r_break_even,
    )


def load_market_data(client: OkxRestClient) -> dict[tuple[str, str], tuple[Instrument, list, str]]:
    market_cache: dict[tuple[str, str], tuple[Instrument, list, str]] = {}
    total = len(SYMBOLS) * len(TIMEFRAMES)
    sequence = 0
    for symbol in SYMBOLS:
        instrument = client.get_instrument(symbol)
        for bar, bar_label in TIMEFRAMES:
            sequence += 1
            print(f"[{sequence}/{total}] load {symbol} {bar_label} {CANDLE_LIMIT} candles")
            candles = _load_backtest_candles(client, symbol, bar, CANDLE_LIMIT)
            note = _build_backtest_data_source_note(client)
            market_cache[(symbol, bar)] = (instrument, candles, note)
    return market_cache
def run_rule_suite(
    market_cache: dict[tuple[str, str], tuple[Instrument, list, str]],
    *,
    dynamic_two_r_break_even: bool,
    rule_code: str,
    rule_label: str,
) -> tuple[list[RunRecord], list[str]]:
    rows: list[RunRecord] = []
    failures: list[str] = []
    total = (
        len(SYMBOLS)
        * len(TIMEFRAMES)
        * len(DIRECTIONS)
        * len(ENTRY_REFERENCE_EMAS)
        * len(ATR_STOP_MULTIPLIERS)
        * len(MAX_ENTRIES_OPTIONS)
    )
    sequence = 0
    for symbol in SYMBOLS:
        for bar, bar_label in TIMEFRAMES:
            instrument, candles, note = market_cache[(symbol, bar)]
            for strategy_id, signal_mode, direction_label in DIRECTIONS:
                for entry_ema in ENTRY_REFERENCE_EMAS:
                    for stop_atr in ATR_STOP_MULTIPLIERS:
                        for max_entries in MAX_ENTRIES_OPTIONS:
                            sequence += 1
                            print(
                                f"[{sequence}/{total}] {rule_code} {symbol} {bar_label} {direction_label} "
                                f"EMA{entry_ema} SLx{format_decimal(stop_atr)} 每波{format_max_entries_label(max_entries)}"
                            )
                            config = build_base_config(
                                symbol=symbol,
                                bar=bar,
                                strategy_id=strategy_id,
                                signal_mode=signal_mode,
                                entry_reference_ema_period=entry_ema,
                                stop_atr=stop_atr,
                                max_entries_per_trend=max_entries,
                                dynamic_two_r_break_even=dynamic_two_r_break_even,
                            )
                            try:
                                result = _run_backtest_with_loaded_data(
                                    candles,
                                    instrument,
                                    config,
                                    data_source_note=note,
                                    maker_fee_rate=MAKER_FEE_RATE,
                                    taker_fee_rate=TAKER_FEE_RATE,
                                )
                            except Exception as exc:
                                failures.append(
                                    f"{rule_label} | {symbol} | {bar} | {direction_label} | EMA{entry_ema} | "
                                    f"SLx{format_decimal(stop_atr)} | 每波{format_max_entries_label(max_entries)} -> {exc}"
                                )
                                continue
                            rows.append(
                                RunRecord(
                                    rule_code=rule_code,
                                    rule_label=rule_label,
                                    dynamic_two_r_break_even=dynamic_two_r_break_even,
                                    symbol=symbol,
                                    bar=bar,
                                    bar_label=bar_label,
                                    direction=direction_label,
                                    entry_reference_ema=entry_ema,
                                    max_entries_per_trend=max_entries,
                                    stop_atr=stop_atr,
                                    total_trades=result.report.total_trades,
                                    win_rate=result.report.win_rate,
                                    total_pnl=result.report.total_pnl,
                                    total_return_pct=result.report.total_return_pct,
                                    average_r_multiple=result.report.average_r_multiple,
                                    max_drawdown=result.report.max_drawdown,
                                    max_drawdown_pct=result.report.max_drawdown_pct,
                                    profit_factor=result.report.profit_factor,
                                    profit_loss_ratio=result.report.profit_loss_ratio,
                                    ending_equity=result.report.ending_equity,
                                    total_fees=result.report.total_fees,
                                    maker_fees=result.report.maker_fees,
                                    taker_fees=result.report.taker_fees,
                                    take_profit_hits=result.report.take_profit_hits,
                                    stop_loss_hits=result.report.stop_loss_hits,
                                    data_source_note=note,
                                )
                            )
    return rows, failures


def pick_best_records(records: Iterable[RunRecord]) -> list[RunRecord]:
    best: dict[tuple[str, str, str], RunRecord] = {}
    for record in records:
        current = best.get(record.best_key)
        if current is None or record.total_pnl > current.total_pnl:
            best[record.best_key] = record
    return sorted(best.values(), key=sort_record)


def compare_records(current_records: Iterable[RunRecord], baseline_records: Iterable[RunRecord], *, best_level: bool) -> list[dict[str, object]]:
    key_name = "best_key" if best_level else "run_key"
    current_map = {getattr(record, key_name): record for record in current_records}
    baseline_map = {getattr(record, key_name): record for record in baseline_records}
    rows: list[dict[str, object]] = []
    for key in sorted(current_map.keys() | baseline_map.keys()):
        current = current_map.get(key)
        baseline = baseline_map.get(key)
        if current is None or baseline is None:
            continue
        pnl_delta = current.total_pnl - baseline.total_pnl
        drawdown_delta = current.max_drawdown - baseline.max_drawdown
        win_rate_delta = current.win_rate - baseline.win_rate
        avg_r_delta = current.average_r_multiple - baseline.average_r_multiple
        if pnl_delta > 0:
            verdict = "improved"
        elif pnl_delta < 0:
            verdict = "worse"
        else:
            verdict = "flat"
        rows.append(
            {
                "symbol": current.symbol,
                "bar": current.bar,
                "bar_label": current.bar_label,
                "direction": current.direction,
                "baseline_rule_label": baseline.rule_label,
                "current_rule_label": current.rule_label,
                "baseline_param": baseline.param_label,
                "current_param": current.param_label,
                "baseline_total_pnl": baseline.total_pnl,
                "current_total_pnl": current.total_pnl,
                "pnl_delta": pnl_delta,
                "baseline_max_drawdown": baseline.max_drawdown,
                "current_max_drawdown": current.max_drawdown,
                "drawdown_delta": drawdown_delta,
                "baseline_win_rate": baseline.win_rate,
                "current_win_rate": current.win_rate,
                "win_rate_delta": win_rate_delta,
                "baseline_average_r_multiple": baseline.average_r_multiple,
                "current_average_r_multiple": current.average_r_multiple,
                "average_r_delta": avg_r_delta,
                "baseline_total_trades": baseline.total_trades,
                "current_total_trades": current.total_trades,
                "trades_delta": current.total_trades - baseline.total_trades,
                "verdict": verdict,
            }
        )
    return rows


def summarize_records(records: Iterable[RunRecord]) -> dict[str, object]:
    items = list(records)
    profitable = [item for item in items if item.total_pnl > 0]
    return {
        "runs": len(items),
        "profitable_runs": len(profitable),
        "avg_total_pnl": average_decimal(item.total_pnl for item in items) or Decimal("0"),
        "avg_max_drawdown": average_decimal(item.max_drawdown for item in items) or Decimal("0"),
        "avg_win_rate": average_decimal(item.win_rate for item in items) or Decimal("0"),
    }


def summarize_comparisons(rows: list[dict[str, object]]) -> dict[str, object]:
    improved = [row for row in rows if row["verdict"] == "improved"]
    worse = [row for row in rows if row["verdict"] == "worse"]
    flat = [row for row in rows if row["verdict"] == "flat"]
    return {
        "count": len(rows),
        "improved": len(improved),
        "worse": len(worse),
        "flat": len(flat),
        "avg_pnl_delta": average_decimal(Decimal(row["pnl_delta"]) for row in rows) or Decimal("0"),
        "avg_drawdown_delta": average_decimal(Decimal(row["drawdown_delta"]) for row in rows) or Decimal("0"),
        "avg_win_rate_delta": average_decimal(Decimal(row["win_rate_delta"]) for row in rows) or Decimal("0"),
    }


def group_rows(rows: Iterable[dict[str, object]], group_key: str) -> list[dict[str, object]]:
    grouped: dict[str, list[dict[str, object]]] = {}
    for row in rows:
        grouped.setdefault(str(row[group_key]), []).append(row)
    ordered_keys = sorted(grouped.keys(), key=timeframe_order if group_key == "bar" else None)
    summary_rows: list[dict[str, object]] = []
    for key in ordered_keys:
        scoped = grouped[key]
        summary_rows.append(
            {
                group_key: key,
                "count": len(scoped),
                "improved": sum(1 for row in scoped if row["verdict"] == "improved"),
                "worse": sum(1 for row in scoped if row["verdict"] == "worse"),
                "flat": sum(1 for row in scoped if row["verdict"] == "flat"),
                "avg_pnl_delta": average_decimal(Decimal(row["pnl_delta"]) for row in scoped) or Decimal("0"),
                "avg_drawdown_delta": average_decimal(Decimal(row["drawdown_delta"]) for row in scoped) or Decimal("0"),
            }
        )
    return summary_rows


def markdown_table(headers: list[str], rows: list[list[str]]) -> str:
    if not rows:
        return "| - |\n| --- |\n| 无数据 |"
    lines = ["| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |"]
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    return "\n".join(lines)


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
def run_rows_to_csv(records: Iterable[RunRecord]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for record in records:
        payload = asdict(record)
        payload["stop_atr"] = format_decimal(record.stop_atr)
        payload["win_rate"] = format_decimal_fixed(record.win_rate, 2)
        payload["total_pnl"] = format_decimal_fixed(record.total_pnl, 4)
        payload["total_return_pct"] = format_decimal_fixed(record.total_return_pct, 2)
        payload["average_r_multiple"] = format_decimal_fixed(record.average_r_multiple, 4)
        payload["max_drawdown"] = format_decimal_fixed(record.max_drawdown, 4)
        payload["max_drawdown_pct"] = format_decimal_fixed(record.max_drawdown_pct, 2)
        payload["profit_factor"] = format_decimal_fixed(record.profit_factor, 4) if record.profit_factor is not None else "-"
        payload["profit_loss_ratio"] = format_decimal_fixed(record.profit_loss_ratio, 4) if record.profit_loss_ratio is not None else "-"
        payload["ending_equity"] = format_decimal_fixed(record.ending_equity, 2)
        payload["total_fees"] = format_decimal_fixed(record.total_fees, 4)
        payload["maker_fees"] = format_decimal_fixed(record.maker_fees, 4)
        payload["taker_fees"] = format_decimal_fixed(record.taker_fees, 4)
        payload["max_entries_label"] = format_max_entries_label(record.max_entries_per_trend)
        payload["param_label"] = record.param_label
        rows.append(payload)
    return rows


def comparison_rows_to_csv(rows: Iterable[dict[str, object]]) -> list[dict[str, object]]:
    result: list[dict[str, object]] = []
    for row in rows:
        result.append(
            {
                "symbol": row["symbol"],
                "bar": row["bar"],
                "bar_label": row["bar_label"],
                "direction": row["direction"],
                "baseline_rule_label": row["baseline_rule_label"],
                "current_rule_label": row["current_rule_label"],
                "baseline_param": row["baseline_param"],
                "current_param": row["current_param"],
                "baseline_total_pnl": format_decimal_fixed(Decimal(row["baseline_total_pnl"]), 4),
                "current_total_pnl": format_decimal_fixed(Decimal(row["current_total_pnl"]), 4),
                "pnl_delta": format_decimal_fixed(Decimal(row["pnl_delta"]), 4),
                "baseline_max_drawdown": format_decimal_fixed(Decimal(row["baseline_max_drawdown"]), 4),
                "current_max_drawdown": format_decimal_fixed(Decimal(row["current_max_drawdown"]), 4),
                "drawdown_delta": format_decimal_fixed(Decimal(row["drawdown_delta"]), 4),
                "baseline_win_rate": format_decimal_fixed(Decimal(row["baseline_win_rate"]), 2),
                "current_win_rate": format_decimal_fixed(Decimal(row["current_win_rate"]), 2),
                "win_rate_delta": format_decimal_fixed(Decimal(row["win_rate_delta"]), 2),
                "baseline_average_r_multiple": format_decimal_fixed(Decimal(row["baseline_average_r_multiple"]), 4),
                "current_average_r_multiple": format_decimal_fixed(Decimal(row["current_average_r_multiple"]), 4),
                "average_r_delta": format_decimal_fixed(Decimal(row["average_r_delta"]), 4),
                "baseline_total_trades": row["baseline_total_trades"],
                "current_total_trades": row["current_total_trades"],
                "trades_delta": row["trades_delta"],
                "verdict": row["verdict"],
            }
        )
    return result


def summary_rows_to_csv(rows: Iterable[dict[str, object]], group_key: str) -> list[dict[str, object]]:
    output: list[dict[str, object]] = []
    for row in rows:
        output.append(
            {
                group_key: row[group_key],
                "count": row["count"],
                "improved": row["improved"],
                "worse": row["worse"],
                "flat": row["flat"],
                "avg_pnl_delta": format_decimal_fixed(Decimal(row["avg_pnl_delta"]), 4),
                "avg_drawdown_delta": format_decimal_fixed(Decimal(row["avg_drawdown_delta"]), 4),
            }
        )
    return output


def top_changed_rows(rows: list[dict[str, object]], *, reverse: bool, limit: int = 12) -> list[dict[str, object]]:
    return sorted(rows, key=lambda row: Decimal(row["pnl_delta"]), reverse=reverse)[:limit]


def build_exports(stamp: str) -> ExportBundle:
    return ExportBundle(
        all_runs_csv=REPORTS_DIR / f"dynamic_2r_switch_all_runs_{stamp}.csv",
        off_best_csv=REPORTS_DIR / f"dynamic_2r_switch_off_best_{stamp}.csv",
        on_best_csv=REPORTS_DIR / f"dynamic_2r_switch_on_best_{stamp}.csv",
        all_compare_csv=REPORTS_DIR / f"dynamic_2r_switch_compare_all_runs_{stamp}.csv",
        best_compare_csv=REPORTS_DIR / f"dynamic_2r_switch_compare_best_{stamp}.csv",
        symbol_summary_csv=REPORTS_DIR / f"dynamic_2r_switch_symbol_summary_{stamp}.csv",
        timeframe_summary_csv=REPORTS_DIR / f"dynamic_2r_switch_timeframe_summary_{stamp}.csv",
        report_md=REPORTS_DIR / f"dynamic_2r_switch_report_{stamp}.md",
        summary_md=REPORTS_DIR / f"dynamic_2r_switch_summary_{stamp}.md",
        summary_json=REPORTS_DIR / f"dynamic_2r_switch_summary_{stamp}.json",
    )


def build_report(
    *,
    off_records: list[RunRecord],
    on_records: list[RunRecord],
    all_run_comparisons: list[dict[str, object]],
    best_comparisons: list[dict[str, object]],
    failures: list[str],
    exports: ExportBundle,
) -> tuple[str, str, dict[str, object]]:
    off_summary = summarize_records(off_records)
    on_summary = summarize_records(on_records)
    all_run_summary = summarize_comparisons(all_run_comparisons)
    best_summary = summarize_comparisons(best_comparisons)
    timeframe_rows = group_rows(best_comparisons, "bar")
    symbol_rows = group_rows(best_comparisons, "symbol")
    top_improved = top_changed_rows(best_comparisons, reverse=True)
    top_worse = top_changed_rows(best_comparisons, reverse=False)

    detailed_lines = [
        "# 动态止盈 2R保本开关对比报告",
        "",
        "## 一、测试口径",
        "",
        f"- 对比对象：{RULES[0][2]} vs {RULES[1][2]}",
        "- 范围：BTC、ETH、BNB、SOL、DOGE",
        "- 周期：5分钟、15分钟、1小时、4小时",
        "- 方向：做多、做空",
        "- 每组 K 线：10000 根",
        "- 参数网格：挂单EMA 21/55、止损 ATR 1/1.5/2、每波开仓次数 0/1/2/3",
        "- 统一手续费：Maker 0.015%、Taker 0.036%",
        "- 下方所有 delta 均按 `2R保本开启 - 2R保本关闭` 计算",
        "",
        "## 二、总体结论",
        "",
        f"- 2R保本关闭（2R直锁1R）共 {off_summary['runs']} 组，平均总盈亏 {format_decimal_fixed(off_summary['avg_total_pnl'], 4)}，平均最大回撤 {format_decimal_fixed(off_summary['avg_max_drawdown'], 4)}。",
        f"- 2R保本开启（2R先保本）共 {on_summary['runs']} 组，平均总盈亏 {format_decimal_fixed(on_summary['avg_total_pnl'], 4)}，平均最大回撤 {format_decimal_fixed(on_summary['avg_max_drawdown'], 4)}。",
        f"- 全量参数逐组对比：改善 {all_run_summary['improved']} 组，变差 {all_run_summary['worse']} 组，持平 {all_run_summary['flat']} 组，平均盈亏变化 {format_decimal_fixed(all_run_summary['avg_pnl_delta'], 4)}。",
        f"- 每个币种/周期/方向只取最佳参数后对比：改善 {best_summary['improved']} 组，变差 {best_summary['worse']} 组，持平 {best_summary['flat']} 组，平均盈亏变化 {format_decimal_fixed(best_summary['avg_pnl_delta'], 4)}，平均回撤变化 {format_decimal_fixed(best_summary['avg_drawdown_delta'], 4)}。",
        "",
        "## 三、分周期结论（最佳参数层）",
        "",
        markdown_table(
            ["周期", "组合数", "改善", "变差", "持平", "平均盈亏变化", "平均回撤变化"],
            [
                [
                    str(row["bar"]),
                    str(row["count"]),
                    str(row["improved"]),
                    str(row["worse"]),
                    str(row["flat"]),
                    format_decimal_fixed(Decimal(row["avg_pnl_delta"]), 4),
                    format_decimal_fixed(Decimal(row["avg_drawdown_delta"]), 4),
                ]
                for row in timeframe_rows
            ],
        ),
        "",
        "## 四、分币种结论（最佳参数层）",
        "",
        markdown_table(
            ["币种", "组合数", "改善", "变差", "持平", "平均盈亏变化", "平均回撤变化"],
            [
                [
                    str(row["symbol"]),
                    str(row["count"]),
                    str(row["improved"]),
                    str(row["worse"]),
                    str(row["flat"]),
                    format_decimal_fixed(Decimal(row["avg_pnl_delta"]), 4),
                    format_decimal_fixed(Decimal(row["avg_drawdown_delta"]), 4),
                ]
                for row in symbol_rows
            ],
        ),
        "",
        "## 五、最佳参数层逐币逐周期对比",
        "",
        markdown_table(
            ["币种", "周期", "方向", "关闭时最佳参数", "开启时最佳参数", "盈亏变化", "回撤变化"],
            [
                [
                    str(row["symbol"]),
                    str(row["bar_label"]),
                    str(row["direction"]),
                    str(row["baseline_param"]),
                    str(row["current_param"]),
                    format_decimal_fixed(Decimal(row["pnl_delta"]), 4),
                    format_decimal_fixed(Decimal(row["drawdown_delta"]), 4),
                ]
                for row in best_comparisons
            ],
        ),
        "",
        "## 六、改善最明显的组合（最佳参数层）",
        "",
        markdown_table(
            ["币种", "周期", "方向", "开启时参数", "盈亏变化", "回撤变化"],
            [
                [
                    str(row["symbol"]),
                    str(row["bar_label"]),
                    str(row["direction"]),
                    str(row["current_param"]),
                    format_decimal_fixed(Decimal(row["pnl_delta"]), 4),
                    format_decimal_fixed(Decimal(row["drawdown_delta"]), 4),
                ]
                for row in top_improved
            ],
        ),
        "",
        "## 七、退步最明显的组合（最佳参数层）",
        "",
        markdown_table(
            ["币种", "周期", "方向", "开启时参数", "盈亏变化", "回撤变化"],
            [
                [
                    str(row["symbol"]),
                    str(row["bar_label"]),
                    str(row["direction"]),
                    str(row["current_param"]),
                    format_decimal_fixed(Decimal(row["pnl_delta"]), 4),
                    format_decimal_fixed(Decimal(row["drawdown_delta"]), 4),
                ]
                for row in top_worse
            ],
        ),
        "",
        "## 八、我的判断",
        "",
        f"- 如果你更在意吃趋势，核心看最佳参数层平均盈亏变化 `{format_decimal_fixed(best_summary['avg_pnl_delta'], 4)}`；如果它为负，说明 `2R先保本` 整体不适合替代 `2R直锁1R`。",
        f"- 如果你更在意控制回撤，重点看最佳参数层平均回撤变化 `{format_decimal_fixed(best_summary['avg_drawdown_delta'], 4)}`；负值说明开启 2R保本 更稳。",
        "- 真正值得实盘关注的是：同时满足 `盈亏改善` 且 `回撤没有明显恶化` 的组合，而不是只看胜率变化。",
        "",
        "## 九、附件",
        "",
        f"- 全量结果：`{exports.all_runs_csv}`",
        f"- 关闭 2R保本 最佳结果：`{exports.off_best_csv}`",
        f"- 开启 2R保本 最佳结果：`{exports.on_best_csv}`",
        f"- 全量逐组对比：`{exports.all_compare_csv}`",
        f"- 最佳参数层对比：`{exports.best_compare_csv}`",
        f"- 币种汇总：`{exports.symbol_summary_csv}`",
        f"- 周期汇总：`{exports.timeframe_summary_csv}`",
        f"- 失败组合数：{len(failures)}",
    ]
    if failures:
        detailed_lines.extend(["", "失败详情："])
        detailed_lines.extend(f"- {item}" for item in failures)

    summary_lines = [
        "# 动态止盈 2R保本开关简明总结",
        "",
        f"- 对比对象：{RULES[0][2]} vs {RULES[1][2]}",
        f"- 本次共跑 {off_summary['runs'] + on_summary['runs']} 组（每个模式 {off_summary['runs']} 组）。",
        f"- 全量逐组对比：改善 {all_run_summary['improved']} 组，变差 {all_run_summary['worse']} 组，持平 {all_run_summary['flat']} 组。",
        f"- 最佳参数层对比：改善 {best_summary['improved']} 组，变差 {best_summary['worse']} 组，持平 {best_summary['flat']} 组。",
        f"- 2R保本关闭 平均总盈亏：{format_decimal_fixed(off_summary['avg_total_pnl'], 4)}；2R保本开启 平均总盈亏：{format_decimal_fixed(on_summary['avg_total_pnl'], 4)}。",
        f"- 最佳参数层平均盈亏变化：{format_decimal_fixed(best_summary['avg_pnl_delta'], 4)}；平均回撤变化：{format_decimal_fixed(best_summary['avg_drawdown_delta'], 4)}。",
        "- 实盘决策优先看最佳参数层，因为它更接近我们真实选策略时的口径。",
    ]

    summary_payload = {
        "baseline_rule": RULES[0][2],
        "current_rule": RULES[1][2],
        "off_summary": {
            "runs": off_summary["runs"],
            "profitable_runs": off_summary["profitable_runs"],
            "avg_total_pnl": format_decimal_fixed(off_summary["avg_total_pnl"], 4),
            "avg_max_drawdown": format_decimal_fixed(off_summary["avg_max_drawdown"], 4),
            "avg_win_rate": format_decimal_fixed(off_summary["avg_win_rate"], 4),
        },
        "on_summary": {
            "runs": on_summary["runs"],
            "profitable_runs": on_summary["profitable_runs"],
            "avg_total_pnl": format_decimal_fixed(on_summary["avg_total_pnl"], 4),
            "avg_max_drawdown": format_decimal_fixed(on_summary["avg_max_drawdown"], 4),
            "avg_win_rate": format_decimal_fixed(on_summary["avg_win_rate"], 4),
        },
        "all_run_comparison": {
            "count": all_run_summary["count"],
            "improved": all_run_summary["improved"],
            "worse": all_run_summary["worse"],
            "flat": all_run_summary["flat"],
            "avg_pnl_delta": format_decimal_fixed(all_run_summary["avg_pnl_delta"], 4),
            "avg_drawdown_delta": format_decimal_fixed(all_run_summary["avg_drawdown_delta"], 4),
            "avg_win_rate_delta": format_decimal_fixed(all_run_summary["avg_win_rate_delta"], 4),
        },
        "best_comparison": {
            "count": best_summary["count"],
            "improved": best_summary["improved"],
            "worse": best_summary["worse"],
            "flat": best_summary["flat"],
            "avg_pnl_delta": format_decimal_fixed(best_summary["avg_pnl_delta"], 4),
            "avg_drawdown_delta": format_decimal_fixed(best_summary["avg_drawdown_delta"], 4),
            "avg_win_rate_delta": format_decimal_fixed(best_summary["avg_win_rate_delta"], 4),
        },
        "failures": failures,
    }
    return "\n".join(detailed_lines), "\n".join(summary_lines), summary_payload


def main() -> None:
    client = OkxRestClient()
    market_cache = load_market_data(client)

    off_records, off_failures = run_rule_suite(
        market_cache,
        dynamic_two_r_break_even=False,
        rule_code=RULES[0][1],
        rule_label=RULES[0][2],
    )
    on_records, on_failures = run_rule_suite(
        market_cache,
        dynamic_two_r_break_even=True,
        rule_code=RULES[1][1],
        rule_label=RULES[1][2],
    )

    failures = off_failures + on_failures
    best_off = pick_best_records(off_records)
    best_on = pick_best_records(on_records)
    all_run_comparisons = compare_records(on_records, off_records, best_level=False)
    best_comparisons = compare_records(best_on, best_off, best_level=True)
    symbol_rows = group_rows(best_comparisons, "symbol")
    timeframe_rows = group_rows(best_comparisons, "bar")

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    exports = build_exports(stamp)

    write_csv(exports.all_runs_csv, run_rows_to_csv(sorted(off_records + on_records, key=sort_record)))
    write_csv(exports.off_best_csv, run_rows_to_csv(best_off))
    write_csv(exports.on_best_csv, run_rows_to_csv(best_on))
    write_csv(exports.all_compare_csv, comparison_rows_to_csv(all_run_comparisons))
    write_csv(exports.best_compare_csv, comparison_rows_to_csv(best_comparisons))
    write_csv(exports.symbol_summary_csv, summary_rows_to_csv(symbol_rows, "symbol"))
    write_csv(exports.timeframe_summary_csv, summary_rows_to_csv(timeframe_rows, "bar"))

    detailed_report, summary_report, summary_payload = build_report(
        off_records=off_records,
        on_records=on_records,
        all_run_comparisons=all_run_comparisons,
        best_comparisons=best_comparisons,
        failures=failures,
        exports=exports,
    )
    exports.report_md.write_text(detailed_report, encoding="utf-8")
    exports.summary_md.write_text(summary_report, encoding="utf-8")
    exports.summary_json.write_text(json.dumps(summary_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"all runs -> {exports.all_runs_csv}")
    print(f"off best -> {exports.off_best_csv}")
    print(f"on best -> {exports.on_best_csv}")
    print(f"all comparison -> {exports.all_compare_csv}")
    print(f"best comparison -> {exports.best_compare_csv}")
    print(f"symbol summary -> {exports.symbol_summary_csv}")
    print(f"timeframe summary -> {exports.timeframe_summary_csv}")
    print(f"report -> {exports.report_md}")
    print(f"summary -> {exports.summary_md}")
    print(f"summary json -> {exports.summary_json}")


if __name__ == "__main__":
    main()
