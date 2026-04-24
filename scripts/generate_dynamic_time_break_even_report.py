from __future__ import annotations

import csv
import json
import sys
from dataclasses import asdict, dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Callable, Iterable

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from okx_quant.backtest import _build_backtest_data_source_note, _load_backtest_candles, _run_backtest_with_loaded_data
from okx_quant.models import Instrument, StrategyConfig
from okx_quant.okx_client import OkxRestClient
from okx_quant.persistence import analysis_report_dir_path
from okx_quant.strategy_catalog import STRATEGY_DYNAMIC_LONG_ID, STRATEGY_DYNAMIC_SHORT_ID

REPORTS_DIR = analysis_report_dir_path()
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
TIME_STOP_BREAK_EVEN_OPTIONS = (0, 5, 10, 15, 20)
CANDLE_LIMIT = 10000
EMA_PERIOD = 21
TREND_EMA_PERIOD = 55
ATR_PERIOD = 10
RISK_AMOUNT = Decimal("100")
INITIAL_CAPITAL = Decimal("10000")
MAKER_FEE_RATE = Decimal("0.00015")
TAKER_FEE_RATE = Decimal("0.00036")
ENTRY_SLIPPAGE_RATE = Decimal("0")
EXIT_SLIPPAGE_RATE = Decimal("0.0003")
DYNAMIC_TWO_R_BREAK_EVEN = True
DYNAMIC_FEE_OFFSET_ENABLED = True
RECOMMENDED_TIME_STOP_TIE_BREAK_ORDER = (10, 5, 15, 0, 20)


@dataclass(frozen=True)
class RunRecord:
    symbol: str
    bar: str
    bar_label: str
    direction: str
    entry_reference_ema: int
    max_entries_per_trend: int
    time_stop_break_even_bars: int
    time_stop_break_even_enabled: bool
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
            f"动态止盈 | 每波开仓{format_max_entries_label(self.max_entries_per_trend)} | "
            f"时间保本{format_time_stop_label(self.time_stop_break_even_bars)}"
        )

    @property
    def group_key(self) -> tuple[str, str, str]:
        return (self.symbol, self.bar, self.direction)

    @property
    def group_key_with_entries(self) -> tuple[str, str, str, int]:
        return (self.symbol, self.bar, self.direction, self.max_entries_per_trend)

    @property
    def group_key_with_time_stop(self) -> tuple[str, str, str, int]:
        return (self.symbol, self.bar, self.direction, self.time_stop_break_even_bars)


@dataclass(frozen=True)
class ExportBundle:
    all_runs_csv: Path
    overall_best_csv: Path
    best_by_entries_csv: Path
    best_by_time_stop_csv: Path
    max_entries_summary_csv: Path
    time_stop_summary_csv: Path
    time_stop_by_timeframe_csv: Path
    time_stop_by_symbol_csv: Path
    best_entries_distribution_csv: Path
    best_time_stop_distribution_csv: Path
    failures_txt: Path
    report_txt: Path
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


def format_rate_pct(value: Decimal) -> str:
    return format_decimal_fixed(value * Decimal("100"), 4)


def format_max_entries_label(value: int) -> str:
    return "不限(0)" if value <= 0 else str(value)


def format_time_stop_label(value: int) -> str:
    return "关闭(0)" if value <= 0 else f"{value}根"


def average_decimal(values: Iterable[Decimal]) -> Decimal | None:
    sequence = list(values)
    if not sequence:
        return None
    return sum(sequence, Decimal("0")) / Decimal(str(len(sequence)))


def build_base_config(
    *,
    symbol: str,
    bar: str,
    strategy_id: str,
    signal_mode: str,
    entry_reference_ema_period: int,
    stop_atr: Decimal,
    max_entries_per_trend: int,
    time_stop_break_even_bars: int,
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
        backtest_entry_slippage_rate=ENTRY_SLIPPAGE_RATE,
        backtest_exit_slippage_rate=EXIT_SLIPPAGE_RATE,
        backtest_slippage_rate=EXIT_SLIPPAGE_RATE,
        dynamic_two_r_break_even=DYNAMIC_TWO_R_BREAK_EVEN,
        dynamic_fee_offset_enabled=DYNAMIC_FEE_OFFSET_ENABLED,
        time_stop_break_even_enabled=time_stop_break_even_bars > 0,
        time_stop_break_even_bars=time_stop_break_even_bars,
    )


def load_market_data(client: OkxRestClient) -> dict[tuple[str, str], tuple[Instrument, list, str]]:
    market_cache: dict[tuple[str, str], tuple[Instrument, list, str]] = {}
    total = len(SYMBOLS) * len(TIMEFRAMES)
    sequence = 0
    for symbol in SYMBOLS:
        instrument = client.get_instrument(symbol)
        for bar, bar_name in TIMEFRAMES:
            sequence += 1
            print(f"[{sequence}/{total}] load {symbol} {bar_name} {CANDLE_LIMIT} candles")
            candles = _load_backtest_candles(client, symbol, bar, CANDLE_LIMIT)
            note = _build_backtest_data_source_note(client)
            market_cache[(symbol, bar)] = (instrument, candles, note)
    return market_cache


def run_dynamic_suite(
    market_cache: dict[tuple[str, str], tuple[Instrument, list, str]],
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
        * len(TIME_STOP_BREAK_EVEN_OPTIONS)
    )
    sequence = 0
    for symbol in SYMBOLS:
        for bar, bar_name in TIMEFRAMES:
            instrument, candles, note = market_cache[(symbol, bar)]
            for strategy_id, signal_mode, direction_name in DIRECTIONS:
                for entry_ema in ENTRY_REFERENCE_EMAS:
                    for stop_atr in ATR_STOP_MULTIPLIERS:
                        for max_entries in MAX_ENTRIES_OPTIONS:
                            for time_stop_bars in TIME_STOP_BREAK_EVEN_OPTIONS:
                                sequence += 1
                                print(
                                    f"[{sequence}/{total}] dynamic {symbol} {bar_name} {direction_name} "
                                    f"EMA{entry_ema} SLx{format_decimal(stop_atr)} "
                                    f"每波{format_max_entries_label(max_entries)} "
                                    f"时间保本{format_time_stop_label(time_stop_bars)}"
                                )
                                config = build_base_config(
                                    symbol=symbol,
                                    bar=bar,
                                    strategy_id=strategy_id,
                                    signal_mode=signal_mode,
                                    entry_reference_ema_period=entry_ema,
                                    stop_atr=stop_atr,
                                    max_entries_per_trend=max_entries,
                                    time_stop_break_even_bars=time_stop_bars,
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
                                        f"{symbol} | {bar} | {direction_name} | EMA{entry_ema} | "
                                        f"SLx{format_decimal(stop_atr)} | 每波{format_max_entries_label(max_entries)} | "
                                        f"时间保本{format_time_stop_label(time_stop_bars)} -> {exc}"
                                    )
                                    continue
                                rows.append(
                                    RunRecord(
                                        symbol=symbol,
                                        bar=bar,
                                        bar_label=bar_name,
                                        direction=direction_name,
                                        entry_reference_ema=entry_ema,
                                        max_entries_per_trend=config.max_entries_per_trend,
                                        time_stop_break_even_bars=config.resolved_time_stop_break_even_bars(),
                                        time_stop_break_even_enabled=bool(config.time_stop_break_even_enabled),
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


def pick_best_records(records: Iterable[RunRecord], key_fn: Callable[[RunRecord], tuple[object, ...]]) -> list[RunRecord]:
    best: dict[tuple[object, ...], RunRecord] = {}
    for record in records:
        key = key_fn(record)
        current = best.get(key)
        if current is None or record.total_pnl > current.total_pnl:
            best[key] = record
    return list(best.values())


def bar_label(bar: str) -> str:
    for value, label in TIMEFRAMES:
        if value == bar:
            return label
    return bar


def timeframe_order(bar: str) -> int:
    sequence = [item[0] for item in TIMEFRAMES]
    return sequence.index(bar) if bar in sequence else len(sequence)


def symbol_order(symbol: str) -> int:
    return SYMBOLS.index(symbol) if symbol in SYMBOLS else len(SYMBOLS)


def direction_order(direction: str) -> int:
    return 0 if direction == "做多" else 1


def max_entries_order(value: int) -> int:
    return MAX_ENTRIES_OPTIONS.index(value) if value in MAX_ENTRIES_OPTIONS else len(MAX_ENTRIES_OPTIONS)


def time_stop_order(value: int) -> int:
    return TIME_STOP_BREAK_EVEN_OPTIONS.index(value) if value in TIME_STOP_BREAK_EVEN_OPTIONS else len(TIME_STOP_BREAK_EVEN_OPTIONS)


def tie_break_order_for_time_stop(value: int) -> int:
    return RECOMMENDED_TIME_STOP_TIE_BREAK_ORDER.index(value) if value in RECOMMENDED_TIME_STOP_TIE_BREAK_ORDER else len(RECOMMENDED_TIME_STOP_TIE_BREAK_ORDER)


def sort_record(row: RunRecord) -> tuple[int, int, int, int, int, int, Decimal]:
    return (
        timeframe_order(row.bar),
        symbol_order(row.symbol),
        direction_order(row.direction),
        time_stop_order(row.time_stop_break_even_bars),
        max_entries_order(row.max_entries_per_trend),
        row.entry_reference_ema,
        row.stop_atr,
    )


def sort_record_with_entries_first(row: RunRecord) -> tuple[int, int, int, int, int, int, Decimal]:
    return (
        max_entries_order(row.max_entries_per_trend),
        timeframe_order(row.bar),
        symbol_order(row.symbol),
        direction_order(row.direction),
        time_stop_order(row.time_stop_break_even_bars),
        row.entry_reference_ema,
        row.stop_atr,
    )


def sort_record_with_time_stop_first(row: RunRecord) -> tuple[int, int, int, int, int, int, Decimal]:
    return (
        time_stop_order(row.time_stop_break_even_bars),
        timeframe_order(row.bar),
        symbol_order(row.symbol),
        direction_order(row.direction),
        max_entries_order(row.max_entries_per_trend),
        row.entry_reference_ema,
        row.stop_atr,
    )


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    if not rows:
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def csv_rows(records: list[RunRecord]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for record in records:
        payload = asdict(record)
        payload["two_r_break_even_enabled"] = "开启" if DYNAMIC_TWO_R_BREAK_EVEN else "关闭"
        payload["fee_offset_enabled"] = "开启" if DYNAMIC_FEE_OFFSET_ENABLED else "关闭"
        payload["entry_slippage_pct"] = format_rate_pct(ENTRY_SLIPPAGE_RATE)
        payload["exit_slippage_pct"] = format_rate_pct(EXIT_SLIPPAGE_RATE)
        payload["stop_atr"] = format_decimal(record.stop_atr)
        payload["max_entries_label"] = format_max_entries_label(record.max_entries_per_trend)
        payload["time_stop_break_even_label"] = format_time_stop_label(record.time_stop_break_even_bars)
        payload["time_stop_break_even_enabled"] = "开启" if record.time_stop_break_even_enabled else "关闭"
        payload["win_rate"] = format_decimal_fixed(record.win_rate, 2)
        payload["total_pnl"] = format_decimal_fixed(record.total_pnl, 4)
        payload["total_return_pct"] = format_decimal_fixed(record.total_return_pct, 2)
        payload["average_r_multiple"] = format_decimal_fixed(record.average_r_multiple, 4)
        payload["max_drawdown"] = format_decimal_fixed(record.max_drawdown, 4)
        payload["max_drawdown_pct"] = format_decimal_fixed(record.max_drawdown_pct, 2)
        payload["profit_factor"] = format_decimal_fixed(record.profit_factor, 4)
        payload["profit_loss_ratio"] = format_decimal_fixed(record.profit_loss_ratio, 4)
        payload["ending_equity"] = format_decimal_fixed(record.ending_equity, 2)
        payload["total_fees"] = format_decimal_fixed(record.total_fees, 4)
        payload["maker_fees"] = format_decimal_fixed(record.maker_fees, 4)
        payload["taker_fees"] = format_decimal_fixed(record.taker_fees, 4)
        payload["param_label"] = record.param_label
        rows.append(payload)
    return rows


def summarize_records(records: list[RunRecord]) -> dict[str, object]:
    profitable = [row for row in records if row.total_pnl > 0]
    return {
        "runs": len(records),
        "profitable_runs": len(profitable),
        "avg_total_pnl": average_decimal(row.total_pnl for row in records),
        "best_total_pnl": max((row.total_pnl for row in records), default=None),
        "worst_total_pnl": min((row.total_pnl for row in records), default=None),
        "avg_max_drawdown": average_decimal(row.max_drawdown for row in records),
        "avg_total_trades": average_decimal(Decimal(row.total_trades) for row in records),
        "avg_win_rate": average_decimal(row.win_rate for row in records),
    }


def build_max_entries_summary_rows(records: list[RunRecord]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for max_entries in MAX_ENTRIES_OPTIONS:
        scoped = [row for row in records if row.max_entries_per_trend == max_entries]
        summary = summarize_records(scoped)
        rows.append(
            {
                "max_entries_per_trend": max_entries,
                "max_entries_label": format_max_entries_label(max_entries),
                "runs": summary["runs"],
                "profitable_runs": summary["profitable_runs"],
                "avg_total_pnl": summary["avg_total_pnl"],
                "avg_max_drawdown": summary["avg_max_drawdown"],
                "avg_total_trades": summary["avg_total_trades"],
                "avg_win_rate": summary["avg_win_rate"],
                "best_total_pnl": summary["best_total_pnl"],
                "worst_total_pnl": summary["worst_total_pnl"],
            }
        )
    return rows


def build_time_stop_summary_rows(records: list[RunRecord]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for time_stop_bars in TIME_STOP_BREAK_EVEN_OPTIONS:
        scoped = [row for row in records if row.time_stop_break_even_bars == time_stop_bars]
        summary = summarize_records(scoped)
        rows.append(
            {
                "time_stop_break_even_bars": time_stop_bars,
                "time_stop_break_even_label": format_time_stop_label(time_stop_bars),
                "runs": summary["runs"],
                "profitable_runs": summary["profitable_runs"],
                "avg_total_pnl": summary["avg_total_pnl"],
                "avg_max_drawdown": summary["avg_max_drawdown"],
                "avg_total_trades": summary["avg_total_trades"],
                "avg_win_rate": summary["avg_win_rate"],
                "best_total_pnl": summary["best_total_pnl"],
                "worst_total_pnl": summary["worst_total_pnl"],
            }
        )
    return rows


def build_time_stop_by_timeframe_rows(records: list[RunRecord]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for bar, bar_name in TIMEFRAMES:
        for time_stop_bars in TIME_STOP_BREAK_EVEN_OPTIONS:
            scoped = [row for row in records if row.bar == bar and row.time_stop_break_even_bars == time_stop_bars]
            summary = summarize_records(scoped)
            rows.append(
                {
                    "bar": bar,
                    "bar_label": bar_name,
                    "time_stop_break_even_bars": time_stop_bars,
                    "time_stop_break_even_label": format_time_stop_label(time_stop_bars),
                    "runs": summary["runs"],
                    "profitable_runs": summary["profitable_runs"],
                    "avg_total_pnl": summary["avg_total_pnl"],
                    "avg_max_drawdown": summary["avg_max_drawdown"],
                    "avg_total_trades": summary["avg_total_trades"],
                    "avg_win_rate": summary["avg_win_rate"],
                }
            )
    return rows


def build_time_stop_by_symbol_rows(records: list[RunRecord]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for symbol in SYMBOLS:
        for time_stop_bars in TIME_STOP_BREAK_EVEN_OPTIONS:
            scoped = [row for row in records if row.symbol == symbol and row.time_stop_break_even_bars == time_stop_bars]
            summary = summarize_records(scoped)
            rows.append(
                {
                    "symbol": symbol,
                    "time_stop_break_even_bars": time_stop_bars,
                    "time_stop_break_even_label": format_time_stop_label(time_stop_bars),
                    "runs": summary["runs"],
                    "profitable_runs": summary["profitable_runs"],
                    "avg_total_pnl": summary["avg_total_pnl"],
                    "avg_max_drawdown": summary["avg_max_drawdown"],
                    "avg_total_trades": summary["avg_total_trades"],
                    "avg_win_rate": summary["avg_win_rate"],
                }
            )
    return rows


def format_summary_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    formatted: list[dict[str, object]] = []
    for item in rows:
        payload = dict(item)
        for key in ("avg_total_pnl", "avg_max_drawdown", "avg_total_trades", "avg_win_rate", "best_total_pnl", "worst_total_pnl"):
            if key in payload:
                digits = 2 if key in ("avg_total_trades", "avg_win_rate") else 4
                payload[key] = format_decimal_fixed(payload[key], digits)
        formatted.append(payload)
    return formatted


def build_best_distribution_rows(records: list[RunRecord], *, dimension: str) -> list[dict[str, object]]:
    if dimension == "entries":
        options = MAX_ENTRIES_OPTIONS
        getter = lambda row: row.max_entries_per_trend
        labeler = format_max_entries_label
        key_name = "max_entries_per_trend"
        label_name = "max_entries_label"
    else:
        options = TIME_STOP_BREAK_EVEN_OPTIONS
        getter = lambda row: row.time_stop_break_even_bars
        labeler = format_time_stop_label
        key_name = "time_stop_break_even_bars"
        label_name = "time_stop_break_even_label"

    counts = {value: 0 for value in options}
    total = len(records)
    for record in records:
        counts[getter(record)] = counts.get(getter(record), 0) + 1

    rows: list[dict[str, object]] = []
    for value in options:
        count = counts.get(value, 0)
        pct = (Decimal(count) * Decimal("100") / Decimal(total)) if total else None
        rows.append(
            {
                key_name: value,
                label_name: labeler(value),
                "count": count,
                "pct_of_best": pct,
            }
        )
    return rows


def format_distribution_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    formatted: list[dict[str, object]] = []
    for item in rows:
        payload = dict(item)
        payload["pct_of_best"] = format_decimal_fixed(payload["pct_of_best"], 2)
        formatted.append(payload)
    return formatted


def build_top_rows_by_entries(records: list[RunRecord]) -> list[RunRecord]:
    rows: list[RunRecord] = []
    for max_entries in MAX_ENTRIES_OPTIONS:
        scoped = [row for row in records if row.max_entries_per_trend == max_entries]
        if scoped:
            rows.append(max(scoped, key=lambda row: row.total_pnl))
    return sorted(rows, key=sort_record_with_entries_first)


def build_top_rows_by_time_stop(records: list[RunRecord]) -> list[RunRecord]:
    rows: list[RunRecord] = []
    for time_stop_bars in TIME_STOP_BREAK_EVEN_OPTIONS:
        scoped = [row for row in records if row.time_stop_break_even_bars == time_stop_bars]
        if scoped:
            rows.append(max(scoped, key=lambda row: row.total_pnl))
    return sorted(rows, key=sort_record_with_time_stop_first)


def summarize_entry_choices(records: list[RunRecord]) -> dict[str, int]:
    counts = {"ema21": 0, "ema55": 0}
    for record in records:
        if record.entry_reference_ema == 21:
            counts["ema21"] += 1
        elif record.entry_reference_ema == 55:
            counts["ema55"] += 1
    return counts


def choose_best_summary_row(rows: list[dict[str, object]], *, value_key: str) -> dict[str, object]:
    def score(row: dict[str, object]) -> tuple[bool, Decimal, Decimal, int]:
        avg_total_pnl = row["avg_total_pnl"]
        avg_max_drawdown = row["avg_max_drawdown"]
        value = int(row[value_key])
        return (
            avg_total_pnl is not None,
            avg_total_pnl if isinstance(avg_total_pnl, Decimal) else Decimal("-999999999"),
            -(avg_max_drawdown if isinstance(avg_max_drawdown, Decimal) else Decimal("999999999")),
            -tie_break_order_for_time_stop(value),
        )

    return max(rows, key=score)


def build_best_time_stop_by_timeframe_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    best_rows: list[dict[str, object]] = []
    for bar, _ in TIMEFRAMES:
        scoped = [row for row in rows if row["bar"] == bar]
        if scoped:
            best_rows.append(choose_best_summary_row(scoped, value_key="time_stop_break_even_bars"))
    return sorted(best_rows, key=lambda row: timeframe_order(str(row["bar"])))


def build_best_time_stop_by_symbol_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    best_rows: list[dict[str, object]] = []
    for symbol in SYMBOLS:
        scoped = [row for row in rows if row["symbol"] == symbol]
        if scoped:
            best_rows.append(choose_best_summary_row(scoped, value_key="time_stop_break_even_bars"))
    return sorted(best_rows, key=lambda row: symbol_order(str(row["symbol"])))


def build_best_max_entries_summary_row(rows: list[dict[str, object]]) -> dict[str, object]:
    return max(
        rows,
        key=lambda row: (
            row["avg_total_pnl"] is not None,
            row["avg_total_pnl"] if isinstance(row["avg_total_pnl"], Decimal) else Decimal("-999999999"),
            -(row["avg_max_drawdown"] if isinstance(row["avg_max_drawdown"], Decimal) else Decimal("999999999")),
            -max_entries_order(int(row["max_entries_per_trend"])),
        ),
    )


def build_time_stop_recommendation(
    time_stop_summary_rows: list[dict[str, object]],
    best_time_stop_distribution_rows: list[dict[str, object]],
    best_by_timeframe_rows: list[dict[str, object]],
    best_by_symbol_rows: list[dict[str, object]],
) -> dict[str, object]:
    overall_map = {int(row["time_stop_break_even_bars"]): row for row in time_stop_summary_rows}
    distribution_map = {int(row["time_stop_break_even_bars"]): int(row["count"]) for row in best_time_stop_distribution_rows}
    timeframe_wins = {value: 0 for value in TIME_STOP_BREAK_EVEN_OPTIONS}
    symbol_wins = {value: 0 for value in TIME_STOP_BREAK_EVEN_OPTIONS}

    for row in best_by_timeframe_rows:
        timeframe_wins[int(row["time_stop_break_even_bars"])] += 1
    for row in best_by_symbol_rows:
        symbol_wins[int(row["time_stop_break_even_bars"])] += 1

    def score(value: int) -> tuple[int, int, int, int, Decimal, Decimal, int]:
        overall = overall_map[value]
        avg_total_pnl = overall["avg_total_pnl"] if isinstance(overall["avg_total_pnl"], Decimal) else Decimal("-999999999")
        avg_max_drawdown = overall["avg_max_drawdown"] if isinstance(overall["avg_max_drawdown"], Decimal) else Decimal("999999999")
        profitable_runs = int(overall["profitable_runs"])
        return (
            timeframe_wins[value],
            symbol_wins[value],
            distribution_map.get(value, 0),
            profitable_runs,
            avg_total_pnl,
            -avg_max_drawdown,
            -tie_break_order_for_time_stop(value),
        )

    recommended_value = max(TIME_STOP_BREAK_EVEN_OPTIONS, key=score)
    overall = overall_map[recommended_value]

    if recommended_value <= 0:
        note = "当前样本里，时间保本开启后更容易过早收紧止损，关闭反而更稳。"
    elif recommended_value <= 5:
        note = "当前样本更偏向尽快保护利润，说明短促波段较多，过慢保本容易回吐。"
    elif recommended_value <= 10:
        note = "当前样本更偏向中性节奏，既给趋势留呼吸，又不会把保护动作拖得太晚。"
    elif recommended_value <= 15:
        note = "当前样本说明行情需要更大的呼吸空间，过早保本会伤害趋势单。"
    else:
        note = "当前样本明显偏向让利润奔跑，只有较晚保本才不至于过早把趋势切掉。"

    return {
        "recommended_value": recommended_value,
        "recommended_label": format_time_stop_label(recommended_value),
        "timeframe_wins": timeframe_wins[recommended_value],
        "symbol_wins": symbol_wins[recommended_value],
        "best_distribution_count": distribution_map.get(recommended_value, 0),
        "profitable_runs": overall["profitable_runs"],
        "avg_total_pnl": overall["avg_total_pnl"],
        "avg_max_drawdown": overall["avg_max_drawdown"],
        "note": note,
    }


def build_text_report(
    *,
    exported_at: datetime,
    records: list[RunRecord],
    failures: list[str],
    exports: ExportBundle,
) -> str:
    overall_best = sorted(pick_best_records(records, lambda row: row.group_key), key=sort_record)
    best_by_entries = sorted(pick_best_records(records, lambda row: row.group_key_with_entries), key=sort_record_with_entries_first)
    best_by_time_stop = sorted(pick_best_records(records, lambda row: row.group_key_with_time_stop), key=sort_record_with_time_stop_first)
    max_entries_summary_rows = build_max_entries_summary_rows(records)
    time_stop_summary_rows = build_time_stop_summary_rows(records)
    time_stop_by_timeframe_rows = build_time_stop_by_timeframe_rows(records)
    time_stop_by_symbol_rows = build_time_stop_by_symbol_rows(records)
    best_entries_distribution_rows = build_best_distribution_rows(overall_best, dimension="entries")
    best_time_stop_distribution_rows = build_best_distribution_rows(overall_best, dimension="time_stop")
    best_time_stop_by_timeframe_rows = build_best_time_stop_by_timeframe_rows(time_stop_by_timeframe_rows)
    best_time_stop_by_symbol_rows = build_best_time_stop_by_symbol_rows(time_stop_by_symbol_rows)
    recommendation = build_time_stop_recommendation(
        time_stop_summary_rows,
        best_time_stop_distribution_rows,
        best_time_stop_by_timeframe_rows,
        best_time_stop_by_symbol_rows,
    )
    overall_summary = summarize_records(records)
    entries_peak = max(best_entries_distribution_rows, key=lambda row: (row["count"], -max_entries_order(int(row["max_entries_per_trend"]))))
    time_stop_peak = max(best_time_stop_distribution_rows, key=lambda row: (row["count"], -time_stop_order(int(row["time_stop_break_even_bars"]))))
    best_entries_avg = build_best_max_entries_summary_row(max_entries_summary_rows)
    entry_stats = summarize_entry_choices(overall_best)
    top_rows = sorted(records, key=lambda row: row.total_pnl, reverse=True)[:20]
    top_by_entries_rows = build_top_rows_by_entries(records)
    top_by_time_stop_rows = build_top_rows_by_time_stop(records)

    lines = [
        "新版全面回测总报告（动态止盈 + 时间保本K线数）",
        "",
        f"导出时间：{exported_at.strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "一、统一口径",
        f"1. 回测范围：{', '.join(symbol.replace('-USDT-SWAP', '') for symbol in SYMBOLS)}；{' / '.join(label for _, label in TIMEFRAMES)}；10000根K线；多空分开。",
        f"2. 策略范围：EMA动态委托-多头、EMA动态委托-空头；挂单EMA 21 / 55；动态止盈。",
        f"3. 固定参数：EMA{EMA_PERIOD}；趋势EMA{TREND_EMA_PERIOD}；ATR{ATR_PERIOD}；单笔风险 {format_decimal(RISK_AMOUNT)}；初始资金 {format_decimal(INITIAL_CAPITAL)}。",
        f"4. 手续费：Maker {format_rate_pct(MAKER_FEE_RATE)}%；Taker {format_rate_pct(TAKER_FEE_RATE)}%。",
        f"5. 双滑点口径：开仓滑点 {format_rate_pct(ENTRY_SLIPPAGE_RATE)}%；平仓滑点 {format_rate_pct(EXIT_SLIPPAGE_RATE)}%。",
        "6. 双滑点说明：当前EMA动态挂单策略按等待单入场，开仓侧不额外加滑点；平仓侧统一按平仓滑点计入。",
        f"7. 2R保本开关：{'开启' if DYNAMIC_TWO_R_BREAK_EVEN else '关闭'}。",
        f"8. 手续费偏移开关：{'开启' if DYNAMIC_FEE_OFFSET_ENABLED else '关闭'}。",
        "9. 变量一：止损 1 / 1.5 / 2 ATR。",
        "10. 变量二：每波开仓次数 0 / 1 / 2 / 3。",
        "11. 变量三：时间保本K线数 0 / 5 / 10 / 15 / 20；其中 0 代表关闭时间保本。",
        "",
        "二、执行概览",
        f"1. 总运行数：{overall_summary['runs']}。",
        f"2. 盈利组合数：{overall_summary['profitable_runs']}。",
        f"3. 平均总盈亏：{format_decimal_fixed(overall_summary['avg_total_pnl'], 4)}。",
        f"4. 平均最大回撤：{format_decimal_fixed(overall_summary['avg_max_drawdown'], 4)}。",
        f"5. 平均交易数：{format_decimal_fixed(overall_summary['avg_total_trades'], 2)}。",
        f"6. 平均胜率：{format_decimal_fixed(overall_summary['avg_win_rate'], 2)}%。",
        f"7. 最终40个最佳组合里，出现最多的每波开仓次数是 {entries_peak['max_entries_label']}，共 {entries_peak['count']} 组。",
        f"8. 最终40个最佳组合里，出现最多的时间保本值是 {time_stop_peak['time_stop_break_even_label']}，共 {time_stop_peak['count']} 组。",
        "",
        "三、时间保本K线数整体结论",
    ]

    for row in time_stop_summary_rows:
        lines.append(
            f"时间保本{row['time_stop_break_even_label']} | 运行数 {row['runs']} | 盈利组合 {row['profitable_runs']} | "
            f"平均总盈亏 {format_decimal_fixed(row['avg_total_pnl'], 4)} | 平均回撤 {format_decimal_fixed(row['avg_max_drawdown'], 4)} | "
            f"平均交易数 {format_decimal_fixed(row['avg_total_trades'], 2)} | 平均胜率 {format_decimal_fixed(row['avg_win_rate'], 2)}% | "
            f"最佳 {format_decimal_fixed(row['best_total_pnl'], 4)} | 最差 {format_decimal_fixed(row['worst_total_pnl'], 4)}"
        )

    lines.extend(
        [
            "",
            "四、时间保本K线数推荐值",
            f"1. 综合推荐值：{recommendation['recommended_label']}。",
            f"2. 推荐理由：在按周期最优里胜出 {recommendation['timeframe_wins']} 次；按币种最优里胜出 {recommendation['symbol_wins']} 次；在最终最佳40组里出现 {recommendation['best_distribution_count']} 次。",
            f"3. 该档整体平均总盈亏：{format_decimal_fixed(recommendation['avg_total_pnl'], 4)}；平均最大回撤：{format_decimal_fixed(recommendation['avg_max_drawdown'], 4)}；盈利组合数：{recommendation['profitable_runs']}。",
            f"4. 交易员解读：{recommendation['note']}",
            "",
            "五、按周期看时间保本K线数",
        ]
    )

    for row in best_time_stop_by_timeframe_rows:
        lines.append(
            f"{row['bar_label']} | 最优时间保本 {row['time_stop_break_even_label']} | "
            f"平均总盈亏 {format_decimal_fixed(row['avg_total_pnl'], 4)} | 平均回撤 {format_decimal_fixed(row['avg_max_drawdown'], 4)} | "
            f"平均交易数 {format_decimal_fixed(row['avg_total_trades'], 2)} | 平均胜率 {format_decimal_fixed(row['avg_win_rate'], 2)}%"
        )

    lines.extend(["", "六、按币种看时间保本K线数"])
    for row in best_time_stop_by_symbol_rows:
        lines.append(
            f"{row['symbol']} | 最优时间保本 {row['time_stop_break_even_label']} | "
            f"平均总盈亏 {format_decimal_fixed(row['avg_total_pnl'], 4)} | 平均回撤 {format_decimal_fixed(row['avg_max_drawdown'], 4)} | "
            f"平均交易数 {format_decimal_fixed(row['avg_total_trades'], 2)} | 平均胜率 {format_decimal_fixed(row['avg_win_rate'], 2)}%"
        )

    lines.extend(["", "七、最终最佳组合中的时间保本分布"])
    for row in best_time_stop_distribution_rows:
        lines.append(
            f"时间保本{row['time_stop_break_even_label']} | 出现 {row['count']} 次 | 占比 {format_decimal_fixed(row['pct_of_best'], 2)}%"
        )

    lines.extend(["", "八、每波开仓次数分组结论"])
    for row in max_entries_summary_rows:
        lines.append(
            f"每波开仓{row['max_entries_label']} | 运行数 {row['runs']} | 盈利组合 {row['profitable_runs']} | "
            f"平均总盈亏 {format_decimal_fixed(row['avg_total_pnl'], 4)} | 平均回撤 {format_decimal_fixed(row['avg_max_drawdown'], 4)} | "
            f"平均交易数 {format_decimal_fixed(row['avg_total_trades'], 2)} | 平均胜率 {format_decimal_fixed(row['avg_win_rate'], 2)}% | "
            f"最佳 {format_decimal_fixed(row['best_total_pnl'], 4)} | 最差 {format_decimal_fixed(row['worst_total_pnl'], 4)}"
        )

    lines.extend(
        [
            "",
            "九、最终最佳组合中的开仓次数分布",
        ]
    )
    for row in best_entries_distribution_rows:
        lines.append(
            f"每波开仓{row['max_entries_label']} | 出现 {row['count']} 次 | 占比 {format_decimal_fixed(row['pct_of_best'], 2)}%"
        )

    lines.extend(["", "十、各时间保本值下的最强组合"])
    for row in top_by_time_stop_rows:
        lines.append(
            f"时间保本{format_time_stop_label(row.time_stop_break_even_bars)} | {row.bar_label} | {row.symbol} | {row.direction} | "
            f"{row.param_label} | 总盈亏 {format_decimal_fixed(row.total_pnl, 4)} | 胜率 {format_decimal_fixed(row.win_rate, 2)}% | "
            f"交易数 {row.total_trades} | 最大回撤 {format_decimal_fixed(row.max_drawdown, 4)}"
        )

    lines.extend(["", "十一、各开仓次数下的最强组合"])
    for row in top_by_entries_rows:
        lines.append(
            f"每波开仓{format_max_entries_label(row.max_entries_per_trend)} | {row.bar_label} | {row.symbol} | {row.direction} | "
            f"{row.param_label} | 总盈亏 {format_decimal_fixed(row.total_pnl, 4)} | 胜率 {format_decimal_fixed(row.win_rate, 2)}% | "
            f"交易数 {row.total_trades} | 最大回撤 {format_decimal_fixed(row.max_drawdown, 4)}"
        )

    lines.extend(["", "十二、最终最佳清单"])
    for row in overall_best:
        lines.append(
            f"{row.bar_label} | {row.symbol} | {row.direction} | {row.param_label} | "
            f"总盈亏 {format_decimal_fixed(row.total_pnl, 4)} | 胜率 {format_decimal_fixed(row.win_rate, 2)}% | "
            f"交易数 {row.total_trades} | 最大回撤 {format_decimal_fixed(row.max_drawdown, 4)}"
        )

    lines.extend(["", "十三、全局前20"])
    for row in top_rows:
        lines.append(
            f"{row.bar_label} | {row.symbol} | {row.direction} | {row.param_label} | "
            f"总盈亏 {format_decimal_fixed(row.total_pnl, 4)} | 胜率 {format_decimal_fixed(row.win_rate, 2)}% | "
            f"交易数 {row.total_trades} | 最大回撤 {format_decimal_fixed(row.max_drawdown, 4)}"
        )

    lines.extend(
        [
            "",
            "十四、EMA挂单偏好",
            f"1. 最终最佳组合中：EMA21 胜出 {entry_stats['ema21']} 组；EMA55 胜出 {entry_stats['ema55']} 组。",
            "",
            "十五、交易员视角结论",
            f"1. 这轮回测的核心不是去证明“时间保本越快越好”，而是找一个在不同币种、不同周期下更均衡的保护点。综合结果看，更中肯的默认值是 {recommendation['recommended_label']}。",
            f"2. 如果你更偏重控制回吐，优先看 {format_time_stop_label(min(value for value in TIME_STOP_BREAK_EVEN_OPTIONS if value > 0))} 到 {format_time_stop_label(10)} 这些较快保护档；如果你更偏重让趋势利润继续跑，重点看 {format_time_stop_label(10)} 到 {format_time_stop_label(20)}。",
            f"3. 每波开仓次数仍然是另一个很强的收益/回撤调节器。按平均结果看，最优档是每波开仓{format_max_entries_label(int(best_entries_avg['max_entries_per_trend']))}。",
            "4. 实盘首轮不要同时把时间保本和开仓次数都调得很激进。更稳的做法是：先用本次推荐的时间保本值，再把开仓次数保持在整体更稳的档位上验证执行一致性。",
            "",
            "十六、数据文件",
            f"1. 全量结果：{exports.all_runs_csv}",
            f"2. 最终最佳：{exports.overall_best_csv}",
            f"3. 最终最佳（按开仓次数拆分）：{exports.best_by_entries_csv}",
            f"4. 最终最佳（按时间保本拆分）：{exports.best_by_time_stop_csv}",
            f"5. 开仓次数汇总：{exports.max_entries_summary_csv}",
            f"6. 时间保本整体汇总：{exports.time_stop_summary_csv}",
            f"7. 时间保本按周期汇总：{exports.time_stop_by_timeframe_csv}",
            f"8. 时间保本按币种汇总：{exports.time_stop_by_symbol_csv}",
            f"9. 最佳组合开仓次数分布：{exports.best_entries_distribution_csv}",
            f"10. 最佳组合时间保本分布：{exports.best_time_stop_distribution_csv}",
            f"11. 失败记录：{exports.failures_txt}",
        ]
    )

    if failures:
        lines.extend(["", "十七、失败记录摘录", f"共 {len(failures)} 条"])
        lines.extend(failures[:100])

    return "\n".join(lines).rstrip() + "\n"


def write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def main() -> None:
    exported_at = datetime.now()
    stamp = exported_at.strftime("%Y%m%d_%H%M%S")
    client = OkxRestClient()

    exports = ExportBundle(
        all_runs_csv=REPORTS_DIR / f"新版全面回测_动态止盈_时间保本_全量结果_{stamp}.csv",
        overall_best_csv=REPORTS_DIR / f"新版全面回测_动态止盈_时间保本_最终最佳_{stamp}.csv",
        best_by_entries_csv=REPORTS_DIR / f"新版全面回测_动态止盈_时间保本_按开仓次数最佳_{stamp}.csv",
        best_by_time_stop_csv=REPORTS_DIR / f"新版全面回测_动态止盈_时间保本_按时间保本最佳_{stamp}.csv",
        max_entries_summary_csv=REPORTS_DIR / f"新版全面回测_动态止盈_开仓次数汇总_{stamp}.csv",
        time_stop_summary_csv=REPORTS_DIR / f"新版全面回测_动态止盈_时间保本汇总_{stamp}.csv",
        time_stop_by_timeframe_csv=REPORTS_DIR / f"新版全面回测_动态止盈_时间保本按周期汇总_{stamp}.csv",
        time_stop_by_symbol_csv=REPORTS_DIR / f"新版全面回测_动态止盈_时间保本按币种汇总_{stamp}.csv",
        best_entries_distribution_csv=REPORTS_DIR / f"新版全面回测_动态止盈_最佳开仓次数分布_{stamp}.csv",
        best_time_stop_distribution_csv=REPORTS_DIR / f"新版全面回测_动态止盈_最佳时间保本分布_{stamp}.csv",
        failures_txt=REPORTS_DIR / f"新版全面回测_动态止盈_时间保本_失败记录_{stamp}.txt",
        report_txt=REPORTS_DIR / f"新版全面回测总报告_动态止盈_时间保本_{stamp}.txt",
        summary_json=REPORTS_DIR / f"新版全面回测摘要_动态止盈_时间保本_{stamp}.json",
    )

    market_cache = load_market_data(client)
    records, failures = run_dynamic_suite(market_cache)

    all_records = sorted(records, key=sort_record)
    overall_best = sorted(pick_best_records(records, lambda row: row.group_key), key=sort_record)
    best_by_entries = sorted(pick_best_records(records, lambda row: row.group_key_with_entries), key=sort_record_with_entries_first)
    best_by_time_stop = sorted(pick_best_records(records, lambda row: row.group_key_with_time_stop), key=sort_record_with_time_stop_first)
    max_entries_summary_rows = build_max_entries_summary_rows(records)
    time_stop_summary_rows = build_time_stop_summary_rows(records)
    time_stop_by_timeframe_rows = build_time_stop_by_timeframe_rows(records)
    time_stop_by_symbol_rows = build_time_stop_by_symbol_rows(records)
    best_entries_distribution_rows = build_best_distribution_rows(overall_best, dimension="entries")
    best_time_stop_distribution_rows = build_best_distribution_rows(overall_best, dimension="time_stop")
    best_time_stop_by_timeframe_rows = build_best_time_stop_by_timeframe_rows(time_stop_by_timeframe_rows)
    best_time_stop_by_symbol_rows = build_best_time_stop_by_symbol_rows(time_stop_by_symbol_rows)
    recommendation = build_time_stop_recommendation(
        time_stop_summary_rows,
        best_time_stop_distribution_rows,
        best_time_stop_by_timeframe_rows,
        best_time_stop_by_symbol_rows,
    )

    write_csv(exports.all_runs_csv, csv_rows(all_records))
    write_csv(exports.overall_best_csv, csv_rows(overall_best))
    write_csv(exports.best_by_entries_csv, csv_rows(best_by_entries))
    write_csv(exports.best_by_time_stop_csv, csv_rows(best_by_time_stop))
    write_csv(exports.max_entries_summary_csv, format_summary_rows(max_entries_summary_rows))
    write_csv(exports.time_stop_summary_csv, format_summary_rows(time_stop_summary_rows))
    write_csv(exports.time_stop_by_timeframe_csv, format_summary_rows(time_stop_by_timeframe_rows))
    write_csv(exports.time_stop_by_symbol_csv, format_summary_rows(time_stop_by_symbol_rows))
    write_csv(exports.best_entries_distribution_csv, format_distribution_rows(best_entries_distribution_rows))
    write_csv(exports.best_time_stop_distribution_csv, format_distribution_rows(best_time_stop_distribution_rows))
    exports.failures_txt.write_text("\n".join(failures) + ("\n" if failures else ""), encoding="utf-8-sig")
    exports.report_txt.write_text(
        build_text_report(
            exported_at=exported_at,
            records=records,
            failures=failures,
            exports=exports,
        ),
        encoding="utf-8-sig",
    )

    write_json(
        exports.summary_json,
        {
            "exported_at": exported_at.strftime("%Y-%m-%d %H:%M:%S"),
            "entry_slippage_pct": format_rate_pct(ENTRY_SLIPPAGE_RATE),
            "exit_slippage_pct": format_rate_pct(EXIT_SLIPPAGE_RATE),
            "dynamic_two_r_break_even": DYNAMIC_TWO_R_BREAK_EVEN,
            "dynamic_fee_offset_enabled": DYNAMIC_FEE_OFFSET_ENABLED,
            "symbols": list(SYMBOLS),
            "timeframes": [value for value, _ in TIMEFRAMES],
            "atr_stop_multipliers": [format_decimal(value) for value in ATR_STOP_MULTIPLIERS],
            "max_entries_options": list(MAX_ENTRIES_OPTIONS),
            "time_stop_break_even_options": list(TIME_STOP_BREAK_EVEN_OPTIONS),
            "recommended_time_stop_break_even": recommendation["recommended_value"],
            "recommended_time_stop_break_even_label": recommendation["recommended_label"],
            "all_runs_csv": str(exports.all_runs_csv),
            "overall_best_csv": str(exports.overall_best_csv),
            "best_by_entries_csv": str(exports.best_by_entries_csv),
            "best_by_time_stop_csv": str(exports.best_by_time_stop_csv),
            "max_entries_summary_csv": str(exports.max_entries_summary_csv),
            "time_stop_summary_csv": str(exports.time_stop_summary_csv),
            "time_stop_by_timeframe_csv": str(exports.time_stop_by_timeframe_csv),
            "time_stop_by_symbol_csv": str(exports.time_stop_by_symbol_csv),
            "best_entries_distribution_csv": str(exports.best_entries_distribution_csv),
            "best_time_stop_distribution_csv": str(exports.best_time_stop_distribution_csv),
            "report_txt": str(exports.report_txt),
            "failures_txt": str(exports.failures_txt),
            "failures": failures,
        },
    )

    print(f"总报告 -> {exports.report_txt}")
    print(f"全量结果 -> {exports.all_runs_csv}")
    print(f"最终最佳 -> {exports.overall_best_csv}")
    print(f"按开仓次数最佳 -> {exports.best_by_entries_csv}")
    print(f"按时间保本最佳 -> {exports.best_by_time_stop_csv}")
    print(f"开仓次数汇总 -> {exports.max_entries_summary_csv}")
    print(f"时间保本汇总 -> {exports.time_stop_summary_csv}")
    print(f"时间保本按周期汇总 -> {exports.time_stop_by_timeframe_csv}")
    print(f"时间保本按币种汇总 -> {exports.time_stop_by_symbol_csv}")
    print(f"最佳开仓次数分布 -> {exports.best_entries_distribution_csv}")
    print(f"最佳时间保本分布 -> {exports.best_time_stop_distribution_csv}")
    print(f"失败记录 -> {exports.failures_txt}")
    print(f"摘要 -> {exports.summary_json}")


if __name__ == "__main__":
    main()
