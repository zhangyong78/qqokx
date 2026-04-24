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
TAKE_RATIOS = (Decimal("1"), Decimal("2"), Decimal("3"))
MAX_ENTRIES_OPTIONS = (0, 1, 2, 3)
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


@dataclass(frozen=True)
class RunRecord:
    mode: str
    symbol: str
    bar: str
    bar_label: str
    direction: str
    entry_reference_ema: int
    max_entries_per_trend: int
    stop_atr: Decimal
    take_ratio: Decimal | None
    take_atr: Decimal | None
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
        if self.mode == "dynamic":
            return (
                f"EMA{self.entry_reference_ema}挂单 | SL x{format_decimal(self.stop_atr)} | "
                f"动态止盈 | 每波开仓{format_max_entries_label(self.max_entries_per_trend)}"
            )
        return (
            f"EMA{self.entry_reference_ema}挂单 | SL x{format_decimal(self.stop_atr)} | "
            f"TP x{format_decimal(self.take_atr or Decimal('0'))} | "
            f"每波开仓{format_max_entries_label(self.max_entries_per_trend)}"
        )

    @property
    def comparison_key(self) -> tuple[str, str, str]:
        return (self.symbol, self.bar, self.direction)

    @property
    def comparison_key_with_entries(self) -> tuple[str, str, str, int]:
        return (self.symbol, self.bar, self.direction, self.max_entries_per_trend)

    @property
    def mode_key(self) -> tuple[str, str, str, str]:
        return (self.mode, self.symbol, self.bar, self.direction)

    @property
    def mode_key_with_entries(self) -> tuple[str, str, str, str, int]:
        return (self.mode, self.symbol, self.bar, self.direction, self.max_entries_per_trend)


@dataclass(frozen=True)
class ExportBundle:
    all_runs_csv: Path
    dynamic_best_csv: Path
    fixed_best_csv: Path
    comparison_csv: Path
    dynamic_best_by_limit_csv: Path
    fixed_best_by_limit_csv: Path
    comparison_by_limit_csv: Path
    limit_summary_csv: Path
    best_limit_distribution_csv: Path
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


def mode_label(mode: str) -> str:
    return "动态止盈" if mode == "dynamic" else "固定止盈"


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
    take_atr: Decimal,
    take_profit_mode: str,
    max_entries_per_trend: int,
) -> StrategyConfig:
    return StrategyConfig(
        inst_id=symbol,
        bar=bar,
        ema_period=EMA_PERIOD,
        trend_ema_period=TREND_EMA_PERIOD,
        big_ema_period=233,
        atr_period=ATR_PERIOD,
        atr_stop_multiplier=stop_atr,
        atr_take_multiplier=take_atr,
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
        take_profit_mode=take_profit_mode,
        max_entries_per_trend=max_entries_per_trend,
        entry_reference_ema_period=entry_reference_ema_period,
        backtest_entry_slippage_rate=ENTRY_SLIPPAGE_RATE,
        backtest_exit_slippage_rate=EXIT_SLIPPAGE_RATE,
        backtest_slippage_rate=EXIT_SLIPPAGE_RATE,
        dynamic_two_r_break_even=DYNAMIC_TWO_R_BREAK_EVEN,
        dynamic_fee_offset_enabled=DYNAMIC_FEE_OFFSET_ENABLED,
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
    )
    sequence = 0
    for symbol in SYMBOLS:
        for bar, bar_name in TIMEFRAMES:
            instrument, candles, note = market_cache[(symbol, bar)]
            for strategy_id, signal_mode, direction_name in DIRECTIONS:
                for entry_ema in ENTRY_REFERENCE_EMAS:
                    for stop_atr in ATR_STOP_MULTIPLIERS:
                        for max_entries in MAX_ENTRIES_OPTIONS:
                            sequence += 1
                            print(
                                f"[{sequence}/{total}] dynamic {symbol} {bar_name} {direction_name} "
                                f"EMA{entry_ema} SLx{format_decimal(stop_atr)} 每波{format_max_entries_label(max_entries)}"
                            )
                            config = build_base_config(
                                symbol=symbol,
                                bar=bar,
                                strategy_id=strategy_id,
                                signal_mode=signal_mode,
                                entry_reference_ema_period=entry_ema,
                                stop_atr=stop_atr,
                                take_atr=stop_atr * Decimal("2"),
                                take_profit_mode="dynamic",
                                max_entries_per_trend=max_entries,
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
                                    f"dynamic | {symbol} | {bar} | {direction_name} | EMA{entry_ema} | "
                                    f"SLx{format_decimal(stop_atr)} | 每波{format_max_entries_label(max_entries)} -> {exc}"
                                )
                                continue
                            rows.append(
                                RunRecord(
                                    mode="dynamic",
                                    symbol=symbol,
                                    bar=bar,
                                    bar_label=bar_name,
                                    direction=direction_name,
                                    entry_reference_ema=entry_ema,
                                    max_entries_per_trend=config.max_entries_per_trend,
                                    stop_atr=stop_atr,
                                    take_ratio=None,
                                    take_atr=None,
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


def run_fixed_suite(
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
        * len(TAKE_RATIOS)
        * len(MAX_ENTRIES_OPTIONS)
    )
    sequence = 0
    for symbol in SYMBOLS:
        for bar, bar_name in TIMEFRAMES:
            instrument, candles, note = market_cache[(symbol, bar)]
            for strategy_id, signal_mode, direction_name in DIRECTIONS:
                for entry_ema in ENTRY_REFERENCE_EMAS:
                    for stop_atr in ATR_STOP_MULTIPLIERS:
                        for take_ratio in TAKE_RATIOS:
                            for max_entries in MAX_ENTRIES_OPTIONS:
                                take_atr = stop_atr * take_ratio
                                sequence += 1
                                print(
                                    f"[{sequence}/{total}] fixed {symbol} {bar_name} {direction_name} "
                                    f"EMA{entry_ema} SLx{format_decimal(stop_atr)} TPx{format_decimal(take_atr)} "
                                    f"每波{format_max_entries_label(max_entries)}"
                                )
                                config = build_base_config(
                                    symbol=symbol,
                                    bar=bar,
                                    strategy_id=strategy_id,
                                    signal_mode=signal_mode,
                                    entry_reference_ema_period=entry_ema,
                                    stop_atr=stop_atr,
                                    take_atr=take_atr,
                                    take_profit_mode="fixed",
                                    max_entries_per_trend=max_entries,
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
                                        f"fixed | {symbol} | {bar} | {direction_name} | EMA{entry_ema} | "
                                        f"SLx{format_decimal(stop_atr)} | TPx{format_decimal(take_atr)} | "
                                        f"每波{format_max_entries_label(max_entries)} -> {exc}"
                                    )
                                    continue
                                rows.append(
                                    RunRecord(
                                        mode="fixed",
                                        symbol=symbol,
                                        bar=bar,
                                        bar_label=bar_name,
                                        direction=direction_name,
                                        entry_reference_ema=entry_ema,
                                        max_entries_per_trend=config.max_entries_per_trend,
                                        stop_atr=stop_atr,
                                        take_ratio=take_ratio,
                                        take_atr=take_atr,
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


def build_mode_comparisons(
    dynamic_rows: list[RunRecord],
    fixed_rows: list[RunRecord],
    *,
    include_limit_in_key: bool,
) -> list[dict[str, object]]:
    dynamic_map = {
        (row.comparison_key_with_entries if include_limit_in_key else row.comparison_key): row for row in dynamic_rows
    }
    fixed_map = {
        (row.comparison_key_with_entries if include_limit_in_key else row.comparison_key): row for row in fixed_rows
    }
    rows: list[dict[str, object]] = []
    for key in sorted(dynamic_map.keys() | fixed_map.keys(), key=lambda item: sort_comparison_key(item, include_limit_in_key)):
        dynamic_row = dynamic_map.get(key)
        fixed_row = fixed_map.get(key)
        winner = "-"
        pnl_gap = Decimal("0")
        if dynamic_row and fixed_row:
            if dynamic_row.total_pnl > fixed_row.total_pnl:
                winner = "dynamic"
                pnl_gap = dynamic_row.total_pnl - fixed_row.total_pnl
            elif fixed_row.total_pnl > dynamic_row.total_pnl:
                winner = "fixed"
                pnl_gap = fixed_row.total_pnl - dynamic_row.total_pnl
            else:
                winner = "tie"
        elif dynamic_row:
            winner = "dynamic"
        elif fixed_row:
            winner = "fixed"

        if include_limit_in_key:
            symbol, bar, direction, max_entries = key
        else:
            symbol, bar, direction = key
            max_entries = dynamic_row.max_entries_per_trend if dynamic_row else fixed_row.max_entries_per_trend

        rows.append(
            {
                "symbol": symbol,
                "bar": bar,
                "bar_label": bar_label(bar),
                "direction": direction,
                "max_entries_per_trend": max_entries,
                "max_entries_label": format_max_entries_label(int(max_entries)),
                "dynamic_param": dynamic_row.param_label if dynamic_row else "-",
                "dynamic_max_entries_per_trend": dynamic_row.max_entries_per_trend if dynamic_row else None,
                "dynamic_max_entries_label": (
                    format_max_entries_label(dynamic_row.max_entries_per_trend) if dynamic_row else "-"
                ),
                "dynamic_pnl": dynamic_row.total_pnl if dynamic_row else None,
                "dynamic_max_drawdown": dynamic_row.max_drawdown if dynamic_row else None,
                "fixed_param": fixed_row.param_label if fixed_row else "-",
                "fixed_max_entries_per_trend": fixed_row.max_entries_per_trend if fixed_row else None,
                "fixed_max_entries_label": (
                    format_max_entries_label(fixed_row.max_entries_per_trend) if fixed_row else "-"
                ),
                "fixed_pnl": fixed_row.total_pnl if fixed_row else None,
                "fixed_max_drawdown": fixed_row.max_drawdown if fixed_row else None,
                "winner": winner,
                "pnl_gap": pnl_gap,
            }
        )
    return rows


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


def sort_comparison_key(item: tuple[object, ...], include_limit_in_key: bool) -> tuple[int, int, int, int]:
    if include_limit_in_key:
        symbol, bar, direction, max_entries = item
        return (max_entries_order(int(max_entries)), timeframe_order(str(bar)), symbol_order(str(symbol)), direction_order(str(direction)))
    symbol, bar, direction = item
    return (timeframe_order(str(bar)), symbol_order(str(symbol)), direction_order(str(direction)), 0)


def sort_record(row: RunRecord) -> tuple[int, int, int, int, int]:
    return (
        timeframe_order(row.bar),
        symbol_order(row.symbol),
        direction_order(row.direction),
        max_entries_order(row.max_entries_per_trend),
        row.entry_reference_ema,
    )


def sort_record_with_limit_first(row: RunRecord) -> tuple[int, int, int, int, int]:
    return (
        max_entries_order(row.max_entries_per_trend),
        timeframe_order(row.bar),
        symbol_order(row.symbol),
        direction_order(row.direction),
        row.entry_reference_ema,
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
        payload["take_ratio"] = format_decimal(record.take_ratio)
        payload["take_atr"] = format_decimal(record.take_atr)
        payload["max_entries_label"] = format_max_entries_label(record.max_entries_per_trend)
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


def format_comparison_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    formatted: list[dict[str, object]] = []
    for item in rows:
        formatted.append(
            {
                "symbol": item["symbol"],
                "bar": item["bar"],
                "bar_label": item["bar_label"],
                "direction": item["direction"],
                "max_entries_per_trend": item["max_entries_per_trend"],
                "max_entries_label": item["max_entries_label"],
                "dynamic_param": item["dynamic_param"],
                "dynamic_max_entries_per_trend": item["dynamic_max_entries_per_trend"],
                "dynamic_max_entries_label": item["dynamic_max_entries_label"],
                "dynamic_pnl": format_decimal_fixed(item["dynamic_pnl"], 4),
                "dynamic_max_drawdown": format_decimal_fixed(item["dynamic_max_drawdown"], 4),
                "fixed_param": item["fixed_param"],
                "fixed_max_entries_per_trend": item["fixed_max_entries_per_trend"],
                "fixed_max_entries_label": item["fixed_max_entries_label"],
                "fixed_pnl": format_decimal_fixed(item["fixed_pnl"], 4),
                "fixed_max_drawdown": format_decimal_fixed(item["fixed_max_drawdown"], 4),
                "winner": item["winner"],
                "pnl_gap": format_decimal_fixed(item["pnl_gap"], 4),
            }
        )
    return formatted

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


def build_limit_summary_rows(dynamic_records: list[RunRecord], fixed_records: list[RunRecord]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for mode, records in (("dynamic", dynamic_records), ("fixed", fixed_records)):
        for max_entries in MAX_ENTRIES_OPTIONS:
            scoped = [row for row in records if row.max_entries_per_trend == max_entries]
            summary = summarize_records(scoped)
            rows.append(
                {
                    "mode": mode,
                    "mode_label": mode_label(mode),
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


def format_limit_summary_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    formatted: list[dict[str, object]] = []
    for item in rows:
        formatted.append(
            {
                "mode": item["mode"],
                "mode_label": item["mode_label"],
                "max_entries_per_trend": item["max_entries_per_trend"],
                "max_entries_label": item["max_entries_label"],
                "runs": item["runs"],
                "profitable_runs": item["profitable_runs"],
                "avg_total_pnl": format_decimal_fixed(item["avg_total_pnl"], 4),
                "avg_max_drawdown": format_decimal_fixed(item["avg_max_drawdown"], 4),
                "avg_total_trades": format_decimal_fixed(item["avg_total_trades"], 2),
                "avg_win_rate": format_decimal_fixed(item["avg_win_rate"], 2),
                "best_total_pnl": format_decimal_fixed(item["best_total_pnl"], 4),
                "worst_total_pnl": format_decimal_fixed(item["worst_total_pnl"], 4),
            }
        )
    return formatted


def build_best_limit_distribution_rows(dynamic_best: list[RunRecord], fixed_best: list[RunRecord]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for mode, records in (("dynamic", dynamic_best), ("fixed", fixed_best)):
        total = len(records)
        counts = {value: 0 for value in MAX_ENTRIES_OPTIONS}
        for record in records:
            counts[record.max_entries_per_trend] = counts.get(record.max_entries_per_trend, 0) + 1
        for value in MAX_ENTRIES_OPTIONS:
            count = counts.get(value, 0)
            pct = (Decimal(count) * Decimal("100") / Decimal(total)) if total else None
            rows.append(
                {
                    "mode": mode,
                    "mode_label": mode_label(mode),
                    "max_entries_per_trend": value,
                    "max_entries_label": format_max_entries_label(value),
                    "count": count,
                    "pct_of_best": pct,
                }
            )
    return rows


def format_best_limit_distribution_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    formatted: list[dict[str, object]] = []
    for item in rows:
        formatted.append(
            {
                "mode": item["mode"],
                "mode_label": item["mode_label"],
                "max_entries_per_trend": item["max_entries_per_trend"],
                "max_entries_label": item["max_entries_label"],
                "count": item["count"],
                "pct_of_best": format_decimal_fixed(item["pct_of_best"], 2),
            }
        )
    return formatted


def build_limit_winner_summary_rows(comparisons_by_limit: list[dict[str, object]]) -> list[dict[str, object]]:
    stats = {
        value: {"dynamic": 0, "fixed": 0, "tie": 0, "total": 0}
        for value in MAX_ENTRIES_OPTIONS
    }
    for item in comparisons_by_limit:
        value = int(item["max_entries_per_trend"])
        winner = str(item["winner"])
        stats[value]["total"] += 1
        if winner in stats[value]:
            stats[value][winner] += 1
    rows: list[dict[str, object]] = []
    for value in MAX_ENTRIES_OPTIONS:
        total = stats[value]["total"]
        fixed_win_rate = (Decimal(stats[value]["fixed"]) * Decimal("100") / Decimal(total)) if total else None
        rows.append(
            {
                "max_entries_per_trend": value,
                "max_entries_label": format_max_entries_label(value),
                "total_groups": total,
                "dynamic_wins": stats[value]["dynamic"],
                "fixed_wins": stats[value]["fixed"],
                "ties": stats[value]["tie"],
                "fixed_win_rate_pct": fixed_win_rate,
            }
        )
    return rows


def format_limit_winner_summary_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    formatted: list[dict[str, object]] = []
    for item in rows:
        formatted.append(
            {
                "max_entries_per_trend": item["max_entries_per_trend"],
                "max_entries_label": item["max_entries_label"],
                "total_groups": item["total_groups"],
                "dynamic_wins": item["dynamic_wins"],
                "fixed_wins": item["fixed_wins"],
                "ties": item["ties"],
                "fixed_win_rate_pct": format_decimal_fixed(item["fixed_win_rate_pct"], 2),
            }
        )
    return formatted


def build_top_rows_by_limit(dynamic_records: list[RunRecord], fixed_records: list[RunRecord]) -> list[RunRecord]:
    rows: list[RunRecord] = []
    for mode, records in (("dynamic", dynamic_records), ("fixed", fixed_records)):
        for max_entries in MAX_ENTRIES_OPTIONS:
            scoped = [row for row in records if row.max_entries_per_trend == max_entries]
            if not scoped:
                continue
            rows.append(max(scoped, key=lambda row: row.total_pnl))
    return sorted(rows, key=lambda row: (row.mode, max_entries_order(row.max_entries_per_trend), -row.total_pnl))


def summarize_entry_choices(records: list[RunRecord]) -> dict[str, int]:
    counts = {"ema21": 0, "ema55": 0}
    for record in records:
        if record.entry_reference_ema == 21:
            counts["ema21"] += 1
        elif record.entry_reference_ema == 55:
            counts["ema55"] += 1
    return counts


def markdown_table(headers: list[str], rows: list[list[str]]) -> str:
    if not rows:
        return "_无数据_\n"
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    return "\n".join(lines) + "\n"


def best_limit_row(limit_summary_rows: list[dict[str, object]], *, mode: str) -> dict[str, object]:
    scoped = [row for row in limit_summary_rows if row["mode"] == mode]
    return max(scoped, key=lambda row: (row["avg_total_pnl"] is not None, row["avg_total_pnl"] or Decimal("-999999999")))


def strongest_distribution_row(distribution_rows: list[dict[str, object]], *, mode: str) -> dict[str, object]:
    scoped = [row for row in distribution_rows if row["mode"] == mode]
    return max(scoped, key=lambda row: (row["count"], -(row["max_entries_per_trend"] if row["max_entries_per_trend"] is not None else 9999)))


def strongest_fixed_challenge_row(limit_winner_rows: list[dict[str, object]]) -> dict[str, object]:
    return max(limit_winner_rows, key=lambda row: (row["fixed_wins"], -(row["max_entries_per_trend"] if row["max_entries_per_trend"] is not None else 9999)))

def build_report(
    *,
    exported_at: datetime,
    dynamic_records: list[RunRecord],
    fixed_records: list[RunRecord],
    failures: list[str],
    exports: ExportBundle,
) -> str:
    dynamic_best = sorted(pick_best_records(dynamic_records, lambda row: row.mode_key), key=sort_record)
    fixed_best = sorted(pick_best_records(fixed_records, lambda row: row.mode_key), key=sort_record)
    dynamic_best_by_limit = sorted(
        pick_best_records(dynamic_records, lambda row: row.mode_key_with_entries),
        key=sort_record_with_limit_first,
    )
    fixed_best_by_limit = sorted(
        pick_best_records(fixed_records, lambda row: row.mode_key_with_entries),
        key=sort_record_with_limit_first,
    )
    comparisons = build_mode_comparisons(dynamic_best, fixed_best, include_limit_in_key=False)
    comparisons_by_limit = build_mode_comparisons(dynamic_best_by_limit, fixed_best_by_limit, include_limit_in_key=True)

    dynamic_summary = summarize_records(dynamic_records)
    fixed_summary = summarize_records(fixed_records)
    limit_summary_rows = build_limit_summary_rows(dynamic_records, fixed_records)
    best_limit_distribution_rows = build_best_limit_distribution_rows(dynamic_best, fixed_best)
    limit_winner_rows = build_limit_winner_summary_rows(comparisons_by_limit)
    top_by_limit_rows = build_top_rows_by_limit(dynamic_records, fixed_records)

    overall_winners = {"dynamic": 0, "fixed": 0, "tie": 0}
    for item in comparisons:
        winner = str(item["winner"])
        if winner in overall_winners:
            overall_winners[winner] += 1

    dynamic_entry_stats = summarize_entry_choices(dynamic_best)
    fixed_entry_stats = summarize_entry_choices(fixed_best)

    dynamic_best_limit = best_limit_row(limit_summary_rows, mode="dynamic")
    fixed_best_limit = best_limit_row(limit_summary_rows, mode="fixed")
    dynamic_distribution_peak = strongest_distribution_row(best_limit_distribution_rows, mode="dynamic")
    fixed_distribution_peak = strongest_distribution_row(best_limit_distribution_rows, mode="fixed")
    fixed_challenge_peak = strongest_fixed_challenge_row(limit_winner_rows)

    limit_summary_table = markdown_table(
        ["模式", "每波开仓次数", "运行数", "盈利组合数", "平均总盈亏", "平均回撤", "平均交易数", "平均胜率", "最佳", "最差"],
        [
            [
                str(row["mode_label"]),
                str(row["max_entries_label"]),
                str(row["runs"]),
                str(row["profitable_runs"]),
                format_decimal_fixed(row["avg_total_pnl"], 4),
                format_decimal_fixed(row["avg_max_drawdown"], 4),
                format_decimal_fixed(row["avg_total_trades"], 2),
                f"{format_decimal_fixed(row['avg_win_rate'], 2)}%",
                format_decimal_fixed(row["best_total_pnl"], 4),
                format_decimal_fixed(row["worst_total_pnl"], 4),
            ]
            for row in limit_summary_rows
        ],
    )

    best_limit_distribution_table = markdown_table(
        ["模式", "每波开仓次数", "在最终最佳组合中出现次数", "占比"],
        [
            [
                str(row["mode_label"]),
                str(row["max_entries_label"]),
                str(row["count"]),
                f"{format_decimal_fixed(row['pct_of_best'], 2)}%",
            ]
            for row in best_limit_distribution_rows
        ],
    )

    limit_winner_table = markdown_table(
        ["每波开仓次数", "对比组数", "动态胜出", "固定胜出", "平局", "固定胜率"],
        [
            [
                str(row["max_entries_label"]),
                str(row["total_groups"]),
                str(row["dynamic_wins"]),
                str(row["fixed_wins"]),
                str(row["ties"]),
                f"{format_decimal_fixed(row['fixed_win_rate_pct'], 2)}%",
            ]
            for row in limit_winner_rows
        ],
    )

    dynamic_table = markdown_table(
        ["周期", "币种", "方向", "最佳参数", "总盈亏", "胜率", "交易数", "最大回撤"],
        [
            [
                row.bar_label,
                row.symbol,
                row.direction,
                row.param_label,
                format_decimal_fixed(row.total_pnl, 4),
                f"{format_decimal_fixed(row.win_rate, 2)}%",
                str(row.total_trades),
                format_decimal_fixed(row.max_drawdown, 4),
            ]
            for row in dynamic_best
        ],
    )

    fixed_table = markdown_table(
        ["周期", "币种", "方向", "最佳参数", "总盈亏", "胜率", "交易数", "最大回撤"],
        [
            [
                row.bar_label,
                row.symbol,
                row.direction,
                row.param_label,
                format_decimal_fixed(row.total_pnl, 4),
                f"{format_decimal_fixed(row.win_rate, 2)}%",
                str(row.total_trades),
                format_decimal_fixed(row.max_drawdown, 4),
            ]
            for row in fixed_best
        ],
    )

    comparison_table = markdown_table(
        ["周期", "币种", "方向", "动态最佳", "动态总盈亏", "固定最佳", "固定总盈亏", "胜出模式", "差值"],
        [
            [
                str(item["bar_label"]),
                str(item["symbol"]),
                str(item["direction"]),
                str(item["dynamic_param"]),
                format_decimal_fixed(item["dynamic_pnl"], 4),
                str(item["fixed_param"]),
                format_decimal_fixed(item["fixed_pnl"], 4),
                str(item["winner"]),
                format_decimal_fixed(item["pnl_gap"], 4),
            ]
            for item in comparisons
        ],
    )

    top_by_limit_table = markdown_table(
        ["模式", "每波开仓次数", "周期", "币种", "方向", "参数", "总盈亏", "胜率", "交易数", "最大回撤"],
        [
            [
                mode_label(row.mode),
                format_max_entries_label(row.max_entries_per_trend),
                row.bar_label,
                row.symbol,
                row.direction,
                row.param_label,
                format_decimal_fixed(row.total_pnl, 4),
                f"{format_decimal_fixed(row.win_rate, 2)}%",
                str(row.total_trades),
                format_decimal_fixed(row.max_drawdown, 4),
            ]
            for row in top_by_limit_rows
        ],
    )

    top_rows = sorted(dynamic_records + fixed_records, key=lambda row: row.total_pnl, reverse=True)[:20]
    top_table = markdown_table(
        ["模式", "周期", "币种", "方向", "参数", "总盈亏", "胜率", "交易数", "最大回撤"],
        [
            [
                mode_label(row.mode),
                row.bar_label,
                row.symbol,
                row.direction,
                row.param_label,
                format_decimal_fixed(row.total_pnl, 4),
                f"{format_decimal_fixed(row.win_rate, 2)}%",
                str(row.total_trades),
                format_decimal_fixed(row.max_drawdown, 4),
            ]
            for row in top_rows
        ],
    )

    lines = [
        "# 全面回测报告（含每波开仓次数 0/1/2/3）",
        "",
        f"导出时间：{exported_at.strftime('%Y-%m-%d %H:%M:%S')}",
        f"回测范围：{', '.join(symbol.replace('-USDT-SWAP', '') for symbol in SYMBOLS)} | "
        f"{' / '.join(label for _, label in TIMEFRAMES)} | 10000根K线 | 多空分开",
        f"基础参数：EMA{EMA_PERIOD} / 趋势EMA{TREND_EMA_PERIOD} / ATR{ATR_PERIOD} / 单笔风险 {format_decimal(RISK_AMOUNT)} / "
        f"初始资金 {format_decimal(INITIAL_CAPITAL)}",
        f"手续费：Maker {format_decimal_fixed(MAKER_FEE_RATE * Decimal('100'), 4)}% / "
        f"Taker {format_decimal_fixed(TAKER_FEE_RATE * Decimal('100'), 4)}%",
        "",
        "## 本次新增变量",
        "",
        "- 每波开仓次数分别测试：不限(0) / 1 / 2 / 3",
        "- 这个参数用于限制单一趋势中的连续开仓次数，重点验证‘趋势延续收益’与‘反转吞噬风险’之间的取舍。",
        "",
        "## 动态止盈口径",
        "",
        "- 挂单参考EMA：EMA21、EMA55",
        "- 止损：1 / 1.5 / 2 ATR",
        "- 止盈：动态止盈规则",
        "- 每波开仓次数：不限(0) / 1 / 2 / 3",
        "",
        "## 固定止盈口径",
        "",
        "- 挂单参考EMA：EMA21、EMA55",
        "- 止损：1 / 1.5 / 2 ATR",
        "- 止盈：1x / 2x / 3x 止损，共9组",
        "- 每波开仓次数：不限(0) / 1 / 2 / 3",
        "",
        "## 总览",
        "",
        f"- 动态止盈运行数：{dynamic_summary['runs']}，盈利组合：{dynamic_summary['profitable_runs']}，平均总盈亏：{format_decimal_fixed(dynamic_summary['avg_total_pnl'], 4)}",
        f"- 固定止盈运行数：{fixed_summary['runs']}，盈利组合：{fixed_summary['profitable_runs']}，平均总盈亏：{format_decimal_fixed(fixed_summary['avg_total_pnl'], 4)}",
        f"- 最终最佳 vs 最终最佳：动态胜出 {overall_winners['dynamic']} 组，固定胜出 {overall_winners['fixed']} 组，平局 {overall_winners['tie']} 组",
        f"- 动态止盈按平均总盈亏最优的开仓次数：{dynamic_best_limit['max_entries_label']}，平均总盈亏 {format_decimal_fixed(dynamic_best_limit['avg_total_pnl'], 4)}",
        f"- 固定止盈按平均总盈亏最优的开仓次数：{fixed_best_limit['max_entries_label']}，平均总盈亏 {format_decimal_fixed(fixed_best_limit['avg_total_pnl'], 4)}",
        f"- 在最终40个动态最佳组合中，出现最多的开仓次数是 {dynamic_distribution_peak['max_entries_label']}，共 {dynamic_distribution_peak['count']} 组",
        f"- 在最终40个固定最佳组合中，出现最多的开仓次数是 {fixed_distribution_peak['max_entries_label']}，共 {fixed_distribution_peak['count']} 组",
        f"- 固定止盈最能挑战动态止盈的开仓次数是 {fixed_challenge_peak['max_entries_label']}，在同限额对比里胜出 {fixed_challenge_peak['fixed_wins']} 组",
        "",
        "## 开仓次数分组概览",
        "",
        limit_summary_table,
        "## 最终最佳组合中的开仓次数分布",
        "",
        best_limit_distribution_table,
        "## 同一开仓次数下：动态 vs 固定",
        "",
        limit_winner_table,
        "## 各开仓次数下的最强组合",
        "",
        top_by_limit_table,
        "## 动态止盈最终最佳结果",
        "",
        dynamic_table,
        "## 固定止盈最终最佳结果",
        "",
        fixed_table,
        "## 最终最佳：动态 vs 固定",
        "",
        comparison_table,
        "## 全局前20",
        "",
        top_table,
        "## EMA挂单偏好",
        "",
        f"- 动态止盈最终最佳组合中：EMA21胜出 {dynamic_entry_stats['ema21']} 组，EMA55胜出 {dynamic_entry_stats['ema55']} 组",
        f"- 固定止盈最终最佳组合中：EMA21胜出 {fixed_entry_stats['ema21']} 组，EMA55胜出 {fixed_entry_stats['ema55']} 组",
        "",
        "## 交易员视角",
        "",
        f"- 如果限制开仓次数（1/2/3）整体优于不限(0)，说明这个市场阶段更需要防止趋势尾端反复追单；如果不限(0)更强，则说明趋势延续性更足。当前动态止盈的平均最优开仓次数是 {dynamic_best_limit['max_entries_label']}，固定止盈的平均最优开仓次数是 {fixed_best_limit['max_entries_label']}。",
        f"- 从最终最佳组合分布看，动态止盈更常落在 {dynamic_distribution_peak['max_entries_label']} 档，固定止盈更常落在 {fixed_distribution_peak['max_entries_label']} 档，这能帮助实盘先把测试集中到更可能有效的开仓次数上。",
        f"- 固定止盈在同一开仓次数下最接近动态的是 {fixed_challenge_peak['max_entries_label']} 档。如果固定止盈要保留成对照组，优先测试这档更有意义。",
        "",
        "## 数据文件",
        "",
        f"- 全量结果：`{exports.all_runs_csv}`",
        f"- 动态最佳：`{exports.dynamic_best_csv}`",
        f"- 固定最佳：`{exports.fixed_best_csv}`",
        f"- 最终模式对比：`{exports.comparison_csv}`",
        f"- 动态最佳（按开仓次数拆分）：`{exports.dynamic_best_by_limit_csv}`",
        f"- 固定最佳（按开仓次数拆分）：`{exports.fixed_best_by_limit_csv}`",
        f"- 同一开仓次数模式对比：`{exports.comparison_by_limit_csv}`",
        f"- 开仓次数汇总：`{exports.limit_summary_csv}`",
        f"- 最佳组合开仓次数分布：`{exports.best_limit_distribution_csv}`",
    ]

    if failures:
        lines.extend(
            [
                "",
                "## 失败记录",
                "",
                f"- 共 {len(failures)} 条",
            ]
        )
        lines.extend(f"- {item}" for item in failures[:100])

    return "\n".join(lines).rstrip() + "\n"


def build_text_report(
    *,
    exported_at: datetime,
    dynamic_records: list[RunRecord],
    fixed_records: list[RunRecord],
    failures: list[str],
    exports: ExportBundle,
) -> str:
    dynamic_best = sorted(pick_best_records(dynamic_records, lambda row: row.mode_key), key=sort_record)
    fixed_best = sorted(pick_best_records(fixed_records, lambda row: row.mode_key), key=sort_record)
    dynamic_best_by_limit = sorted(
        pick_best_records(dynamic_records, lambda row: row.mode_key_with_entries),
        key=sort_record_with_limit_first,
    )
    fixed_best_by_limit = sorted(
        pick_best_records(fixed_records, lambda row: row.mode_key_with_entries),
        key=sort_record_with_limit_first,
    )
    comparisons = build_mode_comparisons(dynamic_best, fixed_best, include_limit_in_key=False)
    comparisons_by_limit = build_mode_comparisons(dynamic_best_by_limit, fixed_best_by_limit, include_limit_in_key=True)
    dynamic_summary = summarize_records(dynamic_records)
    fixed_summary = summarize_records(fixed_records)
    limit_summary_rows = build_limit_summary_rows(dynamic_records, fixed_records)
    best_limit_distribution_rows = build_best_limit_distribution_rows(dynamic_best, fixed_best)
    limit_winner_rows = build_limit_winner_summary_rows(comparisons_by_limit)
    top_by_limit_rows = build_top_rows_by_limit(dynamic_records, fixed_records)
    top_rows = sorted(dynamic_records + fixed_records, key=lambda row: row.total_pnl, reverse=True)[:20]

    overall_winners = {"dynamic": 0, "fixed": 0, "tie": 0}
    for item in comparisons:
        winner = str(item["winner"])
        if winner in overall_winners:
            overall_winners[winner] += 1

    dynamic_entry_stats = summarize_entry_choices(dynamic_best)
    fixed_entry_stats = summarize_entry_choices(fixed_best)

    dynamic_best_limit = best_limit_row(limit_summary_rows, mode="dynamic")
    fixed_best_limit = best_limit_row(limit_summary_rows, mode="fixed")
    dynamic_distribution_peak = strongest_distribution_row(best_limit_distribution_rows, mode="dynamic")
    fixed_distribution_peak = strongest_distribution_row(best_limit_distribution_rows, mode="fixed")
    fixed_challenge_peak = strongest_fixed_challenge_row(limit_winner_rows)

    lines = [
        "新版全面回测总报告（双滑点口径）",
        "",
        f"导出时间：{exported_at.strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "一、统一口径",
        f"1. 回测范围：{', '.join(symbol.replace('-USDT-SWAP', '') for symbol in SYMBOLS)}；{' / '.join(label for _, label in TIMEFRAMES)}；10000根K线；多空分开。",
        f"2. 基础参数：EMA{EMA_PERIOD}；趋势EMA{TREND_EMA_PERIOD}；ATR{ATR_PERIOD}；单笔风险 {format_decimal(RISK_AMOUNT)}；初始资金 {format_decimal(INITIAL_CAPITAL)}。",
        f"3. 手续费：Maker {format_rate_pct(MAKER_FEE_RATE)}%；Taker {format_rate_pct(TAKER_FEE_RATE)}%。",
        f"4. 双滑点口径：开仓滑点 {format_rate_pct(ENTRY_SLIPPAGE_RATE)}%；平仓滑点 {format_rate_pct(EXIT_SLIPPAGE_RATE)}%。",
        "5. 双滑点说明：当前EMA动态挂单策略按等待单入场，开仓侧不额外加滑点；平仓侧统一按平仓滑点计入。",
        f"6. 2R保本开关：{'开启' if DYNAMIC_TWO_R_BREAK_EVEN else '关闭'}。",
        f"7. 手续费偏移开关：{'开启' if DYNAMIC_FEE_OFFSET_ENABLED else '关闭'}。",
        "8. 动态止盈测试：挂单EMA 21/55；止损 1 / 1.5 / 2 ATR；每波开仓次数 0 / 1 / 2 / 3。",
        "9. 固定止盈测试：挂单EMA 21/55；止损 1 / 1.5 / 2 ATR；止盈 1 / 2 / 3 倍止损；每波开仓次数 0 / 1 / 2 / 3。",
        "",
        "二、执行概览",
        f"1. 动态止盈运行数：{dynamic_summary['runs']}；盈利组合：{dynamic_summary['profitable_runs']}；平均总盈亏：{format_decimal_fixed(dynamic_summary['avg_total_pnl'], 4)}；平均回撤：{format_decimal_fixed(dynamic_summary['avg_max_drawdown'], 4)}；平均胜率：{format_decimal_fixed(dynamic_summary['avg_win_rate'], 2)}%。",
        f"2. 固定止盈运行数：{fixed_summary['runs']}；盈利组合：{fixed_summary['profitable_runs']}；平均总盈亏：{format_decimal_fixed(fixed_summary['avg_total_pnl'], 4)}；平均回撤：{format_decimal_fixed(fixed_summary['avg_max_drawdown'], 4)}；平均胜率：{format_decimal_fixed(fixed_summary['avg_win_rate'], 2)}%。",
        f"3. 最终最佳对最终最佳：动态胜出 {overall_winners['dynamic']} 组；固定胜出 {overall_winners['fixed']} 组；平局 {overall_winners['tie']} 组。",
        f"4. 动态止盈平均总盈亏最优的每波开仓次数：{dynamic_best_limit['max_entries_label']}；平均总盈亏 {format_decimal_fixed(dynamic_best_limit['avg_total_pnl'], 4)}。",
        f"5. 固定止盈平均总盈亏最优的每波开仓次数：{fixed_best_limit['max_entries_label']}；平均总盈亏 {format_decimal_fixed(fixed_best_limit['avg_total_pnl'], 4)}。",
        f"6. 动态止盈最终最佳组合里，出现最多的开仓次数是 {dynamic_distribution_peak['max_entries_label']}，共 {dynamic_distribution_peak['count']} 组。",
        f"7. 固定止盈最终最佳组合里，出现最多的开仓次数是 {fixed_distribution_peak['max_entries_label']}，共 {fixed_distribution_peak['count']} 组。",
        f"8. 固定止盈最能挑战动态止盈的开仓次数是 {fixed_challenge_peak['max_entries_label']}，同限额对比里固定胜出 {fixed_challenge_peak['fixed_wins']} 组。",
        "",
        "三、每波开仓次数分组结论",
    ]

    for row in limit_summary_rows:
        lines.append(
            f"{mode_label(str(row['mode']))} | 每波开仓{row['max_entries_label']} | "
            f"运行数 {row['runs']} | 盈利组合 {row['profitable_runs']} | "
            f"平均总盈亏 {format_decimal_fixed(row['avg_total_pnl'], 4)} | "
            f"平均回撤 {format_decimal_fixed(row['avg_max_drawdown'], 4)} | "
            f"平均交易数 {format_decimal_fixed(row['avg_total_trades'], 2)} | "
            f"平均胜率 {format_decimal_fixed(row['avg_win_rate'], 2)}% | "
            f"最佳 {format_decimal_fixed(row['best_total_pnl'], 4)} | "
            f"最差 {format_decimal_fixed(row['worst_total_pnl'], 4)}"
        )

    lines.extend(
        [
            "",
            "四、最终最佳组合中的开仓次数分布",
        ]
    )
    for row in best_limit_distribution_rows:
        lines.append(
            f"{mode_label(str(row['mode']))} | 每波开仓{row['max_entries_label']} | "
            f"出现 {row['count']} 次 | 占比 {format_decimal_fixed(row['pct_of_best'], 2)}%"
        )

    lines.extend(
        [
            "",
            "五、同一开仓次数下的模式胜负",
        ]
    )
    for row in limit_winner_rows:
        lines.append(
            f"每波开仓{row['max_entries_label']} | 对比组数 {row['total_groups']} | "
            f"动态胜出 {row['dynamic_wins']} | 固定胜出 {row['fixed_wins']} | "
            f"平局 {row['ties']} | 固定胜率 {format_decimal_fixed(row['fixed_win_rate_pct'], 2)}%"
        )

    lines.extend(
        [
            "",
            "六、各开仓次数下的最强组合",
        ]
    )
    for row in top_by_limit_rows:
        lines.append(
            f"{mode_label(row.mode)} | 每波开仓{format_max_entries_label(row.max_entries_per_trend)} | "
            f"{row.bar_label} | {row.symbol} | {row.direction} | {row.param_label} | "
            f"总盈亏 {format_decimal_fixed(row.total_pnl, 4)} | 胜率 {format_decimal_fixed(row.win_rate, 2)}% | "
            f"交易数 {row.total_trades} | 最大回撤 {format_decimal_fixed(row.max_drawdown, 4)}"
        )

    lines.extend(
        [
            "",
            "七、动态止盈最终最佳清单",
        ]
    )
    for row in dynamic_best:
        lines.append(
            f"{row.bar_label} | {row.symbol} | {row.direction} | {row.param_label} | "
            f"总盈亏 {format_decimal_fixed(row.total_pnl, 4)} | 胜率 {format_decimal_fixed(row.win_rate, 2)}% | "
            f"交易数 {row.total_trades} | 最大回撤 {format_decimal_fixed(row.max_drawdown, 4)}"
        )

    lines.extend(
        [
            "",
            "八、固定止盈最终最佳清单",
        ]
    )
    for row in fixed_best:
        lines.append(
            f"{row.bar_label} | {row.symbol} | {row.direction} | {row.param_label} | "
            f"总盈亏 {format_decimal_fixed(row.total_pnl, 4)} | 胜率 {format_decimal_fixed(row.win_rate, 2)}% | "
            f"交易数 {row.total_trades} | 最大回撤 {format_decimal_fixed(row.max_drawdown, 4)}"
        )

    lines.extend(
        [
            "",
            "九、最终最佳：动态止盈 vs 固定止盈",
        ]
    )
    for item in comparisons:
        lines.append(
            f"{item['bar_label']} | {item['symbol']} | {item['direction']} | "
            f"动态：{item['dynamic_param']} | 动态总盈亏 {format_decimal_fixed(item['dynamic_pnl'], 4)} | "
            f"固定：{item['fixed_param']} | 固定总盈亏 {format_decimal_fixed(item['fixed_pnl'], 4)} | "
            f"胜出模式 {item['winner']} | 差值 {format_decimal_fixed(item['pnl_gap'], 4)}"
        )

    lines.extend(
        [
            "",
            "十、全局前20",
        ]
    )
    for row in top_rows:
        lines.append(
            f"{mode_label(row.mode)} | {row.bar_label} | {row.symbol} | {row.direction} | {row.param_label} | "
            f"总盈亏 {format_decimal_fixed(row.total_pnl, 4)} | 胜率 {format_decimal_fixed(row.win_rate, 2)}% | "
            f"交易数 {row.total_trades} | 最大回撤 {format_decimal_fixed(row.max_drawdown, 4)}"
        )

    lines.extend(
        [
            "",
            "十一、EMA挂单偏好",
            f"1. 动态止盈最终最佳组合中：EMA21 胜出 {dynamic_entry_stats['ema21']} 组；EMA55 胜出 {dynamic_entry_stats['ema55']} 组。",
            f"2. 固定止盈最终最佳组合中：EMA21 胜出 {fixed_entry_stats['ema21']} 组；EMA55 胜出 {fixed_entry_stats['ema55']} 组。",
            "",
            "十二、交易员视角结论",
            f"1. 从本次双滑点口径看，动态止盈整体平均结果为 {format_decimal_fixed(dynamic_summary['avg_total_pnl'], 4)}，固定止盈整体平均结果为 {format_decimal_fixed(fixed_summary['avg_total_pnl'], 4)}。先看整体均值，再看你关心的币种、周期和方向。",
            f"2. 动态止盈平均最优的开仓次数是 {dynamic_best_limit['max_entries_label']}，固定止盈平均最优的开仓次数是 {fixed_best_limit['max_entries_label']}。这说明“每波开仓次数”仍然是非常关键的收益/回撤调节器。",
            f"3. 动态止盈最终最佳更偏向 {dynamic_distribution_peak['max_entries_label']} 档；固定止盈最终最佳更偏向 {fixed_distribution_peak['max_entries_label']} 档。实盘优先回放这些档位，效率最高。",
            f"4. 固定止盈最有竞争力的是每波开仓 {fixed_challenge_peak['max_entries_label']} 档。如果实盘要保留固定止盈做对照，优先保留这档。",
            "5. 当前双滑点口径下，开仓滑点被明确拆出，但对本策略的等待单入场不产生实际扣减；真正影响结果的是平仓滑点。因此，这版报告更接近该策略的真实执行逻辑，也避免了此前“加滑点反而更好”的失真。",
            "",
            "十三、数据文件",
            f"1. 全量结果：{exports.all_runs_csv}",
            f"2. 动态最佳：{exports.dynamic_best_csv}",
            f"3. 固定最佳：{exports.fixed_best_csv}",
            f"4. 最终模式对比：{exports.comparison_csv}",
            f"5. 动态最佳（按开仓次数拆分）：{exports.dynamic_best_by_limit_csv}",
            f"6. 固定最佳（按开仓次数拆分）：{exports.fixed_best_by_limit_csv}",
            f"7. 同一开仓次数模式对比：{exports.comparison_by_limit_csv}",
            f"8. 开仓次数汇总：{exports.limit_summary_csv}",
            f"9. 最佳组合开仓次数分布：{exports.best_limit_distribution_csv}",
        ]
    )

    if failures:
        lines.extend(["", "十四、失败记录", f"共 {len(failures)} 条"])
        lines.extend(failures[:100])

    return "\n".join(lines).rstrip() + "\n"


def write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> None:
    exported_at = datetime.now()
    stamp = exported_at.strftime("%Y%m%d_%H%M%S")
    client = OkxRestClient()

    exports = ExportBundle(
        all_runs_csv=REPORTS_DIR / f"新版全面回测_全量结果_双滑点口径_{stamp}.csv",
        dynamic_best_csv=REPORTS_DIR / f"新版全面回测_动态最佳_双滑点口径_{stamp}.csv",
        fixed_best_csv=REPORTS_DIR / f"新版全面回测_固定最佳_双滑点口径_{stamp}.csv",
        comparison_csv=REPORTS_DIR / f"新版全面回测_动态固定对比_双滑点口径_{stamp}.csv",
        dynamic_best_by_limit_csv=REPORTS_DIR / f"新版全面回测_动态最佳_按开仓次数_双滑点口径_{stamp}.csv",
        fixed_best_by_limit_csv=REPORTS_DIR / f"新版全面回测_固定最佳_按开仓次数_双滑点口径_{stamp}.csv",
        comparison_by_limit_csv=REPORTS_DIR / f"新版全面回测_同开仓次数模式对比_双滑点口径_{stamp}.csv",
        limit_summary_csv=REPORTS_DIR / f"新版全面回测_开仓次数汇总_双滑点口径_{stamp}.csv",
        best_limit_distribution_csv=REPORTS_DIR / f"新版全面回测_最佳组合开仓次数分布_双滑点口径_{stamp}.csv",
        report_txt=REPORTS_DIR / f"新版全面回测总报告_双滑点口径_{stamp}.txt",
        summary_json=REPORTS_DIR / f"新版全面回测摘要_双滑点口径_{stamp}.json",
    )

    market_cache = load_market_data(client)
    dynamic_records, dynamic_failures = run_dynamic_suite(market_cache)
    fixed_records, fixed_failures = run_fixed_suite(market_cache)
    failures = dynamic_failures + fixed_failures

    all_records = sorted(dynamic_records + fixed_records, key=lambda row: (row.mode, *sort_record(row)))
    dynamic_best = sorted(pick_best_records(dynamic_records, lambda row: row.mode_key), key=sort_record)
    fixed_best = sorted(pick_best_records(fixed_records, lambda row: row.mode_key), key=sort_record)
    dynamic_best_by_limit = sorted(
        pick_best_records(dynamic_records, lambda row: row.mode_key_with_entries),
        key=sort_record_with_limit_first,
    )
    fixed_best_by_limit = sorted(
        pick_best_records(fixed_records, lambda row: row.mode_key_with_entries),
        key=sort_record_with_limit_first,
    )
    comparisons = build_mode_comparisons(dynamic_best, fixed_best, include_limit_in_key=False)
    comparisons_by_limit = build_mode_comparisons(dynamic_best_by_limit, fixed_best_by_limit, include_limit_in_key=True)
    limit_summary_rows = build_limit_summary_rows(dynamic_records, fixed_records)
    best_limit_distribution_rows = build_best_limit_distribution_rows(dynamic_best, fixed_best)

    write_csv(exports.all_runs_csv, csv_rows(all_records))
    write_csv(exports.dynamic_best_csv, csv_rows(dynamic_best))
    write_csv(exports.fixed_best_csv, csv_rows(fixed_best))
    write_csv(exports.comparison_csv, format_comparison_rows(comparisons))
    write_csv(exports.dynamic_best_by_limit_csv, csv_rows(dynamic_best_by_limit))
    write_csv(exports.fixed_best_by_limit_csv, csv_rows(fixed_best_by_limit))
    write_csv(exports.comparison_by_limit_csv, format_comparison_rows(comparisons_by_limit))
    write_csv(exports.limit_summary_csv, format_limit_summary_rows(limit_summary_rows))
    write_csv(exports.best_limit_distribution_csv, format_best_limit_distribution_rows(best_limit_distribution_rows))

    exports.report_txt.write_text(
        build_text_report(
            exported_at=exported_at,
            dynamic_records=dynamic_records,
            fixed_records=fixed_records,
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
            "max_entries_options": list(MAX_ENTRIES_OPTIONS),
            "symbols": list(SYMBOLS),
            "timeframes": [value for value, _ in TIMEFRAMES],
            "all_runs_csv": str(exports.all_runs_csv),
            "dynamic_best_csv": str(exports.dynamic_best_csv),
            "fixed_best_csv": str(exports.fixed_best_csv),
            "comparison_csv": str(exports.comparison_csv),
            "dynamic_best_by_limit_csv": str(exports.dynamic_best_by_limit_csv),
            "fixed_best_by_limit_csv": str(exports.fixed_best_by_limit_csv),
            "comparison_by_limit_csv": str(exports.comparison_by_limit_csv),
            "limit_summary_csv": str(exports.limit_summary_csv),
            "best_limit_distribution_csv": str(exports.best_limit_distribution_csv),
            "report_txt": str(exports.report_txt),
            "failures": failures,
        },
    )

    print(f"总报告 -> {exports.report_txt}")
    print(f"全量结果 -> {exports.all_runs_csv}")
    print(f"动态最佳 -> {exports.dynamic_best_csv}")
    print(f"固定最佳 -> {exports.fixed_best_csv}")
    print(f"模式对比 -> {exports.comparison_csv}")
    print(f"动态最佳_按开仓次数 -> {exports.dynamic_best_by_limit_csv}")
    print(f"固定最佳_按开仓次数 -> {exports.fixed_best_by_limit_csv}")
    print(f"同开仓次数模式对比 -> {exports.comparison_by_limit_csv}")
    print(f"开仓次数汇总 -> {exports.limit_summary_csv}")
    print(f"最佳组合开仓次数分布 -> {exports.best_limit_distribution_csv}")
    print(f"摘要 -> {exports.summary_json}")


if __name__ == "__main__":
    main()
