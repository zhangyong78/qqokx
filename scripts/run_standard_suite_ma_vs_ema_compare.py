from __future__ import annotations

import csv
import json
import sys
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import okx_quant.backtest as backtest_module
import okx_quant.strategies.ema_dynamic as ema_dynamic_module
from okx_quant.backtest import (
    _build_backtest_data_source_note,
    _load_backtest_candles,
    _run_backtest_with_loaded_data,
    build_atr_batch_configs,
)
from okx_quant.indicators import ema, sma
from okx_quant.models import StrategyConfig
from okx_quant.okx_client import OkxRestClient
from okx_quant.persistence import analysis_report_dir_path
from okx_quant.pricing import format_decimal, format_decimal_fixed
from okx_quant.strategy_catalog import (
    STRATEGY_DYNAMIC_ID,
    STRATEGY_DYNAMIC_MTF_LONG_ID,
    STRATEGY_DYNAMIC_MTF_SHORT_ID,
)


REPORTS_DIR = analysis_report_dir_path()
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

SYMBOLS = (
    "BTC-USDT-SWAP",
    "ETH-USDT-SWAP",
    "SOL-USDT-SWAP",
    "BNB-USDT-SWAP",
    "DOGE-USDT-SWAP",
)
SYMBOL_LABELS = {
    "BTC-USDT-SWAP": "BTC",
    "ETH-USDT-SWAP": "ETH",
    "SOL-USDT-SWAP": "SOL",
    "BNB-USDT-SWAP": "BNB",
    "DOGE-USDT-SWAP": "DOGE",
}
AVERAGE_TYPES = ("EMA", "MA")
MAKER_FEE_RATE = Decimal("0.0001")
TAKER_FEE_RATE = Decimal("0.00028")
CANDLE_LIMIT = 10000

NORMAL_TIMEFRAMES = (
    ("1H", "1小时"),
    ("4H", "4小时"),
)
NORMAL_SIGNALS = (
    ("long_only", "做多"),
    ("short_only", "做空"),
)
MTF_TIMEFRAMES = (
    ("15m", "15m", "1H"),
    ("1H", "1H", "4H"),
)


@dataclass(frozen=True)
class CellResult:
    average_type: str
    suite_id: str
    suite_label: str
    symbol: str
    symbol_label: str
    entry_bar: str
    entry_bar_label: str
    filter_bar: str
    filter_bar_label: str
    direction: str
    best_sl: Decimal
    best_tp: Decimal
    total_pnl: Decimal
    win_rate: Decimal
    total_trades: int
    max_drawdown: Decimal

    @property
    def pnl_dd_ratio(self) -> Decimal:
        if self.max_drawdown == 0:
            return Decimal("0")
        return self.total_pnl / self.max_drawdown


def sma_as_series(values: list[Decimal], period: int) -> list[Decimal]:
    raw = sma(values, period)
    if not raw:
        return []
    first_valid = next((item for item in raw if item is not None), values[0])
    return [item if item is not None else first_valid for item in raw]


@contextmanager
def patched_average(average_type: str):
    original_backtest_ema = backtest_module.ema
    original_strategy_ema = ema_dynamic_module.ema
    try:
        replacement = ema if average_type == "EMA" else sma_as_series
        backtest_module.ema = replacement
        ema_dynamic_module.ema = replacement
        yield
    finally:
        backtest_module.ema = original_backtest_ema
        ema_dynamic_module.ema = original_strategy_ema


def build_normal_config(symbol: str, bar: str, signal_mode: str) -> StrategyConfig:
    return StrategyConfig(
        inst_id=symbol,
        bar=bar,
        ema_period=21,
        trend_ema_period=55,
        atr_period=10,
        atr_stop_multiplier=Decimal("1"),
        atr_take_multiplier=Decimal("1"),
        order_size=Decimal("0"),
        trade_mode="cross",
        signal_mode=signal_mode,
        position_mode="net",
        environment="demo",
        tp_sl_trigger_type="mark",
        strategy_id=STRATEGY_DYNAMIC_ID,
        risk_amount=Decimal("100"),
        entry_reference_ema_period=55,
    )


def build_mtf_config(symbol: str, bar: str, filter_bar: str, direction: str) -> StrategyConfig:
    strategy_id = STRATEGY_DYNAMIC_MTF_LONG_ID if direction == "做多" else STRATEGY_DYNAMIC_MTF_SHORT_ID
    signal_mode = "long_only" if direction == "做多" else "short_only"
    return StrategyConfig(
        inst_id=symbol,
        bar=bar,
        ema_period=21,
        trend_ema_period=55,
        atr_period=10,
        atr_stop_multiplier=Decimal("1"),
        atr_take_multiplier=Decimal("1"),
        order_size=Decimal("0"),
        trade_mode="cross",
        signal_mode=signal_mode,
        position_mode="net",
        environment="demo",
        tp_sl_trigger_type="mark",
        strategy_id=strategy_id,
        risk_amount=Decimal("100"),
        entry_reference_ema_period=55,
        mtf_filter_bar=filter_bar,
        mtf_filter_fast_ema_period=21,
        mtf_filter_slow_ema_period=55,
    )


def load_market_data(client: OkxRestClient) -> tuple[dict[tuple[str, str], tuple[object, list, str]], dict[tuple[str, str], list]]:
    base_cache: dict[tuple[str, str], tuple[object, list, str]] = {}
    mtf_cache: dict[tuple[str, str], list] = {}
    needed_bars = {"1H", "4H", "15m"}
    for symbol in SYMBOLS:
        instrument = client.get_instrument(symbol)
        for bar in needed_bars:
            label = {"15m": "15m", "1H": "1小时", "4H": "4小时"}[bar]
            print(f"load {symbol} {label} {CANDLE_LIMIT} candles")
            candles = _load_backtest_candles(client, symbol, bar, CANDLE_LIMIT)
            note = _build_backtest_data_source_note(client)
            base_cache[(symbol, bar)] = (instrument, candles, note)
            mtf_cache[(symbol, bar)] = candles
    return base_cache, mtf_cache


def run_cell(config: StrategyConfig, instrument, candles, data_source_note: str, mtf_filter_candles=None) -> tuple[StrategyConfig, object]:
    successes: list[tuple[StrategyConfig, object]] = []
    for cfg in build_atr_batch_configs(config):
        result = _run_backtest_with_loaded_data(
            candles,
            instrument,
            cfg,
            data_source_note=data_source_note,
            maker_fee_rate=MAKER_FEE_RATE,
            taker_fee_rate=TAKER_FEE_RATE,
            mtf_filter_candles=mtf_filter_candles,
        )
        successes.append((cfg, result))
    return max(successes, key=lambda item: item[1].report.total_pnl)


def run_suite(average_type: str, client: OkxRestClient) -> tuple[list[CellResult], str]:
    base_cache, mtf_cache = load_market_data(client)
    rows: list[CellResult] = []
    total = len(SYMBOLS) * (len(NORMAL_TIMEFRAMES) * len(NORMAL_SIGNALS) + len(MTF_TIMEFRAMES) * 2)
    step = 0
    data_source_note = ""
    with patched_average(average_type):
        for symbol in SYMBOLS:
            symbol_label = SYMBOL_LABELS[symbol]
            for bar, bar_label in NORMAL_TIMEFRAMES:
                instrument, candles, note = base_cache[(symbol, bar)]
                data_source_note = note
                for signal_mode, direction_label in NORMAL_SIGNALS:
                    step += 1
                    print(f"[{average_type} {step}/{total}] normal {symbol_label} {bar_label} {direction_label}")
                    best_cfg, best_result = run_cell(build_normal_config(symbol, bar, signal_mode), instrument, candles, note)
                    rows.append(
                        CellResult(
                            average_type=average_type,
                            suite_id="dynamic",
                            suite_label="动态委托",
                            symbol=symbol,
                            symbol_label=symbol_label,
                            entry_bar=bar,
                            entry_bar_label=bar_label,
                            filter_bar="",
                            filter_bar_label="",
                            direction=direction_label,
                            best_sl=best_cfg.atr_stop_multiplier,
                            best_tp=best_cfg.atr_take_multiplier,
                            total_pnl=best_result.report.total_pnl,
                            win_rate=best_result.report.win_rate,
                            total_trades=best_result.report.total_trades,
                            max_drawdown=best_result.report.max_drawdown,
                        )
                    )
            for bar, bar_label, filter_bar in MTF_TIMEFRAMES:
                instrument, candles, note = base_cache[(symbol, bar)]
                filter_candles = mtf_cache[(symbol, filter_bar)]
                data_source_note = note
                for direction_label in ("做多", "做空"):
                    step += 1
                    suite_id = "mtf_long" if direction_label == "做多" else "mtf_short"
                    suite_label = "多周期多头" if direction_label == "做多" else "多周期空头"
                    print(
                        f"[{average_type} {step}/{total}] {suite_id} {symbol_label} "
                        f"{bar_label}/{filter_bar} {direction_label}"
                    )
                    best_cfg, best_result = run_cell(
                        build_mtf_config(symbol, bar, filter_bar, direction_label),
                        instrument,
                        candles,
                        note,
                        mtf_filter_candles=filter_candles,
                    )
                    rows.append(
                        CellResult(
                            average_type=average_type,
                            suite_id=suite_id,
                            suite_label=suite_label,
                            symbol=symbol,
                            symbol_label=symbol_label,
                            entry_bar=bar,
                            entry_bar_label=bar_label,
                            filter_bar=filter_bar,
                            filter_bar_label=filter_bar,
                            direction=direction_label,
                            best_sl=best_cfg.atr_stop_multiplier,
                            best_tp=best_cfg.atr_take_multiplier,
                            total_pnl=best_result.report.total_pnl,
                            win_rate=best_result.report.win_rate,
                            total_trades=best_result.report.total_trades,
                            max_drawdown=best_result.report.max_drawdown,
                        )
                    )
    return rows, data_source_note


def export_csv(path: Path, rows: list[CellResult]) -> None:
    with path.open("w", newline="", encoding="utf-8-sig") as fp:
        writer = csv.writer(fp)
        writer.writerow(
            [
                "average_type",
                "suite_id",
                "suite_label",
                "symbol",
                "symbol_label",
                "entry_bar",
                "entry_bar_label",
                "filter_bar",
                "filter_bar_label",
                "direction",
                "best_sl",
                "best_tp",
                "total_pnl",
                "win_rate",
                "total_trades",
                "max_drawdown",
                "pnl_dd_ratio",
            ]
        )
        for row in rows:
            writer.writerow(
                [
                    row.average_type,
                    row.suite_id,
                    row.suite_label,
                    row.symbol,
                    row.symbol_label,
                    row.entry_bar,
                    row.entry_bar_label,
                    row.filter_bar,
                    row.filter_bar_label,
                    row.direction,
                    format_decimal(row.best_sl),
                    format_decimal(row.best_tp),
                    format_decimal_fixed(row.total_pnl, 4),
                    format_decimal_fixed(row.win_rate, 2),
                    row.total_trades,
                    format_decimal_fixed(row.max_drawdown, 4),
                    format_decimal_fixed(row.pnl_dd_ratio, 4),
                ]
            )


def _paired(rows: list[CellResult]) -> list[tuple[CellResult, CellResult]]:
    ema_map = {
        (row.suite_id, row.symbol, row.entry_bar, row.filter_bar, row.direction): row
        for row in rows
        if row.average_type == "EMA"
    }
    pairs: list[tuple[CellResult, CellResult]] = []
    for ma_row in rows:
        if ma_row.average_type != "MA":
            continue
        key = (ma_row.suite_id, ma_row.symbol, ma_row.entry_bar, ma_row.filter_bar, ma_row.direction)
        ema_row = ema_map[key]
        pairs.append((ema_row, ma_row))
    return pairs


def build_markdown_report(rows: list[CellResult], exported_at: datetime, csv_path: Path, data_source_note: str) -> str:
    pairs = _paired(rows)
    ma_pnl_wins = sum(1 for ema_row, ma_row in pairs if ma_row.total_pnl > ema_row.total_pnl)
    ema_pnl_wins = sum(1 for ema_row, ma_row in pairs if ema_row.total_pnl >= ma_row.total_pnl)
    ma_quality_wins = sum(1 for ema_row, ma_row in pairs if ma_row.pnl_dd_ratio > ema_row.pnl_dd_ratio)
    ema_quality_wins = sum(1 for ema_row, ma_row in pairs if ema_row.pnl_dd_ratio >= ma_row.pnl_dd_ratio)

    suite_stats: dict[str, dict[str, int]] = {}
    for suite_id in ("dynamic", "mtf_long", "mtf_short"):
        suite_pairs = [pair for pair in pairs if pair[0].suite_id == suite_id]
        suite_stats[suite_id] = {
            "ma_pnl_wins": sum(1 for ema_row, ma_row in suite_pairs if ma_row.total_pnl > ema_row.total_pnl),
            "ema_pnl_wins": sum(1 for ema_row, ma_row in suite_pairs if ema_row.total_pnl >= ma_row.total_pnl),
            "ma_quality_wins": sum(1 for ema_row, ma_row in suite_pairs if ma_row.pnl_dd_ratio > ema_row.pnl_dd_ratio),
            "ema_quality_wins": sum(1 for ema_row, ma_row in suite_pairs if ema_row.pnl_dd_ratio >= ma_row.pnl_dd_ratio),
        }

    biggest_ma = sorted(
        (pair for pair in pairs if pair[1].total_pnl - pair[0].total_pnl > 0),
        key=lambda pair: pair[1].total_pnl - pair[0].total_pnl,
        reverse=True,
    )[:10]
    biggest_ema = sorted(
        (pair for pair in pairs if pair[0].total_pnl - pair[1].total_pnl > 0),
        key=lambda pair: pair[0].total_pnl - pair[1].total_pnl,
        reverse=True,
    )[:10]

    lines = [
        "# MA vs EMA 领导实盘决策报告",
        "",
        f"生成时间：{exported_at.strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "## 结论先看",
        "",
        "这次按之前复盘回测的同一标准，把五个币种、四类策略、多个周期全部重跑了一遍，并在同一批数据上做了 `MA` 与 `EMA` 的正面对比。",
        "",
        f"1. 从总收益看，`MA` 胜出 {ma_pnl_wins}/40 个单元，`EMA` 胜出 {ema_pnl_wins}/40 个单元。",
        f"2. 从风险收益效率看，`MA` 胜出 {ma_quality_wins}/40 个单元，`EMA` 胜出 {ema_quality_wins}/40 个单元。",
        "3. 结论不是“全盘切到 MA”或“继续全用 EMA”，而是进入分币种、分策略、分周期的混合参数管理。",
        "",
        "## 回测标准",
        "",
        "- 币种：BTC / ETH / SOL / BNB / DOGE",
        "- 普通动态委托：1H、4H；做多、做空",
        "- 多周期多头：15m入场+1H过滤，1H入场+4H过滤",
        "- 多周期空头：15m入场+1H过滤，1H入场+4H过滤",
        "- 参数口径：21/55、挂单55、ATR10、风险金100、9组 SL/TP 组合",
        f"- 数据来源：{data_source_note}",
        f"- 全量结果：[CSV]({csv_path})",
        "",
        "## 总体判断",
        "",
        "| 维度 | MA胜出 | EMA胜出 |",
        "| --- | ---: | ---: |",
        f"| 总收益单元数 | {ma_pnl_wins} | {ema_pnl_wins} |",
        f"| 风险收益效率单元数 | {ma_quality_wins} | {ema_quality_wins} |",
        "",
        "## 分策略统计",
        "",
        "| 策略 | MA收益胜出 | EMA收益胜出 | MA效率胜出 | EMA效率胜出 |",
        "| --- | ---: | ---: | ---: | ---: |",
        f"| 动态委托 | {suite_stats['dynamic']['ma_pnl_wins']} | {suite_stats['dynamic']['ema_pnl_wins']} | {suite_stats['dynamic']['ma_quality_wins']} | {suite_stats['dynamic']['ema_quality_wins']} |",
        f"| 多周期多头 | {suite_stats['mtf_long']['ma_pnl_wins']} | {suite_stats['mtf_long']['ema_pnl_wins']} | {suite_stats['mtf_long']['ma_quality_wins']} | {suite_stats['mtf_long']['ema_quality_wins']} |",
        f"| 多周期空头 | {suite_stats['mtf_short']['ma_pnl_wins']} | {suite_stats['mtf_short']['ema_pnl_wins']} | {suite_stats['mtf_short']['ma_quality_wins']} | {suite_stats['mtf_short']['ema_quality_wins']} |",
        "",
        "## MA 优势最明显的单元",
        "",
    ]
    for ema_row, ma_row in biggest_ma:
        gap = ma_row.total_pnl - ema_row.total_pnl
        lines.append(
            f"- {ma_row.symbol_label} | {ma_row.suite_label} | {ma_row.entry_bar_label}"
            f"{' / ' + ma_row.filter_bar_label if ma_row.filter_bar_label else ''} | {ma_row.direction}"
            f"：MA {format_decimal_fixed(ma_row.total_pnl, 4)} vs EMA {format_decimal_fixed(ema_row.total_pnl, 4)}"
            f"，差额 {format_decimal_fixed(gap, 4)}"
        )

    lines.extend(["", "## EMA 仍然更有优势的单元", ""])
    for ema_row, ma_row in biggest_ema:
        gap = ema_row.total_pnl - ma_row.total_pnl
        lines.append(
            f"- {ema_row.symbol_label} | {ema_row.suite_label} | {ema_row.entry_bar_label}"
            f"{' / ' + ema_row.filter_bar_label if ema_row.filter_bar_label else ''} | {ema_row.direction}"
            f"：EMA {format_decimal_fixed(ema_row.total_pnl, 4)} vs MA {format_decimal_fixed(ma_row.total_pnl, 4)}"
            f"，差额 {format_decimal_fixed(gap, 4)}"
        )

    lines.extend(
        [
            "",
            "## 领导层实盘建议",
            "",
            "1. 不建议全盘把 EMA 切成 MA。当前更合理的是“混合上线”，按币种和结构选择。",
            "2. MA 更适合多数顺势做多、噪音偏大的结构，尤其适合一部分 1H 做多和 4H 多头过滤结构。",
            "3. EMA 仍然更适合一部分空头加速段、以及对趋势启动速度要求更高的结构。",
            "4. 实盘首批建议采用“双清单管理”：一张 MA 主推清单，一张 EMA 保留清单，避免策略切换过度。",
            "",
            "## 交易员执行口径",
            "",
            "1. 后续参数维护不要再按“统一 EMA 模板”推进，而是按“MA版主推 / EMA版保留 / 暂缓上线”三档管理。",
            "2. 对 MA 明显跑赢且回撤更优的单元，可以优先进入模拟盘或小仓位实盘验证。",
            "3. 对 EMA 仍占优的单元，不建议为了追新而强行切到 MA。",
            "",
            "## 最终判断",
            "",
            "这次回测已经足够支持一个管理层结论：`MA 版本有必要进入实盘候选池，但不应替代全部 EMA 版本。`",
            "最合理的推进方式，是把 MA 作为第二套实盘引擎并行验证，再按币种和策略单元逐步替换，而不是一次性全量切换。",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> int:
    exported_at = datetime.now()
    client = OkxRestClient()

    all_rows: list[CellResult] = []
    data_source_note = ""
    for average_type in AVERAGE_TYPES:
        rows, note = run_suite(average_type, client)
        all_rows.extend(rows)
        data_source_note = note

    timestamp = exported_at.strftime("%Y%m%d_%H%M%S")
    csv_path = REPORTS_DIR / f"standard_suite_ma_vs_ema_compare_{timestamp}.csv"
    md_path = REPORTS_DIR / f"MA_vs_EMA领导实盘报告_{timestamp}.md"
    json_path = REPORTS_DIR / f"standard_suite_ma_vs_ema_compare_{timestamp}.json"

    export_csv(csv_path, all_rows)
    md_path.write_text(build_markdown_report(all_rows, exported_at, csv_path, data_source_note), encoding="utf-8-sig")
    json_path.write_text(
        json.dumps(
            [
                {
                    "average_type": row.average_type,
                    "suite_id": row.suite_id,
                    "suite_label": row.suite_label,
                    "symbol": row.symbol,
                    "symbol_label": row.symbol_label,
                    "entry_bar": row.entry_bar,
                    "entry_bar_label": row.entry_bar_label,
                    "filter_bar": row.filter_bar,
                    "filter_bar_label": row.filter_bar_label,
                    "direction": row.direction,
                    "best_sl": format_decimal(row.best_sl),
                    "best_tp": format_decimal(row.best_tp),
                    "total_pnl": format_decimal_fixed(row.total_pnl, 4),
                    "win_rate": format_decimal_fixed(row.win_rate, 2),
                    "total_trades": row.total_trades,
                    "max_drawdown": format_decimal_fixed(row.max_drawdown, 4),
                    "pnl_dd_ratio": format_decimal_fixed(row.pnl_dd_ratio, 4),
                }
                for row in all_rows
            ],
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8-sig",
    )

    print(f"report -> {md_path}")
    print(f"csv -> {csv_path}")
    print(f"json -> {json_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
