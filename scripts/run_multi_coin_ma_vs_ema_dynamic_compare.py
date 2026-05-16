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
from okx_quant.backtest import _build_backtest_data_source_note, _load_backtest_candles, _run_backtest_with_loaded_data
from okx_quant.indicators import ema, sma
from okx_quant.models import StrategyConfig
from okx_quant.okx_client import OkxRestClient
from okx_quant.persistence import analysis_report_dir_path
from okx_quant.pricing import format_decimal, format_decimal_fixed
from okx_quant.strategy_catalog import STRATEGY_DYNAMIC_LONG_ID, STRATEGY_DYNAMIC_SHORT_ID


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
TIMEFRAMES = (("1H", "1小时"), ("4H", "4小时"))
DIRECTIONS = (
    (STRATEGY_DYNAMIC_LONG_ID, "long_only", "做多"),
    (STRATEGY_DYNAMIC_SHORT_ID, "short_only", "做空"),
)
AVERAGE_TYPES = ("EMA", "MA")
EMA_PERIOD = 21
TREND_EMA_PERIOD = 55
ENTRY_REFERENCE_EMA_PERIOD = 55
ATR_STOP_MULTIPLIER = Decimal("2")
CANDLE_LIMIT = 10000
ATR_PERIOD = 10
RISK_AMOUNT = Decimal("100")
MAKER_FEE_RATE = Decimal("0.0001")
TAKER_FEE_RATE = Decimal("0.00028")


@dataclass(frozen=True)
class CompareRow:
    symbol: str
    symbol_label: str
    average_type: str
    bar: str
    bar_label: str
    direction: str
    total_pnl: Decimal
    win_rate: Decimal
    total_trades: int
    max_drawdown: Decimal
    avg_r: Decimal

    @property
    def pnl_dd_ratio(self) -> Decimal:
        if self.max_drawdown == 0:
            return Decimal("0")
        return self.total_pnl / self.max_drawdown


def build_config(*, symbol: str, bar: str, strategy_id: str, signal_mode: str) -> StrategyConfig:
    return StrategyConfig(
        inst_id=symbol,
        bar=bar,
        ema_period=EMA_PERIOD,
        trend_ema_period=TREND_EMA_PERIOD,
        big_ema_period=233,
        atr_period=ATR_PERIOD,
        atr_stop_multiplier=ATR_STOP_MULTIPLIER,
        atr_take_multiplier=ATR_STOP_MULTIPLIER * Decimal("2"),
        order_size=Decimal("0"),
        trade_mode="cross",
        signal_mode=signal_mode,
        position_mode="net",
        environment="demo",
        tp_sl_trigger_type="mark",
        strategy_id=strategy_id,
        risk_amount=RISK_AMOUNT,
        entry_reference_ema_period=ENTRY_REFERENCE_EMA_PERIOD,
        take_profit_mode="dynamic",
        max_entries_per_trend=1,
        dynamic_two_r_break_even=True,
        dynamic_fee_offset_enabled=True,
    )


def write_text(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8-sig")


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


def load_market_data(client: OkxRestClient) -> dict[tuple[str, str], tuple[object, list, str]]:
    cache: dict[tuple[str, str], tuple[object, list, str]] = {}
    for symbol in SYMBOLS:
        instrument = client.get_instrument(symbol)
        for bar, bar_label in TIMEFRAMES:
            print(f"load {symbol} {bar_label} {CANDLE_LIMIT} candles")
            candles = _load_backtest_candles(client, symbol, bar, CANDLE_LIMIT)
            note = _build_backtest_data_source_note(client)
            cache[(symbol, bar)] = (instrument, candles, note)
    return cache


def run_compare(client: OkxRestClient) -> tuple[list[CompareRow], str]:
    cache = load_market_data(client)
    rows: list[CompareRow] = []
    total = len(SYMBOLS) * len(AVERAGE_TYPES) * len(TIMEFRAMES) * len(DIRECTIONS)
    step = 0
    data_source_note = ""
    for average_type in AVERAGE_TYPES:
        with patched_average(average_type):
            for symbol in SYMBOLS:
                symbol_label = SYMBOL_LABELS[symbol]
                for bar, bar_label in TIMEFRAMES:
                    instrument, candles, note = cache[(symbol, bar)]
                    data_source_note = note
                    for strategy_id, signal_mode, direction_label in DIRECTIONS:
                        step += 1
                        print(
                            f"[{step}/{total}] {symbol_label} | {average_type} | {bar_label} | {direction_label} | "
                            f"{EMA_PERIOD}/{TREND_EMA_PERIOD} | 挂单{ENTRY_REFERENCE_EMA_PERIOD} | "
                            f"SLx{format_decimal(ATR_STOP_MULTIPLIER)} | 动态止盈"
                        )
                        result = _run_backtest_with_loaded_data(
                            candles,
                            instrument,
                            build_config(symbol=symbol, bar=bar, strategy_id=strategy_id, signal_mode=signal_mode),
                            data_source_note=note,
                            maker_fee_rate=MAKER_FEE_RATE,
                            taker_fee_rate=TAKER_FEE_RATE,
                        )
                        rows.append(
                            CompareRow(
                                symbol=symbol,
                                symbol_label=symbol_label,
                                average_type=average_type,
                                bar=bar,
                                bar_label=bar_label,
                                direction=direction_label,
                                total_pnl=result.report.total_pnl,
                                win_rate=result.report.win_rate,
                                total_trades=result.report.total_trades,
                                max_drawdown=result.report.max_drawdown,
                                avg_r=result.report.average_r_multiple,
                            )
                        )
    return rows, data_source_note


def export_csv(path: Path, rows: list[CompareRow]) -> None:
    with path.open("w", newline="", encoding="utf-8-sig") as fp:
        writer = csv.writer(fp)
        writer.writerow(
            [
                "symbol",
                "symbol_label",
                "average_type",
                "bar",
                "bar_label",
                "direction",
                "total_pnl",
                "win_rate",
                "total_trades",
                "max_drawdown",
                "pnl_dd_ratio",
                "avg_r",
            ]
        )
        for row in rows:
            writer.writerow(
                [
                    row.symbol,
                    row.symbol_label,
                    row.average_type,
                    row.bar,
                    row.bar_label,
                    row.direction,
                    format_decimal_fixed(row.total_pnl, 4),
                    format_decimal_fixed(row.win_rate, 2),
                    row.total_trades,
                    format_decimal_fixed(row.max_drawdown, 4),
                    format_decimal_fixed(row.pnl_dd_ratio, 4),
                    format_decimal_fixed(row.avg_r, 4),
                ]
            )


def build_markdown_report(rows: list[CompareRow], exported_at: datetime, csv_path: Path, data_source_note: str) -> str:
    lines = [
        "# 五币种 MA vs EMA 对比报告",
        "",
        f"生成时间：{exported_at.strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "测试条件：`21/55 + 挂单55 + 2ATR止损 + 动态止盈`",
        "",
        "币种：`BTC / ETH / SOL / BNB / DOGE`",
        "",
        f"数据来源：{data_source_note}",
        "",
        f"原始结果：[CSV]({csv_path})",
        "",
        "## 总表",
        "",
        "| 币种 | 周期 | 方向 | 均线 | 总盈亏 | 胜率 | 交易数 | 最大回撤 | 盈亏/回撤 | 平均R |",
        "| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for symbol in SYMBOLS:
        for bar, bar_label in TIMEFRAMES:
            for direction_label in ("做多", "做空"):
                subset = [
                    row for row in rows
                    if row.symbol == symbol and row.bar == bar and row.direction == direction_label
                ]
                subset.sort(key=lambda row: row.average_type)
                for row in subset:
                    lines.append(
                        f"| {row.symbol_label} | {bar_label} | {direction_label} | {row.average_type} | "
                        f"{format_decimal_fixed(row.total_pnl, 4)} | {format_decimal_fixed(row.win_rate, 2)}% | "
                        f"{row.total_trades} | {format_decimal_fixed(row.max_drawdown, 4)} | "
                        f"{format_decimal_fixed(row.pnl_dd_ratio, 4)} | {format_decimal_fixed(row.avg_r, 4)} |"
                    )
    lines.extend(["", "## 各币结论", ""])
    for symbol in SYMBOLS:
        lines.append(f"### {SYMBOL_LABELS[symbol]}")
        for bar, bar_label in TIMEFRAMES:
            for direction_label in ("做多", "做空"):
                subset = [
                    row for row in rows
                    if row.symbol == symbol and row.bar == bar and row.direction == direction_label
                ]
                pnl_winner = max(subset, key=lambda row: row.total_pnl)
                quality_winner = max(subset, key=lambda row: row.pnl_dd_ratio)
                lines.append(
                    f"- {bar_label} {direction_label}：收益更高=`{pnl_winner.average_type}`，风险收益效率更高=`{quality_winner.average_type}`"
                )
        lines.append("")
    lines.extend(
        [
            "## 交易员总结",
            "",
            "1. 这次对比只替换均线算法，其余参数固定，因此能比较干净地看出 MA 和 EMA 的结构差异。",
            "2. 如果某个币在同一周期方向下 `MA` 更强，通常说明它更需要平滑过滤和更慢的确认。",
            "3. 如果某个币在同一周期方向下 `EMA` 更强，通常说明它更吃趋势启动后的快速跟随。",
            "4. 动态止盈场景里，总收益不是唯一标准，优先同时看 `盈亏/回撤` 和 `平均R`。",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> int:
    exported_at = datetime.now()
    client = OkxRestClient()
    rows, data_source_note = run_compare(client)

    timestamp = exported_at.strftime("%Y%m%d_%H%M%S")
    csv_path = REPORTS_DIR / f"multi_coin_ma_vs_ema_dynamic_compare_{timestamp}.csv"
    md_path = REPORTS_DIR / f"multi_coin_ma_vs_ema_dynamic_compare_{timestamp}.md"
    json_path = REPORTS_DIR / f"multi_coin_ma_vs_ema_dynamic_compare_{timestamp}.json"

    export_csv(csv_path, rows)
    write_text(md_path, build_markdown_report(rows, exported_at, csv_path, data_source_note))
    json_path.write_text(
        json.dumps(
            [
                {
                    "symbol": row.symbol,
                    "symbol_label": row.symbol_label,
                    "average_type": row.average_type,
                    "bar": row.bar,
                    "bar_label": row.bar_label,
                    "direction": row.direction,
                    "total_pnl": format_decimal_fixed(row.total_pnl, 4),
                    "win_rate": format_decimal_fixed(row.win_rate, 2),
                    "total_trades": row.total_trades,
                    "max_drawdown": format_decimal_fixed(row.max_drawdown, 4),
                    "pnl_dd_ratio": format_decimal_fixed(row.pnl_dd_ratio, 4),
                    "avg_r": format_decimal_fixed(row.avg_r, 4),
                }
                for row in rows
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
