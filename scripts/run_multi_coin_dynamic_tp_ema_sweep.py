from __future__ import annotations

import csv
import json
import sys
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from okx_quant.backtest import _build_backtest_data_source_note, _load_backtest_candles, _run_backtest_with_loaded_data
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
EMA_PROFILES = (
    (5, 13),
    (8, 21),
    (13, 34),
    (21, 55),
    (34, 89),
    (55, 144),
)
ENTRY_REFERENCE_EMAS = (0, 5, 8, 13, 21, 34, 55)
ATR_STOP_MULTIPLIERS = (Decimal("1"), Decimal("1.5"), Decimal("2"))
CANDLE_LIMIT = 10000
ATR_PERIOD = 10
RISK_AMOUNT = Decimal("100")
MAKER_FEE_RATE = Decimal("0.0001")
TAKER_FEE_RATE = Decimal("0.00028")


@dataclass(frozen=True)
class SweepRow:
    symbol: str
    symbol_label: str
    bar: str
    bar_label: str
    direction: str
    ema_period: int
    trend_ema_period: int
    entry_reference_ema_period: int
    atr_stop_multiplier: Decimal
    total_pnl: Decimal
    win_rate: Decimal
    total_trades: int
    max_drawdown: Decimal
    avg_r: Decimal

    @property
    def entry_ema_label(self) -> str:
        if self.entry_reference_ema_period <= 0:
            return f"跟随快线EMA{self.ema_period}"
        return f"EMA{self.entry_reference_ema_period}"

    @property
    def trend_label(self) -> str:
        return f"EMA{self.ema_period}/EMA{self.trend_ema_period}"

    @property
    def pnl_dd_ratio(self) -> Decimal:
        if self.max_drawdown == 0:
            return Decimal("0")
        return self.total_pnl / self.max_drawdown


def build_config(
    *,
    symbol: str,
    bar: str,
    strategy_id: str,
    signal_mode: str,
    ema_period: int,
    trend_ema_period: int,
    entry_reference_ema_period: int,
    stop_atr: Decimal,
) -> StrategyConfig:
    return StrategyConfig(
        inst_id=symbol,
        bar=bar,
        ema_period=ema_period,
        trend_ema_period=trend_ema_period,
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
        entry_reference_ema_period=entry_reference_ema_period,
        take_profit_mode="dynamic",
        max_entries_per_trend=1,
        dynamic_two_r_break_even=True,
        dynamic_fee_offset_enabled=True,
    )


def write_text(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8-sig")


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


def run_sweep(client: OkxRestClient) -> tuple[list[SweepRow], str]:
    cache = load_market_data(client)
    rows: list[SweepRow] = []
    total = (
        len(SYMBOLS)
        * len(TIMEFRAMES)
        * len(DIRECTIONS)
        * len(EMA_PROFILES)
        * len(ENTRY_REFERENCE_EMAS)
        * len(ATR_STOP_MULTIPLIERS)
    )
    sequence = 0
    data_source_note = ""
    for symbol in SYMBOLS:
        symbol_label = SYMBOL_LABELS.get(symbol, symbol)
        for bar, bar_label in TIMEFRAMES:
            instrument, candles, note = cache[(symbol, bar)]
            data_source_note = note
            for strategy_id, signal_mode, direction_label in DIRECTIONS:
                for ema_period, trend_ema_period in EMA_PROFILES:
                    if ema_period >= trend_ema_period:
                        continue
                    for entry_reference_ema_period in ENTRY_REFERENCE_EMAS:
                        resolved_entry_ema = ema_period if entry_reference_ema_period <= 0 else entry_reference_ema_period
                        if resolved_entry_ema > trend_ema_period:
                            continue
                        for stop_atr in ATR_STOP_MULTIPLIERS:
                            sequence += 1
                            print(
                                f"[{sequence}/{total}] {symbol_label} {bar_label} {direction_label} "
                                f"EMA{ema_period}/EMA{trend_ema_period} 入场{resolved_entry_ema} "
                                f"动态止盈 SLx{format_decimal(stop_atr)}"
                            )
                            config = build_config(
                                symbol=symbol,
                                bar=bar,
                                strategy_id=strategy_id,
                                signal_mode=signal_mode,
                                ema_period=ema_period,
                                trend_ema_period=trend_ema_period,
                                entry_reference_ema_period=entry_reference_ema_period,
                                stop_atr=stop_atr,
                            )
                            result = _run_backtest_with_loaded_data(
                                candles,
                                instrument,
                                config,
                                data_source_note=note,
                                maker_fee_rate=MAKER_FEE_RATE,
                                taker_fee_rate=TAKER_FEE_RATE,
                            )
                            rows.append(
                                SweepRow(
                                    symbol=symbol,
                                    symbol_label=symbol_label,
                                    bar=bar,
                                    bar_label=bar_label,
                                    direction=direction_label,
                                    ema_period=ema_period,
                                    trend_ema_period=trend_ema_period,
                                    entry_reference_ema_period=entry_reference_ema_period,
                                    atr_stop_multiplier=stop_atr,
                                    total_pnl=result.report.total_pnl,
                                    win_rate=result.report.win_rate,
                                    total_trades=result.report.total_trades,
                                    max_drawdown=result.report.max_drawdown,
                                    avg_r=result.report.average_r_multiple,
                                )
                            )
    return rows, data_source_note


def export_csv(path: Path, rows: list[SweepRow]) -> None:
    with path.open("w", newline="", encoding="utf-8-sig") as fp:
        writer = csv.writer(fp)
        writer.writerow(
            [
                "symbol",
                "symbol_label",
                "bar",
                "bar_label",
                "direction",
                "ema_period",
                "trend_ema_period",
                "entry_reference_ema_period",
                "entry_ema_label",
                "atr_stop_multiplier",
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
                    row.bar,
                    row.bar_label,
                    row.direction,
                    row.ema_period,
                    row.trend_ema_period,
                    row.entry_reference_ema_period,
                    row.entry_ema_label,
                    format_decimal(row.atr_stop_multiplier),
                    format_decimal_fixed(row.total_pnl, 4),
                    format_decimal_fixed(row.win_rate, 2),
                    row.total_trades,
                    format_decimal_fixed(row.max_drawdown, 4),
                    format_decimal_fixed(row.pnl_dd_ratio, 4),
                    format_decimal_fixed(row.avg_r, 4),
                ]
            )


def _best_row(rows: list[SweepRow]) -> SweepRow | None:
    if not rows:
        return None
    return max(rows, key=lambda row: row.total_pnl)


def build_text_report(rows: list[SweepRow], data_source_note: str, exported_at: datetime, csv_path: Path) -> str:
    lines = [
        "五币种 EMA动态委托 动态止盈均线扫参报告",
        "=" * 88,
        f"导出时间：{exported_at.strftime('%Y-%m-%d %H:%M:%S')}",
        "标的：BTC / ETH / SOL / BNB / DOGE",
        "策略：EMA动态委托 做多 / 做空",
        "出场口径：动态止盈",
        "周期：1H、4H",
        "均线组合：5/13、8/21、13/34、21/55、34/89、55/144",
        "挂单参考EMA：跟随快线、5、8、13、21、34、55",
        "止损：1 / 1.5 / 2 ATR",
        "动态止盈设置：2R保本=开启；手续费偏移=开启；每波开仓次数=1",
        f"数据来源：{data_source_note}",
        f"全量结果：{csv_path}",
        "",
        "一、各币种全局最佳",
    ]

    for symbol in SYMBOLS:
        symbol_rows = [row for row in rows if row.symbol == symbol]
        best = _best_row(symbol_rows)
        if best is None:
            continue
        lines.append(
            f"{best.symbol_label} | {best.bar_label} | {best.direction} | {best.trend_label} | "
            f"挂单{best.entry_ema_label} | SLx{format_decimal(best.atr_stop_multiplier)} | "
            f"总盈亏 {format_decimal_fixed(best.total_pnl, 4)} | 胜率 {format_decimal_fixed(best.win_rate, 2)}% | "
            f"交易数 {best.total_trades} | 最大回撤 {format_decimal_fixed(best.max_drawdown, 4)} | "
            f"盈亏/回撤 {format_decimal_fixed(best.pnl_dd_ratio, 4)}"
        )

    lines.extend(["", "二、各币种 1H 做多最佳",])
    for symbol in SYMBOLS:
        symbol_rows = [row for row in rows if row.symbol == symbol and row.bar == "1H" and row.direction == "做多"]
        best = _best_row(symbol_rows)
        if best is None:
            continue
        lines.append(
            f"{best.symbol_label} | {best.trend_label} | 挂单{best.entry_ema_label} | "
            f"SLx{format_decimal(best.atr_stop_multiplier)} | 总盈亏 {format_decimal_fixed(best.total_pnl, 4)} | "
            f"胜率 {format_decimal_fixed(best.win_rate, 2)}% | 交易数 {best.total_trades} | "
            f"最大回撤 {format_decimal_fixed(best.max_drawdown, 4)}"
        )

    lines.extend(["", "三、重点复核：1H 做多 EMA21/55 下 挂单34 vs 挂单55",])
    for symbol in SYMBOLS:
        lines.append(f"{SYMBOL_LABELS[symbol]}：")
        focus_rows = [
            row
            for row in rows
            if row.symbol == symbol
            and row.bar == "1H"
            and row.direction == "做多"
            and row.ema_period == 21
            and row.trend_ema_period == 55
            and row.entry_reference_ema_period in {34, 55}
        ]
        if not focus_rows:
            lines.append("无结果")
            continue
        for stop_atr in ATR_STOP_MULTIPLIERS:
            same_stop_rows = [row for row in focus_rows if row.atr_stop_multiplier == stop_atr]
            same_stop_rows.sort(key=lambda row: row.entry_reference_ema_period)
            for row in same_stop_rows:
                lines.append(
                    f"  挂单{row.entry_ema_label} | SLx{format_decimal(row.atr_stop_multiplier)} | "
                    f"总盈亏 {format_decimal_fixed(row.total_pnl, 4)} | 胜率 {format_decimal_fixed(row.win_rate, 2)}% | "
                    f"交易数 {row.total_trades} | 最大回撤 {format_decimal_fixed(row.max_drawdown, 4)} | "
                    f"盈亏/回撤 {format_decimal_fixed(row.pnl_dd_ratio, 4)} | 平均R {format_decimal_fixed(row.avg_r, 4)}"
                )
            if len(same_stop_rows) == 2:
                better = max(same_stop_rows, key=lambda row: row.total_pnl)
                safer = max(same_stop_rows, key=lambda row: row.pnl_dd_ratio)
                lines.append(f"  同止损结论：收益更高={better.entry_ema_label}；风险收益效率更高={safer.entry_ema_label}")
        lines.append("")

    lines.extend(["四、交易员总括",])
    for symbol in SYMBOLS:
        symbol_rows = [row for row in rows if row.symbol == symbol and row.bar == "1H" and row.direction == "做多"]
        best = _best_row(symbol_rows)
        if best is None:
            continue
        focus_rows = [
            row
            for row in rows
            if row.symbol == symbol
            and row.bar == "1H"
            and row.direction == "做多"
            and row.ema_period == 21
            and row.trend_ema_period == 55
            and row.entry_reference_ema_period in {34, 55}
        ]
        focus_best = _best_row(focus_rows)
        if focus_best is None:
            lines.append(f"{best.symbol_label}：1H 做多最佳为 {best.trend_label} 挂单{best.entry_ema_label}。")
            continue
        style = "更偏进攻型" if focus_best.entry_reference_ema_period == 34 else "更偏稳健型"
        lines.append(
            f"{best.symbol_label}：1H 做多最佳为 {best.trend_label} 挂单{best.entry_ema_label}，"
            f"在 21/55 复核里当前更占优的是 {focus_best.entry_ema_label}，说明该币在动态止盈下{style}。"
        )

    return "\n".join(lines) + "\n"


def main() -> int:
    exported_at = datetime.now()
    client = OkxRestClient()
    rows, data_source_note = run_sweep(client)

    timestamp = exported_at.strftime("%Y%m%d_%H%M%S")
    csv_path = REPORTS_DIR / f"multi_coin_dynamic_tp_ema_sweep_{timestamp}.csv"
    txt_path = REPORTS_DIR / f"multi_coin_dynamic_tp_ema_sweep_{timestamp}.txt"
    json_path = REPORTS_DIR / f"multi_coin_dynamic_tp_ema_sweep_{timestamp}.json"

    export_csv(csv_path, rows)
    write_text(txt_path, build_text_report(rows, data_source_note, exported_at, csv_path))
    json_path.write_text(
        json.dumps(
            [
                {
                    "symbol": row.symbol,
                    "symbol_label": row.symbol_label,
                    "bar": row.bar,
                    "bar_label": row.bar_label,
                    "direction": row.direction,
                    "ema_period": row.ema_period,
                    "trend_ema_period": row.trend_ema_period,
                    "entry_reference_ema_period": row.entry_reference_ema_period,
                    "entry_ema_label": row.entry_ema_label,
                    "atr_stop_multiplier": format_decimal(row.atr_stop_multiplier),
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

    print(f"report -> {txt_path}")
    print(f"csv -> {csv_path}")
    print(f"json -> {json_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
