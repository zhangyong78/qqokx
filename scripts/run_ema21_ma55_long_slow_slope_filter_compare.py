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
from okx_quant.pricing import format_decimal_fixed
from okx_quant.strategy_catalog import STRATEGY_DYNAMIC_LONG_ID


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
BAR = "1H"
CANDLE_LIMIT = 10000
FAST_EMA_PERIOD = 21
SLOW_MA_PERIOD = 55
ENTRY_REFERENCE_MA_PERIOD = 55
ATR_PERIOD = 10
ATR_STOP_MULTIPLIER = Decimal("2")
RISK_AMOUNT = Decimal("100")
MAKER_FEE_RATE = Decimal("0.0001")
TAKER_FEE_RATE = Decimal("0.00028")
TREND_SLOPE_LOOKBACK = 5


@dataclass(frozen=True)
class CompareRow:
    symbol: str
    symbol_label: str
    original_pnl: Decimal
    filtered_pnl: Decimal
    original_win_rate: Decimal
    filtered_win_rate: Decimal
    original_trades: int
    filtered_trades: int
    original_max_drawdown: Decimal
    filtered_max_drawdown: Decimal
    original_avg_r: Decimal
    filtered_avg_r: Decimal

    @property
    def pnl_delta(self) -> Decimal:
        return self.filtered_pnl - self.original_pnl

    @property
    def drawdown_delta(self) -> Decimal:
        return self.filtered_max_drawdown - self.original_max_drawdown

    @property
    def trades_delta(self) -> int:
        return self.filtered_trades - self.original_trades

    @property
    def win_rate_delta(self) -> Decimal:
        return self.filtered_win_rate - self.original_win_rate


def build_config(symbol: str, *, slope_filter_enabled: bool) -> StrategyConfig:
    return StrategyConfig(
        inst_id=symbol,
        bar=BAR,
        ema_period=FAST_EMA_PERIOD,
        trend_ema_type="ma",
        trend_ema_period=SLOW_MA_PERIOD,
        big_ema_period=233,
        atr_period=ATR_PERIOD,
        atr_stop_multiplier=ATR_STOP_MULTIPLIER,
        atr_take_multiplier=ATR_STOP_MULTIPLIER * Decimal("2"),
        order_size=Decimal("0"),
        trade_mode="cross",
        signal_mode="long_only",
        position_mode="net",
        environment="demo",
        tp_sl_trigger_type="mark",
        strategy_id=STRATEGY_DYNAMIC_LONG_ID,
        risk_amount=RISK_AMOUNT,
        entry_reference_ema_type="ma",
        entry_reference_ema_period=ENTRY_REFERENCE_MA_PERIOD,
        take_profit_mode="dynamic",
        max_entries_per_trend=1,
        dynamic_two_r_break_even=True,
        dynamic_fee_offset_enabled=True,
        trend_ema_slope_filter_enabled=slope_filter_enabled,
    )


def run_compare(client: OkxRestClient) -> tuple[list[CompareRow], str]:
    rows: list[CompareRow] = []
    data_source_note = ""
    for index, symbol in enumerate(SYMBOLS, start=1):
        print(f"[{index}/{len(SYMBOLS)}] load {symbol} {BAR} {CANDLE_LIMIT} candles")
        instrument = client.get_instrument(symbol)
        candles = _load_backtest_candles(client, symbol, BAR, CANDLE_LIMIT)
        data_source_note = _build_backtest_data_source_note(client)

        original = _run_backtest_with_loaded_data(
            candles,
            instrument,
            build_config(symbol, slope_filter_enabled=False),
            data_source_note=data_source_note,
            maker_fee_rate=MAKER_FEE_RATE,
            taker_fee_rate=TAKER_FEE_RATE,
        )
        filtered = _run_backtest_with_loaded_data(
            candles,
            instrument,
            build_config(symbol, slope_filter_enabled=True),
            data_source_note=data_source_note,
            maker_fee_rate=MAKER_FEE_RATE,
            taker_fee_rate=TAKER_FEE_RATE,
        )
        rows.append(
            CompareRow(
                symbol=symbol,
                symbol_label=SYMBOL_LABELS.get(symbol, symbol),
                original_pnl=original.report.total_pnl,
                filtered_pnl=filtered.report.total_pnl,
                original_win_rate=original.report.win_rate,
                filtered_win_rate=filtered.report.win_rate,
                original_trades=original.report.total_trades,
                filtered_trades=filtered.report.total_trades,
                original_max_drawdown=original.report.max_drawdown,
                filtered_max_drawdown=filtered.report.max_drawdown,
                original_avg_r=original.report.average_r_multiple,
                filtered_avg_r=filtered.report.average_r_multiple,
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
                "original_pnl",
                "filtered_pnl",
                "pnl_delta",
                "original_win_rate",
                "filtered_win_rate",
                "win_rate_delta",
                "original_trades",
                "filtered_trades",
                "trades_delta",
                "original_max_drawdown",
                "filtered_max_drawdown",
                "drawdown_delta",
                "original_avg_r",
                "filtered_avg_r",
            ]
        )
        for row in rows:
            writer.writerow(
                [
                    row.symbol,
                    row.symbol_label,
                    format_decimal_fixed(row.original_pnl, 4),
                    format_decimal_fixed(row.filtered_pnl, 4),
                    format_decimal_fixed(row.pnl_delta, 4),
                    format_decimal_fixed(row.original_win_rate, 2),
                    format_decimal_fixed(row.filtered_win_rate, 2),
                    format_decimal_fixed(row.win_rate_delta, 2),
                    row.original_trades,
                    row.filtered_trades,
                    row.trades_delta,
                    format_decimal_fixed(row.original_max_drawdown, 4),
                    format_decimal_fixed(row.filtered_max_drawdown, 4),
                    format_decimal_fixed(row.drawdown_delta, 4),
                    format_decimal_fixed(row.original_avg_r, 4),
                    format_decimal_fixed(row.filtered_avg_r, 4),
                ]
            )


def _sum_decimal(values: list[Decimal]) -> Decimal:
    return sum(values, Decimal("0"))


def build_markdown_report(
    rows: list[CompareRow],
    *,
    exported_at: datetime,
    csv_path: Path,
    json_path: Path,
    data_source_note: str,
) -> str:
    total_original_pnl = _sum_decimal([row.original_pnl for row in rows])
    total_filtered_pnl = _sum_decimal([row.filtered_pnl for row in rows])
    total_original_trades = sum(row.original_trades for row in rows)
    total_filtered_trades = sum(row.filtered_trades for row in rows)
    improved = sum(1 for row in rows if row.pnl_delta > 0)
    worsened = sum(1 for row in rows if row.pnl_delta < 0)
    unchanged = len(rows) - improved - worsened
    lines = [
        "# 1H 五币种 EMA21 + MA55 动态委托做多：慢线回归斜率过滤对比",
        "",
        f"生成时间：{exported_at.strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "## 口径",
        "",
        "- 原版：快线 EMA21 / 慢线 MA55 / 挂单参考 MA55 / ATR10 / SL=2ATR / 动态止盈 / 单笔风险 100。",
        f"- 新版：在原版基础上增加过滤，若最近 {TREND_SLOPE_LOOKBACK} 根慢线 MA55 的线性回归斜率 < 0，则不产生新的做多委托。",
        "- 费用：Maker 0.01%，Taker 0.028%。",
        f"- 数据：{data_source_note or 'OKX 历史K线/本地缓存'}。",
        f"- 原始明细：[CSV]({csv_path})；[JSON]({json_path})。",
        "",
        "## 总览",
        "",
        f"- 总盈亏：原版 {format_decimal_fixed(total_original_pnl, 4)}，新版 {format_decimal_fixed(total_filtered_pnl, 4)}，变化 {format_decimal_fixed(total_filtered_pnl - total_original_pnl, 4)}。",
        f"- 总交易数：原版 {total_original_trades}，新版 {total_filtered_trades}，变化 {total_filtered_trades - total_original_trades}。",
        f"- 分币种盈亏改善/变差/持平：{improved}/{worsened}/{unchanged}。",
        "",
        "## 分币种",
        "",
        "| 币种 | 原盈亏 | 新盈亏 | 盈亏变化 | 原胜率 | 新胜率 | 交易数变化 | 原回撤 | 新回撤 | 回撤变化 |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in rows:
        lines.append(
            f"| {row.symbol_label} | {format_decimal_fixed(row.original_pnl, 4)} | "
            f"{format_decimal_fixed(row.filtered_pnl, 4)} | {format_decimal_fixed(row.pnl_delta, 4)} | "
            f"{format_decimal_fixed(row.original_win_rate, 2)}% | {format_decimal_fixed(row.filtered_win_rate, 2)}% | "
            f"{row.trades_delta} | {format_decimal_fixed(row.original_max_drawdown, 4)} | "
            f"{format_decimal_fixed(row.filtered_max_drawdown, 4)} | {format_decimal_fixed(row.drawdown_delta, 4)} |"
        )
    lines.extend(["", "## 结论", ""])
    if total_filtered_pnl > total_original_pnl:
        lines.append("- 新过滤条件在这轮 MA55 样本中提升了总盈亏，可以继续观察它是否同时改善回撤。")
    elif total_filtered_pnl < total_original_pnl:
        lines.append("- 新过滤条件在这轮 MA55 样本中降低了总盈亏，更像是过度过滤，需要谨慎上线。")
    else:
        lines.append("- 新过滤条件在这轮 MA55 样本中没有改变总盈亏。")
    lines.append("- 因为 MA 比 EMA 更慢，肉眼看到“线还在往下”但价格已经重新站上去的情况会更多，所以这套过滤通常比 EMA55 版本更有存在感。")
    return "\n".join(lines) + "\n"


def main() -> int:
    exported_at = datetime.now()
    client = OkxRestClient()
    rows, data_source_note = run_compare(client)
    timestamp = exported_at.strftime("%Y%m%d_%H%M%S")
    csv_path = REPORTS_DIR / f"ema21_ma55_long_slow_slope_filter_compare_{timestamp}.csv"
    json_path = REPORTS_DIR / f"ema21_ma55_long_slow_slope_filter_compare_{timestamp}.json"
    md_path = REPORTS_DIR / f"ema21_ma55_long_slow_slope_filter_compare_{timestamp}.md"

    export_csv(csv_path, rows)
    json_path.write_text(
        json.dumps(
            [
                {
                    "symbol": row.symbol,
                    "symbol_label": row.symbol_label,
                    "original_pnl": format_decimal_fixed(row.original_pnl, 4),
                    "filtered_pnl": format_decimal_fixed(row.filtered_pnl, 4),
                    "pnl_delta": format_decimal_fixed(row.pnl_delta, 4),
                    "original_win_rate": format_decimal_fixed(row.original_win_rate, 2),
                    "filtered_win_rate": format_decimal_fixed(row.filtered_win_rate, 2),
                    "win_rate_delta": format_decimal_fixed(row.win_rate_delta, 2),
                    "original_trades": row.original_trades,
                    "filtered_trades": row.filtered_trades,
                    "trades_delta": row.trades_delta,
                    "original_max_drawdown": format_decimal_fixed(row.original_max_drawdown, 4),
                    "filtered_max_drawdown": format_decimal_fixed(row.filtered_max_drawdown, 4),
                    "drawdown_delta": format_decimal_fixed(row.drawdown_delta, 4),
                    "original_avg_r": format_decimal_fixed(row.original_avg_r, 4),
                    "filtered_avg_r": format_decimal_fixed(row.filtered_avg_r, 4),
                }
                for row in rows
            ],
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8-sig",
    )
    md_path.write_text(
        build_markdown_report(
            rows,
            exported_at=exported_at,
            csv_path=csv_path,
            json_path=json_path,
            data_source_note=data_source_note,
        ),
        encoding="utf-8-sig",
    )

    print(f"report -> {md_path}")
    print(f"csv -> {csv_path}")
    print(f"json -> {json_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
