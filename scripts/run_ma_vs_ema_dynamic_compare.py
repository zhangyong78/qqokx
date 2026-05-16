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

SYMBOL = "ETH-USDT-SWAP"
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


def build_config(*, bar: str, strategy_id: str, signal_mode: str) -> StrategyConfig:
    return StrategyConfig(
        inst_id=SYMBOL,
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
        if average_type == "EMA":
            replacement = ema
        elif average_type == "MA":
            replacement = sma_as_series
        else:
            raise ValueError(f"Unsupported average type: {average_type}")
        backtest_module.ema = replacement
        ema_dynamic_module.ema = replacement
        yield
    finally:
        backtest_module.ema = original_backtest_ema
        ema_dynamic_module.ema = original_strategy_ema


def load_market_data(client: OkxRestClient) -> dict[str, tuple[object, list, str]]:
    cache: dict[str, tuple[object, list, str]] = {}
    instrument = client.get_instrument(SYMBOL)
    for bar, bar_label in TIMEFRAMES:
        print(f"load {SYMBOL} {bar_label} {CANDLE_LIMIT} candles")
        candles = _load_backtest_candles(client, SYMBOL, bar, CANDLE_LIMIT)
        note = _build_backtest_data_source_note(client)
        cache[bar] = (instrument, candles, note)
    return cache


def run_compare(client: OkxRestClient) -> tuple[list[CompareRow], str]:
    cache = load_market_data(client)
    rows: list[CompareRow] = []
    total = len(AVERAGE_TYPES) * len(TIMEFRAMES) * len(DIRECTIONS)
    step = 0
    data_source_note = ""
    for average_type in AVERAGE_TYPES:
        with patched_average(average_type):
            for bar, bar_label in TIMEFRAMES:
                instrument, candles, note = cache[bar]
                data_source_note = note
                for strategy_id, signal_mode, direction_label in DIRECTIONS:
                    step += 1
                    print(
                        f"[{step}/{total}] {average_type} | {bar_label} | {direction_label} | "
                        f"{EMA_PERIOD}/{TREND_EMA_PERIOD} | 挂单{ENTRY_REFERENCE_EMA_PERIOD} | "
                        f"SLx{format_decimal(ATR_STOP_MULTIPLIER)} | 动态止盈"
                    )
                    result = _run_backtest_with_loaded_data(
                        candles,
                        instrument,
                        build_config(bar=bar, strategy_id=strategy_id, signal_mode=signal_mode),
                        data_source_note=note,
                        maker_fee_rate=MAKER_FEE_RATE,
                        taker_fee_rate=TAKER_FEE_RATE,
                    )
                    rows.append(
                        CompareRow(
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


def build_text_report(rows: list[CompareRow], data_source_note: str, exported_at: datetime, csv_path: Path) -> str:
    lines = [
        "ETH MA vs EMA 动态委托对比报告",
        "=" * 88,
        f"导出时间：{exported_at.strftime('%Y-%m-%d %H:%M:%S')}",
        f"标的：{SYMBOL}",
        "测试目标：比较 MA 与 EMA 在同一参数下的差异",
        "固定参数：21 / 55 | 挂单55 | 2ATR止损 | 动态止盈 | 2R保本=开启 | 手续费偏移=开启 | 每波开仓次数=1",
        "周期：1H、4H",
        "方向：做多、做空",
        f"数据来源：{data_source_note}",
        f"全量结果：{csv_path}",
        "",
        "一、结果总表",
    ]

    for bar, bar_label in TIMEFRAMES:
        for direction_label in ("做多", "做空"):
            subset = [row for row in rows if row.bar == bar and row.direction == direction_label]
            subset.sort(key=lambda row: row.average_type)
            lines.append(f"{bar_label} | {direction_label}")
            for row in subset:
                lines.append(
                    f"{row.average_type} | 总盈亏 {format_decimal_fixed(row.total_pnl, 4)} | "
                    f"胜率 {format_decimal_fixed(row.win_rate, 2)}% | 交易数 {row.total_trades} | "
                    f"最大回撤 {format_decimal_fixed(row.max_drawdown, 4)} | "
                    f"盈亏/回撤 {format_decimal_fixed(row.pnl_dd_ratio, 4)} | 平均R {format_decimal_fixed(row.avg_r, 4)}"
                )
            if len(subset) == 2:
                pnl_winner = max(subset, key=lambda row: row.total_pnl)
                quality_winner = max(subset, key=lambda row: row.pnl_dd_ratio)
                lines.append(
                    f"结论：收益更高={pnl_winner.average_type}；风险收益效率更高={quality_winner.average_type}"
                )
            lines.append("")

    lines.extend(
        [
            "二、交易员解读",
            "1. 这次对比只替换均线算法，其余参数完全一致，因此差异可以主要归因于 MA 与 EMA 的响应速度不同。",
            "2. EMA 更贴近最新价格，通常更容易给出更快的趋势确认和更积极的挂单参考。",
            "3. MA 更平滑、滞后更强，通常更偏过滤噪音，但也可能错过一部分趋势起点。",
            "4. 动态止盈环境里，不只要看总收益，还要看谁更容易把仓位留到更高 R 段，因此盈亏回撤比和平均R同样重要。",
        ]
    )
    return "\n".join(lines) + "\n"


def build_markdown_report(rows: list[CompareRow], exported_at: datetime, csv_path: Path) -> str:
    def find_row(average_type: str, bar: str, direction: str) -> CompareRow:
        return next(row for row in rows if row.average_type == average_type and row.bar == bar and row.direction == direction)

    lines = [
        "# ETH MA vs EMA 对比报告",
        "",
        f"生成时间：{exported_at.strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        f"标的：`{SYMBOL}`",
        "",
        "固定参数：`21/55 + 挂单55 + 2ATR止损 + 动态止盈`",
        "",
        f"原始结果：[CSV]({csv_path})",
        "",
        "## 结果表",
        "",
        "| 周期 | 方向 | 均线 | 总盈亏 | 胜率 | 交易数 | 最大回撤 | 盈亏/回撤 | 平均R |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for bar, bar_label in TIMEFRAMES:
        for direction_label in ("做多", "做空"):
            for average_type in AVERAGE_TYPES:
                row = find_row(average_type, bar, direction_label)
                lines.append(
                    f"| {bar_label} | {direction_label} | {average_type} | "
                    f"{format_decimal_fixed(row.total_pnl, 4)} | {format_decimal_fixed(row.win_rate, 2)}% | "
                    f"{row.total_trades} | {format_decimal_fixed(row.max_drawdown, 4)} | "
                    f"{format_decimal_fixed(row.pnl_dd_ratio, 4)} | {format_decimal_fixed(row.avg_r, 4)} |"
                )
    lines.extend(
        [
            "",
            "## 交易员结论",
            "",
            "1. 这次对比只改了均线算法，所以可以直接拿来判断 `MA` 和 `EMA` 在同参数下的行为差异。",
            "2. 如果 `EMA` 更强，通常说明该结构更依赖趋势启动后的快速跟随和更灵敏的挂单锚点。",
            "3. 如果 `MA` 更强，通常说明该结构更需要过滤短期噪音，宁愿牺牲一部分反应速度换稳定性。",
            "4. 动态止盈场景下，优先看谁更能在不明显放大回撤的情况下，把 `平均R` 和 `盈亏/回撤` 做上去。",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> int:
    exported_at = datetime.now()
    client = OkxRestClient()
    rows, data_source_note = run_compare(client)

    timestamp = exported_at.strftime("%Y%m%d_%H%M%S")
    csv_path = REPORTS_DIR / f"ETH_ma_vs_ema_dynamic_compare_{timestamp}.csv"
    txt_path = REPORTS_DIR / f"ETH_ma_vs_ema_dynamic_compare_{timestamp}.txt"
    md_path = REPORTS_DIR / f"ETH_ma_vs_ema_dynamic_compare_{timestamp}.md"
    json_path = REPORTS_DIR / f"ETH_ma_vs_ema_dynamic_compare_{timestamp}.json"

    export_csv(csv_path, rows)
    write_text(txt_path, build_text_report(rows, data_source_note, exported_at, csv_path))
    write_text(md_path, build_markdown_report(rows, exported_at, csv_path))
    json_path.write_text(
        json.dumps(
            [
                {
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

    print(f"report -> {txt_path}")
    print(f"report -> {md_path}")
    print(f"csv -> {csv_path}")
    print(f"json -> {json_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
