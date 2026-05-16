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
from okx_quant.strategy_catalog import STRATEGY_DYNAMIC_ID


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
SIGNALS = (("long_only", "做多"), ("short_only", "做空"))
AVERAGE_PROFILES = (
    ("EMA_EMA", "21EMA/55EMA"),
    ("MA_MA", "21MA/55MA"),
    ("MA_EMA", "21MA/55EMA"),
)
CANDLE_LIMIT = 10000
MAKER_FEE_RATE = Decimal("0.0001")
TAKER_FEE_RATE = Decimal("0.00028")


@dataclass(frozen=True)
class Row:
    profile_id: str
    profile_label: str
    symbol: str
    symbol_label: str
    bar: str
    bar_label: str
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


def mixed_ma_ema(values: list[Decimal], period: int) -> list[Decimal]:
    if period == 21:
        return sma_as_series(values, period)
    return ema(values, period)


@contextmanager
def patched_average(profile_id: str):
    original_backtest_ema = backtest_module.ema
    original_strategy_ema = ema_dynamic_module.ema
    try:
        if profile_id == "EMA_EMA":
            replacement = ema
        elif profile_id == "MA_MA":
            replacement = sma_as_series
        elif profile_id == "MA_EMA":
            replacement = mixed_ma_ema
        else:
            raise ValueError(f"Unsupported average profile: {profile_id}")
        backtest_module.ema = replacement
        ema_dynamic_module.ema = replacement
        yield
    finally:
        backtest_module.ema = original_backtest_ema
        ema_dynamic_module.ema = original_strategy_ema


def build_config(symbol: str, bar: str, signal_mode: str) -> StrategyConfig:
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


def run_cell(config: StrategyConfig, instrument, candles, data_source_note: str) -> tuple[StrategyConfig, object]:
    successes: list[tuple[StrategyConfig, object]] = []
    for cfg in build_atr_batch_configs(config):
        result = _run_backtest_with_loaded_data(
            candles,
            instrument,
            cfg,
            data_source_note=data_source_note,
            maker_fee_rate=MAKER_FEE_RATE,
            taker_fee_rate=TAKER_FEE_RATE,
        )
        successes.append((cfg, result))
    return max(successes, key=lambda item: item[1].report.total_pnl)


def run_suite(client: OkxRestClient) -> tuple[list[Row], str]:
    cache = load_market_data(client)
    rows: list[Row] = []
    data_source_note = ""
    total = len(AVERAGE_PROFILES) * len(SYMBOLS) * len(TIMEFRAMES) * len(SIGNALS)
    step = 0
    for profile_id, profile_label in AVERAGE_PROFILES:
        with patched_average(profile_id):
            for symbol in SYMBOLS:
                symbol_label = SYMBOL_LABELS[symbol]
                for bar, bar_label in TIMEFRAMES:
                    instrument, candles, note = cache[(symbol, bar)]
                    data_source_note = note
                    for signal_mode, direction in SIGNALS:
                        step += 1
                        print(f"[{step}/{total}] {profile_label} {symbol_label} {bar_label} {direction}")
                        best_cfg, best_result = run_cell(build_config(symbol, bar, signal_mode), instrument, candles, note)
                        rows.append(
                            Row(
                                profile_id=profile_id,
                                profile_label=profile_label,
                                symbol=symbol,
                                symbol_label=symbol_label,
                                bar=bar,
                                bar_label=bar_label,
                                direction=direction,
                                best_sl=best_cfg.atr_stop_multiplier,
                                best_tp=best_cfg.atr_take_multiplier,
                                total_pnl=best_result.report.total_pnl,
                                win_rate=best_result.report.win_rate,
                                total_trades=best_result.report.total_trades,
                                max_drawdown=best_result.report.max_drawdown,
                            )
                        )
    return rows, data_source_note


def export_csv(path: Path, rows: list[Row]) -> None:
    with path.open("w", newline="", encoding="utf-8-sig") as fp:
        writer = csv.writer(fp)
        writer.writerow(
            [
                "profile_id",
                "profile_label",
                "symbol",
                "symbol_label",
                "bar",
                "bar_label",
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
                    row.profile_id,
                    row.profile_label,
                    row.symbol,
                    row.symbol_label,
                    row.bar,
                    row.bar_label,
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


def _best(rows: list[Row]) -> Row:
    return max(rows, key=lambda row: (row.pnl_dd_ratio, row.total_pnl))


def build_report(rows: list[Row], exported_at: datetime, csv_path: Path, data_source_note: str) -> str:
    grouped: dict[tuple[str, str, str], list[Row]] = {}
    for row in rows:
        grouped.setdefault((row.symbol, row.bar, row.direction), []).append(row)

    long_groups = [value for key, value in grouped.items() if key[2] == "做多"]
    short_groups = [value for key, value in grouped.items() if key[2] == "做空"]

    def win_counts(groups: list[list[Row]]) -> dict[str, int]:
        counts = {label: 0 for _, label in AVERAGE_PROFILES}
        for group in groups:
            counts[_best(group).profile_label] += 1
        return counts

    long_counts = win_counts(long_groups)
    short_counts = win_counts(short_groups)
    all_counts = win_counts(list(grouped.values()))

    lines = [
        "# 单周期三组均线对比报告",
        "",
        f"生成时间：{exported_at.strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "测试范围：五币种，1H/4H，单周期动态委托，做多/做空。",
        "",
        "三组均线：`21EMA/55EMA`、`21MA/55MA`、`21MA/55EMA`。",
        "",
        "排序口径：优先看`盈亏/回撤比`，其次看`总盈亏`。这更贴近实盘，而不是只追最高收益。",
        "",
        f"数据来源：{data_source_note}",
        "",
        f"全量结果：[CSV]({csv_path})",
        "",
        "## 结论先看",
        "",
        f"- 全部 20 个单元里：`21EMA/55EMA` 胜出 {all_counts['21EMA/55EMA']} 个，`21MA/55MA` 胜出 {all_counts['21MA/55MA']} 个，`21MA/55EMA` 胜出 {all_counts['21MA/55EMA']} 个。",
        f"- 做多 10 个单元里：`21EMA/55EMA` 胜出 {long_counts['21EMA/55EMA']} 个，`21MA/55MA` 胜出 {long_counts['21MA/55MA']} 个，`21MA/55EMA` 胜出 {long_counts['21MA/55EMA']} 个。",
        f"- 做空 10 个单元里：`21EMA/55EMA` 胜出 {short_counts['21EMA/55EMA']} 个，`21MA/55MA` 胜出 {short_counts['21MA/55MA']} 个，`21MA/55EMA` 胜出 {short_counts['21MA/55EMA']} 个。",
        "",
        "## 做多对比",
        "",
        "| 币种 | 周期 | 21EMA/55EMA | 21MA/55MA | 21MA/55EMA | 建议 |",
        "| --- | --- | ---: | ---: | ---: | --- |",
    ]
    for symbol in SYMBOLS:
        for bar, bar_label in TIMEFRAMES:
            group = grouped[(symbol, bar, "做多")]
            by_profile = {row.profile_label: row for row in group}
            best = _best(group)
            lines.append(
                f"| {SYMBOL_LABELS[symbol]} | {bar_label} | "
                f"{format_decimal_fixed(by_profile['21EMA/55EMA'].total_pnl, 4)} / {format_decimal_fixed(by_profile['21EMA/55EMA'].pnl_dd_ratio, 4)} | "
                f"{format_decimal_fixed(by_profile['21MA/55MA'].total_pnl, 4)} / {format_decimal_fixed(by_profile['21MA/55MA'].pnl_dd_ratio, 4)} | "
                f"{format_decimal_fixed(by_profile['21MA/55EMA'].total_pnl, 4)} / {format_decimal_fixed(by_profile['21MA/55EMA'].pnl_dd_ratio, 4)} | "
                f"{best.profile_label} |"
            )

    lines.extend(
        [
            "",
            "表格格式：`总盈亏 / 盈亏回撤比`。",
            "",
            "## 做空对比",
            "",
            "| 币种 | 周期 | 21EMA/55EMA | 21MA/55MA | 21MA/55EMA | 建议 |",
            "| --- | --- | ---: | ---: | ---: | --- |",
        ]
    )
    for symbol in SYMBOLS:
        for bar, bar_label in TIMEFRAMES:
            group = grouped[(symbol, bar, "做空")]
            by_profile = {row.profile_label: row for row in group}
            best = _best(group)
            lines.append(
                f"| {SYMBOL_LABELS[symbol]} | {bar_label} | "
                f"{format_decimal_fixed(by_profile['21EMA/55EMA'].total_pnl, 4)} / {format_decimal_fixed(by_profile['21EMA/55EMA'].pnl_dd_ratio, 4)} | "
                f"{format_decimal_fixed(by_profile['21MA/55MA'].total_pnl, 4)} / {format_decimal_fixed(by_profile['21MA/55MA'].pnl_dd_ratio, 4)} | "
                f"{format_decimal_fixed(by_profile['21MA/55EMA'].total_pnl, 4)} / {format_decimal_fixed(by_profile['21MA/55EMA'].pnl_dd_ratio, 4)} | "
                f"{best.profile_label} |"
            )

    lines.extend(
        [
            "",
            "## 交易员解释",
            "",
            "- `21EMA/55EMA`：反应最快，适合趋势启动和空头加速，但震荡里更容易被假动作影响。",
            "- `21MA/55MA`：最平滑，适合中趋势回调上车，通常对做多更友好。",
            "- `21MA/55EMA`：折中型。21 用 MA 过滤短期噪音，55 和挂单锚点仍用 EMA 保持趋势线灵敏度。",
            "",
            "## 实盘判断",
            "",
            "如果某个单元 `21MA/55EMA` 胜出，说明市场需要过滤短期噪音，但不能让 55 趋势线太慢。",
            "如果 `21MA/55MA` 胜出，说明这类结构更需要整体平滑。",
            "如果 `21EMA/55EMA` 胜出，说明这类结构更依赖速度，尤其是空头或高周期趋势段。",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> int:
    exported_at = datetime.now()
    client = OkxRestClient()
    rows, data_source_note = run_suite(client)

    timestamp = exported_at.strftime("%Y%m%d_%H%M%S")
    csv_path = REPORTS_DIR / f"single_cycle_three_ma_compare_{timestamp}.csv"
    md_path = REPORTS_DIR / f"单周期三组均线对比报告_{timestamp}.md"
    json_path = REPORTS_DIR / f"single_cycle_three_ma_compare_{timestamp}.json"

    export_csv(csv_path, rows)
    md_path.write_text(build_report(rows, exported_at, csv_path, data_source_note), encoding="utf-8-sig")
    json_path.write_text(
        json.dumps(
            [
                {
                    "profile_id": row.profile_id,
                    "profile_label": row.profile_label,
                    "symbol": row.symbol,
                    "symbol_label": row.symbol_label,
                    "bar": row.bar,
                    "bar_label": row.bar_label,
                    "direction": row.direction,
                    "best_sl": format_decimal(row.best_sl),
                    "best_tp": format_decimal(row.best_tp),
                    "total_pnl": format_decimal_fixed(row.total_pnl, 4),
                    "win_rate": format_decimal_fixed(row.win_rate, 2),
                    "total_trades": row.total_trades,
                    "max_drawdown": format_decimal_fixed(row.max_drawdown, 4),
                    "pnl_dd_ratio": format_decimal_fixed(row.pnl_dd_ratio, 4),
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
