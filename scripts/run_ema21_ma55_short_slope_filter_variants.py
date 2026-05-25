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
from okx_quant.strategy_catalog import STRATEGY_DYNAMIC_SHORT_ID


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


@dataclass(frozen=True)
class Variant:
    key: str
    label: str
    slope_filter_enabled: bool
    lookback_bars: int
    min_ratio: Decimal


@dataclass(frozen=True)
class VariantResult:
    total_pnl: Decimal
    win_rate: Decimal
    total_trades: int
    max_drawdown: Decimal
    avg_r: Decimal


@dataclass(frozen=True)
class CompareRow:
    symbol: str
    symbol_label: str
    baseline: VariantResult
    lookback7: VariantResult
    threshold5: VariantResult


VARIANTS = (
    Variant(
        key="baseline",
        label="基线 MA55 做空",
        slope_filter_enabled=False,
        lookback_bars=5,
        min_ratio=Decimal("0"),
    ),
    Variant(
        key="lookback7",
        label="过滤版 7根回归>0",
        slope_filter_enabled=True,
        lookback_bars=7,
        min_ratio=Decimal("0"),
    ),
    Variant(
        key="threshold5",
        label="过滤版 5根回归>+0.0005",
        slope_filter_enabled=True,
        lookback_bars=5,
        min_ratio=Decimal("-0.0005"),
    ),
)


def build_config(symbol: str, variant: Variant) -> StrategyConfig:
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
        signal_mode="short_only",
        position_mode="net",
        environment="demo",
        tp_sl_trigger_type="mark",
        strategy_id=STRATEGY_DYNAMIC_SHORT_ID,
        risk_amount=RISK_AMOUNT,
        entry_reference_ema_type="ma",
        entry_reference_ema_period=ENTRY_REFERENCE_MA_PERIOD,
        take_profit_mode="dynamic",
        max_entries_per_trend=1,
        dynamic_two_r_break_even=True,
        dynamic_fee_offset_enabled=True,
        trend_ema_slope_filter_enabled=variant.slope_filter_enabled,
        trend_ema_slope_filter_lookback_bars=variant.lookback_bars,
        trend_ema_slope_filter_min_ratio=variant.min_ratio,
    )


def _to_result(result: object) -> VariantResult:
    report = result.report
    return VariantResult(
        total_pnl=report.total_pnl,
        win_rate=report.win_rate,
        total_trades=report.total_trades,
        max_drawdown=report.max_drawdown,
        avg_r=report.average_r_multiple,
    )


def run_compare(client: OkxRestClient) -> tuple[list[CompareRow], str]:
    rows: list[CompareRow] = []
    data_source_note = ""
    for index, symbol in enumerate(SYMBOLS, start=1):
        print(f"[{index}/{len(SYMBOLS)}] load {symbol} {BAR} {CANDLE_LIMIT} candles")
        instrument = client.get_instrument(symbol)
        candles = _load_backtest_candles(client, symbol, BAR, CANDLE_LIMIT)
        data_source_note = _build_backtest_data_source_note(client)

        results: dict[str, VariantResult] = {}
        for variant in VARIANTS:
            backtest_result = _run_backtest_with_loaded_data(
                candles,
                instrument,
                build_config(symbol, variant),
                data_source_note=data_source_note,
                maker_fee_rate=MAKER_FEE_RATE,
                taker_fee_rate=TAKER_FEE_RATE,
            )
            results[variant.key] = _to_result(backtest_result)

        rows.append(
            CompareRow(
                symbol=symbol,
                symbol_label=SYMBOL_LABELS.get(symbol, symbol),
                baseline=results["baseline"],
                lookback7=results["lookback7"],
                threshold5=results["threshold5"],
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
                "baseline_pnl",
                "lookback7_pnl",
                "lookback7_delta",
                "threshold5_pnl",
                "threshold5_delta",
                "baseline_trades",
                "lookback7_trades",
                "threshold5_trades",
                "baseline_drawdown",
                "lookback7_drawdown",
                "threshold5_drawdown",
                "baseline_win_rate",
                "lookback7_win_rate",
                "threshold5_win_rate",
            ]
        )
        for row in rows:
            writer.writerow(
                [
                    row.symbol,
                    row.symbol_label,
                    format_decimal_fixed(row.baseline.total_pnl, 4),
                    format_decimal_fixed(row.lookback7.total_pnl, 4),
                    format_decimal_fixed(row.lookback7.total_pnl - row.baseline.total_pnl, 4),
                    format_decimal_fixed(row.threshold5.total_pnl, 4),
                    format_decimal_fixed(row.threshold5.total_pnl - row.baseline.total_pnl, 4),
                    row.baseline.total_trades,
                    row.lookback7.total_trades,
                    row.threshold5.total_trades,
                    format_decimal_fixed(row.baseline.max_drawdown, 4),
                    format_decimal_fixed(row.lookback7.max_drawdown, 4),
                    format_decimal_fixed(row.threshold5.max_drawdown, 4),
                    format_decimal_fixed(row.baseline.win_rate, 2),
                    format_decimal_fixed(row.lookback7.win_rate, 2),
                    format_decimal_fixed(row.threshold5.win_rate, 2),
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
    baseline_total = _sum_decimal([row.baseline.total_pnl for row in rows])
    lookback7_total = _sum_decimal([row.lookback7.total_pnl for row in rows])
    threshold5_total = _sum_decimal([row.threshold5.total_pnl for row in rows])
    baseline_trades = sum(row.baseline.total_trades for row in rows)
    lookback7_trades = sum(row.lookback7.total_trades for row in rows)
    threshold5_trades = sum(row.threshold5.total_trades for row in rows)
    lines = [
        "# 1H 五币种 EMA21 + MA55 做空：MA55斜率温和变体对比",
        "",
        f"生成时间：{exported_at.strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "## 口径",
        "",
        "- 基线：快线 EMA21 / 慢线 MA55 / 挂单 MA55，不加慢线斜率过滤。",
        "- 方案A：最近 7 根 MA55 回归斜率 > 0 时，过滤做空。",
        "- 方案B：最近 5 根 MA55 回归斜率 / 当前 MA55 > +0.0005 时，过滤做空。",
        f"- 数据：{data_source_note or 'OKX 历史K线/本地缓存'}。",
        f"- 原始明细：[CSV]({csv_path})；[JSON]({json_path})。",
        "",
        "## 总览",
        "",
        f"- 基线：总盈亏 {format_decimal_fixed(baseline_total, 4)}，交易数 {baseline_trades}。",
        f"- 方案A：总盈亏 {format_decimal_fixed(lookback7_total, 4)}，相对基线 {format_decimal_fixed(lookback7_total - baseline_total, 4)}，交易数变化 {lookback7_trades - baseline_trades}。",
        f"- 方案B：总盈亏 {format_decimal_fixed(threshold5_total, 4)}，相对基线 {format_decimal_fixed(threshold5_total - baseline_total, 4)}，交易数变化 {threshold5_trades - baseline_trades}。",
        "",
        "## 分币种",
        "",
        "| 币种 | 基线盈亏 | 方案A盈亏 | A变化 | 方案B盈亏 | B变化 | 基线交易数 | A交易数 | B交易数 |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in rows:
        lines.append(
            f"| {row.symbol_label} | {format_decimal_fixed(row.baseline.total_pnl, 4)} | "
            f"{format_decimal_fixed(row.lookback7.total_pnl, 4)} | "
            f"{format_decimal_fixed(row.lookback7.total_pnl - row.baseline.total_pnl, 4)} | "
            f"{format_decimal_fixed(row.threshold5.total_pnl, 4)} | "
            f"{format_decimal_fixed(row.threshold5.total_pnl - row.baseline.total_pnl, 4)} | "
            f"{row.baseline.total_trades} | {row.lookback7.total_trades} | {row.threshold5.total_trades} |"
        )
    lines.extend(["", "## 结论", ""])
    best_key = max(
        {
            "基线": baseline_total,
            "方案A": lookback7_total,
            "方案B": threshold5_total,
        }.items(),
        key=lambda item: item[1],
    )[0]
    lines.append(f"- 这三版里，总盈亏最高的是 `{best_key}`。")
    lines.append("- 如果做空侧也出现“阈值版优于纯方向版”，说明加阈值比单纯看斜率正负更适合作为慢线过滤。")
    return "\n".join(lines) + "\n"


def main() -> int:
    exported_at = datetime.now()
    client = OkxRestClient()
    rows, data_source_note = run_compare(client)
    timestamp = exported_at.strftime("%Y%m%d_%H%M%S")
    csv_path = REPORTS_DIR / f"ema21_ma55_short_slope_filter_variants_{timestamp}.csv"
    json_path = REPORTS_DIR / f"ema21_ma55_short_slope_filter_variants_{timestamp}.json"
    md_path = REPORTS_DIR / f"ema21_ma55_short_slope_filter_variants_{timestamp}.md"

    export_csv(csv_path, rows)
    json_path.write_text(
        json.dumps(
            [
                {
                    "symbol": row.symbol,
                    "symbol_label": row.symbol_label,
                    "baseline": {
                        "total_pnl": format_decimal_fixed(row.baseline.total_pnl, 4),
                        "win_rate": format_decimal_fixed(row.baseline.win_rate, 2),
                        "total_trades": row.baseline.total_trades,
                        "max_drawdown": format_decimal_fixed(row.baseline.max_drawdown, 4),
                    },
                    "lookback7": {
                        "total_pnl": format_decimal_fixed(row.lookback7.total_pnl, 4),
                        "win_rate": format_decimal_fixed(row.lookback7.win_rate, 2),
                        "total_trades": row.lookback7.total_trades,
                        "max_drawdown": format_decimal_fixed(row.lookback7.max_drawdown, 4),
                    },
                    "threshold5": {
                        "total_pnl": format_decimal_fixed(row.threshold5.total_pnl, 4),
                        "win_rate": format_decimal_fixed(row.threshold5.win_rate, 2),
                        "total_trades": row.threshold5.total_trades,
                        "max_drawdown": format_decimal_fixed(row.threshold5.max_drawdown, 4),
                    },
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
