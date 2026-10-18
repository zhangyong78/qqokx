from __future__ import annotations

import bisect
import csv
import sqlite3
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from okx_quant.indicators import ema, sma
from okx_quant.models import Candle
from okx_quant.area_shape_trade import _chart_patterns_by_candle_ts
from research.auto_area_shape_backtest import ASSETS, DB_PATH, HOUR_MS, OUTPUT_DIR as BASE_OUTPUT, load_candles, run_side, summarize


OUTPUT_DIR = Path(r"D:\qqokx\reports\auto_area_ma_filter_backtest_20261018")
VARIANT_LABELS = {
    "baseline": "无均线过滤",
    "1h_ema21_ema55": "1H EMA21/EMA55",
    "4h_ema15_sma50": "4H EMA15/SMA50",
}


def load_4h(asset: str) -> list[Candle]:
    with sqlite3.connect(DB_PATH) as connection:
        rows = connection.execute(
            """SELECT ts, open, high, low, close, volume FROM candles
               WHERE inst_id = ? AND bar = '4H' AND confirmed = 1 ORDER BY ts""",
            (f"{asset}-USDT-SWAP",),
        ).fetchall()
    return [Candle(int(ts), *(Decimal(str(value)) for value in (op, high, low, close, volume)), True)
            for ts, op, high, low, close, volume in rows]


def ma_filter_1h(candles: list[Candle]) -> dict[int, str]:
    closes = [item.close for item in candles]
    fast, slow = ema(closes, 21), ema(closes, 55)
    result: dict[int, str] = {}
    for index, candle in enumerate(candles):
        if fast[index] > slow[index]:
            result[candle.ts] = "long"
        elif fast[index] < slow[index]:
            result[candle.ts] = "short"
    return result


def ma_filter_4h(candles_1h: list[Candle], candles_4h: list[Candle]) -> dict[int, str]:
    closes = [item.close for item in candles_4h]
    fast, slow = ema(closes, 15), sma(closes, 50)
    completed_at = [item.ts + 4 * HOUR_MS for item in candles_4h]
    result: dict[int, str] = {}
    for candle in candles_1h:
        one_hour_close = candle.ts + HOUR_MS
        index = bisect.bisect_right(completed_at, one_hour_close) - 1
        if index < 0 or slow[index] is None:
            continue
        if fast[index] > slow[index]:
            result[candle.ts] = "long"
        elif fast[index] < slow[index]:
            result[candle.ts] = "short"
    return result


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    summaries: list[dict[str, object]] = []
    all_trades: list[dict[str, object]] = []
    for asset in ASSETS:
        candles = load_candles(asset)
        patterns = _chart_patterns_by_candle_ts(candles)
        filters = {
            "baseline": None,
            "1h_ema21_ema55": ma_filter_1h(candles),
            "4h_ema15_sma50": ma_filter_4h(candles, load_4h(asset)),
        }
        for variant, filter_by_ts in filters.items():
            variant_trades: list[dict[str, object]] = []
            for side in ("long", "short"):
                trades = run_side(asset, side, candles, patterns, filter_by_ts)
                for item in trades:
                    item["variant"] = variant
                    item["variant_label"] = VARIANT_LABELS[variant]
                all_trades.extend(trades)
                variant_trades.extend(trades)
                row = summarize(asset, side, trades, candles)
                summaries.append({"variant": variant, **row})
            combined = summarize(asset, "both_independent", variant_trades, candles)
            summaries.append({"variant": variant, **combined})
        print(f"{asset}: {len(candles)} 1H bars; all three variants completed")

    with (OUTPUT_DIR / "summary.csv").open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(summaries[0]))
        writer.writeheader()
        writer.writerows(summaries)
    with (OUTPUT_DIR / "trades.csv").open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(all_trades[0]))
        writer.writeheader()
        writer.writerows(all_trades)

    lines = [
        "# 自动区域策略均线过滤对比（1H 信号）", "",
        f"行情来自 `{DB_PATH}`；回测规则、区域参数、形态信号、信号K高低点止损、下一根1H开盘、1R止盈、对侧区域至少2R空间、手续费、止损优先及计划有效期均与 [上一轮回测](../auto_area_shape_backtest_20261018/report.md) 相同。",
        "",
        "均线规则：仅在反转信号 K 收盘确认快慢线排列；快线>慢线仅做多，快线<慢线仅做空。对比无过滤、1H EMA21/EMA55、4H EMA15/SMA50。4H 过滤严格使用在 1H 信号 K 收盘时已经确认的最后一根 4H K 线，不读取未收盘 4H 线。",
        "",
        "## 两组均线结果（双向独立统计）", "",
        "| 币种 | MA组 | 交易数 | 胜率 | 平均净R | 累计净R | PF | 最大回撤R | 估算净USDT |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for variant in ("1h_ema21_ema55", "4h_ema15_sma50"):
        for row in summaries:
            if row["variant"] != variant or row["side"] != "both_independent":
                continue
            lines.append(
                f"| {row['asset']} | {VARIANT_LABELS[variant]} | {row['trades']} | {Decimal(str(row['win_rate_pct'])):.1f}% | {Decimal(str(row['avg_net_r'])):.3f} | {Decimal(str(row['total_net_r'])):.2f} | {Decimal(str(row['profit_factor'])):.2f} | {Decimal(str(row['max_drawdown_r'])):.2f} | {Decimal(str(row['est_net_usdt_at_10_risk'])):.2f} |"
            )
    lines.extend([
        "", "## 无均线过滤基准", "",
        "| 币种 | 交易数 | 胜率 | 平均净R | 累计净R | PF |",
        "|---|---:|---:|---:|---:|---:|",
    ])
    for row in summaries:
        if row["variant"] == "baseline" and row["side"] == "both_independent":
            lines.append(
                f"| {row['asset']} | {row['trades']} | {Decimal(str(row['win_rate_pct'])):.1f}% | {Decimal(str(row['avg_net_r'])):.3f} | {Decimal(str(row['total_net_r'])):.2f} | {Decimal(str(row['profit_factor'])):.2f} |"
            )
    lines.extend([
        "", "## 跨币种独立汇总", "",
        "| MA组 | 交易数 | 胜率 | 平均净R/笔 | 累计净R | PF | 固定10U风险折算净额 |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ])
    for variant in ("baseline", "1h_ema21_ema55", "4h_ema15_sma50"):
        rows = [row for row in all_trades if row.get("variant") == variant]
        net_values = [Decimal(str(row["net_r"])) for row in rows]
        positive = sum((value for value in net_values if value > 0), Decimal("0"))
        negative = abs(sum((value for value in net_values if value < 0), Decimal("0")))
        winners = sum(value > 0 for value in net_values)
        total_net = sum(net_values, Decimal("0"))
        pf = positive / negative if negative else Decimal("0")
        lines.append(
            f"| {VARIANT_LABELS[variant]} | {len(rows)} | {Decimal(winners) / Decimal(len(rows)) * 100 if rows else Decimal('0'):.1f}% | {total_net / Decimal(len(rows)) if rows else Decimal('0'):.3f} | {total_net:.2f} | {pf:.2f} | {total_net * Decimal('10'):.2f} USDT |"
        )
    lines.extend([
        "", "多空分项、毛R/手续费R、样本区间见 `summary.csv`；逐笔记录见 `trades.csv`。`both_independent` 是多空两套独立策略结果相加，不代表限制为单仓的组合资金曲线。固定单笔 10 USDT 风险只是线性折算，不含合约最小张数取整、滑点及资金费。", "",
    ])
    (OUTPUT_DIR / "report.md").write_text("\n".join(lines), encoding="utf-8")
    print(f"Report: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
