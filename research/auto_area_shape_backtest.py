from __future__ import annotations

import csv
import sqlite3
from collections import defaultdict
from dataclasses import asdict
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from okx_quant.analysis.auto_zones import find_auto_zone_pair
from okx_quant.area_shape_trade import AREA_TAKER_FEE_RATE, _chart_patterns_by_candle_ts
from okx_quant.indicators import atr
from okx_quant.models import Candle


DB_PATH = Path(r"D:\qqokx_data\cache\candle_store.db")
OUTPUT_DIR = Path(r"D:\qqokx\reports\auto_area_shape_backtest_20261018")
ASSETS = ("BTC", "ETH", "SOL", "DOGE", "BNB", "OKB")
HOUR_MS = 3_600_000
LOOKBACK = 240
PLAN_VALID_BARS = 240
MIN_SPACE_R = Decimal("2")
TP_R = Decimal("1")
STOP_OFFSET_ATR = Decimal("0")
AREA_HALF_WIDTH_ATR = Decimal("0.25")
FEE_RATE = AREA_TAKER_FEE_RATE
RISK_USDT = Decimal("10")
PATTERNS = {
    "double_reversal_up", "double_reversal_down", "long_lower_shadow", "long_upper_shadow",
    "top_fractal", "bottom_fractal",
}


def load_candles(asset: str) -> list[Candle]:
    with sqlite3.connect(DB_PATH) as connection:
        rows = connection.execute(
            """SELECT ts, open, high, low, close, volume FROM candles
               WHERE inst_id = ? AND bar = '1H' AND confirmed = 1 ORDER BY ts""",
            (f"{asset}-USDT-SWAP",),
        ).fetchall()
    return [Candle(int(ts), *(Decimal(str(value)) for value in (op, high, low, close, volume)), True)
            for ts, op, high, low, close, volume in rows]


def run_side(
    asset: str, side: str, candles: list[Candle], pattern_map: dict[int, list[tuple[str, str]]],
    filter_by_ts: dict[int, str] | None = None,
) -> list[dict[str, object]]:
    atr_values = atr(candles, 14)
    trades: list[dict[str, object]] = []
    plan_start = -1
    plan_expiry = -1
    support = resistance = None
    pending: dict[str, object] | None = None
    position: dict[str, object] | None = None
    index = LOOKBACK - 1
    while index < len(candles):
        candle = candles[index]
        if position is not None:
            direction = str(position["side"])
            stop = Decimal(str(position["stop"]))
            target = Decimal(str(position["target"]))
            stop_hit = candle.low <= stop if direction == "long" else candle.high >= stop
            target_hit = candle.high >= target if direction == "long" else candle.low <= target
            if stop_hit or target_hit:
                exit_price = stop if stop_hit else target
                gross_r = ((exit_price - Decimal(str(position["entry"]))) if direction == "long"
                           else (Decimal(str(position["entry"])) - exit_price)) / Decimal(str(position["risk"]))
                fee_r = (Decimal(str(position["entry"])) + exit_price) * FEE_RATE / Decimal(str(position["risk"]))
                net_r = gross_r - fee_r
                trades.append({
                    "asset": asset, "side": direction, "pattern": position["pattern"],
                    "zone_support": support, "zone_resistance": resistance,
                    "signal_time_utc": _time(int(position["signal_ts"])),
                    "entry_time_utc": _time(int(position["entry_ts"])), "exit_time_utc": _time(candle.ts),
                    "entry": str(position["entry"]), "stop": str(stop), "target": str(target),
                    "exit": str(exit_price), "result": "stop" if stop_hit else "target",
                    "ambiguous_stop_first": bool(stop_hit and target_hit), "gross_r": str(gross_r),
                    "fees_r": str(fee_r), "net_r": str(net_r), "net_usdt_at_10_risk": str(net_r * RISK_USDT),
                })
                position = None
            index += 1
            continue

        if pending is not None:
            signal_index = int(pending["signal_index"])
            expiry_index = plan_expiry
            entry = candle.open
            stop = Decimal(str(pending["stop"]))
            valid_direction = stop < entry if side == "long" else stop > entry
            if index <= expiry_index and valid_direction:
                risk = abs(entry - stop)
                target_line = resistance if side == "long" else support
                buffer = entry * FEE_RATE
                available = ((Decimal(str(target_line)) - entry - buffer) if side == "long"
                             else (entry - Decimal(str(target_line)) - buffer))
                space_r = available / risk if risk > 0 else Decimal("-1")
                if space_r >= MIN_SPACE_R:
                    target = entry + risk * TP_R + entry * FEE_RATE if side == "long" else entry - risk * TP_R - entry * FEE_RATE
                    position = {
                        "side": side, "signal_index": signal_index, "signal_ts": int(pending["signal_ts"]),
                        "entry_ts": candle.ts, "entry": entry, "stop": stop, "risk": risk,
                        "target": target, "pattern": pending["pattern"],
                    }
                    # next-open entries are active during the remainder of their entry candle.
                    stop_hit = candle.low <= stop if side == "long" else candle.high >= stop
                    target_hit = candle.high >= target if side == "long" else candle.low <= target
                    if stop_hit or target_hit:
                        exit_price = stop if stop_hit else target
                        gross_r = ((exit_price - entry) if side == "long" else (entry - exit_price)) / risk
                        fee_r = (entry + exit_price) * FEE_RATE / risk
                        net_r = gross_r - fee_r
                        trades.append({
                            "asset": asset, "side": side, "pattern": pending["pattern"],
                            "zone_support": support, "zone_resistance": resistance,
                            "signal_time_utc": _time(int(pending["signal_ts"])),
                            "entry_time_utc": _time(candle.ts), "exit_time_utc": _time(candle.ts),
                            "entry": str(entry), "stop": str(stop), "target": str(target),
                            "exit": str(exit_price), "result": "stop" if stop_hit else "target",
                            "ambiguous_stop_first": bool(stop_hit and target_hit), "gross_r": str(gross_r),
                            "fees_r": str(fee_r), "net_r": str(net_r), "net_usdt_at_10_risk": str(net_r * RISK_USDT),
                        })
                        position = None
            pending = None
            index += 1
            continue

        if plan_start < 0 or index > plan_expiry:
            try:
                zones = find_auto_zone_pair(candles[max(0, index - LOOKBACK + 1): index + 1])
            except ValueError:
                index += 1
                continue
            plan_start = index
            plan_expiry = index + PLAN_VALID_BARS
            support, resistance = zones.support.price, zones.resistance.price
            index += 1  # activation uses the current closed candle; it cannot also signal.
            continue

        if index > plan_expiry:
            index += 1
            continue
        current_atr = atr_values[index]
        if current_atr is None or current_atr <= 0:
            index += 1
            continue
        half_width = current_atr * AREA_HALF_WIDTH_ATR
        if side == "long":
            touched = candle.high >= support - half_width and candle.low <= support + half_width
            expected_pattern = {"double_reversal_up", "long_lower_shadow", "bottom_fractal"}
        else:
            touched = candle.high >= resistance - half_width and candle.low <= resistance + half_width
            expected_pattern = {"double_reversal_down", "long_upper_shadow", "top_fractal"}
        matching = next((pattern for pattern, direction in pattern_map.get(candle.ts, ())
                         if direction == side and pattern in PATTERNS.intersection(expected_pattern)), None)
        filter_passes = filter_by_ts is None or filter_by_ts.get(candle.ts) == side
        if touched and matching is not None and filter_passes and index + 1 <= plan_expiry:
            stop = candle.low if side == "long" else candle.high
            pending = {"signal_index": index, "signal_ts": candle.ts, "stop": stop, "pattern": matching}
        index += 1
    return trades


def _time(ts: int) -> str:
    return datetime.fromtimestamp(ts / 1000, tz=timezone.utc).isoformat()


def summarize(asset: str, side: str, trades: list[dict[str, object]], candles: list[Candle]) -> dict[str, object]:
    trades = sorted(trades, key=lambda item: str(item["exit_time_utc"]))
    net = [Decimal(str(item["net_r"])) for item in trades]
    gross = [Decimal(str(item["gross_r"])) for item in trades]
    fees = [Decimal(str(item["fees_r"])) for item in trades]
    wins = [value for value in net if value > 0]
    losses = [value for value in net if value < 0]
    equity = peak = drawdown = Decimal("0")
    for value in net:
        equity += value
        peak = max(peak, equity)
        drawdown = max(drawdown, peak - equity)
    gross_profit = sum(wins, Decimal("0"))
    gross_loss = abs(sum(losses, Decimal("0")))
    return {
        "asset": asset, "side": side, "bars": len(candles),
        "data_start_utc": _time(candles[0].ts) if candles else "",
        "data_end_utc": _time(candles[-1].ts) if candles else "",
        "trades": len(trades), "win_rate_pct": str((Decimal(len(wins)) / Decimal(len(trades)) * 100) if trades else Decimal("0")),
        "avg_net_r": str(sum(net, Decimal("0")) / Decimal(len(net)) if net else Decimal("0")),
        "total_gross_r": str(sum(gross, Decimal("0"))),
        "total_fees_r": str(sum(fees, Decimal("0"))),
        "avg_fees_r": str(sum(fees, Decimal("0")) / Decimal(len(fees)) if fees else Decimal("0")),
        "total_net_r": str(sum(net, Decimal("0"))),
        "profit_factor": str(gross_profit / gross_loss if gross_loss else Decimal("Infinity") if gross_profit else Decimal("0")),
        "max_drawdown_r": str(drawdown),
        "est_net_usdt_at_10_risk": str(sum(net, Decimal("0")) * RISK_USDT),
        "ambiguous_stop_first_bars": sum(bool(item["ambiguous_stop_first"]) for item in trades),
    }


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    all_trades: list[dict[str, object]] = []
    summaries: list[dict[str, object]] = []
    for asset in ASSETS:
        candles = load_candles(asset)
        patterns = _chart_patterns_by_candle_ts(candles)
        asset_trades: list[dict[str, object]] = []
        for side in ("long", "short"):
            trades = run_side(asset, side, candles, patterns)
            all_trades.extend(trades)
            asset_trades.extend(trades)
            summaries.append(summarize(asset, side, trades, candles))
        summaries.append(summarize(asset, "both_independent", asset_trades, candles))
        print(f"{asset}: {len(candles)} candles, {len(asset_trades)} trades")

    if all_trades:
        with (OUTPUT_DIR / "trades.csv").open("w", newline="", encoding="utf-8-sig") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(all_trades[0]))
            writer.writeheader()
            writer.writerows(all_trades)
    with (OUTPUT_DIR / "summary.csv").open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(summaries[0]))
        writer.writeheader()
        writer.writerows(summaries)

    total_rows = [row for row in summaries if row["side"] == "both_independent"]
    lines = [
        "# 自动区域形态策略 1H 回测",
        "",
        f"数据来源：本地 OKX K 线库 `{DB_PATH}`，均为已确认 1H SWAP K 线。",
        "",
        "策略：每次计划用最近 240 根已收盘 K 线自动识别摆动点簇；左右各 3 根确认，摆动反向至少 0.75 ATR，同侧簇间距不超过 0.5 ATR，每簇至少 2 点。区域半宽 0.25 ATR。计划在 240 根信号有效期到期后重新识别。",
        "",
        "入场：支撑区看涨形态做多、压力区看跌形态做空；使用项目 1H 图表形态标记，确认后下一根 K 线开盘入场。止损放信号 K 线低点/高点，止盈 1R；对侧区域需提供至少 2R 空间。",
        "",
        f"费用：按项目费率 `{FEE_RATE}` 估算双边 taker 费用，滑点与资金费未计；同根 K 线止损和止盈都触及时按止损先发生。每笔按 10 USDT 初始风险折算。多空分别独立回测，`both_independent` 是两套独立结果相加，可能存在同时持仓，不是组合资金曲线。",
        "",
        "## 汇总（多空独立结果合并）",
        "",
        "| 币种 | 样本区间 UTC | K线数 | 交易数 | 胜率 | 累计毛R | 手续费R | 平均净R | 累计净R | PF | 最大回撤R | 估算净USDT |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in total_rows:
        lines.append(
            f"| {row['asset']} | {str(row['data_start_utc'])[:10]}–{str(row['data_end_utc'])[:10]} | {row['bars']} | {row['trades']} | {Decimal(str(row['win_rate_pct'])):.1f}% | {Decimal(str(row['total_gross_r'])):.2f} | {Decimal(str(row['total_fees_r'])):.2f} | {Decimal(str(row['avg_net_r'])):.3f} | {Decimal(str(row['total_net_r'])):.2f} | {row['profit_factor']} | {Decimal(str(row['max_drawdown_r'])):.2f} | {Decimal(str(row['est_net_usdt_at_10_risk'])):.2f} |"
        )
    lines.extend(["", "多空分项和逐笔明细见 `summary.csv` 与 `trades.csv`。该复盘用于核对规则行为，不含实盘执行、资金费和冲击成本。", ""])
    (OUTPUT_DIR / "report.md").write_text("\n".join(lines), encoding="utf-8")
    print(f"Report: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
