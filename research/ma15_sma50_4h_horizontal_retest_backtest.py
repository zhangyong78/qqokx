from __future__ import annotations

import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd


DB_PATH = Path(r"D:\qqokx_data\cache\candle_store.db")
OUTPUT_DIR = Path(r"D:\qqokx\reports\ma15_sma50_4h_horizontal_retest_20261017")
ASSETS = ("BTC", "ETH", "SOL", "DOGE", "BNB", "OKB")
BAR_MS = 4 * 60 * 60 * 1000
PIVOT_LEFT = 3
PIVOT_RIGHT = 3
FEE_ROUND_TRIP = 0.00036


def load_data(asset: str) -> pd.DataFrame:
    with sqlite3.connect(DB_PATH) as connection:
        rows = connection.execute(
            """
            SELECT ts, open, high, low, close, confirmed
            FROM candles
            WHERE inst_id = ? AND bar = '4H' AND confirmed = 1
            ORDER BY ts
            """,
            (f"{asset}-USDT-SWAP",),
        ).fetchall()
    frame = pd.DataFrame(rows, columns=("ts", "open", "high", "low", "close", "confirmed"))
    for column in ("open", "high", "low", "close"):
        frame[column] = frame[column].astype(float)
    frame["time"] = pd.to_datetime(frame["ts"], unit="ms", utc=True)
    return frame


def add_indicators(frame: pd.DataFrame) -> pd.DataFrame:
    close = frame["close"]
    frame["ema15"] = close.ewm(span=15, adjust=False).mean()
    frame["sma50"] = close.rolling(50, min_periods=50).mean()
    previous_close = close.shift(1)
    tr = pd.concat(
        (
            frame["high"] - frame["low"],
            (frame["high"] - previous_close).abs(),
            (frame["low"] - previous_close).abs(),
        ),
        axis=1,
    ).max(axis=1)
    frame["atr14"] = tr.ewm(alpha=1 / 14, adjust=False, min_periods=14).mean()
    frame["bear"] = frame["ema15"] < frame["sma50"]
    frame["death_cross"] = frame["bear"] & ~frame["bear"].shift(1, fill_value=False)
    frame["golden_cross"] = ~frame["bear"] & frame["bear"].shift(1, fill_value=False)
    return frame


def is_confirmed_pivot_high(frame: pd.DataFrame, index: int) -> tuple[int, float] | None:
    pivot_index = index - PIVOT_RIGHT
    if pivot_index < PIVOT_LEFT:
        return None
    left = frame["high"].iloc[pivot_index - PIVOT_LEFT : pivot_index]
    right = frame["high"].iloc[pivot_index + 1 : index + 1]
    pivot_high = float(frame.at[pivot_index, "high"])
    if len(left) != PIVOT_LEFT or len(right) != PIVOT_RIGHT:
        return None
    if pivot_high > float(left.max()) and pivot_high > float(right.max()):
        return pivot_index, pivot_high
    return None


def simulate(asset: str) -> tuple[dict[str, object], list[dict[str, object]]]:
    frame = add_indicators(load_data(asset))
    first_valid = frame["sma50"].first_valid_index()
    if first_valid is None:
        return {"asset": asset, "bars": 0, "trades": 0}, []
    analysis_start = int(first_valid) + 500

    latest_resistance: tuple[int, float] | None = None
    setup_active = False
    ema15_retested = False
    setup_cross_ts = 0
    setup_retest_ts = 0
    setup_count = 0
    retest_count = 0
    signal_count = 0
    pending: dict[str, object] | None = None
    position: dict[str, object] | None = None
    trades: list[dict[str, object]] = []

    for index in range(analysis_start, len(frame)):
        row = frame.iloc[index]
        ts = int(row.ts)

        # A confirmed pivot becomes available only after the three right-side
        # candles have closed. It can be used starting with the next candle.
        if pending is not None and int(pending["entry_index"]) == index:
            entry = float(row.open)
            stop = float(pending["stop"])
            if entry < stop:
                risk = stop - entry
                position = {
                    **pending,
                    "entry": entry,
                    "risk": risk,
                    "entry_index": index,
                    "entry_ts": ts,
                    "target": entry - 2.0 * risk,
                }
            pending = None

        if position is not None:
            stop_hit = float(row.high) >= float(position["stop"])
            target_hit = float(row.low) <= float(position["target"])
            if stop_hit:
                exit_price = float(position["stop"])
                exit_reason = "stop"
            elif target_hit:
                exit_price = float(position["target"])
                exit_reason = "target_2R"
            elif bool(row.golden_cross):
                exit_price = float(row.close)
                exit_reason = "golden_cross_close"
            else:
                exit_price = None
                exit_reason = ""
            if exit_price is not None:
                risk = float(position["risk"])
                gross_r = (float(position["entry"]) - exit_price) / risk
                fee_r = float(position["entry"]) * FEE_ROUND_TRIP / risk
                trades.append(
                    {
                        "asset": asset,
                        "setup_death_cross_time": pd.to_datetime(int(position["setup_cross_ts"]), unit="ms", utc=True).isoformat(),
                        "ema15_retest_time": pd.to_datetime(int(position["retest_ts"]), unit="ms", utc=True).isoformat(),
                        "signal_time": pd.to_datetime(int(position["signal_ts"]), unit="ms", utc=True).isoformat(),
                        "entry_time": pd.to_datetime(int(position["entry_ts"]), unit="ms", utc=True).isoformat(),
                        "exit_time": pd.to_datetime(ts, unit="ms", utc=True).isoformat(),
                        "resistance": float(position["resistance"]),
                        "entry": float(position["entry"]),
                        "stop": float(position["stop"]),
                        "target_2R": float(position["target"]),
                        "exit": exit_price,
                        "exit_reason": exit_reason,
                        "gross_r": gross_r,
                        "fee_r_assumption": fee_r,
                        "net_r_estimate": gross_r - fee_r,
                        "hold_4h_bars": index - int(position["entry_index"]),
                    }
                )
                position = None
                setup_active = False
                ema15_retested = False
                setup_cross_ts = 0
                setup_retest_ts = 0

        if position is None and pending is None:
            if bool(row.death_cross):
                setup_active = True
                ema15_retested = False
                setup_cross_ts = ts
                setup_retest_ts = 0
                setup_count += 1
            elif bool(row.golden_cross):
                setup_active = False
                ema15_retested = False
                setup_cross_ts = 0
                setup_retest_ts = 0

            if setup_active and ts > setup_cross_ts and not ema15_retested and float(row.high) >= float(row.ema15):
                ema15_retested = True
                setup_retest_ts = ts
                retest_count += 1

            if setup_active and ema15_retested and latest_resistance is not None:
                resistance_index, resistance = latest_resistance
                # Avoid treating a level as known on its pivot-confirmation bar.
                if resistance_index < index - PIVOT_RIGHT:
                    if float(row.high) >= resistance and float(row.close) < resistance and float(row.close) < float(row.ema15):
                        signal_count += 1
                        stop = float(row.high)
                        next_index = index + 1
                        if next_index < len(frame):
                            pending = {
                                "signal_index": index,
                                "entry_index": next_index,
                                "signal_ts": ts,
                                "setup_cross_ts": setup_cross_ts,
                                "retest_ts": setup_retest_ts,
                                "resistance": resistance,
                                "stop": stop,
                            }
                        setup_active = False
                        ema15_retested = False
                        setup_cross_ts = 0
                        setup_retest_ts = 0

        # Confirm the pivot only after the current candle has completed. It
        # cannot be the resistance level for a signal on this same candle.
        pivot = is_confirmed_pivot_high(frame, index)
        if pivot is not None:
            latest_resistance = pivot

    if position is not None:
        row = frame.iloc[-1]
        exit_price = float(row.close)
        risk = float(position["risk"])
        gross_r = (float(position["entry"]) - exit_price) / risk
        fee_r = float(position["entry"]) * FEE_ROUND_TRIP / risk
        trades.append(
            {
                "asset": asset,
                "setup_death_cross_time": pd.to_datetime(int(position["setup_cross_ts"]), unit="ms", utc=True).isoformat(),
                "ema15_retest_time": pd.to_datetime(int(position["retest_ts"]), unit="ms", utc=True).isoformat(),
                "signal_time": pd.to_datetime(int(position["signal_ts"]), unit="ms", utc=True).isoformat(),
                "entry_time": pd.to_datetime(int(position["entry_ts"]), unit="ms", utc=True).isoformat(),
                "exit_time": pd.to_datetime(int(row.ts), unit="ms", utc=True).isoformat(),
                "resistance": float(position["resistance"]),
                "entry": float(position["entry"]),
                "stop": float(position["stop"]),
                "target_2R": float(position["target"]),
                "exit": exit_price,
                "exit_reason": "end_of_data_mark",
                "gross_r": gross_r,
                "fee_r_assumption": fee_r,
                "net_r_estimate": gross_r - fee_r,
                "hold_4h_bars": len(frame) - 1 - int(position["entry_index"]),
            }
        )

    trades_frame = pd.DataFrame(trades)
    if not trades_frame.empty:
        closed = trades_frame[trades_frame["exit_reason"] != "end_of_data_mark"]
        wins = int((closed["net_r_estimate"] > 0).sum())
        gross_losses = abs(float(closed.loc[closed["net_r_estimate"] < 0, "net_r_estimate"].sum()))
        gross_wins = float(closed.loc[closed["net_r_estimate"] > 0, "net_r_estimate"].sum())
        equity = closed["net_r_estimate"].cumsum()
        drawdown = equity - equity.cummax().clip(lower=0)
        summary: dict[str, object] = {
            "asset": asset,
            "from_utc": frame.loc[analysis_start, "time"].date().isoformat(),
            "to_utc": frame.iloc[-1].time.date().isoformat(),
            "bars_after_warmup": len(frame) - analysis_start,
            "death_cross_setups": setup_count,
            "ema15_retests": retest_count,
            "horizontal_rejection_signals": signal_count,
            "closed_trades": len(closed),
            "end_of_data_positions": int((trades_frame["exit_reason"] == "end_of_data_mark").sum()),
            "win_rate_net_pct": 100.0 * wins / len(closed) if len(closed) else np.nan,
            "total_net_r": float(closed["net_r_estimate"].sum()) if len(closed) else 0.0,
            "avg_net_r": float(closed["net_r_estimate"].mean()) if len(closed) else np.nan,
            "profit_factor_net": gross_wins / gross_losses if gross_losses > 0 else np.nan,
            "max_drawdown_r": float(drawdown.min()) if len(drawdown) else 0.0,
            "median_hold_4h_bars": float(closed["hold_4h_bars"].median()) if len(closed) else np.nan,
        }
    else:
        summary = {
            "asset": asset,
            "from_utc": frame.loc[analysis_start, "time"].date().isoformat(),
            "to_utc": frame.iloc[-1].time.date().isoformat(),
            "bars_after_warmup": len(frame) - analysis_start,
            "death_cross_setups": setup_count,
            "ema15_retests": retest_count,
            "horizontal_rejection_signals": signal_count,
            "closed_trades": 0,
            "end_of_data_positions": 0,
            "win_rate_net_pct": np.nan,
            "total_net_r": 0.0,
            "avg_net_r": np.nan,
            "profit_factor_net": np.nan,
            "max_drawdown_r": 0.0,
            "median_hold_4h_bars": np.nan,
        }
    return summary, trades


def write_report(summaries: list[dict[str, object]], trades: list[dict[str, object]]) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    summary_frame = pd.DataFrame(summaries)
    trade_frame = pd.DataFrame(trades)
    summary_frame.to_csv(OUTPUT_DIR / "summary.csv", index=False, encoding="utf-8-sig")
    trade_frame.to_csv(OUTPUT_DIR / "trades.csv", index=False, encoding="utf-8-sig")

    total_trades = int(summary_frame["closed_trades"].sum())
    total_r = float(summary_frame["total_net_r"].sum())
    columns = list(summary_frame.columns)
    markdown_table = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join("---" for _ in columns) + " |",
    ]
    for record in summary_frame.to_dict(orient="records"):
        cells = []
        for column in columns:
            value = record[column]
            if pd.isna(value):
                cells.append("")
            elif isinstance(value, (float, np.floating)):
                cells.append(f"{value:.2f}")
            else:
                cells.append(str(value))
        markdown_table.append("| " + " | ".join(cells) + " |")
    report_lines = [
        "# 4H EMA15/SMA50 死叉 + EMA15 回测 + 水平压力反转（探索性基线）",
        "",
        "## 机械化规则",
        "",
        "- 品种：BTC、ETH、SOL、DOGE、BNB、OKB USDT 永续；4H 已收盘 K 线。",
        "- 死叉后进入空头观察状态，直到 EMA15 上穿 SMA50；死叉 K 线之后价格最高价首次触及 EMA15，记为回测 EMA15。",
        "- 水平压力线取最近一个 4H 摆动高点：左侧 3 根、右侧 3 根 K 线确认；确认完成后的下一根 K 线起才可使用。",
        "- EMA15 回测之后，若当前 K 线最高价触及/刺破该压力线，且收盘低于压力线和 EMA15，记为反向信号。",
        "- 下一根 4H 开盘做空；止损放信号 K 线高点；止盈 2R。若同一根 K 线同时触及止损和止盈，按止损优先。",
        "- 回测结束时仍未退出的仓位按最后收盘价标记，不计入已平仓统计。每个死叉周期最多一次入场。",
        "",
        "## 成本与限制",
        "",
        f"- 净 R 估算扣除项目 area-shape 默认往返费率 {FEE_ROUND_TRIP:.3%}；未计滑点、资金费、盘口深度和交易所最小张数。",
        "- 这用自动确认的摆动高点代替历史手动画线，也把人工‘反向信号’收窄为单根 K 线的压力拒绝形态。结果只用于判断这套想法是否值得继续细化。",
        "- OHLC 不含 K 线内部先后顺序；同根止损止盈均触及时按保守止损处理。",
        "",
        "## 汇总",
        "",
        f"- 已平仓交易：{total_trades} 笔；合计净结果：{total_r:.2f}R。",
        "- 详细逐币种统计见 `summary.csv`，逐笔记录见 `trades.csv`。",
        "",
        *markdown_table,
        "",
    ]
    (OUTPUT_DIR / "report.md").write_text("\n".join(report_lines), encoding="utf-8")


def main() -> None:
    summaries: list[dict[str, object]] = []
    trades: list[dict[str, object]] = []
    for asset in ASSETS:
        summary, asset_trades = simulate(asset)
        summaries.append(summary)
        trades.extend(asset_trades)
    write_report(summaries, trades)
    print(pd.DataFrame(summaries).to_string(index=False))
    print(f"\nWrote {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
