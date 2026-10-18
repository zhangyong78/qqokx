from __future__ import annotations

import sqlite3
from collections import Counter
from decimal import Decimal
from pathlib import Path

import numpy as np
import pandas as pd

from okx_quant.analysis import PivotDetectionConfig
from okx_quant.indicators import atr
from okx_quant.models import Candle
from okx_quant.signal_replay_engine import SignalReplayConfig, build_signal_replay_dataset

DB_PATH = Path(r"D:\qqokx_data\cache\candle_store.db")
OUTPUT_DIR = Path(r"D:\qqokx\reports\ma15_sma50_4h_pattern_pivot_backtest_20261017")
ASSETS = ("BTC", "ETH", "SOL", "DOGE", "BNB", "OKB")
LOOKBACKS = (24, 48, 72)
MA_PAIRS = {
    "EMA15_SMA50": ("ema15", "sma50", 15, "sma", 50),
    "EMA21_EMA55": ("ema21", "ema55", 21, "ema", 55),
}
FEE_ROUND_TRIP = 0.00036
PIVOT_CONFIG = PivotDetectionConfig(left_bars=3, right_bars=3, atr_period=14, atr_multiplier=Decimal("0.5"), min_index_distance=2)
LEVEL_TOLERANCE_ATR = 0.25


def load_data(asset: str) -> pd.DataFrame:
    with sqlite3.connect(DB_PATH) as connection:
        rows = connection.execute(
            """SELECT ts, open, high, low, close, volume, confirmed
               FROM candles WHERE inst_id = ? AND bar = '4H' AND confirmed = 1 ORDER BY ts""",
            (f"{asset}-USDT-SWAP",),
        ).fetchall()
    frame = pd.DataFrame(rows, columns=("ts", "open", "high", "low", "close", "volume", "confirmed"))
    for column in ("open", "high", "low", "close", "volume"):
        frame[column] = frame[column].astype(float)
    frame["time"] = pd.to_datetime(frame["ts"], unit="ms", utc=True)
    return frame


def make_candles(frame: pd.DataFrame) -> list[Candle]:
    return [
        Candle(
            ts=int(row.ts), open=Decimal(str(row.open)), high=Decimal(str(row.high)),
            low=Decimal(str(row.low)), close=Decimal(str(row.close)),
            volume=Decimal(str(row.volume)), confirmed=True,
        )
        for row in frame.itertuples(index=False)
    ]


def add_indicators(frame: pd.DataFrame) -> pd.DataFrame:
    close = frame.close
    frame["ema15"] = close.ewm(span=15, adjust=False).mean()
    frame["sma50"] = close.rolling(50, min_periods=50).mean()
    frame["ema21"] = close.ewm(span=21, adjust=False).mean()
    frame["ema55"] = close.ewm(span=55, adjust=False).mean()
    previous_close = close.shift(1)
    true_range = pd.concat(
        (frame.high - frame.low, (frame.high - previous_close).abs(), (frame.low - previous_close).abs()), axis=1
    ).max(axis=1)
    frame["atr14"] = true_range.ewm(alpha=1 / 14, adjust=False, min_periods=14).mean()
    return frame


def pattern_events(
    candles: list[Candle], frame: pd.DataFrame, fast_col: str, slow_col: str,
) -> dict[int, list[tuple[str, str]]]:
    # Reuse the project's enabled pattern library. Indicator-combo entries are disabled.
    dataset = build_signal_replay_dataset(
        candles,
        config=SignalReplayConfig(confirmed_only=True, include_long=False, include_short=False, enable_pattern_signals=True),
    )
    events: dict[int, list[tuple[str, str]]] = {}
    for signal in dataset.signals:
        if signal.direction in {"long", "short"}:
            events.setdefault(int(signal.index), []).append((signal.direction, signal.pattern_id))

    # The chart's paired-candle reversal marker is UI-specific, so reproduce its
    # causal body reclaim + one of the documented local-extreme/consolidation/MA structures.
    fast_ma = frame[fast_col].to_numpy()
    slow_ma = frame[slow_col].to_numpy()
    opens, highs, lows, closes = (frame[name].to_numpy() for name in ("open", "high", "low", "close"))
    for i in range(1, len(frame)):
        direction = None
        if closes[i - 1] < opens[i - 1] and closes[i] > opens[i]:
            direction = "long"
        elif closes[i - 1] > opens[i - 1] and closes[i] < opens[i]:
            direction = "short"
        if direction is None or closes[i] == opens[i]:
            continue
        body_mid = (opens[i - 1] + closes[i - 1]) / 2
        reclaims_body = closes[i] >= body_mid if direction == "long" else closes[i] <= body_mid
        start8 = i - 7
        local_extreme = False
        if start8 >= 0:
            pair = min(lows[i - 1], lows[i]) if direction == "long" else max(highs[i - 1], highs[i])
            local_extreme = pair <= lows[start8:i + 1].min() if direction == "long" else pair >= highs[start8:i + 1].max()
        start_box = i - 7
        consolidation = False
        if start_box >= 0:
            box_high = highs[start_box:i - 1].max()
            box_low = lows[start_box:i - 1].min()
            avg_range = np.mean(highs[start_box:i - 1] - lows[start_box:i - 1])
            if avg_range > 0 and box_high - box_low <= avg_range * 3:
                consolidation = (lows[i - 1] < box_low and closes[i] >= box_low) if direction == "long" else (highs[i - 1] > box_high and closes[i] <= box_high)
        ma_reclaim = False
        if not np.isnan(fast_ma[i - 1:i + 1]).any() and not np.isnan(slow_ma[i - 1:i + 1]).any():
            for series in (fast_ma, slow_ma):
                if direction == "long" and closes[i - 1] < series[i - 1] and closes[i] >= series[i]:
                    ma_reclaim = True
                if direction == "short" and closes[i - 1] > series[i - 1] and closes[i] <= series[i]:
                    ma_reclaim = True
        if reclaims_body and (local_extreme or consolidation or ma_reclaim):
            events.setdefault(i, []).append((direction, "double_reversal_up" if direction == "long" else "double_reversal_down"))
    for i in events:
        events[i] = sorted(set(events[i]))
    return events


def pivot_candidates(candles: list[Candle]) -> dict[int, list[dict[str, object]]]:
    """Build raw confirmed candidates; significance filtering is applied online."""
    atr_values = atr(candles, PIVOT_CONFIG.atr_period)
    by_available: dict[int, list[dict[str, object]]] = {}
    for index in range(PIVOT_CONFIG.left_bars, len(candles) - PIVOT_CONFIG.right_bars):
        candle = candles[index]
        left = candles[index - PIVOT_CONFIG.left_bars:index]
        right = candles[index + 1:index + PIVOT_CONFIG.right_bars + 1]
        neighbors = left + right
        for kind, price, neighbor_prices in (
            ("high", candle.high, [item.high for item in neighbors]),
            ("low", candle.low, [item.low for item in neighbors]),
        ):
            is_pivot = price > max(neighbor_prices) if kind == "high" else price < min(neighbor_prices)
            if not is_pivot:
                continue
            distance = min(abs(price - item) for item in neighbor_prices)
            atr_value = atr_values[index]
            strength = distance / atr_value if atr_value is not None and atr_value > 0 else distance
            by_available.setdefault(index + PIVOT_CONFIG.right_bars, []).append({
                "index": index, "available_index": index + PIVOT_CONFIG.right_bars,
                "price": float(price), "kind": kind, "strength": float(strength),
                "atr": float(atr_value) if atr_value is not None else None,
            })
    return by_available


def simulate(asset: str, lookback: int, pair_name: str) -> tuple[dict[str, object], list[dict[str, object]]]:
    fast_col, slow_col, fast_period, slow_type, slow_period = MA_PAIRS[pair_name]
    frame = add_indicators(load_data(asset))
    frame["ma_direction"] = np.where(frame[fast_col] > frame[slow_col], 1, -1)
    frame["cross"] = frame.ma_direction.diff().fillna(0).astype(int)
    candles = make_candles(frame)
    if not candles or frame.sma50.first_valid_index() is None:
        return {"asset": asset, "pivot_lookback_4h": lookback, "closed_trades": 0}, []
    warmup = int(frame.sma50.first_valid_index()) + 100
    events = pattern_events(candles, frame, fast_col, slow_col)
    pivot_candidates_by_available = pivot_candidates(candles)
    active_pivots: list[dict[str, object]] = []
    last_pivot_by_kind: dict[str, dict[str, object]] = {}

    def accept_confirmed_pivots(available_index: int) -> None:
        for candidate in pivot_candidates_by_available.get(available_index, ()):
            kind = str(candidate["kind"])
            previous = last_pivot_by_kind.get(kind)
            if previous is not None:
                distance_bars = int(candidate["index"]) - int(previous["index"])
                if distance_bars < PIVOT_CONFIG.min_index_distance:
                    if float(candidate["strength"]) <= float(previous["strength"]):
                        continue
                    active_pivots.remove(previous)
                elif candidate["atr"] is not None:
                    min_move = float(candidate["atr"]) * float(PIVOT_CONFIG.atr_multiplier)
                    if abs(float(candidate["price"]) - float(previous["price"])) < min_move:
                        continue
            active_pivots.append(candidate)
            last_pivot_by_kind[kind] = candidate

    # Seed only pivots that would already have been confirmed before warmup ends.
    for available_index in range(warmup - 1):
        accept_confirmed_pivots(available_index)

    direction = 0
    cross_index = -1
    retest_index = -1
    traded_this_cycle = False
    pending: dict[str, object] | None = None
    position: dict[str, object] | None = None
    trades: list[dict[str, object]] = []
    counts: Counter[str] = Counter()

    for i in range(warmup, len(frame)):
        row = frame.iloc[i]
        # Candidate pivots become available at the close of their third right
        # bar, so they may first be used by the following candle.
        accept_confirmed_pivots(i - 1)
        # Enter at next 4H open after a fully closed signal bar.
        if pending is not None and int(pending["entry_index"]) == i:
            entry = float(row.open)
            stop = float(pending["stop"])
            side = str(pending["direction"])
            risk = (entry - stop) if side == "long" else (stop - entry)
            if risk > 0:
                target = entry + risk if side == "long" else entry - risk
                position = {**pending, "entry": entry, "risk": risk, "target": target, "entry_index": i, "entry_ts": int(row.ts)}
            else:
                counts["invalid_gap_entries"] += 1
            pending = None

        if position is not None:
            stop_hit = float(row.low) <= float(position["stop"]) if position["direction"] == "long" else float(row.high) >= float(position["stop"])
            target_hit = float(row.high) >= float(position["target"]) if position["direction"] == "long" else float(row.low) <= float(position["target"])
            if stop_hit or target_hit:
                # Conservatively resolve same-bar stop/target ambiguity as a stop.
                exit_price = float(position["stop"]) if stop_hit else float(position["target"])
                gross_r = -1.0 if stop_hit else 1.0
                fee_r = float(position["entry"]) * FEE_ROUND_TRIP / float(position["risk"])
                trades.append({
                "asset": asset, "ma_pair": pair_name, "pivot_lookback_4h": lookback, "direction": position["direction"],
                    "pattern": position["pattern"], "swing_level": position["level"],
                    "cross_time": pd.to_datetime(position["cross_ts"], unit="ms", utc=True).isoformat(),
                    "ema15_retest_time": pd.to_datetime(position["retest_ts"], unit="ms", utc=True).isoformat(),
                    "signal_time": pd.to_datetime(position["signal_ts"], unit="ms", utc=True).isoformat(),
                    "entry_time": pd.to_datetime(position["entry_ts"], unit="ms", utc=True).isoformat(),
                    "exit_time": pd.to_datetime(int(row.ts), unit="ms", utc=True).isoformat(),
                    "entry": position["entry"], "stop": position["stop"], "target_1r": position["target"],
                    "exit": exit_price, "exit_reason": "stop" if stop_hit else "target_1R",
                    "gross_r": gross_r, "fee_r_assumption": fee_r, "net_r_estimate": gross_r - fee_r,
                    "hold_4h_bars": i - int(position["entry_index"]),
                })
                position = None

        cross = int(row.cross)
        if cross:
            direction = 1 if cross > 0 else -1
            cross_index = i
            retest_index = -1
            traded_this_cycle = False
            counts["golden_cross_cycles" if direction > 0 else "death_cross_cycles"] += 1

        if position is not None or pending is not None or direction == 0 or traded_this_cycle:
            continue

        # A retest must happen after the cross. Require the reversal pattern later,
        # keeping the sequence cross -> EMA15 retest -> swing-level rejection explicit.
        if retest_index < 0 and i > cross_index:
            touched_ema = float(row.low) <= float(row[fast_col]) if direction > 0 else float(row.high) >= float(row[fast_col])
            if touched_ema:
                retest_index = i
                counts["ema15_retests"] += 1
            continue
        if retest_index < 0 or i <= retest_index or not events.get(i):
            continue

        signal_direction = "long" if direction > 0 else "short"
        matching = [(side, name) for side, name in events[i] if side == signal_direction]
        if not matching:
            continue
        # Levels are only those confirmed by the current bar; use levels whose
        # originating pivot falls within the selected 4H lookback window.
        active_levels = [
            p for p in active_pivots
            if p["kind"] == ("low" if direction > 0 else "high")
            and int(p["available_index"]) < i
            and i - int(p["index"]) <= lookback
        ]
        if not active_levels:
            continue
        tolerance = float(row.atr14) * LEVEL_TOLERANCE_ATR if pd.notna(row.atr14) else 0.0
        valid_levels = []
        for level in active_levels:
            price = float(level["price"])
            if direction > 0:
                touched = float(row.low) <= price + tolerance and float(row.close) > price
            else:
                touched = float(row.high) >= price - tolerance and float(row.close) < price
            if touched:
                valid_levels.append(level)
        if not valid_levels:
            continue
        # If multiple levels overlap, select the closest level to signal close.
        level = min(valid_levels, key=lambda item: abs(float(row.close) - float(item["price"])))
        side, pattern = sorted(matching)[0]
        stop = float(row.low) if side == "long" else float(row.high)
        pending = {
            "entry_index": i + 1, "direction": side, "pattern": pattern, "level": float(level["price"]),
            "stop": stop, "signal_ts": int(row.ts), "cross_ts": int(frame.iloc[cross_index].ts),
            "retest_ts": int(frame.iloc[retest_index].ts),
        }
        traded_this_cycle = True
        counts["matched_level_pattern_signals"] += 1
        counts[f"pattern_{pattern}"] += 1

    # Open position at sample end is marked to market and excluded from closed-trade stats.
    if position is not None:
        row = frame.iloc[-1]
        side = str(position["direction"])
        exit_price = float(row.close)
        gross_r = (exit_price - float(position["entry"])) / float(position["risk"]) if side == "long" else (float(position["entry"]) - exit_price) / float(position["risk"])
        fee_r = float(position["entry"]) * FEE_ROUND_TRIP / float(position["risk"])
        trades.append({
            "asset": asset, "ma_pair": pair_name, "pivot_lookback_4h": lookback, "direction": side, "pattern": position["pattern"],
            "swing_level": position["level"], "cross_time": pd.to_datetime(position["cross_ts"], unit="ms", utc=True).isoformat(),
            "ema15_retest_time": pd.to_datetime(position["retest_ts"], unit="ms", utc=True).isoformat(),
            "signal_time": pd.to_datetime(position["signal_ts"], unit="ms", utc=True).isoformat(),
            "entry_time": pd.to_datetime(position["entry_ts"], unit="ms", utc=True).isoformat(),
            "exit_time": pd.to_datetime(int(row.ts), unit="ms", utc=True).isoformat(),
            "entry": position["entry"], "stop": position["stop"], "target_1r": position["target"],
            "exit": exit_price, "exit_reason": "end_of_data_mark", "gross_r": gross_r,
            "fee_r_assumption": fee_r, "net_r_estimate": gross_r - fee_r,
            "hold_4h_bars": len(frame) - 1 - int(position["entry_index"]),
        })

    trade_frame = pd.DataFrame(trades)
    closed = trade_frame[trade_frame.exit_reason != "end_of_data_mark"] if not trade_frame.empty else trade_frame
    wins = int((closed.net_r_estimate > 0).sum()) if not closed.empty else 0
    gains = float(closed.loc[closed.net_r_estimate > 0, "net_r_estimate"].sum()) if not closed.empty else 0.0
    losses = abs(float(closed.loc[closed.net_r_estimate < 0, "net_r_estimate"].sum())) if not closed.empty else 0.0
    equity = closed.net_r_estimate.cumsum() if not closed.empty else pd.Series(dtype=float)
    drawdown = equity - equity.cummax().clip(lower=0) if not equity.empty else equity
    summary = {
        "asset": asset, "ma_pair": pair_name, "pivot_lookback_4h": lookback,
        "from_utc": frame.iloc[warmup].time.date().isoformat(), "to_utc": frame.iloc[-1].time.date().isoformat(),
        "bars_after_warmup": len(frame) - warmup,
        **dict(counts),
        "closed_trades": len(closed), "end_of_data_positions": int((trade_frame.exit_reason == "end_of_data_mark").sum()) if not trade_frame.empty else 0,
        "win_rate_net_pct": 100 * wins / len(closed) if len(closed) else np.nan,
        "total_net_r": float(closed.net_r_estimate.sum()) if not closed.empty else 0.0,
        "avg_net_r": float(closed.net_r_estimate.mean()) if not closed.empty else np.nan,
        "profit_factor_net": gains / losses if losses else np.nan,
        "max_drawdown_r": float(drawdown.min()) if not drawdown.empty else 0.0,
        "median_hold_4h_bars": float(closed.hold_4h_bars.median()) if not closed.empty else np.nan,
    }
    return summary, trades


def main() -> None:
    summaries: list[dict[str, object]] = []
    trades: list[dict[str, object]] = []
    for pair_name in MA_PAIRS:
        for asset in ASSETS:
            for lookback in LOOKBACKS:
                print(f"Running {pair_name} {asset} depth={lookback} 4H bars...", flush=True)
                summary, asset_trades = simulate(asset, lookback, pair_name)
                summaries.append(summary)
                trades.extend(asset_trades)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    summary_frame = pd.DataFrame(summaries).fillna({"golden_cross_cycles": 0, "death_cross_cycles": 0, "ema15_retests": 0, "matched_level_pattern_signals": 0, "invalid_gap_entries": 0})
    trade_frame = pd.DataFrame(trades)
    summary_frame.to_csv(OUTPUT_DIR / "summary.csv", index=False, encoding="utf-8-sig")
    trade_frame.to_csv(OUTPUT_DIR / "trades.csv", index=False, encoding="utf-8-sig")
    pattern_counts = trade_frame[trade_frame.exit_reason != "end_of_data_mark"].groupby(["ma_pair", "pivot_lookback_4h", "pattern", "direction"]).size().rename("trades").reset_index() if not trade_frame.empty else pd.DataFrame()
    pattern_counts.to_csv(OUTPUT_DIR / "pattern_counts.csv", index=False, encoding="utf-8-sig")
    columns = list(summary_frame.columns)
    table_rows = [
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
        table_rows.append("| " + " | ".join(cells) + " |")
    table = "\n".join(table_rows)
    report = f'''# 4H EMA15/SMA50 与 EMA21/EMA55 参数对照回测

## 规则

- 数据：OKX BTC、ETH、SOL、DOGE、BNB、OKB USDT 永续，4H 已收盘K线；本地缓存可用历史截至 {summary_frame.to_utc.max()} UTC。
- 同一套规则对照 EMA15/SMA50 与 EMA21/EMA55：金叉后只找多头，死叉后只找空头；交叉后必须先回踩本组快线，再在后续K线上触发反向形态。
- 水平位使用项目摆动点检测器：左3/右3根确认、ATR14×0.5显著性过滤、同类摆动点最小间隔2根。摆动点出现后三根右侧K线收完才可使用。
- 水平位深度分别测试24/48/72根4H K线（约4/8/12天），所有窗口内的已确认同向摆动点都纳入；多单看摆动低点，空单看摆动高点。信号K线需进入水平位±0.25 ATR14容差区并收回线的有效侧。
- 反向形态取项目 `signal_replay_engine` 默认启用的形态信号，另外包含图表UI的“双线向上/向下反转”结构规则；只接受与当前均线方向一致的多/空信号。一次均线交叉周期最多开一笔。
- 两组均线除参数外使用同一摆动点、形态、手续费和出场规则。信号K线收盘后，下一根4H开盘入场；止损为多单信号K线低点、空单信号K线高点；固定止盈1R。若同根K线止损与止盈均触及，按止损先发生处理。开盘跳空导致风险方向无效时跳过。

## 成本和限制

- 扣除假设往返手续费 {FEE_ROUND_TRIP:.3%}；未计滑点、资金费率和盘口冲击。
- 结果以R汇总，不是组合资金曲线；每笔按各自止损距离归一化，没有模拟账户仓位和相关性。
- 形态库默认启用了“大实体”过滤。水平线宽度用0.25 ATR作为机械化替代，真实手动画线具有主观性。摆动点过滤按项目实现计算，但可用时间严格延后到右侧3根K线确认后。
- 只统计已触发止损/止盈的交易；样本末尾未平仓单另列，不纳入胜率、净R、PF和回撤。OHLC同K线内先后无法确定，采取保守止损优先。
- 此结果验证的是“交叉→回踩→摆动位反转形态”的可机械化版本，不能单独证明人工判断更容易或更有优势。

## 各币种、参数组与深度统计

{table}

详细逐笔交易见 `trades.csv`，形态拆分见 `pattern_counts.csv`。
'''
    (OUTPUT_DIR / "report.md").write_text(report, encoding="utf-8")
    print(f"\nWrote {OUTPUT_DIR}")
    print(summary_frame[["ma_pair", "asset", "pivot_lookback_4h", "closed_trades", "win_rate_net_pct", "total_net_r", "profit_factor_net", "max_drawdown_r"]].to_string(index=False))


if __name__ == "__main__":
    main()
