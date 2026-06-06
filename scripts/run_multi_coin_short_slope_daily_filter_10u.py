from __future__ import annotations

import base64
import html
import io
import json
import math
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from okx_quant.candle_cache import load_candle_cache
from okx_quant.models import Candle
from okx_quant.timeframe import closed_candle_available_timestamps


plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "Noto Sans CJK SC", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False


REPORT_DIR = ROOT / "reports"
ENTRY_BAR = "1H"
FILTER_BAR = "1D"
SYMBOLS = ("BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP", "BNB-USDT-SWAP", "DOGE-USDT-SWAP")
COIN_LABELS = {symbol: symbol.split("-")[0] for symbol in SYMBOLS}

RISK_PER_TRADE_U = 10.0
TAKER_FEE_RATE = 0.00036
ATR_PERIOD = 14
ATR_STOP_MULTIPLIER = 2.0
ATR_PERCENTILE_LOOKBACK = 100
ATR_PERCENTILE_MAX = 0.50
SLOPE_THRESHOLD_RATIO = -0.0005
INITIAL_CAPITAL = 10_000.0

HTML_PATH = REPORT_DIR / "multi_coin_short_slope_daily_filter_10u_report.html"
SUMMARY_CSV_PATH = REPORT_DIR / "multi_coin_short_slope_daily_filter_10u_summary.csv"
COIN_CSV_PATH = REPORT_DIR / "multi_coin_short_slope_daily_filter_10u_by_coin.csv"
TRADES_CSV_PATH = REPORT_DIR / "multi_coin_short_slope_daily_filter_10u_trades.csv"
JSON_PATH = REPORT_DIR / "multi_coin_short_slope_daily_filter_10u_summary.json"


@dataclass(frozen=True)
class StrategyVariant:
    key: str
    label: str
    ma_type: str
    period: int
    note: str


@dataclass(frozen=True)
class DailyFilterVariant:
    key: str
    label: str
    ma_type: str | None = None
    period: int | None = None
    note: str = ""


STRATEGY_VARIANTS = (
    StrategyVariant("ema55", "EMA55 斜率空", "ema", 55, "基准慢线，偏稳定"),
    StrategyVariant("ma55", "MA55 斜率空", "ma", 55, "更平滑，但反应更慢"),
    StrategyVariant("ema21", "EMA21 斜率空", "ema", 21, "更快，交易更密"),
    StrategyVariant("ma20", "MA20 斜率空", "ma", 20, "快线版本，容易放大噪音"),
    StrategyVariant("ema34", "EMA34 斜率空", "ema", 34, "建议补充，速度与稳定性居中"),
)

DAILY_FILTERS = (
    DailyFilterVariant("none", "无日线过滤", note="只看 1H 入场"),
    DailyFilterVariant("ema21", "日线 EMA21 过滤", "ema", 21, "日线收盘低于 EMA21 才允许做空"),
    DailyFilterVariant("ema55", "日线 EMA55 过滤", "ema", 55, "更慢的日线趋势门"),
    DailyFilterVariant("ma20", "日线 MA20 过滤", "ma", 20, "月内均价门槛，偏实用"),
)


def main() -> None:
    REPORT_DIR.mkdir(exist_ok=True)
    all_trades: list[dict[str, object]] = []
    summary_rows: list[dict[str, object]] = []
    coin_rows: list[dict[str, object]] = []
    data_ranges: dict[str, dict[str, object]] = {}

    for symbol in SYMBOLS:
        entry_candles = [candle for candle in load_candle_cache(symbol, ENTRY_BAR, limit=None) if candle.confirmed]
        daily_candles = [candle for candle in load_candle_cache(symbol, FILTER_BAR, limit=None) if candle.confirmed]
        if not entry_candles or not daily_candles:
            data_ranges[symbol] = {"error": "missing candles"}
            continue

        entry_frame = build_entry_frame(entry_candles)
        add_entry_indicators(entry_frame)
        daily_bias_map = build_daily_bias_map(entry_candles, daily_candles)
        split_bounds = build_split_bounds(len(entry_frame))
        data_ranges[symbol] = {
            "entry_candles": len(entry_frame),
            "daily_candles": len(daily_candles),
            "start_utc": format_ts(int(entry_frame["ts"].iloc[0])),
            "end_utc": format_ts(int(entry_frame["ts"].iloc[-1])),
        }

        for strategy in STRATEGY_VARIANTS:
            ma_column = ma_column_name(strategy.ma_type, strategy.period)
            for gate in DAILY_FILTERS:
                bias = daily_bias_map[gate.key]
                trades = simulate_short_trades(entry_frame, bias=bias, ma_column=ma_column)
                trades["symbol"] = symbol
                trades["coin"] = COIN_LABELS[symbol]
                trades["strategy_key"] = strategy.key
                trades["strategy_label"] = strategy.label
                trades["daily_filter_key"] = gate.key
                trades["daily_filter_label"] = gate.label
                all_trades.extend(trades.to_dict("records"))
                coin_metrics = flatten_metrics(
                    symbol=symbol,
                    strategy=strategy,
                    gate=gate,
                    trades=trades,
                    bounds=split_bounds,
                )
                coin_rows.append(coin_metrics)

    coin_frame = pd.DataFrame(coin_rows)
    if coin_frame.empty:
        raise RuntimeError("no coin metrics generated")

    trades_frame = pd.DataFrame(all_trades)
    if trades_frame.empty:
        raise RuntimeError("no trades generated")

    summary_frame = build_summary_frame(trades_frame)
    summary_frame["score"] = summary_frame.apply(score_summary_row, axis=1)
    summary_frame = summary_frame.sort_values(["score", "test_pnl_u"], ascending=[False, False]).reset_index(drop=True)
    coin_frame = coin_frame.sort_values(["strategy_key", "daily_filter_key", "coin"]).reset_index(drop=True)
    trades_frame = trades_frame.sort_values(["exit_ts", "entry_ts", "coin"]).reset_index(drop=True)

    summary_frame.to_csv(SUMMARY_CSV_PATH, index=False, encoding="utf-8-sig")
    coin_frame.to_csv(COIN_CSV_PATH, index=False, encoding="utf-8-sig")
    trades_frame.to_csv(TRADES_CSV_PATH, index=False, encoding="utf-8-sig")

    best_row = summary_frame.iloc[0]
    baseline_row = summary_frame[
        (summary_frame["strategy_key"] == "ema55") & (summary_frame["daily_filter_key"] == "none")
    ].iloc[0]
    payload = {
        "created_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "assumptions": {
            "entry_bar": ENTRY_BAR,
            "daily_filter_bar": FILTER_BAR,
            "risk_per_trade_u": RISK_PER_TRADE_U,
            "taker_fee_rate": TAKER_FEE_RATE,
            "atr_period": ATR_PERIOD,
            "atr_stop_multiplier": ATR_STOP_MULTIPLIER,
            "atr_percentile_max": ATR_PERCENTILE_MAX,
            "slope_threshold_ratio": SLOPE_THRESHOLD_RATIO,
            "exit_model": "2R 保本后逐级锁盈，不强制斜率翻正平仓",
            "capital_note": "统计按每笔固定风险 10U，未限制多币同时占用保证金",
        },
        "data_ranges": data_ranges,
        "best_combo": best_row.to_dict(),
        "baseline_combo": baseline_row.to_dict(),
    }
    JSON_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    total_chart = render_total_pnl_chart(summary_frame)
    test_chart = render_test_pnl_chart(summary_frame)
    dd_chart = render_drawdown_chart(summary_frame)
    best_equity_chart = render_best_equity_chart(trades_frame, best_row)

    HTML_PATH.write_text(
        build_html(
            summary_frame=summary_frame,
            coin_frame=coin_frame,
            payload=payload,
            total_chart=total_chart,
            test_chart=test_chart,
            dd_chart=dd_chart,
            best_equity_chart=best_equity_chart,
        ),
        encoding="utf-8",
    )
    print(HTML_PATH)


def build_entry_frame(candles: list[object]) -> pd.DataFrame:
    rows = [
        {
            "ts": int(candle.ts),
            "timestamp": pd.to_datetime(int(candle.ts), unit="ms", utc=True),
            "open": float(candle.open),
            "high": float(candle.high),
            "low": float(candle.low),
            "close": float(candle.close),
            "volume": float(candle.volume),
        }
        for candle in candles
    ]
    return pd.DataFrame(rows).sort_values("timestamp").drop_duplicates("timestamp").reset_index(drop=True)


def add_entry_indicators(df: pd.DataFrame) -> None:
    for variant in STRATEGY_VARIANTS:
        col = ma_column_name(variant.ma_type, variant.period)
        if variant.ma_type == "ema":
            df[col] = df["close"].ewm(span=variant.period, adjust=False, min_periods=variant.period).mean()
        else:
            df[col] = df["close"].rolling(variant.period, min_periods=variant.period).mean()
    prev_close = df["close"].shift(1)
    true_range = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - prev_close).abs(),
            (df["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    df["atr14"] = true_range.ewm(alpha=1 / ATR_PERIOD, adjust=False, min_periods=ATR_PERIOD).mean()
    df["atr_pct"] = rolling_percentile(df["atr14"], ATR_PERCENTILE_LOOKBACK)


def rolling_percentile(series: pd.Series, lookback: int) -> pd.Series:
    return series.rolling(lookback, min_periods=lookback).apply(lambda x: float(np.mean(x <= x[-1])), raw=True)


def build_daily_bias_map(entry_candles: list[object], daily_candles: list[object]) -> dict[str, list[str] | None]:
    typed_daily_candles = [
        candle for candle in daily_candles if isinstance(candle, Candle)
    ] or daily_candles
    daily_frame = pd.DataFrame(
        {
            "ts": [int(candle.ts) for candle in daily_candles],
            "close": [float(candle.close) for candle in daily_candles],
        }
    )
    daily_available_ts = closed_candle_available_timestamps(typed_daily_candles)
    out: dict[str, list[str] | None] = {"none": None}
    for gate in DAILY_FILTERS:
        if gate.period is None or gate.ma_type is None:
            continue
        if gate.ma_type == "ema":
            line = daily_frame["close"].ewm(span=gate.period, adjust=False, min_periods=gate.period).mean()
        else:
            line = daily_frame["close"].rolling(gate.period, min_periods=gate.period).mean()
        biases: list[str] = []
        for candle in entry_candles:
            idx = np.searchsorted(daily_available_ts, int(candle.ts), side="right") - 1
            if idx < 0:
                biases.append("neutral")
                continue
            line_value = float(line.iloc[idx]) if pd.notna(line.iloc[idx]) else math.nan
            daily_close = float(daily_frame["close"].iloc[idx])
            if not np.isfinite(line_value):
                biases.append("neutral")
            elif daily_close < line_value:
                biases.append("short")
            elif daily_close > line_value:
                biases.append("long")
            else:
                biases.append("neutral")
        out[gate.key] = biases
    return out


def simulate_short_trades(
    df: pd.DataFrame,
    *,
    bias: list[str] | None,
    ma_column: str,
) -> pd.DataFrame:
    if bias is not None and len(bias) != len(df):
        raise ValueError(f"bias length mismatch: bias={len(bias)} df={len(df)}")
    trades: list[dict[str, object]] = []
    position: dict[str, float | int | str] | None = None
    start_index = max(ATR_PERCENTILE_LOOKBACK, 60)
    open_values = df["open"].to_numpy(dtype=float)
    high_values = df["high"].to_numpy(dtype=float)
    low_values = df["low"].to_numpy(dtype=float)
    close_values = df["close"].to_numpy(dtype=float)
    ts_values = df["ts"].to_numpy(dtype=np.int64)
    line_values = df[ma_column].to_numpy(dtype=float)
    atr_values = df["atr14"].to_numpy(dtype=float)
    atr_pct_values = df["atr_pct"].to_numpy(dtype=float)

    for index in range(start_index, len(df)):
        line_value = line_values[index]
        prev_line = line_values[index - 1]
        atr_value = atr_values[index]
        atr_pct = atr_pct_values[index]
        close_price = close_values[index]
        if any(math.isnan(value) for value in [line_value, prev_line, atr_value, atr_pct]):
            continue

        # This strategy enters on the current confirmed 1H candle close.
        # The MA/EMA and slope therefore intentionally include the same
        # candle's close and are not shifted to the next bar.
        slope_ratio = (line_value - prev_line) / line_value if line_value else math.nan

        if position is not None:
            exited = process_open_short(
                position,
                candle_open=open_values[index],
                candle_high=high_values[index],
                candle_low=low_values[index],
                candle_close=close_values[index],
                candle_ts=int(ts_values[index]),
                index=index,
                trades=trades,
            )
            if exited:
                position = None

        if position is not None:
            continue
        if not np.isfinite(slope_ratio) or slope_ratio > SLOPE_THRESHOLD_RATIO:
            continue
        if close_price >= line_value:
            continue
        if atr_pct > ATR_PERCENTILE_MAX:
            continue
        if bias is not None and index < len(bias) and bias[index] != "short":
            continue

        risk_per_unit = atr_value * ATR_STOP_MULTIPLIER
        if risk_per_unit <= 0 or not np.isfinite(risk_per_unit):
            continue

        entry_price = close_price
        fee_offset = entry_price * TAKER_FEE_RATE * 2.0
        position = {
            "entry_index": index,
            "entry_ts": int(ts_values[index]),
            "entry_price": entry_price,
            "risk_per_unit": risk_per_unit,
            "stop": entry_price + risk_per_unit,
            "stop_reason": "stop_loss",
            "fee_offset": fee_offset,
            "next_dynamic_r": 2.0,
            "entry_line": line_value,
            "entry_slope_ratio": slope_ratio,
            "entry_atr_pct": atr_pct,
        }

    return pd.DataFrame(trades)


def process_open_short(
    position: dict[str, float | int | str],
    *,
    candle_open: float,
    candle_high: float,
    candle_low: float,
    candle_close: float,
    candle_ts: int,
    index: int,
    trades: list[dict[str, object]],
) -> bool:
    path = candle_path_points(
        candle_open=candle_open,
        candle_high=candle_high,
        candle_low=candle_low,
        candle_close=candle_close,
    )
    for start, end in zip(path, path[1:]):
        if end > start:
            stop_price = float(position["stop"])
            if start <= stop_price <= end:
                trades.append(close_trade(position, index, candle_ts, stop_price, str(position["stop_reason"])))
                return True
        else:
            advance_step_dynamic(position, end)
    return False


def candle_path_points(
    *,
    candle_open: float,
    candle_high: float,
    candle_low: float,
    candle_close: float,
) -> tuple[float, float, float, float]:
    if candle_close >= candle_open:
        return candle_open, candle_low, candle_high, candle_close
    return candle_open, candle_high, candle_low, candle_close


def advance_step_dynamic(position: dict[str, float | int | str], favorable_price: float) -> None:
    entry = float(position["entry_price"])
    risk = float(position["risk_per_unit"])
    fee_offset = float(position["fee_offset"])
    while True:
        next_r = float(position["next_dynamic_r"])
        trigger = entry - risk * next_r - fee_offset
        if favorable_price > trigger:
            break
        if math.isclose(next_r, 2.0):
            locked_r = 0.0
            reason = "break_even_stop"
        else:
            locked_r = max(next_r - 1.0, 0.0)
            reason = f"locked_{int(round(locked_r))}r_stop"
        candidate_stop = entry - risk * locked_r - fee_offset
        if candidate_stop < float(position["stop"]):
            position["stop"] = candidate_stop
            position["stop_reason"] = reason
        position["next_dynamic_r"] = next_r + 1.0


def close_trade(
    position: dict[str, float | int | str],
    exit_index: int,
    exit_ts: int,
    exit_price: float,
    exit_reason: str,
) -> dict[str, object]:
    entry = float(position["entry_price"])
    risk = float(position["risk_per_unit"])
    quantity = RISK_PER_TRADE_U / risk if risk > 0 else 0.0
    pnl_per_unit = (entry - exit_price) - TAKER_FEE_RATE * (entry + exit_price)
    pnl_u = pnl_per_unit * quantity
    return {
        "entry_index": int(position["entry_index"]),
        "exit_index": exit_index,
        "entry_ts": int(position["entry_ts"]),
        "exit_ts": exit_ts,
        "entry_price": entry,
        "exit_price": exit_price,
        "risk_per_unit": risk,
        "pnl_u": pnl_u,
        "r_multiple": pnl_u / RISK_PER_TRADE_U,
        "hold_hours": (exit_ts - int(position["entry_ts"])) / (1000 * 3600),
        "exit_reason": exit_reason,
        "entry_line": float(position["entry_line"]),
        "entry_slope_ratio": float(position["entry_slope_ratio"]),
        "entry_atr_pct": float(position["entry_atr_pct"]),
    }


def build_split_bounds(length: int) -> dict[str, tuple[int, int]]:
    train_end = int(length * 0.6)
    validation_end = int(length * 0.8)
    return {
        "train": (0, train_end - 1),
        "validation": (train_end, validation_end - 1),
        "test": (validation_end, length - 1),
        "all": (0, length - 1),
    }


def split_trades(trades: pd.DataFrame, bounds: tuple[int, int]) -> pd.DataFrame:
    if trades.empty:
        return trades.copy()
    start, end = bounds
    return trades[(trades["exit_index"] >= start) & (trades["exit_index"] <= end)].copy()


def compute_metrics(trades: pd.DataFrame) -> dict[str, float]:
    if trades.empty:
        return {
            "trades": 0.0,
            "total_pnl_u": 0.0,
            "profit_factor": 0.0,
            "win_rate": 0.0,
            "avg_r": 0.0,
            "avg_hold_hours": 0.0,
            "max_drawdown_u": 0.0,
            "return_pct_on_10k": 0.0,
            "big_win_2r_count": 0.0,
            "big_win_5r_count": 0.0,
        }
    pnls = trades["pnl_u"].astype(float)
    rs = trades["r_multiple"].astype(float)
    curve = pnls.cumsum()
    gross_profit = float(pnls[pnls > 0].sum())
    gross_loss = abs(float(pnls[pnls < 0].sum()))
    drawdown = float((curve.cummax() - curve).max())
    total = float(pnls.sum())
    return {
        "trades": float(len(trades)),
        "total_pnl_u": total,
        "profit_factor": gross_profit / gross_loss if gross_loss > 0 else 0.0,
        "win_rate": float((pnls > 0).mean()),
        "avg_r": float(rs.mean()),
        "avg_hold_hours": float(trades["hold_hours"].astype(float).mean()),
        "max_drawdown_u": drawdown,
        "return_pct_on_10k": total / INITIAL_CAPITAL * 100.0,
        "big_win_2r_count": float((rs >= 2).sum()),
        "big_win_5r_count": float((rs >= 5).sum()),
    }


def flatten_metrics(
    *,
    symbol: str,
    strategy: StrategyVariant,
    gate: DailyFilterVariant,
    trades: pd.DataFrame,
    bounds: dict[str, tuple[int, int]],
) -> dict[str, object]:
    all_metrics = compute_metrics(trades)
    test_metrics = compute_metrics(split_trades(trades, bounds["test"]))
    validation_metrics = compute_metrics(split_trades(trades, bounds["validation"]))
    return {
        "symbol": symbol,
        "coin": COIN_LABELS[symbol],
        "strategy_key": strategy.key,
        "strategy_label": strategy.label,
        "daily_filter_key": gate.key,
        "daily_filter_label": gate.label,
        "strategy_note": strategy.note,
        "daily_filter_note": gate.note,
        "all_trades": int(all_metrics["trades"]),
        "all_pnl_u": all_metrics["total_pnl_u"],
        "all_profit_factor": all_metrics["profit_factor"],
        "all_win_rate": all_metrics["win_rate"],
        "all_avg_r": all_metrics["avg_r"],
        "all_avg_hold_hours": all_metrics["avg_hold_hours"],
        "all_max_drawdown_u": all_metrics["max_drawdown_u"],
        "validation_pnl_u": validation_metrics["total_pnl_u"],
        "validation_profit_factor": validation_metrics["profit_factor"],
        "validation_win_rate": validation_metrics["win_rate"],
        "test_trades": int(test_metrics["trades"]),
        "test_pnl_u": test_metrics["total_pnl_u"],
        "test_profit_factor": test_metrics["profit_factor"],
        "test_win_rate": test_metrics["win_rate"],
        "test_avg_r": test_metrics["avg_r"],
        "test_avg_hold_hours": test_metrics["avg_hold_hours"],
        "test_max_drawdown_u": test_metrics["max_drawdown_u"],
    }


def build_summary_frame(trades_frame: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    group_cols = ["strategy_key", "strategy_label", "daily_filter_key", "daily_filter_label"]
    for keys, group in trades_frame.groupby(group_cols, sort=False):
        strategy_key, strategy_label, daily_filter_key, daily_filter_label = keys
        all_metrics = compute_metrics(group)
        test_group = split_global_test_trades(group)
        test_metrics = compute_metrics(test_group)
        by_coin = []
        for coin, coin_group in group.groupby("coin"):
            coin_all = compute_metrics(coin_group)
            coin_test = compute_metrics(split_global_test_trades(coin_group))
            by_coin.append(
                {
                    "coin": coin,
                    "all_pnl_u": coin_all["total_pnl_u"],
                    "test_pnl_u": coin_test["total_pnl_u"],
                    "all_drawdown_u": coin_all["max_drawdown_u"],
                }
            )
        pnl_values = [item["test_pnl_u"] for item in by_coin]
        rows.append(
            {
                "strategy_key": strategy_key,
                "strategy_label": strategy_label,
                "daily_filter_key": daily_filter_key,
                "daily_filter_label": daily_filter_label,
                "coins": len(by_coin),
                "all_trades": int(all_metrics["trades"]),
                "all_pnl_u": all_metrics["total_pnl_u"],
                "all_profit_factor": all_metrics["profit_factor"],
                "all_win_rate": all_metrics["win_rate"],
                "all_avg_r": all_metrics["avg_r"],
                "all_avg_hold_hours": all_metrics["avg_hold_hours"],
                "all_max_drawdown_u": all_metrics["max_drawdown_u"],
                "test_trades": int(test_metrics["trades"]),
                "test_pnl_u": test_metrics["total_pnl_u"],
                "test_profit_factor": test_metrics["profit_factor"],
                "test_win_rate": test_metrics["win_rate"],
                "test_avg_r": test_metrics["avg_r"],
                "test_avg_hold_hours": test_metrics["avg_hold_hours"],
                "test_max_drawdown_u": test_metrics["max_drawdown_u"],
                "test_positive_coins": sum(1 for item in pnl_values if item > 0),
                "test_negative_coins": sum(1 for item in pnl_values if item < 0),
                "test_pnl_std_u": float(np.std(pnl_values)) if pnl_values else 0.0,
                "test_pnl_median_u": float(np.median(pnl_values)) if pnl_values else 0.0,
            }
        )
    return pd.DataFrame(rows)


def split_global_test_trades(group: pd.DataFrame) -> pd.DataFrame:
    masks: list[pd.Series] = []
    for coin, coin_group in group.groupby("coin"):
        if coin_group.empty:
            continue
        max_exit = int(coin_group["exit_index"].max())
        test_start = int(max_exit * 0.8)
        masks.append((group["coin"] == coin) & (group["exit_index"] >= test_start))
    if not masks:
        return group.iloc[0:0].copy()
    combined = masks[0].copy()
    for mask in masks[1:]:
        combined = combined | mask
    return group[combined].copy()


def score_summary_row(row: pd.Series) -> float:
    return (
        float(row["test_pnl_u"])
        - float(row["test_max_drawdown_u"]) * 0.35
        + float(row["test_profit_factor"]) * 40.0
        + float(row["test_positive_coins"]) * 15.0
        - float(row["test_pnl_std_u"]) * 0.10
    )


def render_total_pnl_chart(summary_frame: pd.DataFrame) -> str:
    top = summary_frame.head(10).iloc[::-1]
    fig, ax = plt.subplots(figsize=(10, 5.6))
    ax.barh(top["strategy_label"] + " | " + top["daily_filter_label"], top["all_pnl_u"], color="#16423C")
    ax.set_title("Top 10 全样本总收益（5币合并）")
    ax.set_xlabel("PnL (U)")
    fig.tight_layout()
    return figure_to_base64(fig)


def render_test_pnl_chart(summary_frame: pd.DataFrame) -> str:
    top = summary_frame.head(10).iloc[::-1]
    fig, ax = plt.subplots(figsize=(10, 5.6))
    ax.barh(top["strategy_label"] + " | " + top["daily_filter_label"], top["test_pnl_u"], color="#C84B31")
    ax.set_title("Top 10 测试段收益（更看这个）")
    ax.set_xlabel("PnL (U)")
    fig.tight_layout()
    return figure_to_base64(fig)


def render_drawdown_chart(summary_frame: pd.DataFrame) -> str:
    top = summary_frame.head(10).iloc[::-1]
    fig, ax = plt.subplots(figsize=(10, 5.6))
    ax.barh(top["strategy_label"] + " | " + top["daily_filter_label"], top["test_max_drawdown_u"], color="#6A9C89")
    ax.set_title("Top 10 测试段最大回撤")
    ax.set_xlabel("Drawdown (U)")
    fig.tight_layout()
    return figure_to_base64(fig)


def render_best_equity_chart(trades_frame: pd.DataFrame, best_row: pd.Series) -> str:
    selected = trades_frame[
        (trades_frame["strategy_key"] == best_row["strategy_key"])
        & (trades_frame["daily_filter_key"] == best_row["daily_filter_key"])
    ].copy()
    selected = selected.sort_values(["exit_ts", "entry_ts", "coin"]).reset_index(drop=True)
    selected["equity_u"] = selected["pnl_u"].astype(float).cumsum()
    selected["drawdown_u"] = selected["equity_u"].cummax() - selected["equity_u"]

    fig, axes = plt.subplots(2, 1, figsize=(11, 7.5), sharex=True)
    axes[0].plot(selected["equity_u"].to_numpy(), color="#16423C", linewidth=1.6)
    axes[0].set_title("最佳组合 10U 风险累计收益")
    axes[0].set_ylabel("PnL (U)")
    axes[1].fill_between(
        np.arange(len(selected)),
        selected["drawdown_u"].to_numpy(),
        color="#C84B31",
        alpha=0.35,
    )
    axes[1].set_title("最佳组合回撤")
    axes[1].set_ylabel("Drawdown (U)")
    axes[1].set_xlabel("Trade Sequence")
    fig.tight_layout()
    return figure_to_base64(fig)


def build_html(
    *,
    summary_frame: pd.DataFrame,
    coin_frame: pd.DataFrame,
    payload: dict[str, object],
    total_chart: str,
    test_chart: str,
    dd_chart: str,
    best_equity_chart: str,
) -> str:
    best = summary_frame.iloc[0]
    baseline = summary_frame[
        (summary_frame["strategy_key"] == "ema55") & (summary_frame["daily_filter_key"] == "none")
    ].iloc[0]
    improve_test = float(best["test_pnl_u"]) - float(baseline["test_pnl_u"])
    improve_dd = float(best["test_max_drawdown_u"]) - float(baseline["test_max_drawdown_u"])
    top_table = dataframe_to_html(
        summary_frame.head(12)[
            [
                "strategy_label",
                "daily_filter_label",
                "all_pnl_u",
                "all_profit_factor",
                "all_win_rate",
                "test_pnl_u",
                "test_profit_factor",
                "test_win_rate",
                "test_max_drawdown_u",
                "test_positive_coins",
            ]
        ],
        float_cols={
            "all_pnl_u": 1,
            "all_profit_factor": 2,
            "all_win_rate": 1,
            "test_pnl_u": 1,
            "test_profit_factor": 2,
            "test_win_rate": 1,
            "test_max_drawdown_u": 1,
        },
        percent_cols={"all_win_rate", "test_win_rate"},
    )
    coin_table = dataframe_to_html(
        coin_frame[
            [
                "coin",
                "strategy_label",
                "daily_filter_label",
                "all_pnl_u",
                "all_profit_factor",
                "test_pnl_u",
                "test_profit_factor",
                "test_max_drawdown_u",
            ]
        ].sort_values(["coin", "test_pnl_u"], ascending=[True, False]).head(30),
        float_cols={
            "all_pnl_u": 1,
            "all_profit_factor": 2,
            "test_pnl_u": 1,
            "test_profit_factor": 2,
            "test_max_drawdown_u": 1,
        },
    )
    assumptions = payload["assumptions"]
    data_ranges = payload["data_ranges"]
    data_lines = "".join(
        f"<li><strong>{html.escape(COIN_LABELS.get(symbol, symbol))}</strong>: "
        f"{html.escape(str(info.get('start_utc', '-')))} -> {html.escape(str(info.get('end_utc', '-')))}, "
        f"1H={html.escape(str(info.get('entry_candles', '-')))}, 1D={html.escape(str(info.get('daily_candles', '-')))}</li>"
        for symbol, info in data_ranges.items()
    )
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>5币种均线斜率做空 + 日线过滤 10U 研究</title>
  <style>
    :root {{
      --bg: #f6f1e9;
      --ink: #1f1f1f;
      --muted: #5f6f65;
      --card: rgba(255,255,255,0.78);
      --line: rgba(22,66,60,0.14);
      --accent: #16423c;
      --accent-2: #c84b31;
      --accent-3: #6a9c89;
      --shadow: 0 18px 48px rgba(22,66,60,0.12);
      --radius: 24px;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Microsoft YaHei", "PingFang SC", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(200,75,49,0.14), transparent 30%),
        radial-gradient(circle at bottom right, rgba(22,66,60,0.10), transparent 28%),
        linear-gradient(180deg, #faf6ef 0%, var(--bg) 100%);
    }}
    .wrap {{
      width: min(1180px, calc(100vw - 28px));
      margin: 0 auto;
      padding: 28px 0 56px;
    }}
    .hero {{
      padding: 32px;
      border-radius: 32px;
      background: linear-gradient(135deg, rgba(22,66,60,0.96), rgba(106,156,137,0.92));
      color: #fdfbf7;
      box-shadow: var(--shadow);
      position: relative;
      overflow: hidden;
    }}
    .hero::after {{
      content: "";
      position: absolute;
      inset: auto -60px -80px auto;
      width: 220px;
      height: 220px;
      border-radius: 50%;
      background: rgba(255,255,255,0.08);
    }}
    h1, h2, h3, p {{ margin: 0; }}
    h1 {{
      font-size: clamp(28px, 4vw, 46px);
      line-height: 1.05;
      margin-top: 10px;
      max-width: 760px;
    }}
    .eyebrow {{
      letter-spacing: 0.16em;
      text-transform: uppercase;
      font-size: 12px;
      opacity: 0.88;
    }}
    .hero p {{
      margin-top: 14px;
      max-width: 820px;
      line-height: 1.7;
      color: rgba(253,251,247,0.90);
    }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(12, 1fr);
      gap: 18px;
      margin-top: 20px;
    }}
    .card {{
      background: var(--card);
      backdrop-filter: blur(12px);
      border: 1px solid var(--line);
      border-radius: var(--radius);
      box-shadow: var(--shadow);
      padding: 22px;
    }}
    .stat {{
      grid-column: span 3;
    }}
    .wide {{
      grid-column: span 6;
    }}
    .full {{
      grid-column: 1 / -1;
    }}
    .stat .value {{
      font-size: 32px;
      font-weight: 700;
      color: var(--accent);
      margin-top: 6px;
    }}
    .stat .hint {{
      margin-top: 8px;
      color: var(--muted);
      line-height: 1.6;
      font-size: 13px;
    }}
    .card h2 {{
      font-size: 19px;
      margin-bottom: 12px;
    }}
    .card h3 {{
      font-size: 16px;
      margin-bottom: 10px;
    }}
    ul {{
      margin: 0;
      padding-left: 18px;
      line-height: 1.8;
    }}
    img {{
      width: 100%;
      border-radius: 18px;
      border: 1px solid var(--line);
      background: #fff;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
    }}
    th, td {{
      text-align: left;
      padding: 10px 8px;
      border-bottom: 1px solid rgba(22,66,60,0.10);
      vertical-align: top;
    }}
    th {{
      color: var(--muted);
      font-weight: 600;
      background: rgba(22,66,60,0.03);
      position: sticky;
      top: 0;
    }}
    .badge {{
      display: inline-block;
      padding: 6px 10px;
      border-radius: 999px;
      background: rgba(200,75,49,0.10);
      color: var(--accent-2);
      font-size: 12px;
      font-weight: 700;
    }}
    .muted {{ color: var(--muted); }}
    @media (max-width: 960px) {{
      .stat, .wide {{ grid-column: 1 / -1; }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <div class="eyebrow">10U 战神 / Short Research</div>
      <h1>5 币种均线斜率做空研究：EMA55、MA55、EMA21、MA20、EMA34 + 日线过滤</h1>
      <p>本报告统一用 1H 做空入场、1D 趋势过滤、每笔固定风险 10U 统计，重点看哪套组合在 5 个币上更稳、更抗回撤，而不是只在单币上“看起来很猛”。</p>
    </section>

    <section class="grid">
      <div class="card stat">
        <div class="muted">综合第一名</div>
        <div class="value">{html.escape(str(best["strategy_label"]))}</div>
        <div class="hint">{html.escape(str(best["daily_filter_label"]))}</div>
      </div>
      <div class="card stat">
        <div class="muted">测试段收益</div>
        <div class="value">{float(best["test_pnl_u"]):.1f}U</div>
        <div class="hint">5币合并，固定风险 10U/笔</div>
      </div>
      <div class="card stat">
        <div class="muted">测试段回撤</div>
        <div class="value">{float(best["test_max_drawdown_u"]):.1f}U</div>
        <div class="hint">越低越稳</div>
      </div>
      <div class="card stat">
        <div class="muted">相对基准变化</div>
        <div class="value">{improve_test:+.1f}U</div>
        <div class="hint">基准 = EMA55 斜率空 + 无日线过滤；回撤变化 {improve_dd:+.1f}U</div>
      </div>

      <div class="card wide">
        <h2>研究口径</h2>
        <ul>
          <li>入场周期：{html.escape(str(assumptions["entry_bar"]))}；日线过滤周期：{html.escape(str(assumptions["daily_filter_bar"]))}</li>
          <li>固定风险：{float(assumptions["risk_per_trade_u"]):.1f}U/笔；手续费：{float(assumptions["taker_fee_rate"]) * 100:.3f}% taker</li>
          <li>斜率阈值：单根均线斜率 / 当前均线 ≤ {float(assumptions["slope_threshold_ratio"]):.4f}</li>
          <li>波动过滤：ATR14 百分位 ≤ {float(assumptions["atr_percentile_max"]) * 100:.0f}%</li>
          <li>退出模型：{html.escape(str(assumptions["exit_model"]))}</li>
          <li>资金备注：{html.escape(str(assumptions["capital_note"]))}</li>
        </ul>
      </div>

      <div class="card wide">
        <h2>我给你的直接建议</h2>
        <ul>
          <li>如果你想要稳一点：优先看慢线组合，通常 EMA55 / EMA34 更适合做空波段，不容易被 1H 噪音来回抽脸。</li>
          <li>如果你想要多打单：EMA21 / MA20 会更勤快，但一定更依赖日线过滤，否则手续费和假跌破会吃掉优势。</li>
          <li>日线过滤不是锦上添花，而是空头版本的“生存门槛”。当日线不在空头区间，1H 斜率空很容易变成逆势追空。</li>
        </ul>
      </div>

      <div class="card full">
        <h2>数据覆盖</h2>
        <ul>{data_lines}</ul>
      </div>

      <div class="card wide">
        <h2>全样本排行</h2>
        <img src="data:image/png;base64,{total_chart}" alt="全样本收益图">
      </div>
      <div class="card wide">
        <h2>测试段排行</h2>
        <img src="data:image/png;base64,{test_chart}" alt="测试段收益图">
      </div>
      <div class="card wide">
        <h2>测试段回撤</h2>
        <img src="data:image/png;base64,{dd_chart}" alt="测试段回撤图">
      </div>
      <div class="card wide">
        <h2>最佳组合资金曲线</h2>
        <img src="data:image/png;base64,{best_equity_chart}" alt="最佳组合资金曲线">
      </div>

      <div class="card full">
        <h2>Top 12 组合</h2>
        <div class="badge">优先看 test 段，而不是只看 all 段</div>
        <div style="margin-top:14px; overflow:auto;">{top_table}</div>
      </div>

      <div class="card full">
        <h2>分币结果节选</h2>
        <div class="muted" style="margin-bottom:10px;">每个币只展示靠前结果，避免表过长。</div>
        <div style="overflow:auto;">{coin_table}</div>
      </div>
    </section>
  </div>
</body>
</html>"""


def dataframe_to_html(
    frame: pd.DataFrame,
    *,
    float_cols: dict[str, int] | None = None,
    percent_cols: set[str] | None = None,
) -> str:
    float_cols = float_cols or {}
    percent_cols = percent_cols or set()
    headers = "".join(f"<th>{html.escape(str(col))}</th>" for col in frame.columns)
    rows = []
    for _, row in frame.iterrows():
        cells = []
        for col in frame.columns:
            value = row[col]
            if col in percent_cols:
                text = f"{float(value) * 100:.1f}%"
            elif col in float_cols:
                text = f"{float(value):.{float_cols[col]}f}"
            else:
                text = str(value)
            cells.append(f"<td>{html.escape(text)}</td>")
        rows.append("<tr>" + "".join(cells) + "</tr>")
    return f"<table><thead><tr>{headers}</tr></thead><tbody>{''.join(rows)}</tbody></table>"


def figure_to_base64(fig: plt.Figure) -> str:
    buffer = io.BytesIO()
    fig.savefig(buffer, format="png", dpi=160, bbox_inches="tight")
    plt.close(fig)
    return base64.b64encode(buffer.getvalue()).decode("ascii")


def ma_column_name(ma_type: str, period: int) -> str:
    return f"{ma_type}{period}"


def finite(value: object) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return math.nan
    return out if np.isfinite(out) else math.nan


def format_ts(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


if __name__ == "__main__":
    main()
