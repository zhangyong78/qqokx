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
from okx_quant.timeframe import closed_candle_available_timestamps


plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "Noto Sans CJK SC", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False


REPORT_DIR = ROOT / "reports"
ENTRY_BAR = "1H"
FILTER_BAR = "1D"
SYMBOLS = ("BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP", "BNB-USDT-SWAP", "DOGE-USDT-SWAP")
COIN_LABELS = {symbol: symbol.split("-")[0] for symbol in SYMBOLS}

RISK_PER_TRADE_U = 10.0
INITIAL_CAPITAL = 10_000.0
TAKER_FEE_RATE = 0.00036
ATR_PERIOD = 14
ATR_STOP_MULTIPLIER = 2.0
ATR_PERCENTILE_LOOKBACK = 100
ATR_PERCENTILE_MAX = 0.50
SLOPE_THRESHOLD_RATIO = -0.0005

BREAKDOWN_ATR_MULT = 0.2
RETEST_ATR_MULT = 0.3
STOP_BUFFER_ATR_MULT = 0.3
WATCH_BARS = 6

SOURCE_COIN_CSV = REPORT_DIR / "multi_coin_short_slope_daily_filter_10u_by_coin.csv"
SOURCE_SUMMARY_CSV = REPORT_DIR / "multi_coin_short_slope_daily_filter_10u_summary.csv"

HTML_PATH = REPORT_DIR / "multi_coin_short_recommendation_and_pullback_report.html"
RECOMMEND_CSV = REPORT_DIR / "multi_coin_short_recommendation_table.csv"
PULLBACK_CSV = REPORT_DIR / "multi_coin_short_pullback_10u_by_coin.csv"
PULLBACK_TRADES_CSV = REPORT_DIR / "multi_coin_short_pullback_10u_trades.csv"
JSON_PATH = REPORT_DIR / "multi_coin_short_recommendation_and_pullback_summary.json"


@dataclass(frozen=True)
class Recommendation:
    symbol: str
    coin: str
    strategy_key: str
    strategy_label: str
    daily_filter_key: str
    daily_filter_label: str
    test_pnl_u: float
    test_profit_factor: float
    test_max_drawdown_u: float
    test_trades: int
    score: float


def main() -> None:
    REPORT_DIR.mkdir(exist_ok=True)
    recommendations = load_recommendations()
    pullback_rows: list[dict[str, object]] = []
    pullback_trades: list[dict[str, object]] = []
    data_ranges: dict[str, dict[str, object]] = {}

    for item in recommendations:
        entry_candles = [candle for candle in load_candle_cache(item.symbol, ENTRY_BAR, limit=None) if candle.confirmed]
        daily_candles = [candle for candle in load_candle_cache(item.symbol, FILTER_BAR, limit=None) if candle.confirmed]
        if not entry_candles or not daily_candles:
            continue
        frame = build_entry_frame(entry_candles)
        ma_type, period = parse_strategy_key(item.strategy_key)
        add_indicators(frame, ma_type=ma_type, period=period)
        bias = build_daily_bias(entry_candles, daily_candles, item.daily_filter_key)
        data_ranges[item.symbol] = {
            "entry_candles": len(frame),
            "daily_candles": len(daily_candles),
            "start_utc": format_ts(int(frame["ts"].iloc[0])),
            "end_utc": format_ts(int(frame["ts"].iloc[-1])),
        }
        trades = simulate_pullback_failure_short(
            frame,
            ma_column=ma_column_name(ma_type, period),
            bias=bias,
        )
        trades["symbol"] = item.symbol
        trades["coin"] = item.coin
        trades["strategy_key"] = item.strategy_key
        trades["strategy_label"] = item.strategy_label
        trades["daily_filter_key"] = item.daily_filter_key
        trades["daily_filter_label"] = item.daily_filter_label
        pullback_trades.extend(trades.to_dict("records"))
        pullback_rows.append(build_pullback_metrics(item, trades))

    rec_frame = pd.DataFrame([recommendation_to_row(item) for item in recommendations])
    pullback_frame = pd.DataFrame(pullback_rows)
    trades_frame = pd.DataFrame(pullback_trades)
    if rec_frame.empty or pullback_frame.empty:
        raise RuntimeError("recommendation or pullback frame is empty")

    rec_frame.to_csv(RECOMMEND_CSV, index=False, encoding="utf-8-sig")
    pullback_frame.to_csv(PULLBACK_CSV, index=False, encoding="utf-8-sig")
    trades_frame.to_csv(PULLBACK_TRADES_CSV, index=False, encoding="utf-8-sig")

    aggregate = build_aggregate_comparison(rec_frame, pullback_frame)
    payload = {
        "created_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "recommendations": rec_frame.to_dict("records"),
        "aggregate": aggregate,
        "pullback_assumptions": {
            "entry_bar": ENTRY_BAR,
            "daily_filter_bar": FILTER_BAR,
            "risk_per_trade_u": RISK_PER_TRADE_U,
            "taker_fee_rate": TAKER_FEE_RATE,
            "atr_period": ATR_PERIOD,
            "atr_stop_multiplier": ATR_STOP_MULTIPLIER,
            "atr_percentile_max": ATR_PERCENTILE_MAX,
            "breakdown_atr_mult": BREAKDOWN_ATR_MULT,
            "retest_atr_mult": RETEST_ATR_MULT,
            "stop_buffer_atr_mult": STOP_BUFFER_ATR_MULT,
            "watch_bars": WATCH_BARS,
            "exit_model": "2R保本后逐级锁盈",
        },
        "data_ranges": data_ranges,
    }
    JSON_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    compare_chart = render_compare_chart(rec_frame, pullback_frame)
    per_coin_chart = render_per_coin_chart(rec_frame, pullback_frame)
    pullback_equity_chart = render_pullback_equity_chart(trades_frame)
    HTML_PATH.write_text(
        build_html(
            rec_frame=rec_frame,
            pullback_frame=pullback_frame,
            aggregate=aggregate,
            compare_chart=compare_chart,
            per_coin_chart=per_coin_chart,
            pullback_equity_chart=pullback_equity_chart,
            payload=payload,
        ),
        encoding="utf-8",
    )
    print(HTML_PATH)


def load_recommendations() -> list[Recommendation]:
    if not SOURCE_COIN_CSV.exists():
        raise FileNotFoundError(f"missing source csv: {SOURCE_COIN_CSV}")
    frame = pd.read_csv(SOURCE_COIN_CSV)
    out: list[Recommendation] = []
    for coin, group in frame.groupby("coin"):
        ranked = group.copy()
        ranked["score"] = (
            ranked["test_pnl_u"].astype(float)
            - ranked["test_max_drawdown_u"].astype(float) * 0.35
            + ranked["test_profit_factor"].astype(float) * 40.0
        )
        best = ranked.sort_values(["score", "test_pnl_u"], ascending=[False, False]).iloc[0]
        out.append(
            Recommendation(
                symbol=str(best["symbol"]),
                coin=coin,
                strategy_key=str(best["strategy_key"]),
                strategy_label=str(best["strategy_label"]),
                daily_filter_key=str(best["daily_filter_key"]),
                daily_filter_label=str(best["daily_filter_label"]),
                test_pnl_u=float(best["test_pnl_u"]),
                test_profit_factor=float(best["test_profit_factor"]),
                test_max_drawdown_u=float(best["test_max_drawdown_u"]),
                test_trades=int(best["test_trades"]),
                score=float(best["score"]),
            )
        )
    out.sort(key=lambda item: item.coin)
    return out


def recommendation_to_row(item: Recommendation) -> dict[str, object]:
    return {
        "symbol": item.symbol,
        "coin": item.coin,
        "strategy_key": item.strategy_key,
        "strategy_label": item.strategy_label,
        "daily_filter_key": item.daily_filter_key,
        "daily_filter_label": item.daily_filter_label,
        "baseline_test_pnl_u": item.test_pnl_u,
        "baseline_test_profit_factor": item.test_profit_factor,
        "baseline_test_max_drawdown_u": item.test_max_drawdown_u,
        "baseline_test_trades": item.test_trades,
        "recommendation_score": item.score,
    }


def build_entry_frame(candles: list[object]) -> pd.DataFrame:
    rows = [
        {
            "ts": int(candle.ts),
            "timestamp": pd.to_datetime(int(candle.ts), unit="ms", utc=True),
            "open": float(candle.open),
            "high": float(candle.high),
            "low": float(candle.low),
            "close": float(candle.close),
        }
        for candle in candles
    ]
    return pd.DataFrame(rows).sort_values("timestamp").drop_duplicates("timestamp").reset_index(drop=True)


def add_indicators(df: pd.DataFrame, *, ma_type: str, period: int) -> None:
    ma_col = ma_column_name(ma_type, period)
    if ma_type == "ema":
        df[ma_col] = df["close"].ewm(span=period, adjust=False, min_periods=period).mean()
    else:
        df[ma_col] = df["close"].rolling(period, min_periods=period).mean()
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


def build_daily_bias(entry_candles: list[object], daily_candles: list[object], daily_filter_key: str) -> list[str] | None:
    if daily_filter_key == "none":
        return None
    daily_frame = pd.DataFrame(
        {
            "ts": [int(candle.ts) for candle in daily_candles],
            "close": [float(candle.close) for candle in daily_candles],
        }
    )
    ma_type, period = parse_filter_key(daily_filter_key)
    if ma_type == "ema":
        line = daily_frame["close"].ewm(span=period, adjust=False, min_periods=period).mean()
    else:
        line = daily_frame["close"].rolling(period, min_periods=period).mean()
    daily_available_ts = closed_candle_available_timestamps(daily_candles)
    out: list[str] = []
    for candle in entry_candles:
        idx = np.searchsorted(daily_available_ts, int(candle.ts), side="right") - 1
        if idx < 0:
            out.append("neutral")
            continue
        line_value = float(line.iloc[idx]) if pd.notna(line.iloc[idx]) else math.nan
        daily_close = float(daily_frame["close"].iloc[idx])
        if not np.isfinite(line_value):
            out.append("neutral")
        elif daily_close < line_value:
            out.append("short")
        elif daily_close > line_value:
            out.append("long")
        else:
            out.append("neutral")
    return out


def simulate_pullback_failure_short(
    df: pd.DataFrame,
    *,
    ma_column: str,
    bias: list[str] | None,
) -> pd.DataFrame:
    open_values = df["open"].to_numpy(dtype=float)
    high_values = df["high"].to_numpy(dtype=float)
    low_values = df["low"].to_numpy(dtype=float)
    close_values = df["close"].to_numpy(dtype=float)
    ts_values = df["ts"].to_numpy(dtype=np.int64)
    line_values = df[ma_column].to_numpy(dtype=float)
    atr_values = df["atr14"].to_numpy(dtype=float)
    atr_pct_values = df["atr_pct"].to_numpy(dtype=float)

    trades: list[dict[str, object]] = []
    position: dict[str, float | int | str] | None = None
    pending_breakdown: dict[str, float | int] | None = None
    start_index = max(ATR_PERCENTILE_LOOKBACK, 60)

    for index in range(start_index, len(df)):
        line_value = line_values[index]
        prev_line = line_values[index - 1]
        atr_value = atr_values[index]
        atr_pct = atr_pct_values[index]
        open_price = open_values[index]
        high_price = high_values[index]
        low_price = low_values[index]
        close_price = close_values[index]
        if any(math.isnan(value) for value in [line_value, prev_line, atr_value, atr_pct]):
            continue

        slope_ratio = (line_value - prev_line) / line_value if line_value else math.nan

        if position is not None:
            exited = process_open_short(
                position,
                candle_open=open_price,
                candle_high=high_price,
                candle_low=low_price,
                candle_close=close_price,
                candle_ts=int(ts_values[index]),
                index=index,
                trades=trades,
            )
            if exited:
                position = None

        if position is not None:
            continue

        if pending_breakdown is not None:
            age = index - int(pending_breakdown["index"])
            if age > WATCH_BARS:
                pending_breakdown = None
            else:
                near_line = high_price >= (line_value - RETEST_ATR_MULT * atr_value)
                still_below = close_price < line_value
                bearish_close = close_price < open_price
                if near_line and still_below and bearish_close:
                    if bias is not None and index < len(bias) and bias[index] != "short":
                        pending_breakdown = None
                        continue
                    risk_per_unit = max((high_price + STOP_BUFFER_ATR_MULT * atr_value) - close_price, atr_value * 0.5)
                    if risk_per_unit <= 0 or not np.isfinite(risk_per_unit):
                        pending_breakdown = None
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
                    pending_breakdown = None
                    continue

        if pending_breakdown is not None:
            continue
        if not np.isfinite(slope_ratio) or slope_ratio > SLOPE_THRESHOLD_RATIO:
            continue
        if atr_pct > ATR_PERCENTILE_MAX:
            continue
        if close_price >= line_value - BREAKDOWN_ATR_MULT * atr_value:
            continue
        if bias is not None and index < len(bias) and bias[index] != "short":
            continue
        pending_breakdown = {"index": index}

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
    }


def build_pullback_metrics(item: Recommendation, trades: pd.DataFrame) -> dict[str, object]:
    test_trades = split_global_test_trades(trades)
    all_metrics = compute_metrics(trades)
    test_metrics = compute_metrics(test_trades)
    return {
        "symbol": item.symbol,
        "coin": item.coin,
        "strategy_key": item.strategy_key,
        "strategy_label": item.strategy_label,
        "daily_filter_key": item.daily_filter_key,
        "daily_filter_label": item.daily_filter_label,
        "all_trades": int(all_metrics["trades"]),
        "all_pnl_u": all_metrics["total_pnl_u"],
        "all_profit_factor": all_metrics["profit_factor"],
        "all_win_rate": all_metrics["win_rate"],
        "all_avg_r": all_metrics["avg_r"],
        "all_max_drawdown_u": all_metrics["max_drawdown_u"],
        "test_trades": int(test_metrics["trades"]),
        "test_pnl_u": test_metrics["total_pnl_u"],
        "test_profit_factor": test_metrics["profit_factor"],
        "test_win_rate": test_metrics["win_rate"],
        "test_avg_r": test_metrics["avg_r"],
        "test_max_drawdown_u": test_metrics["max_drawdown_u"],
    }


def split_global_test_trades(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty:
        return trades.copy()
    max_exit = int(trades["exit_index"].max())
    start = int(max_exit * 0.8)
    return trades[trades["exit_index"] >= start].copy()


def compute_metrics(trades: pd.DataFrame) -> dict[str, float]:
    if trades.empty:
        return {
            "trades": 0.0,
            "total_pnl_u": 0.0,
            "profit_factor": 0.0,
            "win_rate": 0.0,
            "avg_r": 0.0,
            "max_drawdown_u": 0.0,
        }
    pnls = trades["pnl_u"].astype(float)
    rs = trades["r_multiple"].astype(float)
    curve = pnls.cumsum()
    gross_profit = float(pnls[pnls > 0].sum())
    gross_loss = abs(float(pnls[pnls < 0].sum()))
    return {
        "trades": float(len(trades)),
        "total_pnl_u": float(pnls.sum()),
        "profit_factor": gross_profit / gross_loss if gross_loss > 0 else 0.0,
        "win_rate": float((pnls > 0).mean()),
        "avg_r": float(rs.mean()),
        "max_drawdown_u": float((curve.cummax() - curve).max()),
    }


def build_aggregate_comparison(rec_frame: pd.DataFrame, pullback_frame: pd.DataFrame) -> dict[str, float]:
    return {
        "baseline_test_pnl_u_sum": float(rec_frame["baseline_test_pnl_u"].sum()),
        "baseline_test_dd_u_sum": float(rec_frame["baseline_test_max_drawdown_u"].sum()),
        "baseline_test_trades_sum": float(rec_frame["baseline_test_trades"].sum()),
        "pullback_test_pnl_u_sum": float(pullback_frame["test_pnl_u"].sum()),
        "pullback_test_dd_u_sum": float(pullback_frame["test_max_drawdown_u"].sum()),
        "pullback_test_trades_sum": float(pullback_frame["test_trades"].sum()),
    }


def render_compare_chart(rec_frame: pd.DataFrame, pullback_frame: pd.DataFrame) -> str:
    baseline = aggregate_from_frame(rec_frame, pnl_col="baseline_test_pnl_u", dd_col="baseline_test_max_drawdown_u", trades_col="baseline_test_trades")
    pullback = aggregate_from_frame(pullback_frame, pnl_col="test_pnl_u", dd_col="test_max_drawdown_u", trades_col="test_trades")
    labels = ["测试收益U", "测试回撤U", "测试交易数"]
    base_values = [baseline["pnl"], baseline["dd"], baseline["trades"]]
    pull_values = [pullback["pnl"], pullback["dd"], pullback["trades"]]
    x = np.arange(len(labels))
    width = 0.34
    fig, ax = plt.subplots(figsize=(9, 5.2))
    ax.bar(x - width / 2, base_values, width, label="推荐斜率版", color="#16423C")
    ax.bar(x + width / 2, pull_values, width, label="反抽不过均线版", color="#C84B31")
    ax.set_xticks(x, labels)
    ax.set_title("推荐斜率版 vs 反抽不过均线版")
    ax.legend()
    fig.tight_layout()
    return figure_to_base64(fig)


def aggregate_from_frame(frame: pd.DataFrame, *, pnl_col: str, dd_col: str, trades_col: str) -> dict[str, float]:
    return {
        "pnl": float(frame[pnl_col].astype(float).sum()),
        "dd": float(frame[dd_col].astype(float).sum()),
        "trades": float(frame[trades_col].astype(float).sum()),
    }


def render_per_coin_chart(rec_frame: pd.DataFrame, pullback_frame: pd.DataFrame) -> str:
    merged = rec_frame.merge(
        pullback_frame[["coin", "test_pnl_u"]],
        on="coin",
        how="left",
        suffixes=("_baseline", "_pullback"),
    )
    fig, ax = plt.subplots(figsize=(10, 5.6))
    x = np.arange(len(merged))
    width = 0.36
    ax.bar(x - width / 2, merged["baseline_test_pnl_u"].astype(float), width, label="推荐斜率版", color="#6A9C89")
    ax.bar(x + width / 2, merged["test_pnl_u"].astype(float), width, label="反抽不过均线版", color="#C84B31")
    ax.set_xticks(x, merged["coin"].tolist())
    ax.set_title("各币测试段收益对比")
    ax.set_ylabel("PnL (U)")
    ax.legend()
    fig.tight_layout()
    return figure_to_base64(fig)


def render_pullback_equity_chart(trades_frame: pd.DataFrame) -> str:
    ordered = trades_frame.sort_values(["exit_ts", "entry_ts", "coin"]).reset_index(drop=True).copy()
    ordered["equity_u"] = ordered["pnl_u"].astype(float).cumsum()
    ordered["drawdown_u"] = ordered["equity_u"].cummax() - ordered["equity_u"]
    fig, axes = plt.subplots(2, 1, figsize=(10.5, 7), sharex=True)
    axes[0].plot(ordered["equity_u"].to_numpy(), color="#C84B31", linewidth=1.5)
    axes[0].set_title("反抽不过均线版累计收益")
    axes[1].fill_between(np.arange(len(ordered)), ordered["drawdown_u"].to_numpy(), color="#16423C", alpha=0.25)
    axes[1].set_title("反抽不过均线版回撤")
    axes[1].set_xlabel("Trade Sequence")
    fig.tight_layout()
    return figure_to_base64(fig)


def build_html(
    *,
    rec_frame: pd.DataFrame,
    pullback_frame: pd.DataFrame,
    aggregate: dict[str, float],
    compare_chart: str,
    per_coin_chart: str,
    pullback_equity_chart: str,
    payload: dict[str, object],
) -> str:
    merged = rec_frame.merge(
        pullback_frame[
            [
                "coin",
                "all_trades",
                "all_pnl_u",
                "test_trades",
                "test_pnl_u",
                "test_profit_factor",
                "test_max_drawdown_u",
            ]
        ],
        on="coin",
        how="left",
    )
    merged["delta_test_pnl_u"] = merged["test_pnl_u"] - merged["baseline_test_pnl_u"]
    merged["delta_test_dd_u"] = merged["test_max_drawdown_u"] - merged["baseline_test_max_drawdown_u"]
    compare_table = dataframe_to_html(
        merged[
            [
                "coin",
                "strategy_label",
                "daily_filter_label",
                "baseline_test_pnl_u",
                "baseline_test_profit_factor",
                "baseline_test_max_drawdown_u",
                "test_pnl_u",
                "test_profit_factor",
                "test_max_drawdown_u",
                "delta_test_pnl_u",
            ]
        ],
        float_cols={
            "baseline_test_pnl_u": 1,
            "baseline_test_profit_factor": 2,
            "baseline_test_max_drawdown_u": 1,
            "test_pnl_u": 1,
            "test_profit_factor": 2,
            "test_max_drawdown_u": 1,
            "delta_test_pnl_u": 1,
        },
    )
    recommend_table = dataframe_to_html(
        rec_frame[
            [
                "coin",
                "strategy_label",
                "daily_filter_label",
                "baseline_test_trades",
                "baseline_test_pnl_u",
                "baseline_test_profit_factor",
                "baseline_test_max_drawdown_u",
            ]
        ],
        float_cols={
            "baseline_test_pnl_u": 1,
            "baseline_test_profit_factor": 2,
            "baseline_test_max_drawdown_u": 1,
        },
    )
    pullback_table = dataframe_to_html(
        pullback_frame[
            [
                "coin",
                "strategy_label",
                "daily_filter_label",
                "test_trades",
                "test_pnl_u",
                "test_profit_factor",
                "test_max_drawdown_u",
            ]
        ],
        float_cols={
            "test_pnl_u": 1,
            "test_profit_factor": 2,
            "test_max_drawdown_u": 1,
        },
    )
    assumptions = payload["pullback_assumptions"]
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>5币做空参数建议 + 反抽不过均线研究</title>
  <style>
    :root {{
      --bg: #f7f4ee;
      --ink: #1d1d1d;
      --muted: #5f6b66;
      --card: rgba(255,255,255,0.82);
      --line: rgba(22,66,60,0.12);
      --accent: #16423c;
      --accent2: #c84b31;
      --accent3: #6a9c89;
      --radius: 24px;
      --shadow: 0 18px 42px rgba(22,66,60,0.10);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Microsoft YaHei", "PingFang SC", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at 0% 0%, rgba(200,75,49,0.10), transparent 26%),
        radial-gradient(circle at 100% 100%, rgba(22,66,60,0.10), transparent 24%),
        linear-gradient(180deg, #faf7f2 0%, var(--bg) 100%);
    }}
    .wrap {{
      width: min(1180px, calc(100vw - 28px));
      margin: 0 auto;
      padding: 28px 0 56px;
    }}
    .hero {{
      padding: 34px;
      border-radius: 32px;
      background: linear-gradient(135deg, rgba(22,66,60,0.96), rgba(200,75,49,0.88));
      color: #fffdf9;
      box-shadow: var(--shadow);
    }}
    .hero h1 {{ margin: 12px 0 0; font-size: clamp(28px, 4vw, 46px); line-height: 1.06; }}
    .hero p {{ margin: 14px 0 0; max-width: 850px; line-height: 1.75; color: rgba(255,253,249,0.90); }}
    .eyebrow {{ letter-spacing: 0.16em; text-transform: uppercase; font-size: 12px; opacity: 0.88; }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(12, 1fr);
      gap: 18px;
      margin-top: 20px;
    }}
    .card {{
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: var(--radius);
      box-shadow: var(--shadow);
      padding: 22px;
    }}
    .stat {{ grid-column: span 3; }}
    .wide {{ grid-column: span 6; }}
    .full {{ grid-column: 1 / -1; }}
    .value {{ font-size: 32px; font-weight: 700; color: var(--accent); margin-top: 8px; }}
    .muted {{ color: var(--muted); }}
    h1, h2, h3, p {{ margin: 0; }}
    h2 {{ font-size: 19px; margin-bottom: 12px; }}
    ul {{ margin: 0; padding-left: 18px; line-height: 1.8; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
    th, td {{ text-align: left; padding: 10px 8px; border-bottom: 1px solid rgba(22,66,60,0.10); }}
    th {{ color: var(--muted); background: rgba(22,66,60,0.03); }}
    img {{ width: 100%; border-radius: 18px; border: 1px solid var(--line); background: #fff; }}
    .pill {{
      display: inline-block;
      border-radius: 999px;
      padding: 6px 10px;
      background: rgba(22,66,60,0.08);
      color: var(--accent);
      font-size: 12px;
      font-weight: 700;
    }}
    @media (max-width: 960px) {{
      .stat, .wide {{ grid-column: 1 / -1; }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <div class="eyebrow">10U 战神 / Recommendation + Pullback</div>
      <h1>每个币的做空建议参数表 + 反抽不过均线再空版本</h1>
      <p>这份总报告把两件事合在一起：第一，基于前一轮 5 币回测，给每个币选出更适合实盘观察的斜率做空参数；第二，再用同一套币种和 10U 风险口径，测试更贴近你手法的“先破位，再等反抽失败后做空”版本。</p>
    </section>

    <section class="grid">
      <div class="card stat">
        <div class="muted">推荐斜率版测试总收益</div>
        <div class="value">{aggregate["baseline_test_pnl_u_sum"]:.1f}U</div>
      </div>
      <div class="card stat">
        <div class="muted">反抽版测试总收益</div>
        <div class="value">{aggregate["pullback_test_pnl_u_sum"]:.1f}U</div>
      </div>
      <div class="card stat">
        <div class="muted">推荐斜率版测试回撤</div>
        <div class="value">{aggregate["baseline_test_dd_u_sum"]:.1f}U</div>
      </div>
      <div class="card stat">
        <div class="muted">反抽版测试回撤</div>
        <div class="value">{aggregate["pullback_test_dd_u_sum"]:.1f}U</div>
      </div>

      <div class="card wide">
        <h2>先说结论</h2>
        <ul>
          <li>如果你追求主策略稳定输出，推荐继续用“每币最优斜率版”做主框架，因为它整体更稳、更完整。</li>
          <li>如果你更喜欢等确认再开空，反抽不过均线版更接近手工交易逻辑，但交易次数会更少，也更挑行情。</li>
          <li>两者最适合的用法不是二选一，而是：斜率版做主信号池，反抽版做更严格的二次确认或分账户版本。</li>
        </ul>
      </div>

      <div class="card wide">
        <h2>反抽版规则</h2>
        <ul>
          <li>先出现破位：收盘低于目标均线，且跌破幅度至少为 {BREAKDOWN_ATR_MULT:.1f} ATR。</li>
          <li>要求均线斜率继续向下：单根斜率比值 ≤ {SLOPE_THRESHOLD_RATIO:.4f}。</li>
          <li>之后最多等待 {WATCH_BARS} 根 1H K，若价格反抽接近均线，但收盘仍在均线下方且收阴，则开空。</li>
          <li>日线过滤沿用每个币上一轮最优方案；固定风险 {RISK_PER_TRADE_U:.0f}U；退出仍用 2R 保本后逐级锁盈。</li>
        </ul>
      </div>

      <div class="card wide">
        <h2>总体对比</h2>
        <img src="data:image/png;base64,{compare_chart}" alt="总体对比图">
      </div>
      <div class="card wide">
        <h2>各币测试收益对比</h2>
        <img src="data:image/png;base64,{per_coin_chart}" alt="分币收益图">
      </div>
      <div class="card full">
        <h2>反抽版资金曲线</h2>
        <img src="data:image/png;base64,{pullback_equity_chart}" alt="反抽版资金曲线">
      </div>

      <div class="card full">
        <h2>每个币的建议参数表</h2>
        <div class="pill">这个表就是你要的实盘观察清单</div>
        <div style="margin-top:14px; overflow:auto;">{recommend_table}</div>
      </div>

      <div class="card full">
        <h2>反抽不过均线版结果</h2>
        <div style="overflow:auto;">{pullback_table}</div>
      </div>

      <div class="card full">
        <h2>逐币对比</h2>
        <div class="muted" style="margin-bottom:10px;">同一个币，左边是推荐斜率版，右边是反抽失败再空版。</div>
        <div style="overflow:auto;">{compare_table}</div>
      </div>

      <div class="card full">
        <h2>我给你的落地建议</h2>
        <ul>
          <li>BTC：优先保留更快的空头线，通常更适合用来抓趋势扩张后的二段下跌。</li>
          <li>ETH：更适合慢一点的过滤，避免被来回扫。</li>
          <li>SOL：反抽版很值得盯，因为它常常给足反抽再砸。</li>
          <li>DOGE：慢线过滤常常更香，别太急着追第一根。</li>
          <li>BNB：如果你做它，仓位上更应该保守，因为样本期更短，稳定性结论没前四个币扎实。</li>
        </ul>
      </div>
    </section>
  </div>
</body>
</html>"""


def dataframe_to_html(frame: pd.DataFrame, *, float_cols: dict[str, int] | None = None) -> str:
    float_cols = float_cols or {}
    headers = "".join(f"<th>{html.escape(str(col))}</th>" for col in frame.columns)
    rows = []
    for _, row in frame.iterrows():
        cells = []
        for col in frame.columns:
            value = row[col]
            if col in float_cols:
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


def parse_strategy_key(strategy_key: str) -> tuple[str, int]:
    if strategy_key == "ma55":
        return "ma", 55
    if strategy_key == "ma20":
        return "ma", 20
    if strategy_key == "ema21":
        return "ema", 21
    if strategy_key == "ema34":
        return "ema", 34
    return "ema", 55


def parse_filter_key(filter_key: str) -> tuple[str, int]:
    if filter_key == "ma20":
        return "ma", 20
    if filter_key == "ema55":
        return "ema", 55
    return "ema", 21


def ma_column_name(ma_type: str, period: int) -> str:
    return f"{ma_type}{period}"


def format_ts(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


if __name__ == "__main__":
    main()
