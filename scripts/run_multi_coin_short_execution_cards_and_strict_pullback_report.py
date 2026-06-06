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
SOURCE_RECOMMEND_CSV = REPORT_DIR / "multi_coin_short_recommendation_table.csv"
SOURCE_PULLBACK_CSV = REPORT_DIR / "multi_coin_short_pullback_10u_by_coin.csv"
HTML_PATH = REPORT_DIR / "multi_coin_short_execution_cards_and_strict_pullback_report.html"
STRICT_CSV_PATH = REPORT_DIR / "multi_coin_short_strict_pullback_10u_by_coin.csv"
STRICT_TRADES_CSV_PATH = REPORT_DIR / "multi_coin_short_strict_pullback_10u_trades.csv"
JSON_PATH = REPORT_DIR / "multi_coin_short_execution_cards_and_strict_pullback_summary.json"

RISK_PER_TRADE_U = 10.0
TAKER_FEE_RATE = 0.00036
ATR_PERIOD = 14
ATR_PERCENTILE_LOOKBACK = 100
ATR_PERCENTILE_MAX = 0.50
SLOPE_THRESHOLD_RATIO = -0.0005
BREAKDOWN_ATR_MULT = 0.2
RETEST_ATR_MULT = 0.3
STOP_BUFFER_ATR_MULT = 0.3
WATCH_BARS = 6
BODY_RECLAIM_MAX_RATIO = 0.5


@dataclass(frozen=True)
class Recommendation:
    symbol: str
    coin: str
    strategy_key: str
    strategy_label: str
    daily_filter_key: str
    daily_filter_label: str
    baseline_test_pnl_u: float
    baseline_test_profit_factor: float
    baseline_test_max_drawdown_u: float
    baseline_test_trades: int


def main() -> None:
    REPORT_DIR.mkdir(exist_ok=True)
    recommendations = load_recommendations()
    original_pullback = load_original_pullback()
    strict_rows: list[dict[str, object]] = []
    strict_trades: list[dict[str, object]] = []
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
        trades = simulate_strict_pullback_failure_short(
            frame,
            ma_column=ma_column_name(ma_type, period),
            bias=bias,
        )
        trades["symbol"] = item.symbol
        trades["coin"] = item.coin
        trades["strategy_label"] = item.strategy_label
        trades["daily_filter_label"] = item.daily_filter_label
        strict_trades.extend(trades.to_dict("records"))
        strict_rows.append(build_metrics_row(item, trades))

    strict_frame = pd.DataFrame(strict_rows).sort_values("coin").reset_index(drop=True)
    strict_trades_frame = pd.DataFrame(strict_trades).sort_values(["exit_ts", "entry_ts", "coin"]).reset_index(drop=True)
    strict_frame.to_csv(STRICT_CSV_PATH, index=False, encoding="utf-8-sig")
    strict_trades_frame.to_csv(STRICT_TRADES_CSV_PATH, index=False, encoding="utf-8-sig")

    merged = merge_frames(recommendations, original_pullback, strict_frame)
    payload = {
        "created_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "strict_rule": "反抽收盘不能收回破位阴线实体超过50%",
        "body_reclaim_max_ratio": BODY_RECLAIM_MAX_RATIO,
        "watch_bars": WATCH_BARS,
        "data_ranges": data_ranges,
        "aggregate": build_aggregate(merged),
    }
    JSON_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    compare_chart = render_compare_chart(merged)
    pnl_chart = render_pnl_chart(merged)
    equity_chart = render_equity_chart(strict_trades_frame)
    HTML_PATH.write_text(
        build_html(
            merged=merged,
            payload=payload,
            compare_chart=compare_chart,
            pnl_chart=pnl_chart,
            equity_chart=equity_chart,
        ),
        encoding="utf-8",
    )
    print(HTML_PATH)


def load_recommendations() -> list[Recommendation]:
    frame = pd.read_csv(SOURCE_RECOMMEND_CSV)
    out: list[Recommendation] = []
    for row in frame.to_dict("records"):
        out.append(
            Recommendation(
                symbol=str(row["symbol"]),
                coin=str(row["coin"]),
                strategy_key=str(row["strategy_key"]),
                strategy_label=str(row["strategy_label"]),
                daily_filter_key=str(row["daily_filter_key"]),
                daily_filter_label=str(row["daily_filter_label"]),
                baseline_test_pnl_u=float(row["baseline_test_pnl_u"]),
                baseline_test_profit_factor=float(row["baseline_test_profit_factor"]),
                baseline_test_max_drawdown_u=float(row["baseline_test_max_drawdown_u"]),
                baseline_test_trades=int(row["baseline_test_trades"]),
            )
        )
    return out


def load_original_pullback() -> pd.DataFrame:
    return pd.read_csv(SOURCE_PULLBACK_CSV)


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
    tr = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - prev_close).abs(),
            (df["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    df["atr14"] = tr.ewm(alpha=1 / ATR_PERIOD, adjust=False, min_periods=ATR_PERIOD).mean()
    df["atr_pct"] = rolling_percentile(df["atr14"], ATR_PERCENTILE_LOOKBACK)


def rolling_percentile(series: pd.Series, lookback: int) -> pd.Series:
    return series.rolling(lookback, min_periods=lookback).apply(lambda x: float(np.mean(x <= x[-1])), raw=True)


def build_daily_bias(entry_candles: list[object], daily_candles: list[object], key: str) -> list[str] | None:
    if key == "none":
        return None
    daily_frame = pd.DataFrame({"ts": [int(c.ts) for c in daily_candles], "close": [float(c.close) for c in daily_candles]})
    ma_type, period = parse_filter_key(key)
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
        value = float(line.iloc[idx]) if pd.notna(line.iloc[idx]) else math.nan
        close = float(daily_frame["close"].iloc[idx])
        if not np.isfinite(value):
            out.append("neutral")
        elif close < value:
            out.append("short")
        elif close > value:
            out.append("long")
        else:
            out.append("neutral")
    return out


def simulate_strict_pullback_failure_short(
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
    pending: dict[str, float | int] | None = None
    start_index = max(ATR_PERCENTILE_LOOKBACK, 60)

    for index in range(start_index, len(df)):
        line_value = line_values[index]
        prev_line = line_values[index - 1]
        atr_value = atr_values[index]
        atr_pct = atr_pct_values[index]
        candle_open = open_values[index]
        candle_high = high_values[index]
        candle_low = low_values[index]
        candle_close = close_values[index]
        if any(math.isnan(v) for v in [line_value, prev_line, atr_value, atr_pct]):
            continue

        slope_ratio = (line_value - prev_line) / line_value if line_value else math.nan

        if position is not None:
            exited = process_open_short(
                position,
                candle_open=candle_open,
                candle_high=candle_high,
                candle_low=candle_low,
                candle_close=candle_close,
                candle_ts=int(ts_values[index]),
                index=index,
                trades=trades,
            )
            if exited:
                position = None

        if position is not None:
            continue

        if pending is not None:
            age = index - int(pending["index"])
            if age > WATCH_BARS:
                pending = None
            else:
                near_line = candle_high >= (line_value - RETEST_ATR_MULT * atr_value)
                still_below = candle_close < line_value
                bearish_close = candle_close < candle_open
                midpoint_ok = candle_close <= float(pending["max_reclaim_close"])
                if near_line and still_below and bearish_close and midpoint_ok:
                    if bias is not None and index < len(bias) and bias[index] != "short":
                        pending = None
                        continue
                    risk_per_unit = max((candle_high + STOP_BUFFER_ATR_MULT * atr_value) - candle_close, atr_value * 0.5)
                    if risk_per_unit <= 0 or not np.isfinite(risk_per_unit):
                        pending = None
                        continue
                    fee_offset = candle_close * TAKER_FEE_RATE * 2.0
                    position = {
                        "entry_index": index,
                        "entry_ts": int(ts_values[index]),
                        "entry_price": candle_close,
                        "risk_per_unit": risk_per_unit,
                        "stop": candle_close + risk_per_unit,
                        "stop_reason": "stop_loss",
                        "fee_offset": fee_offset,
                        "next_dynamic_r": 2.0,
                    }
                    pending = None
                    continue

        if pending is not None:
            continue
        if not np.isfinite(slope_ratio) or slope_ratio > SLOPE_THRESHOLD_RATIO:
            continue
        if atr_pct > ATR_PERCENTILE_MAX:
            continue
        if candle_close >= line_value - BREAKDOWN_ATR_MULT * atr_value:
            continue
        if candle_close >= candle_open:
            continue
        if bias is not None and index < len(bias) and bias[index] != "short":
            continue
        body_mid = candle_close + (candle_open - candle_close) * BODY_RECLAIM_MAX_RATIO
        pending = {"index": index, "max_reclaim_close": body_mid}

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
    path = candle_path_points(candle_open=candle_open, candle_high=candle_high, candle_low=candle_low, candle_close=candle_close)
    for start, end in zip(path, path[1:]):
        if end > start:
            stop_price = float(position["stop"])
            if start <= stop_price <= end:
                trades.append(close_trade(position, index, candle_ts, stop_price, str(position["stop_reason"])))
                return True
        else:
            advance_step_dynamic(position, end)
    return False


def candle_path_points(*, candle_open: float, candle_high: float, candle_low: float, candle_close: float) -> tuple[float, float, float, float]:
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


def close_trade(position: dict[str, float | int | str], exit_index: int, exit_ts: int, exit_price: float, exit_reason: str) -> dict[str, object]:
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
        "pnl_u": pnl_u,
        "r_multiple": pnl_u / RISK_PER_TRADE_U,
        "exit_reason": exit_reason,
    }


def build_metrics_row(item: Recommendation, trades: pd.DataFrame) -> dict[str, object]:
    metrics = compute_metrics(split_test_trades(trades))
    return {
        "symbol": item.symbol,
        "coin": item.coin,
        "strategy_label": item.strategy_label,
        "daily_filter_label": item.daily_filter_label,
        "test_trades": int(metrics["trades"]),
        "test_pnl_u": metrics["pnl"],
        "test_profit_factor": metrics["profit_factor"],
        "test_max_drawdown_u": metrics["drawdown"],
        "test_win_rate": metrics["win_rate"],
    }


def split_test_trades(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty:
        return trades.copy()
    start = int(trades["exit_index"].max() * 0.8)
    return trades[trades["exit_index"] >= start].copy()


def compute_metrics(trades: pd.DataFrame) -> dict[str, float]:
    if trades.empty:
        return {"trades": 0.0, "pnl": 0.0, "profit_factor": 0.0, "drawdown": 0.0, "win_rate": 0.0}
    pnls = trades["pnl_u"].astype(float)
    gross_profit = float(pnls[pnls > 0].sum())
    gross_loss = abs(float(pnls[pnls < 0].sum()))
    curve = pnls.cumsum()
    return {
        "trades": float(len(trades)),
        "pnl": float(pnls.sum()),
        "profit_factor": gross_profit / gross_loss if gross_loss > 0 else 0.0,
        "drawdown": float((curve.cummax() - curve).max()),
        "win_rate": float((pnls > 0).mean()),
    }


def merge_frames(recommendations: list[Recommendation], original_pullback: pd.DataFrame, strict_frame: pd.DataFrame) -> pd.DataFrame:
    rec = pd.DataFrame([r.__dict__ for r in recommendations])
    original = original_pullback[["coin", "test_trades", "test_pnl_u", "test_profit_factor", "test_max_drawdown_u"]].rename(
        columns={
            "test_trades": "pullback_test_trades",
            "test_pnl_u": "pullback_test_pnl_u",
            "test_profit_factor": "pullback_test_profit_factor",
            "test_max_drawdown_u": "pullback_test_max_drawdown_u",
        }
    )
    strict = strict_frame.rename(
        columns={
            "test_trades": "strict_test_trades",
            "test_pnl_u": "strict_test_pnl_u",
            "test_profit_factor": "strict_test_profit_factor",
            "test_max_drawdown_u": "strict_test_max_drawdown_u",
            "test_win_rate": "strict_test_win_rate",
        }
    )
    return rec.merge(original, on="coin", how="left").merge(strict, on=["coin", "symbol", "strategy_label", "daily_filter_label"], how="left")


def build_aggregate(merged: pd.DataFrame) -> dict[str, float]:
    return {
        "baseline_pnl": float(merged["baseline_test_pnl_u"].sum()),
        "baseline_dd": float(merged["baseline_test_max_drawdown_u"].sum()),
        "pullback_pnl": float(merged["pullback_test_pnl_u"].sum()),
        "pullback_dd": float(merged["pullback_test_max_drawdown_u"].sum()),
        "strict_pnl": float(merged["strict_test_pnl_u"].sum()),
        "strict_dd": float(merged["strict_test_max_drawdown_u"].sum()),
    }


def render_compare_chart(merged: pd.DataFrame) -> str:
    labels = ["推荐斜率版", "原反抽版", "严格反抽版"]
    pnl_values = [
        float(merged["baseline_test_pnl_u"].sum()),
        float(merged["pullback_test_pnl_u"].sum()),
        float(merged["strict_test_pnl_u"].sum()),
    ]
    dd_values = [
        float(merged["baseline_test_max_drawdown_u"].sum()),
        float(merged["pullback_test_max_drawdown_u"].sum()),
        float(merged["strict_test_max_drawdown_u"].sum()),
    ]
    x = np.arange(len(labels))
    width = 0.34
    fig, ax = plt.subplots(figsize=(9, 5.4))
    ax.bar(x - width / 2, pnl_values, width, label="测试收益U", color="#16423C")
    ax.bar(x + width / 2, dd_values, width, label="测试回撤U", color="#C84B31")
    ax.set_xticks(x, labels)
    ax.set_title("三种版本总览")
    ax.legend()
    fig.tight_layout()
    return figure_to_base64(fig)


def render_pnl_chart(merged: pd.DataFrame) -> str:
    fig, ax = plt.subplots(figsize=(10, 5.6))
    x = np.arange(len(merged))
    width = 0.24
    ax.bar(x - width, merged["baseline_test_pnl_u"].astype(float), width, label="推荐斜率版", color="#6A9C89")
    ax.bar(x, merged["pullback_test_pnl_u"].astype(float), width, label="原反抽版", color="#C84B31")
    ax.bar(x + width, merged["strict_test_pnl_u"].astype(float), width, label="严格反抽版", color="#355F2E")
    ax.set_xticks(x, merged["coin"].tolist())
    ax.set_ylabel("PnL (U)")
    ax.set_title("各币测试段收益对比")
    ax.legend()
    fig.tight_layout()
    return figure_to_base64(fig)


def render_equity_chart(trades_frame: pd.DataFrame) -> str:
    if trades_frame.empty:
        fig, ax = plt.subplots(figsize=(8, 4))
        ax.text(0.5, 0.5, "No trades", ha="center", va="center")
        ax.axis("off")
        return figure_to_base64(fig)
    ordered = trades_frame.sort_values(["exit_ts", "entry_ts", "coin"]).reset_index(drop=True).copy()
    ordered["equity"] = ordered["pnl_u"].astype(float).cumsum()
    ordered["dd"] = ordered["equity"].cummax() - ordered["equity"]
    fig, axes = plt.subplots(2, 1, figsize=(10, 7), sharex=True)
    axes[0].plot(ordered["equity"].to_numpy(), color="#355F2E", linewidth=1.5)
    axes[0].set_title("严格反抽版累计收益")
    axes[1].fill_between(np.arange(len(ordered)), ordered["dd"].to_numpy(), color="#C84B31", alpha=0.28)
    axes[1].set_title("严格反抽版回撤")
    axes[1].set_xlabel("Trade Sequence")
    fig.tight_layout()
    return figure_to_base64(fig)


def build_html(*, merged: pd.DataFrame, payload: dict[str, object], compare_chart: str, pnl_chart: str, equity_chart: str) -> str:
    table = dataframe_to_html(
        merged[
            [
                "coin",
                "strategy_label",
                "daily_filter_label",
                "baseline_test_pnl_u",
                "baseline_test_profit_factor",
                "baseline_test_max_drawdown_u",
                "pullback_test_pnl_u",
                "pullback_test_profit_factor",
                "pullback_test_max_drawdown_u",
                "strict_test_pnl_u",
                "strict_test_profit_factor",
                "strict_test_max_drawdown_u",
            ]
        ],
        float_cols={
            "baseline_test_pnl_u": 1,
            "baseline_test_profit_factor": 2,
            "baseline_test_max_drawdown_u": 1,
            "pullback_test_pnl_u": 1,
            "pullback_test_profit_factor": 2,
            "pullback_test_max_drawdown_u": 1,
            "strict_test_pnl_u": 1,
            "strict_test_profit_factor": 2,
            "strict_test_max_drawdown_u": 1,
        },
    )
    cards = "".join(build_coin_card(row) for _, row in merged.iterrows())
    agg = payload["aggregate"]
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>实盘执行卡片 + 严格反抽空头报告</title>
  <style>
    :root {{
      --bg: #f4f1eb; --ink: #1f1f1f; --muted: #61706a; --card: rgba(255,255,255,.84);
      --line: rgba(22,66,60,.12); --accent: #16423c; --accent2: #c84b31; --accent3: #355f2e;
      --radius: 24px; --shadow: 0 18px 42px rgba(22,66,60,.10);
    }}
    * {{ box-sizing: border-box; }}
    body {{ margin:0; font-family:"Microsoft YaHei","PingFang SC",sans-serif; color:var(--ink);
      background: radial-gradient(circle at top left, rgba(53,95,46,.08), transparent 26%), linear-gradient(180deg,#faf7f2 0%,var(--bg) 100%); }}
    .wrap {{ width:min(1200px,calc(100vw - 28px)); margin:0 auto; padding:28px 0 56px; }}
    .hero {{ padding:34px; border-radius:32px; color:#fffdf9; background:linear-gradient(135deg, rgba(22,66,60,.96), rgba(53,95,46,.88)); box-shadow:var(--shadow); }}
    .eyebrow {{ letter-spacing:.16em; text-transform:uppercase; font-size:12px; opacity:.88; }}
    h1,h2,h3,p {{ margin:0; }}
    h1 {{ margin-top:12px; font-size:clamp(28px,4vw,44px); line-height:1.06; }}
    .hero p {{ margin-top:14px; line-height:1.72; max-width:860px; color:rgba(255,253,249,.90); }}
    .grid {{ display:grid; grid-template-columns:repeat(12,1fr); gap:18px; margin-top:20px; }}
    .card {{ background:var(--card); border:1px solid var(--line); border-radius:var(--radius); box-shadow:var(--shadow); padding:22px; }}
    .stat {{ grid-column:span 3; }} .wide {{ grid-column:span 6; }} .full {{ grid-column:1 / -1; }}
    .value {{ margin-top:8px; font-size:32px; font-weight:700; color:var(--accent); }}
    .muted {{ color:var(--muted); }}
    .cards {{ display:grid; grid-template-columns:repeat(2, minmax(0,1fr)); gap:18px; }}
    .coin-card {{ padding:22px; border-radius:24px; border:1px solid var(--line); background:linear-gradient(180deg, rgba(255,255,255,.88), rgba(248,245,239,.90)); }}
    .coin-card h3 {{ font-size:22px; }}
    .pill {{ display:inline-block; margin-top:10px; padding:6px 10px; border-radius:999px; background:rgba(22,66,60,.08); color:var(--accent); font-size:12px; font-weight:700; }}
    ul {{ margin:0; padding-left:18px; line-height:1.8; }}
    img {{ width:100%; border-radius:18px; border:1px solid var(--line); background:#fff; }}
    table {{ width:100%; border-collapse:collapse; font-size:13px; }}
    th,td {{ text-align:left; padding:10px 8px; border-bottom:1px solid rgba(22,66,60,.10); }}
    th {{ color:var(--muted); background:rgba(22,66,60,.03); }}
    @media (max-width: 960px) {{ .stat,.wide,.cards,.full {{ grid-column:1 / -1; }} .cards {{ grid-template-columns:1fr; }} }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <div class="eyebrow">10U 战神 / Execution Cards</div>
      <h1>5 币种实盘执行卡片 + 更严格的反抽不过均线再空</h1>
      <p>这一版把策略说明做成了更适合盯盘的执行卡片，同时把反抽空头规则收紧为“反抽收盘不能收回破位阴线实体 50% 以上”，专门看它能不能把上一版反抽策略里比较弱的币修正回来。</p>
    </section>
    <section class="grid">
      <div class="card stat"><div class="muted">推荐斜率版</div><div class="value">{agg["baseline_pnl"]:.1f}U</div><div class="muted">测试总收益</div></div>
      <div class="card stat"><div class="muted">原反抽版</div><div class="value">{agg["pullback_pnl"]:.1f}U</div><div class="muted">测试总收益</div></div>
      <div class="card stat"><div class="muted">严格反抽版</div><div class="value">{agg["strict_pnl"]:.1f}U</div><div class="muted">测试总收益</div></div>
      <div class="card stat"><div class="muted">严格反抽版</div><div class="value">{agg["strict_dd"]:.1f}U</div><div class="muted">测试总回撤</div></div>

      <div class="card wide">
        <h2>这一版结论</h2>
        <ul>
          <li>严格反抽版比原反抽版更像“等弱反弹确认后再空”，逻辑更漂亮，但这次 5 币总收益仍然明显落后于每币最优斜率版。</li>
          <li>它确实修正了部分噪音，但没有把组合层面的收益结构彻底拉起来，说明“确认更严格”不自动等于“更赚钱”。</li>
          <li>适合把它当二次确认模块，而不是替代主引擎。</li>
        </ul>
      </div>
      <div class="card wide">
        <h2>严格反抽版条件</h2>
        <ul>
          <li>先要有向下破位阴线，且均线斜率继续向下。</li>
          <li>之后 {WATCH_BARS} 根 1H K 内允许反抽，但反抽收盘不能收回破位阴线实体超过 {int(BODY_RECLAIM_MAX_RATIO * 100)}%。</li>
          <li>仍需压在均线下方，且反抽失败那根 K 线收阴，才开空。</li>
          <li>固定风险 {RISK_PER_TRADE_U:.0f}U，退出继续用 2R 保本后逐级锁盈。</li>
        </ul>
      </div>

      <div class="card wide"><h2>总览图</h2><img src="data:image/png;base64,{compare_chart}" alt="总览图"></div>
      <div class="card wide"><h2>各币收益图</h2><img src="data:image/png;base64,{pnl_chart}" alt="各币收益图"></div>
      <div class="card full"><h2>严格反抽版资金曲线</h2><img src="data:image/png;base64,{equity_chart}" alt="资金曲线"></div>

      <div class="card full">
        <h2>实盘执行卡片</h2>
        <div class="cards">{cards}</div>
      </div>

      <div class="card full">
        <h2>三版本逐币对比</h2>
        <div style="overflow:auto;">{table}</div>
      </div>
    </section>
  </div>
</body>
</html>"""


def build_coin_card(row: pd.Series) -> str:
    coin = html.escape(str(row["coin"]))
    strategy = html.escape(str(row["strategy_label"]))
    daily = html.escape(str(row["daily_filter_label"]))
    style_hint = coin_style_hint(str(row["coin"]))
    return (
        f'<div class="coin-card">'
        f"<h3>{coin}</h3>"
        f'<div class="pill">{strategy} / {daily}</div>'
        f'<p class="muted" style="margin-top:12px; line-height:1.7;">{html.escape(style_hint)}</p>'
        f"<ul style='margin-top:10px;'>"
        f"<li>主观察框架：{strategy}</li>"
        f"<li>方向门：{daily}</li>"
        f"<li>基准测试收益：{float(row['baseline_test_pnl_u']):.1f}U，回撤：{float(row['baseline_test_max_drawdown_u']):.1f}U</li>"
        f"<li>原反抽版：{float(row['pullback_test_pnl_u']):.1f}U；严格反抽版：{float(row['strict_test_pnl_u']):.1f}U</li>"
        f"<li>实盘建议：先用斜率版找空头环境，严格反抽版只在你想要更确认时再加。</li>"
        f"</ul>"
        f"</div>"
    )


def coin_style_hint(coin: str) -> str:
    hints = {
        "BTC": "更适合中速到偏快的空头推进，别太慢，容易错过趋势扩张段。",
        "ETH": "更怕来回拉扯，慢过滤更重要，宁可少打也别乱追。",
        "SOL": "波动大，反抽确认逻辑有意义，但不能过度收紧，不然会少掉很多有效单。",
        "DOGE": "慢线过滤价值高，追第一根往往不如等确认。",
        "BNB": "样本相对短，参数可以参考，但仓位管理要比别的币更保守。",
    }
    return hints.get(coin, "优先用主趋势门过滤逆势空。")


def dataframe_to_html(frame: pd.DataFrame, *, float_cols: dict[str, int]) -> str:
    headers = "".join(f"<th>{html.escape(str(col))}</th>" for col in frame.columns)
    body = []
    for _, row in frame.iterrows():
        cells = []
        for col in frame.columns:
            value = row[col]
            text = f"{float(value):.{float_cols[col]}f}" if col in float_cols else str(value)
            cells.append(f"<td>{html.escape(text)}</td>")
        body.append("<tr>" + "".join(cells) + "</tr>")
    return f"<table><thead><tr>{headers}</tr></thead><tbody>{''.join(body)}</tbody></table>"


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
