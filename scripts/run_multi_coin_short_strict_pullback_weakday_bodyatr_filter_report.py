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
SOURCE_STRICT_CSV = REPORT_DIR / "multi_coin_short_strict_pullback_10u_by_coin.csv"

HTML_PATH = REPORT_DIR / "multi_coin_short_strict_pullback_weakday_bodyatr_filter_report.html"
SUMMARY_CSV_PATH = REPORT_DIR / "multi_coin_short_strict_pullback_weakday_bodyatr_filter_summary.csv"
BEST_BY_COIN_CSV_PATH = REPORT_DIR / "multi_coin_short_strict_pullback_weakday_bodyatr_filter_best_by_coin.csv"
TRADES_CSV_PATH = REPORT_DIR / "multi_coin_short_strict_pullback_weakday_bodyatr_filter_trades.csv"
JSON_PATH = REPORT_DIR / "multi_coin_short_strict_pullback_weakday_bodyatr_filter_summary.json"

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
BODY_ATR_LIMITS = (0.8, 1.0, 1.2, 1.5, 2.0)


@dataclass(frozen=True)
class Recommendation:
    symbol: str
    coin: str
    strategy_key: str
    strategy_label: str
    daily_filter_key: str
    daily_filter_label: str


def main() -> None:
    REPORT_DIR.mkdir(exist_ok=True)
    recommendations = load_recommendations()
    strict_reference = pd.read_csv(SOURCE_STRICT_CSV)

    summary_rows: list[dict[str, object]] = []
    best_by_coin_rows: list[dict[str, object]] = []
    best_trades_frames: list[pd.DataFrame] = []

    per_coin_best: dict[str, dict[str, object]] = {}

    for item in recommendations:
        entry_candles = [c for c in load_candle_cache(item.symbol, ENTRY_BAR, limit=None) if c.confirmed]
        daily_candles = [c for c in load_candle_cache(item.symbol, FILTER_BAR, limit=None) if c.confirmed]
        if not entry_candles or not daily_candles:
            continue

        frame = build_entry_frame(entry_candles)
        ma_type, period = parse_strategy_key(item.strategy_key)
        add_indicators(frame, ma_type=ma_type, period=period)
        daily_info = build_daily_info(entry_candles, daily_candles, item.daily_filter_key)

        coin_candidates: list[dict[str, object]] = []
        for body_atr_limit in BODY_ATR_LIMITS:
            trades = simulate_variant(
                frame,
                ma_column=ma_column_name(ma_type, period),
                daily_info=daily_info,
                body_atr_limit=body_atr_limit,
            )
            metrics = compute_metrics(split_test_trades(trades))
            score = metrics["pnl"] - metrics["drawdown"] * 0.30 + metrics["profit_factor"] * 25.0
            row = {
                "symbol": item.symbol,
                "coin": item.coin,
                "strategy_label": item.strategy_label,
                "daily_filter_label": item.daily_filter_label,
                "body_atr_limit": body_atr_limit,
                "test_trades": int(metrics["trades"]),
                "test_pnl_u": metrics["pnl"],
                "test_profit_factor": metrics["profit_factor"],
                "test_max_drawdown_u": metrics["drawdown"],
                "test_win_rate": metrics["win_rate"],
                "score": score,
            }
            summary_rows.append(row)
            coin_candidates.append(row)

        best = sorted(coin_candidates, key=lambda x: (x["score"], x["test_pnl_u"]), reverse=True)[0]
        per_coin_best[item.coin] = best
        best_by_coin_rows.append(best)
        best_trades = simulate_variant(
            frame,
            ma_column=ma_column_name(ma_type, period),
            daily_info=daily_info,
            body_atr_limit=float(best["body_atr_limit"]),
        )
        best_trades["symbol"] = item.symbol
        best_trades["coin"] = item.coin
        best_trades["strategy_label"] = item.strategy_label
        best_trades["daily_filter_label"] = item.daily_filter_label
        best_trades["body_atr_limit"] = float(best["body_atr_limit"])
        best_trades_frames.append(best_trades)

    summary_frame = pd.DataFrame(summary_rows).sort_values(["coin", "body_atr_limit"]).reset_index(drop=True)
    best_by_coin_frame = pd.DataFrame(best_by_coin_rows).sort_values("coin").reset_index(drop=True)
    trades_frame = pd.concat(best_trades_frames, ignore_index=True) if best_trades_frames else pd.DataFrame()
    if trades_frame.empty:
        trades_frame = pd.DataFrame(columns=["entry_index", "exit_index", "entry_ts", "exit_ts", "pnl_u", "r_multiple", "exit_reason", "symbol", "coin", "strategy_label", "daily_filter_label", "body_atr_limit"])
    trades_frame = trades_frame.sort_values(["exit_ts", "entry_ts", "coin"]).reset_index(drop=True)

    summary_frame.to_csv(SUMMARY_CSV_PATH, index=False, encoding="utf-8-sig")
    best_by_coin_frame.to_csv(BEST_BY_COIN_CSV_PATH, index=False, encoding="utf-8-sig")
    trades_frame.to_csv(TRADES_CSV_PATH, index=False, encoding="utf-8-sig")

    merged = best_by_coin_frame.merge(
        strict_reference[["coin", "test_trades", "test_pnl_u", "test_profit_factor", "test_max_drawdown_u"]].rename(
            columns={
                "test_trades": "strict_test_trades",
                "test_pnl_u": "strict_test_pnl_u",
                "test_profit_factor": "strict_test_profit_factor",
                "test_max_drawdown_u": "strict_test_max_drawdown_u",
            }
        ),
        on="coin",
        how="left",
    )

    payload = {
        "created_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "assumptions": {
            "strict_pullback": "反抽收盘不能收回破位阴线实体超过50%",
            "weak_day": "日线收阴，且收盘低于对应日线过滤均线",
            "bodyatr_filter": "若破位触发K线实体/ATR超过阈值，则不追空",
            "bodyatr_limits_tested": list(BODY_ATR_LIMITS),
        },
        "aggregate": {
            "strict_pnl": float(merged["strict_test_pnl_u"].sum()),
            "strict_dd": float(merged["strict_test_max_drawdown_u"].sum()),
            "best_bodyatr_pnl": float(merged["test_pnl_u"].sum()),
            "best_bodyatr_dd": float(merged["test_max_drawdown_u"].sum()),
        },
    }
    JSON_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    summary_chart = render_summary_chart(merged)
    heatmap_chart = render_heatmap(summary_frame)
    equity_chart = render_equity_chart(trades_frame)
    HTML_PATH.write_text(
        build_html(
            merged=merged,
            summary_frame=summary_frame,
            payload=payload,
            summary_chart=summary_chart,
            heatmap_chart=heatmap_chart,
            equity_chart=equity_chart,
        ),
        encoding="utf-8",
    )
    print(HTML_PATH)


def load_recommendations() -> list[Recommendation]:
    frame = pd.read_csv(SOURCE_RECOMMEND_CSV)
    return [
        Recommendation(
            symbol=str(row["symbol"]),
            coin=str(row["coin"]),
            strategy_key=str(row["strategy_key"]),
            strategy_label=str(row["strategy_label"]),
            daily_filter_key=str(row["daily_filter_key"]),
            daily_filter_label=str(row["daily_filter_label"]),
        )
        for row in frame.to_dict("records")
    ]


def build_entry_frame(candles: list[object]) -> pd.DataFrame:
    rows = [
        {
            "ts": int(c.ts),
            "timestamp": pd.to_datetime(int(c.ts), unit="ms", utc=True),
            "open": float(c.open),
            "high": float(c.high),
            "low": float(c.low),
            "close": float(c.close),
        }
        for c in candles
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
    df["body_size"] = (df["close"] - df["open"]).abs()


def rolling_percentile(series: pd.Series, lookback: int) -> pd.Series:
    return series.rolling(lookback, min_periods=lookback).apply(lambda x: float(np.mean(x <= x[-1])), raw=True)


def build_daily_info(entry_candles: list[object], daily_candles: list[object], filter_key: str) -> list[dict[str, object]]:
    daily_frame = pd.DataFrame(
        {
            "ts": [int(c.ts) for c in daily_candles],
            "open": [float(c.open) for c in daily_candles],
            "close": [float(c.close) for c in daily_candles],
        }
    )
    if filter_key == "none":
        line = None
    else:
        ma_type, period = parse_filter_key(filter_key)
        if ma_type == "ema":
            line = daily_frame["close"].ewm(span=period, adjust=False, min_periods=period).mean()
        else:
            line = daily_frame["close"].rolling(period, min_periods=period).mean()
    daily_available_ts = closed_candle_available_timestamps(daily_candles)
    out: list[dict[str, object]] = []
    for candle in entry_candles:
        idx = np.searchsorted(daily_available_ts, int(candle.ts), side="right") - 1
        if idx < 0:
            out.append({"weak_day": False})
            continue
        day_open = float(daily_frame["open"].iloc[idx])
        day_close = float(daily_frame["close"].iloc[idx])
        if line is None:
            weak_day = day_close < day_open
        else:
            line_value = float(line.iloc[idx]) if pd.notna(line.iloc[idx]) else math.nan
            weak_day = np.isfinite(line_value) and day_close < day_open and day_close < line_value
        out.append({"weak_day": bool(weak_day)})
    return out


def simulate_variant(
    frame: pd.DataFrame,
    *,
    ma_column: str,
    daily_info: list[dict[str, object]],
    body_atr_limit: float,
) -> pd.DataFrame:
    open_values = frame["open"].to_numpy(dtype=float)
    high_values = frame["high"].to_numpy(dtype=float)
    low_values = frame["low"].to_numpy(dtype=float)
    close_values = frame["close"].to_numpy(dtype=float)
    ts_values = frame["ts"].to_numpy(dtype=np.int64)
    line_values = frame[ma_column].to_numpy(dtype=float)
    atr_values = frame["atr14"].to_numpy(dtype=float)
    atr_pct_values = frame["atr_pct"].to_numpy(dtype=float)
    body_values = frame["body_size"].to_numpy(dtype=float)

    trades: list[dict[str, object]] = []
    position: dict[str, float | int | str] | None = None
    pending: dict[str, float | int] | None = None
    start_index = max(ATR_PERCENTILE_LOOKBACK, 60)

    for index in range(start_index, len(frame)):
        line_value = line_values[index]
        prev_line = line_values[index - 1]
        atr_value = atr_values[index]
        atr_pct = atr_pct_values[index]
        candle_open = open_values[index]
        candle_high = high_values[index]
        candle_low = low_values[index]
        candle_close = close_values[index]
        body_size = body_values[index]
        if any(math.isnan(v) for v in [line_value, prev_line, atr_value, atr_pct, body_size]):
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
                weak_day_ok = bool(daily_info[index]["weak_day"])
                if near_line and still_below and bearish_close and midpoint_ok and weak_day_ok:
                    risk_per_unit = max((candle_high + STOP_BUFFER_ATR_MULT * atr_value) - candle_close, atr_value * 0.5)
                    if risk_per_unit > 0 and np.isfinite(risk_per_unit):
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
        if not bool(daily_info[index]["weak_day"]):
            continue
        if atr_value <= 0:
            continue
        body_atr_ratio = body_size / atr_value
        if body_atr_ratio > body_atr_limit:
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


def render_summary_chart(merged: pd.DataFrame) -> str:
    labels = ["严格反抽版", "Body/ATR最佳版"]
    pnl_values = [float(merged["strict_test_pnl_u"].sum()), float(merged["test_pnl_u"].sum())]
    dd_values = [float(merged["strict_test_max_drawdown_u"].sum()), float(merged["test_max_drawdown_u"].sum())]
    x = np.arange(len(labels))
    width = 0.34
    fig, ax = plt.subplots(figsize=(8.8, 5.2))
    ax.bar(x - width / 2, pnl_values, width, label="测试收益U", color="#16423C")
    ax.bar(x + width / 2, dd_values, width, label="测试回撤U", color="#C84B31")
    ax.set_xticks(x, labels)
    ax.set_title("严格反抽版 vs 实体/ATR过滤最佳版")
    ax.legend()
    fig.tight_layout()
    return figure_to_base64(fig)


def render_heatmap(summary_frame: pd.DataFrame) -> str:
    pivot = summary_frame.pivot(index="coin", columns="body_atr_limit", values="test_pnl_u").reindex(columns=BODY_ATR_LIMITS)
    fig, ax = plt.subplots(figsize=(9.2, 4.8))
    im = ax.imshow(pivot.to_numpy(), cmap="RdYlGn")
    ax.set_xticks(np.arange(len(pivot.columns)), [str(col) for col in pivot.columns])
    ax.set_yticks(np.arange(len(pivot.index)), pivot.index.tolist())
    ax.set_title("不同实体/ATR阈值的测试收益热力图")
    for i in range(len(pivot.index)):
        for j in range(len(pivot.columns)):
            value = pivot.iloc[i, j]
            ax.text(j, i, f"{value:.0f}", ha="center", va="center", color="black", fontsize=9)
    fig.colorbar(im, ax=ax, shrink=0.82)
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
    axes[0].set_title("实体/ATR过滤最佳版累计收益")
    axes[1].fill_between(np.arange(len(ordered)), ordered["dd"].to_numpy(), color="#C84B31", alpha=0.28)
    axes[1].set_title("实体/ATR过滤最佳版回撤")
    axes[1].set_xlabel("Trade Sequence")
    fig.tight_layout()
    return figure_to_base64(fig)


def build_html(
    *,
    merged: pd.DataFrame,
    summary_frame: pd.DataFrame,
    payload: dict[str, object],
    summary_chart: str,
    heatmap_chart: str,
    equity_chart: str,
) -> str:
    agg = payload["aggregate"]
    table = dataframe_to_html(
        merged[
            [
                "coin",
                "strategy_label",
                "daily_filter_label",
                "body_atr_limit",
                "strict_test_pnl_u",
                "strict_test_max_drawdown_u",
                "test_pnl_u",
                "test_max_drawdown_u",
                "test_trades",
                "test_profit_factor",
            ]
        ],
        float_cols={
            "body_atr_limit": 1,
            "strict_test_pnl_u": 1,
            "strict_test_max_drawdown_u": 1,
            "test_pnl_u": 1,
            "test_max_drawdown_u": 1,
            "test_profit_factor": 2,
        },
    )
    assumption_lines = "".join(f"<li>{html.escape(str(v))}</li>" for v in payload["assumptions"].values())
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>反抽50% + 日线弱势日 + 实体ATR不过大不追空</title>
  <style>
    :root {{ --bg:#f5f2ec; --ink:#1f1f1f; --muted:#5f6f68; --card:rgba(255,255,255,.84); --line:rgba(22,66,60,.12); --a:#16423c; --b:#355f2e; --c:#c84b31; --r:24px; --s:0 18px 42px rgba(22,66,60,.10); }}
    * {{ box-sizing:border-box; }} body {{ margin:0; font-family:"Microsoft YaHei","PingFang SC",sans-serif; color:var(--ink); background:linear-gradient(180deg,#faf7f2 0%,var(--bg) 100%); }}
    .wrap {{ width:min(1180px,calc(100vw - 28px)); margin:0 auto; padding:28px 0 56px; }}
    .hero {{ padding:34px; border-radius:32px; color:#fffdf9; background:linear-gradient(135deg, rgba(22,66,60,.96), rgba(53,95,46,.88)); box-shadow:var(--s); }}
    .eyebrow {{ letter-spacing:.16em; text-transform:uppercase; font-size:12px; opacity:.88; }} h1,h2,p {{ margin:0; }} h1 {{ margin-top:12px; font-size:clamp(28px,4vw,44px); line-height:1.06; }} .hero p {{ margin-top:14px; line-height:1.72; max-width:860px; color:rgba(255,253,249,.90); }}
    .grid {{ display:grid; grid-template-columns:repeat(12,1fr); gap:18px; margin-top:20px; }} .card {{ background:var(--card); border:1px solid var(--line); border-radius:var(--r); box-shadow:var(--s); padding:22px; }}
    .stat {{ grid-column:span 3; }} .wide {{ grid-column:span 6; }} .full {{ grid-column:1 / -1; }} .value {{ margin-top:8px; font-size:32px; font-weight:700; color:var(--a); }} .muted {{ color:var(--muted); }}
    ul {{ margin:0; padding-left:18px; line-height:1.8; }} img {{ width:100%; border-radius:18px; border:1px solid var(--line); background:#fff; }}
    table {{ width:100%; border-collapse:collapse; font-size:13px; }} th,td {{ text-align:left; padding:10px 8px; border-bottom:1px solid rgba(22,66,60,.10); }} th {{ color:var(--muted); background:rgba(22,66,60,.03); }}
    @media (max-width: 960px) {{ .stat,.wide {{ grid-column:1 / -1; }} }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <div class="eyebrow">10U 战神 / Body ATR Filter</div>
      <h1>反抽不收回 50% + 只做日线弱势日 + 实体/ATR 过大不追空</h1>
      <p>这版不再按固定时段切掉信号，而是只在“破位 K 线已经太极端”时避免追空。它更接近真实交易的直觉：不是哪个小时不能做，而是涨跌幅已经过头时别追。</p>
    </section>
    <section class="grid">
      <div class="card stat"><div class="muted">严格反抽版</div><div class="value">{agg["strict_pnl"]:.1f}U</div><div class="muted">测试总收益</div></div>
      <div class="card stat"><div class="muted">Body/ATR最佳版</div><div class="value">{agg["best_bodyatr_pnl"]:.1f}U</div><div class="muted">测试总收益</div></div>
      <div class="card stat"><div class="muted">严格反抽版</div><div class="value">{agg["strict_dd"]:.1f}U</div><div class="muted">测试总回撤</div></div>
      <div class="card stat"><div class="muted">Body/ATR最佳版</div><div class="value">{agg["best_bodyatr_dd"]:.1f}U</div><div class="muted">测试总回撤</div></div>

      <div class="card wide">
        <h2>测试假设</h2>
        <ul>{assumption_lines}</ul>
      </div>
      <div class="card wide">
        <h2>我关注的重点</h2>
        <ul>
          <li>这版的目标不是盲目降频，而是避免在“已经走得太猛的那根 K”上追空。</li>
          <li>如果它有效，通常会比固定小时过滤更自然，因为不同币的活跃小时并不一定是坏小时。</li>
          <li>重点看 ETH / SOL 是否被修复，同时 DOGE / BTC 是否不被误杀。</li>
        </ul>
      </div>

      <div class="card wide"><h2>总览图</h2><img src="data:image/png;base64,{summary_chart}" alt="summary"></div>
      <div class="card wide"><h2>阈值热力图</h2><img src="data:image/png;base64,{heatmap_chart}" alt="heatmap"></div>
      <div class="card full"><h2>资金曲线</h2><img src="data:image/png;base64,{equity_chart}" alt="equity"></div>
      <div class="card full"><h2>每个币的最佳阈值</h2><div style="overflow:auto;">{table}</div></div>
    </section>
  </div>
</body>
</html>"""


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


if __name__ == "__main__":
    main()
