from __future__ import annotations

import base64
import html
import io
import json
import math
import sys
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
ENTRY_BARS = ("1H", "4H")
FILTER_BAR = "1D"
SYMBOLS = ("BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP", "BNB-USDT-SWAP", "DOGE-USDT-SWAP")
COIN_LABELS = {symbol: symbol.split("-")[0] for symbol in SYMBOLS}

RISK_PER_TRADE_U = 10.0
TAKER_FEE_RATE = 0.00036
ATR_PERIOD = 14
ATR_PERCENTILE_LOOKBACK = 100
ATR_PERCENTILE_MAX = 0.50
SLOPE_THRESHOLD_RATIO = -0.0005
ATR_STOP_MULTIPLIER = 2.0
INITIAL_CAPITAL = 10_000.0

HTML_PATH = REPORT_DIR / "multi_coin_4h_short_slope_vs_1h_report.html"
SUMMARY_CSV = REPORT_DIR / "multi_coin_4h_short_slope_vs_1h_summary.csv"
BY_COIN_CSV = REPORT_DIR / "multi_coin_4h_short_slope_vs_1h_by_coin.csv"
JSON_PATH = REPORT_DIR / "multi_coin_4h_short_slope_vs_1h_summary.json"


STRATEGY_VARIANTS = (
    ("ema55", "EMA55 斜率空", "ema", 55),
    ("ma55", "MA55 斜率空", "ma", 55),
    ("ema21", "EMA21 斜率空", "ema", 21),
    ("ma20", "MA20 斜率空", "ma", 20),
    ("ema34", "EMA34 斜率空", "ema", 34),
)

DAILY_FILTERS = (
    ("none", "无日线过滤", None, None),
    ("ema21", "日线 EMA21 过滤", "ema", 21),
    ("ema55", "日线 EMA55 过滤", "ema", 55),
    ("ma20", "日线 MA20 过滤", "ma", 20),
)


def main() -> None:
    REPORT_DIR.mkdir(exist_ok=True)
    summary_rows: list[dict[str, object]] = []
    coin_rows: list[dict[str, object]] = []
    data_ranges: dict[str, dict[str, object]] = {}

    for entry_bar in ENTRY_BARS:
        for symbol in SYMBOLS:
            entry_candles = [c for c in load_candle_cache(symbol, entry_bar, limit=None) if c.confirmed]
            daily_candles = [c for c in load_candle_cache(symbol, FILTER_BAR, limit=None) if c.confirmed]
            if not entry_candles or not daily_candles:
                continue
            frame = build_entry_frame(entry_candles)
            add_indicators(frame)
            bias_map = build_daily_bias_map(entry_candles, daily_candles)
            data_ranges[f"{symbol}_{entry_bar}"] = {
                "entry_candles": len(frame),
                "daily_candles": len(daily_candles),
                "start_utc": format_ts(int(frame["ts"].iloc[0])),
                "end_utc": format_ts(int(frame["ts"].iloc[-1])),
            }

            for strategy_key, strategy_label, ma_type, period in STRATEGY_VARIANTS:
                ma_col = ma_column_name(ma_type, period)
                for filter_key, filter_label, _, _ in DAILY_FILTERS:
                    trades = simulate_short_trades(frame, bias=bias_map[filter_key], ma_column=ma_col)
                    all_metrics = compute_metrics(trades)
                    test_metrics = compute_metrics(split_test_trades(trades))
                    row = {
                        "entry_bar": entry_bar,
                        "symbol": symbol,
                        "coin": COIN_LABELS[symbol],
                        "strategy_key": strategy_key,
                        "strategy_label": strategy_label,
                        "daily_filter_key": filter_key,
                        "daily_filter_label": filter_label,
                        "all_trades": int(all_metrics["trades"]),
                        "all_pnl_u": all_metrics["pnl"],
                        "all_profit_factor": all_metrics["profit_factor"],
                        "all_max_drawdown_u": all_metrics["drawdown"],
                        "all_win_rate": all_metrics["win_rate"],
                        "test_trades": int(test_metrics["trades"]),
                        "test_pnl_u": test_metrics["pnl"],
                        "test_profit_factor": test_metrics["profit_factor"],
                        "test_max_drawdown_u": test_metrics["drawdown"],
                        "test_win_rate": test_metrics["win_rate"],
                    }
                    coin_rows.append(row)

        bar_frame = pd.DataFrame([row for row in coin_rows if row["entry_bar"] == entry_bar])
        for (strategy_key, strategy_label, daily_filter_key, daily_filter_label), group in bar_frame.groupby(
            ["strategy_key", "strategy_label", "daily_filter_key", "daily_filter_label"],
            sort=False,
        ):
            all_metrics = {
                "all_pnl_u": float(group["all_pnl_u"].sum()),
                "all_trades": int(group["all_trades"].sum()),
                "all_profit_factor": weighted_pf(group["all_pnl_u"], group["all_max_drawdown_u"], group["all_profit_factor"]),
                "all_max_drawdown_u": float(group["all_max_drawdown_u"].sum()),
                "test_pnl_u": float(group["test_pnl_u"].sum()),
                "test_trades": int(group["test_trades"].sum()),
                "test_profit_factor": weighted_pf(group["test_pnl_u"], group["test_max_drawdown_u"], group["test_profit_factor"]),
                "test_max_drawdown_u": float(group["test_max_drawdown_u"].sum()),
            }
            all_metrics["score"] = all_metrics["test_pnl_u"] - all_metrics["test_max_drawdown_u"] * 0.35 + all_metrics["test_profit_factor"] * 40.0
            summary_rows.append(
                {
                    "entry_bar": entry_bar,
                    "strategy_key": strategy_key,
                    "strategy_label": strategy_label,
                    "daily_filter_key": daily_filter_key,
                    "daily_filter_label": daily_filter_label,
                    **all_metrics,
                }
            )

    summary_frame = pd.DataFrame(summary_rows).sort_values(["entry_bar", "score", "test_pnl_u"], ascending=[True, False, False]).reset_index(drop=True)
    coin_frame = pd.DataFrame(coin_rows).sort_values(["entry_bar", "coin", "all_pnl_u"], ascending=[True, True, False]).reset_index(drop=True)
    summary_frame.to_csv(SUMMARY_CSV, index=False, encoding="utf-8-sig")
    coin_frame.to_csv(BY_COIN_CSV, index=False, encoding="utf-8-sig")

    best_1h = summary_frame[summary_frame["entry_bar"] == "1H"].iloc[0].to_dict()
    best_4h = summary_frame[summary_frame["entry_bar"] == "4H"].iloc[0].to_dict()
    payload = {
        "created_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "assumptions": {
            "risk_per_trade_u": RISK_PER_TRADE_U,
            "taker_fee_rate": TAKER_FEE_RATE,
            "atr_period": ATR_PERIOD,
            "atr_percentile_max": ATR_PERCENTILE_MAX,
            "slope_threshold_ratio": SLOPE_THRESHOLD_RATIO,
            "entry_model": "斜率向下 + 收盘在均线下 + ATR过滤",
            "exit_model": "2R保本后逐级锁盈",
        },
        "data_ranges": data_ranges,
        "best_1h": best_1h,
        "best_4h": best_4h,
    }
    JSON_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    summary_chart = render_summary_chart(summary_frame)
    top_chart = render_top_chart(summary_frame)
    coin_chart = render_coin_chart(coin_frame)
    HTML_PATH.write_text(build_html(summary_frame, coin_frame, payload, summary_chart, top_chart, coin_chart), encoding="utf-8")
    print(HTML_PATH)


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


def add_indicators(df: pd.DataFrame) -> None:
    for _, _, ma_type, period in STRATEGY_VARIANTS:
        col = ma_column_name(ma_type, period)
        if ma_type == "ema":
            df[col] = df["close"].ewm(span=period, adjust=False, min_periods=period).mean()
        else:
            df[col] = df["close"].rolling(period, min_periods=period).mean()
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


def ma_column_name(ma_type: str, period: int) -> str:
    return f"{ma_type}{period}"


def rolling_percentile(series: pd.Series, lookback: int) -> pd.Series:
    return series.rolling(lookback, min_periods=lookback).apply(lambda x: float(np.mean(x <= x[-1])), raw=True)


def build_daily_bias_map(entry_candles: list[object], daily_candles: list[object]) -> dict[str, list[str] | None]:
    daily_frame = pd.DataFrame({"ts": [int(c.ts) for c in daily_candles], "close": [float(c.close) for c in daily_candles]})
    daily_available_ts = closed_candle_available_timestamps(daily_candles)
    out: dict[str, list[str] | None] = {"none": None}
    for key, _, ma_type, period in DAILY_FILTERS:
        if period is None or ma_type is None:
            continue
        if ma_type == "ema":
            line = daily_frame["close"].ewm(span=period, adjust=False, min_periods=period).mean()
        else:
            line = daily_frame["close"].rolling(period, min_periods=period).mean()
        bias = []
        for candle in entry_candles:
            idx = np.searchsorted(daily_available_ts, int(candle.ts), side="right") - 1
            if idx < 0:
                bias.append("neutral")
                continue
            line_value = float(line.iloc[idx]) if pd.notna(line.iloc[idx]) else math.nan
            day_close = float(daily_frame["close"].iloc[idx])
            if not np.isfinite(line_value):
                bias.append("neutral")
            elif day_close < line_value:
                bias.append("short")
            elif day_close > line_value:
                bias.append("long")
            else:
                bias.append("neutral")
        out[key] = bias
    return out


def simulate_short_trades(df: pd.DataFrame, *, bias: list[str] | None, ma_column: str) -> pd.DataFrame:
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
    start_index = max(ATR_PERCENTILE_LOOKBACK, 60)

    for index in range(start_index, len(df)):
        line_value = line_values[index]
        prev_line = line_values[index - 1]
        atr_value = atr_values[index]
        atr_pct = atr_pct_values[index]
        close_price = close_values[index]
        if any(math.isnan(v) for v in [line_value, prev_line, atr_value, atr_pct]):
            continue
        slope_ratio = (line_value - prev_line) / line_value if line_value else math.nan

        if position is not None:
            exited = process_open_short(position, open_values[index], high_values[index], low_values[index], close_values[index], int(ts_values[index]), index, trades)
            if exited:
                position = None
        if position is not None:
            continue
        if not np.isfinite(slope_ratio) or slope_ratio > SLOPE_THRESHOLD_RATIO:
            continue
        if close_price >= line_value or atr_pct > ATR_PERCENTILE_MAX:
            continue
        if bias is not None and index < len(bias) and bias[index] != "short":
            continue
        risk_per_unit = atr_value * ATR_STOP_MULTIPLIER
        if risk_per_unit <= 0 or not np.isfinite(risk_per_unit):
            continue
        fee_offset = close_price * TAKER_FEE_RATE * 2.0
        position = {
            "entry_index": index,
            "entry_ts": int(ts_values[index]),
            "entry_price": close_price,
            "risk_per_unit": risk_per_unit,
            "stop": close_price + risk_per_unit,
            "stop_reason": "stop_loss",
            "fee_offset": fee_offset,
            "next_dynamic_r": 2.0,
        }
    return pd.DataFrame(trades)


def process_open_short(position, candle_open, candle_high, candle_low, candle_close, candle_ts, index, trades) -> bool:
    path = (candle_open, candle_low, candle_high, candle_close) if candle_close >= candle_open else (candle_open, candle_high, candle_low, candle_close)
    for start, end in zip(path, path[1:]):
        if end > start:
            stop_price = float(position["stop"])
            if start <= stop_price <= end:
                trades.append(close_trade(position, index, candle_ts, stop_price, str(position["stop_reason"])))
                return True
        else:
            advance_step_dynamic(position, end)
    return False


def advance_step_dynamic(position, favorable_price: float) -> None:
    entry = float(position["entry_price"])
    risk = float(position["risk_per_unit"])
    fee_offset = float(position["fee_offset"])
    while True:
        next_r = float(position["next_dynamic_r"])
        trigger = entry - risk * next_r - fee_offset
        if favorable_price > trigger:
            break
        locked_r = 0.0 if math.isclose(next_r, 2.0) else max(next_r - 1.0, 0.0)
        reason = "break_even_stop" if math.isclose(next_r, 2.0) else f"locked_{int(round(locked_r))}r_stop"
        candidate_stop = entry - risk * locked_r - fee_offset
        if candidate_stop < float(position["stop"]):
            position["stop"] = candidate_stop
            position["stop_reason"] = reason
        position["next_dynamic_r"] = next_r + 1.0


def close_trade(position, exit_index: int, exit_ts: int, exit_price: float, exit_reason: str) -> dict[str, object]:
    entry = float(position["entry_price"])
    risk = float(position["risk_per_unit"])
    quantity = RISK_PER_TRADE_U / risk if risk > 0 else 0.0
    pnl_per_unit = (entry - exit_price) - TAKER_FEE_RATE * (entry + exit_price)
    pnl_u = pnl_per_unit * quantity
    return {"entry_index": int(position["entry_index"]), "exit_index": exit_index, "entry_ts": int(position["entry_ts"]), "exit_ts": exit_ts, "pnl_u": pnl_u}


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


def weighted_pf(pnl_series: pd.Series, dd_series: pd.Series, pf_series: pd.Series) -> float:
    pnl_weights = pnl_series.abs().astype(float).to_numpy()
    dd_weights = dd_series.astype(float).to_numpy()
    weights = np.where(pnl_weights > 0, pnl_weights, np.where(dd_weights > 0, dd_weights, 1.0))
    values = pf_series.astype(float)
    if float(weights.sum()) <= 0:
        return float(values.mean()) if len(values) else 0.0
    return float((values.to_numpy() * weights).sum() / weights.sum())


def render_summary_chart(summary_frame: pd.DataFrame) -> str:
    grouped = summary_frame.groupby("entry_bar", as_index=False).first()[["entry_bar", "test_pnl_u", "test_max_drawdown_u"]]
    fig, ax = plt.subplots(figsize=(8.4, 4.8))
    x = np.arange(len(grouped))
    width = 0.34
    ax.bar(x - width / 2, grouped["test_pnl_u"], width, label="最佳测试收益U", color="#16423C")
    ax.bar(x + width / 2, grouped["test_max_drawdown_u"], width, label="最佳测试回撤U", color="#C84B31")
    ax.set_xticks(x, grouped["entry_bar"].tolist())
    ax.set_title("1H vs 4H 最佳斜率空")
    ax.legend()
    fig.tight_layout()
    return figure_to_base64(fig)


def render_top_chart(summary_frame: pd.DataFrame) -> str:
    top = summary_frame.groupby("entry_bar", group_keys=False).head(5).copy()
    top["label"] = top["entry_bar"] + " | " + top["strategy_label"] + " | " + top["daily_filter_label"]
    fig, ax = plt.subplots(figsize=(10, 6.2))
    ax.barh(top["label"].iloc[::-1], top["test_pnl_u"].iloc[::-1], color="#6A9C89")
    ax.set_title("各周期 Top 5 组合测试收益")
    ax.set_xlabel("PnL (U)")
    fig.tight_layout()
    return figure_to_base64(fig)


def render_coin_chart(coin_frame: pd.DataFrame) -> str:
    best = coin_frame.sort_values(["entry_bar", "coin", "test_pnl_u"], ascending=[True, True, False]).groupby(["entry_bar", "coin"], as_index=False).first()
    pivot = best.pivot(index="coin", columns="entry_bar", values="test_pnl_u").fillna(0.0)
    fig, ax = plt.subplots(figsize=(9.5, 5.4))
    x = np.arange(len(pivot.index))
    width = 0.34
    ax.bar(x - width / 2, pivot.get("1H", pd.Series([0]*len(x))).to_numpy(), width, label="1H", color="#C84B31")
    ax.bar(x + width / 2, pivot.get("4H", pd.Series([0]*len(x))).to_numpy(), width, label="4H", color="#355F2E")
    ax.set_xticks(x, pivot.index.tolist())
    ax.set_title("每个币最佳测试收益：1H vs 4H")
    ax.set_ylabel("PnL (U)")
    ax.legend()
    fig.tight_layout()
    return figure_to_base64(fig)


def build_html(summary_frame, coin_frame, payload, summary_chart, top_chart, coin_chart) -> str:
    best_1h = payload["best_1h"]
    best_4h = payload["best_4h"]
    delta = float(best_4h["test_pnl_u"]) - float(best_1h["test_pnl_u"])
    top_table = dataframe_to_html(
        summary_frame[
            ["entry_bar", "strategy_label", "daily_filter_label", "all_pnl_u", "all_profit_factor", "test_pnl_u", "test_profit_factor", "test_max_drawdown_u"]
        ].groupby("entry_bar", group_keys=False).head(8),
        {
            "all_pnl_u": 1,
            "all_profit_factor": 2,
            "test_pnl_u": 1,
            "test_profit_factor": 2,
            "test_max_drawdown_u": 1,
        },
    )
    return f"""<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>4H 斜率空 vs 1H 斜率空</title><style>
    :root{{--bg:#f5f2ec;--ink:#1f1f1f;--muted:#5f6f68;--card:rgba(255,255,255,.84);--line:rgba(22,66,60,.12);--a:#16423c;--b:#355f2e;--c:#c84b31;--r:24px;--s:0 18px 42px rgba(22,66,60,.10);}}
    *{{box-sizing:border-box}} body{{margin:0;font-family:"Microsoft YaHei","PingFang SC",sans-serif;color:var(--ink);background:linear-gradient(180deg,#faf7f2 0%,var(--bg) 100%)}} .wrap{{width:min(1180px,calc(100vw - 28px));margin:0 auto;padding:28px 0 56px}} .hero{{padding:34px;border-radius:32px;color:#fffdf9;background:linear-gradient(135deg, rgba(22,66,60,.96), rgba(53,95,46,.88));box-shadow:var(--s)}} .eyebrow{{letter-spacing:.16em;text-transform:uppercase;font-size:12px;opacity:.88}} h1,h2,p{{margin:0}} h1{{margin-top:12px;font-size:clamp(28px,4vw,44px);line-height:1.06}} .hero p{{margin-top:14px;line-height:1.72;max-width:860px;color:rgba(255,253,249,.90)}} .grid{{display:grid;grid-template-columns:repeat(12,1fr);gap:18px;margin-top:20px}} .card{{background:var(--card);border:1px solid var(--line);border-radius:var(--r);box-shadow:var(--s);padding:22px}} .stat{{grid-column:span 3}} .wide{{grid-column:span 6}} .full{{grid-column:1 / -1}} .value{{margin-top:8px;font-size:32px;font-weight:700;color:var(--a)}} .muted{{color:var(--muted)}} ul{{margin:0;padding-left:18px;line-height:1.8}} img{{width:100%;border-radius:18px;border:1px solid var(--line);background:#fff}} table{{width:100%;border-collapse:collapse;font-size:13px}} th,td{{text-align:left;padding:10px 8px;border-bottom:1px solid rgba(22,66,60,.10)}} th{{color:var(--muted);background:rgba(22,66,60,.03)}} @media (max-width:960px){{.stat,.wide{{grid-column:1 / -1}}}}
    </style></head><body><div class="wrap"><section class="hero"><div class="eyebrow">1H vs 4H Short Slope</div><h1>4小时斜率做空，按 1 小时思路复刻，结果怎么样？</h1><p>这份报告用和 1H 相同的核心口径来跑 4H：固定风险 10U、同样的斜率阈值、同样的日线过滤池，同样的动态锁盈。重点看 4H 能不能带来更稳定或更赚钱的结果。</p></section>
    <section class="grid">
      <div class="card stat"><div class="muted">1H 最佳组合</div><div class="value">{html.escape(best_1h['strategy_label'])}</div><div class="muted">{html.escape(best_1h['daily_filter_label'])}</div></div>
      <div class="card stat"><div class="muted">1H 测试收益</div><div class="value">{float(best_1h['test_pnl_u']):.1f}U</div><div class="muted">回撤 {float(best_1h['test_max_drawdown_u']):.1f}U</div></div>
      <div class="card stat"><div class="muted">4H 最佳组合</div><div class="value">{html.escape(best_4h['strategy_label'])}</div><div class="muted">{html.escape(best_4h['daily_filter_label'])}</div></div>
      <div class="card stat"><div class="muted">4H 相对 1H</div><div class="value">{delta:+.1f}U</div><div class="muted">按最佳测试收益比较</div></div>
      <div class="card wide"><h2>我的预期判断</h2><ul><li>4H 通常会更少交易、更慢，但也可能更稳。</li><li>如果 4H 赚钱但明显少于 1H，它更像辅助周期，不一定值得替代 1H。</li><li>如果 4H 某些币明显优于 1H，那适合做分币种分周期部署。</li></ul></div>
      <div class="card wide"><h2>研究口径</h2><ul><li>固定风险 10U / 笔</li><li>斜率阈值：{payload['assumptions']['slope_threshold_ratio']}</li><li>ATR过滤上限：{payload['assumptions']['atr_percentile_max']}</li><li>退出：{html.escape(payload['assumptions']['exit_model'])}</li></ul></div>
      <div class="card wide"><h2>最佳对比</h2><img src="data:image/png;base64,{summary_chart}" alt="summary"></div>
      <div class="card wide"><h2>Top 组合</h2><img src="data:image/png;base64,{top_chart}" alt="top"></div>
      <div class="card full"><h2>分币最佳结果</h2><img src="data:image/png;base64,{coin_chart}" alt="coin"></div>
      <div class="card full"><h2>排行表</h2><div style="overflow:auto;">{top_table}</div></div>
    </section></div></body></html>"""


def dataframe_to_html(frame: pd.DataFrame, float_cols: dict[str, int]) -> str:
    headers = "".join(f"<th>{html.escape(str(col))}</th>" for col in frame.columns)
    rows = []
    for _, row in frame.iterrows():
        cells = []
        for col in frame.columns:
            value = row[col]
            text = f"{float(value):.{float_cols[col]}f}" if col in float_cols else str(value)
            cells.append(f"<td>{html.escape(text)}</td>")
        rows.append("<tr>" + "".join(cells) + "</tr>")
    return f"<table><thead><tr>{headers}</tr></thead><tbody>{''.join(rows)}</tbody></table>"


def figure_to_base64(fig: plt.Figure) -> str:
    buffer = io.BytesIO()
    fig.savefig(buffer, format="png", dpi=160, bbox_inches="tight")
    plt.close(fig)
    return base64.b64encode(buffer.getvalue()).decode("ascii")


def format_ts(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


if __name__ == "__main__":
    main()
