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
HTML_PATH = REPORT_DIR / "bnb_sol_bodyatr_robustness_report.html"
SUMMARY_CSV = REPORT_DIR / "bnb_sol_bodyatr_robustness_summary.csv"
WALK_CSV = REPORT_DIR / "bnb_sol_bodyatr_robustness_walkforward.csv"
JSON_PATH = REPORT_DIR / "bnb_sol_bodyatr_robustness_summary.json"

ENTRY_BAR = "1H"
FILTER_BAR = "1D"
COMMON_START_TS = 1671775200000  # 2022-12-23 06:00:00 UTC
BODY_ATR_LIMITS = (0.8, 1.0, 1.2, 1.5, 2.0)
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
class CoinConfig:
    symbol: str
    coin: str
    ma_type: str
    ma_period: int
    daily_filter_key: str
    daily_filter_label: str


COINS = (
    CoinConfig("BNB-USDT-SWAP", "BNB", "ma", 20, "none", "无日线过滤"),
    CoinConfig("SOL-USDT-SWAP", "SOL", "ma", 20, "ema21", "日线 EMA21 过滤"),
)


def main() -> None:
    REPORT_DIR.mkdir(exist_ok=True)
    summary_rows: list[dict[str, object]] = []
    walk_rows: list[dict[str, object]] = []

    for coin in COINS:
        entry_candles = [c for c in load_candle_cache(coin.symbol, ENTRY_BAR, limit=None) if c.confirmed]
        daily_candles = [c for c in load_candle_cache(coin.symbol, FILTER_BAR, limit=None) if c.confirmed]
        full_frame = build_entry_frame(entry_candles)
        full_frame = full_frame[full_frame["ts"] >= COMMON_START_TS].reset_index(drop=True)
        add_indicators(full_frame, ma_type=coin.ma_type, period=coin.ma_period)
        daily_info = build_daily_info(
            entry_candles=[type("X", (), {"ts": int(ts)}) for ts in full_frame["ts"].tolist()],
            daily_candles=daily_candles,
            filter_key=coin.daily_filter_key,
        )
        for limit in BODY_ATR_LIMITS:
            trades = simulate_variant(full_frame, ma_column=ma_column_name(coin.ma_type, coin.ma_period), daily_info=daily_info, body_atr_limit=limit)
            full_metrics = compute_metrics(trades)
            half_index = len(full_frame) // 2
            train_trades = trades[trades["exit_index"] < half_index].copy()
            test_trades = trades[trades["exit_index"] >= half_index].copy()
            train_metrics = compute_metrics(train_trades)
            test_metrics = compute_metrics(test_trades)
            summary_rows.append(
                {
                    "coin": coin.coin,
                    "body_atr_limit": limit,
                    "common_all_pnl_u": full_metrics["pnl"],
                    "common_all_pf": full_metrics["profit_factor"],
                    "common_all_dd_u": full_metrics["drawdown"],
                    "common_all_trades": int(full_metrics["trades"]),
                    "train_pnl_u": train_metrics["pnl"],
                    "train_pf": train_metrics["profit_factor"],
                    "train_dd_u": train_metrics["drawdown"],
                    "train_trades": int(train_metrics["trades"]),
                    "test_pnl_u": test_metrics["pnl"],
                    "test_pf": test_metrics["profit_factor"],
                    "test_dd_u": test_metrics["drawdown"],
                    "test_trades": int(test_metrics["trades"]),
                    "robust_score": test_metrics["pnl"] - test_metrics["drawdown"] * 0.30 + test_metrics["profit_factor"] * 25.0,
                }
            )

        coin_summary = pd.DataFrame([row for row in summary_rows if row["coin"] == coin.coin])
        best_train = coin_summary.sort_values(["train_pnl_u", "train_pf"], ascending=[False, False]).iloc[0]
        chosen_limit = float(best_train["body_atr_limit"])
        trades = simulate_variant(full_frame, ma_column=ma_column_name(coin.ma_type, coin.ma_period), daily_info=daily_info, body_atr_limit=chosen_limit)
        half_index = len(full_frame) // 2
        walk_rows.append(
            {
                "coin": coin.coin,
                "selected_by_train_limit": chosen_limit,
                "train_pnl_u": float(best_train["train_pnl_u"]),
                "train_pf": float(best_train["train_pf"]),
                "test_pnl_u": float(compute_metrics(trades[trades["exit_index"] >= half_index].copy())["pnl"]),
                "test_pf": float(compute_metrics(trades[trades["exit_index"] >= half_index].copy())["profit_factor"]),
                "test_dd_u": float(compute_metrics(trades[trades["exit_index"] >= half_index].copy())["drawdown"]),
            }
        )

    summary_frame = pd.DataFrame(summary_rows).sort_values(["coin", "body_atr_limit"]).reset_index(drop=True)
    walk_frame = pd.DataFrame(walk_rows).sort_values("coin").reset_index(drop=True)
    summary_frame.to_csv(SUMMARY_CSV, index=False, encoding="utf-8-sig")
    walk_frame.to_csv(WALK_CSV, index=False, encoding="utf-8-sig")

    unified_rows = []
    for limit in BODY_ATR_LIMITS:
        sub = summary_frame[summary_frame["body_atr_limit"] == limit]
        unified_rows.append(
            {
                "body_atr_limit": limit,
                "common_all_pnl_u": float(sub["common_all_pnl_u"].sum()),
                "test_pnl_u": float(sub["test_pnl_u"].sum()),
                "test_dd_u": float(sub["test_dd_u"].sum()),
            }
        )
    unified_frame = pd.DataFrame(unified_rows)
    best_unified = unified_frame.sort_values(["test_pnl_u", "test_dd_u"], ascending=[False, True]).iloc[0].to_dict()

    payload = {
        "created_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "common_start_utc": format_ts(COMMON_START_TS),
        "limits_tested": list(BODY_ATR_LIMITS),
        "walkforward": walk_frame.to_dict("records"),
        "best_unified": best_unified,
    }
    JSON_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    heatmap = render_heatmap(summary_frame)
    unified_chart = render_unified_chart(unified_frame)
    walk_chart = render_walk_chart(walk_frame)
    HTML_PATH.write_text(build_html(summary_frame, walk_frame, unified_frame, payload, heatmap, unified_chart, walk_chart), encoding="utf-8")
    print(HTML_PATH)


def build_entry_frame(candles: list[object]) -> pd.DataFrame:
    rows = [{"ts": int(c.ts), "timestamp": pd.to_datetime(int(c.ts), unit="ms", utc=True), "open": float(c.open), "high": float(c.high), "low": float(c.low), "close": float(c.close)} for c in candles]
    return pd.DataFrame(rows).sort_values("timestamp").drop_duplicates("timestamp").reset_index(drop=True)


def add_indicators(df: pd.DataFrame, *, ma_type: str, period: int) -> None:
    col = ma_column_name(ma_type, period)
    if ma_type == "ema":
        df[col] = df["close"].ewm(span=period, adjust=False, min_periods=period).mean()
    else:
        df[col] = df["close"].rolling(period, min_periods=period).mean()
    prev_close = df["close"].shift(1)
    tr = pd.concat([df["high"] - df["low"], (df["high"] - prev_close).abs(), (df["low"] - prev_close).abs()], axis=1).max(axis=1)
    df["atr14"] = tr.ewm(alpha=1 / ATR_PERIOD, adjust=False, min_periods=ATR_PERIOD).mean()
    df["atr_pct"] = rolling_percentile(df["atr14"], ATR_PERCENTILE_LOOKBACK)
    df["body_size"] = (df["close"] - df["open"]).abs()


def rolling_percentile(series: pd.Series, lookback: int) -> pd.Series:
    return series.rolling(lookback, min_periods=lookback).apply(lambda x: float(np.mean(x <= x[-1])), raw=True)


def build_daily_info(entry_candles: list[object], daily_candles: list[object], filter_key: str) -> list[dict[str, object]]:
    daily_frame = pd.DataFrame({"ts": [int(c.ts) for c in daily_candles], "open": [float(c.open) for c in daily_candles], "close": [float(c.close) for c in daily_candles]})
    if filter_key == "none":
        line = None
    else:
        if filter_key == "ema21":
            line = daily_frame["close"].ewm(span=21, adjust=False, min_periods=21).mean()
        else:
            line = daily_frame["close"].ewm(span=55, adjust=False, min_periods=55).mean()
    daily_available_ts = closed_candle_available_timestamps(daily_candles)
    out = []
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


def simulate_variant(frame: pd.DataFrame, *, ma_column: str, daily_info: list[dict[str, object]], body_atr_limit: float) -> pd.DataFrame:
    open_values = frame["open"].to_numpy(dtype=float)
    high_values = frame["high"].to_numpy(dtype=float)
    low_values = frame["low"].to_numpy(dtype=float)
    close_values = frame["close"].to_numpy(dtype=float)
    ts_values = frame["ts"].to_numpy(dtype=np.int64)
    line_values = frame[ma_column].to_numpy(dtype=float)
    atr_values = frame["atr14"].to_numpy(dtype=float)
    atr_pct_values = frame["atr_pct"].to_numpy(dtype=float)
    body_values = frame["body_size"].to_numpy(dtype=float)
    trades = []
    position = None
    pending = None
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
            exited = process_open_short(position, candle_open=candle_open, candle_high=candle_high, candle_low=candle_low, candle_close=candle_close, candle_ts=int(ts_values[index]), index=index, trades=trades)
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
                        position = {"entry_index": index, "entry_ts": int(ts_values[index]), "entry_price": candle_close, "risk_per_unit": risk_per_unit, "stop": candle_close + risk_per_unit, "stop_reason": "stop_loss", "fee_offset": fee_offset, "next_dynamic_r": 2.0}
                        pending = None
                        continue
        if pending is not None:
            continue
        if not np.isfinite(slope_ratio) or slope_ratio > SLOPE_THRESHOLD_RATIO or atr_pct > ATR_PERCENTILE_MAX:
            continue
        if candle_close >= line_value - BREAKDOWN_ATR_MULT * atr_value or candle_close >= candle_open:
            continue
        if not bool(daily_info[index]["weak_day"]):
            continue
        if atr_value <= 0 or (body_size / atr_value) > body_atr_limit:
            continue
        body_mid = candle_close + (candle_open - candle_close) * BODY_RECLAIM_MAX_RATIO
        pending = {"index": index, "max_reclaim_close": body_mid}
    return pd.DataFrame(trades)


def process_open_short(position, *, candle_open, candle_high, candle_low, candle_close, candle_ts, index, trades) -> bool:
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
    return {"entry_index": int(position["entry_index"]), "exit_index": exit_index, "entry_ts": int(position["entry_ts"]), "exit_ts": exit_ts, "pnl_u": pnl_u, "r_multiple": pnl_u / RISK_PER_TRADE_U, "exit_reason": exit_reason}


def compute_metrics(trades: pd.DataFrame) -> dict[str, float]:
    if trades.empty:
        return {"trades": 0.0, "pnl": 0.0, "profit_factor": 0.0, "drawdown": 0.0}
    pnls = trades["pnl_u"].astype(float)
    gross_profit = float(pnls[pnls > 0].sum())
    gross_loss = abs(float(pnls[pnls < 0].sum()))
    curve = pnls.cumsum()
    return {"trades": float(len(trades)), "pnl": float(pnls.sum()), "profit_factor": gross_profit / gross_loss if gross_loss > 0 else 0.0, "drawdown": float((curve.cummax() - curve).max())}


def render_heatmap(summary_frame: pd.DataFrame) -> str:
    pivot = summary_frame.pivot(index="coin", columns="body_atr_limit", values="test_pnl_u").reindex(columns=BODY_ATR_LIMITS)
    fig, ax = plt.subplots(figsize=(8.6, 4.5))
    im = ax.imshow(pivot.to_numpy(), cmap="RdYlGn")
    ax.set_xticks(np.arange(len(pivot.columns)), [str(col) for col in pivot.columns])
    ax.set_yticks(np.arange(len(pivot.index)), pivot.index.tolist())
    ax.set_title("公共区间后半段测试收益热力图")
    for i in range(len(pivot.index)):
        for j in range(len(pivot.columns)):
            value = pivot.iloc[i, j]
            ax.text(j, i, f"{value:.0f}", ha="center", va="center", fontsize=9)
    fig.colorbar(im, ax=ax, shrink=0.8)
    fig.tight_layout()
    return figure_to_base64(fig)


def render_unified_chart(unified_frame: pd.DataFrame) -> str:
    fig, ax = plt.subplots(figsize=(8.6, 4.8))
    ax.plot(unified_frame["body_atr_limit"], unified_frame["test_pnl_u"], marker="o", color="#16423C", linewidth=2)
    ax.set_title("统一阈值下的BNB+SOL后半段测试收益")
    ax.set_xlabel("Body/ATR Limit")
    ax.set_ylabel("PnL (U)")
    fig.tight_layout()
    return figure_to_base64(fig)


def render_walk_chart(walk_frame: pd.DataFrame) -> str:
    fig, ax = plt.subplots(figsize=(8, 4.8))
    x = np.arange(len(walk_frame))
    width = 0.35
    ax.bar(x - width / 2, walk_frame["train_pnl_u"], width, label="训练段", color="#6A9C89")
    ax.bar(x + width / 2, walk_frame["test_pnl_u"], width, label="测试段", color="#C84B31")
    ax.set_xticks(x, walk_frame["coin"].tolist())
    ax.set_title("Walk-forward 训练/测试收益")
    ax.legend()
    fig.tight_layout()
    return figure_to_base64(fig)


def build_html(summary_frame, walk_frame, unified_frame, payload, heatmap, unified_chart, walk_chart) -> str:
    walk_table = dataframe_to_html(walk_frame, {"selected_by_train_limit":1, "train_pnl_u":1, "train_pf":2, "test_pnl_u":1, "test_pf":2, "test_dd_u":1})
    unified_table = dataframe_to_html(unified_frame, {"body_atr_limit":1, "common_all_pnl_u":1, "test_pnl_u":1, "test_dd_u":1})
    return f"""<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>BNB/SOL 稳健性验证</title><style>
    :root{{--bg:#f5f2ec;--ink:#1f1f1f;--muted:#5f6f68;--card:rgba(255,255,255,.84);--line:rgba(22,66,60,.12);--a:#16423c;--b:#355f2e;--c:#c84b31;--r:24px;--s:0 18px 42px rgba(22,66,60,.10);}}
    *{{box-sizing:border-box}} body{{margin:0;font-family:"Microsoft YaHei","PingFang SC",sans-serif;color:var(--ink);background:linear-gradient(180deg,#faf7f2 0%,var(--bg) 100%)}} .wrap{{width:min(1180px,calc(100vw - 28px));margin:0 auto;padding:28px 0 56px}} .hero{{padding:34px;border-radius:32px;color:#fffdf9;background:linear-gradient(135deg, rgba(22,66,60,.96), rgba(53,95,46,.88));box-shadow:var(--s)}} .eyebrow{{letter-spacing:.16em;text-transform:uppercase;font-size:12px;opacity:.88}} h1,h2,p{{margin:0}} h1{{margin-top:12px;font-size:clamp(28px,4vw,44px);line-height:1.06}} .hero p{{margin-top:14px;line-height:1.72;max-width:860px;color:rgba(255,253,249,.90)}} .grid{{display:grid;grid-template-columns:repeat(12,1fr);gap:18px;margin-top:20px}} .card{{background:var(--card);border:1px solid var(--line);border-radius:var(--r);box-shadow:var(--s);padding:22px}} .wide{{grid-column:span 6}} .full{{grid-column:1 / -1}} .muted{{color:var(--muted)}} img{{width:100%;border-radius:18px;border:1px solid var(--line);background:#fff}} table{{width:100%;border-collapse:collapse;font-size:13px}} th,td{{text-align:left;padding:10px 8px;border-bottom:1px solid rgba(22,66,60,.10)}} th{{color:var(--muted);background:rgba(22,66,60,.03)}} @media (max-width:960px){{.wide{{grid-column:1 / -1}}}}
    </style></head><body><div class="wrap"><section class="hero"><div class="eyebrow">BNB / SOL Robustness</div><h1>BNB 与 SOL 的过拟合风险验证</h1><p>这份专题不再追求“最好看结果”，而是检查这条 Body/ATR 过滤路线在公共区间、walk-forward 和统一阈值下还能不能站住。</p></section><section class="grid">
    <div class="card wide"><h2>验证口径</h2><p class="muted">公共区间从 {html.escape(payload["common_start_utc"])} 开始。先看公共区间全段，再把公共区间切成前半训练、后半测试；同时比较“单币最佳阈值”和“统一阈值”。</p></div>
    <div class="card wide"><h2>初步判断</h2><p class="muted">如果训练段选出来的阈值，到了后半段测试还能赚钱，而且统一阈值也不过分差，那过拟合嫌疑会下降；反之则说明结果更多是后验优化出来的。</p></div>
    <div class="card wide"><h2>热力图</h2><img src="data:image/png;base64,{heatmap}" alt="heatmap"></div>
    <div class="card wide"><h2>统一阈值对比</h2><img src="data:image/png;base64,{unified_chart}" alt="unified"></div>
    <div class="card full"><h2>Walk-forward</h2><img src="data:image/png;base64,{walk_chart}" alt="walk"></div>
    <div class="card full"><h2>Walk-forward 表</h2><div style="overflow:auto;">{walk_table}</div></div>
    <div class="card full"><h2>统一阈值表</h2><div style="overflow:auto;">{unified_table}</div></div>
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


def ma_column_name(ma_type: str, period: int) -> str:
    return f"{ma_type}{period}"


def format_ts(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


if __name__ == "__main__":
    main()
