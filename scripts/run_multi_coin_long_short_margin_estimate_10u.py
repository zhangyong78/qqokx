from __future__ import annotations

import base64
import html
import math
import sys
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from okx_quant.backtest import _run_backtest_with_loaded_data
from okx_quant.candle_cache import load_candle_cache
from okx_quant.models import StrategyConfig
from okx_quant.okx_client import OkxRestClient
from okx_quant.strategy_catalog import STRATEGY_DYNAMIC_LONG_ID


plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "Noto Sans CJK SC", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False


REPORT_DIR = ROOT / "reports"
BAR = "1H"
SYMBOLS = ("BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP", "BNB-USDT-SWAP", "DOGE-USDT-SWAP")
EMA55_SLOPE_THRESHOLD = -0.0005
STOP_ATR_MULTIPLIER = 2.0
ATR_PERCENTILE_MAX = 0.50
ATR_PERCENTILE_LOOKBACK = 100
VOLUME_PERCENTILE_LOOKBACK = 100
FIXED_RISK_AMOUNT = 10.0

LONG_RISK_AMOUNT = Decimal("10")
LONG_INITIAL_CAPITAL = Decimal("10000")
MAKER_FEE_RATE = Decimal("0.0001")
TAKER_FEE_RATE = Decimal("0.00028")
DOGE_LONG_START = pd.Timestamp("2021-06-01", tz="UTC")
LEVERAGES = (1, 2, 3, 5, 10)

TRADES_CSV = REPORT_DIR / "multi_coin_long_short_margin_trades_10u.csv"
SUMMARY_CSV = REPORT_DIR / "multi_coin_long_short_margin_summary_10u.csv"
LEVERAGE_CSV = REPORT_DIR / "multi_coin_long_short_margin_leverage_10u.csv"
HTML_PATH = REPORT_DIR / "multi_coin_long_short_margin_estimate_10u_report.html"
CHART_TOP = REPORT_DIR / "multi_coin_long_short_margin_top_10u.png"
CHART_CONCURRENT = REPORT_DIR / "multi_coin_long_short_margin_concurrent_10u.png"


@dataclass(frozen=True)
class ShortVariant:
    key: str
    label: str


SHORT_STRATEGY = ShortVariant("ema55_slope_short", "EMA55 斜率做空")


def main() -> None:
    REPORT_DIR.mkdir(exist_ok=True)
    client = OkxRestClient()
    trade_rows: list[dict[str, object]] = []
    notes: list[str] = []

    for symbol in SYMBOLS:
        candles = load_candle_cache(symbol, BAR, limit=None)
        if not candles:
            continue

        short_frame = build_frame(candles)
        add_short_indicators(short_frame)
        short_trades = simulate_short_trades(short_frame, symbol)
        trade_rows.extend(short_trades.to_dict("records"))

        try:
            long_rows, note = run_dynamic_long_trades(client, symbol, candles)
            trade_rows.extend(long_rows)
            if note:
                notes.append(note)
        except Exception as exc:
            notes.append(f"{symbol.replace('-USDT-SWAP', '')} 做多未纳入：{exc}")

    trades = pd.DataFrame(trade_rows)
    trades["strategy_label"] = trades["strategy_label"].astype(str)
    trades["coin"] = trades["coin"].astype(str)
    trades["entry_time"] = pd.to_datetime(trades["entry_ts"], unit="ms", utc=True)
    trades["exit_time"] = pd.to_datetime(trades["exit_ts"], unit="ms", utc=True)
    trades = trades.sort_values(["entry_ts", "coin", "strategy_label"]).reset_index(drop=True)
    trades.to_csv(TRADES_CSV, index=False, encoding="utf-8-sig")

    summary = build_summary(trades)
    leverage = build_leverage_table(trades)
    summary.to_csv(SUMMARY_CSV, index=False, encoding="utf-8-sig")
    leverage.to_csv(LEVERAGE_CSV, index=False, encoding="utf-8-sig")

    save_top_chart(summary)
    save_concurrent_chart(trades)

    HTML_PATH.write_text(build_html(trades, summary, leverage, notes), encoding="utf-8")
    print(HTML_PATH)


def build_frame(candles: list[object]) -> pd.DataFrame:
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


def add_short_indicators(df: pd.DataFrame) -> None:
    df["ema55"] = df["close"].ewm(span=55, adjust=False, min_periods=55).mean()
    prev_close = df["close"].shift(1)
    true_range = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - prev_close).abs(),
            (df["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    df["atr14"] = true_range.rolling(14, min_periods=14).mean()
    df["atr_pct"] = rolling_percentile(df["atr14"], ATR_PERCENTILE_LOOKBACK)
    df["volume_pct"] = rolling_percentile(df["volume"], VOLUME_PERCENTILE_LOOKBACK)


def rolling_percentile(series: pd.Series, lookback: int) -> pd.Series:
    return series.rolling(lookback, min_periods=lookback).apply(lambda x: float(np.mean(x <= x[-1])), raw=True)


def candle_path_points(row: pd.Series) -> tuple[float, float, float, float]:
    if float(row["close"]) >= float(row["open"]):
        return float(row["open"]), float(row["low"]), float(row["high"]), float(row["close"])
    return float(row["open"]), float(row["high"]), float(row["low"]), float(row["close"])


def simulate_short_trades(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    trades: list[dict[str, object]] = []
    position: dict[str, float | int | str] | None = None
    start_index = max(ATR_PERCENTILE_LOOKBACK, VOLUME_PERCENTILE_LOOKBACK)
    coin = symbol.replace("-USDT-SWAP", "")

    for index in range(start_index, len(df)):
        row = df.iloc[index]
        current_ema = finite(row["ema55"])
        prev_ema = finite(df.iloc[index - 1]["ema55"])
        atr_value = finite(row["atr14"])
        atr_pct = finite(row["atr_pct"])
        volume_pct = finite(row["volume_pct"])
        if any(math.isnan(value) for value in [current_ema, prev_ema, atr_value, atr_pct, volume_pct]):
            continue
        slope_ratio = (current_ema - prev_ema) / current_ema if current_ema else math.nan

        if position is not None:
            path = candle_path_points(row)
            exited = False
            for start, end in zip(path, path[1:]):
                if end > start:
                    stop_price = float(position["stop"])
                    if start <= stop_price <= end:
                        trades.append(close_short_trade(position, symbol, coin, index, int(row["ts"]), stop_price, str(position["stop_reason"])))
                        position = None
                        exited = True
                        break
                else:
                    advance_short_dynamic(position, end)
            if exited:
                continue

        if position is not None:
            continue
        if slope_ratio > EMA55_SLOPE_THRESHOLD:
            continue
        if atr_pct > ATR_PERCENTILE_MAX:
            continue

        risk_per_unit = atr_value * STOP_ATR_MULTIPLIER
        if not np.isfinite(risk_per_unit) or risk_per_unit <= 0:
            continue
        entry_price = float(row["close"])
        base_qty = FIXED_RISK_AMOUNT / risk_per_unit
        position = {
            "entry_index": index,
            "entry_ts": int(row["ts"]),
            "entry_price": entry_price,
            "risk_per_unit": risk_per_unit,
            "stop": entry_price + risk_per_unit,
            "stop_reason": "stop_loss",
            "next_dynamic_r": 2.0,
            "base_qty": base_qty,
            "notional_usdt": base_qty * entry_price,
        }
    return pd.DataFrame(trades)


def advance_short_dynamic(position: dict[str, float | int | str], favorable_price: float) -> None:
    entry = float(position["entry_price"])
    risk = float(position["risk_per_unit"])
    while True:
        next_r = float(position["next_dynamic_r"])
        trigger = entry - risk * next_r
        if favorable_price > trigger:
            break
        if math.isclose(next_r, 2.0):
            locked_r = 0.0
            reason = "break_even_stop"
        else:
            locked_r = max(next_r - 1.0, 0.0)
            reason = f"locked_{int(round(locked_r))}r_stop"
        candidate_stop = entry - risk * locked_r
        if candidate_stop < float(position["stop"]):
            position["stop"] = candidate_stop
            position["stop_reason"] = reason
        position["next_dynamic_r"] = next_r + 1.0


def close_short_trade(
    position: dict[str, float | int | str],
    symbol: str,
    coin: str,
    exit_index: int,
    exit_ts: int,
    exit_price: float,
    exit_reason: str,
) -> dict[str, object]:
    return {
        "strategy_key": SHORT_STRATEGY.key,
        "strategy_label": SHORT_STRATEGY.label,
        "direction": "short",
        "symbol": symbol,
        "coin": coin,
        "entry_index": int(position["entry_index"]),
        "exit_index": exit_index,
        "entry_ts": int(position["entry_ts"]),
        "exit_ts": exit_ts,
        "entry_price": float(position["entry_price"]),
        "exit_price": float(exit_price),
        "base_qty": float(position["base_qty"]),
        "notional_usdt": float(position["notional_usdt"]),
        "stop_distance_abs": float(position["risk_per_unit"]),
        "stop_distance_pct": float(position["risk_per_unit"]) / float(position["entry_price"]),
        "exit_reason": exit_reason,
    }


def build_dynamic_long_config(symbol: str) -> StrategyConfig:
    return StrategyConfig(
        inst_id=symbol,
        bar=BAR,
        ema_period=21,
        ema_type="ema",
        trend_ema_period=50,
        trend_ema_type="ma",
        entry_reference_ema_period=50,
        entry_reference_ema_type="ma",
        big_ema_period=233,
        atr_period=10,
        atr_stop_multiplier=Decimal("2"),
        atr_take_multiplier=Decimal("2"),
        order_size=Decimal("0"),
        trade_mode="cross",
        signal_mode="long_only",
        position_mode="net",
        environment="demo",
        tp_sl_trigger_type="mark",
        strategy_id=STRATEGY_DYNAMIC_LONG_ID,
        risk_amount=LONG_RISK_AMOUNT,
        backtest_initial_capital=LONG_INITIAL_CAPITAL,
        backtest_sizing_mode="fixed_risk",
        take_profit_mode="dynamic",
        max_entries_per_trend=1,
        dynamic_two_r_break_even=True,
        dynamic_fee_offset_enabled=True,
        trend_ema_slope_filter_enabled=False,
        time_stop_break_even_enabled=False,
        time_stop_break_even_bars=0,
    )


def run_dynamic_long_trades(
    client: OkxRestClient,
    symbol: str,
    candles: list[object],
) -> tuple[list[dict[str, object]], str | None]:
    instrument = client.get_instrument(symbol)
    filtered = [c for c in candles if getattr(c, "confirmed", True)]
    note = None
    if symbol == "DOGE-USDT-SWAP":
        start_ts = int(DOGE_LONG_START.timestamp() * 1000)
        filtered = [c for c in filtered if int(c.ts) >= start_ts]
        note = "DOGE 做多历史估算从 2021-06-01 UTC 开始，早期低价阶段会出现负止损价，未纳入。"
    result = _run_backtest_with_loaded_data(
        filtered,
        instrument,
        build_dynamic_long_config(symbol),
        data_source_note=f"local candle_cache full history | {symbol} {BAR} | candles={len(filtered)}",
        maker_fee_rate=MAKER_FEE_RATE,
        taker_fee_rate=TAKER_FEE_RATE,
    )
    coin = symbol.replace("-USDT-SWAP", "")
    rows: list[dict[str, object]] = []
    for trade in result.trades:
        base_qty = float(abs(trade.size))
        entry_price = float(trade.entry_price)
        stop_loss = float(trade.stop_loss)
        rows.append(
            {
                "strategy_key": "dynamic_long_recommended",
                "strategy_label": "动态委托做多(推荐参数)",
                "direction": "long",
                "symbol": symbol,
                "coin": coin,
                "entry_index": int(trade.entry_index),
                "exit_index": int(trade.exit_index),
                "entry_ts": int(trade.entry_ts),
                "exit_ts": int(trade.exit_ts),
                "entry_price": entry_price,
                "exit_price": float(trade.exit_price),
                "base_qty": base_qty,
                "notional_usdt": base_qty * entry_price,
                "stop_distance_abs": abs(entry_price - stop_loss),
                "stop_distance_pct": abs(entry_price - stop_loss) / entry_price if entry_price > 0 else 0.0,
                "exit_reason": str(trade.exit_reason),
            }
        )
    return rows, note


def build_summary(trades: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for (strategy_label, coin), group in trades.groupby(["strategy_label", "coin"]):
        rows.append(metric_row(strategy_label, coin, group))
    for strategy_label, group in trades.groupby("strategy_label"):
        rows.append(metric_row(strategy_label, "ALL", group))
    return pd.DataFrame(rows).sort_values(["coin", "strategy_label"]).reset_index(drop=True)


def metric_row(strategy_label: str, coin: str, group: pd.DataFrame) -> dict[str, object]:
    stop_pct = group["stop_distance_pct"].astype(float)
    notional = group["notional_usdt"].astype(float)
    return {
        "strategy_label": strategy_label,
        "coin": coin,
        "trades": int(len(group)),
        "avg_notional_usdt": float(notional.mean()),
        "median_notional_usdt": float(notional.median()),
        "p95_notional_usdt": float(np.percentile(notional, 95)),
        "max_notional_usdt": float(notional.max()),
        "avg_stop_pct": float(stop_pct.mean() * 100),
        "median_stop_pct": float(stop_pct.median() * 100),
        "min_stop_pct": float(stop_pct.min() * 100),
    }


def build_leverage_table(trades: pd.DataFrame) -> pd.DataFrame:
    concurrent = concurrent_profile(trades)
    max_concurrent_notional = float(concurrent["total_notional_usdt"].max())
    sum_of_max_single = float(trades.groupby(["strategy_label", "coin"])["notional_usdt"].max().sum())
    rows = []
    for lev in LEVERAGES:
        rows.append(
            {
                "leverage": f"{lev}x",
                "historical_max_margin_usdt": max_concurrent_notional / lev,
                "historical_max_margin_plus30pct_usdt": max_concurrent_notional / lev * 1.3,
                "conservative_upper_margin_usdt": sum_of_max_single / lev,
                "conservative_upper_plus30pct_usdt": sum_of_max_single / lev * 1.3,
            }
        )
    return pd.DataFrame(rows)


def concurrent_profile(trades: pd.DataFrame) -> pd.DataFrame:
    events: list[dict[str, object]] = []
    for row in trades.to_dict("records"):
        events.append({"ts": int(row["entry_ts"]), "delta": float(row["notional_usdt"]), "delta_count": 1})
        events.append({"ts": int(row["exit_ts"]), "delta": -float(row["notional_usdt"]), "delta_count": -1})
    frame = pd.DataFrame(events).sort_values(["ts", "delta_count"])
    frame["total_notional_usdt"] = frame["delta"].cumsum()
    frame["open_positions"] = frame["delta_count"].cumsum()
    return frame


def save_top_chart(summary: pd.DataFrame) -> None:
    focus = summary[(summary["coin"] != "ALL") & (summary["strategy_label"] != "动态委托做多(推荐参数)") | (summary["coin"] != "ALL")]
    focus = summary[summary["coin"] != "ALL"].copy()
    focus["label"] = focus["coin"] + " | " + focus["strategy_label"]
    focus = focus.sort_values("max_notional_usdt", ascending=False)
    ax = focus.plot(x="label", y="max_notional_usdt", kind="bar", figsize=(12, 5), legend=False, color="#1746a2")
    ax.set_title("单笔最大名义仓位")
    ax.set_xlabel("")
    ax.set_ylabel("USDT")
    ax.grid(axis="y", alpha=0.22)
    plt.xticks(rotation=55, ha="right")
    plt.tight_layout()
    plt.savefig(CHART_TOP, dpi=160)
    plt.close()


def save_concurrent_chart(trades: pd.DataFrame) -> None:
    concurrent = concurrent_profile(trades)
    concurrent["time"] = pd.to_datetime(concurrent["ts"], unit="ms", utc=True)
    ax = concurrent.plot(x="time", y="total_notional_usdt", figsize=(12, 4.8), legend=False, color="#15803d")
    ax.set_title("历史同时持仓总名义价值")
    ax.set_xlabel("")
    ax.set_ylabel("USDT")
    ax.grid(alpha=0.22)
    plt.tight_layout()
    plt.savefig(CHART_CONCURRENT, dpi=160)
    plt.close()


def build_html(trades: pd.DataFrame, summary: pd.DataFrame, leverage: pd.DataFrame, notes: list[str]) -> str:
    concurrent = concurrent_profile(trades)
    max_concurrent_notional = float(concurrent["total_notional_usdt"].max())
    max_open_positions = int(concurrent["open_positions"].max())
    sum_of_max_single = float(trades.groupby(["strategy_label", "coin"])["notional_usdt"].max().sum())
    top_trade = trades.loc[trades["notional_usdt"].idxmax()]

    summary_rows = "".join(summary_row_html(row) for row in summary.to_dict("records"))
    leverage_rows = "".join(leverage_row_html(row) for row in leverage.to_dict("records"))
    notes_html = "".join(f"<li>{html.escape(note)}</li>" for note in notes)

    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <title>五币多空 10U 风险金保证金估算</title>
  <style>
    :root {{
      --ink:#172033; --muted:#64748b; --line:#e2e8f0; --blue:#1746a2; --green:#15803d; --orange:#d97706; --red:#b91c1c;
    }}
    * {{ box-sizing:border-box; }}
    body {{ margin:0; font-family:"Microsoft YaHei","Noto Sans CJK SC",sans-serif; color:var(--ink);
      background:radial-gradient(circle at top left,#e0f2fe 0,#fffaf2 36%,#f8fafc 100%); }}
    header {{ padding:36px 42px 18px; }}
    h1 {{ margin:0 0 10px; font-size:30px; }}
    h2 {{ margin:26px 0 12px; font-size:21px; }}
    p {{ line-height:1.75; }}
    .sub {{ color:var(--muted); }}
    .wrap {{ padding:0 42px 42px; }}
    .cards {{ display:grid; grid-template-columns:repeat(5,minmax(0,1fr)); gap:14px; margin:18px 0 20px; }}
    .card, .panel {{ background:rgba(255,255,255,.92); border:1px solid var(--line); border-radius:20px; padding:18px; box-shadow:0 16px 42px rgba(15,23,42,.07); }}
    .k {{ color:var(--muted); font-size:13px; }}
    .v {{ margin-top:8px; font-size:24px; font-weight:800; }}
    .grid {{ display:grid; grid-template-columns:1fr 1fr; gap:18px; }}
    img {{ width:100%; border-radius:16px; border:1px solid var(--line); background:white; }}
    table {{ width:100%; border-collapse:collapse; background:white; border-radius:16px; overflow:hidden; }}
    th,td {{ padding:10px 9px; border-bottom:1px solid var(--line); text-align:right; font-size:13px; white-space:nowrap; }}
    th:first-child,td:first-child,th:nth-child(2),td:nth-child(2) {{ text-align:left; }}
    th {{ background:#f1f5f9; color:#334155; }}
    .table-wrap {{ max-height:520px; overflow:auto; border-radius:16px; border:1px solid var(--line); }}
    .note {{ padding:14px 16px; border-left:5px solid var(--orange); background:#fffbeb; border-radius:12px; }}
    ul {{ line-height:1.8; }}
    @media (max-width: 900px) {{ .cards,.grid {{ grid-template-columns:1fr; }} header,.wrap {{ padding-left:18px; padding-right:18px; }} }}
  </style>
</head>
<body>
  <header>
    <h1>五币多空并跑：10U 风险金保证金估算</h1>
    <p class="sub">口径：5 个币同时启用 `EMA55 斜率做空` 与 `动态委托做多(推荐参数)`，每笔固定风险金 10U。这里不讨论收益，只估算实际会打出多大的名义仓位，以及大概要准备多少保证金。</p>
  </header>
  <main class="wrap">
    <section class="cards">
      <div class="card"><div class="k">单笔最大名义仓位</div><div class="v">{fmt(top_trade['notional_usdt'])}U</div><div class="sub">{html.escape(str(top_trade['coin']))} / {html.escape(str(top_trade['strategy_label']))}</div></div>
      <div class="card"><div class="k">历史最大同时持仓名义</div><div class="v">{fmt(max_concurrent_notional)}U</div></div>
      <div class="card"><div class="k">历史最多同时开仓数</div><div class="v">{max_open_positions}</div></div>
      <div class="card"><div class="k">保守上限名义总额</div><div class="v">{fmt(sum_of_max_single)}U</div></div>
      <div class="card"><div class="k">3x 历史峰值保证金</div><div class="v">{fmt(float(leverage.loc[leverage['leverage']=='3x','historical_max_margin_usdt'].iloc[0]))}U</div></div>
    </section>

    <section class="grid">
      <div class="panel"><img src="data:image/png;base64,{image_b64(CHART_TOP)}" alt="单笔最大名义仓位" /></div>
      <div class="panel"><img src="data:image/png;base64,{image_b64(CHART_CONCURRENT)}" alt="历史同时持仓总名义价值" /></div>
    </section>

    <section class="panel">
      <h2>先看结论</h2>
      <p>因为你用的是固定风险金 10U，理论亏损并不会随着止损缩小而放大，但 <b>名义仓位</b> 会明显放大，所以真正需要关心的是保证金占用。历史回放里，多空并跑 5 币时，最大的同时持仓名义总额大约是 <b>{fmt(max_concurrent_notional)}U</b>；如果按所有币种/方向各自的单笔历史峰值强行叠加，极保守上限会到 <b>{fmt(sum_of_max_single)}U</b>。</p>
      <p class="note">更实用的估算应该看“历史最大同时持仓名义”这一档，再额外留 30% 到 50% 缓冲。因为极保守上限把所有单笔最大仓位当成同一时刻一起出现，这通常会夸大实际需求。</p>
      {f'<ul>{notes_html}</ul>' if notes else ''}
    </section>

    <section class="panel">
      <h2>不同杠杆下需要多少保证金</h2>
      <div class="table-wrap">
        <table>
          <thead><tr><th>杠杆</th><th>历史峰值保证金</th><th>历史峰值+30%</th><th>保守上限保证金</th><th>保守上限+30%</th></tr></thead>
          <tbody>{leverage_rows}</tbody>
        </table>
      </div>
    </section>

    <section class="panel">
      <h2>单笔仓位摘要</h2>
      <div class="table-wrap">
        <table>
          <thead><tr><th>策略</th><th>币种</th><th>交易数</th><th>平均名义U</th><th>中位名义U</th><th>P95名义U</th><th>最大名义U</th><th>平均止损%</th><th>最小止损%</th></tr></thead>
          <tbody>{summary_rows}</tbody>
        </table>
      </div>
    </section>

    <section class="panel">
      <h2>文件输出</h2>
      <p>交易明细：{html.escape(str(TRADES_CSV))}</p>
      <p>单笔摘要：{html.escape(str(SUMMARY_CSV))}</p>
      <p>杠杆估算：{html.escape(str(LEVERAGE_CSV))}</p>
    </section>
  </main>
</body>
</html>"""


def summary_row_html(row: dict[str, object]) -> str:
    return (
        "<tr>"
        f"<td>{html.escape(str(row['strategy_label']))}</td>"
        f"<td>{html.escape(str(row['coin']))}</td>"
        f"<td>{int(row['trades'])}</td>"
        f"<td>{fmt(row['avg_notional_usdt'])}</td>"
        f"<td>{fmt(row['median_notional_usdt'])}</td>"
        f"<td>{fmt(row['p95_notional_usdt'])}</td>"
        f"<td>{fmt(row['max_notional_usdt'])}</td>"
        f"<td>{fmt(row['avg_stop_pct'], 2)}%</td>"
        f"<td>{fmt(row['min_stop_pct'], 2)}%</td>"
        "</tr>"
    )


def leverage_row_html(row: dict[str, object]) -> str:
    return (
        "<tr>"
        f"<td>{html.escape(str(row['leverage']))}</td>"
        f"<td>{fmt(row['historical_max_margin_usdt'])}</td>"
        f"<td>{fmt(row['historical_max_margin_plus30pct_usdt'])}</td>"
        f"<td>{fmt(row['conservative_upper_margin_usdt'])}</td>"
        f"<td>{fmt(row['conservative_upper_plus30pct_usdt'])}</td>"
        "</tr>"
    )


def finite(value: object) -> float:
    result = float(value) if value is not None else math.nan
    return result if np.isfinite(result) else math.nan


def image_b64(path: Path) -> str:
    return base64.b64encode(path.read_bytes()).decode("ascii")


def fmt(value: object, digits: int = 1) -> str:
    if value is None:
        return "-"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "-"
    if math.isnan(number):
        return "-"
    return f"{number:,.{digits}f}"


if __name__ == "__main__":
    main()
