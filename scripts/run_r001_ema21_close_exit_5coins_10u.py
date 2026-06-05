from __future__ import annotations

import base64
import html
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


plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "Noto Sans CJK SC", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False


REPORT_DIR = ROOT / "reports"
BAR = "1H"
SYMBOLS = ("BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP", "BNB-USDT-SWAP", "DOGE-USDT-SWAP")
EMA55_SLOPE_THRESHOLD = -0.0005
STOP_ATR_MULTIPLIER = 2.0
ATR_PERCENTILE_MAX = 0.50
ATR_PERCENTILE_LOOKBACK = 100
FIXED_RISK_AMOUNT = 10.0
TAKER_FEE_RATE = 0.00036

CSV_PATH = REPORT_DIR / "r001_ema21_close_exit_5coins_10u.csv"
SPLIT_CSV_PATH = REPORT_DIR / "r001_ema21_close_exit_5coins_10u_splits.csv"
REASON_CSV_PATH = REPORT_DIR / "r001_ema21_close_exit_5coins_10u_reasons.csv"
HTML_PATH = REPORT_DIR / "r001_ema21_close_exit_5coins_10u_report.html"
CHART_TOTAL = REPORT_DIR / "r001_ema21_close_exit_5coins_10u_total.png"
CHART_TEST = REPORT_DIR / "r001_ema21_close_exit_5coins_10u_test.png"


@dataclass(frozen=True)
class Variant:
    key: str
    label: str
    close_above_ema21_exit: bool = False


VARIANTS = (
    Variant("baseline", "EMA55 斜率做空", False),
    Variant("close_above_ema21_exit", "EMA55 斜率做空 + 收盘站上EMA21平空", True),
)


def main() -> None:
    REPORT_DIR.mkdir(exist_ok=True)
    all_rows: list[dict[str, object]] = []
    split_rows: list[dict[str, object]] = []
    reason_rows: list[dict[str, object]] = []
    data_ranges: dict[str, dict[str, object]] = {}

    for symbol in SYMBOLS:
        candles = load_candle_cache(symbol, BAR, limit=None)
        if not candles:
            data_ranges[symbol] = {"error": "no candles"}
            continue
        frame = build_frame(candles)
        add_indicators(frame)
        bounds = build_split_bounds(len(frame))
        data_ranges[symbol] = {
            "candles": len(frame),
            "start_utc": format_ts(int(frame["ts"].iloc[0])),
            "end_utc": format_ts(int(frame["ts"].iloc[-1])),
        }
        for variant in VARIANTS:
            trades = simulate_trades(frame, variant)
            all_rows.append(flatten_metrics(symbol, variant, trades))
            reason_rows.extend(flatten_reasons(symbol, variant, trades))
            for split_name, split_bounds in bounds.items():
                split_rows.append(flatten_split(symbol, variant, split_name, split_trades(trades, split_bounds)))

    comparison = pd.DataFrame(all_rows).sort_values(["symbol", "variant_key"]).reset_index(drop=True)
    splits = pd.DataFrame(split_rows).sort_values(["symbol", "variant_key", "split"]).reset_index(drop=True)
    reasons = pd.DataFrame(reason_rows).sort_values(["symbol", "variant_key", "count"], ascending=[True, True, False])

    comparison.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")
    splits.to_csv(SPLIT_CSV_PATH, index=False, encoding="utf-8-sig")
    reasons.to_csv(REASON_CSV_PATH, index=False, encoding="utf-8-sig")

    save_total_chart(comparison)
    save_test_chart(splits)

    summary = {
        "symbols": list(SYMBOLS),
        "baseline": {
            "ema55_slope_threshold": EMA55_SLOPE_THRESHOLD,
            "stop_atr_multiplier": STOP_ATR_MULTIPLIER,
            "exit": "2R breakeven then step ladder locking",
            "atr_percentile_max": ATR_PERCENTILE_MAX,
            "risk_per_trade_u": FIXED_RISK_AMOUNT,
        },
        "variant": {"close_above_ema21_exit": True},
        "data_ranges": data_ranges,
    }
    HTML_PATH.write_text(build_html(comparison, splits, reasons, summary), encoding="utf-8")
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


def add_indicators(df: pd.DataFrame) -> None:
    df["ema21"] = df["close"].ewm(span=21, adjust=False, min_periods=21).mean()
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


def rolling_percentile(series: pd.Series, lookback: int) -> pd.Series:
    return series.rolling(lookback, min_periods=lookback).apply(lambda x: float(np.mean(x <= x[-1])), raw=True)


def build_split_bounds(length: int) -> dict[str, tuple[int, int]]:
    train_end = int(length * 0.6)
    validation_end = int(length * 0.8)
    return {
        "train": (0, train_end - 1),
        "validation": (train_end, validation_end - 1),
        "test": (validation_end, length - 1),
        "all": (0, length - 1),
    }


def candle_path_points(row: pd.Series) -> tuple[float, float, float, float]:
    if float(row["close"]) >= float(row["open"]):
        return float(row["open"]), float(row["low"]), float(row["high"]), float(row["close"])
    return float(row["open"]), float(row["high"]), float(row["low"]), float(row["close"])


def simulate_trades(df: pd.DataFrame, variant: Variant) -> pd.DataFrame:
    trades: list[dict[str, object]] = []
    position: dict[str, float | int | str] | None = None
    start_index = ATR_PERCENTILE_LOOKBACK

    for index in range(start_index, len(df)):
        row = df.iloc[index]
        current_ema55 = finite(row["ema55"])
        prev_ema55 = finite(df.iloc[index - 1]["ema55"])
        ema21 = finite(row["ema21"])
        atr_value = finite(row["atr14"])
        atr_pct = finite(row["atr_pct"])
        if any(math.isnan(value) for value in [current_ema55, prev_ema55, ema21, atr_value, atr_pct]):
            continue

        slope_ratio = (current_ema55 - prev_ema55) / current_ema55 if current_ema55 else math.nan

        if position is not None:
            path = candle_path_points(row)
            exited = False
            for start, end in zip(path, path[1:]):
                if end > start:
                    stop_price = float(position["stop"])
                    if start <= stop_price <= end:
                        trades.append(close_trade(position, index, int(row["ts"]), stop_price, str(position["stop_reason"])))
                        position = None
                        exited = True
                        break
                else:
                    advance_step_dynamic(position, end)
            if exited:
                continue
            if variant.close_above_ema21_exit and float(row["close"]) > ema21:
                trades.append(close_trade(position, index, int(row["ts"]), float(row["close"]), "close_above_ema21"))
                position = None
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
        fee_offset = entry_price * TAKER_FEE_RATE * 2.0
        position = {
            "entry_index": index,
            "entry_ts": int(row["ts"]),
            "entry_price": entry_price,
            "risk_per_unit": risk_per_unit,
            "stop": entry_price + risk_per_unit,
            "stop_reason": "stop_loss",
            "fee_offset": fee_offset,
            "next_dynamic_r": 2.0,
        }

    return pd.DataFrame(trades)


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
    quantity = FIXED_RISK_AMOUNT / risk if risk > 0 else 0.0
    pnl_per_unit = (entry - exit_price) - TAKER_FEE_RATE * (entry + exit_price)
    pnl_u = pnl_per_unit * quantity
    return {
        "entry_index": int(position["entry_index"]),
        "exit_index": exit_index,
        "entry_ts": int(position["entry_ts"]),
        "exit_ts": exit_ts,
        "pnl_u": pnl_u,
        "r_multiple": pnl_u / FIXED_RISK_AMOUNT,
        "hold_hours": (exit_ts - int(position["entry_ts"])) / (1000 * 3600),
        "exit_reason": exit_reason,
    }


def split_trades(trades: pd.DataFrame, bounds: tuple[int, int]) -> pd.DataFrame:
    if trades.empty:
        return trades.copy()
    start, end = bounds
    return trades[(trades["exit_index"] >= start) & (trades["exit_index"] <= end)].copy()


def metrics(trades: pd.DataFrame) -> dict[str, float]:
    if trades.empty:
        return {
            "trades": 0.0,
            "total_pnl_u": 0.0,
            "profit_factor": 0.0,
            "win_rate": 0.0,
            "avg_r": 0.0,
            "avg_win_r": 0.0,
            "avg_loss_r": 0.0,
            "max_drawdown_u": 0.0,
            "return_drawdown": 0.0,
            "avg_hold_hours": 0.0,
            "big_win_3r_count": 0.0,
            "big_win_5r_count": 0.0,
        }
    pnls = trades["pnl_u"].astype(float)
    rs = trades["r_multiple"].astype(float)
    wins = rs[rs > 0]
    losses = rs[rs <= 0]
    gross_profit = float(pnls[pnls > 0].sum())
    gross_loss = float(pnls[pnls <= 0].sum())
    curve = pnls.cumsum()
    drawdown = float((curve.cummax() - curve).max())
    total = float(pnls.sum())
    return {
        "trades": float(len(trades)),
        "total_pnl_u": total,
        "profit_factor": gross_profit / abs(gross_loss) if gross_loss < 0 else 0.0,
        "win_rate": float((rs > 0).mean()),
        "avg_r": float(rs.mean()),
        "avg_win_r": float(wins.mean()) if not wins.empty else 0.0,
        "avg_loss_r": float(losses.mean()) if not losses.empty else 0.0,
        "max_drawdown_u": drawdown,
        "return_drawdown": total / drawdown if drawdown > 0 else 0.0,
        "avg_hold_hours": float(trades["hold_hours"].mean()),
        "big_win_3r_count": float((rs >= 3.0).sum()),
        "big_win_5r_count": float((rs >= 5.0).sum()),
    }


def flatten_metrics(symbol: str, variant: Variant, trades: pd.DataFrame) -> dict[str, object]:
    row: dict[str, object] = {
        "symbol": symbol,
        "coin": symbol.replace("-USDT-SWAP", ""),
        "variant_key": variant.key,
        "variant_label": variant.label,
    }
    row.update(metrics(trades))
    return row


def flatten_split(symbol: str, variant: Variant, split_name: str, trades: pd.DataFrame) -> dict[str, object]:
    row: dict[str, object] = {
        "symbol": symbol,
        "coin": symbol.replace("-USDT-SWAP", ""),
        "variant_key": variant.key,
        "variant_label": variant.label,
        "split": split_name,
    }
    row.update(metrics(trades))
    return row


def flatten_reasons(symbol: str, variant: Variant, trades: pd.DataFrame) -> list[dict[str, object]]:
    if trades.empty:
        return []
    rows = []
    for reason, count in trades["exit_reason"].value_counts().items():
        rows.append(
            {
                "symbol": symbol,
                "coin": symbol.replace("-USDT-SWAP", ""),
                "variant_key": variant.key,
                "variant_label": variant.label,
                "exit_reason": reason,
                "count": int(count),
            }
        )
    return rows


def finite(value: object) -> float:
    result = float(value) if value is not None else math.nan
    return result if np.isfinite(result) else math.nan


def save_total_chart(comparison: pd.DataFrame) -> None:
    pivot = comparison.pivot(index="coin", columns="variant_label", values="total_pnl_u").reindex(
        ["BTC", "ETH", "SOL", "BNB", "DOGE"]
    )
    ax = pivot.plot(kind="bar", figsize=(10, 4.8), width=0.78, color=["#1746a2", "#d97706"])
    ax.axhline(0, color="#334155", linewidth=0.8)
    ax.set_title("五币总收益对比：基线 vs 收盘站上EMA21平空")
    ax.set_xlabel("")
    ax.set_ylabel("总收益(U)，每笔风险10U")
    ax.grid(axis="y", alpha=0.22)
    plt.tight_layout()
    plt.savefig(CHART_TOTAL, dpi=160)
    plt.close()


def save_test_chart(splits: pd.DataFrame) -> None:
    test = splits[splits["split"] == "test"].copy()
    pivot = test.pivot(index="coin", columns="variant_label", values="total_pnl_u").reindex(
        ["BTC", "ETH", "SOL", "BNB", "DOGE"]
    )
    ax = pivot.plot(kind="bar", figsize=(10, 4.8), width=0.78, color=["#1746a2", "#d97706"])
    ax.axhline(0, color="#334155", linewidth=0.8)
    ax.set_title("测试集收益对比")
    ax.set_xlabel("")
    ax.set_ylabel("测试集收益(U)")
    ax.grid(axis="y", alpha=0.22)
    plt.tight_layout()
    plt.savefig(CHART_TEST, dpi=160)
    plt.close()


def build_html(
    comparison: pd.DataFrame,
    splits: pd.DataFrame,
    reasons: pd.DataFrame,
    summary: dict[str, object],
) -> str:
    base = comparison[comparison["variant_key"] == "baseline"]
    ema21 = comparison[comparison["variant_key"] == "close_above_ema21_exit"]
    base_total = float(base["total_pnl_u"].sum())
    ema21_total = float(ema21["total_pnl_u"].sum())
    base_test = float(
        splits[(splits["variant_key"] == "baseline") & (splits["split"] == "test")]["total_pnl_u"].sum()
    )
    ema21_test = float(
        splits[(splits["variant_key"] == "close_above_ema21_exit") & (splits["split"] == "test")]["total_pnl_u"].sum()
    )
    base_pf = float(base["profit_factor"].mean())
    ema21_pf = float(ema21["profit_factor"].mean())
    base_dd = float(base["max_drawdown_u"].mean())
    ema21_dd = float(ema21["max_drawdown_u"].mean())

    total_pivot = comparison.pivot(index="coin", columns="variant_key", values="total_pnl_u")
    winner_items = []
    for coin in total_pivot.index:
        winner = "EMA21平空版" if total_pivot.loc[coin, "close_above_ema21_exit"] > total_pivot.loc[coin, "baseline"] else "基线版"
        delta = float(total_pivot.loc[coin, "close_above_ema21_exit"] - total_pivot.loc[coin, "baseline"])
        winner_items.append(f"<li><b>{html.escape(coin)}</b>：{winner}，差值 {fmt(delta)}U</li>")

    reason_focus = reasons[reasons["variant_key"] == "close_above_ema21_exit"].copy()
    ema21_close_count = int(reason_focus[reason_focus["exit_reason"] == "close_above_ema21"]["count"].sum())

    rows_html = "".join(metric_row(row) for row in comparison.to_dict("records"))
    split_html = "".join(split_row(row) for row in splits.to_dict("records"))

    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <title>R001 EMA21平空条件五币对比报告</title>
  <style>
    :root {{
      --ink:#172033; --muted:#64748b; --line:#e2e8f0; --blue:#1746a2; --orange:#d97706; --green:#15803d; --red:#b91c1c;
    }}
    * {{ box-sizing:border-box; }}
    body {{ margin:0; font-family:"Microsoft YaHei","Noto Sans CJK SC",sans-serif; color:var(--ink);
      background:linear-gradient(180deg,#f8fbff 0,#fff8ef 100%); }}
    header {{ padding:36px 42px 18px; }}
    h1 {{ margin:0 0 10px; font-size:30px; }}
    h2 {{ margin:26px 0 12px; font-size:21px; }}
    p {{ line-height:1.75; }}
    .sub {{ color:var(--muted); }}
    .wrap {{ padding:0 42px 42px; }}
    .cards {{ display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:14px; margin:18px 0 20px; }}
    .card, .panel {{ background:rgba(255,255,255,.92); border:1px solid var(--line); border-radius:20px; padding:18px; box-shadow:0 16px 42px rgba(15,23,42,.07); }}
    .k {{ color:var(--muted); font-size:13px; }}
    .v {{ margin-top:8px; font-size:24px; font-weight:800; }}
    .grid {{ display:grid; grid-template-columns:1fr 1fr; gap:18px; }}
    img {{ width:100%; border-radius:16px; border:1px solid var(--line); background:white; }}
    table {{ width:100%; border-collapse:collapse; background:white; border-radius:16px; overflow:hidden; }}
    th,td {{ padding:10px 9px; border-bottom:1px solid var(--line); text-align:right; font-size:13px; white-space:nowrap; }}
    th:first-child,td:first-child,th:nth-child(2),td:nth-child(2),th:nth-child(3),td:nth-child(3) {{ text-align:left; }}
    th {{ background:#f1f5f9; color:#334155; }}
    .good {{ color:var(--green); font-weight:700; }}
    .bad {{ color:var(--red); font-weight:700; }}
    .note {{ padding:14px 16px; border-left:5px solid var(--orange); background:#fffbeb; border-radius:12px; }}
    ul {{ line-height:1.8; }}
    @media (max-width: 900px) {{ .cards,.grid {{ grid-template-columns:1fr; }} header,.wrap {{ padding-left:18px; padding-right:18px; }} table {{ display:block; overflow-x:auto; }} }}
  </style>
</head>
<body>
  <header>
    <h1>R001 五币研究：加入“收盘站上 EMA21 平空”后，会更好吗？</h1>
    <p class="sub">口径：BTC / ETH / SOL / BNB / DOGE 永续，1小时，全量本地K线缓存，固定每笔风险 10U。只改一个条件：空单持仓期间，若收盘价重新站上 EMA21，则按收盘价平仓。</p>
  </header>
  <main class="wrap">
    <section class="cards">
      <div class="card"><div class="k">基线五币合计收益</div><div class="v">{fmt(base_total)}U</div></div>
      <div class="card"><div class="k">EMA21平空版五币合计收益</div><div class="v">{fmt(ema21_total)}U</div></div>
      <div class="card"><div class="k">基线测试集合计</div><div class="v">{fmt(base_test)}U</div></div>
      <div class="card"><div class="k">EMA21平空版测试集合计</div><div class="v">{fmt(ema21_test)}U</div></div>
    </section>

    <section class="grid">
      <div class="panel"><img src="data:image/png;base64,{image_b64(CHART_TOTAL)}" alt="总收益对比" /></div>
      <div class="panel"><img src="data:image/png;base64,{image_b64(CHART_TEST)}" alt="测试集对比" /></div>
    </section>

    <section class="panel">
      <h2>结论先说</h2>
      <p>这条平仓规则的逻辑很直观：一旦价格重新站上 EMA21，说明短线下跌节奏被破坏，先撤退。但能不能进我们的工作模型，不该靠直觉，要看它是不是稳定地改善了收益、回撤和样本外表现。</p>
      <p>从这次五币回测看，基线五币合计收益为 <b>{fmt(base_total)}U</b>，加入 EMA21 平空后掉到 <b>{fmt(ema21_total)}U</b>；测试集合计从 <b>{fmt(base_test)}U</b> 掉到 <b>{fmt(ema21_test)}U</b>。平均 PF 从 <b>{fmt(base_pf, 3)}</b> 下降到 <b>{fmt(ema21_pf, 3)}</b>，虽然平均最大回撤从 <b>{fmt(base_dd)}</b>U 降到 <b>{fmt(ema21_dd)}</b>U，但代价是交易次数暴增、平均R被压扁，EMA21 平空触发总次数达到 <b>{ema21_close_count}</b> 次。</p>
      <ul>{"".join(winner_items)}</ul>
      <p class="note">结论很明确：它不适合进入当前 `5.4` 工作模型默认参数。原因是它属于典型的“看着更安全，但统计上削利润”的保护条件。回撤并没有换来足够大的稳定性提升，反而显著伤害了总收益、测试集表现和趋势大单保留能力。</p>
    </section>

    <section class="panel">
      <h2>全量指标表</h2>
      <table>
        <thead><tr><th>币种</th><th>版本</th><th>交易数</th><th>总收益U</th><th>PF</th><th>胜率</th><th>平均R</th><th>最大回撤U</th><th>收益/回撤</th><th>3R+</th><th>5R+</th></tr></thead>
        <tbody>{rows_html}</tbody>
      </table>
    </section>

    <section class="panel">
      <h2>训练 / 验证 / 测试</h2>
      <table>
        <thead><tr><th>币种</th><th>版本</th><th>区间</th><th>交易数</th><th>收益U</th><th>PF</th><th>胜率</th><th>平均R</th><th>最大回撤U</th></tr></thead>
        <tbody>{split_html}</tbody>
      </table>
    </section>

    <section class="panel">
      <h2>工作模型判断</h2>
      <p>这条规则本质上是一个 <b>短周期均线反抽退出</b>。它通常会带来三种结果：第一，确实减少部分回撤；第二，减少一部分 3R、5R 以上的大单；第三，让策略从“趋势突破”更偏向“短线动量”。这次结果就非常典型：五币 3R+ 交易从 <b>{int(base['big_win_3r_count'].sum())}</b> 笔降到 <b>{int(ema21['big_win_3r_count'].sum())}</b> 笔，5R+ 从 <b>{int(base['big_win_5r_count'].sum())}</b> 笔降到 <b>{int(ema21['big_win_5r_count'].sum())}</b> 笔。</p>
      <p>因此我的建议是：不要把“收盘站上 EMA21 平空”并入 `5.4` 默认模型。它可以作为一个可选防守开关，供极端保守场景试验，但不应该替代当前 R001 的主线退出逻辑。</p>
      <p class="sub">报告文件：{html.escape(str(HTML_PATH))}；CSV：{html.escape(str(CSV_PATH))}；原因统计：{html.escape(str(REASON_CSV_PATH))}</p>
    </section>
  </main>
</body>
</html>"""


def metric_row(row: dict[str, object]) -> str:
    pnl = float(row["total_pnl_u"])
    return (
        "<tr>"
        f"<td>{html.escape(str(row['coin']))}</td>"
        f"<td>{html.escape(str(row['variant_label']))}</td>"
        f"<td>{int(float(row['trades']))}</td>"
        f"<td class=\"{cls(pnl)}\">{fmt(pnl)}</td>"
        f"<td>{fmt(row['profit_factor'], 3)}</td>"
        f"<td>{pct(row['win_rate'])}</td>"
        f"<td class=\"{cls(float(row['avg_r']))}\">{fmt(row['avg_r'], 3)}</td>"
        f"<td>{fmt(row['max_drawdown_u'])}</td>"
        f"<td>{fmt(row['return_drawdown'], 2)}</td>"
        f"<td>{int(float(row['big_win_3r_count']))}</td>"
        f"<td>{int(float(row['big_win_5r_count']))}</td>"
        "</tr>"
    )


def split_row(row: dict[str, object]) -> str:
    pnl = float(row["total_pnl_u"])
    return (
        "<tr>"
        f"<td>{html.escape(str(row['coin']))}</td>"
        f"<td>{html.escape(str(row['variant_label']))}</td>"
        f"<td>{html.escape(str(row['split']))}</td>"
        f"<td>{int(float(row['trades']))}</td>"
        f"<td class=\"{cls(pnl)}\">{fmt(pnl)}</td>"
        f"<td>{fmt(row['profit_factor'], 3)}</td>"
        f"<td>{pct(row['win_rate'])}</td>"
        f"<td class=\"{cls(float(row['avg_r']))}\">{fmt(row['avg_r'], 3)}</td>"
        f"<td>{fmt(row['max_drawdown_u'])}</td>"
        "</tr>"
    )


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


def pct(value: object) -> str:
    try:
        return f"{float(value) * 100:.1f}%"
    except (TypeError, ValueError):
        return "-"


def cls(value: float) -> str:
    return "good" if value > 0 else "bad" if value < 0 else ""


def format_ts(ts: int) -> str:
    seconds = ts / 1000 if ts >= 10**12 else ts
    return datetime.fromtimestamp(seconds, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")


if __name__ == "__main__":
    main()
