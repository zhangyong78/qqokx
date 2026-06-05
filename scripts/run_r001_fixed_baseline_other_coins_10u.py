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
OTHER_SYMBOLS = tuple(symbol for symbol in SYMBOLS if symbol != "BTC-USDT-SWAP")
EMA55_SLOPE_THRESHOLD = -0.0005
STOP_ATR_MULTIPLIER = 2.0
ATR_PERCENTILE_MAX = 0.50
ATR_PERCENTILE_LOOKBACK = 100
VOLUME_PERCENTILE_LOOKBACK = 100
VOLUME_FILTER_MIN = 0.40
FIXED_RISK_AMOUNT = 10.0
TAKER_FEE_RATE = 0.00036

CSV_PATH = REPORT_DIR / "r001_fixed_baseline_other_coins_10u.csv"
SPLIT_CSV_PATH = REPORT_DIR / "r001_fixed_baseline_other_coins_10u_splits.csv"
REASON_CSV_PATH = REPORT_DIR / "r001_fixed_baseline_other_coins_10u_reasons.csv"
SUMMARY_JSON_PATH = REPORT_DIR / "r001_fixed_baseline_other_coins_10u_summary.json"
HTML_PATH = REPORT_DIR / "r001_fixed_baseline_other_coins_10u_report.html"
CHART_TOTAL = REPORT_DIR / "r001_fixed_baseline_other_coins_10u_total.png"
CHART_SPLIT = REPORT_DIR / "r001_fixed_baseline_other_coins_10u_split.png"
CHART_EQUITY = REPORT_DIR / "r001_fixed_baseline_other_coins_10u_equity.png"


@dataclass(frozen=True)
class Variant:
    key: str
    label: str
    volume_min_pct: float | None = None


VARIANTS = (
    Variant("baseline", "固定基线"),
    Variant("volume_ge_40", "固定基线 + 成交量分位>=40%", VOLUME_FILTER_MIN),
)


def main() -> None:
    REPORT_DIR.mkdir(exist_ok=True)
    all_rows: list[dict[str, object]] = []
    split_rows: list[dict[str, object]] = []
    reason_rows: list[dict[str, object]] = []
    trades_map: dict[tuple[str, str], pd.DataFrame] = {}
    data_ranges: dict[str, dict[str, object]] = {}

    for symbol in SYMBOLS:
        candles = load_candle_cache(symbol, BAR, limit=None)
        if not candles:
            data_ranges[symbol] = {"error": "no candles"}
            continue
        frame = build_frame(candles)
        add_indicators(frame)
        split_bounds = build_split_bounds(len(frame))
        data_ranges[symbol] = {
            "candles": len(frame),
            "start_utc": format_ts(int(frame["ts"].iloc[0])),
            "end_utc": format_ts(int(frame["ts"].iloc[-1])),
        }
        for variant in VARIANTS:
            trades = simulate_trades(frame, variant)
            trades_map[(symbol, variant.key)] = trades
            all_rows.append(flatten_metrics(symbol, variant, trades))
            reason_rows.extend(flatten_reasons(symbol, variant, trades))
            for split_name, bounds in split_bounds.items():
                split_rows.append(flatten_split(symbol, variant, split_name, split_trades(trades, bounds)))

    comparison = pd.DataFrame(all_rows)
    splits = pd.DataFrame(split_rows)
    reasons = pd.DataFrame(reason_rows)
    comparison["robust_score"] = comparison.apply(lambda row: robust_score(row, splits), axis=1)
    comparison = comparison.sort_values(["variant_key", "robust_score"], ascending=[True, False]).reset_index(drop=True)

    comparison.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")
    splits.to_csv(SPLIT_CSV_PATH, index=False, encoding="utf-8-sig")
    reasons.to_csv(REASON_CSV_PATH, index=False, encoding="utf-8-sig")

    save_total_chart(comparison)
    save_split_chart(splits)
    save_equity_chart(trades_map)

    summary = {
        "symbols": list(SYMBOLS),
        "other_symbols": list(OTHER_SYMBOLS),
        "fixed_baseline": {
            "ema55_slope_threshold": EMA55_SLOPE_THRESHOLD,
            "stop_atr_multiplier": STOP_ATR_MULTIPLIER,
            "exit": "2R breakeven then step ladder locking",
            "atr_percentile_max": ATR_PERCENTILE_MAX,
            "slope_positive_exit": False,
            "risk_per_trade_u": FIXED_RISK_AMOUNT,
        },
        "v2_reference": {"volume_percentile_min": VOLUME_FILTER_MIN},
        "data_ranges": data_ranges,
    }
    SUMMARY_JSON_PATH.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
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
    start_index = max(ATR_PERCENTILE_LOOKBACK, VOLUME_PERCENTILE_LOOKBACK)

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
                        trades.append(close_trade(position, index, int(row["ts"]), stop_price, str(position["stop_reason"])))
                        position = None
                        exited = True
                        break
                else:
                    advance_step_dynamic(position, end)
            if exited:
                continue

        if position is not None:
            continue
        if slope_ratio > EMA55_SLOPE_THRESHOLD:
            continue
        if atr_pct > ATR_PERCENTILE_MAX:
            continue
        if variant.volume_min_pct is not None and volume_pct < variant.volume_min_pct:
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
            "entry_volume_pct": volume_pct,
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
        "entry_volume_pct": float(position["entry_volume_pct"]),
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
            "avg_entry_volume_pct": 0.0,
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
        "avg_entry_volume_pct": float(trades["entry_volume_pct"].mean()),
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
                "exit_reason": str(reason),
                "exit_label": reason_label(str(reason)),
                "count": int(count),
                "ratio": float(count / len(trades)),
            }
        )
    return rows


def robust_score(row: pd.Series, splits: pd.DataFrame) -> float:
    subset = splits[(splits["symbol"] == row["symbol"]) & (splits["variant_key"] == row["variant_key"])]
    validation = subset[subset["split"] == "validation"].iloc[0]
    test = subset[subset["split"] == "test"].iloc[0]
    min_pf = min(float(validation["profit_factor"]), float(test["profit_factor"]))
    return (
        float(test["total_pnl_u"]) * 1.4
        + float(validation["total_pnl_u"]) * 1.2
        + max(min_pf - 1.0, -1.0) * 100
        + float(row["avg_r"]) * 220
        + float(row["return_drawdown"]) * 70
        - float(row["max_drawdown_u"]) * 0.45
    )


def reason_label(reason: str) -> str:
    if reason == "break_even_stop":
        return "保本"
    if reason == "stop_loss":
        return "止损"
    if reason.startswith("locked_") and reason.endswith("r_stop"):
        return f"{reason.removeprefix('locked_').removesuffix('r_stop')}R"
    return reason


def save_total_chart(comparison: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(11, 5.8))
    pivot = comparison.pivot(index="coin", columns="variant_label", values="total_pnl_u").loc[
        [symbol.replace("-USDT-SWAP", "") for symbol in SYMBOLS]
    ]
    pivot.plot(kind="bar", ax=ax, color=["#1d4ed8", "#0f766e"])
    ax.axhline(0, color="#64748b", linewidth=1)
    ax.set_title("固定基线跨币种全样本收益")
    ax.set_xlabel("")
    ax.set_ylabel("收益 U")
    ax.tick_params(axis="x", labelrotation=0)
    ax.grid(axis="y", alpha=0.22)
    fig.tight_layout()
    fig.savefig(CHART_TOTAL, dpi=160)
    plt.close(fig)


def save_split_chart(splits: pd.DataFrame) -> None:
    baseline = splits[(splits["variant_key"] == "baseline") & (splits["split"].isin(["validation", "test"]))]
    coins = [symbol.replace("-USDT-SWAP", "") for symbol in SYMBOLS]
    validation = [float(baseline[(baseline["coin"] == coin) & (baseline["split"] == "validation")]["total_pnl_u"].iloc[0]) for coin in coins]
    test = [float(baseline[(baseline["coin"] == coin) & (baseline["split"] == "test")]["total_pnl_u"].iloc[0]) for coin in coins]
    x = np.arange(len(coins))
    width = 0.36
    fig, ax = plt.subplots(figsize=(11, 5.8))
    ax.bar(x - width / 2, validation, width, label="validation", color="#0f766e")
    ax.bar(x + width / 2, test, width, label="test", color="#1d4ed8")
    ax.axhline(0, color="#64748b", linewidth=1)
    ax.set_xticks(x, coins)
    ax.set_title("固定基线: 验证/测试收益")
    ax.set_ylabel("收益 U")
    ax.legend()
    ax.grid(axis="y", alpha=0.22)
    fig.tight_layout()
    fig.savefig(CHART_SPLIT, dpi=160)
    plt.close(fig)


def save_equity_chart(trades_map: dict[tuple[str, str], pd.DataFrame]) -> None:
    fig, ax = plt.subplots(figsize=(11, 5.8))
    colors = ["#64748b", "#1d4ed8", "#0f766e", "#b45309", "#be123c"]
    for idx, symbol in enumerate(SYMBOLS):
        trades = trades_map.get((symbol, "baseline"), pd.DataFrame())
        if trades.empty:
            continue
        coin = symbol.replace("-USDT-SWAP", "")
        curve = trades["pnl_u"].astype(float).cumsum()
        ax.plot(np.arange(1, len(curve) + 1), curve, label=coin, linewidth=2, color=colors[idx])
    ax.axhline(0, color="#64748b", linewidth=1, linestyle="--")
    ax.set_title("固定基线: 全样本累计收益曲线")
    ax.set_xlabel("交易序号")
    ax.set_ylabel("累计收益 U")
    ax.legend()
    ax.grid(alpha=0.22)
    fig.tight_layout()
    fig.savefig(CHART_EQUITY, dpi=160)
    plt.close(fig)


def build_html(comparison: pd.DataFrame, splits: pd.DataFrame, reasons: pd.DataFrame, summary: dict[str, object]) -> str:
    baseline = comparison[comparison["variant_key"] == "baseline"].copy()
    other_baseline = baseline[baseline["symbol"].isin(OTHER_SYMBOLS)].copy()
    profitable_other = int((other_baseline["total_pnl_u"].astype(float) > 0).sum())
    best_other = other_baseline.sort_values("total_pnl_u", ascending=False).iloc[0]
    worst_other = other_baseline.sort_values("total_pnl_u", ascending=True).iloc[0]
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>R001 固定基线其它币种回测</title>
<style>
:root {{ --bg:#f4f7fb; --panel:#fff; --ink:#172233; --muted:#64748b; --line:#d9e2ec; --blue:#1d4ed8; --green:#0f766e; --red:#be123c; --amber:#b45309; }}
* {{ box-sizing:border-box; }}
body {{ margin:0; background:var(--bg); color:var(--ink); font-family:"Microsoft YaHei","Segoe UI",Arial,sans-serif; }}
.hero {{ background:linear-gradient(135deg,#102033,#31516b); color:#fff; padding:34px 42px; }}
.hero h1 {{ margin:0 0 10px; font-size:30px; }}
.hero p {{ max-width:1200px; line-height:1.8; color:#dbe7f3; margin:7px 0; }}
.wrap {{ max-width:1320px; margin:0 auto; padding:24px 20px 54px; }}
.grid {{ display:grid; gap:16px; }}
.grid-4 {{ grid-template-columns:repeat(4,minmax(0,1fr)); }}
.grid-3 {{ grid-template-columns:repeat(3,minmax(0,1fr)); }}
.card {{ background:var(--panel); border:1px solid var(--line); border-radius:12px; padding:18px; box-shadow:0 4px 16px rgba(15,23,42,.04); }}
.kpi .label {{ color:var(--muted); font-size:13px; }}
.kpi .value {{ font-size:25px; font-weight:800; margin-top:8px; }}
.kpi .sub {{ color:var(--muted); font-size:13px; line-height:1.55; margin-top:8px; }}
h2 {{ margin:30px 0 14px; font-size:22px; }}
p {{ line-height:1.78; }}
table {{ width:100%; border-collapse:collapse; font-size:12.5px; }}
th,td {{ padding:8px 9px; border-bottom:1px solid var(--line); text-align:right; vertical-align:top; }}
th:first-child,td:first-child {{ text-align:left; }}
th {{ background:#f8fafc; color:#465467; position:sticky; top:0; }}
.tablebox {{ overflow:auto; max-height:620px; }}
img {{ width:100%; display:block; border:1px solid var(--line); border-radius:10px; background:#fff; }}
.good {{ color:var(--green); font-weight:800; }}
.bad {{ color:var(--red); font-weight:800; }}
.warn {{ color:var(--amber); font-weight:800; }}
.note {{ color:var(--muted); font-size:13px; }}
@media (max-width:960px) {{ .grid-4,.grid-3 {{ grid-template-columns:1fr; }} .hero {{ padding:24px 18px; }} .wrap {{ padding:18px 12px 40px; }} }}
</style>
</head>
<body>
<section class="hero">
  <h1>R001 固定基线其它 4 币种回测</h1>
  <p>主测试固定条件：EMA55斜率<=-0.0005，2ATR止损，2R保本后逐级锁盈，ATR分位<=50%，不加斜率转正平仓，每笔风险10U。BTC 作为参考锚点，重点看 ETH / SOL / BNB / DOGE 的迁移效果。</p>
  <p>附带列出 V2 对照：在同一固定基线上增加成交量分位>=40%。主结论仍以固定基线为准。</p>
</section>
<main class="wrap">
  <div class="grid grid-4">
    {kpi("其它4币盈利数", f"{profitable_other}/4", "固定基线全样本")}
    {kpi("其它币最佳", html.escape(str(best_other["coin"])), f"{float(best_other['total_pnl_u']):.1f}U / PF {float(best_other['profit_factor']):.2f}")}
    {kpi("其它币最差", html.escape(str(worst_other["coin"])), f"{float(worst_other['total_pnl_u']):.1f}U / PF {float(worst_other['profit_factor']):.2f}")}
    {kpi("统一风险", "10U/笔", "不按币种重新优化")}
  </div>

  <h2>结论</h2>
  <div class="card">
    {build_conclusion(other_baseline, comparison, splits)}
  </div>

  <h2>图表</h2>
  <div class="grid grid-3">
    <div class="card">{image_tag(CHART_TOTAL)}</div>
    <div class="card">{image_tag(CHART_SPLIT)}</div>
    <div class="card">{image_tag(CHART_EQUITY)}</div>
  </div>

  <h2>综合表</h2>
  <div class="card tablebox">{dataframe_table(comparison.sort_values(["variant_key", "symbol"]), main_columns())}</div>

  <h2>训练 / 验证 / 测试拆分</h2>
  <div class="card tablebox">{dataframe_table(splits.sort_values(["variant_key", "symbol", "split"]), split_columns())}</div>

  <h2>出场原因</h2>
  <div class="card tablebox">{dataframe_table(reasons.sort_values(["variant_key", "symbol", "count"], ascending=[True, True, False]), reason_columns())}</div>

  <h2>数据区间</h2>
  <div class="card tablebox">{data_ranges_table(summary["data_ranges"])}</div>
</main>
</body>
</html>"""


def build_conclusion(other_baseline: pd.DataFrame, comparison: pd.DataFrame, splits: pd.DataFrame) -> str:
    lines = []
    positive = other_baseline[other_baseline["total_pnl_u"].astype(float) > 0]
    if len(positive) == len(other_baseline):
        lines.append("<p><span class='good'>固定基线具备跨币种迁移能力。</span>其它4个币全样本均为正，说明这不是 BTC 单币种偶然结果。</p>")
    elif len(positive) >= 2:
        lines.append("<p><span class='warn'>固定基线有一定迁移能力，但不是所有币都适合。</span>其它4个币里至少一半为正，实盘应按币种分层启用。</p>")
    else:
        lines.append("<p><span class='bad'>固定基线跨币种迁移不足。</span>当前参数更像 BTC 专属，需要按币种重新校准斜率或波动过滤。</p>")

    for row in other_baseline.sort_values("total_pnl_u", ascending=False).itertuples(index=False):
        lines.append(
            f"<p><strong>{html.escape(str(row.coin))}</strong>：全样本 {float(row.total_pnl_u):.1f}U，"
            f"PF {float(row.profit_factor):.2f}，平均R {float(row.avg_r):.3f}，"
            f"最大回撤 {float(row.max_drawdown_u):.1f}U，交易 {int(float(row.trades))} 笔。</p>"
        )

    v2 = comparison[comparison["variant_key"] == "volume_ge_40"].copy()
    merged = other_baseline[["symbol", "coin", "total_pnl_u"]].merge(
        v2[["symbol", "total_pnl_u"]], on="symbol", suffixes=("_baseline", "_v2")
    )
    improved = merged[merged["total_pnl_u_v2"].astype(float) > merged["total_pnl_u_baseline"].astype(float)]
    lines.append(
        f"<p>成交量>=40% 对照：其它4币里有 {len(improved)}/4 个币相对固定基线提升。"
        "如果只在 BTC 上有效，就不要贸然推广成全币种默认；如果多币种都提升，才适合做 V2 全局开关。</p>"
    )
    return "".join(lines)


def main_columns() -> list[tuple[str, str]]:
    return [
        ("coin", "币种"),
        ("variant_label", "版本"),
        ("trades", "交易数"),
        ("total_pnl_u", "收益U"),
        ("profit_factor", "PF"),
        ("win_rate", "胜率"),
        ("avg_r", "平均R"),
        ("avg_win_r", "盈利R"),
        ("avg_loss_r", "亏损R"),
        ("max_drawdown_u", "最大回撤U"),
        ("return_drawdown", "收益/回撤"),
        ("big_win_3r_count", ">=3R"),
        ("big_win_5r_count", ">=5R"),
        ("robust_score", "稳健分"),
    ]


def split_columns() -> list[tuple[str, str]]:
    return [
        ("coin", "币种"),
        ("variant_label", "版本"),
        ("split", "样本"),
        ("trades", "交易数"),
        ("total_pnl_u", "收益U"),
        ("profit_factor", "PF"),
        ("avg_r", "平均R"),
        ("max_drawdown_u", "回撤U"),
        ("big_win_3r_count", ">=3R"),
        ("big_win_5r_count", ">=5R"),
    ]


def reason_columns() -> list[tuple[str, str]]:
    return [
        ("coin", "币种"),
        ("variant_label", "版本"),
        ("exit_label", "出场原因"),
        ("count", "次数"),
        ("ratio", "占比"),
    ]


def dataframe_table(frame: pd.DataFrame, columns: list[tuple[str, str]]) -> str:
    header = "".join(f"<th>{html.escape(label)}</th>" for _, label in columns)
    rows = []
    for item in frame.itertuples(index=False):
        cells = [f"<td>{format_cell(column, getattr(item, column))}</td>" for column, _ in columns]
        rows.append("<tr>" + "".join(cells) + "</tr>")
    return f"<table><thead><tr>{header}</tr></thead><tbody>{''.join(rows)}</tbody></table>"


def data_ranges_table(ranges: dict[str, object]) -> str:
    rows = []
    for symbol, payload in ranges.items():
        item = payload if isinstance(payload, dict) else {}
        rows.append(
            "<tr>"
            f"<td>{html.escape(symbol)}</td>"
            f"<td>{html.escape(str(item.get('candles', '-')))}</td>"
            f"<td>{html.escape(str(item.get('start_utc', item.get('error', '-'))))}</td>"
            f"<td>{html.escape(str(item.get('end_utc', '-')))}</td>"
            "</tr>"
        )
    return "<table><thead><tr><th>币种</th><th>K线数</th><th>开始</th><th>结束</th></tr></thead><tbody>" + "".join(rows) + "</tbody></table>"


def format_cell(column: str, value: object) -> str:
    if isinstance(value, str):
        return html.escape(value)
    number = float(value)
    lower = column.lower()
    if "ratio" in lower or "rate" in lower:
        return f"{number * 100:.1f}%"
    if "pnl_u" in lower or "drawdown_u" in lower:
        return f"{number:.1f}"
    if "profit_factor" in lower or "return_drawdown" in lower:
        return f"{number:.2f}"
    if "avg_r" in lower or "avg_win_r" in lower or "avg_loss_r" in lower:
        return f"{number:.3f}"
    if "trades" in lower or "count" in lower:
        return str(int(round(number)))
    if "score" in lower:
        return f"{number:.1f}"
    return f"{number:.3f}"


def image_tag(path: Path) -> str:
    encoded = base64.b64encode(path.read_bytes()).decode("ascii")
    return f'<img alt="{html.escape(path.stem)}" src="data:image/png;base64,{encoded}">'


def kpi(label: str, value: str, sub: str) -> str:
    return f"""
<div class="card kpi">
  <div class="label">{html.escape(label)}</div>
  <div class="value">{value}</div>
  <div class="sub">{html.escape(sub)}</div>
</div>"""


def finite(value: object) -> float:
    try:
        output = float(value)
    except (TypeError, ValueError):
        return math.nan
    return output if np.isfinite(output) else math.nan


def format_ts(ts: int) -> str:
    return datetime.fromtimestamp(ts / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


if __name__ == "__main__":
    main()
