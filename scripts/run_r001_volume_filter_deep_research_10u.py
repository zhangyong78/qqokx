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
INST_ID = "BTC-USDT-SWAP"
BAR = "1H"
EMA55_SLOPE_THRESHOLD = -0.0005
STOP_ATR_MULTIPLIER = 2.0
ATR_PERCENTILE_MAX = 0.50
ATR_PERCENTILE_LOOKBACK = 100
VOLUME_PERCENTILE_LOOKBACK = 100
FIXED_RISK_AMOUNT = 10.0
TAKER_FEE_RATE = 0.00036

CSV_PATH = REPORT_DIR / "r001_volume_filter_deep_research_10u.csv"
SPLIT_CSV_PATH = REPORT_DIR / "r001_volume_filter_deep_research_10u_splits.csv"
REASON_CSV_PATH = REPORT_DIR / "r001_volume_filter_deep_research_10u_reasons.csv"
JSON_PATH = REPORT_DIR / "r001_volume_filter_deep_research_10u_summary.json"
HTML_PATH = REPORT_DIR / "r001_volume_filter_deep_research_10u_report.html"
CHART_TEST = REPORT_DIR / "r001_volume_filter_deep_research_10u_test.png"
CHART_EQUITY = REPORT_DIR / "r001_volume_filter_deep_research_10u_equity.png"
CHART_STABILITY = REPORT_DIR / "r001_volume_filter_deep_research_10u_stability.png"


@dataclass(frozen=True)
class VolumeFilter:
    key: str
    label: str
    min_pct: float | None = None
    max_pct: float | None = None
    require_volume_rising: bool = False
    require_close_breakdown: bool = False


FILTERS = [
    VolumeFilter("baseline", "基线: 不加成交量过滤"),
    VolumeFilter("vol_ge_40", "成交量分位 >= 40%", min_pct=0.40),
    VolumeFilter("vol_ge_50", "成交量分位 >= 50%", min_pct=0.50),
    VolumeFilter("vol_ge_60", "成交量分位 >= 60%", min_pct=0.60),
    VolumeFilter("vol_ge_70", "成交量分位 >= 70%", min_pct=0.70),
    VolumeFilter("vol_ge_80", "成交量分位 >= 80%", min_pct=0.80),
    VolumeFilter("vol_40_80", "温和放量: 40%-80%", min_pct=0.40, max_pct=0.80),
    VolumeFilter("vol_50_85", "温和放量: 50%-85%", min_pct=0.50, max_pct=0.85),
    VolumeFilter("vol_ge_50_rising", "分位>=50% 且较上一根放量", min_pct=0.50, require_volume_rising=True),
    VolumeFilter("vol_ge_50_breakdown", "分位>=50% 且阴线破前低", min_pct=0.50, require_close_breakdown=True),
]


def main() -> None:
    REPORT_DIR.mkdir(exist_ok=True)
    candles = load_candle_cache(INST_ID, BAR, limit=None)
    if not candles:
        raise RuntimeError(f"no candles found for {INST_ID} {BAR}")

    df = build_frame(candles)
    add_indicators(df)
    split_bounds = build_split_bounds(len(df))

    rows: list[dict[str, object]] = []
    split_rows: list[dict[str, object]] = []
    reason_rows: list[dict[str, object]] = []
    trades_by_key: dict[str, pd.DataFrame] = {}

    for filt in FILTERS:
        trades = simulate_trades(df, filt)
        trades_by_key[filt.key] = trades
        rows.append(flatten_metrics(filt, trades))
        reason_rows.extend(flatten_reason_counts(filt, trades))
        for split_name, bounds in split_bounds.items():
            split_rows.append(flatten_split_metrics(filt, split_name, split_trades(trades, bounds)))

    comparison = pd.DataFrame(rows)
    splits = pd.DataFrame(split_rows)
    reasons = pd.DataFrame(reason_rows)
    comparison["stability_score"] = comparison.apply(lambda row: stability_score(row, splits), axis=1)
    comparison = comparison.sort_values("stability_score", ascending=False).reset_index(drop=True)

    comparison.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")
    splits.to_csv(SPLIT_CSV_PATH, index=False, encoding="utf-8-sig")
    reasons.to_csv(REASON_CSV_PATH, index=False, encoding="utf-8-sig")

    save_test_chart(comparison, splits)
    save_equity_chart(trades_by_key, comparison)
    save_stability_chart(comparison, splits)

    summary = {
        "research_question": "在最终基线版本上，只测试成交量分位过滤是否能改善无量阴跌假突破。",
        "baseline": {
            "entry": "EMA55 slope <= -0.0005",
            "stop": "2.0 ATR",
            "exit": "2R breakeven then step ladder locking",
            "atr_percentile": "<= 50%",
            "slope_positive_exit": False,
            "risk_per_trade_u": FIXED_RISK_AMOUNT,
        },
        "data_start_utc": format_ts(int(df["ts"].iloc[0])),
        "data_end_utc": format_ts(int(df["ts"].iloc[-1])),
        "best": comparison.iloc[0].to_dict(),
    }
    JSON_PATH.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    HTML_PATH.write_text(build_html(comparison, splits, reasons, trades_by_key, summary), encoding="utf-8")
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
    df["volume_rising"] = df["volume"] > df["volume"].shift(1)
    df["bearish_breakdown"] = (df["close"] < df["open"]) & (df["close"] < df["low"].shift(1))


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


def volume_filter_passes(row: pd.Series, filt: VolumeFilter) -> bool:
    pct = float(row["volume_pct"])
    if filt.min_pct is not None and pct < filt.min_pct:
        return False
    if filt.max_pct is not None and pct > filt.max_pct:
        return False
    if filt.require_volume_rising and not bool(row["volume_rising"]):
        return False
    if filt.require_close_breakdown and not bool(row["bearish_breakdown"]):
        return False
    return True


def simulate_trades(df: pd.DataFrame, filt: VolumeFilter) -> pd.DataFrame:
    trades: list[dict[str, object]] = []
    position: dict[str, float | int | str] | None = None

    for index in range(max(ATR_PERCENTILE_LOOKBACK, VOLUME_PERCENTILE_LOOKBACK), len(df)):
        row = df.iloc[index]
        current_ema = finite(row["ema55"])
        previous_ema = finite(df.iloc[index - 1]["ema55"])
        atr_value = finite(row["atr14"])
        atr_pct = finite(row["atr_pct"])
        volume_pct = finite(row["volume_pct"])
        if any(math.isnan(value) for value in [current_ema, previous_ema, atr_value, atr_pct, volume_pct]):
            continue

        slope_ratio = (current_ema - previous_ema) / current_ema if current_ema else math.nan

        if position is not None:
            position["best_low"] = min(float(position["best_low"]), float(row["low"]))
            position["worst_high"] = max(float(position["worst_high"]), float(row["high"]))
            exited = False
            path = candle_path_points(row)
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
        if not volume_filter_passes(row, filt):
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
            "best_low": entry_price,
            "worst_high": entry_price,
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
    hold_hours = (exit_ts - int(position["entry_ts"])) / (1000 * 3600)
    return {
        "entry_index": int(position["entry_index"]),
        "exit_index": exit_index,
        "entry_ts": int(position["entry_ts"]),
        "exit_ts": exit_ts,
        "pnl_u": pnl_u,
        "r_multiple": pnl_u / FIXED_RISK_AMOUNT,
        "hold_hours": hold_hours,
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


def flatten_metrics(filt: VolumeFilter, trades: pd.DataFrame) -> dict[str, object]:
    row: dict[str, object] = {"filter_key": filt.key, "filter_label": filt.label}
    row.update(metrics(trades))
    return row


def flatten_split_metrics(filt: VolumeFilter, split_name: str, trades: pd.DataFrame) -> dict[str, object]:
    row: dict[str, object] = {"filter_key": filt.key, "filter_label": filt.label, "split": split_name}
    row.update(metrics(trades))
    return row


def flatten_reason_counts(filt: VolumeFilter, trades: pd.DataFrame) -> list[dict[str, object]]:
    if trades.empty:
        return []
    rows = []
    for reason, count in trades["exit_reason"].value_counts().items():
        rows.append(
            {
                "filter_key": filt.key,
                "filter_label": filt.label,
                "exit_reason": str(reason),
                "exit_label": reason_label(str(reason)),
                "count": int(count),
                "ratio": float(count / len(trades)),
            }
        )
    return rows


def stability_score(row: pd.Series, splits: pd.DataFrame) -> float:
    item = splits[splits["filter_key"] == row["filter_key"]]
    validation = item[item["split"] == "validation"].iloc[0]
    test = item[item["split"] == "test"].iloc[0]
    train = item[item["split"] == "train"].iloc[0]
    min_pf = min(float(validation["profit_factor"]), float(test["profit_factor"]))
    pnl_std = float(item[item["split"].isin(["train", "validation", "test"])]["total_pnl_u"].std(ddof=0))
    trade_penalty = max(80.0 - float(row["trades"]), 0.0) * 1.2
    return (
        float(test["total_pnl_u"]) * 1.4
        + float(validation["total_pnl_u"]) * 1.2
        + max(min_pf - 1.0, -1.0) * 100.0
        + float(row["avg_r"]) * 220.0
        + float(row["return_drawdown"]) * 70.0
        - float(row["max_drawdown_u"]) * 0.45
        - pnl_std * 0.18
        - trade_penalty
        - max(float(train["total_pnl_u"]) * -0.15, 0.0)
    )


def reason_label(reason: str) -> str:
    if reason == "break_even_stop":
        return "保本"
    if reason == "stop_loss":
        return "止损"
    if reason.startswith("locked_") and reason.endswith("r_stop"):
        return f"{reason.removeprefix('locked_').removesuffix('r_stop')}R"
    return reason


def save_test_chart(comparison: pd.DataFrame, splits: pd.DataFrame) -> None:
    test = splits[splits["split"] == "test"].set_index("filter_key")
    values = [float(test.loc[key, "total_pnl_u"]) for key in comparison["filter_key"]]
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.bar(comparison["filter_label"], values, color="#1d4ed8")
    ax.axhline(0, color="#64748b", linewidth=1)
    ax.set_title("测试集收益: 成交量过滤对比")
    ax.set_ylabel("收益 U")
    ax.tick_params(axis="x", labelrotation=28)
    ax.grid(axis="y", alpha=0.22)
    fig.tight_layout()
    fig.savefig(CHART_TEST, dpi=160)
    plt.close(fig)


def save_equity_chart(trades_by_key: dict[str, pd.DataFrame], comparison: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(12, 6))
    selected = ["baseline"] + [key for key in comparison["filter_key"].head(4).tolist() if key != "baseline"]
    colors = ["#64748b", "#1d4ed8", "#0f766e", "#b45309", "#be123c"]
    for index, key in enumerate(selected[:5]):
        trades = trades_by_key[key]
        if trades.empty:
            continue
        label = next(item.label for item in FILTERS if item.key == key)
        curve = trades["pnl_u"].astype(float).cumsum()
        ax.plot(np.arange(1, len(curve) + 1), curve, label=label, linewidth=2, color=colors[index])
    ax.axhline(0, color="#64748b", linewidth=1, linestyle="--")
    ax.set_title("全样本累计收益曲线: 基线 vs 领先过滤")
    ax.set_xlabel("交易序号")
    ax.set_ylabel("累计收益 U")
    ax.legend(fontsize=9)
    ax.grid(alpha=0.22)
    fig.tight_layout()
    fig.savefig(CHART_EQUITY, dpi=160)
    plt.close(fig)


def save_stability_chart(comparison: pd.DataFrame, splits: pd.DataFrame) -> None:
    top = comparison.head(6)
    fig, ax = plt.subplots(figsize=(12, 6))
    x = np.arange(len(top))
    width = 0.25
    for offset, split_name, color in [(-width, "train", "#94a3b8"), (0, "validation", "#0f766e"), (width, "test", "#1d4ed8")]:
        values = [
            float(splits[(splits["filter_key"] == key) & (splits["split"] == split_name)]["total_pnl_u"].iloc[0])
            for key in top["filter_key"]
        ]
        ax.bar(x + offset, values, width, label=split_name, color=color)
    ax.axhline(0, color="#64748b", linewidth=1)
    ax.set_xticks(x, top["filter_label"], rotation=25, ha="right")
    ax.set_title("训练/验证/测试收益稳定性")
    ax.set_ylabel("收益 U")
    ax.legend()
    ax.grid(axis="y", alpha=0.22)
    fig.tight_layout()
    fig.savefig(CHART_STABILITY, dpi=160)
    plt.close(fig)


def build_html(comparison: pd.DataFrame, splits: pd.DataFrame, reasons: pd.DataFrame, trades_by_key: dict[str, pd.DataFrame], summary: dict[str, object]) -> str:
    best = comparison.iloc[0]
    baseline = comparison[comparison["filter_key"] == "baseline"].iloc[0]
    best_test = splits[(splits["filter_key"] == best["filter_key"]) & (splits["split"] == "test")].iloc[0]
    baseline_test = splits[(splits["filter_key"] == "baseline") & (splits["split"] == "test")].iloc[0]
    conclusion = build_conclusion(best, baseline, best_test, baseline_test)
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>R001 成交量分位深度研究</title>
<style>
:root {{ --bg:#f4f7fb; --panel:#fff; --ink:#172233; --muted:#64748b; --line:#d9e2ec; --blue:#1d4ed8; --green:#0f766e; --amber:#b45309; --red:#be123c; }}
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
.warn {{ color:var(--amber); font-weight:800; }}
.bad {{ color:var(--red); font-weight:800; }}
.note {{ color:var(--muted); font-size:13px; }}
@media (max-width:960px) {{ .grid-4,.grid-3 {{ grid-template-columns:1fr; }} .hero {{ padding:24px 18px; }} .wrap {{ padding:18px 12px 40px; }} }}
</style>
</head>
<body>
<section class="hero">
  <h1>R001 成交量分位深度研究</h1>
  <p>研究口径：不再叠加 MACD、RSI、RVI 等趋势指标，只在最终基线之上测试成交量分位。基线固定为 EMA55斜率<=-0.0005、2ATR止损、2R保本后逐级锁盈、ATR分位<=50%、每笔风险10U，且不加入斜率转正强平。</p>
  <p>核心问题：ATR低波动蓄势期里，开仓K线是否需要“温和放量”来证明真资金参与，从而过滤无量阴跌的假突破。</p>
</section>
<main class="wrap">
  <div class="grid grid-4">
    {kpi("综合最优", html.escape(str(best["filter_label"])), "按验证/测试/PF/回撤/交易数综合")}
    {kpi("最优测试收益", f"{float(best_test['total_pnl_u']):.1f}U", f"基线测试 {float(baseline_test['total_pnl_u']):.1f}U")}
    {kpi("最优全样本", f"{float(best['total_pnl_u']):.1f}U", f"PF {float(best['profit_factor']):.2f} / 回撤 {float(best['max_drawdown_u']):.1f}U")}
    {kpi("交易数", f"{int(float(best['trades']))}", f"基线 {int(float(baseline['trades']))} 笔")}
  </div>

  <h2>结论</h2>
  <div class="card">{conclusion}</div>

  <h2>图表</h2>
  <div class="grid grid-3">
    <div class="card">{image_tag(CHART_TEST)}</div>
    <div class="card">{image_tag(CHART_EQUITY)}</div>
    <div class="card">{image_tag(CHART_STABILITY)}</div>
  </div>

  <h2>综合对比表</h2>
  <div class="card tablebox">{dataframe_table(comparison, main_columns())}</div>

  <h2>训练 / 验证 / 测试拆分</h2>
  <div class="card tablebox">{dataframe_table(splits.sort_values(["filter_label", "split"]), split_columns())}</div>

  <h2>出场原因</h2>
  <div class="card tablebox">{dataframe_table(reasons.sort_values(["filter_label", "count"], ascending=[True, False]), reason_columns())}</div>

  <h2>执行建议</h2>
  <div class="card">
    <p><strong>不要把成交量过滤做成复杂指标簇。</strong>如果最优版本只是轻微改善，可以只把它作为“观察标签”或“降风险过滤”，不要立刻改主策略。</p>
    <p><strong>实盘默认仍应先固化基线。</strong>成交量过滤如果验证/测试都提升，才作为 V2 开关；如果只提高测试集而压低验证集，就说明它更像阶段性噪声。</p>
    <p><strong>最值得保留的形态是温和放量，不是爆量。</strong>过高成交量分位往往意味着行情已经打出剧烈波动，和 ATR<=50% 的低波动蓄势逻辑会互相打架。</p>
  </div>
  <p class="note">数据区间：{html.escape(str(summary['data_start_utc']))} 至 {html.escape(str(summary['data_end_utc']))}。所有结果均按每笔固定风险 10U 计算。</p>
</main>
</body>
</html>"""


def build_conclusion(best: pd.Series, baseline: pd.Series, best_test: pd.Series, baseline_test: pd.Series) -> str:
    improvement = float(best["total_pnl_u"]) - float(baseline["total_pnl_u"])
    test_improvement = float(best_test["total_pnl_u"]) - float(baseline_test["total_pnl_u"])
    if str(best["filter_key"]) == "baseline":
        headline = "<p><span class='warn'>成交量过滤没有战胜基线。</span>这说明当前基线已经足够干净，继续加成交量条件反而可能减少交易、错过趋势尾部。</p>"
    elif improvement > 0 and test_improvement > 0:
        headline = f"<p><span class='good'>成交量过滤有增益。</span>综合最优为 {html.escape(str(best['filter_label']))}，全样本相对基线提升 {improvement:.1f}U，测试集提升 {test_improvement:.1f}U。</p>"
    else:
        headline = f"<p><span class='warn'>成交量过滤只提供局部改善。</span>综合最优为 {html.escape(str(best['filter_label']))}，但相对基线的全样本/测试集改善并不同时成立。</p>"
    return headline + (
        f"<p>基线全样本 {float(baseline['total_pnl_u']):.1f}U，PF {float(baseline['profit_factor']):.2f}，回撤 {float(baseline['max_drawdown_u']):.1f}U；"
        f"最优过滤全样本 {float(best['total_pnl_u']):.1f}U，PF {float(best['profit_factor']):.2f}，回撤 {float(best['max_drawdown_u']):.1f}U。</p>"
        "<p>真正要看的不是收益最高，而是验证集和测试集是否一起变好、交易数是否还够、>=3R/5R 大单有没有被杀掉。成交量过滤如果靠大幅砍交易换来漂亮曲线，就不适合作为默认实盘参数。</p>"
    )


def main_columns() -> list[tuple[str, str]]:
    return [
        ("filter_label", "过滤版本"),
        ("trades", "交易数"),
        ("total_pnl_u", "全样本收益U"),
        ("profit_factor", "PF"),
        ("win_rate", "胜率"),
        ("avg_r", "平均R"),
        ("avg_win_r", "盈利R"),
        ("avg_loss_r", "亏损R"),
        ("max_drawdown_u", "最大回撤U"),
        ("return_drawdown", "收益/回撤"),
        ("avg_entry_volume_pct", "平均量分位"),
        ("big_win_3r_count", ">=3R"),
        ("big_win_5r_count", ">=5R"),
        ("stability_score", "稳定分"),
    ]


def split_columns() -> list[tuple[str, str]]:
    return [
        ("filter_label", "过滤版本"),
        ("split", "样本"),
        ("trades", "交易数"),
        ("total_pnl_u", "收益U"),
        ("profit_factor", "PF"),
        ("win_rate", "胜率"),
        ("avg_r", "平均R"),
        ("max_drawdown_u", "回撤U"),
        ("big_win_3r_count", ">=3R"),
        ("big_win_5r_count", ">=5R"),
    ]


def reason_columns() -> list[tuple[str, str]]:
    return [
        ("filter_label", "过滤版本"),
        ("exit_label", "出场原因"),
        ("count", "次数"),
        ("ratio", "占比"),
    ]


def dataframe_table(frame: pd.DataFrame, columns: list[tuple[str, str]]) -> str:
    header = "".join(f"<th>{html.escape(label)}</th>" for _, label in columns)
    rows = []
    for item in frame.itertuples(index=False):
        cells = []
        for column, _ in columns:
            cells.append(f"<td>{format_cell(column, getattr(item, column))}</td>")
        rows.append("<tr>" + "".join(cells) + "</tr>")
    return f"<table><thead><tr>{header}</tr></thead><tbody>{''.join(rows)}</tbody></table>"


def format_cell(column: str, value: object) -> str:
    if isinstance(value, str):
        return html.escape(value)
    number = float(value)
    lower = column.lower()
    if "ratio" in lower or "rate" in lower or "pct" in lower:
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
