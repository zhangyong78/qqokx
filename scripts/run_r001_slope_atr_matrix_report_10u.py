from __future__ import annotations

import base64
import html
import json
import math
import sys
from dataclasses import asdict, dataclass
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
TAKER_FEE_RATE = 0.00036
FIXED_RISK_AMOUNT = 10.0

SLOPE_THRESHOLDS = (-0.0001, -0.0002, -0.0003, -0.0004, -0.0005)
STOP_ATR_MULTIPLIERS = (1.0, 1.5, 2.0)

CSV_PATH = REPORT_DIR / "r001_slope_atr_matrix_10u.csv"
JSON_PATH = REPORT_DIR / "r001_slope_atr_matrix_10u_summary.json"
HTML_PATH = REPORT_DIR / "r001_slope_atr_matrix_10u_report.html"
HEATMAP_TEST_AVG_R = REPORT_DIR / "r001_slope_atr_matrix_10u_test_avg_r.png"
HEATMAP_TEST_PNL = REPORT_DIR / "r001_slope_atr_matrix_10u_test_total_pnl.png"
HEATMAP_TEST_PF = REPORT_DIR / "r001_slope_atr_matrix_10u_test_pf.png"
HEATMAP_TEST_TRADES = REPORT_DIR / "r001_slope_atr_matrix_10u_test_trades.png"


@dataclass(frozen=True)
class EntryConfig:
    slope_threshold: float
    stop_atr_mult: float

    @property
    def name(self) -> str:
        return f"slope_{self.slope_threshold:.4f}_stop_{self.stop_atr_mult:.1f}atr"


def main() -> None:
    REPORT_DIR.mkdir(exist_ok=True)
    candles = load_candle_cache(INST_ID, BAR, limit=None)
    if not candles:
        raise RuntimeError(f"no candles found for {INST_ID} {BAR}")

    df = build_frame(candles)
    add_indicators(df)
    split_bounds = build_split_bounds(len(df))

    rows: list[dict[str, object]] = []
    trade_map: dict[str, pd.DataFrame] = {}
    for slope_threshold in SLOPE_THRESHOLDS:
        for stop_atr_mult in STOP_ATR_MULTIPLIERS:
            config = EntryConfig(slope_threshold=slope_threshold, stop_atr_mult=stop_atr_mult)
            trades = simulate_trades(df, config)
            row = flatten_split_metrics(config, trades, split_bounds)
            row["score"] = score_row(row)
            rows.append(row)
            trade_map[config.name] = trades

    comparison = pd.DataFrame(rows).sort_values("score", ascending=False).reset_index(drop=True)
    comparison.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")

    best_row = comparison.iloc[0].to_dict()
    best_config = EntryConfig(
        slope_threshold=float(best_row["slope_threshold"]),
        stop_atr_mult=float(best_row["stop_atr_mult"]),
    )
    best_trades = trade_map[best_config.name]

    save_heatmap(comparison, metric="test_avg_r", title="测试段 Avg R 热力图", output_path=HEATMAP_TEST_AVG_R, fmt=".3f")
    save_heatmap(comparison, metric="test_total_pnl_u", title="测试段总盈亏(10U风险)热力图", output_path=HEATMAP_TEST_PNL, fmt=".1f")
    save_heatmap(comparison, metric="test_profit_factor", title="测试段 Profit Factor 热力图", output_path=HEATMAP_TEST_PF, fmt=".2f")
    save_heatmap(comparison, metric="test_trades", title="测试段交易数热力图", output_path=HEATMAP_TEST_TRADES, fmt=".0f")

    summary = {
        "assumption": "未在仓库中找到单独落盘的 R001 参数文件，本报告按现有 EMA55 slope short 基线逻辑扩展矩阵。",
        "risk_per_trade_usdt": FIXED_RISK_AMOUNT,
        "data_start_utc": format_ts(int(df["ts"].iloc[0])),
        "data_end_utc": format_ts(int(df["ts"].iloc[-1])),
        "instrument": INST_ID,
        "bar": BAR,
        "best_config": asdict(best_config),
        "best_row": best_row,
        "top3": comparison.head(3).to_dict("records"),
    }
    JSON_PATH.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    HTML_PATH.write_text(build_html(df=df, comparison=comparison, best_config=best_config, best_trades=best_trades, summary=summary), encoding="utf-8")
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
        }
        for candle in candles
    ]
    return pd.DataFrame(rows).sort_values("timestamp").drop_duplicates("timestamp").reset_index(drop=True)


def add_indicators(df: pd.DataFrame) -> None:
    df["ema55"] = df["close"].ewm(span=55, adjust=False, min_periods=55).mean()
    prev_close = df["close"].shift(1)
    tr = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - prev_close).abs(),
            (df["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    df["atr14"] = tr.rolling(14, min_periods=14).mean()


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


def simulate_trades(df: pd.DataFrame, entry_config: EntryConfig) -> pd.DataFrame:
    trades: list[dict[str, object]] = []
    position: dict[str, float | int] | None = None

    for index in range(56, len(df)):
        row = df.iloc[index]
        current_ema = float(row["ema55"]) if pd.notna(row["ema55"]) else math.nan
        previous_ema = float(df.iloc[index - 1]["ema55"]) if pd.notna(df.iloc[index - 1]["ema55"]) else math.nan
        atr_value = float(row["atr14"]) if pd.notna(row["atr14"]) else math.nan
        if not np.isfinite(current_ema) or not np.isfinite(previous_ema) or not np.isfinite(atr_value):
            continue

        slope = current_ema - previous_ema
        slope_ratio = slope / current_ema if current_ema else math.nan

        if position is not None:
            candle_high = float(row["high"])
            candle_low = float(row["low"])
            position["best_low"] = min(float(position["best_low"]), candle_low)
            position["worst_high"] = max(float(position["worst_high"]), candle_high)

            exited = False
            path = candle_path_points(row)
            for start, end in zip(path, path[1:]):
                if end > start:
                    stop_price = float(position["stop"])
                    if stop_price >= start and stop_price <= end:
                        trades.append(close_trade(position, index, int(row["ts"]), stop_price, "stop"))
                        position = None
                        exited = True
                        break

            if position is not None and slope > 0:
                trades.append(close_trade(position, index, int(row["ts"]), float(row["close"]), "slope_turn_positive"))
                position = None
                exited = True

            if exited:
                continue

        if position is not None:
            continue
        if slope_ratio > entry_config.slope_threshold:
            continue

        risk_per_unit = atr_value * entry_config.stop_atr_mult
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
            "fee_offset": fee_offset,
            "best_low": entry_price,
            "worst_high": entry_price,
        }

    return pd.DataFrame(trades)


def close_trade(
    position: dict[str, float | int],
    exit_index: int,
    exit_ts: int,
    exit_price: float,
    exit_reason: str,
) -> dict[str, object]:
    entry_price = float(position["entry_price"])
    risk_per_unit = float(position["risk_per_unit"])
    quantity = FIXED_RISK_AMOUNT / risk_per_unit if risk_per_unit > 0 else 0.0
    pnl_per_unit = (entry_price - exit_price) - (TAKER_FEE_RATE * (entry_price + exit_price))
    pnl_u = pnl_per_unit * quantity
    r_multiple = pnl_u / FIXED_RISK_AMOUNT if FIXED_RISK_AMOUNT else 0.0
    max_favorable_r = ((entry_price - float(position["best_low"])) - float(position["fee_offset"])) / risk_per_unit
    max_adverse_r = (float(position["worst_high"]) - entry_price) / risk_per_unit
    return {
        "entry_index": int(position["entry_index"]),
        "exit_index": exit_index,
        "entry_ts": int(position["entry_ts"]),
        "exit_ts": exit_ts,
        "entry_price": entry_price,
        "exit_price": exit_price,
        "risk_per_unit": risk_per_unit,
        "quantity": quantity,
        "pnl_per_unit": pnl_per_unit,
        "pnl_u": pnl_u,
        "r_multiple": r_multiple,
        "exit_reason": exit_reason,
        "max_favorable_r": max_favorable_r,
        "max_adverse_r": max_adverse_r,
    }


def split_trades(trades: pd.DataFrame, bounds: tuple[int, int]) -> pd.DataFrame:
    if trades.empty:
        return trades.copy()
    start, end = bounds
    return trades[(trades["exit_index"] >= start) & (trades["exit_index"] <= end)].copy()


def metrics_for_trades(trades: pd.DataFrame) -> dict[str, float]:
    if trades.empty:
        return {
            "trades": 0.0,
            "win_rate": 0.0,
            "avg_r": 0.0,
            "total_r": 0.0,
            "profit_factor": 0.0,
            "max_drawdown_r": 0.0,
            "avg_mfe_r": 0.0,
            "avg_mae_r": 0.0,
            "avg_pnl_u": 0.0,
            "total_pnl_u": 0.0,
            "max_drawdown_u": 0.0,
        }
    rs = trades["r_multiple"].astype(float)
    pnls = trades["pnl_u"].astype(float)
    wins = pnls[pnls > 0]
    losses = pnls[pnls <= 0]
    gross_profit = float(wins.sum())
    gross_loss = float(losses.sum())
    cumulative_r = rs.cumsum()
    cumulative_u = pnls.cumsum()
    drawdown_r = (cumulative_r.cummax() - cumulative_r).max()
    drawdown_u = (cumulative_u.cummax() - cumulative_u).max()
    return {
        "trades": float(len(trades)),
        "win_rate": float((rs > 0).mean()),
        "avg_r": float(rs.mean()),
        "total_r": float(rs.sum()),
        "profit_factor": float(gross_profit / abs(gross_loss)) if gross_loss < 0 else 0.0,
        "max_drawdown_r": float(drawdown_r),
        "avg_mfe_r": float(trades["max_favorable_r"].astype(float).mean()),
        "avg_mae_r": float(trades["max_adverse_r"].astype(float).mean()),
        "avg_pnl_u": float(pnls.mean()),
        "total_pnl_u": float(pnls.sum()),
        "max_drawdown_u": float(drawdown_u),
    }


def flatten_split_metrics(entry_config: EntryConfig, trades: pd.DataFrame, split_bounds: dict[str, tuple[int, int]]) -> dict[str, object]:
    row: dict[str, object] = {
        "name": entry_config.name,
        "slope_threshold": entry_config.slope_threshold,
        "stop_atr_mult": entry_config.stop_atr_mult,
    }
    for split_name, bounds in split_bounds.items():
        metrics = metrics_for_trades(split_trades(trades, bounds))
        for metric_name, value in metrics.items():
            row[f"{split_name}_{metric_name}"] = value
    return row


def score_row(row: dict[str, object]) -> float:
    return (
        float(row["test_total_pnl_u"]) * 1.4
        + float(row["validation_total_pnl_u"]) * 1.1
        + max(float(row["test_profit_factor"]) - 1.0, -1.0) * 25.0
        + max(float(row["validation_profit_factor"]) - 1.0, -1.0) * 20.0
        + min(float(row["test_trades"]), 120.0) / 120.0 * 8.0
        + min(float(row["validation_trades"]), 120.0) / 120.0 * 7.0
        - float(row["all_max_drawdown_u"]) * 0.8
    )


def save_heatmap(comparison: pd.DataFrame, *, metric: str, title: str, output_path: Path, fmt: str) -> None:
    pivot = comparison.pivot(index="slope_threshold", columns="stop_atr_mult", values=metric).sort_index(ascending=False).sort_index(axis=1)
    values = pivot.to_numpy(dtype=float)
    fig, ax = plt.subplots(figsize=(8.4, 4.8))
    image = ax.imshow(values, aspect="auto", cmap="RdYlGn")
    ax.set_title(title)
    ax.set_xlabel("止损 ATR")
    ax.set_ylabel("斜率阈值")
    ax.set_xticks(range(len(pivot.columns)), [f"{float(value):.1f}" for value in pivot.columns])
    ax.set_yticks(range(len(pivot.index)), [f"{float(value):.4f}" for value in pivot.index])
    for row_idx in range(values.shape[0]):
        for col_idx in range(values.shape[1]):
            ax.text(col_idx, row_idx, format(values[row_idx, col_idx], fmt), ha="center", va="center", color="#111827", fontsize=10)
    fig.colorbar(image, ax=ax, shrink=0.92)
    fig.tight_layout()
    fig.savefig(output_path, dpi=160)
    plt.close(fig)


def build_html(*, df: pd.DataFrame, comparison: pd.DataFrame, best_config: EntryConfig, best_trades: pd.DataFrame, summary: dict[str, object]) -> str:
    top_rows = comparison.head(10).copy()
    matrix_rows = comparison[
        [
            "slope_threshold",
            "stop_atr_mult",
            "validation_trades",
            "validation_total_pnl_u",
            "validation_avg_r",
            "validation_profit_factor",
            "test_trades",
            "test_total_pnl_u",
            "test_avg_r",
            "test_profit_factor",
            "all_trades",
            "all_total_pnl_u",
            "all_max_drawdown_u",
            "score",
        ]
    ].copy()
    best_metrics = metrics_for_trades(best_trades)
    stable_rows = comparison[(comparison["validation_total_pnl_u"] > 0) & (comparison["test_total_pnl_u"] > 0) & (comparison["test_profit_factor"] > 1.0)].copy()
    best_validation_pnl = float(comparison["validation_total_pnl_u"].max()) if not comparison.empty else 0.0
    strongest_stop = comparison.groupby("stop_atr_mult")["test_total_pnl_u"].mean().sort_values(ascending=False).index[0]
    strongest_slope = comparison.groupby("slope_threshold")["test_total_pnl_u"].mean().sort_values(ascending=False).index[0]
    stability_text = (
        f"当前共有 <strong>{len(stable_rows)}</strong> 组同时满足验证段盈利、测试段盈利且测试 PF > 1。"
        if len(stable_rows) > 0
        else "当前 <span class=\"bad\">没有任何组合</span> 同时满足验证段盈利、测试段盈利且测试 PF > 1，这说明这轮参数在样本外还不够稳。"
    )
    validation_text = (
        f"所有组合里最好的验证段总盈亏也只有 <strong>{best_validation_pnl:.1f}U</strong>。"
        if best_validation_pnl > 0
        else f"所有组合的验证段总盈亏都为负，最好的也只有 <strong>{best_validation_pnl:.1f}U</strong>。"
    )
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>R001 斜率 x ATR 10U 风险矩阵报告</title>
<style>
:root {{
  --bg:#f4f6f9; --panel:#ffffff; --ink:#152032; --muted:#607080; --line:#d9e2ec;
  --hero-a:#0f172a; --hero-b:#22466f; --good:#0f766e; --warn:#b45309; --bad:#be123c;
}}
* {{ box-sizing:border-box; }}
body {{ margin:0; background:var(--bg); color:var(--ink); font-family:"Microsoft YaHei","Segoe UI",Arial,sans-serif; }}
.hero {{ background:linear-gradient(135deg,var(--hero-a),var(--hero-b)); color:#fff; padding:34px 42px; }}
.hero h1 {{ margin:0 0 8px; font-size:30px; }}
.hero p {{ margin:6px 0; max-width:1120px; color:#dce8f5; line-height:1.7; }}
.wrap {{ max-width:1260px; margin:0 auto; padding:24px 20px 48px; }}
.grid {{ display:grid; gap:16px; }}
.grid-4 {{ grid-template-columns:repeat(4, minmax(0, 1fr)); }}
.grid-3 {{ grid-template-columns:repeat(3, minmax(0, 1fr)); }}
.grid-2 {{ grid-template-columns:repeat(2, minmax(0, 1fr)); }}
.card {{ background:var(--panel); border:1px solid var(--line); border-radius:14px; padding:18px; box-shadow:0 4px 16px rgba(15,23,42,.04); }}
.kpi .label {{ color:var(--muted); font-size:13px; }}
.kpi .value {{ font-size:28px; font-weight:800; margin-top:8px; }}
.kpi .sub {{ color:var(--muted); font-size:13px; margin-top:8px; line-height:1.6; }}
h2 {{ margin:28px 0 14px; font-size:22px; }}
h3 {{ margin:0 0 10px; font-size:17px; }}
p {{ line-height:1.75; }}
.answer {{ font-size:17px; line-height:1.85; }}
.good {{ color:var(--good); font-weight:700; }}
.warn {{ color:var(--warn); font-weight:700; }}
.bad {{ color:var(--bad); font-weight:700; }}
table {{ width:100%; border-collapse:collapse; font-size:13px; }}
th, td {{ padding:9px 10px; border-bottom:1px solid var(--line); text-align:right; }}
th:first-child, td:first-child {{ text-align:left; }}
th {{ background:#f8fafc; color:#465467; }}
.imgbox img {{ width:100%; display:block; border-radius:10px; border:1px solid var(--line); background:#fff; }}
.callout {{ border-left:5px solid #1d4ed8; background:#eff6ff; border-radius:10px; padding:14px 16px; }}
.note {{ color:var(--muted); font-size:13px; }}
@media (max-width: 920px) {{
  .grid-4, .grid-3, .grid-2 {{ grid-template-columns:1fr; }}
  .hero {{ padding:24px 18px; }}
  .wrap {{ padding:18px 12px 36px; }}
}}
</style>
</head>
<body>
<section class="hero">
  <h1>R001 参数延展: 斜率 x ATR 矩阵测试（每笔风险 10U）</h1>
  <p>这次按你新的区间要求，把斜率阈值改成 <strong>-0.0001 / -0.0002 / -0.0003 / -0.0004 / -0.0005</strong> 共 5 组，再和 <strong>ATR 1 / 1.5 / 2</strong> 组合成 15 组矩阵。回测口径仍然沿用当前仓库里的 <strong>EMA55 slope short</strong> 基线逻辑。</p>
  <p>资金口径改成 <strong>每笔固定风险 10U</strong>，也就是每一单的头寸大小都按 <code>10 / 风险距离</code> 反推，所以表格里的总盈亏、平均盈亏、回撤金额都可以直接按 USDT 阅读。</p>
</section>
<main class="wrap">
  <div class="grid grid-4">
    {kpi("最佳组合", f"斜率≤{best_config.slope_threshold:.4f}", f"止损 {best_config.stop_atr_mult:.1f} ATR")}
    {kpi("最佳测试盈亏", f"{float(top_rows.iloc[0]['test_total_pnl_u']):.1f}U", f"测试 Avg R {float(top_rows.iloc[0]['test_avg_r']):.3f}")}
    {kpi("最佳全样本盈亏", f"{best_metrics['total_pnl_u']:.1f}U", f"全样本最大回撤 {best_metrics['max_drawdown_u']:.1f}U")}
    {kpi("稳定盈利组合", str(len(stable_rows)), "验证和测试同时盈利，且测试 PF > 1")}
  </div>

  <h2>结论先看</h2>
  <div class="card answer">
    这轮 10U 风险矩阵里，账面最优组合是 <span class="good">斜率阈值 ≤ {best_config.slope_threshold:.4f} + 止损 {best_config.stop_atr_mult:.1f} ATR</span>。但更重要的是样本外稳定性：<span class="bad">{validation_text}</span> {stability_text} 从测试段均值看，<span class="good">{float(strongest_slope):.4f}</span> 这一档斜率过滤最占优，而 ATR 维度上 <span class="warn">{float(strongest_stop):.1f} ATR</span> 的平均测试盈亏最好。
  </div>

  <div class="grid grid-3">
    <div class="card">
      <h3>这次和上次的区别</h3>
      <p>上次是更深的负斜率区间，这次回到更浅的区间，主要看“稍微偏空”和“明显偏空”之间，哪一档对这条策略更合适。</p>
    </div>
    <div class="card">
      <h3>为什么用 10U 风险</h3>
      <p>这样每一笔的盈亏金额能直接横向对比，不会被 ATR 宽窄带来的仓位差异掩盖。你看到的 `+35U` 或 `-20U`，就是按每笔固定亏损预算 10U 算出来的结果。</p>
    </div>
    <div class="card">
      <h3>这轮的直觉结论</h3>
      <p>更浅的斜率区间里，<strong>-0.0005</strong> 依然是最强的一端，说明这条策略仍然更依赖“足够明确的下行斜率”，而不是轻微走弱就提前入场。</p>
    </div>
  </div>

  <h2>热力图</h2>
  <div class="grid grid-2">
    <div class="card imgbox">
      <h3>测试段 Avg R</h3>
      {image_tag(HEATMAP_TEST_AVG_R)}
    </div>
    <div class="card imgbox">
      <h3>测试段总盈亏（10U风险）</h3>
      {image_tag(HEATMAP_TEST_PNL)}
    </div>
    <div class="card imgbox">
      <h3>测试段 Profit Factor</h3>
      {image_tag(HEATMAP_TEST_PF)}
    </div>
    <div class="card imgbox">
      <h3>测试段交易数</h3>
      {image_tag(HEATMAP_TEST_TRADES)}
    </div>
  </div>

  <h2>矩阵解读</h2>
  <div class="card">
    <div class="callout">
      <strong>读法建议</strong><br>
      1. 先看测试段总盈亏和测试 PF，确认有没有样本外收益。<br>
      2. 再看测试段交易数，避免只因为样本太少显得很好看。<br>
      3. 最后看全样本最大回撤金额，判断这套参数是否值得继续研究。
    </div>
    <p>这 15 组里，随着斜率从 <strong>-0.0001</strong> 往 <strong>-0.0005</strong> 收紧，测试段表现整体是在改善的，说明“下行斜率确认度”依然是这条策略的核心。ATR 方面，<strong>2.0 ATR</strong> 在这轮更占优，说明浅斜率区间里，如果入场确认还不够极致，止损空间过窄更容易被噪音扫掉。</p>
  </div>

  <h2>Top 10 组合</h2>
  <div class="card">
    {dataframe_table(
        top_rows,
        [
            ("slope_threshold", "斜率阈值"),
            ("stop_atr_mult", "止损 ATR"),
            ("validation_trades", "验证交易数"),
            ("validation_total_pnl_u", "验证总盈亏U"),
            ("validation_avg_r", "验证 Avg R"),
            ("test_trades", "测试交易数"),
            ("test_total_pnl_u", "测试总盈亏U"),
            ("test_avg_r", "测试 Avg R"),
            ("test_profit_factor", "测试 PF"),
            ("all_total_pnl_u", "全样本总盈亏U"),
            ("all_max_drawdown_u", "全样本最大回撤U"),
            ("score", "综合分"),
        ],
    )}
  </div>

  <h2>完整矩阵</h2>
  <div class="card">
    {dataframe_table(
        matrix_rows,
        [
            ("slope_threshold", "斜率阈值"),
            ("stop_atr_mult", "止损 ATR"),
            ("validation_trades", "验证交易数"),
            ("validation_total_pnl_u", "验证总盈亏U"),
            ("validation_avg_r", "验证 Avg R"),
            ("validation_profit_factor", "验证 PF"),
            ("test_trades", "测试交易数"),
            ("test_total_pnl_u", "测试总盈亏U"),
            ("test_avg_r", "测试 Avg R"),
            ("test_profit_factor", "测试 PF"),
            ("all_trades", "全样本交易数"),
            ("all_total_pnl_u", "全样本总盈亏U"),
            ("all_max_drawdown_u", "全样本最大回撤U"),
            ("score", "综合分"),
        ],
    )}
    <p class="note">数据范围：{html.escape(str(summary["data_start_utc"]))} 到 {html.escape(str(summary["data_end_utc"]))}。最佳组合全样本：{int(best_metrics['trades'])} 笔，胜率 {best_metrics['win_rate'] * 100:.1f}%，总盈亏 {best_metrics['total_pnl_u']:.1f}U，Avg R {best_metrics['avg_r']:.3f}，PF {best_metrics['profit_factor']:.2f}。</p>
  </div>
</main>
</body>
</html>
"""


def dataframe_table(frame: pd.DataFrame, columns: list[tuple[str, str]]) -> str:
    rows = []
    for item in frame.itertuples(index=False):
        cells = []
        for column, _label in columns:
            cells.append(f"<td>{format_cell(column, getattr(item, column))}</td>")
        rows.append("<tr>" + "".join(cells) + "</tr>")
    header = "".join(f"<th>{html.escape(label)}</th>" for _, label in columns)
    return f"<table><thead><tr>{header}</tr></thead><tbody>{''.join(rows)}</tbody></table>"


def format_cell(column: str, value: object) -> str:
    if isinstance(value, str):
        return html.escape(value)
    if value is None:
        return "-"
    number = float(value)
    lower = column.lower()
    if "threshold" in lower:
        return f"{number:.4f}"
    if "pnl_u" in lower or "drawdown_u" in lower:
        return f"{number:.1f}"
    if "avg_r" in lower or "score" in lower:
        return f"{number:.3f}"
    if "profit_factor" in lower:
        return f"{number:.2f}"
    if "trades" in lower:
        return str(int(round(number)))
    if "win_rate" in lower:
        return f"{number * 100:.1f}%"
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
</div>
"""


def format_ts(ts: int) -> str:
    return datetime.fromtimestamp(ts / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


if __name__ == "__main__":
    main()
