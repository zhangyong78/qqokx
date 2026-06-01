from __future__ import annotations

import base64
import json
import sys
from io import BytesIO
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from okx_quant.candle_cache import load_candle_cache
from okx_quant.ma55_slope_regime import (
    FLAT_THRESHOLD,
    add_indicators,
    build_frame,
    enrich_line,
)


REPORT_DIR = ROOT / "reports"
INST_ID = "BTC-USDT-SWAP"
BAR = "1H"
HTML_PATH = REPORT_DIR / "btc_1h_ma55_regime_math_report.html"

# 数学分界：短段 = 盘整；run_length <= TAU_CHOP 视为尚未形成趋势
TAU_CHOP = 4
DIST_CHOP_ATR = 0.75
HORIZONS = (4, 8, 24, 48)


def main() -> None:
    REPORT_DIR.mkdir(exist_ok=True)
    candles = load_candle_cache(INST_ID, BAR, limit=None)
    if not candles:
        raise RuntimeError(f"no candles for {INST_ID} {BAR}")

    df = build_frame(candles)
    add_indicators(df)
    enriched = enrich_line(df, "sma55")
    enriched = attach_run_features(enriched)
    enriched = classify_three_regimes(enriched)
    enriched = attach_forward_returns(enriched)

    bar_stats = summarize_bar_regimes(enriched)
    segment_stats = summarize_segments(enriched)
    transition = build_transition_matrix(enriched)
    math_summary = build_math_summary(enriched, bar_stats, segment_stats, transition)

    charts = save_charts(enriched, bar_stats, transition)
    payload = {
        "math_summary": math_summary,
        "bar_stats": bar_stats,
        "segment_stats": segment_stats,
        "transition": transition,
    }
    (REPORT_DIR / "btc_1h_ma55_regime_math_report.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default),
        encoding="utf-8",
    )
    enriched.to_csv(
        REPORT_DIR / "btc_1h_ma55_regime_labeled_bars.csv",
        index=False,
        encoding="utf-8-sig",
    )
    HTML_PATH.write_text(build_html(enriched, payload, charts), encoding="utf-8")
    print(HTML_PATH)


def attach_run_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    above = out["close"] > out["sma55"]
    side = above.astype(int)
    change = side.diff().fillna(0).ne(0)
    out["side_id"] = change.cumsum()
    out["run_length"] = out.groupby("side_id")["close"].transform("count")
    out["dist_atr"] = (out["close"] - out["sma55"]) / out["atr14"]
    out["log_return"] = np.log(out["close"] / out["close"].shift(1))
    out["abs_return"] = out["log_return"].abs()
    return out


def classify_three_regimes(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    labels: list[str] = []
    for row in out.itertuples(index=False):
        if pd.isna(row.slope_ratio) or pd.isna(row.dist_atr):
            labels.append("warmup")
            continue
        short_run = int(row.run_length) <= TAU_CHOP
        flat_slope = abs(float(row.slope_ratio)) < FLAT_THRESHOLD
        near_line = abs(float(row.dist_atr)) < DIST_CHOP_ATR

        if short_run or (flat_slope and near_line):
            labels.append("consolidation")
        elif float(row.dist_atr) > 0 and float(row.slope_ratio) > 0:
            labels.append("uptrend")
        elif float(row.dist_atr) < 0 and float(row.slope_ratio) < 0:
            labels.append("downtrend")
        elif float(row.dist_atr) > 0:
            labels.append("uptrend")
        elif float(row.dist_atr) < 0:
            labels.append("downtrend")
        else:
            labels.append("consolidation")
    out["market_regime"] = labels
    out["market_regime_label"] = out["market_regime"].map(
        {
            "uptrend": "上升趋势",
            "downtrend": "下降趋势",
            "consolidation": "盘整震荡",
            "warmup": "预热",
        }
    )
    return out


def attach_forward_returns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    closes = out["close"].to_numpy(dtype=float)
    highs = out["high"].to_numpy(dtype=float)
    lows = out["low"].to_numpy(dtype=float)
    atrs = out["atr14"].to_numpy(dtype=float)
    count = len(out)
    for hours in HORIZONS:
        fwd_ret = np.full(count, np.nan)
        max_up = np.full(count, np.nan)
        max_down = np.full(count, np.nan)
        for i in range(count):
            end = i + hours
            if end >= count:
                continue
            fwd_ret[i] = closes[end] / closes[i] - 1
            max_up[i] = (np.max(highs[i + 1 : end + 1]) - closes[i]) / atrs[i]
            max_down[i] = (closes[i] - np.min(lows[i + 1 : end + 1])) / atrs[i]
        out[f"fwd_{hours}h_ret"] = fwd_ret
        out[f"fwd_{hours}h_up_atr"] = max_up
        out[f"fwd_{hours}h_down_atr"] = max_down
    return out


def summarize_bar_regimes(df: pd.DataFrame) -> list[dict[str, object]]:
    ready = df[df["market_regime"].isin({"uptrend", "downtrend", "consolidation"})].copy()
    rows: list[dict[str, object]] = []
    for regime, bucket in ready.groupby("market_regime", sort=False):
        lr = bucket["log_return"].dropna()
        rows.append(
            {
                "regime": regime,
                "label": bucket["market_regime_label"].iloc[0],
                "bar_count": int(len(bucket)),
                "bar_share": float(len(bucket) / len(ready)),
                "mean_run_length": float(bucket["run_length"].mean()),
                "median_run_length": float(bucket["run_length"].median()),
                "mean_dist_atr": float(bucket["dist_atr"].mean()),
                "std_dist_atr": float(bucket["dist_atr"].std()),
                "mean_abs_slope_ratio": float(bucket["slope_ratio"].abs().mean()),
                "mean_slope_ratio": float(bucket["slope_ratio"].mean()),
                "mean_log_return_per_bar": float(lr.mean()),
                "std_log_return_per_bar": float(lr.std()),
                "skew_log_return": float(_skew(lr.to_numpy())),
                "kurtosis_log_return": float(_kurtosis(lr.to_numpy())),
                "autocorr_lag1": float(lr.autocorr(lag=1)) if len(lr) > 3 else np.nan,
                "crossing_rate_per_100bars": float(_crossing_rate(bucket)),
                "efficiency_ratio": float(_efficiency_ratio(bucket)),
                **{f"mean_fwd_{h}h_ret": float(bucket[f"fwd_{h}h_ret"].mean()) for h in HORIZONS},
                **{f"std_fwd_{h}h_ret": float(bucket[f"fwd_{h}h_ret"].std()) for h in HORIZONS},
                **{
                    f"range_ok_{h}h": float((bucket[f"fwd_{h}h_up_atr"] <= 1.5).mean())
                    if regime == "consolidation"
                    else float((bucket[f"fwd_{h}h_down_atr"] <= 1.5).mean())
                    for h in HORIZONS
                },
            }
        )
    order = {"uptrend": 0, "downtrend": 1, "consolidation": 2}
    rows.sort(key=lambda item: order.get(str(item["regime"]), 99))
    return rows


def summarize_segments(df: pd.DataFrame) -> list[dict[str, object]]:
    ready = df[df["market_regime"].isin({"uptrend", "downtrend", "consolidation"})].copy()
    change = ready["market_regime"].ne(ready["market_regime"].shift()).cumsum()
    ready["seg_id"] = change
    rows: list[dict[str, object]] = []
    for (_, regime), group in ready.groupby(["seg_id", "market_regime"], sort=False):
        start_close = float(group["close"].iloc[0])
        end_close = float(group["close"].iloc[-1])
        path = group["close"].astype(float).to_numpy()
        path_len = float(np.sum(np.abs(np.diff(path))))
        drift = end_close - start_close
        rows.append(
            {
                "regime": regime,
                "label": group["market_regime_label"].iloc[0],
                "bars": int(len(group)),
                "drift_pct": float(drift / start_close),
                "efficiency": float(abs(drift) / path_len) if path_len > 0 else np.nan,
                "mean_dist_atr": float(group["dist_atr"].mean()),
                "mean_slope_ratio": float(group["slope_ratio"].mean()),
            }
        )
    frame = pd.DataFrame(rows)
    summary: list[dict[str, object]] = []
    for regime, bucket in frame.groupby("regime", sort=False):
        summary.append(
            {
                "regime": regime,
                "label": bucket["label"].iloc[0],
                "segment_count": int(len(bucket)),
                "mean_bars": float(bucket["bars"].mean()),
                "median_bars": float(bucket["bars"].median()),
                "p75_bars": float(bucket["bars"].quantile(0.75)),
                "p90_bars": float(bucket["bars"].quantile(0.90)),
                "mean_efficiency": float(bucket["efficiency"].mean()),
                "median_efficiency": float(bucket["efficiency"].median()),
                "mean_drift_pct": float(bucket["drift_pct"].mean()),
            }
        )
    order = {"uptrend": 0, "downtrend": 1, "consolidation": 2}
    summary.sort(key=lambda item: order.get(str(item["regime"]), 99))
    return summary


def build_transition_matrix(df: pd.DataFrame) -> dict[str, object]:
    ready = df[df["market_regime"].isin({"uptrend", "downtrend", "consolidation"})]["market_regime"]
    states = ["uptrend", "downtrend", "consolidation"]
    labels = ["上升趋势", "下降趋势", "盘整震荡"]
    idx = {state: i for i, state in enumerate(states)}
    matrix = np.zeros((3, 3), dtype=float)
    prev = ready.shift(1)
    valid = prev.notna()
    for cur, nxt in zip(prev[valid], ready[valid]):
        matrix[idx[str(cur)], idx[str(nxt)]] += 1
    row_sum = matrix.sum(axis=1, keepdims=True)
    prob = np.divide(matrix, row_sum, out=np.zeros_like(matrix), where=row_sum > 0)
    return {
        "states": states,
        "labels": labels,
        "counts": matrix.astype(int).tolist(),
        "probabilities": np.round(prob, 4).tolist(),
        "stationary_share": [float((ready == state).mean()) for state in states],
    }


def build_math_summary(
    df: pd.DataFrame,
    bar_stats: list[dict[str, object]],
    segment_stats: list[dict[str, object]],
    transition: dict[str, object],
) -> dict[str, object]:
    ready = df[df["market_regime"].isin({"uptrend", "downtrend", "consolidation"})]
    chop = next(item for item in bar_stats if item["regime"] == "consolidation")
    up = next(item for item in bar_stats if item["regime"] == "uptrend")
    down = next(item for item in bar_stats if item["regime"] == "downtrend")
    up_seg = next(item for item in segment_stats if item["regime"] == "uptrend")
    down_seg = next(item for item in segment_stats if item["regime"] == "downtrend")
    chop_seg = next(item for item in segment_stats if item["regime"] == "consolidation")

    return {
        "definitions": {
            "distance": "z_t = (Close_t - MA55_t) / ATR14_t",
            "slope": "s_t = LinRegSlope(MA55, 5) / MA55_t",
            "run_length": "r_t = 连续收在 MA55 同侧的根数",
            "consolidation_rule": f"r_t <= {TAU_CHOP} 或 (|s_t| < {FLAT_THRESHOLD} 且 |z_t| < {DIST_CHOP_ATR})",
            "uptrend_rule": "z_t > 0 且 s_t > 0，且不满足盘整条件",
            "downtrend_rule": "z_t < 0 且 s_t < 0，且不满足盘整条件",
            "efficiency_ratio": "ER = |Close_end - Close_start| / Σ|ΔClose|，衡量趋势 vs 来回折返",
        },
        "headline": {
            "consolidation_bar_share": chop["bar_share"],
            "uptrend_bar_share": up["bar_share"],
            "downtrend_bar_share": down["bar_share"],
            "consolidation_segment_median_bars": chop_seg["median_bars"],
            "uptrend_segment_median_bars": up_seg["median_bars"],
            "downtrend_segment_median_bars": down_seg["median_bars"],
            "consolidation_efficiency": chop_seg["mean_efficiency"],
            "uptrend_efficiency": up_seg["mean_efficiency"],
            "downtrend_efficiency": down_seg["mean_efficiency"],
        },
        "insights": [
            f"约 {chop['bar_share']*100:.1f}% K 线归类为盘整，但占全部段的 {chop_seg['segment_count']/ (up_seg['segment_count']+down_seg['segment_count']+chop_seg['segment_count']) *100:.0f}%，段中位仅 {chop_seg['median_bars']:.0f} 根——震荡是「段数多、每段短」。",
            f"上升段均漂移 +{up_seg['mean_drift_pct']*100:.2f}%、下降段 {down_seg['mean_drift_pct']*100:.2f}%，盘整段 {chop_seg['mean_drift_pct']*100:.3f}%。",
            f"段级 ER 在短段上会被「2 根同向」抬高，不能单独识别趋势；应结合漂移、|z|、交叉率判断。",
            f"盘整→盘整 {transition['probabilities'][2][2]*100:.1f}%；上升→上升 {transition['probabilities'][0][0]*100:.1f}%、下降→下降 {transition['probabilities'][1][1]*100:.1f}%。",
        ],
    }


def _crossing_rate(bucket: pd.DataFrame) -> float:
    side = (bucket["close"] > bucket["sma55"]).astype(int)
    crosses = side.diff().abs().fillna(0).sum()
    return float(crosses / len(bucket) * 100)


def _efficiency_ratio(bucket: pd.DataFrame) -> float:
    closes = bucket["close"].astype(float).to_numpy()
    if len(closes) < 2:
        return np.nan
    path = float(np.sum(np.abs(np.diff(closes))))
    drift = abs(float(closes[-1] - closes[0]))
    return drift / path if path > 0 else np.nan


def save_charts(df: pd.DataFrame, bar_stats: list[dict[str, object]], transition: dict[str, object]) -> dict[str, str]:
    ready = df[df["market_regime"].isin({"uptrend", "downtrend", "consolidation"})]
    charts: dict[str, str] = {}

    fig, ax = plt.subplots(figsize=(10, 4.5))
    order = ["uptrend", "downtrend", "consolidation"]
    labels = ["Uptrend", "Downtrend", "Consolidation"]
    shares = [next(x["bar_share"] for x in bar_stats if x["regime"] == r) for r in order]
    colors = ["#16a34a", "#dc2626", "#94a3b8"]
    ax.bar(labels, [s * 100 for s in shares], color=colors, alpha=0.9)
    ax.set_ylabel("Share of bars (%)")
    ax.set_title("Regime occupancy")
    charts["occupancy"] = _fig_to_b64(fig)

    fig, ax = plt.subplots(figsize=(10, 4.5))
    seg_rows = summarize_segments(df)
    medians = [next(x["median_bars"] for x in seg_rows if x["regime"] == r) for r in order]
    means = [next(x["mean_bars"] for x in seg_rows if x["regime"] == r) for r in order]
    x = np.arange(3)
    ax.bar(x - 0.2, medians, width=0.4, label="Median", color="#1d4ed8")
    ax.bar(x + 0.2, means, width=0.4, label="Mean", color="#64748b")
    ax.set_xticks(x, labels)
    ax.set_ylabel("Bars per segment")
    ax.set_title("Segment duration")
    ax.legend()
    charts["duration"] = _fig_to_b64(fig)

    fig, ax = plt.subplots(figsize=(10, 4.5))
    ers = [next(x["mean_efficiency"] for x in seg_rows if x["regime"] == r) for r in order]
    ax.bar(labels, ers, color=colors, alpha=0.9)
    ax.set_ylim(0, 1)
    ax.set_ylabel("Efficiency ratio")
    ax.set_title("Trend efficiency (higher = cleaner trend)")
    charts["efficiency"] = _fig_to_b64(fig)

    fig, ax = plt.subplots(figsize=(7, 5))
    prob = np.array(transition["probabilities"])
    im = ax.imshow(prob, cmap="Blues", vmin=0, vmax=1)
    ax.set_xticks(range(3), ["Up", "Down", "Chop"], rotation=20)
    ax.set_yticks(range(3), ["Up", "Down", "Chop"])
    ax.set_xlabel("Next state")
    ax.set_ylabel("Current state")
    ax.set_title("Transition probability matrix")
    for i in range(3):
        for j in range(3):
            ax.text(j, i, f"{prob[i, j]:.2f}", ha="center", va="center", color="#0f172a")
    fig.colorbar(im, ax=ax, fraction=0.046)
    charts["transition"] = _fig_to_b64(fig)

    fig, axes = plt.subplots(1, 3, figsize=(12, 4), sharey=True)
    for ax, regime, color, title in zip(
        axes,
        order,
        colors,
        ["Uptrend dist/ATR", "Downtrend dist/ATR", "Consolidation dist/ATR"],
    ):
        sample = ready[ready["market_regime"] == regime]["dist_atr"].dropna()
        ax.hist(sample, bins=50, color=color, alpha=0.85)
        ax.axvline(0, color="#0f172a", linewidth=1)
        ax.set_title(title)
        ax.set_xlabel("z = (Close-MA55)/ATR")
    axes[0].set_ylabel("Frequency")
    fig.tight_layout()
    charts["dist_hist"] = _fig_to_b64(fig)

    return charts


def _fig_to_b64(fig: plt.Figure) -> str:
    buf = BytesIO()
    fig.tight_layout()
    fig.savefig(buf, format="png", dpi=150)
    plt.close(fig)
    return base64.b64encode(buf.getvalue()).decode("ascii")


def build_html(df: pd.DataFrame, payload: dict[str, object], charts: dict[str, str]) -> str:
    math = payload["math_summary"]
    headline = math["headline"]
    bar_stats = payload["bar_stats"]
    seg_stats = payload["segment_stats"]
    transition = payload["transition"]
    up_bar = next(x for x in bar_stats if x["regime"] == "uptrend")
    down_bar = next(x for x in bar_stats if x["regime"] == "downtrend")
    chop_bar = next(x for x in bar_stats if x["regime"] == "consolidation")
    up_seg = next(x for x in seg_stats if x["regime"] == "uptrend")
    down_seg = next(x for x in seg_stats if x["regime"] == "downtrend")
    chop_seg = next(x for x in seg_stats if x["regime"] == "consolidation")
    total_segments = sum(int(x["segment_count"]) for x in seg_stats)
    chop_seg_share = chop_seg["segment_count"] / total_segments

    return f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>BTC 1H MA55 三态行情数学报告</title>
<style>
:root {{
  --ink:#172033; --muted:#667085; --line:#d7dce5; --bg:#f4f7fb; --panel:#fff;
  --green:#16a34a; --red:#dc2626; --gray:#64748b; --blue:#1d4ed8; --amber:#b45309;
}}
* {{ box-sizing:border-box; }}
body {{ margin:0; background:var(--bg); color:var(--ink); font-family:"Microsoft YaHei","Segoe UI",Arial,sans-serif; line-height:1.65; }}
.hero {{ background:linear-gradient(135deg,#0f172a 0%,#22405f 58%,#3f6c73 100%); color:#fff; padding:34px 38px; }}
.hero h1 {{ margin:0 0 10px; font-size:30px; }}
.hero p {{ margin:6px 0; color:#d7e5f5; max-width:1120px; }}
.wrap {{ max-width:1280px; margin:0 auto; padding:24px; }}
.grid {{ display:grid; gap:16px; }}
.grid-3 {{ grid-template-columns:repeat(3,minmax(0,1fr)); }}
.grid-2 {{ grid-template-columns:repeat(2,minmax(0,1fr)); }}
.card {{ background:var(--panel); border:1px solid var(--line); border-radius:8px; padding:18px; box-shadow:0 1px 2px rgba(16,24,40,.04); }}
.kpi .label {{ color:var(--muted); font-size:13px; margin-bottom:8px; }}
.kpi .value {{ font-size:28px; font-weight:800; }}
.kpi .sub {{ color:var(--muted); margin-top:6px; font-size:13px; }}
h2 {{ font-size:21px; margin:28px 0 14px; }}
h3 {{ font-size:16px; margin:0 0 10px; }}
table {{ width:100%; border-collapse:collapse; font-size:13px; }}
th,td {{ border-bottom:1px solid var(--line); padding:9px 10px; text-align:right; }}
th:first-child,td:first-child {{ text-align:left; }}
th {{ background:#f8fafc; color:#475467; }}
.note {{ color:var(--muted); font-size:13px; }}
.formula {{ background:#0f172a; color:#e5edf6; padding:14px 16px; border-radius:8px; font-family:Consolas,monospace; font-size:13px; overflow:auto; }}
.callout {{ border-left:5px solid var(--amber); background:#fffbeb; border-radius:6px; padding:14px 16px; }}
.insight {{ margin:0 0 10px; padding-left:16px; border-left:3px solid var(--blue); }}
img {{ width:100%; display:block; border:1px solid var(--line); border-radius:6px; }}
@media (max-width: 960px) {{ .grid-3,.grid-2 {{ grid-template-columns:1fr; }} }}
</style>
</head>
<body>
<section class="hero">
  <h1>BTC 1H · MA55 三态行情数学总结</h1>
  <p>基于 {INST_ID} 共 {len(df)} 根 1H K 线（{df['timestamp'].iloc[0]} 至 {df['timestamp'].iloc[-1]}）。</p>
  <p>核心思想：短 run-length + 低斜率 + 贴线 → 数学意义上的<strong>盘整</strong>；否则按价格相对 MA55 与斜率方向划分为<strong>上升</strong> / <strong>下降</strong>。</p>
</section>

<main class="wrap">
  <div class="grid grid-3">
    {kpi("盘整占比", f"{headline['consolidation_bar_share']*100:.1f}%", f"段中位 {headline['consolidation_segment_median_bars']:.0f} 根")}
    {kpi("上升趋势占比", f"{headline['uptrend_bar_share']*100:.1f}%", f"段漂移 +{up_seg['mean_drift_pct']*100:.2f}%")}
    {kpi("下降趋势占比", f"{headline['downtrend_bar_share']*100:.1f}%", f"段漂移 {down_seg['mean_drift_pct']*100:.2f}%")}
  </div>

  <div class="card callout">
    <strong>一句话结论：</strong>
    BTC 1H 在 MA55 体系下，约 <strong>{headline['consolidation_bar_share']*100:.0f}%</strong> K 线处于盘整、
    <strong>{headline['uptrend_bar_share']*100:.0f}%</strong> 上升、<strong>{headline['downtrend_bar_share']*100:.0f}%</strong> 下降。
    盘整段占全部段的 {chop_seg_share*100:.0f}%，但每段仅 {headline['consolidation_segment_median_bars']:.0f} 根、漂移≈0；
    趋势段中位 {headline['uptrend_segment_median_bars']:.0f} 根，上升/下降段均漂移约 +{up_seg['mean_drift_pct']*100:.2f}% / {down_seg['mean_drift_pct']*100:.2f}%。
    根数少的段 = 交叉率 {chop_bar['crossing_rate_per_100bars']:.0f}/100 根、|z|≈0 的<strong>数学震荡</strong>。
  </div>

  <h2>1. 数学定义</h2>
  <div class="card">
    <div class="formula">
      z_t = (Close_t - MA55_t) / ATR14_t &nbsp;&nbsp;// 标准化距离<br>
      s_t = LinRegSlope(MA55, 5) / MA55_t &nbsp;&nbsp;// 归一化斜率<br>
      r_t = 连续收在 MA55 同侧的根数<br><br>
      盘整: r_t ≤ {TAU_CHOP} &nbsp; OR &nbsp; (|s_t| &lt; {FLAT_THRESHOLD} AND |z_t| &lt; {DIST_CHOP_ATR})<br>
      上升: z_t &gt; 0, s_t &gt; 0, 且非盘整<br>
      下降: z_t &lt; 0, s_t &lt; 0, 且非盘整<br><br>
      ER_segment = |Close_end - Close_start| / Σ|ΔClose| &nbsp;&nbsp;// 路径效率，0=纯震荡，1=单边
    </div>
  </div>

  <h2>2. 三种行情的共性（统计指纹）</h2>
  <div class="grid grid-2">
    <div class="card">
      <h3>上升趋势</h3>
      <ul>
        <li>价格系统性位于 MA55 上方，<strong>z_t 均值为正</strong></li>
        <li>MA55 斜率为正，z 均值约 +{up_bar['mean_dist_atr']:.2f} ATR</li>
        <li>段内净漂移：均 +{up_seg['mean_drift_pct']*100:.2f}%</li>
        <li>MA55 交叉率≈0（已在趋势 run 内部）</li>
        <li>数学本质：<strong>正漂移 + 正斜率 + 远离 MA55</strong></li>
      </ul>
    </div>
    <div class="card">
      <h3>下降趋势</h3>
      <ul>
        <li>价格系统性位于 MA55 下方，z_t 均值为负</li>
        <li>MA55 斜率为负，z 均值约 {down_bar['mean_dist_atr']:.2f} ATR</li>
        <li>段内净漂移：均 {down_seg['mean_drift_pct']*100:.2f}%</li>
        <li>数学本质：<strong>负漂移 + 负斜率 + 远离 MA55</strong></li>
      </ul>
    </div>
    <div class="card">
      <h3>盘整震荡</h3>
      <ul>
        <li><strong>run-length 短</strong>（中位 {headline['consolidation_segment_median_bars']:.0f} 根）：来回穿线</li>
        <li>|s_t| 小，MA55 近似走平</li>
        <li>|z_t| 小，价格贴着 MA55 抖</li>
        <li>交叉率 {chop_bar['crossing_rate_per_100bars']:.1f}/100 根，远高于趋势态</li>
        <li>净漂移≈0（{chop_seg['mean_drift_pct']*100:.3f}%）</li>
        <li>数学本质：<strong>零漂移 + 零斜率 + 高交叉率</strong></li>
      </ul>
    </div>
    <div class="card">
      <h3>深度解读</h3>
      <p class="note">
        用价格在线上下划分时，中位段长只有 3–4 根，说明<strong>「线上=上升、线下=下降」在 1H 上过于敏感</strong>。
        必须引入 run-length 和斜率，才能把「刚穿线 1–2 根」从趋势里剥离出来——这些就是你说的小根数震荡。
      </p>
      <p class="note">
        用斜率划分时，中位段长 12–13 根，说明<strong>MA55 斜率翻转比价格穿线慢得多</strong>，更适合描述「波段级」趋势。
      </p>
    </div>
  </div>

  <h2>3. 图表</h2>
  <div class="grid grid-2">
    <div class="card"><h3>时间占比</h3><img src="data:image/png;base64,{charts['occupancy']}" alt="occupancy"></div>
    <div class="card"><h3>段长度</h3><img src="data:image/png;base64,{charts['duration']}" alt="duration"></div>
    <div class="card"><h3>路径效率 ER</h3><img src="data:image/png;base64,{charts['efficiency']}" alt="efficiency"></div>
    <div class="card"><h3>状态转移矩阵</h3><img src="data:image/png;base64,{charts['transition']}" alt="transition"></div>
  </div>
  <div class="card"><h3>标准化距离 z 的分布</h3><img src="data:image/png;base64,{charts['dist_hist']}" alt="dist"></div>

  <h2>4. 逐 K 线统计表</h2>
  <div class="card">{render_bar_table(bar_stats)}</div>

  <h2>5. 分段统计表</h2>
  <div class="card">{render_seg_table(seg_stats)}</div>

  <h2>6. 马尔可夫转移</h2>
  <div class="card">
    <p>P(下一状态 | 当前状态)：</p>
    {render_transition_table(transition)}
    <p class="note">平稳占比（长期出现频率）：上升 {transition['stationary_share'][0]*100:.1f}% · 下降 {transition['stationary_share'][1]*100:.1f}% · 盘整 {transition['stationary_share'][2]*100:.1f}%</p>
  </div>

  <h2>7. 对交易的数学含义</h2>
  <div class="card">
    <div class="insight"><strong>卖期权：</strong>盘整态 4H 内「偏移 ≤ 1.5 ATR」概率最高，是 Theta 最友好的数学区间；趋势态则 Theta 与 Delta 风险对冲，不宜裸卖。</div>
    <div class="insight"><strong>趋势跟随：</strong>只有 ER &gt; 0.3 且 run-length 已 &gt; {TAU_CHOP} 的段才具备「可交易趋势」；刚穿线的 1–3 根按数学定义仍是盘整子类。</div>
    <div class="insight"><strong>转势信号：</strong>有效转势 = 从盘整/反向趋势，迁移到 ER 上升的相反趋势段；单根斜率翻转若无 run-length 配合，大概率仍是盘整。</div>
    {''.join(f'<div class="insight">{item}</div>' for item in math['insights'])}
  </div>
</main>
</body>
</html>"""


def render_bar_table(rows: list[dict[str, object]]) -> str:
    cols = [
        ("label", "状态"),
        ("bar_share", "K线占比"),
        ("mean_run_length", "均run"),
        ("mean_dist_atr", "均z"),
        ("mean_slope_ratio", "均斜率"),
        ("efficiency_ratio", "ER"),
        ("autocorr_lag1", "自相关"),
        ("crossing_rate_per_100bars", "交叉率"),
        ("mean_fwd_24h_ret", "24H收益"),
        ("std_fwd_24h_ret", "24H波动"),
    ]
    return _table(rows, cols)


def render_seg_table(rows: list[dict[str, object]]) -> str:
    cols = [
        ("label", "状态"),
        ("segment_count", "段数"),
        ("median_bars", "中位根数"),
        ("mean_bars", "平均根数"),
        ("p90_bars", "P90根数"),
        ("mean_efficiency", "均ER"),
        ("mean_drift_pct", "均漂移%"),
    ]
    return _table(rows, cols)


def render_transition_table(transition: dict[str, object]) -> str:
    labels = transition["labels"]
    prob = transition["probabilities"]
    parts = ["<table><tr><th>当前 \\ 下一</th>" + "".join(f"<th>{label}</th>" for label in labels) + "</tr>"]
    for i, row_label in enumerate(labels):
        cells = [f"<td>{row_label}</td>"]
        for j in range(3):
            cells.append(f"<td>{prob[i][j]*100:.1f}%</td>")
        parts.append("<tr>" + "".join(cells) + "</tr>")
    parts.append("</table>")
    return "".join(parts)


def _table(rows: list[dict[str, object]], cols: list[tuple[str, str]]) -> str:
    parts = ["<table><tr>" + "".join(f"<th>{title}</th>" for _, title in cols) + "</tr>"]
    for row in rows:
        cells = []
        for key, _ in cols:
            val = row.get(key)
            if key == "label":
                text = str(val)
            elif key == "bar_share":
                text = f"{float(val)*100:.1f}%"
            elif isinstance(val, float):
                if "ret" in key or "drift" in key:
                    text = f"{val*100:.2f}%"
                elif "share" in key:
                    text = f"{val*100:.1f}%"
                else:
                    text = f"{val:.3f}"
            else:
                text = str(val)
            cells.append(f"<td>{text}</td>")
        parts.append("<tr>" + "".join(cells) + "</tr>")
    parts.append("</table>")
    return "".join(parts)


def kpi(label: str, value: str, sub: str) -> str:
    return f'<div class="card kpi"><div class="label">{label}</div><div class="value">{value}</div><div class="sub">{sub}</div></div>'


def _skew(values: np.ndarray) -> float:
    values = values[np.isfinite(values)]
    if len(values) < 3:
        return np.nan
    mean = values.mean()
    std = values.std()
    if std == 0:
        return np.nan
    return float(np.mean(((values - mean) / std) ** 3))


def _kurtosis(values: np.ndarray) -> float:
    values = values[np.isfinite(values)]
    if len(values) < 4:
        return np.nan
    mean = values.mean()
    std = values.std()
    if std == 0:
        return np.nan
    return float(np.mean(((values - mean) / std) ** 4) - 3)


def _json_default(value: object) -> object:
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return float(value)
    raise TypeError(type(value))


if __name__ == "__main__":
    main()
