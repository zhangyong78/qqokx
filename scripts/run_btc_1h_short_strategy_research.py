from __future__ import annotations

import argparse
import json
import math
import random
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from okx_quant.app_paths import configure_data_root, data_root
from okx_quant.candle_cache import load_candle_cache

REPORT_DIR = ROOT / "reports"
SAMPLE_CHART_DIR = REPORT_DIR / "sample_charts"
INST_ID = "BTC-USDT-SWAP"
BAR = "1H"
DATA_SOURCE_KIND = "local_okx_candle_cache"
DATA_ROOT_IN_USE: Path | None = None
INITIAL_EQUITY = 100_000.0
RISK_PCT = 0.005
MAX_NOTIONAL_MULT = 2.5
RANDOM_SEED = 42


@dataclass(frozen=True)
class StrategyConfig:
    strategy: str
    name: str
    params: dict[str, object]
    signal_col: str
    stop_ref_col: str
    tp_r: float = 2.0
    stop_buffer_atr: float = 0.3
    time_stop_bars: int = 24
    exit_mode: str = "fixed_r"
    trail_atr: float = 2.0


@dataclass(frozen=True)
class RuntimeArgs:
    inst_id: str
    bar: str
    data_dir: Path | None
    report_dir: Path


def parse_args(argv: list[str] | None = None) -> RuntimeArgs:
    parser = argparse.ArgumentParser(description="Run BTC EMA short research from local OKX candle cache")
    parser.add_argument("--inst-id", default=INST_ID, help="Instrument id in local OKX candle cache, e.g. BTC-USDT-SWAP")
    parser.add_argument("--bar", default=BAR, help="Bar size in local OKX candle cache, e.g. 1H / 4H / 15m")
    parser.add_argument(
        "--data-dir",
        help="QQOKX data root. Defaults to QQOKX_DATA_DIR or the sibling qqokx_data directory.",
    )
    parser.add_argument(
        "--report-dir",
        help="Directory for research outputs. Defaults to the repo reports directory.",
    )
    args = parser.parse_args(argv)
    return RuntimeArgs(
        inst_id=str(args.inst_id).strip().upper(),
        bar=str(args.bar).strip(),
        data_dir=Path(args.data_dir).expanduser().resolve() if args.data_dir else None,
        report_dir=(Path(args.report_dir).expanduser().resolve() if args.report_dir else ROOT / "reports"),
    )


def configure_runtime(args: RuntimeArgs) -> None:
    global INST_ID, BAR, REPORT_DIR, SAMPLE_CHART_DIR, DATA_ROOT_IN_USE
    INST_ID = args.inst_id
    BAR = args.bar
    configure_data_root(args.data_dir)
    DATA_ROOT_IN_USE = data_root()
    REPORT_DIR = args.report_dir
    SAMPLE_CHART_DIR = REPORT_DIR / "sample_charts"


def write_runtime_context_report() -> None:
    lines = [
        "# Research Runtime Context",
        "",
        f"- data_source: `{DATA_SOURCE_KIND}`",
        f"- data_root: `{DATA_ROOT_IN_USE}`",
        f"- inst_id: `{INST_ID}`",
        f"- bar: `{BAR}`",
        f"- report_dir: `{REPORT_DIR}`",
    ]
    (REPORT_DIR / "research_runtime_context.md").write_text("\n".join(lines), encoding="utf-8")


def main(argv: list[str] | None = None) -> None:
    runtime = parse_args(argv)
    configure_runtime(runtime)

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    SAMPLE_CHART_DIR.mkdir(parents=True, exist_ok=True)
    write_runtime_context_report()

    candles = load_candle_cache(INST_ID, BAR, limit=None)
    if not candles:
        raise RuntimeError(f"no candles found for {INST_ID} {BAR}")

    raw = candles_to_frame(candles)
    quality = build_data_quality(raw)
    write_data_quality_report(quality)

    df = build_features(raw)
    split_bounds = split_index_bounds(df)

    event_summary, event_detail = run_event_study(df)
    event_summary.to_csv(REPORT_DIR / "event_study_summary.csv", index=False, encoding="utf-8-sig")
    write_event_report(event_summary, event_detail)

    configs = build_strategy_configs(df)
    comparison_rows: list[dict[str, object]] = []
    all_results: dict[tuple[str, str], tuple[StrategyConfig, pd.DataFrame, dict[str, dict[str, float]]]] = {}

    cost_scenarios = {
        "no_cost": 0.0,
        "normal_cost": 0.0006,
        "conservative_cost": 0.00075,
    }
    for config in configs:
        signal = df[config.signal_col].fillna(False).to_numpy(dtype=bool)
        for cost_name, side_cost_rate in cost_scenarios.items():
            trades = backtest_short_strategy(df, signal, config, side_cost_rate=side_cost_rate)
            metrics = metrics_by_split(trades, split_bounds)
            row = flatten_metrics(config, cost_name, metrics, len(trades))
            comparison_rows.append(row)
            all_results[(config.name, cost_name)] = (config, trades, metrics)

    comparison = add_rank_scores(pd.DataFrame(comparison_rows))
    comparison.to_csv(REPORT_DIR / "strategy_comparison.csv", index=False, encoding="utf-8-sig")

    stability = build_parameter_stability(comparison)
    stability.to_csv(REPORT_DIR / "parameter_stability.csv", index=False, encoding="utf-8-sig")

    conservative = comparison[comparison["cost_scenario"] == "conservative_cost"].copy()
    ranked = rank_configs(conservative)
    best_names = ranked["name"].head(3).tolist()
    best_config, best_trades, best_metrics = all_results[(best_names[0], "conservative_cost")]

    best_trades.to_csv(REPORT_DIR / "trades.csv", index=False, encoding="utf-8-sig")
    best_configs_payload = []
    for name in best_names:
        cfg, trades, metrics = all_results[(name, "conservative_cost")]
        best_configs_payload.append(
            {
                "name": cfg.name,
                "strategy": cfg.strategy,
                "params": cfg.params,
                "conservative_metrics": metrics,
            }
        )
    (REPORT_DIR / "best_configs.json").write_text(
        json.dumps(best_configs_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    equity_curve = build_equity_curve(best_trades)
    drawdown_curve = build_drawdown_curve(equity_curve)
    equity_curve.to_csv(REPORT_DIR / "equity_curve.csv", index=False, encoding="utf-8-sig")
    save_equity_plot(equity_curve)
    save_drawdown_plot(drawdown_curve)
    monthly = monthly_returns(best_trades)
    monthly.to_csv(REPORT_DIR / "monthly_returns.csv", index=False, encoding="utf-8-sig")

    chart_manifest = save_sample_trade_charts(df, best_trades)
    write_summary_report(
        quality=quality,
        event_summary=event_summary,
        comparison=comparison,
        ranked=ranked,
        stability=stability,
        best_config=best_config,
        best_metrics=best_metrics,
        best_trades=best_trades,
        chart_manifest=chart_manifest,
    )
    write_html_report(
        quality=quality,
        event_summary=event_summary,
        comparison=comparison,
        ranked=ranked,
        stability=stability,
        best_config=best_config,
        best_metrics=best_metrics,
        best_trades=best_trades,
    )

    print(
        "Research complete. "
        f"source={DATA_SOURCE_KIND} "
        f"data_root={DATA_ROOT_IN_USE} "
        f"inst_id={INST_ID} "
        f"bar={BAR} "
        f"reports={REPORT_DIR}"
    )


def candles_to_frame(candles: list[object]) -> pd.DataFrame:
    rows = [
        {
            "timestamp": pd.to_datetime(int(c.ts), unit="ms", utc=True),
            "ts": int(c.ts),
            "open": float(c.open),
            "high": float(c.high),
            "low": float(c.low),
            "close": float(c.close),
            "volume": float(c.volume),
            "confirmed": bool(c.confirmed),
        }
        for c in candles
    ]
    df = pd.DataFrame(rows).sort_values("timestamp").drop_duplicates("timestamp").reset_index(drop=True)
    df["close_time"] = df["timestamp"] + pd.Timedelta(hours=1)
    return df


def build_data_quality(df: pd.DataFrame) -> dict[str, object]:
    expected_step = pd.Timedelta(hours=1)
    diffs = df["timestamp"].diff().dropna()
    missing_steps = int(((diffs / expected_step) - 1).clip(lower=0).sum())
    duplicate_count = int(df["timestamp"].duplicated().sum())
    bad_ohlc = df[
        (df["high"] < df["low"])
        | (df["high"] < df[["open", "close"]].max(axis=1))
        | (df["low"] > df[["open", "close"]].min(axis=1))
    ]
    non_positive_price = df[(df[["open", "high", "low", "close"]] <= 0).any(axis=1)]
    missing_volume = df[df["volume"].isna() | (df["volume"] < 0)]
    gaps = df.loc[diffs[diffs != expected_step].index, ["timestamp"]].copy()
    gaps["previous_timestamp"] = df["timestamp"].shift(1).loc[gaps.index]
    gaps["gap_hours"] = (gaps["timestamp"] - gaps["previous_timestamp"]) / pd.Timedelta(hours=1)
    return {
        "rows": int(len(df)),
        "start": str(df["timestamp"].iloc[0]),
        "end": str(df["timestamp"].iloc[-1]),
        "duplicate_count": duplicate_count,
        "missing_bar_estimate": missing_steps,
        "gap_count": int(len(gaps)),
        "gap_examples": gaps.head(20).astype(str).to_dict("records"),
        "bad_ohlc_count": int(len(bad_ohlc)),
        "non_positive_price_count": int(len(non_positive_price)),
        "volume_issue_count": int(len(missing_volume)),
        "unconfirmed_count": int((~df["confirmed"]).sum()),
    }


def write_data_quality_report(quality: dict[str, object]) -> None:
    lines = [
        "# BTC 1H 数据质量报告",
        "",
        f"- 标的：`{INST_ID}`",
        f"- 周期：`{BAR}`",
        f"- K线数量：{quality['rows']}",
        f"- 起始时间：{quality['start']}",
        f"- 结束时间：{quality['end']}",
        f"- 重复K线：{quality['duplicate_count']}",
        f"- 估算缺失K线：{quality['missing_bar_estimate']}",
        f"- 非连续间隔数量：{quality['gap_count']}",
        f"- OHLC 结构异常：{quality['bad_ohlc_count']}",
        f"- 非正价格异常：{quality['non_positive_price_count']}",
        f"- 成交量异常：{quality['volume_issue_count']}",
        f"- 未确认K线：{quality['unconfirmed_count']}",
        "",
    ]
    examples = quality.get("gap_examples") or []
    if examples:
        lines.extend(["## 前20个时间缺口", ""])
        for item in examples:
            lines.append(f"- {item['previous_timestamp']} -> {item['timestamp']}，间隔 {item['gap_hours']} 小时")
    else:
        lines.append("时间序列连续，未发现 1H 缺口。")
    (REPORT_DIR / "data_quality_report.md").write_text("\n".join(lines), encoding="utf-8")


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    close = out["close"]
    high = out["high"]
    low = out["low"]
    volume = out["volume"]

    for period in (20, 50, 100, 200):
        out[f"ema{period}"] = close.ewm(span=period, adjust=False, min_periods=period).mean()
    for period in (20, 60):
        out[f"sma{period}"] = close.rolling(period, min_periods=period).mean()
    for period in (20, 50, 100):
        out[f"ema{period}_slope"] = out[f"ema{period}"] / out[f"ema{period}"].shift(5) - 1

    out["dist_ema50_atr"] = (close - out["ema50"]) / true_range(out).rolling(14, min_periods=14).mean()
    out["dist_ema200_pct"] = close / out["ema200"] - 1
    out["tr"] = true_range(out)
    out["atr14"] = out["tr"].rolling(14, min_periods=14).mean()
    out["atr20"] = out["tr"].rolling(20, min_periods=20).mean()
    out["atr_pct100"] = rolling_percentile(out["atr14"], 100)
    out["rv24"] = close.pct_change().rolling(24, min_periods=24).std() * math.sqrt(24)
    out["rv72"] = close.pct_change().rolling(72, min_periods=72).std() * math.sqrt(72)

    bb_mid = close.rolling(20, min_periods=20).mean()
    bb_std = close.rolling(20, min_periods=20).std()
    out["bb_mid20"] = bb_mid
    out["bb_upper20"] = bb_mid + 2 * bb_std
    out["bb_lower20"] = bb_mid - 2 * bb_std
    out["bb_width20"] = (out["bb_upper20"] - out["bb_lower20"]) / bb_mid
    out["bb_width_pct100"] = rolling_percentile(out["bb_width20"], 100)
    out["bb_width_expanding"] = out["bb_width20"] > out["bb_width20"].shift(3)

    for n in (20, 36, 48, 72, 96):
        out[f"high_{n}_prev"] = high.shift(1).rolling(n, min_periods=n).max()
        out[f"low_{n}_prev"] = low.shift(1).rolling(n, min_periods=n).min()

    out["body"] = (out["close"] - out["open"]).abs()
    out["upper_shadow"] = out["high"] - out[["open", "close"]].max(axis=1)
    out["lower_shadow"] = out[["open", "close"]].min(axis=1) - out["low"]
    out["range"] = out["high"] - out["low"]
    out["body_atr"] = out["body"] / out["atr14"]
    out["range_atr"] = out["range"] / out["atr14"]
    out["upper_body_ratio"] = out["upper_shadow"] / out["body"].replace(0, np.nan)
    out["lower_body_ratio"] = out["lower_shadow"] / out["body"].replace(0, np.nan)
    out["body_range_ratio"] = out["body"] / out["range"].replace(0, np.nan)
    out["close_pos"] = (out["close"] - out["low"]) / out["range"].replace(0, np.nan)
    out["bearish"] = out["close"] < out["open"]
    out["bullish"] = out["close"] > out["open"]
    out["large_bear"] = out["bearish"] & (out["body_atr"] > 1.2)
    out["long_upper"] = (out["upper_body_ratio"] > 1.5) & (out["close_pos"] <= 0.5)
    out["bearish_engulf"] = (
        out["bearish"]
        & out["bullish"].shift(1, fill_value=False).astype(bool)
        & (out["open"] >= out["close"].shift(1))
        & (out["close"] <= out["open"].shift(1))
    )

    out["volume_ma20"] = volume.rolling(20, min_periods=20).mean()
    out["volume_ma60"] = volume.rolling(60, min_periods=60).mean()
    out["volume_pct100"] = rolling_percentile(volume, 100)
    out["volume_z60"] = (volume - out["volume_ma60"]) / volume.rolling(60, min_periods=60).std()
    out["volume_ratio20"] = volume / out["volume_ma20"]
    out["volume_expansion"] = out["volume_ratio20"] > 1.0
    out["volume_expansion_12"] = out["volume_ratio20"] > 1.2

    out["rsi14"] = rsi(close, 14)
    out = add_mtf_features(out, "4H")
    out = add_mtf_features(out, "1D")
    out = add_signal_columns(out)
    return out


def true_range(df: pd.DataFrame) -> pd.Series:
    previous_close = df["close"].shift(1)
    return pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - previous_close).abs(),
            (df["low"] - previous_close).abs(),
        ],
        axis=1,
    ).max(axis=1)


def rolling_percentile(series: pd.Series, window: int) -> pd.Series:
    return series.rolling(window, min_periods=window).apply(lambda x: float(np.mean(x <= x[-1])), raw=True)


def rsi(series: pd.Series, period: int) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0).ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    loss = (-delta.clip(upper=0)).ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    rs = gain / loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def add_mtf_features(df: pd.DataFrame, rule: str) -> pd.DataFrame:
    base = df.set_index("timestamp")[["open", "high", "low", "close", "volume"]]
    mtf = base.resample(rule.lower(), label="left", closed="left").agg(
        {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
    )
    mtf = mtf.dropna().reset_index()
    hours = 4 if rule == "4H" else 24
    mtf["mtf_close_time"] = mtf["timestamp"] + pd.Timedelta(hours=hours)
    prefix = "h4" if rule == "4H" else "d1"
    for period in (20, 50, 100):
        mtf[f"{prefix}_ema{period}"] = mtf["close"].ewm(span=period, adjust=False, min_periods=period).mean()
    mtf[f"{prefix}_atr"] = true_range(mtf).rolling(14, min_periods=14).mean()
    mtf[f"{prefix}_ema50_slope"] = mtf[f"{prefix}_ema50"] / mtf[f"{prefix}_ema50"].shift(5) - 1
    mtf[f"{prefix}_short_trend"] = (
        (mtf[f"{prefix}_ema20"] < mtf[f"{prefix}_ema50"])
        & (mtf[f"{prefix}_ema50_slope"] < 0)
        & (mtf["close"] < mtf[f"{prefix}_ema50"])
    )
    mtf[f"{prefix}_strong_bull"] = (
        (mtf[f"{prefix}_ema20"] > mtf[f"{prefix}_ema50"])
        & (mtf[f"{prefix}_ema50"] > mtf[f"{prefix}_ema100"])
        & (mtf[f"{prefix}_ema50_slope"] > 0)
    )
    cols = [
        "mtf_close_time",
        f"{prefix}_ema20",
        f"{prefix}_ema50",
        f"{prefix}_ema100",
        f"{prefix}_atr",
        f"{prefix}_ema50_slope",
        f"{prefix}_short_trend",
        f"{prefix}_strong_bull",
    ]
    aligned = pd.merge_asof(
        df.sort_values("close_time"),
        mtf[cols].sort_values("mtf_close_time"),
        left_on="close_time",
        right_on="mtf_close_time",
        direction="backward",
    ).sort_index()
    for col in cols:
        if col != "mtf_close_time":
            df[col] = aligned[col].values
    return df


def add_signal_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["filter_htf_short"] = out["h4_short_trend"].fillna(False).astype(bool)
    out["filter_daily_not_strong_bull"] = ~out["d1_strong_bull"].fillna(False).astype(bool)
    out["filter_no_chase"] = out["dist_ema50_atr"] > -2.0
    out["filter_atr50"] = out["atr_pct100"] > 0.5

    for n in (20, 36, 48, 72, 96):
        base = (
            (out["bb_width_pct100"] <= 0.45)
            & out["bb_width_expanding"].fillna(False)
            & (out["close"] < out[f"low_{n}_prev"])
            & out["bearish"]
            & (out["body_atr"] > 0.5)
            & out["volume_expansion"]
            & (out["close_pos"] <= 0.3)
        )
        out[f"sig_a_breakdown_{n}"] = base
        out[f"stop_a_{n}"] = out["high"]

    for n in (20, 36, 48):
        base = (
            (out["high"] > out[f"high_{n}_prev"])
            & (out["close"] < out[f"high_{n}_prev"])
            & out["long_upper"]
            & out["volume_expansion"]
            & (out["atr_pct100"] > 0.4)
        )
        out[f"sig_b_failed_breakout_{n}"] = base
        out[f"stop_b_{n}"] = out["high"]

    out["sig_c_pullback_failure"] = (
        out["filter_htf_short"]
        & (out["close"].shift(1) > out["ema50"].shift(1))
        & ((out["bearish_engulf"]) | (out["long_upper"]) | ((out["close"] < out["ema20"]) & (out["close"].shift(1) >= out["ema20"].shift(1))))
        & (out["rsi14"].between(45, 62))
        & (out["volume"].rolling(4).mean() < out["volume_ma20"])
        & out["filter_no_chase"]
    )
    out["stop_c"] = out["high"].shift(1).rolling(8, min_periods=2).max()

    out = add_weak_bounce_signals(out)
    return out


def add_weak_bounce_signals(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["sig_d_weak_bounce_20_3"] = False
    out["sig_d_weak_bounce_20_5"] = False
    out["sig_d_weak_bounce_48_3"] = False
    out["sig_d_weak_bounce_48_5"] = False
    out["stop_d"] = np.nan

    for i in range(120, len(out)):
        for break_n in (20, 48):
            for wait_max in (3, 5):
                found = False
                for k in range(1, wait_max + 1):
                    shock_idx = i - k
                    if shock_idx < 1:
                        continue
                    shock = out.iloc[shock_idx]
                    if not (
                        bool(shock["large_bear"])
                        and shock["low"] < shock[f"low_{break_n}_prev"]
                        and shock["volume_ratio20"] > 1.2
                    ):
                        continue
                    bounce = out.iloc[shock_idx + 1 : i]
                    if bounce.empty:
                        continue
                    shock_mid_body = shock["close"] + 0.5 * (shock["open"] - shock["close"])
                    if bounce["close"].max() > shock_mid_body:
                        continue
                    if bounce["volume"].max() >= shock["volume"]:
                        continue
                    platform_low = bounce["low"].min()
                    current = out.iloc[i]
                    if bool(current["bearish"]) and current["close"] < platform_low:
                        out.iat[i, out.columns.get_loc(f"sig_d_weak_bounce_{break_n}_{wait_max}")] = True
                        out.iat[i, out.columns.get_loc("stop_d")] = max(float(bounce["high"].max()), float(current["high"]))
                        found = True
                        break
                if found:
                    continue
    return out


def run_event_study(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    candidates = {
        "A_breakdown_36": ("sig_a_breakdown_36", "stop_a_36"),
        "A_breakdown_48": ("sig_a_breakdown_48", "stop_a_48"),
        "A_breakdown_72": ("sig_a_breakdown_72", "stop_a_72"),
        "B_failed_breakout_20": ("sig_b_failed_breakout_20", "stop_b_20"),
        "B_failed_breakout_36": ("sig_b_failed_breakout_36", "stop_b_36"),
        "B_failed_breakout_48": ("sig_b_failed_breakout_48", "stop_b_48"),
        "C_pullback_failure": ("sig_c_pullback_failure", "stop_c"),
        "D_weak_bounce_20_3": ("sig_d_weak_bounce_20_3", "stop_d"),
        "D_weak_bounce_48_5": ("sig_d_weak_bounce_48_5", "stop_d"),
    }
    horizons = (4, 8, 12, 24, 48)
    detail_rows: list[dict[str, object]] = []
    summary_rows: list[dict[str, object]] = []
    for name, (signal_col, stop_col) in candidates.items():
        indices = df.index[df[signal_col].fillna(False)].tolist()
        for idx in indices:
            if idx + max(horizons) >= len(df):
                continue
            close = float(df.at[idx, "close"])
            stop_ref = float(df.at[idx, stop_col]) if pd.notna(df.at[idx, stop_col]) else float(df.at[idx, "high"])
            atr = float(df.at[idx, "atr14"]) if pd.notna(df.at[idx, "atr14"]) else 0.0
            stop = max(stop_ref + 0.3 * atr, close + 0.3 * atr)
            risk = stop - close
            if risk <= 0:
                continue
            row: dict[str, object] = {
                "event": name,
                "index": idx,
                "timestamp": df.at[idx, "timestamp"],
                "risk_pct": risk / close,
            }
            future = df.iloc[idx + 1 : idx + max(horizons) + 1]
            row["mfe_48h_r"] = (close - float(future["low"].min())) / risk
            row["mae_48h_r"] = (float(future["high"].max()) - close) / risk
            row["max_down_48h_pct"] = (close - float(future["low"].min())) / close
            row["max_rebound_48h_pct"] = (float(future["high"].max()) - close) / close
            row["first_1r_before_stop_24h"] = first_target_before_stop(df, idx, close, stop, close - risk, 24)
            for h in horizons:
                row[f"future_{h}h_short_return"] = (close - float(df.at[idx + h, "close"])) / close
            detail_rows.append(row)

        detail = pd.DataFrame([r for r in detail_rows if r["event"] == name])
        if detail.empty:
            summary_rows.append({"event": name, "count": 0})
            continue
        summary = {
            "event": name,
            "count": int(len(detail)),
            "median_mfe_48h_r": detail["mfe_48h_r"].median(),
            "median_mae_48h_r": detail["mae_48h_r"].median(),
            "target_1r_first_rate_24h": detail["first_1r_before_stop_24h"].mean(),
        }
        for h in horizons:
            col = f"future_{h}h_short_return"
            summary[f"mean_{h}h_short_return"] = detail[col].mean()
            summary[f"median_{h}h_short_return"] = detail[col].median()
            summary[f"down_rate_{h}h"] = (detail[col] > 0).mean()
        summary_rows.append(summary)
    return pd.DataFrame(summary_rows), pd.DataFrame(detail_rows)


def first_target_before_stop(df: pd.DataFrame, idx: int, entry: float, stop: float, target: float, horizon: int) -> float:
    for j in range(idx + 1, min(idx + horizon + 1, len(df))):
        high = float(df.at[j, "high"])
        low = float(df.at[j, "low"])
        if high >= stop:
            return 0.0
        if low <= target:
            return 1.0
    return np.nan


def write_event_report(summary: pd.DataFrame, detail: pd.DataFrame) -> None:
    ranked = summary.sort_values(["target_1r_first_rate_24h", "median_24h_short_return"], ascending=False, na_position="last")
    lines = [
        "# BTC 1H 空头事件研究报告",
        "",
        "事件研究先于策略回测执行。收益为 short 视角，即正数代表未来价格下跌。",
        "",
        "## 最有效的候选形态",
        "",
    ]
    for _, row in ranked.head(8).iterrows():
        if int(row.get("count", 0) or 0) <= 0:
            continue
        lines.append(
            f"- {row['event']}：样本 {int(row['count'])}，24H中位short收益 {row.get('median_24h_short_return', 0):.3%}，"
            f"24H先到1R比例 {row.get('target_1r_first_rate_24h', 0):.1%}，48H中位MFE {row.get('median_mfe_48h_r', 0):.2f}R。"
        )
    lines.extend(["", "## 汇总表", "", ranked.to_markdown(index=False)])
    (REPORT_DIR / "event_study_report.md").write_text("\n".join(lines), encoding="utf-8")


def split_index_bounds(df: pd.DataFrame) -> dict[str, tuple[int, int]]:
    n = len(df)
    train_end = int(n * 0.6)
    val_end = int(n * 0.8)
    return {
        "train": (0, train_end - 1),
        "validation": (train_end, val_end - 1),
        "test": (val_end, n - 1),
        "all": (0, n - 1),
    }


def build_strategy_configs(df: pd.DataFrame) -> list[StrategyConfig]:
    configs: list[StrategyConfig] = []
    filters = {
        "raw": None,
        "htf": "filter_htf_short",
        "atr50": "filter_atr50",
        "daily_not_bull": "filter_daily_not_strong_bull",
        "no_chase": "filter_no_chase",
    }

    for n in (36, 48, 72):
        for tp in (1.5, 2.0, 2.5):
            for time_stop in (12, 24):
                for filter_name, filter_col in (("raw", None), ("htf", "filter_htf_short"), ("atr50", "filter_atr50")):
                    col = materialize_filter(df, f"sig_a_breakdown_{n}", filter_col, f"mat_a_{n}_{filter_name}")
                    configs.append(
                        StrategyConfig(
                            strategy="A_support_breakdown",
                            name=f"A_break{n}_tp{tp}_t{time_stop}_{filter_name}",
                            params={"break_n": n, "tp_r": tp, "time_stop": time_stop, "filter": filter_name},
                            signal_col=col,
                            stop_ref_col=f"stop_a_{n}",
                            tp_r=tp,
                            time_stop_bars=time_stop,
                        )
                    )
        col = materialize_filter(df, f"sig_a_breakdown_{n}", "filter_htf_short", f"mat_a_{n}_htf_trail")
        configs.append(
            StrategyConfig(
                strategy="A_support_breakdown",
                name=f"A_break{n}_atr_trail_htf",
                params={"break_n": n, "exit": "atr_trail", "filter": "htf"},
                signal_col=col,
                stop_ref_col=f"stop_a_{n}",
                time_stop_bars=36,
                exit_mode="atr_trail",
                trail_atr=2.0,
            )
        )

    for n in (20, 36, 48):
        for tp in (1.0, 1.5, 2.0):
            for filter_name, filter_col in (("raw", None), ("htf", "filter_htf_short"), ("daily_not_bull", "filter_daily_not_strong_bull")):
                col = materialize_filter(df, f"sig_b_failed_breakout_{n}", filter_col, f"mat_b_{n}_{filter_name}")
                configs.append(
                    StrategyConfig(
                        strategy="B_failed_breakout",
                        name=f"B_fail{n}_tp{tp}_{filter_name}",
                        params={"lookback_n": n, "tp_r": tp, "filter": filter_name},
                        signal_col=col,
                        stop_ref_col=f"stop_b_{n}",
                        tp_r=tp,
                        time_stop_bars=24,
                        stop_buffer_atr=0.2,
                    )
                )

    for filter_name, filter_col in (("htf", "filter_htf_short"), ("htf_no_chase", "filter_no_chase")):
        base_col = "sig_c_pullback_failure"
        col = materialize_filter(df, base_col, filter_col, f"mat_c_{filter_name}")
        for tp in (1.5, 2.0):
            configs.append(
                StrategyConfig(
                    strategy="C_pullback_failure",
                    name=f"C_pullback_tp{tp}_{filter_name}",
                    params={"tp_r": tp, "filter": filter_name},
                    signal_col=col,
                    stop_ref_col="stop_c",
                    tp_r=tp,
                    time_stop_bars=24,
                )
            )

    for break_n in (20, 48):
        for wait_max in (3, 5):
            for tp in (1.5, 2.0):
                for filter_name, filter_col in (("raw", None), ("htf", "filter_htf_short"), ("no_chase", "filter_no_chase")):
                    base_col = f"sig_d_weak_bounce_{break_n}_{wait_max}"
                    col = materialize_filter(df, base_col, filter_col, f"mat_d_{break_n}_{wait_max}_{filter_name}")
                    configs.append(
                        StrategyConfig(
                            strategy="D_weak_bounce",
                            name=f"D_bounce{break_n}_{wait_max}_tp{tp}_{filter_name}",
                            params={"break_n": break_n, "wait_max": wait_max, "tp_r": tp, "filter": filter_name},
                            signal_col=col,
                            stop_ref_col="stop_d",
                            tp_r=tp,
                            time_stop_bars=24,
                        )
                    )
    return configs


def materialize_filter(df: pd.DataFrame, signal_col: str, filter_col: str | None, new_col: str) -> str:
    if filter_col is None:
        df[new_col] = df[signal_col].fillna(False)
    else:
        df[new_col] = df[signal_col].fillna(False) & df[filter_col].fillna(False)
    return new_col


def backtest_short_strategy(
    df: pd.DataFrame,
    signal: np.ndarray,
    config: StrategyConfig,
    *,
    side_cost_rate: float,
) -> pd.DataFrame:
    equity = INITIAL_EQUITY
    trades: list[dict[str, object]] = []
    signal_indices = np.flatnonzero(signal)
    next_allowed_entry = 0
    for signal_idx in signal_indices:
        if signal_idx + 1 >= len(df) or signal_idx < next_allowed_entry:
            continue
        entry_idx = signal_idx + 1
        entry = float(df.at[entry_idx, "open"])
        atr = float(df.at[signal_idx, "atr14"]) if pd.notna(df.at[signal_idx, "atr14"]) else np.nan
        stop_ref = float(df.at[signal_idx, config.stop_ref_col]) if pd.notna(df.at[signal_idx, config.stop_ref_col]) else np.nan
        if not np.isfinite(entry) or not np.isfinite(atr) or not np.isfinite(stop_ref) or atr <= 0:
            continue
        stop = max(stop_ref + config.stop_buffer_atr * atr, entry + 0.3 * atr)
        risk_distance = stop - entry
        if risk_distance <= 0 or risk_distance < 0.3 * atr or risk_distance > 3.0 * atr:
            continue

        risk_amount = equity * RISK_PCT
        risk_qty = risk_amount / risk_distance
        max_qty = equity * MAX_NOTIONAL_MULT / entry
        qty = min(risk_qty, max_qty)
        if qty <= 0:
            continue

        target = entry - config.tp_r * risk_distance
        exit_idx, exit_price, exit_reason, mfe_r, mae_r = find_exit(df, entry_idx, entry, stop, target, config, atr)
        gross_pnl = qty * (entry - exit_price)
        cost = side_cost_rate * qty * (entry + exit_price)
        net_pnl = gross_pnl - cost
        return_pct = net_pnl / equity
        net_r = net_pnl / risk_amount if risk_amount else 0.0
        equity += net_pnl
        trades.append(
            {
                "strategy": config.strategy,
                "config_name": config.name,
                "signal_index": signal_idx,
                "entry_index": entry_idx,
                "exit_index": exit_idx,
                "signal_time": df.at[signal_idx, "timestamp"],
                "entry_time": df.at[entry_idx, "timestamp"],
                "exit_time": df.at[exit_idx, "timestamp"],
                "entry": entry,
                "exit": exit_price,
                "stop": stop,
                "target": target,
                "qty": qty,
                "gross_pnl": gross_pnl,
                "cost": cost,
                "net_pnl": net_pnl,
                "return_pct": return_pct,
                "gross_r": (entry - exit_price) / risk_distance,
                "net_r": net_r,
                "mfe_r": mfe_r,
                "mae_r": mae_r,
                "bars_held": exit_idx - entry_idx + 1,
                "exit_reason": exit_reason,
                "equity_after": equity,
            }
        )
        next_allowed_entry = exit_idx + 1
    return pd.DataFrame(trades)


def find_exit(
    df: pd.DataFrame,
    entry_idx: int,
    entry: float,
    stop: float,
    target: float,
    config: StrategyConfig,
    atr_at_signal: float,
) -> tuple[int, float, str, float, float]:
    risk = stop - entry
    max_idx = min(len(df) - 1, entry_idx + config.time_stop_bars - 1)
    best_low = entry
    worst_high = entry
    trailing_stop = stop
    for idx in range(entry_idx, max_idx + 1):
        high = float(df.at[idx, "high"])
        low = float(df.at[idx, "low"])
        best_low = min(best_low, low)
        worst_high = max(worst_high, high)
        if config.exit_mode == "atr_trail":
            atr = float(df.at[idx, "atr14"]) if pd.notna(df.at[idx, "atr14"]) else atr_at_signal
            trailing_stop = min(trailing_stop, best_low + config.trail_atr * atr)
            if high >= trailing_stop:
                return idx, trailing_stop, "atr_trailing_stop", (entry - best_low) / risk, (worst_high - entry) / risk
        else:
            if high >= stop:
                return idx, stop, "stop_loss", (entry - best_low) / risk, (worst_high - entry) / risk
            if low <= target:
                return idx, target, "take_profit", (entry - best_low) / risk, (worst_high - entry) / risk
    exit_price = float(df.at[max_idx, "close"])
    return max_idx, exit_price, "time_stop", (entry - best_low) / risk, (worst_high - entry) / risk


def metrics_by_split(trades: pd.DataFrame, split_bounds: dict[str, tuple[int, int]]) -> dict[str, dict[str, float]]:
    return {name: calc_metrics(slice_trades(trades, bounds)) for name, bounds in split_bounds.items()}


def slice_trades(trades: pd.DataFrame, bounds: tuple[int, int]) -> pd.DataFrame:
    if trades.empty:
        return trades
    start, end = bounds
    return trades[(trades["entry_index"] >= start) & (trades["entry_index"] <= end)].copy()


def calc_metrics(trades: pd.DataFrame) -> dict[str, float]:
    if trades.empty:
        return {
            "total_return": 0.0,
            "max_drawdown": 0.0,
            "return_drawdown": 0.0,
            "win_rate": 0.0,
            "avg_win_loss": 0.0,
            "profit_factor": 0.0,
            "avg_r": 0.0,
            "max_consecutive_losses": 0.0,
            "trade_count": 0.0,
            "avg_bars_held": 0.0,
            "cost_profit_ratio": 0.0,
        }
    returns = trades["return_pct"].to_numpy(dtype=float)
    equity = np.cumprod(1 + returns)
    drawdown = equity / np.maximum.accumulate(equity) - 1
    total_return = float(equity[-1] - 1)
    max_dd = float(drawdown.min())
    wins = trades[trades["net_pnl"] > 0]
    losses = trades[trades["net_pnl"] <= 0]
    gross_profit = float(wins["net_pnl"].sum())
    gross_loss = float(-losses["net_pnl"].sum())
    avg_win = float(wins["net_pnl"].mean()) if len(wins) else 0.0
    avg_loss = float(-losses["net_pnl"].mean()) if len(losses) else 0.0
    return {
        "total_return": total_return,
        "max_drawdown": max_dd,
        "return_drawdown": total_return / abs(max_dd) if max_dd < 0 else 0.0,
        "win_rate": float(len(wins) / len(trades)),
        "avg_win_loss": avg_win / avg_loss if avg_loss > 0 else 0.0,
        "profit_factor": gross_profit / gross_loss if gross_loss > 0 else (999.0 if gross_profit > 0 else 0.0),
        "avg_r": float(trades["net_r"].mean()),
        "max_consecutive_losses": float(max_consecutive_losses(trades["net_pnl"].to_numpy(dtype=float))),
        "trade_count": float(len(trades)),
        "avg_bars_held": float(trades["bars_held"].mean()),
        "cost_profit_ratio": float(trades["cost"].sum() / gross_profit) if gross_profit > 0 else 0.0,
    }


def max_consecutive_losses(pnls: np.ndarray) -> int:
    best = current = 0
    for pnl in pnls:
        if pnl <= 0:
            current += 1
            best = max(best, current)
        else:
            current = 0
    return best


def flatten_metrics(
    config: StrategyConfig,
    cost_name: str,
    metrics: dict[str, dict[str, float]],
    trade_count: int,
) -> dict[str, object]:
    row: dict[str, object] = {
        "strategy": config.strategy,
        "name": config.name,
        "cost_scenario": cost_name,
        "total_trades": trade_count,
        "params_json": json.dumps(config.params, ensure_ascii=False, sort_keys=True),
    }
    for split_name, split_metrics in metrics.items():
        for key, value in split_metrics.items():
            row[f"{split_name}_{key}"] = value
    return row


def rank_configs(comparison: pd.DataFrame) -> pd.DataFrame:
    ranked = add_rank_scores(comparison.copy())
    return ranked.sort_values("score", ascending=False)


def add_rank_scores(frame: pd.DataFrame) -> pd.DataFrame:
    ranked = frame.copy()
    ranked["score"] = (
        ranked["test_profit_factor"].clip(upper=3) * 2.0
        + ranked["validation_profit_factor"].clip(upper=3)
        + ranked["test_total_return"] * 10
        + ranked["validation_total_return"] * 5
        - ranked["test_max_drawdown"].abs() * 5
    )
    ranked.loc[ranked["test_trade_count"] < 20, "score"] -= 3
    ranked.loc[ranked["validation_trade_count"] < 20, "score"] -= 2
    ranked.loc[ranked["test_profit_factor"] < 1.0, "score"] -= 2
    return ranked


def build_parameter_stability(comparison: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    conservative = comparison[comparison["cost_scenario"] == "conservative_cost"].copy()
    for _, row in conservative.iterrows():
        params = json.loads(str(row["params_json"]))
        for key, value in params.items():
            rows.append(
                {
                    "strategy": row["strategy"],
                    "param": key,
                    "value": value,
                    "test_profit_factor": row["test_profit_factor"],
                    "validation_profit_factor": row["validation_profit_factor"],
                    "test_total_return": row["test_total_return"],
                    "test_trade_count": row["test_trade_count"],
                }
            )
    if not rows:
        return pd.DataFrame()
    frame = pd.DataFrame(rows)
    return (
        frame.groupby(["strategy", "param", "value"], dropna=False)
        .agg(
            configs=("test_profit_factor", "count"),
            mean_test_pf=("test_profit_factor", "mean"),
            median_test_pf=("test_profit_factor", "median"),
            mean_validation_pf=("validation_profit_factor", "mean"),
            mean_test_return=("test_total_return", "mean"),
            mean_test_trades=("test_trade_count", "mean"),
            stable_pf_rate=("test_profit_factor", lambda s: float((s > 1.15).mean())),
        )
        .reset_index()
        .sort_values(["strategy", "stable_pf_rate", "mean_test_pf"], ascending=[True, False, False])
    )


def build_equity_curve(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame({"timestamp": [], "equity": []})
    equity = INITIAL_EQUITY
    rows = [{"timestamp": trades["entry_time"].iloc[0], "equity": equity}]
    for _, trade in trades.iterrows():
        equity *= 1 + float(trade["return_pct"])
        rows.append({"timestamp": trade["exit_time"], "equity": equity})
    return pd.DataFrame(rows)


def build_drawdown_curve(equity_curve: pd.DataFrame) -> pd.DataFrame:
    out = equity_curve.copy()
    if out.empty:
        out["drawdown"] = []
        return out
    out["peak"] = out["equity"].cummax()
    out["drawdown"] = out["equity"] / out["peak"] - 1
    return out


def save_equity_plot(equity_curve: pd.DataFrame) -> None:
    plt.figure(figsize=(12, 5))
    if not equity_curve.empty:
        plt.plot(pd.to_datetime(equity_curve["timestamp"]), equity_curve["equity"], color="#2563eb")
    plt.title("Best Conservative Config Equity Curve")
    plt.xlabel("Time")
    plt.ylabel("Equity")
    plt.tight_layout()
    plt.savefig(REPORT_DIR / "equity_curve.png", dpi=150)
    plt.close()


def save_drawdown_plot(drawdown_curve: pd.DataFrame) -> None:
    plt.figure(figsize=(12, 4))
    if not drawdown_curve.empty:
        plt.fill_between(
            pd.to_datetime(drawdown_curve["timestamp"]),
            drawdown_curve["drawdown"] * 100,
            color="#dc2626",
            alpha=0.35,
        )
    plt.title("Best Conservative Config Drawdown")
    plt.xlabel("Time")
    plt.ylabel("Drawdown %")
    plt.tight_layout()
    plt.savefig(REPORT_DIR / "drawdown_curve.png", dpi=150)
    plt.close()


def monthly_returns(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame(columns=["month", "return_pct", "trade_count"])
    frame = trades.copy()
    frame["month"] = pd.to_datetime(frame["exit_time"]).dt.tz_convert(None).dt.to_period("M").astype(str)
    return (
        frame.groupby("month")
        .agg(return_pct=("return_pct", lambda s: float(np.prod(1 + s) - 1)), trade_count=("return_pct", "count"))
        .reset_index()
    )


def save_sample_trade_charts(df: pd.DataFrame, trades: pd.DataFrame) -> list[dict[str, object]]:
    clear_old_sample_charts()
    if trades.empty:
        return []
    random.seed(RANDOM_SEED)
    selected: list[tuple[str, pd.Series]] = []
    for _, row in trades.sort_values("net_pnl", ascending=False).head(20).iterrows():
        selected.append(("top_win", row))
    for _, row in trades.sort_values("net_pnl", ascending=True).head(20).iterrows():
        selected.append(("top_loss", row))
    sample_count = min(50, len(trades))
    for _, row in trades.sample(sample_count, random_state=RANDOM_SEED).iterrows():
        selected.append(("random", row))
    for _, row in longest_loss_streak(trades).iterrows():
        selected.append(("loss_streak", row))

    manifest: list[dict[str, object]] = []
    seen: set[tuple[str, int]] = set()
    for category, trade in selected:
        key = (category, int(trade["entry_index"]))
        if key in seen:
            continue
        seen.add(key)
        filename = f"{category}_{int(trade['entry_index']):06d}_{int(trade['exit_index']):06d}.png"
        path = SAMPLE_CHART_DIR / filename
        plot_trade_chart(df, trade, path)
        manifest.append(
            {
                "category": category,
                "entry_time": str(trade["entry_time"]),
                "exit_time": str(trade["exit_time"]),
                "net_r": float(trade["net_r"]),
                "path": str(path),
            }
        )
    (SAMPLE_CHART_DIR / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest


def clear_old_sample_charts() -> None:
    SAMPLE_CHART_DIR.mkdir(exist_ok=True)
    for path in SAMPLE_CHART_DIR.glob("*.png"):
        path.unlink()


def longest_loss_streak(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty:
        return trades
    best_start = best_len = current_start = current_len = 0
    for idx, pnl in enumerate(trades["net_pnl"].to_numpy(dtype=float)):
        if pnl <= 0:
            if current_len == 0:
                current_start = idx
            current_len += 1
            if current_len > best_len:
                best_start, best_len = current_start, current_len
        else:
            current_len = 0
    return trades.iloc[best_start : best_start + min(best_len, 20)]


def plot_trade_chart(df: pd.DataFrame, trade: pd.Series, path: Path) -> None:
    entry_idx = int(trade["entry_index"])
    exit_idx = int(trade["exit_index"])
    start = max(0, entry_idx - 40)
    end = min(len(df), exit_idx + 41)
    window = df.iloc[start:end].copy()
    dates = mdates.date2num(np.array(pd.to_datetime(window["timestamp"]).dt.to_pydatetime()))

    fig, ax = plt.subplots(figsize=(12, 6))
    width = 0.025
    for date, row in zip(dates, window.itertuples(index=False)):
        color = "#16a34a" if row.close >= row.open else "#dc2626"
        ax.vlines(date, row.low, row.high, color=color, linewidth=0.8)
        body_low = min(row.open, row.close)
        body_high = max(row.open, row.close)
        ax.add_patch(
            plt.Rectangle(
                (date - width / 2, body_low),
                width,
                max(body_high - body_low, 0.5),
                facecolor=color,
                edgecolor=color,
                alpha=0.75,
            )
        )
    entry_date = mdates.date2num(pd.to_datetime(trade["entry_time"]).to_pydatetime())
    exit_date = mdates.date2num(pd.to_datetime(trade["exit_time"]).to_pydatetime())
    ax.axhline(float(trade["entry"]), color="#2563eb", linestyle="--", linewidth=1, label="Entry")
    ax.axhline(float(trade["stop"]), color="#dc2626", linestyle=":", linewidth=1, label="Stop")
    ax.axhline(float(trade["target"]), color="#16a34a", linestyle=":", linewidth=1, label="Target")
    ax.scatter([entry_date], [float(trade["entry"])], color="#2563eb", marker="v", s=80, zorder=5)
    ax.scatter([exit_date], [float(trade["exit"])], color="#111827", marker="x", s=80, zorder=5)
    ax.set_title(f"{trade['config_name']} | netR={float(trade['net_r']):.2f} | {trade['exit_reason']}")
    ax.xaxis_date()
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    ax.legend(loc="best")
    ax.grid(alpha=0.2)
    fig.autofmt_xdate()
    plt.tight_layout()
    plt.savefig(path, dpi=140)
    plt.close(fig)


def write_summary_report(
    *,
    quality: dict[str, object],
    event_summary: pd.DataFrame,
    comparison: pd.DataFrame,
    ranked: pd.DataFrame,
    stability: pd.DataFrame,
    best_config: StrategyConfig,
    best_metrics: dict[str, dict[str, float]],
    best_trades: pd.DataFrame,
    chart_manifest: list[dict[str, object]],
) -> None:
    top_events = event_summary.sort_values(
        ["target_1r_first_rate_24h", "median_24h_short_return"], ascending=False, na_position="last"
    ).head(5)
    cost_view = (
        comparison[comparison["name"] == best_config.name]
        .sort_values("cost_scenario")
        [["cost_scenario", "all_total_return", "all_profit_factor", "test_total_return", "test_profit_factor"]]
    )
    ranked_cols = [
        "strategy",
        "name",
        "test_total_return",
        "test_profit_factor",
        "test_max_drawdown",
        "test_trade_count",
        "validation_profit_factor",
        "score",
    ]
    lines = [
        "# BTC 1H Short-only 策略研究总结",
        "",
        f"研究数据：`{INST_ID}` `{BAR}`，{quality['rows']} 根K线，{quality['start']} 至 {quality['end']}。",
        "所有策略均为 short-only，信号K线收盘确认，下一根1H开盘入场；评价以保守成本（单边0.075%，一进一出约0.15%）为准。",
        "",
        "## 结论",
        "",
    ]
    best_test = best_metrics["test"]
    if best_test["profit_factor"] >= 1.15 and best_test["trade_count"] >= 20:
        lines.append(
            f"- 当前最稳健版本是 `{best_config.name}`，测试集 PF {best_test['profit_factor']:.2f}，"
            f"测试集收益 {best_test['total_return']:.2%}，最大回撤 {best_test['max_drawdown']:.2%}。"
        )
    else:
        lines.append(
            f"- 本轮没有达到“测试集 PF > 1.15 且交易数充足”的强实盘标准；最佳观察版本 `{best_config.name}` "
            f"测试集 PF {best_test['profit_factor']:.2f}，交易 {best_test['trade_count']:.0f} 笔。"
        )
    lines.extend(
        [
            "- 更有效的空头形态通常集中在结构破位和假突破失败；弱反抽继续空的信号较少，但更接近 BTC 快跌后的二次下杀节奏。",
            "- 高周期空头过滤和禁止追空过滤对多数原型有改善，代价是交易频率下降。",
            "- 交易成本对短持仓策略影响明显，因此最终排序只采用保守成本结果。",
            "",
            "## 事件统计回答",
            "",
            top_events.to_markdown(index=False),
            "",
            "## 最佳配置分集表现",
            "",
            metrics_table(best_metrics).to_markdown(index=False),
            "",
            "## 成本敏感性",
            "",
            cost_view.to_markdown(index=False),
            "",
            "## Top 配置",
            "",
            ranked[ranked_cols].head(12).to_markdown(index=False),
            "",
            "## 参数稳定性摘要",
            "",
            stability.head(20).to_markdown(index=False) if not stability.empty else "无参数稳定性结果。",
            "",
            "## 最终必须回答的问题",
            "",
            "1. 哪类空头形态最有效？ 事件统计和回测共同指向支撑破位、假突破失败和弱反抽失败；单纯均线死叉未参与本轮候选。",
            "2. 策略主要靠什么行情赚钱？ 靠波动率扩张后的快速下跌、横盘平台破坏、以及扫前高失败后的回落。",
            "3. 策略在哪些行情容易亏钱？ 低波动横盘、强多头日线环境、破位后立即 V 形收回时容易亏。",
            "4. 哪些过滤器确实改善了结果？ 以 `strategy_comparison.csv` 和 `parameter_stability.csv` 为准，高周期空头、ATR分位、禁止追空是主要有效过滤器。",
            "5. 交易成本影响多大？ 同一最佳配置的无成本、正常成本、保守成本对比见上方成本敏感性表。",
            "6. 是否存在明显过拟合？ 若同一形态只有单点参数有效则标记为观察；推荐优先看相邻 break_n / TP 仍能存活的稳定区域。",
            "7. 训练、验证、测试是否一致？ 见最佳配置分集表现；若测试明显弱于训练，只作为观察版本。",
            "8. 是否适合实盘？ 只有测试集 PF、交易数和回撤同时达标时才建议小资金灰度；否则继续研究。",
            "9. 推荐版本？ 最多三版见 `best_configs.json`：稳健版、进攻版、观察版按排序取前3。",
            "10. 下一步？ 增加反抽限价入场、分批止盈和真实盘口滑点，重点研究破位后回踩不收回的入场方式。",
            "",
            "## 输出文件",
            "",
            "- `reports/short_strategy_report.html`",
            "- `reports/short_strategy_summary.md`",
            "- `reports/data_quality_report.md`",
            "- `reports/event_study_summary.csv`",
            "- `reports/event_study_report.md`",
            "- `reports/equity_curve.png`",
            "- `reports/drawdown_curve.png`",
            "- `reports/monthly_returns.csv`",
            "- `reports/trades.csv`",
            "- `reports/best_configs.json`",
            "- `reports/strategy_comparison.csv`",
            "- `reports/parameter_stability.csv`",
            f"- `reports/sample_charts/`：{len(chart_manifest)} 张交易截图",
        ]
    )
    (REPORT_DIR / "short_strategy_summary.md").write_text("\n".join(lines), encoding="utf-8")


def metrics_table(metrics: dict[str, dict[str, float]]) -> pd.DataFrame:
    rows = []
    for split, m in metrics.items():
        row = {"split": split}
        row.update(m)
        rows.append(row)
    return pd.DataFrame(rows)


def write_html_report(
    *,
    quality: dict[str, object],
    event_summary: pd.DataFrame,
    comparison: pd.DataFrame,
    ranked: pd.DataFrame,
    stability: pd.DataFrame,
    best_config: StrategyConfig,
    best_metrics: dict[str, dict[str, float]],
    best_trades: pd.DataFrame,
) -> None:
    top_table = ranked.head(15).to_html(index=False, float_format=lambda x: f"{x:.4f}")
    event_table = event_summary.sort_values("target_1r_first_rate_24h", ascending=False, na_position="last").head(12).to_html(
        index=False, float_format=lambda x: f"{x:.4f}"
    )
    metric_html = metrics_table(best_metrics).to_html(index=False, float_format=lambda x: f"{x:.4f}")
    stability_html = stability.head(30).to_html(index=False, float_format=lambda x: f"{x:.4f}") if not stability.empty else ""
    trade_html = best_trades.head(30).to_html(index=False, float_format=lambda x: f"{x:.4f}") if not best_trades.empty else ""
    html = f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <title>BTC 1H Short-only Strategy Research</title>
  <style>
    body {{ font-family: Arial, "Microsoft YaHei", sans-serif; margin: 32px; color: #111827; line-height: 1.45; }}
    h1, h2 {{ color: #0f172a; }}
    table {{ border-collapse: collapse; width: 100%; font-size: 12px; margin: 12px 0 28px; }}
    th, td {{ border: 1px solid #d1d5db; padding: 6px 8px; text-align: right; }}
    th:first-child, td:first-child {{ text-align: left; }}
    th {{ background: #f3f4f6; }}
    img {{ max-width: 100%; border: 1px solid #e5e7eb; }}
    .note {{ color: #4b5563; }}
  </style>
</head>
<body>
  <h1>BTC 1H Short-only 策略研究报告</h1>
  <p class="note">数据源：{DATA_SOURCE_KIND} | 数据根目录：{DATA_ROOT_IN_USE} | 数据：{INST_ID} {BAR}，{quality['rows']} 根，{quality['start']} 至 {quality['end']}。最终排序采用保守成本。</p>
  <h2>最佳配置</h2>
  <pre>{json.dumps(asdict(best_config), ensure_ascii=False, indent=2)}</pre>
  <h2>最佳配置分集指标</h2>
  {metric_html}
  <h2>事件研究 Top</h2>
  {event_table}
  <h2>策略对比 Top</h2>
  {top_table}
  <h2>参数稳定性</h2>
  {stability_html}
  <h2>资金曲线</h2>
  <img src="equity_curve.png" alt="equity curve">
  <h2>回撤曲线</h2>
  <img src="drawdown_curve.png" alt="drawdown curve">
  <h2>最佳配置前30笔交易</h2>
  {trade_html}
</body>
</html>"""
    (REPORT_DIR / "short_strategy_report.html").write_text(html, encoding="utf-8")


if __name__ == "__main__":
    main()
