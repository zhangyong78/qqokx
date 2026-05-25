from __future__ import annotations

import base64
import json
import math
import shutil
import sys
from dataclasses import dataclass
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from okx_quant.candle_cache import load_candle_cache
from scripts.run_btc_1h_short_strategy_research import add_rank_scores, build_features, calc_metrics, rank_configs


REPORT_DIR = ROOT / "reports"
INST_ID = "BTC-USDT-SWAP"
BAR = "15m"
BAR_MINUTES = 15
INITIAL_EQUITY = 100_000.0
RISK_PCT = 0.005
MAX_NOTIONAL_MULT = 2.5
INITIAL_CAPITAL_BOOK = 10_000.0
FIXED_RISK_BOOK = 10.0

PRIMARY_HTML = REPORT_DIR / "short_strategy_15m_followup_report.html"
CURRENT_LEADERSHIP_HTML = REPORT_DIR / "short_strategy_leadership_report.html"
ARCHIVE_1H_HTML = REPORT_DIR / "short_strategy_leadership_report_1h_snapshot.html"


@dataclass(frozen=True)
class StrategyConfig:
    name: str
    signal_col: str
    stop_ref_col: str
    params: dict[str, object]
    exit_mode: str
    tp_r: float = 2.0
    stop_buffer_atr: float = 0.3
    time_stop_bars: int = 96
    trail_atr: float = 2.0
    break_even_r: float = 1.0


def main() -> None:
    REPORT_DIR.mkdir(exist_ok=True)
    maybe_archive_existing_leadership_report()

    candles = load_candle_cache(INST_ID, BAR, limit=None)
    if not candles:
        raise RuntimeError(f"no candles found for {INST_ID} {BAR}")

    raw = candles_to_frame(candles, BAR_MINUTES)
    quality = build_data_quality(raw, BAR_MINUTES)
    df = build_features(raw)
    split_bounds = split_index_bounds(df)

    configs = build_configs(df)
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
            metrics = {name: calc_metrics(slice_trades(trades, bounds)) for name, bounds in split_bounds.items()}
            comparison_rows.append(flatten_metrics(config, cost_name, metrics, len(trades)))
            all_results[(config.name, cost_name)] = (config, trades, metrics)

    comparison = add_rank_scores(pd.DataFrame(comparison_rows))
    comparison.to_csv(REPORT_DIR / "short_strategy_15m_comparison.csv", index=False, encoding="utf-8-sig")

    conservative = comparison[comparison["cost_scenario"] == "conservative_cost"].copy()
    ranked = rank_configs(conservative)
    best_name = ranked.iloc[0]["name"]
    best_config, best_trades, best_metrics = all_results[(best_name, "conservative_cost")]
    best_trades.to_csv(REPORT_DIR / "short_strategy_15m_trades.csv", index=False, encoding="utf-8-sig")

    fixed_risk_book = build_fixed_risk_book(best_trades, INITIAL_CAPITAL_BOOK, FIXED_RISK_BOOK)
    yearly = summarize_period(best_trades, "Y")
    monthly = summarize_period(best_trades, "M").tail(24)
    yearly.to_csv(REPORT_DIR / "short_strategy_15m_yearly.csv", index=False, encoding="utf-8-sig")
    monthly.to_csv(REPORT_DIR / "short_strategy_15m_monthly.csv", index=False, encoding="utf-8-sig")

    equity = build_equity(best_trades)
    save_plot(equity, REPORT_DIR / "short_strategy_15m_equity_curve.png", "15m Best Config Equity")
    drawdown = build_drawdown(equity)
    save_drawdown_plot(drawdown, REPORT_DIR / "short_strategy_15m_drawdown_curve.png", "15m Best Config Drawdown")

    html = build_html(
        quality=quality,
        ranked=ranked,
        comparison=comparison,
        best_config=best_config,
        best_metrics=best_metrics,
        best_trades=best_trades,
        fixed_risk_book=fixed_risk_book,
        yearly=yearly,
        monthly=monthly,
    )
    PRIMARY_HTML.write_text(html, encoding="utf-8")
    CURRENT_LEADERSHIP_HTML.write_text(html, encoding="utf-8")

    payload = {
        "best_name": best_name,
        "best_params": best_config.params,
        "best_metrics": best_metrics,
        "fixed_risk_book": fixed_risk_book,
    }
    (REPORT_DIR / "short_strategy_15m_best.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(PRIMARY_HTML)


def maybe_archive_existing_leadership_report() -> None:
    if CURRENT_LEADERSHIP_HTML.exists() and not ARCHIVE_1H_HTML.exists():
        shutil.copy2(CURRENT_LEADERSHIP_HTML, ARCHIVE_1H_HTML)


def candles_to_frame(candles: list[object], bar_minutes: int) -> pd.DataFrame:
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
    df["close_time"] = df["timestamp"] + pd.Timedelta(minutes=bar_minutes)
    return df


def build_data_quality(df: pd.DataFrame, bar_minutes: int) -> dict[str, object]:
    expected_step = pd.Timedelta(minutes=bar_minutes)
    diffs = df["timestamp"].diff().dropna()
    missing_steps = int(((diffs / expected_step) - 1).clip(lower=0).sum())
    bad_ohlc = df[
        (df["high"] < df["low"])
        | (df["high"] < df[["open", "close"]].max(axis=1))
        | (df["low"] > df[["open", "close"]].min(axis=1))
    ]
    return {
        "rows": int(len(df)),
        "start": str(df["timestamp"].iloc[0]),
        "end": str(df["timestamp"].iloc[-1]),
        "duplicate_count": int(df["timestamp"].duplicated().sum()),
        "missing_bar_estimate": missing_steps,
        "bad_ohlc_count": int(len(bad_ohlc)),
        "unconfirmed_count": int((~df["confirmed"]).sum()),
    }


def build_configs(df: pd.DataFrame) -> list[StrategyConfig]:
    configs: list[StrategyConfig] = []
    signal_bases = [
        ("sig_d_weak_bounce_48_3", "stop_d", {"break_n": 48, "wait_max": 3, "variant": "direct"}),
        ("sig_d_weak_bounce_48_5", "stop_d", {"break_n": 48, "wait_max": 5, "variant": "direct"}),
        ("sig_d_weak_bounce_20_3", "stop_d", {"break_n": 20, "wait_max": 3, "variant": "fast"}),
    ]
    for signal_col, stop_col, base_params in signal_bases:
        configs.extend(
            [
                StrategyConfig(
                    name=f"{signal_col}_fixed_2r",
                    signal_col=signal_col,
                    stop_ref_col=stop_col,
                    params={**base_params, "exit_mode": "fixed_2r"},
                    exit_mode="fixed_r",
                    tp_r=2.0,
                    time_stop_bars=96,
                ),
                StrategyConfig(
                    name=f"{signal_col}_fixed_1p5r",
                    signal_col=signal_col,
                    stop_ref_col=stop_col,
                    params={**base_params, "exit_mode": "fixed_1p5r"},
                    exit_mode="fixed_r",
                    tp_r=1.5,
                    time_stop_bars=96,
                ),
                StrategyConfig(
                    name=f"{signal_col}_atr_trail_2p0",
                    signal_col=signal_col,
                    stop_ref_col=stop_col,
                    params={**base_params, "exit_mode": "atr_trail_2.0"},
                    exit_mode="atr_trail",
                    trail_atr=2.0,
                    time_stop_bars=144,
                ),
                StrategyConfig(
                    name=f"{signal_col}_be1r_trail_2p0",
                    signal_col=signal_col,
                    stop_ref_col=stop_col,
                    params={**base_params, "exit_mode": "be1r_trail_2.0"},
                    exit_mode="be_then_trail",
                    trail_atr=2.0,
                    break_even_r=1.0,
                    time_stop_bars=144,
                ),
                StrategyConfig(
                    name=f"{signal_col}_be1r_hold_3r",
                    signal_col=signal_col,
                    stop_ref_col=stop_col,
                    params={**base_params, "exit_mode": "be1r_hold_3r"},
                    exit_mode="be_then_fixed",
                    tp_r=3.0,
                    break_even_r=1.0,
                    time_stop_bars=144,
                ),
            ]
        )
    return configs


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


def backtest_short_strategy(
    df: pd.DataFrame,
    signal: np.ndarray,
    config: StrategyConfig,
    *,
    side_cost_rate: float,
) -> pd.DataFrame:
    equity = INITIAL_EQUITY
    trades: list[dict[str, object]] = []
    next_allowed_entry = 0
    for signal_idx in np.flatnonzero(signal):
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
        qty = min(risk_amount / risk_distance, equity * MAX_NOTIONAL_MULT / entry)
        if qty <= 0:
            continue

        exit_idx, exit_price, exit_reason, mfe_r, mae_r = find_exit(df, entry_idx, entry, stop, risk_distance, config, atr)
        gross_pnl = qty * (entry - exit_price)
        cost = side_cost_rate * qty * (entry + exit_price)
        net_pnl = gross_pnl - cost
        return_pct = net_pnl / equity
        net_r = net_pnl / risk_amount if risk_amount else 0.0
        equity += net_pnl
        trades.append(
            {
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
    risk_distance: float,
    config: StrategyConfig,
    signal_atr: float,
) -> tuple[int, float, str, float, float]:
    max_idx = min(len(df) - 1, entry_idx + config.time_stop_bars - 1)
    best_low = entry
    worst_high = entry
    trailing_stop = stop
    target = entry - config.tp_r * risk_distance
    break_even_active = False

    for idx in range(entry_idx, max_idx + 1):
        high = float(df.at[idx, "high"])
        low = float(df.at[idx, "low"])
        best_low = min(best_low, low)
        worst_high = max(worst_high, high)
        atr = float(df.at[idx, "atr14"]) if pd.notna(df.at[idx, "atr14"]) else signal_atr

        if config.exit_mode == "fixed_r":
            if high >= stop:
                return idx, stop, "stop_loss", (entry - best_low) / risk_distance, (worst_high - entry) / risk_distance
            if low <= target:
                return idx, target, "take_profit", (entry - best_low) / risk_distance, (worst_high - entry) / risk_distance
            continue

        if config.exit_mode == "atr_trail":
            trailing_stop = min(trailing_stop, best_low + config.trail_atr * atr)
            if high >= trailing_stop:
                return idx, trailing_stop, "atr_trail_stop", (entry - best_low) / risk_distance, (worst_high - entry) / risk_distance
            continue

        if config.exit_mode == "be_then_trail":
            if not break_even_active and low <= entry - config.break_even_r * risk_distance:
                break_even_active = True
                trailing_stop = min(trailing_stop, entry)
            if not break_even_active:
                if high >= stop:
                    return idx, stop, "stop_loss", (entry - best_low) / risk_distance, (worst_high - entry) / risk_distance
            else:
                trailing_stop = min(trailing_stop, best_low + config.trail_atr * atr)
                if high >= trailing_stop:
                    reason = "break_even_stop" if trailing_stop <= entry else "trail_after_break_even"
                    return idx, trailing_stop, reason, (entry - best_low) / risk_distance, (worst_high - entry) / risk_distance
            continue

        if config.exit_mode == "be_then_fixed":
            if not break_even_active and low <= entry - config.break_even_r * risk_distance:
                break_even_active = True
            if high >= (entry if break_even_active else stop):
                exit_px = entry if break_even_active else stop
                reason = "break_even_stop" if break_even_active else "stop_loss"
                return idx, exit_px, reason, (entry - best_low) / risk_distance, (worst_high - entry) / risk_distance
            if low <= target:
                return idx, target, "extended_take_profit", (entry - best_low) / risk_distance, (worst_high - entry) / risk_distance

    exit_price = float(df.at[max_idx, "close"])
    return max_idx, exit_price, "time_stop", (entry - best_low) / risk_distance, (worst_high - entry) / risk_distance


def slice_trades(trades: pd.DataFrame, bounds: tuple[int, int]) -> pd.DataFrame:
    if trades.empty:
        return trades
    start, end = bounds
    return trades[(trades["entry_index"] >= start) & (trades["entry_index"] <= end)].copy()


def flatten_metrics(config: StrategyConfig, cost_name: str, metrics: dict[str, dict[str, float]], trade_count: int) -> dict[str, object]:
    row: dict[str, object] = {
        "name": config.name,
        "cost_scenario": cost_name,
        "total_trades": trade_count,
        "params_json": json.dumps(config.params, ensure_ascii=False, sort_keys=True),
    }
    for split_name, split_metrics in metrics.items():
        for key, value in split_metrics.items():
            row[f"{split_name}_{key}"] = value
    return row


def build_fixed_risk_book(trades: pd.DataFrame, initial_capital: float, fixed_risk: float) -> dict[str, float]:
    frame = trades.copy()
    frame["pnl_fixed"] = frame["net_r"].astype(float) * fixed_risk
    equity = initial_capital + frame["pnl_fixed"].cumsum()
    drawdown = equity / equity.cummax() - 1
    return {
        "initial_capital": initial_capital,
        "fixed_risk": fixed_risk,
        "total_trades": int(len(frame)),
        "win_count": int((frame["pnl_fixed"] > 0).sum()),
        "loss_count": int((frame["pnl_fixed"] <= 0).sum()),
        "win_rate": float((frame["pnl_fixed"] > 0).mean()) if len(frame) else 0.0,
        "total_pnl": float(frame["pnl_fixed"].sum()),
        "final_capital": float(initial_capital + frame["pnl_fixed"].sum()),
        "total_return": float(frame["pnl_fixed"].sum() / initial_capital) if initial_capital else 0.0,
        "avg_pnl": float(frame["pnl_fixed"].mean()) if len(frame) else 0.0,
        "max_drawdown": float(drawdown.min()) if len(frame) else 0.0,
    }


def summarize_period(trades: pd.DataFrame, freq: str) -> pd.DataFrame:
    frame = trades.copy()
    frame["exit_time"] = pd.to_datetime(frame["exit_time"], utc=True).dt.tz_convert(None)
    frame["pnl_fixed"] = frame["net_r"].astype(float) * FIXED_RISK_BOOK
    label = "year" if freq == "Y" else "month"
    frame[label] = frame["exit_time"].dt.to_period(freq).astype(str)
    out = (
        frame.groupby(label, as_index=False)
        .agg(
            trades=("pnl_fixed", "count"),
            wins=("pnl_fixed", lambda s: int((s > 0).sum())),
            losses=("pnl_fixed", lambda s: int((s <= 0).sum())),
            pnl=("pnl_fixed", "sum"),
            avg_pnl=("pnl_fixed", "mean"),
            win_rate=("pnl_fixed", lambda s: float((s > 0).mean())),
        )
        .sort_values(label)
    )
    out["end_capital"] = INITIAL_CAPITAL_BOOK + out["pnl"].cumsum()
    return out


def build_equity(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame({"timestamp": [], "equity": []})
    equity = INITIAL_EQUITY
    rows = [{"timestamp": trades["entry_time"].iloc[0], "equity": equity}]
    for _, trade in trades.iterrows():
        equity *= 1 + float(trade["return_pct"])
        rows.append({"timestamp": trade["exit_time"], "equity": equity})
    return pd.DataFrame(rows)


def build_drawdown(equity: pd.DataFrame) -> pd.DataFrame:
    out = equity.copy()
    if out.empty:
        out["drawdown"] = []
        return out
    out["peak"] = out["equity"].cummax()
    out["drawdown"] = out["equity"] / out["peak"] - 1
    return out


def save_plot(frame: pd.DataFrame, path: Path, title: str) -> None:
    plt.figure(figsize=(12, 5))
    if not frame.empty:
        plt.plot(pd.to_datetime(frame["timestamp"]), frame["equity"], color="#0f766e")
    plt.title(title)
    plt.tight_layout()
    plt.savefig(path, dpi=150)
    plt.close()


def save_drawdown_plot(frame: pd.DataFrame, path: Path, title: str) -> None:
    plt.figure(figsize=(12, 4))
    if not frame.empty:
        plt.fill_between(pd.to_datetime(frame["timestamp"]), frame["drawdown"] * 100, color="#dc2626", alpha=0.35)
    plt.title(title)
    plt.tight_layout()
    plt.savefig(path, dpi=150)
    plt.close()


def build_html(
    *,
    quality: dict[str, object],
    ranked: pd.DataFrame,
    comparison: pd.DataFrame,
    best_config: StrategyConfig,
    best_metrics: dict[str, dict[str, float]],
    best_trades: pd.DataFrame,
    fixed_risk_book: dict[str, float],
    yearly: pd.DataFrame,
    monthly: pd.DataFrame,
) -> str:
    cost_rows = comparison[comparison["name"] == best_config.name].sort_values("cost_scenario")
    top = ranked.head(10).copy()
    summary = (
        "15m 上，当前最稳的是动态保本后追踪，"
        if "be1r_trail" in best_config.name
        else "15m 上，当前最稳的是固定目标版本，"
    )
    sample_table = render_table(top, ["name", "test_total_return", "test_profit_factor", "test_win_rate", "test_trade_count"])
    yearly_table = render_period_table(yearly, "year")
    monthly_table = render_period_table(monthly, "month")
    cost_table = render_table(cost_rows, ["cost_scenario", "all_total_return", "all_profit_factor", "test_total_return", "test_profit_factor"])
    split_table = render_metrics_table(best_metrics)
    fixed_book_table = f"""
    <div class="grid grid-4">
      {kpi("总交易次数", str(int(fixed_risk_book['total_trades'])), f"赚钱 {int(fixed_risk_book['win_count'])} / 亏钱 {int(fixed_risk_book['loss_count'])}")}
      {kpi("最后剩余资金", money(fixed_risk_book['final_capital']), f"累计盈亏 {money(fixed_risk_book['total_pnl'])}")}
      {kpi("固定止损", money(fixed_risk_book['fixed_risk']), f"起始资金 {money(fixed_risk_book['initial_capital'])}")}
      {kpi("全样本胜率", pct(fixed_risk_book['win_rate']), f"最大回撤 {pct(fixed_risk_book['max_drawdown'])}")}
    </div>
    """
    best_config_json = json.dumps({"name": best_config.name, "params": best_config.params, "exit_mode": best_config.exit_mode}, ensure_ascii=False, indent=2)
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>BTC 15m 做空逻辑跟进回测</title>
<style>
:root {{
  --ink:#152033; --muted:#667085; --line:#d7dce5; --bg:#f4f7fb; --panel:#ffffff;
  --good:#0f9f6e; --blue:#1d4ed8; --amber:#b45309; --red:#c2410c;
}}
* {{ box-sizing:border-box; }}
body {{ margin:0; background:var(--bg); color:var(--ink); font-family:"Microsoft YaHei","Segoe UI",Arial,sans-serif; }}
.hero {{ background:linear-gradient(135deg,#0b1220 0%,#18324f 60%,#25556a 100%); color:#fff; padding:36px 40px; }}
.hero h1 {{ margin:0 0 10px; font-size:30px; }}
.hero p {{ margin:6px 0; color:#d7e5f5; max-width:1100px; line-height:1.65; }}
.wrap {{ max-width:1220px; margin:0 auto; padding:24px; }}
.grid {{ display:grid; gap:16px; }}
.grid-4 {{ grid-template-columns:repeat(4,minmax(0,1fr)); }}
.grid-3 {{ grid-template-columns:repeat(3,minmax(0,1fr)); }}
.grid-2 {{ grid-template-columns:repeat(2,minmax(0,1fr)); }}
.card {{ background:var(--panel); border:1px solid var(--line); border-radius:8px; padding:18px; box-shadow:0 1px 2px rgba(16,24,40,.04); }}
.kpi .label {{ color:var(--muted); font-size:13px; margin-bottom:8px; }}
.kpi .value {{ font-size:28px; font-weight:800; }}
.kpi .sub {{ color:var(--muted); margin-top:6px; font-size:13px; }}
h2 {{ font-size:21px; margin:30px 0 14px; }}
h3 {{ font-size:16px; margin:0 0 10px; }}
table {{ width:100%; border-collapse:collapse; font-size:13px; }}
th,td {{ border-bottom:1px solid var(--line); padding:9px 10px; text-align:right; }}
th:first-child,td:first-child {{ text-align:left; }}
th {{ background:#f8fafc; color:#475467; }}
pre {{ background:#0f172a; color:#e5edf6; padding:14px; border-radius:8px; overflow:auto; }}
.note {{ color:var(--muted); line-height:1.65; }}
.callout {{ border-left:5px solid var(--amber); background:#fffbeb; border-radius:6px; padding:14px 16px; line-height:1.7; }}
.imgbox img {{ width:100%; display:block; border:1px solid var(--line); border-radius:6px; }}
.pill {{ display:inline-block; border-radius:999px; padding:4px 10px; font-size:12px; font-weight:700; }}
.pill.blue {{ background:#dbeafe; color:#1e3a8a; }}
.pill.good {{ background:#dcfce7; color:#166534; }}
@media (max-width: 920px) {{
  .grid-4,.grid-3,.grid-2 {{ grid-template-columns:1fr; }}
  .hero {{ padding:26px 22px; }}
  .wrap {{ padding:18px 14px 32px; }}
}}
</style>
</head>
<body>
<section class="hero">
  <h1>BTC 15分钟 做空逻辑全量回测</h1>
  <p>本轮只研究你当前关注的 D 类弱反抽继续空逻辑，不把 1H 四大类全部重做。重点是看这套逻辑到了 15m 还能不能跑，以及动态盈亏比/动态退出能不能比固定 2R 更合适。</p>
  <p>数据：{INST_ID} {BAR}，{quality['rows']} 根K线，{quality['start']} 至 {quality['end']}。评价仍按保守成本：单边 0.075%。</p>
</section>

<main class="wrap">
  <div class="grid grid-4">
    {kpi("最佳版本", best_config.name, "15m 当前第一名")}
    {kpi("测试集 PF", f"{best_metrics['test']['profit_factor']:.2f}", "越大越稳")}
    {kpi("测试集胜率", pct(best_metrics['test']['win_rate']), "真实交易胜率")}
    {kpi("测试集交易数", str(int(best_metrics['test']['trade_count'])), "不能太少")}
  </div>

  <h2>结论先说</h2>
  <div class="card">
    <p>{summary}最佳版本是 <strong>{best_config.name}</strong>。</p>
    <p class="note">如果最佳名里带 `be1r_trail`，表示先拿到 1R 后把止损抬到保本，再用 ATR 追踪；这就是这次 15m 回测里测试的动态盈亏比思路。</p>
  </div>

  <h2>策略定义</h2>
  <div class="grid grid-2">
    <div class="card">
      <h3>信号逻辑</h3>
      <p>沿用 D 类弱反抽继续空：先出现大阴线破位，再等 3 到 5 根 15m K 线的弱反抽，反抽不收回大阴线实体一半，随后跌破小平台再做空。</p>
      <p class="note">本次对比了 `break_n=20/48` 和 `wait_max=3/5` 的近似 15m 版本。</p>
    </div>
    <div class="card">
      <h3>退出逻辑</h3>
      <p><span class="pill blue">固定</span> `1.5R`、`2R`。</p>
      <p><span class="pill good">动态</span> `ATR trailing`、`先到1R保本再ATR追踪`、`先到1R保本再看3R`。</p>
    </div>
  </div>

  <h2>最佳配置分集表现</h2>
  <div class="card">{split_table}</div>

  <h2>固定 10U 风险账本</h2>
  <div class="card">
    <p class="note">按你前面要求的口径：初始资金 10000，每笔固定止损 10U。</p>
    {fixed_book_table}
  </div>

  <h2>年度统计</h2>
  <div class="card">{yearly_table}</div>

  <h2>月度统计</h2>
  <div class="card">{monthly_table}</div>

  <h2>动态思路有没有价值</h2>
  <div class="grid grid-3">
    <div class="card"><h3>固定 2R</h3><p>优点是简单、执行清楚；缺点是 15m 噪音更大，容易在刚起趋势时就提前离场。</p></div>
    <div class="card"><h3>ATR 跟踪</h3><p>优点是能抱住更长趋势；缺点是回吐更明显，回测很依赖波动环境。</p></div>
    <div class="card"><h3>1R 保本后追踪</h3><p>这是最接近“动态盈亏比”的版本：先确认方向正确，再把亏损风险降到接近 0，后面让盈利自己跑。</p></div>
  </div>

  <h2>Top 版本排序</h2>
  <div class="card">{sample_table}</div>

  <h2>成本敏感性</h2>
  <div class="card">{cost_table}</div>

  <h2>最佳配置详情</h2>
  <div class="card"><pre>{best_config_json}</pre></div>

  <h2>曲线</h2>
  <div class="grid grid-2">
    <div class="card imgbox"><h3>资金曲线</h3><img src="data:image/png;base64,{b64(REPORT_DIR / 'short_strategy_15m_equity_curve.png')}" alt="15m equity"></div>
    <div class="card imgbox"><h3>回撤曲线</h3><img src="data:image/png;base64,{b64(REPORT_DIR / 'short_strategy_15m_drawdown_curve.png')}" alt="15m drawdown"></div>
  </div>

  <h2>最后判断</h2>
  <div class="callout">
    15m 的优势是信号更多、节奏更快；劣势是噪音和成本更敏感。是否值得继续深挖，关键看这次最佳版本在测试集里是不是还能维持足够的 PF、交易数和回撤控制。
    这份报告已经把固定版本和动态盈亏比思路放在同一张表里，后面我们可以直接沿着第一名版本继续微调。
  </div>
</main>
</body>
</html>"""


def render_metrics_table(metrics: dict[str, dict[str, float]]) -> str:
    rows = []
    for split, m in metrics.items():
        rows.append(
            {
                "split": split,
                "total_return": m["total_return"],
                "profit_factor": m["profit_factor"],
                "win_rate": m["win_rate"],
                "max_drawdown": m["max_drawdown"],
                "trade_count": m["trade_count"],
                "avg_r": m["avg_r"],
            }
        )
    return render_table(pd.DataFrame(rows), ["split", "total_return", "profit_factor", "win_rate", "max_drawdown", "trade_count", "avg_r"])


def render_period_table(frame: pd.DataFrame, label: str) -> str:
    return render_table(frame, [label, "trades", "wins", "losses", "win_rate", "pnl", "end_capital", "avg_pnl"])


def render_table(frame: pd.DataFrame, columns: list[str]) -> str:
    subset = frame[columns].copy()
    parts = ["<table><tr>" + "".join(f"<th>{col}</th>" for col in columns) + "</tr>"]
    for _, row in subset.iterrows():
        cells = []
        for col in columns:
            value = row[col]
            if isinstance(value, (float, np.floating)):
                if "rate" in col or "return" in col or "drawdown" in col:
                    text = pct(value)
                elif "factor" in col or col == "avg_r":
                    text = f"{value:.2f}"
                else:
                    text = f"{value:.2f}"
            else:
                text = str(value)
            cells.append(f"<td>{text}</td>")
        parts.append("<tr>" + "".join(cells) + "</tr>")
    parts.append("</table>")
    return "".join(parts)


def pct(value: float) -> str:
    return f"{float(value) * 100:.1f}%"


def money(value: float) -> str:
    return f"{float(value):,.2f}"


def kpi(label: str, value: str, sub: str) -> str:
    return f'<div class="card kpi"><div class="label">{label}</div><div class="value">{value}</div><div class="sub">{sub}</div></div>'


def b64(path: Path) -> str:
    return base64.b64encode(path.read_bytes()).decode("ascii")


if __name__ == "__main__":
    main()
