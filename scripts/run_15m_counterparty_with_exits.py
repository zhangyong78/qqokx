from __future__ import annotations

import base64
import json
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

OUT = REPORT_DIR / "short_strategy_15m_counterparty_exits_report.html"
CURRENT_HTML = REPORT_DIR / "short_strategy_15m_counterparty_report.html"
ARCHIVE_HTML = REPORT_DIR / "short_strategy_15m_counterparty_report_snapshot.html"


@dataclass(frozen=True)
class CounterpartyConfig:
    name: str
    signal_col: str
    params: dict[str, object]
    exit_mode: str
    tp_r: float = 2.0
    time_stop_bars: int = 96
    trail_atr: float = 2.0
    break_even_r: float = 1.0


def main() -> None:
    if CURRENT_HTML.exists() and not ARCHIVE_HTML.exists():
        shutil.copy2(CURRENT_HTML, ARCHIVE_HTML)

    candles = load_candle_cache(INST_ID, BAR, limit=None)
    if not candles:
        raise RuntimeError(f"no candles found for {INST_ID} {BAR}")

    raw = candles_to_frame(candles)
    df = build_features(raw)
    split_bounds = split_index_bounds(df)
    quality = {
        "rows": int(len(df)),
        "start": str(df["timestamp"].iloc[0]),
        "end": str(df["timestamp"].iloc[-1]),
    }

    configs = build_configs()
    cost_scenarios = {
        "no_cost": 0.0,
        "normal_cost": 0.0006,
        "conservative_cost": 0.00075,
    }
    comparison_rows: list[dict[str, object]] = []
    all_results: dict[tuple[str, str], tuple[CounterpartyConfig, pd.DataFrame, dict[str, dict[str, float]]]] = {}
    for config in configs:
        signal = df[config.signal_col].fillna(False).to_numpy(dtype=bool)
        for cost_name, side_cost_rate in cost_scenarios.items():
            trades = backtest_counterparty_long(df, signal, config, side_cost_rate=side_cost_rate)
            metrics = {name: calc_metrics(slice_trades(trades, bounds)) for name, bounds in split_bounds.items()}
            comparison_rows.append(flatten_metrics(config, cost_name, metrics, len(trades)))
            all_results[(config.name, cost_name)] = (config, trades, metrics)

    comparison = add_rank_scores(pd.DataFrame(comparison_rows))
    comparison.to_csv(REPORT_DIR / "short_strategy_15m_counterparty_exits_comparison.csv", index=False, encoding="utf-8-sig")

    conservative = comparison[comparison["cost_scenario"] == "conservative_cost"].copy()
    ranked = rank_configs(conservative)
    best_name = ranked.iloc[0]["name"]
    best_config, best_trades, best_metrics = all_results[(best_name, "conservative_cost")]
    best_trades.to_csv(REPORT_DIR / "short_strategy_15m_counterparty_exits_trades.csv", index=False, encoding="utf-8-sig")

    exact_book = build_book(best_trades, initial_capital=INITIAL_EQUITY, pnl_col="net_pnl")
    normalized_book = build_normalized_book(best_trades)
    yearly_exact = summarize_period(best_trades, "Y", "net_pnl", INITIAL_EQUITY)
    monthly_exact = summarize_period(best_trades, "M", "net_pnl", INITIAL_EQUITY).tail(24)
    yearly_norm = summarize_period(best_trades, "Y", "normalized_pnl", INITIAL_CAPITAL_BOOK)
    monthly_norm = summarize_period(best_trades, "M", "normalized_pnl", INITIAL_CAPITAL_BOOK).tail(24)

    exact_equity = build_equity(best_trades["exit_time"], best_trades["net_pnl"], INITIAL_EQUITY)
    norm_equity = build_equity(best_trades["exit_time"], best_trades["normalized_pnl"], INITIAL_CAPITAL_BOOK)
    save_line_plot(exact_equity, REPORT_DIR / "short_strategy_15m_counterparty_exits_exact_equity.png", "Counterparty Long Exact Equity")
    save_drawdown_plot(exact_equity, REPORT_DIR / "short_strategy_15m_counterparty_exits_exact_drawdown.png", "Counterparty Long Exact Drawdown")
    save_line_plot(norm_equity, REPORT_DIR / "short_strategy_15m_counterparty_exits_normalized_equity.png", "Counterparty Long Normalized Equity")
    save_drawdown_plot(norm_equity, REPORT_DIR / "short_strategy_15m_counterparty_exits_normalized_drawdown.png", "Counterparty Long Normalized Drawdown")

    best_payload = {
        "best_name": best_name,
        "best_params": best_config.params,
        "best_metrics": best_metrics,
        "exact_book": exact_book,
        "normalized_book": normalized_book,
    }
    (REPORT_DIR / "short_strategy_15m_counterparty_exits_best.json").write_text(
        json.dumps(best_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    html = build_html(
        quality=quality,
        ranked=ranked,
        comparison=comparison,
        best_config=best_config,
        best_metrics=best_metrics,
        exact_book=exact_book,
        normalized_book=normalized_book,
        yearly_exact=yearly_exact,
        monthly_exact=monthly_exact,
        yearly_norm=yearly_norm,
        monthly_norm=monthly_norm,
    )
    OUT.write_text(html, encoding="utf-8")
    CURRENT_HTML.write_text(html, encoding="utf-8")
    print(OUT)


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
    df["close_time"] = df["timestamp"] + pd.Timedelta(minutes=BAR_MINUTES)
    return df


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


def build_configs() -> list[CounterpartyConfig]:
    bases = [
        ("sig_d_weak_bounce_48_5", {"break_n": 48, "wait_max": 5}),
        ("sig_d_weak_bounce_48_3", {"break_n": 48, "wait_max": 3}),
        ("sig_d_weak_bounce_20_3", {"break_n": 20, "wait_max": 3}),
    ]
    configs: list[CounterpartyConfig] = []
    for signal_col, params in bases:
        configs.extend(
            [
                CounterpartyConfig(
                    name=f"{signal_col}_counter_fixed_1p5r",
                    signal_col=signal_col,
                    params={**params, "direction": "counter_long", "exit_mode": "fixed_1p5r"},
                    exit_mode="fixed_r",
                    tp_r=1.5,
                    time_stop_bars=96,
                ),
                CounterpartyConfig(
                    name=f"{signal_col}_counter_fixed_2r",
                    signal_col=signal_col,
                    params={**params, "direction": "counter_long", "exit_mode": "fixed_2r"},
                    exit_mode="fixed_r",
                    tp_r=2.0,
                    time_stop_bars=96,
                ),
                CounterpartyConfig(
                    name=f"{signal_col}_counter_atr_trail_2p0",
                    signal_col=signal_col,
                    params={**params, "direction": "counter_long", "exit_mode": "atr_trail_2.0"},
                    exit_mode="atr_trail",
                    trail_atr=2.0,
                    time_stop_bars=144,
                ),
                CounterpartyConfig(
                    name=f"{signal_col}_counter_be1r_trail_2p0",
                    signal_col=signal_col,
                    params={**params, "direction": "counter_long", "exit_mode": "be1r_trail_2.0"},
                    exit_mode="be_then_trail",
                    trail_atr=2.0,
                    break_even_r=1.0,
                    time_stop_bars=144,
                ),
                CounterpartyConfig(
                    name=f"{signal_col}_counter_be1r_hold_3r",
                    signal_col=signal_col,
                    params={**params, "direction": "counter_long", "exit_mode": "be1r_hold_3r"},
                    exit_mode="be_then_fixed",
                    tp_r=3.0,
                    break_even_r=1.0,
                    time_stop_bars=144,
                ),
            ]
        )
    return configs


def backtest_counterparty_long(
    df: pd.DataFrame,
    signal: np.ndarray,
    config: CounterpartyConfig,
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
        stop_ref = float(df.at[signal_idx, "stop_d"]) if pd.notna(df.at[signal_idx, "stop_d"]) else np.nan
        if not np.isfinite(entry) or not np.isfinite(atr) or not np.isfinite(stop_ref) or atr <= 0:
            continue
        short_stop = max(stop_ref + 0.3 * atr, entry + 0.3 * atr)
        risk_distance = short_stop - entry
        if risk_distance <= 0 or risk_distance < 0.3 * atr or risk_distance > 3.0 * atr:
            continue

        stop = entry - risk_distance
        risk_amount = equity * RISK_PCT
        qty = min(risk_amount / risk_distance, equity * MAX_NOTIONAL_MULT / entry)
        if qty <= 0:
            continue

        exit_idx, exit_price, exit_reason, mfe_r, mae_r = find_long_exit(df, entry_idx, entry, stop, risk_distance, config, atr)
        gross_pnl = qty * (exit_price - entry)
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
                "target": entry + config.tp_r * risk_distance,
                "qty": qty,
                "gross_pnl": gross_pnl,
                "cost": cost,
                "net_pnl": net_pnl,
                "return_pct": return_pct,
                "gross_r": (exit_price - entry) / risk_distance,
                "net_r": net_r,
                "mfe_r": mfe_r,
                "mae_r": mae_r,
                "bars_held": exit_idx - entry_idx + 1,
                "exit_reason": exit_reason,
                "equity_after": equity,
                "normalized_pnl": net_r * FIXED_RISK_BOOK,
            }
        )
        next_allowed_entry = exit_idx + 1
    return pd.DataFrame(trades)


def find_long_exit(
    df: pd.DataFrame,
    entry_idx: int,
    entry: float,
    stop: float,
    risk_distance: float,
    config: CounterpartyConfig,
    signal_atr: float,
) -> tuple[int, float, str, float, float]:
    max_idx = min(len(df) - 1, entry_idx + config.time_stop_bars - 1)
    best_high = entry
    worst_low = entry
    trailing_stop = stop
    target = entry + config.tp_r * risk_distance
    break_even_active = False

    for idx in range(entry_idx, max_idx + 1):
        high = float(df.at[idx, "high"])
        low = float(df.at[idx, "low"])
        best_high = max(best_high, high)
        worst_low = min(worst_low, low)
        atr = float(df.at[idx, "atr14"]) if pd.notna(df.at[idx, "atr14"]) else signal_atr

        if config.exit_mode == "fixed_r":
            if low <= stop:
                return idx, stop, "stop_loss", (best_high - entry) / risk_distance, (entry - worst_low) / risk_distance
            if high >= target:
                return idx, target, "take_profit", (best_high - entry) / risk_distance, (entry - worst_low) / risk_distance
            continue

        if config.exit_mode == "atr_trail":
            trailing_stop = max(trailing_stop, best_high - config.trail_atr * atr)
            if low <= trailing_stop:
                return idx, trailing_stop, "atr_trail_stop", (best_high - entry) / risk_distance, (entry - worst_low) / risk_distance
            continue

        if config.exit_mode == "be_then_trail":
            if not break_even_active and high >= entry + config.break_even_r * risk_distance:
                break_even_active = True
                trailing_stop = max(trailing_stop, entry)
            if not break_even_active:
                if low <= stop:
                    return idx, stop, "stop_loss", (best_high - entry) / risk_distance, (entry - worst_low) / risk_distance
            else:
                trailing_stop = max(trailing_stop, best_high - config.trail_atr * atr)
                if low <= trailing_stop:
                    reason = "break_even_stop" if trailing_stop >= entry else "trail_after_break_even"
                    return idx, trailing_stop, reason, (best_high - entry) / risk_distance, (entry - worst_low) / risk_distance
            continue

        if config.exit_mode == "be_then_fixed":
            if not break_even_active and high >= entry + config.break_even_r * risk_distance:
                break_even_active = True
            if low <= (entry if break_even_active else stop):
                exit_px = entry if break_even_active else stop
                reason = "break_even_stop" if break_even_active else "stop_loss"
                return idx, exit_px, reason, (best_high - entry) / risk_distance, (entry - worst_low) / risk_distance
            if high >= target:
                return idx, target, "extended_take_profit", (best_high - entry) / risk_distance, (entry - worst_low) / risk_distance

    exit_price = float(df.at[max_idx, "close"])
    return max_idx, exit_price, "time_stop", (best_high - entry) / risk_distance, (entry - worst_low) / risk_distance


def slice_trades(trades: pd.DataFrame, bounds: tuple[int, int]) -> pd.DataFrame:
    if trades.empty:
        return trades
    start, end = bounds
    return trades[(trades["entry_index"] >= start) & (trades["entry_index"] <= end)].copy()


def flatten_metrics(config: CounterpartyConfig, cost_name: str, metrics: dict[str, dict[str, float]], trade_count: int) -> dict[str, object]:
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


def build_book(trades: pd.DataFrame, *, initial_capital: float, pnl_col: str) -> dict[str, float]:
    frame = trades.copy()
    pnls = frame[pnl_col].astype(float).to_numpy()
    equity = initial_capital + np.cumsum(pnls)
    peaks = np.maximum.accumulate(np.r_[initial_capital, equity])[:-1]
    drawdown = equity / peaks - 1
    wins = pnls[pnls > 0]
    losses = pnls[pnls <= 0]
    return {
        "initial_capital": initial_capital,
        "total_trades": int(len(frame)),
        "win_count": int((pnls > 0).sum()),
        "loss_count": int((pnls <= 0).sum()),
        "win_rate": float((pnls > 0).mean()) if len(pnls) else 0.0,
        "total_pnl": float(pnls.sum()),
        "final_capital": float(initial_capital + pnls.sum()),
        "total_return": float(pnls.sum() / initial_capital) if initial_capital else 0.0,
        "avg_pnl": float(pnls.mean()) if len(pnls) else 0.0,
        "max_drawdown": float(drawdown.min()) if len(drawdown) else 0.0,
        "profit_factor": float(wins.sum() / abs(losses.sum())) if losses.sum() < 0 else 999.0,
        "avg_win_loss": float(wins.mean() / abs(losses.mean())) if len(wins) and len(losses) else 0.0,
    }


def build_normalized_book(trades: pd.DataFrame) -> dict[str, float]:
    book = build_book(trades, initial_capital=INITIAL_CAPITAL_BOOK, pnl_col="normalized_pnl")
    book["fixed_risk"] = FIXED_RISK_BOOK
    return book


def summarize_period(trades: pd.DataFrame, freq: str, pnl_col: str, initial_capital: float) -> pd.DataFrame:
    label = "year" if freq == "Y" else "month"
    frame = trades.copy()
    frame["exit_time"] = pd.to_datetime(frame["exit_time"], utc=True).dt.tz_convert(None)
    frame["period"] = frame["exit_time"].dt.to_period(freq).astype(str)
    out = (
        frame.groupby("period", as_index=False)
        .agg(
            trades=(pnl_col, "count"),
            wins=(pnl_col, lambda s: int((s > 0).sum())),
            losses=(pnl_col, lambda s: int((s <= 0).sum())),
            pnl=(pnl_col, "sum"),
            avg_pnl=(pnl_col, "mean"),
            win_rate=(pnl_col, lambda s: float((s > 0).mean())),
        )
        .sort_values("period")
    )
    out["end_capital"] = initial_capital + out["pnl"].cumsum()
    return out.rename(columns={"period": label})


def build_equity(times: pd.Series, pnls: pd.Series, initial_capital: float) -> pd.DataFrame:
    return pd.DataFrame({"timestamp": pd.to_datetime(times), "equity": initial_capital + pd.Series(pnls).cumsum()})


def save_line_plot(frame: pd.DataFrame, path: Path, title: str) -> None:
    plt.figure(figsize=(12, 5))
    plt.plot(frame["timestamp"], frame["equity"], color="#0f766e")
    plt.title(title)
    plt.tight_layout()
    plt.savefig(path, dpi=150)
    plt.close()


def save_drawdown_plot(frame: pd.DataFrame, path: Path, title: str) -> None:
    out = frame.copy()
    out["peak"] = out["equity"].cummax()
    out["drawdown"] = out["equity"] / out["peak"] - 1
    plt.figure(figsize=(12, 4))
    plt.fill_between(out["timestamp"], out["drawdown"] * 100, color="#dc2626", alpha=0.35)
    plt.title(title)
    plt.tight_layout()
    plt.savefig(path, dpi=150)
    plt.close()


def build_html(
    *,
    quality: dict[str, object],
    ranked: pd.DataFrame,
    comparison: pd.DataFrame,
    best_config: CounterpartyConfig,
    best_metrics: dict[str, dict[str, float]],
    exact_book: dict[str, float],
    normalized_book: dict[str, float],
    yearly_exact: pd.DataFrame,
    monthly_exact: pd.DataFrame,
    yearly_norm: pd.DataFrame,
    monthly_norm: pd.DataFrame,
) -> str:
    top = ranked.head(10).copy()
    cost_rows = comparison[comparison["name"] == best_config.name].sort_values("cost_scenario")
    dynamic_won = any(key in best_config.name for key in ("atr_trail", "be1r_trail", "be1r_hold"))
    conclusion = "动态/半动态退出胜出" if dynamic_won else "固定R退出更优"
    best_json = json.dumps({"name": best_config.name, "params": best_config.params, "exit_mode": best_config.exit_mode}, ensure_ascii=False, indent=2)
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>BTC 15m 对手盘固定/动态盈亏比回测</title>
<style>
:root {{
  --ink:#172033; --muted:#667085; --line:#d7dce5; --bg:#f4f7fb; --panel:#fff;
  --good:#0f9f6e; --blue:#1d4ed8; --amber:#b45309;
}}
* {{ box-sizing:border-box; }}
body {{ margin:0; background:var(--bg); color:var(--ink); font-family:"Microsoft YaHei","Segoe UI",Arial,sans-serif; }}
.hero {{ background:linear-gradient(135deg,#101827 0%,#1d3557 58%,#2f6b73 100%); color:#fff; padding:36px 40px; }}
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
.note {{ color:var(--muted); line-height:1.65; }}
.callout {{ border-left:5px solid var(--amber); background:#fffbeb; border-radius:6px; padding:14px 16px; line-height:1.7; }}
.imgbox img {{ width:100%; display:block; border:1px solid var(--line); border-radius:6px; }}
pre {{ background:#0f172a; color:#e5edf6; padding:14px; border-radius:8px; overflow:auto; }}
@media (max-width: 920px) {{
  .grid-4,.grid-3,.grid-2 {{ grid-template-columns:1fr; }}
  .hero {{ padding:26px 22px; }}
  .wrap {{ padding:18px 14px 32px; }}
}}
</style>
</head>
<body>
<section class="hero">
  <h1>BTC 15分钟 对手盘固定/动态盈亏比回测</h1>
  <p>这次不是机械沿用原空单的出场，而是保留同样的 15m 入场时点，把方向翻成对手盘做多，再给它配上固定盈亏比和动态盈亏比两类真实下单方式去跑。</p>
  <p>数据：{INST_ID} {BAR}，{quality['rows']} 根K线，{quality['start']} 至 {quality['end']}。成本仍按保守口径：单边 0.075%。</p>
</section>

<main class="wrap">
  <div class="grid grid-4">
    {kpi("最佳版本", best_config.name, "当前第一名")}
    {kpi("退出结论", conclusion, "看固定R还是动态R更好")}
    {kpi("测试集 PF", f"{best_metrics['test']['profit_factor']:.2f}", "是否超过1")}
    {kpi("测试集收益", pct(best_metrics['test']['total_return']), f"交易数 {int(best_metrics['test']['trade_count'])}")}
  </div>

  <h2>一句话结论</h2>
  <div class="card">
    <p>同样的 15m 反向入场，如果让它自己按固定盈亏比或动态盈亏比去管理仓位，最佳结果是 <strong>{best_config.name}</strong>。</p>
    <p class="note">这能回答你真正想看的问题：不是“反着做能不能赢”，而是“反着做以后，用正常下单逻辑能不能跑出正收益”。</p>
  </div>

  <h2>最佳版本分集表现</h2>
  <div class="card">{render_metrics_table(best_metrics)}</div>

  <h2>Top 版本排序</h2>
  <div class="card">{render_table(top, ["name", "test_total_return", "test_profit_factor", "test_win_rate", "test_trade_count", "score"])}</div>

  <h2>成本敏感性</h2>
  <div class="card">{render_table(cost_rows, ["cost_scenario", "all_total_return", "all_profit_factor", "test_total_return", "test_profit_factor"])}</div>

  <h2>精确成交资金口径</h2>
  <div class="grid grid-4">
    {kpi("总交易次数", str(int(exact_book['total_trades'])), f"赚钱 {int(exact_book['win_count'])} / 亏钱 {int(exact_book['loss_count'])}")}
    {kpi("最终资金", money(exact_book['final_capital']), f"初始 {money(exact_book['initial_capital'])}")}
    {kpi("总收益率", pct(exact_book['total_return']), f"PF {exact_book['profit_factor']:.2f}")}
    {kpi("最大回撤", pct(exact_book['max_drawdown']), f"胜率 {pct(exact_book['win_rate'])}")}
  </div>

  <h2>10U 风险口径</h2>
  <div class="grid grid-4">
    {kpi("起始资金", money(normalized_book['initial_capital']), "按10000记账")}
    {kpi("每笔风险", money(normalized_book['fixed_risk']), "固定10U")}
    {kpi("最后剩余资金", money(normalized_book['final_capital']), f"累计盈亏 {money(normalized_book['total_pnl'])}")}
    {kpi("资金回报率", pct(normalized_book['total_return']), f"最大回撤 {pct(normalized_book['max_drawdown'])}")}
  </div>

  <h2>年度统计</h2>
  <div class="grid grid-2">
    <div class="card">
      <h3>精确成交口径</h3>
      {render_table(yearly_exact, ["year", "trades", "wins", "losses", "win_rate", "pnl", "end_capital"])}
    </div>
    <div class="card">
      <h3>10U 风险口径</h3>
      {render_table(yearly_norm, ["year", "trades", "wins", "losses", "win_rate", "pnl", "end_capital"])}
    </div>
  </div>

  <h2>最近24个月月度统计</h2>
  <div class="grid grid-2">
    <div class="card">
      <h3>精确成交口径</h3>
      {render_table(monthly_exact, ["month", "trades", "wins", "losses", "win_rate", "pnl", "end_capital"])}
    </div>
    <div class="card">
      <h3>10U 风险口径</h3>
      {render_table(monthly_norm, ["month", "trades", "wins", "losses", "win_rate", "pnl", "end_capital"])}
    </div>
  </div>

  <h2>曲线</h2>
  <div class="grid grid-2">
    <div class="card imgbox"><h3>精确成交资金曲线</h3><img src="data:image/png;base64,{b64(REPORT_DIR / 'short_strategy_15m_counterparty_exits_exact_equity.png')}" alt="counterparty exits exact equity"></div>
    <div class="card imgbox"><h3>精确成交回撤</h3><img src="data:image/png;base64,{b64(REPORT_DIR / 'short_strategy_15m_counterparty_exits_exact_drawdown.png')}" alt="counterparty exits exact drawdown"></div>
    <div class="card imgbox"><h3>10U 风险资金曲线</h3><img src="data:image/png;base64,{b64(REPORT_DIR / 'short_strategy_15m_counterparty_exits_normalized_equity.png')}" alt="counterparty exits normalized equity"></div>
    <div class="card imgbox"><h3>10U 风险回撤</h3><img src="data:image/png;base64,{b64(REPORT_DIR / 'short_strategy_15m_counterparty_exits_normalized_drawdown.png')}" alt="counterparty exits normalized drawdown"></div>
  </div>

  <h2>最佳版本详情</h2>
  <div class="card"><pre>{best_json}</pre></div>

  <h2>最后解释</h2>
  <div class="callout">
    这次看的是“对手盘自己下单”的能力，而不是沿用原空单出场的镜像结果。如果最佳版本还是不过关，那就说明问题不只是出场方式，连反向后的信号质量本身也不够强。
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


def render_table(frame: pd.DataFrame, columns: list[str]) -> str:
    subset = frame[columns].copy()
    parts = ["<table><tr>" + "".join(f"<th>{col}</th>" for col in columns) + "</tr>"]
    for _, row in subset.iterrows():
        cells: list[str] = []
        for col in columns:
            value = row[col]
            if isinstance(value, (float, np.floating)):
                if "rate" in col or "return" in col or "drawdown" in col:
                    text = pct(value)
                elif "factor" in col or col == "avg_r" or col == "score":
                    text = f"{value:.2f}"
                else:
                    text = money(value)
            else:
                text = str(value)
            cells.append(f"<td>{text}</td>")
        parts.append("<tr>" + "".join(cells) + "</tr>")
    parts.append("</table>")
    return "".join(parts)


def money(value: float) -> str:
    return f"{float(value):,.2f}"


def pct(value: float) -> str:
    return f"{float(value) * 100:.1f}%"


def kpi(label: str, value: str, sub: str) -> str:
    return f'<div class="card kpi"><div class="label">{label}</div><div class="value">{value}</div><div class="sub">{sub}</div></div>'


def b64(path: Path) -> str:
    return base64.b64encode(path.read_bytes()).decode("ascii")


if __name__ == "__main__":
    main()
