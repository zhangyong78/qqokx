from __future__ import annotations

import base64
import json
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


REPORT_DIR = ROOT / "reports"
INST_ID = "BTC-USDT-SWAP"
BAR = "1H"
INITIAL_EQUITY = 100_000.0
RISK_PCT = 0.005
MAX_NOTIONAL_MULT = 2.5
CONSERVATIVE_SIDE_COST = 0.00075
NORMAL_SIDE_COST = 0.00060
FIXED_RISK_BOOK = 10.0
FIXED_RISK_INITIAL = 10_000.0

HTML_PATH = REPORT_DIR / "btc_1h_ma55_ema55_short_report.html"


@dataclass(frozen=True)
class StrategyConfig:
    name: str
    line_col: str
    line_label: str
    atr_stop_mult: float
    exit_mode: str
    tp_r: float = 2.0
    time_stop_bars: int = 24
    trail_atr_mult: float = 2.0
    break_even_r: float = 1.0


def main() -> None:
    REPORT_DIR.mkdir(exist_ok=True)
    candles = load_candle_cache(INST_ID, BAR, limit=None)
    if not candles:
        raise RuntimeError(f"no candles found for {INST_ID} {BAR}")

    df = build_frame(candles)
    df = add_indicators(df)
    line_stats = build_line_stats(df)

    configs = build_configs()
    split_bounds = split_index_bounds(df)
    rows: list[dict[str, object]] = []
    all_results: dict[tuple[str, str], tuple[StrategyConfig, pd.DataFrame, dict[str, dict[str, float]]]] = {}
    costs = {
        "no_cost": 0.0,
        "normal_cost": NORMAL_SIDE_COST,
        "conservative_cost": CONSERVATIVE_SIDE_COST,
    }
    for config in configs:
        signal = cross_below_signal(df, config.line_col)
        for cost_name, side_cost in costs.items():
            trades = backtest(df, signal, config, side_cost_rate=side_cost)
            metrics = {name: metrics_for(slice_trades(trades, bounds)) for name, bounds in split_bounds.items()}
            rows.append(flatten_metrics(config, cost_name, metrics, len(trades)))
            all_results[(config.name, cost_name)] = (config, trades, metrics)

    comparison = pd.DataFrame(rows)
    comparison["score"] = score_configs(comparison)
    comparison.to_csv(REPORT_DIR / "btc_1h_ma55_ema55_short_comparison.csv", index=False, encoding="utf-8-sig")

    ranked = (
        comparison[comparison["cost_scenario"] == "conservative_cost"]
        .sort_values("score", ascending=False)
        .reset_index(drop=True)
    )
    best_name = str(ranked.iloc[0]["name"])
    best_config, best_trades, best_metrics = all_results[(best_name, "conservative_cost")]
    best_trades.to_csv(REPORT_DIR / "btc_1h_ma55_ema55_short_trades.csv", index=False, encoding="utf-8-sig")

    fixed_book = fixed_risk_book(best_trades)
    yearly = period_table(best_trades, "Y", "year")
    monthly = period_table(best_trades, "M", "month")
    yearly.to_csv(REPORT_DIR / "btc_1h_ma55_ema55_short_yearly.csv", index=False, encoding="utf-8-sig")
    monthly.to_csv(REPORT_DIR / "btc_1h_ma55_ema55_short_monthly.csv", index=False, encoding="utf-8-sig")

    equity = equity_curve(best_trades)
    drawdown = drawdown_curve(equity)
    save_equity_plot(equity, REPORT_DIR / "btc_1h_ma55_ema55_short_equity.png")
    save_drawdown_plot(drawdown, REPORT_DIR / "btc_1h_ma55_ema55_short_drawdown.png")

    payload = {
        "best_name": best_name,
        "best_config": config_to_dict(best_config),
        "best_metrics": best_metrics,
        "fixed_risk_book": fixed_book,
        "line_stats": line_stats,
    }
    (REPORT_DIR / "btc_1h_ma55_ema55_short_best.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    HTML_PATH.write_text(
        build_html(
            df=df,
            line_stats=line_stats,
            comparison=comparison,
            ranked=ranked,
            best_config=best_config,
            best_metrics=best_metrics,
            best_trades=best_trades,
            fixed_book=fixed_book,
            yearly=yearly,
            monthly=monthly.tail(24),
        ),
        encoding="utf-8",
    )
    print(HTML_PATH)


def build_frame(candles: list[object]) -> pd.DataFrame:
    rows = [
        {
            "timestamp": pd.to_datetime(int(c.ts), unit="ms", utc=True),
            "open": float(c.open),
            "high": float(c.high),
            "low": float(c.low),
            "close": float(c.close),
            "volume": float(c.volume),
            "confirmed": bool(c.confirmed),
        }
        for c in candles
    ]
    return pd.DataFrame(rows).sort_values("timestamp").drop_duplicates("timestamp").reset_index(drop=True)


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["sma55"] = out["close"].rolling(55, min_periods=55).mean()
    out["ema55"] = out["close"].ewm(span=55, adjust=False, min_periods=55).mean()
    prev_close = out["close"].shift(1)
    tr = pd.concat(
        [
            out["high"] - out["low"],
            (out["high"] - prev_close).abs(),
            (out["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    out["atr14"] = tr.rolling(14, min_periods=14).mean()
    out["atr_pct"] = out["atr14"].rolling(100, min_periods=100).apply(lambda x: float(np.mean(x <= x[-1])), raw=True)
    return out


def build_line_stats(df: pd.DataFrame) -> dict[str, dict[str, object]]:
    stats: dict[str, dict[str, object]] = {}
    for col, label in (("sma55", "55MA"), ("ema55", "55EMA")):
        ready = df[df[col].notna()].copy()
        above = ready["close"] > ready[col]
        below = ready["close"] < ready[col]
        equal = ready["close"] == ready[col]
        stats[label] = {
            "ready_bars": int(len(ready)),
            "above_bars": int(above.sum()),
            "below_bars": int(below.sum()),
            "equal_bars": int(equal.sum()),
            "above_pct": float(above.mean()) if len(ready) else 0.0,
            "below_pct": float(below.mean()) if len(ready) else 0.0,
            "cross_below_count": int(cross_below_signal(df, col).sum()),
            "cross_above_count": int(((df["close"].shift(1) <= df[col].shift(1)) & (df["close"] > df[col])).sum()),
            "avg_above_streak": average_streak(above.to_numpy(dtype=bool), True),
            "avg_below_streak": average_streak(below.to_numpy(dtype=bool), True),
        }
    return stats


def average_streak(flags: np.ndarray, value: bool) -> float:
    lengths: list[int] = []
    current = 0
    for item in flags:
        if bool(item) is value:
            current += 1
        elif current:
            lengths.append(current)
            current = 0
    if current:
        lengths.append(current)
    return float(np.mean(lengths)) if lengths else 0.0


def cross_below_signal(df: pd.DataFrame, line_col: str) -> pd.Series:
    return (df["close"].shift(1) >= df[line_col].shift(1)) & (df["close"] < df[line_col]) & df[line_col].notna()


def build_configs() -> list[StrategyConfig]:
    configs: list[StrategyConfig] = []
    for line_col, line_label in (("sma55", "55MA"), ("ema55", "55EMA")):
        for stop_mult in (1.0, 1.5, 2.0):
            for tp_r in (1.0, 1.5, 2.0, 3.0):
                configs.append(
                    StrategyConfig(
                        name=f"{line_label}_cross_short_stop{stop_mult:g}_tp{tp_r:g}r_t24",
                        line_col=line_col,
                        line_label=line_label,
                        atr_stop_mult=stop_mult,
                        exit_mode="fixed_r",
                        tp_r=tp_r,
                        time_stop_bars=24,
                    )
                )
            configs.append(
                StrategyConfig(
                    name=f"{line_label}_cross_short_stop{stop_mult:g}_line_reclaim_t36",
                    line_col=line_col,
                    line_label=line_label,
                    atr_stop_mult=stop_mult,
                    exit_mode="line_reclaim",
                    time_stop_bars=36,
                )
            )
            configs.append(
                StrategyConfig(
                    name=f"{line_label}_cross_short_stop{stop_mult:g}_atr_trail2_t48",
                    line_col=line_col,
                    line_label=line_label,
                    atr_stop_mult=stop_mult,
                    exit_mode="atr_trail",
                    trail_atr_mult=2.0,
                    time_stop_bars=48,
                )
            )
            configs.append(
                StrategyConfig(
                    name=f"{line_label}_cross_short_stop{stop_mult:g}_be1r_trail2_t48",
                    line_col=line_col,
                    line_label=line_label,
                    atr_stop_mult=stop_mult,
                    exit_mode="be_then_trail",
                    trail_atr_mult=2.0,
                    break_even_r=1.0,
                    time_stop_bars=48,
                )
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


def backtest(df: pd.DataFrame, signal: pd.Series, config: StrategyConfig, *, side_cost_rate: float) -> pd.DataFrame:
    equity = INITIAL_EQUITY
    trades: list[dict[str, object]] = []
    next_allowed_entry = 0
    for signal_idx in np.flatnonzero(signal.to_numpy(dtype=bool)):
        if signal_idx + 1 >= len(df) or signal_idx < next_allowed_entry:
            continue
        entry_idx = signal_idx + 1
        entry = float(df.at[entry_idx, "open"])
        atr = float(df.at[signal_idx, "atr14"]) if pd.notna(df.at[signal_idx, "atr14"]) else np.nan
        line_value = float(df.at[signal_idx, config.line_col]) if pd.notna(df.at[signal_idx, config.line_col]) else np.nan
        if not np.isfinite(entry) or not np.isfinite(atr) or not np.isfinite(line_value) or atr <= 0:
            continue
        stop = float(df.at[signal_idx, "high"]) + config.atr_stop_mult * atr
        risk_distance = stop - entry
        if risk_distance <= 0 or risk_distance > 5.0 * atr:
            continue

        risk_amount = equity * RISK_PCT
        qty = min(risk_amount / risk_distance, equity * MAX_NOTIONAL_MULT / entry)
        if qty <= 0:
            continue

        exit_idx, exit_price, exit_reason, mfe_r, mae_r = find_exit(df, entry_idx, entry, stop, risk_distance, config)
        gross_pnl = qty * (entry - exit_price)
        cost = side_cost_rate * qty * (entry + exit_price)
        net_pnl = gross_pnl - cost
        return_pct = net_pnl / equity
        net_r = net_pnl / risk_amount if risk_amount else 0.0
        equity += net_pnl
        trades.append(
            {
                "config_name": config.name,
                "line": config.line_label,
                "atr_stop_mult": config.atr_stop_mult,
                "exit_mode": config.exit_mode,
                "signal_index": signal_idx,
                "entry_index": entry_idx,
                "exit_index": exit_idx,
                "signal_time": df.at[signal_idx, "timestamp"],
                "entry_time": df.at[entry_idx, "timestamp"],
                "exit_time": df.at[exit_idx, "timestamp"],
                "entry": entry,
                "exit": exit_price,
                "stop": stop,
                "line_value": line_value,
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
                "normalized_pnl": net_r * FIXED_RISK_BOOK,
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
) -> tuple[int, float, str, float, float]:
    max_idx = min(len(df) - 1, entry_idx + config.time_stop_bars - 1)
    target = entry - config.tp_r * risk_distance
    best_low = entry
    worst_high = entry
    trailing_stop = stop
    break_even_active = False
    for idx in range(entry_idx, max_idx + 1):
        high = float(df.at[idx, "high"])
        low = float(df.at[idx, "low"])
        close = float(df.at[idx, "close"])
        atr = float(df.at[idx, "atr14"]) if pd.notna(df.at[idx, "atr14"]) else risk_distance
        line_value = float(df.at[idx, config.line_col]) if pd.notna(df.at[idx, config.line_col]) else np.nan
        best_low = min(best_low, low)
        worst_high = max(worst_high, high)

        if high >= stop:
            return idx, stop, "stop_loss", (entry - best_low) / risk_distance, (worst_high - entry) / risk_distance

        if config.exit_mode == "fixed_r":
            if low <= target:
                return idx, target, "take_profit", (entry - best_low) / risk_distance, (worst_high - entry) / risk_distance
            continue

        if config.exit_mode == "line_reclaim":
            if np.isfinite(line_value) and close > line_value:
                return idx, close, "line_reclaim", (entry - best_low) / risk_distance, (worst_high - entry) / risk_distance
            continue

        if config.exit_mode == "atr_trail":
            trailing_stop = min(trailing_stop, best_low + config.trail_atr_mult * atr)
            if high >= trailing_stop:
                return idx, trailing_stop, "atr_trail_stop", (entry - best_low) / risk_distance, (worst_high - entry) / risk_distance
            continue

        if config.exit_mode == "be_then_trail":
            if not break_even_active and low <= entry - config.break_even_r * risk_distance:
                break_even_active = True
                stop = min(stop, entry)
                trailing_stop = min(trailing_stop, entry)
            if break_even_active:
                trailing_stop = min(trailing_stop, best_low + config.trail_atr_mult * atr)
                if high >= trailing_stop:
                    return idx, trailing_stop, "be_then_trail_stop", (entry - best_low) / risk_distance, (worst_high - entry) / risk_distance
            continue

    exit_price = float(df.at[max_idx, "close"])
    return max_idx, exit_price, "time_stop", (entry - best_low) / risk_distance, (worst_high - entry) / risk_distance


def slice_trades(trades: pd.DataFrame, bounds: tuple[int, int]) -> pd.DataFrame:
    if trades.empty:
        return trades
    start, end = bounds
    return trades[(trades["entry_index"] >= start) & (trades["entry_index"] <= end)].copy()


def metrics_for(trades: pd.DataFrame) -> dict[str, float]:
    if trades.empty:
        return empty_metrics()
    returns = trades["return_pct"].to_numpy(dtype=float)
    equity = np.cumprod(1 + returns)
    drawdown = equity / np.maximum.accumulate(equity) - 1
    wins = trades[trades["net_pnl"] > 0]
    losses = trades[trades["net_pnl"] <= 0]
    gross_profit = float(wins["net_pnl"].sum())
    gross_loss = float(-losses["net_pnl"].sum())
    return {
        "total_return": float(equity[-1] - 1),
        "max_drawdown": float(drawdown.min()),
        "profit_factor": gross_profit / gross_loss if gross_loss > 0 else (999.0 if gross_profit > 0 else 0.0),
        "win_rate": float(len(wins) / len(trades)),
        "trade_count": float(len(trades)),
        "avg_r": float(trades["net_r"].mean()),
        "avg_bars_held": float(trades["bars_held"].mean()),
        "max_consecutive_losses": float(max_loss_streak(trades["net_pnl"].to_numpy(dtype=float))),
        "cost_profit_ratio": float(trades["cost"].sum() / gross_profit) if gross_profit > 0 else 0.0,
    }


def empty_metrics() -> dict[str, float]:
    return {
        "total_return": 0.0,
        "max_drawdown": 0.0,
        "profit_factor": 0.0,
        "win_rate": 0.0,
        "trade_count": 0.0,
        "avg_r": 0.0,
        "avg_bars_held": 0.0,
        "max_consecutive_losses": 0.0,
        "cost_profit_ratio": 0.0,
    }


def max_loss_streak(pnls: np.ndarray) -> int:
    best = current = 0
    for pnl in pnls:
        if pnl <= 0:
            current += 1
            best = max(best, current)
        else:
            current = 0
    return best


def flatten_metrics(config: StrategyConfig, cost_name: str, metrics: dict[str, dict[str, float]], trade_count: int) -> dict[str, object]:
    row: dict[str, object] = {
        "name": config.name,
        "line": config.line_label,
        "atr_stop_mult": config.atr_stop_mult,
        "exit_mode": config.exit_mode,
        "cost_scenario": cost_name,
        "total_trades": trade_count,
        "params_json": json.dumps(config_to_dict(config), ensure_ascii=False, sort_keys=True),
    }
    for split, values in metrics.items():
        for key, value in values.items():
            row[f"{split}_{key}"] = value
    return row


def score_configs(frame: pd.DataFrame) -> pd.Series:
    score = (
        frame["test_profit_factor"].clip(upper=3) * 2.0
        + frame["validation_profit_factor"].clip(upper=3)
        + frame["test_total_return"] * 8
        + frame["validation_total_return"] * 4
        - frame["test_max_drawdown"].abs() * 4
    )
    score = score.mask(frame["test_trade_count"] < 20, score - 3)
    score = score.mask(frame["validation_trade_count"] < 20, score - 2)
    score = score.mask(frame["test_profit_factor"] < 1.0, score - 2)
    return score


def config_to_dict(config: StrategyConfig) -> dict[str, object]:
    return {
        "name": config.name,
        "line": config.line_label,
        "atr_stop_mult": config.atr_stop_mult,
        "exit_mode": config.exit_mode,
        "tp_r": config.tp_r,
        "time_stop_bars": config.time_stop_bars,
        "trail_atr_mult": config.trail_atr_mult,
        "break_even_r": config.break_even_r,
    }


def fixed_risk_book(trades: pd.DataFrame) -> dict[str, float]:
    if trades.empty:
        return {
            "initial_capital": FIXED_RISK_INITIAL,
            "fixed_risk": FIXED_RISK_BOOK,
            "final_capital": FIXED_RISK_INITIAL,
            "total_pnl": 0.0,
            "total_return": 0.0,
            "win_count": 0,
            "loss_count": 0,
            "win_rate": 0.0,
            "max_drawdown": 0.0,
        }
    pnls = trades["normalized_pnl"].astype(float)
    equity = FIXED_RISK_INITIAL + pnls.cumsum()
    drawdown = equity / equity.cummax() - 1
    return {
        "initial_capital": FIXED_RISK_INITIAL,
        "fixed_risk": FIXED_RISK_BOOK,
        "final_capital": float(FIXED_RISK_INITIAL + pnls.sum()),
        "total_pnl": float(pnls.sum()),
        "total_return": float(pnls.sum() / FIXED_RISK_INITIAL),
        "win_count": int((pnls > 0).sum()),
        "loss_count": int((pnls <= 0).sum()),
        "win_rate": float((pnls > 0).mean()),
        "max_drawdown": float(drawdown.min()),
    }


def period_table(trades: pd.DataFrame, freq: str, label: str) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame(columns=[label, "trades", "wins", "losses", "win_rate", "pnl", "end_capital"])
    frame = trades.copy()
    frame["exit_time"] = pd.to_datetime(frame["exit_time"], utc=True).dt.tz_convert(None)
    frame[label] = frame["exit_time"].dt.to_period(freq).astype(str)
    out = (
        frame.groupby(label, as_index=False)
        .agg(
            trades=("normalized_pnl", "count"),
            wins=("normalized_pnl", lambda s: int((s > 0).sum())),
            losses=("normalized_pnl", lambda s: int((s <= 0).sum())),
            win_rate=("normalized_pnl", lambda s: float((s > 0).mean())),
            pnl=("normalized_pnl", "sum"),
        )
        .sort_values(label)
    )
    out["end_capital"] = FIXED_RISK_INITIAL + out["pnl"].cumsum()
    return out


def equity_curve(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame({"timestamp": [], "equity": []})
    equity = INITIAL_EQUITY
    rows = [{"timestamp": trades["entry_time"].iloc[0], "equity": equity}]
    for _, trade in trades.iterrows():
        equity *= 1 + float(trade["return_pct"])
        rows.append({"timestamp": trade["exit_time"], "equity": equity})
    return pd.DataFrame(rows)


def drawdown_curve(equity: pd.DataFrame) -> pd.DataFrame:
    out = equity.copy()
    if out.empty:
        out["drawdown"] = []
        return out
    out["peak"] = out["equity"].cummax()
    out["drawdown"] = out["equity"] / out["peak"] - 1
    return out


def save_equity_plot(equity: pd.DataFrame, path: Path) -> None:
    plt.figure(figsize=(12, 5))
    if not equity.empty:
        plt.plot(pd.to_datetime(equity["timestamp"]), equity["equity"], color="#1d4ed8")
    plt.title("BTC 1H MA55/EMA55 Short Equity")
    plt.tight_layout()
    plt.savefig(path, dpi=150)
    plt.close()


def save_drawdown_plot(drawdown: pd.DataFrame, path: Path) -> None:
    plt.figure(figsize=(12, 4))
    if not drawdown.empty:
        plt.fill_between(pd.to_datetime(drawdown["timestamp"]), drawdown["drawdown"] * 100, color="#dc2626", alpha=0.35)
    plt.title("BTC 1H MA55/EMA55 Short Drawdown")
    plt.tight_layout()
    plt.savefig(path, dpi=150)
    plt.close()


def build_html(
    *,
    df: pd.DataFrame,
    line_stats: dict[str, dict[str, object]],
    comparison: pd.DataFrame,
    ranked: pd.DataFrame,
    best_config: StrategyConfig,
    best_metrics: dict[str, dict[str, float]],
    best_trades: pd.DataFrame,
    fixed_book: dict[str, float],
    yearly: pd.DataFrame,
    monthly: pd.DataFrame,
) -> str:
    cost_rows = comparison[comparison["name"] == best_config.name].sort_values("cost_scenario")
    best_json = json.dumps(config_to_dict(best_config), ensure_ascii=False, indent=2)
    conclusion = "达到测试集 PF > 1" if best_metrics["test"]["profit_factor"] > 1 else "测试集仍未跑出稳定优势"
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>BTC 1H 55MA/55EMA 跌破做空研究</title>
<style>
:root {{
  --ink:#172033; --muted:#667085; --line:#d7dce5; --bg:#f4f7fb; --panel:#fff;
  --blue:#1d4ed8; --green:#0f9f6e; --amber:#b45309; --red:#b42318;
}}
* {{ box-sizing:border-box; }}
body {{ margin:0; background:var(--bg); color:var(--ink); font-family:"Microsoft YaHei","Segoe UI",Arial,sans-serif; }}
.hero {{ background:linear-gradient(135deg,#0f172a 0%,#263a5f 58%,#39636f 100%); color:#fff; padding:36px 40px; }}
.hero h1 {{ margin:0 0 10px; font-size:30px; }}
.hero p {{ margin:6px 0; color:#d7e5f5; max-width:1120px; line-height:1.65; }}
.wrap {{ max-width:1240px; margin:0 auto; padding:24px; }}
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
  <h1>BTC 1小时 55MA / 55EMA 跌破做空研究</h1>
  <p>逻辑：收盘价从 55MA 或 55EMA 上方跌破到线下，信号K收盘确认后，下一根 1H 开盘做空。止损 = 跌破K线高点 + ATR × 1 / 1.5 / 2，以损定量。</p>
  <p>数据：{INST_ID} {BAR}，{len(df)} 根K线，{df['timestamp'].iloc[0]} 至 {df['timestamp'].iloc[-1]}。主排序采用保守成本：单边 0.075%。</p>
</section>

<main class="wrap">
  <div class="grid grid-4">
    {kpi("最佳版本", best_config.name, "保守成本排序第一")}
    {kpi("测试集 PF", f"{best_metrics['test']['profit_factor']:.2f}", conclusion)}
    {kpi("测试集收益", pct(best_metrics['test']['total_return']), f"交易 {int(best_metrics['test']['trade_count'])} 笔")}
    {kpi("测试集胜率", pct(best_metrics['test']['win_rate']), f"最大回撤 {pct(best_metrics['test']['max_drawdown'])}")}
  </div>

  <h2>55线运行统计</h2>
  <div class="card">{render_line_stats(line_stats)}</div>

  <h2>最佳配置分集表现</h2>
  <div class="card">{render_metrics_table(best_metrics)}</div>

  <h2>Top 回测版本</h2>
  <div class="card">{render_table(ranked.head(15), ["name", "line", "atr_stop_mult", "exit_mode", "test_total_return", "test_profit_factor", "test_win_rate", "test_trade_count", "score"])}</div>

  <h2>成本敏感性</h2>
  <div class="card">{render_table(cost_rows, ["cost_scenario", "all_total_return", "all_profit_factor", "test_total_return", "test_profit_factor"])}</div>

  <h2>10U 风险账本</h2>
  <div class="grid grid-4">
    {kpi("起始资金", money(fixed_book['initial_capital']), "按10000记账")}
    {kpi("每笔风险", money(fixed_book['fixed_risk']), "固定10U")}
    {kpi("最后资金", money(fixed_book['final_capital']), f"累计 {money(fixed_book['total_pnl'])}")}
    {kpi("交易胜负", f"{fixed_book['win_count']} / {fixed_book['loss_count']}", f"胜率 {pct(fixed_book['win_rate'])}")}
  </div>

  <h2>年度与月度</h2>
  <div class="grid grid-2">
    <div class="card"><h3>年度统计</h3>{render_table(yearly, ["year", "trades", "wins", "losses", "win_rate", "pnl", "end_capital"])}</div>
    <div class="card"><h3>最近24个月</h3>{render_table(monthly, ["month", "trades", "wins", "losses", "win_rate", "pnl", "end_capital"])}</div>
  </div>

  <h2>曲线</h2>
  <div class="grid grid-2">
    <div class="card imgbox"><h3>资金曲线</h3><img src="data:image/png;base64,{b64(REPORT_DIR / 'btc_1h_ma55_ema55_short_equity.png')}" alt="equity"></div>
    <div class="card imgbox"><h3>回撤曲线</h3><img src="data:image/png;base64,{b64(REPORT_DIR / 'btc_1h_ma55_ema55_short_drawdown.png')}" alt="drawdown"></div>
  </div>

  <h2>可继续测试的开仓和平仓方案</h2>
  <div class="grid grid-3">
    <div class="card">
      <h3>方案A：纯跌破收盘确认</h3>
      <p>收盘跌破 55MA/55EMA，下一根开盘做空；止损用信号K高点 + 1.0 到 1.5 ATR；止盈先看 1.5R 或 2R。</p>
      <p class="note">优点是执行最干净，缺点是假跌破会比较多。</p>
    </div>
    <div class="card">
      <h3>方案B：跌破后反抽不收回</h3>
      <p>跌破后不立刻追，等 1 到 5 根K线反抽，不能重新站上55线，再跌破反抽低点开空。</p>
      <p class="note">更适合降低假跌破，但交易次数会减少。</p>
    </div>
    <div class="card">
      <h3>方案C：高周期过滤</h3>
      <p>只有 4H EMA20 &lt; EMA50，或价格低于 4H EMA100 时，才允许执行 1H 跌破做空。</p>
      <p class="note">这条是我更推荐的方向，因为 BTC 做空对环境过滤非常敏感。</p>
    </div>
    <div class="card">
      <h3>平仓1：固定R</h3>
      <p>1R 减仓或直接 1.5R / 2R 全平。适合领导看账本，也最容易实盘执行。</p>
    </div>
    <div class="card">
      <h3>平仓2：重新站上55线</h3>
      <p>如果收盘重新站上 55MA/55EMA，说明跌破失败，按收盘价离场。</p>
    </div>
    <div class="card">
      <h3>平仓3：1R保本后ATR追踪</h3>
      <p>先到 1R 后把止损推到开仓价，再用 2ATR 跟踪。适合趋势延伸，但会承受利润回吐。</p>
    </div>
  </div>

  <h2>最佳版本参数</h2>
  <div class="card"><pre>{best_json}</pre></div>

  <h2>结论</h2>
  <div class="callout">
    这套 55MA/55EMA 跌破做空逻辑可以作为“结构转弱信号”继续研究，但单独裸跑不等于最终实盘策略。重点要看测试集是否还能保持 PF、交易次数和回撤；如果裸跌破不稳，下一步更值得做的是加入 4H 空头过滤和“跌破后反抽不收回”的二次确认。
  </div>
</main>
</body>
</html>"""


def render_line_stats(stats: dict[str, dict[str, object]]) -> str:
    rows = []
    for label, item in stats.items():
        rows.append(
            {
                "line": label,
                "ready_bars": item["ready_bars"],
                "above_bars": item["above_bars"],
                "above_pct": item["above_pct"],
                "below_bars": item["below_bars"],
                "below_pct": item["below_pct"],
                "cross_below_count": item["cross_below_count"],
                "cross_above_count": item["cross_above_count"],
                "avg_above_streak": item["avg_above_streak"],
                "avg_below_streak": item["avg_below_streak"],
            }
        )
    return render_table(pd.DataFrame(rows), ["line", "ready_bars", "above_bars", "above_pct", "below_bars", "below_pct", "cross_below_count", "cross_above_count", "avg_above_streak", "avg_below_streak"])


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
        cells = []
        for col in columns:
            value = row[col]
            if isinstance(value, (float, np.floating)):
                if "pct" in col or "rate" in col or "return" in col or "drawdown" in col:
                    text = pct(value)
                elif "factor" in col or col in {"avg_r", "score", "avg_above_streak", "avg_below_streak"}:
                    text = f"{value:.2f}"
                else:
                    text = money(value)
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
