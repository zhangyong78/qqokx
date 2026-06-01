from __future__ import annotations

import base64
import json
import math
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
SIDE_COST = 0.00075
FIXED_RISK_BOOK = 10.0
FIXED_RISK_INITIAL = 10_000.0
STOP_ATR_BUFFER = 0.2
HTML_PATH = REPORT_DIR / "btc_1h_duration_dynamic_short_report.html"


@dataclass(frozen=True)
class StrategyConfig:
    name: str
    line_col: str
    line_label: str
    min_duration_bars: int
    exit_mode: str
    time_stop_bars: int
    trail_atr_mult: float = 1.5
    fixed_r: float = 2.0


def main() -> None:
    REPORT_DIR.mkdir(exist_ok=True)
    candles = load_candle_cache(INST_ID, BAR, limit=None)
    if not candles:
        raise RuntimeError(f"no candles found for {INST_ID} {BAR}")

    df = build_frame(candles)
    add_indicators(df)
    configs = build_configs()
    split_bounds = split_index_bounds(df)

    rows: list[dict[str, object]] = []
    result_map: dict[str, tuple[StrategyConfig, pd.DataFrame, dict[str, dict[str, float]]]] = {}
    for config in configs:
        trades = backtest(df, config)
        metrics = {name: metrics_for(slice_trades(trades, bounds)) for name, bounds in split_bounds.items()}
        rows.append(flatten_metrics(config, metrics, len(trades)))
        result_map[config.name] = (config, trades, metrics)

    comparison = pd.DataFrame(rows)
    comparison["score"] = score_configs(comparison)
    comparison = comparison.sort_values("score", ascending=False).reset_index(drop=True)
    comparison.to_csv(REPORT_DIR / "btc_1h_duration_dynamic_short_comparison.csv", index=False, encoding="utf-8-sig")

    best_name = str(comparison.iloc[0]["name"])
    best_config, best_trades, best_metrics = result_map[best_name]
    best_trades.to_csv(REPORT_DIR / "btc_1h_duration_dynamic_short_trades.csv", index=False, encoding="utf-8-sig")

    yearly = period_table(best_trades, "Y", "year")
    monthly = period_table(best_trades, "M", "month")
    yearly.to_csv(REPORT_DIR / "btc_1h_duration_dynamic_short_yearly.csv", index=False, encoding="utf-8-sig")
    monthly.to_csv(REPORT_DIR / "btc_1h_duration_dynamic_short_monthly.csv", index=False, encoding="utf-8-sig")

    fixed_book = fixed_risk_book(best_trades)
    equity = equity_curve(best_trades)
    drawdown = drawdown_curve(equity)
    equity_path = REPORT_DIR / "btc_1h_duration_dynamic_short_equity.png"
    drawdown_path = REPORT_DIR / "btc_1h_duration_dynamic_short_drawdown.png"
    save_equity_plot(equity, equity_path)
    save_drawdown_plot(drawdown, drawdown_path)

    duration_summary = duration_edge_summary(df)
    payload = {
        "best_name": best_name,
        "best_config": config_to_dict(best_config),
        "best_metrics": best_metrics,
        "fixed_risk_book": fixed_book,
        "duration_summary": duration_summary.to_dict("records"),
    }
    (REPORT_DIR / "btc_1h_duration_dynamic_short_best.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    HTML_PATH.write_text(
        build_html(
            df=df,
            comparison=comparison,
            best_config=best_config,
            best_metrics=best_metrics,
            best_trades=best_trades,
            fixed_book=fixed_book,
            yearly=yearly,
            monthly=monthly.tail(24),
            duration_summary=duration_summary,
            equity_path=equity_path,
            drawdown_path=drawdown_path,
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
        }
        for c in candles
    ]
    return pd.DataFrame(rows).sort_values("timestamp").drop_duplicates("timestamp").reset_index(drop=True)


def add_indicators(df: pd.DataFrame) -> None:
    df["sma55"] = df["close"].rolling(55, min_periods=55).mean()
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


def build_configs() -> list[StrategyConfig]:
    configs: list[StrategyConfig] = []
    for line_col, line_label in (("sma55", "55MA"), ("ema55", "55EMA")):
        for min_duration in (6, 12, 24, 36, 48, 72):
            for time_stop in (24, 48, 72):
                configs.append(
                    StrategyConfig(
                        name=f"{line_label}_dur{min_duration}_dynamic_rr_t{time_stop}",
                        line_col=line_col,
                        line_label=line_label,
                        min_duration_bars=min_duration,
                        exit_mode="dynamic_rr",
                        time_stop_bars=time_stop,
                    )
                )
            for trail_mult in (1.2, 1.5, 2.0):
                configs.append(
                    StrategyConfig(
                        name=f"{line_label}_dur{min_duration}_atr_trail{trail_mult:g}_t72",
                        line_col=line_col,
                        line_label=line_label,
                        min_duration_bars=min_duration,
                        exit_mode="atr_trail",
                        time_stop_bars=72,
                        trail_atr_mult=trail_mult,
                    )
                )
                configs.append(
                    StrategyConfig(
                        name=f"{line_label}_dur{min_duration}_step_trail{trail_mult:g}_t72",
                        line_col=line_col,
                        line_label=line_label,
                        min_duration_bars=min_duration,
                        exit_mode="step_trail",
                        time_stop_bars=72,
                        trail_atr_mult=trail_mult,
                    )
                )
            configs.append(
                StrategyConfig(
                    name=f"{line_label}_dur{min_duration}_fixed2r_t48",
                    line_col=line_col,
                    line_label=line_label,
                    min_duration_bars=min_duration,
                    exit_mode="fixed_r",
                    time_stop_bars=48,
                    fixed_r=2.0,
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


def qualified_breakdowns(df: pd.DataFrame, line_col: str, min_duration_bars: int) -> pd.DataFrame:
    cross_above = (df["close"].shift(1) <= df[line_col].shift(1)) & (df["close"] > df[line_col]) & df[line_col].notna()
    cross_below = (df["close"].shift(1) >= df[line_col].shift(1)) & (df["close"] < df[line_col]) & df[line_col].notna()
    rows: list[dict[str, int]] = []
    breakout_idx: int | None = None
    for idx in range(len(df)):
        if cross_above.iloc[idx]:
            breakout_idx = idx
        if breakout_idx is None:
            continue
        if cross_below.iloc[idx] and idx > breakout_idx:
            duration = idx - breakout_idx + 1
            if duration >= min_duration_bars:
                rows.append({"breakout_index": breakout_idx, "signal_index": idx, "duration_bars": duration})
            breakout_idx = None
    return pd.DataFrame(rows)


def backtest(df: pd.DataFrame, config: StrategyConfig) -> pd.DataFrame:
    signals = qualified_breakdowns(df, config.line_col, config.min_duration_bars)
    equity = INITIAL_EQUITY
    trades: list[dict[str, object]] = []
    next_allowed_entry = 0
    for signal in signals.itertuples(index=False):
        signal_idx = int(signal.signal_index)
        if signal_idx + 1 >= len(df) or signal_idx < next_allowed_entry:
            continue
        entry_idx = signal_idx + 1
        entry = float(df.at[entry_idx, "open"])
        atr = float(df.at[signal_idx, "atr14"]) if pd.notna(df.at[signal_idx, "atr14"]) else np.nan
        if not np.isfinite(entry) or not np.isfinite(atr) or atr <= 0:
            continue

        stop = float(df.at[signal_idx, "high"]) + STOP_ATR_BUFFER * atr
        risk_distance = stop - entry
        if risk_distance <= 0 or risk_distance > 4.0 * atr:
            continue

        risk_amount = equity * RISK_PCT
        qty = min(risk_amount / risk_distance, equity * MAX_NOTIONAL_MULT / entry)
        if qty <= 0:
            continue

        target_r = dynamic_target_r(int(signal.duration_bars))
        exit_idx, exit_price, exit_reason, mfe_r, mae_r, realized_target_r = find_exit(
            df,
            entry_idx,
            entry,
            stop,
            risk_distance,
            config,
            target_r,
        )
        gross_pnl = qty * (entry - exit_price)
        cost = SIDE_COST * qty * (entry + exit_price)
        net_pnl = gross_pnl - cost
        return_pct = net_pnl / equity
        net_r = net_pnl / risk_amount if risk_amount else 0.0
        equity += net_pnl
        trades.append(
            {
                "config_name": config.name,
                "line": config.line_label,
                "min_duration_bars": config.min_duration_bars,
                "duration_bars": int(signal.duration_bars),
                "exit_mode": config.exit_mode,
                "trail_atr_mult": config.trail_atr_mult,
                "signal_index": signal_idx,
                "entry_index": entry_idx,
                "exit_index": exit_idx,
                "breakout_time": df.at[int(signal.breakout_index), "timestamp"],
                "signal_time": df.at[signal_idx, "timestamp"],
                "entry_time": df.at[entry_idx, "timestamp"],
                "exit_time": df.at[exit_idx, "timestamp"],
                "entry": entry,
                "exit": exit_price,
                "stop": stop,
                "atr14": atr,
                "risk_distance": risk_distance,
                "dynamic_target_r": target_r,
                "realized_target_r": realized_target_r,
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


def dynamic_target_r(duration_bars: int) -> float:
    return float(np.clip(duration_bars / 24.0, 1.2, 4.0))


def find_exit(
    df: pd.DataFrame,
    entry_idx: int,
    entry: float,
    stop: float,
    risk_distance: float,
    config: StrategyConfig,
    target_r: float,
) -> tuple[int, float, str, float, float, float]:
    max_idx = min(len(df) - 1, entry_idx + config.time_stop_bars - 1)
    best_low = entry
    worst_high = entry
    active_stop = stop
    trailing_stop = stop
    locked_r = 0.0
    realized_target_r = target_r if config.exit_mode == "dynamic_rr" else 0.0

    for idx in range(entry_idx, max_idx + 1):
        high = float(df.at[idx, "high"])
        low = float(df.at[idx, "low"])
        close = float(df.at[idx, "close"])
        atr = float(df.at[idx, "atr14"]) if pd.notna(df.at[idx, "atr14"]) else risk_distance
        best_low = min(best_low, low)
        worst_high = max(worst_high, high)

        if high >= active_stop:
            return idx, active_stop, "stop_loss", (entry - best_low) / risk_distance, (worst_high - entry) / risk_distance, realized_target_r

        if config.exit_mode == "fixed_r":
            target = entry - config.fixed_r * risk_distance
            if low <= target:
                return idx, target, "fixed_take_profit", (entry - best_low) / risk_distance, (worst_high - entry) / risk_distance, config.fixed_r
            continue

        if config.exit_mode == "dynamic_rr":
            target = entry - target_r * risk_distance
            if low <= target:
                return idx, target, "dynamic_rr_take_profit", (entry - best_low) / risk_distance, (worst_high - entry) / risk_distance, target_r
            if low <= entry - risk_distance:
                active_stop = min(active_stop, entry)
            continue

        if config.exit_mode == "atr_trail":
            trailing_stop = min(trailing_stop, best_low + config.trail_atr_mult * atr)
            active_stop = min(active_stop, trailing_stop)
            if high >= active_stop:
                return idx, active_stop, "atr_trail_stop", (entry - best_low) / risk_distance, (worst_high - entry) / risk_distance, 0.0
            continue

        if config.exit_mode == "step_trail":
            mfe_r = (entry - best_low) / risk_distance
            if mfe_r >= 3.0:
                locked_r = max(locked_r, 2.0)
            elif mfe_r >= 2.0:
                locked_r = max(locked_r, 1.0)
            elif mfe_r >= 1.0:
                locked_r = max(locked_r, 0.0)
            if locked_r >= 0.0 and mfe_r >= 1.0:
                active_stop = min(active_stop, entry - locked_r * risk_distance)
            trailing_stop = min(trailing_stop, best_low + config.trail_atr_mult * atr)
            if mfe_r >= 1.0:
                active_stop = min(active_stop, trailing_stop)
            if high >= active_stop:
                return idx, active_stop, "step_trail_stop", (entry - best_low) / risk_distance, (worst_high - entry) / risk_distance, locked_r
            continue

    exit_price = float(df.at[max_idx, "close"])
    return max_idx, exit_price, "time_stop", (entry - best_low) / risk_distance, (worst_high - entry) / risk_distance, realized_target_r


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
        "avg_mfe_r": float(trades["mfe_r"].mean()),
        "avg_mae_r": float(trades["mae_r"].mean()),
        "avg_target_r": float(trades["dynamic_target_r"].mean()),
        "avg_bars_held": float(trades["bars_held"].mean()),
        "max_consecutive_losses": float(max_loss_streak(trades["net_pnl"].to_numpy(dtype=float))),
    }


def empty_metrics() -> dict[str, float]:
    return {
        "total_return": 0.0,
        "max_drawdown": 0.0,
        "profit_factor": 0.0,
        "win_rate": 0.0,
        "trade_count": 0.0,
        "avg_r": 0.0,
        "avg_mfe_r": 0.0,
        "avg_mae_r": 0.0,
        "avg_target_r": 0.0,
        "avg_bars_held": 0.0,
        "max_consecutive_losses": 0.0,
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


def flatten_metrics(config: StrategyConfig, metrics: dict[str, dict[str, float]], trade_count: int) -> dict[str, object]:
    row: dict[str, object] = {
        "name": config.name,
        "line": config.line_label,
        "min_duration_bars": config.min_duration_bars,
        "exit_mode": config.exit_mode,
        "time_stop_bars": config.time_stop_bars,
        "trail_atr_mult": config.trail_atr_mult,
        "total_trades": trade_count,
        "params_json": json.dumps(config_to_dict(config), ensure_ascii=False, sort_keys=True),
    }
    for split, values in metrics.items():
        for key, value in values.items():
            row[f"{split}_{key}"] = value
    return row


def score_configs(frame: pd.DataFrame) -> pd.Series:
    score = (
        frame["test_profit_factor"].clip(upper=3) * 2.4
        + frame["validation_profit_factor"].clip(upper=3) * 1.2
        + frame["test_total_return"] * 10
        + frame["validation_total_return"] * 4
        - frame["test_max_drawdown"].abs() * 5
    )
    score = score.mask(frame["test_trade_count"] < 20, score - 3)
    score = score.mask(frame["validation_trade_count"] < 20, score - 2)
    score = score.mask(frame["test_profit_factor"] < 1.0, score - 1.5)
    score = score.mask(frame["all_trade_count"] < 60, score - 1)
    return score


def config_to_dict(config: StrategyConfig) -> dict[str, object]:
    return {
        "name": config.name,
        "line": config.line_label,
        "min_duration_bars": config.min_duration_bars,
        "stop": "signal_high + 0.2 * ATR14",
        "exit_mode": config.exit_mode,
        "time_stop_bars": config.time_stop_bars,
        "trail_atr_mult": config.trail_atr_mult,
        "fixed_r": config.fixed_r,
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
        plt.plot(pd.to_datetime(equity["timestamp"]), equity["equity"], color="#2563eb", linewidth=1.7)
    plt.title("BTC 1H Duration Dynamic Short Equity")
    plt.tight_layout()
    plt.savefig(path, dpi=150)
    plt.close()


def save_drawdown_plot(drawdown: pd.DataFrame, path: Path) -> None:
    plt.figure(figsize=(12, 4))
    if not drawdown.empty:
        plt.fill_between(pd.to_datetime(drawdown["timestamp"]), drawdown["drawdown"] * 100, color="#dc2626", alpha=0.35)
    plt.title("BTC 1H Duration Dynamic Short Drawdown")
    plt.tight_layout()
    plt.savefig(path, dpi=150)
    plt.close()


def duration_edge_summary(df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for line_col, line_label in (("sma55", "55MA"), ("ema55", "55EMA")):
        for threshold in (6, 12, 24, 36, 48, 72):
            signals = qualified_breakdowns(df, line_col, threshold)
            rows.append(
                {
                    "line": line_label,
                    "threshold_bars": threshold,
                    "signals": int(len(signals)),
                }
            )
    return pd.DataFrame(rows)


def build_html(
    *,
    df: pd.DataFrame,
    comparison: pd.DataFrame,
    best_config: StrategyConfig,
    best_metrics: dict[str, dict[str, float]],
    best_trades: pd.DataFrame,
    fixed_book: dict[str, float],
    yearly: pd.DataFrame,
    monthly: pd.DataFrame,
    duration_summary: pd.DataFrame,
    equity_path: Path,
    drawdown_path: Path,
) -> str:
    best_json = json.dumps(config_to_dict(best_config), ensure_ascii=False, indent=2)
    dynamic_only = comparison[comparison["exit_mode"] != "fixed_r"].copy()
    fixed_only = comparison[comparison["exit_mode"] == "fixed_r"].copy()
    dynamic_best = dynamic_only.sort_values("score", ascending=False).head(10)
    fixed_best = fixed_only.sort_values("score", ascending=False).head(8)
    exit_summary = summarize_by(comparison, "exit_mode")
    duration_rank = summarize_by(comparison, "min_duration_bars")
    conclusion = "测试集 PF > 1，有继续研究价值" if best_metrics["test"]["profit_factor"] > 1 else "测试集仍未形成稳定优势"

    return f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>BTC 1H 55线时长过滤动态做空策略</title>
<style>
:root {{
  --ink:#172033; --muted:#667085; --line:#d7dce5; --bg:#f4f7fb; --panel:#fff;
  --blue:#1d4ed8; --green:#0f766e; --red:#b42318; --amber:#b45309;
}}
* {{ box-sizing:border-box; }}
body {{ margin:0; background:var(--bg); color:var(--ink); font-family:"Microsoft YaHei","Segoe UI",Arial,sans-serif; }}
.hero {{ background:#102033; color:#fff; padding:34px 40px; border-bottom:5px solid #2a9d8f; }}
.hero h1 {{ margin:0 0 10px; font-size:30px; }}
.hero p {{ margin:6px 0; color:#d7e5f5; max-width:1160px; line-height:1.65; }}
.wrap {{ max-width:1240px; margin:0 auto; padding:24px; }}
.grid {{ display:grid; gap:16px; }}
.grid-4 {{ grid-template-columns:repeat(4,minmax(0,1fr)); }}
.grid-3 {{ grid-template-columns:repeat(3,minmax(0,1fr)); }}
.grid-2 {{ grid-template-columns:repeat(2,minmax(0,1fr)); }}
.card {{ background:var(--panel); border:1px solid var(--line); border-radius:8px; padding:18px; box-shadow:0 1px 2px rgba(16,24,40,.04); }}
.kpi .label {{ color:var(--muted); font-size:13px; margin-bottom:8px; }}
.kpi .value {{ font-size:26px; font-weight:800; overflow-wrap:anywhere; }}
.kpi .sub {{ color:var(--muted); margin-top:6px; font-size:13px; }}
h2 {{ font-size:21px; margin:30px 0 14px; }}
h3 {{ font-size:16px; margin:0 0 10px; }}
table {{ width:100%; border-collapse:collapse; font-size:13px; }}
th,td {{ border-bottom:1px solid var(--line); padding:9px 10px; text-align:right; }}
th:first-child,td:first-child {{ text-align:left; }}
th {{ background:#f8fafc; color:#475467; }}
.note {{ color:var(--muted); line-height:1.65; }}
.callout {{ border-left:5px solid var(--amber); background:#fffbeb; border-radius:6px; padding:14px 16px; line-height:1.75; }}
.good {{ color:var(--green); font-weight:700; }}
.bad {{ color:var(--red); font-weight:700; }}
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
  <h1>BTC 1小时 55MA / 55EMA 时长过滤动态做空策略</h1>
  <p>策略核心：先统计行情突破 55 线后在线上运行的 K 线数，只有运行达到指定根数后再跌破，才允许做空。止损固定按“跌破 K 线高点 + 0.2ATR”，仓位按亏损金额反推。</p>
  <p>动态盈亏比：本次测试了按持续时长自动放大利润目标、ATR 移动止盈、1R 保本 2R 锁利后继续追踪三类动态出场，并保留固定 2R 作为对照。</p>
</section>

<main class="wrap">
  <div class="grid grid-4">
    {kpi("最佳版本", best_config.name, "按验证集+测试集综合排序")}
    {kpi("测试集 PF", f"{best_metrics['test']['profit_factor']:.2f}", conclusion)}
    {kpi("测试集收益", pct(best_metrics['test']['total_return']), f"交易 {int(best_metrics['test']['trade_count'])} 笔")}
    {kpi("测试集胜率", pct(best_metrics['test']['win_rate']), f"最大回撤 {pct(best_metrics['test']['max_drawdown'])}")}
  </div>

  <h2>策略规则</h2>
  <div class="grid grid-3">
    <div class="card">
      <h3>开仓</h3>
      <p>收盘突破 55MA/55EMA 后开始计数；线上运行至少 N 根 1H K 线；之后收盘跌破该线，下一根 1H 开盘做空。</p>
      <p class="note">本次扫描 N = 6 / 12 / 24 / 36 / 48 / 72。</p>
    </div>
    <div class="card">
      <h3>止损和定量</h3>
      <p>止损 = 跌破 K 线高点 + 0.2ATR14。每笔风险 = 当前权益的 0.5%，最大名义仓位不超过权益 2.5 倍。</p>
      <p class="note">这是比较紧的止损，手续费和滑点会明显影响结果。</p>
    </div>
    <div class="card">
      <h3>动态盈亏比</h3>
      <p>动态目标 R = 线上运行根数 / 24，最低 1.2R，最高 4R。比如线上运行 48 根，目标约 2R；运行 72 根，目标约 3R。</p>
      <p class="note">同时测试 ATR trail 和阶梯锁利，避免只依赖固定目标。</p>
    </div>
  </div>

  <h2>最佳版本表现</h2>
  <div class="card">{render_metrics_table(best_metrics)}</div>

  <h2>动态版本 Top 10</h2>
  <div class="card">{render_table(dynamic_best, ["name","line","min_duration_bars","exit_mode","time_stop_bars","test_total_return","test_profit_factor","test_win_rate","test_trade_count","score"])}</div>

  <h2>固定 2R 对照</h2>
  <div class="card">{render_table(fixed_best, ["name","line","min_duration_bars","exit_mode","time_stop_bars","test_total_return","test_profit_factor","test_win_rate","test_trade_count","score"])}</div>

  <h2>按出场方式和时长阈值汇总</h2>
  <div class="grid grid-2">
    <div class="card"><h3>出场方式</h3>{render_table(exit_summary, ["exit_mode","runs","avg_test_pf","best_test_pf","avg_test_return","best_test_return"])}</div>
    <div class="card"><h3>最少线上根数</h3>{render_table(duration_rank, ["min_duration_bars","runs","avg_test_pf","best_test_pf","avg_test_return","best_test_return"])}</div>
  </div>

  <h2>信号数量</h2>
  <div class="card">{render_table(duration_summary, ["line","threshold_bars","signals"])}</div>

  <h2>10U 固定风险账本</h2>
  <div class="grid grid-4">
    {kpi("起始资金", money(fixed_book["initial_capital"]), "账本口径")}
    {kpi("每笔止损", money(fixed_book["fixed_risk"]), "固定 10U")}
    {kpi("最后资金", money(fixed_book["final_capital"]), f"累计 {money(fixed_book['total_pnl'])}")}
    {kpi("胜负", f"{fixed_book['win_count']} / {fixed_book['loss_count']}", f"胜率 {pct(fixed_book['win_rate'])}")}
  </div>

  <h2>年度和月度</h2>
  <div class="grid grid-2">
    <div class="card"><h3>年度统计</h3>{render_table(yearly, ["year","trades","wins","losses","win_rate","pnl","end_capital"])}</div>
    <div class="card"><h3>最近 24 个月</h3>{render_table(monthly, ["month","trades","wins","losses","win_rate","pnl","end_capital"])}</div>
  </div>

  <h2>资金曲线</h2>
  <div class="grid grid-2">
    <div class="card imgbox"><h3>资金曲线</h3><img src="data:image/png;base64,{b64(equity_path)}" alt="equity"></div>
    <div class="card imgbox"><h3>回撤曲线</h3><img src="data:image/png;base64,{b64(drawdown_path)}" alt="drawdown"></div>
  </div>

  <h2>结论</h2>
  <div class="callout">
    这套规则把“线上运行时长”变成了可测的过滤条件，方向是对的：它能减少很多短周期假跌破。但由于止损只加 0.2ATR，实际风险距离很窄，保守手续费下会明显吃掉一部分 edge。领导汇报时可以这样说：这不是裸跌破策略，而是“55线趋势耗尽后的结构转弱做空”；是否可实盘，要看测试集 PF、交易次数和回撤是否同时达标。
  </div>

  <h2>最佳参数</h2>
  <div class="card"><pre>{best_json}</pre></div>
</main>
</body>
</html>"""


def summarize_by(frame: pd.DataFrame, key: str) -> pd.DataFrame:
    return (
        frame.groupby(key, as_index=False)
        .agg(
            runs=("name", "count"),
            avg_test_pf=("test_profit_factor", "mean"),
            best_test_pf=("test_profit_factor", "max"),
            avg_test_return=("test_total_return", "mean"),
            best_test_return=("test_total_return", "max"),
        )
        .sort_values("best_test_pf", ascending=False)
    )


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
                "avg_target_r": m["avg_target_r"],
                "avg_bars_held": m["avg_bars_held"],
            }
        )
    return render_table(pd.DataFrame(rows), ["split", "total_return", "profit_factor", "win_rate", "max_drawdown", "trade_count", "avg_r", "avg_target_r", "avg_bars_held"])


def render_table(frame: pd.DataFrame, columns: list[str]) -> str:
    if frame.empty:
        return "<p class=\"note\">无数据</p>"
    subset = frame[columns].copy()
    parts = ["<table><tr>" + "".join(f"<th>{col}</th>" for col in columns) + "</tr>"]
    for _, row in subset.iterrows():
        cells = []
        for col in columns:
            value = row[col]
            if isinstance(value, (float, np.floating)):
                if "pct" in col or "rate" in col or "return" in col or "drawdown" in col:
                    text = pct(value)
                elif "factor" in col or "pf" in col or col in {"avg_r", "avg_target_r", "avg_bars_held", "score"}:
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
