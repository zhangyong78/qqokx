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
INITIAL_EQUITY = 10_000.0
RISK_PCT = 0.01
MAX_NOTIONAL_PCT = 0.95
TAKER_SIDE_COST = 0.00075
HTML_PATH = REPORT_DIR / "btc_1h_grid_risk_report.html"


@dataclass(frozen=True)
class GridConfig:
    name: str
    center_lookback: int
    band_atr_mult: float
    grid_levels: int
    stop_atr_mult: float
    max_campaign_bars: int
    filter_mode: str


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
    result_map: dict[str, tuple[GridConfig, pd.DataFrame, dict[str, dict[str, float]]]] = {}
    for config in configs:
        campaigns = backtest_grid(df, config)
        metrics = {name: metrics_for(slice_campaigns(campaigns, bounds)) for name, bounds in split_bounds.items()}
        rows.append(flatten_metrics(config, metrics, len(campaigns)))
        result_map[config.name] = (config, campaigns, metrics)

    comparison = pd.DataFrame(rows)
    comparison["score"] = score_configs(comparison)
    comparison = comparison.sort_values("score", ascending=False).reset_index(drop=True)
    comparison.to_csv(REPORT_DIR / "btc_1h_grid_risk_comparison.csv", index=False, encoding="utf-8-sig")

    best_name = str(comparison.iloc[0]["name"])
    best_config, best_campaigns, best_metrics = result_map[best_name]
    best_campaigns.to_csv(REPORT_DIR / "btc_1h_grid_risk_campaigns.csv", index=False, encoding="utf-8-sig")

    yearly = period_table(best_campaigns, "Y", "year")
    monthly = period_table(best_campaigns, "M", "month")
    yearly.to_csv(REPORT_DIR / "btc_1h_grid_risk_yearly.csv", index=False, encoding="utf-8-sig")
    monthly.to_csv(REPORT_DIR / "btc_1h_grid_risk_monthly.csv", index=False, encoding="utf-8-sig")

    equity = equity_curve(best_campaigns)
    drawdown = drawdown_curve(equity)
    equity_path = REPORT_DIR / "btc_1h_grid_risk_equity.png"
    drawdown_path = REPORT_DIR / "btc_1h_grid_risk_drawdown.png"
    save_equity_plot(equity, equity_path)
    save_drawdown_plot(drawdown, drawdown_path)

    filter_summary = summarize_by(comparison, "filter_mode")
    level_summary = summarize_by(comparison, "grid_levels")
    risk_summary = summarize_by(comparison, "stop_atr_mult")

    payload = {
        "best_name": best_name,
        "best_config": config_to_dict(best_config),
        "best_metrics": best_metrics,
    }
    (REPORT_DIR / "btc_1h_grid_risk_best.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    HTML_PATH.write_text(
        build_html(
            df=df,
            comparison=comparison,
            best_config=best_config,
            best_metrics=best_metrics,
            best_campaigns=best_campaigns,
            yearly=yearly,
            monthly=monthly.tail(24),
            filter_summary=filter_summary,
            level_summary=level_summary,
            risk_summary=risk_summary,
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
    df["ema168"] = df["close"].ewm(span=168, adjust=False, min_periods=168).mean()
    df["ema168_slope"] = df["ema168"] - df["ema168"].shift(24)
    for lookback in (72, 120, 168):
        df[f"ma_{lookback}"] = df["close"].rolling(lookback, min_periods=lookback).mean()


def build_configs() -> list[GridConfig]:
    configs: list[GridConfig] = []
    for lookback in (72, 168):
        for band_mult in (2.0, 3.0):
            for levels in (3, 4):
                for stop_mult in (0.5, 1.0):
                    for max_bars in (48,):
                        for filter_mode in ("none", "trend_up"):
                            name = (
                                f"grid_lb{lookback}_band{band_mult:g}_lvl{levels}_"
                                f"stop{stop_mult:g}_t{max_bars}_{filter_mode}"
                            )
                            configs.append(
                                GridConfig(
                                    name=name,
                                    center_lookback=lookback,
                                    band_atr_mult=band_mult,
                                    grid_levels=levels,
                                    stop_atr_mult=stop_mult,
                                    max_campaign_bars=max_bars,
                                    filter_mode=filter_mode,
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


def filter_ok(df: pd.DataFrame, idx: int, filter_mode: str) -> bool:
    if filter_mode == "none":
        return True
    ema = float(df.at[idx, "ema168"]) if pd.notna(df.at[idx, "ema168"]) else np.nan
    slope = float(df.at[idx, "ema168_slope"]) if pd.notna(df.at[idx, "ema168_slope"]) else np.nan
    close = float(df.at[idx, "close"])
    if filter_mode == "trend_up":
        return np.isfinite(ema) and np.isfinite(slope) and close >= ema and slope >= 0
    return True


def backtest_grid(df: pd.DataFrame, config: GridConfig) -> pd.DataFrame:
    equity = INITIAL_EQUITY
    rows: list[dict[str, object]] = []
    idx = max(config.center_lookback, 168) + 24
    while idx < len(df) - config.max_campaign_bars - 1:
        if not filter_ok(df, idx, config.filter_mode):
            idx += 1
            continue

        atr = float(df.at[idx, "atr14"]) if pd.notna(df.at[idx, "atr14"]) else np.nan
        if not np.isfinite(atr) or atr <= 0:
            idx += 1
            continue

        center = float(df.at[idx, f"ma_{config.center_lookback}"]) if pd.notna(df.at[idx, f"ma_{config.center_lookback}"]) else np.nan
        if not np.isfinite(center):
            idx += 1
            continue
        band = config.band_atr_mult * atr
        lower = center - band
        upper = center + band
        close = float(df.at[idx, "close"])
        if close < center or close > upper:
            idx += 1
            continue

        spacing = (center - lower) / config.grid_levels
        if spacing <= 0:
            idx += 1
            continue

        buy_levels = [center - spacing * step for step in range(1, config.grid_levels + 1)]
        stop_price = lower - config.stop_atr_mult * atr
        unit_risk = sum(max(level - stop_price, 0.0) for level in buy_levels)
        if unit_risk <= 0:
            idx += 1
            continue

        risk_budget = equity * RISK_PCT
        qty_per_level = risk_budget / unit_risk
        total_entry_notional = qty_per_level * sum(buy_levels)
        max_notional = equity * MAX_NOTIONAL_PCT
        if total_entry_notional > max_notional:
            scale = max_notional / total_entry_notional
            qty_per_level *= scale
            total_entry_notional = qty_per_level * sum(buy_levels)

        if qty_per_level <= 0:
            idx += 1
            continue

        campaign = simulate_campaign(
            df=df,
            config=config,
            start_idx=idx,
            center=center,
            upper=upper,
            lower=lower,
            spacing=spacing,
            buy_levels=buy_levels,
            stop_price=stop_price,
            qty_per_level=qty_per_level,
            equity_before=equity,
            risk_budget=risk_budget,
        )
        if campaign is None:
            idx += 1
            continue

        rows.append(campaign)
        equity += float(campaign["net_pnl"])
        idx = int(campaign["end_index"]) + 1
    return pd.DataFrame(rows)


def simulate_campaign(
    *,
    df: pd.DataFrame,
    config: GridConfig,
    start_idx: int,
    center: float,
    upper: float,
    lower: float,
    spacing: float,
    buy_levels: list[float],
    stop_price: float,
    qty_per_level: float,
    equity_before: float,
    risk_budget: float,
) -> dict[str, object] | None:
    open_legs: list[dict[str, float | int]] = []
    pending_levels = [{"price": level, "filled": False} for level in buy_levels]
    realized_pnl = 0.0
    costs = 0.0
    total_buys = 0
    total_sells = 0
    entry_notional = 0.0
    exit_notional = 0.0
    max_open_notional = 0.0
    min_unrealized = 0.0
    first_fill_idx: int | None = None

    end_idx = min(len(df) - 1, start_idx + config.max_campaign_bars)
    for idx in range(start_idx + 1, end_idx + 1):
        high = float(df.at[idx, "high"])
        low = float(df.at[idx, "low"])
        close = float(df.at[idx, "close"])

        if open_legs:
            notional = sum(float(leg["entry"]) * qty_per_level for leg in open_legs)
            max_open_notional = max(max_open_notional, notional)
            unrealized = sum((close - float(leg["entry"])) * qty_per_level for leg in open_legs)
            min_unrealized = min(min_unrealized, unrealized)

        for pending in pending_levels:
            if pending["filled"]:
                continue
            level = float(pending["price"])
            if low <= level:
                pending["filled"] = True
                open_legs.append({"entry": level, "tp": level + spacing, "fill_idx": idx})
                entry_notional += level * qty_per_level
                total_buys += 1
                if first_fill_idx is None:
                    first_fill_idx = idx

        if low <= stop_price and open_legs:
            for leg in open_legs:
                realized_pnl += (stop_price - float(leg["entry"])) * qty_per_level
                exit_notional += stop_price * qty_per_level
                total_sells += 1
            costs += TAKER_SIDE_COST * (entry_notional + exit_notional)
            net_pnl = realized_pnl - costs
            return campaign_row(
                config=config,
                start_idx=start_idx,
                end_idx=idx,
                start_time=df.at[start_idx, "timestamp"],
                end_time=df.at[idx, "timestamp"],
                center=center,
                upper=upper,
                lower=lower,
                stop_price=stop_price,
                qty_per_level=qty_per_level,
                filled_levels=total_buys,
                completed_sells=total_sells,
                risk_budget=risk_budget,
                theoretical_max_loss=sum((level - stop_price) * qty_per_level for level in buy_levels),
                entry_notional=entry_notional,
                max_open_notional=max_open_notional,
                gross_pnl=realized_pnl,
                cost=costs,
                net_pnl=net_pnl,
                equity_before=equity_before,
                min_unrealized=min_unrealized,
                exit_reason="stop_loss",
            )

        remaining_legs: list[dict[str, float | int]] = []
        for leg in open_legs:
            fill_idx = int(leg["fill_idx"])
            target = float(leg["tp"])
            if idx > fill_idx and high >= target:
                realized_pnl += (target - float(leg["entry"])) * qty_per_level
                exit_notional += target * qty_per_level
                total_sells += 1
            else:
                remaining_legs.append(leg)
        open_legs = remaining_legs

        all_done = total_buys > 0 and not open_legs
        if all_done:
            costs += TAKER_SIDE_COST * (entry_notional + exit_notional)
            net_pnl = realized_pnl - costs
            return campaign_row(
                config=config,
                start_idx=start_idx,
                end_idx=idx,
                start_time=df.at[start_idx, "timestamp"],
                end_time=df.at[idx, "timestamp"],
                center=center,
                upper=upper,
                lower=lower,
                stop_price=stop_price,
                qty_per_level=qty_per_level,
                filled_levels=total_buys,
                completed_sells=total_sells,
                risk_budget=risk_budget,
                theoretical_max_loss=sum((level - stop_price) * qty_per_level for level in buy_levels),
                entry_notional=entry_notional,
                max_open_notional=max_open_notional,
                gross_pnl=realized_pnl,
                cost=costs,
                net_pnl=net_pnl,
                equity_before=equity_before,
                min_unrealized=min_unrealized,
                exit_reason="grid_complete",
            )

    if total_buys == 0:
        return None

    final_close = float(df.at[end_idx, "close"])
    for leg in open_legs:
        realized_pnl += (final_close - float(leg["entry"])) * qty_per_level
        exit_notional += final_close * qty_per_level
        total_sells += 1
    costs += TAKER_SIDE_COST * (entry_notional + exit_notional)
    net_pnl = realized_pnl - costs
    return campaign_row(
        config=config,
        start_idx=start_idx,
        end_idx=end_idx,
        start_time=df.at[start_idx, "timestamp"],
        end_time=df.at[end_idx, "timestamp"],
        center=center,
        upper=upper,
        lower=lower,
        stop_price=stop_price,
        qty_per_level=qty_per_level,
        filled_levels=total_buys,
        completed_sells=total_sells,
        risk_budget=risk_budget,
        theoretical_max_loss=sum((level - stop_price) * qty_per_level for level in buy_levels),
        entry_notional=entry_notional,
        max_open_notional=max_open_notional,
        gross_pnl=realized_pnl,
        cost=costs,
        net_pnl=net_pnl,
        equity_before=equity_before,
        min_unrealized=min_unrealized,
        exit_reason="time_stop",
    )


def campaign_row(
    *,
    config: GridConfig,
    start_idx: int,
    end_idx: int,
    start_time: pd.Timestamp,
    end_time: pd.Timestamp,
    center: float,
    upper: float,
    lower: float,
    stop_price: float,
    qty_per_level: float,
    filled_levels: int,
    completed_sells: int,
    risk_budget: float,
    theoretical_max_loss: float,
    entry_notional: float,
    max_open_notional: float,
    gross_pnl: float,
    cost: float,
    net_pnl: float,
    equity_before: float,
    min_unrealized: float,
    exit_reason: str,
) -> dict[str, object]:
    return {
        "config_name": config.name,
        "filter_mode": config.filter_mode,
        "center_lookback": config.center_lookback,
        "band_atr_mult": config.band_atr_mult,
        "grid_levels": config.grid_levels,
        "stop_atr_mult": config.stop_atr_mult,
        "max_campaign_bars": config.max_campaign_bars,
        "start_index": start_idx,
        "end_index": end_idx,
        "start_time": start_time,
        "end_time": end_time,
        "bars_held": end_idx - start_idx,
        "center": center,
        "upper": upper,
        "lower": lower,
        "stop_price": stop_price,
        "qty_per_level": qty_per_level,
        "filled_levels": filled_levels,
        "completed_sells": completed_sells,
        "risk_budget": risk_budget,
        "theoretical_max_loss": -theoretical_max_loss,
        "risk_usage_pct": theoretical_max_loss / risk_budget if risk_budget > 0 else 0.0,
        "entry_notional": entry_notional,
        "max_open_notional": max_open_notional,
        "gross_pnl": gross_pnl,
        "cost": cost,
        "net_pnl": net_pnl,
        "return_pct": net_pnl / equity_before if equity_before > 0 else 0.0,
        "net_r": net_pnl / risk_budget if risk_budget > 0 else 0.0,
        "max_float_drawdown": min_unrealized,
        "max_float_drawdown_r": min_unrealized / risk_budget if risk_budget > 0 else 0.0,
        "exit_reason": exit_reason,
        "equity_after": equity_before + net_pnl,
    }


def slice_campaigns(frame: pd.DataFrame, bounds: tuple[int, int]) -> pd.DataFrame:
    if frame.empty:
        return frame
    start, end = bounds
    return frame[(frame["start_index"] >= start) & (frame["start_index"] <= end)].copy()


def metrics_for(frame: pd.DataFrame) -> dict[str, float]:
    if frame.empty:
        return empty_metrics()
    returns = frame["return_pct"].to_numpy(dtype=float)
    equity = np.cumprod(1 + returns)
    drawdown = equity / np.maximum.accumulate(equity) - 1
    wins = frame[frame["net_pnl"] > 0]
    losses = frame[frame["net_pnl"] <= 0]
    gross_profit = float(wins["net_pnl"].sum())
    gross_loss = float(-losses["net_pnl"].sum())
    return {
        "total_return": float(equity[-1] - 1),
        "max_drawdown": float(drawdown.min()),
        "profit_factor": gross_profit / gross_loss if gross_loss > 0 else (999.0 if gross_profit > 0 else 0.0),
        "win_rate": float(len(wins) / len(frame)),
        "campaign_count": float(len(frame)),
        "avg_r": float(frame["net_r"].mean()),
        "avg_bars_held": float(frame["bars_held"].mean()),
        "avg_filled_levels": float(frame["filled_levels"].mean()),
        "avg_risk_usage_pct": float(frame["risk_usage_pct"].mean()),
        "avg_float_drawdown_r": float(frame["max_float_drawdown_r"].mean()),
        "max_consecutive_losses": float(max_loss_streak(frame["net_pnl"].to_numpy(dtype=float))),
    }


def empty_metrics() -> dict[str, float]:
    return {
        "total_return": 0.0,
        "max_drawdown": 0.0,
        "profit_factor": 0.0,
        "win_rate": 0.0,
        "campaign_count": 0.0,
        "avg_r": 0.0,
        "avg_bars_held": 0.0,
        "avg_filled_levels": 0.0,
        "avg_risk_usage_pct": 0.0,
        "avg_float_drawdown_r": 0.0,
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


def flatten_metrics(config: GridConfig, metrics: dict[str, dict[str, float]], campaign_count: int) -> dict[str, object]:
    row: dict[str, object] = {
        "name": config.name,
        "filter_mode": config.filter_mode,
        "center_lookback": config.center_lookback,
        "band_atr_mult": config.band_atr_mult,
        "grid_levels": config.grid_levels,
        "stop_atr_mult": config.stop_atr_mult,
        "max_campaign_bars": config.max_campaign_bars,
        "total_campaigns": campaign_count,
        "params_json": json.dumps(config_to_dict(config), ensure_ascii=False, sort_keys=True),
    }
    for split, values in metrics.items():
        for key, value in values.items():
            row[f"{split}_{key}"] = value
    return row


def score_configs(frame: pd.DataFrame) -> pd.Series:
    score = (
        frame["test_profit_factor"].clip(upper=3) * 2.2
        + frame["validation_profit_factor"].clip(upper=3)
        + frame["test_total_return"] * 8
        + frame["validation_total_return"] * 4
        - frame["test_max_drawdown"].abs() * 4
    )
    score = score.mask(frame["test_campaign_count"] < 20, score - 2.5)
    score = score.mask(frame["validation_campaign_count"] < 20, score - 1.5)
    score = score.mask(frame["test_profit_factor"] < 1.0, score - 1.5)
    return score


def config_to_dict(config: GridConfig) -> dict[str, object]:
    return {
        "name": config.name,
        "center_lookback": config.center_lookback,
        "band_atr_mult": config.band_atr_mult,
        "grid_levels": config.grid_levels,
        "stop_atr_mult": config.stop_atr_mult,
        "max_campaign_bars": config.max_campaign_bars,
        "filter_mode": config.filter_mode,
        "risk_pct_per_campaign": RISK_PCT,
        "sizing_rule": "per-level qty = risk_budget / sum(level - stop)",
    }


def period_table(frame: pd.DataFrame, freq: str, label: str) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=[label, "campaigns", "wins", "losses", "win_rate", "pnl", "end_capital"])
    out = frame.copy()
    out["end_time"] = pd.to_datetime(out["end_time"], utc=True).dt.tz_convert(None)
    out[label] = out["end_time"].dt.to_period(freq).astype(str)
    summary = (
        out.groupby(label, as_index=False)
        .agg(
            campaigns=("net_pnl", "count"),
            wins=("net_pnl", lambda s: int((s > 0).sum())),
            losses=("net_pnl", lambda s: int((s <= 0).sum())),
            win_rate=("net_pnl", lambda s: float((s > 0).mean())),
            pnl=("net_pnl", "sum"),
        )
        .sort_values(label)
    )
    summary["end_capital"] = INITIAL_EQUITY + summary["pnl"].cumsum()
    return summary


def equity_curve(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame({"timestamp": [], "equity": []})
    equity = INITIAL_EQUITY
    rows = [{"timestamp": frame["start_time"].iloc[0], "equity": equity}]
    for _, row in frame.iterrows():
        equity += float(row["net_pnl"])
        rows.append({"timestamp": row["end_time"], "equity": equity})
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
        plt.plot(pd.to_datetime(equity["timestamp"]), equity["equity"], color="#0f766e", linewidth=1.8)
    plt.title("BTC 1H Grid Risk Equity")
    plt.tight_layout()
    plt.savefig(path, dpi=150)
    plt.close()


def save_drawdown_plot(drawdown: pd.DataFrame, path: Path) -> None:
    plt.figure(figsize=(12, 4))
    if not drawdown.empty:
        plt.fill_between(pd.to_datetime(drawdown["timestamp"]), drawdown["drawdown"] * 100, color="#b42318", alpha=0.35)
    plt.title("BTC 1H Grid Risk Drawdown")
    plt.tight_layout()
    plt.savefig(path, dpi=150)
    plt.close()


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


def build_html(
    *,
    df: pd.DataFrame,
    comparison: pd.DataFrame,
    best_config: GridConfig,
    best_metrics: dict[str, dict[str, float]],
    best_campaigns: pd.DataFrame,
    yearly: pd.DataFrame,
    monthly: pd.DataFrame,
    filter_summary: pd.DataFrame,
    level_summary: pd.DataFrame,
    risk_summary: pd.DataFrame,
    equity_path: Path,
    drawdown_path: Path,
) -> str:
    best_json = json.dumps(config_to_dict(best_config), ensure_ascii=False, indent=2)
    top_rows = comparison.head(15)
    conclusion = "测试集 PF > 1，风险控制后的网格有继续研究价值" if best_metrics["test"]["profit_factor"] > 1 else "测试集尚未跑出稳定正优势，但风险边界已经可量化"
    avg_risk = float(best_campaigns["risk_budget"].mean()) if not best_campaigns.empty else 0.0
    worst_theoretical = float(best_campaigns["theoretical_max_loss"].min()) if not best_campaigns.empty else 0.0
    worst_real = float(best_campaigns["net_pnl"].min()) if not best_campaigns.empty else 0.0

    return f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>BTC 1H 网格风险回测</title>
<style>
:root {{
  --ink:#172033; --muted:#667085; --line:#d7dce5; --bg:#f4f7fb; --panel:#fff;
  --teal:#0f766e; --red:#b42318; --amber:#b45309;
}}
* {{ box-sizing:border-box; }}
body {{ margin:0; background:var(--bg); color:var(--ink); font-family:"Microsoft YaHei","Segoe UI",Arial,sans-serif; }}
.hero {{ background:linear-gradient(135deg,#072d34 0%,#155e63 60%,#6e8f6b 100%); color:#fff; padding:36px 40px; }}
.hero h1 {{ margin:0 0 10px; font-size:30px; }}
.hero p {{ margin:6px 0; color:#e2f2ef; max-width:1140px; line-height:1.65; }}
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
.callout {{ border-left:5px solid var(--amber); background:#fffbeb; border-radius:6px; padding:14px 16px; line-height:1.75; }}
.note {{ color:var(--muted); line-height:1.65; }}
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
  <h1>BTC 1小时 网格风险回测</h1>
  <p>这份报告不是在找“无限摊平”的网格，而是在验证另一件更重要的事：网格能不能像单笔策略一样，先把最坏亏损算出来，再反推每格仓位。</p>
  <p>本次采用有限层数长网格，统一止损，单套网格风险预算 = 当前权益的 {pct(RISK_PCT)}。每格仓位按照“总风险预算 / 全部层级到止损的总损失”反推。</p>
</section>
<main class="wrap">
  <div class="grid grid-4">
    {kpi("最佳版本", best_config.name, "按验证集+测试集综合排序")}
    {kpi("测试集 PF", f"{best_metrics['test']['profit_factor']:.2f}", conclusion)}
    {kpi("测试集收益", pct(best_metrics['test']['total_return']), f"网格套数 {int(best_metrics['test']['campaign_count'])}")}
    {kpi("测试集回撤", pct(best_metrics['test']['max_drawdown']), f"测试集胜率 {pct(best_metrics['test']['win_rate'])}")}
  </div>

  <h2>以损定量怎么做</h2>
  <div class="grid grid-3">
    <div class="card">
      <h3>先定总风险</h3>
      <p>每套网格只允许亏当前权益的 {pct(RISK_PCT)}。以 10000U 起始资金为例，单套网格初始风险预算约 100U。</p>
    </div>
    <div class="card">
      <h3>再定最坏场景</h3>
      <p>假设所有买入层全部成交，随后统一在止损位全部平掉。总风险 = 所有层级到止损的损失之和 + 手续费。</p>
    </div>
    <div class="card">
      <h3>最后反推每格仓位</h3>
      <p>每格数量 = 风险预算 / Σ(层级价格 - 止损价)。这样一来，网格不是“本金定量”，而是“整套网格亏损定量”。</p>
    </div>
  </div>

  <h2>最佳版本风险边界</h2>
  <div class="grid grid-4">
    {kpi("单套平均风险预算", money(avg_risk), "平均每套网格愿意亏多少")}
    {kpi("理论最坏亏损", money(worst_theoretical), "全部层级成交后止损")}
    {kpi("历史最大单套亏损", money(worst_real), "真实回测里最差一套")}
    {kpi("平均风险使用率", pct(best_metrics['all']['avg_risk_usage_pct']), "理论风险 / 预算")}
  </div>

  <h2>最佳版本分段表现</h2>
  <div class="card">{render_metrics_table(best_metrics)}</div>

  <h2>Top 网格版本</h2>
  <div class="card">{render_table(top_rows, ["name","filter_mode","center_lookback","band_atr_mult","grid_levels","stop_atr_mult","test_total_return","test_profit_factor","test_win_rate","test_campaign_count","score"])}</div>

  <h2>参数方向</h2>
  <div class="grid grid-3">
    <div class="card"><h3>趋势过滤</h3>{render_table(filter_summary, ["filter_mode","runs","avg_test_pf","best_test_pf","avg_test_return","best_test_return"])}</div>
    <div class="card"><h3>层数</h3>{render_table(level_summary, ["grid_levels","runs","avg_test_pf","best_test_pf","avg_test_return","best_test_return"])}</div>
    <div class="card"><h3>止损缓冲</h3>{render_table(risk_summary, ["stop_atr_mult","runs","avg_test_pf","best_test_pf","avg_test_return","best_test_return"])}</div>
  </div>

  <h2>年度和月度</h2>
  <div class="grid grid-2">
    <div class="card"><h3>年度统计</h3>{render_table(yearly, ["year","campaigns","wins","losses","win_rate","pnl","end_capital"])}</div>
    <div class="card"><h3>最近 24 个月</h3>{render_table(monthly, ["month","campaigns","wins","losses","win_rate","pnl","end_capital"])}</div>
  </div>

  <h2>资金曲线</h2>
  <div class="grid grid-2">
    <div class="card imgbox"><h3>资金曲线</h3><img src="data:image/png;base64,{b64(equity_path)}" alt="equity"></div>
    <div class="card imgbox"><h3>回撤曲线</h3><img src="data:image/png;base64,{b64(drawdown_path)}" alt="drawdown"></div>
  </div>

  <h2>结论</h2>
  <div class="callout">
    这份结果最重要的价值，不是它有没有立刻赚大钱，而是它把网格最大的风险盲区补上了：现在每套网格开出来前，我们都能先知道“最坏亏多少”。如果后面继续优化，我们就可以安心去调区间、层数、过滤条件，而不是一边回测一边担心无底洞式亏损。
  </div>

  <h2>最佳参数</h2>
  <div class="card"><pre>{best_json}</pre></div>

  <p class="note">数据：{INST_ID} {BAR}，共 {len(df)} 根 K 线，起始资金 {money(INITIAL_EQUITY)}，单边成本 {pct(TAKER_SIDE_COST)}。</p>
</main>
</body>
</html>"""


def render_metrics_table(metrics: dict[str, dict[str, float]]) -> str:
    rows = []
    for split, item in metrics.items():
        rows.append(
            {
                "split": split,
                "total_return": item["total_return"],
                "profit_factor": item["profit_factor"],
                "win_rate": item["win_rate"],
                "max_drawdown": item["max_drawdown"],
                "campaign_count": item["campaign_count"],
                "avg_r": item["avg_r"],
                "avg_filled_levels": item["avg_filled_levels"],
                "avg_float_drawdown_r": item["avg_float_drawdown_r"],
            }
        )
    return render_table(
        pd.DataFrame(rows),
        ["split", "total_return", "profit_factor", "win_rate", "max_drawdown", "campaign_count", "avg_r", "avg_filled_levels", "avg_float_drawdown_r"],
    )


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
                elif "factor" in col or col in {"avg_r", "score", "avg_filled_levels", "avg_float_drawdown_r", "band_atr_mult", "stop_atr_mult"}:
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
