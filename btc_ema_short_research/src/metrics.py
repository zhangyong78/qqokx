from __future__ import annotations

import math
from typing import Iterable

import numpy as np
import pandas as pd


def summarize_strategy(
    strategy_name: str,
    trades: pd.DataFrame,
    frame: pd.DataFrame,
    initial_capital: float,
) -> dict[str, object]:
    equity_curve = build_equity_curve(frame, trades, initial_capital)
    drawdown_curve = build_drawdown_curve(equity_curve)
    daily_returns = equity_curve["equity"].pct_change().fillna(0.0)
    total_return = (equity_curve["equity"].iloc[-1] / initial_capital) - 1.0 if len(equity_curve) else 0.0
    max_drawdown = float(drawdown_curve["drawdown"].min()) if len(drawdown_curve) else 0.0
    gross_profit = float(trades.loc[trades["pnl_usdt"] > 0, "pnl_usdt"].sum()) if not trades.empty else 0.0
    gross_loss = float(-trades.loc[trades["pnl_usdt"] <= 0, "pnl_usdt"].sum()) if not trades.empty else 0.0

    annual_return = annualized_return(
        start_equity=initial_capital,
        end_equity=float(equity_curve["equity"].iloc[-1]) if len(equity_curve) else initial_capital,
        start_time=frame["timestamp"].iloc[0] if len(frame) else None,
        end_time=frame["timestamp"].iloc[-1] if len(frame) else None,
    )
    sharpe = sharpe_ratio(daily_returns)
    sortino = sortino_ratio(daily_returns)
    calmar = annual_return / abs(max_drawdown) if max_drawdown < 0 else 0.0

    return {
        "strategy_name": strategy_name,
        "start_date": frame["timestamp"].iloc[0].isoformat() if len(frame) else "",
        "end_date": frame["timestamp"].iloc[-1].isoformat() if len(frame) else "",
        "initial_capital": initial_capital,
        "final_equity": float(equity_curve["equity"].iloc[-1]) if len(equity_curve) else initial_capital,
        "total_return": total_return,
        "annual_return": annual_return,
        "max_drawdown": max_drawdown,
        "profit_factor": (gross_profit / gross_loss) if gross_loss > 0 else (999.0 if gross_profit > 0 else 0.0),
        "win_rate": float((trades["pnl_usdt"] > 0).mean()) if not trades.empty else 0.0,
        "trade_count": int(len(trades)),
        "average_R": float(trades["R_multiple"].mean()) if not trades.empty else 0.0,
        "median_R": float(trades["R_multiple"].median()) if not trades.empty else 0.0,
        "largest_win_R": float(trades["R_multiple"].max()) if not trades.empty else 0.0,
        "largest_loss_R": float(trades["R_multiple"].min()) if not trades.empty else 0.0,
        "average_holding_days": float(trades["holding_days"].mean()) if not trades.empty else 0.0,
        "max_consecutive_losses": max_consecutive_losses(trades["pnl_usdt"]) if not trades.empty else 0,
        "sharpe": sharpe,
        "sortino": sortino,
        "calmar": calmar,
    }


def build_equity_curve(frame: pd.DataFrame, trades: pd.DataFrame, initial_capital: float) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["timestamp", "equity"])

    curve = pd.DataFrame({"timestamp": frame["timestamp"].copy()})
    curve["equity"] = float(initial_capital)
    if trades.empty:
        return curve

    trades_sorted = trades.sort_values("exit_time").reset_index(drop=True)
    equity = float(initial_capital)
    trade_pointer = 0
    values: list[float] = []
    for timestamp in curve["timestamp"]:
        while trade_pointer < len(trades_sorted) and trades_sorted.at[trade_pointer, "exit_time"] <= timestamp:
            equity = float(trades_sorted.at[trade_pointer, "equity_after"])
            trade_pointer += 1
        values.append(equity)
    curve["equity"] = values
    return curve


def build_drawdown_curve(equity_curve: pd.DataFrame) -> pd.DataFrame:
    out = equity_curve.copy()
    if out.empty:
        out["drawdown"] = []
        return out
    peak = out["equity"].cummax()
    out["drawdown"] = out["equity"] / peak - 1.0
    return out


def build_yearly_performance(
    strategy_name: str,
    trades: pd.DataFrame,
    initial_capital: float,
) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame(
            columns=[
                "year",
                "strategy_name",
                "year_start_equity",
                "year_end_equity",
                "year_return",
                "trade_count",
                "win_rate",
                "max_drawdown",
                "profit_factor",
            ]
        )

    frame = trades.copy().sort_values("exit_time").reset_index(drop=True)
    frame["exit_year"] = pd.to_datetime(frame["exit_time"], utc=True).dt.year
    rows: list[dict[str, object]] = []
    running_equity = float(initial_capital)

    for year, group in frame.groupby("exit_year", sort=True):
        year_start_equity = running_equity
        running_equity = float(group["equity_after"].iloc[-1])
        year_curve = group["equity_after"].astype(float)
        year_drawdown = year_curve / year_curve.cummax() - 1.0
        gross_profit = float(group.loc[group["pnl_usdt"] > 0, "pnl_usdt"].sum())
        gross_loss = float(-group.loc[group["pnl_usdt"] <= 0, "pnl_usdt"].sum())
        rows.append(
            {
                "year": int(year),
                "strategy_name": strategy_name,
                "year_start_equity": year_start_equity,
                "year_end_equity": running_equity,
                "year_return": (running_equity / year_start_equity) - 1.0 if year_start_equity else 0.0,
                "trade_count": int(len(group)),
                "win_rate": float((group["pnl_usdt"] > 0).mean()),
                "max_drawdown": float(year_drawdown.min()) if len(year_drawdown) else 0.0,
                "profit_factor": (gross_profit / gross_loss) if gross_loss > 0 else (999.0 if gross_profit > 0 else 0.0),
            }
        )
    return pd.DataFrame(rows)


def annualized_return(start_equity: float, end_equity: float, start_time: pd.Timestamp | None, end_time: pd.Timestamp | None) -> float:
    if start_equity <= 0 or end_equity <= 0 or start_time is None or end_time is None or end_time <= start_time:
        return 0.0
    years = (end_time - start_time).days / 365.25
    if years <= 0:
        return 0.0
    return (end_equity / start_equity) ** (1 / years) - 1.0


def sharpe_ratio(daily_returns: Iterable[float]) -> float:
    series = pd.Series(list(daily_returns), dtype=float)
    std = float(series.std(ddof=0)) if not series.empty else 0.0
    if not np.isfinite(std) or std == 0.0:
        return 0.0
    return float((series.mean() / std) * math.sqrt(365))


def sortino_ratio(daily_returns: Iterable[float]) -> float:
    series = pd.Series(list(daily_returns), dtype=float)
    downside = series[series < 0]
    downside_std = float(downside.std(ddof=0)) if not downside.empty else 0.0
    if not np.isfinite(downside_std) or downside_std == 0.0:
        return 0.0
    return float((series.mean() / downside_std) * math.sqrt(365))


def max_consecutive_losses(pnls: Iterable[float]) -> int:
    best = 0
    current = 0
    for pnl in pnls:
        if float(pnl) <= 0:
            current += 1
            best = max(best, current)
        else:
            current = 0
    return best


def rank_strategies(comparison: pd.DataFrame) -> pd.DataFrame:
    ranked = comparison.copy()
    sample_factor = (ranked["trade_count"].clip(lower=0.0, upper=30.0) / 30.0).fillna(0.0)
    ranked["score"] = sample_factor * (
        ranked["profit_factor"].clip(upper=4.0) * 2.5
        + ranked["annual_return"] * 8.0
        + ranked["average_R"] * 4.0
        - ranked["max_drawdown"].abs() * 6.0
    )
    ranked.loc[ranked["trade_count"] < 30, "score"] -= 3.0
    ranked.loc[ranked["trade_count"] < 10, "score"] -= 4.0
    ranked.loc[ranked["profit_factor"] <= 1.0, "score"] -= 2.0
    ranked.loc[ranked["average_R"] <= 0.0, "score"] -= 1.0
    return ranked.sort_values("score", ascending=False).reset_index(drop=True)
