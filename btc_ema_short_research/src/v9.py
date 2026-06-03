from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import pandas as pd


@dataclass(frozen=True)
class V9Selection:
    schedule_name: str
    cost_scenario_name: str
    description: str


def v9_selections() -> list[V9Selection]:
    return [
        V9Selection("v8_a_flat_1_0", "base_cost", "Flat baseline under original cost assumptions"),
        V9Selection("v8_a_flat_1_0", "stress_cost_2_0x", "Flat baseline under harsh cost assumptions"),
        V9Selection("v8_c_strong_1_25_weak_0_5", "stress_cost_2_0x", "Moderate dynamic sizing under harsh cost assumptions"),
        V9Selection("v8_d_strong_1_5_weak_0_5", "base_cost", "Best dynamic sizing under original costs"),
        V9Selection("v8_d_strong_1_5_weak_0_5", "stress_cost_2_0x", "Best dynamic sizing under harsh cost assumptions"),
    ]


def build_period_performance(
    trades: pd.DataFrame,
    *,
    initial_capital: float,
    freq: Literal["M", "Q"],
    strategy_name: str,
) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame(
            columns=[
                "strategy_name",
                "period_freq",
                "period_label",
                "period_start_equity",
                "period_end_equity",
                "period_return",
                "trade_count",
                "win_rate",
                "max_drawdown",
                "profit_factor",
            ]
        )

    frame = trades.copy().sort_values("exit_time").reset_index(drop=True)
    exit_times = pd.to_datetime(frame["exit_time"], utc=True)
    periods = exit_times.dt.tz_convert("UTC").dt.tz_localize(None).dt.to_period(freq)
    rows: list[dict[str, object]] = []
    running_equity = float(initial_capital)

    for period, group in frame.groupby(periods, sort=True):
        period_start_equity = running_equity
        running_equity = float(group["equity_after"].iloc[-1])
        curve = pd.concat(
            [
                pd.Series([period_start_equity], dtype=float),
                group["equity_after"].astype(float).reset_index(drop=True),
            ],
            ignore_index=True,
        )
        drawdown = curve / curve.cummax() - 1.0
        gross_profit = float(group.loc[group["pnl_usdt"] > 0, "pnl_usdt"].sum())
        gross_loss = float(-group.loc[group["pnl_usdt"] <= 0, "pnl_usdt"].sum())
        rows.append(
            {
                "strategy_name": strategy_name,
                "period_freq": freq,
                "period_label": str(period),
                "period_start_equity": period_start_equity,
                "period_end_equity": running_equity,
                "period_return": (running_equity / period_start_equity) - 1.0 if period_start_equity else 0.0,
                "trade_count": int(len(group)),
                "win_rate": float((group["pnl_usdt"] > 0).mean()),
                "max_drawdown": float(drawdown.min()) if len(drawdown) else 0.0,
                "profit_factor": (gross_profit / gross_loss) if gross_loss > 0 else (999.0 if gross_profit > 0 else 0.0),
            }
        )
    return pd.DataFrame(rows)


def build_streak_pressure(trades: pd.DataFrame, *, strategy_name: str) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame(
            [
                {
                    "strategy_name": strategy_name,
                    "trade_count": 0,
                    "max_consecutive_losses": 0,
                    "worst_consecutive_loss_sum": 0.0,
                    "worst_3_trade_pnl": 0.0,
                    "worst_5_trade_pnl": 0.0,
                    "worst_10_trade_pnl": 0.0,
                }
            ]
        )

    frame = trades.copy().sort_values("entry_time").reset_index(drop=True)
    pnl = frame["pnl_usdt"].astype(float)
    max_losses = 0
    current_losses = 0
    current_loss_sum = 0.0
    worst_loss_sum = 0.0

    for value in pnl:
        if value <= 0:
            current_losses += 1
            current_loss_sum += value
            max_losses = max(max_losses, current_losses)
            worst_loss_sum = min(worst_loss_sum, current_loss_sum)
        else:
            current_losses = 0
            current_loss_sum = 0.0

    return pd.DataFrame(
        [
            {
                "strategy_name": strategy_name,
                "trade_count": int(len(frame)),
                "max_consecutive_losses": int(max_losses),
                "worst_consecutive_loss_sum": float(worst_loss_sum),
                "worst_3_trade_pnl": rolling_min_sum(pnl, 3),
                "worst_5_trade_pnl": rolling_min_sum(pnl, 5),
                "worst_10_trade_pnl": rolling_min_sum(pnl, 10),
            }
        ]
    )


def rolling_min_sum(series: pd.Series, window: int) -> float:
    if len(series) < window or window <= 0:
        return float(series.sum()) if len(series) else 0.0
    return float(series.rolling(window).sum().min())


def summarize_periods(periods: pd.DataFrame) -> pd.DataFrame:
    if periods.empty:
        return pd.DataFrame(
            columns=[
                "strategy_name",
                "schedule_name",
                "cost_scenario_name",
                "period_freq",
                "period_count",
                "positive_periods",
                "positive_period_rate",
                "median_period_return",
                "worst_period_return",
                "worst_period_drawdown",
                "median_profit_factor",
                "average_trade_count",
            ]
        )

    rows: list[dict[str, object]] = []
    group_columns = ["strategy_name", "schedule_name", "cost_scenario_name", "period_freq"]
    for keys, group in periods.groupby(group_columns, sort=False):
        strategy_name, schedule_name, cost_scenario_name, period_freq = keys
        rows.append(
            {
                "strategy_name": strategy_name,
                "schedule_name": schedule_name,
                "cost_scenario_name": cost_scenario_name,
                "period_freq": period_freq,
                "period_count": int(len(group)),
                "positive_periods": int((group["period_return"] > 0).sum()),
                "positive_period_rate": float((group["period_return"] > 0).mean()),
                "median_period_return": float(group["period_return"].median()),
                "worst_period_return": float(group["period_return"].min()),
                "worst_period_drawdown": float(group["max_drawdown"].min()),
                "median_profit_factor": float(group["profit_factor"].median()),
                "average_trade_count": float(group["trade_count"].mean()),
            }
        )
    return pd.DataFrame(rows).sort_values(
        ["period_freq", "median_period_return", "median_profit_factor"],
        ascending=[True, False, False],
    ).reset_index(drop=True)
