from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class RiskSchedule:
    name: str
    strong_multiplier: float
    weak_multiplier: float
    description: str


def v7_risk_schedules() -> list[RiskSchedule]:
    return [
        RiskSchedule("v7_a_flat_1_0", 1.0, 1.0, "Flat baseline risk on every trade"),
        RiskSchedule("v7_b_strong_1_0_weak_0_5", 1.0, 0.5, "Full risk on strong regime, half risk on weak regime"),
        RiskSchedule("v7_c_strong_1_25_weak_0_5", 1.25, 0.5, "1.25x risk on strong regime, half risk on weak regime"),
        RiskSchedule("v7_d_strong_1_5_weak_0_5", 1.5, 0.5, "1.5x risk on strong regime, half risk on weak regime"),
    ]


def tag_strong_regime_trades(trades: pd.DataFrame, strong_signal_times: pd.Series) -> pd.DataFrame:
    if trades.empty:
        out = trades.copy()
        out["is_strong_regime"] = []
        return out
    out = trades.copy()
    signal_times = set(pd.to_datetime(strong_signal_times, utc=True))
    out["signal_time"] = pd.to_datetime(out["entry_signal_bar_time"], utc=True)
    out["is_strong_regime"] = out["signal_time"].isin(signal_times)
    return out


def simulate_dynamic_risk_trades(
    trades: pd.DataFrame,
    *,
    initial_capital: float,
    base_risk_per_trade: float,
    schedule: RiskSchedule,
) -> pd.DataFrame:
    if trades.empty:
        return trades.copy()

    out = trades.sort_values("entry_time").reset_index(drop=True).copy()
    equity = float(initial_capital)

    for idx in out.index:
        row = out.loc[idx]
        is_strong = bool(row.get("is_strong_regime", False))
        multiplier = schedule.strong_multiplier if is_strong else schedule.weak_multiplier
        risk_amount = equity * base_risk_per_trade * multiplier
        base_risk_amount = float(row["risk_amount"]) if float(row["risk_amount"]) != 0 else 1.0
        scale = risk_amount / base_risk_amount if base_risk_amount else 0.0
        pnl_usdt = float(row["R_multiple"]) * risk_amount
        equity_before = equity
        equity_after = equity + pnl_usdt

        out.at[idx, "risk_schedule_name"] = schedule.name
        out.at[idx, "risk_multiplier"] = multiplier
        out.at[idx, "equity_before"] = equity_before
        out.at[idx, "risk_amount"] = risk_amount
        out.at[idx, "pnl_usdt"] = pnl_usdt
        out.at[idx, "equity_after"] = equity_after
        out.at[idx, "position_size_btc"] = float(row["position_size_btc"]) * scale
        out.at[idx, "notional_value"] = float(row["notional_value"]) * scale
        out.at[idx, "entry_fee"] = float(row["entry_fee"]) * scale
        out.at[idx, "exit_fee"] = float(row["exit_fee"]) * scale
        equity = equity_after

    return out
