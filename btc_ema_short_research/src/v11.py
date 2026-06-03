from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from v7 import RiskSchedule


@dataclass(frozen=True)
class LossThrottleRule:
    name: str
    trigger_consecutive_losses: int | None
    reduced_multiplier: float
    description: str


def v11_schedule_names() -> list[str]:
    return [
        "v8_a_flat_1_0",
        "v8_c_strong_1_25_weak_0_5",
        "v8_d_strong_1_5_weak_0_5",
    ]


def v11_loss_throttle_rules() -> list[LossThrottleRule]:
    return [
        LossThrottleRule("v11_a_no_throttle", None, 1.0, "No sequence guardrail. Keep normal risk sizing."),
        LossThrottleRule("v11_b_after_3_losses_half", 3, 0.5, "After 3 consecutive losses, cut risk in half until the next winning trade."),
        LossThrottleRule("v11_c_after_4_losses_half", 4, 0.5, "After 4 consecutive losses, cut risk in half until the next winning trade."),
        LossThrottleRule("v11_d_after_3_losses_quarter", 3, 0.25, "After 3 consecutive losses, cut risk to 25% until the next winning trade."),
    ]


def simulate_loss_throttle_trades(
    trades: pd.DataFrame,
    *,
    initial_capital: float,
    base_risk_per_trade: float,
    schedule: RiskSchedule,
    throttle_rule: LossThrottleRule,
) -> pd.DataFrame:
    if trades.empty:
        return trades.copy()

    out = trades.sort_values("entry_time").reset_index(drop=True).copy()
    equity = float(initial_capital)
    consecutive_losses = 0
    throttle_active = False

    for idx in out.index:
        row = out.loc[idx]
        is_strong = bool(row.get("is_strong_regime", False))
        base_multiplier = schedule.strong_multiplier if is_strong else schedule.weak_multiplier
        throttle_multiplier = throttle_rule.reduced_multiplier if throttle_active and throttle_rule.trigger_consecutive_losses else 1.0
        effective_multiplier = base_multiplier * throttle_multiplier
        risk_amount = equity * base_risk_per_trade * effective_multiplier
        base_risk_amount = float(row["risk_amount"]) if float(row["risk_amount"]) != 0 else 1.0
        scale = risk_amount / base_risk_amount if base_risk_amount else 0.0
        pnl_usdt = float(row["R_multiple"]) * risk_amount
        equity_before = equity
        equity_after = equity + pnl_usdt
        is_loss = pnl_usdt <= 0.0
        throttle_activation_after_trade = False

        out.at[idx, "risk_schedule_name"] = schedule.name
        out.at[idx, "loss_throttle_rule_name"] = throttle_rule.name
        out.at[idx, "base_risk_multiplier"] = base_multiplier
        out.at[idx, "throttle_active_before"] = throttle_active
        out.at[idx, "throttle_multiplier"] = throttle_multiplier
        out.at[idx, "effective_risk_multiplier"] = effective_multiplier
        out.at[idx, "consecutive_losses_before"] = consecutive_losses
        out.at[idx, "equity_before"] = equity_before
        out.at[idx, "risk_amount"] = risk_amount
        out.at[idx, "pnl_usdt"] = pnl_usdt
        out.at[idx, "equity_after"] = equity_after
        out.at[idx, "position_size_btc"] = float(row["position_size_btc"]) * scale
        out.at[idx, "notional_value"] = float(row["notional_value"]) * scale
        out.at[idx, "entry_fee"] = float(row["entry_fee"]) * scale
        out.at[idx, "exit_fee"] = float(row["exit_fee"]) * scale

        if is_loss:
            consecutive_losses += 1
            if (
                throttle_rule.trigger_consecutive_losses is not None
                and not throttle_active
                and consecutive_losses >= throttle_rule.trigger_consecutive_losses
            ):
                throttle_active = True
                throttle_activation_after_trade = True
        else:
            consecutive_losses = 0
            if throttle_active:
                throttle_active = False

        out.at[idx, "consecutive_losses_after"] = consecutive_losses
        out.at[idx, "throttle_activation_after_trade"] = throttle_activation_after_trade
        out.at[idx, "throttle_active_after"] = throttle_active
        equity = equity_after

    return out


def summarize_throttle_usage(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame(
            columns=[
                "strategy_name",
                "schedule_name",
                "loss_throttle_rule_name",
                "activation_count",
                "reduced_risk_trade_count",
                "reduced_risk_win_rate",
                "average_effective_risk_multiplier",
            ]
        )

    rows: list[dict[str, object]] = []
    for keys, group in trades.groupby(["strategy_name", "risk_schedule_name", "loss_throttle_rule_name"], sort=False):
        strategy_name, schedule_name, rule_name = keys
        reduced = group[group["throttle_active_before"] == True].copy()
        rows.append(
            {
                "strategy_name": strategy_name,
                "schedule_name": schedule_name,
                "loss_throttle_rule_name": rule_name,
                "activation_count": int(group["throttle_activation_after_trade"].sum()),
                "reduced_risk_trade_count": int(len(reduced)),
                "reduced_risk_win_rate": float((reduced["pnl_usdt"] > 0).mean()) if not reduced.empty else 0.0,
                "average_effective_risk_multiplier": float(group["effective_risk_multiplier"].mean()),
            }
        )
    return pd.DataFrame(rows).sort_values(
        ["activation_count", "reduced_risk_trade_count", "average_effective_risk_multiplier"],
        ascending=[False, False, False],
    ).reset_index(drop=True)
