from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class MonthlyStopRule:
    name: str
    monthly_loss_limit: float | None
    description: str


def v10_schedule_names() -> list[str]:
    return [
        "v8_a_flat_1_0",
        "v8_c_strong_1_25_weak_0_5",
        "v8_d_strong_1_5_weak_0_5",
    ]


def v10_monthly_stop_rules() -> list[MonthlyStopRule]:
    return [
        MonthlyStopRule("v10_a_no_stop", None, "No monthly stop. Trade every valid setup."),
        MonthlyStopRule("v10_b_stop_0_75pct", 0.0075, "Stop opening new trades for the rest of the month after -0.75% realized loss."),
        MonthlyStopRule("v10_c_stop_1_00pct", 0.0100, "Stop opening new trades for the rest of the month after -1.00% realized loss."),
        MonthlyStopRule("v10_d_stop_1_25pct", 0.0125, "Stop opening new trades for the rest of the month after -1.25% realized loss."),
        MonthlyStopRule("v10_e_stop_1_50pct", 0.0150, "Stop opening new trades for the rest of the month after -1.50% realized loss."),
    ]


def apply_monthly_stop_rule(
    trades: pd.DataFrame,
    *,
    initial_capital: float,
    stop_rule: MonthlyStopRule,
    strategy_name: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if trades.empty:
        return trades.copy(), pd.DataFrame(
            columns=[
                "strategy_name",
                "stop_rule_name",
                "month_label",
                "month_start_equity",
                "month_realized_pnl",
                "month_return",
                "executed_trade_count",
                "skipped_trade_count",
                "triggered",
                "trigger_exit_time",
            ]
        )

    frame = trades.copy().sort_values("entry_time").reset_index(drop=True)
    executed_rows: list[pd.Series] = []
    month_state: dict[pd.Period, dict[str, object]] = {}

    for _, row in frame.iterrows():
        entry_time = pd.to_datetime(row["entry_time"], utc=True)
        exit_time = pd.to_datetime(row["exit_time"], utc=True)
        entry_month = entry_time.tz_convert("UTC").tz_localize(None).to_period("M")
        exit_month = exit_time.tz_convert("UTC").tz_localize(None).to_period("M")

        entry_state = month_state.setdefault(
            entry_month,
            build_month_state(float(row.get("equity_before", initial_capital))),
        )
        if bool(entry_state["triggered"]) and stop_rule.monthly_loss_limit is not None:
            entry_state["skipped_trade_count"] = int(entry_state["skipped_trade_count"]) + 1
            continue

        executed = row.copy()
        executed["monthly_stop_rule_name"] = stop_rule.name
        executed["monthly_stop_active"] = stop_rule.monthly_loss_limit is not None
        executed_rows.append(executed)

        exit_state = month_state.setdefault(
            exit_month,
            build_month_state(float(row.get("equity_before", initial_capital))),
        )
        if float(exit_state["month_start_equity"]) == 0.0:
            exit_state["month_start_equity"] = float(row.get("equity_before", initial_capital))
        exit_state["executed_trade_count"] = int(exit_state["executed_trade_count"]) + 1
        exit_state["month_realized_pnl"] = float(exit_state["month_realized_pnl"]) + float(row["pnl_usdt"])
        exit_state["month_return"] = float(exit_state["month_realized_pnl"]) / float(exit_state["month_start_equity"])

        if (
            stop_rule.monthly_loss_limit is not None
            and not bool(exit_state["triggered"])
            and float(exit_state["month_return"]) <= -float(stop_rule.monthly_loss_limit)
        ):
            exit_state["triggered"] = True
            exit_state["trigger_exit_time"] = exit_time

    executed_frame = pd.DataFrame(executed_rows).reset_index(drop=True) if executed_rows else trades.iloc[0:0].copy()
    month_rows = []
    for month, state in sorted(month_state.items(), key=lambda item: item[0]):
        month_rows.append(
            {
                "strategy_name": strategy_name,
                "stop_rule_name": stop_rule.name,
                "month_label": str(month),
                "month_start_equity": float(state["month_start_equity"]),
                "month_realized_pnl": float(state["month_realized_pnl"]),
                "month_return": float(state["month_return"]),
                "executed_trade_count": int(state["executed_trade_count"]),
                "skipped_trade_count": int(state["skipped_trade_count"]),
                "triggered": bool(state["triggered"]),
                "trigger_exit_time": state["trigger_exit_time"].isoformat() if state["trigger_exit_time"] is not None else "",
            }
        )
    return executed_frame, pd.DataFrame(month_rows)


def build_month_state(start_equity: float) -> dict[str, object]:
    return {
        "month_start_equity": float(start_equity),
        "month_realized_pnl": 0.0,
        "month_return": 0.0,
        "executed_trade_count": 0,
        "skipped_trade_count": 0,
        "triggered": False,
        "trigger_exit_time": None,
    }


def summarize_guardrail_months(months: pd.DataFrame) -> pd.DataFrame:
    if months.empty:
        return pd.DataFrame(
            columns=[
                "strategy_name",
                "stop_rule_name",
                "month_count",
                "triggered_month_count",
                "total_skipped_trades",
                "positive_months",
                "positive_month_rate",
                "worst_month_return",
                "median_month_return",
            ]
        )

    rows: list[dict[str, object]] = []
    for keys, group in months.groupby(["strategy_name", "stop_rule_name"], sort=False):
        strategy_name, stop_rule_name = keys
        rows.append(
            {
                "strategy_name": strategy_name,
                "stop_rule_name": stop_rule_name,
                "month_count": int(len(group)),
                "triggered_month_count": int(group["triggered"].sum()),
                "total_skipped_trades": int(group["skipped_trade_count"].sum()),
                "positive_months": int((group["month_return"] > 0).sum()),
                "positive_month_rate": float((group["month_return"] > 0).mean()),
                "worst_month_return": float(group["month_return"].min()),
                "median_month_return": float(group["month_return"].median()),
            }
        )
    return pd.DataFrame(rows).sort_values(
        ["triggered_month_count", "worst_month_return", "median_month_return"],
        ascending=[True, False, False],
    ).reset_index(drop=True)
