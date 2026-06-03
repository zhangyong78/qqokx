from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import pandas as pd


@dataclass(frozen=True)
class BacktestSettings:
    initial_capital: float
    risk_per_trade: float
    fee_rate: float
    slippage_rate: float
    stop_lookback: int
    stop_atr_multiplier: float


@dataclass(frozen=True)
class ExitRule:
    name: str
    kind: Literal["ema21_reclaim", "fixed_r", "ema21_or_fixed_r", "atr_trail", "atr_trail_or_fixed_r"]
    target_r: float | None = None
    trail_atr_multiplier: float | None = None


def settings_from_config(config: dict[str, object]) -> BacktestSettings:
    return BacktestSettings(
        initial_capital=float(config["initial_capital"]),
        risk_per_trade=float(config["risk_per_trade"]),
        fee_rate=float(config["fee_rate"]),
        slippage_rate=float(config["slippage_rate"]),
        stop_lookback=int(config["stop_lookback"]),
        stop_atr_multiplier=float(config["stop_atr_multiplier"]),
    )


def position_size_for_short(entry_price: float, stop_loss: float, current_equity: float, risk_per_trade: float) -> tuple[float, float]:
    risk_amount = current_equity * risk_per_trade
    stop_distance = abs(stop_loss - entry_price)
    if stop_distance <= 0:
        raise ValueError("stop_distance must be positive")
    return risk_amount / stop_distance, risk_amount


def default_exit_rule() -> ExitRule:
    return ExitRule(name="ema21_reclaim", kind="ema21_reclaim")


def backtest_strategy(
    frame: pd.DataFrame,
    strategy_name: str,
    signal: pd.Series,
    settings: BacktestSettings,
    exit_rule: ExitRule | None = None,
) -> pd.DataFrame:
    trades: list[dict[str, object]] = []
    current_equity = settings.initial_capital
    index = 0
    signal = signal.fillna(False).astype(bool)
    total_bars = len(frame)
    bar_duration_days = infer_bar_duration_days(frame)
    exit_rule = exit_rule or default_exit_rule()

    while index < total_bars - 1:
        if not bool(signal.iloc[index]):
            index += 1
            continue

        entry_index = index + 1
        entry_open = float(frame.at[entry_index, "open"])
        entry_price = entry_open * (1.0 - settings.slippage_rate)
        stop_ref = float(frame.at[index, "highest_high_10"])
        atr_value = float(frame.at[index, "atr14"])
        if pd.isna(stop_ref) or pd.isna(atr_value):
            index += 1
            continue

        stop_loss = stop_ref + settings.stop_atr_multiplier * atr_value
        if stop_loss <= entry_price:
            index += 1
            continue

        position_size_btc, risk_amount = position_size_for_short(
            entry_price=entry_price,
            stop_loss=stop_loss,
            current_equity=current_equity,
            risk_per_trade=settings.risk_per_trade,
        )
        notional_value = entry_price * position_size_btc
        notional_to_equity = notional_value / current_equity if current_equity else 0.0

        exit_index, exit_price, exit_reason = find_exit(
            frame=frame,
            entry_index=entry_index,
            entry_price=entry_price,
            stop_loss=stop_loss,
            risk_per_unit=stop_loss - entry_price,
            slippage_rate=settings.slippage_rate,
            exit_rule=exit_rule,
        )

        entry_fee = notional_value * settings.fee_rate
        exit_notional = exit_price * position_size_btc
        exit_fee = exit_notional * settings.fee_rate
        pnl_usdt = (entry_price - exit_price) * position_size_btc - entry_fee - exit_fee
        r_multiple = pnl_usdt / risk_amount if risk_amount else 0.0
        previous_equity = current_equity
        current_equity = current_equity + pnl_usdt

        trades.append(
            {
                "strategy_name": strategy_name,
                "entry_time": frame.at[entry_index, "timestamp"],
                "entry_price": entry_price,
                "exit_time": frame.at[exit_index, "timestamp"],
                "exit_price": exit_price,
                "stop_loss_price": stop_loss,
                "position_size_btc": position_size_btc,
                "notional_value": notional_value,
                "notional_to_equity": notional_to_equity,
                "risk_amount": risk_amount,
                "pnl_usdt": pnl_usdt,
                "R_multiple": r_multiple,
                "exit_reason": exit_reason,
                "exit_rule_name": exit_rule.name,
                "holding_bars": exit_index - entry_index + 1,
                "holding_days": (exit_index - entry_index + 1) * bar_duration_days,
                "entry_signal_bar_time": frame.at[index, "timestamp"],
                "entry_bar_index": entry_index,
                "exit_bar_index": exit_index,
                "signal_bar_index": index,
                "equity_before": previous_equity,
                "equity_after": current_equity,
                "entry_fee": entry_fee,
                "exit_fee": exit_fee,
            }
        )

        index = max(exit_index, entry_index)

    return pd.DataFrame(trades)


def find_exit(
    frame: pd.DataFrame,
    entry_index: int,
    entry_price: float,
    stop_loss: float,
    risk_per_unit: float,
    slippage_rate: float,
    exit_rule: ExitRule,
) -> tuple[int, float, str]:
    total_bars = len(frame)
    active_stop = stop_loss
    target_price = build_target_price(entry_price=entry_price, risk_per_unit=risk_per_unit, exit_rule=exit_rule)

    for idx in range(entry_index, total_bars):
        intraday_high = float(frame.at[idx, "high"])
        intraday_low = float(frame.at[idx, "low"])
        if intraday_high >= active_stop:
            reason = "stop_loss" if active_stop >= stop_loss else "atr_trailing_stop"
            return idx, active_stop * (1.0 + slippage_rate), reason

        if target_price is not None and intraday_low <= target_price:
            label = format_r_label(exit_rule.target_r)
            return idx, target_price * (1.0 + slippage_rate), f"take_profit_{label}"

        if exit_rule.kind in {"ema21_reclaim", "ema21_or_fixed_r"} and bool(frame.at[idx, "close"] > frame.at[idx, "ema21"]):
            return next_open_or_last_close_exit(frame, idx, slippage_rate, "close_above_ema21")

        if exit_rule.kind in {"atr_trail", "atr_trail_or_fixed_r"}:
            atr_value = float(frame.at[idx, "atr14"])
            if pd.notna(atr_value) and exit_rule.trail_atr_multiplier is not None:
                candidate_stop = intraday_low + (exit_rule.trail_atr_multiplier * atr_value)
                active_stop = min(active_stop, candidate_stop)

    last_index = total_bars - 1
    last_close = float(frame.at[last_index, "close"])
    return last_index, last_close * (1.0 + slippage_rate), "end_of_data"


def build_target_price(entry_price: float, risk_per_unit: float, exit_rule: ExitRule) -> float | None:
    if exit_rule.kind not in {"fixed_r", "ema21_or_fixed_r", "atr_trail_or_fixed_r"}:
        return None
    if exit_rule.target_r is None or risk_per_unit <= 0:
        return None
    return entry_price - (risk_per_unit * exit_rule.target_r)


def next_open_or_last_close_exit(frame: pd.DataFrame, index: int, slippage_rate: float, reason: str) -> tuple[int, float, str]:
    next_index = index + 1
    if next_index < len(frame):
        next_open = float(frame.at[next_index, "open"])
        return next_index, next_open * (1.0 + slippage_rate), reason
    close_price = float(frame.at[index, "close"])
    return index, close_price * (1.0 + slippage_rate), f"{reason}_last_bar"


def format_r_label(value: float | None) -> str:
    if value is None:
        return "unknown"
    text = f"{value:g}"
    return f"{text}R"


def infer_bar_duration_days(frame: pd.DataFrame) -> float:
    if "timestamp" not in frame.columns or len(frame) < 2:
        return 1.0
    diffs = pd.to_datetime(frame["timestamp"], utc=True).sort_values().diff().dropna()
    if diffs.empty:
        return 1.0
    median_seconds = float(diffs.dt.total_seconds().median())
    if median_seconds <= 0:
        return 1.0
    return median_seconds / 86_400.0
