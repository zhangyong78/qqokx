from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from strategies import build_strategy_signals


@dataclass(frozen=True)
class V5StrategyDefinition:
    name: str
    description: str


def v5_strategy_definitions() -> list[V5StrategyDefinition]:
    return [
        V5StrategyDefinition("v5_a_baseline", "Baseline daily regime gate"),
        V5StrategyDefinition("v5_b_close_below_ema21", "Baseline plus daily close below EMA21"),
        V5StrategyDefinition("v5_c_rsi_rebound", "Baseline plus daily RSI rebound window"),
        V5StrategyDefinition("v5_d_close_and_rsi", "Baseline plus daily close below EMA21 and RSI rebound"),
        V5StrategyDefinition("v5_e_close_rsi_ema55", "Close below EMA21, RSI rebound, and EMA55 slope down"),
        V5StrategyDefinition("v5_f_close_breakdown", "Close below EMA21 plus breakdown-and-slope state"),
    ]


def build_v5_signals(entry_frame: pd.DataFrame, config: dict[str, object]) -> dict[str, pd.Series]:
    base = build_strategy_signals(entry_frame, config)["strategy_g_dual_bear_volume"]
    return {
        "v5_a_baseline": entry_frame["env_baseline"] & base,
        "v5_b_close_below_ema21": entry_frame["env_close_below_ema21"] & base,
        "v5_c_rsi_rebound": entry_frame["env_rsi_rebound"] & base,
        "v5_d_close_and_rsi": entry_frame["env_close_below_ema21"] & entry_frame["env_rsi_rebound"] & base,
        "v5_e_close_rsi_ema55": entry_frame["env_close_below_ema21"] & entry_frame["env_rsi_rebound"] & entry_frame["env_ema55_down"] & base,
        "v5_f_close_breakdown": entry_frame["env_close_below_ema21"] & entry_frame["env_breakdown_and_slope"] & base,
    }
