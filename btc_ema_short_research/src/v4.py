from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from mtf import with_close_time
from strategies import build_strategy_signals


@dataclass(frozen=True)
class V4StrategyDefinition:
    name: str
    description: str


def build_daily_environment_state(daily_frame: pd.DataFrame, config: dict[str, object]) -> pd.DataFrame:
    out = with_close_time(daily_frame)
    daily_core = (out["ema21"] < out["ema55"]) & (out["close"] < out["ema55"])
    volume_ok = out["volume"] >= (out["vol_ma20"] * float(config["volume_filter_multiplier"]))
    ema55_down = out["ema55_slope_5"] < 0
    close_below_ema21 = out["close"] < out["ema21"]
    rsi_rebound = out["rsi14"] >= float(config.get("v4_daily_rsi_min", 38.0))
    rsi_not_overheated = out["rsi14"] <= float(config.get("v4_daily_rsi_max", 55.0))
    atr14_median_100 = out["atr14"].rolling(100, min_periods=100).median()
    atr_expansion = out["atr14"] >= atr14_median_100
    volume_strong = out["volume"] >= (out["vol_ma20"] * float(config.get("v4_daily_volume_strong_multiplier", 1.2)))
    bear_gap_pct = (out["ema55"] - out["ema21"]) / out["close"].replace(0.0, pd.NA)
    trend_gap_strong = bear_gap_pct >= float(config.get("v4_daily_trend_gap_min", 0.01))
    close_breakdown = out["close"] < out["low_prev"]

    state = out[["timestamp", "close_time"]].copy()
    state["env_daily_core"] = daily_core.fillna(False).astype(bool)
    state["env_daily_volume_ok"] = volume_ok.fillna(False).astype(bool)
    state["env_daily_ema55_down"] = ema55_down.fillna(False).astype(bool)
    state["env_daily_close_below_ema21"] = close_below_ema21.fillna(False).astype(bool)
    state["env_daily_rsi_rebound"] = (rsi_rebound & rsi_not_overheated).fillna(False).astype(bool)
    state["env_daily_atr_expansion"] = atr_expansion.fillna(False).astype(bool)
    state["env_daily_volume_strong"] = volume_strong.fillna(False).astype(bool)
    state["env_daily_trend_gap_strong"] = trend_gap_strong.fillna(False).astype(bool)
    state["env_daily_close_breakdown"] = close_breakdown.fillna(False).astype(bool)

    state["env_baseline"] = state["env_daily_core"] & state["env_daily_volume_ok"]
    state["env_close_below_ema21"] = state["env_baseline"] & state["env_daily_close_below_ema21"]
    state["env_ema55_down"] = state["env_baseline"] & state["env_daily_ema55_down"]
    state["env_rsi_rebound"] = state["env_baseline"] & state["env_daily_rsi_rebound"]
    state["env_atr_expansion"] = state["env_baseline"] & state["env_daily_atr_expansion"]
    state["env_volume_strong"] = state["env_baseline"] & state["env_daily_volume_strong"]
    state["env_trend_gap_strong"] = state["env_baseline"] & state["env_daily_trend_gap_strong"]
    state["env_breakdown_and_slope"] = state["env_baseline"] & state["env_daily_close_breakdown"] & state["env_daily_ema55_down"]
    return state


def align_daily_environment_to_entry_frame(entry_frame: pd.DataFrame, environment_state: pd.DataFrame) -> pd.DataFrame:
    entry = with_close_time(entry_frame)
    aligned = pd.merge_asof(
        entry.sort_values("close_time"),
        environment_state.sort_values("close_time"),
        on="close_time",
        direction="backward",
        suffixes=("", "_daily"),
    ).sort_index()
    env_columns = [column for column in aligned.columns if column.startswith("env_")]
    for column in env_columns:
        aligned[column] = aligned[column].fillna(False).astype(bool)
    return aligned


def v4_strategy_definitions() -> list[V4StrategyDefinition]:
    return [
        V4StrategyDefinition("v4_a_baseline", "Baseline V2/V3 daily environment"),
        V4StrategyDefinition("v4_b_close_below_ema21", "Baseline plus daily close below EMA21"),
        V4StrategyDefinition("v4_c_ema55_down", "Baseline plus daily EMA55 slope negative"),
        V4StrategyDefinition("v4_d_rsi_rebound", "Baseline plus daily RSI rebound window"),
        V4StrategyDefinition("v4_e_atr_expansion", "Baseline plus daily ATR expansion"),
        V4StrategyDefinition("v4_f_volume_strong", "Baseline plus stronger daily volume"),
        V4StrategyDefinition("v4_g_trend_gap_strong", "Baseline plus stronger EMA21/EMA55 separation"),
        V4StrategyDefinition("v4_h_breakdown_and_slope", "Baseline plus daily breakdown and EMA55 down slope"),
    ]


def build_v4_signals(entry_frame: pd.DataFrame, config: dict[str, object]) -> dict[str, pd.Series]:
    base = build_strategy_signals(entry_frame, config)["strategy_g_dual_bear_volume"]
    return {
        "v4_a_baseline": entry_frame["env_baseline"] & base,
        "v4_b_close_below_ema21": entry_frame["env_close_below_ema21"] & base,
        "v4_c_ema55_down": entry_frame["env_ema55_down"] & base,
        "v4_d_rsi_rebound": entry_frame["env_rsi_rebound"] & base,
        "v4_e_atr_expansion": entry_frame["env_atr_expansion"] & base,
        "v4_f_volume_strong": entry_frame["env_volume_strong"] & base,
        "v4_g_trend_gap_strong": entry_frame["env_trend_gap_strong"] & base,
        "v4_h_breakdown_and_slope": entry_frame["env_breakdown_and_slope"] & base,
    }
