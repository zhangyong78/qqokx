from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from strategies import build_strategy_signals


@dataclass(frozen=True)
class V2StrategyDefinition:
    name: str
    description: str


def infer_bar_timedelta(frame: pd.DataFrame) -> pd.Timedelta:
    if "timestamp" not in frame.columns or len(frame) < 2:
        return pd.Timedelta(days=1)
    diffs = pd.to_datetime(frame["timestamp"], utc=True).sort_values().diff().dropna()
    if diffs.empty:
        return pd.Timedelta(days=1)
    median_diff = diffs.median()
    if pd.isna(median_diff) or median_diff <= pd.Timedelta(0):
        return pd.Timedelta(days=1)
    return median_diff


def with_close_time(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out["close_time"] = pd.to_datetime(out["timestamp"], utc=True) + infer_bar_timedelta(out)
    return out


def build_daily_filter_state(daily_frame: pd.DataFrame, config: dict[str, object]) -> pd.DataFrame:
    out = with_close_time(daily_frame)
    dual_bear = (out["ema21"] < out["ema55"]) & (out["close"] < out["ema55"])
    rsi_filter = out["rsi14"] >= float(config["rsi_filter_threshold"])
    volume_filter = out["volume"] >= (out["vol_ma20"] * float(config["volume_filter_multiplier"]))
    ema55_down = out["ema55_slope_5"] < 0

    out["daily_filter_core"] = dual_bear
    out["daily_filter_rsi"] = dual_bear & rsi_filter
    out["daily_filter_volume"] = dual_bear & volume_filter
    out["daily_filter_ema55"] = (out["close"] < out["ema55"]) & ema55_down
    return out[
        [
            "timestamp",
            "close_time",
            "daily_filter_core",
            "daily_filter_rsi",
            "daily_filter_volume",
            "daily_filter_ema55",
        ]
    ].copy()


def align_daily_filters_to_entry_frame(entry_frame: pd.DataFrame, daily_filter_state: pd.DataFrame) -> pd.DataFrame:
    entry = with_close_time(entry_frame)
    aligned = pd.merge_asof(
        entry.sort_values("close_time"),
        daily_filter_state.sort_values("close_time"),
        on="close_time",
        direction="backward",
        suffixes=("", "_daily"),
    ).sort_index()
    filter_columns = [column for column in aligned.columns if column.startswith("daily_filter_")]
    for column in filter_columns:
        aligned[column] = aligned[column].fillna(False).astype(bool)
    return aligned


def v2_strategy_definitions() -> list[V2StrategyDefinition]:
    return [
        V2StrategyDefinition("v2_a_daily_core_4h_ema21_pullback", "Daily dual-bear direction + 4H EMA21 pullback"),
        V2StrategyDefinition("v2_b_daily_core_4h_ema55_pullback", "Daily dual-bear direction + 4H EMA55 pullback"),
        V2StrategyDefinition("v2_c_daily_core_4h_dual_bear_rsi", "Daily dual-bear direction + 4H RSI-filtered pullback"),
        V2StrategyDefinition("v2_d_daily_core_4h_dual_bear_volume", "Daily dual-bear direction + 4H volume-filtered pullback"),
        V2StrategyDefinition("v2_e_daily_volume_4h_dual_bear_volume", "Daily volume-confirmed bear regime + 4H volume pullback"),
        V2StrategyDefinition("v2_f_daily_ema55_4h_ema55_pullback", "Daily EMA55-down direction + 4H EMA55 pullback"),
    ]


def build_v2_signals(entry_frame: pd.DataFrame, config: dict[str, object]) -> dict[str, pd.Series]:
    base = build_strategy_signals(entry_frame, config)
    return {
        "v2_a_daily_core_4h_ema21_pullback": entry_frame["daily_filter_core"] & base["strategy_a_ema21_pullback"],
        "v2_b_daily_core_4h_ema55_pullback": entry_frame["daily_filter_core"] & base["strategy_b_ema55_pullback"],
        "v2_c_daily_core_4h_dual_bear_rsi": entry_frame["daily_filter_core"] & base["strategy_f_dual_bear_rsi"],
        "v2_d_daily_core_4h_dual_bear_volume": entry_frame["daily_filter_core"] & base["strategy_g_dual_bear_volume"],
        "v2_e_daily_volume_4h_dual_bear_volume": entry_frame["daily_filter_volume"] & base["strategy_g_dual_bear_volume"],
        "v2_f_daily_ema55_4h_ema55_pullback": entry_frame["daily_filter_ema55"] & base["strategy_b_ema55_pullback"],
    }
