from __future__ import annotations

import numpy as np
import pandas as pd


def attach_intraday_features(*, session_daily: pd.DataFrame, session_hourly: pd.DataFrame) -> pd.DataFrame:
    feature_rows = []
    daily_map = session_daily.set_index("session_date")
    for session_date, group in session_hourly.groupby("session_date", sort=True):
        if session_date not in daily_map.index:
            continue
        daily = daily_map.loc[session_date]
        direction = "bull" if daily["close"] > daily["open"] else "bear" if daily["close"] < daily["open"] else "flat"
        sorted_group = group.sort_values("timestamp").reset_index(drop=True)
        current_bj_date = sorted_group["bj_date"].iloc[0]
        feature_rows.append(
            {
                "session_date": session_date,
                "day_low_hour": _extreme_hour(sorted_group, value_col="low", find_max=False),
                "day_high_hour": _extreme_hour(sorted_group, value_col="high", find_max=True),
                "last_below_open_hour": _last_cross_hour(sorted_group, daily_open=daily["open"], is_below=True),
                "last_above_open_hour": _last_cross_hour(sorted_group, daily_open=daily["open"], is_below=False),
                "extension_to_22h": _extension_after_22h(sorted_group, daily_open=daily["open"], daily_close=daily["close"], current_bj_date=current_bj_date, direction=direction),
                "extension_to_next_06h": _extension_to_next_06h(session_hourly, daily_open=daily["open"], daily_close=daily["close"], current_bj_date=current_bj_date, direction=direction),
            }
        )
    features = pd.DataFrame(feature_rows)
    return session_daily.merge(features, on="session_date", how="left")


def _extreme_hour(group: pd.DataFrame, *, value_col: str, find_max: bool) -> str | None:
    idx = group[value_col].idxmax() if find_max else group[value_col].idxmin()
    if pd.isna(idx):
        return None
    return _format_hour(group.loc[idx, "bj_hour"])


def _last_cross_hour(group: pd.DataFrame, *, daily_open: float, is_below: bool) -> str | None:
    mask = group["close"] < daily_open if is_below else group["close"] > daily_open
    matches = group.loc[mask]
    if matches.empty:
        return None
    return _format_hour(matches.iloc[-1]["bj_hour"])


def _extension_after_22h(
    group: pd.DataFrame,
    *,
    daily_open: float,
    daily_close: float,
    current_bj_date: pd.Timestamp,
    direction: str,
) -> float | None:
    if direction == "flat":
        return None
    window = group.loc[(group["bj_date"] == current_bj_date) & (group["bj_hour"] >= 22)]
    if window.empty:
        return None
    return _compute_extension(window=window, daily_open=daily_open, daily_close=daily_close, direction=direction)


def _extension_to_next_06h(
    session_hourly: pd.DataFrame,
    *,
    daily_open: float,
    daily_close: float,
    current_bj_date: pd.Timestamp,
    direction: str,
) -> float | None:
    if direction == "flat":
        return None
    next_bj_date = current_bj_date + pd.Timedelta(days=1)
    window = session_hourly.loc[(session_hourly["bj_date"] == next_bj_date) & (session_hourly["bj_hour"] <= 6)]
    if window.empty:
        return None
    return _compute_extension(window=window, daily_open=daily_open, daily_close=daily_close, direction=direction)


def _compute_extension(
    *,
    window: pd.DataFrame,
    daily_open: float,
    daily_close: float,
    direction: str,
) -> float | None:
    anchor = daily_close
    if direction == "bull":
        move = (window["high"].max() - anchor) / daily_open
    else:
        move = (anchor - window["low"].min()) / daily_open
    return float(max(0, move))


def _format_hour(hour_value: float | int) -> str:
    if pd.isna(hour_value):
        return ""
    return f"{int(hour_value):02d}:00"
