from __future__ import annotations

import numpy as np
import pandas as pd

from stats.indicators import compute_atr, compute_compression_score, compute_ema
from stats.intraday_features import attach_intraday_features


DAY_TYPES = [
    "turn_bull",
    "mid_bull",
    "big_bull",
    "turn_bear",
    "mid_bear",
    "big_bear",
]


def build_daily_samples(
    *,
    hourly_frame: pd.DataFrame,
    daily_frame: pd.DataFrame | None,
    symbol: str | None,
    close_mode: str,
) -> pd.DataFrame:
    offset_hours = 8 if close_mode.lower() == "utc+8" else 0
    session_hourly = _prepare_session_hourly(hourly_frame, offset_hours=offset_hours)
    session_daily = _build_session_daily(session_hourly)
    if daily_frame is not None and close_mode.lower() == "utc+0":
        session_daily = _merge_daily_reference(session_daily, daily_frame)

    session_daily["atr20"] = compute_atr(
        high=session_daily["high"],
        low=session_daily["low"],
        close=session_daily["close"],
        period=20,
    )
    session_daily["ema200"] = compute_ema(session_daily["close"], period=200)
    session_daily["trend_type"] = _classify_trend(session_daily["close"], session_daily["ema200"])
    compression_by_date = compute_compression_score(session_hourly)
    session_daily["compression_score"] = session_daily["session_date"].map(compression_by_date)

    session_daily = _add_shape_metrics(session_daily)
    session_daily["day_type"] = _classify_day_types(session_daily)
    session_daily = attach_intraday_features(session_daily=session_daily, session_hourly=session_hourly)
    session_daily["symbol"] = symbol or _infer_symbol(hourly_frame)
    session_daily["close_mode"] = close_mode.lower()
    session_daily["date"] = session_daily["session_date"].dt.strftime("%Y-%m-%d")

    samples = session_daily.loc[session_daily["day_type"].isin(DAY_TYPES), [
        "date",
        "symbol",
        "close_mode",
        "day_type",
        "trend_type",
        "daily_range_pct",
        "body_ratio",
        "upper_shadow_ratio",
        "lower_shadow_ratio",
        "day_low_hour",
        "day_high_hour",
        "last_below_open_hour",
        "last_above_open_hour",
        "extension_to_22h",
        "extension_to_next_06h",
        "atr20",
        "compression_score",
    ]].copy()
    return samples.reset_index(drop=True)


def _prepare_session_hourly(hourly_frame: pd.DataFrame, *, offset_hours: int) -> pd.DataFrame:
    frame = hourly_frame.copy().sort_values("timestamp").reset_index(drop=True)
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    frame["bj_timestamp"] = frame["timestamp"] + pd.Timedelta(hours=8)
    frame["bj_date"] = frame["bj_timestamp"].dt.floor("D")
    shifted = frame["timestamp"] + pd.Timedelta(hours=offset_hours)
    frame["session_date"] = shifted.dt.floor("D")
    frame["session_hour"] = shifted.dt.hour
    frame["bj_hour"] = frame["bj_timestamp"].dt.hour
    return frame


def _build_session_daily(session_hourly: pd.DataFrame) -> pd.DataFrame:
    daily = (
        session_hourly.groupby("session_date", as_index=False)
        .agg(
            open=("open", "first"),
            high=("high", "max"),
            low=("low", "min"),
            close=("close", "last"),
            volume=("volume", "sum"),
            session_start=("timestamp", "first"),
        )
    )
    return daily


def _merge_daily_reference(session_daily: pd.DataFrame, daily_frame: pd.DataFrame) -> pd.DataFrame:
    reference = daily_frame.copy().sort_values("timestamp").reset_index(drop=True)
    reference["timestamp"] = pd.to_datetime(reference["timestamp"], utc=True)
    reference["session_date"] = reference["timestamp"].dt.floor("D")
    merged = session_daily.merge(
        reference[["session_date", "open", "high", "low", "close", "volume"]],
        on="session_date",
        how="left",
        suffixes=("", "_ref"),
    )
    for column in ("open", "high", "low", "close", "volume"):
        merged[column] = merged[f"{column}_ref"].combine_first(merged[column])
        merged.drop(columns=f"{column}_ref", inplace=True)
    return merged


def _add_shape_metrics(daily: pd.DataFrame) -> pd.DataFrame:
    frame = daily.copy()
    day_range = (frame["high"] - frame["low"]).replace(0, np.nan)
    body = (frame["close"] - frame["open"]).abs()
    frame["daily_range"] = frame["high"] - frame["low"]
    frame["daily_range_pct"] = frame["daily_range"] / frame["open"].replace(0, np.nan)
    frame["body_ratio"] = body / day_range
    frame["upper_shadow_ratio"] = (frame["high"] - frame[["open", "close"]].max(axis=1)) / day_range
    frame["lower_shadow_ratio"] = (frame[["open", "close"]].min(axis=1) - frame["low"]) / day_range
    return frame


def _classify_trend(close: pd.Series, ema200: pd.Series) -> pd.Series:
    trend = np.where(close > ema200 * 1.01, "uptrend", np.where(close < ema200 * 0.99, "downtrend", "sideways"))
    return pd.Series(trend, index=close.index)


def _classify_day_types(daily: pd.DataFrame) -> pd.Series:
    previous = daily.shift(1)
    previous_2 = daily.shift(2)
    previous_3 = daily.shift(3)
    range_nonzero = daily["daily_range"].replace(0, np.nan)

    bullish = daily["close"] > daily["open"]
    bearish = daily["close"] < daily["open"]
    body_ratio = daily["body_ratio"]
    atr20 = daily["atr20"]

    big_bull = bullish & (body_ratio >= 0.6) & ((daily["high"] - daily["close"]) <= range_nonzero * 0.2) & (daily["daily_range"] > atr20 * 1.2)
    big_bear = bearish & (body_ratio >= 0.6) & ((daily["close"] - daily["low"]) <= range_nonzero * 0.2) & (daily["daily_range"] > atr20 * 1.2)
    mid_bull = bullish & body_ratio.between(0.35, 0.6, inclusive="left")
    mid_bear = bearish & body_ratio.between(0.35, 0.6, inclusive="left")

    weak_before_bull = (
        (previous["close"] <= previous["open"])
        | (previous_2["close"] <= previous_2["open"])
        | (previous_3["close"] <= previous_3["open"])
    )
    weak_before_bear = (
        (previous["close"] >= previous["open"])
        | (previous_2["close"] >= previous_2["open"])
        | (previous_3["close"] >= previous_3["open"])
    )

    prev_mid = (previous["open"] + previous["close"]) / 2
    bullish_break = daily["close"] > prev_mid
    bearish_break = daily["close"] < prev_mid
    bullish_engulf = (daily["open"] <= previous["close"]) & (daily["close"] >= previous["open"])
    bearish_engulf = (daily["open"] >= previous["close"]) & (daily["close"] <= previous["open"])
    bullish_reversal = daily["lower_shadow_ratio"] >= 0.4
    bearish_reversal = daily["upper_shadow_ratio"] >= 0.4

    turn_bull = bullish & weak_before_bull & (bullish_break | bullish_engulf | bullish_reversal) & ~big_bull & ~mid_bull
    turn_bear = bearish & weak_before_bear & (bearish_break | bearish_engulf | bearish_reversal) & ~big_bear & ~mid_bear

    day_type = np.select(
        [big_bull, mid_bull, turn_bull, big_bear, mid_bear, turn_bear],
        ["big_bull", "mid_bull", "turn_bull", "big_bear", "mid_bear", "turn_bear"],
        default="other",
    )
    return pd.Series(day_type, index=daily.index)


def _infer_symbol(frame: pd.DataFrame) -> str:
    if "symbol" in frame.columns and frame["symbol"].notna().any():
        return str(frame["symbol"].dropna().iloc[0])
    return "UNKNOWN"
