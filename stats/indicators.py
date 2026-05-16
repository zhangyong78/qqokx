from __future__ import annotations

import numpy as np
import pandas as pd


def compute_ema(values: pd.Series, *, period: int) -> pd.Series:
    return values.ewm(span=period, adjust=False, min_periods=period).mean()


def compute_atr(*, high: pd.Series, low: pd.Series, close: pd.Series, period: int) -> pd.Series:
    prev_close = close.shift(1)
    true_range = pd.concat(
        [
            (high - low),
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return true_range.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()


def compute_compression_score(session_hourly: pd.DataFrame) -> pd.Series:
    frame = session_hourly.copy()
    frame["tr"] = pd.concat(
        [
            frame["high"] - frame["low"],
            (frame["high"] - frame["close"].shift(1)).abs(),
            (frame["low"] - frame["close"].shift(1)).abs(),
        ],
        axis=1,
    ).max(axis=1)
    frame["atr48"] = frame["tr"].ewm(alpha=1 / 48, adjust=False, min_periods=48).mean()
    frame["tr12_mean"] = frame["tr"].rolling(12, min_periods=12).mean()
    frame["compression_raw"] = 1 - (frame["tr12_mean"] / frame["atr48"].replace(0, np.nan))
    day_open_rows = frame.groupby("session_date", as_index=False).head(1).copy()
    day_open_rows["compression_score"] = day_open_rows["compression_raw"].shift(1).clip(lower=-5, upper=5)
    return day_open_rows.set_index("session_date")["compression_score"]
