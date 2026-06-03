from __future__ import annotations

import numpy as np
import pandas as pd


def ema(series: pd.Series, period: int) -> pd.Series:
    return series.astype(float).ewm(span=period, adjust=False, min_periods=period).mean()


def sma(series: pd.Series, period: int) -> pd.Series:
    return series.astype(float).rolling(period, min_periods=period).mean()


def true_range(frame: pd.DataFrame) -> pd.Series:
    prev_close = frame["close"].shift(1)
    return pd.concat(
        [
            frame["high"] - frame["low"],
            (frame["high"] - prev_close).abs(),
            (frame["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)


def atr_wilder(frame: pd.DataFrame, period: int) -> pd.Series:
    tr = true_range(frame)
    return tr.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()


def rsi(series: pd.Series, period: int) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0.0, np.nan)
    rs = rs.where(~avg_loss.eq(0.0), np.inf)
    return 100.0 - (100.0 / (1.0 + rs))


def add_features(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out["ema21"] = ema(out["close"], 21)
    out["ema55"] = ema(out["close"], 55)
    out["atr14"] = atr_wilder(out, 14)
    out["rsi14"] = rsi(out["close"], 14)
    out["vol_ma20"] = sma(out["volume"], 20)
    out["highest_high_10"] = out["high"].rolling(10, min_periods=10).max()
    out["low_prev"] = out["low"].shift(1)
    out["ema55_slope_5"] = out["ema55"] - out["ema55"].shift(5)
    return out
