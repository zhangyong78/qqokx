from __future__ import annotations

import numpy as np
import pandas as pd

LOOKBACK = 5
FLAT_THRESHOLD = 0.0003
STRONG_THRESHOLD = 0.0005
ACCEL_BARS = 3
HORIZONS = (4, 8, 12, 24, 48, 72)
RANGE_ATR_MULTIPLIER = 1.5
STRONG_MOVE_ATR = 0.5

REGIME_LABELS = {
    "warming_up": "预热",
    "flat": "走平震荡",
    "bull_start": "多头启动",
    "bull_run": "多头推进",
    "bull_fade": "多头衰竭",
    "bear_start": "空头启动",
    "bear_run": "空头推进",
    "bear_fade": "空头衰竭",
    "weak_bear": "弱空头",
}

REVERSAL_REGIMES = frozenset({"bull_start", "bear_start", "bull_fade", "bear_fade"})

SIGNAL_META = {
    "bull_start": {
        "label": "转多",
        "bias": "long",
        "marker_shape": "arrowUp",
        "marker_position": "belowBar",
        "color_ma": "#16a34a",
        "color_ema": "#86efac",
    },
    "bear_start": {
        "label": "转空",
        "bias": "short",
        "marker_shape": "arrowDown",
        "marker_position": "aboveBar",
        "color_ma": "#dc2626",
        "color_ema": "#fca5a5",
    },
    "bull_fade": {
        "label": "多头衰竭",
        "bias": "short",
        "marker_shape": "circle",
        "marker_position": "aboveBar",
        "color_ma": "#ca8a04",
        "color_ema": "#fde047",
    },
    "bear_fade": {
        "label": "空头衰竭",
        "bias": "long",
        "marker_shape": "circle",
        "marker_position": "belowBar",
        "color_ma": "#ea580c",
        "color_ema": "#fdba74",
    },
}


def build_frame(candles: list[object]) -> pd.DataFrame:
    rows = [
        {
            "timestamp": pd.to_datetime(int(c.ts), unit="ms", utc=True),
            "open": float(c.open),
            "high": float(c.high),
            "low": float(c.low),
            "close": float(c.close),
            "volume": float(c.volume),
        }
        for c in candles
    ]
    return pd.DataFrame(rows).sort_values("timestamp").drop_duplicates("timestamp").reset_index(drop=True)


def add_indicators(df: pd.DataFrame) -> None:
    df["sma55"] = df["close"].rolling(55, min_periods=55).mean()
    df["ema55"] = df["close"].ewm(span=55, adjust=False, min_periods=55).mean()

    prev_close = df["close"].shift(1)
    tr = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - prev_close).abs(),
            (df["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    df["atr14"] = tr.rolling(14, min_periods=14).mean()


def linear_regression_slope(series: pd.Series) -> float:
    values = series.astype(float).to_numpy()
    if len(values) < 2 or np.isnan(values).any():
        return np.nan
    x = np.arange(len(values), dtype=float)
    x_mean = x.mean()
    y_mean = values.mean()
    numerator = np.sum((x - x_mean) * (values - y_mean))
    denominator = np.sum((x - x_mean) ** 2)
    if denominator == 0:
        return np.nan
    return float(numerator / denominator)


def classify_regime(
    *,
    slope_ratio: float,
    slope_ratio_prev: float,
    decel_streak: int,
    accel_up_streak: int,
) -> str:
    if pd.isna(slope_ratio):
        return "warming_up"
    if pd.notna(slope_ratio_prev):
        if slope_ratio_prev <= 0 < slope_ratio:
            return "bull_start"
        if slope_ratio_prev >= 0 > slope_ratio:
            return "bear_start"
    if abs(slope_ratio) < FLAT_THRESHOLD:
        return "flat"
    if slope_ratio > 0:
        if decel_streak >= ACCEL_BARS:
            return "bull_fade"
        return "bull_run"
    if slope_ratio < -STRONG_THRESHOLD:
        if accel_up_streak >= ACCEL_BARS:
            return "bear_fade"
        return "bear_run"
    return "weak_bear"


def enrich_line(df: pd.DataFrame, line_col: str) -> pd.DataFrame:
    out = df.copy()
    out["line"] = out[line_col]
    out["slope_raw"] = out["line"].rolling(LOOKBACK, min_periods=LOOKBACK).apply(linear_regression_slope, raw=False)
    out["slope_ratio"] = out["slope_raw"] / out["line"]
    out["slope_ratio_prev"] = out["slope_ratio"].shift(1)
    out["slope_accel"] = out["slope_ratio"] - out["slope_ratio_prev"]
    out["slope_strength"] = (out["line"] - out["line"].shift(LOOKBACK)) / out["atr14"]
    out["delta1"] = out["line"] - out["line"].shift(1)
    out["above_line"] = out["close"] > out["line"]
    out["decel_streak"] = (
        (out["slope_accel"] < 0)
        .groupby((out["slope_accel"] >= 0).cumsum())
        .cumcount()
        + 1
    )
    out.loc[out["slope_accel"] >= 0, "decel_streak"] = 0
    out["accel_up_streak"] = (
        (out["slope_accel"] > 0)
        .groupby((out["slope_accel"] <= 0).cumsum())
        .cumcount()
        + 1
    )
    out.loc[out["slope_accel"] <= 0, "accel_up_streak"] = 0
    out["regime"] = [
        classify_regime(
            slope_ratio=row.slope_ratio,
            slope_ratio_prev=row.slope_ratio_prev,
            decel_streak=int(row.decel_streak) if pd.notna(row.decel_streak) else 0,
            accel_up_streak=int(row.accel_up_streak) if pd.notna(row.accel_up_streak) else 0,
        )
        for row in out.itertuples(index=False)
    ]
    out["regime_label"] = out["regime"].map(REGIME_LABELS)
    attach_future_metrics(out)
    return out


def attach_future_metrics(df: pd.DataFrame) -> None:
    highs = df["high"].to_numpy(dtype=float)
    lows = df["low"].to_numpy(dtype=float)
    closes = df["close"].to_numpy(dtype=float)
    atrs = df["atr14"].to_numpy(dtype=float)
    count = len(df)

    for hours in HORIZONS:
        future_close = np.full(count, np.nan)
        future_max_high = np.full(count, np.nan)
        future_min_low = np.full(count, np.nan)
        for index in range(count):
            end = index + hours
            if end >= count:
                continue
            future_close[index] = closes[end]
            future_max_high[index] = float(np.max(highs[index + 1 : end + 1]))
            future_min_low[index] = float(np.min(lows[index + 1 : end + 1]))

        df[f"future_{hours}h_long_return"] = future_close / closes - 1
        df[f"future_{hours}h_short_return"] = closes / future_close - 1
        df[f"future_{hours}h_max_high"] = future_max_high
        df[f"future_{hours}h_min_low"] = future_min_low

        bullish_dir = future_close > closes
        bearish_dir = future_close < closes
        df[f"future_{hours}h_bullish_dir"] = bullish_dir
        df[f"future_{hours}h_bearish_dir"] = bearish_dir

        adverse_down = (closes - future_min_low) / atrs
        adverse_up = (future_max_high - closes) / atrs
        favorable_up = (future_max_high - closes) / atrs
        favorable_down = (closes - future_min_low) / atrs

        df[f"future_{hours}h_adverse_down_atr"] = adverse_down
        df[f"future_{hours}h_adverse_up_atr"] = adverse_up
        df[f"future_{hours}h_favorable_up_atr"] = favorable_up
        df[f"future_{hours}h_favorable_down_atr"] = favorable_down
        df[f"future_{hours}h_range_ok_for_long"] = adverse_down <= RANGE_ATR_MULTIPLIER
        df[f"future_{hours}h_range_ok_for_short"] = adverse_up <= RANGE_ATR_MULTIPLIER
        df[f"future_{hours}h_strong_up"] = favorable_up >= STRONG_MOVE_ATR
        df[f"future_{hours}h_strong_down"] = favorable_down >= STRONG_MOVE_ATR


def extract_reversal_events(enriched: pd.DataFrame, *, line_label: str) -> pd.DataFrame:
    events = enriched[enriched["regime"].isin(REVERSAL_REGIMES)].copy()
    if events.empty:
        return events

    rows: list[dict[str, object]] = []
    for row in events.itertuples(index=False):
        regime = str(row.regime)
        meta = SIGNAL_META[regime]
        bias = meta["bias"]
        item: dict[str, object] = {
            "timestamp": row.timestamp,
            "unix": int(pd.Timestamp(row.timestamp).timestamp()),
            "line": line_label,
            "regime": regime,
            "signal_label": meta["label"],
            "bias": bias,
            "close": float(row.close),
            "line_value": float(row.line),
            "atr14": float(row.atr14) if pd.notna(row.atr14) else np.nan,
            "slope_ratio": float(row.slope_ratio) if pd.notna(row.slope_ratio) else np.nan,
            "above_line": bool(row.above_line),
        }
        for hours in HORIZONS:
            if bias == "long":
                item[f"dir_ok_{hours}h"] = bool(getattr(row, f"future_{hours}h_bullish_dir"))
                item[f"range_ok_{hours}h"] = bool(getattr(row, f"future_{hours}h_range_ok_for_long"))
                item[f"strong_ok_{hours}h"] = bool(getattr(row, f"future_{hours}h_strong_up"))
                item[f"return_{hours}h"] = float(getattr(row, f"future_{hours}h_long_return"))
            else:
                item[f"dir_ok_{hours}h"] = bool(getattr(row, f"future_{hours}h_bearish_dir"))
                item[f"range_ok_{hours}h"] = bool(getattr(row, f"future_{hours}h_range_ok_for_short"))
                item[f"strong_ok_{hours}h"] = bool(getattr(row, f"future_{hours}h_strong_down"))
                item[f"return_{hours}h"] = float(getattr(row, f"future_{hours}h_short_return"))
        rows.append(item)
    return pd.DataFrame(rows)


def summarize_reversal_success(events: pd.DataFrame) -> pd.DataFrame:
    if events.empty:
        return pd.DataFrame()

    rows: list[dict[str, object]] = []
    group_cols = ["line", "regime", "signal_label", "bias"]
    for keys, bucket in events.groupby(group_cols, sort=False):
        line, regime, signal_label, bias = keys
        row: dict[str, object] = {
            "line": line,
            "regime": regime,
            "signal_label": signal_label,
            "bias": bias,
            "count": int(len(bucket)),
        }
        for hours in HORIZONS:
            valid = bucket[f"return_{hours}h"].notna()
            subset = bucket.loc[valid]
            if subset.empty:
                row[f"dir_ok_{hours}h"] = np.nan
                row[f"range_ok_{hours}h"] = np.nan
                row[f"strong_ok_{hours}h"] = np.nan
                row[f"mean_return_{hours}h"] = np.nan
                continue
            row[f"dir_ok_{hours}h"] = float(subset[f"dir_ok_{hours}h"].mean())
            row[f"range_ok_{hours}h"] = float(subset[f"range_ok_{hours}h"].mean())
            row[f"strong_ok_{hours}h"] = float(subset[f"strong_ok_{hours}h"].mean())
            row[f"mean_return_{hours}h"] = float(subset[f"return_{hours}h"].mean())
        rows.append(row)
    return pd.DataFrame(rows)


def build_dual_confirm_events(ma_events: pd.DataFrame, ema_events: pd.DataFrame) -> pd.DataFrame:
    if ma_events.empty or ema_events.empty:
        return pd.DataFrame()

    merged = ma_events.merge(
        ema_events,
        on=["timestamp", "regime"],
        suffixes=("_ma", "_ema"),
        how="inner",
    )
    if merged.empty:
        return merged

    rows: list[dict[str, object]] = []
    for row in merged.itertuples(index=False):
        regime = str(row.regime)
        meta = SIGNAL_META[regime]
        bias = meta["bias"]
        item: dict[str, object] = {
            "timestamp": row.timestamp,
            "unix": int(pd.Timestamp(row.timestamp).timestamp()),
            "line": "MA+EMA",
            "regime": regime,
            "signal_label": f"双确认{meta['label']}",
            "bias": bias,
            "close": float(row.close_ma),
            "ma_slope_ratio": float(row.slope_ratio_ma),
            "ema_slope_ratio": float(row.slope_ratio_ema),
        }
        for hours in HORIZONS:
            if bias == "long":
                item[f"dir_ok_{hours}h"] = bool(getattr(row, f"dir_ok_{hours}h_ma"))
                item[f"range_ok_{hours}h"] = bool(getattr(row, f"range_ok_{hours}h_ma"))
                item[f"strong_ok_{hours}h"] = bool(getattr(row, f"strong_ok_{hours}h_ma"))
                item[f"return_{hours}h"] = float(getattr(row, f"return_{hours}h_ma"))
            else:
                item[f"dir_ok_{hours}h"] = bool(getattr(row, f"dir_ok_{hours}h_ma"))
                item[f"range_ok_{hours}h"] = bool(getattr(row, f"range_ok_{hours}h_ma"))
                item[f"strong_ok_{hours}h"] = bool(getattr(row, f"strong_ok_{hours}h_ma"))
                item[f"return_{hours}h"] = float(getattr(row, f"return_{hours}h_ma"))
        rows.append(item)
    return pd.DataFrame(rows)
