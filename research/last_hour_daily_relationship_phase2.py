from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from research.last_hour_daily_relationship import (
    attach_daily_context,
    build_session_daily,
    candle_color_scalar,
    load_hourly_frame,
)
from stats.indicators import compute_ema


MIN_SAMPLES_FOR_CONCLUSION = 30
DEFAULT_COST_R = 0.05
DEFAULT_MIN_RISK_PCT = 0.001


@dataclass(slots=True)
class Phase2ResearchResult:
    features: pd.DataFrame
    summary: pd.DataFrame
    output_dir: Path


def run_phase2_research(
    *,
    hourly_path: str | Path | None,
    output_dir: str | Path,
    inst_id: str = "BTC-USDT-SWAP",
    bar: str = "1H",
    symbol: str | None = None,
    timezone_offset_hours: int = 8,
    session_close_hour: int = 8,
    cost_r: float = DEFAULT_COST_R,
    min_risk_pct: float = DEFAULT_MIN_RISK_PCT,
    min_samples: int = MIN_SAMPLES_FOR_CONCLUSION,
) -> Phase2ResearchResult:
    hourly_frame = load_hourly_frame(hourly_path=hourly_path, inst_id=inst_id, bar=bar)
    resolved_symbol = symbol or str(hourly_frame["symbol"].dropna().iloc[0])
    features = build_phase2_features(
        hourly_frame=hourly_frame,
        symbol=resolved_symbol,
        timezone_offset_hours=timezone_offset_hours,
        session_close_hour=session_close_hour,
        cost_r=cost_r,
    )
    summary_all = summarize_phase2_conditions(features, min_samples=1)
    summary_extended = summarize_phase2_conditions(features, min_samples=min_samples)
    risk_filtered = features[
        (features["long_risk_pct"] >= min_risk_pct)
        & (features["short_risk_pct"] >= min_risk_pct)
    ].copy()
    summary_risk_filtered = summarize_phase2_conditions(risk_filtered, min_samples=min_samples)
    exit_comparison = build_exit_method_comparison(features, min_samples=min_samples)
    time_stability = build_condition_time_stability(features, min_samples=min_samples)
    train_test = build_train_test_summary(features, min_samples=min_samples)
    threshold_sensitivity = build_threshold_sensitivity(features, cost_r=cost_r, min_samples=min_samples)
    top_long, top_short, no_trade = select_setups(summary_risk_filtered, train_test, min_samples=min_samples)

    output_root = Path(output_dir)
    output_root.mkdir(parents=True, exist_ok=True)
    write_outputs(
        output_root=output_root,
        features=features,
        summary_extended=summary_extended,
        summary_all=summary_all,
        summary_risk_filtered=summary_risk_filtered,
        exit_comparison=exit_comparison,
        time_stability=time_stability,
        train_test=train_test,
        threshold_sensitivity=threshold_sensitivity,
        top_long=top_long,
        top_short=top_short,
        no_trade=no_trade,
        symbol=resolved_symbol,
        inst_id=inst_id,
        bar=bar,
        hourly_path=hourly_path,
        timezone_offset_hours=timezone_offset_hours,
        session_close_hour=session_close_hour,
        cost_r=cost_r,
        min_risk_pct=min_risk_pct,
        min_samples=min_samples,
    )
    return Phase2ResearchResult(features=features, summary=summary_extended, output_dir=output_root)


def build_phase2_features(
    *,
    hourly_frame: pd.DataFrame,
    symbol: str,
    timezone_offset_hours: int = 8,
    session_close_hour: int = 8,
    cost_r: float = DEFAULT_COST_R,
) -> pd.DataFrame:
    hourly = prepare_hourly(hourly_frame, timezone_offset_hours=timezone_offset_hours, session_close_hour=session_close_hour)
    daily = attach_daily_context(build_session_daily(hourly))
    daily = attach_recent_daily_structure(daily)
    grouped_sessions = {session_date: group.reset_index(drop=True) for session_date, group in hourly.groupby("session_date", sort=True)}
    daily_indexed = daily.set_index("session_date")

    rows: list[dict[str, object]] = []
    for session_date, current_day in daily_indexed.iterrows():
        next_session_date = session_date + pd.Timedelta(days=1)
        if current_day["bar_count"] != 24 or next_session_date not in daily_indexed.index:
            continue
        next_day = daily_indexed.loc[next_session_date]
        if next_day["bar_count"] != 24:
            continue
        session_hourly = grouped_sessions.get(session_date)
        next_session_hourly = grouped_sessions.get(next_session_date)
        if session_hourly is None or next_session_hourly is None or len(session_hourly) != 24 or len(next_session_hourly) != 24:
            continue
        signal_row = session_hourly.loc[session_hourly["local_hour"] == (session_close_hour - 1) % 24]
        if len(signal_row) != 1:
            continue
        signal = signal_row.iloc[0]
        pre_signal = session_hourly.loc[session_hourly["timestamp"] < signal["timestamp"]].copy()
        if len(pre_signal) != 23:
            continue
        rows.append(
            build_phase2_row(
                symbol=symbol,
                session_date=session_date,
                next_session_date=next_session_date,
                current_day=current_day,
                next_day=next_day,
                signal=signal,
                pre_signal=pre_signal,
                next_session_hourly=next_session_hourly,
                cost_r=cost_r,
            )
        )
    return pd.DataFrame(rows).sort_values("session_date").reset_index(drop=True)


def prepare_hourly(hourly_frame: pd.DataFrame, *, timezone_offset_hours: int, session_close_hour: int) -> pd.DataFrame:
    hourly = hourly_frame.copy().sort_values("timestamp").reset_index(drop=True)
    hourly["timestamp"] = pd.to_datetime(hourly["timestamp"], utc=True)
    hourly["local_timestamp"] = hourly["timestamp"] + pd.Timedelta(hours=timezone_offset_hours)
    hourly["session_date"] = (hourly["local_timestamp"] - pd.Timedelta(hours=session_close_hour)).dt.floor("D")
    hourly["local_hour"] = hourly["local_timestamp"].dt.hour.astype(int)
    hourly["volume_avg_24_before"] = hourly["volume"].shift(1).rolling(24, min_periods=24).mean()
    hourly["close_ema_24"] = compute_ema(hourly["close"], period=24)
    return hourly


def attach_recent_daily_structure(daily: pd.DataFrame) -> pd.DataFrame:
    out = daily.copy().sort_values("session_date").reset_index(drop=True)
    colors = out["color"]
    out["prev_2_days_bullish"] = (colors == "bull") & (colors.shift(1) == "bull")
    out["prev_2_days_bearish"] = (colors == "bear") & (colors.shift(1) == "bear")
    out["prev_3_days_bullish"] = out["prev_2_days_bullish"] & (colors.shift(2) == "bull")
    out["prev_3_days_bearish"] = out["prev_2_days_bearish"] & (colors.shift(2) == "bear")
    out["big_bull_day"] = (out["color"] == "bull") & (out["body_ratio"] >= 0.6) & (out["range"] >= out["atr20"])
    out["big_bear_day"] = (out["color"] == "bear") & (out["body_ratio"] >= 0.6) & (out["range"] >= out["atr20"])
    out["doji_day"] = out["body_ratio"] <= 0.15
    out["long_wick_day"] = (out["upper_wick_ratio"] >= 0.4) | (out["lower_wick_ratio"] >= 0.4)
    out["recent_3_big_bull_count"] = out["big_bull_day"].rolling(3, min_periods=1).sum()
    out["recent_3_big_bear_count"] = out["big_bear_day"].rolling(3, min_periods=1).sum()
    out["recent_3_doji_count"] = out["doji_day"].rolling(3, min_periods=1).sum()
    out["recent_3_long_wick_count"] = out["long_wick_day"].rolling(3, min_periods=1).sum()
    out["recent_3day_bias"] = np.select(
        [out["prev_3_days_bullish"], out["prev_3_days_bearish"], out["prev_2_days_bullish"], out["prev_2_days_bearish"]],
        ["three_bullish", "three_bearish", "two_bullish", "two_bearish"],
        default="mixed",
    )
    return out


def build_phase2_row(
    *,
    symbol: str,
    session_date: pd.Timestamp,
    next_session_date: pd.Timestamp,
    current_day: pd.Series,
    next_day: pd.Series,
    signal: pd.Series,
    pre_signal: pd.DataFrame,
    next_session_hourly: pd.DataFrame,
    cost_r: float,
) -> dict[str, object]:
    pre_open = float(pre_signal["open"].iloc[0])
    pre_high = float(pre_signal["high"].max())
    pre_low = float(pre_signal["low"].min())
    pre_close = float(pre_signal["close"].iloc[-1])
    temp_color = candle_color_scalar(pre_open, pre_close)
    final_color = str(current_day["color"])
    signal_open = float(signal["open"])
    signal_high = float(signal["high"])
    signal_low = float(signal["low"])
    signal_close = float(signal["close"])
    signal_color = candle_color_scalar(signal_open, signal_close)
    signal_range = max(signal_high - signal_low, 0.0)
    safe_range = signal_range if signal_range > 0 else np.nan
    body_ratio = abs(signal_close - signal_open) / safe_range if np.isfinite(safe_range) else np.nan
    upper_wick_ratio = (signal_high - max(signal_open, signal_close)) / safe_range if np.isfinite(safe_range) else np.nan
    lower_wick_ratio = (min(signal_open, signal_close) - signal_low) / safe_range if np.isfinite(safe_range) else np.nan
    daily_close_location = location_ratio(signal_close, float(current_day["low"]), float(current_day["high"]))
    volume_ratio = signal["volume"] / signal["volume_avg_24_before"] if pd.notna(signal["volume_avg_24_before"]) and signal["volume_avg_24_before"] > 0 else np.nan
    break_high = bool(signal_high > pre_high)
    break_low = bool(signal_low < pre_low)
    close_break_high = bool(signal_close > pre_high)
    close_break_low = bool(signal_close < pre_low)
    failed_up = bool(break_high and not close_break_high)
    failed_down = bool(break_low and not close_break_low)

    row: dict[str, object] = {
        "session_date": session_date.strftime("%Y-%m-%d"),
        "session_date_ts": session_date,
        "year": int(session_date.year),
        "quarter": f"{session_date.year}Q{session_date.quarter}",
        "symbol": symbol,
        "entry_time_utc": signal["timestamp"] + pd.Timedelta(hours=1),
        "entry_time_local": signal["local_timestamp"] + pd.Timedelta(hours=1),
        "next_session_date": next_session_date.strftime("%Y-%m-%d"),
        "temp_day_open_7h": pre_open,
        "temp_day_high_7h": pre_high,
        "temp_day_low_7h": pre_low,
        "temp_day_close_7h": pre_close,
        "temp_day_color_7h": temp_color,
        "prev_day_color": final_color,
        "changed_bear_to_bull": bool(temp_color == "bear" and final_color == "bull"),
        "changed_bull_to_bear": bool(temp_color == "bull" and final_color == "bear"),
        "last_hour_change_type": classify_change_type(temp_color, final_color),
        "signal_color": signal_color,
        "last_hour_wick_type": classify_last_hour_wick(signal_color, body_ratio, upper_wick_ratio, lower_wick_ratio),
        "signal_body_ratio": body_ratio,
        "signal_upper_wick_ratio": upper_wick_ratio,
        "signal_lower_wick_ratio": lower_wick_ratio,
        "signal_close_location": location_ratio(signal_close, signal_low, signal_high),
        "daily_close_location": daily_close_location,
        "close_area": classify_close_area(daily_close_location),
        "signal_volume_ratio_24": volume_ratio,
        "volume_bucket": classify_phase2_volume_bucket(volume_ratio),
        "break_prev_23h_high": break_high,
        "break_prev_23h_low": break_low,
        "close_confirm_up_breakout": close_break_high,
        "close_confirm_down_breakout": close_break_low,
        "failed_up_breakout": failed_up,
        "failed_down_breakout": failed_down,
        "breakout_type": classify_breakout_type(break_high, break_low, close_break_high, close_break_low, failed_up, failed_down),
        "trend_regime": str(current_day["trend_bucket"]),
        "prev_day_trend_bucket": str(current_day["trend_bucket"]),
        "prev_day_breakout_bucket": str(current_day["day_breakout_bucket"]),
        "prev_2_days_bullish": bool(current_day["prev_2_days_bullish"]),
        "prev_2_days_bearish": bool(current_day["prev_2_days_bearish"]),
        "prev_3_days_bullish": bool(current_day["prev_3_days_bullish"]),
        "prev_3_days_bearish": bool(current_day["prev_3_days_bearish"]),
        "big_bull_day": bool(current_day["big_bull_day"]),
        "big_bear_day": bool(current_day["big_bear_day"]),
        "doji_day": bool(current_day["doji_day"]),
        "long_wick_day": bool(current_day["long_wick_day"]),
        "recent_3_big_bull_count": int(current_day["recent_3_big_bull_count"]),
        "recent_3_big_bear_count": int(current_day["recent_3_big_bear_count"]),
        "recent_3_doji_count": int(current_day["recent_3_doji_count"]),
        "recent_3_long_wick_count": int(current_day["recent_3_long_wick_count"]),
        "recent_3day_bias": str(current_day["recent_3day_bias"]),
        "prev_day_open": float(current_day["open"]),
        "prev_day_high": float(current_day["high"]),
        "prev_day_low": float(current_day["low"]),
        "prev_day_close": float(current_day["close"]),
        "prev_day_return_pct": float(current_day["return_pct"]),
        "next_day_open": float(next_day["open"]),
        "next_day_high": float(next_day["high"]),
        "next_day_low": float(next_day["low"]),
        "next_day_close": float(next_day["close"]),
        "next_day_color": str(next_day["color"]),
        "next_day_return_pct": float(next_day["return_pct"]),
        "phase2_core_setup": "__".join(
            [
                str(current_day["trend_bucket"]),
                classify_change_type(temp_color, final_color),
                classify_last_hour_wick(signal_color, body_ratio, upper_wick_ratio, lower_wick_ratio),
                classify_close_area(daily_close_location),
                classify_breakout_type(break_high, break_low, close_break_high, close_break_low, failed_up, failed_down),
            ]
        ),
        "phase2_simple_setup": "__".join(
            [
                signal_color,
                classify_last_hour_wick(signal_color, body_ratio, upper_wick_ratio, lower_wick_ratio),
                classify_close_area(daily_close_location),
                classify_phase2_volume_bucket(volume_ratio),
            ]
        ),
    }
    row.update(evaluate_path_and_exits(next_session_hourly, entry_price=signal_close, stop_price=signal_low, side="long", cost_r=cost_r))
    row.update(evaluate_path_and_exits(next_session_hourly, entry_price=signal_close, stop_price=signal_high, side="short", cost_r=cost_r))
    return row


def evaluate_path_and_exits(
    next_session_hourly: pd.DataFrame,
    *,
    entry_price: float,
    stop_price: float,
    side: str,
    cost_r: float,
) -> dict[str, object]:
    prefix = side.lower()
    risk = entry_price - stop_price if prefix == "long" else stop_price - entry_price
    default = {
        f"{prefix}_entry_price": entry_price,
        f"{prefix}_stop_price": stop_price,
        f"{prefix}_risk": risk,
        f"{prefix}_risk_pct": np.nan,
    }
    if not np.isfinite(risk) or risk <= 0 or entry_price <= 0:
        for name in exit_method_names():
            default[f"{prefix}_{name}_r"] = np.nan
            default[f"{prefix}_{name}_net_r"] = np.nan
        default.update(first_touch_defaults(prefix))
        return default

    target_1r = entry_price + risk if prefix == "long" else entry_price - risk
    target_2r = entry_price + 2 * risk if prefix == "long" else entry_price - 2 * risk
    path = scan_path(next_session_hourly, side=prefix, entry_price=entry_price, stop_price=stop_price, target_1r=target_1r, target_2r=target_2r)
    close_price = float(next_session_hourly["close"].iloc[-1])
    final_close_r = (close_price - entry_price) / risk if prefix == "long" else (entry_price - close_price) / risk
    fixed_1r = fixed_target_r(path["first_stop_index"], path["first_1r_index"], final_close_r, target_r=1.0)
    fixed_2r = fixed_target_r(path["first_stop_index"], path["first_2r_index"], final_close_r, target_r=2.0)
    be_after_1r = breakeven_after_1r(next_session_hourly, path, side=prefix, entry_price=entry_price, stop_price=stop_price, final_close_r=final_close_r)
    half_1r_hold = half_take_profit_hold(next_session_hourly, path, side=prefix, stop_price=stop_price, final_close_r=final_close_r)
    stop_close_r = -1.0 if path["stop_hit"] else final_close_r
    mfe_r = (float(next_session_hourly["high"].max()) - entry_price) / risk if prefix == "long" else (entry_price - float(next_session_hourly["low"].min())) / risk
    mae_r = (entry_price - float(next_session_hourly["low"].min())) / risk if prefix == "long" else (float(next_session_hourly["high"].max()) - entry_price) / risk

    out = {
        f"{prefix}_entry_price": entry_price,
        f"{prefix}_stop_price": stop_price,
        f"{prefix}_risk": risk,
        f"{prefix}_risk_pct": risk / entry_price,
        f"{prefix}_stop_hit": path["stop_hit"],
        f"{prefix}_hit_1r": path["hit_1r"],
        f"{prefix}_hit_2r": path["hit_2r"],
        f"{prefix}_first_touch": path["first_touch"],
        f"{prefix}_hit_stop_first": path["first_touch"] in {"stop", "ambiguous_stop_first"},
        f"{prefix}_hit_1R_first": path["first_touch"] == "1r",
        f"{prefix}_hit_2R_first": path["first_touch"] == "2r",
        f"{prefix}_ambiguous_bars": path["ambiguous_bars"],
        f"{prefix}_mfe_r": mfe_r,
        f"{prefix}_mae_r": mae_r,
        f"{prefix}_next_close_r": final_close_r,
        f"{prefix}_next_close_net_r": final_close_r - cost_r,
        f"{prefix}_stop_or_close_r": stop_close_r,
        f"{prefix}_stop_or_close_net_r": stop_close_r - cost_r,
        f"{prefix}_fixed_1r_r": fixed_1r,
        f"{prefix}_fixed_1r_net_r": fixed_1r - cost_r,
        f"{prefix}_fixed_2r_r": fixed_2r,
        f"{prefix}_fixed_2r_net_r": fixed_2r - cost_r,
        f"{prefix}_breakeven_after_1r_r": be_after_1r,
        f"{prefix}_breakeven_after_1r_net_r": be_after_1r - cost_r,
        f"{prefix}_half_1r_hold_r": half_1r_hold,
        f"{prefix}_half_1r_hold_net_r": half_1r_hold - cost_r,
    }
    return out


def scan_path(
    frame: pd.DataFrame,
    *,
    side: str,
    entry_price: float,
    stop_price: float,
    target_1r: float,
    target_2r: float,
) -> dict[str, object]:
    first_stop_index: int | None = None
    first_1r_index: int | None = None
    first_2r_index: int | None = None
    first_touch = "none"
    ambiguous_bars = 0
    for i, (_, candle) in enumerate(frame.iterrows()):
        high = float(candle["high"])
        low = float(candle["low"])
        if side == "long":
            stop_touched = low <= stop_price
            hit_1r = high >= target_1r
            hit_2r = high >= target_2r
        else:
            stop_touched = high >= stop_price
            hit_1r = low <= target_1r
            hit_2r = low <= target_2r

        if stop_touched and (hit_1r or hit_2r):
            ambiguous_bars += 1
            if first_stop_index is None:
                first_stop_index = i
            if hit_1r and first_1r_index is None:
                first_1r_index = i
            if hit_2r and first_2r_index is None:
                first_2r_index = i
            if first_touch == "none":
                first_touch = "ambiguous_stop_first"
            continue
        if stop_touched and first_stop_index is None:
            first_stop_index = i
            if first_touch == "none":
                first_touch = "stop"
        if hit_2r and first_2r_index is None:
            first_2r_index = i
            if first_1r_index is None:
                first_1r_index = i
            if first_touch == "none":
                first_touch = "2r"
        elif hit_1r and first_1r_index is None:
            first_1r_index = i
            if first_touch == "none":
                first_touch = "1r"

    return {
        "stop_hit": first_stop_index is not None,
        "hit_1r": first_1r_index is not None,
        "hit_2r": first_2r_index is not None,
        "first_stop_index": first_stop_index,
        "first_1r_index": first_1r_index,
        "first_2r_index": first_2r_index,
        "first_touch": first_touch,
        "ambiguous_bars": ambiguous_bars,
    }


def fixed_target_r(first_stop_index: int | None, first_target_index: int | None, final_close_r: float, *, target_r: float) -> float:
    if first_stop_index is None and first_target_index is None:
        return final_close_r
    if first_stop_index is None:
        return target_r
    if first_target_index is None:
        return -1.0
    return -1.0 if first_stop_index <= first_target_index else target_r


def breakeven_after_1r(
    frame: pd.DataFrame,
    path: dict[str, object],
    *,
    side: str,
    entry_price: float,
    stop_price: float,
    final_close_r: float,
) -> float:
    first_stop = path["first_stop_index"]
    first_1r = path["first_1r_index"]
    if first_1r is None:
        return -1.0 if first_stop is not None else final_close_r
    if first_stop is not None and first_stop <= first_1r:
        return -1.0
    after = frame.iloc[int(first_1r) + 1 :]
    if after.empty:
        return final_close_r
    if side == "long" and (after["low"] <= entry_price).any():
        return 0.0
    if side == "short" and (after["high"] >= entry_price).any():
        return 0.0
    return final_close_r


def half_take_profit_hold(
    frame: pd.DataFrame,
    path: dict[str, object],
    *,
    side: str,
    stop_price: float,
    final_close_r: float,
) -> float:
    first_stop = path["first_stop_index"]
    first_1r = path["first_1r_index"]
    if first_1r is None:
        return -1.0 if first_stop is not None else final_close_r
    if first_stop is not None and first_stop <= first_1r:
        return -1.0
    after = frame.iloc[int(first_1r) + 1 :]
    second_half_r = final_close_r
    if side == "long" and not after.empty and (after["low"] <= stop_price).any():
        second_half_r = -1.0
    if side == "short" and not after.empty and (after["high"] >= stop_price).any():
        second_half_r = -1.0
    return 0.5 + 0.5 * second_half_r


def first_touch_defaults(prefix: str) -> dict[str, object]:
    return {
        f"{prefix}_stop_hit": np.nan,
        f"{prefix}_hit_1r": np.nan,
        f"{prefix}_hit_2r": np.nan,
        f"{prefix}_first_touch": "invalid_risk",
        f"{prefix}_hit_stop_first": np.nan,
        f"{prefix}_hit_1R_first": np.nan,
        f"{prefix}_hit_2R_first": np.nan,
        f"{prefix}_ambiguous_bars": 0,
        f"{prefix}_mfe_r": np.nan,
        f"{prefix}_mae_r": np.nan,
    }


def summarize_phase2_conditions(features: pd.DataFrame, *, min_samples: int) -> pd.DataFrame:
    working = features.copy()
    condition_columns = phase2_condition_columns()
    rows: list[dict[str, object]] = []
    for group_name, column_name in condition_columns:
        if column_name is None:
            grouped_items = [("all", working)]
        else:
            grouped_items = sorted(working.groupby(column_name, dropna=False), key=lambda item: str(item[0]))
        for condition_value, subset in grouped_items:
            if len(subset) < min_samples:
                continue
            for side in ("long", "short"):
                rows.append(build_phase2_summary_row(subset, group_name=group_name, condition_value=condition_value, side=side))
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).sort_values(["side", "net_expectancy_R", "sample_count"], ascending=[True, False, False]).reset_index(drop=True)


def build_phase2_summary_row(subset: pd.DataFrame, *, group_name: str, condition_value: object, side: str) -> dict[str, object]:
    prefix = side.lower()
    net = pd.to_numeric(subset[f"{prefix}_stop_or_close_net_r"], errors="coerce")
    gross = pd.to_numeric(subset[f"{prefix}_stop_or_close_r"], errors="coerce")
    close_r = pd.to_numeric(subset[f"{prefix}_next_close_r"], errors="coerce")
    return {
        "condition_group": group_name,
        "condition_value": str(condition_value),
        "side": prefix,
        "sample_count": int(len(subset)),
        "conclusion_level": "conclusion_ok" if len(subset) >= MIN_SAMPLES_FOR_CONCLUSION else "observe_only",
        "next_day_bull_rate": mean_bool(subset["next_day_color"] == "bull"),
        "next_day_bear_rate": mean_bool(subset["next_day_color"] == "bear"),
        "hit_stop_first_rate": mean_bool(subset[f"{prefix}_hit_stop_first"]),
        "hit_1R_first_rate": mean_bool(subset[f"{prefix}_hit_1R_first"]),
        "hit_2R_first_rate": mean_bool(subset[f"{prefix}_hit_2R_first"]),
        "hit_1R_rate": mean_bool(subset[f"{prefix}_hit_1r"]),
        "hit_2R_rate": mean_bool(subset[f"{prefix}_hit_2r"]),
        "stop_rate": mean_bool(subset[f"{prefix}_stop_hit"]),
        "avg_final_close_R": float(close_r.mean()) if close_r.notna().any() else np.nan,
        "median_final_close_R": float(close_r.median()) if close_r.notna().any() else np.nan,
        "gross_expectancy_R": float(gross.mean()) if gross.notna().any() else np.nan,
        "net_expectancy_R": float(net.mean()) if net.notna().any() else np.nan,
        "net_median_R": float(net.median()) if net.notna().any() else np.nan,
        "positive_net_rate": mean_bool(net > 0),
        "avg_mfe_R": float(pd.to_numeric(subset[f"{prefix}_mfe_r"], errors="coerce").mean()),
        "avg_mae_R": float(pd.to_numeric(subset[f"{prefix}_mae_r"], errors="coerce").mean()),
        "avg_risk_pct": float(pd.to_numeric(subset[f"{prefix}_risk_pct"], errors="coerce").mean()),
        "ambiguous_bar_rate": mean_bool(pd.to_numeric(subset[f"{prefix}_ambiguous_bars"], errors="coerce") > 0),
    }


def build_exit_method_comparison(features: pd.DataFrame, *, min_samples: int) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    groups = [("overall", "all", features)]
    for group_name, column in phase2_condition_columns():
        if column is None:
            continue
        for value, subset in features.groupby(column, dropna=False):
            if len(subset) >= min_samples:
                groups.append((group_name, str(value), subset))
    for group_name, value, subset in groups:
        for side in ("long", "short"):
            for method in exit_method_names():
                series = pd.to_numeric(subset[f"{side}_{method}_net_r"], errors="coerce")
                rows.append(
                    {
                        "condition_group": group_name,
                        "condition_value": value,
                        "side": side,
                        "exit_method": method,
                        "sample_count": int(len(subset)),
                        "net_expectancy_R": float(series.mean()) if series.notna().any() else np.nan,
                        "median_net_R": float(series.median()) if series.notna().any() else np.nan,
                        "positive_net_rate": mean_bool(series > 0),
                    }
                )
    return pd.DataFrame(rows).sort_values(["side", "net_expectancy_R", "sample_count"], ascending=[True, False, False]).reset_index(drop=True)


def build_condition_time_stability(features: pd.DataFrame, *, min_samples: int) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    selected_columns = [
        ("overall", None),
        ("last_hour_change_type", "last_hour_change_type"),
        ("last_hour_wick_type", "last_hour_wick_type"),
        ("close_area", "close_area"),
        ("volume_bucket", "volume_bucket"),
        ("breakout_type", "breakout_type"),
        ("trend_regime", "trend_regime"),
        ("recent_3day_bias", "recent_3day_bias"),
    ]
    for period_type, period_column in [("year", "year"), ("quarter", "quarter")]:
        for period, period_frame in features.groupby(period_column, dropna=False):
            for group_name, column in selected_columns:
                grouped_items = [("all", period_frame)] if column is None else period_frame.groupby(column, dropna=False)
                for value, subset in grouped_items:
                    if len(subset) < min_samples:
                        continue
                    for side in ("long", "short"):
                        row = build_phase2_summary_row(subset, group_name=group_name, condition_value=value, side=side)
                        row["period_type"] = period_type
                        row["period"] = str(period)
                        rows.append(row)
    return pd.DataFrame(rows).reset_index(drop=True)


def build_train_test_summary(features: pd.DataFrame, *, min_samples: int) -> pd.DataFrame:
    ordered = features.sort_values("session_date_ts").reset_index(drop=True)
    split_index = int(len(ordered) * 0.7)
    train = ordered.iloc[:split_index].copy()
    test = ordered.iloc[split_index:].copy()
    rows: list[dict[str, object]] = []
    for group_name, column in phase2_condition_columns():
        if column is None:
            grouped_values = ["all"]
        else:
            grouped_values = sorted(set(train[column].dropna().astype(str)) | set(test[column].dropna().astype(str)))
        for value in grouped_values:
            train_subset = train if column is None else train[train[column].astype(str) == value]
            test_subset = test if column is None else test[test[column].astype(str) == value]
            if len(train_subset) < min_samples or len(test_subset) < min_samples:
                continue
            for side in ("long", "short"):
                train_net = pd.to_numeric(train_subset[f"{side}_stop_or_close_net_r"], errors="coerce")
                test_net = pd.to_numeric(test_subset[f"{side}_stop_or_close_net_r"], errors="coerce")
                rows.append(
                    {
                        "condition_group": group_name,
                        "condition_value": value,
                        "side": side,
                        "train_sample_count": int(len(train_subset)),
                        "test_sample_count": int(len(test_subset)),
                        "train_net_expectancy_R": float(train_net.mean()),
                        "test_net_expectancy_R": float(test_net.mean()),
                        "train_stop_first_rate": mean_bool(train_subset[f"{side}_hit_stop_first"]),
                        "test_stop_first_rate": mean_bool(test_subset[f"{side}_hit_stop_first"]),
                        "expectancy_decay_R": float(test_net.mean() - train_net.mean()),
                        "stable_positive": bool(train_net.mean() > 0 and test_net.mean() > 0),
                    }
                )
    return pd.DataFrame(rows).sort_values(["side", "stable_positive", "test_net_expectancy_R"], ascending=[True, False, False]).reset_index(drop=True)


def build_threshold_sensitivity(features: pd.DataFrame, *, cost_r: float, min_samples: int) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    threshold_specs = [
        ("body_strong_min", "signal_body_ratio", [0.5, 0.6, 0.67, 0.75], ">="),
        ("wick_long_min", "signal_upper_wick_ratio", [0.3, 0.35, 0.4, 0.45], ">="),
        ("lower_wick_long_min", "signal_lower_wick_ratio", [0.3, 0.35, 0.4, 0.45], ">="),
        ("volume_high_min", "signal_volume_ratio_24", [1.2, 1.5, 2.0, 2.5], ">="),
        ("close_high_area_min", "daily_close_location", [0.6, 0.67, 0.75, 0.8], ">="),
        ("close_low_area_max", "daily_close_location", [0.2, 0.25, 0.33, 0.4], "<="),
    ]
    for name, column, thresholds, op in threshold_specs:
        for threshold in thresholds:
            subset = features[features[column] >= threshold] if op == ">=" else features[features[column] <= threshold]
            if len(subset) < min_samples:
                continue
            for side in ("long", "short"):
                net = pd.to_numeric(subset[f"{side}_stop_or_close_r"], errors="coerce") - cost_r
                rows.append(
                    {
                        "threshold_name": name,
                        "operator": op,
                        "threshold": threshold,
                        "side": side,
                        "sample_count": int(len(subset)),
                        "net_expectancy_R": float(net.mean()),
                        "hit_stop_first_rate": mean_bool(subset[f"{side}_hit_stop_first"]),
                        "hit_1R_rate": mean_bool(subset[f"{side}_hit_1r"]),
                        "hit_2R_rate": mean_bool(subset[f"{side}_hit_2r"]),
                    }
                )
    return pd.DataFrame(rows).sort_values(["threshold_name", "side", "threshold"]).reset_index(drop=True)


def select_setups(summary: pd.DataFrame, train_test: pd.DataFrame, *, min_samples: int) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if summary.empty:
        empty = pd.DataFrame()
        return empty, empty, empty
    merged = summary.merge(
        train_test[
            [
                "condition_group",
                "condition_value",
                "side",
                "train_sample_count",
                "test_sample_count",
                "train_net_expectancy_R",
                "test_net_expectancy_R",
                "stable_positive",
            ]
        ],
        on=["condition_group", "condition_value", "side"],
        how="left",
    )
    eligible = merged[
        (merged["sample_count"] >= min_samples)
        & (merged["condition_group"] != "overall")
        & (merged["net_expectancy_R"] > 0)
        & (merged["hit_stop_first_rate"] < 0.95)
    ].copy()
    eligible["model_reason"] = eligible.apply(model_reason, axis=1)
    top_long = eligible[eligible["side"] == "long"].sort_values(
        ["stable_positive", "net_expectancy_R", "sample_count"], ascending=[False, False, False]
    ).head(5)
    top_short = eligible[eligible["side"] == "short"].sort_values(
        ["stable_positive", "net_expectancy_R", "sample_count"], ascending=[False, False, False]
    ).head(5)
    avoid = merged[
        (merged["sample_count"] >= min_samples)
        & (merged["condition_group"] != "overall")
        & ((merged["net_expectancy_R"] < 0) | (merged["hit_stop_first_rate"] >= 0.93))
    ].copy()
    avoid["avoid_reason"] = avoid.apply(avoid_reason, axis=1)
    no_trade = avoid.sort_values(["net_expectancy_R", "hit_stop_first_rate"], ascending=[True, False]).head(20)
    return top_long.reset_index(drop=True), top_short.reset_index(drop=True), no_trade.reset_index(drop=True)


def write_outputs(
    *,
    output_root: Path,
    features: pd.DataFrame,
    summary_extended: pd.DataFrame,
    summary_all: pd.DataFrame,
    summary_risk_filtered: pd.DataFrame,
    exit_comparison: pd.DataFrame,
    time_stability: pd.DataFrame,
    train_test: pd.DataFrame,
    threshold_sensitivity: pd.DataFrame,
    top_long: pd.DataFrame,
    top_short: pd.DataFrame,
    no_trade: pd.DataFrame,
    symbol: str,
    inst_id: str,
    bar: str,
    hourly_path: str | Path | None,
    timezone_offset_hours: int,
    session_close_hour: int,
    cost_r: float,
    min_risk_pct: float,
    min_samples: int,
) -> None:
    files = {
        "features_last_hour_daily_extended.csv": features,
        "condition_summary_extended.csv": summary_extended,
        "condition_summary_all_samples.csv": summary_all,
        "condition_summary_risk_filtered.csv": summary_risk_filtered,
        "exit_method_comparison.csv": exit_comparison,
        "condition_time_stability.csv": time_stability,
        "train_test_summary.csv": train_test,
        "threshold_sensitivity.csv": threshold_sensitivity,
        "top_long_setups.csv": top_long,
        "top_short_setups.csv": top_short,
        "no_trade_setups.csv": no_trade,
    }
    for filename, frame in files.items():
        frame.to_csv(output_root / filename, index=False, encoding="utf-8-sig")
    (output_root / "coverage_report.md").write_text(build_coverage_report(), encoding="utf-8-sig")
    (output_root / "research_report_phase2.md").write_text(
        build_phase2_report(
            features=features,
            summary=summary_extended,
            exit_comparison=exit_comparison,
            train_test=train_test,
            threshold_sensitivity=threshold_sensitivity,
            top_long=top_long,
            top_short=top_short,
            no_trade=no_trade,
            symbol=symbol,
            inst_id=inst_id,
            bar=bar,
            hourly_path=hourly_path,
            timezone_offset_hours=timezone_offset_hours,
            session_close_hour=session_close_hour,
            cost_r=cost_r,
            min_risk_pct=min_risk_pct,
            min_samples=min_samples,
        ),
        encoding="utf-8-sig",
    )


def build_coverage_report() -> str:
    rows = [
        ("8:00 session daily aggregation", "covered in phase1", "Reused; daily session is 8:00 to next 8:00."),
        ("7:00-8:00 signal bar", "covered in phase1", "Reused; signal bar is the last hour of the 8:00 session."),
        ("Entry after 8:00 close", "covered in phase1", "Reused."),
        ("Long/short stop from signal low/high", "covered in phase1", "Reused."),
        ("1R/2R/stop/final close R", "covered in phase1", "Extended with first-touch path flags."),
        ("Temporary daily state before 7:00", "added in phase2", "Adds temp_day_*_7h and last_hour_change_type."),
        ("Last hour changes daily color", "added in phase2", "Adds changed_bear_to_bull and changed_bull_to_bear."),
        ("Six-part wick structure", "added in phase2", "Adds last_hour_wick_type."),
        ("Last hour close area in daily range", "added in phase2", "Adds close_area and daily_close_location."),
        ("Four-way volume bucket", "added in phase2", "Adds volume_bucket based on prior 24h average."),
        ("Tail breakout / failed breakout", "added in phase2", "Adds break_prev_23h_high/low and failed_*_breakout."),
        ("Trend regime", "covered and renamed in phase2", "Uses current completed 8:00 daily EMA20/EMA50 regime."),
        ("Recent 2-3 day structure", "added in phase2", "Adds consecutive bull/bear, big day, doji, long wick counts."),
        ("Path backtest first touch", "added in phase2", "Adds hit_stop_first, hit_1R_first, hit_2R_first per side."),
        ("Exit method comparison", "added in phase2", "Exports exit_method_comparison.csv."),
        ("Year/quarter stability", "added in phase2", "Exports condition_time_stability.csv."),
        ("Train/test stability", "added in phase2", "Exports train_test_summary.csv."),
        ("Threshold sensitivity", "added in phase2", "Exports threshold_sensitivity.csv."),
        ("Net expectancy after cost", "added in phase2", "Uses net_expectancy_R with configurable cost_r."),
        ("Top/no-trade model lists", "added in phase2", "Exports top_long/top_short/no_trade setup files."),
    ]
    lines = [
        "# Coverage Report",
        "",
        "This report checks the phase1 coverage against the phase2 request. Phase2 keeps the phase1 data model and adds missing modules as additional outputs.",
        "",
        "| Module | Status | Notes |",
        "| --- | --- | --- |",
    ]
    lines.extend(f"| {module} | {status} | {notes} |" for module, status, notes in rows)
    return "\n".join(lines) + "\n"


def build_phase2_report(
    *,
    features: pd.DataFrame,
    summary: pd.DataFrame,
    exit_comparison: pd.DataFrame,
    train_test: pd.DataFrame,
    threshold_sensitivity: pd.DataFrame,
    top_long: pd.DataFrame,
    top_short: pd.DataFrame,
    no_trade: pd.DataFrame,
    symbol: str,
    inst_id: str,
    bar: str,
    hourly_path: str | Path | None,
    timezone_offset_hours: int,
    session_close_hour: int,
    cost_r: float,
    min_risk_pct: float,
    min_samples: int,
) -> str:
    overall = summary[summary["condition_group"] == "overall"].copy()
    best_exit = exit_comparison[exit_comparison["condition_group"] == "overall"].sort_values("net_expectancy_R", ascending=False).head(10)
    source = f"{hourly_path}" if hourly_path is not None else f"local candle cache: {inst_id}/{bar}"
    lines = [
        "# Phase2 Research Report: 7:00-8:00 Last Hour Daily Relationship",
        "",
        "## Settings",
        f"- symbol: `{symbol}`",
        f"- source: `{source}`",
        f"- local timezone offset: `UTC+{timezone_offset_hours}`",
        f"- daily session: `{session_close_hour:02d}:00` to next `{session_close_hour:02d}:00`",
        f"- transaction cost assumption: `{cost_r:.3f}R` per trade",
        f"- risk filter: both long and short signal stop distance must be at least `{min_risk_pct:.3%}` for risk-filtered summaries",
        f"- conclusion threshold: sample count >= `{min_samples}`",
        "",
        "## Sample Range",
        f"- samples: `{len(features)}`",
        f"- start: `{features['session_date'].iloc[0]}`",
        f"- end: `{features['session_date'].iloc[-1]}`",
        "",
        "## Overall Baseline",
        markdown_table(overall, ["side", "sample_count", "net_expectancy_R", "gross_expectancy_R", "hit_stop_first_rate", "hit_1R_rate", "hit_2R_rate", "avg_final_close_R"]),
        "",
        "## Best Exit Methods Overall",
        markdown_table(best_exit, ["side", "exit_method", "sample_count", "net_expectancy_R", "median_net_R", "positive_net_rate"]),
        "",
        "## Top Long Setups",
        markdown_table(top_long, ["condition_group", "condition_value", "sample_count", "net_expectancy_R", "hit_stop_first_rate", "hit_1R_rate", "hit_2R_rate", "test_net_expectancy_R", "model_reason"]),
        "",
        "## Top Short Setups",
        markdown_table(top_short, ["condition_group", "condition_value", "sample_count", "net_expectancy_R", "hit_stop_first_rate", "hit_1R_rate", "hit_2R_rate", "test_net_expectancy_R", "model_reason"]),
        "",
        "## No Trade Setups",
        markdown_table(no_trade, ["condition_group", "condition_value", "side", "sample_count", "net_expectancy_R", "hit_stop_first_rate", "avoid_reason"]),
        "",
        "## Stability Notes",
        f"- train/test rows exported: `{len(train_test)}`",
        f"- threshold sensitivity rows exported: `{len(threshold_sensitivity)}`",
        "- Treat rows below 30 samples as observation only; they are included in `condition_summary_all_samples.csv` for exploration.",
        "- Phase2 uses only data available at the 8:00 entry decision: the completed 7:00-8:00 bar, the completed 8:00 session day, and prior sessions. The next session is used only for outcomes.",
    ]
    return "\n".join(lines) + "\n"


def phase2_condition_columns() -> list[tuple[str, str | None]]:
    return [
        ("overall", None),
        ("temp_day_color_7h", "temp_day_color_7h"),
        ("prev_day_color", "prev_day_color"),
        ("signal_color", "signal_color"),
        ("changed_bear_to_bull", "changed_bear_to_bull"),
        ("changed_bull_to_bear", "changed_bull_to_bear"),
        ("last_hour_change_type", "last_hour_change_type"),
        ("last_hour_wick_type", "last_hour_wick_type"),
        ("close_area", "close_area"),
        ("volume_bucket", "volume_bucket"),
        ("break_prev_23h_high", "break_prev_23h_high"),
        ("break_prev_23h_low", "break_prev_23h_low"),
        ("failed_up_breakout", "failed_up_breakout"),
        ("failed_down_breakout", "failed_down_breakout"),
        ("breakout_type", "breakout_type"),
        ("trend_regime", "trend_regime"),
        ("prev_2_days_bullish", "prev_2_days_bullish"),
        ("prev_2_days_bearish", "prev_2_days_bearish"),
        ("prev_3_days_bullish", "prev_3_days_bullish"),
        ("prev_3_days_bearish", "prev_3_days_bearish"),
        ("big_bull_day", "big_bull_day"),
        ("big_bear_day", "big_bear_day"),
        ("doji_day", "doji_day"),
        ("long_wick_day", "long_wick_day"),
        ("recent_3day_bias", "recent_3day_bias"),
        ("phase2_simple_setup", "phase2_simple_setup"),
        ("phase2_core_setup", "phase2_core_setup"),
    ]


def exit_method_names() -> list[str]:
    return ["next_close", "stop_or_close", "fixed_1r", "fixed_2r", "breakeven_after_1r", "half_1r_hold"]


def location_ratio(value: float, low: float, high: float) -> float:
    rng = high - low
    if not np.isfinite(rng) or rng <= 0:
        return np.nan
    return (value - low) / rng


def classify_change_type(temp_color: str, final_color: str) -> str:
    if temp_color == "bear" and final_color == "bull":
        return "changed_bear_to_bull"
    if temp_color == "bull" and final_color == "bear":
        return "changed_bull_to_bear"
    if temp_color == final_color:
        return f"kept_{final_color}"
    return f"{temp_color}_to_{final_color}"


def classify_last_hour_wick(signal_color: str, body_ratio: float, upper_wick_ratio: float, lower_wick_ratio: float) -> str:
    if not np.isfinite(body_ratio):
        return "unknown"
    if signal_color == "bull":
        if body_ratio >= 0.6 and upper_wick_ratio <= 0.25 and lower_wick_ratio <= 0.25:
            return "true_strong_bull"
        if upper_wick_ratio >= 0.35:
            return "long_upper_bull"
        if lower_wick_ratio >= 0.35:
            return "long_lower_bull"
        return "normal_bull"
    if signal_color == "bear":
        if body_ratio >= 0.6 and upper_wick_ratio <= 0.25 and lower_wick_ratio <= 0.25:
            return "true_weak_bear"
        if upper_wick_ratio >= 0.35:
            return "long_upper_bear"
        if lower_wick_ratio >= 0.35:
            return "long_lower_bear"
        return "normal_bear"
    return "doji_or_flat"


def classify_close_area(ratio: float) -> str:
    if not np.isfinite(ratio):
        return "unknown"
    if ratio <= 0.33:
        return "low_area"
    if ratio >= 0.67:
        return "high_area"
    return "middle_area"


def classify_phase2_volume_bucket(ratio: float) -> str:
    if not np.isfinite(ratio):
        return "unknown"
    if ratio < 0.75:
        return "shrinking_volume"
    if ratio < 1.5:
        return "normal_volume"
    if ratio < 2.5:
        return "high_volume"
    return "huge_volume"


def classify_breakout_type(break_high: bool, break_low: bool, close_high: bool, close_low: bool, failed_up: bool, failed_down: bool) -> str:
    if close_high:
        return "confirmed_up_breakout"
    if close_low:
        return "confirmed_down_breakout"
    if failed_up and failed_down:
        return "two_sided_failed_breakout"
    if failed_up:
        return "failed_up_breakout"
    if failed_down:
        return "failed_down_breakout"
    if break_high:
        return "wick_up_breakout"
    if break_low:
        return "wick_down_breakout"
    return "no_breakout"


def model_reason(row: pd.Series) -> str:
    parts = [
        f"net {row['net_expectancy_R']:.2f}R",
        f"samples {int(row['sample_count'])}",
        f"stop-first {row['hit_stop_first_rate']:.1%}",
    ]
    if bool(row.get("stable_positive", False)):
        parts.append("train/test both positive")
    return "; ".join(parts)


def avoid_reason(row: pd.Series) -> str:
    reasons = []
    if row["net_expectancy_R"] < 0:
        reasons.append(f"negative net expectancy {row['net_expectancy_R']:.2f}R")
    if row["hit_stop_first_rate"] >= 0.93:
        reasons.append(f"stop-first too high {row['hit_stop_first_rate']:.1%}")
    if not reasons:
        reasons.append("weak reward/risk profile")
    return "; ".join(reasons)


def markdown_table(frame: pd.DataFrame, columns: list[str]) -> str:
    if frame.empty:
        return "_No rows._"
    existing = [column for column in columns if column in frame.columns]
    lines = [
        "| " + " | ".join(existing) + " |",
        "| " + " | ".join(["---"] * len(existing)) + " |",
    ]
    for _, row in frame[existing].iterrows():
        values = []
        for column in existing:
            value = row[column]
            if isinstance(value, (float, np.floating)):
                values.append(f"{value:.3f}" if np.isfinite(value) else "nan")
            else:
                values.append(str(value))
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines)


def mean_bool(values: pd.Series) -> float:
    clean = values.dropna()
    if clean.empty:
        return np.nan
    return float(clean.astype(float).mean())
