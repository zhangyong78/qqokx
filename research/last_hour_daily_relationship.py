from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from okx_quant.candle_store import get_candles
from stats.indicators import compute_atr, compute_ema
from utils.io import load_candle_frame


DEFAULT_MIN_SAMPLES_FOR_REPORT = 30


@dataclass(slots=True)
class LastHourDailyResearchResult:
    features: pd.DataFrame
    summary: pd.DataFrame
    output_dir: Path


def load_hourly_frame(*, hourly_path: str | Path | None, inst_id: str, bar: str) -> pd.DataFrame:
    if hourly_path is not None:
        frame = load_candle_frame(hourly_path)
    else:
        candles = get_candles(inst_id, bar)
        rows = [
            {
                "timestamp": pd.to_datetime(int(candle.ts), unit="ms", utc=True),
                "open": float(candle.open),
                "high": float(candle.high),
                "low": float(candle.low),
                "close": float(candle.close),
                "volume": float(candle.volume),
            }
            for candle in candles
        ]
        frame = pd.DataFrame(rows)
    if frame.empty:
        raise ValueError("hourly frame is empty")
    frame = frame.sort_values("timestamp").drop_duplicates("timestamp").reset_index(drop=True)
    if "symbol" not in frame.columns:
        frame["symbol"] = inst_id
    return frame


def run_last_hour_daily_relationship_research(
    *,
    hourly_path: str | Path | None,
    output_dir: str | Path,
    inst_id: str = "BTC-USDT-SWAP",
    bar: str = "1H",
    symbol: str | None = None,
    timezone_offset_hours: int = 8,
    session_close_hour: int = 8,
    min_samples_for_report: int = DEFAULT_MIN_SAMPLES_FOR_REPORT,
) -> LastHourDailyResearchResult:
    hourly_frame = load_hourly_frame(hourly_path=hourly_path, inst_id=inst_id, bar=bar)
    features = build_last_hour_daily_features(
        hourly_frame=hourly_frame,
        symbol=symbol or str(hourly_frame["symbol"].dropna().iloc[0]),
        timezone_offset_hours=timezone_offset_hours,
        session_close_hour=session_close_hour,
    )
    summary = summarize_conditions(features)
    output_root = Path(output_dir)
    output_root.mkdir(parents=True, exist_ok=True)
    features_path = output_root / "features_last_hour_daily.csv"
    summary_path = output_root / "condition_summary.csv"
    report_path = output_root / "research_report.md"
    features.to_csv(features_path, index=False, encoding="utf-8-sig")
    summary.to_csv(summary_path, index=False, encoding="utf-8-sig")
    report_path.write_text(
        build_research_report(
            features=features,
            summary=summary,
            symbol=symbol or inst_id,
            hourly_path=hourly_path,
            inst_id=inst_id,
            bar=bar,
            timezone_offset_hours=timezone_offset_hours,
            session_close_hour=session_close_hour,
            min_samples_for_report=min_samples_for_report,
        ),
        encoding="utf-8-sig",
    )
    return LastHourDailyResearchResult(features=features, summary=summary, output_dir=output_root)


def build_last_hour_daily_features(
    *,
    hourly_frame: pd.DataFrame,
    symbol: str,
    timezone_offset_hours: int = 8,
    session_close_hour: int = 8,
) -> pd.DataFrame:
    hourly = hourly_frame.copy().sort_values("timestamp").reset_index(drop=True)
    hourly["timestamp"] = pd.to_datetime(hourly["timestamp"], utc=True)
    local_offset = pd.Timedelta(hours=timezone_offset_hours)
    session_offset = pd.Timedelta(hours=session_close_hour)
    hourly["local_timestamp"] = hourly["timestamp"] + local_offset
    hourly["session_date"] = (hourly["local_timestamp"] - session_offset).dt.floor("D")
    hourly["local_hour"] = hourly["local_timestamp"].dt.hour.astype(int)
    hourly["bar_index_in_session"] = hourly.groupby("session_date").cumcount()
    hourly["volume_avg_24_before"] = hourly["volume"].shift(1).rolling(24, min_periods=24).mean()
    hourly["close_ema_24"] = compute_ema(hourly["close"], period=24)

    daily = build_session_daily(hourly)
    daily = attach_daily_context(daily)

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
        row = build_feature_row(
            symbol=symbol,
            session_date=session_date,
            next_session_date=next_session_date,
            current_day=current_day,
            next_day=next_day,
            signal=signal,
            pre_signal=pre_signal,
            next_session_hourly=next_session_hourly,
        )
        rows.append(row)

    features = pd.DataFrame(rows).sort_values("session_date").reset_index(drop=True)
    return features


def build_session_daily(hourly: pd.DataFrame) -> pd.DataFrame:
    daily = (
        hourly.groupby("session_date", as_index=False)
        .agg(
            open=("open", "first"),
            high=("high", "max"),
            low=("low", "min"),
            close=("close", "last"),
            volume=("volume", "sum"),
            session_open_time_utc=("timestamp", "first"),
            session_close_bar_time_utc=("timestamp", "last"),
            session_open_time_local=("local_timestamp", "first"),
            session_close_bar_time_local=("local_timestamp", "last"),
            bar_count=("timestamp", "count"),
        )
    )
    return daily


def attach_daily_context(daily: pd.DataFrame) -> pd.DataFrame:
    out = daily.copy().sort_values("session_date").reset_index(drop=True)
    day_range = (out["high"] - out["low"]).replace(0, np.nan)
    body = (out["close"] - out["open"]).abs()
    out["body"] = body
    out["range"] = out["high"] - out["low"]
    out["return_pct"] = (out["close"] / out["open"]) - 1
    out["body_ratio"] = body / day_range
    out["upper_wick_ratio"] = (out["high"] - out[["open", "close"]].max(axis=1)) / day_range
    out["lower_wick_ratio"] = (out[["open", "close"]].min(axis=1) - out["low"]) / day_range
    out["color"] = candle_color(out["open"], out["close"])
    out["ema20"] = compute_ema(out["close"], period=20)
    out["ema50"] = compute_ema(out["close"], period=50)
    out["atr20"] = compute_atr(high=out["high"], low=out["low"], close=out["close"], period=20)
    out["trend_bucket"] = classify_trend_bucket(out["close"], out["ema20"], out["ema50"])
    out["prior_high"] = out["high"].shift(1)
    out["prior_low"] = out["low"].shift(1)
    out["day_breakout_bucket"] = np.select(
        [
            out["close"] > out["prior_high"],
            out["close"] < out["prior_low"],
            out["high"] > out["prior_high"],
            out["low"] < out["prior_low"],
        ],
        ["close_break_prior_high", "close_break_prior_low", "wick_break_prior_high", "wick_break_prior_low"],
        default="inside_prior_day_range",
    )
    return out


def build_feature_row(
    *,
    symbol: str,
    session_date: pd.Timestamp,
    next_session_date: pd.Timestamp,
    current_day: pd.Series,
    next_day: pd.Series,
    signal: pd.Series,
    pre_signal: pd.DataFrame,
    next_session_hourly: pd.DataFrame,
) -> dict[str, object]:
    signal_range = max(float(signal["high"] - signal["low"]), 0.0)
    signal_body = abs(float(signal["close"] - signal["open"]))
    safe_signal_range = signal_range if signal_range > 0 else np.nan
    signal_body_ratio = signal_body / safe_signal_range if safe_signal_range == safe_signal_range else np.nan
    upper_wick_ratio = (float(signal["high"]) - max(float(signal["open"]), float(signal["close"]))) / safe_signal_range if safe_signal_range == safe_signal_range else np.nan
    lower_wick_ratio = (min(float(signal["open"]), float(signal["close"])) - float(signal["low"])) / safe_signal_range if safe_signal_range == safe_signal_range else np.nan
    close_location = (float(signal["close"]) - float(signal["low"])) / safe_signal_range if safe_signal_range == safe_signal_range else np.nan
    signal_volume_ratio = float(signal["volume"]) / float(signal["volume_avg_24_before"]) if pd.notna(signal["volume_avg_24_before"]) and float(signal["volume_avg_24_before"]) > 0 else np.nan
    pre_signal_high = float(pre_signal["high"].max())
    pre_signal_low = float(pre_signal["low"].min())

    row: dict[str, object] = {
        "session_date": session_date.strftime("%Y-%m-%d"),
        "session_date_ts": session_date,
        "symbol": symbol,
        "entry_time_utc": signal["timestamp"] + pd.Timedelta(hours=1),
        "entry_time_local": signal["local_timestamp"] + pd.Timedelta(hours=1),
        "next_session_date": next_session_date.strftime("%Y-%m-%d"),
        "prev_day_open": float(current_day["open"]),
        "prev_day_high": float(current_day["high"]),
        "prev_day_low": float(current_day["low"]),
        "prev_day_close": float(current_day["close"]),
        "prev_day_volume": float(current_day["volume"]),
        "prev_day_color": str(current_day["color"]),
        "prev_day_return_pct": float(current_day["return_pct"]),
        "prev_day_body_ratio": float_or_nan(current_day["body_ratio"]),
        "prev_day_upper_wick_ratio": float_or_nan(current_day["upper_wick_ratio"]),
        "prev_day_lower_wick_ratio": float_or_nan(current_day["lower_wick_ratio"]),
        "prev_day_trend_bucket": str(current_day["trend_bucket"]),
        "prev_day_breakout_bucket": str(current_day["day_breakout_bucket"]),
        "prev_day_atr20": float_or_nan(current_day["atr20"]),
        "signal_time_utc": signal["timestamp"],
        "signal_time_local": signal["local_timestamp"],
        "signal_open": float(signal["open"]),
        "signal_high": float(signal["high"]),
        "signal_low": float(signal["low"]),
        "signal_close": float(signal["close"]),
        "signal_volume": float(signal["volume"]),
        "signal_color": candle_color_scalar(float(signal["open"]), float(signal["close"])),
        "signal_return_pct": (float(signal["close"]) / float(signal["open"])) - 1 if float(signal["open"]) else np.nan,
        "signal_range": signal_range,
        "signal_body": signal_body,
        "signal_body_ratio": signal_body_ratio,
        "signal_upper_wick_ratio": upper_wick_ratio,
        "signal_lower_wick_ratio": lower_wick_ratio,
        "signal_close_location": close_location,
        "signal_strength_bucket": classify_strength_bucket(signal_body_ratio),
        "signal_wick_bucket": classify_wick_bucket(upper_wick_ratio, lower_wick_ratio),
        "signal_volume_ratio_24": signal_volume_ratio,
        "signal_volume_bucket": classify_volume_bucket(signal_volume_ratio),
        "signal_breakout_bucket": classify_signal_breakout_bucket(
            signal_high=float(signal["high"]),
            signal_low=float(signal["low"]),
            signal_close=float(signal["close"]),
            pre_signal_high=pre_signal_high,
            pre_signal_low=pre_signal_low,
        ),
        "same_direction_prev_day_and_signal": bool(str(current_day["color"]) == candle_color_scalar(float(signal["open"]), float(signal["close"]))),
        "next_day_open": float(next_day["open"]),
        "next_day_high": float(next_day["high"]),
        "next_day_low": float(next_day["low"]),
        "next_day_close": float(next_day["close"]),
        "next_day_volume": float(next_day["volume"]),
        "next_day_color": str(next_day["color"]),
        "next_day_return_pct": float(next_day["return_pct"]),
    }
    row.update(evaluate_side(next_session_hourly, entry_price=float(signal["close"]), stop_price=float(signal["low"]), side="long"))
    row.update(evaluate_side(next_session_hourly, entry_price=float(signal["close"]), stop_price=float(signal["high"]), side="short"))
    return row


def evaluate_side(
    next_session_hourly: pd.DataFrame,
    *,
    entry_price: float,
    stop_price: float,
    side: str,
) -> dict[str, object]:
    prefix = side.lower()
    risk = (entry_price - stop_price) if prefix == "long" else (stop_price - entry_price)
    default = {
        f"{prefix}_entry_price": entry_price,
        f"{prefix}_stop_price": stop_price,
        f"{prefix}_risk": risk,
        f"{prefix}_stop_hit": np.nan,
        f"{prefix}_hit_1r": np.nan,
        f"{prefix}_hit_2r": np.nan,
        f"{prefix}_first_touch": "invalid_risk",
        f"{prefix}_final_close_r": np.nan,
        f"{prefix}_realized_r": np.nan,
        f"{prefix}_mfe_r": np.nan,
        f"{prefix}_mae_r": np.nan,
        f"{prefix}_ambiguous_bars": 0,
    }
    if not np.isfinite(risk) or risk <= 0:
        return default

    target_1r = entry_price + risk if prefix == "long" else entry_price - risk
    target_2r = entry_price + (2 * risk) if prefix == "long" else entry_price - (2 * risk)
    stop_hit = False
    hit_1r = False
    hit_2r = False
    first_touch = "none"
    ambiguous_bars = 0

    for _, candle in next_session_hourly.iterrows():
        high = float(candle["high"])
        low = float(candle["low"])
        if prefix == "long":
            stop_touched = low <= stop_price
            target_1r_touched = high >= target_1r
            target_2r_touched = high >= target_2r
        else:
            stop_touched = high >= stop_price
            target_1r_touched = low <= target_1r
            target_2r_touched = low <= target_2r

        if stop_touched and (target_1r_touched or target_2r_touched):
            ambiguous_bars += 1
            stop_hit = True
            if first_touch == "none":
                first_touch = "ambiguous_stop_first"
            continue

        if target_2r_touched:
            hit_2r = True
            hit_1r = True
            if first_touch == "none":
                first_touch = "2r"
        elif target_1r_touched:
            hit_1r = True
            if first_touch == "none":
                first_touch = "1r"

        if stop_touched:
            stop_hit = True
            if first_touch == "none":
                first_touch = "stop"

    next_close = float(next_session_hourly["close"].iloc[-1])
    final_close_r = ((next_close - entry_price) / risk) if prefix == "long" else ((entry_price - next_close) / risk)
    favorable_move = float(next_session_hourly["high"].max()) - entry_price if prefix == "long" else entry_price - float(next_session_hourly["low"].min())
    adverse_move = entry_price - float(next_session_hourly["low"].min()) if prefix == "long" else float(next_session_hourly["high"].max()) - entry_price
    realized_r = -1.0 if stop_hit else final_close_r

    return {
        f"{prefix}_entry_price": entry_price,
        f"{prefix}_stop_price": stop_price,
        f"{prefix}_risk": risk,
        f"{prefix}_stop_hit": stop_hit,
        f"{prefix}_hit_1r": hit_1r,
        f"{prefix}_hit_2r": hit_2r,
        f"{prefix}_first_touch": first_touch,
        f"{prefix}_final_close_r": final_close_r,
        f"{prefix}_realized_r": realized_r,
        f"{prefix}_mfe_r": favorable_move / risk,
        f"{prefix}_mae_r": adverse_move / risk,
        f"{prefix}_ambiguous_bars": ambiguous_bars,
    }


def summarize_conditions(features: pd.DataFrame) -> pd.DataFrame:
    working = features.copy()
    working["prev_day_signal_combo"] = working["prev_day_color"].astype(str) + "__" + working["signal_color"].astype(str)
    working["trend_signal_combo"] = working["prev_day_trend_bucket"].astype(str) + "__" + working["signal_color"].astype(str)
    working["breakout_signal_combo"] = working["signal_breakout_bucket"].astype(str) + "__" + working["signal_color"].astype(str)

    condition_columns = [
        ("overall", None),
        ("prev_day_color", "prev_day_color"),
        ("signal_color", "signal_color"),
        ("signal_strength_bucket", "signal_strength_bucket"),
        ("signal_wick_bucket", "signal_wick_bucket"),
        ("signal_volume_bucket", "signal_volume_bucket"),
        ("prev_day_trend_bucket", "prev_day_trend_bucket"),
        ("prev_day_breakout_bucket", "prev_day_breakout_bucket"),
        ("signal_breakout_bucket", "signal_breakout_bucket"),
        ("same_direction_prev_day_and_signal", "same_direction_prev_day_and_signal"),
        ("prev_day_signal_combo", "prev_day_signal_combo"),
        ("trend_signal_combo", "trend_signal_combo"),
        ("breakout_signal_combo", "breakout_signal_combo"),
    ]

    rows: list[dict[str, object]] = []
    for group_name, column_name in condition_columns:
        if column_name is None:
            grouped_items = [("all", working)]
        else:
            grouped_items = sorted(working.groupby(column_name, dropna=False), key=lambda item: str(item[0]))
        for condition_value, subset in grouped_items:
            if subset.empty:
                continue
            rows.append(build_summary_row(subset, group_name=group_name, condition_value=condition_value, side="long"))
            rows.append(build_summary_row(subset, group_name=group_name, condition_value=condition_value, side="short"))
    summary = pd.DataFrame(rows)
    summary = summary.sort_values(["side", "condition_group", "sample_count", "expectancy_r"], ascending=[True, True, False, False]).reset_index(drop=True)
    return summary


def build_summary_row(subset: pd.DataFrame, *, group_name: str, condition_value: object, side: str) -> dict[str, object]:
    prefix = side.lower()
    realized = pd.to_numeric(subset[f"{prefix}_realized_r"], errors="coerce")
    final_close_r = pd.to_numeric(subset[f"{prefix}_final_close_r"], errors="coerce")
    mfe_r = pd.to_numeric(subset[f"{prefix}_mfe_r"], errors="coerce")
    mae_r = pd.to_numeric(subset[f"{prefix}_mae_r"], errors="coerce")
    return {
        "condition_group": group_name,
        "condition_value": str(condition_value),
        "side": prefix,
        "sample_count": int(len(subset)),
        "next_day_bull_rate": mean_bool(subset["next_day_color"] == "bull"),
        "next_day_bear_rate": mean_bool(subset["next_day_color"] == "bear"),
        "next_day_doji_rate": mean_bool(subset["next_day_color"] == "doji"),
        "stop_rate": mean_bool(subset[f"{prefix}_stop_hit"]),
        "hit_1r_rate": mean_bool(subset[f"{prefix}_hit_1r"]),
        "hit_2r_rate": mean_bool(subset[f"{prefix}_hit_2r"]),
        "close_positive_rate": mean_bool(final_close_r > 0),
        "expectancy_r": float(realized.mean()) if realized.notna().any() else np.nan,
        "avg_final_close_r": float(final_close_r.mean()) if final_close_r.notna().any() else np.nan,
        "median_final_close_r": float(final_close_r.median()) if final_close_r.notna().any() else np.nan,
        "avg_mfe_r": float(mfe_r.mean()) if mfe_r.notna().any() else np.nan,
        "avg_mae_r": float(mae_r.mean()) if mae_r.notna().any() else np.nan,
        "avg_next_day_return_pct": float(pd.to_numeric(subset["next_day_return_pct"], errors="coerce").mean()),
        "ambiguous_bar_rate": float(pd.to_numeric(subset[f"{prefix}_ambiguous_bars"], errors="coerce").gt(0).mean()),
    }


def build_research_report(
    *,
    features: pd.DataFrame,
    summary: pd.DataFrame,
    symbol: str,
    hourly_path: str | Path | None,
    inst_id: str,
    bar: str,
    timezone_offset_hours: int,
    session_close_hour: int,
    min_samples_for_report: int,
) -> str:
    overall_long = summary[(summary["condition_group"] == "overall") & (summary["side"] == "long")].iloc[0]
    overall_short = summary[(summary["condition_group"] == "overall") & (summary["side"] == "short")].iloc[0]

    stable = summary[(summary["sample_count"] >= min_samples_for_report) & (summary["condition_group"] != "overall")].copy()
    top_long = stable[stable["side"] == "long"].sort_values(["expectancy_r", "sample_count"], ascending=[False, False]).head(8)
    top_short = stable[stable["side"] == "short"].sort_values(["expectancy_r", "sample_count"], ascending=[False, False]).head(8)
    weak_long = stable[stable["side"] == "long"].sort_values(["expectancy_r", "sample_count"], ascending=[True, False]).head(6)
    weak_short = stable[stable["side"] == "short"].sort_values(["expectancy_r", "sample_count"], ascending=[True, False]).head(6)

    lines = [
        "# 7:00-8:00 最后一小时 vs 下一根 8:00 日线 研究报告",
        "",
        "## 研究设置",
        f"- 标的：`{symbol}`（默认缓存标识 `{inst_id}`，周期 `{bar}`）",
        f"- 数据来源：`{hourly_path}`" if hourly_path is not None else f"- 数据来源：本地 candle cache（`{inst_id}` / `{bar}`）",
        f"- 日线定义：本地时间每天 `{session_close_hour:02d}:00` 到次日 `{session_close_hour:02d}:00` 聚合",
        f"- 信号 K 线：本地时间每天 `{(session_close_hour - 1) % 24:02d}:00-{session_close_hour:02d}:00` 的 1 小时 K 线",
        "- 入场假设：信号 K 线收盘后立即入场",
        "- 多头止损：信号 K 线低点；空头止损：信号 K 线高点",
        "- 收益统计：下一根 8:00 日线内是否触发止损、是否触发 1R/2R、下一日线收盘 R、止损约束后的期望 R",
        "- 同根小时线内同时触及止损和目标时，按保守口径记为 `stop first`，避免乐观偏差",
        "- 过滤说明：报告中的重点条件默认要求样本数不少于 "
        f"`{min_samples_for_report}`，以降低小样本误判",
        "",
        "## 数据范围",
        f"- 样本数：`{len(features)}`",
        f"- 信号起点：`{features['session_date'].iloc[0]}`",
        f"- 信号终点：`{features['session_date'].iloc[-1]}`",
        f"- 时区偏移：`UTC+{timezone_offset_hours}`",
        "",
        "## 全样本基线",
        "",
        "### Long（按最后一小时收盘做多）",
        f"- 样本数：`{int(overall_long['sample_count'])}`",
        f"- 下一日日线收阳率：`{pct(overall_long['next_day_bull_rate'])}`；收阴率：`{pct(overall_long['next_day_bear_rate'])}`",
        f"- 1R 命中率：`{pct(overall_long['hit_1r_rate'])}`；2R 命中率：`{pct(overall_long['hit_2r_rate'])}`；止损率：`{pct(overall_long['stop_rate'])}`",
        f"- 下一日线收盘平均 R：`{fmt_num(overall_long['avg_final_close_r'])}`；止损约束后的期望 R：`{fmt_num(overall_long['expectancy_r'])}`",
        "",
        "### Short（按最后一小时收盘做空）",
        f"- 样本数：`{int(overall_short['sample_count'])}`",
        f"- 下一日日线收阳率：`{pct(overall_short['next_day_bull_rate'])}`；收阴率：`{pct(overall_short['next_day_bear_rate'])}`",
        f"- 1R 命中率：`{pct(overall_short['hit_1r_rate'])}`；2R 命中率：`{pct(overall_short['hit_2r_rate'])}`；止损率：`{pct(overall_short['stop_rate'])}`",
        f"- 下一日线收盘平均 R：`{fmt_num(overall_short['avg_final_close_r'])}`；止损约束后的期望 R：`{fmt_num(overall_short['expectancy_r'])}`",
        "",
        "## 第一版重点条件",
        "- 已纳入的条件：前一日日线阴阳、最后一小时阴阳、最后一小时强弱、影线、量能、趋势背景、日内最后一小时突破、前一日相对前日突破。",
        "- 完整结果见 `condition_summary.csv`，以下只摘录样本数足够的高信号条件。",
        "",
        "### Long Top Conditions",
        render_markdown_table(
            top_long,
            columns=[
                "condition_group",
                "condition_value",
                "sample_count",
                "expectancy_r",
                "avg_final_close_r",
                "hit_1r_rate",
                "hit_2r_rate",
                "stop_rate",
            ],
        ),
        "",
        "### Short Top Conditions",
        render_markdown_table(
            top_short,
            columns=[
                "condition_group",
                "condition_value",
                "sample_count",
                "expectancy_r",
                "avg_final_close_r",
                "hit_1r_rate",
                "hit_2r_rate",
                "stop_rate",
            ],
        ),
        "",
        "### Long Avoid Conditions",
        render_markdown_table(
            weak_long,
            columns=[
                "condition_group",
                "condition_value",
                "sample_count",
                "expectancy_r",
                "avg_final_close_r",
                "hit_1r_rate",
                "hit_2r_rate",
                "stop_rate",
            ],
        ),
        "",
        "### Short Avoid Conditions",
        render_markdown_table(
            weak_short,
            columns=[
                "condition_group",
                "condition_value",
                "sample_count",
                "expectancy_r",
                "avg_final_close_r",
                "hit_1r_rate",
                "hit_2r_rate",
                "stop_rate",
            ],
        ),
        "",
        "## 结果解读建议",
        "- `avg_final_close_r` 反映“如果完全持有到下一根 8:00 日线收盘”的方向性优势。",
        "- 当止损非常贴近入场时，`avg_final_close_r` 可能被少量极端样本放大，因此判断优先级时应更看重 `expectancy_r`、`stop_rate` 和样本数。",
        "- `expectancy_r` 反映“带止损但不设止盈、持有到下一根 8:00 收盘”的更接近交易执行的预期值。",
        "- `hit_1r_rate` / `hit_2r_rate` 可以帮助判断是否值得在第二版研究里加入分批止盈、保本或移动止损规则。",
        "- 如果某个条件的命中率看起来很好，但样本数太低，应优先怀疑偶然性而不是直接上实盘结论。",
    ]
    return "\n".join(lines) + "\n"


def render_markdown_table(frame: pd.DataFrame, *, columns: list[str]) -> str:
    if frame.empty:
        return "_无满足样本门槛的条件_"
    headers = columns
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for _, row in frame.iterrows():
        values = []
        for column in columns:
            value = row[column]
            if column == "sample_count":
                values.append(str(int(value)))
            elif column.endswith("_rate"):
                values.append(pct(value))
            elif isinstance(value, (float, np.floating)):
                values.append(fmt_num(value))
            else:
                values.append(str(value))
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines)


def candle_color(open_values: pd.Series, close_values: pd.Series) -> pd.Series:
    return pd.Series(
        np.where(close_values > open_values, "bull", np.where(close_values < open_values, "bear", "doji")),
        index=open_values.index,
    )


def candle_color_scalar(open_price: float, close_price: float) -> str:
    if close_price > open_price:
        return "bull"
    if close_price < open_price:
        return "bear"
    return "doji"


def classify_strength_bucket(body_ratio: float) -> str:
    if not np.isfinite(body_ratio):
        return "unknown"
    if body_ratio >= 0.67:
        return "strong"
    if body_ratio >= 0.33:
        return "medium"
    return "weak"


def classify_wick_bucket(upper_wick_ratio: float, lower_wick_ratio: float) -> str:
    if not np.isfinite(upper_wick_ratio) or not np.isfinite(lower_wick_ratio):
        return "unknown"
    if upper_wick_ratio >= 0.35 and lower_wick_ratio >= 0.35:
        return "two_sided"
    if upper_wick_ratio >= 0.35:
        return "upper_rejection"
    if lower_wick_ratio >= 0.35:
        return "lower_rejection"
    return "balanced"


def classify_volume_bucket(volume_ratio: float) -> str:
    if not np.isfinite(volume_ratio):
        return "unknown"
    if volume_ratio >= 1.5:
        return "high_volume"
    if volume_ratio < 0.75:
        return "low_volume"
    return "normal_volume"


def classify_trend_bucket(close: pd.Series, ema20: pd.Series, ema50: pd.Series) -> pd.Series:
    trend = np.select(
        [
            (close > ema20) & (ema20 > ema50),
            (close < ema20) & (ema20 < ema50),
        ],
        ["uptrend", "downtrend"],
        default="sideways",
    )
    return pd.Series(trend, index=close.index)


def classify_signal_breakout_bucket(
    *,
    signal_high: float,
    signal_low: float,
    signal_close: float,
    pre_signal_high: float,
    pre_signal_low: float,
) -> str:
    if signal_close > pre_signal_high:
        return "close_break_session_high"
    if signal_close < pre_signal_low:
        return "close_break_session_low"
    if signal_high > pre_signal_high:
        return "wick_break_session_high"
    if signal_low < pre_signal_low:
        return "wick_break_session_low"
    return "inside_session_range"


def mean_bool(values: pd.Series) -> float:
    clean = values.dropna()
    if clean.empty:
        return np.nan
    return float(clean.astype(float).mean())


def float_or_nan(value: object) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return np.nan


def pct(value: object) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "nan"
    if not np.isfinite(number):
        return "nan"
    return f"{number * 100:.1f}%"


def fmt_num(value: object) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "nan"
    if not np.isfinite(number):
        return "nan"
    return f"{number:.2f}"
