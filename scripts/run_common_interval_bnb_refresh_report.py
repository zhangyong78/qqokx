from __future__ import annotations

import base64
import html
import io
import json
import math
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from okx_quant.backtest import _run_backtest_with_loaded_data
from okx_quant.candle_cache import load_candle_cache
from okx_quant.okx_client import OkxRestClient
from okx_quant.persistence import analysis_report_dir_path
from okx_quant.pricing import format_decimal_fixed
from okx_quant.timeframe import closed_candle_available_timestamps
from scripts.run_btc_daily_ma_direction_filter_research import (
    LONG_MAKER_FEE_RATE,
    LONG_TAKER_FEE_RATE,
    build_daily_direction_bias,
    format_ts,
)
from scripts.run_leadership_multi_coin_best_params_full_report import (
    LONG_GATES,
    LONG_PROFILES,
    SYMBOLS,
    SYMBOL_LABELS,
    build_concurrent_chart,
    build_equity_chart,
    build_leverage_table,
    build_long_config,
    concurrent_profile,
    dataframe_to_html,
    figure_to_base64,
    filter_scope,
    fmt2,
    metrics_from_frame,
    run_long_trades,
    run_short_trades,
)
from scripts.run_multi_coin_short_recommendation_and_pullback_report import load_recommendations, parse_strategy_key


plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "Noto Sans CJK SC", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False


REPORT_DIR = analysis_report_dir_path()
REPORT_DIR.mkdir(parents=True, exist_ok=True)

STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
HTML_PATH = REPORT_DIR / f"common_interval_bnb_refresh_report_{STAMP}.html"
JSON_PATH = REPORT_DIR / f"common_interval_bnb_refresh_report_{STAMP}.json"
TRADES_CSV = REPORT_DIR / f"common_interval_bnb_refresh_trades_{STAMP}.csv"
SUMMARY_CSV = REPORT_DIR / f"common_interval_bnb_refresh_summary_{STAMP}.csv"
MONTHLY_AGG_CSV = REPORT_DIR / f"common_interval_bnb_refresh_monthly_agg_{STAMP}.csv"
MONTHLY_COIN_CSV = REPORT_DIR / f"common_interval_bnb_refresh_monthly_coin_{STAMP}.csv"
YEARLY_AGG_CSV = REPORT_DIR / f"common_interval_bnb_refresh_yearly_agg_{STAMP}.csv"
YEARLY_COIN_CSV = REPORT_DIR / f"common_interval_bnb_refresh_yearly_coin_{STAMP}.csv"
MARGIN_CSV = REPORT_DIR / f"common_interval_bnb_refresh_margin_{STAMP}.csv"
LOSS_MONTHS_CSV = REPORT_DIR / f"common_interval_bnb_refresh_loss_months_{STAMP}.csv"

INITIAL_CAPITAL = Decimal("10000")
RISK_PER_TRADE_U = Decimal("10")

ATR_PERIOD = 14
ATR_PERCENTILE_LOOKBACK = 100
ATR_PERCENTILE_MAX = 0.50
SLOPE_THRESHOLD_RATIO = -0.0005
BREAKDOWN_ATR_MULT = 0.2
RETEST_ATR_MULT = 0.3
STOP_BUFFER_ATR_MULT = 0.3
WATCH_BARS = 6
BODY_RECLAIM_MAX_RATIO = 0.5
TAKER_FEE_RATE = 0.00036
BNB_BODY_ATR_LIMIT = 1.0


@dataclass(frozen=True)
class GenericShortProfile:
    symbol: str
    coin: str
    strategy_key: str
    strategy_label: str
    ma_type: str
    period: int
    daily_filter_key: str
    daily_filter_label: str
    slope_threshold_ratio: Decimal
    atr_period: int
    atr_stop_multiplier: Decimal
    atr_percentile_max: Decimal
    exit_model: str


def main() -> None:
    client = OkxRestClient()
    generic_short_profiles = load_generic_short_profiles()
    data_ranges, common_start_ts, common_end_ts = load_data_ranges()

    trade_frames: list[pd.DataFrame] = []
    bnb_replace_compare_rows: list[dict[str, object]] = []
    param_rows: list[dict[str, object]] = []

    for symbol in SYMBOLS:
        entry_candles = [c for c in load_candle_cache(symbol, "1H", limit=None) if c.confirmed]
        daily_candles = [c for c in load_candle_cache(symbol, "1D", limit=None) if c.confirmed]
        instrument = client.get_instrument(symbol)

        long_trades = run_long_trades(
            symbol=symbol,
            entry_candles=entry_candles,
            daily_candles=daily_candles,
            instrument=instrument,
            gate=LONG_GATES[symbol],
        )
        long_trades = filter_scope(long_trades, start_ts=common_start_ts, end_ts=common_end_ts)

        if symbol == "BNB-USDT-SWAP":
            old_short = run_short_trades(
                symbol=symbol,
                entry_candles=entry_candles,
                daily_candles=daily_candles,
                profile=generic_short_profiles[symbol],
            )
            old_short = filter_scope(old_short, start_ts=common_start_ts, end_ts=common_end_ts)
            new_short = run_bnb_bodyatr_short(entry_candles=entry_candles, daily_candles=daily_candles)
            new_short = filter_scope(new_short, start_ts=common_start_ts, end_ts=common_end_ts)
            short_trades = new_short
            bnb_replace_compare_rows.extend(build_bnb_replace_rows(long_trades, old_short, new_short))
        else:
            short_trades = run_short_trades(
                symbol=symbol,
                entry_candles=entry_candles,
                daily_candles=daily_candles,
                profile=generic_short_profiles[symbol],
            )
            short_trades = filter_scope(short_trades, start_ts=common_start_ts, end_ts=common_end_ts)

        trade_frames.extend([long_trades, short_trades])
        param_rows.extend(build_symbol_param_rows(symbol, generic_short_profiles))

    trades = pd.concat(trade_frames, ignore_index=True).sort_values(["exit_ts", "entry_ts", "coin", "side"]).reset_index(drop=True)
    trades["entry_time"] = pd.to_datetime(trades["entry_ts"], unit="ms", utc=True)
    trades["exit_time"] = pd.to_datetime(trades["exit_ts"], unit="ms", utc=True)
    trades["period_month"] = trades["exit_time"].dt.strftime("%Y-%m")
    trades["period_year"] = trades["exit_time"].dt.strftime("%Y")

    summary = build_scope_summary_common(trades)
    monthly_agg = build_period_summary_common(trades, period_col="period_month", by_coin=False)
    monthly_coin = build_period_summary_common(trades, period_col="period_month", by_coin=True)
    yearly_agg = build_period_summary_common(trades, period_col="period_year", by_coin=False)
    yearly_coin = build_period_summary_common(trades, period_col="period_year", by_coin=True)
    loss_months = monthly_agg[
        (monthly_agg["coin"] == "ALL")
        & (monthly_agg["side"] == "combined")
        & (monthly_agg["total_pnl_u"] < 0)
    ].copy().sort_values("period")
    margin_table = build_leverage_table(trades)

    TRADES_CSV.write_text(trades.to_csv(index=False, encoding="utf-8-sig"), encoding="utf-8-sig")
    summary.to_csv(SUMMARY_CSV, index=False, encoding="utf-8-sig")
    monthly_agg.to_csv(MONTHLY_AGG_CSV, index=False, encoding="utf-8-sig")
    monthly_coin.to_csv(MONTHLY_COIN_CSV, index=False, encoding="utf-8-sig")
    yearly_agg.to_csv(YEARLY_AGG_CSV, index=False, encoding="utf-8-sig")
    yearly_coin.to_csv(YEARLY_COIN_CSV, index=False, encoding="utf-8-sig")
    margin_table.to_csv(MARGIN_CSV, index=False, encoding="utf-8-sig")
    loss_months.to_csv(LOSS_MONTHS_CSV, index=False, encoding="utf-8-sig")

    payload = build_payload(
        data_ranges=data_ranges,
        common_start_ts=common_start_ts,
        common_end_ts=common_end_ts,
        summary=summary,
        margin_table=margin_table,
        loss_months=loss_months,
        bnb_replace_compare_rows=bnb_replace_compare_rows,
        param_rows=param_rows,
    )
    JSON_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    HTML_PATH.write_text(
        build_html(
            trades=trades,
            summary=summary,
            monthly_agg=monthly_agg,
            monthly_coin=monthly_coin,
            yearly_agg=yearly_agg,
            yearly_coin=yearly_coin,
            margin_table=margin_table,
            loss_months=loss_months,
            data_ranges=data_ranges,
            common_start_ts=common_start_ts,
            common_end_ts=common_end_ts,
            param_rows=param_rows,
            bnb_replace_compare_rows=bnb_replace_compare_rows,
        ),
        encoding="utf-8",
    )
    print(HTML_PATH)


def load_generic_short_profiles() -> dict[str, GenericShortProfile]:
    profiles: dict[str, GenericShortProfile] = {}
    for item in load_recommendations():
        ma_type, period = parse_strategy_key(item.strategy_key)
        profiles[item.symbol] = GenericShortProfile(
            symbol=item.symbol,
            coin=item.coin,
            strategy_key=item.strategy_key,
            strategy_label=item.strategy_label,
            ma_type=ma_type,
            period=period,
            daily_filter_key=item.daily_filter_key,
            daily_filter_label=item.daily_filter_label,
            slope_threshold_ratio=Decimal("-0.0005"),
            atr_period=14,
            atr_stop_multiplier=Decimal("2"),
            atr_percentile_max=Decimal("0.5"),
            exit_model="2R保本后逐级锁盈",
        )
    return profiles


def load_data_ranges() -> tuple[dict[str, dict[str, object]], int, int]:
    data_ranges: dict[str, dict[str, object]] = {}
    starts: list[int] = []
    ends: list[int] = []
    for symbol in SYMBOLS:
        entry_candles = [c for c in load_candle_cache(symbol, "1H", limit=None) if c.confirmed]
        daily_candles = [c for c in load_candle_cache(symbol, "1D", limit=None) if c.confirmed]
        starts.append(entry_candles[0].ts)
        ends.append(entry_candles[-1].ts)
        data_ranges[symbol] = {
            "entry_candles": len(entry_candles),
            "daily_candles": len(daily_candles),
            "start_utc": format_ts(entry_candles[0].ts),
            "end_utc": format_ts(entry_candles[-1].ts),
        }
    return data_ranges, max(starts), min(ends)


def build_symbol_param_rows(symbol: str, generic_short_profiles: dict[str, GenericShortProfile]) -> list[dict[str, object]]:
    long_profile = LONG_PROFILES[symbol]
    long_gate = LONG_GATES[symbol]
    long_row = {
        "coin": SYMBOL_LABELS[symbol],
        "side": "long",
        "strategy_family": "dynamic_long",
        "base_strategy": "1H EMA动态委托做多",
        "fast_line": f"EMA{long_profile.ema_period}",
        "trend_line": f"EMA{long_profile.trend_ema_period}",
        "entry_reference": "跟随快线" if long_profile.entry_reference_ema_period <= 0 else f"EMA{long_profile.entry_reference_ema_period}",
        "daily_gate": long_gate.label,
        "atr_period": 10,
        "atr_stop": f"x{format_decimal_fixed(long_profile.atr_stop_multiplier, 1)}",
        "extra_filter": "每段趋势最多1次开仓",
        "entry_trigger": "回踩参考线挂单",
        "exit_model": "动态止盈 + 2R保本 + 手续费偏移",
    }
    if symbol == "BNB-USDT-SWAP":
        short_row = {
            "coin": "BNB",
            "side": "short",
            "strategy_family": "bodyatr_retest_short",
            "base_strategy": "1H MA20斜率破位回抽做空",
            "fast_line": "MA20",
            "trend_line": "-",
            "entry_reference": "回抽MA20近线做空",
            "daily_gate": "无日线均线过滤；弱日定义=日线收跌",
            "atr_period": ATR_PERIOD,
            "atr_stop": f"stop=当根高点+{STOP_BUFFER_ATR_MULT}ATR，且不少于0.5ATR",
            "extra_filter": f"slope<={SLOPE_THRESHOLD_RATIO}；ATR分位<={ATR_PERCENTILE_MAX}；body/ATR<={BNB_BODY_ATR_LIMIT}",
            "entry_trigger": f"破位后{WATCH_BARS}根内回抽；breakdown={BREAKDOWN_ATR_MULT}ATR；retest={RETEST_ATR_MULT}ATR；回收上限={BODY_RECLAIM_MAX_RATIO}",
            "exit_model": "2R保本后逐级锁盈",
        }
    else:
        profile = generic_short_profiles[symbol]
        short_row = {
            "coin": SYMBOL_LABELS[symbol],
            "side": "short",
            "strategy_family": "slope_short",
            "base_strategy": "1H 斜率做空",
            "fast_line": f"{profile.ma_type.upper()}{profile.period}",
            "trend_line": "-",
            "entry_reference": "收盘确认入场",
            "daily_gate": profile.daily_filter_label,
            "atr_period": profile.atr_period,
            "atr_stop": f"x{format_decimal_fixed(profile.atr_stop_multiplier, 1)}",
            "extra_filter": f"slope<={profile.slope_threshold_ratio}；ATR分位<={profile.atr_percentile_max}",
            "entry_trigger": "均线斜率转弱并满足波动过滤",
            "exit_model": profile.exit_model,
        }
    return [long_row, short_row]


def build_bnb_replace_rows(long_trades: pd.DataFrame, old_short: pd.DataFrame, new_short: pd.DataFrame) -> list[dict[str, object]]:
    rows = []
    views = {
        "BNB_long_only": long_trades,
        "BNB_old_short": old_short,
        "BNB_new_short": new_short,
        "BNB_old_combined": pd.concat([long_trades, old_short], ignore_index=True).sort_values(["exit_ts", "entry_ts"]),
        "BNB_new_combined": pd.concat([long_trades, new_short], ignore_index=True).sort_values(["exit_ts", "entry_ts"]),
    }
    for label, frame in views.items():
        metrics = metrics_from_frame(frame)
        rows.append(
            {
                "view": label,
                "trades": int(metrics["trades"]),
                "total_pnl_u": metrics["total_pnl_u"],
                "profit_factor": metrics["profit_factor"],
                "win_rate": metrics["win_rate"] * 100,
                "max_drawdown_u": metrics["max_drawdown_u"],
            }
        )
    old_metrics = metrics_from_frame(old_short)
    new_metrics = metrics_from_frame(new_short)
    old_combo = metrics_from_frame(views["BNB_old_combined"])
    new_combo = metrics_from_frame(views["BNB_new_combined"])
    rows.append(
        {
            "view": "BNB_delta_new_minus_old_short",
            "trades": int(new_metrics["trades"] - old_metrics["trades"]),
            "total_pnl_u": new_metrics["total_pnl_u"] - old_metrics["total_pnl_u"],
            "profit_factor": new_metrics["profit_factor"] - old_metrics["profit_factor"],
            "win_rate": (new_metrics["win_rate"] - old_metrics["win_rate"]) * 100,
            "max_drawdown_u": new_metrics["max_drawdown_u"] - old_metrics["max_drawdown_u"],
        }
    )
    rows.append(
        {
            "view": "BNB_delta_new_minus_old_combined",
            "trades": int(new_combo["trades"] - old_combo["trades"]),
            "total_pnl_u": new_combo["total_pnl_u"] - old_combo["total_pnl_u"],
            "profit_factor": new_combo["profit_factor"] - old_combo["profit_factor"],
            "win_rate": (new_combo["win_rate"] - old_combo["win_rate"]) * 100,
            "max_drawdown_u": new_combo["max_drawdown_u"] - old_combo["max_drawdown_u"],
        }
    )
    return rows


def build_scope_summary_common(trades: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for coin in [*sorted(trades["coin"].unique()), "ALL"]:
        base = trades if coin == "ALL" else trades[trades["coin"] == coin]
        for side in ("long", "short", "combined"):
            subset = base if side == "combined" else base[base["side"] == side]
            metrics = metrics_from_frame(subset)
            rows.append(
                {
                    "scope": "common",
                    "coin": coin,
                    "side": side,
                    "trades": int(metrics["trades"]),
                    "total_pnl_u": metrics["total_pnl_u"],
                    "profit_factor": metrics["profit_factor"],
                    "win_rate": metrics["win_rate"] * 100,
                    "avg_r": metrics["avg_r"],
                    "avg_hold_hours": metrics["avg_hold_hours"],
                    "max_drawdown_u": metrics["max_drawdown_u"],
                    "return_pct_on_10k": metrics["return_pct_on_10k"],
                }
            )
    return pd.DataFrame(rows)


def build_period_summary_common(trades: pd.DataFrame, *, period_col: str, by_coin: bool) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    coins = sorted(trades["coin"].unique()) if by_coin else ["ALL"]
    for period in sorted(trades[period_col].unique()):
        period_frame = trades[trades[period_col] == period]
        for coin in coins:
            base = period_frame if coin == "ALL" else period_frame[period_frame["coin"] == coin]
            for side in ("long", "short", "combined"):
                subset = base if side == "combined" else base[base["side"] == side]
                metrics = metrics_from_frame(subset)
                rows.append(
                    {
                        "scope": "common",
                        "period": period,
                        "coin": coin,
                        "side": side,
                        "trades": int(metrics["trades"]),
                        "total_pnl_u": metrics["total_pnl_u"],
                        "profit_factor": metrics["profit_factor"],
                        "win_rate": metrics["win_rate"] * 100,
                        "avg_r": metrics["avg_r"],
                        "avg_hold_hours": metrics["avg_hold_hours"],
                        "max_drawdown_u": metrics["max_drawdown_u"],
                        "return_pct_on_10k": metrics["return_pct_on_10k"],
                    }
                )
    return pd.DataFrame(rows)


def run_bnb_bodyatr_short(*, entry_candles, daily_candles) -> pd.DataFrame:
    frame = build_entry_frame(entry_candles)
    add_bnb_indicators(frame, ma_type="ma", period=20)
    daily_info = build_bnb_daily_info(entry_tss=frame["ts"].tolist(), daily_candles=daily_candles)
    trades = simulate_bnb_bodyatr_short_detailed(frame, daily_info=daily_info, body_atr_limit=BNB_BODY_ATR_LIMIT)
    if trades.empty:
        return pd.DataFrame(
            columns=[
                "symbol", "coin", "side", "strategy_family", "strategy_key", "strategy_label", "param_label",
                "entry_ts", "exit_ts", "entry_index", "exit_index", "entry_price", "exit_price", "qty",
                "notional_usdt", "risk_value_u", "pnl_u", "r_multiple", "hold_hours", "exit_reason",
                "daily_gate_key", "daily_gate_label",
            ]
        )
    trades["symbol"] = "BNB-USDT-SWAP"
    trades["coin"] = "BNB"
    trades["side"] = "short"
    trades["strategy_family"] = "bodyatr_retest_short"
    trades["strategy_key"] = "bnb_ma20_bodyatr_1_0"
    trades["strategy_label"] = "做空 | BNB MA20 Body/ATR回抽做空"
    trades["param_label"] = (
        "MA20回抽做空 | 无日线均线过滤(弱日=收跌) | "
        f"slope<={SLOPE_THRESHOLD_RATIO} | ATR14 | body/ATR<={BNB_BODY_ATR_LIMIT} | "
        f"breakdown={BREAKDOWN_ATR_MULT}ATR | retest={RETEST_ATR_MULT}ATR | stop+{STOP_BUFFER_ATR_MULT}ATR | "
        "2R后逐级锁盈"
    )
    trades["daily_gate_key"] = "weak_day_only"
    trades["daily_gate_label"] = "无日线均线过滤；弱日=日线收跌"
    return trades[
        [
            "symbol", "coin", "side", "strategy_family", "strategy_key", "strategy_label", "param_label",
            "entry_ts", "exit_ts", "entry_index", "exit_index", "entry_price", "exit_price", "qty",
            "notional_usdt", "risk_value_u", "pnl_u", "r_multiple", "hold_hours", "exit_reason",
            "daily_gate_key", "daily_gate_label",
        ]
    ].copy()


def build_entry_frame(candles: list[object]) -> pd.DataFrame:
    rows = [
        {
            "ts": int(c.ts),
            "timestamp": pd.to_datetime(int(c.ts), unit="ms", utc=True),
            "open": float(c.open),
            "high": float(c.high),
            "low": float(c.low),
            "close": float(c.close),
        }
        for c in candles
    ]
    return pd.DataFrame(rows).sort_values("timestamp").drop_duplicates("timestamp").reset_index(drop=True)


def add_bnb_indicators(df: pd.DataFrame, *, ma_type: str, period: int) -> None:
    if ma_type == "ema":
        df["ma20_line"] = df["close"].ewm(span=period, adjust=False, min_periods=period).mean()
    else:
        df["ma20_line"] = df["close"].rolling(period, min_periods=period).mean()
    prev_close = df["close"].shift(1)
    tr = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - prev_close).abs(),
            (df["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    df["atr14"] = tr.ewm(alpha=1 / ATR_PERIOD, adjust=False, min_periods=ATR_PERIOD).mean()
    df["atr_pct"] = rolling_percentile(df["atr14"], ATR_PERCENTILE_LOOKBACK)
    df["body_size"] = (df["close"] - df["open"]).abs()


def rolling_percentile(series: pd.Series, lookback: int) -> pd.Series:
    return series.rolling(lookback, min_periods=lookback).apply(lambda x: float(np.mean(x <= x[-1])), raw=True)


def build_bnb_daily_info(*, entry_tss: list[int], daily_candles: list[object]) -> list[dict[str, object]]:
    daily_frame = pd.DataFrame(
        {
            "ts": [int(c.ts) for c in daily_candles],
            "open": [float(c.open) for c in daily_candles],
            "close": [float(c.close) for c in daily_candles],
        }
    )
    daily_available_ts = closed_candle_available_timestamps(daily_candles)
    out: list[dict[str, object]] = []
    for ts in entry_tss:
        idx = np.searchsorted(daily_available_ts, int(ts), side="right") - 1
        if idx < 0 or idx >= len(daily_frame):
            out.append({"weak_day": False})
            continue
        day_open = float(daily_frame["open"].iloc[idx])
        day_close = float(daily_frame["close"].iloc[idx])
        out.append({"weak_day": bool(day_close < day_open)})
    return out


def simulate_bnb_bodyatr_short_detailed(frame: pd.DataFrame, *, daily_info: list[dict[str, object]], body_atr_limit: float) -> pd.DataFrame:
    open_values = frame["open"].to_numpy(dtype=float)
    high_values = frame["high"].to_numpy(dtype=float)
    low_values = frame["low"].to_numpy(dtype=float)
    close_values = frame["close"].to_numpy(dtype=float)
    ts_values = frame["ts"].to_numpy(dtype=np.int64)
    line_values = frame["ma20_line"].to_numpy(dtype=float)
    atr_values = frame["atr14"].to_numpy(dtype=float)
    atr_pct_values = frame["atr_pct"].to_numpy(dtype=float)
    body_values = frame["body_size"].to_numpy(dtype=float)
    trades: list[dict[str, object]] = []
    position = None
    pending = None
    start_index = max(ATR_PERCENTILE_LOOKBACK, 60)
    for index in range(start_index, len(frame)):
        line_value = line_values[index]
        prev_line = line_values[index - 1]
        atr_value = atr_values[index]
        atr_pct = atr_pct_values[index]
        candle_open = open_values[index]
        candle_high = high_values[index]
        candle_low = low_values[index]
        candle_close = close_values[index]
        body_size = body_values[index]
        if any(math.isnan(v) for v in [line_value, prev_line, atr_value, atr_pct, body_size]):
            continue
        slope_ratio = (line_value - prev_line) / line_value if line_value else math.nan
        if position is not None:
            exited = process_bnb_open_short(
                position=position,
                candle_open=candle_open,
                candle_high=candle_high,
                candle_low=candle_low,
                candle_close=candle_close,
                candle_ts=int(ts_values[index]),
                index=index,
                trades=trades,
            )
            if exited:
                position = None
        if position is not None:
            continue
        if pending is not None:
            age = index - int(pending["index"])
            if age > WATCH_BARS:
                pending = None
            else:
                near_line = candle_high >= (line_value - RETEST_ATR_MULT * atr_value)
                still_below = candle_close < line_value
                bearish_close = candle_close < candle_open
                midpoint_ok = candle_close <= float(pending["max_reclaim_close"])
                weak_day_ok = bool(daily_info[index]["weak_day"])
                if near_line and still_below and bearish_close and midpoint_ok and weak_day_ok:
                    risk_per_unit = max((candle_high + STOP_BUFFER_ATR_MULT * atr_value) - candle_close, atr_value * 0.5)
                    if risk_per_unit > 0 and np.isfinite(risk_per_unit):
                        fee_offset = candle_close * TAKER_FEE_RATE * 2.0
                        position = {
                            "entry_index": index,
                            "entry_ts": int(ts_values[index]),
                            "entry_price": candle_close,
                            "risk_per_unit": risk_per_unit,
                            "stop": candle_close + risk_per_unit,
                            "stop_reason": "stop_loss",
                            "fee_offset": fee_offset,
                            "next_dynamic_r": 2.0,
                        }
                        pending = None
                        continue
        if pending is not None:
            continue
        if not np.isfinite(slope_ratio) or slope_ratio > SLOPE_THRESHOLD_RATIO or atr_pct > ATR_PERCENTILE_MAX:
            continue
        if candle_close >= line_value - BREAKDOWN_ATR_MULT * atr_value or candle_close >= candle_open:
            continue
        if not bool(daily_info[index]["weak_day"]):
            continue
        if atr_value <= 0 or (body_size / atr_value) > body_atr_limit:
            continue
        # pending is only created after a bearish breakdown candle, so
        # candle_open > candle_close here. This value is intentionally a
        # reclaim cap inside the bearish body, not a fixed midpoint.
        body_mid = candle_close + (candle_open - candle_close) * BODY_RECLAIM_MAX_RATIO
        pending = {"index": index, "max_reclaim_close": body_mid}
    return pd.DataFrame(trades)


def process_bnb_open_short(*, position, candle_open, candle_high, candle_low, candle_close, candle_ts, index, trades) -> bool:
    path = (candle_open, candle_low, candle_high, candle_close) if candle_close >= candle_open else (candle_open, candle_high, candle_low, candle_close)
    for start, end in zip(path, path[1:]):
        if end > start:
            stop_price = float(position["stop"])
            if start <= stop_price <= end:
                trades.append(close_bnb_trade(position, index, candle_ts, stop_price, str(position["stop_reason"])))
                return True
        else:
            advance_step_dynamic(position, end)
    return False


def advance_step_dynamic(position, favorable_price: float) -> None:
    entry = float(position["entry_price"])
    risk = float(position["risk_per_unit"])
    fee_offset = float(position["fee_offset"])
    while True:
        next_r = float(position["next_dynamic_r"])
        trigger = entry - risk * next_r - fee_offset
        if favorable_price > trigger:
            break
        locked_r = 0.0 if math.isclose(next_r, 2.0) else max(next_r - 1.0, 0.0)
        reason = "break_even_stop" if math.isclose(next_r, 2.0) else f"locked_{int(round(locked_r))}r_stop"
        candidate_stop = entry - risk * locked_r - fee_offset
        if candidate_stop < float(position["stop"]):
            position["stop"] = candidate_stop
            position["stop_reason"] = reason
        position["next_dynamic_r"] = next_r + 1.0


def close_bnb_trade(position, exit_index: int, exit_ts: int, exit_price: float, exit_reason: str) -> dict[str, object]:
    entry = float(position["entry_price"])
    risk = float(position["risk_per_unit"])
    qty = float(RISK_PER_TRADE_U) / risk if risk > 0 else 0.0
    pnl_per_unit = (entry - exit_price) - TAKER_FEE_RATE * (entry + exit_price)
    pnl_u = pnl_per_unit * qty
    return {
        "entry_index": int(position["entry_index"]),
        "exit_index": int(exit_index),
        "entry_ts": int(position["entry_ts"]),
        "exit_ts": int(exit_ts),
        "entry_price": entry,
        "exit_price": float(exit_price),
        "qty": qty,
        "notional_usdt": qty * entry,
        "risk_value_u": float(RISK_PER_TRADE_U),
        "pnl_u": pnl_u,
        "r_multiple": pnl_u / float(RISK_PER_TRADE_U),
        "hold_hours": float((exit_ts - int(position["entry_ts"])) / (1000 * 3600)),
        "exit_reason": exit_reason,
    }


def build_payload(
    *,
    data_ranges: dict[str, dict[str, object]],
    common_start_ts: int,
    common_end_ts: int,
    summary: pd.DataFrame,
    margin_table: pd.DataFrame,
    loss_months: pd.DataFrame,
    bnb_replace_compare_rows: list[dict[str, object]],
    param_rows: list[dict[str, object]],
) -> dict[str, object]:
    combined = pick_summary(summary, "ALL", "combined")
    long_row = pick_summary(summary, "ALL", "long")
    short_row = pick_summary(summary, "ALL", "short")
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "scope": "common_interval_only",
        "risk_per_trade_u": str(RISK_PER_TRADE_U),
        "initial_capital_u": str(INITIAL_CAPITAL),
        "common_interval": {
            "start_utc": format_ts(common_start_ts),
            "end_utc": format_ts(common_end_ts),
        },
        "headline": {
            "combined_pnl_u": combined["total_pnl_u"],
            "long_pnl_u": long_row["total_pnl_u"],
            "short_pnl_u": short_row["total_pnl_u"],
            "final_equity_u": float(INITIAL_CAPITAL) + float(combined["total_pnl_u"]),
            "negative_months_count": int(len(loss_months)),
        },
        "data_ranges": data_ranges,
        "summary": summary.to_dict("records"),
        "margin_table": margin_table.to_dict("records"),
        "loss_months": loss_months.to_dict("records"),
        "bnb_replace_compare": bnb_replace_compare_rows,
        "parameter_rows": param_rows,
    }


def build_html(
    *,
    trades: pd.DataFrame,
    summary: pd.DataFrame,
    monthly_agg: pd.DataFrame,
    monthly_coin: pd.DataFrame,
    yearly_agg: pd.DataFrame,
    yearly_coin: pd.DataFrame,
    margin_table: pd.DataFrame,
    loss_months: pd.DataFrame,
    data_ranges: dict[str, dict[str, object]],
    common_start_ts: int,
    common_end_ts: int,
    param_rows: list[dict[str, object]],
    bnb_replace_compare_rows: list[dict[str, object]],
) -> str:
    total = pick_summary(summary, "ALL", "combined")
    long_row = pick_summary(summary, "ALL", "long")
    short_row = pick_summary(summary, "ALL", "short")
    final_equity = float(INITIAL_CAPITAL) + float(total["total_pnl_u"])
    concurrent = concurrent_profile(trades)
    max_notional = float(concurrent["total_notional_usdt"].max()) if not concurrent.empty else 0.0
    max_positions = int(concurrent["open_positions"].max()) if not concurrent.empty else 0
    worst_month = loss_months.iloc[loss_months["total_pnl_u"].argmin()] if not loss_months.empty else None
    bnb_delta = next((row for row in bnb_replace_compare_rows if row["view"] == "BNB_delta_new_minus_old_combined"), None)

    equity_chart = figure_to_base64(build_equity_chart(trades, "五币公共区间综合资金曲线"))
    monthly_bar = figure_to_base64(build_monthly_pnl_bar(monthly_agg))
    concurrent_chart = figure_to_base64(build_concurrent_chart(concurrent, "公共区间并发名义价值"))
    side_curve_chart = figure_to_base64(build_side_curve_chart(trades))

    summary_view = summary.copy()
    summary_view["win_rate"] = summary_view["win_rate"].astype(float)
    monthly_agg_view = monthly_agg.copy()
    monthly_coin_view = monthly_coin.copy()
    yearly_agg_view = yearly_agg.copy()
    yearly_coin_view = yearly_coin.copy()

    param_table = dataframe_to_html(
        pd.DataFrame(param_rows),
        float_cols=None,
    )
    summary_table = dataframe_to_html(
        summary_view[["coin", "side", "trades", "total_pnl_u", "profit_factor", "win_rate", "avg_r", "avg_hold_hours", "max_drawdown_u", "return_pct_on_10k"]],
        float_cols={"total_pnl_u": 2, "profit_factor": 3, "win_rate": 2, "avg_r": 3, "avg_hold_hours": 1, "max_drawdown_u": 2, "return_pct_on_10k": 2},
    )
    monthly_agg_table = dataframe_to_html(
        monthly_agg_view,
        float_cols={"total_pnl_u": 2, "profit_factor": 3, "win_rate": 2, "avg_r": 3, "avg_hold_hours": 1, "max_drawdown_u": 2, "return_pct_on_10k": 2},
    )
    monthly_coin_table = dataframe_to_html(
        monthly_coin_view,
        float_cols={"total_pnl_u": 2, "profit_factor": 3, "win_rate": 2, "avg_r": 3, "avg_hold_hours": 1, "max_drawdown_u": 2, "return_pct_on_10k": 2},
    )
    yearly_agg_table = dataframe_to_html(
        yearly_agg_view,
        float_cols={"total_pnl_u": 2, "profit_factor": 3, "win_rate": 2, "avg_r": 3, "avg_hold_hours": 1, "max_drawdown_u": 2, "return_pct_on_10k": 2},
    )
    yearly_coin_table = dataframe_to_html(
        yearly_coin_view,
        float_cols={"total_pnl_u": 2, "profit_factor": 3, "win_rate": 2, "avg_r": 3, "avg_hold_hours": 1, "max_drawdown_u": 2, "return_pct_on_10k": 2},
    )
    loss_months_table = dataframe_to_html(
        loss_months if not loss_months.empty else pd.DataFrame([{"period": "none", "coin": "ALL", "side": "combined", "trades": 0, "total_pnl_u": 0.0, "profit_factor": 0.0, "win_rate": 0.0, "avg_r": 0.0, "avg_hold_hours": 0.0, "max_drawdown_u": 0.0, "return_pct_on_10k": 0.0}]),
        float_cols={"total_pnl_u": 2, "profit_factor": 3, "win_rate": 2, "avg_r": 3, "avg_hold_hours": 1, "max_drawdown_u": 2, "return_pct_on_10k": 2},
    )
    margin_table_html = dataframe_to_html(
        margin_table,
        float_cols={
            "historical_max_margin_usdt": 2,
            "historical_max_margin_plus30pct_usdt": 2,
            "conservative_upper_margin_usdt": 2,
            "conservative_upper_plus30pct_usdt": 2,
        },
    )
    bnb_compare_table = dataframe_to_html(
        pd.DataFrame(bnb_replace_compare_rows),
        float_cols={"total_pnl_u": 2, "profit_factor": 3, "win_rate": 2, "max_drawdown_u": 2},
    )
    ranges_table = dataframe_to_html(
        pd.DataFrame(
            [
                {
                    "coin": SYMBOL_LABELS[symbol],
                    "start_utc": data_ranges[symbol]["start_utc"],
                    "end_utc": data_ranges[symbol]["end_utc"],
                    "entry_candles": data_ranges[symbol]["entry_candles"],
                    "daily_candles": data_ranges[symbol]["daily_candles"],
                }
                for symbol in SYMBOLS
            ]
        ),
        float_cols=None,
    )

    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <title>公共区间五币组合领导报告-BNB参数更新版</title>
  <style>
    :root {{
      --bg:#f5f7fb; --panel:#ffffff; --ink:#17233a; --muted:#62748b; --line:#d9e1ec;
      --brand:#123a64; --brand2:#0f766e; --green:#166534; --red:#b42318; --amber:#b45309;
    }}
    * {{ box-sizing:border-box; }}
    body {{ margin:0; font-family:"Microsoft YaHei","Segoe UI",sans-serif; background:var(--bg); color:var(--ink); }}
    .wrap {{ max-width:1600px; margin:0 auto; padding:28px; }}
    .hero {{
      background:linear-gradient(135deg,#102033 0%,#1a4674 52%,#0f766e 100%);
      color:#fff; border-radius:26px; padding:30px 34px; box-shadow:0 20px 48px rgba(15,23,42,.20);
    }}
    .hero h1 {{ margin:0 0 12px; font-size:34px; }}
    .hero p {{ margin:8px 0; line-height:1.8; color:rgba(255,255,255,.93); }}
    .meta {{ display:flex; flex-wrap:wrap; gap:12px; margin-top:18px; }}
    .chip {{ background:rgba(255,255,255,.12); border:1px solid rgba(255,255,255,.18); border-radius:999px; padding:8px 12px; font-size:13px; }}
    .grid {{ display:grid; grid-template-columns:repeat(5,minmax(0,1fr)); gap:16px; margin:22px 0 8px; }}
    .card, .section {{
      background:var(--panel); border:1px solid var(--line); border-radius:22px; box-shadow:0 10px 22px rgba(15,23,42,.05);
    }}
    .card {{ padding:18px; }}
    .card h3 {{ margin:0 0 10px; font-size:16px; }}
    .card p {{ margin:0; color:var(--muted); line-height:1.75; }}
    .section {{ margin-top:22px; padding:24px; }}
    .section h2 {{ margin:0 0 14px; font-size:24px; }}
    .section p, .section li {{ color:var(--muted); line-height:1.8; }}
    .twocol {{ display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:18px; }}
    .chart {{ background:#fbfdff; border:1px solid var(--line); border-radius:18px; padding:16px; }}
    .chart img {{ width:100%; display:block; border-radius:12px; }}
    .table-wrap {{ max-height:560px; overflow:auto; border:1px solid var(--line); border-radius:16px; background:#fff; }}
    table {{ width:100%; border-collapse:collapse; font-size:13px; }}
    th, td {{ padding:9px 10px; border-bottom:1px solid var(--line); text-align:right; white-space:nowrap; }}
    th:first-child, td:first-child {{ text-align:left; }}
    thead th {{ color:var(--muted); font-weight:700; background:#f8fbff; position:sticky; top:0; }}
    .note {{ margin-top:14px; padding:14px 16px; border-left:4px solid var(--brand); background:#eef4ff; border-radius:14px; color:#274064; }}
    .good {{ color:var(--green); font-weight:700; }}
    .bad {{ color:var(--red); font-weight:700; }}
    @media (max-width:1200px) {{ .grid, .twocol {{ grid-template-columns:1fr; }} }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <h1>五币公共区间领导报告：BNB新参数并入版</h1>
      <p>这版只看五个币都能同时覆盖的公共区间，把现有五币组合保留为主框架，只更新 <strong>BNB 空头</strong> 为今天新研究出的 <strong>Body/ATR 回抽做空参数</strong>。统一按 <strong>每笔固定风险 10U</strong> 回放，多空都做，并把综合亏损月份、资金曲线、保证金估算和全部明细参数一起列出。</p>
      <p>本报告适合直接给领导过目：先看总收益和保证金，再看 BNB 替换效果，最后下钻到年度、月度和分币种细节。</p>
      <div class="meta">
        <div class="chip">公共区间：{format_ts(common_start_ts)} -> {format_ts(common_end_ts)}</div>
        <div class="chip">风险口径：每笔 10U</div>
        <div class="chip">初始资金展示：10000U</div>
        <div class="chip">BNB新空头：MA20 + Body/ATR 1.0 + 弱日过滤</div>
        <div class="chip">输出文件：{html.escape(str(HTML_PATH))}</div>
      </div>
    </section>

    <section class="grid">
      <div class="card"><h3>综合最终盈利</h3><p>{fmt2(total["total_pnl_u"])}U<br>期末约 {fmt2(final_equity)}U</p></div>
      <div class="card"><h3>做多 / 做空</h3><p>Long {fmt2(long_row["total_pnl_u"])}U<br>Short {fmt2(short_row["total_pnl_u"])}U</p></div>
      <div class="card"><h3>综合回撤与PF</h3><p>Max DD {fmt2(total["max_drawdown_u"])}U<br>PF {float(total["profit_factor"]):.3f}</p></div>
      <div class="card"><h3>综合亏损月份</h3><p>{len(loss_months)} 个{f"<br>最差月份 {worst_month['period']} / {fmt2(worst_month['total_pnl_u'])}U" if worst_month is not None else "<br>无综合亏损月份"}</p></div>
      <div class="card"><h3>保证金粗估</h3><p>峰值名义 {fmt2(max_notional)}U<br>最大同时持仓 {max_positions} 笔</p></div>
    </section>

    <section class="section">
      <h2>本次更新重点</h2>
      <ul>
        <li>原组合里的 BNB 空头是弱项，旧版公共区间表现偏弱；这次只替换这一块，其余四个币和五个币做多参数保持不动。</li>
        <li>BNB 新空头参数来自今天的稳健性验证专题，walk-forward 选出的核心参数是 <strong>body_atr_limit = 1.0</strong>。</li>
        <li>如果领导更关心“同时跑五个币”的真实组合表现，这份公共区间口径比全量混合口径更直观。</li>
      </ul>
      <div class="note">
        {f"BNB 新参数并入后，BNB 综合收益相对旧版提升 <strong>{fmt2(bnb_delta['total_pnl_u'])}U</strong>，综合 PF 变化 {float(bnb_delta['profit_factor']):+.3f}，综合回撤变化 {fmt2(bnb_delta['max_drawdown_u'])}U。" if bnb_delta else ""}
      </div>
    </section>

    <section class="section">
      <h2>详细参数总表</h2>
      <div class="table-wrap">{param_table}</div>
      <div class="note">
        做多侧表里把每个币的快线、趋势线、挂单参考线、日线闸门和 ATR 止损倍率都单独展开了。做空侧除了老的斜率参数，也把 BNB 新方案里的 Body/ATR、回抽窗口、弱日定义、止损缓冲等细节展开，便于管理层确认“到底换了什么”。 
      </div>
    </section>

    <section class="section">
      <h2>BNB 替换效果对比</h2>
      <div class="table-wrap">{bnb_compare_table}</div>
      <div class="note">
        这张表把 BNB 的 long-only、旧 short、新 short、旧 combined、新 combined 全都放在一起。最关键看的就是 <strong>BNB_new_combined</strong> 和 <strong>BNB_old_combined</strong> 的差异。
      </div>
    </section>

    <section class="section">
      <h2>综合资金曲线与保证金曲线</h2>
      <div class="twocol">
        <div class="chart"><img src="data:image/png;base64,{equity_chart}" alt="综合资金曲线" /></div>
        <div class="chart"><img src="data:image/png;base64,{side_curve_chart}" alt="多空累计曲线" /></div>
      </div>
      <div class="twocol" style="margin-top:18px;">
        <div class="chart"><img src="data:image/png;base64,{monthly_bar}" alt="月度盈亏柱状图" /></div>
        <div class="chart"><img src="data:image/png;base64,{concurrent_chart}" alt="并发名义价值曲线" /></div>
      </div>
    </section>

    <section class="section">
      <h2>公共区间综合汇总</h2>
      <div class="table-wrap">{summary_table}</div>
    </section>

    <section class="section">
      <h2>综合亏损月份</h2>
      <div class="table-wrap">{loss_months_table}</div>
      <div class="note">
        这里只列组合口径 <strong>ALL + combined</strong> 的负收益月份。如果领导要追责到币种层，这些月份可以继续去看下面的“月度分币种表”。
      </div>
    </section>

    <section class="section">
      <h2>年度综合表</h2>
      <div class="table-wrap">{yearly_agg_table}</div>
    </section>

    <section class="section">
      <h2>年度分币种表</h2>
      <div class="table-wrap">{yearly_coin_table}</div>
    </section>

    <section class="section">
      <h2>月度综合表</h2>
      <div class="table-wrap">{monthly_agg_table}</div>
    </section>

    <section class="section">
      <h2>月度分币种表</h2>
      <div class="table-wrap">{monthly_coin_table}</div>
    </section>

    <section class="section">
      <h2>保证金估算</h2>
      <p>这里的保证金是简化估算：先按每笔交易的入场名义金额，统计公共区间历史上同时开仓时的名义总额峰值，再折算成不同杠杆下大概需要准备多少保证金。</p>
      <ul>
        <li>历史峰值并发名义：{fmt2(max_notional)}U</li>
        <li>历史最大同时持仓数：{max_positions} 笔</li>
        <li>建议优先看 <strong>historical_max_margin_plus30pct_usdt</strong>，这是“历史峰值 + 30% 缓冲”口径。</li>
      </ul>
      <div class="table-wrap">{margin_table_html}</div>
    </section>

    <section class="section">
      <h2>样本区间说明</h2>
      <div class="table-wrap">{ranges_table}</div>
      <div class="note">
        虽然本报告只统计公共区间，但日线过滤与长侧趋势识别仍然会利用更早历史做均线预热，避免因为区间起点太近导致前几天均线失真。
      </div>
    </section>
  </div>
</body>
</html>"""


def build_monthly_pnl_bar(monthly_agg: pd.DataFrame):
    frame = monthly_agg[(monthly_agg["coin"] == "ALL") & (monthly_agg["side"] == "combined")].copy()
    colors = ["#15803d" if v >= 0 else "#b42318" for v in frame["total_pnl_u"]]
    fig, ax = plt.subplots(figsize=(11, 4.8))
    ax.bar(frame["period"], frame["total_pnl_u"], color=colors)
    ax.set_title("五币公共区间综合月度盈亏", fontsize=14, pad=12)
    ax.set_ylabel("U")
    ax.tick_params(axis="x", rotation=65)
    ax.grid(axis="y", alpha=0.25)
    fig.tight_layout()
    return fig


def build_side_curve_chart(trades: pd.DataFrame):
    fig, ax = plt.subplots(figsize=(10, 4.8))
    for side, color, label in [
        ("long", "#1d4ed8", "Long"),
        ("short", "#b45309", "Short"),
        ("combined", "#0f766e", "Combined"),
    ]:
        if side == "combined":
            frame = trades.sort_values("exit_ts").copy()
        else:
            frame = trades[trades["side"] == side].sort_values("exit_ts").copy()
        if frame.empty:
            continue
        frame["time"] = pd.to_datetime(frame["exit_ts"], unit="ms", utc=True)
        frame["equity"] = float(INITIAL_CAPITAL) + frame["pnl_u"].astype(float).cumsum()
        ax.plot(frame["time"], frame["equity"], color=color, linewidth=1.4, label=label)
    ax.set_title("多头 / 空头 / 综合 累计资金曲线", fontsize=14, pad=12)
    ax.set_ylabel("U")
    ax.legend()
    ax.grid(alpha=0.22)
    fig.tight_layout()
    return fig


def pick_summary(frame: pd.DataFrame, coin: str, side: str) -> pd.Series:
    return frame[(frame["coin"] == coin) & (frame["side"] == side)].iloc[0]


if __name__ == "__main__":
    main()
