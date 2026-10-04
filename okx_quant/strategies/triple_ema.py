from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Literal

import pandas as pd

from okx_quant.indicators import atr, ema
from okx_quant.models import Candle, SignalDecision, StrategyConfig
from okx_quant.pricing import format_strategy_reason_price


TREND_UP = "UPTREND"
TREND_DOWN = "DOWNTREND"
TREND_SIDEWAY = "SIDEWAY"


@dataclass(frozen=True)
class TripleTrendSnapshot:
    timestamp: pd.Timestamp
    close: float
    ema_fast: float
    ema_mid: float
    ema_slow: float
    trend_state: str
    previous_trend_state: str


@dataclass(frozen=True)
class TripleTrendSignal:
    action: str
    reason: str
    trend_state: str
    timestamp: pd.Timestamp


@dataclass(frozen=True)
class LiveTripleEmaSnapshot:
    candle: Candle
    previous_state: str
    trend_state: str
    fast_ema: Decimal
    middle_ema: Decimal
    slow_ema: Decimal
    atr_value: Decimal | None


def calculate_ema(series: pd.Series, period: int) -> pd.Series:
    """计算指数移动平均线 EMA。"""
    period = max(int(period), 1)
    return series.astype(float).ewm(span=period, adjust=False).mean()


def calculate_triple_ema(
    bars: pd.DataFrame,
    fast_period: int,
    middle_period: int,
    slow_period: int,
) -> pd.DataFrame:
    """计算三条 EMA、趋势状态及趋势刚形成时的入场标记。"""
    if "close" not in bars.columns:
        raise ValueError("bars 必须包含 close 列")

    if min(fast_period, middle_period, slow_period) <= 0:
        raise ValueError("EMA 周期必须大于 0")

    df = bars.copy()
    df["ema_fast"] = calculate_ema(df["close"], fast_period)
    df["ema_mid"] = calculate_ema(df["close"], middle_period)
    df["ema_slow"] = calculate_ema(df["close"], slow_period)

    # 默认是震荡状态。
    df["trend_state"] = TREND_SIDEWAY

    up_condition = (df["ema_fast"] > df["ema_mid"]) & (df["ema_mid"] > df["ema_slow"])
    down_condition = (df["ema_fast"] < df["ema_mid"]) & (df["ema_mid"] < df["ema_slow"])
    df.loc[up_condition, "trend_state"] = TREND_UP
    df.loc[down_condition, "trend_state"] = TREND_DOWN

    df["previous_trend_state"] = df["trend_state"].shift(1).fillna(TREND_SIDEWAY)
    df["long_entry"] = (df["trend_state"] == TREND_UP) & (df["previous_trend_state"] != TREND_UP)
    df["short_entry"] = (df["trend_state"] == TREND_DOWN) & (df["previous_trend_state"] != TREND_DOWN)
    return df


def get_latest_snapshot(signal_frame: pd.DataFrame) -> TripleTrendSnapshot:
    """获取最新一根 K 线的三均线状态。"""
    if signal_frame.empty:
        raise ValueError("signal_frame 为空")

    row = signal_frame.iloc[-1]
    return TripleTrendSnapshot(
        timestamp=pd.Timestamp(signal_frame.index[-1]),
        close=float(row["close"]),
        ema_fast=float(row["ema_fast"]),
        ema_mid=float(row["ema_mid"]),
        ema_slow=float(row["ema_slow"]),
        trend_state=str(row["trend_state"]),
        previous_trend_state=str(row["previous_trend_state"]),
    )


def generate_signal(
    signal_frame: pd.DataFrame,
    *,
    position: str | None = None,
    allow_long: bool = True,
    allow_short: bool = True,
) -> TripleTrendSignal | None:
    """按最新趋势及当前仓位生成开仓、退出或反手信号。"""
    if signal_frame.empty:
        return None

    row = signal_frame.iloc[-1]
    timestamp = pd.Timestamp(signal_frame.index[-1])
    trend_state = str(row["trend_state"])
    previous_state = str(row["previous_trend_state"])

    if position is None:
        if allow_long and trend_state == TREND_UP and previous_state != TREND_UP:
            return TripleTrendSignal("buy", "triple_ema_uptrend", trend_state, timestamp)
        if allow_short and trend_state == TREND_DOWN and previous_state != TREND_DOWN:
            return TripleTrendSignal("sell", "triple_ema_downtrend", trend_state, timestamp)
        return None

    if position == "long":
        if trend_state == TREND_SIDEWAY:
            return TripleTrendSignal("exit", "triple_ema_sideway_exit", trend_state, timestamp)
        if trend_state == TREND_DOWN:
            if allow_short:
                return TripleTrendSignal("reverse_to_short", "triple_ema_reverse_to_short", trend_state, timestamp)
            return TripleTrendSignal("exit", "triple_ema_downtrend_exit", trend_state, timestamp)
        return None

    if position == "short":
        if trend_state == TREND_SIDEWAY:
            return TripleTrendSignal("exit", "triple_ema_sideway_exit", trend_state, timestamp)
        if trend_state == TREND_UP:
            if allow_long:
                return TripleTrendSignal("reverse_to_long", "triple_ema_reverse_to_long", trend_state, timestamp)
            return TripleTrendSignal("exit", "triple_ema_uptrend_exit", trend_state, timestamp)
        return None

    raise ValueError("position 只能是 None、long 或 short")


def evaluate_triple_ema_signal(
    candles: list[Candle],
    config: StrategyConfig,
    *,
    price_increment: Decimal | None = None,
) -> SignalDecision:
    """为实盘和信号监控生成已收盘 K 线上的首次趋势入场信号。"""
    snapshot = latest_live_snapshot(candles, config)
    if snapshot is None:
        minimum = max(
            int(config.ema_period),
            int(config.trend_ema_period),
            int(config.big_ema_period),
            int(config.atr_period) if config.atr_stop_multiplier > 0 else 0,
        ) + 1
        return SignalDecision(
            signal=None,
            reason=f"已收盘 K 线不足，至少需要 {minimum} 根",
            candle_ts=None,
            entry_reference=None,
            atr_value=None,
            ema_value=None,
            signal_candle_high=None,
            signal_candle_low=None,
        )

    def px(value: Decimal) -> str:
        return format_strategy_reason_price(value, price_increment)

    trend_text = (
        f"快EMA{config.ema_period}={px(snapshot.fast_ema)} / "
        f"中EMA{config.trend_ema_period}={px(snapshot.middle_ema)} / "
        f"慢EMA{config.big_ema_period}={px(snapshot.slow_ema)}"
    )
    if (
        snapshot.trend_state == TREND_UP
        and snapshot.previous_state != TREND_UP
        and config.signal_mode != "short_only"
    ):
        return SignalDecision(
            signal="long",
            reason=f"三均线首次形成上升趋势，{trend_text}。",
            candle_ts=snapshot.candle.ts,
            entry_reference=snapshot.candle.close,
            atr_value=snapshot.atr_value,
            ema_value=snapshot.slow_ema,
            signal_candle_high=snapshot.candle.high,
            signal_candle_low=snapshot.candle.low,
        )
    if (
        snapshot.trend_state == TREND_DOWN
        and snapshot.previous_state != TREND_DOWN
        and config.signal_mode != "long_only"
    ):
        return SignalDecision(
            signal="short",
            reason=f"三均线首次形成下降趋势，{trend_text}。",
            candle_ts=snapshot.candle.ts,
            entry_reference=snapshot.candle.close,
            atr_value=snapshot.atr_value,
            ema_value=snapshot.slow_ema,
            signal_candle_high=snapshot.candle.high,
            signal_candle_low=snapshot.candle.low,
        )
    return SignalDecision(
        signal=None,
        reason=f"当前趋势={snapshot.trend_state}，{trend_text}。",
        candle_ts=snapshot.candle.ts,
        entry_reference=None,
        atr_value=snapshot.atr_value,
        ema_value=snapshot.slow_ema,
        signal_candle_high=snapshot.candle.high,
        signal_candle_low=snapshot.candle.low,
    )


def live_position_action(
    candles: list[Candle],
    config: StrategyConfig,
    signal: Literal["long", "short"],
) -> Literal["hold", "exit", "reverse_to_long", "reverse_to_short"]:
    """根据最新已收盘 K 线决定实盘持仓继续、退出或反手。"""
    snapshot = latest_live_snapshot(candles, config)
    if snapshot is None:
        return "hold"
    if snapshot.trend_state == TREND_SIDEWAY:
        return "exit"
    if signal == "long" and snapshot.trend_state == TREND_DOWN:
        return "reverse_to_short" if config.signal_mode != "long_only" else "exit"
    if signal == "short" and snapshot.trend_state == TREND_UP:
        return "reverse_to_long" if config.signal_mode != "short_only" else "exit"
    return "hold"


def latest_live_snapshot(candles: list[Candle], config: StrategyConfig) -> LiveTripleEmaSnapshot | None:
    """返回三均线实盘决策所需的最新及前一根趋势状态。"""
    minimum = max(
        int(config.ema_period),
        int(config.trend_ema_period),
        int(config.big_ema_period),
        int(config.atr_period) if config.atr_stop_multiplier > 0 else 0,
    ) + 1
    if len(candles) < minimum:
        return None
    closes = [candle.close for candle in candles]
    fast_values = ema(closes, int(config.ema_period))
    middle_values = ema(closes, int(config.trend_ema_period))
    slow_values = ema(closes, int(config.big_ema_period))
    previous_state = _trend_state(fast_values[-2], middle_values[-2], slow_values[-2])
    trend_state = _trend_state(fast_values[-1], middle_values[-1], slow_values[-1])
    atr_value = None
    if config.atr_stop_multiplier > 0:
        atr_values = atr(candles, max(int(config.atr_period), 1))
        atr_value = atr_values[-1]
    return LiveTripleEmaSnapshot(
        candle=candles[-1],
        previous_state=previous_state,
        trend_state=trend_state,
        fast_ema=fast_values[-1],
        middle_ema=middle_values[-1],
        slow_ema=slow_values[-1],
        atr_value=atr_value,
    )


def _trend_state(fast_ema: Decimal, middle_ema: Decimal, slow_ema: Decimal) -> str:
    if fast_ema > middle_ema > slow_ema:
        return TREND_UP
    if fast_ema < middle_ema < slow_ema:
        return TREND_DOWN
    return TREND_SIDEWAY
