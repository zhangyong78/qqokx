"""通用交易计划 V1.0 的可计算规则。

该模块只负责把交易计划转换为无未来数据的信号和退出判定，
不直接下单，也不修改现有策略的运行逻辑。回测/实盘接入时应复用
本模块，避免在不同入口重复实现同一套规则。
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Literal

from okx_quant.indicators import atr, ema
from okx_quant.models import Candle


PlanMode = Literal["trend", "trend_pullback", "short_term", "range", "range_breakout"]
PlanSignal = Literal["long", "short"]


@dataclass(frozen=True)
class GeneralTradePlanConfig:
    """通用计划的客观化参数。

    ``trend`` 使用 EMA20/30/40；``short_term`` 使用 EMA20；
    ``range`` 和 ``range_breakout`` 使用前 N 根已收盘 K 线确定箱体。
    """

    mode: PlanMode = "trend"
    fast_ema_period: int = 20
    middle_ema_period: int = 30
    slow_ema_period: int = 40
    short_ema_period: int = 20
    atr_period: int = 14
    pullback_tolerance_atr: Decimal = Decimal("0.25")
    strong_body_atr: Decimal = Decimal("0.80")
    box_lookback_bars: int = 20
    box_boundary_tolerance_atr: Decimal = Decimal("0.25")
    box_breakout_buffer_atr: Decimal = Decimal("0.10")
    box_min_width_atr: Decimal = Decimal("1.0")
    box_max_width_atr: Decimal = Decimal("6.0")
    allow_long: bool = True
    allow_short: bool = True


@dataclass(frozen=True)
class GeneralTradePlanSignal:
    signal: PlanSignal | None
    mode: PlanMode
    reason: str
    candle_ts: int | None
    entry_reference: Decimal | None
    atr_value: Decimal | None
    ema_reference: Decimal | None
    box_upper: Decimal | None = None
    box_lower: Decimal | None = None


def evaluate_general_trade_plan(
    candles: list[Candle] | tuple[Candle, ...],
    config: GeneralTradePlanConfig | None = None,
) -> GeneralTradePlanSignal:
    """只用已确认 K 线评估最新一根信号。

    回踩和突破都在当前已收盘 K 线上确认，调用方应在下一根 K 线执行，
    以避免使用未完成 K 线或未来数据。
    """

    cfg = config or GeneralTradePlanConfig()
    _validate_config(cfg)
    confirmed = tuple(candle for candle in candles if candle.confirmed)
    minimum = _minimum_candles(cfg)
    if len(confirmed) < minimum:
        return _no_signal(cfg, f"已确认K线不足，至少需要 {minimum} 根")

    current = confirmed[-1]
    closes = [candle.close for candle in confirmed]
    atr_values = atr(list(confirmed), cfg.atr_period)
    atr_value = atr_values[-1]
    if atr_value is None or atr_value <= 0:
        return _no_signal(cfg, "当前 ATR 不可用")

    if cfg.mode in {"trend", "trend_pullback"}:
        return _evaluate_trend(confirmed, closes, cfg, atr_value)
    if cfg.mode == "short_term":
        return _evaluate_short_term(confirmed, closes, cfg, atr_value)
    if cfg.mode in {"range", "range_breakout"}:
        return _evaluate_range(confirmed, closes, cfg, atr_value)
    raise ValueError(f"不支持的通用计划模式：{cfg.mode}")


def should_exit_general_trade_plan(
    candles: list[Candle] | tuple[Candle, ...],
    config: GeneralTradePlanConfig,
    signal: PlanSignal,
) -> tuple[bool, str]:
    """判断已确认 K 线是否出现不利信号。

    普通止损和动态止损仍由公共保护模块负责；这里仅提供计划层的
    “出现不利信号及时退出”判定，供回测/实盘入口在确认后执行。
    """

    _validate_config(config)
    confirmed = tuple(candle for candle in candles if candle.confirmed)
    if len(confirmed) < _minimum_candles(config):
        return False, "数据不足"
    current = confirmed[-1]
    closes = [candle.close for candle in confirmed]
    atr_values = atr(list(confirmed), config.atr_period)
    atr_value = atr_values[-1]
    if atr_value is None or atr_value <= 0:
        return False, "ATR不可用"

    if config.mode in {"trend", "trend_pullback", "short_term"}:
        period = config.short_ema_period if config.mode == "short_term" else config.fast_ema_period
        line = ema(closes, period)[-1]
        if signal == "long" and current.close < line:
            return True, f"收盘跌破EMA{period}"
        if signal == "short" and current.close > line:
            return True, f"收盘突破EMA{period}"
        return False, "未出现均线不利信号"

    if len(confirmed) <= config.box_lookback_bars:
        return False, "箱体数据不足"
    window = confirmed[-config.box_lookback_bars - 1 : -1]
    upper = max(candle.high for candle in window)
    lower = min(candle.low for candle in window)
    tolerance = atr_value * config.box_boundary_tolerance_atr
    if config.mode == "range_breakout":
        if signal == "long" and current.close < upper - tolerance:
            return True, "向上突破后收盘回到箱体上沿下方"
        if signal == "short" and current.close > lower + tolerance:
            return True, "向下突破后收盘回到箱体下沿上方"
        return False, "突破结构未失效"
    if signal == "long" and current.close < lower - tolerance:
        return True, "收盘跌破箱体下沿"
    if signal == "short" and current.close > upper + tolerance:
        return True, "收盘突破箱体上沿"
    return False, "箱体边界未失效"


def _evaluate_trend(
    candles: tuple[Candle, ...],
    closes: list[Decimal],
    config: GeneralTradePlanConfig,
    atr_value: Decimal,
) -> GeneralTradePlanSignal:
    fast = ema(closes, config.fast_ema_period)
    middle = ema(closes, config.middle_ema_period)
    slow = ema(closes, config.slow_ema_period)
    previous = candles[-2]
    current = candles[-1]
    current_lines = (fast[-1], middle[-1], slow[-1])
    previous_lines = (fast[-2], middle[-2], slow[-2])
    bullish = current.close > current.open
    bearish = current.close < current.open
    body_is_strong = abs(current.close - current.open) >= atr_value * config.strong_body_atr
    tolerance = atr_value * config.pullback_tolerance_atr
    above_all = current.close > max(current_lines)
    below_all = current.close < min(current_lines)
    previous_above_all = previous.close > max(previous_lines)
    previous_below_all = previous.close < min(previous_lines)
    long_touch = above_all and bullish and any(
        _touches_level(current.low, current.high, line, tolerance) for line in current_lines
    )
    short_touch = below_all and bearish and any(
        _touches_level(current.low, current.high, line, tolerance) for line in current_lines
    )
    long_breakout = (
        config.mode != "trend_pullback"
        and above_all
        and not previous_above_all
        and bullish
        and body_is_strong
    )
    short_breakout = (
        config.mode != "trend_pullback"
        and below_all
        and not previous_below_all
        and bearish
        and body_is_strong
    )
    if config.allow_long and (long_touch or long_breakout):
        kind = "回踩阳线" if long_touch else "强阳线突破"
        return _signal(config, "long", current, atr_value, slow[-1], f"三均线多头排列，{kind}")
    if config.allow_short and (short_touch or short_breakout):
        kind = "反弹阴线" if short_touch else "强阴线跌破"
        return _signal(config, "short", current, atr_value, slow[-1], f"三均线空头排列，{kind}")
    return _no_signal(config, "三均线未出现确认回踩或有效突破", current, atr_value, slow[-1])


def _evaluate_short_term(
    candles: tuple[Candle, ...],
    closes: list[Decimal],
    config: GeneralTradePlanConfig,
    atr_value: Decimal,
) -> GeneralTradePlanSignal:
    line = ema(closes, config.short_ema_period)[-1]
    current = candles[-1]
    tolerance = atr_value * config.pullback_tolerance_atr
    if config.allow_long and current.close > line and current.close > current.open and _touches_level(
        current.low, current.high, line, tolerance
    ):
        return _signal(config, "long", current, atr_value, line, f"30分钟EMA{config.short_ema_period}上方回踩阳线")
    if config.allow_short and current.close < line and current.close < current.open and _touches_level(
        current.low, current.high, line, tolerance
    ):
        return _signal(config, "short", current, atr_value, line, f"30分钟EMA{config.short_ema_period}下方反弹阴线")
    return _no_signal(config, f"价格未确认EMA{config.short_ema_period}回踩/反弹", current, atr_value, line)


def _evaluate_range(
    candles: tuple[Candle, ...],
    closes: list[Decimal],
    config: GeneralTradePlanConfig,
    atr_value: Decimal,
) -> GeneralTradePlanSignal:
    current = candles[-1]
    window = candles[-config.box_lookback_bars - 1 : -1]
    upper = max(candle.high for candle in window)
    lower = min(candle.low for candle in window)
    width = upper - lower
    tolerance = atr_value * config.box_boundary_tolerance_atr
    if width < atr_value * config.box_min_width_atr or width > atr_value * config.box_max_width_atr:
        return _no_signal(config, "箱体宽度不在允许范围内", current, atr_value, None, upper, lower)
    if config.mode == "range_breakout":
        closes_all = [candle.close for candle in candles]
        lines = (
            ema(closes_all, config.fast_ema_period)[-1],
            ema(closes_all, config.middle_ema_period)[-1],
            ema(closes_all, config.slow_ema_period)[-1],
        )
        body_is_strong = abs(current.close - current.open) >= atr_value * config.strong_body_atr
        if config.allow_long and current.close > upper + atr_value * config.box_breakout_buffer_atr and current.close > max(lines) and current.close > current.open and body_is_strong:
            return _signal(config, "long", current, atr_value, max(lines), "强阳线向上突破箱体并站上三均线", upper, lower)
        if config.allow_short and current.close < lower - atr_value * config.box_breakout_buffer_atr and current.close < min(lines) and current.close < current.open and body_is_strong:
            return _signal(config, "short", current, atr_value, min(lines), "强阴线向下跌破箱体并跌破三均线", upper, lower)
        return _no_signal(config, "箱体尚未出现均线共振突破", current, atr_value, None, upper, lower)

    midpoint = lower + width / Decimal("2")
    near_lower = current.low <= lower + tolerance and current.close <= lower + width * Decimal("0.35")
    near_upper = current.high >= upper - tolerance and current.close >= upper - width * Decimal("0.35")
    if config.allow_long and near_lower and current.close > current.open:
        return _signal(config, "long", current, atr_value, lower, "箱体下沿触及支撑后阳线止跌", upper, lower)
    if config.allow_short and near_upper and current.close < current.open:
        return _signal(config, "short", current, atr_value, upper, "箱体上沿触及压力后阴线受阻", upper, lower)
    if lower + width * Decimal("0.35") < current.close < midpoint + width * Decimal("0.15"):
        return _no_signal(config, "价格位于箱体中部，禁止盲目交易", current, atr_value, None, upper, lower)
    return _no_signal(config, "箱体边界未出现确认反应", current, atr_value, None, upper, lower)


def _signal(
    config: GeneralTradePlanConfig,
    signal: PlanSignal,
    candle: Candle,
    atr_value: Decimal,
    ema_reference: Decimal | None,
    reason: str,
    box_upper: Decimal | None = None,
    box_lower: Decimal | None = None,
) -> GeneralTradePlanSignal:
    return GeneralTradePlanSignal(signal, config.mode, reason, candle.ts, candle.close, atr_value, ema_reference, box_upper, box_lower)


def _no_signal(
    config: GeneralTradePlanConfig,
    reason: str,
    candle: Candle | None = None,
    atr_value: Decimal | None = None,
    ema_reference: Decimal | None = None,
    box_upper: Decimal | None = None,
    box_lower: Decimal | None = None,
) -> GeneralTradePlanSignal:
    return GeneralTradePlanSignal(None, config.mode, reason, candle.ts if candle else None, None, atr_value, ema_reference, box_upper, box_lower)


def _touches_level(low: Decimal, high: Decimal, level: Decimal, tolerance: Decimal) -> bool:
    return low <= level + tolerance and high >= level - tolerance


def _minimum_candles(config: GeneralTradePlanConfig) -> int:
    if config.mode == "short_term":
        return max(config.short_ema_period, config.atr_period) + 1
    if config.mode in {"range", "range_breakout"}:
        return max(config.box_lookback_bars + 1, config.atr_period + 1, config.slow_ema_period + 1)
    return max(config.fast_ema_period, config.middle_ema_period, config.slow_ema_period, config.atr_period) + 1


def _validate_config(config: GeneralTradePlanConfig) -> None:
    if config.mode not in {"trend", "trend_pullback", "short_term", "range", "range_breakout"}:
        raise ValueError(f"不支持的通用计划模式：{config.mode}")
    numeric = {
        "fast_ema_period": config.fast_ema_period,
        "middle_ema_period": config.middle_ema_period,
        "slow_ema_period": config.slow_ema_period,
        "short_ema_period": config.short_ema_period,
        "atr_period": config.atr_period,
        "box_lookback_bars": config.box_lookback_bars,
    }
    if any(int(value) <= 0 for value in numeric.values()):
        raise ValueError("均线、ATR和箱体回看周期必须大于0")
    if config.pullback_tolerance_atr < 0 or config.strong_body_atr <= 0:
        raise ValueError("回踩容差不能为负，强K线阈值必须大于0")
    if config.box_boundary_tolerance_atr < 0 or config.box_breakout_buffer_atr < 0:
        raise ValueError("箱体边界容差不能为负")
    if config.box_min_width_atr <= 0 or config.box_max_width_atr < config.box_min_width_atr:
        raise ValueError("箱体宽度范围无效")
