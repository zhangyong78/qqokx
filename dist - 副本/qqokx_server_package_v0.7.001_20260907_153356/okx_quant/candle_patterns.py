from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal
from typing import Literal

from okx_quant.models import Candle

TrendContext = Literal["uptrend", "downtrend", "sideways", "unknown"]
PatternBias = Literal[
    "bullish_reversal",
    "bearish_reversal",
    "bullish_continuation",
    "bearish_continuation",
    "neutral",
]

ZERO = Decimal("0")


@dataclass(frozen=True)
class SingleCandlePatternConfig:
    trend_lookback: int = 3
    trend_change_threshold: Decimal = Decimal("0.01")
    doji_body_ratio_max: Decimal = Decimal("0.10")
    doji_long_shadow_ratio_min: Decimal = Decimal("0.35")
    small_shadow_ratio_max: Decimal = Decimal("0.12")
    hammer_body_ratio_max: Decimal = Decimal("0.35")
    hammer_shadow_body_ratio_min: Decimal = Decimal("2.0")
    hammer_opposite_shadow_body_ratio_max: Decimal = Decimal("0.5")
    marubozu_body_ratio_min: Decimal = Decimal("0.85")
    marubozu_shadow_ratio_max: Decimal = Decimal("0.08")
    spinning_top_body_ratio_max: Decimal = Decimal("0.35")
    spinning_top_shadow_ratio_min: Decimal = Decimal("0.20")


@dataclass(frozen=True)
class SingleCandleMetrics:
    price_range: Decimal
    body: Decimal
    upper_shadow: Decimal
    lower_shadow: Decimal
    body_ratio: Decimal
    upper_shadow_ratio: Decimal
    lower_shadow_ratio: Decimal


@dataclass(frozen=True)
class SingleCandlePatternMatch:
    pattern: str
    bias: PatternBias
    reason: str


@dataclass(frozen=True)
class SingleCandlePatternReport:
    inst_id: str | None
    candle: Candle | None
    trend_context: TrendContext
    matches: tuple[SingleCandlePatternMatch, ...]

    @property
    def candle_ts(self) -> int | None:
        return None if self.candle is None else self.candle.ts

    @property
    def primary_pattern(self) -> str | None:
        if not self.matches:
            return None
        return self.matches[0].pattern


def analyze_single_candle_patterns(
    candles: list[Candle],
    *,
    inst_id: str | None = None,
    config: SingleCandlePatternConfig | None = None,
) -> SingleCandlePatternReport:
    config = config or SingleCandlePatternConfig()
    if not candles:
        return SingleCandlePatternReport(
            inst_id=inst_id,
            candle=None,
            trend_context="unknown",
            matches=(),
        )

    candle = candles[-1]
    metrics = _build_metrics(candle)
    if metrics is None:
        return SingleCandlePatternReport(
            inst_id=inst_id,
            candle=candle,
            trend_context="unknown",
            matches=(),
        )

    trend_context = _infer_trend_context(candles, config)
    matches: list[SingleCandlePatternMatch] = []
    _detect_doji_family(matches, metrics, config)
    _detect_hammer_family(matches, metrics, trend_context, config)
    _detect_inverted_hammer_family(matches, metrics, trend_context, config)
    _detect_marubozu(matches, candle, metrics, config)
    _detect_spinning_top(matches, metrics, config)
    return SingleCandlePatternReport(
        inst_id=inst_id,
        candle=candle,
        trend_context=trend_context,
        matches=tuple(matches),
    )


def analyze_single_candle_pattern_history(
    candles: list[Candle],
    *,
    inst_id: str | None = None,
    config: SingleCandlePatternConfig | None = None,
) -> list[SingleCandlePatternReport]:
    reports: list[SingleCandlePatternReport] = []
    for index in range(len(candles)):
        report = analyze_single_candle_patterns(
            candles[: index + 1],
            inst_id=inst_id,
            config=config,
        )
        if report.matches:
            reports.append(report)
    return reports


def single_candle_report_payload(report: SingleCandlePatternReport) -> dict[str, object]:
    candle_payload: dict[str, object] | None = None
    metrics_payload: dict[str, object] | None = None
    if report.candle is not None:
        candle_payload = {
            "ts": report.candle.ts,
            "open": _decimal_text(report.candle.open),
            "high": _decimal_text(report.candle.high),
            "low": _decimal_text(report.candle.low),
            "close": _decimal_text(report.candle.close),
            "volume": _decimal_text(report.candle.volume),
            "confirmed": report.candle.confirmed,
        }
        metrics = _build_metrics(report.candle)
        if metrics is not None:
            metrics_payload = {
                "range": _decimal_text(metrics.price_range),
                "body": _decimal_text(metrics.body),
                "upper_shadow": _decimal_text(metrics.upper_shadow),
                "lower_shadow": _decimal_text(metrics.lower_shadow),
                "body_ratio": _decimal_text(metrics.body_ratio),
                "upper_shadow_ratio": _decimal_text(metrics.upper_shadow_ratio),
                "lower_shadow_ratio": _decimal_text(metrics.lower_shadow_ratio),
            }
    return {
        "inst_id": report.inst_id,
        "candle_ts": report.candle_ts,
        "trend_context": report.trend_context,
        "primary_pattern": report.primary_pattern,
        "candle": candle_payload,
        "metrics": metrics_payload,
        "matches": [
            {
                "pattern": item.pattern,
                "bias": item.bias,
                "reason": item.reason,
            }
            for item in report.matches
        ],
    }


def single_candle_report_json(report: SingleCandlePatternReport) -> str:
    return json.dumps(single_candle_report_payload(report), ensure_ascii=False, indent=2)


def _build_metrics(candle: Candle) -> SingleCandleMetrics | None:
    price_range = candle.high - candle.low
    if price_range <= 0:
        return None
    body_high = max(candle.open, candle.close)
    body_low = min(candle.open, candle.close)
    body = body_high - body_low
    upper_shadow = candle.high - body_high
    lower_shadow = body_low - candle.low
    return SingleCandleMetrics(
        price_range=price_range,
        body=body,
        upper_shadow=max(upper_shadow, ZERO),
        lower_shadow=max(lower_shadow, ZERO),
        body_ratio=body / price_range,
        upper_shadow_ratio=max(upper_shadow, ZERO) / price_range,
        lower_shadow_ratio=max(lower_shadow, ZERO) / price_range,
    )


def _infer_trend_context(candles: list[Candle], config: SingleCandlePatternConfig) -> TrendContext:
    if len(candles) < config.trend_lookback + 1:
        return "unknown"
    history = candles[-(config.trend_lookback + 1) : -1]
    if not history:
        return "unknown"
    start = history[0].close
    end = history[-1].close
    if start <= 0:
        return "unknown"
    change_ratio = (end - start) / start
    increasing = all(history[index].close > history[index - 1].close for index in range(1, len(history)))
    decreasing = all(history[index].close < history[index - 1].close for index in range(1, len(history)))
    if increasing and change_ratio >= config.trend_change_threshold:
        return "uptrend"
    if decreasing and change_ratio <= -config.trend_change_threshold:
        return "downtrend"
    return "sideways"


def _detect_doji_family(
    matches: list[SingleCandlePatternMatch],
    metrics: SingleCandleMetrics,
    config: SingleCandlePatternConfig,
) -> None:
    if metrics.body_ratio > config.doji_body_ratio_max:
        return
    long_upper = metrics.upper_shadow_ratio >= config.doji_long_shadow_ratio_min
    long_lower = metrics.lower_shadow_ratio >= config.doji_long_shadow_ratio_min
    short_upper = metrics.upper_shadow_ratio <= config.small_shadow_ratio_max
    short_lower = metrics.lower_shadow_ratio <= config.small_shadow_ratio_max
    if long_lower and short_upper:
        matches.append(
            SingleCandlePatternMatch(
                pattern="dragonfly_doji",
                bias="bullish_reversal",
                reason=_ratio_reason("dragonfly_doji", metrics),
            )
        )
    if long_upper and short_lower:
        matches.append(
            SingleCandlePatternMatch(
                pattern="gravestone_doji",
                bias="bearish_reversal",
                reason=_ratio_reason("gravestone_doji", metrics),
            )
        )
    if long_upper and long_lower:
        matches.append(
            SingleCandlePatternMatch(
                pattern="long_legged_doji",
                bias="neutral",
                reason=_ratio_reason("long_legged_doji", metrics),
            )
        )
    matches.append(
        SingleCandlePatternMatch(
            pattern="doji",
            bias="neutral",
            reason=_ratio_reason("doji", metrics),
        )
    )


def _detect_hammer_family(
    matches: list[SingleCandlePatternMatch],
    metrics: SingleCandleMetrics,
    trend_context: TrendContext,
    config: SingleCandlePatternConfig,
) -> None:
    if metrics.body_ratio > config.hammer_body_ratio_max:
        return
    if metrics.body <= 0:
        return
    if metrics.lower_shadow < metrics.body * config.hammer_shadow_body_ratio_min:
        return
    if metrics.upper_shadow > metrics.body * config.hammer_opposite_shadow_body_ratio_max:
        return
    pattern = "hammer"
    bias: PatternBias = "bullish_reversal"
    if trend_context == "uptrend":
        pattern = "hanging_man"
        bias = "bearish_reversal"
    matches.append(
        SingleCandlePatternMatch(
            pattern=pattern,
            bias=bias,
            reason=_ratio_reason(pattern, metrics),
        )
    )


def _detect_inverted_hammer_family(
    matches: list[SingleCandlePatternMatch],
    metrics: SingleCandleMetrics,
    trend_context: TrendContext,
    config: SingleCandlePatternConfig,
) -> None:
    if metrics.body_ratio > config.hammer_body_ratio_max:
        return
    if metrics.body <= 0:
        return
    if metrics.upper_shadow < metrics.body * config.hammer_shadow_body_ratio_min:
        return
    if metrics.lower_shadow > metrics.body * config.hammer_opposite_shadow_body_ratio_max:
        return
    pattern = "inverted_hammer"
    bias: PatternBias = "bullish_reversal"
    if trend_context == "uptrend":
        pattern = "shooting_star"
        bias = "bearish_reversal"
    matches.append(
        SingleCandlePatternMatch(
            pattern=pattern,
            bias=bias,
            reason=_ratio_reason(pattern, metrics),
        )
    )


def _detect_marubozu(
    matches: list[SingleCandlePatternMatch],
    candle: Candle,
    metrics: SingleCandleMetrics,
    config: SingleCandlePatternConfig,
) -> None:
    if metrics.body_ratio < config.marubozu_body_ratio_min:
        return
    if metrics.upper_shadow_ratio > config.marubozu_shadow_ratio_max:
        return
    if metrics.lower_shadow_ratio > config.marubozu_shadow_ratio_max:
        return
    if candle.close > candle.open:
        pattern = "bullish_marubozu"
        bias: PatternBias = "bullish_continuation"
    elif candle.close < candle.open:
        pattern = "bearish_marubozu"
        bias = "bearish_continuation"
    else:
        return
    matches.append(
        SingleCandlePatternMatch(
            pattern=pattern,
            bias=bias,
            reason=_ratio_reason(pattern, metrics),
        )
    )


def _detect_spinning_top(
    matches: list[SingleCandlePatternMatch],
    metrics: SingleCandleMetrics,
    config: SingleCandlePatternConfig,
) -> None:
    if metrics.body_ratio > config.spinning_top_body_ratio_max:
        return
    if metrics.upper_shadow_ratio < config.spinning_top_shadow_ratio_min:
        return
    if metrics.lower_shadow_ratio < config.spinning_top_shadow_ratio_min:
        return
    matches.append(
        SingleCandlePatternMatch(
            pattern="spinning_top",
            bias="neutral",
            reason=_ratio_reason("spinning_top", metrics),
        )
    )


def _ratio_reason(pattern: str, metrics: SingleCandleMetrics) -> str:
    return (
        f"{pattern}: body_ratio={_decimal_text(metrics.body_ratio)}, "
        f"upper_shadow_ratio={_decimal_text(metrics.upper_shadow_ratio)}, "
        f"lower_shadow_ratio={_decimal_text(metrics.lower_shadow_ratio)}"
    )


def _decimal_text(value: Decimal) -> str:
    return format(value.normalize(), "f") if value != value.to_integral() else format(value.quantize(Decimal("1")), "f")
