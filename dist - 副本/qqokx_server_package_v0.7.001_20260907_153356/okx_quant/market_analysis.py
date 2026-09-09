from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal

from okx_quant.models import Candle
from okx_quant.okx_client import OkxRestClient
from okx_quant.persistence import analysis_report_dir_path

VolatilityRegime = Literal["low", "medium", "high", "unknown"]
FactorBias = Literal["bullish", "bearish", "caution", "neutral"]
DirectionMode = Literal["close_to_close", "candle_body"]

ZERO = Decimal("0")
ONE = Decimal("1")
HUNDRED = Decimal("100")


@dataclass(frozen=True)
class MarketAnalysisConfig:
    direction_mode: DirectionMode = "close_to_close"
    exhaustion_bucket_start: int = 7
    first_bearish_min_streak: int = 2
    follow_through_short_horizon: int = 3
    follow_through_long_horizon: int = 5
    pullback_micro_max: Decimal = Decimal("0.02")
    pullback_small_max: Decimal = Decimal("0.05")
    pullback_medium_max: Decimal = Decimal("0.08")
    support_ma_period: int = 20
    support_break_requires_dual_confirmation: bool = True
    volatility_period: int = 20
    annualization_days: int = 365
    low_volatility_quantile: Decimal = Decimal("0.3333333333")
    high_volatility_quantile: Decimal = Decimal("0.6666666667")
    factor_min_samples: int = 20
    factor_alert_min_samples: int = 10
    factor_min_edge: Decimal = Decimal("0.03")
    factor_strong_edge: Decimal = Decimal("0.08")
    support_hold_max_continue_down: Decimal = Decimal("0.40")
    breakdown_min_continue_down: Decimal = Decimal("0.65")


@dataclass(frozen=True)
class StreakContinuationStat:
    streak_label: str
    sample_count: int
    continuation_probability: Decimal | None
    baseline_probability: Decimal | None
    edge_vs_baseline: Decimal | None
    insight: str


@dataclass(frozen=True)
class PostStreakPullbackStat:
    bucket: str
    sample_count: int
    follow_through_3d_samples: int
    follow_through_5d_samples: int
    continue_down_3d_probability: Decimal | None
    continue_down_5d_probability: Decimal | None
    insight: str


@dataclass(frozen=True)
class SupportBreakStat:
    support_status: str
    sample_count: int
    follow_through_5d_samples: int
    continue_down_5d_probability: Decimal | None
    insight: str


@dataclass(frozen=True)
class VolatilityRegimeStat:
    regime: VolatilityRegime
    sample_count: int
    continuation_probability: Decimal | None
    insight: str


@dataclass(frozen=True)
class FactorCandidate:
    key: str
    label: str
    direction_bias: FactorBias
    sample_count: int
    probability: Decimal | None
    edge_vs_reference: Decimal | None
    adopt: bool
    rationale: str


@dataclass(frozen=True)
class ActiveFactor:
    key: str
    label: str
    direction_bias: FactorBias
    score: Decimal
    reason: str


@dataclass(frozen=True)
class MarketAnalysisSnapshot:
    as_of_ts: int | None
    last_close: Decimal | None
    current_bullish_streak: int
    last_completed_bullish_streak: int | None
    latest_pullback_bucket: str | None
    latest_support_break: bool | None
    latest_volatility_regime: VolatilityRegime


@dataclass(frozen=True)
class MarketAnalysisReport:
    inst_id: str | None
    timeframe: str | None
    direction_mode: DirectionMode
    candle_count: int
    period_start_ts: int | None
    period_end_ts: int | None
    baseline_bullish_probability: Decimal | None
    streak_stats: tuple[StreakContinuationStat, ...]
    pullback_stats: tuple[PostStreakPullbackStat, ...]
    support_break_stats: tuple[SupportBreakStat, ...]
    volatility_stats: tuple[VolatilityRegimeStat, ...]
    factor_candidates: tuple[FactorCandidate, ...]
    active_factors: tuple[ActiveFactor, ...]
    snapshot: MarketAnalysisSnapshot
    notes: tuple[str, ...]


def build_market_analysis_report(
    candles: list[Candle],
    *,
    inst_id: str | None = None,
    timeframe: str | None = None,
    config: MarketAnalysisConfig | None = None,
) -> MarketAnalysisReport:
    config = config or MarketAnalysisConfig()
    ordered = sorted(candles, key=lambda item: item.ts)
    baseline_probability = _baseline_bullish_probability(ordered, config)
    support_ma = _simple_moving_average([candle.close for candle in ordered], config.support_ma_period)
    volatility_values = _annualized_volatility_series(ordered, config)
    volatility_bounds = _volatility_regime_bounds(volatility_values, config)

    streak_stats = _analyze_streak_continuation(ordered, baseline_probability, config)
    pullback_stats, support_break_stats = _analyze_post_streak_bearish_follow_through(
        ordered,
        support_ma,
        config,
    )
    volatility_stats = _analyze_volatility_regimes(
        ordered,
        volatility_values,
        volatility_bounds,
        config,
    )
    factor_candidates = _derive_factor_candidates(
        baseline_probability,
        streak_stats,
        pullback_stats,
        support_break_stats,
        volatility_stats,
        config,
    )
    snapshot = _build_snapshot(
        ordered,
        support_ma,
        volatility_values,
        volatility_bounds,
        config,
    )
    active_factors = _derive_active_factors(snapshot, config)
    notes = _build_methodology_notes(config, streak_stats, support_break_stats)

    return MarketAnalysisReport(
        inst_id=inst_id,
        timeframe=timeframe,
        direction_mode=config.direction_mode,
        candle_count=len(ordered),
        period_start_ts=None if not ordered else ordered[0].ts,
        period_end_ts=None if not ordered else ordered[-1].ts,
        baseline_bullish_probability=baseline_probability,
        streak_stats=tuple(streak_stats),
        pullback_stats=tuple(pullback_stats),
        support_break_stats=tuple(support_break_stats),
        volatility_stats=tuple(volatility_stats),
        factor_candidates=tuple(factor_candidates),
        active_factors=tuple(active_factors),
        snapshot=snapshot,
        notes=tuple(notes),
    )


def build_market_analysis_report_from_client(
    client: OkxRestClient,
    inst_id: str,
    *,
    bar: str = "1D",
    limit: int = 0,
    config: MarketAnalysisConfig | None = None,
) -> MarketAnalysisReport:
    candles = client.get_candles_history(inst_id, bar, limit=limit)
    return build_market_analysis_report(
        candles,
        inst_id=inst_id,
        timeframe=bar,
        config=config,
    )


def market_analysis_report_payload(report: MarketAnalysisReport) -> dict[str, object]:
    return {
        "inst_id": report.inst_id,
        "timeframe": report.timeframe,
        "direction_mode": report.direction_mode,
        "candle_count": report.candle_count,
        "period_start_ts": report.period_start_ts,
        "period_end_ts": report.period_end_ts,
        "baseline_bullish_probability": _decimal_text(report.baseline_bullish_probability),
        "streak_stats": [
            {
                "streak_label": item.streak_label,
                "sample_count": item.sample_count,
                "continuation_probability": _decimal_text(item.continuation_probability),
                "baseline_probability": _decimal_text(item.baseline_probability),
                "edge_vs_baseline": _decimal_text(item.edge_vs_baseline),
                "insight": item.insight,
            }
            for item in report.streak_stats
        ],
        "pullback_stats": [
            {
                "bucket": item.bucket,
                "sample_count": item.sample_count,
                "follow_through_3d_samples": item.follow_through_3d_samples,
                "follow_through_5d_samples": item.follow_through_5d_samples,
                "continue_down_3d_probability": _decimal_text(item.continue_down_3d_probability),
                "continue_down_5d_probability": _decimal_text(item.continue_down_5d_probability),
                "insight": item.insight,
            }
            for item in report.pullback_stats
        ],
        "support_break_stats": [
            {
                "support_status": item.support_status,
                "sample_count": item.sample_count,
                "follow_through_5d_samples": item.follow_through_5d_samples,
                "continue_down_5d_probability": _decimal_text(item.continue_down_5d_probability),
                "insight": item.insight,
            }
            for item in report.support_break_stats
        ],
        "volatility_stats": [
            {
                "regime": item.regime,
                "sample_count": item.sample_count,
                "continuation_probability": _decimal_text(item.continuation_probability),
                "insight": item.insight,
            }
            for item in report.volatility_stats
        ],
        "factor_candidates": [
            {
                "key": item.key,
                "label": item.label,
                "direction_bias": item.direction_bias,
                "sample_count": item.sample_count,
                "probability": _decimal_text(item.probability),
                "edge_vs_reference": _decimal_text(item.edge_vs_reference),
                "adopt": item.adopt,
                "rationale": item.rationale,
            }
            for item in report.factor_candidates
        ],
        "active_factors": [
            {
                "key": item.key,
                "label": item.label,
                "direction_bias": item.direction_bias,
                "score": _decimal_text(item.score),
                "reason": item.reason,
            }
            for item in report.active_factors
        ],
        "snapshot": {
            "as_of_ts": report.snapshot.as_of_ts,
            "last_close": _decimal_text(report.snapshot.last_close),
            "current_bullish_streak": report.snapshot.current_bullish_streak,
            "last_completed_bullish_streak": report.snapshot.last_completed_bullish_streak,
            "latest_pullback_bucket": report.snapshot.latest_pullback_bucket,
            "latest_support_break": report.snapshot.latest_support_break,
            "latest_volatility_regime": report.snapshot.latest_volatility_regime,
        },
        "notes": list(report.notes),
    }


def market_analysis_report_json(report: MarketAnalysisReport) -> str:
    return json.dumps(market_analysis_report_payload(report), ensure_ascii=False, indent=2)


def save_market_analysis_report(
    report: MarketAnalysisReport,
    *,
    path: Path | None = None,
    base_dir: Path | None = None,
) -> Path:
    if path is None:
        report_dir = analysis_report_dir_path(base_dir)
        report_dir.mkdir(parents=True, exist_ok=True)
        safe_inst_id = _safe_file_component(report.inst_id or "market")
        safe_timeframe = _safe_file_component(report.timeframe or "na")
        safe_direction_mode = _safe_file_component(report.direction_mode)
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        path = report_dir / (
            f"{safe_inst_id}_{safe_timeframe}_{safe_direction_mode}_market_analysis_{timestamp}.json"
        )
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(market_analysis_report_json(report), encoding="utf-8")
    return path


def _baseline_bullish_probability(
    candles: list[Candle],
    config: MarketAnalysisConfig,
) -> Decimal | None:
    eligible_indexes = _eligible_direction_indexes(candles, config)
    if not eligible_indexes:
        return None
    bullish_count = sum(1 for index in eligible_indexes if _is_positive_bar(candles, index, config))
    return Decimal(bullish_count) / Decimal(len(eligible_indexes))


def _analyze_streak_continuation(
    candles: list[Candle],
    baseline_probability: Decimal | None,
    config: MarketAnalysisConfig,
) -> list[StreakContinuationStat]:
    if len(candles) < 2:
        return []

    observations: dict[str, list[int]] = {}
    current_streak = 0
    for index in range(len(candles) - 1):
        if _is_positive_bar(candles, index, config):
            current_streak += 1
        else:
            current_streak = 0
        if current_streak <= 0:
            continue
        label = _streak_label(current_streak, config.exhaustion_bucket_start)
        observations.setdefault(label, []).append(1 if _is_positive_bar(candles, index + 1, config) else 0)

    labels = sorted(observations.keys(), key=lambda item: _streak_sort_key(item, config.exhaustion_bucket_start))
    stats: list[StreakContinuationStat] = []
    for label in labels:
        sample_count = len(observations[label])
        probability = _probability_from_outcomes(observations[label])
        edge = None if probability is None or baseline_probability is None else probability - baseline_probability
        stats.append(
            StreakContinuationStat(
                streak_label=label,
                sample_count=sample_count,
                continuation_probability=probability,
                baseline_probability=baseline_probability,
                edge_vs_baseline=edge,
                insight=_streak_insight(label, edge),
            )
        )
    return stats


def _analyze_post_streak_bearish_follow_through(
    candles: list[Candle],
    support_ma: list[Decimal | None],
    config: MarketAnalysisConfig,
) -> tuple[list[PostStreakPullbackStat], list[SupportBreakStat]]:
    if len(candles) < 2:
        return [], []

    pullback_records: dict[str, dict[str, int]] = {
        "micro": {"events": 0, "sample_3d": 0, "sample_5d": 0, "down_3d": 0, "down_5d": 0},
        "small": {"events": 0, "sample_3d": 0, "sample_5d": 0, "down_3d": 0, "down_5d": 0},
        "medium": {"events": 0, "sample_3d": 0, "sample_5d": 0, "down_3d": 0, "down_5d": 0},
        "large": {"events": 0, "sample_3d": 0, "sample_5d": 0, "down_3d": 0, "down_5d": 0},
    }
    support_records: dict[str, dict[str, int]] = {
        "support_held": {"events": 0, "sample_5d": 0, "down_5d": 0},
        "support_broken": {"events": 0, "sample_5d": 0, "down_5d": 0},
    }

    bullish_streak = 0
    for index in range(len(candles)):
        if _is_positive_bar(candles, index, config):
            bullish_streak += 1
            continue

        if _is_negative_bar(candles, index, config) and bullish_streak >= config.first_bearish_min_streak:
            streak_start_index = index - bullish_streak
            previous_close = candles[index - 1].close
            pullback_ratio = _close_to_close_pullback(previous_close, candles[index].close)
            bucket = _pullback_bucket(pullback_ratio, config)
            pullback_record = pullback_records[bucket]
            pullback_record["events"] += 1

            if index + config.follow_through_short_horizon < len(candles):
                pullback_record["sample_3d"] += 1
                if candles[index + config.follow_through_short_horizon].close < candles[index].close:
                    pullback_record["down_3d"] += 1
            if index + config.follow_through_long_horizon < len(candles):
                pullback_record["sample_5d"] += 1
                if candles[index + config.follow_through_long_horizon].close < candles[index].close:
                    pullback_record["down_5d"] += 1

            support_break = _classify_support_break(
                candles,
                support_ma,
                index=index,
                streak_start_index=streak_start_index,
                config=config,
            )
            if support_break is not None:
                support_key = "support_broken" if support_break else "support_held"
                support_record = support_records[support_key]
                support_record["events"] += 1
                if index + config.follow_through_long_horizon < len(candles):
                    support_record["sample_5d"] += 1
                    if candles[index + config.follow_through_long_horizon].close < candles[index].close:
                        support_record["down_5d"] += 1

        bullish_streak = 0

    pullback_stats = [
        PostStreakPullbackStat(
            bucket=bucket,
            sample_count=record["events"],
            follow_through_3d_samples=record["sample_3d"],
            follow_through_5d_samples=record["sample_5d"],
            continue_down_3d_probability=_ratio_or_none(record["down_3d"], record["sample_3d"]),
            continue_down_5d_probability=_ratio_or_none(record["down_5d"], record["sample_5d"]),
            insight=_pullback_insight(
                _ratio_or_none(record["down_5d"], record["sample_5d"]),
                bucket,
            ),
        )
        for bucket, record in pullback_records.items()
        if record["events"] > 0
    ]
    support_break_stats = [
        SupportBreakStat(
            support_status=status,
            sample_count=record["events"],
            follow_through_5d_samples=record["sample_5d"],
            continue_down_5d_probability=_ratio_or_none(record["down_5d"], record["sample_5d"]),
            insight=_support_break_insight(
                status,
                _ratio_or_none(record["down_5d"], record["sample_5d"]),
            ),
        )
        for status, record in support_records.items()
        if record["events"] > 0
    ]
    return pullback_stats, support_break_stats


def _analyze_volatility_regimes(
    candles: list[Candle],
    volatility_values: list[Decimal | None],
    bounds: tuple[Decimal | None, Decimal | None],
    config: MarketAnalysisConfig,
) -> list[VolatilityRegimeStat]:
    if len(candles) < 2:
        return []

    records: dict[VolatilityRegime, dict[str, int]] = {
        "low": {"events": 0, "continued": 0},
        "medium": {"events": 0, "continued": 0},
        "high": {"events": 0, "continued": 0},
        "unknown": {"events": 0, "continued": 0},
    }
    current_streak = 0
    for index in range(len(candles) - 1):
        if _is_positive_bar(candles, index, config):
            current_streak += 1
        else:
            current_streak = 0
        if current_streak <= 0:
            continue

        regime = _volatility_regime(volatility_values[index], bounds)
        if regime == "unknown":
            continue
        records[regime]["events"] += 1
        if _is_positive_bar(candles, index + 1, config):
            records[regime]["continued"] += 1

    stats: list[VolatilityRegimeStat] = []
    for regime in ("low", "medium", "high"):
        record = records[regime]
        if record["events"] <= 0:
            continue
        probability = _ratio_or_none(record["continued"], record["events"])
        stats.append(
            VolatilityRegimeStat(
                regime=regime,
                sample_count=record["events"],
                continuation_probability=probability,
                insight=_volatility_insight(regime, probability, config),
            )
        )
    return stats


def _derive_factor_candidates(
    baseline_probability: Decimal | None,
    streak_stats: list[StreakContinuationStat],
    pullback_stats: list[PostStreakPullbackStat],
    support_break_stats: list[SupportBreakStat],
    volatility_stats: list[VolatilityRegimeStat],
    config: MarketAnalysisConfig,
) -> list[FactorCandidate]:
    candidates: list[FactorCandidate] = []
    streak_by_label = {item.streak_label: item for item in streak_stats}
    pullback_by_bucket = {item.bucket: item for item in pullback_stats}
    support_by_status = {item.support_status: item for item in support_break_stats}
    volatility_by_regime = {item.regime: item for item in volatility_stats}

    peak_stat = _best_streak_stat(
        [streak_by_label.get("5"), streak_by_label.get("6")],
        baseline_probability,
        config,
    )
    if peak_stat is not None:
        candidates.append(
            FactorCandidate(
                key="streak_momentum_peak_5_6",
                label="5-6连阳顺势因子",
                direction_bias="bullish",
                sample_count=peak_stat.sample_count,
                probability=peak_stat.continuation_probability,
                edge_vs_reference=peak_stat.edge_vs_baseline,
                adopt=_can_adopt_from_edge(peak_stat.sample_count, peak_stat.edge_vs_baseline, config),
                rationale="5-6连阳通常是惯性最稳定的阶段，可作为顺势持仓或加分因子。",
            )
        )

    exhaustion_stat = streak_by_label.get(f"{config.exhaustion_bucket_start}+")
    if exhaustion_stat is not None and peak_stat is not None:
        continuation_drop = None
        if exhaustion_stat.continuation_probability is not None and peak_stat.continuation_probability is not None:
            continuation_drop = peak_stat.continuation_probability - exhaustion_stat.continuation_probability
        candidates.append(
            FactorCandidate(
                key="streak_exhaustion_ge_7",
                label="7连阳以上衰减因子",
                direction_bias="caution",
                sample_count=exhaustion_stat.sample_count,
                probability=exhaustion_stat.continuation_probability,
                edge_vs_reference=continuation_drop,
                adopt=(
                    exhaustion_stat.sample_count >= config.factor_alert_min_samples
                    and continuation_drop is not None
                    and continuation_drop >= config.factor_min_edge
                ),
                rationale="极端连阳后继续追涨的安全边际会收窄，更适合作为降权或风控因子。",
            )
        )

    support_hold_stat = support_by_status.get("support_held")
    if support_hold_stat is not None:
        probability = support_hold_stat.continue_down_5d_probability
        bullish_edge = None if probability is None else ONE - probability
        candidates.append(
            FactorCandidate(
                key="post_streak_pullback_support_hold",
                label="连阳后守住支撑回踩因子",
                direction_bias="bullish",
                sample_count=support_hold_stat.sample_count,
                probability=probability,
                edge_vs_reference=bullish_edge,
                adopt=(
                    support_hold_stat.sample_count >= config.factor_min_samples
                    and probability is not None
                    and probability <= config.support_hold_max_continue_down
                ),
                rationale="连阳终结后的微阴或小阴若守住启动位与均线，往往更像健康回踩而非趋势破坏。",
            )
        )

    support_break_stat = support_by_status.get("support_broken")
    if support_break_stat is not None:
        probability = support_break_stat.continue_down_5d_probability
        candidates.append(
            FactorCandidate(
                key="post_streak_breakdown",
                label="连阳后破位转弱因子",
                direction_bias="bearish",
                sample_count=support_break_stat.sample_count,
                probability=probability,
                edge_vs_reference=probability,
                adopt=(
                    support_break_stat.sample_count >= config.factor_alert_min_samples
                    and probability is not None
                    and probability >= config.breakdown_min_continue_down
                ),
                rationale="第一根阴线若直接跌穿连阳启动位和均线，后续继续走弱的概率通常明显抬升。",
            )
        )

    low_vol = volatility_by_regime.get("low")
    high_vol = volatility_by_regime.get("high")
    if low_vol is not None:
        low_edge = None
        if high_vol is not None and low_vol.continuation_probability is not None and high_vol.continuation_probability is not None:
            low_edge = low_vol.continuation_probability - high_vol.continuation_probability
        candidates.append(
            FactorCandidate(
                key="low_volatility_streak_quality",
                label="低波动连阳质量因子",
                direction_bias="bullish",
                sample_count=low_vol.sample_count,
                probability=low_vol.continuation_probability,
                edge_vs_reference=low_edge,
                adopt=(
                    low_vol.sample_count >= config.factor_min_samples
                    and low_edge is not None
                    and low_edge >= config.factor_min_edge
                ),
                rationale="低波环境里的连阳更接近稳定趋势推进，可作为顺势信号的加分项。",
            )
        )

    if high_vol is not None:
        edge_vs_baseline = None
        if high_vol.continuation_probability is not None and baseline_probability is not None:
            edge_vs_baseline = high_vol.continuation_probability - baseline_probability
        candidates.append(
            FactorCandidate(
                key="high_volatility_streak_noise",
                label="高波动连阳噪音因子",
                direction_bias="caution",
                sample_count=high_vol.sample_count,
                probability=high_vol.continuation_probability,
                edge_vs_reference=edge_vs_baseline,
                adopt=(
                    high_vol.sample_count >= config.factor_min_samples
                    and edge_vs_baseline is not None
                    and edge_vs_baseline <= ZERO
                ),
                rationale="高波动环境下的连阳更像情绪脉冲，通常不适合直接追涨。",
            )
        )

    micro_or_small = [
        item
        for item in (pullback_by_bucket.get("micro"), pullback_by_bucket.get("small"))
        if item is not None and item.continue_down_5d_probability is not None
    ]
    if micro_or_small:
        best_reentry = min(micro_or_small, key=lambda item: item.continue_down_5d_probability or ONE)
        probability = best_reentry.continue_down_5d_probability
        candidates.append(
            FactorCandidate(
                key="post_streak_small_pullback_reentry",
                label="连阳后轻回踩再入场因子",
                direction_bias="bullish",
                sample_count=best_reentry.sample_count,
                probability=probability,
                edge_vs_reference=None if probability is None else ONE - probability,
                adopt=(
                    best_reentry.sample_count >= config.factor_min_samples
                    and probability is not None
                    and probability <= Decimal("0.50")
                ),
                rationale="轻微回踩后的下跌延续概率若不高，说明它更适合作为回踩再入场线索，而不是反转确认。",
            )
        )

    return candidates


def _build_snapshot(
    candles: list[Candle],
    support_ma: list[Decimal | None],
    volatility_values: list[Decimal | None],
    bounds: tuple[Decimal | None, Decimal | None],
    config: MarketAnalysisConfig,
) -> MarketAnalysisSnapshot:
    if not candles:
        return MarketAnalysisSnapshot(
            as_of_ts=None,
            last_close=None,
            current_bullish_streak=0,
            last_completed_bullish_streak=None,
            latest_pullback_bucket=None,
            latest_support_break=None,
            latest_volatility_regime="unknown",
        )

    latest_volatility_regime = _volatility_regime(volatility_values[-1], bounds)
    current_bullish_streak = _current_bullish_streak(candles, config)
    latest_pullback_bucket: str | None = None
    latest_support_break: bool | None = None
    last_completed_bullish_streak: int | None = None

    if _is_negative_bar(candles, len(candles) - 1, config):
        previous_streak = _bullish_streak_before_last_bearish(candles, config)
        if previous_streak >= config.first_bearish_min_streak:
            last_completed_bullish_streak = previous_streak
            streak_start_index = len(candles) - 1 - previous_streak
            latest_pullback_bucket = _pullback_bucket(
                _close_to_close_pullback(candles[-2].close, candles[-1].close),
                config,
            )
            latest_support_break = _classify_support_break(
                candles,
                support_ma,
                index=len(candles) - 1,
                streak_start_index=streak_start_index,
                config=config,
            )

    return MarketAnalysisSnapshot(
        as_of_ts=candles[-1].ts,
        last_close=candles[-1].close,
        current_bullish_streak=current_bullish_streak,
        last_completed_bullish_streak=last_completed_bullish_streak,
        latest_pullback_bucket=latest_pullback_bucket,
        latest_support_break=latest_support_break,
        latest_volatility_regime=latest_volatility_regime,
    )


def _derive_active_factors(
    snapshot: MarketAnalysisSnapshot,
    config: MarketAnalysisConfig,
) -> list[ActiveFactor]:
    active_factors: list[ActiveFactor] = []
    streak = snapshot.current_bullish_streak
    regime = snapshot.latest_volatility_regime

    if 5 <= streak <= 6 and regime == "low":
        active_factors.append(
            ActiveFactor(
                key="streak_momentum_peak_low_volatility",
                label="低波动5-6连阳顺势",
                direction_bias="bullish",
                score=Decimal("0.85"),
                reason="当前处于低波环境且连阳推进到5-6根，属于相对稳健的趋势延续窗口。",
            )
        )
    elif 2 <= streak <= 3 and regime == "low":
        active_factors.append(
            ActiveFactor(
                key="streak_momentum_building_low_volatility",
                label="低波动2-3连阳观察",
                direction_bias="bullish",
                score=Decimal("0.58"),
                reason="低波环境里的2-3连阳惯性开始建立，但更适合作为持仓确认而非激进追涨。",
            )
        )

    if streak >= config.exhaustion_bucket_start:
        active_factors.append(
            ActiveFactor(
                key="streak_exhaustion_ge_7",
                label="极端连阳衰减预警",
                direction_bias="caution",
                score=Decimal("0.76"),
                reason="连阳已经进入统计上的衰减区间，继续追涨的安全边际在收缩。",
            )
        )

    if streak > 0 and regime == "high":
        active_factors.append(
            ActiveFactor(
                key="high_volatility_streak_caution",
                label="高波动连阳谨慎",
                direction_bias="caution",
                score=Decimal("0.64"),
                reason="当前高波动环境削弱了连阳结构的可持续性，更像情绪脉冲而非稳定趋势。",
            )
        )

    if (
        snapshot.last_completed_bullish_streak is not None
        and snapshot.latest_pullback_bucket in {"micro", "small"}
        and snapshot.latest_support_break is False
    ):
        active_factors.append(
            ActiveFactor(
                key="post_streak_pullback_support_hold",
                label="连阳后健康回踩",
                direction_bias="bullish",
                score=Decimal("0.72"),
                reason="连阳后的第一根阴线属于轻回踩且支撑未破，更偏向二次上攻前的整理。",
            )
        )

    if (
        snapshot.last_completed_bullish_streak is not None
        and (
            snapshot.latest_support_break is True
            or snapshot.latest_pullback_bucket in {"medium", "large"}
        )
    ):
        active_factors.append(
            ActiveFactor(
                key="post_streak_breakdown",
                label="连阳后破位转弱",
                direction_bias="bearish",
                score=Decimal("0.88") if snapshot.latest_support_break else Decimal("0.74"),
                reason="连阳后的首根阴线已经表现出中大级别回撤或破位特征，趋势破坏风险显著上升。",
            )
        )

    return active_factors


def _build_methodology_notes(
    config: MarketAnalysisConfig,
    streak_stats: list[StreakContinuationStat],
    support_break_stats: list[SupportBreakStat],
) -> list[str]:
    if config.direction_mode == "close_to_close":
        direction_note = "连阳定义为连续收盘价高于前一根收盘价，延续概率用下一根收盘是否继续高于前收盘来计算。"
        support_note = "关键支撑默认采用双确认：跌破本轮连阳启动前收盘位，并且跌破"
    else:
        direction_note = "连阳定义为收盘价高于开盘价，延续概率用下一根K线是否继续收阳来计算。"
        support_note = "关键支撑默认采用双确认：跌破本轮连阳启动K线开盘价，并且跌破"
    notes = [
        direction_note,
        "连阳终结后的跟随效应，只统计至少连续两根阳线后的第一根阴线。",
        (
            support_note
            + f"{config.support_ma_period}日均线。"
        ),
        (
            "波动率环境默认采用"
            f"{config.volatility_period}日收盘收益率年化波动率，并按历史三分位划分低、中、高波动。"
        ),
    ]

    small_sample_labels = [
        item.streak_label
        for item in streak_stats
        if item.sample_count < config.factor_alert_min_samples
    ]
    if small_sample_labels:
        notes.append(
            "以下连阳分组样本偏少，建议只作为预警框架参考："
            + ", ".join(small_sample_labels)
            + "。"
        )

    support_small_samples = [
        item.support_status
        for item in support_break_stats
        if item.sample_count < config.factor_alert_min_samples
    ]
    if support_small_samples:
        notes.append(
            "以下支撑破坏分组样本偏少，适合作为风控提示，不宜直接机械交易："
            + ", ".join(support_small_samples)
            + "。"
        )
    return notes


def _annualized_volatility_series(
    candles: list[Candle],
    config: MarketAnalysisConfig,
) -> list[Decimal | None]:
    if not candles:
        return []

    returns: list[Decimal | None] = [None]
    for index in range(1, len(candles)):
        previous_close = candles[index - 1].close
        if previous_close <= 0:
            returns.append(None)
        else:
            returns.append((candles[index].close - previous_close) / previous_close)

    result: list[Decimal | None] = [None] * len(candles)
    annualization_multiplier = Decimal(str(math.sqrt(config.annualization_days)))
    for index in range(len(candles)):
        start = max(1, index - config.volatility_period + 1)
        window = [item for item in returns[start : index + 1] if item is not None]
        if len(window) < config.volatility_period:
            continue
        mean = sum(window, ZERO) / Decimal(len(window))
        variance = sum((item - mean) * (item - mean) for item in window) / Decimal(len(window))
        result[index] = variance.sqrt() * annualization_multiplier
    return result


def _volatility_regime_bounds(
    volatility_values: list[Decimal | None],
    config: MarketAnalysisConfig,
) -> tuple[Decimal | None, Decimal | None]:
    available = sorted(item for item in volatility_values if item is not None)
    if not available:
        return None, None
    return (
        _quantile(available, config.low_volatility_quantile),
        _quantile(available, config.high_volatility_quantile),
    )


def _volatility_regime(
    value: Decimal | None,
    bounds: tuple[Decimal | None, Decimal | None],
) -> VolatilityRegime:
    if value is None:
        return "unknown"
    low_bound, high_bound = bounds
    if low_bound is None or high_bound is None:
        return "unknown"
    if value <= low_bound:
        return "low"
    if value >= high_bound:
        return "high"
    return "medium"


def _simple_moving_average(values: list[Decimal], period: int) -> list[Decimal | None]:
    if period <= 0:
        raise ValueError("period must be positive")
    if not values:
        return []

    result: list[Decimal | None] = [None] * len(values)
    running_sum = ZERO
    for index, value in enumerate(values):
        running_sum += value
        if index >= period:
            running_sum -= values[index - period]
        if index >= period - 1:
            result[index] = running_sum / Decimal(period)
    return result


def _eligible_direction_indexes(
    candles: list[Candle],
    config: MarketAnalysisConfig,
) -> list[int]:
    if config.direction_mode == "close_to_close":
        return list(range(1, len(candles)))
    return list(range(len(candles)))


def _current_bullish_streak(candles: list[Candle], config: MarketAnalysisConfig) -> int:
    streak = 0
    for index in range(len(candles) - 1, -1, -1):
        if _is_positive_bar(candles, index, config):
            streak += 1
            continue
        break
    return streak


def _bullish_streak_before_last_bearish(candles: list[Candle], config: MarketAnalysisConfig) -> int:
    if len(candles) < 2 or not _is_negative_bar(candles, len(candles) - 1, config):
        return 0
    streak = 0
    for index in range(len(candles) - 2, -1, -1):
        if _is_positive_bar(candles, index, config):
            streak += 1
            continue
        break
    return streak


def _classify_support_break(
    candles: list[Candle],
    support_ma: list[Decimal | None],
    *,
    index: int,
    streak_start_index: int,
    config: MarketAnalysisConfig,
) -> bool | None:
    streak_support_level = _streak_support_level(candles, streak_start_index, config)
    broke_start_level = candles[index].close < streak_support_level
    moving_average = support_ma[index]
    if moving_average is None:
        if config.support_break_requires_dual_confirmation:
            return None
        return broke_start_level
    broke_moving_average = candles[index].close < moving_average
    if config.support_break_requires_dual_confirmation:
        return broke_start_level and broke_moving_average
    return broke_start_level or broke_moving_average


def _streak_support_level(
    candles: list[Candle],
    streak_start_index: int,
    config: MarketAnalysisConfig,
) -> Decimal:
    if config.direction_mode == "close_to_close" and streak_start_index > 0:
        return candles[streak_start_index - 1].close
    return candles[streak_start_index].open


def _pullback_bucket(value: Decimal, config: MarketAnalysisConfig) -> str:
    if value < config.pullback_micro_max:
        return "micro"
    if value < config.pullback_small_max:
        return "small"
    if value < config.pullback_medium_max:
        return "medium"
    return "large"


def _best_streak_stat(
    items: list[StreakContinuationStat | None],
    baseline_probability: Decimal | None,
    config: MarketAnalysisConfig,
) -> StreakContinuationStat | None:
    candidates = [
        item
        for item in items
        if item is not None
        and item.sample_count > 0
        and item.continuation_probability is not None
    ]
    if not candidates:
        return None
    if baseline_probability is None:
        return max(candidates, key=lambda item: item.continuation_probability or ZERO)
    return max(
        candidates,
        key=lambda item: (
            item.edge_vs_baseline is not None and item.edge_vs_baseline >= config.factor_min_edge,
            item.edge_vs_baseline or ZERO,
            item.sample_count,
        ),
    )


def _can_adopt_from_edge(
    sample_count: int,
    edge: Decimal | None,
    config: MarketAnalysisConfig,
) -> bool:
    return sample_count >= config.factor_min_samples and edge is not None and edge >= config.factor_min_edge


def _streak_label(streak_length: int, exhaustion_bucket_start: int) -> str:
    if streak_length >= exhaustion_bucket_start:
        return f"{exhaustion_bucket_start}+"
    return str(streak_length)


def _streak_sort_key(label: str, exhaustion_bucket_start: int) -> int:
    if label.endswith("+"):
        return exhaustion_bucket_start
    return int(label)


def _streak_insight(label: str, edge: Decimal | None) -> str:
    if edge is None:
        return "insufficient_data"
    if label.endswith("+") and edge <= Decimal("0.02"):
        return "late_streak_exhaustion"
    if edge < Decimal("0.01"):
        return "near_baseline"
    if edge < Decimal("0.03"):
        return "weak_inertia"
    if edge < Decimal("0.06"):
        return "momentum_building"
    return "strong_momentum"


def _pullback_insight(probability_5d: Decimal | None, bucket: str) -> str:
    if probability_5d is None:
        return "insufficient_data"
    if bucket in {"micro", "small"} and probability_5d <= Decimal("0.50"):
        return "healthy_pullback"
    if bucket == "medium" and probability_5d >= Decimal("0.60"):
        return "trend_damage"
    if bucket == "large" and probability_5d >= Decimal("0.70"):
        return "breakdown_risk"
    return "mixed_follow_through"


def _support_break_insight(status: str, probability_5d: Decimal | None) -> str:
    if probability_5d is None:
        return "insufficient_data"
    if status == "support_held" and probability_5d <= Decimal("0.40"):
        return "likely_false_break"
    if status == "support_broken" and probability_5d >= Decimal("0.65"):
        return "trend_break_confirmed"
    return "mixed_signal"


def _volatility_insight(
    regime: VolatilityRegime,
    continuation_probability: Decimal | None,
    config: MarketAnalysisConfig,
) -> str:
    if continuation_probability is None:
        return "insufficient_data"
    if regime == "low" and continuation_probability >= Decimal("0.50") + config.factor_min_edge:
        return "best_trend_quality"
    if regime == "high" and continuation_probability <= Decimal("0.50"):
        return "noise_prone"
    return "moderate_quality"


def _close_to_close_pullback(previous_close: Decimal, current_close: Decimal) -> Decimal:
    if previous_close <= 0:
        return ZERO
    decline = previous_close - current_close
    if decline <= 0:
        return ZERO
    return decline / previous_close


def _probability_from_outcomes(outcomes: list[int]) -> Decimal | None:
    if not outcomes:
        return None
    return Decimal(sum(outcomes)) / Decimal(len(outcomes))


def _ratio_or_none(numerator: int, denominator: int) -> Decimal | None:
    if denominator <= 0:
        return None
    return Decimal(numerator) / Decimal(denominator)


def _quantile(sorted_values: list[Decimal], quantile: Decimal) -> Decimal:
    if not sorted_values:
        raise ValueError("sorted_values must not be empty")
    if len(sorted_values) == 1:
        return sorted_values[0]

    position = float(quantile) * float(len(sorted_values) - 1)
    lower_index = int(math.floor(position))
    upper_index = int(math.ceil(position))
    if lower_index == upper_index:
        return sorted_values[lower_index]

    fraction = Decimal(str(position - lower_index))
    lower_value = sorted_values[lower_index]
    upper_value = sorted_values[upper_index]
    return lower_value + (upper_value - lower_value) * fraction


def _is_positive_bar(
    candles: list[Candle],
    index: int,
    config: MarketAnalysisConfig,
) -> bool:
    if index < 0 or index >= len(candles):
        return False
    if config.direction_mode == "close_to_close":
        return index > 0 and candles[index].close > candles[index - 1].close
    return candles[index].close > candles[index].open


def _is_negative_bar(
    candles: list[Candle],
    index: int,
    config: MarketAnalysisConfig,
) -> bool:
    if index < 0 or index >= len(candles):
        return False
    if config.direction_mode == "close_to_close":
        return index > 0 and candles[index].close < candles[index - 1].close
    return candles[index].close < candles[index].open


def _decimal_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value, "f")


def _safe_file_component(value: str) -> str:
    compact = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip())
    return compact or "report"
