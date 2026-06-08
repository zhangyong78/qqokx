from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from okx_quant.models import StrategyConfig
from okx_quant.pricing import format_decimal
from okx_quant.strategy_catalog import resolve_dynamic_signal_mode
from okx_quant.strategy_runtime_registry import get_strategy_runtime_profile


@dataclass(frozen=True)
class RiskRecommendation:
    suggested: Decimal
    best: Decimal | None = None


_EMA55_SLOPE_SHORT_RISK = {
    "BTC-USDT-SWAP": RiskRecommendation(Decimal("25"), Decimal("50")),
    "ETH-USDT-SWAP": RiskRecommendation(Decimal("2"), Decimal("5")),
    "SOL-USDT-SWAP": RiskRecommendation(Decimal("0.1"), Decimal("0.3")),
    "BNB-USDT-SWAP": RiskRecommendation(Decimal("30"), Decimal("90")),
    "DOGE-USDT-SWAP": RiskRecommendation(Decimal("0.001"), Decimal("0.001")),
}

_EMA_DYNAMIC_LONG_RISK = {
    "BTC-USDT-SWAP": RiskRecommendation(Decimal("10"), Decimal("15")),
    "ETH-USDT-SWAP": RiskRecommendation(Decimal("1"), Decimal("2")),
    "SOL-USDT-SWAP": RiskRecommendation(Decimal("0.1"), Decimal("0.1")),
    "BNB-USDT-SWAP": RiskRecommendation(Decimal("13"), Decimal("30")),
    "DOGE-USDT-SWAP": RiskRecommendation(Decimal("0.001"), Decimal("0.001")),
}


def recommended_minimum_risk_amount_for_strategy(
    strategy_id: str,
    inst_id: str,
    signal_mode: str,
) -> RiskRecommendation | None:
    normalized_inst_id = str(inst_id or "").strip().upper()
    if not normalized_inst_id:
        return None
    try:
        profile = get_strategy_runtime_profile(strategy_id)
    except KeyError:
        return None
    if profile.family == "ema55_slope_short":
        return _EMA55_SLOPE_SHORT_RISK.get(normalized_inst_id)
    effective_signal_mode = resolve_dynamic_signal_mode(strategy_id, signal_mode)
    if profile.family == "dynamic_order" and effective_signal_mode == "long_only":
        return _EMA_DYNAMIC_LONG_RISK.get(normalized_inst_id)
    return None


def recommended_minimum_risk_amount_for_config(config: StrategyConfig) -> RiskRecommendation | None:
    return recommended_minimum_risk_amount_for_strategy(
        config.strategy_id,
        config.inst_id,
        config.signal_mode,
    )


def format_risk_recommendation(recommendation: RiskRecommendation) -> str:
    suggested_text = format_decimal(recommendation.suggested)
    best = recommendation.best
    if best is None or best == recommendation.suggested:
        return f"建议 {suggested_text}U"
    return f"建议 {suggested_text}U，最佳 {format_decimal(best)}U"
