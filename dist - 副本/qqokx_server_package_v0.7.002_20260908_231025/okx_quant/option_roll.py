from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from bisect import bisect_left
from typing import Literal

from okx_quant.models import Instrument
from okx_quant.okx_client import OkxPosition
from okx_quant.option_strategy import (
    OptionQuote,
    StrategyLegDefinition,
    estimate_leg_greeks,
    infer_implied_volatility_for_leg,
    option_contract_value,
    option_intrinsic_value_at_expiry,
    parse_option_contract,
    parse_option_expiry_datetime,
    resolve_strategy_leg,
)


RollPreference = Literal["credit", "risk", "delta", "time_value", "near_expiry"]
RollStrikeScope = Literal["safer_preferred", "same_preferred", "same_and_safer", "all"]


@dataclass(frozen=True)
class OptionRollSuggestion:
    current_inst_id: str
    current_expiry_code: str
    current_strike: Decimal
    new_inst_id: str
    new_expiry_code: str
    new_expiry_label: str
    new_strike: Decimal
    option_type: Literal["C", "P"]
    days_to_expiry: int
    close_price: Decimal
    close_price_source: str
    open_price: Decimal
    open_price_source: str
    net_credit: Decimal
    current_delta: Decimal | None
    new_delta: Decimal | None
    current_time_value: Decimal | None
    new_time_value: Decimal | None
    candidate_bid: Decimal | None
    candidate_ask: Decimal | None
    candidate_mark: Decimal | None
    price_gap: Decimal | None
    spread_ratio: Decimal | None
    risk_change: str
    roll_type: str
    reason: str
    score: Decimal


@dataclass(frozen=True)
class OptionRollTransferPayload:
    strategy_name: str
    option_family: str
    expiry_code: str
    legs: tuple[StrategyLegDefinition, ...]
    instruments: tuple[Instrument, ...]
    quotes: tuple[OptionQuote, ...]


def is_short_option_position(position: OkxPosition) -> bool:
    pos_side = position.pos_side.strip().lower()
    if pos_side == "short":
        return True
    if pos_side == "long":
        return False
    return position.position < 0


def build_option_roll_suggestions(
    *,
    current_position: OkxPosition,
    current_instrument: Instrument,
    current_quote: OptionQuote,
    candidate_instruments: list[Instrument],
    candidate_quotes_by_inst_id: dict[str, OptionQuote],
    settlement_price: Decimal,
    valuation_time: datetime,
    preference: RollPreference,
    strike_scope: RollStrikeScope,
    preferred_strike_levels: int | None,
    max_results: int,
) -> list[OptionRollSuggestion]:
    parsed_current = parse_option_contract(current_position.inst_id)
    quantity = abs(current_position.position)
    if quantity <= 0 or settlement_price <= 0:
        return []

    close_price, close_source = _short_close_reference(current_quote)
    if close_price is None:
        return []

    current_delta = _estimate_short_position_delta(
        instrument=current_instrument,
        price=current_quote.reference_price or close_price,
        quantity=quantity,
        settlement_price=settlement_price,
        valuation_time=valuation_time,
    )
    current_time_value = _time_value(
        price=current_quote.reference_price or close_price,
        settlement_price=settlement_price,
        strike=parsed_current.strike,
        option_type=parsed_current.option_type,
        contract_value=option_contract_value(current_instrument),
    )
    current_expiry_dt = parse_option_expiry_datetime(parsed_current.expiry_code)
    strike_levels_by_expiry: dict[tuple[str, str], list[Decimal]] = {}
    for instrument in candidate_instruments:
        parsed_bucket = parse_option_contract(instrument.inst_id)
        if parsed_bucket.inst_family != parsed_current.inst_family:
            continue
        if parsed_bucket.option_type != parsed_current.option_type:
            continue
        key = (parsed_bucket.expiry_code, parsed_bucket.option_type)
        levels = strike_levels_by_expiry.setdefault(key, [])
        if parsed_bucket.strike not in levels:
            levels.append(parsed_bucket.strike)
    for levels in strike_levels_by_expiry.values():
        levels.sort()

    suggestions: list[OptionRollSuggestion] = []
    for instrument in candidate_instruments:
        if instrument.inst_id == current_position.inst_id:
            continue
        parsed_candidate = parse_option_contract(instrument.inst_id)
        if parsed_candidate.inst_family != parsed_current.inst_family:
            continue
        if parsed_candidate.option_type != parsed_current.option_type:
            continue
        candidate_expiry_dt = parse_option_expiry_datetime(parsed_candidate.expiry_code)
        if candidate_expiry_dt <= current_expiry_dt:
            continue
        if not _strike_scope_matches(parsed_current, parsed_candidate, strike_scope):
            continue

        quote = candidate_quotes_by_inst_id.get(instrument.inst_id)
        if quote is None:
            continue
        if quote.bid_price is None or quote.bid_price <= 0:
            continue
        open_price, open_source = _short_open_reference(quote)
        if open_price is None:
            continue

        candidate_delta = _estimate_short_position_delta(
            instrument=instrument,
            price=quote.reference_price or open_price,
            quantity=quantity,
            settlement_price=settlement_price,
            valuation_time=valuation_time,
        )
        candidate_time_value = _time_value(
            price=quote.reference_price or open_price,
            settlement_price=settlement_price,
            strike=parsed_candidate.strike,
            option_type=parsed_candidate.option_type,
            contract_value=option_contract_value(instrument),
        )
        net_credit = (
            open_price * option_contract_value(instrument)
            - close_price * option_contract_value(current_instrument)
        ) * quantity
        current_mark_price = current_position.mark_price or current_quote.mark_price
        price_gap = (
            quote.bid_price - current_mark_price
            if quote.bid_price is not None and current_mark_price is not None
            else None
        )
        spread_ratio = _spread_ratio(quote)
        risk_change = _risk_change_label(parsed_current, parsed_candidate)
        roll_type = _roll_type_label(parsed_current, parsed_candidate)
        delta_match_score = _delta_match_score(current_delta, candidate_delta)
        reason = _build_reason(
            net_credit=net_credit,
            risk_change=risk_change,
            current_delta=current_delta,
            candidate_delta=candidate_delta,
            current_time_value=current_time_value,
            candidate_time_value=candidate_time_value,
        )
        score = _score_suggestion(
            preference=preference,
            net_credit=net_credit,
            risk_change=risk_change,
            current_delta=current_delta,
            candidate_delta=candidate_delta,
            delta_match_score=delta_match_score,
            current_time_value=current_time_value,
            candidate_time_value=candidate_time_value,
            spread_ratio=spread_ratio,
            parsed_current=parsed_current,
            parsed_candidate=parsed_candidate,
            candidate_expiry_dt=candidate_expiry_dt,
            valuation_time=valuation_time,
            strike_levels_by_expiry=strike_levels_by_expiry,
            preferred_strike_levels=preferred_strike_levels,
        )
        suggestions.append(
            OptionRollSuggestion(
                current_inst_id=current_position.inst_id,
                current_expiry_code=parsed_current.expiry_code,
                current_strike=parsed_current.strike,
                new_inst_id=instrument.inst_id,
                new_expiry_code=parsed_candidate.expiry_code,
                new_expiry_label=parsed_candidate.expiry_label,
                new_strike=parsed_candidate.strike,
                option_type=parsed_candidate.option_type,
                days_to_expiry=max((candidate_expiry_dt.date() - valuation_time.date()).days, 0),
                close_price=close_price,
                close_price_source=close_source,
                open_price=open_price,
                open_price_source=open_source,
                net_credit=net_credit,
                current_delta=current_delta,
                new_delta=candidate_delta,
                current_time_value=current_time_value,
                new_time_value=candidate_time_value,
                candidate_bid=quote.bid_price,
                candidate_ask=quote.ask_price,
                candidate_mark=quote.mark_price,
                price_gap=price_gap,
                spread_ratio=spread_ratio,
                risk_change=risk_change,
                roll_type=roll_type,
                reason=reason,
                score=score,
            )
        )

    suggestions.sort(
        key=lambda item: (
            item.score,
            item.net_credit,
            Decimal(item.days_to_expiry),
        ),
        reverse=True,
    )
    return suggestions[: max(max_results, 1)]


def build_option_roll_transfer_payload(
    *,
    current_position: OkxPosition,
    current_instrument: Instrument,
    current_quote: OptionQuote,
    suggestion: OptionRollSuggestion,
    candidate_instrument: Instrument,
    candidate_quote: OptionQuote,
) -> OptionRollTransferPayload:
    quantity = abs(current_position.position)
    parsed_current = parse_option_contract(current_position.inst_id)
    parsed_candidate = parse_option_contract(candidate_instrument.inst_id)
    strategy_name = f"展期-{parsed_current.strike}-{parsed_candidate.strike}-{parsed_candidate.expiry_code}"
    legs = (
        StrategyLegDefinition(
            alias="L1",
            inst_id=current_position.inst_id,
            side="buy",
            quantity=quantity,
            premium=suggestion.close_price,
            enabled=True,
        ),
        StrategyLegDefinition(
            alias="L2",
            inst_id=candidate_instrument.inst_id,
            side="sell",
            quantity=quantity,
            premium=suggestion.open_price,
            enabled=True,
        ),
    )
    return OptionRollTransferPayload(
        strategy_name=strategy_name,
        option_family=parsed_current.inst_family,
        expiry_code=suggestion.new_expiry_code,
        legs=legs,
        instruments=(current_instrument, candidate_instrument),
        quotes=(current_quote, candidate_quote),
    )


def _short_close_reference(quote: OptionQuote) -> tuple[Decimal | None, str]:
    if quote.ask_price is not None and quote.ask_price > 0:
        return quote.ask_price, "卖一价"
    if quote.mark_price is not None and quote.mark_price > 0:
        return quote.mark_price, "标记价"
    if quote.last_price is not None and quote.last_price > 0:
        return quote.last_price, "最新价"
    if quote.bid_price is not None and quote.bid_price > 0:
        return quote.bid_price, "买一价"
    return None, "-"


def _short_open_reference(quote: OptionQuote) -> tuple[Decimal | None, str]:
    if quote.bid_price is not None and quote.bid_price > 0:
        return quote.bid_price, "买一价"
    if quote.mark_price is not None and quote.mark_price > 0:
        return quote.mark_price, "标记价"
    if quote.last_price is not None and quote.last_price > 0:
        return quote.last_price, "最新价"
    if quote.ask_price is not None and quote.ask_price > 0:
        return quote.ask_price, "卖一价"
    return None, "-"


def _estimate_short_position_delta(
    *,
    instrument: Instrument,
    price: Decimal,
    quantity: Decimal,
    settlement_price: Decimal,
    valuation_time: datetime,
) -> Decimal | None:
    if price <= 0 or settlement_price <= 0:
        return None
    try:
        leg = resolve_strategy_leg(
            StrategyLegDefinition(alias="TMP", inst_id=instrument.inst_id, side="sell", quantity=quantity, premium=price),
            instrument=instrument,
            quote=OptionQuote(instrument=instrument, mark_price=price, bid_price=price, ask_price=price, last_price=price),
        )
        implied_vol = infer_implied_volatility_for_leg(
            leg,
            settlement_price=settlement_price,
            valuation_time=valuation_time,
        )
        if implied_vol is None:
            return None
        greeks = estimate_leg_greeks(
            leg,
            settlement_price=settlement_price,
            valuation_time=valuation_time,
            implied_volatility=implied_vol,
        )
        return greeks.delta
    except Exception:
        return None


def _time_value(
    *,
    price: Decimal,
    settlement_price: Decimal,
    strike: Decimal,
    option_type: Literal["C", "P"],
    contract_value: Decimal,
) -> Decimal | None:
    if price <= 0 or settlement_price <= 0:
        return None
    intrinsic = option_intrinsic_value_at_expiry(
        option_type=option_type,
        strike=strike,
        settlement_price=settlement_price,
        contract_value=contract_value,
    )
    time_value = price - intrinsic
    return time_value if time_value > 0 else Decimal("0")


def _strike_scope_matches(parsed_current, parsed_candidate, strike_scope: RollStrikeScope) -> bool:
    if strike_scope == "all":
        return True
    if strike_scope == "same_preferred":
        return parsed_candidate.strike == parsed_current.strike
    if strike_scope == "same_and_safer":
        if parsed_current.option_type == "C":
            return parsed_candidate.strike >= parsed_current.strike
        return parsed_candidate.strike <= parsed_current.strike
    if parsed_current.option_type == "C":
        return parsed_candidate.strike >= parsed_current.strike
    return parsed_candidate.strike <= parsed_current.strike


def _spread_ratio(quote: OptionQuote) -> Decimal | None:
    if quote.bid_price is None or quote.ask_price is None:
        return None
    if quote.bid_price <= 0 or quote.ask_price <= 0:
        return None
    mid = (quote.bid_price + quote.ask_price) / Decimal("2")
    if mid <= 0:
        return None
    return (quote.ask_price - quote.bid_price) / mid


def _risk_change_label(parsed_current, parsed_candidate) -> str:
    if parsed_current.option_type == "C":
        if parsed_candidate.strike > parsed_current.strike:
            return "风险下降"
        if parsed_candidate.strike < parsed_current.strike:
            return "风险上升"
    else:
        if parsed_candidate.strike < parsed_current.strike:
            return "风险下降"
        if parsed_candidate.strike > parsed_current.strike:
            return "风险上升"
    return "风险接近"


def _roll_type_label(parsed_current, parsed_candidate) -> str:
    if parsed_candidate.strike == parsed_current.strike:
        return "同执行价展期"
    if parsed_current.option_type == "C":
        return "上移执行价展期" if parsed_candidate.strike > parsed_current.strike else "下移执行价展期"
    return "下移执行价展期" if parsed_candidate.strike < parsed_current.strike else "上移执行价展期"


def _delta_match_score(current_delta: Decimal | None, candidate_delta: Decimal | None) -> Decimal:
    if current_delta is None or candidate_delta is None:
        return Decimal("0")
    return Decimal("1") / (Decimal("1") + abs(candidate_delta - current_delta))


def _build_reason(
    *,
    net_credit: Decimal,
    risk_change: str,
    current_delta: Decimal | None,
    candidate_delta: Decimal | None,
    current_time_value: Decimal | None,
    candidate_time_value: Decimal | None,
) -> str:
    parts: list[str] = []
    parts.append(f"{'净收' if net_credit >= 0 else '净付'}权利金 {net_credit:.6f}")
    parts.append(risk_change)
    if current_delta is not None and candidate_delta is not None:
        parts.append(f"Delta {current_delta:.4f}->{candidate_delta:.4f}")
    if current_time_value is not None and candidate_time_value is not None:
        parts.append(f"时间价值 {current_time_value:.6f}->{candidate_time_value:.6f}")
    return " | ".join(parts)


def _score_suggestion(
    *,
    preference: RollPreference,
    net_credit: Decimal,
    risk_change: str,
    current_delta: Decimal | None,
    candidate_delta: Decimal | None,
    delta_match_score: Decimal,
    current_time_value: Decimal | None,
    candidate_time_value: Decimal | None,
    spread_ratio: Decimal | None,
    parsed_current,
    parsed_candidate,
    candidate_expiry_dt: datetime,
    valuation_time: datetime,
    strike_levels_by_expiry: dict[tuple[str, str], list[Decimal]],
    preferred_strike_levels: int | None,
) -> Decimal:
    score = Decimal("0")
    if net_credit > 0:
        score += net_credit * Decimal("10")
    else:
        score += net_credit * Decimal("4")

    if risk_change == "风险下降":
        score += Decimal("6")
    elif risk_change == "风险接近":
        score += Decimal("2")
    else:
        score -= Decimal("5")

    if current_time_value is not None and candidate_time_value is not None:
        score += (candidate_time_value - current_time_value) * Decimal("12")

    if current_delta is not None and candidate_delta is not None:
        score += delta_match_score * Decimal("5")
        if preference == "risk" and abs(candidate_delta) < abs(current_delta):
            score += Decimal("5")
        if preference == "delta":
            score += delta_match_score * Decimal("8")

    if spread_ratio is not None:
        score -= spread_ratio * Decimal("8")

    extra_days = max((candidate_expiry_dt.date() - valuation_time.date()).days, 0)
    score += Decimal(extra_days) / Decimal("10")

    if preference == "credit":
        score += net_credit * Decimal("6")
    elif preference == "risk":
        if risk_change == "风险下降":
            score += Decimal("8")
    elif preference == "time_value" and current_time_value is not None and candidate_time_value is not None:
        score += (candidate_time_value - current_time_value) * Decimal("15")
    elif preference == "near_expiry":
        score -= Decimal(extra_days) / Decimal("3")

    if parsed_current.option_type == "C" and parsed_candidate.strike > parsed_current.strike:
        score += Decimal("1")
    if parsed_current.option_type == "P" and parsed_candidate.strike < parsed_current.strike:
        score += Decimal("1")
    score += _preferred_strike_bonus(
        parsed_current=parsed_current,
        parsed_candidate=parsed_candidate,
        strike_levels_by_expiry=strike_levels_by_expiry,
        preferred_strike_levels=preferred_strike_levels,
    )
    return score


def _preferred_strike_bonus(
    *,
    parsed_current,
    parsed_candidate,
    strike_levels_by_expiry: dict[tuple[str, str], list[Decimal]],
    preferred_strike_levels: int | None,
) -> Decimal:
    if preferred_strike_levels is None or preferred_strike_levels <= 0:
        return Decimal("0")
    levels = strike_levels_by_expiry.get((parsed_candidate.expiry_code, parsed_candidate.option_type))
    if not levels:
        return Decimal("0")
    directional_steps = _directional_strike_steps(
        current_strike=parsed_current.strike,
        candidate_strike=parsed_candidate.strike,
        option_type=parsed_candidate.option_type,
        levels=levels,
    )
    if directional_steps is None or directional_steps < 0 or directional_steps > preferred_strike_levels:
        return Decimal("0")
    remaining = preferred_strike_levels - directional_steps + 1
    return Decimal(remaining) * Decimal("1.5")


def _directional_strike_steps(
    *,
    current_strike: Decimal,
    candidate_strike: Decimal,
    option_type: Literal["C", "P"],
    levels: list[Decimal],
) -> int | None:
    if not levels:
        return None
    candidate_index = None
    for index, strike in enumerate(levels):
        if strike == candidate_strike:
            candidate_index = index
            break
    if candidate_index is None:
        return None
    current_index = bisect_left(levels, current_strike)
    if option_type == "C":
        return candidate_index - current_index
    return current_index - candidate_index
