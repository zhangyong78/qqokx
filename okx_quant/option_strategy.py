from __future__ import annotations

import ast
import math
from datetime import datetime, timedelta
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Literal

from okx_quant.models import Candle, Instrument


OptionType = Literal["C", "P"]
LegSide = Literal["buy", "sell"]
TRADING_DAYS_PER_YEAR = Decimal("365")
SECONDS_PER_DAY = Decimal("86400")
MIN_SIMULATION_VOLATILITY = Decimal("0.0001")
MAX_SIMULATION_VOLATILITY = Decimal("8")
SIMULATION_VOL_TOLERANCE = Decimal("0.00000001")


@dataclass(frozen=True)
class ParsedOptionContract:
    inst_id: str
    inst_family: str
    expiry_code: str
    strike: Decimal
    option_type: OptionType

    @property
    def expiry_label(self) -> str:
        return format_option_expiry_label(self.expiry_code)


@dataclass(frozen=True)
class OptionQuote:
    instrument: Instrument
    mark_price: Decimal | None = None
    bid_price: Decimal | None = None
    ask_price: Decimal | None = None
    last_price: Decimal | None = None
    index_price: Decimal | None = None

    @property
    def parsed(self) -> ParsedOptionContract:
        return parse_option_contract(self.instrument.inst_id)

    @property
    def reference_price(self) -> Decimal | None:
        return self.mark_price or self.last_price or self.bid_price or self.ask_price


@dataclass(frozen=True)
class OptionChainRow:
    strike: Decimal
    call_quote: OptionQuote | None = None
    put_quote: OptionQuote | None = None


@dataclass
class StrategyLegDefinition:
    alias: str
    inst_id: str
    side: LegSide
    quantity: Decimal
    premium: Decimal | None = None
    delta: Decimal | None = None
    gamma: Decimal | None = None
    theta: Decimal | None = None
    vega: Decimal | None = None
    enabled: bool = True


@dataclass(frozen=True)
class ResolvedStrategyLeg:
    alias: str
    inst_id: str
    side: LegSide
    quantity: Decimal
    premium: Decimal
    inst_family: str
    expiry_code: str
    strike: Decimal
    option_type: OptionType
    contract_value: Decimal


@dataclass
class LinearFormula:
    coefficients: dict[str, Decimal]
    constant: Decimal = Decimal("0")


@dataclass(frozen=True)
class StrategyPayoffPoint:
    underlying_price: Decimal
    pnl: Decimal


@dataclass(frozen=True)
class StrategyPayoffSnapshot:
    points: tuple[StrategyPayoffPoint, ...]
    break_even_prices: tuple[Decimal, ...]
    net_premium: Decimal
    price_lower: Decimal
    price_upper: Decimal
    current_underlying_price: Decimal | None = None


def convert_payoff_snapshot_to_usdt(
    snapshot: StrategyPayoffSnapshot,
    *,
    reference_price: Decimal | None = None,
) -> StrategyPayoffSnapshot:
    converted_points = tuple(
        StrategyPayoffPoint(
            underlying_price=point.underlying_price,
            pnl=point.pnl * point.underlying_price,
        )
        for point in snapshot.points
    )
    effective_reference = reference_price if reference_price is not None else snapshot.current_underlying_price
    net_premium = snapshot.net_premium * effective_reference if effective_reference is not None else snapshot.net_premium
    return StrategyPayoffSnapshot(
        points=converted_points,
        break_even_prices=snapshot.break_even_prices,
        net_premium=net_premium,
        price_lower=snapshot.price_lower,
        price_upper=snapshot.price_upper,
        current_underlying_price=snapshot.current_underlying_price,
    )


def shift_candles(candles: list[Candle], *, offset: Decimal) -> list[Candle]:
    return [
        Candle(
            ts=item.ts,
            open=item.open + offset,
            high=item.high + offset,
            low=item.low + offset,
            close=item.close + offset,
            volume=item.volume,
            confirmed=item.confirmed,
        )
        for item in candles
    ]


def convert_candles_by_reference(candles: list[Candle], reference_candles: list[Candle]) -> list[Candle]:
    reference_map = {item.ts: item for item in reference_candles if item.confirmed}
    converted: list[Candle] = []
    for candle in candles:
        reference = reference_map.get(candle.ts)
        if reference is None:
            continue
        high_value = candle.high * reference.high
        low_value = candle.low * reference.low
        if high_value < low_value:
            high_value, low_value = low_value, high_value
        converted.append(
            Candle(
                ts=candle.ts,
                open=candle.open * reference.open,
                high=high_value,
                low=low_value,
                close=candle.close * reference.close,
                volume=Decimal("0"),
                confirmed=candle.confirmed and reference.confirmed,
            )
        )
    return converted


def parse_option_contract(inst_id: str) -> ParsedOptionContract:
    normalized = inst_id.strip().upper()
    parts = normalized.split("-")
    if len(parts) != 5:
        raise ValueError(f"Unsupported option contract id: {inst_id}")
    option_type = parts[4]
    if option_type not in {"C", "P"}:
        raise ValueError(f"Unsupported option side in contract id: {inst_id}")
    try:
        strike = Decimal(parts[3])
    except InvalidOperation as exc:
        raise ValueError(f"Unsupported option strike in contract id: {inst_id}") from exc
    return ParsedOptionContract(
        inst_id=normalized,
        inst_family=f"{parts[0]}-{parts[1]}",
        expiry_code=parts[2],
        strike=strike,
        option_type=option_type,
    )


def format_option_expiry_label(expiry_code: str) -> str:
    normalized = expiry_code.strip()
    if len(normalized) == 6 and normalized.isdigit():
        return f"20{normalized[:2]}-{normalized[2:4]}-{normalized[4:6]}"
    return normalized


def option_contract_value(instrument: Instrument) -> Decimal:
    ct_val = instrument.ct_val if instrument.ct_val is not None and instrument.ct_val > 0 else Decimal("1")
    ct_mult = instrument.ct_mult if instrument.ct_mult is not None and instrument.ct_mult > 0 else Decimal("1")
    value = ct_val * ct_mult
    return value if value > 0 else Decimal("1")


def build_option_chain_rows(quotes: list[OptionQuote]) -> list[OptionChainRow]:
    grouped: dict[Decimal, dict[str, OptionQuote]] = {}
    for quote in quotes:
        parsed = quote.parsed
        strike_bucket = grouped.setdefault(parsed.strike, {})
        strike_bucket[parsed.option_type] = quote
    rows = [
        OptionChainRow(
            strike=strike,
            call_quote=bucket.get("C"),
            put_quote=bucket.get("P"),
        )
        for strike, bucket in grouped.items()
    ]
    rows.sort(key=lambda item: item.strike)
    return rows


def resolve_strategy_leg(leg: StrategyLegDefinition, instrument: Instrument) -> ResolvedStrategyLeg:
    parsed = parse_option_contract(instrument.inst_id)
    premium = leg.premium
    if premium is None:
        raise ValueError(f"{instrument.inst_id} 缺少期权权利金，无法计算到期盈亏。")
    if leg.quantity <= 0:
        raise ValueError(f"{instrument.inst_id} 数量必须大于 0。")
    return ResolvedStrategyLeg(
        alias=leg.alias.strip(),
        inst_id=instrument.inst_id,
        side=leg.side,
        quantity=leg.quantity,
        premium=premium,
        inst_family=parsed.inst_family,
        expiry_code=parsed.expiry_code,
        strike=parsed.strike,
        option_type=parsed.option_type,
        contract_value=option_contract_value(instrument),
    )


def build_default_formula(legs: list[StrategyLegDefinition]) -> str:
    terms: list[str] = []
    for leg in legs:
        if not leg.enabled:
            continue
        coefficient = leg.quantity if leg.side == "buy" else -leg.quantity
        if coefficient == 0:
            continue
        alias = leg.alias.strip()
        if not alias:
            continue
        sign = "-" if coefficient < 0 else "+"
        abs_coefficient = abs(coefficient)
        if abs_coefficient == 1:
            term = alias
        else:
            term = f"{_format_decimal_plain(abs_coefficient)}*{alias}"
        if not terms:
            terms.append(term if sign == "+" else f"-{term}")
        else:
            terms.append(f"{sign} {term}")
    return " ".join(terms)


def parse_linear_formula(expression: str, *, allowed_names: set[str] | None = None) -> LinearFormula:
    raw = expression.strip()
    if not raw:
        raise ValueError("组合公式不能为空。")
    try:
        node = ast.parse(raw, mode="eval")
    except SyntaxError as exc:
        raise ValueError("组合公式语法错误。") from exc
    coefficients, constant = _parse_linear_node(node.body, allowed_names=allowed_names)
    normalized = {name: value for name, value in coefficients.items() if value != 0}
    return LinearFormula(coefficients=normalized, constant=constant)


def evaluate_linear_formula(
    expression: str | LinearFormula,
    values: dict[str, Decimal],
    *,
    allowed_names: set[str] | None = None,
) -> Decimal:
    formula = expression if isinstance(expression, LinearFormula) else parse_linear_formula(expression, allowed_names=allowed_names)
    total = formula.constant
    for name, coefficient in formula.coefficients.items():
        if name not in values:
            raise ValueError(f"组合公式引用了未知变量 {name}。")
        total += coefficient * values[name]
    return total


def build_composite_candles(
    expression: str,
    candles_by_alias: dict[str, list[Candle]],
    *,
    allowed_names: set[str] | None = None,
) -> list[Candle]:
    formula = parse_linear_formula(expression, allowed_names=allowed_names)
    aliases = [name for name, coefficient in formula.coefficients.items() if coefficient != 0]
    if not aliases:
        raise ValueError("组合公式至少需要引用一个合约别名。")

    candle_maps: dict[str, dict[int, Candle]] = {}
    common_ts: set[int] | None = None
    for alias in aliases:
        candles = candles_by_alias.get(alias, [])
        mapping = {item.ts: item for item in candles}
        if not mapping:
            raise ValueError(f"{alias} 缺少标记价格 K 线。")
        candle_maps[alias] = mapping
        timestamps = set(mapping.keys())
        common_ts = timestamps if common_ts is None else common_ts.intersection(timestamps)
    aligned_ts = sorted(common_ts or ())
    if not aligned_ts:
        raise ValueError("组合公式引用的合约没有可对齐的 K 线时间戳。")

    combined: list[Candle] = []
    for ts in aligned_ts:
        open_value = formula.constant
        close_value = formula.constant
        high_value = formula.constant
        low_value = formula.constant
        confirmed = True
        for alias, coefficient in formula.coefficients.items():
            candle = candle_maps[alias][ts]
            open_value += coefficient * candle.open
            close_value += coefficient * candle.close
            if coefficient >= 0:
                high_value += coefficient * candle.high
                low_value += coefficient * candle.low
            else:
                high_value += coefficient * candle.low
                low_value += coefficient * candle.high
            confirmed = confirmed and candle.confirmed
        if high_value < low_value:
            high_value, low_value = low_value, high_value
        combined.append(
            Candle(
                ts=ts,
                open=open_value,
                high=high_value,
                low=low_value,
                close=close_value,
                volume=Decimal("0"),
                confirmed=confirmed,
            )
        )
    return combined


def build_payoff_snapshot(
    legs: list[ResolvedStrategyLeg],
    *,
    current_underlying_price: Decimal | None = None,
    sample_count: int = 181,
) -> StrategyPayoffSnapshot:
    active_legs = [item for item in legs if item.quantity > 0]
    if not active_legs:
        raise ValueError("至少需要一条有效策略腿。")
    if sample_count < 2:
        raise ValueError("sample_count must be at least 2")

    strikes = [item.strike for item in active_legs]
    anchor = current_underlying_price if current_underlying_price is not None and current_underlying_price > 0 else strikes[len(strikes) // 2]
    low_anchor = min(strikes + [anchor])
    high_anchor = max(strikes + [anchor])
    price_lower = max((low_anchor * Decimal("0.7")), Decimal("0.0001"))
    price_upper = max(high_anchor * Decimal("1.3"), price_lower + Decimal("0.0001"))
    step = (price_upper - price_lower) / Decimal(sample_count - 1)

    net_premium = Decimal("0")
    for leg in active_legs:
        direction = Decimal("1") if leg.side == "buy" else Decimal("-1")
        premium_cost = leg.premium * leg.contract_value * leg.quantity
        net_premium += -direction * premium_cost

    points: list[StrategyPayoffPoint] = []
    for index in range(sample_count):
        underlying_price = price_lower + (step * Decimal(index))
        pnl = net_premium
        for leg in active_legs:
            direction = Decimal("1") if leg.side == "buy" else Decimal("-1")
            intrinsic = option_intrinsic_value_at_expiry(
                settlement_price=underlying_price,
                strike=leg.strike,
                option_type=leg.option_type,
                contract_value=leg.contract_value,
            )
            pnl += direction * intrinsic * leg.quantity
        points.append(StrategyPayoffPoint(underlying_price=underlying_price, pnl=pnl))

    break_even_prices = tuple(_estimate_break_even_prices(points))
    return StrategyPayoffSnapshot(
        points=tuple(points),
        break_even_prices=break_even_prices,
        net_premium=net_premium,
        price_lower=price_lower,
        price_upper=price_upper,
        current_underlying_price=current_underlying_price,
    )


def build_simulated_payoff_snapshot(
    legs: list[ResolvedStrategyLeg],
    *,
    implied_volatility_by_alias: dict[str, Decimal],
    valuation_time: datetime,
    volatility_shift: Decimal = Decimal("0"),
    current_underlying_price: Decimal | None = None,
    sample_count: int = 181,
) -> StrategyPayoffSnapshot:
    active_legs = [item for item in legs if item.quantity > 0]
    if not active_legs:
        raise ValueError("At least one strategy leg is required.")
    if sample_count < 2:
        raise ValueError("sample_count must be at least 2")

    strikes = [item.strike for item in active_legs]
    anchor = current_underlying_price if current_underlying_price is not None and current_underlying_price > 0 else strikes[len(strikes) // 2]
    low_anchor = min(strikes + [anchor])
    high_anchor = max(strikes + [anchor])
    price_lower = max((low_anchor * Decimal("0.7")), Decimal("0.0001"))
    price_upper = max(high_anchor * Decimal("1.3"), price_lower + Decimal("0.0001"))
    step = (price_upper - price_lower) / Decimal(sample_count - 1)

    net_premium = Decimal("0")
    for leg in active_legs:
        direction = Decimal("1") if leg.side == "buy" else Decimal("-1")
        premium_cost = leg.premium * leg.contract_value * leg.quantity
        net_premium += -direction * premium_cost

    points: list[StrategyPayoffPoint] = []
    for index in range(sample_count):
        underlying_price = price_lower + (step * Decimal(index))
        pnl = Decimal("0")
        for leg in active_legs:
            direction = Decimal("1") if leg.side == "buy" else Decimal("-1")
            entry_cost = leg.premium * leg.contract_value
            theoretical_value = simulated_option_value(
                settlement_price=underlying_price,
                leg=leg,
                valuation_time=valuation_time,
                base_implied_volatility=implied_volatility_by_alias.get(leg.alias),
                volatility_shift=volatility_shift,
            )
            pnl += direction * (theoretical_value - entry_cost) * leg.quantity
        points.append(StrategyPayoffPoint(underlying_price=underlying_price, pnl=pnl))

    break_even_prices = tuple(_estimate_break_even_prices(points))
    return StrategyPayoffSnapshot(
        points=tuple(points),
        break_even_prices=break_even_prices,
        net_premium=net_premium,
        price_lower=price_lower,
        price_upper=price_upper,
        current_underlying_price=current_underlying_price,
    )


def option_intrinsic_value_at_expiry(
    *,
    settlement_price: Decimal,
    strike: Decimal,
    option_type: OptionType,
    contract_value: Decimal,
) -> Decimal:
    if settlement_price <= 0:
        return Decimal("0")
    if option_type == "C":
        exercise_ratio = max(settlement_price - strike, Decimal("0")) / settlement_price
    else:
        exercise_ratio = max(strike - settlement_price, Decimal("0")) / settlement_price
    return contract_value * exercise_ratio


def simulated_option_value(
    *,
    settlement_price: Decimal,
    leg: ResolvedStrategyLeg,
    valuation_time: datetime,
    base_implied_volatility: Decimal | None,
    volatility_shift: Decimal = Decimal("0"),
) -> Decimal:
    time_to_expiry = option_time_to_expiry_years(leg.expiry_code, valuation_time=valuation_time)
    if time_to_expiry <= 0:
        return option_intrinsic_value_at_expiry(
            settlement_price=settlement_price,
            strike=leg.strike,
            option_type=leg.option_type,
            contract_value=leg.contract_value,
        )

    effective_volatility = _effective_simulation_volatility(base_implied_volatility, volatility_shift)
    return inverse_black_scholes_price(
        settlement_price=settlement_price,
        strike=leg.strike,
        option_type=leg.option_type,
        contract_value=leg.contract_value,
        time_to_expiry_years=time_to_expiry,
        volatility=effective_volatility,
    )


def infer_implied_volatility_for_leg(
    leg: ResolvedStrategyLeg,
    *,
    settlement_price: Decimal,
    valuation_time: datetime,
    option_price: Decimal | None = None,
) -> Decimal | None:
    reference_option_price = option_price if option_price is not None and option_price > 0 else leg.premium
    theoretical_price = reference_option_price * leg.contract_value
    time_to_expiry = option_time_to_expiry_years(leg.expiry_code, valuation_time=valuation_time)
    if theoretical_price <= 0:
        return MIN_SIMULATION_VOLATILITY
    if time_to_expiry <= 0 or settlement_price <= 0:
        return None
    return infer_inverse_implied_volatility(
        option_value=theoretical_price,
        settlement_price=settlement_price,
        strike=leg.strike,
        option_type=leg.option_type,
        contract_value=leg.contract_value,
        time_to_expiry_years=time_to_expiry,
    )


def estimate_strategy_greeks(
    legs: list[ResolvedStrategyLeg],
    *,
    implied_volatility_by_alias: dict[str, Decimal],
    valuation_time: datetime,
    settlement_price: Decimal,
) -> dict[str, Decimal]:
    totals = {
        "delta": Decimal("0"),
        "gamma": Decimal("0"),
        "theta": Decimal("0"),
        "vega": Decimal("0"),
    }
    active_legs = [item for item in legs if item.quantity > 0]
    if settlement_price <= 0 or not active_legs:
        return totals

    for leg in active_legs:
        greeks = estimate_leg_greeks(
            leg,
            settlement_price=settlement_price,
            valuation_time=valuation_time,
            base_implied_volatility=implied_volatility_by_alias.get(leg.alias),
        )
        direction = Decimal("1") if leg.side == "buy" else Decimal("-1")
        for key, value in greeks.items():
            totals[key] += direction * value * leg.quantity
    return totals


def estimate_leg_greeks(
    leg: ResolvedStrategyLeg,
    *,
    settlement_price: Decimal,
    valuation_time: datetime,
    base_implied_volatility: Decimal | None,
) -> dict[str, Decimal]:
    if settlement_price <= 0:
        return {
            "delta": Decimal("0"),
            "gamma": Decimal("0"),
            "theta": Decimal("0"),
            "vega": Decimal("0"),
        }

    base_value = simulated_option_value(
        settlement_price=settlement_price,
        leg=leg,
        valuation_time=valuation_time,
        base_implied_volatility=base_implied_volatility,
    )
    price_step = max(settlement_price * Decimal("0.005"), Decimal("1"))
    max_down_step = max(settlement_price - Decimal("0.0001"), Decimal("0"))
    price_step = min(price_step, max_down_step)
    if price_step <= 0:
        delta = Decimal("0")
        gamma = Decimal("0")
    else:
        up_value = simulated_option_value(
            settlement_price=settlement_price + price_step,
            leg=leg,
            valuation_time=valuation_time,
            base_implied_volatility=base_implied_volatility,
        )
        down_value = simulated_option_value(
            settlement_price=settlement_price - price_step,
            leg=leg,
            valuation_time=valuation_time,
            base_implied_volatility=base_implied_volatility,
        )
        denominator = price_step * Decimal("2")
        delta = (up_value - down_value) / denominator
        gamma = (up_value - (Decimal("2") * base_value) + down_value) / (price_step * price_step)

    vol_step = Decimal("0.01")
    vega = (
        simulated_option_value(
            settlement_price=settlement_price,
            leg=leg,
            valuation_time=valuation_time,
            base_implied_volatility=base_implied_volatility,
            volatility_shift=vol_step,
        )
        - simulated_option_value(
            settlement_price=settlement_price,
            leg=leg,
            valuation_time=valuation_time,
            base_implied_volatility=base_implied_volatility,
            volatility_shift=-vol_step,
        )
    ) / Decimal("2")
    theta = simulated_option_value(
        settlement_price=settlement_price,
        leg=leg,
        valuation_time=valuation_time + timedelta(days=1),
        base_implied_volatility=base_implied_volatility,
    ) - base_value
    return {
        "delta": delta,
        "gamma": gamma,
        "theta": theta,
        "vega": vega,
    }


def option_time_to_expiry_years(expiry_code: str, *, valuation_time: datetime) -> Decimal:
    expiry_time = parse_option_expiry_datetime(expiry_code)
    delta = expiry_time - valuation_time
    if delta.total_seconds() <= 0:
        return Decimal("0")
    return Decimal(str(delta.total_seconds())) / (TRADING_DAYS_PER_YEAR * SECONDS_PER_DAY)


def parse_option_expiry_datetime(expiry_code: str) -> datetime:
    normalized = expiry_code.strip()
    if len(normalized) != 6 or not normalized.isdigit():
        raise ValueError(f"Unsupported option expiry code: {expiry_code}")
    year = 2000 + int(normalized[:2])
    month = int(normalized[2:4])
    day = int(normalized[4:6])
    return datetime(year, month, day) + timedelta(days=1)


def infer_inverse_implied_volatility(
    *,
    option_value: Decimal,
    settlement_price: Decimal,
    strike: Decimal,
    option_type: OptionType,
    contract_value: Decimal,
    time_to_expiry_years: Decimal,
) -> Decimal | None:
    if option_value <= 0 or settlement_price <= 0 or strike <= 0 or contract_value <= 0 or time_to_expiry_years <= 0:
        return None

    intrinsic_value = option_intrinsic_value_at_expiry(
        settlement_price=settlement_price,
        strike=strike,
        option_type=option_type,
        contract_value=contract_value,
    )
    if option_value <= intrinsic_value + SIMULATION_VOL_TOLERANCE:
        return MIN_SIMULATION_VOLATILITY

    low = MIN_SIMULATION_VOLATILITY
    high = Decimal("1")
    high_value = inverse_black_scholes_price(
        settlement_price=settlement_price,
        strike=strike,
        option_type=option_type,
        contract_value=contract_value,
        time_to_expiry_years=time_to_expiry_years,
        volatility=high,
    )
    while high_value < option_value and high < MAX_SIMULATION_VOLATILITY:
        high *= Decimal("2")
        if high > MAX_SIMULATION_VOLATILITY:
            high = MAX_SIMULATION_VOLATILITY
        high_value = inverse_black_scholes_price(
            settlement_price=settlement_price,
            strike=strike,
            option_type=option_type,
            contract_value=contract_value,
            time_to_expiry_years=time_to_expiry_years,
            volatility=high,
        )
    if high_value < option_value:
        return None

    implied = high
    for _ in range(80):
        implied = (low + high) / Decimal("2")
        model_value = inverse_black_scholes_price(
            settlement_price=settlement_price,
            strike=strike,
            option_type=option_type,
            contract_value=contract_value,
            time_to_expiry_years=time_to_expiry_years,
            volatility=implied,
        )
        difference = model_value - option_value
        if abs(difference) <= SIMULATION_VOL_TOLERANCE:
            return implied
        if difference > 0:
            high = implied
        else:
            low = implied
    return implied


def inverse_black_scholes_price(
    *,
    settlement_price: Decimal,
    strike: Decimal,
    option_type: OptionType,
    contract_value: Decimal,
    time_to_expiry_years: Decimal,
    volatility: Decimal,
) -> Decimal:
    if settlement_price <= 0 or strike <= 0 or contract_value <= 0:
        return Decimal("0")
    if time_to_expiry_years <= 0 or volatility <= 0:
        return option_intrinsic_value_at_expiry(
            settlement_price=settlement_price,
            strike=strike,
            option_type=option_type,
            contract_value=contract_value,
        )

    settlement_float = float(settlement_price)
    strike_float = float(strike)
    time_float = float(time_to_expiry_years)
    volatility_float = float(volatility)
    sigma_root_t = volatility_float * math.sqrt(time_float)
    if sigma_root_t <= 0:
        return option_intrinsic_value_at_expiry(
            settlement_price=settlement_price,
            strike=strike,
            option_type=option_type,
            contract_value=contract_value,
        )

    d1 = (math.log(settlement_float / strike_float) + (0.5 * (volatility_float**2) * time_float)) / sigma_root_t
    d2 = d1 - sigma_root_t
    strike_ratio = strike_float / settlement_float
    if option_type == "C":
        option_ratio = _normal_cdf(d1) - (strike_ratio * _normal_cdf(d2))
    else:
        option_ratio = (strike_ratio * _normal_cdf(-d2)) - _normal_cdf(-d1)
    option_ratio = max(option_ratio, 0.0)
    return contract_value * Decimal(str(option_ratio))


def _effective_simulation_volatility(base_implied_volatility: Decimal | None, volatility_shift: Decimal) -> Decimal:
    base = base_implied_volatility if base_implied_volatility is not None and base_implied_volatility > 0 else MIN_SIMULATION_VOLATILITY
    shifted = base + volatility_shift
    if shifted < MIN_SIMULATION_VOLATILITY:
        return MIN_SIMULATION_VOLATILITY
    if shifted > MAX_SIMULATION_VOLATILITY:
        return MAX_SIMULATION_VOLATILITY
    return shifted


def _parse_linear_node(
    node: ast.AST,
    *,
    allowed_names: set[str] | None,
) -> tuple[dict[str, Decimal], Decimal]:
    if isinstance(node, ast.Name):
        if allowed_names is not None and node.id not in allowed_names:
            raise ValueError(f"组合公式里有未定义别名 {node.id}。")
        return {node.id: Decimal("1")}, Decimal("0")

    if isinstance(node, ast.Constant):
        return {}, _decimal_constant_from_node(node)

    if isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.UAdd, ast.USub)):
        coefficients, constant = _parse_linear_node(node.operand, allowed_names=allowed_names)
        factor = Decimal("-1") if isinstance(node.op, ast.USub) else Decimal("1")
        return _scale_linear_part(coefficients, constant, factor)

    if isinstance(node, ast.BinOp):
        left_coefficients, left_constant = _parse_linear_node(node.left, allowed_names=allowed_names)
        right_coefficients, right_constant = _parse_linear_node(node.right, allowed_names=allowed_names)

        if isinstance(node.op, ast.Add):
            return _combine_linear_parts(
                left_coefficients,
                left_constant,
                right_coefficients,
                right_constant,
                Decimal("1"),
            )
        if isinstance(node.op, ast.Sub):
            return _combine_linear_parts(
                left_coefficients,
                left_constant,
                right_coefficients,
                right_constant,
                Decimal("-1"),
            )
        if isinstance(node.op, ast.Mult):
            if left_coefficients and right_coefficients:
                raise ValueError("组合公式仅支持线性表达式，例如 L1 - 2*L2。")
            if left_coefficients:
                return _scale_linear_part(left_coefficients, left_constant, right_constant)
            if right_coefficients:
                return _scale_linear_part(right_coefficients, right_constant, left_constant)
            return {}, left_constant * right_constant
        if isinstance(node.op, ast.Div):
            if right_coefficients:
                raise ValueError("组合公式的除数不能引用合约别名。")
            if right_constant == 0:
                raise ValueError("组合公式里出现了除以 0。")
            factor = Decimal("1") / right_constant
            return _scale_linear_part(left_coefficients, left_constant, factor)

    raise ValueError("组合公式只支持 + - * / 和括号。")


def _decimal_constant_from_node(node: ast.Constant) -> Decimal:
    value = node.value
    if isinstance(value, bool) or value is None:
        raise ValueError("组合公式里只能使用数字常量。")
    try:
        return Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError("组合公式里只能使用数字常量。") from exc


def _scale_linear_part(
    coefficients: dict[str, Decimal],
    constant: Decimal,
    factor: Decimal,
) -> tuple[dict[str, Decimal], Decimal]:
    scaled = {name: value * factor for name, value in coefficients.items() if (value * factor) != 0}
    return scaled, constant * factor


def _combine_linear_parts(
    left_coefficients: dict[str, Decimal],
    left_constant: Decimal,
    right_coefficients: dict[str, Decimal],
    right_constant: Decimal,
    right_factor: Decimal,
) -> tuple[dict[str, Decimal], Decimal]:
    merged = dict(left_coefficients)
    for name, value in right_coefficients.items():
        merged[name] = merged.get(name, Decimal("0")) + (value * right_factor)
        if merged[name] == 0:
            merged.pop(name, None)
    return merged, left_constant + (right_constant * right_factor)


def _estimate_break_even_prices(points: list[StrategyPayoffPoint]) -> list[Decimal]:
    prices: list[Decimal] = []
    for previous, current in zip(points, points[1:]):
        if previous.pnl == 0:
            prices.append(previous.underlying_price)
            continue
        if current.pnl == 0:
            prices.append(current.underlying_price)
            continue
        if (previous.pnl > 0 and current.pnl < 0) or (previous.pnl < 0 and current.pnl > 0):
            delta_pnl = current.pnl - previous.pnl
            if delta_pnl == 0:
                continue
            ratio = -previous.pnl / delta_pnl
            price = previous.underlying_price + ((current.underlying_price - previous.underlying_price) * ratio)
            prices.append(price)
    deduped: list[Decimal] = []
    for price in prices:
        if any(abs(existing - price) <= Decimal("0.00000001") for existing in deduped):
            continue
        deduped.append(price)
    return deduped


def _normal_cdf(value: float) -> float:
    return 0.5 * (1.0 + math.erf(value / math.sqrt(2.0)))


def _format_decimal_plain(value: Decimal) -> str:
    normalized = value.normalize()
    text = format(normalized, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"
