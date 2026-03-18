from __future__ import annotations

from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP


def snap_to_increment(value: Decimal, increment: Decimal, direction: str = "nearest") -> Decimal:
    if increment <= 0:
        raise ValueError("increment must be positive")

    units = value / increment
    if direction == "up":
        snapped_units = units.to_integral_value(rounding=ROUND_CEILING)
    elif direction == "down":
        snapped_units = units.to_integral_value(rounding=ROUND_FLOOR)
    elif direction == "nearest":
        snapped_units = units.to_integral_value(rounding=ROUND_HALF_UP)
    else:
        raise ValueError(f"Unsupported direction: {direction}")
    return snapped_units * increment


def format_decimal(value: Decimal) -> str:
    normalized = value.normalize()
    text = format(normalized, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def format_decimal_fixed(value: Decimal, places: int = 2) -> str:
    if places < 0:
        raise ValueError("places must be non-negative")
    quant = Decimal("1").scaleb(-places)
    rounded = value.quantize(quant, rounding=ROUND_HALF_UP)
    return format(rounded, f".{places}f")
