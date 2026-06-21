from __future__ import annotations

from decimal import Decimal


def fmt_decimal(value: Decimal | None, places: int | None = None) -> str:
    if value is None:
        return "-"
    if places is None:
        normalized = value.normalize()
        return format(normalized, "f").rstrip("0").rstrip(".") or "0"
    return f"{value:.{places}f}"

