from __future__ import annotations

from decimal import Decimal


def fmt_decimal(value: Decimal | None, places: int | None = None) -> str:
    if value is None:
        return "-"
    if places is None:
        text = format(value, "f")
        if "." in text:
            text = text.rstrip("0").rstrip(".")
        return text or "0"
    return f"{value:.{places}f}"
