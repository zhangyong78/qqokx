from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import math
from typing import Mapping


@dataclass(frozen=True)
class LineAnnotation:
    kind: str
    label: str
    bar_a: float
    bar_b: float
    price_a: Decimal
    price_b: Decimal
    x1: float = 0.0
    y1: float = 0.0
    x2: float = 0.0
    y2: float = 0.0
    color: str = "#1d4ed8"
    desk_ray_action: str = "notify"
    desk_ray_triggered: bool = False
    desk_ray_submit_pending: bool = False
    desk_ray_last_side: int | None = None
    locked: bool = False


@dataclass(frozen=True)
class ChartGeometry:
    plot_left: float
    plot_top: float
    plot_width: float
    plot_height: float
    first_bar: float
    last_bar: float
    min_price: Decimal
    max_price: Decimal


@dataclass(frozen=True)
class HitTarget:
    kind: str
    index: int


@dataclass(frozen=True)
class RiskRewardAnnotation:
    rr_id: str
    side: str
    bar_entry: float
    bar_stop: float
    price_entry: Decimal
    price_stop: Decimal
    price_tp: Decimal
    r_multiple: Decimal = Decimal("2")
    locked: bool = False


def decimal_to_text(value: Decimal) -> str:
    if not value.is_finite():
        return "0"
    text = format(value, "f")
    if "." not in text:
        return text
    return text.rstrip("0").rstrip(".") or "0"


def parse_decimal(value: object, default: Decimal = Decimal("0")) -> Decimal:
    if value is None:
        return default
    if isinstance(value, Decimal):
        return value if value.is_finite() else default
    text = str(value).strip()
    if not text:
        return default
    try:
        parsed = Decimal(text)
    except (InvalidOperation, ValueError):
        return default
    if not parsed.is_finite():
        return default
    return parsed


def rr_target(side: str, entry: Decimal, stop: Decimal, rr: Decimal) -> Decimal:
    return compute_rr_target(side, entry, stop, rr)


def bar_price_to_scene(geometry: ChartGeometry, bar: float, price: Decimal) -> tuple[float, float]:
    bar_span = max(geometry.last_bar - geometry.first_bar, 1.0)
    price_span = max(geometry.max_price - geometry.min_price, Decimal("0.00000001"))
    bar_ratio = (float(bar) - geometry.first_bar) / bar_span
    price_ratio = float((price - geometry.min_price) / price_span)
    x = geometry.plot_left + geometry.plot_width * bar_ratio
    y = geometry.plot_top + geometry.plot_height * (1.0 - price_ratio)
    return (round(x, 6), round(y, 6))


def scene_to_bar_price(geometry: ChartGeometry, x: float, y: float) -> tuple[float, Decimal]:
    clamped_x = _clamp(float(x), geometry.plot_left, geometry.plot_left + geometry.plot_width)
    clamped_y = _clamp(float(y), geometry.plot_top, geometry.plot_top + geometry.plot_height)
    x_ratio = _ratio(clamped_x, geometry.plot_left, geometry.plot_left + geometry.plot_width)
    y_ratio = _ratio(clamped_y, geometry.plot_top, geometry.plot_top + geometry.plot_height)
    bar = geometry.first_bar + (geometry.last_bar - geometry.first_bar) * x_ratio
    price_span = geometry.max_price - geometry.min_price
    price = geometry.max_price - price_span * Decimal(str(y_ratio))
    return (bar, price)


def nearest_line_hit(
    geometry: ChartGeometry,
    lines: list[LineAnnotation],
    *,
    x: float,
    y: float,
    tolerance: float,
) -> HitTarget | None:
    nearest: tuple[float, HitTarget] | None = None
    for index, line in enumerate(lines):
        if line.locked:
            continue
        candidates = (
            ("line_endpoint_a", line.bar_a, line.price_a),
            ("line_endpoint_b", line.bar_b, line.price_b),
        )
        for kind, bar, price in candidates:
            endpoint_x, endpoint_y = bar_price_to_scene(geometry, bar, price)
            distance = math.hypot(endpoint_x - x, endpoint_y - y)
            if distance <= tolerance and (nearest is None or distance < nearest[0]):
                nearest = (distance, HitTarget(kind=kind, index=index))
    return None if nearest is None else nearest[1]


def nearest_rr_hit(
    geometry: ChartGeometry,
    rr_items: list[RiskRewardAnnotation],
    *,
    x: float,
    y: float,
    tolerance: float,
) -> HitTarget | None:
    nearest: tuple[float, HitTarget] | None = None
    for index, rr_item in enumerate(rr_items):
        if rr_item.locked:
            continue
        candidates = (
            ("rr_entry", rr_item.bar_entry, rr_item.price_entry),
            ("rr_stop", rr_item.bar_stop, rr_item.price_stop),
            ("rr_tp", rr_item.bar_entry, rr_item.price_tp),
        )
        for kind, bar, price in candidates:
            point_x, point_y = bar_price_to_scene(geometry, bar, price)
            distance = math.hypot(point_x - x, point_y - y)
            if distance <= tolerance and (nearest is None or distance < nearest[0]):
                nearest = (distance, HitTarget(kind=kind, index=index))
    return None if nearest is None else nearest[1]


def drag_rr_annotation(annotation: RiskRewardAnnotation, handle: str, new_price: Decimal) -> RiskRewardAnnotation:
    minimum_gap = Decimal("0.000001")
    side = annotation.side.strip().lower()
    price_entry = annotation.price_entry
    price_stop = annotation.price_stop
    price_tp = annotation.price_tp
    r_multiple = annotation.r_multiple

    if side == "long":
        if handle == "rr_entry":
            delta = new_price - price_entry
            price_entry += delta
            price_stop += delta
            price_tp += delta
        elif handle == "rr_stop":
            price_stop = min(new_price, price_entry - minimum_gap)
            risk = price_entry - price_stop
            if risk > 0:
                price_tp = price_entry + r_multiple * risk
        elif handle == "rr_tp":
            price_tp = max(new_price, price_entry + minimum_gap)
            risk = price_entry - price_stop
            if risk > 0:
                r_multiple = (price_tp - price_entry) / risk
    elif side == "short":
        if handle == "rr_entry":
            delta = new_price - price_entry
            price_entry += delta
            price_stop += delta
            price_tp += delta
        elif handle == "rr_stop":
            price_stop = max(new_price, price_entry + minimum_gap)
            risk = price_stop - price_entry
            if risk > 0:
                price_tp = price_entry - r_multiple * risk
        elif handle == "rr_tp":
            price_tp = min(new_price, price_entry - minimum_gap)
            risk = price_stop - price_entry
            if risk > 0:
                r_multiple = (price_entry - price_tp) / risk
    else:
        raise ValueError(f"unsupported side: {annotation.side!r}")

    return RiskRewardAnnotation(
        rr_id=annotation.rr_id,
        side=annotation.side,
        bar_entry=annotation.bar_entry,
        bar_stop=annotation.bar_stop,
        price_entry=price_entry,
        price_stop=price_stop,
        price_tp=price_tp,
        r_multiple=r_multiple,
        locked=annotation.locked,
    )


def drag_line_annotation(annotation: LineAnnotation, handle: str, *, new_bar: float, new_price: Decimal) -> LineAnnotation:
    kind = annotation.kind.strip().lower()
    bar_a = float(annotation.bar_a)
    bar_b = float(annotation.bar_b)
    price_a = annotation.price_a
    price_b = annotation.price_b

    if kind in {"horizontal", "stop"}:
        price_a = new_price
        price_b = new_price
    else:
        if handle == "line_endpoint_a":
            bar_a = float(new_bar)
            price_a = new_price
        elif handle == "line_endpoint_b":
            bar_b = float(new_bar)
            price_b = new_price
        if bar_a > bar_b:
            bar_a, bar_b = bar_b, bar_a
            price_a, price_b = price_b, price_a

    return LineAnnotation(
        kind=annotation.kind,
        label=annotation.label,
        bar_a=bar_a,
        bar_b=bar_b,
        price_a=price_a,
        price_b=price_b,
        x1=annotation.x1,
        y1=annotation.y1,
        x2=annotation.x2,
        y2=annotation.y2,
        color=annotation.color,
        desk_ray_action=annotation.desk_ray_action,
        desk_ray_triggered=annotation.desk_ray_triggered,
        desk_ray_submit_pending=annotation.desk_ray_submit_pending,
        desk_ray_last_side=annotation.desk_ray_last_side,
        locked=annotation.locked,
    )


def compute_rr_target(side: str, entry: Decimal, stop: Decimal, r_multiple: Decimal) -> Decimal:
    if r_multiple <= 0:
        raise ValueError("r_multiple must be positive")
    normalized_side = side.strip().lower()
    if normalized_side in {"long", "buy"}:
        risk = entry - stop
        if risk <= 0:
            raise ValueError("long stop must be below entry")
        return entry + risk * r_multiple
    if normalized_side in {"short", "sell"}:
        risk = stop - entry
        if risk <= 0:
            raise ValueError("short stop must be above entry")
        return entry - risk * r_multiple
    raise ValueError(f"unsupported side: {side!r}")


def line_annotation_to_payload(annotation: LineAnnotation) -> dict[str, object]:
    return {
        "kind": annotation.kind,
        "x1": annotation.x1,
        "y1": annotation.y1,
        "x2": annotation.x2,
        "y2": annotation.y2,
        "label": annotation.label,
        "bar_a": annotation.bar_a,
        "bar_b": annotation.bar_b,
        "price_a": decimal_to_text(annotation.price_a),
        "price_b": decimal_to_text(annotation.price_b),
        "color": annotation.color,
        "desk_ray_action": annotation.desk_ray_action,
        "desk_ray_triggered": annotation.desk_ray_triggered,
        "desk_ray_submit_pending": False,
        "desk_ray_last_side": annotation.desk_ray_last_side,
        "locked": annotation.locked,
    }


def line_annotation_from_payload(payload: Mapping[str, object]) -> LineAnnotation:
    price_a = parse_decimal(payload.get("price_a"))
    return LineAnnotation(
        kind=str(payload.get("kind", "") or ""),
        label=str(payload.get("label", "") or ""),
        bar_a=_parse_float(payload.get("bar_a")),
        bar_b=_parse_float(payload.get("bar_b")),
        price_a=price_a,
        price_b=parse_decimal(payload.get("price_b"), price_a),
        x1=_parse_float(payload.get("x1")),
        y1=_parse_float(payload.get("y1")),
        x2=_parse_float(payload.get("x2")),
        y2=_parse_float(payload.get("y2")),
        color=str(payload.get("color", "#1d4ed8") or "#1d4ed8"),
        desk_ray_action=str(payload.get("desk_ray_action", payload.get("action", "notify")) or "notify"),
        desk_ray_triggered=_parse_bool(payload.get("desk_ray_triggered", payload.get("triggered"))),
        desk_ray_submit_pending=False,
        desk_ray_last_side=_parse_optional_int(payload.get("desk_ray_last_side")),
        locked=_parse_bool(payload.get("locked")),
    )


def rr_annotation_to_payload(annotation: RiskRewardAnnotation) -> dict[str, object]:
    return {
        "rr_id": annotation.rr_id,
        "side": annotation.side,
        "bar_entry": annotation.bar_entry,
        "bar_stop": annotation.bar_stop,
        "price_entry": decimal_to_text(annotation.price_entry),
        "price_stop": decimal_to_text(annotation.price_stop),
        "price_tp": decimal_to_text(annotation.price_tp),
        "r_multiple": decimal_to_text(annotation.r_multiple),
        "locked": annotation.locked,
    }


def risk_reward_to_payload(annotation: RiskRewardAnnotation) -> dict[str, object]:
    return rr_annotation_to_payload(annotation)


def rr_annotation_from_payload(payload: Mapping[str, object]) -> RiskRewardAnnotation:
    side = str(payload.get("side", "long") or "long").strip().lower()
    if side not in {"long", "short"}:
        side = "long"
    r_multiple = parse_decimal(payload.get("r_multiple", payload.get("rr")), Decimal("2"))
    if r_multiple <= 0:
        r_multiple = Decimal("2")
    return RiskRewardAnnotation(
        rr_id=str(payload.get("rr_id", "") or ""),
        side=side,
        bar_entry=_parse_float(payload.get("bar_entry")),
        bar_stop=_parse_float(payload.get("bar_stop")),
        price_entry=parse_decimal(payload.get("price_entry", payload.get("entry"))),
        price_stop=parse_decimal(payload.get("price_stop", payload.get("stop"))),
        price_tp=parse_decimal(payload.get("price_tp", payload.get("target"))),
        r_multiple=r_multiple,
        locked=_parse_bool(payload.get("locked")),
    )


def risk_reward_from_payload(payload: Mapping[str, object]) -> RiskRewardAnnotation:
    return rr_annotation_from_payload(payload)


def _parse_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return bool(value)


def _clamp(value: float, minimum: float, maximum: float) -> float:
    low = min(minimum, maximum)
    high = max(minimum, maximum)
    if not math.isfinite(value):
        return low
    return min(max(value, low), high)


def _ratio(value: float, minimum: float, maximum: float) -> float:
    span = maximum - minimum
    if span == 0:
        return 0.0
    return (value - minimum) / span


def _decimal_ratio(value: Decimal, minimum: Decimal, maximum: Decimal) -> float:
    span = maximum - minimum
    if span == 0:
        return 0.0
    return float((value - minimum) / span)


def _parse_float(value: object, default: float = 0.0) -> float:
    if value is None or value == "":
        return default
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    if not math.isfinite(parsed):
        return default
    return parsed


def _parse_optional_int(value: object) -> int | None:
    if value is None or value == "":
        return None
    try:
        parsed = int(value)
    except (OverflowError, TypeError, ValueError):
        return None
    return parsed
