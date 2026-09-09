from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Literal

from okx_quant.models import Instrument
from okx_quant.pricing import snap_to_increment

Direction = Literal["long", "short"]
EntryExecutionMode = Literal["limit", "market", "chase_best_quote"]
ManagementMode = Literal["fixed_tp", "trail_after_1r", "trail_after_2r", "trail_after_3r"]
OrderChannel = Literal["order", "algo"]
TriggerPriceType = Literal["last", "mark", "index"]

_DECIMAL_DISPLAY_INCREMENT = Decimal("0.00000001")
_USD_LIKE_CURRENCIES = {"USD", "USDT", "USDC"}


@dataclass(frozen=True)
class RRTradeSizing:
    requested_risk_amount: Decimal
    risk_per_contract: Decimal
    contract_size: Decimal
    base_size: Decimal | None
    notional_usdt: Decimal | None
    actual_risk_amount: Decimal

    def to_dict(self) -> dict[str, object]:
        return {
            "requested_risk_amount": _decimal_to_text(self.requested_risk_amount),
            "risk_per_contract": _decimal_to_text(self.risk_per_contract),
            "contract_size": _decimal_to_text(self.contract_size),
            "base_size": _decimal_to_text(self.base_size),
            "notional_usdt": _decimal_to_text(self.notional_usdt),
            "actual_risk_amount": _decimal_to_text(self.actual_risk_amount),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "RRTradeSizing":
        return cls(
            requested_risk_amount=_parse_decimal(payload.get("requested_risk_amount")) or Decimal("0"),
            risk_per_contract=_parse_decimal(payload.get("risk_per_contract")) or Decimal("0"),
            contract_size=_parse_decimal(payload.get("contract_size")) or Decimal("0"),
            base_size=_parse_decimal(payload.get("base_size")),
            notional_usdt=_parse_decimal(payload.get("notional_usdt")),
            actual_risk_amount=_parse_decimal(payload.get("actual_risk_amount")) or Decimal("0"),
        )


@dataclass(frozen=True)
class RRTradePlan:
    plan_id: str
    profile_name: str
    environment: str
    inst_id: str
    direction: Direction
    entry_execution_mode: EntryExecutionMode
    management_mode: ManagementMode
    trigger_price_type: TriggerPriceType
    risk_amount: Decimal
    entry_price: Decimal
    stop_loss_price: Decimal
    take_profit_price: Decimal
    management_trigger_price: Decimal | None
    direct_take_profit_r: Decimal
    round_trip_fee_rate: Decimal
    instrument_tick_size: Decimal
    instrument_lot_size: Decimal
    instrument_min_size: Decimal
    instrument_ct_val: Decimal | None
    instrument_ct_mult: Decimal | None
    instrument_ct_val_ccy: str | None
    instrument_settle_ccy: str | None
    sizing: RRTradeSizing
    created_at: datetime

    def to_dict(self) -> dict[str, object]:
        return {
            "plan_id": self.plan_id,
            "profile_name": self.profile_name,
            "environment": self.environment,
            "inst_id": self.inst_id,
            "direction": self.direction,
            "entry_execution_mode": self.entry_execution_mode,
            "management_mode": self.management_mode,
            "trigger_price_type": self.trigger_price_type,
            "risk_amount": _decimal_to_text(self.risk_amount),
            "entry_price": _decimal_to_text(self.entry_price),
            "stop_loss_price": _decimal_to_text(self.stop_loss_price),
            "take_profit_price": _decimal_to_text(self.take_profit_price),
            "management_trigger_price": _decimal_to_text(self.management_trigger_price),
            "direct_take_profit_r": _decimal_to_text(self.direct_take_profit_r),
            "round_trip_fee_rate": _decimal_to_text(self.round_trip_fee_rate),
            "instrument_tick_size": _decimal_to_text(self.instrument_tick_size),
            "instrument_lot_size": _decimal_to_text(self.instrument_lot_size),
            "instrument_min_size": _decimal_to_text(self.instrument_min_size),
            "instrument_ct_val": _decimal_to_text(self.instrument_ct_val),
            "instrument_ct_mult": _decimal_to_text(self.instrument_ct_mult),
            "instrument_ct_val_ccy": self.instrument_ct_val_ccy,
            "instrument_settle_ccy": self.instrument_settle_ccy,
            "sizing": self.sizing.to_dict(),
            "created_at": _datetime_to_text(self.created_at),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "RRTradePlan":
        sizing_payload = payload.get("sizing")
        return cls(
            plan_id=str(payload.get("plan_id", "") or "").strip(),
            profile_name=str(payload.get("profile_name", "") or "").strip(),
            environment=str(payload.get("environment", "") or "").strip(),
            inst_id=str(payload.get("inst_id", "") or "").strip().upper(),
            direction=_normalize_direction(payload.get("direction")),
            entry_execution_mode=_normalize_entry_execution_mode(payload.get("entry_execution_mode")),
            management_mode=_normalize_management_mode(payload.get("management_mode")),
            trigger_price_type=_normalize_trigger_price_type(payload.get("trigger_price_type")),
            risk_amount=_parse_decimal(payload.get("risk_amount")) or Decimal("0"),
            entry_price=_parse_decimal(payload.get("entry_price")) or Decimal("0"),
            stop_loss_price=_parse_decimal(payload.get("stop_loss_price")) or Decimal("0"),
            take_profit_price=_parse_decimal(payload.get("take_profit_price")) or Decimal("0"),
            management_trigger_price=_parse_decimal(payload.get("management_trigger_price")),
            direct_take_profit_r=_parse_decimal(payload.get("direct_take_profit_r")) or Decimal("0"),
            round_trip_fee_rate=_parse_decimal(payload.get("round_trip_fee_rate")) or Decimal("0"),
            instrument_tick_size=_parse_decimal(payload.get("instrument_tick_size")) or Decimal("0"),
            instrument_lot_size=_parse_decimal(payload.get("instrument_lot_size")) or Decimal("0"),
            instrument_min_size=_parse_decimal(payload.get("instrument_min_size")) or Decimal("0"),
            instrument_ct_val=_parse_decimal(payload.get("instrument_ct_val")),
            instrument_ct_mult=_parse_decimal(payload.get("instrument_ct_mult")),
            instrument_ct_val_ccy=_normalize_optional_text(payload.get("instrument_ct_val_ccy")),
            instrument_settle_ccy=_normalize_optional_text(payload.get("instrument_settle_ccy")),
            sizing=RRTradeSizing.from_dict(sizing_payload if isinstance(sizing_payload, dict) else {}),
            created_at=_parse_datetime(payload.get("created_at")) or datetime.now(timezone.utc),
        )


@dataclass(frozen=True)
class RRTradeOrderLink:
    role: str
    channel: OrderChannel
    order_id: str = ""
    algo_id: str = ""
    client_id: str = ""
    state: str = ""
    size: Decimal | None = None
    price: Decimal | None = None
    trigger_price: Decimal | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "role": self.role,
            "channel": self.channel,
            "order_id": self.order_id,
            "algo_id": self.algo_id,
            "client_id": self.client_id,
            "state": self.state,
            "size": _decimal_to_text(self.size),
            "price": _decimal_to_text(self.price),
            "trigger_price": _decimal_to_text(self.trigger_price),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object] | None) -> "RRTradeOrderLink | None":
        if not isinstance(payload, dict):
            return None
        return cls(
            role=str(payload.get("role", "") or "").strip(),
            channel="algo" if str(payload.get("channel", "") or "").strip().lower() == "algo" else "order",
            order_id=str(payload.get("order_id", "") or "").strip(),
            algo_id=str(payload.get("algo_id", "") or "").strip(),
            client_id=str(payload.get("client_id", "") or "").strip(),
            state=str(payload.get("state", "") or "").strip(),
            size=_parse_decimal(payload.get("size")),
            price=_parse_decimal(payload.get("price")),
            trigger_price=_parse_decimal(payload.get("trigger_price")),
        )


@dataclass(frozen=True)
class RRTradeEvent:
    occurred_at: datetime
    kind: str
    message: str

    def to_dict(self) -> dict[str, object]:
        return {
            "occurred_at": _datetime_to_text(self.occurred_at),
            "kind": self.kind,
            "message": self.message,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "RRTradeEvent":
        return cls(
            occurred_at=_parse_datetime(payload.get("occurred_at")) or datetime.now(timezone.utc),
            kind=str(payload.get("kind", "") or "").strip(),
            message=str(payload.get("message", "") or ""),
        )


@dataclass(frozen=True)
class RRTradeLedgerEntry:
    entry_id: str
    status: str
    plan: RRTradePlan
    entry_order: RRTradeOrderLink | None = None
    stop_loss_order: RRTradeOrderLink | None = None
    take_profit_order: RRTradeOrderLink | None = None
    filled_size: Decimal = Decimal("0")
    remaining_size: Decimal | None = None
    events: tuple[RRTradeEvent, ...] = ()
    created_at: datetime | None = None
    updated_at: datetime | None = None

    def to_dict(self) -> dict[str, object]:
        created_at = self.created_at or self.plan.created_at
        updated_at = self.updated_at or created_at
        return {
            "entry_id": self.entry_id,
            "status": self.status,
            "plan": self.plan.to_dict(),
            "entry_order": self.entry_order.to_dict() if self.entry_order else None,
            "stop_loss_order": self.stop_loss_order.to_dict() if self.stop_loss_order else None,
            "take_profit_order": self.take_profit_order.to_dict() if self.take_profit_order else None,
            "filled_size": _decimal_to_text(self.filled_size),
            "remaining_size": _decimal_to_text(self.remaining_size),
            "events": [item.to_dict() for item in self.events],
            "created_at": _datetime_to_text(created_at),
            "updated_at": _datetime_to_text(updated_at),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "RRTradeLedgerEntry":
        plan_payload = payload.get("plan")
        return cls(
            entry_id=str(payload.get("entry_id", "") or "").strip(),
            status=str(payload.get("status", "") or "").strip(),
            plan=RRTradePlan.from_dict(plan_payload if isinstance(plan_payload, dict) else {}),
            entry_order=RRTradeOrderLink.from_dict(payload.get("entry_order") if isinstance(payload.get("entry_order"), dict) else None),
            stop_loss_order=RRTradeOrderLink.from_dict(payload.get("stop_loss_order") if isinstance(payload.get("stop_loss_order"), dict) else None),
            take_profit_order=RRTradeOrderLink.from_dict(payload.get("take_profit_order") if isinstance(payload.get("take_profit_order"), dict) else None),
            filled_size=_parse_decimal(payload.get("filled_size")) or Decimal("0"),
            remaining_size=_parse_decimal(payload.get("remaining_size")),
            events=tuple(
                RRTradeEvent.from_dict(item)
                for item in payload.get("events", []) or []
                if isinstance(item, dict)
            ),
            created_at=_parse_datetime(payload.get("created_at")),
            updated_at=_parse_datetime(payload.get("updated_at")),
        )


def build_rr_trade_plan(
    *,
    plan_id: str,
    profile_name: str,
    environment: str,
    instrument: Instrument,
    direction: Direction,
    entry_execution_mode: EntryExecutionMode,
    management_mode: ManagementMode,
    trigger_price_type: TriggerPriceType,
    risk_amount: Decimal,
    entry_price: Decimal,
    stop_loss_price: Decimal,
    direct_take_profit_r: Decimal,
    round_trip_fee_rate: Decimal,
    created_at: datetime | None = None,
) -> RRTradePlan:
    if instrument.inst_type != "SWAP":
        raise ValueError("RR trade plan currently supports SWAP instruments only")
    if risk_amount <= 0:
        raise ValueError("risk_amount must be positive")
    if entry_price <= 0 or stop_loss_price <= 0:
        raise ValueError("entry_price and stop_loss_price must be positive")
    normalized_direction = _normalize_direction(direction)
    if normalized_direction == "long" and stop_loss_price >= entry_price:
        raise ValueError("long trades require stop_loss_price below entry_price")
    if normalized_direction == "short" and stop_loss_price <= entry_price:
        raise ValueError("short trades require stop_loss_price above entry_price")
    if instrument.lot_size <= 0 or instrument.min_size <= 0:
        raise ValueError("instrument lot_size and min_size must be positive")

    risk_per_contract = abs(entry_price - stop_loss_price) * _instrument_price_delta_multiplier(instrument)
    if risk_per_contract <= 0:
        raise ValueError("risk_per_contract must be positive")

    raw_contract_size = risk_amount / risk_per_contract
    contract_size = snap_to_increment(raw_contract_size, instrument.lot_size, "down")
    if contract_size < instrument.min_size:
        raise ValueError("risk_amount is too small for the instrument minimum size")

    actual_risk_amount = contract_size * risk_per_contract
    base_size = _snap_optional_display(_estimate_base_exposure(instrument, contract_size, entry_price))
    notional_usdt = _snap_optional_display(_estimate_notional_usd(instrument, contract_size, entry_price))

    normalized_mode = _normalize_management_mode(management_mode)
    tp_r = Decimal("1") if normalized_mode == "fixed_tp" else direct_take_profit_r
    if tp_r <= 0:
        raise ValueError("direct_take_profit_r must be positive")
    management_trigger_r = _management_trigger_r(normalized_mode)
    if management_trigger_r is not None and direct_take_profit_r < management_trigger_r:
        raise ValueError("direct_take_profit_r must be greater than or equal to the management trigger R")

    tick_size = instrument.tick_size if instrument.tick_size > 0 else Decimal("0.1")
    take_profit_price = _target_price(
        entry_price=entry_price,
        stop_loss_price=stop_loss_price,
        direction=normalized_direction,
        r_multiplier=tp_r,
        tick_size=tick_size,
        round_trip_fee_rate=round_trip_fee_rate,
    )
    management_trigger_price = None
    if management_trigger_r is not None:
        management_trigger_price = _target_price(
            entry_price=entry_price,
            stop_loss_price=stop_loss_price,
            direction=normalized_direction,
            r_multiplier=management_trigger_r,
            tick_size=tick_size,
            round_trip_fee_rate=Decimal("0"),
        )

    return RRTradePlan(
        plan_id=str(plan_id or "").strip(),
        profile_name=str(profile_name or "").strip(),
        environment=str(environment or "").strip(),
        inst_id=instrument.inst_id,
        direction=normalized_direction,
        entry_execution_mode=_normalize_entry_execution_mode(entry_execution_mode),
        management_mode=normalized_mode,
        trigger_price_type=_normalize_trigger_price_type(trigger_price_type),
        risk_amount=risk_amount,
        entry_price=entry_price,
        stop_loss_price=stop_loss_price,
        take_profit_price=take_profit_price,
        management_trigger_price=management_trigger_price,
        direct_take_profit_r=direct_take_profit_r,
        round_trip_fee_rate=round_trip_fee_rate,
        instrument_tick_size=instrument.tick_size,
        instrument_lot_size=instrument.lot_size,
        instrument_min_size=instrument.min_size,
        instrument_ct_val=instrument.ct_val,
        instrument_ct_mult=instrument.ct_mult,
        instrument_ct_val_ccy=_normalize_optional_text(instrument.ct_val_ccy),
        instrument_settle_ccy=_normalize_optional_text(instrument.settle_ccy),
        sizing=RRTradeSizing(
            requested_risk_amount=risk_amount,
            risk_per_contract=risk_per_contract,
            contract_size=contract_size,
            base_size=base_size,
            notional_usdt=notional_usdt,
            actual_risk_amount=actual_risk_amount,
        ),
        created_at=created_at or datetime.now(timezone.utc),
    )


def _normalize_direction(value: object) -> Direction:
    return "short" if str(value or "").strip().lower() == "short" else "long"


def _normalize_entry_execution_mode(value: object) -> EntryExecutionMode:
    normalized = str(value or "").strip().lower()
    if normalized == "market":
        return "market"
    if normalized == "chase_best_quote":
        return "chase_best_quote"
    return "limit"


def _normalize_management_mode(value: object) -> ManagementMode:
    normalized = str(value or "").strip().lower()
    if normalized == "trail_after_3r":
        return "trail_after_3r"
    if normalized == "trail_after_2r":
        return "trail_after_2r"
    if normalized == "trail_after_1r":
        return "trail_after_1r"
    return "fixed_tp"


def _normalize_trigger_price_type(value: object) -> TriggerPriceType:
    normalized = str(value or "").strip().lower()
    if normalized == "mark":
        return "mark"
    if normalized == "index":
        return "index"
    return "last"


def _management_trigger_r(mode: ManagementMode) -> Decimal | None:
    if mode == "trail_after_1r":
        return Decimal("1")
    if mode == "trail_after_2r":
        return Decimal("2")
    if mode == "trail_after_3r":
        return Decimal("3")
    return None


def _instrument_base_currency(instrument: Instrument) -> str:
    for value in (instrument.uly, instrument.inst_family, instrument.inst_id):
        normalized = str(value or "").strip().upper()
        if normalized:
            return normalized.split("-", 1)[0]
    return ""


def _contract_value(instrument: Instrument) -> Decimal | None:
    if instrument.ct_val is None or instrument.ct_val <= 0:
        return None
    multiplier = instrument.ct_mult if instrument.ct_mult is not None and instrument.ct_mult > 0 else Decimal("1")
    return instrument.ct_val * multiplier


def _instrument_price_delta_multiplier(instrument: Instrument) -> Decimal:
    settle_ccy = str(instrument.settle_ccy or "").strip().upper()
    if settle_ccy not in {"USDT", "USDC"}:
        return Decimal("1")
    contract_value = _contract_value(instrument)
    return contract_value if contract_value is not None and contract_value > 0 else Decimal("1")


def _estimate_base_exposure(
    instrument: Instrument,
    contract_size: Decimal,
    reference_price: Decimal | None,
) -> Decimal | None:
    base_ccy = _instrument_base_currency(instrument)
    ct_val_ccy = str(instrument.ct_val_ccy or "").strip().upper()
    contract_value = _contract_value(instrument)
    if contract_value is None:
        return None
    if ct_val_ccy == base_ccy:
        return contract_size * contract_value
    if ct_val_ccy in _USD_LIKE_CURRENCIES and reference_price is not None and reference_price > 0:
        return (contract_size * contract_value) / reference_price
    return None


def _estimate_notional_usd(
    instrument: Instrument,
    contract_size: Decimal,
    reference_price: Decimal | None,
) -> Decimal | None:
    base_ccy = _instrument_base_currency(instrument)
    ct_val_ccy = str(instrument.ct_val_ccy or "").strip().upper()
    contract_value = _contract_value(instrument)
    if contract_value is None:
        return None
    if ct_val_ccy in _USD_LIKE_CURRENCIES:
        return contract_size * contract_value
    if ct_val_ccy == base_ccy and reference_price is not None and reference_price > 0:
        return contract_size * contract_value * reference_price
    return None


def _target_price(
    *,
    entry_price: Decimal,
    stop_loss_price: Decimal,
    direction: Direction,
    r_multiplier: Decimal,
    tick_size: Decimal,
    round_trip_fee_rate: Decimal,
) -> Decimal:
    risk_distance = abs(entry_price - stop_loss_price) * r_multiplier
    fee_buffer = entry_price * max(round_trip_fee_rate, Decimal("0")) * Decimal("2")
    if direction == "short":
        candidate = entry_price - risk_distance - fee_buffer
        return snap_to_increment(candidate, tick_size, "down")
    candidate = entry_price + risk_distance + fee_buffer
    return snap_to_increment(candidate, tick_size, "up")


def _snap_optional_display(value: Decimal | None) -> Decimal | None:
    if value is None:
        return None
    return snap_to_increment(value, _DECIMAL_DISPLAY_INCREMENT, "nearest")


def _parse_decimal(value: object) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value).strip())
    except (InvalidOperation, ValueError):
        return None


def _decimal_to_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value, "f")


def _parse_datetime(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _datetime_to_text(value: datetime) -> str:
    normalized = value.astimezone(timezone.utc)
    return normalized.isoformat(timespec="seconds").replace("+00:00", "Z")


def _normalize_optional_text(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None
