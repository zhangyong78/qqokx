from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

from okx_quant.persistence import trader_desk_file_path


TRADER_DRAFT_STATUS_VALUES: tuple[str, ...] = ("draft", "ready", "paused")
TRADER_RUN_STATUS_VALUES: tuple[str, ...] = (
    "idle",
    "running",
    "paused_manual",
    "paused_loss",
    "quota_exhausted",
    "stopped",
)
TRADER_GATE_CONDITION_VALUES: tuple[str, ...] = ("always", "above", "below", "between")
TRADER_SLOT_STATUS_VALUES: tuple[str, ...] = (
    "watching",
    "open",
    "closed_profit",
    "closed_loss",
    "closed_manual",
    "stopped",
    "failed",
)


@dataclass
class TraderPriceGate:
    enabled: bool = False
    condition: str = "always"
    trigger_inst_id: str = ""
    trigger_price_type: str = "mark"
    lower_price: Decimal | None = None
    upper_price: Decimal | None = None


@dataclass
class TraderDraftRecord:
    trader_id: str
    template_payload: dict[str, object]
    total_quota: Decimal
    unit_quota: Decimal
    quota_steps: int
    auto_restart_on_profit: bool = True
    pause_on_stop_loss: bool = True
    status: str = "draft"
    notes: str = ""
    gate: TraderPriceGate = field(default_factory=TraderPriceGate)
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)


@dataclass
class TraderRunState:
    trader_id: str
    status: str = "idle"
    paused_reason: str = ""
    armed_session_id: str = ""
    last_started_at: datetime | None = None
    last_event_at: datetime | None = None
    updated_at: datetime | None = None


@dataclass
class TraderSlotRecord:
    slot_id: str
    trader_id: str
    session_id: str
    api_name: str
    strategy_name: str
    symbol: str
    bar: str = ""
    direction_label: str = ""
    status: str = "watching"
    quota_occupied: bool = False
    created_at: datetime = field(default_factory=datetime.now)
    opened_at: datetime | None = None
    closed_at: datetime | None = None
    released_at: datetime | None = None
    entry_price: Decimal | None = None
    exit_price: Decimal | None = None
    size: Decimal | None = None
    net_pnl: Decimal | None = None
    close_reason: str = ""
    history_record_id: str = ""
    note: str = ""
    pending_manual_exit_mode: str = ""
    pending_manual_exit_inst_id: str = ""
    pending_manual_exit_order_id: str = ""
    pending_manual_exit_cl_ord_id: str = ""


@dataclass
class TraderEventRecord:
    event_id: str
    trader_id: str
    created_at: datetime
    level: str
    message: str


@dataclass
class TraderDeskSnapshot:
    drafts: list[TraderDraftRecord] = field(default_factory=list)
    runs: list[TraderRunState] = field(default_factory=list)
    slots: list[TraderSlotRecord] = field(default_factory=list)
    events: list[TraderEventRecord] = field(default_factory=list)


@dataclass
class TraderBookSummary:
    trader_count: int = 0
    profitable_trader_count: int = 0
    losing_trader_count: int = 0
    flat_trader_count: int = 0
    realized_count: int = 0
    win_count: int = 0
    loss_count: int = 0
    manual_count: int = 0
    net_pnl: Decimal = field(default_factory=lambda: Decimal("0"))


def _parse_time(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _parse_decimal(value: object, *, default: Decimal | None = None) -> Decimal | None:
    text = str(value or "").strip()
    if not text:
        return default
    try:
        return Decimal(text)
    except (InvalidOperation, ValueError):
        return default


def _normalize_decimal_text(value: Decimal) -> str:
    text = format(value.normalize(), "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _parse_positive_decimal(value: str, label: str) -> Decimal:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{label}不能为空。")
    try:
        parsed = Decimal(text)
    except InvalidOperation as exc:
        raise ValueError(f"{label}必须是数字。") from exc
    if parsed <= 0:
        raise ValueError(f"{label}必须大于 0。")
    return parsed


def normalize_trader_draft_inputs(
    *,
    total_quota: str,
    unit_quota: str,
    quota_steps: str,
    status: str,
    gate_enabled: bool,
    gate_condition: str,
    gate_trigger_inst_id: str,
    gate_trigger_price_type: str,
    gate_lower_price: str,
    gate_upper_price: str,
) -> dict[str, object]:
    total_value = _parse_positive_decimal(total_quota, "总额度")
    unit_value = _parse_positive_decimal(unit_quota, "固定数量")
    if unit_value > total_value:
        raise ValueError("固定数量不能大于总额度。")

    steps_text = str(quota_steps or "").strip()
    if not steps_text:
        raise ValueError("额度次数不能为空。")
    try:
        steps_value = int(steps_text)
    except ValueError as exc:
        raise ValueError("额度次数必须是正整数。") from exc
    if steps_value <= 0:
        raise ValueError("额度次数必须是正整数。")

    normalized_status = str(status or "").strip() or "draft"
    if normalized_status not in TRADER_DRAFT_STATUS_VALUES:
        raise ValueError("草稿状态无效。")

    normalized_gate_condition = str(gate_condition or "").strip() or "always"
    if normalized_gate_condition not in TRADER_GATE_CONDITION_VALUES:
        raise ValueError("价格开关条件无效。")

    normalized_gate_inst_id = str(gate_trigger_inst_id or "").strip().upper()
    normalized_gate_price_type = str(gate_trigger_price_type or "").strip() or "mark"
    lower_value = _parse_decimal(gate_lower_price)
    upper_value = _parse_decimal(gate_upper_price)

    if gate_enabled and normalized_gate_condition != "always" and not normalized_gate_inst_id:
        raise ValueError("启用价格开关后，请填写价格触发标的。")
    if gate_enabled and normalized_gate_condition == "above" and (lower_value is None or lower_value <= 0):
        raise ValueError("价格高于条件需要填写大于 0 的阈值。")
    if gate_enabled and normalized_gate_condition == "below" and (upper_value is None or upper_value <= 0):
        raise ValueError("价格低于条件需要填写大于 0 的阈值。")
    if gate_enabled and normalized_gate_condition == "between":
        if lower_value is None or lower_value <= 0 or upper_value is None or upper_value <= 0:
            raise ValueError("价格区间条件需要填写两个大于 0 的阈值。")
        if lower_value >= upper_value:
            raise ValueError("价格区间的下限必须小于上限。")

    return {
        "total_quota": total_value,
        "unit_quota": unit_value,
        "quota_steps": steps_value,
        "status": normalized_status,
        "gate": TraderPriceGate(
            enabled=bool(gate_enabled) and normalized_gate_condition != "always",
            condition=normalized_gate_condition,
            trigger_inst_id=normalized_gate_inst_id,
            trigger_price_type=normalized_gate_price_type,
            lower_price=lower_value,
            upper_price=upper_value,
        ),
    }


def trader_gate_allows_price(gate: TraderPriceGate, current_price: Decimal) -> bool:
    if not gate.enabled or gate.condition == "always":
        return True
    if gate.condition == "above":
        return gate.lower_price is not None and current_price >= gate.lower_price
    if gate.condition == "below":
        return gate.upper_price is not None and current_price <= gate.upper_price
    if gate.condition == "between":
        return (
            gate.lower_price is not None
            and gate.upper_price is not None
            and gate.lower_price <= current_price <= gate.upper_price
        )
    return True


def trader_slots_for(slots: list[TraderSlotRecord], trader_id: str) -> list[TraderSlotRecord]:
    return [item for item in slots if item.trader_id == trader_id]


def trader_open_slots(slots: list[TraderSlotRecord], trader_id: str) -> list[TraderSlotRecord]:
    return [
        item
        for item in trader_slots_for(slots, trader_id)
        if item.status == "open" and item.entry_price is not None and item.size is not None
    ]


def _trader_slot_counts_as_realized_close(slot: TraderSlotRecord) -> bool:
    return slot.closed_at is not None and slot.status in {"closed_profit", "closed_loss", "closed_manual"}


def _trader_slot_effective_net_pnl(slot: TraderSlotRecord) -> Decimal | None:
    if not _trader_slot_counts_as_realized_close(slot):
        return None
    return slot.net_pnl if slot.net_pnl is not None else Decimal("0")


def trader_used_quota_steps(slots: list[TraderSlotRecord], trader_id: str) -> int:
    return sum(1 for item in trader_slots_for(slots, trader_id) if item.quota_occupied)


def trader_remaining_quota_steps(draft: TraderDraftRecord, slots: list[TraderSlotRecord]) -> int:
    return max(draft.quota_steps - trader_used_quota_steps(slots, draft.trader_id), 0)


def trader_open_position_summary(slots: list[TraderSlotRecord], trader_id: str) -> tuple[int, Decimal | None, Decimal | None]:
    open_slots = trader_open_slots(slots, trader_id)
    if not open_slots:
        return 0, None, None
    total_size = sum((item.size or Decimal("0")) for item in open_slots)
    if total_size <= 0:
        return len(open_slots), None, total_size
    weighted_notional = sum((item.entry_price or Decimal("0")) * (item.size or Decimal("0")) for item in open_slots)
    average_entry = weighted_notional / total_size
    return len(open_slots), average_entry, total_size


def trader_realized_net_pnl(slots: list[TraderSlotRecord], trader_id: str) -> Decimal:
    return sum(
        (
            (_trader_slot_effective_net_pnl(item) or Decimal("0"))
            for item in trader_slots_for(slots, trader_id)
            if _trader_slot_counts_as_realized_close(item)
        ),
        Decimal("0"),
    )


def trader_realized_close_counts(slots: list[TraderSlotRecord], trader_id: str) -> tuple[int, int, int]:
    closed = [item for item in trader_slots_for(slots, trader_id) if _trader_slot_counts_as_realized_close(item)]
    wins = 0
    losses = 0
    for item in closed:
        if item.status == "closed_manual" and item.net_pnl is None:
            continue
        if (_trader_slot_effective_net_pnl(item) or Decimal("0")) > 0:
            wins += 1
        else:
            losses += 1
    return len(closed), wins, losses


def trader_realized_slots(slots: list[TraderSlotRecord], trader_id: str | None = None) -> list[TraderSlotRecord]:
    normalized_trader_id = str(trader_id or "").strip()
    realized = [
        item
        for item in slots
        if _trader_slot_counts_as_realized_close(item)
        and (not normalized_trader_id or item.trader_id == normalized_trader_id)
    ]
    realized.sort(
        key=lambda item: (
            item.closed_at or item.released_at or item.opened_at or item.created_at,
            item.slot_id,
        ),
        reverse=True,
    )
    return realized


def trader_book_summary(
    drafts: list[TraderDraftRecord],
    slots: list[TraderSlotRecord],
) -> TraderBookSummary:
    trader_ids = {draft.trader_id for draft in drafts}
    realized = trader_realized_slots(slots)
    trader_ids.update(slot.trader_id for slot in realized)
    profitable_trader_count = 0
    losing_trader_count = 0
    flat_trader_count = 0
    for trader_id in trader_ids:
        net_pnl = trader_realized_net_pnl(slots, trader_id)
        if net_pnl > 0:
            profitable_trader_count += 1
        elif net_pnl < 0:
            losing_trader_count += 1
        else:
            flat_trader_count += 1

    win_count = 0
    loss_count = 0
    manual_count = 0
    for slot in realized:
        if slot.status == "closed_manual":
            manual_count += 1
            if slot.net_pnl is None:
                continue
        effective_pnl = _trader_slot_effective_net_pnl(slot)
        if effective_pnl is None:
            continue
        if effective_pnl > 0:
            win_count += 1
        else:
            loss_count += 1

    return TraderBookSummary(
        trader_count=len(trader_ids),
        profitable_trader_count=profitable_trader_count,
        losing_trader_count=losing_trader_count,
        flat_trader_count=flat_trader_count,
        realized_count=len(realized),
        win_count=win_count,
        loss_count=loss_count,
        manual_count=manual_count,
        net_pnl=sum(
            ((_trader_slot_effective_net_pnl(item) or Decimal("0")) for item in realized),
            Decimal("0"),
        ),
    )


def trader_has_watching_slot(slots: list[TraderSlotRecord], trader_id: str) -> bool:
    return any(item.status == "watching" for item in trader_slots_for(slots, trader_id))


def load_trader_desk_snapshot(path: Path | None = None) -> TraderDeskSnapshot:
    target = path or trader_desk_file_path()
    if not target.exists():
        return TraderDeskSnapshot()
    payload = json.loads(target.read_text(encoding="utf-8"))
    if isinstance(payload, list):
        return TraderDeskSnapshot(drafts=[item for item in (_draft_from_legacy_payload(raw) for raw in payload) if item is not None])

    if not isinstance(payload, dict):
        return TraderDeskSnapshot()

    drafts = [item for item in (_draft_from_payload(raw) for raw in payload.get("drafts", [])) if item is not None]
    runs = [item for item in (_run_from_payload(raw) for raw in payload.get("runs", [])) if item is not None]
    slots = [item for item in (_slot_from_payload(raw) for raw in payload.get("slots", [])) if item is not None]
    events = [item for item in (_event_from_payload(raw) for raw in payload.get("events", [])) if item is not None]
    events.sort(key=lambda item: (item.created_at, item.event_id), reverse=True)
    return TraderDeskSnapshot(drafts=drafts, runs=runs, slots=slots, events=events[:400])


def save_trader_desk_snapshot(snapshot: TraderDeskSnapshot, path: Path | None = None) -> Path:
    target = path or trader_desk_file_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "drafts": [_draft_to_payload(item) for item in snapshot.drafts],
        "runs": [_run_to_payload(item) for item in snapshot.runs],
        "slots": [_slot_to_payload(item) for item in snapshot.slots],
        "events": [_event_to_payload(item) for item in sorted(snapshot.events, key=lambda item: (item.created_at, item.event_id), reverse=True)[:400]],
        "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
    }
    temp_path = target.with_suffix(target.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp_path.replace(target)
    return target


def _gate_from_payload(payload: object) -> TraderPriceGate:
    if not isinstance(payload, dict):
        return TraderPriceGate()
    condition = str(payload.get("condition") or "always").strip() or "always"
    if condition not in TRADER_GATE_CONDITION_VALUES:
        condition = "always"
    return TraderPriceGate(
        enabled=bool(payload.get("enabled", False)),
        condition=condition,
        trigger_inst_id=str(payload.get("trigger_inst_id") or "").strip().upper(),
        trigger_price_type=str(payload.get("trigger_price_type") or "mark").strip() or "mark",
        lower_price=_parse_decimal(payload.get("lower_price")),
        upper_price=_parse_decimal(payload.get("upper_price")),
    )


def _gate_to_payload(gate: TraderPriceGate) -> dict[str, object]:
    return {
        "enabled": gate.enabled,
        "condition": gate.condition,
        "trigger_inst_id": gate.trigger_inst_id,
        "trigger_price_type": gate.trigger_price_type,
        "lower_price": _normalize_decimal_text(gate.lower_price) if gate.lower_price is not None else None,
        "upper_price": _normalize_decimal_text(gate.upper_price) if gate.upper_price is not None else None,
    }


def _draft_from_legacy_payload(payload: object) -> TraderDraftRecord | None:
    if not isinstance(payload, dict):
        return None
    trader_id = str(payload.get("trader_id") or "").strip()
    template_payload = payload.get("template_payload")
    if not trader_id or not isinstance(template_payload, dict):
        return None
    created_at = _parse_time(payload.get("created_at")) or datetime.now()
    updated_at = _parse_time(payload.get("updated_at")) or created_at
    total_quota = _parse_decimal(payload.get("total_quota"), default=Decimal("1")) or Decimal("1")
    unit_quota = _parse_decimal(payload.get("unit_quota"), default=Decimal("0.1")) or Decimal("0.1")
    quota_steps = int(str(payload.get("quota_steps") or "10").strip() or "10")
    return TraderDraftRecord(
        trader_id=trader_id,
        template_payload=template_payload,
        total_quota=total_quota,
        unit_quota=unit_quota,
        quota_steps=max(quota_steps, 1),
        auto_restart_on_profit=bool(payload.get("profit_auto_exit", True)),
        pause_on_stop_loss=True,
        status=str(payload.get("status") or "draft"),
        notes=str(payload.get("notes") or ""),
        created_at=created_at,
        updated_at=updated_at,
    )


def _draft_from_payload(payload: object) -> TraderDraftRecord | None:
    if not isinstance(payload, dict):
        return None
    trader_id = str(payload.get("trader_id") or "").strip()
    template_payload = payload.get("template_payload")
    if not trader_id or not isinstance(template_payload, dict):
        return None
    created_at = _parse_time(payload.get("created_at")) or datetime.now()
    updated_at = _parse_time(payload.get("updated_at")) or created_at
    total_quota = _parse_decimal(payload.get("total_quota"), default=Decimal("1")) or Decimal("1")
    unit_quota = _parse_decimal(payload.get("unit_quota"), default=Decimal("0.1")) or Decimal("0.1")
    quota_steps = int(str(payload.get("quota_steps") or "10").strip() or "10")
    status = str(payload.get("status") or "draft").strip() or "draft"
    if status not in TRADER_DRAFT_STATUS_VALUES:
        status = "draft"
    return TraderDraftRecord(
        trader_id=trader_id,
        template_payload=template_payload,
        total_quota=total_quota,
        unit_quota=unit_quota,
        quota_steps=max(quota_steps, 1),
        auto_restart_on_profit=bool(payload.get("auto_restart_on_profit", payload.get("profit_auto_exit", True))),
        pause_on_stop_loss=bool(payload.get("pause_on_stop_loss", True)),
        status=status,
        notes=str(payload.get("notes") or ""),
        gate=_gate_from_payload(payload.get("gate")),
        created_at=created_at,
        updated_at=updated_at,
    )


def _draft_to_payload(draft: TraderDraftRecord) -> dict[str, object]:
    return {
        "trader_id": draft.trader_id,
        "template_payload": draft.template_payload,
        "total_quota": _normalize_decimal_text(draft.total_quota),
        "unit_quota": _normalize_decimal_text(draft.unit_quota),
        "quota_steps": draft.quota_steps,
        "auto_restart_on_profit": draft.auto_restart_on_profit,
        "pause_on_stop_loss": draft.pause_on_stop_loss,
        "status": draft.status,
        "notes": draft.notes,
        "gate": _gate_to_payload(draft.gate),
        "created_at": draft.created_at.isoformat(timespec="seconds"),
        "updated_at": draft.updated_at.isoformat(timespec="seconds"),
    }


def _run_from_payload(payload: object) -> TraderRunState | None:
    if not isinstance(payload, dict):
        return None
    trader_id = str(payload.get("trader_id") or "").strip()
    if not trader_id:
        return None
    status = str(payload.get("status") or "idle").strip() or "idle"
    if status not in TRADER_RUN_STATUS_VALUES:
        status = "idle"
    return TraderRunState(
        trader_id=trader_id,
        status=status,
        paused_reason=str(payload.get("paused_reason") or "").strip(),
        armed_session_id=str(payload.get("armed_session_id") or "").strip(),
        last_started_at=_parse_time(payload.get("last_started_at")),
        last_event_at=_parse_time(payload.get("last_event_at")),
        updated_at=_parse_time(payload.get("updated_at")),
    )


def _run_to_payload(run: TraderRunState) -> dict[str, object]:
    return {
        "trader_id": run.trader_id,
        "status": run.status,
        "paused_reason": run.paused_reason,
        "armed_session_id": run.armed_session_id,
        "last_started_at": run.last_started_at.isoformat(timespec="seconds") if run.last_started_at else None,
        "last_event_at": run.last_event_at.isoformat(timespec="seconds") if run.last_event_at else None,
        "updated_at": run.updated_at.isoformat(timespec="seconds") if run.updated_at else None,
    }


def _slot_from_payload(payload: object) -> TraderSlotRecord | None:
    if not isinstance(payload, dict):
        return None
    slot_id = str(payload.get("slot_id") or "").strip()
    trader_id = str(payload.get("trader_id") or "").strip()
    session_id = str(payload.get("session_id") or "").strip()
    if not slot_id or not trader_id or not session_id:
        return None
    status = str(payload.get("status") or "watching").strip() or "watching"
    if status not in TRADER_SLOT_STATUS_VALUES:
        status = "watching"
    return TraderSlotRecord(
        slot_id=slot_id,
        trader_id=trader_id,
        session_id=session_id,
        api_name=str(payload.get("api_name") or "").strip(),
        strategy_name=str(payload.get("strategy_name") or "").strip(),
        symbol=str(payload.get("symbol") or "").strip(),
        bar=str(payload.get("bar") or "").strip(),
        direction_label=str(payload.get("direction_label") or "").strip(),
        status=status,
        quota_occupied=bool(payload.get("quota_occupied", False)),
        created_at=_parse_time(payload.get("created_at")) or datetime.now(),
        opened_at=_parse_time(payload.get("opened_at")),
        closed_at=_parse_time(payload.get("closed_at")),
        released_at=_parse_time(payload.get("released_at")),
        entry_price=_parse_decimal(payload.get("entry_price")),
        exit_price=_parse_decimal(payload.get("exit_price")),
        size=_parse_decimal(payload.get("size")),
        net_pnl=_parse_decimal(payload.get("net_pnl")),
        close_reason=str(payload.get("close_reason") or "").strip(),
        history_record_id=str(payload.get("history_record_id") or "").strip(),
        note=str(payload.get("note") or "").strip(),
        pending_manual_exit_mode=str(payload.get("pending_manual_exit_mode") or "").strip(),
        pending_manual_exit_inst_id=str(payload.get("pending_manual_exit_inst_id") or "").strip().upper(),
        pending_manual_exit_order_id=str(payload.get("pending_manual_exit_order_id") or "").strip(),
        pending_manual_exit_cl_ord_id=str(payload.get("pending_manual_exit_cl_ord_id") or "").strip(),
    )


def _slot_to_payload(slot: TraderSlotRecord) -> dict[str, object]:
    return {
        "slot_id": slot.slot_id,
        "trader_id": slot.trader_id,
        "session_id": slot.session_id,
        "api_name": slot.api_name,
        "strategy_name": slot.strategy_name,
        "symbol": slot.symbol,
        "bar": slot.bar,
        "direction_label": slot.direction_label,
        "status": slot.status,
        "quota_occupied": slot.quota_occupied,
        "created_at": slot.created_at.isoformat(timespec="seconds"),
        "opened_at": slot.opened_at.isoformat(timespec="seconds") if slot.opened_at else None,
        "closed_at": slot.closed_at.isoformat(timespec="seconds") if slot.closed_at else None,
        "released_at": slot.released_at.isoformat(timespec="seconds") if slot.released_at else None,
        "entry_price": _normalize_decimal_text(slot.entry_price) if slot.entry_price is not None else None,
        "exit_price": _normalize_decimal_text(slot.exit_price) if slot.exit_price is not None else None,
        "size": _normalize_decimal_text(slot.size) if slot.size is not None else None,
        "net_pnl": _normalize_decimal_text(slot.net_pnl) if slot.net_pnl is not None else None,
        "close_reason": slot.close_reason,
        "history_record_id": slot.history_record_id,
        "note": slot.note,
        "pending_manual_exit_mode": slot.pending_manual_exit_mode,
        "pending_manual_exit_inst_id": slot.pending_manual_exit_inst_id,
        "pending_manual_exit_order_id": slot.pending_manual_exit_order_id,
        "pending_manual_exit_cl_ord_id": slot.pending_manual_exit_cl_ord_id,
    }


def _event_from_payload(payload: object) -> TraderEventRecord | None:
    if not isinstance(payload, dict):
        return None
    event_id = str(payload.get("event_id") or "").strip()
    trader_id = str(payload.get("trader_id") or "").strip()
    created_at = _parse_time(payload.get("created_at"))
    if not event_id or not trader_id or created_at is None:
        return None
    return TraderEventRecord(
        event_id=event_id,
        trader_id=trader_id,
        created_at=created_at,
        level=str(payload.get("level") or "info").strip() or "info",
        message=str(payload.get("message") or "").strip(),
    )


def _event_to_payload(event: TraderEventRecord) -> dict[str, object]:
    return {
        "event_id": event.event_id,
        "trader_id": event.trader_id,
        "created_at": event.created_at.isoformat(timespec="seconds"),
        "level": event.level,
        "message": event.message,
    }
