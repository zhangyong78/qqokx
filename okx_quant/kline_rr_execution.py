from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal, ROUND_FLOOR
from hashlib import sha1
from typing import Any

from okx_quant.engine import resolve_open_pos_side
from okx_quant.kline_rr_trade import RRTradeEvent, RRTradeLedgerEntry, RRTradeOrderLink, RRTradePlan
from okx_quant.models import OrderPlan
from okx_quant.okx_client import OkxOrderStatus
from okx_quant.pricing import snap_to_increment


_WORKING_STATES = {"live", "partially_filled"}
_CANCELLED_STATES = {"canceled", "mmp_canceled"}


def best_quote_entry_price(*, plan: RRTradePlan, order_book: Any) -> Decimal:
    """Return the passive best quote required for a chase entry."""
    levels = order_book.bids if plan.direction == "long" else order_book.asks
    if not levels or not levels[0] or levels[0][0] is None:
        side_text = "bid 1" if plan.direction == "long" else "ask 1"
        raise RuntimeError(f"{plan.inst_id} has no usable {side_text} quote")
    price = Decimal(levels[0][0])
    if price <= 0:
        raise RuntimeError(f"{plan.inst_id} returned a non-positive best quote")
    return price


class RRTradeExecutionService:
    """Reconciles one RR-created entry order without creating duplicate exposure."""

    @staticmethod
    def should_monitor_status(status: str) -> bool:
        return str(status or "").strip().lower() in {
            "entry_working",
            "entry_partially_filled",
            "protected",
            "protected_break_even",
            "protected_trailing",
        }

    def activate(self, *, client: Any, credentials: Any, config: Any, plan: RRTradePlan) -> RRTradeLedgerEntry:
        if plan.inst_id.upper().endswith("-SWAP") is False:
            raise ValueError("RR exchange execution currently supports SWAP instruments only")
        entry_price = self._entry_price(client=client, plan=plan)
        order_plan = self._order_plan(plan=plan, entry_price=entry_price, config=config)
        client_id = self._client_id(plan.plan_id, "entry", revision=0)
        result = self._submit_entry(
            client=client,
            credentials=credentials,
            config=config,
            plan=plan,
            order_plan=order_plan,
            client_id=client_id,
        )
        order_id = str(getattr(result, "ord_id", "") or "").strip()
        if not order_id:
            raise RuntimeError("OKX accepted RR entry but did not return an order identifier")
        size = plan.sizing.contract_size
        now = _utc_now()
        return RRTradeLedgerEntry(
            entry_id=plan.plan_id,
            status="entry_working",
            plan=plan,
            entry_order=RRTradeOrderLink(
                role="entry",
                channel="order",
                order_id=order_id,
                client_id=str(getattr(result, "cl_ord_id", "") or client_id),
                state="live",
                size=size,
                price=entry_price,
            ),
            stop_loss_order=self._pending_protection_link(plan=plan, role="stop_loss", size=size),
            take_profit_order=self._pending_protection_link(plan=plan, role="take_profit", size=size),
            filled_size=Decimal("0"),
            remaining_size=size,
            events=(self._event("entry_submitted", f"RR entry submitted at {entry_price}"),),
            created_at=now,
            updated_at=now,
        )

    def reconcile(self, *, client: Any, credentials: Any, config: Any, entry: RRTradeLedgerEntry) -> RRTradeLedgerEntry:
        order = entry.entry_order
        if order is None or not order.order_id:
            return self._with_event(entry, status="manual_review", kind="missing_entry_order", message="RR entry order identifier is missing")
        status = client.get_order(credentials, config, inst_id=entry.plan.inst_id, ord_id=order.order_id)
        return self._reconcile_status(
            client=client,
            credentials=credentials,
            config=config,
            entry=entry,
            exchange_status=status,
        )

    def cancel(
        self,
        *,
        client: Any,
        credentials: Any,
        config: Any,
        entry: RRTradeLedgerEntry,
        confirmed_for_filled: bool,
    ) -> RRTradeLedgerEntry:
        order = entry.entry_order
        if order is None or not order.order_id:
            return self._with_event(entry, status="manual_review", kind="cancel_failed", message="RR entry order identifier is missing")
        status = client.get_order(credentials, config, inst_id=entry.plan.inst_id, ord_id=order.order_id)
        filled_size, remaining_size = self._fill_sizes(entry=entry, exchange_status=status)
        if filled_size > 0 and not confirmed_for_filled:
            return self._with_event(
                entry,
                status="cancel_confirmation_required",
                kind="cancel_confirmation_required",
                message=f"Entry has filled {filled_size}; confirm to cancel only the unfilled remainder and retain protection",
                filled_size=filled_size,
                remaining_size=remaining_size,
            )
        exchange_state = str(status.state or "").strip().lower()
        if exchange_state in _WORKING_STATES:
            client.cancel_order(credentials, config, inst_id=entry.plan.inst_id, ord_id=order.order_id)
        if filled_size > 0:
            updated = self._with_event(
                entry,
                status="protected_cancelled_remainder",
                kind="entry_remainder_cancelled",
                message="Cancelled unfilled entry remainder; existing filled position remains protected",
                filled_size=filled_size,
                remaining_size=remaining_size,
                entry_order=replace(order, state="canceled"),
            )
            return self._ensure_protection_links(updated)
        return self._with_event(
            entry,
            status="cancelled",
            kind="entry_cancelled",
            message="Cancelled unfilled RR entry",
            filled_size=Decimal("0"),
            remaining_size=remaining_size,
            entry_order=replace(order, state="canceled"),
        )

    def _reconcile_status(
        self,
        *,
        client: Any,
        credentials: Any,
        config: Any,
        entry: RRTradeLedgerEntry,
        exchange_status: OkxOrderStatus,
    ) -> RRTradeLedgerEntry:
        filled_size, remaining_size = self._fill_sizes(entry=entry, exchange_status=exchange_status)
        state = str(exchange_status.state or "").strip().lower()
        order = entry.entry_order
        assert order is not None
        refreshed_order = replace(
            order,
            state=state or order.state,
            size=exchange_status.size or order.size,
            price=exchange_status.price or order.price,
        )
        if filled_size > 0:
            status = "protected" if remaining_size <= 0 else "entry_partially_filled"
            if (
                remaining_size <= 0
                and entry.status in {"protected", "protected_break_even", "protected_trailing"}
                and entry.filled_size == filled_size
                and entry.remaining_size == remaining_size
                and entry.entry_order == refreshed_order
            ):
                return self._apply_stop_management(client=client, credentials=credentials, entry=entry)
            if (
                entry.status == status
                and entry.filled_size == filled_size
                and entry.remaining_size == remaining_size
                and entry.entry_order == refreshed_order
                and entry.stop_loss_order is not None
                and entry.take_profit_order is not None
            ):
                return self._apply_stop_management(client=client, credentials=credentials, entry=entry)
            updated = self._with_event(
                entry,
                status=status,
                kind="entry_fill_detected",
                message=f"RR entry filled {filled_size}; chase is stopped and protection remains active",
                filled_size=filled_size,
                remaining_size=remaining_size,
                entry_order=refreshed_order,
            )
            updated = self._ensure_protection_links(updated)
            updated = self._extract_attached_protection_links(updated, exchange_status=exchange_status)
            return self._apply_stop_management(client=client, credentials=credentials, entry=updated)
        if state in _CANCELLED_STATES:
            return self._with_event(
                entry,
                status="cancelled",
                kind="entry_cancelled",
                message="Exchange reports RR entry cancelled before any fill",
                filled_size=Decimal("0"),
                remaining_size=remaining_size,
                entry_order=refreshed_order,
            )
        if state not in _WORKING_STATES:
            return self._with_event(
                entry,
                status="manual_review",
                kind="entry_state_unknown",
                message=f"RR entry has unexpected exchange state: {state or '-'}",
                filled_size=filled_size,
                remaining_size=remaining_size,
                entry_order=refreshed_order,
            )
        if entry.plan.entry_execution_mode != "chase_best_quote":
            return replace(entry, entry_order=refreshed_order, filled_size=filled_size, remaining_size=remaining_size, updated_at=_utc_now())

        target_price = self._entry_price(client=client, plan=entry.plan)
        if refreshed_order.price == target_price:
            return replace(entry, entry_order=refreshed_order, filled_size=filled_size, remaining_size=remaining_size, updated_at=_utc_now())

        client.cancel_order(credentials, config, inst_id=entry.plan.inst_id, ord_id=order.order_id)
        post_cancel = client.get_order(credentials, config, inst_id=entry.plan.inst_id, ord_id=order.order_id)
        post_filled, post_remaining = self._fill_sizes(entry=entry, exchange_status=post_cancel)
        post_state = str(post_cancel.state or "").strip().lower()
        if post_filled > 0:
            updated = self._with_event(
                entry,
                status="protected" if post_remaining <= 0 else "entry_partially_filled",
                kind="fill_during_chase_cancel",
                message="Entry filled while cancelling for chase; replacement was not submitted",
                filled_size=post_filled,
                remaining_size=post_remaining,
                entry_order=replace(refreshed_order, state=post_state, price=post_cancel.price or refreshed_order.price),
            )
            return self._ensure_protection_links(updated)
        if post_state not in _CANCELLED_STATES:
            return self._with_event(
                entry,
                status="manual_review",
                kind="chase_cancel_unconfirmed",
                message="Exchange did not confirm cancellation; replacement was not submitted",
                entry_order=replace(refreshed_order, state=post_state or refreshed_order.state),
            )

        replacement_plan = self._order_plan(plan=entry.plan, entry_price=target_price, config=config)
        revision = len(entry.events) + 1
        client_id = self._client_id(entry.entry_id, "entry", revision=revision)
        result = self._submit_entry(
            client=client,
            credentials=credentials,
            config=config,
            plan=entry.plan,
            order_plan=replacement_plan,
            client_id=client_id,
        )
        replacement_id = str(getattr(result, "ord_id", "") or "").strip()
        if not replacement_id:
            return self._with_event(
                entry,
                status="manual_review",
                kind="chase_replace_missing_id",
                message="Replacement entry response had no order identifier",
                entry_order=replace(refreshed_order, state="canceled"),
            )
        return self._with_event(
            entry,
            status="entry_working",
            kind="entry_repriced",
            message=f"Chase replaced entry at {target_price}",
            filled_size=Decimal("0"),
            remaining_size=entry.plan.sizing.contract_size,
            entry_order=RRTradeOrderLink(
                role="entry",
                channel="order",
                order_id=replacement_id,
                client_id=str(getattr(result, "cl_ord_id", "") or client_id),
                state="live",
                size=entry.plan.sizing.contract_size,
                price=target_price,
            ),
        )

    def _entry_price(self, *, client: Any, plan: RRTradePlan) -> Decimal:
        if plan.entry_execution_mode != "chase_best_quote":
            return plan.entry_price
        return best_quote_entry_price(plan=plan, order_book=client.get_order_book(plan.inst_id, depth=1))

    def _order_plan(self, *, plan: RRTradePlan, entry_price: Decimal, config: Any) -> OrderPlan:
        side = "buy" if plan.direction == "long" else "sell"
        position_mode = str(getattr(config, "position_mode", "net") or "net")
        position_config = replace(config, position_mode=position_mode) if hasattr(config, "__dataclass_fields__") else config
        return OrderPlan(
            inst_id=plan.inst_id,
            side=side,
            pos_side=resolve_open_pos_side(position_config, side),
            size=plan.sizing.contract_size,
            take_profit=plan.take_profit_price,
            stop_loss=plan.stop_loss_price,
            entry_reference=entry_price,
            atr_value=abs(plan.entry_price - plan.stop_loss_price),
            signal=plan.direction,
            candle_ts=0,
            tp_sl_mode="exchange",
        )

    def _submit_entry(self, *, client: Any, credentials: Any, config: Any, plan: RRTradePlan, order_plan: OrderPlan, client_id: str) -> Any:
        submit = client.place_market_order if plan.entry_execution_mode == "market" else client.place_limit_order
        return submit(
            credentials,
            config,
            order_plan,
            cl_ord_id=client_id,
            stop_loss_algo_cl_ord_id=self._client_id(plan.plan_id, "stop_loss", revision=0),
            include_attached_protection=True,
        )

    def _pending_protection_link(self, *, plan: RRTradePlan, role: str, size: Decimal) -> RRTradeOrderLink:
        trigger_price = plan.stop_loss_price if role == "stop_loss" else plan.take_profit_price
        return RRTradeOrderLink(
            role=role,
            channel="algo",
            client_id=self._client_id(plan.plan_id, role, revision=0),
            state="pending_entry_fill",
            size=size,
            trigger_price=trigger_price,
        )

    def _extract_attached_protection_links(
        self,
        entry: RRTradeLedgerEntry,
        *,
        exchange_status: OkxOrderStatus,
    ) -> RRTradeLedgerEntry:
        raw = exchange_status.raw if isinstance(exchange_status.raw, dict) else {}
        attached = raw.get("attachAlgoOrds", [])
        if not isinstance(attached, list):
            return entry
        stop_link = entry.stop_loss_order
        take_profit_link = entry.take_profit_order
        for item in attached:
            if not isinstance(item, dict):
                continue
            algo_id = str(item.get("algoId") or item.get("slAlgoId") or "").strip()
            sl_trigger = _as_decimal(item.get("slTriggerPx"))
            tp_trigger = _as_decimal(item.get("tpTriggerPx"))
            if stop_link is not None and (algo_id or sl_trigger is not None):
                stop_link = replace(
                    stop_link,
                    algo_id=algo_id or stop_link.algo_id,
                    state="live" if algo_id else stop_link.state,
                    trigger_price=sl_trigger or stop_link.trigger_price,
                )
            if take_profit_link is not None and tp_trigger is not None:
                take_profit_link = replace(
                    take_profit_link,
                    state="live" if algo_id else take_profit_link.state,
                    trigger_price=tp_trigger,
                )
        return replace(entry, stop_loss_order=stop_link, take_profit_order=take_profit_link)

    def _apply_stop_management(self, *, client: Any, credentials: Any, entry: RRTradeLedgerEntry) -> RRTradeLedgerEntry:
        trigger_price = entry.plan.management_trigger_price
        if trigger_price is None or entry.plan.management_mode == "fixed_tp":
            return entry
        current_price = client.get_trigger_price(
            entry.plan.inst_id,
            entry.plan.trigger_price_type,
            environment=entry.plan.environment,
        )
        trigger_hit = current_price >= trigger_price if entry.plan.direction == "long" else current_price <= trigger_price
        if not trigger_hit:
            return entry
        stop_link = entry.stop_loss_order
        if stop_link is None or not stop_link.algo_id:
            return self._with_event(
                entry,
                status="manual_review",
                kind="missing_stop_algo_id",
                message="Break-even trigger reached but the attached stop-loss algo identifier is unavailable",
            )
        risk_distance = abs(entry.plan.entry_price - entry.plan.stop_loss_price)
        if risk_distance <= 0:
            return self._with_event(
                entry,
                status="manual_review",
                kind="invalid_risk_distance",
                message="Cannot apply stop management because RR risk distance is invalid",
            )
        progress_r = (
            (current_price - entry.plan.entry_price) / risk_distance
            if entry.plan.direction == "long"
            else (entry.plan.entry_price - current_price) / risk_distance
        )
        completed_r = int(progress_r.to_integral_value(rounding=ROUND_FLOOR))
        trigger_r = int(
            ((trigger_price - entry.plan.entry_price) / risk_distance).copy_abs().to_integral_value(rounding=ROUND_FLOOR)
        )
        locked_r = 0 if completed_r <= trigger_r else completed_r - 1
        fee_offset = abs(entry.plan.entry_price) * max(entry.plan.round_trip_fee_rate, Decimal("0"))
        target_stop = (
            entry.plan.entry_price + (risk_distance * Decimal(locked_r)) + fee_offset
            if entry.plan.direction == "long"
            else entry.plan.entry_price - (risk_distance * Decimal(locked_r)) - fee_offset
        )
        if entry.plan.instrument_tick_size > 0:
            target_stop = snap_to_increment(target_stop, entry.plan.instrument_tick_size, "nearest")
        current_stop = stop_link.trigger_price
        if current_stop is not None:
            if entry.plan.direction == "long" and current_stop >= target_stop:
                return entry
            if entry.plan.direction == "short" and current_stop <= target_stop:
                return entry
        client.amend_algo_order(
            credentials,
            environment=entry.plan.environment,
            inst_id=entry.plan.inst_id,
            algo_id=stop_link.algo_id,
            algo_cl_ord_id=stop_link.client_id or None,
            req_id=self._client_id(entry.entry_id, "amend", revision=len(entry.events) + 1),
            new_stop_loss_trigger_price=target_stop,
            new_stop_loss_trigger_price_type=entry.plan.trigger_price_type,
        )
        is_break_even = locked_r == 0
        updated = self._with_event(
            entry,
            status="protected_break_even" if is_break_even else "protected_trailing",
            kind="stop_moved_to_break_even" if is_break_even else "stop_moved_trailing",
            message=(
                f"Moved stop loss to break-even at {target_stop}"
                if is_break_even
                else f"Moved stop loss to lock {locked_r}R at {target_stop}"
            ),
        )
        return replace(
            updated,
            stop_loss_order=replace(stop_link, state="live", trigger_price=target_stop),
        )

    def _ensure_protection_links(self, entry: RRTradeLedgerEntry) -> RRTradeLedgerEntry:
        size = entry.filled_size if entry.filled_size > 0 else entry.plan.sizing.contract_size
        return replace(
            entry,
            stop_loss_order=entry.stop_loss_order or self._pending_protection_link(plan=entry.plan, role="stop_loss", size=size),
            take_profit_order=entry.take_profit_order or self._pending_protection_link(plan=entry.plan, role="take_profit", size=size),
        )

    def _fill_sizes(self, *, entry: RRTradeLedgerEntry, exchange_status: OkxOrderStatus) -> tuple[Decimal, Decimal]:
        total = exchange_status.size or entry.entry_order.size or entry.plan.sizing.contract_size  # type: ignore[union-attr]
        filled = exchange_status.filled_size or Decimal("0")
        filled = max(filled, Decimal("0"))
        return filled, max(total - filled, Decimal("0"))

    def _with_event(
        self,
        entry: RRTradeLedgerEntry,
        *,
        status: str,
        kind: str,
        message: str,
        filled_size: Decimal | None = None,
        remaining_size: Decimal | None = None,
        entry_order: RRTradeOrderLink | None = None,
    ) -> RRTradeLedgerEntry:
        return replace(
            entry,
            status=status,
            entry_order=entry_order if entry_order is not None else entry.entry_order,
            filled_size=entry.filled_size if filled_size is None else filled_size,
            remaining_size=entry.remaining_size if remaining_size is None else remaining_size,
            events=(*entry.events, self._event(kind, message)),
            updated_at=_utc_now(),
        )

    @staticmethod
    def _client_id(entry_id: str, role: str, *, revision: int) -> str:
        raw = f"{entry_id}|{role}|{revision}".encode("utf-8")
        role_token = "".join(char for char in role.lower() if char.isascii() and char.isalnum())[:3] or "ord"
        return f"rr{role_token}{sha1(raw).hexdigest()[:24]}"[:32]

    @staticmethod
    def _event(kind: str, message: str) -> RRTradeEvent:
        return RRTradeEvent(occurred_at=_utc_now(), kind=kind, message=message)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _as_decimal(value: object) -> Decimal | None:
    try:
        parsed = Decimal(str(value or "").strip())
    except Exception:
        return None
    return parsed if parsed > 0 else None
