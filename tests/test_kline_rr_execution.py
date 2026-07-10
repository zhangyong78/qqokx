from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest import TestCase

from okx_quant.kline_rr_execution import RRTradeExecutionService
from okx_quant.kline_rr_trade import RRTradeLedgerEntry, RRTradeOrderLink, build_rr_trade_plan
from okx_quant.models import Instrument, OrderPlan
from okx_quant.okx_client import OkxOrderBook, OkxOrderResult, OkxOrderStatus


def _instrument() -> Instrument:
    return Instrument(
        inst_id="BTC-USDT-SWAP",
        inst_type="SWAP",
        tick_size=Decimal("0.1"),
        lot_size=Decimal("1"),
        min_size=Decimal("1"),
        state="live",
        settle_ccy="USDT",
        ct_val=Decimal("0.01"),
        ct_mult=Decimal("1"),
        ct_val_ccy="BTC",
        uly="BTC-USDT",
        inst_family="BTC-USDT",
    )


def _plan(*, direction: str = "long", execution_mode: str = "chase_best_quote"):
    return build_rr_trade_plan(
        plan_id="rr-execution",
        profile_name="moni",
        environment="demo",
        instrument=_instrument(),
        direction=direction,
        entry_execution_mode=execution_mode,
        management_mode="fixed_tp",
        trigger_price_type="last",
        risk_amount=Decimal("100"),
        entry_price=Decimal("60000"),
        stop_loss_price=Decimal("59000") if direction == "long" else Decimal("61000"),
        direct_take_profit_r=Decimal("2"),
        round_trip_fee_rate=Decimal("0"),
    )


def _status(*, state: str, price: str = "60000", size: str = "10", filled: str = "0", raw: dict | None = None) -> OkxOrderStatus:
    return OkxOrderStatus(
        ord_id="entry-order-1",
        state=state,
        side="buy",
        ord_type="limit",
        price=Decimal(price),
        avg_price=None,
        size=Decimal(size),
        filled_size=Decimal(filled),
        raw=raw or {},
    )


class _FakeClient:
    def __init__(self, *, bid: str = "60000", ask: str = "60001", statuses: list[OkxOrderStatus] | None = None) -> None:
        self.book = OkxOrderBook(
            inst_id="BTC-USDT-SWAP",
            bids=((Decimal(bid), Decimal("5")),),
            asks=((Decimal(ask), Decimal("5")),),
            raw={},
        )
        self.statuses = list(statuses or [])
        self.submitted: list[tuple[OrderPlan, dict[str, object]]] = []
        self.cancelled: list[str] = []
        self.amended: list[dict[str, object]] = []
        self.trigger_price = Decimal("60000")

    def get_order_book(self, _inst_id: str, depth: int = 1) -> OkxOrderBook:
        return self.book

    def place_limit_order(self, _credentials, _config, plan: OrderPlan, **kwargs) -> OkxOrderResult:
        self.submitted.append((plan, kwargs))
        return OkxOrderResult(ord_id=f"entry-order-{len(self.submitted)}", cl_ord_id=kwargs.get("cl_ord_id"), s_code="0", s_msg="", raw={})

    def place_market_order(self, _credentials, _config, plan: OrderPlan, **kwargs) -> OkxOrderResult:
        self.submitted.append((plan, kwargs))
        return OkxOrderResult(ord_id=f"entry-order-{len(self.submitted)}", cl_ord_id=kwargs.get("cl_ord_id"), s_code="0", s_msg="", raw={})

    def get_order(self, _credentials, _config, **_kwargs) -> OkxOrderStatus:
        return self.statuses.pop(0)

    def cancel_order(self, _credentials, _config, **kwargs) -> OkxOrderResult:
        self.cancelled.append(str(kwargs.get("ord_id") or ""))
        return OkxOrderResult(ord_id=str(kwargs.get("ord_id") or ""), cl_ord_id=None, s_code="0", s_msg="", raw={})

    def get_trigger_price(self, _inst_id: str, _price_type: str, *, environment: str) -> Decimal:
        return self.trigger_price

    def amend_algo_order(self, _credentials, **kwargs) -> OkxOrderResult:
        self.amended.append(kwargs)
        return OkxOrderResult(ord_id="", cl_ord_id=kwargs.get("algo_cl_ord_id"), s_code="0", s_msg="", raw={})


class KlineRRExecutionTest(TestCase):
    def setUp(self) -> None:
        self.service = RRTradeExecutionService()
        self.credentials = SimpleNamespace(profile_name="moni")
        self.config = SimpleNamespace(position_mode="net")

    def test_activate_chase_long_places_bid_one_with_attached_protection(self) -> None:
        client = _FakeClient(bid="60001", ask="60002")

        entry = self.service.activate(client=client, credentials=self.credentials, config=self.config, plan=_plan())

        submitted_plan, kwargs = client.submitted[0]
        self.assertEqual(submitted_plan.entry_reference, Decimal("60001"))
        self.assertEqual(submitted_plan.side, "buy")
        self.assertTrue(kwargs["include_attached_protection"])
        self.assertEqual(entry.status, "entry_working")
        self.assertEqual(entry.remaining_size, Decimal("10"))

    def test_client_order_id_is_okx_safe_alphanumeric_and_within_limit(self) -> None:
        client_id = self.service._client_id("rr execution:BTC-USDT-SWAP", "entry", revision=12)

        self.assertTrue(client_id.isascii())
        self.assertTrue(client_id.isalnum())
        self.assertLessEqual(len(client_id), 32)

    def test_monitoring_status_keeps_break_even_trade_active_for_later_trailing(self) -> None:
        self.assertTrue(self.service.should_monitor_status("entry_working"))
        self.assertTrue(self.service.should_monitor_status("protected_break_even"))
        self.assertFalse(self.service.should_monitor_status("cancelled"))

    def test_reconcile_chase_cancels_then_replaces_only_after_cancel_is_confirmed(self) -> None:
        client = _FakeClient(
            bid="60002",
            statuses=[
                _status(state="live", price="60000"),
                _status(state="canceled", price="60000"),
            ],
        )
        entry = RRTradeLedgerEntry(
            entry_id="rr-execution",
            status="entry_working",
            plan=_plan(),
            entry_order=RRTradeOrderLink(role="entry", channel="order", order_id="entry-order-1", state="live", size=Decimal("10"), price=Decimal("60000")),
            remaining_size=Decimal("10"),
        )

        updated = self.service.reconcile(client=client, credentials=self.credentials, config=self.config, entry=entry)

        self.assertEqual(client.cancelled, ["entry-order-1"])
        self.assertEqual(len(client.submitted), 1)
        self.assertEqual(client.submitted[0][0].entry_reference, Decimal("60002"))
        self.assertEqual(updated.entry_order.order_id, "entry-order-1")
        self.assertEqual(updated.entry_order.price, Decimal("60002"))

    def test_reconcile_filled_entry_never_submits_a_replacement(self) -> None:
        client = _FakeClient(bid="60002", statuses=[_status(state="filled", filled="10")])
        entry = RRTradeLedgerEntry(
            entry_id="rr-execution",
            status="entry_working",
            plan=_plan(),
            entry_order=RRTradeOrderLink(role="entry", channel="order", order_id="entry-order-1", state="live", size=Decimal("10"), price=Decimal("60000")),
            remaining_size=Decimal("10"),
        )

        updated = self.service.reconcile(client=client, credentials=self.credentials, config=self.config, entry=entry)

        self.assertEqual(client.cancelled, [])
        self.assertEqual(client.submitted, [])
        self.assertEqual(updated.status, "protected")
        self.assertEqual(updated.filled_size, Decimal("10"))
        self.assertEqual(updated.remaining_size, Decimal("0"))

    def test_reconcile_unchanged_partial_fill_does_not_append_duplicate_event(self) -> None:
        client = _FakeClient(statuses=[_status(state="partially_filled", filled="3")])
        entry = RRTradeLedgerEntry(
            entry_id="rr-execution",
            status="entry_partially_filled",
            plan=_plan(),
            entry_order=RRTradeOrderLink(role="entry", channel="order", order_id="entry-order-1", state="partially_filled", size=Decimal("10"), price=Decimal("60000")),
            stop_loss_order=RRTradeOrderLink(role="stop_loss", channel="algo", state="live", size=Decimal("3"), trigger_price=Decimal("59000")),
            take_profit_order=RRTradeOrderLink(role="take_profit", channel="algo", state="live", size=Decimal("3"), trigger_price=Decimal("62000")),
            filled_size=Decimal("3"),
            remaining_size=Decimal("7"),
        )

        updated = self.service.reconcile(client=client, credentials=self.credentials, config=self.config, entry=entry)

        self.assertEqual(updated, entry)

    def test_reconcile_trailing_entry_extracts_stop_algo_and_moves_to_break_even(self) -> None:
        plan = build_rr_trade_plan(
            plan_id="rr-trailing",
            profile_name="moni",
            environment="demo",
            instrument=_instrument(),
            direction="long",
            entry_execution_mode="limit",
            management_mode="trail_after_1r",
            trigger_price_type="last",
            risk_amount=Decimal("100"),
            entry_price=Decimal("60000"),
            stop_loss_price=Decimal("59000"),
            direct_take_profit_r=Decimal("5"),
            round_trip_fee_rate=Decimal("0"),
        )
        client = _FakeClient(
            statuses=[
                _status(
                    state="filled",
                    filled="10",
                    raw={"attachAlgoOrds": [{"algoId": "sl-algo-1", "slTriggerPx": "59000", "tpTriggerPx": "65000"}]},
                )
            ]
        )
        client.trigger_price = Decimal("61000")
        entry = RRTradeLedgerEntry(
            entry_id="rr-trailing",
            status="entry_working",
            plan=plan,
            entry_order=RRTradeOrderLink(role="entry", channel="order", order_id="entry-order-1", state="live", size=Decimal("10"), price=Decimal("60000")),
            remaining_size=Decimal("10"),
        )

        updated = self.service.reconcile(client=client, credentials=self.credentials, config=self.config, entry=entry)

        self.assertEqual(updated.status, "protected_break_even")
        self.assertEqual(updated.stop_loss_order.algo_id, "sl-algo-1")
        self.assertEqual(updated.stop_loss_order.trigger_price, Decimal("60000"))
        self.assertEqual(client.amended[0]["new_stop_loss_trigger_price"], Decimal("60000"))

    def test_reconcile_trailing_stop_locks_previous_r_after_break_even(self) -> None:
        plan = build_rr_trade_plan(
            plan_id="rr-trailing-lock",
            profile_name="moni",
            environment="demo",
            instrument=_instrument(),
            direction="long",
            entry_execution_mode="limit",
            management_mode="trail_after_1r",
            trigger_price_type="last",
            risk_amount=Decimal("100"),
            entry_price=Decimal("60000"),
            stop_loss_price=Decimal("59000"),
            direct_take_profit_r=Decimal("5"),
            round_trip_fee_rate=Decimal("0"),
        )
        client = _FakeClient(statuses=[_status(state="filled", filled="10")])
        client.trigger_price = Decimal("63000")
        entry = RRTradeLedgerEntry(
            entry_id="rr-trailing-lock",
            status="protected_break_even",
            plan=plan,
            entry_order=RRTradeOrderLink(role="entry", channel="order", order_id="entry-order-1", state="filled", size=Decimal("10"), price=Decimal("60000")),
            stop_loss_order=RRTradeOrderLink(role="stop_loss", channel="algo", algo_id="sl-algo-1", state="live", size=Decimal("10"), trigger_price=Decimal("60000")),
            take_profit_order=RRTradeOrderLink(role="take_profit", channel="algo", state="live", size=Decimal("10"), trigger_price=Decimal("65000")),
            filled_size=Decimal("10"),
            remaining_size=Decimal("0"),
        )

        updated = self.service.reconcile(client=client, credentials=self.credentials, config=self.config, entry=entry)

        self.assertEqual(updated.status, "protected_trailing")
        self.assertEqual(updated.stop_loss_order.trigger_price, Decimal("62000"))
        self.assertEqual(client.amended[0]["new_stop_loss_trigger_price"], Decimal("62000"))

    def test_reconcile_break_even_includes_round_trip_fee_offset(self) -> None:
        plan = build_rr_trade_plan(
            plan_id="rr-fee-break-even",
            profile_name="moni",
            environment="demo",
            instrument=_instrument(),
            direction="long",
            entry_execution_mode="limit",
            management_mode="trail_after_1r",
            trigger_price_type="last",
            risk_amount=Decimal("100"),
            entry_price=Decimal("60000"),
            stop_loss_price=Decimal("59000"),
            direct_take_profit_r=Decimal("5"),
            round_trip_fee_rate=Decimal("0.001"),
        )
        client = _FakeClient(
            statuses=[
                _status(
                    state="filled",
                    filled="10",
                    raw={"attachAlgoOrds": [{"algoId": "sl-algo-fee", "slTriggerPx": "59000"}]},
                )
            ]
        )
        client.trigger_price = Decimal("61000")
        entry = RRTradeLedgerEntry(
            entry_id="rr-fee-break-even",
            status="entry_working",
            plan=plan,
            entry_order=RRTradeOrderLink(role="entry", channel="order", order_id="entry-order-1", state="live", size=Decimal("10"), price=Decimal("60000")),
            remaining_size=Decimal("10"),
        )

        updated = self.service.reconcile(client=client, credentials=self.credentials, config=self.config, entry=entry)

        self.assertEqual(updated.stop_loss_order.trigger_price, Decimal("60060"))
        self.assertEqual(client.amended[0]["new_stop_loss_trigger_price"], Decimal("60060"))

    def test_cancel_partial_fill_requires_confirmation_and_keeps_protection(self) -> None:
        client = _FakeClient(statuses=[_status(state="partially_filled", filled="3"), _status(state="partially_filled", filled="3")])
        entry = RRTradeLedgerEntry(
            entry_id="rr-execution",
            status="entry_working",
            plan=_plan(),
            entry_order=RRTradeOrderLink(role="entry", channel="order", order_id="entry-order-1", state="live", size=Decimal("10"), price=Decimal("60000")),
            remaining_size=Decimal("10"),
        )

        confirmation = self.service.cancel(client=client, credentials=self.credentials, config=self.config, entry=entry, confirmed_for_filled=False)
        cancelled = self.service.cancel(client=client, credentials=self.credentials, config=self.config, entry=entry, confirmed_for_filled=True)

        self.assertEqual(confirmation.status, "cancel_confirmation_required")
        self.assertEqual(client.cancelled, ["entry-order-1"])
        self.assertEqual(cancelled.status, "protected_cancelled_remainder")
        self.assertEqual(cancelled.filled_size, Decimal("3"))
        self.assertIsNotNone(cancelled.stop_loss_order)
        self.assertIsNotNone(cancelled.take_profit_order)
