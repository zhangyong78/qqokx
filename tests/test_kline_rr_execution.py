from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from okx_quant.kline_rr_execution import RRTradeExecutionService
from okx_quant.kline_rr_trade import RRTradeLedgerEntry, RRTradeOrderLink, build_rr_trade_plan
from okx_quant.models import Candle, Instrument, OrderPlan
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


def _trailing_plan(*, trigger_type: str = "last", management_mode: str = "trail_after_1r"):
    return build_rr_trade_plan(
        plan_id="rr-history",
        profile_name="moni",
        environment="demo",
        instrument=_instrument(),
        direction="long",
        entry_execution_mode="limit",
        management_mode=management_mode,
        trigger_price_type=trigger_type,
        risk_amount=Decimal("100"),
        entry_price=Decimal("60000"),
        stop_loss_price=Decimal("59000"),
        direct_take_profit_r=Decimal("5"),
        round_trip_fee_rate=Decimal("0"),
    )


def _minute_candles(
    *,
    high_at: dict[int, str] | None = None,
    default_high: str = "60800",
    low_at: dict[int, str] | None = None,
    default_low: str = "60400",
) -> list[Candle]:
    high_at = high_at or {}
    low_at = low_at or {}
    start = int(datetime(2026, 10, 21, 10, 0, tzinfo=timezone.utc).timestamp() * 1000)
    return [
        Candle(
            ts=start + minute * 60_000,
            open=Decimal("60500"),
            high=Decimal(high_at.get(minute, default_high)),
            low=Decimal(low_at.get(minute, default_low)),
            close=Decimal("60500"),
            volume=Decimal("1"),
            confirmed=True,
        )
        for minute in range(42)
    ]


def _candles_between(start: datetime, count: int, interval_ms: int, *, high_at: dict[int, str] | None = None) -> list[Candle]:
    high_at = high_at or {}
    start_ms = int(start.timestamp() * 1000)
    return [
        Candle(
            ts=start_ms + index * interval_ms,
            open=Decimal("60500"),
            high=Decimal(high_at.get(index, "60800")),
            low=Decimal("60400"),
            close=Decimal("60500"),
            volume=Decimal("1"),
            confirmed=True,
        )
        for index in range(count)
    ]


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
        self.positions = [SimpleNamespace(inst_id="BTC-USDT-SWAP", position=Decimal("1"), pos_side="net")]
        self.algos: dict[str, dict[str, str]] = {}
        self.confirm_amend = True
        self.order_fills = []
        self.candles: dict[tuple[str, str], list[Candle]] = {}
        self.history_calls: list[tuple[str, str, str]] = []
        self.history_page_size: dict[str, int] = {}

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

    def get_order_fill_history(self, _credentials, **_kwargs):
        return self.order_fills

    def get_candles(self, inst_id: str, bar: str, limit: int = 200):
        self.history_calls.append(("last", inst_id, bar))
        size = min(limit, self.history_page_size.get(bar, limit))
        return self.candles.get((inst_id, bar), [])[-size:]

    def get_candles_history_before(self, inst_id: str, bar: str, before_ts: int, limit: int = 240):
        self.history_calls.append(("last_history", inst_id, bar))
        size = min(limit, self.history_page_size.get(bar, limit))
        return [item for item in self.candles.get((inst_id, bar), []) if item.ts < before_ts][-size:]

    def get_mark_price_candles(self, inst_id: str, bar: str, limit: int = 200):
        self.history_calls.append(("mark", inst_id, bar))
        return self.candles.get((inst_id, bar), [])[-limit:]

    def get_mark_price_candles_history_before(self, inst_id: str, bar: str, before_ts: int, limit: int = 100):
        self.history_calls.append(("mark_history", inst_id, bar))
        return [item for item in self.candles.get((inst_id, bar), []) if item.ts < before_ts][-limit:]

    def get_index_price_candles(self, inst_id: str, bar: str, limit: int = 100):
        self.history_calls.append(("index", inst_id, bar))
        return self.candles.get((inst_id, bar), [])[-limit:]

    def get_index_price_candles_before(self, inst_id: str, bar: str, before_ts: int, limit: int = 100):
        self.history_calls.append(("index_history", inst_id, bar))
        return [item for item in self.candles.get((inst_id, bar), []) if item.ts < before_ts][-limit:]

    def cancel_order(self, _credentials, _config, **kwargs) -> OkxOrderResult:
        self.cancelled.append(str(kwargs.get("ord_id") or ""))
        return OkxOrderResult(ord_id=str(kwargs.get("ord_id") or ""), cl_ord_id=None, s_code="0", s_msg="", raw={})

    def get_trigger_price(self, _inst_id: str, _price_type: str, *, environment: str) -> Decimal:
        return self.trigger_price

    def get_positions(self, _credentials, **_kwargs):
        return self.positions

    def amend_algo_order(self, _credentials, **kwargs) -> OkxOrderResult:
        self.amended.append(kwargs)
        algo_id = str(kwargs.get("algo_id") or "")
        if self.confirm_amend:
            self.algos[algo_id] = {
                "state": "live",
                "slTriggerPx": str(kwargs["new_stop_loss_trigger_price"]),
                "slTriggerPxType": str(kwargs["new_stop_loss_trigger_price_type"]),
            }
        return OkxOrderResult(ord_id="", cl_ord_id=kwargs.get("algo_cl_ord_id"), s_code="0", s_msg="", raw={})

    def get_order_algo(self, _credentials, *, algo_id: str | None, **_kwargs):
        return self.algos.get(str(algo_id or ""))


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
        client.order_fills = [
            SimpleNamespace(fill_time=1_800_000_000_000),
            SimpleNamespace(fill_time=1_800_000_000_250),
        ]
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
        self.assertEqual(updated.entry_filled_at.timestamp(), 1_800_000_000)

    def test_reconcile_preserves_unknown_fill_time_when_history_is_unavailable(self) -> None:
        client = _FakeClient(statuses=[_status(state="filled", filled="10")])

        updated = self.service.reconcile(
            client=client,
            credentials=self.credentials,
            config=self.config,
            entry=RRTradeLedgerEntry(
                entry_id="rr-execution",
                status="entry_working",
                plan=_plan(),
                entry_order=RRTradeOrderLink(role="entry", channel="order", order_id="entry-order-1", state="live", size=Decimal("10"), price=Decimal("60000")),
                remaining_size=Decimal("10"),
            ),
        )

        self.assertEqual(updated.status, "protected")
        self.assertIsNone(updated.entry_filled_at)

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

    def test_reconcile_keeps_old_stop_when_exchange_readback_does_not_confirm_amend(self) -> None:
        plan = build_rr_trade_plan(
            plan_id="rr-unconfirmed-amend",
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
                    raw={"attachAlgoOrds": [{"algoId": "sl-algo-1", "slTriggerPx": "59000", "slTriggerPxType": "last"}]},
                )
            ]
        )
        client.confirm_amend = False
        client.trigger_price = Decimal("61000")
        entry = RRTradeLedgerEntry(
            entry_id="rr-unconfirmed-amend",
            status="entry_working",
            plan=plan,
            entry_order=RRTradeOrderLink(role="entry", channel="order", order_id="entry-order-1", state="live", size=Decimal("10"), price=Decimal("60000")),
            remaining_size=Decimal("10"),
        )

        updated = self.service.reconcile(client=client, credentials=self.credentials, config=self.config, entry=entry)

        self.assertEqual(updated.status, "manual_review")
        self.assertEqual(updated.stop_loss_order.trigger_price, Decimal("59000"))
        self.assertEqual(updated.events[-1].kind, "stop_amend_unconfirmed")

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

    def test_historical_favorable_extreme_amends_after_reversal_and_excludes_fill_minute(self) -> None:
        now = datetime(2026, 10, 21, 10, 42, 30, tzinfo=timezone.utc)
        client = _FakeClient()
        client.trigger_price = Decimal("60500")
        client.candles[("BTC-USDT-SWAP", "1m")] = _minute_candles(
            high_at={15: "70000", 30: "61150"}
        )
        entry = RRTradeLedgerEntry(
            entry_id="rr-history",
            status="protected",
            plan=_trailing_plan(),
            entry_order=RRTradeOrderLink(role="entry", channel="order", order_id="entry-order-1", state="filled", size=Decimal("10"), price=Decimal("60000")),
            stop_loss_order=RRTradeOrderLink(role="stop_loss", channel="algo", algo_id="sl-history", state="live", size=Decimal("10"), trigger_price=Decimal("59000")),
            take_profit_order=RRTradeOrderLink(role="take_profit", channel="algo", algo_id="tp-history", state="live", size=Decimal("10"), trigger_price=Decimal("65000")),
            filled_size=Decimal("10"),
            remaining_size=Decimal("0"),
            entry_filled_at=datetime(2026, 10, 21, 10, 20, 35, tzinfo=timezone.utc),
        )

        with patch("okx_quant.kline_rr_execution._utc_now", return_value=now):
            updated = self.service._apply_stop_management(client=client, credentials=self.credentials, entry=entry)

        self.assertEqual(updated.favorable_price, Decimal("61150"))
        self.assertEqual(updated.stop_loss_order.trigger_price, Decimal("60000"))
        self.assertEqual(updated.status, "protected_break_even")
        self.assertEqual(client.amended[0]["new_stop_loss_trigger_price"], Decimal("60000"))

    def test_short_history_uses_post_fill_low_and_current_price_safety(self) -> None:
        now = datetime(2026, 10, 21, 10, 42, 30, tzinfo=timezone.utc)
        plan = build_rr_trade_plan(
            plan_id="rr-history-short",
            profile_name="moni",
            environment="demo",
            instrument=_instrument(),
            direction="short",
            entry_execution_mode="limit",
            management_mode="trail_after_1r",
            trigger_price_type="last",
            risk_amount=Decimal("100"),
            entry_price=Decimal("60000"),
            stop_loss_price=Decimal("61000"),
            direct_take_profit_r=Decimal("5"),
            round_trip_fee_rate=Decimal("0"),
        )
        client = _FakeClient()
        client.trigger_price = Decimal("59500")
        client.positions = [SimpleNamespace(inst_id="BTC-USDT-SWAP", position=Decimal("-1"), pos_side="net")]
        client.candles[("BTC-USDT-SWAP", "1m")] = _minute_candles(
            low_at={15: "50000", 30: "58850"},
        )
        entry = RRTradeLedgerEntry(
            entry_id="rr-history-short",
            status="protected",
            plan=plan,
            entry_order=RRTradeOrderLink(role="entry", channel="order", order_id="short-entry", state="filled", size=Decimal("10"), price=Decimal("60000")),
            stop_loss_order=RRTradeOrderLink(role="stop_loss", channel="algo", algo_id="sl-short", state="live", size=Decimal("10"), trigger_price=Decimal("61000")),
            take_profit_order=RRTradeOrderLink(role="take_profit", channel="algo", algo_id="tp-short", state="live", size=Decimal("10"), trigger_price=Decimal("55000")),
            filled_size=Decimal("10"),
            remaining_size=Decimal("0"),
            entry_filled_at=datetime(2026, 10, 21, 10, 20, 35, tzinfo=timezone.utc),
        )

        with patch("okx_quant.kline_rr_execution._utc_now", return_value=now):
            updated = self.service._apply_stop_management(client=client, credentials=self.credentials, entry=entry)

        self.assertEqual(updated.favorable_price, Decimal("58850"))
        self.assertEqual(updated.stop_loss_order.trigger_price, Decimal("60000"))
        self.assertEqual(client.amended[0]["new_stop_loss_trigger_price"], Decimal("60000"))

    def test_historical_trailing_stage_is_remembered_until_current_price_allows_amendment(self) -> None:
        now = datetime(2026, 10, 21, 10, 42, 30, tzinfo=timezone.utc)
        client = _FakeClient()
        client.trigger_price = Decimal("61500")
        client.candles[("BTC-USDT-SWAP", "1m")] = _minute_candles(high_at={30: "63000"})
        entry = RRTradeLedgerEntry(
            entry_id="rr-history",
            status="protected_break_even",
            plan=_trailing_plan(management_mode="trail_after_1r"),
            entry_order=RRTradeOrderLink(role="entry", channel="order", order_id="entry-order-1", state="filled", size=Decimal("10"), price=Decimal("60000")),
            stop_loss_order=RRTradeOrderLink(role="stop_loss", channel="algo", algo_id="sl-history", state="live", size=Decimal("10"), trigger_price=Decimal("60000")),
            take_profit_order=RRTradeOrderLink(role="take_profit", channel="algo", algo_id="tp-history", state="live", size=Decimal("10"), trigger_price=Decimal("65000")),
            filled_size=Decimal("10"),
            remaining_size=Decimal("0"),
            entry_filled_at=datetime(2026, 10, 21, 10, 20, 35, tzinfo=timezone.utc),
        )

        with patch("okx_quant.kline_rr_execution._utc_now", return_value=now):
            deferred = self.service._apply_stop_management(client=client, credentials=self.credentials, entry=entry)
            self.assertEqual(deferred.status, "protected_break_even")
            self.assertEqual(deferred.events[-1].kind, "stop_amend_candidate_deferred")
            self.assertEqual(deferred.favorable_price, Decimal("63000"))
            self.assertTrue(self.service.should_monitor_status(deferred.status))

            client.trigger_price = Decimal("62500")
            updated = self.service._apply_stop_management(client=client, credentials=self.credentials, entry=deferred)

        self.assertEqual(updated.stop_loss_order.trigger_price, Decimal("62000"))
        self.assertEqual(client.amended[-1]["new_stop_loss_trigger_price"], Decimal("62000"))

    def test_history_uses_matching_mark_and_index_trigger_products(self) -> None:
        now = datetime(2026, 10, 21, 10, 42, 30, tzinfo=timezone.utc)
        for trigger_type, expected_source, expected_inst_id in (
            ("mark", "mark", "BTC-USDT-SWAP"),
            ("index", "index", "BTC-USDT"),
        ):
            with self.subTest(trigger_type=trigger_type):
                client = _FakeClient()
                client.trigger_price = Decimal("60500")
                client.candles[(expected_inst_id, "1m")] = _minute_candles(high_at={30: "61150"})
                entry = RRTradeLedgerEntry(
                    entry_id="rr-history",
                    status="protected",
                    plan=_trailing_plan(trigger_type=trigger_type),
                    entry_order=RRTradeOrderLink(role="entry", channel="order", order_id="entry-order-1", state="filled", size=Decimal("10"), price=Decimal("60000")),
                    stop_loss_order=RRTradeOrderLink(role="stop_loss", channel="algo", algo_id="sl-history", state="live", size=Decimal("10"), trigger_price=Decimal("59000")),
                    take_profit_order=RRTradeOrderLink(role="take_profit", channel="algo", algo_id="tp-history", state="live", size=Decimal("10"), trigger_price=Decimal("65000")),
                    filled_size=Decimal("10"),
                    remaining_size=Decimal("0"),
                    entry_filled_at=datetime(2026, 10, 21, 10, 20, 35, tzinfo=timezone.utc),
                )

                with patch("okx_quant.kline_rr_execution._utc_now", return_value=now):
                    updated = self.service._apply_stop_management(client=client, credentials=self.credentials, entry=entry)

                self.assertTrue(any(call[0] == expected_source and call[1] == expected_inst_id for call in client.history_calls))
                self.assertEqual(updated.favorable_price, Decimal("61150"))

    def test_historical_recovery_pages_hour_candles_after_multi_hour_monitor_gap(self) -> None:
        now = datetime(2026, 10, 21, 12, 10, 30, tzinfo=timezone.utc)
        client = _FakeClient()
        client.trigger_price = Decimal("62500")
        client.history_page_size["1H"] = 1
        client.candles[("BTC-USDT-SWAP", "1H")] = _candles_between(
            datetime(2026, 10, 21, 9, 0, tzinfo=timezone.utc),
            3,
            3_600_000,
            high_at={2: "63000"},
        )
        entry_minutes = _candles_between(datetime(2026, 10, 21, 8, 21, tzinfo=timezone.utc), 39, 60_000)
        current_minutes = _candles_between(datetime(2026, 10, 21, 12, 0, tzinfo=timezone.utc), 10, 60_000)
        client.candles[("BTC-USDT-SWAP", "1m")] = [*entry_minutes, *current_minutes]
        entry = RRTradeLedgerEntry(
            entry_id="rr-hourly-catchup",
            status="protected",
            plan=_trailing_plan(),
            entry_order=RRTradeOrderLink(role="entry", channel="order", order_id="entry-order-1", state="filled", size=Decimal("10"), price=Decimal("60000")),
            stop_loss_order=RRTradeOrderLink(role="stop_loss", channel="algo", algo_id="sl-hourly", state="live", size=Decimal("10"), trigger_price=Decimal("59000")),
            take_profit_order=RRTradeOrderLink(role="take_profit", channel="algo", algo_id="tp-hourly", state="live", size=Decimal("10"), trigger_price=Decimal("65000")),
            filled_size=Decimal("10"),
            remaining_size=Decimal("0"),
            entry_filled_at=datetime(2026, 10, 21, 8, 20, 35, tzinfo=timezone.utc),
        )

        with patch("okx_quant.kline_rr_execution._utc_now", return_value=now):
            updated = self.service._apply_stop_management(client=client, credentials=self.credentials, entry=entry)

        self.assertEqual(updated.favorable_price, Decimal("63000"))
        self.assertEqual(updated.stop_loss_order.trigger_price, Decimal("62000"))
        self.assertEqual(updated.history_hour_cursor_ts, int(datetime(2026, 10, 21, 11, 0, tzinfo=timezone.utc).timestamp() * 1000))
        self.assertGreaterEqual(sum(call[0] == "last_history" and call[2] == "1H" for call in client.history_calls), 2)

    def test_history_gap_keeps_observed_stage_and_retries_after_data_arrives(self) -> None:
        first_now = datetime(2026, 10, 21, 10, 42, 30, tzinfo=timezone.utc)
        next_now = datetime(2026, 10, 21, 10, 43, 30, tzinfo=timezone.utc)
        client = _FakeClient()
        client.trigger_price = Decimal("61500")
        full_minutes = _minute_candles(high_at={30: "63000"})
        client.candles[("BTC-USDT-SWAP", "1m")] = [item for item in full_minutes if item.ts != full_minutes[31].ts]
        entry = RRTradeLedgerEntry(
            entry_id="rr-history-retry",
            status="protected_break_even",
            plan=_trailing_plan(),
            entry_order=RRTradeOrderLink(role="entry", channel="order", order_id="entry-order-1", state="filled", size=Decimal("10"), price=Decimal("60000")),
            stop_loss_order=RRTradeOrderLink(role="stop_loss", channel="algo", algo_id="sl-retry", state="live", size=Decimal("10"), trigger_price=Decimal("60000")),
            take_profit_order=RRTradeOrderLink(role="take_profit", channel="algo", algo_id="tp-retry", state="live", size=Decimal("10"), trigger_price=Decimal("65000")),
            filled_size=Decimal("10"),
            remaining_size=Decimal("0"),
            entry_filled_at=datetime(2026, 10, 21, 10, 20, 35, tzinfo=timezone.utc),
        )

        with patch("okx_quant.kline_rr_execution._utc_now", return_value=first_now):
            deferred = self.service._apply_stop_management(client=client, credentials=self.credentials, entry=entry)
        self.assertTrue(any(item.kind == "rr_history_catchup_failed" for item in deferred.events))
        self.assertEqual(deferred.events[-1].kind, "stop_amend_candidate_deferred")
        self.assertEqual(deferred.favorable_price, Decimal("63000"))
        self.assertEqual(deferred.stop_loss_order.trigger_price, Decimal("60000"))

        client.candles[("BTC-USDT-SWAP", "1m")] = full_minutes
        client.trigger_price = Decimal("62500")
        with patch("okx_quant.kline_rr_execution._utc_now", return_value=next_now):
            updated = self.service._apply_stop_management(client=client, credentials=self.credentials, entry=deferred)

        self.assertEqual(updated.stop_loss_order.trigger_price, Decimal("62000"))
        self.assertEqual(updated.events[-1].kind, "stop_moved_trailing")

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
