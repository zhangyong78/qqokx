from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from okx_quant.kline_rr_trade import (
    RRTradeEvent,
    RRTradeLedgerEntry,
    RRTradeOrderLink,
    build_rr_trade_plan,
)
from okx_quant.models import Instrument
from okx_quant.persistence import (
    kline_rr_trade_ledger_file_path,
    load_kline_rr_trade_ledger_snapshot,
    save_kline_rr_trade_ledger_snapshot,
)


def _sample_swap_instrument() -> Instrument:
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


class KlineRRTradeTest(TestCase):
    def test_build_rr_trade_plan_computes_linear_swap_size_from_risk(self) -> None:
        plan = build_rr_trade_plan(
            plan_id="rr-1",
            profile_name="QQzhangyong",
            environment="live",
            instrument=_sample_swap_instrument(),
            direction="long",
            entry_execution_mode="limit",
            management_mode="fixed_tp",
            trigger_price_type="last",
            risk_amount=Decimal("100"),
            entry_price=Decimal("60000"),
            stop_loss_price=Decimal("59000"),
            direct_take_profit_r=Decimal("5"),
            round_trip_fee_rate=Decimal("0"),
        )

        self.assertEqual(plan.sizing.contract_size, Decimal("10"))
        self.assertEqual(plan.sizing.base_size, Decimal("0.10"))
        self.assertEqual(plan.sizing.notional_usdt, Decimal("6000.00"))
        self.assertEqual(plan.sizing.actual_risk_amount, Decimal("100.00"))
        self.assertEqual(plan.take_profit_price, Decimal("61000.0"))

    def test_build_rr_trade_plan_rounds_down_so_actual_risk_never_exceeds_budget(self) -> None:
        plan = build_rr_trade_plan(
            plan_id="rr-2",
            profile_name="QQzhangyong",
            environment="live",
            instrument=_sample_swap_instrument(),
            direction="long",
            entry_execution_mode="market",
            management_mode="trail_after_1r",
            trigger_price_type="last",
            risk_amount=Decimal("100"),
            entry_price=Decimal("60333"),
            stop_loss_price=Decimal("59221"),
            direct_take_profit_r=Decimal("5"),
            round_trip_fee_rate=Decimal("0.001"),
        )

        self.assertEqual(plan.sizing.contract_size, Decimal("8"))
        self.assertEqual(plan.sizing.base_size, Decimal("0.08"))
        self.assertLessEqual(plan.sizing.actual_risk_amount, Decimal("100"))
        self.assertEqual(plan.sizing.actual_risk_amount, Decimal("88.96"))

    def test_build_rr_trade_plan_preserves_best_quote_chase_entry_mode(self) -> None:
        plan = build_rr_trade_plan(
            plan_id="rr-chase",
            profile_name="moni",
            environment="demo",
            instrument=_sample_swap_instrument(),
            direction="long",
            entry_execution_mode="chase_best_quote",
            management_mode="fixed_tp",
            trigger_price_type="last",
            risk_amount=Decimal("100"),
            entry_price=Decimal("60000"),
            stop_loss_price=Decimal("59000"),
            direct_take_profit_r=Decimal("2"),
            round_trip_fee_rate=Decimal("0"),
        )

        self.assertEqual(plan.entry_execution_mode, "chase_best_quote")

    def test_build_rr_trade_plan_supports_three_r_break_even_trigger(self) -> None:
        plan = build_rr_trade_plan(
            plan_id="rr-three-r",
            profile_name="moni",
            environment="demo",
            instrument=_sample_swap_instrument(),
            direction="long",
            entry_execution_mode="limit",
            management_mode="trail_after_3r",
            trigger_price_type="last",
            risk_amount=Decimal("100"),
            entry_price=Decimal("60000"),
            stop_loss_price=Decimal("59000"),
            direct_take_profit_r=Decimal("5"),
            round_trip_fee_rate=Decimal("0"),
        )

        self.assertEqual(plan.management_mode, "trail_after_3r")
        self.assertEqual(plan.management_trigger_price, Decimal("63000"))

    def test_save_and_load_kline_rr_trade_ledger_snapshot(self) -> None:
        created_at = datetime(2026, 7, 9, 2, 3, 4, tzinfo=timezone.utc)
        plan = build_rr_trade_plan(
            plan_id="rr-3",
            profile_name="QQzhangyong",
            environment="live",
            instrument=_sample_swap_instrument(),
            direction="short",
            entry_execution_mode="limit",
            management_mode="trail_after_2r",
            trigger_price_type="last",
            risk_amount=Decimal("200"),
            entry_price=Decimal("61000"),
            stop_loss_price=Decimal("62000"),
            direct_take_profit_r=Decimal("6"),
            round_trip_fee_rate=Decimal("0.001"),
            created_at=created_at,
        )
        entry = RRTradeLedgerEntry(
            entry_id="ledger-1",
            status="entry_filled",
            plan=plan,
            entry_order=RRTradeOrderLink(
                role="entry",
                channel="order",
                order_id="123",
                client_id="rr-entry-1",
                state="filled",
                size=Decimal("20"),
                price=Decimal("61000"),
            ),
            stop_loss_order=RRTradeOrderLink(
                role="stop_loss",
                channel="algo",
                algo_id="456",
                client_id="rr-sl-1",
                state="live",
                trigger_price=Decimal("62000"),
            ),
            take_profit_order=RRTradeOrderLink(
                role="take_profit",
                channel="algo",
                algo_id="789",
                client_id="rr-tp-1",
                state="live",
                trigger_price=Decimal("59000"),
            ),
            events=(
                RRTradeEvent(
                    occurred_at=created_at,
                    kind="created",
                    message="创建 RR 交易计划",
                ),
            ),
            created_at=created_at,
            updated_at=created_at,
        )

        with TemporaryDirectory() as temp_dir:
            temp_path = kline_rr_trade_ledger_file_path(Path(temp_dir))
            save_kline_rr_trade_ledger_snapshot([entry.to_dict()], temp_path)
            snapshot = load_kline_rr_trade_ledger_snapshot(temp_path)

        self.assertEqual(len(snapshot["entries"]), 1)
        restored = RRTradeLedgerEntry.from_dict(snapshot["entries"][0])
        self.assertEqual(restored.plan.plan_id, "rr-3")
        self.assertEqual(restored.plan.sizing.contract_size, Decimal("20"))
        self.assertEqual(restored.stop_loss_order.algo_id, "456")
        self.assertEqual(restored.take_profit_order.trigger_price, Decimal("59000"))
        self.assertEqual(restored.events[0].kind, "created")

    def test_rr_trade_ledger_round_trips_partial_entry_fill_state(self) -> None:
        plan = build_rr_trade_plan(
            plan_id="rr-partial",
            profile_name="moni",
            environment="demo",
            instrument=_sample_swap_instrument(),
            direction="short",
            entry_execution_mode="limit",
            management_mode="fixed_tp",
            trigger_price_type="last",
            risk_amount=Decimal("100"),
            entry_price=Decimal("61000"),
            stop_loss_price=Decimal("62000"),
            direct_take_profit_r=Decimal("2"),
            round_trip_fee_rate=Decimal("0"),
        )
        entry = RRTradeLedgerEntry(
            entry_id="ledger-partial",
            status="entry_partially_filled",
            plan=plan,
            entry_order=RRTradeOrderLink(
                role="entry",
                channel="order",
                order_id="order-1",
                state="partially_filled",
                size=Decimal("10"),
                price=Decimal("61000"),
            ),
            filled_size=Decimal("3"),
            remaining_size=Decimal("7"),
        )

        restored = RRTradeLedgerEntry.from_dict(entry.to_dict())

        self.assertEqual(restored.filled_size, Decimal("3"))
        self.assertEqual(restored.remaining_size, Decimal("7"))
