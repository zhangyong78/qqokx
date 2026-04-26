from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from okx_quant.trader_desk import (
    TraderDeskSnapshot,
    TraderBookSummary,
    TraderDraftRecord,
    TraderPriceGate,
    TraderSlotRecord,
    load_trader_desk_snapshot,
    normalize_trader_draft_inputs,
    save_trader_desk_snapshot,
    trader_book_summary,
    trader_gate_allows_price,
    trader_open_position_summary,
    trader_realized_close_counts,
    trader_realized_net_pnl,
    trader_realized_slots,
    trader_remaining_quota_steps,
    trader_used_quota_steps,
)


class TraderDeskModelTest(TestCase):
    def test_normalize_trader_draft_inputs_accepts_between_gate(self) -> None:
        normalized = normalize_trader_draft_inputs(
            total_quota="1.00",
            unit_quota="0.10",
            quota_steps="10",
            status=" ready ",
            gate_enabled=True,
            gate_condition="between",
            gate_trigger_inst_id="btc-usdt-swap",
            gate_trigger_price_type="last",
            gate_lower_price="80000",
            gate_upper_price="90000",
        )

        self.assertEqual(normalized["total_quota"], Decimal("1"))
        self.assertEqual(normalized["unit_quota"], Decimal("0.1"))
        self.assertEqual(normalized["quota_steps"], 10)
        self.assertEqual(normalized["status"], "ready")
        self.assertTrue(normalized["gate"].enabled)
        self.assertEqual(normalized["gate"].trigger_inst_id, "BTC-USDT-SWAP")
        self.assertEqual(normalized["gate"].trigger_price_type, "last")

    def test_trader_gate_allows_price_handles_between_condition(self) -> None:
        gate = TraderPriceGate(
            enabled=True,
            condition="between",
            trigger_inst_id="BTC-USDT-SWAP",
            trigger_price_type="mark",
            lower_price=Decimal("80000"),
            upper_price=Decimal("90000"),
        )

        self.assertTrue(trader_gate_allows_price(gate, Decimal("85000")))
        self.assertFalse(trader_gate_allows_price(gate, Decimal("91000")))

    def test_quota_and_average_summary_track_only_open_slots(self) -> None:
        draft = TraderDraftRecord(
            trader_id="T001",
            template_payload={"strategy_id": "demo"},
            total_quota=Decimal("1"),
            unit_quota=Decimal("0.1"),
            quota_steps=10,
        )
        slots = [
            TraderSlotRecord(
                slot_id="slot-1",
                trader_id="T001",
                session_id="S001",
                api_name="api1",
                strategy_name="demo",
                symbol="BTC-USDT-SWAP",
                status="open",
                quota_occupied=True,
                opened_at=datetime(2026, 4, 24, 8, 0, 0),
                entry_price=Decimal("80000"),
                size=Decimal("0.1"),
            ),
            TraderSlotRecord(
                slot_id="slot-2",
                trader_id="T001",
                session_id="S002",
                api_name="api1",
                strategy_name="demo",
                symbol="BTC-USDT-SWAP",
                status="open",
                quota_occupied=True,
                opened_at=datetime(2026, 4, 24, 8, 5, 0),
                entry_price=Decimal("82000"),
                size=Decimal("0.2"),
            ),
            TraderSlotRecord(
                slot_id="slot-3",
                trader_id="T001",
                session_id="S003",
                api_name="api1",
                strategy_name="demo",
                symbol="BTC-USDT-SWAP",
                status="closed_loss",
                quota_occupied=False,
                closed_at=datetime(2026, 4, 24, 8, 10, 0),
                net_pnl=Decimal("-12.5"),
            ),
        ]

        open_count, average_entry, total_size = trader_open_position_summary(slots, "T001")

        self.assertEqual(open_count, 2)
        self.assertEqual(total_size, Decimal("0.3"))
        self.assertEqual(average_entry, Decimal("81333.33333333333333333333333"))
        self.assertEqual(trader_used_quota_steps(slots, "T001"), 2)
        self.assertEqual(trader_remaining_quota_steps(draft, slots), 8)
        self.assertEqual(trader_realized_net_pnl(slots, "T001"), Decimal("-12.5"))
        self.assertEqual(trader_realized_close_counts(slots, "T001"), (1, 0, 1))

    def test_realized_summary_counts_closed_loss_without_net_pnl_as_zero(self) -> None:
        slots = [
            TraderSlotRecord(
                slot_id="slot-1",
                trader_id="T001",
                session_id="S001",
                api_name="api1",
                strategy_name="demo",
                symbol="BTC-USDT-SWAP",
                status="closed_loss",
                quota_occupied=False,
                closed_at=datetime(2026, 4, 24, 8, 10, 0),
                net_pnl=None,
            )
        ]

        self.assertEqual(trader_realized_net_pnl(slots, "T001"), Decimal("0"))
        self.assertEqual(trader_realized_close_counts(slots, "T001"), (1, 0, 1))

    def test_realized_summary_does_not_count_closed_manual_without_net_pnl_as_loss(self) -> None:
        slots = [
            TraderSlotRecord(
                slot_id="slot-1",
                trader_id="T001",
                session_id="S001",
                api_name="api1",
                strategy_name="demo",
                symbol="BTC-USDT-SWAP",
                status="closed_manual",
                quota_occupied=False,
                closed_at=datetime(2026, 4, 24, 8, 10, 0),
                net_pnl=None,
            )
        ]

        self.assertEqual(trader_realized_net_pnl(slots, "T001"), Decimal("0"))
        self.assertEqual(trader_realized_close_counts(slots, "T001"), (1, 0, 0))

    def test_snapshot_loader_supports_legacy_list_payload(self) -> None:
        with TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "trader_desk.json"
            payload = [
                {
                    "trader_id": "T017",
                    "template_payload": {"strategy_id": "ema_dynamic_long", "symbol": "SOL-USDT-SWAP"},
                    "total_quota": "1",
                    "unit_quota": "0.1",
                    "quota_steps": "10",
                    "profit_auto_exit": True,
                    "status": "draft",
                    "notes": "legacy",
                    "created_at": "2026-04-24T08:39:41",
                    "updated_at": "2026-04-24T08:39:41",
                }
            ]
            path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

            snapshot = load_trader_desk_snapshot(path)

            self.assertEqual(len(snapshot.drafts), 1)
            self.assertEqual(snapshot.drafts[0].trader_id, "T017")
            self.assertEqual(snapshot.drafts[0].unit_quota, Decimal("0.1"))
            self.assertTrue(snapshot.drafts[0].auto_restart_on_profit)

    def test_realized_net_pnl_returns_decimal_zero_when_empty(self) -> None:
        self.assertEqual(trader_realized_net_pnl([], "T001"), Decimal("0"))

    def test_realized_slots_sort_descending_by_close_time(self) -> None:
        slots = [
            TraderSlotRecord(
                slot_id="slot-early",
                trader_id="T001",
                session_id="S001",
                api_name="api1",
                strategy_name="demo",
                symbol="BTC-USDT-SWAP",
                status="closed_profit",
                closed_at=datetime(2026, 4, 24, 8, 10, 0),
                net_pnl=Decimal("1"),
            ),
            TraderSlotRecord(
                slot_id="slot-late",
                trader_id="T002",
                session_id="S002",
                api_name="api1",
                strategy_name="demo",
                symbol="ETH-USDT-SWAP",
                status="closed_loss",
                closed_at=datetime(2026, 4, 24, 8, 20, 0),
                net_pnl=Decimal("-1"),
            ),
        ]

        realized = trader_realized_slots(slots)

        self.assertEqual([slot.slot_id for slot in realized], ["slot-late", "slot-early"])

    def test_trader_book_summary_aggregates_all_trader_ledgers(self) -> None:
        drafts = [
            TraderDraftRecord(
                trader_id="T001",
                template_payload={"strategy_id": "ema_dynamic_long"},
                total_quota=Decimal("1"),
                unit_quota=Decimal("0.1"),
                quota_steps=10,
            ),
            TraderDraftRecord(
                trader_id="T002",
                template_payload={"strategy_id": "ema_dynamic_short"},
                total_quota=Decimal("1"),
                unit_quota=Decimal("0.1"),
                quota_steps=10,
            ),
            TraderDraftRecord(
                trader_id="T003",
                template_payload={"strategy_id": "ema_dynamic_long"},
                total_quota=Decimal("1"),
                unit_quota=Decimal("0.1"),
                quota_steps=10,
            ),
        ]
        slots = [
            TraderSlotRecord(
                slot_id="slot-profit",
                trader_id="T001",
                session_id="S001",
                api_name="api1",
                strategy_name="EMA",
                symbol="BTC-USDT-SWAP",
                status="closed_profit",
                closed_at=datetime(2026, 4, 24, 8, 10, 0),
                net_pnl=Decimal("0.50"),
            ),
            TraderSlotRecord(
                slot_id="slot-loss",
                trader_id="T002",
                session_id="S002",
                api_name="api1",
                strategy_name="EMA",
                symbol="ETH-USDT-SWAP",
                status="closed_loss",
                closed_at=datetime(2026, 4, 24, 8, 20, 0),
                net_pnl=Decimal("-0.20"),
            ),
            TraderSlotRecord(
                slot_id="slot-manual",
                trader_id="T002",
                session_id="S003",
                api_name="api1",
                strategy_name="EMA",
                symbol="ETH-USDT-SWAP",
                status="closed_manual",
                closed_at=datetime(2026, 4, 24, 8, 25, 0),
                net_pnl=None,
            ),
        ]

        summary = trader_book_summary(drafts, slots)

        self.assertIsInstance(summary, TraderBookSummary)
        self.assertEqual(summary.trader_count, 3)
        self.assertEqual(summary.profitable_trader_count, 1)
        self.assertEqual(summary.losing_trader_count, 1)
        self.assertEqual(summary.flat_trader_count, 1)
        self.assertEqual(summary.realized_count, 3)
        self.assertEqual(summary.win_count, 1)
        self.assertEqual(summary.loss_count, 1)
        self.assertEqual(summary.manual_count, 1)
        self.assertEqual(summary.net_pnl, Decimal("0.30"))

    def test_snapshot_round_trip_persists_slots_and_events(self) -> None:
        with TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "trader_desk.json"
            snapshot = TraderDeskSnapshot(
                drafts=[
                    TraderDraftRecord(
                        trader_id="T001",
                        template_payload={"strategy_id": "ema_dynamic_long"},
                        total_quota=Decimal("1"),
                        unit_quota=Decimal("0.1"),
                        quota_steps=10,
                    )
                ],
                slots=[
                    TraderSlotRecord(
                        slot_id="slot-1",
                        trader_id="T001",
                        session_id="S001",
                        api_name="moni",
                        strategy_name="EMA",
                        symbol="BTC-USDT-SWAP",
                        status="open",
                        quota_occupied=True,
                        opened_at=datetime(2026, 4, 24, 9, 0, 0),
                        entry_price=Decimal("81000"),
                        size=Decimal("0.1"),
                    )
                ],
            )

            save_trader_desk_snapshot(snapshot, path)
            loaded = load_trader_desk_snapshot(path)

            self.assertEqual(len(loaded.drafts), 1)
            self.assertEqual(len(loaded.slots), 1)
            self.assertEqual(loaded.slots[0].entry_price, Decimal("81000"))
            self.assertEqual(loaded.slots[0].size, Decimal("0.1"))
