from decimal import Decimal
from unittest import TestCase

from okx_quant.enhanced_ledger import EnhancedLedger
from okx_quant.enhanced_models import OptionLedgerEntry, QuotaRequest, QuotaSnapshot, SpotLedgerEntry, SwapLedgerEntry
from okx_quant.enhanced_quota_engine import QuotaEngine


class EnhancedStrategyFoundationTest(TestCase):
    def test_quota_engine_allocates_long_direction_and_protection(self) -> None:
        engine = QuotaEngine()
        snapshot = QuotaSnapshot(
            underlying_family="BTC-USD",
            long_limit_total=Decimal("3"),
            protected_long_quota_total=Decimal("2"),
            protected_long_quota_used=Decimal("0.5"),
        )
        request = QuotaRequest.from_action(
            underlying_family="BTC-USD",
            action="SWAP_LONG",
            quantity=Decimal("1.2"),
        )

        decision = engine.evaluate(snapshot, request)

        self.assertTrue(decision.allowed)
        self.assertEqual(decision.protected_long_applied, Decimal("1.2"))
        self.assertEqual(decision.unprotected_long_quantity, Decimal("0"))
        self.assertEqual(decision.updated_snapshot.long_limit_used, Decimal("1.2"))
        self.assertEqual(decision.updated_snapshot.protected_long_quota_used, Decimal("1.7"))
        self.assertEqual(
            [item.source_type for item in decision.allocations],
            ["long_direction", "protected_long"],
        )

    def test_quota_engine_rejects_swap_long_when_full_protection_is_required(self) -> None:
        engine = QuotaEngine()
        snapshot = QuotaSnapshot(
            underlying_family="BTC-USD",
            long_limit_total=Decimal("5"),
            protected_long_quota_total=Decimal("0.8"),
        )
        request = QuotaRequest.from_action(
            underlying_family="BTC-USD",
            action="SWAP_LONG",
            quantity=Decimal("1"),
            require_full_protection=True,
        )

        decision = engine.evaluate(snapshot, request)

        self.assertFalse(decision.allowed)
        self.assertIn("insufficient protected long quota", decision.errors[0])
        self.assertEqual(decision.updated_snapshot.long_limit_used, Decimal("0"))
        self.assertEqual(decision.protected_long_applied, Decimal("0.8"))

    def test_quota_engine_can_release_allocations(self) -> None:
        engine = QuotaEngine()
        snapshot = QuotaSnapshot(
            underlying_family="BTC-USD",
            long_limit_total=Decimal("4"),
            protected_long_quota_total=Decimal("2"),
        )
        request = QuotaRequest.from_action(
            underlying_family="BTC-USD",
            action="SWAP_LONG",
            quantity=Decimal("1"),
        )
        decision = engine.evaluate(snapshot, request)
        self.assertTrue(decision.allowed)

        released = engine.release_allocations(decision.updated_snapshot, decision.allocations)

        self.assertEqual(released.long_limit_used, Decimal("0"))
        self.assertEqual(released.protected_long_quota_used, Decimal("0"))

    def test_enhanced_ledger_builds_family_quota_snapshot(self) -> None:
        ledger = EnhancedLedger()
        ledger.upsert_spot_entry(
            SpotLedgerEntry(
                entry_id="spot-1",
                underlying_family="BTC-USD",
                inst_id="BTC-USDT",
                quantity=Decimal("2"),
                avg_cost=Decimal("70000"),
                covered_call_committed_quantity=Decimal("0.4"),
                sell_reserved_quantity=Decimal("0.2"),
            )
        )
        ledger.upsert_swap_entry(
            SwapLedgerEntry(
                position_id="swap-long-1",
                underlying_family="BTC-USD",
                inst_id="BTC-USDT-SWAP",
                direction="long",
                quantity=Decimal("0.6"),
                avg_entry_price=Decimal("80000"),
                break_even_price=Decimal("80200"),
                protected_quantity=Decimal("0.5"),
            )
        )
        ledger.upsert_swap_entry(
            SwapLedgerEntry(
                position_id="swap-short-1",
                underlying_family="BTC-USD",
                inst_id="BTC-USDT-SWAP",
                direction="short",
                quantity=Decimal("0.3"),
                avg_entry_price=Decimal("81000"),
                break_even_price=Decimal("80800"),
                protected_quantity=Decimal("0.2"),
            )
        )
        ledger.upsert_option_entry(
            OptionLedgerEntry(
                position_id="put-1",
                underlying_family="BTC-USD",
                inst_id="BTC-USD-260501-70000-P",
                option_type="put",
                side="buy",
                quantity=Decimal("1"),
                expiry_code="260501",
                strike=Decimal("70000"),
                premium_cashflow=Decimal("-500"),
                protected_long_capacity=Decimal("1"),
            )
        )
        ledger.upsert_option_entry(
            OptionLedgerEntry(
                position_id="call-1",
                underlying_family="BTC-USD",
                inst_id="BTC-USD-260501-95000-C",
                option_type="call",
                side="buy",
                quantity=Decimal("0.4"),
                expiry_code="260501",
                strike=Decimal("95000"),
                premium_cashflow=Decimal("-120"),
                protected_short_capacity=Decimal("0.4"),
            )
        )
        ledger.upsert_option_entry(
            OptionLedgerEntry(
                position_id="short-put-1",
                underlying_family="BTC-USD",
                inst_id="BTC-USD-260501-60000-P",
                option_type="put",
                side="sell",
                quantity=Decimal("0.5"),
                expiry_code="260501",
                strike=Decimal("60000"),
                premium_cashflow=Decimal("220"),
                cash_secured_put_quantity=Decimal("0.5"),
            )
        )

        family_snapshot = ledger.build_family_snapshot(
            "BTC-USD",
            long_limit_total=Decimal("1.5"),
            short_limit_total=Decimal("1"),
            cash_secured_put_quota_total=Decimal("1.2"),
        )
        quota = family_snapshot.quota_snapshot
        assert quota is not None

        self.assertEqual(quota.spot_inventory_total, Decimal("2"))
        self.assertEqual(quota.spot_inventory_reserved, Decimal("0.2"))
        self.assertEqual(quota.covered_call_quota_total, Decimal("1.8"))
        self.assertEqual(quota.covered_call_quota_used, Decimal("0.4"))
        self.assertEqual(quota.long_limit_used, Decimal("0.6"))
        self.assertEqual(quota.short_limit_used, Decimal("0.3"))
        self.assertEqual(quota.protected_long_quota_total, Decimal("1"))
        self.assertEqual(quota.protected_long_quota_used, Decimal("0.5"))
        self.assertEqual(quota.protected_short_quota_total, Decimal("0.4"))
        self.assertEqual(quota.protected_short_quota_used, Decimal("0.2"))
        self.assertEqual(quota.cash_secured_put_quota_used, Decimal("0.5"))

    def test_mark_swap_manual_moves_position_into_manual_pool(self) -> None:
        ledger = EnhancedLedger()
        ledger.upsert_swap_entry(
            SwapLedgerEntry(
                position_id="swap-long-1",
                underlying_family="BTC-USD",
                inst_id="BTC-USDT-SWAP",
                direction="long",
                quantity=Decimal("0.2"),
                avg_entry_price=Decimal("75000"),
                break_even_price=Decimal("75100"),
            )
        )

        updated = ledger.mark_swap_manual("swap-long-1")

        self.assertEqual(updated.pool, "manual")
        self.assertEqual(updated.status, "handoff_manual")
