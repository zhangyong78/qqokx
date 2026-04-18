from __future__ import annotations

import unittest
from decimal import Decimal
from types import SimpleNamespace

from okx_quant.enhanced_live_engine import (
    EnhancedStrategyEngine,
    LiveEnhancedPosition,
    apply_external_fill_reduction,
    derive_spot_signal_inst_id,
    derive_swap_trade_inst_id,
)
from okx_quant.enhanced_registry import EnhancedStrategyRegistry
from okx_quant.enhanced_seed_strategies import register_seed_strategy_package
from okx_quant.models import Candle


class EnhancedLiveEngineHelpersTests(unittest.TestCase):
    def test_derives_spot_and_swap_symbols(self) -> None:
        self.assertEqual(derive_spot_signal_inst_id("BTC-USDT-SWAP"), "BTC-USDT")
        self.assertEqual(derive_spot_signal_inst_id("ETH-USDT"), "ETH-USDT")
        self.assertEqual(derive_swap_trade_inst_id("BTC-USDT"), "BTC-USDT-SWAP")
        self.assertEqual(derive_swap_trade_inst_id("BTC-USDT-SWAP"), "BTC-USDT-SWAP")

    def test_manual_positions_are_reduced_before_auto_positions(self) -> None:
        positions = [
            self._position("auto-1", "long", "opened_auto", Decimal("1")),
            self._position("manual-1", "long", "manual_managed", Decimal("1")),
            self._position("manual-2", "long", "manual_managed", Decimal("2")),
        ]

        survivors, reduced = apply_external_fill_reduction(
            positions,
            quantity=Decimal("1.5"),
            direction="long",
        )

        survivor_by_id = {item.position_id: item for item in survivors}
        self.assertNotIn("manual-1", survivor_by_id)
        self.assertEqual(survivor_by_id["manual-2"].quantity, Decimal("1.5"))
        self.assertEqual(survivor_by_id["auto-1"].quantity, Decimal("1"))
        self.assertEqual([item.position_id for item in reduced], ["manual-1", "manual-2"])
        self.assertEqual([item.closed_qty for item in reduced], [Decimal("1"), Decimal("0.5")])

    def test_load_source_candles_reads_child_signal_source_fields(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.calls: list[tuple[str, str, int]] = []

            def get_candles(self, inst_id: str, bar: str, limit: int):
                self.calls.append((inst_id, bar, limit))
                return [
                    Candle(
                        ts=1,
                        open=Decimal("1"),
                        high=Decimal("2"),
                        low=Decimal("1"),
                        close=Decimal("2"),
                        volume=Decimal("10"),
                        confirmed=True,
                    )
                ]

        registry = EnhancedStrategyRegistry()
        register_seed_strategy_package(registry, spot_inst_id="BTC-USDT", signal_bar="5m")
        client = FakeClient()
        engine = EnhancedStrategyEngine(client, lambda _msg: None)
        state = SimpleNamespace(
            registry=registry,
            enabled_signal_ids=tuple(item.signal_id for item in registry.list_child_signals("spot_enhancement_36")),
        )

        candles_by_source = engine._load_source_candles(state)  # type: ignore[arg-type]

        self.assertIn(("BTC-USDT", "5m"), candles_by_source)
        self.assertEqual(client.calls[0][:2], ("BTC-USDT", "5m"))

    def _position(
        self,
        position_id: str,
        direction: str,
        status: str,
        quantity: Decimal,
    ) -> LiveEnhancedPosition:
        return LiveEnhancedPosition(
            position_id=position_id,
            signal_id=position_id,
            signal_name=position_id,
            playbook_id=position_id,
            playbook_name=position_id,
            playbook_action="SWAP_LONG" if direction == "long" else "SWAP_SHORT",
            direction=direction,  # type: ignore[arg-type]
            source_inst_id="BTC-USDT",
            source_bar="5m",
            trade_inst_id="BTC-USDT-SWAP",
            quantity=quantity,
            signal_price=Decimal("100"),
            entry_price=Decimal("100"),
            entry_ts=1,
            stop_loss_price=None,
            take_profit_price=None,
            max_hold_bars=18,
            fee_rate=Decimal("0"),
            slippage_rate=Decimal("0"),
            trigger_reason="test",
            pos_side="long" if direction == "long" else "short",  # type: ignore[arg-type]
            status=status,  # type: ignore[arg-type]
        )


if __name__ == "__main__":
    unittest.main()
