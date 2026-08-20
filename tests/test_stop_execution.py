from decimal import Decimal
import unittest

from okx_quant.stop_execution import assess_stop_execution


class StopExecutionTest(unittest.TestCase):
    def test_real_eth_case_is_critical_and_separates_slippage_from_fees(self) -> None:
        result = assess_stop_execution(
            direction="long",
            entry_price=Decimal("1881.90"),
            initial_stop_price=Decimal("1876.45"),
            effective_stop_price=Decimal("1876.45"),
            actual_exit_price=Decimal("1872.72"),
            size=Decimal("2.201"),
            actual_price_loss_usdt=Decimal("-20.21"),
        )

        assert result is not None
        self.assertEqual(result.status, "critical")
        self.assertAlmostEqual(float(result.planned_risk_usdt), 11.99545, places=4)
        self.assertAlmostEqual(float(result.stop_slippage_price), 3.73, places=4)
        self.assertAlmostEqual(float(result.stop_slippage_usdt), 8.20973, places=4)
        self.assertAlmostEqual(float(result.stop_overrun_usdt), 8.21455, places=4)
        self.assertAlmostEqual(float(result.stop_overrun_pct), 68.48, places=1)

    def test_dynamic_effective_stop_is_used_for_adverse_slippage(self) -> None:
        result = assess_stop_execution(
            direction="long",
            entry_price=Decimal("100"),
            initial_stop_price=Decimal("95"),
            effective_stop_price=Decimal("102"),
            actual_exit_price=Decimal("101"),
            size=Decimal("2"),
            actual_price_loss_usdt=Decimal("-2"),
        )

        assert result is not None
        self.assertEqual(result.stop_slippage_price, Decimal("1"))
        self.assertEqual(result.stop_slippage_usdt, Decimal("2"))


if __name__ == "__main__":
    unittest.main()
