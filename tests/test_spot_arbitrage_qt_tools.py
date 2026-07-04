from __future__ import annotations

import unittest
from datetime import datetime, timezone
from decimal import Decimal

from okx_quant.arbitrage.models import ArbitrageOpportunity
from okx_quant.models import Candle
from roll_terminal_qt.spot_arbitrage_tools import (
    SpotArbitrageChartWidget,
    SpotArbitrageScanWidget,
    build_spot_arbitrage_spread_candles,
    format_scan_opportunity_cells,
    scan_opportunity_to_view,
)
from tests.qt_test_case import QtWidgetTestCase


class SpotArbitrageQtToolsTests(unittest.TestCase):
    def test_build_spot_arbitrage_spread_candles_matches_old_spread_formula(self) -> None:
        spot = [
            Candle(1_000, Decimal("100"), Decimal("110"), Decimal("90"), Decimal("105"), Decimal("1"), True),
            Candle(2_000, Decimal("120"), Decimal("125"), Decimal("115"), Decimal("121"), Decimal("1"), False),
        ]
        derivative = [
            Candle(1_000, Decimal("108"), Decimal("119"), Decimal("101"), Decimal("111"), Decimal("2"), True),
            Candle(3_000, Decimal("130"), Decimal("135"), Decimal("125"), Decimal("131"), Decimal("2"), True),
        ]

        spread = build_spot_arbitrage_spread_candles(spot, derivative)

        self.assertEqual(len(spread), 1)
        self.assertEqual(spread[0].ts, 1_000)
        self.assertEqual(spread[0].open, Decimal("8"))
        self.assertEqual(spread[0].high, Decimal("11"))
        self.assertEqual(spread[0].low, Decimal("6"))
        self.assertEqual(spread[0].close, Decimal("6"))
        self.assertTrue(spread[0].confirmed)

    def test_scan_opportunity_to_view_uses_spot_as_left_leg_and_professional_template(self) -> None:
        opportunity = ArbitrageOpportunity(
            base_ccy="BTC",
            pair_kind="spot_future",
            pair_kind_label="现货+次季",
            spot_inst_id="BTC-USDT",
            derivative_inst_id="BTC-USD-260925",
            spot_mid=Decimal("60000"),
            derivative_mid=Decimal("61000"),
            basis_abs=Decimal("1000"),
            basis_pct=Decimal("1.6667"),
            funding_rate=None,
            funding_annual_pct=None,
            fee_round_trip_pct=Decimal("0.212"),
            slippage_est_pct=Decimal("0.01"),
            net_annual_pct=Decimal("3.25"),
            days_to_expiry=83,
            scanned_at=datetime(2026, 7, 5, tzinfo=timezone.utc),
        )

        view = scan_opportunity_to_view(opportunity)

        self.assertEqual(view.left_inst_id, "BTC-USDT")
        self.assertEqual(view.right_inst_id, "BTC-USD-260925")
        self.assertEqual(view.left_kind, "现货")
        self.assertEqual(view.right_kind, "衍生品")
        self.assertEqual(view.template, "professional")
        self.assertIn("净年化 3.25%", view.description)

    def test_format_scan_opportunity_cells_keeps_dash_for_missing_funding_and_expiry(self) -> None:
        opportunity = ArbitrageOpportunity(
            base_ccy="ETH",
            pair_kind="spot_swap",
            pair_kind_label="现货+永续",
            spot_inst_id="ETH-USDT",
            derivative_inst_id="ETH-USDT-SWAP",
            spot_mid=Decimal("3000"),
            derivative_mid=Decimal("3010"),
            basis_abs=Decimal("10"),
            basis_pct=Decimal("0.3333"),
            funding_rate=None,
            funding_annual_pct=None,
            fee_round_trip_pct=Decimal("0.212"),
            slippage_est_pct=Decimal("0.015"),
            net_annual_pct=Decimal("1.88"),
            days_to_expiry=None,
            scanned_at=datetime(2026, 7, 5, tzinfo=timezone.utc),
        )

        cells = format_scan_opportunity_cells(opportunity)

        self.assertEqual(cells["base"], "ETH")
        self.assertEqual(cells["funding"], "-")
        self.assertEqual(cells["days"], "-")
        self.assertEqual(cells["spot"], "ETH-USDT")
        self.assertEqual(cells["derivative"], "ETH-USDT-SWAP")


class SpotArbitrageQtWidgetTests(QtWidgetTestCase):
    def test_scan_widget_uses_old_scan_table_columns(self) -> None:
        widget = SpotArbitrageScanWidget()
        try:
            headers = [
                widget._table.horizontalHeaderItem(index).text()
                for index in range(widget._table.columnCount())
            ]
            self.assertEqual(headers[:4], ["币种", "类型", "现货", "衍生品"])
            self.assertIn("净年化%", headers)
        finally:
            self.__class__.dispose_widget(widget)

    def test_chart_widget_defaults_to_old_spot_arbitrage_inputs(self) -> None:
        widget = SpotArbitrageChartWidget()
        try:
            self.assertEqual(widget._spot_input.text(), "BTC-USDT")
            self.assertEqual(widget._limit_input.text(), "300")
            self.assertEqual(widget._bar_combo.currentData(), "4H")
        finally:
            self.__class__.dispose_widget(widget)


if __name__ == "__main__":
    unittest.main()
