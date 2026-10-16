from __future__ import annotations

from decimal import Decimal

from okx_quant.area_shape_monitor import AreaShapeMonitor
from okx_quant.area_shape_trade import AreaShapeConfig, AreaShapePlan
from okx_quant.models import Candle
from roll_terminal_qt.kline_analysis_window import KlineAnalysisWindow, KlineChartPayload
from tests.qt_test_case import QtWidgetTestCase


HOUR = 3_600_000


class AreaShapeKlineUiTests(QtWidgetTestCase):
    def setUp(self) -> None:
        self._original_save = __import__("roll_terminal_qt.kline_analysis_window", fromlist=["save_kline_analysis_workspace_entries"]).save_kline_analysis_workspace_entries
        import roll_terminal_qt.kline_analysis_window as kline_module

        kline_module.save_kline_analysis_workspace_entries = lambda _entries: None
        self.window = KlineAnalysisWindow(embedded=True, preview_mode=True)
        self.window._workspace_entries = {}
        self.window._active_profile_name = lambda: "ui-test"
        self.window._active_environment = lambda: "demo"
        self.window._selected_symbol = lambda: "BTC-USDT-SWAP"
        self.window._render_to_chart = lambda _payload: None
        self.window._period_combo.setCurrentText("1H")
        self.rows = [Candle(n * HOUR, Decimal("100"), Decimal("105"), Decimal("95"), Decimal("101"), Decimal("1"), True) for n in range(1, 21)]
        self.window._pending_payload = KlineChartPayload(
            candles=[{"time": c.ts // 1000, "open": float(c.open), "high": float(c.high), "low": float(c.low), "close": float(c.close)} for c in self.rows],
            raw_candles=self.rows, ema_9=[], ema_21=[], ema_55=[], trend_indicator=[], signal_markers=[], box_overlays=[], stats={},
        )
        self.saved: list[list[dict[str, object]]] = []
        self.monitor = AreaShapeMonitor(store_load=lambda: [], store_save=lambda records: self.saved.append(records))
        self.window.set_area_shape_monitor(self.monitor)
        self._kline_module = kline_module

    def tearDown(self) -> None:
        self.window.begin_shutdown()
        self._app.processEvents()
        self.dispose_widget(self.window)
        self._kline_module.save_kline_analysis_workspace_entries = self._original_save

    def test_repeated_one_click_preserves_dragged_prices(self) -> None:
        self.window._create_area_shape_plan()
        lines = self.window._workspace_entry(symbol="BTC-USDT-SWAP", period="1H")["lines"]
        lines[0]["price_a"] = lines[0]["price_b"] = 98.0
        lines[1]["price_a"] = lines[1]["price_b"] = 104.0

        self.window._create_area_shape_plan()

        plan = self.monitor.plans()[0]
        self.assertEqual((plan.support_price, plan.pressure_price), (Decimal("98.0"), Decimal("104.0")))
        self.assertEqual(len(lines), 2)

    def test_new_plan_reuses_last_context_parameters(self) -> None:
        previous = AreaShapePlan.create(
            profile_name="ui-test", environment="demo", symbol="BTC-USDT-SWAP",
            support_price=Decimal("98"), pressure_price=Decimal("104"),
            config=AreaShapeConfig(risk_amount=Decimal("33")),
        )
        previous.status = "paper_closed"
        self.monitor._plans[previous.plan_id] = previous

        self.window._create_area_shape_plan()

        plans = self.monitor.plans()
        self.assertEqual(len(plans), 2)
        new_plan = next(item for item in plans if item.status == "draft")
        self.assertEqual(new_plan.config.risk_amount, Decimal("33"))
