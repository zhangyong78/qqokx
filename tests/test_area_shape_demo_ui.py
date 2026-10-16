from __future__ import annotations

import threading
import time
from decimal import Decimal
from types import SimpleNamespace

from PySide6.QtCore import QObject, QPoint, Signal

from okx_quant.area_shape_monitor import AreaShapeMonitor
from okx_quant.area_shape_trade import AreaShapePlan
from okx_quant.models import Candle
from roll_terminal_qt.kline_analysis_window import KlineAnalysisWindow, KlineChartPayload
from tests.qt_test_case import QtWidgetTestCase


HOUR = 3_600_000


class _DemoMonitor(QObject):
    session_updated = Signal(object)
    status_changed = Signal(str)

    def __init__(self) -> None:
        super().__init__()
        self._sessions: list[dict[str, object]] = []
        self.arm_calls: list[dict[str, object]] = []
        self.calls: list[tuple[str, str]] = []

    def sessions(self) -> list[dict[str, object]]:
        return [dict(item) for item in self._sessions]

    def request_arm(self, **kwargs) -> None:
        self.arm_calls.append(kwargs)

    def reconcile_readonly(self, session_id: str, *, runtime, unlocked: bool) -> dict[str, object]:
        self.calls.append(("reconcile", threading.current_thread().name))
        item = next(session for session in self._sessions if session["session_id"] == session_id)
        item["readonly_reconciled_at_ms"] = 123
        item["readonly_reconciliation"] = {"entry": {"state": "filled"}, "position": [], "stop": None, "take_profit": None}
        self.session_updated.emit(dict(item))
        return dict(item)

    def rearm_session(self, session_id: str, *, runtime, unlocked: bool) -> dict[str, object]:
        self.calls.append(("rearm", threading.current_thread().name))
        item = next(session for session in self._sessions if session["session_id"] == session_id)
        item["status"] = "armed"
        self.session_updated.emit(dict(item))
        return dict(item)


class AreaShapeDemoUiTests(QtWidgetTestCase):
    def setUp(self) -> None:
        import roll_terminal_qt.kline_analysis_window as kline_module

        self._kline_module = kline_module
        self._original_save = kline_module.save_kline_analysis_workspace_entries
        kline_module.save_kline_analysis_workspace_entries = lambda _entries: None
        self.window = KlineAnalysisWindow(embedded=True, preview_mode=True)
        self.window._workspace_entries = {}
        if self.window._api_profile_combo.findText("ui-test") < 0:
            self.window._api_profile_combo.addItem("ui-test")
        self.window._api_profile_combo.setCurrentText("ui-test")
        self.window._active_profile_name = lambda: "ui-test"
        self.window._active_environment = lambda: "demo"
        self.window._selected_symbol = lambda: "BTC-USDT-SWAP"
        self.window._render_to_chart = lambda _payload: None
        self.window._period_combo.setCurrentText("1H")
        self.window._runtime = SimpleNamespace(
            credential_profile_name="ui-test", environment="demo",
            credentials=SimpleNamespace(profile_name="ui-test"),
        )
        self.window._ensure_profile_access = lambda profile: profile == "ui-test"
        self.rows = [
            Candle(n * HOUR, Decimal("100"), Decimal("105"), Decimal("95"), Decimal("101"), Decimal("1"), True)
            for n in range(1, 21)
        ]
        self.window._pending_payload = KlineChartPayload(
            candles=[{"time": item.ts // 1000, "open": float(item.open), "high": float(item.high), "low": float(item.low), "close": float(item.close)} for item in self.rows],
            raw_candles=self.rows, ema_9=[], ema_21=[], ema_55=[], trend_indicator=[], signal_markers=[], box_overlays=[], stats={},
        )
        self.paper_monitor = AreaShapeMonitor(store_load=lambda: [], store_save=lambda _records: None)
        self.demo_monitor = _DemoMonitor()
        self.window.set_area_shape_monitor(self.paper_monitor)
        self.window.set_area_shape_execution_monitor(self.demo_monitor)

    def tearDown(self) -> None:
        self.window.begin_shutdown()
        self._app.processEvents()
        self.dispose_widget(self.window)
        self._kline_module.save_kline_analysis_workspace_entries = self._original_save

    def _create_demo_draft(self) -> AreaShapePlan:
        self.window._create_area_shape_plan()
        plans = self.paper_monitor.plans()
        self.assertEqual(len(plans), 1)
        return plans[0]

    def test_demo_start_is_one_action_after_unlock_and_paper_baseline(self) -> None:
        self._create_demo_draft()
        self._assert_area_controls_fit_viewport()
        self.window._area_shape_mode_combo.setCurrentIndex(self.window._area_shape_mode_combo.findData("demo"))

        self.window._start_area_shape_monitoring()

        self.assertEqual(len(self.demo_monitor.arm_calls), 1)
        call = self.demo_monitor.arm_calls[0]
        self.assertTrue(call["unlocked"])
        self.assertIs(call["paper_monitor"], self.paper_monitor)
        self.assertEqual(call["plan"].status, "monitoring")
        paper_plan = self.paper_monitor.plans()[0]
        self.assertEqual(paper_plan.status, "monitoring")
        self.assertEqual(paper_plan.execution_mode, "demo_exchange")
        self.assertIn("预检中", self.window._area_shape_status.text())

    def test_demo_start_obeys_profile_unlock_and_live_environment_gate(self) -> None:
        self._create_demo_draft()
        self.window._area_shape_mode_combo.setCurrentIndex(self.window._area_shape_mode_combo.findData("demo"))
        self.window._ensure_profile_access = lambda _profile: False

        self.window._start_area_shape_monitoring()

        self.assertEqual(self.demo_monitor.arm_calls, [])
        self.assertIn("尚未解锁", self.window._area_shape_demo_status.text())
        self.assertEqual(self.paper_monitor.plans()[0].status, "draft")

        self.window._ensure_profile_access = lambda _profile: True
        self.window._runtime = SimpleNamespace(
            credential_profile_name="ui-test", environment="live",
            credentials=SimpleNamespace(profile_name="ui-test"),
        )
        self.window._active_environment = lambda: "live"
        self.window._create_area_shape_plan()
        self.window._area_shape_mode_combo.setCurrentIndex(self.window._area_shape_mode_combo.findData("demo"))
        self.window._start_area_shape_monitoring()
        self.assertEqual(self.demo_monitor.arm_calls, [])
        self.assertIn("只允许OKX模拟盘", self.window._area_shape_demo_status.text())

    def test_readonly_reconcile_and_rearm_run_off_the_ui_thread(self) -> None:
        plan = self._create_demo_draft()
        self.demo_monitor._sessions.append({
            "session_id": "session-1", "plan_id": plan.plan_id, "profile_name": "ui-test",
            "environment": "demo", "symbol": "BTC-USDT-SWAP", "status": "needs_manual_rearm",
            "account_uid": "uid-demo", "events": [], "position": None, "pending_intent": None,
        })
        self.window._area_shape_mode_combo.setCurrentIndex(self.window._area_shape_mode_combo.findData("demo"))
        self.window._refresh_area_shape_execution_status()
        self.assertTrue(self.window._area_shape_reconcile_button.isEnabled())

        self.window._reconcile_area_shape_demo_session()
        self._wait_for_demo_action()
        self.assertEqual(self.demo_monitor.calls[0][0], "reconcile")
        self.assertNotEqual(self.demo_monitor.calls[0][1], threading.current_thread().name)
        self.assertTrue(self.window._area_shape_rearm_button.isEnabled())

        self.window._rearm_area_shape_demo_session()
        self._wait_for_demo_action()
        self.assertEqual(self.demo_monitor.calls[1][0], "rearm")
        self.assertNotEqual(self.demo_monitor.calls[1][1], threading.current_thread().name)
        self.assertIn("模拟盘 armed", self.window._area_shape_demo_status.text())

    def _assert_area_controls_fit_viewport(self) -> None:
        self.window.resize(1440, 920)
        self.window.show()
        self.window._toggle_left_panel_btn.setChecked(False)
        self.window._control_tabs.setCurrentIndex(self.window._control_area_tab_index)
        for _ in range(5):
            self._app.processEvents()

        viewport = self.window._control_scroll.viewport()
        self.assertLessEqual(self.window._control_panel.width(), viewport.width())
        self.assertLessEqual(self.window._control_tabs.width(), viewport.width())
        self.assertLessEqual(self.window._control_tabs.tabBar().width(), self.window._control_tabs.width())
        for widget in (
            self.window._area_shape_mode_combo,
            self.window._area_shape_create_button,
            self.window._area_shape_edit_button,
            self.window._area_shape_start_button,
            self.window._area_shape_pause_button,
            self.window._area_shape_cancel_button,
            self.window._area_shape_reconcile_button,
            self.window._area_shape_rearm_button,
        ):
            right_edge = widget.mapTo(viewport, QPoint(widget.width(), 0)).x()
            self.assertLessEqual(right_edge, viewport.width(), widget.text() if hasattr(widget, "text") else "area control clipped")

        analysis_index = self.window._control_tabs.indexOf(self.window._control_tabs.widget(0))
        original = self.window._control_width_policy_snapshot
        self.window._control_tabs.setCurrentIndex(analysis_index)
        self._app.processEvents()
        self.assertEqual(self.window._control_panel.sizePolicy().horizontalPolicy(), original[0].horizontalPolicy())
        self.assertEqual(self.window._control_tabs.sizePolicy().horizontalPolicy(), original[2].horizontalPolicy())
        self.window._control_tabs.setCurrentIndex(self.window._control_area_tab_index)
        self._app.processEvents()

    def _wait_for_demo_action(self) -> None:
        deadline = time.monotonic() + 3
        while self.window._area_shape_demo_action_pending and time.monotonic() < deadline:
            self._app.processEvents()
            time.sleep(0.01)
        self._app.processEvents()
        self.assertFalse(self.window._area_shape_demo_action_pending)
