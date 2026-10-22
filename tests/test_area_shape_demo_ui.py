from __future__ import annotations

import threading
import time
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

from PySide6.QtCore import QObject, QPoint, Signal

from okx_quant.area_shape_monitor import AreaShapeMonitor
from okx_quant.area_shape_trade import AreaShapeConfig, AreaShapePlan
from okx_quant.area_shape_store import load_area_shape_plans, save_area_shape_plans
from okx_quant.models import Candle
from okx_quant.persistence import load_kline_analysis_workspace_entries
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
        self._confirm_patch = patch(
            "roll_terminal_qt.kline_analysis_window.QMessageBox.question",
            return_value=self._kline_module.QMessageBox.StandardButton.Yes,
        )
        self._confirm_mock = self._confirm_patch.start()
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
        self._confirm_patch.stop()
        self._kline_module.save_kline_analysis_workspace_entries = self._original_save

    def _create_demo_draft(self) -> AreaShapePlan:
        self.window._control_tabs.setCurrentIndex(self.window._control_area_tab_index)
        self.window._area_shape_create_button.click()
        plans = self.paper_monitor.plans()
        self.assertEqual(len(plans), 1)
        self.assertEqual(self.window._control_tabs.currentIndex(), self.window._control_area_tab_index)
        self.assertEqual(self.window._area_shape_create_button.text(), "两线已生成")
        self.assertFalse(self.window._area_shape_create_button.isEnabled())
        self.assertTrue(self.window._area_pair_lines_exist(plans[0]))
        workspace_lines = self.window._workspace_entry(symbol="BTC-USDT-SWAP", period="1H")["lines"]
        self.assertEqual(
            sum(str(item.get("id", "")) in {plans[0].support_line_id, plans[0].pressure_line_id} for item in workspace_lines),
            2,
        )
        chart_lines = getattr(self.window._native_chart_view, "_workspace_lines", [])
        if chart_lines:
            self.assertEqual(
                {str(item.get("id", "")) for item in chart_lines}
                & {plans[0].support_line_id, plans[0].pressure_line_id},
                {plans[0].support_line_id, plans[0].pressure_line_id},
            )
        return plans[0]

    def test_manual_area_prices_update_draft_and_both_chart_rays(self) -> None:
        plan = self._create_demo_draft()

        self.window._apply_area_shape_prices(plan, Decimal("90"), Decimal("110"))

        self.assertEqual(plan.support_price, Decimal("90"))
        self.assertEqual(plan.pressure_price, Decimal("110"))
        lines = self.window._workspace_entry(symbol=plan.symbol, period=plan.period)["lines"]
        support = next(item for item in lines if item["id"] == plan.support_line_id)
        pressure = next(item for item in lines if item["id"] == plan.pressure_line_id)
        self.assertEqual((support["price_a"], support["price_b"]), (90.0, 90.0))
        self.assertEqual((pressure["price_a"], pressure["price_b"]), (110.0, 110.0))
        saved_plan = self.paper_monitor.plans()[0]
        self.assertEqual(saved_plan.support_price, Decimal("90"))
        self.assertEqual(saved_plan.pressure_price, Decimal("110"))
        self.assertIn("下方支撑：90", self.window._area_shape_lines.text())
        self.assertIn("上方压力：110", self.window._area_shape_lines.text())

    def test_delete_frozen_area_line_explains_bound_account_and_safe_next_step(self) -> None:
        plan = self._create_demo_draft()
        self.paper_monitor.start_plan(
            plan.plan_id, last_closed_candle_ts=20 * HOUR,
            profile_name=plan.profile_name, environment=plan.environment,
        )
        lines_before = list(self.window._workspace_entry()["lines"])
        self.window._populate_line_table(selected_index=0)

        self.window._delete_selected_line()

        self.assertEqual(self.window._workspace_entry()["lines"], lines_before)
        status = self.window._area_shape_status.text()
        self.assertIn("ui-test/demo", status)
        self.assertIn("切换账户不会解除绑定", status)
        self.assertIn("先在智能辅助交易中取消计划", status)

    def test_manual_area_prices_reject_invalid_support_pressure_order(self) -> None:
        plan = self._create_demo_draft()
        original_prices = plan.support_price, plan.pressure_price

        with self.assertRaisesRegex(ValueError, "支撑低于上方压力"):
            self.window._apply_area_shape_prices(plan, Decimal("110"), Decimal("100"))

        self.assertEqual((plan.support_price, plan.pressure_price), original_prices)

    def test_manual_area_price_dialog_accepts_typed_price(self) -> None:
        plan = self._create_demo_draft()

        def accept_dialog(dialog) -> int:
            edits = dialog.findChildren(self._kline_module.QLineEdit)
            edits[0].setText("80000")
            edits[1].setText("84000")
            dialog.findChild(self._kline_module.QDialogButtonBox).button(
                self._kline_module.QDialogButtonBox.StandardButton.Ok,
            ).click()
            return dialog.result()

        with patch.object(self._kline_module.QDialog, "exec", new=accept_dialog):
            self.window._edit_area_shape_prices()

        saved_plan = self.paper_monitor.plans()[0]
        self.assertEqual(saved_plan.support_price, Decimal("80000"))
        self.assertEqual(saved_plan.pressure_price, Decimal("84000"))
        self.assertIn("上方压力：84000", self.window._area_shape_lines.text())

    def test_manual_area_price_rebuilds_real_chart_series(self) -> None:
        plan = self._create_demo_draft()
        self.window._render_to_chart = KlineAnalysisWindow._render_to_chart.__get__(self.window)
        self.window._use_native_chart = True
        self.window._native_chart, self.window._native_chart_view = self.window._create_native_chart_view(
            title="测试区域线", allow_draw_clicks=False,
        )
        self.window._native_chart_view.setParent(self.window)

        self.window._apply_area_shape_prices(plan, Decimal("90"), Decimal("110"))

        chart_lines = {item["id"]: item for item in self.window._native_chart_view._workspace_lines}
        self.assertEqual(chart_lines[plan.support_line_id]["price_a"], 90.0)
        self.assertEqual(chart_lines[plan.pressure_line_id]["price_a"], 110.0)
        records = self.window._workspace_entry(symbol=plan.symbol, period=plan.period)["lines"]
        for line_id, price in ((plan.support_line_id, 90.0), (plan.pressure_line_id, 110.0)):
            label = next(item["label"] for item in records if item["id"] == line_id)
            series = next(item for item in self.window._native_chart.series() if item.name() == label)
            self.assertTrue(series.points())
            self.assertTrue(all(point.y() == price for point in series.points()))

    def test_manual_price_cancel_keeps_plan_and_rays_unchanged(self) -> None:
        plan = self._create_demo_draft()
        before = self._kline_module.copy.deepcopy(self.window._workspace_entries)

        def cancel_dialog(dialog) -> int:
            dialog.findChildren(self._kline_module.QLineEdit)[0].setText("90")
            buttons = dialog.findChild(self._kline_module.QDialogButtonBox)
            buttons.button(self._kline_module.QDialogButtonBox.StandardButton.Cancel).click()
            return dialog.result()

        with patch.object(self._kline_module.QDialog, "exec", new=cancel_dialog):
            self.window._edit_area_shape_prices()

        self.assertEqual(self.window._workspace_entries, before)
        self.assertEqual(self.paper_monitor.get_plan(plan.plan_id).support_price, plan.support_price)

    def test_failed_draft_save_keeps_monitor_and_rays_at_old_prices(self) -> None:
        plan = self._create_demo_draft()
        before = self._kline_module.copy.deepcopy(self.window._workspace_entries)

        with patch.object(self.paper_monitor, "_store_save", side_effect=OSError("disk full")):
            with self.assertRaises(OSError):
                self.window._apply_area_shape_prices(plan, Decimal("90"), Decimal("110"))

        self.assertEqual(self.window._workspace_entries, before)
        self.assertEqual(self.paper_monitor.get_plan(plan.plan_id).support_price, Decimal("95"))
        self.assertEqual(self.paper_monitor.get_plan(plan.plan_id).pressure_price, Decimal("105"))

    def test_failed_price_apply_keeps_dialog_open_for_retry(self) -> None:
        plan = self._create_demo_draft()

        def retry_dialog(dialog) -> int:
            edits = dialog.findChildren(self._kline_module.QLineEdit)
            edits[0].setText("90")
            edits[1].setText("110")
            apply_button = dialog.findChild(self._kline_module.QDialogButtonBox).button(
                self._kline_module.QDialogButtonBox.StandardButton.Ok,
            )
            with patch.object(self.paper_monitor, "_store_save", side_effect=OSError("disk full")):
                apply_button.click()
            self.assertEqual(dialog.result(), int(self._kline_module.QDialog.DialogCode.Rejected))
            self.assertEqual(edits[0].text(), "90")
            self.assertEqual(self.paper_monitor.get_plan(plan.plan_id).support_price, Decimal("95"))
            apply_button.click()
            self.assertEqual(dialog.result(), int(self._kline_module.QDialog.DialogCode.Accepted))
            return dialog.result()

        with (
            patch.object(self._kline_module.QDialog, "exec", new=retry_dialog),
            patch.object(self._kline_module.QMessageBox, "warning"),
        ):
            self.window._edit_area_shape_prices()
        self.assertEqual(self.paper_monitor.get_plan(plan.plan_id).support_price, Decimal("90"))

    def test_failed_workspace_save_rolls_draft_prices_back(self) -> None:
        plan = self._create_demo_draft()
        before = self._kline_module.copy.deepcopy(self.window._workspace_entries)
        with patch.object(self.window, "_save_workspace_snapshot", side_effect=OSError("disk full")):
            with self.assertRaises(OSError):
                self.window._apply_area_shape_prices(plan, Decimal("90"), Decimal("110"))
        self.assertEqual(self.window._workspace_entries, before)
        self.assertEqual(self.paper_monitor.get_plan(plan.plan_id).support_price, Decimal("95"))

    def test_price_dialog_rejects_invalid_input_without_modifying_plan(self) -> None:
        plan = self._create_demo_draft()
        before = self._kline_module.copy.deepcopy(self.window._workspace_entries)

        def invalid_dialog(dialog) -> int:
            edits = dialog.findChildren(self._kline_module.QLineEdit)
            button = dialog.findChild(self._kline_module.QDialogButtonBox).button(
                self._kline_module.QDialogButtonBox.StandardButton.Ok,
            )
            for support, pressure in (("", "110"), ("abc", "110"), ("0", "110"), ("-1", "110"),
                                      ("110", "110"), ("120", "110"), ("NaN", "110"),
                                      ("90", "Infinity"), ("90", "1e999")):
                with self.subTest(support=support, pressure=pressure):
                    edits[0].setText(support)
                    edits[1].setText(pressure)
                    button.click()
                    self.assertEqual(dialog.result(), int(self._kline_module.QDialog.DialogCode.Rejected))
                    self.assertEqual(self.window._workspace_entries, before)
                    self.assertEqual(self.paper_monitor.get_plan(plan.plan_id).support_price, Decimal("95"))
            dialog.reject()
            return dialog.result()

        with (
            patch.object(self._kline_module.QDialog, "exec", new=invalid_dialog),
            patch.object(self._kline_module.QMessageBox, "warning"),
        ):
            self.window._edit_area_shape_prices()

    def test_price_dialog_cannot_overwrite_plan_activated_while_open(self) -> None:
        plan = self._create_demo_draft()
        before = self._kline_module.copy.deepcopy(self.window._workspace_entries)

        def stale_dialog(dialog) -> int:
            edits = dialog.findChildren(self._kline_module.QLineEdit)
            edits[0].setText("90")
            edits[1].setText("110")
            self.paper_monitor.start_plan(
                plan.plan_id, last_closed_candle_ts=20 * HOUR,
                profile_name=plan.profile_name, environment=plan.environment,
            )
            dialog.findChild(self._kline_module.QDialogButtonBox).button(
                self._kline_module.QDialogButtonBox.StandardButton.Ok,
            ).click()
            self.assertEqual(dialog.result(), int(self._kline_module.QDialog.DialogCode.Rejected))
            dialog.reject()
            return dialog.result()

        with (
            patch.object(self._kline_module.QDialog, "exec", new=stale_dialog),
            patch.object(self._kline_module.QMessageBox, "warning"),
        ):
            self.window._edit_area_shape_prices()
        self.assertEqual(self.window._workspace_entries, before)
        self.assertEqual(self.paper_monitor.get_plan(plan.plan_id).status, "monitoring")
        self.assertFalse(self.window._area_shape_price_button.isEnabled())

    def test_manual_prices_survive_workspace_and_plan_file_reload(self) -> None:
        plan = self._create_demo_draft()
        with TemporaryDirectory() as directory:
            workspace_path = Path(directory) / "workspace.json"
            plan_path = Path(directory) / "plans.json"
            self.paper_monitor._store_save = lambda records: save_area_shape_plans(records, plan_path)
            with patch.object(
                self.window, "_save_workspace_snapshot",
                side_effect=lambda: self._original_save(self.window._workspace_entries, workspace_path),
            ):
                self.window._apply_area_shape_prices(plan, Decimal("80970.2"), Decimal("84000"))
            restored_monitor = AreaShapeMonitor(
                store_load=lambda: load_area_shape_plans(plan_path), store_save=lambda _records: None,
            )
            self.window._workspace_entries = load_kline_analysis_workspace_entries(workspace_path)
            restored = restored_monitor.get_plan(plan.plan_id)
            self.assertEqual(restored.support_price, Decimal("80970.2"))
            self.assertEqual(restored.pressure_price, Decimal("84000"))
            self.assertEqual(self.window._area_line_price(plan.support_line_id), restored.support_price)
            self.assertEqual(self.window._area_line_price(plan.pressure_line_id), restored.pressure_price)

    def test_demo_start_is_one_action_after_unlock_and_paper_baseline(self) -> None:
        self._create_demo_draft()
        self._assert_area_controls_fit_viewport()
        self.window._area_shape_mode_combo.setCurrentIndex(self.window._area_shape_mode_combo.findData("demo"))

        self.window._start_area_shape_monitoring()

        confirmation = self._confirm_mock.call_args.args[2]
        self.assertIn("做多", confirmation)
        self.assertIn("下方支撑线", confirmation)
        self.assertIn("上方压力线", confirmation)
        self.assertEqual(len(self.demo_monitor.arm_calls), 1)
        call = self.demo_monitor.arm_calls[0]
        self.assertTrue(call["unlocked"])
        self.assertIs(call["paper_monitor"], self.paper_monitor)
        self.assertEqual(call["plan"].status, "monitoring")
        paper_plan = self.paper_monitor.plans()[0]
        self.assertEqual(paper_plan.status, "monitoring")
        self.assertEqual(paper_plan.execution_mode, "demo_exchange")
        self.assertIn("预检中", self.window._area_shape_status.text())

    def test_dragging_one_ray_origin_moves_both_origins_together(self) -> None:
        plan = self._create_demo_draft()
        lines = self.window._workspace_entry(symbol="BTC-USDT-SWAP", period="1H")["lines"]
        support_index = next(index for index, item in enumerate(lines) if item["id"] == plan.support_line_id)
        pressure_index = next(index for index, item in enumerate(lines) if item["id"] == plan.pressure_line_id)
        for index, target_time in ((support_index, 3 * 3600), (pressure_index, 5 * 3600)):
            line = lines[index]
            old_time = line["time_a"]
            self.window._line_drag_state = {
                "index": index, "drag_mode": "endpoint_a", "anchor_line": dict(line),
                "anchor_candle_time": old_time, "anchor_price": line["price_a"],
            }
            self.assertTrue(self.window._apply_line_drag_update(candle_time=target_time, price=line["price_a"]))
            lines = self.window._workspace_entry(symbol="BTC-USDT-SWAP", period="1H")["lines"]
            support = next(item for item in lines if item["id"] == plan.support_line_id)
            pressure = next(item for item in lines if item["id"] == plan.pressure_line_id)
            self.assertEqual(support["time_a"], target_time)
            self.assertEqual(support["time_b"], target_time)
            self.assertEqual(pressure["time_a"], target_time)
            self.assertEqual(pressure["time_b"], target_time)

    def test_dragging_area_price_refreshes_values_and_plan_details(self) -> None:
        plan = self._create_demo_draft()
        lines = self.window._workspace_entry(symbol=plan.symbol, period=plan.period)["lines"]
        support_index = next(index for index, item in enumerate(lines) if item["id"] == plan.support_line_id)
        support_line = lines[support_index]
        self.window._line_drag_state = {
            "index": support_index,
            "drag_mode": "move",
            "anchor_line": dict(support_line),
            "anchor_candle_time": support_line["time_a"],
            "anchor_price": support_line["price_a"],
        }

        with patch.object(self.window, "_resolve_primary_chart_click", return_value=(support_line["time_a"], 90.0)):
            self.window._on_chart_pointer_released(0.0, 0.0)

        self.assertIn("下方支撑：90", self.window._area_shape_lines.text())
        self.assertIn("入场参考：下方支撑 90", self.window._area_shape_plan_details.text())
        self.assertEqual(self.paper_monitor.plans()[0].support_price, Decimal("90"))

    def test_dragging_support_above_pressure_is_rejected_before_chart_changes(self) -> None:
        plan = self._create_demo_draft()
        lines = self.window._workspace_entry(symbol=plan.symbol, period=plan.period)["lines"]
        support_index = next(index for index, item in enumerate(lines) if item["id"] == plan.support_line_id)
        self.window._line_drag_state = {"index": support_index, "drag_mode": "move"}

        self.assertFalse(self.window._apply_line_drag_update(candle_time=3 * 3600, price=110.0))

        self.assertEqual(self.window._area_line_price(plan.support_line_id), Decimal("95"))
        self.assertEqual(self.paper_monitor.get_plan(plan.plan_id).support_price, Decimal("95"))

    def test_future_projection_builds_selected_pattern_over_requested_horizon(self) -> None:
        plan = self._create_demo_draft()
        projection = self.window._build_area_shape_projection(plan, "double_reversal_up", 12)
        self.assertEqual(len(projection["bars"]), 12)
        self.assertEqual(projection["label"], "双线向上反转")
        self.assertEqual(projection["entry"], float(plan.support_price))
        self.assertIn("stop", projection)
        approach = projection["bars"][:projection["approach_bars"]]
        self.assertTrue(any(bar["close"] > bar["open"] for bar in approach))
        self.assertTrue(any(bar["close"] < bar["open"] for bar in approach))
        self.assertTrue(all(bar["low"] > projection["entry"] for bar in approach))

    def test_future_projection_continues_ema15_and_sma50_from_projected_closes(self) -> None:
        candles = [{"close": float(value)} for value in range(1, 61)]
        bars = [
            {"time_ms": 1000.0, "close": 61.0},
            {"time_ms": 2000.0, "close": 62.0},
        ]
        ema_points = self._kline_module._future_moving_average_points(candles, bars, 15, exponential=True)
        sma_points = self._kline_module._future_moving_average_points(candles, bars, 50, exponential=False)
        expected_ema = self._kline_module._to_ema([*range(1, 61), 61.0, 62.0], 15)[-2:]
        expected_sma = self._kline_module._to_sma([*range(1, 61), 61.0, 62.0], 50)[-2:]
        self.assertEqual([point[0] for point in ema_points], [1000.0, 2000.0])
        self.assertEqual([point[1] for point in ema_points], expected_ema)
        self.assertEqual([point[1] for point in sma_points], expected_sma)

    def test_projection_varies_bodies_and_wicks_but_is_repeatable(self) -> None:
        plan = self._create_demo_draft()
        first = self.window._build_area_shape_projection(plan, "double_reversal_up", 24, "long")
        repeated = self.window._build_area_shape_projection(plan, "double_reversal_up", 24, "long")
        self.assertEqual(first, repeated)
        approach = first["bars"][:first["approach_bars"]]
        bodies = {round(abs(bar["close"] - bar["open"]), 6) for bar in approach}
        upper_wicks = {round(bar["high"] - max(bar["open"], bar["close"]), 6) for bar in approach}
        lower_wicks = {round(min(bar["open"], bar["close"]) - bar["low"], 6) for bar in approach}
        self.assertGreater(len(bodies), len(approach) // 2)
        self.assertGreater(len(upper_wicks), len(approach) // 2)
        self.assertGreater(len(lower_wicks), len(approach) // 2)
        self.assertTrue(any(bar["close"] > bar["open"] for bar in approach))
        self.assertTrue(any(bar["close"] < bar["open"] for bar in approach))

    def test_far_projection_extends_horizon_without_oversized_candles(self) -> None:
        plan = self._create_demo_draft()
        plan.pressure_price = Decimal("300")
        projection = self.window._build_area_shape_projection(plan, "double_reversal_down", 4, "short")
        bars = projection["bars"]
        self.assertGreater(len(bars), 4)
        self.assertLessEqual(len(bars), 98)
        self.assertLessEqual(max(abs(bar["close"] - bar["open"]) for bar in bars), 30.0)
        self.assertTrue(all(bar["high"] < 300.0 for bar in bars[:projection["approach_bars"]]))
        self.assertGreaterEqual(max(bar["high"] for bar in bars[-2:]), 300.0)
        previous_close = self.window._pending_payload.candles[-1]["close"]
        for bar in bars:
            self.assertAlmostEqual(bar["open"], previous_close)
            previous_close = bar["close"]

    def test_unreachable_projection_does_not_force_a_jump_to_the_setup(self) -> None:
        plan = self._create_demo_draft()
        plan.pressure_price = Decimal("5000")
        projection = self.window._build_area_shape_projection(plan, "top_fractal", 12, "short")
        self.assertFalse(projection["setup_reached"])
        self.assertIn("尚未到关键线", projection["label"])
        self.assertEqual(len(projection["bars"]), 96)
        self.assertNotIn("stop", projection)
        self.assertLess(max(bar["high"] for bar in projection["bars"]), 5000.0)
        self.assertLessEqual(max(abs(bar["close"] - bar["open"]) for bar in projection["bars"]), 30.0)

    def test_future_projection_supports_short_and_places_shape_at_pressure_line(self) -> None:
        plan = self._create_demo_draft()
        projection = self.window._build_area_shape_projection(plan, "double_reversal_down", 12, "short")
        self.assertEqual(projection["direction"], "short")
        self.assertEqual(projection["entry"], float(plan.pressure_price))
        self.assertEqual(projection["target"], float(plan.support_price))
        signal_bars = projection["bars"][-2:]
        self.assertGreaterEqual(max(bar["high"] for bar in signal_bars), projection["entry"])
        self.assertTrue(any(bar["close"] < bar["open"] for bar in signal_bars))
        self.assertTrue(any(bar["close"] > bar["open"] for bar in signal_bars))

    def test_projection_candles_connect_without_gaps_for_both_sides_and_all_shapes(self) -> None:
        plan = self._create_demo_draft()
        patterns = {
            "long": ("big_bullish", "long_lower_shadow", "double_reversal_up", "inside_bar", "bottom_fractal"),
            "short": ("big_bearish", "long_upper_shadow", "double_reversal_down", "inside_bar", "top_fractal"),
        }
        for side, choices in patterns.items():
            for pattern in choices:
                for horizon in (4, 12, 40):
                    with self.subTest(side=side, pattern=pattern, horizon=horizon):
                        projection = self.window._build_area_shape_projection(plan, pattern, horizon, side)
                        bars = projection["bars"]
                        self.assertEqual(len(bars), horizon)
                        previous_close = self.window._pending_payload.candles[-1]["close"]
                        for bar in bars:
                            self.assertAlmostEqual(bar["open"], previous_close)
                            self.assertLessEqual(bar["low"], min(bar["open"], bar["close"]))
                            self.assertGreaterEqual(bar["high"], max(bar["open"], bar["close"]))
                            previous_close = bar["close"]

    def test_future_projection_is_restored_after_chart_refresh(self) -> None:
        plan = self._create_demo_draft()
        projection = self.window._build_area_shape_projection(plan, "double_reversal_up", 12)
        projection["_context_symbol"] = "BTC-USDT-SWAP"
        projection["_context_period"] = "1H"
        self.window._area_shape_projection = projection

        class _ChartStub:
            projection = None

            def set_future_projection(self, value) -> None:
                self.projection = value

        chart = _ChartStub()
        self.window._restore_area_shape_projection(chart, symbol="BTC-USDT-SWAP", period="1H")
        self.assertIs(chart.projection, projection)

        self.window._restore_area_shape_projection(chart, symbol="ETH-USDT-SWAP", period="1H")
        self.assertIsNone(self.window._area_shape_projection)
        self.assertEqual(self.window._area_shape_projection_button.text(), "未来推演")

    def test_replay_only_processes_candles_after_shared_origin(self) -> None:
        plan = self._create_demo_draft()
        plan.support_price = Decimal("95")
        plan.pressure_price = Decimal("120")
        plan.config = AreaShapeConfig(
            direction="long", atr_period=2, min_space_r=Decimal("1"), stop_offset_atr=Decimal("0"),
            entry_trigger_mode="direct_touch", signal_patterns=("double_reversal_up",),
        )
        candles = [
            Candle(0, Decimal("104"), Decimal("105"), Decimal("102"), Decimal("103"), Decimal("1"), True),
            Candle(HOUR, Decimal("101"), Decimal("102"), Decimal("99"), Decimal("100"), Decimal("1"), True),
            Candle(2 * HOUR, Decimal("96"), Decimal("97"), Decimal("94"), Decimal("96"), Decimal("1"), True),
            Candle(3 * HOUR, Decimal("96"), Decimal("100"), Decimal("94"), Decimal("98"), Decimal("1"), True),
            Candle(4 * HOUR, Decimal("98"), Decimal("102"), Decimal("97"), Decimal("101"), Decimal("1"), True),
        ]
        rows = self.window._simulate_area_shape_replay(plan, candles, start_ms=HOUR, sides=("long",))
        self.assertEqual(len(rows), 1)
        self.assertGreater(rows[0]["entry_ts"], HOUR)
        self.assertEqual(rows[0]["outcome"], "paper_take_profit")

    def test_first_ray_touch_is_reference_only_and_later_touch_can_trade(self) -> None:
        plan = self._create_demo_draft()
        plan.support_price = Decimal("95")
        plan.pressure_price = Decimal("150")
        plan.config = AreaShapeConfig(
            direction="long", min_space_r=Decimal("1"), entry_trigger_mode="direct_touch",
            stop_basis="fixed_distance", fixed_stop_distance=Decimal("10"),
            target_buffer_atr=Decimal("0"), round_trip_fee_rate=Decimal("0"),
            break_even_enabled=False,
        )
        candles = [
            Candle(HOUR, Decimal("100"), Decimal("101"), Decimal("99"), Decimal("100"), Decimal("1"), True),
            Candle(2 * HOUR, Decimal("100"), Decimal("101"), Decimal("94"), Decimal("96"), Decimal("1"), True),
            Candle(3 * HOUR, Decimal("96"), Decimal("101"), Decimal("96"), Decimal("100"), Decimal("1"), True),
            Candle(4 * HOUR, Decimal("100"), Decimal("102"), Decimal("94"), Decimal("96"), Decimal("1"), True),
            Candle(5 * HOUR, Decimal("95"), Decimal("100"), Decimal("94"), Decimal("98"), Decimal("1"), True),
            Candle(6 * HOUR, Decimal("98"), Decimal("120"), Decimal("97"), Decimal("115"), Decimal("1"), True),
        ]

        rows = self.window._simulate_area_shape_replay(plan, candles, start_ms=HOUR, sides=("long",))

        trades = [row for row in rows if row.get("record_type") == "trade"]
        self.assertEqual(len(trades), 1, rows)
        self.assertEqual(trades[0]["signal_ts"], 4 * HOUR)
        self.assertEqual(trades[0]["entry_ts"], 5 * HOUR)

    def test_replay_keeps_insufficient_space_as_potential_opportunity(self) -> None:
        plan = self._create_demo_draft()
        plan.support_price = Decimal("95")
        plan.pressure_price = Decimal("102")
        plan.config = AreaShapeConfig(
            direction="long", min_space_r=Decimal("1"), entry_trigger_mode="direct_touch",
            stop_basis="fixed_distance", fixed_stop_distance=Decimal("10"),
            break_even_enabled=False,
        )
        candles = [
            Candle(HOUR, Decimal("100"), Decimal("101"), Decimal("99"), Decimal("100"), Decimal("1"), True),
            Candle(2 * HOUR, Decimal("100"), Decimal("102"), Decimal("94"), Decimal("100"), Decimal("1"), True),
            Candle(3 * HOUR, Decimal("100"), Decimal("101"), Decimal("99"), Decimal("100"), Decimal("1"), True),
            Candle(4 * HOUR, Decimal("100"), Decimal("102"), Decimal("94"), Decimal("100"), Decimal("1"), True),
            Candle(5 * HOUR, Decimal("100"), Decimal("101"), Decimal("99"), Decimal("100"), Decimal("1"), True),
        ]

        rows = self.window._simulate_area_shape_replay(plan, candles, start_ms=HOUR, sides=("long",))

        self.assertEqual(len(rows), 1, rows)
        self.assertEqual(rows[0]["record_type"], "opportunity")
        self.assertEqual(rows[0]["entry_ts"], 5 * HOUR)
        self.assertEqual(rows[0]["entry"], Decimal("95"))
        self.assertLess(rows[0]["space_r"], Decimal("1"))
        self.assertIn("盈亏比不足", rows[0]["outcome"])

    def test_replay_uses_fixed_net_2r_exit_for_both_sides(self) -> None:
        cases = (
            ("long", Decimal("95"), Decimal("150"), ("100", "102", "94", "100"),
             ("100", "102", "94", "100"), ("100", "110", "99", "105"), ("105", "135", "100", "130")),
            ("short", Decimal("95"), Decimal("150"), ("150", "156", "149", "149"),
             ("149", "151", "148", "150"), ("150", "151", "140", "145"), ("145", "150", "120", "130")),
        )
        for side, support, pressure, touch, second_touch, entry_bar, target_bar in cases:
            with self.subTest(side=side):
                plan = self._create_demo_draft()
                plan.support_price = support
                plan.pressure_price = pressure
                plan.config = AreaShapeConfig(
                    direction=side, min_space_r=Decimal("2"), entry_trigger_mode="direct_touch",
                    stop_basis="fixed_distance", fixed_stop_distance=Decimal("10"),
                    exit_template="lock_2r_at_3r", break_even_enabled=True,
                )
                candles = [
                    Candle(HOUR, Decimal("100"), Decimal("101"), Decimal("99"), Decimal("100"), Decimal("1"), True),
                    Candle(2 * HOUR, *(Decimal(value) for value in touch), Decimal("1"), True),
                    Candle(3 * HOUR, *(Decimal(value) for value in second_touch), Decimal("1"), True),
                    Candle(4 * HOUR, *(Decimal(value) for value in entry_bar), Decimal("1"), True),
                    Candle(5 * HOUR, *(Decimal(value) for value in target_bar), Decimal("1"), True),
                ]

                rows = self.window._simulate_area_shape_replay(plan, candles, start_ms=HOUR, sides=(side,))

                trades = [row for row in rows if row.get("record_type") == "trade"]
                self.assertEqual(len(trades), 1, rows)
                self.assertEqual(trades[0]["outcome"], "paper_take_profit")
                self.assertAlmostEqual(float(trades[0]["r"]), 2.0, places=6)

    def test_replay_stop_uses_stop_price_and_only_two_sided_fees(self) -> None:
        plan = self._create_demo_draft()
        plan.config = AreaShapeConfig(
            round_trip_fee_rate=Decimal("0.00036"), slippage_rate=Decimal("0.01"),
        )
        trade = {
            "side": "short", "entry": Decimal("100"), "stop": Decimal("110"),
            "risk": Decimal("10"), "entry_ts": HOUR,
        }
        # Deliberately make exit_price look like an adverse candle close. A
        # stop event's explicit stop_price must be used for settlement.
        result = self.window._area_shape_replay_result(plan, trade, {
            "kind": "paper_stop", "exit_price": "125", "stop_price": "110",
            "candle_ts": 2 * HOUR,
        })
        expected_fee_r = (Decimal("100") + Decimal("110")) * Decimal("0.00036") / Decimal("10")
        self.assertEqual(result["exit"], Decimal("110"))
        self.assertEqual(result["r"], Decimal("-1") - expected_fee_r)
        self.assertEqual(result["pnl"], result["r"] * plan.config.risk_amount)

    def test_first_fractal_touch_is_reference_and_not_a_replay_opportunity(self) -> None:
        plan = self._create_demo_draft()
        plan.support_price = Decimal("90")
        plan.pressure_price = Decimal("105")
        plan.config = AreaShapeConfig(
            direction="long", min_space_r=Decimal("1"), entry_trigger_mode="pattern",
            signal_patterns=("bottom_fractal",), stop_basis="three_candle", stop_offset_atr=Decimal("0"),
            entry_mode="breakout", break_even_enabled=False,
        )
        candles = [
            Candle(index * HOUR, Decimal("100"), Decimal("101"), Decimal("99"), Decimal("100"), Decimal("1"), True)
            for index in range(1, 11)
        ]
        candles.extend([
            Candle(11 * HOUR, Decimal("100"), Decimal("101"), Decimal("98"), Decimal("99.5"), Decimal("1"), True),
            Candle(12 * HOUR, Decimal("99.5"), Decimal("100"), Decimal("94"), Decimal("95"), Decimal("1"), True),
            Candle(13 * HOUR, Decimal("95"), Decimal("101"), Decimal("90"), Decimal("100"), Decimal("1"), True),
            Candle(14 * HOUR, Decimal("100"), Decimal("102"), Decimal("99"), Decimal("101"), Decimal("1"), True),
        ])

        rows = self.window._simulate_area_shape_replay(plan, candles, start_ms=0, sides=("long",))

        self.assertEqual(rows, [])

    def test_bottom_fractal_is_rejected_when_confirmation_candle_closes_back_below_retrace(self) -> None:
        plan = self._create_demo_draft()
        plan.support_price = Decimal("81146.5")
        plan.pressure_price = Decimal("83953.9")
        plan.config = AreaShapeConfig(
            direction="long", min_space_r=Decimal("2"), entry_trigger_mode="pattern",
            signal_patterns=("double_reversal_up", "bottom_fractal"),
            stop_basis="fixed_distance", fixed_stop_distance=Decimal("1000"),
            break_even_enabled=False,
        )
        candles = [
            Candle(index * HOUR, Decimal("83000"), Decimal("83100"), Decimal("82900"), Decimal("83000"), Decimal("1"), True)
            for index in range(1, 11)
        ]
        candles.extend([
            Candle(11 * HOUR, Decimal("82279.5"), Decimal("82610.9"), Decimal("81711"), Decimal("82141.9"), Decimal("1"), True),
            Candle(12 * HOUR, Decimal("82141.9"), Decimal("82748.1"), Decimal("82090.9"), Decimal("82659.9"), Decimal("1"), True),
            Candle(13 * HOUR, Decimal("82659.9"), Decimal("82659.9"), Decimal("80910.4"), Decimal("80991.9"), Decimal("1"), True),
        ])

        rows = self.window._simulate_area_shape_replay(plan, candles, start_ms=0, sides=("long",))

        self.assertEqual(rows, [])

    def test_replay_records_same_side_signal_during_open_position_as_opportunity(self) -> None:
        plan = self._create_demo_draft()
        plan.support_price = Decimal("95")
        plan.pressure_price = Decimal("125")
        plan.config = AreaShapeConfig(
            direction="long", risk_amount=Decimal("10"), min_space_r=Decimal("1"),
            entry_trigger_mode="direct_touch", stop_basis="fixed_distance",
            fixed_stop_distance=Decimal("10"), break_even_enabled=False,
        )
        candles = [
            Candle(HOUR, Decimal("100"), Decimal("101"), Decimal("99"), Decimal("100"), Decimal("1"), True),
            Candle(2 * HOUR, Decimal("100"), Decimal("102"), Decimal("94"), Decimal("100"), Decimal("1"), True),
            Candle(3 * HOUR, Decimal("100"), Decimal("102"), Decimal("99"), Decimal("100"), Decimal("1"), True),
            Candle(4 * HOUR, Decimal("100"), Decimal("102"), Decimal("94"), Decimal("100"), Decimal("1"), True),
            Candle(5 * HOUR, Decimal("100"), Decimal("102"), Decimal("99"), Decimal("100"), Decimal("1"), True),
            Candle(6 * HOUR, Decimal("100"), Decimal("102"), Decimal("94"), Decimal("100"), Decimal("1"), True),
        ]

        rows = self.window._simulate_area_shape_replay(plan, candles, start_ms=HOUR, sides=("long",))

        self.assertTrue(any(item.get("record_type") == "trade" for item in rows), rows)
        opportunities = [item for item in rows if item.get("record_type") == "opportunity"]
        self.assertEqual(len(opportunities), 1)
        self.assertEqual(opportunities[0]["side"], "long")
        self.assertEqual(opportunities[0]["signal_ts"], 6 * HOUR)
        self.assertIn("未重复开仓", opportunities[0]["outcome"])

    def test_replay_does_not_count_opposite_signal_after_position_closes(self) -> None:
        plan = self._create_demo_draft()
        plan.support_price = Decimal("95")
        plan.pressure_price = Decimal("125")
        plan.config = AreaShapeConfig(
            direction="long", risk_amount=Decimal("10"), min_space_r=Decimal("1"),
            entry_trigger_mode="direct_touch", stop_basis="fixed_distance",
            fixed_stop_distance=Decimal("1"), break_even_enabled=False,
            exit_template="lock_2r_at_3r",
        )
        candles = [
            Candle(HOUR, Decimal("100"), Decimal("101"), Decimal("99"), Decimal("100"), Decimal("1"), True),
            Candle(2 * HOUR, Decimal("100"), Decimal("102"), Decimal("94"), Decimal("100"), Decimal("1"), True),
            Candle(3 * HOUR, Decimal("100"), Decimal("102"), Decimal("99"), Decimal("100"), Decimal("1"), True),
            Candle(4 * HOUR, Decimal("100"), Decimal("102"), Decimal("94"), Decimal("100"), Decimal("1"), True),
            Candle(5 * HOUR, Decimal("100"), Decimal("101"), Decimal("99"), Decimal("100"), Decimal("1"), True),
            Candle(6 * HOUR, Decimal("100"), Decimal("126"), Decimal("99"), Decimal("120"), Decimal("1"), True),
            Candle(7 * HOUR, Decimal("124"), Decimal("126"), Decimal("123"), Decimal("124"), Decimal("1"), True),
            Candle(8 * HOUR, Decimal("124"), Decimal("126"), Decimal("123"), Decimal("124"), Decimal("1"), True),
        ]

        rows = self.window._simulate_area_shape_replay(plan, candles, start_ms=HOUR, sides=("long",))

        self.assertTrue(any(item.get("record_type") == "trade" for item in rows), rows)
        opportunities = [item for item in rows if item.get("record_type") == "opportunity"]
        self.assertEqual(opportunities, [])

    def test_replay_maps_selected_shape_families_without_changing_plan(self) -> None:
        plan = self._create_demo_draft()
        selected = (
            "big_bullish", "long_lower_shadow", "double_reversal_up", "bottom_fractal", "inside_bar",
        )
        plan.config = replace(plan.config, signal_patterns=selected)

        mapped_short = self.window._area_shape_replay_patterns(selected, "short")
        mapped_long = self.window._area_shape_replay_patterns(selected, "long")

        self.assertEqual(
            mapped_short,
            ("big_bearish", "long_upper_shadow", "double_reversal_down", "top_fractal", "inside_bar"),
        )
        self.assertEqual(mapped_long, selected)
        self.assertEqual(plan.config.signal_patterns, selected)

    def test_replay_no_trade_result_is_empty_and_short_and_both_are_supported(self) -> None:
        plan = self._create_demo_draft()
        plan.support_price = Decimal("80")
        plan.pressure_price = Decimal("120")
        plan.config = AreaShapeConfig(
            direction="short", atr_period=2, min_space_r=Decimal("1"), stop_offset_atr=Decimal("0"),
            entry_trigger_mode="direct_touch", signal_patterns=("inside_bar",),
        )
        candles = [
            Candle(n * HOUR, Decimal("100"), Decimal("105"), Decimal("95"), Decimal("101"), Decimal("1"), True)
            for n in range(1, 6)
        ]

        short_rows = self.window._simulate_area_shape_replay(plan, candles, start_ms=HOUR, sides=("short",))
        both_rows = self.window._simulate_area_shape_replay(plan, candles, start_ms=HOUR, sides=("long", "short"))

        self.assertEqual(short_rows, [])
        self.assertEqual(both_rows, [])
        self.assertEqual(plan.config.direction, "short")

    def test_replay_failure_is_reported_in_area_status(self) -> None:
        self.window._area_shape_replay_failed("synthetic replay error")
        self.assertIn("区域复盘失败", self.window._area_shape_status.text())
        self.assertIn("synthetic replay error", self.window._area_shape_status.text())

    def test_replay_runs_off_ui_thread_and_restores_button_after_failure(self) -> None:
        self._create_demo_draft()
        accepted = self._kline_module.QDialog.DialogCode.Accepted
        with patch.object(self._kline_module.QDialog, "exec", return_value=accepted):
            with patch.object(self.window, "_simulate_area_shape_replay", side_effect=RuntimeError("worker failure")):
                self.window._run_area_shape_replay()
                self.assertFalse(self.window._area_shape_replay_button.isEnabled())
                deadline = time.monotonic() + 5
                while self.window._area_shape_replay_thread is not None and time.monotonic() < deadline:
                    self._app.processEvents()
                    time.sleep(0.01)
                self._app.processEvents()

        self.assertIsNone(self.window._area_shape_replay_thread)
        self.assertTrue(self.window._area_shape_replay_button.isEnabled())
        self.assertIn("worker failure", self.window._area_shape_status.text())

    def test_replay_completion_shows_current_ray_snapshot_and_runs_both_sides(self) -> None:
        plan = self._create_demo_draft()
        original_support = plan.support_price
        original_pressure = plan.pressure_price
        records = self.window._workspace_entry(symbol=plan.symbol, period=plan.period)["lines"]
        next(item for item in records if item["id"] == plan.support_line_id)["price_a"] = 97.5
        next(item for item in records if item["id"] == plan.pressure_line_id)["price_a"] = 125.5
        with patch.object(self.window, "_simulate_area_shape_replay", return_value=[]) as simulate:
            with patch.object(self.window, "_show_area_shape_replay_results") as show_results:
                self.window._run_area_shape_replay()
                deadline = time.monotonic() + 5
                while self.window._area_shape_replay_thread is not None and time.monotonic() < deadline:
                    self._app.processEvents()
                    time.sleep(0.01)
                self._app.processEvents()

        self.assertEqual(simulate.call_args.args[0].support_price, Decimal("97.5"))
        self.assertEqual(simulate.call_args.args[0].pressure_price, Decimal("125.5"))
        self.assertEqual(simulate.call_args.kwargs["sides"], ("long", "short"))
        self.assertEqual(show_results.call_args.args[0].support_price, Decimal("97.5"))
        self.assertEqual(show_results.call_args.kwargs["sides"], ("long", "short"))
        self.assertEqual(plan.support_price, original_support)
        self.assertEqual(plan.pressure_price, original_pressure)
        saved = self.window._area_shape_replay_record(plan.plan_id, symbol=plan.symbol, period=plan.period)
        self.assertIsNotNone(saved)
        self.assertEqual(saved["input_signature"], show_results.call_args.kwargs["replay_record"]["input_signature"])

    def test_replay_reopens_saved_results_without_running_simulation_again(self) -> None:
        plan = self._create_demo_draft()
        lines = self.window._workspace_entry(symbol=plan.symbol, period=plan.period)["lines"]
        support = next(item for item in lines if item["id"] == plan.support_line_id)
        pressure = next(item for item in lines if item["id"] == plan.pressure_line_id)
        snapshot = replace(
            plan,
            support_price=Decimal(str(support["price_a"])),
            pressure_price=Decimal(str(pressure["price_a"])),
        )
        start_seconds = int(support["time_a"])
        signature = self.window._area_shape_replay_signature(snapshot, start_seconds)
        rows = [{
            "record_type": "opportunity", "side": "short", "signal_ts": 2 * HOUR,
            "entry_ts": 2 * HOUR, "entry": "105", "stop": "110", "target": "95",
            "space_r": "2", "outcome": "持仓期间出现，未重复开仓",
        }]
        self.window._workspace_entry(symbol=plan.symbol, period=plan.period)["area_replays"] = {
            plan.plan_id: {"input_signature": signature, "rows": rows, "filters": {"kind": "all", "side": "all"}},
        }
        class _NoCandleScan:
            def __iter__(self):
                raise AssertionError("a cache hit must not rescan the candle history")

        self.window._pending_payload = replace(self.window._pending_payload, raw_candles=_NoCandleScan())

        with patch.object(self.window, "_simulate_area_shape_replay") as simulate:
            with patch.object(self.window, "_show_area_shape_replay_results") as show_results:
                self.window._run_area_shape_replay()

        simulate.assert_not_called()
        self.assertEqual(show_results.call_args.args[2], rows)
        self.assertEqual(self.window._area_shape_replay_overlay["rows"], rows)

    def test_replay_reopens_matching_memory_overlay_when_workspace_cache_is_missing(self) -> None:
        plan = self._create_demo_draft()
        lines = self.window._workspace_entry(symbol=plan.symbol, period=plan.period)["lines"]
        support = next(item for item in lines if item["id"] == plan.support_line_id)
        pressure = next(item for item in lines if item["id"] == plan.pressure_line_id)
        snapshot = replace(
            plan,
            support_price=Decimal(str(support["price_a"])),
            pressure_price=Decimal(str(pressure["price_a"])),
        )
        start_seconds = int(support["time_a"])
        signature = self.window._area_shape_replay_signature(snapshot, start_seconds)
        rows = [{
            "record_type": "opportunity", "side": "long", "signal_ts": 2 * HOUR,
            "entry_ts": 2 * HOUR, "entry": "100", "stop": "95", "target": "110",
            "space_r": "2", "outcome": "持仓期间出现",
        }]
        entry = self.window._workspace_entry(symbol=plan.symbol, period=plan.period)
        entry["area_replays"] = {}
        self.window._area_shape_replay_overlay = {
            "symbol": plan.symbol, "period": plan.period,
            "input_signature": signature, "rows": rows, "filters": {"kind": "all", "side": "all"},
        }

        class _NoCandleScan:
            def __iter__(self):
                raise AssertionError("a memory cache hit must not rescan the candle history")

        self.window._pending_payload = replace(self.window._pending_payload, raw_candles=_NoCandleScan())
        with patch.object(self.window, "_simulate_area_shape_replay") as simulate:
            with patch.object(self.window, "_show_area_shape_replay_results") as show_results:
                self.window._run_area_shape_replay()

        simulate.assert_not_called()
        self.assertEqual(show_results.call_args.args[2], rows)

    def test_replay_recomputes_results_saved_under_old_pattern_rules(self) -> None:
        plan = self._create_demo_draft()
        lines = self.window._workspace_entry(symbol=plan.symbol, period=plan.period)["lines"]
        support = next(item for item in lines if item["id"] == plan.support_line_id)
        pressure = next(item for item in lines if item["id"] == plan.pressure_line_id)
        snapshot = replace(
            plan, support_price=Decimal(str(support["price_a"])),
            pressure_price=Decimal(str(pressure["price_a"])),
        )
        signature_data = self._kline_module.json.loads(
            self.window._area_shape_replay_signature(snapshot, int(support["time_a"])),
        )
        self.assertGreater(signature_data["version"], 1)
        signature_data["version"] = 1
        old_signature = self._kline_module.json.dumps(
            signature_data, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        )
        cache = {"input_signature": old_signature, "rows": [], "filters": {}}
        self.window._workspace_entry(symbol=plan.symbol, period=plan.period)["area_replays"] = {plan.plan_id: cache}
        self.window._area_shape_replay_overlay = {"symbol": plan.symbol, "period": plan.period, **cache}

        with patch.object(self.window, "_simulate_area_shape_replay", return_value=[]) as simulate:
            with patch.object(self.window, "_show_area_shape_replay_results"):
                self.window._run_area_shape_replay()
                deadline = time.monotonic() + 5
                while self.window._area_shape_replay_thread is not None and time.monotonic() < deadline:
                    self._app.processEvents()
                    time.sleep(0.01)
                self._app.processEvents()

        simulate.assert_called_once()

    def test_replay_filters_table_and_chart_and_saves_filter_selection(self) -> None:
        plan = self._create_demo_draft()
        rows = [
            {"record_type": "trade", "side": "long", "entry_ts": HOUR, "exit_ts": 2 * HOUR,
             "entry": "100", "stop": "95", "exit": "110", "r": "2", "pnl": "20"},
            {"record_type": "opportunity", "side": "short", "signal_ts": 3 * HOUR, "entry_ts": 3 * HOUR,
             "entry": "105", "stop": "110", "target": "95", "space_r": "2", "outcome": "持仓期间出现"},
        ]
        cache = {"filters": {"kind": "all", "side": "all"}, "rows": rows}
        self.window._workspace_entry(symbol=plan.symbol, period=plan.period).setdefault("area_replays", {})[plan.plan_id] = cache
        self.window._area_shape_replay_overlay = {
            "symbol": plan.symbol, "period": plan.period, "input_signature": "test",
            "rows": rows, "filters": {"kind": "all", "side": "all"},
        }
        class _ChartStub:
            def set_area_shape_replay_trades(self, visible) -> None:
                self.trades = list(visible or [])

        chart = _ChartStub()
        self.window._native_chart_view = chart
        accepted = self._kline_module.QDialog.DialogCode.Accepted

        with patch.object(self._kline_module.QDialog, "exec", return_value=accepted):
            self.window._show_area_shape_replay_results(plan, HOUR // 1000, rows, replay_record=cache)

        dialog = next(item for item in self.window.findChildren(self._kline_module.QDialog) if item.windowTitle() == "区域计划复盘结果")
        table = dialog.findChild(self._kline_module.QTableWidget)
        filters = dialog.findChildren(self._kline_module.QComboBox)
        kind_filter = next(item for item in filters if item.count() == 3)
        self.assertEqual(table.item(0, 1).foreground().color().name(), "#15803d")
        self.assertEqual(table.item(1, 1).foreground().color().name(), "#6d28d9")
        kind_filter.setCurrentIndex(kind_filter.findData("opportunity"))
        self._app.processEvents()

        self.assertEqual(table.rowCount(), 1)
        self.assertEqual(table.item(0, 0).text(), "潜在机会")
        self.assertEqual(chart.trades, [rows[1]])
        self.assertEqual(
            self.window._area_shape_replay_record(plan.plan_id, symbol=plan.symbol, period=plan.period)["filters"]["kind"],
            "opportunity",
        )

    def test_replay_result_dialog_rounds_values_and_fits_all_columns(self) -> None:
        plan = self._create_demo_draft()
        plan.support_price = Decimal("80363.8718251256")
        plan.pressure_price = Decimal("84007.00907776799")
        rows = [{
            "side": "short", "pattern": "long_upper_shadow", "entry": Decimal("83771.4"),
            "stop": Decimal("84414.77835830216821777195846"),
            "exit": Decimal("82424.81264684280015639977916"),
            "r": Decimal("1.999999999999999999999999999"),
            "pnl": Decimal("19.99999999999999999999999999"),
            "entry_ts": 1791741600000, "exit_ts": 1791853200000, "outcome": "paper_stop",
        }, {
            "side": "short", "pattern": "long_upper_shadow", "entry": Decimal("83820.1"),
            "stop": Decimal("84543.49988036458097672462331"),
            "exit": Decimal("81590.35259597170725521425294"),
            "r": Decimal("2.999999999999999999999999999"),
            "pnl": Decimal("29.99999999999999999999999999"),
            "entry_ts": 1791878400000, "exit_ts": 1792072800000, "outcome": "paper_stop",
        }]
        instrument = SimpleNamespace(tick_size=Decimal("0.1"))
        accepted = self._kline_module.QDialog.DialogCode.Accepted
        with patch.object(self.window, "_instrument_for_symbol", return_value=instrument):
            with patch.object(self._kline_module.QDialog, "exec", return_value=accepted):
                self.window._show_area_shape_replay_results(plan, 1791428400, rows, sides=("short",))

        dialog = next(item for item in self.window.findChildren(self._kline_module.QDialog) if item.windowTitle() == "区域计划复盘结果")
        table = dialog.findChild(self._kline_module.QTableWidget)
        summary = dialog.findChild(self._kline_module.QLabel)
        self.assertIsNotNone(table)
        self.assertEqual(table.item(0, 3).text(), "—")
        self.assertEqual(table.item(0, 6).text(), "83771.4")
        self.assertEqual(table.item(0, 7).text(), "84414.8")
        self.assertEqual(table.item(0, 8).text(), "82424.8")
        self.assertEqual(table.item(0, 9).text(), "2.00R")
        self.assertEqual(table.item(0, 10).text(), "20.00")
        self.assertEqual(table.item(0, 1).foreground().color().name(), "#b91c1c")
        self.assertEqual(table.item(0, 1).background().color().name(), "#fff1f0")
        self.assertIn("80363.9 / 压力 84007.0", summary.text())
        self.assertIn("50.00 U", summary.text())
        self.assertIn("5.00R", summary.text())
        self.assertNotIn("999999999", summary.text() + " ".join(
            table.item(row, column).text()
            for row in range(table.rowCount()) for column in range(table.columnCount())
        ))
        self.assertEqual(table.horizontalHeader().sectionResizeMode(0), self._kline_module.QHeaderView.ResizeMode.Interactive)
        self.assertLessEqual(sum(table.columnWidth(index) for index in range(table.columnCount())), 1220)

    def test_direct_fixed_plan_details_match_actual_entry_and_stop_modes(self) -> None:
        plan = self._create_demo_draft()
        plan.config = AreaShapeConfig(
            entry_trigger_mode="direct_touch", stop_basis="fixed_distance",
            fixed_stop_distance=Decimal("1000"), resting_entry_enabled=False,
            break_even_enabled=False,
        )
        details = self.window._area_shape_plan_details_text(plan)
        self.assertIn("预挂入场线GTC", details)
        self.assertIn("固定价差 1000 U", details)
        self.assertIn("当前组合不需ATR", details)
        self.assertIn("保本关闭", details)
        self.assertNotIn("价格受限FOK", details)
        self.assertNotIn("ATR周期", details)
        self.assertNotIn("入场偏移", details)

    def test_replay_requires_the_plan_timeframe(self) -> None:
        self._create_demo_draft()
        self.window._period_combo.setCurrentText("4H")
        self.window._run_area_shape_replay()
        self.assertIn("按 1H K 线复盘", self.window._area_shape_status.text())
        self.assertIn("请切换到 1H", self.window._area_shape_status.text())

    def test_paper_plan_actions_follow_enable_pause_cancel_flow(self) -> None:
        plan = self._create_demo_draft()
        workspace_lines = self.window._workspace_entry(symbol="BTC-USDT-SWAP", period="1H")["lines"]
        support = next(item for item in workspace_lines if item["id"] == plan.support_line_id)
        pressure = next(item for item in workspace_lines if item["id"] == plan.pressure_line_id)
        self.assertEqual(support["kind"], "ray")
        self.assertEqual(pressure["kind"], "ray")
        self.assertEqual(support["time_a"], pressure["time_a"])
        self.assertFalse(self.window._area_shape_create_button.isEnabled())
        line_count = len(workspace_lines)
        self.window._create_area_shape_plan()
        self.assertEqual(len(self.window._workspace_entry(symbol="BTC-USDT-SWAP", period="1H")["lines"]), line_count)
        details = self.window._area_shape_plan_details.text()
        self.assertIn("当前计划参数", details)
        self.assertIn("单笔风险预算", details)
        self.assertIn("最低目标空间", details)
        self.assertIn("1H触发形态", details)
        self.assertEqual(self.window._area_shape_start_button.text(), "启用计划")
        self.assertTrue(self.window._area_shape_start_button.isEnabled())
        self.assertFalse(self.window._area_shape_pause_button.isEnabled())

        self.window._area_shape_mode_combo.setCurrentIndex(self.window._area_shape_mode_combo.findData("paper"))
        self.window._start_area_shape_monitoring()
        self.assertEqual(self.paper_monitor.get_plan(plan.plan_id).status, "monitoring")
        self.assertEqual(self.window._area_shape_start_button.text(), "监控中")
        self.assertFalse(self.window._area_shape_start_button.isEnabled())
        self.assertTrue(self.window._area_shape_pause_button.isEnabled())

        self.window._pause_area_shape_monitoring()
        self.assertEqual(self.paper_monitor.get_plan(plan.plan_id).status, "paused")
        self.assertEqual(self.window._area_shape_start_button.text(), "恢复计划")
        self.assertTrue(self.window._area_shape_start_button.isEnabled())

        self.window._cancel_area_shape_plan()
        self.assertEqual(self.paper_monitor.get_plan(plan.plan_id).status, "cancelled")
        self.assertFalse(self.window._area_shape_start_button.isEnabled())

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

    def test_armed_session_without_position_explains_protection_is_created_after_fill(self) -> None:
        plan = self._create_demo_draft()
        self.demo_monitor._sessions.append({
            "session_id": "session-armed", "plan_id": plan.plan_id, "profile_name": "ui-test",
            "environment": "demo", "symbol": "BTC-USDT-SWAP", "status": "armed",
            "account_uid": "uid-demo", "events": [], "position": None,
            "pending_intent": None, "resting_entry_intent": None,
        })

        self.window._refresh_area_shape_execution_status()

        status = self.window._area_shape_demo_status.text()
        self.assertIn("等待新信号", status)
        self.assertIn("SL -（等待入场成交后生成）", status)
        self.assertIn("TP -（等待入场成交后生成）", status)

    def _assert_area_controls_fit_viewport(self) -> None:
        self.window.resize(1440, 920)
        self.window.show()
        self.window._toggle_left_panel_btn.setChecked(False)
        self.window._control_tabs.setCurrentIndex(self.window._control_area_tab_index)
        self.window._rr_workspace_mode_combo.setCurrentIndex(self.window._rr_workspace_mode_combo.findData("area"))
        self.assertEqual(self.window._control_tabs.currentIndex(), self.window._control_area_tab_index)
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
            self.window._area_shape_replay_button,
            self.window._area_shape_projection_button,
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
        self.window._rr_workspace_mode_combo.setCurrentIndex(self.window._rr_workspace_mode_combo.findData("area"))
        self._app.processEvents()

    def test_rr_and_smart_trade_tabs_only_change_visible_workspace(self) -> None:
        self._create_demo_draft()
        self.assertEqual(self.paper_monitor.plans()[0].status, "draft")
        self.assertEqual(self.demo_monitor.arm_calls, [])

        with (
            patch.object(self.window, "_start_area_shape_monitoring") as start_area,
            patch.object(self.window, "_enable_selected_rr_trade") as enable_rr,
            patch.object(self.window, "_cancel_selected_rr_trade") as cancel_rr,
        ):
            self.window._rr_workspace_mode_combo.setCurrentIndex(
                self.window._rr_workspace_mode_combo.findData("direct")
            )
            self.assertEqual(self.window._control_tabs.currentIndex(), self.window._control_rr_tab_index)
            self.window._rr_workspace_mode_combo.setCurrentIndex(
                self.window._rr_workspace_mode_combo.findData("area")
            )
            self.assertEqual(self.window._control_tabs.currentIndex(), self.window._control_area_tab_index)
            self.window._control_tabs.setCurrentIndex(self.window._control_rr_tab_index)
            self.assertEqual(self.window._rr_workspace_mode_combo.currentData(), "direct")
            self.window._control_tabs.setCurrentIndex(self.window._control_area_tab_index)
            self.assertEqual(self.window._rr_workspace_mode_combo.currentData(), "area")
            start_area.assert_not_called()
            enable_rr.assert_not_called()
            cancel_rr.assert_not_called()

        self.assertEqual(self.paper_monitor.plans()[0].status, "draft")
        self.assertEqual(self.demo_monitor.arm_calls, [])

    def _wait_for_demo_action(self) -> None:
        deadline = time.monotonic() + 3
        while self.window._area_shape_demo_action_pending and time.monotonic() < deadline:
            self._app.processEvents()
            time.sleep(0.01)
        self._app.processEvents()
        self.assertFalse(self.window._area_shape_demo_action_pending)
