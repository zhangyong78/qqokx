from __future__ import annotations

from decimal import Decimal
from itertools import product

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QDialogButtonBox
from tests.qt_test_case import QtWidgetTestCase
from okx_quant.area_shape_trade import AreaShapeConfig
from roll_terminal_qt.area_shape_dialog import AreaShapeConfigDialog


class AreaShapeDialogTests(QtWidgetTestCase):
    def test_stop_options_follow_entry_mode_and_manual_price_requires_correct_side(self):
        dialog = AreaShapeConfigDialog(AreaShapeConfig(), support_price=Decimal(100), pressure_price=Decimal(120))
        try:
            self.assertEqual(dialog.stop_basis.findData("entry_line_atr"), -1)
            self.assertNotEqual(dialog.stop_basis.findData("pattern_candles"), -1)
            dialog.entry_trigger_mode.setCurrentIndex(dialog.entry_trigger_mode.findData("direct_touch"))
            self.assertEqual({dialog.stop_basis.itemData(i) for i in range(dialog.stop_basis.count())},
                             {"fixed_price", "fixed_distance"})
            self.assertTrue(dialog.stop_offset.isHidden())
            dialog.stop_basis.setCurrentIndex(dialog.stop_basis.findData("fixed_price"))
            save = dialog.buttons.button(QDialogButtonBox.StandardButton.Ok)
            dialog.fixed_stop_price.setValue(101)
            self.assertFalse(save.isEnabled())
            dialog.fixed_stop_price.setValue(90)
            self.assertTrue(save.isEnabled())
            self.assertFalse(dialog.config().requires_atr())
            dialog.direction.setCurrentIndex(dialog.direction.findData("short"))
            self.assertFalse(save.isEnabled())
            dialog.fixed_stop_price.setValue(130)
            self.assertTrue(save.isEnabled())
            self.assertEqual(dialog.config().fixed_stop_price, Decimal(130))
        finally:
            self.dispose_widget(dialog)

    def test_atr_defaults_are_period_10_and_stop_offset_0_5(self) -> None:
        dialog = AreaShapeConfigDialog(AreaShapeConfig())
        try:
            self.assertEqual(dialog.atr_period.value(), 10)
            self.assertEqual(dialog.stop_offset.value(), 0.5)
            legacy_config = AreaShapeConfig().to_dict()
            legacy_config.pop("atr_period")
            self.assertEqual(AreaShapeConfig.from_dict(legacy_config).atr_period, 10)
        finally:
            self.dispose_widget(dialog)

    def test_mouse_wheel_does_not_change_dialog_numeric_fields(self) -> None:
        dialog = AreaShapeConfigDialog(AreaShapeConfig())

        class IgnoredWheelEvent:
            ignored = False

            def ignore(self) -> None:
                self.ignored = True

        try:
            fields = (
                dialog.risk_amount,
                dialog.min_space_r,
                dialog.atr_period,
                dialog.fixed_tp_r,
                dialog.time_break_even_bars,
                dialog.lock_trigger_r,
                dialog.valid_bars,
            )
            for field in fields:
                with self.subTest(field=field.objectName()):
                    before = field.value()
                    event = IgnoredWheelEvent()
                    field.wheelEvent(event)
                    self.assertTrue(event.ignored)
                    self.assertEqual(field.value(), before)
        finally:
            self.dispose_widget(dialog)

    def test_direct_touch_disables_pattern_options_but_uses_automatic_gtc(self) -> None:
        dialog = AreaShapeConfigDialog(AreaShapeConfig())
        try:
            dialog.entry_trigger_mode.setCurrentIndex(dialog.entry_trigger_mode.findData("direct_touch"))
            self.assertFalse(dialog.patterns.isEnabled())
            self.assertTrue(dialog.patterns.isHidden())
            self.assertFalse(dialog.direct_touch_note.isHidden())
            self.assertEqual(dialog.trigger_content_label.text(), "价格触发")
            self.assertIn("启用交易时立即向 OKX 提交", dialog.direct_touch_note.text())
            self.assertTrue(dialog.entry_mode_note.isHidden())
            self.assertEqual(dialog.config().entry_trigger_mode, "direct_touch")
            self.assertFalse(dialog.config().resting_entry_enabled)
            dialog.entry_trigger_mode.setCurrentIndex(dialog.entry_trigger_mode.findData("pattern"))
            self.assertFalse(dialog.patterns.isHidden())
            self.assertTrue(dialog.direct_touch_note.isHidden())
            self.assertFalse(dialog.entry_mode_note.isHidden())
            self.assertEqual(dialog.trigger_content_label.text(), "1H形态（可多选）")
            dialog.stop_basis.setCurrentIndex(dialog.stop_basis.findData("pattern_candles"))
            self.assertEqual(dialog.config().stop_basis, "pattern_candles")
            self.assertFalse(dialog.stop_offset.isHidden())
            dialog.stop_basis.setCurrentIndex(dialog.stop_basis.findData("fixed_distance"))
            self.assertTrue(dialog.stop_offset.isHidden())
            dialog.fixed_stop_distance.setValue(1000)
            fixed = dialog.config()
            self.assertEqual(fixed.stop_basis, "fixed_distance")
            self.assertEqual(fixed.fixed_stop_distance, Decimal("1000.0"))
            self.assertTrue(fixed.fee_offset_enabled)
            dialog.fee_offset_enabled.setChecked(False)
            self.assertFalse(dialog.config().fee_offset_enabled)
            self.assertEqual(dialog.config().round_trip_fee_rate, Decimal("0"))
        finally:
            self.dispose_widget(dialog)

    def test_pattern_entry_waits_for_touch_and_shape_without_parallel_gtc_option(self) -> None:
        dialog = AreaShapeConfigDialog(AreaShapeConfig())
        try:
            self.assertFalse(dialog.patterns.isHidden())
            self.assertFalse(dialog.config().resting_entry_enabled)
            self.assertIn("立即FOK吃单", dialog.entry_mode_note.text())
            self.assertEqual(dialog.config().entry_mode, "next_open")
            self.assertEqual(dialog.min_space_r.minimum(), 2)
        finally:
            self.dispose_widget(dialog)

    def test_basic_view_is_compact_and_advanced_r_values_roundtrip(self) -> None:
        config = AreaShapeConfig(
            direction="short", signal_patterns=("double_reversal_down", "long_upper_shadow"),
            fixed_tp_r=Decimal("1.5"), be_trigger_r=Decimal("2.5"),
            lock_trigger_r=Decimal("3.5"), lock_profit_r=Decimal("2.5"),
        )
        dialog = AreaShapeConfigDialog(config)
        try:
            self.assertTrue(dialog.advanced_panel.isHidden())
            self.assertFalse(dialog.fee_offset_enabled.isHidden())
            self.assertEqual(set(dialog._selected_patterns()), {"double_reversal_down", "long_upper_shadow"})
            dialog.advanced_toggle.click()
            self.assertFalse(dialog.advanced_panel.isHidden())
            self.assertEqual(dialog.fixed_tp_r.value(), 1.5)
            self.assertEqual(dialog.be_trigger_r.value(), 2.5)
            result = dialog.config()
            self.assertEqual(set(result.signal_patterns), {"double_reversal_down", "long_upper_shadow"})
            self.assertEqual(result.lock_trigger_r, Decimal("3.5"))
            self.assertEqual(result.lock_profit_r, Decimal("2.5"))
        finally:
            self.dispose_widget(dialog)

    def test_fixed_and_dynamic_exit_share_editable_break_even_settings(self) -> None:
        dialog = AreaShapeConfigDialog(AreaShapeConfig(exit_template="lock_2r_at_3r"))
        try:
            self.assertFalse(dialog.be_trigger_r.isHidden())
            self.assertFalse(dialog.lock_trigger_r.isHidden())
            self.assertFalse(dialog.lock_profit_r.isHidden())
            self.assertTrue(dialog.fixed_tp_r.isHidden())
            self.assertEqual(
                (dialog.be_trigger_r.value(), dialog.lock_trigger_r.value(), dialog.lock_profit_r.value()),
                (2.0, 3.0, 2.0),
            )
            self.assertEqual((dialog.lock_trail_every_r.value(), dialog.lock_trail_add_r.value()), (1.0, 1.0))

            dialog.be_trigger_r.setValue(1.0)
            dialog.lock_trigger_r.setValue(4.0)
            dialog.lock_profit_r.setValue(2.0)
            dialog.lock_trail_every_r.setValue(1.0)
            dialog.lock_trail_add_r.setValue(1.0)
            result = dialog.config()
            self.assertEqual(result.be_trigger_r, Decimal("1.0"))
            self.assertEqual(result.lock_trigger_r, Decimal("4.0"))
            self.assertEqual(result.lock_profit_r, Decimal("2.0"))
            self.assertEqual(result.lock_trail_every_r, Decimal("1.0"))
            self.assertEqual(result.lock_trail_add_r, Decimal("1.0"))

            dialog.exit_template.setCurrentIndex(dialog.exit_template.findData("fixed_1r"))
            self.assertFalse(dialog.be_trigger_r.isHidden())
            self.assertFalse(dialog.fixed_tp_r.isHidden())
            self.assertTrue(dialog.lock_trigger_r.isHidden())
            self.assertTrue(dialog.lock_profit_r.isHidden())
            self.assertTrue(dialog.break_even_enabled.isChecked())
            dialog.break_even_enabled.setChecked(False)
            self.assertTrue(dialog.be_trigger_r.isHidden())
        finally:
            self.dispose_widget(dialog)

    def test_time_break_even_settings_roundtrip(self) -> None:
        dialog = AreaShapeConfigDialog(AreaShapeConfig())
        try:
            dialog.time_break_even_enabled.setChecked(True)
            dialog.time_break_even_bars.setValue(12)
            result = dialog.config()
            self.assertTrue(result.time_break_even_enabled)
            self.assertEqual(result.time_break_even_bars, 12)
        finally:
            self.dispose_widget(dialog)

    def test_switching_direction_offers_supported_bearish_patterns(self) -> None:
        dialog = AreaShapeConfigDialog(AreaShapeConfig(direction="long"))
        try:
            dialog.direction.setCurrentIndex(1)
            offered = {
                dialog.patterns.item(index).data(Qt.ItemDataRole.UserRole)
                for index in range(dialog.patterns.count())
            }
            self.assertEqual(offered, {"big_bearish", "long_upper_shadow", "double_reversal_down", "inside_bar", "top_fractal"})
        finally:
            self.dispose_widget(dialog)

    def test_direction_explains_entry_line_and_opposite_line_r_check(self) -> None:
        dialog = AreaShapeConfigDialog(
            AreaShapeConfig(direction="long"),
            support_price=Decimal("100"), pressure_price=Decimal("120"),
        )
        try:
            self.assertIn("下方支撑 100 是入场参考", dialog.direction_note.text())
            self.assertIn("上方压力 120 用于核算目标空间和盈亏比", dialog.direction_note.text())
            dialog.direction.setCurrentIndex(1)
            self.assertIn("上方压力 120 是入场参考", dialog.direction_note.text())
            self.assertIn("下方支撑 100 用于核算目标空间和盈亏比", dialog.direction_note.text())
        finally:
            self.dispose_widget(dialog)

    def test_combination_matrix_preserves_values_and_only_shows_relevant_controls(self) -> None:
        dialog = AreaShapeConfigDialog(AreaShapeConfig())
        try:
            for direction, trigger, exit_mode, be, time_be, fee, stop, advanced in product(
                ("long", "short"), ("pattern", "direct_touch"),
                ("fixed_1r", "lock_2r_at_3r"), (False, True), (False, True), (False, True),
                ("signal_candle", "pattern_candles", "fixed_price", "fixed_distance"), (False, True),
            ):
                with self.subTest(direction=direction, trigger=trigger, exit=exit_mode,
                                  be=be, time_be=time_be, fee=fee, stop=stop, advanced=advanced):
                    direct = trigger == "direct_touch"
                    effective_stop = "fixed_distance" if direct and stop not in {"fixed_price", "fixed_distance"} else stop
                    for widget, value in ((dialog.direction, direction), (dialog.entry_trigger_mode, trigger),
                                          (dialog.exit_template, exit_mode),
                                          (dialog.stop_basis, effective_stop)):
                        widget.setCurrentIndex(widget.findData(value))
                    dialog.fixed_stop_price.setValue(90)
                    dialog.break_even_enabled.setChecked(be)
                    dialog.time_break_even_enabled.setChecked(time_be)
                    dialog.fee_offset_enabled.setChecked(fee)
                    dialog.advanced_toggle.setChecked(advanced)
                    dynamic = exit_mode != "fixed_1r"
                    self.assertEqual(dialog.patterns.isHidden(), direct)
                    self.assertEqual(dialog.direct_touch_note.isHidden(), not direct)
                    self.assertEqual(dialog.entry_mode_note.isHidden(), direct)
                    self.assertEqual(dialog.stop_basis.currentData(), effective_stop)
                    self.assertEqual(dialog.fixed_stop_distance.isHidden(), effective_stop != "fixed_distance")
                    self.assertEqual(dialog.stop_offset.isHidden(), effective_stop in {"fixed_distance", "fixed_price"})
                    self.assertEqual(dialog.fixed_stop_price.isHidden(), effective_stop != "fixed_price")
                    self.assertEqual(dialog.fixed_tp_r.isHidden(), dynamic)
                    self.assertEqual(dialog.be_trigger_r.isHidden(), not be)
                    self.assertEqual(dialog.time_break_even_enabled.isHidden(), not be)
                    self.assertEqual(dialog.time_break_even_bars.isHidden(), not (be and time_be))
                    self.assertEqual(dialog.advanced_panel.isHidden(), not advanced)
                    for widget in dialog._lock_parameter_widgets:
                        self.assertEqual(widget.isHidden(), not dynamic)
                    config = dialog.config()
                    self.assertEqual(dialog.atr_period.isHidden(), not config.requires_atr())
                    self.assertFalse(config.resting_entry_enabled)
                    self.assertEqual(config.entry_mode, "next_open")
                    self.assertEqual(config.time_break_even_enabled, be and time_be)
                    self.assertEqual(config.fee_offset_enabled, fee)
                    self.assertTrue(dialog.buttons.button(QDialogButtonBox.StandardButton.Ok).isEnabled())
        finally:
            self.dispose_widget(dialog)

    def test_invalid_combinations_stay_in_dialog_and_can_be_corrected(self) -> None:
        dialog = AreaShapeConfigDialog(AreaShapeConfig())
        try:
            save = dialog.buttons.button(QDialogButtonBox.StandardButton.Ok)
            for index in range(dialog.patterns.count()):
                dialog.patterns.item(index).setCheckState(Qt.CheckState.Unchecked)
            self.assertFalse(save.isEnabled())
            dialog.accept()
            self.assertNotEqual(dialog.result(), dialog.DialogCode.Accepted)
            self.assertIn("至少选择", dialog.validation_message.text())
            dialog.entry_trigger_mode.setCurrentIndex(dialog.entry_trigger_mode.findData("direct_touch"))
            self.assertTrue(save.isEnabled())
            dialog.exit_template.setCurrentIndex(dialog.exit_template.findData("lock_2r_at_3r"))
            dialog.lock_trigger_r.setValue(2)
            self.assertFalse(save.isEnabled())
            self.assertIn("大于保本", dialog.validation_message.text())
            dialog.lock_trigger_r.setValue(4)
            dialog.lock_profit_r.setValue(4)
            self.assertIn("首次锁盈", dialog.validation_message.text())
            dialog.lock_profit_r.setValue(2)
            dialog.lock_trail_add_r.setValue(2)
            self.assertIn("止损上移快于浮盈增长", dialog.validation_message.text())
            self.assertTrue(save.isEnabled())
            dialog.lock_trail_add_r.setValue(1)
            dialog.stop_offset.setValue(0)
            self.assertEqual(dialog.validation_message.text(), "")
            self.assertTrue(save.isEnabled())
            dialog.stop_basis.setCurrentIndex(dialog.stop_basis.findData("fixed_distance"))
            self.assertTrue(save.isEnabled())
            dialog.accept()
            self.assertEqual(dialog.result(), dialog.DialogCode.Accepted)
        finally:
            self.dispose_widget(dialog)

    def test_direction_switch_and_legacy_cost_settings_roundtrip(self) -> None:
        dialog = AreaShapeConfigDialog(AreaShapeConfig(
            slippage_rate=Decimal("0.001"), target_buffer_atr=Decimal("0.5"),
            entry_offset_atr=Decimal("0.25"),
        ))
        try:
            for index in range(dialog.patterns.count()):
                item = dialog.patterns.item(index)
                item.setCheckState(Qt.CheckState.Checked if item.data(Qt.ItemDataRole.UserRole) == "big_bullish"
                                   else Qt.CheckState.Unchecked)
            dialog.direction.setCurrentIndex(1)
            self.assertNotIn("big_bullish", dialog._selected_patterns())
            dialog.direction.setCurrentIndex(0)
            self.assertEqual(dialog._selected_patterns(), ("big_bullish",))
            self.assertFalse(dialog.target_buffer.isHidden())
            self.assertFalse(dialog.slippage.isHidden())
            self.assertEqual(dialog.slippage.value(), 0.1)
            result = dialog.config()
            self.assertEqual(result.slippage_rate, Decimal("0.001"))
            self.assertEqual(result.entry_offset_atr, Decimal("0"))
            self.assertEqual(result.target_buffer_atr, Decimal("0.5"))
            self.assertFalse(result.resting_entry_enabled)
        finally:
            self.dispose_widget(dialog)

    def test_atr_controls_follow_stop_and_target_buffer_dependencies(self) -> None:
        dialog = AreaShapeConfigDialog(AreaShapeConfig(
            entry_trigger_mode="direct_touch", stop_basis="fixed_distance",
        ))
        try:
            self.assertTrue(dialog.atr_period.isHidden())
            self.assertIn("无需等待ATR", dialog.atr_note.text())
            dialog.entry_trigger_mode.setCurrentIndex(dialog.entry_trigger_mode.findData("pattern"))
            self.assertTrue(dialog.atr_period.isHidden())
            dialog.entry_trigger_mode.setCurrentIndex(dialog.entry_trigger_mode.findData("direct_touch"))
            self.assertTrue(dialog.atr_period.isHidden())
            dialog.target_buffer.setValue(0.5)
            self.assertFalse(dialog.atr_period.isHidden())
            dialog.target_buffer.setValue(0)
            dialog.entry_trigger_mode.setCurrentIndex(dialog.entry_trigger_mode.findData("pattern"))
            dialog.stop_basis.setCurrentIndex(dialog.stop_basis.findData("pattern_candles"))
            self.assertFalse(dialog.atr_period.isHidden())
        finally:
            self.dispose_widget(dialog)

