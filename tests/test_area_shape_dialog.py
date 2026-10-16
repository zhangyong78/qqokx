from __future__ import annotations

from decimal import Decimal

from PySide6.QtCore import Qt
from tests.qt_test_case import QtWidgetTestCase
from okx_quant.area_shape_trade import AreaShapeConfig
from roll_terminal_qt.area_shape_dialog import AreaShapeConfigDialog


class AreaShapeDialogTests(QtWidgetTestCase):
    def test_resting_gtc_entry_defaults_on_and_can_be_disabled(self) -> None:
        dialog = AreaShapeConfigDialog(AreaShapeConfig())
        try:
            self.assertTrue(dialog.resting_entry_enabled.isChecked())
            dialog.resting_entry_enabled.setChecked(False)
            self.assertFalse(dialog.config().resting_entry_enabled)
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

