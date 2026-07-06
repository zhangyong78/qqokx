from __future__ import annotations

import os
import unittest
from decimal import Decimal
from unittest.mock import patch

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCharts import QCandlestickSeries
from PySide6.QtCore import QPoint, QPointF, Qt
from PySide6.QtTest import QTest

from okx_quant.models import Candle
from tests.qt_test_case import QtWidgetTestCase
from roll_terminal_qt.line_trading_chart import LineTradingChartView
from roll_terminal_qt.line_trading_core import LineAnnotation, RiskRewardAnnotation


class LineTradingChartQtTests(QtWidgetTestCase):
    def test_chart_line_drag_emits_updated_annotation_on_release(self) -> None:
        chart = LineTradingChartView()
        try:
            updates = []
            chart.lineUpdated.connect(lambda index, annotation: updates.append((index, annotation)))
            chart.set_annotations(
                lines=[
                    LineAnnotation("horizontal", "L1", 0, 2, Decimal("100"), Decimal("100")),
                ],
                rr_items=[],
            )
            chart.set_selected_indexes(line_index=0, rr_index=-1)

            with (
                patch.object(chart, "_resolve_hit_target", return_value=("line_endpoint_a", 0)),
                patch.object(chart, "_chart_value_from_position", return_value=(0.0, Decimal("105"))),
            ):
                QTest.mousePress(chart.viewport(), Qt.MouseButton.LeftButton, pos=QPoint(120, 220))
                QTest.mouseRelease(chart.viewport(), Qt.MouseButton.LeftButton, pos=QPoint(140, 260))

            self.assertEqual(len(updates), 1)
            self.assertEqual(updates[0][0], 0)
            self.assertEqual(updates[0][1].price_a, Decimal("105"))
            self.assertEqual(updates[0][1].price_b, Decimal("105"))
        finally:
            self.__class__.dispose_widget(chart)

    def test_chart_line_drag_skips_submit_pending_annotation(self) -> None:
        chart = LineTradingChartView()
        try:
            updates = []
            chart.lineUpdated.connect(lambda index, annotation: updates.append((index, annotation)))
            chart.set_annotations(
                lines=[
                    LineAnnotation(
                        "horizontal",
                        "L1",
                        0,
                        2,
                        Decimal("100"),
                        Decimal("100"),
                        desk_ray_submit_pending=True,
                    ),
                ],
                rr_items=[],
            )
            chart.set_selected_indexes(line_index=0, rr_index=-1)

            with (
                patch.object(chart, "_resolve_hit_target", return_value=("line_endpoint_a", 0)),
                patch.object(chart, "_chart_value_from_position", return_value=(0.0, Decimal("105"))),
            ):
                QTest.mousePress(chart.viewport(), Qt.MouseButton.LeftButton, pos=QPoint(120, 220))
                QTest.mouseRelease(chart.viewport(), Qt.MouseButton.LeftButton, pos=QPoint(140, 260))

            self.assertEqual(updates, [])
        finally:
            self.__class__.dispose_widget(chart)

    def test_chart_rr_drag_emits_updated_annotation_on_release(self) -> None:
        chart = LineTradingChartView()
        try:
            updates = []
            chart.rrUpdated.connect(lambda index, annotation: updates.append((index, annotation)))
            chart.set_annotations(
                lines=[],
                rr_items=[
                    RiskRewardAnnotation(
                        rr_id="rr-1",
                        side="long",
                        bar_entry=0,
                        bar_stop=0,
                        price_entry=Decimal("100"),
                        price_stop=Decimal("95"),
                        price_tp=Decimal("110"),
                    )
                ],
            )
            chart.set_selected_indexes(line_index=-1, rr_index=0)

            with (
                patch.object(chart, "_resolve_hit_target", return_value=("rr_stop", 0)),
                patch.object(chart, "_chart_value_from_position", return_value=(0.0, Decimal("90"))),
            ):
                QTest.mousePress(chart.viewport(), Qt.MouseButton.LeftButton, pos=QPoint(120, 220))
                QTest.mouseRelease(chart.viewport(), Qt.MouseButton.LeftButton, pos=QPoint(140, 260))

            self.assertEqual(len(updates), 1)
            self.assertEqual(updates[0][0], 0)
            self.assertEqual(updates[0][1].price_stop, Decimal("90"))
            self.assertEqual(updates[0][1].price_tp, Decimal("120"))
        finally:
            self.__class__.dispose_widget(chart)

    def test_chart_click_emits_line_selected_when_hit_target_is_line(self) -> None:
        chart = LineTradingChartView()
        try:
            selected = []
            chart.lineSelected.connect(selected.append)

            with patch.object(chart, "_resolve_hit_target", return_value=("line", 2)):
                QTest.mouseClick(chart.viewport(), Qt.MouseButton.LeftButton, pos=QPoint(120, 220))

            self.assertEqual(selected, [2])
        finally:
            self.__class__.dispose_widget(chart)

    def test_chart_click_emits_rr_selected_when_hit_target_is_rr(self) -> None:
        chart = LineTradingChartView()
        try:
            selected = []
            chart.rrSelected.connect(selected.append)

            with patch.object(chart, "_resolve_hit_target", return_value=("rr", 1)):
                QTest.mouseClick(chart.viewport(), Qt.MouseButton.LeftButton, pos=QPoint(220, 180))

            self.assertEqual(selected, [1])
        finally:
            self.__class__.dispose_widget(chart)

    def test_chart_selected_line_uses_wider_pen(self) -> None:
        chart = LineTradingChartView()
        try:
            chart.set_annotations(
                lines=[
                    LineAnnotation("horizontal", "L1", 0, 2, Decimal("100"), Decimal("100")),
                    LineAnnotation("horizontal", "L2", 0, 2, Decimal("110"), Decimal("110")),
                ],
                rr_items=[],
            )
            chart.set_selected_indexes(line_index=1, rr_index=-1)

            named_series = {series.name(): series for series in chart.chart().series()}

            self.assertEqual(named_series["L1 [notify]"].pen().width(), 2)
            self.assertEqual(named_series["L2 [notify]"].pen().width(), 4)
        finally:
            self.__class__.dispose_widget(chart)

    def test_chart_selected_rr_uses_wider_pen_for_rr_levels(self) -> None:
        chart = LineTradingChartView()
        try:
            chart.set_annotations(
                lines=[],
                rr_items=[
                    RiskRewardAnnotation(
                        rr_id="rr-1",
                        side="long",
                        bar_entry=0,
                        bar_stop=0,
                        price_entry=Decimal("100"),
                        price_stop=Decimal("95"),
                        price_tp=Decimal("110"),
                    )
                ],
            )
            chart.set_selected_indexes(line_index=-1, rr_index=0)

            rr_series = {series.name(): series for series in chart.chart().series()}

            self.assertEqual(rr_series["RR entry"].pen().width(), 4)
            self.assertEqual(rr_series["RR stop"].pen().width(), 4)
            self.assertEqual(rr_series["RR tp"].pen().width(), 4)
        finally:
            self.__class__.dispose_widget(chart)

    def test_chart_emits_line_created_after_drag(self) -> None:
        chart = LineTradingChartView()
        try:
            chart.resize(900, 500)
            chart.set_candles(
                [
                    Candle(1000, Decimal("100"), Decimal("110"), Decimal("90"), Decimal("105"), Decimal("1"), True),
                    Candle(2000, Decimal("105"), Decimal("115"), Decimal("95"), Decimal("110"), Decimal("1"), True),
                    Candle(3000, Decimal("110"), Decimal("120"), Decimal("100"), Decimal("115"), Decimal("1"), True),
                ]
            )
            created = []
            chart.lineCreated.connect(created.append)

            chart.set_tool("line")
            with patch.object(
                chart.chart(),
                "mapToValue",
                side_effect=[QPointF(0.5, 101.25), QPointF(2.0, 114.75)],
            ):
                QTest.mousePress(chart.viewport(), Qt.MouseButton.LeftButton, pos=QPoint(100, 400))
                QTest.mouseMove(chart.viewport(), QPoint(400, 120))
                QTest.mouseRelease(chart.viewport(), Qt.MouseButton.LeftButton, pos=QPoint(400, 120))

            self.assertEqual(len(created), 1)
            self.assertEqual(created[0].kind, "line")
            self.assertEqual(created[0].bar_a, 0.5)
            self.assertEqual(created[0].bar_b, 2.0)
            self.assertEqual(created[0].price_a, Decimal("101.250000"))
            self.assertEqual(created[0].price_b, Decimal("114.750000"))
            self.assertEqual(chart._active_tool, "none")
        finally:
            self.__class__.dispose_widget(chart)

    def test_chart_emits_rr_created_after_drag(self) -> None:
        chart = LineTradingChartView()
        try:
            created = []
            chart.rrCreated.connect(created.append)

            chart.set_tool("rr_long")
            with patch.object(
                chart.chart(),
                "mapToValue",
                side_effect=[QPointF(1.0, 120.0), QPointF(3.0, 100.0)],
            ):
                QTest.mousePress(chart.viewport(), Qt.MouseButton.LeftButton, pos=QPoint(100, 120))
                QTest.mouseRelease(chart.viewport(), Qt.MouseButton.LeftButton, pos=QPoint(300, 400))

            self.assertEqual(len(created), 1)
            self.assertEqual(created[0].side, "long")
            self.assertEqual(created[0].bar_entry, 1.0)
            self.assertEqual(created[0].bar_stop, 1.0)
            self.assertEqual(created[0].price_entry, Decimal("120.000000"))
            self.assertEqual(created[0].price_stop, Decimal("100.000000"))
            self.assertEqual(created[0].price_tp, Decimal("160.000000"))
            self.assertEqual(created[0].r_multiple, Decimal("2"))
            self.assertEqual(chart._active_tool, "none")
        finally:
            self.__class__.dispose_widget(chart)

    def test_chart_uses_chart_value_coordinates_for_line_drag(self) -> None:
        chart = LineTradingChartView()
        try:
            created = []
            chart.lineCreated.connect(created.append)

            chart.set_tool("line")
            with patch.object(
                chart.chart(),
                "mapToValue",
                side_effect=[QPointF(1.25, 101.5), QPointF(4.5, 109.25)],
            ):
                QTest.mousePress(chart.viewport(), Qt.MouseButton.LeftButton, pos=QPoint(100, 400))
                QTest.mouseRelease(chart.viewport(), Qt.MouseButton.LeftButton, pos=QPoint(400, 120))

            self.assertEqual(len(created), 1)
            self.assertEqual(created[0].bar_a, 1.25)
            self.assertEqual(created[0].bar_b, 4.5)
            self.assertEqual(created[0].price_a, Decimal("101.500000"))
            self.assertEqual(created[0].price_b, Decimal("109.250000"))
        finally:
            self.__class__.dispose_widget(chart)

    def test_chart_uses_chart_value_coordinates_for_rr_drag(self) -> None:
        chart = LineTradingChartView()
        try:
            created = []
            chart.rrCreated.connect(created.append)

            chart.set_tool("rr_long")
            with patch.object(
                chart.chart(),
                "mapToValue",
                side_effect=[QPointF(1.0, 120.0), QPointF(3.0, 100.0)],
            ):
                QTest.mousePress(chart.viewport(), Qt.MouseButton.LeftButton, pos=QPoint(100, 120))
                QTest.mouseRelease(chart.viewport(), Qt.MouseButton.LeftButton, pos=QPoint(300, 400))

            self.assertEqual(len(created), 1)
            self.assertEqual(created[0].bar_entry, 1.0)
            self.assertEqual(created[0].bar_stop, 1.0)
            self.assertEqual(created[0].price_entry, Decimal("120.000000"))
            self.assertEqual(created[0].price_stop, Decimal("100.000000"))
            self.assertEqual(created[0].price_tp, Decimal("160.000000"))
        finally:
            self.__class__.dispose_widget(chart)

    def test_chart_cancels_stale_drag_on_non_left_release(self) -> None:
        chart = LineTradingChartView()
        try:
            created = []
            chart.lineCreated.connect(created.append)

            chart.set_tool("line")
            QTest.mousePress(chart.viewport(), Qt.MouseButton.LeftButton, pos=QPoint(100, 400))
            QTest.mouseRelease(chart.viewport(), Qt.MouseButton.RightButton, pos=QPoint(120, 390))
            QTest.mouseRelease(chart.viewport(), Qt.MouseButton.LeftButton, pos=QPoint(400, 120))

            self.assertEqual(created, [])
            self.assertIsNone(chart._drag_start)
            self.assertEqual(chart._active_tool, "line")
        finally:
            self.__class__.dispose_widget(chart)

    def test_chart_renders_line_annotation_series_name(self) -> None:
        chart = LineTradingChartView()
        try:
            chart.set_candles(
                [
                    Candle(1000, Decimal("100"), Decimal("110"), Decimal("90"), Decimal("105"), Decimal("1"), True),
                    Candle(2000, Decimal("105"), Decimal("115"), Decimal("95"), Decimal("110"), Decimal("1"), True),
                    Candle(3000, Decimal("110"), Decimal("120"), Decimal("100"), Decimal("115"), Decimal("1"), True),
                ]
            )
            chart.set_annotations(
                lines=[
                    LineAnnotation("horizontal", "H", 0, 2, Decimal("108"), Decimal("108")),
                ],
                rr_items=[],
            )

            names = [series.name() for series in chart.chart().series()]

            self.assertIn("H [notify]", names)
        finally:
            self.__class__.dispose_widget(chart)

    def test_chart_candles_disable_body_outline(self) -> None:
        chart = LineTradingChartView()
        try:
            chart.set_candles(
                [
                    Candle(1000, Decimal("100"), Decimal("110"), Decimal("90"), Decimal("105"), Decimal("1"), True),
                ]
            )

            candle_series = [series for series in chart.chart().series() if isinstance(series, QCandlestickSeries)]

            self.assertEqual(len(candle_series), 1)
            self.assertFalse(candle_series[0].bodyOutlineVisible())
        finally:
            self.__class__.dispose_widget(chart)

    def test_chart_renders_rr_entry_stop_and_take_profit_lines(self) -> None:
        chart = LineTradingChartView()
        try:
            chart.set_annotations(
                lines=[],
                rr_items=[
                    RiskRewardAnnotation(
                        rr_id="rr-1",
                        side="long",
                        bar_entry=0,
                        bar_stop=0,
                        price_entry=Decimal("100"),
                        price_stop=Decimal("95"),
                        price_tp=Decimal("110"),
                    )
                ],
            )

            names = [series.name() for series in chart.chart().series()]

            self.assertIn("RR entry", names)
            self.assertIn("RR stop", names)
            self.assertIn("RR tp", names)
        finally:
            self.__class__.dispose_widget(chart)

    def test_set_candles_rerenders_without_stacking_old_series(self) -> None:
        chart = LineTradingChartView()
        try:
            chart.set_candles(
                [
                    Candle(1000, Decimal("100"), Decimal("110"), Decimal("90"), Decimal("105"), Decimal("1"), True),
                ]
            )
            chart.set_candles(
                [
                    Candle(2000, Decimal("110"), Decimal("120"), Decimal("100"), Decimal("115"), Decimal("1"), True),
                ]
            )

            candle_series = [series for series in chart.chart().series() if isinstance(series, QCandlestickSeries)]

            self.assertEqual(len(candle_series), 1)
        finally:
            self.__class__.dispose_widget(chart)

    def test_set_annotations_copies_input_lists(self) -> None:
        chart = LineTradingChartView()
        try:
            lines = [LineAnnotation("horizontal", "H", 0, 2, Decimal("108"), Decimal("108"))]
            chart.set_annotations(lines=lines, rr_items=[])
            lines.clear()

            names = [series.name() for series in chart.chart().series()]

            self.assertIn("H [notify]", names)
        finally:
            self.__class__.dispose_widget(chart)


if __name__ == "__main__":
    unittest.main()
