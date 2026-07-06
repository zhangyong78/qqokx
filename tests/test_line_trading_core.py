from __future__ import annotations

from decimal import Decimal
import unittest

from roll_terminal_qt.line_trading_core import (
    ChartGeometry,
    HitTarget,
    LineAnnotation,
    RiskRewardAnnotation,
    compute_rr_target,
    line_annotation_from_payload,
    line_annotation_to_payload,
    bar_price_to_scene,
    drag_line_annotation,
    drag_rr_annotation,
    nearest_line_hit,
    nearest_rr_hit,
    parse_decimal,
    rr_annotation_from_payload,
    rr_annotation_to_payload,
    scene_to_bar_price,
)


class LineTradingCoreTests(unittest.TestCase):
    def _sample_geometry(self) -> ChartGeometry:
        return ChartGeometry(
            plot_left=10,
            plot_top=20,
            plot_width=1000,
            plot_height=500,
            first_bar=100,
            last_bar=200,
            min_price=Decimal("50000"),
            max_price=Decimal("60000"),
        )

    def test_chart_geometry_roundtrip_maps_bar_and_price(self) -> None:
        geometry = self._sample_geometry()

        x, y = bar_price_to_scene(geometry, 150, Decimal("55000"))
        bar, price = scene_to_bar_price(geometry, x, y)

        self.assertEqual((x, y), (510.0, 270.0))
        self.assertEqual(bar, 150.0)
        self.assertEqual(price, Decimal("55000"))

    def test_scene_to_bar_price_clamps_to_plot_rect(self) -> None:
        geometry = self._sample_geometry()

        low_bar, high_price = scene_to_bar_price(geometry, -100, -100)
        high_bar, low_price = scene_to_bar_price(geometry, 2000, 2000)

        self.assertEqual(low_bar, 100.0)
        self.assertEqual(high_price, Decimal("60000"))
        self.assertEqual(high_bar, 200.0)
        self.assertEqual(low_price, Decimal("50000"))

    def test_scene_to_bar_price_handles_non_finite_mouse_coordinates(self) -> None:
        geometry = self._sample_geometry()

        bar, price = scene_to_bar_price(geometry, float("nan"), float("inf"))

        self.assertEqual(bar, 100.0)
        self.assertEqual(price, Decimal("60000.0"))

    def test_scene_to_bar_price_handles_zero_and_inverted_ranges(self) -> None:
        zero_geometry = ChartGeometry(10, 20, 0, 0, 100, 200, Decimal("50000"), Decimal("60000"))
        inverted_geometry = ChartGeometry(110, 120, -100, -50, 200, 100, Decimal("50000"), Decimal("60000"))

        zero_bar, zero_price = scene_to_bar_price(zero_geometry, 500, 500)
        inverted_bar, inverted_price = scene_to_bar_price(inverted_geometry, 60, 95)

        self.assertEqual(zero_bar, 100.0)
        self.assertEqual(zero_price, Decimal("60000"))
        self.assertEqual(inverted_bar, 150.0)
        self.assertEqual(inverted_price, Decimal("55000.0"))

    def test_bar_price_to_scene_rounds_to_six_decimals(self) -> None:
        geometry = ChartGeometry(0, 0, 100, 100, 0, 3, Decimal("0"), Decimal("3"))

        x, y = bar_price_to_scene(geometry, 1, Decimal("1"))

        self.assertEqual(x, 33.333333)
        self.assertEqual(y, 66.666667)

    def test_bar_price_to_scene_uses_minimum_spans(self) -> None:
        geometry = ChartGeometry(0, 0, 100, 100, 10, 10, Decimal("5"), Decimal("5"))

        x, y = bar_price_to_scene(geometry, 10, Decimal("5"))

        self.assertEqual((x, y), (0.0, 100.0))

    def test_decimal_to_text_preserves_high_precision_values(self) -> None:
        annotation = LineAnnotation(
            kind="line",
            label="precise",
            bar_a=1,
            bar_b=2,
            price_a=Decimal("123456789012345678901234567890.1234500"),
            price_b=Decimal("1.0000000000000000000000000001"),
        )

        payload = line_annotation_to_payload(annotation)

        self.assertEqual(payload["price_a"], "123456789012345678901234567890.12345")
        self.assertEqual(payload["price_b"], "1.0000000000000000000000000001")

    def test_nearest_line_hit_returns_unlocked_endpoint(self) -> None:
        geometry = self._sample_geometry()
        line = LineAnnotation(
            kind="trend",
            label="A",
            bar_a=150,
            bar_b=180,
            price_a=Decimal("55000"),
            price_b=Decimal("58000"),
        )

        hit = nearest_line_hit(geometry, [line], x=512, y=271, tolerance=5)

        self.assertEqual(hit, HitTarget(kind="line_endpoint_a", index=0))

    def test_nearest_line_hit_skips_locked_lines(self) -> None:
        geometry = self._sample_geometry()
        line = LineAnnotation(
            kind="trend",
            label="A",
            bar_a=150,
            bar_b=180,
            price_a=Decimal("55000"),
            price_b=Decimal("58000"),
            locked=True,
        )

        hit = nearest_line_hit(geometry, [line], x=510, y=270, tolerance=5)

        self.assertIsNone(hit)

    def test_nearest_line_hit_prefers_nearest_candidate_within_tolerance(self) -> None:
        geometry = self._sample_geometry()
        lines = [
            LineAnnotation(
                kind="trend",
                label="far",
                bar_a=150,
                bar_b=180,
                price_a=Decimal("55000"),
                price_b=Decimal("58000"),
            ),
            LineAnnotation(
                kind="trend",
                label="near",
                bar_a=151,
                bar_b=190,
                price_a=Decimal("55000"),
                price_b=Decimal("59000"),
            ),
        ]

        hit = nearest_line_hit(geometry, lines, x=520.5, y=270, tolerance=20)

        self.assertEqual(hit, HitTarget(kind="line_endpoint_a", index=1))

    def test_nearest_rr_hit_returns_nearest_rr_price_line(self) -> None:
        geometry = self._sample_geometry()
        rr_items = [
            RiskRewardAnnotation(
                rr_id="rr-1",
                side="long",
                bar_entry=150,
                bar_stop=150,
                price_entry=Decimal("55000"),
                price_stop=Decimal("54000"),
                price_tp=Decimal("57000"),
            )
        ]

        hit = nearest_rr_hit(geometry, rr_items, x=512, y=271, tolerance=8)

        self.assertEqual(hit, HitTarget(kind="rr_entry", index=0))

    def test_nearest_rr_hit_skips_locked_rr_items(self) -> None:
        geometry = self._sample_geometry()
        rr_items = [
            RiskRewardAnnotation(
                rr_id="rr-1",
                side="long",
                bar_entry=150,
                bar_stop=150,
                price_entry=Decimal("55000"),
                price_stop=Decimal("54000"),
                price_tp=Decimal("57000"),
                locked=True,
            )
        ]

        hit = nearest_rr_hit(geometry, rr_items, x=512, y=271, tolerance=8)

        self.assertIsNone(hit)

    def test_drag_rr_annotation_updates_long_stop_and_recomputes_tp(self) -> None:
        annotation = RiskRewardAnnotation(
            rr_id="rr-1",
            side="long",
            bar_entry=10.0,
            bar_stop=10.0,
            price_entry=Decimal("100"),
            price_stop=Decimal("95"),
            price_tp=Decimal("110"),
            r_multiple=Decimal("2"),
        )

        updated = drag_rr_annotation(annotation, "rr_stop", Decimal("90"))

        self.assertEqual(updated.price_entry, Decimal("100"))
        self.assertEqual(updated.price_stop, Decimal("90"))
        self.assertEqual(updated.price_tp, Decimal("120"))
        self.assertEqual(updated.r_multiple, Decimal("2"))

    def test_drag_rr_annotation_updates_long_tp_and_recomputes_r_multiple(self) -> None:
        annotation = RiskRewardAnnotation(
            rr_id="rr-1",
            side="long",
            bar_entry=10.0,
            bar_stop=10.0,
            price_entry=Decimal("100"),
            price_stop=Decimal("95"),
            price_tp=Decimal("110"),
            r_multiple=Decimal("2"),
        )

        updated = drag_rr_annotation(annotation, "rr_tp", Decimal("115"))

        self.assertEqual(updated.price_entry, Decimal("100"))
        self.assertEqual(updated.price_stop, Decimal("95"))
        self.assertEqual(updated.price_tp, Decimal("115"))
        self.assertEqual(updated.r_multiple, Decimal("3"))

    def test_drag_line_annotation_updates_horizontal_prices_together(self) -> None:
        annotation = LineAnnotation(
            kind="horizontal",
            label="L1",
            bar_a=10.0,
            bar_b=20.0,
            price_a=Decimal("100"),
            price_b=Decimal("100"),
        )

        updated = drag_line_annotation(annotation, "line_endpoint_a", new_bar=12.0, new_price=Decimal("105"))

        self.assertEqual(updated.bar_a, 10.0)
        self.assertEqual(updated.bar_b, 20.0)
        self.assertEqual(updated.price_a, Decimal("105"))
        self.assertEqual(updated.price_b, Decimal("105"))

    def test_drag_line_annotation_reorders_trend_endpoints_when_crossed(self) -> None:
        annotation = LineAnnotation(
            kind="line",
            label="L1",
            bar_a=10.0,
            bar_b=20.0,
            price_a=Decimal("100"),
            price_b=Decimal("110"),
        )

        updated = drag_line_annotation(annotation, "line_endpoint_a", new_bar=25.0, new_price=Decimal("120"))

        self.assertEqual(updated.bar_a, 20.0)
        self.assertEqual(updated.bar_b, 25.0)
        self.assertEqual(updated.price_a, Decimal("110"))
        self.assertEqual(updated.price_b, Decimal("120"))

    def test_line_annotation_roundtrip_preserves_decimal_prices_and_flags(self) -> None:
        annotation = LineAnnotation(
            kind="trend",
            label="Breakout",
            bar_a=12.0,
            bar_b=18.0,
            price_a=Decimal("123.4500"),
            price_b=Decimal("130.0000"),
            x1=1.0,
            y1=2.0,
            x2=3.0,
            y2=4.0,
            color="#22c55e",
            desk_ray_action="long",
            desk_ray_triggered=True,
            desk_ray_submit_pending=True,
            desk_ray_last_side=-1,
            locked=True,
        )

        payload = line_annotation_to_payload(annotation)
        restored = line_annotation_from_payload(payload)

        self.assertEqual(payload["price_a"], "123.45")
        self.assertEqual(payload["price_b"], "130")
        self.assertEqual(payload["x1"], 1.0)
        self.assertEqual(payload["y1"], 2.0)
        self.assertEqual(payload["x2"], 3.0)
        self.assertEqual(payload["y2"], 4.0)
        self.assertEqual(payload["desk_ray_action"], "long")
        self.assertIs(payload["locked"], True)
        self.assertIs(payload["desk_ray_triggered"], True)
        self.assertIs(payload["desk_ray_submit_pending"], False)
        restored_expected = LineAnnotation(
            **{
                **annotation.__dict__,
                "price_a": Decimal("123.45"),
                "price_b": Decimal("130"),
                "desk_ray_submit_pending": False,
            }
        )
        self.assertEqual(restored, restored_expected)

    def test_risk_reward_roundtrip_preserves_decimal_values_and_flags(self) -> None:
        annotation = RiskRewardAnnotation(
            rr_id="rr-1",
            side="short",
            bar_entry=20.0,
            bar_stop=21.0,
            price_entry=Decimal("100.5000"),
            price_stop=Decimal("105.25"),
            price_tp=Decimal("91.000"),
            r_multiple=Decimal("2.0"),
            locked=True,
        )

        payload = rr_annotation_to_payload(annotation)
        restored = rr_annotation_from_payload(payload)

        self.assertEqual(payload["price_entry"], "100.5")
        self.assertEqual(payload["price_tp"], "91")
        self.assertEqual(payload["r_multiple"], "2")
        self.assertIs(payload["locked"], True)
        self.assertEqual(
            restored,
            RiskRewardAnnotation(
                **{
                    **annotation.__dict__,
                    "price_entry": Decimal("100.5"),
                    "price_tp": Decimal("91"),
                    "r_multiple": Decimal("2"),
                }
            ),
        )

    def test_old_tk_payload_keys_are_parsed(self) -> None:
        line = line_annotation_from_payload(
            {
                "kind": "horizontal",
                "x1": 100.0,
                "y1": 200.0,
                "x2": 300.0,
                "y2": 400.0,
                "label": "H-1",
                "bar_a": 12.0,
                "bar_b": 30.0,
                "price_a": "61000",
                "price_b": "61000",
                "color": "#1d4ed8",
                "desk_ray_action": "long",
                "desk_ray_triggered": False,
                "desk_ray_submit_pending": False,
                "desk_ray_last_side": None,
                "locked": True,
            }
        )
        self.assertEqual(line.desk_ray_action, "long")
        self.assertEqual(line.price_a, Decimal("61000"))
        self.assertEqual(line.x1, 100.0)
        self.assertEqual(line.y2, 400.0)
        self.assertFalse(line.desk_ray_submit_pending)
        self.assertTrue(line.locked)

        rr = rr_annotation_from_payload(
            {
                "rr_id": "rr-1",
                "side": "long",
                "bar_entry": 20.0,
                "bar_stop": 20.0,
                "price_entry": "60000",
                "price_stop": "59000",
                "price_tp": "62000",
                "r_multiple": "2",
                "locked": False,
            }
        )
        self.assertEqual(rr.price_tp, Decimal("62000"))
        self.assertEqual(rr_annotation_to_payload(rr)["price_tp"], "62000")

    def test_rr_annotation_normalizes_invalid_legacy_fields(self) -> None:
        rr = rr_annotation_from_payload({"side": "bad", "r_multiple": "0"})

        self.assertEqual(rr.side, "long")
        self.assertEqual(rr.r_multiple, Decimal("2"))

    def test_rr_target_calculates_long_and_short_targets(self) -> None:
        self.assertEqual(compute_rr_target("long", Decimal("100"), Decimal("95"), Decimal("2")), Decimal("110"))
        self.assertEqual(compute_rr_target("buy", Decimal("100"), Decimal("95"), Decimal("1.5")), Decimal("107.5"))
        self.assertEqual(compute_rr_target("short", Decimal("100"), Decimal("105"), Decimal("2")), Decimal("90"))
        self.assertEqual(compute_rr_target("sell", Decimal("100"), Decimal("105"), Decimal("1.5")), Decimal("92.5"))

    def test_rr_target_rejects_invalid_risk_shape(self) -> None:
        with self.assertRaises(ValueError):
            compute_rr_target("long", Decimal("100"), Decimal("105"), Decimal("2"))
        with self.assertRaises(ValueError):
            compute_rr_target("short", Decimal("100"), Decimal("95"), Decimal("2"))
        with self.assertRaises(ValueError):
            compute_rr_target("long", Decimal("100"), Decimal("95"), Decimal("0"))

    def test_parse_decimal_returns_default_for_missing_blank_or_invalid_values(self) -> None:
        default = Decimal("7.5")

        self.assertEqual(parse_decimal(None, default), default)
        self.assertEqual(parse_decimal("", default), default)
        self.assertEqual(parse_decimal("   ", default), default)
        self.assertEqual(parse_decimal("not-a-number", default), default)
        self.assertEqual(parse_decimal("NaN", default), default)
        self.assertEqual(parse_decimal("Infinity", default), default)
        self.assertEqual(parse_decimal(Decimal("NaN"), default), default)
        self.assertEqual(parse_decimal(Decimal("Infinity"), default), default)
        self.assertEqual(parse_decimal("1.2300", default), Decimal("1.2300"))

    def test_invalid_float_coordinates_fall_back_to_zero(self) -> None:
        annotation = line_annotation_from_payload(
            {
                "kind": "line",
                "bar_a": "nan",
                "bar_b": "Infinity",
                "price_a": "1",
                "price_b": "2",
                "x1": "bad",
                "y1": "nan",
                "x2": "Infinity",
                "y2": "-Infinity",
            }
        )

        self.assertEqual(annotation.bar_a, 0.0)
        self.assertEqual(annotation.bar_b, 0.0)
        self.assertEqual(annotation.x1, 0.0)
        self.assertEqual(annotation.y1, 0.0)
        self.assertEqual(annotation.x2, 0.0)
        self.assertEqual(annotation.y2, 0.0)

    def test_string_boolean_fields_are_parsed_consistently(self) -> None:
        annotation = line_annotation_from_payload(
            {
                "kind": "line",
                "price_a": "1",
                "price_b": "2",
                "locked": "yes",
                "desk_ray_triggered": "0",
                "desk_ray_submit_pending": "yes",
            }
        )

        self.assertTrue(annotation.locked)
        self.assertFalse(annotation.desk_ray_triggered)
        self.assertFalse(annotation.desk_ray_submit_pending)

    def test_invalid_last_side_falls_back_to_none(self) -> None:
        annotation = line_annotation_from_payload(
            {
                "kind": "line",
                "price_a": "1",
                "price_b": "2",
                "desk_ray_last_side": float("inf"),
            }
        )

        self.assertIsNone(annotation.desk_ray_last_side)


if __name__ == "__main__":
    unittest.main()
