from __future__ import annotations

import unittest
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

from roll_terminal_qt.kline_alerts import (
    build_workspace_key,
    evaluate_workspace_alerts,
    line_value_at,
    make_line_rule,
    normalize_workspace_entry,
)
from roll_terminal_qt.kline_analysis_window import _line_price_table_text


class KlineAlertTests(unittest.TestCase):
    def test_line_price_table_text_distinguishes_horizontal_and_trend_prices(self) -> None:
        horizontal = make_line_rule(
            kind="horizontal",
            label="H",
            trigger="cross_above",
            action="notify",
            time_a=100,
            price_a=10.5,
            time_b=100,
            price_b=10.5,
        )
        trend = make_line_rule(
            kind="trend",
            label="T",
            trigger="cross_above",
            action="notify",
            time_a=100,
            price_a=10.0,
            time_b=200,
            price_b=20.0,
        )

        self.assertEqual(_line_price_table_text(horizontal), "10.5")
        self.assertEqual(_line_price_table_text(trend), "10 → 20")

    def test_build_workspace_key_normalizes_symbol(self) -> None:
        self.assertEqual(build_workspace_key("btc-usdt-swap", "15m"), "BTC-USDT-SWAP|15m")

    def test_normalize_workspace_entry_sets_defaults(self) -> None:
        normalized = normalize_workspace_entry({})
        self.assertTrue(normalized["alerts"]["ma_cross"]["enabled"])
        self.assertFalse(normalized["alerts"]["box_breakout"]["enabled"])
        self.assertEqual(normalized["lines"], [])

    def test_line_value_at_projects_trend_line(self) -> None:
        line = make_line_rule(
            kind="trend",
            label="trend",
            trigger="cross_above",
            action="notify",
            time_a=100,
            price_a=10.0,
            time_b=200,
            price_b=20.0,
        )
        self.assertAlmostEqual(line_value_at(line, 150), 15.0)

    def test_line_email_configuration_defaults_to_disabled_once(self) -> None:
        line = make_line_rule(
            kind="horizontal",
            label="邮件线",
            trigger="cross_above",
            action="notify",
            time_a=100,
            price_a=10.0,
            time_b=100,
            price_b=10.0,
        )

        self.assertFalse(line.get("email_enabled", True))
        self.assertEqual(line.get("email_delivery_mode"), "once")
        self.assertFalse(line.get("email_sent_once", True))

    def test_make_line_rule_orders_reversed_trend_endpoints(self) -> None:
        line = make_line_rule(
            kind="trend",
            label="trend",
            trigger="cross_above",
            action="notify",
            time_a=300,
            price_a=30.0,
            time_b=100,
            price_b=10.0,
        )
        self.assertEqual((line["time_a"], line["time_b"]), (100, 300))
        self.assertEqual((line["price_a"], line["price_b"]), (10.0, 30.0))

    def test_evaluate_workspace_alerts_emits_ma_cross_once_per_candle(self) -> None:
        workspace = normalize_workspace_entry({})
        candles = [
            {"time": 100, "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0},
            {"time": 200, "open": 2.0, "high": 2.0, "low": 2.0, "close": 2.0},
        ]
        ema_fast = [{"time": 100, "value": 1.0}, {"time": 200, "value": 3.0}]
        ma_slow = [{"time": 100, "value": 2.0}, {"time": 200, "value": 2.5}]

        updated, events, _summary = evaluate_workspace_alerts(
            workspace_entry=workspace,
            candles=candles,
            ema_fast=ema_fast,
            ma_slow=ma_slow,
            raw_candles=[],
        )
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["kind"], "ma_cross")

        updated_again, events_again, _summary_again = evaluate_workspace_alerts(
            workspace_entry=updated,
            candles=candles,
            ema_fast=ema_fast,
            ma_slow=ma_slow,
            raw_candles=[],
        )
        self.assertEqual(events_again, [])
        self.assertEqual(len(updated_again["events"]), 1)

    def test_evaluate_workspace_alerts_emits_line_cross(self) -> None:
        workspace = normalize_workspace_entry(
            {
                "lines": [
                    make_line_rule(
                        kind="horizontal",
                        label="R1",
                        trigger="cross_above",
                        action="notify",
                        time_a=100,
                        price_a=10.0,
                        time_b=100,
                        price_b=10.0,
                    )
                ]
            }
        )
        candles = [
            {"time": 100, "open": 9.0, "high": 9.5, "low": 8.8, "close": 9.5},
            {"time": 200, "open": 9.6, "high": 10.5, "low": 9.4, "close": 10.4},
        ]

        updated, events, _summary = evaluate_workspace_alerts(
            workspace_entry=workspace,
            candles=candles,
            ema_fast=[],
            ma_slow=[],
            raw_candles=[],
        )
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["kind"], "line_alert")
        self.assertTrue(updated["lines"][0]["triggered"])

    def test_line_trade_event_preserves_explicit_trade_configuration(self) -> None:
        line = make_line_rule(
            kind="horizontal",
            label="Long breakout",
            trigger="cross_above",
            action="long",
            time_a=100,
            price_a=10.0,
            time_b=100,
            price_b=10.0,
            trade_enabled=True,
            risk_amount=125.0,
            stop_loss_price=9.0,
            direct_take_profit_r=2.5,
            management_mode="trail_after_2r",
            entry_execution_mode="chase_best_quote",
            fee_offset_enabled=True,
        )
        workspace = normalize_workspace_entry({"lines": [line]})
        candles = [
            {"time": 100, "open": 9.0, "high": 9.5, "low": 8.8, "close": 9.5},
            {"time": 200, "open": 9.6, "high": 10.5, "low": 9.4, "close": 10.4},
        ]

        updated, events, _summary = evaluate_workspace_alerts(
            workspace_entry=workspace,
            candles=candles,
            ema_fast=[],
            ma_slow=[],
            raw_candles=[],
        )

        self.assertEqual(events[0]["line_id"], line["id"])
        self.assertEqual(events[0]["trade_action"], "long")
        self.assertTrue(events[0]["trade_enabled"])
        saved_line = updated["lines"][0]
        self.assertTrue(saved_line["trade_enabled"])
        self.assertEqual(saved_line["risk_amount"], 125.0)
        self.assertEqual(saved_line["stop_loss_price"], 9.0)
        self.assertEqual(saved_line["management_mode"], "trail_after_2r")
        self.assertEqual(saved_line["entry_execution_mode"], "chase_best_quote")

    def test_evaluate_workspace_alerts_preserves_rr_items(self) -> None:
        workspace = normalize_workspace_entry(
            {
                "rr": [
                    {
                        "rr_id": "rr-1",
                        "side": "long",
                        "bar_entry": 12.0,
                        "bar_stop": 12.0,
                        "price_entry": "60000",
                        "price_stop": "59000",
                        "price_tp": "62000",
                        "r_multiple": "2",
                        "locked": False,
                    }
                ]
            }
        )

        updated, events, _summary = evaluate_workspace_alerts(
            workspace_entry=workspace,
            candles=[],
            ema_fast=[],
            ma_slow=[],
            raw_candles=[],
        )

        self.assertEqual(events, [])
        self.assertEqual(len(updated["rr"]), 1)
        self.assertEqual(updated["rr"][0]["rr_id"], "rr-1")

    def test_evaluate_workspace_alerts_emits_box_breakout_from_current_effective_box(self) -> None:
        workspace = normalize_workspace_entry({"alerts": {"box_breakout": {"enabled": True}}})
        raw_candles = [
            SimpleNamespace(
                ts=index * 1000,
                open=Decimal("9.20"),
                high=Decimal("9.40"),
                low=Decimal("9.00"),
                close=Decimal("9.20"),
                confirmed=True,
            )
            for index in range(23)
        ]
        raw_candles.append(
            SimpleNamespace(
                ts=23_000,
                open=Decimal("9.30"),
                high=Decimal("9.80"),
                low=Decimal("9.10"),
                close=Decimal("9.60"),
                confirmed=True,
            )
        )
        raw_candles.append(
            SimpleNamespace(
                ts=24_000,
                open=Decimal("9.70"),
                high=Decimal("10.80"),
                low=Decimal("9.50"),
                close=Decimal("10.60"),
                confirmed=True,
            )
        )

        with patch(
            "roll_terminal_qt.kline_alerts.detect_boxes",
            return_value=[
                SimpleNamespace(
                    start_index=4,
                    end_index=23,
                    upper=Decimal("10.00"),
                    lower=Decimal("9.30"),
                    upper_touches=2,
                    lower_touches=2,
                    violations=0,
                    score=Decimal("88"),
                )
            ],
        ):
            updated, events, _summary = evaluate_workspace_alerts(
                workspace_entry=workspace,
                candles=[],
                ema_fast=[],
                ma_slow=[],
                raw_candles=raw_candles,
            )

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["kind"], "box_breakout")
        self.assertEqual(events[0]["direction"], "cross_above")
        self.assertEqual(updated["alerts"]["box_breakout"]["last_event_candle_time"], 24)

    def test_evaluate_workspace_alerts_ignores_too_wide_box_breakout(self) -> None:
        workspace = normalize_workspace_entry({"alerts": {"box_breakout": {"enabled": True}}})
        raw_candles = [
            SimpleNamespace(
                ts=index * 1000,
                open=Decimal("100"),
                high=Decimal("101"),
                low=Decimal("99"),
                close=Decimal("100"),
                confirmed=True,
            )
            for index in range(24)
        ]
        raw_candles.append(
            SimpleNamespace(
                ts=24_000,
                open=Decimal("100"),
                high=Decimal("112"),
                low=Decimal("99"),
                close=Decimal("112"),
                confirmed=True,
            )
        )

        with patch(
            "roll_terminal_qt.kline_alerts.detect_boxes",
            return_value=[
                SimpleNamespace(
                    start_index=4,
                    end_index=23,
                    upper=Decimal("110"),
                    lower=Decimal("90"),
                    upper_touches=3,
                    lower_touches=3,
                    violations=0,
                    score=Decimal("90"),
                )
            ],
        ):
            updated, events, _summary = evaluate_workspace_alerts(
                workspace_entry=workspace,
                candles=[],
                ema_fast=[],
                ma_slow=[],
                raw_candles=raw_candles,
            )

        self.assertEqual(events, [])
        self.assertEqual(updated["alerts"]["box_breakout"]["last_event_candle_time"], 0)


if __name__ == "__main__":
    unittest.main()
