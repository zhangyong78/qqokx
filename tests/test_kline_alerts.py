from __future__ import annotations

import unittest

from roll_terminal_qt.kline_alerts import (
    build_workspace_key,
    evaluate_workspace_alerts,
    line_value_at,
    make_line_rule,
    normalize_workspace_entry,
)


class KlineAlertTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
