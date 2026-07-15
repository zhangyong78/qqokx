from __future__ import annotations

from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import MagicMock, patch

import roll_terminal_qt.kline_analysis_window as kline_window
from roll_terminal_qt.kline_alerts import make_line_rule, normalize_workspace_entry


def _event(line_id: str, candle_time: int) -> dict[str, object]:
    return {
        "kind": "line_alert",
        "line_id": line_id,
        "trade_action": "notify",
        "direction": "cross_above",
        "candle_time": candle_time,
        "message": "压力线 向上突破 | 07-14 18:00",
    }


class KlineLineEmailAlertTests(TestCase):
    def _delivery_function(self):
        delivery = getattr(kline_window, "_deliver_line_alert_emails", None)
        self.assertIsNotNone(delivery)
        return delivery

    def test_once_mode_queues_one_email_and_persists_sent_state(self) -> None:
        line = make_line_rule(
            kind="horizontal",
            label="压力线",
            trigger="cross_above",
            action="notify",
            time_a=100,
            price_a=10.0,
            time_b=100,
            price_b=10.0,
            email_enabled=True,
            email_delivery_mode="once",
        )
        entry = normalize_workspace_entry({"lines": [line]})
        notifier = MagicMock(signal_notifications_enabled=True)
        deliver = self._delivery_function()

        sent_count = deliver(
            workspace_entry=entry,
            events=[_event(str(line["id"]), 200)],
            symbol="BTC-USDT-SWAP",
            period="1H",
            notifier=notifier,
        )
        repeated_count = deliver(
            workspace_entry=entry,
            events=[_event(str(line["id"]), 300)],
            symbol="BTC-USDT-SWAP",
            period="1H",
            notifier=notifier,
        )

        self.assertEqual(sent_count, 1)
        self.assertEqual(repeated_count, 0)
        self.assertTrue(entry["lines"][0]["email_sent_once"])
        notifier.notify_async.assert_called_once()
        self.assertIn("BTC-USDT-SWAP", notifier.notify_async.call_args.args[0])

    def test_repeat_mode_queues_email_for_each_new_event(self) -> None:
        line = make_line_rule(
            kind="horizontal",
            label="支撑线",
            trigger="cross_below",
            action="notify",
            time_a=100,
            price_a=10.0,
            time_b=100,
            price_b=10.0,
            email_enabled=True,
            email_delivery_mode="repeat",
        )
        entry = normalize_workspace_entry({"lines": [line]})
        notifier = MagicMock(signal_notifications_enabled=True)

        sent_count = self._delivery_function()(
            workspace_entry=entry,
            events=[_event(str(line["id"]), 200), _event(str(line["id"]), 300)],
            symbol="BTC-USDT-SWAP",
            period="1H",
            notifier=notifier,
        )

        self.assertEqual(sent_count, 2)
        self.assertFalse(entry["lines"][0]["email_sent_once"])
        self.assertEqual(notifier.notify_async.call_count, 2)

    def test_disabled_global_email_keeps_once_eligibility(self) -> None:
        line = make_line_rule(
            kind="horizontal",
            label="提醒线",
            trigger="touch",
            action="notify",
            time_a=100,
            price_a=10.0,
            time_b=100,
            price_b=10.0,
            email_enabled=True,
        )
        entry = normalize_workspace_entry({"lines": [line]})
        notifier = MagicMock(signal_notifications_enabled=False)

        sent_count = self._delivery_function()(
            workspace_entry=entry,
            events=[_event(str(line["id"]), 200)],
            symbol="BTC-USDT-SWAP",
            period="1H",
            notifier=notifier,
        )

        self.assertEqual(sent_count, 0)
        self.assertFalse(entry["lines"][0]["email_sent_once"])
        notifier.notify_async.assert_not_called()

    def test_window_dispatch_uses_current_symbol_and_period(self) -> None:
        dispatch = getattr(kline_window.KlineAnalysisWindow, "_dispatch_line_alert_emails", None)
        self.assertIsNotNone(dispatch)
        line = make_line_rule(
            kind="horizontal",
            label="mail line",
            trigger="cross_above",
            action="notify",
            time_a=100,
            price_a=10.0,
            time_b=100,
            price_b=10.0,
            email_enabled=True,
        )
        entry = normalize_workspace_entry({"lines": [line]})
        notifier = MagicMock(signal_notifications_enabled=True)
        window = SimpleNamespace(
            _symbol_combo=SimpleNamespace(currentText=lambda: "ETH-USDT-SWAP"),
            _period_combo=SimpleNamespace(currentText=lambda: "4H"),
        )

        with patch.object(kline_window, "_build_kline_line_email_notifier", return_value=notifier):
            sent_count = dispatch(window, [_event(str(line["id"]), 200)], entry)

        self.assertEqual(sent_count, 1)
        self.assertIn("ETH-USDT-SWAP", notifier.notify_async.call_args.args[0])
        self.assertIn("4H", notifier.notify_async.call_args.args[0])
