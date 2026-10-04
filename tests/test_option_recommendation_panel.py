from __future__ import annotations

from dataclasses import replace
from threading import Event
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import MagicMock, patch

from PySide6.QtTest import QTest
from PySide6.QtWidgets import QGroupBox, QPushButton
from shiboken6 import isValid

from okx_quant.deribit_client import DeribitVolatilityCandle
from okx_quant.option_recommendations import build_option_recommendations
from roll_terminal_qt.option_recommendation_panel import (
    OptionRecommendationPanel, OptionRecommendationThread, _load_recommendation_dvol,
)
from tests.qt_test_case import QtWidgetTestCase
from tests.test_option_recommendations import NOW, DAY_MS, prices, quote, volatility


def snapshot():
    return build_option_recommendations(family="BTC-USD", price_candles=prices(),
                                       dvol_hourly=volatility(), now_ms=NOW)


class BlockingRecommendation(OptionRecommendationThread):
    def __init__(self, *args):
        super().__init__(*args)
        self.release = Event()

    def run(self):
        self.release.wait(3)


class RecommendationPanelTest(QtWidgetTestCase):
    def setUp(self):
        self.panel = OptionRecommendationPanel()
        self.workers = []

    def tearDown(self):
        for worker in self.workers:
            worker.release.set()
            if isValid(worker):
                worker.wait(1000)
        self._app.processEvents()
        self.dispose_widget(self.panel)

    def test_initial_page_has_no_automatic_network_and_unsupported_family_is_disabled(self):
        self.assertIsNone(self.panel._worker)
        self.assertTrue(self.panel._refresh_button.isEnabled())
        self.panel.set_family("SOL-USD")
        self.assertFalse(self.panel._refresh_button.isEnabled())

    def test_old_request_or_family_cannot_replace_latest_market(self):
        snap = snapshot()
        self.panel._apply_snapshot(self.panel._request_id+1, snap)
        self.assertIsNone(self.panel._snapshot)
        self.panel.set_family("ETH-USD")
        self.panel._apply_snapshot(self.panel._request_id, snap)
        self.assertIsNone(self.panel._snapshot)

    def test_naked_seller_is_hidden_until_explicitly_expanded(self):
        snap = snapshot()
        seller = replace(snap.suggestions[0], name="裸卖测试", high_risk=True)
        snap = replace(snap, suggestions=(seller,))
        self.panel._apply_snapshot(self.panel._request_id, snap)
        titles = [box.title() for box in self.panel.findChildren(QGroupBox)]
        self.assertFalse(any("裸卖测试" in title for title in titles))
        self.panel._show_high_risk.setChecked(True)
        self._app.processEvents()
        self.assertTrue(any("裸卖测试" in box.title() for box in self.panel.findChildren(QGroupBox)))

    def test_repeated_refresh_does_not_spawn_duplicate_workers_and_shutdown_waits(self):
        with patch("roll_terminal_qt.option_recommendation_panel.OptionRecommendationThread", BlockingRecommendation):
            self.panel.refresh()
            worker = self.panel._worker
            self.workers.append(worker)
            self.panel.refresh()
            self.assertIs(self.panel._worker, worker)
            self.assertFalse(self.panel.shutdown())
            self.assertFalse(self.panel._freshness_timer.isActive())
            self.panel._apply_snapshot(worker.request_id, snapshot())
            self.assertIsNone(self.panel._snapshot)
            self.panel.resume()
            self.assertTrue(self.panel._freshness_timer.isActive())
            self.assertFalse(self.panel._refresh_button.isEnabled())
            worker.release.set()
            worker.wait(1000)
            QTest.qWait(20)
            self.assertIsNone(self.panel._worker)
            self.assertTrue(self.panel._refresh_button.isEnabled())

    def test_old_analysis_is_explicitly_marked_as_historical(self):
        self.panel._apply_snapshot(self.panel._request_id, snapshot())
        with patch("roll_terminal_qt.option_recommendation_panel.time.time", return_value=(NOW+360_000)/1000):
            self.panel._refresh_freshness()
        self.assertIn("历史参考", self.panel._status.text())

    def test_import_button_emits_selected_structure_and_rejects_stale_or_previous_cards(self):
        snap = build_option_recommendations(family="BTC-USD", price_candles=prices(),
                                           dvol_hourly=volatility(), now_ms=NOW,
                                           quotes=[quote("261120", "120"), quote("261120", "125")])
        self.panel._apply_snapshot(self.panel._request_id, snap)
        button = self.panel._cards.findChild(QPushButton, "ImportRecommendationButton")
        self.assertTrue(button.isEnabled())
        received = []
        self.panel.analysisRequested.connect(lambda *args: received.append(args))
        with patch("roll_terminal_qt.option_recommendation_panel.time.time", return_value=NOW/1000):
            button.click()
        self.assertEqual(received, [(snap, snap.suggestions[0])])
        with patch("roll_terminal_qt.option_recommendation_panel.time.time", return_value=(NOW+300_001)/1000):
            button.click()
        self.assertEqual(len(received), 1)
        self.panel.set_family("ETH-USD")
        button.click()
        self.assertEqual(len(received), 1)

    def test_incomplete_candidate_cannot_be_imported(self):
        snap = snapshot()
        self.panel._apply_snapshot(self.panel._request_id, snap)
        button = self.panel._cards.findChild(QPushButton, "ImportRecommendationButton")
        self.assertFalse(button.isEnabled())


class RecommendationLoadingTest(TestCase):
    def test_short_fresh_dvol_cache_is_backfilled_and_saved(self):
        full = volatility()
        raw = [DeribitVolatilityCandle(c.ts, c.open, c.high, c.low, c.close) for c in full]
        client = MagicMock()
        client.get_volatility_index_candles.return_value = raw
        with patch("roll_terminal_qt.option_recommendation_panel._load_latest_deribit_option_chart_candles", return_value=(full[-48:], "1H", "已刷新")), \
             patch("roll_terminal_qt.option_recommendation_panel._load_deribit_hourly_series_from_cache", return_value=raw[-48:]), \
             patch("roll_terminal_qt.option_recommendation_panel._save_deribit_hourly_series_to_cache") as save, \
             patch("roll_terminal_qt.option_recommendation_panel.DeribitRestClient", return_value=client):
            result, note = _load_recommendation_dvol("BTC", NOW)
            self.assertGreater(len(result), 60*24)
            self.assertIn("统计历史", note)
            self.assertEqual(client.get_volatility_index_candles.call_args.kwargs["start_ts"], NOW-92*DAY_MS)
            save.assert_called_once()

    def test_complete_history_does_not_need_second_full_backfill(self):
        with patch("roll_terminal_qt.option_recommendation_panel._load_latest_deribit_option_chart_candles", return_value=(volatility(), "1H", "已刷新")), \
             patch("roll_terminal_qt.option_recommendation_panel.DeribitRestClient") as client:
            _load_recommendation_dvol("BTC", NOW)
            client.assert_not_called()

    def test_quote_timestamp_missing_stale_and_future_quotes_are_excluded(self):
        client = MagicMock()
        quotes = [quote("261120", str(120+i)) for i in range(5)]
        client.get_candles_history.side_effect = lambda symbol, period, limit: prices()[period]
        client.get_option_instruments.return_value = [q.instrument for q in quotes]
        client.get_tickers.return_value = [SimpleNamespace(inst_id=q.instrument.inst_id, raw=raw)
            for q, raw in zip(quotes, ({"ts": str(NOW)}, {}, {"ts": "0"}, {"ts": str(NOW-600_000)}, {"ts": str(NOW+120_000)}))]
        thread = OptionRecommendationThread(1, "BTC-USD")
        with patch("roll_terminal_qt.option_recommendation_panel.OkxRestClient", return_value=client), \
             patch("roll_terminal_qt.option_recommendation_panel._load_recommendation_dvol", return_value=(volatility(), "")), \
             patch("roll_terminal_qt.option_recommendation_panel.time.time", return_value=NOW/1000), \
             patch("roll_terminal_qt.option_recommendation_panel._build_option_quote", return_value=quotes[0]), \
             patch("roll_terminal_qt.option_recommendation_panel.build_option_recommendations", return_value=snapshot()) as build:
            thread.run()
            self.assertEqual(len(build.call_args.kwargs["quotes"]), 1)
            self.assertEqual(client.get_candles_history.call_count, 4)
