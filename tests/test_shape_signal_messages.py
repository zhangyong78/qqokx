import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from PySide6.QtWidgets import QApplication

from okx_quant import shape_signal_store as store
from okx_quant.shape_signal_store import normalize_subscription
from roll_terminal_qt.launcher import LauncherWindow
from roll_terminal_qt.shape_signal_dialog import ShapeSignalHistoryDialog
from roll_terminal_qt.kline_analysis_window import KlineAnalysisWindow
from roll_terminal_qt.workspace_shell import WorkspaceHeader


class ShapeSignalMessageTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = QApplication.instance() or QApplication([])

    def test_header_unread_status_and_click_route(self):
        header = WorkspaceHeader()
        routes = []
        header.tool_requested.connect(routes.append)
        header.set_shape_message_status(3, '正在监控')
        self.assertIn('3', header.shape_message_button.text())
        self.assertTrue(header.shape_message_button.property('unread'))
        self.assertIn('正在监控', header.shape_message_button.toolTip())
        header.shape_message_button.click()
        self.assertEqual(routes, ['shape-messages'])
        header.set_shape_message_status(0, '正在监控')
        self.assertFalse(header.shape_message_button.property('unread'))
        header.close()

    def test_weekly_period_is_supported_by_shape_subscription(self):
        subscription = normalize_subscription({'periods': ['1W'], 'patterns': ['big_bullish']})
        self.assertEqual(subscription['periods'], ['1W'])

    def test_volatility_tabs_switch_to_existing_dvol_secondary_loader(self):
        window = KlineAnalysisWindow()
        try:
            window._load_secondary_data = Mock()
            dvol_index = window._symbol_tab_bar.count() - 2
            self.assertEqual(window._symbol_tab_bar.tabText(dvol_index), 'BTC-DVOL')
            window._symbol_tab_bar.setCurrentIndex(dvol_index)
            self.assertEqual(window._symbol_combo.currentText(), 'BTC-USDT-SWAP')
            self.assertEqual(window._secondary_chart_kind(), 'volatility')
            self.assertTrue(window._secondary_chart_check.isChecked())
            self.assertEqual(window._active_chart_target, 'secondary')
            window._load_secondary_data.assert_called_once_with(symbol='BTC-USDT-SWAP')
        finally:
            window.close()

    def test_filtered_history_acknowledges_only_displayed_events(self):
        with tempfile.TemporaryDirectory() as folder, patch.object(store, '_path', side_effect=lambda name: Path(folder) / name):
            first = dict(symbol='BTC-USDT-SWAP', period='1H', pattern_id='big_bullish', candle_ts=1000)
            second = dict(first, symbol='ETH-USDT-SWAP')
            store.append_events([first, second])
            dialog = ShapeSignalHistoryDialog()
            dialog._symbol_filter.setCurrentIndex(dialog._symbol_filter.findData(first['symbol']))
            dialog.events_viewed.connect(store.mark_events_read)
            dialog.refresh()
            saved = {event['symbol']: event for event in store.load_events()}
            self.assertTrue(saved[first['symbol']].get('read_at'))
            self.assertFalse(saved[second['symbol']].get('read_at'))
            self.assertEqual(dialog._table.rowCount(), 1)
            dialog.close()

    def test_read_snapshot_preserves_new_arrivals_and_deduplication(self):
        with tempfile.TemporaryDirectory() as folder, patch.object(store, '_path', side_effect=lambda name: Path(folder) / name):
            first = dict(symbol='BTC-USDT-SWAP', period='1H', pattern_id='big_bullish', candle_ts=1000)
            second = dict(first, candle_ts=2000)
            store.append_events([first])
            viewed = store.load_events()
            store.append_events([second])
            store.mark_events_read(viewed)
            saved = store.load_events()
            self.assertFalse(saved[0].get('read_at'))
            self.assertTrue(saved[1].get('read_at'))
            self.assertEqual(store.append_events([first]), [])
            self.assertTrue(store.load_events()[1].get('read_at'))

    def test_today_startup_event_is_queued_for_popup(self):
        timer = Mock()
        timer.isActive.return_value = False
        host = SimpleNamespace(
            _refresh_shape_message_badge=Mock(), _shape_message_dialog=None,
            _shape_startup_events=[], _shape_startup_popup_timer=timer,
            _shape_popup_display_events=[], _shape_popup_box=None,
        )
        event = dict(source='startup', candle_ts=int(datetime.now(timezone.utc).timestamp() * 1000), popup_enabled=True)
        LauncherWindow._on_shape_signal_detected(host, event)
        self.assertEqual(host._shape_startup_events, [event])
        host._shape_startup_popup_timer.start.assert_called_once_with(3000)
        host._refresh_shape_message_badge.assert_called_once()

    def test_live_events_are_queued_for_the_same_batch(self):
        timer = Mock()
        timer.isActive.return_value = False
        host = SimpleNamespace(
            _refresh_shape_message_badge=Mock(), _shape_message_dialog=None,
            _shape_startup_events=[], _shape_startup_popup_timer=timer,
            _shape_popup_display_events=[], _shape_popup_box=None,
        )
        LauncherWindow._on_shape_signal_detected(host, dict(source='live', popup_enabled=True, symbol='BTC-USDT-SWAP'))
        LauncherWindow._on_shape_signal_detected(host, dict(source='live', popup_enabled=True, symbol='ETH-USDT-SWAP'))
        self.assertEqual(len(host._shape_startup_events), 2)
        self.assertEqual(timer.start.call_count, 2)

    def test_late_signal_with_same_candle_time_updates_existing_popup(self):
        class PopupStub:
            def isVisible(self):
                return True

            def setText(self, value):
                self.text = value

        class BatchHost:
            _shape_popup_event_key = staticmethod(LauncherWindow._shape_popup_event_key)
            _shape_popup_event_time = staticmethod(LauncherWindow._shape_popup_event_time)
            _shape_popup_title = staticmethod(LauncherWindow._shape_popup_title)
            _shape_popup_text = LauncherWindow._shape_popup_text
            _update_shape_popup_box = LauncherWindow._update_shape_popup_box
            _shape_message_dialog = None

            def __init__(self):
                self._shape_startup_events = []
                self._shape_startup_popup_timer = Mock()
                self._shape_startup_popup_timer.isActive.return_value = False
                self._shape_popup_box = PopupStub()
                self._shape_popup_display_events = [
                    dict(symbol='SOL-BTC', period='1H', pattern_id='double_reversal_up',
                         direction='long', candle_ts=123000, pattern_name='双线向上反转', close='0.0012')
                ]
                self._refresh_shape_message_badge = Mock()

        host = BatchHost()
        LauncherWindow._on_shape_signal_detected(host, dict(
            symbol='SOL-USDT-SWAP', period='1H', pattern_id='double_reversal_up',
            direction='short', candle_ts=123000, pattern_name='双线向下反转', close='119.37',
            source='startup', popup_enabled=True,
        ))
        self.assertEqual(len(host._shape_popup_display_events), 2)
        self.assertIn('SOL-USDT-SWAP', host._shape_popup_box.text)
        host._shape_startup_popup_timer.start.assert_not_called()

    def test_chart_keeps_signal_when_selected_metric_rank_is_missing(self):
        class ChartFilterStub:
            _show_1h_shape_signal_check = Mock()
            _shape_signal_ma_touch_check = Mock()

            def _shape_signal_size_metric_value(self):
                return 'body'

            def _daily_trend_reference(self, *, period, is_secondary):
                return []

        ChartFilterStub._show_1h_shape_signal_check.isChecked.return_value = True
        ChartFilterStub._shape_signal_ma_touch_check.isChecked.return_value = False
        marker = {
            'pattern_id': 'double_reversal_up',
            'direction': 'long',
            'core_body_rank': None,
            'core_range_rank': 3,
            'core_amplitude_rank': 3,
            'core_ma_touch': True,
            'text': '双线向上反转 核心K前3 @MA50',
        }
        visible = KlineAnalysisWindow._filter_replay_signal_markers_for_chart(
            ChartFilterStub(), [marker], period='1H', is_secondary=False,
        )
        self.assertEqual(len(visible), 1)
        self.assertEqual(visible[0]['core_amplitude_rank'], 3)

    def test_disabled_popup_still_updates_unread_entry(self):
        host = SimpleNamespace(_refresh_shape_message_badge=Mock(), _shape_message_dialog=None)
        LauncherWindow._on_shape_signal_detected(host, dict(source='live', popup_enabled=False))
        host._refresh_shape_message_badge.assert_called_once()
