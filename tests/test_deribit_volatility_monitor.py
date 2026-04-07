from decimal import Decimal
from tkinter import BooleanVar, StringVar, Tcl
from unittest import TestCase

from okx_quant.deribit_client import DeribitVolatilityCandle
from okx_quant.deribit_volatility_monitor import (
    DERIBIT_VOL_RESOLUTION_SECONDS,
    VolatilityMonitorConfig,
    VolatilityMonitorRoundDiagnostic,
    VolatilityMonitorSymbolDiagnostic,
    detect_bearish_reversal_after_rally,
    detect_box_breakout_down,
    detect_box_breakout_up,
    detect_bullish_reversal_after_drop,
    detect_ema34_turn_down,
    detect_ema34_turn_up,
    detect_squeeze_breakout_down,
    detect_squeeze_breakout_up,
    evaluate_volatility_signal_history,
    evaluate_volatility_signal_report,
    format_volatility_diagnostic_round,
)
from okx_quant.deribit_volatility_monitor_ui import DeribitVolatilityMonitorWindow, VolatilityMonitorDefaults


def _candle(
    index: int,
    open_price: str | Decimal,
    high_price: str | Decimal,
    low_price: str | Decimal,
    close_price: str | Decimal,
) -> DeribitVolatilityCandle:
    return DeribitVolatilityCandle(
        ts=(index + 1) * 1000,
        open=Decimal(str(open_price)),
        high=Decimal(str(high_price)),
        low=Decimal(str(low_price)),
        close=Decimal(str(close_price)),
    )


def _make_volatility_monitor_window_stub() -> DeribitVolatilityMonitorWindow:
    interp = Tcl()
    window = DeribitVolatilityMonitorWindow.__new__(DeribitVolatilityMonitorWindow)
    window._defaults = VolatilityMonitorDefaults()
    window.resolution_label = StringVar(master=interp, value=window._defaults.resolution_label)
    window.buffer_seconds = StringVar(master=interp, value=window._defaults.buffer_seconds)
    window.ema_period = StringVar(master=interp, value=window._defaults.ema_period)
    window.trend_streak_bars = StringVar(master=interp, value=window._defaults.trend_streak_bars)
    window.squeeze_bars = StringVar(master=interp, value=window._defaults.squeeze_bars)
    window.lookback_candles = StringVar(master=interp, value=window._defaults.lookback_candles)
    window.cumulative_change_threshold = StringVar(master=interp, value=window._defaults.cumulative_change_threshold)
    window.reversal_body_multiplier = StringVar(master=interp, value=window._defaults.reversal_body_multiplier)
    window.breakout_body_multiplier = StringVar(master=interp, value=window._defaults.breakout_body_multiplier)
    window.squeeze_range_ratio = StringVar(master=interp, value=window._defaults.squeeze_range_ratio)
    window.signal_chart_candle_limit = StringVar(master=interp, value="1000")
    window.signal_preview_currency = StringVar(master=interp, value="BTC")
    window.enable_btc = BooleanVar(master=interp, value=True)
    window.enable_eth = BooleanVar(master=interp, value=False)
    return window


class DeribitVolatilityMonitorSignalsTest(TestCase):
    def test_detect_bearish_reversal_after_rally(self) -> None:
        config = VolatilityMonitorConfig(currencies=("BTC",), trend_streak_bars=4)
        candles = [
            _candle(0, "90", "91", "89", "90"),
            _candle(1, "91", "92", "90", "91"),
            _candle(2, "100", "102", "99", "101"),
            _candle(3, "103", "105", "102", "104"),
            _candle(4, "106", "108", "105", "107"),
            _candle(5, "109", "111", "108", "110"),
            _candle(6, "113", "114", "103", "104"),
        ]

        event = detect_bearish_reversal_after_rally("BTC", candles, config)

        self.assertIsNotNone(event)
        assert event is not None
        self.assertEqual(event.signal_type, "bearish_reversal_after_rally")
        self.assertEqual(event.direction, "down")
        self.assertEqual(event.trigger_value, Decimal("104"))

    def test_detect_bullish_reversal_after_drop(self) -> None:
        config = VolatilityMonitorConfig(currencies=("BTC",), trend_streak_bars=4)
        candles = [
            _candle(0, "90", "91", "89", "90"),
            _candle(1, "91", "92", "90", "91"),
            _candle(2, "110", "111", "108", "109"),
            _candle(3, "107", "108", "105", "106"),
            _candle(4, "104", "105", "102", "103"),
            _candle(5, "101", "102", "99", "100"),
            _candle(6, "97", "108", "96", "107"),
        ]

        event = detect_bullish_reversal_after_drop("BTC", candles, config)

        self.assertIsNotNone(event)
        assert event is not None
        self.assertEqual(event.signal_type, "bullish_reversal_after_drop")
        self.assertEqual(event.direction, "up")
        self.assertEqual(event.trigger_value, Decimal("107"))

    def test_detect_squeeze_breakout_up(self) -> None:
        config = VolatilityMonitorConfig(currencies=("BTC",), squeeze_bars=6)
        candles: list[DeribitVolatilityCandle] = []
        for index in range(24):
            base = Decimal("50") + Decimal(index % 3)
            candles.append(_candle(index, base, base + Decimal("3"), base - Decimal("3"), base + Decimal("1")))
        for index in range(24, 30):
            candles.append(_candle(index, "52", "52.4", "51.6", "52.1"))
        candles.append(_candle(30, "52.2", "57.5", "52", "57"))

        event = detect_squeeze_breakout_up("BTC", candles, config)

        self.assertIsNotNone(event)
        assert event is not None
        self.assertEqual(event.signal_type, "squeeze_breakout_up")
        self.assertEqual(event.direction, "up")
        self.assertEqual(event.trigger_value, Decimal("57"))

    def test_detect_squeeze_breakout_down(self) -> None:
        config = VolatilityMonitorConfig(currencies=("BTC",), squeeze_bars=6)
        candles: list[DeribitVolatilityCandle] = []
        for index in range(24):
            base = Decimal("50") + Decimal(index % 3)
            candles.append(_candle(index, base, base + Decimal("3"), base - Decimal("3"), base + Decimal("1")))
        for index in range(24, 30):
            candles.append(_candle(index, "52", "52.4", "51.6", "52.1"))
        candles.append(_candle(30, "52.2", "52.3", "46.5", "47"))

        event = detect_squeeze_breakout_down("BTC", candles, config)

        self.assertIsNotNone(event)
        assert event is not None
        self.assertEqual(event.signal_type, "squeeze_breakout_down")
        self.assertEqual(event.direction, "down")
        self.assertEqual(event.trigger_value, Decimal("47"))

    def test_detect_box_breakout_up(self) -> None:
        config = VolatilityMonitorConfig(currencies=("BTC",), squeeze_bars=6)
        candles: list[DeribitVolatilityCandle] = []
        for index in range(20):
            base = Decimal("50") + Decimal(index % 2)
            candles.append(_candle(index, base, base + Decimal("2.6"), base - Decimal("2.4"), base + Decimal("0.5")))
        for index in range(20, 30):
            open_price = Decimal("52.0") + Decimal(index % 3) * Decimal("0.2")
            candles.append(_candle(index, open_price, Decimal("54.2"), Decimal("51.2"), Decimal("52.8")))
        candles.append(_candle(30, "53.0", "56.8", "52.8", "56.3"))

        event = detect_box_breakout_up("BTC", candles, config)

        self.assertIsNotNone(event)
        assert event is not None
        self.assertEqual(event.signal_type, "box_breakout_up")
        self.assertEqual(event.direction, "up")
        self.assertIn("箱体高点", event.reason)

    def test_detect_box_breakout_down(self) -> None:
        config = VolatilityMonitorConfig(currencies=("BTC",), squeeze_bars=6)
        candles: list[DeribitVolatilityCandle] = []
        for index in range(20):
            base = Decimal("50") + Decimal(index % 2)
            candles.append(_candle(index, base, base + Decimal("2.6"), base - Decimal("2.4"), base + Decimal("0.5")))
        for index in range(20, 30):
            open_price = Decimal("52.0") + Decimal(index % 3) * Decimal("0.2")
            candles.append(_candle(index, open_price, Decimal("54.2"), Decimal("51.2"), Decimal("52.6")))
        candles.append(_candle(30, "52.5", "52.7", "48.4", "48.9"))

        event = detect_box_breakout_down("BTC", candles, config)

        self.assertIsNotNone(event)
        assert event is not None
        self.assertEqual(event.signal_type, "box_breakout_down")
        self.assertEqual(event.direction, "down")
        self.assertIn("箱体低点", event.reason)

    def test_detect_box_breakout_up_by_valid_close_breakout(self) -> None:
        config = VolatilityMonitorConfig(currencies=("BTC",), squeeze_bars=6)
        candles: list[DeribitVolatilityCandle] = []
        for index in range(20):
            base = Decimal("50.0") + Decimal(index % 2) * Decimal("0.4")
            candles.append(_candle(index, base, base + Decimal("1.8"), base - Decimal("1.6"), base + Decimal("0.4")))
        box_rows = [
            ("52.0", "52.5", "51.7", "52.2"),
            ("52.1", "52.4", "51.8", "52.0"),
            ("52.0", "52.6", "51.9", "52.3"),
            ("52.2", "52.5", "51.9", "52.1"),
            ("52.1", "52.4", "51.8", "52.0"),
            ("52.0", "52.5", "51.8", "52.2"),
            ("52.1", "52.4", "51.9", "52.1"),
            ("52.0", "52.5", "51.9", "52.2"),
            ("52.2", "52.5", "51.8", "52.0"),
            ("52.1", "52.4", "51.9", "52.1"),
        ]
        for offset, row in enumerate(box_rows, start=20):
            candles.append(_candle(offset, *row))
        candles.append(_candle(30, "52.35", "52.90", "52.10", "52.78"))

        event = detect_box_breakout_up("BTC", candles, config)

        self.assertIsNotNone(event)
        assert event is not None
        self.assertEqual(event.signal_type, "box_breakout_up")
        self.assertEqual(event.direction, "up")
        self.assertEqual(event.trigger_value, Decimal("52.78"))

    def test_detect_box_breakout_up_by_body_box_breakout(self) -> None:
        config = VolatilityMonitorConfig(currencies=("BTC",), squeeze_bars=6)
        candles: list[DeribitVolatilityCandle] = []
        for index in range(18):
            base = Decimal("50.0") + Decimal(index % 2) * Decimal("0.3")
            candles.append(_candle(index, base, base + Decimal("1.2"), base - Decimal("1.1"), base + Decimal("0.2")))
        box_rows = [
            ("52.00", "52.40", "51.80", "52.10"),
            ("52.10", "52.50", "51.90", "52.00"),
            ("52.00", "52.45", "51.85", "52.05"),
            ("52.05", "52.50", "51.90", "52.10"),
            ("52.10", "52.55", "51.95", "52.15"),
            ("52.15", "53.80", "52.00", "52.20"),
            ("52.10", "52.50", "51.95", "52.15"),
            ("52.05", "52.45", "51.90", "52.10"),
            ("52.00", "52.40", "51.85", "52.05"),
            ("52.10", "52.55", "51.95", "52.20"),
            ("52.15", "52.60", "51.95", "52.25"),
            ("52.20", "52.65", "52.00", "52.30"),
        ]
        for offset, row in enumerate(box_rows, start=18):
            candles.append(_candle(offset, *row))
        candles.append(_candle(30, "52.28", "52.85", "52.12", "52.68"))

        event = detect_box_breakout_up("BTC", candles, config)

        self.assertIsNotNone(event)
        assert event is not None
        self.assertEqual(event.signal_type, "box_breakout_up")
        self.assertEqual(event.direction, "up")
        self.assertEqual(event.trigger_value, Decimal("52.68"))

    def test_detect_box_breakout_down_by_valid_close_breakout(self) -> None:
        config = VolatilityMonitorConfig(currencies=("BTC",), squeeze_bars=6)
        candles: list[DeribitVolatilityCandle] = []
        for index in range(20):
            base = Decimal("50.0") + Decimal(index % 2) * Decimal("0.4")
            candles.append(_candle(index, base, base + Decimal("1.8"), base - Decimal("1.6"), base + Decimal("0.4")))
        box_rows = [
            ("52.0", "52.5", "51.7", "52.2"),
            ("52.1", "52.4", "51.8", "52.0"),
            ("52.0", "52.6", "51.9", "52.3"),
            ("52.2", "52.5", "51.9", "52.1"),
            ("52.1", "52.4", "51.8", "52.0"),
            ("52.0", "52.5", "51.8", "52.2"),
            ("52.1", "52.4", "51.9", "52.1"),
            ("52.0", "52.5", "51.9", "52.2"),
            ("52.2", "52.5", "51.8", "52.0"),
            ("52.1", "52.4", "51.9", "52.1"),
        ]
        for offset, row in enumerate(box_rows, start=20):
            candles.append(_candle(offset, *row))
        candles.append(_candle(30, "51.95", "52.10", "51.35", "51.44"))

        event = detect_box_breakout_down("BTC", candles, config)

        self.assertIsNotNone(event)
        assert event is not None
        self.assertEqual(event.signal_type, "box_breakout_down")
        self.assertEqual(event.direction, "down")
        self.assertEqual(event.trigger_value, Decimal("51.44"))

    def test_detect_box_breakout_down_by_body_box_breakout(self) -> None:
        config = VolatilityMonitorConfig(currencies=("BTC",), squeeze_bars=6)
        candles: list[DeribitVolatilityCandle] = []
        for index in range(18):
            base = Decimal("50.0") + Decimal(index % 2) * Decimal("0.3")
            candles.append(_candle(index, base, base + Decimal("1.2"), base - Decimal("1.1"), base + Decimal("0.2")))
        box_rows = [
            ("52.40", "52.65", "52.00", "52.30"),
            ("52.35", "52.60", "52.05", "52.25"),
            ("52.30", "52.55", "52.00", "52.20"),
            ("52.25", "52.50", "51.95", "52.15"),
            ("52.20", "52.45", "51.90", "52.10"),
            ("52.15", "52.35", "50.70", "52.05"),
            ("52.20", "52.40", "51.95", "52.10"),
            ("52.25", "52.45", "52.00", "52.15"),
            ("52.30", "52.50", "52.05", "52.20"),
            ("52.35", "52.55", "52.10", "52.25"),
            ("52.30", "52.50", "52.05", "52.20"),
            ("52.25", "52.45", "52.00", "52.15"),
        ]
        for offset, row in enumerate(box_rows, start=18):
            candles.append(_candle(offset, *row))
        candles.append(_candle(30, "52.10", "52.18", "51.35", "51.52"))

        event = detect_box_breakout_down("BTC", candles, config)

        self.assertIsNotNone(event)
        assert event is not None
        self.assertEqual(event.signal_type, "box_breakout_down")
        self.assertEqual(event.direction, "down")
        self.assertEqual(event.trigger_value, Decimal("51.52"))

    def test_detect_ema34_turn_up(self) -> None:
        config = VolatilityMonitorConfig(currencies=("BTC",), ema_period=34)
        closes = [100] * 30 + [95, 90, 85, 80, 75, 70, 65, 60, 80, 100]
        candles = [_candle(index, value, Decimal(str(value)) + Decimal("0.5"), Decimal(str(value)) - Decimal("0.5"), value) for index, value in enumerate(closes)]

        event = detect_ema34_turn_up("BTC", candles, config)

        self.assertIsNotNone(event)
        assert event is not None
        self.assertEqual(event.signal_type, "ema34_turn_up")
        self.assertEqual(event.direction, "up")
        self.assertEqual(event.trigger_value, Decimal("100"))

    def test_detect_ema34_turn_down(self) -> None:
        config = VolatilityMonitorConfig(currencies=("BTC",), ema_period=34)
        closes = [50, 52, 54, 56, 58, 60, 62, 64, 66, 68, 70, 72, 74, 76, 78, 80, 82, 84, 86, 88, 90, 92, 94, 96, 98, 100, 102, 104, 106, 108, 110, 112, 114, 116, 118, 120, 122, 124, 110, 90]
        candles = [_candle(index, value, Decimal(str(value)) + Decimal("0.5"), Decimal(str(value)) - Decimal("0.5"), value) for index, value in enumerate(closes)]

        event = detect_ema34_turn_down("BTC", candles, config)

        self.assertIsNotNone(event)
        assert event is not None
        self.assertEqual(event.signal_type, "ema34_turn_down")
        self.assertEqual(event.direction, "down")
        self.assertEqual(event.trigger_value, Decimal("90"))

    def test_signal_report_respects_enabled_switches(self) -> None:
        config = VolatilityMonitorConfig(
            currencies=("BTC",),
            trend_streak_bars=4,
            enable_bearish_reversal_after_rally=False,
        )
        candles = [
            _candle(0, "90", "91", "89", "90"),
            _candle(1, "91", "92", "90", "91"),
            _candle(2, "100", "102", "99", "101"),
            _candle(3, "103", "105", "102", "104"),
            _candle(4, "106", "108", "105", "107"),
            _candle(5, "109", "111", "108", "110"),
            _candle(6, "113", "114", "103", "104"),
        ]

        report = evaluate_volatility_signal_report(candles, "BTC", config)

        self.assertEqual(len(report.matched_events), 0)
        self.assertEqual(len(report.filtered_events), 1)
        self.assertEqual(report.filtered_events[0].signal_type, "bearish_reversal_after_rally")

    def test_signal_history_replays_matching_bearish_reversal(self) -> None:
        config = VolatilityMonitorConfig(currencies=("BTC",), trend_streak_bars=4)
        candles = [
            _candle(0, "90", "91", "89", "90"),
            _candle(1, "91", "92", "90", "91"),
            _candle(2, "100", "102", "99", "101"),
            _candle(3, "103", "105", "102", "104"),
            _candle(4, "106", "108", "105", "107"),
            _candle(5, "109", "111", "108", "110"),
            _candle(6, "113", "114", "103", "104"),
        ]

        events = evaluate_volatility_signal_history(
            candles,
            "BTC",
            config,
            signal_type="bearish_reversal_after_rally",
        )

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].signal_type, "bearish_reversal_after_rally")
        self.assertEqual(events[0].direction, "down")
        self.assertEqual(events[0].candle_ts, candles[-1].ts)

    def test_format_diagnostic_round_shows_new_filtered_and_errors(self) -> None:
        config = VolatilityMonitorConfig(currencies=("BTC",))
        up_event = detect_ema34_turn_up(
            "BTC",
            [_candle(index, value, Decimal(str(value)) + Decimal("0.5"), Decimal(str(value)) - Decimal("0.5"), value)
             for index, value in enumerate([100] * 30 + [95, 90, 85, 80, 75, 70, 65, 60, 80, 100])],
            config,
        )
        assert up_event is not None
        report = VolatilityMonitorRoundDiagnostic(
            resolution="3600",
            checked_at=1_710_000_000_000,
            reports=(
                VolatilityMonitorSymbolDiagnostic(
                    currency="BTC",
                    candle_ts=1_710_000_000_000,
                    new_events=(up_event,),
                    filtered_events=(up_event,),
                ),
                VolatilityMonitorSymbolDiagnostic(
                    currency="ETH",
                    candle_ts=None,
                    error="network error",
                ),
            ),
        )

        text = format_volatility_diagnostic_round("V01", report)

        self.assertIn("V01", text)
        self.assertIn("ETH DVOL", text)
        self.assertIn("EMA34转强", text)
        self.assertIn("新触发", text)
        self.assertIn("已过滤", text)
        self.assertIn("失败", text)

    def test_resolution_seconds_mapping_contains_hourly(self) -> None:
        self.assertEqual(DERIBIT_VOL_RESOLUTION_SECONDS["3600"], 3600)


class DeribitVolatilityMonitorUiTest(TestCase):
    def test_signal_preview_request_uses_selected_signal_and_candle_limit(self) -> None:
        window = _make_volatility_monitor_window_stub()
        window.signal_preview_currency.set("ETH")
        window.signal_chart_candle_limit.set("1200")

        request = window._build_signal_preview_request("ema34_turn_down")

        self.assertEqual(request.mode, "preview")
        self.assertEqual(request.currency, "ETH")
        self.assertEqual(request.candle_limit, 1200)
        self.assertEqual(request.config.currencies, ("ETH",))
        self.assertFalse(request.config.enable_bearish_reversal_after_rally)
        self.assertFalse(request.config.enable_bullish_reversal_after_drop)
        self.assertFalse(request.config.enable_ema34_turn_up)
        self.assertTrue(request.config.enable_ema34_turn_down)

    def test_collect_configured_currencies_returns_checked_items(self) -> None:
        window = _make_volatility_monitor_window_stub()

        self.assertEqual(window._collect_configured_currencies(), ["BTC"])

        window.enable_btc.set(False)
        window.enable_eth.set(True)
        self.assertEqual(window._collect_configured_currencies(), ["ETH"])
