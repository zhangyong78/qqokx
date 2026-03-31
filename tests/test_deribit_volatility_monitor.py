from decimal import Decimal
from unittest import TestCase

from okx_quant.deribit_client import DeribitVolatilityCandle
from okx_quant.deribit_volatility_monitor import (
    DERIBIT_VOL_RESOLUTION_SECONDS,
    VolatilityMonitorConfig,
    VolatilityMonitorRoundDiagnostic,
    VolatilityMonitorSymbolDiagnostic,
    detect_bearish_reversal_after_rally,
    detect_bullish_reversal_after_drop,
    detect_ema34_turn_down,
    detect_ema34_turn_up,
    detect_squeeze_breakout_down,
    detect_squeeze_breakout_up,
    evaluate_volatility_signal_report,
    format_volatility_diagnostic_round,
)


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
