from decimal import Decimal
from unittest import TestCase

from okx_quant.candle_patterns import (
    analyze_single_candle_pattern_history,
    analyze_single_candle_patterns,
    single_candle_report_payload,
)
from okx_quant.models import Candle


def _candle(
    index: int,
    open_price: str,
    high_price: str,
    low_price: str,
    close_price: str,
) -> Candle:
    return Candle(
        ts=(index + 1) * 1000,
        open=Decimal(open_price),
        high=Decimal(high_price),
        low=Decimal(low_price),
        close=Decimal(close_price),
        volume=Decimal("1"),
        confirmed=True,
    )


class SingleCandlePatternAnalysisTest(TestCase):
    def test_detects_hammer_after_downtrend(self) -> None:
        candles = [
            _candle(0, "102", "103", "100", "101"),
            _candle(1, "101", "101.5", "98.5", "99.5"),
            _candle(2, "99.5", "100", "96.5", "97"),
            _candle(3, "97.0", "97.9", "92.5", "97.7"),
        ]

        report = analyze_single_candle_patterns(candles, inst_id="BTC-USDT-SWAP")

        self.assertEqual(report.trend_context, "downtrend")
        self.assertEqual(report.primary_pattern, "hammer")
        self.assertIn("hammer", [item.pattern for item in report.matches])

    def test_detects_shooting_star_after_uptrend(self) -> None:
        candles = [
            _candle(0, "95", "97", "94.5", "96"),
            _candle(1, "96", "98.5", "95.5", "97.8"),
            _candle(2, "97.8", "100.2", "97.6", "99.4"),
            _candle(3, "99.6", "105.6", "99.4", "100.4"),
        ]

        report = analyze_single_candle_patterns(candles)

        self.assertEqual(report.trend_context, "uptrend")
        self.assertEqual(report.primary_pattern, "shooting_star")
        self.assertIn("shooting_star", [item.pattern for item in report.matches])

    def test_detects_dragonfly_doji(self) -> None:
        candles = [
            _candle(0, "102", "103", "100", "101"),
            _candle(1, "101", "101.5", "98.5", "99.5"),
            _candle(2, "99.5", "100", "96.5", "97"),
            _candle(3, "97.05", "97.10", "93.20", "97.00"),
        ]

        report = analyze_single_candle_patterns(candles)
        patterns = [item.pattern for item in report.matches]

        self.assertEqual(report.primary_pattern, "dragonfly_doji")
        self.assertIn("dragonfly_doji", patterns)
        self.assertIn("doji", patterns)

    def test_detects_bullish_marubozu_and_serializes_as_json_ready_payload(self) -> None:
        candles = [
            _candle(0, "100", "100.2", "99.9", "100.1"),
            _candle(1, "100.2", "105.0", "100.1", "104.9"),
        ]

        report = analyze_single_candle_patterns(candles, inst_id="ETH-USDT-SWAP")
        payload = single_candle_report_payload(report)

        self.assertEqual(report.primary_pattern, "bullish_marubozu")
        self.assertEqual(payload["inst_id"], "ETH-USDT-SWAP")
        self.assertEqual(payload["primary_pattern"], "bullish_marubozu")
        self.assertEqual(payload["candle"]["open"], "100.2")
        self.assertEqual(payload["metrics"]["body_ratio"], "0.9591836734693877551020408163")

    def test_history_returns_only_candles_with_matches(self) -> None:
        candles = [
            _candle(0, "100", "101", "99", "100.5"),
            _candle(1, "100.5", "101", "98", "98.5"),
            _candle(2, "98.5", "99", "96", "96.8"),
            _candle(3, "96.8", "97.6", "92.8", "97.4"),
            _candle(4, "97.4", "99.2", "97.1", "98.8"),
            _candle(5, "98.8", "100.5", "98.6", "100.2"),
            _candle(6, "100.2", "102.0", "100.0", "101.8"),
            _candle(7, "101.9", "107.0", "101.7", "102.6"),
        ]

        history = analyze_single_candle_pattern_history(candles)
        primary_patterns = [report.primary_pattern for report in history]

        self.assertGreaterEqual(len(history), 2)
        self.assertIn("hammer", primary_patterns)
        self.assertIn("shooting_star", primary_patterns)
