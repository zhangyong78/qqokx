import json
import shutil
from decimal import Decimal
from pathlib import Path
from unittest import TestCase
from uuid import uuid4

from okx_quant.market_analysis import (
    MarketAnalysisConfig,
    build_market_analysis_report,
    market_analysis_report_payload,
    save_market_analysis_report,
)
from okx_quant.models import Candle


def _candle(index: int, open_price: Decimal, close_price: Decimal) -> Candle:
    high = max(open_price, close_price) + Decimal("0.2")
    low = min(open_price, close_price) - Decimal("0.2")
    return Candle(
        ts=(index + 1) * 1000,
        open=open_price,
        high=high,
        low=low,
        close=close_price,
        volume=Decimal("1"),
        confirmed=True,
    )


def _candles_from_closes(closes: list[str]) -> list[Candle]:
    decimals = [Decimal(value) for value in closes]
    candles: list[Candle] = []
    for index, close_price in enumerate(decimals):
        if index == 0:
            open_price = close_price - Decimal("0.2")
        else:
            open_price = decimals[index - 1]
        candles.append(_candle(index, open_price, close_price))
    return candles


class MarketAnalysisTest(TestCase):
    def _workspace_temp_dir(self) -> Path:
        temp_dir = Path("tests_artifacts") / uuid4().hex
        temp_dir.mkdir(parents=True, exist_ok=True)
        self.addCleanup(lambda: shutil.rmtree(temp_dir, ignore_errors=True))
        return temp_dir

    def test_streak_continuation_stats_follow_exact_sequence(self) -> None:
        candles = _candles_from_closes(["100", "101", "102", "103", "102"])

        report = build_market_analysis_report(candles, timeframe="1D")
        streak_by_label = {item.streak_label: item for item in report.streak_stats}

        self.assertEqual(streak_by_label["1"].sample_count, 1)
        self.assertEqual(streak_by_label["1"].continuation_probability, Decimal("1"))
        self.assertEqual(streak_by_label["2"].sample_count, 1)
        self.assertEqual(streak_by_label["2"].continuation_probability, Decimal("1"))
        self.assertEqual(streak_by_label["3"].sample_count, 1)
        self.assertEqual(streak_by_label["3"].continuation_probability, Decimal("0"))
        self.assertEqual(report.direction_mode, "close_to_close")

    def test_low_volatility_peak_streak_becomes_active_factor(self) -> None:
        closes = [
            "100", "110", "99", "111", "98", "112", "97", "113", "96", "114",
            "95", "115", "94", "116", "93", "117", "92", "118", "91", "119", "90",
            "90.00", "90.02", "90.04", "90.06", "90.08", "90.10", "90.12", "90.14", "90.16",
            "90.18", "90.20", "90.22", "90.24", "90.24", "90.26", "90.28", "90.30", "90.32", "90.34",
            "90.36",
        ]
        candles = _candles_from_closes(closes)
        config = MarketAnalysisConfig(factor_min_samples=1, factor_alert_min_samples=1)

        report = build_market_analysis_report(
            candles,
            inst_id="BTC-USDT-SWAP",
            timeframe="1D",
            config=config,
        )

        active_keys = {item.key for item in report.active_factors}
        candidate_by_key = {item.key: item for item in report.factor_candidates}

        self.assertEqual(report.snapshot.current_bullish_streak, 6)
        self.assertEqual(report.snapshot.latest_volatility_regime, "low")
        self.assertIn("streak_momentum_peak_low_volatility", active_keys)
        self.assertTrue(candidate_by_key["streak_momentum_peak_5_6"].adopt)

    def test_latest_large_bearish_breakdown_becomes_bearish_factor(self) -> None:
        closes = [
            "100.0", "100.4", "100.8", "101.2", "101.6", "102.0", "102.4", "102.8", "103.2", "103.6",
            "104.0", "104.4", "104.8", "105.2", "105.6", "106.0", "106.4", "106.8", "107.2", "107.2",
            "107.6", "108.0", "108.4", "108.8", "95.0",
        ]
        candles = _candles_from_closes(closes)

        report = build_market_analysis_report(candles, timeframe="1D")
        active_keys = {item.key for item in report.active_factors}

        self.assertEqual(report.snapshot.last_completed_bullish_streak, 4)
        self.assertEqual(report.snapshot.latest_pullback_bucket, "large")
        self.assertTrue(report.snapshot.latest_support_break)
        self.assertIn("post_streak_breakdown", active_keys)

    def test_report_payload_and_save_are_json_ready(self) -> None:
        closes = ["100", "101", "102", "103", "102", "101", "102", "103"]
        candles = _candles_from_closes(closes)
        report = build_market_analysis_report(
            candles,
            inst_id="BTC-USDT-SWAP",
            timeframe="1D",
            config=MarketAnalysisConfig(factor_min_samples=1, factor_alert_min_samples=1),
        )

        payload = market_analysis_report_payload(report)
        temp_dir = self._workspace_temp_dir()
        output_path = temp_dir / "analysis.json"
        saved_path = save_market_analysis_report(report, path=output_path)
        persisted = json.loads(saved_path.read_text(encoding="utf-8"))

        self.assertEqual(payload["inst_id"], "BTC-USDT-SWAP")
        self.assertEqual(payload["timeframe"], "1D")
        self.assertEqual(payload["direction_mode"], "close_to_close")
        self.assertEqual(saved_path, output_path)
        self.assertEqual(persisted["inst_id"], "BTC-USDT-SWAP")
        self.assertEqual(persisted["snapshot"]["as_of_ts"], report.snapshot.as_of_ts)

    def test_candle_body_mode_remains_available(self) -> None:
        candles = [
            _candle(0, Decimal("100"), Decimal("101")),
            _candle(1, Decimal("102"), Decimal("101.5")),
            _candle(2, Decimal("101.0"), Decimal("101.2")),
        ]
        report = build_market_analysis_report(
            candles,
            timeframe="1D",
            config=MarketAnalysisConfig(direction_mode="candle_body", factor_min_samples=1, factor_alert_min_samples=1),
        )

        self.assertEqual(report.direction_mode, "candle_body")
        self.assertEqual(report.snapshot.current_bullish_streak, 1)
        self.assertIsNotNone(report.baseline_bullish_probability)
