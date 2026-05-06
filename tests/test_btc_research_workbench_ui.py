from __future__ import annotations

import json
import shutil
from decimal import Decimal
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch
from uuid import uuid4

from okx_quant.btc_research_workbench_ui import (
    _align_overlay_candles,
    _build_realized_volatility_from_reference,
    _load_historical_analysis_markers,
)
from okx_quant.models import Candle


class BtcResearchWorkbenchHelpersTest(TestCase):
    def _workspace_temp_dir(self) -> Path:
        temp_dir = Path("tests_artifacts") / uuid4().hex
        temp_dir.mkdir(parents=True, exist_ok=True)
        self.addCleanup(lambda: shutil.rmtree(temp_dir, ignore_errors=True))
        return temp_dir

    def test_align_overlay_candles_uses_daily_time_bucket(self) -> None:
        price_candles = [
            Candle(
                ts=1_700_000_000_000,
                open=Decimal("62000"),
                high=Decimal("62600"),
                low=Decimal("61800"),
                close=Decimal("62400"),
                volume=Decimal("100"),
                confirmed=True,
            ),
            Candle(
                ts=1_700_086_400_000,
                open=Decimal("62400"),
                high=Decimal("63100"),
                low=Decimal("62200"),
                close=Decimal("62900"),
                volume=Decimal("120"),
                confirmed=True,
            ),
        ]
        volatility_candles = [
            Candle(
                ts=1_700_000_600_000,
                open=Decimal("42.1"),
                high=Decimal("42.8"),
                low=Decimal("41.7"),
                close=Decimal("42.4"),
                volume=Decimal("0"),
                confirmed=True,
            ),
            Candle(
                ts=1_700_087_000_000,
                open=Decimal("43.0"),
                high=Decimal("43.4"),
                low=Decimal("42.6"),
                close=Decimal("43.1"),
                volume=Decimal("0"),
                confirmed=True,
            ),
        ]

        aligned = _align_overlay_candles(price_candles, volatility_candles, bar="1D")

        self.assertEqual(len(aligned), 2)
        self.assertEqual(aligned[0][0].ts, price_candles[0].ts)
        self.assertEqual(aligned[0][1].ts, volatility_candles[0].ts)
        self.assertEqual(aligned[1][0].ts, price_candles[1].ts)
        self.assertEqual(aligned[1][1].ts, volatility_candles[1].ts)

    def test_build_realized_volatility_from_reference_returns_series(self) -> None:
        reference = [
            Candle(
                ts=1_700_000_000_000 + index * 3_600_000,
                open=Decimal(str(62000 + index * 100)),
                high=Decimal(str(62100 + index * 100)),
                low=Decimal(str(61900 + index * 100)),
                close=Decimal(str(62050 + index * 100)),
                volume=Decimal("10"),
                confirmed=True,
            )
            for index in range(30)
        ]

        series = _build_realized_volatility_from_reference(reference, bar="1H", lookback=20)

        self.assertGreater(len(series), 0)
        self.assertTrue(all(item.confirmed for item in series))
        self.assertTrue(all(item.close > 0 for item in series))

    def test_load_historical_analysis_markers_filters_symbol_and_timeframe(self) -> None:
        temp_dir = self._workspace_temp_dir()
        report_dir = temp_dir / "reports" / "analysis"
        report_dir.mkdir(parents=True, exist_ok=True)
        (report_dir / "btc_report.json").write_text(
            json.dumps(
                {
                    "symbol": "BTC-USDT-SWAP",
                    "generated_at": "2026-05-06T06:00:00Z",
                    "timeframes": [
                        {
                            "timeframe": "4H",
                            "candle_ts": 1_700_000_000_000,
                            "direction": "long",
                            "score": 74,
                            "confidence": "68%",
                        },
                        {
                            "timeframe": "1D",
                            "candle_ts": 1_700_086_400_000,
                            "direction": "short",
                            "score": 28,
                            "confidence": "42%",
                        },
                    ],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (report_dir / "eth_report.json").write_text(
            json.dumps(
                {
                    "symbol": "ETH-USDT-SWAP",
                    "generated_at": "2026-05-06T06:00:00Z",
                    "timeframes": [
                        {
                            "timeframe": "4H",
                            "candle_ts": 1_700_000_000_000,
                            "direction": "long",
                            "score": 50,
                            "confidence": "50%",
                        }
                    ],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        with patch("okx_quant.btc_research_workbench_ui.analysis_report_dir_path", return_value=report_dir):
            markers = _load_historical_analysis_markers("BTC-USDT-SWAP", "4H")

        self.assertEqual(len(markers), 1)
        self.assertEqual(markers[0].timeframe, "4H")
        self.assertEqual(markers[0].direction, "long")
        self.assertEqual(markers[0].score, 74)
