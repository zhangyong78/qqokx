from __future__ import annotations

import json
import shutil
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch
from uuid import uuid4
from zoneinfo import ZoneInfo

from okx_quant.btc_research_workbench_ui import (
    _aggregate_deribit_candles,
    _chart_hover_index_for_x,
    _default_chart_viewport,
    _align_overlay_candles,
    _build_realized_volatility_from_reference,
    _deribit_volatility_bucket_start_ms,
    _format_short_ts,
    _load_historical_analysis_markers,
    _pan_chart_viewport,
    _slot_timestamp,
)
from okx_quant.deribit_client import DeribitVolatilityCandle
from okx_quant.models import Candle


class BtcResearchWorkbenchHelpersTest(TestCase):
    def test_aggregate_deribit_volatility_aligns_4h_to_beijing_bucket(self) -> None:
        cn = ZoneInfo("Asia/Shanghai")
        t0 = int(datetime(2024, 6, 1, 16, 0, tzinfo=cn).astimezone(timezone.utc).timestamp() * 1000)
        t1 = int(datetime(2024, 6, 1, 17, 0, tzinfo=cn).astimezone(timezone.utc).timestamp() * 1000)
        hourly = [
            DeribitVolatilityCandle(ts=t0, open=Decimal("50"), high=Decimal("51"), low=Decimal("49"), close=Decimal("50.5")),
            DeribitVolatilityCandle(ts=t1, open=Decimal("50.5"), high=Decimal("52"), low=Decimal("50"), close=Decimal("51.5")),
        ]
        out = _aggregate_deribit_candles(hourly, 14_400_000)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0].ts, _deribit_volatility_bucket_start_ms(t0, 14_400_000))
        self.assertEqual(out[0].open, Decimal("50"))
        self.assertEqual(out[0].close, Decimal("51.5"))

    def test_format_short_ts_uses_beijing_wall_clock(self) -> None:
        cn = ZoneInfo("Asia/Shanghai")
        dt = datetime(2024, 6, 1, 8, 30, tzinfo=cn)
        ts_ms = int(dt.astimezone(timezone.utc).timestamp() * 1000)
        self.assertIn("06-01", _format_short_ts(ts_ms))
        self.assertIn("08:30", _format_short_ts(ts_ms))

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

    def test_viewport_matches_backtest_chart_windowing(self) -> None:
        start_index, visible_count = _default_chart_viewport(
            519,
            220,
            min_visible=36,
        )

        self.assertEqual(visible_count, 220)
        self.assertEqual(start_index, 299)

        moved_start = _pan_chart_viewport(
            start_index,
            visible_count,
            519,
            6,
            min_visible=36,
        )

        self.assertEqual(moved_start, 299)

    def test_slot_timestamp_extends_future_axis_from_last_candle(self) -> None:
        candles = [
            Candle(
                ts=1_700_000_000_000,
                open=Decimal("1"),
                high=Decimal("1"),
                low=Decimal("1"),
                close=Decimal("1"),
                volume=Decimal("0"),
                confirmed=True,
            ),
            Candle(
                ts=1_700_014_400_000,
                open=Decimal("1"),
                high=Decimal("1"),
                low=Decimal("1"),
                close=Decimal("1"),
                volume=Decimal("0"),
                confirmed=True,
            ),
        ]

        future_ts = _slot_timestamp(candles, 4, 14_400_000)

        self.assertEqual(future_ts, 1_700_057_600_000)

    def test_chart_hover_index_for_x_snaps_to_nearest_candle_center(self) -> None:
        self.assertEqual(
            _chart_hover_index_for_x(
                x=75.0,
                left=50,
                width=200,
                start_index=10,
                end_index=20,
                candle_step=20.0,
            ),
            11,
        )
        self.assertEqual(
            _chart_hover_index_for_x(
                x=49.0,
                left=50,
                width=200,
                start_index=10,
                end_index=20,
                candle_step=20.0,
            ),
            None,
        )
