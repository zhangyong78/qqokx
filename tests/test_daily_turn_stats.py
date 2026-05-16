import tempfile
from pathlib import Path
from unittest import TestCase

import pandas as pd

from research.pipeline import run_daily_turning_point_research


class DailyTurnStatsTest(TestCase):
    def test_research_pipeline_exports_expected_files(self) -> None:
        hourly = pd.DataFrame(
            [
                {"timestamp": "2026-01-01T00:00:00Z", "open": 100, "high": 101, "low": 98, "close": 99, "volume": 1},
                {"timestamp": "2026-01-01T01:00:00Z", "open": 99, "high": 100, "low": 97, "close": 98, "volume": 1},
                {"timestamp": "2026-01-01T02:00:00Z", "open": 98, "high": 99, "low": 95, "close": 96, "volume": 1},
                {"timestamp": "2026-01-01T03:00:00Z", "open": 96, "high": 97, "low": 94, "close": 95, "volume": 1},
                {"timestamp": "2026-01-01T04:00:00Z", "open": 95, "high": 98, "low": 93, "close": 97, "volume": 1},
                {"timestamp": "2026-01-01T05:00:00Z", "open": 97, "high": 101, "low": 96, "close": 100, "volume": 1},
                {"timestamp": "2026-01-01T06:00:00Z", "open": 100, "high": 104, "low": 99, "close": 103, "volume": 1},
                {"timestamp": "2026-01-01T07:00:00Z", "open": 103, "high": 105, "low": 102, "close": 104, "volume": 1},
                {"timestamp": "2026-01-01T08:00:00Z", "open": 104, "high": 106, "low": 103, "close": 105, "volume": 1},
                {"timestamp": "2026-01-01T09:00:00Z", "open": 105, "high": 107, "low": 104, "close": 106, "volume": 1},
                {"timestamp": "2026-01-01T10:00:00Z", "open": 106, "high": 108, "low": 105, "close": 107, "volume": 1},
                {"timestamp": "2026-01-01T11:00:00Z", "open": 107, "high": 109, "low": 106, "close": 108, "volume": 1},
                {"timestamp": "2026-01-01T12:00:00Z", "open": 108, "high": 110, "low": 107, "close": 109, "volume": 1},
                {"timestamp": "2026-01-01T13:00:00Z", "open": 109, "high": 112, "low": 108, "close": 111, "volume": 1},
                {"timestamp": "2026-01-01T14:00:00Z", "open": 111, "high": 114, "low": 110, "close": 113, "volume": 1},
                {"timestamp": "2026-01-01T15:00:00Z", "open": 113, "high": 115, "low": 112, "close": 114, "volume": 1},
                {"timestamp": "2026-01-01T16:00:00Z", "open": 114, "high": 116, "low": 113, "close": 115, "volume": 1},
                {"timestamp": "2026-01-01T17:00:00Z", "open": 115, "high": 117, "low": 114, "close": 116, "volume": 1},
                {"timestamp": "2026-01-01T18:00:00Z", "open": 116, "high": 118, "low": 115, "close": 117, "volume": 1},
                {"timestamp": "2026-01-01T19:00:00Z", "open": 117, "high": 119, "low": 116, "close": 118, "volume": 1},
                {"timestamp": "2026-01-01T20:00:00Z", "open": 118, "high": 120, "low": 117, "close": 119, "volume": 1},
                {"timestamp": "2026-01-01T21:00:00Z", "open": 119, "high": 121, "low": 118, "close": 120, "volume": 1},
                {"timestamp": "2026-01-01T22:00:00Z", "open": 120, "high": 122, "low": 119, "close": 121, "volume": 1},
                {"timestamp": "2026-01-01T23:00:00Z", "open": 121, "high": 124, "low": 120, "close": 123, "volume": 1},
            ]
        )
        daily = pd.DataFrame(
            [
                {"timestamp": "2025-12-31T00:00:00Z", "open": 110, "high": 112, "low": 100, "close": 101, "volume": 24},
                {"timestamp": "2026-01-01T00:00:00Z", "open": 100, "high": 124, "low": 93, "close": 123, "volume": 24},
            ]
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            hourly_path = Path(tmpdir) / "hourly.csv"
            daily_path = Path(tmpdir) / "daily.csv"
            output_dir = Path(tmpdir) / "out"
            hourly.to_csv(hourly_path, index=False)
            daily.to_csv(daily_path, index=False)

            result = run_daily_turning_point_research(
                hourly_path=hourly_path,
                daily_path=daily_path,
                output_dir=output_dir,
                symbol="BTC-USDT-SWAP",
                close_mode="utc+0",
            )

            self.assertTrue((output_dir / "samples.csv").exists())
            self.assertTrue((output_dir / "summary.csv").exists())
            self.assertTrue((output_dir / "heatmap_source.csv").exists())
            self.assertTrue((output_dir / "research_brief.md").exists())
            self.assertIn("day_low_hour", result.samples.columns)
            self.assertIn("extension_to_22h", result.samples.columns)
