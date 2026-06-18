from __future__ import annotations

from unittest import TestCase

import pandas as pd

from research.last_hour_daily_relationship import build_last_hour_daily_features


class LastHourDailyRelationshipResearchTest(TestCase):
    def test_build_features_uses_8am_session_boundary_and_next_session_only(self) -> None:
        rows: list[dict[str, object]] = []
        timestamps = pd.date_range("2026-01-01T00:00:00Z", periods=72, freq="1h", tz="UTC")

        for index, timestamp in enumerate(timestamps):
            session_index = index // 24
            hour_in_session = index % 24

            if session_index == 0:
                if hour_in_session == 0:
                    open_price, high_price, low_price, close_price = 100.0, 100.5, 99.8, 100.0
                elif hour_in_session == 23:
                    open_price, high_price, low_price, close_price = 100.0, 102.0, 99.0, 101.0
                else:
                    open_price, high_price, low_price, close_price = 100.0, 100.6, 99.7, 100.0
            elif session_index == 1:
                if hour_in_session == 0:
                    open_price, high_price, low_price, close_price = 101.0, 103.0, 100.2, 102.4
                elif hour_in_session == 1:
                    open_price, high_price, low_price, close_price = 102.4, 101.8, 98.0, 100.0
                elif hour_in_session == 23:
                    open_price, high_price, low_price, close_price = 100.0, 102.0, 99.8, 102.0
                else:
                    open_price, high_price, low_price, close_price = 100.0, 101.4, 99.5, 100.2
            else:
                if hour_in_session == 23:
                    open_price, high_price, low_price, close_price = 150.0, 200.0, 149.0, 180.0
                else:
                    open_price, high_price, low_price, close_price = 150.0, 151.0, 149.0, 150.0

            rows.append(
                {
                    "timestamp": timestamp,
                    "open": open_price,
                    "high": high_price,
                    "low": low_price,
                    "close": close_price,
                    "volume": 10.0 + hour_in_session,
                    "symbol": "BTC-USDT-SWAP",
                }
            )

        frame = pd.DataFrame(rows)
        features = build_last_hour_daily_features(
            hourly_frame=frame,
            symbol="BTC-USDT-SWAP",
            timezone_offset_hours=8,
            session_close_hour=8,
        )

        first_row = features.loc[features["session_date"] == "2026-01-01"].iloc[0]

        self.assertEqual(str(pd.Timestamp(first_row["signal_time_utc"])), "2026-01-01 23:00:00+00:00")
        self.assertEqual(str(pd.Timestamp(first_row["entry_time_utc"])), "2026-01-02 00:00:00+00:00")
        self.assertEqual(first_row["next_session_date"], "2026-01-02")
        self.assertEqual(first_row["prev_day_color"], "bull")
        self.assertEqual(first_row["signal_color"], "bull")
        self.assertTrue(bool(first_row["long_hit_1r"]))
        self.assertTrue(bool(first_row["long_stop_hit"]))
        self.assertAlmostEqual(float(first_row["long_final_close_r"]), 0.5, places=6)
        self.assertAlmostEqual(float(first_row["long_realized_r"]), -1.0, places=6)
        self.assertTrue(bool(first_row["short_hit_2r"]))
        self.assertTrue(bool(first_row["short_stop_hit"]))
        self.assertAlmostEqual(float(first_row["short_final_close_r"]), -1.0, places=6)
        self.assertAlmostEqual(float(first_row["short_realized_r"]), -1.0, places=6)
