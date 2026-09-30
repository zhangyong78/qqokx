from __future__ import annotations

import json
import math
import sqlite3
import tempfile
import unittest
from contextlib import closing
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from okx_quant.volatility_model_weights import MODELS
from okx_quant.volatility_prediction import SHANGHAI, aggregate_daily, build_features, forecast_daily, predict_local, predict_variance


def daily_samples(count: int = 120) -> pd.DataFrame:
    rng = np.random.default_rng(159)
    close = 30_000 * np.exp(np.cumsum(rng.normal(0, .02, count)))
    return pd.DataFrame({
        "date": pd.date_range("2026-01-01", periods=count).strftime("%Y-%m-%d"),
        "open": close * .99, "high": close * 1.03, "low": close * .97, "close": close,
        "rvvar": rng.uniform(.05, .5, count), "iv": rng.uniform(30, 70, count),
    })


class VolatilityPredictionTests(unittest.TestCase):
    def test_locked_research_golden_forecast(self):
        # Independent 2026-09-29 feature/forecast snapshot from the research run.
        row = pd.DataFrame([{
            "log_var1": -2.1037664590300533, "log_var7": -2.385363715378137,
            "log_var30": -2.128819779317368, "log_iv_var": -2.0634031667709656,
            "iv_change1": 0.00999999999999801, "btc_ret1": -0.0035097641858000215,
            "btc_ret7": -0.03941559669709385, "log_atr": -3.5873375845782993,
            "range_norm": 0.7741345626175503, "weekend_fraction1": 0.0,
        }])
        self.assertAlmostEqual(float(predict_variance(row, 1)[0]), 0.12097545081601917, places=14)
        self.assertAlmostEqual(float(predict_variance(row, 7)[0]), 0.13689825193364152, places=14)

    def test_standardization_intercept_and_smearing(self):
        for horizon, model in MODELS.items():
            row = pd.DataFrame([model["mean"]], columns=model["features"])
            expected = math.exp(model["coef"][0]) * model["smearing"]
            self.assertAlmostEqual(float(predict_variance(row, horizon)[0]), expected, places=14)

    def test_features_are_causal(self):
        raw = daily_samples()
        prefix = build_features(raw.iloc[:90])
        full = build_features(raw)
        pd.testing.assert_frame_equal(prefix, full.iloc[:len(prefix)].reset_index(drop=True))
        for horizon in MODELS:
            np.testing.assert_allclose(predict_variance(prefix, horizon), predict_variance(full, horizon)[:len(prefix)], atol=1e-14)

    def test_incomplete_today_excluded_and_horizon_units(self):
        raw = daily_samples()
        now = datetime(2026, 4, 30, 12, tzinfo=SHANGHAI)
        a = forecast_daily(raw, now=now)
        raw.loc[raw.date >= "2026-04-30", ["iv", "close", "rvvar"]] = 1e6
        b = forecast_daily(raw, now=now)
        self.assertEqual(a, b)
        self.assertTrue(a["is_current"])
        self.assertEqual(a["data_end"], "2026-04-29")
        self.assertEqual(a["forecasts"][0]["target_end"], "2026-04-30")
        self.assertEqual(a["forecasts"][1]["target_end"], "2026-05-06")
        for row in a["forecasts"]:
            expected = row["annualized_rv_percent"] * math.sqrt(row["horizon_days"] / 365)
            self.assertAlmostEqual(expected, row["horizon_move_scale_percent"])
        self.assertIsNone(a["history"][-1]["actual_rv_percent"])
        json.dumps(a, allow_nan=False)

    def test_stale_dates_not_relabelled_as_current(self):
        r = forecast_daily(daily_samples(), now=datetime(2026, 5, 5, tzinfo=SHANGHAI))
        self.assertFalse(r["is_current"])
        self.assertEqual(r["stale_days"], 4)
        self.assertEqual(r["forecasts"][0]["target_end"], "2026-05-01")

    def test_gaps_and_bad_features_fail_closed(self):
        with self.assertRaisesRegex(ValueError, "不连续"):
            build_features(daily_samples().drop(index=88))
        with self.assertRaises(ValueError):
            build_features(daily_samples(60))
        row = pd.DataFrame([MODELS[1]["mean"]], columns=MODELS[1]["features"])
        row.iloc[0, 0] = np.nan
        with self.assertRaises(ValueError):
            predict_variance(row)
        with self.assertRaises(ValueError):
            predict_variance(row.fillna(0), 30)
        with self.assertRaisesRegex(ValueError, "时区"):
            forecast_daily(daily_samples(), now=datetime(2026, 5, 1))

    def test_hourly_read_only_pipeline(self):
        first = datetime(2026, 1, 1, tzinfo=SHANGHAI)
        iv_rows, spot_rows = [], []
        # One predecessor hour supplies midnight's close-to-close return.
        for i in range(-1, 62 * 24):
            ts = int((first + timedelta(hours=i)).timestamp() * 1000)
            close = 30_000 * math.exp(.001 * math.sin(i / 9))
            spot_rows.append({"ts": ts, "open": close, "high": close * 1.01, "low": close * .99, "close": close})
            if i >= 0:
                iv_rows.append({"ts": ts, "open": 40, "high": 41, "low": 39, "close": 40})
        now = datetime(2026, 3, 4, 10, tzinfo=SHANGHAI)
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            cache = root / "volatility.json"
            cache.write_text(json.dumps({"BTC|hourly_base": {"volatility_hourly": iv_rows}}), encoding="utf-8")
            dbpath = root / "candles.db"
            with closing(sqlite3.connect(dbpath)) as db:
                db.execute("CREATE TABLE candles(inst_id TEXT,bar TEXT,ts INTEGER,open REAL,high REAL,low REAL,close REAL,confirmed INTEGER)")
                db.executemany("INSERT INTO candles VALUES(?,?,?,?,?,?,?,?)", [("BTC-USDT", "1H", *row.values(), 1) for row in spot_rows])
                # Must not consume unconfirmed data, even with a duplicate timestamp.
                db.execute("INSERT INTO candles VALUES('BTC-USDT','1H',?,1,1,1,1,0)", (spot_rows[-1]["ts"],))
                db.commit()
            before = (cache.read_bytes(), dbpath.read_bytes())
            result = predict_local(now=now, volatility_path=cache, candle_path=dbpath)
            self.assertEqual(result["complete_daily_samples"], 62)
            self.assertEqual(result["data_end"], "2026-03-03")
            self.assertEqual(before, (cache.read_bytes(), dbpath.read_bytes()))
        # Dropping an hour removes a full day, not a fake zero return.
        incomplete = aggregate_daily(iv_rows, spot_rows[:-5] + spot_rows[-4:], "2026-03-04")
        self.assertEqual(len(incomplete), 61)
        with self.assertRaises(ValueError):
            build_features(aggregate_daily(iv_rows, [r for i, r in enumerate(spot_rows) if i != 720], "2026-03-04"))
        with self.assertRaisesRegex(ValueError, "重复"):
            aggregate_daily(iv_rows + [iv_rows[-1]], spot_rows, "2026-03-04")


if __name__ == "__main__":
    unittest.main()
