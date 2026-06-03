from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd


SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from v6 import build_anchored_walkforward_splits, slice_frame_by_time


class V6RobustnessTest(unittest.TestCase):
    def test_build_anchored_walkforward_splits_respects_min_train_years(self) -> None:
        frame = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(
                    [
                        "2020-01-01 00:00:00+00:00",
                        "2021-01-01 00:00:00+00:00",
                        "2022-01-01 00:00:00+00:00",
                        "2023-01-01 00:00:00+00:00",
                        "2024-01-01 00:00:00+00:00",
                    ]
                )
            }
        )

        splits = build_anchored_walkforward_splits(frame, first_test_year=2021, min_train_years=2)

        self.assertEqual([item.label for item in splits], ["2022_test", "2023_test", "2024_test"])
        self.assertEqual(splits[0].train_end, pd.Timestamp("2022-01-01 00:00:00+00:00"))
        self.assertEqual(splits[0].test_start, pd.Timestamp("2022-01-01 00:00:00+00:00"))

    def test_slice_frame_by_time_preserves_original_index_for_signal_alignment(self) -> None:
        frame = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(
                    [
                        "2024-01-01 00:00:00+00:00",
                        "2024-01-02 00:00:00+00:00",
                        "2024-01-03 00:00:00+00:00",
                    ]
                ),
                "value": [1, 2, 3],
            },
            index=[10, 11, 12],
        )

        sliced = slice_frame_by_time(
            frame,
            pd.Timestamp("2024-01-02 00:00:00+00:00"),
            pd.Timestamp("2024-01-04 00:00:00+00:00"),
        )

        self.assertEqual(list(sliced.index), [11, 12])
        self.assertEqual(list(sliced["value"]), [2, 3])


if __name__ == "__main__":
    unittest.main()
