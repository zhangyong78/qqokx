from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class WalkForwardSplit:
    label: str
    train_start: pd.Timestamp
    train_end: pd.Timestamp
    test_start: pd.Timestamp
    test_end: pd.Timestamp


def build_anchored_walkforward_splits(
    frame: pd.DataFrame,
    *,
    first_test_year: int,
    min_train_years: int = 2,
) -> list[WalkForwardSplit]:
    timestamps = pd.to_datetime(frame["timestamp"], utc=True)
    if timestamps.empty:
        return []

    first_year = int(timestamps.iloc[0].year)
    last_year = int(timestamps.iloc[-1].year)
    effective_first_test_year = max(first_test_year, first_year + min_train_years)
    splits: list[WalkForwardSplit] = []

    for test_year in range(effective_first_test_year, last_year + 1):
        train_start = pd.Timestamp(year=first_year, month=1, day=1, tz="UTC")
        train_end = pd.Timestamp(year=test_year, month=1, day=1, tz="UTC")
        test_start = train_end
        test_end = pd.Timestamp(year=test_year + 1, month=1, day=1, tz="UTC")

        has_train = bool(((timestamps >= train_start) & (timestamps < train_end)).any())
        has_test = bool(((timestamps >= test_start) & (timestamps < test_end)).any())
        if not (has_train and has_test):
            continue

        splits.append(
            WalkForwardSplit(
                label=f"{test_year}_test",
                train_start=train_start,
                train_end=train_end,
                test_start=test_start,
                test_end=test_end,
            )
        )
    return splits


def slice_frame_by_time(frame: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    timestamps = pd.to_datetime(frame["timestamp"], utc=True)
    return frame[(timestamps >= start) & (timestamps < end)].copy()
