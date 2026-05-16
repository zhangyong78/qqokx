from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from export.csv_exporter import export_research_outputs
from research.day_classifier import build_daily_samples
from utils.io import load_candle_frame


@dataclass(slots=True)
class ResearchResult:
    samples: pd.DataFrame
    summary: pd.DataFrame
    heatmap: pd.DataFrame


def run_daily_turning_point_research(
    *,
    hourly_path: str | Path,
    output_dir: str | Path,
    symbol: str | None = None,
    daily_path: str | Path | None = None,
    close_mode: str = "utc+8",
) -> ResearchResult:
    hourly_frame = load_candle_frame(hourly_path)
    daily_frame = load_candle_frame(daily_path) if daily_path else None
    samples = build_daily_samples(
        hourly_frame=hourly_frame,
        daily_frame=daily_frame,
        symbol=symbol,
        close_mode=close_mode,
    )
    summary, heatmap = export_research_outputs(samples=samples, output_dir=output_dir)
    return ResearchResult(samples=samples, summary=summary, heatmap=heatmap)
