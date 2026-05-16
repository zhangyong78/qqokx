from __future__ import annotations

from pathlib import Path

import pandas as pd

from export.research_brief import build_research_brief


def export_research_outputs(*, samples: pd.DataFrame, output_dir: str | Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    target_dir = Path(output_dir)
    target_dir.mkdir(parents=True, exist_ok=True)

    samples_path = target_dir / "samples.csv"
    samples.to_csv(samples_path, index=False, encoding="utf-8-sig")

    summary = _build_summary(samples)
    summary.to_csv(target_dir / "summary.csv", index=False, encoding="utf-8-sig")

    heatmap = _build_heatmap_source(samples)
    heatmap.to_csv(target_dir / "heatmap_source.csv", index=False, encoding="utf-8-sig")

    (target_dir / "research_brief.md").write_text(build_research_brief(), encoding="utf-8")
    return summary, heatmap


def _build_summary(samples: pd.DataFrame) -> pd.DataFrame:
    event_rows = []
    for column in ("day_low_hour", "day_high_hour", "last_below_open_hour", "last_above_open_hour"):
        subset = samples.loc[samples[column].notna() & (samples[column] != ""), ["day_type", "trend_type", column]].copy()
        if subset.empty:
            continue
        subset = subset.rename(columns={column: "hour"})
        subset["metric"] = column
        event_rows.append(subset)
    if not event_rows:
        return pd.DataFrame(columns=["day_type", "trend_type", "metric", "hour", "sample_count", "probability"])
    events = pd.concat(event_rows, ignore_index=True)
    grouped = (
        events.groupby(["day_type", "trend_type", "metric", "hour"])
        .size()
        .rename("sample_count")
        .reset_index()
    )
    totals = grouped.groupby(["day_type", "trend_type", "metric"])["sample_count"].transform("sum")
    grouped["probability"] = grouped["sample_count"] / totals
    return grouped.sort_values(["day_type", "metric", "trend_type", "hour"]).reset_index(drop=True)


def _build_heatmap_source(samples: pd.DataFrame) -> pd.DataFrame:
    events = []
    for metric in ("day_low_hour", "day_high_hour"):
        subset = samples.loc[samples[metric].notna() & (samples[metric] != ""), ["day_type", metric]].copy()
        if subset.empty:
            continue
        subset = subset.rename(columns={metric: "hour"})
        subset["metric"] = metric
        events.append(subset)
    if not events:
        return pd.DataFrame(columns=["day_type", "metric", "hour", "count"])
    frame = pd.concat(events, ignore_index=True)
    heatmap = frame.groupby(["day_type", "metric", "hour"]).size().rename("count").reset_index()
    return heatmap.sort_values(["metric", "day_type", "hour"]).reset_index(drop=True)
