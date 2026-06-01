from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from okx_quant.candle_cache import load_candle_cache
from okx_quant.ma55_slope_regime import add_indicators, build_frame, enrich_line


REPORT_DIR = ROOT / "reports"
INST_ID = "BTC-USDT-SWAP"
BAR = "1H"


def segment_durations(df: pd.DataFrame, mask: pd.Series, up_label: str, down_label: str) -> pd.DataFrame:
    ready = df[mask.notna()].copy()
    ready["state"] = mask.astype(bool)
    changes = ready["state"].astype(int).diff().fillna(0).ne(0)
    ready["segment_id"] = changes.cumsum()

    rows: list[dict[str, object]] = []
    for (_, is_up), group in ready.groupby(["segment_id", "state"], sort=False):
        rows.append(
            {
                "phase": up_label if is_up else down_label,
                "bars": int(len(group)),
                "start": str(group.iloc[0]["timestamp"] if "timestamp" in group.columns else group.iloc[0]["ts"]),
                "end": str(group.iloc[-1]["timestamp"] if "timestamp" in group.columns else group.iloc[-1]["ts"]),
            }
        )
    return pd.DataFrame(rows)


def summarize(segments: pd.DataFrame) -> dict[str, dict[str, float | int]]:
    summary: dict[str, dict[str, float | int]] = {}
    for phase, bucket in segments.groupby("phase", sort=False):
        bars = bucket["bars"]
        summary[str(phase)] = {
            "segment_count": int(len(bucket)),
            "mean_bars": float(bars.mean()),
            "median_bars": float(bars.median()),
            "p25_bars": float(bars.quantile(0.25)),
            "p75_bars": float(bars.quantile(0.75)),
            "p90_bars": float(bars.quantile(0.90)),
            "max_bars": int(bars.max()),
        }
    return summary


def main() -> None:
    REPORT_DIR.mkdir(exist_ok=True)
    candles = load_candle_cache(INST_ID, BAR, limit=None)
    if not candles:
        raise RuntimeError(f"no candles for {INST_ID} {BAR}")

    df = build_frame(candles)
    add_indicators(df)
    enriched = enrich_line(df, "sma55")

    price_segments = segment_durations(
        enriched,
        enriched["close"] > enriched["sma55"],
        up_label="上升阶段(收盘>MA55)",
        down_label="下降阶段(收盘<MA55)",
    )
    slope_segments = segment_durations(
        enriched[enriched["regime"] != "warming_up"].copy(),
        enriched.loc[enriched["regime"] != "warming_up", "slope_ratio"] > 0,
        up_label="上升阶段(MA55斜率>0)",
        down_label="下降阶段(MA55斜率<0)",
    )

    payload = {
        "instrument": INST_ID,
        "bar": BAR,
        "bar_count": len(enriched),
        "start": str(enriched["timestamp"].iloc[0]),
        "end": str(enriched["timestamp"].iloc[-1]),
        "by_price_vs_ma55": summarize(price_segments),
        "by_ma55_slope": summarize(slope_segments),
    }

    price_segments.to_csv(
        REPORT_DIR / "btc_1h_ma55_price_phase_segments.csv",
        index=False,
        encoding="utf-8-sig",
    )
    slope_segments.to_csv(
        REPORT_DIR / "btc_1h_ma55_slope_phase_segments.csv",
        index=False,
        encoding="utf-8-sig",
    )
    out = REPORT_DIR / "btc_1h_ma55_phase_duration.json"
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps(payload, ensure_ascii=False, indent=2))
    print(out)


if __name__ == "__main__":
    main()
