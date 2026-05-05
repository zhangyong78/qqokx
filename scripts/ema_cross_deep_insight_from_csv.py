"""
从矩阵回测 *_full.csv（及同 stem 的 *_summary.json，可选）生成「五币种深度洞察」客户版长文。

用法:
  python scripts/ema_cross_deep_insight_from_csv.py D:/qqokx_data/reports/analysis/ema_cross_matrix_xxx_full.csv
  python scripts/ema_cross_deep_insight_from_csv.py path/to.csv path/to/out.txt
"""

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from okx_quant.ema_cross_insight_text import build_client_deep_insight


def main() -> None:
    if len(sys.argv) < 2:
        print("用法: python scripts/ema_cross_deep_insight_from_csv.py <*_full.csv> [输出.txt]")
        raise SystemExit(2)
    csv_path = Path(sys.argv[1])
    rows = list(csv.DictReader(csv_path.open(encoding="utf-8-sig")))
    stem = csv_path.name.replace("_full.csv", "").replace(".csv", "")
    out_path = Path(sys.argv[2]) if len(sys.argv) > 2 else csv_path.with_name(f"{stem}_deep_insight_客户版.txt")

    json_path = csv_path.with_name(stem + "_summary.json")
    spec: dict = {}
    utc_ts = ""
    smoke = "smoke" in csv_path.name.lower()
    if json_path.exists():
        meta = json.loads(json_path.read_text(encoding="utf-8"))
        spec = dict(meta.get("spec") or {})
        utc_ts = str(meta.get("generated_at_utc") or "")
        smoke = bool(meta.get("smoke", smoke))
    if not spec:
        spec = {
            "strategy": "EMA突破跌破",
            "coins": sorted({str(r.get("inst_id")) for r in rows if r.get("inst_id")}),
            "bars": sorted({str(r.get("bar")) for r in rows if r.get("bar")}),
            "candle_limit": int(rows[0]["candle_count"]) if rows else 0,
            "slippage_each_side": "",
        }
    if not utc_ts:
        if "_" in stem and len(stem) > 20:
            utc_ts = stem.split("_")[-2] + "_" + stem.split("_")[-1] if "_smoke" in stem else stem.split("_")[-1]
        else:
            utc_ts = stem

    text = build_client_deep_insight(rows, utc_ts=utc_ts, spec=spec, smoke=smoke)
    out_path.write_text(text, encoding="utf-8")
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
