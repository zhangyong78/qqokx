from __future__ import annotations

import csv
import html
import importlib.util
import sys
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


MODULE_PATH = ROOT / "scripts" / "research_btc_s096_s097_distance_confirmation_compare.py"
SPEC = importlib.util.spec_from_file_location("distance_research_module", MODULE_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"Unable to load research module: {MODULE_PATH}")
RESEARCH = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = RESEARCH
SPEC.loader.exec_module(RESEARCH)


REPORT_DIR = ROOT / "reports"
REPORT_DIR.mkdir(parents=True, exist_ok=True)
STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
BASENAME = f"btc_s096_s097_min_distance_window_report_{STAMP}"
HTML_PATH = REPORT_DIR / f"{BASENAME}.html"
CSV_PATH = REPORT_DIR / f"{BASENAME}.csv"
LATEST_HTML_PATH = REPORT_DIR / "btc_s096_s097_min_distance_window_report_latest.html"
LATEST_CSV_PATH = REPORT_DIR / "btc_s096_s097_min_distance_window_report_latest.csv"

WINDOWS = (
    ("recent_10000", "Recent 10000 bars", 10_000),
    ("full_history", "Full history", 0),
)


@dataclass(frozen=True)
class Row:
    snapshot_id: str
    window_key: str
    window_label: str
    variant_key: str
    variant_label: str
    trades: int
    total_pnl: Decimal
    return_pct: Decimal
    max_drawdown_pct: Decimal
    profit_factor: Decimal | None
    positive_months: int
    negative_months: int
    worst_month: str


def main() -> None:
    snapshots = RESEARCH.load_snapshots()
    configs = {sid: RESEARCH.config_from_snapshot(snapshots[sid]["config"]) for sid in RESEARCH.SNAPSHOT_IDS}
    variants = [v for v in RESEARCH.VARIANTS if v.key == "baseline" or v.key.startswith("min_distance_")]
    client = RESEARCH.OkxRestClient()

    rows: list[Row] = []
    for snapshot_id in RESEARCH.SNAPSHOT_IDS:
        config = configs[snapshot_id]
        instrument = client.get_instrument(config.inst_id)
        maker_fee_rate = Decimal(str(snapshots[snapshot_id].get("maker_fee_rate", "0")))
        taker_fee_rate = Decimal(str(snapshots[snapshot_id].get("taker_fee_rate", "0")))
        for window_key, window_label, candle_limit in WINDOWS:
            candles = client.get_candles_history(config.inst_id, config.bar, limit=candle_limit)
            for variant in variants:
                result = RESEARCH.run_experiment(
                    candles,
                    instrument,
                    config,
                    variant=variant,
                    maker_fee_rate=maker_fee_rate,
                    taker_fee_rate=taker_fee_rate,
                )
                rows.append(build_row(snapshot_id, window_key, window_label, variant, result))

    write_csv(rows)
    html_text = build_html(rows)
    HTML_PATH.write_text(html_text, encoding="utf-8")
    LATEST_HTML_PATH.write_text(html_text, encoding="utf-8")

    print(HTML_PATH)
    print(CSV_PATH)


def build_row(snapshot_id: str, window_key: str, window_label: str, variant: object, result: object) -> Row:
    report = result.report
    positive_months = sum(1 for stat in result.monthly_stats if stat.total_pnl > 0)
    negative_months = sum(1 for stat in result.monthly_stats if stat.total_pnl < 0)
    worst_month = min(result.monthly_stats, key=lambda stat: stat.total_pnl) if result.monthly_stats else None
    return Row(
        snapshot_id=snapshot_id,
        window_key=window_key,
        window_label=window_label,
        variant_key=variant.key,
        variant_label=variant.label,
        trades=report.total_trades,
        total_pnl=report.total_pnl,
        return_pct=report.total_return_pct,
        max_drawdown_pct=report.max_drawdown_pct,
        profit_factor=report.profit_factor,
        positive_months=positive_months,
        negative_months=negative_months,
        worst_month="-"
        if worst_month is None
        else f"{worst_month.period_label} {fmt(worst_month.total_pnl, 2)}",
    )


def write_csv(rows: list[Row]) -> None:
    fieldnames = [
        "snapshot_id",
        "window_key",
        "window_label",
        "variant_key",
        "variant_label",
        "trades",
        "total_pnl",
        "return_pct",
        "max_drawdown_pct",
        "profit_factor",
        "positive_months",
        "negative_months",
        "worst_month",
    ]
    for path in (CSV_PATH, LATEST_CSV_PATH):
        with path.open("w", newline="", encoding="utf-8-sig") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow(
                    {
                        "snapshot_id": row.snapshot_id,
                        "window_key": row.window_key,
                        "window_label": row.window_label,
                        "variant_key": row.variant_key,
                        "variant_label": row.variant_label,
                        "trades": row.trades,
                        "total_pnl": fmt(row.total_pnl, 2),
                        "return_pct": fmt(row.return_pct, 2),
                        "max_drawdown_pct": fmt(row.max_drawdown_pct, 2),
                        "profit_factor": fmt_or_dash(row.profit_factor, 4),
                        "positive_months": row.positive_months,
                        "negative_months": row.negative_months,
                        "worst_month": row.worst_month,
                    }
                )


def build_html(rows: list[Row]) -> str:
    findings = build_findings(rows)
    finding_items = "".join(f"<li>{html.escape(item)}</li>" for item in findings)
    sections = []
    for snapshot_id in RESEARCH.SNAPSHOT_IDS:
        for window_key, window_label, _ in WINDOWS:
            subset = [row for row in rows if row.snapshot_id == snapshot_id and row.window_key == window_key]
            sections.append(
                f"<section class='panel'><h2>{html.escape(snapshot_id)} | {html.escape(window_label)}</h2>{build_table(subset)}</section>"
            )
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>BTC S096/S097 Min Distance Window Report</title>
  <style>
    body {{ margin: 0; font-family: "Segoe UI", "Microsoft YaHei", sans-serif; color: #1f2937; background: #f7f8fb; }}
    .wrap {{ max-width: 1280px; margin: 0 auto; padding: 28px 20px 56px; }}
    .panel {{ background: #fff; border: 1px solid #d9e2ec; border-radius: 8px; padding: 18px 20px; margin-bottom: 16px; }}
    h1 {{ margin: 0 0 8px; font-size: 28px; }}
    h2 {{ margin: 0 0 10px; font-size: 19px; }}
    p, li {{ line-height: 1.55; }}
    .muted {{ color: #667085; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 12.5px; }}
    th, td {{ padding: 9px 7px; border-bottom: 1px solid #e6edf5; text-align: right; vertical-align: top; }}
    th:first-child, td:first-child, th:last-child, td:last-child {{ text-align: left; }}
    th {{ color: #667085; font-weight: 700; background: #f2f6fb; }}
    .good {{ color: #047857; font-weight: 700; }}
    .bad {{ color: #b42318; font-weight: 700; }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="panel">
      <h1>BTC S096/S097 Min Distance Window Report</h1>
      <p class="muted">Generated at {html.escape(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))}. Research-only comparison for baseline and min-distance sweep across recent and full-history windows.</p>
    </section>
    <section class="panel">
      <h2>Findings</h2>
      <ul>{finding_items}</ul>
    </section>
    {''.join(sections)}
  </div>
</body>
</html>
"""


def build_findings(rows: list[Row]) -> list[str]:
    findings: list[str] = []
    for snapshot_id in RESEARCH.SNAPSHOT_IDS:
        for window_key, window_label, _ in WINDOWS:
            subset = [row for row in rows if row.snapshot_id == snapshot_id and row.window_key == window_key]
            baseline = next(row for row in subset if row.variant_key == "baseline")
            best = max(subset, key=lambda row: (row.return_pct, row.profit_factor or Decimal("-1")))
            findings.append(
                f"{snapshot_id} {window_label}: best variant is {best.variant_label} ({fmt(best.return_pct, 2)}%, DD {fmt(best.max_drawdown_pct, 2)}%, PF {fmt_or_dash(best.profit_factor, 4)})."
            )
            if best.variant_key != "baseline":
                findings.append(
                    f"{snapshot_id} {window_label}: versus baseline, best variant changes PnL by {fmt(best.total_pnl - baseline.total_pnl, 2, signed=True)}U and DD by {fmt(best.max_drawdown_pct - baseline.max_drawdown_pct, 2, signed=True)} pct points."
                )
    return findings


def build_table(rows: list[Row]) -> str:
    ordered_keys = ["baseline"] + sorted([row.variant_key for row in rows if row.variant_key != "baseline"])
    ordered = sorted(rows, key=lambda row: ordered_keys.index(row.variant_key))
    body = []
    for row in ordered:
        body.append(
            "<tr>"
            f"<td>{html.escape(row.variant_label)}</td>"
            f"<td>{row.trades}</td>"
            f"<td class='{metric_class(row.total_pnl)}'>{fmt(row.total_pnl, 2)}</td>"
            f"<td class='{metric_class(row.return_pct)}'>{fmt(row.return_pct, 2)}%</td>"
            f"<td>{fmt(row.max_drawdown_pct, 2)}%</td>"
            f"<td>{fmt_or_dash(row.profit_factor, 4)}</td>"
            f"<td>{row.positive_months}/{row.negative_months}</td>"
            f"<td>{html.escape(row.worst_month)}</td>"
            "</tr>"
        )
    return (
        "<table><thead><tr>"
        "<th>Variant</th><th>Trades</th><th>Total PnL</th><th>Return</th><th>Max DD</th><th>PF</th><th>+/- Months</th><th>Worst Month</th>"
        "</tr></thead>"
        f"<tbody>{''.join(body)}</tbody></table>"
    )


def metric_class(value: Decimal) -> str:
    return "good" if value > 0 else "bad" if value < 0 else ""


def fmt_or_dash(value: Decimal | None, places: int) -> str:
    if value is None:
        return "-"
    return fmt(value, places)


def fmt(value: Decimal, places: int, *, signed: bool = False) -> str:
    quant = Decimal(1).scaleb(-places)
    text = f"{value.quantize(quant):f}"
    if signed and value > 0:
        return f"+{text}"
    return text


if __name__ == "__main__":
    main()
