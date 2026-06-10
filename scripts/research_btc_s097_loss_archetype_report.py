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
SPEC = importlib.util.spec_from_file_location("distance_research_module_loss", MODULE_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"Unable to load research module: {MODULE_PATH}")
RESEARCH = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = RESEARCH
SPEC.loader.exec_module(RESEARCH)


REPORT_DIR = ROOT / "reports"
REPORT_DIR.mkdir(parents=True, exist_ok=True)
STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
BASENAME = f"btc_s097_loss_archetype_report_{STAMP}"
HTML_PATH = REPORT_DIR / f"{BASENAME}.html"
SUMMARY_CSV_PATH = REPORT_DIR / f"{BASENAME}_summary.csv"
DETAIL_CSV_PATH = REPORT_DIR / f"{BASENAME}_details.csv"
LATEST_HTML_PATH = REPORT_DIR / "btc_s097_loss_archetype_report_latest.html"
LATEST_SUMMARY_CSV_PATH = REPORT_DIR / "btc_s097_loss_archetype_report_latest_summary.csv"
LATEST_DETAIL_CSV_PATH = REPORT_DIR / "btc_s097_loss_archetype_report_latest_details.csv"

SNAPSHOT_ID = "S097"
WINDOWS = (
    ("recent_10000", "Recent 10000 bars", 10_000),
    ("full_history", "Full history", 0),
)


@dataclass(frozen=True)
class LossDetailRow:
    window_key: str
    window_label: str
    entry_time: str
    exit_time: str
    bars_held: int
    pnl: Decimal
    r_multiple: Decimal
    exit_reason: str
    archetype: str
    distance_bucket: str
    entry_distance_atr: Decimal
    mfe_r: Decimal
    mae_r: Decimal
    max_favorable_atr: Decimal
    max_adverse_atr: Decimal


@dataclass(frozen=True)
class SummaryRow:
    window_key: str
    window_label: str
    group_type: str
    group_key: str
    trades: int
    total_pnl: Decimal
    avg_pnl: Decimal
    avg_r: Decimal
    share_of_loss_pct: Decimal


def main() -> None:
    snapshots = RESEARCH.load_snapshots()
    config = RESEARCH.config_from_snapshot(snapshots[SNAPSHOT_ID]["config"])
    maker_fee_rate = Decimal(str(snapshots[SNAPSHOT_ID].get("maker_fee_rate", "0")))
    taker_fee_rate = Decimal(str(snapshots[SNAPSHOT_ID].get("taker_fee_rate", "0")))
    client = RESEARCH.OkxRestClient()
    instrument = client.get_instrument(config.inst_id)
    baseline_variant = next(variant for variant in RESEARCH.VARIANTS if variant.key == "baseline")

    detail_rows: list[LossDetailRow] = []
    summary_rows: list[SummaryRow] = []
    findings: list[str] = []

    for window_key, window_label, candle_limit in WINDOWS:
        candles = client.get_candles_history(config.inst_id, config.bar, limit=candle_limit)
        result = RESEARCH.run_experiment(
            candles,
            instrument,
            config,
            variant=baseline_variant,
            maker_fee_rate=maker_fee_rate,
            taker_fee_rate=taker_fee_rate,
        )
        window_details = build_loss_detail_rows(window_key, window_label, result)
        detail_rows.extend(window_details)
        summary_rows.extend(build_summary_rows(window_key, window_label, window_details))
        findings.extend(build_findings_for_window(window_label, window_details))

    write_summary_csv(summary_rows)
    write_detail_csv(detail_rows)
    html_text = build_html(summary_rows, detail_rows, findings)
    HTML_PATH.write_text(html_text, encoding="utf-8")
    LATEST_HTML_PATH.write_text(html_text, encoding="utf-8")

    print(HTML_PATH)
    print(SUMMARY_CSV_PATH)
    print(DETAIL_CSV_PATH)


def build_loss_detail_rows(window_key: str, window_label: str, result: object) -> list[LossDetailRow]:
    rows: list[LossDetailRow] = []
    for trade in result.trades:
        if trade.pnl >= 0:
            continue
        rows.append(build_loss_detail_row(window_key, window_label, result, trade))
    rows.sort(key=lambda row: row.pnl)
    return rows


def build_loss_detail_row(window_key: str, window_label: str, result: object, trade: object) -> LossDetailRow:
    candles = result.candles
    entry_candle = candles[trade.entry_index]
    atr_value = trade.atr_value if trade.atr_value and trade.atr_value > 0 else Decimal("0")
    risk_per_unit = (trade.stop_loss - trade.entry_price) if trade.signal == "short" else (trade.entry_price - trade.stop_loss)
    holding_slice = candles[trade.entry_index : trade.exit_index + 1]
    lowest_low = min(candle.low for candle in holding_slice)
    highest_high = max(candle.high for candle in holding_slice)

    if trade.signal == "short":
        mfe_price = trade.entry_price - lowest_low
        mae_price = highest_high - trade.entry_price
        entry_distance_atr = Decimal("0")
        ema_value = result.ema_values[trade.entry_index]
        if ema_value is not None and atr_value > 0 and entry_candle.close < ema_value:
            entry_distance_atr = (ema_value - entry_candle.close) / atr_value
    else:
        mfe_price = highest_high - trade.entry_price
        mae_price = trade.entry_price - lowest_low
        entry_distance_atr = Decimal("0")

    mfe_r = Decimal("0") if risk_per_unit <= 0 else mfe_price / risk_per_unit
    mae_r = Decimal("0") if risk_per_unit <= 0 else mae_price / risk_per_unit
    max_favorable_atr = Decimal("0") if atr_value <= 0 else mfe_price / atr_value
    max_adverse_atr = Decimal("0") if atr_value <= 0 else mae_price / atr_value
    bars_held = max(trade.exit_index - trade.entry_index, 0)
    distance_bucket = distance_bucket_label(entry_distance_atr)
    archetype = classify_loss_archetype(
        exit_reason=str(trade.exit_reason),
        bars_held=bars_held,
        mfe_r=mfe_r,
    )
    return LossDetailRow(
        window_key=window_key,
        window_label=window_label,
        entry_time=format_ts(trade.entry_ts),
        exit_time=format_ts(trade.exit_ts),
        bars_held=bars_held,
        pnl=trade.pnl,
        r_multiple=trade.r_multiple,
        exit_reason=str(trade.exit_reason),
        archetype=archetype,
        distance_bucket=distance_bucket,
        entry_distance_atr=entry_distance_atr,
        mfe_r=mfe_r,
        mae_r=mae_r,
        max_favorable_atr=max_favorable_atr,
        max_adverse_atr=max_adverse_atr,
    )


def classify_loss_archetype(*, exit_reason: str, bars_held: int, mfe_r: Decimal) -> str:
    if exit_reason == "slope_turn_positive":
        return "slope_flip_loss"
    if exit_reason != "stop_loss":
        return f"other_{exit_reason}"
    if bars_held <= 3 and mfe_r < Decimal("0.5"):
        return "instant_rebound_stop"
    if bars_held <= 12 and mfe_r < Decimal("0.5"):
        return "failed_followthrough_stop"
    if mfe_r >= Decimal("1.0"):
        return "gave_back_after_1r"
    if mfe_r >= Decimal("0.5"):
        return "partial_work_then_stop"
    return "late_grind_stop"


def distance_bucket_label(distance_atr: Decimal) -> str:
    if distance_atr <= Decimal("1.0"):
        return "<=1.0 ATR"
    if distance_atr <= Decimal("1.5"):
        return "1.0-1.5 ATR"
    if distance_atr <= Decimal("2.0"):
        return "1.5-2.0 ATR"
    if distance_atr <= Decimal("3.0"):
        return "2.0-3.0 ATR"
    return ">3.0 ATR"


def build_summary_rows(window_key: str, window_label: str, detail_rows: list[LossDetailRow]) -> list[SummaryRow]:
    rows: list[SummaryRow] = []
    rows.extend(summarize_group(window_key, window_label, detail_rows, "archetype", lambda row: row.archetype))
    rows.extend(summarize_group(window_key, window_label, detail_rows, "distance_bucket", lambda row: row.distance_bucket))
    rows.extend(
        summarize_group(
            window_key,
            window_label,
            detail_rows,
            "archetype_x_distance",
            lambda row: f"{row.archetype} | {row.distance_bucket}",
        )
    )
    return rows


def summarize_group(window_key: str, window_label: str, detail_rows: list[LossDetailRow], group_type: str, key_fn) -> list[SummaryRow]:
    groups: dict[str, list[LossDetailRow]] = {}
    total_loss_abs = abs(sum((row.pnl for row in detail_rows), Decimal("0"))) or Decimal("1")
    for row in detail_rows:
        groups.setdefault(key_fn(row), []).append(row)
    summary: list[SummaryRow] = []
    for group_key, rows in sorted(groups.items(), key=lambda item: sum((row.pnl for row in item[1]), Decimal("0"))):
        total_pnl = sum((row.pnl for row in rows), Decimal("0"))
        total_r = sum((row.r_multiple for row in rows), Decimal("0"))
        summary.append(
            SummaryRow(
                window_key=window_key,
                window_label=window_label,
                group_type=group_type,
                group_key=group_key,
                trades=len(rows),
                total_pnl=total_pnl,
                avg_pnl=total_pnl / Decimal(len(rows)),
                avg_r=total_r / Decimal(len(rows)),
                share_of_loss_pct=(abs(total_pnl) / total_loss_abs) * Decimal("100"),
            )
        )
    return summary


def build_findings_for_window(window_label: str, detail_rows: list[LossDetailRow]) -> list[str]:
    findings: list[str] = []
    if not detail_rows:
        return findings
    archetype_summaries = summarize_group("tmp", window_label, detail_rows, "archetype", lambda row: row.archetype)
    distance_summaries = summarize_group("tmp", window_label, detail_rows, "distance_bucket", lambda row: row.distance_bucket)
    combo_summaries = summarize_group("tmp", window_label, detail_rows, "archetype_x_distance", lambda row: f"{row.archetype} | {row.distance_bucket}")
    worst_archetype = archetype_summaries[0]
    worst_distance = distance_summaries[0]
    worst_combo = combo_summaries[0]
    top_losses = detail_rows[:10]
    instant_rebound_far = [
        row for row in detail_rows if row.archetype == "instant_rebound_stop" and row.distance_bucket in {"2.0-3.0 ATR", ">3.0 ATR"}
    ]
    far_quick_fail = [
        row
        for row in detail_rows
        if row.distance_bucket in {"2.0-3.0 ATR", ">3.0 ATR"}
        and row.archetype in {"instant_rebound_stop", "failed_followthrough_stop"}
    ]
    far_giveback = [
        row
        for row in detail_rows
        if row.distance_bucket in {"2.0-3.0 ATR", ">3.0 ATR"}
        and row.archetype in {"gave_back_after_1r", "partial_work_then_stop"}
    ]
    findings.append(
        f"{window_label}: largest loss archetype is {worst_archetype.group_key}, contributing {fmt(abs(worst_archetype.total_pnl), 2)}U ({fmt(worst_archetype.share_of_loss_pct, 2)}%) across {worst_archetype.trades} trades."
    )
    findings.append(
        f"{window_label}: largest distance bucket is {worst_distance.group_key}, contributing {fmt(abs(worst_distance.total_pnl), 2)}U ({fmt(worst_distance.share_of_loss_pct, 2)}%)."
    )
    findings.append(
        f"{window_label}: most damaging combined pattern is {worst_combo.group_key}, contributing {fmt(abs(worst_combo.total_pnl), 2)}U ({fmt(worst_combo.share_of_loss_pct, 2)}%)."
    )
    findings.append(
        f"{window_label}: top 10 worst losses contain {len(instant_rebound_far)} instant rebound stops from far-distance entries (>2 ATR) in the full loss set."
    )
    findings.append(
        f"{window_label}: far-distance quick failures (>2 ATR + immediate/early stop) contribute {fmt(abs(sum((row.pnl for row in far_quick_fail), Decimal('0'))), 2)}U, while far-distance give-back losses contribute {fmt(abs(sum((row.pnl for row in far_giveback), Decimal('0'))), 2)}U."
    )
    return findings


def write_summary_csv(rows: list[SummaryRow]) -> None:
    fieldnames = [
        "window_key",
        "window_label",
        "group_type",
        "group_key",
        "trades",
        "total_pnl",
        "avg_pnl",
        "avg_r",
        "share_of_loss_pct",
    ]
    for path in (SUMMARY_CSV_PATH, LATEST_SUMMARY_CSV_PATH):
        with path.open("w", newline="", encoding="utf-8-sig") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow(
                    {
                        "window_key": row.window_key,
                        "window_label": row.window_label,
                        "group_type": row.group_type,
                        "group_key": row.group_key,
                        "trades": row.trades,
                        "total_pnl": fmt(row.total_pnl, 2),
                        "avg_pnl": fmt(row.avg_pnl, 2),
                        "avg_r": fmt(row.avg_r, 4),
                        "share_of_loss_pct": fmt(row.share_of_loss_pct, 2),
                    }
                )


def write_detail_csv(rows: list[LossDetailRow]) -> None:
    fieldnames = [
        "window_key",
        "window_label",
        "entry_time",
        "exit_time",
        "bars_held",
        "pnl",
        "r_multiple",
        "exit_reason",
        "archetype",
        "distance_bucket",
        "entry_distance_atr",
        "mfe_r",
        "mae_r",
        "max_favorable_atr",
        "max_adverse_atr",
    ]
    for path in (DETAIL_CSV_PATH, LATEST_DETAIL_CSV_PATH):
        with path.open("w", newline="", encoding="utf-8-sig") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow(
                    {
                        "window_key": row.window_key,
                        "window_label": row.window_label,
                        "entry_time": row.entry_time,
                        "exit_time": row.exit_time,
                        "bars_held": row.bars_held,
                        "pnl": fmt(row.pnl, 2),
                        "r_multiple": fmt(row.r_multiple, 4),
                        "exit_reason": row.exit_reason,
                        "archetype": row.archetype,
                        "distance_bucket": row.distance_bucket,
                        "entry_distance_atr": fmt(row.entry_distance_atr, 4),
                        "mfe_r": fmt(row.mfe_r, 4),
                        "mae_r": fmt(row.mae_r, 4),
                        "max_favorable_atr": fmt(row.max_favorable_atr, 4),
                        "max_adverse_atr": fmt(row.max_adverse_atr, 4),
                    }
                )


def build_html(summary_rows: list[SummaryRow], detail_rows: list[LossDetailRow], findings: list[str]) -> str:
    finding_items = "".join(f"<li>{html.escape(item)}</li>" for item in findings)
    sections: list[str] = []
    for window_key, window_label, _ in WINDOWS:
        window_summary = [row for row in summary_rows if row.window_key == window_key]
        window_details = [row for row in detail_rows if row.window_key == window_key]
        sections.append(
            f"<section class='panel'><h2>{html.escape(window_label)} | Loss Archetypes</h2>{build_summary_table(window_summary, 'archetype')}</section>"
        )
        sections.append(
            f"<section class='panel'><h2>{html.escape(window_label)} | Distance Buckets</h2>{build_summary_table(window_summary, 'distance_bucket')}</section>"
        )
        sections.append(
            f"<section class='panel'><h2>{html.escape(window_label)} | Archetype x Distance</h2>{build_summary_table(window_summary, 'archetype_x_distance', limit=12)}</section>"
        )
        sections.append(
            f"<section class='panel'><h2>{html.escape(window_label)} | Worst Losses</h2>{build_detail_table(window_details[:20])}</section>"
        )
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>BTC S097 Loss Archetype Report</title>
  <style>
    body {{ margin: 0; font-family: "Segoe UI", "Microsoft YaHei", sans-serif; color: #1f2937; background: #f7f8fb; }}
    .wrap {{ max-width: 1320px; margin: 0 auto; padding: 28px 20px 56px; }}
    .panel {{ background: #fff; border: 1px solid #d9e2ec; border-radius: 8px; padding: 18px 20px; margin-bottom: 16px; }}
    h1 {{ margin: 0 0 8px; font-size: 28px; }}
    h2 {{ margin: 0 0 10px; font-size: 19px; }}
    p, li {{ line-height: 1.55; }}
    .muted {{ color: #667085; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 12.5px; }}
    th, td {{ padding: 9px 7px; border-bottom: 1px solid #e6edf5; text-align: right; vertical-align: top; }}
    th:first-child, td:first-child, th:nth-child(2), td:nth-child(2), th:last-child, td:last-child {{ text-align: left; }}
    th {{ color: #667085; font-weight: 700; background: #f2f6fb; }}
    .bad {{ color: #b42318; font-weight: 700; }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="panel">
      <h1>BTC S097 Loss Archetype Report</h1>
      <p class="muted">Generated at {html.escape(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))}. Research-only study on losing trades for the baseline S097 configuration.</p>
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


def build_summary_table(rows: list[SummaryRow], group_type: str, *, limit: int | None = None) -> str:
    subset = [row for row in rows if row.group_type == group_type]
    if limit is not None:
        subset = subset[:limit]
    body = []
    for row in subset:
        body.append(
            "<tr>"
            f"<td>{html.escape(row.group_key)}</td>"
            f"<td>{row.trades}</td>"
            f"<td class='bad'>{fmt(row.total_pnl, 2)}</td>"
            f"<td>{fmt(row.avg_pnl, 2)}</td>"
            f"<td>{fmt(row.avg_r, 4)}</td>"
            f"<td>{fmt(row.share_of_loss_pct, 2)}%</td>"
            "</tr>"
        )
    return (
        "<table><thead><tr>"
        "<th>Group</th><th>Trades</th><th>Total PnL</th><th>Avg PnL</th><th>Avg R</th><th>Loss Share</th>"
        "</tr></thead>"
        f"<tbody>{''.join(body)}</tbody></table>"
    )


def build_detail_table(rows: list[LossDetailRow]) -> str:
    body = []
    for row in rows:
        body.append(
            "<tr>"
            f"<td>{html.escape(row.entry_time)}</td>"
            f"<td>{html.escape(row.exit_time)}</td>"
            f"<td>{row.bars_held}</td>"
            f"<td class='bad'>{fmt(row.pnl, 2)}</td>"
            f"<td>{fmt(row.r_multiple, 4)}</td>"
            f"<td>{html.escape(row.archetype)}</td>"
            f"<td>{html.escape(row.distance_bucket)}</td>"
            f"<td>{fmt(row.entry_distance_atr, 4)}</td>"
            f"<td>{fmt(row.mfe_r, 4)}</td>"
            f"<td>{fmt(row.mae_r, 4)}</td>"
            f"<td>{html.escape(row.exit_reason)}</td>"
            "</tr>"
        )
    return (
        "<table><thead><tr>"
        "<th>Entry</th><th>Exit</th><th>Bars</th><th>PnL</th><th>R</th><th>Archetype</th>"
        "<th>Distance</th><th>Entry Dist ATR</th><th>MFE R</th><th>MAE R</th><th>Exit Reason</th>"
        "</tr></thead>"
        f"<tbody>{''.join(body)}</tbody></table>"
    )


def fmt(value: Decimal, places: int) -> str:
    quant = Decimal(1).scaleb(-places)
    return f"{value.quantize(quant):f}"


def format_ts(ts: int) -> str:
    return datetime.fromtimestamp(ts / 1000).strftime("%Y-%m-%d %H:%M")


if __name__ == "__main__":
    main()
