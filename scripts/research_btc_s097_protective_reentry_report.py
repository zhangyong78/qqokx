from __future__ import annotations

import csv
import html
import importlib.util
import sys
from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


MODULE_PATH = ROOT / "scripts" / "research_btc_s096_s097_distance_confirmation_compare.py"
SPEC = importlib.util.spec_from_file_location("distance_research_module_protective_reentry", MODULE_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"Unable to load research module: {MODULE_PATH}")
RESEARCH = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = RESEARCH
SPEC.loader.exec_module(RESEARCH)

from okx_quant.backtest import BacktestResult, BacktestTrade, _run_backtest_with_loaded_data


REPORT_DIR = ROOT / "reports"
REPORT_DIR.mkdir(parents=True, exist_ok=True)
STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
BASENAME = f"btc_s097_protective_reentry_report_{STAMP}"
HTML_PATH = REPORT_DIR / f"{BASENAME}.html"
PERFORMANCE_CSV_PATH = REPORT_DIR / f"{BASENAME}_performance.csv"
CONTRIBUTION_CSV_PATH = REPORT_DIR / f"{BASENAME}_contribution.csv"
LATEST_HTML_PATH = REPORT_DIR / "btc_s097_protective_reentry_report_latest.html"
LATEST_PERFORMANCE_CSV_PATH = REPORT_DIR / "btc_s097_protective_reentry_report_latest_performance.csv"
LATEST_CONTRIBUTION_CSV_PATH = REPORT_DIR / "btc_s097_protective_reentry_report_latest_contribution.csv"

SNAPSHOT_ID = "S097"
WINDOWS = (
    ("recent_10000", "Recent 10000 bars", 10_000),
    ("full_history", "Full history", 0),
)


@dataclass(frozen=True)
class Variant:
    key: str
    label: str
    config_updates: dict[str, Any]


@dataclass(frozen=True)
class PerformanceRow:
    window_key: str
    window_label: str
    variant_key: str
    variant_label: str
    trades: int
    wins: int
    losses: int
    win_rate_pct: Decimal
    total_pnl: Decimal
    return_pct: Decimal
    max_drawdown_pct: Decimal
    profit_factor: Decimal | None
    avg_r: Decimal
    stop_loss_hits: int
    total_fees: Decimal
    positive_months: int
    negative_months: int
    worst_month: str


@dataclass(frozen=True)
class ContributionRow:
    window_key: str
    window_label: str
    prev_exit_group: str
    trades: int
    wins: int
    win_rate_pct: Decimal
    total_pnl: Decimal
    avg_pnl: Decimal
    avg_r: Decimal
    share_of_total_pnl_pct: Decimal


VARIANTS = (
    Variant("baseline", "Baseline", {}),
    Variant("same_bar_block", "Block same-bar reentry", {"ema55_slope_same_bar_reentry_block": True}),
    Variant(
        "protect_exit_bear_bar",
        "After protected exit: require bearish bar",
        {"ema55_slope_dynamic_exit_requires_bear_reentry": True},
    ),
    Variant(
        "protect_exit_bear_break_prev_low",
        "After protected exit: bearish bar + close below prev low",
        {
            "ema55_slope_dynamic_exit_requires_bear_reentry": True,
            "ema55_slope_dynamic_exit_bear_reentry_break_prev_low": True,
        },
    ),
    Variant(
        "protect_exit_ema_reclaim",
        "After protected exit: wait EMA55 reclaim then rebreak",
        {"ema55_slope_dynamic_exit_requires_ema_reclaim": True},
    ),
    Variant(
        "locked_exit_ema21_near",
        "After locked exit: wait near EMA21 then rebreak",
        {
            "ema55_slope_locked_reentry_requires_ema21_near": True,
            "ema55_slope_locked_reentry_min_r": 3,
            "ema55_slope_locked_reentry_max_r": 13,
        },
    ),
)


def main() -> None:
    snapshots = RESEARCH.load_snapshots()
    base_config = RESEARCH.config_from_snapshot(snapshots[SNAPSHOT_ID]["config"])
    maker_fee_rate = Decimal(str(snapshots[SNAPSHOT_ID].get("maker_fee_rate", "0")))
    taker_fee_rate = Decimal(str(snapshots[SNAPSHOT_ID].get("taker_fee_rate", "0")))
    client = RESEARCH.OkxRestClient()
    instrument = client.get_instrument(base_config.inst_id)

    performance_rows: list[PerformanceRow] = []
    contribution_rows: list[ContributionRow] = []
    for window_key, window_label, candle_limit in WINDOWS:
        candles = client.get_candles_history(base_config.inst_id, base_config.bar, limit=candle_limit)
        baseline_result: BacktestResult | None = None
        for variant in VARIANTS:
            config = replace(base_config, **variant.config_updates)
            result = _run_backtest_with_loaded_data(
                candles,
                instrument,
                config,
                maker_fee_rate=maker_fee_rate,
                taker_fee_rate=taker_fee_rate,
            )
            if variant.key == "baseline":
                baseline_result = result
            performance_rows.append(build_performance_row(window_key, window_label, variant, result))
        if baseline_result is not None:
            contribution_rows.extend(build_contribution_rows(window_key, window_label, baseline_result.trades))

    write_performance_csv(performance_rows)
    write_contribution_csv(contribution_rows)
    html_text = build_html(performance_rows, contribution_rows)
    HTML_PATH.write_text(html_text, encoding="utf-8")
    LATEST_HTML_PATH.write_text(html_text, encoding="utf-8")

    print(HTML_PATH)
    print(PERFORMANCE_CSV_PATH)
    print(CONTRIBUTION_CSV_PATH)
    for row in performance_rows:
        print(
            f"{row.window_key},{row.variant_key},trades={row.trades},"
            f"pnl={fmt(row.total_pnl, 2)},dd={fmt(row.max_drawdown_pct, 2)},pf={fmt_or_dash(row.profit_factor, 4)}"
        )


def build_performance_row(window_key: str, window_label: str, variant: Variant, result: BacktestResult) -> PerformanceRow:
    report = result.report
    monthly = result.monthly_stats
    positive_months = sum(1 for stat in monthly if stat.total_pnl > 0)
    negative_months = sum(1 for stat in monthly if stat.total_pnl < 0)
    worst_month = min(monthly, key=lambda stat: stat.total_pnl) if monthly else None
    return PerformanceRow(
        window_key=window_key,
        window_label=window_label,
        variant_key=variant.key,
        variant_label=variant.label,
        trades=report.total_trades,
        wins=report.win_trades,
        losses=report.loss_trades,
        win_rate_pct=report.win_rate,
        total_pnl=report.total_pnl,
        return_pct=report.total_return_pct,
        max_drawdown_pct=report.max_drawdown_pct,
        profit_factor=report.profit_factor,
        avg_r=report.average_r_multiple,
        stop_loss_hits=report.stop_loss_hits,
        total_fees=report.total_fees,
        positive_months=positive_months,
        negative_months=negative_months,
        worst_month="-" if worst_month is None else f"{worst_month.period_label} {fmt(worst_month.total_pnl, 2)}",
    )


def build_contribution_rows(window_key: str, window_label: str, trades: list[BacktestTrade]) -> list[ContributionRow]:
    groups: dict[str, list[BacktestTrade]] = {}
    previous_trade: BacktestTrade | None = None
    for trade in sorted(trades, key=lambda item: (item.entry_index, item.exit_index)):
        group_key = previous_exit_group(previous_trade)
        groups.setdefault(group_key, []).append(trade)
        previous_trade = trade
    total_pnl = sum((trade.pnl for trade in trades), Decimal("0")) or Decimal("1")
    rows: list[ContributionRow] = []
    for group_key, group_trades in sorted(groups.items(), key=lambda item: sum((trade.pnl for trade in item[1]), Decimal("0")), reverse=True):
        wins = [trade for trade in group_trades if trade.pnl > 0]
        group_pnl = sum((trade.pnl for trade in group_trades), Decimal("0"))
        rows.append(
            ContributionRow(
                window_key=window_key,
                window_label=window_label,
                prev_exit_group=group_key,
                trades=len(group_trades),
                wins=len(wins),
                win_rate_pct=(Decimal(len(wins)) / Decimal(len(group_trades))) * Decimal("100"),
                total_pnl=group_pnl,
                avg_pnl=group_pnl / Decimal(len(group_trades)),
                avg_r=sum((trade.r_multiple for trade in group_trades), Decimal("0")) / Decimal(len(group_trades)),
                share_of_total_pnl_pct=(group_pnl / total_pnl) * Decimal("100"),
            )
        )
    return rows


def previous_exit_group(previous_trade: BacktestTrade | None) -> str:
    if previous_trade is None:
        return "first_trade"
    reason = str(previous_trade.exit_reason)
    if is_protective_exit(reason):
        return "after_protective_exit"
    if reason == "stop_loss":
        return "after_stop_loss"
    if reason == "slope_turn_positive":
        return "after_slope_turn_positive"
    return f"after_{reason}"


def is_protective_exit(exit_reason: str) -> bool:
    return exit_reason == "break_even_stop" or exit_reason.startswith("locked_") or exit_reason.startswith("far_early_lock_")


def write_performance_csv(rows: list[PerformanceRow]) -> None:
    fieldnames = list(PerformanceRow.__dataclass_fields__)
    for path in (PERFORMANCE_CSV_PATH, LATEST_PERFORMANCE_CSV_PATH):
        with path.open("w", newline="", encoding="utf-8-sig") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow(format_dataclass_row(row))


def write_contribution_csv(rows: list[ContributionRow]) -> None:
    fieldnames = list(ContributionRow.__dataclass_fields__)
    for path in (CONTRIBUTION_CSV_PATH, LATEST_CONTRIBUTION_CSV_PATH):
        with path.open("w", newline="", encoding="utf-8-sig") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow(format_dataclass_row(row))


def format_dataclass_row(row: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for key in row.__dataclass_fields__:
        value = getattr(row, key)
        if isinstance(value, Decimal):
            places = 4 if key in {"profit_factor", "avg_r"} else 2
            payload[key] = fmt(value, places)
        else:
            payload[key] = "" if value is None else value
    return payload


def build_html(performance_rows: list[PerformanceRow], contribution_rows: list[ContributionRow]) -> str:
    findings = build_findings(performance_rows, contribution_rows)
    finding_items = "".join(f"<li>{html.escape(item)}</li>" for item in findings)
    sections: list[str] = []
    for window_key, window_label, _ in WINDOWS:
        perf_subset = [row for row in performance_rows if row.window_key == window_key]
        contrib_subset = [row for row in contribution_rows if row.window_key == window_key]
        sections.append(f"<section class='panel'><h2>{html.escape(window_label)} | Reentry Constraint Tests</h2>{build_performance_table(perf_subset)}</section>")
        sections.append(f"<section class='panel'><h2>{html.escape(window_label)} | Baseline Contribution by Previous Exit</h2>{build_contribution_table(contrib_subset)}</section>")
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>BTC S097 Protective Reentry Report</title>
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
    .good {{ color: #047857; font-weight: 700; }}
    .bad {{ color: #b42318; font-weight: 700; }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="panel">
      <h1>BTC S097 Protective Reentry Report</h1>
      <p class="muted">Generated at {html.escape(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))}. Research-only study. Original strategy code and default parameters are untouched.</p>
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


def build_findings(performance_rows: list[PerformanceRow], contribution_rows: list[ContributionRow]) -> list[str]:
    findings: list[str] = []
    for window_key, window_label, _ in WINDOWS:
        perf_subset = [row for row in performance_rows if row.window_key == window_key]
        baseline = next(row for row in perf_subset if row.variant_key == "baseline")
        best_return = max(perf_subset, key=lambda row: (row.return_pct, row.profit_factor or Decimal("-1")))
        lowest_dd = min(perf_subset, key=lambda row: row.max_drawdown_pct)
        contribution_subset = [row for row in contribution_rows if row.window_key == window_key]
        protective = next((row for row in contribution_subset if row.prev_exit_group == "after_protective_exit"), None)
        findings.append(
            f"{window_label}: baseline after-protective-exit trades contribute {fmt(protective.total_pnl, 2) if protective else '0.00'}U across {protective.trades if protective else 0} trades."
        )
        findings.append(
            f"{window_label}: best return variant is {best_return.variant_label} ({fmt(best_return.return_pct, 2)}%, DD {fmt(best_return.max_drawdown_pct, 2)}%, PF {fmt_or_dash(best_return.profit_factor, 4)})."
        )
        findings.append(
            f"{window_label}: lowest drawdown variant is {lowest_dd.variant_label} (DD {fmt(lowest_dd.max_drawdown_pct, 2)}%, return {fmt(lowest_dd.return_pct, 2)}%)."
        )
        for row in perf_subset:
            if row.variant_key == "baseline":
                continue
            findings.append(
                f"{window_label} {row.variant_label}: PnL {fmt(row.total_pnl - baseline.total_pnl, 2, signed=True)}U vs baseline, DD {fmt(row.max_drawdown_pct - baseline.max_drawdown_pct, 2, signed=True)} pct points."
            )
    return findings


def build_performance_table(rows: list[PerformanceRow]) -> str:
    ordered_keys = [variant.key for variant in VARIANTS]
    ordered = sorted(rows, key=lambda row: ordered_keys.index(row.variant_key))
    body = []
    for row in ordered:
        body.append(
            "<tr>"
            f"<td>{html.escape(row.variant_label)}</td>"
            f"<td>{row.trades}</td>"
            f"<td>{row.wins}/{row.losses}</td>"
            f"<td>{fmt(row.win_rate_pct, 2)}%</td>"
            f"<td class='{metric_class(row.total_pnl)}'>{fmt(row.total_pnl, 2)}</td>"
            f"<td>{fmt(row.return_pct, 2)}%</td>"
            f"<td>{fmt(row.max_drawdown_pct, 2)}%</td>"
            f"<td>{fmt_or_dash(row.profit_factor, 4)}</td>"
            f"<td>{row.stop_loss_hits}</td>"
            f"<td>{html.escape(row.worst_month)}</td>"
            "</tr>"
        )
    return (
        "<table><thead><tr>"
        "<th>Variant</th><th>Trades</th><th>W/L</th><th>Win Rate</th><th>Total PnL</th><th>Return</th><th>Max DD</th><th>PF</th><th>SL Hits</th><th>Worst Month</th>"
        "</tr></thead>"
        f"<tbody>{''.join(body)}</tbody></table>"
    )


def build_contribution_table(rows: list[ContributionRow]) -> str:
    body = []
    for row in rows:
        body.append(
            "<tr>"
            f"<td>{html.escape(row.prev_exit_group)}</td>"
            f"<td>{row.trades}</td>"
            f"<td>{row.wins}</td>"
            f"<td>{fmt(row.win_rate_pct, 2)}%</td>"
            f"<td class='{metric_class(row.total_pnl)}'>{fmt(row.total_pnl, 2)}</td>"
            f"<td>{fmt(row.avg_pnl, 2)}</td>"
            f"<td>{fmt(row.avg_r, 4)}</td>"
            f"<td>{fmt(row.share_of_total_pnl_pct, 2)}%</td>"
            "</tr>"
        )
    return (
        "<table><thead><tr>"
        "<th>Previous Exit Group</th><th>Trades</th><th>Wins</th><th>Win Rate</th><th>Total PnL</th><th>Avg PnL</th><th>Avg R</th><th>Share Of Total PnL</th>"
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
