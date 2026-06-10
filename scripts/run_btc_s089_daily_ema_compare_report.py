from __future__ import annotations

import csv
import html
import sys
from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from okx_quant.backtest import BacktestResult, run_backtest, summarize_trade_exit_reasons
from okx_quant.models import StrategyConfig
from okx_quant.okx_client import OkxRestClient
from okx_quant.strategy_catalog import STRATEGY_BTC_EMA55_SLOPE_SHORT_ID
from okx_quant.strategy_symbol_defaults import get_strategy_symbol_parameter_defaults


SYMBOL = "BTC-USDT-SWAP"
BAR = "1H"
INITIAL_CAPITAL = Decimal("10000")
RISK_AMOUNT = Decimal("100")
SHORT_TAKER_FEE_RATE = Decimal("0.00036")

REPORT_DIR = ROOT / "reports"
REPORT_DIR.mkdir(parents=True, exist_ok=True)
STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
BASENAME = f"btc_s089_daily_ema_compare_{STAMP}"
HTML_PATH = REPORT_DIR / f"{BASENAME}.html"
CSV_PATH = REPORT_DIR / f"{BASENAME}.csv"
LATEST_HTML_PATH = REPORT_DIR / "btc_s089_daily_ema_compare_latest.html"
LATEST_CSV_PATH = REPORT_DIR / "btc_s089_daily_ema_compare_latest.csv"


@dataclass(frozen=True)
class Variant:
    key: str
    label: str
    daily_filter_enabled: bool
    daily_filter_period: int


@dataclass(frozen=True)
class WindowSpec:
    key: str
    label: str
    candle_limit: int


@dataclass(frozen=True)
class Row:
    window_key: str
    window_label: str
    variant_key: str
    variant_label: str
    candle_count: int
    data_start: str
    data_end: str
    trades: int
    wins: int
    losses: int
    breakeven: int
    win_rate_pct: Decimal
    total_pnl: Decimal
    return_pct: Decimal
    ending_equity: Decimal
    max_drawdown: Decimal
    max_drawdown_pct: Decimal
    profit_factor: Decimal | None
    avg_r: Decimal
    avg_pnl: Decimal
    average_win: Decimal
    average_loss: Decimal
    longest_losing_streak: int
    stop_loss_hits: int
    exit_summary: str
    daily_filter_summary: str


VARIANTS = (
    Variant(
        key="baseline",
        label="Current S089",
        daily_filter_enabled=False,
        daily_filter_period=0,
    ),
    Variant(
        key="daily_ema21",
        label="S089 + Daily EMA21",
        daily_filter_enabled=True,
        daily_filter_period=21,
    ),
    Variant(
        key="daily_ema13",
        label="S089 + Daily EMA13",
        daily_filter_enabled=True,
        daily_filter_period=13,
    ),
)

WINDOWS = (
    WindowSpec(key="recent_10000", label="Recent 10000 bars", candle_limit=10_000),
    WindowSpec(key="full_history", label="Full history", candle_limit=0),
)


def main() -> None:
    client = OkxRestClient()
    rows: list[Row] = []

    base_config = build_base_config()
    for window in WINDOWS:
        for variant in VARIANTS:
            config = apply_variant(base_config, variant)
            result = run_backtest(
                client,
                config,
                candle_limit=window.candle_limit,
                taker_fee_rate=SHORT_TAKER_FEE_RATE,
            )
            rows.append(build_row(window, variant, result))

    write_csv(rows)
    html_text = build_html(base_config, rows)
    HTML_PATH.write_text(html_text, encoding="utf-8")
    LATEST_HTML_PATH.write_text(html_text, encoding="utf-8")

    print(HTML_PATH)
    print(CSV_PATH)


def build_base_config() -> StrategyConfig:
    defaults = get_strategy_symbol_parameter_defaults(
        STRATEGY_BTC_EMA55_SLOPE_SHORT_ID,
        SYMBOL,
        "backtest",
    )
    return StrategyConfig(
        inst_id=SYMBOL,
        bar=str(defaults.get("bar", BAR)),
        ema_type=str(defaults.get("ema_type", "ema")),
        ema_period=int(defaults.get("ema_period", 55)),
        trend_ema_type=str(defaults.get("trend_ema_type", "ema")),
        trend_ema_period=int(defaults.get("trend_ema_period", 55)),
        big_ema_period=233,
        atr_period=int(defaults.get("atr_period", 14)),
        atr_stop_multiplier=Decimal(str(defaults.get("atr_stop_multiplier", "1.5"))),
        atr_take_multiplier=Decimal("4"),
        order_size=Decimal("0"),
        trade_mode="cross",
        signal_mode="short_only",
        position_mode="net",
        environment="demo",
        tp_sl_trigger_type="mark",
        strategy_id=STRATEGY_BTC_EMA55_SLOPE_SHORT_ID,
        take_profit_mode="dynamic",
        risk_amount=RISK_AMOUNT,
        backtest_initial_capital=INITIAL_CAPITAL,
        backtest_sizing_mode="fixed_risk",
        backtest_compounding=False,
        trend_ema_slope_filter_min_ratio=Decimal(str(defaults.get("trend_ema_slope_filter_min_ratio", "-0.0005"))),
        ema55_slope_negative_entry_bars=int(defaults.get("ema55_slope_negative_entry_bars", 1)),
        ema55_slope_exit_enabled=bool(defaults.get("ema55_slope_exit_enabled", True)),
        ema55_slope_lock_profit_enabled=bool(defaults.get("ema55_slope_lock_profit_enabled", True)),
        ema55_slope_lock_profit_trigger_r=int(defaults.get("ema55_slope_lock_profit_trigger_r", 2)),
        backtest_entry_slippage_rate=Decimal("0"),
        backtest_exit_slippage_rate=Decimal("0"),
        backtest_funding_rate=Decimal("0"),
        daily_filter_enabled=False,
        daily_filter_bar="1D",
        daily_filter_boundary="bjt_08",
        daily_filter_mode="disabled",
        daily_filter_scope="short_only",
        daily_filter_ma_type="ema",
        daily_filter_period=21,
    )


def apply_variant(base_config: StrategyConfig, variant: Variant) -> StrategyConfig:
    if not variant.daily_filter_enabled:
        return replace(
            base_config,
            daily_filter_enabled=False,
            daily_filter_mode="disabled",
        )
    return replace(
        base_config,
        daily_filter_enabled=True,
        daily_filter_mode="close_vs_ma",
        daily_filter_period=variant.daily_filter_period,
    )


def build_row(window: WindowSpec, variant: Variant, result: BacktestResult) -> Row:
    report = result.report
    trades = result.trades
    candles = result.candles
    return Row(
        window_key=window.key,
        window_label=window.label,
        variant_key=variant.key,
        variant_label=variant.label,
        candle_count=len(candles),
        data_start=format_ts(candles[0].ts) if candles else "-",
        data_end=format_ts(candles[-1].ts) if candles else "-",
        trades=report.total_trades,
        wins=report.win_trades,
        losses=report.loss_trades,
        breakeven=report.breakeven_trades,
        win_rate_pct=report.win_rate,
        total_pnl=report.total_pnl,
        return_pct=report.total_return_pct,
        ending_equity=report.ending_equity,
        max_drawdown=report.max_drawdown,
        max_drawdown_pct=report.max_drawdown_pct,
        profit_factor=report.profit_factor,
        avg_r=report.average_r_multiple,
        avg_pnl=report.average_pnl,
        average_win=report.average_win,
        average_loss=report.average_loss,
        longest_losing_streak=longest_losing_streak(trades),
        stop_loss_hits=report.stop_loss_hits,
        exit_summary=" / ".join(f"{label}:{count}" for label, count in summarize_trade_exit_reasons(trades)),
        daily_filter_summary=result.daily_filter_mode if result.daily_filter_enabled else "disabled",
    )


def longest_losing_streak(trades: list[object]) -> int:
    best = 0
    current = 0
    for trade in trades:
        if getattr(trade, "pnl", Decimal("0")) < 0:
            current += 1
            best = max(best, current)
        else:
            current = 0
    return best


def write_csv(rows: list[Row]) -> None:
    fieldnames = [
        "window_key",
        "window_label",
        "variant_key",
        "variant_label",
        "candle_count",
        "data_start",
        "data_end",
        "trades",
        "wins",
        "losses",
        "breakeven",
        "win_rate_pct",
        "total_pnl",
        "return_pct",
        "ending_equity",
        "max_drawdown",
        "max_drawdown_pct",
        "profit_factor",
        "avg_r",
        "avg_pnl",
        "average_win",
        "average_loss",
        "longest_losing_streak",
        "stop_loss_hits",
        "exit_summary",
    ]
    for path in (CSV_PATH, LATEST_CSV_PATH):
        with path.open("w", newline="", encoding="utf-8-sig") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow(
                    {
                        "window_key": row.window_key,
                        "window_label": row.window_label,
                        "variant_key": row.variant_key,
                        "variant_label": row.variant_label,
                        "candle_count": row.candle_count,
                        "data_start": row.data_start,
                        "data_end": row.data_end,
                        "trades": row.trades,
                        "wins": row.wins,
                        "losses": row.losses,
                        "breakeven": row.breakeven,
                        "win_rate_pct": fmt(row.win_rate_pct, 2),
                        "total_pnl": fmt(row.total_pnl, 2),
                        "return_pct": fmt(row.return_pct, 2),
                        "ending_equity": fmt(row.ending_equity, 2),
                        "max_drawdown": fmt(row.max_drawdown, 2),
                        "max_drawdown_pct": fmt(row.max_drawdown_pct, 2),
                        "profit_factor": "" if row.profit_factor is None else fmt(row.profit_factor, 2),
                        "avg_r": fmt(row.avg_r, 3),
                        "avg_pnl": fmt(row.avg_pnl, 2),
                        "average_win": fmt(row.average_win, 2),
                        "average_loss": fmt(row.average_loss, 2),
                        "longest_losing_streak": row.longest_losing_streak,
                        "stop_loss_hits": row.stop_loss_hits,
                        "exit_summary": row.exit_summary,
                    }
                )


def build_html(base_config: StrategyConfig, rows: list[Row]) -> str:
    recent_rows = [row for row in rows if row.window_key == "recent_10000"]
    full_rows = [row for row in rows if row.window_key == "full_history"]

    findings = build_findings(recent_rows, full_rows)
    recent_table = build_table(recent_rows)
    full_table = build_table(full_rows)

    assumptions = [
        f"S089 baseline assumption: current `{STRATEGY_BTC_EMA55_SLOPE_SHORT_ID}` BTC default backtest config",
        f"Symbol: {SYMBOL} | Bar: {base_config.bar} | Initial capital: {fmt(INITIAL_CAPITAL, 0)}U | Fixed risk: {fmt(RISK_AMOUNT, 0)}U",
        f"EMA: {base_config.ema_type.upper()}{base_config.ema_period} | ATR period: {base_config.atr_period} | ATR stop: {fmt(base_config.atr_stop_multiplier, 2)}",
        f"Slope threshold: {fmt(base_config.trend_ema_slope_filter_min_ratio, 4)} | Negative bars: {base_config.ema55_slope_negative_entry_bars}",
        f"Exit: slope turn positive={'on' if base_config.ema55_slope_exit_enabled else 'off'} | lock profit={'on' if base_config.ema55_slope_lock_profit_enabled else 'off'} @ {base_config.ema55_slope_lock_profit_trigger_r}R",
        f"Cost model: short taker fee {fmt(SHORT_TAKER_FEE_RATE * Decimal('100'), 3)}% | slippage 0 | funding 0",
        "Daily filter variants: previous closed daily close < daily EMA13 / EMA21, boundary=BJT 08:00, scope=short_only",
    ]

    finding_items = "".join(f"<li>{html.escape(item)}</li>" for item in findings)
    assumption_items = "".join(f"<li>{html.escape(item)}</li>" for item in assumptions)

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>BTC S089 Daily EMA Filter Compare</title>
  <style>
    :root {{
      --bg: #f5f1ea;
      --panel: #fffdfa;
      --ink: #1d2a34;
      --muted: #61707d;
      --line: #dccfbf;
      --good: #0f766e;
      --bad: #b45309;
      --accent: #8f3b24;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Segoe UI", "PingFang SC", "Microsoft YaHei", sans-serif;
      color: var(--ink);
      background: linear-gradient(180deg, #f7f4ee 0%, var(--bg) 100%);
    }}
    .wrap {{
      max-width: 1240px;
      margin: 0 auto;
      padding: 28px 20px 56px;
    }}
    .hero, .panel {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 20px 22px;
      box-shadow: 0 10px 30px rgba(29, 42, 52, 0.06);
      margin-bottom: 18px;
    }}
    h1, h2 {{ margin: 0 0 10px; }}
    h1 {{ font-size: 30px; }}
    h2 {{ font-size: 20px; }}
    p, li {{ line-height: 1.55; }}
    .muted {{ color: var(--muted); }}
    .chips {{ display: flex; flex-wrap: wrap; gap: 8px; margin-top: 10px; }}
    .chip {{
      padding: 6px 10px;
      border-radius: 999px;
      background: #f3e8da;
      color: var(--accent);
      font-size: 12px;
      font-weight: 600;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
    }}
    th, td {{
      padding: 10px 8px;
      border-bottom: 1px solid #eee4d7;
      text-align: right;
      vertical-align: top;
    }}
    th:first-child, td:first-child,
    th:nth-child(2), td:nth-child(2) {{
      text-align: left;
    }}
    th {{
      color: var(--muted);
      font-weight: 700;
      background: #fbf7f0;
    }}
    .good {{ color: var(--good); font-weight: 700; }}
    .bad {{ color: var(--bad); font-weight: 700; }}
    code {{
      font-family: Consolas, "SFMono-Regular", monospace;
      background: #f5ede2;
      padding: 1px 4px;
      border-radius: 4px;
    }}
    ul {{
      margin: 8px 0 0;
      padding-left: 18px;
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <div class="chip">BTC slope short compare</div>
      <h1>S089 vs Daily EMA Filters</h1>
      <p class="muted">Generated at {html.escape(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))}. This report compares the current BTC slope-short baseline with two daily bias gates: <code>EMA21</code> and <code>EMA13</code>.</p>
      <div class="chips">
        <div class="chip">Baseline: Current S089</div>
        <div class="chip">Variant A: Daily EMA21</div>
        <div class="chip">Variant B: Daily EMA13</div>
      </div>
    </section>

    <section class="panel">
      <h2>Assumptions</h2>
      <ul>{assumption_items}</ul>
    </section>

    <section class="panel">
      <h2>Findings</h2>
      <ul>{finding_items}</ul>
    </section>

    <section class="panel">
      <h2>Recent 10000 Bars</h2>
      {recent_table}
    </section>

    <section class="panel">
      <h2>Full History</h2>
      {full_table}
    </section>
  </div>
</body>
</html>
"""


def build_table(rows: list[Row]) -> str:
    ordered = sorted(rows, key=lambda row: row.return_pct, reverse=True)
    body = []
    for row in ordered:
        body.append(
            "<tr>"
            f"<td>{html.escape(row.variant_label)}</td>"
            f"<td>{html.escape(row.data_start)}<br><span class='muted'>{html.escape(row.data_end)}</span></td>"
            f"<td>{row.candle_count}</td>"
            f"<td>{row.trades}</td>"
            f"<td>{row.wins}/{row.losses}/{row.breakeven}</td>"
            f"<td>{fmt(row.win_rate_pct, 2)}%</td>"
            f"<td class='{metric_class(row.total_pnl)}'>{fmt(row.total_pnl, 2)}</td>"
            f"<td class='{metric_class(row.return_pct)}'>{fmt(row.return_pct, 2)}%</td>"
            f"<td>{fmt(row.max_drawdown, 2)}U / {fmt(row.max_drawdown_pct, 2)}%</td>"
            f"<td>{'-' if row.profit_factor is None else fmt(row.profit_factor, 2)}</td>"
            f"<td>{fmt(row.avg_r, 3)}</td>"
            f"<td>{row.longest_losing_streak}</td>"
            f"<td>{html.escape(row.exit_summary)}</td>"
            "</tr>"
        )
    return (
        "<table>"
        "<thead><tr>"
        "<th>Variant</th>"
        "<th>Range</th>"
        "<th>Candles</th>"
        "<th>Trades</th>"
        "<th>W/L/BE</th>"
        "<th>Win Rate</th>"
        "<th>Total PnL</th>"
        "<th>Return</th>"
        "<th>Max DD</th>"
        "<th>PF</th>"
        "<th>Avg R</th>"
        "<th>LLS</th>"
        "<th>Exit Summary</th>"
        "</tr></thead>"
        f"<tbody>{''.join(body)}</tbody>"
        "</table>"
    )


def build_findings(recent_rows: list[Row], full_rows: list[Row]) -> list[str]:
    findings: list[str] = []
    if recent_rows:
        best_recent = max(recent_rows, key=lambda row: row.return_pct)
        lowest_recent_dd = min(recent_rows, key=lambda row: row.max_drawdown_pct)
        findings.append(
            f"Recent 10000 bars best return: {best_recent.variant_label} ({fmt(best_recent.return_pct, 2)}%, PF {fmt_or_dash(best_recent.profit_factor, 2)}, DD {fmt(best_recent.max_drawdown_pct, 2)}%)."
        )
        findings.append(
            f"Recent 10000 bars lowest drawdown: {lowest_recent_dd.variant_label} (DD {fmt(lowest_recent_dd.max_drawdown_pct, 2)}%, return {fmt(lowest_recent_dd.return_pct, 2)}%)."
        )
    if full_rows:
        best_full = max(full_rows, key=lambda row: row.return_pct)
        lowest_full_dd = min(full_rows, key=lambda row: row.max_drawdown_pct)
        best_full_pf = max(
            full_rows,
            key=lambda row: row.profit_factor if row.profit_factor is not None else Decimal("-1"),
        )
        findings.append(
            f"Full history best return: {best_full.variant_label} ({fmt(best_full.return_pct, 2)}%, PF {fmt_or_dash(best_full.profit_factor, 2)}, DD {fmt(best_full.max_drawdown_pct, 2)}%)."
        )
        findings.append(
            f"Full history lowest drawdown: {lowest_full_dd.variant_label} (DD {fmt(lowest_full_dd.max_drawdown_pct, 2)}%, return {fmt(lowest_full_dd.return_pct, 2)}%)."
        )
        findings.append(
            f"Full history best PF: {best_full_pf.variant_label} (PF {fmt_or_dash(best_full_pf.profit_factor, 2)}, trades {best_full_pf.trades}, return {fmt(best_full_pf.return_pct, 2)}%)."
        )
        recommendation = recommend_variant(full_rows)
        findings.append(recommendation)
    return findings


def recommend_variant(full_rows: list[Row]) -> str:
    scored = sorted(
        full_rows,
        key=lambda row: (
            row.return_pct - (row.max_drawdown_pct * Decimal("0.6")) + ((row.profit_factor or Decimal("0")) * Decimal("5")),
            -row.longest_losing_streak,
        ),
        reverse=True,
    )
    pick = scored[0]
    return (
        f"Risk-adjusted pick from full history: {pick.variant_label}. "
        f"It combines return {fmt(pick.return_pct, 2)}%, drawdown {fmt(pick.max_drawdown_pct, 2)}%, "
        f"PF {fmt_or_dash(pick.profit_factor, 2)}, and longest losing streak {pick.longest_losing_streak}."
    )


def metric_class(value: Decimal) -> str:
    return "good" if value > 0 else ("bad" if value < 0 else "")


def fmt(value: Decimal, places: int) -> str:
    quant = Decimal("1") if places == 0 else Decimal("1").scaleb(-places)
    return format(value.quantize(quant), "f")


def fmt_or_dash(value: Decimal | None, places: int) -> str:
    if value is None:
        return "-"
    return fmt(value, places)


def format_ts(ts: int) -> str:
    return datetime.utcfromtimestamp(ts / 1000).strftime("%Y-%m-%d %H:%M")


if __name__ == "__main__":
    main()
