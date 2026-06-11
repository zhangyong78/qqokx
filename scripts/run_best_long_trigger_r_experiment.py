from __future__ import annotations

import csv
import json
import sys
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import okx_quant.backtest as backtest_module
from okx_quant.candle_cache import load_candle_cache
from okx_quant.models import StrategyConfig
from okx_quant.okx_client import OkxRestClient
from okx_quant.persistence import analysis_report_dir_path
from okx_quant.pricing import format_decimal_fixed
from scripts.build_best_parameter_bundle import build_specs
from scripts.run_btc_daily_ma_direction_filter_research import (
    LONG_MAKER_FEE_RATE,
    LONG_TAKER_FEE_RATE,
    SplitMetrics,
    build_metrics,
)


REPORT_DIR = analysis_report_dir_path()
REPORT_DIR.mkdir(parents=True, exist_ok=True)
PROJECT_REPORT_DIR = ROOT / "reports"
PROJECT_REPORT_DIR.mkdir(parents=True, exist_ok=True)

STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
BASE_NAME = f"best_long_trigger_r_experiment_{STAMP}"
HTML_PATH = REPORT_DIR / f"{BASE_NAME}.html"
CSV_PATH = REPORT_DIR / f"{BASE_NAME}.csv"
JSON_PATH = REPORT_DIR / f"{BASE_NAME}.json"
PROJECT_HTML_PATH = PROJECT_REPORT_DIR / "best_long_trigger_r_experiment_latest.html"

TRIGGER_RS = (2, 3, 4, 5, 6, 7, 8)


@dataclass(frozen=True)
class StrategySpec:
    symbol: str
    label: str
    config: StrategyConfig
    core_label: str


@dataclass(frozen=True)
class VariantResult:
    symbol: str
    label: str
    trigger_r: int
    metrics: SplitMetrics
    trade_count: int
    core_label: str
    candle_count: int
    start_ts: int
    end_ts: int


def _html_text(text: str) -> str:
    escaped = (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )
    return "".join(f"&#x{ord(ch):X};" if ord(ch) > 127 else ch for ch in escaped)


def _fmt_decimal(value: Decimal, digits: int = 4) -> str:
    return format_decimal_fixed(value, digits)


def _fmt_pf(value: Decimal | None) -> str:
    if value is None:
        return "-"
    return format_decimal_fixed(value, 4)


@contextmanager
def patched_dynamic_trigger_r(trigger_r: int):
    original = backtest_module._create_open_position

    def wrapped_create_open_position(*args, **kwargs):
        if kwargs.get("dynamic_take_profit_enabled"):
            kwargs.setdefault("next_dynamic_trigger_r", trigger_r)
        return original(*args, **kwargs)

    backtest_module._create_open_position = wrapped_create_open_position
    try:
        yield
    finally:
        backtest_module._create_open_position = original


def load_long_specs() -> tuple[StrategySpec, ...]:
    specs = []
    for bundle_spec in build_specs():
        if bundle_spec.side != "\u505a\u591a":
            continue
        specs.append(
            StrategySpec(
                symbol=bundle_spec.symbol,
                label=bundle_spec.symbol.replace("-USDT-SWAP", ""),
                config=bundle_spec.config,
                core_label=bundle_spec.core_label,
            )
        )
    return tuple(specs)


def run_variant(
    client: OkxRestClient,
    spec: StrategySpec,
    trigger_r: int,
) -> tuple[VariantResult, list]:
    candles = [candle for candle in load_candle_cache(spec.symbol, spec.config.bar, limit=None) if candle.confirmed]
    if not candles:
        raise RuntimeError(f"missing candles for {spec.symbol} {spec.config.bar}")

    config = replace(
        spec.config,
        environment="demo",
        ema55_slope_lock_profit_trigger_r=trigger_r,
        dynamic_two_r_break_even=True,
        dynamic_fee_offset_enabled=True,
    )

    with patched_dynamic_trigger_r(trigger_r):
        result = backtest_module._run_backtest_with_loaded_data(
            candles,
            client.get_instrument(spec.symbol),
            config,
            data_source_note=f"local candle_cache full history | {spec.symbol} | trigger_r={trigger_r}",
            maker_fee_rate=LONG_MAKER_FEE_RATE,
            taker_fee_rate=LONG_TAKER_FEE_RATE,
        )

    trades = list(result.trades)
    return (
        VariantResult(
            symbol=spec.symbol,
            label=spec.label,
            trigger_r=trigger_r,
            metrics=build_metrics(trades),
            trade_count=len(trades),
            core_label=spec.core_label,
            candle_count=len(candles),
            start_ts=candles[0].ts,
            end_ts=candles[-1].ts,
        ),
        trades,
    )


def build_rows(results: list[VariantResult]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for item in results:
        rows.append(
            {
                "symbol": item.label,
                "trigger_r": item.trigger_r,
                "trades": item.trade_count,
                "pnl_u": float(item.metrics.pnl),
                "return_pct": float(item.metrics.return_pct),
                "win_rate_pct": float(item.metrics.win_rate),
                "profit_factor": None if item.metrics.profit_factor is None else float(item.metrics.profit_factor),
                "avg_r": float(item.metrics.avg_r),
                "max_drawdown_u": float(item.metrics.max_drawdown),
                "core": item.core_label,
                "candle_count": item.candle_count,
                "start_ts": item.start_ts,
                "end_ts": item.end_ts,
            }
        )
    return rows


def aggregate_by_trigger(
    trades_by_variant: dict[tuple[str, int], list],
    specs: tuple[StrategySpec, ...],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for trigger_r in TRIGGER_RS:
        combined = []
        for spec in specs:
            combined.extend(trades_by_variant[(spec.symbol, trigger_r)])
        combined.sort(key=lambda trade: (trade.exit_ts, trade.entry_ts, trade.signal))
        metrics = build_metrics(combined)
        rows.append(
            {
                "trigger_r": trigger_r,
                "trades": len(combined),
                "pnl_u": float(metrics.pnl),
                "return_pct": float(metrics.return_pct),
                "win_rate_pct": float(metrics.win_rate),
                "profit_factor": None if metrics.profit_factor is None else float(metrics.profit_factor),
                "avg_r": float(metrics.avg_r),
                "max_drawdown_u": float(metrics.max_drawdown),
            }
        )
    return rows


def best_row(rows: list[dict[str, object]], key: str) -> dict[str, object]:
    return max(rows, key=lambda row: row[key])


def build_payload(
    per_symbol_rows: list[dict[str, object]],
    aggregate_rows: list[dict[str, object]],
) -> dict[str, object]:
    grouped: dict[str, list[dict[str, object]]] = {}
    for row in per_symbol_rows:
        grouped.setdefault(str(row["symbol"]), []).append(row)
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "trigger_r_values": list(TRIGGER_RS),
        "aggregate_best_by_pnl": best_row(aggregate_rows, "pnl_u"),
        "aggregate_best_by_profit_factor": best_row(
            [row for row in aggregate_rows if row["profit_factor"] is not None],
            "profit_factor",
        ),
        "symbols": {
            symbol: {
                "rows": rows,
                "best_by_pnl": best_row(rows, "pnl_u"),
                "best_by_profit_factor": best_row(
                    [row for row in rows if row["profit_factor"] is not None],
                    "profit_factor",
                ),
            }
            for symbol, rows in grouped.items()
        },
        "aggregate_rows": aggregate_rows,
        "per_symbol_rows": per_symbol_rows,
    }


def write_csv(per_symbol_rows: list[dict[str, object]], aggregate_rows: list[dict[str, object]]) -> None:
    fieldnames = [
        "scope",
        "symbol",
        "trigger_r",
        "trades",
        "pnl_u",
        "return_pct",
        "win_rate_pct",
        "profit_factor",
        "avg_r",
        "max_drawdown_u",
        "core",
        "candle_count",
    ]
    with CSV_PATH.open("w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in per_symbol_rows:
            writer.writerow(
                {
                    "scope": "symbol",
                    "symbol": row["symbol"],
                    "trigger_r": row["trigger_r"],
                    "trades": row["trades"],
                    "pnl_u": row["pnl_u"],
                    "return_pct": row["return_pct"],
                    "win_rate_pct": row["win_rate_pct"],
                    "profit_factor": row["profit_factor"],
                    "avg_r": row["avg_r"],
                    "max_drawdown_u": row["max_drawdown_u"],
                    "core": row["core"],
                    "candle_count": row["candle_count"],
                }
            )
        for row in aggregate_rows:
            writer.writerow(
                {
                    "scope": "aggregate",
                    "symbol": "ALL",
                    "trigger_r": row["trigger_r"],
                    "trades": row["trades"],
                    "pnl_u": row["pnl_u"],
                    "return_pct": row["return_pct"],
                    "win_rate_pct": row["win_rate_pct"],
                    "profit_factor": row["profit_factor"],
                    "avg_r": row["avg_r"],
                    "max_drawdown_u": row["max_drawdown_u"],
                    "core": "",
                    "candle_count": "",
                }
            )


def build_html(per_symbol_rows: list[dict[str, object]], aggregate_rows: list[dict[str, object]]) -> str:
    aggregate_best = best_row(aggregate_rows, "pnl_u")
    aggregate_pf_best = best_row(
        [row for row in aggregate_rows if row["profit_factor"] is not None],
        "profit_factor",
    )

    grouped: dict[str, list[dict[str, object]]] = {}
    for row in per_symbol_rows:
        grouped.setdefault(str(row["symbol"]), []).append(row)

    def render_table(rows: list[dict[str, object]], include_symbol: bool) -> str:
        header = (
            "<tr>"
            + ("<th>Symbol</th>" if include_symbol else "")
            + "<th>Trigger R</th><th>Trades</th><th>PnL</th><th>Return %</th>"
            + "<th>Win %</th><th>PF</th><th>Avg R</th><th>Max DD</th></tr>"
        )
        body_rows = []
        for row in rows:
            parts = ["<tr>"]
            if include_symbol:
                parts.append(f"<td>{_html_text(str(row['symbol']))}</td>")
            parts.extend(
                [
                    f"<td>{row['trigger_r']}</td>",
                    f"<td>{row['trades']}</td>",
                    f"<td>{_html_text(_fmt_decimal(Decimal(str(row['pnl_u']))))}</td>",
                    f"<td>{_html_text(_fmt_decimal(Decimal(str(row['return_pct'])), 2))}</td>",
                    f"<td>{_html_text(_fmt_decimal(Decimal(str(row['win_rate_pct'])), 2))}</td>",
                    f"<td>{_html_text(_fmt_pf(None if row['profit_factor'] is None else Decimal(str(row['profit_factor']))))}</td>",
                    f"<td>{_html_text(_fmt_decimal(Decimal(str(row['avg_r'])), 4))}</td>",
                    f"<td>{_html_text(_fmt_decimal(Decimal(str(row['max_drawdown_u'])), 4))}</td>",
                ]
            )
            parts.append("</tr>")
            body_rows.append("".join(parts))
        return "<table><thead>" + header + "</thead><tbody>" + "".join(body_rows) + "</tbody></table>"

    cards = []
    for symbol, rows in grouped.items():
        rows_sorted = sorted(rows, key=lambda item: int(item["trigger_r"]))
        best_pnl_row = best_row(rows_sorted, "pnl_u")
        best_pf_row = best_row(
            [row for row in rows_sorted if row["profit_factor"] is not None],
            "profit_factor",
        )
        cards.append(
            "<section class=\"card\">"
            f"<h2>{_html_text(symbol)}</h2>"
            f"<p>{_html_text(str(rows_sorted[0]['core']))}</p>"
            f"<p><strong>{_html_text('最佳PnL')}</strong>: R={best_pnl_row['trigger_r']} / {_html_text(_fmt_decimal(Decimal(str(best_pnl_row['pnl_u']))))}U</p>"
            f"<p><strong>{_html_text('最佳PF')}</strong>: R={best_pf_row['trigger_r']} / {_html_text(_fmt_pf(Decimal(str(best_pf_row['profit_factor']))))}</p>"
            f"{render_table(rows_sorted, include_symbol=False)}"
            "</section>"
        )

    generated_at = _html_text(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    trigger_r_labels = _html_text(" / ".join(f"{value}R" for value in TRIGGER_RS))
    html_text = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <title>{_html_text('最佳多头首档R实验')}</title>
  <style>
    :root {{
      --bg: #f6f0e4;
      --panel: #fffaf2;
      --line: #dccfb7;
      --ink: #1f2a30;
      --accent: #0f766e;
      --warm: #a16207;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      padding: 28px;
      background:
        radial-gradient(circle at top right, rgba(15,118,110,0.12), transparent 30%),
        radial-gradient(circle at top left, rgba(161,98,7,0.10), transparent 24%),
        var(--bg);
      color: var(--ink);
      font-family: "Microsoft YaHei UI", "Microsoft YaHei", sans-serif;
      line-height: 1.6;
    }}
    .hero, .card {{
      max-width: 1400px;
      margin: 0 auto 18px auto;
      background: rgba(255, 250, 242, 0.94);
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 22px 24px;
      box-shadow: 0 18px 50px rgba(44, 38, 26, 0.08);
    }}
    h1, h2 {{ margin: 0 0 12px 0; }}
    h1 {{ font-size: 38px; color: #0b5d58; }}
    h2 {{ font-size: 24px; color: #8a4b08; }}
    p {{ margin: 0 0 10px 0; }}
    .meta {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
      gap: 12px;
      margin-top: 14px;
    }}
    .chip {{
      background: #edf7f4;
      border: 1px solid rgba(15,118,110,0.18);
      border-radius: 14px;
      padding: 12px 14px;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 14px;
      margin-top: 10px;
    }}
    th, td {{
      border-bottom: 1px solid var(--line);
      padding: 10px 8px;
      text-align: left;
      vertical-align: top;
    }}
    th {{
      background: rgba(237,247,244,0.9);
      color: #0b5d58;
    }}
  </style>
</head>
<body>
  <section class="hero">
    <h1>{_html_text('最佳多头首档R实验')}</h1>
    <p>{_html_text('只测试最佳参数组合包里的 4 个做多策略；其余参数不变，只比较首档触发R。')}</p>
    <div class="meta">
      <div class="chip"><strong>{_html_text('生成时间')}</strong><br>{generated_at}</div>
      <div class="chip"><strong>{_html_text('实验档位')}</strong><br>{trigger_r_labels}</div>
      <div class="chip"><strong>{_html_text('组合PnL最佳')}</strong><br>{_html_text(f"R={aggregate_best['trigger_r']} / {format_decimal_fixed(Decimal(str(aggregate_best['pnl_u'])), 4)}U")}</div>
      <div class="chip"><strong>{_html_text('组合PF最佳')}</strong><br>{_html_text(f"R={aggregate_pf_best['trigger_r']} / {_fmt_pf(Decimal(str(aggregate_pf_best['profit_factor'])))}")}</div>
    </div>
  </section>
  <section class="card">
    <h2>{_html_text('总组合对比')}</h2>
    {render_table(sorted(aggregate_rows, key=lambda item: int(item['trigger_r'])), include_symbol=False)}
  </section>
  {''.join(cards)}
</body>
</html>
"""
    return html_text


def main() -> None:
    client = OkxRestClient()
    specs = load_long_specs()
    per_symbol_results: list[VariantResult] = []
    trades_by_variant: dict[tuple[str, int], list] = {}

    for spec in specs:
        for trigger_r in TRIGGER_RS:
            print(f"run {spec.label} trigger_r={trigger_r}")
            variant, trades = run_variant(client, spec, trigger_r)
            per_symbol_results.append(variant)
            trades_by_variant[(spec.symbol, trigger_r)] = trades

    per_symbol_rows = build_rows(per_symbol_results)
    aggregate_rows = aggregate_by_trigger(trades_by_variant, specs)
    payload = build_payload(per_symbol_rows, aggregate_rows)

    JSON_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8-sig")
    write_csv(per_symbol_rows, aggregate_rows)
    html_text = build_html(per_symbol_rows, aggregate_rows)
    HTML_PATH.write_text(html_text, encoding="utf-8-sig")
    PROJECT_HTML_PATH.write_text(html_text, encoding="utf-8-sig")

    print(HTML_PATH)
    print(CSV_PATH)
    print(JSON_PATH)


if __name__ == "__main__":
    main()
