from __future__ import annotations

import csv
import json
import sys
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from okx_quant.backtest import _run_backtest_with_loaded_data
from okx_quant.candle_cache import load_candle_cache
from okx_quant.models import StrategyConfig
from okx_quant.okx_client import OkxRestClient
from okx_quant.persistence import analysis_report_dir_path
from okx_quant.pricing import format_decimal_fixed
from scripts.build_best_parameter_bundle import build_specs
from scripts.run_btc_daily_ma_direction_filter_research import SHORT_TAKER_FEE_RATE, SplitMetrics, build_metrics


REPORT_DIR = analysis_report_dir_path()
REPORT_DIR.mkdir(parents=True, exist_ok=True)
PROJECT_REPORT_DIR = ROOT / "reports"
PROJECT_REPORT_DIR.mkdir(parents=True, exist_ok=True)

STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
BASE_NAME = f"best_short_line_experiment_{STAMP}"
HTML_PATH = REPORT_DIR / f"{BASE_NAME}.html"
CSV_PATH = REPORT_DIR / f"{BASE_NAME}.csv"
JSON_PATH = REPORT_DIR / f"{BASE_NAME}.json"
PROJECT_HTML_PATH = PROJECT_REPORT_DIR / "best_short_line_experiment_latest.html"


@dataclass(frozen=True)
class LineVariant:
    key: str
    label: str
    ma_type: str
    period: int
    is_baseline: bool = False


@dataclass(frozen=True)
class StrategySpec:
    symbol: str
    label: str
    config: StrategyConfig
    strategy_label: str
    baseline_core_label: str


@dataclass(frozen=True)
class VariantResult:
    symbol: str
    label: str
    variant_key: str
    variant_label: str
    line_type: str
    line_period: int
    metrics: SplitMetrics
    trade_count: int
    strategy_label: str
    baseline_core_label: str
    candle_count: int


LINE_VARIANTS: tuple[LineVariant, ...] = (
    LineVariant("baseline", "当前定稿", "", 0, True),
    LineVariant("ma55", "MA55", "ma", 55),
    LineVariant("ma34", "MA34", "ma", 34),
    LineVariant("ma21", "MA21", "ma", 21),
    LineVariant("ema34", "EMA34", "ema", 34),
    LineVariant("ema21", "EMA21", "ema", 21),
    LineVariant("ma60", "MA60", "ma", 60),
    LineVariant("ema60", "EMA60", "ema", 60),
)


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


def load_short_specs() -> tuple[StrategySpec, ...]:
    specs = []
    for bundle_spec in build_specs():
        if bundle_spec.side != "\u505a\u7a7a":
            continue
        specs.append(
            StrategySpec(
                symbol=bundle_spec.symbol,
                label=bundle_spec.symbol.replace("-USDT-SWAP", ""),
                config=bundle_spec.config,
                strategy_label=bundle_spec.strategy_label,
                baseline_core_label=bundle_spec.core_label,
            )
        )
    return tuple(specs)


def resolve_variant_config(spec: StrategySpec, variant: LineVariant) -> StrategyConfig:
    if variant.is_baseline:
        return replace(spec.config, environment="demo")
    return replace(
        spec.config,
        environment="demo",
        ema_type=variant.ma_type,
        ema_period=variant.period,
        trend_ema_type=variant.ma_type,
        trend_ema_period=variant.period,
    )


def run_variant(
    client: OkxRestClient,
    spec: StrategySpec,
    variant: LineVariant,
) -> tuple[VariantResult, list]:
    candles = [candle for candle in load_candle_cache(spec.symbol, spec.config.bar, limit=None) if candle.confirmed]
    if not candles:
        raise RuntimeError(f"missing candles for {spec.symbol} {spec.config.bar}")
    config = resolve_variant_config(spec, variant)
    result = _run_backtest_with_loaded_data(
        candles,
        client.get_instrument(spec.symbol),
        config,
        data_source_note=f"local candle_cache full history | {spec.symbol} | line={variant.label}",
        taker_fee_rate=SHORT_TAKER_FEE_RATE,
    )
    trades = list(result.trades)
    return (
        VariantResult(
            symbol=spec.symbol,
            label=spec.label,
            variant_key=variant.key,
            variant_label=variant.label,
            line_type=config.ema_type if not variant.is_baseline else spec.config.ema_type,
            line_period=config.ema_period if not variant.is_baseline else spec.config.ema_period,
            metrics=build_metrics(trades),
            trade_count=len(trades),
            strategy_label=spec.strategy_label,
            baseline_core_label=spec.baseline_core_label,
            candle_count=len(candles),
        ),
        trades,
    )


def build_rows(results: list[VariantResult]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for item in results:
        rows.append(
            {
                "symbol": item.label,
                "variant_key": item.variant_key,
                "variant_label": item.variant_label,
                "line_type": item.line_type,
                "line_period": item.line_period,
                "trades": item.trade_count,
                "pnl_u": float(item.metrics.pnl),
                "return_pct": float(item.metrics.return_pct),
                "win_rate_pct": float(item.metrics.win_rate),
                "profit_factor": None if item.metrics.profit_factor is None else float(item.metrics.profit_factor),
                "avg_r": float(item.metrics.avg_r),
                "max_drawdown_u": float(item.metrics.max_drawdown),
                "strategy": item.strategy_label,
                "baseline_core": item.baseline_core_label,
                "candle_count": item.candle_count,
            }
        )
    return rows


def aggregate_by_variant(
    trades_by_variant: dict[tuple[str, str], list],
    specs: tuple[StrategySpec, ...],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for variant in LINE_VARIANTS:
        combined = []
        for spec in specs:
            combined.extend(trades_by_variant[(spec.symbol, variant.key)])
        combined.sort(key=lambda trade: (trade.exit_ts, trade.entry_ts, trade.signal))
        metrics = build_metrics(combined)
        rows.append(
            {
                "variant_key": variant.key,
                "variant_label": variant.label,
                "line_type": variant.ma_type,
                "line_period": variant.period,
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
        "variants": [
            {
                "key": variant.key,
                "label": variant.label,
                "ma_type": variant.ma_type,
                "period": variant.period,
                "is_baseline": variant.is_baseline,
            }
            for variant in LINE_VARIANTS
        ],
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
        "variant_key",
        "variant_label",
        "line_type",
        "line_period",
        "trades",
        "pnl_u",
        "return_pct",
        "win_rate_pct",
        "profit_factor",
        "avg_r",
        "max_drawdown_u",
        "strategy",
        "baseline_core",
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
                    "variant_key": row["variant_key"],
                    "variant_label": row["variant_label"],
                    "line_type": row["line_type"],
                    "line_period": row["line_period"],
                    "trades": row["trades"],
                    "pnl_u": row["pnl_u"],
                    "return_pct": row["return_pct"],
                    "win_rate_pct": row["win_rate_pct"],
                    "profit_factor": row["profit_factor"],
                    "avg_r": row["avg_r"],
                    "max_drawdown_u": row["max_drawdown_u"],
                    "strategy": row["strategy"],
                    "baseline_core": row["baseline_core"],
                    "candle_count": row["candle_count"],
                }
            )
        for row in aggregate_rows:
            writer.writerow(
                {
                    "scope": "aggregate",
                    "symbol": "ALL",
                    "variant_key": row["variant_key"],
                    "variant_label": row["variant_label"],
                    "line_type": row["line_type"],
                    "line_period": row["line_period"],
                    "trades": row["trades"],
                    "pnl_u": row["pnl_u"],
                    "return_pct": row["return_pct"],
                    "win_rate_pct": row["win_rate_pct"],
                    "profit_factor": row["profit_factor"],
                    "avg_r": row["avg_r"],
                    "max_drawdown_u": row["max_drawdown_u"],
                    "strategy": "",
                    "baseline_core": "",
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
            + "<th>Variant</th><th>Trades</th><th>PnL</th><th>Return %</th>"
            + "<th>Win %</th><th>PF</th><th>Avg R</th><th>Max DD</th></tr>"
        )
        body_rows = []
        for row in rows:
            parts = ["<tr>"]
            if include_symbol:
                parts.append(f"<td>{_html_text(str(row['symbol']))}</td>")
            parts.extend(
                [
                    f"<td>{_html_text(str(row['variant_label']))}</td>",
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
        rows_sorted = sorted(rows, key=lambda item: (0 if item["variant_key"] == "baseline" else 1, str(item["variant_label"])))
        best_pnl_row = best_row(rows_sorted, "pnl_u")
        best_pf_row = best_row(
            [row for row in rows_sorted if row["profit_factor"] is not None],
            "profit_factor",
        )
        cards.append(
            "<section class=\"card\">"
            f"<h2>{_html_text(symbol)}</h2>"
            f"<p>{_html_text(str(rows_sorted[0]['strategy']))} | {_html_text(str(rows_sorted[0]['baseline_core']))}</p>"
            f"<p><strong>{_html_text('最佳PnL')}</strong>: {_html_text(str(best_pnl_row['variant_label']))} / {_html_text(_fmt_decimal(Decimal(str(best_pnl_row['pnl_u']))))}U</p>"
            f"<p><strong>{_html_text('最佳PF')}</strong>: {_html_text(str(best_pf_row['variant_label']))} / {_html_text(_fmt_pf(Decimal(str(best_pf_row['profit_factor']))))}</p>"
            f"{render_table(rows_sorted, include_symbol=False)}"
            "</section>"
        )

    generated_at = _html_text(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    html_text = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <title>{_html_text('最佳空头均线替换实验')}</title>
  <style>
    :root {{
      --bg: #f4efe6;
      --panel: #fffaf2;
      --line: #dccfb7;
      --ink: #1f2a30;
      --accent: #7f1d1d;
      --cool: #0f766e;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      padding: 28px;
      background:
        radial-gradient(circle at top right, rgba(127,29,29,0.10), transparent 30%),
        radial-gradient(circle at top left, rgba(15,118,110,0.10), transparent 24%),
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
    h1 {{ font-size: 38px; color: #7f1d1d; }}
    h2 {{ font-size: 24px; color: #0f766e; }}
    p {{ margin: 0 0 10px 0; }}
    .meta {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
      gap: 12px;
      margin-top: 14px;
    }}
    .chip {{
      background: #f8ecec;
      border: 1px solid rgba(127,29,29,0.18);
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
      background: rgba(248,236,236,0.9);
      color: #7f1d1d;
    }}
  </style>
</head>
<body>
  <section class="hero">
    <h1>{_html_text('最佳空头均线替换实验')}</h1>
    <p>{_html_text('只测试当前最佳参数组合包里的 4 个做空策略；ATR、5R、手续费偏移、斜率转正平仓等其余参数不动，只替换主均线与趋势均线。')}</p>
    <div class="meta">
      <div class="chip"><strong>{_html_text('生成时间')}</strong><br>{generated_at}</div>
      <div class="chip"><strong>{_html_text('组合PnL最佳')}</strong><br>{_html_text(f"{aggregate_best['variant_label']} / {format_decimal_fixed(Decimal(str(aggregate_best['pnl_u'])), 4)}U")}</div>
      <div class="chip"><strong>{_html_text('组合PF最佳')}</strong><br>{_html_text(f"{aggregate_pf_best['variant_label']} / {_fmt_pf(Decimal(str(aggregate_pf_best['profit_factor'])))}")}</div>
      <div class="chip"><strong>{_html_text('测试候选')}</strong><br>{_html_text('MA55 / MA34 / MA21 / EMA34 / EMA21 / MA60 / EMA60')}</div>
    </div>
  </section>
  <section class="card">
    <h2>{_html_text('总组合对比')}</h2>
    {render_table(sorted(aggregate_rows, key=lambda item: (0 if item['variant_key'] == 'baseline' else 1, str(item['variant_label']))), include_symbol=False)}
  </section>
  {''.join(cards)}
</body>
</html>
"""
    return html_text


def main() -> None:
    client = OkxRestClient()
    specs = load_short_specs()
    per_symbol_results: list[VariantResult] = []
    trades_by_variant: dict[tuple[str, str], list] = {}

    for spec in specs:
        for variant in LINE_VARIANTS:
            print(f"run {spec.label} variant={variant.label}")
            variant_result, trades = run_variant(client, spec, variant)
            per_symbol_results.append(variant_result)
            trades_by_variant[(spec.symbol, variant.key)] = trades

    per_symbol_rows = build_rows(per_symbol_results)
    aggregate_rows = aggregate_by_variant(trades_by_variant, specs)
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
