from __future__ import annotations

import base64
import html
import io
import json
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from okx_quant.backtest import BacktestTrade, _run_backtest_with_loaded_data
from okx_quant.candle_cache import load_candle_cache
from okx_quant.okx_client import OkxRestClient
from okx_quant.persistence import analysis_report_dir_path
from okx_quant.pricing import format_decimal_fixed
from scripts.run_btc_daily_ma_direction_filter_research import (
    LONG_MAKER_FEE_RATE,
    LONG_TAKER_FEE_RATE,
    SHORT_TAKER_FEE_RATE,
    SplitMetrics,
    build_daily_direction_bias,
    build_metrics,
    build_split_bounds,
    filter_split_trades,
    format_ts,
)
from scripts.run_multi_coin_best_long_daily_gate_report import (
    ENTRY_LIMIT,
    GATES,
    LONG_PROFILES,
    SYMBOLS,
    SYMBOL_LABELS,
    GateOption,
    build_long_config,
    build_short_config,
)


plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "Noto Sans CJK SC", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False


REPORT_DIR = analysis_report_dir_path()
REPORT_DIR.mkdir(parents=True, exist_ok=True)

STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
HTML_PATH = REPORT_DIR / f"multi_coin_long_short_merged_report_{STAMP}.html"
CSV_PATH = REPORT_DIR / f"multi_coin_long_short_merged_report_{STAMP}.csv"
JSON_PATH = REPORT_DIR / f"multi_coin_long_short_merged_report_{STAMP}.json"


@dataclass(frozen=True)
class VariantResult:
    gate: GateOption
    all_metrics: SplitMetrics
    test_metrics: SplitMetrics
    trades: list[BacktestTrade]
    test_trades: list[BacktestTrade]


@dataclass(frozen=True)
class ComboResult:
    long_gate: GateOption
    short_gate: GateOption
    all_metrics: SplitMetrics
    test_metrics: SplitMetrics
    trades: list[BacktestTrade]
    test_trades: list[BacktestTrade]


@dataclass(frozen=True)
class SymbolStudy:
    symbol: str
    label: str
    start_ts: int
    end_ts: int
    entry_candles: int
    filter_candles: int
    long_results: dict[str, VariantResult]
    short_results: dict[str, VariantResult]
    combos: list[ComboResult]


def main() -> None:
    client = OkxRestClient()
    studies = [run_symbol_study(client, symbol) for symbol in SYMBOLS]

    aggregate_long = build_aggregate_side(studies, side="long")
    aggregate_short = build_aggregate_side(studies, side="short")
    aggregate_combos = build_aggregate_combos(studies)

    long_best = max(aggregate_long.values(), key=lambda item: item.test_metrics.pnl)
    short_best = max(aggregate_short.values(), key=lambda item: item.test_metrics.pnl)
    combo_best = max(aggregate_combos, key=lambda item: item.test_metrics.pnl)
    combo_baseline = find_combo(aggregate_combos, "none", "none")

    frame = build_frame(studies, aggregate_long, aggregate_short, aggregate_combos)
    frame.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")

    payload = build_payload(studies, aggregate_long, aggregate_short, aggregate_combos, long_best, short_best, combo_best)
    JSON_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    HTML_PATH.write_text(
        build_html(studies, aggregate_long, aggregate_short, aggregate_combos, long_best, short_best, combo_best, combo_baseline),
        encoding="utf-8",
    )
    print(HTML_PATH)


def run_symbol_study(client: OkxRestClient, symbol: str) -> SymbolStudy:
    entry_candles = [candle for candle in load_candle_cache(symbol, "1H", limit=ENTRY_LIMIT) if candle.confirmed]
    filter_candles = [candle for candle in load_candle_cache(symbol, "1D", limit=None) if candle.confirmed]
    if not entry_candles or not filter_candles:
        raise RuntimeError(f"missing local candles for {symbol}")

    instrument = client.get_instrument(symbol)
    bounds = build_split_bounds(len(entry_candles))
    bias_map: dict[str, list[str] | None] = {"none": None}
    for gate in GATES:
        if gate.period is None:
            continue
        bias_map[gate.key] = build_daily_direction_bias(entry_candles, filter_candles, gate)

    long_results: dict[str, VariantResult] = {}
    short_results: dict[str, VariantResult] = {}
    for gate in GATES:
        bias = bias_map[gate.key]
        long_results[gate.key] = run_side_variant(
            symbol=symbol,
            side="long",
            gate=gate,
            entry_candles=entry_candles,
            instrument=instrument,
            test_bounds=bounds["test"],
            direction_filter_bias=bias,
        )
        short_results[gate.key] = run_side_variant(
            symbol=symbol,
            side="short",
            gate=gate,
            entry_candles=entry_candles,
            instrument=instrument,
            test_bounds=bounds["test"],
            direction_filter_bias=bias,
        )

    combos = build_symbol_combos(long_results, short_results)
    return SymbolStudy(
        symbol=symbol,
        label=SYMBOL_LABELS[symbol],
        start_ts=entry_candles[0].ts,
        end_ts=entry_candles[-1].ts,
        entry_candles=len(entry_candles),
        filter_candles=len(filter_candles),
        long_results=long_results,
        short_results=short_results,
        combos=combos,
    )


def run_side_variant(
    *,
    symbol: str,
    side: str,
    gate: GateOption,
    entry_candles,
    instrument,
    test_bounds,
    direction_filter_bias,
) -> VariantResult:
    if side == "long":
        config = build_long_config(symbol)
        result = _run_backtest_with_loaded_data(
            entry_candles,
            instrument,
            config,
            data_source_note=f"local candle_cache full history | {symbol} 1H candles={len(entry_candles)}",
            maker_fee_rate=LONG_MAKER_FEE_RATE,
            taker_fee_rate=LONG_TAKER_FEE_RATE,
            direction_filter_bias=direction_filter_bias,
        )
    else:
        config = build_short_config(symbol)
        result = _run_backtest_with_loaded_data(
            entry_candles,
            instrument,
            config,
            data_source_note=f"local candle_cache full history | {symbol} 1H candles={len(entry_candles)}",
            taker_fee_rate=SHORT_TAKER_FEE_RATE,
            direction_filter_bias=direction_filter_bias,
        )
    trades = list(result.trades)
    test_trades = filter_split_trades(trades, test_bounds)
    return VariantResult(
        gate=gate,
        all_metrics=build_metrics(trades),
        test_metrics=build_metrics(test_trades),
        trades=trades,
        test_trades=test_trades,
    )


def build_symbol_combos(long_results: dict[str, VariantResult], short_results: dict[str, VariantResult]) -> list[ComboResult]:
    combos: list[ComboResult] = []
    for long_gate in GATES:
        for short_gate in GATES:
            all_trades = sorted(
                [*long_results[long_gate.key].trades, *short_results[short_gate.key].trades],
                key=lambda trade: (trade.exit_ts, trade.entry_ts, trade.signal),
            )
            test_trades = sorted(
                [*long_results[long_gate.key].test_trades, *short_results[short_gate.key].test_trades],
                key=lambda trade: (trade.exit_ts, trade.entry_ts, trade.signal),
            )
            combos.append(
                ComboResult(
                    long_gate=long_gate,
                    short_gate=short_gate,
                    all_metrics=build_metrics(all_trades),
                    test_metrics=build_metrics(test_trades),
                    trades=all_trades,
                    test_trades=test_trades,
                )
            )
    return combos


def build_aggregate_side(studies: list[SymbolStudy], *, side: str) -> dict[str, VariantResult]:
    aggregate: dict[str, VariantResult] = {}
    for gate in GATES:
        all_trades: list[BacktestTrade] = []
        test_trades: list[BacktestTrade] = []
        for study in studies:
            item = study.long_results[gate.key] if side == "long" else study.short_results[gate.key]
            all_trades.extend(item.trades)
            test_trades.extend(item.test_trades)
        all_trades.sort(key=lambda trade: (trade.exit_ts, trade.entry_ts, trade.signal))
        test_trades.sort(key=lambda trade: (trade.exit_ts, trade.entry_ts, trade.signal))
        aggregate[gate.key] = VariantResult(
            gate=gate,
            all_metrics=build_metrics(all_trades),
            test_metrics=build_metrics(test_trades),
            trades=all_trades,
            test_trades=test_trades,
        )
    return aggregate


def build_aggregate_combos(studies: list[SymbolStudy]) -> list[ComboResult]:
    combos: list[ComboResult] = []
    for long_gate in GATES:
        for short_gate in GATES:
            all_trades: list[BacktestTrade] = []
            test_trades: list[BacktestTrade] = []
            for study in studies:
                combo = find_combo(study.combos, long_gate.key, short_gate.key)
                all_trades.extend(combo.trades)
                test_trades.extend(combo.test_trades)
            all_trades.sort(key=lambda trade: (trade.exit_ts, trade.entry_ts, trade.signal))
            test_trades.sort(key=lambda trade: (trade.exit_ts, trade.entry_ts, trade.signal))
            combos.append(
                ComboResult(
                    long_gate=long_gate,
                    short_gate=short_gate,
                    all_metrics=build_metrics(all_trades),
                    test_metrics=build_metrics(test_trades),
                    trades=all_trades,
                    test_trades=test_trades,
                )
            )
    return combos


def build_frame(
    studies: list[SymbolStudy],
    aggregate_long: dict[str, VariantResult],
    aggregate_short: dict[str, VariantResult],
    aggregate_combos: list[ComboResult],
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for gate in GATES:
        rows.append(side_row("aggregate_long", "ALL", aggregate_long["none"], aggregate_long[gate.key]))
        rows.append(side_row("aggregate_short", "ALL", aggregate_short["none"], aggregate_short[gate.key]))
    combo_baseline = find_combo(aggregate_combos, "none", "none")
    for combo in aggregate_combos:
        rows.append(combo_row("aggregate_combo", "ALL", combo_baseline, combo))

    for study in studies:
        long_base = study.long_results["none"]
        short_base = study.short_results["none"]
        combo_base = find_combo(study.combos, "none", "none")
        for gate in GATES:
            rows.append(side_row("symbol_long", study.label, long_base, study.long_results[gate.key]))
            rows.append(side_row("symbol_short", study.label, short_base, study.short_results[gate.key]))
        for combo in study.combos:
            if combo.long_gate.key == "none" and combo.short_gate.key == "none":
                rows.append(combo_row("symbol_combo", study.label, combo_base, combo))
                continue
            if combo.long_gate.key in {"none", "ema_5", "ma_5"} and combo.short_gate.key in {"none", "ema_5", "ma_5"}:
                rows.append(combo_row("symbol_combo", study.label, combo_base, combo))
    return pd.DataFrame(rows)


def side_row(scope: str, label: str, baseline: VariantResult, variant: VariantResult) -> dict[str, object]:
    return {
        "scope": scope,
        "label": label,
        "gate_label": variant.gate.label,
        "baseline_all_pnl": float(baseline.all_metrics.pnl),
        "baseline_test_pnl": float(baseline.test_metrics.pnl),
        "variant_all_pnl": float(variant.all_metrics.pnl),
        "variant_test_pnl": float(variant.test_metrics.pnl),
        "variant_all_trades": variant.all_metrics.trades,
        "variant_test_trades": variant.test_metrics.trades,
        "variant_all_pf": None if variant.all_metrics.profit_factor is None else float(variant.all_metrics.profit_factor),
        "variant_test_pf": None if variant.test_metrics.profit_factor is None else float(variant.test_metrics.profit_factor),
        "variant_all_drawdown": float(variant.all_metrics.max_drawdown),
        "variant_test_drawdown": float(variant.test_metrics.max_drawdown),
        "all_delta": float(variant.all_metrics.pnl - baseline.all_metrics.pnl),
        "test_delta": float(variant.test_metrics.pnl - baseline.test_metrics.pnl),
    }


def combo_row(scope: str, label: str, baseline: ComboResult, combo: ComboResult) -> dict[str, object]:
    return {
        "scope": scope,
        "label": label,
        "long_gate_label": combo.long_gate.label,
        "short_gate_label": combo.short_gate.label,
        "baseline_all_pnl": float(baseline.all_metrics.pnl),
        "baseline_test_pnl": float(baseline.test_metrics.pnl),
        "combo_all_pnl": float(combo.all_metrics.pnl),
        "combo_test_pnl": float(combo.test_metrics.pnl),
        "combo_all_trades": combo.all_metrics.trades,
        "combo_test_trades": combo.test_metrics.trades,
        "combo_all_pf": None if combo.all_metrics.profit_factor is None else float(combo.all_metrics.profit_factor),
        "combo_test_pf": None if combo.test_metrics.profit_factor is None else float(combo.test_metrics.profit_factor),
        "combo_all_drawdown": float(combo.all_metrics.max_drawdown),
        "combo_test_drawdown": float(combo.test_metrics.max_drawdown),
        "all_delta": float(combo.all_metrics.pnl - baseline.all_metrics.pnl),
        "test_delta": float(combo.test_metrics.pnl - baseline.test_metrics.pnl),
    }


def build_payload(
    studies: list[SymbolStudy],
    aggregate_long: dict[str, VariantResult],
    aggregate_short: dict[str, VariantResult],
    aggregate_combos: list[ComboResult],
    long_best: VariantResult,
    short_best: VariantResult,
    combo_best: ComboResult,
) -> dict[str, object]:
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "entry_bar": "1H",
        "filter_bar": "1D",
        "entry_limit": ENTRY_LIMIT,
        "assumption": {
            "risk_amount": "10U per trade",
            "long_side": "五币种做多使用各自历史最优参数",
            "short_side": "五币种做空统一使用 EMA55 斜率做空模板",
            "gate_rule": "日线收盘高于日线均线时只允许做多，低于日线均线时只允许做空",
        },
        "aggregate": {
            "long_baseline": variant_payload(aggregate_long["none"]),
            "long_best": variant_payload(long_best),
            "short_baseline": variant_payload(aggregate_short["none"]),
            "short_best": variant_payload(short_best),
            "combo_baseline": combo_payload(find_combo(aggregate_combos, "none", "none")),
            "combo_best": combo_payload(combo_best),
        },
        "symbols": [symbol_payload(study) for study in studies],
    }


def symbol_payload(study: SymbolStudy) -> dict[str, object]:
    long_best = max(study.long_results.values(), key=lambda item: item.test_metrics.pnl)
    short_best = max(study.short_results.values(), key=lambda item: item.test_metrics.pnl)
    combo_best = max(study.combos, key=lambda item: item.test_metrics.pnl)
    return {
        "symbol": study.symbol,
        "label": study.label,
        "sample": {
            "entry_candles": study.entry_candles,
            "filter_candles": study.filter_candles,
            "start_utc": format_ts(study.start_ts),
            "end_utc": format_ts(study.end_ts),
        },
        "long_profile": long_profile_payload(study.symbol),
        "long_baseline": variant_payload(study.long_results["none"]),
        "long_best": variant_payload(long_best),
        "short_baseline": variant_payload(study.short_results["none"]),
        "short_best": variant_payload(short_best),
        "combo_baseline": combo_payload(find_combo(study.combos, "none", "none")),
        "combo_best": combo_payload(combo_best),
    }


def variant_payload(item: VariantResult) -> dict[str, object]:
    return {
        "gate": asdict(item.gate),
        "all_metrics": split_payload(item.all_metrics),
        "test_metrics": split_payload(item.test_metrics),
    }


def combo_payload(item: ComboResult) -> dict[str, object]:
    return {
        "long_gate": asdict(item.long_gate),
        "short_gate": asdict(item.short_gate),
        "all_metrics": split_payload(item.all_metrics),
        "test_metrics": split_payload(item.test_metrics),
    }


def split_payload(metrics: SplitMetrics) -> dict[str, object]:
    return {
        "pnl": str(metrics.pnl),
        "trades": metrics.trades,
        "win_rate": str(metrics.win_rate),
        "profit_factor": None if metrics.profit_factor is None else str(metrics.profit_factor),
        "avg_r": str(metrics.avg_r),
        "max_drawdown": str(metrics.max_drawdown),
        "return_pct": str(metrics.return_pct),
    }


def long_profile_payload(symbol: str) -> dict[str, object]:
    profile = LONG_PROFILES[symbol]
    return {
        "symbol": profile.symbol,
        "label": profile.label,
        "ema_period": profile.ema_period,
        "trend_ema_period": profile.trend_ema_period,
        "entry_reference_ema_period": profile.entry_reference_ema_period,
        "atr_stop_multiplier": str(profile.atr_stop_multiplier),
    }


def build_html(
    studies: list[SymbolStudy],
    aggregate_long: dict[str, VariantResult],
    aggregate_short: dict[str, VariantResult],
    aggregate_combos: list[ComboResult],
    long_best: VariantResult,
    short_best: VariantResult,
    combo_best: ComboResult,
    combo_baseline: ComboResult,
) -> str:
    long_ranked = sorted(aggregate_long.values(), key=lambda item: item.test_metrics.pnl, reverse=True)
    short_ranked = sorted(aggregate_short.values(), key=lambda item: item.test_metrics.pnl, reverse=True)
    combo_ranked = sorted(aggregate_combos, key=lambda item: item.test_metrics.pnl, reverse=True)

    summary_cards = [
        summary_card(
            "纯做多最优",
            f"{html.escape(long_best.gate.label)}<br>测试 {fmt(long_best.test_metrics.pnl)}U<br>比基线多 {fmt(long_best.test_metrics.pnl - aggregate_long['none'].test_metrics.pnl)}U",
        ),
        summary_card(
            "纯做空最优",
            f"{html.escape(short_best.gate.label)}<br>测试 {fmt(short_best.test_metrics.pnl)}U<br>比基线多 {fmt(short_best.test_metrics.pnl - aggregate_short['none'].test_metrics.pnl)}U",
        ),
        summary_card(
            "多空组合最优",
            f"{html.escape(combo_best.long_gate.label)} / {html.escape(combo_best.short_gate.label)}<br>测试 {fmt(combo_best.test_metrics.pnl)}U<br>比基线多 {fmt(combo_best.test_metrics.pnl - combo_baseline.test_metrics.pnl)}U",
        ),
        summary_card(
            "组合全样本",
            f"基线 {fmt(combo_baseline.all_metrics.pnl)}U<br>最优 {fmt(combo_best.all_metrics.pnl)}U<br>增量 {fmt(combo_best.all_metrics.pnl - combo_baseline.all_metrics.pnl)}U",
        ),
    ]

    symbol_rows = []
    for study in studies:
        long_best_symbol = max(study.long_results.values(), key=lambda item: item.test_metrics.pnl)
        short_best_symbol = max(study.short_results.values(), key=lambda item: item.test_metrics.pnl)
        combo_best_symbol = max(study.combos, key=lambda item: item.test_metrics.pnl)
        symbol_rows.append(
            "<tr>"
            f"<td>{html.escape(study.label)}</td>"
            f"<td>{profile_label(study.symbol)}</td>"
            f"<td>{fmt(study.long_results['none'].test_metrics.pnl)}</td>"
            f"<td>{html.escape(long_best_symbol.gate.label)}</td>"
            f"<td class=\"{'good' if long_best_symbol.test_metrics.pnl - study.long_results['none'].test_metrics.pnl >= 0 else 'bad'}\">{fmt(long_best_symbol.test_metrics.pnl - study.long_results['none'].test_metrics.pnl)}</td>"
            f"<td>{fmt(study.short_results['none'].test_metrics.pnl)}</td>"
            f"<td>{html.escape(short_best_symbol.gate.label)}</td>"
            f"<td class=\"{'good' if short_best_symbol.test_metrics.pnl - study.short_results['none'].test_metrics.pnl >= 0 else 'bad'}\">{fmt(short_best_symbol.test_metrics.pnl - study.short_results['none'].test_metrics.pnl)}</td>"
            f"<td>{html.escape(combo_best_symbol.long_gate.label)} / {html.escape(combo_best_symbol.short_gate.label)}</td>"
            f"<td class=\"{'good' if combo_best_symbol.test_metrics.pnl - find_combo(study.combos, 'none', 'none').test_metrics.pnl >= 0 else 'bad'}\">{fmt(combo_best_symbol.test_metrics.pnl - find_combo(study.combos, 'none', 'none').test_metrics.pnl)}</td>"
            "</tr>"
        )

    long_table = side_table_html(long_ranked[:7], aggregate_long["none"], "纯做多排行")
    short_table = side_table_html(short_ranked[:7], aggregate_short["none"], "纯做空排行")
    combo_table = combo_table_html(combo_ranked[:12], combo_baseline, "多空组合排行")
    delta_chart = fig_to_base64(build_delta_chart(studies))
    heatmap = fig_to_base64(build_combo_heatmap(combo_ranked))

    start_ts = min(study.start_ts for study in studies)
    end_ts = max(study.end_ts for study in studies)
    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <title>五币种多空合并研究报告</title>
  <style>
    :root {{
      --bg:#f5f7fb;
      --panel:#ffffff;
      --ink:#122033;
      --muted:#5b6b82;
      --line:#d8e1ec;
      --blue:#1d4ed8;
      --teal:#0f766e;
      --green:#166534;
      --red:#b42318;
    }}
    * {{ box-sizing:border-box; }}
    body {{ margin:0; font-family:"Microsoft YaHei","Segoe UI",sans-serif; background:var(--bg); color:var(--ink); }}
    .wrap {{ max-width:1500px; margin:0 auto; padding:28px; }}
    .hero {{
      background:linear-gradient(135deg,#102033 0%,#1a4c7d 55%,#0f766e 100%);
      color:#fff;
      border-radius:24px;
      padding:30px 34px;
      box-shadow:0 18px 40px rgba(15,23,42,.22);
    }}
    .hero h1 {{ margin:0 0 12px; font-size:34px; }}
    .hero p {{ margin:8px 0; line-height:1.7; color:rgba(255,255,255,.93); }}
    .meta {{ display:flex; flex-wrap:wrap; gap:12px; margin-top:18px; }}
    .chip {{
      background:rgba(255,255,255,.12);
      border:1px solid rgba(255,255,255,.18);
      border-radius:999px;
      padding:8px 12px;
      font-size:13px;
    }}
    .grid {{
      display:grid;
      grid-template-columns:repeat(4,minmax(0,1fr));
      gap:16px;
      margin:22px 0 8px;
    }}
    .card {{
      background:var(--panel);
      border:1px solid var(--line);
      border-radius:20px;
      padding:18px;
      box-shadow:0 10px 22px rgba(15,23,42,.05);
    }}
    .card h3 {{ margin:0 0 10px; font-size:16px; }}
    .card p {{ margin:0; color:var(--muted); line-height:1.7; }}
    .section {{
      margin-top:22px;
      background:var(--panel);
      border:1px solid var(--line);
      border-radius:24px;
      padding:24px;
      box-shadow:0 10px 22px rgba(15,23,42,.05);
    }}
    .section h2 {{ margin:0 0 14px; font-size:24px; }}
    .section p, .section li {{ color:var(--muted); line-height:1.8; }}
    .twocol {{
      display:grid;
      grid-template-columns:repeat(2,minmax(0,1fr));
      gap:18px;
    }}
    .chart {{
      background:#fbfdff;
      border:1px solid var(--line);
      border-radius:18px;
      padding:16px;
    }}
    .chart img {{ width:100%; display:block; border-radius:12px; }}
    table {{ width:100%; border-collapse:collapse; font-size:14px; }}
    th, td {{ padding:10px 12px; border-bottom:1px solid var(--line); text-align:right; }}
    th:first-child, td:first-child {{ text-align:left; }}
    thead th {{ color:var(--muted); font-weight:700; background:#f8fbff; }}
    .good {{ color:var(--green); font-weight:700; }}
    .bad {{ color:var(--red); font-weight:700; }}
    .note {{
      margin-top:14px;
      padding:14px 16px;
      border-left:4px solid var(--blue);
      background:#eef4ff;
      border-radius:14px;
      color:#274064;
    }}
    @media (max-width:1100px) {{
      .grid, .twocol {{ grid-template-columns:1fr; }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <h1>五币种多空合并研究报告</h1>
      <p>这份报告把五个币种的 <strong>纯做多</strong>、<strong>纯做空</strong>、以及 <strong>多空合并组合</strong> 放到一张表里统一比较。做多侧使用各自历史最优参数，做空侧使用统一的 <strong>EMA55 斜率做空模板</strong>，每次固定风险 <strong>10U</strong>。</p>
      <p>方向闸门统一定义为：<strong>日线收盘高于日线均线时只允许做多，低于日线均线时只允许做空</strong>。</p>
      <div class="meta">
        <div class="chip">样本区间：{format_ts(start_ts)} -> {format_ts(end_ts)}</div>
        <div class="chip">1H 样本：最近 {ENTRY_LIMIT:,} 根确认K线</div>
        <div class="chip">币种：BTC / ETH / SOL / BNB / DOGE</div>
        <div class="chip">输出：{html.escape(str(CSV_PATH))}</div>
      </div>
    </section>

    <section class="grid">
      {''.join(summary_cards)}
    </section>

    <section class="section">
      <h2>核心结论</h2>
      <ul>
        <li>纯做多最优闸门是 <strong>{html.escape(long_best.gate.label)}</strong>，测试段盈利 <strong>{fmt(long_best.test_metrics.pnl)}U</strong>，比无过滤多 <strong>{fmt(long_best.test_metrics.pnl - aggregate_long['none'].test_metrics.pnl)}U</strong>。</li>
        <li>纯做空最优闸门是 <strong>{html.escape(short_best.gate.label)}</strong>，测试段盈利 <strong>{fmt(short_best.test_metrics.pnl)}U</strong>，比无过滤多 <strong>{fmt(short_best.test_metrics.pnl - aggregate_short['none'].test_metrics.pnl)}U</strong>。</li>
        <li>多空合并最优组合是 <strong>{html.escape(combo_best.long_gate.label)} / {html.escape(combo_best.short_gate.label)}</strong>，测试段盈利 <strong>{fmt(combo_best.test_metrics.pnl)}U</strong>，比组合基线多 <strong>{fmt(combo_best.test_metrics.pnl - combo_baseline.test_metrics.pnl)}U</strong>。</li>
      </ul>
      <div class="note">
        从这轮结果看，做多侧的日线过滤增益依旧大于做空侧；但做空侧加上合适的快线闸门后，仍然能继续把总组合往上推。
      </div>
    </section>

    <section class="section">
      <h2>聚合排行</h2>
      <div class="twocol">
        <div>{long_table}</div>
        <div>{short_table}</div>
      </div>
    </section>

    <section class="section">
      <h2>组合排行</h2>
      {combo_table}
    </section>

    <section class="section">
      <h2>分币种总览</h2>
      <table>
        <thead>
          <tr>
            <th>币种</th>
            <th>做多参数</th>
            <th>做多基线测试</th>
            <th>做多最佳</th>
            <th>做多增量</th>
            <th>做空基线测试</th>
            <th>做空最佳</th>
            <th>做空增量</th>
            <th>组合最佳</th>
            <th>组合增量</th>
          </tr>
        </thead>
        <tbody>
          {''.join(symbol_rows)}
        </tbody>
      </table>
    </section>

    <section class="section">
      <h2>图表</h2>
      <div class="twocol">
        <div class="chart">
          <img src="data:image/png;base64,{delta_chart}" alt="分币种长短侧增量" />
        </div>
        <div class="chart">
          <img src="data:image/png;base64,{heatmap}" alt="组合热力图" />
        </div>
      </div>
    </section>
  </div>
</body>
</html>"""


def side_table_html(items: list[VariantResult], baseline: VariantResult, caption: str) -> str:
    rows = []
    for item in items:
        delta = item.test_metrics.pnl - baseline.test_metrics.pnl
        rows.append(
            "<tr>"
            f"<td>{html.escape(item.gate.label)}</td>"
            f"<td>{fmt(item.test_metrics.pnl)}</td>"
            f"<td class=\"{'good' if delta >= 0 else 'bad'}\">{fmt(delta)}</td>"
            f"<td>{item.test_metrics.trades}</td>"
            f"<td>{fmt_pf(item.test_metrics.profit_factor)}</td>"
            f"<td>{fmt(item.all_metrics.pnl)}</td>"
            "</tr>"
        )
    return (
        f"<h3>{html.escape(caption)}</h3>"
        "<table><thead><tr>"
        "<th>闸门</th><th>测试PnL</th><th>相对基线</th><th>测试交易</th><th>测试PF</th><th>全样本PnL</th>"
        "</tr></thead><tbody>"
        + "".join(rows)
        + "</tbody></table>"
    )


def combo_table_html(items: list[ComboResult], baseline: ComboResult, caption: str) -> str:
    rows = []
    for item in items:
        delta = item.test_metrics.pnl - baseline.test_metrics.pnl
        rows.append(
            "<tr>"
            f"<td>{html.escape(item.long_gate.label)} / {html.escape(item.short_gate.label)}</td>"
            f"<td>{fmt(item.test_metrics.pnl)}</td>"
            f"<td class=\"{'good' if delta >= 0 else 'bad'}\">{fmt(delta)}</td>"
            f"<td>{item.test_metrics.trades}</td>"
            f"<td>{fmt_pf(item.test_metrics.profit_factor)}</td>"
            f"<td>{fmt(item.all_metrics.pnl)}</td>"
            "</tr>"
        )
    return (
        f"<h3>{html.escape(caption)}</h3>"
        "<table><thead><tr>"
        "<th>长侧 / 短侧</th><th>测试PnL</th><th>相对基线</th><th>测试交易</th><th>测试PF</th><th>全样本PnL</th>"
        "</tr></thead><tbody>"
        + "".join(rows)
        + "</tbody></table>"
    )


def build_delta_chart(studies: list[SymbolStudy]):
    labels = [study.label for study in studies]
    long_values = []
    short_values = []
    for study in studies:
        long_best = max(study.long_results.values(), key=lambda item: item.test_metrics.pnl)
        short_best = max(study.short_results.values(), key=lambda item: item.test_metrics.pnl)
        long_values.append(float(long_best.test_metrics.pnl - study.long_results["none"].test_metrics.pnl))
        short_values.append(float(short_best.test_metrics.pnl - study.short_results["none"].test_metrics.pnl))
    x = range(len(labels))
    fig, ax = plt.subplots(figsize=(10, 5.5))
    ax.bar([i - 0.18 for i in x], long_values, width=0.36, label="Long delta", color="#1d4ed8")
    ax.bar([i + 0.18 for i in x], short_values, width=0.36, label="Short delta", color="#0f766e")
    ax.set_xticks(list(x))
    ax.set_xticklabels(labels)
    ax.set_title("各币种测试段增量", fontsize=14, pad=12)
    ax.set_ylabel("PnL Δ (U)")
    ax.legend()
    fig.tight_layout()
    return fig


def build_combo_heatmap(combos: list[ComboResult]):
    ordered = sorted(combos, key=lambda item: item.test_metrics.pnl, reverse=True)
    frame = pd.DataFrame(
        [
            {
                "long": item.long_gate.label,
                "short": item.short_gate.label,
                "test_pnl": float(item.test_metrics.pnl),
                "delta": float(item.test_metrics.pnl - find_combo(combos, "none", "none").test_metrics.pnl),
            }
            for item in ordered
        ]
    )
    value_map = frame.pivot(index="long", columns="short", values="test_pnl").reindex(
        index=[gate.label for gate in GATES],
        columns=[gate.label for gate in GATES],
    )
    delta_map = frame.pivot(index="long", columns="short", values="delta").reindex(
        index=[gate.label for gate in GATES],
        columns=[gate.label for gate in GATES],
    )
    fig, ax = plt.subplots(figsize=(10, 6.3))
    image = ax.imshow(value_map.values, cmap="YlGnBu")
    ax.set_xticks(range(len(value_map.columns)))
    ax.set_xticklabels(value_map.columns, rotation=30, ha="right")
    ax.set_yticks(range(len(value_map.index)))
    ax.set_yticklabels(value_map.index)
    ax.set_title("多空组合测试段热力图", fontsize=14, pad=12)
    for row in range(len(value_map.index)):
        for col in range(len(value_map.columns)):
            value = value_map.iloc[row, col]
            delta = delta_map.iloc[row, col]
            ax.text(col, row, f"{value:.0f}\nΔ{delta:.0f}", ha="center", va="center", fontsize=8, color="#102033")
    fig.colorbar(image, ax=ax, shrink=0.8)
    fig.tight_layout()
    return fig


def summary_card(title: str, body: str) -> str:
    return f'<div class="card"><h3>{html.escape(title)}</h3><p>{body}</p></div>'


def profile_label(symbol: str) -> str:
    profile = LONG_PROFILES[symbol]
    entry = f"EMA{profile.entry_reference_ema_period}" if profile.entry_reference_ema_period > 0 else f"跟随 EMA{profile.ema_period}"
    return f"EMA{profile.ema_period}/EMA{profile.trend_ema_period} + {entry} + SLx{format_decimal_fixed(profile.atr_stop_multiplier, 1)}"


def fig_to_base64(fig) -> str:
    buffer = io.BytesIO()
    fig.savefig(buffer, format="png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    return base64.b64encode(buffer.getvalue()).decode("ascii")


def find_combo(combos: list[ComboResult], long_gate_key: str, short_gate_key: str) -> ComboResult:
    return next(item for item in combos if item.long_gate.key == long_gate_key and item.short_gate.key == short_gate_key)


def fmt(value: Decimal) -> str:
    return format_decimal_fixed(value, 4)


def fmt_pf(value: Decimal | None) -> str:
    if value is None:
        return "-"
    return format_decimal_fixed(value, 4)


if __name__ == "__main__":
    main()
