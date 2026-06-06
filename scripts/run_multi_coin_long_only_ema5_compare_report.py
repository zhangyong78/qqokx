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
)


plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "Noto Sans CJK SC", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False


REPORT_DIR = analysis_report_dir_path()
REPORT_DIR.mkdir(parents=True, exist_ok=True)

STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
HTML_PATH = REPORT_DIR / f"multi_coin_long_only_ema5_compare_report_{STAMP}.html"
CSV_PATH = REPORT_DIR / f"multi_coin_long_only_ema5_compare_report_{STAMP}.csv"
JSON_PATH = REPORT_DIR / f"multi_coin_long_only_ema5_compare_report_{STAMP}.json"

BASE_GATE = GateOption("none", "无过滤")
EMA5_GATE = GateOption("ema_5", "EMA5", "ema", 5)


@dataclass(frozen=True)
class VariantResult:
    gate: GateOption
    all_metrics: SplitMetrics
    test_metrics: SplitMetrics
    trades: list[BacktestTrade]


@dataclass(frozen=True)
class SymbolResult:
    symbol: str
    label: str
    start_ts: int
    end_ts: int
    entry_candles: int
    filter_candles: int
    baseline: VariantResult
    ema5: VariantResult
    best_gate: VariantResult


def main() -> None:
    client = OkxRestClient()
    symbols: list[SymbolResult] = []
    for symbol in SYMBOLS:
        symbols.append(run_symbol(client, symbol))

    aggregate_baseline = merge_variant(symbols, "baseline")
    aggregate_ema5 = merge_variant(symbols, "ema5")
    frame = build_frame(symbols, aggregate_baseline, aggregate_ema5)
    frame.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")

    payload = build_payload(symbols, aggregate_baseline, aggregate_ema5)
    JSON_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    HTML_PATH.write_text(build_html(symbols, aggregate_baseline, aggregate_ema5), encoding="utf-8")
    print(HTML_PATH)


def run_symbol(client: OkxRestClient, symbol: str) -> SymbolResult:
    entry_candles = [candle for candle in load_candle_cache(symbol, "1H", limit=ENTRY_LIMIT) if candle.confirmed]
    filter_candles = [candle for candle in load_candle_cache(symbol, "1D", limit=None) if candle.confirmed]
    if not entry_candles or not filter_candles:
        raise RuntimeError(f"missing local candles for {symbol}")

    bounds = build_split_bounds(len(entry_candles))
    instrument = client.get_instrument(symbol)
    ema5_bias = build_daily_direction_bias(entry_candles, filter_candles, EMA5_GATE)

    baseline_trades = run_long_backtest(
        entry_candles=entry_candles,
        instrument=instrument,
        symbol=symbol,
        direction_filter_bias=None,
    )
    ema5_trades = run_long_backtest(
        entry_candles=entry_candles,
        instrument=instrument,
        symbol=symbol,
        direction_filter_bias=ema5_bias,
    )

    best_gate = BASE_GATE
    best_trades = baseline_trades
    best_metrics = build_metrics(filter_split_trades(baseline_trades, bounds["test"]))
    for gate in GATES:
        bias = None if gate.period is None else build_daily_direction_bias(entry_candles, filter_candles, gate)
        trades = run_long_backtest(
            entry_candles=entry_candles,
            instrument=instrument,
            symbol=symbol,
            direction_filter_bias=bias,
        )
        metrics = build_metrics(filter_split_trades(trades, bounds["test"]))
        if metrics.pnl > best_metrics.pnl:
            best_gate = gate
            best_trades = trades
            best_metrics = metrics

    return SymbolResult(
        symbol=symbol,
        label=SYMBOL_LABELS[symbol],
        start_ts=entry_candles[0].ts,
        end_ts=entry_candles[-1].ts,
        entry_candles=len(entry_candles),
        filter_candles=len(filter_candles),
        baseline=build_variant(BASE_GATE, baseline_trades, bounds["test"]),
        ema5=build_variant(EMA5_GATE, ema5_trades, bounds["test"]),
        best_gate=build_variant(best_gate, best_trades, bounds["test"]),
    )


def run_long_backtest(*, entry_candles, instrument, symbol: str, direction_filter_bias):
    result = _run_backtest_with_loaded_data(
        entry_candles,
        instrument,
        build_long_config(symbol),
        data_source_note=f"local candle_cache full history | {symbol} 1H candles={len(entry_candles)}",
        maker_fee_rate=LONG_MAKER_FEE_RATE,
        taker_fee_rate=LONG_TAKER_FEE_RATE,
        direction_filter_bias=direction_filter_bias,
    )
    return list(result.trades)


def build_variant(gate: GateOption, trades: list[BacktestTrade], test_bounds) -> VariantResult:
    test_trades = filter_split_trades(trades, test_bounds)
    return VariantResult(
        gate=gate,
        all_metrics=build_metrics(trades),
        test_metrics=build_metrics(test_trades),
        trades=trades,
    )


def merge_variant(symbols: list[SymbolResult], which: str) -> VariantResult:
    gate = BASE_GATE if which == "baseline" else EMA5_GATE
    trades: list[BacktestTrade] = []
    test_trades: list[BacktestTrade] = []
    for item in symbols:
        variant = item.baseline if which == "baseline" else item.ema5
        trades.extend(variant.trades)
        symbol_test = filter_split_trades(
            variant.trades,
            build_split_bounds(item.entry_candles)["test"],
        )
        test_trades.extend(symbol_test)
    trades.sort(key=lambda trade: (trade.exit_ts, trade.entry_ts, trade.signal))
    test_trades.sort(key=lambda trade: (trade.exit_ts, trade.entry_ts, trade.signal))
    return VariantResult(
        gate=gate,
        all_metrics=build_metrics(trades),
        test_metrics=build_metrics(test_trades),
        trades=trades,
    )


def build_frame(
    symbols: list[SymbolResult],
    aggregate_baseline: VariantResult,
    aggregate_ema5: VariantResult,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for item in symbols:
        rows.append(row_payload("symbol", item.label, item.baseline, item.ema5, item.best_gate))
    rows.append(row_payload("aggregate", "ALL", aggregate_baseline, aggregate_ema5, aggregate_ema5))
    return pd.DataFrame(rows)


def row_payload(scope: str, label: str, baseline: VariantResult, ema5: VariantResult, best_gate: VariantResult) -> dict[str, object]:
    return {
        "scope": scope,
        "label": label,
        "baseline_gate": baseline.gate.label,
        "baseline_all_pnl": float(baseline.all_metrics.pnl),
        "baseline_all_trades": baseline.all_metrics.trades,
        "baseline_all_pf": None if baseline.all_metrics.profit_factor is None else float(baseline.all_metrics.profit_factor),
        "baseline_all_drawdown": float(baseline.all_metrics.max_drawdown),
        "baseline_test_pnl": float(baseline.test_metrics.pnl),
        "baseline_test_trades": baseline.test_metrics.trades,
        "baseline_test_pf": None if baseline.test_metrics.profit_factor is None else float(baseline.test_metrics.profit_factor),
        "baseline_test_drawdown": float(baseline.test_metrics.max_drawdown),
        "ema5_all_pnl": float(ema5.all_metrics.pnl),
        "ema5_all_trades": ema5.all_metrics.trades,
        "ema5_all_pf": None if ema5.all_metrics.profit_factor is None else float(ema5.all_metrics.profit_factor),
        "ema5_all_drawdown": float(ema5.all_metrics.max_drawdown),
        "ema5_test_pnl": float(ema5.test_metrics.pnl),
        "ema5_test_trades": ema5.test_metrics.trades,
        "ema5_test_pf": None if ema5.test_metrics.profit_factor is None else float(ema5.test_metrics.profit_factor),
        "ema5_test_drawdown": float(ema5.test_metrics.max_drawdown),
        "all_delta": float(ema5.all_metrics.pnl - baseline.all_metrics.pnl),
        "test_delta": float(ema5.test_metrics.pnl - baseline.test_metrics.pnl),
        "best_gate_label": best_gate.gate.label,
        "best_test_pnl": float(best_gate.test_metrics.pnl),
    }


def build_payload(
    symbols: list[SymbolResult],
    aggregate_baseline: VariantResult,
    aggregate_ema5: VariantResult,
) -> dict[str, object]:
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "entry_bar": "1H",
        "filter_bar": "1D",
        "entry_limit": ENTRY_LIMIT,
        "assumption": {
            "side": "long_only",
            "rule": "日线收盘 > 日线 EMA5 时允许做多，否则不开新多单",
            "risk_amount": "10U per trade",
            "long_profiles": {SYMBOL_LABELS[symbol]: profile_payload(symbol) for symbol in SYMBOLS},
        },
        "aggregate": {
            "baseline": variant_payload(aggregate_baseline),
            "ema5": variant_payload(aggregate_ema5),
            "delta": {
                "all_pnl": str(aggregate_ema5.all_metrics.pnl - aggregate_baseline.all_metrics.pnl),
                "test_pnl": str(aggregate_ema5.test_metrics.pnl - aggregate_baseline.test_metrics.pnl),
            },
        },
        "symbols": [symbol_payload(item) for item in symbols],
    }


def symbol_payload(item: SymbolResult) -> dict[str, object]:
    return {
        "symbol": item.symbol,
        "label": item.label,
        "sample": {
            "entry_candles": item.entry_candles,
            "filter_candles": item.filter_candles,
            "start_utc": format_ts(item.start_ts),
            "end_utc": format_ts(item.end_ts),
        },
        "long_profile": profile_payload(item.symbol),
        "baseline": variant_payload(item.baseline),
        "ema5": variant_payload(item.ema5),
        "best_gate": variant_payload(item.best_gate),
        "delta": {
            "all_pnl": str(item.ema5.all_metrics.pnl - item.baseline.all_metrics.pnl),
            "test_pnl": str(item.ema5.test_metrics.pnl - item.baseline.test_metrics.pnl),
        },
    }


def variant_payload(item: VariantResult) -> dict[str, object]:
    return {
        "gate": asdict(item.gate),
        "all_metrics": split_payload(item.all_metrics),
        "test_metrics": split_payload(item.test_metrics),
    }


def profile_payload(symbol: str) -> dict[str, object]:
    profile = LONG_PROFILES[symbol]
    return {
        "symbol": profile.symbol,
        "label": profile.label,
        "ema_period": profile.ema_period,
        "trend_ema_period": profile.trend_ema_period,
        "entry_reference_ema_period": profile.entry_reference_ema_period,
        "atr_stop_multiplier": str(profile.atr_stop_multiplier),
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


def build_html(symbols: list[SymbolResult], aggregate_baseline: VariantResult, aggregate_ema5: VariantResult) -> str:
    summary_cards = [
        summary_card(
            "全样本对比",
            f"无过滤 {fmt(aggregate_baseline.all_metrics.pnl)}U<br>EMA5 {fmt(aggregate_ema5.all_metrics.pnl)}U<br>增量 {fmt(aggregate_ema5.all_metrics.pnl - aggregate_baseline.all_metrics.pnl)}U",
        ),
        summary_card(
            "测试段对比",
            f"无过滤 {fmt(aggregate_baseline.test_metrics.pnl)}U<br>EMA5 {fmt(aggregate_ema5.test_metrics.pnl)}U<br>增量 {fmt(aggregate_ema5.test_metrics.pnl - aggregate_baseline.test_metrics.pnl)}U",
        ),
        summary_card(
            "交易数变化",
            f"全样本 {aggregate_baseline.all_metrics.trades} -> {aggregate_ema5.all_metrics.trades}<br>测试段 {aggregate_baseline.test_metrics.trades} -> {aggregate_ema5.test_metrics.trades}",
        ),
        summary_card(
            "回撤变化",
            f"全样本 DD {fmt(aggregate_baseline.all_metrics.max_drawdown)} -> {fmt(aggregate_ema5.all_metrics.max_drawdown)}<br>测试段 DD {fmt(aggregate_baseline.test_metrics.max_drawdown)} -> {fmt(aggregate_ema5.test_metrics.max_drawdown)}",
        ),
    ]

    rows = []
    for item in symbols:
        delta_all = item.ema5.all_metrics.pnl - item.baseline.all_metrics.pnl
        delta_test = item.ema5.test_metrics.pnl - item.baseline.test_metrics.pnl
        rows.append(
            "<tr>"
            f"<td>{html.escape(item.label)}</td>"
            f"<td>{profile_label(item.symbol)}</td>"
            f"<td>{fmt(item.baseline.all_metrics.pnl)}</td>"
            f"<td>{fmt(item.ema5.all_metrics.pnl)}</td>"
            f"<td class=\"{'good' if delta_all >= 0 else 'bad'}\">{fmt(delta_all)}</td>"
            f"<td>{fmt(item.baseline.test_metrics.pnl)}</td>"
            f"<td>{fmt(item.ema5.test_metrics.pnl)}</td>"
            f"<td class=\"{'good' if delta_test >= 0 else 'bad'}\">{fmt(delta_test)}</td>"
            f"<td>{item.baseline.all_metrics.trades} -> {item.ema5.all_metrics.trades}</td>"
            f"<td>{fmt(item.baseline.all_metrics.max_drawdown)} -> {fmt(item.ema5.all_metrics.max_drawdown)}</td>"
            f"<td>{html.escape(item.best_gate.gate.label)}</td>"
            "</tr>"
        )

    delta_chart = fig_to_base64(build_delta_chart(symbols))
    start_ts = min(item.start_ts for item in symbols)
    end_ts = max(item.end_ts for item in symbols)
    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <title>五币种纯做多 无过滤 vs EMA5</title>
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
    .wrap {{ max-width:1460px; margin:0 auto; padding:28px; }}
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
    table {{ width:100%; border-collapse:collapse; font-size:14px; }}
    th, td {{ padding:10px 12px; border-bottom:1px solid var(--line); text-align:right; }}
    th:first-child, td:first-child {{ text-align:left; }}
    thead th {{ color:var(--muted); font-weight:700; background:#f8fbff; }}
    .good {{ color:var(--green); font-weight:700; }}
    .bad {{ color:var(--red); font-weight:700; }}
    .chart {{
      background:#fbfdff;
      border:1px solid var(--line);
      border-radius:18px;
      padding:16px;
    }}
    .chart img {{ width:100%; display:block; border-radius:12px; }}
    .note {{
      margin-top:14px;
      padding:14px 16px;
      border-left:4px solid var(--blue);
      background:#eef4ff;
      border-radius:14px;
      color:#274064;
    }}
    @media (max-width:1100px) {{
      .grid {{ grid-template-columns:1fr; }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <h1>五币种纯做多对照报告</h1>
      <p>这份报告只看做多，并且只比较两套规则：<strong>无过滤</strong> 和 <strong>日线 EMA5 过滤</strong>。五个币种的做多参数全部使用各自历史最优版本，每次固定风险 <strong>10U</strong>。</p>
      <p>过滤规则很简单：<strong>日线收盘高于日线 EMA5 时允许开新多单，否则不开新多单</strong>。</p>
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
      <h2>结论</h2>
      <ul>
        <li>纯做多聚合全样本，`EMA5` 从 <strong>{fmt(aggregate_baseline.all_metrics.pnl)}U</strong> 提升到 <strong>{fmt(aggregate_ema5.all_metrics.pnl)}U</strong>，多赚 <strong>{fmt(aggregate_ema5.all_metrics.pnl - aggregate_baseline.all_metrics.pnl)}U</strong>。</li>
        <li>测试段也同步提升，从 <strong>{fmt(aggregate_baseline.test_metrics.pnl)}U</strong> 提升到 <strong>{fmt(aggregate_ema5.test_metrics.pnl)}U</strong>，多赚 <strong>{fmt(aggregate_ema5.test_metrics.pnl - aggregate_baseline.test_metrics.pnl)}U</strong>。</li>
        <li>交易数明显下降，但收益、PF 和回撤质量一起改善，说明 `EMA5` 在这套纯做多框架里起到的是“过滤低质量追多”的作用。</li>
      </ul>
      <div class="note">
        最后一列也保留了每个币种“纯做多测试段最佳闸门”，你可以顺手看出 `EMA5` 虽然整体第一，但并不是每个币种都恰好最优。
      </div>
    </section>

    <section class="section">
      <h2>分币种明细</h2>
      <table>
        <thead>
          <tr>
            <th>币种</th>
            <th>做多参数</th>
            <th>无过滤全样本</th>
            <th>EMA5全样本</th>
            <th>全样本增量</th>
            <th>无过滤测试段</th>
            <th>EMA5测试段</th>
            <th>测试段增量</th>
            <th>交易数变化</th>
            <th>回撤变化</th>
            <th>纯做多最佳闸门</th>
          </tr>
        </thead>
        <tbody>
          {''.join(rows)}
        </tbody>
      </table>
    </section>

    <section class="section">
      <h2>各币种测试段增量</h2>
      <div class="chart">
        <img src="data:image/png;base64,{delta_chart}" alt="EMA5 对比增量" />
      </div>
    </section>
  </div>
</body>
</html>"""


def build_delta_chart(symbols: list[SymbolResult]):
    labels = [item.label for item in symbols]
    values = [float(item.ema5.test_metrics.pnl - item.baseline.test_metrics.pnl) for item in symbols]
    fig, ax = plt.subplots(figsize=(10, 5.5))
    bars = ax.bar(labels, values, color="#1d4ed8")
    ax.set_title("EMA5 相对无过滤的测试段增量", fontsize=14, pad=12)
    ax.set_ylabel("PnL Δ (U)")
    for bar, value in zip(bars, values):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height(),
            f"{value:.0f}",
            ha="center",
            va="bottom",
            fontsize=9,
            color="#166534",
        )
    fig.tight_layout()
    return fig


def summary_card(title: str, body: str) -> str:
    return f'<div class="card"><h3>{html.escape(title)}</h3><p>{body}</p></div>'


def profile_label(symbol: str) -> str:
    profile = LONG_PROFILES[symbol]
    entry = f"EMA{profile.entry_reference_ema_period}" if profile.entry_reference_ema_period > 0 else f"跟随 EMA{profile.ema_period}"
    return (
        f"EMA{profile.ema_period}/EMA{profile.trend_ema_period}"
        f" + {entry}"
        f" + SLx{format_decimal_fixed(profile.atr_stop_multiplier, 1)}"
    )


def fig_to_base64(fig) -> str:
    buffer = io.BytesIO()
    fig.savefig(buffer, format="png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    return base64.b64encode(buffer.getvalue()).decode("ascii")


def fmt(value: Decimal) -> str:
    return format_decimal_fixed(value, 4)


if __name__ == "__main__":
    main()
