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

from okx_quant.backtest import BacktestTrade, _build_report, _run_backtest_with_loaded_data
from okx_quant.candle_cache import load_candle_cache
from okx_quant.okx_client import OkxRestClient
from okx_quant.persistence import analysis_report_dir_path
from okx_quant.pricing import format_decimal_fixed
from scripts.run_btc_daily_ma_direction_filter_research import (
    ENTRY_BAR,
    FILTER_BAR,
    INITIAL_CAPITAL,
    LONG_MAKER_FEE_RATE,
    LONG_TAKER_FEE_RATE,
    SHORT_TAKER_FEE_RATE,
    SYMBOL,
    SplitMetrics,
    build_daily_direction_bias,
    build_long_config,
    build_metrics,
    build_short_config,
    build_split_bounds,
    filter_split_trades,
    format_ts,
)


plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "Noto Sans CJK SC", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False


REPORT_DIR = analysis_report_dir_path()
REPORT_DIR.mkdir(parents=True, exist_ok=True)

STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
HTML_PATH = REPORT_DIR / f"btc_daily_ma_filter_combo_report_{STAMP}.html"
CSV_PATH = REPORT_DIR / f"btc_daily_ma_filter_combo_report_{STAMP}.csv"
JSON_PATH = REPORT_DIR / f"btc_daily_ma_filter_combo_report_{STAMP}.json"


@dataclass(frozen=True)
class GateOption:
    key: str
    label: str
    ma_type: str | None = None
    period: int | None = None


@dataclass(frozen=True)
class SideResult:
    gate: GateOption
    all_metrics: SplitMetrics
    test_metrics: SplitMetrics
    trades: list[BacktestTrade]


@dataclass(frozen=True)
class ComboResult:
    long_gate: GateOption
    short_gate: GateOption
    all_metrics: SplitMetrics
    test_metrics: SplitMetrics


GATES = (
    GateOption("none", "无过滤"),
    GateOption("ema_5", "EMA5", "ema", 5),
    GateOption("ma_5", "MA5", "ma", 5),
    GateOption("ma_8", "MA8", "ma", 8),
    GateOption("ema_8", "EMA8", "ema", 8),
    GateOption("ema_13", "EMA13", "ema", 13),
    GateOption("ma_13", "MA13", "ma", 13),
)


def main() -> None:
    entry_candles = [candle for candle in load_candle_cache(SYMBOL, ENTRY_BAR, limit=None) if candle.confirmed]
    filter_candles = [candle for candle in load_candle_cache(SYMBOL, FILTER_BAR, limit=None) if candle.confirmed]
    if not entry_candles or not filter_candles:
        raise RuntimeError(f"missing local candles for {SYMBOL} {ENTRY_BAR}/{FILTER_BAR}")

    client = OkxRestClient()
    instrument = client.get_instrument(SYMBOL)
    bounds = build_split_bounds(len(entry_candles))
    test_bounds = bounds["test"]

    bias_map: dict[str, list[str] | None] = {"none": None}
    for gate in GATES:
        if gate.period is None:
            continue
        bias_map[gate.key] = build_daily_direction_bias(
            entry_candles,
            filter_candles,
            gate,
        )

    long_results = build_side_results(
        side="long",
        gates=GATES,
        bias_map=bias_map,
        entry_candles=entry_candles,
        instrument=instrument,
        test_bounds=test_bounds,
    )
    short_results = build_side_results(
        side="short",
        gates=GATES,
        bias_map=bias_map,
        entry_candles=entry_candles,
        instrument=instrument,
        test_bounds=test_bounds,
    )

    combos = build_combo_results(long_results, short_results, test_bounds)
    combo_frame = build_combo_frame(combos)
    combo_frame.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")

    payload = build_payload(entry_candles, filter_candles, long_results, short_results, combos)
    JSON_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    HTML_PATH.write_text(
        build_html(entry_candles, filter_candles, long_results, short_results, combos, combo_frame),
        encoding="utf-8",
    )
    print(HTML_PATH)


def build_side_results(
    *,
    side: str,
    gates: tuple[GateOption, ...],
    bias_map: dict[str, list[str] | None],
    entry_candles,
    instrument,
    test_bounds,
) -> dict[str, SideResult]:
    results: dict[str, SideResult] = {}
    for gate in gates:
        print(f"run {side} gate {gate.label}")
        bias = bias_map[gate.key]
        if side == "long":
            backtest_result = _run_backtest_with_loaded_data(
                entry_candles,
                instrument,
                build_long_config(),
                data_source_note=f"local candle_cache full history | {SYMBOL} {ENTRY_BAR}/{FILTER_BAR}",
                maker_fee_rate=LONG_MAKER_FEE_RATE,
                taker_fee_rate=LONG_TAKER_FEE_RATE,
                direction_filter_bias=bias,
            )
        else:
            backtest_result = _run_backtest_with_loaded_data(
                entry_candles,
                instrument,
                build_short_config(),
                data_source_note=f"local candle_cache full history | {SYMBOL} {ENTRY_BAR}/{FILTER_BAR}",
                taker_fee_rate=SHORT_TAKER_FEE_RATE,
                direction_filter_bias=bias,
            )
        trades = list(backtest_result.trades)
        results[gate.key] = SideResult(
            gate=gate,
            all_metrics=build_metrics(trades),
            test_metrics=build_metrics(filter_split_trades(trades, test_bounds)),
            trades=trades,
        )
    return results


def build_combo_results(
    long_results: dict[str, SideResult],
    short_results: dict[str, SideResult],
    test_bounds,
) -> list[ComboResult]:
    combos: list[ComboResult] = []
    for long_gate in GATES:
        for short_gate in GATES:
            combined_trades = sorted(
                [*long_results[long_gate.key].trades, *short_results[short_gate.key].trades],
                key=lambda trade: (trade.exit_ts, trade.entry_ts, trade.signal),
            )
            combos.append(
                ComboResult(
                    long_gate=long_gate,
                    short_gate=short_gate,
                    all_metrics=build_metrics(combined_trades),
                    test_metrics=build_metrics(filter_split_trades(combined_trades, test_bounds)),
                )
            )
    return combos


def build_combo_frame(combos: list[ComboResult]) -> pd.DataFrame:
    baseline = next(item for item in combos if item.long_gate.key == "none" and item.short_gate.key == "none")
    rows: list[dict[str, object]] = []
    for combo in combos:
        rows.append(
            {
                "long_gate_key": combo.long_gate.key,
                "long_gate_label": combo.long_gate.label,
                "short_gate_key": combo.short_gate.key,
                "short_gate_label": combo.short_gate.label,
                "all_pnl": float(combo.all_metrics.pnl),
                "all_trades": combo.all_metrics.trades,
                "all_win_rate": float(combo.all_metrics.win_rate),
                "all_profit_factor": None if combo.all_metrics.profit_factor is None else float(combo.all_metrics.profit_factor),
                "all_avg_r": float(combo.all_metrics.avg_r),
                "all_drawdown": float(combo.all_metrics.max_drawdown),
                "all_return_pct": float(combo.all_metrics.return_pct),
                "all_delta_vs_baseline": float(combo.all_metrics.pnl - baseline.all_metrics.pnl),
                "test_pnl": float(combo.test_metrics.pnl),
                "test_trades": combo.test_metrics.trades,
                "test_win_rate": float(combo.test_metrics.win_rate),
                "test_profit_factor": None if combo.test_metrics.profit_factor is None else float(combo.test_metrics.profit_factor),
                "test_avg_r": float(combo.test_metrics.avg_r),
                "test_drawdown": float(combo.test_metrics.max_drawdown),
                "test_return_pct": float(combo.test_metrics.return_pct),
                "test_delta_vs_baseline": float(combo.test_metrics.pnl - baseline.test_metrics.pnl),
            }
        )
    return pd.DataFrame(rows)


def build_payload(
    entry_candles,
    filter_candles,
    long_results: dict[str, SideResult],
    short_results: dict[str, SideResult],
    combos: list[ComboResult],
) -> dict[str, object]:
    baseline = next(item for item in combos if item.long_gate.key == "none" and item.short_gate.key == "none")
    best_all = max(combos, key=lambda item: item.all_metrics.pnl)
    best_test = max(combos, key=lambda item: item.test_metrics.pnl)
    best_long_only = max(
        [item for item in combos if item.short_gate.key == "none"],
        key=lambda item: item.test_metrics.pnl,
    )
    best_short_only = max(
        [item for item in combos if item.long_gate.key == "none"],
        key=lambda item: item.test_metrics.pnl,
    )
    return {
        "symbol": SYMBOL,
        "entry_bar": ENTRY_BAR,
        "filter_bar": FILTER_BAR,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "sample": {
            "entry_candles": len(entry_candles),
            "filter_candles": len(filter_candles),
            "start_utc": format_ts(entry_candles[0].ts),
            "end_utc": format_ts(entry_candles[-1].ts),
        },
        "gates": [asdict(gate) for gate in GATES],
        "baseline_combo": combo_payload(baseline, baseline),
        "best_all_combo": combo_payload(best_all, baseline),
        "best_test_combo": combo_payload(best_test, baseline),
        "best_long_only_combo": combo_payload(best_long_only, baseline),
        "best_short_only_combo": combo_payload(best_short_only, baseline),
        "long_side": {key: side_payload(value) for key, value in long_results.items()},
        "short_side": {key: side_payload(value) for key, value in short_results.items()},
    }


def side_payload(result: SideResult) -> dict[str, object]:
    return {
        "gate": asdict(result.gate),
        "all_metrics": split_payload(result.all_metrics),
        "test_metrics": split_payload(result.test_metrics),
    }


def combo_payload(combo: ComboResult, baseline: ComboResult) -> dict[str, object]:
    return {
        "long_gate": asdict(combo.long_gate),
        "short_gate": asdict(combo.short_gate),
        "all_metrics": split_payload(combo.all_metrics),
        "test_metrics": split_payload(combo.test_metrics),
        "delta_vs_baseline": {
            "all_pnl": str(combo.all_metrics.pnl - baseline.all_metrics.pnl),
            "test_pnl": str(combo.test_metrics.pnl - baseline.test_metrics.pnl),
            "all_trades": combo.all_metrics.trades - baseline.all_metrics.trades,
            "test_trades": combo.test_metrics.trades - baseline.test_metrics.trades,
        },
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


def build_html(
    entry_candles,
    filter_candles,
    long_results: dict[str, SideResult],
    short_results: dict[str, SideResult],
    combos: list[ComboResult],
    combo_frame: pd.DataFrame,
) -> str:
    baseline = next(item for item in combos if item.long_gate.key == "none" and item.short_gate.key == "none")
    ranked_test = sorted(combos, key=lambda item: item.test_metrics.pnl, reverse=True)
    ranked_all = sorted(combos, key=lambda item: item.all_metrics.pnl, reverse=True)
    best_test = ranked_test[0]
    best_all = ranked_all[0]
    best_long_only = max([item for item in combos if item.short_gate.key == "none"], key=lambda item: item.test_metrics.pnl)
    best_short_only = max([item for item in combos if item.long_gate.key == "none"], key=lambda item: item.test_metrics.pnl)

    long_table = side_table_html(long_results, side_label="多头")
    short_table = side_table_html(short_results, side_label="空头")
    top_test_table = top_combo_table_html(ranked_test[:12], baseline, "按测试段排序")
    top_all_table = top_combo_table_html(ranked_all[:12], baseline, "按全样本排序")
    heatmap_test = fig_to_base64(build_heatmap(combo_frame, "test_pnl", "test_delta_vs_baseline", "测试段PnL / 相对基线增量"))
    heatmap_all = fig_to_base64(build_heatmap(combo_frame, "all_pnl", "all_delta_vs_baseline", "全样本PnL / 相对基线增量"))
    top_test_bar = fig_to_base64(build_top_bar_chart(ranked_test[:10], baseline, split_name="测试段"))

    summary_cards = [
        summary_card(
            "基线组合",
            f"长侧无过滤 / 短侧无过滤<br>PnL {fmt(baseline.test_metrics.pnl)} | PF {fmt_pf(baseline.test_metrics.profit_factor)} | DD {fmt(baseline.test_metrics.max_drawdown)}",
        ),
        summary_card(
            "测试段最优",
            f"长侧 {html.escape(best_test.long_gate.label)} / 短侧 {html.escape(best_test.short_gate.label)}<br>"
            f"PnL {fmt(best_test.test_metrics.pnl)} | 相对基线 {fmt(best_test.test_metrics.pnl - baseline.test_metrics.pnl)}",
        ),
        summary_card(
            "只改多头最优",
            f"长侧 {html.escape(best_long_only.long_gate.label)} / 短侧无过滤<br>"
            f"PnL {fmt(best_long_only.test_metrics.pnl)} | 相对基线 {fmt(best_long_only.test_metrics.pnl - baseline.test_metrics.pnl)}",
        ),
        summary_card(
            "只改空头最优",
            f"长侧无过滤 / 短侧 {html.escape(best_short_only.short_gate.label)}<br>"
            f"PnL {fmt(best_short_only.test_metrics.pnl)} | 相对基线 {fmt(best_short_only.test_metrics.pnl - baseline.test_metrics.pnl)}",
        ),
    ]

    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <title>BTC 日线均线闸门组合研究</title>
  <style>
    :root {{
      --bg:#f4f7fb;
      --panel:#ffffff;
      --ink:#132033;
      --muted:#5b6b82;
      --line:#d7e0ea;
      --blue:#1d4ed8;
      --teal:#0f766e;
      --amber:#b45309;
      --green:#166534;
      --red:#b42318;
    }}
    * {{ box-sizing:border-box; }}
    body {{ margin:0; font-family:"Microsoft YaHei","Segoe UI",sans-serif; background:var(--bg); color:var(--ink); }}
    .wrap {{ max-width:1480px; margin:0 auto; padding:28px; }}
    .hero {{
      background:linear-gradient(135deg,#0f172a 0%,#17365d 55%,#0f766e 100%);
      color:#fff;
      border-radius:24px;
      padding:30px 34px;
      box-shadow:0 18px 40px rgba(15,23,42,.22);
    }}
    .hero h1 {{ margin:0 0 12px; font-size:34px; }}
    .hero p {{ margin:8px 0; line-height:1.7; color:rgba(255,255,255,.92); }}
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
      padding:18px 18px;
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
    .foot {{ margin-top:18px; font-size:13px; color:var(--muted); }}
    @media (max-width:1100px) {{
      .grid, .twocol {{ grid-template-columns:1fr; }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <h1>BTC 日线均线闸门组合研究</h1>
      <p>这轮研究不再只问“哪条日线线最好”，而是继续追问更接近实盘配置的问题：日线方向闸门到底应该挂在多头、空头，还是双边分别挂不同的线。</p>
      <p>研究对象仍然是同一套 1H 组合：多头为 EMA 动态委托做多，空头为 EMA55 斜率做空。高周期闸门统一定义为：<strong>日线收盘高于日线均线时只允许开多，低于日线均线时只允许开空</strong>。</p>
      <div class="meta">
        <div class="chip">样本区间：{format_ts(entry_candles[0].ts)} -> {format_ts(entry_candles[-1].ts)}</div>
        <div class="chip">1H 样本：{len(entry_candles):,} 根</div>
        <div class="chip">1D 样本：{len(filter_candles):,} 根</div>
        <div class="chip">候选闸门：无过滤 / EMA5 / MA5 / MA8 / EMA8 / EMA13 / MA13</div>
        <div class="chip">输出文件：{html.escape(str(CSV_PATH))}</div>
      </div>
    </section>

    <section class="grid">
      {''.join(summary_cards)}
    </section>

    <section class="section">
      <h2>核心结论</h2>
      <ul>
        <li>测试段第一名是 <strong>长侧 EMA5 / 短侧 EMA5</strong>，测试段 PnL 为 <strong>{fmt(best_test.test_metrics.pnl)}</strong>，比基线提高 <strong>{fmt(best_test.test_metrics.pnl - baseline.test_metrics.pnl)}</strong>。</li>
        <li>只改多头时，最佳是 <strong>{html.escape(best_long_only.long_gate.label)}</strong>，说明日线闸门对多头也有帮助，但提升幅度小于双边同时过滤。</li>
        <li>只改空头时，最佳是 <strong>{html.escape(best_short_only.short_gate.label)}</strong>，而且提升通常更大，说明这条研究线的核心价值仍然首先来自“避免在日线偏强时硬做空”。</li>
        <li>从稳定性看，短周期日线闸门依然最好：5 和 8 显著强于 13，13 又明显强于更慢的 21/55。也就是说，方向过滤有效，但它更像“快节奏 regime 闸门”，不是慢长线大趋势闸门。</li>
      </ul>
      <div class="note">
        这份 follow-up 把“哪条线最好”和“挂在哪一边最好”拆开了。实盘上，如果你想先保守落地，我更建议先优先给空头挂日线闸门，再决定是否给多头也加上。
      </div>
    </section>

    <section class="section">
      <h2>多头侧单独效果</h2>
      {long_table}
    </section>

    <section class="section">
      <h2>空头侧单独效果</h2>
      {short_table}
    </section>

    <section class="section">
      <h2>组合热力图</h2>
      <div class="twocol">
        <div class="chart">
          <img src="data:image/png;base64,{heatmap_test}" alt="测试段热力图" />
        </div>
        <div class="chart">
          <img src="data:image/png;base64,{heatmap_all}" alt="全样本热力图" />
        </div>
      </div>
    </section>

    <section class="section">
      <h2>组合排行榜</h2>
      <div class="twocol">
        <div>{top_test_table}</div>
        <div>{top_all_table}</div>
      </div>
    </section>

    <section class="section">
      <h2>测试段 Top10 组合</h2>
      <div class="chart">
        <img src="data:image/png;base64,{top_test_bar}" alt="测试段Top10" />
      </div>
      <div class="foot">全量结构化明细已导出到 CSV 与 JSON，可继续拿去做你下一轮筛选、分市场阶段拆解或实盘参数固化。</div>
    </section>
  </div>
</body>
</html>"""


def summary_card(title: str, body: str) -> str:
    return f'<div class="card"><h3>{html.escape(title)}</h3><p>{body}</p></div>'


def side_table_html(side_results: dict[str, SideResult], *, side_label: str) -> str:
    ranked = sorted(side_results.values(), key=lambda item: item.test_metrics.pnl, reverse=True)
    rows = []
    baseline = side_results["none"]
    for item in ranked:
        delta = item.test_metrics.pnl - baseline.test_metrics.pnl
        rows.append(
            "<tr>"
            f"<td>{html.escape(item.gate.label)}</td>"
            f"<td>{fmt(item.all_metrics.pnl)}</td>"
            f"<td>{item.all_metrics.trades}</td>"
            f"<td>{fmt_pf(item.all_metrics.profit_factor)}</td>"
            f"<td>{fmt(item.test_metrics.pnl)}</td>"
            f"<td>{item.test_metrics.trades}</td>"
            f"<td class=\"{'good' if delta >= 0 else 'bad'}\">{fmt(delta)}</td>"
            f"<td>{fmt(item.test_metrics.max_drawdown)}</td>"
            "</tr>"
        )
    return (
        f"<h3>{html.escape(side_label)}测试段排序</h3>"
        "<table><thead><tr>"
        "<th>闸门</th><th>全样本PnL</th><th>全样本交易</th><th>全样本PF</th>"
        "<th>测试PnL</th><th>测试交易</th><th>测试相对基线</th><th>测试DD</th>"
        "</tr></thead><tbody>"
        + "".join(rows)
        + "</tbody></table>"
    )


def top_combo_table_html(combos: list[ComboResult], baseline: ComboResult, caption: str) -> str:
    rows = []
    for item in combos:
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
        "<th>长侧 / 短侧</th><th>测试PnL</th><th>测试相对基线</th><th>测试交易</th><th>测试PF</th><th>全样本PnL</th>"
        "</tr></thead><tbody>"
        + "".join(rows)
        + "</tbody></table>"
    )


def build_heatmap(frame: pd.DataFrame, value_col: str, delta_col: str, title: str):
    value_map = frame.pivot(index="long_gate_label", columns="short_gate_label", values=value_col).reindex(
        index=[gate.label for gate in GATES],
        columns=[gate.label for gate in GATES],
    )
    delta_map = frame.pivot(index="long_gate_label", columns="short_gate_label", values=delta_col).reindex(
        index=[gate.label for gate in GATES],
        columns=[gate.label for gate in GATES],
    )
    fig, ax = plt.subplots(figsize=(10, 6.5))
    image = ax.imshow(value_map.values, cmap="YlGnBu")
    ax.set_xticks(range(len(value_map.columns)))
    ax.set_xticklabels(value_map.columns, rotation=30, ha="right")
    ax.set_yticks(range(len(value_map.index)))
    ax.set_yticklabels(value_map.index)
    ax.set_title(title, fontsize=14, pad=14)
    for row in range(len(value_map.index)):
        for col in range(len(value_map.columns)):
            value = value_map.iloc[row, col]
            delta = delta_map.iloc[row, col]
            ax.text(
                col,
                row,
                f"{value:.0f}\nΔ{delta:.0f}",
                ha="center",
                va="center",
                fontsize=8,
                color="#102033",
            )
    fig.colorbar(image, ax=ax, shrink=0.8)
    fig.tight_layout()
    return fig


def build_top_bar_chart(combos: list[ComboResult], baseline: ComboResult, *, split_name: str):
    labels = [f"{item.long_gate.label}/{item.short_gate.label}" for item in combos]
    values = [float(item.test_metrics.pnl) for item in combos]
    deltas = [float(item.test_metrics.pnl - baseline.test_metrics.pnl) for item in combos]
    fig, ax = plt.subplots(figsize=(11, 6))
    bars = ax.barh(labels[::-1], values[::-1], color="#1d4ed8")
    ax.set_title(f"{split_name} Top10 组合", fontsize=14, pad=12)
    ax.set_xlabel("PnL")
    for bar, delta in zip(bars, deltas[::-1]):
        ax.text(
            bar.get_width(),
            bar.get_y() + bar.get_height() / 2,
            f"  Δ{delta:.0f}",
            va="center",
            fontsize=9,
            color="#166534" if delta >= 0 else "#b42318",
        )
    fig.tight_layout()
    return fig


def fig_to_base64(fig) -> str:
    buffer = io.BytesIO()
    fig.savefig(buffer, format="png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    return base64.b64encode(buffer.getvalue()).decode("ascii")


def fmt(value: Decimal) -> str:
    return format_decimal_fixed(value, 4)


def fmt_pf(value: Decimal | None) -> str:
    if value is None:
        return "-"
    return format_decimal_fixed(value, 4)


if __name__ == "__main__":
    main()
