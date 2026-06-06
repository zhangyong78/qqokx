from __future__ import annotations

import json
import sys
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from okx_quant.backtest import _run_backtest_with_loaded_data, summarize_trade_exit_reasons
from okx_quant.candle_cache import load_candle_cache
from okx_quant.indicators import atr, ema
from okx_quant.models import Candle, StrategyConfig
from okx_quant.okx_client import OkxRestClient
from okx_quant.persistence import analysis_report_dir_path
from okx_quant.pricing import format_decimal_fixed
from okx_quant.strategies.adaptive_ema_rail import (
    ADAPTIVE_RAIL_STATE_BROKEN,
    ADAPTIVE_RAIL_STATE_CONFIRMED,
    adaptive_rail_candidate_periods,
    evaluate_adaptive_rail_signal,
)
from okx_quant.strategy_catalog import STRATEGY_ADAPTIVE_EMA_RAIL_LONG_ID


SYMBOL = "BTC-USDT-SWAP"
BAR = "4H"
INITIAL_CAPITAL = Decimal("10000")
RISK_AMOUNT = Decimal("10")
MAKER_FEE = Decimal("0.00015")
TAKER_FEE = Decimal("0.00036")
REPORT_DIR = analysis_report_dir_path()


def _config() -> StrategyConfig:
    return StrategyConfig(
        inst_id=SYMBOL,
        bar=BAR,
        ema_period=21,
        trend_ema_period=55,
        big_ema_period=233,
        atr_period=10,
        atr_stop_multiplier=Decimal("1.5"),
        atr_take_multiplier=Decimal("4"),
        order_size=Decimal("0"),
        trade_mode="cross",
        signal_mode="long_only",
        position_mode="net",
        environment="demo",
        tp_sl_trigger_type="mark",
        strategy_id=STRATEGY_ADAPTIVE_EMA_RAIL_LONG_ID,
        risk_amount=RISK_AMOUNT,
        entry_reference_ema_period=55,
        take_profit_mode="dynamic",
        max_entries_per_trend=1,
        dynamic_two_r_break_even=True,
        dynamic_fee_offset_enabled=True,
        time_stop_break_even_enabled=False,
        time_stop_break_even_bars=0,
        hold_close_exit_bars=0,
        backtest_initial_capital=INITIAL_CAPITAL,
        backtest_sizing_mode="fixed_risk",
        backtest_compounding=False,
        backtest_entry_slippage_rate=Decimal("0"),
        backtest_exit_slippage_rate=Decimal("0"),
        backtest_slippage_rate=Decimal("0"),
        backtest_funding_rate=Decimal("0"),
        rail_break_atr_ratio=Decimal("1.5"),
        rail_reclaim_bars=2,
        rail_switch_min_score_delta=Decimal("12"),
        rail_candidate_ema_periods=(21, 34, 55, 89),
        rail_fast_gate_enabled=True,
        rail_fast_gate_period=21,
        rail_fast_min_gap_ema200_atr=Decimal("5.0"),
        rail_fast_min_spread_trend_atr=Decimal("1.5"),
        rail_fast_max_recent_range_atr=Decimal("3.0"),
        rail_fast_recent_range_bars=8,
    )


def _fmt(value: Decimal | None, digits: int = 2) -> str:
    if value is None:
        return "-"
    return format_decimal_fixed(value, digits)


def _fmt_dt(ts: int) -> str:
    return datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")


def _build_rail_history(candles: list[Candle], config: StrategyConfig) -> dict[str, object]:
    closes = [candle.close for candle in candles]
    candidate_periods = adaptive_rail_candidate_periods(config)
    ema_periods = {period for period in candidate_periods if period > 0}
    ema_periods.add(int(config.trend_ema_period))
    if bool(config.rail_fast_gate_enabled) and int(config.rail_fast_gate_period) > 0:
        ema_periods.add(int(config.rail_fast_gate_period))
    ema_by_period = {period: ema(closes, period) for period in sorted(ema_periods)}
    ema200_values = ema(closes, 200)
    atr_values = atr(candles, config.atr_period)

    times: list[str] = []
    state_values: list[int | None] = []
    dominant_periods: list[int | None] = []
    dominant_scores: list[float | None] = []
    current_period: int | None = None
    for index, candle in enumerate(candles):
        snapshot = evaluate_adaptive_rail_signal(
            candles,
            index,
            ema_by_period=ema_by_period,
            ema200_values=ema200_values,
            atr_values=atr_values,
            config=config,
            current_period=current_period,
        )
        if snapshot.state == ADAPTIVE_RAIL_STATE_BROKEN:
            current_period = None
        elif snapshot.dominant_period is not None:
            current_period = snapshot.dominant_period

        times.append(_fmt_dt(candle.ts))
        dominant_periods.append(snapshot.dominant_period)
        dominant_scores.append(float(snapshot.dominant_score) if snapshot.dominant_period is not None else None)
        if snapshot.state == ADAPTIVE_RAIL_STATE_CONFIRMED:
            state_values.append(1)
        elif snapshot.state == ADAPTIVE_RAIL_STATE_BROKEN:
            state_values.append(-1)
        else:
            state_values.append(0)

    dominant_counter = Counter(period for period in dominant_periods if period is not None)
    return {
        "times": times,
        "state": state_values,
        "dominant_period": dominant_periods,
        "dominant_score": dominant_scores,
        "ema_by_period": {
            str(period): [float(value) if value is not None else None for value in values]
            for period, values in ema_by_period.items()
        },
        "ema200": [float(value) if value is not None else None for value in ema200_values],
        "dominant_distribution": [
            {"period": period, "bars": bars}
            for period, bars in sorted(dominant_counter.items(), key=lambda item: (-item[1], item[0]))
        ],
    }


def _trade_markers(result) -> dict[str, list[dict[str, object]]]:
    winners: list[dict[str, object]] = []
    losers: list[dict[str, object]] = []
    exits: list[dict[str, object]] = []
    for trade in result.trades:
        entry_item = {
            "x": _fmt_dt(trade.entry_ts),
            "y": float(trade.entry_price),
            "text": f"EMA{trade.adaptive_rail_period or '-'} | {float(trade.pnl):.2f}U | R {float(trade.r_multiple):.2f}",
        }
        if trade.pnl >= 0:
            winners.append(entry_item)
        else:
            losers.append(entry_item)
        exits.append(
            {
                "x": _fmt_dt(trade.exit_ts),
                "y": float(trade.exit_price),
                "text": f"{trade.exit_reason} | {float(trade.pnl):.2f}U",
            }
        )
    return {"winners": winners, "losers": losers, "exits": exits}


def _recent_trades(result) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for trade in reversed(result.trades[-12:]):
        rows.append(
            {
                "entry": _fmt_dt(trade.entry_ts),
                "exit": _fmt_dt(trade.exit_ts),
                "period": f"EMA{trade.adaptive_rail_period}" if trade.adaptive_rail_period else "-",
                "entry_price": float(trade.entry_price),
                "exit_price": float(trade.exit_price),
                "pnl": float(trade.pnl),
                "r": float(trade.r_multiple),
                "exit_reason": trade.exit_reason,
            }
        )
    return rows


def _build_payload(result, rail_history: dict[str, object]) -> dict[str, object]:
    report = result.report
    exit_summary = [{"label": label, "count": count} for label, count in summarize_trade_exit_reasons(result.trades)]
    return {
        "meta": {
            "symbol": SYMBOL,
            "bar": BAR,
            "title": "Adaptive Rail 自适应均线专题报告",
            "start": _fmt_dt(result.candles[0].ts),
            "end": _fmt_dt(result.candles[-1].ts),
            "initialCapital": float(result.initial_capital),
            "candidatePeriods": [21, 34, 55, 89],
            "ema21Gate": {
                "enabled": True,
                "closeMinusEma200Atr": 5.0,
                "ema21MinusEma55Atr": 1.5,
                "recentRangeAtr": 3.0,
                "recentBars": 8,
            },
        },
        "summary": {
            "endingEquity": float(report.ending_equity),
            "totalReturnPct": float(report.total_return_pct),
            "totalPnl": float(report.total_pnl),
            "maxDrawdownPct": float(report.max_drawdown_pct),
            "profitFactor": float(report.profit_factor) if report.profit_factor is not None else None,
            "avgR": float(report.average_r_multiple),
            "trades": report.total_trades,
            "winRatePct": float(report.win_rate),
            "takeProfitHits": report.take_profit_hits,
            "stopLossHits": report.stop_loss_hits,
        },
        "candles": {
            "time": [_fmt_dt(candle.ts) for candle in result.candles],
            "open": [float(candle.open) for candle in result.candles],
            "high": [float(candle.high) for candle in result.candles],
            "low": [float(candle.low) for candle in result.candles],
            "close": [float(candle.close) for candle in result.candles],
        },
        "netValueCurve": [float(value) for value in result.net_value_curve],
        "drawdownPctCurve": [float(value) for value in result.drawdown_pct_curve],
        "tradeMarkers": _trade_markers(result),
        "recentTrades": _recent_trades(result),
        "exitSummary": exit_summary,
        "railHistory": rail_history,
    }


def _build_html(payload: dict[str, object], report_path: Path) -> str:
    dataset = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Adaptive Rail 自适应均线专题报告</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<style>
:root {{
  --bg:#eef3f8; --panel:#fff; --ink:#152133; --muted:#667085; --line:#d9e2ec;
  --teal:#0f766e; --green:#16a34a; --red:#dc2626; --amber:#d97706; --blue:#1d4ed8; --navy:#0f172a;
}}
* {{ box-sizing:border-box; }}
body {{
  margin:0;
  background:
    radial-gradient(circle at top left, rgba(29,78,216,.10), transparent 30%),
    radial-gradient(circle at top right, rgba(15,118,110,.10), transparent 26%),
    var(--bg);
  color:var(--ink);
  font-family:"Microsoft YaHei","Segoe UI",Arial,sans-serif;
}}
.hero {{
  padding:38px 42px 34px;
  background:linear-gradient(135deg,#0f172a 0%,#17304d 52%,#0f766e 100%);
  color:#fff;
}}
.hero h1 {{ margin:0 0 10px; font-size:32px; }}
.hero p {{ margin:6px 0; color:#dbe7f3; line-height:1.7; max-width:1160px; }}
.wrap {{ max-width:1380px; margin:0 auto; padding:24px 24px 44px; }}
.grid {{ display:grid; gap:16px; }}
.grid-4 {{ grid-template-columns:repeat(4,minmax(0,1fr)); }}
.grid-2 {{ grid-template-columns:repeat(2,minmax(0,1fr)); }}
.card {{
  background:var(--panel);
  border:1px solid var(--line);
  border-radius:18px;
  padding:18px;
  box-shadow:0 8px 24px rgba(15,23,42,.05);
}}
.kpi .label {{ color:var(--muted); font-size:13px; margin-bottom:10px; }}
.kpi .value {{ color:var(--navy); font-size:28px; font-weight:800; line-height:1.1; }}
.kpi .sub {{ color:var(--muted); font-size:13px; margin-top:8px; }}
h2 {{ margin:30px 0 14px; font-size:22px; }}
h3 {{ margin:0 0 12px; font-size:16px; }}
.chart {{ height:520px; }}
.chart-sm {{ height:320px; }}
.legend {{ display:flex; gap:14px; flex-wrap:wrap; color:var(--muted); font-size:12px; margin-top:8px; }}
.legend span::before {{ content:""; display:inline-block; width:10px; height:10px; border-radius:999px; margin-right:6px; vertical-align:middle; }}
.winner::before {{ background:var(--green); }}
.loser::before {{ background:var(--red); }}
.exit::before {{ background:var(--amber); }}
.confirmed::before {{ background:var(--blue); }}
table {{ width:100%; border-collapse:collapse; font-size:13px; }}
th,td {{ padding:10px 10px; border-bottom:1px solid var(--line); text-align:right; }}
th:first-child,td:first-child {{ text-align:left; }}
th {{ color:#475467; background:#f8fbff; }}
.note {{ color:var(--muted); line-height:1.7; }}
.callout {{ border-left:5px solid var(--teal); background:linear-gradient(180deg, rgba(15,118,110,.08), rgba(15,118,110,.03)); border-radius:12px; padding:16px 18px; }}
@media (max-width: 960px) {{
  .grid-4,.grid-2 {{ grid-template-columns:1fr; }}
  .hero {{ padding:28px 22px; }}
  .wrap {{ padding:18px 14px 32px; }}
}}
</style>
</head>
<body>
<section class="hero">
  <h1>Adaptive Rail 自适应均线专题报告</h1>
  <p>这份页面只看你让我研究的 <strong>Adaptive EMA Rail</strong>，不混入五币主线。口径是当前正式默认底座：<strong>BTC 4H + 21/34/55/89 + EMA21 Gate</strong>，每笔固定风险 10U，初始资金 10000U。</p>
  <p>区间：{payload["meta"]["start"]} UTC 到 {payload["meta"]["end"]} UTC | 文件：{report_path}</p>
</section>

<main class="wrap">
  <div class="grid grid-4">
    <div class="card kpi"><div class="label">结束权益</div><div class="value" id="endingEquity"></div><div class="sub">初始资金 10,000U</div></div>
    <div class="card kpi"><div class="label">总收益率</div><div class="value" id="totalReturn"></div><div class="sub" id="totalPnl"></div></div>
    <div class="card kpi"><div class="label">最大回撤</div><div class="value" id="maxDrawdown"></div><div class="sub">净值口径</div></div>
    <div class="card kpi"><div class="label">PF / 平均R</div><div class="value" id="pfValue"></div><div class="sub" id="avgR"></div></div>
    <div class="card kpi"><div class="label">交易次数</div><div class="value" id="tradeCount"></div><div class="sub" id="winRate"></div></div>
    <div class="card kpi"><div class="label">轨道候选池</div><div class="value">21 / 34 / 55 / 89</div><div class="sub">默认正式底座</div></div>
    <div class="card kpi"><div class="label">EMA21 Gate</div><div class="value">开启</div><div class="sub">5ATR / 1.5ATR / 3ATR</div></div>
    <div class="card kpi"><div class="label">止盈 / 止损</div><div class="value" id="tpSl"></div><div class="sub">动态 TP + 2R 保本</div></div>
  </div>

  <h2>怎么看这页</h2>
  <div class="card callout">
    <p>这一页重点不是看它绝对赚多少，而是看它在 K 线上到底做了什么：主导轨道什么时候切换、交易更常围绕哪条 EMA 发生、结构破坏退出是否频繁，以及资金曲线是不是像我们前面研究结论那样“更干净但更低频”。</p>
  </div>

  <h2>K 线与轨道</h2>
  <div class="card">
    <div id="klineChart" class="chart"></div>
    <div class="legend">
      <span class="winner">盈利入场</span>
      <span class="loser">亏损入场</span>
      <span class="exit">平仓点</span>
      <span class="confirmed">主导轨道状态线</span>
    </div>
  </div>

  <h2>资金与回撤</h2>
  <div class="grid grid-2">
    <div class="card"><div id="equityChart" class="chart-sm"></div></div>
    <div class="card"><div id="drawdownChart" class="chart-sm"></div></div>
  </div>

  <h2>轨道分布与平仓原因</h2>
  <div class="grid grid-2">
    <div class="card"><div id="railDistributionChart" class="chart-sm"></div></div>
    <div class="card"><div id="exitReasonChart" class="chart-sm"></div></div>
  </div>

  <h2>最近 12 笔交易</h2>
  <div class="card">
    <table id="tradeTable"></table>
  </div>
</main>

<script>
const DATA = {dataset};

function fmtNumber(value, digits = 2) {{
  return Number(value || 0).toLocaleString('en-US', {{ minimumFractionDigits: digits, maximumFractionDigits: digits }});
}}
function fmtPct(value, digits = 2) {{ return `${{fmtNumber(value, digits)}}%`; }}

function fillSummary() {{
  const s = DATA.summary;
  document.getElementById('endingEquity').textContent = `${{fmtNumber(s.endingEquity, 2)}}U`;
  document.getElementById('totalReturn').textContent = fmtPct(s.totalReturnPct, 2);
  document.getElementById('totalPnl').textContent = `总盈亏 ${{fmtNumber(s.totalPnl, 2)}}U`;
  document.getElementById('maxDrawdown').textContent = fmtPct(s.maxDrawdownPct, 2);
  document.getElementById('pfValue').textContent = `${{fmtNumber(s.profitFactor, 4)}}`;
  document.getElementById('avgR').textContent = `平均R ${{fmtNumber(s.avgR, 4)}}`;
  document.getElementById('tradeCount').textContent = `${{s.trades}}`;
  document.getElementById('winRate').textContent = `胜率 ${{fmtPct(s.winRatePct, 2)}}`;
  document.getElementById('tpSl').textContent = `${{s.takeProfitHits}} / ${{s.stopLossHits}}`;
}}

function scatter(name, items, marker) {{
  return {{
    type:'scatter', mode:'markers', name,
    x: items.map(i => i.x),
    y: items.map(i => i.y),
    text: items.map(i => i.text),
    hovertemplate: '%{{x}}<br>%{{y:.4f}}<br>%{{text}}<extra></extra>',
    marker
  }};
}}

function renderKline() {{
  const c = DATA.candles;
  const rail = DATA.railHistory;
  const traces = [
    {{
      type:'candlestick',
      x:c.time, open:c.open, high:c.high, low:c.low, close:c.close,
      name:'K线',
      increasing:{{ line:{{ color:'#16a34a' }} }},
      decreasing:{{ line:{{ color:'#dc2626' }} }},
      xaxis:'x', yaxis:'y'
    }},
    {{
      type:'scatter',
      mode:'lines',
      name:'EMA21',
      x:c.time, y:rail.ema_by_period["21"],
      line:{{ color:'#2563eb', width:1.8 }}
    }},
    {{
      type:'scatter',
      mode:'lines',
      name:'EMA34',
      x:c.time, y:rail.ema_by_period["34"],
      line:{{ color:'#7c3aed', width:1.8 }}
    }},
    {{
      type:'scatter',
      mode:'lines',
      name:'EMA55',
      x:c.time, y:rail.ema_by_period["55"],
      line:{{ color:'#0f766e', width:1.8 }}
    }},
    {{
      type:'scatter',
      mode:'lines',
      name:'EMA89',
      x:c.time, y:rail.ema_by_period["89"],
      line:{{ color:'#d97706', width:1.8 }}
    }},
    {{
      type:'scatter',
      mode:'lines',
      name:'EMA200',
      x:c.time, y:rail.ema200,
      line:{{ color:'#111827', width:1.6, dash:'dot' }}
    }},
    scatter('盈利入场', DATA.tradeMarkers.winners, {{ color:'#16a34a', symbol:'triangle-up', size:10 }}),
    scatter('亏损入场', DATA.tradeMarkers.losers, {{ color:'#dc2626', symbol:'triangle-up', size:10 }}),
    scatter('平仓点', DATA.tradeMarkers.exits, {{ color:'#f59e0b', symbol:'circle', size:6 }}),
    {{
      type:'scatter',
      mode:'lines',
      name:'轨道状态',
      x:rail.times,
      y:rail.dominant_period.map(v => v == null ? null : v),
      yaxis:'y2',
      line:{{ color:'#1d4ed8', width:2 }},
      hovertemplate:'%{{x}}<br>主导轨道 EMA%{{y}}<extra></extra>'
    }}
  ];
  Plotly.newPlot('klineChart', traces, {{
    margin:{{ l:50, r:50, t:10, b:36 }},
    paper_bgcolor:'#ffffff',
    plot_bgcolor:'#ffffff',
    xaxis:{{ rangeslider:{{ visible:false }}, showgrid:false }},
    yaxis:{{ title:'Price', gridcolor:'#e8eef5' }},
    yaxis2:{{ title:'Dominant EMA', overlaying:'y', side:'right', showgrid:false, rangemode:'tozero' }},
    legend:{{ orientation:'h', yanchor:'bottom', y:1.02, xanchor:'left', x:0 }},
    hovermode:'x unified'
  }}, {{ responsive:true, displaylogo:false }});
}}

function renderEquity() {{
  Plotly.newPlot('equityChart', [{{
    type:'scatter', mode:'lines', name:'净值',
    x:DATA.candles.time, y:DATA.netValueCurve,
    line:{{ color:'#0f766e', width:2.4 }},
    fill:'tozeroy', fillcolor:'rgba(15,118,110,.14)'
  }}], {{
    margin:{{ l:48, r:20, t:28, b:32 }},
    paper_bgcolor:'#ffffff', plot_bgcolor:'#ffffff',
    title:'资金曲线', xaxis:{{ showgrid:false }}, yaxis:{{ gridcolor:'#e8eef5' }}
  }}, {{ responsive:true, displaylogo:false }});
}}

function renderDrawdown() {{
  Plotly.newPlot('drawdownChart', [{{
    type:'scatter', mode:'lines', name:'回撤%',
    x:DATA.candles.time, y:DATA.drawdownPctCurve,
    line:{{ color:'#c2410c', width:2.4 }},
    fill:'tozeroy', fillcolor:'rgba(194,65,12,.14)'
  }}], {{
    margin:{{ l:48, r:20, t:28, b:32 }},
    paper_bgcolor:'#ffffff', plot_bgcolor:'#ffffff',
    title:'回撤曲线', xaxis:{{ showgrid:false }}, yaxis:{{ gridcolor:'#e8eef5', ticksuffix:'%' }}
  }}, {{ responsive:true, displaylogo:false }});
}}

function renderRailDistribution() {{
  const dist = DATA.railHistory.dominant_distribution;
  Plotly.newPlot('railDistributionChart', [{{
    type:'bar',
    x:dist.map(i => `EMA${{i.period}}`),
    y:dist.map(i => i.bars),
    marker:{{ color:['#2563eb','#7c3aed','#0f766e','#d97706'] }}
  }}], {{
    margin:{{ l:48, r:20, t:28, b:36 }},
    paper_bgcolor:'#ffffff', plot_bgcolor:'#ffffff',
    title:'主导轨道 bars 分布', yaxis:{{ gridcolor:'#e8eef5' }}
  }}, {{ responsive:true, displaylogo:false }});
}}

function renderExitSummary() {{
  Plotly.newPlot('exitReasonChart', [{{
    type:'bar',
    x:DATA.exitSummary.map(i => i.label),
    y:DATA.exitSummary.map(i => i.count),
    marker:{{ color:'#1d4ed8' }}
  }}], {{
    margin:{{ l:48, r:20, t:28, b:72 }},
    paper_bgcolor:'#ffffff', plot_bgcolor:'#ffffff',
    title:'平仓原因分布', yaxis:{{ gridcolor:'#e8eef5' }}
  }}, {{ responsive:true, displaylogo:false }});
}}

function renderTrades() {{
  const rows = DATA.recentTrades;
  const html = [
    '<thead><tr><th>开仓</th><th>平仓</th><th>轨道</th><th>开仓价</th><th>平仓价</th><th>盈亏(U)</th><th>R</th><th>原因</th></tr></thead><tbody>'
  ];
  rows.forEach(row => {{
    html.push(`<tr>
      <td>${{row.entry}}</td>
      <td>${{row.exit}}</td>
      <td>${{row.period}}</td>
      <td>${{fmtNumber(row.entry_price, 4)}}</td>
      <td>${{fmtNumber(row.exit_price, 4)}}</td>
      <td>${{fmtNumber(row.pnl, 2)}}</td>
      <td>${{fmtNumber(row.r, 2)}}</td>
      <td>${{row.exit_reason}}</td>
    </tr>`);
  }});
  html.push('</tbody>');
  document.getElementById('tradeTable').innerHTML = html.join('');
}}

fillSummary();
renderKline();
renderEquity();
renderDrawdown();
renderRailDistribution();
renderExitSummary();
renderTrades();
</script>
</body>
</html>
"""


def main() -> None:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_path = REPORT_DIR / f"adaptive_ema_rail_kline_report_{stamp}.html"

    client = OkxRestClient()
    instrument = client.get_instrument(SYMBOL)
    candles = [candle for candle in load_candle_cache(SYMBOL, BAR, limit=None) if candle.confirmed]
    config = _config()
    result = _run_backtest_with_loaded_data(
        candles,
        instrument,
        config,
        data_source_note=f"local candle_cache full history | {SYMBOL} {BAR} | candles={len(candles)}",
        maker_fee_rate=MAKER_FEE,
        taker_fee_rate=TAKER_FEE,
    )
    payload = _build_payload(result, _build_rail_history(candles, config))
    out_path.write_text(_build_html(payload, out_path), encoding="utf-8")
    print(out_path)


if __name__ == "__main__":
    main()
