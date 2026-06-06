from __future__ import annotations

import json
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from okx_quant.candle_cache import load_candle_cache
from okx_quant.persistence import analysis_report_dir_path


REPORT_DIR = analysis_report_dir_path()
STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
HTML_PATH = REPORT_DIR / f"multi_coin_trade_kline_dashboard_{STAMP}.html"

COIN_ORDER = ["BTC", "ETH", "SOL", "BNB", "DOGE"]
SCOPES = ("full", "common")
DEFAULT_SCENARIO = "bjt_08"
DEFAULT_SCOPE = "common"
PAD_HOURS = 240


def _find_latest_input_files() -> tuple[Path, Path, Path]:
    trade_files = sorted(
        REPORT_DIR.glob("leadership_daily_boundary_compare_trades_*.csv"),
        key=lambda item: item.stat().st_mtime,
    )
    if not trade_files:
        raise RuntimeError("No leadership daily boundary compare trade csv found.")
    trade_path = trade_files[-1]
    match = re.search(r"leadership_daily_boundary_compare_trades_(\d{8}_\d{6})\.csv$", trade_path.name)
    if not match:
        raise RuntimeError(f"Unexpected trade file name: {trade_path.name}")
    stamp = match.group(1)
    json_path = REPORT_DIR / f"leadership_daily_boundary_compare_report_{stamp}.json"
    params_path = REPORT_DIR / f"leadership_daily_boundary_compare_params_{stamp}.csv"
    if not json_path.exists():
        raise RuntimeError(f"Missing report json: {json_path}")
    if not params_path.exists():
        raise RuntimeError(f"Missing params csv: {params_path}")
    return trade_path, json_path, params_path


def _parse_utc_text(value: str) -> int:
    dt = datetime.strptime(value.strip(), "%Y-%m-%d %H:%M:%S UTC").replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def _filter_scope(frame: pd.DataFrame, *, start_ts: int | None = None, end_ts: int | None = None) -> pd.DataFrame:
    out = frame.copy()
    if start_ts is not None:
        out = out[out["entry_ts"] >= start_ts]
    if end_ts is not None:
        out = out[out["exit_ts"] <= end_ts]
    return out.sort_values(["exit_ts", "entry_ts", "coin", "side"]).reset_index(drop=True)


def _metrics(frame: pd.DataFrame) -> dict[str, float]:
    if frame.empty:
        return {
            "trades": 0.0,
            "total_pnl_u": 0.0,
            "win_rate_pct": 0.0,
            "profit_factor": 0.0,
            "avg_r": 0.0,
            "avg_hold_hours": 0.0,
            "long_trades": 0.0,
            "short_trades": 0.0,
            "long_pnl_u": 0.0,
            "short_pnl_u": 0.0,
        }
    pnl = frame["pnl_u"].astype(float)
    gross_profit = float(pnl[pnl > 0].sum())
    gross_loss = abs(float(pnl[pnl < 0].sum()))
    long_frame = frame[frame["side"] == "long"]
    short_frame = frame[frame["side"] == "short"]
    return {
        "trades": float(len(frame)),
        "total_pnl_u": float(pnl.sum()),
        "win_rate_pct": float((pnl > 0).mean() * 100),
        "profit_factor": gross_profit / gross_loss if gross_loss > 0 else 0.0,
        "avg_r": float(frame["r_multiple"].astype(float).mean()) if len(frame) else 0.0,
        "avg_hold_hours": float(frame["hold_hours"].astype(float).mean()) if len(frame) else 0.0,
        "long_trades": float(len(long_frame)),
        "short_trades": float(len(short_frame)),
        "long_pnl_u": float(long_frame["pnl_u"].astype(float).sum()) if len(long_frame) else 0.0,
        "short_pnl_u": float(short_frame["pnl_u"].astype(float).sum()) if len(short_frame) else 0.0,
    }


def _fmt_dt(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")


def _build_markers(frame: pd.DataFrame) -> dict[str, list[dict[str, object]]]:
    long_entries: list[dict[str, object]] = []
    long_exits: list[dict[str, object]] = []
    short_entries: list[dict[str, object]] = []
    short_exits: list[dict[str, object]] = []
    for row in frame.itertuples(index=False):
        entry = {
            "x": _fmt_dt(int(row.entry_ts)),
            "y": float(row.entry_price),
            "text": f"{row.side[0].upper()}入 {float(row.pnl_u):.2f}U",
        }
        exit_item = {
            "x": _fmt_dt(int(row.exit_ts)),
            "y": float(row.exit_price),
            "text": f"{row.side[0].upper()}出 {float(row.pnl_u):.2f}U",
        }
        if row.side == "long":
            long_entries.append(entry)
            long_exits.append(exit_item)
        else:
            short_entries.append(entry)
            short_exits.append(exit_item)
    return {
        "longEntries": long_entries,
        "longExits": long_exits,
        "shortEntries": short_entries,
        "shortExits": short_exits,
    }


def _build_recent_trades(frame: pd.DataFrame) -> list[dict[str, object]]:
    recent = frame.sort_values(["exit_ts", "entry_ts"], ascending=False).head(8)
    rows: list[dict[str, object]] = []
    for row in recent.itertuples(index=False):
        rows.append(
            {
                "side": row.side,
                "entryTime": _fmt_dt(int(row.entry_ts)),
                "exitTime": _fmt_dt(int(row.exit_ts)),
                "entryPrice": float(row.entry_price),
                "exitPrice": float(row.exit_price),
                "pnlU": float(row.pnl_u),
                "r": float(row.r_multiple),
                "exitReason": str(row.exit_reason),
            }
        )
    return rows


def _build_coin_profile(params: pd.DataFrame, coin: str) -> dict[str, str]:
    rows = params[params["coin"] == coin].copy()
    if rows.empty:
        return {"longRule": "-", "shortRule": "-"}

    def pack(side: str) -> str:
        side_rows = rows[rows["side"] == side]
        if side_rows.empty:
            return "-"
        item = side_rows.iloc[0]
        return (
            f"{item['strategy_family']} | {item['fast_line']} / {item['trend_line']} | "
            f"{item['daily_gate_rule']} | {item['take_profit_model']}"
        )

    return {
        "longRule": pack("long"),
        "shortRule": pack("short"),
    }


def _build_dataset() -> dict[str, object]:
    trade_path, json_path, params_path = _find_latest_input_files()
    payload = json.loads(json_path.read_text(encoding="utf-8"))
    trades = pd.read_csv(trade_path)
    params = pd.read_csv(params_path)

    common_start_ts = _parse_utc_text(payload["common_interval"]["start_utc"])
    common_end_ts = _parse_utc_text(payload["common_interval"]["end_utc"])

    dataset: dict[str, object] = {
        "meta": {
            "title": "五币种交易 K 线总览",
            "tradeCsv": str(trade_path),
            "sourceJson": str(json_path),
            "paramsCsv": str(params_path),
            "riskPerTradeU": payload.get("risk_per_trade_u", "10"),
            "initialCapitalU": payload.get("initial_capital_u", "10000"),
            "commonStart": _fmt_dt(common_start_ts),
            "commonEnd": _fmt_dt(common_end_ts),
            "commonStartMs": common_start_ts,
            "commonEndMs": common_end_ts,
            "defaultScenario": DEFAULT_SCENARIO,
            "defaultScope": DEFAULT_SCOPE,
        },
        "coins": {},
        "overview": {},
    }

    scenario_map = {
        "bjt_00": "北京时间0点日线",
        "bjt_08": "北京时间8点日线",
    }
    dataset["meta"]["scenarioLabels"] = scenario_map

    overview: dict[str, dict[str, object]] = {}
    for scenario_key in scenario_map:
        scenario_trades = trades[trades["scenario"] == scenario_key].copy()
        overview[scenario_key] = {}
        for scope in SCOPES:
            scoped = (
                _filter_scope(scenario_trades, start_ts=common_start_ts, end_ts=common_end_ts)
                if scope == "common"
                else scenario_trades.sort_values(["exit_ts", "entry_ts", "coin", "side"]).reset_index(drop=True)
            )
            rows: list[dict[str, object]] = []
            for coin in COIN_ORDER:
                coin_frame = scoped[scoped["coin"] == coin].copy()
                row = {"coin": coin, **_metrics(coin_frame)}
                rows.append(row)
            overview[scenario_key][scope] = rows
    dataset["overview"] = overview

    for coin in COIN_ORDER:
        coin_trades = trades[trades["coin"] == coin].copy()
        if coin_trades.empty:
            continue
        symbol = str(coin_trades["symbol"].iloc[0])
        earliest_trade_ts = int(coin_trades["entry_ts"].min()) - PAD_HOURS * 3_600_000
        candles = [candle for candle in load_candle_cache(symbol, "1H", limit=None) if candle.confirmed]
        candles = [candle for candle in candles if candle.ts >= earliest_trade_ts]
        candle_payload = {
            "time": [_fmt_dt(candle.ts) for candle in candles],
            "open": [float(candle.open) for candle in candles],
            "high": [float(candle.high) for candle in candles],
            "low": [float(candle.low) for candle in candles],
            "close": [float(candle.close) for candle in candles],
        }
        views: dict[str, object] = {}
        for scenario_key in scenario_map:
            scenario_trades = coin_trades[coin_trades["scenario"] == scenario_key].copy()
            for scope in SCOPES:
                scoped = (
                    _filter_scope(scenario_trades, start_ts=common_start_ts, end_ts=common_end_ts)
                    if scope == "common"
                    else scenario_trades.sort_values(["exit_ts", "entry_ts"]).reset_index(drop=True)
                )
                key = f"{scenario_key}__{scope}"
                views[key] = {
                    "metrics": _metrics(scoped),
                    "markers": _build_markers(scoped),
                    "recentTrades": _build_recent_trades(scoped),
                }
        dataset["coins"][coin] = {
            "coin": coin,
            "symbol": symbol,
            "profile": _build_coin_profile(params, coin),
            "candles": candle_payload,
            "views": views,
        }
    return dataset


def _build_html(dataset: dict[str, object]) -> str:
    data_json = json.dumps(dataset, ensure_ascii=False, separators=(",", ":"))
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>五币种交易 K 线总览</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<style>
:root {{
  --bg:#eef3f8;
  --panel:#ffffff;
  --ink:#142033;
  --muted:#667085;
  --line:#d8e2ec;
  --blue:#1d4ed8;
  --teal:#0f766e;
  --green:#16a34a;
  --red:#dc2626;
  --amber:#d97706;
}}
* {{ box-sizing:border-box; }}
body {{
  margin:0;
  font-family:"Microsoft YaHei","Segoe UI",Arial,sans-serif;
  background:
    radial-gradient(circle at top left, rgba(29,78,216,.10), transparent 28%),
    radial-gradient(circle at top right, rgba(15,118,110,.08), transparent 26%),
    var(--bg);
  color:var(--ink);
}}
.hero {{
  padding:36px 42px;
  color:#fff;
  background:linear-gradient(135deg,#0f172a 0%,#17304d 58%,#0f766e 100%);
}}
.hero h1 {{ margin:0 0 10px; font-size:32px; }}
.hero p {{ margin:6px 0; color:#dbe7f3; line-height:1.7; max-width:1180px; }}
.wrap {{ max-width:1440px; margin:0 auto; padding:24px 24px 42px; }}
.toolbar {{
  display:flex;
  flex-wrap:wrap;
  gap:14px;
  align-items:end;
  background:var(--panel);
  border:1px solid var(--line);
  border-radius:18px;
  padding:16px 18px;
  box-shadow:0 8px 24px rgba(15,23,42,.05);
}}
.control label {{
  display:block;
  color:var(--muted);
  font-size:13px;
  margin-bottom:6px;
}}
.control select {{
  min-width:220px;
  border:1px solid var(--line);
  border-radius:10px;
  padding:10px 12px;
  font:inherit;
  background:#fff;
}}
.note {{
  color:var(--muted);
  font-size:13px;
}}
.section {{
  margin-top:20px;
  background:var(--panel);
  border:1px solid var(--line);
  border-radius:18px;
  padding:18px;
  box-shadow:0 8px 24px rgba(15,23,42,.05);
}}
.section h2 {{
  margin:0 0 14px;
  font-size:22px;
}}
.overview-table, .trade-table {{
  width:100%;
  border-collapse:collapse;
  font-size:13px;
}}
.overview-table th, .overview-table td,
.trade-table th, .trade-table td {{
  padding:10px 10px;
  border-bottom:1px solid var(--line);
  text-align:right;
}}
.overview-table th:first-child, .overview-table td:first-child,
.trade-table th:first-child, .trade-table td:first-child {{
  text-align:left;
}}
.overview-table th, .trade-table th {{
  color:#475467;
  background:#f8fbff;
}}
.coin-grid {{
  display:grid;
  gap:18px;
  margin-top:20px;
}}
.coin-card {{
  background:var(--panel);
  border:1px solid var(--line);
  border-radius:22px;
  padding:18px;
  box-shadow:0 10px 24px rgba(15,23,42,.05);
}}
.coin-head {{
  display:flex;
  justify-content:space-between;
  gap:16px;
  flex-wrap:wrap;
  align-items:flex-start;
  margin-bottom:12px;
}}
.coin-head h3 {{
  margin:0 0 6px;
  font-size:24px;
}}
.pill {{
  display:inline-block;
  padding:4px 10px;
  border-radius:999px;
  background:#dbeafe;
  color:#1e3a8a;
  font-size:12px;
  font-weight:700;
}}
.kpi-grid {{
  display:grid;
  grid-template-columns:repeat(5,minmax(0,1fr));
  gap:12px;
  margin:12px 0 16px;
}}
.kpi {{
  border:1px solid var(--line);
  border-radius:14px;
  padding:12px 14px;
  background:#fbfdff;
}}
.kpi .label {{
  color:var(--muted);
  font-size:12px;
  margin-bottom:8px;
}}
.kpi .value {{
  font-size:24px;
  font-weight:800;
  color:#0f172a;
}}
.kpi .sub {{
  color:var(--muted);
  font-size:12px;
  margin-top:6px;
}}
.rules {{
  display:grid;
  grid-template-columns:repeat(2,minmax(0,1fr));
  gap:14px;
  margin-bottom:14px;
}}
.rule-box {{
  border:1px solid var(--line);
  border-radius:14px;
  padding:12px 14px;
  background:#f8fbff;
}}
.rule-box strong {{
  display:block;
  margin-bottom:8px;
}}
.chart {{
  height:460px;
}}
.trade-wrap {{
  margin-top:14px;
}}
.legend {{
  display:flex;
  gap:14px;
  flex-wrap:wrap;
  color:var(--muted);
  font-size:12px;
  margin:8px 0 4px;
}}
.legend span::before {{
  content:"";
  display:inline-block;
  width:10px;
  height:10px;
  border-radius:999px;
  margin-right:6px;
  vertical-align:middle;
}}
.legend .long-entry::before {{ background:var(--green); }}
.legend .long-exit::before {{ background:#22c55e; }}
.legend .short-entry::before {{ background:var(--red); }}
.legend .short-exit::before {{ background:var(--amber); }}
@media (max-width: 1080px) {{
  .kpi-grid, .rules {{ grid-template-columns:1fr; }}
}}
</style>
</head>
<body>
<section class="hero">
  <h1>五币种交易 K 线总览</h1>
  <p>这份面板对应你现在那套五币种多空组合研究，口径是每笔固定风险 <strong>{dataset["meta"]["riskPerTradeU"]}U</strong>、初始资金 <strong>{dataset["meta"]["initialCapitalU"]}U</strong>。页面会把 <strong>BTC / ETH / SOL / BNB / DOGE</strong> 的 1H K 线和交易点位画出来，方便你直接看每个币的大致交易节奏。</p>
  <p>默认视角是 <strong>北京时间8点日线 + common区间</strong>，因为这个更适合五币种横向比较。原始文件：{dataset["meta"]["tradeCsv"]}</p>
</section>

<main class="wrap">
  <section class="toolbar">
    <div class="control">
      <label for="scenarioSelect">日线标准</label>
      <select id="scenarioSelect">
        <option value="bjt_00">北京时间0点日线</option>
        <option value="bjt_08" selected>北京时间8点日线</option>
      </select>
    </div>
    <div class="control">
      <label for="scopeSelect">观察范围</label>
      <select id="scopeSelect">
        <option value="full">Full History</option>
        <option value="common" selected>Common Interval</option>
      </select>
    </div>
    <div class="note">
      common 区间：{dataset["meta"]["commonStart"]} UTC 到 {dataset["meta"]["commonEnd"]} UTC
    </div>
  </section>

  <section class="section">
    <h2>总览</h2>
    <table class="overview-table" id="overviewTable"></table>
  </section>

  <section class="coin-grid" id="coinGrid"></section>
</main>

<script>
const DATASET = {data_json};

function fmtNumber(value, digits = 2) {{
  return Number(value || 0).toLocaleString('en-US', {{
    minimumFractionDigits: digits,
    maximumFractionDigits: digits
  }});
}}

function fmtPct(value, digits = 2) {{
  return `${{fmtNumber(value, digits)}}%`;
}}

function fmtInt(value) {{
  return Number(value || 0).toLocaleString('en-US', {{ maximumFractionDigits: 0 }});
}}

function coinViewKey(scenario, scope) {{
  return `${{scenario}}__${{scope}}`;
}}

function buildOverviewTable(scenario, scope) {{
  const rows = DATASET.overview[scenario][scope] || [];
  const table = document.getElementById('overviewTable');
  const html = [
    '<thead><tr>',
    '<th>Coin</th><th>Trades</th><th>Total PnL (U)</th><th>Win Rate</th><th>PF</th><th>Avg R</th><th>Long Trades</th><th>Short Trades</th><th>Long PnL</th><th>Short PnL</th>',
    '</tr></thead><tbody>'
  ];
  rows.forEach((row) => {{
    html.push(
      `<tr>
        <td>${{row.coin}}</td>
        <td>${{fmtInt(row.trades)}}</td>
        <td>${{fmtNumber(row.total_pnl_u, 2)}}</td>
        <td>${{fmtPct(row.win_rate_pct, 2)}}</td>
        <td>${{fmtNumber(row.profit_factor, 4)}}</td>
        <td>${{fmtNumber(row.avg_r, 4)}}</td>
        <td>${{fmtInt(row.long_trades)}}</td>
        <td>${{fmtInt(row.short_trades)}}</td>
        <td>${{fmtNumber(row.long_pnl_u, 2)}}</td>
        <td>${{fmtNumber(row.short_pnl_u, 2)}}</td>
      </tr>`
    );
  }});
  html.push('</tbody>');
  table.innerHTML = html.join('');
}}

const chartState = {{}};

function buildCoinCard(coin) {{
  const card = document.createElement('section');
  card.className = 'coin-card';
  card.id = `coin-${{coin.coin}}`;
  card.innerHTML = `
    <div class="coin-head">
      <div>
        <h3>${{coin.coin}}</h3>
        <div class="pill">${{coin.symbol}} | 1H candles</div>
      </div>
      <div class="note">默认显示完整 K 线，只切换交易点位和横向对比范围。</div>
    </div>
    <div class="kpi-grid" id="kpis-${{coin.coin}}"></div>
    <div class="rules">
      <div class="rule-box"><strong>做多逻辑</strong><span>${{coin.profile.longRule}}</span></div>
      <div class="rule-box"><strong>做空逻辑</strong><span>${{coin.profile.shortRule}}</span></div>
    </div>
    <div class="legend">
      <span class="long-entry">做多入场</span>
      <span class="long-exit">做多平仓</span>
      <span class="short-entry">做空入场</span>
      <span class="short-exit">做空平仓</span>
    </div>
    <div class="chart" id="chart-${{coin.coin}}"></div>
    <div class="trade-wrap">
      <table class="trade-table" id="trades-${{coin.coin}}"></table>
    </div>
  `;
  return card;
}}

function buildCandlesTrace(coin) {{
  return {{
    type: 'candlestick',
    x: coin.candles.time,
    open: coin.candles.open,
    high: coin.candles.high,
    low: coin.candles.low,
    close: coin.candles.close,
    name: `${{coin.coin}} K线`,
    increasing: {{ line: {{ color: '#16a34a' }}, fillcolor: '#16a34a' }},
    decreasing: {{ line: {{ color: '#dc2626' }}, fillcolor: '#dc2626' }},
    hoverlabel: {{ namelength: -1 }},
  }};
}}

function buildScatter(name, markers, markerStyle) {{
  return {{
    type: 'scatter',
    mode: 'markers',
    name,
    x: markers.map(item => item.x),
    y: markers.map(item => item.y),
    text: markers.map(item => item.text),
    hovertemplate: '%{{x}}<br>%{{y:.6f}}<br>%{{text}}<extra></extra>',
    marker: markerStyle,
  }};
}}

function updateCoinCard(coin, scenario, scope) {{
  const key = coinViewKey(scenario, scope);
  const view = coin.views[key];
  const metrics = view.metrics;
  const kpis = document.getElementById(`kpis-${{coin.coin}}`);
  kpis.innerHTML = [
    ['交易数', fmtInt(metrics.trades), `Long ${{fmtInt(metrics.long_trades)}} / Short ${{fmtInt(metrics.short_trades)}}`],
    ['总盈亏', `${{fmtNumber(metrics.total_pnl_u, 2)}}U`, `Long ${{fmtNumber(metrics.long_pnl_u, 2)}} / Short ${{fmtNumber(metrics.short_pnl_u, 2)}}`],
    ['胜率', fmtPct(metrics.win_rate_pct, 2), `PF ${{fmtNumber(metrics.profit_factor, 4)}}`],
    ['平均R', fmtNumber(metrics.avg_r, 4), `平均持仓 ${{fmtNumber(metrics.avg_hold_hours, 1)}}h`],
    ['当前视角', DATASET.meta.scenarioLabels[scenario], scope === 'common' ? 'Common Interval' : 'Full History'],
  ].map(item => `
    <div class="kpi">
      <div class="label">${{item[0]}}</div>
      <div class="value">${{item[1]}}</div>
      <div class="sub">${{item[2]}}</div>
    </div>
  `).join('');

  const traces = [
    buildCandlesTrace(coin),
    buildScatter('做多入场', view.markers.longEntries, {{ color:'#16a34a', symbol:'triangle-up', size:9 }}),
    buildScatter('做多平仓', view.markers.longExits, {{ color:'#22c55e', symbol:'circle', size:6 }}),
    buildScatter('做空入场', view.markers.shortEntries, {{ color:'#dc2626', symbol:'triangle-down', size:9 }}),
    buildScatter('做空平仓', view.markers.shortExits, {{ color:'#f59e0b', symbol:'circle', size:6 }}),
  ];

  const layout = {{
    margin: {{ l: 48, r: 20, t: 14, b: 34 }},
    paper_bgcolor: '#ffffff',
    plot_bgcolor: '#ffffff',
    showlegend: false,
    xaxis: {{
      rangeslider: {{ visible: false }},
      showgrid: false,
      range: scope === 'common'
        ? [DATASET.meta.commonStart, DATASET.meta.commonEnd]
        : undefined,
    }},
    yaxis: {{
      showgrid: true,
      gridcolor: '#e8eef5',
      zeroline: false,
      fixedrange: false,
    }},
    hovermode: 'closest',
  }};

  Plotly.react(`chart-${{coin.coin}}`, traces, layout, {{
    responsive: true,
    displaylogo: false,
    modeBarButtonsToRemove: ['lasso2d', 'select2d'],
  }});

  const tradeTable = document.getElementById(`trades-${{coin.coin}}`);
  const tradeRows = view.recentTrades;
  const html = [
    '<thead><tr>',
    '<th>方向</th><th>开仓时间</th><th>平仓时间</th><th>开仓价</th><th>平仓价</th><th>盈亏(U)</th><th>R</th><th>原因</th>',
    '</tr></thead><tbody>'
  ];
  tradeRows.forEach((row) => {{
    html.push(
      `<tr>
        <td>${{row.side}}</td>
        <td>${{row.entryTime}}</td>
        <td>${{row.exitTime}}</td>
        <td>${{fmtNumber(row.entryPrice, 4)}}</td>
        <td>${{fmtNumber(row.exitPrice, 4)}}</td>
        <td>${{fmtNumber(row.pnlU, 2)}}</td>
        <td>${{fmtNumber(row.r, 2)}}</td>
        <td>${{row.exitReason}}</td>
      </tr>`
    );
  }});
  if (!tradeRows.length) {{
    html.push('<tr><td colspan="8">当前视角没有交易。</td></tr>');
  }}
  html.push('</tbody>');
  tradeTable.innerHTML = html.join('');
}}

function renderAll() {{
  const scenario = document.getElementById('scenarioSelect').value;
  const scope = document.getElementById('scopeSelect').value;
  buildOverviewTable(scenario, scope);
  Object.values(DATASET.coins).forEach((coin) => updateCoinCard(coin, scenario, scope));
}}

function bootstrap() {{
  const grid = document.getElementById('coinGrid');
  Object.values(DATASET.coins).forEach((coin) => {{
    grid.appendChild(buildCoinCard(coin));
  }});
  document.getElementById('scenarioSelect').value = DATASET.meta.defaultScenario;
  document.getElementById('scopeSelect').value = DATASET.meta.defaultScope;
  document.getElementById('scenarioSelect').addEventListener('change', renderAll);
  document.getElementById('scopeSelect').addEventListener('change', renderAll);
  renderAll();
}}

bootstrap();
</script>
</body>
</html>
"""


def main() -> None:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    dataset = _build_dataset()
    HTML_PATH.write_text(_build_html(dataset), encoding="utf-8")
    print(HTML_PATH)


if __name__ == "__main__":
    main()
