from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from okx_quant.candle_cache import load_candle_cache
from okx_quant.ma55_slope_regime import (
    HORIZONS,
    RANGE_ATR_MULTIPLIER,
    SIGNAL_META,
    add_indicators,
    build_dual_confirm_events,
    build_frame,
    enrich_line,
    extract_reversal_events,
    summarize_reversal_success,
)


REPORT_DIR = ROOT / "reports"
INST_ID = "BTC-USDT-SWAP"
BAR = "1H"
HTML_PATH = REPORT_DIR / "btc_1h_ma55_slope_reversal_chart.html"
DEFAULT_VISIBLE_BARS = 1500


def main() -> None:
    REPORT_DIR.mkdir(exist_ok=True)
    candles = load_candle_cache(INST_ID, BAR, limit=None)
    if not candles:
        raise RuntimeError(f"no candles found for {INST_ID} {BAR}")

    df = build_frame(candles)
    add_indicators(df)
    ma_enriched = enrich_line(df, "sma55")
    ema_enriched = enrich_line(df, "ema55")

    ma_events = extract_reversal_events(ma_enriched, line_label="55MA")
    ema_events = extract_reversal_events(ema_enriched, line_label="55EMA")
    dual_events = build_dual_confirm_events(ma_events, ema_events)
    all_events = pd.concat([ma_events, ema_events, dual_events], ignore_index=True, sort=False)

    ma_stats = summarize_reversal_success(ma_events)
    ema_stats = summarize_reversal_success(ema_events)
    dual_stats = summarize_reversal_success(dual_events)
    combined_stats = pd.concat([ma_stats, ema_stats, dual_stats], ignore_index=True, sort=False)

    all_events.to_csv(
        REPORT_DIR / "btc_1h_ma55_slope_reversal_events.csv",
        index=False,
        encoding="utf-8-sig",
    )
    combined_stats.to_csv(
        REPORT_DIR / "btc_1h_ma55_slope_reversal_success.csv",
        index=False,
        encoding="utf-8-sig",
    )

    payload = build_chart_payload(df, ma_enriched, ema_enriched, all_events, combined_stats)
    (REPORT_DIR / "btc_1h_ma55_slope_reversal_chart.json").write_text(
        json.dumps(payload["summary"], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    HTML_PATH.write_text(build_html(df, payload), encoding="utf-8")
    print(HTML_PATH)


def build_chart_payload(
    df: pd.DataFrame,
    ma_enriched: pd.DataFrame,
    ema_enriched: pd.DataFrame,
    events: pd.DataFrame,
    stats: pd.DataFrame,
) -> dict[str, object]:
    candles = []
    ma_line = []
    ema_line = []
    for row in df.itertuples(index=False):
        unix = int(pd.Timestamp(row.timestamp).timestamp())
        candles.append(
            {
                "time": unix,
                "open": float(row.open),
                "high": float(row.high),
                "low": float(row.low),
                "close": float(row.close),
            }
        )
        if pd.notna(row.sma55):
            ma_line.append({"time": unix, "value": float(row.sma55)})
        if pd.notna(row.ema55):
            ema_line.append({"time": unix, "value": float(row.ema55)})

    markers = build_markers(events)
    stats_records = stats.to_dict("records") if not stats.empty else []
    event_records = events.copy()
    event_records["timestamp"] = event_records["timestamp"].astype(str)
    return {
        "candles": candles,
        "ma55": ma_line,
        "ema55": ema_line,
        "markers": markers,
        "stats": stats_records,
        "events_preview": event_records.tail(80).to_dict("records"),
        "summary": {
            "bar_count": len(df),
            "event_count": len(events),
            "start": str(df["timestamp"].iloc[0]),
            "end": str(df["timestamp"].iloc[-1]),
            "stats": stats_records,
        },
    }


def build_markers(events: pd.DataFrame) -> list[dict[str, object]]:
    markers: list[dict[str, object]] = []
    for row in events.itertuples(index=False):
        regime = str(row.regime)
        meta = SIGNAL_META[regime]
        line = str(row.line)
        if line == "MA+EMA":
            color = "#7c3aed"
            text = str(row.signal_label)
            shape = "square"
            position = meta["marker_position"]
        elif line == "55MA":
            color = meta["color_ma"]
            text = f"MA{meta['label']}"
            shape = meta["marker_shape"]
            position = meta["marker_position"]
        else:
            color = meta["color_ema"]
            text = f"EMA{meta['label']}"
            shape = "circle" if meta["marker_shape"].startswith("arrow") else meta["marker_shape"]
            position = meta["marker_position"]

        markers.append(
            {
                "time": int(row.unix),
                "position": position,
                "color": color,
                "shape": shape,
                "text": text,
                "line": line,
                "regime": regime,
                "signal_label": str(row.signal_label),
            }
        )
    markers.sort(key=lambda item: item["time"])
    return markers


def build_html(df: pd.DataFrame, payload: dict[str, object]) -> str:
    chart_json = json.dumps(
        {
            "candles": payload["candles"],
            "ma55": payload["ma55"],
            "ema55": payload["ema55"],
            "markers": payload["markers"],
            "defaultVisibleBars": DEFAULT_VISIBLE_BARS,
        },
        ensure_ascii=False,
    )
    stats_html = render_stats_table(payload["stats"])
    events_html = render_events_table(payload["events_preview"])
    horizon_labels = "/".join(f"{hours}H" for hours in HORIZONS)

    return f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>BTC 1H 55MA/55EMA 转势K线图</title>
<style>
:root {{
  --ink:#172033; --muted:#667085; --line:#d7dce5; --bg:#f4f7fb; --panel:#fff;
  --green:#16a34a; --red:#dc2626; --amber:#b45309; --purple:#7c3aed;
}}
* {{ box-sizing:border-box; }}
body {{ margin:0; background:var(--bg); color:var(--ink); font-family:"Microsoft YaHei","Segoe UI",Arial,sans-serif; }}
.hero {{ background:linear-gradient(135deg,#0f172a 0%,#22405f 58%,#3f6c73 100%); color:#fff; padding:28px 32px; }}
.hero h1 {{ margin:0 0 8px; font-size:28px; }}
.hero p {{ margin:5px 0; color:#d7e5f5; line-height:1.6; }}
.toolbar {{ display:flex; flex-wrap:wrap; gap:10px; padding:14px 18px; background:#fff; border-bottom:1px solid var(--line); position:sticky; top:0; z-index:20; }}
.toolbar button, .toolbar label {{ font-size:13px; }}
.toolbar button {{ border:1px solid var(--line); background:#fff; border-radius:6px; padding:8px 12px; cursor:pointer; }}
.toolbar button.active {{ background:#0f172a; color:#fff; border-color:#0f172a; }}
.legend {{ display:flex; flex-wrap:wrap; gap:14px; padding:12px 18px; background:#fff; border-bottom:1px solid var(--line); font-size:13px; }}
.legend span {{ display:inline-flex; align-items:center; gap:6px; }}
.dot {{ width:10px; height:10px; border-radius:50%; display:inline-block; }}
.wrap {{ max-width:1440px; margin:0 auto; padding:18px; }}
#chart {{ height:720px; background:#fff; border:1px solid var(--line); border-radius:8px; }}
.card {{ background:var(--panel); border:1px solid var(--line); border-radius:8px; padding:16px; margin-top:16px; }}
h2 {{ font-size:20px; margin:22px 0 12px; }}
h3 {{ font-size:16px; margin:0 0 10px; }}
table {{ width:100%; border-collapse:collapse; font-size:12px; }}
th,td {{ border-bottom:1px solid var(--line); padding:8px 9px; text-align:right; white-space:nowrap; }}
th:first-child,td:first-child {{ text-align:left; white-space:normal; }}
th {{ background:#f8fafc; color:#475467; position:sticky; top:0; }}
.note {{ color:var(--muted); line-height:1.65; font-size:13px; }}
.grid-2 {{ display:grid; grid-template-columns:1fr 1fr; gap:16px; }}
@media (max-width: 980px) {{ .grid-2 {{ grid-template-columns:1fr; }} #chart {{ height:560px; }} }}
</style>
</head>
<body>
<section class="hero">
  <h1>BTC 1小时 完整K线 + 斜率转势标注</h1>
  <p>数据：{INST_ID} {BAR}，共 {len(df)} 根K线，{df["timestamp"].iloc[0]} 至 {df["timestamp"].iloc[-1]}。</p>
  <p>图表可缩放拖动。默认显示最近 {DEFAULT_VISIBLE_BARS} 根；点击工具栏可切换全历史 / 最近窗口，并按信号类型筛选标注。</p>
</section>

<div class="toolbar">
  <button id="btn-recent" class="active" type="button">最近 {DEFAULT_VISIBLE_BARS} 根</button>
  <button id="btn-all" type="button">全历史</button>
  <button id="btn-reset" type="button">重置视图</button>
  <label><input id="filter-ma" type="checkbox" checked> 55MA 标注</label>
  <label><input id="filter-ema" type="checkbox" checked> 55EMA 标注</label>
  <label><input id="filter-dual" type="checkbox" checked> 双确认</label>
  <label><input id="filter-turn" type="checkbox" checked> 转多/转空</label>
  <label><input id="filter-fade" type="checkbox" checked> 衰竭信号</label>
</div>

<div class="legend">
  <span><i class="dot" style="background:var(--green)"></i>MA转多</span>
  <span><i class="dot" style="background:#86efac"></i>EMA转多</span>
  <span><i class="dot" style="background:var(--red)"></i>MA转空</span>
  <span><i class="dot" style="background:#fca5a5"></i>EMA转空</span>
  <span><i class="dot" style="background:var(--amber)"></i>多头衰竭</span>
  <span><i class="dot" style="background:#ea580c"></i>空头衰竭</span>
  <span><i class="dot" style="background:var(--purple)"></i>双确认</span>
</div>

<main class="wrap">
  <div id="chart"></div>

  <div class="card">
    <h2>怎么读成功率</h2>
    <p class="note">
      <strong>方向成功率</strong>：信号出现后，到 {horizon_labels} 收盘，价格是否朝信号方向走。
      转多/空头衰竭看上涨；转空/多头衰竭看下跌。<br>
      <strong>区间成功率</strong>：未来窗口内，不利方向偏移是否 &lt;= {RANGE_ATR_MULTIPLIER} ATR。
      这项更接近卖期权：标的不大幅突破，时间价值更容易留下。<br>
      <strong>强趋势成功率</strong>：有利方向是否走出 &gt;= 0.5 ATR。适合评估方向单，不一定适合卖期权。
    </p>
  </div>

  <div class="card">
    <h2>转势信号成功率统计</h2>
    {stats_html}
  </div>

  <div class="grid-2">
    <div class="card">
      <h3>卖期权视角提示</h3>
      <p class="note">
        若你准备做<strong>卖期权</strong>，优先看 <strong>区间成功率</strong>，不要只看方向成功率。
        例如转空后方向成功率一般，但 48H 区间成功率若较高，说明标的不容易急拉，更适合卖 Call 或 Call spread。
        转多后若区间成功率高，则更适合卖 Put 或 Put spread。
      </p>
    </div>
    <div class="card">
      <h3>观察建议</h3>
      <p class="note">
        先重点看 <strong>双确认转多/转空</strong> 与 <strong>MA55 转势</strong>。
        EMA55 信号更密、更灵敏，适合当预警；MA55 和双确认更适合做期权策略过滤。
      </p>
    </div>
  </div>

  <div class="card">
    <h2>最近 80 个转势事件</h2>
    {events_html}
  </div>
</main>

<script src="https://unpkg.com/lightweight-charts@4.2.1/dist/lightweight-charts.standalone.production.js"></script>
<script>
const chartData = {chart_json};

const chartEl = document.getElementById('chart');
const chart = LightweightCharts.createChart(chartEl, {{
  layout: {{ background: {{ color: '#ffffff' }}, textColor: '#334155' }},
  rightPriceScale: {{ borderVisible: false }},
  timeScale: {{ borderVisible: false, timeVisible: true, secondsVisible: false }},
  grid: {{
    vertLines: {{ color: '#eef2f7' }},
    horzLines: {{ color: '#eef2f7' }},
  }},
  crosshair: {{ mode: LightweightCharts.CrosshairMode.Normal }},
}});
const candleSeries = chart.addCandlestickSeries({{
  upColor: '#16a34a',
  downColor: '#dc2626',
  borderUpColor: '#16a34a',
  borderDownColor: '#dc2626',
  wickUpColor: '#16a34a',
  wickDownColor: '#dc2626',
}});
const maSeries = chart.addLineSeries({{ color: '#1d4ed8', lineWidth: 2, title: 'MA55' }});
const emaSeries = chart.addLineSeries({{ color: '#f59e0b', lineWidth: 2, title: 'EMA55' }});

candleSeries.setData(chartData.candles);
maSeries.setData(chartData.ma55);
emaSeries.setData(chartData.ema55);

function filteredMarkers() {{
  const showMa = document.getElementById('filter-ma').checked;
  const showEma = document.getElementById('filter-ema').checked;
  const showDual = document.getElementById('filter-dual').checked;
  const showTurn = document.getElementById('filter-turn').checked;
  const showFade = document.getElementById('filter-fade').checked;
  return chartData.markers.filter((marker) => {{
    if (marker.line === '55MA' && !showMa) return false;
    if (marker.line === '55EMA' && !showEma) return false;
    if (marker.line === 'MA+EMA' && !showDual) return false;
    if ((marker.regime === 'bull_start' || marker.regime === 'bear_start') && !showTurn) return false;
    if ((marker.regime === 'bull_fade' || marker.regime === 'bear_fade') && !showFade) return false;
    return true;
  }});
}}

function applyMarkers() {{
  candleSeries.setMarkers(filteredMarkers());
}}

function showRecent() {{
  const bars = chartData.defaultVisibleBars;
  const last = chartData.candles[chartData.candles.length - 1];
  const firstIndex = Math.max(0, chartData.candles.length - bars);
  const first = chartData.candles[firstIndex];
  chart.timeScale().setVisibleRange({{ from: first.time, to: last.time }});
}}

function showAll() {{
  chart.timeScale().fitContent();
}}

applyMarkers();
showRecent();

document.getElementById('btn-recent').addEventListener('click', () => {{
  document.getElementById('btn-recent').classList.add('active');
  document.getElementById('btn-all').classList.remove('active');
  showRecent();
}});
document.getElementById('btn-all').addEventListener('click', () => {{
  document.getElementById('btn-all').classList.add('active');
  document.getElementById('btn-recent').classList.remove('active');
  showAll();
}});
document.getElementById('btn-reset').addEventListener('click', () => showRecent());
['filter-ma','filter-ema','filter-dual','filter-turn','filter-fade'].forEach((id) => {{
  document.getElementById(id).addEventListener('change', applyMarkers);
}});
window.addEventListener('resize', () => chart.applyOptions({{ width: chartEl.clientWidth }}));
chart.applyOptions({{ width: chartEl.clientWidth }});
</script>
</body>
</html>"""


def render_stats_table(stats: list[dict[str, object]]) -> str:
    if not stats:
        return "<p class='note'>暂无统计数据</p>"
    columns = ["line", "signal_label", "count"]
    for hours in HORIZONS:
        columns.extend(
            [
                f"dir_ok_{hours}h",
                f"range_ok_{hours}h",
                f"strong_ok_{hours}h",
                f"mean_return_{hours}h",
            ]
        )
    header_map = {
        "line": "线",
        "signal_label": "信号",
        "count": "次数",
    }
    for hours in HORIZONS:
        header_map[f"dir_ok_{hours}h"] = f"{hours}H方向"
        header_map[f"range_ok_{hours}h"] = f"{hours}H区间"
        header_map[f"strong_ok_{hours}h"] = f"{hours}H强趋势"
        header_map[f"mean_return_{hours}h"] = f"{hours}H均收益"

    parts = ["<div style='overflow:auto'><table><tr>"]
    for col in columns:
        parts.append(f"<th>{header_map.get(col, col)}</th>")
    parts.append("</tr>")
    for row in stats:
        parts.append("<tr>")
        for col in columns:
            value = row.get(col)
            if isinstance(value, float):
                if col.startswith("mean_return"):
                    text = f"{value * 100:.2f}%"
                else:
                    text = f"{value * 100:.1f}%"
            else:
                text = str(value)
            parts.append(f"<td>{text}</td>")
        parts.append("</tr>")
    parts.append("</table></div>")
    return "".join(parts)


def render_events_table(events: list[dict[str, object]]) -> str:
    if not events:
        return "<p class='note'>暂无事件</p>"
    columns = ["timestamp", "line", "signal_label", "close", "slope_ratio", "dir_ok_24h", "range_ok_48h", "return_24h"]
    header_map = {
        "timestamp": "时间(UTC)",
        "line": "线",
        "signal_label": "信号",
        "close": "收盘",
        "slope_ratio": "slope_ratio",
        "dir_ok_24h": "24H方向",
        "range_ok_48h": "48H区间",
        "return_24h": "24H收益",
    }
    parts = ["<div style='overflow:auto'><table><tr>"]
    for col in columns:
        parts.append(f"<th>{header_map.get(col, col)}</th>")
    parts.append("</tr>")
    for row in events:
        parts.append("<tr>")
        for col in columns:
            value = row.get(col)
            if col == "dir_ok_24h" or col == "range_ok_48h":
                text = "是" if value else "否"
            elif col == "return_24h" and isinstance(value, (float, int)):
                text = f"{float(value) * 100:.2f}%"
            elif col == "slope_ratio" and isinstance(value, (float, int)):
                text = f"{float(value):.6f}"
            elif col == "close" and isinstance(value, (float, int)):
                text = f"{float(value):.2f}"
            else:
                text = str(value)
            parts.append(f"<td>{text}</td>")
        parts.append("</tr>")
    parts.append("</table></div>")
    return "".join(parts)


if __name__ == "__main__":
    main()
