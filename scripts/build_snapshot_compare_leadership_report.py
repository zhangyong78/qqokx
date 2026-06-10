from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd


WORKSPACE_ROOT = Path(__file__).resolve().parent.parent
DATA_ROOT = Path("D:/qqokx_data")
BACKTEST_HISTORY_PATH = DATA_ROOT / "state" / "backtest_history.json"
ECHARTS_JS_PATH = WORKSPACE_ROOT / "third_party" / "echarts.min.js"
OUTPUT_PATH = WORKSPACE_ROOT / "reports" / "S057_S058_leadership_compare.html"
SNAPSHOT_IDS = ("S057", "S058")
INITIAL_CAPITAL = 10_000.0


@dataclass
class SnapshotBundle:
    snapshot_id: str
    payload: dict[str, Any]
    capital: pd.DataFrame
    operations: pd.DataFrame
    audit: dict[str, Any]
    monthly_u: pd.DataFrame
    yearly_u: pd.DataFrame
    exit_reasons: pd.DataFrame


def load_history() -> dict[str, dict[str, Any]]:
    payload = json.loads(BACKTEST_HISTORY_PATH.read_text(encoding="utf-8"))
    mapping: dict[str, dict[str, Any]] = {}
    for item in payload.get("records", []):
        snapshot_id = str(item.get("snapshot_id", "")).strip()
        if snapshot_id:
            mapping[snapshot_id] = item
    return mapping


def build_period_u_frame(capital: pd.DataFrame, freq: str, label_fmt: str, label_col: str) -> pd.DataFrame:
    series = (
        capital.set_index("datetime")["marked_equity_liquidation_basis"]
        .resample(freq)
        .last()
        .dropna()
        .to_frame("ending_equity")
    )
    series["begin_equity"] = series["ending_equity"].shift(1).fillna(INITIAL_CAPITAL)
    series["change_u"] = series["ending_equity"] - series["begin_equity"]
    series[label_col] = series.index.strftime(label_fmt)
    return series.reset_index(drop=True)


def load_snapshot_bundle(snapshot_id: str, history: dict[str, dict[str, Any]]) -> SnapshotBundle:
    payload = history[snapshot_id]
    export_base = Path(str(payload["export_path"]))

    capital = pd.read_csv(export_base.with_suffix(".capital.csv"))
    capital["datetime"] = pd.to_datetime(capital["datetime"])
    for col in ("marked_equity_liquidation_basis", "marked_drawdown_liquidation_pct"):
        capital[col] = pd.to_numeric(capital[col], errors="coerce")

    operations = pd.read_csv(export_base.with_suffix(".operations.csv"))
    operations["datetime"] = pd.to_datetime(operations["datetime"])
    operations["pnl"] = pd.to_numeric(operations["pnl"], errors="coerce").fillna(0.0)
    exit_rows = operations[operations["action"] == "exit"].copy()
    exit_reasons = (
        exit_rows.groupby("reason", as_index=False)
        .agg(trades=("position_id", "count"), pnl=("pnl", "sum"))
        .sort_values("trades", ascending=False)
    )

    audit = json.loads(export_base.with_suffix(".audit.json").read_text(encoding="utf-8"))

    return SnapshotBundle(
        snapshot_id=snapshot_id,
        payload=payload,
        capital=capital,
        operations=operations,
        audit=audit,
        monthly_u=build_period_u_frame(capital, "ME", "%Y-%m", "period"),
        yearly_u=build_period_u_frame(capital, "YE", "%Y", "period"),
        exit_reasons=exit_reasons,
    )


def fmt_num(value: float, digits: int = 2) -> str:
    return f"{value:,.{digits}f}"


def summarize_combined(s057: SnapshotBundle, s058: SnapshotBundle) -> tuple[pd.DataFrame, pd.DataFrame]:
    monthly = pd.merge(
        s057.monthly_u[["period", "change_u", "ending_equity"]],
        s058.monthly_u[["period", "change_u", "ending_equity"]],
        on="period",
        how="outer",
        suffixes=("_s057", "_s058"),
    ).fillna(0.0)
    monthly = monthly.sort_values("period")
    monthly["combined_change_u"] = monthly["change_u_s057"] + monthly["change_u_s058"]
    monthly["combined_ending_equity"] = INITIAL_CAPITAL * 2 + monthly["combined_change_u"].cumsum()

    yearly = pd.merge(
        s057.yearly_u[["period", "change_u"]],
        s058.yearly_u[["period", "change_u"]],
        on="period",
        how="outer",
        suffixes=("_s057", "_s058"),
    ).fillna(0.0)
    yearly = yearly.sort_values("period")
    yearly["combined_change_u"] = yearly["change_u_s057"] + yearly["change_u_s058"]
    yearly["combined_ending_equity"] = INITIAL_CAPITAL * 2 + yearly["combined_change_u"].cumsum()
    return monthly, yearly


def build_summary_cards(s057: SnapshotBundle, s058: SnapshotBundle, combined_monthly: pd.DataFrame) -> str:
    r1 = s057.payload["report"]
    r2 = s058.payload["report"]
    worst_057 = float(s057.monthly_u["change_u"].min())
    worst_058 = float(s058.monthly_u["change_u"].min())
    worst_combined_row = combined_monthly.loc[combined_monthly["combined_change_u"].idxmin()]
    cards = [
        ("S057 期末净值", f"{fmt_num(float(r1['ending_equity']))}U"),
        ("S058 期末净值", f"{fmt_num(float(r2['ending_equity']))}U"),
        ("S057 单月最大亏损", f"{fmt_num(worst_057)}U"),
        ("S058 单月最大亏损", f"{fmt_num(worst_058)}U"),
        ("多空互补后单月最大亏损", f"{fmt_num(float(worst_combined_row['combined_change_u']))}U"),
        ("多空最差月份", str(worst_combined_row["period"])),
        ("多空互补后期末净值", f"{fmt_num(float(combined_monthly['combined_ending_equity'].iloc[-1]))}U"),
        ("多空互补后月度为负次数", str(int((combined_monthly["combined_change_u"] < 0).sum()))),
    ]
    return "\n".join(
        f'<div class="card"><div class="label">{title}</div><div class="value">{value}</div></div>'
        for title, value in cards
    )


def build_parameter_rows(s057: SnapshotBundle, s058: SnapshotBundle) -> str:
    p1 = s057.payload["config"]
    p2 = s058.payload["config"]
    keys = [
        ("策略ID", "strategy_id"),
        ("方向", "signal_mode"),
        ("交易对", "inst_id"),
        ("周期", "bar"),
        ("主均线", "ema_period"),
        ("趋势均线", "trend_ema_period"),
        ("挂单参考均线", "entry_reference_ema_period"),
        ("ATR周期", "atr_period"),
        ("止损倍数", "atr_stop_multiplier"),
        ("止盈倍数", "atr_take_multiplier"),
        ("每趋势最大开仓", "max_entries_per_trend"),
        ("斜率阈值", "trend_ema_slope_filter_min_ratio"),
        ("初始资金", "backtest_initial_capital"),
        ("风险金", "risk_amount"),
    ]
    return "\n".join(
        f"<tr><td>{label}</td><td>{p1.get(key, '')}</td><td>{p2.get(key, '')}</td></tr>"
        for label, key in keys
    )


def build_monthly_rows(s057: SnapshotBundle, s058: SnapshotBundle, combined_monthly: pd.DataFrame) -> str:
    rows = []
    for _, row in combined_monthly.iterrows():
        rows.append(
            "<tr>"
            f"<td>{row['period']}</td>"
            f"<td>{fmt_num(float(row['change_u_s057']))}U</td>"
            f"<td>{fmt_num(float(row['ending_equity_s057']))}U</td>"
            f"<td>{fmt_num(float(row['change_u_s058']))}U</td>"
            f"<td>{fmt_num(float(row['ending_equity_s058']))}U</td>"
            f"<td><b>{fmt_num(float(row['combined_change_u']))}U</b></td>"
            f"<td>{fmt_num(float(row['combined_ending_equity']))}U</td>"
            "</tr>"
        )
    return "\n".join(rows)


def build_yearly_rows(s057: SnapshotBundle, s058: SnapshotBundle, combined_yearly: pd.DataFrame) -> str:
    merged = pd.merge(
        s057.yearly_u,
        s058.yearly_u,
        on="period",
        how="outer",
        suffixes=("_s057", "_s058"),
    ).fillna(0.0)
    merged = merged.sort_values("period")
    merged = pd.merge(
        merged,
        combined_yearly[["period", "combined_change_u", "combined_ending_equity"]],
        on="period",
        how="left",
    )
    rows = []
    for _, row in merged.iterrows():
        rows.append(
            "<tr>"
            f"<td>{row['period']}</td>"
            f"<td>{fmt_num(float(row['change_u_s057']))}U</td>"
            f"<td>{fmt_num(float(row['ending_equity_s057']))}U</td>"
            f"<td>{fmt_num(float(row['change_u_s058']))}U</td>"
            f"<td>{fmt_num(float(row['ending_equity_s058']))}U</td>"
            f"<td><b>{fmt_num(float(row['combined_change_u']))}U</b></td>"
            f"<td>{fmt_num(float(row['combined_ending_equity']))}U</td>"
            "</tr>"
        )
    return "\n".join(rows)


def build_reason_rows(s057: SnapshotBundle, s058: SnapshotBundle) -> str:
    merged = pd.merge(
        s057.exit_reasons,
        s058.exit_reasons,
        on="reason",
        how="outer",
        suffixes=("_s057", "_s058"),
    ).fillna(0.0)
    merged = merged.sort_values(["trades_s057", "trades_s058"], ascending=False)
    rows = []
    for _, row in merged.iterrows():
        rows.append(
            "<tr>"
            f"<td>{row['reason']}</td>"
            f"<td>{int(row['trades_s057'])}</td>"
            f"<td>{fmt_num(float(row['pnl_s057']))}U</td>"
            f"<td>{int(row['trades_s058'])}</td>"
            f"<td>{fmt_num(float(row['pnl_s058']))}U</td>"
            "</tr>"
        )
    return "\n".join(rows)


def make_chart_payload(s057: SnapshotBundle, s058: SnapshotBundle, combined_monthly: pd.DataFrame, combined_yearly: pd.DataFrame) -> dict[str, Any]:
    return {
        "monthly": {
            "labels": combined_monthly["period"].tolist(),
            "s057": [round(float(v), 2) for v in combined_monthly["change_u_s057"]],
            "s058": [round(float(v), 2) for v in combined_monthly["change_u_s058"]],
            "combined": [round(float(v), 2) for v in combined_monthly["combined_change_u"]],
        },
        "yearly": {
            "labels": combined_yearly["period"].tolist(),
            "s057": [round(float(v), 2) for v in combined_yearly["change_u_s057"]],
            "s058": [round(float(v), 2) for v in combined_yearly["change_u_s058"]],
            "combined": [round(float(v), 2) for v in combined_yearly["combined_change_u"]],
        },
    }


def build_html(s057: SnapshotBundle, s058: SnapshotBundle, combined_monthly: pd.DataFrame, combined_yearly: pd.DataFrame) -> str:
    charts = json.dumps(
        make_chart_payload(s057, s058, combined_monthly, combined_yearly),
        ensure_ascii=False,
    )
    echarts_js = ECHARTS_JS_PATH.read_text(encoding="utf-8")
    worst_combined = combined_monthly.loc[combined_monthly["combined_change_u"].idxmin()]

    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <title>S057 S058 资金变化对比</title>
  <style>
    :root {{
      --bg: #f6f3ed;
      --card: #fffdfa;
      --ink: #1d2433;
      --muted: #6b7280;
      --line: #ddd4c6;
      --teal: #0d5c63;
      --orange: #c86b3c;
      --gold: #d4a017;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Segoe UI", "PingFang SC", "Microsoft YaHei", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at left top, rgba(13, 92, 99, 0.10), transparent 25%),
        radial-gradient(circle at right top, rgba(200, 107, 60, 0.12), transparent 24%),
        var(--bg);
    }}
    .wrap {{ max-width: 1520px; margin: 0 auto; padding: 28px; }}
    .hero, .section {{
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 22px;
      box-shadow: 0 12px 32px rgba(34, 41, 47, 0.06);
    }}
    .hero {{ padding: 30px 32px; }}
    .section {{ margin-top: 20px; padding: 24px; }}
    h1 {{ margin: 0 0 10px; font-size: 38px; }}
    h2 {{ margin: 0 0 12px; font-size: 24px; }}
    p {{ margin: 0; line-height: 1.7; }}
    .muted {{ color: var(--muted); }}
    .pill {{
      display: inline-block;
      margin-top: 12px;
      margin-right: 8px;
      padding: 6px 12px;
      border-radius: 999px;
      background: rgba(13, 92, 99, 0.10);
      color: var(--teal);
      font-size: 13px;
    }}
    .cards {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 14px;
      margin-top: 18px;
    }}
    .card {{
      border: 1px solid #ece4d8;
      border-radius: 18px;
      padding: 16px;
      background: linear-gradient(180deg, #fffdfa, #f3ece0);
    }}
    .label {{ font-size: 13px; color: var(--muted); margin-bottom: 8px; }}
    .value {{ font-size: 24px; font-weight: 800; }}
    .grid-2 {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }}
    .insight {{
      background: #f3ebe0;
      border-left: 5px solid var(--orange);
      border-radius: 16px;
      padding: 18px;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 14px;
    }}
    th, td {{
      padding: 10px 12px;
      border-bottom: 1px solid #ece4d8;
      text-align: left;
      vertical-align: top;
    }}
    th {{
      background: rgba(13, 92, 99, 0.08);
      font-size: 13px;
    }}
    .table-wrap {{
      max-height: 460px;
      overflow: auto;
      border: 1px solid #ece4d8;
      border-radius: 16px;
    }}
    .chart {{ height: 380px; width: 100%; }}
    .small-chart {{ height: 320px; }}
    .footer {{ margin-top: 18px; font-size: 13px; color: var(--muted); }}
    @media (max-width: 1100px) {{
      .grid-2 {{ grid-template-columns: 1fr; }}
      h1 {{ font-size: 30px; }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="hero">
      <h1>S057 / S058 月度年度资金变化</h1>
      <p class="muted">口径统一：单策略固定风险金 100U，金额全部按 U 看，不看百分比。月度与年度变化基于月末/年末盯市净值变化；多空互补口径为 S057 与 S058 每月资金变化直接相加。</p>
      <div>
        <span class="pill">S057：做多动态挂单</span>
        <span class="pill">S058：EMA55 斜率做空</span>
        <span class="pill">多空最差月份：{worst_combined['period']}</span>
      </div>
      <div class="cards">
        {build_summary_cards(s057, s058, combined_monthly)}
      </div>
    </div>

    <div class="section">
      <div class="grid-2">
        <div>
          <h2>你最关心的结论</h2>
          <div class="insight">
            <p>如果只看每月按 U 的资金变化，S057 单月最差为 <b>{fmt_num(float(s057.monthly_u['change_u'].min()))}U</b>，S058 单月最差为 <b>{fmt_num(float(s058.monthly_u['change_u'].min()))}U</b>。</p>
            <p style="margin-top:10px;">多空互补后，历史上最差单月出现在 <b>{worst_combined['period']}</b>，当月合并亏损 <b>{fmt_num(float(worst_combined['combined_change_u']))}U</b>。</p>
            <p style="margin-top:10px;">这意味着：两边并不是每个月都互相完全对冲，但合并后仍然可以直接看到每个月实际亏多少、赚多少。</p>
          </div>
        </div>
        <div>
          <h2>核心参数</h2>
          <div class="table-wrap">
            <table>
              <thead><tr><th>项目</th><th>S057</th><th>S058</th></tr></thead>
              <tbody>{build_parameter_rows(s057, s058)}</tbody>
            </table>
          </div>
        </div>
      </div>
    </div>

    <div class="section">
      <h2>月度资金变化对比（U）</h2>
      <div id="monthlyChart" class="chart"></div>
    </div>

    <div class="section">
      <h2>年度资金变化对比（U）</h2>
      <div id="yearlyChart" class="chart small-chart"></div>
    </div>

    <div class="section">
      <h2>月度明细表</h2>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>月份</th>
              <th>S057 当月变化</th>
              <th>S057 月末净值</th>
              <th>S058 当月变化</th>
              <th>S058 月末净值</th>
              <th>多空合计当月变化</th>
              <th>多空合计月末净值</th>
            </tr>
          </thead>
          <tbody>{build_monthly_rows(s057, s058, combined_monthly)}</tbody>
        </table>
      </div>
    </div>

    <div class="section">
      <h2>年度明细表</h2>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>年份</th>
              <th>S057 年度变化</th>
              <th>S057 年末净值</th>
              <th>S058 年度变化</th>
              <th>S058 年末净值</th>
              <th>多空合计年度变化</th>
              <th>多空合计年末净值</th>
            </tr>
          </thead>
          <tbody>{build_yearly_rows(s057, s058, combined_yearly)}</tbody>
        </table>
      </div>
    </div>

    <div class="section">
      <h2>平仓原因参考</h2>
      <div class="table-wrap">
        <table>
          <thead><tr><th>原因</th><th>S057 笔数</th><th>S057 净利润</th><th>S058 笔数</th><th>S058 净利润</th></tr></thead>
          <tbody>{build_reason_rows(s057, s058)}</tbody>
        </table>
      </div>
    </div>

    <div class="footer">
      数据源：{BACKTEST_HISTORY_PATH}<br>
      S057 导出：{s057.payload['export_path']}<br>
      S058 导出：{s058.payload['export_path']}
    </div>
  </div>

  <script>{echarts_js}</script>
  <script>
    const payload = {charts};
    const color057 = '#0d5c63';
    const color058 = '#c86b3c';
    const colorCombined = '#d4a017';

    function renderBar(el, title, labels, s057, s058, combined) {{
      const chart = echarts.init(document.getElementById(el));
      chart.setOption({{
        title: {{ text: title, left: 10, top: 6, textStyle: {{ fontSize: 16 }} }},
        tooltip: {{ trigger: 'axis' }},
        legend: {{ top: 8, right: 10, data: ['S057', 'S058', '多空合计'] }},
        grid: {{ left: 58, right: 24, top: 54, bottom: el === 'monthlyChart' ? 90 : 48 }},
        xAxis: {{
          type: 'category',
          data: labels,
          axisLabel: el === 'monthlyChart' ? {{ rotate: 60, fontSize: 11 }} : {{}}
        }},
        yAxis: {{
          type: 'value',
          axisLabel: {{ formatter: value => `${{value}}U` }}
        }},
        series: [
          {{ name: 'S057', type: 'bar', itemStyle: {{ color: color057 }}, data: s057 }},
          {{ name: 'S058', type: 'bar', itemStyle: {{ color: color058 }}, data: s058 }},
          {{ name: '多空合计', type: 'line', smooth: true, showSymbol: false, lineStyle: {{ width: 3, color: colorCombined }}, data: combined }}
        ]
      }});
      window.addEventListener('resize', () => chart.resize());
    }}

    renderBar('monthlyChart', '每个月资金变化（U）', payload.monthly.labels, payload.monthly.s057, payload.monthly.s058, payload.monthly.combined);
    renderBar('yearlyChart', '每年资金变化（U）', payload.yearly.labels, payload.yearly.s057, payload.yearly.s058, payload.yearly.combined);
  </script>
</body>
</html>
"""


def main() -> None:
    history = load_history()
    missing = [snapshot_id for snapshot_id in SNAPSHOT_IDS if snapshot_id not in history]
    if missing:
        raise SystemExit(f"missing snapshot ids: {', '.join(missing)}")

    s057 = load_snapshot_bundle("S057", history)
    s058 = load_snapshot_bundle("S058", history)
    combined_monthly, combined_yearly = summarize_combined(s057, s058)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(build_html(s057, s058, combined_monthly, combined_yearly), encoding="utf-8")
    print(OUTPUT_PATH)


if __name__ == "__main__":
    main()
