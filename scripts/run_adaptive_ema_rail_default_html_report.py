from __future__ import annotations

import html
import sys
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from okx_quant.backtest import (
    _run_backtest_with_loaded_data,
    format_backtest_report,
    summarize_trade_exit_reasons,
)
from okx_quant.candle_cache import load_candle_cache
from okx_quant.models import StrategyConfig
from okx_quant.okx_client import OkxRestClient
from okx_quant.persistence import analysis_report_dir_path
from okx_quant.pricing import format_decimal_fixed
from okx_quant.strategy_catalog import STRATEGY_ADAPTIVE_EMA_RAIL_LONG_ID


SYMBOL = "BTC-USDT-SWAP"
BAR = "4H"
INITIAL_CAPITAL = Decimal("10000")
RISK_AMOUNT = Decimal("10")
MAKER_FEE = Decimal("0.00015")
TAKER_FEE = Decimal("0.00036")


def _fmt_number(value: Decimal | None, digits: int = 2) -> str:
    if value is None:
        return "-"
    return format_decimal_fixed(value, digits)


def _fmt_money(value: Decimal | None, digits: int = 2) -> str:
    if value is None:
        return "-"
    return f"{float(value):,.{digits}f}"


def _fmt_pct(value: Decimal | None, digits: int = 2) -> str:
    if value is None:
        return "-"
    return f"{_fmt_number(value, digits)}%"


def _fmt_ts(ts: int) -> str:
    if ts >= 10**12:
        return datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    if ts >= 10**9:
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return str(ts)


def _baseline_config() -> StrategyConfig:
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


def _sparkline_svg(values: list[Decimal], *, color: str, fill: str, height: int = 280) -> str:
    width = 980
    pad_left = 28
    pad_right = 18
    pad_top = 18
    pad_bottom = 24
    plot_width = width - pad_left - pad_right
    plot_height = height - pad_top - pad_bottom
    numeric = [float(item) for item in values]
    if not numeric:
        return '<div class="empty-chart">No data</div>'

    min_v = min(numeric)
    max_v = max(numeric)
    span = max_v - min_v
    if span == 0:
        span = max(abs(max_v), 1.0)
        min_v -= span * 0.5
        max_v += span * 0.5
        span = max_v - min_v

    def project(index: int, value: float) -> tuple[float, float]:
        x = pad_left + (plot_width * index / max(len(numeric) - 1, 1))
        y = pad_top + (max_v - value) / span * plot_height
        return (x, y)

    points = [project(index, value) for index, value in enumerate(numeric)]
    line_points = " ".join(f"{x:.2f},{y:.2f}" for x, y in points)
    area_points = " ".join(
        [f"{pad_left:.2f},{pad_top + plot_height:.2f}"]
        + [f"{x:.2f},{y:.2f}" for x, y in points]
        + [f"{pad_left + plot_width:.2f},{pad_top + plot_height:.2f}"]
    )
    grid_lines = []
    for step in range(5):
        y = pad_top + plot_height * step / 4
        grid_lines.append(
            f'<line x1="{pad_left}" y1="{y:.2f}" x2="{pad_left + plot_width}" y2="{y:.2f}" class="grid-line" />'
        )

    return f"""
    <svg viewBox="0 0 {width} {height}" role="img" aria-label="chart">
      <rect x="0" y="0" width="{width}" height="{height}" rx="12" fill="transparent"></rect>
      {''.join(grid_lines)}
      <polyline points="{area_points}" fill="{fill}" stroke="none"></polyline>
      <polyline points="{line_points}" fill="none" stroke="{color}" stroke-width="3.2" stroke-linecap="round" stroke-linejoin="round"></polyline>
    </svg>
    """


def _kpi(label: str, value: str, sub: str) -> str:
    return (
        '<div class="card kpi">'
        f'<div class="label">{html.escape(label)}</div>'
        f'<div class="value">{html.escape(value)}</div>'
        f'<div class="sub">{html.escape(sub)}</div>'
        "</div>"
    )


def _table(headers: list[str], rows: list[list[str]]) -> str:
    head = "".join(f"<th>{html.escape(header)}</th>" for header in headers)
    body_rows = []
    for row in rows:
        body_rows.append("<tr>" + "".join(f"<td>{cell}</td>" for cell in row) + "</tr>")
    return f"<table><thead><tr>{head}</tr></thead><tbody>{''.join(body_rows)}</tbody></table>"


def _render_period_table(periods: list, label: str) -> str:
    rows: list[list[str]] = []
    for item in periods:
        rows.append(
            [
                html.escape(item.period_label),
                str(item.trades),
                _fmt_pct(item.win_rate, 2),
                _fmt_money(item.total_pnl, 2),
                _fmt_pct(item.return_pct, 2),
                _fmt_money(item.start_equity, 2),
                _fmt_money(item.end_equity, 2),
                _fmt_pct(item.max_drawdown_pct, 2),
            ]
        )
    return _table(
        [label, "交易数", "胜率", "总盈亏", "收益率", "期初资金", "期末资金", "最大回撤"],
        rows,
    )


def _render_distribution(counter: Counter[int]) -> str:
    if not counter:
        return '<div class="note">当前没有成交分布可展示。</div>'
    total = sum(counter.values())
    rows = []
    for period, count in sorted(counter.items()):
        share = (Decimal(count) / Decimal(total)) * Decimal("100")
        rows.append(
            """
            <div class="bar-row">
              <div class="bar-label">EMA%s</div>
              <div class="bar-track"><span style="width:%s%%"></span></div>
              <div class="bar-value">%s 笔 / %s</div>
            </div>
            """
            % (period, format_decimal_fixed(share, 2), count, _fmt_pct(share, 2))
        )
    return "".join(rows)


def _render_rail_stats(result) -> str:
    stats = result.adaptive_rail_stats
    if stats is None:
        return '<div class="note">当前结果没有自适应轨道统计。</div>'

    dominant_rows = [
        [
            f"EMA{item.period}",
            str(item.bars),
            _fmt_pct(item.share_pct, 2),
        ]
        for item in stats.dominant_period_frequencies
    ]

    return (
        '<div class="grid grid-4">'
        + _kpi("确认覆盖率", _fmt_pct(stats.confirmed_coverage_pct, 2), "确认态占评估 bars")
        + _kpi("破坏态占比", _fmt_pct(stats.broken_state_pct, 2), "broken bars / evaluation bars")
        + _kpi("轨道切换次数", str(stats.dominant_rail_switches), "主导轨道切换总数")
        + _kpi(
            "平均持有 bars",
            _fmt_number(stats.average_dominant_rail_hold_bars, 2),
            f"最长 {stats.max_dominant_rail_hold_bars} bars",
        )
        + "</div>"
        + '<div class="card inset-card">'
        + _table(["主导轨道", "bars", "占比"], dominant_rows)
        + "</div>"
    )


def _render_trade_table(result) -> str:
    trades = result.trades[-12:]
    if not trades:
        return '<div class="note">当前没有交易明细。</div>'
    rows = []
    for trade in reversed(trades):
        rows.append(
            [
                html.escape(_fmt_ts(trade.entry_ts)),
                html.escape(_fmt_ts(trade.exit_ts)),
                html.escape(f"EMA{trade.adaptive_rail_period}" if trade.adaptive_rail_period else "-"),
                _fmt_money(trade.entry_price, 2),
                _fmt_money(trade.exit_price, 2),
                _fmt_money(trade.pnl, 2),
                _fmt_number(trade.r_multiple, 2),
                _fmt_money(trade.total_fee, 4),
                html.escape(trade.exit_reason),
            ]
        )
    return _table(
        ["开仓时间", "平仓时间", "入场轨道", "开仓价", "平仓价", "净盈亏", "R", "手续费", "平仓原因"],
        rows,
    )


def _render_config(config: StrategyConfig) -> str:
    lines = [
        f"symbol = {config.inst_id}",
        f"bar = {config.bar}",
        f"strategy_id = {config.strategy_id}",
        f"candidate_periods = {list(config.rail_candidate_ema_periods)}",
        f"atr_stop_multiplier = {config.atr_stop_multiplier}",
        f"atr_take_multiplier = {config.atr_take_multiplier}",
        f"rail_break_atr_ratio = {config.rail_break_atr_ratio}",
        f"rail_reclaim_bars = {config.rail_reclaim_bars}",
        f"rail_switch_min_score_delta = {config.rail_switch_min_score_delta}",
        f"rail_fast_gate_enabled = {config.rail_fast_gate_enabled}",
        f"rail_fast_gate_period = {config.rail_fast_gate_period}",
        f"rail_fast_min_gap_ema200_atr = {config.rail_fast_min_gap_ema200_atr}",
        f"rail_fast_min_spread_trend_atr = {config.rail_fast_min_spread_trend_atr}",
        f"rail_fast_max_recent_range_atr = {config.rail_fast_max_recent_range_atr}",
        f"rail_fast_recent_range_bars = {config.rail_fast_recent_range_bars}",
        f"take_profit_mode = {config.take_profit_mode}",
        f"dynamic_two_r_break_even = {config.dynamic_two_r_break_even}",
        f"dynamic_fee_offset_enabled = {config.dynamic_fee_offset_enabled}",
    ]
    return "<pre>" + html.escape("\n".join(lines)) + "</pre>"


def _build_html(result, config: StrategyConfig, report_path: Path) -> str:
    report = result.report
    candles = result.candles
    start_label = _fmt_ts(candles[0].ts) if candles else "-"
    end_label = _fmt_ts(candles[-1].ts) if candles else "-"
    exit_reason_rows = summarize_trade_exit_reasons(result.trades)
    exit_rows = [[html.escape(label), str(count)] for label, count in exit_reason_rows]
    rail_trade_counter = Counter(
        trade.adaptive_rail_period for trade in result.trades if trade.adaptive_rail_period is not None
    )
    backtest_text = format_backtest_report(result)
    equity_svg = _sparkline_svg(result.net_value_curve, color="#0f766e", fill="rgba(15,118,110,0.14)")
    drawdown_svg = _sparkline_svg(
        result.drawdown_pct_curve,
        color="#c2410c",
        fill="rgba(194,65,12,0.14)",
    )

    return f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Adaptive EMA Rail 默认底座回测报告</title>
<style>
:root {{
  --bg:#eef3f8;
  --panel:#ffffff;
  --ink:#152133;
  --muted:#667085;
  --line:#d9e2ec;
  --teal:#0f766e;
  --teal-soft:rgba(15,118,110,.14);
  --orange:#c2410c;
  --orange-soft:rgba(194,65,12,.14);
  --navy:#0f172a;
  --blue:#1d4ed8;
}}
* {{ box-sizing:border-box; }}
body {{
  margin:0;
  background:
    radial-gradient(circle at top left, rgba(29,78,216,.10), transparent 32%),
    radial-gradient(circle at top right, rgba(15,118,110,.10), transparent 28%),
    var(--bg);
  color:var(--ink);
  font-family:"Microsoft YaHei","Segoe UI",Arial,sans-serif;
}}
.hero {{
  padding:38px 42px 34px;
  color:#fff;
  background:linear-gradient(135deg,#0f172a 0%,#17304d 52%,#0f766e 100%);
}}
.hero h1 {{ margin:0 0 10px; font-size:32px; }}
.hero p {{ margin:6px 0; max-width:1100px; color:#dbe7f3; line-height:1.7; }}
.wrap {{ max-width:1280px; margin:0 auto; padding:24px 24px 44px; }}
.grid {{ display:grid; gap:16px; }}
.grid-4 {{ grid-template-columns:repeat(4,minmax(0,1fr)); }}
.grid-3 {{ grid-template-columns:repeat(3,minmax(0,1fr)); }}
.grid-2 {{ grid-template-columns:repeat(2,minmax(0,1fr)); }}
.card {{
  background:var(--panel);
  border:1px solid var(--line);
  border-radius:16px;
  padding:18px;
  box-shadow:0 8px 24px rgba(15,23,42,.05);
}}
.inset-card {{ margin-top:16px; }}
.kpi .label {{ color:var(--muted); font-size:13px; margin-bottom:10px; }}
.kpi .value {{ color:var(--navy); font-size:28px; font-weight:800; line-height:1.1; }}
.kpi .sub {{ color:var(--muted); font-size:13px; margin-top:8px; }}
h2 {{ margin:30px 0 14px; font-size:22px; }}
h3 {{ margin:0 0 12px; font-size:16px; }}
p {{ line-height:1.7; }}
.note {{ color:var(--muted); }}
.pill {{
  display:inline-block;
  padding:4px 10px;
  border-radius:999px;
  font-size:12px;
  font-weight:700;
  background:#dbeafe;
  color:#1e3a8a;
}}
.chart-card svg {{ width:100%; height:auto; display:block; }}
.chart-meta {{
  display:flex;
  justify-content:space-between;
  gap:12px;
  margin-top:10px;
  color:var(--muted);
  font-size:13px;
}}
.grid-line {{ stroke:#dbe5ef; stroke-width:1; }}
table {{ width:100%; border-collapse:collapse; font-size:13px; }}
th,td {{ padding:10px 10px; border-bottom:1px solid var(--line); text-align:right; }}
th:first-child,td:first-child {{ text-align:left; }}
th {{ background:#f8fbfe; color:#475467; }}
pre {{
  margin:0;
  background:#0f172a;
  color:#e2e8f0;
  border-radius:14px;
  padding:16px;
  overflow:auto;
  font-size:12px;
  line-height:1.65;
}}
.callout {{
  border-left:5px solid var(--teal);
  background:linear-gradient(180deg, rgba(15,118,110,.08), rgba(15,118,110,.03));
  border-radius:12px;
  padding:16px 18px;
}}
.bar-row {{
  display:grid;
  grid-template-columns:96px 1fr 130px;
  gap:12px;
  align-items:center;
  margin:12px 0;
  font-size:13px;
}}
.bar-label {{ font-weight:700; color:var(--navy); }}
.bar-track {{
  height:12px;
  border-radius:999px;
  background:#e5edf5;
  overflow:hidden;
}}
.bar-track span {{
  display:block;
  height:100%;
  border-radius:999px;
  background:linear-gradient(90deg,#1d4ed8,#0f766e);
}}
.empty-chart {{
  padding:48px 16px;
  text-align:center;
  color:var(--muted);
}}
@media (max-width: 960px) {{
  .grid-4,.grid-3,.grid-2 {{ grid-template-columns:1fr; }}
  .hero {{ padding:28px 22px; }}
  .wrap {{ padding:18px 14px 32px; }}
  .chart-meta {{ flex-direction:column; }}
  .bar-row {{ grid-template-columns:1fr; gap:6px; }}
}}
</style>
</head>
<body>
<section class="hero">
  <h1>Adaptive EMA Rail 默认底座回测报告</h1>
  <p>这份 HTML 报告对应当前正式研究底座：<strong>Balanced 4H + 21/34/55/89 + EMA21 Gate</strong>。定位不是追求绝对最高收益，而是看它作为 BTC 4H 质量型做多底座时，资金曲线、回撤质量和轨道结构是否足够干净。</p>
  <p>标的：{html.escape(SYMBOL)} | 周期：{html.escape(BAR)} | 区间：{html.escape(start_label)} 到 {html.escape(end_label)} | 输出：{html.escape(str(report_path))}</p>
</section>

<main class="wrap">
  <div class="grid grid-4">
    {_kpi("结束权益", _fmt_money(report.ending_equity, 2), f"初始资金 {_fmt_money(result.initial_capital, 2)}")}
    {_kpi("总收益率", _fmt_pct(report.total_return_pct, 2), f"总盈亏 {_fmt_money(report.total_pnl, 2)}")}
    {_kpi("最大回撤", _fmt_pct(report.max_drawdown_pct, 2), f"金额 {_fmt_money(report.max_drawdown, 2)}")}
    {_kpi("Profit Factor", _fmt_number(report.profit_factor, 4), f"平均R {_fmt_number(report.average_r_multiple, 4)}")}
    {_kpi("交易次数", str(report.total_trades), f"胜率 {_fmt_pct(report.win_rate, 2)}")}
    {_kpi("手续费合计", _fmt_money(report.total_fees, 4), f"滑点 {_fmt_money(report.slippage_costs, 4)}")}
    {_kpi("止盈 / 止损", f"{report.take_profit_hits} / {report.stop_loss_hits}", "含保本与结构破坏退出")}
    {_kpi("最新底座", "4H / BTC / LONG", "Balanced + EMA21 gate")}
  </div>

  <h2>怎么看它</h2>
  <div class="card callout">
    <p>这套策略我准备把它用成 <strong>BTC 4H 的低回撤趋势腿</strong>，不是全场景主收益引擎。你重点看三件事：资金曲线是不是够平、回撤是不是长期受控、以及收益是不是主要来自少量高质量结构，而不是高频碰运气。</p>
  </div>

  <h2>曲线</h2>
  <div class="grid grid-2">
    <div class="card chart-card">
      <h3>资金曲线</h3>
      {equity_svg}
      <div class="chart-meta">
        <span>起点：{_fmt_money(result.net_value_curve[0] if result.net_value_curve else result.initial_capital, 2)}</span>
        <span>终点：{_fmt_money(result.net_value_curve[-1] if result.net_value_curve else report.ending_equity, 2)}</span>
        <span>区间：{html.escape(start_label)} 到 {html.escape(end_label)}</span>
      </div>
    </div>
    <div class="card chart-card">
      <h3>回撤曲线</h3>
      {drawdown_svg}
      <div class="chart-meta">
        <span>最大回撤：{_fmt_pct(report.max_drawdown_pct, 2)}</span>
        <span>回撤金额：{_fmt_money(report.max_drawdown, 2)}</span>
        <span>曲线口径：净值相对历史峰值</span>
      </div>
    </div>
  </div>

  <h2>账户摘要</h2>
  <div class="grid grid-3">
    <div class="card">
      <h3>收益质量</h3>
      <p>总收益率 <strong>{_fmt_pct(report.total_return_pct, 2)}</strong>，Profit Factor <strong>{_fmt_number(report.profit_factor, 4)}</strong>，平均每笔 <strong>{_fmt_money(report.average_pnl, 4)}</strong>，平均 R <strong>{_fmt_number(report.average_r_multiple, 4)}</strong>。</p>
    </div>
    <div class="card">
      <h3>风险质量</h3>
      <p>最大回撤 <strong>{_fmt_pct(report.max_drawdown_pct, 2)}</strong>，胜率 <strong>{_fmt_pct(report.win_rate, 2)}</strong>，盈利 / 亏损 / 持平 = <strong>{report.win_trades} / {report.loss_trades} / {report.breakeven_trades}</strong>。</p>
    </div>
    <div class="card">
      <h3>交易成本</h3>
      <p>手续费合计 <strong>{_fmt_money(report.total_fees, 4)}</strong>，其中 Maker <strong>{_fmt_money(report.maker_fees, 4)}</strong>，Taker <strong>{_fmt_money(report.taker_fees, 4)}</strong>，资金费 <strong>{_fmt_money(report.funding_costs, 4)}</strong>。</p>
    </div>
  </div>

  <h2>年度和月度</h2>
  <div class="grid grid-2">
    <div class="card">
      <h3>年度统计</h3>
      {_render_period_table(result.yearly_stats, "年份")}
    </div>
    <div class="card">
      <h3>月度统计</h3>
      {_render_period_table(result.monthly_stats, "月份")}
    </div>
  </div>

  <h2>轨道结构</h2>
  <div class="card">
    {_render_rail_stats(result)}
  </div>

  <h2>按入场轨道的成交分布</h2>
  <div class="card">
    {_render_distribution(rail_trade_counter)}
  </div>

  <h2>平仓原因</h2>
  <div class="card">
    {_table(["原因", "次数"], exit_rows)}
  </div>

  <h2>最近 12 笔交易</h2>
  <div class="card">
    {_render_trade_table(result)}
  </div>

  <h2>当前默认参数</h2>
  <div class="card">
    {_render_config(config)}
  </div>

  <h2>完整回测摘要</h2>
  <div class="card">
    <pre>{html.escape(backtest_text)}</pre>
  </div>
</main>
</body>
</html>
"""


def main() -> None:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = analysis_report_dir_path()
    out_dir.mkdir(parents=True, exist_ok=True)
    html_path = out_dir / f"adaptive_ema_rail_default_html_report_{stamp}.html"

    client = OkxRestClient()
    instrument = client.get_instrument(SYMBOL)
    candles = [candle for candle in load_candle_cache(SYMBOL, BAR, limit=None) if candle.confirmed]
    config = _baseline_config()
    result = _run_backtest_with_loaded_data(
        candles,
        instrument,
        config,
        data_source_note=f"local candle_cache full history | {SYMBOL} {BAR} | candles={len(candles)}",
        maker_fee_rate=MAKER_FEE,
        taker_fee_rate=TAKER_FEE,
    )
    html_path.write_text(_build_html(result, config, html_path), encoding="utf-8")
    print(html_path)


if __name__ == "__main__":
    main()
