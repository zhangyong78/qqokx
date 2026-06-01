from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from okx_quant.candle_cache import load_candle_cache
from okx_quant.ma55_strategy import (
    Ma55StrategyConfig,
    decision_to_dict,
    evaluate_ma55_strategy_from_candles,
)


REPORT_DIR = ROOT / "reports"
INST_ID = "BTC-USDT-SWAP"
BAR = "1H"
HTML_PATH = REPORT_DIR / "btc_1h_ma55_strategy.html"

STATS = {
    "bear_start": {"label": "MA55 转空 → 卖 Call Spread", "grade": "A", "range_4h": 0.807, "range_8h": 0.640, "count": 947},
    "bull_start": {"label": "MA55 转多 → 卖 Put Spread", "grade": "A", "range_4h": 0.782, "range_8h": 0.635, "count": 947},
    "bull_fade": {"label": "MA55 多头衰竭 → 卖 Call Spread", "grade": "B", "range_4h": 0.790, "range_8h": 0.634, "count": 6861},
    "bear_fade": {"label": "MA55 空头衰竭 → 卖 Put Spread", "grade": "B", "range_4h": 0.786, "range_8h": 0.644, "count": 3797},
    "dual_bear_start": {"label": "MA+EMA 双确认转空", "grade": "S", "range_4h": 0.787, "range_8h": 0.630, "count": 108},
    "dual_bull_start": {"label": "MA+EMA 双确认转多", "grade": "S", "range_4h": 0.814, "range_8h": 0.663, "count": 86},
}


def main() -> None:
    REPORT_DIR.mkdir(exist_ok=True)
    candles = load_candle_cache(INST_ID, BAR, limit=None)
    if not candles:
        raise RuntimeError(f"no candles for {INST_ID} {BAR}")

    config = Ma55StrategyConfig()
    decision = evaluate_ma55_strategy_from_candles(candles, config=config)
    payload = decision_to_dict(decision)
    (REPORT_DIR / "btc_1h_ma55_strategy_signal.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    HTML_PATH.write_text(build_html(decision, config), encoding="utf-8")
    print(HTML_PATH)
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def build_html(decision: object, config: Ma55StrategyConfig) -> str:
    action_map = {
        "sell_call_spread": "卖 Call Spread",
        "sell_put_spread": "卖 Put Spread",
        "long": "做多（波段回调）",
        "short": "做空（波段反弹）",
        "neutral": "观望",
    }
    action_text = action_map.get(decision.action, decision.action)
    grade_text = {"A": "标准仓", "B": "半仓", "S": "1.5倍仓", "none": "无"}.get(decision.signal_grade, decision.signal_grade)

    strike_block = ""
    if decision.strike_short_call is not None:
        strike_block = f"<p>Call Spread：<strong>{decision.strike_short_call:.0f} / {decision.strike_long_call:.0f}</strong></p>"
    elif decision.strike_short_put is not None:
        strike_block = f"<p>Put Spread：<strong>{decision.strike_short_put:.0f} / {decision.strike_long_put:.0f}</strong></p>"

    stats_rows = "".join(
        f"<tr><td>{item['label']}</td><td>{item['grade']}</td><td>{item['count']}</td>"
        f"<td>{item['range_4h']*100:.1f}%</td><td>{item['range_8h']*100:.1f}%</td></tr>"
        for item in STATS.values()
    )

    return f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>BTC 1H MA55 明确策略 v1</title>
<style>
:root {{
  --ink:#172033; --muted:#667085; --line:#d7dce5; --bg:#f4f7fb; --panel:#fff;
  --green:#16a34a; --red:#dc2626; --blue:#1d4ed8; --amber:#b45309;
}}
* {{ box-sizing:border-box; }}
body {{ margin:0; background:var(--bg); color:var(--ink); font-family:"Microsoft YaHei","Segoe UI",Arial,sans-serif; line-height:1.65; }}
.hero {{ background:linear-gradient(135deg,#0f172a 0%,#22405f 58%,#3f6c73 100%); color:#fff; padding:34px 38px; }}
.hero h1 {{ margin:0 0 10px; font-size:30px; }}
.hero p {{ margin:6px 0; color:#d7e5f5; max-width:1120px; }}
.wrap {{ max-width:1200px; margin:0 auto; padding:24px; }}
.card {{ background:var(--panel); border:1px solid var(--line); border-radius:8px; padding:18px; margin-bottom:16px; }}
.grid-2 {{ display:grid; grid-template-columns:1fr 1fr; gap:16px; }}
.grid-3 {{ display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:16px; }}
h2 {{ font-size:21px; margin:24px 0 12px; }}
h3 {{ font-size:16px; margin:0 0 10px; }}
table {{ width:100%; border-collapse:collapse; font-size:13px; }}
th,td {{ border-bottom:1px solid var(--line); padding:9px 10px; text-align:right; }}
th:first-child,td:first-child {{ text-align:left; }}
th {{ background:#f8fafc; color:#475467; }}
.tag {{ display:inline-block; padding:2px 8px; border-radius:999px; font-size:12px; font-weight:700; }}
.tag-a {{ background:#dbeafe; color:#1d4ed8; }}
.tag-b {{ background:#fef3c7; color:#b45309; }}
.tag-s {{ background:#ede9fe; color:#6d28d9; }}
.tag-none {{ background:#f1f5f9; color:#64748b; }}
.callout {{ border-left:5px solid var(--amber); background:#fffbeb; border-radius:6px; padding:14px 16px; }}
.rule {{ margin:0 0 8px; }}
.formula {{ background:#0f172a; color:#e5edf6; padding:14px 16px; border-radius:8px; font-family:Consolas,monospace; font-size:13px; overflow:auto; }}
@media (max-width: 960px) {{ .grid-2,.grid-3 {{ grid-template-columns:1fr; }} }}
</style>
</head>
<body>
<section class="hero">
  <h1>BTC 1H · MA55 明确策略 v1</h1>
  <p>唯一主信号线：<strong>MA55</strong>。EMA55 仅用于 S 级双确认。基于 56k+ 根 1H 历史统计：4H 区间成功率 78–81%。</p>
  <p>主战场：<strong>卖期权价差</strong>（1–2 DTE）；辅战场：已确认波段上的 MA55 回调/反弹方向单。</p>
</section>

<main class="wrap">
  <div class="card callout">
    <h2>当前信号（{decision.timestamp}）</h2>
    <p><strong>动作：</strong>{action_text} · <span class="tag tag-{decision.signal_grade.lower()}">{grade_text}</span> · 仓位系数 {decision.size_multiplier:.1f}x</p>
    <p><strong>信号：</strong>{decision.signal_name}</p>
    <p><strong>MA55 斜率态：</strong>{decision.slope_regime_label} · <strong>三态：</strong>{decision.market_regime_label}</p>
    <p>收盘 {decision.close:.2f} · MA55 {decision.ma55:.2f} · ATR14 {decision.atr14:.2f} · run {decision.run_length} 根</p>
    {strike_block}
    <p><strong>止损参考：</strong>{f"{decision.stop_price:.2f}" if decision.stop_price else "—"}</p>
    <p><strong>理由：</strong>{decision.reason}</p>
    <p class="note">通过过滤：{", ".join(decision.filters_passed) or "无"} · 未过：{", ".join(decision.filters_failed) or "无"}</p>
  </div>

  <h2>策略总览</h2>
  <div class="grid-3">
    <div class="card"><h3>第一步：三态过滤</h3><p class="rule">盘整 → <strong>不做</strong></p><p class="rule">上升/下降 → 才允许看信号</p></div>
    <div class="card"><h3>第二步：MA55 斜率事件</h3><p class="rule">转空/转多 = A 级</p><p class="rule">衰竭 = B 级（半仓）</p></div>
    <div class="card"><h3>第三步：执行</h3><p class="rule">卖 1–2 DTE 价差</p><p class="rule">8H 或 50% 权利金止盈</p></div>
  </div>

  <h2>1. 状态定义（每根 1H 收盘计算）</h2>
  <div class="card formula">
    z = (Close - MA55) / ATR14<br>
    s = LinRegSlope(MA55, 5) / MA55<br>
    r = 连续收在 MA55 同侧根数<br><br>
    盘整：r ≤ {config.tau_chop} 或 (|s| &lt; {config.flat_threshold} 且 |z| &lt; {config.dist_chop_atr}) → <strong>禁止新开仓</strong><br>
    上升三态：z&gt;0 且 s&gt;0 且非盘整<br>
    下降三态：z&lt;0 且 s&lt;0 且非盘整
  </div>

  <h2>2. 入场规则（主策略：卖期权价差）</h2>
  <div class="card">
    <table>
      <tr><th>信号</th><th>级别</th><th>动作</th><th>过滤条件</th><th>4H区间率</th></tr>
      <tr><td>MA55 转空</td><td><span class="tag tag-a">A</span></td><td>卖 Call Spread</td><td>收盘&lt;MA55；非上升三态</td><td>80.7%</td></tr>
      <tr><td>MA55 转多</td><td><span class="tag tag-a">A</span></td><td>卖 Put Spread</td><td>收盘&gt;MA55；非下降三态</td><td>78.2%</td></tr>
      <tr><td>MA55 多头衰竭</td><td><span class="tag tag-b">B</span></td><td>卖 Call Spread</td><td>斜率仍&gt;0，连续3根减速</td><td>79.0%</td></tr>
      <tr><td>MA55 空头衰竭</td><td><span class="tag tag-b">B</span></td><td>卖 Put Spread</td><td>斜率仍&lt;0，连续3根回升</td><td>78.6%</td></tr>
      <tr><td>MA+EMA 同根转势</td><td><span class="tag tag-s">S</span></td><td>同上，仓位×1.5</td><td>双确认</td><td>78–81%</td></tr>
    </table>
  </div>

  <h2>3. 行权价与到期</h2>
  <div class="card formula">
    到期：1–2 天（DTE ≤ {config.max_dte_days}），持有目标 {config.hold_hours}H<br><br>
    卖 Call：K_short = ceil(Close + {config.short_otm_atr}×ATR)<br>
             K_long  = ceil(Close + ({config.short_otm_atr}+{config.spread_width_atr})×ATR)<br><br>
    卖 Put ：K_short = floor(Close - {config.short_otm_atr}×ATR)<br>
             K_long  = floor(Close - ({config.short_otm_atr}+{config.spread_width_atr})×ATR)<br><br>
    行权价步长：BTC 取整到 100
  </div>

  <h2>4. 仓位与风控</h2>
  <div class="grid-2">
    <div class="card">
      <h3>仓位</h3>
      <p>A 级：账户风险 1.0%</p>
      <p>B 级：账户风险 0.5%</p>
      <p>S 级：账户风险 1.5%</p>
      <p>张数 = 风险金额 / (价差宽度 - 净收权利金)</p>
      <p>同方向最多 2 笔重叠；总卖方风险 ≤ 3%</p>
    </div>
    <div class="card">
      <h3>平仓</h3>
      <p><strong>止盈：</strong>浮盈 ≥ 收权利金 {config.take_profit_credit_pct * 100:.0f}%</p>
      <p><strong>时间：</strong>持有满 {config.hold_hours}H 且浮盈&gt;0</p>
      <p><strong>止损：</strong>标的触及短腿行权价；或逆向突破 {config.stop_atr} ATR</p>
      <p><strong>强制：</strong>MA55 斜率反向翻转；三态回到盘整</p>
    </div>
  </div>

  <h2>5. 辅策略：波段方向单（仅在已确认趋势）</h2>
  <div class="card">
    <p class="rule"><strong>做多：</strong>上升三态 + bull_run + run&gt;{config.tau_chop} → MA55 附近挂多，止损 MA55 - {config.stop_atr} ATR</p>
    <p class="rule"><strong>做空：</strong>下降三态 + bear_run + run&gt;{config.tau_chop} → MA55 附近挂空，止损 MA55 + {config.stop_atr} ATR</p>
    <p class="note">方向单胜率仅 ~50%，仅作辅助；主策略仍是卖价差。</p>
  </div>

  <h2>6. 明确不做的事</h2>
  <div class="card">
    <p>❌ 盘整态新开仓（段中位仅 2 根，交叉率 33/100）</p>
    <p>❌ 只看 EMA55 单独下单</p>
    <p>❌ 裸卖 Call / Put</p>
    <p>❌ DTE &gt; 2 天（48H 区间成功率降至 ~25%）</p>
    <p>❌ 双推进强趋势中追卖（bear_run/bull_run 中段）</p>
  </div>

  <h2>7. 历史统计支撑</h2>
  <div class="card">
    <table>
      <tr><th>规则</th><th>级别</th><th>样本</th><th>4H区间</th><th>8H区间</th></tr>
      {stats_rows}
    </table>
    <p class="note">区间成功 = 未来窗口内逆向偏移 ≤ 1.5 ATR。这是卖期权的核心统计，不是方向胜率。</p>
  </div>

  <h2>8. 每根 1H 收盘执行清单</h2>
  <div class="card">
    <p>1. 计算 z, s, r → 三态是否盘整？是 → 停止</p>
    <p>2. MA55 斜率是否刚发生转势/衰竭？</p>
    <p>3. 过滤：收盘在 MA55 哪一侧？是否与三态冲突？</p>
    <p>4. EMA55 是否同根确认 → 定 A/B/S 级别</p>
    <p>5. 算 ATR 行权价，选 1–2 DTE 合约，下 spread</p>
    <p>6. 设 8H 提醒 + 短腿触及提醒</p>
  </div>
</main>
</body>
</html>"""


if __name__ == "__main__":
    main()
