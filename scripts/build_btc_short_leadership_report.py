from __future__ import annotations

import base64
import json
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
REPORT_DIR = ROOT / "reports"
OUT = REPORT_DIR / "short_strategy_leadership_report.html"
INITIAL_CAPITAL = 10_000.0
FIXED_RISK_U = 10.0


def main() -> None:
    comparison = pd.read_csv(REPORT_DIR / "strategy_comparison.csv")
    events = pd.read_csv(REPORT_DIR / "event_study_summary.csv")
    stability = pd.read_csv(REPORT_DIR / "parameter_stability.csv")
    trades = pd.read_csv(REPORT_DIR / "trades.csv")
    best_configs = json.loads((REPORT_DIR / "best_configs.json").read_text(encoding="utf-8"))
    best = best_configs[0]
    best_name = best["name"]
    best_metrics = best["conservative_metrics"]

    best_rows = comparison[(comparison["name"] == best_name)].copy()
    cost_rows = best_rows.sort_values("cost_scenario")
    top = (
        comparison[comparison["cost_scenario"] == "conservative_cost"]
        .sort_values("score", ascending=False)
        .head(8)
        .copy()
    )
    book = build_fixed_risk_book(trades, initial_capital=INITIAL_CAPITAL, fixed_risk=FIXED_RISK_U)

    exact_event = pick_event_for_best(best, events)
    png_count = len(list((REPORT_DIR / "sample_charts").glob("*.png")))
    sample_chart = pick_sample_chart()

    html = f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>BTC 1H 做空策略领导版报告</title>
<style>
:root {{
  --ink:#172033; --muted:#667085; --line:#d8dee9; --bg:#f5f7fb; --panel:#ffffff;
  --good:#0f9f6e; --bad:#c2410c; --blue:#1d4ed8; --navy:#111827; --amber:#b45309;
}}
* {{ box-sizing:border-box; }}
body {{ margin:0; font-family: "Microsoft YaHei", "Segoe UI", Arial, sans-serif; background:var(--bg); color:var(--ink); }}
.hero {{ background:linear-gradient(135deg,#101827 0%,#243044 62%,#324155 100%); color:white; padding:34px 42px 30px; }}
.hero h1 {{ margin:0 0 10px; font-size:30px; letter-spacing:0; }}
.hero p {{ margin:6px 0; color:#d5dbea; font-size:15px; max-width:1120px; }}
.wrap {{ max-width:1220px; margin:0 auto; padding:26px 24px 48px; }}
.grid {{ display:grid; gap:16px; }}
.grid-4 {{ grid-template-columns:repeat(4,minmax(0,1fr)); }}
.grid-3 {{ grid-template-columns:repeat(3,minmax(0,1fr)); }}
.grid-2 {{ grid-template-columns:repeat(2,minmax(0,1fr)); }}
.card {{ background:var(--panel); border:1px solid var(--line); border-radius:8px; padding:18px; box-shadow:0 1px 2px rgba(16,24,40,.04); }}
.kpi .label {{ color:var(--muted); font-size:13px; margin-bottom:8px; }}
.kpi .value {{ font-size:28px; font-weight:800; color:var(--navy); }}
.kpi .sub {{ color:var(--muted); margin-top:6px; font-size:13px; }}
h2 {{ font-size:21px; margin:30px 0 14px; }}
h3 {{ font-size:16px; margin:0 0 10px; }}
.pill {{ display:inline-block; border-radius:999px; padding:4px 10px; font-size:12px; font-weight:700; }}
.pill.good {{ color:#05603a; background:#dcfae6; }}
.pill.warn {{ color:#92400e; background:#fef3c7; }}
.pill.blue {{ color:#1e3a8a; background:#dbeafe; }}
.answer {{ font-size:17px; line-height:1.7; }}
.answer strong {{ color:var(--navy); }}
.barrow {{ display:grid; grid-template-columns:170px 1fr 72px; gap:12px; align-items:center; margin:10px 0; font-size:13px; }}
.bar {{ height:12px; background:#e6eaf2; border-radius:999px; overflow:hidden; }}
.bar span {{ display:block; height:100%; background:var(--blue); border-radius:999px; }}
.bar.good span {{ background:var(--good); }}
.bar.warn span {{ background:var(--amber); }}
table {{ border-collapse:collapse; width:100%; font-size:13px; background:white; }}
th,td {{ border-bottom:1px solid var(--line); padding:9px 10px; text-align:right; }}
th:first-child,td:first-child {{ text-align:left; }}
th {{ color:#475467; background:#f8fafc; font-weight:700; }}
.note {{ color:var(--muted); font-size:13px; line-height:1.6; }}
.split-card {{ border-left:5px solid var(--blue); }}
.split-card.test {{ border-left-color:var(--good); }}
.split-card.train {{ border-left-color:#64748b; }}
.split-card.val {{ border-left-color:#7c3aed; }}
.imgbox img {{ width:100%; border:1px solid var(--line); border-radius:6px; display:block; }}
.callout {{ border-left:5px solid var(--amber); background:#fffbeb; padding:14px 16px; border-radius:6px; line-height:1.65; }}
.small {{ font-size:12px; color:var(--muted); }}
@media (max-width: 920px) {{
  .grid-4,.grid-3,.grid-2 {{ grid-template-columns:1fr; }}
  .hero {{ padding:26px 22px; }}
  .wrap {{ padding:20px 14px 36px; }}
  .barrow {{ grid-template-columns:1fr; gap:6px; }}
}}
</style>
</head>
<body>
<section class="hero">
  <h1>BTC 1小时 Short-only 策略研究结论</h1>
  <p>基于 BTC-USDT-SWAP 1小时历史K线，先做事件统计，再做策略回测；没有简单反向套用 EMA21/EMA55 多头逻辑。</p>
  <p>最终评价采用保守成本：单边 0.075%，一进一出约 0.15%。</p>
</section>

<main class="wrap">
  <div class="grid grid-4">
    {kpi("推荐版本", best_name, "D类：大阴线后弱反抽继续空")}
    {kpi("测试集 PF", pct_or_num(best_metrics["test"]["profit_factor"], pct=False), "目标线：> 1.15")}
    {kpi("测试集胜率", pct(best_metrics["test"]["win_rate"]), "真实交易胜率，不是事件胜率")}
    {kpi("测试集最大回撤", pct(best_metrics["test"]["max_drawdown"]), "保守成本后")}
  </div>

  <h2>一句话结论</h2>
  <div class="card answer">
    推荐版本 <strong>{best_name}</strong> 不是靠高胜率赚钱，而是靠<strong>反抽失败后的 2R 固定赔率</strong>赚钱。
    测试集交易胜率为 <strong>{pct(best_metrics["test"]["win_rate"])}</strong>，Profit Factor 为
    <strong>{best_metrics["test"]["profit_factor"]:.2f}</strong>，说明亏损次数并不少，但盈利单平均能覆盖亏损单。
  </div>

  <h2>按你说的资金口径重算</h2>
  <div class="grid grid-4">
    {kpi("起始资金", money(book["initial_capital"]), "按你给的 10000 记账")}
    {kpi("单笔止损", money(book["fixed_risk"]), "每笔固定亏损上限")}
    {kpi("最后剩余资金", money(book["final_capital"]), f"总盈亏 {money(book['total_pnl'])}")}
    {kpi("总交易次数", str(int(book["total_trades"])), f"赚钱 {int(book['win_count'])} / 亏钱 {int(book['loss_count'])}")}
  </div>

  <div class="grid grid-3">
    <div class="card">
      <h3>一眼看懂</h3>
      <p class="answer">如果把每笔风险固定成 <strong>10U</strong>，这套策略在全样本一共交易 <strong>{int(book["total_trades"])}</strong> 次，
      其中赚钱 <strong>{int(book["win_count"])}</strong> 次，亏钱 <strong>{int(book["loss_count"])}</strong> 次，胜率 <strong>{pct(book["win_rate"])}</strong>。</p>
    </div>
    <div class="card">
      <h3>资金结果</h3>
      <p class="answer">起始 <strong>{money(book["initial_capital"])}</strong>，最后剩余 <strong>{money(book["final_capital"])}</strong>，
      累计赚亏 <strong>{money(book["total_pnl"])}</strong>，资金回报率 <strong>{pct(book["total_return"])}</strong>。</p>
    </div>
    <div class="card">
      <h3>风险结果</h3>
      <p class="answer">最大资金回撤约 <strong>{pct(book["max_drawdown"])}</strong>，最大连续亏损 <strong>{int(book["max_loss_streak"])}</strong> 笔，
      平均每笔盈亏 <strong>{money(book["avg_pnl"])}</strong>。</p>
    </div>
  </div>

  <h2>年度统计</h2>
  <div class="card">
    {period_table(book["yearly"], "year")}
  </div>

  <h2>月度统计</h2>
  <div class="card">
    <p class="note">按平仓月份统计。这里的盈亏已经按“每笔止损 10U”重新换算。</p>
    {period_table(book["monthly"], "month")}
  </div>

  <h2>它是不是动态盈亏比？</h2>
  <div class="grid grid-2">
    <div class="card">
      <h3>当前推荐版</h3>
      <p class="answer"><span class="pill blue">固定 2R</span> 入场后使用固定止盈：目标价 = 入场价 - 2 × 初始风险R。止损是反抽高点 + 0.3 ATR 缓冲，仓位按每笔风险 0.5% 计算。</p>
      <p class="note">所以它不是动态盈亏比；它是“动态止损距离 + 固定R倍数止盈 + 风险定仓”。</p>
    </div>
    <div class="card">
      <h3>本轮也测试过</h3>
      <p class="answer"><span class="pill warn">动态退出未胜出</span> 对支撑破位原型测试过 ATR trailing stop，但在本轮样本里不如固定R版本稳定。</p>
      <p class="note">后续可以继续测试“先1R减仓、余仓ATR跟踪”的半动态版本。</p>
    </div>
  </div>

  <h2>胜率看两个口径</h2>
  <div class="grid grid-2">
    <div class="card">
      <h3>真实交易胜率：按进出场后净利润统计</h3>
      {bar("训练集", best_metrics["train"]["win_rate"], "train")}
      {bar("验证集", best_metrics["validation"]["win_rate"], "validation")}
      {bar("测试集", best_metrics["test"]["win_rate"], "test")}
      {bar("全样本", best_metrics["all"]["win_rate"], "all")}
      <p class="note">这是真正执行止盈止损和成本后的交易胜率。测试集为 {pct(best_metrics["test"]["win_rate"])}。</p>
    </div>
    <div class="card">
      <h3>信号事件胜率：信号后价格是否下跌</h3>
      {event_bars(exact_event)}
      <p class="note">D类信号的原始方向胜率不高，但“先触发1R而不是止损”的比例较高；它更像赔率型信号，不是高命中率信号。</p>
    </div>
  </div>

  <h2>训练 / 验证 / 测试表现</h2>
  <div class="grid grid-4">
    {split_card("训练集", best_metrics["train"], "train")}
    {split_card("验证集", best_metrics["validation"], "val")}
    {split_card("测试集", best_metrics["test"], "test")}
    {split_card("全样本", best_metrics["all"], "all")}
  </div>

  <h2>成本敏感性</h2>
  <div class="card">
    <p class="note">领导重点看这一段：不加成本会明显高估收益。最终排序使用保守成本。</p>
    {cost_table(cost_rows)}
  </div>

  <h2>为什么选 D 类弱反抽，而不是简单破位或假突破？</h2>
  <div class="grid grid-3">
    <div class="card"><h3>市场含义</h3><p>先有大阴线打破结构，随后反抽无力，说明追空不是第一时间追，而是在弱反弹失败后进场。</p></div>
    <div class="card"><h3>交易逻辑</h3><p>止损放在反抽高点上方，风险定义清楚；止盈用 2R，避免小波动里过早离场。</p></div>
    <div class="card"><h3>风险画像</h3><p>训练集弱、验证/测试更好，说明还不能重仓实盘；适合先作为观察版或小资金灰度。</p></div>
  </div>

  <h2>候选策略排序</h2>
  <div class="card">
    {top_table(top)}
    <p class="note">PF=999 的项目通常交易太少且没有亏损，不能按表面数值解读，因此推荐看交易数足够的前几名。</p>
  </div>

  <h2>参数稳定性要点</h2>
  <div class="card">
    {stability_table(stability)}
    <p class="note">稳定性不是只看单个最优参数，而是看相邻参数区域是否也能存活。本轮 D类更靠前，但样本数仍有限；A类 break=48、TP=2.0~2.5 是下一步值得继续研究的区域。</p>
  </div>

  <h2>资金曲线和回撤</h2>
  <div class="grid grid-2">
    <div class="card imgbox"><h3>资金曲线</h3>{img("equity_curve.png")}</div>
    <div class="card imgbox"><h3>回撤曲线</h3>{img("drawdown_curve.png")}</div>
  </div>

  <h2>典型交易截图</h2>
  <div class="card imgbox">
    <p class="note">已生成 {png_count} 张交易截图，包括最大盈利、最大亏损、随机样本和连续亏损区间。下面展示一张代表图。</p>
    {sample_chart}
  </div>

  <h2>给领导的最终判断</h2>
  <div class="callout">
    当前策略已经不是“拍脑袋反做多”，而是从空头事件统计里筛出的弱反抽失败模型。
    但它不是高胜率策略，测试集胜率约 {pct(best_metrics["test"]["win_rate"])}，靠 2R 赔率和清晰止损生存。
    如果按你说的口径，每笔只亏 10U、起始 10000，这套样本最后是 <strong>{money(book["final_capital"])}</strong>。
    建议定位为<strong>观察版/小资金灰度</strong>，下一步优先研发半动态止盈、反抽限价入场、以及更严格的强多头环境过滤。
  </div>
</main>
</body>
</html>
"""
    OUT.write_text(html, encoding="utf-8")
    print(OUT)


def pick_event_for_best(best: dict[str, object], events: pd.DataFrame) -> pd.Series:
    params = best["params"]
    break_n = int(params.get("break_n", 48))
    preferred = f"D_weak_bounce_{break_n}_5"
    if preferred in set(events["event"]):
        return events[events["event"] == preferred].iloc[0]
    fallback = "D_weak_bounce_20_3"
    return events[events["event"] == fallback].iloc[0]


def pct(value: float) -> str:
    return f"{float(value) * 100:.1f}%"


def money(value: float) -> str:
    return f"{float(value):,.2f}"


def pct_or_num(value: float, *, pct: bool) -> str:
    return f"{float(value) * 100:.1f}%" if pct else f"{float(value):.2f}"


def kpi(label: str, value: str, sub: str) -> str:
    return f'<div class="card kpi"><div class="label">{label}</div><div class="value">{value}</div><div class="sub">{sub}</div></div>'


def bar(label: str, value: float, klass: str) -> str:
    width = max(0, min(100, float(value) * 100))
    tone = "good" if width >= 48 else "warn"
    return f'<div class="barrow"><div>{label}</div><div class="bar {tone}"><span style="width:{width:.1f}%"></span></div><div>{width:.1f}%</div></div>'


def event_bars(row: pd.Series) -> str:
    labels = [
        ("4小时后下跌率", "down_rate_4h"),
        ("8小时后下跌率", "down_rate_8h"),
        ("12小时后下跌率", "down_rate_12h"),
        ("24小时后下跌率", "down_rate_24h"),
        ("24小时先到1R比例", "target_1r_first_rate_24h"),
    ]
    header = f'<p class="note">参考事件：{row["event"]}，样本 {int(row["count"])} 个。</p>'
    return header + "\n".join(bar(label, float(row[col]), "event") for label, col in labels)


def split_card(title: str, metrics: dict[str, float], klass: str) -> str:
    return f"""
<div class="card split-card {klass}">
  <h3>{title}</h3>
  <div class="barrow"><div>收益</div><div class="bar good"><span style="width:{clip_bar(metrics['total_return'])}%"></span></div><div>{pct(metrics['total_return'])}</div></div>
  <p>PF：<strong>{metrics['profit_factor']:.2f}</strong></p>
  <p>胜率：<strong>{pct(metrics['win_rate'])}</strong></p>
  <p>最大回撤：<strong>{pct(metrics['max_drawdown'])}</strong></p>
  <p>交易数：<strong>{metrics['trade_count']:.0f}</strong></p>
</div>
"""


def clip_bar(value: float) -> float:
    return max(2, min(100, abs(float(value)) * 800))


def cost_table(rows: pd.DataFrame) -> str:
    labels = {"no_cost": "无成本", "normal_cost": "正常成本", "conservative_cost": "保守成本"}
    parts = ["<table><tr><th>成本场景</th><th>全样本收益</th><th>全样本PF</th><th>测试集收益</th><th>测试集PF</th></tr>"]
    for _, row in rows.iterrows():
        parts.append(
            f"<tr><td>{labels.get(row['cost_scenario'], row['cost_scenario'])}</td>"
            f"<td>{pct(row['all_total_return'])}</td><td>{row['all_profit_factor']:.2f}</td>"
            f"<td>{pct(row['test_total_return'])}</td><td>{row['test_profit_factor']:.2f}</td></tr>"
        )
    parts.append("</table>")
    return "\n".join(parts)


def top_table(rows: pd.DataFrame) -> str:
    parts = ["<table><tr><th>排名</th><th>策略</th><th>版本</th><th>测试收益</th><th>测试PF</th><th>测试胜率</th><th>测试交易数</th></tr>"]
    for i, (_, row) in enumerate(rows.iterrows(), start=1):
        parts.append(
            f"<tr><td>{i}</td><td>{row['strategy']}</td><td>{row['name']}</td>"
            f"<td>{pct(row['test_total_return'])}</td><td>{row['test_profit_factor']:.2f}</td>"
            f"<td>{pct(row['test_win_rate'])}</td><td>{row['test_trade_count']:.0f}</td></tr>"
        )
    parts.append("</table>")
    return "\n".join(parts)


def stability_table(stability: pd.DataFrame) -> str:
    rows = stability.head(10)
    parts = ["<table><tr><th>策略</th><th>参数</th><th>取值</th><th>测试PF均值</th><th>稳定达标率</th><th>平均测试交易数</th></tr>"]
    for _, row in rows.iterrows():
        parts.append(
            f"<tr><td>{row['strategy']}</td><td>{row['param']}</td><td>{row['value']}</td>"
            f"<td>{row['mean_test_pf']:.2f}</td><td>{pct(row['stable_pf_rate'])}</td><td>{row['mean_test_trades']:.1f}</td></tr>"
        )
    parts.append("</table>")
    return "\n".join(parts)


def build_fixed_risk_book(trades: pd.DataFrame, *, initial_capital: float, fixed_risk: float) -> dict[str, object]:
    frame = trades.copy()
    frame["exit_time"] = pd.to_datetime(frame["exit_time"], utc=True)
    frame["pnl_fixed"] = frame["net_r"].astype(float) * fixed_risk
    frame["capital_after_fixed"] = initial_capital + frame["pnl_fixed"].cumsum()
    frame["win_flag"] = frame["pnl_fixed"] > 0
    frame["loss_flag"] = frame["pnl_fixed"] <= 0
    frame["year"] = frame["exit_time"].dt.year.astype(str)
    frame["month"] = frame["exit_time"].dt.tz_convert(None).dt.to_period("M").astype(str)

    equity = frame["capital_after_fixed"]
    drawdown = equity / equity.cummax() - 1

    yearly = (
        frame.groupby("year", as_index=False)
        .agg(
            trades=("pnl_fixed", "count"),
            wins=("win_flag", "sum"),
            losses=("loss_flag", "sum"),
            pnl=("pnl_fixed", "sum"),
            avg_pnl=("pnl_fixed", "mean"),
            win_rate=("win_flag", "mean"),
        )
        .sort_values("year")
    )
    yearly["end_capital"] = initial_capital + yearly["pnl"].cumsum()

    monthly = (
        frame.groupby("month", as_index=False)
        .agg(
            trades=("pnl_fixed", "count"),
            wins=("win_flag", "sum"),
            losses=("loss_flag", "sum"),
            pnl=("pnl_fixed", "sum"),
            avg_pnl=("pnl_fixed", "mean"),
            win_rate=("win_flag", "mean"),
        )
        .sort_values("month")
    )
    monthly["end_capital"] = initial_capital + monthly["pnl"].cumsum()

    pnl_values = frame["pnl_fixed"].tolist()
    max_loss_streak = 0
    current_loss_streak = 0
    for pnl in pnl_values:
        if pnl <= 0:
            current_loss_streak += 1
            max_loss_streak = max(max_loss_streak, current_loss_streak)
        else:
            current_loss_streak = 0

    total_pnl = float(frame["pnl_fixed"].sum())
    return {
        "initial_capital": initial_capital,
        "fixed_risk": fixed_risk,
        "final_capital": initial_capital + total_pnl,
        "total_pnl": total_pnl,
        "total_return": total_pnl / initial_capital,
        "total_trades": int(len(frame)),
        "win_count": int(frame["win_flag"].sum()),
        "loss_count": int(frame["loss_flag"].sum()),
        "win_rate": float(frame["win_flag"].mean()),
        "avg_pnl": float(frame["pnl_fixed"].mean()),
        "max_drawdown": float(drawdown.min()) if not drawdown.empty else 0.0,
        "max_loss_streak": int(max_loss_streak),
        "yearly": yearly,
        "monthly": monthly.tail(24),
    }


def period_table(frame: pd.DataFrame, period_col: str) -> str:
    label = "年度" if period_col == "year" else "月份"
    parts = [
        f"<table><tr><th>{label}</th><th>交易次数</th><th>赚钱次数</th><th>亏钱次数</th><th>胜率</th><th>当期盈亏</th><th>期末资金</th><th>平均每笔</th></tr>"
    ]
    for _, row in frame.iterrows():
        parts.append(
            f"<tr><td>{row[period_col]}</td><td>{int(row['trades'])}</td><td>{int(row['wins'])}</td><td>{int(row['losses'])}</td>"
            f"<td>{pct(row['win_rate'])}</td><td>{money(row['pnl'])}</td><td>{money(row['end_capital'])}</td><td>{money(row['avg_pnl'])}</td></tr>"
        )
    parts.append("</table>")
    return "\n".join(parts)


def img(name: str) -> str:
    path = REPORT_DIR / name
    return f'<img src="data:image/png;base64,{b64(path)}" alt="{name}">'


def pick_sample_chart() -> str:
    candidates = sorted((REPORT_DIR / "sample_charts").glob("top_win_*.png"))
    if not candidates:
        candidates = sorted((REPORT_DIR / "sample_charts").glob("*.png"))
    if not candidates:
        return '<p class="note">未找到样例交易图。</p>'
    path = candidates[0]
    return f'<img src="data:image/png;base64,{b64(path)}" alt="{path.name}"><p class="small">{path.name}</p>'


def b64(path: Path) -> str:
    return base64.b64encode(path.read_bytes()).decode("ascii")


if __name__ == "__main__":
    main()
