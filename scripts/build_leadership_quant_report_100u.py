from __future__ import annotations

import base64
import html
import io
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
REPORT_DIR = ROOT / "reports" / "best_parameter_bundle_1h_standard_100u"

REPORT_HTML = REPORT_DIR / "report.html"
LEADER_REPORT_HTML = REPORT_DIR / "leader_report.html"
TRADES_CSV = REPORT_DIR / "trades.csv"
SUMMARY_CSV = REPORT_DIR / "summary.csv"
EQUITY_CSV = REPORT_DIR / "equity_curve.csv"
MONTHLY_BREAKDOWN_CSV = REPORT_DIR / "monthly_side_coin_breakdown.csv"
YEARLY_BREAKDOWN_CSV = REPORT_DIR / "yearly_side_coin_breakdown.csv"


plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "Noto Sans CJK SC", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False


def money(value: float) -> str:
    return f"{value:,.2f}U"


def pct(value: float) -> str:
    return f"{value:,.2f}%"


def img64(fig) -> str:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    return base64.b64encode(buf.getvalue()).decode("ascii")


def table_html(df: pd.DataFrame, *, max_rows: int | None = None, classes: str = "") -> str:
    if df.empty:
        return "<p class='muted'>暂无数据</p>"
    data = df.head(max_rows) if max_rows is not None else df
    cls = f" class='{classes}'" if classes else ""
    head = "".join(f"<th>{html.escape(str(col))}</th>" for col in data.columns)
    rows = []
    for _, row in data.iterrows():
        cells = "".join(f"<td>{html.escape(str(value))}</td>" for value in row.tolist())
        rows.append(f"<tr>{cells}</tr>")
    return f"<table{cls}><thead><tr>{head}</tr></thead><tbody>{''.join(rows)}</tbody></table>"


def make_equity_chart(equity: pd.DataFrame) -> str:
    fig, ax = plt.subplots(figsize=(11, 4.2))
    x = pd.to_datetime(equity["时间"])
    ax.plot(x, equity["总权益"], color="#155e75", linewidth=1.5)
    ax.fill_between(x, equity["总权益"], equity["总权益"].min(), color="#67e8f9", alpha=0.18)
    ax.set_title("资金曲线：最终赚多少钱")
    ax.set_ylabel("账户权益 U")
    ax.grid(alpha=0.22)
    return img64(fig)


def make_drawdown_chart(equity: pd.DataFrame) -> str:
    fig, ax = plt.subplots(figsize=(11, 3.8))
    x = pd.to_datetime(equity["时间"])
    drawdown_u = equity["历史峰值"] - equity["总权益"]
    ax.plot(x, drawdown_u, color="#b91c1c", linewidth=1.3)
    ax.fill_between(x, drawdown_u, 0, color="#fecaca", alpha=0.45)
    ax.set_title("回撤曲线：中途最多亏过多少")
    ax.set_ylabel("回撤 U")
    ax.grid(alpha=0.22)
    return img64(fig)


def make_bar_chart(df: pd.DataFrame, *, label_col: str, value_col: str, title: str) -> str:
    fig, ax = plt.subplots(figsize=(9.5, 4))
    colors = ["#16a34a" if float(v) >= 0 else "#dc2626" for v in df[value_col]]
    ax.bar(df[label_col].astype(str), df[value_col].astype(float), color=colors)
    ax.axhline(0, color="#111827", linewidth=0.8)
    ax.set_title(title)
    ax.set_ylabel("利润 U")
    ax.grid(axis="y", alpha=0.22)
    plt.xticks(rotation=18, ha="right")
    return img64(fig)


def make_cost_chart(cost_rows: pd.DataFrame) -> str:
    fig, ax = plt.subplots(figsize=(8.5, 3.8))
    colors = ["#0f766e", "#d97706", "#dc2626", "#2563eb"]
    ax.bar(cost_rows["项目"], cost_rows["金额U"], color=colors)
    ax.axhline(0, color="#111827", linewidth=0.8)
    ax.set_title("利润被成本消耗的过程")
    ax.set_ylabel("U")
    ax.grid(axis="y", alpha=0.22)
    plt.xticks(rotation=12, ha="right")
    return img64(fig)


def profit_factor(pnl: pd.Series) -> float:
    wins = pnl[pnl > 0].sum()
    losses = abs(pnl[pnl < 0].sum())
    if losses == 0:
        return 999.0 if wins > 0 else 0.0
    return float(wins / losses)


def summarize_group(df: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for key, part in df.groupby(group_cols, sort=True):
        if not isinstance(key, tuple):
            key = (key,)
        pnl = part["盈亏"].astype(float)
        row = {col: value for col, value in zip(group_cols, key)}
        row.update(
            {
                "交易次数": len(part),
                "胜率": pct((pnl > 0).mean() * 100 if len(part) else 0),
                "Profit Factor": f"{profit_factor(pnl):.4f}",
                "手续费": money(float(part["手续费"].sum())),
                "滑点影响": money(float(part["滑点成本"].sum())),
                "净利润": money(float(pnl.sum())),
            }
        )
        rows.append(row)
    return pd.DataFrame(rows)


def numeric_pnl(df: pd.DataFrame, col: str = "净利润") -> pd.Series:
    return df[col].astype(str).str.replace("U", "").str.replace(",", "").astype(float)


def sort_by_money(df: pd.DataFrame, col: str = "净利润", ascending: bool = False) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    out["_money_sort"] = numeric_pnl(out, col)
    return out.sort_values("_money_sort", ascending=ascending).drop(columns=["_money_sort"])


def format_breakdown_table(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    for col in ["当期利润U", "累计利润U"]:
        if col in out.columns:
            out[col] = out[col].map(money)
    return out


def period_side_coin_table(breakdown: pd.DataFrame, *, period_col_name: str) -> pd.DataFrame:
    data = breakdown[breakdown["项目"] == "分项"].copy()
    if data.empty:
        return pd.DataFrame(columns=[period_col_name, "币种", "多头利润", "空头利润", "合计利润"])

    pivot = data.pivot_table(
        index=["期间", "币种"],
        columns="方向",
        values="当期利润U",
        aggfunc="sum",
        fill_value=0.0,
    ).reset_index()
    for side in ["多头", "空头"]:
        if side not in pivot.columns:
            pivot[side] = 0.0
    pivot["合计利润"] = pivot["多头"] + pivot["空头"]
    pivot = pivot.rename(
        columns={
            "期间": period_col_name,
            "多头": "多头利润",
            "空头": "空头利润",
        }
    )
    for col in ["多头利润", "空头利润", "合计利润"]:
        pivot[col] = pivot[col].map(money)
    return pivot[[period_col_name, "币种", "多头利润", "空头利润", "合计利润"]].sort_values([period_col_name, "币种"])


def period_coin_sections(table: pd.DataFrame, *, period_col: str) -> str:
    if table.empty:
        return "<p class='muted'>暂无数据</p>"
    sections: list[str] = []
    for coin, part in table.groupby("币种", sort=True):
        sections.append(
            f"<h3>{html.escape(str(coin))} {html.escape(period_col)}多空表现</h3>"
            f"<div class='scroll small-scroll'>{table_html(part.drop(columns=['币种']))}</div>"
        )
    return "".join(sections)


def main() -> None:
    trades = pd.read_csv(TRADES_CSV, encoding="utf-8-sig")
    summary = pd.read_csv(SUMMARY_CSV, encoding="utf-8-sig")
    equity = pd.read_csv(EQUITY_CSV, encoding="utf-8-sig")
    monthly_breakdown = pd.read_csv(MONTHLY_BREAKDOWN_CSV, encoding="utf-8-sig")
    yearly_breakdown = pd.read_csv(YEARLY_BREAKDOWN_CSV, encoding="utf-8-sig")

    initial_capital = float(equity["总权益"].iloc[0])
    final_equity = float(equity["总权益"].iloc[-1])
    total_pnl = final_equity - initial_capital
    total_return = total_pnl / initial_capital * 100 if initial_capital else 0.0
    drawdown_u = equity["历史峰值"] - equity["总权益"]
    max_drawdown_u = float(drawdown_u.max())
    max_drawdown_idx = drawdown_u.idxmax()
    peak_idx = equity.loc[:max_drawdown_idx, "总权益"].idxmax()
    peak_equity = float(equity.loc[peak_idx, "总权益"])
    trough_equity = float(equity.loc[max_drawdown_idx, "总权益"])
    peak_time = str(equity.loc[peak_idx, "时间"])
    trough_time = str(equity.loc[max_drawdown_idx, "时间"])
    max_drawdown_pct = max_drawdown_u / peak_equity * 100 if peak_equity else 0.0
    max_drawdown_initial_pct = max_drawdown_u / initial_capital * 100 if initial_capital else 0.0
    total_trades = int(len(trades))
    win_rate = float((trades["盈亏"] > 0).mean() * 100)
    pf = profit_factor(trades["盈亏"].astype(float))
    payoff_ratio = summary.loc[(summary["分类"] == "组合总览") & (summary["名称"] == "组合合计"), "盈亏比"].iloc[0]

    fee_total = float(trades["手续费"].sum())
    slippage_total = float(trades["滑点成本"].sum())
    pre_cost_profit = total_pnl + fee_total + slippage_total
    cost_total = fee_total + slippage_total
    cost_ratio = cost_total / abs(pre_cost_profit) * 100 if pre_cost_profit else 0.0

    side_summary = sort_by_money(summarize_group(trades, ["方向"]))
    coin_total = summarize_group(trades, ["币种"]).copy()
    coin_total = sort_by_money(coin_total)
    side_coin = sort_by_money(summarize_group(trades, ["币种", "方向"]))
    strategy_total = sort_by_money(summarize_group(trades, ["策略"]))
    strategy_coin_side = sort_by_money(summarize_group(trades, ["策略", "币种", "方向"]))

    coin_side_win = side_coin[["币种", "方向", "交易次数", "胜率", "Profit Factor", "净利润"]].copy()
    yearly_coin_side = period_side_coin_table(yearly_breakdown, period_col_name="年份")
    monthly_coin_side = period_side_coin_table(monthly_breakdown, period_col_name="月份")

    yearly_total = yearly_breakdown[(yearly_breakdown["方向"] == "合计") & (yearly_breakdown["币种"] == "合计")].copy()
    yearly_total = yearly_total[["期间", "交易次数", "胜率", "Profit Factor", "当期利润U", "累计利润U"]]
    yearly_total["当期利润U"] = yearly_total["当期利润U"].map(money)
    yearly_total["累计利润U"] = yearly_total["累计利润U"].map(money)

    yearly_side = yearly_breakdown[yearly_breakdown["币种"] == "合计"].copy()
    yearly_side = yearly_side[["期间", "方向", "交易次数", "胜率", "Profit Factor", "当期利润U", "累计利润U"]]
    yearly_side["当期利润U"] = yearly_side["当期利润U"].map(money)
    yearly_side["累计利润U"] = yearly_side["累计利润U"].map(money)

    monthly_total = monthly_breakdown[(monthly_breakdown["方向"] == "合计") & (monthly_breakdown["币种"] == "合计")].copy()
    best_months = monthly_total.sort_values("当期利润U", ascending=False).head(8)
    worst_months = monthly_total.sort_values("当期利润U", ascending=True).head(8)
    best_months = best_months[["期间", "交易次数", "胜率", "Profit Factor", "当期利润U", "累计利润U"]]
    worst_months = worst_months[["期间", "交易次数", "胜率", "Profit Factor", "当期利润U", "累计利润U"]]
    for frame in (best_months, worst_months):
        frame["当期利润U"] = frame["当期利润U"].map(money)
        frame["累计利润U"] = frame["累计利润U"].map(money)

    cost_rows = pd.DataFrame(
        [
            {"项目": "成本前利润", "金额U": round(pre_cost_profit, 2)},
            {"项目": "扣手续费", "金额U": round(-fee_total, 2)},
            {"项目": "扣滑点影响", "金额U": round(-slippage_total, 2)},
            {"项目": "最终净利润", "金额U": round(total_pnl, 2)},
        ]
    )

    charts = {
        "equity": make_equity_chart(equity),
        "drawdown": make_drawdown_chart(equity),
        "coin": make_bar_chart(
            pd.DataFrame(
                {
                    "币种": trades.groupby("币种")["盈亏"].sum().sort_values(ascending=False).index,
                    "利润U": trades.groupby("币种")["盈亏"].sum().sort_values(ascending=False).values,
                }
            ),
            label_col="币种",
            value_col="利润U",
            title="币种贡献：谁在赚钱，谁在拖累",
        ),
        "side": make_bar_chart(
            pd.DataFrame(
                {
                    "方向": trades.groupby("方向")["盈亏"].sum().sort_values(ascending=False).index,
                    "利润U": trades.groupby("方向")["盈亏"].sum().sort_values(ascending=False).values,
                }
            ),
            label_col="方向",
            value_col="利润U",
            title="多空贡献：多头和空头分别贡献多少",
        ),
        "strategy": make_bar_chart(
            pd.DataFrame(
                {
                    "策略": trades.groupby("策略")["盈亏"].sum().sort_values(ascending=False).index,
                    "利润U": trades.groupby("策略")["盈亏"].sum().sort_values(ascending=False).values,
                }
            ),
            label_col="策略",
            value_col="利润U",
            title="策略贡献：哪套参数赚钱，哪套参数拖累",
        ),
        "cost": make_cost_chart(cost_rows),
    }

    best_coin = coin_total.iloc[0]["币种"] if not coin_total.empty else ""
    worst_coin = coin_total.iloc[-1]["币种"] if not coin_total.empty else ""
    eth_short = trades[(trades["币种"] == "ETH") & (trades["方向"] == "空头")]
    risk_level = "可以继续研究，但暂不建议直接放大实盘" if max_drawdown_pct > 50 else "风险可控，仍需小资金验证"

    cards = [
        ("最终赚多少钱", money(total_pnl), f"初始 {money(initial_capital)}，最终 {money(final_equity)}"),
        ("总收益率", pct(total_return), "固定风险 100U/笔，不复利"),
        ("最大中途亏损", money(max_drawdown_u), f"从峰值权益回撤 {pct(max_drawdown_pct)}"),
        ("交易次数", f"{total_trades:,} 笔", "样本量充足，不是少数交易偶然结果"),
        ("胜率", pct(win_rate), "胜率低，靠盈亏比赚钱"),
        ("Profit Factor", f"{pf:.4f}", "大于 1 代表总体盈利"),
        ("成本前利润", money(pre_cost_profit), "扣交易成本前的理论利润"),
        ("交易成本", money(cost_total), f"约吃掉成本前利润 {pct(cost_ratio)}"),
    ]

    html_text = f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <title>最佳参数组合包 1H 回测汇报版</title>
  <style>
    body {{ margin:0; font-family:"Microsoft YaHei UI","Microsoft YaHei",Arial,sans-serif; color:#17202a; background:#f7f8fa; }}
    .wrap {{ max-width:1420px; margin:0 auto; padding:28px; }}
    .hero {{ background:#ffffff; border-bottom:4px solid #155e75; padding:26px 30px; box-shadow:0 8px 24px rgba(15,23,42,.08); }}
    h1 {{ margin:0 0 8px; font-size:30px; }}
    h2 {{ margin:0 0 14px; font-size:22px; }}
    h3 {{ margin:20px 0 10px; font-size:17px; }}
    p {{ line-height:1.72; }}
    .muted {{ color:#667085; }}
    .grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(230px,1fr)); gap:12px; margin:18px 0; }}
    .card {{ background:#fff; border:1px solid #dde3ea; border-radius:8px; padding:15px; }}
    .card b {{ display:block; font-size:13px; color:#667085; margin-bottom:8px; }}
    .value {{ font-size:24px; font-weight:800; color:#111827; }}
    .good {{ color:#15803d; font-weight:700; }}
    .bad {{ color:#b91c1c; font-weight:700; }}
    .section {{ padding:24px 30px; background:#fff; margin-top:18px; border:1px solid #dde3ea; border-radius:8px; }}
    .split {{ display:grid; grid-template-columns:1fr 1fr; gap:18px; }}
    img {{ width:100%; border:1px solid #e5e7eb; border-radius:6px; background:#fff; }}
    table {{ width:100%; border-collapse:collapse; font-size:13px; }}
    th,td {{ border:1px solid #e5e7eb; padding:8px 9px; text-align:left; }}
    th {{ background:#eef4f7; }}
    .scroll {{ overflow:auto; max-height:560px; border:1px solid #e5e7eb; }}
    .small-scroll {{ max-height:360px; margin-bottom:16px; }}
    .note {{ background:#f0f9ff; border-left:4px solid #0284c7; padding:12px 14px; margin:12px 0; }}
    .warn {{ background:#fff7ed; border-left:4px solid #f97316; padding:12px 14px; margin:12px 0; }}
  </style>
</head>
<body>
<div class="wrap">
  <div class="hero">
    <h1>多币种多空量化组合回测汇报版</h1>
    <p class="muted">口径：1小时全量数据，固定风险 100U/笔，不复利，计入手续费和 0.03% 开平仓滑点影响。报告目标是回答：赚多少钱、最大亏多少钱、谁赚钱、谁拖累、成本压力在哪里。</p>
  </div>

  <div class="grid">
    {''.join(f"<div class='card'><b>{html.escape(k)}</b><div class='value'>{html.escape(v)}</div><p class='muted'>{html.escape(t)}</p></div>" for k,v,t in cards)}
  </div>

  <div class="section">
    <h2>一句话结论</h2>
    <p>这套组合在回测中最终净赚 <span class="good">{money(total_pnl)}</span>，盈利主要来自多头趋势策略，同时空头也提供了正收益。它不是高胜率系统，胜率只有 <b>{pct(win_rate)}</b>，核心依赖较大的盈利单覆盖大量小亏单。</p>
    <p class="warn">需要给领导讲清楚：这属于“低胜率、盈亏比驱动”的量化组合。账面盈利不错，但最大回撤达到 <b>{money(max_drawdown_u)}</b>，从历史峰值权益回撤 <b>{pct(max_drawdown_pct)}</b>，所以不能只看最终利润，必须控制启动资金、杠杆和单笔风险。</p>
    <p><b>实盘价值判断：</b>{risk_level}。当前版本更适合先做小资金验证和成本敏感性跟踪，而不是马上大资金满仓复制。</p>
  </div>

  <div class="section">
    <h2>资金曲线与最大风险</h2>
    <div class="split">
      <img src="data:image/png;base64,{charts['equity']}" alt="资金曲线">
      <img src="data:image/png;base64,{charts['drawdown']}" alt="回撤曲线">
    </div>
    <div class="note">看资金曲线要先看两件事：最终是否向上，回撤是否能承受。本次最终向上，但中途回撤很大，这意味着策略“能赚钱”，不等于“任何资金规模都能舒服持有”。</div>
    <p class="muted">最大回撤发生在 {html.escape(trough_time)}。它是从 {html.escape(peak_time)} 的峰值权益 {money(peak_equity)} 回落到 {money(trough_equity)}，中间吐回 {money(max_drawdown_u)}；辅助口径看，相当于初始资金的 {pct(max_drawdown_initial_pct)}。</p>
  </div>

  <div class="section">
    <h2>手续费和滑点到底是什么意思</h2>
    <div class="split">
      <div>
        <p><b>手续费</b>是交易所收的钱，每开仓和平仓都会发生。它不看你这笔单子赚还是亏，只要成交就收。</p>
        <p><b>滑点</b>是理想成交价和真实成交价之间的差。本报告按开仓和平仓各 0.03% 估算，意思是实盘成交通常不会刚好踩在回测理想价上。</p>
        <p><b>本次成本解释：</b>策略成本前理论利润约 <b>{money(pre_cost_profit)}</b>，扣掉手续费 <b>{money(fee_total)}</b> 和滑点影响 <b>{money(slippage_total)}</b> 后，剩下净利润 <b>{money(total_pnl)}</b>。</p>
      </div>
      <img src="data:image/png;base64,{charts['cost']}" alt="成本解释">
    </div>
    <div class="warn">成本越高，说明策略边际越容易被吃掉。当前成本合计约占成本前利润 <b>{pct(cost_ratio)}</b>，这部分必须在实盘跟踪中单独盯住。</div>
  </div>

  <div class="section">
    <h2>多头赚钱还是空头赚钱</h2>
    <div class="split">
      <img src="data:image/png;base64,{charts['side']}" alt="多空贡献">
      <div>{table_html(side_summary)}</div>
    </div>
  </div>

  <div class="section">
    <h2>哪些币种赚钱，哪些币种拖累</h2>
    <div class="split">
      <img src="data:image/png;base64,{charts['coin']}" alt="币种贡献">
      <div>
        <p>最佳币种：<b>{html.escape(str(best_coin))}</b>。最弱币种：<b>{html.escape(str(worst_coin))}</b>。</p>
        <p>ETH 空头是当前明显拖累项，净利润约 <b>{money(float(eth_short['盈亏'].sum()))}</b>，后续应单独复核参数或考虑降低权重。</p>
      </div>
    </div>
    <h3>币种总览</h3>
    {table_html(coin_total)}
    <h3>多空按币种拆分</h3>
    {table_html(side_coin)}
  </div>

  <div class="section">
    <h2>横向比较：策略、币种、方向</h2>
    <p class="muted">这一段专门用来横向看谁在贡献利润、谁在消耗利润。领导看这里可以快速决定哪些策略继续保留，哪些币种或方向要降权、暂停或重新筛参。</p>
    <div class="split">
      <img src="data:image/png;base64,{charts['strategy']}" alt="策略贡献">
      <div>
        <h3>策略总览</h3>
        {table_html(strategy_total)}
      </div>
    </div>
    <h3>币种多头/空头胜率横比</h3>
    {table_html(coin_side_win)}
    <h3>策略 x 币种 x 方向明细</h3>
    <div class="scroll">{table_html(strategy_coin_side)}</div>
  </div>

  <div class="section">
    <h2>年度表现</h2>
    <p class="muted">年度表用来判断策略是不是只靠某一年赚钱。如果只有一两年赚钱，过拟合风险会更高。</p>
    {table_html(yearly_total)}
    <h3>年度多空拆分</h3>
    {table_html(yearly_side)}
    <h3>单个币种年度多头/空头表现</h3>
    <div class="scroll">{table_html(yearly_coin_side)}</div>
  </div>

  <div class="section">
    <h2>月度表现</h2>
    <div class="split">
      <div>
        <h3>最好月份</h3>
        {table_html(best_months)}
      </div>
      <div>
        <h3>最差月份</h3>
        {table_html(worst_months)}
      </div>
    </div>
    <h3>完整月度多空币种拆分</h3>
    <div class="scroll">{table_html(monthly_breakdown)}</div>
    <h3>单个币种月度多头/空头表现</h3>
    <div class="scroll">{table_html(monthly_coin_side)}</div>
    <h3>按币种展开：年度多空表现</h3>
    {period_coin_sections(yearly_coin_side, period_col="年度")}
    <h3>按币种展开：月度多空表现</h3>
    {period_coin_sections(monthly_coin_side, period_col="月度")}
  </div>

  <div class="section">
    <h2>管理层关注点</h2>
    <p><b>可以肯定的地方：</b>全量样本有 {total_trades:,} 笔，利润不是少数几笔偶然交易堆出来；多头和空头均有贡献，DOGE 空头、SOL 多头、BTC 多头是主要正贡献来源。</p>
    <p><b>必须警惕的地方：</b>最大回撤较大，且胜率较低，投资人看到连续亏损时心理压力会很大。ETH 空头表现弱，建议降低权重或重新筛参。</p>
    <p><b>下一步建议：</b>实盘前先做小资金跟踪，重点盯四个指标：真实手续费、真实滑点、连续亏损次数、ETH 空头是否继续拖累。</p>
  </div>
</div>
</body>
</html>"""

    REPORT_HTML.write_text(html_text, encoding="utf-8")
    LEADER_REPORT_HTML.write_text(html_text, encoding="utf-8")
    print(REPORT_HTML)


if __name__ == "__main__":
    main()
