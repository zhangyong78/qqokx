from __future__ import annotations

import base64
import html
import math
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
REPORT_DIR = ROOT / "reports"

SHORT_TRADES_CSV = REPORT_DIR / "r001_fixed_baseline_5coins_trades_10u.csv"
LONG_TRADES_CSV = REPORT_DIR / "dynamic_long_recommended_5coins_trades_10u.csv"

SHORT_KEY = "btc_r001_short"
LONG_KEY = "btc_dynamic_long"
COMBINED_KEY = "btc_parallel_combined"

SHORT_LABEL = "BTC EMA55斜率做空"
LONG_LABEL = "BTC动态做多(推荐参数)"
COMBINED_LABEL = "BTC多空并行合成"

MONTHLY_CSV = REPORT_DIR / "btc_long_short_parallel_monthly_10u.csv"
YEARLY_CSV = REPORT_DIR / "btc_long_short_parallel_yearly_10u.csv"
SUMMARY_CSV = REPORT_DIR / "btc_long_short_parallel_summary_10u.csv"
HTML_PATH = REPORT_DIR / "btc_long_short_parallel_summary_10u_report.html"
CHART_MONTHLY = REPORT_DIR / "btc_long_short_parallel_monthly_10u.png"
CHART_YEARLY = REPORT_DIR / "btc_long_short_parallel_yearly_10u.png"
CHART_EQUITY = REPORT_DIR / "btc_long_short_parallel_equity_10u.png"


plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "Noto Sans CJK SC", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False


def main() -> None:
    REPORT_DIR.mkdir(exist_ok=True)
    short_trades = load_btc_trades(SHORT_TRADES_CSV, SHORT_KEY, SHORT_LABEL)
    long_trades = load_btc_trades(LONG_TRADES_CSV, LONG_KEY, LONG_LABEL)
    combined_trades = build_combined(short_trades, long_trades)

    summary = pd.DataFrame(
        [
            summary_row(short_trades, SHORT_KEY, SHORT_LABEL),
            summary_row(long_trades, LONG_KEY, LONG_LABEL),
            summary_row(combined_trades, COMBINED_KEY, COMBINED_LABEL),
        ]
    )
    monthly = pd.concat(
        [
            build_period_table(short_trades, "month", SHORT_KEY, SHORT_LABEL),
            build_period_table(long_trades, "month", LONG_KEY, LONG_LABEL),
            build_period_table(combined_trades, "month", COMBINED_KEY, COMBINED_LABEL),
        ],
        ignore_index=True,
    ).sort_values(["period", "strategy_key"])
    yearly = pd.concat(
        [
            build_period_table(short_trades, "year", SHORT_KEY, SHORT_LABEL),
            build_period_table(long_trades, "year", LONG_KEY, LONG_LABEL),
            build_period_table(combined_trades, "year", COMBINED_KEY, COMBINED_LABEL),
        ],
        ignore_index=True,
    ).sort_values(["period", "strategy_key"])

    summary.to_csv(SUMMARY_CSV, index=False, encoding="utf-8-sig")
    monthly.to_csv(MONTHLY_CSV, index=False, encoding="utf-8-sig")
    yearly.to_csv(YEARLY_CSV, index=False, encoding="utf-8-sig")

    save_monthly_chart(monthly)
    save_yearly_chart(yearly)
    save_equity_chart(short_trades, long_trades, combined_trades)

    HTML_PATH.write_text(build_html(summary, monthly, yearly), encoding="utf-8")
    print(HTML_PATH)


def load_btc_trades(path: Path, strategy_key: str, strategy_label: str) -> pd.DataFrame:
    frame = pd.read_csv(path)
    frame = frame[frame["coin"] == "BTC"].copy()
    frame["exit_time"] = pd.to_datetime(frame["exit_time"], utc=True)
    frame["year"] = frame["year"].astype(str)
    frame["month"] = frame["month"].astype(str)
    frame["strategy_key"] = strategy_key
    frame["strategy_label"] = strategy_label
    return frame.sort_values("exit_ts").reset_index(drop=True)


def build_combined(short_trades: pd.DataFrame, long_trades: pd.DataFrame) -> pd.DataFrame:
    combined = pd.concat([short_trades.copy(), long_trades.copy()], ignore_index=True)
    combined["strategy_key"] = COMBINED_KEY
    combined["strategy_label"] = COMBINED_LABEL
    return combined.sort_values("exit_ts").reset_index(drop=True)


def summary_row(trades: pd.DataFrame, strategy_key: str, strategy_label: str) -> dict[str, object]:
    return {"strategy_key": strategy_key, "strategy_label": strategy_label, **metrics(trades)}


def build_period_table(trades: pd.DataFrame, period_col: str, strategy_key: str, strategy_label: str) -> pd.DataFrame:
    grouped = (
        trades.groupby(period_col, as_index=False)
        .agg(
            trades=("pnl_u", "size"),
            total_pnl_u=("pnl_u", "sum"),
            win_rate=("pnl_u", lambda s: float((s > 0).mean())),
            avg_r=("r_multiple", "mean"),
            avg_hold_hours=("hold_hours", "mean"),
            gross_profit=("pnl_u", lambda s: float(s[s > 0].sum())),
            gross_loss=("pnl_u", lambda s: float(s[s <= 0].sum())),
            big_win_3r_count=("r_multiple", lambda s: float((s >= 3.0).sum())),
            big_win_5r_count=("r_multiple", lambda s: float((s >= 5.0).sum())),
        )
    )
    grouped["profit_factor"] = grouped.apply(
        lambda row: row["gross_profit"] / abs(row["gross_loss"]) if row["gross_loss"] < 0 else 0.0,
        axis=1,
    )
    grouped = grouped.rename(columns={period_col: "period"})
    grouped["period"] = grouped["period"].astype(str)
    grouped.insert(0, "strategy_key", strategy_key)
    grouped.insert(1, "strategy_label", strategy_label)
    return grouped


def metrics(trades: pd.DataFrame) -> dict[str, float]:
    if trades.empty:
        return {
            "trades": 0.0,
            "total_pnl_u": 0.0,
            "profit_factor": 0.0,
            "win_rate": 0.0,
            "avg_r": 0.0,
            "avg_hold_hours": 0.0,
            "max_drawdown_u": 0.0,
            "return_drawdown": 0.0,
            "big_win_3r_count": 0.0,
            "big_win_5r_count": 0.0,
        }
    pnls = trades["pnl_u"].astype(float)
    gross_profit = float(pnls[pnls > 0].sum())
    gross_loss = float(pnls[pnls <= 0].sum())
    curve = pnls.cumsum()
    drawdown = float((curve.cummax() - curve).max())
    total = float(pnls.sum())
    return {
        "trades": float(len(trades)),
        "total_pnl_u": total,
        "profit_factor": gross_profit / abs(gross_loss) if gross_loss < 0 else 0.0,
        "win_rate": float((pnls > 0).mean()),
        "avg_r": float(trades["r_multiple"].astype(float).mean()),
        "avg_hold_hours": float(trades["hold_hours"].astype(float).mean()),
        "max_drawdown_u": drawdown,
        "return_drawdown": total / drawdown if drawdown > 0 else 0.0,
        "big_win_3r_count": float((trades["r_multiple"].astype(float) >= 3.0).sum()),
        "big_win_5r_count": float((trades["r_multiple"].astype(float) >= 5.0).sum()),
    }


def save_monthly_chart(monthly: pd.DataFrame) -> None:
    pivot = monthly.pivot(index="period", columns="strategy_label", values="total_pnl_u").sort_index()
    ax = pivot.plot(kind="bar", figsize=(14, 4.8), width=0.84, color=["#1746a2", "#d97706", "#15803d"])
    ax.axhline(0, color="#334155", linewidth=0.8)
    ax.set_title("BTC 月度盈亏：做空 / 做多 / 合成")
    ax.set_xlabel("")
    ax.set_ylabel("月度盈亏(U)")
    ax.grid(axis="y", alpha=0.22)
    plt.xticks(rotation=60, ha="right")
    plt.tight_layout()
    plt.savefig(CHART_MONTHLY, dpi=160)
    plt.close()


def save_yearly_chart(yearly: pd.DataFrame) -> None:
    pivot = yearly.pivot(index="period", columns="strategy_label", values="total_pnl_u").sort_index()
    ax = pivot.plot(kind="bar", figsize=(9, 4.8), width=0.8, color=["#1746a2", "#d97706", "#15803d"])
    ax.axhline(0, color="#334155", linewidth=0.8)
    ax.set_title("BTC 年度盈亏：做空 / 做多 / 合成")
    ax.set_xlabel("")
    ax.set_ylabel("年度盈亏(U)")
    ax.grid(axis="y", alpha=0.22)
    plt.tight_layout()
    plt.savefig(CHART_YEARLY, dpi=160)
    plt.close()


def save_equity_chart(short_trades: pd.DataFrame, long_trades: pd.DataFrame, combined_trades: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(10.5, 5.2))
    for trades, label, color in [
        (short_trades, SHORT_LABEL, "#1746a2"),
        (long_trades, LONG_LABEL, "#d97706"),
        (combined_trades, COMBINED_LABEL, "#15803d"),
    ]:
        curve = trades[["exit_time", "pnl_u"]].copy()
        curve["equity_u"] = curve["pnl_u"].astype(float).cumsum()
        ax.plot(curve["exit_time"], curve["equity_u"], label=label, linewidth=2, color=color)
    ax.axhline(0, color="#334155", linewidth=0.8)
    ax.set_title("BTC 累计收益曲线：做空 / 做多 / 合成")
    ax.set_xlabel("")
    ax.set_ylabel("累计收益(U)")
    ax.grid(alpha=0.22)
    ax.legend()
    plt.tight_layout()
    plt.savefig(CHART_EQUITY, dpi=160)
    plt.close()


def build_html(summary: pd.DataFrame, monthly: pd.DataFrame, yearly: pd.DataFrame) -> str:
    short_total = float(summary.loc[summary["strategy_key"] == SHORT_KEY, "total_pnl_u"].iloc[0])
    long_total = float(summary.loc[summary["strategy_key"] == LONG_KEY, "total_pnl_u"].iloc[0])
    combined_total = float(summary.loc[summary["strategy_key"] == COMBINED_KEY, "total_pnl_u"].iloc[0])
    combined_dd = float(summary.loc[summary["strategy_key"] == COMBINED_KEY, "max_drawdown_u"].iloc[0])
    combined_pf = float(summary.loc[summary["strategy_key"] == COMBINED_KEY, "profit_factor"].iloc[0])
    best_year = pick_best_period(yearly, COMBINED_KEY)
    worst_year = pick_worst_period(yearly, COMBINED_KEY)

    summary_rows = "".join(summary_row_html(row) for row in summary.to_dict("records"))
    monthly_rows = "".join(period_row_html(row) for row in monthly.to_dict("records"))
    yearly_rows = "".join(period_row_html(row) for row in yearly.to_dict("records"))

    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <title>BTC 多空平行对比综合报告</title>
  <style>
    :root {{
      --ink:#172033; --muted:#64748b; --line:#e2e8f0; --blue:#1746a2; --orange:#d97706; --green:#15803d; --red:#b91c1c;
    }}
    * {{ box-sizing:border-box; }}
    body {{ margin:0; font-family:"Microsoft YaHei","Noto Sans CJK SC",sans-serif; color:var(--ink);
      background:radial-gradient(circle at top left,#e0f2fe 0,#fffaf2 36%,#f8fafc 100%); }}
    header {{ padding:36px 42px 18px; }}
    h1 {{ margin:0 0 10px; font-size:30px; }}
    h2 {{ margin:26px 0 12px; font-size:21px; }}
    p {{ line-height:1.75; }}
    .sub {{ color:var(--muted); }}
    .wrap {{ padding:0 42px 42px; }}
    .cards {{ display:grid; grid-template-columns:repeat(5,minmax(0,1fr)); gap:14px; margin:18px 0 20px; }}
    .card, .panel {{ background:rgba(255,255,255,.92); border:1px solid var(--line); border-radius:20px; padding:18px; box-shadow:0 16px 42px rgba(15,23,42,.07); }}
    .k {{ color:var(--muted); font-size:13px; }}
    .v {{ margin-top:8px; font-size:24px; font-weight:800; }}
    .grid {{ display:grid; grid-template-columns:1fr 1fr; gap:18px; }}
    img {{ width:100%; border-radius:16px; border:1px solid var(--line); background:white; }}
    table {{ width:100%; border-collapse:collapse; background:white; border-radius:16px; overflow:hidden; }}
    th,td {{ padding:10px 9px; border-bottom:1px solid var(--line); text-align:right; font-size:13px; white-space:nowrap; }}
    th:first-child,td:first-child,th:nth-child(2),td:nth-child(2) {{ text-align:left; }}
    th {{ background:#f1f5f9; color:#334155; position:sticky; top:0; }}
    .good {{ color:var(--green); font-weight:700; }}
    .bad {{ color:var(--red); font-weight:700; }}
    .table-wrap {{ max-height:520px; overflow:auto; border-radius:16px; border:1px solid var(--line); }}
    .note {{ padding:14px 16px; border-left:5px solid var(--orange); background:#fffbeb; border-radius:12px; }}
    @media (max-width: 900px) {{ .cards,.grid {{ grid-template-columns:1fr; }} header,.wrap {{ padding-left:18px; padding-right:18px; }} }}
  </style>
</head>
<body>
  <header>
    <h1>BTC 多空平行对比 + 综合统计</h1>
    <p class="sub">做空使用当前 EMA55 斜率做空；做多使用此前推荐的动态委托做多参数。这里不是五币，而是只统计 BTC，并把两条系统当作平行策略，再做一层合成统计。</p>
  </header>
  <main class="wrap">
    <section class="cards">
      <div class="card"><div class="k">做空总收益</div><div class="v">{fmt(short_total)}U</div></div>
      <div class="card"><div class="k">做多总收益</div><div class="v">{fmt(long_total)}U</div></div>
      <div class="card"><div class="k">多空合成总收益</div><div class="v">{fmt(combined_total)}U</div></div>
      <div class="card"><div class="k">合成 PF</div><div class="v">{fmt(combined_pf, 3)}</div></div>
      <div class="card"><div class="k">合成最大回撤</div><div class="v">{fmt(combined_dd)}U</div></div>
    </section>

    <section class="grid">
      <div class="panel"><img src="data:image/png;base64,{image_b64(CHART_MONTHLY)}" alt="月度对比" /></div>
      <div class="panel"><img src="data:image/png;base64,{image_b64(CHART_YEARLY)}" alt="年度对比" /></div>
    </section>
    <section class="panel">
      <img src="data:image/png;base64,{image_b64(CHART_EQUITY)}" alt="累计收益曲线" />
    </section>

    <section class="panel">
      <h2>综合判断</h2>
      <p>这份报告最核心的是看 BTC 上多空两条系统是不是互补。单看总收益，做空基线是 <b>{fmt(short_total)}U</b>，动态做多是 <b>{fmt(long_total)}U</b>，两条平行叠加后的合成收益是 <b>{fmt(combined_total)}U</b>。</p>
      <p>从年度看，合成系统最佳年份是 <b>{best_year[0]}</b>，收益 <b>{fmt(best_year[1])}U</b>；最差年份是 <b>{worst_year[0]}</b>，收益 <b>{fmt(worst_year[1])}U</b>。这能帮我们判断：BTC 上到底是单做空更纯粹，还是多空并行更平滑。</p>
      <p class="note">这里的“合成”是把做多和做空视为两条平行独立策略，按同样 10U 风险分别运行，再把收益流合并统计。它反映的是组合层面的风格互补，不代表同一时刻只允许单方向持仓。</p>
    </section>

    <section class="panel">
      <h2>综合指标</h2>
      <div class="table-wrap">
        <table>
          <thead><tr><th>策略</th><th>交易数</th><th>总收益U</th><th>PF</th><th>胜率</th><th>平均R</th><th>平均持仓h</th><th>最大回撤U</th><th>收益/回撤</th><th>3R+</th><th>5R+</th></tr></thead>
          <tbody>{summary_rows}</tbody>
        </table>
      </div>
    </section>

    <section class="panel">
      <h2>月度明细</h2>
      <div class="table-wrap">
        <table>
          <thead><tr><th>策略</th><th>期间</th><th>交易数</th><th>盈亏U</th><th>PF</th><th>胜率</th><th>平均R</th><th>平均持仓h</th><th>3R+</th><th>5R+</th></tr></thead>
          <tbody>{monthly_rows}</tbody>
        </table>
      </div>
    </section>

    <section class="panel">
      <h2>年度明细</h2>
      <div class="table-wrap">
        <table>
          <thead><tr><th>策略</th><th>期间</th><th>交易数</th><th>盈亏U</th><th>PF</th><th>胜率</th><th>平均R</th><th>平均持仓h</th><th>3R+</th><th>5R+</th></tr></thead>
          <tbody>{yearly_rows}</tbody>
        </table>
      </div>
    </section>

    <section class="panel">
      <h2>文件输出</h2>
      <p>综合指标 CSV：{html.escape(str(SUMMARY_CSV))}</p>
      <p>月度 CSV：{html.escape(str(MONTHLY_CSV))}</p>
      <p>年度 CSV：{html.escape(str(YEARLY_CSV))}</p>
    </section>
  </main>
</body>
</html>"""


def pick_best_period(period_df: pd.DataFrame, strategy_key: str) -> tuple[str, float]:
    subset = period_df[period_df["strategy_key"] == strategy_key].copy()
    row = subset.loc[subset["total_pnl_u"].idxmax()]
    return str(row["period"]), float(row["total_pnl_u"])


def pick_worst_period(period_df: pd.DataFrame, strategy_key: str) -> tuple[str, float]:
    subset = period_df[period_df["strategy_key"] == strategy_key].copy()
    row = subset.loc[subset["total_pnl_u"].idxmin()]
    return str(row["period"]), float(row["total_pnl_u"])


def summary_row_html(row: dict[str, object]) -> str:
    pnl = float(row["total_pnl_u"])
    return (
        "<tr>"
        f"<td>{html.escape(str(row['strategy_label']))}</td>"
        f"<td>{int(row['trades'])}</td>"
        f"<td class=\"{cls(pnl)}\">{fmt(pnl)}</td>"
        f"<td>{fmt(row['profit_factor'], 3)}</td>"
        f"<td>{pct(row['win_rate'])}</td>"
        f"<td class=\"{cls(float(row['avg_r']))}\">{fmt(row['avg_r'], 3)}</td>"
        f"<td>{fmt(row['avg_hold_hours'], 1)}</td>"
        f"<td>{fmt(row['max_drawdown_u'])}</td>"
        f"<td>{fmt(row['return_drawdown'], 2)}</td>"
        f"<td>{int(row['big_win_3r_count'])}</td>"
        f"<td>{int(row['big_win_5r_count'])}</td>"
        "</tr>"
    )


def period_row_html(row: dict[str, object]) -> str:
    pnl = float(row["total_pnl_u"])
    return (
        "<tr>"
        f"<td>{html.escape(str(row['strategy_label']))}</td>"
        f"<td>{html.escape(str(row['period']))}</td>"
        f"<td>{int(row['trades'])}</td>"
        f"<td class=\"{cls(pnl)}\">{fmt(pnl)}</td>"
        f"<td>{fmt(row['profit_factor'], 3)}</td>"
        f"<td>{pct(row['win_rate'])}</td>"
        f"<td class=\"{cls(float(row['avg_r']))}\">{fmt(row['avg_r'], 3)}</td>"
        f"<td>{fmt(row['avg_hold_hours'], 1)}</td>"
        f"<td>{int(row['big_win_3r_count'])}</td>"
        f"<td>{int(row['big_win_5r_count'])}</td>"
        "</tr>"
    )


def image_b64(path: Path) -> str:
    return base64.b64encode(path.read_bytes()).decode("ascii")


def fmt(value: object, digits: int = 1) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "-"
    if math.isnan(number):
        return "-"
    return f"{number:,.{digits}f}"


def pct(value: object) -> str:
    try:
        return f"{float(value) * 100:.1f}%"
    except (TypeError, ValueError):
        return "-"


def cls(value: float) -> str:
    return "good" if value > 0 else "bad" if value < 0 else ""


if __name__ == "__main__":
    main()
