from __future__ import annotations

import base64
import json
import shutil
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
REPORT_DIR = ROOT / "reports"
SOURCE_TRADES = REPORT_DIR / "short_strategy_15m_trades.csv"
SOURCE_BEST = REPORT_DIR / "short_strategy_15m_best.json"
SOURCE_SHORT_HTML = REPORT_DIR / "short_strategy_15m_followup_report.html"
ARCHIVE_SHORT_HTML = REPORT_DIR / "short_strategy_15m_followup_short_snapshot.html"
OUT = REPORT_DIR / "short_strategy_15m_counterparty_report.html"
CURRENT_HTML = REPORT_DIR / "short_strategy_15m_followup_report.html"
INITIAL_EQUITY = 100_000.0
NORMALIZED_INITIAL_CAPITAL = 10_000.0
NORMALIZED_FIXED_RISK = 10.0


def main() -> None:
    if SOURCE_SHORT_HTML.exists() and not ARCHIVE_SHORT_HTML.exists():
        shutil.copy2(SOURCE_SHORT_HTML, ARCHIVE_SHORT_HTML)

    trades = pd.read_csv(SOURCE_TRADES)
    best = json.loads(SOURCE_BEST.read_text(encoding="utf-8"))
    counterparty = build_counterparty_trades(trades)
    exact_metrics = calc_exact_metrics(counterparty)
    normalized_book = build_normalized_book(counterparty)
    yearly_exact = summarize_period(counterparty, "Y", "net_pnl_counterparty", INITIAL_EQUITY)
    monthly_exact = summarize_period(counterparty, "M", "net_pnl_counterparty", INITIAL_EQUITY).tail(24)
    yearly_norm = summarize_period(counterparty, "Y", "normalized_pnl", NORMALIZED_INITIAL_CAPITAL)
    monthly_norm = summarize_period(counterparty, "M", "normalized_pnl", NORMALIZED_INITIAL_CAPITAL).tail(24)

    exact_equity = build_equity(counterparty["exit_time"], counterparty["net_pnl_counterparty"], INITIAL_EQUITY)
    norm_equity = build_equity(counterparty["exit_time"], counterparty["normalized_pnl"], NORMALIZED_INITIAL_CAPITAL)
    save_line_plot(exact_equity, REPORT_DIR / "short_strategy_15m_counterparty_exact_equity.png", "15m Counterparty Exact Equity")
    save_drawdown_plot(exact_equity, REPORT_DIR / "short_strategy_15m_counterparty_exact_drawdown.png", "15m Counterparty Exact Drawdown")
    save_line_plot(norm_equity, REPORT_DIR / "short_strategy_15m_counterparty_normalized_equity.png", "15m Counterparty Normalized Equity")
    save_drawdown_plot(norm_equity, REPORT_DIR / "short_strategy_15m_counterparty_normalized_drawdown.png", "15m Counterparty Normalized Drawdown")

    html = build_html(
        best=best,
        counterparty=counterparty,
        exact_metrics=exact_metrics,
        normalized_book=normalized_book,
        yearly_exact=yearly_exact,
        monthly_exact=monthly_exact,
        yearly_norm=yearly_norm,
        monthly_norm=monthly_norm,
    )
    OUT.write_text(html, encoding="utf-8")
    CURRENT_HTML.write_text(html, encoding="utf-8")
    print(OUT)


def build_counterparty_trades(trades: pd.DataFrame) -> pd.DataFrame:
    frame = trades.copy()
    frame["entry_time"] = pd.to_datetime(frame["entry_time"], utc=True)
    frame["exit_time"] = pd.to_datetime(frame["exit_time"], utc=True)
    frame["gross_pnl"] = frame["gross_pnl"].astype(float)
    frame["cost"] = frame["cost"].astype(float)
    frame["net_pnl"] = frame["net_pnl"].astype(float)
    frame["return_pct"] = frame["return_pct"].astype(float)
    frame["net_r"] = frame["net_r"].astype(float)
    frame["gross_r"] = frame["gross_r"].astype(float)

    # 对手盘：同样的成交、同样的成本，只是站在单子的另一边。
    frame["gross_pnl_counterparty"] = -frame["gross_pnl"]
    frame["net_pnl_counterparty"] = frame["gross_pnl_counterparty"] - frame["cost"]
    frame["gross_r_counterparty"] = -frame["gross_r"]

    risk_amount = frame["net_pnl"] / frame["net_r"].replace(0, np.nan)
    frame["risk_amount_proxy"] = risk_amount.abs()
    frame["net_r_counterparty"] = frame["net_pnl_counterparty"] / frame["risk_amount_proxy"]
    frame["normalized_pnl"] = frame["net_r_counterparty"] * NORMALIZED_FIXED_RISK
    frame["win_counterparty"] = frame["net_pnl_counterparty"] > 0
    frame["loss_counterparty"] = frame["net_pnl_counterparty"] <= 0
    return frame


def calc_exact_metrics(counterparty: pd.DataFrame) -> dict[str, float]:
    pnls = counterparty["net_pnl_counterparty"].to_numpy(dtype=float)
    equity = INITIAL_EQUITY + np.cumsum(pnls)
    peaks = np.maximum.accumulate(np.r_[INITIAL_EQUITY, equity])[:-1]
    drawdown = equity / peaks - 1
    wins = pnls[pnls > 0]
    losses = pnls[pnls <= 0]
    return {
        "total_trades": int(len(counterparty)),
        "win_count": int((pnls > 0).sum()),
        "loss_count": int((pnls <= 0).sum()),
        "win_rate": float((pnls > 0).mean()) if len(pnls) else 0.0,
        "total_pnl": float(pnls.sum()),
        "final_equity": float(INITIAL_EQUITY + pnls.sum()),
        "total_return": float(pnls.sum() / INITIAL_EQUITY),
        "max_drawdown": float(drawdown.min()) if len(drawdown) else 0.0,
        "avg_pnl": float(pnls.mean()) if len(pnls) else 0.0,
        "profit_factor": float(wins.sum() / abs(losses.sum())) if losses.sum() < 0 else 999.0,
        "avg_win_loss": float(wins.mean() / abs(losses.mean())) if len(wins) and len(losses) else 0.0,
    }


def build_normalized_book(counterparty: pd.DataFrame) -> dict[str, float]:
    pnls = counterparty["normalized_pnl"].to_numpy(dtype=float)
    equity = NORMALIZED_INITIAL_CAPITAL + np.cumsum(pnls)
    peaks = np.maximum.accumulate(np.r_[NORMALIZED_INITIAL_CAPITAL, equity])[:-1]
    drawdown = equity / peaks - 1
    return {
        "initial_capital": NORMALIZED_INITIAL_CAPITAL,
        "fixed_risk": NORMALIZED_FIXED_RISK,
        "final_capital": float(NORMALIZED_INITIAL_CAPITAL + pnls.sum()),
        "total_pnl": float(pnls.sum()),
        "total_return": float(pnls.sum() / NORMALIZED_INITIAL_CAPITAL),
        "max_drawdown": float(drawdown.min()) if len(drawdown) else 0.0,
        "avg_pnl": float(pnls.mean()) if len(pnls) else 0.0,
    }


def summarize_period(counterparty: pd.DataFrame, freq: str, pnl_col: str, initial_capital: float) -> pd.DataFrame:
    label = "year" if freq == "Y" else "month"
    frame = counterparty.copy()
    frame["period"] = frame["exit_time"].dt.tz_convert(None).dt.to_period(freq).astype(str)
    out = (
        frame.groupby("period", as_index=False)
        .agg(
            trades=(pnl_col, "count"),
            wins=(pnl_col, lambda s: int((s > 0).sum())),
            losses=(pnl_col, lambda s: int((s <= 0).sum())),
            pnl=(pnl_col, "sum"),
            avg_pnl=(pnl_col, "mean"),
            win_rate=(pnl_col, lambda s: float((s > 0).mean())),
        )
        .sort_values("period")
    )
    out["end_capital"] = initial_capital + out["pnl"].cumsum()
    out = out.rename(columns={"period": label})
    return out


def build_equity(times: pd.Series, pnls: pd.Series, initial_capital: float) -> pd.DataFrame:
    return pd.DataFrame({"timestamp": pd.to_datetime(times), "equity": initial_capital + pnls.cumsum()})


def save_line_plot(frame: pd.DataFrame, path: Path, title: str) -> None:
    plt.figure(figsize=(12, 5))
    plt.plot(frame["timestamp"], frame["equity"], color="#0f766e")
    plt.title(title)
    plt.tight_layout()
    plt.savefig(path, dpi=150)
    plt.close()


def save_drawdown_plot(frame: pd.DataFrame, path: Path, title: str) -> None:
    out = frame.copy()
    out["peak"] = out["equity"].cummax()
    out["drawdown"] = out["equity"] / out["peak"] - 1
    plt.figure(figsize=(12, 4))
    plt.fill_between(out["timestamp"], out["drawdown"] * 100, color="#dc2626", alpha=0.35)
    plt.title(title)
    plt.tight_layout()
    plt.savefig(path, dpi=150)
    plt.close()


def build_html(
    *,
    best: dict[str, object],
    counterparty: pd.DataFrame,
    exact_metrics: dict[str, float],
    normalized_book: dict[str, float],
    yearly_exact: pd.DataFrame,
    monthly_exact: pd.DataFrame,
    yearly_norm: pd.DataFrame,
    monthly_norm: pd.DataFrame,
) -> str:
    original = best["best_metrics"]["all"]
    compare_rows = pd.DataFrame(
        [
            {
                "strategy": "原15m做空",
                "trades": int(original["trade_count"]),
                "win_rate": float(original["win_rate"]),
                "profit_factor": float(original["profit_factor"]),
                "total_return": float(original["total_return"]),
            },
            {
                "strategy": "对手盘反向",
                "trades": exact_metrics["total_trades"],
                "win_rate": exact_metrics["win_rate"],
                "profit_factor": exact_metrics["profit_factor"],
                "total_return": exact_metrics["total_return"],
            },
        ]
    )

    conclusion = "能盈利" if exact_metrics["total_pnl"] > 0 else "不能盈利"
    html = f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>BTC 15m 对手盘回测报告</title>
<style>
:root {{
  --ink:#172033; --muted:#667085; --line:#d7dce5; --bg:#f4f7fb; --panel:#fff;
  --good:#0f9f6e; --blue:#1d4ed8; --amber:#b45309; --red:#b42318;
}}
* {{ box-sizing:border-box; }}
body {{ margin:0; background:var(--bg); color:var(--ink); font-family:"Microsoft YaHei","Segoe UI",Arial,sans-serif; }}
.hero {{ background:linear-gradient(135deg,#121a2b 0%,#204064 58%,#2a6174 100%); color:#fff; padding:36px 40px; }}
.hero h1 {{ margin:0 0 10px; font-size:30px; }}
.hero p {{ margin:6px 0; color:#d7e5f5; max-width:1100px; line-height:1.65; }}
.wrap {{ max-width:1220px; margin:0 auto; padding:24px; }}
.grid {{ display:grid; gap:16px; }}
.grid-4 {{ grid-template-columns:repeat(4,minmax(0,1fr)); }}
.grid-3 {{ grid-template-columns:repeat(3,minmax(0,1fr)); }}
.grid-2 {{ grid-template-columns:repeat(2,minmax(0,1fr)); }}
.card {{ background:var(--panel); border:1px solid var(--line); border-radius:8px; padding:18px; box-shadow:0 1px 2px rgba(16,24,40,.04); }}
.kpi .label {{ color:var(--muted); font-size:13px; margin-bottom:8px; }}
.kpi .value {{ font-size:28px; font-weight:800; }}
.kpi .sub {{ color:var(--muted); margin-top:6px; font-size:13px; }}
h2 {{ font-size:21px; margin:30px 0 14px; }}
h3 {{ font-size:16px; margin:0 0 10px; }}
table {{ width:100%; border-collapse:collapse; font-size:13px; }}
th,td {{ border-bottom:1px solid var(--line); padding:9px 10px; text-align:right; }}
th:first-child,td:first-child {{ text-align:left; }}
th {{ background:#f8fafc; color:#475467; }}
.note {{ color:var(--muted); line-height:1.65; }}
.callout {{ border-left:5px solid var(--amber); background:#fffbeb; border-radius:6px; padding:14px 16px; line-height:1.7; }}
.imgbox img {{ width:100%; display:block; border:1px solid var(--line); border-radius:6px; }}
pre {{ background:#0f172a; color:#e5edf6; padding:14px; border-radius:8px; overflow:auto; }}
@media (max-width: 920px) {{
  .grid-4,.grid-3,.grid-2 {{ grid-template-columns:1fr; }}
  .hero {{ padding:26px 22px; }}
  .wrap {{ padding:18px 14px 32px; }}
}}
</style>
</head>
<body>
<section class="hero">
  <h1>BTC 15分钟 对手盘逻辑回测</h1>
  <p>问题很直接：既然这套 15m 做空逻辑全样本亏了 242 次、只赢了 119 次，那如果站在它的对手盘，也就是每次都接这笔单的另一边，最后能不能赚钱？</p>
  <p>这里我没有重新发明新的多头策略，而是做了更严格的检验：同样的进场、同样的出场、同样的手续费和滑点，只把方向翻到单子的另一边。</p>
</section>

<main class="wrap">
  <div class="grid grid-4">
    {kpi("结论", conclusion, "按这套对手盘定义")}
    {kpi("总交易次数", str(int(exact_metrics['total_trades'])), f"赚钱 {exact_metrics['win_count']} / 亏钱 {exact_metrics['loss_count']}")}
    {kpi("最终资金", money(exact_metrics['final_equity']), f"初始 {money(INITIAL_EQUITY)}")}
    {kpi("总收益率", pct(exact_metrics['total_return']), f"PF {exact_metrics['profit_factor']:.2f}")}
  </div>

  <h2>一句话判断</h2>
  <div class="card">
    <p>对手盘 <strong>{conclusion}</strong>。如果按这套 15m 做空策略的每一笔成交，反过来站在另一边去做，最终资金会变成 <strong>{money(exact_metrics['final_equity'])}</strong>，累计盈亏 <strong>{money(exact_metrics['total_pnl'])}</strong>。</p>
    <p class="note">这不是在做一个新策略，而是在问“这批单子的另一边值不值得做”。这个口径最适合验证“输赢方向是否真的反过来就能变成 edge”。</p>
  </div>

  <h2>原策略 vs 对手盘</h2>
  <div class="card">{render_table(compare_rows, ["strategy", "trades", "win_rate", "profit_factor", "total_return"])}</div>

  <h2>对手盘核心结果</h2>
  <div class="grid grid-3">
    <div class="card">
      <h3>精确成交口径</h3>
      <p>按原策略的真实成交数量、真实出场点、真实成本，初始资金 <strong>{money(INITIAL_EQUITY)}</strong>，最后剩余 <strong>{money(exact_metrics['final_equity'])}</strong>。</p>
    </div>
    <div class="card">
      <h3>胜率</h3>
      <p>赚钱 <strong>{exact_metrics['win_count']}</strong> 次，亏钱 <strong>{exact_metrics['loss_count']}</strong> 次，胜率 <strong>{pct(exact_metrics['win_rate'])}</strong>。</p>
    </div>
    <div class="card">
      <h3>质量</h3>
      <p>Profit Factor <strong>{exact_metrics['profit_factor']:.2f}</strong>，平均每笔 <strong>{money(exact_metrics['avg_pnl'])}</strong>，最大回撤 <strong>{pct(exact_metrics['max_drawdown'])}</strong>。</p>
    </div>
  </div>

  <h2>按你熟悉的 10U 风险口径重算</h2>
  <div class="grid grid-4">
    {kpi("起始资金", money(normalized_book['initial_capital']), "按 10000 记账")}
    {kpi("每笔风险", money(normalized_book['fixed_risk']), "固定 10U")}
    {kpi("最后剩余资金", money(normalized_book['final_capital']), f"累计盈亏 {money(normalized_book['total_pnl'])}")}
    {kpi("资金回报率", pct(normalized_book['total_return']), f"最大回撤 {pct(normalized_book['max_drawdown'])}")}
  </div>

  <h2>年度统计</h2>
  <div class="grid grid-2">
    <div class="card">
      <h3>精确成交口径</h3>
      {render_table(yearly_exact, ["year", "trades", "wins", "losses", "win_rate", "pnl", "end_capital"])}
    </div>
    <div class="card">
      <h3>10U 风险口径</h3>
      {render_table(yearly_norm, ["year", "trades", "wins", "losses", "win_rate", "pnl", "end_capital"])}
    </div>
  </div>

  <h2>最近24个月月度统计</h2>
  <div class="grid grid-2">
    <div class="card">
      <h3>精确成交口径</h3>
      {render_table(monthly_exact, ["month", "trades", "wins", "losses", "win_rate", "pnl", "end_capital"])}
    </div>
    <div class="card">
      <h3>10U 风险口径</h3>
      {render_table(monthly_norm, ["month", "trades", "wins", "losses", "win_rate", "pnl", "end_capital"])}
    </div>
  </div>

  <h2>曲线</h2>
  <div class="grid grid-2">
    <div class="card imgbox"><h3>精确成交资金曲线</h3><img src="data:image/png;base64,{b64(REPORT_DIR / 'short_strategy_15m_counterparty_exact_equity.png')}" alt="counterparty exact equity"></div>
    <div class="card imgbox"><h3>精确成交回撤</h3><img src="data:image/png;base64,{b64(REPORT_DIR / 'short_strategy_15m_counterparty_exact_drawdown.png')}" alt="counterparty exact drawdown"></div>
    <div class="card imgbox"><h3>10U 风险资金曲线</h3><img src="data:image/png;base64,{b64(REPORT_DIR / 'short_strategy_15m_counterparty_normalized_equity.png')}" alt="counterparty normalized equity"></div>
    <div class="card imgbox"><h3>10U 风险回撤</h3><img src="data:image/png;base64,{b64(REPORT_DIR / 'short_strategy_15m_counterparty_normalized_drawdown.png')}" alt="counterparty normalized drawdown"></div>
  </div>

  <h2>最佳空头版本原始信息</h2>
  <div class="card"><pre>{json.dumps(best, ensure_ascii=False, indent=2)}</pre></div>

  <h2>最后解释</h2>
  <div class="callout">
    做空策略亏得多，不代表它的对手盘一定能稳定赚钱。原因很简单：原策略虽然输了很多次，但盈利单的赔率比亏损单大；而对手盘接过去之后，等于是把“高赔率少胜”翻成了“低赔率多胜”，再叠加同样的手续费，未必就有优势。
    这次报告已经把这个问题单独算清楚了。
  </div>
</main>
</body>
</html>"""
    return html


def render_table(frame: pd.DataFrame, columns: list[str]) -> str:
    subset = frame[columns].copy()
    parts = ["<table><tr>" + "".join(f"<th>{col}</th>" for col in columns) + "</tr>"]
    for _, row in subset.iterrows():
        cells: list[str] = []
        for col in columns:
            value = row[col]
            if isinstance(value, (float, np.floating)):
                if "rate" in col or "return" in col:
                    text = pct(value)
                elif "profit_factor" in col:
                    text = f"{value:.2f}"
                else:
                    text = money(value)
            else:
                text = str(value)
            cells.append(f"<td>{text}</td>")
        parts.append("<tr>" + "".join(cells) + "</tr>")
    parts.append("</table>")
    return "".join(parts)


def money(value: float) -> str:
    return f"{float(value):,.2f}"


def pct(value: float) -> str:
    return f"{float(value) * 100:.1f}%"


def kpi(label: str, value: str, sub: str) -> str:
    return f'<div class="card kpi"><div class="label">{label}</div><div class="value">{value}</div><div class="sub">{sub}</div></div>'


def b64(path: Path) -> str:
    return base64.b64encode(path.read_bytes()).decode("ascii")


if __name__ == "__main__":
    main()
