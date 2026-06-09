from __future__ import annotations

import base64
import html
import io
import json
import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import scripts.run_best_parameter_bundle_1h_standard_portfolio as base
from okx_quant.okx_client import OkxRestClient
from okx_quant.strategy_profiles import read_strategy_bundle


plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "Noto Sans CJK SC", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False


INITIAL_CAPITAL = Decimal("10000")
FIXED_RISK_AMOUNT = Decimal("100")

LONG_DIR = ROOT / "reports" / "best_parameter_bundle_1h_long_only_100u"
SHORT_DIR = ROOT / "reports" / "best_parameter_bundle_1h_short_only_100u"


def main() -> None:
    bundle = read_strategy_bundle(base.PACKAGE_PATH)
    client = OkxRestClient()
    candidates, data_ranges, assumptions = base.build_candidate_trades(
        bundle_path=base.PACKAGE_PATH,
        client=client,
        bundle=bundle,
        base_initial_capital=INITIAL_CAPITAL,
        base_risk_amount=FIXED_RISK_AMOUNT,
    )
    common_assumptions = {
        **assumptions,
        "standard_mode": "100U固定风险",
        "initial_capital": str(INITIAL_CAPITAL),
        "risk_amount": str(FIXED_RISK_AMOUNT),
        "constraints_enabled": False,
        "compounding": False,
    }
    run_side_report(
        side_label="多头",
        output_dir=LONG_DIR,
        candidates=[item for item in candidates if item.side == "多头"],
        data_ranges=data_ranges,
        assumptions=common_assumptions,
    )
    run_side_report(
        side_label="空头",
        output_dir=SHORT_DIR,
        candidates=[item for item in candidates if item.side == "空头"],
        data_ranges=data_ranges,
        assumptions=common_assumptions,
    )
    print(LONG_DIR / "report.html")
    print(SHORT_DIR / "report.html")


def run_side_report(
    *,
    side_label: str,
    output_dir: Path,
    candidates: list[base.CandidateTrade],
    data_ranges: dict[str, dict[str, Any]],
    assumptions: dict[str, Any],
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    simulation = base.simulate_portfolio(
        candidates=candidates,
        initial_capital=INITIAL_CAPITAL,
        risk_per_trade=Decimal("0"),
        max_positions=999999,
        max_long_positions=999999,
        max_short_positions=999999,
        max_total_exposure=Decimal("1000000"),
        max_symbol_exposure=Decimal("1000000"),
        fixed_risk_amount=FIXED_RISK_AMOUNT,
    )
    relevant_ranges = {
        key: value
        for key, value in data_ranges.items()
        if any(item.symbol == key for item in candidates)
    }
    start_ts = min(item["start_ts"] for item in relevant_ranges.values())
    end_ts = max(item["end_ts"] for item in relevant_ranges.values())

    equity_curve = build_equity_curve_u(
        initial_capital=INITIAL_CAPITAL,
        executed_trades=simulation["executed_trades"],
        start_ts=start_ts,
        end_ts=end_ts,
    )
    trades_df = base.build_executed_trade_frame(simulation["executed_trades"])
    trades_export = build_trades_export_u(trades_df)
    summary_df = build_summary_u(trades_df)
    monthly_df = build_monthly_u(equity_curve)
    yearly_df = build_yearly_u(equity_curve, trades_df)
    manifest = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "side": side_label,
        "initial_capital": str(INITIAL_CAPITAL),
        "risk_amount": str(FIXED_RISK_AMOUNT),
        "trade_count": int(len(trades_df)),
        "output_dir": str(output_dir),
    }

    (output_dir / "trades.csv").write_text(trades_export.to_csv(index=False, encoding="utf-8-sig"), encoding="utf-8-sig")
    (output_dir / "summary.csv").write_text(summary_df.to_csv(index=False, encoding="utf-8-sig"), encoding="utf-8-sig")
    (output_dir / "equity_curve.csv").write_text(equity_curve.to_csv(index=False, encoding="utf-8-sig"), encoding="utf-8-sig")
    (output_dir / "monthly_returns.csv").write_text(monthly_df.to_csv(index=False, encoding="utf-8-sig"), encoding="utf-8-sig")
    (output_dir / "yearly_returns.csv").write_text(yearly_df.to_csv(index=False, encoding="utf-8-sig"), encoding="utf-8-sig")
    (output_dir / "run_manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    report_html = build_side_html(
        side_label=side_label,
        assumptions=assumptions,
        relevant_ranges=relevant_ranges,
        equity_curve=equity_curve,
        trades_df=trades_df,
        trades_export=trades_export,
        summary_df=summary_df,
        monthly_df=monthly_df,
        yearly_df=yearly_df,
    )
    (output_dir / "report.html").write_text(report_html, encoding="utf-8")


def build_equity_curve_u(
    *,
    initial_capital: Decimal,
    executed_trades,
    start_ts: int,
    end_ts: int,
) -> pd.DataFrame:
    curve = base.build_hourly_equity_curve(
        start_ts=start_ts,
        end_ts=end_ts,
        initial_capital=initial_capital,
        executed_trades=executed_trades,
    ).copy()
    curve["回撤U"] = curve["历史峰值"] - curve["总权益"]
    curve["累计利润U"] = curve["总权益"] - float(initial_capital)
    return curve[["时间", "总权益", "历史峰值", "回撤U", "累计利润U"]]


def compute_drawdown_u(pnl_series: pd.Series) -> float:
    if pnl_series.empty:
        return 0.0
    cumulative = pnl_series.astype(float).cumsum()
    peak = cumulative.cummax()
    drawdown = peak - cumulative
    return float(drawdown.max()) if not drawdown.empty else 0.0


def build_summary_u(trades_df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    overall_profit = float(trades_df["pnl"].sum()) if not trades_df.empty else 0.0
    overall_drawdown = compute_drawdown_u(trades_df["pnl"]) if not trades_df.empty else 0.0
    rows.append(
        {
            "分类": "总览",
            "名称": "组合合计",
            "交易次数": int(len(trades_df)),
            "胜率%": round(float((trades_df["pnl"] > 0).mean()) * 100.0, 2) if not trades_df.empty else 0.0,
            "利润U": round(overall_profit, 2),
            "最大回撤U": round(overall_drawdown, 2),
            "Profit Factor": round(profit_factor(trades_df["pnl"]) if not trades_df.empty else 0.0, 4),
            "平均R": round(float(trades_df["r_multiple"].mean()), 4) if not trades_df.empty else 0.0,
        }
    )
    for coin, subset in trades_df.groupby("coin", sort=True):
        rows.append(
            {
                "分类": "币种",
                "名称": coin,
                "交易次数": int(len(subset)),
                "胜率%": round(float((subset["pnl"] > 0).mean()) * 100.0, 2),
                "利润U": round(float(subset["pnl"].sum()), 2),
                "最大回撤U": round(compute_drawdown_u(subset["pnl"]), 2),
                "Profit Factor": round(profit_factor(subset["pnl"]), 4),
                "平均R": round(float(subset["r_multiple"].mean()), 4),
            }
        )
    for strategy_name, subset in trades_df.groupby("strategy_name", sort=True):
        rows.append(
            {
                "分类": "策略",
                "名称": strategy_name,
                "交易次数": int(len(subset)),
                "胜率%": round(float((subset["pnl"] > 0).mean()) * 100.0, 2),
                "利润U": round(float(subset["pnl"].sum()), 2),
                "最大回撤U": round(compute_drawdown_u(subset["pnl"]), 2),
                "Profit Factor": round(profit_factor(subset["pnl"]), 4),
                "平均R": round(float(subset["r_multiple"].mean()), 4),
            }
        )
    return pd.DataFrame(rows)


def build_monthly_u(equity_curve: pd.DataFrame) -> pd.DataFrame:
    curve = equity_curve.copy()
    curve["dt"] = pd.to_datetime(curve["时间"])
    daily = curve.set_index("dt")["总权益"].resample("D").last().ffill()
    month_end = daily.resample("ME").last()
    month_start = month_end.shift(1).fillna(float(INITIAL_CAPITAL))
    pnl_u = month_end - month_start
    return pd.DataFrame(
        {
            "月份": month_end.index.strftime("%Y-%m"),
            "月初资金U": month_start.values,
            "月末资金U": month_end.values,
            "月利润U": pnl_u.values,
        }
    )


def build_yearly_u(equity_curve: pd.DataFrame, trades_df: pd.DataFrame) -> pd.DataFrame:
    curve = equity_curve.copy()
    curve["dt"] = pd.to_datetime(curve["时间"])
    curve["year"] = curve["dt"].dt.strftime("%Y")
    rows: list[dict[str, Any]] = []
    for year, subset in curve.groupby("year", sort=True):
        start_equity = float(subset["总权益"].iloc[0])
        end_equity = float(subset["总权益"].iloc[-1])
        year_trades = trades_df[trades_df["year"] == year]
        rows.append(
            {
                "年份": year,
                "年初资金U": round(start_equity, 2),
                "年末资金U": round(end_equity, 2),
                "年利润U": round(end_equity - start_equity, 2),
                "最大回撤U": round(float((subset["历史峰值"] - subset["总权益"]).max()), 2),
                "交易次数": int(len(year_trades)),
                "胜率%": round(float((year_trades["pnl"] > 0).mean()) * 100.0, 2) if not year_trades.empty else 0.0,
                "Profit Factor": round(profit_factor(year_trades["pnl"]) if not year_trades.empty else 0.0, 4),
            }
        )
    return pd.DataFrame(rows)


def build_trades_export_u(trades_df: pd.DataFrame) -> pd.DataFrame:
    if trades_df.empty:
        return pd.DataFrame(
            columns=["编号", "币种", "策略", "方向", "开仓时间", "平仓时间", "利润U", "最大风险U", "R倍数", "平仓原因"]
        )
    out = trades_df.copy()
    out["编号"] = out["trade_no"]
    out["币种"] = out["coin"]
    out["策略"] = out["strategy_name"]
    out["方向"] = out["side"]
    out["开仓时间"] = out["entry_time_bjt"]
    out["平仓时间"] = out["exit_time_bjt"]
    out["开仓价"] = out["entry_price"].map(lambda value: round(float(value), 8))
    out["平仓价"] = out["exit_price"].map(lambda value: round(float(value), 8))
    out["利润U"] = out["pnl"].map(lambda value: round(float(value), 2))
    out["最大风险U"] = out["risk_value"].map(lambda value: round(float(value), 2))
    out["手续费U"] = out["total_fee"].map(lambda value: round(float(value), 2))
    out["滑点U"] = out["slippage_cost"].map(lambda value: round(float(value), 2))
    out["R倍数"] = out["r_multiple"].map(lambda value: round(float(value), 4))
    out["平仓原因"] = out["exit_reason"]
    return out[
        [
            "编号",
            "币种",
            "策略",
            "方向",
            "开仓时间",
            "开仓价",
            "平仓时间",
            "平仓价",
            "利润U",
            "最大风险U",
            "手续费U",
            "滑点U",
            "R倍数",
            "平仓原因",
        ]
    ]


def profit_factor(pnl_series: pd.Series) -> float:
    if pnl_series.empty:
        return 0.0
    pnl = pnl_series.astype(float)
    gross_profit = float(pnl[pnl > 0].sum())
    gross_loss = abs(float(pnl[pnl < 0].sum()))
    if gross_loss == 0:
        return 999.0 if gross_profit > 0 else 0.0
    return gross_profit / gross_loss


def fig_to_base64(fig) -> str:
    buffer = io.BytesIO()
    fig.savefig(buffer, format="png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    return base64.b64encode(buffer.getvalue()).decode("ascii")


def build_line_chart(df: pd.DataFrame, y_col: str, title: str, color: str) -> str:
    fig, ax = plt.subplots(figsize=(10, 3.8))
    x = pd.to_datetime(df["时间"])
    ax.plot(x, df[y_col], color=color, linewidth=1.4)
    ax.set_title(title)
    ax.grid(alpha=0.2)
    return fig_to_base64(fig)


def build_bar_chart(labels: list[str], values: list[float], title: str, color: str) -> str:
    fig, ax = plt.subplots(figsize=(8, 3.8))
    ax.bar(labels, values, color=color)
    ax.set_title(title)
    ax.grid(axis="y", alpha=0.2)
    plt.xticks(rotation=25, ha="right")
    return fig_to_base64(fig)


def render_table(df: pd.DataFrame) -> str:
    if df.empty:
        return "<p class='empty'>暂无数据。</p>"
    headers = "".join(f"<th>{html.escape(str(col))}</th>" for col in df.columns)
    rows = []
    for _, row in df.iterrows():
        cells = "".join(f"<td>{html.escape(str(value))}</td>" for value in row.tolist())
        rows.append(f"<tr>{cells}</tr>")
    return f"<table><thead><tr>{headers}</tr></thead><tbody>{''.join(rows)}</tbody></table>"


def build_side_html(
    *,
    side_label: str,
    assumptions: dict[str, Any],
    relevant_ranges: dict[str, dict[str, Any]],
    equity_curve: pd.DataFrame,
    trades_df: pd.DataFrame,
    trades_export: pd.DataFrame,
    summary_df: pd.DataFrame,
    monthly_df: pd.DataFrame,
    yearly_df: pd.DataFrame,
) -> str:
    total_profit_u = float(trades_df["pnl"].sum()) if not trades_df.empty else 0.0
    max_drawdown_u = float((equity_curve["回撤U"].max())) if not equity_curve.empty else 0.0
    final_equity = float(equity_curve["总权益"].iloc[-1]) if not equity_curve.empty else float(INITIAL_CAPITAL)
    win_rate = float((trades_df["pnl"] > 0).mean()) * 100.0 if not trades_df.empty else 0.0
    pf = profit_factor(trades_df["pnl"]) if not trades_df.empty else 0.0
    avg_r = float(trades_df["r_multiple"].mean()) if not trades_df.empty else 0.0
    cards = [
        ("回测方向", side_label),
        ("初始资金U", f"{float(INITIAL_CAPITAL):,.2f}"),
        ("最终资金U", f"{final_equity:,.2f}"),
        ("总利润U", f"{total_profit_u:,.2f}"),
        ("最大回撤U", f"{max_drawdown_u:,.2f}"),
        ("交易次数", str(len(trades_df))),
        ("胜率", f"{win_rate:.2f}%"),
        ("Profit Factor", f"{pf:.4f}"),
        ("平均R", f"{avg_r:.4f}"),
    ]
    range_df = pd.DataFrame(
        [
            {
                "币种": item["coin"],
                "起始时间": pd.to_datetime(item["start_ts"], unit="ms", utc=True).tz_convert("Asia/Shanghai").strftime("%Y-%m-%d %H:%M:%S"),
                "结束时间": pd.to_datetime(item["end_ts"], unit="ms", utc=True).tz_convert("Asia/Shanghai").strftime("%Y-%m-%d %H:%M:%S"),
                "1H K线数量": item["candles"],
            }
            for item in relevant_ranges.values()
        ]
    ).sort_values("币种")
    assumptions_html = "".join(
        f"<li><strong>{html.escape(str(key))}</strong>：{html.escape(str(value))}</li>"
        for key, value in assumptions.items()
        if key in {"standard_mode", "initial_capital", "risk_amount", "compounding", "signal_rule", "long_fee_model", "short_fee_model"}
    )
    coin_profit = (
        summary_df[summary_df["分类"] == "币种"]
        .sort_values("利润U", ascending=False)
        .reset_index(drop=True)
    )
    strategy_profit = (
        summary_df[summary_df["分类"] == "策略"]
        .sort_values("利润U", ascending=False)
        .reset_index(drop=True)
    )
    equity_chart = build_line_chart(equity_curve, "总权益", f"{side_label}净值曲线（U）", "#1d3557")
    drawdown_chart = build_line_chart(equity_curve, "回撤U", f"{side_label}回撤曲线（U）", "#c1121f")
    coin_chart = build_bar_chart(coin_profit["名称"].tolist(), coin_profit["利润U"].tolist(), f"{side_label}币种利润U", "#2a9d8f") if not coin_profit.empty else ""
    strategy_chart = build_bar_chart(strategy_profit["名称"].tolist(), strategy_profit["利润U"].tolist(), f"{side_label}策略利润U", "#6d597a") if not strategy_profit.empty else ""
    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <title>{side_label}单独回测报告（100U固定风险）</title>
  <style>
    body {{
      margin: 0;
      font-family: "Microsoft YaHei UI", "Microsoft YaHei", sans-serif;
      background: #f6f3ed;
      color: #1f2933;
    }}
    .page {{
      max-width: 1400px;
      margin: 0 auto;
      padding: 24px;
    }}
    .hero {{
      background: linear-gradient(135deg, #0f4c5c 0%, #4d908e 100%);
      color: #fff;
      border-radius: 18px;
      padding: 28px 32px;
      margin-bottom: 20px;
    }}
    .cards {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 12px;
      margin-bottom: 18px;
    }}
    .card {{
      background: #fffdf9;
      border: 1px solid #e8dfd2;
      border-radius: 16px;
      padding: 16px;
    }}
    .metric {{
      font-size: 24px;
      font-weight: 700;
    }}
    .section {{
      background: #fffdf9;
      border: 1px solid #e8dfd2;
      border-radius: 18px;
      padding: 20px 22px;
      margin-bottom: 18px;
    }}
    .grid-2 {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(420px, 1fr));
      gap: 18px;
    }}
    img.chart {{
      width: 100%;
      border-radius: 14px;
      border: 1px solid #ece6da;
      background: #fff;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
    }}
    th, td {{
      border: 1px solid #ece4d7;
      padding: 8px 10px;
      text-align: left;
      vertical-align: top;
    }}
    th {{
      background: #efe6d8;
    }}
    .empty {{
      color: #6b7280;
    }}
  </style>
</head>
<body>
  <div class="page">
    <section class="hero">
      <h1>{html.escape(side_label)}单独回测报告</h1>
      <p>口径：1H 全量数据、100U 固定风险、非复利、不加组合约束、下一根开盘成交、计入手续费与滑点。</p>
    </section>
    <section class="cards">
      {"".join(f"<div class='card'><h3>{html.escape(label)}</h3><div class='metric'>{html.escape(value)}</div></div>" for label, value in cards)}
    </section>
    <section class="section">
      <h2>执行假设</h2>
      <ul>{assumptions_html}</ul>
    </section>
    <section class="section">
      <h2>数据覆盖</h2>
      {render_table(range_df)}
    </section>
    <section class="section">
      <h2>核心图表</h2>
      <div class="grid-2">
        <div><img class="chart" src="data:image/png;base64,{equity_chart}" alt="equity"></div>
        <div><img class="chart" src="data:image/png;base64,{drawdown_chart}" alt="drawdown"></div>
        <div><img class="chart" src="data:image/png;base64,{coin_chart}" alt="coin_profit"></div>
        <div><img class="chart" src="data:image/png;base64,{strategy_chart}" alt="strategy_profit"></div>
      </div>
    </section>
    <section class="section">
      <h2>汇总统计（U口径）</h2>
      {render_table(summary_df)}
    </section>
    <section class="section">
      <h2>月度利润（U）</h2>
      {render_table(monthly_df)}
      <h2>年度利润（U）</h2>
      {render_table(yearly_df)}
    </section>
    <section class="section">
      <h2>交易明细（U口径）</h2>
      {render_table(trades_export)}
    </section>
  </div>
</body>
</html>"""


if __name__ == "__main__":
    main()
