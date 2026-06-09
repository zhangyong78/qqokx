from __future__ import annotations

import html
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
OLD_DIR = ROOT / "reports" / "best_parameter_bundle_1h_standard_100u_before_bear_reentry_compare"
NEW_DIR = ROOT / "reports" / "best_parameter_bundle_1h_standard_100u"
OUTPUT_HTML = NEW_DIR / "bear_reentry_compare_report.html"
OUTPUT_CSV = NEW_DIR / "bear_reentry_compare_summary.csv"


def money(value: float) -> str:
    return f"{value:,.2f}U"


def pct(value: float) -> str:
    return f"{value:,.2f}%"


def load_summary(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, encoding="utf-8-sig")


def extract_rows(df: pd.DataFrame, category: str, key_col: str = "名称") -> pd.DataFrame:
    part = df[df["分类"] == category].copy()
    part = part[[key_col, "方向", "交易次数", "胜率", "收益率", "最大回撤", "Profit Factor", "最终收益"]]
    return part


def compare_table(old_df: pd.DataFrame, new_df: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    merged = old_df.merge(new_df, on=keys, how="outer", suffixes=("_旧", "_新")).fillna(0)
    merged["交易次数变化"] = merged["交易次数_新"] - merged["交易次数_旧"]
    merged["胜率变化"] = merged["胜率_新"] - merged["胜率_旧"]
    merged["收益率变化"] = merged["收益率_新"] - merged["收益率_旧"]
    merged["最大回撤变化"] = merged["最大回撤_新"] - merged["最大回撤_旧"]
    merged["Profit Factor变化"] = merged["Profit Factor_新"] - merged["Profit Factor_旧"]
    merged["最终收益变化"] = merged["最终收益_新"] - merged["最终收益_旧"]
    return merged


def render_table(df: pd.DataFrame) -> str:
    if df.empty:
        return "<p>暂无数据</p>"
    safe = df.copy()
    for col in safe.columns:
        if "收益" in col and "率" not in col and "Profit" not in col:
            safe[col] = safe[col].map(lambda v: money(float(v)))
        elif "胜率" in col or "收益率" in col or "最大回撤" in col:
            safe[col] = safe[col].map(lambda v: pct(float(v)))
        elif "Profit Factor" in col:
            safe[col] = safe[col].map(lambda v: f"{float(v):.4f}")
    head = "".join(f"<th>{html.escape(str(col))}</th>" for col in safe.columns)
    rows = []
    for _, row in safe.iterrows():
        cells = "".join(f"<td>{html.escape(str(value))}</td>" for value in row.tolist())
        rows.append(f"<tr>{cells}</tr>")
    return f"<table><thead><tr>{head}</tr></thead><tbody>{''.join(rows)}</tbody></table>"


def main() -> None:
    old_summary = load_summary(OLD_DIR / "summary.csv")
    new_summary = load_summary(NEW_DIR / "summary.csv")

    combo_old = extract_rows(old_summary, "组合总览")
    combo_new = extract_rows(new_summary, "组合总览")
    side_old = extract_rows(old_summary, "方向统计")
    side_new = extract_rows(new_summary, "方向统计")
    coin_old = extract_rows(old_summary, "币种统计")
    coin_new = extract_rows(new_summary, "币种统计")
    strategy_old = extract_rows(old_summary, "策略统计")
    strategy_new = extract_rows(new_summary, "策略统计")

    combo_compare = compare_table(combo_old, combo_new, ["名称", "方向"])
    side_compare = compare_table(side_old, side_new, ["名称", "方向"])
    coin_compare = compare_table(coin_old, coin_new, ["名称", "方向"]).sort_values("最终收益变化")
    strategy_compare = compare_table(strategy_old, strategy_new, ["名称", "方向"]).sort_values("最终收益变化")

    top_worsen = strategy_compare.nsmallest(5, "最终收益变化")[
        ["名称", "方向", "交易次数变化", "最终收益变化", "胜率变化"]
    ]
    top_improve = strategy_compare.nlargest(5, "最终收益变化")[
        ["名称", "方向", "交易次数变化", "最终收益变化", "胜率变化"]
    ]

    compare_export = pd.concat(
        [
            combo_compare.assign(分类="组合总览"),
            side_compare.assign(分类="方向统计"),
            coin_compare.assign(分类="币种统计"),
            strategy_compare.assign(分类="策略统计"),
        ],
        ignore_index=True,
    )
    compare_export.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")

    combo_row = combo_compare.iloc[0]
    html_text = f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <title>斜率做空重开仓规则变更对比报告</title>
  <style>
    body {{ font-family:"Microsoft YaHei UI","Microsoft YaHei",Arial,sans-serif; margin:0; background:#f5f7fa; color:#17202a; }}
    .wrap {{ max-width:1440px; margin:0 auto; padding:28px; }}
    .hero, .section {{ background:#fff; border:1px solid #dbe3ea; border-radius:8px; padding:24px 28px; margin-bottom:18px; }}
    .hero {{ border-top:5px solid #0f766e; }}
    .grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(220px,1fr)); gap:12px; margin-top:16px; }}
    .card {{ background:#f8fafc; border:1px solid #e5e7eb; border-radius:8px; padding:14px; }}
    .k {{ font-size:13px; color:#667085; margin-bottom:8px; }}
    .v {{ font-size:24px; font-weight:800; }}
    .up {{ color:#15803d; }}
    .down {{ color:#b91c1c; }}
    table {{ width:100%; border-collapse:collapse; font-size:13px; }}
    th, td {{ border:1px solid #e5e7eb; padding:8px 9px; text-align:left; }}
    th {{ background:#edf4f7; }}
    .scroll {{ overflow:auto; max-height:560px; }}
    p {{ line-height:1.7; }}
  </style>
</head>
<body>
<div class="wrap">
  <div class="hero">
    <h1>斜率做空“动态保护出场后需等新阴线再开空”对比报告</h1>
    <p>本次对比的是同一套 1H 最佳参数组合，只修改一条空头再入场纪律：斜率做空若被保本/锁盈类动态保护打出后，不再允许马上重开，必须等后续新的阴线且做空条件仍成立，才允许再开空。</p>
    <div class="grid">
      <div class="card"><div class="k">组合净利润变化</div><div class="v {'up' if float(combo_row['最终收益变化']) >= 0 else 'down'}">{money(float(combo_row['最终收益变化']))}</div></div>
      <div class="card"><div class="k">组合交易次数变化</div><div class="v">{int(combo_row['交易次数变化']):+d}</div></div>
      <div class="card"><div class="k">组合胜率变化</div><div class="v">{pct(float(combo_row['胜率变化']))}</div></div>
      <div class="card"><div class="k">组合最大回撤变化</div><div class="v">{pct(float(combo_row['最大回撤变化']))}</div></div>
    </div>
  </div>

  <div class="section">
    <h2>一句话结论</h2>
    <p>新规则让空头少做了一部分“刚被动态保护洗掉后又立刻追回去”的交易。结果是组合总交易数从 <b>{int(combo_row['交易次数_旧'])}</b> 笔降到 <b>{int(combo_row['交易次数_新'])}</b> 笔，净利润从 <b>{money(float(combo_row['最终收益_旧']))}</b> 变为 <b>{money(float(combo_row['最终收益_新']))}</b>，变化为 <b>{money(float(combo_row['最终收益变化']))}</b>。</p>
    <p>这次调整不是“全面变好”，而是把空头重开仓行为收紧了。它改善了部分币种的空头质量，但也砍掉了一些原本能赚钱的快速再入场，所以更像是在换取更克制的执行纪律，而不是单纯追求更高收益。</p>
  </div>

  <div class="section">
    <h2>组合与方向对比</h2>
    {render_table(combo_compare)}
    <h3>方向对比</h3>
    {render_table(side_compare)}
  </div>

  <div class="section">
    <h2>币种对比</h2>
    <div class="scroll">{render_table(coin_compare)}</div>
  </div>

  <div class="section">
    <h2>策略对比</h2>
    <p>最值得盯的是几条斜率做空策略，因为这次规则只改了它们的再入场纪律。</p>
    <h3>拖累最大的变化</h3>
    {render_table(top_worsen)}
    <h3>改善最大的变化</h3>
    {render_table(top_improve)}
    <h3>完整策略明细</h3>
    <div class="scroll">{render_table(strategy_compare)}</div>
  </div>
</div>
</body>
</html>"""

    OUTPUT_HTML.write_text(html_text, encoding="utf-8")
    print(OUTPUT_HTML)


if __name__ == "__main__":
    main()
