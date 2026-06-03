from __future__ import annotations

from html import escape
from pathlib import Path

import pandas as pd


def write_v11_reports(
    output_md_path: Path,
    output_html_path: Path,
    *,
    config: dict[str, object],
    metadata: dict[str, str],
    comparison: pd.DataFrame,
    throttle_usage: pd.DataFrame,
    streak_pressure: pd.DataFrame,
) -> None:
    best = comparison.iloc[0]
    base_v8d = select_row(comparison, "v8_d_strong_1_5_weak_0_5", "v11_a_no_throttle")
    base_v8c = select_row(comparison, "v8_c_strong_1_25_weak_0_5", "v11_a_no_throttle")
    base_flat = select_row(comparison, "v8_a_flat_1_0", "v11_a_no_throttle")
    recommendation = choose_v11_recommendation(comparison)

    lines = [
        "# V11 Trade-Sequence Guardrail Report",
        "",
        "## 1. Study Goal",
        "",
        "Check whether short-cycle protection is worth doing by reducing risk after consecutive losses, instead of waiting for a calendar-month stop.",
        "",
        "## 2. Fixed Trade Logic",
        "",
        f"- baseline entry gate: `{config['v6_baseline_strategy']}`",
        f"- strong regime definition: `{config['v6_candidate_strategy']}`",
        f"- cost scenario: `{config['v11_cost_scenario']}`",
        "- exit rule: EMA21 reclaim on 4H close, execute on next 4H open",
        "- only the risk schedule and loss-sequence throttle rule vary",
        "",
        "## 3. Data Source",
        "",
        f"- source: `{metadata['data_source']}`",
        f"- data_root: `{metadata['data_root']}`",
        f"- symbol: `{metadata['symbol']}`",
        f"- timeframes: `{metadata['timeframe']}`",
        "",
        "## 4. Full-History Comparison",
        "",
        comparison.to_markdown(index=False),
        "",
        "## 5. Guardrail Usage",
        "",
        throttle_usage.to_markdown(index=False) if not throttle_usage.empty else "No throttle rows.",
        "",
        "## 6. Sequence Pressure",
        "",
        streak_pressure.to_markdown(index=False) if not streak_pressure.empty else "No streak rows.",
        "",
        "## 7. Findings",
        "",
        f"- best V11 cell: {best['strategy_name']} with total_return={best['total_return']:.2%}, profit_factor={best['profit_factor']:.2f}, max_drawdown={best['max_drawdown']:.2%}",
        f"- V8-d base without sequence throttle: total_return={base_v8d['total_return']:.2%}, profit_factor={base_v8d['profit_factor']:.2f}, max_drawdown={base_v8d['max_drawdown']:.2%}",
        f"- V8-c base without sequence throttle: total_return={base_v8c['total_return']:.2%}, profit_factor={base_v8c['profit_factor']:.2f}, max_drawdown={base_v8c['max_drawdown']:.2%}",
        f"- flat base without sequence throttle: total_return={base_flat['total_return']:.2%}, profit_factor={base_flat['profit_factor']:.2f}, max_drawdown={base_flat['max_drawdown']:.2%}",
        build_delta_line(best, base_v8d),
        build_usage_line(throttle_usage, best["strategy_name"]),
        build_streak_line(streak_pressure, best["strategy_name"], base_v8d["strategy_name"]),
        "",
        "## 8. Recommendation",
        "",
        f"Carry forward: `{recommendation}`",
        "",
        "## 9. Interpretation",
        "",
        "- If a sequence throttle improves the worst loss cluster or drawdown while preserving most of the return, this research path is worth keeping.",
        "- If it barely changes anything, then the pain is already embedded in sparse trade timing and this line should stop here.",
        "",
        "## 10. Output Notes",
        "",
        f"- results_dir: `{config['v11_results_dir']}`",
        f"- tested_schedule_count: {comparison['schedule_name'].nunique() if not comparison.empty else 0}",
        f"- tested_rule_count: {comparison['loss_throttle_rule_name'].nunique() if not comparison.empty else 0}",
    ]
    output_md_path.write_text("\n".join(lines), encoding="utf-8")

    html = build_v11_html(
        config=config,
        metadata=metadata,
        comparison=comparison,
        throttle_usage=throttle_usage,
        streak_pressure=streak_pressure,
        recommendation=recommendation,
    )
    output_html_path.write_text(html, encoding="utf-8")


def choose_v11_recommendation(comparison: pd.DataFrame) -> str:
    ranked = comparison.sort_values(
        ["profit_factor", "total_return", "max_drawdown"],
        ascending=[False, False, False],
    )
    return str(ranked.iloc[0]["strategy_name"])


def select_row(comparison: pd.DataFrame, schedule_name: str, loss_throttle_rule_name: str) -> pd.Series:
    return comparison[
        (comparison["schedule_name"] == schedule_name) & (comparison["loss_throttle_rule_name"] == loss_throttle_rule_name)
    ].iloc[0]


def build_delta_line(best: pd.Series, base_v8d: pd.Series) -> str:
    return (
        f"- best vs V8-d base: return_delta={(float(best['total_return']) - float(base_v8d['total_return'])):.2%}, "
        f"pf_delta={(float(best['profit_factor']) - float(base_v8d['profit_factor'])):.2f}, "
        f"drawdown_delta={(float(best['max_drawdown']) - float(base_v8d['max_drawdown'])):.2%}"
    )


def build_usage_line(throttle_usage: pd.DataFrame, strategy_name: str) -> str:
    row = throttle_usage[throttle_usage["strategy_name"] == strategy_name].iloc[0]
    return (
        f"- guardrail usage: activation_count={int(row['activation_count'])}, "
        f"reduced_risk_trade_count={int(row['reduced_risk_trade_count'])}, "
        f"average_effective_risk_multiplier={float(row['average_effective_risk_multiplier']):.2f}"
    )


def build_streak_line(streak_pressure: pd.DataFrame, strategy_name: str, base_strategy_name: str) -> str:
    row = streak_pressure[streak_pressure["strategy_name"] == strategy_name].iloc[0]
    base = streak_pressure[streak_pressure["strategy_name"] == base_strategy_name].iloc[0]
    return (
        f"- worst 10-trade pressure: candidate={float(row['worst_10_trade_pnl']):.2f} vs V8-d base={float(base['worst_10_trade_pnl']):.2f}; "
        f"max_consecutive_losses candidate={int(row['max_consecutive_losses'])} vs base={int(base['max_consecutive_losses'])}"
    )


def build_v11_html(
    *,
    config: dict[str, object],
    metadata: dict[str, str],
    comparison: pd.DataFrame,
    throttle_usage: pd.DataFrame,
    streak_pressure: pd.DataFrame,
    recommendation: str,
) -> str:
    best = comparison.iloc[0]
    base_v8d = select_row(comparison, "v8_d_strong_1_5_weak_0_5", "v11_a_no_throttle")
    v9_logic = [
        "V9 proved the pain was concentrated in loss clusters, not in whole months.",
        "V10 proved monthly stop rules were too slow because they triggered late and skipped zero trades.",
        "That made short-cycle guardrails the only remaining protection path worth testing.",
    ]
    style = """
body { font-family: 'Segoe UI', sans-serif; margin: 32px auto; max-width: 1100px; color: #1f2937; background: #f7f4ed; line-height: 1.6; }
h1, h2, h3 { color: #111827; }
.card { background: #fffdf8; border: 1px solid #e5dccb; border-radius: 14px; padding: 20px 24px; margin: 18px 0; box-shadow: 0 8px 24px rgba(17,24,39,0.06); }
.hero { background: linear-gradient(135deg, #f4efe4 0%, #fffdf8 60%, #eef5f1 100%); }
.metric-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; margin-top: 16px; }
.metric { background: #fff; border: 1px solid #e8dfd1; border-radius: 12px; padding: 14px; }
.metric-label { font-size: 12px; color: #6b7280; text-transform: uppercase; letter-spacing: 0.04em; }
.metric-value { font-size: 24px; font-weight: 700; margin-top: 6px; color: #111827; }
table { width: 100%; border-collapse: collapse; font-size: 14px; }
th, td { border: 1px solid #e5dccb; padding: 8px 10px; text-align: left; }
th { background: #f3ecdf; }
ul { margin: 0; padding-left: 20px; }
.yes { color: #0f766e; font-weight: 700; }
.no { color: #b45309; font-weight: 700; }
code { background: #f3ecdf; padding: 2px 6px; border-radius: 6px; }
"""
    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>V11 Trade Sequence Guardrail Report</title>
  <style>{style}</style>
</head>
<body>
  <section class="card hero">
    <h1>V11 研究结论</h1>
    <p><span class="yes">这个方向有必要继续做</span>，而且现在已经有了第一轮直接数据。原因不是月度停手有效，而是 <code>V9</code> 和 <code>V10</code> 一起证明：真正的痛点在连续亏损簇，月度规则太慢。</p>
    <div class="metric-grid">
      <div class="metric"><div class="metric-label">当前推荐</div><div class="metric-value">{escape(recommendation)}</div></div>
      <div class="metric"><div class="metric-label">最佳总收益</div><div class="metric-value">{best['total_return']:.2%}</div></div>
      <div class="metric"><div class="metric-label">最佳 PF</div><div class="metric-value">{best['profit_factor']:.2f}</div></div>
      <div class="metric"><div class="metric-label">最佳回撤</div><div class="metric-value">{best['max_drawdown']:.2%}</div></div>
    </div>
  </section>

  <section class="card">
    <h2>为什么这条线值得继续</h2>
    <ul>
      {''.join(f'<li>{escape(item)}</li>' for item in v9_logic)}
    </ul>
  </section>

  <section class="card">
    <h2>固定前提</h2>
    <ul>
      <li>数据源：{escape(metadata['data_source'])}</li>
      <li>数据目录：{escape(metadata['data_root'])}</li>
      <li>标的：{escape(metadata['symbol'])}</li>
      <li>周期：{escape(metadata['timeframe'])}</li>
      <li>成本场景：{escape(str(config['v11_cost_scenario']))}</li>
    </ul>
  </section>

  <section class="card">
    <h2>V11 全量结果</h2>
    {comparison.to_html(index=False, border=0)}
  </section>

  <section class="card">
    <h2>Guardrail 使用情况</h2>
    {throttle_usage.to_html(index=False, border=0)}
  </section>

  <section class="card">
    <h2>连续亏损压力</h2>
    {streak_pressure.to_html(index=False, border=0)}
  </section>

  <section class="card">
    <h2>一句话判断</h2>
    <p>如果目标是继续优化实盘体验，<span class="yes">短节奏保护值得继续研究</span>。但是否值得正式保留，要看它能不能在不明显伤害收益的前提下，真正改善 <code>v8_d</code> 的亏损簇压力。</p>
    <p>本轮基准参考 <code>v8_d + no throttle</code> 为：收益 {base_v8d['total_return']:.2%}，PF {base_v8d['profit_factor']:.2f}，回撤 {base_v8d['max_drawdown']:.2%}。</p>
  </section>
</body>
</html>"""
