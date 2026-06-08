from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path
from shutil import copyfile

import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import scripts.run_multi_coin_long_plus_short_reentry_color_compare_5coins_10u as base


STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
BASENAME = f"multi_coin_long_plus_short_candidate_baseline_vs_original_5coins_10u_{STAMP}"

base.SHORT_VARIANTS = (
    base.ShortVariant(
        key="original",
        label="原做空",
        note="原始 EMA55 斜率做空：动态保护出场后，只要信号仍成立，就允许继续做空。",
    ),
    base.ShortVariant(
        key="bull_wait_bear_locked_2r",
        label="2R阳线等阴线做空",
        note="仅当 locked_2r_stop 出场且平仓当根收阳时，后续必须等新的阴线且做空条件仍成立才允许再空；其他动态保护出场仍按原逻辑处理。",
        dynamic_exit_bull_bar_requires_bear_reentry=True,
        dynamic_exit_bull_bar_reentry_min_r=2,
        dynamic_exit_bull_bar_reentry_max_r=2,
    ),
)

base.BASENAME = BASENAME
base.HTML_PATH = base.REPORT_DIR / f"{BASENAME}.html"
base.JSON_PATH = base.REPORT_DIR / f"{BASENAME}.json"
base.SUMMARY_CSV_PATH = base.REPORT_DIR / f"{BASENAME}.csv"
base.COIN_CSV_PATH = base.REPORT_DIR / f"{BASENAME}_by_coin.csv"
base.MONTHLY_CSV_PATH = base.REPORT_DIR / f"{BASENAME}_monthly.csv"
base.YEARLY_CSV_PATH = base.REPORT_DIR / f"{BASENAME}_yearly.csv"
base.COIN_MONTHLY_CSV_PATH = base.REPORT_DIR / f"{BASENAME}_monthly_by_coin.csv"
base.COIN_YEARLY_CSV_PATH = base.REPORT_DIR / f"{BASENAME}_yearly_by_coin.csv"
base.PROJECT_HTML_PATH = base.PROJECT_REPORT_DIR / "multi_coin_long_plus_short_candidate_baseline_vs_original_5coins_10u.html"


def main() -> None:
    client = base.OkxRestClient()
    studies = [base.run_symbol_study(client, symbol) for symbol in base.SYMBOLS]

    aggregate_long = base.aggregate_side([study.long_run for study in studies], label="做多")
    aggregate_short = {
        variant.key: base.aggregate_side([study.short_runs[variant.key] for study in studies], label=variant.label)
        for variant in base.SHORT_VARIANTS
    }
    aggregate_combo = {
        variant.key: base.aggregate_combo_side([study.combo_runs[variant.key] for study in studies], label=f"做多 + {variant.label}")
        for variant in base.SHORT_VARIANTS
    }

    summary_frame = base.build_summary_frame(aggregate_long, aggregate_short, aggregate_combo)
    coin_frame = build_coin_frame(studies)
    monthly_frame = base.build_period_frame(aggregate_long, aggregate_short, aggregate_combo, period="month")
    yearly_frame = base.build_period_frame(aggregate_long, aggregate_short, aggregate_combo, period="year")
    coin_monthly_frame = base.build_coin_period_frame(studies, period="month")
    coin_yearly_frame = base.build_coin_period_frame(studies, period="year")

    summary_frame.to_csv(base.SUMMARY_CSV_PATH, index=False, encoding="utf-8-sig")
    coin_frame.to_csv(base.COIN_CSV_PATH, index=False, encoding="utf-8-sig")
    monthly_frame.to_csv(base.MONTHLY_CSV_PATH, index=False, encoding="utf-8-sig")
    yearly_frame.to_csv(base.YEARLY_CSV_PATH, index=False, encoding="utf-8-sig")
    coin_monthly_frame.to_csv(base.COIN_MONTHLY_CSV_PATH, index=False, encoding="utf-8-sig")
    coin_yearly_frame.to_csv(base.COIN_YEARLY_CSV_PATH, index=False, encoding="utf-8-sig")

    payload = build_payload(studies, aggregate_long, aggregate_short, aggregate_combo, summary_frame, coin_frame)
    base.JSON_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    base.HTML_PATH.write_text(
        build_html(
            studies,
            aggregate_long,
            aggregate_short,
            aggregate_combo,
            summary_frame,
            coin_frame,
            monthly_frame,
            yearly_frame,
            coin_monthly_frame,
            coin_yearly_frame,
        ),
        encoding="utf-8",
    )
    copyfile(base.HTML_PATH, base.PROJECT_HTML_PATH)
    print(base.HTML_PATH)
    print(base.PROJECT_HTML_PATH)


def build_coin_frame(studies: list[base.SymbolStudy]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for study in studies:
        original_short = study.short_runs["original"]
        candidate_short = study.short_runs["bull_wait_bear_locked_2r"]
        original_combo = study.combo_runs["original"]
        candidate_combo = study.combo_runs["bull_wait_bear_locked_2r"]
        rows.append(
            {
                "coin": study.label,
                "start": base.format_ts(study.start_ts),
                "end": base.format_ts(study.end_ts),
                "candles": study.candle_count,
                "long_test_pnl_u": float(study.long_run.test_metrics.pnl),
                "short_original_test_pnl_u": float(original_short.test_metrics.pnl),
                "short_candidate_test_pnl_u": float(candidate_short.test_metrics.pnl),
                "short_candidate_delta_test_u": float(candidate_short.test_metrics.pnl - original_short.test_metrics.pnl),
                "combo_original_test_pnl_u": float(original_combo.test_metrics.pnl),
                "combo_candidate_test_pnl_u": float(candidate_combo.test_metrics.pnl),
                "combo_candidate_delta_test_u": float(candidate_combo.test_metrics.pnl - original_combo.test_metrics.pnl),
                "combo_original_test_drawdown_u": float(original_combo.test_metrics.max_drawdown),
                "combo_candidate_test_drawdown_u": float(candidate_combo.test_metrics.max_drawdown),
                "combo_candidate_drawdown_delta_test_u": float(candidate_combo.test_metrics.max_drawdown - original_combo.test_metrics.max_drawdown),
            }
        )
    return pd.DataFrame(rows)


def build_payload(
    studies: list[base.SymbolStudy],
    aggregate_long: base.AggregateRun,
    aggregate_short: dict[str, base.AggregateRun],
    aggregate_combo: dict[str, base.AggregateRun],
    summary_frame: pd.DataFrame,
    coin_frame: pd.DataFrame,
) -> dict[str, object]:
    return {
        "generated_at": datetime.now().isoformat(),
        "html_path": str(base.HTML_PATH),
        "project_html_path": str(base.PROJECT_HTML_PATH),
        "summary": summary_frame.to_dict(orient="records"),
        "by_coin": coin_frame.to_dict(orient="records"),
        "study_range": {
            "start": min(base.format_ts(study.start_ts) for study in studies),
            "end": max(base.format_ts(study.end_ts) for study in studies),
        },
        "all_sample": {
            "original": str(aggregate_combo["original"].all_metrics.pnl),
            "candidate": str(aggregate_combo["bull_wait_bear_locked_2r"].all_metrics.pnl),
        },
        "test_sample": {
            "original": str(aggregate_combo["original"].test_metrics.pnl),
            "candidate": str(aggregate_combo["bull_wait_bear_locked_2r"].test_metrics.pnl),
        },
        "long_only_test_pnl_u": str(aggregate_long.test_metrics.pnl),
    }


def build_html(
    studies: list[base.SymbolStudy],
    aggregate_long: base.AggregateRun,
    aggregate_short: dict[str, base.AggregateRun],
    aggregate_combo: dict[str, base.AggregateRun],
    summary_frame: pd.DataFrame,
    coin_frame: pd.DataFrame,
    monthly_frame: pd.DataFrame,
    yearly_frame: pd.DataFrame,
    coin_monthly_frame: pd.DataFrame,
    coin_yearly_frame: pd.DataFrame,
) -> str:
    original_combo = aggregate_combo["original"]
    candidate_combo = aggregate_combo["bull_wait_bear_locked_2r"]
    delta_all = candidate_combo.all_metrics.pnl - original_combo.all_metrics.pnl
    delta_test = candidate_combo.test_metrics.pnl - original_combo.test_metrics.pnl
    dd_all = candidate_combo.all_metrics.max_drawdown - original_combo.all_metrics.max_drawdown
    dd_test = candidate_combo.test_metrics.max_drawdown - original_combo.test_metrics.max_drawdown
    all_curve = base.build_equity_curve_image(
        {
            "做多": aggregate_long.trades,
            "做多 + 原做空": original_combo.trades,
            "做多 + 2R阳线等阴线做空": candidate_combo.trades,
        },
        "全样本累计净利润",
    )
    test_curve = base.build_equity_curve_image(
        {
            "做多": aggregate_long.test_trades,
            "做多 + 原做空": original_combo.test_trades,
            "做多 + 2R阳线等阴线做空": candidate_combo.test_trades,
        },
        "测试段累计净利润",
    )
    monthly_pivot = base.build_period_pivot_html(monthly_frame, "月度汇总")
    yearly_pivot = base.build_period_pivot_html(yearly_frame, "年度汇总")
    coin_yearly_sections = base.build_coin_period_sections(coin_yearly_frame, "年度")
    coin_monthly_sections = base.build_coin_period_sections(coin_monthly_frame, "月度")
    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <title>候选基线 vs 原做空 定稿对比</title>
  <style>
    :root {{
      --bg: #f5f1e8;
      --panel: #fffdf8;
      --ink: #1f1b16;
      --muted: #6c6257;
      --line: #d9cfc1;
      --accent: #9a3412;
      --good: #166534;
      --bad: #b91c1c;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      background: radial-gradient(circle at top left, rgba(154,52,18,0.08), transparent 32%), linear-gradient(180deg, #f8f3ea 0%, var(--bg) 100%);
      color: var(--ink);
      font-family: "Segoe UI", "Microsoft YaHei", sans-serif;
      line-height: 1.55;
    }}
    .wrap {{ width: min(1420px, calc(100vw - 48px)); margin: 0 auto; padding: 28px 0 40px; }}
    .hero, .section {{ background: rgba(255,255,255,0.78); border: 1px solid rgba(217,207,193,0.9); border-radius: 24px; padding: 24px; }}
    .section {{ margin-top: 24px; }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 14px; margin-top: 18px; }}
    .card {{ background: var(--panel); border: 1px solid var(--line); border-radius: 18px; padding: 16px 18px; }}
    .label {{ color: var(--muted); font-size: 13px; }}
    .value {{ font-size: 28px; font-weight: 700; margin-top: 6px; }}
    .good {{ color: var(--good); }}
    .bad {{ color: var(--bad); }}
    table {{ width: 100%; border-collapse: collapse; margin-top: 14px; background: var(--panel); }}
    th, td {{ padding: 10px 12px; border-bottom: 1px solid var(--line); text-align: right; white-space: nowrap; font-size: 13px; }}
    th:first-child, td:first-child {{ text-align: left; }}
    thead th {{ background: #efe5d8; }}
    .two-col {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(360px, 1fr)); gap: 16px; }}
    .img-box {{ margin-top: 16px; background: var(--panel); border: 1px solid var(--line); border-radius: 18px; padding: 10px; }}
    img {{ display: block; width: 100%; height: auto; border-radius: 12px; }}
    p {{ color: var(--muted); }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <h1>候选基线 vs 原做空 定稿对比</h1>
      <p>候选基线规则：仅当 <code>locked_2r_stop</code> 出场且平仓当根收阳时，后续必须等新的阴线且做空条件仍成立才允许再空。</p>
      <div class="grid">
        <div class="card"><div class="label">原组合全样本</div><div class="value">{base.fmt_u(original_combo.all_metrics.pnl)}</div></div>
        <div class="card"><div class="label">候选组合全样本</div><div class="value">{base.fmt_u(candidate_combo.all_metrics.pnl)}</div></div>
        <div class="card"><div class="label">全样本 Delta</div><div class="value {'good' if delta_all >= 0 else 'bad'}">{base.fmt_u(delta_all)}</div></div>
        <div class="card"><div class="label">原组合测试段</div><div class="value">{base.fmt_u(original_combo.test_metrics.pnl)}</div></div>
        <div class="card"><div class="label">候选组合测试段</div><div class="value">{base.fmt_u(candidate_combo.test_metrics.pnl)}</div></div>
        <div class="card"><div class="label">测试段 Delta</div><div class="value {'good' if delta_test >= 0 else 'bad'}">{base.fmt_u(delta_test)}</div></div>
      </div>
    </section>

    <section class="section">
      <h2>结论</h2>
      <p>这版候选基线相对原做空，在综合多空口径下，全样本变化 <span class="{'good' if delta_all >= 0 else 'bad'}">{base.fmt_u(delta_all)}</span>，测试段变化 <span class="{'good' if delta_test >= 0 else 'bad'}">{base.fmt_u(delta_test)}</span>。</p>
      <p>回撤方面，全样本变化 <span class="{'good' if dd_all <= 0 else 'bad'}">{base.fmt_u(dd_all)}</span>，测试段变化 <span class="{'good' if dd_test <= 0 else 'bad'}">{base.fmt_u(dd_test)}</span>。</p>
    </section>

    <section class="section">
      <h2>总览对比</h2>
      {base.dataframe_to_html(summary_frame)}
    </section>

    <section class="section">
      <h2>累计净利润曲线</h2>
      <div class="two-col">
        <div class="img-box"><h3>全样本</h3><img alt="全样本累计净利润曲线" src="data:image/png;base64,{all_curve}" /></div>
        <div class="img-box"><h3>测试段</h3><img alt="测试段累计净利润曲线" src="data:image/png;base64,{test_curve}" /></div>
      </div>
    </section>

    <section class="section">
      <h2>分币种测试段</h2>
      {base.dataframe_to_html(coin_frame)}
    </section>

    <section class="section">
      <h2>月度与年度汇总</h2>
      <div class="two-col">
        <div>{monthly_pivot}</div>
        <div>{yearly_pivot}</div>
      </div>
    </section>

    <section class="section">
      <h2>分币种年度明细</h2>
      {coin_yearly_sections}
    </section>

    <section class="section">
      <h2>分币种月度明细</h2>
      {coin_monthly_sections}
    </section>
  </div>
</body>
</html>"""


if __name__ == "__main__":
    main()
