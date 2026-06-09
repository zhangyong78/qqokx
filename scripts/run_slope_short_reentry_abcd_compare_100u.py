from __future__ import annotations

import html
import json
import sys
from dataclasses import replace
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import scripts.run_best_parameter_bundle_1h_standard_portfolio as base
from okx_quant.models import StrategyConfig
from okx_quant.okx_client import OkxRestClient
from okx_quant.strategy_catalog import STRATEGY_BODY_RETEST_SHORT_ID
from okx_quant.strategy_profiles import StrategyBundle, build_strategy_profile_from_config
from scripts.build_best_parameter_bundle import (
    LONG_SPECS,
    SHORT_SPECS,
    build_body_retest_short_config,
    build_dynamic_long_config,
    build_slope_short_config,
)


OUTPUT_DIR = ROOT / "reports" / "slope_short_reentry_abcd_compare_100u"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

SUMMARY_CSV = OUTPUT_DIR / "variant_summary.csv"
DETAIL_CSV = OUTPUT_DIR / "variant_detail_summary.csv"
HTML_REPORT = OUTPUT_DIR / "report.html"
MANIFEST_JSON = OUTPUT_DIR / "run_manifest.json"

INITIAL_CAPITAL = Decimal("10000")
FIXED_RISK = Decimal("100")
STOP_OPTIONS = (Decimal("1.5"), Decimal("2.0"), Decimal("2.5"), Decimal("3.0"))
SCENARIO_OPTIONS = ("A", "B", "C")


def money(value: float) -> str:
    return f"{value:,.2f}U"


def pct(value: float) -> str:
    return f"{value:,.2f}%"


def table_html(df: pd.DataFrame, max_rows: int | None = None) -> str:
    if df.empty:
        return "<p>暂无数据</p>"
    part = df.head(max_rows) if max_rows is not None else df
    head = "".join(f"<th>{html.escape(str(col))}</th>" for col in part.columns)
    rows = []
    for _, row in part.iterrows():
        cells = "".join(f"<td>{html.escape(str(value))}</td>" for value in row.tolist())
        rows.append(f"<tr>{cells}</tr>")
    return f"<table><thead><tr>{head}</tr></thead><tbody>{''.join(rows)}</tbody></table>"


def build_profiles_for_variant(*, scenario: str, stop_atr: Decimal):
    profiles = []

    for spec in LONG_SPECS:
        config = build_dynamic_long_config(spec)
        profiles.append(
            build_strategy_profile_from_config(
                profile_id=spec.profile_id,
                profile_name=spec.profile_name,
                strategy_id=config.strategy_id,
                symbol=spec.symbol,
                config=config,
                direction_label="多头",
                run_mode_label="实盘",
                tags=("ABCD", scenario),
                notes=spec.notes,
                source_report="ABCD研究对比",
            )
        )

    for spec in SHORT_SPECS:
        if spec.strategy_id == STRATEGY_BODY_RETEST_SHORT_ID:
            config = build_body_retest_short_config(spec)
        else:
            config = build_slope_short_config(spec)
            config = replace(
                config,
                atr_stop_multiplier=stop_atr,
                atr_take_multiplier=stop_atr * Decimal("2"),
            )
            config = apply_slope_scenario(config=config, scenario=scenario, symbol=spec.symbol)

        profiles.append(
            build_strategy_profile_from_config(
                profile_id=f"{spec.profile_id}_{scenario}_sl{str(stop_atr).replace('.', '_')}",
                profile_name=f"{spec.profile_name} | {scenario} | SL{stop_atr}",
                strategy_id=config.strategy_id,
                symbol=spec.symbol,
                config=config,
                direction_label="空头",
                run_mode_label="实盘",
                tags=("ABCD", scenario, f"SL{stop_atr}"),
                notes=spec.notes,
                source_report="ABCD研究对比",
            )
        )

    return tuple(profiles)


def apply_slope_scenario(*, config: StrategyConfig, scenario: str, symbol: str) -> StrategyConfig:
    base_kwargs: dict[str, Any] = {
        "ema55_slope_same_bar_reentry_block": False,
        "ema55_slope_dynamic_exit_requires_bear_reentry": False,
        "ema55_slope_dynamic_exit_bear_reentry_break_prev_low": False,
    }

    if scenario == "A":
        base_kwargs["ema55_slope_same_bar_reentry_block"] = True
    elif scenario == "B":
        base_kwargs["ema55_slope_same_bar_reentry_block"] = True
        base_kwargs["ema55_slope_dynamic_exit_requires_bear_reentry"] = True
        base_kwargs["ema55_slope_dynamic_exit_bear_reentry_break_prev_low"] = True
    elif scenario == "C":
        base_kwargs["ema55_slope_same_bar_reentry_block"] = True
        if symbol == "SOL-USDT-SWAP":
            base_kwargs["ema55_slope_dynamic_exit_requires_bear_reentry"] = True
            base_kwargs["ema55_slope_dynamic_exit_bear_reentry_break_prev_low"] = True
    else:
        raise ValueError(f"unknown scenario: {scenario}")

    return replace(config, **base_kwargs)


def run_variant(*, client: OkxRestClient, scenario: str, stop_atr: Decimal) -> dict[str, Any]:
    bundle = StrategyBundle(
        bundle_version=1,
        bundle_name=f"ABCD研究 | {scenario} | SL{stop_atr}",
        profiles=build_profiles_for_variant(scenario=scenario, stop_atr=stop_atr),
        created_at=datetime.now().isoformat(timespec="seconds"),
        source_report="ABCD研究对比",
        auto_start_on_import=False,
    )

    candidates, data_ranges, assumptions = base.build_candidate_trades(
        bundle_path=Path(f"ABCD/{scenario}/SL{stop_atr}"),
        client=client,
        bundle=bundle,
        base_initial_capital=INITIAL_CAPITAL,
        base_risk_amount=FIXED_RISK,
    )
    assumptions.update({"scenario": scenario, "stop_atr": str(stop_atr)})

    simulation = base.simulate_portfolio(
        candidates=candidates,
        initial_capital=INITIAL_CAPITAL,
        risk_per_trade=Decimal("0"),
        max_positions=999999,
        max_long_positions=999999,
        max_short_positions=999999,
        max_total_exposure=Decimal("1000000"),
        max_symbol_exposure=Decimal("1000000"),
        fixed_risk_amount=FIXED_RISK,
        preserve_candidate_size=True,
    )

    start_ts = min(item["start_ts"] for item in data_ranges.values())
    end_ts = max(item["end_ts"] for item in data_ranges.values())
    equity_hourly = base.build_hourly_equity_curve(
        start_ts=start_ts,
        end_ts=end_ts,
        initial_capital=INITIAL_CAPITAL,
        executed_trades=simulation["executed_trades"],
    )
    executed_df = base.build_executed_trade_frame(simulation["executed_trades"])
    overall_metrics = base.compute_trade_metrics(executed_df, INITIAL_CAPITAL)
    drawdown_meta = base.compute_drawdown_metadata(equity_hourly)
    side_summary = base.build_side_summary(executed_df)
    symbol_summary = base.build_symbol_summary(executed_df)
    strategy_summary = base.build_strategy_summary(executed_df)

    short_summary = side_summary[side_summary["方向"] == "空头"]
    short_pnl = float(short_summary["最终收益"].sum()) if not short_summary.empty else 0.0
    long_summary = side_summary[side_summary["方向"] == "多头"]
    long_pnl = float(long_summary["最终收益"].sum()) if not long_summary.empty else 0.0

    symbol_total = symbol_summary[symbol_summary["方向"] == "合计"].sort_values("最终收益", ascending=False)
    strategy_total = (
        strategy_summary.groupby("策略", as_index=False)["最终收益"].sum().sort_values("最终收益", ascending=False)
        if not strategy_summary.empty
        else pd.DataFrame(columns=["策略", "最终收益"])
    )

    return {
        "scenario": scenario,
        "stop_atr": float(stop_atr),
        "label": f"{scenario} | SL{stop_atr}",
        "assumptions": assumptions,
        "overall_metrics": overall_metrics,
        "drawdown_meta": drawdown_meta,
        "side_summary": side_summary,
        "symbol_summary": symbol_summary,
        "strategy_summary": strategy_summary,
        "executed_df": executed_df,
        "total_pnl": float(overall_metrics["total_pnl"]),
        "max_drawdown_u": float(overall_metrics["max_drawdown_amount"]),
        "max_drawdown_pct": abs(float(drawdown_meta["max_drawdown_pct"])) * 100.0,
        "trades": int(overall_metrics["trades"]),
        "win_rate": float(overall_metrics["win_rate"]) * 100.0,
        "profit_factor": float(overall_metrics["profit_factor"] or 0),
        "avg_r": float(overall_metrics["avg_r"]),
        "long_pnl": long_pnl,
        "short_pnl": short_pnl,
        "best_coin": "" if symbol_total.empty else str(symbol_total.iloc[0]["币种"]),
        "worst_coin": "" if symbol_total.empty else str(symbol_total.iloc[-1]["币种"]),
        "best_strategy": "" if strategy_total.empty else str(strategy_total.iloc[0]["策略"]),
        "worst_strategy": "" if strategy_total.empty else str(strategy_total.iloc[-1]["策略"]),
    }


def build_variant_summary_frame(results: list[dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for item in results:
        rows.append(
            {
                "方案": item["scenario"],
                "止损ATR": item["stop_atr"],
                "标签": item["label"],
                "净利润U": round(item["total_pnl"], 2),
                "最大回撤U": round(item["max_drawdown_u"], 2),
                "最大回撤%": round(item["max_drawdown_pct"], 2),
                "交易次数": item["trades"],
                "胜率%": round(item["win_rate"], 2),
                "Profit Factor": round(item["profit_factor"], 4),
                "平均R": round(item["avg_r"], 4),
                "多头利润U": round(item["long_pnl"], 2),
                "空头利润U": round(item["short_pnl"], 2),
                "最佳币种": item["best_coin"],
                "最差币种": item["worst_coin"],
                "最佳策略": item["best_strategy"],
                "最差策略": item["worst_strategy"],
            }
        )
    return pd.DataFrame(rows).sort_values(["方案", "止损ATR"]).reset_index(drop=True)


def build_detail_summary_frame(results: list[dict[str, Any]]) -> pd.DataFrame:
    frames = []
    for item in results:
        side = item["side_summary"].copy()
        side.insert(0, "分类", "方向统计")
        symbol = item["symbol_summary"].copy()
        side.insert(0, "标签", item["label"])
        symbol.insert(0, "分类", "币种统计")
        symbol.insert(0, "标签", item["label"])
        strategy = item["strategy_summary"].copy()
        strategy.insert(0, "分类", "策略统计")
        strategy.insert(0, "标签", item["label"])
        frames.extend([side, symbol, strategy])
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def main() -> None:
    client = OkxRestClient()
    results: list[dict[str, Any]] = []
    for scenario in SCENARIO_OPTIONS:
        for stop_atr in STOP_OPTIONS:
            print(f"running {scenario} stop={stop_atr}")
            results.append(run_variant(client=client, scenario=scenario, stop_atr=stop_atr))

    summary_frame = build_variant_summary_frame(results)
    detail_frame = build_detail_summary_frame(results)
    summary_frame.to_csv(SUMMARY_CSV, index=False, encoding="utf-8-sig")
    detail_frame.to_csv(DETAIL_CSV, index=False, encoding="utf-8-sig")

    stop_2 = summary_frame[summary_frame["止损ATR"] == 2.0].copy().sort_values("净利润U", ascending=False)
    matrix_rank = summary_frame.sort_values(["净利润U", "最大回撤%"], ascending=[False, True]).reset_index(drop=True)
    best_overall = matrix_rank.iloc[0]
    best_by_scenario = (
        summary_frame.sort_values(["方案", "净利润U", "最大回撤%"], ascending=[True, False, True])
        .groupby("方案", as_index=False)
        .first()
    )

    html_text = f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <title>斜率做空 ABCD 方案研究对比</title>
  <style>
    body {{ margin:0; font-family:"Microsoft YaHei UI","Microsoft YaHei",Arial,sans-serif; background:#f6f8fb; color:#17202a; }}
    .wrap {{ max-width:1480px; margin:0 auto; padding:28px; }}
    .hero, .section {{ background:#fff; border:1px solid #d9e2ec; border-radius:8px; padding:24px 28px; margin-bottom:18px; }}
    .hero {{ border-top:5px solid #0f766e; }}
    .grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(220px,1fr)); gap:12px; margin-top:16px; }}
    .card {{ background:#f8fafc; border:1px solid #e5e7eb; border-radius:8px; padding:14px; }}
    .k {{ font-size:13px; color:#667085; margin-bottom:8px; }}
    .v {{ font-size:24px; font-weight:800; }}
    .section h2 {{ margin-top:0; }}
    table {{ width:100%; border-collapse:collapse; font-size:13px; }}
    th, td {{ border:1px solid #e5e7eb; padding:8px 9px; text-align:left; }}
    th {{ background:#edf4f7; }}
    .scroll {{ overflow:auto; max-height:620px; }}
    p {{ line-height:1.72; }}
  </style>
</head>
<body>
<div class="wrap">
  <div class="hero">
    <h1>斜率做空再开仓 ABCD 研究对比</h1>
    <p>研究口径：组合仍为 1H 最佳参数包，固定每笔风险 100U，不复利，统一计入手续费与滑点。多头保持不变，仅针对斜率做空的再开仓纪律与止损宽度做研究。假设说明：B 里的“前低”按“当前阴线收盘跌破前一根 K 线低点”执行；C 为分币种方案，SOL 用 B，其余斜率空头币种用 A，BNB 回抽做空维持原规则；D 不是单独一条规则，而是对 A/B/C 各扫止损 ATR 1.5 / 2.0 / 2.5 / 3.0。</p>
    <div class="grid">
      <div class="card"><div class="k">矩阵冠军</div><div class="v">{html.escape(str(best_overall["标签"]))}</div></div>
      <div class="card"><div class="k">冠军净利润</div><div class="v">{money(float(best_overall["净利润U"]))}</div></div>
      <div class="card"><div class="k">冠军最大回撤</div><div class="v">{pct(float(best_overall["最大回撤%"]))}</div></div>
      <div class="card"><div class="k">冠军交易次数</div><div class="v">{int(best_overall["交易次数"])}</div></div>
    </div>
  </div>

  <div class="section">
    <h2>方案解释</h2>
    <p><b>A：</b>恢复原重开仓逻辑，但禁止同一根 K 线刚平仓就立刻再开。</p>
    <p><b>B：</b>动态保护出场后，必须等待新的阴线，且该阴线收盘跌破前一根 K 线低点，同时斜率做空条件仍成立，才允许再开。</p>
    <p><b>C：</b>分币种处理，SOL 用 B，BTC / ETH / DOGE 用 A，BNB 回抽做空不改。</p>
    <p><b>D：</b>在 A / B / C 上分别扫止损 ATR 1.5 / 2.0 / 2.5 / 3.0。</p>
  </div>

  <div class="section">
    <h2>A / B / C 基线对比（止损 2ATR）</h2>
    {table_html(stop_2)}
  </div>

  <div class="section">
    <h2>D 止损矩阵总表</h2>
    <div class="scroll">{table_html(matrix_rank)}</div>
  </div>

  <div class="section">
    <h2>每种方案的最佳止损档</h2>
    {table_html(best_by_scenario)}
  </div>
</body>
</html>"""

    HTML_REPORT.write_text(html_text, encoding="utf-8")
    MANIFEST_JSON.write_text(
        json.dumps(
            {
                "generated_at": datetime.now().isoformat(timespec="seconds"),
                "summary_csv": str(SUMMARY_CSV),
                "detail_csv": str(DETAIL_CSV),
                "report_html": str(HTML_REPORT),
                "variants": len(results),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    print(HTML_REPORT)


if __name__ == "__main__":
    main()
