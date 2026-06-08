from __future__ import annotations

import json
import sys
from dataclasses import asdict
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import scripts.run_best_parameter_bundle_1h_standard_portfolio as base
from okx_quant.okx_client import OkxRestClient
from okx_quant.strategy_profiles import read_strategy_bundle


OUTPUT_DIR = ROOT / "reports" / "best_parameter_bundle_1h_constraint_compare"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

SUMMARY_CSV = OUTPUT_DIR / "comparison_summary.csv"
MARKDOWN_PATH = OUTPUT_DIR / "comparison_report.md"
JSON_PATH = OUTPUT_DIR / "comparison_payload.json"


def main() -> None:
    bundle = read_strategy_bundle(base.PACKAGE_PATH)
    client = OkxRestClient()
    candidates, data_ranges, assumptions = base.build_candidate_trades(
        bundle_path=base.PACKAGE_PATH,
        client=client,
        bundle=bundle,
    )

    start_ts = min(item["start_ts"] for item in data_ranges.values())
    end_ts = max(item["end_ts"] for item in data_ranges.values())

    scenarios: list[dict[str, Any]] = []

    dev_exec = simulate_unconstrained_dev(candidates, dev_initial_capital=Decimal("10000"))
    scenarios.append(
        build_scenario_summary(
            scenario_name="开发口径_无约束_固定10U",
            description="所有信号全部成交；不做统一资金池竞争；沿用开发期每笔固定10U风险的原始成交结果。",
            initial_capital=Decimal("10000"),
            executed=dev_exec,
            rejected_count=0,
            start_ts=start_ts,
            end_ts=end_ts,
        )
    )

    unified_free = base.simulate_portfolio(
        candidates=candidates,
        initial_capital=base.INITIAL_CAPITAL,
        risk_per_trade=base.RISK_PER_TRADE,
        max_positions=999999,
        max_long_positions=999999,
        max_short_positions=999999,
        max_total_exposure=Decimal("1000000"),
        max_symbol_exposure=Decimal("1000000"),
    )
    scenarios.append(
        build_scenario_summary(
            scenario_name="统一资金池_无约束_100k_1pct",
            description="统一资金池100000U，每笔按权益1%风险缩放，但不限制总仓位、多空仓位、总暴露、单币种暴露。",
            initial_capital=base.INITIAL_CAPITAL,
            executed=unified_free["executed_trades"],
            rejected_count=len(unified_free["rejected_signals"]),
            start_ts=start_ts,
            end_ts=end_ts,
        )
    )

    unified_standard = base.simulate_portfolio(
        candidates=candidates,
        initial_capital=base.INITIAL_CAPITAL,
        risk_per_trade=base.RISK_PER_TRADE,
        max_positions=base.MAX_POSITIONS,
        max_long_positions=base.MAX_LONG_POSITIONS,
        max_short_positions=base.MAX_SHORT_POSITIONS,
        max_total_exposure=base.MAX_TOTAL_EXPOSURE,
        max_symbol_exposure=base.MAX_SYMBOL_EXPOSURE,
    )
    scenarios.append(
        build_scenario_summary(
            scenario_name="统一资金池_正式约束_100k_1pct",
            description="统一资金池100000U，每笔按权益1%风险缩放，并启用总持仓、多空持仓、总暴露、单币种暴露正式约束。",
            initial_capital=base.INITIAL_CAPITAL,
            executed=unified_standard["executed_trades"],
            rejected_count=len(unified_standard["rejected_signals"]),
            start_ts=start_ts,
            end_ts=end_ts,
        )
    )

    summary_df = pd.DataFrame(scenarios)
    summary_df.to_csv(SUMMARY_CSV, index=False, encoding="utf-8-sig")

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "package_path": str(base.PACKAGE_PATH),
        "output_dir": str(OUTPUT_DIR),
        "summary_csv": str(SUMMARY_CSV),
        "scenarios": scenarios,
        "assumptions": assumptions,
    }
    JSON_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    MARKDOWN_PATH.write_text(build_markdown_report(summary_df), encoding="utf-8")
    print(SUMMARY_CSV)


def simulate_unconstrained_dev(candidates: list[base.CandidateTrade], *, dev_initial_capital: Decimal) -> list[base.ExecutedTrade]:
    ordered = sorted(candidates, key=lambda item: (item.exit_ts, item.entry_ts, item.coin, item.side))
    executed: list[base.ExecutedTrade] = []
    running = dev_initial_capital
    for trade_no, candidate in enumerate(ordered, start=1):
        capital_before = running
        running += candidate.base_pnl
        executed.append(
            base.ExecutedTrade(
                trade_no=trade_no,
                candidate_id=candidate.candidate_id,
                profile_id=candidate.profile_id,
                strategy_name=candidate.strategy_name,
                strategy_id=candidate.strategy_id,
                symbol=candidate.symbol,
                coin=candidate.coin,
                side=candidate.side,
                entry_ts=candidate.entry_ts,
                exit_ts=candidate.exit_ts,
                entry_price=candidate.entry_price,
                exit_price=candidate.exit_price,
                stop_loss=candidate.stop_loss,
                take_profit=candidate.take_profit,
                scaled_size=candidate.size,
                scaled_notional=candidate.base_notional,
                scaled_risk_value=candidate.base_risk_value,
                scaled_gross_pnl=candidate.base_gross_pnl,
                scaled_pnl=candidate.base_pnl,
                scaled_entry_fee=candidate.base_entry_fee,
                scaled_exit_fee=candidate.base_exit_fee,
                scaled_total_fee=candidate.base_total_fee,
                scaled_slippage_cost=candidate.base_slippage_cost,
                scaled_funding_cost=candidate.base_funding_cost,
                r_multiple=candidate.r_multiple,
                exit_reason=candidate.exit_reason,
                exit_reason_label=candidate.exit_reason_label,
                fee_model=candidate.fee_model,
                capital_before_entry=capital_before,
                capital_after_exit=running,
            )
        )
    return executed


def build_scenario_summary(
    *,
    scenario_name: str,
    description: str,
    initial_capital: Decimal,
    executed: list[base.ExecutedTrade],
    rejected_count: int,
    start_ts: int,
    end_ts: int,
) -> dict[str, Any]:
    executed_df = base.build_executed_trade_frame(executed)
    metrics = base.compute_trade_metrics(executed_df, initial_capital)
    equity = base.build_hourly_equity_curve(
        start_ts=start_ts,
        end_ts=end_ts,
        initial_capital=initial_capital,
        executed_trades=executed,
    )
    drawdown = base.compute_drawdown_metadata(equity)
    annual_return = base.annualized_return(equity)
    sharpe = base.sharpe_ratio(equity)
    calmar = base.calmar_ratio(annual_return, drawdown["max_drawdown_pct"])
    long_pnl = float(executed_df.loc[executed_df["side"] == "多头", "pnl"].sum()) if not executed_df.empty else 0.0
    short_pnl = float(executed_df.loc[executed_df["side"] == "空头", "pnl"].sum()) if not executed_df.empty else 0.0
    first_trade_risk = float(executed[0].scaled_risk_value) if executed else 0.0
    return {
        "场景": scenario_name,
        "说明": description,
        "初始资金": float(initial_capital),
        "首笔风险金额": round(first_trade_risk, 2),
        "交易次数": int(metrics["trades"]),
        "拒绝信号数": int(rejected_count),
        "总收益U": round(metrics["total_pnl"], 2),
        "总收益率%": round(metrics["total_return_pct"] * 100.0, 2),
        "最大回撤%": round(drawdown["max_drawdown_pct"] * 100.0, 2),
        "胜率%": round(metrics["win_rate"] * 100.0, 2),
        "ProfitFactor": round(metrics["profit_factor"], 4),
        "平均R": round(metrics["avg_r"], 4),
        "年化收益率%": round(annual_return * 100.0, 2),
        "夏普": round(sharpe, 4),
        "卡玛": round(calmar, 4),
        "多头贡献U": round(long_pnl, 2),
        "空头贡献U": round(short_pnl, 2),
        "最大回撤开始": drawdown["start_time"],
        "最大回撤结束": drawdown["end_time"],
    }


def build_markdown_report(summary_df: pd.DataFrame) -> str:
    if summary_df.empty:
        return "# 约束对比报告\n\n无数据。"
    lines = [
        "# 最佳参数组合包 1H 约束对比报告",
        "",
        f"- 生成时间：`{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`",
        f"- 组合包：`{base.PACKAGE_PATH}`",
        "",
        "## 核心对比",
        "",
    ]
    for _, row in summary_df.iterrows():
        lines.extend(
            [
                f"### {row['场景']}",
                "",
                f"- 说明：{row['说明']}",
                f"- 初始资金：`{row['初始资金']}`",
                f"- 首笔风险金额：`{row['首笔风险金额']}`",
                f"- 交易次数：`{row['交易次数']}`",
                f"- 拒绝信号数：`{row['拒绝信号数']}`",
                f"- 总收益：`{row['总收益U']}U`",
                f"- 总收益率：`{row['总收益率%']}%`",
                f"- 最大回撤：`{row['最大回撤%']}%`",
                f"- 胜率：`{row['胜率%']}%`",
                f"- Profit Factor：`{row['ProfitFactor']}`",
                f"- 多头贡献：`{row['多头贡献U']}U`",
                f"- 空头贡献：`{row['空头贡献U']}U`",
                "",
            ]
        )
    if len(summary_df) >= 3:
        dev_row = summary_df.iloc[0]
        free_row = summary_df.iloc[1]
        std_row = summary_df.iloc[2]
        risk_factor = 0.0
        if float(dev_row["首笔风险金额"]) > 0:
            risk_factor = float(free_row["首笔风险金额"]) / float(dev_row["首笔风险金额"])
        lines.extend(
            [
                "## 诊断结论",
                "",
                f"- 从开发口径到统一资金池无约束口径，首笔风险金额从 `{dev_row['首笔风险金额']}` 放大到 `{free_row['首笔风险金额']}`，约为 `{risk_factor:.1f}` 倍。",
                f"- 如果统一资金池无约束结果已经和开发口径差很多，说明主要问题不是仓位限制，而是风险金额口径已经切换。",
                f"- 如果统一资金池无约束和统一资金池正式约束差很多，说明主要问题是资金竞争、总暴露和单币种暴露限制。",
                f"- 本次正式约束场景被拒信号 `{std_row['拒绝信号数']}` 次，可直接用来判断约束对结果的影响有多大。",
                "",
            ]
        )
    return "\n".join(lines)


if __name__ == "__main__":
    main()
