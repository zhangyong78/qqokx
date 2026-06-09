from __future__ import annotations

import json
import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import scripts.run_best_parameter_bundle_1h_standard_portfolio as base
from okx_quant.okx_client import OkxRestClient
from okx_quant.strategy_profiles import read_strategy_bundle


OUTPUT_DIR = ROOT / "reports" / "best_parameter_bundle_1h_standard_100u"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

REPORT_HTML = OUTPUT_DIR / "report.html"
TRADES_CSV = OUTPUT_DIR / "trades.csv"
SUMMARY_CSV = OUTPUT_DIR / "summary.csv"
EQUITY_CURVE_CSV = OUTPUT_DIR / "equity_curve.csv"
MONTHLY_RETURNS_CSV = OUTPUT_DIR / "monthly_returns.csv"
YEARLY_RETURNS_CSV = OUTPUT_DIR / "yearly_returns.csv"
MONTHLY_BREAKDOWN_CSV = OUTPUT_DIR / "monthly_side_coin_breakdown.csv"
YEARLY_BREAKDOWN_CSV = OUTPUT_DIR / "yearly_side_coin_breakdown.csv"

INITIAL_CAPITAL = Decimal("10000")
FIXED_RISK_AMOUNT = Decimal("100")


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
    assumptions.update(
        {
            "standard_mode": "100U固定风险",
            "capital_constraints_enabled": False,
            "risk_amount": str(FIXED_RISK_AMOUNT),
            "initial_capital": str(INITIAL_CAPITAL),
            "compounding": False,
        }
    )

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
    equity_hourly.to_csv(EQUITY_CURVE_CSV, index=False, encoding="utf-8-sig")

    executed_df = base.build_executed_trade_frame(simulation["executed_trades"])
    trades_export = base.build_trades_export(executed_df)
    if "累计利润" not in trades_export.columns and "盈亏" in trades_export.columns:
        insert_at = trades_export.columns.get_loc("盈亏") + 1
        trades_export.insert(insert_at, "累计利润", trades_export["盈亏"].astype(float).cumsum().round(2))
    trades_export.to_csv(TRADES_CSV, index=False, encoding="utf-8-sig")

    overall_metrics = base.compute_trade_metrics(executed_df, INITIAL_CAPITAL)
    drawdown_meta = base.compute_drawdown_metadata(equity_hourly)
    utilization = base.compute_utilization(
        executed_trades=simulation["executed_trades"],
        start_ts=start_ts,
        end_ts=end_ts,
        initial_capital=INITIAL_CAPITAL,
    )
    monthly_wide, monthly_detail = base.build_monthly_returns_table(equity_hourly)
    yearly_table = base.build_yearly_returns_table(equity_hourly, executed_df)
    monthly_breakdown = base.build_period_profit_breakdown(executed_df, period="monthly")
    yearly_breakdown = base.build_period_profit_breakdown(executed_df, period="yearly")
    monthly_wide.to_csv(MONTHLY_RETURNS_CSV, index=False, encoding="utf-8-sig")
    yearly_table.to_csv(YEARLY_RETURNS_CSV, index=False, encoding="utf-8-sig")
    monthly_breakdown.to_csv(MONTHLY_BREAKDOWN_CSV, index=False, encoding="utf-8-sig")
    yearly_breakdown.to_csv(YEARLY_BREAKDOWN_CSV, index=False, encoding="utf-8-sig")

    side_summary = base.build_side_summary(executed_df)
    symbol_summary = base.build_symbol_summary(executed_df)
    strategy_summary = base.build_strategy_summary(executed_df)
    rejection_summary = base.build_rejection_summary(simulation["rejected_signals"])
    summary_export = base.build_summary_export(
        overall_metrics=overall_metrics,
        side_summary=side_summary,
        symbol_summary=symbol_summary,
        strategy_summary=strategy_summary,
        rejection_summary=rejection_summary,
        utilization=utilization,
    )
    summary_export.to_csv(SUMMARY_CSV, index=False, encoding="utf-8-sig")

    stress_fee = base.build_stress_table(
        candidates,
        fee_multipliers=[0, 1, 2, 3],
        slippage_multiplier=Decimal("1"),
    )
    stress_slippage = base.build_stress_table(
        candidates,
        fee_multipliers=[1],
        slippage_multiplier=None,
    )
    regime_table = base.build_regime_summary(equity_hourly, executed_df)
    correlation_matrix = base.build_coin_correlation_matrix(executed_df)
    auto_summary = base.build_auto_summary(
        executed_df=executed_df,
        overall_metrics=overall_metrics,
        side_summary=side_summary,
        strategy_summary=strategy_summary,
        symbol_summary=symbol_summary,
        rejection_summary=rejection_summary,
        drawdown_meta=drawdown_meta,
    )

    REPORT_HTML.write_text(
        base.build_html_report_extended(
            bundle_name=f"{bundle.bundle_name} | 标准100U口径",
            assumptions=assumptions,
            data_ranges=data_ranges,
            overall_metrics=overall_metrics,
            drawdown_meta=drawdown_meta,
            utilization=utilization,
            auto_summary=auto_summary,
            side_summary=side_summary,
            symbol_summary=symbol_summary,
            strategy_summary=strategy_summary,
            rejection_summary=rejection_summary,
            monthly_wide=monthly_wide,
            yearly_table=yearly_table,
            stress_fee=stress_fee,
            stress_slippage=stress_slippage,
            regime_table=regime_table,
            correlation_matrix=correlation_matrix,
            trades_export=trades_export,
            equity_hourly=equity_hourly,
            monthly_detail=monthly_detail,
            monthly_breakdown=monthly_breakdown,
            yearly_breakdown=yearly_breakdown,
        ),
        encoding="utf-8",
    )

    manifest = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "output_dir": str(OUTPUT_DIR),
        "report_html": str(REPORT_HTML),
        "trades_csv": str(TRADES_CSV),
        "summary_csv": str(SUMMARY_CSV),
        "equity_curve_csv": str(EQUITY_CURVE_CSV),
        "monthly_returns_csv": str(MONTHLY_RETURNS_CSV),
        "yearly_returns_csv": str(YEARLY_RETURNS_CSV),
        "monthly_breakdown_csv": str(MONTHLY_BREAKDOWN_CSV),
        "yearly_breakdown_csv": str(YEARLY_BREAKDOWN_CSV),
        "initial_capital": str(INITIAL_CAPITAL),
        "risk_amount": str(FIXED_RISK_AMOUNT),
        "constraints_enabled": False,
    }
    (OUTPUT_DIR / "run_manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(REPORT_HTML)


if __name__ == "__main__":
    main()
