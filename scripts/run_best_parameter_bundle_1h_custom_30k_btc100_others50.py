from __future__ import annotations

import json
import sys
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


OUTPUT_DIR = ROOT / "reports" / "best_parameter_bundle_1h_custom_30k_btc100_others50"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

REPORT_HTML = OUTPUT_DIR / "report.html"
TRADES_CSV = OUTPUT_DIR / "trades.csv"
SUMMARY_CSV = OUTPUT_DIR / "summary.csv"
EQUITY_CURVE_CSV = OUTPUT_DIR / "equity_curve.csv"
MONTHLY_RETURNS_CSV = OUTPUT_DIR / "monthly_returns.csv"
YEARLY_RETURNS_CSV = OUTPUT_DIR / "yearly_returns.csv"
LEVERAGE_DIAGNOSTICS_CSV = OUTPUT_DIR / "leverage_diagnostics.csv"

INITIAL_CAPITAL = Decimal("30000")
DEFAULT_RISK_AMOUNT = Decimal("50")
BTC_RISK_AMOUNT = Decimal("100")
ASSUMED_LEVERAGE = Decimal("20")


def risk_for_candidate(candidate: base.CandidateTrade) -> Decimal:
    return BTC_RISK_AMOUNT if candidate.coin.upper() == "BTC" else DEFAULT_RISK_AMOUNT


def simulate_custom_portfolio(
    *,
    candidates: list[base.CandidateTrade],
    initial_capital: Decimal,
    fee_multiplier: Decimal = Decimal("1"),
    slippage_multiplier: Decimal = Decimal("1"),
) -> dict[str, list[Any]]:
    ordered = sorted(candidates, key=lambda item: (item.entry_ts, item.exit_ts, item.coin, item.side))
    open_positions: list[dict[str, Any]] = []
    executed: list[base.ExecutedTrade] = []
    rejected: list[base.RejectedSignal] = []
    equity = initial_capital
    trade_no = 0

    def close_until(ts: int) -> None:
        nonlocal equity
        closable = sorted(
            [item for item in open_positions if int(item["exit_ts"]) <= ts],
            key=lambda item: (int(item["exit_ts"]), int(item["entry_ts"]), str(item["coin"]), str(item["side"])),
        )
        for position in closable:
            equity += Decimal(position["scaled_pnl"])
            executed.append(
                base.ExecutedTrade(
                    trade_no=int(position["trade_no"]),
                    candidate_id=str(position["candidate_id"]),
                    profile_id=str(position["profile_id"]),
                    strategy_name=str(position["strategy_name"]),
                    strategy_id=str(position["strategy_id"]),
                    symbol=str(position["symbol"]),
                    coin=str(position["coin"]),
                    side=str(position["side"]),
                    entry_ts=int(position["entry_ts"]),
                    exit_ts=int(position["exit_ts"]),
                    entry_price=Decimal(position["entry_price"]),
                    exit_price=Decimal(position["exit_price"]),
                    stop_loss=Decimal(position["stop_loss"]),
                    take_profit=Decimal(position["take_profit"]),
                    scaled_size=Decimal(position["scaled_size"]),
                    scaled_notional=Decimal(position["scaled_notional"]),
                    scaled_risk_value=Decimal(position["scaled_risk_value"]),
                    scaled_gross_pnl=Decimal(position["scaled_gross_pnl"]),
                    scaled_pnl=Decimal(position["scaled_pnl"]),
                    scaled_entry_fee=Decimal(position["scaled_entry_fee"]),
                    scaled_exit_fee=Decimal(position["scaled_exit_fee"]),
                    scaled_total_fee=Decimal(position["scaled_total_fee"]),
                    scaled_slippage_cost=Decimal(position["scaled_slippage_cost"]),
                    scaled_funding_cost=Decimal(position["scaled_funding_cost"]),
                    r_multiple=Decimal(position["r_multiple"]),
                    exit_reason=str(position["exit_reason"]),
                    exit_reason_label=str(position["exit_reason_label"]),
                    fee_model=str(position["fee_model"]),
                    capital_before_entry=Decimal(position["capital_before_entry"]),
                    capital_after_exit=equity,
                )
            )
            open_positions.remove(position)

    for candidate in ordered:
        close_until(candidate.entry_ts)
        if candidate.base_risk_value <= 0:
            rejected.append(
                base.RejectedSignal(
                    candidate_id=candidate.candidate_id,
                    profile_id=candidate.profile_id,
                    strategy_name=candidate.strategy_name,
                    symbol=candidate.symbol,
                    coin=candidate.coin,
                    side=candidate.side,
                    entry_ts=candidate.entry_ts,
                    reason="原始风险值无效",
                    capital_snapshot=equity,
                )
            )
            continue
        if equity <= 0:
            rejected.append(
                base.RejectedSignal(
                    candidate_id=candidate.candidate_id,
                    profile_id=candidate.profile_id,
                    strategy_name=candidate.strategy_name,
                    symbol=candidate.symbol,
                    coin=candidate.coin,
                    side=candidate.side,
                    entry_ts=candidate.entry_ts,
                    reason="权益已归零",
                    capital_snapshot=equity,
                )
            )
            continue

        target_risk = risk_for_candidate(candidate)
        scale = Decimal("0") if target_risk <= 0 else target_risk / candidate.base_risk_value
        scaled_notional = candidate.base_notional * scale
        if scaled_notional <= 0:
            rejected.append(
                base.RejectedSignal(
                    candidate_id=candidate.candidate_id,
                    profile_id=candidate.profile_id,
                    strategy_name=candidate.strategy_name,
                    symbol=candidate.symbol,
                    coin=candidate.coin,
                    side=candidate.side,
                    entry_ts=candidate.entry_ts,
                    reason="仓位规模无效",
                    capital_snapshot=equity,
                )
            )
            continue

        trade_no += 1
        scaled_entry_fee = candidate.base_entry_fee * scale * fee_multiplier
        scaled_exit_fee = candidate.base_exit_fee * scale * fee_multiplier
        scaled_total_fee = candidate.base_total_fee * scale * fee_multiplier
        scaled_slippage_cost = candidate.base_slippage_cost * scale * slippage_multiplier
        scaled_funding_cost = candidate.base_funding_cost * scale
        scaled_gross_pnl = candidate.base_gross_pnl * scale
        scaled_pnl = scaled_gross_pnl - scaled_total_fee - scaled_slippage_cost - scaled_funding_cost

        open_positions.append(
            {
                "trade_no": trade_no,
                "candidate_id": candidate.candidate_id,
                "profile_id": candidate.profile_id,
                "strategy_name": candidate.strategy_name,
                "strategy_id": candidate.strategy_id,
                "symbol": candidate.symbol,
                "coin": candidate.coin,
                "side": candidate.side,
                "entry_ts": candidate.entry_ts,
                "exit_ts": candidate.exit_ts,
                "entry_price": candidate.entry_price,
                "exit_price": candidate.exit_price,
                "stop_loss": candidate.stop_loss,
                "take_profit": candidate.take_profit,
                "scaled_size": candidate.size * scale,
                "scaled_notional": scaled_notional,
                "scaled_risk_value": candidate.base_risk_value * scale,
                "scaled_gross_pnl": scaled_gross_pnl,
                "scaled_pnl": scaled_pnl,
                "scaled_entry_fee": scaled_entry_fee,
                "scaled_exit_fee": scaled_exit_fee,
                "scaled_total_fee": scaled_total_fee,
                "scaled_slippage_cost": scaled_slippage_cost,
                "scaled_funding_cost": scaled_funding_cost,
                "r_multiple": candidate.r_multiple,
                "exit_reason": candidate.exit_reason,
                "exit_reason_label": candidate.exit_reason_label,
                "fee_model": candidate.fee_model,
                "capital_before_entry": equity,
            }
        )

    close_until(10**18)
    executed = sorted(executed, key=lambda item: (item.exit_ts, item.entry_ts, item.coin, item.side))
    return {"executed_trades": executed, "rejected_signals": rejected}


def build_custom_stress_table(
    candidates: list[base.CandidateTrade],
    *,
    fee_multipliers: list[int],
    slippage_multiplier: Decimal | None,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    if slippage_multiplier is not None:
        for fee_mult in fee_multipliers:
            result = simulate_custom_portfolio(
                candidates=candidates,
                initial_capital=INITIAL_CAPITAL,
                fee_multiplier=Decimal(str(fee_mult)),
                slippage_multiplier=slippage_multiplier,
            )
            executed_df = base.build_executed_trade_frame(result["executed_trades"])
            metrics = base.compute_trade_metrics(executed_df, INITIAL_CAPITAL)
            rows.append(
                {
                    "测试项": f"手续费 {fee_mult}x",
                    "总收益率": round(metrics["total_return_pct"] * 100.0, 2),
                    "最大回撤": round(metrics["max_drawdown_pct"] * 100.0, 2),
                    "最终收益U": round(metrics["total_pnl"], 2),
                }
            )
    else:
        for slip_mult in (0, 1, 2):
            result = simulate_custom_portfolio(
                candidates=candidates,
                initial_capital=INITIAL_CAPITAL,
                fee_multiplier=Decimal("1"),
                slippage_multiplier=Decimal(str(slip_mult)),
            )
            executed_df = base.build_executed_trade_frame(result["executed_trades"])
            metrics = base.compute_trade_metrics(executed_df, INITIAL_CAPITAL)
            rows.append(
                {
                    "测试项": f"滑点 {slip_mult}x",
                    "总收益率": round(metrics["total_return_pct"] * 100.0, 2),
                    "最大回撤": round(metrics["max_drawdown_pct"] * 100.0, 2),
                    "最终收益U": round(metrics["total_pnl"], 2),
                }
            )
    return pd.DataFrame(rows)


def build_leverage_diagnostics(executed_trades: list[base.ExecutedTrade], *, leverage: Decimal) -> pd.DataFrame:
    # Treat positions as active on [entry_ts, exit_ts). This prevents same-timestamp
    # stop-outs from lingering in the open-position set.
    timeline: dict[int, dict[str, Any]] = {}
    for trade in executed_trades:
        if trade.exit_ts <= trade.entry_ts:
            continue
        entry_bucket = timeline.setdefault(
            trade.entry_ts,
            {"entry_notional": Decimal("0"), "entry_count": 0, "exit_notional": Decimal("0"), "exit_count": 0, "exit_equity": None},
        )
        entry_bucket["entry_notional"] += trade.scaled_notional
        entry_bucket["entry_count"] += 1

        exit_bucket = timeline.setdefault(
            trade.exit_ts,
            {"entry_notional": Decimal("0"), "entry_count": 0, "exit_notional": Decimal("0"), "exit_count": 0, "exit_equity": None},
        )
        exit_bucket["exit_notional"] += trade.scaled_notional
        exit_bucket["exit_count"] += 1
        exit_bucket["exit_equity"] = trade.capital_after_exit

    realized_equity = INITIAL_CAPITAL
    current_notional = Decimal("0")
    current_positions = 0
    max_notional = Decimal("0")
    max_margin = Decimal("0")
    worst_margin_ratio = Decimal("0")
    max_positions = 0
    snapshot_ts = 0
    snapshot_equity = INITIAL_CAPITAL

    for ts in sorted(timeline):
        bucket = timeline[ts]
        if bucket["exit_equity"] is not None:
            realized_equity = Decimal(bucket["exit_equity"])
        current_notional -= Decimal(bucket["exit_notional"])
        current_positions -= int(bucket["exit_count"])
        current_notional += Decimal(bucket["entry_notional"])
        current_positions += int(bucket["entry_count"])

        current_margin = current_notional / leverage if leverage > 0 else Decimal("0")
        current_ratio = Decimal("0") if realized_equity <= 0 else current_margin / realized_equity

        if current_notional > max_notional:
            max_notional = current_notional
            max_margin = current_margin
            max_positions = current_positions
            snapshot_ts = ts
            snapshot_equity = realized_equity
        if current_ratio > worst_margin_ratio:
            worst_margin_ratio = current_ratio

    safe_buffer = snapshot_equity - max_margin
    rows = [
        {"指标": "假设杠杆倍数", "数值": float(leverage)},
        {"指标": "最大同时名义仓位U", "数值": float(max_notional)},
        {"指标": "对应保证金占用U", "数值": float(max_margin)},
        {"指标": "峰值时已实现权益U", "数值": float(snapshot_equity)},
        {"指标": "峰值时剩余缓冲U", "数值": float(safe_buffer)},
        {"指标": "最差保证金占用比", "数值": float(worst_margin_ratio * Decimal('100'))},
        {"指标": "峰值同时持仓数", "数值": int(max_positions)},
        {"指标": "峰值时间", "数值": pd.to_datetime(snapshot_ts, unit='ms', utc=True).tz_convert('Asia/Shanghai').strftime('%Y-%m-%d %H:%M:%S') if snapshot_ts else ""},
    ]
    return pd.DataFrame(rows)


def main() -> None:
    bundle = read_strategy_bundle(base.PACKAGE_PATH)
    client = OkxRestClient()
    candidates, data_ranges, assumptions = base.build_candidate_trades(
        bundle_path=base.PACKAGE_PATH,
        client=client,
        bundle=bundle,
        base_initial_capital=INITIAL_CAPITAL,
        base_risk_amount=DEFAULT_RISK_AMOUNT,
    )
    assumptions.update(
        {
            "standard_mode": "30000U启动资金 | BTC 100U风险 | 其他50U风险",
            "capital_constraints_enabled": False,
            "initial_capital": str(INITIAL_CAPITAL),
            "risk_scheme": "BTC 100U / 其他币种 50U",
            "compounding": False,
            "assumed_leverage": "20x（仅做保证金占用估算，未模拟强平）",
        }
    )

    simulation = simulate_custom_portfolio(
        candidates=candidates,
        initial_capital=INITIAL_CAPITAL,
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
    monthly_wide.to_csv(MONTHLY_RETURNS_CSV, index=False, encoding="utf-8-sig")
    yearly_table.to_csv(YEARLY_RETURNS_CSV, index=False, encoding="utf-8-sig")

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

    leverage_diagnostics = build_leverage_diagnostics(simulation["executed_trades"], leverage=ASSUMED_LEVERAGE)
    leverage_diagnostics.to_csv(LEVERAGE_DIAGNOSTICS_CSV, index=False, encoding="utf-8-sig")

    stress_fee = build_custom_stress_table(
        candidates,
        fee_multipliers=[0, 1, 2, 3],
        slippage_multiplier=Decimal("1"),
    )
    stress_slippage = build_custom_stress_table(
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
        base.build_html_report(
            bundle_name=f"{bundle.bundle_name} | 30000U自定义风险口径",
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
        "leverage_diagnostics_csv": str(LEVERAGE_DIAGNOSTICS_CSV),
        "initial_capital": str(INITIAL_CAPITAL),
        "risk_scheme": "BTC 100U / 其他50U",
        "assumed_leverage": "20x (estimate only)",
        "constraints_enabled": False,
    }
    (OUTPUT_DIR / "run_manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(REPORT_HTML)


if __name__ == "__main__":
    main()
