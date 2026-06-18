from __future__ import annotations

import base64
import html
import io
import json
import math
import sys
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from okx_quant.backtest import BacktestTrade, _run_backtest_with_loaded_data, format_trade_exit_reason
from okx_quant.candle_cache import load_candle_cache
from okx_quant.models import StrategyConfig, normalize_dynamic_protection_rules
from okx_quant.okx_client import OkxRestClient
from okx_quant.persistence import analysis_report_dir_path
from okx_quant.strategy_profiles import read_strategy_bundle
from scripts.run_btc_daily_ma_direction_filter_research import (
    LONG_MAKER_FEE_RATE,
    LONG_TAKER_FEE_RATE,
    SHORT_TAKER_FEE_RATE,
)


plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "Noto Sans CJK SC", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False


PACKAGE_PATH = analysis_report_dir_path() / "packages" / "最佳参数组合包.json"
OUTPUT_DIR = ROOT / "reports" / "best_parameter_bundle_1h_full_standard"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

REPORT_HTML = OUTPUT_DIR / "report.html"
TRADES_CSV = OUTPUT_DIR / "trades.csv"
SUMMARY_CSV = OUTPUT_DIR / "summary.csv"
EQUITY_CURVE_CSV = OUTPUT_DIR / "equity_curve.csv"
MONTHLY_RETURNS_CSV = OUTPUT_DIR / "monthly_returns.csv"
YEARLY_RETURNS_CSV = OUTPUT_DIR / "yearly_returns.csv"

INITIAL_CAPITAL = Decimal("100000")
RISK_PER_TRADE = Decimal("0.01")
MAX_POSITIONS = 10
MAX_LONG_POSITIONS = 6
MAX_SHORT_POSITIONS = 6
MAX_TOTAL_EXPOSURE = Decimal("1.0")
MAX_SYMBOL_EXPOSURE = Decimal("0.35")
FORMAL_SLIPPAGE = Decimal("0.0003")
FORMAL_FUNDING_RATE = Decimal("0")
CALENDAR_DAYS = Decimal("365")


@dataclass(frozen=True)
class CandidateTrade:
    candidate_id: str
    profile_id: str
    strategy_name: str
    strategy_id: str
    symbol: str
    coin: str
    side: str
    entry_ts: int
    exit_ts: int
    entry_price: Decimal
    exit_price: Decimal
    stop_loss: Decimal
    take_profit: Decimal
    size: Decimal
    base_notional: Decimal
    base_gross_pnl: Decimal
    base_pnl: Decimal
    base_risk_value: Decimal
    r_multiple: Decimal
    exit_reason: str
    exit_reason_label: str
    base_entry_fee: Decimal
    base_exit_fee: Decimal
    base_total_fee: Decimal
    base_slippage_cost: Decimal
    base_funding_cost: Decimal
    data_start_ts: int
    data_end_ts: int
    candle_count: int
    fee_model: str
    wave_entry_sequence: int = 0


@dataclass(frozen=True)
class ExecutedTrade:
    trade_no: int
    candidate_id: str
    profile_id: str
    strategy_name: str
    strategy_id: str
    symbol: str
    coin: str
    side: str
    entry_ts: int
    exit_ts: int
    entry_price: Decimal
    exit_price: Decimal
    stop_loss: Decimal
    take_profit: Decimal
    scaled_size: Decimal
    scaled_notional: Decimal
    scaled_risk_value: Decimal
    scaled_gross_pnl: Decimal
    scaled_pnl: Decimal
    scaled_entry_fee: Decimal
    scaled_exit_fee: Decimal
    scaled_total_fee: Decimal
    scaled_slippage_cost: Decimal
    scaled_funding_cost: Decimal
    r_multiple: Decimal
    exit_reason: str
    exit_reason_label: str
    fee_model: str
    capital_before_entry: Decimal
    capital_after_exit: Decimal
    wave_entry_sequence: int = 0


@dataclass(frozen=True)
class RejectedSignal:
    candidate_id: str
    profile_id: str
    strategy_name: str
    symbol: str
    coin: str
    side: str
    entry_ts: int
    reason: str
    capital_snapshot: Decimal


def main() -> None:
    if not PACKAGE_PATH.exists():
        raise FileNotFoundError(f"未找到最佳参数组合包：{PACKAGE_PATH}")

    bundle = read_strategy_bundle(PACKAGE_PATH)
    client = OkxRestClient()
    candidates, data_ranges, assumptions = build_candidate_trades(bundle_path=PACKAGE_PATH, client=client, bundle=bundle)

    simulation = simulate_portfolio(
        candidates=candidates,
        initial_capital=INITIAL_CAPITAL,
        risk_per_trade=RISK_PER_TRADE,
        max_positions=MAX_POSITIONS,
        max_long_positions=MAX_LONG_POSITIONS,
        max_short_positions=MAX_SHORT_POSITIONS,
        max_total_exposure=MAX_TOTAL_EXPOSURE,
        max_symbol_exposure=MAX_SYMBOL_EXPOSURE,
    )

    start_ts = min(item["start_ts"] for item in data_ranges.values())
    end_ts = max(item["end_ts"] for item in data_ranges.values())
    equity_hourly = build_hourly_equity_curve(
        start_ts=start_ts,
        end_ts=end_ts,
        initial_capital=INITIAL_CAPITAL,
        executed_trades=simulation["executed_trades"],
    )
    equity_hourly.to_csv(EQUITY_CURVE_CSV, index=False, encoding="utf-8-sig")

    executed_df = build_executed_trade_frame(simulation["executed_trades"])
    trades_export = build_trades_export(executed_df)
    trades_export.to_csv(TRADES_CSV, index=False, encoding="utf-8-sig")

    overall_metrics = compute_trade_metrics(executed_df, INITIAL_CAPITAL)
    drawdown_meta = compute_drawdown_metadata(equity_hourly)
    utilization = compute_utilization(
        executed_trades=simulation["executed_trades"],
        start_ts=start_ts,
        end_ts=end_ts,
        initial_capital=INITIAL_CAPITAL,
    )
    monthly_wide, monthly_detail = build_monthly_returns_table(equity_hourly)
    yearly_table = build_yearly_returns_table(equity_hourly, executed_df)
    monthly_wide.to_csv(MONTHLY_RETURNS_CSV, index=False, encoding="utf-8-sig")
    yearly_table.to_csv(YEARLY_RETURNS_CSV, index=False, encoding="utf-8-sig")

    side_summary = build_side_summary(executed_df)
    symbol_summary = build_symbol_summary(executed_df)
    strategy_summary = build_strategy_summary(executed_df)
    rejection_summary = build_rejection_summary(simulation["rejected_signals"])
    summary_export = build_summary_export(
        overall_metrics=overall_metrics,
        side_summary=side_summary,
        symbol_summary=symbol_summary,
        strategy_summary=strategy_summary,
        rejection_summary=rejection_summary,
        utilization=utilization,
    )
    summary_export.to_csv(SUMMARY_CSV, index=False, encoding="utf-8-sig")

    stress_fee = build_stress_table(candidates, fee_multipliers=[0, 1, 2, 3], slippage_multiplier=1)
    stress_slippage = build_stress_table(candidates, fee_multipliers=[1], slippage_multiplier=None)
    regime_table = build_regime_summary(equity_hourly, executed_df)
    correlation_matrix = build_coin_correlation_matrix(executed_df)
    auto_summary = build_auto_summary(
        executed_df=executed_df,
        overall_metrics=overall_metrics,
        side_summary=side_summary,
        strategy_summary=strategy_summary,
        symbol_summary=symbol_summary,
        rejection_summary=rejection_summary,
        drawdown_meta=drawdown_meta,
    )

    REPORT_HTML.write_text(
        build_html_report(
            bundle_name=bundle.bundle_name,
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

    payload = {
        "bundle_name": bundle.bundle_name,
        "bundle_path": str(PACKAGE_PATH),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "output_dir": str(OUTPUT_DIR),
        "report_html": str(REPORT_HTML),
        "trades_csv": str(TRADES_CSV),
        "summary_csv": str(SUMMARY_CSV),
        "equity_curve_csv": str(EQUITY_CURVE_CSV),
        "monthly_returns_csv": str(MONTHLY_RETURNS_CSV),
        "yearly_returns_csv": str(YEARLY_RETURNS_CSV),
    }
    (OUTPUT_DIR / "run_manifest.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(REPORT_HTML)


def build_candidate_trades(
    *,
    bundle_path: Path,
    client: OkxRestClient,
    bundle,
    base_initial_capital: Decimal = Decimal("10000"),
    base_risk_amount: Decimal = Decimal("10"),
) -> tuple[list[CandidateTrade], dict[str, dict[str, Any]], dict[str, Any]]:
    candidates: list[CandidateTrade] = []
    data_ranges: dict[str, dict[str, Any]] = {}
    for profile in bundle.profiles:
        config = deserialize_strategy_config(profile.config_snapshot)
        config = StrategyConfig(
            **{
                **config.__dict__,
                "bar": "1H",
                "backtest_initial_capital": base_initial_capital,
                "backtest_sizing_mode": "fixed_risk",
                "backtest_compounding": False,
                "backtest_risk_percent": None,
                "risk_amount": base_risk_amount,
                "backtest_entry_slippage_rate": FORMAL_SLIPPAGE,
                "backtest_exit_slippage_rate": FORMAL_SLIPPAGE,
                "backtest_slippage_rate": FORMAL_SLIPPAGE,
                "backtest_funding_rate": FORMAL_FUNDING_RATE,
                "backtest_profile_id": profile.profile_id,
                "backtest_profile_name": profile.profile_name,
                "backtest_profile_summary": profile.notes,
            }
        )
        candles = [item for item in load_candle_cache(profile.symbol, "1H", limit=None) if item.confirmed]
        if not candles:
            raise RuntimeError(f"缺少 {profile.symbol} 1H 已收盘K线")
        instrument = client.get_instrument(profile.symbol)
        maker_fee_rate = LONG_MAKER_FEE_RATE if config.signal_mode == "long_only" else Decimal("0")
        taker_fee_rate = LONG_TAKER_FEE_RATE if config.signal_mode == "long_only" else SHORT_TAKER_FEE_RATE
        result = _run_backtest_with_loaded_data(
            candles,
            instrument,
            config,
            data_source_note=f"最佳参数组合包正式组合回测 | {profile.profile_name}",
            maker_fee_rate=maker_fee_rate,
            taker_fee_rate=taker_fee_rate,
        )
        coin = profile.symbol.replace("-USDT-SWAP", "")
        data_ranges.setdefault(
            profile.symbol,
            {
                "coin": coin,
                "start_ts": candles[0].ts,
                "end_ts": candles[-1].ts,
                "candles": len(candles),
            },
        )
        fee_model = "开仓 Maker / 平仓 Taker" if config.signal_mode == "long_only" else "双边 Taker"
        for index, trade in enumerate(result.trades, start=1):
            candidates.append(
                CandidateTrade(
                    candidate_id=f"{profile.profile_id}_{index:04d}",
                    profile_id=profile.profile_id,
                    strategy_name=profile.profile_name,
                    strategy_id=profile.strategy_id,
                    symbol=profile.symbol,
                    coin=coin,
                    side="多头" if trade.signal == "long" else "空头",
                    entry_ts=int(trade.entry_ts),
                    exit_ts=int(trade.exit_ts),
                    entry_price=trade.entry_price,
                    exit_price=trade.exit_price,
                    stop_loss=trade.stop_loss,
                    take_profit=trade.take_profit,
                    size=trade.size,
                    base_notional=abs(trade.size) * trade.entry_price,
                    base_gross_pnl=trade.gross_pnl,
                    base_pnl=trade.pnl,
                    base_risk_value=abs(trade.risk_value),
                    r_multiple=trade.r_multiple,
                    exit_reason=trade.exit_reason,
                    exit_reason_label=format_trade_exit_reason(trade.exit_reason),
                    base_entry_fee=trade.entry_fee,
                    base_exit_fee=trade.exit_fee,
                    base_total_fee=trade.total_fee,
                    base_slippage_cost=trade.slippage_cost,
                    base_funding_cost=trade.funding_cost,
                    data_start_ts=candles[0].ts,
                    data_end_ts=candles[-1].ts,
                    candle_count=len(candles),
                    fee_model=fee_model,
                    wave_entry_sequence=int(trade.wave_entry_sequence),
                )
            )
    assumptions = {
        "bundle_path": str(bundle_path),
        "base_initial_capital": str(base_initial_capital),
        "base_risk_amount": str(base_risk_amount),
        "initial_capital": str(INITIAL_CAPITAL),
        "risk_per_trade": str(RISK_PER_TRADE),
        "max_positions": MAX_POSITIONS,
        "max_long_positions": MAX_LONG_POSITIONS,
        "max_short_positions": MAX_SHORT_POSITIONS,
        "max_total_exposure": str(MAX_TOTAL_EXPOSURE),
        "max_symbol_exposure": str(MAX_SYMBOL_EXPOSURE),
        "formal_slippage": str(FORMAL_SLIPPAGE),
        "long_fee_model": f"Maker {LONG_MAKER_FEE_RATE} / Taker {LONG_TAKER_FEE_RATE}",
        "short_fee_model": f"Taker {SHORT_TAKER_FEE_RATE}",
        "signal_rule": "当前K线收盘确认，下一根K线开盘成交",
        "note": "max_symbol_exposure 本次按 35% 作为组合默认约束执行。",
    }
    return candidates, data_ranges, assumptions


def deserialize_strategy_config(payload: dict[str, object]) -> StrategyConfig:
    legacy_slippage_rate = Decimal(str(payload.get("backtest_slippage_rate", "0")))
    entry_slippage_rate = (
        legacy_slippage_rate
        if payload.get("backtest_entry_slippage_rate") in (None, "")
        else Decimal(str(payload.get("backtest_entry_slippage_rate")))
    )
    exit_slippage_rate = (
        legacy_slippage_rate
        if payload.get("backtest_exit_slippage_rate") in (None, "")
        else Decimal(str(payload.get("backtest_exit_slippage_rate")))
    )

    def coerce_bool(value: object, default: bool) -> bool:
        if isinstance(value, bool):
            return value
        if value is None:
            return default
        raw = str(value).strip().lower()
        if raw in {"1", "true", "yes", "on", "enabled"}:
            return True
        if raw in {"0", "false", "no", "off", "disabled"}:
            return False
        return default

    rail_candidate_periods_raw = payload.get("rail_candidate_ema_periods", (21, 34, 55, 89))
    if isinstance(rail_candidate_periods_raw, (list, tuple)):
        rail_candidate_periods = tuple(int(item) for item in rail_candidate_periods_raw if int(item) > 0)
    else:
        rail_candidate_periods = (21, 34, 55, 89)

    return StrategyConfig(
        inst_id=str(payload.get("inst_id", "")),
        bar=str(payload.get("bar", "1H")),
        ema_type=str(payload.get("ema_type", "ema")),
        ema_period=int(payload.get("ema_period", 21)),
        trend_ema_type=str(payload.get("trend_ema_type", "ema")),
        trend_ema_period=int(payload.get("trend_ema_period", 55)),
        big_ema_period=int(payload.get("big_ema_period", 233)),
        entry_reference_ema_type=str(payload.get("entry_reference_ema_type", payload.get("ema_type", "ema"))),
        entry_reference_ema_period=int(payload.get("entry_reference_ema_period", 55)),
        atr_period=int(payload.get("atr_period", 14)),
        atr_stop_multiplier=Decimal(str(payload.get("atr_stop_multiplier", "1.5"))),
        atr_take_multiplier=Decimal(str(payload.get("atr_take_multiplier", "4"))),
        order_size=Decimal(str(payload.get("order_size", "0"))),
        trade_mode=str(payload.get("trade_mode", "cross")),
        signal_mode=str(payload.get("signal_mode", "both")),
        position_mode=str(payload.get("position_mode", "net")),
        environment=str(payload.get("environment", "demo")),
        tp_sl_trigger_type=str(payload.get("tp_sl_trigger_type", "mark")),
        strategy_id=str(payload.get("strategy_id", "ema_dynamic_order")),
        poll_seconds=float(payload.get("poll_seconds", 10.0)),
        risk_amount=None if payload.get("risk_amount") in (None, "") else Decimal(str(payload.get("risk_amount"))),
        trade_inst_id=None if payload.get("trade_inst_id") in (None, "") else str(payload.get("trade_inst_id")),
        tp_sl_mode=str(payload.get("tp_sl_mode", "exchange")),
        local_tp_sl_inst_id=None
        if payload.get("local_tp_sl_inst_id") in (None, "")
        else str(payload.get("local_tp_sl_inst_id")),
        entry_side_mode=str(payload.get("entry_side_mode", "follow_signal")),
        run_mode=str(payload.get("run_mode", "trade")),
        take_profit_mode=str(payload.get("take_profit_mode", "dynamic")),
        max_entries_per_trend=int(payload.get("max_entries_per_trend", 1)),
        reentry_confirmation_enabled=coerce_bool(payload.get("reentry_confirmation_enabled"), False),
        reentry_confirmation_min_sequence=int(payload.get("reentry_confirmation_min_sequence", 0)),
        reentry_confirmation_ma_type=str(payload.get("reentry_confirmation_ma_type", "ema")),
        reentry_confirmation_ma_period=int(payload.get("reentry_confirmation_ma_period", 21)),
        dynamic_two_r_break_even=bool(payload.get("dynamic_two_r_break_even", True)),
        dynamic_break_even_trigger_r=int(payload.get("dynamic_break_even_trigger_r", 2)),
        dynamic_fee_offset_enabled=bool(payload.get("dynamic_fee_offset_enabled", True)),
        dynamic_protection_rules=normalize_dynamic_protection_rules(payload.get("dynamic_protection_rules")),
        ema55_slope_lock_profit_trigger_r=int(payload.get("ema55_slope_lock_profit_trigger_r", 5)),
        dynamic_first_lock_r=int(payload.get("dynamic_first_lock_r", 0)),
        dynamic_trailing_step_r=int(payload.get("dynamic_trailing_step_r", 1)),
        trend_ema_slope_filter_enabled=coerce_bool(payload.get("trend_ema_slope_filter_enabled"), True),
        ema55_slope_exit_enabled=coerce_bool(payload.get("ema55_slope_exit_enabled"), True),
        ema55_slope_same_bar_reentry_block=coerce_bool(payload.get("ema55_slope_same_bar_reentry_block"), False),
        ema55_slope_dynamic_exit_requires_bear_reentry=coerce_bool(
            payload.get("ema55_slope_dynamic_exit_requires_bear_reentry"),
            False,
        ),
        ema55_slope_dynamic_exit_bear_reentry_break_prev_low=coerce_bool(
            payload.get("ema55_slope_dynamic_exit_bear_reentry_break_prev_low"),
            False,
        ),
        ema55_slope_dynamic_exit_requires_ema_reclaim=coerce_bool(
            payload.get("ema55_slope_dynamic_exit_requires_ema_reclaim"),
            False,
        ),
        ema55_slope_locked_reentry_requires_ema21_near=coerce_bool(
            payload.get("ema55_slope_locked_reentry_requires_ema21_near"),
            False,
        ),
        ema55_slope_locked_reentry_min_r=int(payload.get("ema55_slope_locked_reentry_min_r", 0)),
        ema55_slope_locked_reentry_max_r=int(payload.get("ema55_slope_locked_reentry_max_r", 0)),
        ema55_slope_dynamic_exit_bull_bar_requires_bear_reentry=coerce_bool(
            payload.get("ema55_slope_dynamic_exit_bull_bar_requires_bear_reentry"),
            False,
        ),
        ema55_slope_dynamic_exit_bull_bar_reentry_min_r=int(
            payload.get("ema55_slope_dynamic_exit_bull_bar_reentry_min_r", 0)
        ),
        ema55_slope_dynamic_exit_bull_bar_reentry_max_r=int(
            payload.get("ema55_slope_dynamic_exit_bull_bar_reentry_max_r", 0)
        ),
        trend_ema_slope_filter_lookback_bars=int(payload.get("trend_ema_slope_filter_lookback_bars", 5)),
        trend_ema_slope_filter_min_ratio=Decimal(str(payload.get("trend_ema_slope_filter_min_ratio", "0"))),
        atr_percentile_filter_max=Decimal(str(payload.get("atr_percentile_filter_max", "0"))),
        body_retest_breakdown_atr_multiplier=Decimal(str(payload.get("body_retest_breakdown_atr_multiplier", "0.2"))),
        body_retest_retest_atr_multiplier=Decimal(str(payload.get("body_retest_retest_atr_multiplier", "0.3"))),
        body_retest_stop_buffer_atr_multiplier=Decimal(
            str(payload.get("body_retest_stop_buffer_atr_multiplier", "0.3"))
        ),
        body_retest_body_atr_limit=Decimal(str(payload.get("body_retest_body_atr_limit", "1.0"))),
        body_retest_watch_bars=int(payload.get("body_retest_watch_bars", 6)),
        time_stop_break_even_enabled=bool(payload.get("time_stop_break_even_enabled", False)),
        time_stop_break_even_bars=int(payload.get("time_stop_break_even_bars", 10)),
        hold_close_exit_bars=int(payload.get("hold_close_exit_bars", 0)),
        mtf_filter_inst_id=None if payload.get("mtf_filter_inst_id") in (None, "") else str(payload.get("mtf_filter_inst_id")),
        mtf_filter_bar=None if payload.get("mtf_filter_bar") in (None, "") else str(payload.get("mtf_filter_bar")),
        mtf_filter_fast_ema_period=int(payload.get("mtf_filter_fast_ema_period", 21)),
        mtf_filter_slow_ema_period=int(payload.get("mtf_filter_slow_ema_period", 55)),
        mtf_reversal_mode=str(payload.get("mtf_reversal_mode", "block_new_entries")),
        daily_filter_inst_id=None
        if payload.get("daily_filter_inst_id") in (None, "")
        else str(payload.get("daily_filter_inst_id")),
        daily_filter_bar=None if payload.get("daily_filter_bar") in (None, "") else str(payload.get("daily_filter_bar")),
        daily_filter_boundary=str(payload.get("daily_filter_boundary", "exchange")),
        daily_filter_enabled=coerce_bool(payload.get("daily_filter_enabled"), False),
        daily_filter_mode=str(payload.get("daily_filter_mode", "disabled")),
        daily_filter_scope=str(payload.get("daily_filter_scope", "both")),
        daily_filter_ma_type=str(payload.get("daily_filter_ma_type", "ema")),
        daily_filter_period=int(payload.get("daily_filter_period", 5)),
        rail_candidate_ema_periods=rail_candidate_periods,
        rail_touch_atr_ratio=Decimal(str(payload.get("rail_touch_atr_ratio", "0.2"))),
        rail_bounce_atr_ratio=Decimal(str(payload.get("rail_bounce_atr_ratio", "0.6"))),
        rail_bounce_confirm_bars=int(payload.get("rail_bounce_confirm_bars", 3)),
        rail_break_atr_ratio=Decimal(str(payload.get("rail_break_atr_ratio", "1.0"))),
        rail_reclaim_bars=int(payload.get("rail_reclaim_bars", 2)),
        rail_score_lookback_bars=int(payload.get("rail_score_lookback_bars", 60)),
        rail_switch_min_score_delta=Decimal(str(payload.get("rail_switch_min_score_delta", "8"))),
        rail_min_touches=int(payload.get("rail_min_touches", 2)),
        rail_min_bounces=int(payload.get("rail_min_bounces", 1)),
        rail_fast_gate_enabled=coerce_bool(payload.get("rail_fast_gate_enabled"), True),
        rail_fast_gate_period=int(payload.get("rail_fast_gate_period", 21)),
        rail_fast_min_gap_ema200_atr=Decimal(str(payload.get("rail_fast_min_gap_ema200_atr", "5.0"))),
        rail_fast_min_spread_trend_atr=Decimal(str(payload.get("rail_fast_min_spread_trend_atr", "1.5"))),
        rail_fast_max_recent_range_atr=Decimal(str(payload.get("rail_fast_max_recent_range_atr", "3.0"))),
        rail_fast_recent_range_bars=int(payload.get("rail_fast_recent_range_bars", 8)),
        backtest_profile_id=str(payload.get("backtest_profile_id", "")),
        backtest_profile_name=str(payload.get("backtest_profile_name", "")),
        backtest_profile_summary=str(payload.get("backtest_profile_summary", "")),
        backtest_initial_capital=Decimal(str(payload.get("backtest_initial_capital", "10000"))),
        backtest_sizing_mode=str(payload.get("backtest_sizing_mode", "fixed_risk")),
        backtest_risk_percent=None
        if payload.get("backtest_risk_percent") in (None, "")
        else Decimal(str(payload.get("backtest_risk_percent"))),
        backtest_compounding=bool(payload.get("backtest_compounding", False)),
        backtest_entry_slippage_rate=entry_slippage_rate,
        backtest_exit_slippage_rate=exit_slippage_rate,
        backtest_slippage_rate=legacy_slippage_rate,
        backtest_funding_rate=Decimal(str(payload.get("backtest_funding_rate", "0"))),
    )


def simulate_portfolio(
    *,
    candidates: list[CandidateTrade],
    initial_capital: Decimal,
    risk_per_trade: Decimal,
    max_positions: int,
    max_long_positions: int,
    max_short_positions: int,
    max_total_exposure: Decimal,
    max_symbol_exposure: Decimal,
    fixed_risk_amount: Decimal | None = None,
    fee_multiplier: Decimal = Decimal("1"),
    slippage_multiplier: Decimal = Decimal("1"),
    preserve_candidate_size: bool = False,
) -> dict[str, Any]:
    ordered = sorted(candidates, key=lambda item: (item.entry_ts, item.strategy_name, item.symbol, item.side))
    open_positions: list[dict[str, Any]] = []
    executed: list[ExecutedTrade] = []
    rejected: list[RejectedSignal] = []
    equity = initial_capital
    trade_no = 0

    def close_until(ts: int) -> None:
        nonlocal equity
        closable = sorted((item for item in open_positions if int(item["exit_ts"]) <= ts), key=lambda item: int(item["exit_ts"]))
        for position in closable:
            equity += Decimal(position["scaled_pnl"])
            trade = ExecutedTrade(
                trade_no=position["trade_no"],
                candidate_id=position["candidate_id"],
                profile_id=position["profile_id"],
                strategy_name=position["strategy_name"],
                strategy_id=position["strategy_id"],
                symbol=position["symbol"],
                coin=position["coin"],
                side=position["side"],
                entry_ts=position["entry_ts"],
                exit_ts=position["exit_ts"],
                entry_price=position["entry_price"],
                exit_price=position["exit_price"],
                stop_loss=position["stop_loss"],
                take_profit=position["take_profit"],
                scaled_size=position["scaled_size"],
                scaled_notional=position["scaled_notional"],
                scaled_risk_value=position["scaled_risk_value"],
                scaled_gross_pnl=position["scaled_gross_pnl"],
                scaled_pnl=position["scaled_pnl"],
                scaled_entry_fee=position["scaled_entry_fee"],
                scaled_exit_fee=position["scaled_exit_fee"],
                scaled_total_fee=position["scaled_total_fee"],
                scaled_slippage_cost=position["scaled_slippage_cost"],
                scaled_funding_cost=position["scaled_funding_cost"],
                r_multiple=position["r_multiple"],
                exit_reason=position["exit_reason"],
                exit_reason_label=position["exit_reason_label"],
                fee_model=position["fee_model"],
                capital_before_entry=position["capital_before_entry"],
                capital_after_exit=equity,
                wave_entry_sequence=int(position.get("wave_entry_sequence", 0)),
            )
            executed.append(trade)
            open_positions.remove(position)

    for candidate in ordered:
        close_until(candidate.entry_ts)
        if candidate.base_risk_value <= 0:
            rejected.append(
                RejectedSignal(
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

        side_open_count = sum(1 for item in open_positions if item["side"] == candidate.side)
        total_notional = sum(Decimal(item["scaled_notional"]) for item in open_positions)
        symbol_notional = sum(
            Decimal(item["scaled_notional"]) for item in open_positions if item["symbol"] == candidate.symbol
        )

        if len(open_positions) >= max_positions:
            reject_reason = "超过总持仓上限"
        elif candidate.side == "多头" and side_open_count >= max_long_positions:
            reject_reason = "超过多头持仓上限"
        elif candidate.side == "空头" and side_open_count >= max_short_positions:
            reject_reason = "超过空头持仓上限"
        elif equity <= 0:
            reject_reason = "权益已归零"
        else:
            reject_reason = ""

        if preserve_candidate_size:
            target_risk = candidate.base_risk_value
            scale = Decimal("1")
        else:
            target_risk = fixed_risk_amount if fixed_risk_amount is not None else (equity * risk_per_trade if equity > 0 else Decimal("0"))
            scale = Decimal("0") if target_risk <= 0 else target_risk / candidate.base_risk_value
        scaled_notional = candidate.base_notional * scale

        if not reject_reason and total_notional + scaled_notional > equity * max_total_exposure:
            reject_reason = "超过组合总暴露上限"
        if not reject_reason and symbol_notional + scaled_notional > equity * max_symbol_exposure:
            reject_reason = "超过单币种暴露上限"
        if not reject_reason and scaled_notional <= 0:
            reject_reason = "仓位规模无效"

        if reject_reason:
            rejected.append(
                RejectedSignal(
                    candidate_id=candidate.candidate_id,
                    profile_id=candidate.profile_id,
                    strategy_name=candidate.strategy_name,
                    symbol=candidate.symbol,
                    coin=candidate.coin,
                    side=candidate.side,
                    entry_ts=candidate.entry_ts,
                    reason=reject_reason,
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

        # BacktestTrade.gross_pnl is already based on slipped entry/exit prices.
        # base_pnl therefore already includes the baseline slippage effect, while
        # slippage_cost is kept separately for reporting and stress adjustments.
        baseline_gross_pnl = candidate.base_gross_pnl * scale
        baseline_pnl = candidate.base_pnl * scale
        fee_delta = (fee_multiplier - Decimal("1")) * candidate.base_total_fee * scale
        slippage_delta = (slippage_multiplier - Decimal("1")) * candidate.base_slippage_cost * scale
        scaled_gross_pnl = baseline_gross_pnl - slippage_delta
        scaled_pnl = baseline_pnl - fee_delta - slippage_delta
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
                "wave_entry_sequence": candidate.wave_entry_sequence,
            }
        )

    close_until(10**18)
    executed = sorted(executed, key=lambda item: (item.exit_ts, item.entry_ts, item.coin, item.side))
    return {"executed_trades": executed, "rejected_signals": rejected}


def build_hourly_equity_curve(
    *,
    start_ts: int,
    end_ts: int,
    initial_capital: Decimal,
    executed_trades: list[ExecutedTrade],
) -> pd.DataFrame:
    event_rows = [{"ts": start_ts, "equity": float(initial_capital)}]
    running = initial_capital
    for trade in sorted(executed_trades, key=lambda item: (item.exit_ts, item.entry_ts)):
        running += trade.scaled_pnl
        event_rows.append({"ts": trade.exit_ts, "equity": float(running)})
    events = pd.DataFrame(event_rows).sort_values("ts").drop_duplicates(subset=["ts"], keep="last")
    timeline = pd.DataFrame({"ts": pd.date_range(pd.to_datetime(start_ts, unit="ms", utc=True), pd.to_datetime(end_ts, unit="ms", utc=True), freq="1h", inclusive="both").view("int64") // 10**6})
    curve = pd.merge_asof(timeline.sort_values("ts"), events.sort_values("ts"), on="ts", direction="backward")
    curve["equity"] = curve["equity"].fillna(float(initial_capital))
    curve["时间"] = pd.to_datetime(curve["ts"], unit="ms", utc=True).dt.tz_convert("Asia/Shanghai").dt.strftime("%Y-%m-%d %H:%M:%S")
    curve["总权益"] = curve["equity"]
    curve["历史峰值"] = curve["总权益"].cummax()
    curve["当前回撤"] = curve["总权益"] / curve["历史峰值"] - 1.0
    curve["累计收益率"] = curve["总权益"] / float(initial_capital) - 1.0
    return curve[["时间", "总权益", "历史峰值", "当前回撤", "累计收益率"]]


def build_executed_trade_frame(executed_trades: list[ExecutedTrade]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for trade in executed_trades:
        entry_dt = pd.to_datetime(trade.entry_ts, unit="ms", utc=True).tz_convert("Asia/Shanghai")
        exit_dt = pd.to_datetime(trade.exit_ts, unit="ms", utc=True).tz_convert("Asia/Shanghai")
        returns = float(trade.scaled_pnl / trade.capital_before_entry) if trade.capital_before_entry > 0 else 0.0
        rows.append(
            {
                "trade_no": trade.trade_no,
                "coin": trade.coin,
                "symbol": trade.symbol,
                "strategy_name": trade.strategy_name,
                "strategy_id": trade.strategy_id,
                "side": trade.side,
                "wave_entry_sequence": int(trade.wave_entry_sequence),
                "entry_ts": trade.entry_ts,
                "entry_time_bjt": entry_dt.strftime("%Y-%m-%d %H:%M:%S"),
                "entry_price": float(trade.entry_price),
                "exit_ts": trade.exit_ts,
                "exit_time_bjt": exit_dt.strftime("%Y-%m-%d %H:%M:%S"),
                "exit_price": float(trade.exit_price),
                "pnl": float(trade.scaled_pnl),
                "return_pct": returns * 100.0,
                "r_multiple": float(trade.r_multiple),
                "exit_reason": trade.exit_reason_label,
                "size": float(trade.scaled_size),
                "notional": float(trade.scaled_notional),
                "risk_value": float(trade.scaled_risk_value),
                "entry_fee": float(trade.scaled_entry_fee),
                "exit_fee": float(trade.scaled_exit_fee),
                "total_fee": float(trade.scaled_total_fee),
                "slippage_cost": float(trade.scaled_slippage_cost),
                "funding_cost": float(trade.scaled_funding_cost),
                "capital_before_entry": float(trade.capital_before_entry),
                "capital_after_exit": float(trade.capital_after_exit),
                "fee_model": trade.fee_model,
                "year": exit_dt.strftime("%Y"),
                "month": exit_dt.strftime("%Y-%m"),
            }
        )
    if not rows:
        return pd.DataFrame(
            columns=[
                "trade_no",
                "coin",
                "symbol",
                "strategy_name",
                "strategy_id",
                "side",
                "wave_entry_sequence",
                "entry_ts",
                "entry_time_bjt",
                "entry_price",
                "exit_ts",
                "exit_time_bjt",
                "exit_price",
                "pnl",
                "return_pct",
                "r_multiple",
                "exit_reason",
                "size",
                "notional",
                "risk_value",
                "entry_fee",
                "exit_fee",
                "total_fee",
                "slippage_cost",
                "funding_cost",
                "capital_before_entry",
                "capital_after_exit",
                "fee_model",
                "year",
                "month",
            ]
        )
    return pd.DataFrame(rows).sort_values(["exit_ts", "entry_ts", "coin", "side"]).reset_index(drop=True)


def build_trades_export(executed_df: pd.DataFrame) -> pd.DataFrame:
    if executed_df.empty:
        return pd.DataFrame(
            columns=["编号", "币种", "策略", "方向", "本波第几次", "开仓时间", "开仓价", "平仓时间", "平仓价", "盈亏", "收益率", "R倍数", "平仓原因"]
        )
    out = executed_df.copy()
    out["编号"] = out["trade_no"]
    out["币种"] = out["coin"]
    out["策略"] = out["strategy_name"]
    out["方向"] = out["side"]
    out["本波第几次"] = out["wave_entry_sequence"].fillna(0).astype(int)
    out["开仓时间"] = out["entry_time_bjt"]
    out["开仓价"] = out["entry_price"].map(lambda value: round(float(value), 8))
    out["平仓时间"] = out["exit_time_bjt"]
    out["平仓价"] = out["exit_price"].map(lambda value: round(float(value), 8))
    out["盈亏"] = out["pnl"].map(lambda value: round(float(value), 2))
    out["收益率"] = out["return_pct"].map(lambda value: round(float(value), 2))
    out["R倍数"] = out["r_multiple"].map(lambda value: round(float(value), 4))
    out["平仓原因"] = out["exit_reason"]
    out["成交数量"] = out["size"].map(lambda value: round(float(value), 8))
    out["名义仓位"] = out["notional"].map(lambda value: round(float(value), 2))
    out["风险金额"] = out["risk_value"].map(lambda value: round(float(value), 2))
    out["手续费"] = out["total_fee"].map(lambda value: round(float(value), 2))
    out["滑点成本"] = out["slippage_cost"].map(lambda value: round(float(value), 2))
    out["资金占用前权益"] = out["capital_before_entry"].map(lambda value: round(float(value), 2))
    out["平仓后权益"] = out["capital_after_exit"].map(lambda value: round(float(value), 2))
    out["费率口径"] = out["fee_model"]
    return out[
        [
            "编号",
            "币种",
            "策略",
            "方向",
            "本波第几次",
            "开仓时间",
            "开仓价",
            "平仓时间",
            "平仓价",
            "成交数量",
            "名义仓位",
            "风险金额",
            "手续费",
            "滑点成本",
            "盈亏",
            "收益率",
            "R倍数",
            "平仓原因",
            "资金占用前权益",
            "平仓后权益",
            "费率口径",
        ]
    ]


def compute_trade_metrics(trades: pd.DataFrame, initial_capital: Decimal) -> dict[str, Any]:
    if trades.empty:
        return {
            "trades": 0,
            "wins": 0,
            "losses": 0,
            "win_rate": 0.0,
            "total_pnl": 0.0,
            "total_return_pct": 0.0,
            "profit_factor": 0.0,
            "payoff_ratio": 0.0,
            "avg_r": 0.0,
            "max_r": 0.0,
            "min_r": 0.0,
            "expectancy": 0.0,
            "max_drawdown_pct": 0.0,
            "max_drawdown_amount": 0.0,
        }
    pnl = trades["pnl"].astype(float)
    wins = trades.loc[trades["pnl"] > 0]
    losses = trades.loc[trades["pnl"] < 0]
    win_rate = float((pnl > 0).mean()) if len(trades) else 0.0
    gross_profit = float(wins["pnl"].sum()) if not wins.empty else 0.0
    gross_loss = abs(float(losses["pnl"].sum())) if not losses.empty else 0.0
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else (999.0 if gross_profit > 0 else 0.0)
    avg_win = float(wins["pnl"].mean()) if not wins.empty else 0.0
    avg_loss = abs(float(losses["pnl"].mean())) if not losses.empty else 0.0
    payoff_ratio = avg_win / avg_loss if avg_loss > 0 else (999.0 if avg_win > 0 else 0.0)
    cumulative = pnl.cumsum()
    peak = cumulative.cummax()
    drawdown_amount = peak - cumulative
    max_drawdown_amount = float(drawdown_amount.max()) if len(drawdown_amount) else 0.0
    max_drawdown_pct = max_drawdown_amount / float(initial_capital) if initial_capital > 0 else 0.0
    return {
        "trades": int(len(trades)),
        "wins": int((pnl > 0).sum()),
        "losses": int((pnl < 0).sum()),
        "win_rate": win_rate,
        "total_pnl": float(pnl.sum()),
        "total_return_pct": float(pnl.sum()) / float(initial_capital) if initial_capital > 0 else 0.0,
        "profit_factor": profit_factor,
        "payoff_ratio": payoff_ratio,
        "avg_r": float(trades["r_multiple"].mean()) if len(trades) else 0.0,
        "max_r": float(trades["r_multiple"].max()) if len(trades) else 0.0,
        "min_r": float(trades["r_multiple"].min()) if len(trades) else 0.0,
        "expectancy": float(pnl.mean()) if len(trades) else 0.0,
        "max_drawdown_pct": max_drawdown_pct,
        "max_drawdown_amount": max_drawdown_amount,
        "fees": float(trades["total_fee"].sum()),
        "slippage": float(trades["slippage_cost"].sum()),
        "funding": float(trades["funding_cost"].sum()),
    }


def compute_drawdown_metadata(equity_curve: pd.DataFrame) -> dict[str, Any]:
    if equity_curve.empty:
        return {
            "max_drawdown_pct": 0.0,
            "start_time": "",
            "end_time": "",
            "recovery_time": "",
            "recovery_bars": 0,
        }
    curve = equity_curve.copy()
    curve["cummax"] = curve["总权益"].cummax()
    curve["drawdown"] = curve["总权益"] / curve["cummax"] - 1.0
    worst_index = curve["drawdown"].idxmin()
    worst_row = curve.loc[worst_index]
    peak_index = curve.loc[:worst_index, "总权益"].idxmax()
    peak_row = curve.loc[peak_index]
    recovery_rows = curve.loc[worst_index:]
    recovered = recovery_rows[recovery_rows["总权益"] >= peak_row["总权益"]]
    recovery_time = recovered.iloc[0]["时间"] if not recovered.empty else ""
    recovery_bars = int(recovered.index[0] - worst_index) if not recovered.empty else 0
    return {
        "max_drawdown_pct": float(worst_row["drawdown"]),
        "start_time": peak_row["时间"],
        "end_time": worst_row["时间"],
        "recovery_time": recovery_time,
        "recovery_bars": recovery_bars,
    }


def compute_utilization(*, executed_trades: list[ExecutedTrade], start_ts: int, end_ts: int, initial_capital: Decimal) -> dict[str, float]:
    events: list[tuple[int, Decimal]] = [(start_ts, Decimal("0")), (end_ts, Decimal("0"))]
    for trade in executed_trades:
        events.append((trade.entry_ts, trade.scaled_notional))
        events.append((trade.exit_ts, -trade.scaled_notional))
    events.sort(key=lambda item: item[0])
    current_notional = Decimal("0")
    weighted_notional = Decimal("0")
    total_duration = Decimal("0")
    peak_notional = Decimal("0")
    for index, (ts, delta) in enumerate(events[:-1]):
        current_notional += delta
        peak_notional = max(peak_notional, current_notional)
        next_ts = events[index + 1][0]
        duration = Decimal(str(max(next_ts - ts, 0)))
        weighted_notional += current_notional * duration
        total_duration += duration
    avg_notional = weighted_notional / total_duration if total_duration > 0 else Decimal("0")
    return {
        "average_utilization_pct": float(avg_notional / initial_capital) if initial_capital > 0 else 0.0,
        "peak_utilization_pct": float(peak_notional / initial_capital) if initial_capital > 0 else 0.0,
        "peak_notional": float(peak_notional),
    }


def build_monthly_returns_table(equity_curve: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    curve = equity_curve.copy()
    curve["dt"] = pd.to_datetime(curve["时间"])
    daily = curve.set_index("dt")["总权益"].resample("D").last().ffill()
    month_end = daily.resample("ME").last()
    month_start = month_end.shift(1).fillna(month_end.iloc[0])
    monthly_returns = (month_end / month_start - 1.0).fillna(0.0)
    detail = pd.DataFrame(
        {
            "period": monthly_returns.index.strftime("%Y-%m"),
            "year": monthly_returns.index.strftime("%Y"),
            "month": monthly_returns.index.month,
            "return_pct": monthly_returns.values * 100.0,
        }
    )
    if detail.empty:
        wide = pd.DataFrame(columns=["年份"] + [f"{index}月" for index in range(1, 13)] + ["年收益"])
        return wide, detail
    pivot = detail.pivot(index="year", columns="month", values="return_pct").sort_index()
    wide_rows: list[dict[str, Any]] = []
    for year, row in pivot.iterrows():
        item: dict[str, Any] = {"年份": year}
        annual = 1.0
        for month in range(1, 13):
            value = row.get(month)
            item[f"{month}月"] = "" if pd.isna(value) else round(float(value), 2)
            if not pd.isna(value):
                annual *= 1.0 + float(value) / 100.0
        item["年收益"] = round((annual - 1.0) * 100.0, 2)
        wide_rows.append(item)
    wide = pd.DataFrame(wide_rows)
    return wide, detail


def build_yearly_returns_table(equity_curve: pd.DataFrame, trades: pd.DataFrame) -> pd.DataFrame:
    curve = equity_curve.copy()
    curve["dt"] = pd.to_datetime(curve["时间"])
    curve["year"] = curve["dt"].dt.strftime("%Y")
    rows: list[dict[str, Any]] = []
    for year, subset in curve.groupby("year", sort=True):
        start_equity = float(subset["总权益"].iloc[0])
        end_equity = float(subset["总权益"].iloc[-1])
        year_trades = trades[trades["year"] == year]
        metrics = compute_trade_metrics(year_trades, Decimal(str(start_equity)))
        running_peak = subset["总权益"].cummax()
        drawdown = subset["总权益"] / running_peak - 1.0
        rows.append(
            {
                "年份": year,
                "年初资金": round(start_equity, 2),
                "年末资金": round(end_equity, 2),
                "年收益率": round((end_equity / start_equity - 1.0) * 100.0 if start_equity > 0 else 0.0, 2),
                "最大回撤": round(float(drawdown.min()) * 100.0, 2),
                "交易次数": int(len(year_trades)),
                "胜率": round(metrics["win_rate"] * 100.0, 2),
                "Profit Factor": round(metrics["profit_factor"], 4),
            }
        )
    return pd.DataFrame(rows)


def build_side_summary(trades: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for side_label, subset in list(trades.groupby("side", sort=False)) + [("合计", trades)]:
        if side_label == "合计":
            side_name = "合计"
        else:
            side_name = side_label
        metrics = compute_trade_metrics(subset, INITIAL_CAPITAL)
        rows.append(
            {
                "方向": side_name,
                "交易次数": metrics["trades"],
                "胜率": round(metrics["win_rate"] * 100.0, 2),
                "收益率": round(metrics["total_return_pct"] * 100.0, 2),
                "最大回撤": round(metrics["max_drawdown_pct"] * 100.0, 2),
                "盈亏比": round(metrics["payoff_ratio"], 4),
                "Profit Factor": round(metrics["profit_factor"], 4),
                "最终收益": round(metrics["total_pnl"], 2),
            }
        )
    return pd.DataFrame(rows)


def build_symbol_summary(trades: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for coin in sorted(trades["coin"].dropna().unique()):
        coin_subset = trades[trades["coin"] == coin]
        for side in ("多头", "空头"):
            side_subset = coin_subset[coin_subset["side"] == side]
            metrics = compute_trade_metrics(side_subset, INITIAL_CAPITAL)
            rows.append(
                {
                    "币种": coin,
                    "方向": side,
                    "交易次数": metrics["trades"],
                    "胜率": round(metrics["win_rate"] * 100.0, 2),
                    "收益率": round(metrics["total_return_pct"] * 100.0, 2),
                    "最大回撤": round(metrics["max_drawdown_pct"] * 100.0, 2),
                    "盈亏比": round(metrics["payoff_ratio"], 4),
                    "Profit Factor": round(metrics["profit_factor"], 4),
                    "平均R": round(metrics["avg_r"], 4),
                    "最终收益": round(metrics["total_pnl"], 2),
                }
            )
        total_metrics = compute_trade_metrics(coin_subset, INITIAL_CAPITAL)
        rows.append(
            {
                "币种": coin,
                "方向": "合计",
                "交易次数": total_metrics["trades"],
                "胜率": round(total_metrics["win_rate"] * 100.0, 2),
                "收益率": round(total_metrics["total_return_pct"] * 100.0, 2),
                "最大回撤": round(total_metrics["max_drawdown_pct"] * 100.0, 2),
                "盈亏比": round(total_metrics["payoff_ratio"], 4),
                "Profit Factor": round(total_metrics["profit_factor"], 4),
                "平均R": round(total_metrics["avg_r"], 4),
                "最终收益": round(total_metrics["total_pnl"], 2),
            }
        )
    return pd.DataFrame(rows)


def build_strategy_summary(trades: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for (strategy_name, coin, side), subset in trades.groupby(["strategy_name", "coin", "side"], sort=True):
        metrics = compute_trade_metrics(subset, INITIAL_CAPITAL)
        rows.append(
            {
                "策略": strategy_name,
                "币种": coin,
                "方向": side,
                "交易次数": metrics["trades"],
                "胜率": round(metrics["win_rate"] * 100.0, 2),
                "收益率": round(metrics["total_return_pct"] * 100.0, 2),
                "最大回撤": round(metrics["max_drawdown_pct"] * 100.0, 2),
                "盈亏比": round(metrics["payoff_ratio"], 4),
                "Profit Factor": round(metrics["profit_factor"], 4),
                "平均R": round(metrics["avg_r"], 4),
                "最终收益": round(metrics["total_pnl"], 2),
            }
        )
    return pd.DataFrame(rows).sort_values(["最终收益", "交易次数"], ascending=[False, False]).reset_index(drop=True)


def build_rejection_summary(rejected_signals: list[RejectedSignal]) -> pd.DataFrame:
    if not rejected_signals:
        return pd.DataFrame(columns=["拒绝原因", "次数"])
    frame = pd.DataFrame(
        [{"拒绝原因": item.reason, "次数": 1} for item in rejected_signals]
    )
    return frame.groupby("拒绝原因", as_index=False)["次数"].sum().sort_values("次数", ascending=False)


def build_summary_export(
    *,
    overall_metrics: dict[str, Any],
    side_summary: pd.DataFrame,
    symbol_summary: pd.DataFrame,
    strategy_summary: pd.DataFrame,
    rejection_summary: pd.DataFrame,
    utilization: dict[str, float],
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = [
        {
            "分类": "组合总览",
            "名称": "组合合计",
            "方向": "合计",
            "交易次数": overall_metrics["trades"],
            "胜率": round(overall_metrics["win_rate"] * 100.0, 2),
            "收益率": round(overall_metrics["total_return_pct"] * 100.0, 2),
            "最大回撤": round(overall_metrics["max_drawdown_pct"] * 100.0, 2),
            "盈亏比": round(overall_metrics["payoff_ratio"], 4),
            "Profit Factor": round(overall_metrics["profit_factor"], 4),
            "平均R": round(overall_metrics["avg_r"], 4),
            "最终收益": round(overall_metrics["total_pnl"], 2),
            "备注": (
                f"平均资金利用率 {utilization['average_utilization_pct'] * 100:.2f}% | "
                f"峰值资金利用率 {utilization['peak_utilization_pct'] * 100:.2f}%"
            ),
        }
    ]
    for _, row in side_summary.iterrows():
        rows.append(
            {
                "分类": "方向统计",
                "名称": row["方向"],
                "方向": row["方向"],
                "交易次数": row["交易次数"],
                "胜率": row["胜率"],
                "收益率": row["收益率"],
                "最大回撤": row["最大回撤"],
                "盈亏比": row["盈亏比"],
                "Profit Factor": row["Profit Factor"],
                "平均R": "",
                "最终收益": row["最终收益"],
                "备注": "",
            }
        )
    for _, row in symbol_summary.iterrows():
        rows.append(
            {
                "分类": "币种统计",
                "名称": row["币种"],
                "方向": row["方向"],
                "交易次数": row["交易次数"],
                "胜率": row["胜率"],
                "收益率": row["收益率"],
                "最大回撤": row["最大回撤"],
                "盈亏比": row["盈亏比"],
                "Profit Factor": row["Profit Factor"],
                "平均R": row["平均R"],
                "最终收益": row["最终收益"],
                "备注": "",
            }
        )
    for _, row in strategy_summary.iterrows():
        rows.append(
            {
                "分类": "策略统计",
                "名称": row["策略"],
                "方向": row["方向"],
                "交易次数": row["交易次数"],
                "胜率": row["胜率"],
                "收益率": row["收益率"],
                "最大回撤": row["最大回撤"],
                "盈亏比": row["盈亏比"],
                "Profit Factor": row["Profit Factor"],
                "平均R": row["平均R"],
                "最终收益": row["最终收益"],
                "备注": row["币种"],
            }
        )
    for _, row in rejection_summary.iterrows():
        rows.append(
            {
                "分类": "拒绝信号",
                "名称": row["拒绝原因"],
                "方向": "",
                "交易次数": row["次数"],
                "胜率": "",
                "收益率": "",
                "最大回撤": "",
                "盈亏比": "",
                "Profit Factor": "",
                "平均R": "",
                "最终收益": "",
                "备注": "未成交统计",
            }
        )
    return pd.DataFrame(rows)


def build_stress_table(
    candidates: list[CandidateTrade],
    *,
    fee_multipliers: list[int],
    slippage_multiplier: Decimal | None,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    if slippage_multiplier is not None:
        for fee_mult in fee_multipliers:
            result = simulate_portfolio(
                candidates=candidates,
                initial_capital=INITIAL_CAPITAL,
                risk_per_trade=RISK_PER_TRADE,
                max_positions=MAX_POSITIONS,
                max_long_positions=MAX_LONG_POSITIONS,
                max_short_positions=MAX_SHORT_POSITIONS,
                max_total_exposure=MAX_TOTAL_EXPOSURE,
                max_symbol_exposure=MAX_SYMBOL_EXPOSURE,
                fee_multiplier=Decimal(str(fee_mult)),
                slippage_multiplier=slippage_multiplier,
            )
            executed_df = build_executed_trade_frame(result["executed_trades"])
            metrics = compute_trade_metrics(executed_df, INITIAL_CAPITAL)
            rows.append(
                {
                    "测试项": f"手续费 {fee_mult}x",
                    "总收益率": round(metrics["total_return_pct"] * 100.0, 2),
                    "最大回撤": round(metrics["max_drawdown_pct"] * 100.0, 2),
                    "最终收益": round(metrics["total_pnl"], 2),
                }
            )
    else:
        for slip_mult in (0, 1, 2):
            result = simulate_portfolio(
                candidates=candidates,
                initial_capital=INITIAL_CAPITAL,
                risk_per_trade=RISK_PER_TRADE,
                max_positions=MAX_POSITIONS,
                max_long_positions=MAX_LONG_POSITIONS,
                max_short_positions=MAX_SHORT_POSITIONS,
                max_total_exposure=MAX_TOTAL_EXPOSURE,
                max_symbol_exposure=MAX_SYMBOL_EXPOSURE,
                fee_multiplier=Decimal("1"),
                slippage_multiplier=Decimal(str(slip_mult)),
            )
            executed_df = build_executed_trade_frame(result["executed_trades"])
            metrics = compute_trade_metrics(executed_df, INITIAL_CAPITAL)
            rows.append(
                {
                    "测试项": f"滑点 {slip_mult}x",
                    "总收益率": round(metrics["total_return_pct"] * 100.0, 2),
                    "最大回撤": round(metrics["max_drawdown_pct"] * 100.0, 2),
                    "最终收益": round(metrics["total_pnl"], 2),
                }
            )
    return pd.DataFrame(rows)


def build_regime_summary(equity_curve: pd.DataFrame, trades: pd.DataFrame) -> pd.DataFrame:
    periods = [
        ("2020 暴跌", "2020-02-15", "2020-04-30"),
        ("2021 牛市", "2021-01-01", "2021-12-31"),
        ("2022 熊市", "2022-01-01", "2022-12-31"),
        ("2024 以后阶段", "2024-01-01", "2099-12-31"),
    ]
    curve = equity_curve.copy()
    curve["dt"] = pd.to_datetime(curve["时间"])
    trades = trades.copy()
    trades["dt"] = pd.to_datetime(trades["exit_time_bjt"])
    rows: list[dict[str, Any]] = []
    for label, start_text, end_text in periods:
        start = pd.Timestamp(start_text)
        end = pd.Timestamp(end_text) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
        subset_curve = curve[(curve["dt"] >= start) & (curve["dt"] <= end)]
        subset_trades = trades[(trades["dt"] >= start) & (trades["dt"] <= end)]
        if subset_curve.empty:
            rows.append({"阶段": label, "收益率": "", "最大回撤": "", "交易次数": 0, "胜率": "", "Profit Factor": ""})
            continue
        start_equity = float(subset_curve["总权益"].iloc[0])
        end_equity = float(subset_curve["总权益"].iloc[-1])
        drawdown = subset_curve["总权益"] / subset_curve["总权益"].cummax() - 1.0
        metrics = compute_trade_metrics(subset_trades, Decimal(str(start_equity)))
        rows.append(
            {
                "阶段": label,
                "收益率": round((end_equity / start_equity - 1.0) * 100.0 if start_equity > 0 else 0.0, 2),
                "最大回撤": round(float(drawdown.min()) * 100.0, 2),
                "交易次数": int(len(subset_trades)),
                "胜率": round(metrics["win_rate"] * 100.0, 2),
                "Profit Factor": round(metrics["profit_factor"], 4),
            }
        )
    return pd.DataFrame(rows)


def build_coin_correlation_matrix(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame()
    monthly = (
        trades.groupby(["month", "coin"], as_index=False)["pnl"]
        .sum()
        .pivot(index="month", columns="coin", values="pnl")
        .fillna(0.0)
    )
    return monthly.corr()


def compute_risk_text(
    *,
    executed_df: pd.DataFrame,
    symbol_summary: pd.DataFrame,
    yearly_table: pd.DataFrame,
    rejection_summary: pd.DataFrame,
) -> dict[str, str]:
    profitable_years = int((yearly_table["年收益率"].fillna(0) > 0).sum()) if not yearly_table.empty else 0
    total_years = int(len(yearly_table))
    profitable_coin_count = int((symbol_summary[symbol_summary["方向"] == "合计"]["最终收益"] > 0).sum()) if not symbol_summary.empty else 0
    total_coins = int(len(symbol_summary[symbol_summary["方向"] == "合计"])) if not symbol_summary.empty else 0
    trade_count = int(len(executed_df))
    overfit_flags: list[str] = []
    if total_years > 0 and profitable_years <= max(1, total_years // 2):
        overfit_flags.append("赚钱年份集中，存在年份依赖。")
    if total_coins > 0 and profitable_coin_count <= max(1, total_coins // 2):
        overfit_flags.append("赚钱币种集中，存在币种依赖。")
    if trade_count < 80:
        overfit_flags.append("交易样本偏少，统计稳定性不足。")
    overfit_text = "；".join(overfit_flags) if overfit_flags else "赚钱年份和赚钱币种分布尚可，交易样本量也达到基础要求。"

    rejection_total = int(rejection_summary["次数"].sum()) if not rejection_summary.empty else 0
    capital_risk_text = (
        f"本次因资金/仓位/暴露限制被拒绝的信号共 {rejection_total} 次，"
        f"说明组合在高并发时存在真实资金竞争。"
        if rejection_total > 0
        else "本次未出现因资金与仓位限制导致的大规模拒单。"
    )
    short_subset = executed_df[executed_df["side"] == "空头"]
    long_subset = executed_df[executed_df["side"] == "多头"]
    short_metrics = compute_trade_metrics(short_subset, INITIAL_CAPITAL)
    long_metrics = compute_trade_metrics(long_subset, INITIAL_CAPITAL)
    short_text = (
        f"空头共成交 {short_metrics['trades']} 笔，贡献 {short_metrics['total_pnl']:.2f}U，"
        f"胜率 {short_metrics['win_rate'] * 100:.2f}%，最大回撤 {short_metrics['max_drawdown_pct'] * 100:.2f}%。"
    )
    correlation_text = "相关性矩阵已按币种月度收益输出，可重点观察 BTC 与其他币种收益是否同涨同跌。"
    return {
        "过拟合风险": overfit_text,
        "多币种相关性风险": correlation_text,
        "空头策略风险": short_text,
        "资金占用风险": capital_risk_text,
        "多空对比": (
            f"多头贡献 {long_metrics['total_pnl']:.2f}U，空头贡献 {short_metrics['total_pnl']:.2f}U，"
            "可直接观察哪一侧在拉收益、哪一侧在拉回撤。"
        ),
    }


def build_auto_summary(
    *,
    executed_df: pd.DataFrame,
    overall_metrics: dict[str, Any],
    side_summary: pd.DataFrame,
    strategy_summary: pd.DataFrame,
    symbol_summary: pd.DataFrame,
    rejection_summary: pd.DataFrame,
    drawdown_meta: dict[str, Any],
) -> dict[str, str]:
    best_coin = "无"
    worst_coin = "无"
    symbol_totals = symbol_summary[symbol_summary["方向"] == "合计"].sort_values("最终收益", ascending=False)
    if not symbol_totals.empty:
        best_coin = str(symbol_totals.iloc[0]["币种"])
        worst_coin = str(symbol_totals.iloc[-1]["币种"])
    best_strategy = "无"
    worst_strategy = "无"
    strategy_totals = (
        strategy_summary.groupby("策略", as_index=False)["最终收益"]
        .sum()
        .sort_values("最终收益", ascending=False)
    )
    if not strategy_totals.empty:
        best_strategy = str(strategy_totals.iloc[0]["策略"])
        worst_strategy = str(strategy_totals.iloc[-1]["策略"])
    long_pnl = float(side_summary.loc[side_summary["方向"] == "多头", "最终收益"].sum()) if not side_summary.empty else 0.0
    short_pnl = float(side_summary.loc[side_summary["方向"] == "空头", "最终收益"].sum()) if not side_summary.empty else 0.0
    rejection_total = int(rejection_summary["次数"].sum()) if not rejection_summary.empty else 0
    live_value = "当前不建议实盘"
    if (
        overall_metrics["total_return_pct"] > 0
        and overall_metrics["max_drawdown_pct"] < 0.25
        and overall_metrics["profit_factor"] > 1.2
        and overall_metrics["trades"] >= 100
    ):
        live_value = "具备条件化实盘价值"
    elif overall_metrics["total_return_pct"] > 0 and overall_metrics["profit_factor"] > 1.0:
        live_value = "可进入二轮验证"
    return {
        "组合总收益": f"{overall_metrics['total_return_pct'] * 100:.2f}%",
        "最大回撤": f"{overall_metrics['max_drawdown_pct'] * 100:.2f}%",
        "多头贡献": f"{long_pnl:.2f}U",
        "空头贡献": f"{short_pnl:.2f}U",
        "最佳币种": best_coin,
        "最差币种": worst_coin,
        "最佳策略": best_strategy,
        "最差策略": worst_strategy,
        "最大风险时期": f"{drawdown_meta['start_time']} 至 {drawdown_meta['end_time']}",
        "是否具备实盘价值": live_value,
        "资金拥堵说明": f"被资金和仓位限制拒绝的信号共 {rejection_total} 次。",
    }


def annualized_return(equity_curve: pd.DataFrame) -> float:
    if equity_curve.empty:
        return 0.0
    start_equity = float(equity_curve["总权益"].iloc[0])
    end_equity = float(equity_curve["总权益"].iloc[-1])
    start_dt = pd.to_datetime(equity_curve["时间"].iloc[0])
    end_dt = pd.to_datetime(equity_curve["时间"].iloc[-1])
    days = max((end_dt - start_dt).total_seconds() / 86400.0, 1.0)
    if start_equity <= 0 or end_equity <= 0:
        return 0.0
    return (end_equity / start_equity) ** (365.0 / days) - 1.0


def sharpe_ratio(equity_curve: pd.DataFrame) -> float:
    if equity_curve.empty:
        return 0.0
    curve = equity_curve.copy()
    curve["dt"] = pd.to_datetime(curve["时间"])
    daily = curve.set_index("dt")["总权益"].resample("D").last().ffill()
    returns = daily.pct_change().dropna()
    if returns.empty or float(returns.std()) == 0.0:
        return 0.0
    return float(returns.mean() / returns.std() * math.sqrt(365))


def annualized_volatility(equity_curve: pd.DataFrame) -> float:
    if equity_curve.empty:
        return 0.0
    curve = equity_curve.copy()
    curve["dt"] = pd.to_datetime(curve["时间"])
    daily = curve.set_index("dt")["总权益"].resample("D").last().ffill()
    returns = daily.pct_change().dropna()
    if returns.empty:
        return 0.0
    return float(returns.std() * math.sqrt(365))


def calmar_ratio(annual_return: float, max_drawdown_pct: float) -> float:
    if max_drawdown_pct >= 0:
        return 0.0
    return annual_return / abs(max_drawdown_pct)


def fmt_pct(value: float) -> str:
    return f"{value * 100:.2f}%"


def fmt_num(value: float, digits: int = 2) -> str:
    return f"{value:,.{digits}f}"


def fig_to_base64(fig) -> str:
    buffer = io.BytesIO()
    fig.savefig(buffer, format="png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    return base64.b64encode(buffer.getvalue()).decode("ascii")


def build_line_chart(equity_curve: pd.DataFrame, *, y_col: str, title: str, color: str) -> str:
    fig, ax = plt.subplots(figsize=(10, 3.8))
    x = pd.to_datetime(equity_curve["时间"])
    ax.plot(x, equity_curve[y_col], color=color, linewidth=1.4)
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


def build_r_histogram(r_values: pd.Series) -> str:
    fig, ax = plt.subplots(figsize=(8, 3.8))
    ax.hist(r_values.astype(float), bins=20, color="#e07a5f", edgecolor="white")
    ax.set_title("R倍数分布图")
    ax.grid(alpha=0.2)
    return fig_to_base64(fig)


def build_heatmap(monthly_wide: pd.DataFrame) -> str:
    fig, ax = plt.subplots(figsize=(11, 3.8))
    if monthly_wide.empty:
        ax.text(0.5, 0.5, "无月度收益数据", ha="center", va="center")
        ax.axis("off")
        return fig_to_base64(fig)
    cols = [f"{index}月" for index in range(1, 13)]
    data = monthly_wide[cols].apply(pd.to_numeric, errors="coerce").to_numpy()
    im = ax.imshow(data, cmap="RdYlGn", aspect="auto")
    ax.set_title("月度收益热力图")
    ax.set_yticks(range(len(monthly_wide)))
    ax.set_yticklabels(monthly_wide["年份"].tolist())
    ax.set_xticks(range(len(cols)))
    ax.set_xticklabels(cols)
    for row_index in range(data.shape[0]):
        for col_index in range(data.shape[1]):
            value = data[row_index, col_index]
            if not np.isnan(value):
                ax.text(col_index, row_index, f"{value:.1f}", ha="center", va="center", fontsize=8)
    fig.colorbar(im, ax=ax, fraction=0.025, pad=0.02)
    return fig_to_base64(fig)


def render_table(df: pd.DataFrame, *, table_id: str = "", max_rows: int | None = None) -> str:
    if df.empty:
        return "<p class='empty'>暂无数据。</p>"
    data = df.head(max_rows) if max_rows is not None else df
    table_attr = f" id='{table_id}'" if table_id else ""
    headers = "".join(f"<th>{html.escape(str(col))}</th>" for col in data.columns)
    rows = []
    for _, row in data.iterrows():
        cells = "".join(f"<td>{html.escape(str(value))}</td>" for value in row.tolist())
        rows.append(f"<tr>{cells}</tr>")
    return f"<table{table_attr}><thead><tr>{headers}</tr></thead><tbody>{''.join(rows)}</tbody></table>"


def build_html_report(
    *,
    bundle_name: str,
    assumptions: dict[str, Any],
    data_ranges: dict[str, dict[str, Any]],
    overall_metrics: dict[str, Any],
    drawdown_meta: dict[str, Any],
    utilization: dict[str, float],
    auto_summary: dict[str, str],
    side_summary: pd.DataFrame,
    symbol_summary: pd.DataFrame,
    strategy_summary: pd.DataFrame,
    rejection_summary: pd.DataFrame,
    monthly_wide: pd.DataFrame,
    yearly_table: pd.DataFrame,
    stress_fee: pd.DataFrame,
    stress_slippage: pd.DataFrame,
    regime_table: pd.DataFrame,
    correlation_matrix: pd.DataFrame,
    trades_export: pd.DataFrame,
    equity_hourly: pd.DataFrame,
    monthly_detail: pd.DataFrame,
) -> str:
    annual_return = annualized_return(equity_hourly)
    sharpe = sharpe_ratio(equity_hourly)
    annual_vol = annualized_volatility(equity_hourly)
    calmar = calmar_ratio(annual_return, drawdown_meta["max_drawdown_pct"])
    risk_text = compute_risk_text(
        executed_df=build_executed_trade_frame_from_export(trades_export),
        symbol_summary=symbol_summary,
        yearly_table=yearly_table,
        rejection_summary=rejection_summary,
    )
    start_label = pd.to_datetime(min(item["start_ts"] for item in data_ranges.values()), unit="ms", utc=True).tz_convert("Asia/Shanghai").strftime("%Y-%m-%d %H:%M:%S")
    end_label = pd.to_datetime(max(item["end_ts"] for item in data_ranges.values()), unit="ms", utc=True).tz_convert("Asia/Shanghai").strftime("%Y-%m-%d %H:%M:%S")
    side_contrib = side_summary[side_summary["方向"].isin(["多头", "空头"])]
    strategy_contrib = (
        strategy_summary.groupby("策略", as_index=False)["最终收益"]
        .sum()
        .sort_values("最终收益", ascending=False)
    )
    coin_contrib = symbol_summary[symbol_summary["方向"] == "合计"].sort_values("最终收益", ascending=False)
    yearly_bar = build_bar_chart(yearly_table["年份"].tolist(), yearly_table["年收益率"].tolist(), "年度收益柱状图", "#2a9d8f") if not yearly_table.empty else ""
    coin_bar = build_bar_chart(coin_contrib["币种"].tolist(), coin_contrib["最终收益"].tolist(), "币种贡献图", "#264653") if not coin_contrib.empty else ""
    side_bar = build_bar_chart(side_contrib["方向"].tolist(), side_contrib["最终收益"].tolist(), "多空贡献图", "#e76f51") if not side_contrib.empty else ""
    strategy_bar = build_bar_chart(strategy_contrib["策略"].tolist(), strategy_contrib["最终收益"].tolist(), "策略贡献图", "#6d597a") if not strategy_contrib.empty else ""
    r_hist = build_r_histogram(build_executed_trade_frame_from_export(trades_export)["r_multiple"]) if not trades_export.empty else ""
    equity_chart = build_line_chart(equity_hourly, y_col="总权益", title="组合净值曲线", color="#1d3557")
    drawdown_chart = build_line_chart(equity_hourly, y_col="当前回撤", title="组合回撤曲线", color="#c1121f")
    monthly_heatmap = build_heatmap(monthly_wide)

    summary_cards = [
        ("回测区间", f"{start_label} ~ {end_label}"),
        ("测试币种数量", str(len(data_ranges))),
        ("策略数量", str(sum(1 for _ in strategy_contrib["策略"]) if not strategy_contrib.empty else 0)),
        ("初始资金", f"{fmt_num(float(INITIAL_CAPITAL))} USDT"),
        ("最终资金", f"{fmt_num(float(INITIAL_CAPITAL) + overall_metrics['total_pnl'])} USDT"),
        ("总收益率", fmt_pct(overall_metrics["total_return_pct"])),
        ("年化收益率", fmt_pct(annual_return)),
        ("最大回撤", fmt_pct(drawdown_meta["max_drawdown_pct"])),
        ("夏普比率", f"{sharpe:.3f}"),
        ("卡玛比率", f"{calmar:.3f}"),
        ("总交易次数", str(overall_metrics["trades"])),
        ("胜率", fmt_pct(overall_metrics["win_rate"])),
        ("盈亏比", f"{overall_metrics['payoff_ratio']:.3f}"),
        ("Profit Factor", f"{overall_metrics['profit_factor']:.3f}"),
    ]

    assumptions_html = "".join(
        f"<li><strong>{html.escape(str(key))}</strong>：{html.escape(str(value))}</li>"
        for key, value in assumptions.items()
    )
    data_range_df = pd.DataFrame(
        [
            {
                "币种": item["coin"],
                "起始时间": pd.to_datetime(item["start_ts"], unit="ms", utc=True).tz_convert("Asia/Shanghai").strftime("%Y-%m-%d %H:%M:%S"),
                "结束时间": pd.to_datetime(item["end_ts"], unit="ms", utc=True).tz_convert("Asia/Shanghai").strftime("%Y-%m-%d %H:%M:%S"),
                "1H K线数量": item["candles"],
            }
            for item in data_ranges.values()
        ]
    ).sort_values("币种")

    trades_js = trades_export.to_dict(orient="records")
    correlation_table = correlation_matrix.round(4) if not correlation_matrix.empty else pd.DataFrame()
    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <title>最佳参数组合包 1H 全量正式组合回测报告</title>
  <style>
    body {{
      margin: 0;
      font-family: "Microsoft YaHei UI", "Microsoft YaHei", sans-serif;
      background: #f4f1ea;
      color: #1f2933;
    }}
    .page {{
      max-width: 1440px;
      margin: 0 auto;
      padding: 24px;
    }}
    .hero {{
      background: linear-gradient(135deg, #10375c 0%, #1b4d3e 100%);
      color: #fff;
      border-radius: 18px;
      padding: 28px 32px;
      margin-bottom: 20px;
      box-shadow: 0 20px 40px rgba(16, 55, 92, 0.18);
    }}
    .hero h1 {{
      margin: 0 0 8px;
      font-size: 28px;
    }}
    .hero p {{
      margin: 6px 0;
      opacity: 0.92;
    }}
    .cards {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 12px;
      margin-bottom: 20px;
    }}
    .card {{
      background: #fffdf9;
      border: 1px solid #e7dfd0;
      border-radius: 16px;
      padding: 16px;
      box-shadow: 0 10px 24px rgba(70, 62, 48, 0.06);
    }}
    .card h3 {{
      margin: 0 0 10px;
      font-size: 15px;
      color: #6b7280;
    }}
    .metric {{
      font-size: 24px;
      font-weight: 700;
      color: #0f172a;
    }}
    .section {{
      background: #fffdf9;
      border: 1px solid #e7dfd0;
      border-radius: 18px;
      padding: 20px 22px;
      margin-bottom: 18px;
      box-shadow: 0 10px 24px rgba(70, 62, 48, 0.06);
    }}
    .section h2 {{
      margin: 0 0 14px;
      font-size: 22px;
      color: #1f2933;
    }}
    .section h3 {{
      margin: 18px 0 12px;
      font-size: 18px;
      color: #2f4858;
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
      cursor: pointer;
      position: sticky;
      top: 0;
    }}
    .muted {{
      color: #6b7280;
    }}
    .summary-list {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
      gap: 10px;
      margin: 0;
      padding: 0;
      list-style: none;
    }}
    .summary-list li {{
      background: #f8f4ec;
      border-radius: 12px;
      padding: 10px 12px;
    }}
    .toolbar {{
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      margin-bottom: 12px;
    }}
    input, select {{
      padding: 8px 10px;
      border: 1px solid #d7d0c3;
      border-radius: 10px;
      font-family: inherit;
      background: #fff;
    }}
    .footer-links a {{
      color: #0f5b78;
      text-decoration: none;
      margin-right: 12px;
    }}
    .empty {{
      color: #6b7280;
    }}
  </style>
</head>
<body>
  <div class="page">
    <section class="hero">
      <h1>最佳参数组合包 1H 全量正式组合回测报告</h1>
      <p>组合包：<strong>{html.escape(bundle_name)}</strong></p>
      <p>口径：统一资金池、多币种、多策略、多空并行、当前K线收盘确认、下一根1H开盘成交、计入手续费与滑点。</p>
      <p>输出目录：<code>{html.escape(str(OUTPUT_DIR))}</code></p>
    </section>

    <section class="cards">
      {"".join(f"<div class='card'><h3>{html.escape(label)}</h3><div class='metric'>{html.escape(value)}</div></div>" for label, value in summary_cards)}
    </section>

    <section class="section">
      <h2>策略结论摘要</h2>
      <ul class="summary-list">
        {"".join(f"<li><strong>{html.escape(key)}</strong><br>{html.escape(value)}</li>" for key, value in auto_summary.items())}
      </ul>
    </section>

    <section class="section">
      <h2>执行假设</h2>
      <ul>
        {assumptions_html}
      </ul>
      <p class="muted">说明：本次按正式回测标准强制加入 0.03% 单边滑点。做多策略保留“开仓 Maker / 平仓 Taker”费率口径；做空策略按双边 Taker 口径。</p>
    </section>

    <section class="section">
      <h2>数据覆盖</h2>
      {render_table(data_range_df)}
    </section>

    <section class="section">
      <h2>核心图表</h2>
      <div class="grid-2">
        <div>
          <h3>组合净值曲线</h3>
          <img class="chart" src="data:image/png;base64,{equity_chart}" alt="equity">
        </div>
        <div>
          <h3>组合回撤曲线</h3>
          <img class="chart" src="data:image/png;base64,{drawdown_chart}" alt="drawdown">
        </div>
        <div>
          <h3>月度收益热力图</h3>
          <img class="chart" src="data:image/png;base64,{monthly_heatmap}" alt="monthly_heatmap">
        </div>
        <div>
          <h3>年度收益柱状图</h3>
          <img class="chart" src="data:image/png;base64,{yearly_bar}" alt="yearly_bar">
        </div>
        <div>
          <h3>币种贡献图</h3>
          <img class="chart" src="data:image/png;base64,{coin_bar}" alt="coin_bar">
        </div>
        <div>
          <h3>多空贡献图</h3>
          <img class="chart" src="data:image/png;base64,{side_bar}" alt="side_bar">
        </div>
        <div>
          <h3>策略贡献图</h3>
          <img class="chart" src="data:image/png;base64,{strategy_bar}" alt="strategy_bar">
        </div>
        <div>
          <h3>R倍数分布图</h3>
          <img class="chart" src="data:image/png;base64,{r_hist}" alt="r_hist">
        </div>
      </div>
    </section>

    <section class="section">
      <h2>多空统计</h2>
      {render_table(side_summary)}
      <p class="muted">资金利用率：平均 {utilization['average_utilization_pct'] * 100:.2f}% ，峰值 {utilization['peak_utilization_pct'] * 100:.2f}% ，峰值名义仓位 {utilization['peak_notional']:.2f}U。</p>
    </section>

    <section class="section">
      <h2>分币种统计</h2>
      {render_table(symbol_summary)}
    </section>

    <section class="section">
      <h2>策略统计</h2>
      {render_table(strategy_summary)}
    </section>

    <section class="section">
      <h2>月度收益表</h2>
      {render_table(monthly_wide)}
      <h3>年度收益表</h3>
      {render_table(yearly_table)}
    </section>

    <section class="section">
      <h2>风险分析</h2>
      <h3>过拟合风险</h3>
      <p>{html.escape(risk_text["过拟合风险"])}</p>
      <h3>多币种相关性风险</h3>
      <p>{html.escape(risk_text["多币种相关性风险"])}</p>
      {render_table(correlation_table)}
      <h3>空头策略风险</h3>
      <p>{html.escape(risk_text["空头策略风险"])}</p>
      <h3>手续费压力测试</h3>
      {render_table(stress_fee)}
      <h3>滑点压力测试</h3>
      {render_table(stress_slippage)}
      <h3>资金占用风险</h3>
      <p>{html.escape(risk_text["资金占用风险"])}</p>
      {render_table(rejection_summary)}
      <h3>极端行情风险</h3>
      {render_table(regime_table)}
      <h3>回撤定位</h3>
      <p>最大回撤开始：{html.escape(str(drawdown_meta["start_time"]))}；结束：{html.escape(str(drawdown_meta["end_time"]))}；修复时间：{html.escape(str(drawdown_meta["recovery_time"])) or "尚未修复"}。</p>
      <p>年化波动率：{annual_vol * 100:.2f}% ，夏普：{sharpe:.3f} ，卡玛：{calmar:.3f}。</p>
    </section>

    <section class="section">
      <h2>全量交易明细</h2>
      <div class="toolbar">
        <input id="tradeSearch" type="text" placeholder="搜索币种 / 策略 / 平仓原因">
        <select id="sideFilter">
          <option value="">全部方向</option>
          <option value="多头">多头</option>
          <option value="空头">空头</option>
        </select>
        <select id="coinFilter">
          <option value="">全部币种</option>
          {"".join(f"<option value='{html.escape(str(coin))}'>{html.escape(str(coin))}</option>" for coin in sorted(trades_export['币种'].dropna().unique())) if not trades_export.empty else ""}
        </select>
      </div>
      {render_table(trades_export, table_id="tradesTable")}
    </section>

    <section class="section footer-links">
      <h2>导出文件</h2>
      <p>
        <a href="trades.csv">trades.csv</a>
        <a href="summary.csv">summary.csv</a>
        <a href="equity_curve.csv">equity_curve.csv</a>
        <a href="monthly_returns.csv">monthly_returns.csv</a>
        <a href="yearly_returns.csv">yearly_returns.csv</a>
      </p>
    </section>
  </div>

  <script>
    const tradeRows = {json.dumps(trades_js, ensure_ascii=False)};
    const table = document.getElementById("tradesTable");
    const searchInput = document.getElementById("tradeSearch");
    const sideFilter = document.getElementById("sideFilter");
    const coinFilter = document.getElementById("coinFilter");

    function renderFilteredTable() {{
      const keyword = searchInput.value.trim().toLowerCase();
      const side = sideFilter.value;
      const coin = coinFilter.value;
      const tbody = table.querySelector("tbody");
      const rows = [...tbody.querySelectorAll("tr")];
      rows.forEach(row => row.remove());
      tradeRows.forEach(item => {{
        const searchable = JSON.stringify(item).toLowerCase();
        if (keyword && !searchable.includes(keyword)) return;
        if (side && item["方向"] !== side) return;
        if (coin && item["币种"] !== coin) return;
        const tr = document.createElement("tr");
        Object.values(item).forEach(value => {{
          const td = document.createElement("td");
          td.textContent = value;
          tr.appendChild(td);
        }});
        tbody.appendChild(tr);
      }});
    }}

    [searchInput, sideFilter, coinFilter].forEach(el => el.addEventListener("input", renderFilteredTable));
    [sideFilter, coinFilter].forEach(el => el.addEventListener("change", renderFilteredTable));

    table.querySelectorAll("th").forEach((header, index) => {{
      let asc = true;
      header.addEventListener("click", () => {{
        tradeRows.sort((a, b) => {{
          const av = Object.values(a)[index];
          const bv = Object.values(b)[index];
          const an = Number(av);
          const bn = Number(bv);
          let cmp = 0;
          if (!Number.isNaN(an) && !Number.isNaN(bn)) {{
            cmp = an - bn;
          }} else {{
            cmp = String(av).localeCompare(String(bv), "zh-CN");
          }}
          return asc ? cmp : -cmp;
        }});
        asc = !asc;
        renderFilteredTable();
      }});
    }});
  </script>
</body>
</html>"""


def build_executed_trade_frame_from_export(trades_export: pd.DataFrame) -> pd.DataFrame:
    if trades_export.empty:
        return pd.DataFrame(columns=["side", "coin", "strategy_name", "r_multiple", "pnl", "year", "exit_time_bjt"])
    return pd.DataFrame(
        {
            "side": trades_export["方向"],
            "coin": trades_export["币种"],
            "strategy_name": trades_export["策略"],
            "r_multiple": trades_export["R倍数"].astype(float),
            "pnl": trades_export["盈亏"].astype(float),
            "year": pd.to_datetime(trades_export["平仓时间"]).dt.strftime("%Y"),
            "month": pd.to_datetime(trades_export["平仓时间"]).dt.strftime("%Y-%m"),
            "exit_time_bjt": trades_export["平仓时间"],
            "total_fee": trades_export["手续费"].astype(float),
            "slippage_cost": trades_export["滑点成本"].astype(float),
            "funding_cost": 0.0,
        }
    )


def build_period_profit_breakdown(trades: pd.DataFrame, *, period: str) -> pd.DataFrame:
    columns = ["期间", "方向", "币种", "项目", "交易次数", "胜率", "Profit Factor", "当期利润U", "累计利润U"]
    if trades.empty:
        return pd.DataFrame(columns=columns)

    freq = "M" if period == "monthly" else "Y"
    label_fmt = "%Y-%m" if period == "monthly" else "%Y"
    frame = trades.copy()
    frame["period_dt"] = pd.to_datetime(frame["exit_time_bjt"]).dt.to_period(freq).dt.to_timestamp()
    frame["期间"] = frame["period_dt"].dt.strftime(label_fmt)

    rows: list[dict[str, Any]] = []
    for period_dt, period_subset in frame.groupby("period_dt", sort=True):
        period_label = period_dt.strftime(label_fmt)
        for side in ("多头", "空头"):
            side_subset = period_subset[period_subset["side"] == side]
            for coin in sorted(side_subset["coin"].dropna().unique()):
                coin_subset = side_subset[side_subset["coin"] == coin]
                metrics = compute_trade_metrics(coin_subset, INITIAL_CAPITAL)
                rows.append(
                    {
                        "期间": period_label,
                        "方向": side,
                        "币种": coin,
                        "项目": "分项",
                        "交易次数": metrics["trades"],
                        "胜率": round(metrics["win_rate"] * 100.0, 2),
                        "Profit Factor": round(metrics["profit_factor"], 4),
                        "当期利润U": round(metrics["total_pnl"], 2),
                    }
                )
            if not side_subset.empty:
                side_metrics = compute_trade_metrics(side_subset, INITIAL_CAPITAL)
                rows.append(
                    {
                        "期间": period_label,
                        "方向": side,
                        "币种": "合计",
                        "项目": "总项",
                        "交易次数": side_metrics["trades"],
                        "胜率": round(side_metrics["win_rate"] * 100.0, 2),
                        "Profit Factor": round(side_metrics["profit_factor"], 4),
                        "当期利润U": round(side_metrics["total_pnl"], 2),
                    }
                )

        total_metrics = compute_trade_metrics(period_subset, INITIAL_CAPITAL)
        rows.append(
            {
                "期间": period_label,
                "方向": "合计",
                "币种": "合计",
                "项目": "总项",
                "交易次数": total_metrics["trades"],
                "胜率": round(total_metrics["win_rate"] * 100.0, 2),
                "Profit Factor": round(total_metrics["profit_factor"], 4),
                "当期利润U": round(total_metrics["total_pnl"], 2),
            }
        )

    breakdown = pd.DataFrame(rows)
    breakdown["累计利润U"] = (
        breakdown.sort_values(["方向", "币种", "项目", "期间"])
        .groupby(["方向", "币种", "项目"])["当期利润U"]
        .cumsum()
        .round(2)
    )
    return breakdown[columns].sort_values(["期间", "方向", "项目", "币种"]).reset_index(drop=True)


def build_html_report_extended(
    *,
    monthly_breakdown: pd.DataFrame,
    yearly_breakdown: pd.DataFrame,
    **kwargs: Any,
) -> str:
    html_text = build_html_report(**kwargs)
    extra_section = f"""
    <section class="section">
      <h2>月度利润拆分</h2>
      <p class="muted">按平仓时间归属。项目=分项表示单币种，项目=总项表示方向合计或组合合计。</p>
      {render_table(monthly_breakdown)}
      <h3>年度利润拆分</h3>
      {render_table(yearly_breakdown)}
    </section>
    """
    return html_text.replace("</body>", f"{extra_section}\n</body>")


if __name__ == "__main__":
    main()
