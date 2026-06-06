from __future__ import annotations

import html
import json
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from okx_quant.candle_cache import load_candle_cache
from okx_quant.okx_client import OkxRestClient
from okx_quant.persistence import analysis_report_dir_path
from scripts.run_btc_daily_ma_direction_filter_research import format_ts
from scripts.run_common_interval_bnb_refresh_report import run_bnb_bodyatr_short
from scripts.run_leadership_multi_coin_best_params_full_report import (
    LONG_GATES,
    LONG_PROFILES,
    SYMBOLS,
    SYMBOL_LABELS,
    build_concurrent_chart,
    build_equity_chart,
    build_leverage_table,
    concurrent_profile,
    dataframe_to_html,
    figure_to_base64,
    filter_scope,
    fmt2,
    metrics_from_frame,
    run_long_trades,
    run_short_trades,
)
from scripts.run_multi_coin_short_recommendation_and_pullback_report import (
    load_recommendations,
    parse_strategy_key,
)


plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "Noto Sans CJK SC", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False


REPORT_DIR = analysis_report_dir_path()
REPORT_DIR.mkdir(parents=True, exist_ok=True)

STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
HTML_PATH = REPORT_DIR / f"leadership_corrected_full_common_audit_report_{STAMP}.html"
JSON_PATH = REPORT_DIR / f"leadership_corrected_full_common_audit_report_{STAMP}.json"
TRADES_CSV = REPORT_DIR / f"leadership_corrected_full_common_audit_trades_{STAMP}.csv"
SUMMARY_FULL_CSV = REPORT_DIR / f"leadership_corrected_full_common_audit_summary_full_{STAMP}.csv"
SUMMARY_COMMON_CSV = REPORT_DIR / f"leadership_corrected_full_common_audit_summary_common_{STAMP}.csv"
MONTHLY_AGG_FULL_CSV = REPORT_DIR / f"leadership_corrected_full_common_audit_monthly_agg_full_{STAMP}.csv"
MONTHLY_AGG_COMMON_CSV = REPORT_DIR / f"leadership_corrected_full_common_audit_monthly_agg_common_{STAMP}.csv"
MONTHLY_COIN_FULL_CSV = REPORT_DIR / f"leadership_corrected_full_common_audit_monthly_coin_full_{STAMP}.csv"
MONTHLY_COIN_COMMON_CSV = REPORT_DIR / f"leadership_corrected_full_common_audit_monthly_coin_common_{STAMP}.csv"
YEARLY_AGG_FULL_CSV = REPORT_DIR / f"leadership_corrected_full_common_audit_yearly_agg_full_{STAMP}.csv"
YEARLY_AGG_COMMON_CSV = REPORT_DIR / f"leadership_corrected_full_common_audit_yearly_agg_common_{STAMP}.csv"
YEARLY_COIN_FULL_CSV = REPORT_DIR / f"leadership_corrected_full_common_audit_yearly_coin_full_{STAMP}.csv"
YEARLY_COIN_COMMON_CSV = REPORT_DIR / f"leadership_corrected_full_common_audit_yearly_coin_common_{STAMP}.csv"
MARGIN_FULL_CSV = REPORT_DIR / f"leadership_corrected_full_common_audit_margin_full_{STAMP}.csv"
MARGIN_COMMON_CSV = REPORT_DIR / f"leadership_corrected_full_common_audit_margin_common_{STAMP}.csv"
LOSS_MONTHS_FULL_CSV = REPORT_DIR / f"leadership_corrected_full_common_audit_loss_months_full_{STAMP}.csv"
LOSS_MONTHS_COMMON_CSV = REPORT_DIR / f"leadership_corrected_full_common_audit_loss_months_common_{STAMP}.csv"
BNB_COMPARE_CSV = REPORT_DIR / f"leadership_corrected_full_common_audit_bnb_compare_{STAMP}.csv"

INITIAL_CAPITAL = Decimal("10000")
RISK_PER_TRADE_U = Decimal("10")


def main() -> None:
    client = OkxRestClient()
    short_profiles = load_short_profiles()
    data_ranges, common_start_ts, common_end_ts = load_data_ranges()

    all_trade_frames: list[pd.DataFrame] = []
    bnb_compare_rows: list[dict[str, object]] = []

    for symbol in SYMBOLS:
        entry_candles = [c for c in load_candle_cache(symbol, "1H", limit=None) if c.confirmed]
        daily_candles = [c for c in load_candle_cache(symbol, "1D", limit=None) if c.confirmed]
        instrument = client.get_instrument(symbol)

        long_trades = run_long_trades(
            symbol=symbol,
            entry_candles=entry_candles,
            daily_candles=daily_candles,
            instrument=instrument,
            gate=LONG_GATES[symbol],
        )
        if symbol == "BNB-USDT-SWAP":
            old_short = run_short_trades(
                symbol=symbol,
                entry_candles=entry_candles,
                daily_candles=daily_candles,
                profile=short_profiles[symbol],
            )
            new_short = run_bnb_bodyatr_short(entry_candles=entry_candles, daily_candles=daily_candles)
            all_trade_frames.extend([long_trades, new_short])
            bnb_compare_rows.extend(
                build_bnb_compare_rows(
                    long_trades=long_trades,
                    old_short=old_short,
                    new_short=new_short,
                    common_start_ts=common_start_ts,
                    common_end_ts=common_end_ts,
                )
            )
        else:
            short_trades = run_short_trades(
                symbol=symbol,
                entry_candles=entry_candles,
                daily_candles=daily_candles,
                profile=short_profiles[symbol],
            )
            all_trade_frames.extend([long_trades, short_trades])

    trades_full = (
        pd.concat(all_trade_frames, ignore_index=True)
        .sort_values(["exit_ts", "entry_ts", "coin", "side"])
        .reset_index(drop=True)
    )
    trades_full["entry_time"] = pd.to_datetime(trades_full["entry_ts"], unit="ms", utc=True)
    trades_full["exit_time"] = pd.to_datetime(trades_full["exit_ts"], unit="ms", utc=True)
    trades_full["period_month"] = trades_full["exit_time"].dt.strftime("%Y-%m")
    trades_full["period_year"] = trades_full["exit_time"].dt.strftime("%Y")

    trades_common = filter_scope(trades_full, start_ts=common_start_ts, end_ts=common_end_ts)
    trades_common["entry_time"] = pd.to_datetime(trades_common["entry_ts"], unit="ms", utc=True)
    trades_common["exit_time"] = pd.to_datetime(trades_common["exit_ts"], unit="ms", utc=True)
    trades_common["period_month"] = trades_common["exit_time"].dt.strftime("%Y-%m")
    trades_common["period_year"] = trades_common["exit_time"].dt.strftime("%Y")

    summary_full = build_scope_summary(trades_full, "full")
    summary_common = build_scope_summary(trades_common, "common")
    monthly_agg_full = build_period_summary(trades_full, "period_month", "full", by_coin=False)
    monthly_agg_common = build_period_summary(trades_common, "period_month", "common", by_coin=False)
    monthly_coin_full = build_period_summary(trades_full, "period_month", "full", by_coin=True)
    monthly_coin_common = build_period_summary(trades_common, "period_month", "common", by_coin=True)
    yearly_agg_full = build_period_summary(trades_full, "period_year", "full", by_coin=False)
    yearly_agg_common = build_period_summary(trades_common, "period_year", "common", by_coin=False)
    yearly_coin_full = build_period_summary(trades_full, "period_year", "full", by_coin=True)
    yearly_coin_common = build_period_summary(trades_common, "period_year", "common", by_coin=True)
    loss_months_full = build_negative_months(monthly_agg_full)
    loss_months_common = build_negative_months(monthly_agg_common)
    margin_full = build_margin_sufficiency_table(build_leverage_table(trades_full))
    margin_common = build_margin_sufficiency_table(build_leverage_table(trades_common))
    param_table = build_param_table(short_profiles)
    audit_table = build_audit_table()

    TRADES_CSV.write_text(trades_full.to_csv(index=False, encoding="utf-8-sig"), encoding="utf-8-sig")
    summary_full.to_csv(SUMMARY_FULL_CSV, index=False, encoding="utf-8-sig")
    summary_common.to_csv(SUMMARY_COMMON_CSV, index=False, encoding="utf-8-sig")
    monthly_agg_full.to_csv(MONTHLY_AGG_FULL_CSV, index=False, encoding="utf-8-sig")
    monthly_agg_common.to_csv(MONTHLY_AGG_COMMON_CSV, index=False, encoding="utf-8-sig")
    monthly_coin_full.to_csv(MONTHLY_COIN_FULL_CSV, index=False, encoding="utf-8-sig")
    monthly_coin_common.to_csv(MONTHLY_COIN_COMMON_CSV, index=False, encoding="utf-8-sig")
    yearly_agg_full.to_csv(YEARLY_AGG_FULL_CSV, index=False, encoding="utf-8-sig")
    yearly_agg_common.to_csv(YEARLY_AGG_COMMON_CSV, index=False, encoding="utf-8-sig")
    yearly_coin_full.to_csv(YEARLY_COIN_FULL_CSV, index=False, encoding="utf-8-sig")
    yearly_coin_common.to_csv(YEARLY_COIN_COMMON_CSV, index=False, encoding="utf-8-sig")
    margin_full.to_csv(MARGIN_FULL_CSV, index=False, encoding="utf-8-sig")
    margin_common.to_csv(MARGIN_COMMON_CSV, index=False, encoding="utf-8-sig")
    loss_months_full.to_csv(LOSS_MONTHS_FULL_CSV, index=False, encoding="utf-8-sig")
    loss_months_common.to_csv(LOSS_MONTHS_COMMON_CSV, index=False, encoding="utf-8-sig")
    pd.DataFrame(bnb_compare_rows).to_csv(BNB_COMPARE_CSV, index=False, encoding="utf-8-sig")

    payload = build_payload(
        data_ranges=data_ranges,
        common_start_ts=common_start_ts,
        common_end_ts=common_end_ts,
        summary_full=summary_full,
        summary_common=summary_common,
        loss_months_full=loss_months_full,
        loss_months_common=loss_months_common,
        margin_full=margin_full,
        margin_common=margin_common,
        param_table=param_table,
        audit_table=audit_table,
        bnb_compare_rows=bnb_compare_rows,
    )
    JSON_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    HTML_PATH.write_text(
        build_html(
            trades_full=trades_full,
            trades_common=trades_common,
            summary_full=summary_full,
            summary_common=summary_common,
            monthly_agg_full=monthly_agg_full,
            monthly_agg_common=monthly_agg_common,
            monthly_coin_full=monthly_coin_full,
            monthly_coin_common=monthly_coin_common,
            yearly_agg_full=yearly_agg_full,
            yearly_agg_common=yearly_agg_common,
            yearly_coin_full=yearly_coin_full,
            yearly_coin_common=yearly_coin_common,
            loss_months_full=loss_months_full,
            loss_months_common=loss_months_common,
            margin_full=margin_full,
            margin_common=margin_common,
            data_ranges=data_ranges,
            common_start_ts=common_start_ts,
            common_end_ts=common_end_ts,
            param_table=param_table,
            audit_table=audit_table,
            bnb_compare_rows=bnb_compare_rows,
        ),
        encoding="utf-8",
    )
    print(HTML_PATH)


def load_short_profiles() -> dict[str, dict[str, object]]:
    profiles: dict[str, dict[str, object]] = {}
    for item in load_recommendations():
        ma_type, period = parse_strategy_key(item.strategy_key)
        profiles[item.symbol] = {
            "symbol": item.symbol,
            "coin": item.coin,
            "strategy_key": item.strategy_key,
            "strategy_label": item.strategy_label,
            "ma_type": ma_type,
            "period": period,
            "daily_filter_key": item.daily_filter_key,
            "daily_filter_label": item.daily_filter_label,
            "slope_threshold_ratio": Decimal("-0.0005"),
            "atr_period": 14,
            "atr_stop_multiplier": Decimal("2"),
            "atr_percentile_max": Decimal("0.5"),
            "exit_model": "2R保本后逐级锁盈",
        }
    return profiles


def load_data_ranges() -> tuple[dict[str, dict[str, object]], int, int]:
    data_ranges: dict[str, dict[str, object]] = {}
    starts: list[int] = []
    ends: list[int] = []
    for symbol in SYMBOLS:
        entry_candles = [c for c in load_candle_cache(symbol, "1H", limit=None) if c.confirmed]
        daily_candles = [c for c in load_candle_cache(symbol, "1D", limit=None) if c.confirmed]
        starts.append(entry_candles[0].ts)
        ends.append(entry_candles[-1].ts)
        data_ranges[symbol] = {
            "entry_candles": len(entry_candles),
            "daily_candles": len(daily_candles),
            "start_utc": format_ts(entry_candles[0].ts),
            "end_utc": format_ts(entry_candles[-1].ts),
        }
    return data_ranges, max(starts), min(ends)


def build_scope_summary(trades: pd.DataFrame, scope_label: str) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for coin in [*sorted(trades["coin"].unique()), "ALL"]:
        base = trades if coin == "ALL" else trades[trades["coin"] == coin]
        for side in ("long", "short", "combined"):
            subset = base if side == "combined" else base[base["side"] == side]
            metrics = metrics_from_frame(subset)
            rows.append(
                {
                    "scope": scope_label,
                    "coin": coin,
                    "side": side,
                    "trades": int(metrics["trades"]),
                    "total_pnl_u": metrics["total_pnl_u"],
                    "profit_factor": metrics["profit_factor"],
                    "win_rate": float(metrics["win_rate"]) * 100,
                    "avg_r": metrics["avg_r"],
                    "avg_hold_hours": metrics["avg_hold_hours"],
                    "max_drawdown_u": metrics["max_drawdown_u"],
                    "return_pct_on_10k": metrics["return_pct_on_10k"],
                }
            )
    return pd.DataFrame(rows)


def build_period_summary(trades: pd.DataFrame, period_col: str, scope_label: str, *, by_coin: bool) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    coins = sorted(trades["coin"].unique()) if by_coin else ["ALL"]
    for period in sorted(trades[period_col].unique()):
        period_frame = trades[trades[period_col] == period]
        for coin in coins:
            base = period_frame if coin == "ALL" else period_frame[period_frame["coin"] == coin]
            for side in ("long", "short", "combined"):
                subset = base if side == "combined" else base[base["side"] == side]
                metrics = metrics_from_frame(subset)
                rows.append(
                    {
                        "scope": scope_label,
                        "period": period,
                        "coin": coin,
                        "side": side,
                        "trades": int(metrics["trades"]),
                        "total_pnl_u": metrics["total_pnl_u"],
                        "profit_factor": metrics["profit_factor"],
                        "win_rate": float(metrics["win_rate"]) * 100,
                        "avg_r": metrics["avg_r"],
                        "avg_hold_hours": metrics["avg_hold_hours"],
                        "max_drawdown_u": metrics["max_drawdown_u"],
                        "return_pct_on_10k": metrics["return_pct_on_10k"],
                    }
                )
    return pd.DataFrame(rows)


def build_negative_months(monthly_agg: pd.DataFrame) -> pd.DataFrame:
    return monthly_agg[
        (monthly_agg["coin"] == "ALL")
        & (monthly_agg["side"] == "combined")
        & (monthly_agg["total_pnl_u"] < 0)
    ].copy().sort_values("period")


def build_margin_sufficiency_table(margin_table: pd.DataFrame) -> pd.DataFrame:
    frame = margin_table.copy()
    frame["enough_for_10k_hist_peak"] = frame["historical_max_margin_usdt"].astype(float) <= 10000.0
    frame["enough_for_10k_hist_peak_plus30pct"] = frame["historical_max_margin_plus30pct_usdt"].astype(float) <= 10000.0
    frame["enough_for_10k_conservative"] = frame["conservative_upper_margin_usdt"].astype(float) <= 10000.0
    frame["enough_for_10k_conservative_plus30pct"] = frame["conservative_upper_plus30pct_usdt"].astype(float) <= 10000.0
    return frame


def build_param_table(short_profiles: dict[str, dict[str, object]]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for symbol in SYMBOLS:
        long_profile = LONG_PROFILES[symbol]
        long_gate = LONG_GATES[symbol]
        rows.append(
            {
                "coin": SYMBOL_LABELS[symbol],
                "side": "long",
                "strategy": "1H EMA动态委托做多",
                "fast_line": f"EMA{long_profile.ema_period}",
                "trend_line": f"EMA{long_profile.trend_ema_period}",
                "entry_reference": "跟随快线" if long_profile.entry_reference_ema_period <= 0 else f"EMA{long_profile.entry_reference_ema_period}",
                "daily_filter": long_gate.label,
                "risk_model": "fixed_risk 10U",
                "atr_period": 10,
                "atr_stop": f"x{format_decimal_fixed(long_profile.atr_stop_multiplier, 1)}",
                "entry_logic": "回踩参考线挂单；每段趋势最多1次",
                "exit_logic": "动态止盈 + 2R保本 + 手续费偏移",
            }
        )
        if symbol == "BNB-USDT-SWAP":
            rows.append(
                {
                    "coin": "BNB",
                    "side": "short",
                    "strategy": "1H MA20斜率破位回抽做空",
                    "fast_line": "MA20",
                    "trend_line": "-",
                    "entry_reference": "回抽MA20近线做空",
                    "daily_filter": "仅使用上一根已收日线；弱日定义=日线收跌",
                    "risk_model": "fixed_risk 10U",
                    "atr_period": 14,
                    "atr_stop": "高点+0.3ATR，且不少于0.5ATR",
                    "entry_logic": "slope<=-0.0005；ATR分位<=0.5；body/ATR<=1.0；breakdown=0.2ATR；retest=0.3ATR；watch=6 bars",
                    "exit_logic": "2R保本后逐级锁盈",
                }
            )
        else:
            profile = short_profiles[symbol]
            rows.append(
                {
                    "coin": SYMBOL_LABELS[symbol],
                    "side": "short",
                    "strategy": "1H 斜率做空",
                    "fast_line": f"{profile['ma_type'].upper()}{profile['period']}",
                    "trend_line": "-",
                    "entry_reference": "收盘确认入场",
                    "daily_filter": f"仅使用上一根已收日线；{profile['daily_filter_label']}",
                    "risk_model": "fixed_risk 10U",
                    "atr_period": profile["atr_period"],
                    "atr_stop": f"x{format_decimal_fixed(profile['atr_stop_multiplier'], 1)}",
                    "entry_logic": f"slope<={profile['slope_threshold_ratio']}；ATR分位<={profile['atr_percentile_max']}",
                    "exit_logic": profile["exit_model"],
                }
            )
    return pd.DataFrame(rows)


def build_audit_table() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "audit_item": "日线过滤是否使用昨天收盘",
                "status": "Yes",
                "detail": "已修正为仅使用当时已经收盘的上一根日线。实现方式是把1D K线的可用时间定义为 ts + 1D周期长度，1H入场只能读取该时间之前的日线值。",
            },
            {
                "audit_item": "是否使用未确认K线",
                "status": "No",
                "detail": "1H与1D样本都明确过滤 confirmed=True，未收盘K线不参与研究和回测。",
            },
            {
                "audit_item": "公共区间指标预热",
                "status": "Expected",
                "detail": "公共区间统计只截取共同时间段内开平仓的交易，但均线和ATR允许使用区间前历史做预热，这属于正常做法，不是未来函数。",
            },
            {
                "audit_item": "资金占用与保证金",
                "status": "Approximation",
                "detail": "报告给出名义价值并发峰值与不同杠杆下的保证金估算，但未模拟交易所逐仓/全仓强平路径、资金费、临时提保、滑点放大。",
            },
            {
                "audit_item": "成交与市场冲击",
                "status": "Open Risk",
                "detail": "当前回测已含手续费，但未额外建模订单簿滑点、成交深度限制、极端跳空与流动性抽干。",
            },
        ]
    )


def build_bnb_compare_rows(
    *,
    long_trades: pd.DataFrame,
    old_short: pd.DataFrame,
    new_short: pd.DataFrame,
    common_start_ts: int,
    common_end_ts: int,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for scope_name, scope_long, scope_old_short, scope_new_short in [
        (
            "full",
            long_trades,
            old_short,
            new_short,
        ),
        (
            "common",
            filter_scope(long_trades, start_ts=common_start_ts, end_ts=common_end_ts),
            filter_scope(old_short, start_ts=common_start_ts, end_ts=common_end_ts),
            filter_scope(new_short, start_ts=common_start_ts, end_ts=common_end_ts),
        ),
    ]:
        old_combined = pd.concat([scope_long, scope_old_short], ignore_index=True).sort_values(["exit_ts", "entry_ts"])
        new_combined = pd.concat([scope_long, scope_new_short], ignore_index=True).sort_values(["exit_ts", "entry_ts"])
        for label, frame in [
            (f"{scope_name}_BNB_long_only", scope_long),
            (f"{scope_name}_BNB_old_short", scope_old_short),
            (f"{scope_name}_BNB_new_short", scope_new_short),
            (f"{scope_name}_BNB_old_combined", old_combined),
            (f"{scope_name}_BNB_new_combined", new_combined),
        ]:
            metrics = metrics_from_frame(frame)
            rows.append(
                {
                    "scope": scope_name,
                    "view": label,
                    "trades": int(metrics["trades"]),
                    "total_pnl_u": metrics["total_pnl_u"],
                    "profit_factor": metrics["profit_factor"],
                    "win_rate": float(metrics["win_rate"]) * 100,
                    "max_drawdown_u": metrics["max_drawdown_u"],
                }
            )
        old_short_m = metrics_from_frame(scope_old_short)
        new_short_m = metrics_from_frame(scope_new_short)
        old_combo_m = metrics_from_frame(old_combined)
        new_combo_m = metrics_from_frame(new_combined)
        rows.append(
            {
                "scope": scope_name,
                "view": f"{scope_name}_delta_new_minus_old_short",
                "trades": int(new_short_m["trades"] - old_short_m["trades"]),
                "total_pnl_u": new_short_m["total_pnl_u"] - old_short_m["total_pnl_u"],
                "profit_factor": new_short_m["profit_factor"] - old_short_m["profit_factor"],
                "win_rate": (float(new_short_m["win_rate"]) - float(old_short_m["win_rate"])) * 100,
                "max_drawdown_u": new_short_m["max_drawdown_u"] - old_short_m["max_drawdown_u"],
            }
        )
        rows.append(
            {
                "scope": scope_name,
                "view": f"{scope_name}_delta_new_minus_old_combined",
                "trades": int(new_combo_m["trades"] - old_combo_m["trades"]),
                "total_pnl_u": new_combo_m["total_pnl_u"] - old_combo_m["total_pnl_u"],
                "profit_factor": new_combo_m["profit_factor"] - old_combo_m["profit_factor"],
                "win_rate": (float(new_combo_m["win_rate"]) - float(old_combo_m["win_rate"])) * 100,
                "max_drawdown_u": new_combo_m["max_drawdown_u"] - old_combo_m["max_drawdown_u"],
            }
        )
    return rows


def build_payload(
    *,
    data_ranges: dict[str, dict[str, object]],
    common_start_ts: int,
    common_end_ts: int,
    summary_full: pd.DataFrame,
    summary_common: pd.DataFrame,
    loss_months_full: pd.DataFrame,
    loss_months_common: pd.DataFrame,
    margin_full: pd.DataFrame,
    margin_common: pd.DataFrame,
    param_table: pd.DataFrame,
    audit_table: pd.DataFrame,
    bnb_compare_rows: list[dict[str, object]],
) -> dict[str, object]:
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "risk_per_trade_u": str(RISK_PER_TRADE_U),
        "initial_capital_u": str(INITIAL_CAPITAL),
        "common_interval": {
            "start_utc": format_ts(common_start_ts),
            "end_utc": format_ts(common_end_ts),
        },
        "data_ranges": data_ranges,
        "summary_full": summary_full.to_dict("records"),
        "summary_common": summary_common.to_dict("records"),
        "loss_months_full": loss_months_full.to_dict("records"),
        "loss_months_common": loss_months_common.to_dict("records"),
        "margin_full": margin_full.to_dict("records"),
        "margin_common": margin_common.to_dict("records"),
        "parameter_table": param_table.to_dict("records"),
        "audit_table": audit_table.to_dict("records"),
        "bnb_replace_compare": bnb_compare_rows,
    }


def build_html(
    *,
    trades_full: pd.DataFrame,
    trades_common: pd.DataFrame,
    summary_full: pd.DataFrame,
    summary_common: pd.DataFrame,
    monthly_agg_full: pd.DataFrame,
    monthly_agg_common: pd.DataFrame,
    monthly_coin_full: pd.DataFrame,
    monthly_coin_common: pd.DataFrame,
    yearly_agg_full: pd.DataFrame,
    yearly_agg_common: pd.DataFrame,
    yearly_coin_full: pd.DataFrame,
    yearly_coin_common: pd.DataFrame,
    loss_months_full: pd.DataFrame,
    loss_months_common: pd.DataFrame,
    margin_full: pd.DataFrame,
    margin_common: pd.DataFrame,
    data_ranges: dict[str, dict[str, object]],
    common_start_ts: int,
    common_end_ts: int,
    param_table: pd.DataFrame,
    audit_table: pd.DataFrame,
    bnb_compare_rows: list[dict[str, object]],
) -> str:
    full_total = pick_summary(summary_full, "ALL", "combined")
    full_long = pick_summary(summary_full, "ALL", "long")
    full_short = pick_summary(summary_full, "ALL", "short")
    common_total = pick_summary(summary_common, "ALL", "combined")
    common_long = pick_summary(summary_common, "ALL", "long")
    common_short = pick_summary(summary_common, "ALL", "short")

    full_final_equity = float(INITIAL_CAPITAL) + float(full_total["total_pnl_u"])
    common_final_equity = float(INITIAL_CAPITAL) + float(common_total["total_pnl_u"])

    full_concurrent = concurrent_profile(trades_full)
    common_concurrent = concurrent_profile(trades_common)
    full_max_notional = float(full_concurrent["total_notional_usdt"].max()) if not full_concurrent.empty else 0.0
    common_max_notional = float(common_concurrent["total_notional_usdt"].max()) if not common_concurrent.empty else 0.0
    full_max_positions = int(full_concurrent["open_positions"].max()) if not full_concurrent.empty else 0
    common_max_positions = int(common_concurrent["open_positions"].max()) if not common_concurrent.empty else 0

    full_monthly_neg_count = len(loss_months_full)
    common_monthly_neg_count = len(loss_months_common)
    full_worst_month = loss_months_full.iloc[loss_months_full["total_pnl_u"].argmin()] if not loss_months_full.empty else None
    common_worst_month = loss_months_common.iloc[loss_months_common["total_pnl_u"].argmin()] if not loss_months_common.empty else None

    full_hist_plus30 = first_true_leverage(margin_full, "enough_for_10k_hist_peak_plus30pct")
    common_hist_plus30 = first_true_leverage(margin_common, "enough_for_10k_hist_peak_plus30pct")
    full_cons_plus30 = first_true_leverage(margin_full, "enough_for_10k_conservative_plus30pct")
    common_cons_plus30 = first_true_leverage(margin_common, "enough_for_10k_conservative_plus30pct")

    equity_full = figure_to_base64(build_equity_chart(trades_full, "全量综合资金曲线"))
    equity_common = figure_to_base64(build_equity_chart(trades_common, "公共区间综合资金曲线"))
    side_curve_full = figure_to_base64(build_side_curve_chart(trades_full, "全量多空累计资金曲线"))
    side_curve_common = figure_to_base64(build_side_curve_chart(trades_common, "公共区间多空累计资金曲线"))
    monthly_full = figure_to_base64(build_monthly_pnl_bar(monthly_agg_full, "全量综合月度盈亏"))
    monthly_common = figure_to_base64(build_monthly_pnl_bar(monthly_agg_common, "公共区间综合月度盈亏"))
    notional_full = figure_to_base64(build_concurrent_chart(full_concurrent, "全量并发名义价值"))
    notional_common = figure_to_base64(build_concurrent_chart(common_concurrent, "公共区间并发名义价值"))

    float_cols_summary = {
        "total_pnl_u": 2,
        "profit_factor": 3,
        "win_rate": 2,
        "avg_r": 3,
        "avg_hold_hours": 1,
        "max_drawdown_u": 2,
        "return_pct_on_10k": 2,
    }
    float_cols_margin = {
        "historical_max_margin_usdt": 2,
        "historical_max_margin_plus30pct_usdt": 2,
        "conservative_upper_margin_usdt": 2,
        "conservative_upper_plus30pct_usdt": 2,
    }
    float_cols_bnb = {
        "total_pnl_u": 2,
        "profit_factor": 3,
        "win_rate": 2,
        "max_drawdown_u": 2,
    }

    ranges_table = dataframe_to_html(
        pd.DataFrame(
            [
                {
                    "coin": SYMBOL_LABELS[symbol],
                    "start_utc": data_ranges[symbol]["start_utc"],
                    "end_utc": data_ranges[symbol]["end_utc"],
                    "entry_candles": data_ranges[symbol]["entry_candles"],
                    "daily_candles": data_ranges[symbol]["daily_candles"],
                }
                for symbol in SYMBOLS
            ]
        ),
        float_cols=None,
    )

    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <title>五币全量与公共区间领导审计报告</title>
  <style>
    :root {{
      --bg:#f4f7fb; --panel:#ffffff; --ink:#152238; --muted:#63748a; --line:#dbe3ed;
      --brand:#123a64; --brand2:#0f766e; --green:#166534; --red:#b42318; --amber:#b45309;
    }}
    * {{ box-sizing:border-box; }}
    body {{ margin:0; font-family:"Microsoft YaHei","Segoe UI",sans-serif; background:var(--bg); color:var(--ink); }}
    .wrap {{ max-width:1620px; margin:0 auto; padding:28px; }}
    .hero {{
      background:linear-gradient(135deg,#102033 0%,#1c4470 48%,#0f766e 100%);
      color:#fff; border-radius:26px; padding:30px 34px; box-shadow:0 20px 48px rgba(15,23,42,.20);
    }}
    .hero h1 {{ margin:0 0 12px; font-size:34px; }}
    .hero p {{ margin:8px 0; line-height:1.78; color:rgba(255,255,255,.93); }}
    .meta {{ display:flex; flex-wrap:wrap; gap:12px; margin-top:18px; }}
    .chip {{ background:rgba(255,255,255,.12); border:1px solid rgba(255,255,255,.18); border-radius:999px; padding:8px 12px; font-size:13px; }}
    .grid {{ display:grid; grid-template-columns:repeat(6,minmax(0,1fr)); gap:16px; margin:22px 0 8px; }}
    .card, .section {{ background:var(--panel); border:1px solid var(--line); border-radius:22px; box-shadow:0 10px 22px rgba(15,23,42,.05); }}
    .card {{ padding:18px; }}
    .card h3 {{ margin:0 0 10px; font-size:16px; }}
    .card p {{ margin:0; color:var(--muted); line-height:1.7; }}
    .section {{ margin-top:22px; padding:24px; }}
    .section h2 {{ margin:0 0 14px; font-size:24px; }}
    .section p, .section li {{ color:var(--muted); line-height:1.8; }}
    .twocol {{ display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:18px; }}
    .table-wrap {{ max-height:560px; overflow:auto; border:1px solid var(--line); border-radius:16px; background:#fff; }}
    .chart {{ background:#fbfdff; border:1px solid var(--line); border-radius:18px; padding:16px; }}
    .chart img {{ width:100%; display:block; border-radius:12px; }}
    table {{ width:100%; border-collapse:collapse; font-size:13px; }}
    th, td {{ padding:9px 10px; border-bottom:1px solid var(--line); text-align:right; white-space:nowrap; }}
    th:first-child, td:first-child {{ text-align:left; }}
    thead th {{ color:var(--muted); font-weight:700; background:#f8fbff; position:sticky; top:0; }}
    .note {{ margin-top:14px; padding:14px 16px; border-left:4px solid var(--brand); background:#eef4ff; border-radius:14px; color:#274064; }}
    @media (max-width:1260px) {{ .grid, .twocol {{ grid-template-columns:1fr; }} }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <h1>五币全量与公共区间领导审计报告</h1>
      <p>这份报告使用修正后的无未来数据口径，重新回放 `BTC / ETH / SOL / BNB / DOGE` 五个币种。每个币种都按 <strong>固定风险 10U</strong> 开仓，展示全量样本与五币公共区间两套结果，并把参数、月度、年度、多空分项、综合资金曲线、保证金估算和方法审计一起展开。</p>
      <p>本版最关键的纠错是：<strong>日线过滤不再提前读取当天未收盘日K</strong>。现在 `1H` 入场只能看到当时已经收盘的上一根 `1D` K线，因此报告结果可作为正式汇报口径。</p>
      <div class="meta">
        <div class="chip">风险口径：每笔 10U</div>
        <div class="chip">展示资金：10000U</div>
        <div class="chip">公共区间：{format_ts(common_start_ts)} -> {format_ts(common_end_ts)}</div>
        <div class="chip">五币共同覆盖口径已单独列出</div>
        <div class="chip">BNB 空头使用最新 Body/ATR 新参数</div>
        <div class="chip">输出文件：{html.escape(str(HTML_PATH))}</div>
      </div>
    </section>

    <section class="grid">
      <div class="card"><h3>全量综合盈利</h3><p>{fmt2(full_total["total_pnl_u"])}U<br>期末约 {fmt2(full_final_equity)}U</p></div>
      <div class="card"><h3>全量多 / 空</h3><p>Long {fmt2(full_long["total_pnl_u"])}U<br>Short {fmt2(full_short["total_pnl_u"])}U</p></div>
      <div class="card"><h3>全量亏损月份</h3><p>{full_monthly_neg_count} 个{f"<br>最差 {full_worst_month['period']} / {fmt2(full_worst_month['total_pnl_u'])}U" if full_worst_month is not None else ""}</p></div>
      <div class="card"><h3>全量10000U够吗</h3><p>历史峰值+30%：{full_hist_plus30}<br>保守上沿+30%：{full_cons_plus30}</p></div>
      <div class="card"><h3>公共区间综合盈利</h3><p>{fmt2(common_total["total_pnl_u"])}U<br>期末约 {fmt2(common_final_equity)}U</p></div>
      <div class="card"><h3>公共区间10000U够吗</h3><p>历史峰值+30%：{common_hist_plus30}<br>保守上沿+30%：{common_cons_plus30}</p></div>
    </section>

    <section class="section">
      <h2>执行结论</h2>
      <ul>
        <li>日线过滤现在等价于“只用昨天已经收盘的日线信息”，不再存在当天日线未收盘就提前读取收盘价的问题。</li>
        <li>全量样本与公共区间都单独给出，避免把不同币种起点差异混在一起误读。</li>
        <li>`BNB` 空头新参数已经并入正式组合，报告里单独列出新旧效果对比。</li>
        <li>`10000U` 是否充足不再只给一句话，而是分成“历史峰值 + 30%缓冲”和“保守上沿 + 30%缓冲”两种口径判断。</li>
      </ul>
      <div class="note">
        这份报告更像“审计版经营看板”，不是单纯的回测截图。数字、参数和方法论是同一口径的，便于给大领导直接过目。
      </div>
    </section>

    <section class="section">
      <h2>方法审计</h2>
      <div class="table-wrap">{dataframe_to_html(audit_table, float_cols=None)}</div>
      <div class="note">
        重点结论：<strong>日线过滤使用的是上一根已收日线</strong>。如果某根 `1H` 发生在 `2025-06-06 13:00 UTC`，它看到的是已经在 `2025-06-06 00:00 UTC` 收完的那根日线，也就是通常口语里的“昨天收盘”，而不是正在形成中的当天日K。
      </div>
    </section>

    <section class="section">
      <h2>详细参数总表</h2>
      <div class="table-wrap">{dataframe_to_html(param_table, float_cols=None)}</div>
    </section>

    <section class="section">
      <h2>BNB 新旧对比</h2>
      <div class="table-wrap">{dataframe_to_html(pd.DataFrame(bnb_compare_rows), float_cols=float_cols_bnb)}</div>
      <div class="note">
        这张表同时列出全量和公共区间两套 `BNB` 结果。关注 `delta_new_minus_old_short` 与 `delta_new_minus_old_combined` 两行，就能直接看到新参数替换后到底带来了多少净改善。
      </div>
    </section>

    <section class="section">
      <h2>资金曲线与并发名义</h2>
      <div class="twocol">
        <div class="chart"><img src="data:image/png;base64,{equity_full}" alt="全量综合资金曲线" /></div>
        <div class="chart"><img src="data:image/png;base64,{equity_common}" alt="公共区间综合资金曲线" /></div>
      </div>
      <div class="twocol" style="margin-top:18px;">
        <div class="chart"><img src="data:image/png;base64,{side_curve_full}" alt="全量多空累计资金曲线" /></div>
        <div class="chart"><img src="data:image/png;base64,{side_curve_common}" alt="公共区间多空累计资金曲线" /></div>
      </div>
      <div class="twocol" style="margin-top:18px;">
        <div class="chart"><img src="data:image/png;base64,{monthly_full}" alt="全量综合月度盈亏" /></div>
        <div class="chart"><img src="data:image/png;base64,{monthly_common}" alt="公共区间综合月度盈亏" /></div>
      </div>
      <div class="twocol" style="margin-top:18px;">
        <div class="chart"><img src="data:image/png;base64,{notional_full}" alt="全量并发名义价值" /></div>
        <div class="chart"><img src="data:image/png;base64,{notional_common}" alt="公共区间并发名义价值" /></div>
      </div>
    </section>

    <section class="section">
      <h2>综合汇总</h2>
      <h3>全量</h3>
      <div class="table-wrap">{dataframe_to_html(summary_full[["coin", "side", "trades", "total_pnl_u", "profit_factor", "win_rate", "avg_r", "avg_hold_hours", "max_drawdown_u", "return_pct_on_10k"]], float_cols=float_cols_summary)}</div>
      <h3 style="margin-top:18px;">公共区间</h3>
      <div class="table-wrap">{dataframe_to_html(summary_common[["coin", "side", "trades", "total_pnl_u", "profit_factor", "win_rate", "avg_r", "avg_hold_hours", "max_drawdown_u", "return_pct_on_10k"]], float_cols=float_cols_summary)}</div>
    </section>

    <section class="section">
      <h2>综合亏损月份</h2>
      <h3>全量</h3>
      <div class="table-wrap">{dataframe_to_html(loss_months_full if not loss_months_full.empty else pd.DataFrame([{{"period":"none","coin":"ALL","side":"combined","trades":0,"total_pnl_u":0.0,"profit_factor":0.0,"win_rate":0.0,"avg_r":0.0,"avg_hold_hours":0.0,"max_drawdown_u":0.0,"return_pct_on_10k":0.0}}]), float_cols=float_cols_summary)}</div>
      <h3 style="margin-top:18px;">公共区间</h3>
      <div class="table-wrap">{dataframe_to_html(loss_months_common if not loss_months_common.empty else pd.DataFrame([{{"period":"none","coin":"ALL","side":"combined","trades":0,"total_pnl_u":0.0,"profit_factor":0.0,"win_rate":0.0,"avg_r":0.0,"avg_hold_hours":0.0,"max_drawdown_u":0.0,"return_pct_on_10k":0.0}}]), float_cols=float_cols_summary)}</div>
    </section>

    <section class="section">
      <h2>年度综合表</h2>
      <h3>全量</h3>
      <div class="table-wrap">{dataframe_to_html(yearly_agg_full, float_cols=float_cols_summary)}</div>
      <h3 style="margin-top:18px;">公共区间</h3>
      <div class="table-wrap">{dataframe_to_html(yearly_agg_common, float_cols=float_cols_summary)}</div>
    </section>

    <section class="section">
      <h2>年度分币种表</h2>
      <h3>全量</h3>
      <div class="table-wrap">{dataframe_to_html(yearly_coin_full, float_cols=float_cols_summary)}</div>
      <h3 style="margin-top:18px;">公共区间</h3>
      <div class="table-wrap">{dataframe_to_html(yearly_coin_common, float_cols=float_cols_summary)}</div>
    </section>

    <section class="section">
      <h2>月度综合表</h2>
      <h3>全量</h3>
      <div class="table-wrap">{dataframe_to_html(monthly_agg_full, float_cols=float_cols_summary)}</div>
      <h3 style="margin-top:18px;">公共区间</h3>
      <div class="table-wrap">{dataframe_to_html(monthly_agg_common, float_cols=float_cols_summary)}</div>
    </section>

    <section class="section">
      <h2>月度分币种表</h2>
      <h3>全量</h3>
      <div class="table-wrap">{dataframe_to_html(monthly_coin_full, float_cols=float_cols_summary)}</div>
      <h3 style="margin-top:18px;">公共区间</h3>
      <div class="table-wrap">{dataframe_to_html(monthly_coin_common, float_cols=float_cols_summary)}</div>
    </section>

    <section class="section">
      <h2>保证金充足性</h2>
      <p>这里的保证金评估分两层。第一层是历史真实并发峰值名义价值，第二层是更保守的“各币种多空最大单边名义相加”上沿。每层再加一档 `30%` 缓冲，用来判断 `10000U` 是否够用。</p>
      <ul>
        <li>全量峰值并发名义：{fmt2(full_max_notional)}U，最大同时持仓：{full_max_positions} 笔</li>
        <li>公共区间峰值并发名义：{fmt2(common_max_notional)}U，最大同时持仓：{common_max_positions} 笔</li>
        <li>全量 `10000U` 在“历史峰值+30%”口径下，从 {full_hist_plus30} 开始够用；在“保守上沿+30%”口径下，从 {full_cons_plus30} 开始够用。</li>
        <li>公共区间 `10000U` 在“历史峰值+30%”口径下，从 {common_hist_plus30} 开始够用；在“保守上沿+30%”口径下，从 {common_cons_plus30} 开始够用。</li>
      </ul>
      <h3>全量</h3>
      <div class="table-wrap">{dataframe_to_html(margin_full, float_cols=float_cols_margin)}</div>
      <h3 style="margin-top:18px;">公共区间</h3>
      <div class="table-wrap">{dataframe_to_html(margin_common, float_cols=float_cols_margin)}</div>
    </section>

    <section class="section">
      <h2>样本区间说明</h2>
      <div class="table-wrap">{ranges_table}</div>
      <div class="note">
        全量样本反映各币各自的历史覆盖长度。公共区间从 `BNB` 数据起点开始，是五币都能同时参与组合回放的真实共同窗口。
      </div>
    </section>
  </div>
</body>
</html>"""


def build_monthly_pnl_bar(monthly_agg: pd.DataFrame, title: str):
    frame = monthly_agg[(monthly_agg["coin"] == "ALL") & (monthly_agg["side"] == "combined")].copy()
    colors = ["#15803d" if v >= 0 else "#b42318" for v in frame["total_pnl_u"]]
    fig, ax = plt.subplots(figsize=(11, 4.8))
    ax.bar(frame["period"], frame["total_pnl_u"], color=colors)
    ax.set_title(title, fontsize=14, pad=12)
    ax.set_ylabel("U")
    ax.tick_params(axis="x", rotation=65)
    ax.grid(axis="y", alpha=0.25)
    fig.tight_layout()
    return fig


def build_side_curve_chart(trades: pd.DataFrame, title: str):
    fig, ax = plt.subplots(figsize=(10, 4.8))
    for side, color, label in [
        ("long", "#1d4ed8", "Long"),
        ("short", "#b45309", "Short"),
        ("combined", "#0f766e", "Combined"),
    ]:
        frame = trades.sort_values("exit_ts").copy() if side == "combined" else trades[trades["side"] == side].sort_values("exit_ts").copy()
        if frame.empty:
            continue
        frame["time"] = pd.to_datetime(frame["exit_ts"], unit="ms", utc=True)
        frame["equity"] = float(INITIAL_CAPITAL) + frame["pnl_u"].astype(float).cumsum()
        ax.plot(frame["time"], frame["equity"], color=color, linewidth=1.4, label=label)
    ax.set_title(title, fontsize=14, pad=12)
    ax.set_ylabel("U")
    ax.legend()
    ax.grid(alpha=0.22)
    fig.tight_layout()
    return fig


def pick_summary(frame: pd.DataFrame, coin: str, side: str) -> pd.Series:
    return frame[(frame["coin"] == coin) & (frame["side"] == side)].iloc[0]


def first_true_leverage(frame: pd.DataFrame, column: str) -> str:
    hit = frame[frame[column] == True]
    if hit.empty:
        return "10x以上仍不足"
    return str(hit.iloc[0]["leverage"])


if __name__ == "__main__":
    main()
