from __future__ import annotations

import html
import json
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from okx_quant.candle_cache import load_candle_cache
from okx_quant.models import Candle
from okx_quant.okx_client import OkxRestClient
from okx_quant.persistence import analysis_report_dir_path
from scripts.run_btc_daily_ma_direction_filter_research import format_ts
from scripts.run_common_interval_bnb_refresh_report import run_bnb_bodyatr_short
from scripts.run_leadership_multi_coin_best_params_full_report import (
    INITIAL_CAPITAL,
    LONG_GATES,
    LONG_PROFILES,
    RISK_PER_TRADE_U,
    SYMBOLS,
    SYMBOL_LABELS,
    build_concurrent_chart,
    build_equity_chart,
    build_leverage_table,
    build_period_summary,
    build_scope_summary,
    concurrent_profile,
    dataframe_to_html,
    figure_to_base64,
    filter_scope,
    fmt2,
    run_long_trades,
    run_short_trades,
)
from scripts.run_multi_coin_short_recommendation_and_pullback_report import (
    load_recommendations,
    parse_strategy_key,
)


DAY_MS = 86_400_000
HOUR_MS = 3_600_000
REPORT_DIR = analysis_report_dir_path()
REPORT_DIR.mkdir(parents=True, exist_ok=True)
STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")

HTML_PATH = REPORT_DIR / f"leadership_daily_boundary_compare_report_{STAMP}.html"
JSON_PATH = REPORT_DIR / f"leadership_daily_boundary_compare_report_{STAMP}.json"
SUMMARY_CSV = REPORT_DIR / f"leadership_daily_boundary_compare_summary_{STAMP}.csv"
MONTHLY_CSV = REPORT_DIR / f"leadership_daily_boundary_compare_monthly_{STAMP}.csv"
YEARLY_CSV = REPORT_DIR / f"leadership_daily_boundary_compare_yearly_{STAMP}.csv"
MARGIN_CSV = REPORT_DIR / f"leadership_daily_boundary_compare_margin_{STAMP}.csv"
LOSS_MONTHS_CSV = REPORT_DIR / f"leadership_daily_boundary_compare_loss_months_{STAMP}.csv"
AUDIT_CSV = REPORT_DIR / f"leadership_daily_boundary_compare_audit_{STAMP}.csv"
PARAMS_CSV = REPORT_DIR / f"leadership_daily_boundary_compare_params_{STAMP}.csv"
TRADES_CSV = REPORT_DIR / f"leadership_daily_boundary_compare_trades_{STAMP}.csv"


@dataclass(frozen=True)
class DailyStandard:
    key: str
    label: str
    anchor_offset_ms: int
    close_note: str


@dataclass(frozen=True)
class ShortProfile:
    symbol: str
    coin: str
    strategy_key: str
    strategy_label: str
    ma_type: str
    period: int
    daily_filter_key: str
    daily_filter_label: str
    slope_threshold_ratio: Decimal
    atr_period: int
    atr_stop_multiplier: Decimal
    atr_percentile_max: Decimal
    exit_model: str


STANDARDS = (
    DailyStandard(
        key="bjt_00",
        label="北京时间0点日线",
        anchor_offset_ms=16 * HOUR_MS,
        close_note="日线区间为北京时间00:00到次日00:00；1H只能看到上一根已在北京时间00:00收盘的日线。",
    ),
    DailyStandard(
        key="bjt_08",
        label="北京时间8点日线",
        anchor_offset_ms=0,
        close_note="日线区间为北京时间08:00到次日08:00；1H只能看到上一根已在北京时间08:00收盘的日线。",
    ),
)


def main() -> None:
    client = OkxRestClient()
    short_profiles = load_short_profiles()
    entry_candles_by_symbol = {
        symbol: [c for c in load_candle_cache(symbol, "1H", limit=None) if c.confirmed]
        for symbol in SYMBOLS
    }
    instruments = {symbol: client.get_instrument(symbol) for symbol in SYMBOLS}
    common_start_ts = max(candles[0].ts for candles in entry_candles_by_symbol.values())
    common_end_ts = min(candles[-1].ts for candles in entry_candles_by_symbol.values())

    scenario_results: dict[str, dict[str, object]] = {}
    all_summaries: list[pd.DataFrame] = []
    all_monthly: list[pd.DataFrame] = []
    all_yearly: list[pd.DataFrame] = []
    all_margins: list[pd.DataFrame] = []
    all_loss_months: list[pd.DataFrame] = []
    all_audits: list[pd.DataFrame] = []
    all_trades: list[pd.DataFrame] = []

    for standard in STANDARDS:
        result = run_scenario(
            standard=standard,
            short_profiles=short_profiles,
            entry_candles_by_symbol=entry_candles_by_symbol,
            instruments=instruments,
            common_start_ts=common_start_ts,
            common_end_ts=common_end_ts,
        )
        scenario_results[standard.key] = result
        all_summaries.extend([result["summary_full"], result["summary_common"]])
        all_monthly.extend([result["monthly_agg_full"], result["monthly_agg_common"], result["monthly_coin_full"], result["monthly_coin_common"]])
        all_yearly.extend([result["yearly_agg_full"], result["yearly_agg_common"], result["yearly_coin_full"], result["yearly_coin_common"]])
        all_margins.extend([result["margin_full"], result["margin_common"]])
        all_loss_months.extend([result["loss_months_full"], result["loss_months_common"]])
        all_audits.append(result["audit_table"])
        all_trades.append(result["trades_full"])

    params_table = build_param_table(short_profiles)
    summary_compare = build_summary_compare_table(scenario_results)
    coin_compare = build_coin_compare_table(scenario_results)

    pd.concat(all_summaries, ignore_index=True).to_csv(SUMMARY_CSV, index=False, encoding="utf-8-sig")
    pd.concat(all_monthly, ignore_index=True).to_csv(MONTHLY_CSV, index=False, encoding="utf-8-sig")
    pd.concat(all_yearly, ignore_index=True).to_csv(YEARLY_CSV, index=False, encoding="utf-8-sig")
    pd.concat(all_margins, ignore_index=True).to_csv(MARGIN_CSV, index=False, encoding="utf-8-sig")
    pd.concat(all_loss_months, ignore_index=True).to_csv(LOSS_MONTHS_CSV, index=False, encoding="utf-8-sig")
    pd.concat(all_audits, ignore_index=True).to_csv(AUDIT_CSV, index=False, encoding="utf-8-sig")
    params_table.to_csv(PARAMS_CSV, index=False, encoding="utf-8-sig")
    pd.concat(all_trades, ignore_index=True).to_csv(TRADES_CSV, index=False, encoding="utf-8-sig")

    payload = build_payload(
        common_start_ts=common_start_ts,
        common_end_ts=common_end_ts,
        params_table=params_table,
        summary_compare=summary_compare,
        coin_compare=coin_compare,
        scenario_results=scenario_results,
    )
    JSON_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    HTML_PATH.write_text(
        build_html(
            common_start_ts=common_start_ts,
            common_end_ts=common_end_ts,
            params_table=params_table,
            summary_compare=summary_compare,
            coin_compare=coin_compare,
            scenario_results=scenario_results,
        ),
        encoding="utf-8",
    )
    print(HTML_PATH)


def load_short_profiles() -> dict[str, ShortProfile]:
    profiles: dict[str, ShortProfile] = {}
    for item in load_recommendations():
        ma_type, period = parse_strategy_key(item.strategy_key)
        profiles[item.symbol] = ShortProfile(
            symbol=item.symbol,
            coin=item.coin,
            strategy_key=item.strategy_key,
            strategy_label=item.strategy_label,
            ma_type=ma_type,
            period=period,
            daily_filter_key=item.daily_filter_key,
            daily_filter_label=item.daily_filter_label,
            slope_threshold_ratio=Decimal("-0.0005"),
            atr_period=14,
            atr_stop_multiplier=Decimal("2"),
            atr_percentile_max=Decimal("0.5"),
            exit_model="2R保本后逐级锁盈",
        )
    return profiles


def run_scenario(
    *,
    standard: DailyStandard,
    short_profiles: dict[str, ShortProfile],
    entry_candles_by_symbol: dict[str, list[Candle]],
    instruments: dict[str, object],
    common_start_ts: int,
    common_end_ts: int,
) -> dict[str, object]:
    trade_frames: list[pd.DataFrame] = []
    audit_rows: list[dict[str, object]] = []
    data_ranges: dict[str, dict[str, object]] = {}

    for symbol in SYMBOLS:
        entry_candles = entry_candles_by_symbol[symbol]
        daily_candles, daily_audit = aggregate_hourly_to_daily(entry_candles, anchor_offset_ms=standard.anchor_offset_ms)
        data_ranges[symbol] = {
            "coin": SYMBOL_LABELS[symbol],
            "entry_start_utc": format_ts(entry_candles[0].ts),
            "entry_end_utc": format_ts(entry_candles[-1].ts),
            "entry_candles": len(entry_candles),
            "daily_candles": len(daily_candles),
            "first_daily_open_utc": format_ts(daily_candles[0].ts),
            "last_daily_open_utc": format_ts(daily_candles[-1].ts),
        }
        audit_rows.append(
            build_symbol_audit_row(
                standard=standard,
                symbol=symbol,
                entry_candles=entry_candles,
                daily_audit=daily_audit,
            )
        )

        long_trades = run_long_trades(
            symbol=symbol,
            entry_candles=entry_candles,
            daily_candles=daily_candles,
            instrument=instruments[symbol],
            gate=LONG_GATES[symbol],
        )
        if symbol == "BNB-USDT-SWAP":
            short_trades = run_bnb_bodyatr_short(entry_candles=entry_candles, daily_candles=daily_candles)
            short_trades["param_label"] = (
                "MA20回抽做空 | 弱日=上一根已收盘日线收跌 | "
                "slope<=-0.0005 | ATR14 | body/ATR<=1.0 | breakdown=0.2ATR | "
                "retest=0.3ATR | stop+0.3ATR | 2R后逐级锁盈"
            )
        else:
            short_trades = run_short_trades(
                symbol=symbol,
                entry_candles=entry_candles,
                daily_candles=daily_candles,
                profile=short_profiles[symbol],
            )

        for frame in (long_trades, short_trades):
            if frame.empty:
                continue
            frame["scenario"] = standard.key
            frame["daily_standard"] = standard.label
        trade_frames.extend([long_trades, short_trades])

    trades_full = (
        pd.concat(trade_frames, ignore_index=True)
        .sort_values(["exit_ts", "entry_ts", "coin", "side"])
        .reset_index(drop=True)
    )
    trades_full = add_time_columns(trades_full)
    trades_common = add_time_columns(filter_scope(trades_full, start_ts=common_start_ts, end_ts=common_end_ts))

    summary_full = build_scope_summary(trades_full, scope_label="full")
    summary_common = build_scope_summary(trades_common, scope_label="common")
    monthly_agg_full = build_period_summary(trades_full, period_col="period_month", scope_label="full", by_coin=False)
    monthly_agg_common = build_period_summary(trades_common, period_col="period_month", scope_label="common", by_coin=False)
    monthly_coin_full = build_period_summary(trades_full, period_col="period_month", scope_label="full", by_coin=True)
    monthly_coin_common = build_period_summary(trades_common, period_col="period_month", scope_label="common", by_coin=True)
    yearly_agg_full = build_period_summary(trades_full, period_col="period_year", scope_label="full", by_coin=False)
    yearly_agg_common = build_period_summary(trades_common, period_col="period_year", scope_label="common", by_coin=False)
    yearly_coin_full = build_period_summary(trades_full, period_col="period_year", scope_label="full", by_coin=True)
    yearly_coin_common = build_period_summary(trades_common, period_col="period_year", scope_label="common", by_coin=True)

    for frame in (
        summary_full,
        summary_common,
        monthly_agg_full,
        monthly_agg_common,
        monthly_coin_full,
        monthly_coin_common,
        yearly_agg_full,
        yearly_agg_common,
        yearly_coin_full,
        yearly_coin_common,
    ):
        frame.insert(0, "scenario", standard.key)
        frame.insert(1, "daily_standard", standard.label)

    loss_months_full = build_negative_months(monthly_agg_full, standard=standard, scope="full")
    loss_months_common = build_negative_months(monthly_agg_common, standard=standard, scope="common")
    margin_full = build_margin_sufficiency_table(trades_full, standard=standard, scope="full")
    margin_common = build_margin_sufficiency_table(trades_common, standard=standard, scope="common")
    audit_table = pd.DataFrame(audit_rows)

    return {
        "standard": standard,
        "data_ranges": data_ranges,
        "audit_table": audit_table,
        "trades_full": trades_full,
        "trades_common": trades_common,
        "summary_full": summary_full,
        "summary_common": summary_common,
        "monthly_agg_full": monthly_agg_full,
        "monthly_agg_common": monthly_agg_common,
        "monthly_coin_full": monthly_coin_full,
        "monthly_coin_common": monthly_coin_common,
        "yearly_agg_full": yearly_agg_full,
        "yearly_agg_common": yearly_agg_common,
        "yearly_coin_full": yearly_coin_full,
        "yearly_coin_common": yearly_coin_common,
        "loss_months_full": loss_months_full,
        "loss_months_common": loss_months_common,
        "margin_full": margin_full,
        "margin_common": margin_common,
    }


def add_time_columns(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out["entry_time"] = pd.to_datetime(out["entry_ts"], unit="ms", utc=True)
    out["exit_time"] = pd.to_datetime(out["exit_ts"], unit="ms", utc=True)
    out["period_month"] = out["exit_time"].dt.strftime("%Y-%m")
    out["period_year"] = out["exit_time"].dt.strftime("%Y")
    return out


def aggregate_hourly_to_daily(
    candles: list[Candle],
    *,
    anchor_offset_ms: int,
) -> tuple[list[Candle], pd.DataFrame]:
    buckets: dict[int, list[Candle]] = {}
    for candle in candles:
        bucket_ts = ((int(candle.ts) - anchor_offset_ms) // DAY_MS) * DAY_MS + anchor_offset_ms
        buckets.setdefault(bucket_ts, []).append(candle)

    aggregated: list[Candle] = []
    audit_rows: list[dict[str, object]] = []
    for bucket_ts in sorted(buckets):
        group = sorted(buckets[bucket_ts], key=lambda item: int(item.ts))
        aggregated.append(
            Candle(
                ts=int(bucket_ts),
                open=group[0].open,
                high=max(item.high for item in group),
                low=min(item.low for item in group),
                close=group[-1].close,
                volume=sum((item.volume for item in group), Decimal("0")),
                confirmed=all(item.confirmed for item in group),
            )
        )
        audit_rows.append(
            {
                "bucket_open_ts": int(bucket_ts),
                "hours_in_bucket": len(group),
                "first_hour_ts": int(group[0].ts),
                "last_hour_ts": int(group[-1].ts),
                "is_full_24h": len(group) == 24,
            }
        )
    return aggregated, pd.DataFrame(audit_rows)


def build_symbol_audit_row(
    *,
    standard: DailyStandard,
    symbol: str,
    entry_candles: list[Candle],
    daily_audit: pd.DataFrame,
) -> dict[str, object]:
    ts_values = [int(c.ts) for c in entry_candles]
    gaps = [ts_values[idx] - ts_values[idx - 1] for idx in range(1, len(ts_values))]
    missing_hour_gaps = sum(max((gap // HOUR_MS) - 1, 0) for gap in gaps if gap > HOUR_MS)
    max_gap_hours = max((gap / HOUR_MS for gap in gaps), default=1.0)
    partial_count = int((daily_audit["hours_in_bucket"] != 24).sum()) if not daily_audit.empty else 0
    return {
        "scenario": standard.key,
        "daily_standard": standard.label,
        "coin": SYMBOL_LABELS[symbol],
        "daily_bars": int(len(daily_audit)),
        "full_24h_bars": int((daily_audit["hours_in_bucket"] == 24).sum()) if not daily_audit.empty else 0,
        "partial_daily_bars": partial_count,
        "min_hours_in_daily_bar": int(daily_audit["hours_in_bucket"].min()) if not daily_audit.empty else 0,
        "median_hours_in_daily_bar": float(daily_audit["hours_in_bucket"].median()) if not daily_audit.empty else 0.0,
        "max_hours_in_daily_bar": int(daily_audit["hours_in_bucket"].max()) if not daily_audit.empty else 0,
        "missing_hour_gaps": int(missing_hour_gaps),
        "max_gap_hours": float(max_gap_hours),
        "visibility_rule": "仅上一根已收盘日线可见",
        "same_day_close_leak": "否",
    }


def build_negative_months(monthly_agg: pd.DataFrame, *, standard: DailyStandard, scope: str) -> pd.DataFrame:
    out = monthly_agg[
        (monthly_agg["coin"] == "ALL")
        & (monthly_agg["side"] == "combined")
        & (monthly_agg["total_pnl_u"] < 0)
    ].copy()
    out["scope"] = scope
    out["daily_standard"] = standard.label
    out["scenario"] = standard.key
    return out.sort_values("period").reset_index(drop=True)


def build_margin_sufficiency_table(trades: pd.DataFrame, *, standard: DailyStandard, scope: str) -> pd.DataFrame:
    leverage_table = build_leverage_table(trades).copy()
    leverage_table.insert(0, "scope", scope)
    leverage_table.insert(0, "daily_standard", standard.label)
    leverage_table.insert(0, "scenario", standard.key)
    leverage_table["capital_u"] = float(INITIAL_CAPITAL)
    leverage_table["enough_by_hist_plus30"] = leverage_table["historical_max_margin_plus30pct_usdt"] <= float(INITIAL_CAPITAL)
    leverage_table["enough_by_conservative_plus30"] = leverage_table["conservative_upper_plus30pct_usdt"] <= float(INITIAL_CAPITAL)
    leverage_table["headroom_hist_plus30_u"] = float(INITIAL_CAPITAL) - leverage_table["historical_max_margin_plus30pct_usdt"]
    leverage_table["headroom_conservative_plus30_u"] = float(INITIAL_CAPITAL) - leverage_table["conservative_upper_plus30pct_usdt"]
    return leverage_table


def build_param_table(short_profiles: dict[str, ShortProfile]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for symbol in SYMBOLS:
        long_profile = LONG_PROFILES[symbol]
        long_gate = LONG_GATES[symbol]
        rows.append(
            {
                "coin": SYMBOL_LABELS[symbol],
                "side": "long",
                "entry_bar": "1H",
                "strategy_family": "dynamic_long",
                "fast_line": f"EMA{long_profile.ema_period}",
                "trend_line": f"EMA{long_profile.trend_ema_period}",
                "entry_reference": f"EMA{long_profile.entry_reference_ema_period}" if long_profile.entry_reference_ema_period > 0 else f"跟随EMA{long_profile.ema_period}",
                "atr_stop": f"{long_profile.atr_stop_multiplier} ATR",
                "take_profit_model": "动态止盈 + 2R保本 + 手续费偏移 + 每趋势最多1次",
                "daily_gate_rule": f"日线收盘 vs {long_gate.label}",
                "daily_boundary_visibility": "仅上一根已收盘日线",
                "special_notes": "每笔固定风险10U",
            }
        )

        if symbol == "BNB-USDT-SWAP":
            rows.append(
                {
                    "coin": "BNB",
                    "side": "short",
                    "entry_bar": "1H",
                    "strategy_family": "bodyatr_retest_short",
                    "fast_line": "MA20",
                    "trend_line": "MA20斜率<=-0.0005",
                    "entry_reference": "breakdown=0.2ATR, retest=0.3ATR",
                    "atr_stop": "stop+0.3ATR, ATR14",
                    "take_profit_model": "2R保本后逐级锁盈",
                    "daily_gate_rule": "弱日=上一根已收盘日线收跌",
                    "daily_boundary_visibility": "仅上一根已收盘日线",
                    "special_notes": "body/ATR<=1.0, ATR分位<=0.5, watch bars=6",
                }
            )
        else:
            profile = short_profiles[symbol]
            rows.append(
                {
                    "coin": SYMBOL_LABELS[symbol],
                    "side": "short",
                    "entry_bar": "1H",
                    "strategy_family": "slope_short",
                    "fast_line": f"{profile.ma_type.upper()}{profile.period}",
                    "trend_line": f"{profile.ma_type.upper()}{profile.period}斜率<=-0.0005",
                    "entry_reference": profile.strategy_label,
                    "atr_stop": f"ATR{profile.atr_period} x {profile.atr_stop_multiplier}",
                    "take_profit_model": profile.exit_model,
                    "daily_gate_rule": profile.daily_filter_label,
                    "daily_boundary_visibility": "仅上一根已收盘日线",
                    "special_notes": f"ATR分位<={profile.atr_percentile_max}, 每笔固定风险10U",
                }
            )
    return pd.DataFrame(rows)


def build_summary_compare_table(scenario_results: dict[str, dict[str, object]]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for standard in STANDARDS:
        result = scenario_results[standard.key]
        for scope, frame in (("full", result["summary_full"]), ("common", result["summary_common"])):
            for side in ("combined", "long", "short"):
                row = frame[(frame["coin"] == "ALL") & (frame["side"] == side)].iloc[0]
                rows.append(
                    {
                        "scenario": standard.key,
                        "daily_standard": standard.label,
                        "scope": scope,
                        "side": side,
                        "trades": int(row["trades"]),
                        "total_pnl_u": float(row["total_pnl_u"]),
                        "profit_factor": float(row["profit_factor"]),
                        "win_rate_pct": float(row["win_rate"]) * 100.0,
                        "max_drawdown_u": float(row["max_drawdown_u"]),
                        "final_equity_u": float(INITIAL_CAPITAL) + float(row["total_pnl_u"]),
                    }
                )
    compare = pd.DataFrame(rows)
    pivot = compare.pivot_table(
        index=["scope", "side"],
        columns="scenario",
        values=["total_pnl_u", "profit_factor", "win_rate_pct", "max_drawdown_u", "final_equity_u", "trades"],
        aggfunc="first",
    )
    pivot.columns = [f"{metric}_{scenario}" for metric, scenario in pivot.columns]
    out = pivot.reset_index()
    out["delta_pnl_bjt00_minus_bjt08"] = out["total_pnl_u_bjt_00"] - out["total_pnl_u_bjt_08"]
    out["delta_pf_bjt00_minus_bjt08"] = out["profit_factor_bjt_00"] - out["profit_factor_bjt_08"]
    out["delta_dd_bjt00_minus_bjt08"] = out["max_drawdown_u_bjt_00"] - out["max_drawdown_u_bjt_08"]
    return out


def build_coin_compare_table(scenario_results: dict[str, dict[str, object]]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for standard in STANDARDS:
        frame = scenario_results[standard.key]["summary_common"]
        subset = frame[(frame["side"] == "combined") & (frame["coin"] != "ALL")].copy()
        subset["scenario"] = standard.key
        subset["daily_standard"] = standard.label
        rows.append(subset[["scenario", "daily_standard", "coin", "trades", "total_pnl_u", "profit_factor", "win_rate", "max_drawdown_u"]])
    combined = pd.concat(rows, ignore_index=True)
    pivot = combined.pivot_table(
        index="coin",
        columns="scenario",
        values=["total_pnl_u", "profit_factor", "win_rate", "max_drawdown_u", "trades"],
        aggfunc="first",
    )
    pivot.columns = [f"{metric}_{scenario}" for metric, scenario in pivot.columns]
    out = pivot.reset_index()
    out["delta_pnl_bjt00_minus_bjt08"] = out["total_pnl_u_bjt_00"] - out["total_pnl_u_bjt_08"]
    return out


def build_payload(
    *,
    common_start_ts: int,
    common_end_ts: int,
    params_table: pd.DataFrame,
    summary_compare: pd.DataFrame,
    coin_compare: pd.DataFrame,
    scenario_results: dict[str, dict[str, object]],
) -> dict[str, object]:
    scenarios_payload: dict[str, object] = {}
    for standard in STANDARDS:
        result = scenario_results[standard.key]
        scenarios_payload[standard.key] = {
            "daily_standard": standard.label,
            "close_note": standard.close_note,
            "data_ranges": result["data_ranges"],
            "summary_full": result["summary_full"].to_dict("records"),
            "summary_common": result["summary_common"].to_dict("records"),
            "loss_months_full": result["loss_months_full"].to_dict("records"),
            "loss_months_common": result["loss_months_common"].to_dict("records"),
            "margin_full": result["margin_full"].to_dict("records"),
            "margin_common": result["margin_common"].to_dict("records"),
            "audit": result["audit_table"].to_dict("records"),
        }
    return {
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "risk_per_trade_u": str(RISK_PER_TRADE_U),
        "initial_capital_u": str(INITIAL_CAPITAL),
        "common_interval": {
            "start_utc": format_ts(common_start_ts),
            "end_utc": format_ts(common_end_ts),
        },
        "daily_visibility_rule": "仅使用当时已经收盘的上一根日线；不是读取当天尚未收盘日线的close。",
        "params": params_table.to_dict("records"),
        "summary_compare": summary_compare.to_dict("records"),
        "coin_compare_common": coin_compare.to_dict("records"),
        "scenarios": scenarios_payload,
    }


def build_html(
    *,
    common_start_ts: int,
    common_end_ts: int,
    params_table: pd.DataFrame,
    summary_compare: pd.DataFrame,
    coin_compare: pd.DataFrame,
    scenario_results: dict[str, dict[str, object]],
) -> str:
    scenario_sections = "".join(build_scenario_section(scenario_results[standard.key]) for standard in STANDARDS)
    hero_cards = []
    for standard in STANDARDS:
        result = scenario_results[standard.key]
        full_total = pick_summary(result["summary_full"], "ALL", "combined")
        common_total = pick_summary(result["summary_common"], "ALL", "combined")
        margin_common = result["margin_common"]
        conservative_3x = margin_common.loc[margin_common["leverage"] == "3x", "conservative_upper_plus30pct_usdt"].iloc[0]
        hero_cards.append(
            f"""
            <div class="card">
              <h3>{html.escape(standard.label)}</h3>
              <p>全量综合盈利 {fmt2(full_total["total_pnl_u"])}U</p>
              <p>公共区间综合盈利 {fmt2(common_total["total_pnl_u"])}U</p>
              <p>公共区间3x保守保证金+30%：{fmt2(conservative_3x)}U</p>
            </div>
            """
        )

    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <title>五币种双日线标准领导版审计报告</title>
  <style>
    :root {{
      --bg:#f3f7fb; --panel:#ffffff; --ink:#102033; --muted:#53657b; --line:#d6e1ec;
      --blue:#1859d1; --teal:#0f766e; --green:#166534; --red:#b42318; --amber:#b45309;
    }}
    * {{ box-sizing:border-box; }}
    body {{ margin:0; font-family:"Microsoft YaHei","Segoe UI",sans-serif; background:var(--bg); color:var(--ink); }}
    .wrap {{ max-width:1600px; margin:0 auto; padding:28px; }}
    .hero {{
      background:linear-gradient(135deg,#102033 0%,#173f67 55%,#0f766e 100%);
      color:#fff; border-radius:24px; padding:30px 34px; box-shadow:0 16px 40px rgba(15,23,42,.2);
    }}
    .hero h1 {{ margin:0 0 12px; font-size:34px; }}
    .hero p {{ margin:8px 0; line-height:1.75; color:rgba(255,255,255,.92); }}
    .meta {{ display:flex; flex-wrap:wrap; gap:10px; margin-top:18px; }}
    .chip {{ background:rgba(255,255,255,.12); border:1px solid rgba(255,255,255,.16); border-radius:999px; padding:8px 12px; font-size:13px; }}
    .grid {{ display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:16px; margin-top:22px; }}
    .card,.section {{ background:var(--panel); border:1px solid var(--line); border-radius:22px; box-shadow:0 10px 22px rgba(15,23,42,.05); }}
    .card {{ padding:18px; }}
    .card h3 {{ margin:0 0 10px; font-size:18px; }}
    .card p {{ margin:5px 0; color:var(--muted); }}
    .section {{ margin-top:22px; padding:24px; }}
    .section h2 {{ margin:0 0 14px; font-size:24px; }}
    .section h3 {{ margin:18px 0 10px; font-size:18px; }}
    .section p,.section li {{ color:var(--muted); line-height:1.8; }}
    .twocol {{ display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:18px; }}
    .chart {{ background:#fbfdff; border:1px solid var(--line); border-radius:18px; padding:16px; }}
    .chart img {{ width:100%; display:block; border-radius:12px; }}
    .table-wrap {{ max-height:560px; overflow:auto; border:1px solid var(--line); border-radius:16px; background:#fff; }}
    table {{ width:100%; border-collapse:collapse; font-size:13px; }}
    th, td {{ padding:9px 10px; border-bottom:1px solid var(--line); text-align:right; white-space:nowrap; }}
    th:first-child, td:first-child {{ text-align:left; }}
    thead th {{ background:#f8fbff; color:var(--muted); position:sticky; top:0; }}
    .note {{ margin-top:14px; padding:14px 16px; border-left:4px solid var(--blue); background:#eef4ff; border-radius:14px; color:#26405f; }}
    .warn {{ border-left-color:var(--amber); background:#fff7ed; color:#7c4a03; }}
    .risk {{ border-left-color:var(--red); background:#fef2f2; color:#7f1d1d; }}
    @media (max-width:1100px) {{ .grid,.twocol {{ grid-template-columns:1fr; }} }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <h1>五币种多空最佳参数 + 双日线标准审计报告</h1>
      <p>本报告按 <strong>每个币种每笔固定风险 10U</strong>、初始资金 <strong>10000U</strong>、五币种多空同时运行的口径，重跑了全量历史，并额外对比了两种日线边界：<strong>北京时间0点</strong> 与 <strong>北京时间8点</strong>。</p>
      <p>所有日线过滤都统一为 <strong>仅使用当时已经收盘的上一根日线</strong>。也就是说，1H 入场时不会读取当天尚未收盘的日线 close，避免未来数据。</p>
      <div class="meta">
        <div class="chip">公共区间：{format_ts(common_start_ts)} -> {format_ts(common_end_ts)}</div>
        <div class="chip">风险口径：每笔固定风险 {RISK_PER_TRADE_U}U</div>
        <div class="chip">初始资金：{INITIAL_CAPITAL}U</div>
        <div class="chip">报告文件：{html.escape(str(HTML_PATH))}</div>
      </div>
    </section>

    <section class="grid">
      {''.join(hero_cards)}
    </section>

    <section class="section">
      <h2>先看结论</h2>
      <ul>
        <li>日线过滤现在不是“昨天自然日收盘价”，而是“按该日线标准定义的上一根已收盘日线”。北京时间0点和北京时间8点会得到两套不同的日线序列。</li>
        <li>这次两套标准都从同一份本地 1H 确认K线重新合成日线，避免直接混用交易所原生 1D 导致边界不一致。</li>
        <li>报告同时给出全量样本和五币公共区间、年度/月度、五币分项、多空分项、综合资金曲线、亏损月份以及保证金充足性判断。</li>
      </ul>
      <div class="note">
        判断“10000U 是否够用”时，我用了两层口径：历史并发名义仓位 +30% 缓冲，以及更保守的“各币多空最大单笔名义仓位求和” +30% 缓冲。后者更适合领导快速审视资金安全边界。
      </div>
    </section>

    <section class="section">
      <h2>参数总表</h2>
      <div class="table-wrap">{dataframe_to_html(params_table)}</div>
      <div class="note">
        BNB 空头已替换为最新研究出的 Body/ATR 回抽做空参数。其“弱日”判断同样只使用上一根已收盘日线，因此不会提前看到当前正在形成的日线。
      </div>
    </section>

    <section class="section">
      <h2>双标准对比总表</h2>
      <div class="table-wrap">{dataframe_to_html(summary_compare, float_cols={"total_pnl_u_bjt_00": 2, "total_pnl_u_bjt_08": 2, "profit_factor_bjt_00": 3, "profit_factor_bjt_08": 3, "win_rate_pct_bjt_00": 2, "win_rate_pct_bjt_08": 2, "max_drawdown_u_bjt_00": 2, "max_drawdown_u_bjt_08": 2, "final_equity_u_bjt_00": 2, "final_equity_u_bjt_08": 2, "delta_pnl_bjt00_minus_bjt08": 2, "delta_pf_bjt00_minus_bjt08": 3, "delta_dd_bjt00_minus_bjt08": 2})}</div>
      <h3>公共区间分币种综合对比</h3>
      <div class="table-wrap">{dataframe_to_html(coin_compare, float_cols={"total_pnl_u_bjt_00": 2, "total_pnl_u_bjt_08": 2, "profit_factor_bjt_00": 3, "profit_factor_bjt_08": 3, "win_rate_bjt_00": 3, "win_rate_bjt_08": 3, "max_drawdown_u_bjt_00": 2, "max_drawdown_u_bjt_08": 2, "delta_pnl_bjt00_minus_bjt08": 2})}</div>
    </section>

    <section class="section">
      <h2>审计说明</h2>
      <ul>
        <li>日线过滤来源：全部由本地 1H 确认K线重采样合成，避免直接依赖不同边界的交易所原生 1D。</li>
        <li>可见性规则：某根日线只有在它完整走完 24 小时后，才允许被 1H 策略读取。</li>
        <li>这意味着不存在“当天日线还没收完，1H 已经先看到它的 close”这种未来数据。</li>
        <li>若样本开头或结尾处存在不足 24 根小时K的日线桶，它们会被记录在审计表中；这些桶不会导致未来数据，但会提醒你样本边缘存在不完整日线。</li>
        <li>仍需关注的剩余问题主要是：小时K若本身缺失，合成日线质量会受影响；报告已把缺口数量和日线桶完整度列出。</li>
      </ul>
      <div class="note warn">
        “昨天收盘”这个说法只在北京时间0点日线下接近直觉成立。更准确的表达始终应当是：<strong>上一根按当前日线标准已经收盘的日线</strong>。
      </div>
    </section>

    {scenario_sections}
  </div>
</body>
</html>"""


def build_scenario_section(result: dict[str, object]) -> str:
    standard: DailyStandard = result["standard"]
    summary_full = result["summary_full"]
    summary_common = result["summary_common"]
    full_total = pick_summary(summary_full, "ALL", "combined")
    full_long = pick_summary(summary_full, "ALL", "long")
    full_short = pick_summary(summary_full, "ALL", "short")
    common_total = pick_summary(summary_common, "ALL", "combined")
    common_long = pick_summary(summary_common, "ALL", "long")
    common_short = pick_summary(summary_common, "ALL", "short")
    audit_table = result["audit_table"]
    margin_full = result["margin_full"]
    margin_common = result["margin_common"]
    trades_full = result["trades_full"]
    trades_common = result["trades_common"]
    loss_months_full = result["loss_months_full"]
    loss_months_common = result["loss_months_common"]
    full_concurrent = concurrent_profile(trades_full)
    common_concurrent = concurrent_profile(trades_common)

    equity_full = figure_to_base64(build_equity_chart(trades_full, f"{standard.label} 全量资金曲线"))
    equity_common = figure_to_base64(build_equity_chart(trades_common, f"{standard.label} 公共区间资金曲线"))
    concurrent_full = figure_to_base64(build_concurrent_chart(full_concurrent, f"{standard.label} 全量并发名义"))
    concurrent_common = figure_to_base64(build_concurrent_chart(common_concurrent, f"{standard.label} 公共区间并发名义"))

    sufficiency_note_full = margin_note(margin_full)
    sufficiency_note_common = margin_note(margin_common)

    return f"""
    <section class="section">
      <h2>{html.escape(standard.label)}</h2>
      <p>{html.escape(standard.close_note)}</p>
      <div class="grid">
        <div class="card">
          <h3>全量综合</h3>
          <p>盈利 {fmt2(full_total["total_pnl_u"])}U</p>
          <p>Long {fmt2(full_long["total_pnl_u"])}U / Short {fmt2(full_short["total_pnl_u"])}U</p>
          <p>期末资金 {fmt2(float(INITIAL_CAPITAL) + float(full_total["total_pnl_u"]))}U</p>
        </div>
        <div class="card">
          <h3>公共区间综合</h3>
          <p>盈利 {fmt2(common_total["total_pnl_u"])}U</p>
          <p>Long {fmt2(common_long["total_pnl_u"])}U / Short {fmt2(common_short["total_pnl_u"])}U</p>
          <p>期末资金 {fmt2(float(INITIAL_CAPITAL) + float(common_total["total_pnl_u"]))}U</p>
        </div>
        <div class="card">
          <h3>10000U 保证金结论</h3>
          <p>全量：{html.escape(sufficiency_note_full)}</p>
          <p>公共区间：{html.escape(sufficiency_note_common)}</p>
        </div>
      </div>

      <div class="twocol">
        <div class="chart">
          <h3>全量资金曲线</h3>
          <img src="data:image/png;base64,{equity_full}" alt="{html.escape(standard.label)} 全量资金曲线" />
        </div>
        <div class="chart">
          <h3>公共区间资金曲线</h3>
          <img src="data:image/png;base64,{equity_common}" alt="{html.escape(standard.label)} 公共区间资金曲线" />
        </div>
      </div>
      <div class="twocol" style="margin-top:18px;">
        <div class="chart">
          <h3>全量并发名义</h3>
          <img src="data:image/png;base64,{concurrent_full}" alt="{html.escape(standard.label)} 全量并发名义" />
        </div>
        <div class="chart">
          <h3>公共区间并发名义</h3>
          <img src="data:image/png;base64,{concurrent_common}" alt="{html.escape(standard.label)} 公共区间并发名义" />
        </div>
      </div>

      <h3>全量汇总</h3>
      <div class="table-wrap">{dataframe_to_html(summary_full, float_cols={"total_pnl_u": 2, "profit_factor": 3, "win_rate": 3, "avg_r": 3, "avg_hold_hours": 1, "max_drawdown_u": 2, "return_pct_on_10k": 2})}</div>
      <h3>公共区间汇总</h3>
      <div class="table-wrap">{dataframe_to_html(summary_common, float_cols={"total_pnl_u": 2, "profit_factor": 3, "win_rate": 3, "avg_r": 3, "avg_hold_hours": 1, "max_drawdown_u": 2, "return_pct_on_10k": 2})}</div>

      <h3>全量年度汇总</h3>
      <div class="table-wrap">{dataframe_to_html(result["yearly_agg_full"], float_cols={"total_pnl_u": 2, "profit_factor": 3, "win_rate": 3, "avg_r": 3, "avg_hold_hours": 1, "max_drawdown_u": 2, "return_pct_on_10k": 2})}</div>
      <h3>公共区间年度汇总</h3>
      <div class="table-wrap">{dataframe_to_html(result["yearly_agg_common"], float_cols={"total_pnl_u": 2, "profit_factor": 3, "win_rate": 3, "avg_r": 3, "avg_hold_hours": 1, "max_drawdown_u": 2, "return_pct_on_10k": 2})}</div>
      <h3>全量年度分币种</h3>
      <div class="table-wrap">{dataframe_to_html(result["yearly_coin_full"], float_cols={"total_pnl_u": 2, "profit_factor": 3, "win_rate": 3, "avg_r": 3, "avg_hold_hours": 1, "max_drawdown_u": 2, "return_pct_on_10k": 2})}</div>
      <h3>公共区间年度分币种</h3>
      <div class="table-wrap">{dataframe_to_html(result["yearly_coin_common"], float_cols={"total_pnl_u": 2, "profit_factor": 3, "win_rate": 3, "avg_r": 3, "avg_hold_hours": 1, "max_drawdown_u": 2, "return_pct_on_10k": 2})}</div>

      <h3>全量月度汇总</h3>
      <div class="table-wrap">{dataframe_to_html(result["monthly_agg_full"], float_cols={"total_pnl_u": 2, "profit_factor": 3, "win_rate": 3, "avg_r": 3, "avg_hold_hours": 1, "max_drawdown_u": 2, "return_pct_on_10k": 2})}</div>
      <h3>公共区间月度汇总</h3>
      <div class="table-wrap">{dataframe_to_html(result["monthly_agg_common"], float_cols={"total_pnl_u": 2, "profit_factor": 3, "win_rate": 3, "avg_r": 3, "avg_hold_hours": 1, "max_drawdown_u": 2, "return_pct_on_10k": 2})}</div>
      <h3>全量月度分币种</h3>
      <div class="table-wrap">{dataframe_to_html(result["monthly_coin_full"], float_cols={"total_pnl_u": 2, "profit_factor": 3, "win_rate": 3, "avg_r": 3, "avg_hold_hours": 1, "max_drawdown_u": 2, "return_pct_on_10k": 2})}</div>
      <h3>公共区间月度分币种</h3>
      <div class="table-wrap">{dataframe_to_html(result["monthly_coin_common"], float_cols={"total_pnl_u": 2, "profit_factor": 3, "win_rate": 3, "avg_r": 3, "avg_hold_hours": 1, "max_drawdown_u": 2, "return_pct_on_10k": 2})}</div>

      <h3>综合亏损月份</h3>
      <div class="table-wrap">{dataframe_to_html(pd.concat([loss_months_full, loss_months_common], ignore_index=True), float_cols={"total_pnl_u": 2, "profit_factor": 3, "win_rate": 3, "avg_r": 3, "avg_hold_hours": 1, "max_drawdown_u": 2, "return_pct_on_10k": 2})}</div>

      <h3>保证金估算</h3>
      <div class="table-wrap">{dataframe_to_html(pd.concat([margin_full, margin_common], ignore_index=True), float_cols={"historical_max_margin_usdt": 2, "historical_max_margin_plus30pct_usdt": 2, "conservative_upper_margin_usdt": 2, "conservative_upper_plus30pct_usdt": 2, "capital_u": 2, "headroom_hist_plus30_u": 2, "headroom_conservative_plus30_u": 2})}</div>
      <div class="note risk">{html.escape(sufficiency_note_common)}</div>

      <h3>数据质量与未来函数审计</h3>
      <div class="table-wrap">{dataframe_to_html(audit_table, float_cols={"median_hours_in_daily_bar": 1, "max_gap_hours": 1})}</div>
    </section>
    """


def margin_note(margin_table: pd.DataFrame) -> str:
    common = margin_table.copy()
    good_rows = common[common["enough_by_conservative_plus30"]]
    if good_rows.empty:
        row = common.iloc[-1]
        return f"按更保守口径，连 {row['leverage']} 都不够，10000U 偏紧。"
    row = good_rows.iloc[0]
    return f"按更保守口径，{row['leverage']} 起 10000U 充足，剩余缓冲约 {fmt2(row['headroom_conservative_plus30_u'])}U。"


def pick_summary(frame: pd.DataFrame, coin: str, side: str) -> pd.Series:
    return frame[(frame["coin"] == coin) & (frame["side"] == side)].iloc[0]


if __name__ == "__main__":
    main()
