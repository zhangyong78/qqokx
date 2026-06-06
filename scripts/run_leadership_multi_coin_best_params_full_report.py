from __future__ import annotations

import base64
import html
import io
import json
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from okx_quant.backtest import BacktestTrade, _run_backtest_with_loaded_data
from okx_quant.candle_cache import load_candle_cache
from okx_quant.okx_client import OkxRestClient
from okx_quant.persistence import analysis_report_dir_path
from okx_quant.pricing import format_decimal_fixed
from scripts.run_btc_daily_ma_direction_filter_research import (
    LONG_MAKER_FEE_RATE,
    LONG_TAKER_FEE_RATE,
    SplitMetrics,
    build_daily_direction_bias,
    build_metrics,
    format_ts,
)
from scripts.run_multi_coin_best_long_daily_gate_report import LONG_PROFILES, SYMBOLS, SYMBOL_LABELS, build_long_config
from scripts.run_multi_coin_short_recommendation_and_pullback_report import load_recommendations, parse_strategy_key
from scripts.run_multi_coin_short_slope_daily_filter_10u import (
    build_daily_bias_map,
    build_entry_frame,
    add_entry_indicators,
    simulate_short_trades,
)


plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "Noto Sans CJK SC", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False


REPORT_DIR = analysis_report_dir_path()
REPORT_DIR.mkdir(parents=True, exist_ok=True)

STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
HTML_PATH = REPORT_DIR / f"leadership_multi_coin_best_params_full_report_{STAMP}.html"
JSON_PATH = REPORT_DIR / f"leadership_multi_coin_best_params_full_report_{STAMP}.json"
TRADES_CSV = REPORT_DIR / f"leadership_multi_coin_best_params_full_trades_{STAMP}.csv"
SUMMARY_FULL_CSV = REPORT_DIR / f"leadership_multi_coin_best_params_summary_full_{STAMP}.csv"
SUMMARY_COMMON_CSV = REPORT_DIR / f"leadership_multi_coin_best_params_summary_common_{STAMP}.csv"
YEARLY_AGG_FULL_CSV = REPORT_DIR / f"leadership_multi_coin_best_params_yearly_agg_full_{STAMP}.csv"
YEARLY_AGG_COMMON_CSV = REPORT_DIR / f"leadership_multi_coin_best_params_yearly_agg_common_{STAMP}.csv"
YEARLY_COIN_FULL_CSV = REPORT_DIR / f"leadership_multi_coin_best_params_yearly_coin_full_{STAMP}.csv"
YEARLY_COIN_COMMON_CSV = REPORT_DIR / f"leadership_multi_coin_best_params_yearly_coin_common_{STAMP}.csv"
MONTHLY_AGG_FULL_CSV = REPORT_DIR / f"leadership_multi_coin_best_params_monthly_agg_full_{STAMP}.csv"
MONTHLY_AGG_COMMON_CSV = REPORT_DIR / f"leadership_multi_coin_best_params_monthly_agg_common_{STAMP}.csv"
MONTHLY_COIN_FULL_CSV = REPORT_DIR / f"leadership_multi_coin_best_params_monthly_coin_full_{STAMP}.csv"
MONTHLY_COIN_COMMON_CSV = REPORT_DIR / f"leadership_multi_coin_best_params_monthly_coin_common_{STAMP}.csv"
MARGIN_FULL_CSV = REPORT_DIR / f"leadership_multi_coin_best_params_margin_full_{STAMP}.csv"
MARGIN_COMMON_CSV = REPORT_DIR / f"leadership_multi_coin_best_params_margin_common_{STAMP}.csv"

RISK_PER_TRADE_U = Decimal("10")
INITIAL_CAPITAL = Decimal("10000")


@dataclass(frozen=True)
class GateOption:
    key: str
    label: str
    ma_type: str | None = None
    period: int | None = None


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


LONG_GATES = {
    "BTC-USDT-SWAP": GateOption("ema_5", "EMA5", "ema", 5),
    "ETH-USDT-SWAP": GateOption("ma_5", "MA5", "ma", 5),
    "SOL-USDT-SWAP": GateOption("ema_5", "EMA5", "ema", 5),
    "BNB-USDT-SWAP": GateOption("ema_5", "EMA5", "ema", 5),
    "DOGE-USDT-SWAP": GateOption("ma_13", "MA13", "ma", 13),
}


def main() -> None:
    short_profiles = load_short_profiles()
    client = OkxRestClient()

    trade_frames: list[pd.DataFrame] = []
    data_ranges: dict[str, dict[str, object]] = {}
    for symbol in SYMBOLS:
        entry_candles = [candle for candle in load_candle_cache(symbol, "1H", limit=None) if candle.confirmed]
        daily_candles = [candle for candle in load_candle_cache(symbol, "1D", limit=None) if candle.confirmed]
        if not entry_candles or not daily_candles:
            raise RuntimeError(f"missing candles for {symbol}")
        instrument = client.get_instrument(symbol)

        data_ranges[symbol] = {
            "entry_candles": len(entry_candles),
            "daily_candles": len(daily_candles),
            "start_utc": format_ts(entry_candles[0].ts),
            "end_utc": format_ts(entry_candles[-1].ts),
        }

        long_trades = run_long_trades(
            symbol=symbol,
            entry_candles=entry_candles,
            daily_candles=daily_candles,
            instrument=instrument,
            gate=LONG_GATES[symbol],
        )
        short_trades = run_short_trades(
            symbol=symbol,
            entry_candles=entry_candles,
            daily_candles=daily_candles,
            profile=short_profiles[symbol],
        )
        trade_frames.extend([long_trades, short_trades])

    trades = pd.concat(trade_frames, ignore_index=True).sort_values(["exit_ts", "entry_ts", "coin", "side"]).reset_index(drop=True)
    trades["entry_time"] = pd.to_datetime(trades["entry_ts"], unit="ms", utc=True)
    trades["exit_time"] = pd.to_datetime(trades["exit_ts"], unit="ms", utc=True)
    trades["period_year"] = trades["exit_time"].dt.strftime("%Y")
    trades["period_month"] = trades["exit_time"].dt.strftime("%Y-%m")
    TRADES_CSV.write_text(trades.to_csv(index=False, encoding="utf-8-sig"), encoding="utf-8-sig")

    common_start_ts, common_end_ts = compute_common_interval(data_ranges)
    common_trades = filter_scope(trades, start_ts=common_start_ts, end_ts=common_end_ts)

    summary_full = build_scope_summary(trades, scope_label="full")
    summary_common = build_scope_summary(common_trades, scope_label="common")
    summary_full.to_csv(SUMMARY_FULL_CSV, index=False, encoding="utf-8-sig")
    summary_common.to_csv(SUMMARY_COMMON_CSV, index=False, encoding="utf-8-sig")

    yearly_agg_full = build_period_summary(trades, period_col="period_year", scope_label="full", by_coin=False)
    yearly_agg_common = build_period_summary(common_trades, period_col="period_year", scope_label="common", by_coin=False)
    yearly_coin_full = build_period_summary(trades, period_col="period_year", scope_label="full", by_coin=True)
    yearly_coin_common = build_period_summary(common_trades, period_col="period_year", scope_label="common", by_coin=True)
    monthly_agg_full = build_period_summary(trades, period_col="period_month", scope_label="full", by_coin=False)
    monthly_agg_common = build_period_summary(common_trades, period_col="period_month", scope_label="common", by_coin=False)
    monthly_coin_full = build_period_summary(trades, period_col="period_month", scope_label="full", by_coin=True)
    monthly_coin_common = build_period_summary(common_trades, period_col="period_month", scope_label="common", by_coin=True)

    yearly_agg_full.to_csv(YEARLY_AGG_FULL_CSV, index=False, encoding="utf-8-sig")
    yearly_agg_common.to_csv(YEARLY_AGG_COMMON_CSV, index=False, encoding="utf-8-sig")
    yearly_coin_full.to_csv(YEARLY_COIN_FULL_CSV, index=False, encoding="utf-8-sig")
    yearly_coin_common.to_csv(YEARLY_COIN_COMMON_CSV, index=False, encoding="utf-8-sig")
    monthly_agg_full.to_csv(MONTHLY_AGG_FULL_CSV, index=False, encoding="utf-8-sig")
    monthly_agg_common.to_csv(MONTHLY_AGG_COMMON_CSV, index=False, encoding="utf-8-sig")
    monthly_coin_full.to_csv(MONTHLY_COIN_FULL_CSV, index=False, encoding="utf-8-sig")
    monthly_coin_common.to_csv(MONTHLY_COIN_COMMON_CSV, index=False, encoding="utf-8-sig")

    margin_full = build_leverage_table(trades)
    margin_common = build_leverage_table(common_trades)
    margin_full.to_csv(MARGIN_FULL_CSV, index=False, encoding="utf-8-sig")
    margin_common.to_csv(MARGIN_COMMON_CSV, index=False, encoding="utf-8-sig")

    payload = build_payload(
        data_ranges=data_ranges,
        common_start_ts=common_start_ts,
        common_end_ts=common_end_ts,
        summary_full=summary_full,
        summary_common=summary_common,
        short_profiles=short_profiles,
    )
    JSON_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    HTML_PATH.write_text(
        build_html(
            trades=trades,
            common_trades=common_trades,
            data_ranges=data_ranges,
            common_start_ts=common_start_ts,
            common_end_ts=common_end_ts,
            summary_full=summary_full,
            summary_common=summary_common,
            yearly_agg_full=yearly_agg_full,
            yearly_agg_common=yearly_agg_common,
            yearly_coin_full=yearly_coin_full,
            yearly_coin_common=yearly_coin_common,
            monthly_agg_full=monthly_agg_full,
            monthly_agg_common=monthly_agg_common,
            monthly_coin_full=monthly_coin_full,
            monthly_coin_common=monthly_coin_common,
            margin_full=margin_full,
            margin_common=margin_common,
            short_profiles=short_profiles,
        ),
        encoding="utf-8",
    )
    print(HTML_PATH)


def load_short_profiles() -> dict[str, ShortProfile]:
    recommendations = load_recommendations()
    profiles: dict[str, ShortProfile] = {}
    for item in recommendations:
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


def run_long_trades(
    *,
    symbol: str,
    entry_candles,
    daily_candles,
    instrument,
    gate: GateOption,
) -> pd.DataFrame:
    bias = build_daily_direction_bias(entry_candles, daily_candles, gate)
    result = _run_backtest_with_loaded_data(
        entry_candles,
        instrument,
        build_long_config(symbol),
        data_source_note="leadership full sample long",
        maker_fee_rate=LONG_MAKER_FEE_RATE,
        taker_fee_rate=LONG_TAKER_FEE_RATE,
        direction_filter_bias=bias,
    )
    rows = [long_trade_to_row(symbol, gate, trade) for trade in result.trades]
    return pd.DataFrame(rows)


def long_trade_to_row(symbol: str, gate: GateOption, trade: BacktestTrade) -> dict[str, object]:
    return {
        "symbol": symbol,
        "coin": SYMBOL_LABELS[symbol],
        "side": "long",
        "strategy_family": "dynamic_long",
        "strategy_key": f"long_{gate.key}",
        "strategy_label": f"做多 | {gate.label}",
        "param_label": (
            f"{LONG_PROFILES[symbol].profile_label} | 日线{gate.label}过滤"
        ),
        "entry_ts": int(trade.entry_ts),
        "exit_ts": int(trade.exit_ts),
        "entry_index": int(trade.entry_index),
        "exit_index": int(trade.exit_index),
        "entry_price": float(trade.entry_price),
        "exit_price": float(trade.exit_price),
        "qty": float(abs(trade.size)),
        "notional_usdt": float(abs(trade.size * trade.entry_price)),
        "risk_value_u": float(trade.risk_value),
        "pnl_u": float(trade.pnl),
        "r_multiple": float(trade.r_multiple),
        "hold_hours": float((trade.exit_ts - trade.entry_ts) / (1000 * 3600)),
        "exit_reason": str(trade.exit_reason),
        "daily_gate_key": gate.key,
        "daily_gate_label": gate.label,
    }


def run_short_trades(
    *,
    symbol: str,
    entry_candles,
    daily_candles,
    profile: ShortProfile,
) -> pd.DataFrame:
    frame = build_entry_frame(entry_candles)
    add_entry_indicators(frame)
    bias_map = build_daily_bias_map(entry_candles, daily_candles)
    bias = bias_map[profile.daily_filter_key]
    trades = simulate_short_trades(frame, bias=bias, ma_column=f"{profile.ma_type}{profile.period}").copy()
    if trades.empty:
        return pd.DataFrame(columns=[
            "symbol", "coin", "side", "strategy_family", "strategy_key", "strategy_label", "param_label",
            "entry_ts", "exit_ts", "entry_index", "exit_index", "entry_price", "exit_price", "qty",
            "notional_usdt", "risk_value_u", "pnl_u", "r_multiple", "hold_hours", "exit_reason",
            "daily_gate_key", "daily_gate_label",
        ])
    trades["symbol"] = symbol
    trades["coin"] = profile.coin
    trades["side"] = "short"
    trades["strategy_family"] = "slope_short"
    trades["strategy_key"] = profile.strategy_key
    trades["strategy_label"] = f"做空 | {profile.strategy_label}"
    trades["param_label"] = (
        f"{profile.strategy_label} | {profile.daily_filter_label} | slope<=-0.0005 | ATR14x2 | 2R后逐级锁盈"
    )
    trades["qty"] = trades["risk_per_unit"].astype(float).rdiv(float(RISK_PER_TRADE_U))
    trades["notional_usdt"] = trades["qty"] * trades["entry_price"].astype(float)
    trades["risk_value_u"] = float(RISK_PER_TRADE_U)
    trades["daily_gate_key"] = profile.daily_filter_key
    trades["daily_gate_label"] = profile.daily_filter_label
    trades["exit_reason"] = trades["exit_reason"].astype(str)
    return trades[
        [
            "symbol", "coin", "side", "strategy_family", "strategy_key", "strategy_label", "param_label",
            "entry_ts", "exit_ts", "entry_index", "exit_index", "entry_price", "exit_price", "qty",
            "notional_usdt", "risk_value_u", "pnl_u", "r_multiple", "hold_hours", "exit_reason",
            "daily_gate_key", "daily_gate_label",
        ]
    ].copy()


def compute_common_interval(data_ranges: dict[str, dict[str, object]]) -> tuple[int, int]:
    starts: list[int] = []
    ends: list[int] = []
    for symbol in SYMBOLS:
        entry_candles = [candle for candle in load_candle_cache(symbol, "1H", limit=None) if candle.confirmed]
        starts.append(entry_candles[0].ts)
        ends.append(entry_candles[-1].ts)
    return max(starts), min(ends)


def filter_scope(trades: pd.DataFrame, *, start_ts: int | None = None, end_ts: int | None = None) -> pd.DataFrame:
    out = trades.copy()
    if start_ts is not None:
        out = out[out["entry_ts"] >= start_ts]
    if end_ts is not None:
        out = out[out["exit_ts"] <= end_ts]
    return out.sort_values(["exit_ts", "entry_ts", "coin", "side"]).reset_index(drop=True)


def metrics_from_frame(trades: pd.DataFrame) -> dict[str, float]:
    if trades.empty:
        return {
            "trades": 0.0,
            "total_pnl_u": 0.0,
            "win_rate": 0.0,
            "profit_factor": 0.0,
            "avg_r": 0.0,
            "avg_hold_hours": 0.0,
            "max_drawdown_u": 0.0,
            "return_pct_on_10k": 0.0,
        }
    pnls = trades["pnl_u"].astype(float)
    rs = trades["r_multiple"].astype(float)
    curve = pnls.cumsum()
    gross_profit = float(pnls[pnls > 0].sum())
    gross_loss = abs(float(pnls[pnls < 0].sum()))
    return {
        "trades": float(len(trades)),
        "total_pnl_u": float(pnls.sum()),
        "win_rate": float((pnls > 0).mean()),
        "profit_factor": gross_profit / gross_loss if gross_loss > 0 else 0.0,
        "avg_r": float(rs.mean()) if not rs.empty else 0.0,
        "avg_hold_hours": float(trades["hold_hours"].astype(float).mean()) if len(trades) else 0.0,
        "max_drawdown_u": float((curve.cummax() - curve).max()) if len(curve) else 0.0,
        "return_pct_on_10k": float(pnls.sum() / float(INITIAL_CAPITAL) * 100),
    }


def build_scope_summary(trades: pd.DataFrame, *, scope_label: str) -> pd.DataFrame:
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
                    "win_rate": metrics["win_rate"],
                    "avg_r": metrics["avg_r"],
                    "avg_hold_hours": metrics["avg_hold_hours"],
                    "max_drawdown_u": metrics["max_drawdown_u"],
                    "return_pct_on_10k": metrics["return_pct_on_10k"],
                }
            )
    return pd.DataFrame(rows)


def build_period_summary(trades: pd.DataFrame, *, period_col: str, scope_label: str, by_coin: bool) -> pd.DataFrame:
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
                        "win_rate": metrics["win_rate"],
                        "avg_r": metrics["avg_r"],
                        "avg_hold_hours": metrics["avg_hold_hours"],
                        "max_drawdown_u": metrics["max_drawdown_u"],
                        "return_pct_on_10k": metrics["return_pct_on_10k"],
                    }
                )
    return pd.DataFrame(rows)


def concurrent_profile(trades: pd.DataFrame) -> pd.DataFrame:
    events: list[dict[str, object]] = []
    for row in trades.to_dict("records"):
        events.append({"ts": int(row["entry_ts"]), "delta": float(row["notional_usdt"]), "delta_count": 1})
        events.append({"ts": int(row["exit_ts"]), "delta": -float(row["notional_usdt"]), "delta_count": -1})
    frame = pd.DataFrame(events).sort_values(["ts", "delta_count"]).reset_index(drop=True)
    frame["total_notional_usdt"] = frame["delta"].cumsum()
    frame["open_positions"] = frame["delta_count"].cumsum()
    return frame


def build_leverage_table(trades: pd.DataFrame) -> pd.DataFrame:
    concurrent = concurrent_profile(trades)
    max_concurrent_notional = float(concurrent["total_notional_usdt"].max()) if not concurrent.empty else 0.0
    sum_of_max_single = float(trades.groupby(["coin", "side"])["notional_usdt"].max().sum()) if not trades.empty else 0.0
    rows: list[dict[str, object]] = []
    for lev in (1, 2, 3, 5, 10):
        rows.append(
            {
                "leverage": f"{lev}x",
                "historical_max_margin_usdt": max_concurrent_notional / lev,
                "historical_max_margin_plus30pct_usdt": max_concurrent_notional / lev * 1.3,
                "conservative_upper_margin_usdt": sum_of_max_single / lev,
                "conservative_upper_plus30pct_usdt": sum_of_max_single / lev * 1.3,
            }
        )
    return pd.DataFrame(rows)


def build_payload(
    *,
    data_ranges: dict[str, dict[str, object]],
    common_start_ts: int,
    common_end_ts: int,
    summary_full: pd.DataFrame,
    summary_common: pd.DataFrame,
    short_profiles: dict[str, ShortProfile],
) -> dict[str, object]:
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "initial_capital_u": str(INITIAL_CAPITAL),
        "risk_per_trade_u": str(RISK_PER_TRADE_U),
        "data_ranges": data_ranges,
        "common_interval": {
            "start_utc": format_ts(common_start_ts),
            "end_utc": format_ts(common_end_ts),
        },
        "long_parameters": {
            symbol: {
                "profile": LONG_PROFILES[symbol].profile_label,
                "daily_gate": LONG_GATES[symbol].label,
            }
            for symbol in SYMBOLS
        },
        "short_parameters": {
            symbol: {
                "strategy_label": profile.strategy_label,
                "daily_filter_label": profile.daily_filter_label,
                "slope_threshold_ratio": str(profile.slope_threshold_ratio),
                "atr_period": profile.atr_period,
                "atr_stop_multiplier": str(profile.atr_stop_multiplier),
                "atr_percentile_max": str(profile.atr_percentile_max),
                "exit_model": profile.exit_model,
            }
            for symbol, profile in short_profiles.items()
        },
        "summary_full": summary_full.to_dict("records"),
        "summary_common": summary_common.to_dict("records"),
    }


def build_html(
    *,
    trades: pd.DataFrame,
    common_trades: pd.DataFrame,
    data_ranges: dict[str, dict[str, object]],
    common_start_ts: int,
    common_end_ts: int,
    summary_full: pd.DataFrame,
    summary_common: pd.DataFrame,
    yearly_agg_full: pd.DataFrame,
    yearly_agg_common: pd.DataFrame,
    yearly_coin_full: pd.DataFrame,
    yearly_coin_common: pd.DataFrame,
    monthly_agg_full: pd.DataFrame,
    monthly_agg_common: pd.DataFrame,
    monthly_coin_full: pd.DataFrame,
    monthly_coin_common: pd.DataFrame,
    margin_full: pd.DataFrame,
    margin_common: pd.DataFrame,
    short_profiles: dict[str, ShortProfile],
) -> str:
    full_total = pick_summary(summary_full, "ALL", "combined")
    full_long = pick_summary(summary_full, "ALL", "long")
    full_short = pick_summary(summary_full, "ALL", "short")
    common_total = pick_summary(summary_common, "ALL", "combined")
    common_long = pick_summary(summary_common, "ALL", "long")
    common_short = pick_summary(summary_common, "ALL", "short")
    final_equity_full = float(INITIAL_CAPITAL) + float(full_total["total_pnl_u"])
    final_equity_common = float(INITIAL_CAPITAL) + float(common_total["total_pnl_u"])

    full_concurrent = concurrent_profile(trades)
    common_concurrent = concurrent_profile(common_trades)
    full_max_notional = float(full_concurrent["total_notional_usdt"].max()) if not full_concurrent.empty else 0.0
    common_max_notional = float(common_concurrent["total_notional_usdt"].max()) if not common_concurrent.empty else 0.0
    full_max_positions = int(full_concurrent["open_positions"].max()) if not full_concurrent.empty else 0
    common_max_positions = int(common_concurrent["open_positions"].max()) if not common_concurrent.empty else 0

    equity_full = figure_to_base64(build_equity_chart(trades, "全量综合资金曲线"))
    equity_common = figure_to_base64(build_equity_chart(common_trades, "公共区间综合资金曲线"))
    margin_chart_full = figure_to_base64(build_concurrent_chart(full_concurrent, "全量同时持仓名义"))
    margin_chart_common = figure_to_base64(build_concurrent_chart(common_concurrent, "公共区间同时持仓名义"))

    param_rows = build_param_rows(short_profiles)
    range_rows = build_range_rows(data_ranges)
    full_coin_table = dataframe_to_html(
        scope_coin_view(summary_full),
        float_cols={"total_pnl_u": 2, "profit_factor": 3, "win_rate": 3, "avg_r": 3, "avg_hold_hours": 1, "max_drawdown_u": 2, "return_pct_on_10k": 2},
    )
    common_coin_table = dataframe_to_html(
        scope_coin_view(summary_common),
        float_cols={"total_pnl_u": 2, "profit_factor": 3, "win_rate": 3, "avg_r": 3, "avg_hold_hours": 1, "max_drawdown_u": 2, "return_pct_on_10k": 2},
    )

    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <title>五币种最佳参数全量领导版报告</title>
  <style>
    :root {{
      --bg:#f4f7fb; --panel:#ffffff; --ink:#132033; --muted:#5b6b82; --line:#d7e0ea;
      --blue:#1d4ed8; --teal:#0f766e; --green:#166534; --red:#b42318; --amber:#b45309;
    }}
    * {{ box-sizing:border-box; }}
    body {{ margin:0; font-family:"Microsoft YaHei","Segoe UI",sans-serif; background:var(--bg); color:var(--ink); }}
    .wrap {{ max-width:1580px; margin:0 auto; padding:28px; }}
    .hero {{
      background:linear-gradient(135deg,#102033 0%,#173f67 54%,#0f766e 100%);
      color:#fff; border-radius:24px; padding:30px 34px; box-shadow:0 18px 40px rgba(15,23,42,.22);
    }}
    .hero h1 {{ margin:0 0 12px; font-size:34px; }}
    .hero p {{ margin:8px 0; line-height:1.75; color:rgba(255,255,255,.93); }}
    .meta {{ display:flex; flex-wrap:wrap; gap:12px; margin-top:18px; }}
    .chip {{
      background:rgba(255,255,255,.12); border:1px solid rgba(255,255,255,.18); border-radius:999px; padding:8px 12px; font-size:13px;
    }}
    .grid {{ display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:16px; margin:22px 0 8px; }}
    .card, .section {{
      background:var(--panel); border:1px solid var(--line); border-radius:22px; box-shadow:0 10px 22px rgba(15,23,42,.05);
    }}
    .card {{ padding:18px; }}
    .card h3 {{ margin:0 0 8px; font-size:16px; }}
    .card p {{ margin:0; color:var(--muted); line-height:1.7; }}
    .section {{ margin-top:22px; padding:24px; }}
    .section h2 {{ margin:0 0 14px; font-size:24px; }}
    .section p, .section li {{ color:var(--muted); line-height:1.8; }}
    .twocol {{ display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:18px; }}
    .chart {{ background:#fbfdff; border:1px solid var(--line); border-radius:18px; padding:16px; }}
    .chart img {{ width:100%; display:block; border-radius:12px; }}
    .table-wrap {{ max-height:560px; overflow:auto; border:1px solid var(--line); border-radius:16px; background:#fff; }}
    table {{ width:100%; border-collapse:collapse; font-size:13px; }}
    th, td {{ padding:9px 10px; border-bottom:1px solid var(--line); text-align:right; white-space:nowrap; }}
    th:first-child, td:first-child {{ text-align:left; }}
    thead th {{ color:var(--muted); font-weight:700; background:#f8fbff; position:sticky; top:0; }}
    .note {{ margin-top:14px; padding:14px 16px; border-left:4px solid var(--blue); background:#eef4ff; border-radius:14px; color:#274064; }}
    @media (max-width:1100px) {{ .grid, .twocol {{ grid-template-columns:1fr; }} }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <h1>五币种最佳参数全量领导版报告</h1>
      <p>这份报告把五个币种的做多和做空都切换到当前研究阶段的最佳参数，再用 <strong>每笔固定风险 10U</strong> 的统一口径做全量回放。做多侧使用各币种的最佳动态委托参数与对应日线闸门；做空侧全部替换成最新的 <strong>斜率做空最佳参数</strong> 与对应日线过滤。</p>
      <p>报告同时给出 <strong>全量样本</strong> 和 <strong>五币公共区间</strong> 两套口径，并拆出年度、月度、分币种、多空分侧、综合合并、以及简化保证金估算，方便直接给管理层过目。</p>
      <div class="meta">
        <div class="chip">风险口径：每笔固定风险 10U</div>
        <div class="chip">初始资金：10000U（只作收益展示）</div>
        <div class="chip">全量样本：各币使用本地全部确认K线</div>
        <div class="chip">公共区间：{format_ts(common_start_ts)} -> {format_ts(common_end_ts)}</div>
        <div class="chip">输出文件：{html.escape(str(HTML_PATH))}</div>
      </div>
    </section>

    <section class="grid">
      <div class="card"><h3>全量综合盈利</h3><p>{fmt2(full_total["total_pnl_u"])}U<br>期末约 {fmt2(final_equity_full)}U</p></div>
      <div class="card"><h3>全量多头 / 空头</h3><p>Long {fmt2(full_long["total_pnl_u"])}U<br>Short {fmt2(full_short["total_pnl_u"])}U</p></div>
      <div class="card"><h3>公共区间综合盈利</h3><p>{fmt2(common_total["total_pnl_u"])}U<br>期末约 {fmt2(final_equity_common)}U</p></div>
      <div class="card"><h3>保证金粗估</h3><p>全量峰值名义 {fmt2(full_max_notional)}U<br>公共区间峰值名义 {fmt2(common_max_notional)}U</p></div>
    </section>

    <section class="section">
      <h2>参数总表</h2>
      <div class="table-wrap">{param_rows}</div>
      <div class="note">
        做多侧保留了此前五币种的最佳参数与对应最佳日线门；做空侧全部替换成最新的斜率做空推荐表。这里的“最佳”来源于当前本地研究表，而不是这份全量回放后再重新反向优化。
      </div>
    </section>

    <section class="section">
      <h2>数据区间</h2>
      <div class="table-wrap">{range_rows}</div>
      <div class="note">
        公共区间定义为五个币都有数据覆盖的共同时间段。它更适合横向比较 5 个币同时启用时的真实组合表现；全量区间则更适合看每个币种历史能力上限。
      </div>
    </section>

    <section class="section">
      <h2>全量 vs 公共区间 总览</h2>
      <div class="twocol">
        <div class="chart"><img src="data:image/png;base64,{equity_full}" alt="全量综合资金曲线" /></div>
        <div class="chart"><img src="data:image/png;base64,{equity_common}" alt="公共区间综合资金曲线" /></div>
      </div>
      <div class="twocol" style="margin-top:18px;">
        <div class="chart"><img src="data:image/png;base64,{margin_chart_full}" alt="全量同时持仓名义" /></div>
        <div class="chart"><img src="data:image/png;base64,{margin_chart_common}" alt="公共区间同时持仓名义" /></div>
      </div>
    </section>

    <section class="section">
      <h2>全量分币种总览</h2>
      <div class="table-wrap">{full_coin_table}</div>
    </section>

    <section class="section">
      <h2>公共区间分币种总览</h2>
      <div class="table-wrap">{common_coin_table}</div>
    </section>

    <section class="section">
      <h2>年度聚合表</h2>
      <h3>全量</h3>
      <div class="table-wrap">{dataframe_to_html(yearly_agg_full, float_cols={"total_pnl_u": 2, "profit_factor": 3, "win_rate": 3, "avg_r": 3, "avg_hold_hours": 1, "max_drawdown_u": 2, "return_pct_on_10k": 2})}</div>
      <h3 style="margin-top:18px;">公共区间</h3>
      <div class="table-wrap">{dataframe_to_html(yearly_agg_common, float_cols={"total_pnl_u": 2, "profit_factor": 3, "win_rate": 3, "avg_r": 3, "avg_hold_hours": 1, "max_drawdown_u": 2, "return_pct_on_10k": 2})}</div>
    </section>

    <section class="section">
      <h2>年度分币种表</h2>
      <h3>全量</h3>
      <div class="table-wrap">{dataframe_to_html(yearly_coin_full, float_cols={"total_pnl_u": 2, "profit_factor": 3, "win_rate": 3, "avg_r": 3, "avg_hold_hours": 1, "max_drawdown_u": 2, "return_pct_on_10k": 2})}</div>
      <h3 style="margin-top:18px;">公共区间</h3>
      <div class="table-wrap">{dataframe_to_html(yearly_coin_common, float_cols={"total_pnl_u": 2, "profit_factor": 3, "win_rate": 3, "avg_r": 3, "avg_hold_hours": 1, "max_drawdown_u": 2, "return_pct_on_10k": 2})}</div>
    </section>

    <section class="section">
      <h2>月度聚合表</h2>
      <h3>全量</h3>
      <div class="table-wrap">{dataframe_to_html(monthly_agg_full, float_cols={"total_pnl_u": 2, "profit_factor": 3, "win_rate": 3, "avg_r": 3, "avg_hold_hours": 1, "max_drawdown_u": 2, "return_pct_on_10k": 2})}</div>
      <h3 style="margin-top:18px;">公共区间</h3>
      <div class="table-wrap">{dataframe_to_html(monthly_agg_common, float_cols={"total_pnl_u": 2, "profit_factor": 3, "win_rate": 3, "avg_r": 3, "avg_hold_hours": 1, "max_drawdown_u": 2, "return_pct_on_10k": 2})}</div>
    </section>

    <section class="section">
      <h2>月度分币种表</h2>
      <h3>全量</h3>
      <div class="table-wrap">{dataframe_to_html(monthly_coin_full, float_cols={"total_pnl_u": 2, "profit_factor": 3, "win_rate": 3, "avg_r": 3, "avg_hold_hours": 1, "max_drawdown_u": 2, "return_pct_on_10k": 2})}</div>
      <h3 style="margin-top:18px;">公共区间</h3>
      <div class="table-wrap">{dataframe_to_html(monthly_coin_common, float_cols={"total_pnl_u": 2, "profit_factor": 3, "win_rate": 3, "avg_r": 3, "avg_hold_hours": 1, "max_drawdown_u": 2, "return_pct_on_10k": 2})}</div>
    </section>

    <section class="section">
      <h2>保证金估算</h2>
      <p>这里用的是简单名义仓位法：按每笔交易的入场名义金额估算，统计历史上同一时刻所有持仓名义的叠加峰值，再折算成 1x / 2x / 3x / 5x / 10x 杠杆下大概需要准备的保证金。</p>
      <ul>
        <li>全量最大同时持仓名义：{fmt2(full_max_notional)}U，最大同时持仓数：{full_max_positions}</li>
        <li>公共区间最大同时持仓名义：{fmt2(common_max_notional)}U，最大同时持仓数：{common_max_positions}</li>
        <li>这只是粗估，不包含滑点跳空、资金费恶化、交易所临时调高保证金等极端情况。</li>
      </ul>
      <h3>全量杠杆表</h3>
      <div class="table-wrap">{dataframe_to_html(margin_full, float_cols={"historical_max_margin_usdt": 2, "historical_max_margin_plus30pct_usdt": 2, "conservative_upper_margin_usdt": 2, "conservative_upper_plus30pct_usdt": 2})}</div>
      <h3 style="margin-top:18px;">公共区间杠杆表</h3>
      <div class="table-wrap">{dataframe_to_html(margin_common, float_cols={"historical_max_margin_usdt": 2, "historical_max_margin_plus30pct_usdt": 2, "conservative_upper_margin_usdt": 2, "conservative_upper_plus30pct_usdt": 2})}</div>
    </section>
  </div>
</body>
</html>"""


def build_param_rows(short_profiles: dict[str, ShortProfile]) -> str:
    rows = []
    for symbol in SYMBOLS:
        long_profile = LONG_PROFILES[symbol]
        long_gate = LONG_GATES[symbol]
        short_profile = short_profiles[symbol]
        rows.append(
            "<tr>"
            f"<td>{html.escape(SYMBOL_LABELS[symbol])}</td>"
            f"<td>{html.escape(long_profile.profile_label)}</td>"
            f"<td>{html.escape(long_gate.label)}</td>"
            f"<td>{html.escape(short_profile.strategy_label)}</td>"
            f"<td>{html.escape(short_profile.daily_filter_label)}</td>"
            f"<td>{short_profile.slope_threshold_ratio}</td>"
            f"<td>ATR{short_profile.atr_period} x {short_profile.atr_stop_multiplier}</td>"
            f"<td>{short_profile.atr_percentile_max}</td>"
            f"<td>{html.escape(short_profile.exit_model)}</td>"
            "</tr>"
        )
    return (
        "<table><thead><tr>"
        "<th>币种</th><th>做多参数</th><th>做多日线门</th><th>做空参数</th><th>做空日线门</th>"
        "<th>空头斜率阈值</th><th>空头止损</th><th>ATR分位上限</th><th>空头退出模型</th>"
        "</tr></thead><tbody>"
        + "".join(rows)
        + "</tbody></table>"
    )


def build_range_rows(data_ranges: dict[str, dict[str, object]]) -> str:
    rows = []
    for symbol in SYMBOLS:
        item = data_ranges[symbol]
        rows.append(
            "<tr>"
            f"<td>{html.escape(SYMBOL_LABELS[symbol])}</td>"
            f"<td>{item['start_utc']}</td>"
            f"<td>{item['end_utc']}</td>"
            f"<td>{item['entry_candles']}</td>"
            f"<td>{item['daily_candles']}</td>"
            "</tr>"
        )
    return (
        "<table><thead><tr><th>币种</th><th>1H起点</th><th>1H终点</th><th>1H样本</th><th>1D样本</th></tr></thead><tbody>"
        + "".join(rows)
        + "</tbody></table>"
    )


def scope_coin_view(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out["win_rate"] = out["win_rate"].astype(float) * 100
    return out[["coin", "side", "trades", "total_pnl_u", "profit_factor", "win_rate", "avg_r", "avg_hold_hours", "max_drawdown_u", "return_pct_on_10k"]]


def pick_summary(frame: pd.DataFrame, coin: str, side: str) -> pd.Series:
    return frame[(frame["coin"] == coin) & (frame["side"] == side)].iloc[0]


def build_equity_chart(trades: pd.DataFrame, title: str):
    frame = trades.sort_values("exit_ts").copy()
    frame["time"] = pd.to_datetime(frame["exit_ts"], unit="ms", utc=True)
    frame["equity"] = float(INITIAL_CAPITAL) + frame["pnl_u"].astype(float).cumsum()
    fig, ax = plt.subplots(figsize=(10, 4.8))
    ax.plot(frame["time"], frame["equity"], color="#1d4ed8", linewidth=1.6)
    ax.set_title(title, fontsize=14, pad=12)
    ax.set_ylabel("U")
    ax.grid(alpha=0.22)
    fig.tight_layout()
    return fig


def build_concurrent_chart(concurrent: pd.DataFrame, title: str):
    fig, ax = plt.subplots(figsize=(10, 4.8))
    if not concurrent.empty:
        times = pd.to_datetime(concurrent["ts"], unit="ms", utc=True)
        ax.plot(times, concurrent["total_notional_usdt"], color="#15803d", linewidth=1.5)
    ax.set_title(title, fontsize=14, pad=12)
    ax.set_ylabel("USDT")
    ax.grid(alpha=0.22)
    fig.tight_layout()
    return fig


def dataframe_to_html(frame: pd.DataFrame, *, float_cols: dict[str, int] | None = None) -> str:
    float_cols = float_cols or {}
    headers = "".join(f"<th>{html.escape(str(col))}</th>" for col in frame.columns)
    rows = []
    for _, row in frame.iterrows():
        cells = []
        for col in frame.columns:
            value = row[col]
            if col in float_cols and value != "":
                text = f"{float(value):.{float_cols[col]}f}"
            else:
                text = str(value)
            cells.append(f"<td>{html.escape(text)}</td>")
        rows.append("<tr>" + "".join(cells) + "</tr>")
    return f"<table><thead><tr>{headers}</tr></thead><tbody>{''.join(rows)}</tbody></table>"


def figure_to_base64(fig: plt.Figure) -> str:
    buffer = io.BytesIO()
    fig.savefig(buffer, format="png", dpi=160, bbox_inches="tight")
    plt.close(fig)
    return base64.b64encode(buffer.getvalue()).decode("ascii")


def fmt2(value: object) -> str:
    return format_decimal_fixed(Decimal(str(value)), 2)


if __name__ == "__main__":
    main()
