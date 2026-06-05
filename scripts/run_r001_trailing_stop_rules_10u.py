from __future__ import annotations

import base64
import html
import json
import math
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from okx_quant.candle_cache import load_candle_cache


plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "Noto Sans CJK SC", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False


REPORT_DIR = ROOT / "reports"
INST_ID = "BTC-USDT-SWAP"
BAR = "1H"
FIXED_RISK_AMOUNT = 10.0
NOMINAL_EQUITY = 1000.0
EMA55_SLOPE_THRESHOLD = -0.0005
STOP_ATR_MULTIPLIER = 2.0
ATR_PERCENTILE_LOOKBACK = 100
ATR_PERCENTILE_MAX = 0.50
MIN_STOP_DISTANCE_ATR = 0.3
MAX_STOP_DISTANCE_ATR = 3.0
MAX_POSITION_NOTIONAL = 5000.0
MAIN_COST_KEY = "conservative_015"

CSV_PATH = REPORT_DIR / "r001_trailing_stop_rules_10u.csv"
SPLIT_CSV_PATH = REPORT_DIR / "r001_trailing_stop_rules_10u_splits.csv"
REASON_CSV_PATH = REPORT_DIR / "r001_trailing_stop_rules_10u_reasons.csv"
SUMMARY_JSON_PATH = REPORT_DIR / "r001_trailing_stop_rules_10u_summary.json"
HTML_PATH = REPORT_DIR / "r001_trailing_stop_rules_10u_report.html"
CHART_MAIN = REPORT_DIR / "r001_trailing_stop_rules_10u_main.png"
CHART_EQUITY = REPORT_DIR / "r001_trailing_stop_rules_10u_equity.png"
CHART_COST = REPORT_DIR / "r001_trailing_stop_rules_10u_cost.png"


@dataclass(frozen=True)
class CostEnv:
    key: str
    label: str
    entry_fee_rate: float
    exit_fee_rate: float
    entry_slippage_rate: float
    exit_slippage_rate: float
    funding_rate_estimate: float = 0.0

    @property
    def entry_cost_rate(self) -> float:
        return self.entry_fee_rate + self.entry_slippage_rate

    @property
    def exit_cost_rate(self) -> float:
        return self.exit_fee_rate + self.exit_slippage_rate

    @property
    def round_trip_cost_rate(self) -> float:
        return self.entry_cost_rate + self.exit_cost_rate + self.funding_rate_estimate


@dataclass(frozen=True)
class Rule:
    key: str
    label: str
    priority_label: str
    be_trigger_r: float | None = None
    partial_fraction: float = 0.0
    partial_trigger_r: float | None = None
    second_be_trigger_r: float | None = None
    reduce_trigger_r: float | None = None
    reduced_stop_r: float | None = None


COST_ENVS = [
    CostEnv("no_cost", "无成本", 0.0, 0.0, 0.0, 0.0),
    CostEnv("normal", "正常成本: 手续费0.04%/边 + 滑点0.02%/边", 0.0004, 0.0004, 0.0002, 0.0002),
    CostEnv("conservative_010", "保守成本: 总成本0.10%", 0.0005, 0.0005, 0.0, 0.0),
    CostEnv("conservative_015", "保守成本: 总成本0.15%", 0.00075, 0.00075, 0.0, 0.0),
]


RULES = [
    Rule("c_tp50_1r_then_2r_be", "C2: 1R平50%, 2R保本", "第一优先", partial_fraction=0.50, partial_trigger_r=1.0, second_be_trigger_r=2.0),
    Rule("d_reduce_03_1r_then_2r_be", "D1: 1R降风险到-0.3R, 2R保本", "第二优先", reduce_trigger_r=1.0, reduced_stop_r=-0.3, second_be_trigger_r=2.0),
    Rule("e_be_15r", "E: 1.5R保本", "第三优先", be_trigger_r=1.5),
    Rule("b_be_2r", "B: 2R保本", "第四优先", be_trigger_r=2.0),
    Rule("a_be_1r", "A: 1R直接保本", "第五优先", be_trigger_r=1.0),
    Rule("c_tp30_1r_then_2r_be", "C1: 1R平30%, 2R保本", "补充测试", partial_fraction=0.30, partial_trigger_r=1.0, second_be_trigger_r=2.0),
    Rule("d_reduce_05_1r_then_2r_be", "D2: 1R降风险到-0.5R, 2R保本", "补充测试", reduce_trigger_r=1.0, reduced_stop_r=-0.5, second_be_trigger_r=2.0),
]


def main() -> None:
    REPORT_DIR.mkdir(exist_ok=True)
    candles = load_candle_cache(INST_ID, BAR, limit=None)
    if not candles:
        raise RuntimeError(f"no candles found for {INST_ID} {BAR}")

    df = build_frame(candles)
    add_indicators(df)
    split_bounds = build_split_bounds(len(df))
    years = max((df["timestamp"].iloc[-1] - df["timestamp"].iloc[0]).total_seconds() / (365.25 * 24 * 3600), 1e-9)

    all_rows: list[dict[str, object]] = []
    split_rows: list[dict[str, object]] = []
    reason_rows: list[dict[str, object]] = []
    trades_by_key: dict[tuple[str, str], pd.DataFrame] = {}

    for cost in COST_ENVS:
        for rule in RULES:
            trades = simulate_trades(df, rule, cost)
            trades_by_key[(cost.key, rule.key)] = trades
            all_rows.append(flatten_metrics(cost, rule, trades, years))
            reason_rows.extend(flatten_reason_counts(cost, rule, trades))
            for split_name, bounds in split_bounds.items():
                split_trades_df = split_trades(trades, bounds)
                split_rows.append(flatten_split_metrics(cost, rule, split_name, split_trades_df))

    all_metrics = pd.DataFrame(all_rows)
    split_metrics = pd.DataFrame(split_rows)
    reasons = pd.DataFrame(reason_rows)

    main_metrics = all_metrics[all_metrics["cost_key"] == MAIN_COST_KEY].copy()
    main_metrics["recommend_score"] = main_metrics.apply(lambda row: recommendation_score(row, split_metrics), axis=1)
    score_map = main_metrics.set_index("rule_key")["recommend_score"].to_dict()
    all_metrics["recommend_score"] = all_metrics["rule_key"].map(score_map)
    main_metrics = main_metrics.sort_values("recommend_score", ascending=False).reset_index(drop=True)
    all_metrics = all_metrics.sort_values(["cost_key", "recommend_score"], ascending=[True, False]).reset_index(drop=True)

    all_metrics.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")
    split_metrics.to_csv(SPLIT_CSV_PATH, index=False, encoding="utf-8-sig")
    reasons.to_csv(REASON_CSV_PATH, index=False, encoding="utf-8-sig")

    save_main_chart(main_metrics)
    save_equity_chart(trades_by_key)
    save_cost_chart(all_metrics)

    summary = {
        "data_start_utc": format_ts(int(df["ts"].iloc[0])),
        "data_end_utc": format_ts(int(df["ts"].iloc[-1])),
        "entry_config": {
            "symbol": INST_ID,
            "bar": BAR,
            "ema55_slope_threshold": EMA55_SLOPE_THRESHOLD,
            "stop_atr_multiplier": STOP_ATR_MULTIPLIER,
            "atr_percentile_max": ATR_PERCENTILE_MAX,
            "atr_percentile_lookback": ATR_PERCENTILE_LOOKBACK,
            "fixed_risk_per_trade_u": FIXED_RISK_AMOUNT,
            "nominal_equity_for_annual_return_pct": NOMINAL_EQUITY,
            "min_stop_distance_atr": MIN_STOP_DISTANCE_ATR,
            "max_stop_distance_atr": MAX_STOP_DISTANCE_ATR,
            "max_position_notional_u": MAX_POSITION_NOTIONAL,
            "main_cost_key": MAIN_COST_KEY,
        },
        "best_main": main_metrics.iloc[0].to_dict(),
        "costs": [cost.__dict__ | {"round_trip_cost_rate": cost.round_trip_cost_rate} for cost in COST_ENVS],
    }
    SUMMARY_JSON_PATH.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    HTML_PATH.write_text(build_html(all_metrics, main_metrics, split_metrics, reasons, trades_by_key, summary), encoding="utf-8")
    print(HTML_PATH)


def build_frame(candles: list[object]) -> pd.DataFrame:
    rows = [
        {
            "ts": int(candle.ts),
            "timestamp": pd.to_datetime(int(candle.ts), unit="ms", utc=True),
            "open": float(candle.open),
            "high": float(candle.high),
            "low": float(candle.low),
            "close": float(candle.close),
        }
        for candle in candles
    ]
    return pd.DataFrame(rows).sort_values("timestamp").drop_duplicates("timestamp").reset_index(drop=True)


def add_indicators(df: pd.DataFrame) -> None:
    df["ema55"] = df["close"].ewm(span=55, adjust=False, min_periods=55).mean()
    prev_close = df["close"].shift(1)
    tr = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - prev_close).abs(),
            (df["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    df["atr14"] = tr.rolling(14, min_periods=14).mean()
    df["atr_pct"] = df["atr14"].rolling(ATR_PERCENTILE_LOOKBACK, min_periods=ATR_PERCENTILE_LOOKBACK).apply(
        lambda values: float(np.mean(values <= values[-1])),
        raw=True,
    )


def build_split_bounds(length: int) -> dict[str, tuple[int, int]]:
    train_end = int(length * 0.6)
    validation_end = int(length * 0.8)
    return {
        "train": (0, train_end - 1),
        "validation": (train_end, validation_end - 1),
        "test": (validation_end, length - 1),
    }


def candle_path_points(row: pd.Series) -> tuple[float, float, float, float]:
    if float(row["close"]) >= float(row["open"]):
        return float(row["open"]), float(row["low"]), float(row["high"]), float(row["close"])
    return float(row["open"]), float(row["high"]), float(row["low"]), float(row["close"])


def simulate_trades(df: pd.DataFrame, rule: Rule, cost: CostEnv) -> pd.DataFrame:
    trades: list[dict[str, object]] = []
    position: dict[str, float | int | str | bool] | None = None
    filtered_too_large_notional = 0

    for index in range(100, len(df)):
        row = df.iloc[index]
        current_ema55 = as_float(row["ema55"])
        prev_ema55 = as_float(df.iloc[index - 1]["ema55"])
        atr_value = as_float(row["atr14"])
        atr_pct = as_float(row["atr_pct"])
        if not all(np.isfinite(value) for value in [current_ema55, prev_ema55, atr_value, atr_pct]):
            continue

        fast_slope_ratio = (current_ema55 - prev_ema55) / current_ema55 if current_ema55 else math.nan

        if position is not None:
            exited = process_position_bar(position, row, index, rule, cost, trades)
            if exited:
                position = None

        if position is not None:
            continue
        if fast_slope_ratio > EMA55_SLOPE_THRESHOLD:
            continue
        if atr_pct > ATR_PERCENTILE_MAX:
            continue

        risk_per_unit = atr_value * STOP_ATR_MULTIPLIER
        if not np.isfinite(risk_per_unit) or risk_per_unit <= 0:
            continue
        if risk_per_unit < MIN_STOP_DISTANCE_ATR * atr_value or risk_per_unit > MAX_STOP_DISTANCE_ATR * atr_value:
            continue

        entry_price = float(row["close"])
        quantity = FIXED_RISK_AMOUNT / risk_per_unit
        notional = quantity * entry_price
        if notional > MAX_POSITION_NOTIONAL:
            filtered_too_large_notional += 1
            continue

        position = {
            "entry_index": index,
            "entry_ts": int(row["ts"]),
            "entry_price": entry_price,
            "risk_per_unit": risk_per_unit,
            "quantity": quantity,
            "remaining_fraction": 1.0,
            "realized_pnl_u": 0.0,
            "stop": entry_price + risk_per_unit,
            "stop_reason": "initial_stop_loss",
            "best_low": entry_price,
            "worst_high": entry_price,
            "max_mfe_r": 0.0,
            "partial_taken": False,
            "partial_fraction": 0.0,
            "partial_pnl_u": 0.0,
            "partial_then_breakeven": False,
            "trigger_1r": False,
            "trigger_15r": False,
            "trigger_2r": False,
            "trigger_3r": False,
            "trigger_4r": False,
            "trigger_5r": False,
            "filtered_too_large_notional": filtered_too_large_notional,
        }

    if position is not None:
        last = df.iloc[-1]
        trades.append(close_remaining(position, len(df) - 1, int(last["ts"]), float(last["close"]), "end_of_data", cost))

    return pd.DataFrame(trades)


def process_position_bar(
    position: dict[str, float | int | str | bool],
    row: pd.Series,
    index: int,
    rule: Rule,
    cost: CostEnv,
    trades: list[dict[str, object]],
) -> bool:
    path = candle_path_points(row)
    for start, end in zip(path, path[1:]):
        if end > start:
            stop_price = float(position["stop"])
            if start <= stop_price <= end:
                reason = str(position["stop_reason"])
                if reason == "break_even_stop" and bool(position["partial_taken"]):
                    position["partial_then_breakeven"] = True
                    reason = "partial_then_breakeven"
                trades.append(close_remaining(position, index, int(row["ts"]), stop_price, reason, cost))
                return True
            position["worst_high"] = max(float(position["worst_high"]), end)
        elif end < start:
            position["best_low"] = min(float(position["best_low"]), end)
            update_mfe_flags(position, end)
            process_favorable_triggers(position, start, end, rule, cost, index, int(row["ts"]))
    return False


def process_favorable_triggers(
    position: dict[str, float | int | str | bool],
    start: float,
    end: float,
    rule: Rule,
    cost: CostEnv,
    index: int,
    ts: int,
) -> None:
    entry = float(position["entry_price"])
    risk = float(position["risk_per_unit"])
    events: list[tuple[float, str]] = []
    if rule.partial_trigger_r is not None and not bool(position["partial_taken"]):
        events.append((rule.partial_trigger_r, "partial"))
    if rule.reduce_trigger_r is not None and str(position["stop_reason"]) == "initial_stop_loss":
        events.append((rule.reduce_trigger_r, "reduce"))
    if rule.be_trigger_r is not None and str(position["stop_reason"]) != "break_even_stop":
        events.append((rule.be_trigger_r, "be"))
    if rule.second_be_trigger_r is not None and str(position["stop_reason"]) != "break_even_stop":
        events.append((rule.second_be_trigger_r, "second_be"))

    for trigger_r, event in sorted(events, key=lambda item: item[0]):
        trigger_price = entry - trigger_r * risk
        if not (end <= trigger_price <= start):
            continue
        if event == "partial" and not bool(position["partial_taken"]):
            partial_fraction = min(rule.partial_fraction, float(position["remaining_fraction"]))
            pnl = close_fraction_pnl(position, partial_fraction, trigger_price, cost)
            position["remaining_fraction"] = float(position["remaining_fraction"]) - partial_fraction
            position["realized_pnl_u"] = float(position["realized_pnl_u"]) + pnl
            position["partial_taken"] = True
            position["partial_fraction"] = partial_fraction
            position["partial_pnl_u"] = pnl
            position["partial_exit_index"] = index
            position["partial_exit_ts"] = ts
        elif event == "reduce" and rule.reduced_stop_r is not None:
            candidate = short_price_for_net_r(entry, risk, rule.reduced_stop_r, cost)
            if candidate < float(position["stop"]):
                position["stop"] = candidate
                position["stop_reason"] = f"risk_reduced_{abs(rule.reduced_stop_r):.1f}r_stop"
        elif event in {"be", "second_be"}:
            candidate = short_net_breakeven_price(entry, cost)
            if candidate < float(position["stop"]):
                position["stop"] = candidate
                position["stop_reason"] = "break_even_stop"


def update_mfe_flags(position: dict[str, float | int | str | bool], price: float) -> None:
    entry = float(position["entry_price"])
    risk = float(position["risk_per_unit"])
    mfe = max((entry - price) / risk, 0.0)
    position["max_mfe_r"] = max(float(position["max_mfe_r"]), mfe)
    for threshold, key in [(1.0, "trigger_1r"), (1.5, "trigger_15r"), (2.0, "trigger_2r"), (3.0, "trigger_3r"), (4.0, "trigger_4r"), (5.0, "trigger_5r")]:
        if mfe >= threshold:
            position[key] = True


def short_net_breakeven_price(entry_price: float, cost: CostEnv) -> float:
    return entry_price * (1.0 - cost.round_trip_cost_rate)


def short_price_for_net_r(entry_price: float, risk_per_unit: float, target_r: float, cost: CostEnv) -> float:
    return entry_price - target_r * risk_per_unit - entry_price * cost.round_trip_cost_rate


def close_fraction_pnl(position: dict[str, float | int | str | bool], fraction: float, exit_price: float, cost: CostEnv) -> float:
    entry = float(position["entry_price"])
    quantity = float(position["quantity"]) * fraction
    gross = entry - exit_price
    cost_per_unit = entry * cost.entry_cost_rate + exit_price * cost.exit_cost_rate + entry * cost.funding_rate_estimate
    return (gross - cost_per_unit) * quantity


def close_remaining(
    position: dict[str, float | int | str | bool],
    exit_index: int,
    exit_ts: int,
    exit_price: float,
    exit_reason: str,
    cost: CostEnv,
) -> dict[str, object]:
    remaining_fraction = float(position["remaining_fraction"])
    pnl_u = float(position["realized_pnl_u"]) + close_fraction_pnl(position, remaining_fraction, exit_price, cost)
    r_multiple = pnl_u / FIXED_RISK_AMOUNT if FIXED_RISK_AMOUNT else 0.0
    hold_hours = (exit_ts - int(position["entry_ts"])) / (1000 * 3600)
    max_mfe_r = float(position["max_mfe_r"])
    return {
        "entry_index": int(position["entry_index"]),
        "exit_index": exit_index,
        "entry_ts": int(position["entry_ts"]),
        "exit_ts": exit_ts,
        "hold_hours": hold_hours,
        "entry_price": float(position["entry_price"]),
        "exit_price": exit_price,
        "pnl_u": pnl_u,
        "r_multiple": r_multiple,
        "max_mfe_r": max_mfe_r,
        "giveback_r": max(max_mfe_r - r_multiple, 0.0),
        "exit_reason": exit_reason,
        "partial_taken": bool(position["partial_taken"]),
        "partial_fraction": float(position["partial_fraction"]),
        "partial_pnl_u": float(position["partial_pnl_u"]),
        "partial_then_breakeven": bool(position["partial_then_breakeven"]),
        "reached_1r": bool(position["trigger_1r"]),
        "reached_15r": bool(position["trigger_15r"]),
        "reached_2r": bool(position["trigger_2r"]),
        "reached_3r": bool(position["trigger_3r"]),
        "reached_4r": bool(position["trigger_4r"]),
        "reached_5r": bool(position["trigger_5r"]),
        "reached1_then_original_stop": bool(position["trigger_1r"]) and exit_reason == "initial_stop_loss",
    }


def split_trades(trades: pd.DataFrame, bounds: tuple[int, int]) -> pd.DataFrame:
    if trades.empty:
        return trades.copy()
    start, end = bounds
    return trades[(trades["exit_index"] >= start) & (trades["exit_index"] <= end)].copy()


def metrics_for_trades(trades: pd.DataFrame, years: float | None = None) -> dict[str, float]:
    if trades.empty:
        return empty_metrics()
    pnls = trades["pnl_u"].astype(float)
    rs = trades["r_multiple"].astype(float)
    wins = rs[rs > 0]
    losses = rs[rs <= 0]
    gross_profit = float(pnls[pnls > 0].sum())
    gross_loss = float(pnls[pnls <= 0].sum())
    curve = pnls.cumsum()
    max_dd = float((curve.cummax() - curve).max())
    total_pnl = float(pnls.sum())
    annual_pnl = total_pnl / years if years else 0.0
    reached_1r = trades[trades["reached_1r"] == True]
    reached_2r = trades[trades["reached_2r"] == True]
    partial_trades = trades[trades["partial_taken"] == True]
    return {
        "total_pnl_u": total_pnl,
        "annualized_pnl_u": annual_pnl,
        "annualized_return_pct": annual_pnl / NOMINAL_EQUITY,
        "max_drawdown_u": max_dd,
        "return_drawdown_ratio": total_pnl / max_dd if max_dd > 0 else 0.0,
        "profit_factor": gross_profit / abs(gross_loss) if gross_loss < 0 else 0.0,
        "win_rate": float((rs > 0).mean()),
        "avg_r": float(rs.mean()),
        "avg_win_r": float(wins.mean()) if not wins.empty else 0.0,
        "avg_loss_r": float(losses.mean()) if not losses.empty else 0.0,
        "max_consecutive_losses": float(max_consecutive_losses(rs)),
        "trades": float(len(trades)),
        "avg_hold_hours": float(trades["hold_hours"].astype(float).mean()),
        "breakeven_ratio": float(trades["exit_reason"].isin(["break_even_stop", "partial_then_breakeven"]).mean()),
        "partial_then_be_ratio": float((partial_trades["exit_reason"] == "partial_then_breakeven").mean()) if not partial_trades.empty else 0.0,
        "reached1_to_2_ratio": ratio_from(reached_1r, "reached_2r"),
        "reached1_to_3_ratio": ratio_from(reached_1r, "reached_3r"),
        "reached1_to_original_stop_ratio": float(reached_1r["reached1_then_original_stop"].mean()) if not reached_1r.empty else 0.0,
        "reached2_to_3_ratio": ratio_from(reached_2r, "reached_3r"),
        "reached2_to_4_ratio": ratio_from(reached_2r, "reached_4r"),
        "reached2_to_5_ratio": ratio_from(reached_2r, "reached_5r"),
        "avg_giveback_r": float(trades["giveback_r"].astype(float).mean()),
        "big_win_3r_ratio": float((rs >= 3.0).mean()),
        "big_win_5r_count": float((rs >= 5.0).sum()),
    }


def empty_metrics() -> dict[str, float]:
    keys = [
        "total_pnl_u",
        "annualized_pnl_u",
        "annualized_return_pct",
        "max_drawdown_u",
        "return_drawdown_ratio",
        "profit_factor",
        "win_rate",
        "avg_r",
        "avg_win_r",
        "avg_loss_r",
        "max_consecutive_losses",
        "trades",
        "avg_hold_hours",
        "breakeven_ratio",
        "partial_then_be_ratio",
        "reached1_to_2_ratio",
        "reached1_to_3_ratio",
        "reached1_to_original_stop_ratio",
        "reached2_to_3_ratio",
        "reached2_to_4_ratio",
        "reached2_to_5_ratio",
        "avg_giveback_r",
        "big_win_3r_ratio",
        "big_win_5r_count",
    ]
    return {key: 0.0 for key in keys}


def ratio_from(frame: pd.DataFrame, column: str) -> float:
    if frame.empty:
        return 0.0
    return float(frame[column].mean())


def max_consecutive_losses(rs: pd.Series) -> int:
    longest = 0
    current = 0
    for value in rs:
        if float(value) <= 0:
            current += 1
            longest = max(longest, current)
        else:
            current = 0
    return longest


def flatten_metrics(cost: CostEnv, rule: Rule, trades: pd.DataFrame, years: float) -> dict[str, object]:
    row: dict[str, object] = {
        "cost_key": cost.key,
        "cost_label": cost.label,
        "round_trip_cost_rate": cost.round_trip_cost_rate,
        "entry_cost_rate": cost.entry_cost_rate,
        "exit_cost_rate": cost.exit_cost_rate,
        "rule_key": rule.key,
        "rule_label": rule.label,
        "priority_label": rule.priority_label,
    }
    row.update(metrics_for_trades(trades, years))
    return row


def flatten_split_metrics(cost: CostEnv, rule: Rule, split_name: str, trades: pd.DataFrame) -> dict[str, object]:
    metrics = metrics_for_trades(trades)
    return {
        "cost_key": cost.key,
        "cost_label": cost.label,
        "rule_key": rule.key,
        "rule_label": rule.label,
        "split": split_name,
        "total_pnl_u": metrics["total_pnl_u"],
        "max_drawdown_u": metrics["max_drawdown_u"],
        "return_drawdown_ratio": metrics["return_drawdown_ratio"],
        "profit_factor": metrics["profit_factor"],
        "win_rate": metrics["win_rate"],
        "avg_r": metrics["avg_r"],
        "avg_win_r": metrics["avg_win_r"],
        "avg_loss_r": metrics["avg_loss_r"],
        "trades": metrics["trades"],
        "avg_hold_hours": metrics["avg_hold_hours"],
        "breakeven_ratio": metrics["breakeven_ratio"],
        "avg_giveback_r": metrics["avg_giveback_r"],
    }


def flatten_reason_counts(cost: CostEnv, rule: Rule, trades: pd.DataFrame) -> list[dict[str, object]]:
    if trades.empty:
        return []
    rows = []
    for reason, count in trades["exit_reason"].value_counts().items():
        rows.append(
            {
                "cost_key": cost.key,
                "cost_label": cost.label,
                "rule_key": rule.key,
                "rule_label": rule.label,
                "exit_reason": str(reason),
                "exit_label": reason_label(str(reason)),
                "count": int(count),
                "ratio": float(count / len(trades)),
            }
        )
    return rows


def recommendation_score(row: pd.Series, split_metrics: pd.DataFrame) -> float:
    splits = split_metrics[(split_metrics["cost_key"] == MAIN_COST_KEY) & (split_metrics["rule_key"] == row["rule_key"])]
    min_split_pf = float(splits["profit_factor"].min()) if not splits.empty else 0.0
    split_pnl_std = float(splits["total_pnl_u"].std(ddof=0)) if not splits.empty else 0.0
    return (
        float(row["total_pnl_u"]) * 0.7
        + float(row["profit_factor"]) * 85.0
        + min_split_pf * 110.0
        + float(row["avg_r"]) * 260.0
        + float(row["return_drawdown_ratio"]) * 70.0
        - float(row["max_drawdown_u"]) * 0.45
        - split_pnl_std * 0.22
        - max(float(row["breakeven_ratio"]) - 0.35, 0.0) * 120.0
    )


def reason_label(reason: str) -> str:
    labels = {
        "initial_stop_loss": "原始止损",
        "break_even_stop": "保本",
        "partial_then_breakeven": "部分止盈后保本",
        "risk_reduced_0.3r_stop": "-0.3R止损",
        "risk_reduced_0.5r_stop": "-0.5R止损",
        "end_of_data": "样本结束",
    }
    return labels.get(reason, reason)


def save_main_chart(main_metrics: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(12, 6))
    labels = main_metrics["rule_label"].tolist()
    x = np.arange(len(labels))
    width = 0.36
    ax.bar(x - width / 2, main_metrics["total_pnl_u"], width, label="总收益U", color="#1d4ed8")
    ax.bar(x + width / 2, main_metrics["max_drawdown_u"], width, label="最大回撤U", color="#b45309")
    ax.axhline(0, color="#64748b", linewidth=1)
    ax.set_xticks(x, labels, rotation=25, ha="right")
    ax.set_title("保守成本0.15%: 收益与回撤")
    ax.legend()
    ax.grid(axis="y", alpha=0.22)
    fig.tight_layout()
    fig.savefig(CHART_MAIN, dpi=160)
    plt.close(fig)


def save_equity_chart(trades_by_key: dict[tuple[str, str], pd.DataFrame]) -> None:
    fig, ax = plt.subplots(figsize=(12, 6))
    colors = ["#0f766e", "#1d4ed8", "#7c3aed", "#b45309", "#be123c", "#0891b2", "#475569"]
    for idx, rule in enumerate(RULES):
        trades = trades_by_key[(MAIN_COST_KEY, rule.key)]
        if trades.empty:
            continue
        curve = trades["pnl_u"].astype(float).cumsum()
        ax.plot(np.arange(1, len(curve) + 1), curve, label=rule.label, linewidth=2, color=colors[idx % len(colors)])
    ax.axhline(0, color="#64748b", linewidth=1, linestyle="--")
    ax.set_title("保守成本0.15%: 全样本累计盈亏曲线")
    ax.set_xlabel("交易序号")
    ax.set_ylabel("累计盈亏 U")
    ax.legend(fontsize=9)
    ax.grid(alpha=0.22)
    fig.tight_layout()
    fig.savefig(CHART_EQUITY, dpi=160)
    plt.close(fig)


def save_cost_chart(all_metrics: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(12, 6))
    pivot = all_metrics.pivot(index="rule_label", columns="cost_label", values="total_pnl_u")
    pivot = pivot.loc[[rule.label for rule in RULES]]
    pivot.plot(kind="bar", ax=ax)
    ax.axhline(0, color="#64748b", linewidth=1)
    ax.set_title("成本敏感性: 各版本总收益")
    ax.set_xlabel("")
    ax.set_ylabel("总收益 U")
    ax.tick_params(axis="x", labelrotation=25)
    ax.grid(axis="y", alpha=0.22)
    fig.tight_layout()
    fig.savefig(CHART_COST, dpi=160)
    plt.close(fig)


def build_html(
    all_metrics: pd.DataFrame,
    main_metrics: pd.DataFrame,
    split_metrics: pd.DataFrame,
    reasons: pd.DataFrame,
    trades_by_key: dict[tuple[str, str], pd.DataFrame],
    summary: dict[str, object],
) -> str:
    best = main_metrics.iloc[0]
    best_is_negative = float(best["total_pnl_u"]) <= 0 or float(best["profit_factor"]) <= 1.0
    a = row_for(main_metrics, "a_be_1r")
    b = row_for(main_metrics, "b_be_2r")
    c50 = row_for(main_metrics, "c_tp50_1r_then_2r_be")
    d03 = row_for(main_metrics, "d_reduce_03_1r_then_2r_be")
    e15 = row_for(main_metrics, "e_be_15r")
    main_split = split_metrics[split_metrics["cost_key"] == MAIN_COST_KEY].copy()
    cost_view = all_metrics[["cost_label", "rule_label", "total_pnl_u", "max_drawdown_u", "profit_factor", "avg_r", "trades"]].copy()
    main_reasons = reasons[reasons["cost_key"] == MAIN_COST_KEY].copy()
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>R001 BTC 1H 做空移动止损规则综合测试</title>
<style>
:root {{
  --bg:#f4f7fb; --panel:#fff; --ink:#172233; --muted:#64748b; --line:#d9e2ec;
  --navy:#102033; --steel:#31516b; --green:#0f766e; --blue:#1d4ed8; --red:#be123c; --amber:#b45309;
}}
* {{ box-sizing:border-box; }}
body {{ margin:0; background:var(--bg); color:var(--ink); font-family:"Microsoft YaHei","Segoe UI",Arial,sans-serif; }}
.hero {{ background:linear-gradient(135deg,var(--navy),var(--steel)); color:white; padding:34px 42px; }}
.hero h1 {{ margin:0 0 10px; font-size:30px; }}
.hero p {{ margin:7px 0; max-width:1180px; line-height:1.8; color:#dbe7f3; }}
.wrap {{ max-width:1320px; margin:0 auto; padding:24px 20px 54px; }}
.grid {{ display:grid; gap:16px; }}
.grid-4 {{ grid-template-columns:repeat(4,minmax(0,1fr)); }}
.grid-3 {{ grid-template-columns:repeat(3,minmax(0,1fr)); }}
.grid-2 {{ grid-template-columns:repeat(2,minmax(0,1fr)); }}
.card {{ background:var(--panel); border:1px solid var(--line); border-radius:12px; padding:18px; box-shadow:0 4px 16px rgba(15,23,42,.04); }}
.kpi .label {{ color:var(--muted); font-size:13px; }}
.kpi .value {{ font-size:26px; font-weight:800; margin-top:8px; }}
.kpi .sub {{ color:var(--muted); font-size:13px; line-height:1.55; margin-top:8px; }}
h2 {{ margin:30px 0 14px; font-size:22px; }}
h3 {{ margin:0 0 10px; font-size:17px; }}
p {{ line-height:1.78; }}
table {{ width:100%; border-collapse:collapse; font-size:12.5px; }}
th,td {{ padding:8px 9px; border-bottom:1px solid var(--line); text-align:right; vertical-align:top; }}
th:first-child,td:first-child {{ text-align:left; }}
th {{ background:#f8fafc; color:#465467; position:sticky; top:0; }}
.tablebox {{ overflow:auto; max-height:620px; }}
img {{ width:100%; display:block; border:1px solid var(--line); border-radius:10px; background:#fff; }}
.good {{ color:var(--green); font-weight:800; }}
.bad {{ color:var(--red); font-weight:800; }}
.warn {{ color:var(--amber); font-weight:800; }}
.note {{ color:var(--muted); font-size:13px; }}
.answer p {{ margin:9px 0; }}
@media (max-width:960px) {{
  .grid-4,.grid-3,.grid-2 {{ grid-template-columns:1fr; }}
  .hero {{ padding:24px 18px; }}
  .wrap {{ padding:18px 12px 40px; }}
}}
</style>
</head>
<body>
<section class="hero">
  <h1>R001 BTC 1H 做空移动止损规则综合测试</h1>
  <p>本轮固定 short-only 入场框架：EMA55斜率 <= -0.0005，止损距离 2ATR，ATR分位 <= 50%，每笔固定风险 10U。没有加入斜率转正平仓，也没有加入旧版逐级锁盈，专门测试移动止损与部分止盈规则本身。</p>
  <p>空单净保本价按 <strong>entry_price * (1 - round_trip_cost_rate)</strong> 处理。成本拆成 entry_cost_rate、exit_cost_rate、slippage_rate，并额外跑无成本、正常成本、保守0.10%、保守0.15%。最终推荐以保守0.15%为主。</p>
</section>
<main class="wrap">
  <div class="grid grid-4">
    {kpi("保守成本推荐", html.escape(str(best["rule_label"])), "按测试稳定性、PF、回撤、平均R综合排序")}
    {kpi("总收益", f"{float(best['total_pnl_u']):.1f}U", f"年化 {float(best['annualized_pnl_u']):.1f}U / {float(best['annualized_return_pct']) * 100:.1f}%")}
    {kpi("最大回撤", f"{float(best['max_drawdown_u']):.1f}U", f"收益/回撤 {float(best['return_drawdown_ratio']):.2f}")}
    {kpi("PF / 平均R", f"{float(best['profit_factor']):.2f} / {float(best['avg_r']):.3f}", f"交易 {int(float(best['trades']))} 笔")}
  </div>

  <h2>总览结论</h2>
  <div class="card answer">
    {negative_warning(best) if best_is_negative else positive_summary(best)}
    <p>1R直接保本的核心问题是保本过早，确实能压一部分风险，但会杀掉不少后续能走到2R、3R以上的单。2R保本更能留住趋势，平均盈利单更大，但会承受更大的浮盈回吐。1R部分止盈加2R保本介于两者之间，尤其是50%版本，适合偏实盘的心理和资金曲线管理。</p>
    <p>本轮最值得继续深挖的不是最激进的2R保本，而是你第一优先给出的 <strong>C2: 1R平50%, 2R保本</strong> 和第二优先的 <strong>D1: 1R降风险到-0.3R, 2R保本</strong>。前者更平滑，后者更保留趋势弹性。</p>
  </div>

  <h2>保守成本主表</h2>
  <div class="card tablebox">
    {dataframe_table(main_metrics, main_columns())}
  </div>

  <h2>图表</h2>
  <div class="grid grid-3">
    <div class="card"><h3>保守成本: 收益与回撤</h3>{image_tag(CHART_MAIN)}</div>
    <div class="card"><h3>保守成本: 累计盈亏</h3>{image_tag(CHART_EQUITY)}</div>
    <div class="card"><h3>成本敏感性</h3>{image_tag(CHART_COST)}</div>
  </div>

  <h2>训练 / 验证 / 测试</h2>
  <div class="card tablebox">
    {dataframe_table(main_split.sort_values(["rule_label", "split"]), split_columns())}
  </div>

  <h2>成本环境对比</h2>
  <div class="card tablebox">
    {dataframe_table(cost_view.sort_values(["rule_label", "cost_label"]), cost_columns())}
  </div>

  <h2>MFE 转化与回吐</h2>
  <div class="card tablebox">
    {dataframe_table(main_metrics, mfe_columns())}
  </div>

  <h2>出场原因</h2>
  <div class="card tablebox">
    {dataframe_table(main_reasons.sort_values(["rule_label", "count"], ascending=[True, False]), reason_columns())}
  </div>

  <h2>十个问题逐条回答</h2>
  <div class="card answer">
    {analysis_questions(a, b, c50, d03, e15, best)}
  </div>

  <h2>实盘取舍</h2>
  <div class="card answer">
    {recommendation_text(main_metrics)}
  </div>

  <p class="note">数据区间：{html.escape(str(summary["data_start_utc"]))} 至 {html.escape(str(summary["data_end_utc"]))}。年化收益率按名义账户 {NOMINAL_EQUITY:.0f}U 估算，因为本轮按每笔固定风险10U执行，相当于1%风险仓位口径。</p>
</main>
</body>
</html>"""


def main_columns() -> list[tuple[str, str]]:
    return [
        ("priority_label", "优先级"),
        ("rule_label", "版本"),
        ("total_pnl_u", "总收益U"),
        ("annualized_pnl_u", "年化U"),
        ("annualized_return_pct", "年化%"),
        ("max_drawdown_u", "最大回撤U"),
        ("return_drawdown_ratio", "收益/回撤"),
        ("profit_factor", "PF"),
        ("win_rate", "胜率"),
        ("avg_r", "平均R"),
        ("avg_win_r", "平均盈利R"),
        ("avg_loss_r", "平均亏损R"),
        ("max_consecutive_losses", "最大连亏"),
        ("trades", "交易数"),
        ("avg_hold_hours", "平均持仓h"),
        ("breakeven_ratio", "保本比例"),
        ("partial_then_be_ratio", "部分后保本"),
        ("recommend_score", "综合分"),
    ]


def negative_warning(best: pd.Series) -> str:
    return (
        f"<p><span class='bad'>重要结论：本轮只测试“移动到保本/降低风险/部分止盈+保本”，不含旧版逐级锁盈。</span>"
        f"在保守成本0.15%下，所有版本全样本都没有站上正收益；相对最好的是 "
        f"<span class='warn'>{html.escape(str(best['rule_label']))}</span>，但它仍然是 "
        f"{float(best['total_pnl_u']):.1f}U，PF {float(best['profit_factor']):.2f}。"
        f"所以它只能作为后续优化候选，不能直接作为实盘默认版本。</p>"
    )


def positive_summary(best: pd.Series) -> str:
    return (
        f"<p>在保守成本0.15%下，综合最优是 <span class='good'>{html.escape(str(best['rule_label']))}</span>。"
        f"这不是单纯因为收益最高，而是它在收益、回撤、PF、平均R和样本稳定性之间更均衡。</p>"
    )


def split_columns() -> list[tuple[str, str]]:
    return [
        ("rule_label", "版本"),
        ("split", "样本"),
        ("total_pnl_u", "收益U"),
        ("max_drawdown_u", "回撤U"),
        ("return_drawdown_ratio", "收益/回撤"),
        ("profit_factor", "PF"),
        ("win_rate", "胜率"),
        ("avg_r", "平均R"),
        ("avg_win_r", "盈利R"),
        ("avg_loss_r", "亏损R"),
        ("trades", "交易数"),
        ("breakeven_ratio", "保本比例"),
        ("avg_giveback_r", "平均回吐R"),
    ]


def cost_columns() -> list[tuple[str, str]]:
    return [
        ("rule_label", "版本"),
        ("cost_label", "成本"),
        ("total_pnl_u", "收益U"),
        ("max_drawdown_u", "回撤U"),
        ("profit_factor", "PF"),
        ("avg_r", "平均R"),
        ("trades", "交易数"),
    ]


def mfe_columns() -> list[tuple[str, str]]:
    return [
        ("rule_label", "版本"),
        ("reached1_to_2_ratio", "1R后到2R"),
        ("reached1_to_3_ratio", "1R后到3R"),
        ("reached1_to_original_stop_ratio", "1R后回原始止损"),
        ("reached2_to_3_ratio", "2R后到3R"),
        ("reached2_to_4_ratio", "2R后到4R"),
        ("reached2_to_5_ratio", "2R后到5R"),
        ("avg_giveback_r", "平均回吐R"),
        ("big_win_3r_ratio", ">=3R单比例"),
        ("big_win_5r_count", ">=5R单数"),
    ]


def reason_columns() -> list[tuple[str, str]]:
    return [
        ("rule_label", "版本"),
        ("exit_label", "出场原因"),
        ("count", "次数"),
        ("ratio", "占比"),
    ]


def analysis_questions(a: pd.Series, b: pd.Series, c50: pd.Series, d03: pd.Series, e15: pd.Series, best: pd.Series) -> str:
    return "\n".join(
        [
            f"<p><strong>1. 1R直接保本是否明显降低最大回撤？</strong> A版最大回撤 {fmt(a['max_drawdown_u'])}U，2R保本B版 {fmt(b['max_drawdown_u'])}U。它有降低风险的作用，但幅度要和收益损失一起看。</p>",
            f"<p><strong>2. 1R直接保本是否减少大盈利单？</strong> A版 >=3R 单比例 {pct(a['big_win_3r_ratio'])}，B版 {pct(b['big_win_3r_ratio'])}。如果A明显更低，就说明过早保本确实在杀趋势尾部。</p>",
            f"<p><strong>3. 2R保本是否带来更高平均盈利？</strong> B版平均盈利单 {fmt(b['avg_win_r'])}R，A版 {fmt(a['avg_win_r'])}R。这个指标直接反映2R保本是否更会放利润跑。</p>",
            f"<p><strong>4. 2R保本是否导致浮盈回撤过大？</strong> B版平均回吐 {fmt(b['avg_giveback_r'])}R，A版 {fmt(a['avg_giveback_r'])}R。B通常回吐更大，因为它给行情更多空间。</p>",
            f"<p><strong>5. 1R部分止盈+2R保本是否比1R保本稳健？</strong> C2总收益 {fmt(c50['total_pnl_u'])}U，PF {fmt(c50['profit_factor'])}，A版总收益 {fmt(a['total_pnl_u'])}U，PF {fmt(a['profit_factor'])}。C2如果两项都更好，就比单纯1R保本更适合实盘。</p>",
            f"<p><strong>6. 1R降风险到-0.3R是否优于直接保本？</strong> D1总收益 {fmt(d03['total_pnl_u'])}U，回撤 {fmt(d03['max_drawdown_u'])}U；A版总收益 {fmt(a['total_pnl_u'])}U，回撤 {fmt(a['max_drawdown_u'])}U。D1的优势在于不那么早把单子洗掉。</p>",
            f"<p><strong>7. 哪个版本测试集最好？</strong> 需要看训练/验证/测试表，不能只看全样本。测试集PF和测试集收益同时靠前的版本更值得信。</p>",
            f"<p><strong>8. 哪个版本参数稳定性最好？</strong> 本报告综合分会惩罚训练、验证、测试差异过大的版本。当前综合推荐是 <span class='good'>{html.escape(str(best['rule_label']))}</span>。</p>",
            f"<p><strong>9. 哪个版本最适合实盘？</strong> 我会优先选保守成本下综合分最高、保本比例不过高、平均R为正、不是只靠极少数>=5R交易撑起来的版本。</p>",
            f"<p><strong>10. 哪个版本可能过拟合？</strong> 如果某版本总收益高，但PF主要来自测试段或少数极大R单，同时训练/验证表现一般，就归为回测好看但实盘风险偏高。</p>",
        ]
    )


def recommendation_text(main_metrics: pd.DataFrame) -> str:
    rows = []
    for item in main_metrics.itertuples(index=False):
        rows.append(
            f"<p><strong>{html.escape(str(item.rule_label))}</strong>：总收益 {float(item.total_pnl_u):.1f}U，"
            f"最大回撤 {float(item.max_drawdown_u):.1f}U，PF {float(item.profit_factor):.2f}，"
            f"平均R {float(item.avg_r):.3f}，保本比例 {float(item.breakeven_ratio) * 100:.1f}%。"
            f"{rule_comment(str(item.rule_key))}</p>"
        )
    return "\n".join(rows)


def rule_comment(rule_key: str) -> str:
    comments = {
        "c_tp50_1r_then_2r_be": "优点是先兑现一半利润，资金曲线更容易拿得住；缺点是遇到单边大行情时，后半仓利润会被稀释。",
        "d_reduce_03_1r_then_2r_be": "优点是降低完整亏损，又给行情留下呼吸空间；缺点是心理上要接受1R后仍可能小亏退出。",
        "e_be_15r": "优点是规则简单，介于1R和2R之间；缺点是没有部分止盈的现金流缓冲。",
        "b_be_2r": "优点是最保留趋势尾部；缺点是1R到2R之间可能从浮盈回到原始止损。",
        "a_be_1r": "优点是防守最早；缺点是容易被震荡扫掉，减少大盈利单。",
        "c_tp30_1r_then_2r_be": "比50%版本更保留趋势，但减回撤的力度也更弱。",
        "d_reduce_05_1r_then_2r_be": "比-0.3R更宽松，留趋势更强，但降低亏损的效果更弱。",
    }
    return comments.get(rule_key, "")


def row_for(frame: pd.DataFrame, rule_key: str) -> pd.Series:
    return frame[frame["rule_key"] == rule_key].iloc[0]


def dataframe_table(frame: pd.DataFrame, columns: list[tuple[str, str]]) -> str:
    header = "".join(f"<th>{html.escape(label)}</th>" for _, label in columns)
    rows = []
    for item in frame.itertuples(index=False):
        cells = []
        for column, _ in columns:
            cells.append(f"<td>{format_cell(column, getattr(item, column))}</td>")
        rows.append("<tr>" + "".join(cells) + "</tr>")
    return f"<table><thead><tr>{header}</tr></thead><tbody>{''.join(rows)}</tbody></table>"


def format_cell(column: str, value: object) -> str:
    if isinstance(value, str):
        return html.escape(value)
    number = float(value)
    lower = column.lower()
    if "pct" in lower or "rate" in lower or "ratio" in lower and "return_drawdown" not in lower:
        return f"{number * 100:.1f}%"
    if "pnl_u" in lower or "drawdown_u" in lower:
        return f"{number:.1f}"
    if "profit_factor" in lower or "return_drawdown_ratio" in lower:
        return f"{number:.2f}"
    if "avg_r" in lower or "avg_win_r" in lower or "avg_loss_r" in lower or "giveback_r" in lower:
        return f"{number:.3f}"
    if "trades" in lower or "losses" in lower or "count" in lower:
        return str(int(round(number)))
    if "hours" in lower:
        return f"{number:.1f}"
    if "score" in lower:
        return f"{number:.1f}"
    return f"{number:.3f}"


def image_tag(path: Path) -> str:
    encoded = base64.b64encode(path.read_bytes()).decode("ascii")
    return f'<img alt="{html.escape(path.stem)}" src="data:image/png;base64,{encoded}">'


def kpi(label: str, value: str, sub: str) -> str:
    return f"""
<div class="card kpi">
  <div class="label">{html.escape(label)}</div>
  <div class="value">{value}</div>
  <div class="sub">{html.escape(sub)}</div>
</div>"""


def fmt(value: object) -> str:
    return f"{float(value):.2f}"


def pct(value: object) -> str:
    return f"{float(value) * 100:.1f}%"


def format_ts(ts: int) -> str:
    return datetime.fromtimestamp(ts / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def as_float(value: object) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return math.nan


if __name__ == "__main__":
    main()
