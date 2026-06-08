from __future__ import annotations

import base64
import html
import io
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
SCRIPTS_DIR = ROOT / "scripts"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import run_ema55_slope_short_reentry_ema21_rebound_matrix_5coins_10u as base


plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "Noto Sans CJK SC", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False


REPORT_DIR = ROOT / "reports"
ANALYSIS_DIR = ROOT.parent / "qqokx_data" / "reports" / "analysis"
ENTRY_BAR = base.ENTRY_BAR
SYMBOLS = base.SYMBOLS
COIN_LABELS = base.COIN_LABELS
RISK_PER_TRADE_U = base.RISK_PER_TRADE_U
TAKER_FEE_RATE = base.TAKER_FEE_RATE
ATR_STOP_MULTIPLIER = base.ATR_STOP_MULTIPLIER
ATR_PERCENTILE_LOOKBACK = base.ATR_PERCENTILE_LOOKBACK
ATR_PERCENTILE_MAX = base.ATR_PERCENTILE_MAX
EMA55_SLOPE_THRESHOLD = base.EMA55_SLOPE_THRESHOLD
INITIAL_CAPITAL = base.INITIAL_CAPITAL
RESET_NEAR_ATR_RATIO = 0.30


@dataclass(frozen=True)
class Variant:
    key: str
    label: str
    note: str
    locked_reset_rule: str = "none"
    locked_min_r: int = 0
    locked_max_r: int = 999
    extend_dynamic_stop_if_signal: bool = False


VARIANTS = (
    Variant(
        key="baseline",
        label="原策略",
        note="平仓后不做再入场状态管理；动态止盈触发即按原止损价平仓。",
    ),
    Variant(
        key="locked_all_ema21_near",
        label="全部锁盈后EMA21接近再开",
        note="所有 locked_xr_stop 出场后，都要先反抽到 EMA21 附近，再跌回 EMA21 下方才允许再空。",
        locked_reset_rule="near",
        locked_min_r=1,
    ),
    Variant(
        key="locked_2_only_ema21_near",
        label="仅2R锁盈后EMA21接近再开",
        note="只针对 locked_2r_stop：先接近 EMA21，再跌回 EMA21 下方才允许再空。",
        locked_reset_rule="near",
        locked_min_r=2,
        locked_max_r=2,
    ),
    Variant(
        key="locked_2_3_ema21_near",
        label="仅2R-3R锁盈后EMA21接近再开",
        note="只针对 locked_2r_stop 和 locked_3r_stop 做 EMA21 反抽-再转弱状态机。",
        locked_reset_rule="near",
        locked_min_r=2,
        locked_max_r=3,
    ),
    Variant(
        key="locked_3plus_ema21_near",
        label="仅3R以上锁盈后EMA21接近再开",
        note="只针对 locked_3r_stop 及以上做 EMA21 反抽-再转弱状态机。",
        locked_reset_rule="near",
        locked_min_r=3,
    ),
    Variant(
        key="extend_1r_if_signal",
        label="动态止盈触发且信号仍在则多给1R",
        note="当动态止盈本来要触发、且本根 K 收盘仍满足原做空条件时，不立即平仓，而是把当前止损位再放宽 1R 后退出。",
        extend_dynamic_stop_if_signal=True,
    ),
    Variant(
        key="locked_2_3_near_plus_extend",
        label="2R-3R重置 + 信号仍在多给1R",
        note="先对 locked_2r/3r 出场加 EMA21 重置；若动态止盈触发时信号仍在，再额外放宽 1R。",
        locked_reset_rule="near",
        locked_min_r=2,
        locked_max_r=3,
        extend_dynamic_stop_if_signal=True,
    ),
)


def main() -> None:
    REPORT_DIR.mkdir(exist_ok=True)
    ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    basename = f"ema55_slope_short_reentry_and_extend_compare_5coins_10u_{stamp}"
    html_path = ANALYSIS_DIR / f"{basename}.html"
    csv_path = ANALYSIS_DIR / f"{basename}.csv"
    coin_csv_path = ANALYSIS_DIR / f"{basename}_by_coin.csv"
    trades_csv_path = ANALYSIS_DIR / f"{basename}_trades.csv"
    json_path = ANALYSIS_DIR / f"{basename}.json"

    all_trades: list[dict[str, object]] = []
    coin_rows: list[dict[str, object]] = []
    data_ranges: dict[str, dict[str, object]] = {}

    for symbol in SYMBOLS:
        candles = base.load_candle_cache(symbol, ENTRY_BAR, limit=None)
        if not candles:
            data_ranges[symbol] = {"error": "missing candles"}
            continue
        frame = base.build_frame(candles)
        base.add_indicators(frame)
        bounds = base.build_split_bounds(len(frame))
        data_ranges[symbol] = {
            "entry_candles": len(frame),
            "start_utc": base.format_ts(int(frame["ts"].iloc[0])),
            "end_utc": base.format_ts(int(frame["ts"].iloc[-1])),
        }

        for variant in VARIANTS:
            trades = simulate_trades(frame, variant)
            if trades.empty:
                trades = empty_trades_frame()
            trades["symbol"] = symbol
            trades["coin"] = COIN_LABELS[symbol]
            trades["mode_key"] = variant.key
            trades["mode_label"] = variant.label
            trades["split"] = trades["exit_index"].apply(lambda idx: base.split_name_for_index(int(idx), bounds))
            trades["year"] = pd.to_datetime(trades["exit_ts"], unit="ms", utc=True).dt.strftime("%Y")
            trades["mode_note"] = variant.note
            all_trades.extend(trades.to_dict("records"))
            coin_rows.append(flatten_coin_metrics(symbol=symbol, variant=variant, trades=trades))

    trades_frame = pd.DataFrame(all_trades)
    if trades_frame.empty:
        raise RuntimeError("no trades generated")

    trades_frame = trades_frame.sort_values(["mode_key", "exit_ts", "entry_ts", "coin"]).reset_index(drop=True)
    coin_frame = pd.DataFrame(coin_rows).sort_values(["coin", "mode_key"]).reset_index(drop=True)
    summary_frame = build_summary_frame(trades_frame)
    summary_frame["score"] = summary_frame.apply(score_summary_row, axis=1)
    summary_frame = summary_frame.sort_values(["score", "test_pnl_u"], ascending=[False, False]).reset_index(drop=True)

    csv_path.write_text(summary_frame.to_csv(index=False), encoding="utf-8-sig")
    coin_csv_path.write_text(coin_frame.to_csv(index=False), encoding="utf-8-sig")
    trades_csv_path.write_text(trades_frame.to_csv(index=False), encoding="utf-8-sig")
    payload = build_payload(summary_frame, coin_frame, trades_frame, data_ranges)
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    html_path.write_text(build_html(summary_frame, coin_frame, trades_frame, data_ranges), encoding="utf-8")
    print(html_path)


def empty_trades_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "entry_index",
            "exit_index",
            "entry_ts",
            "exit_ts",
            "entry_price",
            "exit_price",
            "risk_per_unit",
            "pnl_u",
            "r_multiple",
            "hold_hours",
            "exit_reason",
            "entry_ema21",
            "entry_ema55",
            "entry_slope_ratio",
            "entry_atr_pct",
            "reentry_gate_triggered",
            "reentry_gate_type",
            "extension_used",
            "extension_from_reason",
        ]
    )


def simulate_trades(df: pd.DataFrame, variant: Variant) -> pd.DataFrame:
    trades: list[dict[str, object]] = []
    position: dict[str, float | int | str] | None = None
    reset_state: str | None = None
    gate_trigger_count = 0
    gate_type = ""
    start_index = max(ATR_PERCENTILE_LOOKBACK, 60)

    for index in range(start_index, len(df)):
        row = df.iloc[index]
        ema21 = finite(row["ema21"])
        ema55 = finite(row["ema55"])
        prev_ema55 = finite(df.iloc[index - 1]["ema55"])
        atr_value = finite(row["atr14"])
        atr_pct = finite(row["atr_pct"])
        close_price = finite(row["close"])
        if any(math.isnan(value) for value in [ema21, ema55, prev_ema55, atr_value, atr_pct, close_price]):
            continue

        slope_ratio = (ema55 - prev_ema55) / ema55 if ema55 else math.nan
        signal_valid = (
            np.isfinite(slope_ratio)
            and slope_ratio <= EMA55_SLOPE_THRESHOLD
            and atr_pct <= ATR_PERCENTILE_MAX
            and atr_value > 0
        )

        if position is not None:
            closed_trade = process_open_short(
                position,
                variant,
                candle_open=finite(row["open"]),
                candle_high=finite(row["high"]),
                candle_low=finite(row["low"]),
                candle_close=close_price,
                signal_valid=bool(signal_valid),
                candle_ts=int(row["ts"]),
                index=index,
            )
            if closed_trade is not None:
                trades.append(closed_trade)
                exit_reason = str(closed_trade["exit_reason"])
                position = None
                locked_r = locked_r_from_exit_reason(exit_reason)
                if (
                    variant.locked_reset_rule != "none"
                    and locked_r is not None
                    and variant.locked_min_r <= locked_r <= variant.locked_max_r
                ):
                    reset_state = "await_rebound"
                    gate_trigger_count += 1
                    gate_type = f"locked_{locked_r}_{variant.locked_reset_rule}"

        if position is not None:
            continue

        if reset_state is not None:
            if not reset_ready(variant.locked_reset_rule, reset_state, close_price, ema21, atr_value):
                continue
            if reset_state == "await_rebound":
                reset_state = "await_rebreak"
                continue
            reset_state = None
            gate_type = ""

        if not signal_valid:
            continue

        entry_price = close_price
        fee_offset = entry_price * TAKER_FEE_RATE * 2.0
        risk_per_unit = atr_value * ATR_STOP_MULTIPLIER
        position = {
            "entry_index": index,
            "entry_ts": int(row["ts"]),
            "entry_price": entry_price,
            "risk_per_unit": risk_per_unit,
            "stop": entry_price + risk_per_unit,
            "stop_reason": "stop_loss",
            "fee_offset": fee_offset,
            "next_dynamic_r": 2.0,
            "entry_ema21": ema21,
            "entry_ema55": ema55,
            "entry_slope_ratio": slope_ratio,
            "entry_atr_pct": atr_pct,
            "reentry_gate_triggered": 1 if gate_trigger_count > 0 else 0,
            "reentry_gate_type": gate_type,
            "extension_used": 0,
            "extension_from_reason": "",
        }
        gate_trigger_count = 0

    return pd.DataFrame(trades)


def process_open_short(
    position: dict[str, float | int | str],
    variant: Variant,
    *,
    candle_open: float,
    candle_high: float,
    candle_low: float,
    candle_close: float,
    signal_valid: bool,
    candle_ts: int,
    index: int,
) -> dict[str, object] | None:
    path = base.candle_path_points(
        candle_open=candle_open,
        candle_high=candle_high,
        candle_low=candle_low,
        candle_close=candle_close,
    )
    for start, end in zip(path, path[1:]):
        if end > start:
            while True:
                stop_price = float(position["stop"])
                if not (start <= stop_price <= end):
                    break
                if maybe_extend_dynamic_stop(position, variant, stop_price=stop_price, segment_end=end, signal_valid=signal_valid):
                    continue
                return close_trade(position, index, candle_ts, stop_price, str(position["stop_reason"]))
        else:
            advance_step_dynamic(position, end)
    return None


def maybe_extend_dynamic_stop(
    position: dict[str, float | int | str],
    variant: Variant,
    *,
    stop_price: float,
    segment_end: float,
    signal_valid: bool,
) -> bool:
    if not variant.extend_dynamic_stop_if_signal:
        return False
    if not signal_valid:
        return False
    if bool(position["extension_used"]):
        return False
    reason = str(position["stop_reason"])
    if reason != "break_even_stop" and locked_r_from_exit_reason(reason) is None:
        return False
    risk = float(position["risk_per_unit"])
    extended_stop = stop_price + risk
    position["stop"] = extended_stop
    position["stop_reason"] = "extended_1r_signal_hold_stop"
    position["extension_used"] = 1
    position["extension_from_reason"] = reason
    return extended_stop <= segment_end


def advance_step_dynamic(position: dict[str, float | int | str], favorable_price: float) -> None:
    entry = float(position["entry_price"])
    risk = float(position["risk_per_unit"])
    fee_offset = float(position["fee_offset"])
    while True:
        next_r = float(position["next_dynamic_r"])
        trigger = entry - risk * next_r - fee_offset
        if favorable_price > trigger:
            break
        if math.isclose(next_r, 2.0):
            locked_r = 0.0
            reason = "break_even_stop"
        else:
            locked_r = max(next_r - 1.0, 0.0)
            reason = f"locked_{int(round(locked_r))}r_stop"
        candidate_stop = entry - risk * locked_r - fee_offset
        if candidate_stop < float(position["stop"]):
            position["stop"] = candidate_stop
            position["stop_reason"] = reason
        position["next_dynamic_r"] = next_r + 1.0


def reset_ready(rule: str, state: str, close_price: float, line_value: float, atr_value: float) -> bool:
    if rule == "near":
        if state == "await_rebound":
            return abs(close_price - line_value) <= atr_value * RESET_NEAR_ATR_RATIO
        return close_price < line_value
    return True


def locked_r_from_exit_reason(exit_reason: str | None) -> int | None:
    if not exit_reason or not exit_reason.startswith("locked_") or not exit_reason.endswith("r_stop"):
        return None
    raw = exit_reason.removeprefix("locked_").removesuffix("r_stop")
    try:
        return int(raw)
    except ValueError:
        return None


def close_trade(
    position: dict[str, float | int | str],
    exit_index: int,
    exit_ts: int,
    exit_price: float,
    exit_reason: str,
) -> dict[str, object]:
    entry = float(position["entry_price"])
    risk = float(position["risk_per_unit"])
    quantity = RISK_PER_TRADE_U / risk if risk > 0 else 0.0
    pnl_per_unit = (entry - exit_price) - TAKER_FEE_RATE * (entry + exit_price)
    pnl_u = pnl_per_unit * quantity
    return {
        "entry_index": int(position["entry_index"]),
        "exit_index": exit_index,
        "entry_ts": int(position["entry_ts"]),
        "exit_ts": exit_ts,
        "entry_price": entry,
        "exit_price": exit_price,
        "risk_per_unit": risk,
        "pnl_u": pnl_u,
        "r_multiple": pnl_u / RISK_PER_TRADE_U,
        "hold_hours": (exit_ts - int(position["entry_ts"])) / (1000 * 3600),
        "exit_reason": exit_reason,
        "entry_ema21": float(position["entry_ema21"]),
        "entry_ema55": float(position["entry_ema55"]),
        "entry_slope_ratio": float(position["entry_slope_ratio"]),
        "entry_atr_pct": float(position["entry_atr_pct"]),
        "reentry_gate_triggered": int(position["reentry_gate_triggered"]),
        "reentry_gate_type": str(position["reentry_gate_type"]),
        "extension_used": int(position["extension_used"]),
        "extension_from_reason": str(position["extension_from_reason"]),
    }


def split_trades(trades: pd.DataFrame, split_name: str) -> pd.DataFrame:
    if trades.empty:
        return trades.copy()
    return trades[trades["split"] == split_name].copy()


def compute_metrics(trades: pd.DataFrame) -> dict[str, float]:
    if trades.empty:
        return {
            "trades": 0.0,
            "total_pnl_u": 0.0,
            "profit_factor": 0.0,
            "win_rate": 0.0,
            "avg_r": 0.0,
            "avg_hold_hours": 0.0,
            "max_drawdown_u": 0.0,
            "gate_trade_rate": 0.0,
            "extension_rate": 0.0,
        }
    pnls = trades["pnl_u"].astype(float)
    gross_profit = float(pnls[pnls > 0].sum())
    gross_loss = abs(float(pnls[pnls < 0].sum()))
    curve = pnls.cumsum()
    drawdown = float((curve.cummax() - curve).max())
    return {
        "trades": float(len(trades)),
        "total_pnl_u": float(pnls.sum()),
        "profit_factor": gross_profit / gross_loss if gross_loss > 0 else 0.0,
        "win_rate": float((pnls > 0).mean()),
        "avg_r": float(trades["r_multiple"].astype(float).mean()),
        "avg_hold_hours": float(trades["hold_hours"].astype(float).mean()),
        "max_drawdown_u": drawdown,
        "gate_trade_rate": float(trades["reentry_gate_triggered"].astype(float).mean()),
        "extension_rate": float(trades["extension_used"].astype(float).mean()),
    }


def flatten_coin_metrics(*, symbol: str, variant: Variant, trades: pd.DataFrame) -> dict[str, object]:
    all_metrics = compute_metrics(trades)
    validation_metrics = compute_metrics(split_trades(trades, "validation"))
    test_metrics = compute_metrics(split_trades(trades, "test"))
    return {
        "symbol": symbol,
        "coin": COIN_LABELS[symbol],
        "mode_key": variant.key,
        "mode_label": variant.label,
        "mode_note": variant.note,
        "all_trades": int(all_metrics["trades"]),
        "all_pnl_u": all_metrics["total_pnl_u"],
        "all_profit_factor": all_metrics["profit_factor"],
        "all_win_rate": all_metrics["win_rate"],
        "all_avg_r": all_metrics["avg_r"],
        "all_avg_hold_hours": all_metrics["avg_hold_hours"],
        "all_max_drawdown_u": all_metrics["max_drawdown_u"],
        "all_gate_trade_rate": all_metrics["gate_trade_rate"],
        "all_extension_rate": all_metrics["extension_rate"],
        "validation_pnl_u": validation_metrics["total_pnl_u"],
        "test_trades": int(test_metrics["trades"]),
        "test_pnl_u": test_metrics["total_pnl_u"],
        "test_profit_factor": test_metrics["profit_factor"],
        "test_win_rate": test_metrics["win_rate"],
        "test_avg_r": test_metrics["avg_r"],
        "test_avg_hold_hours": test_metrics["avg_hold_hours"],
        "test_max_drawdown_u": test_metrics["max_drawdown_u"],
        "test_gate_trade_rate": test_metrics["gate_trade_rate"],
        "test_extension_rate": test_metrics["extension_rate"],
    }


def build_summary_frame(trades_frame: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for variant in VARIANTS:
        group = trades_frame[trades_frame["mode_key"] == variant.key].copy()
        all_metrics = compute_metrics(group)
        validation_metrics = compute_metrics(split_trades(group, "validation"))
        test_metrics = compute_metrics(split_trades(group, "test"))
        pnl_values = [compute_metrics(split_trades(coin_group, "test"))["total_pnl_u"] for _, coin_group in group.groupby("coin")]
        rows.append(
            {
                "mode_key": variant.key,
                "mode_label": variant.label,
                "mode_note": variant.note,
                "all_trades": int(all_metrics["trades"]),
                "all_pnl_u": all_metrics["total_pnl_u"],
                "all_profit_factor": all_metrics["profit_factor"],
                "all_win_rate": all_metrics["win_rate"],
                "all_avg_r": all_metrics["avg_r"],
                "all_avg_hold_hours": all_metrics["avg_hold_hours"],
                "all_max_drawdown_u": all_metrics["max_drawdown_u"],
                "all_gate_trade_rate": all_metrics["gate_trade_rate"],
                "all_extension_rate": all_metrics["extension_rate"],
                "validation_pnl_u": validation_metrics["total_pnl_u"],
                "test_trades": int(test_metrics["trades"]),
                "test_pnl_u": test_metrics["total_pnl_u"],
                "test_profit_factor": test_metrics["profit_factor"],
                "test_win_rate": test_metrics["win_rate"],
                "test_avg_r": test_metrics["avg_r"],
                "test_avg_hold_hours": test_metrics["avg_hold_hours"],
                "test_max_drawdown_u": test_metrics["max_drawdown_u"],
                "test_gate_trade_rate": test_metrics["gate_trade_rate"],
                "test_extension_rate": test_metrics["extension_rate"],
                "test_positive_coins": sum(1 for value in pnl_values if value > 0),
                "test_pnl_std_u": float(np.std(pnl_values)) if pnl_values else 0.0,
            }
        )
    return pd.DataFrame(rows)


def score_summary_row(row: pd.Series) -> float:
    return (
        float(row["test_pnl_u"])
        - float(row["test_max_drawdown_u"]) * 0.30
        + float(row["test_profit_factor"]) * 35.0
        + float(row["test_positive_coins"]) * 18.0
        - float(row["test_pnl_std_u"]) * 0.10
    )


def build_payload(
    summary_frame: pd.DataFrame,
    coin_frame: pd.DataFrame,
    trades_frame: pd.DataFrame,
    data_ranges: dict[str, dict[str, object]],
) -> dict[str, object]:
    exit_reason_counts = (
        trades_frame.groupby(["mode_key", "mode_label", "exit_reason"], as_index=False)
        .size()
        .rename(columns={"size": "count"})
    )
    gate_rows = (
        trades_frame.groupby(["mode_key", "mode_label", "reentry_gate_type"], as_index=False)
        .size()
        .rename(columns={"size": "count"})
    )
    extension_rows = (
        trades_frame.groupby(["mode_key", "mode_label", "extension_from_reason"], as_index=False)
        .agg(count=("extension_used", "sum"))
    )
    return {
        "created_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "assumptions": {
            "entry_model": "Original EMA55 slope short baseline.",
            "exit_model": "Original 2R break-even then stepwise locked-R trailing stop.",
            "near_reset_atr_ratio": RESET_NEAR_ATR_RATIO,
            "extend_rule": "When a dynamic stop would be hit and the bar close still satisfies the original short entry condition, allow one extra 1R adverse space, then exit.",
            "variants": [variant.__dict__ for variant in VARIANTS],
        },
        "data_ranges": data_ranges,
        "summary_rows": summary_frame.to_dict("records"),
        "coin_rows": coin_frame.to_dict("records"),
        "exit_reason_rows": exit_reason_counts.to_dict("records"),
        "gate_rows": gate_rows.to_dict("records"),
        "extension_rows": extension_rows.to_dict("records"),
    }


def render_bar(summary_frame: pd.DataFrame, column: str, title: str, color: str) -> str:
    plot_frame = summary_frame.sort_values(column, ascending=True)
    fig, ax = plt.subplots(figsize=(11.0, 6.0))
    ax.barh(plot_frame["mode_label"], plot_frame[column], color=color)
    ax.set_title(title)
    ax.set_xlabel("U" if "pnl" in column or "drawdown" in column else column)
    fig.tight_layout()
    return figure_to_base64(fig)


def render_coin_heatmap(coin_frame: pd.DataFrame, column: str, title: str) -> str:
    pivot = coin_frame.pivot(index="coin", columns="mode_label", values=column).reindex(
        index=[COIN_LABELS[s] for s in SYMBOLS]
    )
    fig, ax = plt.subplots(figsize=(13.2, 4.8))
    image = ax.imshow(pivot.to_numpy(dtype=float), cmap="RdYlGn", aspect="auto")
    ax.set_xticks(range(len(pivot.columns)))
    ax.set_xticklabels(pivot.columns, rotation=16, ha="right")
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels(pivot.index)
    ax.set_title(title)
    for row_index in range(len(pivot.index)):
        for col_index in range(len(pivot.columns)):
            value = pivot.iloc[row_index, col_index]
            text = f"{float(value):.2f}" if pd.notna(value) else "-"
            ax.text(col_index, row_index, text, ha="center", va="center", fontsize=8, color="#111827")
    fig.colorbar(image, ax=ax, shrink=0.85)
    fig.tight_layout()
    return figure_to_base64(fig)


def render_best_equity_chart(trades_frame: pd.DataFrame, best_row: pd.Series) -> str:
    selected = trades_frame[trades_frame["mode_key"] == best_row["mode_key"]].copy()
    selected = selected.sort_values(["exit_ts", "entry_ts", "coin"]).reset_index(drop=True)
    selected["equity_u"] = selected["pnl_u"].astype(float).cumsum()
    selected["drawdown_u"] = selected["equity_u"].cummax() - selected["equity_u"]
    fig, axes = plt.subplots(2, 1, figsize=(11.2, 7.2), sharex=True)
    axes[0].plot(selected["equity_u"].to_numpy(), color="#145c54", linewidth=1.8)
    axes[0].set_title(f"综合评分最优资金曲线：{best_row['mode_label']}")
    axes[0].set_ylabel("PnL (U)")
    axes[1].fill_between(np.arange(len(selected)), selected["drawdown_u"].to_numpy(), color="#b86b3f", alpha=0.28)
    axes[1].set_title("回撤")
    axes[1].set_ylabel("Drawdown (U)")
    axes[1].set_xlabel("Trade Sequence")
    fig.tight_layout()
    return figure_to_base64(fig)


def build_html(
    summary_frame: pd.DataFrame,
    coin_frame: pd.DataFrame,
    trades_frame: pd.DataFrame,
    data_ranges: dict[str, dict[str, object]],
) -> str:
    best = summary_frame.iloc[0]
    baseline = summary_frame[summary_frame["mode_key"] == "baseline"].iloc[0]
    best_test = summary_frame.sort_values("test_pnl_u", ascending=False).iloc[0]
    improve_test = float(best["test_pnl_u"]) - float(baseline["test_pnl_u"])
    improve_all = float(best["all_pnl_u"]) - float(baseline["all_pnl_u"])
    improve_dd = float(best["test_max_drawdown_u"]) - float(baseline["test_max_drawdown_u"])
    data_lines = "".join(
        f"<li><strong>{html.escape(COIN_LABELS.get(symbol, symbol))}</strong>: "
        f"{html.escape(str(info.get('start_utc', '-')))} -> {html.escape(str(info.get('end_utc', '-')))}, "
        f"1H={html.escape(str(info.get('entry_candles', '-')))}</li>"
        for symbol, info in data_ranges.items()
    )
    mode_cards = "".join(
        f"<div class='mode-card'><h3>{html.escape(v.label)}</h3><p>{html.escape(v.note)}</p></div>" for v in VARIANTS
    )
    summary_table = dataframe_to_html(
        summary_frame[
            [
                "mode_label",
                "all_pnl_u",
                "all_trades",
                "all_profit_factor",
                "all_max_drawdown_u",
                "all_gate_trade_rate",
                "all_extension_rate",
                "validation_pnl_u",
                "test_pnl_u",
                "test_trades",
                "test_profit_factor",
                "test_max_drawdown_u",
                "test_gate_trade_rate",
                "test_extension_rate",
                "score",
            ]
        ],
        float_cols={
            "all_pnl_u": 1,
            "all_profit_factor": 2,
            "all_max_drawdown_u": 1,
            "all_gate_trade_rate": 2,
            "all_extension_rate": 2,
            "validation_pnl_u": 1,
            "test_pnl_u": 1,
            "test_profit_factor": 2,
            "test_max_drawdown_u": 1,
            "test_gate_trade_rate": 2,
            "test_extension_rate": 2,
            "score": 1,
        },
    )
    coin_table = dataframe_to_html(
        coin_frame[
            [
                "coin",
                "mode_label",
                "all_pnl_u",
                "all_trades",
                "test_pnl_u",
                "test_trades",
                "test_profit_factor",
                "test_max_drawdown_u",
                "test_gate_trade_rate",
                "test_extension_rate",
            ]
        ].sort_values(["coin", "test_pnl_u"], ascending=[True, False]),
        float_cols={
            "all_pnl_u": 1,
            "test_pnl_u": 1,
            "test_profit_factor": 2,
            "test_max_drawdown_u": 1,
            "test_gate_trade_rate": 2,
            "test_extension_rate": 2,
        },
    )
    reason_rows = (
        trades_frame.groupby(["mode_label", "exit_reason"], as_index=False)
        .size()
        .rename(columns={"size": "count"})
        .sort_values(["mode_label", "count"], ascending=[True, False])
    )
    reason_table = dataframe_to_html(reason_rows, float_cols={"count": 0})

    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>EMA55 斜率做空再入场与延迟止盈对比</title>
  <style>
    :root {{
      --bg:#f6f7fb; --panel:#fff; --ink:#1a2333; --muted:#667085; --line:rgba(26,35,51,.10);
      --accent:#145c54; --accent2:#b86b3f; --shadow:0 18px 38px rgba(15,23,42,.08);
    }}
    * {{ box-sizing:border-box; }}
    body {{
      margin:0; color:var(--ink); font-family:"Microsoft YaHei","Segoe UI",sans-serif;
      background:
        radial-gradient(circle at 12% 0%, rgba(20,92,84,.11), transparent 28%),
        radial-gradient(circle at 88% 8%, rgba(184,107,63,.10), transparent 24%),
        linear-gradient(180deg,#fbfcfe 0%,var(--bg) 100%);
    }}
    .wrap {{ width:min(1260px,calc(100vw - 30px)); margin:0 auto; padding:28px 0 56px; }}
    .hero {{ border-radius:28px; padding:30px; color:#fff; background:linear-gradient(135deg,#145c54,#263445); box-shadow:var(--shadow); }}
    .hero h1 {{ margin:10px 0 8px; font-size:34px; line-height:1.08; }}
    .hero p {{ margin:8px 0 0; max-width:980px; line-height:1.72; color:rgba(255,255,255,.9); }}
    .eyebrow {{ font-size:12px; text-transform:uppercase; letter-spacing:.16em; opacity:.82; }}
    .grid {{ display:grid; grid-template-columns:repeat(12,1fr); gap:18px; margin-top:20px; }}
    .card {{ background:var(--panel); border:1px solid var(--line); border-radius:22px; box-shadow:var(--shadow); padding:22px; }}
    .stat {{ grid-column:span 3; }} .wide {{ grid-column:span 6; }} .full {{ grid-column:1/-1; }}
    .k {{ color:var(--muted); font-size:13px; }} .v {{ font-size:28px; font-weight:800; margin-top:8px; color:var(--accent); }}
    .s,.note {{ color:var(--muted); font-size:13px; line-height:1.65; margin-top:8px; }}
    h2 {{ margin:0 0 12px; font-size:20px; }} h3 {{ margin:0 0 8px; font-size:16px; }} p {{ margin:0; line-height:1.7; }}
    ul {{ margin:0; padding-left:18px; line-height:1.8; }}
    .mode-grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(230px,1fr)); gap:14px; }}
    .mode-card {{ border:1px solid var(--line); border-radius:16px; padding:16px; background:#f9fbfd; }}
    img {{ width:100%; border-radius:16px; border:1px solid var(--line); background:#fff; }}
    table {{ width:100%; border-collapse:collapse; font-size:13px; }}
    th,td {{ text-align:left; padding:10px 8px; border-bottom:1px solid rgba(26,35,51,.08); white-space:nowrap; }}
    th {{ color:var(--muted); background:#f7fafc; position:sticky; top:0; }} .scroll {{ overflow:auto; }}
    @media(max-width:960px) {{ .stat,.wide {{ grid-column:1/-1; }} .hero h1 {{ font-size:28px; }} }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <div class="eyebrow">Slope Short / Re-entry + Exit Compare</div>
      <h1>再入场状态机 vs 动态止盈多给 1R</h1>
      <p>这轮把两条思路放到同一张桌子上：一类是只改 locked_xr_stop 出场后的再入场状态机；另一类是当动态止盈本来要触发、但这根 K 收盘仍满足原做空条件时，不立即平仓，而是再放宽 1R。所有版本都保持原入场、原风险金和原样本切分不变。</p>
    </section>
    <section class="grid">
      <div class="card stat"><div class="k">综合评分最优</div><div class="v">{html.escape(str(best["mode_label"]))}</div><div class="s">测试段 {float(best["test_pnl_u"]):.1f}U，回撤 {float(best["test_max_drawdown_u"]):.1f}U。</div></div>
      <div class="card stat"><div class="k">相对原策略</div><div class="v">{improve_test:+.1f}U</div><div class="s">测试段净差；全样本 {improve_all:+.1f}U。</div></div>
      <div class="card stat"><div class="k">回撤变化</div><div class="v">{improve_dd:+.1f}U</div><div class="s">负值代表测试段最大回撤更低。</div></div>
      <div class="card stat"><div class="k">测试段收益第一</div><div class="v">{html.escape(str(best_test["mode_label"]))}</div><div class="s">{float(best_test["test_pnl_u"]):.1f}U，交易 {int(best_test["test_trades"])} 笔。</div></div>

      <div class="card wide"><h2>测试口径</h2><ul>
        <li>标的：{", ".join(COIN_LABELS[s] for s in SYMBOLS)}</li>
        <li>入场：EMA55 单根斜率 <= {EMA55_SLOPE_THRESHOLD:.4f}，ATR 百分位 <= {ATR_PERCENTILE_MAX:.2f}</li>
        <li>原始出场：2R 保本，之后按 locked-R 逐级锁盈</li>
        <li>再入场规则只对 `locked_xr_stop` 做分层状态机；延迟止盈规则只在“本来要被动态止盈、且收盘仍满足原开空条件”时生效</li>
      </ul></div>
      <div class="card wide"><h2>对比版本</h2><div class="mode-grid">{mode_cards}</div></div>
      <div class="card full"><h2>数据覆盖</h2><ul>{data_lines}</ul></div>

      <div class="card wide"><h2>全样本总盈亏</h2><img src="data:image/png;base64,{render_bar(summary_frame, "all_pnl_u", "全样本总盈亏", "#145c54")}" alt="all_pnl"></div>
      <div class="card wide"><h2>测试段总盈亏</h2><img src="data:image/png;base64,{render_bar(summary_frame, "test_pnl_u", "测试段总盈亏", "#b86b3f")}" alt="test_pnl"></div>
      <div class="card wide"><h2>测试段最大回撤</h2><img src="data:image/png;base64,{render_bar(summary_frame, "test_max_drawdown_u", "测试段最大回撤", "#667085")}" alt="drawdown"></div>
      <div class="card wide"><h2>综合最优资金曲线</h2><img src="data:image/png;base64,{render_best_equity_chart(trades_frame, best)}" alt="equity"></div>

      <div class="card full"><h2>模式总表</h2><div class="note">`gate_trade_rate` 表示状态机门槛实际影响了多少交易；`extension_rate` 表示有多少交易真正触发了“多给 1R”规则。</div><div class="scroll" style="margin-top:12px;">{summary_table}</div></div>
      <div class="card wide"><h2>各币种测试段 PnL</h2><img src="data:image/png;base64,{render_coin_heatmap(coin_frame, "test_pnl_u", "各币种测试段 PnL")}" alt="coin_test"></div>
      <div class="card wide"><h2>各币种延迟止盈触发率</h2><img src="data:image/png;base64,{render_coin_heatmap(coin_frame, "test_extension_rate", "各币种测试段 extension_rate")}" alt="coin_extension"></div>
      <div class="card full"><h2>币种明细</h2><div class="scroll">{coin_table}</div></div>
      <div class="card full"><h2>出场原因分布</h2><div class="scroll">{reason_table}</div></div>
    </section>
  </div>
</body>
</html>"""


def dataframe_to_html(frame: pd.DataFrame, *, float_cols: dict[str, int] | None = None) -> str:
    float_cols = float_cols or {}
    headers = "".join(f"<th>{html.escape(str(col))}</th>" for col in frame.columns)
    rows = []
    for _, row in frame.iterrows():
        cells = []
        for col in frame.columns:
            value = row[col]
            if col in float_cols:
                text = "-" if pd.isna(value) else f"{float(value):.{float_cols[col]}f}"
            else:
                text = "-" if pd.isna(value) else str(value)
            cells.append(f"<td>{html.escape(text)}</td>")
        rows.append("<tr>" + "".join(cells) + "</tr>")
    return f"<table><thead><tr>{headers}</tr></thead><tbody>{''.join(rows)}</tbody></table>"


def figure_to_base64(fig: plt.Figure) -> str:
    buffer = io.BytesIO()
    fig.savefig(buffer, format="png", dpi=160, bbox_inches="tight")
    plt.close(fig)
    return base64.b64encode(buffer.getvalue()).decode("ascii")


def finite(value: object) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return math.nan
    return out if np.isfinite(out) else math.nan


if __name__ == "__main__":
    main()
