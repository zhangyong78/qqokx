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
STRONG_TREND_SLOPE = -0.0008
STRONG_TREND_EMA21_GAP_ATR = 0.25


@dataclass(frozen=True)
class ExitVariant:
    key: str
    label: str
    note: str
    kind: str
    break_even_r: float = 2.0
    activation_r: float = 4.0
    keep_profit_ratio: float = 0.65


VARIANTS = (
    ExitVariant(
        key="baseline_ladder",
        label="原策略逐级锁盈",
        note="2R 保本，3R 起每 1R 推一次止损。作为当前基线。",
        kind="baseline",
    ),
    ExitVariant(
        key="trend_delay_4r_ladder",
        label="强趋势单4R后锁盈",
        note="普通单仍按原策略；只有满足强趋势条件的单，2R 后只保本，4R 才开始锁盈。",
        kind="trend_delay_4r_ladder",
    ),
    ExitVariant(
        key="trend_wide_2r_ladder",
        label="强趋势单每2R推进",
        note="普通单维持原策略；强趋势单改为 4R 锁 1R、6R 锁 3R，减少趋势中途被洗出。",
        kind="trend_wide_2r_ladder",
    ),
    ExitVariant(
        key="trend_ema21_after_4r",
        label="强趋势单4R后EMA21跟踪",
        note="普通单维持原策略；强趋势单在 4R 后切换为 EMA21 跟踪止盈。",
        kind="trend_ema21_after_4r",
    ),
    ExitVariant(
        key="trend_pullback35_after_4r",
        label="强趋势单4R后回撤35%",
        note="普通单维持原策略；强趋势单在 4R 后改成保留 65% 最大浮盈的回撤止盈。",
        kind="trend_pullback35_after_4r",
        keep_profit_ratio=0.65,
    ),
)


def main() -> None:
    REPORT_DIR.mkdir(exist_ok=True)
    ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    basename = f"ema55_slope_short_conditional_exit_variants_5coins_10u_{stamp}"
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
    json_path.write_text(
        json.dumps(build_payload(summary_frame, coin_frame, trades_frame, data_ranges), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
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
            "max_favorable_r",
            "trend_mode_activated",
        ]
    )


def simulate_trades(df: pd.DataFrame, variant: ExitVariant) -> pd.DataFrame:
    trades: list[dict[str, object]] = []
    position: dict[str, float | int | str] | None = None
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

        if position is not None:
            closed_trade = process_open_short(
                position,
                variant,
                candle_open=finite(row["open"]),
                candle_high=finite(row["high"]),
                candle_low=finite(row["low"]),
                candle_close=close_price,
                ema21=ema21,
                ema55=ema55,
                atr_value=atr_value,
                slope_ratio=slope_ratio,
                candle_ts=int(row["ts"]),
                index=index,
            )
            if closed_trade is not None:
                trades.append(closed_trade)
                position = None

        if position is not None:
            continue
        if not np.isfinite(slope_ratio) or slope_ratio > EMA55_SLOPE_THRESHOLD:
            continue
        if atr_pct > ATR_PERCENTILE_MAX:
            continue
        if atr_value <= 0:
            continue

        entry_price = close_price
        risk_per_unit = atr_value * ATR_STOP_MULTIPLIER
        position = {
            "entry_index": index,
            "entry_ts": int(row["ts"]),
            "entry_price": entry_price,
            "risk_per_unit": risk_per_unit,
            "stop": entry_price + risk_per_unit,
            "stop_reason": "stop_loss",
            "fee_offset": entry_price * TAKER_FEE_RATE * 2.0,
            "next_trigger_r": 2.0,
            "break_even_done": 0.0,
            "max_favorable_r": 0.0,
            "trend_mode_activated": 0.0,
            "entry_ema21": ema21,
            "entry_ema55": ema55,
            "entry_slope_ratio": slope_ratio,
            "entry_atr_pct": atr_pct,
        }

    return pd.DataFrame(trades)


def process_open_short(
    position: dict[str, float | int | str],
    variant: ExitVariant,
    *,
    candle_open: float,
    candle_high: float,
    candle_low: float,
    candle_close: float,
    ema21: float,
    ema55: float,
    atr_value: float,
    slope_ratio: float,
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
            stop_price = float(position["stop"])
            if start <= stop_price <= end:
                return close_trade(position, index, candle_ts, stop_price, str(position["stop_reason"]))
        else:
            advance_exit_stop(
                position,
                variant,
                favorable_price=end,
                close_price=candle_close,
                ema21=ema21,
                ema55=ema55,
                atr_value=atr_value,
                slope_ratio=slope_ratio,
            )
    return None


def advance_exit_stop(
    position: dict[str, float | int | str],
    variant: ExitVariant,
    *,
    favorable_price: float,
    close_price: float,
    ema21: float,
    ema55: float,
    atr_value: float,
    slope_ratio: float,
) -> None:
    entry = float(position["entry_price"])
    risk = float(position["risk_per_unit"])
    fee_offset = float(position["fee_offset"])
    favorable_r = max((entry - favorable_price - fee_offset) / risk, 0.0)
    position["max_favorable_r"] = max(float(position["max_favorable_r"]), favorable_r)

    if favorable_r >= 2.0 and not bool(position["break_even_done"]):
        update_stop(position, entry - fee_offset, "break_even_stop")
        position["break_even_done"] = 1.0
        position["next_trigger_r"] = 3.0

    trend_active = is_strong_trend_short(
        slope_ratio=slope_ratio,
        close_price=close_price,
        ema21=ema21,
        ema55=ema55,
        atr_value=atr_value,
    )

    if variant.kind == "baseline":
        run_baseline_ladder(position, favorable_r)
        return

    if trend_active and favorable_r >= 2.0:
        position["trend_mode_activated"] = 1.0

    if not bool(position["trend_mode_activated"]):
        run_baseline_ladder(position, favorable_r)
        return

    if variant.kind == "trend_delay_4r_ladder":
        run_delayed_ladder(position, favorable_r, step_r=1.0)
        return
    if variant.kind == "trend_wide_2r_ladder":
        run_delayed_ladder(position, favorable_r, step_r=2.0)
        return
    if variant.kind == "trend_ema21_after_4r":
        if favorable_r >= variant.activation_r:
            update_stop(position, ema21 - fee_offset, "ema21_trail_stop")
        return
    if variant.kind == "trend_pullback35_after_4r":
        if float(position["max_favorable_r"]) >= variant.activation_r:
            locked_r = float(position["max_favorable_r"]) * variant.keep_profit_ratio
            update_stop(position, entry - risk * locked_r - fee_offset, f"pullback_keep_{int(variant.keep_profit_ratio * 100)}pct")
        return

    run_baseline_ladder(position, favorable_r)


def run_baseline_ladder(position: dict[str, float | int | str], favorable_r: float) -> None:
    entry = float(position["entry_price"])
    risk = float(position["risk_per_unit"])
    fee_offset = float(position["fee_offset"])
    while float(position["next_trigger_r"]) <= favorable_r:
        trigger_r = float(position["next_trigger_r"])
        if trigger_r <= 2.0:
            update_stop(position, entry - fee_offset, "break_even_stop")
        else:
            locked_r = max(trigger_r - 1.0, 0.0)
            update_stop(position, entry - risk * locked_r - fee_offset, f"locked_{format_r(locked_r)}r_stop")
        position["next_trigger_r"] = trigger_r + 1.0


def run_delayed_ladder(position: dict[str, float | int | str], favorable_r: float, *, step_r: float) -> None:
    entry = float(position["entry_price"])
    risk = float(position["risk_per_unit"])
    fee_offset = float(position["fee_offset"])
    next_trigger_r = max(float(position["next_trigger_r"]), 4.0)
    while next_trigger_r <= favorable_r:
        locked_r = max(next_trigger_r - 3.0, 0.0)
        update_stop(position, entry - risk * locked_r - fee_offset, f"locked_{format_r(locked_r)}r_stop")
        next_trigger_r += step_r
    position["next_trigger_r"] = next_trigger_r


def is_strong_trend_short(
    *,
    slope_ratio: float,
    close_price: float,
    ema21: float,
    ema55: float,
    atr_value: float,
) -> bool:
    if not np.isfinite(slope_ratio) or not np.isfinite(close_price) or not np.isfinite(ema21) or not np.isfinite(ema55):
        return False
    if atr_value <= 0:
        return False
    return (
        slope_ratio <= STRONG_TREND_SLOPE
        and close_price < ema21 < ema55
        and (ema21 - close_price) >= atr_value * STRONG_TREND_EMA21_GAP_ATR
    )


def update_stop(position: dict[str, float | int | str], candidate_stop: float, reason: str) -> None:
    if candidate_stop < float(position["stop"]):
        position["stop"] = candidate_stop
        position["stop_reason"] = reason


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
        "max_favorable_r": float(position["max_favorable_r"]),
        "trend_mode_activated": int(float(position["trend_mode_activated"]) > 0),
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
            "avg_max_favorable_r": 0.0,
            "trend_activation_rate": 0.0,
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
        "avg_max_favorable_r": float(trades["max_favorable_r"].astype(float).mean()),
        "trend_activation_rate": float(trades["trend_mode_activated"].astype(float).mean()),
    }


def flatten_coin_metrics(*, symbol: str, variant: ExitVariant, trades: pd.DataFrame) -> dict[str, object]:
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
        "all_avg_hold_hours": all_metrics["avg_hold_hours"],
        "all_max_drawdown_u": all_metrics["max_drawdown_u"],
        "all_avg_max_favorable_r": all_metrics["avg_max_favorable_r"],
        "all_trend_activation_rate": all_metrics["trend_activation_rate"],
        "validation_pnl_u": validation_metrics["total_pnl_u"],
        "test_trades": int(test_metrics["trades"]),
        "test_pnl_u": test_metrics["total_pnl_u"],
        "test_profit_factor": test_metrics["profit_factor"],
        "test_avg_hold_hours": test_metrics["avg_hold_hours"],
        "test_max_drawdown_u": test_metrics["max_drawdown_u"],
        "test_avg_max_favorable_r": test_metrics["avg_max_favorable_r"],
        "test_trend_activation_rate": test_metrics["trend_activation_rate"],
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
                "all_avg_hold_hours": all_metrics["avg_hold_hours"],
                "all_max_drawdown_u": all_metrics["max_drawdown_u"],
                "all_avg_max_favorable_r": all_metrics["avg_max_favorable_r"],
                "all_trend_activation_rate": all_metrics["trend_activation_rate"],
                "validation_pnl_u": validation_metrics["total_pnl_u"],
                "test_trades": int(test_metrics["trades"]),
                "test_pnl_u": test_metrics["total_pnl_u"],
                "test_profit_factor": test_metrics["profit_factor"],
                "test_avg_hold_hours": test_metrics["avg_hold_hours"],
                "test_max_drawdown_u": test_metrics["max_drawdown_u"],
                "test_avg_max_favorable_r": test_metrics["avg_max_favorable_r"],
                "test_trend_activation_rate": test_metrics["trend_activation_rate"],
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
    return {
        "created_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "assumptions": {
            "entry": "EMA55 slope short baseline; only exit logic changes.",
            "strong_trend_rule": {
                "slope_ratio_max": STRONG_TREND_SLOPE,
                "structure": "close < ema21 < ema55",
                "ema21_gap_atr": STRONG_TREND_EMA21_GAP_ATR,
            },
            "variants": [variant.__dict__ for variant in VARIANTS],
        },
        "data_ranges": data_ranges,
        "summary_rows": summary_frame.to_dict("records"),
        "coin_rows": coin_frame.to_dict("records"),
        "exit_reason_rows": exit_reason_counts.to_dict("records"),
    }


def render_bar(summary_frame: pd.DataFrame, column: str, title: str, color: str) -> str:
    plot_frame = summary_frame.sort_values(column, ascending=True)
    fig, ax = plt.subplots(figsize=(10.8, 5.8))
    ax.barh(plot_frame["mode_label"], plot_frame[column], color=color)
    ax.set_title(title)
    ax.set_xlabel("U" if "pnl" in column or "drawdown" in column else column)
    fig.tight_layout()
    return figure_to_base64(fig)


def render_coin_heatmap(coin_frame: pd.DataFrame, column: str, title: str) -> str:
    pivot = coin_frame.pivot(index="coin", columns="mode_label", values=column).reindex(
        index=[COIN_LABELS[s] for s in SYMBOLS]
    )
    fig, ax = plt.subplots(figsize=(12.8, 4.8))
    image = ax.imshow(pivot.to_numpy(dtype=float), cmap="RdYlGn", aspect="auto")
    ax.set_xticks(range(len(pivot.columns)))
    ax.set_xticklabels(pivot.columns, rotation=16, ha="right")
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels(pivot.index)
    ax.set_title(title)
    for row_index in range(len(pivot.index)):
        for col_index in range(len(pivot.columns)):
            value = pivot.iloc[row_index, col_index]
            text = f"{float(value):.1f}" if pd.notna(value) else "-"
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
    axes[0].plot(selected["equity_u"].to_numpy(), color="#1f6f5d", linewidth=1.8)
    axes[0].set_title(f"综合评分最优资金曲线：{best_row['mode_label']}")
    axes[0].set_ylabel("PnL (U)")
    axes[1].fill_between(np.arange(len(selected)), selected["drawdown_u"].to_numpy(), color="#c56a45", alpha=0.28)
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
    baseline = summary_frame[summary_frame["mode_key"] == "baseline_ladder"].iloc[0]
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
                "all_avg_hold_hours",
                "all_trend_activation_rate",
                "validation_pnl_u",
                "test_pnl_u",
                "test_trades",
                "test_profit_factor",
                "test_max_drawdown_u",
                "test_avg_hold_hours",
                "test_avg_max_favorable_r",
                "test_trend_activation_rate",
                "score",
            ]
        ],
        float_cols={
            "all_pnl_u": 1,
            "all_profit_factor": 2,
            "all_max_drawdown_u": 1,
            "all_avg_hold_hours": 1,
            "all_trend_activation_rate": 2,
            "validation_pnl_u": 1,
            "test_pnl_u": 1,
            "test_profit_factor": 2,
            "test_max_drawdown_u": 1,
            "test_avg_hold_hours": 1,
            "test_avg_max_favorable_r": 2,
            "test_trend_activation_rate": 2,
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
                "test_avg_hold_hours",
                "test_trend_activation_rate",
            ]
        ].sort_values(["coin", "test_pnl_u"], ascending=[True, False]),
        float_cols={
            "all_pnl_u": 1,
            "test_pnl_u": 1,
            "test_profit_factor": 2,
            "test_max_drawdown_u": 1,
            "test_avg_hold_hours": 1,
            "test_trend_activation_rate": 2,
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
  <title>EMA55 斜率做空条件式慢止盈报告</title>
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
    .wrap {{ width:min(1240px,calc(100vw - 30px)); margin:0 auto; padding:28px 0 56px; }}
    .hero {{ border-radius:28px; padding:30px; color:#fff; background:linear-gradient(135deg,#145c54,#263445); box-shadow:var(--shadow); }}
    .hero h1 {{ margin:10px 0 8px; font-size:34px; line-height:1.08; }}
    .hero p {{ margin:8px 0 0; max-width:940px; line-height:1.72; color:rgba(255,255,255,.9); }}
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
      <div class="eyebrow">Slope Short / Conditional Exit</div>
      <h1>EMA55 斜率做空条件式慢止盈</h1>
      <p>这轮只做一件事：不把“慢止盈”全局套到所有单子上，而是只在明显强趋势单里放宽止盈。强趋势定义统一为 EMA55 斜率更陡、价格结构持续弱、且价格与 EMA21 有明确距离。</p>
    </section>
    <section class="grid">
      <div class="card stat"><div class="k">综合评分最优</div><div class="v">{html.escape(str(best["mode_label"]))}</div><div class="s">测试段 {float(best["test_pnl_u"]):.1f}U，回撤 {float(best["test_max_drawdown_u"]):.1f}U。</div></div>
      <div class="card stat"><div class="k">相对原策略</div><div class="v">{improve_test:+.1f}U</div><div class="s">测试段净差；全样本 {improve_all:+.1f}U。</div></div>
      <div class="card stat"><div class="k">回撤变化</div><div class="v">{improve_dd:+.1f}U</div><div class="s">负值表示比原策略更稳。</div></div>
      <div class="card stat"><div class="k">测试段收益第一</div><div class="v">{html.escape(str(best_test["mode_label"]))}</div><div class="s">{float(best_test["test_pnl_u"]):.1f}U，交易 {int(best_test["test_trades"])} 笔。</div></div>

      <div class="card wide"><h2>测试口径</h2><ul>
        <li>标的：{", ".join(COIN_LABELS[s] for s in SYMBOLS)}</li>
        <li>入场：EMA55 单根斜率 <= {EMA55_SLOPE_THRESHOLD:.4f}，ATR 百分位 <= {ATR_PERCENTILE_MAX:.2f}</li>
        <li>强趋势条件：斜率 <= {STRONG_TREND_SLOPE:.4f}，且 `close < ema21 < ema55`，并且 `EMA21-close >= {STRONG_TREND_EMA21_GAP_ATR:.2f} ATR`</li>
        <li>只有强趋势单才放宽止盈；普通单全部继续按原策略逐级锁盈。</li>
      </ul></div>
      <div class="card wide"><h2>混合版本</h2><div class="mode-grid">{mode_cards}</div></div>
      <div class="card full"><h2>数据覆盖</h2><ul>{data_lines}</ul></div>

      <div class="card wide"><h2>全样本总盈亏</h2><img src="data:image/png;base64,{render_bar(summary_frame, "all_pnl_u", "全样本总盈亏", "#145c54")}" alt="all_pnl"></div>
      <div class="card wide"><h2>测试段总盈亏</h2><img src="data:image/png;base64,{render_bar(summary_frame, "test_pnl_u", "测试段总盈亏", "#b86b3f")}" alt="test_pnl"></div>
      <div class="card wide"><h2>测试段最大回撤</h2><img src="data:image/png;base64,{render_bar(summary_frame, "test_max_drawdown_u", "测试段最大回撤", "#667085")}" alt="drawdown"></div>
      <div class="card wide"><h2>综合最优资金曲线</h2><img src="data:image/png;base64,{render_best_equity_chart(trades_frame, best)}" alt="equity"></div>

      <div class="card full"><h2>模式总表</h2><div class="note">这里特别把“趋势模式触发率”单列出来，方便判断到底是规则本身有效，还是几乎没触发所以看起来接近原策略。</div><div class="scroll" style="margin-top:12px;">{summary_table}</div></div>
      <div class="card wide"><h2>各币种测试段 PnL</h2><img src="data:image/png;base64,{render_coin_heatmap(coin_frame, "test_pnl_u", "各币种测试段 PnL")}" alt="coin_test"></div>
      <div class="card wide"><h2>各币种趋势触发率</h2><img src="data:image/png;base64,{render_coin_heatmap(coin_frame, "test_trend_activation_rate", "各币种测试段趋势触发率")}" alt="coin_rate"></div>
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


def format_r(value: float) -> str:
    if math.isclose(value, round(value)):
        return str(int(round(value)))
    return str(value).replace(".", "p")


if __name__ == "__main__":
    main()
