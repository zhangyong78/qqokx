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
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from okx_quant.candle_cache import load_candle_cache


plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "Noto Sans CJK SC", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False


REPORT_DIR = ROOT / "reports"
ANALYSIS_DIR = ROOT.parent / "qqokx_data" / "reports" / "analysis"
ENTRY_BAR = "1H"
SYMBOLS = ("BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP", "BNB-USDT-SWAP", "DOGE-USDT-SWAP")
COIN_LABELS = {symbol: symbol.split("-")[0] for symbol in SYMBOLS}

RISK_PER_TRADE_U = 10.0
TAKER_FEE_RATE = 0.00036
ATR_PERIOD = 14
ATR_STOP_MULTIPLIER = 2.0
ATR_PERCENTILE_LOOKBACK = 100
ATR_PERCENTILE_MAX = 0.50
EMA55_SLOPE_THRESHOLD = -0.0005
INITIAL_CAPITAL = 10_000.0


@dataclass(frozen=True)
class ReentryMode:
    key: str
    label: str
    note: str
    same_bar_block: bool = False
    cooldown_bars: int = 0
    dynamic_exit_requires_slope_reset: bool = False
    require_ema55_reclaim_cycle: bool = False


MODES = (
    ReentryMode(
        key="baseline",
        label="当前逻辑",
        note="动态止盈/保本出场后，若同根或下一根仍满足斜率做空条件，就可以立刻再次开空。",
    ),
    ReentryMode(
        key="same_bar_block",
        label="同根禁再开",
        note="本根 K 线刚刚出场，则本根不允许再次开空；下一根恢复正常。",
        same_bar_block=True,
    ),
    ReentryMode(
        key="cooldown_3",
        label="冷却 3 根",
        note="任意出场后，额外冷却 3 根 1H K 线，再允许重新开空。",
        cooldown_bars=3,
    ),
    ReentryMode(
        key="cooldown_6",
        label="冷却 6 根",
        note="任意出场后，额外冷却 6 根 1H K 线，再允许重新开空。",
        cooldown_bars=6,
    ),
    ReentryMode(
        key="dynamic_slope_reset",
        label="动态出场先斜率重置",
        note="若因保本或锁盈类动态保护出场，下一次必须先看到斜率失效，再重新跌回做空阈值下方才允许再开。",
        dynamic_exit_requires_slope_reset=True,
    ),
    ReentryMode(
        key="ema55_reclaim_rebreak",
        label="先上穿 EMA55 再跌回",
        note="出场后，价格必须先重新站上 EMA55，随后再跌回 EMA55 下方，才允许下一次开空。",
        require_ema55_reclaim_cycle=True,
    ),
)


def main() -> None:
    REPORT_DIR.mkdir(exist_ok=True)
    ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    basename = f"ema55_slope_short_reentry_modes_5coins_10u_{stamp}"
    html_path = ANALYSIS_DIR / f"{basename}.html"
    csv_path = ANALYSIS_DIR / f"{basename}.csv"
    coin_csv_path = ANALYSIS_DIR / f"{basename}_by_coin.csv"
    trades_csv_path = ANALYSIS_DIR / f"{basename}_trades.csv"
    json_path = ANALYSIS_DIR / f"{basename}.json"

    all_trades: list[dict[str, object]] = []
    coin_rows: list[dict[str, object]] = []
    data_ranges: dict[str, dict[str, object]] = {}

    for symbol in SYMBOLS:
        candles = load_candle_cache(symbol, ENTRY_BAR, limit=None)
        if not candles:
            data_ranges[symbol] = {"error": "missing candles"}
            continue
        frame = build_frame(candles)
        add_indicators(frame)
        bounds = build_split_bounds(len(frame))
        data_ranges[symbol] = {
            "entry_candles": len(frame),
            "start_utc": format_ts(int(frame["ts"].iloc[0])),
            "end_utc": format_ts(int(frame["ts"].iloc[-1])),
        }

        for mode in MODES:
            trades = simulate_trades(frame, mode)
            if trades.empty:
                trades = empty_trades_frame()
            trades["symbol"] = symbol
            trades["coin"] = COIN_LABELS[symbol]
            trades["mode_key"] = mode.key
            trades["mode_label"] = mode.label
            trades["split"] = trades["exit_index"].apply(lambda idx: split_name_for_index(int(idx), bounds))
            trades["year"] = pd.to_datetime(trades["exit_ts"], unit="ms", utc=True).dt.strftime("%Y")
            trades["mode_note"] = mode.note
            all_trades.extend(trades.to_dict("records"))
            coin_rows.append(flatten_coin_metrics(symbol=symbol, mode=mode, trades=trades))

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


def build_frame(candles: list[object]) -> pd.DataFrame:
    rows = [
        {
            "ts": int(candle.ts),
            "timestamp": pd.to_datetime(int(candle.ts), unit="ms", utc=True),
            "open": float(candle.open),
            "high": float(candle.high),
            "low": float(candle.low),
            "close": float(candle.close),
            "volume": float(candle.volume),
        }
        for candle in candles
    ]
    return pd.DataFrame(rows).sort_values("timestamp").drop_duplicates("timestamp").reset_index(drop=True)


def add_indicators(df: pd.DataFrame) -> None:
    df["ema55"] = df["close"].ewm(span=55, adjust=False, min_periods=55).mean()
    prev_close = df["close"].shift(1)
    true_range = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - prev_close).abs(),
            (df["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    df["atr14"] = true_range.rolling(ATR_PERIOD, min_periods=ATR_PERIOD).mean()
    df["atr_pct"] = rolling_percentile(df["atr14"], ATR_PERCENTILE_LOOKBACK)


def rolling_percentile(series: pd.Series, lookback: int) -> pd.Series:
    return series.rolling(lookback, min_periods=lookback).apply(lambda x: float(np.mean(x <= x[-1])), raw=True)


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
            "entry_ema55",
            "entry_slope_ratio",
            "entry_atr_pct",
        ]
    )


def simulate_trades(df: pd.DataFrame, mode: ReentryMode) -> pd.DataFrame:
    trades: list[dict[str, object]] = []
    position: dict[str, float | int | str] | None = None
    blocked_until_index = -1
    slope_reset_state: str | None = None
    reclaim_state: str | None = None
    start_index = max(ATR_PERCENTILE_LOOKBACK, 60)

    for index in range(start_index, len(df)):
        row = df.iloc[index]
        current_ema = finite(row["ema55"])
        prev_ema = finite(df.iloc[index - 1]["ema55"])
        atr_value = finite(row["atr14"])
        atr_pct = finite(row["atr_pct"])
        close_price = finite(row["close"])
        if any(math.isnan(value) for value in [current_ema, prev_ema, atr_value, atr_pct, close_price]):
            continue

        slope_ratio = (current_ema - prev_ema) / current_ema if current_ema else math.nan
        exited_this_bar = False
        last_exit_reason: str | None = None

        if position is not None:
            closed_trade = process_open_short(
                position,
                candle_open=finite(row["open"]),
                candle_high=finite(row["high"]),
                candle_low=finite(row["low"]),
                candle_close=close_price,
                candle_ts=int(row["ts"]),
                index=index,
            )
            if closed_trade is not None:
                trades.append(closed_trade)
                exited_this_bar = True
                last_exit_reason = str(closed_trade["exit_reason"])
                position = None
                if mode.cooldown_bars > 0:
                    blocked_until_index = index + mode.cooldown_bars
                if mode.dynamic_exit_requires_slope_reset and is_dynamic_protect_exit(last_exit_reason):
                    slope_reset_state = "await_above_threshold"
                if mode.require_ema55_reclaim_cycle:
                    reclaim_state = "await_reclaim_above_ema55"

        if position is not None:
            continue
        if mode.same_bar_block and exited_this_bar:
            continue
        if mode.cooldown_bars > 0 and index <= blocked_until_index:
            continue

        if mode.dynamic_exit_requires_slope_reset and slope_reset_state is not None:
            if slope_reset_state == "await_above_threshold":
                if np.isfinite(slope_ratio) and slope_ratio > EMA55_SLOPE_THRESHOLD:
                    slope_reset_state = "await_rebreak_below_threshold"
                continue
            if slope_reset_state == "await_rebreak_below_threshold" and np.isfinite(slope_ratio) and slope_ratio > EMA55_SLOPE_THRESHOLD:
                continue

        if mode.require_ema55_reclaim_cycle and reclaim_state is not None:
            if reclaim_state == "await_reclaim_above_ema55":
                if close_price >= current_ema:
                    reclaim_state = "await_rebreak_below_ema55"
                continue
            if reclaim_state == "await_rebreak_below_ema55" and close_price >= current_ema:
                continue

        if not np.isfinite(slope_ratio) or slope_ratio > EMA55_SLOPE_THRESHOLD:
            continue
        if atr_pct > ATR_PERCENTILE_MAX:
            continue
        if atr_value <= 0:
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
            "entry_ema55": current_ema,
            "entry_slope_ratio": slope_ratio,
            "entry_atr_pct": atr_pct,
        }
        slope_reset_state = None
        reclaim_state = None

    return pd.DataFrame(trades)


def process_open_short(
    position: dict[str, float | int | str],
    *,
    candle_open: float,
    candle_high: float,
    candle_low: float,
    candle_close: float,
    candle_ts: int,
    index: int,
) -> dict[str, object] | None:
    path = candle_path_points(
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
            advance_step_dynamic(position, end)
    return None


def candle_path_points(
    *,
    candle_open: float,
    candle_high: float,
    candle_low: float,
    candle_close: float,
) -> tuple[float, float, float, float]:
    if candle_close >= candle_open:
        return candle_open, candle_low, candle_high, candle_close
    return candle_open, candle_high, candle_low, candle_close


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
        "entry_ema55": float(position["entry_ema55"]),
        "entry_slope_ratio": float(position["entry_slope_ratio"]),
        "entry_atr_pct": float(position["entry_atr_pct"]),
    }


def is_dynamic_protect_exit(exit_reason: str) -> bool:
    return exit_reason == "break_even_stop" or exit_reason.startswith("locked_")


def build_split_bounds(length: int) -> dict[str, tuple[int, int]]:
    train_end = int(length * 0.6)
    validation_end = int(length * 0.8)
    return {
        "train": (0, train_end - 1),
        "validation": (train_end, validation_end - 1),
        "test": (validation_end, length - 1),
        "all": (0, length - 1),
    }


def split_name_for_index(index: int, bounds: dict[str, tuple[int, int]]) -> str:
    for name in ("train", "validation", "test"):
        start, end = bounds[name]
        if start <= index <= end:
            return name
    return "all"


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
            "return_pct_on_10k": 0.0,
        }
    pnls = trades["pnl_u"].astype(float)
    rs = trades["r_multiple"].astype(float)
    curve = pnls.cumsum()
    gross_profit = float(pnls[pnls > 0].sum())
    gross_loss = abs(float(pnls[pnls < 0].sum()))
    drawdown = float((curve.cummax() - curve).max())
    total = float(pnls.sum())
    return {
        "trades": float(len(trades)),
        "total_pnl_u": total,
        "profit_factor": gross_profit / gross_loss if gross_loss > 0 else 0.0,
        "win_rate": float((pnls > 0).mean()),
        "avg_r": float(rs.mean()),
        "avg_hold_hours": float(trades["hold_hours"].astype(float).mean()),
        "max_drawdown_u": drawdown,
        "return_pct_on_10k": total / INITIAL_CAPITAL * 100.0,
    }


def flatten_coin_metrics(*, symbol: str, mode: ReentryMode, trades: pd.DataFrame) -> dict[str, object]:
    all_metrics = compute_metrics(trades)
    test_metrics = compute_metrics(split_trades(trades, "test"))
    validation_metrics = compute_metrics(split_trades(trades, "validation"))
    return {
        "symbol": symbol,
        "coin": COIN_LABELS[symbol],
        "mode_key": mode.key,
        "mode_label": mode.label,
        "mode_note": mode.note,
        "all_trades": int(all_metrics["trades"]),
        "all_pnl_u": all_metrics["total_pnl_u"],
        "all_profit_factor": all_metrics["profit_factor"],
        "all_win_rate": all_metrics["win_rate"],
        "all_avg_r": all_metrics["avg_r"],
        "all_avg_hold_hours": all_metrics["avg_hold_hours"],
        "all_max_drawdown_u": all_metrics["max_drawdown_u"],
        "validation_pnl_u": validation_metrics["total_pnl_u"],
        "validation_profit_factor": validation_metrics["profit_factor"],
        "validation_win_rate": validation_metrics["win_rate"],
        "test_trades": int(test_metrics["trades"]),
        "test_pnl_u": test_metrics["total_pnl_u"],
        "test_profit_factor": test_metrics["profit_factor"],
        "test_win_rate": test_metrics["win_rate"],
        "test_avg_r": test_metrics["avg_r"],
        "test_avg_hold_hours": test_metrics["avg_hold_hours"],
        "test_max_drawdown_u": test_metrics["max_drawdown_u"],
    }


def build_summary_frame(trades_frame: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for mode in MODES:
        group = trades_frame[trades_frame["mode_key"] == mode.key].copy()
        all_metrics = compute_metrics(group)
        test_metrics = compute_metrics(split_trades(group, "test"))
        by_coin = []
        for coin, coin_group in group.groupby("coin"):
            coin_all = compute_metrics(coin_group)
            coin_test = compute_metrics(split_trades(coin_group, "test"))
            by_coin.append(
                {
                    "coin": coin,
                    "all_pnl_u": coin_all["total_pnl_u"],
                    "test_pnl_u": coin_test["total_pnl_u"],
                    "all_drawdown_u": coin_all["max_drawdown_u"],
                }
            )
        pnl_values = [item["test_pnl_u"] for item in by_coin]
        rows.append(
            {
                "mode_key": mode.key,
                "mode_label": mode.label,
                "mode_note": mode.note,
                "coins": len(by_coin),
                "all_trades": int(all_metrics["trades"]),
                "all_pnl_u": all_metrics["total_pnl_u"],
                "all_profit_factor": all_metrics["profit_factor"],
                "all_win_rate": all_metrics["win_rate"],
                "all_avg_r": all_metrics["avg_r"],
                "all_avg_hold_hours": all_metrics["avg_hold_hours"],
                "all_max_drawdown_u": all_metrics["max_drawdown_u"],
                "test_trades": int(test_metrics["trades"]),
                "test_pnl_u": test_metrics["total_pnl_u"],
                "test_profit_factor": test_metrics["profit_factor"],
                "test_win_rate": test_metrics["win_rate"],
                "test_avg_r": test_metrics["avg_r"],
                "test_avg_hold_hours": test_metrics["avg_hold_hours"],
                "test_max_drawdown_u": test_metrics["max_drawdown_u"],
                "test_positive_coins": sum(1 for item in pnl_values if item > 0),
                "test_negative_coins": sum(1 for item in pnl_values if item < 0),
                "test_pnl_std_u": float(np.std(pnl_values)) if pnl_values else 0.0,
                "test_pnl_median_u": float(np.median(pnl_values)) if pnl_values else 0.0,
            }
        )
    return pd.DataFrame(rows)


def score_summary_row(row: pd.Series) -> float:
    return (
        float(row["test_pnl_u"])
        - float(row["test_max_drawdown_u"]) * 0.35
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
    yearly_frame = (
        trades_frame.groupby(["mode_key", "mode_label", "year"], as_index=False)
        .agg(trades=("pnl_u", "size"), total_pnl_u=("pnl_u", "sum"))
    )
    return {
        "created_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "assumptions": {
            "entry_bar": ENTRY_BAR,
            "risk_per_trade_u": RISK_PER_TRADE_U,
            "taker_fee_rate": TAKER_FEE_RATE,
            "atr_period": ATR_PERIOD,
            "atr_stop_multiplier": ATR_STOP_MULTIPLIER,
            "atr_percentile_max": ATR_PERCENTILE_MAX,
            "slope_threshold_ratio": EMA55_SLOPE_THRESHOLD,
            "exit_model": "2R 保本后逐级锁盈，不强制斜率翻正平仓",
            "mode_set_note": "本次按 5 种再入场优化模式测试，并保留当前逻辑作为对照。",
        },
        "data_ranges": data_ranges,
        "summary_rows": summary_frame.to_dict("records"),
        "coin_rows": coin_frame.to_dict("records"),
        "exit_reason_rows": exit_reason_counts.to_dict("records"),
        "yearly_rows": yearly_frame.to_dict("records"),
    }


def render_total_pnl_chart(summary_frame: pd.DataFrame) -> str:
    plot_frame = summary_frame.sort_values("all_pnl_u", ascending=True)
    fig, ax = plt.subplots(figsize=(10.8, 5.8))
    ax.barh(plot_frame["mode_label"], plot_frame["all_pnl_u"], color="#0f766e")
    ax.set_title("全样本总盈亏对比")
    ax.set_xlabel("PnL (U)")
    fig.tight_layout()
    return figure_to_base64(fig)


def render_test_pnl_chart(summary_frame: pd.DataFrame) -> str:
    plot_frame = summary_frame.sort_values("test_pnl_u", ascending=True)
    fig, ax = plt.subplots(figsize=(10.8, 5.8))
    ax.barh(plot_frame["mode_label"], plot_frame["test_pnl_u"], color="#b45309")
    ax.set_title("测试段总盈亏对比")
    ax.set_xlabel("PnL (U)")
    fig.tight_layout()
    return figure_to_base64(fig)


def render_drawdown_chart(summary_frame: pd.DataFrame) -> str:
    plot_frame = summary_frame.sort_values("test_max_drawdown_u", ascending=True)
    fig, ax = plt.subplots(figsize=(10.8, 5.8))
    ax.barh(plot_frame["mode_label"], plot_frame["test_max_drawdown_u"], color="#64748b")
    ax.set_title("测试段最大回撤对比")
    ax.set_xlabel("Drawdown (U)")
    fig.tight_layout()
    return figure_to_base64(fig)


def render_coin_heatmap(coin_frame: pd.DataFrame, column: str, title: str) -> str:
    pivot = coin_frame.pivot(index="coin", columns="mode_label", values=column).reindex(index=[COIN_LABELS[s] for s in SYMBOLS])
    fig, ax = plt.subplots(figsize=(12, 4.6))
    image = ax.imshow(pivot.to_numpy(dtype=float), cmap="RdYlGn", aspect="auto")
    ax.set_xticks(range(len(pivot.columns)))
    ax.set_xticklabels(pivot.columns, rotation=18, ha="right")
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels(pivot.index)
    ax.set_title(title)
    for row_index in range(len(pivot.index)):
        for col_index in range(len(pivot.columns)):
            value = pivot.iloc[row_index, col_index]
            text = f"{float(value):.1f}" if pd.notna(value) else "-"
            ax.text(col_index, row_index, text, ha="center", va="center", fontsize=9, color="#111827")
    fig.colorbar(image, ax=ax, shrink=0.85)
    fig.tight_layout()
    return figure_to_base64(fig)


def render_yearly_chart(trades_frame: pd.DataFrame) -> str:
    yearly = (
        trades_frame.groupby(["year", "mode_label"], as_index=False)["pnl_u"]
        .sum()
        .pivot(index="year", columns="mode_label", values="pnl_u")
        .fillna(0)
        .sort_index()
    )
    fig, ax = plt.subplots(figsize=(11.5, 5.8))
    yearly.plot(kind="bar", ax=ax, width=0.82)
    ax.set_title("年度总盈亏对比")
    ax.set_xlabel("")
    ax.set_ylabel("PnL (U)")
    ax.axhline(0, color="#475569", linewidth=1)
    ax.grid(axis="y", alpha=0.2)
    plt.xticks(rotation=0)
    fig.tight_layout()
    return figure_to_base64(fig)


def render_best_equity_chart(trades_frame: pd.DataFrame, best_row: pd.Series) -> str:
    selected = trades_frame[trades_frame["mode_key"] == best_row["mode_key"]].copy()
    selected = selected.sort_values(["exit_ts", "entry_ts", "coin"]).reset_index(drop=True)
    selected["equity_u"] = selected["pnl_u"].astype(float).cumsum()
    selected["drawdown_u"] = selected["equity_u"].cummax() - selected["equity_u"]

    fig, axes = plt.subplots(2, 1, figsize=(11.2, 7.4), sharex=True)
    axes[0].plot(selected["equity_u"].to_numpy(), color="#0f766e", linewidth=1.8)
    axes[0].set_title(f"最佳模式资金曲线: {best_row['mode_label']}")
    axes[0].set_ylabel("PnL (U)")
    axes[1].fill_between(np.arange(len(selected)), selected["drawdown_u"].to_numpy(), color="#b45309", alpha=0.28)
    axes[1].set_title("最佳模式回撤")
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
    improve_test = float(best["test_pnl_u"]) - float(baseline["test_pnl_u"])
    improve_all = float(best["all_pnl_u"]) - float(baseline["all_pnl_u"])
    improve_dd = float(best["test_max_drawdown_u"]) - float(baseline["test_max_drawdown_u"])

    summary_chart = render_total_pnl_chart(summary_frame)
    test_chart = render_test_pnl_chart(summary_frame)
    dd_chart = render_drawdown_chart(summary_frame)
    test_heatmap = render_coin_heatmap(coin_frame, "test_pnl_u", "各币种测试段 PnL")
    all_heatmap = render_coin_heatmap(coin_frame, "all_pnl_u", "各币种全样本 PnL")
    yearly_chart = render_yearly_chart(trades_frame)
    best_equity_chart = render_best_equity_chart(trades_frame, best)

    reason_rows = (
        trades_frame.groupby(["mode_label", "exit_reason"], as_index=False)
        .size()
        .rename(columns={"size": "count"})
        .sort_values(["mode_label", "count"], ascending=[True, False])
    )

    data_lines = "".join(
        f"<li><strong>{html.escape(COIN_LABELS.get(symbol, symbol))}</strong>: "
        f"{html.escape(str(info.get('start_utc', '-')))} -> {html.escape(str(info.get('end_utc', '-')))}, "
        f"1H={html.escape(str(info.get('entry_candles', '-')))}</li>"
        for symbol, info in data_ranges.items()
    )

    summary_table = dataframe_to_html(
        summary_frame[
            [
                "mode_label",
                "all_pnl_u",
                "all_trades",
                "all_profit_factor",
                "test_pnl_u",
                "test_trades",
                "test_profit_factor",
                "test_max_drawdown_u",
                "test_positive_coins",
            ]
        ],
        float_cols={
            "all_pnl_u": 1,
            "all_profit_factor": 2,
            "test_pnl_u": 1,
            "test_profit_factor": 2,
            "test_max_drawdown_u": 1,
        },
    )
    coin_table = dataframe_to_html(
        coin_frame[
            [
                "coin",
                "mode_label",
                "all_pnl_u",
                "all_profit_factor",
                "test_pnl_u",
                "test_profit_factor",
                "test_max_drawdown_u",
            ]
        ].sort_values(["coin", "test_pnl_u"], ascending=[True, False]),
        float_cols={
            "all_pnl_u": 1,
            "all_profit_factor": 2,
            "test_pnl_u": 1,
            "test_profit_factor": 2,
            "test_max_drawdown_u": 1,
        },
    )
    reason_table = dataframe_to_html(reason_rows, float_cols={"count": 0})

    mode_cards = "".join(
        (
            '<div class="mode-card">'
            f"<h3>{html.escape(mode.label)}</h3>"
            f"<p>{html.escape(mode.note)}</p>"
            "</div>"
        )
        for mode in MODES
        if mode.key != "baseline"
    )

    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>EMA55 斜率做空再入场模式 5 币 10U 详细报告</title>
  <style>
    :root {{
      --bg: #f5f7fb;
      --panel: #ffffff;
      --ink: #172033;
      --muted: #64748b;
      --line: rgba(23,32,51,0.10);
      --accent: #0f766e;
      --accent-2: #b45309;
      --accent-3: #334155;
      --shadow: 0 18px 40px rgba(15, 23, 42, 0.08);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      color: var(--ink);
      font-family: "Microsoft YaHei", "Segoe UI", sans-serif;
      background:
        radial-gradient(circle at top left, rgba(15,118,110,0.08), transparent 28%),
        radial-gradient(circle at top right, rgba(180,83,9,0.08), transparent 24%),
        linear-gradient(180deg, #fbfcfe 0%, var(--bg) 100%);
    }}
    .wrap {{
      width: min(1220px, calc(100vw - 30px));
      margin: 0 auto;
      padding: 28px 0 56px;
    }}
    .hero {{
      border-radius: 28px;
      padding: 30px;
      color: white;
      background: linear-gradient(135deg, rgba(15,118,110,0.96), rgba(30,41,59,0.95));
      box-shadow: var(--shadow);
    }}
    .hero h1 {{
      margin: 10px 0 8px;
      font-size: 34px;
      line-height: 1.08;
    }}
    .hero p {{
      margin: 8px 0 0;
      max-width: 860px;
      line-height: 1.7;
      color: rgba(255,255,255,0.90);
    }}
    .eyebrow {{
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.16em;
      opacity: 0.82;
    }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(12, 1fr);
      gap: 18px;
      margin-top: 20px;
    }}
    .card {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 22px;
      box-shadow: var(--shadow);
      padding: 22px;
    }}
    .stat {{ grid-column: span 3; }}
    .wide {{ grid-column: span 6; }}
    .full {{ grid-column: 1 / -1; }}
    .stat .k {{ color: var(--muted); font-size: 13px; }}
    .stat .v {{ font-size: 30px; font-weight: 700; margin-top: 8px; color: var(--accent); }}
    .stat .s {{ margin-top: 8px; color: var(--muted); line-height: 1.6; font-size: 13px; }}
    h2 {{ margin: 0 0 12px; font-size: 20px; }}
    h3 {{ margin: 0 0 8px; font-size: 16px; }}
    p {{ margin: 0; line-height: 1.7; }}
    ul {{ margin: 0; padding-left: 18px; line-height: 1.8; }}
    .mode-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 14px;
    }}
    .mode-card {{
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 16px;
      background: #f8fbfd;
    }}
    img {{
      width: 100%;
      border-radius: 16px;
      border: 1px solid var(--line);
      background: white;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
    }}
    th, td {{
      text-align: left;
      padding: 10px 8px;
      border-bottom: 1px solid rgba(23,32,51,0.08);
      vertical-align: top;
      white-space: nowrap;
    }}
    th {{
      color: var(--muted);
      background: #f7fafc;
      position: sticky;
      top: 0;
    }}
    .scroll {{ overflow: auto; }}
    .note {{
      color: var(--muted);
      font-size: 13px;
      line-height: 1.7;
    }}
    @media (max-width: 960px) {{
      .stat, .wide {{ grid-column: 1 / -1; }}
      .hero h1 {{ font-size: 28px; }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <div class="eyebrow">Slope Short / Re-entry Study</div>
      <h1>EMA55 斜率做空再入场模式</h1>
      <p>本次只聚焦当前这条 <strong>EMA55 斜率做空</strong> 研究线：5 个币种、1H、固定风险金 10U、动态盈亏比出场。我们把你关心的“刚出场又开单”问题拆成 <strong>5 种再入场约束模式</strong>，再保留当前逻辑做对照。</p>
    </section>

    <section class="grid">
      <div class="card stat">
        <div class="k">测试段最优</div>
        <div class="v">{html.escape(str(best["mode_label"]))}</div>
        <div class="s">测试段 PnL {float(best["test_pnl_u"]):.1f}U</div>
      </div>
      <div class="card stat">
        <div class="k">相对当前逻辑</div>
        <div class="v">{improve_test:+.1f}U</div>
        <div class="s">测试段提升；全样本变化 {improve_all:+.1f}U</div>
      </div>
      <div class="card stat">
        <div class="k">测试段回撤变化</div>
        <div class="v">{improve_dd:+.1f}U</div>
        <div class="s">负值代表比当前逻辑更稳</div>
      </div>
      <div class="card stat">
        <div class="k">当前逻辑测试段</div>
        <div class="v">{float(baseline["test_pnl_u"]):.1f}U</div>
        <div class="s">作为全部优化模式的对照组</div>
      </div>

      <div class="card wide">
        <h2>研究口径</h2>
        <ul>
          <li>标的：{", ".join(COIN_LABELS[s] for s in SYMBOLS)}</li>
          <li>入场：EMA55 单根斜率比率 <= {EMA55_SLOPE_THRESHOLD:.4f}</li>
          <li>出场：2R 保本后逐级锁盈，未使用“斜率转正强平”</li>
          <li>风控：ATR14 * {ATR_STOP_MULTIPLIER:.1f}，ATR 百分位上限 {ATR_PERCENTILE_MAX:.2f}</li>
          <li>成本：taker {TAKER_FEE_RATE * 100:.3f}% ，固定风险 {RISK_PER_TRADE_U:.1f}U / 笔</li>
        </ul>
      </div>

      <div class="card wide">
        <h2>5 种优化模式</h2>
        <div class="mode-grid">{mode_cards}</div>
      </div>

      <div class="card full">
        <h2>数据覆盖</h2>
        <ul>{data_lines}</ul>
      </div>

      <div class="card wide">
        <h2>全样本总盈亏</h2>
        <img src="data:image/png;base64,{summary_chart}" alt="all_pnl">
      </div>
      <div class="card wide">
        <h2>测试段总盈亏</h2>
        <img src="data:image/png;base64,{test_chart}" alt="test_pnl">
      </div>
      <div class="card wide">
        <h2>测试段最大回撤</h2>
        <img src="data:image/png;base64,{dd_chart}" alt="drawdown">
      </div>
      <div class="card wide">
        <h2>最佳模式资金曲线</h2>
        <img src="data:image/png;base64,{best_equity_chart}" alt="best_equity">
      </div>

      <div class="card full">
        <h2>模式总表</h2>
        <div class="note">默认先看测试段，再对照全样本和交易数，避免只看账面冠军。</div>
        <div class="scroll" style="margin-top:12px;">{summary_table}</div>
      </div>

      <div class="card wide">
        <h2>各币种测试段热力图</h2>
        <img src="data:image/png;base64,{test_heatmap}" alt="test_heatmap">
      </div>
      <div class="card wide">
        <h2>各币种全样本热力图</h2>
        <img src="data:image/png;base64,{all_heatmap}" alt="all_heatmap">
      </div>

      <div class="card full">
        <h2>币种明细</h2>
        <div class="scroll">{coin_table}</div>
      </div>

      <div class="card wide">
        <h2>年度表现</h2>
        <img src="data:image/png;base64,{yearly_chart}" alt="yearly_chart">
      </div>
      <div class="card wide">
        <h2>出场原因分布</h2>
        <div class="scroll">{reason_table}</div>
      </div>
    </section>
  </div>
</body>
</html>"""


def dataframe_to_html(
    frame: pd.DataFrame,
    *,
    float_cols: dict[str, int] | None = None,
) -> str:
    float_cols = float_cols or {}
    headers = "".join(f"<th>{html.escape(str(col))}</th>" for col in frame.columns)
    rows = []
    for _, row in frame.iterrows():
        cells = []
        for col in frame.columns:
            value = row[col]
            if col in float_cols:
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


def finite(value: object) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return math.nan
    return out if np.isfinite(out) else math.nan


def format_ts(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


if __name__ == "__main__":
    main()
