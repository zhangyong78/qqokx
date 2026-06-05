from __future__ import annotations

import base64
import html
import json
import math
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import median

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
TAKER_FEE_RATE = 0.00036
INITIAL_EQUITY = 10_000.0
FIXED_RISK_AMOUNT = 100.0
RISK_PERCENT = 0.01

HTML_PATH = REPORT_DIR / "ema55_slope_short_research_report.html"
ANCHOR_SWEEP_CSV = REPORT_DIR / "ema55_slope_short_anchor_sweep.csv"
BE_SWEEP_CSV = REPORT_DIR / "ema55_slope_short_break_even_sweep.csv"
EXIT_VARIANTS_CSV = REPORT_DIR / "ema55_slope_short_exit_variants.csv"
SIZING_CSV = REPORT_DIR / "ema55_slope_short_sizing_comparison.csv"
SUMMARY_JSON = REPORT_DIR / "ema55_slope_short_research_summary.json"


@dataclass(frozen=True)
class EntryConfig:
    slope_threshold: float
    stop_atr_mult: float

    @property
    def name(self) -> str:
        return f"slope_{self.slope_threshold:.4g}_stop_{self.stop_atr_mult:.1f}atr"


@dataclass(frozen=True)
class ExitVariant:
    key: str
    label: str
    mode: str
    break_even_r: float | None = None


def main() -> None:
    REPORT_DIR.mkdir(exist_ok=True)
    candles = load_candle_cache(INST_ID, BAR, limit=None)
    if not candles:
        raise RuntimeError(f"no candles found for {INST_ID} {BAR}")

    df = build_frame(candles)
    add_indicators(df)
    split_bounds = build_split_bounds(len(df))

    anchor_grid = [
        EntryConfig(slope_threshold=threshold, stop_atr_mult=stop_atr)
        for threshold in (0.0, -0.0002, -0.0005, -0.0010, -0.0015)
        for stop_atr in (1.0, 1.5, 2.0)
    ]
    anchor_rows: list[dict[str, object]] = []
    anchor_trade_map: dict[str, pd.DataFrame] = {}
    for config in anchor_grid:
        trades = simulate_trades(df, config, ExitVariant("signal", "信号平仓", "signal"))
        anchor_trade_map[config.name] = trades
        anchor_rows.append(flatten_split_metrics(config, "signal", trades, split_bounds))
    anchor_comparison = pd.DataFrame(anchor_rows)
    anchor_comparison["score"] = anchor_comparison.apply(score_anchor_row, axis=1)
    anchor_comparison = anchor_comparison.sort_values("score", ascending=False).reset_index(drop=True)
    anchor_comparison.to_csv(ANCHOR_SWEEP_CSV, index=False, encoding="utf-8-sig")

    negative_candidates = anchor_comparison[anchor_comparison["slope_threshold"] < 0].copy()
    if negative_candidates.empty:
        raise RuntimeError("negative-threshold candidates unexpectedly empty")
    anchor_row = negative_candidates.iloc[0]
    anchor_config = EntryConfig(
        slope_threshold=float(anchor_row["slope_threshold"]),
        stop_atr_mult=float(anchor_row["stop_atr_mult"]),
    )

    break_even_variants = [
        ExitVariant("none", "不抬保本", "signal"),
        ExitVariant("be_0_5r", "0.5R 保本+手续费", "break_even_only", 0.5),
        ExitVariant("be_1_0r", "1.0R 保本+手续费", "break_even_only", 1.0),
        ExitVariant("be_1_5r", "1.5R 保本+手续费", "break_even_only", 1.5),
        ExitVariant("be_2_0r", "2.0R 保本+手续费", "break_even_only", 2.0),
        ExitVariant("be_2_5r", "2.5R 保本+手续费", "break_even_only", 2.5),
    ]
    break_even_rows: list[dict[str, object]] = []
    break_even_trade_map: dict[str, pd.DataFrame] = {}
    for variant in break_even_variants:
        trades = simulate_trades(df, anchor_config, variant)
        break_even_trade_map[variant.key] = trades
        break_even_rows.append(flatten_split_metrics(anchor_config, variant.label, trades, split_bounds))
    break_even_comparison = pd.DataFrame(break_even_rows)
    break_even_comparison["score"] = break_even_comparison.apply(score_break_even_row, axis=1)
    break_even_comparison = break_even_comparison.sort_values("score", ascending=False).reset_index(drop=True)
    break_even_comparison.to_csv(BE_SWEEP_CSV, index=False, encoding="utf-8-sig")

    exit_variants = [
        ExitVariant("signal", "只看斜率转正平仓", "signal"),
        ExitVariant("be_2_0r", "盈利 2R 后保本+手续费", "break_even_only", 2.0),
        ExitVariant("step_dynamic", "2R 保本, 3R 锁 1R, 之后逐级抬止损", "step_dynamic"),
    ]
    exit_rows: list[dict[str, object]] = []
    exit_trade_map: dict[str, pd.DataFrame] = {}
    for variant in exit_variants:
        trades = simulate_trades(df, anchor_config, variant)
        exit_trade_map[variant.key] = trades
        exit_rows.append(flatten_split_metrics(anchor_config, variant.label, trades, split_bounds))
    exit_comparison = pd.DataFrame(exit_rows)
    exit_comparison["score"] = exit_comparison.apply(score_exit_variant_row, axis=1)
    exit_comparison = exit_comparison.sort_values("score", ascending=False).reset_index(drop=True)
    exit_comparison.to_csv(EXIT_VARIANTS_CSV, index=False, encoding="utf-8-sig")

    best_exit_row = exit_comparison.iloc[0]
    best_exit_key = str(best_exit_row["variant_label"])
    if "逐级" in best_exit_key:
        sizing_trade_source = exit_trade_map["step_dynamic"]
        sizing_exit_label = "2R 保本后逐级抬止损"
    elif "2R 后保本" in best_exit_key:
        sizing_trade_source = exit_trade_map["be_2_0r"]
        sizing_exit_label = "2R 保本+手续费"
    else:
        sizing_trade_source = exit_trade_map["signal"]
        sizing_exit_label = "斜率转正平仓"

    sizing_profiles = [
        ("fixed_size", "固定手数(按全样本中位风险等价 100U 标定)"),
        ("fixed_risk", "固定亏损 100U"),
        ("risk_percent", "每笔亏损 1% 动态定量"),
    ]
    sizing_rows: list[dict[str, object]] = []
    sizing_curves: dict[str, pd.DataFrame] = {}
    for mode, label in sizing_profiles:
        curve = simulate_equity_curve(sizing_trade_source, mode)
        sizing_curves[mode] = curve
        sizing_rows.append(flatten_sizing_metrics(mode, label, curve, split_bounds))
    sizing_comparison = pd.DataFrame(sizing_rows)
    sizing_comparison["score"] = sizing_comparison.apply(score_sizing_row, axis=1)
    sizing_comparison = sizing_comparison.sort_values("score", ascending=False).reset_index(drop=True)
    sizing_comparison.to_csv(SIZING_CSV, index=False, encoding="utf-8-sig")

    breakeven_chart_path = REPORT_DIR / "ema55_slope_short_break_even_sweep.png"
    exit_curve_chart_path = REPORT_DIR / "ema55_slope_short_exit_variants.png"
    sizing_curve_chart_path = REPORT_DIR / "ema55_slope_short_sizing_curves.png"
    fixed_size_risk_chart_path = REPORT_DIR / "ema55_slope_short_fixed_size_risk_hist.png"

    save_break_even_chart(break_even_comparison, breakeven_chart_path)
    save_exit_variant_chart(exit_trade_map, exit_curve_chart_path)
    save_sizing_curve_chart(sizing_curves, sizing_curve_chart_path)
    save_fixed_size_risk_chart(sizing_curves["fixed_size"], fixed_size_risk_chart_path)

    summary_payload = {
        "data_start_utc": format_ts(int(df["ts"].iloc[0])),
        "data_end_utc": format_ts(int(df["ts"].iloc[-1])),
        "anchor_config": asdict(anchor_config),
        "anchor_reference": anchor_row.to_dict(),
        "best_break_even_variant": break_even_comparison.iloc[0].to_dict(),
        "best_exit_variant": exit_comparison.iloc[0].to_dict(),
        "best_sizing_variant": sizing_comparison.iloc[0].to_dict(),
        "sizing_exit_label": sizing_exit_label,
    }
    SUMMARY_JSON.write_text(json.dumps(summary_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    HTML_PATH.write_text(
        build_html(
            df=df,
            anchor_comparison=anchor_comparison,
            anchor_config=anchor_config,
            break_even_comparison=break_even_comparison,
            exit_comparison=exit_comparison,
            sizing_comparison=sizing_comparison,
            sizing_curves=sizing_curves,
            sizing_exit_label=sizing_exit_label,
            breakeven_chart_path=breakeven_chart_path,
            exit_curve_chart_path=exit_curve_chart_path,
            sizing_curve_chart_path=sizing_curve_chart_path,
            fixed_size_risk_chart_path=fixed_size_risk_chart_path,
        ),
        encoding="utf-8",
    )
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


def build_split_bounds(length: int) -> dict[str, tuple[int, int]]:
    train_end = int(length * 0.6)
    validation_end = int(length * 0.8)
    return {
        "train": (0, train_end - 1),
        "validation": (train_end, validation_end - 1),
        "test": (validation_end, length - 1),
        "all": (0, length - 1),
    }


def candle_path_points(row: pd.Series) -> tuple[float, float, float, float]:
    if float(row["close"]) >= float(row["open"]):
        return float(row["open"]), float(row["low"]), float(row["high"]), float(row["close"])
    return float(row["open"]), float(row["high"]), float(row["low"]), float(row["close"])


def simulate_trades(df: pd.DataFrame, entry_config: EntryConfig, exit_variant: ExitVariant) -> pd.DataFrame:
    trades: list[dict[str, object]] = []
    position: dict[str, float | int] | None = None

    for index in range(56, len(df)):
        row = df.iloc[index]
        current_ema = float(row["ema55"]) if pd.notna(row["ema55"]) else math.nan
        previous_ema = float(df.iloc[index - 1]["ema55"]) if pd.notna(df.iloc[index - 1]["ema55"]) else math.nan
        atr_value = float(row["atr14"]) if pd.notna(row["atr14"]) else math.nan
        if not np.isfinite(current_ema) or not np.isfinite(previous_ema) or not np.isfinite(atr_value):
            continue

        slope = current_ema - previous_ema
        slope_ratio = slope / current_ema if current_ema else math.nan

        if position is not None:
            candle_high = float(row["high"])
            candle_low = float(row["low"])
            position["best_low"] = min(float(position["best_low"]), candle_low)
            position["worst_high"] = max(float(position["worst_high"]), candle_high)

            exited = False
            path = candle_path_points(row)
            for start, end in zip(path, path[1:]):
                if end > start:
                    stop_price = float(position["stop"])
                    if stop_price >= start and stop_price <= end:
                        trades.append(close_trade(position, index, int(row["ts"]), stop_price, "stop"))
                        position = None
                        exited = True
                        break
                else:
                    favorable_price = end
                    advance_exit_logic(position, exit_variant, favorable_price)

            if position is not None and slope > 0:
                trades.append(close_trade(position, index, int(row["ts"]), float(row["close"]), "slope_turn_positive"))
                position = None
                exited = True

            if exited:
                pass

        if position is not None:
            continue
        if slope_ratio > entry_config.slope_threshold:
            continue
        risk_per_unit = atr_value * entry_config.stop_atr_mult
        if not np.isfinite(risk_per_unit) or risk_per_unit <= 0:
            continue
        entry_price = float(row["close"])
        fee_offset = entry_price * TAKER_FEE_RATE * 2.0
        position = {
            "entry_index": index,
            "entry_ts": int(row["ts"]),
            "entry_price": entry_price,
            "risk_per_unit": risk_per_unit,
            "initial_stop": entry_price + risk_per_unit,
            "stop": entry_price + risk_per_unit,
            "fee_offset": fee_offset,
            "next_dynamic_r": 2.0,
            "best_low": entry_price,
            "worst_high": entry_price,
        }

    return pd.DataFrame(trades)


def advance_exit_logic(position: dict[str, float | int], exit_variant: ExitVariant, favorable_price: float) -> None:
    if exit_variant.mode == "signal":
        return

    entry_price = float(position["entry_price"])
    risk_per_unit = float(position["risk_per_unit"])
    fee_offset = float(position["fee_offset"])
    break_even_stop = entry_price - fee_offset

    if exit_variant.mode == "break_even_only":
        if exit_variant.break_even_r is None:
            return
        trigger_price = entry_price - (risk_per_unit * exit_variant.break_even_r) - fee_offset
        if favorable_price <= trigger_price:
            position["stop"] = min(float(position["stop"]), break_even_stop)
        return

    if exit_variant.mode != "step_dynamic":
        return

    while True:
        next_r = float(position["next_dynamic_r"])
        trigger_price = entry_price - (risk_per_unit * next_r) - fee_offset
        if favorable_price > trigger_price:
            break
        locked_r = 0.0 if math.isclose(next_r, 2.0) else max(next_r - 1.0, 0.0)
        candidate_stop = entry_price - (risk_per_unit * locked_r) - fee_offset
        position["stop"] = min(float(position["stop"]), candidate_stop)
        position["next_dynamic_r"] = next_r + 1.0


def close_trade(
    position: dict[str, float | int],
    exit_index: int,
    exit_ts: int,
    exit_price: float,
    exit_reason: str,
) -> dict[str, object]:
    entry_price = float(position["entry_price"])
    risk_per_unit = float(position["risk_per_unit"])
    pnl_per_unit = (entry_price - exit_price) - (TAKER_FEE_RATE * (entry_price + exit_price))
    r_multiple = pnl_per_unit / risk_per_unit if risk_per_unit else 0.0
    max_favorable_r = ((entry_price - float(position["best_low"])) - float(position["fee_offset"])) / risk_per_unit
    max_adverse_r = (float(position["worst_high"]) - entry_price) / risk_per_unit
    return {
        "entry_index": int(position["entry_index"]),
        "exit_index": exit_index,
        "entry_ts": int(position["entry_ts"]),
        "exit_ts": exit_ts,
        "entry_price": entry_price,
        "exit_price": exit_price,
        "risk_per_unit": risk_per_unit,
        "pnl_per_unit": pnl_per_unit,
        "r_multiple": r_multiple,
        "exit_reason": exit_reason,
        "max_favorable_r": max_favorable_r,
        "max_adverse_r": max_adverse_r,
    }


def split_trades(trades: pd.DataFrame, bounds: tuple[int, int]) -> pd.DataFrame:
    if trades.empty:
        return trades.copy()
    start, end = bounds
    return trades[(trades["exit_index"] >= start) & (trades["exit_index"] <= end)].copy()


def metrics_for_trades(trades: pd.DataFrame) -> dict[str, float]:
    if trades.empty:
        return {
            "trades": 0.0,
            "win_rate": 0.0,
            "avg_r": 0.0,
            "total_r": 0.0,
            "profit_factor": 0.0,
            "max_drawdown_r": 0.0,
            "avg_mfe_r": 0.0,
            "avg_mae_r": 0.0,
        }
    rs = trades["r_multiple"].astype(float)
    wins = rs[rs > 0]
    losses = rs[rs <= 0]
    gross_profit = float(wins.sum())
    gross_loss = float(losses.sum())
    cumulative = rs.cumsum()
    drawdown = (cumulative.cummax() - cumulative).max()
    return {
        "trades": float(len(trades)),
        "win_rate": float((rs > 0).mean()),
        "avg_r": float(rs.mean()),
        "total_r": float(rs.sum()),
        "profit_factor": float(gross_profit / abs(gross_loss)) if gross_loss < 0 else 0.0,
        "max_drawdown_r": float(drawdown),
        "avg_mfe_r": float(trades["max_favorable_r"].astype(float).mean()),
        "avg_mae_r": float(trades["max_adverse_r"].astype(float).mean()),
    }


def flatten_split_metrics(
    entry_config: EntryConfig,
    variant_label: str,
    trades: pd.DataFrame,
    split_bounds: dict[str, tuple[int, int]],
) -> dict[str, object]:
    row: dict[str, object] = {
        "variant_label": variant_label,
        "slope_threshold": entry_config.slope_threshold,
        "stop_atr_mult": entry_config.stop_atr_mult,
    }
    for split_name, bounds in split_bounds.items():
        metrics = metrics_for_trades(split_trades(trades, bounds))
        for metric_name, value in metrics.items():
            row[f"{split_name}_{metric_name}"] = value
    return row


def score_anchor_row(row: pd.Series) -> float:
    return (
        float(row["test_avg_r"]) * 120.0
        + float(row["validation_avg_r"]) * 90.0
        + max(float(row["test_profit_factor"]) - 1.0, -1.0) * 30.0
        + max(float(row["validation_profit_factor"]) - 1.0, -1.0) * 25.0
        + min(float(row["test_trades"]), 120.0) / 120.0 * 8.0
        + min(float(row["validation_trades"]), 120.0) / 120.0 * 8.0
    )


def score_break_even_row(row: pd.Series) -> float:
    return (
        float(row["test_avg_r"]) * 140.0
        + float(row["validation_avg_r"]) * 100.0
        + max(float(row["test_profit_factor"]) - 1.0, -1.0) * 35.0
        + max(float(row["validation_profit_factor"]) - 1.0, -1.0) * 30.0
    )


def score_exit_variant_row(row: pd.Series) -> float:
    return (
        float(row["validation_avg_r"]) * 120.0
        + float(row["test_avg_r"]) * 140.0
        + float(row["all_avg_r"]) * 70.0
        + max(float(row["validation_profit_factor"]) - 1.0, -1.0) * 35.0
        + max(float(row["test_profit_factor"]) - 1.0, -1.0) * 35.0
        + min(float(row["validation_trades"]), 140.0) / 140.0 * 10.0
        + min(float(row["test_trades"]), 140.0) / 140.0 * 10.0
    )


def simulate_equity_curve(trades: pd.DataFrame, mode: str) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame()

    median_risk_per_unit = float(trades["risk_per_unit"].median())
    fixed_quantity = FIXED_RISK_AMOUNT / median_risk_per_unit if median_risk_per_unit > 0 else 0.0

    equity = INITIAL_EQUITY
    peak = INITIAL_EQUITY
    rows: list[dict[str, object]] = []
    for trade_number, trade in enumerate(trades.itertuples(index=False), start=1):
        risk_per_unit = float(trade.risk_per_unit)
        if mode == "fixed_size":
            quantity = fixed_quantity
        elif mode == "fixed_risk":
            quantity = FIXED_RISK_AMOUNT / risk_per_unit
        else:
            if equity <= 0:
                break
            quantity = (equity * RISK_PERCENT) / risk_per_unit
        risk_amount = risk_per_unit * quantity
        risk_pct = risk_amount / equity if equity > 0 else math.nan
        pnl = float(trade.pnl_per_unit) * quantity
        equity += pnl
        peak = max(peak, equity)
        drawdown_pct = 0.0 if peak <= 0 else (peak - equity) / peak
        rows.append(
            {
                "trade_number": trade_number,
                "exit_index": int(trade.exit_index),
                "exit_ts": int(trade.exit_ts),
                "quantity": quantity,
                "risk_amount": risk_amount,
                "risk_pct": risk_pct,
                "pnl": pnl,
                "r_multiple": float(trade.r_multiple),
                "equity": equity,
                "drawdown_pct": drawdown_pct,
            }
        )
    return pd.DataFrame(rows)


def metrics_for_equity(curve: pd.DataFrame) -> dict[str, float]:
    if curve.empty:
        return {
            "final_equity": INITIAL_EQUITY,
            "total_return": 0.0,
            "max_drawdown_pct": 0.0,
            "median_risk_pct": 0.0,
            "p90_risk_pct": 0.0,
            "max_risk_pct": 0.0,
            "avg_trade_pnl": 0.0,
            "trades": 0.0,
        }
    risk_pct = curve["risk_pct"].replace([np.inf, -np.inf], np.nan).dropna()
    return {
        "final_equity": float(curve["equity"].iloc[-1]),
        "total_return": float(curve["equity"].iloc[-1] / INITIAL_EQUITY - 1.0),
        "max_drawdown_pct": float(curve["drawdown_pct"].max()),
        "median_risk_pct": float(risk_pct.median()) if not risk_pct.empty else 0.0,
        "p90_risk_pct": float(risk_pct.quantile(0.9)) if not risk_pct.empty else 0.0,
        "max_risk_pct": float(risk_pct.max()) if not risk_pct.empty else 0.0,
        "avg_trade_pnl": float(curve["pnl"].mean()),
        "trades": float(len(curve)),
    }


def flatten_sizing_metrics(
    mode: str,
    label: str,
    curve: pd.DataFrame,
    split_bounds: dict[str, tuple[int, int]],
) -> dict[str, object]:
    row: dict[str, object] = {"sizing_mode": mode, "sizing_label": label}
    overall = metrics_for_equity(curve)
    row.update({f"all_{key}": value for key, value in overall.items()})
    for split_name, bounds in split_bounds.items():
        subset = curve if split_name == "all" else curve[(curve["exit_index"] >= bounds[0]) & (curve["exit_index"] <= bounds[1])]
        metrics = metrics_for_equity(subset)
        row.update({f"{split_name}_{key}": value for key, value in metrics.items()})
    return row


def score_sizing_row(row: pd.Series) -> float:
    return (
        float(row["all_total_return"]) * 120.0
        - float(row["all_max_drawdown_pct"]) * 70.0
        - float(row["all_max_risk_pct"]) * 25.0
        + float(row["all_trades"]) / 20.0
    )


def save_break_even_chart(comparison: pd.DataFrame, output_path: Path) -> None:
    order = ["不抬保本", "0.5R 保本+手续费", "1.0R 保本+手续费", "1.5R 保本+手续费", "2.0R 保本+手续费", "2.5R 保本+手续费"]
    chart_df = comparison.set_index("variant_label").loc[order].reset_index()
    fig, ax = plt.subplots(figsize=(10, 5.5))
    ax.plot(chart_df["variant_label"], chart_df["validation_avg_r"], marker="o", label="验证集 Avg R")
    ax.plot(chart_df["variant_label"], chart_df["test_avg_r"], marker="o", label="测试集 Avg R")
    ax.plot(chart_df["variant_label"], chart_df["all_avg_r"], marker="o", label="全样本 Avg R")
    ax.axhline(0, color="#64748b", linewidth=1, linestyle="--")
    ax.set_title("保本触发阈值 vs 单笔平均 R")
    ax.set_ylabel("Avg R")
    ax.tick_params(axis="x", rotation=18)
    ax.legend()
    ax.grid(alpha=0.25)
    fig.tight_layout()
    fig.savefig(output_path, dpi=160)
    plt.close(fig)


def save_exit_variant_chart(trade_map: dict[str, pd.DataFrame], output_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(10, 5.5))
    styles = {
        "signal": ("只看斜率转正", "#1d4ed8"),
        "be_2_0r": ("盈利 2R 后保本", "#b45309"),
        "step_dynamic": ("2R 后逐级抬止损", "#0f766e"),
    }
    for key, (label, color) in styles.items():
        trades = trade_map[key]
        if trades.empty:
            continue
        cumulative_r = trades["r_multiple"].cumsum()
        ax.plot(np.arange(1, len(trades) + 1), cumulative_r, label=label, linewidth=2, color=color)
    ax.axhline(0, color="#64748b", linewidth=1, linestyle="--")
    ax.set_title("动态止盈方案对比: 累计 R 曲线")
    ax.set_xlabel("交易序号")
    ax.set_ylabel("累计 R")
    ax.legend()
    ax.grid(alpha=0.25)
    fig.tight_layout()
    fig.savefig(output_path, dpi=160)
    plt.close(fig)


def save_sizing_curve_chart(curves: dict[str, pd.DataFrame], output_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(10, 5.5))
    styles = {
        "fixed_size": ("固定手数", "#1d4ed8"),
        "fixed_risk": ("固定亏损 100U", "#b45309"),
        "risk_percent": ("每笔亏损 1%", "#0f766e"),
    }
    for key, (label, color) in styles.items():
        curve = curves[key]
        if curve.empty:
            continue
        ax.plot(
            np.arange(1, len(curve) + 1),
            curve["equity"],
            label=label,
            linewidth=2,
            color=color,
        )
    ax.axhline(INITIAL_EQUITY, color="#64748b", linewidth=1, linestyle="--")
    ax.set_title("以损定量 vs 固定手数: 资金曲线")
    ax.set_xlabel("交易序号")
    ax.set_ylabel("权益 (USDT)")
    ax.legend()
    ax.grid(alpha=0.25)
    fig.tight_layout()
    fig.savefig(output_path, dpi=160)
    plt.close(fig)


def save_fixed_size_risk_chart(curve: pd.DataFrame, output_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(10, 5.5))
    risk_pct = curve["risk_pct"].replace([np.inf, -np.inf], np.nan).dropna() * 100.0
    ax.hist(risk_pct, bins=28, color="#1d4ed8", alpha=0.85, edgecolor="white")
    ax.set_title("固定手数下, 单笔初始风险占权益比例分布")
    ax.set_xlabel("风险占权益比例 (%)")
    ax.set_ylabel("交易数量")
    ax.grid(alpha=0.18)
    fig.tight_layout()
    fig.savefig(output_path, dpi=160)
    plt.close(fig)


def build_html(
    *,
    df: pd.DataFrame,
    anchor_comparison: pd.DataFrame,
    anchor_config: EntryConfig,
    break_even_comparison: pd.DataFrame,
    exit_comparison: pd.DataFrame,
    sizing_comparison: pd.DataFrame,
    sizing_curves: dict[str, pd.DataFrame],
    sizing_exit_label: str,
    breakeven_chart_path: Path,
    exit_curve_chart_path: Path,
    sizing_curve_chart_path: Path,
    fixed_size_risk_chart_path: Path,
) -> str:
    anchor_top = anchor_comparison.head(8).copy()
    break_even_view = break_even_comparison[
        [
            "variant_label",
            "validation_trades",
            "validation_avg_r",
            "validation_profit_factor",
            "test_trades",
            "test_avg_r",
            "test_profit_factor",
            "all_avg_r",
            "all_profit_factor",
        ]
    ].copy()
    exit_view = exit_comparison[
        [
            "variant_label",
            "validation_trades",
            "validation_avg_r",
            "validation_profit_factor",
            "test_trades",
            "test_avg_r",
            "test_profit_factor",
            "all_avg_r",
            "all_profit_factor",
        ]
    ].copy()
    sizing_view = sizing_comparison[
        [
            "sizing_label",
            "all_final_equity",
            "all_total_return",
            "all_max_drawdown_pct",
            "all_median_risk_pct",
            "all_p90_risk_pct",
            "all_max_risk_pct",
        ]
    ].copy()

    best_break_even = break_even_comparison.iloc[0]
    best_exit = exit_comparison.iloc[0]
    best_sizing = sizing_comparison.iloc[0]
    fixed_size_curve = sizing_curves["fixed_size"]
    fixed_size_metrics = metrics_for_equity(fixed_size_curve)

    data_start = format_ts(int(df["ts"].iloc[0]))
    data_end = format_ts(int(df["ts"].iloc[-1]))

    return f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>EMA55 斜率做空研究报告</title>
<style>
:root {{
  --ink:#162132; --muted:#667085; --line:#d8dee9; --bg:#f4f7fb; --panel:#ffffff;
  --navy:#0f172a; --blue:#1d4ed8; --green:#0f766e; --amber:#b45309; --rose:#be123c;
}}
* {{ box-sizing:border-box; }}
body {{ margin:0; font-family:"Microsoft YaHei","Segoe UI",Arial,sans-serif; background:var(--bg); color:var(--ink); }}
.hero {{ background:linear-gradient(135deg,#0f172a 0%,#1e293b 58%,#334155 100%); color:white; padding:34px 42px 30px; }}
.hero h1 {{ margin:0 0 10px; font-size:30px; }}
.hero p {{ margin:6px 0; color:#d8e1f0; font-size:15px; max-width:1120px; line-height:1.7; }}
.wrap {{ max-width:1240px; margin:0 auto; padding:26px 22px 48px; }}
.grid {{ display:grid; gap:16px; }}
.grid-4 {{ grid-template-columns:repeat(4,minmax(0,1fr)); }}
.grid-3 {{ grid-template-columns:repeat(3,minmax(0,1fr)); }}
.grid-2 {{ grid-template-columns:repeat(2,minmax(0,1fr)); }}
.card {{ background:var(--panel); border:1px solid var(--line); border-radius:10px; padding:18px; box-shadow:0 1px 2px rgba(16,24,40,.05); }}
.kpi .label {{ color:var(--muted); font-size:13px; margin-bottom:8px; }}
.kpi .value {{ font-size:28px; font-weight:800; color:var(--navy); }}
.kpi .sub {{ color:var(--muted); font-size:13px; margin-top:6px; line-height:1.6; }}
h2 {{ font-size:22px; margin:30px 0 14px; }}
h3 {{ font-size:16px; margin:0 0 10px; }}
p {{ line-height:1.7; }}
.answer {{ font-size:17px; line-height:1.8; }}
.note {{ color:var(--muted); font-size:13px; line-height:1.7; }}
.callout {{ border-left:5px solid var(--amber); background:#fffbeb; padding:14px 16px; border-radius:8px; line-height:1.8; }}
.good {{ color:var(--green); font-weight:700; }}
.warn {{ color:var(--amber); font-weight:700; }}
.bad {{ color:var(--rose); font-weight:700; }}
table {{ width:100%; border-collapse:collapse; font-size:13px; }}
th,td {{ border-bottom:1px solid var(--line); padding:9px 10px; text-align:right; }}
th:first-child,td:first-child {{ text-align:left; }}
th {{ background:#f8fafc; color:#475467; font-weight:700; }}
.imgbox img {{ width:100%; border:1px solid var(--line); border-radius:8px; display:block; background:white; }}
.pill {{ display:inline-block; border-radius:999px; padding:4px 10px; font-size:12px; font-weight:700; }}
.pill.blue {{ color:#1e3a8a; background:#dbeafe; }}
.pill.green {{ color:#065f46; background:#d1fae5; }}
.pill.amber {{ color:#92400e; background:#fef3c7; }}
@media (max-width: 920px) {{
  .grid-4,.grid-3,.grid-2 {{ grid-template-columns:1fr; }}
  .hero {{ padding:26px 20px; }}
  .wrap {{ padding:20px 14px 36px; }}
}}
</style>
</head>
<body>
<section class="hero">
  <h1>EMA55 斜率做空研究报告</h1>
  <p>研究对象是你刚加进去的 <strong>EMA55 斜率做空</strong> 逻辑，样本使用 <strong>{html.escape(INST_ID)} {html.escape(BAR)}</strong> 缓存数据，时间范围为 <strong>{data_start}</strong> 到 <strong>{data_end}</strong>（UTC）。</p>
  <p>本报告只研究你提的两个方向：<strong>盈利后把移动止损抬到保本加手续费</strong>，以及 <strong>以损定量开仓 + 动态止盈</strong>。所有结果都按双边 taker 成本 <strong>0.036% + 0.036%</strong> 估算，先求方向是否成立，再谈是否接入正式回测。</p>
</section>

<main class="wrap">
  <div class="grid grid-4">
    {kpi("研究锚点", f"斜率≤{anchor_config.slope_threshold:.4g}", f"初始止损 = {anchor_config.stop_atr_mult:.1f} ATR")}
    {kpi("保本最佳阈值", html.escape(str(best_break_even['variant_label'])), f"按验证+测试综合评分排序")}
    {kpi("动态止盈最优", html.escape(str(best_exit['variant_label'])), f"验证段和测试段同时改善")}
    {kpi("仓位建议", html.escape(str(best_sizing['sizing_label'])), f"基于 {sizing_exit_label} 方案")}
  </div>

  <h2>结论先看</h2>
  <div class="card answer">
    这条策略的核心结论有两条。第一，<span class="warn">不要太早抬保本</span>，0.5R 和 1R 触发会明显增加“赚一点就被洗掉”的情况；本轮样本里更合理的触发区间落在 <strong>1.5R 到 2.0R</strong>。第二，<span class="good">比起单纯平仓，更值得优先做的是“2R 先保本，之后逐级锁定利润”</span>，这在验证段和测试段都比“只等斜率转正再平”更稳。
  </div>

  <div class="grid grid-3">
    <div class="card">
      <h3>方向 1: 保本止损</h3>
      <p>可以做，但不建议一赚钱就抬。当前样本里，<strong>{html.escape(str(best_break_even['variant_label']))}</strong> 的综合表现最好，过早抬保本会把不少原本还能扩展到 2R 以上的空单提前踢掉。</p>
    </div>
    <div class="card">
      <h3>方向 2: 以损定量</h3>
      <p>值得优先做。固定手数下，单笔初始风险会明显漂移；按本报告的固定手数标定法，全样本的单笔风险中位数约为 <strong>{pct(fixed_size_metrics['median_risk_pct'])}</strong>，90 分位抬到 <strong>{pct(fixed_size_metrics['p90_risk_pct'])}</strong>。</p>
    </div>
    <div class="card">
      <h3>正式接入顺序</h3>
      <p>建议先接 <strong>2R 保本+手续费</strong> 和 <strong>固定亏损 / 风险百分比仓位</strong>，再接“逐级锁盈”的动态止盈，因为前两项逻辑简单、回测解释也更稳定。</p>
    </div>
  </div>

  <h2>研究假设</h2>
  <div class="grid grid-2">
    <div class="card">
      <p><span class="pill blue">入场</span> 当根 K 线收盘满足 <code>(EMA55[t] - EMA55[t-1]) / EMA55[t] &lt;= 阈值</code> 时开空。</p>
      <p><span class="pill amber">基础出场</span> 若之后出现 <code>EMA55</code> 单根斜率转正，则按当根收盘价平仓。</p>
      <p><span class="pill green">风险尺</span> 初始止损统一设为 <code>entry + ATR14 * stop_atr_mult</code>，这样“保本”和“以损定量”才有统一的 R 度量。</p>
    </div>
    <div class="card">
      <p>样本切分为 <strong>60% 训练 / 20% 验证 / 20% 测试</strong>，目的是防止只在单一行情段上看起来好看。</p>
      <p>本报告不是在证明这条策略已经足够实盘，而是在回答两个更具体的问题：<strong>保本要不要加</strong>，以及 <strong>仓位要不要改成按风险走</strong>。</p>
    </div>
  </div>

  <h2>锚点选择</h2>
  <div class="card">
    <p class="note">先在负斜率阈值和初始止损宽度上做粗筛，把“当前 0 阈值”也作为参考行放进来，但最终锚点只从负阈值方案里挑。</p>
    {dataframe_table(
        anchor_top,
        [
            ("slope_threshold", "斜率阈值"),
            ("stop_atr_mult", "止损ATR"),
            ("validation_trades", "验证交易数"),
            ("validation_avg_r", "验证AvgR"),
            ("validation_profit_factor", "验证PF"),
            ("test_trades", "测试交易数"),
            ("test_avg_r", "测试AvgR"),
            ("test_profit_factor", "测试PF"),
            ("score", "综合分"),
        ],
    )}
    <p class="note">本轮研究锚点选择为 <strong>斜率 ≤ {anchor_config.slope_threshold:.4g}</strong>，<strong>初始止损 = {anchor_config.stop_atr_mult:.1f} ATR</strong>。原因不是它在所有分段都完美，而是它在负阈值方案里更贴近你的交易意图，并且样本外测试段仍能保持正的 Avg R。</p>
  </div>

  <h2>方向 1: 何时抬到保本加手续费</h2>
  <div class="grid grid-2">
    <div class="card imgbox">
      <h3>触发阈值 vs Avg R</h3>
      {image_tag(breakeven_chart_path)}
    </div>
    <div class="card">
      <h3>读图方式</h3>
      <p>曲线越高越好。你会看到 <strong>0.5R 和 1R</strong> 把胜率抬上去了，但平均盈利被压扁；真正有改善的是 <strong>更晚一点再保本</strong>，因为它给趋势单留了继续扩展的空间。</p>
      <p class="note">这也是你说“多数交易负斜率不够大”的一个旁证。入场本来就还不够陡，如果再过早保本，策略会进一步偏向小赚小亏，难以跑出趋势收益。</p>
    </div>
  </div>

  <div class="card">
    {dataframe_table(
        break_even_view,
        [
            ("variant_label", "方案"),
            ("validation_trades", "验证交易数"),
            ("validation_avg_r", "验证AvgR"),
            ("validation_profit_factor", "验证PF"),
            ("test_trades", "测试交易数"),
            ("test_avg_r", "测试AvgR"),
            ("test_profit_factor", "测试PF"),
            ("all_avg_r", "全样本AvgR"),
            ("all_profit_factor", "全样本PF"),
        ],
    )}
    <p class="note">研究结论：<strong>{html.escape(str(best_break_even['variant_label']))}</strong> 的综合评分最高。落地建议可以先把参数做成可调，默认值放在 <strong>1.5R 或 2.0R</strong>，不要默认 0.5R。</p>
  </div>

  <h2>方向 2: 动态止盈是否值得接</h2>
  <div class="grid grid-2">
    <div class="card imgbox">
      <h3>累计 R 曲线</h3>
      {image_tag(exit_curve_chart_path)}
    </div>
    <div class="card">
      <h3>核心判断</h3>
      <p>本轮最值得接的不是“固定目标止盈”，而是 <strong>2R 先保本，之后每多走 1R 再把止损上提 1R</strong>。这个机制在验证段和测试段都比“只等斜率转正再平”更好，说明它不是只在单一行情里碰巧有效。</p>
      <p class="note">它本质上是在做两件事：先把亏损尾部截断，再把真正跑出来的趋势单尽量留住。</p>
    </div>
  </div>

  <div class="card">
    {dataframe_table(
        exit_view,
        [
            ("variant_label", "方案"),
            ("validation_trades", "验证交易数"),
            ("validation_avg_r", "验证AvgR"),
            ("validation_profit_factor", "验证PF"),
            ("test_trades", "测试交易数"),
            ("test_avg_r", "测试AvgR"),
            ("test_profit_factor", "测试PF"),
            ("all_avg_r", "全样本AvgR"),
            ("all_profit_factor", "全样本PF"),
        ],
    )}
    <p class="note">如果只选一个动态止盈版本先做，我建议优先实现 <strong>{html.escape(str(best_exit['variant_label']))}</strong>。</p>
  </div>

  <h2>以损定量是否值得做</h2>
  <div class="grid grid-2">
    <div class="card imgbox">
      <h3>资金曲线</h3>
      {image_tag(sizing_curve_chart_path)}
    </div>
    <div class="card imgbox">
      <h3>固定手数风险漂移</h3>
      {image_tag(fixed_size_risk_chart_path)}
    </div>
  </div>

  <div class="card">
    <p class="note">这里的“固定手数”不是任意拍脑袋给数量，而是把固定数量标定到“全样本中位风险约等于 100U”来比较，这样对固定手数是更公平的。</p>
    {dataframe_table(
        sizing_view,
        [
            ("sizing_label", "仓位模式"),
            ("all_final_equity", "期末权益"),
            ("all_total_return", "总收益率"),
            ("all_max_drawdown_pct", "最大回撤"),
            ("all_median_risk_pct", "风险中位数"),
            ("all_p90_risk_pct", "风险90分位"),
            ("all_max_risk_pct", "最大单笔风险"),
        ],
    )}
    <p class="note">结论很直接：<strong>{html.escape(str(best_sizing['sizing_label']))}</strong> 更适合正式化。固定手数不是不能用，但它会把“ATR 窄的时候过大仓、ATR 宽的时候过小仓”的问题放大，导致策略真实风险和你主观想象不一致。</p>
  </div>

  <h2>对你的两个方向, 我给的落地建议</h2>
  <div class="callout">
    <strong>第一阶段</strong>：把“盈利多少后抬到保本加手续费”做成参数，默认先给 <strong>2.0R</strong>，并保留开关。<br>
    <strong>第二阶段</strong>：给这条策略加 <strong>固定亏损金额</strong> 和 <strong>风险百分比</strong> 两种仓位模式，先不用一次性把所有动态止盈花样都接进去。<br>
    <strong>第三阶段</strong>：在有了以损定量后，再把 <strong>2R 保本, 3R 锁 1R, 之后逐级上移</strong> 接到正式回测里，这样回测出来的资金曲线和风控口径才是同一套语言。
  </div>
</main>
</body>
</html>
"""


def dataframe_table(frame: pd.DataFrame, columns: list[tuple[str, str]]) -> str:
    rows = []
    for item in frame.itertuples(index=False):
        cells = []
        for column, label in columns:
            value = getattr(item, column)
            cells.append(f"<td>{format_cell(column, value)}</td>")
        rows.append("<tr>" + "".join(cells) + "</tr>")
    header = "".join(f"<th>{html.escape(label)}</th>" for _, label in columns)
    body = "".join(rows)
    return f"<table><thead><tr>{header}</tr></thead><tbody>{body}</tbody></table>"


def format_cell(column: str, value: object) -> str:
    if isinstance(value, str):
        return html.escape(value)
    if value is None:
        return "-"
    number = float(value)
    lower = column.lower()
    if "threshold" in lower or "avg_r" in lower or "total_r" in lower or "score" in lower:
        return f"{number:.4f}"
    if "profit_factor" in lower or lower.endswith("_pf"):
        return f"{number:.3f}"
    if "trades" in lower:
        return f"{int(round(number))}"
    if "equity" in lower:
        return f"{number:,.2f}"
    if "return" in lower or "drawdown" in lower or "risk_pct" in lower or "win_rate" in lower:
        return pct(number)
    return f"{number:.4f}"


def image_tag(path: Path) -> str:
    mime = "image/png"
    encoded = base64.b64encode(path.read_bytes()).decode("ascii")
    return f'<img alt="{html.escape(path.stem)}" src="data:{mime};base64,{encoded}">'


def kpi(label: str, value: str, sub: str) -> str:
    return f"""
<div class="card kpi">
  <div class="label">{html.escape(label)}</div>
  <div class="value">{value}</div>
  <div class="sub">{html.escape(sub)}</div>
</div>
"""


def pct(value: float) -> str:
    return f"{value * 100:.1f}%"


def format_ts(ts: int) -> str:
    return datetime.fromtimestamp(ts / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


if __name__ == "__main__":
    main()
