from __future__ import annotations

import argparse
import json
import math
import shutil
from dataclasses import asdict, dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from okx_quant.app_paths import configure_data_root, data_root
from okx_quant.candle_cache import load_candle_cache


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_REPORT_DIR = ROOT / "reports" / "btc_1h_ema55_simple_backtest"
ECHARTS_VENDOR_PATH = ROOT / "third_party" / "echarts.min.js"


@dataclass(frozen=True)
class StrategyConfig:
    name: str
    signal_column: str
    signal_label: str


@dataclass(frozen=True)
class BacktestConfig:
    inst_id: str = "BTC-USDT-SWAP"
    bar: str = "1H"
    initial_capital: float = 10_000.0
    fee_rate: float = 0.0004
    slippage_rate: float = 0.0002
    risk_per_trade: float = 0.005
    max_notional_multiple: float = 3.0
    ema_period: int = 55
    atr_period: int = 14
    breakdown_atr_multiple: float = 0.2
    retest_atr_multiple: float = 0.3
    slope_lookback: int = 3
    watch_bars: int = 12
    stop_buffer_atr: float = 0.5
    breakeven_r: float = 1.0
    partial_r: float = 1.5
    partial_close_fraction: float = 0.30
    trail_start_r: float = 2.0
    trail_lookback: int = 10
    trail_atr_multiple: float = 2.0


@dataclass(frozen=True)
class RuntimeConfig:
    data_dir: Path | None
    report_dir: Path
    backtest: BacktestConfig


def parse_args(argv: list[str] | None = None) -> RuntimeConfig:
    parser = argparse.ArgumentParser(description="Run the BTC 1H EMA55 simple short backtest.")
    parser.add_argument("--inst-id", default="BTC-USDT-SWAP")
    parser.add_argument("--bar", default="1H")
    parser.add_argument("--data-dir")
    parser.add_argument("--report-dir")
    parser.add_argument("--initial-capital", type=float, default=10_000.0)
    parser.add_argument("--fee-rate", type=float, default=0.0004)
    parser.add_argument("--slippage-rate", type=float, default=0.0002)
    parser.add_argument("--risk-per-trade", type=float, default=0.005)
    parser.add_argument("--max-notional-multiple", type=float, default=3.0)
    args = parser.parse_args(argv)
    return RuntimeConfig(
        data_dir=Path(args.data_dir).expanduser().resolve() if args.data_dir else None,
        report_dir=Path(args.report_dir).expanduser().resolve() if args.report_dir else DEFAULT_REPORT_DIR,
        backtest=BacktestConfig(
            inst_id=str(args.inst_id).strip().upper(),
            bar=str(args.bar).strip(),
            initial_capital=float(args.initial_capital),
            fee_rate=float(args.fee_rate),
            slippage_rate=float(args.slippage_rate),
            risk_per_trade=float(args.risk_per_trade),
            max_notional_multiple=float(args.max_notional_multiple),
        ),
    )


def main(argv: list[str] | None = None) -> None:
    runtime = parse_args(argv)
    configure_data_root(runtime.data_dir)
    runtime.report_dir.mkdir(parents=True, exist_ok=True)

    candles = load_candle_cache(
        runtime.backtest.inst_id,
        runtime.backtest.bar,
        limit=None,
    )
    if not candles:
        raise RuntimeError(
            f"no candles found for {runtime.backtest.inst_id} {runtime.backtest.bar} under {data_root()}"
        )

    frame = candles_to_frame(candles)
    frame = add_features(frame, runtime.backtest)
    frame = add_signal_columns(frame, runtime.backtest)

    strategies = [
        StrategyConfig(
            name="pullback_failure_short",
            signal_column="pullback_signal",
            signal_label="Breakdown then pullback failure",
        ),
        StrategyConfig(
            name="direct_breakdown_short",
            signal_column="direct_signal",
            signal_label="Breakdown and short next open",
        ),
    ]

    strategy_results: list[pd.DataFrame] = []
    summary_rows: list[dict[str, object]] = []
    equity_curves: list[pd.DataFrame] = []

    for strategy in strategies:
        trades = backtest_strategy(frame, strategy, runtime.backtest)
        summary = summarize_trades(
            trades=trades,
            strategy=strategy,
            initial_capital=runtime.backtest.initial_capital,
            data_start=frame["timestamp"].iloc[0],
            data_end=frame["timestamp"].iloc[-1],
        )
        strategy_results.append(trades)
        summary_rows.append(summary)
        equity_curves.append(build_equity_curve(trades, strategy.name, runtime.backtest.initial_capital))

    summary_frame = pd.DataFrame(summary_rows)
    trades_frame = pd.concat(strategy_results, ignore_index=True) if strategy_results else pd.DataFrame()
    equity_curve = pd.concat(equity_curves, ignore_index=True) if equity_curves else pd.DataFrame()

    save_outputs(
        report_dir=runtime.report_dir,
        data_root_path=data_root(),
        runtime=runtime,
        frame=frame,
        summary_frame=summary_frame,
        trades_frame=trades_frame,
        equity_curve=equity_curve,
    )

    print(
        "Simple EMA55 short backtest complete. "
        f"data_root={data_root()} "
        f"inst_id={runtime.backtest.inst_id} "
        f"bar={runtime.backtest.bar} "
        f"report_dir={runtime.report_dir}"
    )


def candles_to_frame(candles: list[object]) -> pd.DataFrame:
    rows = [
        {
            "timestamp": pd.to_datetime(int(c.ts), unit="ms", utc=True),
            "ts": int(c.ts),
            "open": float(c.open),
            "high": float(c.high),
            "low": float(c.low),
            "close": float(c.close),
            "volume": float(c.volume),
        }
        for c in candles
    ]
    return pd.DataFrame(rows).sort_values("timestamp").drop_duplicates("timestamp").reset_index(drop=True)


def add_features(frame: pd.DataFrame, config: BacktestConfig) -> pd.DataFrame:
    out = frame.copy()
    out["ema55"] = out["close"].ewm(
        span=config.ema_period,
        adjust=False,
        min_periods=config.ema_period,
    ).mean()
    out["tr"] = true_range(out)
    out["atr14"] = out["tr"].ewm(
        alpha=1 / config.atr_period,
        adjust=False,
        min_periods=config.atr_period,
    ).mean()
    return out


def true_range(frame: pd.DataFrame) -> pd.Series:
    previous_close = frame["close"].shift(1)
    return pd.concat(
        [
            frame["high"] - frame["low"],
            (frame["high"] - previous_close).abs(),
            (frame["low"] - previous_close).abs(),
        ],
        axis=1,
    ).max(axis=1)


def add_signal_columns(frame: pd.DataFrame, config: BacktestConfig) -> pd.DataFrame:
    out = frame.copy()
    out["breakdown_signal"] = (
        (out["close"] < (out["ema55"] - (config.breakdown_atr_multiple * out["atr14"])))
        & (out["ema55"] < out["ema55"].shift(config.slope_lookback))
    )
    out["pullback_touch"] = out["high"] >= (out["ema55"] - (config.retest_atr_multiple * out["atr14"]))
    out["pullback_failure_bar"] = (
        out["pullback_touch"] & (out["close"] < out["ema55"]) & (out["close"] < out["open"])
    )
    out["direct_signal"] = out["breakdown_signal"].fillna(False)
    out["pullback_signal"] = build_pullback_signals(out, config)
    return out


def build_pullback_signals(frame: pd.DataFrame, config: BacktestConfig) -> pd.Series:
    signal = pd.Series(False, index=frame.index, dtype=bool)
    watch_break_index: int | None = None
    watch_expiry_index: int | None = None

    for idx in frame.index:
        row = frame.iloc[idx]
        if pd.isna(row["ema55"]) or pd.isna(row["atr14"]):
            watch_break_index = None
            watch_expiry_index = None
            continue

        if watch_break_index is None:
            if bool(row["breakdown_signal"]):
                watch_break_index = int(idx)
                watch_expiry_index = int(idx + config.watch_bars)
            continue

        if idx <= watch_break_index:
            continue

        if watch_expiry_index is not None and idx > watch_expiry_index:
            watch_break_index = int(idx) if bool(row["breakdown_signal"]) else None
            watch_expiry_index = int(idx + config.watch_bars) if bool(row["breakdown_signal"]) else None
            continue

        if bool(row["pullback_failure_bar"]):
            signal.iloc[idx] = True
            watch_break_index = None
            watch_expiry_index = None

    return signal


def backtest_strategy(frame: pd.DataFrame, strategy: StrategyConfig, config: BacktestConfig) -> pd.DataFrame:
    trades: list[dict[str, object]] = []
    current_equity = config.initial_capital
    signal = frame[strategy.signal_column].fillna(False).astype(bool)
    idx = 0

    while idx < len(frame) - 1:
        if not bool(signal.iloc[idx]):
            idx += 1
            continue

        trade = simulate_trade(
            frame=frame,
            signal_index=idx,
            strategy=strategy,
            config=config,
            current_equity=current_equity,
        )
        if trade is None:
            idx += 1
            continue

        current_equity = float(trade["equity_after"])
        trades.append(trade)
        idx = int(trade["exit_index"]) + 1

    return pd.DataFrame(trades)


def simulate_trade(
    frame: pd.DataFrame,
    signal_index: int,
    strategy: StrategyConfig,
    config: BacktestConfig,
    current_equity: float,
) -> dict[str, object] | None:
    entry_index = signal_index + 1
    if entry_index >= len(frame):
        return None

    signal_bar = frame.iloc[signal_index]
    if pd.isna(signal_bar["ema55"]) or pd.isna(signal_bar["atr14"]):
        return None

    entry_open = float(frame.at[entry_index, "open"])
    entry_price = entry_open * (1.0 - config.slippage_rate)
    initial_stop = max(
        float(signal_bar["high"]),
        float(signal_bar["ema55"] + (config.stop_buffer_atr * signal_bar["atr14"])),
    )
    if initial_stop <= entry_price:
        return None

    stop_distance = initial_stop - entry_price
    target_risk_amount = current_equity * config.risk_per_trade
    uncapped_size = target_risk_amount / stop_distance
    max_size = (current_equity * config.max_notional_multiple) / entry_price
    position_size = min(uncapped_size, max_size)
    if position_size <= 0:
        return None

    actual_risk_amount = stop_distance * position_size
    entry_notional = entry_price * position_size
    entry_fee = entry_notional * config.fee_rate
    realized_pnl = -entry_fee
    realized_exit_notional = 0.0
    realized_exit_fees = 0.0
    remaining_size = position_size
    breakeven_armed = False
    partial_taken = False
    trail_armed = False
    hit_1r = False
    breakeven_trigger_index: int | None = None
    partial_trigger_index: int | None = None
    trail_trigger_index: int | None = None
    active_stop = initial_stop
    exit_reason = "end_of_data"
    final_exit_index = len(frame) - 1

    for bar_index in range(entry_index, len(frame)):
        bar = frame.iloc[bar_index]
        high = float(bar["high"])
        low = float(bar["low"])
        close = float(bar["close"])

        if high >= active_stop:
            exit_fill = active_stop * (1.0 + config.slippage_rate)
            exit_fee = (exit_fill * remaining_size) * config.fee_rate
            realized_pnl += (entry_price - exit_fill) * remaining_size - exit_fee
            realized_exit_notional += exit_fill * remaining_size
            realized_exit_fees += exit_fee
            exit_reason = classify_stop_reason(active_stop, initial_stop, entry_price)
            final_exit_index = bar_index
            remaining_size = 0.0
            break

        if (not hit_1r) and low <= (entry_price - (config.breakeven_r * stop_distance)):
            hit_1r = True
            breakeven_armed = True
            breakeven_trigger_index = bar_index
            active_stop = min(active_stop, entry_price)

        if (not partial_taken) and low <= (entry_price - (config.partial_r * stop_distance)):
            partial_size = position_size * config.partial_close_fraction
            partial_fill = (entry_price - (config.partial_r * stop_distance)) * (1.0 + config.slippage_rate)
            partial_fee = (partial_fill * partial_size) * config.fee_rate
            realized_pnl += (entry_price - partial_fill) * partial_size - partial_fee
            realized_exit_notional += partial_fill * partial_size
            realized_exit_fees += partial_fee
            remaining_size -= partial_size
            partial_taken = True
            partial_trigger_index = bar_index

        if low <= (entry_price - (config.trail_start_r * stop_distance)):
            trail_armed = True
            if trail_trigger_index is None:
                trail_trigger_index = bar_index

        if trail_armed:
            window_start = max(entry_index, bar_index - config.trail_lookback + 1)
            trailing_low = float(frame.iloc[window_start : bar_index + 1]["low"].min())
            trailing_level = trailing_low + (config.trail_atr_multiple * float(bar["atr14"]))
            if close > trailing_level:
                exit_fill = close * (1.0 + config.slippage_rate)
                exit_fee = (exit_fill * remaining_size) * config.fee_rate
                realized_pnl += (entry_price - exit_fill) * remaining_size - exit_fee
                realized_exit_notional += exit_fill * remaining_size
                realized_exit_fees += exit_fee
                exit_reason = "dynamic_trailing_exit"
                final_exit_index = bar_index
                remaining_size = 0.0
                break

        if close > float(bar["ema55"]):
            exit_fill = close * (1.0 + config.slippage_rate)
            exit_fee = (exit_fill * remaining_size) * config.fee_rate
            realized_pnl += (entry_price - exit_fill) * remaining_size - exit_fee
            realized_exit_notional += exit_fill * remaining_size
            realized_exit_fees += exit_fee
            exit_reason = "close_above_ema55"
            final_exit_index = bar_index
            remaining_size = 0.0
            break

    if remaining_size > 0:
        exit_fill = float(frame.at[len(frame) - 1, "close"]) * (1.0 + config.slippage_rate)
        exit_fee = (exit_fill * remaining_size) * config.fee_rate
        realized_pnl += (entry_price - exit_fill) * remaining_size - exit_fee
        realized_exit_notional += exit_fill * remaining_size
        realized_exit_fees += exit_fee
        final_exit_index = len(frame) - 1
        remaining_size = 0.0

    weighted_exit_price = realized_exit_notional / position_size if position_size else math.nan
    pnl_pct = realized_pnl / current_equity if current_equity else 0.0
    equity_after = current_equity + realized_pnl

    return {
        "strategy_name": strategy.name,
        "strategy_label": strategy.signal_label,
        "signal_index": signal_index,
        "entry_index": entry_index,
        "exit_index": final_exit_index,
        "signal_time": frame.at[signal_index, "timestamp"],
        "entry_time": frame.at[entry_index, "timestamp"],
        "exit_time": frame.at[final_exit_index, "timestamp"],
        "entry_price": entry_price,
        "exit_price": weighted_exit_price,
        "initial_stop_price": initial_stop,
        "position_size": position_size,
        "entry_notional": entry_notional,
        "entry_fee": entry_fee,
        "exit_fees": realized_exit_fees,
        "actual_risk_amount": actual_risk_amount,
        "target_risk_amount": target_risk_amount,
        "hit_1r": hit_1r,
        "partial_take_profit": partial_taken,
        "trail_armed": trail_armed,
        "breakeven_armed": breakeven_armed,
        "breakeven_trigger_index": breakeven_trigger_index,
        "partial_trigger_index": partial_trigger_index,
        "trail_trigger_index": trail_trigger_index,
        "exit_reason": exit_reason,
        "pnl_amount": realized_pnl,
        "pnl_pct": pnl_pct,
        "r_multiple": realized_pnl / actual_risk_amount if actual_risk_amount else 0.0,
        "bars_held": final_exit_index - entry_index + 1,
        "equity_before": current_equity,
        "equity_after": equity_after,
    }


def classify_stop_reason(active_stop: float, initial_stop: float, entry_price: float) -> str:
    if math.isclose(active_stop, entry_price, rel_tol=1e-9, abs_tol=1e-9):
        return "breakeven_stop"
    if math.isclose(active_stop, initial_stop, rel_tol=1e-9, abs_tol=1e-9):
        return "stop_loss"
    return "managed_stop"


def summarize_trades(
    *,
    trades: pd.DataFrame,
    strategy: StrategyConfig,
    initial_capital: float,
    data_start: pd.Timestamp,
    data_end: pd.Timestamp,
) -> dict[str, object]:
    if trades.empty:
        return {
            "strategy_name": strategy.name,
            "strategy_label": strategy.signal_label,
            "trade_count": 0,
            "win_rate": 0.0,
            "total_return": 0.0,
            "annual_return": 0.0,
            "max_drawdown": 0.0,
            "payoff_ratio": 0.0,
            "average_win": 0.0,
            "average_loss": 0.0,
            "profit_factor": 0.0,
            "max_consecutive_losses": 0,
            "final_equity": initial_capital,
            "average_r": 0.0,
        }

    pnl = trades["pnl_amount"].astype(float)
    winners = pnl[pnl > 0]
    losers = pnl[pnl < 0]
    final_equity = float(trades["equity_after"].iloc[-1])
    total_return = (final_equity / initial_capital) - 1.0 if initial_capital else 0.0
    elapsed_days = max(
        (pd.to_datetime(data_end, utc=True) - pd.to_datetime(data_start, utc=True)).total_seconds() / 86_400.0,
        1.0,
    )
    annual_return = (final_equity / initial_capital) ** (365.0 / elapsed_days) - 1.0 if initial_capital else 0.0
    equity_curve = build_equity_curve(trades, strategy.name, initial_capital)
    max_drawdown = calculate_max_drawdown(equity_curve["equity"])
    gross_profit = float(winners.sum()) if not winners.empty else 0.0
    gross_loss = abs(float(losers.sum())) if not losers.empty else 0.0

    return {
        "strategy_name": strategy.name,
        "strategy_label": strategy.signal_label,
        "trade_count": int(len(trades)),
        "win_rate": float((pnl > 0).mean()),
        "total_return": float(total_return),
        "annual_return": float(annual_return),
        "max_drawdown": float(max_drawdown),
        "payoff_ratio": float(winners.mean() / abs(losers.mean())) if not winners.empty and not losers.empty else 0.0,
        "average_win": float(winners.mean()) if not winners.empty else 0.0,
        "average_loss": float(losers.mean()) if not losers.empty else 0.0,
        "profit_factor": float(gross_profit / gross_loss) if gross_loss > 0 else 0.0,
        "max_consecutive_losses": int(max_consecutive_losses(pnl.to_numpy(dtype=float))),
        "final_equity": float(final_equity),
        "average_r": float(trades["r_multiple"].mean()),
    }


def build_equity_curve(trades: pd.DataFrame, strategy_name: str, initial_capital: float) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame(
            {
                "strategy_name": [strategy_name],
                "timestamp": [pd.NaT],
                "equity": [initial_capital],
            }
        )

    rows = [{"strategy_name": strategy_name, "timestamp": trades["entry_time"].iloc[0], "equity": initial_capital}]
    for _, trade in trades.iterrows():
        rows.append(
            {
                "strategy_name": strategy_name,
                "timestamp": trade["exit_time"],
                "equity": float(trade["equity_after"]),
            }
        )
    return pd.DataFrame(rows)


def calculate_max_drawdown(equity: pd.Series) -> float:
    if equity.empty:
        return 0.0
    peaks = equity.cummax()
    drawdown = (equity / peaks) - 1.0
    return float(drawdown.min())


def max_consecutive_losses(pnl: np.ndarray) -> int:
    best = 0
    current = 0
    for value in pnl:
        if value <= 0:
            current += 1
            best = max(best, current)
        else:
            current = 0
    return best


def save_outputs(
    *,
    report_dir: Path,
    data_root_path: Path,
    runtime: RuntimeConfig,
    frame: pd.DataFrame,
    summary_frame: pd.DataFrame,
    trades_frame: pd.DataFrame,
    equity_curve: pd.DataFrame,
) -> None:
    summary_frame.to_csv(report_dir / "strategy_summary.csv", index=False, encoding="utf-8-sig")
    trades_frame.to_csv(report_dir / "trade_details.csv", index=False, encoding="utf-8-sig")
    equity_curve.to_csv(report_dir / "equity_curve.csv", index=False, encoding="utf-8-sig")
    (report_dir / "params.json").write_text(
        json.dumps(asdict(runtime.backtest), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    runtime_lines = [
        "# Runtime Context",
        "",
        f"- data_root: `{data_root_path}`",
        f"- inst_id: `{runtime.backtest.inst_id}`",
        f"- bar: `{runtime.backtest.bar}`",
        f"- report_dir: `{report_dir}`",
        f"- data_start: `{frame['timestamp'].iloc[0]}`",
        f"- data_end: `{frame['timestamp'].iloc[-1]}`",
        f"- bar_count: `{len(frame)}`",
        "",
        "## Assumptions",
        "",
        "- The local OKX `BTC-USDT-SWAP` 1H cache is used as the available BTC proxy for this brief.",
        "- Pullback observation starts after a confirmed breakdown bar and looks at the next 12 bars.",
        "- Close-driven exits (`dynamic_trailing_exit` and `close_above_ema55`) execute on the same bar close with exit slippage.",
        "- If a bar could both stop out and hit a profit condition, the stop is processed first.",
    ]
    (report_dir / "research_runtime_context.md").write_text("\n".join(runtime_lines), encoding="utf-8")

    markdown_table = summary_frame[
        [
            "strategy_name",
            "trade_count",
            "win_rate",
            "total_return",
            "annual_return",
            "max_drawdown",
            "payoff_ratio",
            "average_win",
            "average_loss",
            "profit_factor",
            "max_consecutive_losses",
            "final_equity",
            "average_r",
        ]
    ].copy()
    for column in ["win_rate", "total_return", "annual_return", "max_drawdown"]:
        markdown_table[column] = markdown_table[column].map(lambda value: f"{float(value):.2%}")
    for column in ["payoff_ratio", "average_win", "average_loss", "profit_factor", "final_equity", "average_r"]:
        markdown_table[column] = markdown_table[column].map(lambda value: f"{float(value):.4f}")

    strategy_summary = [
        "# BTC 1H EMA55 Simple Backtest",
        "",
        "## Result Table",
        "",
        dataframe_to_markdown(markdown_table),
        "",
        "## Conclusion",
        "",
        build_conclusion(summary_frame),
    ]
    (report_dir / "strategy_summary.md").write_text("\n".join(strategy_summary), encoding="utf-8")

    save_equity_plot(equity_curve, report_dir / "equity_curve.png")
    save_review_html(
        report_dir=report_dir,
        frame=frame,
        summary_frame=summary_frame,
        trades_frame=trades_frame,
        runtime=runtime,
    )


def build_conclusion(summary_frame: pd.DataFrame) -> str:
    if summary_frame.empty:
        return "No trades were produced, so there is no edge to evaluate."

    by_name = summary_frame.set_index("strategy_name")
    main_row = by_name.loc["pullback_failure_short"]
    base_row = by_name.loc["direct_breakdown_short"]

    if (
        float(main_row["profit_factor"]) > float(base_row["profit_factor"])
        and float(main_row["max_drawdown"]) >= float(base_row["max_drawdown"])
        and float(main_row["total_return"]) > 0
    ):
        return (
            "The pullback-failure entry looks worth continuing because it improved trade quality over the direct "
            "breakdown baseline while staying profitable."
        )

    if float(main_row["profit_factor"]) > 1.0 and float(main_row["total_return"]) > 0:
        return (
            "The pullback-failure entry is viable but mixed. It is profitable on this sample, yet the advantage "
            "over direct breakdown entries is not decisive enough to call it clearly superior."
        )

    return (
        "The pullback-failure entry does not look strong enough in its current form. It is better treated as an "
        "observation branch than a production-ready setup."
    )


def save_equity_plot(equity_curve: pd.DataFrame, target: Path) -> None:
    import matplotlib.pyplot as plt

    plt.figure(figsize=(12, 5))
    for strategy_name, group in equity_curve.groupby("strategy_name", dropna=False):
        clean = group.dropna(subset=["timestamp"]).copy()
        if clean.empty:
            continue
        plt.plot(
            pd.to_datetime(clean["timestamp"]),
            clean["equity"],
            linewidth=2.0,
            label=str(strategy_name),
        )
    plt.title("BTC 1H EMA55 Simple Backtest Equity Curve")
    plt.xlabel("Time")
    plt.ylabel("Equity")
    plt.grid(alpha=0.2)
    plt.legend(loc="best")
    plt.tight_layout()
    plt.savefig(target, dpi=160)
    plt.close()


def dataframe_to_markdown(frame: pd.DataFrame) -> str:
    if frame.empty:
        return "No rows."
    headers = [str(column) for column in frame.columns]
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for row in frame.itertuples(index=False, name=None):
        lines.append("| " + " | ".join(str(value) for value in row) + " |")
    return "\n".join(lines)


def sanitize_review_html(html: str) -> str:
    doctype = "<!doctype html>"
    start = html.rfind(doctype)
    if start >= 0:
        html = html[start:]
    else:
        html_start = html.rfind('<html lang="zh-CN">')
        if html_start >= 0:
            html = doctype + "\n" + html[html_start:]

    end = html.rfind("</html>")
    if end >= 0:
        html = html[: end + len("</html>")]

    replacements = {
        "<style>\n  <style>": "<style>",
        "Landscape workspace 路 chart first": "Landscape workspace / chart first",
        "Landscape workspace 閻?chart first": "Landscape workspace / chart first",
        "BTC 1H EMA55 缂傚倸鍊烽悞锔剧矙閹烘鍋嬫繝濠傜墕濮规煡鏌熼悧鍫熺凡鐎瑰憡绻冩穱濠囶敍濞嗘帩鍔呭┑鈩冨絻椤兘寮?": "BTC 1H EMA55 Trade Review",
    }
    for old, new in replacements.items():
        html = html.replace(old, new)

    return html


def save_review_html(
    *,
    report_dir: Path,
    frame: pd.DataFrame,
    summary_frame: pd.DataFrame,
    trades_frame: pd.DataFrame,
    runtime: RuntimeConfig,
) -> None:
    candle_payload = [
        {
            "i": int(idx),
            "t": pd.to_datetime(row["timestamp"], utc=True).isoformat(),
            "o": round(float(row["open"]), 4),
            "h": round(float(row["high"]), 4),
            "l": round(float(row["low"]), 4),
            "c": round(float(row["close"]), 4),
            "e": None if pd.isna(row["ema55"]) else round(float(row["ema55"]), 4),
            "a": None if pd.isna(row["atr14"]) else round(float(row["atr14"]), 4),
        }
        for idx, row in frame.iterrows()
    ]
    trade_columns = [
        "strategy_name",
        "strategy_label",
        "signal_index",
        "entry_index",
        "exit_index",
        "signal_time",
        "entry_time",
        "exit_time",
        "entry_price",
        "exit_price",
        "initial_stop_price",
        "position_size",
        "actual_risk_amount",
        "hit_1r",
        "partial_take_profit",
        "trail_armed",
        "breakeven_armed",
        "breakeven_trigger_index",
        "partial_trigger_index",
        "trail_trigger_index",
        "exit_reason",
        "pnl_amount",
        "pnl_pct",
        "r_multiple",
        "bars_held",
        "equity_before",
        "equity_after",
    ]
    trade_payload: list[dict[str, object]] = []
    if not trades_frame.empty:
        for _, row in trades_frame[trade_columns].iterrows():
            trade_payload.append(
                {
                    "strategy_name": str(row["strategy_name"]),
                    "strategy_label": str(row["strategy_label"]),
                    "signal_index": int(row["signal_index"]),
                    "entry_index": int(row["entry_index"]),
                    "exit_index": int(row["exit_index"]),
                    "signal_time": pd.to_datetime(row["signal_time"], utc=True).isoformat(),
                    "entry_time": pd.to_datetime(row["entry_time"], utc=True).isoformat(),
                    "exit_time": pd.to_datetime(row["exit_time"], utc=True).isoformat(),
                    "entry_price": round(float(row["entry_price"]), 4),
                    "exit_price": round(float(row["exit_price"]), 4),
                    "initial_stop_price": round(float(row["initial_stop_price"]), 4),
                    "position_size": round(float(row["position_size"]), 6),
                    "actual_risk_amount": round(float(row["actual_risk_amount"]), 4),
                    "hit_1r": bool(row["hit_1r"]),
                    "partial_take_profit": bool(row["partial_take_profit"]),
                    "trail_armed": bool(row["trail_armed"]),
                    "breakeven_armed": bool(row["breakeven_armed"]),
                    "breakeven_trigger_index": None if pd.isna(row["breakeven_trigger_index"]) else int(row["breakeven_trigger_index"]),
                    "partial_trigger_index": None if pd.isna(row["partial_trigger_index"]) else int(row["partial_trigger_index"]),
                    "trail_trigger_index": None if pd.isna(row["trail_trigger_index"]) else int(row["trail_trigger_index"]),
                    "exit_reason": str(row["exit_reason"]),
                    "pnl_amount": round(float(row["pnl_amount"]), 4),
                    "pnl_pct": float(row["pnl_pct"]),
                    "r_multiple": round(float(row["r_multiple"]), 4),
                    "bars_held": int(row["bars_held"]),
                    "equity_before": round(float(row["equity_before"]), 4),
                    "equity_after": round(float(row["equity_after"]), 4),
                }
            )
    summary_payload = summary_frame.to_dict(orient="records")
    conditions_html = build_conditions_html(runtime.backtest)
    runtime_note = (
        f"data range: {frame['timestamp'].iloc[0]} to {frame['timestamp'].iloc[-1]}; "
        f"{len(frame)} {runtime.backtest.bar} candles; "
        f"fee {runtime.backtest.fee_rate:.2%} one-way; "
        f"slippage {runtime.backtest.slippage_rate:.2%} one-way."
    )
    html = f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <script src="./echarts.min.js"></script>
  <title>BTC 1H EMA55 缂傚倸鍊烽悞锔剧矙閹烘鍋嬫繝濠傜墕濮规煡鏌熼悧鍫熺凡鐎瑰憡绻冩穱濠囶敍濞嗘帩鍔呭┑鈩冨絻椤兘寮?/title>
  <script src="https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js"></script>
  <style>
    :root {{
      --bg: #f4f1ea;
      --panel: #fffdf8;
      --line: #ddd6c7;
      --ink: #1f2937;
      --muted: #6b7280;
      --accent: #a34a28;
      --green: #0f8b6d;
      --red: #b42318;
      --gold: #b98900;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Microsoft YaHei UI", "PingFang SC", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top right, rgba(163, 74, 40, 0.08), transparent 28%),
        linear-gradient(180deg, #f8f4eb 0%, var(--bg) 100%);
    }}
    .page {{
      width: min(1500px, calc(100vw - 32px));
      margin: 24px auto 40px;
    }}
    .hero, .panel {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 18px;
      box-shadow: 0 10px 30px rgba(15, 23, 42, 0.06);
    }}
    .hero {{
      padding: 24px 28px;
      margin-bottom: 18px;
    }}
    h1 {{
      margin: 0 0 10px;
      font-size: 28px;
      letter-spacing: 0.02em;
    }}
    .subtle {{
      color: var(--muted);
      line-height: 1.6;
    }}
    .grid {{
      display: grid;
      gap: 16px;
    }}
    .summary-grid {{
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      margin-top: 18px;
    }}
    .card {{
      padding: 16px;
      border: 1px solid var(--line);
      border-radius: 14px;
      background: rgba(255, 255, 255, 0.8);
    }}
    .label {{
      font-size: 12px;
      text-transform: uppercase;
      color: var(--muted);
      letter-spacing: 0.08em;
    }}
    .value {{
      margin-top: 6px;
      font-size: 24px;
      font-weight: 700;
    }}
    .layout {{
      display: grid;
      grid-template-columns: minmax(0, 1fr);
      gap: 18px;
    }}
    .panel {{
      padding: 18px;
    }}
    .panel h2 {{
      margin: 0 0 12px;
      font-size: 18px;
    }}
    .section + .section {{
      margin-top: 16px;
      padding-top: 16px;
      border-top: 1px solid var(--line);
    }}
    .control {{
      display: grid;
      gap: 6px;
      margin-bottom: 0;
    }}
    select, input {{
      width: 100%;
      border: 1px solid #cbbfa8;
      border-radius: 10px;
      background: #fff;
      color: var(--ink);
      padding: 10px 12px;
      font-size: 14px;
    }}
    .two-col {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 10px;
    }}
    .stats-table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
    }}
    .stats-table td {{
      padding: 8px 0;
      border-bottom: 1px dashed var(--line);
      vertical-align: top;
    }}
    .stats-table td:first-child {{
      color: var(--muted);
      width: 42%;
      padding-right: 10px;
    }}
    .badge-row {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-top: 12px;
    }}
    .badge {{
      padding: 6px 10px;
      border-radius: 999px;
      font-size: 12px;
      border: 1px solid transparent;
      background: #f4efe4;
    }}
    .badge.green {{ color: var(--green); border-color: rgba(15, 139, 109, 0.18); background: rgba(15, 139, 109, 0.08); }}
    .badge.red {{ color: var(--red); border-color: rgba(180, 35, 24, 0.18); background: rgba(180, 35, 24, 0.08); }}
    .badge.gold {{ color: var(--gold); border-color: rgba(185, 137, 0, 0.22); background: rgba(185, 137, 0, 0.10); }}
    .conditions {{
      font-size: 14px;
      line-height: 1.75;
    }}
    .conditions h3 {{
      margin: 14px 0 6px;
      font-size: 15px;
    }}
    .conditions ul {{
      margin: 0;
      padding-left: 18px;
    }}
    #chart {{
      width: 100%;
      height: calc(100vh - 280px);
      min-height: 760px;
    }}
    .footer-note {{
      margin-top: 14px;
      color: var(--muted);
      font-size: 13px;
      line-height: 1.6;
    }}
    .toolbar {{
      display: grid;
      grid-template-columns: 180px 280px 1fr 1fr;
      gap: 12px;
      align-items: end;
      margin-bottom: 16px;
    }}
    .info-grid {{
      display: grid;
      grid-template-columns: minmax(300px, 420px) minmax(0, 1fr);
      gap: 16px;
      margin-top: 16px;
    }}
    @media (max-width: 1120px) {{
      .toolbar {{
        grid-template-columns: 1fr 1fr;
      }}
      .info-grid {{
        grid-template-columns: 1fr;
      }}
      #chart {{
        height: 620px;
      }}
    }}
    @media (max-width: 720px) {{
      .toolbar {{
        grid-template-columns: 1fr;
      }}
    }}
  </style>
</head>
<body>
  <div class="page">
    <section class="hero">
      <h1>BTC 1H EMA55 缂傚倸鍊烽悞锔剧矙閹烘鍋嬫繝濠傜墕濮规煡鏌熼悧鍫熺凡鐎瑰憡绻冩穱濠囶敍濞嗘帩鍔呭┑鈩冨絻椤兘寮?/h1>
      <div class="subtle">{runtime_note}</div>
      <div id="summary-cards" class="grid summary-grid"></div>
    </section>

    <div class="layout">
      <section class="panel">
        <h2>K缂傚倸鍊烽悞锕傚磿閹惰姤鍊舵繝闈涙灩?/h2>
        <div class="toolbar">
          <div class="control">
            <label for="view-mode">闂傚倷鐒﹂幃鍫曞磿閹绘帞鏆︽慨妞诲亾濠碘剝鎸冲畷姗€濡告惔锝呮灈闁搞劍鍎抽悾鐑藉炊閳哄﹥鏁?/label>
            <select id="view-mode">
              <option value="continuous" selected>闂備礁鎼ˇ顐﹀疾濠靛纾婚柣鏂垮悑閸嬨劌鈹戦悩宕囶暡闁搞倕瀚伴幃姗€鎮欓棃娑楀闂?/option>
              <option value="focus">婵犵數鍋涢悺銊╁吹鎼淬劌纾归柡宥庣仜閿濆憘鏃堝川椤斿吋鐣繝娈垮枟閿氱€规洦鍓氭穱?/option>
            </select>
          </div>
          <div class="control">
            <label for="strategy-select">缂傚倸鍊烽悞锔剧矙閹烘鍋嬫繝濠傜墕濮?/label>
            <select id="strategy-select"></select>
          </div>
          <div class="control">
            <label for="trade-select">婵犵數鍋涢悺銊╁吹鎼淬劌纾归柡宥庣仜?/label>
            <select id="trade-select"></select>
          </div>
          <div class="two-col">
            <div class="control">
              <label for="before-bars">闂傚倷绀侀幉锟犲箰閸濄儳鐭撻柛顐ｆ礃閸嬵亪鏌涘鍐ㄢ挃缂?/label>
              <input id="before-bars" type="number" min="20" max="500" step="10" value="80">
            </div>
            <div class="control">
              <label for="after-bars">闂傚倷绀侀幉锟犳嚌閹灐褰掓倻閼恒儱浠鹃梺鍛婄矊婢х晫妲?/label>
              <input id="after-bars" type="number" min="20" max="500" step="10" value="80">
            </div>
        </div>
        <div id="chart"></div>
        <div class="footer-note">
          闂備礁鎼ˇ顐﹀疾濠靛纾婚柣鏂垮悑閸嬨劌鈹戦悩宕囶暡闁搞倕瀚伴幃姗€鎮欓棃娑楀闂佺粯绻嶉崹杈ㄧ┍婵犲浂鏁嶆俊鐐额嚙娴滃墽鈧娲栧ú銊┿€侀崨顔剧闁哄鍨甸幃鎴犵棯閺夎法孝閾伙絽顭跨捄铏圭伇闁活厼妫濋弻锝夊箣閻愬棙鍨甸埢鎾剁磼濡偐顔曢梺鍓插亖閸庢彃鈽夎閳ь剙婀遍…鍫モ€﹀畡鎵殾闁挎繂顦伴崕鎾绘煙濞堝灝鏋ゆ俊顐灦閺屾盯寮介悽鐢碉紵缂備讲鍋撳璺侯儛濞堜粙鏌ｉ幇顖氱厫缁绢厼澧介幉鎼佹偋閸噥妫冮悗瑙勬礃缁诲牓骞冮埡鍛殤妞ゆ帒鍊愰鍫熲拺闁告稑锕ョ亸顓犵磼婢跺﹦鍩ｉ柟顔芥そ婵＄兘鍩￠崒姘偅闂備線娼ч敍蹇涘礋椤撶喐顔忛梻鍌欒兌椤牏鎹㈤崼銉ョ闁硅揪绠戠壕鍧楁煛婢跺鐒鹃柛銈嗘礋閺屽秷顧侀柛鎾存皑缁?濠德板€楁慨鐑藉磻濞戙垹鍌ㄩ柡宥庡亜閸ㄦ繈鏌曟繛鐐珕闁稿﹦鍏橀弻锝堫槻闁硅姤绮撻悰顕€濮€閻樺棛鎳撻埞鍐垂椤旂懓浜鹃柡宥庣仜閿濆憘鏃堝川椤斿吋鐣繝娈垮枟閿氱€规洦鍓氭穱濠囨嚍閵夈倗绠氬銈嗙墬閵囨盯宕戦幘宕囨殾闁搞儮鏅╅弨銊モ攽閻愬瓨缍戦柛姘儔閹虫繃銈ｉ崘銊ョ€棅顐㈡处缁嬫垿鎮欐繝鍥ㄧ厽闁归偊鍘肩徊鑽ょ磼閳ь剛鈧綆鍓涚壕鍏间繆閵堝嫮鍔嶉柣鎾卞劦閺岋綁顢斿鍛拫闂佸搫鑻悧鎾翠繆濮濆矈妲鹃悗瑙勬礀閻栧ジ寮诲☉妯锋瀻婵炲棙鍨归妶鐑芥⒑?          婵犵數鍋為崹鍫曞箰閸洖纾规俊銈呮嫅缂嶆牠鏌熼悜妯烘闁绘梻鍘ч悘宕団偓瑙勬礀濞层劑銆侀崨瀛樷拻濞撴艾娲ら弸鐔兼煕閵婏妇绠樻俊鍙夊姍閹囧醇椤愶絿鏆繝纰樻閸ㄦ娊骞婇幘缁樺剨闁靛ě鍕瀾闂佺粯顨呴悧濠勪焊椤撶儐鐔嗙憸蹇旀叏閻㈢數鐭夌€广儱顦洿闂佺硶鍓濋敃鈺侇焽閵娾晜鈷戦柛娑橈功婢ь剚淇婇悙鑸殿棄閾荤偤鏌涜椤ㄥ懓绻氶梻浣侯焾閺堫剟宕欓悷鎷旓絾绻濆顓犲幗闂佸啿鎼导鍛搭敂閸ャ儰姹楀┑鐐村灦閿曗晠宕靛Δ鍛厱闁哄洨鍋涢。宕囩磼閹邦収娈旈柍瑙勫灴閸ㄩ箖宕樺ù瀣壕鐟滅増甯楅崐璺侯熆閼搁潧濮囬悷娆欑畵閹﹢鎮欑捄杞版睏濡炪倕绻樻禍鍫曞蓟閳ユ剚鍚嬮柛銉㈡櫆閹劗绱撻崒娆掑厡闁稿鎸鹃埀顒傜懗閸忕偓鐩弫鍐磼濮樿京鍘俊鐐€栭悧妤冩崲閸岀偛鍌ㄩ柟缁㈠枟閸嬧剝绻涢崱妯兼噮闁伙綀浜幉鎼佸级閸喒鍋撻幖渚婄稏闁靛繈鍊曢～鍛存煃閵夈儳锛嶉柡灞界墦濮婃椽宕ㄦ繝鍕櫑闂佺粯顨呭Λ婊堝Φ閹版澘钃熼柕澶涚畱濞堟垿姊虹粙鍖″姛闁哥喐锕㈠鎾閻樼數鍘梻浣烘嚀椤曨厽鎱ㄩ悽鍛婂仼闁告稑鐡ㄩ悡鏇㈡煙閸濆嫭顥炴繛鍛缁绘稓鍠婇崡鐐典画缂備礁鍊圭敮鐐哄箯鐎ｎ喖鎹舵い鎾跺濡插嘲鈹戦悙瀛樼稇闁告艾顑夐弫瀣⒑閸涘鎴︽儎椤栫偟宓侀柟閭﹀幖缁剁偤鎮楅敐澶嬫锭濠㈣锕㈠娲偡閻楀牐纭€闂佸摜濮撮柊锝堟闂佸啿鎼幊蹇涘磻閻樼粯鐓曢柍鈺佸暙婵偓闂?        </div>
        <div class="info-grid">
          <div class="section">
            <h2>闂佽崵鍠愮划搴㈡櫠濡ゅ懎绠伴柛娑橈攻濞呯娀鏌ｅΟ铏癸紞妞も晜鐓￠弻鏇㈠醇濠靛浂妫″?/h2>
            <table class="stats-table" id="trade-stats"></table>
            <div id="trade-badges" class="badge-row"></div>
          </div>
          <div class="section conditions">
            <h2>闂傚倷鑳剁划顖滃垝瀹ュ拋鐔嗘俊顖滃皑閼版寧銇勮箛鎾跺闁稿骸绉归弻娑㈠即閵娿儰绨电紓渚囧枤閺佸寮诲☉妯滄梹鎷呴崷顓фО婵犵數鍋熼埛鍫ュ垂鏉堚晝鐭?/h2>
            {conditions_html}
          </div>
        </div>
      </section>
    </div>
  </div>

  <script>
    const candles = {json.dumps(candle_payload, ensure_ascii=False, separators=(",", ":"))};
    const trades = {json.dumps(trade_payload, ensure_ascii=False, separators=(",", ":"))};
    const summaries = {json.dumps(summary_payload, ensure_ascii=False, separators=(",", ":"))};

    const summaryCards = document.getElementById("summary-cards");
    const viewModeSelect = document.getElementById("view-mode");
    const strategySelect = document.getElementById("strategy-select");
    const tradeSelect = document.getElementById("trade-select");
    const beforeBarsInput = document.getElementById("before-bars");
    const afterBarsInput = document.getElementById("after-bars");
    const tradeStats = document.getElementById("trade-stats");
    const tradeBadges = document.getElementById("trade-badges");
    const chart = echarts.init(document.getElementById("chart"), null, {{ renderer: "canvas" }});

    function pct(value) {{
      return `${{(Number(value) * 100).toFixed(2)}}%`;
    }}

    function num(value, digits = 4) {{
      return Number(value).toFixed(digits);
    }}

    function buildSummaryCards() {{
      summaryCards.innerHTML = summaries.map((item) => `
        <div class="card">
          <div class="label">${{item.strategy_name}}</div>
          <div class="value">${{pct(item.total_return)}}</div>
          <div class="subtle">
            婵犵數鍋涢悺銊╁吹鎼淬劌纾归柡宥庣仜?${{item.trade_count}} 缂?br>
            闂傚倷娴囨竟鍫濃枍閺囩姴鍨濋幖绮规閸?${{pct(item.win_rate)}}<br>
            PF ${{num(item.profit_factor, 2)}} | 闂傚倷鐒﹂幃鍫曞磿閹惰棄纾婚柣鎰惈缁?${{pct(item.max_drawdown)}}
          </div>
        </div>
      `).join("");
    }}

    function strategyNames() {{
      return [...new Set(trades.map((item) => item.strategy_name))];
    }}

    function populateStrategies() {{
      strategySelect.innerHTML = strategyNames().map((name) => {{
        const first = trades.find((item) => item.strategy_name === name);
        const label = first ? first.strategy_label : name;
        return `<option value="${{name}}">${{label}} (${{name}})</option>`;
      }}).join("");
    }}

    function strategyTrades() {{
      return trades.filter((item) => item.strategy_name === strategySelect.value);
    }}

    function populateTrades() {{
      const items = strategyTrades();
      tradeSelect.innerHTML = items.map((item, idx) => `
        <option value="${{idx}}">
          #${{idx + 1}} | ${{item.entry_time.slice(0, 16).replace("T", " ")}} | R=${{num(item.r_multiple, 2)}} | ${{item.exit_reason}}
        </option>
      `).join("");
    }}

    function currentTrade() {{
      const items = strategyTrades();
      if (!items.length) {{
        return null;
      }}
      const idx = Number(tradeSelect.value || 0);
      return items[Math.max(0, Math.min(items.length - 1, idx))];
    }}

    function currentWindowBounds(trade) {{
      const beforeBars = Number(beforeBarsInput.value || 80);
      const afterBars = Number(afterBarsInput.value || 80);
      if (viewModeSelect.value === "focus") {{
        return {{
          start: Math.max(0, trade.signal_index - beforeBars),
          end: Math.min(candles.length - 1, trade.exit_index + afterBars),
        }};
      }}
      return {{
        start: 0,
        end: candles.length - 1,
      }};
    }}

    function badge(text, tone) {{
      return `<span class="badge ${{tone}}">${{text}}</span>`;
    }}

    function renderTradeStats(trade) {{
      if (!trade) {{
        tradeStats.innerHTML = "<tr><td>闂傚倷鑳剁划顖炩€﹂崼銉ユ槬闁哄稁鍘奸悞?/td><td>闂傚倷绀侀幖顐⑽涘Δ鍛９闁荤喐瀚堝☉銏犖ч柛鈩冪懐濞叉悂姊洪棃娑氬婵☆偅鐟╅敐?/td></tr>";
        tradeBadges.innerHTML = "";
        return;
      }}
      const rows = [
        ["婵犵數鍎戠徊钘壝洪悩璇茬劦妞ゆ巻鍋撻柛鐔稿缁绘盯宕堕浣哄幐闂佸憡鍔戦崝搴ㄥ春閿濆棔绻?, trade.signal_time.slice(0, 16).replace("T", " ")],
        ["闂佽瀛╅鏍窗閹烘纾婚柟鎹愮М瑜版帗鍋愰柧蹇ｅ亜闂夊秴鈹戦敍鍕粶闁稿﹤鐏濋～?, trade.entry_time.slice(0, 16).replace("T", " ")],
        ["濠德板€楁慨鐑藉磻濞戙垹鍌ㄩ柡宥庡亜閸ㄦ繈鏌曟繛鐐珔缂侇偄绉归弻娑㈩敃閿濆棛顦ㄩ梺?, trade.exit_time.slice(0, 16).replace("T", " ")],
        ["闂佽瀛╅鏍窗閹烘纾婚柟鎹愮М瑜版帗鍋愰柧蹇ｅ亜閻噣姊?, num(trade.entry_price)],
        ["濠德板€楁慨鐑藉磻濞戙垹鍌ㄩ柡宥庡亜閸ㄦ繈鏌曟繝蹇擃洭妞?, num(trade.exit_price)],
        ["闂傚倷绀侀幉锛勬暜濡ゅ啯宕查柛宀€鍎戠紞鏍煙闁箑澧绘繛闂村嵆閺屾洝绠涙繛鎯т壕鐎规洖娲﹂悞?, num(trade.initial_stop_price)],
        ["婵犵數鍋涢顓熸叏閻㈢绠板瀣捣缁€?, num(trade.position_size, 6)],
        ["闂備浇顕ф绋匡耿闁秴纾婚柕鍫濇媼閻庤埖銇勯弽銊ょ繁婵℃彃缍婇獮鏍庨鈧俊浠嬫煟濠垫挾鐣遍摶?, num(trade.actual_risk_amount)],
        ["闂傚倷绀佺紞濠囧疾閹绘帩娓婚柟鐑樻煥閸ㄦ繈鏌嶉锝庣劷缂?, String(trade.bars_held)],
        ["闂傚倸鍊风欢锟犲磻閳ь剟鏌涚€ｎ偅宕岄柡灞剧洴楠炲鈹戦崶銊ュ壍闂佹眹鍩勯崹濂稿磻婵犲倻鏆?, trade.exit_reason],
        ["闂傚倷绀侀幉锟犮€冮崱妞曞搫顭ㄩ崼鐔蜂画濡炪倕绻愰悧濠囧磿鎼淬劍鐓曟い鎰剁悼閻瞼鐥?, num(trade.pnl_amount)],
        ["闂傚倷绀侀幉锟犮€冮崱妞曞搫顭ㄩ崼鐔蜂画濡炪倕绻愰悧鍡欑磼閳哄懏鐓曢柟鎹愬皺閸斿秹鏌熺捄鍝勵伃闁?, pct(trade.pnl_pct)],
        ["R闂傚倷鑳堕…鍫ユ晝閵堝洨鐭撻柣銏㈩焾濮?, num(trade.r_multiple, 4)],
        ["闂傚倷绀侀幖顐λ囬鐐茬柈闁哄绨遍弸鏍ㄣ亜閹惧崬鐏╅柣顓燁殕娣囧﹪顢涘顓熷創濡?, `${{num(trade.equity_before)}} 闂?${{num(trade.equity_after)}}`],
      ];
      tradeStats.innerHTML = rows.map(([left, right]) => `<tr><td>${{left}}</td><td>${{right}}</td></tr>`).join("");
      const badges = [];
      badges.push(badge(`闂傚倸鍊风欢锟犲磻閳ь剟鏌涚€ｎ偅宕岄柡? ${{trade.exit_reason}}`, trade.pnl_amount >= 0 ? "green" : "red"));
      badges.push(badge(`1R婵犵數鍎戠徊钘壝洪敂鐐床闁告劦鐓夐懓? ${{trade.hit_1r ? "闂? : "闂?}}`, trade.hit_1r ? "gold" : ""));
      badges.push(badge(`1.5R闂傚倷绀侀幉锟犲垂閻㈠灚宕查悗锝庡墮閸? ${{trade.partial_take_profit ? "闂? : "闂?}}`, trade.partial_take_profit ? "green" : ""));
      badges.push(badge(`2R闂傚倷绀侀幉锟犲蓟閿濆绀夌€广儱顦悞鍨亜閹达絾纭堕柛鏂跨У閵囧嫰骞掗幘鑸靛垱闂佽鍨扮€氭澘鐣烽悡搴樻斀闁搞儜鍐棛闂? ${{trade.trail_armed ? "闂? : "闂?}}`, trade.trail_armed ? "green" : ""));
      badges.push(badge(`婵犵數鍎戠徊钘壝洪敂鐐床闁告劦鐓夐懓鍧楁煛婢跺鍎ユ繛闂村嵆閺屾洝绠涙繛鎯т壕鐎规洖娲﹂悞鎯р攽閻愭潙鐏﹂柟鍛婃倐閺佸啴鏁傜捄銊︽: ${{trade.breakeven_armed ? "闂? : "闂?}}`, trade.breakeven_armed ? "gold" : ""));
      tradeBadges.innerHTML = badges.join("");
    }}

    function renderChart() {{
      const trade = currentTrade();
      if (!trade) {{
        chart.clear();
        return;
      }}
      const isFocusMode = viewModeSelect.value === "focus";
      const bounds = currentWindowBounds(trade);
      const start = bounds.start;
      const end = bounds.end;
      const window = candles.slice(start, end + 1);
      const categories = window.map((item) => item.t.slice(0, 16).replace("T", " "));
      const ohlc = window.map((item) => [item.o, item.c, item.l, item.h]);
      const ema = window.map((item) => item.e);
      const signalCandle = candles[trade.signal_index];
      const entryCandle = candles[trade.entry_index];
      const exitCandle = candles[trade.exit_index];
      const risk = trade.initial_stop_price - trade.entry_price;
      const breakevenPrice = trade.entry_price;
      const takeProfitPrice = trade.entry_price - (1.5 * risk);
      const trailStartPrice = trade.entry_price - (2.0 * risk);
      const items = strategyTrades();
      const allEntries = items.map((item) => {{
        const candle = candles[item.entry_index];
        return [candle.t.slice(0, 16).replace("T", " "), item.entry_price];
      }});
      const allExits = items.map((item) => {{
        const candle = candles[item.exit_index];
        return [candle.t.slice(0, 16).replace("T", " "), item.exit_price];
      }});

      const startPercent = viewModeSelect.value === "continuous"
        ? Math.max(0, ((trade.signal_index - 300) / Math.max(1, candles.length - 1)) * 100)
        : 0;
      const endPercent = viewModeSelect.value === "continuous"
        ? Math.min(100, ((trade.exit_index + 300) / Math.max(1, candles.length - 1)) * 100)
        : 100;

      const segmentData = (startIndex, endIndex, price) => {{
        if (startIndex == null || endIndex == null || startIndex > endIndex) {{
          return [];
        }}
        const left = candles[startIndex];
        const right = candles[endIndex];
        return [
          [left.t.slice(0, 16).replace("T", " "), price],
          [right.t.slice(0, 16).replace("T", " "), price],
        ];
      }};
      const stopSegment = segmentData(trade.entry_index, trade.exit_index, trade.initial_stop_price);
      const breakevenSegment = trade.breakeven_armed
        ? segmentData(trade.breakeven_trigger_index ?? trade.entry_index, trade.exit_index, breakevenPrice)
        : [];
      const tpSegment = segmentData(trade.entry_index, trade.exit_index, takeProfitPrice);
      const tradePathSegment = [
        [entryCandle.t.slice(0, 16).replace("T", " "), trade.entry_price],
        [exitCandle.t.slice(0, 16).replace("T", " "), trade.exit_price],
      ];
      const slMarkerData = (trade.exit_reason === "stop_loss" || trade.exit_reason === "managed_stop")
        ? [[exitCandle.t.slice(0, 16).replace("T", " "), trade.exit_price]]
        : [];
      const beMarkerData = trade.exit_reason === "breakeven_stop"
        ? [[exitCandle.t.slice(0, 16).replace("T", " "), trade.exit_price]]
        : [];
      const tpMarkerData = (trade.pnl_amount > 0 && trade.exit_reason !== "breakeven_stop" && trade.exit_reason !== "stop_loss" && trade.exit_reason !== "managed_stop")
        ? [[exitCandle.t.slice(0, 16).replace("T", " "), trade.exit_price]]
        : [];

      const tradeLineDataFor = (list, kind) => {{
        const rows = [];
        for (const item of list) {{
          const itemEntry = candles[item.entry_index];
          const itemExit = candles[item.exit_index];
          const itemRisk = item.initial_stop_price - item.entry_price;
          if (!itemEntry || !itemExit || !Number.isFinite(itemRisk)) {{
            continue;
          }}
          const x1 = itemEntry.t.slice(0, 16).replace("T", " ");
          const x2 = itemExit.t.slice(0, 16).replace("T", " ");
          if (kind === "path") {{
            rows.push([x1, item.entry_price], [x2, item.exit_price], [null, null]);
            continue;
          }}
          if (kind === "sl") {{
            rows.push([x1, item.initial_stop_price], [x2, item.initial_stop_price], [null, null]);
            continue;
          }}
          if (kind === "tp") {{
            const itemTp = item.entry_price - (1.5 * itemRisk);
            rows.push([x1, itemTp], [x2, itemTp], [null, null]);
            continue;
          }}
          if (kind === "be" && item.breakeven_armed) {{
            const beStart = candles[item.breakeven_trigger_index ?? item.entry_index];
            if (beStart) {{
              rows.push(
                [beStart.t.slice(0, 16).replace("T", " "), item.entry_price],
                [x2, item.entry_price],
                [null, null],
              );
            }}
          }}
        }}
        return rows;
      }};

      const backgroundTradePathData = isFocusMode ? [] : tradeLineDataFor(items, "path");
      const backgroundStopData = isFocusMode ? [] : tradeLineDataFor(items, "sl");
      const backgroundTakeProfitData = isFocusMode ? [] : tradeLineDataFor(items, "tp");
      const backgroundBreakevenData = isFocusMode ? [] : tradeLineDataFor(items, "be");

      chart.setOption({{
        animation: false,
        backgroundColor: "#fffdf8",
        legend: {{
          top: 12,
          data: ["K缂?, "EMA55", "Trade Path", "SL", "BE", "TP", "All Entries", "All Exits", "Signal", "Entry", "Exit"]
        }},
        tooltip: {{
          trigger: "axis",
          axisPointer: {{ type: "cross" }},
        }},
        grid: {{
          left: 52,
          right: 24,
          top: 54,
          bottom: 88,
        }},
        xAxis: {{
          type: "category",
          data: categories,
          boundaryGap: true,
          axisLine: {{ lineStyle: {{ color: "#9ca3af" }} }},
          axisLabel: {{ color: "#6b7280" }},
        }},
        yAxis: {{
          scale: true,
          axisLine: {{ show: false }},
          splitLine: {{ lineStyle: {{ color: "rgba(107, 114, 128, 0.12)" }} }},
          axisLabel: {{ color: "#6b7280" }},
        }},
        dataZoom: [
          {{ type: "inside", start: startPercent, end: endPercent }},
          {{ type: "slider", bottom: 28, height: 22, start: startPercent, end: endPercent }}
        ],
        series: [
          {{
            name: "K",
            type: "candlestick",
            data: ohlc,
            itemStyle: {{
              color: "#159570",
              color0: "#d14e2f",
              borderColor: "#159570",
              borderColor0: "#d14e2f"
            }},
            markArea: {{
              itemStyle: {{ color: "rgba(163, 74, 40, 0.08)" }},
              data: [[
                {{ xAxis: entryCandle.t.slice(0, 16).replace("T", " ") }},
                {{ xAxis: exitCandle.t.slice(0, 16).replace("T", " ") }}
              ]]
            }},
            markLine: {{
              symbol: "none",
              label: {{ show: false }},
              lineStyle: {{ opacity: 0 }},
              data: [
                {{ yAxis: trade.entry_price, name: "Entry" }},
                {{ yAxis: trade.exit_price, name: "Exit" }}
              ]
            }},
          }},
          {{
            name: "EMA55",
            type: "line",
            data: ema,
            smooth: true,
            showSymbol: false,
            connectNulls: false,
            lineStyle: {{ width: 2, color: "#2563eb" }},
          }},
          {{
            name: "All Trade Paths",
            type: "line",
            data: backgroundTradePathData,
            showSymbol: false,
            connectNulls: false,
            symbolSize: 0,
            lineStyle: {{ width: 1.2, color: "rgba(29, 78, 216, 0.50)", type: "solid" }},
            tooltip: {{ show: false }},
          }},
          {{
            name: "All SL",
            type: "line",
            data: backgroundStopData,
            showSymbol: false,
            connectNulls: false,
            symbolSize: 0,
            lineStyle: {{ width: 1.0, color: "rgba(209, 78, 47, 0.50)", type: "dashed" }},
            tooltip: {{ show: false }},
          }},
          {{
            name: "All TP",
            type: "line",
            data: backgroundTakeProfitData,
            showSymbol: false,
            connectNulls: false,
            symbolSize: 0,
            lineStyle: {{ width: 1.0, color: "rgba(21, 149, 112, 0.55)", type: "dashed" }},
            tooltip: {{ show: false }},
          }},
          {{
            name: "All BE",
            type: "line",
            data: backgroundBreakevenData,
            showSymbol: false,
            connectNulls: false,
            symbolSize: 0,
            lineStyle: {{ width: 1.0, color: "rgba(29, 78, 216, 0.40)", type: "solid" }},
            tooltip: {{ show: false }},
          }},
          {{
            name: "SL",
            type: "line",
            data: stopSegment,
            step: "end",
            showSymbol: false,
            symbolSize: 0,
            lineStyle: {{ width: 1.4, color: "#d14e2f", type: "dashed" }},
            label: {{
              show: true,
              position: "end",
              formatter: "SL",
              color: "#d14e2f",
              fontWeight: 700,
            }},
          }},
          {{
            name: "Trade Path",
            type: "line",
            data: tradePathSegment,
            showSymbol: false,
            symbolSize: 0,
            lineStyle: {{ width: 2, color: "#1d4ed8", type: "solid" }},
          }},
            type: "scatter",
            data: [],
            tooltip: {{ show: false }},
          }},
            type: "scatter",
            data: [],
            tooltip: {{ show: false }},
          }},
          {{
            name: "BE",
            type: "line",
            data: breakevenSegment,
            step: "end",
            showSymbol: false,
            symbolSize: 0,
            lineStyle: {{ width: 1.5, color: "#1d4ed8", type: "solid" }},
            label: {{
              show: breakevenSegment.length > 0,
              position: "end",
              formatter: "BE",
              color: "#1d4ed8",
              fontWeight: 700,
            }},
          }},
          {{
            name: "TP",
            type: "line",
            data: tpSegment,
            step: "end",
            showSymbol: false,
            symbolSize: 0,
            lineStyle: {{ width: 1.4, color: "#159570", type: "dashed" }},
            label: {{
              show: true,
              position: "end",
              formatter: "TP",
              color: "#159570",
              fontWeight: 700,
            }},
          }},
          {{
            name: "All Entries",
            type: "scatter",
            symbolSize: 7,
            itemStyle: {{ color: "rgba(17, 24, 39, 0.45)" }},
            data: isFocusMode ? [] : allEntries,
          }},
          {{
            name: "All Exits",
            type: "scatter",
            symbolSize: 7,
            itemStyle: {{ color: "rgba(180, 35, 24, 0.35)" }},
            data: isFocusMode ? [] : allExits,
          }},
          {{
            name: "Signal",
            type: "scatter",
            symbolSize: 14,
            itemStyle: {{ color: "#f59e0b" }},
            data: [[signalCandle.t.slice(0, 16).replace("T", " "), signalCandle.c]],
          }},
          {{
            name: "Entry",
            type: "scatter",
            symbolSize: 18,
            symbol: "triangle",
            itemStyle: {{ color: "#1d4ed8" }},
            data: [[entryCandle.t.slice(0, 16).replace("T", " "), trade.entry_price]],
          }},
          {{
            name: "Exit",
            type: "scatter",
            symbolSize: 18,
            symbol: "circle",
            itemStyle: {{ color: trade.exit_reason === "stop_loss" || trade.exit_reason === "managed_stop" || trade.exit_reason === "breakeven_stop" ? "#d14e2f" : "#159570" }},
            data: [[exitCandle.t.slice(0, 16).replace("T", " "), trade.exit_price]],
          }},
          {{
            name: "SL Tag",
            type: "scatter",
            symbolSize: 14,
            itemStyle: {{ color: "#d14e2f" }},
            label: {{
              show: true,
              formatter: "SL",
              position: "right",
              color: "#d14e2f",
              fontWeight: 700,
            }},
            data: slMarkerData,
          }},
          {{
            name: "BE Tag",
            type: "scatter",
            symbolSize: 14,
            itemStyle: {{ color: "#1d4ed8" }},
            label: {{
              show: true,
              formatter: "BE",
              position: "right",
              color: "#1d4ed8",
              fontWeight: 700,
            }},
            data: beMarkerData,
          }},
          {{
            name: "TP Tag",
            type: "scatter",
            symbolSize: 14,
            itemStyle: {{ color: "#159570" }},
            label: {{
              show: true,
              formatter: "TP",
              position: "right",
              color: "#159570",
              fontWeight: 700,
            }},
            data: tpMarkerData,
          }}
        ]
      }}, true);
      renderTradeLineLayer();
      window.setTimeout(() => {{
        renderingChart = false;
      }}, 0);
    }}

    function refresh() {{
      renderTradeStats(currentTrade());
      renderChart();
    }}

    buildSummaryCards();
    populateStrategies();
    populateTrades();
    refresh();

    strategySelect.addEventListener("change", () => {{
      populateTrades();
      refresh();
    }});
    viewModeSelect.addEventListener("change", refresh);
    tradeSelect.addEventListener("change", refresh);
    beforeBarsInput.addEventListener("change", refresh);
    afterBarsInput.addEventListener("change", refresh);
    window.addEventListener("resize", () => chart.resize());
  </script>
</body>
</html>"""
    html = build_review_page_html(
        candle_payload=candle_payload,
        trade_payload=trade_payload,
        summary_payload=summary_payload,
        conditions_html=conditions_html,
        runtime_note=(
            f"data range: {frame['timestamp'].iloc[0]} to {frame['timestamp'].iloc[-1]}; "
            f"{len(frame)} {runtime.backtest.bar} candles; "
            f"fee {runtime.backtest.fee_rate:.2%} one-way; "
            f"slippage {runtime.backtest.slippage_rate:.2%} one-way."
        ),
    )
    html = sanitize_review_html(html)
    (report_dir / "trade_review.html").write_text(html, encoding="utf-8")
    if not ECHARTS_VENDOR_PATH.exists():
        raise FileNotFoundError(f"Missing local ECharts bundle: {ECHARTS_VENDOR_PATH}")
    shutil.copy2(ECHARTS_VENDOR_PATH, report_dir / "echarts.min.js")


def build_conditions_html(config: BacktestConfig) -> str:
    return f"""
<h3>Main Setup: breakdown then failed pullback short</h3>
<ul>
  <li>Valid breakdown first: close &lt; EMA55 - {config.breakdown_atr_multiple:.1f} x ATR14.</li>
  <li>EMA55 slope down: current EMA55 &lt; EMA55 from {config.slope_lookback} bars ago.</li>
  <li>After the breakdown, wait up to {config.watch_bars} bars for the pullback sequence.</li>
  <li>Pullback must reach back near EMA55: high &gt;= EMA55 - {config.retest_atr_multiple:.1f} x ATR14.</li>
  <li>Failure bar confirms the short bias: close &lt; EMA55 and close &lt; open.</li>
  <li>Enter short on the next bar open after confirmation.</li>
</ul>
<h3>Alternative Setup: direct short after breakdown</h3>
<ul>
  <li>If the breakdown is valid and EMA55 is sloping down, the direct-short branch can enter on the next bar open.</li>
</ul>
<h3>Risk and Position</h3>
<ul>
  <li>Initial stop = max(signal bar high, EMA55 + {config.stop_buffer_atr:.1f} x ATR14).</li>
  <li>Risk per trade = account equity x {config.risk_per_trade:.2%}.</li>
  <li>Position size is capped by the configured notional limit of {config.max_notional_multiple:.1f}x equity.</li>
</ul>
<h3>Management</h3>
<ul>
  <li>At 1R, the trade becomes breakeven eligible.</li>
  <li>At {config.partial_r:.1f}R, partial take profit closes {config.partial_close_fraction:.0%}.</li>
  <li>At {config.trail_start_r:.1f}R, trailing stop logic activates using the last {config.trail_lookback} bars plus {config.trail_atr_multiple:.1f} x ATR14.</li>
  <li>Close-above-EMA55 logic can force an exit if the short thesis is invalidated.</li>
</ul>
"""


def build_review_page_html(
    candle_payload: list[dict[str, object]],
    trade_payload: list[dict[str, object]],
    summary_payload: list[dict[str, object]],
    conditions_html: str,
    runtime_note: str,
) -> str:
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>BTC 1H EMA55 Trade Review</title>
  <script src="./echarts.min.js"></script>
  <style>
  <style>
    :root {{
      --bg: #f3efe6;
      --panel: rgba(255, 252, 246, 0.94);
      --panel-strong: #fffdf8;
      --line: rgba(94, 74, 49, 0.14);
      --line-strong: rgba(94, 74, 49, 0.24);
      --ink: #1d2733;
      --muted: #6d7883;
      --accent: #a85c2a;
      --accent-soft: rgba(168, 92, 42, 0.1);
      --green: #177a54;
      --red: #bf3a2e;
      --gold: #b88618;
      --blue: #1d5fd1;
      --shadow: 0 22px 48px rgba(29, 39, 51, 0.08);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Segoe UI", "PingFang SC", "Microsoft YaHei UI", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top right, rgba(168, 92, 42, 0.12), transparent 24%),
        radial-gradient(circle at left 16%, rgba(29, 95, 209, 0.06), transparent 28%),
        linear-gradient(180deg, #f8f4ea 0%, var(--bg) 100%);
    }}
    .page {{
      width: min(1780px, calc(100vw - 20px));
      margin: 10px auto 18px;
    }}
    .hero,
    .workspace {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 24px;
      box-shadow: var(--shadow);
      backdrop-filter: blur(12px);
    }}
    .hero {{
      padding: 24px 28px 22px;
      margin-bottom: 14px;
    }}
    .hero-top {{
      display: flex;
      justify-content: space-between;
      align-items: flex-start;
      gap: 18px;
      margin-bottom: 10px;
    }}
    .eyebrow {{
      margin: 0 0 8px;
      font-size: 11px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.16em;
      color: var(--accent);
    }}
    h1 {{
      margin: 0;
      font-size: clamp(26px, 3vw, 38px);
      letter-spacing: 0.01em;
    }}
    .hero-chip {{
      flex: 0 0 auto;
      padding: 10px 14px;
      border-radius: 999px;
      border: 1px solid rgba(168, 92, 42, 0.18);
      background: var(--accent-soft);
      color: var(--accent);
      font-size: 13px;
      font-weight: 700;
      white-space: nowrap;
    }}
    .subtle {{
      color: var(--muted);
      line-height: 1.65;
      max-width: 1120px;
    }}
    .grid {{
      display: grid;
      gap: 16px;
    }}
    .summary-grid {{
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      margin-top: 18px;
    }}
    .card {{
      padding: 16px 16px 18px;
      border: 1px solid var(--line);
      border-radius: 16px;
      background: linear-gradient(180deg, rgba(255, 255, 255, 0.86) 0%, rgba(249, 245, 237, 0.9) 100%);
    }}
    .label {{
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: var(--muted);
    }}
    .value {{
      margin-top: 6px;
      font-size: 24px;
      font-weight: 700;
    }}
    .workspace {{
      padding: 18px;
    }}
    .workspace-head {{
      display: flex;
      justify-content: space-between;
      align-items: end;
      gap: 18px;
      margin-bottom: 16px;
    }}
    .workspace-tools {{
      display: flex;
      align-items: center;
      gap: 12px;
      margin-left: auto;
    }}
    .workspace-head h2 {{
      margin: 0;
      font-size: 22px;
    }}
    .head-note {{
      max-width: 440px;
      color: var(--muted);
      line-height: 1.6;
      font-size: 13px;
      text-align: right;
    }}
    .toolbar {{
      display: grid;
      grid-template-columns: 180px 240px minmax(240px, 1fr) 170px 170px;
      gap: 12px;
      align-items: end;
      margin-bottom: 16px;
    }}
    .control {{
      display: grid;
      gap: 6px;
    }}
    .control label {{
      font-size: 12px;
      letter-spacing: 0.06em;
      text-transform: uppercase;
      color: var(--muted);
      font-weight: 700;
    }}
    select,
    input {{
      width: 100%;
      border: 1px solid rgba(94, 74, 49, 0.18);
      border-radius: 12px;
      background: rgba(255, 255, 255, 0.92);
      color: var(--ink);
      padding: 11px 12px;
      font-size: 14px;
      transition: border-color 120ms ease, box-shadow 120ms ease;
    }}
    select:focus,
    input:focus {{
      outline: none;
      border-color: rgba(29, 95, 209, 0.36);
      box-shadow: 0 0 0 4px rgba(29, 95, 209, 0.08);
    }}
    .workspace-grid {{
      display: grid;
      grid-template-columns: minmax(0, 1fr) 360px;
      gap: 16px;
      align-items: start;
    }}
    .workspace-grid.chart-only {{
      grid-template-columns: minmax(0, 1fr);
    }}
    .workspace-grid.chart-only .sidebar {{
      display: none;
    }}
    .chart-shell {{
      border: 1px solid var(--line);
      border-radius: 20px;
      background: linear-gradient(180deg, rgba(255, 255, 255, 0.76) 0%, rgba(249, 245, 237, 0.94) 100%);
      padding: 14px 14px 12px;
      overflow: hidden;
    }}
    #chart {{
      width: 100%;
      height: calc(100vh - 220px);
      min-height: 760px;
    }}
    .footer-note {{
      margin-top: 10px;
      padding-top: 10px;
      border-top: 1px dashed var(--line-strong);
      color: var(--muted);
      font-size: 13px;
      line-height: 1.65;
    }}
    .sidebar {{
      position: sticky;
      top: 12px;
      display: grid;
      gap: 16px;
      align-self: start;
    }}
    .drawer {{
      border: 1px solid var(--line);
      border-radius: 18px;
      background: var(--panel-strong);
      padding: 16px;
      box-shadow: 0 12px 30px rgba(29, 39, 51, 0.05);
    }}
    .drawer-head {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      margin-bottom: 12px;
    }}
    .drawer-title {{
      margin: 0;
      font-size: 16px;
      font-weight: 700;
    }}
    .drawer-toggle {{
      border: 1px solid rgba(94, 74, 49, 0.18);
      border-radius: 999px;
      background: rgba(255, 255, 255, 0.92);
      color: var(--muted);
      padding: 6px 12px;
      font-size: 12px;
      font-weight: 700;
      cursor: pointer;
    }}
    .drawer-toggle:hover {{
      border-color: rgba(29, 95, 209, 0.24);
      color: var(--blue);
    }}
    .stats-table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
    }}
    .stats-table td {{
      padding: 9px 0;
      border-bottom: 1px dashed var(--line);
      vertical-align: top;
    }}
    .stats-table td:first-child {{
      width: 40%;
      color: var(--muted);
      padding-right: 10px;
    }}
    .badge-row {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-top: 12px;
    }}
    .badge {{
      padding: 6px 10px;
      border-radius: 999px;
      font-size: 12px;
      border: 1px solid transparent;
      background: #f4efe4;
    }}
    .badge.green {{
      color: var(--green);
      border-color: rgba(23, 122, 84, 0.18);
      background: rgba(23, 122, 84, 0.08);
    }}
    .badge.red {{
      color: var(--red);
      border-color: rgba(191, 58, 46, 0.18);
      background: rgba(191, 58, 46, 0.08);
    }}
    .badge.gold {{
      color: var(--gold);
      border-color: rgba(184, 134, 24, 0.22);
      background: rgba(184, 134, 24, 0.1);
    }}
    .conditions {{
      font-size: 14px;
      line-height: 1.75;
    }}
    .conditions h3 {{
      margin: 14px 0 6px;
      font-size: 15px;
    }}
    .conditions ul {{
      margin: 0;
      padding-left: 18px;
    }}
    .conditions li + li {{
      margin-top: 4px;
    }}
    @media (max-width: 1360px) {{
      .toolbar {{
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }}
      .workspace-grid {{
        grid-template-columns: 1fr;
      }}
      .sidebar {{
        position: static;
      }}
      .head-note {{
        text-align: left;
        max-width: none;
      }}
      #chart {{
        height: 720px;
      }}
    }}
    @media (max-width: 860px) {{
      .page {{
        width: calc(100vw - 10px);
        margin: 5px auto 10px;
      }}
      .hero,
      .workspace {{
        border-radius: 18px;
      }}
      .hero,
      .workspace {{
        padding-left: 14px;
        padding-right: 14px;
      }}
      .hero-top,
      .workspace-head {{
        display: grid;
      }}
      .workspace-tools {{
        margin-left: 0;
        justify-content: flex-start;
      }}
      .toolbar {{
        grid-template-columns: 1fr;
      }}
      #chart {{
        height: 560px;
        min-height: 560px;
      }}
    }}
  </style>
</head>
<body>
          <h1>BTC 1H EMA55 Trade Review</h1>
    <section class="hero">
        <div class="hero-chip">Landscape workspace 路 chart first</div>
        <div>
          <p class="eyebrow">BTC 1H / EMA55 / Review</p>
          <h1>BTC 1H EMA55 缂傚倸鍊烽悞锔剧矙閹烘鍋嬫繝濠傜墕濮规煡鏌熼悧鍫熺凡鐎瑰憡绻冩穱濠囶敍濞嗘帩鍔呭┑鈩冨絻椤兘寮?/h1>
        </div>
        <div class="hero-chip">闂傚倷鐒﹂幃鍫曞磿閹绘帞鏆︽慨妞诲亾濠碘剝鎸冲畷姗€鍩℃笟鍥ф婵犵妲呴崹浼村箹椤愩倖鏆?闂?濠电姷顣藉Σ鍛村垂閸忚偐顩叉繝濠傜墛閸庢捇鏌涢埄鍐炬闁哥喎鎳橀弻鏇熷緞閸績鍋撳Δ鍐煓濠电姴娲﹂悡?/div>
      </div>
      <div class="subtle">{runtime_note}</div>
      <div id="summary-cards" class="grid summary-grid"></div>
    </section>
          <h2>K-line Overview</h2>
    <section class="workspace">
      <div class="workspace-head">
          <div class="head-note">Continuous mode keeps the full market background visible. Focus mode zooms in on the selected trade.</div>
          <button type="button" class="drawer-toggle" id="sidebar-toggle">Hide Sidebar</button>
          <h2>K缂傚倸鍊烽懗鍫曞磻閹惧灈鍋撶粭娑樻搐閻掑灚銇勯幒宥囧妽妞ゎ偅顨嗛妵?/h2>
        </div>
        <div class="workspace-tools">
          <div class="head-note">闂備礁鎼ˇ顐﹀疾濠靛纾婚柣鏂垮悑閸嬨劌鈹戦悩宕囶暡闁搞倕瀚伴幃姗€鎮欓棃娑楀闂佺粯绻嶉崹鍫曞蓟濞戞ǚ鏋庢俊顖氭贡閼宠櫣绱撴担铏瑰笡鐎光偓閹间礁绠栭柟顖嗏偓閺嬪酣鐓崶銊︾濮濆洭姊绘担鍛婂暈闁圭鎽滅划鏃囥亹閹烘垹鍔﹀銈嗗坊閸嬫挻銇勯敂璇叉珝闁硅櫕鐟╅獮瀣倷濞堟寧閿ら梻渚€娼ч悧鍡椢涘▎鎿勭稏婵犲﹤鐗婇悡鐔兼煏婵炲灝鍔氭い蹇婃櫆缁绘稓鈧稒銆為幋鐘电當闁绘棁銆€閸嬫捇鏁愭惔鈥茬敖闂佹悶鍔岄妶鎼佸箖鐟欏嫭濯寸紒瀣硶閻╁海绱撴担浠嬪摵缂佽鐗撻獮鍐╃鐎ｎ€晠鏌ㄩ弬鍓х煓婵″墽鍏樺鍝劽虹拋宕囩泿濡炪倖鍨甸幊妯讳繆閻㈢鐐婇柍鍝勫€归崕閬嶆煟鎼搭垳绉靛ù婊勭矒閸╋綁濮€閵堝棛鍙嗗┑鐐村灦閻熝囧煟閵夛附鍙忔慨妤€鐗忛悾鐢碘偓娈垮枙閸楁娊鐛鈧弻鍛槈濮樺灈鏀洪梻鍌欒兌缁垳鏁鍡欎笉闁硅揪绲绘禍鍦喐閺傛鍤曢柕濞炬櫅缁犳氨鎲哥€ｎ偆顩查柨婵嗘处閸犳劙鏌ｅΔ鈧悧濠勪焊椤撶儐鐔嗙憸蹇旀叏闂堟侗鐒介悹鍥ㄧゴ閺嬪酣骞栨潏鍓хУ濞寸厧娲ㄧ槐鎾存媴鐠団剝鐣烽梺鍝勭焿缂嶄線宕洪埀?/div>
          <button type="button" class="drawer-toggle" id="sidebar-toggle">闂傚倷绀侀幖顐も偓姘煎墯閺呰埖绂掔€ｎ€附鎱ㄥ鍡楀箻闁崇懓绉归弻宥夊煛娴ｅ憡娈ㄧ紓浣介哺閻楃娀寮?/button>
          <label for="view-mode">View Mode</label>
      </div>
            <option value="continuous" selected>Continuous Background</option>
            <option value="focus">Trade Focus</option>
        <div class="control">
          <label for="view-mode">闂傚倷鐒﹂幃鍫曞磿閹绘帞鏆︽慨妞诲亾濠碘剝鎸冲畷姗€濡告惔锝呮灈闁搞劍鍎抽悾鐑藉炊閳哄﹥鏁?/label>
          <select id="view-mode">
          <label for="strategy-select">Strategy</label>
            <option value="focus">婵犵數鍋涢悺銊╁吹鎼淬劌纾归柡宥庣仜閿濆憘鏃堝川椤斿吋鐣繝娈垮枟閿氱€规洦鍓氭穱?/option>
          </select>
        </div>
          <label for="trade-select">Trade</label>
          <label for="strategy-select">缂傚倸鍊烽悞锔剧矙閹烘鍋嬫繝濠傜墕濮?/label>
          <select id="strategy-select"></select>
        </div>
          <label for="before-bars">Bars Before</label>
          <label for="trade-select">婵犵數鍋涢悺銊╁吹鎼淬劌纾归柡宥庣仜?/label>
          <select id="trade-select"></select>
        </div>
          <label for="after-bars">Bars After</label>
          <label for="before-bars">闂傚倷绀侀幉锟犲箰閸濄儳鐭撻柛顐ｆ礃閸嬵亪鏌涘鍐ㄢ挃缂?/label>
          <input id="before-bars" type="number" min="20" max="500" step="10" value="80">
        </div>
        <div class="control">
          <label for="after-bars">闂傚倷绀侀幉锟犳嚌閹灐褰掓倻閼恒儱浠鹃梺鍛婄矊婢х晫妲?/label>
          <input id="after-bars" type="number" min="20" max="500" step="10" value="80">
        </div>
          <div class="footer-note">Trade lines follow the review rules: entry-to-exit path, original SL, original 2R TP, and BE only when triggered by the strategy.</div>

      <div class="workspace-grid" id="workspace-grid">
        <div class="chart-shell">
          <div id="chart"></div>
          <div class="footer-note">闂傚倷鐒﹂惇褰掑垂閼姐倕鍨濇い鏍ㄧ矋閸忔粓鏌涢锝嗙闁绘劕锕弻娑樷攽閸℃浼€闂佸搫妫旈崡鎶藉蓟閵娿儮妲堟繛鍡樺笒椤ユ繄绱撴担浠嬪摵闁搞劌澧庣划瀣箳濡も偓缁犲鏌ц箛锝呬簻闁逞屽墰閺佸摜妲愰幒妤€妫樻繛鍡樺劤閸撲即鎮楃憴鍕碍婵☆偅绻堥獮鍐偩鐏炴儳鐝伴梺鍦帛鐢鎯侀弮鍫熲拺闁告稑顭悞浠嬫煕閻樻煡鍙勯柨婵堝仱楠炲洭顢楁径瀣€冮梻渚€娼х换鍡涘焵椤掆偓绾绢參寮婚崼銏㈢＝濞达絿鎳撻崝銈夋煟韫囨梻绠炴鐐茬箲鐎靛ジ寮堕幋婵嗕憾闂備胶绮弻銊┿€冩惔鈽嗙劷?2R 濠电姵顔栭崰妤冪紦閸ф鍨傞悹鍥ㄧゴ閺嬫牠鏌嶉崫鍕殶婵☆偒鍨堕弻娑㈠冀閻㈢數锛熺紓浣插亾濠㈣泛艌濡插牓鏌熼幆褍鑸归柍褜鍓氶悧鏇⑩€﹂崶顒夋晬婵犲﹤鎳忛敍蹇涙⒑缁夊棗瀚峰▓鏃堟煕濡粯灏﹂柡灞剧☉閳诲骸螣閸︻厾娉挎繝鐢靛仜濡酣宕归搹鍏夊亾娴ｅ啫浜圭紒顔界懇瀹曞綊顢欓幆褍绗撻梻浣告惈椤︻偊寮插鍫熷仭鐟滄棃骞冮悜绛嬫晜闁告侗鍨煎ú鎼佹⒑闂堟稓澧曟俊顐ｇ懇閿濈偛顭ㄩ崨顖滐紳婵炶揪缍€椤浜搁悧鍫㈢缁炬澘宕禍浼存煙椤栨艾鏆ｇ€规洜鍠栭、娑橆潩椤愩倕鍤繝鐢靛仦閸ㄥ爼骞愰幘顔肩；闁瑰墽绮悡鏇㈡煙閸濆嫮肖濞戞捁灏欑槐鎺楁焼瀹ュ嫭鍠氶梺鐟扮－閸犳牗淇婇悿顖ｆЬ闂佺濮ょ划鎾诲蓟閿濆鏅查柛鎰╁妼婵倝鎮跺顓犵疄闁哄本绋戦埢搴ㄥ箛椤掑倷鍖栫紓鍌欒兌缁垰鐣濈粙娆惧殨闁汇垹澹婇弫鍡椻攽閻樻彃顏い锔诲櫍濮婃椽宕ㄦ繝鍌氼潓缂備礁顦遍弻澶娢?BE 缂傚倸鍊烽悞锕€鐣峰鈧弫鎾诲Ψ閳轰胶鍔?/div>
              <div class="drawer-title">Current Trade</div>

        <aside class="sidebar" id="review-sidebar">
          <section class="drawer">
            <div class="drawer-head">
              <div class="drawer-title">闂佽崵鍠愮划搴㈡櫠濡ゅ懎绠伴柛娑橈攻濞呯娀鏌ｅΟ铏癸紞妞も晜鐓￠弻鏇㈠醇濠靛浂妫″?/div>
            </div>
            <div class="drawer-body">
              <table class="stats-table" id="trade-stats"></table>
              <div class="drawer-title">Current Rules</div>
            </div>
          </section>
          <section class="drawer conditions">
            <div class="drawer-head">
              <div class="drawer-title">闂佽崵鍠愮划搴㈡櫠濡ゅ懎绠伴柛娑橈攻濞呯娀鏌ｅΟ娆惧殭缁惧墽绮幈銊ヮ潨閸℃顫╃紓浣插亾閻庯綆鍠楅悡娑橆熆鐠虹儤顥戦柛瀣崌瀹曪絾寰勫Ο杞扮紦</div>
            </div>
            <div class="drawer-body">
              {conditions_html}
            </div>
          </section>
        </aside>
      </div>
    </section>
  </div>

  <script>
    const candles = {json.dumps(candle_payload, ensure_ascii=False, separators=(",", ":"))};
    const trades = {json.dumps(trade_payload, ensure_ascii=False, separators=(",", ":"))};
    const summaries = {json.dumps(summary_payload, ensure_ascii=False, separators=(",", ":"))};

    const summaryCards = document.getElementById("summary-cards");
    const viewModeSelect = document.getElementById("view-mode");
    const strategySelect = document.getElementById("strategy-select");
    const tradeSelect = document.getElementById("trade-select");
    const beforeBarsInput = document.getElementById("before-bars");
    const afterBarsInput = document.getElementById("after-bars");
    const tradeStats = document.getElementById("trade-stats");
    const tradeBadges = document.getElementById("trade-badges");
    const workspaceGrid = document.getElementById("workspace-grid");
    const reviewSidebar = document.getElementById("review-sidebar");
    const sidebarToggle = document.getElementById("sidebar-toggle");
    const chart = echarts.init(document.getElementById("chart"), null, {{ renderer: "canvas" }});
    let activeZoom = {{ start: 0, end: 100 }};
    let renderingChart = false;

    function pct(value) {{
      return `${{(Number(value) * 100).toFixed(2)}}%`;
    }}

    function num(value, digits = 4) {{
      return Number(value).toFixed(digits);
    }}

    function tsLabel(value) {{
      return String(value).slice(0, 16).replace("T", " ");
    }}

    function buildSummaryCards() {{
            Trades ${{item.trade_count}}<br>
            Win Rate ${{pct(item.win_rate)}}<br>
            PF ${{num(item.profit_factor, 2)}} | Max DD ${{pct(item.max_drawdown)}}
          <div class="value">${{pct(item.total_return)}}</div>
          <div class="subtle">
            婵犵數鍋涢悺銊╁吹鎼淬劌纾归柡宥庣仜?${{item.trade_count}} 缂?br>
            闂傚倷娴囨竟鍫濃枍閺囩姴鍨濋幖绮规閸?${{pct(item.win_rate)}}<br>
            PF ${{num(item.profit_factor, 2)}} | 闂傚倷鐒﹂幃鍫曞磿閹惰棄纾婚柣鎰惈缁?${{pct(item.max_drawdown)}}
          </div>
        </div>
      `).join("");
    }}

    function strategyNames() {{
      return [...new Set(trades.map((item) => item.strategy_name))];
    }}

    function populateStrategies() {{
      strategySelect.innerHTML = strategyNames().map((name) => {{
        const first = trades.find((item) => item.strategy_name === name);
        const label = first ? first.strategy_label : name;
        return `<option value="${{name}}">${{label}} (${{name}})</option>`;
      }}).join("");
    }}

    function strategyTrades() {{
      return trades.filter((item) => item.strategy_name === strategySelect.value);
    }}

    function populateTrades() {{
      const items = strategyTrades();
      tradeSelect.innerHTML = items.map((item, idx) => `
        <option value="${{idx}}">
          #${{idx + 1}} | ${{tsLabel(item.entry_time)}} | R=${{num(item.r_multiple, 2)}} | ${{item.exit_reason}}
        </option>
      `).join("");
    }}

    function currentTrade() {{
      const items = strategyTrades();
      if (!items.length) {{
        return null;
      }}
      const idx = Number(tradeSelect.value || 0);
      return items[Math.max(0, Math.min(items.length - 1, idx))];
    }}

    function currentWindowBounds(trade) {{
      const beforeBars = Number(beforeBarsInput.value || 80);
      const afterBars = Number(afterBarsInput.value || 80);
      if (viewModeSelect.value === "focus") {{
        return {{
          start: Math.max(0, trade.signal_index - beforeBars),
          end: Math.min(candles.length - 1, trade.exit_index + afterBars),
        }};
      }}
      return {{
        start: 0,
        end: candles.length - 1,
      }};
    }}

    function badge(text, tone) {{
      return `<span class="badge ${{tone}}">${{text}}</span>`;
      sidebarToggle.textContent = hidden ? "Show Sidebar" : "Hide Sidebar";

    function setSidebarHidden(hidden) {{
      workspaceGrid.classList.toggle("chart-only", hidden);
      reviewSidebar.hidden = hidden;
      sidebarToggle.textContent = hidden ? "闂傚倷绀侀幖顐も偓姘煎墯閺呰埖绂掔€ｎ€附鎱ㄥ鍡楀箻闁崇懓绉归弻宥夊煛娴ｅ憡娈ㄧ紓浣介哺閻楃娀寮? : "闂傚倸鍊搁崐鎼佸箠韫囨稑绀夋俊顖欑秿濞戞矮娌柛鎾椻偓閺€鎶芥⒑閺傘儲娅呴柛鐔叉櫇濡叉劙寮介鐔哄幐?;
      try {{
        window.localStorage.setItem("btc_review_sidebar_hidden", hidden ? "1" : "0");
      }} catch (error) {{
      }}
      chart.resize();
    }}

    function loadSidebarState() {{
      try {{
        const saved = window.localStorage.getItem("btc_review_sidebar_hidden");
        return saved == null ? true : saved === "1";
      }} catch (error) {{
        return true;
        tradeStats.innerHTML = "<tr><td>No trade</td><td>Select a trade to inspect details.</td></tr>";
    }}

    function renderTradeStats(trade) {{
      if (!trade) {{
        ["Signal Time", tsLabel(trade.signal_time)],
        ["Entry Time", tsLabel(trade.entry_time)],
        ["Exit Time", tsLabel(trade.exit_time)],
        ["Entry Price", num(trade.entry_price)],
        ["Exit Price", num(trade.exit_price)],
        ["Initial Stop", num(trade.initial_stop_price)],
        ["Position Size", num(trade.position_size, 6)],
        ["Risk Amount", num(trade.actual_risk_amount)],
        ["Bars Held", String(trade.bars_held)],
        ["Exit Reason", trade.exit_reason],
        ["PnL", num(trade.pnl_amount)],
        ["Return", pct(trade.pnl_pct)],
        ["R Multiple", num(trade.r_multiple, 4)],
        ["Equity", `${{num(trade.equity_before)}} -> ${{num(trade.equity_after)}}`],
        ["闂傚倸鍊风欢锟犲磻閳ь剟鏌涚€ｎ偅宕岄柡灞剧洴楠炲鈹戦崶銊ュ壍闂佹眹鍩勯崹濂稿磻婵犲倻鏆?, trade.exit_reason],
        ["闂傚倷绀侀幉锟犮€冮崱妞曞搫顭ㄩ崼鐔蜂画濡炪倕绻愰悧濠囧磿鎼淬劍鐓曟い鎰剁悼閻瞼鐥?, num(trade.pnl_amount)],
        ["闂傚倷绀侀幉锟犮€冮崱妞曞搫顭ㄩ崼鐔蜂画濡炪倕绻愰悧鍡欑磼閳哄懏鐓曢柟鎹愬皺閸斿秹鏌熺捄鍝勵伃闁?, pct(trade.pnl_pct)],
      badges.push(badge(`Exit: ${{trade.exit_reason}}`, trade.pnl_amount >= 0 ? "green" : "red"));
      badges.push(badge(`1R BE: ${{trade.hit_1r ? "Yes" : "No"}}`, trade.hit_1r ? "gold" : ""));
      badges.push(badge(`1.5R Partial: ${{trade.partial_take_profit ? "Yes" : "No"}}`, trade.partial_take_profit ? "green" : ""));
      badges.push(badge(`2R Trail: ${{trade.trail_armed ? "Yes" : "No"}}`, trade.trail_armed ? "green" : ""));
      badges.push(badge(`BE Moved: ${{trade.breakeven_armed ? "Yes" : "No"}}`, trade.breakeven_armed ? "gold" : ""));
      badges.push(badge(`闂傚倸鍊风欢锟犲磻閳ь剟鏌涚€ｎ偅宕岄柡? ${{trade.exit_reason}}`, trade.pnl_amount >= 0 ? "green" : "red"));
      badges.push(badge(`1R婵犵數鍎戠徊钘壝洪敂鐐床闁告劦鐓夐懓? ${{trade.hit_1r ? "闂? : "闂?}}`, trade.hit_1r ? "gold" : ""));
      badges.push(badge(`1.5R闂傚倷绀侀幉锟犲垂閻㈠灚宕查悗锝庡墮閸? ${{trade.partial_take_profit ? "闂? : "闂?}}`, trade.partial_take_profit ? "green" : ""));
      badges.push(badge(`2R闂傚倷绀侀幉锟犲蓟閿濆绀夌€广儱顦悞鍨亜閹达絾纭堕柛鏂跨У閵囧嫰骞掗幘鑸靛垱闂? ${{trade.trail_armed ? "闂? : "闂?}}`, trade.trail_armed ? "green" : ""));
      badges.push(badge(`婵犵數鍎戠徊钘壝洪敂鐐床闁告劦鐓夐懓鍧楁煛婢跺鍎ユ繛闂村嵆閺屾洝绠涙繛鎯т壕鐎规洖娲﹂悞鎯р攽閻愭潙鐏﹂柟鍛婃倐閺佸啴鏁傜捄銊︽: ${{trade.breakeven_armed ? "闂? : "闂?}}`, trade.breakeven_armed ? "gold" : ""));
      tradeBadges.innerHTML = badges.join("");
    }}

    function renderChart() {{
      const trade = currentTrade();
      if (!trade) {{
        chart.clear();
        return;
      }}

      const isFocusMode = viewModeSelect.value === "focus";
      const bounds = currentWindowBounds(trade);
      const windowCandles = candles.slice(bounds.start, bounds.end + 1);
      const categories = windowCandles.map((item) => tsLabel(item.t));
      const ohlc = windowCandles.map((item) => [item.o, item.c, item.l, item.h]);
      const ema = windowCandles.map((item) => item.e);
      const signalCandle = candles[trade.signal_index];
      const entryCandle = candles[trade.entry_index];
      const exitCandle = candles[trade.exit_index];
      const startPercent = 0;
      const endPercent = 100;
      const visualTrades = isFocusMode ? [trade] : strategyTrades();

      const buildTradeVisuals = (list) => {{
        const visuals = {{
          pathSegments: [],
          slSegments: [],
          beSegments: [],
          tpSegments: [],
          signal: [],
          entry: [],
          exit: [],
          slTag: [],
          beTag: [],
          tpTag: [],
        }};
        for (const item of list) {{
          const signalBar = candles[item.signal_index];
          const entryBar = candles[item.entry_index];
          const exitBar = candles[item.exit_index];
          if (!signalBar || !entryBar || !exitBar) {{
            continue;
          }}
          const isShortItem = item.initial_stop_price >= item.entry_price;
          const itemRisk = Math.abs(item.initial_stop_price - item.entry_price);
          if (!Number.isFinite(itemRisk) || itemRisk <= 0) {{
            continue;
          }}
          const entryX = tsLabel(entryBar.t);
          const exitX = tsLabel(exitBar.t);
          const signalX = tsLabel(signalBar.t);
          const tpPrice = isShortItem
            ? item.entry_price - (2.0 * itemRisk)
            : item.entry_price + (2.0 * itemRisk);

          visuals.pathSegments.push([
            {{ coord: [entryX, item.entry_price] }},
            {{ coord: [exitX, item.exit_price] }},
          ]);
          visuals.slSegments.push([
            {{ coord: [entryX, item.initial_stop_price] }},
            {{ coord: [exitX, item.initial_stop_price] }},
          ]);
          visuals.tpSegments.push([
            {{ coord: [entryX, tpPrice] }},
            {{ coord: [exitX, tpPrice] }},
          ]);

          if (item.breakeven_armed) {{
            const beStart = candles[item.breakeven_trigger_index ?? item.entry_index];
            if (beStart) {{
              visuals.beSegments.push([
                {{ coord: [tsLabel(beStart.t), item.entry_price] }},
                {{ coord: [exitX, item.entry_price] }},
              ]);
            }}
          }}

          visuals.signal.push([signalX, signalBar.c]);
          visuals.entry.push([entryX, item.entry_price]);
          visuals.exit.push([exitX, item.exit_price]);

          if (item.exit_reason === "stop_loss" || item.exit_reason === "managed_stop") {{
            visuals.slTag.push([exitX, item.exit_price]);
          }} else if (item.exit_reason === "breakeven_stop") {{
            visuals.beTag.push([exitX, item.exit_price]);
          }} else if (item.pnl_amount > 0) {{
            visuals.tpTag.push([exitX, item.exit_price]);
          }}
        }}
        return visuals;
      }};

      const tradeVisuals = buildTradeVisuals(visualTrades);
      const markerSizeSignal = isFocusMode ? 12 : 8;
      const markerSizeEntry = isFocusMode ? 16 : 10;
      const markerSizeExit = isFocusMode ? 16 : 10;
      const markerSizeTag = isFocusMode ? 14 : 10;
      const pathLineWidth = isFocusMode ? 2.1 : 1.6;
      const assistLineWidth = isFocusMode ? 1.5 : 1.1;

      chart.setOption({{
        animation: false,
        backgroundColor: "transparent",
        legend: {{
          top: 10,
          itemWidth: 18,
          itemHeight: 10,
          textStyle: {{ color: "#52606d" }},
          data: ["K缂?, "EMA55", "闂佽崵鍠愮划搴㈡櫠濡ゅ懎绠伴柛娑橈攻濞呯娀鏌ｅΟ铏逛粵鐎规洖顦伴妵鍕冀閵婏妇娈ょ紓?, "闂佽崵鍠愮划搴㈡櫠濡ゅ懎绠伴柛娑橈攻濞呯娀鏌ｉ弴鐘冲€?, "闂佽崵鍠愮划搴㈡櫠濡ゅ懎绠伴柛娑橈攻濞呯娀鏌ｉ弴鐔村亶", "闂佽崵鍠愮划搴㈡櫠濡ゅ懎绠伴柛娑橈攻濞呯娀鏌ｉ弴鐘冲瘧", "闂佽崵鍠愮划搴㈡櫠濡ゅ懎绠伴柛娑橈攻濞呯娀鏌ｅΟ铏癸紞闁崇懓绉归弻宥夊Ψ閵夈儲姣愭繝?, "闂佽崵鍠愮划搴㈡櫠濡ゅ懎绠伴柛娑橈攻濞呯娀鏌ｅΟ鐑樷枙闁搞倖娲熼弻宥堫檨闁告挻姘ㄧ划?, "闂佽崵鍠愮划搴㈡櫠濡ゅ懎绠伴柛娑橈攻濞呯娀鏌ｅΟ鑲╁笡闁绘帒顭烽弻宥堫檨闁告挾鍠庨悾?],
        }},
        tooltip: {{
          trigger: "axis",
          axisPointer: {{ type: "cross" }},
        }},
        grid: {{
          left: 58,
          right: 22,
          top: 56,
          bottom: 92,
        }},
        xAxis: {{
          type: "category",
          data: categories,
          boundaryGap: true,
          axisLine: {{ lineStyle: {{ color: "rgba(109, 120, 131, 0.8)" }} }},
          axisLabel: {{ color: "#6d7883", hideOverlap: true }},
        }},
        yAxis: {{
          scale: true,
          axisLine: {{ show: false }},
          splitLine: {{ lineStyle: {{ color: "rgba(109, 120, 131, 0.14)" }} }},
          axisLabel: {{ color: "#6d7883" }},
        }},
        dataZoom: [
          {{ type: "inside", start: startPercent, end: endPercent }},
          {{
            type: "slider",
            bottom: 28,
            height: 22,
            start: startPercent,
            end: endPercent,
            borderColor: "rgba(94, 74, 49, 0.16)",
            backgroundColor: "rgba(255, 255, 255, 0.55)",
            fillerColor: "rgba(29, 95, 209, 0.10)",
            dataBackground: {{
              lineStyle: {{ color: "rgba(109, 120, 131, 0.35)" }},
              areaStyle: {{ color: "rgba(109, 120, 131, 0.08)" }},
            }},
          }},
        ],
        series: [
          {{
            name: "K",
            type: "candlestick",
            data: ohlc,
            itemStyle: {{
              color: "#159570",
              color0: "#d14e2f",
              borderColor: "#159570",
              borderColor0: "#d14e2f",
            }},
            markArea: {{
              silent: true,
              itemStyle: {{ color: "rgba(168, 92, 42, 0.08)" }},
              data: [[
                {{ xAxis: tsLabel(entryCandle.t) }},
                {{ xAxis: tsLabel(exitCandle.t) }},
              ]],
            }},
          }},
          {{
            name: "EMA55",
            type: "line",
            data: ema,
            smooth: true,
            showSymbol: false,
            connectNulls: false,
            lineStyle: {{ width: 2, color: "#2563eb" }},
          }},
          {{
            name: "闂佽崵鍠愮划搴㈡櫠濡ゅ懎绠伴柛娑橈攻濞呯娀鏌ｉ弴鐘冲€?,
            type: "line",
            data: stopSegment,
            step: "end",
            showSymbol: false,
            symbolSize: 0,
            lineStyle: {{ width: 1.5, color: "#d14e2f", type: "dashed" }},
            label: {{ show: false }},
          }},
          {{
            name: "Trade Path",
            type: "line",
            data: tradePathSegment,
            showSymbol: false,
            symbolSize: 0,
            lineStyle: {{ width: 2.1, color: "#1d4ed8" }},
          }},
          {{
            name: "闂佽崵鍠愮划搴㈡櫠濡ゅ懎绠伴柛娑橈攻濞呯娀鏌ｉ弴鐔村亶",
            type: "line",
            data: breakevenSegment,
            step: "end",
            showSymbol: false,
            symbolSize: 0,
            lineStyle: {{ width: 1.6, color: "#1d4ed8" }},
            label: {{
              show: false,
              position: "end",
              formatter: "BE",
              color: "#1d4ed8",
              fontWeight: 700,
            }},
          }},
          {{
            name: "闂佽崵鍠愮划搴㈡櫠濡ゅ懎绠伴柛娑橈攻濞呯娀鏌ｉ弴鐘冲瘧",
            type: "line",
            data: tpSegment,
            step: "end",
            showSymbol: false,
            symbolSize: 0,
            lineStyle: {{ width: 1.5, color: "#159570", type: "dashed" }},
            label: {{ show: false }},
          }},
          {{
            name: "Signal",
            type: "scatter",
            symbolSize: 12,
            itemStyle: {{ color: "#f59e0b" }},
            data: [[tsLabel(signalCandle.t), signalCandle.c]],
          }},
          {{
            name: "Entry",
            type: "scatter",
            symbolSize: 16,
            symbol: "triangle",
            itemStyle: {{ color: "#1d4ed8" }},
            data: [[tsLabel(entryCandle.t), trade.entry_price]],
          }},
          {{
            name: "Exit",
            type: "scatter",
            symbolSize: 16,
            symbol: "circle",
            itemStyle: {{
              color: ["stop_loss", "managed_stop", "breakeven_stop"].includes(trade.exit_reason) ? "#d14e2f" : "#159570",
            }},
            data: [[tsLabel(exitCandle.t), trade.exit_price]],
          }},
          {{
            name: "SL Tag",
            type: "scatter",
            symbolSize: 14,
            itemStyle: {{ color: "#d14e2f" }},
            label: {{
              show: true,
              formatter: "SL",
              position: "right",
              color: "#d14e2f",
              fontWeight: 700,
            }},
            data: slMarkerData,
            tooltip: {{ show: false }},
          }},
          {{
            name: "BE Tag",
            type: "scatter",
            symbolSize: 14,
            itemStyle: {{ color: "#1d4ed8" }},
            label: {{
              show: true,
              formatter: "BE",
              position: "right",
              color: "#1d4ed8",
              fontWeight: 700,
            }},
            data: beMarkerData,
            tooltip: {{ show: false }},
          }},
          {{
            name: "TP Tag",
            type: "scatter",
            symbolSize: 14,
            itemStyle: {{ color: "#159570" }},
            label: {{
              show: true,
              formatter: "TP",
              position: "right",
              color: "#159570",
              fontWeight: 700,
            }},
            data: tpMarkerData,
            tooltip: {{ show: false }},
          }},
        ],
      }}, true);
    }}

    function renderChartAllTrades() {{
      const trade = currentTrade();
      if (!trade) {{
        chart.clear();
        return;
      }}

      const isFocusMode = viewModeSelect.value === "focus";
      const bounds = currentWindowBounds(trade);
      const windowCandles = candles.slice(bounds.start, bounds.end + 1);
      const categories = windowCandles.map((item) => tsLabel(item.t));
      const ohlc = windowCandles.map((item) => [item.o, item.c, item.l, item.h]);
      const ema = windowCandles.map((item) => item.e);
      const entryCandle = candles[trade.entry_index];
      const exitCandle = candles[trade.exit_index];
      const startPercent = 0;
      const endPercent = 100;
      const visualTrades = isFocusMode ? [trade] : strategyTrades();

      const buildTradeVisuals = (list) => {{
        const visuals = {{
          path: [],
          sl: [],
          be: [],
          tp: [],
          signal: [],
          entry: [],
          exit: [],
          slTag: [],
          beTag: [],
          tpTag: [],
        }};

        for (const item of list) {{
          const signalBar = candles[item.signal_index];
          const entryBar = candles[item.entry_index];
          const exitBar = candles[item.exit_index];
          if (!signalBar || !entryBar || !exitBar) {{
            continue;
          }}

          const itemRisk = Math.abs(item.initial_stop_price - item.entry_price);
          if (!Number.isFinite(itemRisk) || itemRisk <= 0) {{
            continue;
          }}

          const isShortItem = item.initial_stop_price >= item.entry_price;
          const signalX = tsLabel(signalBar.t);
          const entryX = tsLabel(entryBar.t);
          const exitX = tsLabel(exitBar.t);
          const tpPrice = isShortItem
            ? item.entry_price - (2.0 * itemRisk)
            : item.entry_price + (2.0 * itemRisk);

          visuals.path.push([entryX, item.entry_price], [exitX, item.exit_price], [null, null]);
          visuals.sl.push([entryX, item.initial_stop_price], [exitX, item.initial_stop_price], [null, null]);
          visuals.tp.push([entryX, tpPrice], [exitX, tpPrice], [null, null]);

          if (item.breakeven_armed) {{
            const beStart = candles[item.breakeven_trigger_index ?? item.entry_index];
            if (beStart) {{
              visuals.be.push([tsLabel(beStart.t), item.entry_price], [exitX, item.entry_price], [null, null]);
            }}
          }}

          visuals.signal.push([signalX, signalBar.c]);
          visuals.entry.push([entryX, item.entry_price]);
          visuals.exit.push([exitX, item.exit_price]);

          if (item.exit_reason === "stop_loss" || item.exit_reason === "managed_stop") {{
            visuals.slTag.push([exitX, item.exit_price]);
          }} else if (item.exit_reason === "breakeven_stop") {{
            visuals.beTag.push([exitX, item.exit_price]);
          }} else if (item.pnl_amount > 0) {{
            visuals.tpTag.push([exitX, item.exit_price]);
          }}
        }}

        return visuals;
      }};

      const tradeVisuals = buildTradeVisuals(visualTrades);
      const markerSizeSignal = isFocusMode ? 12 : 8;
      const markerSizeEntry = isFocusMode ? 16 : 10;
      const markerSizeExit = isFocusMode ? 16 : 10;
      const markerSizeTag = isFocusMode ? 14 : 10;
      const pathLineWidth = isFocusMode ? 2.1 : 1.5;
      const assistLineWidth = isFocusMode ? 1.5 : 1.1;

      chart.clear();
      chart.setOption({{
        animation: false,
        backgroundColor: "transparent",
        legend: {{
          top: 10,
          itemWidth: 18,
          itemHeight: 10,
          textStyle: {{ color: "#52606d" }},
          data: ["K", "EMA55", "Trade Path", "SL", "BE", "TP", "Signal", "Entry", "Exit"],
        }},
        tooltip: {{
          trigger: "axis",
          axisPointer: {{ type: "cross" }},
        }},
        grid: {{
          left: 58,
          right: 22,
          top: 56,
          bottom: 92,
        }},
        xAxis: {{
          type: "category",
          data: categories,
          boundaryGap: true,
          axisLine: {{ lineStyle: {{ color: "rgba(109, 120, 131, 0.8)" }} }},
          axisLabel: {{ color: "#6d7883", hideOverlap: true }},
        }},
        yAxis: {{
          scale: true,
          axisLine: {{ show: false }},
          splitLine: {{ lineStyle: {{ color: "rgba(109, 120, 131, 0.14)" }} }},
          axisLabel: {{ color: "#6d7883" }},
        }},
        dataZoom: [
          {{ type: "inside", start: startPercent, end: endPercent }},
          {{
            type: "slider",
            bottom: 28,
            height: 22,
            start: startPercent,
            end: endPercent,
            borderColor: "rgba(94, 74, 49, 0.16)",
            backgroundColor: "rgba(255, 255, 255, 0.55)",
            fillerColor: "rgba(29, 95, 209, 0.10)",
            dataBackground: {{
              lineStyle: {{ color: "rgba(109, 120, 131, 0.35)" }},
              areaStyle: {{ color: "rgba(109, 120, 131, 0.08)" }},
            }},
          }},
        ],
        series: [
          {{
            name: "K",
            type: "candlestick",
            data: ohlc,
            itemStyle: {{
              color: "#159570",
              color0: "#d14e2f",
              borderColor: "#159570",
              borderColor0: "#d14e2f",
            }},
            markArea: {{
              silent: true,
              itemStyle: {{ color: "rgba(168, 92, 42, 0.08)" }},
              data: [[
                {{ xAxis: tsLabel(entryCandle.t) }},
                {{ xAxis: tsLabel(exitCandle.t) }},
              ]],
            }},
          }},
          {{
            name: "EMA55",
            type: "line",
            data: ema,
            smooth: true,
            showSymbol: false,
            connectNulls: false,
            lineStyle: {{ width: 2, color: "#2563eb" }},
          }},
          {{
            name: "SL",
            type: "line",
            data: [],
            showSymbol: false,
            tooltip: {{ show: false }},
            markLine: {{
              symbol: ["none", "none"],
              animation: false,
              silent: true,
              label: {{ show: false }},
              lineStyle: {{ width: assistLineWidth, color: "#d14e2f", type: "dashed" }},
              data: tradeVisuals.slSegments,
            }},
          }},
          {{
            name: "Trade Path",
            type: "line",
            data: tradeVisuals.path,
            showSymbol: false,
            symbolSize: 0,
            connectNulls: false,
            lineStyle: {{ width: pathLineWidth, color: "#1d4ed8" }},
          }},
          {{
            name: "BE",
            type: "line",
            data: tradeVisuals.be,
            step: "end",
            showSymbol: false,
            symbolSize: 0,
            connectNulls: false,
            lineStyle: {{ width: assistLineWidth, color: "#1d4ed8" }},
            label: {{ show: false }},
          }},
          {{
            name: "TP",
            type: "line",
            data: tradeVisuals.tp,
            step: "end",
            showSymbol: false,
            symbolSize: 0,
            connectNulls: false,
            lineStyle: {{ width: assistLineWidth, color: "#159570", type: "dashed" }},
            label: {{ show: false }},
          }},
          {{
            name: "Signal",
            type: "scatter",
            symbolSize: markerSizeSignal,
            itemStyle: {{ color: "#f59e0b" }},
            data: tradeVisuals.signal,
          }},
          {{
            name: "Entry",
            type: "scatter",
            symbolSize: markerSizeEntry,
            symbol: "triangle",
            itemStyle: {{ color: "#1d4ed8" }},
            data: tradeVisuals.entry,
          }},
          {{
            name: "Exit",
            type: "scatter",
            symbolSize: markerSizeExit,
            symbol: "circle",
            itemStyle: {{ color: "#c66f54" }},
            data: tradeVisuals.exit,
          }},
          {{
            name: "SL Tag",
            type: "scatter",
            symbolSize: markerSizeTag,
            itemStyle: {{ color: "#d14e2f" }},
            label: {{
              show: true,
              formatter: "SL",
              position: "right",
              color: "#d14e2f",
              fontWeight: 700,
            }},
            data: tradeVisuals.slTag,
            tooltip: {{ show: false }},
          }},
          {{
            name: "BE Tag",
            type: "scatter",
            symbolSize: markerSizeTag,
            itemStyle: {{ color: "#1d4ed8" }},
            label: {{
              show: true,
              formatter: "BE",
              position: "right",
              color: "#1d4ed8",
              fontWeight: 700,
            }},
            data: tradeVisuals.beTag,
            tooltip: {{ show: false }},
          }},
          {{
            name: "TP Tag",
            type: "scatter",
            symbolSize: markerSizeTag,
            itemStyle: {{ color: "#159570" }},
            label: {{
              show: true,
              formatter: "TP",
              position: "right",
              color: "#159570",
              fontWeight: 700,
            }},
            data: tradeVisuals.tpTag,
            tooltip: {{ show: false }},
          }},
        ],
      }}, true);
    }}

    function renderChartSegmentedAllTrades() {{
      const trade = currentTrade();
      if (!trade) {{
        chart.clear();
        return;
      }}

      const isFocusMode = viewModeSelect.value === "focus";
      const bounds = currentWindowBounds(trade);
      const windowCandles = candles.slice(bounds.start, bounds.end + 1);
      const categories = windowCandles.map((item) => tsLabel(item.t));
      const ohlc = windowCandles.map((item) => [item.o, item.c, item.l, item.h]);
      const ema = windowCandles.map((item) => item.e);
      const entryCandle = candles[trade.entry_index];
      const exitCandle = candles[trade.exit_index];
      const zoomStartIndex = isFocusMode
        ? bounds.start
        : Math.floor((activeZoom.start / 100) * Math.max(1, candles.length - 1));
      const zoomEndIndex = isFocusMode
        ? bounds.end
        : Math.ceil((activeZoom.end / 100) * Math.max(1, candles.length - 1));
      const visualTrades = isFocusMode
        ? [trade]
        : strategyTrades().filter((item) => (
          (item.entry_index >= zoomStartIndex && item.entry_index <= zoomEndIndex)
          || (item.exit_index >= zoomStartIndex && item.exit_index <= zoomEndIndex)
        ));

      const visuals = {{
        pathSegments: [],
        slSegments: [],
        beSegments: [],
        tpSegments: [],
        signal: [],
        entry: [],
        exit: [],
        slTag: [],
        beTag: [],
        tpTag: [],
      }};

      for (const item of visualTrades) {{
        const signalBar = candles[item.signal_index];
        const entryBar = candles[item.entry_index];
        const exitBar = candles[item.exit_index];
        if (!signalBar || !entryBar || !exitBar) {{
          continue;
        }}

        const risk = Math.abs(item.initial_stop_price - item.entry_price);
        if (!Number.isFinite(risk) || risk <= 0) {{
          continue;
        }}

        const isShortItem = item.initial_stop_price >= item.entry_price;
        const signalX = tsLabel(signalBar.t);
        const entryX = tsLabel(entryBar.t);
        const exitX = tsLabel(exitBar.t);
        const entryRelIndex = item.entry_index - bounds.start;
        const exitRelIndex = item.exit_index - bounds.start;
        if (exitRelIndex < 0 || entryRelIndex > windowCandles.length - 1) {{
          continue;
        }}
        const tpPrice = isShortItem
          ? item.entry_price - (2.0 * risk)
          : item.entry_price + (2.0 * risk);

        visuals.pathSegments.push([entryRelIndex, item.entry_price, exitRelIndex, item.exit_price]);
        visuals.slSegments.push([entryRelIndex, item.initial_stop_price, exitRelIndex, item.initial_stop_price]);
        visuals.tpSegments.push([entryRelIndex, tpPrice, exitRelIndex, tpPrice]);

        if (item.breakeven_armed) {{
          const beStart = candles[item.breakeven_trigger_index ?? item.entry_index];
          if (beStart) {{
            const beRelIndex = (item.breakeven_trigger_index ?? item.entry_index) - bounds.start;
            visuals.beSegments.push([beRelIndex, item.entry_price, exitRelIndex, item.entry_price]);
          }}
        }}

        visuals.signal.push([signalX, signalBar.c]);
        visuals.entry.push([entryX, item.entry_price]);
        visuals.exit.push([exitX, item.exit_price]);

        if (item.exit_reason === "stop_loss" || item.exit_reason === "managed_stop") {{
          visuals.slTag.push([exitX, item.exit_price]);
        }} else if (item.exit_reason === "breakeven_stop") {{
          visuals.beTag.push([exitX, item.exit_price]);
        }} else if (item.pnl_amount > 0) {{
          visuals.tpTag.push([exitX, item.exit_price]);
        }}
      }}

      const markerSizeSignal = isFocusMode ? 12 : 8;
      const markerSizeEntry = isFocusMode ? 16 : 10;
      const markerSizeExit = isFocusMode ? 16 : 10;
      const markerSizeTag = isFocusMode ? 14 : 10;
      const pathLineWidth = isFocusMode ? 2.2 : 1.25;
      const assistLineWidth = isFocusMode ? 1.5 : 0.8;
      const chartZoomStart = isFocusMode ? 0 : activeZoom.start;
      const chartZoomEnd = isFocusMode ? 100 : activeZoom.end;
      const makeGraphicLineElements = (segments, color, width, lineType, opacity) => {{
        const dash = lineType === "dashed" ? [5, 4] : null;
        return segments.map((segment) => {{
          const start = chart.convertToPixel({{ xAxisIndex: 0, yAxisIndex: 0 }}, [segment[0], segment[1]]);
          const end = chart.convertToPixel({{ xAxisIndex: 0, yAxisIndex: 0 }}, [segment[2], segment[3]]);
          if (!start || !end || !Number.isFinite(start[0]) || !Number.isFinite(end[0])) {{
            return null;
          }}
          return {{
            type: "line",
            silent: true,
            z: 8,
            shape: {{
              x1: start[0],
              y1: start[1],
              x2: end[0],
              y2: end[1],
            }},
            style: {{
              stroke: color,
              lineWidth: width,
              lineDash: dash,
              opacity,
            }},
          }};
        }}).filter(Boolean);
      }};

      const renderTradeLineLayer = () => {{
        const grid = chart.getModel().getComponent("grid").coordinateSystem.getRect();
        const lineOpacity = isFocusMode ? 1 : 0.78;
        const children = [
          ...makeGraphicLineElements(visuals.slSegments, "#d14e2f", assistLineWidth, "dashed", lineOpacity),
          ...makeGraphicLineElements(visuals.tpSegments, "#159570", assistLineWidth, "dashed", lineOpacity),
          ...makeGraphicLineElements(visuals.beSegments, "#1d4ed8", assistLineWidth, "solid", lineOpacity),
          ...makeGraphicLineElements(visuals.pathSegments, "#7c3aed", pathLineWidth, "solid", lineOpacity),
        ];
        chart.setOption({{
          graphic: [{{
            id: "trade-line-layer",
            type: "group",
            silent: true,
            z: 8,
            clipPath: {{
              type: "rect",
              shape: {{
                x: grid.x,
                y: grid.y,
                width: grid.width,
                height: grid.height,
              }},
            }},
            children,
          }}],
        }}, false);
      }};
      const makeSegmentRenderer = () => () => null;

      chart.clear();
      renderingChart = true;
      chart.setOption({{
        animation: false,
        backgroundColor: "transparent",
        legend: {{
          top: 10,
          itemWidth: 18,
          itemHeight: 10,
          textStyle: {{ color: "#52606d" }},
          data: ["K", "EMA55", "Trade Path", "SL", "BE", "TP", "Signal", "Entry", "Exit"],
        }},
        tooltip: {{
          trigger: "axis",
          axisPointer: {{ type: "cross" }},
        }},
        grid: {{
          left: 58,
          right: 22,
          top: 56,
          bottom: 92,
        }},
        xAxis: {{
          type: "category",
          data: categories,
          boundaryGap: true,
          axisLine: {{ lineStyle: {{ color: "rgba(109, 120, 131, 0.8)" }} }},
          axisLabel: {{ color: "#6d7883", hideOverlap: true }},
        }},
        yAxis: {{
          scale: true,
          axisLine: {{ show: false }},
          splitLine: {{ lineStyle: {{ color: "rgba(109, 120, 131, 0.14)" }} }},
          axisLabel: {{ color: "#6d7883" }},
        }},
        dataZoom: [
          {{
            type: "inside",
            start: chartZoomStart,
            end: chartZoomEnd,
            filterMode: "weakFilter",
            moveOnMouseMove: true,
            moveOnMouseWheel: true,
            zoomOnMouseWheel: "shift",
            preventDefaultMouseMove: true,
          }},
          {{
            type: "slider",
            bottom: 28,
            height: 22,
            start: chartZoomStart,
            end: chartZoomEnd,
            filterMode: "weakFilter",
            borderColor: "rgba(94, 74, 49, 0.16)",
            backgroundColor: "rgba(255, 255, 255, 0.55)",
            fillerColor: "rgba(29, 95, 209, 0.10)",
            dataBackground: {{
              lineStyle: {{ color: "rgba(109, 120, 131, 0.35)" }},
              areaStyle: {{ color: "rgba(109, 120, 131, 0.08)" }},
            }},
          }},
        ],
        series: [
          {{
            name: "K",
            type: "candlestick",
            data: ohlc,
            itemStyle: {{
              color: "#159570",
              color0: "#d14e2f",
              borderColor: "#159570",
              borderColor0: "#d14e2f",
            }},
            markArea: {{
              silent: true,
              itemStyle: {{ color: "rgba(168, 92, 42, 0.08)" }},
              data: [[
                {{ xAxis: tsLabel(entryCandle.t) }},
                {{ xAxis: tsLabel(exitCandle.t) }},
              ]],
            }},
          }},
          {{
            name: "EMA55",
            type: "line",
            data: ema,
            smooth: true,
            showSymbol: false,
            connectNulls: false,
            lineStyle: {{ width: 2, color: "#2563eb" }},
          }},
          {{
            name: "Signal",
            type: "scatter",
            symbolSize: markerSizeSignal,
            itemStyle: {{ color: "#f59e0b" }},
            data: visuals.signal,
          }},
          {{
            name: "Entry",
            type: "scatter",
            symbolSize: markerSizeEntry,
            symbol: "triangle",
            itemStyle: {{ color: "#1d4ed8" }},
            data: visuals.entry,
          }},
          {{
            name: "Exit",
            type: "scatter",
            symbolSize: markerSizeExit,
            symbol: "circle",
            itemStyle: {{ color: "#c66f54" }},
            data: visuals.exit,
          }},
          {{
            name: "SL Tag",
            type: "scatter",
            symbolSize: markerSizeTag,
            itemStyle: {{ color: "#d14e2f" }},
            label: {{
              show: true,
              formatter: "SL",
              position: "right",
              color: "#d14e2f",
              fontWeight: 700,
            }},
            data: visuals.slTag,
            tooltip: {{ show: false }},
          }},
          {{
            name: "BE Tag",
            type: "scatter",
            symbolSize: markerSizeTag,
            itemStyle: {{ color: "#1d4ed8" }},
            label: {{
              show: true,
              formatter: "BE",
              position: "right",
              color: "#1d4ed8",
              fontWeight: 700,
            }},
            data: visuals.beTag,
            tooltip: {{ show: false }},
          }},
          {{
            name: "TP Tag",
            type: "scatter",
            symbolSize: markerSizeTag,
            itemStyle: {{ color: "#159570" }},
            label: {{
              show: true,
              formatter: "TP",
              position: "right",
              color: "#159570",
              fontWeight: 700,
            }},
            data: visuals.tpTag,
            tooltip: {{ show: false }},
          }},
        ],
      }}, true);
      window.setTimeout(() => {{
        renderingChart = false;
      }}, 0);
    }}

    renderChart = renderChartSegmentedAllTrades;
    renderChartAllTrades = renderChartSegmentedAllTrades;

    function refresh() {{
      renderTradeStats(currentTrade());
      renderChartSegmentedAllTrades();
    }}

    buildSummaryCards();
    populateStrategies();
    populateTrades();
    setSidebarHidden(loadSidebarState());
    refresh();

    strategySelect.addEventListener("change", () => {{
      populateTrades();
      refresh();
    }});
    viewModeSelect.addEventListener("change", refresh);
    tradeSelect.addEventListener("change", refresh);
    beforeBarsInput.addEventListener("change", refresh);
    afterBarsInput.addEventListener("change", refresh);
    sidebarToggle.addEventListener("click", () => {{
      setSidebarHidden(!workspaceGrid.classList.contains("chart-only"));
    }});
    chart.on("dataZoom", (event) => {{
      if (renderingChart || viewModeSelect.value === "focus") {{
        return;
      }}
      const payload = event.batch && event.batch.length ? event.batch[0] : event;
      if (payload.start == null || payload.end == null) {{
        return;
      }}
      activeZoom = {{ start: payload.start, end: payload.end }};
      renderChartSegmentedAllTrades();
    }});
    window.addEventListener("resize", () => chart.resize());
  </script>
</body>
</html>"""


if __name__ == "__main__":
    main()
