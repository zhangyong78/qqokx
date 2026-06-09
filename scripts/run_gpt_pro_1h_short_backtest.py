from __future__ import annotations

import argparse
import json
import math
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from okx_quant.app_paths import configure_data_root, data_root
from okx_quant.candle_cache import load_candle_cache


DEFAULT_REPORT_DIR = ROOT / "reports" / "gpt_pro_1h_short_backtest"


@dataclass(frozen=True)
class StrategyConfig:
    initial_equity: float = 10_000.0
    risk_pct: float = 0.01
    fixed_risk_amount: float | None = None
    fee_rate: float = 0.0005
    max_leverage: float = 3.0
    ema_fast: int = 20
    ema_mid: int = 50
    atr_period: int = 14
    rsi_period: int = 14
    volume_ma_period: int = 20
    support_window: int = 20
    ema4h_fast: int = 20
    ema4h_mid: int = 50
    ema4h_slow: int = 200
    require_4h_ema_slope_down: bool = True
    breakdown_atr_mult: float = 0.20
    retest_atr_mult: float = 0.30
    stop_atr_buffer: float = 0.25
    max_retest_bars: int = 8
    rsi_short_threshold: float = 50.0
    use_volume_filter: bool = False
    volume_mult: float = 1.10
    tp1_r: float = 1.0
    tp2_r: float = 2.0
    tp3_r: float = 3.0
    tp1_close_pct: float = 0.30
    tp2_close_pct: float = 0.40
    tp3_close_pct: float = 0.30
    move_stop_to_breakeven_after_tp1: bool = True
    fee_adjusted_breakeven: bool = True
    loss_streak_stop: int = 0
    cooldown_days: int = 0


@dataclass(frozen=True)
class RuntimeArgs:
    inst_id: str
    bar: str
    candle_limit: int
    report_dir: Path
    data_dir: Path | None
    out_prefix: str
    config: StrategyConfig


@dataclass
class PendingShortSetup:
    breakdown_idx: int
    breakdown_time: pd.Timestamp
    breakdown_level: float
    expire_idx: int


@dataclass
class ScheduledShortEntry:
    signal_idx: int
    signal_time: pd.Timestamp
    entry_idx: int
    breakdown_level: float
    retest_high: float
    atr_at_signal: float
    stop_price: float


@dataclass
class ShortPosition:
    entry_idx: int
    entry_time: pd.Timestamp
    entry_price: float
    initial_stop: float
    current_stop: float
    tp1: float
    tp2: float
    tp3: float
    qty: float
    remaining_qty: float
    risk_per_unit: float
    equity_before: float
    entry_fee: float
    realized_pnl: float
    tp1_hit: bool = False
    tp2_hit: bool = False
    tp3_hit: bool = False
    exit_fills: list[dict[str, Any]] | None = None

    def __post_init__(self) -> None:
        if self.exit_fills is None:
            self.exit_fills = []


def parse_args(argv: list[str] | None = None) -> RuntimeArgs:
    parser = argparse.ArgumentParser(description="Run the GPT Pro 1H short strategy on local full-history candle cache.")
    parser.add_argument("--inst-id", default="BTC-USDT-SWAP", help="Instrument id, default BTC-USDT-SWAP.")
    parser.add_argument("--bar", default="1H", help="Entry timeframe. This script expects 1H data.")
    parser.add_argument("--report-dir", help="Output directory. Defaults to reports/gpt_pro_1h_short_backtest.")
    parser.add_argument("--data-dir", help="QQOKX data root. Defaults to the app data root.")
    parser.add_argument("--out-prefix", default="btc_gpt_pro_1h_short", help="Prefix for exported files.")
    parser.add_argument("--candle-limit", type=int, default=0, help="0 means full history, positive means latest N candles.")
    parser.add_argument("--initial-equity", type=float, default=10_000.0)
    parser.add_argument("--risk-pct", type=float, default=0.01)
    parser.add_argument("--fixed-risk-amount", type=float, help="Fixed risk amount per trade. Overrides risk_pct when provided.")
    parser.add_argument("--fee-rate", type=float, default=0.0005)
    parser.add_argument("--max-leverage", type=float, default=3.0)
    parser.add_argument("--support-window", type=int, default=20)
    parser.add_argument("--breakdown-atr", type=float, default=0.20)
    parser.add_argument("--retest-atr", type=float, default=0.30)
    parser.add_argument("--stop-atr-buffer", type=float, default=0.25)
    parser.add_argument("--max-retest-bars", type=int, default=8)
    parser.add_argument("--rsi-short-threshold", type=float, default=50.0)
    parser.add_argument("--use-volume-filter", action="store_true")
    parser.add_argument("--volume-mult", type=float, default=1.10)
    parser.add_argument("--no-4h-slope-filter", action="store_true")
    parser.add_argument("--loss-streak-stop", type=int, default=0, help="Pause after this many consecutive losing trades. 0 disables.")
    parser.add_argument("--cooldown-days", type=int, default=0, help="Cooldown days after the loss-streak stop triggers.")
    args = parser.parse_args(argv)
    config = StrategyConfig(
        initial_equity=args.initial_equity,
        risk_pct=args.risk_pct,
        fixed_risk_amount=args.fixed_risk_amount,
        fee_rate=args.fee_rate,
        max_leverage=args.max_leverage,
        support_window=args.support_window,
        breakdown_atr_mult=args.breakdown_atr,
        retest_atr_mult=args.retest_atr,
        stop_atr_buffer=args.stop_atr_buffer,
        max_retest_bars=args.max_retest_bars,
        rsi_short_threshold=args.rsi_short_threshold,
        use_volume_filter=args.use_volume_filter,
        volume_mult=args.volume_mult,
        require_4h_ema_slope_down=not args.no_4h_slope_filter,
        loss_streak_stop=max(int(args.loss_streak_stop), 0),
        cooldown_days=max(int(args.cooldown_days), 0),
    )
    return RuntimeArgs(
        inst_id=str(args.inst_id).strip().upper(),
        bar=str(args.bar).strip(),
        candle_limit=max(int(args.candle_limit), 0),
        report_dir=Path(args.report_dir).expanduser().resolve() if args.report_dir else DEFAULT_REPORT_DIR,
        data_dir=Path(args.data_dir).expanduser().resolve() if args.data_dir else None,
        out_prefix=str(args.out_prefix).strip(),
        config=config,
    )


def ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False, min_periods=period).mean()


def atr(df: pd.DataFrame, period: int) -> pd.Series:
    prev_close = df["close"].shift(1)
    tr = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - prev_close).abs(),
            (df["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return tr.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()


def rsi(series: pd.Series, period: int) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    out = 100 - (100 / (1 + rs))
    return out.fillna(50.0)


def candles_to_frame(inst_id: str, bar: str, *, candle_limit: int = 0) -> pd.DataFrame:
    fetch_limit = None if candle_limit <= 0 else candle_limit
    candles = [item for item in load_candle_cache(inst_id, bar, limit=fetch_limit) if item.confirmed]
    if not candles:
        raise RuntimeError(f"no confirmed candles found for {inst_id} {bar} under {data_root()}")
    rows = [
        {
            "timestamp": pd.to_datetime(int(candle.ts), unit="ms", utc=True),
            "ts": int(candle.ts),
            "open": float(candle.open),
            "high": float(candle.high),
            "low": float(candle.low),
            "close": float(candle.close),
            "volume": float(candle.volume),
        }
        for candle in candles
    ]
    df = pd.DataFrame(rows).sort_values("timestamp").drop_duplicates("timestamp", keep="last").reset_index(drop=True)
    return df


def add_1h_features(df: pd.DataFrame, cfg: StrategyConfig) -> pd.DataFrame:
    out = df.copy()
    out["ema20"] = ema(out["close"], cfg.ema_fast)
    out["ema50"] = ema(out["close"], cfg.ema_mid)
    out["atr14"] = atr(out, cfg.atr_period)
    out["rsi14"] = rsi(out["close"], cfg.rsi_period)
    out["volume_ma20"] = out["volume"].rolling(cfg.volume_ma_period, min_periods=cfg.volume_ma_period).mean()
    out["support"] = out["low"].rolling(cfg.support_window, min_periods=cfg.support_window).min().shift(1)
    return out


def add_4h_features_without_lookahead(df_1h: pd.DataFrame, cfg: StrategyConfig) -> pd.DataFrame:
    base = df_1h.set_index("timestamp")[["open", "high", "low", "close", "volume"]]
    df_4h = base.resample("4h", label="left", closed="left").agg(
        {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
    ).dropna()
    df_4h["ema20_4h"] = ema(df_4h["close"], cfg.ema4h_fast)
    df_4h["ema50_4h"] = ema(df_4h["close"], cfg.ema4h_mid)
    df_4h["ema200_4h"] = ema(df_4h["close"], cfg.ema4h_slow)
    df_4h["ema20_4h_slope_down"] = df_4h["ema20_4h"] < df_4h["ema20_4h"].shift(1)
    trend = (df_4h["close"] < df_4h["ema200_4h"]) & (df_4h["ema20_4h"] < df_4h["ema50_4h"])
    if cfg.require_4h_ema_slope_down:
        trend = trend & df_4h["ema20_4h_slope_down"]
    features = df_4h[
        ["close", "ema20_4h", "ema50_4h", "ema200_4h", "ema20_4h_slope_down"]
    ].rename(columns={"close": "close_4h"})
    features["trend_short_4h"] = trend
    features.index = features.index + pd.Timedelta(hours=4)
    aligned = features.reindex(df_1h["timestamp"], method="ffill")
    aligned = aligned.reset_index(drop=True)
    out = df_1h.reset_index(drop=True).copy()
    for column in aligned.columns:
        out[column] = aligned[column]
    return out


def prepare_features(df: pd.DataFrame, cfg: StrategyConfig) -> pd.DataFrame:
    return add_4h_features_without_lookahead(add_1h_features(df, cfg), cfg)


def is_valid_number(value: Any) -> bool:
    try:
        return value is not None and np.isfinite(float(value))
    except Exception:
        return False


def calc_short_qty(equity: float, entry_price: float, stop_price: float, cfg: StrategyConfig) -> tuple[float, float, float]:
    risk_per_unit = stop_price - entry_price
    if risk_per_unit <= 0:
        return 0.0, risk_per_unit, 0.0
    risk_amount = float(cfg.fixed_risk_amount) if cfg.fixed_risk_amount is not None else equity * cfg.risk_pct
    qty_by_risk = risk_amount / risk_per_unit
    max_notional = equity * cfg.max_leverage
    max_qty_by_leverage = max_notional / entry_price
    qty = min(qty_by_risk, max_qty_by_leverage)
    notional = qty * entry_price
    return qty, risk_per_unit, notional


def close_short_qty(
    position: ShortPosition,
    qty_to_close: float,
    exit_price: float,
    exit_time: pd.Timestamp,
    reason: str,
    cfg: StrategyConfig,
) -> None:
    qty = min(qty_to_close, position.remaining_qty)
    if qty <= 0:
        return
    gross_pnl = qty * (position.entry_price - exit_price)
    exit_fee = qty * exit_price * cfg.fee_rate
    net_pnl = gross_pnl - exit_fee
    position.realized_pnl += net_pnl
    position.remaining_qty -= qty
    assert position.exit_fills is not None
    position.exit_fills.append(
        {
            "time": str(exit_time),
            "price": float(exit_price),
            "qty": float(qty),
            "gross_pnl": float(gross_pnl),
            "fee": float(exit_fee),
            "net_pnl_after_exit_fee": float(net_pnl),
            "reason": reason,
        }
    )


def finalize_trade_record(position: ShortPosition, final_time: pd.Timestamp, final_reason: str) -> dict[str, Any]:
    fills = position.exit_fills or []
    total_exit_qty = sum(float(fill["qty"]) for fill in fills)
    weighted_exit = (
        sum(float(fill["price"]) * float(fill["qty"]) for fill in fills) / total_exit_qty if total_exit_qty > 0 else np.nan
    )
    initial_risk_usdt = position.qty * position.risk_per_unit
    pnl_r = position.realized_pnl / initial_risk_usdt if initial_risk_usdt > 0 else np.nan
    return {
        "entry_time": str(position.entry_time),
        "entry_price": float(position.entry_price),
        "initial_stop": float(position.initial_stop),
        "final_stop": float(position.current_stop),
        "tp1": float(position.tp1),
        "tp2": float(position.tp2),
        "tp3": float(position.tp3),
        "qty": float(position.qty),
        "notional": float(position.qty * position.entry_price),
        "leverage_used": float((position.qty * position.entry_price) / position.equity_before),
        "entry_fee": float(position.entry_fee),
        "exit_time": str(final_time),
        "avg_exit_price": float(weighted_exit),
        "pnl": float(position.realized_pnl),
        "pnl_R": float(pnl_r),
        "return_on_equity_before": float(position.realized_pnl / position.equity_before),
        "exit_reason": final_reason,
        "tp1_hit": bool(position.tp1_hit),
        "tp2_hit": bool(position.tp2_hit),
        "tp3_hit": bool(position.tp3_hit),
        "partial_exits": json.dumps(fills, ensure_ascii=False),
    }


def mark_to_market_equity(closed_equity: float, position: ShortPosition | None, current_close: float, cfg: StrategyConfig) -> float:
    if position is None:
        return closed_equity
    unrealized = position.remaining_qty * (position.entry_price - current_close)
    estimated_exit_fee = position.remaining_qty * current_close * cfg.fee_rate
    return closed_equity + position.realized_pnl + unrealized - estimated_exit_fee


def maybe_open_scheduled_short(
    i: int,
    timestamp: pd.Timestamp,
    row: pd.Series,
    scheduled: ScheduledShortEntry | None,
    equity: float,
    cfg: StrategyConfig,
) -> tuple[ShortPosition | None, ScheduledShortEntry | None, str | None]:
    if scheduled is None or i != scheduled.entry_idx:
        return None, scheduled, None
    entry_price = float(row["open"])
    stop_price = float(scheduled.stop_price)
    if entry_price >= stop_price:
        return None, None, f"{timestamp}: skipped short because next open {entry_price:.6f} >= stop {stop_price:.6f}"
    qty, risk_per_unit, notional = calc_short_qty(equity, entry_price, stop_price, cfg)
    if qty <= 0 or notional <= 0:
        return None, None, f"{timestamp}: skipped short because position sizing was invalid"
    tp1 = entry_price - cfg.tp1_r * risk_per_unit
    tp2 = entry_price - cfg.tp2_r * risk_per_unit
    tp3 = entry_price - cfg.tp3_r * risk_per_unit
    entry_fee = qty * entry_price * cfg.fee_rate
    position = ShortPosition(
        entry_idx=i,
        entry_time=timestamp,
        entry_price=entry_price,
        initial_stop=stop_price,
        current_stop=stop_price,
        tp1=tp1,
        tp2=tp2,
        tp3=tp3,
        qty=qty,
        remaining_qty=qty,
        risk_per_unit=risk_per_unit,
        equity_before=equity,
        entry_fee=entry_fee,
        realized_pnl=-entry_fee,
    )
    return position, None, None


def manage_open_short(
    position: ShortPosition,
    timestamp: pd.Timestamp,
    row: pd.Series,
    cfg: StrategyConfig,
) -> tuple[ShortPosition | None, dict[str, Any] | None]:
    open_price = float(row["open"])
    high = float(row["high"])
    low = float(row["low"])
    stop_fill_price = None
    if open_price >= position.current_stop:
        stop_fill_price = open_price
    elif high >= position.current_stop:
        stop_fill_price = position.current_stop
    if stop_fill_price is not None:
        reason = "breakeven_or_trailing_stop" if position.tp1_hit else "stop_loss"
        close_short_qty(position, position.remaining_qty, stop_fill_price, timestamp, reason, cfg)
        return None, finalize_trade_record(position, timestamp, reason)

    original_qty = position.qty
    if (not position.tp1_hit) and low <= position.tp1:
        close_short_qty(position, original_qty * cfg.tp1_close_pct, position.tp1, timestamp, "tp1_1R", cfg)
        position.tp1_hit = True
        if cfg.move_stop_to_breakeven_after_tp1:
            candidate_stop = position.entry_price
            if cfg.fee_adjusted_breakeven:
                candidate_stop = position.entry_price * (1 - 2 * cfg.fee_rate)
                current_close = float(row["close"])
                if candidate_stop <= current_close:
                    candidate_stop = position.entry_price
            position.current_stop = min(position.current_stop, candidate_stop)

    if (not position.tp2_hit) and low <= position.tp2:
        close_short_qty(position, original_qty * cfg.tp2_close_pct, position.tp2, timestamp, "tp2_2R", cfg)
        position.tp2_hit = True

    if (not position.tp3_hit) and low <= position.tp3:
        close_short_qty(position, position.remaining_qty, position.tp3, timestamp, "tp3_3R", cfg)
        position.tp3_hit = True
        return None, finalize_trade_record(position, timestamp, "tp3_3R")

    if position.remaining_qty <= position.qty * 1e-10:
        return None, finalize_trade_record(position, timestamp, "fully_closed")
    return position, None


def check_breakdown_signal(row: pd.Series, cfg: StrategyConfig) -> bool:
    if any(not is_valid_number(row.get(column)) for column in ("support", "atr14")):
        return False
    if not bool(row.get("trend_short_4h", False)):
        return False
    return float(row["close"]) < float(row["support"]) - cfg.breakdown_atr_mult * float(row["atr14"])


def check_retest_and_rejection(row: pd.Series, setup: PendingShortSetup, cfg: StrategyConfig) -> bool:
    for column in ("atr14", "ema20", "rsi14", "volume_ma20"):
        if not is_valid_number(row.get(column)):
            return False
    if not bool(row.get("trend_short_4h", False)):
        return False
    breakdown_level = float(setup.breakdown_level)
    atr_value = float(row["atr14"])
    retest = float(row["high"]) >= breakdown_level - cfg.retest_atr_mult * atr_value
    rejection = (
        float(row["close"]) < float(row["open"])
        and float(row["close"]) < breakdown_level
        and float(row["close"]) < float(row["ema20"])
        and float(row["rsi14"]) < cfg.rsi_short_threshold
    )
    if cfg.use_volume_filter:
        rejection = rejection and float(row["volume"]) > float(row["volume_ma20"]) * cfg.volume_mult
    return bool(retest and rejection)


def run_backtest(df: pd.DataFrame, cfg: StrategyConfig) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    if len(df) < 300:
        raise ValueError("need at least 300 1H candles to run this strategy")
    equity = float(cfg.initial_equity)
    pending: PendingShortSetup | None = None
    scheduled: ScheduledShortEntry | None = None
    position: ShortPosition | None = None
    trades: list[dict[str, Any]] = []
    equity_curve_rows: list[dict[str, Any]] = []
    skipped_entries: list[str] = []
    consecutive_losses = 0
    cooldown_until: pd.Timestamp | None = None
    cooldown_trigger_count = 0
    timestamps = list(df["timestamp"])

    for i, timestamp in enumerate(timestamps):
        row = df.iloc[i]
        if position is None and scheduled is not None and i == scheduled.entry_idx:
            position, scheduled, msg = maybe_open_scheduled_short(i, timestamp, row, scheduled, equity, cfg)
            if msg:
                skipped_entries.append(msg)

        if position is not None:
            position, record = manage_open_short(position, timestamp, row, cfg)
            if record is not None:
                equity += float(record["pnl"])
                trades.append(record)
                if float(record["pnl"]) < 0:
                    consecutive_losses += 1
                else:
                    consecutive_losses = 0
                if cfg.loss_streak_stop > 0 and cfg.cooldown_days > 0 and consecutive_losses >= cfg.loss_streak_stop:
                    cooldown_until = timestamp + pd.Timedelta(days=cfg.cooldown_days)
                    cooldown_trigger_count += 1
                    consecutive_losses = 0
                position = None
                pending = None
                scheduled = None

        equity_curve_rows.append(
            {
                "timestamp": timestamp,
                "equity": float(mark_to_market_equity(equity, position, float(row["close"]), cfg)),
                "closed_equity": float(equity),
                "has_position": position is not None,
                "cooldown_active": bool(position is None and cooldown_until is not None and timestamp < cooldown_until),
            }
        )

        if position is not None or scheduled is not None:
            continue
        if cooldown_until is not None and timestamp < cooldown_until:
            continue
        if i >= len(df) - 1:
            continue
        if pending is not None and i > pending.expire_idx:
            pending = None
        if pending is not None and i > pending.breakdown_idx:
            if check_retest_and_rejection(row, pending, cfg):
                scheduled = ScheduledShortEntry(
                    signal_idx=i,
                    signal_time=timestamp,
                    entry_idx=i + 1,
                    breakdown_level=float(pending.breakdown_level),
                    retest_high=float(row["high"]),
                    atr_at_signal=float(row["atr14"]),
                    stop_price=float(row["high"]) + cfg.stop_atr_buffer * float(row["atr14"]),
                )
                pending = None
                continue
        if check_breakdown_signal(row, cfg):
            pending = PendingShortSetup(
                breakdown_idx=i,
                breakdown_time=timestamp,
                breakdown_level=float(row["support"]),
                expire_idx=min(i + cfg.max_retest_bars, len(df) - 1),
            )

    if position is not None:
        last_row = df.iloc[-1]
        close_short_qty(position, position.remaining_qty, float(last_row["close"]), last_row["timestamp"], "end_of_data", cfg)
        record = finalize_trade_record(position, last_row["timestamp"], "end_of_data")
        equity += float(record["pnl"])
        trades.append(record)
        if equity_curve_rows:
            equity_curve_rows[-1]["equity"] = float(equity)
            equity_curve_rows[-1]["closed_equity"] = float(equity)
            equity_curve_rows[-1]["has_position"] = False

    trades_df = pd.DataFrame(trades)
    equity_curve = pd.DataFrame(equity_curve_rows)
    metrics = calculate_metrics(trades_df, equity_curve, cfg)
    metrics["skipped_entries_count"] = len(skipped_entries)
    metrics["skipped_entries_sample"] = skipped_entries[:10]
    metrics["cooldown_trigger_count"] = cooldown_trigger_count
    metrics["loss_guard"] = (
        f"{cfg.loss_streak_stop} losses -> pause {cfg.cooldown_days}d"
        if cfg.loss_streak_stop > 0 and cfg.cooldown_days > 0
        else "off"
    )
    metrics["data_start_utc"] = str(df["timestamp"].iloc[0])
    metrics["data_end_utc"] = str(df["timestamp"].iloc[-1])
    metrics["candle_count"] = int(len(df))
    return trades_df, equity_curve, metrics


def calculate_max_drawdown(equity: pd.Series) -> tuple[float, pd.Timestamp | None, pd.Timestamp | None]:
    if equity.empty:
        return 0.0, None, None
    rolling_peak = equity.cummax()
    drawdown = equity / rolling_peak - 1.0
    max_dd = float(drawdown.min())
    dd_end = drawdown.idxmin()
    if pd.isna(dd_end):
        return max_dd, None, None
    dd_start = equity.loc[:dd_end].idxmax()
    ts_series = equity.index if isinstance(equity.index, pd.DatetimeIndex) else None
    if ts_series is None:
        return max_dd, None, None
    return max_dd, dd_start, dd_end


def longest_losing_streak(pnl_series: pd.Series) -> int:
    best = 0
    current = 0
    for pnl in pnl_series:
        if pnl < 0:
            current += 1
            best = max(best, current)
        else:
            current = 0
    return best


def calculate_metrics(trades_df: pd.DataFrame, equity_curve: pd.DataFrame, cfg: StrategyConfig) -> dict[str, Any]:
    indexed = equity_curve.set_index("timestamp") if not equity_curve.empty else equity_curve
    final_equity = float(indexed["equity"].iloc[-1]) if not indexed.empty else cfg.initial_equity
    total_return = final_equity / cfg.initial_equity - 1.0
    max_dd, dd_start, dd_end = calculate_max_drawdown(indexed["equity"]) if not indexed.empty else (0.0, None, None)
    metrics: dict[str, Any] = {
        "initial_equity": cfg.initial_equity,
        "final_equity": final_equity,
        "total_return_pct": total_return * 100,
        "max_drawdown_pct": max_dd * 100,
        "max_drawdown_start": str(dd_start) if dd_start is not None else None,
        "max_drawdown_end": str(dd_end) if dd_end is not None else None,
        "trade_count": 0,
        "win_rate_pct": None,
        "profit_factor": None,
        "avg_pnl": None,
        "avg_R": None,
        "avg_win": None,
        "avg_loss": None,
        "win_loss_ratio": None,
        "longest_losing_streak": 0,
        "gross_profit": 0.0,
        "gross_loss": 0.0,
        "config": asdict(cfg),
    }
    if trades_df.empty:
        return metrics
    pnl = trades_df["pnl"].astype(float)
    pnl_r = trades_df["pnl_R"].astype(float)
    wins = pnl[pnl > 0]
    losses = pnl[pnl < 0]
    gross_profit = float(wins.sum()) if not wins.empty else 0.0
    gross_loss = float(-losses.sum()) if not losses.empty else 0.0
    metrics.update(
        {
            "trade_count": int(len(trades_df)),
            "win_rate_pct": float((pnl > 0).mean() * 100),
            "profit_factor": float(gross_profit / gross_loss) if gross_loss > 0 else math.inf,
            "avg_pnl": float(pnl.mean()),
            "avg_R": float(pnl_r.mean()),
            "avg_win": float(wins.mean()) if not wins.empty else None,
            "avg_loss": float(losses.mean()) if not losses.empty else None,
            "win_loss_ratio": float(wins.mean() / abs(losses.mean())) if (not wins.empty and not losses.empty) else None,
            "longest_losing_streak": int(longest_losing_streak(pnl)),
            "gross_profit": gross_profit,
            "gross_loss": gross_loss,
        }
    )
    return metrics


def monthly_returns(equity_curve: pd.DataFrame) -> pd.DataFrame:
    if equity_curve.empty:
        return pd.DataFrame(columns=["month", "monthly_return_pct", "month_end_equity"])
    indexed = equity_curve.set_index("timestamp")
    monthly_equity = indexed["equity"].resample("ME").last().dropna()
    if monthly_equity.empty:
        return pd.DataFrame(columns=["month", "monthly_return_pct", "month_end_equity"])
    monthly_ret = monthly_equity.pct_change()
    if len(monthly_ret) > 0:
        monthly_ret.iloc[0] = monthly_equity.iloc[0] / indexed["equity"].iloc[0] - 1.0
    return pd.DataFrame(
        {
            "month": monthly_equity.index.astype(str),
            "monthly_return_pct": monthly_ret.values * 100,
            "month_end_equity": monthly_equity.values,
        }
    )


def save_outputs(
    report_dir: Path,
    out_prefix: str,
    trades_df: pd.DataFrame,
    equity_curve: pd.DataFrame,
    metrics: dict[str, Any],
) -> dict[str, Path]:
    report_dir.mkdir(parents=True, exist_ok=True)
    trades_path = report_dir / f"{out_prefix}_trades.csv"
    equity_path = report_dir / f"{out_prefix}_equity_curve.csv"
    monthly_path = report_dir / f"{out_prefix}_monthly_returns.csv"
    metrics_path = report_dir / f"{out_prefix}_metrics.json"
    summary_path = report_dir / f"{out_prefix}_summary.md"
    manifest_path = report_dir / f"{out_prefix}_run_manifest.json"

    trades_df.to_csv(trades_path, index=False, encoding="utf-8-sig")
    equity_curve.to_csv(equity_path, index=False, encoding="utf-8-sig")
    monthly_returns(equity_curve).to_csv(monthly_path, index=False, encoding="utf-8-sig")
    metrics_path.write_text(json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8")
    summary_path.write_text(build_summary_markdown(metrics), encoding="utf-8")
    manifest_path.write_text(
        json.dumps(
            {
                "output_files": {
                    "trades": str(trades_path),
                    "equity_curve": str(equity_path),
                    "monthly_returns": str(monthly_path),
                    "metrics": str(metrics_path),
                    "summary": str(summary_path),
                },
                "metrics_snapshot": {
                    "final_equity": metrics["final_equity"],
                    "total_return_pct": metrics["total_return_pct"],
                    "max_drawdown_pct": metrics["max_drawdown_pct"],
                    "trade_count": metrics["trade_count"],
                    "win_rate_pct": metrics["win_rate_pct"],
                    "profit_factor": metrics["profit_factor"],
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return {
        "trades": trades_path,
        "equity_curve": equity_path,
        "monthly_returns": monthly_path,
        "metrics": metrics_path,
        "summary": summary_path,
        "manifest": manifest_path,
    }


def build_summary_markdown(metrics: dict[str, Any]) -> str:
    profit_factor = metrics["profit_factor"]
    pf_text = "inf" if profit_factor is not None and math.isinf(float(profit_factor)) else f"{float(profit_factor):.4f}"
    lines = [
        "# GPT Pro 1H Short Backtest",
        "",
        f"- Data range: `{metrics['data_start_utc']}` -> `{metrics['data_end_utc']}`",
        f"- Candles: `{metrics['candle_count']}`",
        f"- Initial equity: `{metrics['initial_equity']:.2f}`",
        f"- Loss guard: `{metrics['loss_guard']}`",
        f"- Final equity: `{metrics['final_equity']:.2f}`",
        f"- Total return: `{metrics['total_return_pct']:.2f}%`",
        f"- Max drawdown: `{metrics['max_drawdown_pct']:.2f}%`",
        f"- Trades: `{metrics['trade_count']}`",
        f"- Win rate: `{0.0 if metrics['win_rate_pct'] is None else metrics['win_rate_pct']:.2f}%`",
        f"- Profit factor: `{pf_text}`",
        f"- Avg pnl: `{0.0 if metrics['avg_pnl'] is None else metrics['avg_pnl']:.4f}`",
        f"- Avg R: `{0.0 if metrics['avg_R'] is None else metrics['avg_R']:.4f}`",
        f"- Longest losing streak: `{metrics['longest_losing_streak']}`",
        f"- Skipped entries: `{metrics['skipped_entries_count']}`",
        f"- Cooldown triggers: `{metrics['cooldown_trigger_count']}`",
    ]
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> None:
    runtime = parse_args(argv)
    configure_data_root(runtime.data_dir)
    if runtime.bar.upper() != "1H":
        raise ValueError("this strategy currently expects 1H entry candles")
    raw = candles_to_frame(runtime.inst_id, runtime.bar, candle_limit=runtime.candle_limit)
    features = prepare_features(raw, runtime.config)
    trades_df, equity_curve, metrics = run_backtest(features, runtime.config)
    paths = save_outputs(runtime.report_dir, runtime.out_prefix, trades_df, equity_curve, metrics)
    print(
        json.dumps(
            {
                "inst_id": runtime.inst_id,
                "bar": runtime.bar,
                "data_root": str(data_root()),
                "report_dir": str(runtime.report_dir),
                "final_equity": round(float(metrics["final_equity"]), 4),
                "total_return_pct": round(float(metrics["total_return_pct"]), 4),
                "max_drawdown_pct": round(float(metrics["max_drawdown_pct"]), 4),
                "trade_count": int(metrics["trade_count"]),
                "win_rate_pct": None if metrics["win_rate_pct"] is None else round(float(metrics["win_rate_pct"]), 4),
                "profit_factor": None
                if metrics["profit_factor"] is None
                else ("inf" if math.isinf(float(metrics["profit_factor"])) else round(float(metrics["profit_factor"]), 6)),
                "outputs": {key: str(value) for key, value in paths.items()},
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
