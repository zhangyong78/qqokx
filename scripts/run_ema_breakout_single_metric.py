from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from okx_quant.backtest import _build_backtest_data_source_note, _load_backtest_candles, _required_backtest_preload_candles, _run_backtest_with_loaded_data, run_backtest
from okx_quant.models import StrategyConfig
from okx_quant.okx_client import OkxRestClient

EMA_PERIOD = 21
TREND_EMA_PERIOD = 55
ATR_PERIOD = 10
INITIAL_CAPITAL = Decimal("10000")
RISK_AMOUNT = Decimal("10")
MAKER_FEE = Decimal("0.00015")
TAKER_FEE = Decimal("0.00036")


def build_strategy_config(
    *,
    inst_id: str,
    strategy_id: str,
    bar: str,
    entry_reference_ema_period: int,
    atr_stop_multiplier: Decimal,
    atr_take_multiplier: Decimal,
    slippage_rate: Decimal,
) -> StrategyConfig:
    signal_mode = "long_only" if strategy_id == "ema_breakout_long" else "short_only"
    return StrategyConfig(
        inst_id=inst_id,
        bar=bar,
        ema_period=EMA_PERIOD,
        trend_ema_period=TREND_EMA_PERIOD,
        big_ema_period=233,
        atr_period=ATR_PERIOD,
        atr_stop_multiplier=atr_stop_multiplier,
        atr_take_multiplier=atr_take_multiplier,
        order_size=Decimal("0"),
        trade_mode="cross",
        signal_mode=signal_mode,
        position_mode="net",
        environment="demo",
        tp_sl_trigger_type="mark",
        strategy_id=strategy_id,
        risk_amount=RISK_AMOUNT,
        entry_reference_ema_period=entry_reference_ema_period,
        take_profit_mode="dynamic",
        max_entries_per_trend=1,
        dynamic_two_r_break_even=True,
        dynamic_fee_offset_enabled=True,
        time_stop_break_even_enabled=False,
        time_stop_break_even_bars=10,
        hold_close_exit_bars=0,
        backtest_initial_capital=INITIAL_CAPITAL,
        backtest_sizing_mode="fixed_risk",
        backtest_compounding=False,
        backtest_entry_slippage_rate=slippage_rate,
        backtest_exit_slippage_rate=slippage_rate,
        backtest_slippage_rate=slippage_rate,
        backtest_funding_rate=Decimal("0"),
    )


def fmt(value, digits: int = 4) -> str:
    if value is None:
        return "-"
    if isinstance(value, Decimal):
        return f"{value:.{digits}f}"
    return str(value)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--strategy-id", required=True)
    parser.add_argument("--bar", required=True)
    parser.add_argument("--entry-ema", required=True, type=int)
    parser.add_argument("--stop", required=True)
    parser.add_argument("--take", required=True)
    parser.add_argument("--slippage", required=True)
    parser.add_argument("--limit", required=True, type=int)
    parser.add_argument("--mode", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--symbol", default="ETH-USDT-SWAP")
    args = parser.parse_args()
    symbol = args.symbol.strip().upper()

    config = build_strategy_config(
        inst_id=symbol,
        strategy_id=args.strategy_id,
        bar=args.bar,
        entry_reference_ema_period=args.entry_ema,
        atr_stop_multiplier=Decimal(args.stop),
        atr_take_multiplier=Decimal(args.take),
        slippage_rate=Decimal(args.slippage),
    )
    client = OkxRestClient()

    if args.limit == 0 or args.limit > 10000:
        instrument = client.get_instrument(symbol)
        preload_count = _required_backtest_preload_candles(config)
        candles = _load_backtest_candles(
            client,
            symbol,
            args.bar,
            args.limit,
            start_ts=0 if args.limit == 0 else None,
            end_ts=int(datetime.now(timezone.utc).timestamp() * 1000) if args.limit == 0 else None,
            preload_count=preload_count,
        )
        result = _run_backtest_with_loaded_data(
            candles,
            instrument,
            config,
            data_source_note=_build_backtest_data_source_note(client),
            maker_fee_rate=MAKER_FEE,
            taker_fee_rate=TAKER_FEE,
        )
    else:
        result = run_backtest(
            client,
            config,
            candle_limit=args.limit,
            maker_fee_rate=MAKER_FEE,
            taker_fee_rate=TAKER_FEE,
        )

    report = result.report
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "symbol": symbol,
        "mode": args.mode,
        "strategy_id": args.strategy_id,
        "bar": args.bar,
        "entry_reference_ema": args.entry_ema,
        "atr_stop_multiplier": args.stop,
        "atr_take_multiplier": args.take,
        "slippage": args.slippage,
        "limit": args.limit,
        "candle_count": len(result.candles),
        "total_trades": report.total_trades,
        "win_rate": fmt(report.win_rate, 2),
        "total_pnl": fmt(report.total_pnl),
        "total_return_pct": fmt(report.total_return_pct, 2),
        "average_r_multiple": fmt(report.average_r_multiple),
        "max_drawdown_pct": fmt(report.max_drawdown_pct, 2),
        "profit_factor": fmt(report.profit_factor),
        "ending_equity": fmt(report.ending_equity),
        "total_fees": fmt(report.total_fees),
        "slippage_costs": fmt(report.slippage_costs),
        "take_profit_hits": report.take_profit_hits,
        "stop_loss_hits": report.stop_loss_hits,
        "data_source_note": result.data_source_note,
    }
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"saved {out_path}", flush=True)


if __name__ == "__main__":
    main()
