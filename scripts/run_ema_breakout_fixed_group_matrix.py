from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from okx_quant.backtest import (
    _build_backtest_data_source_note,
    _load_backtest_candles,
    _required_backtest_preload_candles,
    _run_backtest_with_loaded_data,
)
from okx_quant.models import StrategyConfig
from okx_quant.okx_client import OkxRestClient
from okx_quant.strategy_catalog import STRATEGY_EMA_BREAKDOWN_SHORT_ID, STRATEGY_EMA_BREAKOUT_LONG_ID

ENTRY_REFERENCE_EMAS: tuple[int, ...] = (21, 55)
STOP_ATRS: tuple[Decimal, ...] = (Decimal("1"), Decimal("1.5"), Decimal("2"))
TAKE_RATIOS: tuple[Decimal, ...] = (Decimal("1"), Decimal("2"), Decimal("3"), Decimal("4"))
EMA_PERIOD = 21
TREND_EMA_PERIOD = 55
ATR_PERIOD = 10
CANDLE_LIMIT = 10_000
INITIAL_CAPITAL = Decimal("10000")
RISK_AMOUNT = Decimal("10")
MAKER_FEE = Decimal("0.00015")
TAKER_FEE = Decimal("0.00036")


@dataclass(frozen=True)
class MatrixRow:
    strategy_id: str
    bar: str
    entry_reference_ema: int
    atr_stop_multiplier: str
    atr_take_multiplier: str
    take_ratio: str
    total_trades: int
    win_rate: str
    total_pnl: str
    total_return_pct: str
    average_r_multiple: str
    max_drawdown_pct: str
    profit_factor: str
    ending_equity: str
    total_fees: str
    slippage_costs: str
    take_profit_hits: int
    stop_loss_hits: int


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
    signal_mode = "long_only" if strategy_id == STRATEGY_EMA_BREAKOUT_LONG_ID else "short_only"
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
        take_profit_mode="fixed",
        max_entries_per_trend=1,
        dynamic_two_r_break_even=False,
        dynamic_fee_offset_enabled=False,
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


def format_decimal(value: Decimal | None, digits: int = 4) -> str:
    if value is None:
        return "-"
    return f"{value:.{digits}f}"


def winner_sort_key(row: MatrixRow) -> tuple[Decimal, Decimal, Decimal, Decimal]:
    profit_factor = Decimal(row.profit_factor) if row.profit_factor != "-" else Decimal("0")
    return (
        Decimal(row.total_pnl),
        profit_factor,
        -Decimal(row.max_drawdown_pct),
        Decimal(row.average_r_multiple),
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--strategy-id", required=True)
    parser.add_argument("--bar", required=True)
    parser.add_argument("--slippage", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--symbol", default="ETH-USDT-SWAP")
    args = parser.parse_args()

    strategy_id = args.strategy_id.strip()
    bar = args.bar.strip()
    symbol = args.symbol.strip().upper()
    slippage = Decimal(str(args.slippage))
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    client = OkxRestClient()
    instrument = client.get_instrument(symbol)
    preload_config = build_strategy_config(
        inst_id=symbol,
        strategy_id=strategy_id,
        bar=bar,
        entry_reference_ema_period=max(ENTRY_REFERENCE_EMAS),
        atr_stop_multiplier=max(STOP_ATRS),
        atr_take_multiplier=max(STOP_ATRS) * max(TAKE_RATIOS),
        slippage_rate=slippage,
    )
    preload_count = _required_backtest_preload_candles(preload_config)
    candles = _load_backtest_candles(
        client,
        symbol,
        bar,
        CANDLE_LIMIT,
        preload_count=preload_count,
    )
    data_source_note = _build_backtest_data_source_note(client)

    rows: list[MatrixRow] = []
    total = len(ENTRY_REFERENCE_EMAS) * len(STOP_ATRS) * len(TAKE_RATIOS)
    step = 0
    for entry_reference_ema in ENTRY_REFERENCE_EMAS:
        for stop_atr in STOP_ATRS:
            for take_ratio in TAKE_RATIOS:
                take_atr = stop_atr * take_ratio
                step += 1
                print(
                    f"[{step}/{total}] fixed {symbol} {strategy_id} {bar} "
                    f"EMA{entry_reference_ema} SLx{stop_atr} TP{take_ratio}R",
                    flush=True,
                )
                config = build_strategy_config(
                    inst_id=symbol,
                    strategy_id=strategy_id,
                    bar=bar,
                    entry_reference_ema_period=entry_reference_ema,
                    atr_stop_multiplier=stop_atr,
                    atr_take_multiplier=take_atr,
                    slippage_rate=slippage,
                )
                result = _run_backtest_with_loaded_data(
                    candles,
                    instrument,
                    config,
                    data_source_note=data_source_note,
                    maker_fee_rate=MAKER_FEE,
                    taker_fee_rate=TAKER_FEE,
                )
                report = result.report
                rows.append(
                    MatrixRow(
                        strategy_id=strategy_id,
                        bar=bar,
                        entry_reference_ema=entry_reference_ema,
                        atr_stop_multiplier=str(stop_atr),
                        atr_take_multiplier=str(take_atr),
                        take_ratio=str(take_ratio),
                        total_trades=report.total_trades,
                        win_rate=format_decimal(report.win_rate, 2),
                        total_pnl=format_decimal(report.total_pnl, 4),
                        total_return_pct=format_decimal(report.total_return_pct, 2),
                        average_r_multiple=format_decimal(report.average_r_multiple, 4),
                        max_drawdown_pct=format_decimal(report.max_drawdown_pct, 2),
                        profit_factor=format_decimal(report.profit_factor, 4) if report.profit_factor is not None else "-",
                        ending_equity=format_decimal(report.ending_equity, 4),
                        total_fees=format_decimal(report.total_fees, 4),
                        slippage_costs=format_decimal(report.slippage_costs, 4),
                        take_profit_hits=report.take_profit_hits,
                        stop_loss_hits=report.stop_loss_hits,
                    )
                )

    winner = sorted(rows, key=winner_sort_key, reverse=True)[0]
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "symbol": symbol,
        "strategy_id": strategy_id,
        "bar": bar,
        "candle_limit": CANDLE_LIMIT,
        "slippage": str(slippage),
        "take_profit_mode": "fixed",
        "dynamic_two_r_break_even": False,
        "dynamic_fee_offset_enabled": False,
        "take_profit_ratio_mode": "risk_multiple",
        "data_source_note": data_source_note,
        "rows": [asdict(item) for item in rows],
        "winner": asdict(winner),
    }
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"saved {out_path}", flush=True)


if __name__ == "__main__":
    main()
