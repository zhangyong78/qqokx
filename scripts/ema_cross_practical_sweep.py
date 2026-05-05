"""
EMA 突破/跌破策略 · 极简实战扫参（BTC/ETH，1H 信号 + 可选 4H EMA 方向过滤）

思路对齐：
- 分段：按自然年（2023/2024/2025）分别跑（数据不足则跳过）
- 锁死成本：吃单 0.036%、开/平滑点各 0.03%（与矩阵脚本一致）
- 第一阶段只扫：参考 EMA 周期 × ATR 止损 × 固定/动态止盈（止盈 ATR 倍数固定为 2）
- 多空分开：long_only / short_only 各跑
- 输出：CSV + 终端打印「稳健 3 组」「进攻 3 组」

用法:
  python scripts/ema_cross_practical_sweep.py
  python scripts/ema_cross_practical_sweep.py --no-4h-filter   # 不做 4H 方向过滤
"""

from __future__ import annotations

import argparse
import csv
import sys
from datetime import datetime, timezone
from decimal import Decimal
from itertools import product
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from okx_quant.backtest import run_backtest
from okx_quant.models import StrategyConfig
from okx_quant.okx_client import OkxRestClient
from okx_quant.strategy_catalog import STRATEGY_EMA_BREAKDOWN_SHORT_ID, STRATEGY_EMA_BREAKOUT_LONG_ID

COINS = ("BTC-USDT-SWAP", "ETH-USDT-SWAP")
BAR_1H = "1H"
BAR_4H = "4H"
EMA_OPTIONS = (21, 55)
STOP_ATRS = (Decimal("1"), Decimal("1.5"), Decimal("2"))
TP_MODES = ("fixed", "dynamic")
# 第一阶段固定止盈距离：入场 ± 2×ATR（与止损倍数独立）
TAKE_ATR_MULT = Decimal("2")
MAKER = Decimal("0.00015")
TAKER = Decimal("0.00036")
SLIP = Decimal("0.0003")
ATR_PERIOD = 10
RISK = Decimal("100")
CAPITAL = Decimal("10000")
CANDLE_LIMIT = 10_000


def _year_bounds_ms(year: int) -> tuple[int, int]:
    start = datetime(year, 1, 1, tzinfo=timezone.utc)
    end = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
    return int(start.timestamp() * 1000), int(end.timestamp() * 1000)


def _base_config(
    *,
    inst_id: str,
    signal_mode: str,
    entry_ref: int,
    stop_atr: Decimal,
    take_profit_mode: str,
    use_4h: bool,
) -> StrategyConfig:
    kw: dict = dict(
        inst_id=inst_id,
        bar=BAR_1H,
        ema_period=entry_ref,
        trend_ema_period=55,
        big_ema_period=0,
        atr_period=ATR_PERIOD,
        atr_stop_multiplier=stop_atr,
        atr_take_multiplier=TAKE_ATR_MULT,
        order_size=Decimal("0"),
        trade_mode="cross",
        signal_mode=signal_mode,
        position_mode="net",
        environment="demo",
        tp_sl_trigger_type="mark",
        strategy_id=STRATEGY_EMA_BREAKOUT_LONG_ID
        if signal_mode == "long_only"
        else STRATEGY_EMA_BREAKDOWN_SHORT_ID,
        risk_amount=RISK,
        entry_reference_ema_period=entry_ref,
        take_profit_mode=take_profit_mode,
        max_entries_per_trend=1,
        dynamic_two_r_break_even=True,
        dynamic_fee_offset_enabled=True,
        time_stop_break_even_enabled=False,
        time_stop_break_even_bars=0,
        hold_close_exit_bars=0,
        backtest_initial_capital=CAPITAL,
        backtest_entry_slippage_rate=SLIP,
        backtest_exit_slippage_rate=SLIP,
    )
    if use_4h:
        kw["cross_higher_tf_inst_id"] = inst_id
        kw["cross_higher_tf_bar"] = BAR_4H
        kw["cross_higher_tf_ref_ema_period"] = entry_ref
    else:
        kw["cross_higher_tf_inst_id"] = None
        kw["cross_higher_tf_bar"] = None
        kw["cross_higher_tf_ref_ema_period"] = 0
    return StrategyConfig(**kw)


def _row(
    *,
    year: int,
    inst_id: str,
    direction: str,
    use_4h: bool,
    entry_ref: int,
    stop_atr: Decimal,
    tp_mode: str,
    result,
) -> dict:
    rep = result.report
    return {
        "year": year,
        "inst_id": inst_id,
        "direction": direction,
        "filter_4h_ema": "on" if use_4h else "off",
        "entry_ref_ema": entry_ref,
        "stop_atr": str(stop_atr),
        "take_atr_mult": str(TAKE_ATR_MULT),
        "take_profit_mode": tp_mode,
        "trades": rep.total_trades,
        "total_pnl": float(rep.total_pnl),
        "max_drawdown_pct": float(rep.max_drawdown_pct),
        "profit_factor": float(rep.profit_factor) if rep.profit_factor is not None else "",
        "win_rate_pct": float(rep.win_rate),
    }


def _pick_robust(rows: list[dict], *, n: int = 3) -> list[dict]:
    cand = [
        r
        for r in rows
        if int(r.get("trades") or 0) >= 15
        and float(r.get("max_drawdown_pct") or 999) <= 22
        and r.get("profit_factor") not in ("", None)
        and float(r["profit_factor"]) >= 1.05
    ]
    cand.sort(key=lambda r: float(r["total_pnl"]), reverse=True)
    return cand[:n]


def _pick_aggressive(rows: list[dict], *, n: int = 3) -> list[dict]:
    cand = sorted(rows, key=lambda r: float(r["total_pnl"]), reverse=True)
    return cand[:n]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--no-4h-filter", action="store_true", help="关闭 4H EMA 方向过滤")
    parser.add_argument(
        "--years",
        type=str,
        default="2023,2024,2025",
        help="逗号分隔年份，例如 2025 或 2023,2024,2025",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=Path(r"D:\qqokx_data\reports\analysis") / "ema_cross_practical_sweep.csv",
    )
    args = parser.parse_args()
    use_4h = not args.no_4h_filter
    years = tuple(int(y.strip()) for y in args.years.split(",") if y.strip())

    client = OkxRestClient()
    all_rows: list[dict] = []
    directions = (("long_only", "long"), ("short_only", "short"))

    total_runs = len(years) * len(COINS) * len(directions) * len(EMA_OPTIONS) * len(STOP_ATRS) * len(TP_MODES)
    done = 0
    for year in years:
        start_ts, end_ts = _year_bounds_ms(year)
        for inst_id in COINS:
            for sig, dshort in directions:
                for entry_ref, stop_atr, tp_mode in product(EMA_OPTIONS, STOP_ATRS, TP_MODES):
                    cfg = _base_config(
                        inst_id=inst_id,
                        signal_mode=sig,
                        entry_ref=entry_ref,
                        stop_atr=stop_atr,
                        take_profit_mode=tp_mode,
                        use_4h=use_4h,
                    )
                    try:
                        res = run_backtest(
                            client,
                            cfg,
                            candle_limit=CANDLE_LIMIT,
                            start_ts=start_ts,
                            end_ts=end_ts,
                            maker_fee_rate=MAKER,
                            taker_fee_rate=TAKER,
                        )
                    except Exception as exc:
                        print(f"skip {year} {inst_id} {dshort} ref={entry_ref} sl={stop_atr} {tp_mode}: {exc}")
                        continue
                    if not res.candles:
                        continue
                    all_rows.append(
                        _row(
                            year=year,
                            inst_id=inst_id,
                            direction=dshort,
                            use_4h=use_4h,
                            entry_ref=entry_ref,
                            stop_atr=stop_atr,
                            tp_mode=tp_mode,
                            result=res,
                        )
                    )
                    done += 1
                    if done % 20 == 0 or done == total_runs:
                        print(f"progress {done}/{total_runs}", flush=True)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    if not all_rows:
        print("无有效结果（可能该区间无 K 线或网络失败）。")
        return
    keys = list(all_rows[0].keys())
    with args.out.open("w", newline="", encoding="utf-8-sig") as handle:
        w = csv.DictWriter(handle, fieldnames=keys)
        w.writeheader()
        w.writerows(all_rows)
    print(f"Wrote {args.out}  rows={len(all_rows)}")

    robust = _pick_robust(all_rows)
    aggressive = _pick_aggressive(all_rows)
    print("\n=== 稳健 3 组（样本交易≥15、回撤%≤22、盈利因子≥1.05，按总盈亏）===")
    for r in robust:
        print(r)
    print("\n=== 进攻 3 组（按总盈亏）===")
    for r in aggressive:
        print(r)


if __name__ == "__main__":
    main()
