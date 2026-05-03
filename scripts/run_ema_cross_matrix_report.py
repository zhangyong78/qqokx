"""
Batch EMA 穿越（永续）参数矩阵回测，并输出 CSV / JSON / TXT 报告。

默认与「新版全面回测」系列口径对齐：5 永续 × 4 周期 × 多空 × 参考 EMA 21/55 ×
止损 ATR 1/1.5/2 × 止盈 ATR 1/2/3 × 固定/动态止盈 × 时间保本 1～10 根 ×
满 N 根收盘价平仓 N=1～5；每组 10000 根 K 线；开仓/平仓滑点各 0.03%；
Maker 0.015%、Taker 0.036%；每波最多开仓 1；2R 保本与手续费偏移开启。

全量笛卡尔积共 72000 组；首次运行会按标的+周期拉取 K 线（可命中本地缓存）。

用法:
  python scripts/run_ema_cross_matrix_report.py
  python scripts/run_ema_cross_matrix_report.py --smoke   # 小规模试跑（800 根 K 线/组，约数十秒）
  python scripts/run_ema_cross_matrix_report.py --out-dir D:/qqokx_data/reports/analysis

全量 72000 组 × 10000 根 K 线：首次会拉取 20 个序列的行情，耗时可从数十分钟到数小时，视网络与缓存而定。
"""

from __future__ import annotations

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import argparse
import csv
import json
import statistics
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from itertools import product
from typing import Any

from okx_quant.backtest import (
    _load_backtest_candles,
    _required_backtest_preload_candles,
    _run_backtest_with_loaded_data,
)
from okx_quant.ema_cross_insight_text import build_client_deep_insight
from okx_quant.models import StrategyConfig
from okx_quant.okx_client import OkxRestClient
from okx_quant.strategy_catalog import STRATEGY_CROSS_ID

COINS = (
    "BTC-USDT-SWAP",
    "ETH-USDT-SWAP",
    "BNB-USDT-SWAP",
    "SOL-USDT-SWAP",
    "DOGE-USDT-SWAP",
)
BARS = ("5m", "15m", "1H", "4H")
BAR_LABEL_ZH = {"5m": "5分钟", "15m": "15分钟", "1H": "1小时", "4H": "4小时"}
DIRECTIONS: tuple[str, ...] = ("long_only", "short_only")
DIRECTION_LABEL_ZH = {"long_only": "做多", "short_only": "做空"}
ENTRY_EMAS = (21, 55)
STOP_ATRS = (Decimal("1"), Decimal("1.5"), Decimal("2"))
TAKE_ATRS = (Decimal("1"), Decimal("2"), Decimal("3"))
TP_MODES: tuple[str, ...] = ("fixed", "dynamic")
TIME_BE_BARS = tuple(range(1, 11))
HOLD_CLOSE_BARS = (1, 2, 3, 4, 5)

SLIPPAGE_RATE = Decimal("0.0003")
MAKER_FEE = Decimal("0.00015")
TAKER_FEE = Decimal("0.00036")
CANDLE_LIMIT = 10_000
SMOKE_CANDLE_LIMIT = 800
ATR_PERIOD = 10
RISK_AMOUNT = Decimal("100")
INITIAL_CAPITAL = Decimal("10000")


def _worst_case_preload_config() -> StrategyConfig:
    return StrategyConfig(
        inst_id="BTC-USDT-SWAP",
        bar="1H",
        ema_period=55,
        trend_ema_period=55,
        big_ema_period=0,
        atr_period=ATR_PERIOD,
        atr_stop_multiplier=Decimal("1"),
        atr_take_multiplier=Decimal("1"),
        order_size=Decimal("0"),
        trade_mode="cross",
        signal_mode="long_only",
        position_mode="net",
        environment="demo",
        tp_sl_trigger_type="mark",
        strategy_id=STRATEGY_CROSS_ID,
        risk_amount=RISK_AMOUNT,
        entry_reference_ema_period=55,
        take_profit_mode="dynamic",
        max_entries_per_trend=1,
        dynamic_two_r_break_even=True,
        dynamic_fee_offset_enabled=True,
        time_stop_break_even_enabled=True,
        time_stop_break_even_bars=10,
        hold_close_exit_bars=5,
        backtest_initial_capital=INITIAL_CAPITAL,
        backtest_entry_slippage_rate=SLIPPAGE_RATE,
        backtest_exit_slippage_rate=SLIPPAGE_RATE,
    )


def build_config(
    *,
    inst_id: str,
    bar: str,
    signal_mode: str,
    entry_ref_ema: int,
    stop_atr: Decimal,
    take_atr: Decimal,
    take_profit_mode: str,
    time_stop_break_even_bars: int,
    hold_close_exit_bars: int,
) -> StrategyConfig:
    return StrategyConfig(
        inst_id=inst_id,
        bar=bar,
        ema_period=entry_ref_ema,
        trend_ema_period=55,
        big_ema_period=0,
        atr_period=ATR_PERIOD,
        atr_stop_multiplier=stop_atr,
        atr_take_multiplier=take_atr,
        order_size=Decimal("0"),
        trade_mode="cross",
        signal_mode=signal_mode,  # type: ignore[arg-type]
        position_mode="net",
        environment="demo",
        tp_sl_trigger_type="mark",
        strategy_id=STRATEGY_CROSS_ID,
        risk_amount=RISK_AMOUNT,
        entry_reference_ema_period=entry_ref_ema,
        take_profit_mode=take_profit_mode,  # type: ignore[arg-type]
        max_entries_per_trend=1,
        dynamic_two_r_break_even=True,
        dynamic_fee_offset_enabled=True,
        time_stop_break_even_enabled=True,
        time_stop_break_even_bars=time_stop_break_even_bars,
        hold_close_exit_bars=hold_close_exit_bars,
        backtest_initial_capital=INITIAL_CAPITAL,
        backtest_entry_slippage_rate=SLIPPAGE_RATE,
        backtest_exit_slippage_rate=SLIPPAGE_RATE,
    )


def _json_safe(obj: Any) -> Any:
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, dict):
        return {k: _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_json_safe(v) for v in obj]
    return obj


def _mean(values: list[float]) -> float:
    return float(sum(values) / len(values)) if values else 0.0


def _aggregate_by_key(rows: list[dict[str, Any]], key: str) -> dict[str, dict[str, float]]:
    buckets: dict[str, list[float]] = defaultdict(list)
    for row in rows:
        buckets[str(row[key])].append(float(row["total_pnl"]))
    return {k: {"avg_total_pnl": _mean(v), "count": float(len(v))} for k, v in sorted(buckets.items())}


def run_matrix(
    *,
    smoke: bool,
    out_dir: Path,
    client: OkxRestClient,
) -> tuple[Path, Path, Path, Path]:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    stem = f"ema_cross_matrix_{ts}{'_smoke' if smoke else ''}"
    out_dir.mkdir(parents=True, exist_ok=True)
    candle_limit = SMOKE_CANDLE_LIMIT if smoke else CANDLE_LIMIT
    csv_path = out_dir / f"{stem}_full.csv"
    json_path = out_dir / f"{stem}_summary.json"
    report_path = out_dir / f"{stem}_report.txt"
    leader_path = out_dir / f"{stem}_leader_onepage.txt"

    coins = COINS[:1] if smoke else COINS
    bars = BARS[:1] if smoke else BARS
    directions = DIRECTIONS
    entry_emas = ENTRY_EMAS[:1] if smoke else ENTRY_EMAS
    stop_atrs = STOP_ATRS[:1] if smoke else STOP_ATRS
    take_atrs = TAKE_ATRS[:1] if smoke else TAKE_ATRS
    tp_modes = TP_MODES
    time_be = TIME_BE_BARS[:2] if smoke else TIME_BE_BARS
    hold_close = HOLD_CLOSE_BARS[:2] if smoke else HOLD_CLOSE_BARS

    grid = list(
        product(
            coins,
            bars,
            directions,
            entry_emas,
            stop_atrs,
            take_atrs,
            tp_modes,
            time_be,
            hold_close,
        )
    )
    preload_cfg = _worst_case_preload_config()
    preload_need = _required_backtest_preload_candles(preload_cfg)

    candle_cache: dict[tuple[str, str], list[Any]] = {}
    instrument_cache: dict[str, Any] = {}
    data_notes: dict[tuple[str, str], str] = {}

    for inst_id in coins:
        instrument_cache[inst_id] = client.get_instrument(inst_id)
        for bar in bars:
            key = (inst_id, bar)
            if key in candle_cache:
                continue
            candles = _load_backtest_candles(
                client,
                inst_id,
                bar,
                candle_limit,
                preload_count=preload_need,
            )
            candle_cache[key] = candles
            stats = getattr(client, "last_candle_history_stats", None)
            if isinstance(stats, dict):
                parts = [
                    f"请求{candle_limit}根",
                    f"实际{len(candles)}根",
                ]
                if stats.get("cache_hit_count"):
                    parts.append(f"缓存命中{stats.get('cache_hit_count')}")
                data_notes[key] = " | ".join(parts)
            else:
                data_notes[key] = f"实际{len(candles)}根"
            print(f"loaded {inst_id} {bar}: {len(candles)} candles", flush=True)

    rows: list[dict[str, Any]] = []
    total = len(grid)
    for i, params in enumerate(grid, start=1):
        (
            inst_id,
            bar,
            signal_mode,
            entry_ref_ema,
            stop_atr,
            take_atr,
            take_profit_mode,
            time_be_bars,
            hold_bars,
        ) = params
        cfg = build_config(
            inst_id=inst_id,
            bar=bar,
            signal_mode=signal_mode,
            entry_ref_ema=entry_ref_ema,
            stop_atr=stop_atr,
            take_atr=take_atr,
            take_profit_mode=take_profit_mode,
            time_stop_break_even_bars=time_be_bars,
            hold_close_exit_bars=hold_bars,
        )
        candles = candle_cache[(inst_id, bar)]
        instrument = instrument_cache[inst_id]
        result = _run_backtest_with_loaded_data(
            candles,
            instrument,
            cfg,
            data_source_note=data_notes.get((inst_id, bar), ""),
            maker_fee_rate=MAKER_FEE,
            taker_fee_rate=TAKER_FEE,
        )
        rep = result.report
        rows.append(
            {
                "inst_id": inst_id,
                "bar": bar,
                "bar_label_zh": BAR_LABEL_ZH.get(bar, bar),
                "direction": signal_mode,
                "direction_label_zh": DIRECTION_LABEL_ZH[signal_mode],
                "entry_reference_ema": entry_ref_ema,
                "atr_period": ATR_PERIOD,
                "stop_atr": str(stop_atr),
                "take_atr": str(take_atr),
                "take_profit_mode": take_profit_mode,
                "time_stop_break_even_enabled": True,
                "time_stop_break_even_bars": time_be_bars,
                "hold_close_exit_bars": hold_bars,
                "candle_count": len(candles),
                "total_trades": rep.total_trades,
                "win_rate_pct": float(rep.win_rate),
                "total_pnl": float(rep.total_pnl),
                "total_return_pct": float(rep.total_return_pct),
                "profit_factor": float(rep.profit_factor) if rep.profit_factor is not None else None,
                "max_drawdown": float(rep.max_drawdown),
                "max_drawdown_pct": float(rep.max_drawdown_pct),
                "average_r_multiple": float(rep.average_r_multiple),
                "ending_equity": float(rep.ending_equity),
                "total_fees": float(rep.total_fees),
                "slippage_costs": float(rep.slippage_costs),
                "take_profit_hits": rep.take_profit_hits,
                "stop_loss_hits": rep.stop_loss_hits,
                "data_source_note": data_notes.get((inst_id, bar), ""),
            }
        )
        if i % 500 == 0 or i == total:
            print(f"progress {i}/{total}", flush=True)

    fieldnames = list(rows[0].keys()) if rows else []
    with csv_path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    pnls = [float(r["total_pnl"]) for r in rows]
    sorted_by_pnl = sorted(rows, key=lambda r: float(r["total_pnl"]), reverse=True)
    summary: dict[str, Any] = {
        "generated_at_utc": ts,
        "smoke": smoke,
        "total_runs": len(rows),
        "spec": {
            "strategy": "EMA穿越",
            "coins": list(coins),
            "bars": list(bars),
            "directions": list(directions),
            "entry_reference_emas": list(entry_emas),
            "stop_atr": [str(x) for x in stop_atrs],
            "take_atr": [str(x) for x in take_atrs],
            "take_profit_modes": list(tp_modes),
            "time_stop_break_even_bars": list(time_be),
            "hold_close_exit_bars": list(hold_close),
            "candle_limit": candle_limit,
            "slippage_each_side": str(SLIPPAGE_RATE),
            "maker_fee": str(MAKER_FEE),
            "taker_fee": str(TAKER_FEE),
            "initial_capital": str(INITIAL_CAPITAL),
            "risk_amount_per_trade": str(RISK_AMOUNT),
            "max_entries_per_trend": 1,
            "dynamic_two_r_break_even": True,
            "dynamic_fee_offset_enabled": True,
        },
        "distribution": {
            "avg_total_pnl": _mean(pnls),
            "median_total_pnl": float(statistics.median(pnls)) if pnls else 0.0,
            "min_total_pnl": min(pnls) if pnls else 0.0,
            "max_total_pnl": max(pnls) if pnls else 0.0,
        },
        "by_bar": _aggregate_by_key(rows, "bar"),
        "by_direction": _aggregate_by_key(rows, "direction_label_zh"),
        "by_take_profit_mode": _aggregate_by_key(rows, "take_profit_mode"),
        "by_time_stop_break_even_bars": _aggregate_by_key(rows, "time_stop_break_even_bars"),
        "by_hold_close_exit_bars": _aggregate_by_key(rows, "hold_close_exit_bars"),
        "by_entry_reference_ema": _aggregate_by_key(rows, "entry_reference_ema"),
        "top_30_by_total_pnl": sorted_by_pnl[:30],
        "bottom_15_by_total_pnl": sorted_by_pnl[-15:],
    }
    json_path.write_text(json.dumps(_json_safe(summary), ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        f"EMA 穿越策略 · 全参数矩阵回测总报告（{'试跑' if smoke else '全量'}）",
        "",
        f"出具时间（UTC）：{ts}",
        f"总运行组数：{len(rows)}",
        "",
        "一、测试范围（与需求对齐）",
        "1. 策略：EMA 穿越；方向：只做多、只做空分别统计。",
        f"2. 币种（USDT 永续）：{', '.join(coins)}。",
        f"3. 穿越参考 EMA 周期：{', '.join(str(x) for x in entry_emas)}。",
        f"4. K 线周期：{', '.join(BAR_LABEL_ZH.get(b, b) for b in bars)}。",
        f"5. 每组目标 {candle_limit} 根 K 线（实际根数见 CSV 的 candle_count / data_source_note）。",
        f"6. 止损 ATR：{', '.join(str(x) for x in stop_atrs)}；止盈 ATR（相对入场）：{', '.join(str(x) for x in take_atrs)}；"
        f"止盈模式：{', '.join(tp_modes)}。",
        "7. 开仓与平仓滑点各 0.03%（模型内为小数费率）。",
        f"8. 时间保本：开启；K 线档：{', '.join(str(x) for x in time_be)}。",
        f"9. 满 N 根收盘价平仓：N ∈ {{{', '.join(str(x) for x in hold_close)}}}。",
        "10. 手续费：Maker 0.015%、Taker 0.036%；每波最多开仓 1；2R 保本与手续费偏移开启。",
        "",
        "二、全样本分布",
        f"平均总盈亏（账户货币）：{summary['distribution']['avg_total_pnl']:.4f}",
        f"中位数总盈亏：{summary['distribution']['median_total_pnl']:.4f}",
        f"最大总盈亏：{summary['distribution']['max_total_pnl']:.4f}",
        f"最小总盈亏：{summary['distribution']['min_total_pnl']:.4f}",
        "",
        "三、按周期平均总盈亏（粗览）",
    ]
    for bar, payload in summary["by_bar"].items():
        lines.append(f"- {BAR_LABEL_ZH.get(bar, bar)}：平均 {payload['avg_total_pnl']:.4f}（样本 {int(payload['count'])} 组）")
    lines += ["", "四、按方向平均总盈亏"]
    for label, payload in summary["by_direction"].items():
        lines.append(f"- {label}：平均 {payload['avg_total_pnl']:.4f}")
    lines += ["", "五、固定 vs 动态止盈（平均总盈亏）"]
    for mode, payload in summary["by_take_profit_mode"].items():
        lines.append(f"- {mode}：平均 {payload['avg_total_pnl']:.4f}")
    lines += ["", "六、时间保本 K 线数（全局平均总盈亏，档间对比）"]
    for k in sorted(summary["by_time_stop_break_even_bars"], key=lambda x: int(x)):
        p = summary["by_time_stop_break_even_bars"][k]
        lines.append(f"- {k} 根：平均 {p['avg_total_pnl']:.4f}")
    lines += ["", "七、满 N 根收盘价平仓（全局平均总盈亏）"]
    for k in sorted(summary["by_hold_close_exit_bars"], key=lambda x: int(x)):
        p = summary["by_hold_close_exit_bars"][k]
        lines.append(f"- N={k}：平均 {p['avg_total_pnl']:.4f}")
    lines += ["", "八、总盈亏前 15 组参数（详见 CSV / JSON）"]
    for idx, item in enumerate(sorted_by_pnl[:15], start=1):
        lines.append(
            f"{idx}. {item['inst_id']} {item['bar_label_zh']} {item['direction_label_zh']} "
            f"EMA{item['entry_reference_ema']} SL×{item['stop_atr']} TP×{item['take_atr']} "
            f"{item['take_profit_mode']} 时间保本{item['time_stop_break_even_bars']} "
            f"收盘平仓{item['hold_close_exit_bars']} → 总盈亏 {item['total_pnl']:.4f}"
        )
    lines += [
        "",
        "九、使用说明",
        "1. 完整结果在 *_full.csv，可透视表按币种、周期、方向、参数切片。",
        "2. 短周期样本时间跨度有限，结论以 1H/4H 为主更稳妥（与历史报告口径一致）。",
        "3. 若需缩小规模，可使用 --smoke 或自行改脚本顶部的元组常量。",
    ]
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    pos_count = sum(1 for r in rows if float(r["total_pnl"]) > 0)
    leader = [
        "一页版摘要（EMA 穿越 · 矩阵回测）",
        "",
        f"出具时间（UTC）：{ts}",
        f"总组数：{len(rows)}；盈利组数：{pos_count}（{100.0 * pos_count / len(rows):.2f}%）" if rows else "",
        "",
        "核心分布：",
        f"- 全部样本平均总盈亏：{summary['distribution']['avg_total_pnl']:.4f}",
        f"- 中位数总盈亏：{summary['distribution']['median_total_pnl']:.4f}",
        "",
        "按方向：",
    ]
    for label, payload in summary["by_direction"].items():
        leader.append(f"- {label} 平均 {payload['avg_total_pnl']:.4f}")
    leader += [
        "",
        "按止盈模式：",
    ]
    for mode, payload in summary["by_take_profit_mode"].items():
        leader.append(f"- {mode} 平均 {payload['avg_total_pnl']:.4f}")
    leader += [
        "",
        "执行建议：请结合 CSV 中分币种、分周期最优档，避免单看全样本平均；高周期结论更可复用。",
    ]
    leader_path.write_text("\n".join(leader) + "\n", encoding="utf-8")

    deep_path = out_dir / f"{stem}_deep_insight_客户版.txt"
    deep_path.write_text(
        build_client_deep_insight(rows, utc_ts=ts, spec=summary["spec"], smoke=smoke),
        encoding="utf-8",
    )

    print(
        f"Wrote:\n  {csv_path}\n  {json_path}\n  {report_path}\n  {leader_path}\n  {deep_path}",
        flush=True,
    )
    return csv_path, json_path, report_path, leader_path, deep_path


def main() -> None:
    parser = argparse.ArgumentParser(description="EMA 穿越矩阵回测报告")
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path(r"D:\qqokx_data\reports\analysis"),
        help="输出目录（默认与历史 Codex 报告一致）",
    )
    parser.add_argument("--smoke", action="store_true", help="小规模试跑（极少参数组合）")
    args = parser.parse_args()

    client = OkxRestClient()
    run_matrix(smoke=args.smoke, out_dir=args.out_dir, client=client)


if __name__ == "__main__":
    main()
