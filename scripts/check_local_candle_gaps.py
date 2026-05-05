"""
检查本地 candle_cache JSON 中某标的、某周期在指定自然月内是否缺 K 线（按时间戳等间隔检测）。

默认：ETH-USDT-SWAP、15m、2026 年 4 月（UTC，与 OKX 返回的 ts 一致）。

用法:
  python scripts/check_local_candle_gaps.py
  python scripts/check_local_candle_gaps.py --data-dir D:/qqokx_data
  python scripts/check_local_candle_gaps.py --inst-id ETH-USDT-SWAP --bar 15m --year 2026 --month 4
  python scripts/check_local_candle_gaps.py --inst-id BTC-USDT-SWAP --bar 1H --start-ms 1714521600000 --end-ms 1717200000000
"""

from __future__ import annotations

import argparse
import json
import sys
from calendar import monthrange
from datetime import datetime, timezone
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from okx_quant.app_paths import configure_data_root, data_root
from okx_quant.candle_cache import candle_cache_file_path, load_candle_cache
from okx_quant.candle_continuity import (
    bar_step_ms,
    count_bar_opens_in_half_open_range,
    find_candle_gaps_half_open_range,
    total_missing_bars,
)
def _month_range_ms_utc(year: int, month: int) -> tuple[int, int]:
    start = datetime(year, month, 1, tzinfo=timezone.utc)
    if month == 12:
        end_exclusive = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        end_exclusive = datetime(year, month + 1, 1, tzinfo=timezone.utc)
    return int(start.timestamp() * 1000), int(end_exclusive.timestamp() * 1000)


def _fmt_ts(ts: int) -> str:
    return datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def _raw_duplicate_ts_count(path: Path) -> tuple[int, int]:
    """返回 (总行数, 唯一 ts 数) 用于发现缓存里重复时间戳。"""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return -1, -1
    rows = payload.get("candles") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        return -1, -1
    ts_list: list[int] = []
    for item in rows:
        if isinstance(item, dict) and "ts" in item:
            try:
                ts_list.append(int(item["ts"]))
            except Exception:
                continue
    return len(ts_list), len(set(ts_list))


def main() -> int:
    parser = argparse.ArgumentParser(description="检查本地 candle_cache 指定月份 K 线是否连续")
    parser.add_argument("--inst-id", default="ETH-USDT-SWAP", help="标的，如 ETH-USDT-SWAP")
    parser.add_argument("--bar", default="15m", help="周期，如 15m / 1H")
    parser.add_argument("--year", type=int, default=2026, help="自然月模式：与 --month 合用")
    parser.add_argument("--month", type=int, default=4)
    parser.add_argument(
        "--start-ms",
        type=int,
        default=None,
        help="半开区间起点（毫秒 UTC）；与 --end-ms 合用时代替 --year/--month",
    )
    parser.add_argument(
        "--end-ms",
        type=int,
        default=None,
        dest="end_ms",
        help="半开区间终点 exclusive（毫秒 UTC）",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=None,
        help="数据根目录（其下应有 cache/candle_cache/），默认使用 QQOKX_DATA_DIR 或项目同级 qqokx_data",
    )
    parser.add_argument(
        "--max-gaps-print",
        type=int,
        default=30,
        help="最多打印多少段缺口详情（避免刷屏）",
    )
    args = parser.parse_args()

    if args.data_dir is not None:
        configure_data_root(args.data_dir.resolve())

    if (args.start_ms is None) ^ (args.end_ms is None):
        print("错误: --start-ms 与 --end-ms 必须同时提供或同时省略。")
        return 2
    range_mode = args.start_ms is not None and args.end_ms is not None

    try:
        step = bar_step_ms(args.bar)
    except ValueError as exc:
        print(str(exc))
        return 2

    cache_path = candle_cache_file_path(args.inst_id, args.bar)
    root = data_root()
    print(f"数据根目录: {root}")
    print(f"缓存文件:   {cache_path}")
    print(f"存在:       {cache_path.exists()}")

    if not range_mode:
        start_ms, end_ex_ms = _month_range_ms_utc(args.year, args.month)
        days = monthrange(args.year, args.month)[1]
        range_label = f"{args.year}-{args.month:02d} 共 {days} 天"
    else:
        start_ms, end_ex_ms = int(args.start_ms), int(args.end_ms)
        if start_ms >= end_ex_ms:
            print("错误: --start-ms 必须小于 --end-ms（exclusive）。")
            return 2
        days = 0
        range_label = "自定义半开区间"

    expected_slots = count_bar_opens_in_half_open_range(start_ms, end_ex_ms, step)
    print(
        f"检查区间: {_fmt_ts(start_ms)} ～ {_fmt_ts(end_ex_ms)}（不含结束边界）"
        f" | {range_label} | {args.bar} 满格约 {expected_slots} 根（步长 {step // 1000 // 60} 分钟）"
    )

    if cache_path.exists():
        raw_n, uniq_n = _raw_duplicate_ts_count(cache_path)
        if raw_n >= 0:
            print(f"原始文件: {raw_n} 条 candle 记录, 唯一 ts={uniq_n}")
            if raw_n != uniq_n:
                print(f"警告: 原始 JSON 内存在重复 ts，共 {raw_n - uniq_n} 条重复。")

    candles = load_candle_cache(args.inst_id, args.bar)
    in_month = [c for c in candles if start_ms <= c.ts < end_ex_ms]
    in_month.sort(key=lambda c: c.ts)

    if not in_month:
        print("该检查区间内没有任何 K 线（或缓存为空 / 未覆盖该区间）。")
        print("RESULT: FAIL — 区间内无数据，测试未通过。（退出码 1）")
        return 1

    print(f"区间内载入: {len(in_month)} 根（merge 后按 ts 去重）")
    print(f"第一根:   {_fmt_ts(in_month[0].ts)} close={in_month[0].close}")
    print(f"最后一根: {_fmt_ts(in_month[-1].ts)} close={in_month[-1].close}")

    gaps, cont_warnings = find_candle_gaps_half_open_range(
        candles,
        start_ms=start_ms,
        end_exclusive_ms=end_ex_ms,
        step_ms=step,
    )
    for w in cont_warnings:
        print(f"警告: {w}")

    if not gaps:
        print("结论: 区间内相邻 K 线步长一致，且与区间边界对齐（在已缓存数据前提下未见缺口）。")
        print(f"RESULT: PASS — 本地缓存 {args.bar} 连续，测试通过。")
        return 0

    total_missing = total_missing_bars(gaps)
    print(f"结论: 发现 {len(gaps)} 段缺口/未覆盖区间，合计约缺 {total_missing} 根（相对满格 {expected_slots} 根）。")
    for i, (g0, g1, n) in enumerate(gaps[: max(0, args.max_gaps_print)]):
        print(f"  [{i + 1}] 缺约 {n} 根 | 从 {_fmt_ts(g0)} 到 {_fmt_ts(g1)}（不含 {_fmt_ts(g1)}）")
    if len(gaps) > args.max_gaps_print:
        print(f"  ... 另有 {len(gaps) - args.max_gaps_print} 段未打印")
    print(
        f"RESULT: FAIL — 仍有缺口，测试未通过。"
        f"（{len(gaps)} 段，约缺 {total_missing}/{expected_slots} 根；进程退出码 1）"
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
