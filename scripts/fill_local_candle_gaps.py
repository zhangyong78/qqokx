"""
按连续性检测结果，从 OKX 拉取缺段并写入本地 candle_cache（依赖公网与 OkxRestClient 配置）。

用法:
  python scripts/fill_local_candle_gaps.py --inst-id ETH-USDT-SWAP --bar 15m --year 2026 --month 4
  python scripts/fill_local_candle_gaps.py --inst-id BTC-USDT-SWAP --bar 1H --start-ms ... --end-ms ... --data-dir D:/qqokx_data
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from okx_quant.app_paths import configure_data_root, data_root
from okx_quant.candle_cache import candle_cache_file_path, load_candle_cache
from okx_quant.candle_continuity import bar_step_ms, find_candle_gaps_half_open_range, total_missing_bars
from okx_quant.okx_client import OkxRestClient


def _month_range_ms_utc(year: int, month: int) -> tuple[int, int]:
    start = datetime(year, month, 1, tzinfo=timezone.utc)
    if month == 12:
        end_exclusive = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        end_exclusive = datetime(year, month + 1, 1, tzinfo=timezone.utc)
    return int(start.timestamp() * 1000), int(end_exclusive.timestamp() * 1000)


def _fmt_ts(ts: int) -> str:
    return datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def main() -> int:
    parser = argparse.ArgumentParser(description="根据缺口分段从 OKX 补齐本地 candle_cache")
    parser.add_argument("--inst-id", required=True, help="标的，如 ETH-USDT-SWAP")
    parser.add_argument("--bar", required=True, help="周期，如 15m / 1H")
    parser.add_argument("--year", type=int, default=2026)
    parser.add_argument("--month", type=int, default=4)
    parser.add_argument("--start-ms", type=int, default=None)
    parser.add_argument("--end-ms", type=int, default=None, dest="end_ms")
    parser.add_argument("--data-dir", type=Path, default=None)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="只打印将拉取的区间，不请求网络、不写缓存",
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

    if not range_mode:
        start_ms, end_ex_ms = _month_range_ms_utc(args.year, args.month)
        label = f"{args.year}-{args.month:02d}"
    else:
        start_ms, end_ex_ms = int(args.start_ms), int(args.end_ms)
        if start_ms >= end_ex_ms:
            print("错误: --start-ms 必须小于 --end-ms。")
            return 2
        label = "自定义区间"

    root = data_root()
    cache_path = candle_cache_file_path(args.inst_id, args.bar)
    print(f"数据根: {root}")
    print(f"缓存:   {cache_path} | 区间 {label} [{_fmt_ts(start_ms)}, {_fmt_ts(end_ex_ms)})")

    candles = load_candle_cache(args.inst_id, args.bar)
    gaps, warnings = find_candle_gaps_half_open_range(
        candles,
        start_ms=start_ms,
        end_exclusive_ms=end_ex_ms,
        step_ms=step,
    )
    for w in warnings:
        print(f"警告: {w}")

    if not gaps:
        print("未发现缺口，无需补齐。")
        return 0

    total = total_missing_bars(gaps)
    print(f"共 {len(gaps)} 段缺口，约缺 {total} 根。将按段调用 get_candles_history_range(limit=0)。")

    if args.dry_run:
        for i, (g0, g1, n) in enumerate(gaps, start=1):
            end_inc = g1 - step
            print(f"  [{i}] n≈{n} | {_fmt_ts(g0)} … {_fmt_ts(end_inc)} (inclusive end)")
        print("已 --dry-run，未拉取。去掉该参数后执行写入。")
        return 0

    client = OkxRestClient()
    for i, (g0, g1, n) in enumerate(gaps, start=1):
        end_inc = g1 - step
        if end_inc < g0:
            print(f"  [{i}] 跳过异常段 g0={g0} g1={g1}")
            continue
        print(f"  [{i}/{len(gaps)}] 拉取 {_fmt_ts(g0)} … {_fmt_ts(end_inc)}（约 {n} 根）…")
        client.get_candles_history_range(
            args.inst_id,
            args.bar,
            start_ts=g0,
            end_ts=end_inc,
            limit=0,
            preload_count=0,
        )

    after = load_candle_cache(args.inst_id, args.bar)
    gaps2, _ = find_candle_gaps_half_open_range(
        after,
        start_ms=start_ms,
        end_exclusive_ms=end_ex_ms,
        step_ms=step,
    )
    if gaps2:
        print(f"补齐后仍有 {len(gaps2)} 段缺口（合计约 {total_missing_bars(gaps2)} 根），请检查网络或 API 限制。")
        return 1
    print("补齐完成：该区间内缓存已连续。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
