"""K 线时间序列连续性检测（按固定 bar 步长）。"""

from __future__ import annotations

from okx_quant.models import Candle

# 与 OKX 常见 K 线开盘时间步长一致（毫秒）
BAR_STEP_MS: dict[str, int] = {
    "1m": 60_000,
    "3m": 180_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1H": 3_600_000,
    "2H": 7_200_000,
    "4H": 14_400_000,
    "6H": 21_600_000,
    "12H": 43_200_000,
    "1D": 86_400_000,
}


def bar_step_ms(bar: str) -> int:
    step = BAR_STEP_MS.get(bar.strip())
    if step is None:
        raise ValueError(f"不支持的 bar={bar!r}，已知: {sorted(BAR_STEP_MS)}")
    return step


def align_window_start_ms(ts_ms: int, step_ms: int) -> int:
    """将窗口起点向下对齐到 bar 开盘栅格（UTC 毫秒）。"""
    if step_ms <= 0:
        raise ValueError("step_ms 必须为正")
    r = ts_ms % step_ms
    return ts_ms - r


def count_bar_opens_in_half_open_range(start_ms: int, end_exclusive_ms: int, step_ms: int) -> int:
    """统计半开区间 [start_ms, end_exclusive_ms) 内、从 start_ms 起每隔 step_ms 的开盘点个数。"""
    n = 0
    t = start_ms
    while t < end_exclusive_ms:
        n += 1
        t += step_ms
    return n


def find_candle_gaps_in_window(
    candles: list[Candle],
    *,
    window_start_ms: int,
    window_end_ms_inclusive: int,
    step_ms: int,
) -> tuple[list[tuple[int, int, int]], list[str]]:
    """
    在相对 [window_start_ms, window_end_ms_inclusive] 的语义下检测缺口（起止向下对齐到 bar 开盘栅格）。

    返回 (gaps, warnings)，gaps 元组为 (缺口首根开盘 ts, 缺口末 exclusive, 缺根数)。
    """
    ws = align_window_start_ms(window_start_ms, step_ms)
    last_open = align_window_start_ms(window_end_ms_inclusive, step_ms)
    end_ex = last_open + step_ms
    if ws >= end_ex:
        return [], []
    return find_candle_gaps_half_open_range(
        candles,
        start_ms=ws,
        end_exclusive_ms=end_ex,
        step_ms=step_ms,
    )


def total_missing_bars(gaps: list[tuple[int, int, int]]) -> int:
    return sum(g[2] for g in gaps)


def find_candle_gaps_half_open_range(
    candles: list[Candle],
    *,
    start_ms: int,
    end_exclusive_ms: int,
    step_ms: int,
) -> tuple[list[tuple[int, int, int]], list[str]]:
    """
    在半开区间 [start_ms, end_exclusive_ms) 内检测缺根（与 scripts/check_local_candle_gaps 语义一致）。

    返回 (gaps, warnings)；warnings 含时间步异常（间隔非 step 整数倍等）。
    """
    warnings: list[str] = []
    if start_ms >= end_exclusive_ms:
        return [], warnings

    in_range = [c for c in candles if start_ms <= c.ts < end_exclusive_ms]
    in_range.sort(key=lambda c: c.ts)
    gaps: list[tuple[int, int, int]] = []

    if not in_range:
        miss_all = count_bar_opens_in_half_open_range(start_ms, end_exclusive_ms, step_ms)
        if miss_all > 0:
            gaps.append((start_ms, end_exclusive_ms, miss_all))
        return gaps, warnings

    miss_head = count_bar_opens_in_half_open_range(start_ms, in_range[0].ts, step_ms)
    if miss_head > 0:
        gaps.append((start_ms, in_range[0].ts, miss_head))

    for prev, cur in zip(in_range, in_range[1:]):
        delta = cur.ts - prev.ts
        if delta == step_ms:
            continue
        if delta < step_ms:
            warnings.append(
                f"时间间隔小于步长：prev_ts={prev.ts} cur_ts={cur.ts} delta_ms={delta} step_ms={step_ms}"
            )
            continue
        if delta % step_ms != 0:
            warnings.append(
                f"时间间隔不是步长整数倍：prev_ts={prev.ts} cur_ts={cur.ts} delta_ms={delta} step_ms={step_ms}"
            )
            continue
        missing = delta // step_ms - 1
        gap_open = prev.ts + step_ms
        gap_end_ex = cur.ts
        gaps.append((gap_open, gap_end_ex, int(missing)))

    miss_tail = count_bar_opens_in_half_open_range(in_range[-1].ts + step_ms, end_exclusive_ms, step_ms)
    if miss_tail > 0:
        gaps.append((in_range[-1].ts + step_ms, end_exclusive_ms, miss_tail))

    return gaps, warnings
