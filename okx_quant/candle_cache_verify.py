"""本地 candle_cache 时间连续性校验，并在有缺口时按段从 OKX 拉取补齐。"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from okx_quant.candle_cache import load_candle_cache
from okx_quant.candle_continuity import bar_step_ms, find_candle_gaps_half_open_range, total_missing_bars


@dataclass(frozen=True)
class CacheVerifyOutcome:
    inst_id: str
    bar: str
    """无致命错误且补齐后无缺口时为 True。"""
    ok: bool
    candle_count_before: int
    candle_count_after: int
    missing_bars_before: int
    missing_bars_after: int
    gap_segments_before: int
    gap_segments_after: int
    range_fetch_calls: int
    did_full_history_fetch: bool
    warnings: tuple[str, ...]
    error: str | None


def verify_and_repair_cached_candles(
    client: object,
    inst_id: str,
    bar: str,
    *,
    base_dir: Path | None = None,
    max_repair_rounds: int = 12,
) -> CacheVerifyOutcome:
    """
    在缓存时间跨度 [最早 ts, 最晚 ts] 上按 bar 步长检查连续性；若有缺口则调用
    ``get_candles_history_range(..., limit=0)`` 分段补齐（写盘由 client 完成）。

    若缓存为空，先尝试 ``get_candles_history(..., limit=0)`` 全量拉取再校验。
    """
    step = bar_step_ms(bar)
    warnings: list[str] = []
    range_calls = 0
    did_full = False

    def _reload() -> list:
        return load_candle_cache(inst_id, bar, base_dir=base_dir)

    candles = _reload()
    if not candles:
        history = getattr(client, "get_candles_history", None)
        if not callable(history):
            return CacheVerifyOutcome(
                inst_id=inst_id,
                bar=bar,
                ok=False,
                candle_count_before=0,
                candle_count_after=0,
                missing_bars_before=0,
                missing_bars_after=0,
                gap_segments_before=0,
                gap_segments_after=0,
                range_fetch_calls=0,
                did_full_history_fetch=False,
                warnings=(),
                error="客户端不支持 get_candles_history，无法从空缓存补数。",
            )
        try:
            history(inst_id, bar, limit=0)
            did_full = True
        except Exception as exc:
            return CacheVerifyOutcome(
                inst_id=inst_id,
                bar=bar,
                ok=False,
                candle_count_before=0,
                candle_count_after=0,
                missing_bars_before=0,
                missing_bars_after=0,
                gap_segments_before=0,
                gap_segments_after=0,
                range_fetch_calls=0,
                did_full_history_fetch=False,
                warnings=(),
                error=str(exc),
            )
        candles = _reload()

    if not candles:
        return CacheVerifyOutcome(
            inst_id=inst_id,
            bar=bar,
            ok=False,
            candle_count_before=0,
            candle_count_after=0,
            missing_bars_before=0,
            missing_bars_after=0,
            gap_segments_before=0,
            gap_segments_after=0,
            range_fetch_calls=0,
            did_full_history_fetch=did_full,
            warnings=(),
            error="全量拉取后缓存仍为空，请检查网络与 API。" if did_full else "无本地缓存且未能拉取数据。",
        )

    count_before = len(candles)
    mn, mx = candles[0].ts, candles[-1].ts
    end_ex = mx + step

    gaps, w0 = find_candle_gaps_half_open_range(
        candles,
        start_ms=mn,
        end_exclusive_ms=end_ex,
        step_ms=step,
    )
    warnings.extend(w0)
    missing_before = total_missing_bars(gaps)
    seg_before = len(gaps)

    range_fn = getattr(client, "get_candles_history_range", None)
    if gaps and not callable(range_fn):
        return CacheVerifyOutcome(
            inst_id=inst_id,
            bar=bar,
            ok=False,
            candle_count_before=count_before,
            candle_count_after=count_before,
            missing_bars_before=missing_before,
            missing_bars_after=missing_before,
            gap_segments_before=seg_before,
            gap_segments_after=seg_before,
            range_fetch_calls=0,
            did_full_history_fetch=did_full,
            warnings=tuple(warnings),
            error="客户端不支持 get_candles_history_range，无法自动补缺口。",
        )

    prev_missing = None
    for _ in range(max(1, max_repair_rounds)):
        if not gaps:
            break
        cur_missing = total_missing_bars(gaps)
        if prev_missing is not None and cur_missing >= prev_missing:
            break
        prev_missing = cur_missing

        for g0, g1, _n in gaps:
            end_inc = g1 - step
            if end_inc < g0:
                continue
            range_fn(
                inst_id,
                bar,
                start_ts=g0,
                end_ts=end_inc,
                limit=0,
                preload_count=0,
            )
            range_calls += 1

        candles = _reload()
        if not candles:
            break
        mn, mx = candles[0].ts, candles[-1].ts
        end_ex = mx + step
        gaps, w_extra = find_candle_gaps_half_open_range(
            candles,
            start_ms=mn,
            end_exclusive_ms=end_ex,
            step_ms=step,
        )
        warnings.extend(w_extra)

    if not candles:
        return CacheVerifyOutcome(
            inst_id=inst_id,
            bar=bar,
            ok=False,
            candle_count_before=count_before,
            candle_count_after=0,
            missing_bars_before=missing_before,
            missing_bars_after=missing_before,
            gap_segments_before=seg_before,
            gap_segments_after=0,
            range_fetch_calls=range_calls,
            did_full_history_fetch=did_full,
            warnings=tuple(warnings),
            error="补数后缓存为空，请检查磁盘或权限。",
        )

    count_after = len(candles)
    missing_after = total_missing_bars(gaps)
    seg_after = len(gaps)
    ok = missing_after == 0
    err: str | None = None
    if missing_after > 0:
        err = f"仍有约 {missing_after} 根缺口（{seg_after} 段），请检查网络或稍后重试。"

    return CacheVerifyOutcome(
        inst_id=inst_id,
        bar=bar,
        ok=ok,
        candle_count_before=count_before,
        candle_count_after=count_after,
        missing_bars_before=missing_before,
        missing_bars_after=missing_after,
        gap_segments_before=seg_before,
        gap_segments_after=seg_after,
        range_fetch_calls=range_calls,
        did_full_history_fetch=did_full,
        warnings=tuple(warnings),
        error=err,
    )
