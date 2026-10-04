"""Small, deterministic price-chart trade ideas. No orders, sizing or leverage."""
from __future__ import annotations

from dataclasses import dataclass, replace
from decimal import Decimal
from typing import Sequence

from okx_quant.indicators import atr, ema, sma
from okx_quant.models import Candle

HOUR_MS = 3_600_000
DAY_MS = 24 * HOUR_MS
STEPS = {"15M": HOUR_MS // 4, "1H": HOUR_MS, "4H": 4 * HOUR_MS,
         "1D": DAY_MS, "1DUTC": DAY_MS, "1W": 7 * DAY_MS}


@dataclass(frozen=True)
class KlineTradeIdea:
    symbol: str
    period: str
    analyzed_at_ms: int
    action: str = "观望"
    direction: str = "wait"
    status: str = "暂无明确入场条件"
    reasons: tuple[str, ...] = ()
    candle_ts: int | None = None
    close: Decimal | None = None
    atr_value: Decimal | None = None
    atr_percent: Decimal | None = None
    entry_low: Decimal | None = None
    entry_high: Decimal | None = None
    stop: Decimal | None = None
    target_2r: Decimal | None = None
    target_3r: Decimal | None = None
    worst_rr: Decimal | None = None
    trigger: str = ""
    invalidation: str = ""


def _positive(value: Decimal) -> bool:
    return isinstance(value, Decimal) and value.is_finite() and value > 0


def build_kline_trade_idea(*, symbol: str, period: str, candles: Sequence[Candle], now_ms: int) -> KlineTradeIdea:
    symbol = symbol.strip().upper()
    result = KlineTradeIdea(symbol, period, now_ms)
    parts = symbol.split("-")
    perpetual = len(parts) == 3 and parts[-1] == "SWAP"
    spot = len(parts) == 2 and all(parts) and parts[-1] != "DVOL" and not symbol.startswith("__")
    if not (perpetual or spot) or not all(parts):
        return replace(result, reasons=("仅支持普通现货和永续交易对；波动率指数、期权、交割合约不生成此类做单参考。",))
    normalized = period.upper()
    step = STEPS.get(normalized)
    if step is None:
        return replace(result, reasons=("当前周期暂不支持。",))
    offset = -8 * HOUR_MS if normalized == "1D" else (4 * DAY_MS - 8 * HOUR_MS if normalized == "1W" else 0)
    valid = {}
    for candle in candles:
        if (isinstance(candle, Candle) and candle.confirmed and candle.ts >= 0
                and candle.ts + step <= now_ms and (candle.ts-offset) % step == 0
                and all(_positive(v) for v in (candle.open, candle.high, candle.low, candle.close))
                and candle.low <= min(candle.open, candle.close)
                and candle.high >= max(candle.open, candle.close)):
            valid[candle.ts] = candle
    series = [valid[ts] for ts in sorted(valid)][-250:]
    if len(series) < 60:
        return replace(result, reasons=("普通、已收盘有效K线不足60根；请先加载当前交易对/周期。",))
    current = series[-1]
    result = replace(result, candle_ts=current.ts, close=current.close)
    if now_ms - current.ts - step > 2 * step:
        return replace(result, reasons=("K线缓存已过期；请点击“加载”刷新后重新查看做单参考。",))
    recent = series[-60:]
    if any(b.ts - a.ts != step for a, b in zip(recent, recent[1:])):
        return replace(result, reasons=("最近60根K线存在缺口，暂不生成入场价位。",))
    closes = [c.close for c in series]
    fast = ema(closes, 15)
    slow = sma(closes, 50)[-1]
    width = atr(series, 14)[-1]
    if slow is None or width is None or width <= 0:
        return replace(result, reasons=("无法计算有效ATR或均线。",))
    result = replace(result, atr_value=width, atr_percent=width/current.close*100)
    if result.atr_percent > 10:
        return replace(result, reasons=("当前ATR超过价格的10%，波动过大，先观望。",))
    long = current.close > fast[-1] > slow and fast[-1] > fast[-4] and fast[-1]-slow > width*Decimal("0.15")
    short = current.close < fast[-1] < slow and fast[-1] < fast[-4] and slow-fast[-1] > width*Decimal("0.15")
    if not (long or short):
        return replace(result, reasons=("EMA15、SMA50与收盘方向未形成一致趋势，等待条件明确。",))
    if short and spot:
        return replace(result, action="减仓/观望", status="现货偏空，仅已有持仓的风险观察",
                       reasons=("收盘低于EMA15和SMA50，EMA15向下。", "普通现货不推荐开空；没有持仓则观望，已有持仓可人工评估减仓。"),
                       invalidation="重新站回EMA15且趋势转强后再评估新增现货买入。")
    if abs(current.close-fast[-1]) > width*Decimal("1.5"):
        return replace(result, reasons=("方向偏多，但价格离EMA15过远，不追涨。" if long else "方向偏空，但价格离EMA15过远，不追空。",))
    entry_low = fast[-1]-width*Decimal("0.15")
    entry_high = fast[-1]+width*Decimal("0.15")
    stop = (min(min(c.low for c in series[-5:]), entry_low-width)-width*Decimal("0.15") if long
            else max(max(c.high for c in series[-5:]), entry_high+width)+width*Decimal("0.15"))
    risk = abs(fast[-1]-stop)
    if entry_low <= 0 or stop <= 0 or risk > 4 * width:
        return replace(result, reasons=("结构止损过远或价位无效，当前不适合构造此入场计划。",))
    sign = Decimal(1 if long else -1)
    target_2r = fast[-1] + sign*2*risk
    target_3r = fast[-1] + sign*3*risk
    if target_2r <= 0 or target_3r <= 0:
        return replace(result, reasons=("目标价格无效，当前不构造此入场计划。",))
    worst_entry = entry_high if long else entry_low
    worst_rr = abs(target_2r-worst_entry) / abs(worst_entry-stop)
    return replace(result, action="条件做多" if long and perpetual else "条件买入" if long else "条件做空",
                   direction="long" if long else "short", status="回踩入场候选，尚未确认，不是立即市价单",
                   reasons=("收盘、EMA15、SMA50偏多且EMA15向上。" if long else "收盘、EMA15、SMA50偏空且EMA15向下。",
                            "用ATR14和最近5根结构点规划止损；只有当前周期判断，尚无跨周期确认。"),
                   entry_low=entry_low, entry_high=entry_high, stop=stop, target_2r=target_2r,
                   target_3r=target_3r, worst_rr=worst_rr,
                   trigger="等待价格回到参考区间，随后新的一根普通K线收盘重新站上EMA15，再人工确认入场。" if long
                           else "等待价格反弹到参考区间，随后新的一根普通K线收盘重新跌回EMA15下方，再人工确认入场。",
                   invalidation="触及结构止损、趋势转向、直接远离入场区间或未触发时，取消候选，不追价。")
