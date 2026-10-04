"""Explainable swap observations for cached asset / DVOL overviews.

These are unbacktested rule templates, not execution signals. Indicators use
closed candles and the same EMA15 / MA50 as the overview charts.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Mapping, Sequence

from okx_quant.indicators import atr, ema, sma
from okx_quant.models import Candle


HOUR_MS = 3_600_000
DAY_MS = 24 * HOUR_MS
PERIOD_MS = {"1W": 7 * DAY_MS, "1D": DAY_MS, "4H": 4 * HOUR_MS, "1H": HOUR_MS}
REQUIRED_PERIODS = ("1D", "4H", "1H")
MINIMUM_BARS = 60
TREND_ATR_MARGIN = Decimal("0.2")


@dataclass(frozen=True)
class ContractTrend:
    period: str
    direction: str
    close: Decimal
    ema15: Decimal
    ma50: Decimal
    atr14: Decimal
    range_high: Decimal
    range_low: Decimal
    candle_ts: int


@dataclass(frozen=True)
class ContractRecommendation:
    analyzed_at_ms: int
    trends: tuple[ContractTrend, ...]
    direction: str
    strategy: str
    status: str
    reason: str
    entry_condition: str
    invalidation: str
    management: str
    volatility_note: str
    blockers: tuple[str, ...] = ()


def _closed_candles(candles: Sequence[Candle], period: str, now_ms: int) -> list[Candle]:
    step = PERIOD_MS[period]
    offset = -8 * HOUR_MS if period == "1D" else (-(3 * DAY_MS + 8 * HOUR_MS) if period == "1W" else 0)
    by_ts = {}
    for candle in candles:
        values = (candle.open, candle.high, candle.low, candle.close)
        if (candle.confirmed and candle.ts >= 0 and candle.ts + step <= now_ms
                and (candle.ts - offset) % step == 0
                and all(value.is_finite() and value > 0 for value in values)
                and candle.low <= min(candle.open, candle.close)
                and candle.high >= max(candle.open, candle.close)):
            by_ts[candle.ts] = candle
    return [by_ts[ts] for ts in sorted(by_ts)]


def _reading(period: str, candles: list[Candle]) -> ContractTrend:
    closes = [c.close for c in candles]
    fast = ema(closes, 15)
    slow = sma(closes, 50)[-1]
    volatility = atr(candles, 14)[-1]
    assert slow is not None and volatility is not None
    margin = volatility * TREND_ATR_MARGIN
    direction = "震荡"
    if closes[-1] > fast[-1] > slow and fast[-1] - slow > margin and fast[-1] > fast[-4]:
        direction = "偏多"
    elif closes[-1] < fast[-1] < slow and slow - fast[-1] > margin and fast[-1] < fast[-4]:
        direction = "偏空"
    return ContractTrend(period, direction, closes[-1], fast[-1], slow, volatility,
                         max(c.high for c in candles[-21:-1]), min(c.low for c in candles[-21:-1]), candles[-1].ts)


def _volatility_note(candles: Sequence[Candle], now_ms: int, *, asset_label: str) -> str:
    hourly = _closed_candles(candles, "1H", now_ms)
    if not hourly or now_ms - hourly[-1].ts - HOUR_MS > 2 * HOUR_MS:
        return f"DVOL 缺失或过期，波动背景未知；合约方向仍按 {asset_label} 判断。"
    latest = hourly[-1]
    prefix = f"DVOL {latest.close:.2f}%"
    recent = hourly[-25:]
    if len(recent) < 25 or any(b.ts - a.ts != HOUR_MS for a, b in zip(recent, recent[1:])):
        return prefix + "；24h 数据不足或有缺口，无法判断扩张/收缩。"
    change = latest.close - recent[0].close
    background = ("波动扩张，留意假突破及止损滑点" if change > 1 else
                  "波动收缩，等待价格确认，突破可能缺乏延续" if change < -1 else "波动变化平缓")
    return f"{prefix} · 24h {change:+.2f} 点；{background}。DVOL 不决定多空。"


def build_contract_recommendation(
    *, price_candles: Mapping[str, Sequence[Candle]], dvol_hourly: Sequence[Candle] = (), now_ms: int,
    asset_label: str = "BTC",
) -> ContractRecommendation:
    asset_label = asset_label.strip().upper()
    blockers = []
    trends = []
    closed = {}
    for period in PERIOD_MS:
        candles = _closed_candles(price_candles.get(period, ()), period, now_ms)
        closed[period] = candles
        issue = ""
        if len(candles) < MINIMUM_BARS:
            issue = f"{period} 已收盘有效K线不足 {MINIMUM_BARS} 根"
        elif any(b.ts - a.ts != PERIOD_MS[period] for a, b in zip(candles[-MINIMUM_BARS:], candles[-MINIMUM_BARS+1:])):
            issue = f"{period} 最近K线存在缺口"
        elif now_ms - candles[-1].ts - PERIOD_MS[period] > 2 * PERIOD_MS[period]:
            issue = f"{period} 数据过期"
        if issue:
            if period in REQUIRED_PERIODS:
                blockers.append(issue)
        else:
            trends.append(_reading(period, candles))

    direction, strategy, status = "观望", "等待行情确认", "暂无入场条件"
    reason = ""
    entry = "等待日线与4H同向，1H出现对应的回踩或突破确认。"
    invalidation = "未满足条件时不建立新仓。"
    management = "规则候选未经收益回测；只作人工判断参考，不自动下单。"
    if blockers:
        strategy, status = "等待数据更新", "数据不足，暂停推荐"
        reason = "；".join(blockers) + "。请先更新K线数据，再刷新总览。"
        entry = "补齐日线、4H、1H连续收盘K线后重新判断。"
    else:
        reading = {t.period: t for t in trends}
        daily, four_hour, hourly = (reading[p] for p in REQUIRED_PERIODS)
        reason = f"日线{daily.direction} / 4H{four_hour.direction} / 1H{hourly.direction}"
        weekly = reading.get("1W")
        if weekly is not None:
            reason += f" / 周线{weekly.direction}"
        else:
            reason += " / 周线背景不可用"
        if daily.direction == four_hour.direction and daily.direction in {"偏多", "偏空"}:
            bullish = daily.direction == "偏多"
            direction = daily.direction
            side = "做多" if bullish else "做空"
            if hourly.direction != direction:
                strategy, status = f"等待1H恢复后{side}", "执行周期尚未同向"
                entry = f"等待1H恢复{direction}（价格、EMA15、MA50顺序及EMA15斜率同向），再观察回踩/突破。"
                invalidation = "日线与4H不再同向时取消此方向观察。"
            else:
                latest = closed["1H"][-1]
                boundary = hourly.range_high if bullish else hourly.range_low
                breakout = hourly.close > boundary if bullish else hourly.close < boundary
                pullback = latest.low <= hourly.ema15 if bullish else latest.high >= hourly.ema15
                if breakout:
                    strategy, status = f"突破回测{side}", "1H突破已确认，等待回测"
                    location = "上沿" if bullish else "下沿"
                    recovery = "不破并收回其上方" if bullish else "受阻并收回其下方"
                    entry = f"前20根1H{location} {boundary:,.2f} 已突破；等待1H回测该价位{recovery}。"
                    invalidation = ("止损参考回测低点下方" if bullish else "止损参考回测高点上方") + "留0.2×ATR14缓冲；收回原区间或4H反转则失效。"
                else:
                    strategy = f"顺势回踩{side}"
                    status = "最近1H回踩已确认，等待触发" if pullback else "等待1H回踩确认"
                    if pullback:
                        trigger = latest.high if bullish else latest.low
                        stop = latest.low - hourly.atr14 * TREND_ATR_MARGIN if bullish else latest.high + hourly.atr14 * TREND_ATR_MARGIN
                        entry = f"最近收盘1H已触及EMA15并收回；等待{'向上突破' if bullish else '向下跌破'}确认K线{'高点' if bullish else '低点'} {trigger:,.2f}。"
                        invalidation = f"止损参考 {stop:,.2f}（确认K线极值外0.2×ATR14）；1H收盘{'跌破' if bullish else '站回'}MA50 {hourly.ma50:,.2f} 或4H反转则失效。"
                    else:
                        entry = f"等待1H触及EMA15 {hourly.ema15:,.2f} 后收回其{'上方' if bullish else '下方'}，再突破确认K线{'高点' if bullish else '低点'}。"
                        invalidation = f"止损参考确认K线{'低点下方' if bullish else '高点上方'}留0.2×ATR14缓冲；1H收盘{'跌破' if bullish else '站回'}MA50 {hourly.ma50:,.2f} 或4H反转则失效。"
                management = f"ATR14 {hourly.atr14:,.2f}；实际入场至止损为1R，2R作为目标观察；手续费、滑点和资金费另计。"
            if weekly is not None and weekly.direction in {"偏多", "偏空"} and weekly.direction != direction:
                reason += "；与周线相反，属于逆大周期交易"
        else:
            strategy, status = "观望，等待多周期同向", "日线/4H震荡或分歧"
            reason += "；目前缺少一致方向"

    return ContractRecommendation(now_ms, tuple(trends), direction, strategy, status, reason,
                                  entry, invalidation, management,
                                  _volatility_note(dvol_hourly, now_ms, asset_label=asset_label), tuple(blockers))
