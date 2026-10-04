"""Explainable option structure suggestions; no order or position side effects.

Rules follow the user's 期权策略执行体系 and 我的读书笔记. The numeric
classification thresholds below are configurable engineering defaults, not
backtested win rates. DVOL is a Deribit 30-day index, not an OKX contract IV.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import math
import statistics
from typing import Mapping, Sequence

from okx_quant.indicators import atr, ema
from okx_quant.models import Candle
from okx_quant.option_strategy import OptionQuote, StrategyLegDefinition, parse_option_contract
from okx_quant.option_roll import OptionRollTransferPayload

HOUR_MS = 3_600_000
DAY_MS = 24 * HOUR_MS
PERIOD_MS = {"15m": HOUR_MS // 4, "1H": HOUR_MS, "4H": 4 * HOUR_MS, "1D": DAY_MS}


@dataclass(frozen=True)
class RecommendationRules:
    low_percentile: Decimal = Decimal("30")
    high_percentile: Decimal = Decimal("70")
    history_days: int = 90
    minimum_history_days: int = 60
    minimum_trend_bars: int = 60
    max_spread_ratio: Decimal = Decimal("0.30")


@dataclass(frozen=True)
class TrendReading:
    period: str
    direction: str
    close: Decimal
    fast: Decimal
    slow: Decimal
    breakout: str
    pullback: bool
    candle_ts: int


@dataclass(frozen=True)
class SuggestedLeg:
    inst_id: str
    side: str
    ratio: int
    strike: Decimal
    reference_price: Decimal
    quote: OptionQuote | None = None


@dataclass(frozen=True)
class StrategySuggestion:
    key: str
    name: str
    structure: str
    dte_min: int
    dte_max: int
    reason: str
    exit_condition: str
    risk: str
    source: str
    high_risk: bool = False
    legs: tuple[SuggestedLeg, ...] = ()
    expiry: str = ""
    dte: Decimal | None = None
    contract_note: str = ""


@dataclass(frozen=True)
class RecommendationSnapshot:
    family: str
    analyzed_at_ms: int
    trends: tuple[TrendReading, ...]
    dvol: Decimal | None
    dvol_change_points: Decimal | None
    dvol_percentile: Decimal | None
    history_days: int
    realized_vol_5: Decimal | None
    realized_vol_20: Decimal | None
    suggestions: tuple[StrategySuggestion, ...]
    blockers: tuple[str, ...]
    notes: tuple[str, ...]
    dvol_candle_ts: int | None = None


def build_recommendation_analysis_payload(
    snapshot: RecommendationSnapshot, suggestion: StrategySuggestion, *, base_quantity: Decimal, now_ms: int,
) -> OptionRollTransferPayload:
    """Prepare a local analysis draft, retaining bid/ask reference entry costs."""
    if snapshot.blockers or suggestion not in snapshot.suggestions or not suggestion.legs:
        raise ValueError("该推荐没有可带入的完整合约结构，请重新分析行情。")
    if now_ms - snapshot.analyzed_at_ms > 300_000 or snapshot.analyzed_at_ms - now_ms > 60_000:
        raise ValueError("推荐结果已过期，请重新分析当前行情后再带入。")
    if not _valid_price(base_quantity):
        raise ValueError("默认数量（张）必须为有限正数。")
    if option_days_to_expiry(suggestion.expiry, now_ms) <= 0:
        raise ValueError("推荐合约已到期，请重新分析行情。")
    reference = next((t.close for t in snapshot.trends if t.period == "1H"), None)
    legs, instruments, quotes = [], [], []
    faces = set()
    for index, leg in enumerate(suggestion.legs, 1):
        quote = leg.quote
        if quote is None or quote.instrument.inst_id != leg.inst_id:
            raise ValueError("推荐合约的面值或报价信息不完整，请重新分析行情。")
        parsed = parse_option_contract(leg.inst_id)
        if (parsed.inst_family != snapshot.family or parsed.expiry_code != suggestion.expiry
                or parsed.strike != leg.strike or leg.side not in {"buy", "sell"}
                or type(leg.ratio) is not int or leg.ratio <= 0 or not _valid_price(leg.reference_price)):
            raise ValueError("推荐合约、方向、比例或参考权利金无效。")
        instrument = quote.instrument
        mult = instrument.ct_mult if instrument.ct_mult is not None else Decimal("1")
        if instrument.state != "live" or not _valid_price(instrument.ct_val) or not _valid_price(mult):
            raise ValueError("推荐合约已失效或面值无效。")
        faces.add((instrument.ct_val, mult, instrument.ct_val_ccy, instrument.settle_ccy))
        if not _valid_price(quote.index_price):
            if not _valid_price(reference):
                raise ValueError("缺少有效标的价格，无法带入分析。")
            quote = replace(quote, index_price=reference)
        legs.append(StrategyLegDefinition(f"L{index}", leg.inst_id, leg.side,
                                          base_quantity * leg.ratio, premium=leg.reference_price))
        instruments.append(instrument)
        quotes.append(quote)
    if len(faces) != 1:
        raise ValueError("推荐结构的面值或结算币不一致，无法带入分析。")
    return OptionRollTransferPayload(
        strategy_name=f"推荐分析-{suggestion.name}-{suggestion.expiry}", option_family=snapshot.family,
        expiry_code=suggestion.expiry, legs=tuple(legs), instruments=tuple(instruments), quotes=tuple(quotes),
    )


def _valid_price(value: Decimal | None) -> bool:
    return value is not None and value.is_finite() and value > 0


def _closed_series(candles: Sequence[Candle], period_ms: int, now_ms: int) -> list[Candle]:
    by_ts: dict[int, Candle] = {}
    offset = -8 * HOUR_MS if period_ms == DAY_MS else 0
    for candle in candles:
        values = (candle.open, candle.high, candle.low, candle.close)
        if (
            candle.confirmed and candle.ts >= 0 and candle.ts + period_ms <= now_ms
            and (candle.ts - offset) % period_ms == 0
            and all(_valid_price(value) for value in values)
            and candle.low <= min(candle.open, candle.close)
            and candle.high >= max(candle.open, candle.close)
            and candle.low <= candle.high
        ):
            by_ts[candle.ts] = candle
    return [by_ts[ts] for ts in sorted(by_ts)]


def _trend(period: str, candles: list[Candle]) -> TrendReading:
    closes = [candle.close for candle in candles]
    fast, slow = ema(closes, 21), ema(closes, 55)
    close = closes[-1]
    width = (atr(candles, 14)[-1] or close * Decimal("0.01")) * Decimal("0.20")
    direction = "震荡"
    if fast[-1] - slow[-1] > width and close > slow[-1] and fast[-1] > fast[-4]:
        direction = "偏多"
    elif slow[-1] - fast[-1] > width and close < slow[-1] and fast[-1] < fast[-4]:
        direction = "偏空"
    previous = candles[-21:-1]
    breakout = "向上突破" if close > max(c.high for c in previous) else (
        "向下突破" if close < min(c.low for c in previous) else "无突破"
    )
    current = candles[-1]
    pullback = (
        direction == "偏多" and current.low <= slow[-1] * Decimal("1.003") and close >= fast[-1]
    ) or (
        direction == "偏空" and current.high >= slow[-1] * Decimal("0.997") and close <= fast[-1]
    )
    return TrendReading(period, direction, close, fast[-1], slow[-1], breakout, pullback, current.ts)


def _daily_dvol_closes(hourly: list[Candle]) -> list[Decimal]:
    """Complete 24-hour days beginning at UTC+8 midnight; no partial days."""
    groups: dict[int, list[Candle]] = {}
    offset = -8 * HOUR_MS
    for candle in hourly:
        day_start = ((candle.ts - offset) // DAY_MS) * DAY_MS + offset
        groups.setdefault(day_start, []).append(candle)
    result: list[Decimal] = []
    for start in sorted(groups):
        day = groups[start]
        if len(day) == 24 and all(c.ts == start + i * HOUR_MS for i, c in enumerate(day)):
            result.append(day[-1].close)
    return result


def _realized_vol(candles: list[Candle], days: int) -> Decimal | None:
    if len(candles) < days + 1:
        return None
    closes = [float(c.close) for c in candles[-days-1:]]
    returns = [math.log(right / left) for left, right in zip(closes, closes[1:])]
    return Decimal(str(statistics.stdev(returns) * math.sqrt(365) * 100))


def option_days_to_expiry(expiry: str, now_ms: int) -> Decimal:
    """OKX options expire at 08:00 UTC (16:00 UTC+8) on the expiry date."""
    date = datetime.strptime(expiry, "%y%m%d").replace(hour=8, tzinfo=timezone.utc)
    return Decimal(int(date.timestamp() * 1000) - now_ms) / Decimal(DAY_MS)


def _match_contracts(
    suggestion: StrategySuggestion, family: str, spot: Decimal, quotes: Sequence[OptionQuote],
    now_ms: int, rules: RecommendationRules,
) -> StrategySuggestion:
    from dataclasses import replace

    by_expiry: dict[str, dict[tuple[str, Decimal], OptionQuote]] = {}
    for quote in quotes:
        try:
            parsed = parse_option_contract(quote.instrument.inst_id)
            dte = option_days_to_expiry(parsed.expiry_code, now_ms)
            if not _valid_price(parsed.strike):
                continue
        except (ValueError, InvalidOperation):
            continue
        if parsed.inst_family != family or quote.instrument.state != "live":
            continue
        if not (_valid_price(quote.bid_price) and _valid_price(quote.ask_price)):
            continue
        bid, ask = quote.bid_price, quote.ask_price
        assert bid is not None and ask is not None
        if ask < bid or (ask - bid) / ((ask + bid) / 2) > rules.max_spread_ratio:
            continue
        if suggestion.dte_min <= dte <= suggestion.dte_max:
            by_expiry.setdefault(parsed.expiry_code, {})[(parsed.option_type, parsed.strike)] = quote

    expiries = sorted(by_expiry, key=lambda expiry: abs(
        option_days_to_expiry(expiry, now_ms) - Decimal(suggestion.dte_min + suggestion.dte_max) / 2
    ))
    for expiry in expiries:
        available = by_expiry[expiry]
        strikes = sorted({strike for _, strike in available})
        paired = [strike for strike in strikes if ("C", strike) in available and ("P", strike) in available]
        key = suggestion.key
        option_type = "P" if key.startswith("put") or key == "bear_put" else "C"
        typed = [strike for strike in strikes if (option_type, strike) in available]
        if key.endswith("straddle"):
            if not paired:
                continue
            atm = min(paired, key=lambda strike: abs(strike - spot))
            side = "sell" if key.startswith("short") else "buy"
            specs = [("C", atm, side, 1), ("P", atm, side, 1)]
        elif key.endswith("strangle"):
            puts = [strike for strike in strikes if strike < spot and ("P", strike) in available]
            calls = [strike for strike in strikes if strike > spot and ("C", strike) in available]
            if not puts or not calls:
                continue
            side = "sell" if key.startswith("short") else "buy"
            specs = [("P", max(puts), side, 1), ("C", min(calls), side, 1)]
        else:
            if len(typed) < 2:
                continue
            atm = min(typed, key=lambda strike: abs(strike - spot))
            others = [strike for strike in typed if strike > atm] if option_type == "C" else [strike for strike in typed if strike < atm]
            if not others:
                continue
            outer = min(others) if option_type == "C" else max(others)
            if "backspread" in key:
                specs = [(option_type, atm, "sell", 1), (option_type, outer, "buy", 2)]
            else:
                specs = [(option_type, atm, "buy", 1), (option_type, outer, "sell", 2 if "ratio" in key else 1)]
        selected = [available[(ot, strike)] for ot, strike, _, _ in specs]
        values = [(q.instrument.ct_val, q.instrument.ct_mult if q.instrument.ct_mult is not None else Decimal("1"), q.instrument.ct_val_ccy, q.instrument.settle_ccy) for q in selected]
        if any(not _valid_price(item[0]) or not _valid_price(item[1]) for item in values) or len(set(values)) != 1:
            continue
        legs = tuple(SuggestedLeg(q.instrument.inst_id, side, ratio, strike,
                                 q.ask_price if side == "buy" else q.bid_price, quote=q)
                     for q, (_, strike, side, ratio) in zip(selected, specs))
        return replace(suggestion, legs=legs, expiry=expiry, dte=option_days_to_expiry(expiry, now_ms),
                       contract_note="同到期日、同面值；买入按卖一、卖出按买一参考。数量为结构比例，非实际建议仓位。")
    return replace(suggestion, contract_note="暂无满足到期区间且双边报价价差不超过30%的完整结构；等待期权链确认。")


def build_option_recommendations(
    *, family: str, price_candles: Mapping[str, Sequence[Candle]], dvol_hourly: Sequence[Candle],
    quotes: Sequence[OptionQuote] = (), now_ms: int, rules: RecommendationRules = RecommendationRules(),
) -> RecommendationSnapshot:
    family = family.strip().upper()
    blockers: list[str] = []
    notes = ["规则候选未经收益回测，匹配条件不代表胜率。DVOL为Deribit 30天隐含波动率指数，不等于OKX具体合约IV。",
             "日线按UTC+8；高低波动率以最近90个完整日的分位判断（低≤30%，高≥70%）；EMA21/55来自执行体系。",
             "工程默认条件：EMA差值超过0.2×ATR14且EMA21斜率同向；突破取前20根高低；DVOL变化超过±1点/24h。阈值尚未回测。",
             "未读取事件日历和各到期日IV/偏度，日历/对角价差暂不推荐。"]
    if family not in {"BTC-USD", "ETH-USD"}:
        blockers.append("当前规则仅支持BTC-USD、ETH-USD币本位期权，其他系列暂不推荐。")
    trends: list[TrendReading] = []
    closed: dict[str, list[Candle]] = {}
    for period, period_ms in PERIOD_MS.items():
        candles = _closed_series(price_candles.get(period, ()), period_ms, now_ms)
        closed[period] = candles
        if len(candles) < rules.minimum_trend_bars:
            blockers.append(f"{period} 已收盘有效K线少于{rules.minimum_trend_bars}根。")
            continue
        recent = candles[-rules.minimum_trend_bars:]
        if any(right.ts - left.ts != period_ms for left, right in zip(recent, recent[1:])):
            blockers.append(f"{period} 最近K线存在缺口。")
        if now_ms - candles[-1].ts - period_ms > 2 * period_ms:
            blockers.append(f"{period} 数据过期，请刷新行情。")
        trends.append(_trend(period, candles))
    hourly = [candle for candle in _closed_series(dvol_hourly, HOUR_MS, now_ms)
              if candle.ts >= now_ms - (rules.history_days + 2) * DAY_MS]
    dvol = hourly[-1].close if hourly else None
    dvol_change = None
    daily_vol = _daily_dvol_closes(hourly)[-rules.history_days:]
    percentile = None
    if len(daily_vol) < rules.minimum_history_days:
        blockers.append(f"DVOL完整日样本不足{rules.minimum_history_days}天，无法判断相对高低。")
    elif dvol is not None:
        less = sum(value < dvol for value in daily_vol)
        equal = sum(value == dvol for value in daily_vol)
        percentile = (Decimal(less) + Decimal(equal) / 2) * 100 / len(daily_vol)
    if not hourly or now_ms - hourly[-1].ts - HOUR_MS > 2 * HOUR_MS:
        blockers.append("DVOL小时线缺失或过期，请刷新波动率。")
    elif len(hourly) >= 25 and all(right.ts - left.ts == HOUR_MS for left, right in zip(hourly[-25:], hourly[-24:])):
        dvol_change = hourly[-1].close - hourly[-25].close
    else:
        blockers.append("DVOL最近24小时存在缺口，无法判断变化。")
    hv5, hv20 = _realized_vol(closed["1D"], 5), _realized_vol(closed["1D"], 20)
    suggestions: list[StrategySuggestion] = []
    if not blockers:
        reading = {t.period: t for t in trends}
        daily, hourly_trend, four_hour, entry = (reading[p] for p in ("1D", "1H", "4H", "15m"))
        bullish = all(t.direction == "偏多" for t in (daily, hourly_trend, four_hour))
        bearish = all(t.direction == "偏空" for t in (daily, hourly_trend, four_hour))
        low = percentile is not None and percentile <= rules.low_percentile
        high = percentile is not None and percentile >= rules.high_percentile
        rising = dvol_change is not None and dvol_change > 1
        falling = dvol_change is not None and dvol_change < -1
        breakout = hourly_trend.breakout != "无突破"

        def add(key: str, name: str, structure: str, dte: tuple[int, int], reason: str,
                exit_condition: str, risk: str, pages: str, high_risk: bool = False) -> None:
            suggestion = StrategySuggestion(key, name, structure, *dte, reason, exit_condition,
                                            risk, pages, high_risk)
            suggestions.append(_match_contracts(suggestion, family, hourly_trend.close, quotes, now_ms, rules))

        if bullish or bearish:
            bull = bullish
            entry_note = "15m回踩条件已满足" if entry.pullback and entry.direction == hourly_trend.direction else "15m回踩尚未确认，先观察"
            add("bull_call" if bull else "bear_put", "牛市认购价差" if bull else "熊市认沽价差",
                "买1低行权价Call + 卖1高行权价Call" if bull else "买1高行权价Put + 卖1低行权价Put",
                (30, 60), f"日线、4H与1H方向一致；{entry_note}。",
                "1H方向反转或15m EMA21/55反向交叉时重新评估；不足14DTE进入减仓观察。",
                "仍有趋势反转、价差成交和时间衰减风险；到期图不能替代持仓期间风险。", "执行体系第1-6、14-16页")
        if low and (breakout or rising):
            add("long_straddle", "平值双买", "买1平值Call + 买1同执行价Put", (20, 45),
                "DVOL处于历史低分位，已出现1H突破或DVOL抬升；关注波动扩张。",
                "突破失败、DVOL回落或行情未扩张时重估；注意Theta消耗。",
                "两腿权利金均可能损失；低波动率不保证未来会有大行情。", "执行体系第22-24页；读书笔记第39、43、89页")
            add("long_strangle", "虚值双买", "买1虚值Put + 买1虚值Call", (20, 45),
                "低DVOL且波动扩张条件初步出现，可与平值双买比较成本。",
                "价格没有持续突破、DVOL回落时重估。", "成本较低但需要更大价格移动，两腿仍可能同时亏损。",
                "执行体系第25-26页")
        if low and rising and breakout:
            if hourly_trend.breakout == "向上突破" and four_hour.direction == "偏多":
                add("call_backspread", "认购反比例价差", "卖1低行权价Call + 买2高行权价Call", (20, 45),
                    "低DVOL、向上突破与4H偏多同时出现，DVOL正在抬升。",
                    "加速失败或价格停留在买方行权价附近时重估；不足14DTE减仓观察。",
                    "中间价格区域会有亏损凹谷；结构是否净付费及实际Greeks需要合约报价确认。", "执行体系第19-20页")
            elif hourly_trend.breakout == "向下突破" and four_hour.direction == "偏空":
                add("put_backspread", "认沽反比例价差", "卖1高行权价Put + 买2低行权价Put", (20, 45),
                    "低DVOL、向下突破与4H偏空同时出现，DVOL正在抬升。",
                    "下跌加速失败、反弹或DVOL回落时重估；不足14DTE减仓观察。",
                    "中间价格区域可能亏损；方向正确但幅度不足也可能亏损。", "执行体系第21-22页")
        cooling = hv5 is not None and hv20 is not None and hv5 < hv20
        if high and falling and cooling and not breakout:
            if bullish or bearish:
                add("call_ratio" if bullish else "put_ratio", "认购比例价差" if bullish else "认沽比例价差",
                    "买1低行权价Call + 卖2高行权价Call" if bullish else "买1高行权价Put + 卖2低行权价Put",
                    (20, 45), "DVOL高分位回落、HV5低于HV20，趋势尚在且未发生1H突破。",
                    "接近卖方行权价主动评估退出；突破卖方行权价或不足14DTE进入防守。",
                    "存在未覆盖卖方敞口，急涨/急跌可能扩大损失；保证金及强平风险需单独核对。",
                    "执行体系第7-13、17-18页；读书笔记第7、47页", True)
            elif daily.direction == "震荡" and four_hour.direction == "震荡" and hourly_trend.direction == "震荡":
                for key, name, structure in (
                    ("short_strangle", "虚值双卖", "卖1虚值Put + 卖1虚值Call"),
                    ("short_straddle", "平值双卖", "卖1平值Put + 卖1同执行价Call"),
                ):
                    add(key, name, structure, (20, 45), "多周期震荡、DVOL高位回落且HV5低于HV20。",
                        "突破震荡区间或波动重新扩张即重新评估；不足14DTE进入防守。",
                        "裸露双卖：单边行情、跳空及保证金变化可造成重大损失；不可只看收取的权利金。",
                        "执行体系第24-28页；读书笔记第1、89页", True)
        if not suggestions:
            notes.append("当前未同时满足候选结构的条件，建议等待趋势/回踩或波动率变化确认。")
        notes.append("HV5/HV20按UTC+8日收盘对数收益、365天年化；DVOL与HV期限不同，仅用于背景比较。")
    return RecommendationSnapshot(family, now_ms, tuple(trends), dvol, dvol_change, percentile,
                                  len(daily_vol), hv5, hv20, tuple(suggestions), tuple(blockers),
                                  tuple(notes), hourly[-1].ts if hourly else None)
