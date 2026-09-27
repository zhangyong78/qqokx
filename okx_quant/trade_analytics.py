"""Read-only analysis of completed position lifecycles, with explicit valuation."""
from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Iterable, Mapping

from okx_quant.daily_trade_report import DailyTrade, REPORT_TIMEZONE, _datetime, _decimal, daily_trade_from_position_history

ZERO = Decimal("0")
CONTRACT_TYPES = {"SWAP", "FUTURES", "OPTION"}
PRODUCT_NAMES = {"SPOT": "现货", "SWAP": "永续", "FUTURES": "交割", "OPTION": "期权", "OTHER": "其他"}


def instrument_type(symbol: str) -> str:
    parts = symbol.upper().split("-")
    if parts[-1:] == ["SWAP"]:
        return "SWAP"
    if len(parts) >= 5 and parts[-1] in {"C", "P"}:
        return "OPTION"
    if len(parts) == 3 and parts[-1].isdigit():
        return "FUTURES"
    return "SPOT" if len(parts) == 2 else "OTHER"


def position_history_trade(item: object, *, profile_name: str, environment: str,
                           usdt_prices: Mapping[str, object] | None = None) -> DailyTrade:
    trade = daily_trade_from_position_history(item, api_name=profile_name, environment=environment)
    raw = getattr(item, "raw", {}) or {}
    kind = str(getattr(item, "inst_type", "") or raw.get("instType") or instrument_type(trade.symbol)).upper()
    parts = trade.symbol.upper().split("-")
    currency = str(raw.get("ccy") or raw.get("pnlCcy") or "").strip().upper()
    if currency in {"", "-"}:
        currency = parts[0] if kind == "OPTION" or (kind in CONTRACT_TYPES and len(parts) > 1 and parts[1] == "USD") else (parts[1] if len(parts) > 1 else "")
    native = _decimal(getattr(item, "realized_pnl", None))
    fee = _decimal(getattr(item, "fee", None))
    funding = _decimal(getattr(item, "funding_fee", None))
    if native is None:
        # A gross P&L alone is not a net P&L. Only reconstruct when required
        # components are supplied, including explicit zeroes.
        gross = _decimal(getattr(item, "pnl", None))
        if gross is not None and fee is not None and (kind != "SWAP" or funding is not None):
            native = gross + fee + (funding or ZERO)
            native += _decimal(raw.get("liqPenalty")) or ZERO
            native += _decimal(getattr(item, "settle_pnl", None)) or ZERO
    rate = Decimal("1") if currency == "USDT" else _decimal((usdt_prices or {}).get(currency))
    if rate is not None and (not rate.is_finite() or rate <= 0):
        rate = None
    note = "" if currency == "USDT" else (f"{currency} 按当前参考汇率折合 USDT，非历史成交汇率" if rate is not None else f"缺少 {currency or '结算币种'} 汇率，未计入金额合计")
    if native is None:
        note = "缺少已实现净盈亏，未计入金额合计"
    closing_type = str(raw.get("type") or "")
    meta = raw.get("qqokxHistoryMeta") or {}
    partial = closing_type in {"1", "4", "5"} or (not closing_type and bool(meta.get("isPartialClose")))
    opened = trade.opened_at or _datetime(raw.get("cTime"))
    key = f"{profile_name}:{environment}:{trade.symbol}:{raw.get('posId', '')}:{raw.get('cTime', '') or trade.trade_key}"
    convert = lambda value: value * rate if value is not None and rate is not None else None
    return replace(
        trade, trade_key=key, inst_type=kind, pnl_currency=currency, native_net_pnl=native,
        opened_at=opened, net_pnl=convert(native), fee=convert(fee), funding_fee=convert(funding),
        gross_pnl=convert(native - (fee or ZERO) - (funding or ZERO)) if native is not None else None,
        valuation_note=note, pnl_ratio=_decimal(raw.get("pnlRatio")), is_closed=not partial,
        status="部分平仓" if partial else ("已结算" if trade.closed_at is not None else "待核对"),
    )


def completed_trades(trades: Iterable[DailyTrade], start: date, end: date, *, contracts_only: bool = False) -> tuple[DailyTrade, ...]:
    return tuple(t for t in trades if t.is_closed and t.closed_at is not None
                 and start <= t.closed_at.astimezone(REPORT_TIMEZONE).date() <= end
                 and (not contracts_only or (t.inst_type or instrument_type(t.symbol)) in CONTRACT_TYPES))


@dataclass(frozen=True)
class TradeMetrics:
    closed_count: int
    valued_count: int
    win_count: int
    loss_count: int
    flat_count: int
    net_pnl: Decimal | None
    profit: Decimal | None
    loss: Decimal | None
    average_pnl: Decimal | None
    average_win: Decimal | None
    average_loss: Decimal | None
    win_rate: Decimal | None
    loss_rate: Decimal | None
    payoff_ratio: Decimal | None
    profit_factor: Decimal | None
    average_r: Decimal | None
    risk_count: int
    average_return: Decimal | None
    average_seconds: float | None
    winning_seconds: float | None
    losing_seconds: float | None
    max_win_streak: int
    max_loss_streak: int


def metrics(trades: Iterable[DailyTrade]) -> TradeMetrics:
    rows = sorted(trades, key=lambda t: (t.closed_at or datetime.min.replace(tzinfo=REPORT_TIMEZONE), t.trade_key))
    valued = [t for t in rows if t.net_pnl is not None]
    wins = [t for t in valued if t.net_pnl > 0]
    losses = [t for t in valued if t.net_pnl < 0]
    total = sum((t.net_pnl for t in valued), ZERO) if valued else None
    profit = sum((t.net_pnl for t in wins), ZERO) if valued else None
    loss = sum((t.net_pnl for t in losses), ZERO) if valued else None
    avg_win = profit / len(wins) if wins else None
    avg_loss = loss / len(losses) if losses else None
    win_streak = loss_streak = max_win = max_loss = 0
    for row in rows:
        win_streak = win_streak + 1 if row.net_pnl is not None and row.net_pnl > 0 else 0
        loss_streak = loss_streak + 1 if row.net_pnl is not None and row.net_pnl < 0 else 0
        max_win, max_loss = max(max_win, win_streak), max(max_loss, loss_streak)
    risk_rows = [t for t in valued if t.risk_amount is not None and t.risk_amount > 0]
    returns = [t.pnl_ratio for t in rows if t.pnl_ratio is not None]

    def holding(items):
        durations = [(t.closed_at - t.opened_at).total_seconds() for t in items
                     if t.closed_at is not None and t.opened_at is not None and t.closed_at >= t.opened_at]
        return sum(durations) / len(durations) if durations else None

    return TradeMetrics(
        len(rows), len(valued), len(wins), len(losses), sum(t.net_pnl == 0 for t in valued),
        total, profit, loss, total / len(valued) if valued else None, avg_win, avg_loss,
        Decimal(len(wins)) / len(valued) * 100 if valued else None,
        Decimal(len(losses)) / len(valued) * 100 if valued else None,
        avg_win / abs(avg_loss) if avg_win is not None and avg_loss else None,
        profit / abs(loss) if loss else None,
        sum((t.net_pnl / t.risk_amount for t in risk_rows), ZERO) / len(risk_rows) if risk_rows else None,
        len(risk_rows), sum(returns, ZERO) / len(returns) * 100 if returns else None,
        holding(rows), holding(wins), holding(losses), max_win, max_loss,
    )


def daily_pnl(trades: Iterable[DailyTrade]) -> dict[date, Decimal | None]:
    result: dict[date, Decimal | None] = {}
    for trade in trades:
        if trade.closed_at is None:
            continue
        day = trade.closed_at.astimezone(REPORT_TIMEZONE).date()
        result.setdefault(day, None)
        if trade.net_pnl is not None:
            result[day] = (result[day] or ZERO) + trade.net_pnl
    return result


def cumulative_pnl(trades: Iterable[DailyTrade], start: date, end: date) -> list[tuple[date, Decimal]]:
    days = daily_pnl(trades)
    if not any(value is not None for value in days.values()):
        return []
    points, total, day = [], ZERO, start
    while day <= end:
        total += days.get(day) or ZERO
        points.append((day, total))
        day += timedelta(days=1)
    return points


def grouped_trades(trades: Iterable[DailyTrade], group: str) -> dict[str, tuple[DailyTrade, ...]]:
    result: dict[str, list[DailyTrade]] = {}
    for trade in trades:
        if group == "asset":
            key = trade.symbol.split("-", 1)[0]
        elif group == "product":
            kind = trade.inst_type or instrument_type(trade.symbol)
            key = kind if kind in PRODUCT_NAMES else "OTHER"
        else:
            key = trade.symbol
        result.setdefault(key, []).append(trade)
    return {key: tuple(value) for key, value in result.items()}


def equity_points(records: Iterable[dict], start: date, end: date) -> list[tuple[datetime, Decimal]]:
    points = {}
    for record in records:
        stamp, value = _datetime(record.get("time")), _decimal(record.get("total_equity"))
        if stamp is not None and value is not None and value.is_finite() and start <= stamp.date() <= end:
            points[stamp] = value
    return sorted(points.items())


def opening_equity(records: Iterable[dict], start: date) -> Decimal | None:
    """Only a nearby sample at/before midnight is a valid reference denominator."""
    boundary = datetime.combine(start, datetime.min.time(), REPORT_TIMEZONE)
    candidates = []
    for record in records:
        stamp, value = _datetime(record.get("time")), _decimal(record.get("total_equity"))
        if stamp is not None and value is not None and value.is_finite() and value > 0 and timedelta(0) <= boundary - stamp <= timedelta(hours=1):
            candidates.append((stamp, value))
    return max(candidates)[1] if candidates else None
