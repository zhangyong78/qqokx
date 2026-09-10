from __future__ import annotations

"""Shared daily trade reporting rules for the Tk server UI and Qt local UI."""

import csv
import html
import io
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Iterable, Mapping, Sequence


REPORT_TIMEZONE = timezone(timedelta(hours=8), name="Asia/Shanghai")


def _decimal(value: object | None) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _datetime(value: object | None) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, (int, float)):
        number = float(value)
        if number > 10_000_000_000:
            number /= 1000
        parsed = datetime.fromtimestamp(number, tz=timezone.utc)
    else:
        text = str(value).strip()
        try:
            if text.isdigit():
                return _datetime(int(text))
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except (TypeError, ValueError, OverflowError):
            return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=REPORT_TIMEZONE)
    return parsed.astimezone(REPORT_TIMEZONE)


def _raw_datetime(raw: Mapping[str, object], *keys: str) -> datetime | None:
    for key in keys:
        parsed = _datetime(raw.get(key))
        if parsed is not None:
            return parsed
    return None


def _normalize_trade_datetimes(trade: "DailyTrade") -> "DailyTrade":
    """Return a report trade whose timestamps are consistently timezone-aware.

    Older ledger/cache records can carry naive ``datetime`` objects while newer
    records use Asia/Shanghai-aware values.  Normalizing at the report boundary
    keeps sorting and date grouping safe for mixed historical data.
    """
    opened_at = _datetime(trade.opened_at)
    closed_at = _datetime(trade.closed_at)
    if opened_at == trade.opened_at and closed_at == trade.closed_at:
        return trade
    return replace(trade, opened_at=opened_at, closed_at=closed_at)


def _sum_decimal(*values: Decimal | None) -> Decimal | None:
    present = [value for value in values if value is not None]
    return sum(present, Decimal("0")) if present else None


@dataclass(frozen=True)
class DailyTrade:
    trade_key: str
    api_name: str
    environment: str
    symbol: str
    strategy_name: str
    session_id: str
    direction: str
    opened_at: datetime | None
    closed_at: datetime | None
    entry_price: Decimal | None
    exit_price: Decimal | None
    size: Decimal | None
    gross_pnl: Decimal | None
    fee: Decimal | None
    funding_fee: Decimal | None
    net_pnl: Decimal | None
    close_reason: str
    status: str
    source: str


@dataclass(frozen=True)
class DailyTradeSummary:
    report_date: date
    api_name: str
    opened_count: int
    closed_count: int
    win_count: int
    loss_count: int
    gross_pnl: Decimal
    fee: Decimal
    funding_fee: Decimal
    net_pnl: Decimal
    open_count: int


@dataclass(frozen=True)
class DailyGroupSummary:
    report_date: date
    api_name: str
    group_name: str
    symbol: str
    closed_count: int
    win_count: int
    loss_count: int
    net_pnl: Decimal


@dataclass(frozen=True)
class DailyTradeReport:
    start_date: date
    end_date: date
    trades: tuple[DailyTrade, ...]
    daily: tuple[DailyTradeSummary, ...]
    by_symbol: tuple[DailyGroupSummary, ...]
    by_strategy: tuple[DailyGroupSummary, ...]


def daily_trade_from_strategy_ledger(record: object, *, source: str = "策略账本") -> DailyTrade:
    fee = _sum_decimal(_decimal(getattr(record, "entry_fee", None)), _decimal(getattr(record, "exit_fee", None))) or Decimal("0")
    funding_fee = _decimal(getattr(record, "funding_fee", None)) or Decimal("0")
    gross = _decimal(getattr(record, "gross_pnl", None))
    net = _decimal(getattr(record, "net_pnl", None))
    if gross is None and net is not None:
        gross = net - fee - funding_fee
    if net is None and gross is not None:
        net = gross + fee + funding_fee
    return DailyTrade(
        trade_key=str(getattr(record, "record_id", "") or getattr(record, "round_id", "") or "trade"),
        api_name=str(getattr(record, "api_name", "") or "-").strip() or "-",
        environment=str(getattr(record, "environment", "") or "live").strip() or "live",
        symbol=str(getattr(record, "symbol", "") or "-").strip() or "-",
        strategy_name=str(getattr(record, "strategy_name", "") or "-").strip() or "-",
        session_id=str(getattr(record, "session_id", "") or "-").strip() or "-",
        direction=str(getattr(record, "direction_label", "") or "-").strip() or "-",
        opened_at=_datetime(getattr(record, "opened_at", None)),
        closed_at=_datetime(getattr(record, "closed_at", None)),
        entry_price=_decimal(getattr(record, "entry_price", None)),
        exit_price=_decimal(getattr(record, "exit_price", None)),
        size=_decimal(getattr(record, "size", None)),
        gross_pnl=_decimal(gross),
        fee=fee,
        funding_fee=funding_fee,
        net_pnl=_decimal(net),
        close_reason=str(getattr(record, "close_reason", "") or "-").strip() or "-",
        status="已结算" if getattr(record, "closed_at", None) is not None else "持仓中",
        source=source,
    )


def daily_trade_from_position_history(
    item: object,
    *,
    api_name: str,
    environment: str,
    source: str = "OKX历史仓位",
) -> DailyTrade:
    raw = getattr(item, "raw", {})
    raw = raw if isinstance(raw, Mapping) else {}
    closed_at = _datetime(getattr(item, "update_time", None)) or _raw_datetime(raw, "uTime", "closeTime", "cTime")
    opened_at = _raw_datetime(raw, "openTime", "openTimeMs", "startTime", "beginTime")
    net = _decimal(getattr(item, "realized_pnl", None))
    if net is None:
        net = _decimal(getattr(item, "pnl", None)) or _decimal(getattr(item, "settle_pnl", None))
    fee = _decimal(getattr(item, "fee", None)) or Decimal("0")
    funding_fee = _decimal(getattr(item, "funding_fee", None)) or Decimal("0")
    gross = net - fee - funding_fee if net is not None else None
    return DailyTrade(
        trade_key=str(raw.get("posId") or raw.get("cTime") or f"{api_name}:{getattr(item, 'inst_id', '')}:{closed_at or '-'}"),
        api_name=str(api_name or "-").strip() or "-",
        environment=str(environment or "live").strip() or "live",
        symbol=str(getattr(item, "inst_id", "") or "-").strip() or "-",
        strategy_name="交易所历史（未关联策略）",
        session_id="-",
        direction=str(getattr(item, "direction", None) or getattr(item, "pos_side", None) or "-").strip() or "-",
        opened_at=opened_at,
        closed_at=closed_at,
        entry_price=_decimal(getattr(item, "open_avg_price", None)),
        exit_price=_decimal(getattr(item, "close_avg_price", None)),
        size=_decimal(getattr(item, "close_size", None)),
        gross_pnl=gross,
        fee=fee,
        funding_fee=funding_fee,
        net_pnl=net,
        close_reason=str(raw.get("closeType") or raw.get("closeReason") or "-").strip() or "-",
        status="已结算" if closed_at is not None else "待核对",
        source=source,
    )


def _empty_daily(report_date: date, api_name: str) -> dict[str, object]:
    return {
        "report_date": report_date,
        "api_name": api_name,
        "opened_count": 0,
        "closed_count": 0,
        "win_count": 0,
        "loss_count": 0,
        "gross_pnl": Decimal("0"),
        "fee": Decimal("0"),
        "funding_fee": Decimal("0"),
        "net_pnl": Decimal("0"),
        "open_count": 0,
    }


def _in_range(value: datetime | None, start_date: date, end_date: date) -> bool:
    return value is not None and start_date <= value.date() <= end_date


def build_daily_trade_report(
    trades: Iterable[DailyTrade],
    *,
    start_date: date,
    end_date: date,
    api_name: str = "全部API",
    asset_filter: str = "全部币种",
) -> DailyTradeReport:
    selected_asset = str(asset_filter or "全部币种").strip().upper()
    normalized_trades = (_normalize_trade_datetimes(trade) for trade in trades)
    selected = [
        trade
        for trade in normalized_trades
        if (_in_range(trade.opened_at, start_date, end_date) or _in_range(trade.closed_at, start_date, end_date))
        and (api_name == "全部API" or trade.api_name.casefold() == api_name.casefold())
        and (
            selected_asset in {"", "全部币种", "ALL", "全部"}
            or str(trade.symbol or "").strip().upper().split("-", 1)[0] == selected_asset
        )
    ]
    selected.sort(key=lambda trade: (trade.closed_at or trade.opened_at or datetime.min.replace(tzinfo=REPORT_TIMEZONE), trade.trade_key), reverse=True)
    daily_values: dict[tuple[date, str], dict[str, object]] = {}
    symbol_values: dict[tuple[date, str, str], dict[str, object]] = {}
    strategy_values: dict[tuple[date, str, str], dict[str, object]] = {}
    for trade in selected:
        api = trade.api_name
        if _in_range(trade.opened_at, start_date, end_date):
            key = (trade.opened_at.date(), api)
            daily_values.setdefault(key, _empty_daily(*key))
            daily_values[key]["opened_count"] = int(daily_values[key]["opened_count"]) + 1
            if trade.closed_at is None:
                daily_values[key]["open_count"] = int(daily_values[key]["open_count"]) + 1
        if not _in_range(trade.closed_at, start_date, end_date):
            continue
        report_date = trade.closed_at.date()
        key = (report_date, api)
        daily_values.setdefault(key, _empty_daily(*key))
        daily = daily_values[key]
        daily["closed_count"] = int(daily["closed_count"]) + 1
        net = trade.net_pnl or Decimal("0")
        daily["win_count"] = int(daily["win_count"]) + (1 if net > 0 else 0)
        daily["loss_count"] = int(daily["loss_count"]) + (1 if net < 0 else 0)
        daily["gross_pnl"] = daily["gross_pnl"] + (trade.gross_pnl or Decimal("0"))
        daily["fee"] = daily["fee"] + (trade.fee or Decimal("0"))
        daily["funding_fee"] = daily["funding_fee"] + (trade.funding_fee or Decimal("0"))
        daily["net_pnl"] = daily["net_pnl"] + net
        for bucket, group_name in ((symbol_values, trade.symbol), (strategy_values, trade.strategy_name)):
            group_key = (report_date, api, group_name)
            row = bucket.setdefault(
                group_key,
                {
                    "report_date": report_date,
                    "api_name": api,
                    "group_name": group_name,
                    "symbol": trade.symbol,
                    "closed_count": 0,
                    "win_count": 0,
                    "loss_count": 0,
                    "net_pnl": Decimal("0"),
                },
            )
            row["closed_count"] = int(row["closed_count"]) + 1
            row["win_count"] = int(row["win_count"]) + (1 if net > 0 else 0)
            row["loss_count"] = int(row["loss_count"]) + (1 if net < 0 else 0)
            row["net_pnl"] = row["net_pnl"] + net

    daily = tuple(
        DailyTradeSummary(**row)
        for row in sorted(daily_values.values(), key=lambda row: (row["report_date"], row["api_name"]), reverse=True)
    )
    by_symbol = tuple(
        DailyGroupSummary(**row)
        for row in sorted(symbol_values.values(), key=lambda row: (row["report_date"], row["net_pnl"]), reverse=True)
    )
    by_strategy = tuple(
        DailyGroupSummary(**row)
        for row in sorted(strategy_values.values(), key=lambda row: (row["report_date"], row["net_pnl"]), reverse=True)
    )
    return DailyTradeReport(start_date, end_date, tuple(selected), daily, by_symbol, by_strategy)


def format_report_decimal(value: Decimal | None, *, signed: bool = False) -> str:
    if value is None:
        return "-"
    quantized = value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    prefix = "+" if signed and quantized > 0 else ""
    return f"{prefix}{quantized:.2f}U"


def format_report_price(value: Decimal | None, symbol: str = "") -> str:
    """Format report prices without exposing raw average-fill float noise."""
    if value is None:
        return "-"
    asset = str(symbol or "").strip().upper().split("-", 1)[0]
    places = 5 if asset == "DOGE" else 8
    quantized = value.quantize(Decimal("1").scaleb(-places), rounding=ROUND_HALF_UP)
    text = format(quantized, "f").rstrip("0").rstrip(".")
    return text or "0"


def report_to_csv(report: DailyTradeReport) -> str:
    output = io.StringIO(newline="")
    writer = csv.writer(output)
    writer.writerow(("平仓日期", "API", "品种", "策略", "会话", "方向", "开仓时间", "平仓时间", "开仓价", "平仓价", "数量", "净盈亏", "手续费", "资金费", "状态", "来源", "平仓原因"))
    for trade in report.trades:
        writer.writerow(
            (
                trade.closed_at.strftime("%Y-%m-%d") if trade.closed_at else "",
                trade.api_name,
                trade.symbol,
                trade.strategy_name,
                trade.session_id,
                trade.direction,
                trade.opened_at.isoformat(timespec="seconds") if trade.opened_at else "",
                trade.closed_at.isoformat(timespec="seconds") if trade.closed_at else "",
                str(trade.entry_price or ""),
                str(trade.exit_price or ""),
                str(trade.size or ""),
                str(trade.net_pnl or ""),
                str(trade.fee or ""),
                str(trade.funding_fee or ""),
                trade.status,
                trade.source,
                trade.close_reason,
            )
        )
    return output.getvalue()


def report_to_html(report: DailyTradeReport) -> str:
    rows = []
    for trade in report.trades:
        cells = (
            trade.closed_at.strftime("%Y-%m-%d %H:%M") if trade.closed_at else "-",
            trade.api_name,
            trade.symbol,
            trade.strategy_name,
            trade.session_id,
            format_report_decimal(trade.net_pnl, signed=True),
            trade.status,
        )
        rows.append("<tr>" + "".join(f"<td>{html.escape(str(cell))}</td>" for cell in cells) + "</tr>")
    summary = "；".join(
        f"{item.report_date} {item.api_name}：平仓{item.closed_count}笔，净盈亏{format_report_decimal(item.net_pnl, signed=True)}"
        for item in report.daily
    ) or "该日期范围没有交易记录。"
    return (
        "<!doctype html><meta charset='utf-8'><title>API每日交易报表</title>"
        "<style>body{font-family:Microsoft YaHei,Segoe UI,sans-serif;margin:24px}"
        "table{border-collapse:collapse;width:100%}th,td{border:1px solid #d9dee5;padding:6px 8px;text-align:left}"
        "th{background:#f3f5f7}</style>"
        f"<h1>API每日交易报表</h1><p>{html.escape(summary)}</p>"
        "<table><thead><tr><th>平仓时间</th><th>API</th><th>品种</th><th>策略</th><th>会话</th><th>净盈亏</th><th>状态</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody></table>"
    )
