from __future__ import annotations

import argparse
import json
import shutil
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
import re
from typing import Iterable

from okx_quant.persistence import (
    load_strategy_history_snapshot,
    load_strategy_trade_ledger_snapshot,
    save_strategy_history_snapshot,
    save_strategy_trade_ledger_snapshot,
    state_dir_path,
    strategy_history_file_path,
    strategy_trade_ledger_file_path,
)


DEFAULT_TARGET_STRATEGY_IDS = ("ema55_slope_short", "btc_ema55_slope_short")
DEFAULT_FUTURES_TAKER_FEE_RATE = Decimal("0.00036")

_SESSION_LOG_LINE_RE = re.compile(
    r"^\[(?P<month>\d{2})-(?P<day>\d{2}) (?P<clock>\d{2}:\d{2}:\d{2})\] "
    r"\[(?P<api>[^\]]+)\] "
    r"\[(?P<session>S\d+) (?P<strategy>.+?) (?P<symbol>[A-Z0-9-]+)\] "
    r"(?P<message>.*)$"
)
_ORDER_ID_RE = re.compile(r"\bordId=(?P<value>[A-Za-z0-9]+)")
_CLIENT_ORDER_ID_RE = re.compile(r"\bclOrdId=(?P<value>[A-Za-z0-9]+)")
_PRICE_RE = re.compile(r"成交均价=(?P<value>-?\d+(?:\.\d+)?)")
_SIZE_RE = re.compile(r"成交数量=(?P<value>-?\d+(?:\.\d+)?)张")
_SIGNAL_BAR_RE = re.compile(r"(?P<value>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) \|")


@dataclass(frozen=True)
class SessionLogEvent:
    at: datetime
    message: str
    symbol: str


@dataclass
class ParsedTradeRound:
    session_id: str
    symbol: str
    opened_at: datetime
    closed_at: datetime
    entry_order_id: str = ""
    entry_client_order_id: str = ""
    exit_order_id: str = ""
    signal_bar_at: datetime | None = None
    entry_price_log: Decimal | None = None
    entry_size_log: Decimal | None = None
    exit_price_log: Decimal | None = None
    exit_size_log: Decimal | None = None
    close_reason: str = ""
    reason_confidence: str = "medium"
    summary_note: str = ""


@dataclass
class _OpenTradeState:
    session_id: str
    symbol: str
    opened_at: datetime
    signal_bar_at: datetime | None = None
    entry_order_id: str = ""
    entry_client_order_id: str = ""
    entry_price_log: Decimal | None = None
    entry_size_log: Decimal | None = None


@dataclass(frozen=True)
class BackfillResult:
    scanned_history_count: int
    candidate_round_count: int
    added_record_count: int
    updated_history_count: int
    ledger_path: Path
    history_path: Path
    backup_paths: tuple[Path, ...]


def _parse_decimal(raw: object) -> Decimal | None:
    if raw in {None, ""}:
        return None
    try:
        return Decimal(str(raw))
    except (InvalidOperation, ValueError):
        return None


def _format_decimal(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value, "f")


def _parse_iso_datetime(raw: object) -> datetime | None:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        return None


def _line_datetime_from_match(match: re.Match[str], *, default_year: int) -> datetime:
    month = int(match.group("month"))
    day = int(match.group("day"))
    clock = match.group("clock")
    return datetime.strptime(f"{default_year:04d}-{month:02d}-{day:02d} {clock}", "%Y-%m-%d %H:%M:%S")


def _extract_regex_decimal(pattern: re.Pattern[str], text: str) -> Decimal | None:
    matched = pattern.search(text)
    if matched is None:
        return None
    return _parse_decimal(matched.group("value"))


def _extract_regex_text(pattern: re.Pattern[str], text: str) -> str:
    matched = pattern.search(text)
    if matched is None:
        return ""
    return str(matched.group("value") or "").strip()


def _extract_signal_bar_at(message: str) -> datetime | None:
    matched = _SIGNAL_BAR_RE.search(message)
    if matched is None:
        return None
    try:
        return datetime.strptime(matched.group("value"), "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def _is_entry_fill_message(message: str) -> bool:
    return "本地下单成交" in message


def _derive_close_reason(message: str, pending_reason: str) -> tuple[str, str]:
    if "斜率转正平仓已成交" in message:
        return "斜率转正平仓", "high"
    if "止损" in message and "已成交" in message:
        return "本地止损触发", "high"
    if "止盈" in message and "已成交" in message:
        return "本地止盈触发", "high"
    if "平仓已成交" in message:
        return pending_reason or "本地主动平仓", "medium"
    return pending_reason or "本地平仓成交", "low"


def _is_close_fill_message(message: str) -> bool:
    if "已成交" not in message:
        return False
    return "平仓" in message or "止损" in message or "止盈" in message


def _close_reason_hint(message: str) -> str:
    if "斜率转正平仓" in message:
        return "斜率转正平仓"
    if "止损" in message:
        return "本地止损触发"
    if "止盈" in message:
        return "本地止盈触发"
    if "平仓" in message:
        return "本地主动平仓"
    return ""


def _parse_session_log_events(path: Path, *, session_id: str, api_name: str) -> list[SessionLogEvent]:
    if not path.exists():
        return []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except UnicodeDecodeError:
        lines = path.read_text(encoding="utf-8-sig").splitlines()
    default_year = _parse_year_from_log_path(path) or datetime.now().year
    events: list[SessionLogEvent] = []
    for line in lines:
        matched = _SESSION_LOG_LINE_RE.match(line.strip())
        if matched is None:
            continue
        if matched.group("session") != session_id or matched.group("api") != api_name:
            continue
        events.append(
            SessionLogEvent(
                at=_line_datetime_from_match(matched, default_year=default_year),
                message=matched.group("message").strip(),
                symbol=matched.group("symbol").strip().upper(),
            )
        )
    return events


def _parse_year_from_log_path(path: Path) -> int | None:
    for part in path.parts:
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", part):
            return int(part[:4])
    matched = re.search(r"20\d{2}", str(path))
    if matched is None:
        return None
    return int(matched.group(0))


def _candidate_session_log_paths(
    *,
    state_dir: Path,
    session_id: str,
    api_name: str,
    explicit_log_file: str,
) -> list[Path]:
    paths: dict[str, Path] = {}
    explicit = Path(explicit_log_file) if explicit_log_file else None
    if explicit is not None:
        paths[str(explicit)] = explicit
    logs_root = state_dir.parent / "logs" / "strategy_sessions"
    if logs_root.exists():
        pattern = f"**/*__{api_name}__{session_id}__*.log"
        for item in logs_root.glob(pattern):
            paths[str(item)] = item
    return sorted(paths.values(), key=lambda item: str(item))


def parse_trade_rounds_for_history_record(history_record: dict[str, object], *, state_dir: Path) -> list[ParsedTradeRound]:
    session_id = str(history_record.get("session_id") or "").strip()
    api_name = str(history_record.get("api_name") or "").strip()
    if not session_id or not api_name:
        return []
    events: list[SessionLogEvent] = []
    for path in _candidate_session_log_paths(
        state_dir=state_dir,
        session_id=session_id,
        api_name=api_name,
        explicit_log_file=str(history_record.get("log_file_path") or ""),
    ):
        events.extend(_parse_session_log_events(path, session_id=session_id, api_name=api_name))
    if not events:
        return []
    events.sort(key=lambda item: item.at)

    rounds: list[ParsedTradeRound] = []
    current_trade: _OpenTradeState | None = None
    pending_signal_bar_at: datetime | None = None
    pending_close_reason = ""
    for event in events:
        signal_bar_at = _extract_signal_bar_at(event.message)
        if signal_bar_at is not None:
            pending_signal_bar_at = signal_bar_at
        close_reason_hint = _close_reason_hint(event.message)
        if close_reason_hint:
            pending_close_reason = close_reason_hint
        if _is_entry_fill_message(event.message):
            current_trade = _OpenTradeState(
                session_id=session_id,
                symbol=event.symbol,
                opened_at=event.at,
                signal_bar_at=pending_signal_bar_at,
                entry_order_id=_extract_regex_text(_ORDER_ID_RE, event.message),
                entry_price_log=_extract_regex_decimal(_PRICE_RE, event.message),
                entry_size_log=_extract_regex_decimal(_SIZE_RE, event.message),
            )
            continue
        if current_trade is not None and "委托追踪" in event.message:
            order_id = _extract_regex_text(_ORDER_ID_RE, event.message)
            if not current_trade.entry_order_id or order_id == current_trade.entry_order_id:
                client_order_id = _extract_regex_text(_CLIENT_ORDER_ID_RE, event.message)
                if client_order_id:
                    current_trade.entry_client_order_id = client_order_id
            continue
        if current_trade is not None and _is_close_fill_message(event.message):
            close_reason, confidence = _derive_close_reason(event.message, pending_close_reason)
            rounds.append(
                ParsedTradeRound(
                    session_id=session_id,
                    symbol=event.symbol or current_trade.symbol,
                    opened_at=current_trade.opened_at,
                    closed_at=event.at,
                    entry_order_id=current_trade.entry_order_id,
                    entry_client_order_id=current_trade.entry_client_order_id,
                    exit_order_id=_extract_regex_text(_ORDER_ID_RE, event.message),
                    signal_bar_at=current_trade.signal_bar_at,
                    entry_price_log=current_trade.entry_price_log,
                    entry_size_log=current_trade.entry_size_log,
                    exit_price_log=_extract_regex_decimal(_PRICE_RE, event.message),
                    exit_size_log=_extract_regex_decimal(_SIZE_RE, event.message),
                    close_reason=close_reason,
                    reason_confidence=confidence,
                    summary_note="回补自会话日志",
                )
            )
            current_trade = None
            pending_close_reason = ""
    return rounds


def _history_side_sign(history_record: dict[str, object]) -> int | None:
    strategy_id = str(history_record.get("strategy_id") or "").strip()
    direction_label = str(history_record.get("direction_label") or "").strip()
    signal_mode = str((history_record.get("config_snapshot") or {}).get("signal_mode") or "").strip().lower()
    if strategy_id in {"ema55_slope_short", "btc_ema55_slope_short"}:
        return -1
    if "做空" in direction_label or signal_mode == "short_only":
        return -1
    if "做多" in direction_label or signal_mode == "long_only":
        return 1
    return None


def _extract_asset_key(inst_id: str) -> str:
    return str(inst_id or "").split("-")[0].strip().upper()


def _extract_quote_key(inst_id: str) -> str:
    parts = str(inst_id or "").split("-")
    if len(parts) < 2:
        return ""
    return parts[1].strip().upper()


def _fallback_contract_value(inst_id: str) -> tuple[Decimal | None, str | None]:
    asset = _extract_asset_key(inst_id)
    quote = _extract_quote_key(inst_id)
    if quote in {"USDT", "USDC"}:
        linear_contract_values = {
            "BTC": Decimal("0.01"),
            "ETH": Decimal("0.1"),
            "BNB": Decimal("0.01"),
            "OKB": Decimal("0.01"),
            "SOL": Decimal("1"),
            "DOGE": Decimal("1000"),
            "XRP": Decimal("100"),
        }
        value = linear_contract_values.get(asset)
        if value is not None:
            return value, asset
    if quote == "USD" and asset == "BTC":
        return Decimal("100"), "USD"
    return None, None


def _display_amount_from_contracts(inst_id: str, size: Decimal, reference_price: Decimal | None) -> tuple[Decimal | None, str | None]:
    contract_value, currency = _fallback_contract_value(inst_id)
    if contract_value is None or currency is None:
        return None, None
    quote = _extract_quote_key(inst_id)
    base = _extract_asset_key(inst_id)
    if currency in {"USD", "USDT", "USDC"} and quote in {"USD", "USDT", "USDC"} and reference_price is not None and reference_price > 0:
        return abs(size) * contract_value / reference_price, base or None
    return abs(size) * contract_value, currency


def _estimate_fee(price: Decimal | None, amount: Decimal | None, fee_rate: Decimal) -> Decimal | None:
    if price is None or amount is None or amount <= 0 or fee_rate <= 0:
        return None
    return -(abs(price * amount) * fee_rate)


def _weighted_average_fill_price(fills: list[dict[str, object]]) -> Decimal | None:
    total_size = Decimal("0")
    total_value = Decimal("0")
    for item in fills:
        fill_price = _parse_decimal(item.get("fill_price"))
        fill_size = _parse_decimal(item.get("fill_size"))
        if fill_price is None or fill_size is None or fill_size <= 0:
            continue
        total_size += fill_size
        total_value += fill_price * fill_size
    if total_size <= 0:
        return None
    return total_value / total_size


def _sum_decimal_field(items: Iterable[dict[str, object]], field: str) -> Decimal | None:
    total = Decimal("0")
    seen = False
    for item in items:
        value = _parse_decimal(item.get(field))
        if value is None:
            continue
        total += value
        seen = True
    return total if seen else None


def _load_history_records_map(path: Path) -> dict[tuple[str, str], list[dict[str, object]]]:
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    raw_records = payload.get("records")
    if not isinstance(raw_records, list):
        return {}
    grouped: dict[tuple[str, str], list[dict[str, object]]] = {}
    for item in raw_records:
        if not isinstance(item, dict):
            continue
        api_name = str(item.get("api_name") or "").strip()
        order_id = str(item.get("order_id") or "").strip()
        if not api_name or not order_id:
            continue
        grouped.setdefault((api_name, order_id), []).append(item)
    return grouped


def _load_order_records_map(path: Path) -> dict[tuple[str, str], dict[str, object]]:
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    raw_records = payload.get("records")
    if not isinstance(raw_records, list):
        return {}
    grouped: dict[tuple[str, str], dict[str, object]] = {}
    for item in raw_records:
        if not isinstance(item, dict):
            continue
        api_name = str(item.get("api_name") or "").strip()
        order_id = str(item.get("order_id") or "").strip()
        if not api_name or not order_id:
            continue
        grouped[(api_name, order_id)] = item
    return grouped


@dataclass
class _ApiEnvironmentHistoryCache:
    fills_by_api_order_id: dict[tuple[str, str], list[dict[str, object]]]
    orders_by_api_order_id: dict[tuple[str, str], dict[str, object]]


def _load_api_environment_history_cache(state_dir: Path, *, api_name: str, environment: str) -> _ApiEnvironmentHistoryCache:
    history_root = state_dir / "history" / api_name / environment
    return _ApiEnvironmentHistoryCache(
        fills_by_api_order_id=_load_history_records_map(history_root / "fills_history.json"),
        orders_by_api_order_id=_load_order_records_map(history_root / "order_history.json"),
    )


def _resolve_trade_fill_snapshot(
    cache: _ApiEnvironmentHistoryCache,
    *,
    api_name: str,
    order_id: str,
    log_price: Decimal | None,
    log_size: Decimal | None,
    fallback_at: datetime,
) -> tuple[Decimal | None, Decimal | None, Decimal | None, datetime]:
    if not order_id:
        return log_price, log_size, None, fallback_at
    fills = cache.fills_by_api_order_id.get((api_name, order_id), [])
    order = cache.orders_by_api_order_id.get((api_name, order_id))
    price = _weighted_average_fill_price(fills)
    size = _sum_decimal_field(fills, "fill_size")
    fee = _sum_decimal_field(fills, "fill_fee")
    if price is None and order is not None:
        price = _parse_decimal(order.get("avg_price")) or _parse_decimal(order.get("price"))
    if size is None and order is not None:
        size = (
            _parse_decimal(order.get("filled_size"))
            or _parse_decimal(order.get("actual_size"))
            or _parse_decimal(order.get("size"))
        )
    at = fallback_at
    if fills:
        fill_times = [int(item.get("fill_time") or 0) for item in fills if int(item.get("fill_time") or 0) > 0]
        if fill_times:
            at = datetime.fromtimestamp(max(fill_times) / 1000)
    elif order is not None:
        order_ms = int(order.get("updated_time") or order.get("created_time") or 0)
        if order_ms > 0:
            at = datetime.fromtimestamp(order_ms / 1000)
    return price or log_price, size or log_size, fee, at


def _business_key(record: dict[str, object]) -> tuple[str, ...]:
    round_id = str(record.get("round_id") or "").strip()
    if round_id:
        return ("round", str(record.get("session_id") or "").strip(), round_id)
    return (
        "legacy",
        str(record.get("session_id") or "").strip(),
        str(record.get("symbol") or "").strip().upper(),
        str(record.get("opened_at") or "").strip(),
        str(record.get("closed_at") or "").strip(),
        str(record.get("entry_order_id") or "").strip(),
        str(record.get("exit_order_id") or "").strip(),
        str(record.get("protective_algo_id") or "").strip(),
        str(record.get("size") or "").strip(),
        str(record.get("entry_price") or "").strip(),
        str(record.get("exit_price") or "").strip(),
    )


def _build_record_id(existing_ids: set[str], *, session_id: str, closed_at: datetime) -> str:
    base = f"{closed_at.strftime('%Y%m%d%H%M%S%f')}-{session_id}"
    candidate = base
    suffix = 2
    while candidate in existing_ids:
        candidate = f"{base}-{suffix}"
        suffix += 1
    existing_ids.add(candidate)
    return candidate


def _build_ledger_record(
    history_record: dict[str, object],
    round_info: ParsedTradeRound,
    *,
    cache: _ApiEnvironmentHistoryCache,
    existing_ids: set[str],
) -> dict[str, object] | None:
    api_name = str(history_record.get("api_name") or "").strip()
    environment = str((history_record.get("config_snapshot") or {}).get("environment") or "").strip() or "live"
    entry_price, entry_size, entry_fee, opened_at = _resolve_trade_fill_snapshot(
        cache,
        api_name=api_name,
        order_id=round_info.entry_order_id,
        log_price=round_info.entry_price_log,
        log_size=round_info.entry_size_log,
        fallback_at=round_info.opened_at,
    )
    exit_price, exit_size, exit_fee, closed_at = _resolve_trade_fill_snapshot(
        cache,
        api_name=api_name,
        order_id=round_info.exit_order_id,
        log_price=round_info.exit_price_log,
        log_size=round_info.exit_size_log,
        fallback_at=round_info.closed_at,
    )
    size = entry_size or exit_size or round_info.entry_size_log or round_info.exit_size_log
    if entry_price is None or exit_price is None or size is None or size <= 0:
        return None

    direction_sign = _history_side_sign(history_record)
    display_amount, display_currency = _display_amount_from_contracts(round_info.symbol, size, entry_price)
    if direction_sign is None or display_amount is None or display_amount <= 0:
        return None
    if display_currency != _extract_asset_key(round_info.symbol):
        return None

    gross_pnl = (exit_price - entry_price) * display_amount * Decimal(direction_sign)
    entry_fee_exact = entry_fee is not None
    exit_fee_exact = exit_fee is not None
    if entry_fee is None:
        entry_fee = _estimate_fee(entry_price, display_amount, DEFAULT_FUTURES_TAKER_FEE_RATE)
    if exit_fee is None:
        exit_fee = _estimate_fee(exit_price, display_amount, DEFAULT_FUTURES_TAKER_FEE_RATE)
    funding_fee = Decimal("0")
    net_pnl = gross_pnl + (entry_fee or Decimal("0")) + (exit_fee or Decimal("0")) + funding_fee

    fee_mode = "成交历史" if entry_fee_exact and exit_fee_exact else "估算"
    summary_note = f"{round_info.summary_note}；手续费来源={fee_mode}；资金费按 0 回补"
    return {
        "record_id": _build_record_id(existing_ids, session_id=round_info.session_id, closed_at=closed_at),
        "history_record_id": str(history_record.get("record_id") or "").strip(),
        "session_id": round_info.session_id,
        "round_id": "",
        "api_name": api_name,
        "strategy_id": str(history_record.get("strategy_id") or "").strip(),
        "strategy_name": str(history_record.get("strategy_name") or "").strip(),
        "symbol": round_info.symbol,
        "direction_label": str(history_record.get("direction_label") or "").strip(),
        "run_mode_label": str(history_record.get("run_mode_label") or "").strip(),
        "environment": environment,
        "signal_bar_at": round_info.signal_bar_at.isoformat(timespec="seconds") if round_info.signal_bar_at is not None else None,
        "opened_at": opened_at.isoformat(timespec="seconds"),
        "closed_at": closed_at.isoformat(timespec="seconds"),
        "entry_order_id": round_info.entry_order_id,
        "entry_client_order_id": round_info.entry_client_order_id,
        "exit_order_id": round_info.exit_order_id,
        "protective_algo_id": "",
        "protective_algo_cl_ord_id": "",
        "entry_price": _format_decimal(entry_price),
        "exit_price": _format_decimal(exit_price),
        "size": _format_decimal(size),
        "entry_fee": _format_decimal(entry_fee),
        "exit_fee": _format_decimal(exit_fee),
        "funding_fee": _format_decimal(funding_fee),
        "gross_pnl": _format_decimal(gross_pnl),
        "net_pnl": _format_decimal(net_pnl),
        "close_reason": round_info.close_reason,
        "reason_confidence": round_info.reason_confidence,
        "summary_note": summary_note,
        "updated_at": datetime.now().isoformat(timespec="seconds"),
    }


def _rebuild_history_financials(history_records: list[dict[str, object]], ledger_records: list[dict[str, object]]) -> int:
    grouped: dict[str, list[dict[str, object]]] = {}
    for item in ledger_records:
        history_record_id = str(item.get("history_record_id") or "").strip()
        if history_record_id:
            grouped.setdefault(history_record_id, []).append(item)

    changed = 0
    for record in history_records:
        matched = grouped.get(str(record.get("record_id") or "").strip(), [])
        matched.sort(key=lambda item: (str(item.get("closed_at") or ""), str(item.get("record_id") or "")), reverse=True)
        next_trade_count = len(matched)
        next_win_count = sum(1 for item in matched if (_parse_decimal(item.get("net_pnl")) or Decimal("0")) > 0)
        next_gross = sum(((_parse_decimal(item.get("gross_pnl")) or Decimal("0")) for item in matched), Decimal("0"))
        next_fee = sum(
            (
                (_parse_decimal(item.get("entry_fee")) or Decimal("0"))
                + (_parse_decimal(item.get("exit_fee")) or Decimal("0"))
                for item in matched
            ),
            Decimal("0"),
        )
        next_funding = sum(((_parse_decimal(item.get("funding_fee")) or Decimal("0")) for item in matched), Decimal("0"))
        next_net = sum(((_parse_decimal(item.get("net_pnl")) or Decimal("0")) for item in matched), Decimal("0"))
        next_last_reason = str(matched[0].get("close_reason") or "").strip() if matched else ""
        previous = (
            int(record.get("trade_count", 0) or 0),
            int(record.get("win_count", 0) or 0),
            str(record.get("gross_pnl_total") or "0"),
            str(record.get("fee_total") or "0"),
            str(record.get("funding_total") or "0"),
            str(record.get("net_pnl_total") or "0"),
            str(record.get("last_close_reason") or ""),
        )
        current = (
            next_trade_count,
            next_win_count,
            format(next_gross, "f"),
            format(next_fee, "f"),
            format(next_funding, "f"),
            format(next_net, "f"),
            next_last_reason,
        )
        if current == previous:
            continue
        record["trade_count"] = next_trade_count
        record["win_count"] = next_win_count
        record["gross_pnl_total"] = format(next_gross, "f")
        record["fee_total"] = format(next_fee, "f")
        record["funding_total"] = format(next_funding, "f")
        record["net_pnl_total"] = format(next_net, "f")
        record["last_close_reason"] = next_last_reason
        record["updated_at"] = datetime.now().isoformat(timespec="seconds")
        changed += 1
    return changed


def _backup_file(path: Path) -> Path:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = path.with_name(f"{path.name}.{stamp}.bak")
    shutil.copy2(path, backup_path)
    return backup_path


def backfill_strategy_trade_ledger(
    *,
    state_dir: Path | None = None,
    strategy_ids: tuple[str, ...] = DEFAULT_TARGET_STRATEGY_IDS,
    write: bool = False,
) -> BackfillResult:
    resolved_state_dir = state_dir or state_dir_path()
    ledger_path = strategy_trade_ledger_file_path(resolved_state_dir)
    history_path = strategy_history_file_path(resolved_state_dir)
    ledger_records = list(load_strategy_trade_ledger_snapshot(ledger_path).get("records", []))
    history_records = list(load_strategy_history_snapshot(history_path).get("records", []))

    normalized_targets = {str(item).strip() for item in strategy_ids if str(item).strip()}
    existing_ids = {str(item.get("record_id") or "").strip() for item in ledger_records}
    existing_keys = {_business_key(item) for item in ledger_records}

    caches: dict[tuple[str, str], _ApiEnvironmentHistoryCache] = {}
    scanned_history_count = 0
    candidate_round_count = 0
    added_record_count = 0
    for history_record in history_records:
        strategy_id = str(history_record.get("strategy_id") or "").strip()
        if strategy_id not in normalized_targets:
            continue
        scanned_history_count += 1
        rounds = parse_trade_rounds_for_history_record(history_record, state_dir=resolved_state_dir)
        if not rounds:
            continue
        environment = str((history_record.get("config_snapshot") or {}).get("environment") or "").strip() or "live"
        cache_key = (str(history_record.get("api_name") or "").strip(), environment)
        cache = caches.get(cache_key)
        if cache is None:
            cache = _load_api_environment_history_cache(
                resolved_state_dir,
                api_name=cache_key[0],
                environment=cache_key[1],
            )
            caches[cache_key] = cache
        for round_info in rounds:
            candidate_round_count += 1
            record = _build_ledger_record(history_record, round_info, cache=cache, existing_ids=existing_ids)
            if record is None:
                continue
            business_key = _business_key(record)
            if business_key in existing_keys:
                continue
            ledger_records.append(record)
            existing_keys.add(business_key)
            added_record_count += 1

    updated_history_count = _rebuild_history_financials(history_records, ledger_records)
    backup_paths: list[Path] = []
    if write and (added_record_count > 0 or updated_history_count > 0):
        if ledger_path.exists():
            backup_paths.append(_backup_file(ledger_path))
        if history_path.exists():
            backup_paths.append(_backup_file(history_path))
        save_strategy_trade_ledger_snapshot(ledger_records, ledger_path)
        save_strategy_history_snapshot(history_records, history_path)

    return BackfillResult(
        scanned_history_count=scanned_history_count,
        candidate_round_count=candidate_round_count,
        added_record_count=added_record_count,
        updated_history_count=updated_history_count,
        ledger_path=ledger_path,
        history_path=history_path,
        backup_paths=tuple(backup_paths),
    )


def _build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="回补普通量化策略漏记的总账本记录。")
    parser.add_argument(
        "--state-dir",
        default="",
        help="状态目录，默认使用程序标准 state 目录。",
    )
    parser.add_argument(
        "--strategy-id",
        dest="strategy_ids",
        action="append",
        default=[],
        help="只回补指定策略 ID，可重复传入；默认回补均线斜率做空相关策略。",
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="实际写入文件；不传时仅做 dry-run 预览。",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_argument_parser()
    args = parser.parse_args(argv)
    strategy_ids = tuple(args.strategy_ids) if args.strategy_ids else DEFAULT_TARGET_STRATEGY_IDS
    result = backfill_strategy_trade_ledger(
        state_dir=Path(args.state_dir) if args.state_dir else None,
        strategy_ids=strategy_ids,
        write=bool(args.write),
    )
    mode = "WRITE" if args.write else "DRY-RUN"
    print(
        f"[{mode}] 扫描会话={result.scanned_history_count} | 候选成交={result.candidate_round_count} | "
        f"新增账本={result.added_record_count} | 更新历史={result.updated_history_count}"
    )
    print(f"总账本：{result.ledger_path}")
    print(f"会话历史：{result.history_path}")
    for backup_path in result.backup_paths:
        print(f"备份：{backup_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
