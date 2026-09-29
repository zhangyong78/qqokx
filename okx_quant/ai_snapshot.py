"""Read-only, ChatGPT-friendly account snapshot generation.

The module deliberately has no order or trading calls.  It collects derivative
positions, market candles and optional Deribit DVOL data, then writes one
immutable JSON file per API profile.
"""
from __future__ import annotations

import json
import re
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable, Iterable, Sequence
from uuid import uuid4

from okx_quant.app_paths import ai_snapshots_dir_path
from okx_quant.deribit_client import DeribitRestClient, DeribitVolatilityCandle
from okx_quant.models import Candle, Credentials
from okx_quant.okx_client import OkxFillHistoryItem, OkxPosition, OkxRestClient
from okx_quant.persistence import (
    ai_snapshot_watchlist_file_path,
    load_position_notes_snapshot,
)

SNAPSHOT_SCHEMA_VERSION = "1.0"
MARKET_BARS = ("1W", "1D", "4H", "1H")
MARKET_LIMIT = 1200
DVOL_HOURLY_LIMIT_DAYS = 370
DEFAULT_WATCHLIST = ("BTC",)
_SAFE_SYMBOL = re.compile(r"^[A-Z0-9]{1,20}$")
_SAFE_PROFILE = re.compile(r"[^A-Za-z0-9_.-]+")


class FreshnessCheckError(RuntimeError):
    """Raised when the two external market sources do not share a current hour."""


def normalize_watch_symbol(value: object) -> str:
    text = str(value or "").strip().upper().replace("/", "-")
    if text.endswith("-USDT-SWAP"):
        text = text[:-10]
    elif text.endswith("-USDT"):
        text = text[:-5]
    text = text.strip(" -")
    if not _SAFE_SYMBOL.fullmatch(text):
        return ""
    return text


def normalize_watchlist(values: Iterable[object]) -> list[str]:
    result: list[str] = []
    for value in values:
        symbol = normalize_watch_symbol(value)
        if symbol and symbol not in result:
            result.append(symbol)
    return sorted(result)


def _profile_slug(value: str) -> str:
    slug = _SAFE_PROFILE.sub("_", str(value or "").strip()).strip("._")
    return slug or "default"


def merge_asset_bases(held_instruments: Iterable[str], watchlist: Iterable[object]) -> list[str]:
    held: list[str] = []
    for inst_id in held_instruments:
        symbol = normalize_watch_symbol(str(inst_id).split("-", 1)[0])
        if symbol and symbol not in held:
            held.append(symbol)
    return sorted(set(held) | set(normalize_watchlist(watchlist)))


def load_ai_watchlist(profile_name: str, *, path: Path | None = None) -> list[str]:
    target = path or ai_snapshot_watchlist_file_path()
    if not target.exists():
        return []
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except Exception:
        return []
    profiles = payload.get("profiles", {}) if isinstance(payload, dict) else {}
    if not isinstance(profiles, dict):
        return []
    value = profiles.get(str(profile_name).strip(), [])
    return normalize_watchlist(value if isinstance(value, list) else [])


def save_ai_watchlist(profile_name: str, values: Iterable[object], *, path: Path | None = None) -> Path:
    target = path or ai_snapshot_watchlist_file_path()
    profiles: dict[str, list[str]] = {}
    if target.exists():
        try:
            payload = json.loads(target.read_text(encoding="utf-8"))
            raw_profiles = payload.get("profiles", {}) if isinstance(payload, dict) else {}
            if isinstance(raw_profiles, dict):
                profiles = {str(key): normalize_watchlist(value if isinstance(value, list) else []) for key, value in raw_profiles.items()}
        except Exception:
            profiles = {}
    key = str(profile_name or "").strip() or "default"
    profiles[key] = normalize_watchlist(values)
    target.parent.mkdir(parents=True, exist_ok=True)
    temp = target.with_suffix(target.suffix + ".tmp")
    temp.write_text(json.dumps({"version": 1, "profiles": profiles}, ensure_ascii=False, indent=2), encoding="utf-8")
    temp.replace(target)
    return target


def _decimal(value: object) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _number(value: object) -> str | None:
    value = _decimal(value)
    return format(value, "f") if value is not None else None


def _iso_ms(ts: int | None) -> str | None:
    if not ts:
        return None
    return datetime.fromtimestamp(int(ts) / 1000, tz=timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _safe_raw(value: object) -> object:
    if isinstance(value, dict):
        result: dict[str, object] = {}
        for key, item in value.items():
            lowered = str(key).lower()
            if any(token in lowered for token in ("apikey", "secret", "passphrase", "password")):
                continue
            result[str(key)] = _safe_raw(item)
        return result
    if isinstance(value, (list, tuple)):
        return [_safe_raw(item) for item in value]
    if isinstance(value, Decimal):
        return format(value, "f")
    return value


def _ema(values: Sequence[float], period: int) -> list[float]:
    multiplier = 2.0 / (period + 1)
    result: list[float] = []
    current: float | None = None
    for value in values:
        current = value if current is None else (value - current) * multiplier + current
        result.append(current)
    return result


def _sma(values: Sequence[float], period: int) -> list[float]:
    result: list[float] = []
    for index in range(1, len(values) + 1):
        window = values[max(0, index - period):index]
        result.append(sum(window) / len(window))
    return result


def _bar_step_ms(interval: str) -> int:
    normalized = str(interval or "").strip().upper()
    return {
        "1H": 60 * 60 * 1000,
        "4H": 4 * 60 * 60 * 1000,
        "1D": 24 * 60 * 60 * 1000,
        "1W": 7 * 24 * 60 * 60 * 1000,
    }.get(normalized, 60 * 60 * 1000)


def _bar_semantics(
    *,
    interval: str,
    bar_start_ms: int,
    snapshot_time_ms: int,
    is_closed: bool,
    fetch_age_seconds: int,
    fetch_age_field: str,
) -> dict[str, object]:
    step_ms = _bar_step_ms(interval)
    bar_end_ms = bar_start_ms + step_ms
    if is_closed:
        elapsed_ms = step_ms
        minutes_to_close = 0.0
        progress_pct = 100.0
        status = "CLOSED"
        reference_level = "CONFIRMED"
    else:
        elapsed_ms = max(0, min(step_ms, snapshot_time_ms - bar_start_ms))
        minutes_to_close = max(0.0, (bar_end_ms - snapshot_time_ms) / 60_000)
        progress_pct = max(0.0, min(100.0, elapsed_ms / step_ms * 100))
        if minutes_to_close <= 15:
            status, reference_level = "NEAR_CLOSE", "HIGH"
        elif minutes_to_close <= 30:
            status, reference_level = "FORMING", "MEDIUM"
        else:
            status, reference_level = "EARLY", "LOW"
    return {
        "interval": interval,
        "bar_start": _iso_ms(bar_start_ms),
        "bar_end": _iso_ms(bar_end_ms),
        "elapsed_minutes": round(elapsed_ms / 60_000, 1),
        "minutes_to_close": round(minutes_to_close, 1),
        "bar_progress_pct": round(progress_pct, 1),
        "bar_status": status,
        "reference_level": reference_level,
        "is_closed": bool(is_closed),
        fetch_age_field: max(0, int(fetch_age_seconds)),
        "indicator_is_provisional": not bool(is_closed),
    }


def _bar_views(rows: Sequence[dict[str, object]]) -> dict[str, object]:
    if not rows:
        return {"last_closed_bar": None, "current_bar": None}
    current = rows[-1] if not bool(rows[-1].get("is_closed")) else None
    last_closed = next((row for row in reversed(rows) if bool(row.get("is_closed"))), None)
    return {
        "last_closed_bar": dict(last_closed) if last_closed is not None else None,
        "current_bar": dict(current) if current is not None else None,
    }


def build_market_rows(
    candles: Sequence[Candle],
    *,
    limit: int = 250,
    interval: str = "1H",
    snapshot_time_ms: int | None = None,
    fetched_at_ms: int | None = None,
) -> list[dict[str, object]]:
    """Compute indicators before slicing, preserving the chart's warm-up semantics."""
    ordered = sorted(candles, key=lambda item: item.ts)
    if not ordered:
        return []
    closes = [float(item.close) for item in ordered]
    ema15 = _ema(closes, 15)
    ma50 = _sma(closes, 50)
    selected = ordered[-limit:]
    start = len(ordered) - len(selected)
    snapshot_time_ms = int(snapshot_time_ms or datetime.now(timezone.utc).timestamp() * 1000)
    fetched_at_ms = int(fetched_at_ms or snapshot_time_ms)
    fetch_age_seconds = max(0, (snapshot_time_ms - fetched_at_ms) // 1000)
    rows: list[dict[str, object]] = []
    for index, candle in enumerate(selected, start=start):
        row = {
                "timestamp_ms": int(candle.ts),
                "timestamp": _iso_ms(int(candle.ts)),
                "open": _number(candle.open),
                "high": _number(candle.high),
                "low": _number(candle.low),
                "close": _number(candle.close),
                "volume": _number(candle.volume),
                "ema15": format(ema15[index], ".12g"),
                "ma50": format(ma50[index], ".12g"),
            }
        row.update(
            _bar_semantics(
                interval=interval,
                bar_start_ms=int(candle.ts),
                snapshot_time_ms=snapshot_time_ms,
                is_closed=bool(candle.confirmed),
                fetch_age_seconds=fetch_age_seconds,
                fetch_age_field="market_fetch_age_seconds",
            )
        )
        rows.append(row)
    return rows


def _aggregate_dvol(candles: Sequence[DeribitVolatilityCandle], step_ms: int) -> list[DeribitVolatilityCandle]:
    buckets: dict[int, list[DeribitVolatilityCandle]] = {}
    for candle in candles:
        buckets.setdefault((int(candle.ts) // step_ms) * step_ms, []).append(candle)
    result: list[DeribitVolatilityCandle] = []
    for ts, items in sorted(buckets.items()):
        result.append(DeribitVolatilityCandle(ts, items[0].open, max(x.high for x in items), min(x.low for x in items), items[-1].close))
    return result


def build_dvol_rows(
    candles: Sequence[DeribitVolatilityCandle],
    *,
    step_ms: int,
    now_ms: int,
    limit: int = 250,
    interval: str | None = None,
    fetched_at_ms: int | None = None,
) -> list[dict[str, object]]:
    aggregated = _aggregate_dvol(sorted(candles, key=lambda item: item.ts), step_ms)
    closes = [float(item.close) for item in aggregated]
    ema15 = _ema(closes, 15)
    ma50 = _sma(closes, 50)
    rows: list[dict[str, object]] = []
    start = max(0, len(aggregated) - limit)
    interval = interval or {60 * 60 * 1000: "1H", 4 * 60 * 60 * 1000: "4H", 24 * 60 * 60 * 1000: "1D"}.get(step_ms, "1H")
    fetched_at_ms = int(fetched_at_ms or now_ms)
    fetch_age_seconds = max(0, (now_ms - fetched_at_ms) // 1000)
    for index, candle in enumerate(aggregated[-limit:], start=start):
        row = {
                "timestamp_ms": int(candle.ts),
                "timestamp": _iso_ms(candle.ts),
                "open": _number(candle.open),
                "high": _number(candle.high),
                "low": _number(candle.low),
                "close": _number(candle.close),
                "ema15": format(ema15[index], ".12g"),
                "ma50": format(ma50[index], ".12g"),
            }
        row.update(
            _bar_semantics(
                interval=interval,
                bar_start_ms=int(candle.ts),
                snapshot_time_ms=now_ms,
                is_closed=int(candle.ts) + step_ms <= now_ms,
                fetch_age_seconds=fetch_age_seconds,
                fetch_age_field="volatility_fetch_age_seconds",
            )
        )
        rows.append(row)
    return rows


def _position_note(position: OkxPosition, profile_name: str, environment: str, notes: dict[str, object]) -> str | None:
    for item in notes.get("current_notes", []) if isinstance(notes, dict) else []:
        if not isinstance(item, dict):
            continue
        if (
            str(item.get("profile_name", "")) == profile_name
            and str(item.get("environment", "")) == environment
            and str(item.get("inst_id", "")).upper() == position.inst_id.upper()
            and str(item.get("pos_side", "")).lower() == position.pos_side.lower()
            and str(item.get("mgn_mode", "")).lower() == position.mgn_mode.lower()
        ):
            return str(item.get("note", "")) or None
    return None


def _position_payload(position: OkxPosition, *, profile_name: str, environment: str, notes: dict[str, object]) -> dict[str, object]:
    raw = position.raw or {}
    base = normalize_watch_symbol(position.inst_id.split("-", 1)[0])
    return {
        "account_alias": profile_name,
        "instrument_id": position.inst_id,
        "instrument_type": position.inst_type.upper(),
        "underlying": base,
        "trade_origin": "manual",
        "side": position.pos_side or ("long" if position.position > 0 else "short"),
        "position_side": position.pos_side,
        "margin_mode": position.mgn_mode,
        "quantity": _number(position.position),
        "available_quantity": _number(position.avail_position),
        "entry_price": _number(position.avg_price),
        "mark_price": _number(position.mark_price),
        "index_price": _number(raw.get("idxPx")),
        "last_price": _number(position.last_price),
        "liquidation_price": _number(position.liquidation_price),
        "leverage": _number(position.leverage),
        "margin_currency": position.margin_ccy,
        "margin": _number(raw.get("margin") or raw.get("imr")),
        "initial_margin": _number(position.initial_margin),
        "maintenance_margin": _number(position.maintenance_margin),
        "unrealized_pnl": _number(position.unrealized_pnl),
        "unrealized_pnl_ratio": _number(position.unrealized_pnl_ratio),
        "pnl_percent": _number(position.unrealized_pnl_ratio),
        "realized_pnl": _number(position.realized_pnl),
        "funding_rate": _number(raw.get("fundingRate")),
        "open_time": _iso_ms(int(raw["cTime"])) if str(raw.get("cTime", "")).isdigit() else None,
        "contract_value": _number(raw.get("ctVal")),
        "position_value": _number(raw.get("notionalUsd") or raw.get("notionalCcy")),
        "delta": _number(position.delta),
        "gamma": _number(position.gamma),
        "vega": _number(position.vega),
        "theta": _number(position.theta),
        "note": _position_note(position, profile_name, environment, notes),
        "option": {
            "strike": _number(raw.get("stkPx")),
            "expiry": raw.get("expTime"),
            "option_type": raw.get("optType"),
            "implied_volatility": _number(raw.get("markVol") or raw.get("markIv") or raw.get("impliedVolatility")),
        } if position.inst_type.upper() == "OPTION" else None,
        "raw": _safe_raw(raw),
    }


def _fill_payload(item: OkxFillHistoryItem) -> dict[str, object]:
    raw = item.raw or {}
    return {
        "account_alias": "",
        "fill_time": _iso_ms(item.fill_time),
        "fill_time_ms": item.fill_time,
        "instrument_id": item.inst_id,
        "instrument_type": item.inst_type.upper(),
        "underlying": normalize_watch_symbol(item.inst_id.split("-", 1)[0]),
        "side": item.side,
        "action": "REDUCE" if str(raw.get("reduceOnly", "")).lower() == "true" else "TRADE",
        "position_side": item.pos_side,
        "price": _number(item.fill_price),
        "quantity": _number(item.fill_size),
        "fee": _number(item.fill_fee),
        "fee_currency": item.fee_currency,
        "pnl": _number(item.pnl),
        "realized_pnl": _number(item.pnl),
        "position_before": None,
        "position_after": None,
        "note": None,
        "order_id": item.order_id,
        "trade_id": item.trade_id,
        "client_order_id": raw.get("clOrdId"),
        "reduce_only": raw.get("reduceOnly"),
        "raw": _safe_raw(raw),
    }


class AISnapshotBuilder:
    def __init__(
        self,
        *,
        client: OkxRestClient | Any | None = None,
        volatility_client: DeribitRestClient | Any | None = None,
        now: datetime | None = None,
        progress_callback: Callable[[str], None] | None = None,
    ) -> None:
        self.client = client or OkxRestClient()
        self.volatility_client = volatility_client or DeribitRestClient()
        self.now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        self.progress_callback = progress_callback or (lambda _message: None)

    def _progress(self, message: str) -> None:
        self.progress_callback(message)

    def _freshness_preflight(
        self,
        assets: Sequence[str],
        *,
        now_ms: int,
        require_dvol: bool = True,
    ) -> dict[str, object]:
        """Refresh the latest 1H candle from each source before reading history."""
        last_error = ""
        for attempt in range(1, 4):
            self._progress(f"补充最新行情（第 {attempt}/3 次）")
            checked: dict[str, object] = {}
            try:
                for base in assets:
                    inst_id = f"{base}-USDT-SWAP"
                    okx_rows = self.client.get_candles(inst_id, "1H", limit=3)
                    okx_latest = max((int(item.ts) for item in okx_rows), default=0)
                    if not okx_latest:
                        raise FreshnessCheckError(f"{base}：OKX 没有返回最新 1H 行情")
                    dvol_latest: int | None = None
                    dvol_error = ""
                    if base in {"BTC", "ETH"}:
                        try:
                            dvol_rows = self.volatility_client.get_volatility_index_candles(
                                base,
                                "3600",
                                start_ts=now_ms - 3 * 60 * 60 * 1000,
                                end_ts=now_ms,
                                max_records=None,
                            )
                            dvol_latest = max((int(item.ts) for item in dvol_rows), default=0) or None
                            if dvol_latest is None:
                                dvol_error = "波动率网站没有返回最新 1H 数据"
                        except Exception as exc:
                            dvol_error = str(exc)[:240] or exc.__class__.__name__
                        if dvol_error and require_dvol:
                            raise FreshnessCheckError(f"{base}：{dvol_error}")
                    okx_bar_age = max(0, now_ms - okx_latest)
                    dvol_bar_age = max(0, now_ms - dvol_latest) if dvol_latest else None
                    if okx_bar_age > 60 * 60 * 1000:
                        raise FreshnessCheckError(f"{base}：OKX 当前K线已运行 {okx_bar_age // 60_000} 分钟，超过允许新鲜度")
                    if dvol_bar_age is not None and dvol_bar_age > 60 * 60 * 1000:
                        raise FreshnessCheckError(f"{base}：波动率当前K线已运行 {dvol_bar_age // 60_000} 分钟，超过允许新鲜度")
                    if dvol_latest is not None and abs(okx_latest - dvol_latest) > 60 * 60 * 1000:
                        raise FreshnessCheckError(f"{base}：OKX 与波动率最新小时不一致")
                    checked[base] = {
                        "okx_latest": _iso_ms(okx_latest),
                        "volatility_latest": _iso_ms(dvol_latest),
                        "okx_bar_elapsed_minutes": round(okx_bar_age / 60_000, 1),
                        "volatility_bar_elapsed_minutes": round(dvol_bar_age / 60_000, 1) if dvol_bar_age is not None else None,
                        "okx_fetch_age_seconds": 0,
                        "volatility_fetch_age_seconds": 0 if dvol_latest is not None else None,
                        "volatility_status": "MISSING" if dvol_error else ("OK" if dvol_latest is not None else "NOT_CONFIGURED"),
                        "volatility_error": dvol_error or None,
                        "aligned_within_60m": dvol_latest is None or abs(okx_latest - dvol_latest) <= 60 * 60 * 1000,
                    }
                self._progress("最新行情已补齐，时间一致，开始读取历史数据")
                return {"passed": True, "attempts": attempt, "checked_at": _iso_ms(now_ms), "assets": checked}
            except Exception as exc:
                last_error = str(exc) or exc.__class__.__name__
                if attempt < 3:
                    self._progress(f"数据尚未一致，等待后重试：{last_error}")
                    time.sleep(1.0)
        raise FreshnessCheckError(f"新鲜度检查未通过（已重试 3 次）：{last_error}")

    def build(
        self,
        *,
        credentials: Credentials,
        profile_name: str,
        environment: str,
        watchlist: Iterable[object] = (),
        output_root: Path | None = None,
    ) -> dict[str, object]:
        profile_name = str(profile_name or credentials.profile_name or "default").strip()
        environment = str(environment or "live").strip().lower()
        watch_symbols = normalize_watchlist(watchlist)
        self._progress("读取衍生品持仓")
        positions: list[OkxPosition] = []
        for inst_type in ("SWAP", "FUTURES", "OPTION"):
            positions.extend(self.client.get_positions(credentials, environment=environment, inst_type=inst_type, prefer_cache=False))
        positions = [item for item in positions if item.inst_type.upper() in {"SWAP", "FUTURES", "OPTION"} and item.position != 0]
        assets = merge_asset_bases((item.inst_id for item in positions), watch_symbols)
        if not assets:
            assets = list(DEFAULT_WATCHLIST)
            watch_symbols = list(DEFAULT_WATCHLIST)
        now_ms = int(self.now.timestamp() * 1000)
        freshness_preflight = self._freshness_preflight(assets, now_ms=now_ms)
        notes = load_position_notes_snapshot()
        warnings: list[str] = []
        account_overview: dict[str, object] = {}
        market_fetch_marks: dict[tuple[str, str], float] = {}
        volatility_fetch_marks: dict[str, float] = {}
        try:
            overview = self.client.get_account_overview(credentials, environment=environment, prefer_cache=False)
            account_overview = {
                "total_equity": _number(overview.total_equity),
                "adjusted_equity": _number(overview.adjusted_equity),
                "isolated_equity": _number(overview.isolated_equity),
                "available_equity": _number(overview.available_equity),
                "unrealized_pnl": _number(overview.unrealized_pnl),
                "initial_margin": _number(overview.initial_margin),
                "maintenance_margin": _number(overview.maintenance_margin),
                "order_frozen": _number(overview.order_frozen),
                "notional_usd": _number(overview.notional_usd),
                "details": [_safe_raw(item.raw) for item in overview.details],
            }
        except Exception as exc:
            warnings.append(f"账户权益不可用：{str(exc)[:240]}")
            account_overview = {"available": False, "reason": str(exc)[:240]}
        asset_payload: dict[str, object] = {}
        latest_market: list[int] = []
        for base in assets:
            self._progress(f"读取 {base} 行情")
            inst_id = f"{base}-USDT-SWAP"
            market: dict[str, object] = {}
            bar_counts: dict[str, int] = {}
            for bar in MARKET_BARS:
                candles = self.client.get_candles_history(inst_id, bar, limit=MARKET_LIMIT)
                market_fetch_marks[(base, bar)] = time.monotonic()
                market_fetched_at_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
                rows = build_market_rows(
                    candles,
                    limit=250,
                    interval=bar,
                    snapshot_time_ms=now_ms,
                    fetched_at_ms=market_fetched_at_ms,
                )
                if not rows:
                    raise ValueError(f"{base} {bar} 没有可用的核心行情数据")
                bar_counts[bar] = len(rows)
                if len(rows) < 250:
                    warnings.append(f"{base} {bar} 只有 {len(rows)} 根，少于要求的 250 根")
                market[bar] = {
                    "instrument_id": inst_id,
                    "candles": rows,
                    "data_as_of": rows[-1]["timestamp"] if rows else None,
                    "last_closed_bar": _bar_views(rows)["last_closed_bar"],
                    "current_bar": _bar_views(rows)["current_bar"],
                }
                if rows:
                    latest_market.append(int(rows[-1]["timestamp_ms"]))
            volatility: dict[str, object] = {"enabled": False, "reason": "not_available"}
            if base in {"BTC", "ETH"}:
                try:
                    start_ms = now_ms - DVOL_HOURLY_LIMIT_DAYS * 24 * 60 * 60 * 1000
                    hourly = self.volatility_client.get_volatility_index_candles(
                        base, "3600", start_ts=start_ms, end_ts=now_ms, max_records=None
                    )
                    volatility_fetch_marks[base] = time.monotonic()
                    volatility = {
                        "enabled": bool(hourly),
                        "index": f"{base}-DVOL",
                        "hourly": build_dvol_rows(hourly, step_ms=60 * 60 * 1000, now_ms=now_ms, interval="1H", fetched_at_ms=int(datetime.now(timezone.utc).timestamp() * 1000)),
                        "4H": build_dvol_rows(hourly, step_ms=4 * 60 * 60 * 1000, now_ms=now_ms, interval="4H", fetched_at_ms=int(datetime.now(timezone.utc).timestamp() * 1000)),
                        "1D": build_dvol_rows(hourly, step_ms=24 * 60 * 60 * 1000, now_ms=now_ms, interval="1D", fetched_at_ms=int(datetime.now(timezone.utc).timestamp() * 1000)),
                        "data_as_of": _iso_ms(hourly[-1].ts) if hourly else None,
                    }
                    volatility["bar_views"] = {
                        "1H": _bar_views(volatility["hourly"]),
                        "4H": _bar_views(volatility["4H"]),
                        "1D": _bar_views(volatility["1D"]),
                    }
                except Exception as exc:
                    reason = str(exc)[:240]
                    warnings.append(f"{base} DVOL 不可用：{reason}")
                    volatility = {"enabled": False, "reason": reason}
            market_latest = max(
                (int(value["candles"][-1]["timestamp_ms"]) for value in market.values() if value.get("candles")),
                default=0,
            )
            volatility_rows = volatility.get("hourly", []) if isinstance(volatility, dict) else []
            volatility_latest = int(volatility_rows[-1]["timestamp_ms"]) if volatility_rows else None
            if volatility_latest and abs(market_latest - volatility_latest) > 60 * 60 * 1000:
                warnings.append(f"{base} 行情与 DVOL 有效小时不一致")
            market_bar_elapsed_minutes = max(0, now_ms - market_latest) // 60_000 if market_latest else None
            volatility_bar_elapsed_minutes = max(0, now_ms - volatility_latest) // 60_000 if volatility_latest else None
            if market_bar_elapsed_minutes is not None and market_bar_elapsed_minutes > 60:
                warnings.append(f"{base} 当前行情K线已运行 {market_bar_elapsed_minutes} 分钟")
            if volatility_bar_elapsed_minutes is not None and volatility_bar_elapsed_minutes > 60:
                warnings.append(f"{base} 当前DVOL K线已运行 {volatility_bar_elapsed_minutes} 分钟")
            asset_payload[base] = {
                "market": market,
                "volatility": volatility,
                "held": any(item.inst_id.upper().startswith(f"{base}-") for item in positions),
                "freshness": {
                    "market_latest": _iso_ms(market_latest),
                    "volatility_latest": _iso_ms(volatility_latest),
                    "market_bar_elapsed_minutes": market_bar_elapsed_minutes,
                    "volatility_bar_elapsed_minutes": volatility_bar_elapsed_minutes,
                    "market_fetch_age_seconds": 0,
                    "volatility_fetch_age_seconds": 0 if volatility_latest else None,
                    "aligned_within_60m": not volatility_latest or abs(market_latest - volatility_latest) <= 60 * 60 * 1000,
                    "bar_counts": bar_counts,
                },
            }
        snapshot_fetch_mark = time.monotonic()
        for base, asset in asset_payload.items():
            freshness = asset.get("freshness", {}) if isinstance(asset, dict) else {}
            if isinstance(freshness, dict):
                market_ages = [
                    snapshot_fetch_mark - mark
                    for (marked_base, _period), mark in market_fetch_marks.items()
                    if marked_base == base
                ]
                freshness["market_fetch_age_seconds"] = int(max(market_ages, default=0.0))
                if base in volatility_fetch_marks:
                    freshness["volatility_fetch_age_seconds"] = int(
                        max(0.0, snapshot_fetch_mark - volatility_fetch_marks[base])
                    )
            market_block = asset.get("market", {}) if isinstance(asset, dict) else {}
            if isinstance(market_block, dict):
                for period, block in market_block.items():
                    if not isinstance(block, dict) or not isinstance(block.get("candles"), list):
                        continue
                    age = int(max(0.0, snapshot_fetch_mark - market_fetch_marks.get((base, period), snapshot_fetch_mark)))
                    for row in block["candles"]:
                        if isinstance(row, dict):
                            row["market_fetch_age_seconds"] = age
                    views = _bar_views(block["candles"])
                    block["last_closed_bar"] = views["last_closed_bar"]
                    block["current_bar"] = views["current_bar"]
            volatility = asset.get("volatility", {}) if isinstance(asset, dict) else {}
            if isinstance(volatility, dict):
                age = int(max(0.0, snapshot_fetch_mark - volatility_fetch_marks.get(base, snapshot_fetch_mark)))
                for period in ("hourly", "4H", "1D"):
                    rows = volatility.get(period)
                    if not isinstance(rows, list):
                        continue
                    for row in rows:
                        if isinstance(row, dict):
                            row["volatility_fetch_age_seconds"] = age
                if volatility.get("enabled"):
                    volatility["bar_views"] = {
                        "1H": _bar_views(volatility.get("hourly", [])),
                        "4H": _bar_views(volatility.get("4H", [])),
                        "1D": _bar_views(volatility.get("1D", [])),
                    }
        self._progress("整理持仓、交易和质量信息")
        position_rows = [_position_payload(item, profile_name=profile_name, environment=environment, notes=notes) for item in positions]
        try:
            fills = self.client.get_fills_history(credentials, environment=environment, inst_types=("SWAP", "FUTURES", "OPTION"), limit=100)
        except Exception:
            fills = []
        cutoff_ms = now_ms - 7 * 24 * 60 * 60 * 1000
        fill_rows = []
        for item in fills:
            if item.fill_time and item.fill_time < cutoff_ms:
                continue
            row = _fill_payload(item)
            row["account_alias"] = profile_name
            fill_rows.append(row)
        summary_fields = ("unrealized_pnl", "realized_pnl", "delta", "gamma", "vega", "theta", "initial_margin", "maintenance_margin")
        summary: dict[str, object] = {field: _number(sum((_decimal(row.get(field)) or Decimal("0") for row in position_rows), Decimal("0"))) for field in summary_fields}
        summary["position_count"] = len(position_rows)
        summary["asset_count"] = len(assets)
        summary["watchlist_only_assets"] = sorted(base for base in assets if not any(row["underlying"] == base for row in position_rows))
        effective_ms = max(latest_market) if latest_market else now_ms
        snapshot_time = self.now.isoformat(timespec="seconds").replace("+00:00", "Z")
        payload: dict[str, object] = {
            "schema_version": SNAPSHOT_SCHEMA_VERSION,
            "snapshot_id": str(uuid4()),
            "snapshot_time": snapshot_time,
            "effective_as_of": _iso_ms(effective_ms),
            "effective_as_of_hour": datetime.fromtimestamp(effective_ms / 1000, tz=timezone.utc).replace(minute=0, second=0, microsecond=0).isoformat().replace("+00:00", "Z"),
            "timezone": "Asia/Shanghai",
            "bar_trust_policy": {
                "near_close_minutes": 15,
                "forming_minutes": 30,
                "levels": {"CLOSED": "CONFIRMED", "NEAR_CLOSE": "HIGH", "FORMING": "MEDIUM", "EARLY": "LOW"},
            },
            "account": {
                "account_alias": profile_name,
                "profile_name": profile_name,
                "environment": environment,
                "spot_included": False,
                "programmatic_trading": False if profile_name == "159" else None,
                "balance": account_overview,
            },
            "watchlist": watch_symbols,
            "freshness_preflight": freshness_preflight,
            "assets": asset_payload,
            "manual_positions": position_rows,
            "portfolio_summary": summary,
            "recent_manual_trades": fill_rows,
            "data_quality": {
                "status": "PASS" if not warnings else "DEGRADED",
                "warnings": warnings,
                "market_asset_count": len(asset_payload),
                "market_latest": _iso_ms(effective_ms),
                "dvol_alignment_required": any(bool((item.get("volatility") or {}).get("enabled")) for item in asset_payload.values() if isinstance(item, dict)),
            },
        }
        output_dir = Path(output_root) if output_root is not None else ai_snapshots_dir_path()
        destination = output_dir / _profile_slug(profile_name) / self.now.strftime("%Y") / self.now.strftime("%m") / self.now.strftime("%d")
        destination.mkdir(parents=True, exist_ok=True)
        target = destination / f"qqokx_AI_Snapshot_{_profile_slug(profile_name)}_{self.now.strftime('%Y%m%d_%H%M%S')}.json"
        if target.exists():
            target = destination / f"qqokx_AI_Snapshot_{_profile_slug(profile_name)}_{self.now.strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:8]}.json"
        temp = target.with_suffix(target.suffix + ".tmp")
        temp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        temp.replace(target)
        payload["file_path"] = str(target)
        self._progress(f"快照已保存：{target}")
        return payload


QUICK_MARKET_BAR_LIMITS: dict[str, int] = {"1W": 40, "1D": 80, "4H": 100, "1H": 120}
QUICK_DVOL_BAR_LIMITS: dict[str, int] = {"1D": 60, "4H": 80, "1H": 100}
QUICK_MARKET_COLUMNS = ("ts", "o", "h", "l", "c", "v", "ema15", "ma50", "closed")
QUICK_DVOL_COLUMNS = ("ts", "o", "h", "l", "c", "ema15", "ma50", "closed")


def _quick_number(value: object) -> int | float | None:
    decimal = _decimal(value)
    if decimal is None:
        return None
    number = float(decimal)
    if number.is_integer() and abs(number) < 10**15:
        return int(number)
    return number


def _quick_relation(left: object, right: object) -> str | None:
    left_number = _quick_number(left)
    right_number = _quick_number(right)
    if left_number is None or right_number is None:
        return None
    if left_number > right_number:
        return "ABOVE"
    if left_number < right_number:
        return "BELOW"
    return "EQUAL"


def _quick_slope_direction(value: object) -> str | None:
    slope = _quick_number(value)
    if slope is None:
        return None
    if slope > 0:
        return "UP"
    if slope < 0:
        return "DOWN"
    return "FLAT"


def _quick_bar_reference(row: dict[str, object] | None, *, index: int | None) -> dict[str, object] | None:
    if row is None:
        return None
    return {
        "index": index,
        "ts": row.get("timestamp"),
        "close": _quick_number(row.get("close")),
        "ema15": _quick_number(row.get("ema15")),
        "ma50": _quick_number(row.get("ma50")),
        "closed": 1 if bool(row.get("is_closed")) else 0,
    }


def _parse_iso_ms(value: object) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return int(datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp() * 1000)
    except (TypeError, ValueError, OverflowError):
        return None


def _quick_state(rows: Sequence[dict[str, object]]) -> dict[str, object]:
    if not rows:
        return {}
    row = rows[-1]
    previous = rows[-2] if len(rows) > 1 else None
    price = _quick_number(row.get("close"))
    ema15 = _quick_number(row.get("ema15"))
    ma50 = _quick_number(row.get("ma50"))
    ema15_slope = None
    ma50_slope = None
    if previous is not None:
        previous_ema = _quick_number(previous.get("ema15"))
        previous_ma = _quick_number(previous.get("ma50"))
        if ema15 is not None and previous_ema is not None:
            ema15_slope = round(ema15 - previous_ema, 8)
        if ma50 is not None and previous_ma is not None:
            ma50_slope = round(ma50 - previous_ma, 8)

    def distance(value: float | int | None) -> float | None:
        if price is None or value in (None, 0):
            return None
        return round((price - float(value)) / float(value) * 100, 4)

    return {
        "price": price,
        "ema15": ema15,
        "ma50": ma50,
        "ema15_vs_ma50": _quick_relation(ema15, ma50),
        "price_vs_ema15": _quick_relation(price, ema15),
        "price_vs_ma50": _quick_relation(price, ma50),
        "ema15_slope": ema15_slope,
        "ma50_slope": ma50_slope,
        "ema15_slope_direction": _quick_slope_direction(ema15_slope),
        "ma50_slope_direction": _quick_slope_direction(ma50_slope),
        "distance_to_ema15_pct": distance(ema15),
        "distance_to_ma50_pct": distance(ma50),
    }


def _quick_bar_state(rows: Sequence[dict[str, object]], *, snapshot_time: str) -> dict[str, object]:
    if not rows:
        return {
            "snapshot_time": snapshot_time,
            "current_bar_start": None,
            "current_bar_end": None,
            "is_closed": None,
            "elapsed_minutes": None,
            "minutes_to_close": None,
            "bar_progress_pct": None,
            "bar_status": "MISSING",
            "reference_level": "NONE",
            "indicator_is_provisional": None,
            "latest_closed_bar_time": None,
            "last_closed_bar_index": None,
            "current_bar_index": None,
        }
    current = rows[-1] if not bool(rows[-1].get("is_closed")) else None
    last_closed = next((row for row in reversed(rows) if bool(row.get("is_closed"))), None)
    active = current or last_closed or rows[-1]
    return {
        "snapshot_time": snapshot_time,
        "current_bar_start": current.get("bar_start") if current else None,
        "current_bar_end": current.get("bar_end") if current else None,
        "is_closed": bool(active.get("is_closed")),
        "elapsed_minutes": active.get("elapsed_minutes"),
        "minutes_to_close": active.get("minutes_to_close"),
        "bar_progress_pct": active.get("bar_progress_pct"),
        "bar_status": active.get("bar_status"),
        "reference_level": active.get("reference_level"),
        "indicator_is_provisional": bool(active.get("indicator_is_provisional")),
        "latest_closed_bar_time": last_closed.get("timestamp") if last_closed else None,
        "last_closed_bar_index": -2 if current is not None and last_closed is not None else (-1 if last_closed is not None else None),
        "current_bar_index": -1 if current is not None else None,
        "market_fetch_age_seconds": active.get("market_fetch_age_seconds"),
        "volatility_fetch_age_seconds": active.get("volatility_fetch_age_seconds"),
    }


def _quick_market_period(rows: Sequence[dict[str, object]], *, limit: int, snapshot_time: str, interval: str) -> dict[str, object]:
    selected = list(rows[-limit:])
    bars = [
        [
            row.get("timestamp"),
            _quick_number(row.get("open")),
            _quick_number(row.get("high")),
            _quick_number(row.get("low")),
            _quick_number(row.get("close")),
            _quick_number(row.get("volume")),
            _quick_number(row.get("ema15")),
            _quick_number(row.get("ma50")),
            1 if bool(row.get("is_closed")) else 0,
        ]
        for row in selected
    ]
    current = selected[-1] if selected and not bool(selected[-1].get("is_closed")) else None
    last_closed = next((row for row in reversed(selected) if bool(row.get("is_closed"))), None)
    return {
        "interval": interval,
        "columns": list(QUICK_MARKET_COLUMNS),
        "bars": bars,
        "state": _quick_state(selected),
        "bar_state": _quick_bar_state(selected, snapshot_time=snapshot_time),
        "last_closed_bar": _quick_bar_reference(last_closed, index=-2 if current is not None and last_closed else (-1 if last_closed else None)),
        "current_bar": _quick_bar_reference(current, index=-1 if current else None),
    }


def _quick_dvol_period(rows: Sequence[dict[str, object]], *, limit: int, snapshot_time: str, interval: str) -> dict[str, object]:
    selected = list(rows[-limit:])
    bars = [
        [
            row.get("timestamp"),
            _quick_number(row.get("open")),
            _quick_number(row.get("high")),
            _quick_number(row.get("low")),
            _quick_number(row.get("close")),
            _quick_number(row.get("ema15")),
            _quick_number(row.get("ma50")),
            1 if bool(row.get("is_closed")) else 0,
        ]
        for row in selected
    ]
    current = selected[-1] if selected and not bool(selected[-1].get("is_closed")) else None
    last_closed = next((row for row in reversed(selected) if bool(row.get("is_closed"))), None)
    state = _quick_state(selected)
    state.pop("distance_to_ema15_pct", None)
    state.pop("distance_to_ma50_pct", None)
    state["value"] = state.pop("price", None)
    state["value_vs_ema15"] = state.pop("price_vs_ema15", None)
    return {
        "interval": interval,
        "columns": list(QUICK_DVOL_COLUMNS),
        "bars": bars,
        "state": state,
        "bar_state": _quick_bar_state(selected, snapshot_time=snapshot_time),
        "last_closed_bar": _quick_bar_reference(last_closed, index=-2 if current is not None and last_closed else (-1 if last_closed else None)),
        "current_bar": _quick_bar_reference(current, index=-1 if current else None),
    }


def _quick_option_contract_fields(inst_id: str) -> dict[str, object]:
    parts = [part for part in str(inst_id or "").split("-") if part]
    result: dict[str, object] = {"option_type": None, "expiry": None, "strike": None}
    if len(parts) >= 5:
        suffix = parts[-1].upper()
        if suffix in {"C", "P"}:
            result["option_type"] = "call" if suffix == "C" else "put"
        result["expiry"] = parts[-3]
        result["strike"] = _quick_number(parts[-2])
    return result


def _quick_option_chain_rows(
    tickers: Iterable[object],
    *,
    expiry: str | None = None,
    strike_min: object = None,
    strike_max: object = None,
) -> list[dict[str, object]]:
    normalized_expiry = str(expiry or "").strip()
    minimum = _quick_number(strike_min)
    maximum = _quick_number(strike_max)
    rows: list[dict[str, object]] = []
    for ticker in tickers:
        inst_id = str(getattr(ticker, "inst_id", "") or "")
        fields = _quick_option_contract_fields(inst_id)
        ticker_expiry = str(fields.get("expiry") or "")
        strike = _quick_number(fields.get("strike"))
        if normalized_expiry and ticker_expiry != normalized_expiry:
            continue
        if minimum is not None and (strike is None or strike < minimum):
            continue
        if maximum is not None and (strike is None or strike > maximum):
            continue
        raw = getattr(ticker, "raw", {}) or {}
        rows.append(
            {
                "instrument_id": inst_id,
                "option_type": fields.get("option_type"),
                "expiry": ticker_expiry or None,
                "strike": strike,
                "bid": _quick_number(getattr(ticker, "bid", None) or raw.get("bidPx")),
                "ask": _quick_number(getattr(ticker, "ask", None) or raw.get("askPx")),
                "mark_price": _quick_number(getattr(ticker, "mark", None) or raw.get("markPx")),
                "iv": _quick_number(raw.get("markVol") or raw.get("markIv") or raw.get("impliedVolatility")),
                "delta": _quick_number(raw.get("delta") or raw.get("deltaPA")),
                "gamma": _quick_number(raw.get("gamma") or raw.get("gammaPA")),
                "vega": _quick_number(raw.get("vega") or raw.get("vegaPA")),
                "theta": _quick_number(raw.get("theta") or raw.get("thetaPA")),
            }
        )
    rows.sort(key=lambda item: (str(item.get("expiry") or ""), float(item.get("strike") or 0), str(item.get("instrument_id") or "")))
    return rows


def _refresh_quick_bar_state(
    block: dict[str, object],
    *,
    snapshot_time_ms: int,
    snapshot_time: str,
    fetch_age_field: str,
    fetch_age_seconds: int,
) -> None:
    bars = block.get("bars")
    if not isinstance(bars, list) or not bars:
        return
    last = bars[-1]
    if not isinstance(last, list) or len(last) < 2:
        return
    bar_start_ms = _parse_iso_ms(last[0])
    if bar_start_ms is None:
        return
    is_closed = bool(last[-1])
    interval = str(block.get("interval") or "1H")
    semantics = _bar_semantics(
        interval=interval,
        bar_start_ms=bar_start_ms,
        snapshot_time_ms=snapshot_time_ms,
        is_closed=is_closed,
        fetch_age_seconds=fetch_age_seconds,
        fetch_age_field=fetch_age_field,
    )
    last_closed_time = None
    last_closed_index = None
    if is_closed:
        last_closed_time = str(last[0])
        last_closed_index = -1
    elif len(bars) >= 2:
        previous = bars[-2]
        last_closed_time = str(previous[0]) if isinstance(previous, list) else None
        last_closed_index = -2
    bar_state = {
        "snapshot_time": snapshot_time,
        "current_bar_start": semantics["bar_start"] if not is_closed else None,
        "current_bar_end": semantics["bar_end"] if not is_closed else None,
        "is_closed": is_closed,
        "elapsed_minutes": semantics["elapsed_minutes"],
        "minutes_to_close": semantics["minutes_to_close"],
        "bar_progress_pct": semantics["bar_progress_pct"],
        "bar_status": semantics["bar_status"],
        "reference_level": semantics["reference_level"],
        "indicator_is_provisional": semantics["indicator_is_provisional"],
        "latest_closed_bar_time": last_closed_time,
        "last_closed_bar_index": last_closed_index,
        "current_bar_index": None if is_closed else -1,
        fetch_age_field: fetch_age_seconds,
    }
    block["bar_state"] = bar_state


def _quick_position_payload(
    position: OkxPosition,
    *,
    profile_name: str,
    environment: str,
    notes: dict[str, object],
    option_quote: dict[str, object] | None = None,
) -> dict[str, object]:
    full = _position_payload(position, profile_name=profile_name, environment=environment, notes=notes)
    raw = position.raw or {}
    position_side = str(position.pos_side or "").strip().lower()
    side = position_side if position_side in {"long", "short"} else ("long" if position.position > 0 else "short")
    if position.inst_type.upper() == "OPTION":
        option = full.get("option") if isinstance(full.get("option"), dict) else {}
        contract = _quick_option_contract_fields(position.inst_id)
        quote = option_quote if isinstance(option_quote, dict) else {}
        return {
            "instrument_id": position.inst_id,
            "instrument_type": "OPTION",
            "underlying": full.get("underlying"),
            "option_type": option.get("option_type") or raw.get("optType") or contract["option_type"],
            "side": side,
            "expiry": option.get("expiry") or raw.get("expTime") or contract["expiry"],
            "strike": option.get("strike") or _quick_number(raw.get("stkPx")) or contract["strike"],
            "size": _quick_number(position.position),
            "entry_price": _quick_number(position.avg_price),
            "mark_price": _quick_number(position.mark_price),
            "bid": _quick_number(raw.get("bidPx") or raw.get("bid") or quote.get("bid")),
            "ask": _quick_number(raw.get("askPx") or raw.get("ask") or quote.get("ask")),
            "iv": option.get("implied_volatility") or _quick_number(raw.get("markVol") or raw.get("markIv") or raw.get("impliedVolatility")) or quote.get("iv"),
            "delta": _quick_number(position.delta) if position.delta is not None else _quick_number(quote.get("delta")),
            "gamma": _quick_number(position.gamma) if position.gamma is not None else _quick_number(quote.get("gamma")),
            "vega": _quick_number(position.vega) if position.vega is not None else _quick_number(quote.get("vega")),
            "theta": _quick_number(position.theta) if position.theta is not None else _quick_number(quote.get("theta")),
            "upl": _quick_number(position.unrealized_pnl),
            "settlement": raw.get("settleCcy") or position.margin_ccy,
            "margin_ccy": position.margin_ccy,
            "note": full.get("note"),
        }
    return {
        "instrument_id": position.inst_id,
        "instrument_type": position.inst_type.upper(),
        "underlying": full.get("underlying"),
        "side": side,
        "size": _quick_number(position.position),
        "entry_price": _quick_number(position.avg_price),
        "mark_price": _quick_number(position.mark_price),
        "index_price": _quick_number(raw.get("idxPx")),
        "upl": _quick_number(position.unrealized_pnl),
        "realized_pnl": _quick_number(position.realized_pnl),
        "leverage": _quick_number(position.leverage),
        "margin_mode": position.mgn_mode,
        "margin_ccy": position.margin_ccy,
        "note": full.get("note"),
    }


class AIQuickSnapshotBuilder(AISnapshotBuilder):
    def build(
        self,
        *,
        credentials: Credentials,
        profile_name: str,
        environment: str,
        symbols: Iterable[object] = ("BTC",),
        include_option_chain: bool = False,
        option_chain_expiry: str | None = None,
        option_chain_strike_min: object = None,
        option_chain_strike_max: object = None,
        output_root: Path | None = None,
    ) -> dict[str, object]:
        selected_symbols = normalize_watchlist(symbols) or ["BTC"]
        profile_name = str(profile_name or credentials.profile_name or "default").strip()
        environment = str(environment or "live").strip().lower()
        self._progress("读取分析标的的人工衍生品持仓")
        positions: list[OkxPosition] = []
        for inst_type in ("SWAP", "FUTURES", "OPTION"):
            positions.extend(self.client.get_positions(credentials, environment=environment, inst_type=inst_type, prefer_cache=False))
        selected_positions = [
            item for item in positions
            if item.inst_type.upper() in {"SWAP", "FUTURES", "OPTION"}
            and item.position != 0
            and normalize_watch_symbol(item.inst_id.split("-", 1)[0]) in selected_symbols
        ]
        option_quotes: dict[str, dict[str, object]] = {}
        get_ticker = getattr(self.client, "get_ticker", None)
        if callable(get_ticker):
            for item in selected_positions:
                if item.inst_type.upper() != "OPTION":
                    continue
                try:
                    ticker = get_ticker(item.inst_id)
                    raw_ticker = getattr(ticker, "raw", {}) or {}
                    option_quotes[item.inst_id.upper()] = {
                        "bid": getattr(ticker, "bid", None) or raw_ticker.get("bidPx"),
                        "ask": getattr(ticker, "ask", None) or raw_ticker.get("askPx"),
                        "iv": raw_ticker.get("markVol") or raw_ticker.get("markIv") or raw_ticker.get("impliedVolatility"),
                        "delta": raw_ticker.get("delta") or raw_ticker.get("deltaPA"),
                        "gamma": raw_ticker.get("gamma") or raw_ticker.get("gammaPA"),
                        "vega": raw_ticker.get("vega") or raw_ticker.get("vegaPA"),
                        "theta": raw_ticker.get("theta") or raw_ticker.get("thetaPA"),
                    }
                except Exception:
                    continue
        now_ms = int(self.now.timestamp() * 1000)
        freshness_preflight = self._freshness_preflight(selected_symbols, now_ms=now_ms, require_dvol=False)
        notes = load_position_notes_snapshot()
        warnings: list[str] = []
        market_fetch_marks: dict[tuple[str, str], float] = {}
        volatility_fetch_marks: dict[str, float] = {}
        market_fetch_times: dict[tuple[str, str], str] = {}
        volatility_fetch_times: dict[str, str] = {}
        asset_payload: dict[str, object] = {}
        market_ok = True
        dvol_status = "NOT_CONFIGURED"
        alignment_ok = True
        for base in selected_symbols:
            self._progress(f"生成 {base} 精简行情")
            inst_id = f"{base}-USDT-SWAP"
            market: dict[str, object] = {}
            for bar, limit in QUICK_MARKET_BAR_LIMITS.items():
                candles = self.client.get_candles_history(inst_id, bar, limit=MARKET_LIMIT)
                market_fetch_marks[(base, bar)] = time.monotonic()
                market_fetch_times[(base, bar)] = _iso_ms(int(datetime.now(timezone.utc).timestamp() * 1000)) or ""
                rows = build_market_rows(candles, limit=MARKET_LIMIT, interval=bar, snapshot_time_ms=now_ms)
                if not rows:
                    market_ok = False
                    warnings.append(f"{base} {bar} market data missing")
                if len(rows) < limit:
                    warnings.append(f"{base} {bar} quick output has only {len(rows)} bars; requested {limit}")
                market[bar] = _quick_market_period(rows, limit=limit, snapshot_time=_iso_ms(now_ms) or "", interval=bar)
            volatility: dict[str, object] = {"index": f"{base}-DVOL", "status": "NOT_CONFIGURED"}
            if base in {"BTC", "ETH"}:
                try:
                    hourly = self.volatility_client.get_volatility_index_candles(
                        base,
                        "3600",
                        start_ts=now_ms - DVOL_HOURLY_LIMIT_DAYS * 24 * 60 * 60 * 1000,
                        end_ts=now_ms,
                        max_records=None,
                    )
                    volatility_fetch_marks[base] = time.monotonic()
                    volatility_fetch_times[base] = _iso_ms(int(datetime.now(timezone.utc).timestamp() * 1000)) or ""
                    if not hourly:
                        raise FreshnessCheckError(f"{base} DVOL unavailable")
                    volatility = {
                        "index": f"{base}-DVOL",
                        "status": "OK",
                        "1D": _quick_dvol_period(build_dvol_rows(hourly, step_ms=24 * 60 * 60 * 1000, now_ms=now_ms, interval="1D"), limit=QUICK_DVOL_BAR_LIMITS["1D"], snapshot_time=_iso_ms(now_ms) or "", interval="1D"),
                        "4H": _quick_dvol_period(build_dvol_rows(hourly, step_ms=4 * 60 * 60 * 1000, now_ms=now_ms, interval="4H"), limit=QUICK_DVOL_BAR_LIMITS["4H"], snapshot_time=_iso_ms(now_ms) or "", interval="4H"),
                        "1H": _quick_dvol_period(build_dvol_rows(hourly, step_ms=60 * 60 * 1000, now_ms=now_ms, interval="1H"), limit=QUICK_DVOL_BAR_LIMITS["1H"], snapshot_time=_iso_ms(now_ms) or "", interval="1H"),
                    }
                    details = freshness_preflight.get("assets", {}).get(base, {})
                    if isinstance(details, dict):
                        alignment_ok = alignment_ok and bool(details.get("aligned_within_60m", True))
                except Exception as exc:
                    volatility = {"index": f"{base}-DVOL", "status": "MISSING", "reason": str(exc)[:240]}
                    dvol_status = "MISSING"
                    warnings.append(f"{base} DVOL unavailable")
            if volatility.get("status") == "OK":
                dvol_status = "OK"
            related_positions = [
                _quick_position_payload(
                    item,
                    profile_name=profile_name,
                    environment=environment,
                    notes=notes,
                    option_quote=option_quotes.get(item.inst_id.upper()),
                )
                for item in selected_positions
                if normalize_watch_symbol(item.inst_id.split("-", 1)[0]) == base
            ]
            derivatives = [item for item in related_positions if item.get("instrument_type") != "OPTION"]
            options = [item for item in related_positions if item.get("instrument_type") == "OPTION"]
            option_chain: dict[str, object] = {"included": False}
            if include_option_chain:
                get_tickers = getattr(self.client, "get_tickers", None)
                if callable(get_tickers):
                    try:
                        chain_rows = _quick_option_chain_rows(
                            get_tickers("OPTION", inst_family=base),
                            expiry=option_chain_expiry,
                            strike_min=option_chain_strike_min,
                            strike_max=option_chain_strike_max,
                        )
                        option_chain = {
                            "included": True,
                            "expiry": option_chain_expiry,
                            "strike_min": _quick_number(option_chain_strike_min),
                            "strike_max": _quick_number(option_chain_strike_max),
                            "rows": chain_rows,
                        }
                    except Exception as exc:
                        option_chain = {"included": True, "error": str(exc)[:240], "rows": []}
                        warnings.append(f"{base} option chain unavailable")
                else:
                    option_chain = {"included": True, "error": "option ticker API unavailable", "rows": []}
                    warnings.append(f"{base} option chain unavailable")
            net_fields = {
                field: _number(
                    sum(
                        (getattr(item, field) or Decimal("0") for item in selected_positions if normalize_watch_symbol(item.inst_id.split("-", 1)[0]) == base),
                        Decimal("0"),
                    )
                )
                for field in ("delta", "gamma", "vega", "theta")
            }
            one_hour_state = market.get("1H", {}).get("bar_state", {}) if isinstance(market.get("1H"), dict) else {}
            asset_payload[base] = {
                "market": market,
                "volatility": volatility,
                "positions": {"derivatives": derivatives, "options": options},
                "position_summary": {
                    "net_delta": net_fields["delta"],
                    "net_gamma": net_fields["gamma"],
                    "net_vega": net_fields["vega"],
                    "net_theta": net_fields["theta"],
                },
                "option_chain": option_chain,
                "freshness": {
                    "snapshot_time": _iso_ms(now_ms),
                    "market_fetch_time": market_fetch_times.get((base, "1H")),
                    "market_fetch_age_seconds": 0,
                    "current_bar_elapsed_minutes": one_hour_state.get("elapsed_minutes") if isinstance(one_hour_state, dict) else None,
                    "minutes_to_close": one_hour_state.get("minutes_to_close") if isinstance(one_hour_state, dict) else None,
                    "latest_closed_bar_time": one_hour_state.get("latest_closed_bar_time") if isinstance(one_hour_state, dict) else None,
                    "status": "FRESH",
                },
            }
        snapshot_fetch_mark = time.monotonic()
        final_snapshot_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        final_snapshot_time = _iso_ms(final_snapshot_ms) or self.now.isoformat(timespec="milliseconds").replace("+00:00", "Z")
        for base, asset in asset_payload.items():
            market = asset.get("market", {}) if isinstance(asset, dict) else {}
            freshness = asset.get("freshness", {}) if isinstance(asset, dict) else {}
            if isinstance(freshness, dict):
                market_age = int(max(0.0, snapshot_fetch_mark - market_fetch_marks.get((base, "1H"), snapshot_fetch_mark)))
                freshness["market_fetch_age_seconds"] = market_age
                freshness["market_fetch_time"] = market_fetch_times.get((base, "1H"))
                freshness["snapshot_time"] = final_snapshot_time
                one_hour_state = market.get("1H", {}).get("bar_state", {}) if isinstance(market.get("1H"), dict) else {}
                if isinstance(one_hour_state, dict):
                    freshness["current_bar_elapsed_minutes"] = one_hour_state.get("elapsed_minutes")
                    freshness["minutes_to_close"] = one_hour_state.get("minutes_to_close")
                    freshness["latest_closed_bar_time"] = one_hour_state.get("latest_closed_bar_time")
                freshness["status"] = "FRESH" if market_ok else "MISSING"
            if isinstance(market, dict):
                for period, block in market.items():
                    if not isinstance(block, dict) or not isinstance(block.get("bars"), list):
                        continue
                    age = int(max(0.0, snapshot_fetch_mark - market_fetch_marks.get((base, period), snapshot_fetch_mark)))
                    _refresh_quick_bar_state(
                        block,
                        snapshot_time_ms=final_snapshot_ms,
                        snapshot_time=final_snapshot_time,
                        fetch_age_field="market_fetch_age_seconds",
                        fetch_age_seconds=age,
                    )
                    if period == "1H" and isinstance(freshness, dict):
                        current_state = block.get("bar_state", {})
                        if isinstance(current_state, dict):
                            freshness["current_bar_elapsed_minutes"] = current_state.get("elapsed_minutes")
                            freshness["minutes_to_close"] = current_state.get("minutes_to_close")
                            freshness["latest_closed_bar_time"] = current_state.get("latest_closed_bar_time")
            volatility = asset.get("volatility", {}) if isinstance(asset, dict) else {}
            if isinstance(volatility, dict):
                age = int(max(0.0, snapshot_fetch_mark - volatility_fetch_marks.get(base, snapshot_fetch_mark)))
                if isinstance(freshness, dict) and base in volatility_fetch_marks:
                    freshness["volatility_fetch_time"] = volatility_fetch_times.get(base)
                    freshness["volatility_fetch_age_seconds"] = age
                for period in ("1D", "4H", "1H"):
                    block = volatility.get(period)
                    if isinstance(block, dict):
                        _refresh_quick_bar_state(
                            block,
                            snapshot_time_ms=final_snapshot_ms,
                            snapshot_time=final_snapshot_time,
                            fetch_age_field="volatility_fetch_age_seconds",
                            fetch_age_seconds=age,
                        )
        try:
            fills = self.client.get_fills_history(credentials, environment=environment, inst_types=("SWAP", "FUTURES", "OPTION"), limit=100)
        except Exception:
            fills = []
        cutoff_ms = now_ms - 7 * 24 * 60 * 60 * 1000
        related_trades: list[dict[str, object]] = []
        for item in fills:
            if item.fill_time and item.fill_time < cutoff_ms:
                continue
            if normalize_watch_symbol(item.inst_id.split("-", 1)[0]) not in selected_symbols:
                continue
            row = {key: value for key, value in _fill_payload(item).items() if key not in {"raw", "position_before", "position_after"}}
            row["account_alias"] = profile_name
            related_trades.append(row)
            if len(related_trades) >= 20:
                break
        if dvol_status == "NOT_CONFIGURED":
            dvol_quality = "NOT_CONFIGURED"
        else:
            dvol_quality = dvol_status
        snapshot_time = final_snapshot_time
        payload: dict[str, object] = {
            "schema": "qqokx_ai_quick_snapshot",
            "version": "1.0",
            "snapshot_time": snapshot_time,
            "effective_as_of": _iso_ms(now_ms),
            "timezone": "Asia/Shanghai",
            "account_alias": profile_name,
            "profile_name": profile_name,
            "environment": environment,
            "spot_included": False,
            "symbols": selected_symbols,
            "quick_snapshot_bar_limits": QUICK_MARKET_BAR_LIMITS,
            "quick_snapshot_dvol_limits": QUICK_DVOL_BAR_LIMITS,
            "freshness_preflight": freshness_preflight,
            "data_quality": {
                "market": "OK" if market_ok else "MISSING",
                "dvol": dvol_quality,
                "positions": "OK",
                "options": "OK",
                "market_dvol_aligned": alignment_ok,
                "warnings": warnings,
            },
            "recent_related_trades": related_trades,
        }
        payload.update(asset_payload)
        slug = "_".join(selected_symbols) if len(selected_symbols) <= 3 else "MULTI"
        destination = (Path(output_root) if output_root is not None else ai_snapshots_dir_path()) / _profile_slug(profile_name) / self.now.strftime("%Y") / self.now.strftime("%m") / self.now.strftime("%d")
        destination.mkdir(parents=True, exist_ok=True)
        target = destination / f"qqokx_AI_Quick_{slug}_{self.now.strftime('%Y%m%d_%H%M%S')}.json"
        if target.exists():
            target = destination / f"qqokx_AI_Quick_{slug}_{self.now.strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:8]}.json"
        temp = target.with_suffix(target.suffix + ".tmp")
        temp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        temp.replace(target)
        payload["file_path"] = str(target)
        self._progress(f"AI 精简快照已保存：{target}")
        return payload


def generate_ai_quick_snapshot(
    *,
    credentials: Credentials,
    profile_name: str,
    environment: str,
    symbols: Iterable[object] = ("BTC",),
    include_option_chain: bool = False,
    option_chain_expiry: str | None = None,
    option_chain_strike_min: object = None,
    option_chain_strike_max: object = None,
    client: OkxRestClient | Any | None = None,
    volatility_client: DeribitRestClient | Any | None = None,
    now: datetime | None = None,
    output_root: Path | None = None,
    progress_callback: Callable[[str], None] | None = None,
) -> dict[str, object]:
    return AIQuickSnapshotBuilder(
        client=client,
        volatility_client=volatility_client,
        now=now,
        progress_callback=progress_callback,
    ).build(
        credentials=credentials,
        profile_name=profile_name,
        environment=environment,
        symbols=symbols,
        include_option_chain=include_option_chain,
        option_chain_expiry=option_chain_expiry,
        option_chain_strike_min=option_chain_strike_min,
        option_chain_strike_max=option_chain_strike_max,
        output_root=output_root,
    )
