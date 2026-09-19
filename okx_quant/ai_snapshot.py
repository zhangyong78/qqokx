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


def build_market_rows(candles: Sequence[Candle], *, limit: int = 250) -> list[dict[str, object]]:
    """Compute indicators before slicing, preserving the chart's warm-up semantics."""
    ordered = sorted(candles, key=lambda item: item.ts)
    if not ordered:
        return []
    closes = [float(item.close) for item in ordered]
    ema15 = _ema(closes, 15)
    ma50 = _sma(closes, 50)
    selected = ordered[-limit:]
    start = len(ordered) - len(selected)
    rows: list[dict[str, object]] = []
    for index, candle in enumerate(selected, start=start):
        rows.append(
            {
                "timestamp_ms": int(candle.ts),
                "timestamp": _iso_ms(int(candle.ts)),
                "open": _number(candle.open),
                "high": _number(candle.high),
                "low": _number(candle.low),
                "close": _number(candle.close),
                "volume": _number(candle.volume),
                "ema15": format(ema15[index], ".12g"),
                "ma50": format(ma50[index], ".12g"),
                "is_closed": bool(candle.confirmed),
            }
        )
    return rows


def _aggregate_dvol(candles: Sequence[DeribitVolatilityCandle], step_ms: int) -> list[DeribitVolatilityCandle]:
    buckets: dict[int, list[DeribitVolatilityCandle]] = {}
    for candle in candles:
        buckets.setdefault((int(candle.ts) // step_ms) * step_ms, []).append(candle)
    result: list[DeribitVolatilityCandle] = []
    for ts, items in sorted(buckets.items()):
        result.append(DeribitVolatilityCandle(ts, items[0].open, max(x.high for x in items), min(x.low for x in items), items[-1].close))
    return result


def build_dvol_rows(candles: Sequence[DeribitVolatilityCandle], *, step_ms: int, now_ms: int, limit: int = 250) -> list[dict[str, object]]:
    aggregated = _aggregate_dvol(sorted(candles, key=lambda item: item.ts), step_ms)
    closes = [float(item.close) for item in aggregated]
    ema15 = _ema(closes, 15)
    ma50 = _sma(closes, 50)
    rows: list[dict[str, object]] = []
    start = max(0, len(aggregated) - limit)
    for index, candle in enumerate(aggregated[-limit:], start=start):
        rows.append(
            {
                "timestamp_ms": int(candle.ts),
                "timestamp": _iso_ms(candle.ts),
                "open": _number(candle.open),
                "high": _number(candle.high),
                "low": _number(candle.low),
                "close": _number(candle.close),
                "ema15": format(ema15[index], ".12g"),
                "ma50": format(ma50[index], ".12g"),
                "is_closed": int(candle.ts) + step_ms <= now_ms,
            }
        )
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

    def _freshness_preflight(self, assets: Sequence[str], *, now_ms: int) -> dict[str, object]:
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
                    if base in {"BTC", "ETH"}:
                        dvol_rows = self.volatility_client.get_volatility_index_candles(
                            base,
                            "3600",
                            start_ts=now_ms - 3 * 60 * 60 * 1000,
                            end_ts=now_ms,
                            max_records=None,
                        )
                        dvol_latest = max((int(item.ts) for item in dvol_rows), default=0) or None
                        if dvol_latest is None:
                            raise FreshnessCheckError(f"{base}：波动率网站没有返回最新 1H 数据")
                    okx_age = max(0, now_ms - okx_latest)
                    dvol_age = max(0, now_ms - dvol_latest) if dvol_latest else None
                    if okx_age > 60 * 60 * 1000:
                        raise FreshnessCheckError(f"{base}：OKX 最新行情已滞后 {okx_age // 60_000} 分钟")
                    if dvol_age is not None and dvol_age > 60 * 60 * 1000:
                        raise FreshnessCheckError(f"{base}：波动率数据已滞后 {dvol_age // 60_000} 分钟")
                    if dvol_latest is not None and abs(okx_latest - dvol_latest) > 60 * 60 * 1000:
                        raise FreshnessCheckError(f"{base}：OKX 与波动率最新小时不一致")
                    checked[base] = {
                        "okx_latest": _iso_ms(okx_latest),
                        "volatility_latest": _iso_ms(dvol_latest),
                        "okx_age_minutes": okx_age // 60_000,
                        "volatility_age_minutes": dvol_age,
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
                rows = build_market_rows(candles, limit=250)
                if not rows:
                    raise ValueError(f"{base} {bar} 没有可用的核心行情数据")
                bar_counts[bar] = len(rows)
                if len(rows) < 250:
                    warnings.append(f"{base} {bar} 只有 {len(rows)} 根，少于要求的 250 根")
                market[bar] = {"instrument_id": inst_id, "candles": rows, "data_as_of": rows[-1]["timestamp"] if rows else None}
                if rows:
                    latest_market.append(int(rows[-1]["timestamp_ms"]))
            volatility: dict[str, object] = {"enabled": False, "reason": "not_available"}
            if base in {"BTC", "ETH"}:
                try:
                    start_ms = now_ms - DVOL_HOURLY_LIMIT_DAYS * 24 * 60 * 60 * 1000
                    hourly = self.volatility_client.get_volatility_index_candles(
                        base, "3600", start_ts=start_ms, end_ts=now_ms, max_records=None
                    )
                    volatility = {
                        "enabled": bool(hourly),
                        "index": f"{base}-DVOL",
                        "hourly": build_dvol_rows(hourly, step_ms=60 * 60 * 1000, now_ms=now_ms),
                        "4H": build_dvol_rows(hourly, step_ms=4 * 60 * 60 * 1000, now_ms=now_ms),
                        "1D": build_dvol_rows(hourly, step_ms=24 * 60 * 60 * 1000, now_ms=now_ms),
                        "data_as_of": _iso_ms(hourly[-1].ts) if hourly else None,
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
            market_age_minutes = max(0, now_ms - market_latest) // 60_000 if market_latest else None
            volatility_age_minutes = max(0, now_ms - volatility_latest) // 60_000 if volatility_latest else None
            if market_age_minutes is not None and market_age_minutes > 60:
                warnings.append(f"{base} 核心行情滞后 {market_age_minutes} 分钟")
            if volatility_age_minutes is not None and volatility_age_minutes > 60:
                warnings.append(f"{base} DVOL 滞后 {volatility_age_minutes} 分钟")
            asset_payload[base] = {
                "market": market,
                "volatility": volatility,
                "held": any(item.inst_id.upper().startswith(f"{base}-") for item in positions),
                "freshness": {
                    "market_latest": _iso_ms(market_latest),
                    "volatility_latest": _iso_ms(volatility_latest),
                    "market_age_minutes": market_age_minutes,
                    "volatility_age_minutes": volatility_age_minutes,
                    "aligned_within_60m": not volatility_latest or abs(market_latest - volatility_latest) <= 60 * 60 * 1000,
                    "bar_counts": bar_counts,
                },
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
            "effective_as_of_hour": datetime.fromtimestamp(effective_ms / 1000, tz=timezone.utc).replace(minute=0, second=0, microsecond=0).isoformat().replace("+00:00", "Z"),
            "timezone": "Asia/Shanghai",
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
