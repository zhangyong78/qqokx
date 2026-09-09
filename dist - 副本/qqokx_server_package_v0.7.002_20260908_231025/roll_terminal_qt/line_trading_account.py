from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal

from okx_quant.models import Credentials


@dataclass(frozen=True)
class LineTradingDeskRuntime:
    credentials: Credentials
    environment: str
    trade_mode: str
    position_mode: str
    credential_profile_name: str


def build_runtime_from_profile_payload(
    *,
    profile_name: str,
    payload: Mapping[str, object],
    notification_snapshot: Mapping[str, object],
) -> LineTradingDeskRuntime:
    api_key = str(payload.get("api_key", "") or "").strip()
    secret_key = str(payload.get("secret_key", "") or "").strip()
    passphrase = str(payload.get("passphrase", "") or "").strip()
    if not api_key or not secret_key or not passphrase:
        raise ValueError(f"API Profile {profile_name or '-'} 缺少完整 API 凭证。")
    profile_environment = str(payload.get("environment", "") or "").strip().lower()
    environment = profile_environment if profile_environment in {"demo", "live"} else _environment_from_settings(
        notification_snapshot.get("environment_label"),
    )
    return LineTradingDeskRuntime(
        credentials=Credentials(
            api_key=api_key,
            secret_key=secret_key,
            passphrase=passphrase,
            profile_name=profile_name,
        ),
        environment=environment,
        trade_mode=_trade_mode_from_settings(notification_snapshot.get("trade_mode_label")),
        position_mode=_position_mode_from_settings(notification_snapshot.get("position_mode_label")),
        credential_profile_name=profile_name,
    )


def build_rr_order_intent(
    *,
    symbol: str,
    side: str,
    entry_price: Decimal,
    stop_price: Decimal,
    take_profit: Decimal,
    risk_usdt: Decimal,
    order_mode: str,
) -> dict[str, object]:
    normalized_symbol = str(symbol or "").strip().upper()
    if not normalized_symbol:
        raise ValueError("symbol is required")
    if side not in {"long", "short"}:
        raise ValueError("side must be long or short")
    if risk_usdt <= 0:
        raise ValueError("risk_usdt must be positive")
    if order_mode not in {"limit", "trigger"}:
        raise ValueError("order_mode must be limit or trigger")
    if side == "long":
        if stop_price >= entry_price:
            raise ValueError("long stop must be below entry")
        if take_profit <= entry_price:
            raise ValueError("long take_profit must be above entry")
    else:
        if stop_price <= entry_price:
            raise ValueError("short stop must be above entry")
        if take_profit >= entry_price:
            raise ValueError("short take_profit must be below entry")
    return {
        "inst_id": normalized_symbol,
        "direction": side,
        "entry_price": entry_price,
        "stop_price": stop_price,
        "take_profit": take_profit,
        "risk_usdt": risk_usdt,
        "order_mode": order_mode,
    }


def _fmt(value: object) -> str:
    if value is None:
        return "-"
    if isinstance(value, Decimal):
        if not value.is_finite():
            return "-"
        text = format(value, "f")
        text = text.rstrip("0").rstrip(".") if "." in text else text
        return "0" if text in {"", "-0"} else text
    return str(value)


def position_row_cells(position: object) -> list[str]:
    return [
        _fmt(_field(position, "inst_id")),
        _fmt(_field(position, "pos_side")),
        _fmt(_field(position, "position")),
        _fmt(_field(position, "avg_price")),
        _fmt(_field(position, "mark_price")),
        _fmt(_field(position, "upl")),
    ]


def _field(source: object, name: str) -> object:
    if isinstance(source, Mapping):
        return source.get(name)
    return getattr(source, name, None)


def _environment_from_settings(label: object, fallback: str = "demo") -> str:
    text = str(label or "").strip().lower()
    if text.endswith("live") or "live" in text:
        return "live"
    if text.endswith("demo") or "demo" in text:
        return "demo"
    return fallback


def _trade_mode_from_settings(label: object) -> str:
    text = str(label or "").strip().lower()
    return "isolated" if "isolated" in text else "cross"


def _position_mode_from_settings(label: object) -> str:
    text = str(label or "").strip().lower()
    return "long_short" if "long/short" in text or "long_short" in text else "net"
