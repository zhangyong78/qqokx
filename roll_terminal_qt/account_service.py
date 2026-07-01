from __future__ import annotations

import time
from dataclasses import dataclass
from decimal import Decimal

from PySide6.QtCore import QThread, Signal

from okx_quant.arbitrage.models import ArbitrageTradeRuntime
from okx_quant.models import Instrument
from okx_quant.okx_client import OkxPosition, OkxRestClient


@dataclass(frozen=True)
class FuturesPositionView:
    position_key: str
    inst_id: str
    inst_type: str
    side: str
    available: Decimal
    contracts: Decimal
    api_available: Decimal
    api_contracts: Decimal
    lot_size: Decimal
    notional_base: Decimal | None
    contract_value: Decimal | None
    contract_value_ccy: str
    notional_value: Decimal | None
    label: str


class AccountFeedThread(QThread):
    positions_ready = Signal(object)
    spot_balances_ready = Signal(object)
    status_changed = Signal(str)

    def __init__(self, runtime: ArbitrageTradeRuntime | None) -> None:
        super().__init__()
        self._runtime = runtime
        self._client = OkxRestClient()
        self._running = True

    def stop(self) -> None:
        self._running = False

    def run(self) -> None:
        while self._running:
            if self._runtime is None:
                self.status_changed.emit("API 配置不可用")
                time.sleep(2)
                continue
            try:
                positions = self._load_positions()
                spot_balances = self._load_spot_balance_lookup()
                self.positions_ready.emit(positions)
                self.spot_balances_ready.emit(spot_balances)
                self.status_changed.emit(self._build_status_text(len(positions)))
                time.sleep(2.5)
            except Exception as exc:  # noqa: BLE001
                self.status_changed.emit(f"持仓读取异常：{exc}")
                time.sleep(2.5)

    def _load_positions(self) -> list[FuturesPositionView]:
        assert self._runtime is not None
        cached_payload = self._client.get_cached_private_positions(
            self._runtime.credentials,
            environment=self._runtime.environment,
        )
        if cached_payload is not None:
            _, cached_positions = cached_payload
            derivatives = [item for item in cached_positions if item.inst_type.upper() in {"FUTURES", "SWAP", "OPTION"}]
            if derivatives:
                return [self._to_view(item) for item in derivatives]
        rest_positions = [
            *self._client.get_positions(
                self._runtime.credentials,
                environment=self._runtime.environment,
                inst_type="FUTURES",
                prefer_cache=False,
            ),
            *self._client.get_positions(
                self._runtime.credentials,
                environment=self._runtime.environment,
                inst_type="SWAP",
                prefer_cache=False,
            ),
            *self._client.get_positions(
                self._runtime.credentials,
                environment=self._runtime.environment,
                inst_type="OPTION",
                prefer_cache=False,
            ),
        ]
        return [self._to_view(item) for item in rest_positions]

    def _load_spot_balance_lookup(self) -> dict[str, str]:
        assert self._runtime is not None
        overview = None
        cached_payload = self._client.get_cached_private_account_overview(
            self._runtime.credentials,
            environment=self._runtime.environment,
        )
        if cached_payload is not None:
            _, overview = cached_payload
        if overview is None:
            try:
                overview = self._client.get_account_overview(
                    self._runtime.credentials,
                    environment=self._runtime.environment,
                    prefer_cache=False,
                )
            except Exception:
                return {}
        details = getattr(overview, "details", ()) or ()
        lookup: dict[str, str] = {}
        for asset in details:
            ccy = str(getattr(asset, "ccy", "") or "").strip().upper()
            if not ccy or ccy in {"USDT", "USDC", "USD"}:
                continue
            available = getattr(asset, "available_balance", None)
            equity = getattr(asset, "equity", None)
            cash_balance = getattr(asset, "cash_balance", None)
            total = equity if equity is not None else cash_balance
            qty = available if available is not None and available > 0 else total
            if qty is None or qty <= 0:
                continue
            text = f"{ccy}-USDT | 可用 {_fmt(qty)} {ccy}"
            if total is not None and total > 0 and total != qty:
                text += f" | 余额 {_fmt(total)} {ccy}"
            lookup[ccy] = text
        return lookup

    def _build_status_text(self, position_count: int) -> str:
        assert self._runtime is not None
        try:
            status = self._client.get_private_ws_debug_status(
                self._runtime.credentials,
                environment=self._runtime.environment,
            )
        except Exception:
            status = {}
        connected = "在线" if status.get("connected") else "未就绪"
        positions_version = int(status.get("positions_version") or 0)
        order_version = int(status.get("version") or 0)
        return f"衍生品持仓 {position_count} 条 | 私有WS {connected} | 持仓v{positions_version} 订单v{order_version}"

    def _to_view(self, position: OkxPosition) -> FuturesPositionView:
        api_available = abs(position.avail_position or position.position)
        api_contracts = abs(position.position)
        pos_side = (position.pos_side or "").strip().lower()
        if pos_side in {"long", "short"}:
            side = pos_side
        else:
            side = "short" if position.position < 0 else "long"
        side_text = "\u7a7a" if side == "short" else "\u591a"
        base_ccy = _base_ccy(position.inst_id)
        base_qty = None
        contract_value = None
        contract_value_ccy = ""
        notional_value = None
        lot_size = Decimal("1")
        try:
            instrument = self._client.get_instrument(position.inst_id, prefer_cached=True)
            lot_size = instrument.lot_size if instrument.lot_size > 0 else Decimal("1")
            contracts = api_available
            contract_value = _contract_value(instrument)
            contract_value_ccy = (instrument.ct_val_ccy or "").strip().upper()
            if contract_value is not None:
                notional_value = contracts * contract_value
            reference_price = self._reference_price(position)
            base_qty = _base_qty_from_contracts(
                contracts,
                instrument=instrument,
                base_ccy=base_ccy,
                reference_price=reference_price,
            )
        except Exception:
            contracts = api_available
            base_qty = None
            contract_value = None
            contract_value_ccy = ""
            notional_value = None
            lot_size = Decimal("1")
        base_text = "-" if base_qty is None else _fmt(base_qty)
        contract_text = _contract_text(contract_value, contract_value_ccy)
        notional_text = _notional_text(notional_value, contract_value_ccy)
        inst_type_text = str(position.inst_type or "").strip().upper() or "FUTURES"
        label = (
            f"{position.inst_id} | {side_text} | 可平 {_fmt(contracts)} 张 | {contract_text} | {notional_text} | "
            f"折合 {base_text} {base_ccy} | {inst_type_text} | {position.mgn_mode}/{pos_side or 'net'}"
        )
        return FuturesPositionView(
            position_key=_position_key(position.inst_id, side),
            inst_id=position.inst_id,
            inst_type=inst_type_text,
            side=side,
            available=contracts,
            contracts=api_contracts,
            api_available=api_available,
            api_contracts=api_contracts,
            lot_size=lot_size,
            notional_base=base_qty,
            contract_value=contract_value,
            contract_value_ccy=contract_value_ccy,
            notional_value=notional_value,
            label=label,
        )

    def _reference_price(self, position: OkxPosition) -> Decimal | None:
        if self._runtime is None:
            return None
        for price in (position.mark_price, position.last_price, position.avg_price):
            if price is not None and price > 0:
                return price
        inst_id = position.inst_id
        try:
            ticker_payload = self._client.get_cached_public_ticker(inst_id, environment=self._runtime.environment)
            if ticker_payload is not None:
                ticker = ticker_payload[1]
                price = ticker.mark or ticker.last or ticker.bid or ticker.ask
                if price is not None and price > 0:
                    return price
        except Exception:
            pass
        try:
            ticker = self._client.get_ticker(inst_id)
            price = ticker.mark or ticker.last or ticker.bid or ticker.ask
            if price is not None and price > 0:
                return price
        except Exception:
            return None
        return None


def _base_qty_from_contracts(
    contracts: Decimal,
    *,
    instrument: Instrument,
    base_ccy: str,
    reference_price: Decimal | None,
) -> Decimal | None:
    if instrument.ct_val is None or instrument.ct_val <= 0:
        return None
    one_contract_value = _contract_value(instrument)
    if one_contract_value is None:
        return None
    contract_value = max(contracts, Decimal("0")) * one_contract_value
    value_ccy = (instrument.ct_val_ccy or "").strip().upper()
    if value_ccy in {"USD", "USDT", "USDC"}:
        if reference_price is None or reference_price <= 0:
            return None
        return _round_base(contract_value / reference_price)
    return _round_base(contract_value)


def _base_ccy(inst_id: str) -> str:
    return (inst_id or "").strip().upper().split("-", 1)[0] or "BTC"


def _position_key(inst_id: str, side: str) -> str:
    inst = (inst_id or "").strip().upper()
    normalized_side = (side or "").strip().lower() or "net"
    return f"{inst}|{normalized_side}"


def _contract_value(instrument: Instrument) -> Decimal | None:
    if instrument.ct_val is None or instrument.ct_val <= 0:
        return None
    multiplier = instrument.ct_mult if instrument.ct_mult is not None and instrument.ct_mult > 0 else Decimal("1")
    return instrument.ct_val * multiplier


def _contract_text(value: Decimal | None, ccy: str) -> str:
    if value is None:
        return "1张=未知"
    suffix = f" {ccy}" if ccy else ""
    return f"1张={_fmt(value)}{suffix}"


def _notional_text(value: Decimal | None, ccy: str) -> str:
    if value is None:
        return "面值 -"
    suffix = f" {ccy}" if ccy else ""
    return f"面值 {_fmt(value)}{suffix}"


def _round_base(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.00000001"))


def _fmt(value: Decimal) -> str:
    magnitude = abs(value)
    if magnitude >= Decimal("1000"):
        text = format(value.quantize(Decimal("0.01")), "f")
    elif magnitude >= Decimal("1"):
        text = format(value.quantize(Decimal("0.0001")), "f")
    elif magnitude >= Decimal("0.01"):
        text = format(value.quantize(Decimal("0.000001")), "f")
    elif magnitude >= Decimal("0.0001"):
        text = format(value.quantize(Decimal("0.00000001")), "f")
    else:
        text = format(value.quantize(Decimal("0.000000000001")), "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"
