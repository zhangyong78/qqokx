from __future__ import annotations

import threading
import time
import json
import os
from hashlib import sha256
from dataclasses import dataclass, replace
from decimal import Decimal, InvalidOperation
from pathlib import Path

from PySide6.QtCore import QThread, Signal, Slot
from PySide6.QtWidgets import (
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QTextEdit,
    QVBoxLayout,
)

from okx_quant.arbitrage.arbitrage_executor import _build_strategy_config
from okx_quant.app_paths import state_dir_path
from okx_quant.arbitrage.models import ArbitrageTradeRuntime
from okx_quant.client_order_id import new_custom_order_id
from okx_quant.models import Instrument
from okx_quant.okx_client import OkxApiError, OkxOrderBook, OkxOrderStatus, OkxPosition, OkxRestClient, OkxTicker
from okx_quant.option_strategy import parse_option_contract
from okx_quant.pricing import format_decimal, snap_to_increment
from roll_terminal_qt.app_icon import apply_qt_window_icon
from roll_terminal_qt.runtime import load_runtime


DEFAULT_BATCH_SIZE = Decimal("1")
DEFAULT_WAIT_SECONDS = Decimal("120")
DEFAULT_MARK_DEVIATION_PCT = Decimal("5")
MAX_MARKET_DATA_AGE_MS = 15_000
MAX_MARKET_DATA_FUTURE_SKEW_MS = 5_000


@dataclass(frozen=True)
class OptionRollExecutionPlan:
    current_inst_id: str
    target_inst_id: str
    direction: str
    total_qty: Decimal
    wait_seconds: float
    max_mark_deviation_pct: Decimal


@dataclass(frozen=True)
class OptionRollRefreshResult:
    positions: tuple[OkxPosition, ...]
    instruments: dict[str, Instrument]
    tickers: dict[str, OkxTicker]


def _position_key(position: OkxPosition) -> str:
    return f"{position.inst_id.strip().upper()}|{position.pos_side.strip().lower()}"


def _position_direction(position: OkxPosition) -> str:
    raw = str(position.pos_side or "").strip().lower()
    if raw in {"long", "short"}:
        return raw
    return "long" if position.position > 0 else "short"


def _position_available(position: OkxPosition) -> Decimal:
    available = position.avail_position
    return abs(available) if available is not None else Decimal("0")


TERMINAL_ORDER_STATES = {"filled", "canceled", "mmp_canceled"}


@dataclass
class _TrackedOrder:
    inst_id: str
    cl_ord_id: str
    label: str
    ord_id: str = ""
    status: OkxOrderStatus | None = None


def _journal_path(runtime: ArbitrageTradeRuntime) -> Path:
    account = runtime.credentials.profile_name or runtime.credentials.api_key
    key = sha256(f"{runtime.environment}|{account}".encode("utf-8")).hexdigest()[:20]
    return state_dir_path() / f"option_roll_pending_{key}.json"


def _validate_roll_contracts(current: Instrument, target: Instrument) -> None:
    old = parse_option_contract(current.inst_id)
    new = parse_option_contract(target.inst_id)
    if current.inst_type != "OPTION" or target.inst_type != "OPTION":
        raise ValueError("期权移仓只能使用期权合约")
    if str(current.state).lower() != "live" or str(target.state).lower() != "live":
        raise ValueError("旧/新期权不再处于可交易状态")
    if old.inst_family != new.inst_family or old.option_type != new.option_type or old.strike != new.strike:
        raise ValueError("目标期权必须与旧期权同币种、同行权价且同为看涨或看跌")
    if new.expiry_code <= old.expiry_code:
        raise ValueError("目标期权到期日必须晚于旧期权")
    if (current.ct_val, current.ct_mult, current.ct_val_ccy) != (target.ct_val, target.ct_mult, target.ct_val_ccy):
        raise ValueError("旧/新期权合约乘数不同，不能按 1 张对 1 张移仓")


def _passive_option_price(
    *,
    side: str,
    ticker: OkxTicker,
    order_book: OkxOrderBook,
) -> Decimal:
    normalized_side = side.strip().lower()
    if normalized_side == "buy":
        raw = order_book.bids[0][0] if order_book.bids else ticker.bid
    elif normalized_side == "sell":
        raw = order_book.asks[0][0] if order_book.asks else ticker.ask
    else:
        raise ValueError(f"不支持的期权订单方向：{side}")
    if raw is None or raw <= 0:
        raise ValueError(f"{ticker.inst_id} 缺少有效的{'买一' if normalized_side == 'buy' else '卖一'}价格")
    return raw


def _mark_deviation_pct(price: Decimal, mark_price: Decimal) -> Decimal:
    if mark_price <= 0:
        raise ValueError("期权标记价格无效")
    return abs(price - mark_price) / mark_price * Decimal("100")


def _require_fresh_market_timestamp(raw: dict[str, object], *, label: str, now_ms: int | None = None) -> int:
    try:
        timestamp = int(raw.get("ts", ""))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label}缺少有效交易所时间戳，暂停下单") from exc
    current_ms = int(time.time() * 1000) if now_ms is None else now_ms
    age = current_ms - timestamp
    if age > MAX_MARKET_DATA_AGE_MS or age < -MAX_MARKET_DATA_FUTURE_SKEW_MS:
        raise ValueError(f"{label}行情时间戳异常（延迟 {age} 毫秒），暂停下单")
    return timestamp


def _validate_quote_freshness(ticker: OkxTicker, order_book: OkxOrderBook, *, now_ms: int | None = None) -> None:
    _require_fresh_market_timestamp(ticker.raw, label=f"{ticker.inst_id} ticker", now_ms=now_ms)
    _require_fresh_market_timestamp(order_book.raw, label=f"{ticker.inst_id} order book", now_ms=now_ms)
    _require_fresh_market_timestamp({"ts": ticker.raw.get("markTs")}, label=f"{ticker.inst_id} mark price", now_ms=now_ms)
    if ticker.bid is not None and ticker.ask is not None and ticker.bid > ticker.ask:
        raise ValueError(f"{ticker.inst_id} 买卖报价交叉，暂停下单")
    if order_book.bids and order_book.asks and order_book.bids[0][0] > order_book.asks[0][0]:
        raise ValueError(f"{ticker.inst_id} 盘口买卖价交叉，暂停下单")


def _snap_option_price(
    price: Decimal,
    instrument: Instrument,
    *,
    side: str,
    tick_bands: object | None = None,
) -> Decimal:
    tick_size = instrument.tick_size
    if isinstance(tick_bands, (list, tuple)):
        for band in tick_bands:
            minimum = getattr(band, "min_price", None)
            maximum = getattr(band, "max_price", None)
            candidate_tick = getattr(band, "tick_size", None)
            if (
                isinstance(minimum, Decimal)
                and isinstance(candidate_tick, Decimal)
                and candidate_tick > 0
                and price >= minimum
                and (maximum is None or price < maximum)
            ):
                tick_size = candidate_tick
                break
    if tick_size is None or tick_size <= 0:
        raise ValueError(f"{instrument.inst_id} 缺少有效期权价格精度")
    rounding = "down" if side.strip().lower() == "buy" else "up"
    snapped = snap_to_increment(price, tick_size, rounding)
    if snapped <= 0:
        raise ValueError(f"{instrument.inst_id} 计算出的委托价格无效")
    return snapped


class _OptionRollRefreshThread(QThread):
    completed = Signal(object)
    failed = Signal(str)

    def __init__(self, *, runtime: ArbitrageTradeRuntime, client: OkxRestClient, parent: QDialog) -> None:
        super().__init__(parent)
        self._runtime = runtime
        self._client = client

    def run(self) -> None:
        try:
            positions = tuple(
                position
                for position in self._client.get_positions(
                    self._runtime.credentials,
                    environment=self._runtime.environment,
                    inst_type="OPTION",
                    prefer_cache=False,
                )
                if abs(position.position) > 0
            )
            families = {parse_option_contract(item.inst_id).inst_family for item in positions}
            instruments: dict[str, Instrument] = {}
            tickers: dict[str, OkxTicker] = {}
            for family in sorted(families):
                for instrument in self._client.get_option_instruments(inst_family=family):
                    if str(instrument.state or "").strip().lower() == "live":
                        instruments[instrument.inst_id] = instrument
                for ticker in self._client.get_tickers("OPTION", inst_family=family):
                    tickers[ticker.inst_id] = ticker
            self.completed.emit(OptionRollRefreshResult(positions, instruments, tickers))
        except Exception as exc:  # noqa: BLE001
            self.failed.emit(str(exc))


class _OptionRollExecutionThread(QThread):
    message = Signal(str)
    completed = Signal(bool, str)

    def __init__(
        self,
        *,
        runtime: ArbitrageTradeRuntime,
        client: OkxRestClient,
        plan: OptionRollExecutionPlan,
        parent: QDialog,
    ) -> None:
        super().__init__(parent)
        self._runtime = runtime
        self._client = client
        self._plan = plan
        self._stop_event = threading.Event()
        self._active_orders: list[_TrackedOrder] = []
        self._journal = _journal_path(runtime)
        self._journal_stage = "idle"
        self._baseline_current: Decimal | None = None
        self._baseline_target: Decimal | None = None

    def request_stop(self) -> None:
        self._stop_event.set()

    def _log(self, text: str) -> None:
        self.message.emit(text)

    def _config(self, inst_id: str):
        return _build_strategy_config(inst_id, self._runtime)

    def _save_journal(self) -> None:
        payload = {
            "current_inst_id": self._plan.current_inst_id,
            "target_inst_id": self._plan.target_inst_id,
            "direction": self._plan.direction,
            "stage": self._journal_stage,
            "baseline_current": str(self._baseline_current) if self._baseline_current is not None else None,
            "baseline_target": str(self._baseline_target) if self._baseline_target is not None else None,
            "orders": [
                {"inst_id": item.inst_id, "cl_ord_id": item.cl_ord_id, "ord_id": item.ord_id,
                 "label": item.label, "state": item.status.state if item.status else "unknown",
                 "filled": str(self._filled(item.status))}
                for item in self._active_orders
            ],
        }
        temporary = self._journal.with_name(f"{self._journal.name}.{os.getpid()}.tmp")
        try:
            temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            os.replace(temporary, self._journal)
        finally:
            temporary.unlink(missing_ok=True)

    def _clear_journal(self) -> None:
        self._journal.unlink(missing_ok=True)
        self._active_orders.clear()

    def _order_status(self, order: _TrackedOrder) -> OkxOrderStatus:
        return self._client.get_order(
            self._runtime.credentials,
            self._config(order.inst_id),
            inst_id=order.inst_id,
            ord_id=order.ord_id or None,
            cl_ord_id=None if order.ord_id else order.cl_ord_id,
            request_timeout=8.0,
        )

    def _read_status(self, order: _TrackedOrder) -> OkxOrderStatus:
        status = self._order_status(order)
        order.status = status
        if not order.ord_id and status.ord_id:
            order.ord_id = status.ord_id
        self._save_journal()
        return status

    def _cancel_active_orders(self) -> bool:
        """A cancel response is only an acknowledgement; require a terminal order state."""
        if not self._active_orders:
            return True
        deadline = time.monotonic() + 90.0
        while time.monotonic() < deadline:
            unresolved = []
            for order in self._active_orders:
                try:
                    status = self._read_status(order)
                    if status.state.lower() in TERMINAL_ORDER_STATES:
                        continue
                except Exception as exc:  # noqa: BLE001
                    self._log(f"{order.label} 状态暂不可查（{order.cl_ord_id}）：{exc}")
                unresolved.append(order)
                try:
                    self._client.cancel_order(
                        self._runtime.credentials, self._config(order.inst_id),
                        inst_id=order.inst_id, ord_id=order.ord_id or None,
                        cl_ord_id=None if order.ord_id else order.cl_ord_id,
                        request_timeout=8.0,
                    )
                except Exception as exc:  # noqa: BLE001
                    self._log(f"{order.label} 撤单请求异常：{exc}；继续核对最终状态")
            if not unresolved:
                return True
            time.sleep(2.0)
        self._log("撤单后仍有订单状态未知或未终结；已保留订单编号并禁止继续移仓，请在 OKX 核对。")
        return False

    @staticmethod
    def _filled(status: OkxOrderStatus | None) -> Decimal:
        return abs(status.filled_size or Decimal("0")) if status is not None else Decimal("0")

    def _load_quote(self, inst_id: str) -> tuple[Instrument, OkxTicker, OkxOrderBook]:
        instrument = self._client.get_instrument(inst_id)
        ticker = self._client.get_ticker(inst_id)
        order_book = self._client.get_order_book(inst_id, depth=5)
        mark_price, mark_ts = self._client.get_mark_price_snapshot(inst_id)
        now_ms = int(time.time() * 1000)
        if mark_price <= 0:
            raise ValueError(f"{inst_id} 缺少有效标记价格，暂停下单")
        ticker = replace(ticker, mark=mark_price, raw={**ticker.raw, "markTs": str(mark_ts)})
        _validate_quote_freshness(ticker, order_book, now_ms=now_ms)
        return instrument, ticker, order_book

    def _place_pair(self, direction: str) -> tuple[_TrackedOrder, _TrackedOrder]:
        current_inst, target_inst = self._plan.current_inst_id, self._plan.target_inst_id
        current_instrument, current_ticker, current_book = self._load_quote(current_inst)
        target_instrument, target_ticker, target_book = self._load_quote(target_inst)
        _validate_roll_contracts(current_instrument, target_instrument)
        close_side = "sell" if direction == "long" else "buy"
        open_side = "buy" if direction == "long" else "sell"
        current_raw = _passive_option_price(side=close_side, ticker=current_ticker, order_book=current_book)
        target_raw = _passive_option_price(side=open_side, ticker=target_ticker, order_book=target_book)
        current_tick_bands = self._client.get_option_tick_bands(parse_option_contract(current_inst).inst_family)
        target_tick_bands = self._client.get_option_tick_bands(parse_option_contract(target_inst).inst_family)
        current_price = _snap_option_price(
            current_raw,
            current_instrument,
            side=close_side,
            tick_bands=current_tick_bands,
        )
        target_price = _snap_option_price(
            target_raw,
            target_instrument,
            side=open_side,
            tick_bands=target_tick_bands,
        )
        for label, price, ticker in (
            ("旧期权", current_price, current_ticker),
            ("目标期权", target_price, target_ticker),
        ):
            deviation = _mark_deviation_pct(price, ticker.mark or Decimal("0"))
            if deviation > self._plan.max_mark_deviation_pct:
                raise ValueError(
                    f"{label}委托价 {format_decimal(price)} 与标记价 {format_decimal(ticker.mark or Decimal('0'))} "
                    f"偏离 {format_decimal(deviation)}%，超过限制 {format_decimal(self._plan.max_mark_deviation_pct)}%"
                )
        self._log(
            f"本轮1张：旧期权 {close_side} {format_decimal(current_price)}（标记 {format_decimal(current_ticker.mark or Decimal('0'))}） | "
            f"目标期权 {open_side} {format_decimal(target_price)}（标记 {format_decimal(target_ticker.mark or Decimal('0'))}）"
        )
        now_ms = int(time.time() * 1000)
        _validate_quote_freshness(current_ticker, current_book, now_ms=now_ms)
        _validate_quote_freshness(target_ticker, target_book, now_ms=now_ms)
        orders: list[_TrackedOrder] = []
        for inst_id, side, price, instrument, label in (
            (current_inst, close_side, current_price, current_instrument, "旧期权腿"),
            (target_inst, open_side, target_price, target_instrument, "目标期权腿"),
        ):
            if self._stop_event.is_set():
                raise RuntimeError("用户要求停止；不再提交下一腿")
            order = _TrackedOrder(inst_id, new_custom_order_id("optroll"), label)
            self._active_orders.append(order)
            self._journal_stage = "placing"
            # Persist the client order ID before the network call: a timeout may still mean accepted.
            self._save_journal()
            try:
                result = self._client.place_simple_order(
                    self._runtime.credentials, self._config(inst_id),
                    inst_id=inst_id, side=side, size=DEFAULT_BATCH_SIZE,
                    ord_type="post_only", price=price, cl_ord_id=order.cl_ord_id,
                    instrument=instrument,
                )
            except OkxApiError as exc:
                # An order-level sCode is an explicit rejection, unlike transport/timeout errors.
                if exc.code:
                    self._active_orders.remove(order)
                    self._save_journal()
                raise
            order.ord_id = result.ord_id
            self._save_journal()
            orders.append(order)
        self._journal_stage = "active"
        self._save_journal()
        return orders[0], orders[1]

    def _wait_pair(self, orders: tuple[_TrackedOrder, _TrackedOrder]) -> tuple[Decimal, Decimal]:
        deadline = time.monotonic() + self._plan.wait_seconds
        while time.monotonic() < deadline and not self._stop_event.is_set():
            for order in orders:
                try:
                    self._read_status(order)
                except Exception as exc:  # noqa: BLE001
                    self._log(f"订单 {order.cl_ord_id} 状态查询异常：{exc}")
            current_filled = self._filled(orders[0].status)
            target_filled = self._filled(orders[1].status)
            if all(order.status is not None and order.status.state.lower() == "filled" for order in orders):
                if current_filled != DEFAULT_BATCH_SIZE or target_filled != DEFAULT_BATCH_SIZE:
                    raise RuntimeError(
                        f"订单显示终态已成交，但累计成交量与本轮 1 张不符："
                        f"旧腿 {format_decimal(current_filled)}，目标腿 {format_decimal(target_filled)}"
                    )
                return current_filled, target_filled
            if any(
                order.status is not None and order.status.state.lower() in {"canceled", "mmp_canceled"}
                for order in orders
            ):
                break
            if current_filled > 0 or target_filled > 0:
                self._log(
                    f"等待双腿配对：旧期权已成 {format_decimal(current_filled)} 张，"
                    f"目标期权已成 {format_decimal(target_filled)} 张"
                )
            self._stop_event.wait(1.0)
        if not self._cancel_active_orders():
            raise RuntimeError("订单撤销或最终成交量无法确认，禁止按 0 成交继续执行")
        return self._filled(orders[0].status), self._filled(orders[1].status)

    def _position_snapshot(self) -> tuple[Decimal, Decimal, Decimal, Decimal]:
        positions = self._client.get_positions(
            self._runtime.credentials,
            environment=self._runtime.environment,
            inst_type="OPTION",
            prefer_cache=False,
        )
        current = [item for item in positions if item.inst_id == self._plan.current_inst_id and _position_direction(item) == self._plan.direction]
        target = [item for item in positions if item.inst_id == self._plan.target_inst_id and _position_direction(item) == self._plan.direction]
        opposite_target = [item for item in positions if item.inst_id == self._plan.target_inst_id and _position_direction(item) != self._plan.direction]
        return (
            sum((abs(item.position) for item in current), Decimal("0")),
            sum((abs(item.position) for item in target), Decimal("0")),
            sum((_position_available(item) for item in current), Decimal("0")),
            sum((abs(item.position) for item in opposite_target), Decimal("0")),
        )

    def _position_snapshot_with_retry(self, *, context: str, timeout_seconds: float = 60.0) -> tuple[Decimal, Decimal, Decimal, Decimal]:
        """Wait out a brief VPN/API interruption before declaring a safe stop."""
        deadline = time.monotonic() + timeout_seconds
        last_error: Exception | None = None
        while time.monotonic() < deadline:
            if self._stop_event.is_set():
                raise RuntimeError("用户要求停止；未提交新订单")
            try:
                return self._position_snapshot()
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                remaining = max(0, int(deadline - time.monotonic()))
                self._log(f"{context}持仓查询暂不可用，{remaining} 秒内继续重试：{exc}")
                self._stop_event.wait(min(2.0, max(0.1, deadline - time.monotonic())))
        raise RuntimeError(f"{context}持仓查询连续 {int(timeout_seconds)} 秒不可用，未提交新订单：{last_error}")

    def _verify_positions(self, before_current: Decimal, before_target: Decimal) -> bool:
        expected_current = max(before_current - DEFAULT_BATCH_SIZE, Decimal("0"))
        expected_target = before_target + DEFAULT_BATCH_SIZE
        deadline = time.monotonic() + 60.0
        latest_current = Decimal("0")
        latest_target = Decimal("0")
        had_snapshot = False
        while time.monotonic() < deadline:
            try:
                latest_current, latest_target, _, opposite_target = self._position_snapshot()
                had_snapshot = True
                self._log(
                    f"持仓复核：旧期权 {format_decimal(latest_current)} 张（预期 {format_decimal(expected_current)}），"
                    f"目标期权 {format_decimal(latest_target)} 张（预期 {format_decimal(expected_target)}）"
                )
                if latest_current == expected_current and latest_target == expected_target and opposite_target == 0:
                    return True
            except Exception as exc:  # noqa: BLE001
                self._log(f"持仓查询暂不可用，60 秒内继续重试：{exc}")
            time.sleep(1.0)
        if not had_snapshot:
            self._log("持仓复核超时：期间没有获得可信的持仓快照。")
            return False
        self._log(
            f"持仓复核超时：旧期权 {format_decimal(latest_current)} 张，"
            f"目标期权 {format_decimal(latest_target)} 张；已暂停。"
        )
        return False

    def run(self) -> None:
        completed = Decimal("0")
        owns_journal = False
        try:
            self._journal.parent.mkdir(parents=True, exist_ok=True)
            with self._journal.open("x", encoding="utf-8") as handle:
                handle.write("{}")
            owns_journal = True
            self._save_journal()
            while completed < self._plan.total_qty:
                if self._stop_event.is_set():
                    if not self._active_orders:
                        self._clear_journal()
                    self.completed.emit(False, "已停止期权移仓，未完成部分未继续下单。")
                    return
                before_current, before_target, available_current, opposite_target = self._position_snapshot_with_retry(
                    context="移仓前",
                )
                if available_current < DEFAULT_BATCH_SIZE:
                    self._clear_journal()
                    self.completed.emit(False, "旧期权可移仓持仓不足 1 张，已暂停。")
                    return
                if opposite_target > 0:
                    self._clear_journal()
                    self.completed.emit(False, "目标期权已有反向持仓，为避免净额抵消造成数量错配，已暂停。")
                    return
                self._baseline_current = before_current
                self._baseline_target = before_target
                self._journal_stage = "placing"
                self._save_journal()
                orders = self._place_pair(self._plan.direction)
                current_filled, target_filled = self._wait_pair(orders)
                if current_filled >= DEFAULT_BATCH_SIZE and target_filled >= DEFAULT_BATCH_SIZE:
                    if not self._verify_positions(before_current, before_target):
                        self.completed.emit(False, "双腿订单已成交，但持仓未能严格复核；已保留订单记录，禁止继续。")
                        return
                    completed += DEFAULT_BATCH_SIZE
                    self._active_orders.clear()
                    self._baseline_current = max(before_current - DEFAULT_BATCH_SIZE, Decimal("0"))
                    self._baseline_target = before_target + DEFAULT_BATCH_SIZE
                    self._journal_stage = "idle"
                    self._save_journal()
                    self._log(f"本轮配对完成：{format_decimal(completed)}/{format_decimal(self._plan.total_qty)} 张")
                    continue
                if current_filled == 0 and target_filled == 0 and all(
                    order.status is not None and order.status.state.lower() in {"canceled", "mmp_canceled"}
                    for order in orders
                ):
                    self._clear_journal()
                    self.completed.emit(False, "本轮等待结束，两腿均未成交且已确认撤单；未追价或自动重挂。")
                    return
                self.completed.emit(
                    False,
                    f"单腿成交导致未配对：旧期权 {format_decimal(current_filled)} 张，"
                    f"目标期权 {format_decimal(target_filled)} 张；已暂停并保留订单记录，请先核对持仓。",
                )
                return
            self._clear_journal()
            self.completed.emit(True, f"期权移仓完成，共配对 {format_decimal(completed)} 张。")
        except FileExistsError:
            self.completed.emit(False, f"存在未核对的期权移仓记录，禁止新下单：{self._journal}")
        except Exception as exc:  # noqa: BLE001
            confirmed = self._cancel_active_orders()
            if owns_journal and confirmed and all(
                order.status is not None and order.status.state.lower() in {"canceled", "mmp_canceled"}
                and self._filled(order.status) == 0 for order in self._active_orders
            ):
                self._clear_journal()
            elif owns_journal and not self._active_orders:
                self._clear_journal()
            elif owns_journal and self._journal.exists():
                self._log(f"请核对旧/新期权持仓及未完成订单；恢复前不要重启移仓：{self._journal}")
            self.completed.emit(False, f"期权移仓已暂停：{exc}")


class _OptionRollRecoveryThread(QThread):
    message = Signal(str)
    completed = Signal(bool, str)

    def __init__(self, *, runtime: ArbitrageTradeRuntime, client: OkxRestClient, journal: Path, parent: QDialog) -> None:
        super().__init__(parent)
        self._runtime = runtime
        self._client = client
        self._journal = journal
        self.result: tuple[bool, str] | None = None

    def _finish(self, safe_to_resume: bool, message: str) -> None:
        self.result = (safe_to_resume, message)
        self.completed.emit(safe_to_resume, message)

    def run(self) -> None:
        try:
            record = json.loads(self._journal.read_text(encoding="utf-8"))
            current_inst = str(record.get("current_inst_id") or "")
            target_inst = str(record.get("target_inst_id") or "")
            direction = str(record.get("direction") or "")
            if not current_inst or not target_inst or direction not in {"long", "short"}:
                self._finish(False, "遗留记录缺少合约或方向信息，保护记录已保留。")
                return
            order_rows = record.get("orders")
            if not isinstance(order_rows, list):
                self._finish(False, "遗留记录格式无效，保护记录已保留。")
                return
            if not order_rows:
                self._journal.unlink(missing_ok=True)
                self._finish(True, "记录中没有未完成订单编号，已清除空保护记录。")
                return
            statuses: list[tuple[dict[str, object], OkxOrderStatus]] = []
            deadline = time.monotonic() + 90.0
            while time.monotonic() < deadline:
                all_terminal = True
                statuses.clear()
                for row in order_rows:
                    if not isinstance(row, dict):
                        self._finish(False, "遗留订单记录格式无效，保护记录已保留。")
                        return
                    inst_id = str(row.get("inst_id") or "")
                    ord_id = str(row.get("ord_id") or "")
                    cl_ord_id = str(row.get("cl_ord_id") or "")
                    if not inst_id or not (ord_id or cl_ord_id):
                        self._finish(False, "遗留记录缺少订单编号，无法安全核对，保护记录已保留。")
                        return
                    try:
                        status = self._client.get_order(
                            self._runtime.credentials,
                            _build_strategy_config(inst_id, self._runtime),
                            inst_id=inst_id,
                            ord_id=ord_id or None,
                            cl_ord_id=None if ord_id else cl_ord_id,
                            request_timeout=8.0,
                        )
                        statuses.append((row, status))
                        if status.state.lower() not in TERMINAL_ORDER_STATES:
                            all_terminal = False
                            self._client.cancel_order(
                                self._runtime.credentials,
                                _build_strategy_config(inst_id, self._runtime),
                                inst_id=inst_id,
                                ord_id=ord_id or None,
                                cl_ord_id=None if ord_id else cl_ord_id,
                                request_timeout=8.0,
                            )
                            self.message.emit(f"已请求撤销遗留活动单 {cl_ord_id or ord_id}，继续核实终态。")
                    except Exception as exc:  # noqa: BLE001
                        all_terminal = False
                        self.message.emit(f"遗留订单 {cl_ord_id or ord_id} 暂时无法确认：{exc}")
                if all_terminal and len(statuses) == len(order_rows):
                    break
                time.sleep(2.0)
            else:
                self._finish(False, "遗留订单在 90 秒内仍未全部进入可确认终态，保护记录保留；请在 OKX 查看。")
                return

            baseline_current = Decimal(str(record.get("baseline_current")))
            baseline_target = Decimal(str(record.get("baseline_target")))
            old_filled = sum(
                (abs(status.filled_size or Decimal("0")) for row, status in statuses if str(row.get("inst_id")) == current_inst),
                Decimal("0"),
            )
            target_filled = sum(
                (abs(status.filled_size or Decimal("0")) for row, status in statuses if str(row.get("inst_id")) == target_inst),
                Decimal("0"),
            )
            if old_filled != target_filled:
                self._finish(False, f"两腿累计成交不平：旧腿 {format_decimal(old_filled)}，目标腿 {format_decimal(target_filled)}；记录保留。")
                return

            expected_current = max(baseline_current - old_filled, Decimal("0"))
            expected_target = baseline_target + target_filled
            position_deadline = time.monotonic() + 60.0
            current_qty = target_qty = opposite_target = Decimal("0")
            while time.monotonic() < position_deadline:
                try:
                    positions = self._client.get_positions(
                        self._runtime.credentials,
                        environment=self._runtime.environment,
                        inst_type="OPTION",
                        prefer_cache=False,
                    )
                    current_qty = sum((abs(item.position) for item in positions if item.inst_id == current_inst and _position_direction(item) == direction), Decimal("0"))
                    target_qty = sum((abs(item.position) for item in positions if item.inst_id == target_inst and _position_direction(item) == direction), Decimal("0"))
                    opposite_target = sum((abs(item.position) for item in positions if item.inst_id == target_inst and _position_direction(item) != direction), Decimal("0"))
                    if current_qty == expected_current and target_qty == expected_target and opposite_target == 0:
                        self._journal.unlink(missing_ok=True)
                        self._finish(True, f"订单终态、双腿成交和当前持仓一致（各 {format_decimal(old_filled)} 张）；已解除移仓保护。")
                        return
                    self.message.emit(
                        f"等待持仓同步：旧腿 {format_decimal(current_qty)}/{format_decimal(expected_current)}，"
                        f"目标腿 {format_decimal(target_qty)}/{format_decimal(expected_target)}。"
                    )
                except Exception as exc:  # noqa: BLE001
                    self.message.emit(f"持仓暂不可查询，60 秒内继续核对：{exc}")
                time.sleep(1.0)
            self._finish(
                False,
                f"订单已终结，但持仓在 60 秒内仍未与成交核对一致（旧腿 {format_decimal(current_qty)}/{format_decimal(expected_current)}，"
                f"目标腿 {format_decimal(target_qty)}/{format_decimal(expected_target)}）；记录保留。",
            )
        except Exception as exc:  # noqa: BLE001
            self._finish(False, f"核对遗留移仓失败，保护记录保留：{exc}")


class OptionRollExecutionQtWindow(QDialog):
    def __init__(self, *, profile_name: str = "", parent=None) -> None:  # noqa: ANN001
        super().__init__(parent)
        apply_qt_window_icon(self)
        self.setWindowTitle("期权移仓")
        self.resize(980, 680)
        self._profile_name = str(profile_name or "").strip()
        self._client = OkxRestClient()
        self._positions: dict[str, OkxPosition] = {}
        self._instruments: dict[str, Instrument] = {}
        self._tickers: dict[str, OkxTicker] = {}
        self._refresh_thread: _OptionRollRefreshThread | None = None
        self._execution_thread: _OptionRollExecutionThread | None = None
        self._recovery_thread: _OptionRollRecoveryThread | None = None
        self._execution_result: tuple[bool, str] | None = None
        self._recovery_result: tuple[bool, str] | None = None
        self._build_ui()
        self.refresh_data()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        title = QLabel("期权移仓")
        title.setObjectName("SectionTitle")
        layout.addWidget(title)
        hint = QLabel(
            "独立期权移仓：每轮固定 1 张，A/B 两腿依次提交 post-only 限价单（非交易所原子双腿单）。"
            "价格参考买一/卖一，并受标记价偏离限制；不会使用市价补齐或自动追价。"
        )
        hint.setWordWrap(True)
        hint.setObjectName("Subtle")
        layout.addWidget(hint)

        form = QFormLayout()
        self._current_combo = QComboBox()
        self._current_combo.currentIndexChanged.connect(self._on_current_changed)
        self._target_combo = QComboBox()
        self._target_combo.currentIndexChanged.connect(self._render_quotes)
        form.addRow("旧期权持仓 A 腿", self._current_combo)
        form.addRow("目标期权 B 腿", self._target_combo)
        self._qty = QLineEdit("1")
        self._wait = QLineEdit(str(DEFAULT_WAIT_SECONDS))
        self._deviation = QLineEdit(str(DEFAULT_MARK_DEVIATION_PCT))
        form.addRow("总移仓张数", self._qty)
        form.addRow("每轮张数", QLabel("1（固定）"))
        form.addRow("每轮最长等待(秒)", self._wait)
        form.addRow("委托价相对标记价最大偏离(%)", self._deviation)
        layout.addLayout(form)

        quote_row = QHBoxLayout()
        self._quote_label = QLabel("等待行情...")
        self._quote_label.setWordWrap(True)
        quote_row.addWidget(self._quote_label, 1)
        self._refresh_button = QPushButton("刷新期权持仓和行情")
        self._refresh_button.clicked.connect(self.refresh_data)
        quote_row.addWidget(self._refresh_button)
        self._recovery_button = QPushButton("核对遗留移仓")
        self._recovery_button.clicked.connect(self.start_recovery)
        self._recovery_button.setEnabled(False)
        quote_row.addWidget(self._recovery_button)
        layout.addLayout(quote_row)

        self._status = QLabel("准备中")
        self._status.setObjectName("Subtle")
        self._status.setWordWrap(True)
        layout.addWidget(self._status)
        self._log = QTextEdit()
        self._log.setReadOnly(True)
        layout.addWidget(self._log, 1)

        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Close)
        self._start_button = buttons.addButton("开始期权移仓", QDialogButtonBox.ButtonRole.ActionRole)
        self._stop_button = buttons.addButton("停止并撤销未成交单", QDialogButtonBox.ButtonRole.DestructiveRole)
        self._stop_button.setEnabled(False)
        self._start_button.clicked.connect(self.start_execution)
        self._stop_button.clicked.connect(self.stop_execution)
        buttons.rejected.connect(self.close)
        layout.addWidget(buttons)

    def apply_workspace_profile(self, profile_name: str) -> None:
        target = str(profile_name or "").strip()
        if target and target != self._profile_name:
            self._profile_name = target
            self.refresh_data()

    def _runtime(self) -> ArbitrageTradeRuntime | None:
        runtime = load_runtime(self._profile_name or None)
        return runtime

    @Slot()
    def refresh_data(self) -> None:
        if ((self._execution_thread is not None and self._execution_thread.isRunning())
            or (self._recovery_thread is not None and self._recovery_thread.isRunning())):
            self._status.setText("当前正在执行期权移仓，不能刷新并覆盖执行状态。")
            return
        runtime = self._runtime()
        if runtime is None:
            self._status.setText("当前 API Profile 不可用，请先选择有效账户。")
            return
        self._recovery_button.setEnabled(_journal_path(runtime).exists())
        if self._refresh_thread is not None and self._refresh_thread.isRunning():
            return
        self._refresh_button.setEnabled(False)
        self._status.setText("正在读取期权持仓、合约和行情...")
        thread = _OptionRollRefreshThread(runtime=runtime, client=self._client, parent=self)
        thread.completed.connect(self._apply_refresh)
        thread.failed.connect(self._refresh_failed)
        thread.finished.connect(self._refresh_finished)
        thread.finished.connect(thread.deleteLater)
        self._refresh_thread = thread
        thread.start()

    @Slot(object)
    def _apply_refresh(self, result: object) -> None:
        if not isinstance(result, OptionRollRefreshResult):
            return
        self._positions = {_position_key(item): item for item in result.positions}
        self._instruments = result.instruments
        self._tickers = result.tickers
        previous_current = self._current_combo.currentData()
        self._current_combo.blockSignals(True)
        self._current_combo.clear()
        for key, position in self._positions.items():
            direction = _position_direction(position)
            self._current_combo.addItem(
                f"{position.inst_id} | {'多' if direction == 'long' else '空'} | 可用 {format_decimal(_position_available(position))} 张",
                key,
            )
        if previous_current:
            index = self._current_combo.findData(previous_current)
            if index >= 0:
                self._current_combo.setCurrentIndex(index)
        self._current_combo.blockSignals(False)
        self._on_current_changed()
        self._status.setText(f"已读取 {len(self._positions)} 条期权持仓，可选择目标期权。")

    @Slot(str)
    def _refresh_failed(self, message: str) -> None:
        self._status.setText(f"读取期权数据失败：{message}")

    @Slot()
    def _refresh_finished(self) -> None:
        self._refresh_thread = None
        self._refresh_button.setEnabled(True)
        runtime = self._runtime()
        pending = _journal_path(runtime).exists() if runtime is not None else False
        self._recovery_button.setEnabled(pending)
        self._start_button.setEnabled(runtime is not None and not pending)

    @Slot()
    def _on_current_changed(self) -> None:
        current = self._positions.get(str(self._current_combo.currentData() or ""))
        self._target_combo.blockSignals(True)
        self._target_combo.clear()
        if current is not None:
            current_contract = parse_option_contract(current.inst_id)
            family = current_contract.inst_family
            current_instrument = self._instruments.get(current.inst_id)
            for inst_id, instrument in sorted(self._instruments.items()):
                if inst_id == current.inst_id or (instrument.inst_family or "").upper() != family.upper():
                    continue
                target_contract = parse_option_contract(inst_id)
                if (target_contract.option_type != current_contract.option_type
                    or target_contract.strike != current_contract.strike
                    or target_contract.expiry_code <= current_contract.expiry_code):
                    continue
                if current_instrument is not None and (
                    instrument.ct_val != current_instrument.ct_val
                    or instrument.ct_mult != current_instrument.ct_mult
                    or instrument.ct_val_ccy != current_instrument.ct_val_ccy
                ):
                    continue
                quote = self._tickers.get(inst_id)
                mark = format_decimal(quote.mark) if quote and quote.mark else "下单前实时读取"
                bid = format_decimal(quote.bid) if quote and quote.bid else "-"
                ask = format_decimal(quote.ask) if quote and quote.ask else "-"
                self._target_combo.addItem(f"{inst_id} | 标记 {mark} | 买 {bid} / 卖 {ask}", inst_id)
        self._target_combo.blockSignals(False)
        self._render_quotes()

    @Slot()
    def _render_quotes(self) -> None:
        current = self._positions.get(str(self._current_combo.currentData() or ""))
        target_inst = str(self._target_combo.currentData() or "")
        target_quote = self._tickers.get(target_inst)
        if current is None or target_quote is None:
            self._quote_label.setText("请选择有效的旧期权和目标期权。")
            return
        current_quote = self._tickers.get(current.inst_id)
        self._quote_label.setText(
            f"A腿 {current.inst_id}：标记 {format_decimal(current_quote.mark) if current_quote and current_quote.mark else '下单前实时读取'} "
            f"| 买一 {format_decimal(current_quote.bid) if current_quote and current_quote.bid else '-'} "
            f"| 卖一 {format_decimal(current_quote.ask) if current_quote and current_quote.ask else '-'}\n"
            f"B腿 {target_inst}：标记 {format_decimal(target_quote.mark) if target_quote.mark else '下单前实时读取'} "
            f"| 买一 {format_decimal(target_quote.bid) if target_quote.bid else '-'} "
            f"| 卖一 {format_decimal(target_quote.ask) if target_quote.ask else '-'}"
        )

    def _build_plan(self) -> OptionRollExecutionPlan | None:
        current = self._positions.get(str(self._current_combo.currentData() or ""))
        target_inst = str(self._target_combo.currentData() or "")
        if current is None or not target_inst:
            QMessageBox.warning(self, "期权移仓", "请先选择旧期权持仓和目标期权。")
            return None
        pending_path = _journal_path(self._runtime()) if self._runtime() is not None else None
        if pending_path is not None and pending_path.exists():
            QMessageBox.warning(self, "期权移仓", f"上次执行存在未核对订单，已禁止新下单。请先核对 OKX 订单和双腿持仓：\n{pending_path}")
            return None
        try:
            total_qty = Decimal(self._qty.text().strip())
            wait_seconds = float(Decimal(self._wait.text().strip()))
            max_deviation = Decimal(self._deviation.text().strip())
        except (InvalidOperation, ValueError) as exc:
            QMessageBox.warning(self, "参数错误", f"参数格式不正确：{exc}")
            return None
        if not total_qty.is_finite() or total_qty <= 0 or total_qty != total_qty.to_integral_value():
            QMessageBox.warning(self, "参数错误", "总移仓张数必须是正整数。")
            return None
        if total_qty > _position_available(current):
            QMessageBox.warning(self, "参数错误", "总移仓张数超过旧期权当前可用持仓。")
            return None
        if not (0 < wait_seconds <= 86400) or not max_deviation.is_finite() or not (0 < max_deviation <= 100):
            QMessageBox.warning(self, "参数错误", "等待时间需在 0～86400 秒内，标记价偏离限制需在 0～100% 内。")
            return None
        return OptionRollExecutionPlan(
            current_inst_id=current.inst_id,
            target_inst_id=target_inst,
            direction=_position_direction(current),
            total_qty=total_qty,
            wait_seconds=wait_seconds,
            max_mark_deviation_pct=max_deviation,
        )

    @Slot()
    def start_execution(self) -> None:
        if ((self._execution_thread is not None and self._execution_thread.isRunning())
            or (self._recovery_thread is not None and self._recovery_thread.isRunning())
            or (self._refresh_thread is not None and self._refresh_thread.isRunning())):
            return
        runtime = self._runtime()
        plan = self._build_plan()
        if runtime is None or plan is None:
            return
        answer = QMessageBox.question(
            self,
            "确认开始期权移仓",
            f"旧期权：{plan.current_inst_id}\n目标期权：{plan.target_inst_id}\n"
            f"方向：{'多' if plan.direction == 'long' else '空'}\n"
            f"总数量：{format_decimal(plan.total_qty)} 张，每轮 1 张\n\n"
            "将依次提交两腿 post-only 限价单；存在短暂单腿暴露风险，单腿成交或持仓核对异常会暂停。确认开始？",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )
        if answer != QMessageBox.StandardButton.Yes:
            return
        self._start_button.setEnabled(False)
        self._stop_button.setEnabled(True)
        self._refresh_button.setEnabled(False)
        self._recovery_button.setEnabled(False)
        self._log.clear()
        self._status.setText("期权移仓执行中...")
        self._execution_result = None
        thread = _OptionRollExecutionThread(runtime=runtime, client=self._client, plan=plan, parent=self)
        thread.message.connect(self._log.append)
        thread.completed.connect(self._record_execution_result)
        thread.finished.connect(self._execution_finished)
        thread.finished.connect(thread.deleteLater)
        self._execution_thread = thread
        thread.start()

    @Slot()
    def start_recovery(self) -> None:
        if ((self._recovery_thread is not None and self._recovery_thread.isRunning())
            or (self._execution_thread is not None and self._execution_thread.isRunning())
            or (self._refresh_thread is not None and self._refresh_thread.isRunning())):
            self._status.setText("当前有刷新或移仓任务正在运行，暂时不能检查遗留记录。")
            return
        runtime = self._runtime()
        if runtime is None:
            self._status.setText("当前 API Profile 不可用，无法核对遗留移仓。")
            return
        journal = _journal_path(runtime)
        if not journal.exists():
            self._recovery_button.setEnabled(False)
            QMessageBox.information(self, "核对遗留移仓", "没有待核对的期权移仓记录。")
            return
        answer = QMessageBox.question(
            self,
            "核对遗留移仓",
            "将查询记录中的订单；若发现活动委托，会请求撤销。只有订单进入终态、两腿成交相等且持仓与记录一致时，才会解除保护。继续？",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )
        if answer != QMessageBox.StandardButton.Yes:
            return
        self._recovery_result = None
        self._recovery_button.setEnabled(False)
        self._start_button.setEnabled(False)
        self._refresh_button.setEnabled(False)
        self._status.setText("正在核对遗留订单、双腿成交和持仓...")
        thread = _OptionRollRecoveryThread(runtime=runtime, client=self._client, journal=journal, parent=self)
        thread.message.connect(self._log.append)
        thread.completed.connect(self._record_recovery_result)
        thread.finished.connect(self._recovery_finished)
        thread.finished.connect(thread.deleteLater)
        self._recovery_thread = thread
        thread.start()

    @Slot()
    def stop_execution(self) -> None:
        if self._execution_thread is not None and self._execution_thread.isRunning():
            self._status.setText("正在停止并撤销未成交委托，请等待订单状态核对完成...")
            self._execution_thread.request_stop()

    @Slot(bool, str)
    def _record_execution_result(self, success: bool, message: str) -> None:
        self._execution_result = (success, message)

    @Slot(bool, str)
    def _record_recovery_result(self, safe_to_resume: bool, message: str) -> None:
        self._recovery_result = (safe_to_resume, message)

    @Slot()
    def _recovery_finished(self) -> None:
        safe_to_resume, message = self._recovery_result or (False, "核对线程结束，但没有核对结果；保护记录保留。")
        self._recovery_thread = None
        self._start_button.setEnabled(not _journal_path(self._runtime()).exists() if self._runtime() is not None else False)
        self._refresh_button.setEnabled(True)
        self._recovery_button.setEnabled(_journal_path(self._runtime()).exists() if self._runtime() is not None else False)
        self._status.setText(message)
        self._log.append(message)
        if safe_to_resume:
            self.refresh_data()

    @Slot()
    def _execution_finished(self) -> None:
        success, message = self._execution_result or (False, "期权移仓线程已结束，但未收到执行结果，请核对订单及持仓。")
        self._execution_thread = None
        self._start_button.setEnabled(False)
        self._stop_button.setEnabled(False)
        self._refresh_button.setEnabled(True)
        self._status.setText(message)
        self._log.append(message)
        self._recovery_button.setEnabled(_journal_path(self._runtime()).exists() if self._runtime() is not None else False)
        self._start_button.setEnabled(not self._recovery_button.isEnabled())
        if success:
            self.refresh_data()

    def closeEvent(self, event) -> None:  # noqa: ANN001
        if self._execution_thread is not None and self._execution_thread.isRunning():
            self._execution_thread.request_stop()
            self._status.setText("正在停止执行，请等待未成交委托撤销并完成状态核对后再关闭窗口。")
            event.ignore()
            return
        if self._refresh_thread is not None and self._refresh_thread.isRunning():
            event.ignore()
            return
        if self._recovery_thread is not None and self._recovery_thread.isRunning():
            event.ignore()
            return
        event.accept()
