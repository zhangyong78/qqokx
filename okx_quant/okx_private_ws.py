from __future__ import annotations

import asyncio
import json
import time
import threading
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Callable

from okx_quant.models import Credentials

try:
    import websockets
except Exception:  # noqa: BLE001
    websockets = None


Logger = Callable[[str], None]


def _position_snapshot_key(item: dict[str, Any]) -> tuple[str, str, str] | None:
    inst_id = str(item.get("instId") or "").strip().upper()
    if not inst_id:
        return None
    pos_side = str(item.get("posSide") or "").strip().lower() or "net"
    mgn_mode = str(item.get("mgnMode") or "").strip().lower()
    return inst_id, pos_side, mgn_mode


def _ws_position_has_nonzero_size(item: dict[str, Any]) -> bool:
    try:
        position = Decimal(str(item.get("pos") or "0").strip() or "0")
    except (InvalidOperation, ValueError):
        return False
    return position != 0


def _ws_timestamp_seconds() -> str:
    return str(int(time.time()))


def _sign_ws_login(timestamp: str, secret_key: str) -> str:
    from okx_quant.okx_client import _sign_request

    return _sign_request(timestamp, "GET", "/users/self/verify", "", secret_key)


@dataclass(frozen=True)
class OkxPrivateWsRecord:
    version: int
    payload: dict[str, Any]
    received_at: float


class OkxPrivateWsConnectionUnavailable(RuntimeError):
    pass


class OkxPrivateWsConnection:
    _PRIVATE_URL = "wss://ws.okx.com:8443/ws/v5/private"
    _DEMO_PRIVATE_URL = "wss://wspap.okx.com:8443/ws/v5/private"

    def __init__(
        self,
        credentials: Credentials,
        *,
        environment: str,
        logger: Logger | None = None,
    ) -> None:
        self._credentials = credentials
        self._environment = environment
        self._logger = logger or (lambda _message: None)
        self._lock = threading.Condition()
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._socket = None
        self._connected = False
        self._last_error = ""
        self._last_error_logged = ""
        self._version = 0
        self._order_by_ord_id: dict[str, OkxPrivateWsRecord] = {}
        self._order_by_cl_ord_id: dict[str, OkxPrivateWsRecord] = {}
        self._positions_snapshot: OkxPrivateWsRecord | None = None
        self._account_snapshot: OkxPrivateWsRecord | None = None

    @property
    def last_error(self) -> str:
        with self._lock:
            return self._last_error

    def debug_status(self) -> dict[str, Any]:
        with self._lock:
            positions_version = self._positions_snapshot.version if self._positions_snapshot is not None else 0
            positions_received_at = self._positions_snapshot.received_at if self._positions_snapshot is not None else None
            account_version = self._account_snapshot.version if self._account_snapshot is not None else 0
            account_received_at = self._account_snapshot.received_at if self._account_snapshot is not None else None
            return {
                "connected": self._connected,
                "last_error": self._last_error,
                "version": self._version,
                "positions_version": positions_version,
                "positions_received_at": positions_received_at,
                "account_version": account_version,
                "account_received_at": account_received_at,
                "environment": self._environment,
            }

    def start(self) -> None:
        if websockets is None:
            raise OkxPrivateWsConnectionUnavailable("websockets 依赖不可用，无法启用 OKX 私有 WS。")
        with self._lock:
            thread = self._thread
            if thread is not None and thread.is_alive():
                return
            self._stop_event.clear()
            self._thread = threading.Thread(
                target=self._run_forever,
                daemon=True,
                name=f"okx-private-ws-{self._credentials.profile_name or 'default'}-{self._environment}",
            )
            self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        loop = self._loop
        socket = self._socket
        if loop is not None and socket is not None:
            try:
                asyncio.run_coroutine_threadsafe(socket.close(), loop).result(timeout=2.0)
            except Exception:  # noqa: BLE001
                pass
        with self._lock:
            thread = self._thread
        if thread is not None:
            thread.join(timeout=3.0)

    def get_latest_order(
        self,
        *,
        ord_id: str | None = None,
        cl_ord_id: str | None = None,
    ) -> tuple[int, dict[str, Any]] | None:
        with self._lock:
            record = self._resolve_order_record_locked(ord_id=ord_id, cl_ord_id=cl_ord_id)
            if record is None:
                return None
            return record.version, dict(record.payload)

    def get_latest_orders(self, *, limit: int = 50) -> tuple[int, tuple[dict[str, Any], ...]] | None:
        with self._lock:
            records_by_key: dict[tuple[str, str], OkxPrivateWsRecord] = {}
            for record in self._order_by_ord_id.values():
                ord_id = str(record.payload.get("ordId") or "").strip()
                cl_ord_id = str(record.payload.get("clOrdId") or "").strip()
                records_by_key[(ord_id, cl_ord_id)] = record
            for record in self._order_by_cl_ord_id.values():
                ord_id = str(record.payload.get("ordId") or "").strip()
                cl_ord_id = str(record.payload.get("clOrdId") or "").strip()
                records_by_key[(ord_id, cl_ord_id)] = record
            if not records_by_key:
                return None
            records = sorted(records_by_key.values(), key=lambda item: item.received_at, reverse=True)
            capped = records[: max(1, limit)]
            version = max(item.version for item in capped)
            return version, tuple(dict(item.payload) for item in capped)

    def wait_for_order_update(
        self,
        *,
        ord_id: str | None = None,
        cl_ord_id: str | None = None,
        after_version: int = 0,
        timeout: float = 1.0,
    ) -> tuple[int, dict[str, Any]] | None:
        deadline = time.monotonic() + max(timeout, 0.0)
        with self._lock:
            while True:
                record = self._resolve_order_record_locked(ord_id=ord_id, cl_ord_id=cl_ord_id)
                if record is not None and record.version > after_version:
                    return record.version, dict(record.payload)
                remaining = deadline - time.monotonic()
                if remaining <= 0 or self._stop_event.is_set():
                    return None
                self._lock.wait(timeout=remaining)

    def get_latest_positions(self) -> tuple[int, tuple[dict[str, Any], ...]] | None:
        with self._lock:
            record = self._positions_snapshot
            if record is None:
                return None
            return record.version, tuple(dict(item) for item in record.payload.get("data", ()))

    def get_latest_account(self) -> tuple[int, tuple[dict[str, Any], ...]] | None:
        with self._lock:
            record = self._account_snapshot
            if record is None:
                return None
            return record.version, tuple(dict(item) for item in record.payload.get("data", ()))

    def _resolve_order_record_locked(
        self,
        *,
        ord_id: str | None,
        cl_ord_id: str | None,
    ) -> OkxPrivateWsRecord | None:
        if ord_id:
            record = self._order_by_ord_id.get(ord_id)
            if record is not None:
                return record
        if cl_ord_id:
            return self._order_by_cl_ord_id.get(cl_ord_id)
        return None

    def _run_forever(self) -> None:
        try:
            asyncio.run(self._run_forever_async())
        finally:
            with self._lock:
                self._connected = False
                self._loop = None
                self._socket = None
                self._lock.notify_all()

    async def _run_forever_async(self) -> None:
        self._loop = asyncio.get_running_loop()
        reconnect_delay = 1.0
        while not self._stop_event.is_set():
            try:
                await self._run_connection_once()
                reconnect_delay = 1.0
            except Exception as exc:  # noqa: BLE001
                self._set_last_error(str(exc).strip() or exc.__class__.__name__)
                self._log_error_once(f"OKX 私有 WS 断开，准备重连：{self._last_error}")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 15.0)

    async def _run_connection_once(self) -> None:
        assert websockets is not None
        url = self._DEMO_PRIVATE_URL if self._environment == "demo" else self._PRIVATE_URL
        headers: dict[str, str] = {}
        if self._environment == "demo":
            headers["x-simulated-trading"] = "1"
        connect_kwargs = {
            "ping_interval": 20,
            "ping_timeout": 20,
            "open_timeout": 20,
            "close_timeout": 5,
            "max_queue": 1000,
        }
        if headers:
            connect_kwargs["additional_headers"] = headers
        try:
            socket_context = websockets.connect(url, **connect_kwargs)
        except TypeError:
            if headers:
                connect_kwargs.pop("additional_headers", None)
                connect_kwargs["extra_headers"] = headers
            socket_context = websockets.connect(url, **connect_kwargs)
        async with socket_context as socket:
            with self._lock:
                self._socket = socket
                self._connected = True
                self._last_error = ""
                self._last_error_logged = ""
                self._lock.notify_all()
            await self._login(socket)
            await self._subscribe(socket)
            while not self._stop_event.is_set():
                message = await socket.recv()
                if isinstance(message, bytes):
                    message = message.decode("utf-8", errors="replace")
                await self._handle_message(socket, message)
        with self._lock:
            self._connected = False
            self._socket = None

    async def _login(self, socket) -> None:  # noqa: ANN001
        timestamp = _ws_timestamp_seconds()
        payload = {
            "op": "login",
            "args": [
                {
                    "apiKey": self._credentials.api_key,
                    "passphrase": self._credentials.passphrase,
                    "timestamp": timestamp,
                    "sign": _sign_ws_login(timestamp, self._credentials.secret_key),
                }
            ],
        }
        await socket.send(json.dumps(payload, separators=(",", ":")))
        await self._expect_event(socket, "login")

    async def _subscribe(self, socket) -> None:  # noqa: ANN001
        payload = {
            "op": "subscribe",
            "args": [
                {"channel": "account"},
                {"channel": "positions", "instType": "ANY"},
                {"channel": "orders", "instType": "ANY"},
            ],
        }
        await socket.send(json.dumps(payload, separators=(",", ":")))
        await self._expect_event(socket, "subscribe")

    async def _expect_event(self, socket, expected: str) -> None:  # noqa: ANN001
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            message = await socket.recv()
            if isinstance(message, bytes):
                message = message.decode("utf-8", errors="replace")
            if message == "ping":
                await socket.send("pong")
                continue
            payload = json.loads(message)
            event = str(payload.get("event") or "").strip().lower()
            if event == "error":
                raise OkxPrivateWsConnectionUnavailable(str(payload.get("msg") or payload))
            if event == expected:
                return
            await self._handle_payload(payload)
        raise OkxPrivateWsConnectionUnavailable(f"OKX 私有 WS 未在超时内返回 {expected} 事件。")

    async def _handle_message(self, socket, message: str) -> None:  # noqa: ANN001
        if message == "ping":
            await socket.send("pong")
            return
        payload = json.loads(message)
        await self._handle_payload(payload)

    async def _handle_payload(self, payload: dict[str, Any]) -> None:
        event = str(payload.get("event") or "").strip().lower()
        if event == "error":
            raise OkxPrivateWsConnectionUnavailable(str(payload.get("msg") or payload))
        arg = payload.get("arg")
        if not isinstance(arg, dict):
            return
        channel = str(arg.get("channel") or "").strip().lower()
        data = payload.get("data")
        if not isinstance(data, list):
            return
        if channel == "orders":
            self._store_orders(data)
        elif channel == "positions":
            self._store_positions(data)
        elif channel == "account":
            self._store_account(data)

    def _store_orders(self, items: list[dict[str, Any]]) -> None:
        with self._lock:
            for item in items:
                if not isinstance(item, dict):
                    continue
                ord_id = str(item.get("ordId") or "").strip()
                cl_ord_id = str(item.get("clOrdId") or "").strip()
                if not ord_id and not cl_ord_id:
                    continue
                self._version += 1
                record = OkxPrivateWsRecord(version=self._version, payload=dict(item), received_at=time.time())
                if ord_id:
                    self._order_by_ord_id[ord_id] = record
                if cl_ord_id:
                    self._order_by_cl_ord_id[cl_ord_id] = record
            self._lock.notify_all()

    def _store_positions(self, items: list[dict[str, Any]]) -> None:
        with self._lock:
            merged_by_key: dict[tuple[str, str, str], dict[str, Any]] = {}
            if self._positions_snapshot is not None:
                existing_items = self._positions_snapshot.payload.get("data", ())
                for existing in existing_items:
                    if not isinstance(existing, dict):
                        continue
                    key = _position_snapshot_key(existing)
                    if key is None:
                        continue
                    merged_by_key[key] = dict(existing)
            for item in items:
                if not isinstance(item, dict):
                    continue
                key = _position_snapshot_key(item)
                if key is None:
                    continue
                if _ws_position_has_nonzero_size(item):
                    merged_by_key[key] = dict(item)
                else:
                    merged_by_key.pop(key, None)
            self._version += 1
            self._positions_snapshot = OkxPrivateWsRecord(
                version=self._version,
                payload={
                    "data": [
                        merged_by_key[key]
                        for key in sorted(merged_by_key, key=lambda item: (item[0], item[1], item[2]))
                    ]
                },
                received_at=time.time(),
            )
            self._lock.notify_all()

    def _store_account(self, items: list[dict[str, Any]]) -> None:
        with self._lock:
            self._version += 1
            self._account_snapshot = OkxPrivateWsRecord(
                version=self._version,
                payload={"data": [dict(item) for item in items if isinstance(item, dict)]},
                received_at=time.time(),
            )
            self._lock.notify_all()

    def _set_last_error(self, message: str) -> None:
        with self._lock:
            self._connected = False
            self._last_error = message
            self._lock.notify_all()

    def _log_error_once(self, message: str) -> None:
        with self._lock:
            if message == self._last_error_logged:
                return
            self._last_error_logged = message
        self._logger(message)
