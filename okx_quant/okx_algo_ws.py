from __future__ import annotations

import asyncio
import json
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable

from okx_quant.models import Credentials

try:
    import websockets
except Exception:  # noqa: BLE001
    websockets = None


Logger = Callable[[str], None]
UpdateListener = Callable[[str, int], None]


def _ws_timestamp_seconds() -> str:
    return str(int(time.time()))


def _sign_ws_login(timestamp: str, secret_key: str) -> str:
    from okx_quant.okx_client import _sign_request

    return _sign_request(timestamp, "GET", "/users/self/verify", "", secret_key)


def _algo_order_key(item: dict[str, Any]) -> tuple[str, str, str] | None:
    algo_id = str(item.get("algoId") or "").strip()
    algo_cl_ord_id = str(item.get("algoClOrdId") or "").strip()
    ord_id = str(item.get("ordId") or item.get("actualOrdId") or "").strip()
    if not algo_id and not algo_cl_ord_id and not ord_id:
        return None
    return algo_id, algo_cl_ord_id, ord_id


@dataclass(frozen=True)
class OkxAlgoWsRecord:
    version: int
    payload: dict[str, Any]
    received_at: float


class OkxAlgoWsConnectionUnavailable(RuntimeError):
    pass


class OkxAlgoWsConnection:
    _BUSINESS_URL = "wss://ws.okx.com:8443/ws/v5/business"
    _DEMO_BUSINESS_URL = "wss://wspap.okx.com:8443/ws/v5/business"

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
        self._orders_by_key: dict[tuple[str, str, str], OkxAlgoWsRecord] = {}
        self._update_listeners: set[UpdateListener] = set()

    def debug_status(self) -> dict[str, Any]:
        with self._lock:
            return {
                "connected": self._connected,
                "last_error": self._last_error,
                "version": self._version,
                "order_count": len(self._orders_by_key),
                "environment": self._environment,
            }

    def start(self) -> None:
        if websockets is None:
            raise OkxAlgoWsConnectionUnavailable("websockets dependency is unavailable")
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stop_event.clear()
            self._thread = threading.Thread(
                target=self._run_forever,
                daemon=True,
                name=f"okx-algo-ws-{self._credentials.profile_name or 'default'}-{self._environment}",
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

    def get_latest_orders(self, *, limit: int = 80) -> tuple[int, tuple[dict[str, Any], ...]] | None:
        with self._lock:
            if not self._orders_by_key:
                return None
            records = sorted(self._orders_by_key.values(), key=lambda record: record.received_at, reverse=True)
            capped = records[: max(1, limit)]
            return max(record.version for record in capped), tuple(dict(record.payload) for record in capped)

    def add_update_listener(self, listener: UpdateListener) -> Callable[[], None]:
        with self._lock:
            self._update_listeners.add(listener)

        def unsubscribe() -> None:
            with self._lock:
                self._update_listeners.discard(listener)

        return unsubscribe

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
                self._log_error_once(f"OKX algo WS disconnected; retrying: {self._last_error}")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 15.0)

    async def _run_connection_once(self) -> None:
        assert websockets is not None
        url = self._DEMO_BUSINESS_URL if self._environment == "demo" else self._BUSINESS_URL
        headers: dict[str, str] = {"x-simulated-trading": "1"} if self._environment == "demo" else {}
        connect_kwargs: dict[str, Any] = {
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

    async def _login(self, socket: Any) -> None:
        timestamp = _ws_timestamp_seconds()
        payload = {
            "op": "login",
            "args": [{
                "apiKey": self._credentials.api_key,
                "passphrase": self._credentials.passphrase,
                "timestamp": timestamp,
                "sign": _sign_ws_login(timestamp, self._credentials.secret_key),
            }],
        }
        await socket.send(json.dumps(payload, separators=(",", ":")))
        await self._expect_event(socket, "login")

    async def _subscribe(self, socket: Any) -> None:
        payload = {"op": "subscribe", "args": [{"channel": "orders-algo", "instType": "ANY"}]}
        await socket.send(json.dumps(payload, separators=(",", ":")))
        await self._expect_event(socket, "subscribe")

    async def _expect_event(self, socket: Any, expected: str) -> None:
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
                raise OkxAlgoWsConnectionUnavailable(str(payload.get("msg") or payload))
            if event == expected:
                return
            await self._handle_payload(payload)
        raise OkxAlgoWsConnectionUnavailable(f"OKX algo WS did not return {expected} before timeout")

    async def _handle_message(self, socket: Any, message: str) -> None:
        if message == "ping":
            await socket.send("pong")
            return
        await self._handle_payload(json.loads(message))

    async def _handle_payload(self, payload: dict[str, Any]) -> None:
        event = str(payload.get("event") or "").strip().lower()
        if event == "error":
            raise OkxAlgoWsConnectionUnavailable(str(payload.get("msg") or payload))
        arg = payload.get("arg")
        data = payload.get("data")
        if not isinstance(arg, dict) or not isinstance(data, list):
            return
        if str(arg.get("channel") or "").strip().lower() == "orders-algo":
            self._store_orders(data)

    def _store_orders(self, items: list[dict[str, Any]]) -> None:
        changed = False
        with self._lock:
            for item in items:
                if not isinstance(item, dict):
                    continue
                key = _algo_order_key(item)
                if key is None:
                    continue
                changed = True
                self._version += 1
                self._orders_by_key[key] = OkxAlgoWsRecord(
                    version=self._version,
                    payload=dict(item),
                    received_at=time.time(),
                )
            update_version = self._version
            self._lock.notify_all()
        if changed:
            self._notify_update("orders-algo", update_version)

    def _notify_update(self, channel: str, version: int) -> None:
        with self._lock:
            listeners = tuple(self._update_listeners)
        for listener in listeners:
            try:
                listener(channel, version)
            except Exception:  # noqa: BLE001
                continue

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
