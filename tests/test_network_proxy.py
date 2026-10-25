from __future__ import annotations

import base64
import asyncio
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch
from urllib import request
from urllib.error import URLError

import pytest

from okx_quant import network_proxy, network_transport, websockets_compat
from okx_quant.network_proxy import NetworkProxySettings


@pytest.fixture
def proxy_file(tmp_path, monkeypatch):
    path = tmp_path / "network_proxy.json"
    monkeypatch.setattr(network_proxy, "network_proxy_file_path", lambda: path)
    monkeypatch.setattr(network_proxy, "_runtime_settings", {})
    monkeypatch.setattr(network_transport, "_proxy_settings", None)
    old_opener = request._opener
    yield path
    request._opener = old_opener


@pytest.mark.parametrize("url", [
    "", "127.0.0.1:10808", "socks5://host:1080", "https://host:443", "http://host",
    "http://host:0", "http://host:65536", "http://host:bad", "http://host:80/path",
    "http://host:80?q=1", "http://host:80#fragment", "http://bad host:80",
    "http://user@host:80",
    "http://host%0a:80", "http://host:80\nHeader:bad", "http://host\\other:80",
])
def test_invalid_custom_proxy_is_rejected_without_changing_saved_settings(proxy_file, url):
    network_proxy.save_network_proxy_settings("direct", "")
    original = proxy_file.read_bytes()
    with pytest.raises(ValueError, match="HTTP 代理地址"):
        network_proxy.save_network_proxy_settings("custom", url)
    assert proxy_file.read_bytes() == original


def test_save_is_encrypted_and_does_not_switch_running_requests(proxy_file):
    assert network_proxy.runtime_network_proxy_settings() == NetworkProxySettings()
    url = "http://user:secret@127.0.0.1:3128"
    network_proxy.save_network_proxy_settings("custom", url)
    assert "secret" not in proxy_file.read_text(encoding="utf-8")
    assert network_proxy.load_network_proxy_settings() == NetworkProxySettings("custom", url)
    assert network_proxy.runtime_network_proxy_settings() == NetworkProxySettings()
    # A new process reads the saved mode for both transports.
    network_proxy._runtime_settings.clear()
    assert network_proxy.runtime_network_proxy_settings() == NetworkProxySettings("custom", url)


def test_corrupted_settings_block_requests_and_can_be_repaired_for_restart(proxy_file):
    proxy_file.write_text('{"mode":"custom","url":"dpapi:broken"}', encoding="utf-8")
    with pytest.raises(ValueError, match="代理设置读取失败"):
        network_transport.refresh_http_proxy_settings()
    network_proxy.save_network_proxy_settings("direct", "")
    with pytest.raises(ValueError, match="代理设置读取失败"):
        network_proxy.runtime_network_proxy_settings()
    network_proxy._runtime_settings.clear()
    assert network_proxy.runtime_network_proxy_settings().mode == "direct"


@pytest.mark.parametrize("mode, expected", [("direct", None), ("system", True), ("custom", "http://proxy:3128")])
def test_rest_and_websocket_resolve_the_same_mode(proxy_file, mode, expected):
    network_proxy.save_network_proxy_settings(mode, "http://proxy:3128")
    network_proxy._runtime_settings.clear()
    with (
        patch.object(request, "getproxies", return_value={"https": "http://system:7890"}) as system,
        patch.object(request, "build_opener") as build,
        patch.object(request, "install_opener"),
        patch.object(websockets_compat, "_modern_proxy_supported", True),
        patch.object(websockets_compat, "_modern_connect") as connect,
    ):
        network_transport.refresh_http_proxy_settings()
        handler = build.call_args.args[0]
        assert handler.proxies == (
            {} if mode == "direct" else {"https": "http://system:7890"} if mode == "system"
            else {"http": expected, "https": expected}
        )
        websockets_compat.connect_okx_websocket("wss://ws.okx.com/ws/v5/public", proxy="http://wrong:80")
        assert connect.call_args.kwargs["proxy"] == expected
        if mode != "system":
            system.assert_not_called()


def test_old_websocket_dependency_cannot_silently_bypass_custom_proxy(proxy_file):
    network_proxy.save_network_proxy_settings("custom", "http://proxy:3128")
    network_proxy._runtime_settings.clear()
    with (
        patch.object(websockets_compat, "_modern_proxy_supported", False),
        patch.object(websockets_compat, "_legacy_connect") as legacy,
        pytest.raises(RuntimeError, match="websockets >= 15"),
    ):
        websockets_compat.connect_okx_websocket("wss://ws.okx.com/ws/v5/public")
    legacy.assert_not_called()


@pytest.fixture
def local_http_server():
    received = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            received.append((self.command, self.path, self.headers.get("Proxy-Authorization")))
            body = b'{"ok":true}'
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_CONNECT(self):
            received.append((self.command, self.path, self.headers.get("Proxy-Authorization")))
            self.send_error(502, "Simulated unavailable upstream")

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"127.0.0.1:{server.server_port}", received
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_direct_ignores_system_proxy_and_custom_ignores_system_bypass(proxy_file, local_http_server):
    address, received = local_http_server
    network_proxy.save_network_proxy_settings("direct", "")
    network_proxy._runtime_settings.clear()
    with patch.object(request, "getproxies", return_value={"http": "http://127.0.0.1:1"}):
        network_transport.refresh_http_proxy_settings()
        with request.urlopen(f"http://{address}/direct", timeout=2) as response:
            assert json.load(response) == {"ok": True}
    assert received == [("GET", "/direct", None)]

    network_proxy.save_network_proxy_settings("custom", f"http://user:pass@{address}")
    network_proxy._runtime_settings.clear()
    with patch.object(request, "proxy_bypass", return_value=True):
        network_transport.refresh_http_proxy_settings()
        with request.urlopen("http://upstream.invalid/custom", timeout=2) as response:
            assert json.load(response) == {"ok": True}
        with pytest.raises(URLError, match="502"):
            request.urlopen("https://upstream.invalid/private", timeout=2)
    auth = "Basic " + base64.b64encode(b"user:pass").decode()
    assert received[1:] == [
        ("GET", "http://upstream.invalid/custom", auth),
        ("CONNECT", "upstream.invalid:443", auth),
    ]


@pytest.mark.parametrize("mode", ["direct", "custom"])
def test_websocket_uses_selected_route_despite_system_proxy_and_bypass(proxy_file, mode):
    from websockets.asyncio.server import serve

    async def scenario():
        forwarded = []

        async def endpoint(socket):
            await socket.send("ready")
            await socket.wait_closed()

        async def proxy(reader, writer):
            header = await reader.readuntil(b"\r\n\r\n")
            authority = header.split(b" ", 2)[1].decode()
            forwarded.append(authority)
            host, port = authority.rsplit(":", 1)
            upstream_reader, upstream_writer = await asyncio.open_connection(host, int(port))
            writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            await writer.drain()

            async def relay(source, destination):
                try:
                    while data := await source.read(8192):
                        destination.write(data)
                        await destination.drain()
                finally:
                    destination.close()

            await asyncio.gather(relay(reader, upstream_writer), relay(upstream_reader, writer))

        async with serve(endpoint, "127.0.0.1", 0) as server:
            target = f"127.0.0.1:{server.sockets[0].getsockname()[1]}"
            async with await asyncio.start_server(proxy, "127.0.0.1", 0) as gateway:
                network_proxy.save_network_proxy_settings(mode, f"http://127.0.0.1:{gateway.sockets[0].getsockname()[1]}")
                network_proxy._runtime_settings.clear()
                with (
                    patch.object(request, "getproxies", return_value={"ws": "http://127.0.0.1:1"}),
                    patch.object(request, "proxy_bypass", return_value=True),
                ):
                    async with websockets_compat.connect_okx_websocket(f"ws://{target}", open_timeout=2) as socket:
                        assert await asyncio.wait_for(socket.recv(), 2) == "ready"
            assert forwarded == ([target] if mode == "custom" else [])

    asyncio.run(scenario())


@pytest.mark.parametrize("mode", ["direct", "system", "custom"])
def test_qt_settings_save_proxy_independently_and_with_main_save(proxy_file, tmp_path, mode):
    pytest.importorskip("PySide6")
    from PySide6.QtWidgets import QApplication
    from okx_quant.persistence import load_notification_snapshot
    from roll_terminal_qt import account_settings_dialog as ui

    app = QApplication.instance() or QApplication([])
    notification = load_notification_snapshot(path=tmp_path / "absent.json")
    with (
        patch.object(ui, "load_credentials_profiles_snapshot", return_value={"selected_profile": "test", "profiles": {"test": {}}}),
        patch.object(ui, "load_notification_snapshot", return_value=notification),
        patch.object(ui, "save_credentials_profiles_snapshot"),
        patch.object(ui, "save_notification_snapshot"),
        patch.object(ui.QMessageBox, "information"),
        patch.object(ui.QMessageBox, "warning") as warning,
    ):
        dialog = ui.AccountSettingsDialog()
        try:
            assert dialog.proxy_mode.currentData() == "system"
            assert not dialog.proxy_url.isEnabled()
            ui.AccountSettingsDialog._set_combo(dialog.proxy_mode, mode)
            assert dialog.proxy_url.isEnabled() == (mode == "custom")
            if mode == "custom":
                dialog.proxy_url.setText("socks5://proxy:1080")
                dialog._save_proxy()
                warning.assert_called_once()
                assert not proxy_file.exists()
                dialog.proxy_url.setText("http://proxy:3128")
            assert dialog._save()
            assert network_proxy.load_network_proxy_settings() == NetworkProxySettings(mode, "http://proxy:3128" if mode == "custom" else "")
            # The independent button can also save without writing credentials.
            dialog._set_combo(dialog.proxy_mode, "direct")
            dialog._save_proxy()
            assert network_proxy.load_network_proxy_settings().mode == "direct"
        finally:
            dialog.close()
            dialog.deleteLater()
            app.processEvents()


def test_tk_proxy_settings_validate_and_save_without_changing_current_route(proxy_file):
    import tkinter as tk
    from tkinter import ttk
    from types import SimpleNamespace
    from okx_quant import ui_shell as ui

    root = tk.Tk()
    root.withdraw()
    def hidden_window(parent):
        window = tk.Toplevel(parent)
        window.withdraw()
        return window

    def descendants(widget):
        for child in widget.winfo_children():
            yield child
            yield from descendants(child)

    try:
        with (
            patch.object(ui, "Toplevel", side_effect=hidden_window),
            patch.object(ui.messagebox, "showinfo"),
            patch.object(ui.messagebox, "showerror") as error,
        ):
            ui.QuantApp.open_network_proxy_settings_window(SimpleNamespace(root=root, _settings_window=None))
            widgets = list(descendants(root))
            combo = next(item for item in widgets if isinstance(item, ttk.Combobox))
            entry = next(item for item in widgets if isinstance(item, ttk.Entry) and not isinstance(item, ttk.Combobox))
            save = next(item for item in widgets if isinstance(item, ttk.Button) and item.cget("text") == "保存代理设置")
            assert combo.get() == "跟随系统代理"
            assert str(entry.cget("state")) == "disabled"
            combo.set("自定义 HTTP 代理地址")
            combo.event_generate("<<ComboboxSelected>>")
            entry.insert(0, "invalid")
            save.invoke()
            error.assert_called_once()
            assert not proxy_file.exists()
            entry.delete(0, tk.END)
            entry.insert(0, "http://proxy:3128")
            save.invoke()
            assert network_proxy.load_network_proxy_settings() == NetworkProxySettings("custom", "http://proxy:3128")
            assert network_proxy.runtime_network_proxy_settings() == NetworkProxySettings()
    finally:
        root.destroy()
