"""Standalone stdlib market relay; runs beside old servers without importing their code."""
from __future__ import annotations

import argparse
from collections import deque
import hmac
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import re
import socket
import ssl
import threading
import time
from urllib.parse import urlsplit

if __package__:
    from . import market_relay_protocol as protocol
else:
    import market_relay_protocol as protocol


class Server(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address, token: str, *, tls_context=None, max_connections=16, requests_per_minute=240):
        if not protocol.valid_token(token):
            raise ValueError("Use a random token with 32–128 URL-safe characters.")
        self.token = token
        self.tls_context = tls_context
        self.guard = threading.Lock()
        self.times = deque()
        self.requests_per_minute = requests_per_minute
        self.slots = threading.BoundedSemaphore(max_connections)
        self.upstream = protocol.upstream
        if ":" in address[0]:
            self.address_family = socket.AF_INET6
        super().__init__(address, Handler)

    def process_request(self, connection, address):
        # Bound worker creation, including unauthenticated and TLS connections.
        if not self.slots.acquire(blocking=False):
            self.shutdown_request(connection)
            return
        try:
            super().process_request(connection, address)
        except Exception:
            self.slots.release()
            raise

    def process_request_thread(self, connection, address):
        try:
            connection.settimeout(6.0)
            if self.tls_context is not None:
                try:
                    connection = self.tls_context.wrap_socket(connection, server_side=True)
                except (OSError, ssl.SSLError):
                    self.shutdown_request(connection)
                    return
            super().process_request_thread(connection, address)
        finally:
            self.slots.release()


class Handler(BaseHTTPRequestHandler):
    server_version = "QQOKXMarketRelay/1"
    sys_version = ""

    def log_message(self, *args):
        pass

    def do_GET(self):
        supplied = self.headers.get(protocol.TOKEN_HEADER, "")
        if not protocol.valid_token(supplied) or not hmac.compare_digest(supplied, self.server.token):
            self.reply(401, {"error": "unauthorized"})
            return
        if len(self.path) > 2200 or self.headers.get("Content-Length", "0") != "0" or self.headers.get("Transfer-Encoding"):
            self.reply(400, {"error": "invalid request"})
            return
        request_id = self.headers.get(protocol.REQUEST_HEADER, "")
        if not re.fullmatch(r"[a-f0-9]{32}", request_id):
            self.reply(400, {"error": "invalid request ID"})
            return
        try:
            parsed = urlsplit(self.path)
            path = parsed.path.removeprefix(protocol.PREFIX)
            if parsed.scheme or parsed.netloc or parsed.fragment or not parsed.path.startswith(protocol.PREFIX + "/"):
                raise ValueError("invalid target")
            protocol.validate(path, parsed.query)
        except ValueError:
            self.reply(400, {"error": "forbidden endpoint or parameters"})
            return
        with self.server.guard:
            now = time.monotonic()
            while self.server.times and now - self.server.times[0] >= 60:
                self.server.times.popleft()
            limited = len(self.server.times) >= self.server.requests_per_minute
            if not limited:
                self.server.times.append(now)
        if limited:
            self.reply(429, {"error": "rate limited"})
            return
        try:
            payload = self.server.upstream(path, parsed.query, timeout=5.0)
            self.reply(200, {"protocol": 1, "request_id": request_id,
                             "target": path + ("?" + parsed.query if parsed.query else ""),
                             "served_at": time.time(), "payload": payload})
        except Exception:
            self.reply(502, {"error": "upstream unavailable"})

    def reply(self, status, payload):
        data = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        try:
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(data)
        except OSError:
            pass


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--cert")
    parser.add_argument("--key")
    args = parser.parse_args()
    token = os.environ.get("MARKET_RELAY_TOKEN", "")
    if not protocol.valid_token(token):
        parser.error("Set MARKET_RELAY_TOKEN to a random token with 32–128 URL-safe characters.")
    if bool(args.cert) != bool(args.key):
        parser.error("Provide both --cert and --key.")
    if not args.cert and args.host not in {"127.0.0.1", "::1", "localhost"}:
        parser.error("Public binding requires TLS; use --cert/--key or a TLS reverse proxy to loopback.")
    context = None
    if args.cert:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        context.load_cert_chain(args.cert, args.key)
    with Server((args.host, args.port), token, tls_context=context) as server:
        print(f"Market relay listening on {args.host}:{args.port}")
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
