"""Stdlib-only protocol shared with the standalone market relay."""
from __future__ import annotations

import json
import re
from urllib import parse, request

MAX_RESPONSE_BYTES = 8_000_000
PREFIX = "/v1/market"
TOKEN_HEADER = "X-Market-Relay-Token"
REQUEST_HEADER = "X-Market-Relay-Request"
_CANDLES = {"instId", "bar", "after", "before", "limit"}
RULES = {
    "/api/v5/market/ticker": ({"instId"}, {"instId"}),
    "/api/v5/market/tickers": ({"instType", "uly", "instFamily"}, {"instType"}),
    "/api/v5/market/books": ({"instId", "sz"}, {"instId"}),
    **{f"/api/v5/market/{name}": (_CANDLES, {"instId", "bar"}) for name in (
        "candles", "history-candles", "mark-price-candles", "history-mark-price-candles",
        "index-candles", "history-index-candles",
    )},
    "/api/v5/public/mark-price": ({"instType", "instId", "uly", "instFamily"}, {"instType"}),
    "/api/v5/public/instruments": ({"instType", "instId", "uly", "instFamily"}, {"instType"}),
    "/api/v5/public/price-limit": ({"instId"}, {"instId"}),
    "/api/v5/public/instrument-tick-bands": ({"instType", "instFamily"}, {"instType"}),
}
_BARS = {"1s", "1m", "3m", "5m", "15m", "30m", "1H", "2H", "4H", "6H", "12H",
         "1D", "2D", "3D", "1W", "1M", "3M", "6M", "1Y"}
_BARS |= {bar + "utc" for bar in _BARS if bar[-1] in "HDWMY"}


class NoRedirect(request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


def valid_token(token: str) -> bool:
    return isinstance(token, str) and re.fullmatch(r"[A-Za-z0-9_-]{32,128}", token) is not None


def validate(path: str, query: str) -> dict[str, str]:
    if path not in RULES or len(query) > 2048:
        raise ValueError("forbidden request")
    allowed, required = RULES[path]
    pairs = parse.parse_qsl(query, keep_blank_values=True, strict_parsing=True, max_num_fields=12)
    params = dict(pairs)
    if len(params) != len(pairs) or not required.issubset(params):
        raise ValueError("missing or duplicate parameters")
    for key, value in pairs:
        if key not in allowed or not value or len(value) > 128:
            raise ValueError("invalid parameters")
        if key in {"instId", "uly", "instFamily"} and not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9.\-]{0,79}", value):
            raise ValueError("invalid instrument")
        if key in {"limit", "sz", "after", "before"}:
            if not re.fullmatch(r"[0-9]{1,16}", value):
                raise ValueError("invalid numeric parameter")
            cap = 400 if key == "sz" else 300
            if "index" in path or "history-mark" in path:
                cap = 100
            if key in {"limit", "sz"} and not 1 <= int(value) <= cap:
                raise ValueError("numeric parameter out of range")
    if "bar" in params and params["bar"] not in _BARS:
        raise ValueError("invalid bar")
    if "instType" in params and params["instType"] not in {"SPOT", "MARGIN", "SWAP", "FUTURES", "OPTION"}:
        raise ValueError("invalid instrument type")
    return params


def upstream(path: str, query: str, timeout: float = 5.0) -> dict:
    validate(path, query)
    # Fixed destination: no arbitrary URLs, account headers or local proxy settings.
    opener = request.build_opener(request.ProxyHandler({}), NoRedirect())
    target = "https://www.okx.com" + path + ("?" + query if query else "")
    req = request.Request(target, headers={"Accept": "application/json", "User-Agent": "QQOKX-Market-Relay/1"})
    with opener.open(req, timeout=timeout) as response:
        raw = response.read(MAX_RESPONSE_BYTES + 1)
    if len(raw) > MAX_RESPONSE_BYTES:
        raise ValueError("response too large")
    payload = json.loads(raw)
    if not isinstance(payload, dict) or not isinstance(payload.get("data"), list) or not isinstance(payload.get("code"), str):
        raise ValueError("invalid upstream reply")
    # Business errors stay business errors for the existing OKX error parser.
    return payload
