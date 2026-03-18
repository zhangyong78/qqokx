from __future__ import annotations

import base64
import hashlib
import hmac
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
from urllib import error, parse, request

from okx_quant.models import Candle, Credentials, Instrument, OrderPlan, StrategyConfig
from okx_quant.pricing import format_decimal


DEFAULT_HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/134.0 Safari/537.36"
    ),
}


class OkxApiError(RuntimeError):
    def __init__(self, message: str, *, code: str | None = None, status: int | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.status = status


@dataclass
class OkxOrderResult:
    ord_id: str
    cl_ord_id: str | None
    s_code: str
    s_msg: str
    raw: dict[str, Any]


@dataclass(frozen=True)
class OkxOrderStatus:
    ord_id: str
    state: str
    side: str | None
    ord_type: str | None
    price: Decimal | None
    size: Decimal | None
    filled_size: Decimal | None
    raw: dict[str, Any]


class OkxRestClient:
    base_url = "https://www.okx.com"

    def get_swap_instruments(self) -> list[Instrument]:
        payload = self._request("GET", "/api/v5/public/instruments", params={"instType": "SWAP"})
        instruments: list[Instrument] = []
        for item in payload["data"]:
            instruments.append(
                Instrument(
                    inst_id=item["instId"],
                    tick_size=Decimal(item["tickSz"]),
                    lot_size=Decimal(item["lotSz"]),
                    min_size=Decimal(item["minSz"]),
                    state=item.get("state", ""),
                    settle_ccy=item.get("settleCcy"),
                    ct_val=Decimal(item["ctVal"]) if item.get("ctVal") else None,
                    ct_val_ccy=item.get("ctValCcy"),
                )
            )
        instruments.sort(key=lambda item: item.inst_id)
        return instruments

    def get_instrument(self, inst_id: str) -> Instrument:
        for instrument in self.get_swap_instruments():
            if instrument.inst_id == inst_id:
                return instrument
        raise OkxApiError(f"未找到可交易合约：{inst_id}")

    def get_candles(self, inst_id: str, bar: str, limit: int = 200) -> list[Candle]:
        payload = self._request(
            "GET",
            "/api/v5/market/candles",
            params={"instId": inst_id, "bar": bar, "limit": str(limit)},
        )
        candles: list[Candle] = []
        for row in payload["data"]:
            candles.append(
                Candle(
                    ts=int(row[0]),
                    open=Decimal(row[1]),
                    high=Decimal(row[2]),
                    low=Decimal(row[3]),
                    close=Decimal(row[4]),
                    volume=Decimal(row[5]),
                    confirmed=row[8] == "1",
                )
            )
        candles.sort(key=lambda candle: candle.ts)
        return candles

    def place_market_order(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        plan: OrderPlan,
    ) -> OkxOrderResult:
        order: dict[str, Any] = {
            "instId": plan.inst_id,
            "tdMode": config.trade_mode,
            "side": plan.side,
            "ordType": "market",
            "sz": format_decimal(plan.size),
            "attachAlgoOrds": [
                {
                    "tpTriggerPx": format_decimal(plan.take_profit),
                    "tpOrdPx": "-1",
                    "tpTriggerPxType": config.tp_sl_trigger_type,
                    "slTriggerPx": format_decimal(plan.stop_loss),
                    "slOrdPx": "-1",
                    "slTriggerPxType": config.tp_sl_trigger_type,
                }
            ],
        }
        if plan.pos_side:
            order["posSide"] = plan.pos_side

        payload = self._request(
            "POST",
            "/api/v5/trade/order",
            body=order,
            auth=True,
            credentials=credentials,
            simulated=config.environment == "demo",
        )
        return self._parse_order_result(payload, empty_message="OKX 返回了空的市价下单结果")

    def place_limit_order(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        plan: OrderPlan,
    ) -> OkxOrderResult:
        order: dict[str, Any] = {
            "instId": plan.inst_id,
            "tdMode": config.trade_mode,
            "side": plan.side,
            "ordType": "limit",
            "px": format_decimal(plan.entry_reference),
            "sz": format_decimal(plan.size),
            "attachAlgoOrds": [
                {
                    "tpTriggerPx": format_decimal(plan.take_profit),
                    "tpOrdPx": "-1",
                    "tpTriggerPxType": config.tp_sl_trigger_type,
                    "slTriggerPx": format_decimal(plan.stop_loss),
                    "slOrdPx": "-1",
                    "slTriggerPxType": config.tp_sl_trigger_type,
                }
            ],
        }
        if plan.pos_side:
            order["posSide"] = plan.pos_side

        payload = self._request(
            "POST",
            "/api/v5/trade/order",
            body=order,
            auth=True,
            credentials=credentials,
            simulated=config.environment == "demo",
        )
        return self._parse_order_result(payload, empty_message="OKX 返回了空的限价下单结果")

    def get_order(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        *,
        inst_id: str,
        ord_id: str,
    ) -> OkxOrderStatus:
        payload = self._request(
            "GET",
            "/api/v5/trade/order",
            params={"instId": inst_id, "ordId": ord_id},
            auth=True,
            credentials=credentials,
            simulated=config.environment == "demo",
        )
        if not payload["data"]:
            raise OkxApiError(f"OKX 未返回订单状态：{ord_id}")

        first = payload["data"][0]
        return OkxOrderStatus(
            ord_id=first.get("ordId", ord_id),
            state=first.get("state", ""),
            side=first.get("side"),
            ord_type=first.get("ordType"),
            price=_to_decimal(first.get("px")),
            size=_to_decimal(first.get("sz")),
            filled_size=_to_decimal(first.get("accFillSz")),
            raw=first,
        )

    def cancel_order(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        *,
        inst_id: str,
        ord_id: str,
    ) -> OkxOrderResult:
        payload = self._request(
            "POST",
            "/api/v5/trade/cancel-order",
            body={"instId": inst_id, "ordId": ord_id},
            auth=True,
            credentials=credentials,
            simulated=config.environment == "demo",
        )
        return self._parse_order_result(payload, empty_message="OKX 返回了空的撤单结果")

    def _parse_order_result(self, payload: dict[str, Any], *, empty_message: str) -> OkxOrderResult:
        if not payload["data"]:
            raise OkxApiError(empty_message)

        first = payload["data"][0]
        if first.get("sCode") not in {None, "", "0"}:
            raise OkxApiError(first.get("sMsg", "OKX 订单请求被拒绝"), code=first.get("sCode"))

        return OkxOrderResult(
            ord_id=first.get("ordId", ""),
            cl_ord_id=first.get("clOrdId"),
            s_code=first.get("sCode", "0"),
            s_msg=first.get("sMsg", ""),
            raw=payload,
        )

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, str] | None = None,
        body: dict[str, Any] | None = None,
        auth: bool = False,
        credentials: Credentials | None = None,
        simulated: bool = False,
    ) -> dict[str, Any]:
        if params:
            query_string = parse.urlencode(params)
            path = f"{path}?{query_string}"

        data = b""
        body_text = ""
        if body is not None:
            body_text = json.dumps(body, separators=(",", ":"))
            data = body_text.encode("utf-8")

        headers = dict(DEFAULT_HEADERS)
        if auth:
            if credentials is None:
                raise ValueError("鉴权请求缺少 API 凭证")
            timestamp = _okx_timestamp()
            signature = _sign_request(timestamp, method, path, body_text, credentials.secret_key)
            headers.update(
                {
                    "OK-ACCESS-KEY": credentials.api_key,
                    "OK-ACCESS-SIGN": signature,
                    "OK-ACCESS-TIMESTAMP": timestamp,
                    "OK-ACCESS-PASSPHRASE": credentials.passphrase,
                }
            )

        if simulated:
            headers["x-simulated-trading"] = "1"

        url = f"{self.base_url}{path}"
        req = request.Request(url, data=data or None, headers=headers, method=method.upper())

        try:
            with request.urlopen(req, timeout=20) as response:
                content = response.read().decode("utf-8")
                payload = json.loads(content)
        except error.HTTPError as exc:
            body_text = exc.read().decode("utf-8", errors="replace")
            raise OkxApiError(f"HTTP {exc.code}: {body_text}", status=exc.code) from exc
        except error.URLError as exc:
            raise OkxApiError(f"网络错误：{exc.reason}") from exc

        if payload.get("code") not in {None, "0"}:
            raise OkxApiError(payload.get("msg", "OKX API 错误"), code=payload.get("code"))
        return payload


def _to_decimal(value: Any) -> Decimal | None:
    if value in {None, ""}:
        return None
    return Decimal(str(value))


def _okx_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _sign_request(timestamp: str, method: str, path: str, body: str, secret_key: str) -> str:
    prehash = f"{timestamp}{method.upper()}{path}{body}"
    digest = hmac.new(secret_key.encode("utf-8"), prehash.encode("utf-8"), hashlib.sha256).digest()
    return base64.b64encode(digest).decode("utf-8")
