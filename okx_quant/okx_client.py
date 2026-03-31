from __future__ import annotations

import base64
import hashlib
import hmac
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
from urllib import error, parse, request

from okx_quant.candle_cache import (
    DEFAULT_CANDLE_CACHE_CAPACITY,
    load_candle_cache,
    merge_candles,
    save_candle_cache,
)
from okx_quant.models import Candle, Credentials, Instrument, OrderPlan, StrategyConfig, TriggerPriceType
from okx_quant.pricing import format_decimal, snap_to_increment


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

OPTION_ID_PATTERN = re.compile(r"^[A-Z0-9]+-[A-Z0-9]+-\d{6}-[0-9]+-[CP]$")
MAX_PUBLIC_CANDLE_LIMIT = 300


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
    avg_price: Decimal | None
    size: Decimal | None
    filled_size: Decimal | None
    raw: dict[str, Any]


@dataclass(frozen=True)
class OkxTicker:
    inst_id: str
    last: Decimal | None
    bid: Decimal | None
    ask: Decimal | None
    mark: Decimal | None
    index: Decimal | None
    raw: dict[str, Any]


@dataclass(frozen=True)
class OkxPosition:
    inst_id: str
    inst_type: str
    pos_side: str
    mgn_mode: str
    position: Decimal
    avail_position: Decimal | None
    avg_price: Decimal | None
    mark_price: Decimal | None
    unrealized_pnl: Decimal | None
    unrealized_pnl_ratio: Decimal | None
    liquidation_price: Decimal | None
    leverage: Decimal | None
    margin_ccy: str | None
    last_price: Decimal | None
    realized_pnl: Decimal | None
    margin_ratio: Decimal | None
    initial_margin: Decimal | None
    maintenance_margin: Decimal | None
    delta: Decimal | None
    gamma: Decimal | None
    vega: Decimal | None
    theta: Decimal | None
    raw: dict[str, Any]


@dataclass(frozen=True)
class OkxFillHistoryItem:
    fill_time: int | None
    inst_id: str
    inst_type: str
    side: str | None
    pos_side: str | None
    fill_price: Decimal | None
    fill_size: Decimal | None
    fill_fee: Decimal | None
    fee_currency: str | None
    pnl: Decimal | None
    order_id: str | None
    trade_id: str | None
    exec_type: str | None
    raw: dict[str, Any]


@dataclass(frozen=True)
class OkxPositionHistoryItem:
    update_time: int | None
    inst_id: str
    inst_type: str
    mgn_mode: str | None
    pos_side: str | None
    direction: str | None
    open_avg_price: Decimal | None
    close_avg_price: Decimal | None
    close_size: Decimal | None
    pnl: Decimal | None
    realized_pnl: Decimal | None
    settle_pnl: Decimal | None
    raw: dict[str, Any]


@dataclass(frozen=True)
class OkxAccountAssetItem:
    ccy: str
    equity: Decimal | None
    equity_usd: Decimal | None
    cash_balance: Decimal | None
    available_balance: Decimal | None
    available_equity: Decimal | None
    frozen_balance: Decimal | None
    unrealized_pnl: Decimal | None
    discount_equity: Decimal | None
    liability: Decimal | None
    cross_liability: Decimal | None
    interest: Decimal | None
    raw: dict[str, Any]


@dataclass(frozen=True)
class OkxAccountOverview:
    total_equity: Decimal | None
    adjusted_equity: Decimal | None
    isolated_equity: Decimal | None
    available_equity: Decimal | None
    unrealized_pnl: Decimal | None
    initial_margin: Decimal | None
    maintenance_margin: Decimal | None
    order_frozen: Decimal | None
    notional_usd: Decimal | None
    details: tuple[OkxAccountAssetItem, ...]
    raw: dict[str, Any]


@dataclass(frozen=True)
class OkxAccountConfig:
    account_level: str | None
    position_mode: str | None
    auto_loan: bool | None
    greeks_type: str | None
    level: str | None
    raw: dict[str, Any]


class OkxRestClient:
    base_url = "https://www.okx.com"

    def get_instruments(
        self,
        inst_type: str,
        *,
        uly: str | None = None,
        inst_family: str | None = None,
    ) -> list[Instrument]:
        params = {"instType": inst_type.upper()}
        if uly:
            params["uly"] = uly
        if inst_family:
            params["instFamily"] = inst_family

        payload = self._request("GET", "/api/v5/public/instruments", params=params)
        instruments: list[Instrument] = []
        for item in payload["data"]:
            instruments.append(
                Instrument(
                    inst_id=item["instId"],
                    inst_type=item["instType"],
                    tick_size=_decimal_or(item.get("tickSz"), Decimal("0.00000001")),
                    lot_size=_decimal_or(item.get("lotSz"), Decimal("1")),
                    min_size=_decimal_or(item.get("minSz"), _decimal_or(item.get("lotSz"), Decimal("1"))),
                    state=item.get("state", ""),
                    settle_ccy=item.get("settleCcy"),
                    ct_val=_to_decimal(item.get("ctVal")),
                    ct_mult=_to_decimal(item.get("ctMult")),
                    ct_val_ccy=item.get("ctValCcy"),
                    uly=item.get("uly"),
                    inst_family=item.get("instFamily"),
                )
            )
        instruments.sort(key=lambda item: item.inst_id)
        return instruments

    def get_swap_instruments(self) -> list[Instrument]:
        return self.get_instruments("SWAP")

    def get_option_instruments(self, *, uly: str | None = None, inst_family: str | None = None) -> list[Instrument]:
        return self.get_instruments("OPTION", uly=uly, inst_family=inst_family)

    def get_spot_instruments(self) -> list[Instrument]:
        return self.get_instruments("SPOT")

    def get_instrument(self, inst_id: str) -> Instrument:
        normalized = inst_id.strip().upper()
        inst_type = infer_inst_type(normalized)
        inst_family = infer_option_family(normalized) if inst_type == "OPTION" else None
        for instrument in self.get_instruments(inst_type, inst_family=inst_family):
            if instrument.inst_id == normalized:
                return instrument
        raise OkxApiError(f"未找到可交易标的：{normalized}")

    def get_candles(self, inst_id: str, bar: str, limit: int = 200) -> list[Candle]:
        payload = self._request(
            "GET",
            "/api/v5/market/candles",
            params={"instId": inst_id, "bar": bar, "limit": str(max(1, min(limit, MAX_PUBLIC_CANDLE_LIMIT)))},
        )
        return self._parse_candles_payload(payload)

    def get_candles_history(self, inst_id: str, bar: str, limit: int = 200) -> list[Candle]:
        requested_limit = max(1, limit)
        cached = load_candle_cache(inst_id, bar)
        cached_ts = {candle.ts for candle in cached}
        latest_added_ts: set[int] = set()
        older_added_ts: set[int] = set()

        try:
            latest_batch = self._fetch_history_candles_page(
                inst_id,
                bar,
                limit=min(requested_limit, MAX_PUBLIC_CANDLE_LIMIT),
            )
        except Exception:
            returned = cached[-requested_limit:]
            self.last_candle_history_stats = {
                "cache_hit_count": min(len(returned), requested_limit),
                "latest_fetch_count": 0,
                "older_fetch_count": 0,
                "requested_count": requested_limit,
                "returned_count": len(returned),
            }
            if len(cached) >= requested_limit:
                return returned
            raise

        latest_batch_ts = {candle.ts for candle in latest_batch}
        latest_added_ts = latest_batch_ts - cached_ts

        collected = merge_candles(cached, latest_batch)
        after = str(collected[0].ts) if collected else None

        while len(collected) < requested_limit and after is not None:
            page_limit = min(requested_limit - len(collected), MAX_PUBLIC_CANDLE_LIMIT)
            batch = self._fetch_history_candles_page(inst_id, bar, limit=page_limit, after=after)
            if not batch:
                break
            previous_ts = {candle.ts for candle in collected}
            previous_oldest = collected[0].ts if collected else None
            collected = merge_candles(collected, batch)
            older_added_ts.update({candle.ts for candle in batch} - previous_ts)
            oldest_ts = collected[0].ts if collected else None
            if oldest_ts is None or oldest_ts == previous_oldest:
                break
            after = str(oldest_ts)
            if len(batch) < page_limit:
                break

        save_candle_cache(
            inst_id,
            bar,
            collected,
            max_records=max(DEFAULT_CANDLE_CACHE_CAPACITY, requested_limit),
        )
        returned = collected[-requested_limit:]
        returned_ts = {candle.ts for candle in returned}
        self.last_candle_history_stats = {
            "cache_hit_count": len(returned_ts & cached_ts),
            "latest_fetch_count": len(returned_ts & latest_added_ts),
            "older_fetch_count": len(returned_ts & older_added_ts),
            "requested_count": requested_limit,
            "returned_count": len(returned),
        }
        return returned

    def _fetch_history_candles_page(
        self,
        inst_id: str,
        bar: str,
        *,
        limit: int,
        after: str | None = None,
    ) -> list[Candle]:
        params = {"instId": inst_id, "bar": bar, "limit": str(max(1, min(limit, MAX_PUBLIC_CANDLE_LIMIT)))}
        if after is not None:
            params["after"] = after
        payload = self._request("GET", "/api/v5/market/history-candles", params=params)
        return self._parse_candles_payload(payload)

    def _parse_candles_payload(self, payload: dict[str, Any]) -> list[Candle]:
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
                    confirmed=(row[8] == "1") if len(row) > 8 else True,
                )
            )
        candles.sort(key=lambda candle: candle.ts)
        return candles

    def get_mark_price_candles(self, inst_id: str, bar: str, limit: int = 200) -> list[Candle]:
        payload = self._request(
            "GET",
            "/api/v5/market/mark-price-candles",
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
                    volume=Decimal("0"),
                    confirmed=(row[5] == "1") if len(row) > 5 else True,
                )
            )
        candles.sort(key=lambda candle: candle.ts)
        return candles

    def get_ticker(self, inst_id: str) -> OkxTicker:
        payload = self._request("GET", "/api/v5/market/ticker", params={"instId": inst_id})
        if not payload["data"]:
            raise OkxApiError(f"OKX 未返回行情：{inst_id}")
        first = payload["data"][0]
        return OkxTicker(
            inst_id=inst_id,
            last=_to_decimal(first.get("last")),
            bid=_to_decimal(first.get("bidPx")),
            ask=_to_decimal(first.get("askPx")),
            mark=_to_decimal(first.get("markPx")),
            index=_to_decimal(first.get("idxPx")),
            raw=first,
        )

    def get_mark_price(self, inst_id: str) -> Decimal:
        payload = self._request(
            "GET",
            "/api/v5/public/mark-price",
            params={"instType": infer_inst_type(inst_id), "instId": inst_id},
        )
        if not payload.get("data"):
            raise OkxApiError(f"{inst_id} 缺少标记价格，无法触发")
        mark_price = _to_decimal(payload["data"][0].get("markPx"))
        if mark_price is None:
            raise OkxApiError(f"{inst_id} 缺少标记价格，无法触发")
        return mark_price

    def get_trigger_price(self, inst_id: str, price_type: TriggerPriceType) -> Decimal:
        ticker = self.get_ticker(inst_id)
        if price_type == "mark" and ticker.mark is None:
            return self.get_mark_price(inst_id)
        if price_type == "last":
            if ticker.last is None:
                raise OkxApiError(f"{inst_id} 缺少最新成交价，无法触发")
            return ticker.last
        if price_type == "mark":
            if ticker.mark is None:
                raise OkxApiError(f"{inst_id} 缺少标记价格，无法触发")
            return ticker.mark
        if price_type == "index":
            if ticker.index is None:
                raise OkxApiError(f"{inst_id} 缺少指数价格，无法触发")
            return ticker.index
        raise ValueError(f"Unsupported trigger price type: {price_type}")

    def get_positions(
        self,
        credentials: Credentials,
        *,
        environment: str,
        inst_type: str | None = None,
    ) -> list[OkxPosition]:
        params: dict[str, str] | None = None
        if inst_type:
            params = {"instType": inst_type.upper()}

        payload = self._request(
            "GET",
            "/api/v5/account/positions",
            params=params,
            auth=True,
            credentials=credentials,
            simulated=environment == "demo",
        )
        positions: list[OkxPosition] = []
        for item in payload.get("data", []):
            position = _to_decimal(item.get("pos")) or Decimal("0")
            if position == 0:
                continue
            positions.append(
                OkxPosition(
                    inst_id=item.get("instId", ""),
                    inst_type=item.get("instType", ""),
                    pos_side=item.get("posSide", ""),
                    mgn_mode=item.get("mgnMode", ""),
                    position=position,
                    avail_position=_to_decimal(item.get("availPos")),
                    avg_price=_to_decimal(item.get("avgPx")),
                    mark_price=_to_decimal(item.get("markPx")),
                    unrealized_pnl=_to_decimal(item.get("upl")),
                    unrealized_pnl_ratio=_to_decimal(item.get("uplRatio")),
                    liquidation_price=_to_decimal(item.get("liqPx")),
                    leverage=_to_decimal(item.get("lever")),
                    margin_ccy=item.get("ccy"),
                    last_price=_to_decimal(item.get("last")),
                    realized_pnl=_to_decimal(item.get("realizedPnl")),
                    margin_ratio=_to_decimal(item.get("mgnRatio")),
                    initial_margin=_to_decimal(item.get("imr")),
                    maintenance_margin=_to_decimal(item.get("mmr")),
                    delta=_first_decimal(item.get("deltaPA"), item.get("deltaBS")),
                    gamma=_first_decimal(item.get("gammaPA"), item.get("gammaBS")),
                    vega=_first_decimal(item.get("vegaPA"), item.get("vegaBS")),
                    theta=_first_decimal(item.get("thetaPA"), item.get("thetaBS")),
                    raw=item,
                )
            )
        positions.sort(key=lambda item: (item.inst_type, item.inst_id, item.pos_side))
        return positions

    def get_account_overview(
        self,
        credentials: Credentials,
        *,
        environment: str,
    ) -> OkxAccountOverview:
        payload = self._request(
            "GET",
            "/api/v5/account/balance",
            auth=True,
            credentials=credentials,
            simulated=environment == "demo",
        )
        first = (payload.get("data") or [{}])[0]
        details: list[OkxAccountAssetItem] = []
        for item in first.get("details", []):
            details.append(
                OkxAccountAssetItem(
                    ccy=item.get("ccy", ""),
                    equity=_first_decimal(item.get("eq"), item.get("cashBal")),
                    equity_usd=_to_decimal(item.get("eqUsd")),
                    cash_balance=_to_decimal(item.get("cashBal")),
                    available_balance=_to_decimal(item.get("availBal")),
                    available_equity=_to_decimal(item.get("availEq")),
                    frozen_balance=_first_decimal(item.get("frozenBal"), item.get("ordFrozen"), item.get("fixedBal")),
                    unrealized_pnl=_to_decimal(item.get("upl")),
                    discount_equity=_to_decimal(item.get("disEq")),
                    liability=_first_decimal(item.get("liab"), item.get("uplLiab")),
                    cross_liability=_to_decimal(item.get("crossLiab")),
                    interest=_to_decimal(item.get("interest")),
                    raw=item,
                )
            )
        details.sort(key=lambda asset: (asset.equity_usd or Decimal("0"), asset.equity or Decimal("0")), reverse=True)
        return OkxAccountOverview(
            total_equity=_to_decimal(first.get("totalEq")),
            adjusted_equity=_to_decimal(first.get("adjEq")),
            isolated_equity=_to_decimal(first.get("isoEq")),
            available_equity=_to_decimal(first.get("availEq")),
            unrealized_pnl=_to_decimal(first.get("upl")),
            initial_margin=_to_decimal(first.get("imr")),
            maintenance_margin=_to_decimal(first.get("mmr")),
            order_frozen=_first_decimal(first.get("ordFroz"), first.get("frozenBal")),
            notional_usd=_to_decimal(first.get("notionalUsd")),
            details=tuple(details),
            raw=first,
        )

    def get_account_config(
        self,
        credentials: Credentials,
        *,
        environment: str,
    ) -> OkxAccountConfig:
        payload = self._request(
            "GET",
            "/api/v5/account/config",
            auth=True,
            credentials=credentials,
            simulated=environment == "demo",
        )
        first = (payload.get("data") or [{}])[0]
        auto_loan_raw = first.get("autoLoan")
        auto_loan: bool | None
        if auto_loan_raw in {None, ""}:
            auto_loan = None
        else:
            auto_loan = str(auto_loan_raw).strip().lower() in {"true", "on", "1"}
        return OkxAccountConfig(
            account_level=first.get("acctLv"),
            position_mode=first.get("posMode"),
            auto_loan=auto_loan,
            greeks_type=first.get("greeksType"),
            level=first.get("level"),
            raw=first,
        )

    def get_fills_history(
        self,
        credentials: Credentials,
        *,
        environment: str,
        inst_types: tuple[str, ...] = ("SWAP", "FUTURES", "OPTION", "SPOT"),
        limit: int = 100,
    ) -> list[OkxFillHistoryItem]:
        items: list[OkxFillHistoryItem] = []
        per_type_limit = max(1, min(limit, 100))
        for inst_type in inst_types:
            payload = self._request(
                "GET",
                "/api/v5/trade/fills-history",
                params={"instType": inst_type, "limit": str(per_type_limit)},
                auth=True,
                credentials=credentials,
                simulated=environment == "demo",
            )
            for item in payload.get("data", []):
                items.append(
                    OkxFillHistoryItem(
                        fill_time=_to_int(item.get("fillTime"), item.get("ts"), item.get("cTime")),
                        inst_id=item.get("instId", ""),
                        inst_type=item.get("instType", inst_type),
                        side=item.get("side"),
                        pos_side=item.get("posSide"),
                        fill_price=_to_decimal(item.get("fillPx")),
                        fill_size=_to_decimal(item.get("fillSz")),
                        fill_fee=_to_decimal(item.get("fillFee")),
                        fee_currency=item.get("fillFeeCcy") or item.get("feeCcy"),
                        pnl=_to_decimal(item.get("fillPnl")),
                        order_id=item.get("ordId"),
                        trade_id=item.get("tradeId"),
                        exec_type=item.get("execType"),
                        raw=item,
                    )
                )
        items.sort(key=lambda item: item.fill_time or 0, reverse=True)
        return items[:limit]

    def get_positions_history(
        self,
        credentials: Credentials,
        *,
        environment: str,
        inst_types: tuple[str, ...] = ("SWAP", "FUTURES", "OPTION"),
        limit: int = 100,
    ) -> list[OkxPositionHistoryItem]:
        items: list[OkxPositionHistoryItem] = []
        per_type_limit = max(1, min(limit, 100))
        for inst_type in inst_types:
            payload = self._request(
                "GET",
                "/api/v5/account/positions-history",
                params={"instType": inst_type, "limit": str(per_type_limit)},
                auth=True,
                credentials=credentials,
                simulated=environment == "demo",
            )
            for item in payload.get("data", []):
                items.append(
                    OkxPositionHistoryItem(
                        update_time=_to_int(item.get("uTime"), item.get("cTime"), item.get("ts")),
                        inst_id=item.get("instId", ""),
                        inst_type=item.get("instType", inst_type),
                        mgn_mode=item.get("mgnMode"),
                        pos_side=item.get("posSide"),
                        direction=item.get("direction"),
                        open_avg_price=_to_decimal(item.get("openAvgPx")),
                        close_avg_price=_to_decimal(item.get("closeAvgPx")),
                        close_size=_first_decimal(item.get("closeTotalPos"), item.get("closePos"), item.get("closeSz")),
                        pnl=_to_decimal(item.get("pnl")),
                        realized_pnl=_to_decimal(item.get("realizedPnl")),
                        settle_pnl=_to_decimal(item.get("settledPnl")),
                        raw=item,
                    )
                )
        items.sort(key=lambda item: item.update_time or 0, reverse=True)
        return items[:limit]

    def place_market_order(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        plan: OrderPlan,
    ) -> OkxOrderResult:
        instrument = self.get_instrument(plan.inst_id)
        if instrument.inst_type == "OPTION":
            raise OkxApiError("OKX 期权不支持这里的市价附带止盈止损下单，请改走本地下单/本地止盈止损流程")

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
        instrument = self.get_instrument(plan.inst_id)
        if instrument.inst_type == "OPTION":
            raise OkxApiError("OKX 期权不支持这里的限价附带止盈止损下单，请改走本地下单/本地止盈止损流程")

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

    def place_simple_order(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        *,
        inst_id: str,
        side: str,
        size: Decimal,
        ord_type: str,
        pos_side: str | None = None,
        price: Decimal | None = None,
        cl_ord_id: str | None = None,
    ) -> OkxOrderResult:
        order: dict[str, Any] = {
            "instId": inst_id,
            "tdMode": config.trade_mode,
            "side": side,
            "ordType": ord_type,
            "sz": format_decimal(size),
        }
        if pos_side:
            order["posSide"] = pos_side
        if price is not None:
            order["px"] = format_decimal(price)
        if cl_ord_id:
            order["clOrdId"] = cl_ord_id

        payload = self._request(
            "POST",
            "/api/v5/trade/order",
            body=order,
            auth=True,
            credentials=credentials,
            simulated=config.environment == "demo",
        )
        return self._parse_order_result(payload, empty_message="OKX 返回了空的下单结果")

    def place_aggressive_limit_order(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        instrument: Instrument,
        *,
        side: str,
        size: Decimal,
        pos_side: str | None = None,
    ) -> OkxOrderResult:
        ticker = self.get_ticker(instrument.inst_id)
        base_price = _pick_aggressive_price(ticker, side)
        if base_price is None or base_price <= 0:
            raise OkxApiError(f"{instrument.inst_id} 缺少可用买一卖一/最新价，无法下单")

        if side == "buy":
            order_price = snap_to_increment(base_price + (instrument.tick_size * 2), instrument.tick_size, "up")
        else:
            raw_price = base_price - (instrument.tick_size * 2)
            if raw_price <= 0:
                raw_price = instrument.tick_size
            order_price = snap_to_increment(raw_price, instrument.tick_size, "down")

        return self.place_simple_order(
            credentials,
            config,
            inst_id=instrument.inst_id,
            side=side,
            size=size,
            ord_type="ioc",
            pos_side=pos_side,
            price=order_price,
        )

    def get_order(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        *,
        inst_id: str,
        ord_id: str | None = None,
        cl_ord_id: str | None = None,
    ) -> OkxOrderStatus:
        if not ord_id and not cl_ord_id:
            raise ValueError("ord_id 和 cl_ord_id 至少需要提供一个。")
        params = {"instId": inst_id}
        if ord_id:
            params["ordId"] = ord_id
        if cl_ord_id:
            params["clOrdId"] = cl_ord_id
        payload = self._request(
            "GET",
            "/api/v5/trade/order",
            params=params,
            auth=True,
            credentials=credentials,
            simulated=config.environment == "demo",
        )
        if not payload["data"]:
            order_key = ord_id or cl_ord_id or ""
            raise OkxApiError(f"OKX 未返回订单状态：{order_key}")

        first = payload["data"][0]
        return OkxOrderStatus(
            ord_id=first.get("ordId", ord_id or ""),
            state=first.get("state", ""),
            side=first.get("side"),
            ord_type=first.get("ordType"),
            price=_to_decimal(first.get("px")),
            avg_price=_to_decimal(first.get("avgPx")),
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


def infer_inst_type(inst_id: str) -> str:
    normalized = inst_id.strip().upper()
    if normalized.endswith("-SWAP"):
        return "SWAP"
    if OPTION_ID_PATTERN.match(normalized):
        return "OPTION"
    return "SPOT"


def infer_option_family(inst_id: str) -> str | None:
    normalized = inst_id.strip().upper()
    if not OPTION_ID_PATTERN.match(normalized):
        return None
    parts = normalized.split("-")
    return f"{parts[0]}-{parts[1]}"


def _pick_aggressive_price(ticker: OkxTicker, side: str) -> Decimal | None:
    if side == "buy":
        return ticker.ask or ticker.last or ticker.bid
    return ticker.bid or ticker.last or ticker.ask


def _to_decimal(value: Any) -> Decimal | None:
    if value in {None, ""}:
        return None
    return Decimal(str(value))


def _decimal_or(value: Any, fallback: Decimal) -> Decimal:
    decimal_value = _to_decimal(value)
    return decimal_value if decimal_value is not None else fallback


def _first_decimal(*values: Any) -> Decimal | None:
    for value in values:
        decimal_value = _to_decimal(value)
        if decimal_value is not None:
            return decimal_value
    return None


def _to_int(*values: Any) -> int | None:
    for value in values:
        if value in {None, ""}:
            continue
        try:
            return int(str(value))
        except (TypeError, ValueError):
            continue
    return None


def _okx_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _sign_request(timestamp: str, method: str, path: str, body: str, secret_key: str) -> str:
    prehash = f"{timestamp}{method.upper()}{path}{body}"
    digest = hmac.new(secret_key.encode("utf-8"), prehash.encode("utf-8"), hashlib.sha256).digest()
    return base64.b64encode(digest).decode("utf-8")
