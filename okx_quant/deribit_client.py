from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal
from typing import Any
from urllib import error, parse, request


DEFAULT_HEADERS = {
    "Accept": "application/json",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/134.0 Safari/537.36"
    ),
}


class DeribitApiError(RuntimeError):
    def __init__(self, message: str, *, code: int | None = None) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class DeribitVolatilityCandle:
    ts: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal


class DeribitRestClient:
    base_url = "https://www.deribit.com"

    def get_volatility_index_candles(
        self,
        currency: str,
        resolution: str,
        *,
        start_ts: int,
        end_ts: int,
        max_records: int | None = None,
    ) -> list[DeribitVolatilityCandle]:
        if start_ts > end_ts:
            raise ValueError("开始时间不能大于结束时间")

        normalized_currency = currency.strip().upper()
        normalized_resolution = resolution.strip()
        current_end = end_ts
        seen_ts: set[int] = set()
        collected: list[DeribitVolatilityCandle] = []

        for _ in range(50):
            payload = self._request(
                "/api/v2/public/get_volatility_index_data",
                params={
                    "currency": normalized_currency,
                    "start_timestamp": str(start_ts),
                    "end_timestamp": str(current_end),
                    "resolution": normalized_resolution,
                },
            )
            result = payload.get("result")
            if not isinstance(result, dict):
                raise DeribitApiError("Deribit 返回数据缺少 result")
            candles = self._parse_candles(result.get("data"))
            for candle in candles:
                if candle.ts in seen_ts:
                    continue
                if start_ts <= candle.ts <= end_ts:
                    seen_ts.add(candle.ts)
                    collected.append(candle)
            continuation = result.get("continuation")
            if not candles:
                break
            oldest_ts = min(candle.ts for candle in candles)
            if oldest_ts <= start_ts:
                break
            if continuation in (None, "", current_end):
                break
            try:
                current_end = int(continuation)
            except (TypeError, ValueError):
                break

        collected.sort(key=lambda item: item.ts)
        if max_records is not None and max_records > 0:
            return collected[-max_records:]
        return collected

    def _parse_candles(self, payload: object) -> list[DeribitVolatilityCandle]:
        if not isinstance(payload, list):
            return []
        candles: list[DeribitVolatilityCandle] = []
        for item in payload:
            if not isinstance(item, list) or len(item) < 5:
                continue
            try:
                candles.append(
                    DeribitVolatilityCandle(
                        ts=int(item[0]),
                        open=Decimal(str(item[1])),
                        high=Decimal(str(item[2])),
                        low=Decimal(str(item[3])),
                        close=Decimal(str(item[4])),
                    )
                )
            except Exception:
                continue
        return candles

    def _request(self, path: str, *, params: dict[str, str]) -> dict[str, Any]:
        query = parse.urlencode(params)
        url = f"{self.base_url}{path}?{query}"
        req = request.Request(url, headers=DEFAULT_HEADERS, method="GET")
        try:
            with request.urlopen(req, timeout=20) as response:
                content = response.read().decode("utf-8")
        except error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="ignore")
            raise DeribitApiError(f"Deribit 请求失败：HTTP {exc.code} {body}", code=exc.code) from exc
        except error.URLError as exc:
            raise DeribitApiError(f"Deribit 网络请求失败：{exc.reason}") from exc

        try:
            payload = json.loads(content)
        except json.JSONDecodeError as exc:
            raise DeribitApiError("Deribit 返回内容不是合法 JSON") from exc

        error_payload = payload.get("error")
        if isinstance(error_payload, dict):
            code = error_payload.get("code")
            message = error_payload.get("message", "未知错误")
            raise DeribitApiError(f"Deribit 接口报错：{message}", code=code if isinstance(code, int) else None)
        return payload
