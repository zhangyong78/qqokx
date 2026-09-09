from __future__ import annotations

import time
from decimal import Decimal

from PySide6.QtCore import QThread, Signal

from okx_quant.arbitrage.basis_calculator import mid_price
from okx_quant.okx_client import OkxOrderBook, OkxRestClient, OkxTicker

from roll_terminal_qt.models import DepthRow, LegMarket, MarketPairSnapshot


class MarketFeedThread(QThread):
    snapshot_ready = Signal(object)
    status_changed = Signal(str)

    def __init__(self, *, environment: str = "live", depth: int = 10) -> None:
        super().__init__()
        self._client = OkxRestClient()
        self._environment = environment
        self._depth = depth
        self._current_inst_id = "BTC-USD-260626"
        self._target_inst_id = "BTC-USD-260925"
        self._running = True
        self._source = "starting"

    def set_pair(self, current_inst_id: str, target_inst_id: str) -> None:
        self._current_inst_id = current_inst_id.strip().upper()
        self._target_inst_id = target_inst_id.strip().upper()
        self.status_changed.emit(f"切换订阅：{self._current_inst_id} / {self._target_inst_id}")

    def stop(self) -> None:
        self._running = False

    def run(self) -> None:
        last_version = 0
        while self._running:
            current_id = self._current_inst_id
            target_id = self._target_inst_id
            try:
                self._client.ensure_public_ws_market_watch(current_id, environment=self._environment)
                self._client.ensure_public_ws_market_watch(target_id, environment=self._environment)
                snapshot, last_version = self._build_snapshot_from_ws_or_rest(
                    current_id,
                    target_id,
                    after_version=last_version,
                )
                self.snapshot_ready.emit(snapshot)
            except Exception as exc:  # noqa: BLE001
                self.status_changed.emit(f"行情源异常：{exc}")
                time.sleep(0.8)

    def _build_snapshot_from_ws_or_rest(
        self,
        current_id: str,
        target_id: str,
        *,
        after_version: int,
    ) -> tuple[MarketPairSnapshot, int]:
        current_ticker_payload = self._client.get_cached_public_ticker(current_id, environment=self._environment)
        target_ticker_payload = self._client.get_cached_public_ticker(target_id, environment=self._environment)
        current_book_payload = self._client.get_cached_public_order_book(current_id, environment=self._environment)
        target_book_payload = self._client.get_cached_public_order_book(target_id, environment=self._environment)
        if current_ticker_payload and target_ticker_payload and current_book_payload and target_book_payload:
            source = "WS"
            version = max(
                current_ticker_payload[0],
                target_ticker_payload[0],
                current_book_payload[0],
                target_book_payload[0],
            )
            current_ticker = current_ticker_payload[1]
            target_ticker = target_ticker_payload[1]
            current_book = current_book_payload[1]
            target_book = target_book_payload[1]
            wait_after = version
        else:
            source = "REST"
            current_ticker = self._client.get_ticker(current_id)
            target_ticker = self._client.get_ticker(target_id)
            current_book = self._client.get_order_book(current_id, depth=self._depth)
            target_book = self._client.get_order_book(target_id, depth=self._depth)
            version = after_version
            wait_after = after_version

        snapshot = self._compose_snapshot(
            current_id=current_id,
            target_id=target_id,
            current_ticker=current_ticker,
            target_ticker=target_ticker,
            current_book=current_book,
            target_book=target_book,
            source=source,
        )
        if source == "WS":
            self._client.wait_public_market_update(
                (current_id, target_id),
                environment=self._environment,
                after_version=wait_after,
                timeout=0.5,
            )
        else:
            time.sleep(0.7)
        return snapshot, version

    def _compose_snapshot(
        self,
        *,
        current_id: str,
        target_id: str,
        current_ticker: OkxTicker,
        target_ticker: OkxTicker,
        current_book: OkxOrderBook,
        target_book: OkxOrderBook,
        source: str,
    ) -> MarketPairSnapshot:
        current_mid = mid_price(current_ticker.bid, current_ticker.ask) or current_ticker.last
        target_mid = mid_price(target_ticker.bid, target_ticker.ask) or target_ticker.last
        spread_abs = None
        spread_pct = None
        if current_mid is not None and current_mid > 0 and target_mid is not None:
            spread_abs = target_mid - current_mid
            spread_pct = (spread_abs / current_mid) * Decimal("100")
        return MarketPairSnapshot(
            current=self._leg_from_payload(current_id, current_ticker, current_book, source),
            target=self._leg_from_payload(target_id, target_ticker, target_book, source),
            spread_abs=spread_abs,
            spread_pct=spread_pct,
            updated_at=time.strftime("%H:%M:%S"),
            status=f"{source} 行情 {time.strftime('%H:%M:%S')}",
        )

    def _leg_from_payload(
        self,
        inst_id: str,
        ticker: OkxTicker,
        order_book: OkxOrderBook,
        source: str,
    ) -> LegMarket:
        bids = tuple(DepthRow(price, size) for price, size in order_book.bids[: self._depth])
        asks = tuple(DepthRow(price, size) for price, size in order_book.asks[: self._depth])
        return LegMarket(
            inst_id=inst_id,
            last=ticker.last,
            bid=ticker.bid,
            ask=ticker.ask,
            bids=bids,
            asks=asks,
            source=source,
        )

