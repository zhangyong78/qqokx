from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from decimal import Decimal
from typing import Callable

from okx_quant.arbitrage.basis_calculator import compute_basis, mid_price
from okx_quant.arbitrage.fill_reconciler import (
    derivative_contracts_from_spot_base,
    estimate_cash_and_carry_pnl,
    format_reconcile_message,
    reconcile_fill,
    spot_base_from_derivative_fill,
)
from okx_quant.arbitrage.models import ArbitrageLedgerEntry, ArbitrageTradeRuntime, SizeUnit
from okx_quant.arbitrage.position_ledger import find_ledger_entry, load_open_ledger_entries, upsert_ledger_entry
from okx_quant.arbitrage.size_converter import preview_arbitrage_size
from okx_quant.models import StrategyConfig
from okx_quant.okx_client import OkxApiError, OkxRestClient
from okx_quant.pricing import format_decimal, snap_to_increment

Logger = Callable[[str], None]
FillWaitSeconds = 90.0
PollSeconds = 1.0


@dataclass(frozen=True)
class ArbitrageOpenRequest:
    base_ccy: str
    spot_inst_id: str
    derivative_inst_id: str
    size: Decimal
    size_unit: SizeUnit
    trigger_mode: str
    open_spread_pct_max: Decimal | None
    open_spread_abs_max: Decimal | None
    spot_limit_price: Decimal | None
    derivative_limit_price: Decimal | None
    use_limit_orders: bool
    max_slippage: Decimal
    batch_count: int = 1
    batch_contract_qty: Decimal | None = None
    execution_mode: str = "dual_taker"
    maker_wait_seconds: float = 6.0
    chase_limit: int = 3


@dataclass(frozen=True)
class ArbitrageCloseRequest:
    entry_id: str | None
    max_slippage: Decimal
    use_limit_orders: bool
    spot_limit_price: Decimal | None = None
    derivative_limit_price: Decimal | None = None
    close_derivative_qty: Decimal | None = None
    batch_count: int = 1
    batch_contract_qty: Decimal | None = None
    execution_mode: str = "dual_taker"
    maker_wait_seconds: float = 6.0
    chase_limit: int = 3


@dataclass(frozen=True)
class ArbitrageRollRequest:
    entry_id: str | None
    target_derivative_inst_id: str
    max_slippage: Decimal
    use_limit_orders: bool
    roll_derivative_qty: Decimal | None = None
    current_derivative_limit_price: Decimal | None = None
    target_derivative_limit_price: Decimal | None = None
    batch_count: int = 1
    batch_contract_qty: Decimal | None = None
    execution_mode: str = "dual_taker"
    maker_wait_seconds: float = 6.0
    chase_limit: int = 3
    base_ccy: str | None = None
    spot_inst_id: str | None = None
    current_derivative_inst_id: str | None = None
    spot_qty: Decimal | None = None
    current_derivative_qty: Decimal | None = None


@dataclass
class ArbitrageOpenResult:
    success: bool
    message: str
    spot_filled_qty: Decimal = Decimal("0")
    derivative_filled_qty: Decimal = Decimal("0")
    spot_avg_price: Decimal | None = None
    derivative_avg_price: Decimal | None = None
    ledger_entry_id: str | None = None


@dataclass
class ArbitrageCloseResult:
    success: bool
    message: str
    closed_count: int = 0
    entry_ids: tuple[str, ...] = ()
    total_pnl: Decimal | None = None


@dataclass
class ArbitrageRollResult:
    success: bool
    message: str
    rolled_derivative_qty: Decimal = Decimal("0")
    target_derivative_filled_qty: Decimal = Decimal("0")
    current_derivative_avg_price: Decimal | None = None
    target_derivative_avg_price: Decimal | None = None
    entry_id: str | None = None


def _split_derivative_batches(
    total_qty: Decimal,
    *,
    instrument,
    batch_count: int = 1,
    batch_contract_qty: Decimal | None = None,
) -> list[Decimal]:
    lot_size = instrument.lot_size
    min_size = instrument.min_size
    normalized_total = snap_to_increment(total_qty, lot_size, "down")
    if normalized_total <= 0:
        raise OkxApiError("按合约最小变动单位向下取整后，总数量为 0。")
    if batch_contract_qty is not None:
        normalized_batch = snap_to_increment(batch_contract_qty, lot_size, "down")
        if normalized_batch < min_size:
            raise OkxApiError("每批张数小于合约最小下单量。")
        batches: list[Decimal] = []
        remaining = normalized_total
        while remaining > 0:
            current = min(normalized_batch, remaining)
            current = snap_to_increment(current, lot_size, "down")
            if current < min_size:
                if not batches:
                    raise OkxApiError("剩余数量不足最小下单量。")
                batches[-1] += remaining
                remaining = Decimal("0")
                break
            batches.append(current)
            remaining -= current
        return batches
    if batch_count <= 1:
        return [normalized_total]
    base_batch = snap_to_increment(normalized_total / Decimal(batch_count), lot_size, "down")
    if base_batch < min_size:
        raise OkxApiError("分批次数过大，单批数量低于合约最小下单量。")
    batches: list[Decimal] = []
    remaining = normalized_total
    while len(batches) < batch_count - 1 and remaining - base_batch >= min_size:
        batches.append(base_batch)
        remaining -= base_batch
    if remaining < min_size:
        raise OkxApiError("剩余数量不足最小下单量，请减少分批次数。")
    batches.append(remaining)
    return batches


def _build_strategy_config(inst_id: str, runtime: ArbitrageTradeRuntime) -> StrategyConfig:
    return StrategyConfig(
        inst_id=inst_id,
        bar="1m",
        ema_period=1,
        atr_period=1,
        atr_stop_multiplier=Decimal("1"),
        atr_take_multiplier=Decimal("1"),
        order_size=Decimal("1"),
        trade_mode=runtime.trade_mode,
        signal_mode="long_only",
        position_mode=runtime.position_mode,
        environment=runtime.environment,
        tp_sl_trigger_type="last",
        strategy_id="arbitrage_runtime",
    )


def _wait_order_fill(
    client: OkxRestClient,
    *,
    credentials,
    config: StrategyConfig,
    inst_id: str,
    ord_id: str,
    expected_size: Decimal,
    logger: Logger,
    label: str,
) -> tuple[Decimal, Decimal | None]:
    deadline = time.time() + FillWaitSeconds
    last_filled = Decimal("0")
    avg_price: Decimal | None = None
    while time.time() < deadline:
        status = client.get_order(credentials, config, inst_id=inst_id, ord_id=ord_id)
        filled = status.filled_size or Decimal("0")
        avg_price = status.avg_price
        state = (status.state or "").lower()
        if filled > last_filled:
            logger(f"{label} 成交进度 {format_decimal(filled)} / {format_decimal(expected_size)}")
            last_filled = filled
        if state == "filled" or filled >= expected_size:
            return filled, avg_price
        if state in {"canceled", "cancelled"}:
            if filled > 0:
                return filled, avg_price
            raise OkxApiError(f"{label} 订单已撤销，未成交。")
        time.sleep(PollSeconds)
    if last_filled > 0:
        logger(f"{label} 等待超时，已部分成交 {format_decimal(last_filled)}。")
        return last_filled, avg_price
    raise OkxApiError(f"{label} 等待成交超时。")


def _wait_order_fill_with_private_ws(
    client: OkxRestClient,
    *,
    credentials,
    config: StrategyConfig,
    inst_id: str,
    ord_id: str,
    expected_size: Decimal,
    logger: Logger,
    label: str,
) -> tuple[Decimal, Decimal | None]:
    deadline = time.time() + FillWaitSeconds
    last_filled = Decimal("0")
    avg_price: Decimal | None = None
    ws_version = 0
    while time.time() < deadline:
        remaining = max(0.0, deadline - time.time())
        status = None
        wait_private_update = getattr(client, "wait_private_order_update", None)
        if callable(wait_private_update):
            timeout_seconds = min(remaining, PollSeconds)
            if timeout_seconds > 0:
                try:
                    ws_payload = wait_private_update(
                        credentials,
                        environment=config.environment,
                        inst_id=inst_id,
                        ord_id=ord_id,
                        after_version=ws_version,
                        timeout=timeout_seconds,
                    )
                except Exception:  # noqa: BLE001
                    ws_payload = None
                if ws_payload is not None:
                    ws_version, status = ws_payload
        if status is None:
            status = client.get_order(credentials, config, inst_id=inst_id, ord_id=ord_id)
        filled = status.filled_size or Decimal("0")
        avg_price = status.avg_price
        state = (status.state or "").lower()
        if filled > last_filled:
            logger(f"{label} 成交进度 {format_decimal(filled)} / {format_decimal(expected_size)}")
            last_filled = filled
        if state == "filled" or filled >= expected_size:
            return filled, avg_price
        if state in {"canceled", "cancelled"}:
            if filled > 0:
                return filled, avg_price
            raise OkxApiError(f"{label} 订单已撤销，未成交。")
        if ws_version > 0:
            continue
        time.sleep(PollSeconds)
    if last_filled > 0:
        logger(f"{label} 等待超时，已部分成交 {format_decimal(last_filled)}。")
        return last_filled, avg_price
    raise OkxApiError(f"{label} 等待成交超时。")


_wait_order_fill = _wait_order_fill_with_private_ws


class ArbitrageExecutor:
    def __init__(self, client: OkxRestClient, *, logger: Logger | None = None) -> None:
        self._client = client
        self._logger = logger or (lambda _message: None)

    def _blend_avg_price(
        self,
        current_avg: Decimal | None,
        current_size: Decimal,
        new_avg: Decimal | None,
        new_size: Decimal,
    ) -> Decimal | None:
        if new_avg is None or new_size <= 0:
            return current_avg
        if current_avg is None or current_size <= 0:
            return new_avg
        total_size = current_size + new_size
        if total_size <= 0:
            return current_avg
        return ((current_avg * current_size) + (new_avg * new_size)) / total_size

    def _resolve_passive_price(self, instrument, *, side: str) -> Decimal:
        order_book = None
        try:
            order_book = self._client.get_order_book(instrument.inst_id, depth=5)
        except Exception:
            order_book = None
        ticker = self._client.get_ticker(instrument.inst_id)
        normalized_side = side.strip().lower()
        if normalized_side == "buy":
            raw = order_book.bids[0][0] if order_book is not None and order_book.bids else ticker.bid
            if raw is None or raw <= 0:
                raise OkxApiError(f"{instrument.inst_id} 缺少买一价，无法挂被动买单。")
            return snap_to_increment(raw, instrument.tick_size, "down")
        raw = order_book.asks[0][0] if order_book is not None and order_book.asks else ticker.ask
        if raw is None or raw <= 0:
            raise OkxApiError(f"{instrument.inst_id} 缺少卖一价，无法挂被动卖单。")
        return snap_to_increment(raw, instrument.tick_size, "up")

    def _wait_order_fill_until(
        self,
        *,
        credentials,
        config: StrategyConfig,
        inst_id: str,
        ord_id: str,
        expected_size: Decimal,
        timeout_seconds: float,
        label: str,
    ) -> tuple[Decimal, Decimal | None, bool]:
        deadline = time.time() + timeout_seconds
        last_filled = Decimal("0")
        avg_price: Decimal | None = None
        filled_completely = False
        ws_version = 0
        while time.time() < deadline:
            remaining = max(0.0, deadline - time.time())
            status = None
            wait_private_update = getattr(self._client, "wait_private_order_update", None)
            if callable(wait_private_update):
                timeout = min(remaining, PollSeconds)
                if timeout > 0:
                    try:
                        ws_payload = wait_private_update(
                            credentials,
                            environment=config.environment,
                            inst_id=inst_id,
                            ord_id=ord_id,
                            after_version=ws_version,
                            timeout=timeout,
                        )
                    except Exception:  # noqa: BLE001
                        ws_payload = None
                    if ws_payload is not None:
                        ws_version, status = ws_payload
            if status is None:
                status = self._client.get_order(credentials, config, inst_id=inst_id, ord_id=ord_id)
            filled = status.filled_size or Decimal("0")
            avg_price = status.avg_price
            state = (status.state or "").lower()
            if filled > last_filled:
                self._logger(f"{label} 成交进度 {format_decimal(filled)} / {format_decimal(expected_size)}")
                last_filled = filled
            if state == "filled" or filled >= expected_size:
                filled_completely = True
                break
            if state in {"canceled", "cancelled"}:
                break
            if ws_version > 0:
                continue
            time.sleep(PollSeconds)
        return last_filled, avg_price, filled_completely

    def _execute_taker_leg(
        self,
        *,
        credentials,
        config: StrategyConfig,
        inst_id: str,
        side: str,
        size: Decimal,
        label: str,
        pos_side: str | None = None,
        reduce_only: bool = False,
    ) -> tuple[Decimal, Decimal | None]:
        result = self._client.place_simple_order(
            credentials,
            config,
            inst_id=inst_id,
            side=side,
            size=size,
            ord_type="market",
            pos_side=pos_side,
            reduce_only=reduce_only,
            cl_ord_id=f"arb{uuid.uuid4().hex[:14]}",
        )
        return _wait_order_fill(
            self._client,
            credentials=credentials,
            config=config,
            inst_id=inst_id,
            ord_id=result.ord_id,
            expected_size=size,
            logger=self._logger,
            label=label,
        )

    def _open_maker_taker(
        self,
        request: ArbitrageOpenRequest,
        *,
        runtime: ArbitrageTradeRuntime,
        spot_inst,
        deriv_inst,
        preview,
    ) -> tuple[Decimal, Decimal | None, Decimal, Decimal | None]:
        credentials = runtime.credentials
        spot_config = _build_strategy_config(request.spot_inst_id, runtime)
        deriv_config = _build_strategy_config(request.derivative_inst_id, runtime)
        derivative_pos_side = "short" if runtime.position_mode == "long_short" else None
        maker_leg = "spot" if request.execution_mode == "spot_maker_derivative_taker" else "derivative"
        total_spot_filled = Decimal("0")
        total_derivative_filled = Decimal("0")
        spot_avg: Decimal | None = None
        deriv_avg: Decimal | None = None
        residual_spot_qty = Decimal("0")
        remaining_derivative_qty = preview.swap_contracts
        reference_price = self._client.get_ticker(request.derivative_inst_id).last or self._client.get_ticker(request.spot_inst_id).last

        for attempt in range(max(0, request.chase_limit) + 1):
            if remaining_derivative_qty < deriv_inst.min_size:
                break
            if maker_leg == "derivative":
                maker_inst_id = request.derivative_inst_id
                maker_config = deriv_config
                maker_side = "sell"
                maker_pos_side = derivative_pos_side
                maker_reduce_only = False
                maker_size = remaining_derivative_qty
                maker_instrument = deriv_inst
                maker_label = f"套利开仓挂单腿(合约) 第 {attempt + 1} 次"
            else:
                maker_inst_id = request.spot_inst_id
                maker_config = spot_config
                maker_side = "buy"
                maker_pos_side = None
                maker_reduce_only = False
                maker_size = snap_to_increment(
                    spot_base_from_derivative_fill(
                        derivative_filled_contracts=remaining_derivative_qty,
                        derivative_instrument=deriv_inst,
                    ),
                    spot_inst.lot_size,
                    "down",
                )
                maker_instrument = spot_inst
                maker_label = f"套利开仓挂单腿(现货) 第 {attempt + 1} 次"
            if maker_size <= 0:
                raise OkxApiError("挂单腿当前批次数量为 0。")
            maker_order = self._client.place_simple_order(
                credentials,
                maker_config,
                inst_id=maker_inst_id,
                side=maker_side,
                size=maker_size,
                ord_type="post_only",
                pos_side=maker_pos_side,
                price=self._resolve_passive_price(maker_instrument, side=maker_side),
                reduce_only=maker_reduce_only,
                cl_ord_id=f"arb{uuid.uuid4().hex[:14]}",
            )
            maker_filled, maker_avg, maker_done = self._wait_order_fill_until(
                credentials=credentials,
                config=maker_config,
                inst_id=maker_inst_id,
                ord_id=maker_order.ord_id,
                expected_size=maker_size,
                timeout_seconds=request.maker_wait_seconds,
                label=maker_label,
            )
            if not maker_done:
                try:
                    self._client.cancel_order(credentials, maker_config, inst_id=maker_inst_id, ord_id=maker_order.ord_id)
                except Exception:
                    pass
            if maker_leg == "derivative":
                if maker_filled <= 0:
                    if attempt >= request.chase_limit:
                        raise OkxApiError("合约挂单腿未成交，已达到最大追单次数。")
                    continue
                hedge_spot_qty = snap_to_increment(
                    spot_base_from_derivative_fill(
                        derivative_filled_contracts=maker_filled,
                        derivative_instrument=deriv_inst,
                    ),
                    spot_inst.lot_size,
                    "down",
                )
                if hedge_spot_qty <= 0:
                    raise OkxApiError("合约挂单腿成交后换算出的现货数量为 0。")
                spot_filled_once, spot_avg_once = self._execute_taker_leg(
                    credentials=credentials,
                    config=spot_config,
                    inst_id=request.spot_inst_id,
                    side="buy",
                    size=hedge_spot_qty,
                    label="套利开仓现货吃单腿",
                )
                deriv_avg = self._blend_avg_price(deriv_avg, total_derivative_filled, maker_avg, maker_filled)
                spot_avg = self._blend_avg_price(spot_avg, total_spot_filled, spot_avg_once, spot_filled_once)
                total_derivative_filled += maker_filled
                total_spot_filled += spot_filled_once
            else:
                if maker_filled <= 0:
                    if attempt >= request.chase_limit:
                        raise OkxApiError("现货挂单腿未成交，已达到最大追单次数。")
                    continue
                residual_spot_qty += maker_filled
                hedge_derivative_qty = derivative_contracts_from_spot_base(
                    spot_base_qty=residual_spot_qty,
                    derivative_instrument=deriv_inst,
                )
                if hedge_derivative_qty <= 0:
                    if attempt >= request.chase_limit:
                        raise OkxApiError("现货挂单腿已部分成交，但不足以换算成最小合约张数。")
                    continue
                deriv_filled_once, deriv_avg_once = self._execute_taker_leg(
                    credentials=credentials,
                    config=deriv_config,
                    inst_id=request.derivative_inst_id,
                    side="sell",
                    size=hedge_derivative_qty,
                    label="套利开仓合约吃单腿",
                    pos_side=derivative_pos_side,
                )
                used_spot_qty = snap_to_increment(
                    spot_base_from_derivative_fill(
                        derivative_filled_contracts=deriv_filled_once,
                        derivative_instrument=deriv_inst,
                    ),
                    spot_inst.lot_size,
                    "down",
                )
                residual_spot_qty = max(residual_spot_qty - used_spot_qty, Decimal("0"))
                spot_avg = self._blend_avg_price(spot_avg, total_spot_filled, maker_avg, used_spot_qty)
                deriv_avg = self._blend_avg_price(deriv_avg, total_derivative_filled, deriv_avg_once, deriv_filled_once)
                total_spot_filled += used_spot_qty
                total_derivative_filled += deriv_filled_once
            remaining_derivative_qty = max(preview.swap_contracts - total_derivative_filled, Decimal("0"))

        if total_spot_filled <= 0 or total_derivative_filled <= 0:
            raise OkxApiError("当前未形成有效的套利开仓成交。")
        if maker_leg == "spot" and residual_spot_qty >= spot_inst.lot_size:
            self._logger(f"现货挂单腿有剩余未完全对冲：{format_decimal(residual_spot_qty)} {request.base_ccy}")
        return total_spot_filled, spot_avg, total_derivative_filled, deriv_avg

    def _close_maker_taker(
        self,
        entry: ArbitrageLedgerEntry,
        request: ArbitrageCloseRequest,
        *,
        runtime: ArbitrageTradeRuntime,
        spot_inst,
        deriv_inst,
        planned_derivative_qty: Decimal,
    ) -> tuple[Decimal, Decimal | None, Decimal, Decimal | None]:
        credentials = runtime.credentials
        spot_config = _build_strategy_config(entry.spot_inst_id, runtime)
        deriv_config = _build_strategy_config(entry.derivative_inst_id, runtime)
        derivative_pos_side = "short" if runtime.position_mode == "long_short" else "short"
        maker_leg = "spot" if request.execution_mode == "spot_maker_derivative_taker" else "derivative"
        total_spot_filled = Decimal("0")
        total_derivative_filled = Decimal("0")
        spot_avg: Decimal | None = None
        deriv_avg: Decimal | None = None
        residual_spot_qty = Decimal("0")
        remaining_derivative_qty = planned_derivative_qty

        for attempt in range(max(0, request.chase_limit) + 1):
            if remaining_derivative_qty < deriv_inst.min_size:
                break
            if maker_leg == "derivative":
                maker_inst_id = entry.derivative_inst_id
                maker_config = deriv_config
                maker_side = "buy"
                maker_pos_side = derivative_pos_side
                maker_reduce_only = True
                maker_size = remaining_derivative_qty
                maker_instrument = deriv_inst
                maker_label = f"套利平仓挂单腿(合约) 第 {attempt + 1} 次"
            else:
                maker_inst_id = entry.spot_inst_id
                maker_config = spot_config
                maker_side = "sell"
                maker_pos_side = None
                maker_reduce_only = False
                maker_size = snap_to_increment(
                    spot_base_from_derivative_fill(
                        derivative_filled_contracts=remaining_derivative_qty,
                        derivative_instrument=deriv_inst,
                    ),
                    spot_inst.lot_size,
                    "down",
                )
                maker_instrument = spot_inst
                maker_label = f"套利平仓挂单腿(现货) 第 {attempt + 1} 次"
            if maker_size <= 0:
                raise OkxApiError("挂单腿当前批次数量为 0。")
            maker_order = self._client.place_simple_order(
                credentials,
                maker_config,
                inst_id=maker_inst_id,
                side=maker_side,
                size=maker_size,
                ord_type="post_only",
                pos_side=maker_pos_side,
                price=self._resolve_passive_price(maker_instrument, side=maker_side),
                reduce_only=maker_reduce_only,
                cl_ord_id=f"arb{uuid.uuid4().hex[:14]}",
            )
            maker_filled, maker_avg, maker_done = self._wait_order_fill_until(
                credentials=credentials,
                config=maker_config,
                inst_id=maker_inst_id,
                ord_id=maker_order.ord_id,
                expected_size=maker_size,
                timeout_seconds=request.maker_wait_seconds,
                label=maker_label,
            )
            if not maker_done:
                try:
                    self._client.cancel_order(credentials, maker_config, inst_id=maker_inst_id, ord_id=maker_order.ord_id)
                except Exception:
                    pass
            if maker_leg == "derivative":
                if maker_filled <= 0:
                    if attempt >= request.chase_limit:
                        raise OkxApiError("合约挂单腿未成交，已达到最大追单次数。")
                    continue
                hedge_spot_qty = snap_to_increment(
                    spot_base_from_derivative_fill(
                        derivative_filled_contracts=maker_filled,
                        derivative_instrument=deriv_inst,
                    ),
                    spot_inst.lot_size,
                    "down",
                )
                if hedge_spot_qty <= 0:
                    raise OkxApiError("合约挂单腿成交后换算出的现货数量为 0。")
                spot_filled_once, spot_avg_once = self._execute_taker_leg(
                    credentials=credentials,
                    config=spot_config,
                    inst_id=entry.spot_inst_id,
                    side="sell",
                    size=hedge_spot_qty,
                    label="套利平仓现货吃单腿",
                )
                deriv_avg = self._blend_avg_price(deriv_avg, total_derivative_filled, maker_avg, maker_filled)
                spot_avg = self._blend_avg_price(spot_avg, total_spot_filled, spot_avg_once, spot_filled_once)
                total_derivative_filled += maker_filled
                total_spot_filled += spot_filled_once
            else:
                if maker_filled <= 0:
                    if attempt >= request.chase_limit:
                        raise OkxApiError("现货挂单腿未成交，已达到最大追单次数。")
                    continue
                residual_spot_qty += maker_filled
                hedge_derivative_qty = derivative_contracts_from_spot_base(
                    spot_base_qty=residual_spot_qty,
                    derivative_instrument=deriv_inst,
                )
                if hedge_derivative_qty <= 0:
                    if attempt >= request.chase_limit:
                        raise OkxApiError("现货挂单腿已部分成交，但不足以换算成最小合约张数。")
                    continue
                deriv_filled_once, deriv_avg_once = self._execute_taker_leg(
                    credentials=credentials,
                    config=deriv_config,
                    inst_id=entry.derivative_inst_id,
                    side="buy",
                    size=hedge_derivative_qty,
                    label="套利平仓合约吃单腿",
                    pos_side=derivative_pos_side,
                    reduce_only=True,
                )
                used_spot_qty = snap_to_increment(
                    spot_base_from_derivative_fill(
                        derivative_filled_contracts=deriv_filled_once,
                        derivative_instrument=deriv_inst,
                    ),
                    spot_inst.lot_size,
                    "down",
                )
                residual_spot_qty = max(residual_spot_qty - used_spot_qty, Decimal("0"))
                spot_avg = self._blend_avg_price(spot_avg, total_spot_filled, maker_avg, used_spot_qty)
                deriv_avg = self._blend_avg_price(deriv_avg, total_derivative_filled, deriv_avg_once, deriv_filled_once)
                total_spot_filled += used_spot_qty
                total_derivative_filled += deriv_filled_once
            remaining_derivative_qty = max(planned_derivative_qty - total_derivative_filled, Decimal("0"))

        if total_spot_filled <= 0 or total_derivative_filled <= 0:
            raise OkxApiError("当前未形成有效的套利平仓成交。")
        if maker_leg == "spot" and residual_spot_qty >= spot_inst.lot_size:
            self._logger(f"现货挂单腿有剩余未完全对冲：{format_decimal(residual_spot_qty)} {entry.base_ccy}")
        return total_derivative_filled, deriv_avg, total_spot_filled, spot_avg

    def open_cash_and_carry(
        self,
        request: ArbitrageOpenRequest,
        *,
        runtime: ArbitrageTradeRuntime,
    ) -> ArbitrageOpenResult:
        credentials = runtime.credentials
        spot_inst = self._client.get_instrument(request.spot_inst_id)
        deriv_inst = self._client.get_instrument(request.derivative_inst_id)
        spot_ticker = self._client.get_ticker(request.spot_inst_id)
        spot_mid = mid_price(spot_ticker.bid, spot_ticker.ask)
        if spot_mid is None:
            return ArbitrageOpenResult(success=False, message=f"无法获取 {request.spot_inst_id} 报价。")

        preview = preview_arbitrage_size(
            size=request.size,
            unit=request.size_unit,
            spot_mid=spot_mid,
            spot_instrument=spot_inst,
            swap_instrument=deriv_inst,
        )
        if preview.spot_base_qty <= 0 or preview.swap_contracts <= 0:
            return ArbitrageOpenResult(success=False, message="换算后的现货或合约数量为 0，请增大投入。")
        try:
            planned_batches = _split_derivative_batches(
                preview.swap_contracts,
                instrument=deriv_inst,
                batch_count=request.batch_count,
                batch_contract_qty=request.batch_contract_qty,
            )
        except OkxApiError as exc:
            return ArbitrageOpenResult(success=False, message=str(exc))
        if len(planned_batches) > 1:
            total_spot_filled = Decimal("0")
            total_derivative_filled = Decimal("0")
            spot_avg: Decimal | None = None
            deriv_avg: Decimal | None = None
            ledger_entry_ids: list[str] = []
            batch_messages: list[str] = []
            for index, batch_qty in enumerate(planned_batches, start=1):
                batch_request = replace(
                    request,
                    size=batch_qty,
                    size_unit="contracts",
                    batch_count=1,
                    batch_contract_qty=None,
                )
                batch_result = self.open_cash_and_carry(batch_request, runtime=runtime)
                batch_messages.append(f"第 {index}/{len(planned_batches)} 批：{batch_result.message}")
                if batch_result.spot_filled_qty > 0:
                    spot_avg = self._blend_avg_price(
                        spot_avg,
                        total_spot_filled,
                        batch_result.spot_avg_price,
                        batch_result.spot_filled_qty,
                    )
                    total_spot_filled += batch_result.spot_filled_qty
                if batch_result.derivative_filled_qty > 0:
                    deriv_avg = self._blend_avg_price(
                        deriv_avg,
                        total_derivative_filled,
                        batch_result.derivative_avg_price,
                        batch_result.derivative_filled_qty,
                    )
                    total_derivative_filled += batch_result.derivative_filled_qty
                if batch_result.ledger_entry_id:
                    ledger_entry_ids.append(batch_result.ledger_entry_id)
                if not batch_result.success:
                    return ArbitrageOpenResult(
                        success=False,
                        message=(
                            f"分批开仓中断：已完成 {index - 1}/{len(planned_batches)} 批。\n"
                            + "\n".join(batch_messages)
                        ),
                        spot_filled_qty=total_spot_filled,
                        derivative_filled_qty=total_derivative_filled,
                        spot_avg_price=spot_avg,
                        derivative_avg_price=deriv_avg,
                        ledger_entry_id=ledger_entry_ids[0] if len(ledger_entry_ids) == 1 else None,
                    )
            return ArbitrageOpenResult(
                success=True,
                message=(
                    f"分批开仓完成：共 {len(planned_batches)} 批，现货 {format_decimal(total_spot_filled)}，"
                    f"合约 {format_decimal(total_derivative_filled)} 张。\n" + "\n".join(batch_messages)
                ),
                spot_filled_qty=total_spot_filled,
                derivative_filled_qty=total_derivative_filled,
                spot_avg_price=spot_avg,
                derivative_avg_price=deriv_avg,
                ledger_entry_id=ledger_entry_ids[0] if len(ledger_entry_ids) == 1 else None,
            )

        spot_price = self._resolve_spot_buy_price(request, spot_inst, spot_ticker)
        deriv_price = self._resolve_derivative_sell_price(request, deriv_inst, request.derivative_inst_id)

        self._logger(
            "套利开仓："
            f"现货买入 {format_decimal(preview.spot_base_qty)} @ {format_decimal(spot_price)}，"
            f"合约卖出 {format_decimal(preview.swap_contracts)} 张 @ {format_decimal(deriv_price)}"
        )

        try:
            if request.execution_mode in {"spot_maker_derivative_taker", "derivative_maker_spot_taker"}:
                spot_filled, spot_avg, deriv_filled, deriv_avg = self._open_maker_taker(
                    request,
                    runtime=runtime,
                    spot_inst=spot_inst,
                    deriv_inst=deriv_inst,
                    preview=preview,
                )
                spot_reconciled = reconcile_fill(planned_size=preview.spot_base_qty, filled_size=spot_filled, avg_price=spot_avg)
                deriv_reconciled = reconcile_fill(
                    planned_size=preview.swap_contracts,
                    filled_size=deriv_filled,
                    avg_price=deriv_avg,
                )
                self._logger(format_reconcile_message("现货成交校验", spot_reconciled))
                self._logger(format_reconcile_message("合约成交校验", deriv_reconciled))
            else:
                spot_config = _build_strategy_config(request.spot_inst_id, runtime)
                deriv_config = _build_strategy_config(request.derivative_inst_id, runtime)
                spot_ord_type = "limit" if request.use_limit_orders else "market"
                deriv_ord_type = "limit" if request.use_limit_orders else "market"
                spot_result = self._client.place_simple_order(
                    credentials,
                    spot_config,
                    inst_id=request.spot_inst_id,
                    side="buy",
                    size=preview.spot_base_qty,
                    ord_type=spot_ord_type,
                    price=spot_price if spot_ord_type == "limit" else None,
                    cl_ord_id=f"arb{uuid.uuid4().hex[:14]}",
                )
                spot_filled, spot_avg = _wait_order_fill(
                    self._client,
                    credentials=credentials,
                    config=spot_config,
                    inst_id=request.spot_inst_id,
                    ord_id=spot_result.ord_id,
                    expected_size=preview.spot_base_qty,
                    logger=self._logger,
                    label="现货腿",
                )
                spot_reconciled = reconcile_fill(planned_size=preview.spot_base_qty, filled_size=spot_filled, avg_price=spot_avg)
                self._logger(format_reconcile_message("现货成交校验", spot_reconciled))

                if spot_filled <= 0:
                    return ArbitrageOpenResult(success=False, message="现货腿未成交。")

                adjusted_contracts = derivative_contracts_from_spot_base(
                    spot_base_qty=spot_reconciled.filled_size,
                    derivative_instrument=deriv_inst,
                )
                deriv_result = self._client.place_simple_order(
                    credentials,
                    deriv_config,
                    inst_id=request.derivative_inst_id,
                    side="sell",
                    size=adjusted_contracts,
                    ord_type=deriv_ord_type,
                    price=deriv_price if deriv_ord_type == "limit" else None,
                    cl_ord_id=f"arb{uuid.uuid4().hex[:14]}",
                )
                deriv_filled, deriv_avg = _wait_order_fill(
                    self._client,
                    credentials=credentials,
                    config=deriv_config,
                    inst_id=request.derivative_inst_id,
                    ord_id=deriv_result.ord_id,
                    expected_size=adjusted_contracts,
                    logger=self._logger,
                    label="合约腿",
                )
                deriv_reconciled = reconcile_fill(
                    planned_size=adjusted_contracts,
                    filled_size=deriv_filled,
                    avg_price=deriv_avg,
                )
                self._logger(format_reconcile_message("合约成交校验", deriv_reconciled))
        except OkxApiError as exc:
            return ArbitrageOpenResult(success=False, message=str(exc))
        except Exception as exc:
            return ArbitrageOpenResult(success=False, message=f"开仓异常：{exc}")

        basis_pct: Decimal | None = None
        if spot_avg and deriv_avg and spot_avg > 0:
            _, basis_pct = compute_basis(spot_avg, deriv_avg)
            basis_pct *= Decimal("100")

        entry_id = uuid.uuid4().hex
        opened_at = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
        ledger_entry = ArbitrageLedgerEntry(
            entry_id=entry_id,
            base_ccy=request.base_ccy,
            pair_kind="spot_swap",
            spot_inst_id=request.spot_inst_id,
            derivative_inst_id=request.derivative_inst_id,
            spot_qty=spot_reconciled.filled_size,
            derivative_qty=deriv_reconciled.filled_size,
            open_spot_price=spot_avg,
            open_derivative_price=deriv_avg,
            close_spot_price=None,
            close_derivative_price=None,
            basis_at_open_pct=basis_pct,
            fee_total=Decimal("0"),
            funding_total=Decimal("0"),
            realized_pnl=None,
            close_mode="open",
            opened_at=opened_at,
            closed_at=None,
            notes="自动开仓",
        )
        upsert_ledger_entry(ledger_entry)

        message = (
            f"开仓完成：现货 {format_decimal(spot_reconciled.filled_size)}，"
            f"合约 {format_decimal(deriv_reconciled.filled_size)} 张。"
        )
        self._logger(message)
        return ArbitrageOpenResult(
            success=True,
            message=message,
            spot_filled_qty=spot_reconciled.filled_size,
            derivative_filled_qty=deriv_reconciled.filled_size,
            spot_avg_price=spot_avg,
            derivative_avg_price=deriv_avg,
            ledger_entry_id=entry_id,
        )

    def close_cash_and_carry(
        self,
        request: ArbitrageCloseRequest,
        *,
        runtime: ArbitrageTradeRuntime,
    ) -> ArbitrageCloseResult:
        if request.close_derivative_qty is not None and not request.entry_id:
            return ArbitrageCloseResult(
                success=False,
                message="指定平仓数量时，请先选择一条具体的套利持仓。",
                closed_count=0,
            )
        targets = load_open_ledger_entries()
        if request.entry_id:
            targets = [item for item in targets if item.entry_id == request.entry_id]
        if not targets:
            return ArbitrageCloseResult(success=False, message="没有可平仓的套利持仓。", closed_count=0)

        closed_ids: list[str] = []
        total_pnl = Decimal("0")
        errors: list[str] = []
        for entry in targets:
            try:
                pnl = self._close_single_entry(entry, request, runtime=runtime)
                closed_ids.append(entry.entry_id)
                if pnl is not None:
                    total_pnl += pnl
            except Exception as exc:
                errors.append(f"{entry.base_ccy}: {exc}")
                self._logger(f"平仓失败 {entry.base_ccy}：{exc}")

        if not closed_ids:
            return ArbitrageCloseResult(success=False, message="；".join(errors) or "平仓失败。", closed_count=0)
        message = f"已平仓 {len(closed_ids)} 笔"
        if errors:
            message += f"，{len(errors)} 笔失败"
        if closed_ids:
            message += f"，合计盈亏约 {format_decimal(total_pnl)} USDT"
        return ArbitrageCloseResult(
            success=True,
            message=message,
            closed_count=len(closed_ids),
            entry_ids=tuple(closed_ids),
            total_pnl=total_pnl,
        )

    def _close_single_entry(
        self,
        entry: ArbitrageLedgerEntry,
        request: ArbitrageCloseRequest,
        *,
        runtime: ArbitrageTradeRuntime,
    ) -> Decimal | None:
        credentials = runtime.credentials
        spot_inst = self._client.get_instrument(entry.spot_inst_id)
        deriv_inst = self._client.get_instrument(entry.derivative_inst_id)
        deriv_ticker = self._client.get_ticker(entry.derivative_inst_id)
        spot_ticker = self._client.get_ticker(entry.spot_inst_id)
        planned_derivative_qty = self._resolve_close_derivative_qty(entry, request, deriv_inst)
        planned_batches = _split_derivative_batches(
            planned_derivative_qty,
            instrument=deriv_inst,
            batch_count=request.batch_count,
            batch_contract_qty=request.batch_contract_qty,
        )
        if len(planned_batches) > 1:
            total_pnl = Decimal("0")
            for index, batch_qty in enumerate(planned_batches, start=1):
                current_entry = find_ledger_entry(entry.entry_id)
                if current_entry is None or current_entry.close_mode != "open":
                    raise OkxApiError(f"分批平仓第 {index} 批前未找到可继续处理的 open 持仓。")
                batch_request = replace(
                    request,
                    close_derivative_qty=batch_qty,
                    batch_count=1,
                    batch_contract_qty=None,
                )
                self._logger(f"分批平仓：第 {index}/{len(planned_batches)} 批，目标 {format_decimal(batch_qty)} 张")
                batch_pnl = self._close_single_entry(current_entry, batch_request, runtime=runtime)
                if batch_pnl is not None:
                    total_pnl += batch_pnl
            return total_pnl
        planned_spot_qty = snap_to_increment(
            spot_base_from_derivative_fill(
                derivative_filled_contracts=planned_derivative_qty,
                derivative_instrument=deriv_inst,
            ),
            spot_inst.lot_size,
            "down",
        )

        deriv_price = self._resolve_derivative_buy_price(request, deriv_inst, deriv_ticker)
        spot_price = self._resolve_spot_sell_price(request, spot_inst, spot_ticker)

        self._logger(
            f"套利平仓 {entry.base_ccy}："
            f"合约买入 {format_decimal(planned_derivative_qty)} 张 @ {format_decimal(deriv_price)}，"
            f"现货卖出 {format_decimal(planned_spot_qty)} @ {format_decimal(spot_price)}"
        )
        if request.execution_mode in {"spot_maker_derivative_taker", "derivative_maker_spot_taker"}:
            deriv_filled, deriv_avg, spot_filled, spot_avg = self._close_maker_taker(
                entry,
                request,
                runtime=runtime,
                spot_inst=spot_inst,
                deriv_inst=deriv_inst,
                planned_derivative_qty=planned_derivative_qty,
            )
            deriv_reconciled = reconcile_fill(
                planned_size=planned_derivative_qty,
                filled_size=deriv_filled,
                avg_price=deriv_avg,
            )
            self._logger(format_reconcile_message("平仓合约校验", deriv_reconciled))
            spot_qty = snap_to_increment(
                spot_base_from_derivative_fill(
                    derivative_filled_contracts=deriv_reconciled.filled_size,
                    derivative_instrument=deriv_inst,
                ),
                spot_inst.lot_size,
                "down",
            )
            if spot_qty <= 0:
                raise OkxApiError("合约平仓后换算现货数量为 0。")
            spot_reconciled = reconcile_fill(planned_size=spot_qty, filled_size=spot_filled, avg_price=spot_avg)
            self._logger(format_reconcile_message("平仓现货校验", spot_reconciled))
        else:
            deriv_config = _build_strategy_config(entry.derivative_inst_id, runtime)
            spot_config = _build_strategy_config(entry.spot_inst_id, runtime)
            deriv_ord_type = "limit" if request.use_limit_orders else "market"
            spot_ord_type = "limit" if request.use_limit_orders else "market"
            deriv_result = self._client.place_simple_order(
                credentials,
                deriv_config,
                inst_id=entry.derivative_inst_id,
                side="buy",
                size=planned_derivative_qty,
                ord_type=deriv_ord_type,
                price=deriv_price if deriv_ord_type == "limit" else None,
                reduce_only=True,
                pos_side="short",
                cl_ord_id=f"arb{uuid.uuid4().hex[:14]}",
            )
            deriv_filled, deriv_avg = _wait_order_fill(
                self._client,
                credentials=credentials,
                config=deriv_config,
                inst_id=entry.derivative_inst_id,
                ord_id=deriv_result.ord_id,
                expected_size=planned_derivative_qty,
                logger=self._logger,
                label="平仓合约腿",
            )
            deriv_reconciled = reconcile_fill(
                planned_size=planned_derivative_qty,
                filled_size=deriv_filled,
                avg_price=deriv_avg,
            )
            self._logger(format_reconcile_message("平仓合约校验", deriv_reconciled))

            spot_qty = snap_to_increment(
                spot_base_from_derivative_fill(
                    derivative_filled_contracts=deriv_reconciled.filled_size,
                    derivative_instrument=deriv_inst,
                ),
                spot_inst.lot_size,
                "down",
            )
            if spot_qty <= 0:
                raise OkxApiError("合约平仓后换算现货数量为 0。")

            spot_result = self._client.place_simple_order(
                credentials,
                spot_config,
                inst_id=entry.spot_inst_id,
                side="sell",
                size=spot_qty,
                ord_type=spot_ord_type,
                price=spot_price if spot_ord_type == "limit" else None,
                cl_ord_id=f"arb{uuid.uuid4().hex[:14]}",
            )
            spot_filled, spot_avg = _wait_order_fill(
                self._client,
                credentials=credentials,
                config=spot_config,
                inst_id=entry.spot_inst_id,
                ord_id=spot_result.ord_id,
                expected_size=spot_qty,
                logger=self._logger,
                label="平仓现货腿",
            )
            spot_reconciled = reconcile_fill(planned_size=spot_qty, filled_size=spot_filled, avg_price=spot_avg)
            self._logger(format_reconcile_message("平仓现货校验", spot_reconciled))

        pnl = estimate_cash_and_carry_pnl(
            spot_qty=spot_reconciled.filled_size,
            open_spot_price=entry.open_spot_price,
            close_spot_price=spot_avg,
            open_deriv_price=entry.open_derivative_price,
            close_deriv_price=deriv_avg,
            derivative_instrument=deriv_inst,
            derivative_qty=deriv_reconciled.filled_size,
        )
        closed_at = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
        remaining_derivative_qty = snap_to_increment(
            max(entry.derivative_qty - deriv_reconciled.filled_size, Decimal("0")),
            deriv_inst.lot_size,
            "down",
        )
        remaining_spot_qty = snap_to_increment(
            max(entry.spot_qty - spot_reconciled.filled_size, Decimal("0")),
            spot_inst.lot_size,
            "down",
        )
        has_remaining_pair = remaining_derivative_qty > 0 and remaining_spot_qty > 0
        if has_remaining_pair:
            open_entry = ArbitrageLedgerEntry(
                entry_id=entry.entry_id,
                base_ccy=entry.base_ccy,
                pair_kind=entry.pair_kind,
                spot_inst_id=entry.spot_inst_id,
                derivative_inst_id=entry.derivative_inst_id,
                spot_qty=remaining_spot_qty,
                derivative_qty=remaining_derivative_qty,
                open_spot_price=entry.open_spot_price,
                open_derivative_price=entry.open_derivative_price,
                close_spot_price=None,
                close_derivative_price=None,
                basis_at_open_pct=entry.basis_at_open_pct,
                fee_total=entry.fee_total,
                funding_total=entry.funding_total,
                realized_pnl=None,
                close_mode="open",
                opened_at=entry.opened_at,
                closed_at=None,
                notes=entry.notes,
            )
            upsert_ledger_entry(open_entry)
            closed_entry = ArbitrageLedgerEntry(
                entry_id=uuid.uuid4().hex,
                base_ccy=entry.base_ccy,
                pair_kind=entry.pair_kind,
                spot_inst_id=entry.spot_inst_id,
                derivative_inst_id=entry.derivative_inst_id,
                spot_qty=spot_reconciled.filled_size,
                derivative_qty=deriv_reconciled.filled_size,
                open_spot_price=entry.open_spot_price,
                open_derivative_price=entry.open_derivative_price,
                close_spot_price=spot_avg,
                close_derivative_price=deriv_avg,
                basis_at_open_pct=entry.basis_at_open_pct,
                fee_total=Decimal("0"),
                funding_total=Decimal("0"),
                realized_pnl=pnl,
                close_mode="partial",
                opened_at=entry.opened_at,
                closed_at=closed_at,
                notes=(entry.notes + " | 部分平仓") if entry.notes else "部分平仓",
            )
            upsert_ledger_entry(closed_entry)
        else:
            updated = ArbitrageLedgerEntry(
                entry_id=entry.entry_id,
                base_ccy=entry.base_ccy,
                pair_kind=entry.pair_kind,
                spot_inst_id=entry.spot_inst_id,
                derivative_inst_id=entry.derivative_inst_id,
                spot_qty=spot_reconciled.filled_size,
                derivative_qty=deriv_reconciled.filled_size,
                open_spot_price=entry.open_spot_price,
                open_derivative_price=entry.open_derivative_price,
                close_spot_price=spot_avg,
                close_derivative_price=deriv_avg,
                basis_at_open_pct=entry.basis_at_open_pct,
                fee_total=entry.fee_total,
                funding_total=entry.funding_total,
                realized_pnl=pnl,
                close_mode="full",
                opened_at=entry.opened_at,
                closed_at=closed_at,
                notes=(entry.notes + " | 已平仓") if entry.notes else "已平仓",
            )
            upsert_ledger_entry(updated)
        self._logger(f"{entry.base_ccy} 平仓完成，盈亏约 {format_decimal(pnl) if pnl is not None else '-'} USDT")
        return pnl

    def roll_cash_and_carry(
        self,
        request: ArbitrageRollRequest,
        *,
        runtime: ArbitrageTradeRuntime,
    ) -> ArbitrageRollResult:
        entry = find_ledger_entry(request.entry_id)
        if entry is None or entry.close_mode != "open":
            return ArbitrageRollResult(success=False, message="未找到可移仓的 open 套利持仓。")
        if entry.derivative_inst_id == request.target_derivative_inst_id:
            return ArbitrageRollResult(success=False, message="目标交割合约不能与当前合约相同。")
        try:
            current_inst = self._client.get_instrument(entry.derivative_inst_id)
            target_inst = self._client.get_instrument(request.target_derivative_inst_id)
            spot_inst = self._client.get_instrument(entry.spot_inst_id)
            roll_derivative_qty = self._resolve_roll_derivative_qty(entry, request, current_inst)
            planned_batches = _split_derivative_batches(
                roll_derivative_qty,
                instrument=current_inst,
                batch_count=request.batch_count,
                batch_contract_qty=request.batch_contract_qty,
            )
        except OkxApiError as exc:
            return ArbitrageRollResult(success=False, message=str(exc))
        except Exception as exc:
            return ArbitrageRollResult(success=False, message=f"移仓参数异常：{exc}")

        total_current_filled = Decimal("0")
        total_target_filled = Decimal("0")
        current_avg: Decimal | None = None
        target_avg: Decimal | None = None
        try:
            for index, batch_qty in enumerate(planned_batches, start=1):
                self._logger(f"交割合约移仓：第 {index}/{len(planned_batches)} 批，目标 {format_decimal(batch_qty)} 张")
                batch_request = replace(
                    request,
                    roll_derivative_qty=batch_qty,
                    batch_count=1,
                    batch_contract_qty=None,
                )
                batch_current_filled, batch_current_avg, batch_target_filled, batch_target_avg = self._roll_single_batch(
                    entry,
                    batch_request,
                    runtime=runtime,
                    current_inst=current_inst,
                    target_inst=target_inst,
                    planned_derivative_qty=batch_qty,
                )
                current_avg = self._blend_avg_price(current_avg, total_current_filled, batch_current_avg, batch_current_filled)
                target_avg = self._blend_avg_price(target_avg, total_target_filled, batch_target_avg, batch_target_filled)
                total_current_filled += batch_current_filled
                total_target_filled += batch_target_filled
        except OkxApiError as exc:
            return ArbitrageRollResult(
                success=False,
                message=(
                    f"移仓中断：已完成当前合约回补 {format_decimal(total_current_filled)} 张，"
                    f"目标合约开出 {format_decimal(total_target_filled)} 张。原因：{exc}"
                ),
                rolled_derivative_qty=total_current_filled,
                target_derivative_filled_qty=total_target_filled,
                current_derivative_avg_price=current_avg,
                target_derivative_avg_price=target_avg,
                entry_id=entry.entry_id,
            )
        except Exception as exc:
            return ArbitrageRollResult(
                success=False,
                message=f"移仓异常：{exc}",
                rolled_derivative_qty=total_current_filled,
                target_derivative_filled_qty=total_target_filled,
                current_derivative_avg_price=current_avg,
                target_derivative_avg_price=target_avg,
                entry_id=entry.entry_id,
            )

        rolled_spot_qty = (
            entry.spot_qty
            if total_current_filled >= entry.derivative_qty
            else snap_to_increment(
                entry.spot_qty * total_current_filled / max(entry.derivative_qty, Decimal("1e-18")),
                spot_inst.lot_size,
                "down",
            )
        )
        if rolled_spot_qty <= 0:
            rolled_spot_qty = entry.spot_qty
        remaining_derivative_qty = snap_to_increment(
            max(entry.derivative_qty - total_current_filled, Decimal("0")),
            current_inst.lot_size,
            "down",
        )
        remaining_spot_qty = snap_to_increment(
            max(entry.spot_qty - rolled_spot_qty, Decimal("0")),
            spot_inst.lot_size,
            "down",
        )
        moved_note = f"移仓：{entry.derivative_inst_id} -> {request.target_derivative_inst_id}"

        if remaining_derivative_qty > 0 and remaining_spot_qty > 0:
            updated_current = ArbitrageLedgerEntry(
                entry_id=entry.entry_id,
                base_ccy=entry.base_ccy,
                pair_kind=entry.pair_kind,
                spot_inst_id=entry.spot_inst_id,
                derivative_inst_id=entry.derivative_inst_id,
                spot_qty=remaining_spot_qty,
                derivative_qty=remaining_derivative_qty,
                open_spot_price=entry.open_spot_price,
                open_derivative_price=entry.open_derivative_price,
                close_spot_price=None,
                close_derivative_price=None,
                basis_at_open_pct=entry.basis_at_open_pct,
                fee_total=entry.fee_total,
                funding_total=entry.funding_total,
                realized_pnl=None,
                close_mode="open",
                opened_at=entry.opened_at,
                closed_at=None,
                notes=entry.notes,
            )
            upsert_ledger_entry(updated_current)
            new_entry = ArbitrageLedgerEntry(
                entry_id=uuid.uuid4().hex,
                base_ccy=entry.base_ccy,
                pair_kind=entry.pair_kind,
                spot_inst_id=entry.spot_inst_id,
                derivative_inst_id=request.target_derivative_inst_id,
                spot_qty=rolled_spot_qty,
                derivative_qty=total_target_filled,
                open_spot_price=entry.open_spot_price,
                open_derivative_price=target_avg,
                close_spot_price=None,
                close_derivative_price=None,
                basis_at_open_pct=entry.basis_at_open_pct,
                fee_total=Decimal("0"),
                funding_total=Decimal("0"),
                realized_pnl=None,
                close_mode="open",
                opened_at=datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
                closed_at=None,
                notes=(entry.notes + " | " + moved_note) if entry.notes else moved_note,
            )
            upsert_ledger_entry(new_entry)
        else:
            updated_entry = ArbitrageLedgerEntry(
                entry_id=entry.entry_id,
                base_ccy=entry.base_ccy,
                pair_kind=entry.pair_kind,
                spot_inst_id=entry.spot_inst_id,
                derivative_inst_id=request.target_derivative_inst_id,
                spot_qty=entry.spot_qty,
                derivative_qty=total_target_filled,
                open_spot_price=entry.open_spot_price,
                open_derivative_price=target_avg,
                close_spot_price=None,
                close_derivative_price=None,
                basis_at_open_pct=entry.basis_at_open_pct,
                fee_total=entry.fee_total,
                funding_total=entry.funding_total,
                realized_pnl=None,
                close_mode="open",
                opened_at=entry.opened_at,
                closed_at=None,
                notes=(entry.notes + " | " + moved_note) if entry.notes else moved_note,
            )
            upsert_ledger_entry(updated_entry)

        message = (
            f"移仓完成：回补 {entry.derivative_inst_id} {format_decimal(total_current_filled)} 张，"
            f"开出 {request.target_derivative_inst_id} {format_decimal(total_target_filled)} 张。"
        )
        self._logger(message)
        return ArbitrageRollResult(
            success=True,
            message=message,
            rolled_derivative_qty=total_current_filled,
            target_derivative_filled_qty=total_target_filled,
            current_derivative_avg_price=current_avg,
            target_derivative_avg_price=target_avg,
            entry_id=entry.entry_id,
        )

    def roll_cash_and_carry(
        self,
        request: ArbitrageRollRequest,
        *,
        runtime: ArbitrageTradeRuntime,
    ) -> ArbitrageRollResult:
        tracked_by_ledger = bool(request.entry_id)
        if request.entry_id:
            entry = find_ledger_entry(request.entry_id)
            if entry is None or entry.close_mode != "open":
                return ArbitrageRollResult(success=False, message="未找到可移仓的 open 套利持仓。")
        else:
            if not request.current_derivative_inst_id:
                return ArbitrageRollResult(success=False, message="缺少当前交割合约持仓，无法执行移仓。")
            if not request.spot_inst_id:
                return ArbitrageRollResult(success=False, message="缺少配对现货持仓，无法执行移仓。")
            current_derivative_qty = request.current_derivative_qty or request.roll_derivative_qty
            if current_derivative_qty is None or current_derivative_qty <= 0:
                return ArbitrageRollResult(success=False, message="缺少有效的当前交割合约持仓数量。")
            entry = ArbitrageLedgerEntry(
                entry_id="",
                base_ccy=(request.base_ccy or request.current_derivative_inst_id.split("-")[0]).strip().upper(),
                pair_kind="spot_future",
                spot_inst_id=request.spot_inst_id,
                derivative_inst_id=request.current_derivative_inst_id,
                spot_qty=max(request.spot_qty or Decimal("0"), Decimal("0")),
                derivative_qty=max(current_derivative_qty, Decimal("0")),
                open_spot_price=None,
                open_derivative_price=None,
                close_spot_price=None,
                close_derivative_price=None,
                basis_at_open_pct=None,
                fee_total=Decimal("0"),
                funding_total=Decimal("0"),
                realized_pnl=None,
                close_mode="open",
                opened_at=datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
                closed_at=None,
                notes="live_roll_source",
            )
        if entry.derivative_inst_id == request.target_derivative_inst_id:
            return ArbitrageRollResult(success=False, message="目标交割合约不能与当前合约相同。")
        try:
            current_inst = self._client.get_instrument(entry.derivative_inst_id)
            target_inst = self._client.get_instrument(request.target_derivative_inst_id)
            spot_inst = self._client.get_instrument(entry.spot_inst_id)
            roll_derivative_qty = self._resolve_roll_derivative_qty(entry, request, current_inst)
            planned_batches = _split_derivative_batches(
                roll_derivative_qty,
                instrument=current_inst,
                batch_count=request.batch_count,
                batch_contract_qty=request.batch_contract_qty,
            )
        except OkxApiError as exc:
            return ArbitrageRollResult(success=False, message=str(exc))
        except Exception as exc:
            return ArbitrageRollResult(success=False, message=f"移仓参数异常：{exc}")

        total_current_filled = Decimal("0")
        total_target_filled = Decimal("0")
        current_avg: Decimal | None = None
        target_avg: Decimal | None = None
        try:
            for index, batch_qty in enumerate(planned_batches, start=1):
                self._logger(f"交割合约移仓：第 {index}/{len(planned_batches)} 批，目标 {format_decimal(batch_qty)} 张")
                batch_request = replace(
                    request,
                    roll_derivative_qty=batch_qty,
                    batch_count=1,
                    batch_contract_qty=None,
                )
                batch_current_filled, batch_current_avg, batch_target_filled, batch_target_avg = self._roll_single_batch(
                    entry,
                    batch_request,
                    runtime=runtime,
                    current_inst=current_inst,
                    target_inst=target_inst,
                    planned_derivative_qty=batch_qty,
                )
                current_avg = self._blend_avg_price(current_avg, total_current_filled, batch_current_avg, batch_current_filled)
                target_avg = self._blend_avg_price(target_avg, total_target_filled, batch_target_avg, batch_target_filled)
                total_current_filled += batch_current_filled
                total_target_filled += batch_target_filled
        except OkxApiError as exc:
            return ArbitrageRollResult(
                success=False,
                message=(
                    f"移仓中断：已完成当前合约回补 {format_decimal(total_current_filled)} 张，"
                    f"目标合约开出 {format_decimal(total_target_filled)} 张。原因：{exc}"
                ),
                rolled_derivative_qty=total_current_filled,
                target_derivative_filled_qty=total_target_filled,
                current_derivative_avg_price=current_avg,
                target_derivative_avg_price=target_avg,
                entry_id=entry.entry_id or None,
            )
        except Exception as exc:
            return ArbitrageRollResult(
                success=False,
                message=f"移仓异常：{exc}",
                rolled_derivative_qty=total_current_filled,
                target_derivative_filled_qty=total_target_filled,
                current_derivative_avg_price=current_avg,
                target_derivative_avg_price=target_avg,
                entry_id=entry.entry_id or None,
            )

        if not tracked_by_ledger:
            message = (
                f"移仓完成：回补 {entry.derivative_inst_id} {format_decimal(total_current_filled)} 张，"
                f"开出 {request.target_derivative_inst_id} {format_decimal(total_target_filled)} 张。"
                "未更新本地套利账本（来源为当前现有持仓）。"
            )
            self._logger(message)
            return ArbitrageRollResult(
                success=True,
                message=message,
                rolled_derivative_qty=total_current_filled,
                target_derivative_filled_qty=total_target_filled,
                current_derivative_avg_price=current_avg,
                target_derivative_avg_price=target_avg,
                entry_id=None,
            )

        rolled_spot_qty = (
            entry.spot_qty
            if total_current_filled >= entry.derivative_qty
            else snap_to_increment(
                entry.spot_qty * total_current_filled / max(entry.derivative_qty, Decimal("1e-18")),
                spot_inst.lot_size,
                "down",
            )
        )
        if rolled_spot_qty <= 0:
            rolled_spot_qty = entry.spot_qty
        remaining_derivative_qty = snap_to_increment(
            max(entry.derivative_qty - total_current_filled, Decimal("0")),
            current_inst.lot_size,
            "down",
        )
        remaining_spot_qty = snap_to_increment(
            max(entry.spot_qty - rolled_spot_qty, Decimal("0")),
            spot_inst.lot_size,
            "down",
        )
        moved_note = f"移仓：{entry.derivative_inst_id} -> {request.target_derivative_inst_id}"

        if remaining_derivative_qty > 0 and remaining_spot_qty > 0:
            updated_current = ArbitrageLedgerEntry(
                entry_id=entry.entry_id,
                base_ccy=entry.base_ccy,
                pair_kind=entry.pair_kind,
                spot_inst_id=entry.spot_inst_id,
                derivative_inst_id=entry.derivative_inst_id,
                spot_qty=remaining_spot_qty,
                derivative_qty=remaining_derivative_qty,
                open_spot_price=entry.open_spot_price,
                open_derivative_price=entry.open_derivative_price,
                close_spot_price=None,
                close_derivative_price=None,
                basis_at_open_pct=entry.basis_at_open_pct,
                fee_total=entry.fee_total,
                funding_total=entry.funding_total,
                realized_pnl=None,
                close_mode="open",
                opened_at=entry.opened_at,
                closed_at=None,
                notes=entry.notes,
            )
            upsert_ledger_entry(updated_current)
            new_entry = ArbitrageLedgerEntry(
                entry_id=uuid.uuid4().hex,
                base_ccy=entry.base_ccy,
                pair_kind=entry.pair_kind,
                spot_inst_id=entry.spot_inst_id,
                derivative_inst_id=request.target_derivative_inst_id,
                spot_qty=rolled_spot_qty,
                derivative_qty=total_target_filled,
                open_spot_price=entry.open_spot_price,
                open_derivative_price=target_avg,
                close_spot_price=None,
                close_derivative_price=None,
                basis_at_open_pct=entry.basis_at_open_pct,
                fee_total=Decimal("0"),
                funding_total=Decimal("0"),
                realized_pnl=None,
                close_mode="open",
                opened_at=datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
                closed_at=None,
                notes=(entry.notes + " | " + moved_note) if entry.notes else moved_note,
            )
            upsert_ledger_entry(new_entry)
        else:
            updated_entry = ArbitrageLedgerEntry(
                entry_id=entry.entry_id,
                base_ccy=entry.base_ccy,
                pair_kind=entry.pair_kind,
                spot_inst_id=entry.spot_inst_id,
                derivative_inst_id=request.target_derivative_inst_id,
                spot_qty=entry.spot_qty,
                derivative_qty=total_target_filled,
                open_spot_price=entry.open_spot_price,
                open_derivative_price=target_avg,
                close_spot_price=None,
                close_derivative_price=None,
                basis_at_open_pct=entry.basis_at_open_pct,
                fee_total=entry.fee_total,
                funding_total=entry.funding_total,
                realized_pnl=None,
                close_mode="open",
                opened_at=entry.opened_at,
                closed_at=None,
                notes=(entry.notes + " | " + moved_note) if entry.notes else moved_note,
            )
            upsert_ledger_entry(updated_entry)

        message = (
            f"移仓完成：回补 {entry.derivative_inst_id} {format_decimal(total_current_filled)} 张，"
            f"开出 {request.target_derivative_inst_id} {format_decimal(total_target_filled)} 张。"
        )
        self._logger(message)
        return ArbitrageRollResult(
            success=True,
            message=message,
            rolled_derivative_qty=total_current_filled,
            target_derivative_filled_qty=total_target_filled,
            current_derivative_avg_price=current_avg,
            target_derivative_avg_price=target_avg,
            entry_id=entry.entry_id,
        )

    def _roll_single_batch(
        self,
        entry: ArbitrageLedgerEntry,
        request: ArbitrageRollRequest,
        *,
        runtime: ArbitrageTradeRuntime,
        current_inst,
        target_inst,
        planned_derivative_qty: Decimal,
    ) -> tuple[Decimal, Decimal | None, Decimal, Decimal | None]:
        credentials = runtime.credentials
        current_config = _build_strategy_config(entry.derivative_inst_id, runtime)
        target_config = _build_strategy_config(request.target_derivative_inst_id, runtime)
        derivative_pos_side = "short" if runtime.position_mode == "long_short" else None
        if request.execution_mode == "old_maker_new_taker":
            total_current_filled = Decimal("0")
            total_target_filled = Decimal("0")
            current_avg: Decimal | None = None
            target_avg: Decimal | None = None
            remaining_qty = planned_derivative_qty
            for attempt in range(max(0, request.chase_limit) + 1):
                if remaining_qty < current_inst.min_size:
                    break
                current_order = self._client.place_simple_order(
                    credentials,
                    current_config,
                    inst_id=entry.derivative_inst_id,
                    side="buy",
                    size=remaining_qty,
                    ord_type="post_only",
                    pos_side=derivative_pos_side,
                    price=self._resolve_roll_current_buy_price(request, current_inst),
                    reduce_only=True,
                    cl_ord_id=f"arb{uuid.uuid4().hex[:14]}",
                )
                current_filled, current_avg_once, current_done = self._wait_order_fill_until(
                    credentials=credentials,
                    config=current_config,
                    inst_id=entry.derivative_inst_id,
                    ord_id=current_order.ord_id,
                    expected_size=remaining_qty,
                    timeout_seconds=request.maker_wait_seconds,
                    label=f"移仓旧合约挂单腿 第 {attempt + 1} 次",
                )
                if not current_done:
                    try:
                        self._client.cancel_order(credentials, current_config, inst_id=entry.derivative_inst_id, ord_id=current_order.ord_id)
                    except Exception:
                        pass
                if current_filled <= 0:
                    if attempt >= request.chase_limit:
                        raise OkxApiError("旧合约挂单腿未成交，已达到最大追单次数。")
                    continue
                target_filled_once, target_avg_once = self._execute_taker_leg(
                    credentials=credentials,
                    config=target_config,
                    inst_id=request.target_derivative_inst_id,
                    side="sell",
                    size=current_filled,
                    label="移仓目标合约吃单腿",
                    pos_side=derivative_pos_side,
                )
                current_avg = self._blend_avg_price(current_avg, total_current_filled, current_avg_once, current_filled)
                target_avg = self._blend_avg_price(target_avg, total_target_filled, target_avg_once, target_filled_once)
                total_current_filled += current_filled
                total_target_filled += target_filled_once
                remaining_qty = max(planned_derivative_qty - total_current_filled, Decimal("0"))
            if total_current_filled <= 0 or total_target_filled <= 0:
                raise OkxApiError("当前未形成有效的交割合约移仓成交。")
            return total_current_filled, current_avg, total_target_filled, target_avg

        if request.execution_mode == "new_maker_old_taker":
            total_current_filled = Decimal("0")
            total_target_filled = Decimal("0")
            current_avg: Decimal | None = None
            target_avg: Decimal | None = None
            remaining_qty = planned_derivative_qty
            for attempt in range(max(0, request.chase_limit) + 1):
                if remaining_qty < target_inst.min_size:
                    break
                target_order = self._client.place_simple_order(
                    credentials,
                    target_config,
                    inst_id=request.target_derivative_inst_id,
                    side="sell",
                    size=remaining_qty,
                    ord_type="post_only",
                    pos_side=derivative_pos_side,
                    price=self._resolve_roll_target_sell_price(request, target_inst),
                    reduce_only=False,
                    cl_ord_id=f"arb{uuid.uuid4().hex[:14]}",
                )
                target_filled, target_avg_once, target_done = self._wait_order_fill_until(
                    credentials=credentials,
                    config=target_config,
                    inst_id=request.target_derivative_inst_id,
                    ord_id=target_order.ord_id,
                    expected_size=remaining_qty,
                    timeout_seconds=request.maker_wait_seconds,
                    label=f"移仓目标合约挂单腿 第 {attempt + 1} 次",
                )
                if not target_done:
                    try:
                        self._client.cancel_order(credentials, target_config, inst_id=request.target_derivative_inst_id, ord_id=target_order.ord_id)
                    except Exception:
                        pass
                if target_filled <= 0:
                    if attempt >= request.chase_limit:
                        raise OkxApiError("目标合约挂单腿未成交，已达到最大追单次数。")
                    continue
                current_filled_once, current_avg_once = self._execute_taker_leg(
                    credentials=credentials,
                    config=current_config,
                    inst_id=entry.derivative_inst_id,
                    side="buy",
                    size=target_filled,
                    label="移仓旧合约吃单腿",
                    pos_side=derivative_pos_side,
                    reduce_only=True,
                )
                current_avg = self._blend_avg_price(current_avg, total_current_filled, current_avg_once, current_filled_once)
                target_avg = self._blend_avg_price(target_avg, total_target_filled, target_avg_once, target_filled)
                total_current_filled += current_filled_once
                total_target_filled += target_filled
                remaining_qty = max(planned_derivative_qty - total_target_filled, Decimal("0"))
            if total_current_filled <= 0 or total_target_filled <= 0:
                raise OkxApiError("当前未形成有效的交割合约移仓成交。")
            return total_current_filled, current_avg, total_target_filled, target_avg

        current_ticker = self._client.get_ticker(entry.derivative_inst_id)
        target_ticker = self._client.get_ticker(request.target_derivative_inst_id)
        current_buy_price = self._resolve_roll_current_buy_price(request, current_inst, ticker=current_ticker)
        target_sell_price = self._resolve_roll_target_sell_price(request, target_inst, ticker=target_ticker)
        current_ord_type = "limit" if request.use_limit_orders else "market"
        target_ord_type = "limit" if request.use_limit_orders else "market"
        current_result = self._client.place_simple_order(
            credentials,
            current_config,
            inst_id=entry.derivative_inst_id,
            side="buy",
            size=planned_derivative_qty,
            ord_type=current_ord_type,
            price=current_buy_price if current_ord_type == "limit" else None,
            reduce_only=True,
            pos_side=derivative_pos_side,
            cl_ord_id=f"arb{uuid.uuid4().hex[:14]}",
        )
        current_filled, current_avg = _wait_order_fill(
            self._client,
            credentials=credentials,
            config=current_config,
            inst_id=entry.derivative_inst_id,
            ord_id=current_result.ord_id,
            expected_size=planned_derivative_qty,
            logger=self._logger,
            label="移仓旧合约腿",
        )
        if current_filled <= 0:
            raise OkxApiError("旧合约腿未成交。")
        target_result = self._client.place_simple_order(
            credentials,
            target_config,
            inst_id=request.target_derivative_inst_id,
            side="sell",
            size=current_filled,
            ord_type=target_ord_type,
            price=target_sell_price if target_ord_type == "limit" else None,
            reduce_only=False,
            pos_side=derivative_pos_side,
            cl_ord_id=f"arb{uuid.uuid4().hex[:14]}",
        )
        target_filled, target_avg = _wait_order_fill(
            self._client,
            credentials=credentials,
            config=target_config,
            inst_id=request.target_derivative_inst_id,
            ord_id=target_result.ord_id,
            expected_size=current_filled,
            logger=self._logger,
            label="移仓目标合约腿",
        )
        return current_filled, current_avg, target_filled, target_avg

    def _resolve_roll_derivative_qty(self, entry: ArbitrageLedgerEntry, request: ArbitrageRollRequest, instrument) -> Decimal:
        if request.roll_derivative_qty is None:
            return entry.derivative_qty
        requested_qty = snap_to_increment(request.roll_derivative_qty, instrument.lot_size, "down")
        if requested_qty <= 0:
            raise OkxApiError("移仓数量按合约最小变动单位向下取整后为 0，请加大数量。")
        if requested_qty > entry.derivative_qty:
            raise OkxApiError(f"移仓数量不能超过当前持仓 {format_decimal(entry.derivative_qty)} 张。")
        return requested_qty

    def _resolve_close_derivative_qty(self, entry: ArbitrageLedgerEntry, request: ArbitrageCloseRequest, instrument) -> Decimal:
        if request.close_derivative_qty is None:
            return entry.derivative_qty
        requested_qty = snap_to_increment(request.close_derivative_qty, instrument.lot_size, "down")
        if requested_qty <= 0:
            raise OkxApiError("平仓数量按合约最小变动单位向下取整后为 0，请加大数量。")
        if requested_qty > entry.derivative_qty:
            raise OkxApiError(f"平仓数量不能超过当前持仓 {format_decimal(entry.derivative_qty)} 张。")
        return requested_qty

    def _resolve_spot_buy_price(self, request: ArbitrageOpenRequest, instrument, ticker) -> Decimal:
        if request.spot_limit_price is not None and request.spot_limit_price > 0:
            return snap_to_increment(request.spot_limit_price, instrument.tick_size, "up")
        ask = ticker.ask
        if ask is None or ask <= 0:
            raise OkxApiError(f"{instrument.inst_id} 缺少卖一价。")
        raw = ask * (Decimal("1") + request.max_slippage)
        return snap_to_increment(raw, instrument.tick_size, "up")

    def _resolve_derivative_sell_price(self, request: ArbitrageOpenRequest, instrument, inst_id: str) -> Decimal:
        if request.derivative_limit_price is not None and request.derivative_limit_price > 0:
            return snap_to_increment(request.derivative_limit_price, instrument.tick_size, "down")
        ticker = self._client.get_ticker(inst_id)
        bid = ticker.bid
        if bid is None or bid <= 0:
            raise OkxApiError(f"{inst_id} 缺少买一价。")
        raw = bid * (Decimal("1") - request.max_slippage)
        return snap_to_increment(raw, instrument.tick_size, "down")

    def _resolve_derivative_buy_price(self, request: ArbitrageCloseRequest, instrument, ticker) -> Decimal:
        if request.derivative_limit_price is not None and request.derivative_limit_price > 0:
            return snap_to_increment(request.derivative_limit_price, instrument.tick_size, "up")
        ask = ticker.ask
        if ask is None or ask <= 0:
            raise OkxApiError(f"{instrument.inst_id} 缺少卖一价。")
        raw = ask * (Decimal("1") + request.max_slippage)
        return snap_to_increment(raw, instrument.tick_size, "up")

    def _resolve_spot_sell_price(self, request: ArbitrageCloseRequest, instrument, ticker) -> Decimal:
        if request.spot_limit_price is not None and request.spot_limit_price > 0:
            return snap_to_increment(request.spot_limit_price, instrument.tick_size, "down")
        bid = ticker.bid
        if bid is None or bid <= 0:
            raise OkxApiError(f"{instrument.inst_id} 缺少买一价。")
        raw = bid * (Decimal("1") - request.max_slippage)
        return snap_to_increment(raw, instrument.tick_size, "down")

    def _resolve_roll_current_buy_price(self, request: ArbitrageRollRequest, instrument, ticker=None) -> Decimal:
        if request.current_derivative_limit_price is not None and request.current_derivative_limit_price > 0:
            return snap_to_increment(request.current_derivative_limit_price, instrument.tick_size, "up")
        ticker = ticker or self._client.get_ticker(instrument.inst_id)
        ask = ticker.ask
        if ask is None or ask <= 0:
            raise OkxApiError(f"{instrument.inst_id} 缺少卖一价。")
        raw = ask * (Decimal("1") + request.max_slippage)
        return snap_to_increment(raw, instrument.tick_size, "up")

    def _resolve_roll_target_sell_price(self, request: ArbitrageRollRequest, instrument, ticker=None) -> Decimal:
        if request.target_derivative_limit_price is not None and request.target_derivative_limit_price > 0:
            return snap_to_increment(request.target_derivative_limit_price, instrument.tick_size, "down")
        ticker = ticker or self._client.get_ticker(instrument.inst_id)
        bid = ticker.bid
        if bid is None or bid <= 0:
            raise OkxApiError(f"{instrument.inst_id} 缺少买一价。")
        raw = bid * (Decimal("1") - request.max_slippage)
        return snap_to_increment(raw, instrument.tick_size, "down")
