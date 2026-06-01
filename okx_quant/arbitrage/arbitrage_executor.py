from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
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
from okx_quant.arbitrage.position_ledger import load_open_ledger_entries, upsert_ledger_entry
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
    spot_limit_price: Decimal | None
    derivative_limit_price: Decimal | None
    use_limit_orders: bool
    max_slippage: Decimal


@dataclass(frozen=True)
class ArbitrageCloseRequest:
    entry_id: str | None
    max_slippage: Decimal
    use_limit_orders: bool
    spot_limit_price: Decimal | None = None
    derivative_limit_price: Decimal | None = None


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


class ArbitrageExecutor:
    def __init__(self, client: OkxRestClient, *, logger: Logger | None = None) -> None:
        self._client = client
        self._logger = logger or (lambda _message: None)

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

        spot_price = self._resolve_spot_buy_price(request, spot_inst, spot_ticker)
        deriv_price = self._resolve_derivative_sell_price(request, deriv_inst, request.derivative_inst_id)

        self._logger(
            "套利开仓："
            f"现货买入 {format_decimal(preview.spot_base_qty)} @ {format_decimal(spot_price)}，"
            f"合约卖出 {format_decimal(preview.swap_contracts)} 张 @ {format_decimal(deriv_price)}"
        )

        spot_config = _build_strategy_config(request.spot_inst_id, runtime)
        deriv_config = _build_strategy_config(request.derivative_inst_id, runtime)
        spot_ord_type = "limit" if request.use_limit_orders else "market"
        deriv_ord_type = "limit" if request.use_limit_orders else "market"

        try:
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

        deriv_price = self._resolve_derivative_buy_price(request, deriv_inst, deriv_ticker)
        spot_price = self._resolve_spot_sell_price(request, spot_inst, spot_ticker)

        deriv_config = _build_strategy_config(entry.derivative_inst_id, runtime)
        spot_config = _build_strategy_config(entry.spot_inst_id, runtime)
        deriv_ord_type = "limit" if request.use_limit_orders else "market"
        spot_ord_type = "limit" if request.use_limit_orders else "market"

        self._logger(
            f"套利平仓 {entry.base_ccy}："
            f"合约买入 {format_decimal(entry.derivative_qty)} 张 @ {format_decimal(deriv_price)}，"
            f"现货卖出 {format_decimal(entry.spot_qty)} @ {format_decimal(spot_price)}"
        )

        deriv_result = self._client.place_simple_order(
            credentials,
            deriv_config,
            inst_id=entry.derivative_inst_id,
            side="buy",
            size=entry.derivative_qty,
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
            expected_size=entry.derivative_qty,
            logger=self._logger,
            label="平仓合约腿",
        )
        deriv_reconciled = reconcile_fill(
            planned_size=entry.derivative_qty,
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
