from __future__ import annotations

import unittest
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory

from okx_quant.models import Credentials, Instrument
from okx_quant.okx_client import (
    OkxAccountOverview,
    OkxApiError,
    OkxOrderBook,
    OkxOrderResult,
    OkxOrderStatus,
    OkxPosition,
    OkxTicker,
)
from okx_quant.smart_order import (
    STATUS_POSITION_LIMIT,
    STATUS_RECOVERABLE,
    STATUS_STOPPED,
    STATUS_WAIT_FILL,
    SmartOrderManager,
    SmartOrderRuntimeConfig,
    _SmartOrderTask,
    build_option_rule_ladder_prices,
    build_rule_ladder_prices,
    compute_next_grid_order_price,
    resolve_best_quote_price,
)


class _FakeClient:
    def __init__(self) -> None:
        self.ticker_calls = 0
        self.order_book_calls = 0
        self.placed_orders: list[dict[str, object]] = []
        self.aggressive_orders: list[dict[str, object]] = []
        self.canceled_orders: list[str] = []
        self.positions: list[OkxPosition] = []
        self.submit_exceptions: list[Exception | None] = []
        self.cancel_exceptions: list[Exception | None] = []
        self.order_results: list[dict[str, object]] = []
        self.order_lookup: dict[str, dict[str, object]] = {}
        self.account_overview = OkxAccountOverview(
            total_equity=None,
            adjusted_equity=None,
            isolated_equity=None,
            available_equity=None,
            unrealized_pnl=None,
            initial_margin=None,
            maintenance_margin=None,
            order_frozen=None,
            notional_usd=None,
            details=(),
            raw={},
        )

    def get_ticker(self, inst_id: str) -> OkxTicker:
        self.ticker_calls += 1
        return OkxTicker(
            inst_id=inst_id,
            last=Decimal("0.010"),
            bid=Decimal("0.0099"),
            ask=Decimal("0.0101"),
            mark=Decimal("0.01"),
            index=Decimal("0.01"),
            raw={},
        )

    def get_order_book(self, inst_id: str, depth: int = 50) -> OkxOrderBook:
        self.order_book_calls += 1
        return OkxOrderBook(
            inst_id=inst_id,
            bids=((Decimal("0.0100"), Decimal("8")), (Decimal("0.0099"), Decimal("5"))),
            asks=((Decimal("0.0101"), Decimal("9")), (Decimal("0.0102"), Decimal("4"))),
            raw={},
        )

    def place_simple_order(self, credentials, config, **kwargs) -> OkxOrderResult:  # noqa: ANN001
        self.placed_orders.append(kwargs)
        ord_id = f"ord{len(self.placed_orders):03d}"
        cl_ord_id = kwargs.get("cl_ord_id")
        order_result = self.order_results.pop(0) if self.order_results else {}
        status_payload = {
            "ord_id": ord_id,
            "state": order_result.get("state", "live"),
            "price": order_result.get("price", kwargs.get("price")),
            "avg_price": order_result.get("avg_price"),
            "size": order_result.get("size", kwargs.get("size")),
            "filled_size": order_result.get("filled_size", Decimal("0")),
        }
        self.order_lookup[ord_id] = status_payload
        if cl_ord_id:
            self.order_lookup[f"cl:{cl_ord_id}"] = status_payload
        submit_exc = self.submit_exceptions.pop(0) if self.submit_exceptions else None
        if submit_exc is not None:
            raise submit_exc
        return OkxOrderResult(
            ord_id=ord_id,
            cl_ord_id=cl_ord_id,
            s_code="0",
            s_msg="",
            raw={},
        )

    def get_order(self, credentials, config, *, inst_id: str, ord_id: str | None = None, cl_ord_id: str | None = None) -> OkxOrderStatus:  # noqa: ANN001
        key = ord_id or (f"cl:{cl_ord_id}" if cl_ord_id else None)
        if key and key in self.order_lookup:
            payload = self.order_lookup[key]
            return OkxOrderStatus(
                ord_id=str(payload.get("ord_id") or ord_id or "ord001"),
                state=str(payload.get("state") or "live"),
                side="buy",
                ord_type="limit",
                price=payload.get("price"),  # type: ignore[arg-type]
                avg_price=payload.get("avg_price"),  # type: ignore[arg-type]
                size=payload.get("size"),  # type: ignore[arg-type]
                filled_size=payload.get("filled_size"),  # type: ignore[arg-type]
                raw={},
            )
        return OkxOrderStatus(
            ord_id=ord_id or "ord001",
            state="canceled",
            side="buy",
            ord_type="limit",
            price=Decimal("0.0100"),
            avg_price=None,
            size=Decimal("1"),
            filled_size=Decimal("0"),
            raw={},
        )

    def cancel_order(self, credentials, config, *, inst_id: str, ord_id: str) -> OkxOrderResult:  # noqa: ANN001
        self.canceled_orders.append(ord_id)
        payload = self.order_lookup.get(ord_id)
        if payload is not None:
            payload["state"] = "canceled"
        cancel_exc = self.cancel_exceptions.pop(0) if self.cancel_exceptions else None
        if cancel_exc is not None:
            raise cancel_exc
        return OkxOrderResult(ord_id=ord_id, cl_ord_id=None, s_code="0", s_msg="", raw={})

    def place_aggressive_limit_order(self, credentials, config, instrument, **kwargs) -> OkxOrderResult:  # noqa: ANN001
        self.aggressive_orders.append({"instrument": instrument.inst_id, **kwargs})
        return self.place_simple_order(
            credentials,
            config,
            inst_id=instrument.inst_id,
            side=kwargs["side"],
            size=kwargs["size"],
            ord_type="ioc",
            pos_side=kwargs.get("pos_side"),
            price=Decimal("0.0101"),
            cl_ord_id=kwargs.get("cl_ord_id"),
        )

    def get_positions(self, credentials, *, environment: str, inst_type: str | None = None):  # noqa: ANN001
        if inst_type is None:
            return list(self.positions)
        return [item for item in self.positions if item.inst_type == inst_type]

    def get_account_overview(self, credentials, *, environment: str):  # noqa: ANN001
        return self.account_overview

    def get_trigger_price(self, inst_id: str, price_type: str):  # noqa: ANN001
        return Decimal("0.010")


def _make_runtime(*, position_mode: str = "net") -> SmartOrderRuntimeConfig:
    return SmartOrderRuntimeConfig(
        credentials=Credentials(api_key="a", secret_key="b", passphrase="c"),
        environment="demo",
        trade_mode="cross",
        position_mode=position_mode,  # type: ignore[arg-type]
    )


def _make_option_instrument(inst_id: str = "BTC-USD-TEST") -> Instrument:
    return Instrument(
        inst_id=inst_id,
        inst_type="OPTION",
        tick_size=Decimal("0.0001"),
        lot_size=Decimal("1"),
        min_size=Decimal("1"),
        state="live",
    )


def _make_swap_instrument(inst_id: str = "ETH-USDT-SWAP") -> Instrument:
    return Instrument(
        inst_id=inst_id,
        inst_type="SWAP",
        tick_size=Decimal("0.01"),
        lot_size=Decimal("0.01"),
        min_size=Decimal("0.01"),
        state="live",
    )


class SmartOrderLogicTests(unittest.TestCase):
    def test_manager_treats_remote_end_closed_as_transient_error(self) -> None:
        manager = SmartOrderManager(_FakeClient())

        self.assertTrue(
            manager._is_transient_error(RuntimeError("Remote end closed connection without response"))
        )

    def test_buy_fill_uses_long_step_for_next_sell(self) -> None:
        next_side, next_price = compute_next_grid_order_price(
            filled_side="buy",
            fill_price=Decimal("0.010"),
            long_step=Decimal("0.005"),
            short_step=Decimal("0.003"),
            tick_size=Decimal("0.0001"),
        )
        self.assertEqual("sell", next_side)
        self.assertEqual(Decimal("0.0150"), next_price)

    def test_sell_fill_uses_short_step_for_next_buy(self) -> None:
        next_side, next_price = compute_next_grid_order_price(
            filled_side="sell",
            fill_price=Decimal("0.015"),
            long_step=Decimal("0.005"),
            short_step=Decimal("0.003"),
            tick_size=Decimal("0.0001"),
        )
        self.assertEqual("buy", next_side)
        self.assertEqual(Decimal("0.0120"), next_price)

    def test_build_rule_ladder_prices_keeps_center_and_steps(self) -> None:
        prices = build_rule_ladder_prices(
            center_price=Decimal("1.00"),
            tick_size=Decimal("0.01"),
            levels_each_side=2,
        )
        self.assertEqual(
            [Decimal("1.02"), Decimal("1.01"), Decimal("1.00"), Decimal("0.99"), Decimal("0.98")],
            prices,
        )

    def test_build_option_rule_ladder_prices_uses_price_bands(self) -> None:
        prices = build_option_rule_ladder_prices(
            center_price=Decimal("0.0050"),
            levels_each_side=3,
        )
        self.assertEqual(
            [
                Decimal("0.0065"),
                Decimal("0.0060"),
                Decimal("0.0055"),
                Decimal("0.0050"),
                Decimal("0.0049"),
                Decimal("0.0048"),
                Decimal("0.0047"),
            ],
            prices,
        )

    def test_manager_maps_exchange_book_and_task_labels_into_rule_ladder(self) -> None:
        client = _FakeClient()
        with TemporaryDirectory() as temp_dir:
            manager = SmartOrderManager(client, storage_path=Path(temp_dir) / ".okx_quant_smart_order_tasks.json")
            try:
                instrument = Instrument(
                    inst_id="BTC-USD-TEST",
                    inst_type="OPTION",
                    tick_size=Decimal("0.0001"),
                    lot_size=Decimal("1"),
                    min_size=Decimal("1"),
                    state="live",
                )
                runtime = SmartOrderRuntimeConfig(
                    credentials=Credentials(api_key="a", secret_key="b", passphrase="c"),
                    environment="demo",
                    trade_mode="cross",
                    position_mode="net",
                )
                manager.set_contract(instrument)
                manager.ensure_market_snapshot(instrument, force=True)
                manager._tasks["G001"] = _SmartOrderTask(
                    task_id="G001",
                    task_type="grid",
                    inst_id=instrument.inst_id,
                    instrument=instrument,
                    runtime=runtime,
                    side="buy",
                    size=Decimal("1"),
                    active_order_price=Decimal("0.0100"),
                    active_order_size=Decimal("2"),
                    active_order_side="buy",
                    waiting_for_fill=True,
                )
                manager._tasks["G002"] = _SmartOrderTask(
                    task_id="G002",
                    task_type="grid",
                    inst_id=instrument.inst_id,
                    instrument=instrument,
                    runtime=runtime,
                    side="sell",
                    size=Decimal("1"),
                    active_order_price=Decimal("0.0101"),
                    active_order_size=Decimal("3"),
                    active_order_side="sell",
                    waiting_for_fill=True,
                )
                ladder = manager.build_ladder(
                    instrument,
                    levels_each_side=1,
                    price_increment=instrument.tick_size,
                )
                ladder_map = {item.price: item for item in ladder}
                self.assertEqual(Decimal("8"), ladder_map[Decimal("0.0100")].buy_working)
                self.assertEqual(Decimal("9"), ladder_map[Decimal("0.0101")].sell_working)
                self.assertEqual(("G001:买2",), ladder_map[Decimal("0.0100")].working_labels)
                self.assertEqual(("G002:卖3",), ladder_map[Decimal("0.0101")].working_labels)
                self.assertTrue(ladder_map[Decimal("0.0100")].is_last_price)
                self.assertTrue(ladder_map[Decimal("0.0100")].is_best_bid)
                self.assertTrue(ladder_map[Decimal("0.0101")].is_best_ask)
                self.assertEqual(1, client.ticker_calls)
                self.assertEqual(1, client.order_book_calls)
            finally:
                manager.destroy()

    def test_manager_supports_price_filter_bucketing(self) -> None:
        client = _FakeClient()
        with TemporaryDirectory() as temp_dir:
            manager = SmartOrderManager(client, storage_path=Path(temp_dir) / ".okx_quant_smart_order_tasks.json")
            try:
                instrument = Instrument(
                    inst_id="BTC-USD-TEST",
                    inst_type="OPTION",
                    tick_size=Decimal("0.0001"),
                    lot_size=Decimal("1"),
                    min_size=Decimal("1"),
                    state="live",
                )
                manager.set_contract(instrument)
                manager.ensure_market_snapshot(instrument, force=True)
                ladder = manager.build_ladder(
                    instrument,
                    levels_each_side=1,
                    price_increment=Decimal("0.001"),
                )
                ladder_map = {item.price: item for item in ladder}
                self.assertEqual([Decimal("0.011"), Decimal("0.010"), Decimal("0.009")], [item.price for item in ladder])
                self.assertEqual(Decimal("8"), ladder_map[Decimal("0.010")].buy_working)
                self.assertEqual(Decimal("13"), ladder_map[Decimal("0.011")].sell_working)
            finally:
                manager.destroy()

    def test_manager_uses_option_price_bands_in_auto_ladder(self) -> None:
        client = _FakeClient()
        with TemporaryDirectory() as temp_dir:
            manager = SmartOrderManager(client, storage_path=Path(temp_dir) / ".okx_quant_smart_order_tasks.json")
            try:
                instrument = _make_option_instrument()
                manager.set_contract(instrument)
                manager.ensure_market_snapshot(instrument, force=True)
                ladder = manager.build_ladder(instrument, levels_each_side=1, price_increment=None)
                self.assertEqual([Decimal("0.0105"), Decimal("0.0100"), Decimal("0.0095")], [item.price for item in ladder])
                ladder_map = {item.price: item for item in ladder}
                self.assertEqual(Decimal("13"), ladder_map[Decimal("0.0105")].sell_working)
                self.assertEqual(Decimal("13"), ladder_map[Decimal("0.0095")].buy_working)
                self.assertTrue(ladder_map[Decimal("0.0100")].is_last_price)
            finally:
                manager.destroy()

    def test_resolve_best_quote_price_uses_bid_for_buy_and_ask_for_sell(self) -> None:
        ticker = OkxTicker(
            inst_id="BTC-USDT",
            last=Decimal("10"),
            bid=Decimal("9.9"),
            ask=Decimal("10.1"),
            mark=Decimal("10"),
            index=Decimal("10"),
            raw={},
        )
        order_book = OkxOrderBook(
            inst_id="BTC-USDT",
            bids=((Decimal("9.8"), Decimal("1")),),
            asks=((Decimal("10.2"), Decimal("1")),),
            raw={},
        )
        self.assertEqual(
            Decimal("9.8"),
            resolve_best_quote_price(side="buy", ticker=ticker, order_book=order_book, tick_size=Decimal("0.1")),
        )
        self.assertEqual(
            Decimal("10.2"),
            resolve_best_quote_price(side="sell", ticker=ticker, order_book=order_book, tick_size=Decimal("0.1")),
        )

    def test_manager_restores_tasks_from_persistence_as_recoverable(self) -> None:
        client = _FakeClient()
        instrument = Instrument(
            inst_id="BTC-USD-TEST",
            inst_type="OPTION",
            tick_size=Decimal("0.0001"),
            lot_size=Decimal("1"),
            min_size=Decimal("1"),
            state="live",
        )
        runtime = SmartOrderRuntimeConfig(
            credentials=Credentials(api_key="a", secret_key="b", passphrase="c"),
            environment="demo",
            trade_mode="cross",
            position_mode="net",
        )
        with TemporaryDirectory() as temp_dir:
            storage_path = Path(temp_dir) / ".okx_quant_smart_order_tasks.json"
            manager = SmartOrderManager(client, storage_path=storage_path)
            try:
                manager.start_condition_task(
                    instrument=instrument,
                    runtime=runtime,
                    side="buy",
                    size=Decimal("1"),
                    trigger_inst_id=instrument.inst_id,
                    trigger_price_type="last",
                    trigger_direction="above",
                    trigger_price=Decimal("0.0110"),
                    exec_mode="limit",
                    exec_price=Decimal("0.0111"),
                    take_profit=None,
                    stop_loss=None,
                )
            finally:
                manager.destroy()

            recovered = SmartOrderManager(client, storage_path=storage_path)
            try:
                snapshots = recovered.list_tasks()
                self.assertEqual(1, len(snapshots))
                self.assertEqual("待恢复", snapshots[0].status)
                self.assertEqual(instrument.inst_id, snapshots[0].inst_id)
                self.assertEqual(instrument.inst_id, recovered.locked_inst_id)
            finally:
                recovered.destroy()

    def test_restart_recovered_grid_task_reuses_saved_price(self) -> None:
        client = _FakeClient()
        instrument = Instrument(
            inst_id="BTC-USD-TEST",
            inst_type="OPTION",
            tick_size=Decimal("0.0001"),
            lot_size=Decimal("1"),
            min_size=Decimal("1"),
            state="live",
        )
        runtime = SmartOrderRuntimeConfig(
            credentials=Credentials(api_key="a", secret_key="b", passphrase="c"),
            environment="demo",
            trade_mode="cross",
            position_mode="net",
        )
        with TemporaryDirectory() as temp_dir:
            storage_path = Path(temp_dir) / ".okx_quant_smart_order_tasks.json"
            manager = SmartOrderManager(client, storage_path=storage_path)
            try:
                task_id = manager.start_grid_task(
                    instrument=instrument,
                    runtime=runtime,
                    side="buy",
                    entry_price=Decimal("0.0100"),
                    size=Decimal("1"),
                    long_step=Decimal("0.005"),
                    short_step=Decimal("0.005"),
                    cycle_mode="continuous",
                    cycle_limit=None,
                )
            finally:
                manager.destroy()

            recovered = SmartOrderManager(client, storage_path=storage_path)
            try:
                recovered.restart_task(task_id, runtime)
                snapshots = recovered.list_tasks()
                self.assertEqual(STATUS_WAIT_FILL, snapshots[0].status)
                self.assertEqual(Decimal("0.0100"), snapshots[0].active_order_price)
            finally:
                recovered.destroy()

    def test_manager_clears_stale_lock_when_only_terminal_tasks_are_restored(self) -> None:
        client = _FakeClient()
        instrument = Instrument(
            inst_id="BTC-USD-TEST",
            inst_type="OPTION",
            tick_size=Decimal("0.0001"),
            lot_size=Decimal("1"),
            min_size=Decimal("1"),
            state="live",
        )
        runtime = SmartOrderRuntimeConfig(
            credentials=Credentials(api_key="a", secret_key="b", passphrase="c"),
            environment="demo",
            trade_mode="cross",
            position_mode="net",
        )
        with TemporaryDirectory() as temp_dir:
            storage_path = Path(temp_dir) / ".okx_quant_smart_order_tasks.json"
            manager = SmartOrderManager(client, storage_path=storage_path)
            try:
                task_id = manager.start_grid_task(
                    instrument=instrument,
                    runtime=runtime,
                    side="buy",
                    entry_price=Decimal("0.0100"),
                    size=Decimal("1"),
                    long_step=Decimal("0.005"),
                    short_step=Decimal("0.005"),
                    cycle_mode="continuous",
                    cycle_limit=None,
                )
                manager.stop_task(task_id, runtime)
                import time
                deadline = time.time() + 3.0
                while time.time() < deadline:
                    snapshots = manager.list_tasks()
                    if snapshots and snapshots[0].status == STATUS_STOPPED:
                        break
                    time.sleep(0.1)
            finally:
                manager.destroy()

            recovered = SmartOrderManager(client, storage_path=storage_path)
            try:
                snapshots = recovered.list_tasks()
                self.assertEqual(1, len(snapshots))
                self.assertEqual(STATUS_STOPPED, snapshots[0].status)
                self.assertIsNone(recovered.locked_inst_id)
                self.assertIsNone(recovered.locked_instrument)
            finally:
                recovered.destroy()

    def test_remove_task_deletes_stopped_task_and_unlocks(self) -> None:
        client = _FakeClient()
        instrument = Instrument(
            inst_id="BTC-USD-TEST",
            inst_type="OPTION",
            tick_size=Decimal("0.0001"),
            lot_size=Decimal("1"),
            min_size=Decimal("1"),
            state="live",
        )
        runtime = SmartOrderRuntimeConfig(
            credentials=Credentials(api_key="a", secret_key="b", passphrase="c"),
            environment="demo",
            trade_mode="cross",
            position_mode="net",
        )
        with TemporaryDirectory() as temp_dir:
            storage_path = Path(temp_dir) / ".okx_quant_smart_order_tasks.json"
            manager = SmartOrderManager(client, storage_path=storage_path)
            try:
                task_id = manager.start_grid_task(
                    instrument=instrument,
                    runtime=runtime,
                    side="buy",
                    entry_price=Decimal("0.0100"),
                    size=Decimal("1"),
                    long_step=Decimal("0.005"),
                    short_step=Decimal("0.005"),
                    cycle_mode="continuous",
                    cycle_limit=None,
                )
                manager.stop_task(task_id, runtime)
                import time
                deadline = time.time() + 3.0
                while time.time() < deadline:
                    snapshots = manager.list_tasks()
                    if snapshots and snapshots[0].status == STATUS_STOPPED:
                        break
                    time.sleep(0.1)
                manager.remove_task(task_id)
                self.assertEqual([], manager.list_tasks())
                self.assertIsNone(manager.locked_inst_id)
            finally:
                manager.destroy()

    def test_remove_task_rejects_running_task(self) -> None:
        client = _FakeClient()
        instrument = Instrument(
            inst_id="BTC-USD-TEST",
            inst_type="OPTION",
            tick_size=Decimal("0.0001"),
            lot_size=Decimal("1"),
            min_size=Decimal("1"),
            state="live",
        )
        runtime = SmartOrderRuntimeConfig(
            credentials=Credentials(api_key="a", secret_key="b", passphrase="c"),
            environment="demo",
            trade_mode="cross",
            position_mode="net",
        )
        with TemporaryDirectory() as temp_dir:
            storage_path = Path(temp_dir) / ".okx_quant_smart_order_tasks.json"
            manager = SmartOrderManager(client, storage_path=storage_path)
            try:
                task_id = manager.start_grid_task(
                    instrument=instrument,
                    runtime=runtime,
                    side="buy",
                    entry_price=Decimal("0.0100"),
                    size=Decimal("1"),
                    long_step=Decimal("0.005"),
                    short_step=Decimal("0.005"),
                    cycle_mode="continuous",
                    cycle_limit=None,
                )
                with self.assertRaisesRegex(RuntimeError, "不能直接删除"):
                    manager.remove_task(task_id)
            finally:
                manager.destroy()


    def test_opening_capacity_blocks_when_long_limit_exceeded(self) -> None:
        client = _FakeClient()
        client.positions = [
            OkxPosition(
                inst_id="BTC-USD-260331-66500-P",
                inst_type="OPTION",
                pos_side="net",
                mgn_mode="cross",
                position=Decimal("1"),
                avail_position=Decimal("1"),
                avg_price=None,
                mark_price=None,
                unrealized_pnl=None,
                unrealized_pnl_ratio=None,
                liquidation_price=None,
                leverage=None,
                margin_ccy=None,
                last_price=None,
                realized_pnl=None,
                margin_ratio=None,
                initial_margin=None,
                maintenance_margin=None,
                delta=None,
                gamma=None,
                vega=None,
                theta=None,
                raw={},
            )
        ]
        instrument = Instrument(
            inst_id="BTC-USD-260331-66500-P",
            inst_type="OPTION",
            tick_size=Decimal("0.0001"),
            lot_size=Decimal("1"),
            min_size=Decimal("1"),
            state="live",
        )
        runtime = SmartOrderRuntimeConfig(
            credentials=Credentials(api_key="a", secret_key="b", passphrase="c"),
            environment="demo",
            trade_mode="cross",
            position_mode="net",
        )
        with TemporaryDirectory() as temp_dir:
            manager = SmartOrderManager(client, storage_path=Path(temp_dir) / ".okx_quant_smart_order_tasks.json")
            try:
                manager.set_position_limits(enabled=True, long_limit=Decimal("1.5"), short_limit=Decimal("2"))
                with self.assertRaisesRegex(RuntimeError, "多头总仓位限制触发"):
                    manager.validate_opening_capacity(
                        instrument=instrument,
                        runtime=runtime,
                        side="buy",
                        size=Decimal("1"),
                    )
            finally:
                manager.destroy()

    def test_tp_sl_task_does_not_consume_position_limit(self) -> None:
        client = _FakeClient()
        instrument = Instrument(
                inst_id="BTC-USD-260331-66500-P",
            inst_type="OPTION",
            tick_size=Decimal("0.0001"),
            lot_size=Decimal("1"),
            min_size=Decimal("1"),
            state="live",
        )
        runtime = SmartOrderRuntimeConfig(
            credentials=Credentials(api_key="a", secret_key="b", passphrase="c"),
            environment="demo",
            trade_mode="cross",
            position_mode="net",
        )
        with TemporaryDirectory() as temp_dir:
            manager = SmartOrderManager(client, storage_path=Path(temp_dir) / ".okx_quant_smart_order_tasks.json")
            try:
                manager.set_position_limits(enabled=True, long_limit=Decimal("2"), short_limit=Decimal("2"))
                manager.start_tp_sl_task(
                    instrument=instrument,
                    runtime=runtime,
                    position_side="long",
                    size=Decimal("1"),
                    trigger_inst_id=instrument.inst_id,
                    trigger_price_type="last",
                    take_profit=Decimal("0.02"),
                    stop_loss=None,
                )
                state = manager.get_position_limit_state(instrument, runtime, force=True)
                self.assertEqual(Decimal("0"), state.reserved_long)
                self.assertEqual(Decimal("0"), state.reserved_short)
            finally:
                manager.destroy()

    def test_condition_task_freezes_and_recovers_when_position_limit_changes(self) -> None:
        client = _FakeClient()
        instrument = Instrument(
            inst_id="BTC-USD-260331-66500-P",
            inst_type="OPTION",
            tick_size=Decimal("0.0001"),
            lot_size=Decimal("1"),
            min_size=Decimal("1"),
            state="live",
        )
        runtime = SmartOrderRuntimeConfig(
            credentials=Credentials(api_key="a", secret_key="b", passphrase="c"),
            environment="demo",
            trade_mode="cross",
            position_mode="net",
        )
        with TemporaryDirectory() as temp_dir:
            manager = SmartOrderManager(client, storage_path=Path(temp_dir) / ".okx_quant_smart_order_tasks.json")
            try:
                manager.set_position_limits(enabled=True, long_limit=Decimal("1"), short_limit=Decimal("1"))
                task_id = manager.start_condition_task(
                    instrument=instrument,
                    runtime=runtime,
                    side="buy",
                    size=Decimal("1"),
                    trigger_inst_id=instrument.inst_id,
                    trigger_price_type="last",
                    trigger_direction="above",
                    trigger_price=Decimal("0.011"),
                    exec_mode="limit",
                    exec_price=Decimal("0.0112"),
                    take_profit=None,
                    stop_loss=None,
                )
                task = manager._tasks[task_id]
                client.positions = [
                    OkxPosition(
                        inst_id="BTC-USD-260331-66500-P",
                        inst_type="OPTION",
                        pos_side="net",
                        mgn_mode="cross",
                        position=Decimal("1"),
                        avail_position=Decimal("1"),
                        avg_price=None,
                        mark_price=None,
                        unrealized_pnl=None,
                        unrealized_pnl_ratio=None,
                        liquidation_price=None,
                        leverage=None,
                        margin_ccy=None,
                        last_price=None,
                        realized_pnl=None,
                        margin_ratio=None,
                        initial_margin=None,
                        maintenance_margin=None,
                        delta=None,
                        gamma=None,
                        vega=None,
                        theta=None,
                        raw={},
                    )
                ]
                manager._invalidate_actual_usage_cache()
                manager._tick_task(task)
                self.assertEqual(STATUS_POSITION_LIMIT, task.status)
                client.positions = []
                manager._invalidate_actual_usage_cache()
                manager._tick_task(task)
                self.assertEqual("等待触发", task.status)
            finally:
                manager.destroy()

    def test_position_limit_persistence_round_trip(self) -> None:
        client = _FakeClient()
        with TemporaryDirectory() as temp_dir:
            storage_path = Path(temp_dir) / ".okx_quant_smart_order_tasks.json"
            manager = SmartOrderManager(client, storage_path=storage_path)
            try:
                manager.set_position_limits(enabled=True, long_limit=Decimal("3"), short_limit=Decimal("2"))
            finally:
                manager.destroy()
            restored = SmartOrderManager(client, storage_path=storage_path)
            try:
                enabled, long_limit, short_limit = restored.get_position_limit_config()
                self.assertTrue(enabled)
                self.assertEqual(Decimal("3"), long_limit)
                self.assertEqual(Decimal("2"), short_limit)
            finally:
                restored.destroy()

    def test_submit_limit_order_recovers_when_response_is_lost(self) -> None:
        client = _FakeClient()
        client.submit_exceptions = [OkxApiError("SSL handshake timed out")]
        with TemporaryDirectory() as temp_dir:
            manager = SmartOrderManager(client, storage_path=Path(temp_dir) / ".okx_quant_smart_order_tasks.json")
            try:
                instrument = _make_option_instrument()
                runtime = _make_runtime()
                task = _SmartOrderTask(
                    task_id="G900",
                    task_type="grid",
                    inst_id=instrument.inst_id,
                    instrument=instrument,
                    runtime=runtime,
                    side="buy",
                    size=Decimal("1"),
                    initial_side="buy",
                    long_step=Decimal("0.001"),
                    short_step=Decimal("0.001"),
                )
                manager._submit_limit_order(
                    task,
                    side="buy",
                    price=Decimal("0.0100"),
                    size=Decimal("1"),
                    message_prefix="测试限价委托",
                )
                self.assertEqual(1, len(client.placed_orders))
                self.assertEqual("ord001", task.active_order_id)
                self.assertEqual(STATUS_WAIT_FILL, task.status)
                self.assertTrue(task.waiting_for_fill)
                self.assertIsNotNone(task.active_order_cl_ord_id)
            finally:
                manager.destroy()

    def test_handle_stop_recovers_when_cancel_response_is_lost(self) -> None:
        client = _FakeClient()
        with TemporaryDirectory() as temp_dir:
            manager = SmartOrderManager(client, storage_path=Path(temp_dir) / ".okx_quant_smart_order_tasks.json")
            try:
                instrument = _make_option_instrument()
                runtime = _make_runtime()
                task = _SmartOrderTask(
                    task_id="G901",
                    task_type="grid",
                    inst_id=instrument.inst_id,
                    instrument=instrument,
                    runtime=runtime,
                    side="buy",
                    size=Decimal("1"),
                    initial_side="buy",
                    long_step=Decimal("0.001"),
                    short_step=Decimal("0.001"),
                )
                manager._submit_limit_order(
                    task,
                    side="buy",
                    price=Decimal("0.0100"),
                    size=Decimal("1"),
                    message_prefix="测试停止撤单",
                )
                client.cancel_exceptions = [OkxApiError("read timed out")]
                task.stop_requested = True

                manager._handle_stop(task)

                self.assertEqual(STATUS_STOPPED, task.status)
                self.assertFalse(task.waiting_for_fill)
                self.assertIsNone(task.active_order_id)
                self.assertIsNone(task.active_order_cl_ord_id)
                self.assertIn("ord001", client.canceled_orders)
            finally:
                manager.destroy()

    def test_submit_aggressive_order_recovers_when_response_is_lost(self) -> None:
        client = _FakeClient()
        client.submit_exceptions = [OkxApiError("connection reset by peer")]
        with TemporaryDirectory() as temp_dir:
            manager = SmartOrderManager(client, storage_path=Path(temp_dir) / ".okx_quant_smart_order_tasks.json")
            try:
                instrument = _make_option_instrument()
                runtime = _make_runtime()
                task = _SmartOrderTask(
                    task_id="T902",
                    task_type="tp_sl",
                    inst_id=instrument.inst_id,
                    instrument=instrument,
                    runtime=runtime,
                    side="sell",
                    size=Decimal("1"),
                    protection_position_side="long",
                )
                manager._submit_aggressive_order(
                    task,
                    side="sell",
                    size=Decimal("1"),
                    message_prefix="测试止盈止损平仓",
                )
                self.assertEqual(1, len(client.aggressive_orders))
                self.assertEqual("ord001", task.active_order_id)
                self.assertEqual(STATUS_WAIT_FILL, task.status)
                self.assertTrue(task.waiting_for_fill)
                self.assertIsNotNone(task.active_order_cl_ord_id)
            finally:
                manager.destroy()

    def test_grid_long_short_uses_long_pos_side_for_open_and_reverse_orders(self) -> None:
        client = _FakeClient()
        with TemporaryDirectory() as temp_dir:
            manager = SmartOrderManager(client, storage_path=Path(temp_dir) / ".okx_quant_smart_order_tasks.json")
            try:
                instrument = _make_swap_instrument()
                runtime = _make_runtime(position_mode="long_short")
                task_id = manager.start_grid_task(
                    instrument=instrument,
                    runtime=runtime,
                    side="buy",
                    entry_price=Decimal("2300"),
                    size=Decimal("0.01"),
                    long_step=Decimal("10"),
                    short_step=Decimal("10"),
                    cycle_mode="counted",
                    cycle_limit=2,
                )
                task = manager._tasks[task_id]
                self.assertEqual("buy", client.placed_orders[0]["side"])
                self.assertEqual("long", client.placed_orders[0]["pos_side"])

                manager._handle_order_filled(
                    task,
                    OkxOrderStatus(
                        ord_id=task.active_order_id or "ord001",
                        state="filled",
                        side="buy",
                        ord_type="limit",
                        price=Decimal("2300"),
                        avg_price=Decimal("2300"),
                        size=Decimal("0.01"),
                        filled_size=Decimal("0.01"),
                        raw={},
                    ),
                )

                self.assertEqual(2, len(client.placed_orders))
                self.assertEqual("sell", client.placed_orders[1]["side"])
                self.assertEqual("long", client.placed_orders[1]["pos_side"])
            finally:
                manager.destroy()

    def test_grid_long_short_sell_side_uses_short_pos_side(self) -> None:
        client = _FakeClient()
        with TemporaryDirectory() as temp_dir:
            manager = SmartOrderManager(client, storage_path=Path(temp_dir) / ".okx_quant_smart_order_tasks.json")
            try:
                instrument = _make_swap_instrument()
                runtime = _make_runtime(position_mode="long_short")
                manager.start_grid_task(
                    instrument=instrument,
                    runtime=runtime,
                    side="sell",
                    entry_price=Decimal("2300"),
                    size=Decimal("0.01"),
                    long_step=Decimal("10"),
                    short_step=Decimal("10"),
                    cycle_mode="counted",
                    cycle_limit=1,
                )
                self.assertEqual("sell", client.placed_orders[0]["side"])
                self.assertEqual("short", client.placed_orders[0]["pos_side"])
            finally:
                manager.destroy()

    def test_option_grid_long_short_does_not_send_pos_side(self) -> None:
        client = _FakeClient()
        with TemporaryDirectory() as temp_dir:
            manager = SmartOrderManager(client, storage_path=Path(temp_dir) / ".okx_quant_smart_order_tasks.json")
            try:
                instrument = _make_option_instrument()
                runtime = _make_runtime(position_mode="long_short")
                task_id = manager.start_grid_task(
                    instrument=instrument,
                    runtime=runtime,
                    side="buy",
                    entry_price=Decimal("0.0100"),
                    size=Decimal("1"),
                    long_step=Decimal("0.0010"),
                    short_step=Decimal("0.0010"),
                    cycle_mode="counted",
                    cycle_limit=1,
                )
                task = manager._tasks[task_id]
                self.assertIsNone(client.placed_orders[0]["pos_side"])

                manager._handle_order_filled(
                    task,
                    OkxOrderStatus(
                        ord_id=task.active_order_id or "ord001",
                        state="filled",
                        side="buy",
                        ord_type="limit",
                        price=Decimal("0.0100"),
                        avg_price=Decimal("0.0100"),
                        size=Decimal("1"),
                        filled_size=Decimal("1"),
                        raw={},
                    ),
                )

                self.assertEqual(2, len(client.placed_orders))
                self.assertEqual("sell", client.placed_orders[1]["side"])
                self.assertIsNone(client.placed_orders[1]["pos_side"])
            finally:
                manager.destroy()

if __name__ == "__main__":
    unittest.main()
