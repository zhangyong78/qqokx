from __future__ import annotations

from decimal import Decimal
import time

from okx_quant.area_shape_monitor import AreaShapeMonitor
from okx_quant.area_shape_trade import AreaShapeConfig, AreaShapePlan
from okx_quant.models import Candle


HOUR = 3_600_000


def candle(ts: int, close: str = "100", *, confirmed: bool = True) -> Candle:
    return Candle(ts, Decimal(close), Decimal(close), Decimal(close), Decimal(close), Decimal("1"), confirmed)


def plan(*, status: str = "monitoring") -> AreaShapePlan:
    result = AreaShapePlan.create(
        profile_name="demo-profile", environment="demo", symbol="BTC-USDT-SWAP",
        support_price=Decimal("95"), pressure_price=Decimal("120"),
        config=AreaShapeConfig(atr_period=2),
    )
    result.status = status
    result.last_candle_ts = HOUR
    result.activated_at = "test"
    result.expires_at_candle_ts = 100 * HOUR
    return result


class FakeClient:
    def __init__(self, candles: list[Candle] | None = None) -> None:
        self.candles = list(candles or [])
        self.watch_calls: list[object] = []
        self.history_calls = 0
        self.callback = None
        self.closed = False

    def get_candles_history(self, symbol: str, period: str, *, limit: int) -> list[Candle]:
        self.history_calls += 1
        assert symbol == "BTC-USDT-SWAP" and period == "1H" and limit == 240
        return list(self.candles)

    def watch_candle(self, key: object, callback: object):
        self.watch_calls.append(key)
        self.callback = callback
        return lambda: None

    def close(self) -> None:
        self.closed = True


def test_reconcile_runs_even_when_websocket_subscription_looks_healthy() -> None:
    saved: list[list[dict[str, object]]] = []
    client = FakeClient([candle(HOUR), candle(2 * HOUR), candle(3 * HOUR)])
    target = plan()
    monitor = AreaShapeMonitor(
        client_factory=lambda: client, store_load=lambda: [target.to_dict()],
        store_save=lambda records: saved.append(records),
    )
    monitor._rebuild_connections()
    key = (target.symbol, target.environment)
    assert key in monitor._subscriptions
    calls = client.history_calls
    monitor._last_reconcile[key] = 0.0

    monitor._retry_missing_subscriptions()

    assert client.history_calls == calls + 1
    restored = monitor.get_plan(target.plan_id)
    assert restored is not None
    assert restored.last_candle_ts == 3 * HOUR
    assert saved
    monitor.stop()


def test_no_event_paper_candle_cursor_change_is_persisted() -> None:
    saved: list[list[dict[str, object]]] = []
    target = plan(status="paper_position")
    target.paper_position = {
        "side": "long", "entry": "100", "stop": "90", "initial_stop": "90",
        "risk_distance": "10", "take_profit": "", "last_checked_ts": HOUR,
        "stage": "initial", "signal_key": "once", "entry_ts": HOUR,
    }
    monitor = AreaShapeMonitor(
        store_load=lambda: [target.to_dict()], store_save=lambda records: saved.append(records),
    )

    monitor._handle_candle(f"{target.symbol}|{target.environment}", candle(2 * HOUR, "101"))

    restored = monitor.get_plan(target.plan_id)
    assert restored is not None
    assert restored.paper_position is not None
    assert restored.paper_position["last_checked_ts"] == 2 * HOUR
    assert saved
    monitor.stop()


def test_live_last_price_cross_enters_direct_touch_paper_plan_at_the_line() -> None:
    saved: list[list[dict[str, object]]] = []
    target = AreaShapePlan.create(
        profile_name="demo-profile", environment="demo", symbol="BTC-USDT-SWAP",
        support_price=Decimal("95"), pressure_price=Decimal("120"),
        config=AreaShapeConfig(
            direction="long", entry_trigger_mode="direct_touch", stop_basis="fixed_distance",
            fixed_stop_distance=Decimal("5"), atr_period=2,
        ),
    )
    target.status = "monitoring"
    target.activation_candle_ts = HOUR
    target.last_candle_ts = HOUR
    target.expires_at_candle_ts = 100 * HOUR
    monitor = AreaShapeMonitor(
        store_load=lambda: [target.to_dict()], store_save=lambda records: saved.append(records),
    )
    key = (target.symbol, target.environment)
    monitor._candles[key] = [candle(2 * HOUR), candle(3 * HOUR)]

    monitor._handle_candle(
        f"{target.symbol}|{target.environment}",
        Candle(4 * HOUR, Decimal("100"), Decimal("100"), Decimal("94"), Decimal("94"), Decimal("1"), False),
    )

    restored = monitor.get_plan(target.plan_id)
    assert restored is not None
    assert restored.status == "paper_position"
    assert restored.paper_position is not None
    assert restored.paper_position["entry"] == "95"
    assert restored.paper_position["trigger_source"] == "live_last_price"
    assert saved
    monitor.stop()


def test_websocket_gap_blocks_later_candle_until_contiguous_rest_replay() -> None:
    saved: list[list[dict[str, object]]] = []
    target = plan()
    client = FakeClient([candle(HOUR), candle(2 * HOUR), candle(3 * HOUR)])
    monitor = AreaShapeMonitor(
        client_factory=lambda: client, store_load=lambda: [target.to_dict()],
        store_save=lambda records: saved.append(records),
    )
    key = (target.symbol, target.environment)
    monitor._candles[key] = [candle(HOUR)]

    monitor._handle_candle(f"{target.symbol}|{target.environment}", candle(3 * HOUR))

    assert target.last_candle_ts == HOUR
    assert key in monitor._catchup_required
    monitor._catch_up_key(key)
    restored = monitor.get_plan(target.plan_id)
    assert restored is not None
    assert restored.last_candle_ts == 3 * HOUR
    assert key not in monitor._catchup_required
    monitor.stop()


def test_worker_survives_rebuild_and_candle_processing_failures() -> None:
    messages: list[str] = []
    monitor = AreaShapeMonitor(store_load=lambda: [], store_save=lambda _: None)
    monitor.status_changed.connect(messages.append)
    attempts = 0

    def fail_first_rebuild() -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("temporary rebuild error")

    monitor._rebuild_connections = fail_first_rebuild  # type: ignore[method-assign]
    monitor.start()
    try:
        assert monitor._worker is not None
        monitor._worker.join(timeout=0.05)
        assert monitor._worker.is_alive()
        monitor._handle_candle = lambda *_: (_ for _ in ()).throw(RuntimeError("temporary candle error"))  # type: ignore[method-assign]
        monitor._queue.put_nowait(("BTC-USDT-SWAP|demo", candle(HOUR)))
        deadline = time.monotonic() + 1.0
        while not monitor._catchup_required and time.monotonic() < deadline:
            time.sleep(0.01)
        assert monitor._worker.is_alive()
        assert ("BTC-USDT-SWAP", "demo") in monitor._catchup_required
    finally:
        monitor.stop()


def test_demo_mode_is_frozen_atomically_before_paper_worker_observes_candle() -> None:
    saved: list[list[dict[str, object]]] = []
    target = plan(status="draft")
    target.last_candle_ts = 0
    monitor = AreaShapeMonitor(
        store_load=lambda: [target.to_dict()], store_save=lambda records: saved.append(records),
    )
    started = monitor.start_plan(target.plan_id, last_closed_candle_ts=HOUR,
                                 profile_name="demo-profile", environment="demo",
                                 execution_mode="demo_exchange")
    assert started.execution_mode == "demo_exchange"
    assert saved[-1][0]["execution_mode"] == "demo_exchange"
    key = (target.symbol, target.environment)
    monitor._candles[key] = [candle(HOUR)]

    from unittest.mock import patch
    with patch.object(AreaShapePlan, "observe", autospec=True) as observe:
        monitor._handle_candle(f"{target.symbol}|{target.environment}", candle(2 * HOUR))
    observe.assert_not_called()
    assert monitor.get_plan(target.plan_id).execution_mode == "demo_exchange"
    assert monitor.get_plan(target.plan_id).last_candle_ts == HOUR
    # Async preflight finalization is idempotent and cannot expose paper mode.
    assert monitor.mark_demo_execution(target.plan_id).execution_mode == "demo_exchange"
    monitor.stop()

