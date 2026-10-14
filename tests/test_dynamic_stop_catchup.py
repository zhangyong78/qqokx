from dataclasses import replace
from decimal import Decimal
from threading import Event
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from okx_quant.engine import DynamicStopTriggerState, FilledPosition, StrategyEngine
from okx_quant.models import Candle, Credentials, DynamicProtectionRule, Instrument, StrategyConfig
from okx_quant.okx_client import OkxApiError, OkxRestClient


D = Decimal
MINUTE = 60_000
HOUR = 60 * MINUTE
NOW = 12 * HOUR + 35 * MINUTE


@pytest.fixture(autouse=True)
def block_real_requests(monkeypatch):
    monkeypatch.setattr(OkxRestClient, "_request", Mock(side_effect=AssertionError("Real network access is forbidden in these tests")))


def candle(ts, high="105", low="95", *, confirmed=True):
    return Candle(ts, D("100"), D(high), D(low), D("100"), D("1"), confirmed)


class CandleClient:
    """Offline candle source; small pages exercise real cursor pagination."""

    def __init__(self, hours=(), minutes=(), page_size=300):
        self.rows = {"1H": list(hours), "1m": list(minutes)}
        self.page_size = page_size
        self.calls = []

    def page(self, source, inst_id, bar, limit, before=None):
        self.calls.append((source, inst_id, bar, before))
        rows = sorted(self.rows[bar], key=lambda row: row.ts)
        if before is not None:
            rows = [row for row in rows if row.ts < before]
        return rows[-min(limit, self.page_size):]

    def get_candles(self, inst_id, bar, limit):
        return self.page("last", inst_id, bar, limit)

    def get_candles_history_before(self, inst_id, bar, before_ts, limit):
        return self.page("last", inst_id, bar, limit, before_ts)

    def get_mark_price_candles(self, inst_id, bar, limit):
        return self.page("mark", inst_id, bar, limit)

    def get_mark_price_candles_before(self, inst_id, bar, before_ts, limit):
        return self.page("mark-recent", inst_id, bar, limit, before_ts)

    def get_mark_price_candles_history_before(self, inst_id, bar, before_ts, limit):
        return self.page("mark", inst_id, bar, limit, before_ts)

    def get_index_price_candles(self, inst_id, bar, limit):
        return self.page("index", inst_id, bar, limit)

    def get_index_price_candles_before(self, inst_id, bar, before_ts, limit):
        return self.page("index", inst_id, bar, limit, before_ts)


def config():
    return StrategyConfig(
        inst_id="BTC-USDT-SWAP", bar="1H", ema_period=21, atr_period=10,
        atr_stop_multiplier=D("2"), atr_take_multiplier=D("4"), order_size=D("1"),
        trade_mode="cross", signal_mode="both", position_mode="net", environment="demo",
        tp_sl_trigger_type="mark", dynamic_two_r_break_even=False,
        ema55_slope_lock_profit_trigger_r=4, dynamic_first_lock_r=1,
        dynamic_fee_offset_enabled=False,
        dynamic_protection_rules=(DynamicProtectionRule(4, "lock_profit", 1),),
    )


def position(direction="long"):
    return FilledPosition(
        "ord-1", "cl-1", "BTC-USDT-SWAP", "buy" if direction == "long" else "sell",
        "sell" if direction == "long" else "buy", None, D("1"), D("100"), HOUR,
    )


INSTRUMENT = Instrument("BTC-USDT-SWAP", "SWAP", D("0.1"), D("0.01"), D("0.01"), "live")
CREDS = Credentials("offline", "offline", "offline")


def step_engine(direction="long"):
    engine = StrategyEngine(CandleClient(), lambda _text: None)
    exchange_stop = [D("90") if direction == "long" else D("110")]

    def pending(*_args, **_kwargs):
        return SimpleNamespace(
            algo_id="algo-1", algo_client_order_id="acl-1",
            stop_loss_trigger_price=exchange_stop[0], stop_loss_trigger_price_type="mark",
        )

    engine._find_managed_position = Mock(return_value=SimpleNamespace(position=D("1")))
    engine._find_pending_algo_order_for_dynamic_stop = Mock(side_effect=pending)
    engine._notify_exchange_dynamic_stop_close = Mock()
    engine._stop_event = Event()
    engine._stop_event.wait = Mock(return_value=False)
    engine._amend_algo_order_with_recovery = Mock(
        side_effect=lambda *_args, **kwargs: exchange_stop.__setitem__(0, kwargs["new_stop_loss_trigger_price"])
    )
    return engine, exchange_stop


def run_step(engine, direction="long", strategy_config=None, **overrides):
    values = dict(
        trade_instrument=INSTRUMENT, position=position(direction), stop_loss_algo_cl_ord_id="acl-1",
        stop_loss_algo_id="algo-1", current_stop_loss=D("90") if direction == "long" else D("110"),
        initial_stop_loss=D("90") if direction == "long" else D("110"), next_trigger_r=4,
        risk_per_unit=D("10"), amend_failures=0,
    )
    values.update(overrides)
    return engine._monitor_exchange_dynamic_stop_once(CREDS, strategy_config or config(), **values)


@pytest.mark.parametrize("direction,expected", [("long", "145"), ("short", "55")])
def test_restore_excludes_pre_entry_and_unfinished_bars_and_covers_first_hour(direction, expected):
    entry = HOUR + 10 * MINUTE + 30_000
    client = CandleClient(
        hours=[candle(HOUR, "999", "1"), candle(2 * HOUR, "125", "75"),
               candle(12 * HOUR, "888", "2", confirmed=False)],
        minutes=[candle(HOUR + 10 * MINUTE, "999", "1"),
                 candle(HOUR + 11 * MINUTE, "145", "55"),
                 candle(NOW - MINUTE, "110", "90"),
                 candle(NOW, "999", "1", confirmed=False)],
    )
    engine = StrategyEngine(client, lambda _text: None)
    with patch("okx_quant.engine.time.time", return_value=NOW / 1000):
        extreme, cursor, count = engine._restore_dynamic_stop_favorable_price(
            inst_id=INSTRUMENT.inst_id, trigger_type="mark", direction=direction,
            entry_price=D("100"), entry_ts=entry,
        )
    assert extreme == D(expected)
    assert cursor <= NOW - MINUTE
    assert count == 1


@pytest.mark.parametrize("source,index_id", [("last", "BTC-USDT-SWAP"), ("mark", "BTC-USDT-SWAP"), ("index", "BTC-USDT")])
def test_catchup_uses_same_trigger_source_for_latest_and_history(source, index_id):
    client = CandleClient(minutes=[candle(NOW - MINUTE)])
    engine = StrategyEngine(client, lambda _text: None)
    engine._dynamic_stop_candle_page(INSTRUMENT.inst_id, source, "1m")
    engine._dynamic_stop_candle_page(INSTRUMENT.inst_id, source, "1m", before_ts=NOW)
    assert [(call[0], call[1]) for call in client.calls] == [(source, index_id), (source, index_id)]


def test_minute_scan_does_not_skip_a_late_confirmed_spike():
    start = NOW - 3 * MINUTE
    client = CandleClient(minutes=[candle(start, "145", confirmed=False), candle(start + MINUTE, "110")])
    engine = StrategyEngine(client, lambda _text: None)
    with patch("okx_quant.engine.time.time", return_value=NOW / 1000):
        _, cursor = engine._scan_dynamic_stop_minute_candles(
            inst_id=INSTRUMENT.inst_id, trigger_type="mark", last_candle_ts=start - MINUTE,
            minimum_candle_ts=HOUR, direction="long",
        )
        client.rows["1m"][0] = candle(start, "145", confirmed=True)
        extreme, new_cursor = engine._scan_dynamic_stop_minute_candles(
            inst_id=INSTRUMENT.inst_id, trigger_type="mark", last_candle_ts=cursor,
            minimum_candle_ts=HOUR, direction="long",
        )
    assert extreme == D("145")
    assert new_cursor == start + MINUTE


def test_minute_scan_keeps_cursor_before_missing_candle():
    start = NOW - 4 * MINUTE
    client = CandleClient(minutes=[candle(start, "105"), candle(start + 2 * MINUTE, "110")])
    engine = StrategyEngine(client, lambda _text: None)
    with patch("okx_quant.engine.time.time", return_value=NOW / 1000):
        _, cursor = engine._scan_dynamic_stop_minute_candles(
            inst_id=INSTRUMENT.inst_id, trigger_type="mark", last_candle_ts=start - MINUTE,
            minimum_candle_ts=HOUR, direction="long",
        )
        client.rows["1m"].append(candle(start + MINUTE, "145"))
        extreme, new_cursor = engine._scan_dynamic_stop_minute_candles(
            inst_id=INSTRUMENT.inst_id, trigger_type="mark", last_candle_ts=cursor,
            minimum_candle_ts=HOUR, direction="long",
        )
    assert extreme == D("145")
    assert new_cursor == start + 2 * MINUTE


@pytest.mark.parametrize("direction,expected", [("long", "145"), ("short", "55")])
def test_minute_scan_pages_back_across_gap(direction, expected):
    start = NOW - 10 * MINUTE
    rows = [candle(start + i * MINUTE, "105", "95") for i in range(10)]
    rows[1] = candle(start + MINUTE, "145", "55")
    client = CandleClient(minutes=rows, page_size=2)
    engine = StrategyEngine(client, lambda _text: None)
    with patch("okx_quant.engine.time.time", return_value=NOW / 1000):
        extreme, cursor = engine._scan_dynamic_stop_minute_candles(
            inst_id=INSTRUMENT.inst_id, trigger_type="mark", last_candle_ts=start - MINUTE,
            minimum_candle_ts=HOUR, direction=direction,
        )
    assert extreme == D(expected)
    assert cursor == NOW - MINUTE
    assert len(client.calls) > 1


@pytest.mark.parametrize("direction,peak,current,target", [("long", "145", "120", "110"), ("short", "55", "80", "90")])
def test_historical_trigger_amends_only_against_current_safe_price(direction, peak, current, target):
    engine, exchange_stop = step_engine(direction)
    engine._get_trigger_price_with_retry = Mock(return_value=D(current))
    result = run_step(engine, direction, favorable_price=D(peak))
    assert result.current_stop_loss == D(target)
    assert exchange_stop[0] == D(target)
    engine._amend_algo_order_with_recovery.assert_called_once()


@pytest.mark.parametrize("direction,peak,current", [("long", "145", "109"), ("short", "55", "91")])
def test_expired_historical_candidate_does_not_book_fill_or_advance_state(direction, peak, current):
    engine, exchange_stop = step_engine(direction)
    original = exchange_stop[0]
    engine._get_trigger_price_with_retry = Mock(return_value=D(current))
    result = run_step(engine, direction, favorable_price=D(peak))
    assert result.current_stop_loss == original
    assert result.next_trigger_r == 4
    engine._amend_algo_order_with_recovery.assert_not_called()
    engine._notify_exchange_dynamic_stop_close.assert_not_called()


def test_monitor_retains_live_spike_until_candle_confirmation_after_failed_amend():
    engine, exchange_stop = step_engine()
    engine._find_managed_position.side_effect = [SimpleNamespace(position=D("1")), SimpleNamespace(position=D("1")), None]
    engine._get_trigger_price_with_retry = Mock(side_effect=[D("145"), D("109"), D("111"), D("111")])
    with patch("okx_quant.engine.time.time", return_value=NOW / 1000):
        engine._monitor_exchange_dynamic_stop(
            CREDS, config(), trade_instrument=INSTRUMENT, position=position(), initial_stop_loss=D("90"),
            stop_loss_algo_id="algo-1", stop_loss_algo_cl_ord_id="acl-1",
        )
    assert exchange_stop[0] == D("110")
    engine._amend_algo_order_with_recovery.assert_called_once()


def test_success_ack_without_exchange_stop_update_does_not_advance_stage():
    engine, exchange_stop = step_engine()
    engine._get_trigger_price_with_retry = Mock(return_value=D("120"))
    engine._amend_algo_order_with_recovery = Mock(return_value=None)
    result = run_step(engine, favorable_price=D("145"))
    assert exchange_stop[0] == D("90")
    assert result.current_stop_loss == D("90")
    assert result.next_trigger_r == 4


def test_exchange_confirmed_stop_is_adopted_without_duplicate_amend():
    engine, exchange_stop = step_engine()
    exchange_stop[0] = D("110")
    engine._get_trigger_price_with_retry = Mock(return_value=D("120"))
    result = run_step(engine, favorable_price=D("145"))
    assert result.current_stop_loss == D("110")
    engine._amend_algo_order_with_recovery.assert_not_called()


def test_manual_management_never_amends_from_historical_peak():
    engine, exchange_stop = step_engine()
    engine.get_manual_trade_control = Mock(return_value=SimpleNamespace(management_mode="manual", stop_loss=D("95")))
    result = run_step(engine, favorable_price=D("999"))
    assert result.current_stop_loss == D("95")
    assert exchange_stop[0] == D("90")
    engine._amend_algo_order_with_recovery.assert_not_called()


def test_history_failure_leaves_real_time_monitor_running():
    engine, exchange_stop = step_engine()
    engine._dynamic_stop_candle_page = Mock(side_effect=OkxApiError("offline timeout"))
    engine._find_managed_position.side_effect = [SimpleNamespace(position=D("1")), None]
    engine._get_trigger_price_with_retry = Mock(return_value=D("145"))
    with patch("okx_quant.engine.time.time", return_value=NOW / 1000):
        engine._monitor_exchange_dynamic_stop(
            CREDS, config(), trade_instrument=INSTRUMENT, position=position(), initial_stop_loss=D("90"),
            stop_loss_algo_id="algo-1", stop_loss_algo_cl_ord_id="acl-1",
        )
    assert exchange_stop[0] == D("110")


def test_mark_history_page_uses_historical_endpoint():
    client = OkxRestClient()
    client._request = Mock(return_value={"data": [["1000", "100", "145", "95", "110", "1"]]})
    engine = StrategyEngine(client, lambda _text: None)
    rows = engine._dynamic_stop_candle_page("BTC-USDT-SWAP", "mark", "1H", before_ts=2000)
    assert rows[0].high == D("145")
    assert client._request.call_args.args[1] == "/api/v5/market/history-mark-price-candles"
    assert client._request.call_args.kwargs["params"]["limit"] == "100"


def test_index_candle_page_parses_confirmation_and_uses_api_limit():
    client = OkxRestClient()
    client._request = Mock(return_value={"data": [["2000", "100", "145", "95", "110", "0"], ["1000", "100", "125", "95", "110", "1"]]})
    rows = client.get_index_price_candles("BTC-USDT", "1m", 300)
    assert [row.ts for row in rows] == [1000, 2000]
    assert [row.confirmed for row in rows] == [True, False]
    assert client._request.call_args.kwargs["params"]["limit"] == "100"


def test_restore_does_not_use_future_entry_hour_minutes_even_if_marked_confirmed():
    client = CandleClient(minutes=[candle(NOW - MINUTE, "110"), candle(NOW + MINUTE, "999")])
    engine = StrategyEngine(client, lambda _text: None)
    with patch("okx_quant.engine.time.time", return_value=NOW / 1000):
        peak, _, _ = engine._restore_dynamic_stop_favorable_price(
            inst_id=INSTRUMENT.inst_id, trigger_type="mark", direction="long",
            entry_price=D("100"), entry_ts=NOW - 10 * MINUTE,
        )
    assert peak == D("110")


def test_restore_keeps_cursor_before_delayed_confirmation():
    rows = [candle(12 * HOUR + i * MINUTE, "105") for i in range(35)]
    rows[33] = candle(12 * HOUR + 33 * MINUTE, "145", confirmed=False)
    client = CandleClient(minutes=rows)
    engine = StrategyEngine(client, lambda _text: None)
    with patch("okx_quant.engine.time.time", return_value=NOW / 1000):
        _, cursor, _ = engine._restore_dynamic_stop_favorable_price(
            inst_id=INSTRUMENT.inst_id, trigger_type="mark", direction="long",
            entry_price=D("100"), entry_ts=HOUR,
        )
    assert cursor == 12 * HOUR + 32 * MINUTE


def test_monitor_recovers_missed_old_hourly_trigger_on_restart():
    engine, exchange_stop = step_engine()
    engine._client = CandleClient(hours=[candle(2 * HOUR, "145")])
    engine._find_managed_position.side_effect = [SimpleNamespace(position=D("1")), None]
    engine._get_trigger_price_with_retry = Mock(return_value=D("120"))
    with patch("okx_quant.engine.time.time", return_value=NOW / 1000):
        engine._monitor_exchange_dynamic_stop(
            CREDS, config(), trade_instrument=INSTRUMENT, position=position(), initial_stop_loss=D("90"),
            stop_loss_algo_id="algo-1", stop_loss_algo_cl_ord_id="acl-1",
        )
    assert exchange_stop[0] == D("110")


def test_monitor_retains_peak_when_refresh_price_read_fails():
    engine, exchange_stop = step_engine()
    engine._find_managed_position.side_effect = [SimpleNamespace(position=D("1")), SimpleNamespace(position=D("1")), None]
    engine._get_trigger_price_with_retry = Mock(side_effect=[D("145"), OkxApiError("The read operation timed out"), D("120"), D("120")])
    with patch("okx_quant.engine.time.time", return_value=NOW / 1000):
        engine._monitor_exchange_dynamic_stop(
            CREDS, config(), trade_instrument=INSTRUMENT, position=position(), initial_stop_loss=D("90"),
            stop_loss_algo_id="algo-1", stop_loss_algo_cl_ord_id="acl-1",
        )
    assert exchange_stop[0] == D("110")


@pytest.mark.parametrize("direction,peak,current,target", [("long", "145", "120", "110"), ("short", "55", "80", "90")])
def test_amend_failure_retains_observed_stage_for_next_poll(direction, peak, current, target):
    engine, exchange_stop = step_engine(direction)
    state = DynamicStopTriggerState(D("100"))
    engine._get_trigger_price_with_retry = Mock(side_effect=[D(peak), D(peak), D(current)])
    engine._amend_algo_order_with_recovery = Mock(side_effect=OkxApiError("service unavailable"))
    result = run_step(engine, direction, trigger_state=state)
    assert result.next_trigger_r == 4
    assert state.favorable_price == D(peak)
    engine._get_trigger_price_with_retry = Mock(return_value=D(current))
    engine._amend_algo_order_with_recovery = Mock(
        side_effect=lambda *_args, **kwargs: exchange_stop.__setitem__(0, kwargs["new_stop_loss_trigger_price"])
    )
    result = run_step(engine, direction, trigger_state=state, amend_failures=result.amend_failures)
    assert result.current_stop_loss == D(target)


def test_mismatched_exchange_trigger_source_prevents_amend():
    engine, _ = step_engine()
    engine._find_pending_algo_order_for_dynamic_stop = Mock(return_value=SimpleNamespace(
        algo_id="algo-1", algo_client_order_id="acl-1", stop_loss_trigger_price=D("90"), stop_loss_trigger_price_type="last",
    ))
    result = run_step(engine, favorable_price=D("145"))
    assert result.current_stop_loss == D("90")
    engine._amend_algo_order_with_recovery.assert_not_called()


def test_index_futures_id_maps_to_underlying_index():
    client = CandleClient()
    engine = StrategyEngine(client, lambda _text: None)
    engine._dynamic_stop_candle_page("BTC-USDT-261225", "index", "1m")
    assert client.calls[0][1] == "BTC-USDT"


def test_eth_recorded_spike_can_lock_one_r_after_retrace_without_synthetic_close():
    engine, exchange_stop = step_engine()
    exchange_stop[0] = D("1866.75")
    eth_config = replace(config(), inst_id="ETH-USDT-SWAP", dynamic_fee_offset_enabled=True,
                         dynamic_two_r_break_even=True, dynamic_break_even_trigger_r=1)
    eth_position = replace(position(), inst_id="ETH-USDT-SWAP", entry_price=D("1865.40"))
    eth_instrument = replace(INSTRUMENT, inst_id="ETH-USDT-SWAP", tick_size=D("0.01"))
    engine._get_trigger_price_with_retry = Mock(return_value=D("1928.19"))
    result = run_step(
        engine, strategy_config=eth_config, trade_instrument=eth_instrument, position=eth_position,
        current_stop_loss=D("1866.75"), initial_stop_loss=D("1847.29"), risk_per_unit=D("18.11"),
        favorable_price=D("1941.77"),
    )
    assert result.current_stop_loss == D("1884.86")
    assert exchange_stop[0] == D("1884.86")
    engine._notify_exchange_dynamic_stop_close.assert_not_called()


def test_unknown_entry_time_does_not_backfill_prices_from_before_unknown_position():
    engine, exchange_stop = step_engine()
    engine._dynamic_stop_candle_page = Mock(return_value=[candle(NOW - MINUTE, "999")])
    engine._find_managed_position.side_effect = [SimpleNamespace(position=D("1")), None]
    engine._get_trigger_price_with_retry = Mock(return_value=D("105"))
    with patch("okx_quant.engine.time.time", return_value=NOW / 1000):
        engine._monitor_exchange_dynamic_stop(
            CREDS, config(), trade_instrument=INSTRUMENT, position=replace(position(), entry_ts=0), initial_stop_loss=D("90"),
            stop_loss_algo_id="algo-1", stop_loss_algo_cl_ord_id="acl-1",
        )
    assert exchange_stop[0] == D("90")
    engine._dynamic_stop_candle_page.assert_not_called()
    engine._amend_algo_order_with_recovery.assert_not_called()
