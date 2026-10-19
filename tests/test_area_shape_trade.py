from __future__ import annotations

from decimal import Decimal

import pytest

from okx_quant.area_shape_trade import (
    AreaShapeConfig, AreaShapePlan, area_shape_lock_profit_r, area_shape_net_progress_r,
    area_shape_price_for_net_r, calculate_initial_stop,
)
from okx_quant.models import Candle


HOUR = 3_600_000


def _candle(ts: int, o: str, h: str, low: str, close: str, *, confirmed: bool = True) -> Candle:
    return Candle(ts, Decimal(o), Decimal(h), Decimal(low), Decimal(close), Decimal("1"), confirmed)


def _history(*, next_open_low: str = "96") -> list[Candle]:
    warmup = [
        _candle((index - 10) * HOUR, "100", "100.5", "99.5", "100")
        for index in range(10)
    ]
    return warmup + [
        _candle(0, "104", "105", "102", "103"),
        _candle(HOUR, "101", "102", "99", "100"),
        _candle(2 * HOUR, "96", "97.2", "93", "97"),  # confirmed hammer in the support area
        _candle(3 * HOUR, "97.5", "101", next_open_low, "100"),
    ]


def _plan(config: AreaShapeConfig | None = None) -> AreaShapePlan:
    plan = AreaShapePlan.create(
        profile_name="demo-profile", environment="demo", symbol="BTC-USDT-SWAP",
        support_price=Decimal("95"), pressure_price=Decimal("120"), config=config or AreaShapeConfig(atr_period=2, area_half_width_atr=Decimal("0.25")),
    )
    plan.activate(last_closed_candle_ts=HOUR, profile_name="demo-profile", environment="demo")
    return plan


def test_confirmed_area_pattern_only_creates_pending_paper_entry() -> None:
    plan = _plan()
    event = plan.observe(_history()[:-1])
    assert event is not None and event["kind"] == "paper_pending"
    assert plan.status == "paper_pending"
    assert "entry" not in plan.paper_position
    assert plan.paper_position["signal_candle_ts"] == 2 * HOUR
    assert plan.paper_position["pattern"] == "long_lower_shadow"


def test_pattern_without_exact_entry_line_touch_is_ignored() -> None:
    plan = _plan()
    plan.support_price = Decimal("98")  # candle high is 97.2; former ATR zone alone must not count.
    event = plan.observe(_history()[:-1])
    assert event is None
    assert plan.status == "monitoring"


@pytest.mark.parametrize("direction", ["long", "short"])
@pytest.mark.parametrize("trigger_mode", ["pattern", "direct_touch"])
def test_entry_requires_actual_line_touch_in_both_directions(direction, trigger_mode) -> None:
    config = AreaShapeConfig(direction=direction, entry_trigger_mode=trigger_mode, atr_period=2)
    plan = _plan(config)
    history = _history()[:-1]
    if direction == "long":
        plan.support_price = Decimal("98")
        exact_line = Decimal("97.2")
    else:
        history = [Candle(item.ts, 200 - item.open, 200 - item.low, 200 - item.high,
                          200 - item.close, item.volume, item.confirmed) for item in history]
        plan.support_price = Decimal("80")
        plan.pressure_price = Decimal("102")
        exact_line = Decimal("102.8")
    assert plan.observe(history) is None
    assert plan.status == "monitoring"
    plan.last_candle_ts = HOUR
    if direction == "long":
        plan.support_price = exact_line
    else:
        plan.pressure_price = exact_line
    event = plan.observe(history)
    assert event is not None and event["kind"] == "paper_pending"


def test_fixed_distance_direct_entry_does_not_wait_for_unused_atr_warmup() -> None:
    config = AreaShapeConfig(
        atr_period=200, entry_trigger_mode="direct_touch", stop_basis="fixed_distance",
        fixed_stop_distance=Decimal("5"),
    )
    plan = _plan(config)
    assert not config.requires_atr()
    event = plan.observe([_candle(2 * HOUR, "96", "97", "94", "96")])
    assert event is not None and event["kind"] == "paper_pending"
    assert plan.paper_position["stop"] == "90"
    event = plan.observe([
        _candle(2 * HOUR, "96", "97", "94", "96"),
        _candle(3 * HOUR, "96", "99", "95", "98"),
    ])
    assert event is not None and event["kind"] == "paper_entry"


def test_atr_target_buffer_still_requires_warmup_with_fixed_stop() -> None:
    config = AreaShapeConfig(
        atr_period=200, entry_trigger_mode="direct_touch", stop_basis="fixed_distance",
        fixed_stop_distance=Decimal("5"), target_buffer_atr=Decimal("0.5"),
    )
    plan = _plan(config)
    assert config.requires_atr()
    assert plan.observe([_candle(2 * HOUR, "96", "97", "94", "96")]) is None


def test_direct_touch_skips_pattern_check_and_three_candle_stop_uses_recent_extreme() -> None:
    config = AreaShapeConfig(
        atr_period=2, entry_trigger_mode="direct_touch", stop_basis="three_candle",
        signal_patterns=("double_reversal_up",), stop_offset_atr=Decimal("0.2"),
    )
    plan = _plan(config)
    history = [
        _candle(0, "104", "105", "102", "103"),
        _candle(HOUR, "101", "102", "99", "100"),
        _candle(2 * HOUR, "96", "97.2", "93", "97"),
    ]
    event = plan.observe(history)
    assert event is not None and event["kind"] == "paper_pending"
    assert plan.paper_position["pattern"] == "direct_touch"
    assert Decimal(plan.paper_position["stop"]) < Decimal("93")
    assert plan.paper_position["stop_basis"] == "three_candle"


@pytest.mark.parametrize(("direction", "entry_line", "expected_stop"), [
    ("long", "8095", "7095"),
    ("short", "8105", "9105"),
])
def test_fixed_stop_distance_uses_the_directional_entry_line(direction, entry_line, expected_stop) -> None:
    config = AreaShapeConfig(
        direction=direction, atr_period=2, entry_trigger_mode="direct_touch",
        stop_basis="fixed_distance", fixed_stop_distance=Decimal("1000"),
    )
    stop = calculate_initial_stop(
        config, direction, entry_reference=Decimal(entry_line),
        signal_candle=_candle(HOUR, "8100", "8120", "8080", "8100"),
        recent_candles=[], atr_value=Decimal("10"),
    )
    assert stop == Decimal(expected_stop)


def test_fee_offset_switch_roundtrips_and_old_saved_config_keeps_it_disabled() -> None:
    config = AreaShapeConfig(fee_offset_enabled=True)
    restored = AreaShapeConfig.from_dict(config.to_dict())
    assert restored.fee_offset_enabled is True
    old = config.to_dict()
    old.pop("fee_offset_enabled")
    assert AreaShapeConfig.from_dict(old).fee_offset_enabled is False


def test_old_fixed_target_config_keeps_break_even_disabled_and_new_setting_roundtrips() -> None:
    old = AreaShapeConfig().to_dict()
    old.pop("break_even_enabled")
    assert AreaShapeConfig.from_dict(old).break_even_enabled is False
    new = AreaShapeConfig(break_even_enabled=True, time_break_even_enabled=True, time_break_even_bars=8)
    restored = AreaShapeConfig.from_dict(new.to_dict())
    assert restored.break_even_enabled is True
    assert restored.time_break_even_enabled is True
    assert restored.time_break_even_bars == 8


def test_fixed_target_can_move_stop_to_break_even_without_changing_take_profit() -> None:
    config = AreaShapeConfig(
        break_even_enabled=True, be_trigger_r=Decimal("1"), fixed_tp_r=Decimal("10"),
        round_trip_fee_rate=Decimal("0"), fee_offset_enabled=False,
    )
    plan = _plan(config)
    plan.status = "paper_position"
    plan.paper_position = {
        "side": "long", "entry": "100", "stop": "95", "initial_stop": "95",
        "risk_distance": "5", "take_profit": "150", "last_checked_ts": HOUR,
        "stage": "initial", "entry_ts": HOUR,
    }
    event = plan.observe_paper_candle(_candle(2 * HOUR, "105", "106", "102", "105"))
    assert event is not None and event["kind"] == "paper_stop_tightened"
    assert Decimal(plan.paper_position["stop"]) == Decimal("100")
    assert plan.paper_position["take_profit"] == "150"
    assert plan.status == "paper_break_even"


def test_time_break_even_waits_for_elapsed_hour_and_nonnegative_net_progress() -> None:
    config = AreaShapeConfig(
        break_even_enabled=True, be_trigger_r=Decimal("50"), lock_trigger_r=Decimal("60"),
        time_break_even_enabled=True, time_break_even_bars=2,
        fee_offset_enabled=False,
    )
    plan = _plan(config)
    plan.status = "paper_position"
    plan.paper_position = {
        "side": "long", "entry": "100", "stop": "95", "initial_stop": "95",
        "risk_distance": "5", "take_profit": "150", "last_checked_ts": HOUR,
        "stage": "initial", "entry_ts": HOUR,
    }
    assert plan.observe_paper_candle(_candle(2 * HOUR, "100", "101", "99", "100")) is None
    event = plan.observe_paper_candle(_candle(3 * HOUR, "100", "101", "100.2", "100.5"))
    assert event is not None and event["kind"] == "paper_stop_tightened"
    assert Decimal(plan.paper_position["stop"]) == Decimal("100")


def test_paper_fill_uses_future_next_open_and_not_signal_close() -> None:
    plan = _plan()
    plan.observe(_history()[:-1])
    event = plan.observe(_history())
    assert event is not None and event["kind"] == "paper_entry"
    assert plan.paper_position["entry"] == "97.5"
    assert plan.paper_position["entry_ts"] == 3 * HOUR
    assert plan.paper_position["last_checked_ts"] == 3 * HOUR
    assert plan.paper_position["contracts"] is None
    assert "未核验" in plan.paper_position["sizing_status"]


@pytest.mark.parametrize("template,target_r", [("fixed_1r", Decimal(2)), ("lock_2r_at_3r", Decimal(15))])
def test_paper_exit_target_includes_fees_and_dynamic_remote_cap(template, target_r):
    config = AreaShapeConfig(atr_period=2, exit_template=template, fixed_tp_r=Decimal(2),
                             round_trip_fee_rate=Decimal("0.00036"), slippage_rate=Decimal(0))
    plan = _plan(config)
    plan.observe(_history()[:-1])
    plan.observe(_history())
    position = plan.paper_position
    assert position is not None
    progress = area_shape_net_progress_r(config, direction="long", entry=Decimal(position["entry"]),
        initial_stop=Decimal(position["initial_stop"]), price=Decimal(position["take_profit"]),
        fee_rate=config.round_trip_fee_rate)
    assert abs(progress-target_r) < Decimal("1e-20")


def test_next_open_fill_checks_fill_bar_stop_conservatively() -> None:
    plan = _plan()
    plan.observe(_history()[:-1])
    history = _history(next_open_low="91")
    plan.observe(history)
    assert plan.status == "paper_closed"
    assert plan.paper_position is None
    assert any(item["kind"] == "paper_stop" for item in plan.events)


def test_breakout_fill_bar_with_stop_is_closed_and_marked_ambiguous() -> None:
    config = AreaShapeConfig(atr_period=2, area_half_width_atr=Decimal("0.25"), entry_mode="breakout")
    plan = _plan(config)
    history = _history()
    plan.observe(history[:-1])
    breakout = _candle(3 * HOUR, "97", "103", "90", "101")
    plan.observe([*history[:-1], breakout])
    assert plan.status == "paper_closed"
    stopped = next(item for item in plan.events if item["kind"] == "paper_stop")
    assert stopped["ambiguous_bar"] is True


def test_ladder_keeps_triggered_stop_pending_when_bar_path_is_ambiguous() -> None:
    config = AreaShapeConfig(
        exit_template="lock_2r_at_3r", round_trip_fee_rate=Decimal("0.001"),
        atr_period=2,
    )
    plan = _plan(config)
    entry = Decimal("100")
    risk = Decimal("5")
    plan.status = "paper_position"
    plan.paper_position = {
        "side": "long", "entry": str(entry), "stop": "95", "initial_stop": "95",
        "risk_distance": str(risk), "take_profit": "", "last_checked_ts": HOUR,
        "stage": "initial", "signal_key": "one", "entry_ts": HOUR,
    }
    ambiguous = _candle(2 * HOUR, "100", "116", "108", "111")
    event = plan.observe_paper_candle(ambiguous)
    assert event is not None and event["kind"] == "paper_stop_pending"
    assert plan.paper_position["stop"] == "95"
    assert plan.paper_position["pending_stage"] == "locked"
    assert plan.status == "paper_position"

    retry = _candle(3 * HOUR, "111", "112", "110.5", "111")
    event = plan.observe_paper_candle(retry)
    assert event is not None and event["kind"] == "paper_stop_tightened"
    assert plan.paper_position["stage"] == "locked"
    assert Decimal(plan.paper_position["stop"]) > Decimal("110")


def test_stepped_lock_moves_one_r_for_each_one_r_after_lock_start() -> None:
    config = AreaShapeConfig(
        exit_template="lock_3r_at_4r", be_trigger_r=Decimal("1"),
        lock_trigger_r=Decimal("4"), lock_profit_r=Decimal("2"),
        lock_trail_every_r=Decimal("1"), lock_trail_add_r=Decimal("1"),
    )
    assert area_shape_lock_profit_r(config, Decimal("3.99")) == Decimal("2")
    assert area_shape_lock_profit_r(config, Decimal("4")) == Decimal("2")
    assert area_shape_lock_profit_r(config, Decimal("4.99")) == Decimal("2")
    assert area_shape_lock_profit_r(config, Decimal("5")) == Decimal("3")
    assert area_shape_lock_profit_r(config, Decimal("6")) == Decimal("4")


def test_fee_adjusted_trigger_and_lock_levels_use_net_r_for_both_directions() -> None:
    config = AreaShapeConfig(fee_offset_enabled=True)
    fee_rate = Decimal("0.00036")
    for direction, entry, initial_stop in (
        ("long", Decimal("100"), Decimal("95")),
        ("short", Decimal("100"), Decimal("105")),
    ):
        trigger_price = area_shape_price_for_net_r(
            entry=entry, initial_stop=initial_stop, direction=direction,
            target_r=Decimal("3"), fee_rate=fee_rate,
        )
        net_progress = area_shape_net_progress_r(
            config, direction=direction, entry=entry, initial_stop=initial_stop,
            price=trigger_price, fee_rate=fee_rate,
        )
        assert abs(net_progress - Decimal("3")) < Decimal("1e-24")
        lock_stop = area_shape_price_for_net_r(
            entry=entry, initial_stop=initial_stop, direction=direction,
            target_r=Decimal("2"), fee_rate=fee_rate,
        )
        lock_progress = area_shape_net_progress_r(
            config, direction=direction, entry=entry, initial_stop=initial_stop,
            price=lock_stop, fee_rate=fee_rate,
        )
        assert abs(lock_progress - Decimal("2")) < Decimal("1e-24")


def test_paper_position_moves_stop_from_two_r_to_three_r_at_five_r() -> None:
    config = AreaShapeConfig(
        exit_template="lock_3r_at_4r", be_trigger_r=Decimal("1"),
        lock_trigger_r=Decimal("4"), lock_profit_r=Decimal("2"), atr_period=2,
        fee_offset_enabled=False,
    )
    plan = _plan(config)
    plan.status = "paper_position"
    plan.paper_position = {
        "side": "long", "entry": "100", "stop": "95", "initial_stop": "95",
        "risk_distance": "5", "take_profit": "", "last_checked_ts": HOUR,
        "stage": "initial", "signal_key": "one", "entry_ts": HOUR,
    }

    first = plan.observe_paper_candle(_candle(2 * HOUR, "100", "120", "111", "115"))
    assert first is not None and first["kind"] == "paper_stop_tightened"
    assert plan.paper_position["stop"] == "110"

    second = plan.observe_paper_candle(_candle(3 * HOUR, "115", "125", "116", "122"))
    assert second is not None and second["kind"] == "paper_stop_tightened"
    assert plan.paper_position["stop"] == "115"


def test_config_rejects_nonfinite_risk_unknown_price_source_and_bad_exit_thresholds() -> None:
    with pytest.raises(ValueError):
        AreaShapeConfig(risk_amount=Decimal("Infinity")).validate()
    with pytest.raises(ValueError):
        AreaShapeConfig(trigger_price_type="mark").validate()
    with pytest.raises(ValueError):
        AreaShapeConfig(exit_template="lock_2r_at_3r", be_trigger_r=Decimal("3"),
                        lock_trigger_r=Decimal("3")).validate()
    with pytest.raises(ValueError):
        AreaShapeConfig(lock_trail_every_r=Decimal("0")).validate()


def test_plan_activation_is_bound_to_profile_and_environment() -> None:
    plan = AreaShapePlan.create(
        profile_name="demo-profile", environment="demo", symbol="BTC-USDT-SWAP",
        support_price=Decimal("95"), pressure_price=Decimal("120"), config=AreaShapeConfig(atr_period=2),
    )
    with pytest.raises(ValueError, match="account context"):
        plan.activate(last_closed_candle_ts=2 * HOUR, profile_name="other", environment="demo")


def test_plan_storage_restore_rejects_unknown_schema_and_context() -> None:
    plan = _plan()
    payload = plan.to_dict()
    payload["version"] = 99
    with pytest.raises(ValueError, match="version"):
        AreaShapePlan.from_dict(payload)
    payload = plan.to_dict()
    payload["environment"] = "unknown"
    with pytest.raises(ValueError, match="identity"):
        AreaShapePlan.from_dict(payload)
