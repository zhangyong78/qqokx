from __future__ import annotations

from decimal import Decimal

import pytest

from okx_quant.area_shape_trade import AreaShapeConfig, AreaShapePlan
from okx_quant.models import Candle


HOUR = 3_600_000


def _candle(ts: int, o: str, h: str, low: str, close: str, *, confirmed: bool = True) -> Candle:
    return Candle(ts, Decimal(o), Decimal(h), Decimal(low), Decimal(close), Decimal("1"), confirmed)


def _history(*, next_open_low: str = "96") -> list[Candle]:
    return [
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
    event = plan.observe(_history()[:3])
    assert event is not None and event["kind"] == "paper_pending"
    assert plan.status == "paper_pending"
    assert "entry" not in plan.paper_position
    assert plan.paper_position["signal_candle_ts"] == 2 * HOUR
    assert plan.paper_position["pattern"] == "long_lower_shadow"


def test_pattern_without_exact_entry_line_touch_is_ignored() -> None:
    plan = _plan()
    plan.support_price = Decimal("98")  # candle high is 97.2; former ATR zone alone must not count.
    event = plan.observe(_history()[:3])
    assert event is None
    assert plan.status == "monitoring"


def test_paper_fill_uses_future_next_open_and_not_signal_close() -> None:
    plan = _plan()
    plan.observe(_history()[:3])
    event = plan.observe(_history())
    assert event is not None and event["kind"] == "paper_entry"
    assert plan.paper_position["entry"] == "97.5"
    assert plan.paper_position["entry_ts"] == 3 * HOUR
    assert plan.paper_position["last_checked_ts"] == 3 * HOUR
    assert plan.paper_position["contracts"] is None
    assert "未核验" in plan.paper_position["sizing_status"]


def test_next_open_fill_checks_fill_bar_stop_conservatively() -> None:
    plan = _plan()
    plan.observe(_history()[:3])
    history = _history(next_open_low="91")
    plan.observe(history)
    assert plan.status == "paper_closed"
    assert plan.paper_position is None
    assert any(item["kind"] == "paper_stop" for item in plan.events)


def test_breakout_fill_bar_with_stop_is_closed_and_marked_ambiguous() -> None:
    config = AreaShapeConfig(atr_period=2, area_half_width_atr=Decimal("0.25"), entry_mode="breakout")
    plan = _plan(config)
    history = _history()
    plan.observe(history[:3])
    breakout = _candle(3 * HOUR, "97", "103", "90", "101")
    plan.observe([*history[:3], breakout])
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


def test_config_rejects_nonfinite_risk_unknown_price_source_and_bad_exit_thresholds() -> None:
    with pytest.raises(ValueError):
        AreaShapeConfig(risk_amount=Decimal("Infinity")).validate()
    with pytest.raises(ValueError):
        AreaShapeConfig(trigger_price_type="mark").validate()
    with pytest.raises(ValueError):
        AreaShapeConfig(be_trigger_r=Decimal("3"), lock_trigger_r=Decimal("3")).validate()


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
