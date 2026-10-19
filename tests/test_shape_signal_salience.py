from types import SimpleNamespace

import pytest

from roll_terminal_qt import kline_analysis_window as signals


def _candle(open_: float, high: float, low: float, close: float, ts: int = 0):
    return SimpleNamespace(
        open=open_, high=high, low=low, close=close, ts=ts, confirmed=True,
    )


def _refs():
    return [_candle(100, 100 + value, 100, 100 + value * 0.9) for value in range(3, 13)]


def _signal(index: int, candle_count: int):
    return SimpleNamespace(
        pattern_id="inside_bar", index=index, candle_count=candle_count,
        direction="long", ts=index, score=60, pattern_name="inside_bar",
    )


def _build(sample, monkeypatch, *, index: int, candle_count: int):
    monkeypatch.setattr(
        signals, "build_signal_replay_dataset",
        lambda *args, **kwargs: SimpleNamespace(signals=[_signal(index, candle_count)]),
    )
    return signals._build_replay_signal_markers(
        candles=sample, period="1H", ema15_values=[100.0] * len(sample),
        sma50_values=[100.0] * len(sample),
    )


def test_pattern_cores_share_the_ten_candles_before_the_pattern():
    candles = [_candle(100, 101, 100, 100) for _ in range(10)]
    candles.extend([_candle(100, 110, 100, 109), _candle(100, 101, 100, 100)])

    core = signals._signal_core_amplitude_candle(
        candles=candles, signal_index=11, candle_count=2, lookback=10,
        top_n=4, metric="range", pattern_start=10,
    )

    assert core == (10, 1)


@pytest.mark.parametrize("candle_count", [1, 2, 3])
@pytest.mark.parametrize(("candidate_range", "expected_rank"), [(9.5, 4), (9.0, None)])
def test_one_two_three_candle_patterns_use_prior_window_and_rank_boundary(
    monkeypatch, candle_count, candidate_range, expected_rank,
):
    refs = _refs()
    pattern = [
        _candle(100, 100 + candidate_range, 100, 100 + candidate_range - 0.1),
        *[_candle(100, 101, 100, 100.9) for _ in range(candle_count - 1)],
    ]
    sample = [*refs, *pattern]
    signal_index = len(sample) - 1

    markers = _build(sample, monkeypatch, index=signal_index, candle_count=candle_count)
    pattern_markers = [item for item in markers if item["pattern_id"] == "inside_bar"]

    if expected_rank is None:
        assert pattern_markers == []  # insertion rank 5 is outside the top 4.
        return
    assert len(pattern_markers) == 1
    assert pattern_markers[0]["core_range_rank"] == expected_rank
    assert pattern_markers[0]["core_index"] == len(refs)

    future = _candle(100, 200, 100, 199.9, ts=signal_index + 1)
    with_future = _build([*sample, future], monkeypatch, index=signal_index, candle_count=candle_count)
    existing = [item for item in with_future if item["pattern_id"] == "inside_bar"]
    assert existing[0]["core_range_rank"] == pattern_markers[0]["core_range_rank"]
    assert existing[0]["core_index"] == pattern_markers[0]["core_index"]


def test_range_rank_uses_conservative_ties_and_requires_ten_prior_candles():
    tied = [_candle(100, 110, 100, 101) for _ in range(10)]
    tied.append(_candle(100, 110, 100, 101))
    assert signals._signal_core_amplitude_candle(
        candles=tied, signal_index=10, candle_count=1, lookback=10,
        top_n=4, metric="range", pattern_start=10,
    ) is None
    short_history = tied[1:]
    assert signals._signal_core_amplitude_candle(
        candles=short_history, signal_index=9, candle_count=1, lookback=10,
        top_n=4, metric="range", pattern_start=9,
    ) is None


def test_body_rank_cannot_bypass_range_gate(monkeypatch):
    refs = [_candle(100, 110, 100, 101) for _ in range(10)]
    candidate = _candle(100, 109, 100, 108.9)
    assert _build([*refs, candidate], monkeypatch, index=10, candle_count=1) == []


def test_unconfirmed_reference_or_pattern_member_rejects_previously_valid_signal(monkeypatch):
    refs = _refs()
    pattern = [
        _candle(100, 109.5, 100, 109.4),
        _candle(100, 101, 100, 100.9),
    ]
    sample = [*refs, *pattern]
    assert _build(sample, monkeypatch, index=11, candle_count=2)

    unconfirmed_reference = [*sample]
    unconfirmed_reference[0] = _candle(100, 103, 100, 102.9)
    unconfirmed_reference[0].confirmed = False
    assert _build(unconfirmed_reference, monkeypatch, index=11, candle_count=2) == []

    unconfirmed_member = [*sample]
    unconfirmed_member[10] = _candle(100, 109.5, 100, 109.4)
    unconfirmed_member[10].confirmed = False
    assert _build(unconfirmed_member, monkeypatch, index=11, candle_count=2) == []


def test_invalid_signal_index_keeps_none_result():
    sample = _refs()
    assert signals._signal_core_amplitude_candle(
        candles=sample, signal_index=-1, candle_count=1, pattern_start=0,
    ) is None
    assert signals._signal_core_amplitude_candle(
        candles=sample, signal_index=len(sample), candle_count=1, pattern_start=0,
    ) is None

@pytest.mark.parametrize(
    ("side", "pattern_id", "direction"),
    [("lower", "long_lower_shadow", "long"), ("upper", "long_upper_shadow", "short")],
)
def test_shadow_pattern_requires_fifty_five_percent_range_and_mirrors(
    monkeypatch, side, pattern_id, direction,
):
    monkeypatch.setattr(
        signals, "build_signal_replay_dataset", lambda *args, **kwargs: SimpleNamespace(signals=[]),
    )
    candles = [_candle(100, 100.5, 99.5, 100) for _ in range(10)]
    if side == "lower":
        below = _candle(100, 102, 97.6, 101)  # 2.4 / 4.4 < 55%.
        above = _candle(100, 102, 97.5, 101)  # 2.5 / 4.5 > 55%.
    else:
        below = _candle(100, 103.4, 99, 101)  # 2.4 / 4.4 < 55%.
        above = _candle(100, 103.5, 99, 101)  # 2.5 / 4.5 > 55%.

    def build(candidate):
        sample = [*candles, candidate]
        return signals._build_replay_signal_markers(
            candles=sample, period="1H", ema15_values=[100.0] * len(sample),
            sma50_values=[100.0] * len(sample),
        )

    assert all(item["pattern_id"] != pattern_id for item in build(below))
    accepted = [item for item in build(above) if item["pattern_id"] == pattern_id]
    assert len(accepted) == 1
    assert accepted[0]["direction"] == direction
    assert "核心K前" in accepted[0]["text"] and "影线前" in accepted[0]["text"]
    assert "@MA50" in accepted[0]["text"]

