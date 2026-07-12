from __future__ import annotations

from decimal import Decimal

from okx_quant.models import Candle
from okx_quant.okx_candle_ws import CandleStreamKey, CandleStreamState, _parse_okx_candle


def _candle(ts: int, close: str, *, confirmed: bool = False) -> Candle:
    return Candle(ts, Decimal("10"), Decimal("12"), Decimal("9"), Decimal(close), Decimal("3"), confirmed)


def test_candle_stream_replaces_open_candle_and_appends_next_candle() -> None:
    state = CandleStreamState([_candle(1000, "10")])

    state.apply(_candle(1000, "11"))
    state.apply(_candle(2000, "12"))

    assert [item.close for item in state.candles] == [Decimal("11"), Decimal("12")]


def test_parse_okx_candle_keeps_confirm_flag_and_subscription_key() -> None:
    candle = _parse_okx_candle(["1000", "10", "12", "9", "11", "3", "0", "0", "1"])

    assert candle == _candle(1000, "11", confirmed=True)
    assert CandleStreamKey("btc-usdt-swap", "1H", "demo").channel == "candle1H"
