from __future__ import annotations

from decimal import Decimal
from unittest import TestCase

from okx_quant.okx_client import OkxPositionHistoryItem
from roll_terminal_qt.kline_analysis_window import (
    _best_parameter_indicator_specs,
    _build_kline_history_trade_markers,
    _filter_kline_history_trade_markers,
    _history_trade_markers_for_candles,
)


def _position(
    *,
    inst_id: str,
    direction: str,
    opened_at: int,
    update_time: int,
    open_price: str,
    close_price: str,
    pnl: str,
) -> OkxPositionHistoryItem:
    return OkxPositionHistoryItem(
        update_time=update_time,
        inst_id=inst_id,
        inst_type="SWAP",
        mgn_mode="cross",
        pos_side="net",
        direction=direction,
        open_avg_price=Decimal(open_price),
        close_avg_price=Decimal(close_price),
        close_size=Decimal("1"),
        pnl=Decimal(pnl),
        realized_pnl=Decimal(pnl),
        settle_pnl=None,
        raw={"cTime": str(opened_at), "uTime": str(update_time)},
    )


class KlineHistoryTradeMarkersTest(TestCase):
    def test_history_trade_markers_include_open_and_close_events(self) -> None:
        markers = _build_kline_history_trade_markers(
            [
                _position(
                    inst_id="ETH-USDT-SWAP", direction="long", opened_at=1_700_000_000_000,
                    update_time=1_700_000_300_000, open_price="1990", close_price="2000", pnl="12.34",
                ),
                _position(
                    inst_id="ETH-USDT-SWAP", direction="short", opened_at=1_700_003_600_000,
                    update_time=1_700_003_800_000, open_price="2020", close_price="2010", pnl="-5",
                ),
                _position(
                    inst_id="BTC-USDT-SWAP", direction="long", opened_at=1_700_000_000_000,
                    update_time=1_700_000_300_000, open_price="59900", close_price="60000", pnl="1",
                ),
            ],
            symbol="ETH-USDT-SWAP",
        )

        self.assertEqual([(item.event, item.direction) for item in markers], [("open", "long"), ("close", "long"), ("open", "short"), ("close", "short")])
        self.assertEqual(len(_filter_kline_history_trade_markers(markers, direction_filter="long")), 2)
        self.assertEqual(len(_filter_kline_history_trade_markers(markers, direction_filter="short")), 2)
        rendered = _history_trade_markers_for_candles(
            markers,
            [
                {"time": 1_700_000_000, "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1},
                {"time": 1_700_003_600, "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1},
            ],
        )
        self.assertEqual(
            [(item["index"], item["label"]) for item in rendered],
            [(0, "开多"), (0, "平多 +12.34"), (1, "开空"), (1, "平空 -5.00")],
        )
        self.assertEqual([item["color"] for item in rendered], ["#38bdf8", "#f97316", "#38bdf8", "#f97316"])

    def test_best_parameter_indicators_use_symbol_defaults_on_one_hour_only(self) -> None:
        specs = _best_parameter_indicator_specs("ETH-USDT-SWAP", "1H")

        self.assertIn(("最佳 EMA 21", "ema", 21, "#f59e0b"), specs)
        self.assertIn(("最佳 EMA 55", "ema", 55, "#8b5cf6"), specs)
        long_specs = _best_parameter_indicator_specs("ETH-USDT-SWAP", "1H", direction_filter="long")
        short_specs = _best_parameter_indicator_specs("ETH-USDT-SWAP", "1H", direction_filter="short")
        self.assertEqual({item[2] for item in long_specs}, {21, 55})
        self.assertEqual({item[2] for item in short_specs}, {61})
        self.assertEqual(short_specs[0][1], "ma")
        doge_specs = _best_parameter_indicator_specs("DOGE-USDT-SWAP", "1H")
        self.assertIn(("最佳 EMA 5", "ema", 5, "#f59e0b"), doge_specs)
        self.assertIn(("最佳 EMA 13", "ema", 13, "#8b5cf6"), doge_specs)
        self.assertNotEqual({item[2] for item in specs}, {item[2] for item in doge_specs})
        self.assertEqual(_best_parameter_indicator_specs("ETH-USDT-SWAP", "4H"), [])
