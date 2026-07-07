from __future__ import annotations

import unittest
from decimal import Decimal
from types import SimpleNamespace

from scripts.run_moni_arbitrage_smoke import (
    _pair_close_target_derivative_qty,
    _pair_close_remaining_ok,
    _is_transient_busy_error,
    build_pair_close_harness,
    _is_pair_close_derivative_position,
    _cleanup_order_kwargs_for_position,
    _immediate_auto_open_spread_abs_max,
    _load_live_positions,
    _split_cleanup_sizes,
    resolve_moni_test_derivative_ids,
)
from okx_quant.okx_client import OkxPosition


class MoniSmokeScriptTest(unittest.TestCase):
    def test_resolve_moni_test_derivative_ids_prefers_current_live_swap_and_nearest_usd_future(self) -> None:
        open_inst_id, pair_close_inst_id = resolve_moni_test_derivative_ids(
            [
                "BTC-USD-260731",
                "BTC-USD-260710",
                "BTC-USD_UM-260710",
                "BTC-USDT-SWAP",
                "BTC-USD-SWAP",
            ]
        )

        self.assertEqual(open_inst_id, "BTC-USDT-SWAP")
        self.assertEqual(pair_close_inst_id, "BTC-USD-260710")

    def test_resolve_moni_test_derivative_ids_requires_usd_future_for_pair_close(self) -> None:
        with self.assertRaises(RuntimeError):
            resolve_moni_test_derivative_ids(
                [
                    "BTC-USDT-SWAP",
                    "BTC-USD-SWAP",
                ]
            )

    def test_immediate_auto_open_spread_abs_max_triggers_for_negative_spread(self) -> None:
        spread_abs = Decimal("-29.7")

        trigger_abs_max = _immediate_auto_open_spread_abs_max(spread_abs)

        self.assertLessEqual(trigger_abs_max, spread_abs)

    def test_immediate_auto_open_spread_abs_max_triggers_for_positive_spread(self) -> None:
        spread_abs = Decimal("12.4")

        trigger_abs_max = _immediate_auto_open_spread_abs_max(spread_abs)

        self.assertLessEqual(trigger_abs_max, spread_abs)

    def test_cleanup_order_kwargs_for_short_derivative_use_reduce_only(self) -> None:
        position = OkxPosition(
            inst_id="BTC-USD-260710",
            inst_type="FUTURES",
            pos_side="short",
            mgn_mode="cross",
            position=Decimal("20"),
            avail_position=Decimal("12"),
            avg_price=None,
            mark_price=None,
            unrealized_pnl=None,
            unrealized_pnl_ratio=None,
            liquidation_price=None,
            leverage=None,
            margin_ccy="BTC",
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

        kwargs = _cleanup_order_kwargs_for_position(position)

        assert kwargs is not None
        self.assertEqual(kwargs["side"], "buy")
        self.assertEqual(kwargs["size"], Decimal("12"))
        self.assertTrue(kwargs["reduce_only"])
        self.assertEqual(kwargs["pos_side"], "short")

    def test_cleanup_order_kwargs_for_spot_sell_without_reduce_only(self) -> None:
        position = OkxPosition(
            inst_id="BTC-USDT",
            inst_type="SPOT",
            pos_side="net",
            mgn_mode="cash",
            position=Decimal("0.02"),
            avail_position=Decimal("0.0199"),
            avg_price=None,
            mark_price=None,
            unrealized_pnl=None,
            unrealized_pnl_ratio=None,
            liquidation_price=None,
            leverage=None,
            margin_ccy="BTC",
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

        kwargs = _cleanup_order_kwargs_for_position(position)

        assert kwargs is not None
        self.assertEqual(kwargs["side"], "sell")
        self.assertEqual(kwargs["size"], Decimal("0.0199"))
        self.assertFalse(kwargs["reduce_only"])
        self.assertIsNone(kwargs["pos_side"])

    def test_pair_close_derivative_match_uses_pos_side_for_short_positions(self) -> None:
        position = OkxPosition(
            inst_id="BTC-USD-260710",
            inst_type="FUTURES",
            pos_side="short",
            mgn_mode="cross",
            position=Decimal("4"),
            avail_position=Decimal("4"),
            avg_price=None,
            mark_price=None,
            unrealized_pnl=None,
            unrealized_pnl_ratio=None,
            liquidation_price=None,
            leverage=None,
            margin_ccy="BTC",
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

        self.assertTrue(_is_pair_close_derivative_position(position, derivative_inst_id="BTC-USD-260710"))

    def test_split_cleanup_sizes_chunks_large_derivative_positions(self) -> None:
        self.assertEqual(_split_cleanup_sizes(Decimal("128"), inst_type="SWAP"), (Decimal("100"), Decimal("28")))
        self.assertEqual(_split_cleanup_sizes(Decimal("4"), inst_type="FUTURES"), (Decimal("4"),))

    def test_load_live_positions_bypasses_cached_private_snapshots(self) -> None:
        calls: list[tuple[str, bool]] = []
        derivative = OkxPosition(
            inst_id="BTC-USDT-SWAP",
            inst_type="SWAP",
            pos_side="short",
            mgn_mode="cross",
            position=Decimal("4"),
            avail_position=Decimal("4"),
            avg_price=None,
            mark_price=None,
            unrealized_pnl=None,
            unrealized_pnl_ratio=None,
            liquidation_price=None,
            leverage=None,
            margin_ccy="USDT",
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

        class _Client:
            def get_positions(self, credentials, *, environment: str, prefer_cache: bool = True):
                calls.append(("positions", prefer_cache))
                return [derivative]

            def get_account_overview(self, credentials, *, environment: str, prefer_cache: bool = True):
                calls.append(("overview", prefer_cache))
                return SimpleNamespace(details=[])

        runtime = SimpleNamespace(credentials=object(), environment="demo")

        positions = _load_live_positions(_Client(), runtime)

        self.assertEqual(positions, [derivative])
        self.assertEqual(calls, [("positions", False), ("overview", False)])

    def test_build_pair_close_harness_initializes_roll_auto_fields(self) -> None:
        harness = build_pair_close_harness(object(), SimpleNamespace(credentials=object(), environment="demo"))

        self.assertTrue(hasattr(harness, "_roll_auto_thread"))
        self.assertTrue(hasattr(harness, "_roll_auto_stop_event"))
        self.assertTrue(hasattr(harness, "_roll_auto_session"))

    def test_pair_close_target_derivative_qty_uses_live_max_closeable_amount(self) -> None:
        spot_position = OkxPosition(
            inst_id="BTC-USDT",
            inst_type="SPOT",
            pos_side="net",
            mgn_mode="cash",
            position=Decimal("0.0399"),
            avail_position=Decimal("0.0399"),
            avg_price=None,
            mark_price=None,
            unrealized_pnl=None,
            unrealized_pnl_ratio=None,
            liquidation_price=None,
            leverage=None,
            margin_ccy="BTC",
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
        derivative_position = OkxPosition(
            inst_id="BTC-USDT-SWAP",
            inst_type="SWAP",
            pos_side="short",
            mgn_mode="cross",
            position=Decimal("4"),
            avail_position=Decimal("4"),
            avg_price=None,
            mark_price=Decimal("63200"),
            unrealized_pnl=None,
            unrealized_pnl_ratio=None,
            liquidation_price=None,
            leverage=None,
            margin_ccy="USDT",
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
        spot_instrument = SimpleNamespace(lot_size=Decimal("0.0001"))
        derivative_instrument = SimpleNamespace(
            inst_type="SWAP",
            lot_size=Decimal("0.01"),
            min_size=Decimal("0.01"),
            ct_val=Decimal("0.01"),
            ct_mult=Decimal("1"),
            ct_val_ccy="BTC",
        )

        qty = _pair_close_target_derivative_qty(
            spot_position,
            derivative_position,
            spot_instrument=spot_instrument,
            derivative_instrument=derivative_instrument,
        )

        self.assertEqual(qty, Decimal("3.99"))

    def test_pair_close_remaining_ok_treats_min_lot_dust_as_flat(self) -> None:
        derivative = OkxPosition(
            inst_id="BTC-USDT-SWAP",
            inst_type="SWAP",
            pos_side="short",
            mgn_mode="cross",
            position=Decimal("0.01"),
            avail_position=Decimal("0.01"),
            avg_price=None,
            mark_price=None,
            unrealized_pnl=None,
            unrealized_pnl_ratio=None,
            liquidation_price=None,
            leverage=None,
            margin_ccy="USDT",
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
        spot = OkxPosition(
            inst_id="BTC-USDT",
            inst_type="SPOT",
            pos_side="net",
            mgn_mode="cash",
            position=Decimal("0.00006"),
            avail_position=Decimal("0.00006"),
            avg_price=None,
            mark_price=None,
            unrealized_pnl=None,
            unrealized_pnl_ratio=None,
            liquidation_price=None,
            leverage=None,
            margin_ccy="BTC",
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

        class _Client:
            def get_positions(self, credentials, *, environment: str, prefer_cache: bool = True):
                return [derivative]

            def get_account_overview(self, credentials, *, environment: str, prefer_cache: bool = True):
                return SimpleNamespace(
                    details=[
                        SimpleNamespace(
                            ccy="BTC",
                            available_balance=Decimal("0.00006"),
                            equity=Decimal("0.00006"),
                        )
                    ]
                )

            def get_instrument(self, inst_id: str):
                if inst_id == "BTC-USDT":
                    return SimpleNamespace(lot_size=Decimal("0.0001"))
                if inst_id == "BTC-USDT-SWAP":
                    return SimpleNamespace(
                        lot_size=Decimal("0.01"),
                        min_size=Decimal("0.01"),
                        inst_type="SWAP",
                        ct_val=Decimal("0.01"),
                        ct_mult=Decimal("1"),
                        ct_val_ccy="BTC",
                    )
                raise AssertionError(inst_id)

        ok, rows = _pair_close_remaining_ok(
            _Client(),
            SimpleNamespace(credentials=object(), environment="demo"),
            derivative_inst_id="BTC-USDT-SWAP",
        )

        self.assertTrue(ok)
        self.assertEqual(
            rows,
            [
                "BTC-USDT-SWAP | pos=0.01 | avail=0.01 | side=short",
                "BTC-USDT | pos=0.00006 | avail=0.00006 | side=net",
            ],
        )

    def test_is_transient_busy_error_recognizes_okx_50013(self) -> None:
        self.assertTrue(_is_transient_busy_error('失败：HTTP 500: {"code":"50013","msg":"当前系统繁忙，请稍后重试"}'))
        self.assertFalse(_is_transient_busy_error("普通失败"))


if __name__ == "__main__":
    unittest.main()
