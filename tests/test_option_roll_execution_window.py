from __future__ import annotations

import unittest
from decimal import Decimal
import json
import time
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch
from okx_quant.okx_client import OkxApiError, OkxTicker

from roll_terminal_qt.option_roll_execution_window import (
    OptionRollExecutionPlan,
    _OptionRollExecutionThread,
    _TrackedOrder,
    _mark_deviation_pct,
    _passive_option_price,
    _position_available,
    _require_fresh_market_timestamp,
    _snap_option_price,
    _validate_roll_contracts,
)


class OptionRollExecutionWindowTests(unittest.TestCase):
    def test_passive_price_uses_bid_for_buy_and_ask_for_sell(self) -> None:
        ticker = SimpleNamespace(inst_id="BTC-USD-261225-80000-C", bid=Decimal("0.01"), ask=Decimal("0.012"))
        book = SimpleNamespace(
            bids=((Decimal("0.0101"), Decimal("1")),),
            asks=((Decimal("0.0119"), Decimal("2")),),
        )

        self.assertEqual(_passive_option_price(side="buy", ticker=ticker, order_book=book), Decimal("0.0101"))
        self.assertEqual(_passive_option_price(side="sell", ticker=ticker, order_book=book), Decimal("0.0119"))

    def test_option_price_uses_matching_tick_band(self) -> None:
        instrument = SimpleNamespace(inst_id="BTC-USD-261225-80000-C", tick_size=Decimal("0.0001"))
        bands = (
            SimpleNamespace(min_price=Decimal("0"), max_price=Decimal("0.01"), tick_size=Decimal("0.00001")),
            SimpleNamespace(min_price=Decimal("0.01"), max_price=None, tick_size=Decimal("0.0001")),
        )

        self.assertEqual(
            _snap_option_price(Decimal("0.009876"), instrument, side="buy", tick_bands=bands),
            Decimal("0.00987"),
        )
        self.assertEqual(
            _snap_option_price(Decimal("0.01234"), instrument, side="sell", tick_bands=bands),
            Decimal("0.0124"),
        )

    def test_mark_deviation_is_percentage(self) -> None:
        self.assertEqual(_mark_deviation_pct(Decimal("0.011"), Decimal("0.01")), Decimal("10.0"))

    def test_market_timestamp_must_be_fresh(self) -> None:
        self.assertEqual(_require_fresh_market_timestamp({"ts": "10000"}, label="ticker", now_ms=20000), 10000)
        with self.assertRaisesRegex(ValueError, "延迟"):
            _require_fresh_market_timestamp({"ts": "1"}, label="order book", now_ms=20000)

    def test_load_quote_uses_mark_price_endpoint_and_checks_all_timestamps(self) -> None:
        class Client:
            instrument = SimpleNamespace(inst_id="BTC-USD-261225-80000-C", inst_type="OPTION", state="live")
            ticker = OkxTicker(inst_id=instrument.inst_id, last=None, bid=Decimal("0.01"), ask=Decimal("0.02"), mark=None, index=None, raw={"ts": "100000"})
            book = SimpleNamespace(bids=((Decimal("0.01"), Decimal("1")),), asks=((Decimal("0.02"), Decimal("1")),), raw={"ts": "100000"})

            def get_instrument(self, inst_id):
                return self.instrument

            def get_ticker(self, inst_id):
                return self.ticker

            def get_order_book(self, inst_id, depth):
                return self.book

            def get_mark_price_snapshot(self, inst_id):
                return Decimal("0.015"), 100000

        with TemporaryDirectory() as directory, patch("roll_terminal_qt.option_roll_execution_window.time.time", return_value=100.0):
            worker = self._worker(Client(), Path(directory) / "journal.json")
            _, ticker, _ = worker._load_quote("BTC-USD-261225-80000-C")
            self.assertEqual(ticker.mark, Decimal("0.015"))

    def test_zero_available_position_does_not_fall_back_to_total(self) -> None:
        position = SimpleNamespace(position=Decimal("5"), avail_position=Decimal("0"))
        self.assertEqual(_position_available(position), Decimal("0"))
        position.avail_position = None
        self.assertEqual(_position_available(position), Decimal("0"))

    def test_roll_rejects_different_strike_or_option_type(self) -> None:
        def instrument(inst_id: str) -> SimpleNamespace:
            return SimpleNamespace(inst_id=inst_id, inst_type="OPTION", state="live", ct_val=Decimal("1"), ct_mult=Decimal("1"), ct_val_ccy="BTC")

        with self.assertRaisesRegex(ValueError, "同行权价"):
            _validate_roll_contracts(instrument("BTC-USD-261225-80000-C"), instrument("BTC-USD-270326-90000-C"))

    def _worker(self, client: object, journal: Path) -> _OptionRollExecutionThread:
        runtime = SimpleNamespace(
            credentials=SimpleNamespace(profile_name="test", api_key="test"), environment="demo",
        )
        plan = OptionRollExecutionPlan(
            "BTC-USD-261225-80000-C", "BTC-USD-270326-80000-C", "long",
            Decimal("1"), 120.0, Decimal("5"),
        )
        worker = _OptionRollExecutionThread(runtime=runtime, client=client, plan=plan, parent=None)
        worker._journal = journal
        worker._config = lambda inst_id: SimpleNamespace(inst_id=inst_id)
        return worker

    def test_cancel_ack_requires_terminal_state(self) -> None:
        class Client:
            cancelled = False
            calls = 0

            def get_order(self, *args, **kwargs):
                self.calls += 1
                return SimpleNamespace(ord_id="123", state="canceled" if self.cancelled else "live", filled_size=Decimal("0"))

            def cancel_order(self, *args, **kwargs):
                self.cancelled = True

        with TemporaryDirectory() as directory, patch("roll_terminal_qt.option_roll_execution_window.time.sleep"):
            client = Client()
            worker = self._worker(client, Path(directory) / "pending.json")
            worker._active_orders = [_TrackedOrder("BTC-USD-261225-80000-C", "optroll1", "旧腿", "123")]
            self.assertTrue(worker._cancel_active_orders())
            self.assertGreaterEqual(client.calls, 2)
            self.assertEqual(worker._active_orders[0].status.state, "canceled")

    def test_submit_timeout_reconciles_by_client_id_without_second_order(self) -> None:
        class Client:
            sent: list[str] = []
            cancelled = False

            def get_positions(self, *args, **kwargs):
                return [SimpleNamespace(
                    inst_id="BTC-USD-261225-80000-C", pos_side="net", position=Decimal("1"),
                    avail_position=Decimal("1"),
                )]

            def get_option_tick_bands(self, *args):
                return []

            def place_simple_order(self, *args, **kwargs):
                self.sent.append(kwargs["cl_ord_id"])
                raise TimeoutError("response lost after exchange accepted")

            def get_order(self, *args, **kwargs):
                return SimpleNamespace(ord_id="123", state="canceled" if self.cancelled else "live", filled_size=Decimal("0"))

            def cancel_order(self, *args, **kwargs):
                self.cancelled = True

        with TemporaryDirectory() as directory, patch("roll_terminal_qt.option_roll_execution_window.time.sleep"):
            client = Client()
            journal = Path(directory) / "pending.json"
            worker = self._worker(client, journal)
            def instrument(inst_id: str) -> SimpleNamespace:
                return SimpleNamespace(inst_id=inst_id, inst_type="OPTION", state="live", ct_val=Decimal("1"), ct_mult=Decimal("1"), ct_val_ccy="BTC", tick_size=Decimal("0.0001"))
            now_ts = str(int(time.time() * 1000))
            book = SimpleNamespace(bids=((Decimal("0.01"), Decimal("1")),), asks=((Decimal("0.01"), Decimal("1")),), raw={"ts": now_ts})
            worker._load_quote = lambda inst_id: (
                instrument(inst_id),
                SimpleNamespace(inst_id=inst_id, mark=Decimal("0.01"), bid=Decimal("0.01"), ask=Decimal("0.01"), raw={"ts": now_ts, "markTs": now_ts}),
                book,
            )
            results: list[tuple[bool, str]] = []
            worker.completed.connect(lambda success, message: results.append((success, message)))
            worker.run()
            self.assertEqual(len(client.sent), 1)
            self.assertFalse(results[0][0])
            self.assertFalse(journal.exists())

    def test_explicit_exchange_price_rejection_stops_before_second_leg(self) -> None:
        class Client:
            submit_count = 0

            def get_positions(self, *args, **kwargs):
                return [SimpleNamespace(
                    inst_id="BTC-USD-261225-80000-C", pos_side="net", position=Decimal("-1"),
                    avail_position=Decimal("1"),
                )]

            def get_option_tick_bands(self, *args):
                return []

            def place_simple_order(self, *args, **kwargs):
                self.submit_count += 1
                raise OkxApiError("exchange price band rejected", code="51006")

        with TemporaryDirectory() as directory:
            client = Client()
            journal = Path(directory) / "pending.json"
            worker = self._worker(client, journal)
            worker._plan = OptionRollExecutionPlan(
                worker._plan.current_inst_id,
                worker._plan.target_inst_id,
                "short",
                worker._plan.total_qty,
                worker._plan.wait_seconds,
                worker._plan.max_mark_deviation_pct,
            )
            now_ts = str(int(time.time() * 1000))

            def instrument(inst_id: str) -> SimpleNamespace:
                return SimpleNamespace(inst_id=inst_id, inst_type="OPTION", state="live", ct_val=Decimal("1"), ct_mult=Decimal("1"), ct_val_ccy="BTC", tick_size=Decimal("0.0001"))

            worker._load_quote = lambda inst_id: (
                instrument(inst_id),
                SimpleNamespace(inst_id=inst_id, mark=Decimal("0.01"), bid=Decimal("0.01"), ask=Decimal("0.01"), raw={"ts": now_ts, "markTs": now_ts}),
                SimpleNamespace(bids=((Decimal("0.01"), Decimal("1")),), asks=((Decimal("0.01"), Decimal("1")),), raw={"ts": now_ts}),
            )
            results: list[tuple[bool, str]] = []
            worker.completed.connect(lambda success, message: results.append((success, message)))
            worker.run()

            self.assertEqual(client.submit_count, 1)
            self.assertFalse(results[0][0])
            self.assertFalse(journal.exists())

    def test_unmatched_fill_keeps_recovery_journal(self) -> None:
        class Client:
            def get_order(self, *args, **kwargs):
                return SimpleNamespace(ord_id=kwargs.get("ord_id") or "123", state="filled" if kwargs.get("ord_id") == "123" else "canceled", filled_size=Decimal("1") if kwargs.get("ord_id") == "123" else Decimal("0"))

            def cancel_order(self, *args, **kwargs):
                raise AssertionError("terminal orders must not be canceled")

        with TemporaryDirectory() as directory:
            worker = self._worker(Client(), Path(directory) / "pending.json")
            worker._active_orders = [
                _TrackedOrder(worker._plan.current_inst_id, "optroll1", "旧腿", "123"),
                _TrackedOrder(worker._plan.target_inst_id, "optroll2", "目标腿", "456"),
            ]
            filled = worker._wait_pair(tuple(worker._active_orders))
            self.assertEqual(filled, (Decimal("1"), Decimal("0")))
            self.assertTrue(worker._journal.exists())

    def test_filled_leg_waits_for_other_live_leg(self) -> None:
        class Client:
            target_queries = 0

            def get_order(self, credentials, config, *, inst_id, **kwargs):
                if inst_id == "BTC-USD-261225-80000-C":
                    return SimpleNamespace(ord_id="old", state="filled", filled_size=Decimal("1"))
                self.target_queries += 1
                state = "live" if self.target_queries == 1 else "filled"
                return SimpleNamespace(ord_id="new", state=state, filled_size=Decimal("1") if state == "filled" else Decimal("0"))

            def cancel_order(self, *args, **kwargs):
                raise AssertionError("the filled leg must not trigger cancellation of the live peer")

        with TemporaryDirectory() as directory:
            worker = self._worker(Client(), Path(directory) / "journal.json")
            orders = (
                _TrackedOrder(worker._plan.current_inst_id, "old-cl", "旧期权腿", "old"),
                _TrackedOrder(worker._plan.target_inst_id, "new-cl", "目标期权腿", "new"),
            )
            with patch("roll_terminal_qt.option_roll_execution_window.time.monotonic", side_effect=[0, 1, 2]), patch.object(worker._stop_event, "wait", return_value=False):
                self.assertEqual(worker._wait_pair(orders), (Decimal("1"), Decimal("1")))

    def test_recovery_clears_only_when_terminal_fills_match_positions(self) -> None:
        from roll_terminal_qt.option_roll_execution_window import _OptionRollRecoveryThread

        class Client:
            def get_order(self, *args, **kwargs):
                return SimpleNamespace(ord_id=kwargs["ord_id"], state="filled", filled_size=Decimal("1"))

            def get_positions(self, *args, **kwargs):
                return [
                    SimpleNamespace(inst_id="BTC-USD-270326-80000-C", pos_side="net", position=Decimal("1")),
                ]

        with TemporaryDirectory() as directory:
            journal = Path(directory) / "pending.json"
            journal.write_text(json.dumps({
                "current_inst_id": "BTC-USD-261225-80000-C",
                "target_inst_id": "BTC-USD-270326-80000-C",
                "direction": "long",
                "baseline_current": "1",
                "baseline_target": "0",
                "orders": [
                    {"inst_id": "BTC-USD-261225-80000-C", "cl_ord_id": "a", "ord_id": "old"},
                    {"inst_id": "BTC-USD-270326-80000-C", "cl_ord_id": "b", "ord_id": "new"},
                ],
            }), encoding="utf-8")
            runtime = SimpleNamespace(credentials=SimpleNamespace(profile_name="test"), environment="demo", trade_mode="cross", position_mode="net")
            worker = _OptionRollRecoveryThread(runtime=runtime, client=Client(), journal=journal, parent=None)
            worker.run()
            self.assertTrue(worker.result[0])
            self.assertFalse(journal.exists())

    def test_recovery_retains_journal_when_leg_fills_do_not_match(self) -> None:
        from roll_terminal_qt.option_roll_execution_window import _OptionRollRecoveryThread

        class Client:
            def get_order(self, credentials, config, *, inst_id, ord_id, **kwargs):
                return SimpleNamespace(ord_id=ord_id, state="filled", filled_size=Decimal("1") if inst_id.endswith("261225-80000-C") else Decimal("0"))

            def get_positions(self, *args, **kwargs):
                return []

        with TemporaryDirectory() as directory:
            journal = Path(directory) / "pending.json"
            journal.write_text(json.dumps({
                "current_inst_id": "BTC-USD-261225-80000-C",
                "target_inst_id": "BTC-USD-270326-80000-C",
                "direction": "long",
                "baseline_current": "1",
                "baseline_target": "0",
                "orders": [
                    {"inst_id": "BTC-USD-261225-80000-C", "cl_ord_id": "a", "ord_id": "old"},
                    {"inst_id": "BTC-USD-270326-80000-C", "cl_ord_id": "b", "ord_id": "new"},
                ],
            }), encoding="utf-8")
            runtime = SimpleNamespace(credentials=SimpleNamespace(profile_name="test"), environment="demo", trade_mode="cross", position_mode="net")
            worker = _OptionRollRecoveryThread(runtime=runtime, client=Client(), journal=journal, parent=None)
            worker.run()
            self.assertFalse(worker.result[0])
            self.assertTrue(journal.exists())

    def test_position_recheck_recovers_after_short_network_error(self) -> None:
        worker = self._worker(object(), Path("unused-option-roll-test.json"))
        snapshots = iter([TimeoutError("short VPN interruption"), (Decimal("0"), Decimal("1"), Decimal("0"), Decimal("0"))])

        def snapshot():
            result = next(snapshots)
            if isinstance(result, Exception):
                raise result
            return result

        worker._position_snapshot = snapshot
        with patch("roll_terminal_qt.option_roll_execution_window.time.sleep"):
            self.assertTrue(worker._verify_positions(Decimal("1"), Decimal("0")))

    def test_initial_position_snapshot_retries_after_short_network_error(self) -> None:
        worker = self._worker(object(), Path("unused-option-roll-test.json"))
        snapshots = iter([TimeoutError("short VPN interruption"), (Decimal("1"), Decimal("0"), Decimal("1"), Decimal("0"))])

        def snapshot():
            result = next(snapshots)
            if isinstance(result, Exception):
                raise result
            return result

        worker._position_snapshot = snapshot
        with patch.object(worker._stop_event, "wait", return_value=False):
            self.assertEqual(
                worker._position_snapshot_with_retry(context="移仓前", timeout_seconds=3),
                (Decimal("1"), Decimal("0"), Decimal("1"), Decimal("0")),
            )

    def test_unknown_cancel_status_keeps_order_identity(self) -> None:
        class Client:
            def get_order(self, *args, **kwargs):
                raise TimeoutError("VPN offline")

            def cancel_order(self, *args, **kwargs):
                raise TimeoutError("VPN offline")

        with TemporaryDirectory() as directory:
            worker = self._worker(Client(), Path(directory) / "pending.json")
            worker._active_orders = [_TrackedOrder(worker._plan.current_inst_id, "optroll1", "旧腿")]
            worker._save_journal()
            with patch("roll_terminal_qt.option_roll_execution_window.time.monotonic", side_effect=[0, 1, 91]), patch("roll_terminal_qt.option_roll_execution_window.time.sleep"):
                self.assertFalse(worker._cancel_active_orders())
            self.assertTrue(worker._journal.exists())
            self.assertEqual(worker._active_orders[0].cl_ord_id, "optroll1")


if __name__ == "__main__":
    unittest.main()
