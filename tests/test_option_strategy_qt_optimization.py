from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from threading import Event
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from PySide6.QtCore import QThread
from PySide6.QtTest import QTest

from okx_quant.models import Instrument
from okx_quant.option_roll import OptionRollTransferPayload
from okx_quant.option_strategy import (
    OptionChainRow, OptionQuote, StrategyLegDefinition, StrategyPayoffPoint,
    StrategyPayoffSnapshot, build_default_formula,
)
from roll_terminal_qt.option_strategy_window import (
    ChainSnapshot, ChartSnapshot, ImportSnapshot, OptionStrategyQtWindow,
)
from tests.qt_test_case import QtWidgetTestCase
from tests.test_option_recommendations import NOW, prices, volatility, quote as recommendation_quote
from okx_quant.option_recommendations import build_option_recommendations
from PySide6.QtWidgets import QMessageBox, QPushButton


def quote(strike: int = 80000, expiry: str = "261225") -> OptionQuote:
    instrument = Instrument(
        inst_id=f"BTC-USD-{expiry}-{strike}-C", inst_type="OPTION",
        tick_size=Decimal("0.0001"), lot_size=Decimal("1"), min_size=Decimal("1"),
        state="live", ct_val=Decimal("1"), ct_mult=Decimal("0.01"),
        ct_val_ccy="BTC", inst_family="BTC-USD",
    )
    return OptionQuote(instrument, mark_price=Decimal("0.05"), index_price=Decimal("80500"))


def chain_snapshot(expiry: str = "261225") -> ChainSnapshot:
    quotes = tuple(quote(strike, expiry) for strike in (70000, 80000, 90000))
    return ChainSnapshot(
        "BTC-USD", expiry, (expiry,),
        tuple(OptionChainRow(Decimal(strike), call_quote=item) for strike, item in zip((70000, 80000, 90000), quotes)),
        quotes, tuple(item.instrument for item in quotes), {}, Decimal("80500"), {},
    )


class BlockingWorker(QThread):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.entered = Event()
        self.release = Event()

    def run(self):
        self.entered.set()
        self.release.wait(3)


class OptionStrategyOptimizationTest(QtWidgetTestCase):
    def setUp(self):
        self.patchers = [
            patch("roll_terminal_qt.option_strategy_window._shared_client", return_value=MagicMock()),
            patch("roll_terminal_qt.option_strategy_window.load_runtime", return_value=SimpleNamespace(credential_profile_name="159")),
            patch("roll_terminal_qt.option_strategy_window.load_option_strategies_snapshot", return_value={}),
            patch("roll_terminal_qt.option_strategy_window.QTimer.singleShot"),
        ]
        for patcher in self.patchers:
            patcher.start()
        self.window = OptionStrategyQtWindow()
        self.workers = []

    def tearDown(self):
        for worker in self.workers:
            worker.release.set()
            worker.wait(1000)
        self._app.processEvents()
        self.dispose_widget(self.window)
        for patcher in reversed(self.patchers):
            patcher.stop()

    def seed_chain(self):
        self.window._apply_chain_snapshot(self.window._chain_request_id, chain_snapshot())

    def add_leg(self):
        self.seed_chain()
        self.window.add_selected_chain_leg("C", "buy")

    def recommendation(self):
        snap = build_option_recommendations(family="BTC-USD", price_candles=prices(),
                                           dvol_hourly=volatility(), now_ms=NOW,
                                           quotes=[recommendation_quote("261120", "120"), recommendation_quote("261120", "125")])
        self.window._recommendation_panel._apply_snapshot(self.window._recommendation_panel._request_id, snap)
        return snap, snap.suggestions[0]

    def test_recommendation_button_loads_legs_default_size_prices_formula_and_starts_analysis(self):
        snap, suggestion = self.recommendation()
        self.window._default_qty_edit.setText("5")
        self.window._tabs.setCurrentWidget(self.window._recommendation_panel)
        button = self.window._recommendation_panel._cards.findChild(QPushButton, "ImportRecommendationButton")
        with patch("roll_terminal_qt.option_strategy_window.time.time", return_value=NOW/1000), \
             patch.object(self.window, "_start_thread") as start, \
             patch("roll_terminal_qt.option_strategy_window.QMessageBox.question") as question:
            button.click()
            question.assert_not_called()
            start.assert_called_once()
            self.assertEqual(start.call_args.args[0], "chart")
        self.assertEqual([leg.inst_id for leg in self.window._legs], [leg.inst_id for leg in suggestion.legs])
        self.assertEqual([leg.quantity for leg in self.window._legs], [Decimal(5), Decimal(5)])
        self.assertEqual([leg.premium for leg in self.window._legs], [Decimal("0.021"), Decimal("0.02")])
        self.assertEqual(self.window._formula_edit.text(), "5*L1 - 5*L2")
        self.assertEqual(self.window._selected_expiry_code(), "261120")
        self.assertEqual(self.window._tabs.currentIndex(), 0)
        self.assertIn("0.05 BTC", self.window._legs_table.item(0, 7).text())
        self.window._client.get_ticker.assert_not_called()

    def test_existing_analysis_is_kept_on_cancel_and_replaced_only_on_confirm(self):
        self.add_leg()
        old = self.window._legs[:]
        formula = self.window._formula_edit.text()
        snap, suggestion = self.recommendation()
        with patch("roll_terminal_qt.option_strategy_window.time.time", return_value=NOW/1000), \
             patch.object(self.window, "_start_thread") as start, \
             patch("roll_terminal_qt.option_strategy_window.QMessageBox.question", return_value=QMessageBox.StandardButton.No):
            self.window._import_recommendation_for_analysis(snap, suggestion)
            start.assert_not_called()
        self.assertEqual(self.window._legs, old)
        self.assertEqual(self.window._formula_edit.text(), formula)
        with patch("roll_terminal_qt.option_strategy_window.time.time", return_value=NOW/1000), \
             patch.object(self.window, "_start_thread") as start, \
             patch("roll_terminal_qt.option_strategy_window.QMessageBox.question", return_value=QMessageBox.StandardButton.Yes):
            self.window._import_recommendation_for_analysis(snap, suggestion)
            start.assert_called_once()
        self.assertEqual(len(self.window._legs), 2)
        self.assertNotEqual(self.window._legs, old)

    def test_imported_recommendation_completes_both_chart_calculations(self):
        snap, suggestion = self.recommendation()
        source = prices()["1H"]
        self.window._client.get_ticker.return_value = SimpleNamespace(
            mark=Decimal("0.020"), index=Decimal("117.9"), last=Decimal("117.9"),
            bid=Decimal("0.019"), ask=Decimal("0.021"),
        )
        self.window._client.get_candles_history.return_value = source
        self.window._client.get_mark_price_candles.return_value = [
            replace(c, open=Decimal("0.020"), high=Decimal("0.021"), low=Decimal("0.019"), close=Decimal("0.020"))
            for c in source
        ]
        with patch("roll_terminal_qt.option_strategy_window.time.time", return_value=NOW/1000), \
             patch("roll_terminal_qt.option_strategy_window._load_deribit_option_chart_candles", return_value=(volatility()[-180:], "1H", "模拟")), \
             patch.object(self.window, "_start_thread", side_effect=lambda key, thread: thread.run()), \
             patch("roll_terminal_qt.option_strategy_window.QMessageBox.critical") as error:
            self.window._import_recommendation_for_analysis(snap, suggestion)
            error.assert_not_called()
        self.assertIsNotNone(self.window._latest_payoff_snapshot)
        self.assertTrue(self.window._latest_payoff_snapshot.points)
        self.assertEqual(len(self.window._latest_combo_candles), 180)
        self.assertEqual(self.window._latest_chart_formula, "L1 - L2")

    def test_import_failure_or_stale_confirmation_keeps_existing_analysis(self):
        self.add_leg()
        old = self.window._legs[:]
        snap, suggestion = self.recommendation()
        with patch("roll_terminal_qt.option_strategy_window.time.time", return_value=NOW/1000), \
             patch("roll_terminal_qt.option_strategy_window.QMessageBox.warning") as warning, \
             patch.object(self.window, "_start_thread") as start:
            self.window._default_qty_edit.setText("NaN")
            self.window._import_recommendation_for_analysis(snap, suggestion)
            warning.assert_called_once()
            start.assert_not_called()
        self.assertEqual(self.window._legs, old)
        self.window._default_qty_edit.setText("1")
        with patch("roll_terminal_qt.option_strategy_window.time.time", side_effect=[NOW/1000, (NOW+300_001)/1000]), \
             patch("roll_terminal_qt.option_strategy_window.QMessageBox.question", return_value=QMessageBox.StandardButton.Yes), \
             patch("roll_terminal_qt.option_strategy_window.QMessageBox.warning") as warning, \
             patch.object(self.window, "_start_thread") as start:
            self.window._import_recommendation_for_analysis(snap, suggestion)
            warning.assert_called_once()
            start.assert_not_called()
        self.assertEqual(self.window._legs, old)

    def test_initial_chain_selects_atm_refresh_preserves_strike_and_atm_button_returns(self):
        self.seed_chain()
        self.assertEqual(self.window._selected_chain_row().strike, Decimal("80000"))
        self.window._chain_table.selectRow(2)
        self.window._apply_chain_snapshot(self.window._chain_request_id, chain_snapshot())
        self.assertEqual(self.window._selected_chain_row().strike, Decimal("90000"))
        self.window._locate_atm_button.click()
        self.assertEqual(self.window._selected_chain_row().strike, Decimal("80000"))
        self.window._apply_chain_snapshot(self.window._chain_request_id, chain_snapshot("270326"))
        self.assertEqual(self.window._selected_chain_row().strike, Decimal("80000"))

    def test_chain_requires_exact_request_and_family(self):
        self.seed_chain()
        original = self.window._chain_rows[:]
        changed = replace(chain_snapshot(), chain_rows=())
        self.window._apply_chain_snapshot(self.window._chain_request_id + 1, changed)
        self.assertEqual(self.window._chain_rows, original)
        self.window._apply_chain_snapshot(self.window._chain_request_id, replace(changed, family="ETH-USD"))
        self.assertEqual(self.window._chain_rows, original)
        request_id = self.window._chain_request_id
        self.window._family_combo.setCurrentText("ETH-USD")
        self.window._apply_chain_snapshot(request_id, chain_snapshot())
        self.assertEqual(self.window._chain_rows, [])

    def test_core_columns_hide_details_without_discarding_data(self):
        self.add_leg()
        self.assertFalse(self.window._legs_table.isColumnHidden(8))
        self.assertFalse(self.window._legs_table.isColumnHidden(9))
        self.assertTrue(self.window._legs_table.isColumnHidden(15))
        before = self.window._legs_table.item(0, 13).text()
        self.window._leg_details_check.setChecked(True)
        self.assertFalse(self.window._legs_table.isColumnHidden(15))
        self.assertEqual(self.window._legs_table.item(0, 13).text(), before)

    def test_counterparty_price_uses_ask_for_buy_and_bid_for_sell(self):
        self.add_leg()
        inst_id = self.window._legs[0].inst_id
        current = self.window._quotes_by_inst_id[inst_id]
        self.window._quotes_by_inst_id[inst_id] = replace(
            current,
            bid_price=Decimal("0.0490"),
            ask_price=Decimal("0.0510"),
            last_price=Decimal("0.0500"),
        )
        self.window._render_legs()
        self.assertEqual(self.window._legs_table.item(0, 9).text(), "0.0510")
        self.window._legs[0].side = "sell"
        self.window._render_legs()
        self.assertEqual(self.window._legs_table.item(0, 9).text(), "0.0490")

    def test_mark_price_does_not_fall_back_to_last_trade_price(self):
        self.add_leg()
        inst_id = self.window._legs[0].inst_id
        current = self.window._quotes_by_inst_id[inst_id]
        self.window._quotes_by_inst_id[inst_id] = replace(
            current,
            mark_price=None,
            last_price=Decimal("0.0175"),
            bid_price=Decimal("0.0090"),
            ask_price=Decimal("0.0105"),
        )
        self.window._render_legs()
        self.assertIsNone(self.window._leg_mark_price(inst_id))
        self.assertEqual(self.window._legs_table.item(0, 11).text(), "-")

        self.window._quotes_by_inst_id[inst_id] = replace(
            self.window._quotes_by_inst_id[inst_id],
            mark_price=Decimal("0.0073"),
        )
        self.window._render_legs()
        self.assertEqual(self.window._leg_mark_price(inst_id), Decimal("0.0073"))
        self.assertEqual(self.window._legs_table.item(0, 11).text(), "0.0073")

    def test_default_quantity_is_contracts_and_coin_conversion_is_visible(self):
        self.seed_chain()
        self.window._default_qty_edit.setText("5")
        self.assertIn("5 张 ≈ 0.05 BTC", self.window._default_qty_hint_label.text())
        self.window.add_selected_chain_leg("C", "buy")
        self.assertEqual(self.window._legs[0].quantity, Decimal("5"))
        self.assertEqual(self.window._legs_table.item(0, 6).text(), "5")
        self.assertEqual(self.window._legs_table.item(0, 7).text(), "0.05 BTC")

    def test_combo_total_shows_contracts_net_premium_and_greeks(self):
        self.add_leg()
        self.window.add_selected_chain_leg("C", "sell")
        self.window._legs[1].premium = Decimal("0.04")
        self.window._legs[0].delta = Decimal("0.10")
        self.window._legs[1].delta = Decimal("-0.03")
        self.window._legs[0].gamma = Decimal("0.01")
        self.window._legs[1].gamma = Decimal("-0.02")
        self.window._legs[0].vega = Decimal("0.20")
        self.window._legs[1].vega = Decimal("0.10")
        self.window._legs[0].theta = Decimal("-0.04")
        self.window._legs[1].theta = Decimal("0.01")
        self.window._option_taker_fee_rate = Decimal("0.0003")
        self.window._render_legs()
        self.window._refresh_strategy_summary()

        total_row = 2
        self.assertIn("总 2 张", self.window._legs_table.item(total_row, 6).text())
        self.assertIn("BTC 净支出 0.0001", self.window._legs_table.item(total_row, 14).text())
        self.assertIn("含手续费 0.000006", self.window._legs_table.item(total_row, 14).text())
        self.assertEqual(self.window._legs_table.item(total_row, 10).text(), "8.533")
        self.assertEqual(self.window._legs_table.item(total_row, 12).text(), "0")
        self.assertEqual(self.window._legs_table.item(total_row, 15).text(), "0.07")
        self.assertIn("总张数 2", self.window._strategy_summary_label.text())
        self.assertIn("净支出 0.0001 BTC", self.window._strategy_summary_label.text())
        self.assertIn("手续费估算 0.000006 BTC", self.window._strategy_summary_label.text())
        self.assertIn("Delta 0.07", self.window._strategy_summary_label.text())

    def test_double_click_edits_quantity_and_premium_only_on_correct_columns(self):
        self.add_leg()
        with patch.object(self.window, "edit_selected_leg_quantity") as quantity, patch.object(self.window, "edit_selected_leg_premium") as premium:
            self.window._on_legs_table_double_clicked(0, 7)
            premium.assert_not_called()
            self.window._on_legs_table_double_clicked(0, 8)
            premium.assert_called_once()
            self.window._on_legs_table_double_clicked(0, 6)
            quantity.assert_called_once()

    def test_quantity_edit_updates_formula_and_clears_old_calculation(self):
        self.add_leg()
        self.window._legs_table.selectRow(0)
        self.window._latest_combo_value = Decimal("10")
        with patch("roll_terminal_qt.option_strategy_window.QInputDialog.getText", return_value=("5", True)):
            self.window.edit_selected_leg_quantity()
        self.assertEqual(self.window._formula_edit.text(), "5*L1")
        self.assertEqual(self.window._legs_table.item(0, 7).text(), "0.05 BTC")
        self.assertIsNone(self.window._latest_combo_value)

    def test_repeated_matching_requests_do_not_create_more_workers(self):
        self.add_leg()
        family = self.window._family_combo.currentText()
        active = MagicMock()
        active.isRunning.return_value = True
        active._request_id = self.window._chain_request_id
        active._family = family
        active._preferred_expiry = self.window._selected_expiry_code()
        self.window._worker_threads["chain"] = active
        with patch.object(self.window, "_start_thread") as start:
            self.window.refresh_chain()
            start.assert_not_called()
        self.window._worker_threads.clear()
        active._request_id = self.window._leg_quotes_request_id
        self.window._worker_threads["leg_quotes"] = active
        with patch.object(self.window, "_start_thread") as start:
            self.window.refresh_leg_quotes()
            start.assert_not_called()
        self.window._worker_threads.clear()

    def test_successful_import_syncs_formula_and_clears_old_plot(self):
        self.add_leg()
        q = quote(100000)
        leg = StrategyLegDefinition("L5", q.instrument.inst_id, "sell", Decimal("2"), Decimal("0.02"))
        snapshot = ImportSnapshot("BTC-USD", "261225", "family", True, ((leg, q.instrument, q),), (), {})
        self.window._latest_combo_value = Decimal("99")
        self.window._apply_imported_positions(self.window._position_import_request_id, snapshot)
        self.assertEqual(self.window._legs, [leg])
        self.assertEqual(self.window._formula_edit.text(), "-2*L5")
        self.assertIsNone(self.window._latest_combo_value)

    def test_add_remove_updates_default_formula_preserves_custom_formula(self):
        self.add_leg()
        self.window.add_selected_chain_leg("C", "sell")
        self.assertEqual(self.window._formula_edit.text(), build_default_formula(self.window._legs))
        self.window._legs_table.selectRow(0)
        self.window.remove_selected_leg()
        self.assertEqual(self.window._formula_edit.text(), "-L2")
        self.window._formula_edit.setText("3*L2 + 0.5")
        self.window.add_selected_chain_leg("C", "buy")
        self.assertEqual(self.window._formula_edit.text(), "3*L2 + 0.5")

    def test_strategy_mutation_clears_plot_and_rejects_queued_old_chart(self):
        self.add_leg()
        old_request = self.window._chart_request_id
        snapshot = ChartSnapshot((), 1000, {}, None, {}, Decimal("123"), None, (), "L1", Decimal("80500"), (), {}, None)
        self.window._latest_combo_value = Decimal("10")
        self.window.add_selected_chain_leg("C", "sell")
        self.window._apply_chart_snapshot(old_request, snapshot)
        self.assertIsNone(self.window._latest_combo_value)
        self.assertIn("重新计算", self.window._combo_summary_label.text())
        self.assertFalse(self.window._latest_resolved_legs)

    def test_premium_edit_allows_zero_and_invalidates_old_payoff(self):
        self.add_leg()
        old_request = self.window._chart_request_id
        self.window._legs_table.selectRow(0)
        with patch("roll_terminal_qt.option_strategy_window.QInputDialog.getText", return_value=("0", True)):
            self.window.edit_selected_leg_premium()
        self.assertEqual(self.window._legs[0].premium, Decimal("0"))
        self.assertGreater(self.window._chart_request_id, old_request)
        with patch("roll_terminal_qt.option_strategy_window.QInputDialog.getText", return_value=("", True)):
            self.window.edit_selected_leg_premium()
        self.assertIsNone(self.window._legs[0].premium)
        with patch("roll_terminal_qt.option_strategy_window.QInputDialog.getText", return_value=("Infinity", True)), patch("roll_terminal_qt.option_strategy_window.QMessageBox.critical") as error:
            self.window.edit_selected_leg_premium()
        error.assert_called_once()
        self.assertIsNone(self.window._legs[0].premium)

    def test_finite_quantity_validation_rejects_nan_and_infinity(self):
        for value in ("NaN", "sNaN", "Infinity", "-Infinity"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.window._parse_positive_decimal(value, "数量")

    def test_missing_current_quote_is_not_reported_as_zero_pnl(self):
        self.add_leg()
        self.window._quotes_by_inst_id.clear()
        self.window._refresh_strategy_summary()
        self.assertIn("当前组合浮盈亏 -", self.window._strategy_summary_label.text())

    def test_payoff_extrema_are_explicitly_in_displayed_range_and_currency(self):
        self.add_leg()
        self.window._latest_payoff_snapshot = StrategyPayoffSnapshot(
            (StrategyPayoffPoint(Decimal("70000"), Decimal("-0.03")),
             StrategyPayoffPoint(Decimal("90000"), Decimal("0.06"))),
            (), Decimal("-0.01"), Decimal("70000"), Decimal("90000"), Decimal("80500"),
        )
        self.window._refresh_chart_display()
        summary = self.window._payoff_summary_label.text()
        self.assertIn("图示标的价格范围", summary)
        self.assertIn("范围内最高盈亏 0.06 BTC", summary)
        self.assertIn("范围内最低盈亏 -0.03 BTC", summary)
        self.assertIn("未含手续费", summary)
        self.assertNotIn("最大收益", summary)

    def test_late_quote_or_import_cannot_update_replaced_strategy(self):
        self.add_leg()
        quote_request = self.window._leg_quotes_request_id
        import_request = self.window._position_import_request_id
        q = quote(100000)
        self.window.clear_legs()
        self.window._apply_refreshed_leg_quotes(quote_request, ((q.instrument.inst_id, q.instrument, q),))
        self.assertNotIn(q.instrument.inst_id, self.window._quotes_by_inst_id)
        imported = ImportSnapshot("BTC-USD", "261225", "family", True, ((StrategyLegDefinition("L5", q.instrument.inst_id, "buy", Decimal("1")), q.instrument, q),), (), {})
        self.window._apply_imported_positions(import_request, imported)
        self.assertEqual(self.window._legs, [])

    def test_old_worker_finishing_does_not_remove_new_worker(self):
        old, new = BlockingWorker(self.window), BlockingWorker(self.window)
        self.workers.extend((old, new))
        self.window._start_thread("chain", old)
        self.assertTrue(old.entered.wait(1))
        self.window._start_thread("chain", new)
        self.assertTrue(new.entered.wait(1))
        self.assertTrue(old.isInterruptionRequested())
        self.assertIn(old, self.window._retired_worker_threads)
        old.release.set()
        old.wait(1000)
        self._app.processEvents()
        self.assertIs(self.window._worker_threads["chain"], new)
        self.assertNotIn(old, self.window._retired_worker_threads)
        self.workers.remove(old)  # Its deferred deletion can now run safely.

    def test_close_keeps_running_worker_alive_and_defers_destruction(self):
        worker = BlockingWorker(self.window)
        self.workers.append(worker)
        self.window._start_thread("chain", worker)
        self.assertTrue(worker.entered.wait(1))
        self.assertFalse(self.window.close())
        self.assertTrue(worker.isInterruptionRequested())
        self.assertTrue(self.window._close_retry_timer.isActive())
        self.assertIs(self.window._worker_threads["chain"], worker)
        worker.release.set()
        worker.wait(1000)
        self._app.processEvents()
        self.workers.remove(worker)
        QTest.qWait(120)
        self.assertFalse(self.window._close_retry_timer.isActive())

    def test_closed_window_can_reopen_and_rejects_pre_close_results(self):
        self.add_leg()
        self.window.show()
        names = ("_chain_request_id", "_position_import_request_id", "_chart_request_id",
                 "_overlay_chart_request_id", "_leg_quotes_request_id")
        old_ids = {name: getattr(self.window, name) for name in names}
        self.assertTrue(self.window.close())
        self.assertTrue(self.window._closing)
        for name in names:
            self.assertGreater(getattr(self.window, name), old_ids[name])
        self.window.show()
        self.assertFalse(self.window._closing)
        self.assertFalse(self.window._close_retry_timer.isActive())
        old_snapshot = ChartSnapshot((), 1000, {}, None, {}, Decimal("123"), None, (),
                                     "L1", Decimal("80500"), (), {}, None)
        self.window._apply_chart_snapshot(old_ids["_chart_request_id"], old_snapshot)
        self.assertIsNone(self.window._latest_combo_value)
        with patch.object(self.window, "_start_thread") as start:
            self.window.refresh_charts()
            start.assert_called_once()
        if hasattr(self.window, "_recommendation_panel"):
            self.assertFalse(self.window._recommendation_panel._closing)
            self.assertTrue(self.window._recommendation_panel._freshness_timer.isActive())

    def test_close_also_waits_for_recommendation_worker(self):
        panel = self.window._recommendation_panel
        worker = BlockingWorker(panel)
        panel._worker = worker
        worker.finished.connect(panel._worker_finished)
        self.workers.append(worker)
        worker.start()
        self.assertTrue(worker.entered.wait(1))
        self.assertFalse(self.window.close())
        self.assertTrue(self.window._close_retry_timer.isActive())
        self.assertTrue(worker.isInterruptionRequested())
        worker.release.set()
        worker.wait(1000)
        self.workers.remove(worker)
        self._app.processEvents()
        QTest.qWait(120)
        self.assertIsNone(panel._worker)
        self.assertFalse(self.window._close_retry_timer.isActive())

    def test_reopening_during_pending_close_cancels_delayed_close(self):
        self.window.show()
        worker = BlockingWorker(self.window)
        self.workers.append(worker)
        self.window._start_thread("chain", worker)
        self.assertTrue(worker.entered.wait(1))
        self.assertFalse(self.window.close())
        self.assertTrue(self.window._close_retry_timer.isActive())
        self.window.show()
        self.assertFalse(self.window._closing)
        self.assertFalse(self.window._close_retry_timer.isActive())
        self.assertTrue(self.window.isVisible())
        worker.release.set()
        worker.wait(1000)
        self._app.processEvents()
        self.workers.remove(worker)
        QTest.qWait(120)
        self.assertTrue(self.window.isVisible())
        self.assertFalse(self.window._closing)

    def test_import_after_close_resumes_before_show_and_starts_chart_refresh(self):
        self.window.show()
        self.window.close()
        self.assertTrue(self.window._closing)
        q = quote()
        leg = StrategyLegDefinition("L1", q.instrument.inst_id, "buy", Decimal("5"), Decimal("0.02"))
        payload = OptionRollTransferPayload(
            strategy_name="复开持仓分析", option_family="BTC-USD", expiry_code="261225",
            legs=(leg,), instruments=(q.instrument,), quotes=(q,),
        )
        with patch.object(self.window, "_start_thread") as start:
            self.window.load_roll_transfer_payload(payload)
            start.assert_called_once()
            self.assertEqual(start.call_args.args[0], "chart")
        self.assertFalse(self.window._closing)
        self.assertFalse(self.window._close_retry_timer.isActive())
        self.assertEqual(self.window._legs[0].quantity, Decimal("5"))
        self.assertEqual(self.window._formula_edit.text(), "5*L1")

    def test_slider_events_are_debounced(self):
        with patch.object(self.window, "_refresh_payoff_simulation") as recalculate:
            # The timer connection holds the original bound method; reconnect
            # for this behavioral test to count delivered simulation requests.
            self.window._payoff_simulation_timer.timeout.disconnect()
            self.window._payoff_simulation_timer.timeout.connect(recalculate)
            for value in range(1, 20):
                self.window._payoff_vol_slider.setValue(value)
            self.assertEqual(recalculate.call_count, 0)
            QTest.qWait(150)
            self.assertEqual(recalculate.call_count, 1)
