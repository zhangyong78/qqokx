import json
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest import TestCase

from okx_quant.enhanced_backtest import EnhancedBacktestLab, export_lab_report_markdown
from okx_quant.enhanced_evidence_chart import export_evidence_chart_bundle
from okx_quant.enhanced_gate_engine import EnhancedGateEngine
from okx_quant.enhanced_history_loader import load_segmented_history_candles
from okx_quant.enhanced_models import (
    ChildSignalConfig,
    ChildSignalLabProfile,
    ExecutionPlaybookConfig,
    GateRuleConfig,
    LabSimulationConfig,
    ParentStrategyConfig,
    QuotaSnapshot,
    SignalRuleMatch,
    SignalSource,
)
from okx_quant.enhanced_position_ledger import (
    TOTAL_BUCKET_MODE_DEFAULT,
    TOTAL_BUCKET_MODE_PLAYBOOK_SOURCE,
    build_active_position_ledger_rows,
    build_active_position_summary_rows,
    build_total_position_summary_rows,
)
from okx_quant.enhanced_review_pages import (
    build_manual_review_rows,
    write_manual_review_gallery_html,
    write_position_management_html,
    write_total_position_management_html,
)
from okx_quant.enhanced_registry import EnhancedStrategyRegistry
from okx_quant.enhanced_runtime_config import (
    apply_strategy_runtime_payload,
    build_strategy_runtime_payload,
    get_strategy_runtime_payload,
    load_runtime_store,
    write_strategy_runtime_payload,
)
from okx_quant.enhanced_seed_strategies import (
    PARENT_STRATEGY_NAME,
    SEED_SIGNAL_IDS,
    register_seed_strategy_package,
)
from okx_quant.enhanced_signal_engine import (
    EnhancedSignalEngine,
    close_crosses_above_lookback_high,
)
from okx_quant.models import Candle
from scripts.generate_enhanced_seed_lab_realdata_report import (
    apply_runtime_overrides,
    write_evidence_chart_manifest_csv,
    write_evidence_index_csv,
    write_evidences_json,
    write_manual_review_manifest_csv,
    write_position_ledger_csv,
    write_position_ledger_summary_csv,
    write_total_position_summary_csv,
    write_signal_states_csv,
)


class EnhancedStrategyLabTest(TestCase):
    def _build_5m_candles(self, closes: list[str]) -> list[Candle]:
        candles: list[Candle] = []
        previous_close = Decimal(closes[0])
        for index, raw_close in enumerate(closes, start=1):
            close = Decimal(raw_close)
            open_price = previous_close
            high = max(open_price, close)
            low = min(open_price, close)
            candles.append(Candle(index, open_price, high, low, close, Decimal("1"), True))
            previous_close = close
        return candles

    def _build_candles(self, rows: list[tuple[str, str, str, str]]) -> list[Candle]:
        candles: list[Candle] = []
        for index, (open_price, high, low, close) in enumerate(rows, start=1):
            candles.append(
                Candle(
                    index,
                    Decimal(open_price),
                    Decimal(high),
                    Decimal(low),
                    Decimal(close),
                    Decimal("1"),
                    True,
                )
            )
        return candles

    def test_signal_engine_aggregates_5m_into_15m_and_finds_breakout(self) -> None:
        engine = EnhancedSignalEngine()
        base_candles = self._build_5m_candles(
            ["100", "100", "100", "101", "101", "101", "102", "102", "102", "110", "110", "110"]
        )
        config = ChildSignalConfig(
            signal_id="sig-breakout",
            signal_name="Breakout",
            source=SignalSource(market="SPOT", inst_id="BTC-USDT", bar="15m"),
            underlying_family="BTC-USD",
            direction_bias="long",
            trigger_rule_id="breakout_3",
            invalidation_rule_id="never",
            evidence_template_id="default",
        )

        aggregated = engine.aggregate_candles(base_candles, base_bar="5m", target_bar="15m")
        events = engine.evaluate_signal(
            config,
            aggregated,
            trigger_rule=lambda candles, index: close_crosses_above_lookback_high(candles, index, 3),
        )

        self.assertEqual(len(aggregated), 4)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].candle_ts, 12)
        self.assertEqual(events[0].signal_price, Decimal("110"))
        self.assertEqual(events[0].bar_step, 3)

    def test_gate_engine_blocks_when_allow_rule_is_not_satisfied(self) -> None:
        gate_engine = EnhancedGateEngine()
        gate_rule = GateRuleConfig(
            gate_id="gate-1",
            gate_name="high_only",
            underlying_family="BTC-USD",
            condition_expr="price >= 90000",
            effect="allow_open",
        )
        variables = gate_engine.build_gate_variables(price=Decimal("85000"))

        allowed, reason, checks = gate_engine.evaluate_gates([gate_rule], variables)

        self.assertFalse(allowed)
        self.assertEqual(reason, "high_only: allow_rule_not_satisfied")
        self.assertEqual(len(checks), 1)
        self.assertFalse(checks[0].matched)

    def test_enhanced_backtest_lab_runs_signal_gate_playbook_closure(self) -> None:
        registry = EnhancedStrategyRegistry()
        registry.register_parent_strategy(
            ParentStrategyConfig(
                strategy_id="spot_enhancement_36",
                strategy_name=PARENT_STRATEGY_NAME,
            )
        )
        registry.register_child_signal(
            "spot_enhancement_36",
            ChildSignalConfig(
                signal_id="sig-breakout",
                signal_name="Breakout",
                source=SignalSource(market="SPOT", inst_id="BTC-USDT", bar="15m"),
                underlying_family="BTC-USD",
                direction_bias="long",
                trigger_rule_id="breakout_3",
                invalidation_rule_id="never",
                evidence_template_id="default",
            ),
        )
        registry.register_playbook(
            ExecutionPlaybookConfig(
                playbook_id="swap-long",
                playbook_name="swap long",
                action="SWAP_LONG",
                underlying_family="BTC-USD",
                sizing_mode="fixed_slot",
                slot_size=Decimal("1"),
            )
        )
        registry.register_gate_rule(
            GateRuleConfig(
                gate_id="gate-price",
                gate_name="price_gate",
                underlying_family="BTC-USD",
                condition_expr="price >= 105",
                effect="allow_open",
            )
        )
        registry.register_gate_rule(
            GateRuleConfig(
                gate_id="gate-protection",
                gate_name="protection_gate",
                underlying_family="BTC-USD",
                condition_expr="protected_long_available >= 1",
                effect="allow_open",
            )
        )
        registry.register_trigger_rule(
            "breakout_3",
            lambda candles, index: close_crosses_above_lookback_high(candles, index, 3),
        )
        registry.register_invalidation_rule("never", lambda candles, index: None)
        registry.bind_signal(
            "sig-breakout",
            playbook_ids=["swap-long"],
            gate_ids=["gate-price", "gate-protection"],
        )

        lab = EnhancedBacktestLab(registry)
        result = lab.run(
            parent_strategy_id="spot_enhancement_36",
            candle_feeds={
                ("SPOT", "BTC-USDT"): self._build_5m_candles(
                    ["100", "100", "100", "101", "101", "101", "102", "102", "102", "110", "110", "110"]
                )
            },
            base_bar="5m",
            quota_snapshots={
                "BTC-USD": QuotaSnapshot(
                    underlying_family="BTC-USD",
                    long_limit_total=Decimal("3"),
                    protected_long_quota_total=Decimal("1.5"),
                )
            },
        )

        self.assertEqual(result.parent_strategy_name, PARENT_STRATEGY_NAME)
        self.assertEqual(result.simulation_config.exit_mode, "fixed_hold")
        self.assertEqual(len(result.summaries), 1)
        summary = result.summaries[0]
        self.assertEqual(summary.total_signal_events, 1)
        self.assertEqual(summary.routed_events, 1)
        self.assertEqual(summary.accepted_events, 1)
        self.assertEqual(summary.rejected_events, 0)
        self.assertEqual(summary.lab_profile_name, "")
        self.assertEqual(summary.applied_simulation_config.exit_mode, "fixed_hold")
        self.assertEqual(summary.playbook_accept_counts, (("swap long", 1),))
        self.assertEqual(len(result.events), 1)
        self.assertTrue(result.events[0].accepted)
        self.assertEqual(result.events[0].position_size, Decimal("1"))
        self.assertEqual(result.events[0].quota_allocations[0][0], "long_direction")

    def test_enhanced_backtest_lab_records_gate_rejection_reason(self) -> None:
        registry = EnhancedStrategyRegistry()
        registry.register_parent_strategy(
            ParentStrategyConfig(
                strategy_id="spot_enhancement_36",
                strategy_name=PARENT_STRATEGY_NAME,
            )
        )
        registry.register_child_signal(
            "spot_enhancement_36",
            ChildSignalConfig(
                signal_id="sig-breakout",
                signal_name="Breakout",
                source=SignalSource(market="SPOT", inst_id="BTC-USDT", bar="15m"),
                underlying_family="BTC-USD",
                direction_bias="long",
                trigger_rule_id="breakout_3",
                invalidation_rule_id="never",
                evidence_template_id="default",
            ),
        )
        registry.register_playbook(
            ExecutionPlaybookConfig(
                playbook_id="swap-long",
                playbook_name="swap long",
                action="SWAP_LONG",
                underlying_family="BTC-USD",
                sizing_mode="fixed_slot",
                slot_size=Decimal("1"),
            )
        )
        registry.register_gate_rule(
            GateRuleConfig(
                gate_id="gate-price",
                gate_name="price_gate",
                underlying_family="BTC-USD",
                condition_expr="price >= 120",
                effect="allow_open",
            )
        )
        registry.register_trigger_rule(
            "breakout_3",
            lambda candles, index: close_crosses_above_lookback_high(candles, index, 3),
        )
        registry.register_invalidation_rule("never", lambda candles, index: None)
        registry.bind_signal("sig-breakout", playbook_ids=["swap-long"], gate_ids=["gate-price"])

        lab = EnhancedBacktestLab(registry)
        result = lab.run(
            parent_strategy_id="spot_enhancement_36",
            candle_feeds={
                ("SPOT", "BTC-USDT"): self._build_5m_candles(
                    ["100", "100", "100", "101", "101", "101", "102", "102", "102", "110", "110", "110"]
                )
            },
            base_bar="5m",
        )

        summary = result.summaries[0]
        self.assertEqual(summary.accepted_events, 0)
        self.assertEqual(summary.rejected_events, 1)
        self.assertEqual(summary.gate_rejected_events, 1)
        self.assertEqual(summary.quota_rejected_events, 0)
        self.assertEqual(summary.rejection_reasons, (("price_gate: allow_rule_not_satisfied", 1),))
        self.assertFalse(result.events[0].accepted)
        self.assertEqual(result.events[0].lifecycle_status, "gate_rejected")
        self.assertEqual(result.events[0].quota_reason, "skipped_due_to_gate_reject")

    def test_enhanced_backtest_lab_releases_quota_after_hold_window(self) -> None:
        registry = EnhancedStrategyRegistry()
        registry.register_parent_strategy(
            ParentStrategyConfig(
                strategy_id="spot_enhancement_36",
                strategy_name=PARENT_STRATEGY_NAME,
            )
        )
        registry.register_child_signal(
            "spot_enhancement_36",
            ChildSignalConfig(
                signal_id="sig-breakout",
                signal_name="Repeated Breakout",
                source=SignalSource(market="SPOT", inst_id="BTC-USDT", bar="5m"),
                underlying_family="BTC-USD",
                direction_bias="long",
                trigger_rule_id="breakout_2",
                invalidation_rule_id="never",
                evidence_template_id="default",
            ),
        )
        registry.register_playbook(
            ExecutionPlaybookConfig(
                playbook_id="swap-long",
                playbook_name="swap long",
                action="SWAP_LONG",
                underlying_family="BTC-USD",
                sizing_mode="fixed_slot",
                slot_size=Decimal("1"),
                lab_hold_bars=3,
            )
        )
        registry.register_trigger_rule(
            "breakout_2",
            lambda candles, index: close_crosses_above_lookback_high(candles, index, 2),
        )
        registry.register_invalidation_rule("never", lambda candles, index: None)
        registry.bind_signal("sig-breakout", playbook_ids=["swap-long"])

        result = EnhancedBacktestLab(registry).run(
            parent_strategy_id="spot_enhancement_36",
            candle_feeds={
                ("SPOT", "BTC-USDT"): self._build_5m_candles(
                    ["100", "99", "101", "100", "102", "101", "103"]
                )
            },
            quota_snapshots={
                "BTC-USD": QuotaSnapshot(
                    underlying_family="BTC-USD",
                    long_limit_total=Decimal("1"),
                    protected_long_quota_total=Decimal("1"),
                )
            },
        )

        summary = result.summaries[0]
        self.assertEqual(summary.total_signal_events, 3)
        self.assertEqual(summary.accepted_events, 2)
        self.assertEqual(summary.quota_rejected_events, 1)
        self.assertEqual(
            summary.rejection_reasons,
            (("insufficient long direction quota: need 1, have 0", 1),),
        )
        self.assertEqual([item.accepted for item in result.events], [True, False, True])

    def test_tp_sl_handoff_keeps_quota_occupied(self) -> None:
        registry = EnhancedStrategyRegistry()
        registry.register_parent_strategy(
            ParentStrategyConfig(
                strategy_id="spot_enhancement_36",
                strategy_name=PARENT_STRATEGY_NAME,
            )
        )
        registry.register_child_signal(
            "spot_enhancement_36",
            ChildSignalConfig(
                signal_id="sig-manual",
                signal_name="Manual Handoff Test",
                source=SignalSource(market="SPOT", inst_id="BTC-USDT", bar="5m"),
                underlying_family="BTC-USD",
                direction_bias="long",
                trigger_rule_id="manual_rule",
                invalidation_rule_id="never",
                evidence_template_id="default",
            ),
        )
        registry.register_playbook(
            ExecutionPlaybookConfig(
                playbook_id="swap-long",
                playbook_name="swap long",
                action="SWAP_LONG",
                underlying_family="BTC-USD",
                sizing_mode="fixed_slot",
                slot_size=Decimal("1"),
            )
        )
        registry.register_trigger_rule(
            "manual_rule",
            lambda candles, index: (
                SignalRuleMatch(True, "manual trigger", candles[index].close) if index in {1, 4} else None
            ),
        )
        registry.register_invalidation_rule("never", lambda candles, index: None)
        registry.bind_signal("sig-manual", playbook_ids=["swap-long"])

        candles = self._build_candles(
            [
                ("100", "100.2", "99.8", "100"),
                ("100", "100.5", "99.9", "100"),
                ("100", "100.1", "99.0", "99.2"),
                ("99.2", "99.5", "98.8", "99.1"),
                ("99.1", "100.4", "99.0", "100"),
                ("100", "100.5", "99.8", "100.1"),
            ]
        )
        result = EnhancedBacktestLab(registry).run(
            parent_strategy_id="spot_enhancement_36",
            candle_feeds={("SPOT", "BTC-USDT"): candles},
            quota_snapshots={
                "BTC-USD": QuotaSnapshot(
                    underlying_family="BTC-USD",
                    long_limit_total=Decimal("1"),
                    protected_long_quota_total=Decimal("1"),
                )
            },
            simulation_config=LabSimulationConfig(
                exit_mode="tp_sl_handoff",
                max_hold_bars=3,
                stop_loss_pct=Decimal("0.005"),
                take_profit_pct=Decimal("0.01"),
                stop_hit_mode="handoff_manual",
            ),
        )

        summary = result.summaries[0]
        self.assertEqual(summary.accepted_events, 1)
        self.assertEqual(summary.manual_handoff_events, 1)
        self.assertEqual(summary.quota_rejected_events, 1)
        self.assertEqual(result.events[0].lifecycle_status, "handoff_manual")
        self.assertEqual(result.events[1].lifecycle_status, "quota_rejected")
        self.assertEqual(result.ending_quota_snapshots[0].long_limit_used, Decimal("1"))

    def test_tp_sl_take_profit_records_realized_pnl(self) -> None:
        registry = EnhancedStrategyRegistry()
        registry.register_parent_strategy(
            ParentStrategyConfig(
                strategy_id="spot_enhancement_36",
                strategy_name=PARENT_STRATEGY_NAME,
            )
        )
        registry.register_child_signal(
            "spot_enhancement_36",
            ChildSignalConfig(
                signal_id="sig-tp",
                signal_name="TP Test",
                source=SignalSource(market="SPOT", inst_id="BTC-USDT", bar="5m"),
                underlying_family="BTC-USD",
                direction_bias="long",
                trigger_rule_id="tp_rule",
                invalidation_rule_id="never",
                evidence_template_id="default",
            ),
        )
        registry.register_playbook(
            ExecutionPlaybookConfig(
                playbook_id="swap-long",
                playbook_name="swap long",
                action="SWAP_LONG",
                underlying_family="BTC-USD",
                sizing_mode="fixed_slot",
                slot_size=Decimal("2"),
            )
        )
        registry.register_trigger_rule(
            "tp_rule",
            lambda candles, index: SignalRuleMatch(True, "tp trigger", candles[index].close) if index == 1 else None,
        )
        registry.register_invalidation_rule("never", lambda candles, index: None)
        registry.bind_signal("sig-tp", playbook_ids=["swap-long"])

        candles = self._build_candles(
            [
                ("100", "100.1", "99.9", "100"),
                ("100", "100.2", "99.8", "100"),
                ("100", "103", "99.7", "102.5"),
                ("102.5", "102.6", "102.0", "102.2"),
            ]
        )
        result = EnhancedBacktestLab(registry).run(
            parent_strategy_id="spot_enhancement_36",
            candle_feeds={("SPOT", "BTC-USDT"): candles},
            quota_snapshots={
                "BTC-USD": QuotaSnapshot(
                    underlying_family="BTC-USD",
                    long_limit_total=Decimal("2"),
                    protected_long_quota_total=Decimal("2"),
                )
            },
            simulation_config=LabSimulationConfig(
                exit_mode="tp_sl_handoff",
                max_hold_bars=2,
                stop_loss_pct=Decimal("0.005"),
                take_profit_pct=Decimal("0.02"),
                fee_rate=Decimal("0.001"),
                stop_hit_mode="close_loss",
            ),
        )

        summary = result.summaries[0]
        self.assertEqual(summary.realized_outcomes, 1)
        self.assertEqual(summary.take_profit_closed_events, 1)
        self.assertEqual(summary.manual_handoff_events, 0)
        self.assertEqual(summary.win_events, 1)
        self.assertEqual(summary.total_pnl_value, Decimal("3.596"))
        self.assertEqual(result.events[0].realized_pnl_value, Decimal("3.596"))
        self.assertEqual(result.events[0].lifecycle_status, "take_profit_closed")

    def test_signal_lab_profile_overrides_global_simulation(self) -> None:
        registry = EnhancedStrategyRegistry()
        registry.register_parent_strategy(
            ParentStrategyConfig(
                strategy_id="spot_enhancement_36",
                strategy_name=PARENT_STRATEGY_NAME,
            )
        )
        registry.register_child_signal(
            "spot_enhancement_36",
            ChildSignalConfig(
                signal_id="sig-profile",
                signal_name="Profile Test",
                source=SignalSource(market="SPOT", inst_id="BTC-USDT", bar="5m"),
                underlying_family="BTC-USD",
                direction_bias="long",
                trigger_rule_id="profile_rule",
                invalidation_rule_id="never",
                evidence_template_id="default",
            ),
        )
        registry.register_playbook(
            ExecutionPlaybookConfig(
                playbook_id="swap-long",
                playbook_name="swap long",
                action="SWAP_LONG",
                underlying_family="BTC-USD",
                sizing_mode="fixed_slot",
                slot_size=Decimal("1"),
                lab_hold_bars=5,
            )
        )
        registry.register_signal_lab_profile(
            ChildSignalLabProfile(
                signal_id="sig-profile",
                profile_name="profile_override",
                exit_mode="tp_sl_handoff",
                max_hold_bars=7,
                stop_loss_pct=Decimal("0.004"),
                take_profit_pct=Decimal("0.012"),
                stop_hit_mode="handoff_manual",
            )
        )
        registry.register_trigger_rule(
            "profile_rule",
            lambda candles, index: (
                SignalRuleMatch(True, "profile trigger", candles[index].close) if index == 1 else None
            ),
        )
        registry.register_invalidation_rule("never", lambda candles, index: None)
        registry.bind_signal("sig-profile", playbook_ids=["swap-long"])

        candles = self._build_candles(
            [
                ("100", "100.2", "99.8", "100"),
                ("100", "100.2", "99.8", "100"),
                ("100", "101.5", "99.9", "101"),
                ("101", "101.3", "100.8", "101.1"),
            ]
        )
        result = EnhancedBacktestLab(registry).run(
            parent_strategy_id="spot_enhancement_36",
            candle_feeds={("SPOT", "BTC-USDT"): candles},
            quota_snapshots={
                "BTC-USD": QuotaSnapshot(
                    underlying_family="BTC-USD",
                    long_limit_total=Decimal("1"),
                    protected_long_quota_total=Decimal("1"),
                )
            },
            simulation_config=LabSimulationConfig(
                exit_mode="fixed_hold",
                fixed_hold_bars=2,
                stop_loss_pct=Decimal("0.02"),
                take_profit_pct=Decimal("0.04"),
            ),
        )

        summary = result.summaries[0]
        self.assertEqual(summary.lab_profile_name, "profile_override")
        self.assertEqual(summary.applied_simulation_config.exit_mode, "tp_sl_handoff")
        self.assertEqual(summary.applied_simulation_config.max_hold_bars, 7)
        self.assertEqual(summary.applied_simulation_config.stop_loss_pct, Decimal("0.004"))
        self.assertEqual(summary.applied_simulation_config.take_profit_pct, Decimal("0.012"))

    def test_runtime_overrides_can_toggle_signal_and_patch_profile(self) -> None:
        registry = EnhancedStrategyRegistry()
        register_seed_strategy_package(registry)

        with TemporaryDirectory() as tmp_dir:
            overrides_path = Path(tmp_dir) / "profile_overrides.json"
            overrides_path.write_text(
                json.dumps(
                    {
                        "seed_ma_breakout_long": {
                            "profile_name": "hot_patch",
                            "max_hold_bars": 9,
                            "take_profit_pct": "0.015",
                        }
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            runtime = apply_runtime_overrides(
                registry,
                SimpleNamespace(
                    disable_signals="seed_range_breakdown_short",
                    enable_signals="",
                    clear_lab_profiles="",
                    profile_overrides_file=str(overrides_path),
                ),
            )

        self.assertFalse(registry.get_child_signal("seed_range_breakdown_short").enabled)
        self.assertIn("seed_range_breakdown_short", runtime["disabled_signals"])
        patched_profile = registry.get_signal_lab_profile("seed_ma_breakout_long")
        self.assertIsNotNone(patched_profile)
        self.assertEqual(patched_profile.profile_name, "hot_patch")
        self.assertEqual(patched_profile.max_hold_bars, 9)
        self.assertEqual(patched_profile.take_profit_pct, Decimal("0.015"))
        self.assertEqual(
            runtime["patched_profiles"]["seed_ma_breakout_long"]["take_profit_pct"],
            "0.015",
        )

        result = EnhancedBacktestLab(registry).run(
            parent_strategy_id="spot_enhancement_36",
            candle_feeds={
                ("SPOT", "BTC-USDT"): self._build_5m_candles(
                    [
                        "100",
                        "101",
                        "102",
                        "103",
                        "104",
                        "102",
                        "104",
                        "106",
                        "108",
                        "107",
                        "109",
                        "111",
                    ]
                )
            },
            quota_snapshots={
                "BTC-USD": QuotaSnapshot(
                    underlying_family="BTC-USD",
                    long_limit_total=Decimal("2"),
                    short_limit_total=Decimal("1"),
                    protected_long_quota_total=Decimal("2"),
                    protected_short_quota_total=Decimal("1"),
                )
            },
        )

        summary_ids = {item.signal_id for item in result.summaries}
        self.assertNotIn("seed_range_breakdown_short", summary_ids)
        breakout_summary = next(item for item in result.summaries if item.signal_id == "seed_ma_breakout_long")
        self.assertEqual(breakout_summary.lab_profile_name, "hot_patch")
        self.assertEqual(breakout_summary.applied_simulation_config.max_hold_bars, 9)
        self.assertEqual(breakout_summary.applied_simulation_config.take_profit_pct, Decimal("0.015"))

    def test_runtime_config_file_roundtrip_persists_signal_states(self) -> None:
        registry = EnhancedStrategyRegistry()
        register_seed_strategy_package(registry)
        registry.set_child_signal_enabled("seed_range_breakdown_short", False)
        registry.replace_signal_lab_profile(
            "seed_ma_breakout_long",
            ChildSignalLabProfile(
                signal_id="seed_ma_breakout_long",
                profile_name="saved_runtime_profile",
                exit_mode="tp_sl_handoff",
                max_hold_bars=9,
                stop_loss_pct=Decimal("0.0041"),
                take_profit_pct=Decimal("0.0123"),
                stop_hit_mode="handoff_manual",
            ),
        )
        registry.replace_signal_lab_profile("seed_pullback_reclaim_long", None)

        payload = build_strategy_runtime_payload(
            registry,
            parent_strategy_id="spot_enhancement_36",
        )

        with TemporaryDirectory() as tmp_dir:
            runtime_path = Path(tmp_dir) / ".okx_quant_enhanced_strategy_runtime.json"
            write_strategy_runtime_payload(runtime_path, strategy_payload=payload)
            store = load_runtime_store(runtime_path)
            loaded_payload = get_strategy_runtime_payload(store, "spot_enhancement_36")

            self.assertIsNotNone(loaded_payload)
            assert loaded_payload is not None
            self.assertIn("seed_ma_breakout_long", loaded_payload["signals"])
            self.assertFalse(loaded_payload["signals"]["seed_range_breakdown_short"]["enabled"])
            self.assertIsNone(loaded_payload["signals"]["seed_pullback_reclaim_long"]["lab_profile"])

            fresh_registry = EnhancedStrategyRegistry()
            register_seed_strategy_package(fresh_registry)
            applied = apply_strategy_runtime_payload(
                fresh_registry,
                parent_strategy_id="spot_enhancement_36",
                strategy_payload=loaded_payload,
            )

        self.assertIn("seed_range_breakdown_short", applied["disabled_signals"])
        self.assertIn("seed_pullback_reclaim_long", applied["cleared_lab_profiles"])
        self.assertIn("seed_ma_breakout_long", applied["replaced_lab_profiles"])
        self.assertFalse(fresh_registry.get_child_signal("seed_range_breakdown_short").enabled)
        self.assertIsNone(fresh_registry.get_signal_lab_profile("seed_pullback_reclaim_long"))
        patched_profile = fresh_registry.get_signal_lab_profile("seed_ma_breakout_long")
        self.assertIsNotNone(patched_profile)
        assert patched_profile is not None
        self.assertEqual(patched_profile.profile_name, "saved_runtime_profile")
        self.assertEqual(patched_profile.max_hold_bars, 9)
        self.assertEqual(patched_profile.take_profit_pct, Decimal("0.0123"))

    def test_lab_export_records_evidence_and_supporting_files(self) -> None:
        registry = EnhancedStrategyRegistry()
        registry.register_parent_strategy(
            ParentStrategyConfig(
                strategy_id="spot_enhancement_36",
                strategy_name=PARENT_STRATEGY_NAME,
            )
        )
        registry.register_child_signal(
            "spot_enhancement_36",
            ChildSignalConfig(
                signal_id="sig-evidence",
                signal_name="Evidence Test",
                source=SignalSource(market="SPOT", inst_id="BTC-USDT", bar="5m"),
                underlying_family="BTC-USD",
                direction_bias="long",
                trigger_rule_id="evidence_rule",
                invalidation_rule_id="never",
                evidence_template_id="default",
            ),
        )
        registry.register_playbook(
            ExecutionPlaybookConfig(
                playbook_id="swap-long",
                playbook_name="swap long",
                action="SWAP_LONG",
                underlying_family="BTC-USD",
                sizing_mode="fixed_slot",
                slot_size=Decimal("1"),
            )
        )
        registry.register_trigger_rule(
            "evidence_rule",
            lambda candles, index: (
                SignalRuleMatch(True, "evidence trigger", candles[index].close) if index == 2 else None
            ),
        )
        registry.register_invalidation_rule("never", lambda candles, index: None)
        registry.bind_signal("sig-evidence", playbook_ids=["swap-long"])

        result = EnhancedBacktestLab(registry).run(
            parent_strategy_id="spot_enhancement_36",
            candle_feeds={
                ("SPOT", "BTC-USDT"): self._build_5m_candles(
                    ["100", "100.5", "101", "101.5", "102", "102.5"]
                )
            },
            quota_snapshots={
                "BTC-USD": QuotaSnapshot(
                    underlying_family="BTC-USD",
                    long_limit_total=Decimal("1"),
                    protected_long_quota_total=Decimal("1"),
                )
            },
            simulation_config=LabSimulationConfig(
                exit_mode="tp_sl_handoff",
                max_hold_bars=2,
                stop_loss_pct=Decimal("0.004"),
                take_profit_pct=Decimal("0.008"),
                stop_hit_mode="handoff_manual",
            ),
        )

        self.assertEqual(len(result.evidences), 1)
        evidence = result.evidences[0]
        self.assertEqual(result.events[0].evidence_id, evidence.evidence_id)
        self.assertEqual(evidence.signal_id, "sig-evidence")
        self.assertGreaterEqual(len(evidence.setup_candles), 1)
        self.assertIsNotNone(evidence.trigger_candle)
        self.assertEqual(evidence.trigger_candle.close, Decimal("101"))

        with TemporaryDirectory() as tmp_dir:
            export_paths = export_lab_report_markdown(result, Path(tmp_dir) / "evidence_lab.md")
            payload = json.loads(export_paths["json"].read_text(encoding="utf-8"))
            self.assertEqual(len(payload["evidences"]), 1)
            self.assertEqual(payload["events"][0]["evidence_id"], payload["evidences"][0]["evidence_id"])

            chart_bundle = export_evidence_chart_bundle(
                result=result,
                target_dir=Path(tmp_dir) / "charts",
                latest_reference_candles={("SPOT", "BTC-USDT"): self._build_5m_candles(["100", "100.5", "101", "101.5", "102", "102.5"])[-1]},
            )

            signal_states_csv = write_signal_states_csv(
                Path(tmp_dir) / "signal_states.csv",
                registry=registry,
                parent_strategy_id="spot_enhancement_36",
                base_simulation=LabSimulationConfig(),
            )
            evidence_index_csv = write_evidence_index_csv(
                Path(tmp_dir) / "evidence_index.csv",
                result,
                chart_paths_by_evidence=chart_bundle["chart_paths_by_evidence"],
            )
            evidences_json = write_evidences_json(Path(tmp_dir) / "evidences.json", result)
            evidence_chart_manifest_csv = write_evidence_chart_manifest_csv(
                Path(tmp_dir) / "evidence_chart_manifest.csv",
                chart_bundle["manifest_rows"],
            )
            manual_review_rows = build_manual_review_rows(
                [
                    {
                        "manual_pool": "manual",
                        "signal_id": evidence.signal_id,
                        "signal_name": evidence.signal_name,
                        "playbook_id": evidence.playbook_id,
                        "playbook_name": evidence.playbook_name,
                        "position_side": "long",
                        "entry_ts": result.events[0].candle_ts,
                        "handoff_ts": result.events[0].exit_ts,
                        "entry_price": result.events[0].entry_price,
                        "position_size": result.events[0].position_size,
                        "latest_reference_price": Decimal("102.5"),
                        "break_even_reference_price": Decimal("101.2"),
                        "group_net_break_even_reference_price": Decimal("101.2"),
                        "group_unified_close_reference_price": Decimal("101.2"),
                        "group_buffer_to_unified_close_pct": Decimal("1.2846"),
                        "group_target_reduce_reference_price": Decimal("101.7"),
                        "group_buffer_to_target_reduce_pct": Decimal("0.7866"),
                        "group_target_small_profit_reference_price": Decimal("102.2"),
                        "group_buffer_to_target_small_profit_pct": Decimal("0.2935"),
                        "group_risk_priority_rank": 4,
                        "group_risk_priority_label": "P4_达到小赚区",
                        "group_risk_priority_note": "已经达到目标小赚线，可以优先考虑统一锁盈或择机整组处理。",
                        "group_suggested_action": "可择机统一锁盈",
                        "estimated_net_pnl_value_if_closed_now": Decimal("1.25"),
                        "bars_since_entry_to_reference": 3,
                        "trigger_reason": result.events[0].trigger_reason,
                        "evidence_summary": result.events[0].evidence_summary,
                        "evidence_id": evidence.evidence_id,
                        "ledger_id": evidence.evidence_id,
                        "lifecycle_status": result.events[0].lifecycle_status,
                        "chart_path": chart_bundle["chart_paths_by_evidence"][evidence.evidence_id],
                    }
                ]
            )
            manual_review_manifest_csv = write_manual_review_manifest_csv(
                Path(tmp_dir) / "manual_review_manifest.csv",
                manual_review_rows,
            )
            manual_review_gallery_html = write_manual_review_gallery_html(
                Path(tmp_dir) / "manual_review_gallery.html",
                parent_strategy_name=PARENT_STRATEGY_NAME,
                rows=manual_review_rows,
            )
            total_position_summary_rows = build_total_position_summary_rows(
                [
                    {
                        "parent_strategy_id": "spot_enhancement_36",
                        "parent_strategy_name": PARENT_STRATEGY_NAME,
                        "manual_pool": "manual",
                        "underlying_family": "BTC-USD",
                        "source_market": "SPOT",
                        "source_inst_id": "BTC-USDT",
                        "source_bar": "5m",
                        "playbook_id": evidence.playbook_id,
                        "playbook_name": evidence.playbook_name,
                        "playbook_action": "SWAP_LONG",
                        "position_side": "long",
                        "signal_id": evidence.signal_id,
                        "signal_name": evidence.signal_name,
                        "active_position_count": 1,
                        "total_position_size": Decimal("1"),
                        "total_position_cost_value": Decimal("101"),
                        "weighted_avg_entry_price": Decimal("101"),
                        "latest_reference_price": Decimal("102.5"),
                        "weighted_break_even_reference_price": Decimal("101.2"),
                        "reference_line_capacity_value": Decimal("1"),
                        "reference_line_constant_value": Decimal("101.2"),
                        "net_break_even_reference_price": Decimal("101.2"),
                        "unified_close_reference_price": Decimal("101.2"),
                        "buffer_to_unified_close_pct": Decimal("1.2846"),
                        "buffer_to_unified_close_value": Decimal("1.3"),
                        "target_reduce_net_pnl_pct": Decimal("0.5"),
                        "target_reduce_net_pnl_value": Decimal("0.505"),
                        "target_reduce_reference_price": Decimal("101.7"),
                        "buffer_to_target_reduce_pct": Decimal("0.7866"),
                        "buffer_to_target_reduce_value": Decimal("0.8"),
                        "target_small_profit_net_pnl_pct": Decimal("1.0"),
                        "target_small_profit_net_pnl_value": Decimal("1.01"),
                        "target_small_profit_reference_price": Decimal("102.2"),
                        "buffer_to_target_small_profit_pct": Decimal("0.2935"),
                        "buffer_to_target_small_profit_value": Decimal("0.3"),
                        "risk_priority_rank": 4,
                        "risk_priority_label": "P4_达到小赚区",
                        "risk_priority_note": "已经达到目标小赚线，可以优先考虑统一锁盈或择机整组处理。",
                        "suggested_action": "可择机统一锁盈",
                        "estimated_gross_pnl_value_if_closed_now": Decimal("1.5"),
                        "entry_fee_cost_value": Decimal("0.1"),
                        "estimated_exit_fee_cost_value_if_closed_now": Decimal("0.1"),
                        "estimated_fee_cost_value_if_closed_now": Decimal("0.2"),
                        "entry_slippage_cost_value": Decimal("0.05"),
                        "estimated_exit_slippage_cost_value_if_closed_now": Decimal("0.05"),
                        "estimated_slippage_cost_value_if_closed_now": Decimal("0.1"),
                        "estimated_funding_cost_value_to_reference": Decimal("0"),
                        "estimated_net_pnl_value_if_closed_now": Decimal("1.25"),
                        "estimated_net_pnl_pct_if_closed_now": Decimal("1.2376"),
                        "reference_ts": result.events[0].exit_ts or result.events[0].candle_ts,
                        "first_entry_ts": result.events[0].candle_ts,
                        "last_entry_ts": result.events[0].candle_ts,
                        "evidence_ids": evidence.evidence_id,
                    }
                ]
            )
            position_management_html = write_position_management_html(
                Path(tmp_dir) / "position_management.html",
                parent_strategy_name=PARENT_STRATEGY_NAME,
                summary_rows=[
                    {
                        "parent_strategy_id": "spot_enhancement_36",
                        "parent_strategy_name": PARENT_STRATEGY_NAME,
                        "manual_pool": "manual",
                        "underlying_family": "BTC-USD",
                        "source_market": "SPOT",
                        "source_inst_id": "BTC-USDT",
                        "source_bar": "5m",
                        "playbook_id": evidence.playbook_id,
                        "playbook_name": evidence.playbook_name,
                        "playbook_action": "SWAP_LONG",
                        "position_side": "long",
                        "signal_id": evidence.signal_id,
                        "signal_name": evidence.signal_name,
                        "active_position_count": 1,
                        "total_position_size": Decimal("1"),
                        "total_position_cost_value": Decimal("101"),
                        "weighted_avg_entry_price": Decimal("101"),
                        "latest_reference_price": Decimal("102.5"),
                        "weighted_break_even_reference_price": Decimal("101.2"),
                        "reference_line_capacity_value": Decimal("1"),
                        "reference_line_constant_value": Decimal("101.2"),
                        "net_break_even_reference_price": Decimal("101.2"),
                        "unified_close_reference_price": Decimal("101.2"),
                        "buffer_to_unified_close_pct": Decimal("1.2846"),
                        "buffer_to_unified_close_value": Decimal("1.3"),
                        "target_reduce_net_pnl_pct": Decimal("0.5"),
                        "target_reduce_net_pnl_value": Decimal("0.505"),
                        "target_reduce_reference_price": Decimal("101.7"),
                        "buffer_to_target_reduce_pct": Decimal("0.7866"),
                        "buffer_to_target_reduce_value": Decimal("0.8"),
                        "target_small_profit_net_pnl_pct": Decimal("1.0"),
                        "target_small_profit_net_pnl_value": Decimal("1.01"),
                        "target_small_profit_reference_price": Decimal("102.2"),
                        "buffer_to_target_small_profit_pct": Decimal("0.2935"),
                        "buffer_to_target_small_profit_value": Decimal("0.3"),
                        "risk_priority_rank": 4,
                        "risk_priority_label": "P4_达到小赚区",
                        "risk_priority_note": "已经达到目标小赚线，可以优先考虑统一锁盈或择机整组处理。",
                        "suggested_action": "可择机统一锁盈",
                        "estimated_gross_pnl_value_if_closed_now": Decimal("1.5"),
                        "entry_fee_cost_value": Decimal("0.1"),
                        "estimated_exit_fee_cost_value_if_closed_now": Decimal("0.1"),
                        "estimated_fee_cost_value_if_closed_now": Decimal("0.2"),
                        "entry_slippage_cost_value": Decimal("0.05"),
                        "estimated_exit_slippage_cost_value_if_closed_now": Decimal("0.05"),
                        "estimated_slippage_cost_value_if_closed_now": Decimal("0.1"),
                        "estimated_funding_cost_value_to_reference": Decimal("0"),
                        "estimated_net_pnl_value_if_closed_now": Decimal("1.25"),
                        "estimated_net_pnl_pct_if_closed_now": Decimal("1.2376"),
                        "reference_ts": result.events[0].exit_ts or result.events[0].candle_ts,
                        "first_entry_ts": result.events[0].candle_ts,
                        "last_entry_ts": result.events[0].candle_ts,
                        "evidence_ids": evidence.evidence_id,
                    }
                ],
                ledger_rows=manual_review_rows,
            )
            total_position_summary_csv = write_total_position_summary_csv(
                Path(tmp_dir) / "total_position_summary.csv",
                total_position_summary_rows,
            )
            total_position_management_html = write_total_position_management_html(
                Path(tmp_dir) / "total_position_management.html",
                parent_strategy_name=PARENT_STRATEGY_NAME,
                total_summary_rows=total_position_summary_rows,
                summary_rows=[
                    {
                        "parent_strategy_id": "spot_enhancement_36",
                        "parent_strategy_name": PARENT_STRATEGY_NAME,
                        "manual_pool": "manual",
                        "underlying_family": "BTC-USD",
                        "source_market": "SPOT",
                        "source_inst_id": "BTC-USDT",
                        "source_bar": "5m",
                        "playbook_id": evidence.playbook_id,
                        "playbook_name": evidence.playbook_name,
                        "playbook_action": "SWAP_LONG",
                        "position_side": "long",
                        "signal_id": evidence.signal_id,
                        "signal_name": evidence.signal_name,
                        "active_position_count": 1,
                        "total_position_size": Decimal("1"),
                        "total_position_cost_value": Decimal("101"),
                        "weighted_avg_entry_price": Decimal("101"),
                        "latest_reference_price": Decimal("102.5"),
                        "weighted_break_even_reference_price": Decimal("101.2"),
                        "reference_line_capacity_value": Decimal("1"),
                        "reference_line_constant_value": Decimal("101.2"),
                        "net_break_even_reference_price": Decimal("101.2"),
                        "unified_close_reference_price": Decimal("101.2"),
                        "buffer_to_unified_close_pct": Decimal("1.2846"),
                        "buffer_to_unified_close_value": Decimal("1.3"),
                        "target_reduce_net_pnl_pct": Decimal("0.5"),
                        "target_reduce_net_pnl_value": Decimal("0.505"),
                        "target_reduce_reference_price": Decimal("101.7"),
                        "buffer_to_target_reduce_pct": Decimal("0.7866"),
                        "buffer_to_target_reduce_value": Decimal("0.8"),
                        "target_small_profit_net_pnl_pct": Decimal("1.0"),
                        "target_small_profit_net_pnl_value": Decimal("1.01"),
                        "target_small_profit_reference_price": Decimal("102.2"),
                        "buffer_to_target_small_profit_pct": Decimal("0.2935"),
                        "buffer_to_target_small_profit_value": Decimal("0.3"),
                        "risk_priority_rank": 4,
                        "risk_priority_label": "P4_达到小赚区",
                        "risk_priority_note": "已经达到目标小赚线，可以优先考虑统一锁盈或择机整组处理。",
                        "suggested_action": "可择机统一锁盈",
                        "estimated_gross_pnl_value_if_closed_now": Decimal("1.5"),
                        "entry_fee_cost_value": Decimal("0.1"),
                        "estimated_exit_fee_cost_value_if_closed_now": Decimal("0.1"),
                        "estimated_fee_cost_value_if_closed_now": Decimal("0.2"),
                        "entry_slippage_cost_value": Decimal("0.05"),
                        "estimated_exit_slippage_cost_value_if_closed_now": Decimal("0.05"),
                        "estimated_slippage_cost_value_if_closed_now": Decimal("0.1"),
                        "estimated_funding_cost_value_to_reference": Decimal("0"),
                        "estimated_net_pnl_value_if_closed_now": Decimal("1.25"),
                        "estimated_net_pnl_pct_if_closed_now": Decimal("1.2376"),
                        "reference_ts": result.events[0].exit_ts or result.events[0].candle_ts,
                        "first_entry_ts": result.events[0].candle_ts,
                        "last_entry_ts": result.events[0].candle_ts,
                        "evidence_ids": evidence.evidence_id,
                    }
                ],
                group_detail_path=Path(tmp_dir) / "position_management.html",
            )

            self.assertIn("signal_id", signal_states_csv.read_text(encoding="utf-8"))
            self.assertIn(evidence.evidence_id, evidence_index_csv.read_text(encoding="utf-8"))
            standalone_payload = json.loads(evidences_json.read_text(encoding="utf-8"))
            self.assertEqual(standalone_payload["count"], 1)
            self.assertEqual(
                standalone_payload["evidences"][0]["trigger_candle"]["close"],
                "101",
            )
            self.assertTrue((Path(tmp_dir) / "charts" / "index.html").exists())
            self.assertTrue(any(path.suffix == ".svg" for path in (Path(tmp_dir) / "charts").iterdir()))
            self.assertIn(".svg", evidence_chart_manifest_csv.read_text(encoding="utf-8"))
            self.assertIn(".svg", evidence_index_csv.read_text(encoding="utf-8"))
            self.assertIn(".svg", manual_review_manifest_csv.read_text(encoding="utf-8"))
            self.assertIn("人工接管专用画廊", manual_review_gallery_html.read_text(encoding="utf-8"))
            self.assertIn("人工处理清单", position_management_html.read_text(encoding="utf-8"))
            self.assertIn("group_target_reduce_reference_price", manual_review_manifest_csv.read_text(encoding="utf-8"))
            self.assertIn("目标减仓线", position_management_html.read_text(encoding="utf-8"))
            self.assertIn("pool_state", total_position_summary_csv.read_text(encoding="utf-8"))
            self.assertIn("总持仓总览", total_position_management_html.read_text(encoding="utf-8"))

    def test_active_position_ledger_tracks_manual_handoff_costs(self) -> None:
        registry = EnhancedStrategyRegistry()
        registry.register_parent_strategy(
            ParentStrategyConfig(
                strategy_id="spot_enhancement_36",
                strategy_name=PARENT_STRATEGY_NAME,
            )
        )
        registry.register_child_signal(
            "spot_enhancement_36",
            ChildSignalConfig(
                signal_id="sig-manual-ledger",
                signal_name="Manual Ledger Test",
                source=SignalSource(market="SPOT", inst_id="BTC-USDT", bar="5m"),
                underlying_family="BTC-USD",
                direction_bias="long",
                trigger_rule_id="manual_ledger_rule",
                invalidation_rule_id="never",
                evidence_template_id="default",
            ),
        )
        registry.register_playbook(
            ExecutionPlaybookConfig(
                playbook_id="swap-long",
                playbook_name="swap long",
                action="SWAP_LONG",
                underlying_family="BTC-USD",
                sizing_mode="fixed_slot",
                slot_size=Decimal("1"),
            )
        )
        registry.register_trigger_rule(
            "manual_ledger_rule",
            lambda candles, index: (
                SignalRuleMatch(True, "manual ledger trigger", candles[index].close) if index in {1, 4} else None
            ),
        )
        registry.register_invalidation_rule("never", lambda candles, index: None)
        registry.bind_signal("sig-manual-ledger", playbook_ids=["swap-long"])

        candles = self._build_candles(
            [
                ("100", "100.2", "99.8", "100"),
                ("100", "100.5", "99.9", "100"),
                ("100", "100.1", "99.0", "99.2"),
                ("99.2", "99.5", "98.8", "99.1"),
                ("99.1", "100.4", "99.0", "100"),
                ("100", "100.5", "99.8", "100.1"),
            ]
        )
        result = EnhancedBacktestLab(registry).run(
            parent_strategy_id="spot_enhancement_36",
            candle_feeds={("SPOT", "BTC-USDT"): candles},
            quota_snapshots={
                "BTC-USD": QuotaSnapshot(
                    underlying_family="BTC-USD",
                    long_limit_total=Decimal("1"),
                    protected_long_quota_total=Decimal("1"),
                )
            },
            simulation_config=LabSimulationConfig(
                exit_mode="tp_sl_handoff",
                max_hold_bars=3,
                stop_loss_pct=Decimal("0.005"),
                take_profit_pct=Decimal("0.01"),
                stop_hit_mode="handoff_manual",
            ),
        )

        ledger_rows = build_active_position_ledger_rows(
            result=result,
            playbook_actions={"swap-long": "SWAP_LONG"},
            latest_reference_candles={("SPOT", "BTC-USDT"): candles[-1]},
        )
        self.assertEqual(len(ledger_rows), 1)
        row = ledger_rows[0]
        self.assertEqual(row["manual_pool"], "manual")
        self.assertEqual(row["position_side"], "long")
        self.assertEqual(row["latest_reference_price"], Decimal("100.1"))
        self.assertEqual(row["break_even_reference_price"], Decimal("100"))
        self.assertEqual(row["reference_line_capacity_value"], Decimal("1"))
        self.assertEqual(row["reference_line_constant_value"], Decimal("100"))
        self.assertEqual(row["estimated_net_pnl_value_if_closed_now"], Decimal("0.1"))
        self.assertEqual(row["handoff_reason"], "stop_loss_handoff")

        summary_rows = build_active_position_summary_rows(ledger_rows)
        self.assertEqual(len(summary_rows), 1)
        self.assertEqual(summary_rows[0]["active_position_count"], 1)
        self.assertEqual(summary_rows[0]["weighted_avg_entry_price"], Decimal("100"))
        self.assertEqual(summary_rows[0]["net_break_even_reference_price"], Decimal("100"))
        self.assertEqual(summary_rows[0]["unified_close_reference_price"], Decimal("100"))
        self.assertEqual(summary_rows[0]["target_reduce_reference_price"], Decimal("100.5"))
        self.assertEqual(summary_rows[0]["target_small_profit_reference_price"], Decimal("101"))
        self.assertEqual(summary_rows[0]["buffer_to_unified_close_pct"], Decimal("0.100"))
        self.assertEqual(summary_rows[0]["risk_priority_label"], "P3_盈利保护组")
        self.assertEqual(summary_rows[0]["estimated_net_pnl_value_if_closed_now"], Decimal("0.1"))
        total_summary_rows = build_total_position_summary_rows(summary_rows)
        self.assertEqual(total_summary_rows[0]["manual_position_count"], 1)
        self.assertEqual(total_summary_rows[0]["auto_position_count"], 0)
        self.assertEqual(total_summary_rows[0]["pool_state"], "manual_only")
        self.assertEqual(total_summary_rows[0]["target_reduce_reference_price"], Decimal("100.5"))

        with TemporaryDirectory() as tmp_dir:
            chart_bundle = export_evidence_chart_bundle(
                result=result,
                target_dir=Path(tmp_dir) / "charts",
                latest_reference_candles={("SPOT", "BTC-USDT"): candles[-1]},
            )
            for item in ledger_rows:
                item["chart_path"] = chart_bundle["chart_paths_by_evidence"].get(str(item["evidence_id"]), "")
            manual_review_rows = build_manual_review_rows(ledger_rows, summary_rows=summary_rows)
            self.assertEqual(manual_review_rows[0]["group_net_break_even_reference_price"], Decimal("100"))
            self.assertEqual(manual_review_rows[0]["group_unified_close_reference_price"], Decimal("100"))
            self.assertEqual(manual_review_rows[0]["group_target_reduce_reference_price"], Decimal("100.5"))
            self.assertEqual(manual_review_rows[0]["group_target_small_profit_reference_price"], Decimal("101"))
            self.assertEqual(manual_review_rows[0]["group_risk_priority_label"], "P3_盈利保护组")
            ledger_csv = write_position_ledger_csv(Path(tmp_dir) / "position_ledger.csv", ledger_rows)
            summary_csv = write_position_ledger_summary_csv(
                Path(tmp_dir) / "position_ledger_summary.csv",
                summary_rows,
            )
            manual_review_manifest_csv = write_manual_review_manifest_csv(
                Path(tmp_dir) / "manual_review_manifest.csv",
                manual_review_rows,
            )
            manual_review_gallery_html = write_manual_review_gallery_html(
                Path(tmp_dir) / "manual_review_gallery.html",
                parent_strategy_name=PARENT_STRATEGY_NAME,
                rows=manual_review_rows,
            )
            position_management_html = write_position_management_html(
                Path(tmp_dir) / "position_management.html",
                parent_strategy_name=PARENT_STRATEGY_NAME,
                summary_rows=summary_rows,
                ledger_rows=manual_review_rows,
            )
            total_summary_csv = write_total_position_summary_csv(
                Path(tmp_dir) / "total_position_summary.csv",
                total_summary_rows,
            )
            total_position_management_html = write_total_position_management_html(
                Path(tmp_dir) / "total_position_management.html",
                parent_strategy_name=PARENT_STRATEGY_NAME,
                total_summary_rows=total_summary_rows,
                summary_rows=summary_rows,
                group_detail_path=Path(tmp_dir) / "position_management.html",
            )
            ledger_text = ledger_csv.read_text(encoding="utf-8")
            summary_text = summary_csv.read_text(encoding="utf-8")
            self.assertIn("manual_pool", ledger_text)
            self.assertIn("sig-manual-ledger", ledger_text)
            self.assertIn("0.1", ledger_text)
            self.assertIn(".svg", ledger_text)
            self.assertIn("weighted_avg_entry_price", summary_text)
            self.assertIn("100.1", summary_text)
            self.assertIn("risk_priority_label", summary_text)
            self.assertIn("P3_盈利保护组", summary_text)
            self.assertIn("target_reduce_reference_price", summary_text)
            self.assertIn("target_small_profit_reference_price", summary_text)
            self.assertIn("sig-manual-ledger", manual_review_manifest_csv.read_text(encoding="utf-8"))
            self.assertIn("group_unified_close_reference_price", manual_review_manifest_csv.read_text(encoding="utf-8"))
            self.assertIn("group_target_reduce_reference_price", manual_review_manifest_csv.read_text(encoding="utf-8"))
            self.assertIn("sig-manual-ledger", manual_review_gallery_html.read_text(encoding="utf-8"))
            manage_text = position_management_html.read_text(encoding="utf-8")
            self.assertIn("看图", manage_text)
            self.assertIn("目标减仓线", manage_text)
            self.assertIn("三线参考", manage_text)
            self.assertIn("pool_state", total_summary_csv.read_text(encoding="utf-8"))
            self.assertIn("总持仓总览", total_position_management_html.read_text(encoding="utf-8"))

    def test_active_position_summary_builds_multi_reference_lines_for_short_group(self) -> None:
        summary_rows = build_active_position_summary_rows(
            [
                {
                    "parent_strategy_id": "spot_enhancement_36",
                    "parent_strategy_name": PARENT_STRATEGY_NAME,
                    "manual_pool": "manual",
                    "underlying_family": "BTC-USD",
                    "source_market": "SPOT",
                    "source_inst_id": "BTC-USDT",
                    "source_bar": "5m",
                    "playbook_id": "swap-short",
                    "playbook_name": "swap short",
                    "playbook_action": "SWAP_SHORT",
                    "position_side": "short",
                    "signal_id": "sig-short-grid",
                    "signal_name": "Short Grid",
                    "position_size": Decimal("2"),
                    "entry_price": Decimal("100"),
                    "position_cost_value": Decimal("200"),
                    "latest_reference_price": Decimal("99"),
                    "break_even_reference_price": Decimal("100"),
                    "estimated_gross_pnl_value_if_closed_now": Decimal("2"),
                    "entry_fee_cost_value": Decimal("0"),
                    "estimated_exit_fee_cost_value_if_closed_now": Decimal("0"),
                    "estimated_fee_cost_value_if_closed_now": Decimal("0"),
                    "entry_slippage_cost_value": Decimal("0"),
                    "estimated_exit_slippage_cost_value_if_closed_now": Decimal("0"),
                    "estimated_slippage_cost_value_if_closed_now": Decimal("0"),
                    "estimated_funding_cost_value_to_reference": Decimal("0"),
                    "estimated_net_pnl_value_if_closed_now": Decimal("2"),
                    "reference_line_capacity_value": Decimal("2"),
                    "reference_line_constant_value": Decimal("200"),
                    "reference_ts": 6,
                    "entry_ts": 1,
                    "evidence_id": "short-evidence-1",
                }
            ]
        )

        self.assertEqual(len(summary_rows), 1)
        self.assertEqual(summary_rows[0]["net_break_even_reference_price"], Decimal("100"))
        self.assertEqual(summary_rows[0]["target_reduce_reference_price"], Decimal("99.5"))
        self.assertEqual(summary_rows[0]["target_small_profit_reference_price"], Decimal("99"))
        self.assertEqual(summary_rows[0]["buffer_to_unified_close_pct"], Decimal("1.00"))
        self.assertEqual(summary_rows[0]["buffer_to_target_small_profit_pct"], Decimal("0"))
        self.assertEqual(summary_rows[0]["risk_priority_label"], "P4_达到小赚区")

    def test_total_position_summary_merges_manual_and_auto_groups(self) -> None:
        total_summary_rows = build_total_position_summary_rows(
            [
                {
                    "parent_strategy_id": "spot_enhancement_36",
                    "parent_strategy_name": PARENT_STRATEGY_NAME,
                    "manual_pool": "manual",
                    "underlying_family": "BTC-USD",
                    "source_market": "SPOT",
                    "source_inst_id": "BTC-USDT",
                    "source_bar": "5m",
                    "playbook_id": "swap-long-a",
                    "playbook_name": "swap long a",
                    "playbook_action": "SWAP_LONG",
                    "position_side": "long",
                    "signal_id": "sig-a",
                    "signal_name": "Signal A",
                    "active_position_count": 1,
                    "total_position_size": Decimal("1"),
                    "total_position_cost_value": Decimal("100"),
                    "weighted_avg_entry_price": Decimal("100"),
                    "latest_reference_price": Decimal("101"),
                    "weighted_break_even_reference_price": Decimal("100"),
                    "reference_line_capacity_value": Decimal("1"),
                    "reference_line_constant_value": Decimal("100"),
                    "estimated_gross_pnl_value_if_closed_now": Decimal("1"),
                    "entry_fee_cost_value": Decimal("0"),
                    "estimated_exit_fee_cost_value_if_closed_now": Decimal("0"),
                    "estimated_fee_cost_value_if_closed_now": Decimal("0"),
                    "entry_slippage_cost_value": Decimal("0"),
                    "estimated_exit_slippage_cost_value_if_closed_now": Decimal("0"),
                    "estimated_slippage_cost_value_if_closed_now": Decimal("0"),
                    "estimated_funding_cost_value_to_reference": Decimal("0"),
                    "estimated_net_pnl_value_if_closed_now": Decimal("1"),
                    "estimated_net_pnl_pct_if_closed_now": Decimal("1"),
                    "reference_ts": 10,
                    "first_entry_ts": 1,
                    "last_entry_ts": 1,
                    "risk_priority_rank": 3,
                    "risk_priority_label": "P3_盈利保护组",
                    "risk_priority_note": "",
                    "suggested_action": "",
                },
                {
                    "parent_strategy_id": "spot_enhancement_36",
                    "parent_strategy_name": PARENT_STRATEGY_NAME,
                    "manual_pool": "auto",
                    "underlying_family": "BTC-USD",
                    "source_market": "SPOT",
                    "source_inst_id": "BTC-USDT",
                    "source_bar": "5m",
                    "playbook_id": "swap-long-b",
                    "playbook_name": "swap long b",
                    "playbook_action": "SWAP_LONG",
                    "position_side": "long",
                    "signal_id": "sig-b",
                    "signal_name": "Signal B",
                    "active_position_count": 2,
                    "total_position_size": Decimal("2"),
                    "total_position_cost_value": Decimal("200"),
                    "weighted_avg_entry_price": Decimal("100"),
                    "latest_reference_price": Decimal("101"),
                    "weighted_break_even_reference_price": Decimal("100"),
                    "reference_line_capacity_value": Decimal("2"),
                    "reference_line_constant_value": Decimal("200"),
                    "estimated_gross_pnl_value_if_closed_now": Decimal("2"),
                    "entry_fee_cost_value": Decimal("0"),
                    "estimated_exit_fee_cost_value_if_closed_now": Decimal("0"),
                    "estimated_fee_cost_value_if_closed_now": Decimal("0"),
                    "entry_slippage_cost_value": Decimal("0"),
                    "estimated_exit_slippage_cost_value_if_closed_now": Decimal("0"),
                    "estimated_slippage_cost_value_if_closed_now": Decimal("0"),
                    "estimated_funding_cost_value_to_reference": Decimal("0"),
                    "estimated_net_pnl_value_if_closed_now": Decimal("2"),
                    "estimated_net_pnl_pct_if_closed_now": Decimal("1"),
                    "reference_ts": 10,
                    "first_entry_ts": 2,
                    "last_entry_ts": 2,
                    "risk_priority_rank": 4,
                    "risk_priority_label": "P4_达到小赚区",
                    "risk_priority_note": "",
                    "suggested_action": "",
                },
            ]
        )

        self.assertEqual(len(total_summary_rows), 1)
        self.assertEqual(total_summary_rows[0]["pool_state"], "mixed")
        self.assertEqual(total_summary_rows[0]["manual_position_count"], 1)
        self.assertEqual(total_summary_rows[0]["auto_position_count"], 2)
        self.assertEqual(total_summary_rows[0]["signal_coverage_count"], 2)
        self.assertEqual(total_summary_rows[0]["signal_ids"], "sig-a; sig-b")
        self.assertEqual(total_summary_rows[0]["bucket_mode"], TOTAL_BUCKET_MODE_DEFAULT)
        self.assertEqual(total_summary_rows[0]["bucket_mode_label"], "按标的家族+方向")
        self.assertEqual(total_summary_rows[0]["target_reduce_reference_price"], Decimal("100.5"))

    def test_total_position_summary_supports_bucket_mode_switch(self) -> None:
        summary_rows = [
            {
                "parent_strategy_id": "spot_enhancement_36",
                "parent_strategy_name": PARENT_STRATEGY_NAME,
                "manual_pool": "manual",
                "underlying_family": "BTC-USD",
                "source_market": "SPOT",
                "source_inst_id": "BTC-USDT",
                "source_bar": "5m",
                "playbook_id": "spot-buy-a",
                "playbook_name": "spot buy a",
                "playbook_action": "SPOT_BUY",
                "position_side": "long",
                "signal_id": "sig-spot-a",
                "signal_name": "Spot A",
                "active_position_count": 1,
                "total_position_size": Decimal("1"),
                "total_position_cost_value": Decimal("100"),
                "weighted_avg_entry_price": Decimal("100"),
                "latest_reference_price": Decimal("101"),
                "weighted_break_even_reference_price": Decimal("100"),
                "reference_line_capacity_value": Decimal("1"),
                "reference_line_constant_value": Decimal("100"),
                "estimated_gross_pnl_value_if_closed_now": Decimal("1"),
                "entry_fee_cost_value": Decimal("0"),
                "estimated_exit_fee_cost_value_if_closed_now": Decimal("0"),
                "estimated_fee_cost_value_if_closed_now": Decimal("0"),
                "entry_slippage_cost_value": Decimal("0"),
                "estimated_exit_slippage_cost_value_if_closed_now": Decimal("0"),
                "estimated_slippage_cost_value_if_closed_now": Decimal("0"),
                "estimated_funding_cost_value_to_reference": Decimal("0"),
                "estimated_net_pnl_value_if_closed_now": Decimal("1"),
                "estimated_net_pnl_pct_if_closed_now": Decimal("1"),
                "reference_ts": 10,
                "first_entry_ts": 1,
                "last_entry_ts": 1,
                "risk_priority_rank": 4,
                "risk_priority_label": "P4_达到小赚区",
                "risk_priority_note": "",
                "suggested_action": "",
            },
            {
                "parent_strategy_id": "spot_enhancement_36",
                "parent_strategy_name": PARENT_STRATEGY_NAME,
                "manual_pool": "auto",
                "underlying_family": "BTC-USD",
                "source_market": "SWAP",
                "source_inst_id": "BTC-USDT-SWAP",
                "source_bar": "15m",
                "playbook_id": "swap-long-b",
                "playbook_name": "swap long b",
                "playbook_action": "SWAP_LONG",
                "position_side": "long",
                "signal_id": "sig-swap-b",
                "signal_name": "Swap B",
                "active_position_count": 2,
                "total_position_size": Decimal("2"),
                "total_position_cost_value": Decimal("200"),
                "weighted_avg_entry_price": Decimal("100"),
                "latest_reference_price": Decimal("101"),
                "weighted_break_even_reference_price": Decimal("100"),
                "reference_line_capacity_value": Decimal("2"),
                "reference_line_constant_value": Decimal("200"),
                "estimated_gross_pnl_value_if_closed_now": Decimal("2"),
                "entry_fee_cost_value": Decimal("0"),
                "estimated_exit_fee_cost_value_if_closed_now": Decimal("0"),
                "estimated_fee_cost_value_if_closed_now": Decimal("0"),
                "entry_slippage_cost_value": Decimal("0"),
                "estimated_exit_slippage_cost_value_if_closed_now": Decimal("0"),
                "estimated_slippage_cost_value_if_closed_now": Decimal("0"),
                "estimated_funding_cost_value_to_reference": Decimal("0"),
                "estimated_net_pnl_value_if_closed_now": Decimal("2"),
                "estimated_net_pnl_pct_if_closed_now": Decimal("1"),
                "reference_ts": 10,
                "first_entry_ts": 2,
                "last_entry_ts": 2,
                "risk_priority_rank": 4,
                "risk_priority_label": "P4_达到小赚区",
                "risk_priority_note": "",
                "suggested_action": "",
            },
        ]

        default_rows = build_total_position_summary_rows(summary_rows)
        strict_rows = build_total_position_summary_rows(
            summary_rows,
            bucket_mode=TOTAL_BUCKET_MODE_PLAYBOOK_SOURCE,
        )

        self.assertEqual(TOTAL_BUCKET_MODE_DEFAULT, "underlying_direction")
        self.assertEqual(len(default_rows), 1)
        self.assertEqual(len(strict_rows), 2)
        self.assertEqual(default_rows[0]["bucket_mode"], "underlying_direction")
        self.assertEqual(default_rows[0]["bucket_mode_label"], "按标的家族+方向")
        self.assertEqual(default_rows[0]["playbook_actions"], "SPOT_BUY; SWAP_LONG")
        self.assertEqual(default_rows[0]["source_count"], 2)

    def test_total_position_summary_keeps_option_price_domain_separate_in_underlying_mode(self) -> None:
        summary_rows = [
            {
                "parent_strategy_id": "spot_enhancement_36",
                "parent_strategy_name": PARENT_STRATEGY_NAME,
                "manual_pool": "manual",
                "underlying_family": "BTC-USD",
                "source_market": "SWAP",
                "source_inst_id": "BTC-USDT-SWAP",
                "source_bar": "5m",
                "playbook_id": "swap-short-a",
                "playbook_name": "swap short a",
                "playbook_action": "SWAP_SHORT",
                "position_side": "short",
                "signal_id": "sig-swap-short",
                "signal_name": "Swap Short",
                "active_position_count": 1,
                "total_position_size": Decimal("1"),
                "total_position_cost_value": Decimal("100"),
                "weighted_avg_entry_price": Decimal("100"),
                "latest_reference_price": Decimal("99"),
                "weighted_break_even_reference_price": Decimal("100"),
                "reference_line_capacity_value": Decimal("1"),
                "reference_line_constant_value": Decimal("100"),
                "estimated_gross_pnl_value_if_closed_now": Decimal("1"),
                "entry_fee_cost_value": Decimal("0"),
                "estimated_exit_fee_cost_value_if_closed_now": Decimal("0"),
                "estimated_fee_cost_value_if_closed_now": Decimal("0"),
                "entry_slippage_cost_value": Decimal("0"),
                "estimated_exit_slippage_cost_value_if_closed_now": Decimal("0"),
                "estimated_slippage_cost_value_if_closed_now": Decimal("0"),
                "estimated_funding_cost_value_to_reference": Decimal("0"),
                "estimated_net_pnl_value_if_closed_now": Decimal("1"),
                "estimated_net_pnl_pct_if_closed_now": Decimal("1"),
                "reference_ts": 10,
                "first_entry_ts": 1,
                "last_entry_ts": 1,
                "risk_priority_rank": 4,
                "risk_priority_label": "P4_达到小赚区",
                "risk_priority_note": "",
                "suggested_action": "",
            },
            {
                "parent_strategy_id": "spot_enhancement_36",
                "parent_strategy_name": PARENT_STRATEGY_NAME,
                "manual_pool": "auto",
                "underlying_family": "BTC-USD",
                "source_market": "OPTION",
                "source_inst_id": "BTC-USD-260630-90000-C",
                "source_bar": "5m",
                "playbook_id": "option-short-call",
                "playbook_name": "option short call",
                "playbook_action": "OPTION_SHORT_CALL",
                "position_side": "short",
                "signal_id": "sig-option-short",
                "signal_name": "Option Short",
                "active_position_count": 1,
                "total_position_size": Decimal("1"),
                "total_position_cost_value": Decimal("10"),
                "weighted_avg_entry_price": Decimal("10"),
                "latest_reference_price": Decimal("9"),
                "weighted_break_even_reference_price": Decimal("10"),
                "reference_line_capacity_value": Decimal("1"),
                "reference_line_constant_value": Decimal("10"),
                "estimated_gross_pnl_value_if_closed_now": Decimal("1"),
                "entry_fee_cost_value": Decimal("0"),
                "estimated_exit_fee_cost_value_if_closed_now": Decimal("0"),
                "estimated_fee_cost_value_if_closed_now": Decimal("0"),
                "entry_slippage_cost_value": Decimal("0"),
                "estimated_exit_slippage_cost_value_if_closed_now": Decimal("0"),
                "estimated_slippage_cost_value_if_closed_now": Decimal("0"),
                "estimated_funding_cost_value_to_reference": Decimal("0"),
                "estimated_net_pnl_value_if_closed_now": Decimal("1"),
                "estimated_net_pnl_pct_if_closed_now": Decimal("10"),
                "reference_ts": 10,
                "first_entry_ts": 2,
                "last_entry_ts": 2,
                "risk_priority_rank": 4,
                "risk_priority_label": "P4_达到小赚区",
                "risk_priority_note": "",
                "suggested_action": "",
            },
        ]

        merged_rows = build_total_position_summary_rows(summary_rows, bucket_mode="underlying_direction")

        self.assertEqual(len(merged_rows), 2)
        self.assertEqual(
            {row["price_domain_label"] for row in merged_rows},
            {"BTC-USD", "BTC-USD-260630-90000-C"},
        )

    def test_seed_strategy_package_registers_four_templates_and_exports_report(self) -> None:
        registry = EnhancedStrategyRegistry()
        register_seed_strategy_package(registry)
        child_signals = registry.list_child_signals("spot_enhancement_36")
        self.assertEqual({item.signal_id for item in child_signals}, set(SEED_SIGNAL_IDS))
        self.assertEqual(len(registry.list_signal_lab_profiles("spot_enhancement_36")), 4)

        lab = EnhancedBacktestLab(registry)
        result = lab.run(
            parent_strategy_id="spot_enhancement_36",
            candle_feeds={
                ("SPOT", "BTC-USDT"): self._build_5m_candles(
                    [
                        "100", "101", "102", "103", "104", "102", "104", "106", "108", "107", "109", "111",
                        "110", "108", "106", "104", "103", "101", "100", "98", "99", "101", "103", "105",
                    ]
                )
            },
            quota_snapshots={
                "BTC-USD": QuotaSnapshot(
                    underlying_family="BTC-USD",
                    long_limit_total=Decimal("2"),
                    short_limit_total=Decimal("1"),
                    protected_long_quota_total=Decimal("2"),
                    protected_short_quota_total=Decimal("1"),
                )
            },
        )
        with TemporaryDirectory() as tmp_dir:
            paths = export_lab_report_markdown(result, Path(tmp_dir) / "seed_lab_report.md")
            self.assertTrue(paths["report"].exists())
            self.assertTrue(paths["json"].exists())
            content = paths["report"].read_text(encoding="utf-8")
            self.assertIn(PARENT_STRATEGY_NAME, content)
            self.assertIn("子策略参数表", content)
            self.assertIn("第01计_均线突破", content)
            self.assertIn("突破顺势_轻止损", content)

    def test_segmented_history_loader_merges_segments(self) -> None:
        base = self._build_5m_candles(["100", "101", "102", "103", "104", "105"])

        def fake_loader(inst_id, bar, limit, *, start_ts, end_ts, preload_count):
            candles = [item for item in base if start_ts <= item.ts <= end_ts]
            if preload_count > 0:
                candles = [item for item in base if item.ts < start_ts][-preload_count:] + candles
            return candles

        result = load_segmented_history_candles(
            fake_loader,
            inst_id="BTC-USDT",
            bar="5m",
            start_ts=2,
            end_ts=6,
            preload_count=1,
            segment_ms=2,
        )

        self.assertEqual(len(result.segments), 3)
        self.assertEqual(result.segments[0].preload_count, 1)
        self.assertEqual(result.segments[1].preload_count, 0)
        self.assertEqual([item.ts for item in result.candles], [1, 2, 3, 4, 5, 6])
