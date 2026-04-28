from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import MagicMock, patch

from okx_quant.signal_monitor_ui import SignalMonitorWindow, _ObserverDraft, _ObserverPreset, _normalize_template_payload
from okx_quant.strategy_catalog import STRATEGY_CROSS_ID, STRATEGY_DYNAMIC_LONG_ID, STRATEGY_EMA5_EMA8_ID, get_strategy_definition
from okx_quant.ui import QuantApp


class _SessionTreeStub:
    def __init__(self) -> None:
        self.rows: dict[str, dict[str, object]] = {}
        self._selection: list[str] = []
        self.focused: str | None = None
        self.seen: str | None = None

    @staticmethod
    def winfo_exists() -> bool:
        return True

    def exists(self, iid: str) -> bool:
        return iid in self.rows

    @staticmethod
    def identify_column(_x: int) -> str:
        return "#1"

    @staticmethod
    def identify_row(_y: int) -> str:
        return ""

    @staticmethod
    def identify_region(_x: int, _y: int) -> str:
        return "heading"

    def insert(self, _parent: str, _index: object, *, iid: str, values: tuple[object, ...]) -> None:
        self.rows[iid] = {"values": values}

    def item(self, iid: str, option: str | None = None):
        row = self.rows[iid]
        if option is None:
            return row
        return row.get(option)

    def delete(self, iid: str) -> None:
        self.rows.pop(iid, None)
        self._selection = [item for item in self._selection if item != iid]
        if self.focused == iid:
            self.focused = None
        if self.seen == iid:
            self.seen = None

    def get_children(self) -> tuple[str, ...]:
        return tuple(self.rows.keys())

    def selection(self) -> tuple[str, ...]:
        return tuple(self._selection)

    def selection_set(self, iid: str) -> None:
        self._selection = [iid]

    def focus(self, iid: str) -> None:
        self.focused = iid

    def see(self, iid: str) -> None:
        self.seen = iid


class _Var:
    def __init__(self, value: object = "") -> None:
        self._value = value

    def get(self) -> object:
        return self._value

    def set(self, value: object) -> None:
        self._value = value


class SignalObserverTemplateNormalizationTest(TestCase):
    def test_session_tree_double_click_hint_maps_supported_columns(self) -> None:
        self.assertEqual(SignalMonitorWindow._session_tree_double_click_hint("#1"), "双击打开这条会话的独立日志")
        self.assertEqual(SignalMonitorWindow._session_tree_double_click_hint("#3"), "双击打开这条会话的实时K线图")
        self.assertEqual(SignalMonitorWindow._session_tree_double_click_hint("#2"), "")

    def test_session_tree_double_click_opens_log_and_live_chart(self) -> None:
        tree = _SessionTreeStub()
        tree.rows["S01"] = {"values": ("S01", "EMA", "BTC-USDT-SWAP", "moni", "运行中", "-")}
        window = object.__new__(SignalMonitorWindow)
        window.session_tree = tree
        opened_logs: list[str] = []
        opened_charts: list[str] = []
        window._session_log_opener = opened_logs.append
        window._session_chart_opener = opened_charts.append

        tree.identify_row = lambda _y: "S01"
        tree.identify_column = lambda _x: "#1"
        result = SignalMonitorWindow._on_session_tree_double_click(window, SimpleNamespace(x=8, y=12))
        self.assertEqual(result, "break")
        self.assertEqual(opened_logs, ["S01"])

        tree.identify_column = lambda _x: "#3"
        result = SignalMonitorWindow._on_session_tree_double_click(window, SimpleNamespace(x=24, y=12))
        self.assertEqual(result, "break")
        self.assertEqual(opened_charts, ["S01"])

    def test_normalize_template_payload_applies_fixed_dynamic_signal_mode(self) -> None:
        payload = {
            "strategy_id": STRATEGY_DYNAMIC_LONG_ID,
            "strategy_name": "",
            "direction_label": "",
            "run_mode_label": "",
            "config_snapshot": {
                "strategy_id": STRATEGY_DYNAMIC_LONG_ID,
                "signal_mode": "both",
                "bar": "1H",
            },
        }

        normalized = _normalize_template_payload(payload)

        self.assertEqual(normalized["strategy_name"], get_strategy_definition(STRATEGY_DYNAMIC_LONG_ID).name)
        self.assertEqual(normalized["direction_label"], "只做多")
        self.assertEqual(normalized["run_mode_label"], "只发邮件")
        self.assertEqual(normalized["config_snapshot"]["signal_mode"], "long_only")

    def test_normalize_template_payload_applies_fixed_ema5_8_values(self) -> None:
        payload = {
            "strategy_id": STRATEGY_EMA5_EMA8_ID,
            "config_snapshot": {
                "strategy_id": STRATEGY_EMA5_EMA8_ID,
            },
        }

        normalized = _normalize_template_payload(payload)
        snapshot = normalized["config_snapshot"]

        self.assertEqual(snapshot["bar"], "4H")
        self.assertEqual(snapshot["ema_period"], 5)
        self.assertEqual(snapshot["trend_ema_period"], 8)
        self.assertEqual(snapshot["big_ema_period"], 233)

    def test_save_selected_draft_updates_payload_from_editor(self) -> None:
        draft = _ObserverDraft(
            draft_id="D001",
            template_payload={
                "strategy_id": STRATEGY_CROSS_ID,
                "strategy_name": get_strategy_definition(STRATEGY_CROSS_ID).name,
                "api_name": "moni",
                "direction_label": "双向",
                "run_mode_label": "只发邮件",
                "symbol": "BTC-USDT-SWAP",
                "config_snapshot": {
                    "strategy_id": STRATEGY_CROSS_ID,
                    "inst_id": "BTC-USDT-SWAP",
                    "bar": "1H",
                    "signal_mode": "both",
                    "ema_period": "21",
                    "trend_ema_period": "55",
                    "big_ema_period": "233",
                    "atr_period": "10",
                    "atr_stop_multiplier": "2",
                    "atr_take_multiplier": "4",
                },
            },
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )
        window = object.__new__(SignalMonitorWindow)
        window.window = object()
        window.draft_tree = None
        window._selected_single_draft = lambda: draft
        window._editor_symbol = _Var("ETH-USDT-SWAP")
        window._editor_parameter_vars = {
            "bar": _Var("4H"),
            "signal_mode": _Var("只做多"),
            "ema_period": _Var("34"),
            "trend_ema_period": _Var("89"),
            "big_ema_period": _Var("233"),
            "atr_period": _Var("14"),
            "atr_stop_multiplier": _Var("1.5"),
            "atr_take_multiplier": _Var("3"),
        }
        window._save_drafts = MagicMock()
        window._refresh_views = MagicMock()
        window._refresh_editor_from_selection = MagicMock()
        logged: list[str] = []
        window._append_log = logged.append

        SignalMonitorWindow.save_selected_draft(window)

        self.assertEqual(draft.template_payload["symbol"], "ETH-USDT-SWAP")
        self.assertEqual(draft.template_payload["direction_label"], "只做多")
        snapshot = draft.template_payload["config_snapshot"]
        self.assertEqual(snapshot["inst_id"], "ETH-USDT-SWAP")
        self.assertEqual(snapshot["bar"], "4H")
        self.assertEqual(snapshot["signal_mode"], "long_only")
        self.assertEqual(snapshot["ema_period"], "34")
        self.assertEqual(snapshot["trend_ema_period"], "89")
        self.assertEqual(snapshot["atr_period"], "14")
        window._save_drafts.assert_called_once()
        window._refresh_views.assert_called_once()
        window._refresh_editor_from_selection.assert_called_once()
        self.assertEqual(logged, ["[D001] 已更新观察模板参数。"])

    def test_save_current_as_preset_adds_named_preset(self) -> None:
        draft = _ObserverDraft(
            draft_id="D002",
            template_payload={
                "strategy_id": STRATEGY_CROSS_ID,
                "strategy_name": get_strategy_definition(STRATEGY_CROSS_ID).name,
                "direction_label": "双向",
                "run_mode_label": "只发邮件",
                "symbol": "BTC-USDT-SWAP",
                "config_snapshot": {
                    "strategy_id": STRATEGY_CROSS_ID,
                    "inst_id": "BTC-USDT-SWAP",
                    "bar": "4H",
                    "signal_mode": "both",
                },
            },
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )
        window = object.__new__(SignalMonitorWindow)
        window.window = object()
        window._selected_single_draft = lambda: draft
        window._preset_name = _Var("4H 趋势观察")
        window._preset_choice = _Var("")
        window._presets = []
        window._find_preset = lambda name: SignalMonitorWindow._find_preset(window, name)
        window._save_presets = MagicMock()
        window._refresh_preset_choices = MagicMock()
        logged: list[str] = []
        window._append_log = logged.append

        SignalMonitorWindow.save_current_as_preset(window)

        self.assertEqual(len(window._presets), 1)
        self.assertEqual(window._presets[0].preset_name, "4H 趋势观察")
        self.assertEqual(window._preset_choice.get(), "4H 趋势观察")
        window._save_presets.assert_called_once()
        window._refresh_preset_choices.assert_called_once()
        self.assertEqual(logged, ["[预设] 已保存观察预设：4H 趋势观察"])

    def test_apply_selected_preset_keeps_current_symbol_and_updates_params(self) -> None:
        draft = _ObserverDraft(
            draft_id="D003",
            template_payload={
                "strategy_id": STRATEGY_CROSS_ID,
                "strategy_name": get_strategy_definition(STRATEGY_CROSS_ID).name,
                "direction_label": "双向",
                "run_mode_label": "只发邮件",
                "symbol": "ETH-USDT-SWAP",
                "config_snapshot": {
                    "strategy_id": STRATEGY_CROSS_ID,
                    "inst_id": "ETH-USDT-SWAP",
                    "bar": "1H",
                    "signal_mode": "both",
                    "ema_period": "21",
                    "trend_ema_period": "55",
                    "big_ema_period": "233",
                },
            },
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )
        preset = _ObserverPreset(
            preset_name="4H 趋势观察",
            template_payload={
                "strategy_id": STRATEGY_CROSS_ID,
                "strategy_name": get_strategy_definition(STRATEGY_CROSS_ID).name,
                "direction_label": "只做空",
                "run_mode_label": "只发邮件",
                "symbol": "BTC-USDT-SWAP",
                "config_snapshot": {
                    "strategy_id": STRATEGY_CROSS_ID,
                    "inst_id": "BTC-USDT-SWAP",
                    "bar": "4H",
                    "signal_mode": "short_only",
                    "ema_period": "34",
                    "trend_ema_period": "89",
                    "big_ema_period": "233",
                },
            },
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )
        window = object.__new__(SignalMonitorWindow)
        window.window = object()
        window.draft_tree = None
        window._selected_single_draft = lambda: draft
        window._selected_preset = lambda: preset
        window._save_drafts = MagicMock()
        window._refresh_views = MagicMock()
        window._refresh_editor_from_selection = MagicMock()
        logged: list[str] = []
        window._append_log = logged.append

        SignalMonitorWindow.apply_selected_preset(window)

        snapshot = draft.template_payload["config_snapshot"]
        self.assertEqual(draft.template_payload["symbol"], "ETH-USDT-SWAP")
        self.assertEqual(snapshot["inst_id"], "ETH-USDT-SWAP")
        self.assertEqual(snapshot["bar"], "4H")
        self.assertEqual(snapshot["signal_mode"], "short_only")
        self.assertEqual(snapshot["ema_period"], "34")
        self.assertEqual(draft.template_payload["direction_label"], "只做空")
        window._save_drafts.assert_called_once()
        window._refresh_views.assert_called_once()
        window._refresh_editor_from_selection.assert_called_once()
        self.assertEqual(logged, ["[D003] 已套用观察预设：4H 趋势观察"])


class SignalMonitorWindowDeleteSessionsTest(TestCase):
    def test_delete_selected_sessions_calls_deleter_and_logs_success(self) -> None:
        window = object.__new__(SignalMonitorWindow)
        window.window = object()
        window.session_tree = SimpleNamespace(selection=lambda: ("S01", "S02"))
        window._session_deleter = MagicMock(return_value=(2, []))
        window._refresh_views = MagicMock()
        logged: list[str] = []
        window._append_log = logged.append

        with patch("okx_quant.signal_monitor_ui.messagebox.askyesno", return_value=True) as askyesno, patch(
            "okx_quant.signal_monitor_ui.messagebox.showinfo"
        ) as showinfo, patch("okx_quant.signal_monitor_ui.messagebox.showerror") as showerror:
            SignalMonitorWindow.delete_selected_sessions(window)

        askyesno.assert_called_once()
        window._session_deleter.assert_called_once_with(["S01", "S02"])
        window._refresh_views.assert_called_once()
        self.assertEqual(logged, ["已删除 2 个 signal_only 会话记录。"])
        showinfo.assert_not_called()
        showerror.assert_not_called()

    def test_delete_selected_sessions_warns_when_selected_rows_cannot_be_removed(self) -> None:
        window = object.__new__(SignalMonitorWindow)
        window.window = object()
        window.session_tree = SimpleNamespace(selection=lambda: ("S01",))
        window._session_deleter = MagicMock(return_value=(0, ["S01"]))
        window._refresh_views = MagicMock()
        window._append_log = MagicMock()

        with patch("okx_quant.signal_monitor_ui.messagebox.askyesno", return_value=True), patch(
            "okx_quant.signal_monitor_ui.messagebox.showinfo"
        ) as showinfo:
            SignalMonitorWindow.delete_selected_sessions(window)

        showinfo.assert_called_once()
        self.assertIn("当前还不能删除", showinfo.call_args.args[1])
        window._append_log.assert_not_called()
        window._refresh_views.assert_called_once()


class SignalObserverSessionDeleteTest(TestCase):
    def test_delete_signal_observer_sessions_removes_only_stopped_signal_only_sessions(self) -> None:
        tree = _SessionTreeStub()
        tree.insert("", "end", iid="S01", values=("S01",))
        tree.insert("", "end", iid="S02", values=("S02",))
        tree.selection_set("S01")
        stopped_session = SimpleNamespace(
            session_id="S01",
            status="已停止",
            engine=SimpleNamespace(is_running=False),
            config=SimpleNamespace(run_mode="signal_only"),
        )
        running_session = SimpleNamespace(
            session_id="S02",
            status="运行中",
            engine=SimpleNamespace(is_running=True),
            config=SimpleNamespace(run_mode="signal_only"),
        )
        app = SimpleNamespace(
            sessions={"S01": stopped_session, "S02": running_session},
            session_tree=tree,
            _remove_recoverable_strategy_session=MagicMock(),
            _refresh_selected_session_details=MagicMock(),
            _refresh_running_session_summary=MagicMock(),
        )

        deleted_count, blocked_ids = QuantApp._delete_signal_observer_sessions_by_id(app, ["S01", "S02"])

        self.assertEqual(deleted_count, 1)
        self.assertEqual(blocked_ids, ["S02"])
        self.assertNotIn("S01", app.sessions)
        self.assertFalse(tree.exists("S01"))
        self.assertEqual(tree.selection(), ("S02",))
        self.assertEqual(tree.focused, "S02")
        self.assertEqual(tree.seen, "S02")
        app._remove_recoverable_strategy_session.assert_called_once_with("S01")
        app._refresh_selected_session_details.assert_called_once()
        app._refresh_running_session_summary.assert_called_once()
