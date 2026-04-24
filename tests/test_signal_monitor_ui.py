from __future__ import annotations

from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import MagicMock, patch

from okx_quant.signal_monitor_ui import SignalMonitorWindow
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

    def insert(self, _parent: str, _index: object, *, iid: str, values: tuple[object, ...]) -> None:
        self.rows[iid] = {"values": values}

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
