from __future__ import annotations

import argparse
import ctypes
import sys
import threading
import tkinter as tk
from collections.abc import Callable
from pathlib import Path
from tkinter import messagebox, ttk

from okx_quant.app_paths import configure_data_root, data_root
from okx_quant.email_schedule_manager import (
    EmailArchiveRecord,
    EmailScheduleSnapshotBundle,
    EmailScheduledTaskEvent,
    EmailScheduledTaskSnapshot,
    collect_email_schedule_snapshot,
    format_event_id,
    format_task_result,
    normalize_event_task_name,
    open_path,
    start_email_schedule_task,
    summarize_event_message,
    task_slot_label,
)
from okx_quant.persistence import analysis_report_dir_path


class EmailScheduleManagerWindow:
    def __init__(
        self,
        parent: tk.Misc | None = None,
        *,
        on_close: Callable[[], None] | None = None,
    ) -> None:
        self._is_standalone = parent is None
        self._on_close_callback = on_close
        self.window = tk.Tk() if parent is None else tk.Toplevel(parent)
        self.root = self.window
        self.window.title("邮件任务管理器")
        self.window.geometry("1260x860")
        self.window.minsize(1100, 720)
        if parent is not None:
            try:
                self.window.transient(parent)
            except Exception:
                pass
        self.window.protocol("WM_DELETE_WINDOW", self.destroy)

        self._style = ttk.Style(self.window)
        try:
            self._style.theme_use("clam")
        except Exception:
            pass

        self.data_root_text = tk.StringVar(value=f"数据目录：{data_root()}")
        self.status_text = tk.StringVar(value="准备刷新")
        self.task_detail_text = tk.StringVar(value="选中一条任务后，这里会显示完整执行命令和运行设置。")
        self.archive_detail_text = tk.StringVar(value="选中一条归档后，这里会显示主题、文件路径和投递信息。")

        self._task_rows: dict[str, EmailScheduledTaskSnapshot] = {}
        self._event_rows: dict[str, EmailScheduledTaskEvent] = {}
        self._archive_rows: dict[str, EmailArchiveRecord] = {}
        self._refresh_inflight = False

        self._build_layout()
        self.window.after(120, self.refresh_now)
        self.window.after(30_000, self._auto_refresh)

    def run(self) -> None:
        if self._is_standalone:
            self.window.mainloop()

    def show(self) -> None:
        try:
            self.window.deiconify()
            self.window.lift()
            self.window.focus_force()
        except Exception:
            pass
        self.refresh_now()

    def destroy(self) -> None:
        if not self._widget_exists(self.window):
            return
        self.window.destroy()
        if self._on_close_callback is not None:
            self._on_close_callback()

    def _build_layout(self) -> None:
        container = ttk.Frame(self.window, padding=12)
        container.pack(fill="both", expand=True)

        header = ttk.Frame(container)
        header.pack(fill="x")
        ttk.Label(header, text="邮件任务管理器", font=("Microsoft YaHei UI", 16, "bold")).pack(side="left")

        actions = ttk.Frame(header)
        actions.pack(side="right")
        ttk.Button(actions, text="刷新", command=self.refresh_now).pack(side="left", padx=(0, 8))
        ttk.Button(actions, text="立即运行选中任务", command=self.run_selected_task).pack(side="left", padx=(0, 8))
        ttk.Button(actions, text="打开归档目录", command=self.open_archive_dir).pack(side="left", padx=(0, 8))
        ttk.Button(actions, text="打开分析目录", command=self.open_analysis_dir).pack(side="left")

        ttk.Label(container, textvariable=self.data_root_text, foreground="#555").pack(anchor="w", pady=(8, 2))
        ttk.Label(container, textvariable=self.status_text, foreground="#555").pack(anchor="w", pady=(0, 10))

        task_box = ttk.LabelFrame(container, text="任务状态", padding=10)
        task_box.pack(fill="x")

        task_wrap = ttk.Frame(task_box)
        task_wrap.pack(fill="x", expand=True)
        task_columns = ("slot", "state", "next_run", "last_run", "result", "missed", "logon", "battery", "catchup")
        self.task_tree = ttk.Treeview(task_wrap, columns=task_columns, show="headings", height=8)
        task_headings = {
            "slot": ("时段", 70),
            "state": ("状态", 90),
            "next_run": ("下次运行", 160),
            "last_run": ("上次运行", 160),
            "result": ("上次结果", 110),
            "missed": ("漏跑", 60),
            "logon": ("登录模式", 110),
            "battery": ("电池策略", 110),
            "catchup": ("补跑", 70),
        }
        for column, (title, width) in task_headings.items():
            self.task_tree.heading(column, text=title)
            self.task_tree.column(column, width=width, anchor="center")
        self.task_tree.column("next_run", anchor="w")
        self.task_tree.column("last_run", anchor="w")
        self.task_tree.pack(side="left", fill="x", expand=True)
        task_scroll = ttk.Scrollbar(task_wrap, orient="vertical", command=self.task_tree.yview)
        task_scroll.pack(side="right", fill="y")
        self.task_tree.configure(yscrollcommand=task_scroll.set)
        self.task_tree.bind("<<TreeviewSelect>>", self._on_task_select)

        ttk.Label(task_box, textvariable=self.task_detail_text, wraplength=1200, justify="left", foreground="#555").pack(
            anchor="w",
            pady=(8, 0),
        )

        notebook = ttk.Notebook(container)
        notebook.pack(fill="both", expand=True, pady=(12, 0))

        history_tab = ttk.Frame(notebook, padding=10)
        archive_tab = ttk.Frame(notebook, padding=10)
        notebook.add(history_tab, text="调度历史")
        notebook.add(archive_tab, text="邮件归档")

        self._build_history_tab(history_tab)
        self._build_archive_tab(archive_tab)

    def _build_history_tab(self, parent: ttk.Frame) -> None:
        pane = ttk.Panedwindow(parent, orient="vertical")
        pane.pack(fill="both", expand=True)

        top = ttk.Frame(pane)
        bottom = ttk.Frame(pane)
        pane.add(top, weight=3)
        pane.add(bottom, weight=2)

        history_columns = ("time", "task", "event", "level", "summary")
        self.history_tree = ttk.Treeview(top, columns=history_columns, show="headings", height=18)
        headings = {
            "time": ("时间", 160),
            "task": ("任务", 210),
            "event": ("事件", 100),
            "level": ("级别", 90),
            "summary": ("摘要", 640),
        }
        for column, (title, width) in headings.items():
            self.history_tree.heading(column, text=title)
            self.history_tree.column(column, width=width, anchor="w")
        self.history_tree.column("event", anchor="center")
        self.history_tree.column("level", anchor="center")
        self.history_tree.pack(side="left", fill="both", expand=True)
        scroll = ttk.Scrollbar(top, orient="vertical", command=self.history_tree.yview)
        scroll.pack(side="right", fill="y")
        self.history_tree.configure(yscrollcommand=scroll.set)
        self.history_tree.bind("<<TreeviewSelect>>", self._on_event_select)

        ttk.Label(bottom, text="完整事件").pack(anchor="w", pady=(0, 6))
        self.history_detail = tk.Text(bottom, height=12, wrap="word")
        self.history_detail.pack(fill="both", expand=True)
        self.history_detail.configure(state="disabled")

    def _build_archive_tab(self, parent: ttk.Frame) -> None:
        toolbar = ttk.Frame(parent)
        toolbar.pack(fill="x", pady=(0, 8))
        ttk.Button(toolbar, text="打开 HTML", command=self.open_selected_archive_html).pack(side="left", padx=(0, 8))
        ttk.Button(toolbar, text="打开 JSON", command=self.open_selected_archive_json).pack(side="left", padx=(0, 8))
        ttk.Button(toolbar, text="打开报告", command=self.open_selected_archive_report).pack(side="left")

        pane = ttk.Panedwindow(parent, orient="vertical")
        pane.pack(fill="both", expand=True)

        top = ttk.Frame(pane)
        bottom = ttk.Frame(pane)
        pane.add(top, weight=3)
        pane.add(bottom, weight=2)

        archive_columns = ("archived_at", "status", "release_slot", "analysis_slot", "generated_at", "subject")
        self.archive_tree = ttk.Treeview(top, columns=archive_columns, show="headings", height=18)
        headings = {
            "archived_at": ("归档时间", 170),
            "status": ("投递状态", 120),
            "release_slot": ("释放时段", 100),
            "analysis_slot": ("分析时段", 100),
            "generated_at": ("分析生成", 170),
            "subject": ("主题", 560),
        }
        for column, (title, width) in headings.items():
            self.archive_tree.heading(column, text=title)
            self.archive_tree.column(column, width=width, anchor="w")
        self.archive_tree.pack(side="left", fill="both", expand=True)
        scroll = ttk.Scrollbar(top, orient="vertical", command=self.archive_tree.yview)
        scroll.pack(side="right", fill="y")
        self.archive_tree.configure(yscrollcommand=scroll.set)
        self.archive_tree.bind("<<TreeviewSelect>>", self._on_archive_select)

        ttk.Label(bottom, textvariable=self.archive_detail_text, wraplength=1180, justify="left").pack(anchor="w")

    def refresh_now(self) -> None:
        if not self._widget_exists(self.window):
            return
        if self._refresh_inflight:
            return
        self._refresh_inflight = True
        self.status_text.set("正在刷新任务状态、调度历史和归档记录…")
        threading.Thread(target=self._refresh_worker, daemon=True).start()

    def _refresh_worker(self) -> None:
        try:
            bundle = collect_email_schedule_snapshot()
        except Exception as exc:
            error_text = str(exc)
            self._safe_after(lambda error_text=error_text: self._finish_refresh_error(error_text))
            return
        self._safe_after(lambda bundle=bundle: self._finish_refresh_success(bundle))

    def _finish_refresh_success(self, bundle: EmailScheduleSnapshotBundle) -> None:
        if not self._widget_exists(self.window):
            self._refresh_inflight = False
            return
        self._refresh_inflight = False
        self._render_tasks(bundle.tasks)
        self._render_events(bundle.events)
        self._render_archives(bundle.archives)
        self.status_text.set(
            f"已刷新：{len(bundle.tasks)} 条任务，{len(bundle.events)} 条历史事件，{len(bundle.archives)} 条邮件归档"
        )

    def _finish_refresh_error(self, error_text: str) -> None:
        if not self._widget_exists(self.window):
            self._refresh_inflight = False
            return
        self._refresh_inflight = False
        self.status_text.set(f"刷新失败：{error_text}")

    def _render_tasks(self, tasks: list[EmailScheduledTaskSnapshot]) -> None:
        if not self._widget_exists(self.task_tree):
            return
        selected = self._selected_item_key(self.task_tree)
        self.task_tree.delete(*self.task_tree.get_children())
        self._task_rows.clear()
        for index, task in enumerate(tasks):
            iid = f"task-{index}"
            self._task_rows[iid] = task
            battery_text = "允许" if not task.disallow_start_if_on_batteries else "受限"
            catchup_text = "开" if task.start_when_available else "关"
            self.task_tree.insert(
                "",
                "end",
                iid=iid,
                values=(
                    task_slot_label(task.task_name),
                    task.state or "-",
                    task.next_run_time or "-",
                    task.last_run_time or "-",
                    format_task_result(task.last_result),
                    str(task.missed_runs),
                    task.logon_type or "-",
                    battery_text,
                    catchup_text,
                ),
            )
        self._restore_selection(self.task_tree, self._task_rows, selected, self._on_task_select)

    def _render_events(self, events: list[EmailScheduledTaskEvent]) -> None:
        if not self._widget_exists(self.history_tree):
            return
        selected = self._selected_item_key(self.history_tree)
        self.history_tree.delete(*self.history_tree.get_children())
        self._event_rows.clear()
        for index, event in enumerate(events):
            iid = f"event-{index}"
            self._event_rows[iid] = event
            self.history_tree.insert(
                "",
                "end",
                iid=iid,
                values=(
                    event.time_created or "-",
                    normalize_event_task_name(event.task_name, event.message) or "-",
                    format_event_id(event.event_id),
                    event.level or "-",
                    summarize_event_message(event.message),
                ),
            )
        self._restore_selection(self.history_tree, self._event_rows, selected, self._on_event_select)

    def _render_archives(self, archives: list[EmailArchiveRecord]) -> None:
        if not self._widget_exists(self.archive_tree):
            return
        selected = self._selected_item_key(self.archive_tree)
        self.archive_tree.delete(*self.archive_tree.get_children())
        self._archive_rows.clear()
        for index, record in enumerate(archives):
            iid = f"archive-{index}"
            self._archive_rows[iid] = record
            self.archive_tree.insert(
                "",
                "end",
                iid=iid,
                values=(
                    record.archived_at or "-",
                    record.delivery_status or "-",
                    record.scheduled_release_slot or "-",
                    record.analysis_slot or "-",
                    record.generated_at or "-",
                    record.subject or "-",
                ),
            )
        self._restore_selection(self.archive_tree, self._archive_rows, selected, self._on_archive_select)

    def _restore_selection(self, tree: ttk.Treeview, rows: dict[str, object], selected_key: str, callback: Callable[[], None]) -> None:
        if not self._widget_exists(tree):
            return
        if not rows:
            callback()
            return
        if selected_key:
            for iid, row in rows.items():
                key = getattr(row, "task_name", None) or getattr(row, "meta_path", None) or getattr(row, "time_created", None)
                if str(key) == selected_key:
                    tree.selection_set(iid)
                    tree.focus(iid)
                    callback()
                    return
        first = next(iter(rows))
        tree.selection_set(first)
        tree.focus(first)
        callback()

    def _selected_item_key(self, tree: ttk.Treeview) -> str:
        if not self._widget_exists(tree):
            return ""
        selected = tree.selection()
        if not selected:
            return ""
        iid = selected[0]
        row = self._task_rows.get(iid) or self._event_rows.get(iid) or self._archive_rows.get(iid)
        if row is None:
            return ""
        return str(getattr(row, "task_name", None) or getattr(row, "meta_path", None) or getattr(row, "time_created", None) or "")

    def _on_task_select(self, *_args: object) -> None:
        if not self._widget_exists(self.task_tree):
            return
        selected = self.task_tree.selection()
        if not selected:
            self.task_detail_text.set("选中一条任务后，这里会显示完整执行命令和运行设置。")
            return
        task = self._task_rows.get(selected[0])
        if task is None:
            return
        battery_text = "允许电池启动" if not task.disallow_start_if_on_batteries else "电池模式不启动"
        stop_text = "切电池不中断" if not task.stop_if_going_on_batteries else "切电池会中断"
        catchup_text = "错过后会补跑" if task.start_when_available else "错过后不补跑"
        self.task_detail_text.set(
            f"任务：{task.task_name} | 命令：{task.command_line or '-'} | "
            f"登录模式：{task.logon_type or '-'} | {battery_text} | {stop_text} | {catchup_text}"
        )

    def _on_event_select(self, *_args: object) -> None:
        if not self._widget_exists(self.history_tree):
            return
        selected = self.history_tree.selection()
        if not selected:
            self._set_text(self.history_detail, "")
            return
        event = self._event_rows.get(selected[0])
        if event is None:
            return
        header = (
            f"时间：{event.time_created or '-'}\n"
            f"任务：{normalize_event_task_name(event.task_name, event.message) or '-'}\n"
            f"事件：{format_event_id(event.event_id)}\n"
            f"级别：{event.level or '-'}\n"
            f"来源：{event.provider or '-'}\n\n"
        )
        self._set_text(self.history_detail, header + (event.message or ""))

    def _on_archive_select(self, *_args: object) -> None:
        if not self._widget_exists(self.archive_tree):
            return
        selected = self.archive_tree.selection()
        if not selected:
            self.archive_detail_text.set("选中一条归档后，这里会显示主题、文件路径和投递信息。")
            return
        record = self._archive_rows.get(selected[0])
        if record is None:
            return
        symbols = ", ".join(record.symbols) if record.symbols else "-"
        self.archive_detail_text.set(
            f"主题：{record.subject or '-'} | 状态：{record.delivery_status or '-'} | "
            f"分析时段：{record.analysis_slot or '-'} | 释放时段：{record.scheduled_release_slot or '-'} | "
            f"币种：{symbols} | HTML：{record.archive_html_path or '-'} | JSON：{record.meta_path} | 报告：{record.report_path or '-'}"
        )

    def run_selected_task(self) -> None:
        if not self._widget_exists(self.window):
            return
        selected = self.task_tree.selection()
        if not selected:
            messagebox.showinfo("提示", "请先选中一条任务。", parent=self.window)
            return
        task = self._task_rows.get(selected[0])
        if task is None:
            return
        self.status_text.set(f"正在触发任务：{task.task_name}")

        def worker() -> None:
            try:
                start_email_schedule_task(task.task_name)
            except Exception as exc:
                error_text = str(exc)
                self._safe_after(lambda error_text=error_text: messagebox.showerror("运行失败", error_text, parent=self.window))
                self._safe_after(lambda: self.status_text.set(f"触发失败：{task.task_name}"))
                return
            self._safe_after(lambda: self.status_text.set(f"已触发任务：{task.task_name}"))
            self._safe_after(self.refresh_now, delay_ms=2500)

        threading.Thread(target=worker, daemon=True).start()

    def open_archive_dir(self) -> None:
        self._open_path(analysis_report_dir_path() / "email_archives")

    def open_analysis_dir(self) -> None:
        self._open_path(analysis_report_dir_path())

    def open_selected_archive_html(self) -> None:
        record = self._selected_archive()
        if record is None or record.archive_html_path is None:
            messagebox.showinfo("提示", "当前归档没有 HTML 文件。", parent=self.window)
            return
        self._open_path(record.archive_html_path)

    def open_selected_archive_json(self) -> None:
        record = self._selected_archive()
        if record is None:
            messagebox.showinfo("提示", "请先选中一条归档。", parent=self.window)
            return
        self._open_path(record.meta_path)

    def open_selected_archive_report(self) -> None:
        record = self._selected_archive()
        if record is None or record.report_path is None:
            messagebox.showinfo("提示", "当前归档没有关联报告。", parent=self.window)
            return
        self._open_path(record.report_path)

    def _selected_archive(self) -> EmailArchiveRecord | None:
        selected = self.archive_tree.selection()
        if not selected:
            return None
        return self._archive_rows.get(selected[0])

    def _open_path(self, path: Path) -> None:
        try:
            open_path(path)
        except Exception as exc:
            messagebox.showerror("打开失败", str(exc), parent=self.window)

    def _set_text(self, widget: tk.Text, value: str) -> None:
        if not self._widget_exists(widget):
            return
        widget.configure(state="normal")
        widget.delete("1.0", "end")
        widget.insert("1.0", value)
        widget.configure(state="disabled")

    def _auto_refresh(self) -> None:
        if not self._widget_exists(self.window):
            return
        self.refresh_now()
        self.window.after(30_000, self._auto_refresh)

    def _safe_after(self, callback: Callable[[], None], *, delay_ms: int = 0) -> None:
        if not self._widget_exists(self.window):
            return
        try:
            self.window.after(delay_ms, callback)
        except Exception:
            return

    @staticmethod
    def _widget_exists(widget: object) -> bool:
        if widget is None:
            return False
        winfo_exists = getattr(widget, "winfo_exists", None)
        if not callable(winfo_exists):
            return False
        try:
            return bool(winfo_exists())
        except Exception:
            return False


def _enable_windows_dpi_awareness() -> None:
    if sys.platform != "win32":
        return
    try:
        user32 = ctypes.windll.user32
    except Exception:
        return
    for setter in (
        lambda: user32.SetProcessDpiAwarenessContext(ctypes.c_void_p(-4)),
        lambda: ctypes.windll.shcore.SetProcessDpiAwareness(2),
        lambda: user32.SetProcessDPIAware(),
    ):
        try:
            setter()
            return
        except Exception:
            continue


def main(argv: list[str] | None = None) -> None:
    _enable_windows_dpi_awareness()
    parser = argparse.ArgumentParser(description="Open QQOKX email schedule manager")
    parser.add_argument("--data-dir", help="Path to the shared QQOKX data directory")
    args = parser.parse_args(argv)
    if args.data_dir:
        configure_data_root(args.data_dir)
    else:
        data_root()
    EmailScheduleManagerWindow().run()


if __name__ == "__main__":
    main()
