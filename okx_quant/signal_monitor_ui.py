from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from tkinter import BooleanVar, END, StringVar, Text, Toplevel
from tkinter import messagebox, ttk
from typing import Callable

from okx_quant.notifications import EmailNotifier
from okx_quant.pricing import format_decimal_by_increment
from okx_quant.signal_monitor import (
    DEFAULT_MONITOR_SYMBOLS,
    MonitorRoundDiagnostic,
    MonitorSignalEvent,
    SignalMonitor,
    SignalMonitorConfig,
)


NotifierFactory = Callable[[], EmailNotifier | None]
Logger = Callable[[str], None]


@dataclass(frozen=True)
class SignalMonitorDefaults:
    bar: str = "4H"
    poll_seconds: str = "10"
    pattern_ema_period: str = "55"
    ema_near_tolerance: str = "0.001"
    body_ratio_threshold: str = "0.5"
    wick_ratio_threshold: str = "0.6"


@dataclass
class _SignalMonitorTask:
    session_id: str
    config: SignalMonitorConfig
    monitor: SignalMonitor
    started_at: datetime
    status: str = "运行中"


class SignalMonitorWindow:
    def __init__(
        self,
        parent,
        client,
        notifier_factory: NotifierFactory,
        logger: Logger,
    ) -> None:
        self.client = client
        self._notifier_factory = notifier_factory
        self._logger = logger
        self._defaults = SignalMonitorDefaults()
        self._tasks: dict[str, _SignalMonitorTask] = {}
        self._task_counter = 0
        self._selected_task_id: str | None = None
        self._refresh_job: str | None = None
        self._destroying = False
        self._diagnostic_lines: deque[str] = deque(maxlen=120)

        self.window = Toplevel(parent)
        self.window.title("信号监控")
        self.window.geometry("1160x1040")
        self.window.minsize(1000, 900)
        self.window.protocol("WM_DELETE_WINDOW", self._on_close)

        self.bar = StringVar(value=self._defaults.bar)
        self.poll_seconds = StringVar(value=self._defaults.poll_seconds)
        self.custom_symbols = StringVar()
        self.enable_ema21_55_cross = BooleanVar(value=True)
        self.enable_ema55_slope_turn = BooleanVar(value=True)
        self.enable_ema55_breakout = BooleanVar(value=True)
        self.enable_candle_pattern = BooleanVar(value=True)
        self.pattern_ema_period = StringVar(value=self._defaults.pattern_ema_period)
        self.ema_near_tolerance = StringVar(value=self._defaults.ema_near_tolerance)
        self.body_ratio_threshold = StringVar(value=self._defaults.body_ratio_threshold)
        self.wick_ratio_threshold = StringVar(value=self._defaults.wick_ratio_threshold)
        self.status_text = StringVar(value="运行中任务: 0 | 全部任务: 0")

        self._symbol_vars: dict[str, tuple[str, BooleanVar]] = {
            label: (inst_id, BooleanVar(value=True)) for label, inst_id in DEFAULT_MONITOR_SYMBOLS
        }
        self.tasks_tree: ttk.Treeview | None = None
        self.signal_tree: ttk.Treeview | None = None
        self.log_text: Text | None = None
        self.diagnostic_text: Text | None = None

        self._build_layout()
        self._schedule_refresh()

    def show(self) -> None:
        if not self.window.winfo_exists():
            return
        self.window.deiconify()
        self.window.lift()
        self.window.focus_force()
        self._refresh_task_views()

    def destroy(self) -> None:
        self._destroying = True
        self.stop_all()
        if self._refresh_job is not None:
            try:
                self.window.after_cancel(self._refresh_job)
            except Exception:
                pass
            self._refresh_job = None
        if self.window.winfo_exists():
            self.window.destroy()

    def _build_layout(self) -> None:
        self.window.columnconfigure(0, weight=1)
        self.window.rowconfigure(2, weight=0)
        self.window.rowconfigure(3, weight=1)
        self.window.rowconfigure(4, weight=1)
        self.window.rowconfigure(5, weight=1)

        header = ttk.Frame(self.window, padding=(16, 16, 16, 8))
        header.grid(row=0, column=0, sticky="ew")
        header.columnconfigure(0, weight=1)
        ttk.Label(header, text="多币种信号监控", font=("Microsoft YaHei UI", 18, "bold")).grid(
            row=0, column=0, sticky="w"
        )
        ttk.Label(header, textvariable=self.status_text).grid(row=0, column=1, sticky="e")

        controls = ttk.Frame(self.window, padding=(16, 0, 16, 8))
        controls.grid(row=1, column=0, sticky="nsew")
        controls.columnconfigure(0, weight=1)
        controls.columnconfigure(1, weight=1)

        base_frame = ttk.LabelFrame(controls, text="基本监控配置", padding=14)
        base_frame.grid(row=0, column=0, sticky="nsew", padx=(0, 8))
        base_frame.columnconfigure(0, weight=1)
        base_frame.columnconfigure(1, weight=1)

        ttk.Label(base_frame, text="监控币种").grid(row=0, column=0, sticky="w")
        symbol_box = ttk.LabelFrame(base_frame, text="监控币种", padding=10)
        symbol_box.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(6, 0))
        symbol_box.columnconfigure(0, weight=1)
        for index, (label, (_, var)) in enumerate(self._symbol_vars.items()):
            ttk.Checkbutton(symbol_box, text=label, variable=var).grid(
                row=index // 2, column=index % 2, sticky="w", padx=(0, 24), pady=4
            )

        ttk.Label(base_frame, text="额外币种（逗号分隔）").grid(row=2, column=0, sticky="w", pady=(12, 0))
        ttk.Entry(base_frame, textvariable=self.custom_symbols).grid(
            row=3, column=0, columnspan=2, sticky="ew", pady=(6, 0)
        )

        ttk.Label(base_frame, text="时间周期").grid(row=4, column=0, sticky="w", pady=(12, 0))
        ttk.Combobox(
            base_frame,
            textvariable=self.bar,
            values=["15m", "1H", "4H"],
            state="readonly",
        ).grid(row=5, column=0, sticky="ew", pady=(6, 0), padx=(0, 12))
        ttk.Label(base_frame, text="收线缓冲秒数").grid(row=4, column=1, sticky="w", pady=(12, 0))
        ttk.Entry(base_frame, textvariable=self.poll_seconds).grid(row=5, column=1, sticky="ew", pady=(6, 0))

        signal_frame = ttk.LabelFrame(controls, text="信号配置", padding=14)
        signal_frame.grid(row=0, column=1, sticky="nsew", padx=(8, 0))
        signal_frame.columnconfigure(0, weight=1)

        ttk.Checkbutton(signal_frame, text="EMA21/55金叉死叉", variable=self.enable_ema21_55_cross).grid(
            row=0, column=0, sticky="w", pady=4
        )
        ttk.Checkbutton(signal_frame, text="EMA55斜率改变", variable=self.enable_ema55_slope_turn).grid(
            row=1, column=0, sticky="w", pady=4
        )
        ttk.Checkbutton(signal_frame, text="EMA55突破", variable=self.enable_ema55_breakout).grid(
            row=2, column=0, sticky="w", pady=4
        )
        ttk.Checkbutton(signal_frame, text="K线形态信号", variable=self.enable_candle_pattern).grid(
            row=3, column=0, sticky="w", pady=4
        )

        pattern_box = ttk.LabelFrame(signal_frame, text="K线形态信号参数", padding=10)
        pattern_box.grid(row=4, column=0, sticky="ew", pady=(12, 0))
        pattern_box.columnconfigure(1, weight=1)

        ttk.Label(pattern_box, text="EMA周期").grid(row=0, column=0, sticky="w")
        ttk.Entry(pattern_box, textvariable=self.pattern_ema_period).grid(row=0, column=1, sticky="ew", pady=4)
        ttk.Label(pattern_box, text="EMA附近容差").grid(row=1, column=0, sticky="w")
        ttk.Entry(pattern_box, textvariable=self.ema_near_tolerance).grid(row=1, column=1, sticky="ew", pady=4)
        ttk.Label(pattern_box, text="大K线实体比例阈值").grid(row=2, column=0, sticky="w")
        ttk.Entry(pattern_box, textvariable=self.body_ratio_threshold).grid(row=2, column=1, sticky="ew", pady=4)
        ttk.Label(pattern_box, text="影线比例阈值").grid(row=3, column=0, sticky="w")
        ttk.Entry(pattern_box, textvariable=self.wick_ratio_threshold).grid(row=3, column=1, sticky="ew", pady=4)

        button_row = ttk.Frame(signal_frame)
        button_row.grid(row=5, column=0, sticky="w", pady=(14, 0))
        ttk.Button(button_row, text="启动任务", command=self.start).grid(row=0, column=0, padx=(0, 8))
        ttk.Button(button_row, text="停止选中任务", command=self.stop).grid(row=0, column=1, padx=(0, 8))
        ttk.Button(button_row, text="停止全部任务", command=self.stop_all).grid(row=0, column=2)
        ttk.Label(
            signal_frame,
            text=(
                "这里改成任务模式后，可以分别启动 15m / 1H / 4H 等不同周期。"
                "每个任务会等到对应周期收线后再检测。关闭窗口只会隐藏面板，不会停止已运行任务。"
            ),
            wraplength=430,
            justify="left",
        ).grid(row=6, column=0, sticky="w", pady=(12, 0))

        task_frame = ttk.LabelFrame(self.window, text="监控任务", padding=12)
        task_frame.grid(row=2, column=0, sticky="nsew", padx=16, pady=(0, 8))
        task_frame.columnconfigure(0, weight=1)
        task_frame.rowconfigure(1, weight=1)

        task_header = ttk.Frame(task_frame)
        task_header.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 8))
        task_header.columnconfigure(0, weight=1)
        ttk.Label(
            task_header,
            text="每次点击“启动任务”，都会按当前周期和参数创建一个独立监控任务。",
        ).grid(row=0, column=0, sticky="w")
        ttk.Button(task_header, text="清理已停止", command=self.clear_stopped_tasks).grid(row=0, column=1, sticky="e")

        self.tasks_tree = ttk.Treeview(
            task_frame,
            columns=("task", "bar", "symbols", "status", "started"),
            show="headings",
            selectmode="browse",
            height=6,
        )
        self.tasks_tree.heading("task", text="任务")
        self.tasks_tree.heading("bar", text="周期")
        self.tasks_tree.heading("symbols", text="币种")
        self.tasks_tree.heading("status", text="状态")
        self.tasks_tree.heading("started", text="启动时间")
        self.tasks_tree.column("task", width=90, anchor="center")
        self.tasks_tree.column("bar", width=90, anchor="center")
        self.tasks_tree.column("symbols", width=420, anchor="w")
        self.tasks_tree.column("status", width=110, anchor="center")
        self.tasks_tree.column("started", width=150, anchor="center")
        self.tasks_tree.grid(row=1, column=0, sticky="nsew")
        self.tasks_tree.bind("<<TreeviewSelect>>", self._on_task_selected)

        task_scroll = ttk.Scrollbar(task_frame, orient="vertical", command=self.tasks_tree.yview)
        task_scroll.grid(row=1, column=1, sticky="ns")
        self.tasks_tree.configure(yscrollcommand=task_scroll.set)

        signal_list_frame = ttk.LabelFrame(self.window, text="最近触发信号", padding=12)
        signal_list_frame.grid(row=3, column=0, sticky="nsew", padx=16, pady=(0, 8))
        signal_list_frame.columnconfigure(0, weight=1)
        signal_list_frame.rowconfigure(0, weight=1)

        self.signal_tree = ttk.Treeview(
            signal_list_frame,
            columns=("time", "task", "bar", "symbol", "type", "direction", "price", "reason"),
            show="headings",
            selectmode="browse",
        )
        self.signal_tree.heading("time", text="时间")
        self.signal_tree.heading("task", text="任务")
        self.signal_tree.heading("bar", text="周期")
        self.signal_tree.heading("symbol", text="币种")
        self.signal_tree.heading("type", text="信号")
        self.signal_tree.heading("direction", text="方向")
        self.signal_tree.heading("price", text="参考价")
        self.signal_tree.heading("reason", text="说明")
        self.signal_tree.column("time", width=145, anchor="center")
        self.signal_tree.column("task", width=80, anchor="center")
        self.signal_tree.column("bar", width=80, anchor="center")
        self.signal_tree.column("symbol", width=120, anchor="w")
        self.signal_tree.column("type", width=130, anchor="center")
        self.signal_tree.column("direction", width=70, anchor="center")
        self.signal_tree.column("price", width=100, anchor="e")
        self.signal_tree.column("reason", width=420, anchor="w")
        self.signal_tree.grid(row=0, column=0, sticky="nsew")

        signal_scroll = ttk.Scrollbar(signal_list_frame, orient="vertical", command=self.signal_tree.yview)
        signal_scroll.grid(row=0, column=1, sticky="ns")
        self.signal_tree.configure(yscrollcommand=signal_scroll.set)

        log_frame = ttk.LabelFrame(self.window, text="监控日志", padding=12)
        log_frame.grid(row=4, column=0, sticky="nsew", padx=16, pady=(0, 16))
        log_frame.columnconfigure(0, weight=1)
        log_frame.rowconfigure(0, weight=1)

        self.log_text = Text(log_frame, height=12, wrap="word", font=("Consolas", 10))
        self.log_text.grid(row=0, column=0, sticky="nsew")
        log_scroll = ttk.Scrollbar(log_frame, orient="vertical", command=self.log_text.yview)
        log_scroll.grid(row=0, column=1, sticky="ns")
        self.log_text.configure(yscrollcommand=log_scroll.set)

        diagnostic_frame = ttk.LabelFrame(self.window, text="实时诊断", padding=12)
        diagnostic_frame.grid(row=5, column=0, sticky="nsew", padx=16, pady=(0, 16))
        diagnostic_frame.columnconfigure(0, weight=1)
        diagnostic_frame.rowconfigure(1, weight=1)

        ttk.Label(
            diagnostic_frame,
            text="显示每轮收线检测里命中的信号、因未勾选被过滤的信号，以及同一根K线的重复抑制情况。",
            justify="left",
        ).grid(row=0, column=0, sticky="w", pady=(0, 8))

        self.diagnostic_text = Text(diagnostic_frame, height=10, wrap="word", font=("Consolas", 10))
        self.diagnostic_text.grid(row=1, column=0, sticky="nsew")
        diagnostic_scroll = ttk.Scrollbar(diagnostic_frame, orient="vertical", command=self.diagnostic_text.yview)
        diagnostic_scroll.grid(row=1, column=1, sticky="ns")
        self.diagnostic_text.configure(yscrollcommand=diagnostic_scroll.set)

    def start(self) -> None:
        try:
            config = self._build_config()
            notifier = self._notifier_factory()
        except Exception as exc:
            messagebox.showerror("启动失败", str(exc), parent=self.window)
            return

        duplicate = self._find_running_duplicate(config)
        if duplicate is not None:
            messagebox.showinfo(
                "提示",
                f"已有相同配置的监控任务正在运行：{duplicate.session_id} / {duplicate.config.bar}",
                parent=self.window,
            )
            return

        self._task_counter += 1
        session_id = f"M{self._task_counter:02d}"
        monitor = SignalMonitor(
            self.client,
            logger=lambda message, sid=session_id: self._queue_ui(lambda: self._log(sid, message)),
            event_callback=lambda event, sid=session_id, bar=config.bar: self._queue_ui(
                lambda: self._append_event(sid, bar, event)
            ),
            diagnostic_callback=lambda diagnostic, sid=session_id: self._queue_ui(
                lambda: self._append_diagnostic(sid, diagnostic)
            ),
            email_sender=self._build_email_sender(notifier, session_id),
            monitor_name=f"信号任务 {session_id}",
        )
        task = _SignalMonitorTask(
            session_id=session_id,
            config=config,
            monitor=monitor,
            started_at=datetime.now(),
        )
        self._tasks[session_id] = task
        monitor.start(config)
        self._selected_task_id = session_id
        self._log(session_id, f"任务已启动 | 周期={config.bar} | 币种数={len(config.symbols)}")
        self._refresh_task_views()

    def stop(self) -> None:
        task = self._selected_task()
        if task is None:
            messagebox.showinfo("提示", "请先在任务列表里选中一个监控任务。", parent=self.window)
            return
        if task.monitor.is_running:
            task.status = "停止中"
            task.monitor.stop()
            self._log(task.session_id, "已请求停止任务。")
        else:
            task.status = "已停止"
        self._refresh_task_views()

    def stop_all(self) -> None:
        for task in self._tasks.values():
            if task.monitor.is_running:
                task.status = "停止中"
                task.monitor.stop()
        self._refresh_task_views()

    def clear_stopped_tasks(self) -> None:
        removable_ids = [task_id for task_id, task in self._tasks.items() if not task.monitor.is_running]
        for task_id in removable_ids:
            del self._tasks[task_id]
        if self._selected_task_id in removable_ids:
            self._selected_task_id = None
        self._refresh_task_views()

    def _build_email_sender(
        self,
        notifier: EmailNotifier | None,
        session_id: str,
    ) -> Callable[[MonitorSignalEvent, str], None] | None:
        if notifier is None or not notifier.signal_notifications_enabled:
            return None

        def _sender(event: MonitorSignalEvent, bar: str) -> None:
            subject = f"[QQOKX] 信号监控 | {session_id} | {bar} | {event.symbol} | {event.signal_type}"
            body = "\n".join(
                [
                    "模块：多币种信号监控",
                    f"任务：{session_id}",
                    f"时间周期：{bar}",
                    f"币种：{event.symbol}",
                    f"信号：{event.signal_type}",
                    f"方向：{event.direction}",
                    f"参考价：{format_decimal_by_increment(event.trigger_price, event.tick_size)}",
                    f"说明：{event.reason}",
                ]
            )
            notifier.notify_async(subject, body)

        return _sender

    def _build_config(self) -> SignalMonitorConfig:
        symbols = [inst_id for _, (inst_id, var) in self._symbol_vars.items() if var.get()]
        for item in self.custom_symbols.get().replace(";", ",").split(","):
            cleaned = item.strip().upper()
            if cleaned:
                symbols.append(_normalize_symbol_input(cleaned))
        symbols = list(dict.fromkeys(symbols))
        if not symbols:
            raise ValueError("请至少选择一个监控币种")
        if not any(
            (
                self.enable_ema21_55_cross.get(),
                self.enable_ema55_slope_turn.get(),
                self.enable_ema55_breakout.get(),
                self.enable_candle_pattern.get(),
            )
        ):
            raise ValueError("请至少选择一种信号")

        return SignalMonitorConfig(
            symbols=tuple(symbols),
            bar=self.bar.get(),
            poll_seconds=float(self._parse_positive_decimal(self.poll_seconds.get(), "收线缓冲秒数")),
            enable_ema21_55_cross=self.enable_ema21_55_cross.get(),
            enable_ema55_slope_turn=self.enable_ema55_slope_turn.get(),
            enable_ema55_breakout=self.enable_ema55_breakout.get(),
            enable_candle_pattern=self.enable_candle_pattern.get(),
            pattern_ema_period=self._parse_positive_int(self.pattern_ema_period.get(), "EMA周期"),
            ema_near_tolerance=self._parse_positive_decimal(self.ema_near_tolerance.get(), "EMA附近容差"),
            body_ratio_threshold=self._parse_ratio(self.body_ratio_threshold.get(), "大K线实体比例阈值"),
            wick_ratio_threshold=self._parse_ratio(self.wick_ratio_threshold.get(), "影线比例阈值"),
        )

    def _append_event(self, session_id: str, bar: str, event: MonitorSignalEvent) -> None:
        if self.signal_tree is None:
            return
        values = (
            datetime.fromtimestamp(event.candle_ts / 1000).strftime("%Y-%m-%d %H:%M:%S"),
            session_id,
            bar,
            event.symbol,
            event.signal_type,
            "做多" if event.direction == "long" else "做空",
            format_decimal_by_increment(event.trigger_price, event.tick_size),
            event.reason,
        )
        self.signal_tree.insert("", 0, values=values)
        children = self.signal_tree.get_children()
        for item_id in children[300:]:
            self.signal_tree.delete(item_id)

    def _append_diagnostic(self, session_id: str, diagnostic: MonitorRoundDiagnostic) -> None:
        if self.diagnostic_text is None:
            return
        self._diagnostic_lines.append(_format_monitor_diagnostic_round(session_id, diagnostic))
        self.diagnostic_text.delete("1.0", END)
        self.diagnostic_text.insert("1.0", "\n\n".join(reversed(self._diagnostic_lines)))
        self.diagnostic_text.see("1.0")

    def _log(self, session_id: str, message: str) -> None:
        if self.log_text is None:
            return
        timestamp = datetime.now().strftime("%H:%M:%S")
        line = f"[{timestamp}] [{session_id}] {message}\n"
        self.log_text.insert(END, line)
        self.log_text.see(END)
        self._logger(f"[信号监控 {session_id}] {message}")

    def _parse_positive_int(self, raw: str, field_name: str) -> int:
        value = int(raw)
        if value <= 0:
            raise ValueError(f"{field_name} 必须大于 0")
        return value

    def _parse_positive_decimal(self, raw: str, field_name: str) -> Decimal:
        try:
            value = Decimal(raw)
        except InvalidOperation as exc:
            raise ValueError(f"{field_name} 不是有效数字") from exc
        if value <= 0:
            raise ValueError(f"{field_name} 必须大于 0")
        return value

    def _parse_ratio(self, raw: str, field_name: str) -> Decimal:
        value = self._parse_positive_decimal(raw, field_name)
        if value > 1:
            raise ValueError(f"{field_name} 建议填写 0 到 1 之间的小数")
        return value

    def _on_task_selected(self, *_: object) -> None:
        if self.tasks_tree is None:
            return
        selection = self.tasks_tree.selection()
        self._selected_task_id = selection[0] if selection else None

    def _selected_task(self) -> _SignalMonitorTask | None:
        if self._selected_task_id is None:
            return None
        return self._tasks.get(self._selected_task_id)

    def _find_running_duplicate(self, config: SignalMonitorConfig) -> _SignalMonitorTask | None:
        for task in self._tasks.values():
            if task.monitor.is_running and task.config == config:
                return task
        return None

    def _schedule_refresh(self) -> None:
        if self._destroying or not self.window.winfo_exists():
            return
        self._refresh_task_views()
        self._refresh_job = self.window.after(1000, self._schedule_refresh)

    def _refresh_task_views(self) -> None:
        self._update_task_statuses()
        self._update_status_text()
        self._refresh_task_tree()

    def _update_task_statuses(self) -> None:
        for task in self._tasks.values():
            if task.monitor.is_running:
                if task.status not in {"运行中", "停止中"}:
                    task.status = "运行中"
                continue
            if task.status == "停止中":
                task.status = "已停止"
            elif task.status == "运行中":
                task.status = "已停止"

    def _update_status_text(self) -> None:
        running_count = sum(1 for task in self._tasks.values() if task.monitor.is_running)
        self.status_text.set(f"运行中任务: {running_count} | 全部任务: {len(self._tasks)}")

    def _refresh_task_tree(self) -> None:
        if self.tasks_tree is None:
            return
        selected_before = self.tasks_tree.selection()
        self.tasks_tree.delete(*self.tasks_tree.get_children())
        for task in sorted(self._tasks.values(), key=lambda item: item.started_at, reverse=True):
            values = (
                task.session_id,
                task.config.bar,
                _summarize_symbols(task.config.symbols),
                task.status,
                task.started_at.strftime("%Y-%m-%d %H:%M:%S"),
            )
            self.tasks_tree.insert("", END, iid=task.session_id, values=values)
        target = None
        if selected_before and self.tasks_tree.exists(selected_before[0]):
            target = selected_before[0]
        elif self._selected_task_id and self.tasks_tree.exists(self._selected_task_id):
            target = self._selected_task_id
        if target is not None:
            self.tasks_tree.selection_set(target)
            self.tasks_tree.focus(target)
            self._selected_task_id = target
        elif self.tasks_tree.get_children():
            self._selected_task_id = self.tasks_tree.get_children()[0]
        else:
            self._selected_task_id = None

    def _queue_ui(self, callback: Callable[[], None]) -> None:
        if self._destroying:
            return
        try:
            if self.window.winfo_exists():
                self.window.after(0, callback)
        except Exception:
            pass

    def _on_close(self) -> None:
        self.window.withdraw()
        self._logger("[信号监控] 窗口已隐藏，监控任务继续运行。")


def _normalize_symbol_input(raw: str) -> str:
    cleaned = raw.strip().upper()
    if "-" in cleaned:
        return cleaned
    if cleaned.endswith("USDT") and len(cleaned) > 4:
        return f"{cleaned[:-4]}-USDT-SWAP"
    return cleaned


def _summarize_symbols(symbols: tuple[str, ...]) -> str:
    if len(symbols) <= 3:
        return ", ".join(symbols)
    preview = ", ".join(symbols[:3])
    return f"{preview} 等 {len(symbols)} 个"


def _format_monitor_diagnostic_round(session_id: str, diagnostic: MonitorRoundDiagnostic) -> str:
    checked_at = datetime.fromtimestamp(diagnostic.checked_at / 1000).strftime("%Y-%m-%d %H:%M:%S")
    lines = [f"[{checked_at}] [{session_id}] {diagnostic.bar} 收线诊断"]

    for report in diagnostic.reports:
        if report.error:
            lines.append(f"  {report.symbol} | 失败: {report.error}")
            continue

        parts: list[str] = []
        if report.new_events:
            parts.append(f"新触发: {_format_signal_group(report.new_events)}")
        if report.filtered_events:
            parts.append(f"已过滤: {_format_signal_group(report.filtered_events)}")
        if report.duplicate_events:
            parts.append(f"重复抑制: {_format_signal_group(report.duplicate_events)}")
        if report.note and not parts:
            parts.append(report.note)
        if not parts:
            continue
        lines.append(f"  {report.symbol} | " + " | ".join(parts))

    if len(lines) == 1:
        lines.append("  本轮无信号。")
    return "\n".join(lines)


def _format_signal_group(events: tuple[MonitorSignalEvent, ...]) -> str:
    formatted = []
    for event in events:
        direction = "做多" if event.direction == "long" else "做空"
        formatted.append(f"{event.signal_type}/{direction}")
    return "，".join(formatted)
