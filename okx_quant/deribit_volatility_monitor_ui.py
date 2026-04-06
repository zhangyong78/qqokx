from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from tkinter import BooleanVar, END, StringVar, Text, Toplevel
from tkinter import messagebox, ttk
from typing import Callable

from okx_quant.deribit_client import DeribitRestClient
from okx_quant.deribit_volatility_monitor import (
    DERIBIT_VOL_RESOLUTION_SECONDS,
    DeribitVolatilityMonitor,
    VolatilityMonitorConfig,
    VolatilityMonitorRoundDiagnostic,
    VolatilitySignalEvent,
    VOL_DIRECTION_LABELS,
    VOL_SIGNAL_LABELS,
    format_volatility_diagnostic_round,
)
from okx_quant.notifications import EmailNotifier
from okx_quant.pricing import format_decimal_fixed
from okx_quant.window_layout import apply_adaptive_window_geometry


NotifierFactory = Callable[[], EmailNotifier | None]
Logger = Callable[[str], None]

DERIBIT_VOL_MONITOR_RESOLUTION_OPTIONS = {
    "1小时": "3600",
    "12小时": "43200",
    "1天": "1D",
}


@dataclass(frozen=True)
class VolatilityMonitorDefaults:
    resolution_label: str = "1小时"
    buffer_seconds: str = "10"
    ema_period: str = "34"
    trend_streak_bars: str = "4"
    squeeze_bars: str = "6"
    lookback_candles: str = "180"
    cumulative_change_threshold: str = "0.06"
    reversal_body_multiplier: str = "1.8"
    breakout_body_multiplier: str = "2.0"
    squeeze_range_ratio: str = "0.65"


@dataclass
class _VolatilityMonitorTask:
    session_id: str
    config: VolatilityMonitorConfig
    monitor: DeribitVolatilityMonitor
    started_at: datetime
    status: str = "运行中"


class DeribitVolatilityMonitorWindow:
    def __init__(
        self,
        parent,
        client: DeribitRestClient,
        notifier_factory: NotifierFactory,
        logger: Logger,
    ) -> None:
        self.client = client
        self._notifier_factory = notifier_factory
        self._logger = logger
        self._defaults = VolatilityMonitorDefaults()
        self._tasks: dict[str, _VolatilityMonitorTask] = {}
        self._task_counter = 0
        self._selected_task_id: str | None = None
        self._refresh_job: str | None = None
        self._destroying = False
        self._diagnostic_lines: deque[str] = deque(maxlen=120)

        self.window = Toplevel(parent)
        self.window.title("Deribit 波动率监控")
        apply_adaptive_window_geometry(
            self.window,
            width_ratio=0.8,
            height_ratio=0.84,
            min_width=1040,
            min_height=900,
            max_width=1640,
            max_height=1180,
        )
        self.window.protocol("WM_DELETE_WINDOW", self._on_close)

        self.resolution_label = StringVar(value=self._defaults.resolution_label)
        self.buffer_seconds = StringVar(value=self._defaults.buffer_seconds)
        self.ema_period = StringVar(value=self._defaults.ema_period)
        self.trend_streak_bars = StringVar(value=self._defaults.trend_streak_bars)
        self.squeeze_bars = StringVar(value=self._defaults.squeeze_bars)
        self.lookback_candles = StringVar(value=self._defaults.lookback_candles)
        self.cumulative_change_threshold = StringVar(value=self._defaults.cumulative_change_threshold)
        self.reversal_body_multiplier = StringVar(value=self._defaults.reversal_body_multiplier)
        self.breakout_body_multiplier = StringVar(value=self._defaults.breakout_body_multiplier)
        self.squeeze_range_ratio = StringVar(value=self._defaults.squeeze_range_ratio)
        self.status_text = StringVar(value="运行中任务: 0 | 全部任务: 0")

        self.enable_btc = BooleanVar(value=True)
        self.enable_eth = BooleanVar(value=False)
        self.enable_bearish_reversal_after_rally = BooleanVar(value=True)
        self.enable_bullish_reversal_after_drop = BooleanVar(value=True)
        self.enable_squeeze_breakout_up = BooleanVar(value=True)
        self.enable_squeeze_breakout_down = BooleanVar(value=True)
        self.enable_box_breakout_up = BooleanVar(value=True)
        self.enable_box_breakout_down = BooleanVar(value=True)
        self.enable_ema34_turn_up = BooleanVar(value=True)
        self.enable_ema34_turn_down = BooleanVar(value=True)

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
        ttk.Label(header, text="Deribit 波动率监控", font=("Microsoft YaHei UI", 18, "bold")).grid(
            row=0, column=0, sticky="w"
        )
        ttk.Label(header, textvariable=self.status_text).grid(row=0, column=1, sticky="e")

        controls = ttk.Frame(self.window, padding=(16, 0, 16, 8))
        controls.grid(row=1, column=0, sticky="nsew")
        controls.columnconfigure(0, weight=1)
        controls.columnconfigure(1, weight=1)

        base_frame = ttk.LabelFrame(controls, text="监控参数", padding=14)
        base_frame.grid(row=0, column=0, sticky="nsew", padx=(0, 8))
        base_frame.columnconfigure(0, weight=1)
        base_frame.columnconfigure(1, weight=1)

        currency_box = ttk.LabelFrame(base_frame, text="监控币种", padding=10)
        currency_box.grid(row=0, column=0, columnspan=2, sticky="ew")
        ttk.Checkbutton(currency_box, text="BTC DVOL", variable=self.enable_btc).grid(row=0, column=0, sticky="w", padx=(0, 16))
        ttk.Checkbutton(currency_box, text="ETH DVOL", variable=self.enable_eth).grid(row=0, column=1, sticky="w")

        ttk.Label(base_frame, text="时间周期").grid(row=1, column=0, sticky="w", pady=(12, 0))
        ttk.Combobox(
            base_frame,
            textvariable=self.resolution_label,
            values=list(DERIBIT_VOL_MONITOR_RESOLUTION_OPTIONS.keys()),
            state="readonly",
        ).grid(row=2, column=0, sticky="ew", pady=(6, 0), padx=(0, 12))

        ttk.Label(base_frame, text="收线缓冲秒数").grid(row=1, column=1, sticky="w", pady=(12, 0))
        ttk.Entry(base_frame, textvariable=self.buffer_seconds).grid(row=2, column=1, sticky="ew", pady=(6, 0))

        ttk.Label(base_frame, text="EMA周期").grid(row=3, column=0, sticky="w", pady=(12, 0))
        ttk.Entry(base_frame, textvariable=self.ema_period).grid(row=4, column=0, sticky="ew", pady=(6, 0), padx=(0, 12))

        ttk.Label(base_frame, text="连续涨跌根数").grid(row=3, column=1, sticky="w", pady=(12, 0))
        ttk.Entry(base_frame, textvariable=self.trend_streak_bars).grid(row=4, column=1, sticky="ew", pady=(6, 0))

        ttk.Label(base_frame, text="窄幅统计根数").grid(row=5, column=0, sticky="w", pady=(12, 0))
        ttk.Entry(base_frame, textvariable=self.squeeze_bars).grid(row=6, column=0, sticky="ew", pady=(6, 0), padx=(0, 12))

        ttk.Label(base_frame, text="历史K线数量").grid(row=5, column=1, sticky="w", pady=(12, 0))
        ttk.Entry(base_frame, textvariable=self.lookback_candles).grid(row=6, column=1, sticky="ew", pady=(6, 0))

        ttk.Label(base_frame, text="连续涨跌阈值").grid(row=7, column=0, sticky="w", pady=(12, 0))
        ttk.Entry(base_frame, textvariable=self.cumulative_change_threshold).grid(row=8, column=0, sticky="ew", pady=(6, 0), padx=(0, 12))

        ttk.Label(base_frame, text="反转实体倍数").grid(row=7, column=1, sticky="w", pady=(12, 0))
        ttk.Entry(base_frame, textvariable=self.reversal_body_multiplier).grid(row=8, column=1, sticky="ew", pady=(6, 0))

        ttk.Label(base_frame, text="突破实体倍数").grid(row=9, column=0, sticky="w", pady=(12, 0))
        ttk.Entry(base_frame, textvariable=self.breakout_body_multiplier).grid(row=10, column=0, sticky="ew", pady=(6, 0), padx=(0, 12))

        ttk.Label(base_frame, text="窄幅压缩比例").grid(row=9, column=1, sticky="w", pady=(12, 0))
        ttk.Entry(base_frame, textvariable=self.squeeze_range_ratio).grid(row=10, column=1, sticky="ew", pady=(6, 0))

        signal_frame = ttk.LabelFrame(controls, text="明显信号配置", padding=14)
        signal_frame.grid(row=0, column=1, sticky="nsew", padx=(8, 0))
        signal_frame.columnconfigure(0, weight=1)

        ttk.Checkbutton(
            signal_frame,
            text="连涨后大阴反转",
            variable=self.enable_bearish_reversal_after_rally,
        ).grid(row=0, column=0, sticky="w", pady=4)
        ttk.Checkbutton(
            signal_frame,
            text="连跌后大阳反转",
            variable=self.enable_bullish_reversal_after_drop,
        ).grid(row=1, column=0, sticky="w", pady=4)
        ttk.Checkbutton(
            signal_frame,
            text="窄幅后大阳突破",
            variable=self.enable_squeeze_breakout_up,
        ).grid(row=2, column=0, sticky="w", pady=4)
        ttk.Checkbutton(
            signal_frame,
            text="窄幅后大阴突破",
            variable=self.enable_squeeze_breakout_down,
        ).grid(row=3, column=0, sticky="w", pady=4)
        ttk.Checkbutton(
            signal_frame,
            text="箱体向上突破",
            variable=self.enable_box_breakout_up,
        ).grid(row=4, column=0, sticky="w", pady=4)
        ttk.Checkbutton(
            signal_frame,
            text="箱体向下突破",
            variable=self.enable_box_breakout_down,
        ).grid(row=5, column=0, sticky="w", pady=4)
        ttk.Checkbutton(
            signal_frame,
            text="EMA34转强",
            variable=self.enable_ema34_turn_up,
        ).grid(row=6, column=0, sticky="w", pady=4)
        ttk.Checkbutton(
            signal_frame,
            text="EMA34转弱",
            variable=self.enable_ema34_turn_down,
        ).grid(row=7, column=0, sticky="w", pady=4)

        button_row = ttk.Frame(signal_frame)
        button_row.grid(row=8, column=0, sticky="w", pady=(14, 0))
        ttk.Button(button_row, text="启动任务", command=self.start).grid(row=0, column=0, padx=(0, 8))
        ttk.Button(button_row, text="停止选中任务", command=self.stop).grid(row=0, column=1, padx=(0, 8))
        ttk.Button(button_row, text="停止全部任务", command=self.stop_all).grid(row=0, column=2)

        ttk.Label(
            signal_frame,
            text=(
                "波动率监控会在对应周期收线并等待缓冲秒数后再检测。"
                "窗口关闭只会隐藏，不会停止已运行任务；信号邮件沿用程序当前邮件设置。"
            ),
            wraplength=430,
            justify="left",
        ).grid(row=9, column=0, sticky="w", pady=(12, 0))

        task_frame = ttk.LabelFrame(self.window, text="监控任务", padding=12)
        task_frame.grid(row=2, column=0, sticky="nsew", padx=16, pady=(0, 8))
        task_frame.columnconfigure(0, weight=1)
        task_frame.rowconfigure(1, weight=1)

        task_header = ttk.Frame(task_frame)
        task_header.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 8))
        task_header.columnconfigure(0, weight=1)
        ttk.Label(task_header, text="每次点击“启动任务”，都会按当前参数创建一个独立波动率监控任务。").grid(
            row=0, column=0, sticky="w"
        )
        ttk.Button(task_header, text="清理已停止", command=self.clear_stopped_tasks).grid(row=0, column=1, sticky="e")

        self.tasks_tree = ttk.Treeview(
            task_frame,
            columns=("task", "resolution", "currencies", "status", "started"),
            show="headings",
            selectmode="browse",
            height=5,
        )
        for column, label, width, anchor in (
            ("task", "任务", 90, "center"),
            ("resolution", "周期", 100, "center"),
            ("currencies", "币种", 300, "w"),
            ("status", "状态", 110, "center"),
            ("started", "启动时间", 160, "center"),
        ):
            self.tasks_tree.heading(column, text=label)
            self.tasks_tree.column(column, width=width, anchor=anchor)
        self.tasks_tree.grid(row=1, column=0, sticky="nsew")
        self.tasks_tree.bind("<<TreeviewSelect>>", self._on_task_selected)
        task_scroll = ttk.Scrollbar(task_frame, orient="vertical", command=self.tasks_tree.yview)
        task_scroll.grid(row=1, column=1, sticky="ns")
        self.tasks_tree.configure(yscrollcommand=task_scroll.set)

        signal_frame = ttk.LabelFrame(self.window, text="最近触发信号", padding=12)
        signal_frame.grid(row=3, column=0, sticky="nsew", padx=16, pady=(0, 8))
        signal_frame.columnconfigure(0, weight=1)
        signal_frame.rowconfigure(0, weight=1)

        self.signal_tree = ttk.Treeview(
            signal_frame,
            columns=("time", "task", "resolution", "symbol", "signal", "direction", "value", "reason"),
            show="headings",
            selectmode="browse",
        )
        for column, label, width, anchor in (
            ("time", "时间", 150, "center"),
            ("task", "任务", 80, "center"),
            ("resolution", "周期", 90, "center"),
            ("symbol", "标的", 110, "w"),
            ("signal", "信号", 150, "center"),
            ("direction", "方向", 80, "center"),
            ("value", "收盘值", 100, "e"),
            ("reason", "说明", 460, "w"),
        ):
            self.signal_tree.heading(column, text=label)
            self.signal_tree.column(column, width=width, anchor=anchor)
        self.signal_tree.grid(row=0, column=0, sticky="nsew")
        signal_scroll = ttk.Scrollbar(signal_frame, orient="vertical", command=self.signal_tree.yview)
        signal_scroll.grid(row=0, column=1, sticky="ns")
        self.signal_tree.configure(yscrollcommand=signal_scroll.set)

        log_frame = ttk.LabelFrame(self.window, text="监控日志", padding=12)
        log_frame.grid(row=4, column=0, sticky="nsew", padx=16, pady=(0, 8))
        log_frame.columnconfigure(0, weight=1)
        log_frame.rowconfigure(0, weight=1)
        self.log_text = Text(log_frame, height=10, wrap="word", font=("Consolas", 10))
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
            text="显示每轮收线检测里命中的信号、被过滤的信号，以及没有新K线时的等待状态。",
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
                f"已有相同配置的波动率监控任务正在运行：{duplicate.session_id} / {_resolution_label(duplicate.config.resolution)}",
                parent=self.window,
            )
            return

        self._task_counter += 1
        session_id = f"V{self._task_counter:02d}"
        monitor = DeribitVolatilityMonitor(
            self.client,
            logger=lambda message, sid=session_id: self._queue_ui(lambda: self._log(sid, message)),
            event_callback=lambda event, sid=session_id, resolution=config.resolution: self._queue_ui(
                lambda: self._append_event(sid, resolution, event)
            ),
            diagnostic_callback=lambda diagnostic, sid=session_id: self._queue_ui(
                lambda: self._append_diagnostic(sid, diagnostic)
            ),
            email_sender=self._build_email_sender(notifier, session_id),
            monitor_name=f"波动率任务 {session_id}",
        )
        task = _VolatilityMonitorTask(
            session_id=session_id,
            config=config,
            monitor=monitor,
            started_at=datetime.now(),
        )
        self._tasks[session_id] = task
        monitor.start(config)
        self._selected_task_id = session_id
        self._log(session_id, f"任务已启动 | 周期={_resolution_label(config.resolution)} | 币种={', '.join(config.currencies)}")
        self._refresh_task_views()

    def stop(self) -> None:
        task = self._selected_task()
        if task is None:
            messagebox.showinfo("提示", "请先在任务列表里选中一个波动率监控任务。", parent=self.window)
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
    ) -> Callable[[VolatilitySignalEvent, str], None] | None:
        if notifier is None or not notifier.signal_notifications_enabled:
            return None

        def _sender(event: VolatilitySignalEvent, resolution: str) -> None:
            subject = (
                f"[QQOKX] 波动率信号 | {session_id} | {_resolution_label(resolution)} | "
                f"{event.symbol} | {VOL_SIGNAL_LABELS[event.signal_type]}"
            )
            body = "\n".join(
                [
                    "模块：Deribit波动率监控",
                    f"任务：{session_id}",
                    f"周期：{_resolution_label(resolution)}",
                    f"标的：{event.symbol}",
                    f"信号：{VOL_SIGNAL_LABELS[event.signal_type]}",
                    f"方向：{VOL_DIRECTION_LABELS[event.direction]}",
                    f"收盘值：{format_decimal_fixed(event.trigger_value, event.decimal_places)}",
                    f"说明：{event.reason}",
                ]
            )
            notifier.notify_async(subject, body)

        return _sender

    def _build_config(self) -> VolatilityMonitorConfig:
        currencies: list[str] = []
        if self.enable_btc.get():
            currencies.append("BTC")
        if self.enable_eth.get():
            currencies.append("ETH")
        if not currencies:
            raise ValueError("请至少选择一个监控币种。")
        if not any(
            (
                self.enable_bearish_reversal_after_rally.get(),
                self.enable_bullish_reversal_after_drop.get(),
                self.enable_squeeze_breakout_up.get(),
                self.enable_squeeze_breakout_down.get(),
                self.enable_box_breakout_up.get(),
                self.enable_box_breakout_down.get(),
                self.enable_ema34_turn_up.get(),
                self.enable_ema34_turn_down.get(),
            )
        ):
            raise ValueError("请至少选择一种明显信号。")

        return VolatilityMonitorConfig(
            currencies=tuple(currencies),
            resolution=DERIBIT_VOL_MONITOR_RESOLUTION_OPTIONS[self.resolution_label.get()],
            buffer_seconds=float(self._parse_positive_decimal(self.buffer_seconds.get(), "收线缓冲秒数")),
            enable_bearish_reversal_after_rally=self.enable_bearish_reversal_after_rally.get(),
            enable_bullish_reversal_after_drop=self.enable_bullish_reversal_after_drop.get(),
            enable_squeeze_breakout_up=self.enable_squeeze_breakout_up.get(),
            enable_squeeze_breakout_down=self.enable_squeeze_breakout_down.get(),
            enable_box_breakout_up=self.enable_box_breakout_up.get(),
            enable_box_breakout_down=self.enable_box_breakout_down.get(),
            enable_ema34_turn_up=self.enable_ema34_turn_up.get(),
            enable_ema34_turn_down=self.enable_ema34_turn_down.get(),
            ema_period=self._parse_positive_int(self.ema_period.get(), "EMA周期"),
            trend_streak_bars=self._parse_positive_int(self.trend_streak_bars.get(), "连续涨跌根数"),
            squeeze_bars=self._parse_positive_int(self.squeeze_bars.get(), "窄幅统计根数"),
            lookback_candles=self._parse_positive_int(self.lookback_candles.get(), "历史K线数量"),
            cumulative_change_threshold=self._parse_positive_decimal(self.cumulative_change_threshold.get(), "连续涨跌阈值"),
            reversal_body_multiplier=self._parse_positive_decimal(self.reversal_body_multiplier.get(), "反转实体倍数"),
            breakout_body_multiplier=self._parse_positive_decimal(self.breakout_body_multiplier.get(), "突破实体倍数"),
            squeeze_range_ratio=self._parse_ratio(self.squeeze_range_ratio.get(), "窄幅压缩比例"),
        )

    def _append_event(self, session_id: str, resolution: str, event: VolatilitySignalEvent) -> None:
        if self.signal_tree is None:
            return
        values = (
            datetime.fromtimestamp(event.candle_ts / 1000).strftime("%Y-%m-%d %H:%M:%S"),
            session_id,
            _resolution_label(resolution),
            event.symbol,
            VOL_SIGNAL_LABELS[event.signal_type],
            VOL_DIRECTION_LABELS[event.direction],
            format_decimal_fixed(event.trigger_value, event.decimal_places),
            event.reason,
        )
        self.signal_tree.insert("", 0, values=values)
        children = self.signal_tree.get_children()
        for item_id in children[300:]:
            self.signal_tree.delete(item_id)

    def _append_diagnostic(self, session_id: str, diagnostic: VolatilityMonitorRoundDiagnostic) -> None:
        if self.diagnostic_text is None:
            return
        self._diagnostic_lines.append(format_volatility_diagnostic_round(session_id, diagnostic))
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
        self._logger(f"[波动率监控 {session_id}] {message}")

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

    def _selected_task(self) -> _VolatilityMonitorTask | None:
        if self._selected_task_id is None:
            return None
        return self._tasks.get(self._selected_task_id)

    def _find_running_duplicate(self, config: VolatilityMonitorConfig) -> _VolatilityMonitorTask | None:
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
                _resolution_label(task.config.resolution),
                ", ".join(f"{item} DVOL" for item in task.config.currencies),
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
        self._logger("[波动率监控] 窗口已隐藏，监控任务继续运行。")


def _resolution_label(value: str) -> str:
    for label, raw in DERIBIT_VOL_MONITOR_RESOLUTION_OPTIONS.items():
        if raw == value:
            return label
    seconds = DERIBIT_VOL_RESOLUTION_SECONDS.get(value)
    if seconds == 3_600:
        return "1小时"
    if seconds == 43_200:
        return "12小时"
    if seconds == 86_400:
        return "1天"
    return value
