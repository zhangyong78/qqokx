from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
import threading
from tkinter import BooleanVar, Canvas, END, StringVar, Text, Toplevel
from tkinter import messagebox, ttk
from typing import Callable, Literal

from okx_quant.indicators import ema
from okx_quant.models import Candle
from okx_quant.notifications import EmailNotifier
from okx_quant.pricing import format_decimal_by_increment, format_decimal_fixed
from okx_quant.signal_monitor import (
    DEFAULT_MONITOR_SYMBOLS,
    MonitorRoundDiagnostic,
    MonitorSignalEvent,
    SignalMonitor,
    SignalMonitorConfig,
    SignalType,
    evaluate_monitor_signal_history,
)
from okx_quant.window_layout import apply_adaptive_window_geometry, apply_window_icon


NotifierFactory = Callable[[], EmailNotifier | None]
Logger = Callable[[str], None]
DEFAULT_SIGNAL_CHART_CANDLE_LIMIT = 1000
MAX_SIGNAL_CHART_CANDLE_LIMIT = 10000


@dataclass(frozen=True)
class _SignalEventRowPayload:
    session_id: str
    bar: str
    config: SignalMonitorConfig
    event: MonitorSignalEvent


@dataclass(frozen=True)
class _SignalDefinition:
    signal_type: SignalType
    label: str


@dataclass(frozen=True)
class _SignalChartRequest:
    mode: Literal["event", "preview"]
    symbol: str
    bar: str
    config: SignalMonitorConfig
    signal_type: SignalType
    candle_limit: int
    selected_event: MonitorSignalEvent | None = None
    session_id: str | None = None


SIGNAL_DEFINITIONS: tuple[_SignalDefinition, ...] = (
    _SignalDefinition("ema21_55_cross", "EMA21/55金叉死叉"),
    _SignalDefinition("ema55_slope_turn", "EMA55斜率改变"),
    _SignalDefinition("ema55_breakout", "EMA55突破"),
    _SignalDefinition("candle_pattern", "K线形态信号"),
)
SIGNAL_TYPE_LABELS = {item.signal_type: item.label for item in SIGNAL_DEFINITIONS}


@dataclass(frozen=True)
class _SignalChartDataset:
    request: _SignalChartRequest
    candles: tuple[Candle, ...]
    history_events: tuple[MonitorSignalEvent, ...]
    tick_size: Decimal | None
    primary_ema_label: str | None
    primary_ema_values: tuple[Decimal, ...]
    secondary_ema_label: str | None
    secondary_ema_values: tuple[Decimal, ...]


@dataclass
class _SignalChartViewport:
    start_index: int = 0
    visible_count: int | None = None
    pan_anchor_x: int | None = None
    pan_anchor_start: int = 0


@dataclass(frozen=True)
class _SignalChartRenderState:
    left: int
    right: int
    top: int
    bottom: int
    width: int
    height: int
    start_index: int
    end_index: int
    candle_step: float
    price_min: Decimal
    price_max: Decimal


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
        self._signal_row_payloads: dict[str, _SignalEventRowPayload] = {}
        self._signal_chart_window: Toplevel | None = None
        self._signal_chart_canvas: Canvas | None = None
        self._signal_chart_dataset: _SignalChartDataset | None = None
        self._signal_chart_active_request: _SignalChartRequest | None = None
        self._signal_chart_request_id = 0
        self._signal_chart_summary = StringVar(value="选择最近触发信号，或使用右侧按钮预览当前信号配置。")
        self._signal_chart_hint = StringVar(
            value="橙色标记为当前信号，蓝色为做多历史信号，紫色为做空历史信号；支持滚轮缩放、左键拖动和平移、悬浮十字光标。"
        )
        self._signal_chart_view = _SignalChartViewport()
        self._signal_chart_render_state: _SignalChartRenderState | None = None
        self._signal_chart_hover_index: int | None = None
        self._signal_chart_hover_y: float | None = None
        self._content_pane: ttk.Panedwindow | None = None
        self._signal_preview_symbol_box: ttk.Combobox | None = None

        self.window = Toplevel(parent)
        self.window.title("信号监控")
        apply_window_icon(self.window)
        apply_adaptive_window_geometry(
            self.window,
            width_ratio=0.84,
            height_ratio=0.88,
            min_width=1260,
            min_height=980,
            max_width=1780,
            max_height=1260,
        )
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
        self.signal_chart_candle_limit = StringVar(value=str(DEFAULT_SIGNAL_CHART_CANDLE_LIMIT))
        self.signal_preview_symbol = StringVar(value=DEFAULT_MONITOR_SYMBOLS[0][1])
        self.status_text = StringVar(value="运行中任务: 0 | 全部任务: 0")

        self._symbol_vars: dict[str, tuple[str, BooleanVar]] = {
            label: (inst_id, BooleanVar(value=True)) for label, inst_id in DEFAULT_MONITOR_SYMBOLS
        }
        self._signal_enabled_vars: dict[SignalType, BooleanVar] = {
            "ema21_55_cross": self.enable_ema21_55_cross,
            "ema55_slope_turn": self.enable_ema55_slope_turn,
            "ema55_breakout": self.enable_ema55_breakout,
            "candle_pattern": self.enable_candle_pattern,
        }
        self.tasks_tree: ttk.Treeview | None = None
        self.signal_tree: ttk.Treeview | None = None
        self.log_text: Text | None = None
        self.diagnostic_text: Text | None = None

        self.custom_symbols.trace_add("write", lambda *_: self._sync_signal_preview_symbol())
        for _, var in self._symbol_vars.values():
            var.trace_add("write", lambda *_: self._sync_signal_preview_symbol())

        self._build_layout()
        self._sync_signal_preview_symbol()
        self.window.after_idle(self._apply_initial_layout_preferences)
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
        self._close_signal_chart_window()
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
        self.window.rowconfigure(2, weight=1)

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
        signal_frame.columnconfigure(1, weight=0)

        for row_index, definition in enumerate(SIGNAL_DEFINITIONS):
            ttk.Checkbutton(
                signal_frame,
                text=definition.label,
                variable=self._signal_enabled_vars[definition.signal_type],
            ).grid(row=row_index, column=0, sticky="w", pady=4)
            ttk.Button(
                signal_frame,
                text="查看K线",
                command=lambda signal_type=definition.signal_type: self._open_signal_preview(signal_type),
            ).grid(row=row_index, column=1, sticky="e", pady=2)

        preview_box = ttk.LabelFrame(signal_frame, text="信号预览", padding=10)
        preview_box.grid(row=4, column=0, columnspan=2, sticky="ew", pady=(12, 0))
        preview_box.columnconfigure(1, weight=1)
        preview_box.columnconfigure(3, weight=1)
        ttk.Label(preview_box, text="预览币种").grid(row=0, column=0, sticky="w")
        preview_symbol_box = ttk.Combobox(preview_box, textvariable=self.signal_preview_symbol)
        preview_symbol_box.grid(row=0, column=1, sticky="ew", padx=(8, 12))
        self._signal_preview_symbol_box = preview_symbol_box
        ttk.Label(preview_box, text="K线数量").grid(row=0, column=2, sticky="w")
        ttk.Entry(preview_box, textvariable=self.signal_chart_candle_limit, width=10).grid(
            row=0, column=3, sticky="w", padx=(8, 0)
        )
        ttk.Label(
            preview_box,
            text="点击上方按钮，可按当前参数直接预览该信号在历史K线中的触发位置。",
            foreground="#57606a",
            justify="left",
        ).grid(row=1, column=0, columnspan=4, sticky="w", pady=(8, 0))

        pattern_box = ttk.LabelFrame(signal_frame, text="K线形态信号参数", padding=10)
        pattern_box.grid(row=5, column=0, columnspan=2, sticky="ew", pady=(12, 0))
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
        button_row.grid(row=6, column=0, columnspan=2, sticky="w", pady=(14, 0))
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
        ).grid(row=7, column=0, columnspan=2, sticky="w", pady=(12, 0))

        content_pane = ttk.Panedwindow(self.window, orient="vertical")
        content_pane.grid(row=2, column=0, sticky="nsew", padx=16, pady=(0, 16))
        self._content_pane = content_pane

        task_frame = ttk.LabelFrame(content_pane, text="监控任务", padding=12)
        task_frame.columnconfigure(0, weight=1)
        task_frame.rowconfigure(1, weight=1)
        content_pane.add(task_frame, weight=2)

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

        signal_list_frame = ttk.LabelFrame(content_pane, text="最近触发信号", padding=12)
        signal_list_frame.columnconfigure(0, weight=1)
        signal_list_frame.rowconfigure(0, weight=1)
        content_pane.add(signal_list_frame, weight=3)

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
        self.signal_tree.bind("<<TreeviewSelect>>", self._on_signal_selected)

        signal_scroll = ttk.Scrollbar(signal_list_frame, orient="vertical", command=self.signal_tree.yview)
        signal_scroll.grid(row=0, column=1, sticky="ns")
        self.signal_tree.configure(yscrollcommand=signal_scroll.set)

        log_frame = ttk.LabelFrame(content_pane, text="监控日志", padding=12)
        log_frame.columnconfigure(0, weight=1)
        log_frame.rowconfigure(0, weight=1)
        content_pane.add(log_frame, weight=3)

        self.log_text = Text(log_frame, height=12, wrap="word", font=("Consolas", 10))
        self.log_text.grid(row=0, column=0, sticky="nsew")
        log_scroll = ttk.Scrollbar(log_frame, orient="vertical", command=self.log_text.yview)
        log_scroll.grid(row=0, column=1, sticky="ns")
        self.log_text.configure(yscrollcommand=log_scroll.set)

        diagnostic_frame = ttk.LabelFrame(content_pane, text="实时诊断", padding=12)
        diagnostic_frame.columnconfigure(0, weight=1)
        diagnostic_frame.rowconfigure(1, weight=1)
        content_pane.add(diagnostic_frame, weight=2)

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

    def _apply_initial_layout_preferences(self) -> None:
        self.window.update_idletasks()
        if self._content_pane is None or len(self._content_pane.panes()) < 4:
            return

        window_height = max(self.window.winfo_height(), self.window.winfo_reqheight())
        content_height = max(self._content_pane.winfo_height(), window_height - 300)
        if window_height < 1020:
            ratios = (0.14, 0.31, 0.24)
        else:
            ratios = (0.15, 0.33, 0.24)

        positions = [
            max(120, int(content_height * ratios[0])),
            max(360, int(content_height * (ratios[0] + ratios[1]))),
            max(560, int(content_height * (ratios[0] + ratios[1] + ratios[2]))),
        ]
        for index, position in enumerate(positions):
            try:
                self._content_pane.sashpos(index, position)
            except Exception:
                break

    def _collect_configured_symbols(self) -> list[str]:
        symbols = [inst_id for _, (inst_id, var) in self._symbol_vars.items() if var.get()]
        for item in self.custom_symbols.get().replace(";", ",").split(","):
            cleaned = item.strip().upper()
            if cleaned:
                symbols.append(_normalize_symbol_input(cleaned))
        return list(dict.fromkeys(symbols))

    def _sync_signal_preview_symbol(self) -> None:
        configured_symbols = self._collect_configured_symbols()
        if self._signal_preview_symbol_box is not None:
            self._signal_preview_symbol_box.configure(values=configured_symbols)

        current = self.signal_preview_symbol.get().strip()
        if current:
            return
        if configured_symbols:
            self.signal_preview_symbol.set(configured_symbols[0])
            return
        self.signal_preview_symbol.set(DEFAULT_MONITOR_SYMBOLS[0][1])

    def _parse_signal_chart_candle_limit(self) -> int:
        value = self._parse_positive_int(self.signal_chart_candle_limit.get(), "信号图K线数量")
        if value > MAX_SIGNAL_CHART_CANDLE_LIMIT:
            raise ValueError(f"信号图K线数量最多支持 {MAX_SIGNAL_CHART_CANDLE_LIMIT} 根")
        return value

    def _build_signal_preview_request(self, signal_type: SignalType) -> _SignalChartRequest:
        symbol = _normalize_symbol_input(self.signal_preview_symbol.get())
        if not symbol:
            raise ValueError("请先填写要预览的币种")
        config = SignalMonitorConfig(
            symbols=(symbol,),
            bar=self.bar.get(),
            poll_seconds=float(self._defaults.poll_seconds),
            enable_ema21_55_cross=signal_type == "ema21_55_cross",
            enable_ema55_slope_turn=signal_type == "ema55_slope_turn",
            enable_ema55_breakout=signal_type == "ema55_breakout",
            enable_candle_pattern=signal_type == "candle_pattern",
            pattern_ema_period=self._parse_positive_int(self.pattern_ema_period.get(), "EMA周期"),
            ema_near_tolerance=self._parse_positive_decimal(self.ema_near_tolerance.get(), "EMA附近容差"),
            body_ratio_threshold=self._parse_ratio(self.body_ratio_threshold.get(), "大K线实体比例阈值"),
            wick_ratio_threshold=self._parse_ratio(self.wick_ratio_threshold.get(), "影线比例阈值"),
        )
        return _SignalChartRequest(
            mode="preview",
            symbol=symbol,
            bar=config.bar,
            config=config,
            signal_type=signal_type,
            candle_limit=self._parse_signal_chart_candle_limit(),
        )

    def _build_signal_event_request(self, payload: _SignalEventRowPayload) -> _SignalChartRequest:
        return _SignalChartRequest(
            mode="event",
            symbol=payload.event.symbol,
            bar=payload.bar,
            config=payload.config,
            signal_type=payload.event.signal_type,
            candle_limit=self._parse_signal_chart_candle_limit(),
            selected_event=payload.event,
            session_id=payload.session_id,
        )

    def _open_signal_preview(self, signal_type: SignalType) -> None:
        try:
            request = self._build_signal_preview_request(signal_type)
        except Exception as exc:
            messagebox.showerror("预览失败", str(exc), parent=self.window)
            return
        self._open_signal_chart(request)

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
        symbols = self._collect_configured_symbols()
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
        task = self._tasks.get(session_id)
        values = (
            datetime.fromtimestamp(event.candle_ts / 1000).strftime("%Y-%m-%d %H:%M:%S"),
            session_id,
            bar,
            event.symbol,
            SIGNAL_TYPE_LABELS.get(event.signal_type, event.signal_type),
            "做多" if event.direction == "long" else "做空",
            format_decimal_by_increment(event.trigger_price, event.tick_size),
            event.reason,
        )
        item_id = self.signal_tree.insert("", 0, values=values)
        if task is not None:
            self._signal_row_payloads[item_id] = _SignalEventRowPayload(
                session_id=session_id,
                bar=bar,
                config=task.config,
                event=event,
            )
        children = self.signal_tree.get_children()
        for item_id in children[300:]:
            self._signal_row_payloads.pop(item_id, None)
            self.signal_tree.delete(item_id)

    def _on_signal_selected(self, *_: object) -> None:
        if self.signal_tree is None:
            return
        selection = self.signal_tree.selection()
        if not selection:
            return
        payload = self._signal_row_payloads.get(selection[0])
        if payload is None:
            return
        try:
            request = self._build_signal_event_request(payload)
        except Exception as exc:
            messagebox.showerror("信号图加载失败", str(exc), parent=self.window)
            return
        self._open_signal_chart(request)

    def _open_signal_chart(self, request: _SignalChartRequest) -> None:
        self._signal_chart_active_request = request
        self._signal_chart_dataset = None
        self._ensure_signal_chart_window()
        signal_label = SIGNAL_TYPE_LABELS.get(request.signal_type, request.signal_type)
        if request.mode == "preview":
            self._signal_chart_summary.set(
                f"正在加载 {request.symbol} {request.bar} 的 {request.candle_limit} 根 K 线，并预览 {signal_label} 的历史触发位置..."
            )
            self._signal_chart_hint.set(
                "蓝色为做多信号，紫色为做空信号；支持滚轮缩放、左键拖动和平移，修改 K 线数量后可刷新当前图表。"
            )
        else:
            self._signal_chart_summary.set(
                f"正在加载 {request.symbol} {request.bar} 的 {request.candle_limit} 根 K 线，并回放 {signal_label} 历史信号..."
            )
            self._signal_chart_hint.set(
                "橙色标记为当前信号，蓝色为做多历史信号，紫色为做空历史信号；支持滚轮缩放、左键拖动和平移、悬浮十字光标。"
            )
        if self._signal_chart_canvas is not None:
            self._clear_signal_chart_canvas(self._signal_chart_canvas, message="正在加载 K 线与历史信号...")
        self._signal_chart_request_id += 1
        request_id = self._signal_chart_request_id
        threading.Thread(
            target=self._load_signal_chart_dataset,
            args=(request_id, request),
            daemon=True,
            name="qqokx-signal-chart",
        ).start()

    def _ensure_signal_chart_window(self) -> None:
        if self._signal_chart_window is not None and self._signal_chart_window.winfo_exists():
            self._signal_chart_window.deiconify()
            self._signal_chart_window.lift()
            self._signal_chart_window.focus_force()
            return

        chart_window = Toplevel(self.window)
        chart_window.title("信号 K 线回放")
        apply_adaptive_window_geometry(
            chart_window,
            width_ratio=0.88,
            height_ratio=0.78,
            min_width=1160,
            min_height=720,
            max_width=1860,
            max_height=1120,
        )
        chart_window.columnconfigure(0, weight=1)
        chart_window.rowconfigure(1, weight=1)
        chart_window.protocol("WM_DELETE_WINDOW", self._close_signal_chart_window)

        header = ttk.Frame(chart_window, padding=(12, 12, 12, 0))
        header.grid(row=0, column=0, sticky="ew")
        header.columnconfigure(0, weight=1)
        ttk.Label(
            header,
            textvariable=self._signal_chart_summary,
            justify="left",
            wraplength=1180,
        ).grid(row=0, column=0, sticky="w")
        control_box = ttk.Frame(header)
        control_box.grid(row=0, column=1, sticky="e", padx=(12, 0))
        ttk.Label(control_box, text="K线数").grid(row=0, column=0, sticky="e", padx=(0, 6))
        ttk.Entry(control_box, textvariable=self.signal_chart_candle_limit, width=8).grid(row=0, column=1, sticky="e")
        ttk.Button(control_box, text="刷新当前", command=self._refresh_active_signal_chart).grid(
            row=0, column=2, sticky="e", padx=(8, 8)
        )
        ttk.Button(control_box, text="重置视图", command=self._reset_signal_chart_view).grid(
            row=0, column=3, sticky="e", padx=(0, 8)
        )
        ttk.Button(control_box, text="关闭", command=self._close_signal_chart_window).grid(row=0, column=4, sticky="e")
        ttk.Label(
            header,
            textvariable=self._signal_chart_hint,
            justify="left",
            foreground="#57606a",
            wraplength=1180,
        ).grid(row=1, column=0, columnspan=2, sticky="w", pady=(8, 0))

        chart_canvas = Canvas(chart_window, background="#ffffff", highlightthickness=0)
        chart_canvas.grid(row=1, column=0, sticky="nsew", padx=12, pady=12)
        chart_canvas.bind("<Configure>", lambda *_: self._redraw_signal_chart())
        self._bind_signal_chart_interactions(chart_canvas)

        self._signal_chart_window = chart_window
        self._signal_chart_canvas = chart_canvas
        if self._signal_chart_dataset is not None:
            self._redraw_signal_chart()
        else:
            self._clear_signal_chart_canvas(chart_canvas)

    def _refresh_active_signal_chart(self) -> None:
        request = self._signal_chart_active_request
        if request is None:
            return
        try:
            if request.mode == "preview":
                refreshed_request = self._build_signal_preview_request(request.signal_type)
            else:
                refreshed_request = _SignalChartRequest(
                    mode="event",
                    symbol=request.symbol,
                    bar=request.bar,
                    config=request.config,
                    signal_type=request.signal_type,
                    candle_limit=self._parse_signal_chart_candle_limit(),
                    selected_event=request.selected_event,
                    session_id=request.session_id,
                )
        except Exception as exc:
            messagebox.showerror("刷新失败", str(exc), parent=self._signal_chart_window or self.window)
            return
        self._open_signal_chart(refreshed_request)

    def _bind_signal_chart_interactions(self, canvas: Canvas) -> None:
        canvas.bind("<MouseWheel>", lambda event, target=canvas: self._on_signal_chart_mousewheel(target, event))
        canvas.bind("<ButtonPress-1>", lambda event, target=canvas: self._on_signal_chart_press(target, event))
        canvas.bind("<B1-Motion>", lambda event, target=canvas: self._on_signal_chart_drag(target, event))
        canvas.bind("<ButtonRelease-1>", lambda _event, target=canvas: self._on_signal_chart_release(target))
        canvas.bind("<Motion>", lambda event, target=canvas: self._on_signal_chart_motion(target, event))
        canvas.bind("<Leave>", lambda _event, target=canvas: self._clear_signal_chart_hover(target))

    def _reset_signal_chart_view(self) -> None:
        self._signal_chart_view = _SignalChartViewport()
        self._signal_chart_hover_index = None
        self._signal_chart_hover_y = None
        self._redraw_signal_chart()

    def _on_signal_chart_mousewheel(self, canvas: Canvas, event: object) -> None:
        dataset = self._signal_chart_dataset
        if dataset is None:
            return
        delta = getattr(event, "delta", 0)
        if delta == 0:
            return
        width = max(canvas.winfo_width(), 960)
        left = 56
        right = 20
        inner_width = max(width - left - right, 1)
        cursor_x = float(getattr(event, "x", left + inner_width / 2))
        anchor_ratio = min(max((cursor_x - left) / inner_width, 0.0), 1.0)
        next_start_index, next_visible_count = _zoom_signal_chart_viewport(
            start_index=self._signal_chart_view.start_index,
            visible_count=self._signal_chart_view.visible_count,
            total_count=len(dataset.candles),
            anchor_ratio=anchor_ratio,
            zoom_in=delta > 0,
        )
        if (
            next_start_index == self._signal_chart_view.start_index
            and next_visible_count == self._signal_chart_view.visible_count
        ):
            return
        self._signal_chart_view.start_index = next_start_index
        self._signal_chart_view.visible_count = next_visible_count
        self._redraw_signal_chart()

    def _on_signal_chart_press(self, canvas: Canvas, event: object) -> None:
        if self._signal_chart_dataset is None:
            return
        self._signal_chart_view.pan_anchor_x = int(getattr(event, "x", 0))
        self._signal_chart_view.pan_anchor_start = self._signal_chart_view.start_index
        self._clear_signal_chart_hover(canvas)

    def _on_signal_chart_drag(self, canvas: Canvas, event: object) -> None:
        dataset = self._signal_chart_dataset
        if dataset is None:
            return
        if self._signal_chart_view.pan_anchor_x is None:
            return
        width = max(canvas.winfo_width(), 960)
        left = 56
        right = 20
        inner_width = max(width - left - right, 1)
        _, visible_count = _normalize_signal_chart_viewport(
            self._signal_chart_view.start_index,
            self._signal_chart_view.visible_count,
            len(dataset.candles),
        )
        candle_step = inner_width / max(visible_count, 1)
        current_x = int(getattr(event, "x", self._signal_chart_view.pan_anchor_x))
        shift = int(round((self._signal_chart_view.pan_anchor_x - current_x) / max(candle_step, 1)))
        next_start_index = _pan_signal_chart_viewport(
            self._signal_chart_view.pan_anchor_start,
            visible_count,
            len(dataset.candles),
            shift,
        )
        if next_start_index == self._signal_chart_view.start_index:
            return
        self._signal_chart_view.start_index = next_start_index
        self._redraw_signal_chart(fast_mode=True)

    def _on_signal_chart_release(self, canvas: Canvas) -> None:
        self._signal_chart_view.pan_anchor_x = None
        self._redraw_signal_chart()

    def _on_signal_chart_motion(self, canvas: Canvas, event: object) -> None:
        if self._signal_chart_dataset is None:
            return
        state = self._signal_chart_render_state
        if state is None:
            return
        index = _signal_chart_hover_index_for_x(
            x=float(getattr(event, "x", -1)),
            left=state.left,
            width=state.width - state.left - state.right,
            start_index=state.start_index,
            end_index=state.end_index,
            candle_step=state.candle_step,
        )
        hover_y = float(getattr(event, "y", -1))
        if self._signal_chart_hover_index == index and self._signal_chart_hover_y == hover_y:
            return
        self._signal_chart_hover_index = index
        self._signal_chart_hover_y = hover_y
        self._render_signal_chart_hover(canvas)

    def _clear_signal_chart_hover(self, canvas: Canvas) -> None:
        self._signal_chart_hover_index = None
        self._signal_chart_hover_y = None
        canvas.delete("signal-chart-hover")

    def _load_signal_chart_dataset(self, request_id: int, request: _SignalChartRequest) -> None:
        try:
            history_fetcher = getattr(self.client, "get_candles_history", None)
            if callable(history_fetcher):
                candles = history_fetcher(request.symbol, request.bar, limit=request.candle_limit)
            else:
                candles = self.client.get_candles(request.symbol, request.bar, limit=request.candle_limit)
            candles = [item for item in candles if item.confirmed]
            tick_size = request.selected_event.tick_size if request.selected_event is not None else None
            if tick_size is None:
                try:
                    tick_size = self.client.get_instrument(request.symbol).tick_size
                except Exception:
                    tick_size = None
            history_events = evaluate_monitor_signal_history(
                candles,
                request.symbol,
                request.config,
                tick_size=tick_size,
                signal_type=request.signal_type,
            )
            primary_label, primary_values, secondary_label, secondary_values = _build_signal_chart_ema_series(
                candles,
                request.config,
                request.signal_type,
            )
            dataset = _SignalChartDataset(
                request=request,
                candles=tuple(candles),
                history_events=tuple(history_events),
                tick_size=tick_size,
                primary_ema_label=primary_label,
                primary_ema_values=tuple(primary_values),
                secondary_ema_label=secondary_label,
                secondary_ema_values=tuple(secondary_values),
            )
        except Exception as exc:
            self._queue_ui(lambda rid=request_id, message=str(exc): self._apply_signal_chart_error(rid, message))
            return

        self._queue_ui(lambda rid=request_id, data=dataset: self._apply_signal_chart_dataset(rid, data))

    def _apply_signal_chart_dataset(self, request_id: int, dataset: _SignalChartDataset) -> None:
        if request_id != self._signal_chart_request_id:
            return
        self._signal_chart_dataset = dataset
        self._signal_chart_active_request = dataset.request
        self._signal_chart_view = _SignalChartViewport()
        self._signal_chart_render_state = None
        self._signal_chart_hover_index = None
        self._signal_chart_hover_y = None
        request = dataset.request
        signal_label = SIGNAL_TYPE_LABELS.get(request.signal_type, request.signal_type)
        history_count = len(dataset.history_events)
        if request.selected_event is not None:
            selected_event = request.selected_event
            selected_found = any(_monitor_events_match(item, selected_event) for item in dataset.history_events)
            summary = (
                f"币种: {request.symbol} | 周期: {request.bar} | 规则: {signal_label} | "
                f"当前信号时间: {datetime.fromtimestamp(selected_event.candle_ts / 1000).strftime('%Y-%m-%d %H:%M:%S')} | "
                f"当前加载 K 线: {len(dataset.candles)} 根 | 历史同规则信号: {history_count} 个"
            )
            if not selected_found:
                summary = f"{summary} | 当前信号不在已加载 K 线范围内"
        else:
            latest_event_time = (
                datetime.fromtimestamp(dataset.history_events[-1].candle_ts / 1000).strftime("%Y-%m-%d %H:%M:%S")
                if dataset.history_events
                else "暂无"
            )
            summary = (
                f"币种: {request.symbol} | 周期: {request.bar} | 规则: {signal_label} | "
                f"当前加载 K 线: {len(dataset.candles)} 根 | 触发次数: {history_count} 个 | 最近触发: {latest_event_time}"
            )
        self._signal_chart_summary.set(summary)
        self._redraw_signal_chart()

    def _apply_signal_chart_error(self, request_id: int, message: str) -> None:
        if request_id != self._signal_chart_request_id:
            return
        self._signal_chart_dataset = None
        self._signal_chart_render_state = None
        self._signal_chart_hover_index = None
        self._signal_chart_hover_y = None
        self._signal_chart_summary.set(f"信号图加载失败: {message}")
        if self._signal_chart_canvas is not None:
            self._clear_signal_chart_canvas(self._signal_chart_canvas, message="K 线加载失败，请重试。")

    def _redraw_signal_chart(self) -> None:
        if self._signal_chart_canvas is None or not self._signal_chart_canvas.winfo_exists():
            return
        if self._signal_chart_dataset is None:
            self._clear_signal_chart_canvas(self._signal_chart_canvas)
            return
        self._draw_signal_chart(self._signal_chart_canvas, self._signal_chart_dataset)

    def _draw_signal_chart(self, canvas: Canvas, dataset: _SignalChartDataset) -> None:
        canvas.delete("all")
        candles = list(dataset.candles)
        if not candles:
            self._clear_signal_chart_canvas(canvas, message="没有拿到可绘制的 K 线数据。")
            return

        width = max(canvas.winfo_width(), 960)
        height = max(canvas.winfo_height(), 520)
        left = 56
        right = 20
        top = 20
        bottom = 34
        inner_width = width - left - right
        inner_height = height - top - bottom
        if inner_width <= 0 or inner_height <= 0:
            return

        candle_step = inner_width / max(len(candles), 1)
        body_width = max(1.0, min(10.0, candle_step * 0.62))
        plotted_prices = [float(item.high) for item in candles] + [float(item.low) for item in candles]
        plotted_prices.extend(float(item) for item in dataset.primary_ema_values)
        plotted_prices.extend(float(item) for item in dataset.secondary_ema_values)
        if dataset.history_events:
            plotted_prices.extend(float(item.trigger_price) for item in dataset.history_events)
        price_max = max(plotted_prices)
        price_min = min(plotted_prices)
        if price_max == price_min:
            price_max += 1
            price_min -= 1

        def x_for(index: int) -> float:
            return left + (index * candle_step) + (candle_step / 2)

        def y_for(price: Decimal) -> float:
            ratio = (price_max - float(price)) / max(price_max - price_min, 1e-12)
            return top + (ratio * inner_height)

        canvas.create_rectangle(left, top, width - right, height - bottom, outline="#d0d7de")
        for price_value in _chart_price_axis_values(Decimal(str(price_min)), Decimal(str(price_max)), steps=4):
            y = y_for(price_value)
            canvas.create_line(left, y, width - right, y, fill="#eaeef2", dash=(2, 4))
            canvas.create_text(
                left - 8,
                y,
                text=_format_chart_axis_price(price_value),
                anchor="e",
                fill="#57606a",
                font=("Microsoft YaHei UI", 9),
            )

        for index, candle in enumerate(candles):
            x = x_for(index)
            open_y = y_for(candle.open)
            close_y = y_for(candle.close)
            high_y = y_for(candle.high)
            low_y = y_for(candle.low)
            color = _signal_chart_candle_color(candle.open, candle.close)
            canvas.create_line(x, high_y, x, low_y, fill=color, width=1)
            body_top = min(open_y, close_y)
            body_bottom = max(open_y, close_y)
            if abs(body_bottom - body_top) < 1:
                body_bottom = body_top + 1
            canvas.create_rectangle(
                x - (body_width / 2),
                body_top,
                x + (body_width / 2),
                body_bottom,
                outline=color,
                fill=color,
            )

        if len(dataset.primary_ema_values) >= 2:
            primary_points: list[float] = []
            for index, value in enumerate(dataset.primary_ema_values):
                primary_points.extend((x_for(index), y_for(value)))
            canvas.create_line(*primary_points, fill="#ff8c00", width=2, smooth=True)
        if len(dataset.secondary_ema_values) >= 2:
            secondary_points: list[float] = []
            for index, value in enumerate(dataset.secondary_ema_values):
                secondary_points.extend((x_for(index), y_for(value)))
            canvas.create_line(*secondary_points, fill="#0a7f5a", width=2, smooth=True)

        candle_index_by_ts = {item.ts: index for index, item in enumerate(candles)}
        selected_event = dataset.request.selected_event
        for event in dataset.history_events:
            candle_index = candle_index_by_ts.get(event.candle_ts)
            if candle_index is None:
                continue
            candle = candles[candle_index]
            x = x_for(candle_index)
            is_selected = selected_event is not None and _monitor_events_match(event, selected_event)
            marker_color = "#f59e0b" if is_selected else ("#0969da" if event.direction == "long" else "#8250df")
            marker_size = 9 if is_selected else 6
            if event.direction == "long":
                marker_y = min(y_for(candle.low) + 18, height - bottom - 10)
                points = (
                    x,
                    marker_y - marker_size,
                    x - marker_size,
                    marker_y + marker_size,
                    x + marker_size,
                    marker_y + marker_size,
                )
            else:
                marker_y = max(y_for(candle.high) - 18, top + 10)
                points = (
                    x,
                    marker_y + marker_size,
                    x - marker_size,
                    marker_y - marker_size,
                    x + marker_size,
                    marker_y - marker_size,
                )
            canvas.create_polygon(*points, fill=marker_color, outline="")
            if is_selected:
                canvas.create_line(x, top, x, height - bottom, fill="#f59e0b", dash=(4, 4))
                canvas.create_text(
                    x + 8,
                    marker_y - 12 if event.direction == "short" else marker_y + 12,
                    text="当前",
                    anchor="w",
                    fill="#b54708",
                    font=("Microsoft YaHei UI", 9, "bold"),
                )

        for time_index in _chart_time_label_indices(len(candles), target_labels=6):
            x = x_for(time_index)
            canvas.create_line(x, top, x, height - bottom, fill="#f3f4f6", dash=(2, 4))
            canvas.create_line(x, height - bottom, x, height - bottom + 5, fill="#8c959f")
            canvas.create_text(
                x,
                height - bottom + 8,
                text=_format_chart_timestamp(candles[time_index].ts),
                anchor="n",
                fill="#57606a",
                font=("Microsoft YaHei UI", 9),
            )

        legend_lines = ["K线"]
        if dataset.primary_ema_label:
            legend_lines.append(dataset.primary_ema_label)
        if dataset.secondary_ema_label:
            legend_lines.append(dataset.secondary_ema_label)
        legend_lines.append("橙色=当前")
        legend_lines.append("蓝色=做多历史")
        legend_lines.append("紫色=做空历史")
        canvas.create_text(
            width - right,
            top + 10,
            text=" | ".join(legend_lines),
            anchor="ne",
            fill="#57606a",
            font=("Microsoft YaHei UI", 9, "bold"),
        )

    def _clear_signal_chart_canvas(self, canvas: Canvas, *, message: str | None = None) -> None:
        self._signal_chart_render_state = None
        self._signal_chart_hover_index = None
        self._signal_chart_hover_y = None
        canvas.delete("all")
        width = max(canvas.winfo_width(), 960)
        height = max(canvas.winfo_height(), 520)
        canvas.create_rectangle(0, 0, width, height, outline="", fill="#ffffff")
        canvas.create_text(
            width / 2,
            height / 2,
            text=message or "选择最近触发信号，或点击右侧信号配置里的“查看K线”按钮进行预览。",
            fill="#6e7781",
            font=("Microsoft YaHei UI", 11),
        )

    def _close_signal_chart_window(self) -> None:
        if self._signal_chart_window is not None and self._signal_chart_window.winfo_exists():
            self._signal_chart_window.destroy()
        self._signal_chart_window = None
        self._signal_chart_canvas = None

    def _redraw_signal_chart(self, *, fast_mode: bool = False) -> None:
        if self._signal_chart_canvas is None or not self._signal_chart_canvas.winfo_exists():
            return
        if self._signal_chart_dataset is None:
            self._clear_signal_chart_canvas(self._signal_chart_canvas)
            return
        self._draw_signal_chart(self._signal_chart_canvas, self._signal_chart_dataset, fast_mode=fast_mode)

    def _draw_signal_chart(self, canvas: Canvas, dataset: _SignalChartDataset, *, fast_mode: bool = False) -> None:
        canvas.delete("all")
        candles = list(dataset.candles)
        if not candles:
            self._clear_signal_chart_canvas(canvas, message="没有拿到可绘制的 K 线数据。")
            return

        width = max(canvas.winfo_width(), 960)
        height = max(canvas.winfo_height(), 520)
        left = 56
        right = 20
        top = 20
        bottom = 34
        inner_width = width - left - right
        inner_height = height - top - bottom
        if inner_width <= 0 or inner_height <= 0:
            return

        start_index, visible_count = _normalize_signal_chart_viewport(
            self._signal_chart_view.start_index,
            self._signal_chart_view.visible_count,
            len(candles),
        )
        self._signal_chart_view.start_index = start_index
        self._signal_chart_view.visible_count = visible_count
        end_index = start_index + visible_count

        visible_candles = candles[start_index:end_index]
        visible_primary_ema = list(dataset.primary_ema_values[start_index:end_index])
        visible_secondary_ema = list(dataset.secondary_ema_values[start_index:end_index])
        candle_index_by_ts = {item.ts: index for index, item in enumerate(candles)}
        visible_history_events = [
            item
            for item in dataset.history_events
            if start_index <= candle_index_by_ts.get(item.candle_ts, -1) < end_index
        ]

        plotted_prices = [float(item.high) for item in visible_candles] + [float(item.low) for item in visible_candles]
        plotted_prices.extend(float(item) for item in visible_primary_ema)
        plotted_prices.extend(float(item) for item in visible_secondary_ema)
        plotted_prices.extend(float(item.trigger_price) for item in visible_history_events)
        price_max = Decimal(str(max(plotted_prices)))
        price_min = Decimal(str(min(plotted_prices)))
        if price_max == price_min:
            price_max += Decimal("1")
            price_min -= Decimal("1")

        candle_step = inner_width / max(visible_count, 1)
        self._signal_chart_render_state = _SignalChartRenderState(
            left=left,
            right=right,
            top=top,
            bottom=bottom,
            width=width,
            height=height,
            start_index=start_index,
            end_index=end_index,
            candle_step=candle_step,
            price_min=price_min,
            price_max=price_max,
        )

        def x_for(global_index: int) -> float:
            return left + ((global_index - start_index) * candle_step) + (candle_step / 2)

        def y_for(price: Decimal) -> float:
            ratio = (float(price_max) - float(price)) / max(float(price_max - price_min), 1e-12)
            return top + (ratio * inner_height)

        body_width = max(2.0, min(10.0, candle_step * 0.62))
        canvas.create_rectangle(left, top, width - right, height - bottom, outline="#d0d7de")
        canvas.create_text(
            left,
            top - 6,
            text=f"显示 {start_index + 1}-{min(end_index, len(candles))}/{len(candles)} | 滚轮缩放 | 左键拖动 | 悬浮十字光标",
            anchor="sw",
            fill="#57606a",
            font=("Microsoft YaHei UI", 9),
        )

        for price_value in _chart_price_axis_values(price_min, price_max, steps=2 if fast_mode else 4):
            y = y_for(price_value)
            canvas.create_line(left, y, width - right, y, fill="#eaeef2", dash=(2, 4))
            canvas.create_text(
                left - 8,
                y,
                text=_format_chart_axis_price(price_value),
                anchor="e",
                fill="#57606a",
                font=("Microsoft YaHei UI", 9),
            )

        for index, candle in enumerate(visible_candles, start=start_index):
            x = x_for(index)
            open_y = y_for(candle.open)
            close_y = y_for(candle.close)
            high_y = y_for(candle.high)
            low_y = y_for(candle.low)
            color = _signal_chart_candle_color(candle.open, candle.close)
            canvas.create_line(x, high_y, x, low_y, fill=color, width=1)
            body_top = min(open_y, close_y)
            body_bottom = max(open_y, close_y)
            if abs(body_bottom - body_top) < 1:
                body_bottom = body_top + 1
            canvas.create_rectangle(
                x - (body_width / 2),
                body_top,
                x + (body_width / 2),
                body_bottom,
                outline=color,
                fill=color,
            )

        if len(visible_primary_ema) >= 2:
            primary_points: list[float] = []
            for index, value in enumerate(visible_primary_ema, start=start_index):
                primary_points.extend((x_for(index), y_for(value)))
            canvas.create_line(*primary_points, fill="#ff8c00", width=2, smooth=not fast_mode)
        if len(visible_secondary_ema) >= 2:
            secondary_points: list[float] = []
            for index, value in enumerate(visible_secondary_ema, start=start_index):
                secondary_points.extend((x_for(index), y_for(value)))
            canvas.create_line(*secondary_points, fill="#0a7f5a", width=2, smooth=not fast_mode)

        selected_event = dataset.request.selected_event
        for event in visible_history_events:
            candle_index = candle_index_by_ts.get(event.candle_ts)
            if candle_index is None:
                continue
            candle = candles[candle_index]
            x = x_for(candle_index)
            is_selected = selected_event is not None and _monitor_events_match(event, selected_event)
            marker_color = "#f59e0b" if is_selected else ("#0969da" if event.direction == "long" else "#8250df")
            marker_size = 9 if is_selected else 6
            if event.direction == "long":
                marker_y = min(y_for(candle.low) + 18, height - bottom - 10)
                points = (
                    x,
                    marker_y - marker_size,
                    x - marker_size,
                    marker_y + marker_size,
                    x + marker_size,
                    marker_y + marker_size,
                )
            else:
                marker_y = max(y_for(candle.high) - 18, top + 10)
                points = (
                    x,
                    marker_y + marker_size,
                    x - marker_size,
                    marker_y - marker_size,
                    x + marker_size,
                    marker_y - marker_size,
                )
            canvas.create_polygon(*points, fill=marker_color, outline="")
            if is_selected:
                canvas.create_line(x, top, x, height - bottom, fill="#f59e0b", dash=(4, 4))
                canvas.create_text(
                    x + 8,
                    marker_y - 12 if event.direction == "short" else marker_y + 12,
                    text="当前",
                    anchor="w",
                    fill="#b54708",
                    font=("Microsoft YaHei UI", 9, "bold"),
                )

        for time_index in _chart_time_label_indices(start_index, end_index, target_labels=4 if fast_mode else 6):
            x = x_for(time_index)
            canvas.create_line(x, top, x, height - bottom, fill="#f3f4f6", dash=(2, 4))
            canvas.create_line(x, height - bottom, x, height - bottom + 5, fill="#8c959f")
            canvas.create_text(
                x,
                height - bottom + 8,
                text=_format_chart_timestamp(candles[time_index].ts),
                anchor="n",
                fill="#57606a",
                font=("Microsoft YaHei UI", 9),
            )

        legend_lines = ["K线"]
        if dataset.primary_ema_label:
            legend_lines.append(dataset.primary_ema_label)
        if dataset.secondary_ema_label:
            legend_lines.append(dataset.secondary_ema_label)
        if selected_event is not None:
            legend_lines.append("橙色=当前")
        legend_lines.append("蓝色=做多历史")
        legend_lines.append("紫色=做空历史")
        canvas.create_text(
            width - right,
            top + 10,
            text=" | ".join(legend_lines),
            anchor="ne",
            fill="#57606a",
            font=("Microsoft YaHei UI", 9, "bold"),
        )

        if fast_mode:
            canvas.delete("signal-chart-hover")
        else:
            self._render_signal_chart_hover(canvas)

    def _render_signal_chart_hover(self, canvas: Canvas) -> None:
        canvas.delete("signal-chart-hover")
        dataset = self._signal_chart_dataset
        state = self._signal_chart_render_state
        hover_index = self._signal_chart_hover_index
        hover_y = self._signal_chart_hover_y
        if dataset is None or state is None or hover_index is None or hover_y is None:
            return
        if not (state.start_index <= hover_index < state.end_index):
            return

        clamped_y = min(max(hover_y, state.top), state.height - state.bottom)
        x = state.left + ((hover_index - state.start_index) * state.candle_step) + (state.candle_step / 2)
        canvas.create_line(
            x,
            state.top,
            x,
            state.height - state.bottom,
            fill="#8b949e",
            dash=(4, 4),
            tags=("signal-chart-hover",),
        )
        canvas.create_line(
            state.left,
            clamped_y,
            state.width - state.right,
            clamped_y,
            fill="#8b949e",
            dash=(4, 4),
            tags=("signal-chart-hover",),
        )

        candle = dataset.candles[hover_index]
        hover_events = [item for item in dataset.history_events if item.candle_ts == candle.ts]
        lines = _format_signal_chart_hover_lines(
            candle=candle,
            primary_ema_label=dataset.primary_ema_label,
            primary_ema_value=dataset.primary_ema_values[hover_index] if dataset.primary_ema_values else None,
            secondary_ema_label=dataset.secondary_ema_label,
            secondary_ema_value=dataset.secondary_ema_values[hover_index] if dataset.secondary_ema_values else None,
            hover_events=hover_events,
            tick_size=dataset.tick_size,
        )

        text_anchor = "nw"
        text_x = state.left + 10
        if x < (state.width * 0.55):
            text_anchor = "ne"
            text_x = state.width - state.right - 10
        text_item = canvas.create_text(
            text_x,
            state.top + 10,
            text="\n".join(lines),
            anchor=text_anchor,
            fill="#24292f",
            font=("Microsoft YaHei UI", 9),
            tags=("signal-chart-hover",),
        )
        x1, y1, x2, y2 = canvas.bbox(text_item)
        background = canvas.create_rectangle(
            x1 - 8,
            y1 - 6,
            x2 + 8,
            y2 + 6,
            fill="#ffffff",
            outline="#d0d7de",
            tags=("signal-chart-hover",),
        )
        canvas.tag_lower(background, text_item)

        hover_price = _signal_chart_price_for_y(clamped_y, state)
        price_item = canvas.create_text(
            state.width - state.right + 4,
            clamped_y,
            text=_format_chart_axis_price(hover_price),
            anchor="w",
            fill="#24292f",
            font=("Microsoft YaHei UI", 9),
            tags=("signal-chart-hover",),
        )
        px1, py1, px2, py2 = canvas.bbox(price_item)
        price_bg = canvas.create_rectangle(
            px1 - 4,
            py1 - 3,
            px2 + 4,
            py2 + 3,
            fill="#ffffff",
            outline="#d0d7de",
            tags=("signal-chart-hover",),
        )
        canvas.tag_lower(price_bg, price_item)

    def _close_signal_chart_window(self) -> None:
        if self._signal_chart_window is not None and self._signal_chart_window.winfo_exists():
            self._signal_chart_window.destroy()
        self._signal_chart_window = None
        self._signal_chart_canvas = None
        self._signal_chart_render_state = None
        self._signal_chart_hover_index = None
        self._signal_chart_hover_y = None
        self._signal_chart_view = _SignalChartViewport()

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
        self._close_signal_chart_window()
        self.window.withdraw()
        self._logger("[信号监控] 窗口已隐藏，监控任务继续运行。")


def _build_signal_chart_ema_series(
    candles: list[Candle],
    config: SignalMonitorConfig,
    signal_type: str,
) -> tuple[str | None, list[Decimal], str | None, list[Decimal]]:
    closes = [item.close for item in candles]
    if not closes:
        return None, [], None, []
    if signal_type == "ema21_55_cross":
        return "EMA21", ema(closes, 21), "EMA55", ema(closes, 55)
    if signal_type in {"ema55_slope_turn", "ema55_breakout"}:
        return "EMA55", ema(closes, 55), None, []
    if signal_type == "candle_pattern":
        label = f"EMA{config.pattern_ema_period}"
        return label, ema(closes, config.pattern_ema_period), None, []
    return None, [], None, []


def _monitor_events_match(left: MonitorSignalEvent, right: MonitorSignalEvent) -> bool:
    return (
        left.symbol == right.symbol
        and left.signal_type == right.signal_type
        and left.direction == right.direction
        and left.candle_ts == right.candle_ts
    )


def _chart_price_axis_values(price_min: Decimal, price_max: Decimal, *, steps: int = 4) -> list[Decimal]:
    if steps <= 0:
        return [price_min, price_max]
    if price_max <= price_min:
        return [price_min]
    step = (price_max - price_min) / Decimal(steps)
    return [price_min + (step * Decimal(index)) for index in range(steps + 1)]


def _format_chart_axis_price(value: Decimal) -> str:
    absolute = abs(value)
    if absolute >= Decimal("1000"):
        places = 1
    elif absolute >= Decimal("1"):
        places = 2
    elif absolute >= Decimal("0.1"):
        places = 4
    else:
        places = 5
    return format_decimal_fixed(value, places)


def _signal_chart_candle_color(open_price: Decimal, close_price: Decimal) -> str:
    return "#1a7f37" if close_price >= open_price else "#d1242f"


def _format_chart_timestamp(ts: int) -> str:
    if ts >= 10**12:
        dt = datetime.fromtimestamp(ts / 1000)
        return dt.strftime("%m-%d %H:%M")
    if ts >= 10**9:
        dt = datetime.fromtimestamp(ts)
        return dt.strftime("%m-%d %H:%M")
    return str(ts)


def _chart_time_label_indices(start_index: int, end_index: int | None = None, *, target_labels: int = 6) -> list[int]:
    if end_index is None:
        end_index = start_index
        start_index = 0
    visible_count = max(end_index - start_index, 0)
    if visible_count <= 0:
        return []
    if visible_count <= target_labels:
        return list(range(start_index, end_index))
    span = visible_count - 1
    indices = {
        start_index + int(round(span * label_index / max(target_labels - 1, 1)))
        for label_index in range(target_labels)
    }
    return sorted(index for index in indices if start_index <= index < end_index)


def _normalize_signal_chart_viewport(
    start_index: int,
    visible_count: int | None,
    total_count: int,
    *,
    min_visible: int = 24,
) -> tuple[int, int]:
    if total_count <= 0:
        return 0, 0
    normalized_min_visible = max(1, min(min_visible, total_count))
    normalized_visible = total_count if visible_count is None else max(
        normalized_min_visible,
        min(visible_count, total_count),
    )
    max_start = max(total_count - normalized_visible, 0)
    normalized_start = max(0, min(start_index, max_start))
    return normalized_start, normalized_visible


def _zoom_signal_chart_viewport(
    *,
    start_index: int,
    visible_count: int | None,
    total_count: int,
    anchor_ratio: float,
    zoom_in: bool,
    min_visible: int = 24,
) -> tuple[int, int]:
    normalized_start, normalized_visible = _normalize_signal_chart_viewport(
        start_index,
        visible_count,
        total_count,
        min_visible=min_visible,
    )
    if total_count <= 0:
        return 0, 0
    factor = 0.8 if zoom_in else 1.25
    target_visible = int(round(normalized_visible * factor))
    min_count = max(1, min(min_visible, total_count))
    target_visible = max(min_count, min(target_visible, total_count))
    if target_visible == normalized_visible:
        return normalized_start, normalized_visible
    clamped_ratio = min(max(anchor_ratio, 0.0), 1.0)
    anchor_index = normalized_start + (normalized_visible * clamped_ratio)
    target_start = int(round(anchor_index - (target_visible * clamped_ratio)))
    return _normalize_signal_chart_viewport(
        target_start,
        target_visible,
        total_count,
        min_visible=min_visible,
    )


def _pan_signal_chart_viewport(
    start_index: int,
    visible_count: int | None,
    total_count: int,
    shift: int,
    *,
    min_visible: int = 24,
) -> int:
    normalized_start, normalized_visible = _normalize_signal_chart_viewport(
        start_index,
        visible_count,
        total_count,
        min_visible=min_visible,
    )
    target_start, _ = _normalize_signal_chart_viewport(
        normalized_start + shift,
        normalized_visible,
        total_count,
        min_visible=min_visible,
    )
    return target_start


def _signal_chart_hover_index_for_x(
    *,
    x: float,
    left: int,
    width: int,
    start_index: int,
    end_index: int,
    candle_step: float,
) -> int | None:
    if width <= 0 or candle_step <= 0:
        return None
    if x < left or x > left + width:
        return None
    relative = x - left - (candle_step / 2)
    offset = int(round(relative / candle_step))
    index = start_index + offset
    if index < start_index or index >= end_index:
        return None
    return index


def _signal_chart_price_for_y(y: float, state: _SignalChartRenderState) -> Decimal:
    panel_height = max(state.height - state.top - state.bottom, 1)
    ratio = min(max((y - state.top) / panel_height, 0.0), 1.0)
    price_span = state.price_max - state.price_min
    return state.price_max - (price_span * Decimal(str(ratio)))


def _format_signal_chart_hover_lines(
    *,
    candle: Candle,
    primary_ema_label: str | None,
    primary_ema_value: Decimal | None,
    secondary_ema_label: str | None,
    secondary_ema_value: Decimal | None,
    hover_events: list[MonitorSignalEvent],
    tick_size: Decimal | None,
) -> list[str]:
    lines = [
        f"时间: {_format_chart_timestamp(candle.ts)}",
        (
            "O/H/L/C: "
            f"{format_decimal_by_increment(candle.open, tick_size)} / "
            f"{format_decimal_by_increment(candle.high, tick_size)} / "
            f"{format_decimal_by_increment(candle.low, tick_size)} / "
            f"{format_decimal_by_increment(candle.close, tick_size)}"
        ),
    ]
    if primary_ema_label and primary_ema_value is not None:
        lines.append(f"{primary_ema_label}: {format_decimal_by_increment(primary_ema_value, tick_size)}")
    if secondary_ema_label and secondary_ema_value is not None:
        lines.append(f"{secondary_ema_label}: {format_decimal_by_increment(secondary_ema_value, tick_size)}")
    if hover_events:
        signal_text = ", ".join(
            f"{SIGNAL_TYPE_LABELS.get(item.signal_type, item.signal_type)}/{'做多' if item.direction == 'long' else '做空'}"
            for item in hover_events
        )
        lines.append(f"信号: {signal_text}")
    return lines


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
