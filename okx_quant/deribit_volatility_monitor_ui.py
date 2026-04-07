from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
import threading
import time
from tkinter import BooleanVar, Canvas, END, StringVar, Text, Toplevel
from tkinter import messagebox, ttk
from typing import Callable, Literal

from okx_quant.deribit_client import DeribitRestClient, DeribitVolatilityCandle
from okx_quant.deribit_volatility_monitor import (
    DERIBIT_VOL_RESOLUTION_SECONDS,
    DeribitVolatilityMonitor,
    VolatilityMonitorConfig,
    VolatilityMonitorRoundDiagnostic,
    VolatilitySignalEvent,
    VolatilitySignalType,
    VOL_DIRECTION_LABELS,
    VOL_SIGNAL_LABELS,
    evaluate_volatility_signal_history,
    format_volatility_diagnostic_round,
)
from okx_quant.deribit_volatility_ui import _normalize_chart_viewport, _pan_chart_viewport, _zoom_chart_viewport
from okx_quant.indicators import ema
from okx_quant.notifications import EmailNotifier
from okx_quant.pricing import format_decimal_fixed
from okx_quant.window_layout import apply_adaptive_window_geometry


NotifierFactory = Callable[[], EmailNotifier | None]
Logger = Callable[[str], None]
DEFAULT_VOL_SIGNAL_CHART_CANDLE_LIMIT = 1000
MAX_VOL_SIGNAL_CHART_CANDLE_LIMIT = 10000

DERIBIT_VOL_MONITOR_RESOLUTION_OPTIONS = {
    "1小时": "3600",
    "12小时": "43200",
    "1天": "1D",
}


@dataclass(frozen=True)
class _VolatilitySignalRowPayload:
    session_id: str
    resolution: str
    config: VolatilityMonitorConfig
    event: VolatilitySignalEvent


@dataclass(frozen=True)
class _VolatilitySignalDefinition:
    signal_type: VolatilitySignalType
    label: str


@dataclass(frozen=True)
class _VolatilityChartRequest:
    mode: Literal["event", "preview"]
    currency: str
    resolution: str
    config: VolatilityMonitorConfig
    signal_type: VolatilitySignalType
    candle_limit: int
    selected_event: VolatilitySignalEvent | None = None
    session_id: str | None = None


@dataclass(frozen=True)
class _VolatilityChartDataset:
    request: _VolatilityChartRequest
    candles: tuple[DeribitVolatilityCandle, ...]
    history_events: tuple[VolatilitySignalEvent, ...]
    decimal_places: int
    ema_label: str | None
    ema_values: tuple[Decimal, ...]


@dataclass
class _VolatilityChartViewport:
    start_index: int = 0
    visible_count: int | None = None
    pan_anchor_x: int | None = None
    pan_anchor_start: int = 0


@dataclass(frozen=True)
class _VolatilityChartRenderState:
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


VOL_SIGNAL_DEFINITIONS: tuple[_VolatilitySignalDefinition, ...] = (
    _VolatilitySignalDefinition("bearish_reversal_after_rally", VOL_SIGNAL_LABELS["bearish_reversal_after_rally"]),
    _VolatilitySignalDefinition("bullish_reversal_after_drop", VOL_SIGNAL_LABELS["bullish_reversal_after_drop"]),
    _VolatilitySignalDefinition("squeeze_breakout_up", VOL_SIGNAL_LABELS["squeeze_breakout_up"]),
    _VolatilitySignalDefinition("squeeze_breakout_down", VOL_SIGNAL_LABELS["squeeze_breakout_down"]),
    _VolatilitySignalDefinition("box_breakout_up", VOL_SIGNAL_LABELS["box_breakout_up"]),
    _VolatilitySignalDefinition("box_breakout_down", VOL_SIGNAL_LABELS["box_breakout_down"]),
    _VolatilitySignalDefinition("ema34_turn_up", VOL_SIGNAL_LABELS["ema34_turn_up"]),
    _VolatilitySignalDefinition("ema34_turn_down", VOL_SIGNAL_LABELS["ema34_turn_down"]),
)


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
        self._signal_row_payloads: dict[str, _VolatilitySignalRowPayload] = {}
        self._signal_chart_window: Toplevel | None = None
        self._signal_chart_canvas: Canvas | None = None
        self._signal_chart_dataset: _VolatilityChartDataset | None = None
        self._signal_chart_active_request: _VolatilityChartRequest | None = None
        self._signal_chart_request_id = 0
        self._signal_chart_summary = StringVar(value="选择最近触发信号，或使用右侧按钮预览当前波动率信号配置。")
        self._signal_chart_hint = StringVar(
            value="橙色标记为当前信号，蓝色为上行历史信号，紫色为下行历史信号；支持滚轮缩放、左键拖动和平移、悬浮十字光标。"
        )
        self._signal_chart_view = _VolatilityChartViewport()
        self._signal_chart_render_state: _VolatilityChartRenderState | None = None
        self._signal_chart_hover_index: int | None = None
        self._signal_chart_hover_y: float | None = None

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
        self.signal_chart_candle_limit = StringVar(value=str(DEFAULT_VOL_SIGNAL_CHART_CANDLE_LIMIT))
        self.signal_preview_currency = StringVar(value="BTC")
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
        self._content_pane: ttk.Panedwindow | None = None
        self._signal_enabled_vars: dict[VolatilitySignalType, BooleanVar] = {
            "bearish_reversal_after_rally": self.enable_bearish_reversal_after_rally,
            "bullish_reversal_after_drop": self.enable_bullish_reversal_after_drop,
            "squeeze_breakout_up": self.enable_squeeze_breakout_up,
            "squeeze_breakout_down": self.enable_squeeze_breakout_down,
            "box_breakout_up": self.enable_box_breakout_up,
            "box_breakout_down": self.enable_box_breakout_down,
            "ema34_turn_up": self.enable_ema34_turn_up,
            "ema34_turn_down": self.enable_ema34_turn_down,
        }

        self.tasks_tree: ttk.Treeview | None = None
        self.signal_tree: ttk.Treeview | None = None
        self.log_text: Text | None = None
        self.diagnostic_text: Text | None = None

        self._build_layout()
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
        signal_frame.columnconfigure(1, weight=0)

        for row_index, definition in enumerate(VOL_SIGNAL_DEFINITIONS):
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
        preview_box.grid(row=8, column=0, columnspan=2, sticky="ew", pady=(12, 0))
        preview_box.columnconfigure(1, weight=1)
        preview_box.columnconfigure(3, weight=1)
        ttk.Label(preview_box, text="预览标的").grid(row=0, column=0, sticky="w")
        ttk.Combobox(
            preview_box,
            textvariable=self.signal_preview_currency,
            values=("BTC", "ETH"),
            state="readonly",
        ).grid(row=0, column=1, sticky="ew", padx=(8, 12))
        ttk.Label(preview_box, text="K线数量").grid(row=0, column=2, sticky="w")
        ttk.Entry(preview_box, textvariable=self.signal_chart_candle_limit, width=10).grid(
            row=0, column=3, sticky="w", padx=(8, 0)
        )
        ttk.Label(
            preview_box,
            text="点击上方按钮，可按当前参数直接预览该波动率信号在历史K线中的触发位置。",
            foreground="#57606a",
            justify="left",
        ).grid(row=1, column=0, columnspan=4, sticky="w", pady=(8, 0))

        button_row = ttk.Frame(signal_frame)
        button_row.grid(row=9, column=0, columnspan=2, sticky="w", pady=(14, 0))
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
        ).grid(row=10, column=0, columnspan=2, sticky="w", pady=(12, 0))

        content_pane = ttk.Panedwindow(self.window, orient="vertical")
        content_pane.grid(row=2, column=0, sticky="nsew", padx=16, pady=(0, 16))
        self._content_pane = content_pane

        task_frame = ttk.LabelFrame(content_pane, text="监控任务", padding=12)
        task_frame.columnconfigure(0, weight=1)
        task_frame.rowconfigure(1, weight=1)

        task_header = ttk.Frame(task_frame)
        task_header.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 8))
        task_header.columnconfigure(0, weight=1)
        ttk.Label(
            task_header,
            text="每次点击“启动任务”，都会按当前参数创建一个独立波动率监控任务。",
        ).grid(row=0, column=0, sticky="w")
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
        content_pane.add(task_frame, weight=2)

        signal_frame = ttk.LabelFrame(content_pane, text="最近触发信号", padding=12)
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
        self.signal_tree.bind("<<TreeviewSelect>>", self._on_signal_selected)
        signal_scroll = ttk.Scrollbar(signal_frame, orient="vertical", command=self.signal_tree.yview)
        signal_scroll.grid(row=0, column=1, sticky="ns")
        self.signal_tree.configure(yscrollcommand=signal_scroll.set)
        content_pane.add(signal_frame, weight=3)

        log_frame = ttk.LabelFrame(content_pane, text="监控日志", padding=12)
        log_frame.columnconfigure(0, weight=1)
        log_frame.rowconfigure(0, weight=1)
        self.log_text = Text(log_frame, height=10, wrap="word", font=("Consolas", 10))
        self.log_text.grid(row=0, column=0, sticky="nsew")
        log_scroll = ttk.Scrollbar(log_frame, orient="vertical", command=self.log_text.yview)
        log_scroll.grid(row=0, column=1, sticky="ns")
        self.log_text.configure(yscrollcommand=log_scroll.set)
        content_pane.add(log_frame, weight=3)

        diagnostic_frame = ttk.LabelFrame(content_pane, text="实时诊断", padding=12)
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
        content_pane.add(diagnostic_frame, weight=2)

    def _apply_initial_layout_preferences(self) -> None:
        self.window.update_idletasks()
        if self._content_pane is None or len(self._content_pane.panes()) < 4:
            return

        window_height = max(self.window.winfo_height(), self.window.winfo_reqheight())
        content_height = max(self._content_pane.winfo_height(), window_height - 300)
        if window_height < 980:
            ratios = (0.14, 0.31, 0.24)
        else:
            ratios = (0.15, 0.33, 0.24)

        positions = [
            max(120, int(content_height * ratios[0])),
            max(340, int(content_height * (ratios[0] + ratios[1]))),
            max(540, int(content_height * (ratios[0] + ratios[1] + ratios[2]))),
        ]
        for index, position in enumerate(positions):
            try:
                self._content_pane.sashpos(index, position)
            except Exception:
                break

    def _collect_configured_currencies(self) -> list[str]:
        currencies: list[str] = []
        if self.enable_btc.get():
            currencies.append("BTC")
        if self.enable_eth.get():
            currencies.append("ETH")
        return currencies

    def _parse_signal_chart_candle_limit(self) -> int:
        value = self._parse_positive_int(self.signal_chart_candle_limit.get(), "信号图K线数量")
        if value > MAX_VOL_SIGNAL_CHART_CANDLE_LIMIT:
            raise ValueError(f"信号图K线数量最多支持 {MAX_VOL_SIGNAL_CHART_CANDLE_LIMIT} 根")
        return value

    def _build_signal_preview_request(self, signal_type: VolatilitySignalType) -> _VolatilityChartRequest:
        currency = self.signal_preview_currency.get().strip().upper()
        if currency not in {"BTC", "ETH"}:
            raise ValueError("预览标的仅支持 BTC 或 ETH")
        config = VolatilityMonitorConfig(
            currencies=(currency,),
            resolution=DERIBIT_VOL_MONITOR_RESOLUTION_OPTIONS[self.resolution_label.get()],
            buffer_seconds=float(self._defaults.buffer_seconds),
            enable_bearish_reversal_after_rally=signal_type == "bearish_reversal_after_rally",
            enable_bullish_reversal_after_drop=signal_type == "bullish_reversal_after_drop",
            enable_squeeze_breakout_up=signal_type == "squeeze_breakout_up",
            enable_squeeze_breakout_down=signal_type == "squeeze_breakout_down",
            enable_box_breakout_up=signal_type == "box_breakout_up",
            enable_box_breakout_down=signal_type == "box_breakout_down",
            enable_ema34_turn_up=signal_type == "ema34_turn_up",
            enable_ema34_turn_down=signal_type == "ema34_turn_down",
            ema_period=self._parse_positive_int(self.ema_period.get(), "EMA周期"),
            trend_streak_bars=self._parse_positive_int(self.trend_streak_bars.get(), "连续涨跌根数"),
            squeeze_bars=self._parse_positive_int(self.squeeze_bars.get(), "窄幅统计根数"),
            lookback_candles=self._parse_positive_int(self.lookback_candles.get(), "历史K线数量"),
            cumulative_change_threshold=self._parse_positive_decimal(self.cumulative_change_threshold.get(), "连续涨跌阈值"),
            reversal_body_multiplier=self._parse_positive_decimal(self.reversal_body_multiplier.get(), "反转实体倍数"),
            breakout_body_multiplier=self._parse_positive_decimal(self.breakout_body_multiplier.get(), "突破实体倍数"),
            squeeze_range_ratio=self._parse_ratio(self.squeeze_range_ratio.get(), "窄幅压缩比例"),
        )
        return _VolatilityChartRequest(
            mode="preview",
            currency=currency,
            resolution=config.resolution,
            config=config,
            signal_type=signal_type,
            candle_limit=self._parse_signal_chart_candle_limit(),
        )

    def _build_signal_event_request(self, payload: _VolatilitySignalRowPayload) -> _VolatilityChartRequest:
        return _VolatilityChartRequest(
            mode="event",
            currency=payload.event.currency,
            resolution=payload.resolution,
            config=payload.config,
            signal_type=payload.event.signal_type,
            candle_limit=self._parse_signal_chart_candle_limit(),
            selected_event=payload.event,
            session_id=payload.session_id,
        )

    def _open_signal_preview(self, signal_type: VolatilitySignalType) -> None:
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
        currencies = self._collect_configured_currencies()
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
        task = self._tasks.get(session_id)
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
        item_id = self.signal_tree.insert("", 0, values=values)
        if task is not None:
            self._signal_row_payloads[item_id] = _VolatilitySignalRowPayload(
                session_id=session_id,
                resolution=resolution,
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

    def _open_signal_chart(self, request: _VolatilityChartRequest) -> None:
        self._signal_chart_active_request = request
        self._signal_chart_dataset = None
        self._ensure_signal_chart_window()
        signal_label = VOL_SIGNAL_LABELS.get(request.signal_type, request.signal_type)
        resolution_label = _resolution_label(request.resolution)
        if request.mode == "preview":
            self._signal_chart_summary.set(
                f"正在加载 {request.currency} DVOL {resolution_label} 的 {request.candle_limit} 根 K 线，并预览 {signal_label} 的历史触发位置..."
            )
            self._signal_chart_hint.set(
                "蓝色为上行信号，紫色为下行信号；支持滚轮缩放、左键拖动和平移，修改 K 线数量后可刷新当前图表。"
            )
        else:
            self._signal_chart_summary.set(
                f"正在加载 {request.currency} DVOL {resolution_label} 的 {request.candle_limit} 根 K 线，并回放 {signal_label} 历史信号..."
            )
            self._signal_chart_hint.set(
                "橙色标记为当前信号，蓝色为上行历史信号，紫色为下行历史信号；支持滚轮缩放、左键拖动和平移、悬浮十字光标。"
            )
        if self._signal_chart_canvas is not None:
            self._clear_signal_chart_canvas(self._signal_chart_canvas, message="正在加载 K 线与历史信号...")
        self._signal_chart_request_id += 1
        request_id = self._signal_chart_request_id
        threading.Thread(
            target=self._load_signal_chart_dataset,
            args=(request_id, request),
            daemon=True,
            name="qqokx-deribit-signal-chart",
        ).start()

    def _ensure_signal_chart_window(self) -> None:
        if self._signal_chart_window is not None and self._signal_chart_window.winfo_exists():
            self._signal_chart_window.deiconify()
            self._signal_chart_window.lift()
            self._signal_chart_window.focus_force()
            return

        chart_window = Toplevel(self.window)
        chart_window.title("波动率信号 K 线回放")
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
                refreshed_request = _VolatilityChartRequest(
                    mode="event",
                    currency=request.currency,
                    resolution=request.resolution,
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
        self._signal_chart_view = _VolatilityChartViewport()
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
        next_start_index, next_visible_count = _zoom_chart_viewport(
            start_index=self._signal_chart_view.start_index,
            visible_count=self._signal_chart_view.visible_count,
            total_count=len(dataset.candles),
            anchor_ratio=anchor_ratio,
            zoom_in=delta > 0,
            min_visible=24,
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
        if dataset is None or self._signal_chart_view.pan_anchor_x is None:
            return
        width = max(canvas.winfo_width(), 960)
        left = 56
        right = 20
        inner_width = max(width - left - right, 1)
        _, visible_count = _normalize_chart_viewport(
            self._signal_chart_view.start_index,
            self._signal_chart_view.visible_count,
            len(dataset.candles),
            min_visible=24,
        )
        candle_step = inner_width / max(visible_count, 1)
        current_x = int(getattr(event, "x", self._signal_chart_view.pan_anchor_x))
        shift = int(round((self._signal_chart_view.pan_anchor_x - current_x) / max(candle_step, 1)))
        next_start_index = _pan_chart_viewport(
            self._signal_chart_view.pan_anchor_start,
            visible_count,
            len(dataset.candles),
            shift,
            min_visible=24,
        )
        if next_start_index == self._signal_chart_view.start_index:
            return
        self._signal_chart_view.start_index = next_start_index
        self._redraw_signal_chart(fast_mode=True)

    def _on_signal_chart_release(self, _canvas: Canvas) -> None:
        self._signal_chart_view.pan_anchor_x = None
        self._redraw_signal_chart()

    def _on_signal_chart_motion(self, canvas: Canvas, event: object) -> None:
        if self._signal_chart_dataset is None:
            return
        state = self._signal_chart_render_state
        if state is None:
            return
        index = _volatility_chart_hover_index_for_x(
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

    def _load_signal_chart_dataset(self, request_id: int, request: _VolatilityChartRequest) -> None:
        try:
            resolution_seconds = DERIBIT_VOL_RESOLUTION_SECONDS[request.resolution]
            end_ts = int(time.time() * 1000)
            span_seconds = resolution_seconds * max(request.candle_limit + 8, 16)
            start_ts = end_ts - (span_seconds * 1000)
            candles = self.client.get_volatility_index_candles(
                request.currency,
                request.resolution,
                start_ts=start_ts,
                end_ts=end_ts,
                max_records=request.candle_limit,
            )
            history_events = evaluate_volatility_signal_history(
                candles,
                request.currency,
                request.config,
                signal_type=request.signal_type,
            )
            ema_label, ema_values = _build_volatility_chart_ema_series(candles, request.config)
            dataset = _VolatilityChartDataset(
                request=request,
                candles=tuple(candles),
                history_events=tuple(history_events),
                decimal_places=_decimal_places_for_volatility_candles(candles),
                ema_label=ema_label,
                ema_values=tuple(ema_values),
            )
        except Exception as exc:
            self._queue_ui(lambda rid=request_id, message=str(exc): self._apply_signal_chart_error(rid, message))
            return

        self._queue_ui(lambda rid=request_id, data=dataset: self._apply_signal_chart_dataset(rid, data))

    def _apply_signal_chart_dataset(self, request_id: int, dataset: _VolatilityChartDataset) -> None:
        if request_id != self._signal_chart_request_id:
            return
        self._signal_chart_dataset = dataset
        self._signal_chart_active_request = dataset.request
        self._signal_chart_view = _VolatilityChartViewport()
        self._signal_chart_render_state = None
        self._signal_chart_hover_index = None
        self._signal_chart_hover_y = None
        request = dataset.request
        signal_label = VOL_SIGNAL_LABELS.get(request.signal_type, request.signal_type)
        history_count = len(dataset.history_events)
        resolution_label = _resolution_label(request.resolution)
        if request.selected_event is not None:
            selected_event = request.selected_event
            selected_found = any(_volatility_events_match(item, selected_event) for item in dataset.history_events)
            summary = (
                f"标的: {request.currency} DVOL | 周期: {resolution_label} | 规则: {signal_label} | "
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
                f"标的: {request.currency} DVOL | 周期: {resolution_label} | 规则: {signal_label} | "
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

    def _redraw_signal_chart(self, *, fast_mode: bool = False) -> None:
        if self._signal_chart_canvas is None or not self._signal_chart_canvas.winfo_exists():
            return
        if self._signal_chart_dataset is None:
            self._clear_signal_chart_canvas(self._signal_chart_canvas)
            return
        self._draw_signal_chart(self._signal_chart_canvas, self._signal_chart_dataset, fast_mode=fast_mode)

    def _draw_signal_chart(self, canvas: Canvas, dataset: _VolatilityChartDataset, *, fast_mode: bool = False) -> None:
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

        start_index, visible_count = _normalize_chart_viewport(
            self._signal_chart_view.start_index,
            self._signal_chart_view.visible_count,
            len(candles),
            min_visible=24,
        )
        self._signal_chart_view.start_index = start_index
        self._signal_chart_view.visible_count = visible_count
        end_index = start_index + visible_count

        visible_candles = candles[start_index:end_index]
        visible_ema_values = list(dataset.ema_values[start_index:end_index])
        candle_index_by_ts = {item.ts: index for index, item in enumerate(candles)}
        visible_history_events = [
            item
            for item in dataset.history_events
            if start_index <= candle_index_by_ts.get(item.candle_ts, -1) < end_index
        ]

        plotted_prices = [float(item.high) for item in visible_candles] + [float(item.low) for item in visible_candles]
        plotted_prices.extend(float(item) for item in visible_ema_values)
        plotted_prices.extend(float(item.trigger_value) for item in visible_history_events)
        price_max = Decimal(str(max(plotted_prices)))
        price_min = Decimal(str(min(plotted_prices)))
        if price_max == price_min:
            price_max += Decimal("1")
            price_min -= Decimal("1")

        candle_step = inner_width / max(visible_count, 1)
        self._signal_chart_render_state = _VolatilityChartRenderState(
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

        for price_value in _volatility_chart_price_axis_values(price_min, price_max, steps=2 if fast_mode else 4):
            y = y_for(price_value)
            canvas.create_line(left, y, width - right, y, fill="#eaeef2", dash=(2, 4))
            canvas.create_text(
                left - 8,
                y,
                text=_format_volatility_chart_price(price_value, dataset.decimal_places),
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
            color = "#1a7f37" if candle.close >= candle.open else "#d1242f"
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

        if len(visible_ema_values) >= 2:
            ema_points: list[float] = []
            for index, value in enumerate(visible_ema_values, start=start_index):
                ema_points.extend((x_for(index), y_for(value)))
            canvas.create_line(*ema_points, fill="#ff8c00", width=2, smooth=not fast_mode)

        selected_event = dataset.request.selected_event
        for event in visible_history_events:
            candle_index = candle_index_by_ts.get(event.candle_ts)
            if candle_index is None:
                continue
            candle = candles[candle_index]
            x = x_for(candle_index)
            is_selected = selected_event is not None and _volatility_events_match(event, selected_event)
            marker_color = "#f59e0b" if is_selected else ("#0969da" if event.direction == "up" else "#8250df")
            marker_size = 9 if is_selected else 6
            if event.direction == "up":
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
                    marker_y - 12 if event.direction == "down" else marker_y + 12,
                    text="当前",
                    anchor="w",
                    fill="#b54708",
                    font=("Microsoft YaHei UI", 9, "bold"),
                )

        for time_index in _volatility_chart_time_label_indices(start_index, end_index, target_labels=4 if fast_mode else 6):
            x = x_for(time_index)
            canvas.create_line(x, top, x, height - bottom, fill="#f3f4f6", dash=(2, 4))
            canvas.create_line(x, height - bottom, x, height - bottom + 5, fill="#8c959f")
            canvas.create_text(
                x,
                height - bottom + 8,
                text=_format_volatility_chart_timestamp(candles[time_index].ts),
                anchor="n",
                fill="#57606a",
                font=("Microsoft YaHei UI", 9),
            )

        legend_lines = ["K线"]
        if dataset.ema_label:
            legend_lines.append(dataset.ema_label)
        if selected_event is not None:
            legend_lines.append("橙色=当前")
        legend_lines.append("蓝色=上行历史")
        legend_lines.append("紫色=下行历史")
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
        lines = _format_volatility_chart_hover_lines(
            candle=candle,
            ema_label=dataset.ema_label,
            ema_value=dataset.ema_values[hover_index] if dataset.ema_values else None,
            hover_events=hover_events,
            decimal_places=dataset.decimal_places,
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

        hover_price = _volatility_chart_price_for_y(clamped_y, state)
        price_item = canvas.create_text(
            state.width - state.right + 4,
            clamped_y,
            text=_format_volatility_chart_price(hover_price, dataset.decimal_places),
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
            text=message or "选择最近触发信号，或点击右侧明显信号配置里的“查看K线”按钮进行预览。",
            fill="#6e7781",
            font=("Microsoft YaHei UI", 11),
        )

    def _close_signal_chart_window(self) -> None:
        if self._signal_chart_window is not None and self._signal_chart_window.winfo_exists():
            self._signal_chart_window.destroy()
        self._signal_chart_window = None
        self._signal_chart_canvas = None
        self._signal_chart_render_state = None
        self._signal_chart_hover_index = None
        self._signal_chart_hover_y = None
        self._signal_chart_view = _VolatilityChartViewport()

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
        self._close_signal_chart_window()
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


def _build_volatility_chart_ema_series(
    candles: list[DeribitVolatilityCandle],
    config: VolatilityMonitorConfig,
) -> tuple[str | None, list[Decimal]]:
    closes = [item.close for item in candles]
    if not closes:
        return None, []
    label = f"EMA{config.ema_period}"
    return label, ema(closes, config.ema_period)


def _volatility_events_match(left: VolatilitySignalEvent, right: VolatilitySignalEvent) -> bool:
    return (
        left.currency == right.currency
        and left.signal_type == right.signal_type
        and left.direction == right.direction
        and left.candle_ts == right.candle_ts
    )


def _decimal_places_for_volatility_candles(candles: list[DeribitVolatilityCandle]) -> int:
    places = 2
    for candle in candles[-40:]:
        for value in (candle.open, candle.high, candle.low, candle.close):
            places = max(places, max(-value.normalize().as_tuple().exponent, 0))
    return min(places, 6)


def _volatility_chart_price_axis_values(price_min: Decimal, price_max: Decimal, *, steps: int = 4) -> list[Decimal]:
    if steps <= 0:
        return [price_min, price_max]
    if price_max <= price_min:
        return [price_min]
    step = (price_max - price_min) / Decimal(steps)
    return [price_min + (step * Decimal(index)) for index in range(steps + 1)]


def _format_volatility_chart_price(value: Decimal, decimal_places: int) -> str:
    absolute = abs(value)
    if absolute >= Decimal("100"):
        places = max(2, min(decimal_places, 2))
    elif absolute >= Decimal("1"):
        places = max(2, min(decimal_places, 3))
    else:
        places = max(3, min(decimal_places, 5))
    return format_decimal_fixed(value, places)


def _format_volatility_chart_timestamp(ts: int) -> str:
    return datetime.fromtimestamp(ts / 1000).strftime("%m-%d %H:%M")


def _volatility_chart_time_label_indices(
    start_index: int,
    end_index: int | None = None,
    *,
    target_labels: int = 6,
) -> list[int]:
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


def _volatility_chart_hover_index_for_x(
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


def _volatility_chart_price_for_y(y: float, state: _VolatilityChartRenderState) -> Decimal:
    panel_height = max(state.height - state.top - state.bottom, 1)
    ratio = min(max((y - state.top) / panel_height, 0.0), 1.0)
    price_span = state.price_max - state.price_min
    return state.price_max - (price_span * Decimal(str(ratio)))


def _format_volatility_chart_hover_lines(
    *,
    candle: DeribitVolatilityCandle,
    ema_label: str | None,
    ema_value: Decimal | None,
    hover_events: list[VolatilitySignalEvent],
    decimal_places: int,
) -> list[str]:
    lines = [
        f"时间: {datetime.fromtimestamp(candle.ts / 1000).strftime('%Y-%m-%d %H:%M:%S')}",
        (
            "O/H/L/C: "
            f"{format_decimal_fixed(candle.open, decimal_places)} / "
            f"{format_decimal_fixed(candle.high, decimal_places)} / "
            f"{format_decimal_fixed(candle.low, decimal_places)} / "
            f"{format_decimal_fixed(candle.close, decimal_places)}"
        ),
    ]
    if ema_label and ema_value is not None:
        lines.append(f"{ema_label}: {format_decimal_fixed(ema_value, decimal_places)}")
    if hover_events:
        signal_text = ", ".join(
            f"{VOL_SIGNAL_LABELS[item.signal_type]}/{VOL_DIRECTION_LABELS[item.direction]}"
            for item in hover_events
        )
        lines.append(f"信号: {signal_text}")
    return lines
