from __future__ import annotations

import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from tkinter import BooleanVar, Canvas, END, StringVar, Toplevel, messagebox
from tkinter import ttk
from typing import Any

try:
    from zoneinfo import ZoneInfo

    _CHINA_TZ = ZoneInfo("Asia/Shanghai")
except Exception:  # pragma: no cover
    _CHINA_TZ = timezone(timedelta(hours=8))

from okx_quant.btc_market_analyzer import load_btc_market_email_notifier
from okx_quant.models import Candle
from okx_quant.notifications import EmailNotifier
from okx_quant.signal_replay_engine import SignalReplayConfig, SignalReplayDataset, SignalReplayPoint, build_signal_replay_dataset
from okx_quant.window_layout import apply_adaptive_window_geometry, apply_fill_window_geometry, apply_window_icon, toggle_toplevel_maximize

_AUTO_REFRESH_MS = 60_000


@dataclass(frozen=True)
class _ChartGeometry:
    left: float
    top: float
    right: float
    bottom: float
    start: int
    end: int
    slot: float
    value_min: float
    value_max: float


class SignalReplayMockWindow:
    """First functional version of the signal replay lab.

    The class name stays as-is because the main shell already imports this
    prototype entry. Internally this is now data-driven rather than static.
    """

    def __init__(self, parent: Toplevel, *, client: Any | None = None, logger=None) -> None:
        self._client = client
        self._logger = logger or (lambda _message: None)
        self._load_token = 0
        self._redraw_job: str | None = None
        self._auto_refresh_job: str | None = None
        self._dataset: SignalReplayDataset | None = None
        self._notifier: EmailNotifier | None = load_btc_market_email_notifier()
        self._emailed_signal_keys: set[str] = set()
        self._is_loading = False
        self._visible_start = 0
        self._visible_count = 180
        self._hover_index: int | None = None
        self._selected_index: int | None = None
        self._pan_anchor_x: int | None = None
        self._pan_anchor_start = 0
        self._main_geometry: _ChartGeometry | None = None
        self._signal_by_tree_id: dict[str, SignalReplayPoint] = {}

        self.window = Toplevel(parent)
        self.window.title("信号复盘实验室")
        self.window.protocol("WM_DELETE_WINDOW", self.destroy)
        apply_window_icon(self.window)
        apply_adaptive_window_geometry(
            self.window,
            width_ratio=0.92,
            height_ratio=0.9,
            min_width=1380,
            min_height=880,
            max_width=1860,
            max_height=1220,
        )
        if not toggle_toplevel_maximize(self.window):
            apply_fill_window_geometry(self.window, min_width=1380, min_height=880)

        self.symbol = StringVar(value="BTC-USDT-SWAP")
        self.bar = StringVar(value="1H")
        self.lookback = StringVar(value="720")
        self.bias_min_pct = StringVar(value="-1.5")
        self.bias_max_pct = StringVar(value="2.8")
        self.near_ema_max_pct = StringVar(value="0.6")
        self.volume_multiplier = StringVar(value="1.2")
        self.atr_min_pct = StringVar(value="0.8")
        self.enable_trend_filter = BooleanVar(value=True)
        self.enable_pullback_trigger = BooleanVar(value=True)
        self.enable_macd_filter = BooleanVar(value=True)
        self.enable_volume_filter = BooleanVar(value=True)
        self.enable_bias_filter = BooleanVar(value=True)
        self.enable_near_ema_filter = BooleanVar(value=False)
        self.enable_atr_filter = BooleanVar(value=False)
        self.include_long = BooleanVar(value=True)
        self.include_short = BooleanVar(value=True)
        self.confirmed_only = BooleanVar(value=True)
        self.enable_pattern_signals = BooleanVar(value=True)
        self.auto_refresh_enabled = BooleanVar(value=True)
        self.latest_signal_email_enabled = BooleanVar(value=True)
        self.enable_big_bullish = BooleanVar(value=True)
        self.enable_big_bearish = BooleanVar(value=True)
        self.enable_long_upper_shadow = BooleanVar(value=True)
        self.enable_long_lower_shadow = BooleanVar(value=True)
        self.enable_false_breakdown = BooleanVar(value=True)
        self.enable_false_breakout = BooleanVar(value=True)
        self.enable_inside_bar = BooleanVar(value=True)
        self.enable_top_fractal = BooleanVar(value=True)
        self.enable_bottom_fractal = BooleanVar(value=True)
        self.enable_large_move_gate = BooleanVar(value=True)
        self.enable_large_move_mean = BooleanVar(value=True)
        self.enable_large_move_atr = BooleanVar(value=True)
        self.enable_large_move_body_ratio = BooleanVar(value=False)
        self.enable_large_move_fixed = BooleanVar(value=False)
        self.mean_body_period = StringVar(value="20")
        self.mean_body_multiplier = StringVar(value="1.8")
        self.large_move_atr_period = StringVar(value="14")
        self.large_move_atr_multiplier = StringVar(value="1.2")
        self.body_ratio_threshold = StringVar(value="0.6")
        self.fixed_body_threshold = StringVar(value="0")
        self.fractal_trend_lookback = StringVar(value="5")
        self.fractal_trend_min_bars = StringVar(value="3")
        self.false_break_reference_lookback = StringVar(value="6")
        self.false_break_min_pct = StringVar(value="0.05")
        self.false_break_atr_multiplier = StringVar(value="0.1")
        self.false_break_reclaim_position = StringVar(value="0.6")

        self.status_text = StringVar(value="等待加载 1H K线。")
        self.summary_text = StringVar(value="BTC / 1H / EMA21-55 / MACD / 乖离率 / 成交量")
        self.signal_set_text = StringVar(value="趋势过滤 + 回踩确认 + MACD 共振 + 量能放大")
        self.stats_text = StringVar(value="-")
        self.selected_signal_text = StringVar(value="尚未选择信号。")
        self.card_title_vars = [StringVar(value="-") for _ in range(3)]
        self.card_detail_vars = [StringVar(value="-") for _ in range(3)]

        self._build_layout()
        self._bind_events()
        self._apply_empty_state()
        if self._client is not None:
            self.window.after(100, self.load_replay_data)
        self._schedule_auto_refresh()

    def show(self) -> None:
        self.window.deiconify()
        self.window.lift()
        self.window.focus_force()

    def destroy(self) -> None:
        if self._redraw_job is not None:
            try:
                self.window.after_cancel(self._redraw_job)
            except Exception:
                pass
            self._redraw_job = None
        if self._auto_refresh_job is not None:
            try:
                self.window.after_cancel(self._auto_refresh_job)
            except Exception:
                pass
            self._auto_refresh_job = None
        if self.window.winfo_exists():
            self.window.destroy()

    def _build_layout(self) -> None:
        self.window.columnconfigure(0, weight=1)
        self.window.rowconfigure(2, weight=1)

        header = ttk.Frame(self.window, padding=(18, 16, 18, 8))
        header.grid(row=0, column=0, sticky="ew")
        header.columnconfigure(0, weight=1)
        ttk.Label(header, text="信号复盘实验室", font=("Microsoft YaHei UI", 18, "bold")).grid(row=0, column=0, sticky="w")
        ttk.Label(header, textvariable=self.status_text, justify="right").grid(row=0, column=1, sticky="e")
        ttk.Label(header, textvariable=self.summary_text, foreground="#4f6b8a").grid(
            row=1, column=0, columnspan=2, sticky="w", pady=(6, 0)
        )

        self._build_filter_panel()

        body = ttk.Panedwindow(self.window, orient="horizontal")
        body.grid(row=2, column=0, sticky="nsew", padx=18, pady=(0, 16))
        left = ttk.Frame(body, padding=(0, 0, 10, 0))
        center = ttk.Frame(body, padding=(4, 0))
        right = ttk.Frame(body, padding=(10, 0, 0, 0))
        body.add(left, weight=15)
        body.add(center, weight=63)
        body.add(right, weight=22)

        self._build_left_panel(left)
        self._build_center_panel(center)
        self._build_right_panel(right)

    def _build_filter_panel(self) -> None:
        filters = ttk.LabelFrame(self.window, text="条件组合区", padding=14)
        filters.grid(row=1, column=0, sticky="ew", padx=18, pady=(0, 10))
        for col in range(11):
            filters.columnconfigure(col, weight=1)

        fields = (
            ("标的", self.symbol, 15),
            ("周期", self.bar, 6),
            ("K线数量", self.lookback, 7),
            ("乖离下限%", self.bias_min_pct, 8),
            ("乖离上限%", self.bias_max_pct, 8),
            ("均线附近%", self.near_ema_max_pct, 8),
            ("量能倍数", self.volume_multiplier, 8),
            ("ATR下限%", self.atr_min_pct, 8),
        )
        for index, (label, variable, width) in enumerate(fields):
            ttk.Label(filters, text=label).grid(row=0, column=index, sticky="w", pady=(0, 4), padx=(0, 8))
            entry = ttk.Entry(filters, textvariable=variable, width=width)
            entry.grid(row=1, column=index, sticky="ew", padx=(0, 8))
            if variable is self.bar:
                entry.configure(state="readonly")

        ttk.Button(filters, text="加载 / 刷新", command=self.load_replay_data).grid(row=1, column=8, sticky="ew", padx=(6, 0))
        ttk.Button(filters, text="应用条件", command=self.rebuild_signals).grid(row=1, column=9, sticky="ew", padx=(6, 0))
        ttk.Button(filters, text="重置视图", command=self.reset_view).grid(row=1, column=10, sticky="ew", padx=(6, 0))

        option_row = ttk.Frame(filters)
        option_row.grid(row=2, column=0, columnspan=11, sticky="ew", pady=(10, 0))
        options = (
            ("趋势过滤", self.enable_trend_filter),
            ("回踩确认", self.enable_pullback_trigger),
            ("MACD 共振", self.enable_macd_filter),
            ("量能放大", self.enable_volume_filter),
            ("乖离约束", self.enable_bias_filter),
            ("均线附近", self.enable_near_ema_filter),
            ("ATR 过滤", self.enable_atr_filter),
            ("显示多头", self.include_long),
            ("显示空头", self.include_short),
            ("仅已确认K线", self.confirmed_only),
        )
        for index, (label, variable) in enumerate(options):
            ttk.Checkbutton(option_row, text=label, variable=variable, command=self.rebuild_signals).grid(
                row=0, column=index, sticky="w", padx=(0, 14)
            )
        base_column = len(options)
        ttk.Checkbutton(option_row, text="自动刷新", variable=self.auto_refresh_enabled, command=self._schedule_auto_refresh).grid(
            row=0, column=base_column, sticky="w", padx=(0, 14)
        )
        ttk.Checkbutton(option_row, text="最新K线邮件", variable=self.latest_signal_email_enabled).grid(
            row=0, column=base_column + 1, sticky="w", padx=(0, 14)
        )

        pattern_row = ttk.Frame(filters)
        pattern_row.grid(row=3, column=0, columnspan=11, sticky="ew", pady=(10, 0))
        ttk.Label(pattern_row, text="K线信号").grid(row=0, column=0, sticky="w", padx=(0, 8))
        pattern_options = (
            ("大阳", self.enable_big_bullish),
            ("大阴", self.enable_big_bearish),
            ("长上影", self.enable_long_upper_shadow),
            ("长下影", self.enable_long_lower_shadow),
            ("假跌破", self.enable_false_breakdown),
            ("假突破", self.enable_false_breakout),
            ("孕育", self.enable_inside_bar),
            ("顶分型", self.enable_top_fractal),
            ("底分型", self.enable_bottom_fractal),
        )
        ttk.Checkbutton(pattern_row, text="启用形态", variable=self.enable_pattern_signals, command=self.rebuild_signals).grid(
            row=0, column=1, sticky="w", padx=(0, 12)
        )
        for index, (label, variable) in enumerate(pattern_options, start=2):
            ttk.Checkbutton(pattern_row, text=label, variable=variable, command=self.rebuild_signals).grid(
                row=0, column=index, sticky="w", padx=(0, 12)
            )

        large_row = ttk.Frame(filters)
        large_row.grid(row=4, column=0, columnspan=11, sticky="ew", pady=(8, 0))
        ttk.Label(large_row, text="大波动").grid(row=0, column=0, sticky="w", padx=(0, 8))
        large_options = (
            ("门控", self.enable_large_move_gate),
            ("均值", self.enable_large_move_mean),
            ("ATR", self.enable_large_move_atr),
            ("实体占比", self.enable_large_move_body_ratio),
            ("固定阈值", self.enable_large_move_fixed),
        )
        for index, (label, variable) in enumerate(large_options, start=1):
            ttk.Checkbutton(large_row, text=label, variable=variable, command=self.rebuild_signals).grid(
                row=0, column=index, sticky="w", padx=(0, 10)
            )
        large_fields = (
            ("均值N", self.mean_body_period, 5),
            ("倍数", self.mean_body_multiplier, 5),
            ("ATR N", self.large_move_atr_period, 5),
            ("ATR倍数", self.large_move_atr_multiplier, 5),
            ("占比", self.body_ratio_threshold, 5),
            ("固定", self.fixed_body_threshold, 7),
            ("分型前N", self.fractal_trend_lookback, 5),
            ("同向数", self.fractal_trend_min_bars, 5),
            ("假破N", self.false_break_reference_lookback, 5),
            ("刺破%", self.false_break_min_pct, 5),
            ("刺破ATR", self.false_break_atr_multiplier, 5),
            ("收回位", self.false_break_reclaim_position, 5),
        )
        start_col = len(large_options) + 1
        for offset, (label, variable, width) in enumerate(large_fields):
            col = start_col + (offset * 2)
            ttk.Label(large_row, text=label).grid(row=0, column=col, sticky="w", padx=(0, 4))
            ttk.Entry(large_row, textvariable=variable, width=width).grid(row=0, column=col + 1, sticky="w", padx=(0, 8))

    def _build_left_panel(self, parent: ttk.Frame) -> None:
        parent.columnconfigure(0, weight=1)
        parent.rowconfigure(3, weight=1)
        info = ttk.LabelFrame(parent, text="策略结构", padding=12)
        info.grid(row=0, column=0, sticky="ew")
        ttk.Label(info, textvariable=self.signal_set_text, wraplength=220, justify="left").grid(row=0, column=0, sticky="w")
        ttk.Label(
            info,
            text=(
                "目的：验证某根 1H K线在当下是否值得作为开仓机会。\n\n"
                "趋势层：EMA21 / EMA55 位置和斜率\n"
                "触发层：回踩站回、反抽失败、MACD 金叉死叉\n"
                "过滤层：乖离率、均线附近、量能、ATR 波动"
            ),
            wraplength=220,
            justify="left",
        ).grid(row=1, column=0, sticky="w", pady=(10, 0))

        stats = ttk.LabelFrame(parent, text="信号概述", padding=12)
        stats.grid(row=1, column=0, sticky="ew", pady=(10, 0))
        ttk.Label(stats, textvariable=self.stats_text, wraplength=220, justify="left").grid(row=0, column=0, sticky="w")

        legend = ttk.LabelFrame(parent, text="图上元素", padding=12)
        legend.grid(row=2, column=0, sticky="ew", pady=(10, 0))
        for idx, (label, color) in enumerate(
            (
                ("K线：价格", "#d8dee9"),
                ("橙线：EMA21", "#f59e0b"),
                ("蓝线：EMA55", "#38bdf8"),
                ("绿三角：开多候选", "#22c55e"),
                ("红三角：开空候选", "#ef4444"),
                ("紫线：乖离率", "#c084fc"),
            )
        ):
            row = ttk.Frame(legend)
            row.grid(row=idx, column=0, sticky="ew", pady=2)
            swatch = Canvas(row, width=18, height=12, highlightthickness=0, background="#ffffff")
            swatch.grid(row=0, column=0, sticky="w", padx=(0, 8))
            swatch.create_rectangle(1, 2, 17, 10, outline=color, fill=color)
            ttk.Label(row, text=label).grid(row=0, column=1, sticky="w")

        notes = ttk.LabelFrame(parent, text="图表交互", padding=12)
        notes.grid(row=3, column=0, sticky="nsew", pady=(10, 0))
        ttk.Label(
            notes,
            text="滚轮缩放，左键拖动平移，移动鼠标看十字线，点击 K线或右侧样本可锁定当前复盘点。",
            wraplength=220,
            justify="left",
        ).grid(row=0, column=0, sticky="w")

    def _build_center_panel(self, parent: ttk.Frame) -> None:
        parent.columnconfigure(0, weight=1)
        parent.rowconfigure(1, weight=1)
        header = ttk.Frame(parent)
        header.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        header.columnconfigure(0, weight=1)
        ttk.Label(header, text="复盘图表区", font=("Microsoft YaHei UI", 12, "bold")).grid(row=0, column=0, sticky="w")
        ttk.Label(header, text="主图区优先，承接缩放、拖动、点选复盘点。", foreground="#5d6876").grid(row=0, column=1, sticky="e")

        chart_frame = ttk.Frame(parent)
        chart_frame.grid(row=1, column=0, sticky="nsew")
        chart_frame.columnconfigure(0, weight=1)
        chart_frame.rowconfigure(0, weight=8)
        chart_frame.rowconfigure(1, weight=2)
        chart_frame.rowconfigure(2, weight=2)
        chart_frame.rowconfigure(3, weight=2)

        self.main_chart = Canvas(chart_frame, background="#0f172a", highlightthickness=0, height=480, cursor="crosshair")
        self.main_chart.grid(row=0, column=0, sticky="nsew")
        self.macd_chart = Canvas(chart_frame, background="#111827", highlightthickness=0, height=120, cursor="crosshair")
        self.macd_chart.grid(row=1, column=0, sticky="nsew", pady=(8, 0))
        self.bias_chart = Canvas(chart_frame, background="#111827", highlightthickness=0, height=110, cursor="crosshair")
        self.bias_chart.grid(row=2, column=0, sticky="nsew", pady=(8, 0))
        self.volume_chart = Canvas(chart_frame, background="#111827", highlightthickness=0, height=110, cursor="crosshair")
        self.volume_chart.grid(row=3, column=0, sticky="nsew", pady=(8, 0))

    def _build_right_panel(self, parent: ttk.Frame) -> None:
        parent.columnconfigure(0, weight=1)
        parent.rowconfigure(2, weight=1)
        cards = ttk.LabelFrame(parent, text="当前 K线信号卡", padding=12)
        cards.grid(row=0, column=0, sticky="ew")
        for idx in range(3):
            frame = ttk.Frame(cards, padding=(0, 4))
            frame.grid(row=idx, column=0, sticky="ew")
            frame.columnconfigure(0, weight=1)
            ttk.Label(frame, textvariable=self.card_title_vars[idx], font=("Microsoft YaHei UI", 10, "bold")).grid(row=0, column=0, sticky="w")
            ttk.Label(frame, textvariable=self.card_detail_vars[idx], wraplength=310, foreground="#5d6876", justify="left").grid(
                row=1, column=0, sticky="w"
            )

        selection = ttk.LabelFrame(parent, text="选中复盘点", padding=12)
        selection.grid(row=1, column=0, sticky="ew", pady=(10, 0))
        ttk.Label(selection, textvariable=self.selected_signal_text, wraplength=320, justify="left").grid(row=0, column=0, sticky="w")

        samples = ttk.LabelFrame(parent, text="样本结果区", padding=12)
        samples.grid(row=2, column=0, sticky="nsew", pady=(10, 0))
        samples.columnconfigure(0, weight=1)
        samples.rowconfigure(0, weight=1)
        self.sample_tree = ttk.Treeview(
            samples,
            columns=("time", "pattern", "bars", "side", "score", "ret24"),
            show="headings",
            height=16,
        )
        self.sample_tree.grid(row=0, column=0, sticky="nsew")
        scrollbar = ttk.Scrollbar(samples, orient="vertical", command=self.sample_tree.yview)
        scrollbar.grid(row=0, column=1, sticky="ns")
        self.sample_tree.configure(yscrollcommand=scrollbar.set)
        for key, label, width in (
            ("time", "时间", 94),
            ("pattern", "信号", 86),
            ("bars", "K", 34),
            ("side", "方向", 54),
            ("score", "评分", 54),
            ("ret24", "24H", 68),
        ):
            self.sample_tree.heading(key, text=label)
            self.sample_tree.column(key, width=width, anchor="center")

    def _bind_events(self) -> None:
        for canvas in (self.main_chart, self.macd_chart, self.bias_chart, self.volume_chart):
            canvas.bind("<Configure>", lambda _event: self._schedule_redraw(), add="+")
            canvas.bind("<MouseWheel>", self._on_mousewheel, add="+")
            canvas.bind("<Motion>", self._on_mouse_motion, add="+")
            canvas.bind("<Leave>", self._on_mouse_leave, add="+")
            canvas.bind("<Button-1>", self._on_mouse_press, add="+")
            canvas.bind("<B1-Motion>", self._on_mouse_drag, add="+")
            canvas.bind("<ButtonRelease-1>", self._on_mouse_release, add="+")
        self.sample_tree.bind("<<TreeviewSelect>>", self._on_sample_selected, add="+")

    def _load_replay_data(self, *, trigger_email: bool) -> None:
        if self._client is None:
            self.status_text.set("当前没有行情客户端，无法加载真实 K线。")
            return
        if self._is_loading:
            return
        symbol = self.symbol.get().strip().upper() or "BTC-USDT-SWAP"
        try:
            limit = max(120, min(int(self.lookback.get().strip() or "720"), 5000))
        except ValueError:
            messagebox.showerror("参数错误", "K线数量必须是整数。", parent=self.window)
            return
        self.lookback.set(str(limit))
        self._load_token += 1
        token = self._load_token
        previous_latest_ts = self._dataset.candles[-1].ts if self._dataset is not None and self._dataset.candles else None
        self._is_loading = True
        self.status_text.set(f"正在加载 {symbol} 1H K线...")

        def worker() -> None:
            try:
                candles = self._client.get_candles_history(symbol, "1H", limit=limit)
            except Exception as exc:
                self.window.after(0, lambda error=exc: self._apply_load_error_v2(token, error))
                return
            self.window.after(
                0,
                lambda result=candles, last_ts=previous_latest_ts, should_email=trigger_email: self._apply_loaded_candles_v2(
                    token,
                    result,
                    previous_latest_ts=last_ts,
                    trigger_email=should_email,
                ),
            )

        threading.Thread(target=worker, daemon=True, name="signal-replay-load").start()

    def load_replay_data(self) -> None:
        self._load_replay_data(trigger_email=False)
        return
        if self._client is None:
            self.status_text.set("当前没有行情客户端，无法加载真实 K线。")
            return
        symbol = self.symbol.get().strip().upper() or "BTC-USDT-SWAP"
        try:
            limit = max(120, min(int(self.lookback.get().strip() or "720"), 5000))
        except ValueError:
            messagebox.showerror("参数错误", "K线数量必须是整数。", parent=self.window)
            return
        self.lookback.set(str(limit))
        self._load_token += 1
        token = self._load_token
        self.status_text.set(f"正在加载 {symbol} 1H K线...")

        def worker() -> None:
            try:
                candles = self._client.get_candles_history(symbol, "1H", limit=limit)
            except Exception as exc:
                self.window.after(0, lambda error=exc: self._apply_load_error(token, error))
                return
            self.window.after(0, lambda result=candles: self._apply_loaded_candles(token, result))

        threading.Thread(target=worker, daemon=True, name="signal-replay-load").start()

    def _apply_loaded_candles_v2(
        self,
        token: int,
        candles: list[Candle],
        *,
        previous_latest_ts: int | None,
        trigger_email: bool,
    ) -> None:
        if token != self._load_token:
            return
        self._is_loading = False
        try:
            self._dataset = build_signal_replay_dataset(candles, config=self._current_config())
        except Exception as exc:
            self._apply_load_error_v2(token, exc)
            return
        latest_ts = self._dataset.candles[-1].ts if self._dataset.candles else None
        self.reset_view()
        count = len(self._dataset.candles)
        self.summary_text.set(f"{self.symbol.get().strip().upper()} / 1H / K线 {count} / 信号 {len(self._dataset.signals)}")
        self.status_text.set(f"已加载 {count} 根 1H K线，生成 {len(self._dataset.signals)} 个候选信号。")
        self._refresh_side_panels()
        self._schedule_redraw()
        if trigger_email and latest_ts is not None and previous_latest_ts is not None and latest_ts > previous_latest_ts:
            self._handle_latest_candle_refresh()
        self._logger(f"[信号复盘实验室] 已加载 {self.symbol.get().strip().upper()} 1H K线 {count} 根")

    def _apply_load_error_v2(self, token: int, exc: Exception) -> None:
        if token != self._load_token:
            return
        self._is_loading = False
        self.status_text.set(f"加载失败：{exc}")
        messagebox.showerror("加载失败", f"加载信号复盘数据时出错：\n{exc}", parent=self.window)

    def _apply_loaded_candles(
        self,
        token: int,
        candles: list[Candle],
        *,
        previous_latest_ts: int | None = None,
        trigger_email: bool = False,
    ) -> None:
        if token != self._load_token:
            return
        self._is_loading = False
        try:
            self._dataset = build_signal_replay_dataset(candles, config=self._current_config())
        except Exception as exc:
            self._apply_load_error(token, exc)
            return
        latest_ts = self._dataset.candles[-1].ts if self._dataset.candles else None
        self.reset_view()
        count = len(self._dataset.candles)
        self.summary_text.set(f"{self.symbol.get().strip().upper()} / 1H / K线 {count} / 信号 {len(self._dataset.signals)}")
        self.status_text.set(f"已加载 {count} 根 1H K线，生成 {len(self._dataset.signals)} 个候选信号。")
        self._refresh_side_panels()
        self._schedule_redraw()
        self._logger(f"[信号复盘实验室] 已加载 {self.symbol.get().strip().upper()} 1H K线 {count} 根")

    def _apply_load_error(self, token: int, exc: Exception) -> None:
        if token != self._load_token:
            return
        self.status_text.set(f"加载失败：{exc}")
        messagebox.showerror("加载失败", f"加载信号复盘数据时出错：\n{exc}", parent=self.window)

    def rebuild_signals(self) -> None:
        if self._dataset is None:
            return
        try:
            self._dataset = build_signal_replay_dataset(list(self._dataset.candles), config=self._current_config())
        except Exception as exc:
            messagebox.showerror("参数错误", f"重新计算信号失败：\n{exc}", parent=self.window)
            return
        self.status_text.set(f"已按当前条件刷新，候选信号 {len(self._dataset.signals)} 个。")
        self.summary_text.set(f"{self.symbol.get().strip().upper()} / 1H / K线 {len(self._dataset.candles)} / 信号 {len(self._dataset.signals)}")
        self._refresh_side_panels()
        self._schedule_redraw()

    def reset_view(self) -> None:
        if self._dataset is None:
            return
        total = len(self._dataset.candles)
        self._visible_count = min(max(self._visible_count, 80), max(total, 80))
        self._visible_start = max(0, total - self._visible_count)
        self._hover_index = None
        self._selected_index = None
        self._schedule_redraw()

    def _current_config(self) -> SignalReplayConfig:
        return SignalReplayConfig(
            bias_min_pct=self._parse_decimal(self.bias_min_pct.get(), Decimal("-1.5"), "乖离下限"),
            bias_max_pct=self._parse_decimal(self.bias_max_pct.get(), Decimal("2.8"), "乖离上限"),
            near_ema_max_pct=self._parse_decimal(self.near_ema_max_pct.get(), Decimal("0.6"), "均线附近"),
            volume_multiplier=self._parse_decimal(self.volume_multiplier.get(), Decimal("1.2"), "量能倍数"),
            atr_min_pct=self._parse_decimal(self.atr_min_pct.get(), Decimal("0.8"), "ATR下限"),
            enable_trend_filter=self.enable_trend_filter.get(),
            enable_pullback_trigger=self.enable_pullback_trigger.get(),
            enable_macd_filter=self.enable_macd_filter.get(),
            enable_volume_filter=self.enable_volume_filter.get(),
            enable_bias_filter=self.enable_bias_filter.get(),
            enable_near_ema_filter=self.enable_near_ema_filter.get(),
            enable_atr_filter=self.enable_atr_filter.get(),
            include_long=self.include_long.get(),
            include_short=self.include_short.get(),
            confirmed_only=self.confirmed_only.get(),
            enable_pattern_signals=self.enable_pattern_signals.get(),
            enable_big_bullish=self.enable_big_bullish.get(),
            enable_big_bearish=self.enable_big_bearish.get(),
            enable_long_upper_shadow=self.enable_long_upper_shadow.get(),
            enable_long_lower_shadow=self.enable_long_lower_shadow.get(),
            enable_false_breakdown=self.enable_false_breakdown.get(),
            enable_false_breakout=self.enable_false_breakout.get(),
            enable_inside_bar=self.enable_inside_bar.get(),
            enable_top_fractal=self.enable_top_fractal.get(),
            enable_bottom_fractal=self.enable_bottom_fractal.get(),
            enable_large_move_gate=self.enable_large_move_gate.get(),
            enable_large_move_mean=self.enable_large_move_mean.get(),
            enable_large_move_atr=self.enable_large_move_atr.get(),
            enable_large_move_body_ratio=self.enable_large_move_body_ratio.get(),
            enable_large_move_fixed=self.enable_large_move_fixed.get(),
            mean_body_period=self._parse_int(self.mean_body_period.get(), 20, "均值N"),
            mean_body_multiplier=self._parse_decimal(self.mean_body_multiplier.get(), Decimal("1.8"), "大波动均值倍数"),
            large_move_atr_period=self._parse_int(self.large_move_atr_period.get(), 14, "大波动ATR周期"),
            large_move_atr_multiplier=self._parse_decimal(self.large_move_atr_multiplier.get(), Decimal("1.2"), "大波动ATR倍数"),
            body_ratio_threshold=self._parse_decimal(self.body_ratio_threshold.get(), Decimal("0.6"), "实体占比"),
            fixed_body_threshold=self._parse_decimal(self.fixed_body_threshold.get(), Decimal("0"), "固定实体阈值"),
            fractal_trend_lookback=self._parse_int(self.fractal_trend_lookback.get(), 5, "分型前N"),
            fractal_trend_min_bars=self._parse_int(self.fractal_trend_min_bars.get(), 3, "同向数"),
            false_break_reference_lookback=self._parse_int(self.false_break_reference_lookback.get(), 6, "假破N"),
            false_break_min_pct=self._parse_decimal(self.false_break_min_pct.get(), Decimal("0.05"), "刺破%"),
            false_break_atr_multiplier=self._parse_decimal(self.false_break_atr_multiplier.get(), Decimal("0.1"), "刺破ATR"),
            false_break_reclaim_position=self._parse_decimal(self.false_break_reclaim_position.get(), Decimal("0.6"), "收回位"),
        )

    def _refresh_side_panels(self) -> None:
        dataset = self._dataset
        if dataset is None:
            self._apply_empty_state()
            return
        summary = dataset.summary
        hit = "-" if summary.hit_rate_24h is None else f"{_fmt_decimal(summary.hit_rate_24h, 1)}%"
        avg = "-" if summary.avg_return_24h_pct is None else f"{_fmt_signed(summary.avg_return_24h_pct, 2)}%"
        self.stats_text.set(
            f"候选信号：{summary.total}\n"
            f"开多：{summary.long_count}   开空：{summary.short_count}\n"
            f"24H已验证：{summary.completed_24h}\n"
            f"24H有效率：{hit}\n"
            f"24H平均收益：{avg}"
        )
        self._populate_signal_cards()
        self._populate_sample_tree()
        self._update_selected_text()

    def _apply_empty_state(self) -> None:
        self.stats_text.set("暂无数据。")
        self.selected_signal_text.set("尚未选择信号。")
        for title, detail in zip(self.card_title_vars, self.card_detail_vars):
            title.set("-")
            detail.set("-")

    def _populate_signal_cards(self) -> None:
        dataset = self._dataset
        if dataset is None:
            return
        target_signal = self._signal_at_index(self._selected_index) or self._signal_at_index(self._hover_index)
        latest = dataset.candles[-1] if dataset.candles else None
        if target_signal is not None:
            self.card_title_vars[0].set(f"{target_signal.pattern_name}  {target_signal.score}分")
            self.card_detail_vars[0].set(
                f"{_format_ts(target_signal.ts)} | {_direction_label(target_signal.direction)} | {target_signal.candle_count}根K线"
            )
            self.card_title_vars[1].set("触发原因")
            self.card_detail_vars[1].set(target_signal.reason)
            validation = target_signal.validation
            self.card_title_vars[2].set("后验表现")
            rule_text = "、".join(target_signal.large_move_rules) if target_signal.large_move_rules else "-"
            self.card_detail_vars[2].set(
                f"4H {_fmt_optional_signed(validation.return_4h_pct)} | "
                f"12H {_fmt_optional_signed(validation.return_12h_pct)} | "
                f"24H {_fmt_optional_signed(validation.return_24h_pct)} | "
                f"MFE {_fmt_optional_signed(validation.max_favorable_excursion_pct)} / "
                f"MAE {_fmt_optional_signed(validation.max_adverse_excursion_pct)} | 大波动 {rule_text}"
            )
            return
        if latest is None:
            self._apply_empty_state()
            return
        self.card_title_vars[0].set("当前最新K线")
        self.card_detail_vars[0].set(f"{_format_ts(latest.ts)} | C {latest.close}")
        self.card_title_vars[1].set("当前组合")
        self.card_detail_vars[1].set(self.signal_set_text.get())
        self.card_title_vars[2].set("提示")
        self.card_detail_vars[2].set("移动到图上信号点，或在右侧样本区选择一条记录。")

    def _populate_sample_tree(self) -> None:
        if self.sample_tree is None:
            return
        dataset = self._dataset
        self.sample_tree.delete(*self.sample_tree.get_children())
        self._signal_by_tree_id.clear()
        if dataset is None:
            return
        for seq, signal in enumerate(reversed(dataset.signals), start=1):
            tree_id = f"S{seq:04d}"
            self._signal_by_tree_id[tree_id] = signal
            self.sample_tree.insert(
                "",
                END,
                iid=tree_id,
                values=(
                    _format_ts(signal.ts, short=True),
                    signal.pattern_name,
                    str(signal.candle_count),
                    _direction_short_label(signal.direction),
                    str(signal.score),
                    _fmt_optional_signed(signal.validation.return_24h_pct),
                ),
            )

    def _schedule_redraw(self) -> None:
        if self._redraw_job is not None:
            try:
                self.window.after_cancel(self._redraw_job)
            except Exception:
                pass
        self._redraw_job = self.window.after(16, self._redraw_all)

    def _redraw_all(self) -> None:
        self._redraw_job = None
        self._draw_main_chart()
        self._draw_macd_chart()
        self._draw_bias_chart()
        self._draw_volume_chart()

    def _draw_main_chart(self) -> None:
        canvas = self.main_chart
        width = max(canvas.winfo_width(), 900)
        height = max(canvas.winfo_height(), 460)
        canvas.delete("all")
        canvas.create_rectangle(0, 0, width, height, fill="#0f172a", outline="")
        dataset = self._dataset
        if dataset is None or not dataset.candles:
            canvas.create_text(width / 2, height / 2, text="加载数据后显示 1H K线和信号点", fill="#94a3b8", font=("Microsoft YaHei UI", 12))
            return
        start, end = self._visible_range()
        visible = dataset.candles[start:end]
        values = [float(item.high) for item in visible] + [float(item.low) for item in visible]
        values.extend(float(item) for item in dataset.ema_fast[start:end])
        values.extend(float(item) for item in dataset.ema_slow[start:end])
        geometry = self._build_geometry(canvas, values, start, end, top=22, bottom_pad=30)
        self._main_geometry = geometry
        self._draw_grid(canvas, width, height, geometry, rows=5, cols=8, label_values=True)

        for index in range(start, end):
            candle = dataset.candles[index]
            x = self._x_for_index(geometry, index)
            y_open = self._y_for_value(geometry, float(candle.open))
            y_close = self._y_for_value(geometry, float(candle.close))
            y_high = self._y_for_value(geometry, float(candle.high))
            y_low = self._y_for_value(geometry, float(candle.low))
            color = "#d8dee9" if candle.close >= candle.open else "#ef4444"
            canvas.create_line(x, y_high, x, y_low, fill=color, width=1)
            half = max(min(geometry.slot * 0.32, 7), 2)
            canvas.create_rectangle(x - half, min(y_open, y_close), x + half, max(y_open, y_close), fill=color, outline=color)

        self._draw_series(canvas, geometry, dataset.ema_fast, start, end, "#f59e0b", width=2)
        self._draw_series(canvas, geometry, dataset.ema_slow, start, end, "#38bdf8", width=2)
        self._draw_signal_markers(canvas, geometry, start, end)
        self._draw_crosshair(canvas, geometry)
        canvas.create_text(14, 10, text="价格 / EMA21 / EMA55", anchor="nw", fill="#e2e8f0", font=("Microsoft YaHei UI", 10, "bold"))

    def _draw_macd_chart(self) -> None:
        dataset = self._dataset
        self._draw_indicator_background(self.macd_chart, "MACD")
        if dataset is None or not dataset.candles:
            return
        start, end = self._visible_range()
        values = [float(item) for item in dataset.macd_histogram[start:end] + dataset.macd_line[start:end] + dataset.macd_signal[start:end]]
        geometry = self._build_geometry(self.macd_chart, values + [0.0], start, end, top=16, bottom_pad=18)
        zero_y = self._y_for_value(geometry, 0.0)
        self.macd_chart.create_line(geometry.left, zero_y, geometry.right, zero_y, fill="#475569", dash=(3, 3))
        for index in range(start, end):
            hist = float(dataset.macd_histogram[index])
            x = self._x_for_index(geometry, index)
            y = self._y_for_value(geometry, hist)
            color = "#22c55e" if hist >= 0 else "#ef4444"
            self.macd_chart.create_rectangle(x - geometry.slot * 0.25, zero_y, x + geometry.slot * 0.25, y, fill=color, outline=color)
        self._draw_series(self.macd_chart, geometry, dataset.macd_line, start, end, "#f59e0b", width=2)
        self._draw_series(self.macd_chart, geometry, dataset.macd_signal, start, end, "#60a5fa", width=2)

    def _draw_bias_chart(self) -> None:
        dataset = self._dataset
        self._draw_indicator_background(self.bias_chart, "乖离率")
        if dataset is None or not dataset.candles:
            return
        start, end = self._visible_range()
        values = [float(item) for item in dataset.bias_pct[start:end] if item is not None]
        config = self._current_config()
        values.extend([float(config.bias_min_pct), float(config.bias_max_pct), 0.0])
        geometry = self._build_geometry(self.bias_chart, values, start, end, top=16, bottom_pad=18)
        for level, label in ((config.bias_min_pct, f"{config.bias_min_pct}%"), (config.bias_max_pct, f"{config.bias_max_pct}%")):
            y = self._y_for_value(geometry, float(level))
            self.bias_chart.create_line(geometry.left, y, geometry.right, y, fill="#fbbf24", dash=(4, 3))
            self.bias_chart.create_text(geometry.right - 4, y - 2, text=label, anchor="e", fill="#fcd34d", font=("Consolas", 9))
        self._draw_optional_series(self.bias_chart, geometry, dataset.bias_pct, start, end, "#c084fc", width=2)

    def _draw_volume_chart(self) -> None:
        dataset = self._dataset
        self._draw_indicator_background(self.volume_chart, "成交量")
        if dataset is None or not dataset.candles:
            return
        start, end = self._visible_range()
        volumes = [float(item.volume) for item in dataset.candles[start:end]]
        ma_values = [float(item) for item in dataset.volume_ma[start:end] if item is not None]
        geometry = self._build_geometry(self.volume_chart, volumes + ma_values + [0.0], start, end, top=16, bottom_pad=18)
        baseline = self._y_for_value(geometry, 0.0)
        for index in range(start, end):
            x = self._x_for_index(geometry, index)
            y = self._y_for_value(geometry, float(dataset.candles[index].volume))
            color = "#22c55e" if dataset.candles[index].close >= dataset.candles[index].open else "#ef4444"
            self.volume_chart.create_rectangle(x - geometry.slot * 0.25, baseline, x + geometry.slot * 0.25, y, fill=color, outline=color)
        self._draw_optional_series(self.volume_chart, geometry, dataset.volume_ma, start, end, "#f59e0b", width=2)

    def _draw_indicator_background(self, canvas: Canvas, title: str) -> None:
        width = max(canvas.winfo_width(), 900)
        height = max(canvas.winfo_height(), 100)
        canvas.delete("all")
        canvas.create_rectangle(0, 0, width, height, fill="#111827", outline="")
        for row in range(1, 3):
            y = height * row / 3
            canvas.create_line(0, y, width, y, fill="#293347")
        for col in range(1, 8):
            x = width * col / 8
            canvas.create_line(x, 0, x, height, fill="#293347")
        canvas.create_text(14, 10, text=title, anchor="nw", fill="#e2e8f0", font=("Microsoft YaHei UI", 10, "bold"))

    def _draw_signal_markers(self, canvas: Canvas, geometry: _ChartGeometry, start: int, end: int) -> None:
        dataset = self._dataset
        if dataset is None:
            return
        for signal in dataset.signals:
            if signal.index < start or signal.index >= end:
                continue
            candle = dataset.candles[signal.index]
            x = self._x_for_index(geometry, signal.index)
            if signal.direction == "long":
                y = self._y_for_value(geometry, float(candle.low)) + 18
                canvas.create_polygon(x, y - 14, x - 8, y + 4, x + 8, y + 4, fill="#22c55e", outline="")
            else:
                y = self._y_for_value(geometry, float(candle.high)) - 18
                canvas.create_polygon(x - 8, y - 4, x + 8, y - 4, x, y + 14, fill="#ef4444", outline="")
            if signal.index == self._selected_index:
                canvas.create_oval(x - 13, y - 13, x + 13, y + 13, outline="#fcd34d", width=2)

    def _draw_grid(self, canvas: Canvas, width: int, height: int, geometry: _ChartGeometry, *, rows: int, cols: int, label_values: bool) -> None:
        for row in range(1, rows):
            y = geometry.top + (geometry.bottom - geometry.top) * row / rows
            canvas.create_line(0, y, width, y, fill="#253047")
        for col in range(1, cols):
            x = geometry.left + (geometry.right - geometry.left) * col / cols
            canvas.create_line(x, 0, x, height, fill="#253047")
        if label_values:
            for row in range(rows + 1):
                value = geometry.value_max - (geometry.value_max - geometry.value_min) * row / rows
                y = geometry.top + (geometry.bottom - geometry.top) * row / rows
                canvas.create_text(width - 8, y, text=f"{value:.2f}", anchor="e", fill="#94a3b8", font=("Consolas", 9))

    def _draw_crosshair(self, canvas: Canvas, geometry: _ChartGeometry) -> None:
        index = self._selected_index if self._selected_index is not None else self._hover_index
        if index is None or index < geometry.start or index >= geometry.end:
            return
        x = self._x_for_index(geometry, index)
        canvas.create_line(x, geometry.top, x, geometry.bottom, fill="#f8fafc", dash=(3, 4))

    def _draw_series(self, canvas: Canvas, geometry: _ChartGeometry, values: tuple[Decimal, ...], start: int, end: int, color: str, *, width: int) -> None:
        points: list[float] = []
        for index in range(start, end):
            points.extend((self._x_for_index(geometry, index), self._y_for_value(geometry, float(values[index]))))
        if len(points) >= 4:
            canvas.create_line(*points, fill=color, width=width, smooth=True)

    def _draw_optional_series(
        self,
        canvas: Canvas,
        geometry: _ChartGeometry,
        values: tuple[Decimal | None, ...],
        start: int,
        end: int,
        color: str,
        *,
        width: int,
    ) -> None:
        points: list[float] = []
        for index in range(start, end):
            value = values[index]
            if value is None:
                if len(points) >= 4:
                    canvas.create_line(*points, fill=color, width=width, smooth=True)
                points = []
                continue
            points.extend((self._x_for_index(geometry, index), self._y_for_value(geometry, float(value))))
        if len(points) >= 4:
            canvas.create_line(*points, fill=color, width=width, smooth=True)

    def _build_geometry(self, canvas: Canvas, values: list[float], start: int, end: int, *, top: int, bottom_pad: int) -> _ChartGeometry:
        width = max(canvas.winfo_width(), 900)
        height = max(canvas.winfo_height(), 100)
        left = 58
        right = width - 48
        bottom = height - bottom_pad
        value_min = min(values) if values else 0.0
        value_max = max(values) if values else 1.0
        if value_max <= value_min:
            value_max = value_min + 1.0
        padding = (value_max - value_min) * 0.08
        value_min -= padding
        value_max += padding
        visible = max(end - start, 1)
        return _ChartGeometry(left, top, right, bottom, start, end, (right - left) / visible, value_min, value_max)

    def _visible_range(self) -> tuple[int, int]:
        dataset = self._dataset
        total = len(dataset.candles) if dataset is not None else 0
        visible = max(30, min(self._visible_count, max(total, 1)))
        self._visible_start = max(0, min(self._visible_start, max(total - visible, 0)))
        return self._visible_start, min(self._visible_start + visible, total)

    def _x_for_index(self, geometry: _ChartGeometry, index: int) -> float:
        return geometry.left + ((index - geometry.start) + 0.5) * geometry.slot

    def _y_for_value(self, geometry: _ChartGeometry, value: float) -> float:
        span = geometry.value_max - geometry.value_min
        return geometry.bottom - ((value - geometry.value_min) / span) * (geometry.bottom - geometry.top)

    def _index_for_x(self, x: float) -> int | None:
        geometry = self._main_geometry
        dataset = self._dataset
        if geometry is None or dataset is None:
            return None
        index = geometry.start + int((x - geometry.left) / max(geometry.slot, 1))
        if 0 <= index < len(dataset.candles):
            return index
        return None

    def _on_mousewheel(self, event) -> None:
        dataset = self._dataset
        if dataset is None:
            return
        anchor = self._index_for_x(float(event.x)) or (self._visible_start + self._visible_count // 2)
        old_count = self._visible_count
        if event.delta > 0:
            self._visible_count = max(40, int(self._visible_count * 0.82))
        else:
            self._visible_count = min(len(dataset.candles), int(self._visible_count * 1.18) + 1)
        ratio = 0.5 if old_count <= 0 else (anchor - self._visible_start) / old_count
        self._visible_start = int(anchor - self._visible_count * ratio)
        self._schedule_redraw()

    def _on_mouse_motion(self, event) -> None:
        index = self._index_for_x(float(event.x))
        if index == self._hover_index:
            return
        self._hover_index = index
        self._populate_signal_cards()
        self._schedule_redraw()

    def _on_mouse_leave(self, _event) -> None:
        self._hover_index = None
        self._populate_signal_cards()
        self._schedule_redraw()

    def _on_mouse_press(self, event) -> None:
        self._pan_anchor_x = int(event.x)
        self._pan_anchor_start = self._visible_start
        index = self._index_for_x(float(event.x))
        if index is not None:
            self._selected_index = index
            self._update_selected_text()
            self._populate_signal_cards()
            self._schedule_redraw()

    def _on_mouse_drag(self, event) -> None:
        if self._pan_anchor_x is None or self._main_geometry is None:
            return
        shift = int((self._pan_anchor_x - int(event.x)) / max(self._main_geometry.slot, 1))
        self._visible_start = self._pan_anchor_start + shift
        self._schedule_redraw()

    def _on_mouse_release(self, _event) -> None:
        self._pan_anchor_x = None

    def _on_sample_selected(self, _event=None) -> None:
        selected = self.sample_tree.selection()
        if not selected:
            return
        signal = self._signal_by_tree_id.get(selected[0])
        if signal is None:
            return
        self._selected_index = signal.index
        self._visible_start = max(0, signal.index - self._visible_count // 2)
        self._update_selected_text()
        self._populate_signal_cards()
        self._schedule_redraw()

    def _signal_at_index(self, index: int | None) -> SignalReplayPoint | None:
        if index is None or self._dataset is None:
            return None
        return next((item for item in self._dataset.signals if item.index == index), None)

    def _update_selected_text(self) -> None:
        dataset = self._dataset
        if dataset is None or self._selected_index is None or not (0 <= self._selected_index < len(dataset.candles)):
            self.selected_signal_text.set("尚未选择信号。")
            return
        candle = dataset.candles[self._selected_index]
        signal = self._signal_at_index(self._selected_index)
        if signal is None:
            self.selected_signal_text.set(f"{_format_ts(candle.ts)}\nC {candle.close}\n当前 K线不是候选信号。")
            return
        self.selected_signal_text.set(
            f"{_format_ts(signal.ts)}\n"
            f"{_direction_label(signal.direction)} | {signal.score}分\n"
            f"{signal.pattern_name} | {signal.candle_count}根K线\n"
            f"24H {_fmt_optional_signed(signal.validation.return_24h_pct)}"
        )

    def _schedule_auto_refresh(self) -> None:
        if self._auto_refresh_job is not None:
            try:
                self.window.after_cancel(self._auto_refresh_job)
            except Exception:
                pass
            self._auto_refresh_job = None
        if not self.auto_refresh_enabled.get() or not self.window.winfo_exists():
            return
        self._auto_refresh_job = self.window.after(_AUTO_REFRESH_MS, self._run_auto_refresh)

    def _run_auto_refresh(self) -> None:
        self._auto_refresh_job = None
        if not self.window.winfo_exists():
            return
        if self.auto_refresh_enabled.get():
            self._load_replay_data(trigger_email=True)
        self._schedule_auto_refresh()

    def _handle_latest_candle_refresh(self) -> None:
        dataset = self._dataset
        if dataset is None or not dataset.candles:
            return
        latest_ts = dataset.candles[-1].ts
        latest_signals = [signal for signal in dataset.signals if signal.ts == latest_ts]
        symbol = self.symbol.get().strip().upper() or "BTC-USDT-SWAP"
        if not latest_signals:
            self._logger(f"[信号复盘实验室] {symbol} 最新 1H K线无新信号")
            return
        self._logger(f"[信号复盘实验室] {symbol} 最新 1H K线生成 {len(latest_signals)} 个新信号")
        if self.latest_signal_email_enabled.get():
            self._send_latest_signal_emails(latest_signals)

    def _send_latest_signal_emails(self, signals: list[SignalReplayPoint]) -> None:
        notifier = self._notifier or load_btc_market_email_notifier()
        self._notifier = notifier
        if notifier is None or not notifier.signal_notifications_enabled:
            self._logger("[信号复盘实验室] 最新K线信号邮件未发送：请检查全局邮件和信号通知设置")
            return
        symbol = self.symbol.get().strip().upper() or "BTC-USDT-SWAP"
        bar = self.bar.get().strip() or "1H"
        sent_count = 0
        for signal in signals:
            dedupe_key = _signal_dedupe_key(symbol, bar, signal)
            if dedupe_key in self._emailed_signal_keys:
                continue
            notifier.notify_async(
                _build_signal_email_subject(symbol, bar, signal),
                _build_signal_email_body(symbol, bar, signal),
            )
            self._emailed_signal_keys.add(dedupe_key)
            sent_count += 1
        if sent_count > 0:
            self._logger(f"[信号复盘实验室] {symbol} 最新K线信号邮件已发送 {sent_count} 封")

    @staticmethod
    def _parse_decimal(value: str, fallback: Decimal, label: str) -> Decimal:
        try:
            return Decimal(str(value).strip())
        except (InvalidOperation, ValueError):
            raise ValueError(f"{label} 必须是数字")

    @staticmethod
    def _parse_int(value: str, fallback: int, label: str) -> int:
        try:
            parsed = int(str(value).strip())
        except ValueError:
            raise ValueError(f"{label} 必须是整数")
        if parsed <= 0:
            raise ValueError(f"{label} 必须大于0")
        return parsed


def _direction_label(direction: str) -> str:
    if direction == "long":
        return "开多"
    if direction == "short":
        return "开空"
    return "观察"


def _direction_short_label(direction: str) -> str:
    if direction == "long":
        return "多"
    if direction == "short":
        return "空"
    return "观"


def _format_ts(ts: int, *, short: bool = False) -> str:
    dt = datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc).astimezone(_CHINA_TZ)
    return dt.strftime("%m-%d %H:%M") if short else dt.strftime("%Y-%m-%d %H:%M")


def _fmt_decimal(value: Decimal, places: int) -> str:
    return f"{value:.{places}f}"


def _fmt_signed(value: Decimal, places: int = 2) -> str:
    prefix = "+" if value > 0 else ""
    return f"{prefix}{value:.{places}f}"


def _fmt_optional_signed(value: Decimal | None) -> str:
    if value is None:
        return "-"
    return f"{_fmt_signed(value, 2)}%"
