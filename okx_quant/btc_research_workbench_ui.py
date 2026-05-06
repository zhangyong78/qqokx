from __future__ import annotations

import json
import math
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from tkinter import END, Canvas, StringVar, Text, Toplevel
from tkinter import ttk
from typing import Any, Callable

from okx_quant.persistence import (
    analysis_report_dir_path,
    deribit_volatility_cache_file_path,
    load_btc_research_workbench_state,
    load_journal_entries_snapshot,
    save_btc_research_workbench_state,
)
from okx_quant.deribit_client import DeribitVolatilityCandle
from okx_quant.journal import JournalEntry
from okx_quant.models import Candle
from okx_quant.window_layout import (
    apply_adaptive_window_geometry,
    apply_fill_window_geometry,
    apply_window_icon,
    toggle_toplevel_maximize,
)


Logger = Callable[[str], None]
DrawingTool = str

DRAWING_TOOL_OPTIONS: tuple[tuple[str, str], ...] = (
    ("observe", "观察"),
    ("trend_line", "趋势线"),
    ("horizontal_line", "水平线"),
    ("rectangle", "矩形"),
    ("parallel_channel", "平行通道"),
)

DERIBIT_RESOLUTION_BY_BAR = {
    "1H": "3600",
    "4H": "14400",
    "1D": "1D",
}

TIMEFRAME_MS = {
    "1H": 3_600_000,
    "4H": 14_400_000,
    "1D": 86_400_000,
}


@dataclass(frozen=True)
class ChartBounds:
    left: float
    top: float
    right: float
    bottom: float

    def contains(self, x: float, y: float) -> bool:
        return self.left <= x <= self.right and self.top <= y <= self.bottom


@dataclass(frozen=True)
class DrawingAnnotation:
    tool: DrawingTool
    start_index: int
    end_index: int
    price_a: float
    price_b: float


@dataclass(frozen=True)
class HistoricalAnalysisMarker:
    generated_at: str
    timeframe: str
    candle_ts: int
    direction: str
    score: int
    confidence: str


@dataclass
class ChartViewport:
    start_index: int = 0
    visible_count: int | None = None
    pan_anchor_x: int | None = None
    pan_anchor_start: int = 0


class BtcResearchWorkbenchWindow:
    def __init__(
        self,
        parent: Toplevel,
        *,
        client: Any | None = None,
        deribit_client: Any | None = None,
        logger: Logger | None = None,
    ) -> None:
        self._client = client
        self._deribit_client = deribit_client
        self._logger = logger or (lambda _message: None)
        self._entries: list[JournalEntry] = []
        self._selected_entry_id = ""
        self._candles: list[Candle] = []
        self._volatility_candles: list[Candle] = []
        self._overlay_pairs: list[tuple[Candle, Candle]] = []
        self._drawings: list[DrawingAnnotation] = []
        self._historical_markers: list[HistoricalAnalysisMarker] = []
        self._chart_load_token = 0
        self._volatility_load_token = 0
        self._chart_render_token = 0
        self._viewport = ChartViewport()
        self._active_chart_bounds: ChartBounds | None = None
        self._active_chart_prices: tuple[float, float] | None = None
        self._pending_draw_start: tuple[float, float] | None = None
        self._pending_draw_end: tuple[float, float] | None = None
        self._drag_canvas: Canvas | None = None
        self._viewport_dirty = False

        self.window = Toplevel(parent)
        self.window.title("BTC研究工作台")
        self.window.protocol("WM_DELETE_WINDOW", self.destroy)
        apply_window_icon(self.window)
        apply_adaptive_window_geometry(
            self.window,
            width_ratio=0.9,
            height_ratio=0.86,
            min_width=1320,
            min_height=860,
            max_width=1760,
            max_height=1120,
        )
        if not toggle_toplevel_maximize(self.window):
            apply_fill_window_geometry(
                self.window,
                min_width=1320,
                min_height=860,
            )

        self.status_text = StringVar(value="BTC研究工作台已就绪")
        self.chart_status_text = StringVar(value="主图 / 波动率K线 / 叠加对比")
        self.chart_timeframe = StringVar(value="4H")
        self.drawing_tool = StringVar(value="观察")
        self.volatility_summary_text = StringVar(value="等待加载波动率数据。")
        self.overlay_summary_text = StringVar(value="等待加载叠加对比数据。")
        self.detail_title_text = StringVar(value="-")
        self.detail_meta_text = StringVar(value="-")

        self._build_layout()
        self._load_entries(select_latest=True)
        if self._client is not None:
            self.window.after(120, self._load_chart_candles)

    def show(self) -> None:
        self.window.deiconify()
        self.window.lift()
        self.window.focus_force()

    def destroy(self) -> None:
        if self.window.winfo_exists():
            self.window.destroy()

    def _build_layout(self) -> None:
        self.window.columnconfigure(0, weight=1)
        self.window.rowconfigure(1, weight=1)

        header = ttk.Frame(self.window, padding=(14, 12, 14, 8))
        header.grid(row=0, column=0, sticky="ew")
        header.columnconfigure(0, weight=1)
        ttk.Label(header, text="BTC研究工作台", font=("Microsoft YaHei UI", 16, "bold")).grid(row=0, column=0, sticky="w")
        ttk.Label(header, textvariable=self.status_text).grid(row=0, column=1, sticky="e")

        body = ttk.Panedwindow(self.window, orient="horizontal")
        body.grid(row=1, column=0, sticky="nsew", padx=14, pady=(0, 12))

        left = ttk.Frame(body, padding=(0, 0, 8, 0))
        left.columnconfigure(0, weight=1)
        left.rowconfigure(1, weight=1)
        body.add(left, weight=24)

        center = ttk.Frame(body, padding=(8, 0))
        center.columnconfigure(0, weight=1)
        center.rowconfigure(1, weight=1)
        center.rowconfigure(2, weight=0)
        body.add(center, weight=50)

        right = ttk.Frame(body, padding=(8, 0, 0, 0))
        right.columnconfigure(0, weight=1)
        right.rowconfigure(3, weight=1)
        right.rowconfigure(5, weight=1)
        body.add(right, weight=26)

        sample_header = ttk.Frame(left)
        sample_header.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        sample_header.columnconfigure(0, weight=1)
        ttk.Label(sample_header, text="研究样本", font=("Microsoft YaHei UI", 11, "bold")).grid(row=0, column=0, sticky="w")
        ttk.Button(sample_header, text="刷新", command=lambda: self._load_entries(select_latest=False)).grid(row=0, column=1)

        self.sample_tree = ttk.Treeview(
            left,
            columns=("time", "symbol", "type", "bias"),
            show="headings",
            selectmode="browse",
            height=30,
        )
        self.sample_tree.heading("time", text="时间")
        self.sample_tree.heading("symbol", text="标的")
        self.sample_tree.heading("type", text="类型")
        self.sample_tree.heading("bias", text="方向")
        self.sample_tree.column("time", width=118, anchor="center")
        self.sample_tree.column("symbol", width=118, anchor="w")
        self.sample_tree.column("type", width=106, anchor="center")
        self.sample_tree.column("bias", width=72, anchor="center")
        self.sample_tree.grid(row=1, column=0, sticky="nsew")
        self.sample_tree.bind("<<TreeviewSelect>>", self._on_sample_select)
        sample_scroll = ttk.Scrollbar(left, orient="vertical", command=self.sample_tree.yview)
        sample_scroll.grid(row=1, column=1, sticky="ns")
        self.sample_tree.configure(yscrollcommand=sample_scroll.set)

        chart_header = ttk.Frame(center)
        chart_header.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        chart_header.columnconfigure(0, weight=1)
        ttk.Label(chart_header, textvariable=self.chart_status_text, font=("Microsoft YaHei UI", 11, "bold")).grid(row=0, column=0, sticky="w")
        ttk.Label(chart_header, text="周期").grid(row=0, column=1, padx=(10, 4))
        ttk.Combobox(chart_header, textvariable=self.chart_timeframe, values=("1H", "4H", "1D"), width=7, state="readonly").grid(row=0, column=2)
        ttk.Button(chart_header, text="加载K线", command=self._load_chart_candles).grid(row=0, column=3, padx=(8, 0))
        ttk.Button(chart_header, text="重置视图", command=self._reset_chart_view).grid(row=0, column=4, padx=(8, 0))
        ttk.Button(chart_header, text="刷新分析锚点", command=self._reload_historical_markers).grid(row=0, column=5, padx=(8, 0))

        notebook = ttk.Notebook(center)
        notebook.grid(row=1, column=0, sticky="nsew")
        notebook.bind("<<NotebookTabChanged>>", lambda _event: self._schedule_chart_redraw(), add="+")
        self.chart_notebook = notebook

        main_tab = ttk.Frame(notebook, padding=10)
        main_tab.columnconfigure(0, weight=1)
        main_tab.rowconfigure(1, weight=1)
        notebook.add(main_tab, text="BTC主图")

        draw_toolbar = ttk.Frame(main_tab)
        draw_toolbar.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        draw_toolbar.columnconfigure(8, weight=1)
        ttk.Label(draw_toolbar, text="人工工具").grid(row=0, column=0, sticky="w")
        ttk.Combobox(
            draw_toolbar,
            textvariable=self.drawing_tool,
            state="readonly",
            width=10,
            values=tuple(label for _, label in DRAWING_TOOL_OPTIONS),
        ).grid(row=0, column=1, padx=(6, 0))
        ttk.Button(draw_toolbar, text="撤销一笔", command=self._remove_last_drawing).grid(row=0, column=2, padx=(8, 0))
        ttk.Button(draw_toolbar, text="清空画图", command=self._clear_drawings).grid(row=0, column=3, padx=(8, 0))
        ttk.Label(draw_toolbar, text="支持：矩形 / 趋势线 / 水平线 / 平行通道 | 滚轮缩放 | 拖动平移").grid(row=0, column=8, sticky="e")

        self.chart_canvas = Canvas(main_tab, background="#ffffff", highlightthickness=0, cursor="crosshair")
        self.chart_canvas.grid(row=1, column=0, sticky="nsew")

        vol_tab = ttk.Frame(notebook, padding=10)
        vol_tab.columnconfigure(0, weight=1)
        vol_tab.rowconfigure(1, weight=1)
        notebook.add(vol_tab, text="波动率K线")
        ttk.Label(vol_tab, textvariable=self.volatility_summary_text, justify="left").grid(row=0, column=0, sticky="w", pady=(0, 8))
        self.volatility_canvas = Canvas(vol_tab, background="#ffffff", highlightthickness=0, cursor="crosshair")
        self.volatility_canvas.grid(row=1, column=0, sticky="nsew")

        overlay_tab = ttk.Frame(notebook, padding=10)
        overlay_tab.columnconfigure(0, weight=1)
        overlay_tab.rowconfigure(1, weight=1)
        notebook.add(overlay_tab, text="叠加对比")
        ttk.Label(overlay_tab, textvariable=self.overlay_summary_text, justify="left").grid(row=0, column=0, sticky="w", pady=(0, 8))
        overlay_pane = ttk.Panedwindow(overlay_tab, orient="vertical")
        overlay_pane.grid(row=1, column=0, sticky="nsew")

        overlay_price_frame = ttk.Frame(overlay_pane)
        overlay_price_frame.columnconfigure(0, weight=1)
        overlay_price_frame.rowconfigure(1, weight=1)
        ttk.Label(overlay_price_frame, text="BTC价格K线").grid(row=0, column=0, sticky="w", pady=(0, 4))
        self.overlay_price_canvas = Canvas(overlay_price_frame, background="#ffffff", highlightthickness=0, cursor="crosshair")
        self.overlay_price_canvas.grid(row=1, column=0, sticky="nsew")
        overlay_pane.add(overlay_price_frame, weight=3)

        overlay_vol_frame = ttk.Frame(overlay_pane)
        overlay_vol_frame.columnconfigure(0, weight=1)
        overlay_vol_frame.rowconfigure(1, weight=1)
        ttk.Label(overlay_vol_frame, text="波动率指数 / 历史波动率").grid(row=0, column=0, sticky="w", pady=(0, 4))
        self.overlay_vol_canvas = Canvas(overlay_vol_frame, background="#ffffff", highlightthickness=0, cursor="crosshair")
        self.overlay_vol_canvas.grid(row=1, column=0, sticky="nsew")
        overlay_pane.add(overlay_vol_frame, weight=2)

        for canvas in (self.chart_canvas, self.volatility_canvas, self.overlay_price_canvas, self.overlay_vol_canvas):
            canvas.bind("<Configure>", lambda _event: self._schedule_chart_redraw(), add="+")
            canvas.bind("<MouseWheel>", self._on_chart_mousewheel, add="+")
            canvas.bind("<Button-1>", self._on_chart_press, add="+")
            canvas.bind("<B1-Motion>", self._on_chart_drag, add="+")
            canvas.bind("<ButtonRelease-1>", self._on_chart_release, add="+")
            canvas.bind("<Double-Button-1>", lambda _event: self._reset_chart_view(), add="+")

        signal_box = ttk.LabelFrame(center, text="程序分析 / 波动率叠加", padding=10)
        signal_box.grid(row=2, column=0, sticky="ew", pady=(10, 0))
        signal_box.columnconfigure(0, weight=1)
        self.signal_text = Text(signal_box, height=6, wrap="word", font=("Microsoft YaHei UI", 10))
        self.signal_text.grid(row=0, column=0, sticky="ew")
        self._replace_text(
            self.signal_text,
            "这里会汇总程序分析、历史分析锚点和波动率状态。当前版本先完成视口、叠加和复盘锚点底座。",
        )

        detail_header = ttk.Frame(right)
        detail_header.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        detail_header.columnconfigure(0, weight=1)
        ttk.Label(detail_header, textvariable=self.detail_title_text, font=("Microsoft YaHei UI", 12, "bold")).grid(row=0, column=0, sticky="w")
        ttk.Label(right, textvariable=self.detail_meta_text).grid(row=1, column=0, sticky="w", pady=(0, 8))

        ttk.Label(right, text="原始随笔", font=("Microsoft YaHei UI", 10, "bold")).grid(row=2, column=0, sticky="w")
        self.raw_text = Text(right, height=9, wrap="word", font=("Microsoft YaHei UI", 10))
        self.raw_text.grid(row=3, column=0, sticky="nsew", pady=(4, 10))

        ttk.Label(right, text="结构化 JSON", font=("Microsoft YaHei UI", 10, "bold")).grid(row=4, column=0, sticky="w")
        self.json_text = Text(right, height=12, wrap="none", font=("Consolas", 9))
        self.json_text.grid(row=5, column=0, sticky="nsew", pady=(4, 0))

    def _load_entries(self, *, select_latest: bool) -> None:
        snapshot = load_journal_entries_snapshot()
        self._entries = [JournalEntry.from_dict(item) for item in snapshot.get("entries", []) or [] if isinstance(item, dict)]
        for item_id in self.sample_tree.get_children():
            self.sample_tree.delete(item_id)
        for entry in self._entries:
            extraction = entry.extraction
            payload = extraction.raw_payload if extraction and isinstance(extraction.raw_payload, dict) else {}
            symbol = extraction.inst_id if extraction and extraction.inst_id else (extraction.symbol if extraction else "-")
            record_type = str(payload.get("record_type", "") or "-")
            bias = extraction.bias if extraction else "-"
            self.sample_tree.insert(
                "",
                END,
                iid=entry.entry_id,
                values=(_format_local_time(entry.created_at), symbol or "-", record_type, bias),
            )
        if self._entries and (select_latest or not self._selected_entry_id):
            self.sample_tree.selection_set(self._entries[0].entry_id)
            self._select_entry(self._entries[0])
        else:
            self._schedule_chart_redraw()
        self.status_text.set(f"已加载 {len(self._entries)} 条研究样本")

    def _on_sample_select(self, _event=None) -> None:
        selected = self.sample_tree.selection()
        if not selected:
            return
        entry = next((item for item in self._entries if item.entry_id == selected[0]), None)
        if entry is not None:
            self._select_entry(entry)

    def _select_entry(self, entry: JournalEntry) -> None:
        self._selected_entry_id = entry.entry_id
        payload = _entry_payload(entry)
        title = str(payload.get("title", "") or "").strip() or (entry.raw_text[:28] + "..." if len(entry.raw_text) > 28 else entry.raw_text)
        extraction = entry.extraction
        meta = []
        if extraction:
            meta.append(extraction.inst_id or extraction.symbol or "-")
            meta.append("/".join(extraction.timeframes) or "-")
            meta.append(extraction.bias)
        self.detail_title_text.set(title or "-")
        self.detail_meta_text.set(" | ".join(meta) if meta else "-")
        self._replace_text(self.raw_text, entry.raw_text)
        self._replace_text(self.json_text, json.dumps(payload, ensure_ascii=False, indent=2) if payload else "")

    def _load_chart_candles(self) -> None:
        if self._client is None:
            self.chart_status_text.set("未接入 OKX 行情客户端，当前只能显示静态工作台。")
            self._schedule_chart_redraw()
            return
        timeframe = self.chart_timeframe.get().strip() or "4H"
        self._chart_load_token += 1
        token = self._chart_load_token
        self.chart_status_text.set(f"正在加载 BTC-USDT-SWAP {timeframe} K线...")

        def worker() -> None:
            try:
                candles = self._client.get_candles_history("BTC-USDT-SWAP", timeframe, limit=520)
            except Exception as exc:
                self.window.after(0, lambda: self._apply_chart_error(token, str(exc)))
                return
            self.window.after(0, lambda: self._apply_chart_candles(token, timeframe, candles))

        threading.Thread(target=worker, daemon=True).start()

    def _apply_chart_candles(self, token: int, timeframe: str, candles: list[Candle]) -> None:
        if token != self._chart_load_token:
            return
        self._candles = [candle for candle in candles if candle.confirmed]
        self._load_saved_state_for_current_view()
        self.chart_status_text.set(f"BTC-USDT-SWAP {timeframe} | 已加载 {len(self._candles)} 根K线")
        self._load_volatility_series()
        self._reload_historical_markers()
        self._update_signal_text()
        self._schedule_chart_redraw()
        self._logger(f"[BTC研究工作台] 价格K线加载完成 | BTC-USDT-SWAP {timeframe} {len(self._candles)}")

    def _apply_chart_error(self, token: int, message: str) -> None:
        if token != self._chart_load_token:
            return
        self.chart_status_text.set(f"K线加载失败：{message}")
        self._logger(f"[BTC研究工作台] 价格K线加载失败 | {message}")
        self._schedule_chart_redraw()

    def _load_volatility_series(self) -> None:
        timeframe = self.chart_timeframe.get().strip() or "4H"
        self._volatility_load_token += 1
        token = self._volatility_load_token

        def worker() -> None:
            requested_limit = max(self._viewport.visible_count or 220, 220)
            volatility = self._load_deribit_volatility_from_cache(timeframe, requested_limit=requested_limit)
            if not volatility:
                volatility = self._load_deribit_volatility_live(timeframe, requested_limit=requested_limit)
            if not volatility and self._candles:
                volatility = _build_realized_volatility_from_reference(self._candles, bar=timeframe, lookback=20)
            self.window.after(0, lambda: self._apply_volatility_series(token, volatility))

        threading.Thread(target=worker, daemon=True).start()

    def _apply_volatility_series(self, token: int, candles: list[Candle]) -> None:
        if token != self._volatility_load_token:
            return
        self._volatility_candles = candles
        self._overlay_pairs = _align_overlay_candles(self._candles, self._volatility_candles, bar=self.chart_timeframe.get().strip() or "4H")
        if self._volatility_candles:
            latest = self._volatility_candles[-1]
            source_name = "Deribit 波动率指数" if _looks_like_deribit_series(self._volatility_candles) else "程序历史波动率"
            self.volatility_summary_text.set(
                f"{source_name} | 周期 {self.chart_timeframe.get()} | 根数 {len(self._volatility_candles)} | 最新 C {latest.close:.2f}"
            )
        else:
            self.volatility_summary_text.set("暂无可用波动率数据。")
        if self._overlay_pairs:
            latest_price, latest_vol = self._overlay_pairs[-1]
            self.overlay_summary_text.set(
                f"叠加对比 | 对齐根数 {len(self._overlay_pairs)} | 价格 C {latest_price.close} | 波动率 C {latest_vol.close:.2f}"
            )
        elif self._candles and self._volatility_candles:
            self.overlay_summary_text.set(
                f"叠加对比 | 价格K线 {len(self._candles)} 根 | 波动率K线 {len(self._volatility_candles)} 根 | 当前按双图并排复盘"
            )
        else:
            self.overlay_summary_text.set("叠加对比需要同时具备价格K线与波动率K线。")
        self._update_signal_text()
        self._schedule_chart_redraw()

    def _reload_historical_markers(self) -> None:
        timeframe = self.chart_timeframe.get().strip() or "4H"
        self._historical_markers = _load_historical_analysis_markers("BTC-USDT-SWAP", timeframe)
        self._update_signal_text()
        self._schedule_chart_redraw()

    def _update_signal_text(self) -> None:
        if len(self._candles) < 2:
            self._replace_text(self.signal_text, "价格K线样本不足。")
            return
        latest = self._candles[-1]
        previous = self._candles[-2]
        close_change = ((latest.close - previous.close) / previous.close * 100) if previous.close else Decimal("0")
        ranges = [((candle.high - candle.low) / candle.close * 100) for candle in self._candles[-30:] if candle.close]
        avg_range = (sum(ranges, start=Decimal("0")) / len(ranges)) if ranges else Decimal("0")
        lines = [
            f"最新收盘：{latest.close}",
            f"上一根涨跌：{close_change:.2f}%",
            f"近30根平均振幅：{avg_range:.2f}%",
        ]
        if self._volatility_candles:
            vol_latest = self._volatility_candles[-1]
            lines.append(f"波动率最新值：{vol_latest.close:.2f}")
        if self._historical_markers:
            lines.append(f"历史分析锚点：{len(self._historical_markers)} 条")
        lines.append("滚轮缩放、拖动画布平移，画图和视口都会保留到下次打开。")
        self._replace_text(self.signal_text, "\n".join(lines))

    def _schedule_chart_redraw(self) -> None:
        token = self._chart_render_token + 1
        self._chart_render_token = token
        self.window.after(16, lambda: self._run_scheduled_redraw(token))

    def _run_scheduled_redraw(self, token: int) -> None:
        if token != self._chart_render_token or not self.window.winfo_exists():
            return
        self._render_all_tabs()

    def _render_all_tabs(self) -> None:
        self._render_main_chart()
        self._render_volatility_tab()
        self._render_overlay_tab()

    def _render_main_chart(self) -> None:
        if not self._candles:
            self._render_placeholder(self.chart_canvas, "加载 BTC K 线后，这里显示主图、历史分析锚点和人工划线。")
            return
        visible = self._visible_candles(self._candles)
        self._render_candle_chart(
            self.chart_canvas,
            visible,
            full_candles=self._candles,
            title=f"BTC-USDT-SWAP {self.chart_timeframe.get()} | 主图",
            subtitle="历史分析锚点 / 趋势线 / 水平线 / 矩形 / 平行通道",
            axis_suffix="",
            show_drawings=True,
            show_markers=True,
        )

    def _render_volatility_tab(self) -> None:
        if not self._volatility_candles:
            self._render_placeholder(self.volatility_canvas, "暂无可用波动率数据。")
            return
        visible = self._visible_candles(self._volatility_candles)
        self._render_candle_chart(
            self.volatility_canvas,
            visible,
            full_candles=self._volatility_candles,
            title="波动率K线",
            subtitle="参考 Deribit 波动率指数窗口的双图逻辑。",
            axis_suffix="%",
        )

    def _render_overlay_tab(self) -> None:
        if not self._candles and not self._volatility_candles:
            self._render_placeholder(self.overlay_price_canvas, "暂无可用叠加对比数据。")
            self._render_placeholder(self.overlay_vol_canvas, "暂无可用叠加对比数据。")
            return
        if self._candles:
            self._render_candle_chart(
                self.overlay_price_canvas,
                self._visible_candles(self._candles),
                full_candles=self._candles,
                title="BTC价格K线",
                subtitle="上图固定显示价格K线，即使日线与波动率时间戳未完全重合也能复盘。",
                axis_suffix="",
            )
        else:
            self._render_placeholder(self.overlay_price_canvas, "暂无可用 BTC 价格K线。")
        if self._volatility_candles:
            self._render_candle_chart(
                self.overlay_vol_canvas,
                self._visible_candles(self._volatility_candles),
                full_candles=self._volatility_candles,
                title="波动率指数 / 历史波动率",
                subtitle="下图显示同视口下的波动率序列。",
                axis_suffix="%",
            )
        else:
            self._render_placeholder(self.overlay_vol_canvas, "暂无可用波动率K线。")

    def _render_candle_chart(
        self,
        canvas: Canvas,
        visible_candles: list[Candle],
        *,
        full_candles: list[Candle],
        title: str,
        subtitle: str,
        axis_suffix: str,
        show_drawings: bool = False,
        show_markers: bool = False,
    ) -> None:
        canvas.delete("all")
        width = max(canvas.winfo_width(), 720)
        height = max(canvas.winfo_height(), 300)
        canvas.create_rectangle(0, 0, width, height, outline="", fill="#ffffff")
        if not visible_candles:
            canvas.create_text(width / 2, height / 2, text="当前视口没有可显示的K线。", fill="#6b7280", font=("Microsoft YaHei UI", 11))
            return

        left = 64
        right = 24
        top = 28
        bottom = 42
        inner_width = width - left - right
        inner_height = height - top - bottom
        if inner_width <= 0 or inner_height <= 0:
            return

        highs = [float(item.high) for item in visible_candles]
        lows = [float(item.low) for item in visible_candles]
        max_price = max(highs)
        min_price = min(lows)
        if max_price == min_price:
            max_price += 1.0
            min_price -= 1.0
        else:
            pad = max((max_price - min_price) * 0.06, 0.01)
            max_price += pad
            min_price -= pad
        price_span = max(max_price - min_price, 1e-9)

        def x_for(index: int) -> float:
            if len(visible_candles) <= 1:
                return left + inner_width / 2
            return left + (index / max(len(visible_candles) - 1, 1)) * inner_width

        def y_for(price: float) -> float:
            return top + ((max_price - price) / price_span) * inner_height

        canvas.create_rectangle(left, top, width - right, height - bottom, outline="#d0d7de")
        for step_index in range(5):
            y = top + step_index * (inner_height / 4)
            price = max_price - step_index * (price_span / 4)
            canvas.create_line(left, y, width - right, y, fill="#edf0f3", dash=(2, 4))
            canvas.create_text(left - 8, y, text=f"{price:,.2f}{axis_suffix}", anchor="e", fill="#57606a", font=("Microsoft YaHei UI", 9))

        step = inner_width / max(len(visible_candles), 1)
        body_width = max(min(step * 0.56, 10), 2)
        for index, candle in enumerate(visible_candles):
            x = x_for(index)
            open_y = y_for(float(candle.open))
            close_y = y_for(float(candle.close))
            high_y = y_for(float(candle.high))
            low_y = y_for(float(candle.low))
            color = "#16a34a" if candle.close >= candle.open else "#dc2626"
            body_top = min(open_y, close_y)
            body_bottom = max(open_y, close_y)
            if abs(body_bottom - body_top) < 1:
                body_bottom = body_top + 1
            canvas.create_line(x, high_y, x, low_y, fill=color)
            canvas.create_rectangle(x - (body_width / 2), body_top, x + (body_width / 2), body_bottom, outline=color, fill=color)

        for index in _sample_time_indices(len(visible_candles)):
            x = x_for(index)
            canvas.create_line(x, top, x, height - bottom, fill="#eef2f7", dash=(2, 4))
            canvas.create_text(x, height - bottom + 16, text=_format_short_ts(visible_candles[index].ts), anchor="n", fill="#57606a")

        latest = visible_candles[-1]
        latest_y = y_for(float(latest.close))
        canvas.create_line(left, latest_y, width - right, latest_y, fill="#c69026", dash=(4, 3))
        canvas.create_text(width - right - 4, latest_y - 10, text=f"{latest.close}", anchor="e", fill="#9a6700", font=("Microsoft YaHei UI", 9, "bold"))
        canvas.create_text(left, 12, text=title, anchor="w", fill="#111827", font=("Microsoft YaHei UI", 10, "bold"))
        canvas.create_text(left, height - 14, text=subtitle, anchor="w", fill="#6b7280", font=("Microsoft YaHei UI", 9))

        if canvas is self.chart_canvas:
            self._active_chart_bounds = ChartBounds(left=left, top=top, right=width - right, bottom=height - bottom)
            self._active_chart_prices = (min_price, max_price)
        if show_markers and canvas is self.chart_canvas:
            self._draw_historical_markers(canvas, visible_candles, full_candles, x_for, y_for)
        if show_drawings and canvas is self.chart_canvas:
            self._draw_saved_annotations(canvas, visible_candles, full_candles, x_for, y_for)
            self._draw_pending_annotation(canvas, visible_candles, full_candles, x_for, y_for)

    def _render_placeholder(self, canvas: Canvas, message: str) -> None:
        canvas.delete("all")
        width = max(canvas.winfo_width(), 720)
        height = max(canvas.winfo_height(), 300)
        canvas.create_rectangle(0, 0, width, height, outline="", fill="#ffffff")
        canvas.create_rectangle(24, 24, width - 24, height - 24, outline="#d0d7de")
        canvas.create_text(width / 2, height / 2, text=message, fill="#6b7280", font=("Microsoft YaHei UI", 11))

    def _draw_saved_annotations(
        self,
        canvas: Canvas,
        visible_candles: list[Candle],
        full_candles: list[Candle],
        x_for: Callable[[int], float],
        y_for: Callable[[float], float],
    ) -> None:
        visible_start = _visible_start_index(self._viewport, len(full_candles), min_visible=36)
        visible_end = visible_start + len(visible_candles) - 1
        for annotation in self._drawings:
            self._draw_annotation(canvas, annotation, visible_start, visible_end, x_for, y_for, dashed=False)

    def _draw_pending_annotation(
        self,
        canvas: Canvas,
        visible_candles: list[Candle],
        full_candles: list[Candle],
        x_for: Callable[[int], float],
        y_for: Callable[[float], float],
    ) -> None:
        if self._pending_draw_start is None or self._pending_draw_end is None:
            return
        annotation = self._annotation_from_points(self._tool_key(), self._pending_draw_start, self._pending_draw_end)
        if annotation is None:
            return
        visible_start = _visible_start_index(self._viewport, len(full_candles), min_visible=36)
        visible_end = visible_start + len(visible_candles) - 1
        self._draw_annotation(canvas, annotation, visible_start, visible_end, x_for, y_for, dashed=True)

    def _draw_annotation(
        self,
        canvas: Canvas,
        annotation: DrawingAnnotation,
        visible_start: int,
        visible_end: int,
        x_for: Callable[[int], float],
        y_for: Callable[[float], float],
        *,
        dashed: bool,
    ) -> None:
        if annotation.end_index < visible_start or annotation.start_index > visible_end:
            return
        dash = (5, 4) if dashed else ()
        color = {
            "trend_line": "#2563eb",
            "horizontal_line": "#dc2626",
            "rectangle": "#0f766e",
            "parallel_channel": "#7c3aed",
        }.get(annotation.tool, "#2563eb")

        def local_x(global_index: int) -> float:
            return x_for(max(0, min(global_index - visible_start, visible_end - visible_start)))

        x1 = local_x(annotation.start_index)
        x2 = local_x(annotation.end_index)
        y1 = y_for(annotation.price_a)
        y2 = y_for(annotation.price_b)
        if annotation.tool == "horizontal_line":
            canvas.create_line(self._active_chart_bounds.left, y1, self._active_chart_bounds.right, y1, fill=color, width=2, dash=dash)
        elif annotation.tool == "trend_line":
            canvas.create_line(x1, y1, x2, y2, fill=color, width=2, dash=dash)
        elif annotation.tool == "rectangle":
            canvas.create_rectangle(x1, y1, x2, y2, outline=color, width=2, dash=dash)
        elif annotation.tool == "parallel_channel":
            top_y = min(y1, y2)
            bottom_y = max(y1, y2)
            canvas.create_line(x1, top_y, x2, top_y, fill=color, width=2, dash=dash)
            canvas.create_line(x1, bottom_y, x2, bottom_y, fill=color, width=2, dash=dash)
            canvas.create_line(x1, top_y, x1, bottom_y, fill=color, width=1, dash=dash)
            canvas.create_line(x2, top_y, x2, bottom_y, fill=color, width=1, dash=dash)

    def _draw_historical_markers(
        self,
        canvas: Canvas,
        visible_candles: list[Candle],
        full_candles: list[Candle],
        x_for: Callable[[int], float],
        y_for: Callable[[float], float],
    ) -> None:
        if not self._historical_markers:
            return
        visible_start = _visible_start_index(self._viewport, len(full_candles), min_visible=36)
        by_ts = {candle.ts: (visible_start + index, candle) for index, candle in enumerate(visible_candles)}
        for marker in self._historical_markers[-80:]:
            matched = _nearest_candle_for_marker(marker.candle_ts, visible_candles, visible_start)
            if matched is None:
                continue
            global_index, candle = matched
            local_index = global_index - visible_start
            x = x_for(local_index)
            y = y_for(float(candle.high)) - 12
            color = {"long": "#16a34a", "short": "#dc2626", "neutral": "#b45309"}.get(marker.direction, "#2563eb")
            canvas.create_oval(x - 4, y - 4, x + 4, y + 4, fill=color, outline=color)
            canvas.create_text(x + 7, y - 2, text=f"{marker.timeframe} {marker.direction} {marker.score}", anchor="w", fill=color, font=("Consolas", 8, "bold"))

    def _tool_key(self) -> DrawingTool:
        current_label = self.drawing_tool.get().strip()
        for key, label in DRAWING_TOOL_OPTIONS:
            if label == current_label:
                return key
        return "observe"

    def _on_chart_mousewheel(self, event) -> None:
        base = self._base_series_for_interaction()
        if not base:
            return
        canvas = event.widget
        width = max(canvas.winfo_width(), 720)
        left = 64
        right = 24
        inner_width = max(width - left - right, 1)
        anchor_ratio = min(max((float(getattr(event, "x", left)) - left) / inner_width, 0.0), 1.0)
        next_start, next_visible = _zoom_chart_viewport(
            start_index=self._viewport.start_index,
            visible_count=self._viewport.visible_count,
            total_count=len(base),
            anchor_ratio=anchor_ratio,
            zoom_in=getattr(event, "delta", 0) > 0,
            min_visible=36,
        )
        if next_start == self._viewport.start_index and next_visible == self._viewport.visible_count:
            return
        self._viewport.start_index = next_start
        self._viewport.visible_count = next_visible
        self._save_current_state()
        self._schedule_chart_redraw()

    def _on_chart_press(self, event) -> None:
        self._viewport_dirty = False
        canvas = event.widget
        if canvas is not self.chart_canvas:
            self._viewport.pan_anchor_x = int(getattr(event, "x", 0))
            self._viewport.pan_anchor_start = self._viewport.start_index
            self._drag_canvas = canvas
            return
        if self._active_chart_bounds is None or not self._active_chart_bounds.contains(event.x, event.y):
            self._viewport.pan_anchor_x = int(getattr(event, "x", 0))
            self._viewport.pan_anchor_start = self._viewport.start_index
            self._drag_canvas = canvas
            return
        tool = self._tool_key()
        if tool == "observe":
            self._viewport.pan_anchor_x = int(getattr(event, "x", 0))
            self._viewport.pan_anchor_start = self._viewport.start_index
            self._drag_canvas = canvas
            return
        if tool == "horizontal_line":
            annotation = self._annotation_from_points(tool, (event.x, event.y), (event.x, event.y))
            if annotation is not None:
                self._drawings.append(annotation)
                self._save_current_state()
                self._schedule_chart_redraw()
            return
        self._pending_draw_start = (event.x, event.y)
        self._pending_draw_end = (event.x, event.y)
        self._drag_canvas = None

    def _on_chart_drag(self, event) -> None:
        if self._pending_draw_start is not None:
            self._pending_draw_end = (event.x, event.y)
            self._schedule_chart_redraw()
            return
        base = self._base_series_for_interaction()
        if not base or self._viewport.pan_anchor_x is None:
            return
        canvas = event.widget
        width = max(canvas.winfo_width(), 720)
        left = 64
        right = 24
        inner_width = max(width - left - right, 1)
        visible_count = self._viewport.visible_count or len(base)
        step = inner_width / max(visible_count, 1)
        delta_px = int(getattr(event, "x", 0)) - self._viewport.pan_anchor_x
        index_delta = int(round(delta_px / max(step, 1)))
        self._viewport.start_index = _pan_chart_viewport(
            self._viewport.pan_anchor_start,
            visible_count,
            len(base),
            index_delta,
            min_visible=36,
        )
        self._viewport_dirty = True
        self._schedule_chart_redraw()

    def _on_chart_release(self, event) -> None:
        if self._pending_draw_start is not None:
            self._pending_draw_end = (event.x, event.y)
            annotation = self._annotation_from_points(self._tool_key(), self._pending_draw_start, self._pending_draw_end)
            self._pending_draw_start = None
            self._pending_draw_end = None
            if annotation is not None:
                self._drawings.append(annotation)
                self._save_current_state()
            self._schedule_chart_redraw()
        elif self._viewport_dirty:
            self._save_current_state()
            self._viewport_dirty = False
        self._viewport.pan_anchor_x = None
        self._drag_canvas = None

    def _annotation_from_points(
        self,
        tool: DrawingTool,
        start: tuple[float, float],
        end: tuple[float, float],
    ) -> DrawingAnnotation | None:
        if self._active_chart_bounds is None or self._active_chart_prices is None or not self._candles:
            return None
        start_index, start_price = self._chart_point_to_index_price(*start)
        end_index, end_price = self._chart_point_to_index_price(*end)
        if start_index is None or start_price is None or end_index is None or end_price is None:
            return None
        if tool == "horizontal_line":
            end_index = len(self._candles) - 1
            end_price = start_price
        elif tool in {"trend_line", "rectangle", "parallel_channel"} and abs(end_index - start_index) < 1 and abs(end_price - start_price) < 1e-6:
            return None
        start_idx = start_index
        end_idx = end_index
        if tool in {"rectangle", "parallel_channel"} and start_idx > end_idx:
            start_idx, end_idx = end_idx, start_idx
        return DrawingAnnotation(
            tool=tool,
            start_index=start_idx,
            end_index=end_idx,
            price_a=start_price,
            price_b=end_price,
        )

    def _chart_point_to_index_price(self, x: float, y: float) -> tuple[int | None, float | None]:
        bounds = self._active_chart_bounds
        price_range = self._active_chart_prices
        if bounds is None or price_range is None or not bounds.contains(x, y) or not self._candles:
            return None, None
        visible = self._visible_candles(self._candles)
        visible_start = _visible_start_index(self._viewport, len(self._candles), min_visible=36)
        min_price, max_price = price_range
        width = max(bounds.right - bounds.left, 1)
        height = max(bounds.bottom - bounds.top, 1)
        ratio_x = min(max((x - bounds.left) / width, 0.0), 1.0)
        local_index = int(round(ratio_x * max(len(visible) - 1, 0)))
        global_index = visible_start + local_index
        ratio_y = min(max((y - bounds.top) / height, 0.0), 1.0)
        price = max_price - ratio_y * (max_price - min_price)
        return global_index, price

    def _remove_last_drawing(self) -> None:
        if self._drawings:
            self._drawings.pop()
            self._save_current_state()
            self._schedule_chart_redraw()

    def _clear_drawings(self) -> None:
        self._drawings.clear()
        self._save_current_state()
        self._schedule_chart_redraw()

    def _reset_chart_view(self) -> None:
        base = self._base_series_for_interaction()
        if not base:
            return
        start_index, visible_count = _default_chart_viewport(len(base), min(220, len(base)), min_visible=36)
        self._viewport = ChartViewport(start_index=start_index, visible_count=visible_count)
        self._save_current_state()
        self._schedule_chart_redraw()

    def _visible_candles(self, candles: list[Candle]) -> list[Candle]:
        if not candles:
            return []
        start_index, visible_count = _normalize_chart_viewport(
            self._viewport.start_index,
            self._viewport.visible_count,
            len(candles),
            min_visible=36,
        )
        self._viewport.start_index = start_index
        self._viewport.visible_count = visible_count
        end_index = min(len(candles), start_index + visible_count)
        return candles[start_index:end_index]

    def _base_series_for_interaction(self) -> list[Candle]:
        return self._candles or self._volatility_candles

    def _save_current_state(self) -> None:
        key = self._state_key()
        snapshot = load_btc_research_workbench_state()
        drawings = dict(snapshot.get("drawings", {}))
        viewports = dict(snapshot.get("viewports", {}))
        drawings[key] = [annotation.__dict__ for annotation in self._drawings]
        viewports[key] = {
            "start_index": self._viewport.start_index,
            "visible_count": self._viewport.visible_count,
        }
        try:
            save_btc_research_workbench_state({"drawings": drawings, "viewports": viewports})
        except PermissionError as exc:
            self._logger(f"[BTC研究工作台] 状态保存被占用，已跳过本次写入 | {exc}")

    def _load_saved_state_for_current_view(self) -> None:
        key = self._state_key()
        snapshot = load_btc_research_workbench_state()
        raw_drawings = snapshot.get("drawings", {})
        raw_viewports = snapshot.get("viewports", {})
        self._drawings = []
        if isinstance(raw_drawings, dict):
            for item in raw_drawings.get(key, []) or []:
                if not isinstance(item, dict):
                    continue
                try:
                    self._drawings.append(
                        DrawingAnnotation(
                            tool=str(item.get("tool", "observe")),
                            start_index=int(item.get("start_index", 0) or 0),
                            end_index=int(item.get("end_index", 0) or 0),
                            price_a=float(item.get("price_a", 0.0) or 0.0),
                            price_b=float(item.get("price_b", 0.0) or 0.0),
                        )
                    )
                except (TypeError, ValueError):
                    continue
        if isinstance(raw_viewports, dict):
            payload = raw_viewports.get(key, {})
            if isinstance(payload, dict):
                try:
                    self._viewport = ChartViewport(
                        start_index=int(payload.get("start_index", 0) or 0),
                        visible_count=int(payload.get("visible_count")) if payload.get("visible_count") is not None else None,
                    )
                except (TypeError, ValueError):
                    self._viewport = ChartViewport()
            else:
                self._viewport = ChartViewport()

    def _state_key(self) -> str:
        timeframe = self.chart_timeframe.get().strip() or "4H"
        return f"BTC-USDT-SWAP|{timeframe}"

    def _load_deribit_volatility_from_cache(self, bar: str, *, requested_limit: int) -> list[Candle]:
        cache_path = deribit_volatility_cache_file_path()
        if not cache_path.exists():
            return []
        try:
            payload = json.loads(cache_path.read_text(encoding="utf-8"))
        except Exception:
            return []
        if not isinstance(payload, dict):
            return []
        item = payload.get("BTC|hourly_base")
        if not isinstance(item, dict):
            return []
        base_series: list[DeribitVolatilityCandle] = []
        for raw in item.get("volatility_hourly", []):
            if not isinstance(raw, dict):
                continue
            try:
                base_series.append(
                    DeribitVolatilityCandle(
                        ts=int(raw["ts"]),
                        open=Decimal(str(raw["open"])),
                        high=Decimal(str(raw["high"])),
                        low=Decimal(str(raw["low"])),
                        close=Decimal(str(raw["close"])),
                    )
                )
            except Exception:
                continue
        base_series.sort(key=lambda candle: candle.ts)
        if not base_series:
            return []
        resolution = DERIBIT_RESOLUTION_BY_BAR.get(bar, "3600")
        if resolution == "14400":
            base_series = _aggregate_deribit_candles(base_series, 14_400_000)
        elif resolution == "1D":
            base_series = _aggregate_deribit_candles(base_series, 86_400_000)
        if requested_limit > 0:
            base_series = base_series[-requested_limit:]
        return [_candle_from_deribit(item) for item in base_series]

    def _load_deribit_volatility_live(self, bar: str, *, requested_limit: int) -> list[Candle]:
        if self._deribit_client is None:
            return []
        resolution = DERIBIT_RESOLUTION_BY_BAR.get(bar, "3600")
        end_ts = int(datetime.now(timezone.utc).timestamp() * 1000)
        lookback_days = {"3600": 30, "14400": 90, "1D": 240}.get(resolution, 30)
        start_ts = int((datetime.now(timezone.utc) - timedelta(days=lookback_days)).timestamp() * 1000)
        try:
            candles = self._deribit_client.get_volatility_index_candles(
                "BTC",
                resolution,
                start_ts=start_ts,
                end_ts=end_ts,
                max_records=requested_limit,
            )
        except Exception:
            return []
        return [_candle_from_deribit(item) for item in candles]

    def _replace_text(self, widget: Text, content: str) -> None:
        widget.delete("1.0", END)
        widget.insert("1.0", content)


def _entry_payload(entry: JournalEntry) -> dict[str, object]:
    extraction = entry.extraction
    if extraction and isinstance(extraction.raw_payload, dict):
        return extraction.raw_payload
    return extraction.to_dict() if extraction else {}


def _format_local_time(value: datetime) -> str:
    target = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return target.astimezone().strftime("%Y-%m-%d %H:%M")


def _sample_time_indices(count: int) -> list[int]:
    if count <= 1:
        return [0]
    target = min(6, count)
    return sorted({min(count - 1, max(0, int(round(index * (count - 1) / max(target - 1, 1))))) for index in range(target)})


def _format_short_ts(ts: int) -> str:
    try:
        return datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime("%m-%d %H:%M")
    except Exception:
        return "-"


def _aggregate_deribit_candles(candles: list[DeribitVolatilityCandle], resolution_ms: int) -> list[DeribitVolatilityCandle]:
    grouped: list[list[DeribitVolatilityCandle]] = []
    current_bucket: int | None = None
    current_group: list[DeribitVolatilityCandle] = []
    for candle in candles:
        bucket = candle.ts // resolution_ms
        if current_bucket is None or bucket != current_bucket:
            if current_group:
                grouped.append(current_group)
            current_bucket = bucket
            current_group = [candle]
        else:
            current_group.append(candle)
    if current_group:
        grouped.append(current_group)
    aggregated: list[DeribitVolatilityCandle] = []
    for group in grouped:
        aggregated.append(
            DeribitVolatilityCandle(
                ts=group[0].ts,
                open=group[0].open,
                high=max(item.high for item in group),
                low=min(item.low for item in group),
                close=group[-1].close,
            )
        )
    return aggregated


def _candle_from_deribit(item: DeribitVolatilityCandle) -> Candle:
    return Candle(
        ts=item.ts,
        open=item.open,
        high=item.high,
        low=item.low,
        close=item.close,
        volume=Decimal("0"),
        confirmed=True,
    )


def _annualization_factor_for_bar(bar: str) -> float:
    periods_per_year = {
        "1H": 365 * 24,
        "4H": 365 * 6,
        "1D": 365,
    }.get(bar.strip())
    return math.sqrt(periods_per_year) if periods_per_year else 0.0


def _build_realized_volatility_from_reference(reference_candles: list[Candle], *, bar: str, lookback: int) -> list[Candle]:
    confirmed = [item for item in reference_candles if item.confirmed]
    if len(confirmed) < lookback + 1:
        return []
    annualization = _annualization_factor_for_bar(bar)
    if annualization <= 0:
        return []
    output: list[Candle] = []
    previous_close_vol: float | None = None
    for index in range(lookback, len(confirmed)):
        closes = [float(item.close) for item in confirmed[index - lookback : index + 1]]
        if any(value <= 0 for value in closes):
            continue
        returns = [math.log(closes[offset] / closes[offset - 1]) for offset in range(1, len(closes))]
        if not returns:
            continue
        mean_return = sum(returns) / len(returns)
        variance = sum((item - mean_return) ** 2 for item in returns) / len(returns)
        close_vol = math.sqrt(max(variance, 0.0)) * annualization * 100.0
        open_vol = previous_close_vol if previous_close_vol is not None else close_vol
        candle = confirmed[index]
        output.append(
            Candle(
                ts=candle.ts,
                open=Decimal(str(open_vol)),
                high=Decimal(str(max(open_vol, close_vol))),
                low=Decimal(str(min(open_vol, close_vol))),
                close=Decimal(str(close_vol)),
                volume=Decimal("0"),
                confirmed=candle.confirmed,
            )
        )
        previous_close_vol = close_vol
    return output


def _align_overlay_candles(price_candles: list[Candle], volatility_candles: list[Candle], *, bar: str) -> list[tuple[Candle, Candle]]:
    if not price_candles or not volatility_candles:
        return []
    bucket_ms = TIMEFRAME_MS.get(bar, 3_600_000)
    volatility_by_bucket = {item.ts // bucket_ms: item for item in volatility_candles}
    aligned: list[tuple[Candle, Candle]] = []
    for price in price_candles:
        bucket = price.ts // bucket_ms
        match = volatility_by_bucket.get(bucket)
        if match is not None:
            aligned.append((price, match))
    return aligned


def _looks_like_deribit_series(candles: list[Candle]) -> bool:
    return any(float(item.close) > 30.0 for item in candles[-16:])


def _normalize_chart_viewport(
    start_index: int,
    visible_count: int | None,
    total_count: int,
    *,
    min_visible: int,
) -> tuple[int, int]:
    if total_count <= 0:
        return 0, max(min_visible, 1)
    normalized_visible = visible_count if visible_count is not None else total_count
    normalized_visible = min(total_count, max(min_visible, normalized_visible))
    normalized_start = max(0, min(start_index, total_count - normalized_visible))
    return normalized_start, normalized_visible


def _default_chart_viewport(total_count: int, requested_limit: int, *, min_visible: int) -> tuple[int, int]:
    if total_count <= 0:
        return 0, max(min_visible, 1)
    normalized_visible = min(total_count, max(min_visible, requested_limit))
    normalized_start = max(0, total_count - normalized_visible)
    return normalized_start, normalized_visible


def _zoom_chart_viewport(
    *,
    start_index: int,
    visible_count: int | None,
    total_count: int,
    anchor_ratio: float,
    zoom_in: bool,
    min_visible: int,
) -> tuple[int, int]:
    current_start, current_visible = _normalize_chart_viewport(start_index, visible_count, total_count, min_visible=min_visible)
    if total_count <= 0:
        return current_start, current_visible
    step = max(1, int(round(current_visible * 0.18)))
    next_visible = current_visible - step if zoom_in else current_visible + step
    next_visible = max(min_visible, min(total_count, next_visible))
    anchor_index = current_start + int(round(anchor_ratio * max(current_visible - 1, 0)))
    next_start = anchor_index - int(round(anchor_ratio * max(next_visible - 1, 0)))
    return _normalize_chart_viewport(next_start, next_visible, total_count, min_visible=min_visible)


def _pan_chart_viewport(
    start_index: int,
    visible_count: int,
    total_count: int,
    index_delta: int,
    *,
    min_visible: int,
) -> int:
    next_start, _ = _normalize_chart_viewport(start_index + index_delta, visible_count, total_count, min_visible=min_visible)
    return next_start


def _visible_start_index(viewport: ChartViewport, total_count: int, *, min_visible: int) -> int:
    start_index, _ = _normalize_chart_viewport(viewport.start_index, viewport.visible_count, total_count, min_visible=min_visible)
    return start_index


def _nearest_candle_for_marker(
    candle_ts: int,
    visible_candles: list[Candle],
    visible_start: int,
) -> tuple[int, Candle] | None:
    nearest: tuple[int, Candle] | None = None
    nearest_delta: int | None = None
    for offset, candle in enumerate(visible_candles):
        delta = abs(int(candle.ts) - int(candle_ts))
        if nearest_delta is None or delta < nearest_delta:
            nearest = (visible_start + offset, candle)
            nearest_delta = delta
    return nearest if nearest_delta is not None and nearest_delta <= 3 * 86_400_000 else None


def _load_historical_analysis_markers(symbol: str, timeframe: str) -> list[HistoricalAnalysisMarker]:
    report_dir = analysis_report_dir_path()
    if not report_dir.exists():
        return []
    markers: list[HistoricalAnalysisMarker] = []
    for path in sorted(report_dir.glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        if str(payload.get("symbol", "") or "").strip().upper() != symbol.upper():
            continue
        raw_timeframes = payload.get("timeframes")
        if not isinstance(raw_timeframes, list):
            continue
        for item in raw_timeframes:
            if not isinstance(item, dict):
                continue
            if str(item.get("timeframe", "") or "").strip().upper() != timeframe.upper():
                continue
            candle_ts = item.get("candle_ts")
            try:
                normalized_ts = int(candle_ts)
            except (TypeError, ValueError):
                continue
            markers.append(
                HistoricalAnalysisMarker(
                    generated_at=str(payload.get("generated_at", "") or "").strip(),
                    timeframe=timeframe.upper(),
                    candle_ts=normalized_ts,
                    direction=str(item.get("direction", "") or "").strip(),
                    score=int(item.get("score", 0) or 0),
                    confidence=str(item.get("confidence", "") or "").strip(),
                )
            )
    markers.sort(key=lambda item: item.candle_ts)
    return markers
