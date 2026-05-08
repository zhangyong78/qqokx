from __future__ import annotations

import json
import math
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

try:
    from zoneinfo import ZoneInfo

    _CHINA_TZ = ZoneInfo("Asia/Shanghai")
except Exception:  # pragma: no cover - 极少数环境缺少 tz 数据
    _CHINA_TZ = timezone(timedelta(hours=8))
from decimal import Decimal
from pathlib import Path
from tkinter import END, Canvas, StringVar, Text, Toplevel, filedialog, messagebox
from tkinter import TclError
from tkinter import ttk
from typing import Any, Callable

from okx_quant.btc_market_analyzer import BtcMarketAnalyzerConfig, analyze_btc_market_at_time, save_btc_market_analysis
from okx_quant.market_analysis import MarketAnalysisConfig
from okx_quant.persistence import (
    analysis_report_dir_path,
    deribit_volatility_cache_file_path,
    load_btc_research_workbench_state,
    load_journal_entries_snapshot,
    save_journal_entries_snapshot,
    save_btc_research_workbench_state,
)
from okx_quant.deribit_client import DeribitVolatilityCandle
from okx_quant.journal import (
    JournalEntry,
    JournalExtractionResult,
    build_ai_extraction_prompt,
    create_journal_entry,
    extract_journal_locally,
    parse_ai_extraction_paste,
)
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
    price_c: float | None = None


@dataclass(frozen=True)
class HistoricalAnalysisMarker:
    generated_at: str
    timeframe: str
    candle_ts: int
    direction: str
    score: int
    confidence: str
    verdict: str = ""
    return_24h_pct: str = ""


@dataclass(frozen=True)
class ReplayStatsRow:
    signal_type: str
    samples: int
    effective: int
    partial: int
    invalid: int
    pending: int
    hit_rate: float
    avg_24h_return_pct: float | None


@dataclass
class ChartViewport:
    start_index: int = 0
    visible_count: int | None = None
    pan_anchor_x: int | None = None
    pan_anchor_start: int = 0


@dataclass(frozen=True)
class ChartRenderState:
    bounds: ChartBounds
    min_price: float
    max_price: float
    visible_start: int
    visible_end: int
    visible_slots: int
    candle_step: float
    timeframe_ms: int
    full_candles: tuple[Candle, ...]


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
        self._current_extraction: JournalExtractionResult | None = None
        self._attachment_paths: list[str] = []
        self._candles: list[Candle] = []
        self._volatility_candles: list[Candle] = []
        self._overlay_pairs: list[tuple[Candle, Candle]] = []
        self._drawings: list[DrawingAnnotation] = []
        self._historical_markers: list[HistoricalAnalysisMarker] = []
        self._chart_load_token = 0
        self._volatility_load_token = 0
        self._chart_render_token = 0
        self._displayed_chart_timeframe = "4H"
        self._viewport = ChartViewport()
        self._active_chart_bounds: ChartBounds | None = None
        self._active_chart_prices: tuple[float, float] | None = None
        self._active_visible_start = 0
        self._active_visible_end = 0
        self._active_visible_slots = 0
        self._pending_draw_start: tuple[float, float] | None = None
        self._pending_draw_end: tuple[float, float] | None = None
        self._drag_canvas: Canvas | None = None
        self._viewport_dirty = False
        self._is_panning = False
        self._render_state_by_canvas: dict[str, ChartRenderState] = {}
        self._chart_hover_indices: dict[int, int | None] = {}
        self._hover_canvas: Canvas | None = None
        self._hover_position: tuple[float, float] | None = None
        self._last_selected_chart_candle: Candle | None = None
        self._replay_request_token = 0

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
        self.attachment_text = StringVar(value="附件：-")
        self.preview_status_text = StringVar(value="未提炼")
        self.preview_source_text = StringVar(value="-")
        self.preview_symbol_text = StringVar(value="-")
        self.preview_timeframes_text = StringVar(value="-")
        self.preview_bias_text = StringVar(value="-")
        self.preview_action_text = StringVar(value="-")
        self.preview_verification_text = StringVar(value="-")
        self.preview_summary_text = StringVar(value="")
        self.replay_selection_text = StringVar(value="未选择K线")
        self.replay_stats_summary_text = StringVar(value="等待加载复盘统计。")
        self._replay_stats_tree: ttk.Treeview | None = None

        self._build_layout()
        self._load_entries(select_latest=True)
        self._refresh_replay_statistics()
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
        right.rowconfigure(4, weight=3)
        right.rowconfigure(7, weight=2)
        right.rowconfigure(10, weight=3)
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
        self._chart_timeframe_combo = ttk.Combobox(
            chart_header,
            textvariable=self.chart_timeframe,
            values=("1H", "4H", "1D"),
            width=7,
            state="readonly",
        )
        self._chart_timeframe_combo.grid(row=0, column=2)
        self._chart_timeframe_combo.bind("<<ComboboxSelected>>", self._on_chart_timeframe_selected)
        ttk.Button(chart_header, text="加载K线", command=self._load_chart_candles).grid(row=0, column=3, padx=(8, 0))
        ttk.Button(chart_header, text="重置视图", command=self._reset_chart_view).grid(row=0, column=4, padx=(8, 0))
        ttk.Button(chart_header, text="刷新分析锚点", command=self._reload_historical_markers).grid(row=0, column=5, padx=(8, 0))
        ttk.Button(chart_header, text="按当前K线复盘", command=self._replay_from_current_chart_point).grid(row=0, column=6, padx=(8, 0))

        notebook = ttk.Notebook(center)
        notebook.grid(row=1, column=0, sticky="nsew")
        notebook.bind("<<NotebookTabChanged>>", lambda _event: self._schedule_chart_redraw(), add="+")
        self.chart_notebook = notebook

        main_tab = ttk.Frame(notebook, padding=10)
        main_tab.columnconfigure(0, weight=1)
        main_tab.rowconfigure(1, weight=1)
        notebook.add(main_tab, text="BTC主图")
        self._main_tab = main_tab

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
        self._volatility_tab = vol_tab
        ttk.Label(vol_tab, textvariable=self.volatility_summary_text, justify="left").grid(row=0, column=0, sticky="w", pady=(0, 8))
        self.volatility_canvas = Canvas(vol_tab, background="#ffffff", highlightthickness=0, cursor="crosshair")
        self.volatility_canvas.grid(row=1, column=0, sticky="nsew")

        overlay_tab = ttk.Frame(notebook, padding=10)
        overlay_tab.columnconfigure(0, weight=1)
        overlay_tab.rowconfigure(1, weight=1)
        notebook.add(overlay_tab, text="叠加对比")
        self._overlay_tab = overlay_tab
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

        stats_tab = ttk.Frame(notebook, padding=10)
        stats_tab.columnconfigure(0, weight=1)
        stats_tab.rowconfigure(2, weight=1)
        notebook.add(stats_tab, text="复盘统计")
        self._stats_tab = stats_tab

        stats_header = ttk.Frame(stats_tab)
        stats_header.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        stats_header.columnconfigure(1, weight=1)
        ttk.Label(stats_header, text="当前选中K线").grid(row=0, column=0, sticky="w")
        ttk.Label(stats_header, textvariable=self.replay_selection_text, justify="left").grid(row=0, column=1, sticky="w", padx=(8, 0))
        ttk.Button(stats_header, text="刷新统计", command=self._refresh_replay_statistics).grid(row=0, column=2, padx=(8, 0))

        ttk.Label(stats_tab, textvariable=self.replay_stats_summary_text, justify="left", wraplength=860).grid(
            row=1,
            column=0,
            sticky="ew",
            pady=(0, 8),
        )

        replay_stats_tree = ttk.Treeview(
            stats_tab,
            columns=("signal_type", "samples", "effective", "partial", "invalid", "pending", "hit_rate", "avg24h"),
            show="headings",
            height=14,
        )
        replay_stats_tree.grid(row=2, column=0, sticky="nsew")
        for key, label, width in (
            ("signal_type", "信号类型", 180),
            ("samples", "样本数", 76),
            ("effective", "有效", 70),
            ("partial", "部分有效", 78),
            ("invalid", "无效", 70),
            ("pending", "待验证", 70),
            ("hit_rate", "命中率", 76),
            ("avg24h", "24H均值", 90),
        ):
            replay_stats_tree.heading(key, text=label)
            replay_stats_tree.column(key, width=width, anchor="center")
        self._replay_stats_tree = replay_stats_tree

        for canvas in (self.chart_canvas, self.volatility_canvas, self.overlay_price_canvas, self.overlay_vol_canvas):
            canvas.bind("<Configure>", lambda _event: self._schedule_chart_redraw(), add="+")
            canvas.bind("<MouseWheel>", self._on_chart_mousewheel, add="+")
            canvas.bind("<Motion>", self._on_chart_motion, add="+")
            canvas.bind("<Leave>", self._on_chart_leave, add="+")
            canvas.bind("<Button-1>", self._on_chart_press, add="+")
            canvas.bind("<B1-Motion>", self._on_chart_drag, add="+")
            canvas.bind("<ButtonRelease-1>", self._on_chart_release, add="+")
        self.chart_canvas.bind("<Double-Button-1>", self._on_main_chart_double_click, add="+")
        for canvas in (self.volatility_canvas, self.overlay_price_canvas, self.overlay_vol_canvas):
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
        ttk.Button(detail_header, text="新建日记", command=self._new_entry).grid(row=0, column=1, padx=(8, 0))
        ttk.Button(detail_header, text="保存日记", command=self._save_diary_append_new_sample).grid(row=0, column=2, padx=(8, 0))
        ttk.Button(detail_header, text="更新当前样本", command=self._update_selected_journal_entry).grid(row=0, column=3, padx=(8, 0))
        ttk.Label(right, textvariable=self.detail_meta_text).grid(row=1, column=0, sticky="w")

        journal_actions = ttk.Frame(right)
        journal_actions.grid(row=2, column=0, sticky="ew", pady=(6, 8))
        ttk.Button(journal_actions, text="本地提炼", command=self._extract_local).grid(row=0, column=0)
        ttk.Button(journal_actions, text="复制AI提示词", command=self._copy_ai_prompt).grid(row=0, column=1, padx=(8, 0))
        ttk.Button(journal_actions, text="导入AI JSON", command=self._import_ai_paste).grid(row=0, column=2, padx=(8, 0))
        ttk.Button(journal_actions, text="添加截图", command=self._add_attachment).grid(row=0, column=3, padx=(8, 0))
        ttk.Button(journal_actions, text="复制结构化JSON", command=self._copy_structured_json).grid(row=0, column=4, padx=(8, 0))

        ttk.Label(right, text="原始随笔", font=("Microsoft YaHei UI", 10, "bold")).grid(row=3, column=0, sticky="w")
        self.raw_text = Text(right, height=9, wrap="word", font=("Microsoft YaHei UI", 10))
        self.raw_text.grid(row=4, column=0, sticky="nsew", pady=(4, 6))
        ttk.Label(right, textvariable=self.attachment_text).grid(row=5, column=0, sticky="w", pady=(0, 8))

        ttk.Label(right, text="AI 提炼粘贴区", font=("Microsoft YaHei UI", 10, "bold")).grid(row=6, column=0, sticky="w")
        self.ai_text = Text(right, height=6, wrap="word", font=("Consolas", 9))
        self.ai_text.grid(row=7, column=0, sticky="nsew", pady=(4, 8))

        preview_frame = ttk.LabelFrame(right, text="提炼结果", padding=8)
        preview_frame.grid(row=8, column=0, sticky="ew")
        preview_frame.columnconfigure(1, weight=1)
        preview_rows = (
            ("状态", self.preview_status_text),
            ("来源", self.preview_source_text),
            ("标的", self.preview_symbol_text),
            ("周期", self.preview_timeframes_text),
            ("方向", self.preview_bias_text),
            ("动作", self.preview_action_text),
            ("验证", self.preview_verification_text),
            ("摘要", self.preview_summary_text),
        )
        for index, (label, variable) in enumerate(preview_rows):
            ttk.Label(preview_frame, text=label).grid(row=index, column=0, sticky="nw", padx=(0, 8), pady=1)
            ttk.Label(preview_frame, textvariable=variable, justify="left", wraplength=360).grid(row=index, column=1, sticky="nw", pady=1)

        ttk.Label(right, text="结构化 JSON", font=("Microsoft YaHei UI", 10, "bold")).grid(row=9, column=0, sticky="w", pady=(8, 0))
        self.json_text = Text(right, height=12, wrap="none", font=("Consolas", 9))
        self.json_text.grid(row=10, column=0, sticky="nsew", pady=(4, 0))

    def _load_entries(self, *, select_latest: bool, auto_select: bool = True) -> None:
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
        if auto_select and self._entries and (select_latest or not self._selected_entry_id):
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
        self._current_extraction = entry.extraction
        self._attachment_paths = list(entry.attachments)
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
        self._replace_text(self.ai_text, "")
        self._replace_text(self.json_text, json.dumps(payload, ensure_ascii=False, indent=2) if payload else "")
        self.attachment_text.set(_format_attachment_text(self._attachment_paths))
        self._set_preview(extraction)

    def _new_entry(self) -> None:
        self._selected_entry_id = ""
        self._current_extraction = None
        self._attachment_paths = []
        self.detail_title_text.set("新建日记")
        self.detail_meta_text.set("-")
        self._replace_text(self.raw_text, "")
        self._replace_text(self.ai_text, "")
        self._replace_text(self.json_text, "")
        self.attachment_text.set(_format_attachment_text(self._attachment_paths))
        self._set_preview(None)
        self.status_text.set("已新建空白行情日记。")

    def _extract_local(self) -> None:
        raw_text = self._current_raw_text()
        if not raw_text.strip():
            messagebox.showinfo("提示", "请先输入行情随笔。", parent=self.window)
            return
        extraction = extract_journal_locally(raw_text)
        self._current_extraction = extraction
        self._set_preview(extraction)
        self.status_text.set("已完成本地提炼。")

    def _copy_ai_prompt(self) -> None:
        raw_text = self._current_raw_text()
        if not raw_text.strip():
            messagebox.showinfo("提示", "请先输入行情随笔。", parent=self.window)
            return
        prompt = build_ai_extraction_prompt(raw_text)
        self.window.clipboard_clear()
        self.window.clipboard_append(prompt)
        self.status_text.set("AI 提示词已复制到剪贴板。")

    def _import_ai_paste(self) -> None:
        content = self.ai_text.get("1.0", END).strip()
        if not content:
            messagebox.showinfo("提示", "请先粘贴 AI 输出的 JSON。", parent=self.window)
            return
        try:
            extraction = parse_ai_extraction_paste(content)
        except Exception as exc:
            messagebox.showerror("导入失败", f"AI JSON 解析失败：{exc}", parent=self.window)
            return
        self._current_extraction = extraction
        self._set_preview(extraction)
        self.status_text.set("已导入 AI 提炼结果。")

    def _save_diary_append_new_sample(self) -> None:
        """日记式保存：每次在左侧追加一行新样本，不覆盖当前选中项。"""
        raw_text = self._current_raw_text()
        if not raw_text.strip():
            messagebox.showinfo("提示", "请先输入行情随笔。", parent=self.window)
            return
        entry = create_journal_entry(
            raw_text,
            attachments=tuple(self._attachment_paths),
            extraction=self._current_extraction,
        )
        self._entries.insert(0, entry)
        save_journal_entries_snapshot([item.to_dict() for item in self._entries])
        self._load_entries(select_latest=False, auto_select=False)
        self._selected_entry_id = ""
        self._new_entry()
        self.status_text.set(f"已追加 1 条研究样本（共 {len(self._entries)} 条），可继续书写下一条。")

    def _update_selected_journal_entry(self) -> None:
        """覆盖保存：仅更新左侧当前选中的那条样本。"""
        if not self._selected_entry_id.strip():
            messagebox.showinfo("提示", "请先在左侧选中要更新的样本。", parent=self.window)
            return
        raw_text = self._current_raw_text()
        if not raw_text.strip():
            messagebox.showinfo("提示", "请先输入行情随笔。", parent=self.window)
            return
        existing = next((item for item in self._entries if item.entry_id == self._selected_entry_id), None)
        if existing is None:
            messagebox.showinfo("提示", "选中的样本已不存在，请先点「刷新」再试。", parent=self.window)
            return
        now = datetime.now(timezone.utc)
        entry = JournalEntry(
            entry_id=existing.entry_id,
            raw_text=raw_text,
            created_at=existing.created_at,
            updated_at=now,
            attachments=tuple(self._attachment_paths),
            status="review" if self._current_extraction else existing.status,
            extraction=self._current_extraction,
            notes=existing.notes,
        )
        self._entries = [entry if item.entry_id == existing.entry_id else item for item in self._entries]
        save_journal_entries_snapshot([item.to_dict() for item in self._entries])
        self._load_entries(select_latest=False)
        if self._selected_entry_id and self.sample_tree.exists(self._selected_entry_id):
            self.sample_tree.selection_set(self._selected_entry_id)
            self.sample_tree.see(self._selected_entry_id)
            selected = next((item for item in self._entries if item.entry_id == self._selected_entry_id), None)
            if selected is not None:
                self._select_entry(selected)
        self.status_text.set("当前样本已更新。")

    def _add_attachment(self) -> None:
        selected = filedialog.askopenfilenames(
            parent=self.window,
            title="选择截图或附件",
            filetypes=(
                ("图片", "*.png;*.jpg;*.jpeg;*.webp;*.bmp"),
                ("所有文件", "*.*"),
            ),
        )
        if not selected:
            return
        for path in selected:
            normalized = str(Path(path))
            if normalized not in self._attachment_paths:
                self._attachment_paths.append(normalized)
        self.attachment_text.set(_format_attachment_text(self._attachment_paths))
        self.status_text.set(f"已添加 {len(selected)} 个附件。")

    def _copy_structured_json(self) -> None:
        content = self.json_text.get("1.0", END).strip()
        if not content:
            messagebox.showinfo("提示", "当前还没有结构化 JSON。", parent=self.window)
            return
        self.window.clipboard_clear()
        self.window.clipboard_append(content)
        self.status_text.set("结构化 JSON 已复制到剪贴板。")

    def _set_preview(self, extraction: JournalExtractionResult | None) -> None:
        if extraction is None:
            self.preview_status_text.set("未提炼")
            self.preview_source_text.set("-")
            self.preview_symbol_text.set("-")
            self.preview_timeframes_text.set("-")
            self.preview_bias_text.set("-")
            self.preview_action_text.set("-")
            self.preview_verification_text.set("-")
            self.preview_summary_text.set("")
            return
        payload = extraction.raw_payload if isinstance(extraction.raw_payload, dict) else {}
        self.preview_status_text.set("待确认" if extraction.needs_review else "已结构化")
        self.preview_source_text.set(_source_label(extraction.source))
        self.preview_symbol_text.set(extraction.inst_id or extraction.symbol or "-")
        self.preview_timeframes_text.set(" / ".join(extraction.timeframes) or "-")
        self.preview_bias_text.set(_bias_label(extraction.bias))
        self.preview_action_text.set(_action_label(extraction.planned_action))
        self.preview_verification_text.set(_verification_windows(payload) or "-")
        self.preview_summary_text.set(extraction.summary or _hypothesis_statement(payload) or "")

    def _on_chart_timeframe_selected(self, _event: object | None = None) -> None:
        if self._client is None:
            self._schedule_chart_redraw()
            return
        self._load_chart_candles()

    def _effective_chart_bar(self) -> str:
        return (getattr(self, "_displayed_chart_timeframe", "") or "").strip() or self.chart_timeframe.get().strip() or "4H"

    def _load_chart_candles(self) -> None:
        if self._client is None:
            self.chart_status_text.set("未接入 OKX 行情客户端，当前只能显示静态工作台。")
            self._schedule_chart_redraw()
            return
        timeframe = self.chart_timeframe.get().strip() or "4H"
        self._chart_load_token += 1
        token = self._chart_load_token
        self._candles = []
        self._volatility_candles = []
        self._overlay_pairs = []
        self._schedule_chart_redraw()
        self.chart_status_text.set(f"正在加载 BTC-USDT-SWAP {timeframe} K线...")

        def worker() -> None:
            bar = self.chart_timeframe.get().strip() or "4H"
            try:
                candles = self._client.get_candles_history("BTC-USDT-SWAP", bar, limit=520)
            except Exception as exc:
                self.window.after(0, lambda e=str(exc): self._apply_chart_error(token, e))
                return
            self.window.after(0, lambda: self._apply_chart_candles(token, bar, candles))

        threading.Thread(target=worker, daemon=True).start()

    def _apply_chart_candles(self, token: int, timeframe: str, candles: list[Candle]) -> None:
        if token != self._chart_load_token:
            return
        confirmed_only = [candle for candle in candles if candle.confirmed]
        self._candles = confirmed_only if confirmed_only else list(candles)
        self._displayed_chart_timeframe = timeframe
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
        self._volatility_load_token += 1
        token = self._volatility_load_token

        def worker() -> None:
            requested_limit = max(self._viewport.visible_count or 220, 220)
            bar = self._effective_chart_bar()
            volatility = self._load_deribit_volatility_from_cache(bar, requested_limit=requested_limit)
            if not volatility:
                volatility = self._load_deribit_volatility_live(bar, requested_limit=requested_limit)
            if not volatility and self._candles:
                volatility = _build_realized_volatility_from_reference(self._candles, bar=bar, lookback=20)
            self.window.after(0, lambda: self._apply_volatility_series(token, volatility))

        threading.Thread(target=worker, daemon=True).start()

    def _apply_volatility_series(self, token: int, candles: list[Candle]) -> None:
        if token != self._volatility_load_token:
            return
        self._volatility_candles = candles
        bar = self._effective_chart_bar()
        self._overlay_pairs = _align_overlay_candles(self._candles, self._volatility_candles, bar=bar)
        if self._volatility_candles:
            latest = self._volatility_candles[-1]
            source_name = "Deribit 波动率指数" if _looks_like_deribit_series(self._volatility_candles) else "程序历史波动率"
            self.volatility_summary_text.set(
                f"{source_name} | 周期 {bar} | 根数 {len(self._volatility_candles)} | 最新 C {latest.close:.2f}"
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
        timeframe = self._effective_chart_bar()
        self._historical_markers = _load_historical_analysis_markers("BTC-USDT-SWAP", timeframe)
        self._update_signal_text()
        self._refresh_replay_statistics()
        self._schedule_chart_redraw()

    def _refresh_replay_statistics(self) -> None:
        timeframe = self._effective_chart_bar()
        summary, rows = _load_replay_statistics("BTC-USDT-SWAP", timeframe)
        self.replay_stats_summary_text.set(summary)
        if self._replay_stats_tree is None:
            return
        self._replay_stats_tree.delete(*self._replay_stats_tree.get_children())
        for row in rows:
            self._replay_stats_tree.insert(
                "",
                END,
                values=(
                    row.signal_type,
                    row.samples,
                    row.effective,
                    row.partial,
                    row.invalid,
                    row.pending,
                    f"{row.hit_rate:.1f}%",
                    "-" if row.avg_24h_return_pct is None else f"{row.avg_24h_return_pct:.2f}%",
                ),
            )

    def _replay_from_current_chart_point(self) -> None:
        if self._client is None:
            messagebox.showinfo("提示", "当前未接入行情客户端，无法发起复盘。", parent=self.window)
            return
        candle = self._current_hovered_chart_candle()
        if candle is None:
            messagebox.showinfo("提示", "请先把十字光标移动到主图的某根K线上。", parent=self.window)
            return
        timeframe = self._effective_chart_bar()
        analysis_dt = datetime.fromtimestamp(int(candle.ts) / 1000.0, tz=timezone.utc).astimezone(_CHINA_TZ)
        config = BtcMarketAnalyzerConfig(
            timeframes=(timeframe,),
            history_limits=((timeframe, 5000),),
            probability_config=MarketAnalysisConfig(direction_mode="close_to_close"),
        )
        self._replay_request_token += 1
        request_token = self._replay_request_token
        self.status_text.set(f"正在按 {timeframe} { _format_short_ts(int(candle.ts)) } 发起复盘，请稍候。")
        self.replay_selection_text.set(f"{timeframe} | {_format_short_ts(int(candle.ts))} | C {candle.close}")

        def worker() -> None:
            try:
                analysis = analyze_btc_market_at_time(
                    self._client,
                    symbol="BTC-USDT-SWAP",
                    analysis_dt=analysis_dt,
                    config=config,
                )
                report_path = save_btc_market_analysis(analysis)
                self.window.after(0, lambda: self._on_replay_completed(request_token, analysis, report_path))
            except Exception as exc:
                self.window.after(0, lambda error=exc: self._on_replay_failed(request_token, error))

        threading.Thread(target=worker, daemon=True, name="btc-research-replay").start()

    def _on_main_chart_double_click(self, _event=None) -> None:
        self._replay_from_current_chart_point()

    def _on_replay_completed(self, request_token: int, analysis: Any, report_path: Path) -> None:
        if request_token != self._replay_request_token:
            return
        self.status_text.set(
            f"复盘完成 | {_format_short_ts(_analysis_primary_candle_ts(analysis) or 0)} | 方向 {analysis.direction} | 评分 {analysis.score}"
        )
        self._reload_historical_markers()
        self._refresh_replay_statistics()
        self._logger(f"[BTC研究工作台] 复盘完成 | 报告={report_path}")

    def _on_replay_failed(self, request_token: int, exc: Exception) -> None:
        if request_token != self._replay_request_token:
            return
        self.status_text.set(f"复盘失败：{exc}")
        self._logger(f"[BTC研究工作台] 复盘失败 | {exc}")
        messagebox.showerror("复盘失败", f"按当前K线发起复盘时出错：\n{exc}", parent=self.window)

    def _current_hovered_chart_candle(self) -> Candle | None:
        state = self._render_state_by_canvas.get(str(self.chart_canvas))
        if state is not None:
            hovered_index = self._chart_hover_indices.get(id(self.chart_canvas))
            if hovered_index is not None:
                candles = list(state.full_candles)
                if 0 <= hovered_index < len(candles):
                    candle = candles[hovered_index]
                    self._last_selected_chart_candle = candle
                    return candle
        return self._last_selected_chart_candle

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
        active_tab = self._active_chart_tab()
        if active_tab == "volatility":
            self._render_volatility_tab()
            return
        if active_tab == "overlay":
            self._render_overlay_tab()
            return
        self._render_main_chart()

    def _active_chart_tab(self) -> str:
        try:
            selected = str(self.chart_notebook.select())
        except TclError:
            return "main"
        if selected == str(self._volatility_tab):
            return "volatility"
        if selected == str(self._overlay_tab):
            return "overlay"
        return "main"

    def _render_main_chart(self) -> None:
        if not self._candles:
            self._render_placeholder(self.chart_canvas, "加载 BTC K 线后，这里显示主图、历史分析锚点和人工划线。")
            return
        visible, visible_start, visible_end = self._visible_window(self._candles, persist=True)
        bar = self._effective_chart_bar()
        self._render_candle_chart(
            self.chart_canvas,
            visible,
            full_candles=self._candles,
            visible_start=visible_start,
            visible_slots=max(visible_end - visible_start, 0),
            base_index=visible_start,
            timeframe_ms=TIMEFRAME_MS.get(bar, 14_400_000),
            title=f"BTC-USDT-SWAP {bar} | 主图",
            subtitle="历史分析锚点 / 趋势线 / 水平线 / 矩形 / 平行通道",
            axis_suffix="",
            show_drawings=True,
            show_markers=True,
        )

    def _render_volatility_tab(self) -> None:
        if not self._volatility_candles:
            self._render_placeholder(self.volatility_canvas, "暂无可用波动率数据。")
            return
        visible, visible_start, visible_end = self._visible_window(self._volatility_candles, persist=False)
        bar = self._effective_chart_bar()
        self._render_candle_chart(
            self.volatility_canvas,
            visible,
            full_candles=self._volatility_candles,
            visible_start=visible_start,
            visible_slots=max(visible_end - visible_start, 0),
            base_index=visible_start,
            timeframe_ms=TIMEFRAME_MS.get(bar, 14_400_000),
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
            visible, visible_start, visible_end = self._visible_window(self._candles, persist=False)
            bar = self._effective_chart_bar()
            self._render_candle_chart(
                self.overlay_price_canvas,
                visible,
                full_candles=self._candles,
                visible_start=visible_start,
                visible_slots=max(visible_end - visible_start, 0),
                base_index=visible_start,
                timeframe_ms=TIMEFRAME_MS.get(bar, 14_400_000),
                title="BTC价格K线",
                subtitle="上图固定显示价格K线，即使日线与波动率时间戳未完全重合也能复盘。",
                axis_suffix="",
            )
        else:
            self._render_placeholder(self.overlay_price_canvas, "暂无可用 BTC 价格K线。")
        if self._volatility_candles:
            visible, visible_start, visible_end = self._visible_window(self._volatility_candles, persist=False)
            bar = self._effective_chart_bar()
            self._render_candle_chart(
                self.overlay_vol_canvas,
                visible,
                full_candles=self._volatility_candles,
                visible_start=visible_start,
                visible_slots=max(visible_end - visible_start, 0),
                base_index=visible_start,
                timeframe_ms=TIMEFRAME_MS.get(bar, 14_400_000),
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
        visible_start: int,
        visible_slots: int,
        base_index: int,
        timeframe_ms: int,
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

        def x_for(index: float) -> float:
            if visible_slots <= 1:
                return left + inner_width / 2
            return left + (index * (inner_width / max(visible_slots, 1))) + ((inner_width / max(visible_slots, 1)) / 2)

        def y_for(price: float) -> float:
            return top + ((max_price - price) / price_span) * inner_height

        canvas.create_rectangle(left, top, width - right, height - bottom, outline="#d0d7de")
        for step_index in range(5):
            y = top + step_index * (inner_height / 4)
            price = max_price - step_index * (price_span / 4)
            canvas.create_line(left, y, width - right, y, fill="#edf0f3", dash=(2, 4))
            canvas.create_text(left - 8, y, text=f"{price:,.2f}{axis_suffix}", anchor="e", fill="#57606a", font=("Microsoft YaHei UI", 9))

        candle_step = inner_width / max(visible_slots, 1)
        body_width = max(min(candle_step * 0.6, 10), 2)
        for index, candle in enumerate(visible_candles):
            global_index = base_index + index
            x = x_for(global_index - visible_start)
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

        visible_end = visible_start + visible_slots
        for global_index in _chart_time_label_indices(visible_start, visible_end, target_labels=6):
            x = x_for(global_index - visible_start)
            canvas.create_line(x, top, x, height - bottom, fill="#eef2f7", dash=(2, 4))
            canvas.create_text(
                x,
                height - bottom + 16,
                text=_format_short_ts(full_candles[global_index].ts),
                anchor="n",
                fill="#57606a",
            )

        latest = full_candles[-1]
        latest_y = y_for(float(latest.close))
        canvas.create_line(left, latest_y, width - right, latest_y, fill="#c69026", dash=(4, 3))
        latest_slot_index = max(0, min((len(full_candles) - 1) - visible_start, visible_slots - 1))
        latest_x = x_for(latest_slot_index)
        canvas.create_text(
            min(max(latest_x + 8, left + 8), width - right - 4),
            latest_y - 10,
            text=f"{latest.close}",
            anchor="w",
            fill="#9a6700",
            font=("Microsoft YaHei UI", 9, "bold"),
        )
        canvas.create_text(left, 12, text=title, anchor="w", fill="#111827", font=("Microsoft YaHei UI", 10, "bold"))
        canvas.create_text(left, height - 14, text=subtitle, anchor="w", fill="#6b7280", font=("Microsoft YaHei UI", 9))

        if canvas is self.chart_canvas:
            self._active_chart_bounds = ChartBounds(left=left, top=top, right=width - right, bottom=height - bottom)
            self._active_chart_prices = (min_price, max_price)
            self._active_visible_start = visible_start
            self._active_visible_end = visible_end
            self._active_visible_slots = visible_slots
        self._render_state_by_canvas[str(canvas)] = ChartRenderState(
            bounds=ChartBounds(left=left, top=top, right=width - right, bottom=height - bottom),
            min_price=min_price,
            max_price=max_price,
            visible_start=visible_start,
            visible_end=visible_end,
            visible_slots=visible_slots,
            candle_step=candle_step,
            timeframe_ms=timeframe_ms,
            full_candles=tuple(full_candles),
        )
        if show_markers and canvas is self.chart_canvas:
            self._draw_historical_markers(canvas, visible_candles, visible_start, base_index, x_for, y_for)
        if show_drawings and canvas is self.chart_canvas:
            self._draw_saved_annotations(canvas, visible_start, visible_slots, x_for, y_for)
            self._draw_pending_annotation(canvas, visible_start, visible_slots, x_for, y_for)
        if self._hover_canvas is canvas and self._hover_position is not None:
            self._draw_crosshair_overlay(canvas)

    def _render_placeholder(self, canvas: Canvas, message: str) -> None:
        canvas.delete("all")
        self._render_state_by_canvas.pop(str(canvas), None)
        width = max(canvas.winfo_width(), 720)
        height = max(canvas.winfo_height(), 300)
        canvas.create_rectangle(0, 0, width, height, outline="", fill="#ffffff")
        canvas.create_rectangle(24, 24, width - 24, height - 24, outline="#d0d7de")
        canvas.create_text(width / 2, height / 2, text=message, fill="#6b7280", font=("Microsoft YaHei UI", 11))

    def _draw_saved_annotations(
        self,
        canvas: Canvas,
        visible_start: int,
        visible_slots: int,
        x_for: Callable[[float], float],
        y_for: Callable[[float], float],
    ) -> None:
        visible_end = visible_start + visible_slots - 1
        for annotation in self._drawings:
            self._draw_annotation(canvas, annotation, visible_start, visible_end, x_for, y_for, dashed=False)

    def _draw_pending_annotation(
        self,
        canvas: Canvas,
        visible_start: int,
        visible_slots: int,
        x_for: Callable[[float], float],
        y_for: Callable[[float], float],
    ) -> None:
        if self._pending_draw_start is None or self._pending_draw_end is None:
            return
        annotation = self._annotation_from_points(self._tool_key(), self._pending_draw_start, self._pending_draw_end)
        if annotation is None:
            return
        visible_end = visible_start + visible_slots - 1
        self._draw_annotation(canvas, annotation, visible_start, visible_end, x_for, y_for, dashed=True)

    def _draw_annotation(
        self,
        canvas: Canvas,
        annotation: DrawingAnnotation,
        visible_start: int,
        visible_end: int,
        x_for: Callable[[float], float],
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
            return x_for(max(0.0, min(global_index - visible_start, visible_end - visible_start)))

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
            width_px = abs(y_for(annotation.price_a + (annotation.price_c or 0.0)) - y1)
            dx = x2 - x1
            dy = y2 - y1
            line_length = math.hypot(dx, dy)
            if line_length <= 1e-6:
                return
            normal_x = -dy / line_length
            normal_y = dx / line_length
            offset_x = normal_x * width_px
            offset_y = normal_y * width_px
            upper_start = (x1 + offset_x, y1 + offset_y)
            upper_end = (x2 + offset_x, y2 + offset_y)
            lower_end = (x2 - offset_x, y2 - offset_y)
            lower_start = (x1 - offset_x, y1 - offset_y)
            fill_color = "#c7d7fe" if not dashed else "#dde7ff"
            canvas.create_polygon(
                upper_start[0],
                upper_start[1],
                upper_end[0],
                upper_end[1],
                lower_end[0],
                lower_end[1],
                lower_start[0],
                lower_start[1],
                outline="",
                fill=fill_color,
                stipple="gray25",
            )
            canvas.create_line(*upper_start, *upper_end, fill=color, width=2, dash=dash)
            canvas.create_line(*lower_start, *lower_end, fill=color, width=2, dash=dash)
            canvas.create_line(x1, y1, x2, y2, fill=color, width=1, dash=(5, 4))
            self._draw_channel_handle(canvas, *upper_start, color)
            self._draw_channel_handle(canvas, *upper_end, color)
            self._draw_channel_handle(canvas, *lower_start, color)
            self._draw_channel_handle(canvas, *lower_end, color)
            self._draw_channel_handle(canvas, (upper_start[0] + upper_end[0]) / 2, (upper_start[1] + upper_end[1]) / 2, color, square=True)
            self._draw_channel_handle(canvas, (lower_start[0] + lower_end[0]) / 2, (lower_start[1] + lower_end[1]) / 2, color, square=True)

    def _draw_channel_handle(self, canvas: Canvas, x: float, y: float, color: str, *, square: bool = False) -> None:
        radius = 4
        if square:
            canvas.create_rectangle(
                x - radius,
                y - radius,
                x + radius,
                y + radius,
                outline=color,
                fill="#ffffff",
                width=2,
            )
            return
        canvas.create_oval(
            x - radius,
            y - radius,
            x + radius,
            y + radius,
            outline=color,
            fill="#ffffff",
            width=2,
        )

    def _draw_historical_markers(
        self,
        canvas: Canvas,
        visible_candles: list[Candle],
        visible_start: int,
        base_index: int,
        x_for: Callable[[float], float],
        y_for: Callable[[float], float],
    ) -> None:
        if not self._historical_markers:
            return
        for marker in self._historical_markers[-80:]:
            matched = _nearest_candle_for_marker(marker.candle_ts, visible_candles, base_index)
            if matched is None:
                continue
            global_index, candle = matched
            local_index = global_index - visible_start
            x = x_for(local_index)
            y = y_for(float(candle.high)) - 12
            color = {"long": "#16a34a", "short": "#dc2626", "neutral": "#b45309"}.get(marker.direction, "#2563eb")
            canvas.create_oval(x - 4, y - 4, x + 4, y + 4, fill=color, outline=color)
            marker_text = f"{marker.timeframe} {marker.direction} {marker.score}"
            if marker.verdict:
                marker_text += f" | {marker.verdict}"
            if marker.return_24h_pct:
                marker_text += f" | 24H {marker.return_24h_pct}%"
            canvas.create_text(x + 7, y - 2, text=marker_text, anchor="w", fill=color, font=("Consolas", 8, "bold"))

    def _on_chart_motion(self, event) -> None:
        if self._is_panning:
            return
        canvas = event.widget
        self._hover_canvas = canvas
        self._hover_position = (float(getattr(event, "x", 0.0)), float(getattr(event, "y", 0.0)))
        state = self._render_state_by_canvas.get(str(canvas))
        if state is None:
            return
        hover_index = _chart_hover_index_for_x(
            x=float(getattr(event, "x", -1.0)),
            left=int(state.bounds.left),
            width=int(state.bounds.right - state.bounds.left),
            start_index=state.visible_start,
            end_index=state.visible_end,
            candle_step=state.candle_step,
        )
        current = self._chart_hover_indices.get(id(canvas))
        if current == hover_index:
            return
        self._chart_hover_indices[id(canvas)] = hover_index
        if canvas is self.chart_canvas:
            candle = self._current_hovered_chart_candle()
            if candle is not None:
                self._last_selected_chart_candle = candle
                self.replay_selection_text.set(
                    f"{self._effective_chart_bar()} | {_format_short_ts(int(candle.ts))} | O {candle.open} C {candle.close}"
                )
        self._draw_crosshair_overlay(canvas)

    def _on_chart_leave(self, event) -> None:
        canvas = event.widget
        if self._is_panning and canvas is self._drag_canvas:
            return
        if self._hover_canvas is canvas:
            self._hover_canvas = None
            self._hover_position = None
        self._chart_hover_indices[id(canvas)] = None
        canvas.delete("crosshair")

    def _draw_crosshair_overlay(self, canvas: Canvas) -> None:
        canvas.delete("crosshair")
        state = self._render_state_by_canvas.get(str(canvas))
        if state is None or self._hover_canvas is not canvas or self._hover_position is None:
            return
        x, y = self._hover_position
        bounds = state.bounds
        if not bounds.contains(x, y):
            return
        width = max(canvas.winfo_width(), 720)
        price_range = max(state.max_price - state.min_price, 1e-9)
        ratio_y = min(max((y - bounds.top) / max(bounds.bottom - bounds.top, 1), 0.0), 1.0)
        global_index = self._chart_hover_indices.get(id(canvas))
        if global_index is None:
            return
        snapped_x = bounds.left + ((global_index - state.visible_start) * state.candle_step) + (state.candle_step / 2)
        price = state.max_price - ratio_y * price_range
        ts = _slot_timestamp(list(state.full_candles), global_index, state.timeframe_ms)
        price_text = f"{price:,.2f}"
        time_text = _format_short_ts(ts)

        canvas.create_line(snapped_x, bounds.top, snapped_x, bounds.bottom, fill="#94a3b8", dash=(2, 4), tags="crosshair")
        canvas.create_line(bounds.left, y, bounds.right, y, fill="#94a3b8", dash=(2, 4), tags="crosshair")

        price_box_left = bounds.right + 4
        price_box_top = y - 10
        price_box_right = width - 4
        price_box_bottom = y + 10
        canvas.create_rectangle(
            price_box_left,
            price_box_top,
            price_box_right,
            price_box_bottom,
            outline="#1f2937",
            fill="#1f2937",
            tags="crosshair",
        )
        canvas.create_text(
            (price_box_left + price_box_right) / 2,
            y,
            text=price_text,
            fill="#ffffff",
            font=("Consolas", 9, "bold"),
            tags="crosshair",
        )

        time_box_half = 48
        time_left = max(bounds.left, min(snapped_x - time_box_half, bounds.right - time_box_half * 2))
        time_right = min(bounds.right, time_left + time_box_half * 2)
        time_top = bounds.bottom + 4
        time_bottom = time_top + 18
        canvas.create_rectangle(
            time_left,
            time_top,
            time_right,
            time_bottom,
            outline="#1f2937",
            fill="#1f2937",
            tags="crosshair",
        )
        canvas.create_text(
            (time_left + time_right) / 2,
            (time_top + time_bottom) / 2,
            text=time_text,
            fill="#ffffff",
            font=("Consolas", 8, "bold"),
            tags="crosshair",
        )

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
        current_visible = self._viewport.visible_count
        next_start, next_visible = _zoom_chart_viewport(
            start_index=self._viewport.start_index,
            visible_count=current_visible,
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
        self._is_panning = False
        canvas = event.widget
        if canvas is not self.chart_canvas:
            self._viewport.pan_anchor_x = int(getattr(event, "x", 0))
            self._viewport.pan_anchor_start = self._viewport.start_index
            self._drag_canvas = canvas
            self._is_panning = True
            canvas.delete("crosshair")
            try:
                canvas.grab_set()
            except TclError:
                pass
            return
        if self._active_chart_bounds is None or not self._active_chart_bounds.contains(event.x, event.y):
            self._viewport.pan_anchor_x = int(getattr(event, "x", 0))
            self._viewport.pan_anchor_start = self._viewport.start_index
            self._drag_canvas = canvas
            self._is_panning = True
            canvas.delete("crosshair")
            try:
                canvas.grab_set()
            except TclError:
                pass
            return
        tool = self._tool_key()
        if tool == "observe":
            self._viewport.pan_anchor_x = int(getattr(event, "x", 0))
            self._viewport.pan_anchor_start = self._viewport.start_index
            self._drag_canvas = canvas
            self._is_panning = True
            canvas.delete("crosshair")
            try:
                canvas.grab_set()
            except TclError:
                pass
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
        if not self._is_panning:
            return
        base = self._base_series_for_interaction()
        if not base or self._viewport.pan_anchor_x is None or event.widget is not self._drag_canvas:
            return
        canvas = event.widget
        width = max(canvas.winfo_width(), 720)
        left = 64
        right = 24
        inner_width = max(width - left - right, 1)
        _, visible_count = _normalize_chart_viewport(
            self._viewport.start_index,
            self._viewport.visible_count,
            len(base),
            min_visible=36,
        )
        candle_step = inner_width / max(visible_count, 1)
        current_x = int(getattr(event, "x", self._viewport.pan_anchor_x))
        shift = int(round((self._viewport.pan_anchor_x - current_x) / max(candle_step, 1)))
        next_start = _pan_chart_viewport(
            self._viewport.pan_anchor_start,
            visible_count,
            len(base),
            shift,
            min_visible=36,
        )
        if abs(next_start - self._viewport.start_index) < 1e-6:
            return
        self._viewport.start_index = next_start
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
        self._is_panning = False
        self._viewport.pan_anchor_x = None
        if self._drag_canvas is not None:
            try:
                self._drag_canvas.grab_release()
            except TclError:
                pass
            self._hover_canvas = self._drag_canvas
            self._hover_position = (float(getattr(event, "x", 0.0)), float(getattr(event, "y", 0.0)))
            state = self._render_state_by_canvas.get(str(self._drag_canvas))
            if state is not None:
                self._chart_hover_indices[id(self._drag_canvas)] = _chart_hover_index_for_x(
                    x=float(getattr(event, "x", -1.0)),
                    left=int(state.bounds.left),
                    width=int(state.bounds.right - state.bounds.left),
                    start_index=state.visible_start,
                    end_index=state.visible_end,
                    candle_step=state.candle_step,
                )
            self._draw_crosshair_overlay(self._drag_canvas)
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
        price_c = None
        if tool == "parallel_channel":
            price_c = self._price_offset_for_pixels(34)
        return DrawingAnnotation(
            tool=tool,
            start_index=start_idx,
            end_index=end_idx,
            price_a=start_price,
            price_b=end_price,
            price_c=price_c,
        )

    def _chart_point_to_index_price(self, x: float, y: float) -> tuple[int | None, float | None]:
        bounds = self._active_chart_bounds
        price_range = self._active_chart_prices
        if bounds is None or price_range is None or not bounds.contains(x, y) or not self._candles:
            return None, None
        visible_start = self._active_visible_start
        visible_end = self._active_visible_end
        visible_slots = max(self._active_visible_slots, 1)
        min_price, max_price = price_range
        width = max(bounds.right - bounds.left, 1)
        height = max(bounds.bottom - bounds.top, 1)
        candle_step = width / max(visible_slots, 1)
        global_index = _chart_hover_index_for_x(
            x=float(x),
            left=int(bounds.left),
            width=int(width),
            start_index=visible_start,
            end_index=visible_end,
            candle_step=candle_step,
        )
        if global_index is None:
            return None, None
        ratio_y = min(max((y - bounds.top) / height, 0.0), 1.0)
        price = max_price - ratio_y * (max_price - min_price)
        return global_index, price

    def _price_offset_for_pixels(self, pixels: float) -> float:
        bounds = self._active_chart_bounds
        price_range = self._active_chart_prices
        if bounds is None or price_range is None:
            return 0.0
        min_price, max_price = price_range
        height = max(bounds.bottom - bounds.top, 1.0)
        return max((max_price - min_price) * (pixels / height), 1e-6)

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
        start_index, visible_count = _default_chart_viewport(
            len(base),
            len(base),
            min_visible=36,
        )
        self._viewport = ChartViewport(start_index=start_index, visible_count=visible_count)
        self._save_current_state()
        self._schedule_chart_redraw()

    def _visible_window(self, candles: list[Candle], *, persist: bool) -> tuple[list[Candle], int, int]:
        if not candles:
            return [], 0, 0
        start_index, visible_count = _normalize_chart_viewport(
            self._viewport.start_index,
            self._viewport.visible_count,
            len(candles),
            min_visible=36,
        )
        if persist:
            self._viewport.start_index = start_index
            self._viewport.visible_count = visible_count
        end_index = min(len(candles), start_index + visible_count)
        return candles[start_index:end_index], start_index, end_index

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
                            price_c=float(item.get("price_c")) if item.get("price_c") is not None else None,
                        )
                    )
                except (TypeError, ValueError):
                    continue
        if isinstance(raw_viewports, dict):
            payload = raw_viewports.get(key, {})
            if isinstance(payload, dict):
                try:
                    self._viewport = ChartViewport(
                        start_index=int(float(payload.get("start_index", 0) or 0.0)),
                        visible_count=int(payload.get("visible_count")) if payload.get("visible_count") is not None else None,
                    )
                except (TypeError, ValueError):
                    self._viewport = ChartViewport()
            else:
                self._viewport = ChartViewport()

    def _state_key(self) -> str:
        timeframe = self.chart_timeframe.get().strip() or "4H"
        return f"BTC-USDT-SWAP|{timeframe}"

    def _current_raw_text(self) -> str:
        return self.raw_text.get("1.0", END).strip()

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
        # 始终拉 1 小时序列，再用 _aggregate_deribit_candles 按北京时间合成 1H/4H/1D，
        # 避免 Deribit 原生 4H/1D 使用 UTC 边界与 OKX 中国区习惯不一致。
        if resolution == "14400":
            hourly_records = min(20_000, max(requested_limit * 8, 960))
        elif resolution == "1D":
            hourly_records = min(20_000, max(requested_limit * 28, 2_000))
        else:
            hourly_records = min(20_000, max(requested_limit, 500))
        try:
            hourly = self._deribit_client.get_volatility_index_candles(
                "BTC",
                "3600",
                start_ts=start_ts,
                end_ts=end_ts,
                max_records=hourly_records,
            )
        except Exception:
            return []
        if not hourly:
            return []
        agg_ms = {"3600": 3_600_000, "14400": 14_400_000, "1D": 86_400_000}.get(resolution, 3_600_000)
        merged = _aggregate_deribit_candles(hourly, agg_ms)
        if requested_limit > 0:
            merged = merged[-requested_limit:]
        return [_candle_from_deribit(item) for item in merged]

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


def _bias_label(value: str) -> str:
    return {
        "long": "偏多",
        "short": "偏空",
        "neutral": "震荡/观察",
        "unknown": "待确认",
    }.get(value, value or "待确认")


def _source_label(value: str) -> str:
    return {
        "local_rules": "本地规则",
        "ai_paste": "AI 粘贴",
        "api": "API",
    }.get(value, value or "-")


def _action_label(value: str) -> str:
    return {
        "open_long": "准备做多",
        "open_short": "准备做空",
        "observe": "继续观察",
        "unknown": "待确认",
    }.get(value, value or "待确认")


def _hypothesis_statement(payload: dict[str, object]) -> str:
    hypothesis = payload.get("hypothesis")
    if not isinstance(hypothesis, dict):
        return ""
    return str(hypothesis.get("statement", "") or "").strip()


def _verification_windows(payload: dict[str, object]) -> str:
    verification_plan = payload.get("verification_plan")
    if not isinstance(verification_plan, dict):
        return ""
    windows = verification_plan.get("review_windows")
    if isinstance(windows, list):
        return " / ".join(str(item).strip() for item in windows if str(item).strip())
    return str(windows or "").strip()


def _format_attachment_text(paths: list[str]) -> str:
    if not paths:
        return "附件：-"
    if len(paths) == 1:
        return f"附件：{paths[0]}"
    return f"附件：{len(paths)} 个文件"


def _sample_time_indices(count: int) -> list[int]:
    if count <= 1:
        return [0]
    target = min(6, count)
    return sorted({min(count - 1, max(0, int(round(index * (count - 1) / max(target - 1, 1))))) for index in range(target)})


def _format_short_ts(ts: int) -> str:
    """K 线横轴时间：按北京时间（Asia/Shanghai）显示，与 OKX 中国区习惯一致。"""
    try:
        dt_utc = datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc)
        return dt_utc.astimezone(_CHINA_TZ).strftime("%m-%d %H:%M")
    except Exception:
        return "-"


def _slot_timestamp(candles: list[Candle], slot_index: float | int, timeframe_ms: int) -> int:
    if not candles:
        return 0
    numeric_index = max(float(slot_index), 0.0)
    discrete_index = int(round(numeric_index))
    if discrete_index < len(candles):
        return int(candles[discrete_index].ts)
    last_ts = int(candles[-1].ts)
    extra_steps = discrete_index - (len(candles) - 1)
    return last_ts + extra_steps * max(timeframe_ms, 1)


def _deribit_volatility_bucket_start_ms(ts_ms: int, resolution_ms: int) -> int:
    """Deribit 小时波动率聚合成 1H/4H/1D 时，按北京时间（UTC+8）对齐 K 线边界。"""
    if resolution_ms not in (3_600_000, 14_400_000, 86_400_000):
        return (ts_ms // resolution_ms) * resolution_ms
    dt_utc = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
    local = dt_utc.astimezone(_CHINA_TZ)
    if resolution_ms == 3_600_000:
        floored = local.replace(minute=0, second=0, microsecond=0)
    elif resolution_ms == 14_400_000:
        floored = local.replace(minute=0, second=0, microsecond=0)
        floored = floored.replace(hour=(floored.hour // 4) * 4)
    else:
        floored = local.replace(hour=0, minute=0, second=0, microsecond=0)
    return int(floored.astimezone(timezone.utc).timestamp() * 1000)


def _aggregate_deribit_candles(candles: list[DeribitVolatilityCandle], resolution_ms: int) -> list[DeribitVolatilityCandle]:
    if not candles:
        return []
    buckets: dict[int, list[DeribitVolatilityCandle]] = {}
    for candle in sorted(candles, key=lambda item: item.ts):
        key = _deribit_volatility_bucket_start_ms(int(candle.ts), resolution_ms)
        buckets.setdefault(key, []).append(candle)
    aggregated: list[DeribitVolatilityCandle] = []
    for key in sorted(buckets):
        group = buckets[key]
        aggregated.append(
            DeribitVolatilityCandle(
                ts=key,
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
        return 0, 0
    normalized_min_visible = max(1, min(min_visible, total_count))
    normalized_visible = total_count if visible_count is None else max(normalized_min_visible, min(visible_count, total_count))
    max_start = max(total_count - normalized_visible, 0)
    normalized_start = max(0, min(start_index, max_start))
    return normalized_start, normalized_visible


def _default_chart_viewport(total_count: int, requested_limit: int, *, min_visible: int) -> tuple[int, int]:
    normalized_start, normalized_visible = _normalize_chart_viewport(0, requested_limit, total_count, min_visible=min_visible)
    if total_count <= 0:
        return normalized_start, normalized_visible
    max_start = max(total_count - normalized_visible, 0)
    return max_start, normalized_visible


def _zoom_chart_viewport(
    *,
    start_index: int,
    visible_count: int | None,
    total_count: int,
    anchor_ratio: float,
    zoom_in: bool,
    min_visible: int,
) -> tuple[int, int]:
    current_start, current_visible = _normalize_chart_viewport(
        start_index,
        visible_count,
        total_count,
        min_visible=min_visible,
    )
    if total_count <= 0:
        return 0, 0
    factor = 0.8 if zoom_in else 1.25
    target_visible = int(round(current_visible * factor))
    min_count = max(1, min(min_visible, total_count))
    target_visible = max(min_count, min(target_visible, total_count))
    if target_visible == current_visible:
        return current_start, current_visible
    clamped_ratio = min(max(anchor_ratio, 0.0), 1.0)
    anchor_index = current_start + (current_visible * clamped_ratio)
    target_start = int(round(anchor_index - (target_visible * clamped_ratio)))
    return _normalize_chart_viewport(target_start, target_visible, total_count, min_visible=min_visible)


def _pan_chart_viewport(
    start_index: int,
    visible_count: int,
    total_count: int,
    index_delta: int,
    *,
    min_visible: int,
 ) -> int:
    normalized_start, normalized_visible = _normalize_chart_viewport(
        start_index,
        visible_count,
        total_count,
        min_visible=min_visible,
    )
    next_start, _ = _normalize_chart_viewport(
        normalized_start + index_delta,
        normalized_visible,
        total_count,
        min_visible=min_visible,
    )
    return next_start


def _chart_time_label_indices(start_index: int, end_index: int, *, target_labels: int = 6) -> list[int]:
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


def _chart_hover_index_for_x(
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
        validation = payload.get("validation") if isinstance(payload.get("validation"), dict) else {}
        verdict = str(validation.get("verdict", "") or "").strip()
        return_24h_pct = ""
        windows = validation.get("windows")
        if isinstance(windows, list):
            for window in windows:
                if not isinstance(window, dict):
                    continue
                if int(window.get("hours", 0) or 0) != 24:
                    continue
                return_24h_pct = str(window.get("return_pct", "") or "").strip()
                break
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
                    verdict=verdict,
                    return_24h_pct=return_24h_pct,
                )
            )
    markers.sort(key=lambda item: item.candle_ts)
    return markers


def _analysis_primary_candle_ts(analysis: Any) -> int | None:
    for item in getattr(analysis, "timeframes", ()) or ():
        candle_ts = getattr(item, "candle_ts", None)
        if candle_ts is not None:
            try:
                return int(candle_ts)
            except (TypeError, ValueError):
                return None
    return None


def _load_replay_statistics(symbol: str, timeframe: str) -> tuple[str, list[ReplayStatsRow]]:
    report_dir = analysis_report_dir_path()
    if not report_dir.exists():
        return "暂无复盘统计样本。", []
    records: list[dict[str, object]] = []
    for path in sorted(report_dir.glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        if str(payload.get("symbol", "") or "").strip().upper() != symbol.upper():
            continue
        if str(payload.get("mode", "") or "").strip() != "historical_replay":
            continue
        validation = payload.get("validation")
        if not isinstance(validation, dict):
            continue
        tf_payload = _matching_timeframe_payload(payload, timeframe)
        if tf_payload is None:
            continue
        records.append(
            {
                "signal_type": _primary_signal_type_from_report(payload, tf_payload),
                "verdict": str(validation.get("verdict", "") or "").strip() or "pending",
                "return_24h": _validation_24h_return(validation),
            }
        )
    if not records:
        return f"当前 {timeframe} 暂无复盘样本。", []

    by_signal: dict[str, list[dict[str, object]]] = {}
    for record in records:
        by_signal.setdefault(str(record["signal_type"]), []).append(record)

    rows: list[ReplayStatsRow] = []
    for signal_type, items in by_signal.items():
        effective = sum(1 for item in items if item["verdict"] == "effective")
        partial = sum(1 for item in items if item["verdict"] == "partially_effective")
        invalid = sum(1 for item in items if item["verdict"] == "invalid")
        pending = sum(1 for item in items if item["verdict"] not in {"effective", "partially_effective", "invalid", "mixed"})
        mixed = sum(1 for item in items if item["verdict"] == "mixed")
        invalid += mixed
        completed = max(len(items) - pending, 0)
        hit_rate = ((effective + partial) / completed * 100.0) if completed > 0 else 0.0
        returns = [float(item["return_24h"]) for item in items if isinstance(item.get("return_24h"), (int, float))]
        avg_return = (sum(returns) / len(returns)) if returns else None
        rows.append(
            ReplayStatsRow(
                signal_type=signal_type,
                samples=len(items),
                effective=effective,
                partial=partial,
                invalid=invalid,
                pending=pending,
                hit_rate=hit_rate,
                avg_24h_return_pct=avg_return,
            )
        )

    rows.sort(key=lambda item: (item.samples, item.hit_rate, 0.0 if item.avg_24h_return_pct is None else item.avg_24h_return_pct), reverse=True)
    total = len(records)
    total_effective = sum(item.effective for item in rows)
    total_partial = sum(item.partial for item in rows)
    total_pending = sum(item.pending for item in rows)
    completed_total = max(total - total_pending, 0)
    overall_hit = ((total_effective + total_partial) / completed_total * 100.0) if completed_total > 0 else 0.0
    summary = (
        f"{timeframe} 复盘样本 {total} 条 | 已完成 {completed_total} 条 | 命中率 {overall_hit:.1f}% | "
        f"有效 {total_effective} | 部分有效 {total_partial} | 待验证 {total_pending}"
    )
    return summary, rows


def _matching_timeframe_payload(payload: dict[str, object], timeframe: str) -> dict[str, object] | None:
    raw_timeframes = payload.get("timeframes")
    if not isinstance(raw_timeframes, list):
        return None
    for item in raw_timeframes:
        if not isinstance(item, dict):
            continue
        if str(item.get("timeframe", "") or "").strip().upper() == timeframe.upper():
            return item
    return None


def _primary_signal_type_from_report(payload: dict[str, object], timeframe_payload: dict[str, object]) -> str:
    raw_signals = timeframe_payload.get("signals")
    if isinstance(raw_signals, list):
        for item in raw_signals:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name", "") or "").strip()
            category = str(item.get("category", "") or "").strip()
            if category == "resonance":
                continue
            if name:
                return f"{category}:{name}" if category else name
    top_level = payload.get("signals")
    if isinstance(top_level, list):
        for item in top_level:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name", "") or "").strip()
            category = str(item.get("category", "") or "").strip()
            if category == "resonance":
                continue
            if name:
                return f"{category}:{name}" if category else name
    return "unknown"


def _validation_24h_return(validation: dict[str, object]) -> float | None:
    windows = validation.get("windows")
    if not isinstance(windows, list):
        return None
    for item in windows:
        if not isinstance(item, dict):
            continue
        if int(item.get("hours", 0) or 0) != 24:
            continue
        try:
            return float(item.get("return_pct"))
        except (TypeError, ValueError):
            return None
    return None
