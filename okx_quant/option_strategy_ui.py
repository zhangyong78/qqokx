from __future__ import annotations

import threading
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
import math
from tkinter import BOTH, END, BooleanVar, Canvas, DoubleVar, StringVar, Toplevel, simpledialog
from tkinter import messagebox, ttk
from typing import Any, Callable

from okx_quant.log_utils import ensure_log_timestamp
from okx_quant.models import Candle, Instrument
from okx_quant.okx_client import OkxPosition, OkxRestClient, OkxTicker
from okx_quant.option_strategy import (
    OptionChainRow,
    OptionQuote,
    ResolvedStrategyLeg,
    StrategyLegDefinition,
    StrategyPayoffPoint,
    StrategyPayoffSnapshot,
    build_composite_candles,
    build_default_formula,
    build_option_chain_rows,
    build_payoff_snapshot,
    build_simulated_payoff_snapshot,
    convert_candles_by_reference,
    convert_payoff_snapshot_to_usdt,
    estimate_leg_greeks,
    evaluate_linear_formula,
    format_option_expiry_label,
    infer_implied_volatility_for_leg,
    option_contract_value,
    parse_linear_formula,
    parse_option_contract,
    parse_option_expiry_datetime,
    resolve_strategy_leg,
)
from okx_quant.persistence import load_option_strategies_snapshot, save_option_strategies_snapshot
from okx_quant.pricing import decimal_places_for_increment, format_decimal, format_decimal_by_increment, format_decimal_fixed
from okx_quant.window_layout import apply_adaptive_window_geometry


Logger = Callable[[str], None]
BAR_OPTIONS = ["1m", "3m", "5m", "15m", "1H", "4H"]
DEFAULT_OPTION_FAMILY_OPTIONS = ("BTC-USD", "ETH-USD")
MAX_OPTION_COMBO_CANDLES = 2000


@dataclass(frozen=True)
class ChartBounds:
    left: float
    top: float
    right: float
    bottom: float

    def contains(self, x: float, y: float) -> bool:
        return self.left <= x <= self.right and self.top <= y <= self.bottom


@dataclass(frozen=True)
class PayoffChartHoverState:
    bounds: ChartBounds
    primary_points: tuple[StrategyPayoffPoint, ...]
    reference_points: tuple[StrategyPayoffPoint, ...]
    x_positions: tuple[float, ...]
    primary_y_positions: tuple[float, ...]
    reference_y_positions: tuple[float, ...]
    value_ccy: str
    primary_label: str
    reference_label: str
    primary_color: str
    reference_color: str


@dataclass(frozen=True)
class ComboChartHoverState:
    bounds: ChartBounds
    candles: tuple[Candle, ...]
    x_positions: tuple[float, ...]
    close_y_positions: tuple[float, ...]
    candle_step: float
    value_ccy: str


@dataclass
class KlineChartViewState:
    start_index: int = 0
    visible_count: int | None = None
    auto_full: bool = True


class OptionStrategyCalculatorWindow:
    def __init__(
        self,
        parent,
        client: OkxRestClient,
        *,
        runtime_provider: Callable[[], Any | None] | None = None,
        logger: Logger | None = None,
    ) -> None:
        self.client = client
        self.runtime_provider = runtime_provider
        self.logger = logger

        self.window = Toplevel(parent)
        self.window.title("期权策略计算器")
        apply_adaptive_window_geometry(
            self.window,
            width_ratio=0.9,
            height_ratio=0.88,
            min_width=1380,
            min_height=940,
            max_width=1880,
            max_height=1260,
        )
        self.window.protocol("WM_DELETE_WINDOW", self.window.withdraw)

        self.status_text = StringVar(value="正在加载期权系列...")
        self.strategy_name = StringVar()
        self.saved_strategy_name = StringVar()
        self.option_family = StringVar()
        self.expiry_code = StringVar()
        self.default_quantity = StringVar(value="1")
        self.bar = StringVar(value="1H")
        self.candle_limit = StringVar(value="1000")
        self.chart_display_ccy = StringVar(value="结算币")
        self.combo_hide_wicks = BooleanVar(value=False)
        self.payoff_time_progress = DoubleVar(value=100.0)
        self.payoff_vol_shift_percent = DoubleVar(value=0.0)
        self.payoff_sim_date_text = StringVar(value="估值日 -")
        self.payoff_vol_shift_text = StringVar(value="波动率平移 0%")
        self.formula = StringVar()
        self.chain_selection_text = StringVar(value="选择一个行权价后，可把认购 / 认沽直接加入策略腿。")
        self.strategy_summary_text = StringVar(value="暂无策略腿。")
        self.payoff_summary_text = StringVar(value="加入策略腿后，可生成到期盈亏图。")
        self.combo_summary_text = StringVar(value="组合 K 线采用 OKX 期权标记价格。")
        self.volatility_summary_text = StringVar(value="波动率 K 线基于标的现货历史收盘收益率估算。")
        self.overlay_summary_text = StringVar(value="叠加图使用左轴显示组合K线，右轴显示历史波动率K线。")

        self._family_combo: ttk.Combobox | None = None
        self._expiry_combo: ttk.Combobox | None = None
        self._saved_strategy_combo: ttk.Combobox | None = None
        self._chain_frame: ttk.LabelFrame | None = None
        self._chain_tree: ttk.Treeview | None = None
        self._legs_tree: ttk.Treeview | None = None
        self._payoff_canvas: Canvas | None = None
        self._combo_canvas: Canvas | None = None
        self._big_chart_window: Toplevel | None = None
        self._big_chart_notebook: ttk.Notebook | None = None
        self._big_payoff_canvas: Canvas | None = None
        self._big_combo_canvas: Canvas | None = None
        self._big_volatility_canvas: Canvas | None = None
        self._big_overlay_combo_canvas: Canvas | None = None
        self._big_overlay_volatility_canvas: Canvas | None = None
        self._big_chart_redraw_after_id: str | None = None

        self._all_option_instruments: list[Instrument] = []
        self._family_instruments_cache: dict[str, list[Instrument]] = {}
        self._family_tickers_cache: dict[str, dict[str, OkxTicker]] = {}
        self._instrument_map: dict[str, Instrument] = {}
        self._quotes_by_inst_id: dict[str, OptionQuote] = {}
        self._chain_rows: list[OptionChainRow] = []
        self._legs: list[StrategyLegDefinition] = []
        self._saved_strategies: list[dict[str, object]] = []
        self._current_underlying_price: Decimal | None = None
        self._latest_spot_usdt_price: Decimal | None = None
        self._latest_spot_usdt_candles: list[Candle] = []
        self._latest_combo_candles: list[Candle] = []
        self._latest_payoff_snapshot: StrategyPayoffSnapshot | None = None
        self._latest_expiry_payoff_snapshot: StrategyPayoffSnapshot | None = None
        self._latest_combo_value: Decimal | None = None
        self._latest_combo_requested_limit: int | None = None
        self._latest_combo_source_counts: dict[str, int] = {}
        self._latest_chart_formula = ""
        self._latest_resolved_legs: list[ResolvedStrategyLeg] = []
        self._latest_implied_volatility_by_alias: dict[str, Decimal] = {}
        self._latest_payoff_loaded_at: datetime | None = None
        self._latest_payoff_expiry_at: datetime | None = None
        self._payoff_hover_state: PayoffChartHoverState | None = None
        self._big_payoff_hover_state: PayoffChartHoverState | None = None
        self._combo_hover_state: ComboChartHoverState | None = None
        self._big_combo_hover_state: ComboChartHoverState | None = None
        self._big_volatility_hover_state: ComboChartHoverState | None = None
        self._big_overlay_combo_hover_state: ComboChartHoverState | None = None
        self._big_overlay_volatility_hover_state: ComboChartHoverState | None = None
        self._kline_view_states: dict[str, KlineChartViewState] = {}
        self._kline_drag_states: dict[str, tuple[float, int, int]] = {}
        self._did_initial_chain_refresh = False
        self._chain_request_id = 0
        self._chart_request_id = 0
        self._position_import_request_id = 0
        self._alias_counter = 0

        self._load_saved_strategies()
        self._build_layout()
        self._refresh_saved_strategy_options()
        self._seed_family_options()

    def show(self) -> None:
        if not self.window.winfo_exists():
            return
        self.window.deiconify()
        self.window.lift()
        self.window.focus_force()
        if not self._did_initial_chain_refresh and self.option_family.get().strip():
            self._did_initial_chain_refresh = True
            self.window.after(120, self.refresh_chain)

    def _build_layout(self) -> None:
        self.window.columnconfigure(0, weight=1)
        self.window.rowconfigure(2, weight=1)

        header = ttk.Frame(self.window, padding=(16, 16, 16, 8))
        header.grid(row=0, column=0, sticky="ew")
        header.columnconfigure(0, weight=1)
        ttk.Label(header, text="期权策略计算器", font=("Microsoft YaHei UI", 18, "bold")).grid(
            row=0, column=0, sticky="w"
        )
        ttk.Label(header, textvariable=self.status_text, justify="right").grid(row=0, column=1, sticky="e")

        controls = ttk.LabelFrame(self.window, text="策略设置", padding=(14, 12))
        controls.grid(row=1, column=0, sticky="ew", padx=16, pady=(0, 8))
        controls.columnconfigure(0, weight=1)
        controls.columnconfigure(1, weight=1)

        strategy_panel = ttk.Frame(controls)
        strategy_panel.grid(row=0, column=0, sticky="nsew", padx=(0, 10))
        strategy_panel.columnconfigure(1, weight=1)
        ttk.Label(strategy_panel, text="策略", font=("Microsoft YaHei UI", 10, "bold")).grid(
            row=0, column=0, columnspan=3, sticky="w", pady=(0, 8)
        )
        ttk.Label(strategy_panel, text="策略名称").grid(row=1, column=0, sticky="w")
        ttk.Entry(strategy_panel, textvariable=self.strategy_name, width=28).grid(
            row=1, column=1, columnspan=2, sticky="ew", padx=(8, 0)
        )
        ttk.Label(strategy_panel, text="已保存策略").grid(row=2, column=0, sticky="w", pady=(8, 0))
        saved_combo = ttk.Combobox(strategy_panel, textvariable=self.saved_strategy_name, state="readonly", width=26)
        saved_combo.grid(row=2, column=1, sticky="ew", padx=(8, 8), pady=(8, 0))
        self._saved_strategy_combo = saved_combo
        strategy_actions = ttk.Frame(strategy_panel)
        strategy_actions.grid(row=2, column=2, sticky="e", pady=(8, 0))
        ttk.Button(strategy_actions, text="加载", width=10, command=self.load_selected_strategy).grid(
            row=0, column=0, sticky="ew"
        )
        ttk.Button(strategy_actions, text="保存", width=10, command=self.save_current_strategy).grid(
            row=0, column=1, sticky="ew", padx=(6, 0)
        )
        ttk.Button(strategy_actions, text="删除", width=10, command=self.delete_selected_strategy).grid(
            row=0, column=2, sticky="ew", padx=(6, 0)
        )

        market_panel = ttk.Frame(controls)
        market_panel.grid(row=0, column=1, sticky="nsew", padx=(10, 0))
        market_panel.columnconfigure(1, weight=1)
        market_panel.columnconfigure(3, weight=1)
        ttk.Label(market_panel, text="期权链", font=("Microsoft YaHei UI", 10, "bold")).grid(
            row=0, column=0, columnspan=7, sticky="w", pady=(0, 8)
        )
        ttk.Label(market_panel, text="期权系列").grid(row=1, column=0, sticky="w")
        family_combo = ttk.Combobox(market_panel, textvariable=self.option_family, width=18)
        family_combo.grid(row=1, column=1, sticky="ew", padx=(8, 14))
        family_combo.bind("<<ComboboxSelected>>", self._on_family_selected)
        self._family_combo = family_combo
        ttk.Label(market_panel, text="到期日").grid(row=1, column=2, sticky="w")
        expiry_combo = ttk.Combobox(market_panel, textvariable=self.expiry_code, state="readonly", width=18)
        expiry_combo.grid(row=1, column=3, sticky="ew", padx=(8, 14))
        expiry_combo.bind("<<ComboboxSelected>>", self._on_expiry_selected)
        self._expiry_combo = expiry_combo
        ttk.Button(market_panel, text="刷新期权链", width=14, command=self.refresh_chain).grid(
            row=1, column=4, sticky="e"
        )
        ttk.Button(market_panel, text="导入到期持仓", width=14, command=self.import_current_expiry_positions).grid(
            row=1, column=5, sticky="e", padx=(8, 0)
        )
        ttk.Button(market_panel, text="导入系列持仓", width=14, command=self.import_current_family_positions).grid(
            row=1, column=6, sticky="e", padx=(8, 0)
        )
        ttk.Label(market_panel, text="默认数量").grid(row=2, column=0, sticky="w", pady=(8, 0))
        ttk.Entry(market_panel, textvariable=self.default_quantity, width=12).grid(
            row=2, column=1, sticky="w", padx=(8, 14), pady=(8, 0)
        )
        ttk.Label(
            market_panel,
            text="先选系列并刷新；也可直接把当前到期日或整个系列的持仓导入策略腿。",
            justify="left",
        ).grid(row=2, column=2, columnspan=5, sticky="w", pady=(8, 0))

        formula_panel = ttk.Frame(controls)
        formula_panel.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(12, 0))
        formula_panel.columnconfigure(1, weight=1)
        ttk.Label(formula_panel, text="图表与公式", font=("Microsoft YaHei UI", 10, "bold")).grid(
            row=0, column=0, columnspan=3, sticky="w", pady=(0, 8)
        )
        ttk.Label(formula_panel, text="组合公式").grid(row=1, column=0, sticky="w")
        ttk.Entry(formula_panel, textvariable=self.formula).grid(row=1, column=1, sticky="ew", padx=(8, 10))
        formula_actions = ttk.Frame(formula_panel)
        formula_actions.grid(row=1, column=2, sticky="e")
        ttk.Button(formula_actions, text="默认公式", width=12, command=self.use_default_formula).grid(
            row=0, column=0, sticky="ew"
        )
        ttk.Button(formula_actions, text="刷新图表", width=12, command=self.refresh_charts).grid(
            row=0, column=1, sticky="ew", padx=(6, 0)
        )
        ttk.Button(formula_actions, text="图表大窗", width=12, command=self.open_big_chart_window).grid(
            row=0, column=2, sticky="ew", padx=(6, 0)
        )
        ttk.Label(
            formula_panel,
            text="公式支持线性表达式，例如 L1 - 2*L2 + 0.5；K 线周期和数量在“组合K线”页签切换。",
            wraplength=1240,
            justify="left",
        ).grid(row=2, column=0, columnspan=3, sticky="w", pady=(8, 0))

        body = ttk.Panedwindow(self.window, orient="vertical")
        body.grid(row=2, column=0, sticky="nsew", padx=16, pady=(0, 16))

        upper = ttk.Panedwindow(body, orient="horizontal")
        body.add(upper, weight=3)

        chain_frame = ttk.LabelFrame(upper, text="期权链", padding=12)
        chain_frame.columnconfigure(0, weight=1)
        chain_frame.rowconfigure(1, weight=1)
        upper.add(chain_frame, weight=5)
        self._chain_frame = chain_frame

        ttk.Label(chain_frame, textvariable=self.chain_selection_text, justify="left", wraplength=720).grid(
            row=0, column=0, sticky="w", pady=(0, 8)
        )
        chain_tree = ttk.Treeview(
            chain_frame,
            columns=("call_mark", "call_bid", "call_ask", "strike", "put_bid", "put_ask", "put_mark"),
            show="headings",
            selectmode="browse",
        )
        for column, label, width in (
            ("call_mark", "认购标记", 100),
            ("call_bid", "认购买一", 100),
            ("call_ask", "认购卖一", 100),
            ("strike", "行权价", 96),
            ("put_bid", "认沽买一", 100),
            ("put_ask", "认沽卖一", 100),
            ("put_mark", "认沽标记", 100),
        ):
            chain_tree.heading(column, text=label)
            chain_tree.column(column, width=width, anchor="e" if column != "strike" else "center")
        chain_tree.grid(row=1, column=0, sticky="nsew")
        chain_tree.bind("<<TreeviewSelect>>", self._on_chain_selected)
        chain_scroll = ttk.Scrollbar(chain_frame, orient="vertical", command=chain_tree.yview)
        chain_scroll.grid(row=1, column=1, sticky="ns")
        chain_tree.configure(yscrollcommand=chain_scroll.set)
        self._chain_tree = chain_tree

        chain_actions = ttk.Frame(chain_frame)
        chain_actions.grid(row=2, column=0, sticky="ew", pady=(10, 0))
        for column in range(4):
            chain_actions.columnconfigure(column, weight=1)
        ttk.Button(chain_actions, text="添加认购买入", command=lambda: self.add_selected_chain_leg("C", "buy")).grid(
            row=0, column=0, sticky="ew", padx=(0, 8)
        )
        ttk.Button(chain_actions, text="添加认购卖出", command=lambda: self.add_selected_chain_leg("C", "sell")).grid(
            row=0, column=1, sticky="ew", padx=(0, 8)
        )
        ttk.Button(chain_actions, text="添加认沽买入", command=lambda: self.add_selected_chain_leg("P", "buy")).grid(
            row=0, column=2, sticky="ew", padx=(0, 8)
        )
        ttk.Button(chain_actions, text="添加认沽卖出", command=lambda: self.add_selected_chain_leg("P", "sell")).grid(
            row=0, column=3, sticky="ew"
        )

        strategy_frame = ttk.Frame(upper, padding=0)
        strategy_frame.columnconfigure(0, weight=1)
        strategy_frame.rowconfigure(1, weight=1)
        upper.add(strategy_frame, weight=4)

        ttk.Label(strategy_frame, textvariable=self.strategy_summary_text, justify="left", wraplength=620).grid(
            row=0, column=0, sticky="w", pady=(0, 8)
        )

        legs_tree = ttk.Treeview(
            strategy_frame,
            columns=(
                "alias",
                "inst_id",
                "type",
                "expiry",
                "strike",
                "side",
                "qty",
                "premium",
                "mark",
                "contract",
                "premium_total",
                "delta",
                "gamma",
                "theta",
                "vega",
            ),
            show="headings",
            selectmode="browse",
        )
        for column, label, width, anchor in (
            ("alias", "别名", 58, "center"),
            ("inst_id", "合约", 250, "w"),
            ("type", "类别", 64, "center"),
            ("expiry", "到期日", 96, "center"),
            ("strike", "行权价", 92, "e"),
            ("side", "买卖", 64, "center"),
            ("qty", "数量", 70, "e"),
            ("premium", "持仓价", 90, "e"),
            ("mark", "标记价", 90, "e"),
            ("contract", "每张面值", 90, "e"),
            ("premium_total", "权利金合计", 110, "e"),
            ("delta", "Delta", 88, "e"),
            ("gamma", "Gamma", 88, "e"),
            ("theta", "Theta", 88, "e"),
            ("vega", "Vega", 88, "e"),
        ):
            legs_tree.heading(column, text=label)
            legs_tree.column(column, width=width, anchor=anchor)
        legs_tree.grid(row=1, column=0, sticky="nsew")
        legs_tree.bind("<Double-1>", self._on_legs_tree_double_click)
        legs_scroll = ttk.Scrollbar(strategy_frame, orient="vertical", command=legs_tree.yview)
        legs_scroll.grid(row=1, column=1, sticky="ns")
        legs_tree.configure(yscrollcommand=legs_scroll.set)
        self._legs_tree = legs_tree

        legs_actions = ttk.Frame(strategy_frame)
        legs_actions.grid(row=2, column=0, sticky="ew", pady=(10, 0))
        for column in range(5):
            legs_actions.columnconfigure(column, weight=1)
        ttk.Button(legs_actions, text="删除选中腿", command=self.remove_selected_leg).grid(
            row=0, column=0, sticky="ew", padx=(0, 8)
        )
        ttk.Button(legs_actions, text="修改数量", command=self.edit_selected_leg_quantity).grid(
            row=0, column=1, sticky="ew", padx=(0, 8)
        )
        ttk.Button(legs_actions, text="修改持仓价", command=self.edit_selected_leg_premium).grid(
            row=0, column=2, sticky="ew", padx=(0, 8)
        )
        ttk.Button(legs_actions, text="清空策略腿", command=self.clear_legs).grid(
            row=0, column=3, sticky="ew", padx=(0, 8)
        )
        ttk.Button(legs_actions, text="刷新腿报价", command=self.refresh_leg_quotes).grid(row=0, column=4, sticky="ew")

        charts = ttk.Notebook(body)
        body.add(charts, weight=3)

        payoff_tab = ttk.Frame(charts, padding=12)
        payoff_tab.columnconfigure(0, weight=1)
        payoff_tab.rowconfigure(2, weight=1)
        payoff_top = ttk.Frame(payoff_tab)
        payoff_top.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        payoff_top.columnconfigure(0, weight=1)
        ttk.Label(payoff_top, textvariable=self.payoff_summary_text, justify="left", wraplength=1040).grid(
            row=0, column=0, sticky="w"
        )
        ttk.Label(payoff_top, text="图表币种").grid(row=0, column=1, sticky="e", padx=(8, 6))
        payoff_ccy_combo = ttk.Combobox(
            payoff_top,
            textvariable=self.chart_display_ccy,
            values=("USDT", "结算币"),
            state="readonly",
            width=10,
        )
        payoff_ccy_combo.grid(row=0, column=2, sticky="e", padx=(0, 8))
        payoff_ccy_combo.bind("<<ComboboxSelected>>", lambda _event: self._refresh_chart_display())
        ttk.Button(payoff_top, text="重新计算", command=self.refresh_charts).grid(row=0, column=3, sticky="e")

        payoff_controls = ttk.Frame(payoff_tab)
        payoff_controls.grid(row=1, column=0, sticky="ew", pady=(0, 8))
        payoff_controls.columnconfigure(1, weight=1)
        payoff_controls.columnconfigure(4, weight=1)
        ttk.Label(payoff_controls, textvariable=self.payoff_sim_date_text).grid(row=0, column=0, sticky="w", padx=(0, 8))
        ttk.Scale(
            payoff_controls,
            from_=0.0,
            to=100.0,
            variable=self.payoff_time_progress,
            command=self._on_payoff_time_slider_changed,
        ).grid(row=0, column=1, sticky="ew", padx=(0, 16))
        ttk.Label(payoff_controls, textvariable=self.payoff_vol_shift_text).grid(row=0, column=2, sticky="w", padx=(0, 8))
        ttk.Scale(
            payoff_controls,
            from_=-70.0,
            to=200.0,
            variable=self.payoff_vol_shift_percent,
            command=self._on_payoff_vol_shift_changed,
        ).grid(row=0, column=4, sticky="ew")
        payoff_canvas = Canvas(payoff_tab, background="#ffffff", highlightthickness=0, cursor="crosshair")
        payoff_canvas.grid(row=2, column=0, sticky="nsew")
        payoff_canvas.bind("<Motion>", self._on_payoff_canvas_motion)
        payoff_canvas.bind("<Leave>", lambda _event: self._clear_chart_hover(payoff_canvas))
        charts.add(payoff_tab, text="到期盈亏图")
        self._payoff_canvas = payoff_canvas

        combo_tab = ttk.Frame(charts, padding=12)
        combo_tab.columnconfigure(0, weight=1)
        combo_tab.rowconfigure(2, weight=1)
        combo_toolbar = ttk.Frame(combo_tab)
        combo_toolbar.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        combo_toolbar.columnconfigure(8, weight=1)
        ttk.Label(combo_toolbar, text="K线周期").grid(row=0, column=0, sticky="w")
        combo_bar_combo = ttk.Combobox(combo_toolbar, textvariable=self.bar, values=BAR_OPTIONS, state="readonly", width=10)
        combo_bar_combo.grid(row=0, column=1, sticky="w", padx=(6, 14))
        combo_bar_combo.bind("<<ComboboxSelected>>", lambda _event: self.refresh_combo_chart(silent=True))
        ttk.Label(combo_toolbar, text="K线数量").grid(row=0, column=2, sticky="w")
        combo_limit_entry = ttk.Entry(combo_toolbar, textvariable=self.candle_limit, width=10)
        combo_limit_entry.grid(row=0, column=3, sticky="w", padx=(6, 14))
        combo_limit_entry.bind("<Return>", lambda _event: self.refresh_combo_chart())
        ttk.Label(combo_toolbar, text="图表币种").grid(row=0, column=4, sticky="w")
        combo_ccy_combo = ttk.Combobox(
            combo_toolbar,
            textvariable=self.chart_display_ccy,
            values=("USDT", "结算币"),
            state="readonly",
            width=10,
        )
        combo_ccy_combo.grid(row=0, column=5, sticky="w", padx=(6, 14))
        combo_ccy_combo.bind("<<ComboboxSelected>>", lambda _event: self._refresh_chart_display(combo_only=True))
        ttk.Label(combo_toolbar, text="只影响组合K线").grid(row=0, column=6, sticky="w")
        ttk.Checkbutton(
            combo_toolbar,
            text="消除影线",
            variable=self.combo_hide_wicks,
            command=lambda: self._refresh_chart_display(combo_only=True),
        ).grid(row=0, column=7, sticky="w", padx=(10, 0))
        ttk.Button(combo_toolbar, text="刷新组合K线", command=self.refresh_combo_chart).grid(row=0, column=9, sticky="e")
        ttk.Label(combo_tab, textvariable=self.combo_summary_text, justify="left", wraplength=1040).grid(
            row=1, column=0, sticky="w", pady=(0, 8)
        )
        combo_canvas = Canvas(combo_tab, background="#ffffff", highlightthickness=0, cursor="crosshair")
        combo_canvas.grid(row=2, column=0, sticky="nsew")
        combo_canvas.bind("<Motion>", self._on_combo_canvas_motion)
        combo_canvas.bind("<Leave>", lambda _event: self._clear_chart_hover(combo_canvas))
        combo_canvas.bind("<Configure>", lambda _event: self._redraw_kline_chart("main_combo"))
        self._bind_kline_chart_interactions(combo_canvas, "main_combo")
        charts.add(combo_tab, text="组合K线")
        self._combo_canvas = combo_canvas
        self._update_chain_context_ui()
        self._update_payoff_simulation_labels()

        if self._payoff_canvas is not None:
            self._clear_canvas(self._payoff_canvas, "加入策略腿后，可生成到期盈亏图。")
        if self._combo_canvas is not None:
            self._clear_canvas(self._combo_canvas, "组合 K 线使用期权标记价格；先加入策略腿再生成。")

    def _load_saved_strategies(self) -> None:
        try:
            snapshot = load_option_strategies_snapshot()
        except Exception as exc:  # noqa: BLE001
            self._log(f"[期权策略] 读取已保存策略失败：{exc}")
            self._saved_strategies = []
            return
        records = snapshot.get("strategies", [])
        self._saved_strategies = [item for item in records if isinstance(item, dict)]

    def _refresh_saved_strategy_options(self) -> None:
        names = [str(item.get("name", "")) for item in self._saved_strategies if str(item.get("name", "")).strip()]
        if self._saved_strategy_combo is not None:
            self._saved_strategy_combo.configure(values=names)
        current = self.saved_strategy_name.get().strip()
        if current not in names:
            self.saved_strategy_name.set(names[0] if names else "")

    def _seed_family_options(self) -> None:
        values = list(DEFAULT_OPTION_FAMILY_OPTIONS)
        current = self.option_family.get().strip().upper()
        if current and current not in values:
            values.insert(0, current)
        if self._family_combo is not None:
            self._family_combo.configure(values=values)
        if not current and values:
            self.option_family.set(values[0])
        self.status_text.set("首次打开会自动刷新默认期权系列；也可以切换系列后手动刷新期权链。")
        self._update_chain_context_ui()

    def _on_family_selected(self, _event=None) -> None:
        self._sync_expiry_options()
        self._chain_rows = []
        if self._chain_tree is not None:
            self._chain_tree.delete(*self._chain_tree.get_children())
        self._update_chain_context_ui()
        self._set_chain_selection_text("点击“刷新期权链”后，会把当前系列的所有到期日刷新出来。")

    def _on_expiry_selected(self, _event=None) -> None:
        if self.expiry_code.get().strip():
            self._update_chain_context_ui()
            if self._apply_cached_expiry_selection():
                return
            self.refresh_chain()

    def _family_instruments(self, family: str) -> list[Instrument]:
        normalized = family.strip().upper()
        if not normalized:
            return []
        return list(self._family_instruments_cache.get(normalized, ()))

    def _fetch_family_instruments_remote(self, family: str) -> list[Instrument]:
        normalized = family.strip().upper()
        if not normalized:
            return []
        try:
            return self.client.get_instruments("OPTION", uly=normalized)
        except Exception:
            return self.client.get_option_instruments(inst_family=normalized)

    def _fetch_family_tickers_remote(self, family: str) -> list[OkxTicker]:
        normalized = family.strip().upper()
        if not normalized:
            return []
        try:
            return self.client.get_tickers("OPTION", uly=normalized)
        except Exception:
            return self.client.get_tickers("OPTION", inst_family=normalized)

    def _sync_expiry_options(self, *, preferred: str | None = None) -> None:
        family = self.option_family.get().strip().upper()
        instruments = self._family_instruments(family) if family else []
        expiries = sorted({parse_option_contract(item.inst_id).expiry_code for item in instruments})
        if self._expiry_combo is not None:
            display_values = [f"{code} ({format_option_expiry_label(code)})" for code in expiries]
            self._expiry_combo.configure(values=display_values)
        current = preferred or self._selected_expiry_code()
        if current in expiries:
            self.expiry_code.set(f"{current} ({format_option_expiry_label(current)})")
        elif expiries:
            self.expiry_code.set(f"{expiries[0]} ({format_option_expiry_label(expiries[0])})")
        else:
            self.expiry_code.set("")
        self._update_chain_context_ui()

    def refresh_chain(self) -> None:
        family = self.option_family.get().strip().upper()
        if not family:
            messagebox.showerror("期权链参数错误", "请先输入或选择期权系列。", parent=self.window)
            return

        self._chain_request_id += 1
        request_id = self._chain_request_id
        preferred_expiry = self._selected_expiry_code()
        self.status_text.set(f"正在加载 {family} 全部到期日...")
        threading.Thread(
            target=self._load_chain_worker,
            args=(request_id, family, preferred_expiry),
            daemon=True,
        ).start()

    def _load_chain_worker(self, request_id: int, family: str, preferred_expiry: str) -> None:
        try:
            family_instruments = self._fetch_family_instruments_remote(family)
            tickers = self._fetch_family_tickers_remote(family)
            tickers_by_inst_id = {item.inst_id: item for item in tickers}
            expiries = sorted({parse_option_contract(item.inst_id).expiry_code for item in family_instruments})
            selected_expiry = preferred_expiry if preferred_expiry in expiries else (expiries[0] if expiries else "")
            selected_instruments = [
                item for item in family_instruments if parse_option_contract(item.inst_id).expiry_code == selected_expiry
            ]
            quotes: list[OptionQuote] = []
            for instrument in selected_instruments:
                ticker = tickers_by_inst_id.get(instrument.inst_id)
                quotes.append(_build_option_quote(instrument, ticker))
            chain_rows = build_option_chain_rows(quotes)
            underlying_price = next((item.index_price for item in quotes if item.index_price is not None), None)
            self.window.after(
                0,
                lambda: self._apply_chain_snapshot(
                    request_id=request_id,
                    family=family,
                    expiry=selected_expiry,
                    expiries=expiries,
                    chain_rows=chain_rows,
                    quotes=quotes,
                    tickers_by_inst_id=tickers_by_inst_id,
                    family_instruments=family_instruments,
                    underlying_price=underlying_price,
                ),
            )
        except Exception as exc:  # noqa: BLE001
            self.window.after(0, lambda error=exc: self._show_chain_error(request_id, error))

    def _apply_chain_snapshot(
        self,
        *,
        request_id: int,
        family: str,
        expiry: str,
        expiries: list[str],
        chain_rows: list[OptionChainRow],
        quotes: list[OptionQuote],
        tickers_by_inst_id: dict[str, OkxTicker] | None,
        family_instruments: list[Instrument],
        underlying_price: Decimal | None,
    ) -> None:
        if request_id != self._chain_request_id or not self.window.winfo_exists():
            return
        self._chain_rows = chain_rows
        self._current_underlying_price = underlying_price
        self._family_instruments_cache[family] = list(family_instruments)
        if tickers_by_inst_id is not None:
            self._family_tickers_cache[family] = dict(tickers_by_inst_id)
        self._all_option_instruments = list(family_instruments)
        for instrument in family_instruments:
            self._instrument_map[instrument.inst_id] = instrument
        for quote in quotes:
            self._quotes_by_inst_id[quote.instrument.inst_id] = quote
        if self._family_combo is not None:
            current_values = [str(item) for item in self._family_combo.cget("values")]
            if family not in current_values:
                current_values.append(family)
                current_values.sort()
                self._family_combo.configure(values=current_values)
        self._sync_expiry_options(preferred=expiry)
        self._update_chain_context_ui(row_count=len(chain_rows))
        self._render_chain_rows()
        self.status_text.set(f"{family} 已刷新出 {len(expiries)} 个到期日，当前显示 {expiry or '-'}。")
        self._refresh_strategy_summary()

    def _show_chain_error(self, request_id: int, exc: Exception) -> None:
        if request_id != self._chain_request_id:
            return
        self.status_text.set("期权链加载失败")
        messagebox.showerror("期权链加载失败", str(exc), parent=self.window)

    def import_current_expiry_positions(self) -> None:
        self._start_import_positions(scope="expiry")

    def import_current_family_positions(self) -> None:
        self._start_import_positions(scope="family")

    def _start_import_positions(self, *, scope: str) -> None:
        family = self.option_family.get().strip().upper()
        expiry = self._selected_expiry_code()
        if not family:
            messagebox.showerror("导入持仓失败", "请先选择期权系列。", parent=self.window)
            return
        if scope == "expiry" and not expiry:
            messagebox.showerror("导入持仓失败", "请先选择到期日。", parent=self.window)
            return

        replace_existing = True
        if self._legs:
            answer = messagebox.askyesnocancel(
                "导入持仓",
                "当前已有策略腿。\n\n选择“是”清空后导入；选择“否”追加导入；选择“取消”放弃本次导入。",
                parent=self.window,
            )
            if answer is None:
                return
            replace_existing = bool(answer)

        self._position_import_request_id += 1
        request_id = self._position_import_request_id
        scope_text = "当前到期日" if scope == "expiry" else "当前系列"
        self.status_text.set(f"正在导入{scope_text}持仓...")
        threading.Thread(
            target=self._load_positions_worker,
            args=(request_id, family, expiry, scope, replace_existing),
            daemon=True,
        ).start()

    def _load_positions_worker(
        self,
        request_id: int,
        family: str,
        expiry: str,
        scope: str,
        replace_existing: bool,
    ) -> None:
        try:
            runtime = self.runtime_provider() if self.runtime_provider is not None else None
            if runtime is None or not isinstance(runtime, tuple) or len(runtime) != 2:
                raise ValueError("当前未配置可用运行环境，无法导入账户持仓。")
            credentials, environment = runtime
            positions = self.client.get_positions(credentials, environment=environment, inst_type="OPTION")
            filtered_positions = _filter_option_positions(positions, family=family, expiry_code=expiry if scope == "expiry" else None)
            if not filtered_positions:
                scope_text = f"{family} {expiry}" if scope == "expiry" and expiry else family
                raise ValueError(f"当前账户没有可导入的 {scope_text} 期权持仓。")

            family_instruments = self._family_instruments_cache.get(family) or self._fetch_family_instruments_remote(family)
            tickers_by_inst_id = self._family_tickers_cache.get(family)
            if tickers_by_inst_id is None:
                tickers = self._fetch_family_tickers_remote(family)
                tickers_by_inst_id = {item.inst_id: item for item in tickers}

            imports: list[tuple[StrategyLegDefinition, Instrument, OptionQuote | None]] = []
            alias_start = 0 if replace_existing else self._alias_counter
            next_alias = alias_start
            instrument_lookup = {item.inst_id: item for item in family_instruments}
            for position in filtered_positions:
                instrument = instrument_lookup.get(position.inst_id) or self.client.get_instrument(position.inst_id)
                ticker = tickers_by_inst_id.get(position.inst_id)
                quote = _build_option_quote(instrument, ticker) if ticker is not None else None
                next_alias += 1
                side, quantity = _position_side_and_quantity(position)
                premium = position.avg_price
                if premium is None and quote is not None:
                    premium = quote.reference_price
                imports.append(
                    (
                        StrategyLegDefinition(
                            alias=f"L{next_alias}",
                            inst_id=position.inst_id,
                            side=side,
                            quantity=quantity,
                            premium=premium,
                            enabled=True,
                        ),
                        instrument,
                        quote,
                    )
                )

            self.window.after(
                0,
                lambda: self._apply_imported_positions(
                    request_id=request_id,
                    family=family,
                    expiry=expiry,
                    scope=scope,
                    replace_existing=replace_existing,
                    imported=imports,
                    family_instruments=family_instruments,
                    tickers_by_inst_id=tickers_by_inst_id,
                ),
            )
        except Exception as exc:  # noqa: BLE001
            self.window.after(0, lambda error=exc: self._show_import_positions_error(request_id, error))

    def _apply_imported_positions(
        self,
        *,
        request_id: int,
        family: str,
        expiry: str,
        scope: str,
        replace_existing: bool,
        imported: list[tuple[StrategyLegDefinition, Instrument, OptionQuote | None]],
        family_instruments: list[Instrument],
        tickers_by_inst_id: dict[str, OkxTicker],
    ) -> None:
        if request_id != self._position_import_request_id or not self.window.winfo_exists():
            return

        old_default_formula = build_default_formula(self._legs) if self._legs else ""
        if replace_existing:
            self._legs = []
            self._latest_payoff_snapshot = None
            self._latest_expiry_payoff_snapshot = None
            self._latest_combo_candles = []
            self._latest_combo_value = None
            self._latest_chart_formula = ""
            self._latest_resolved_legs = []
            self._latest_implied_volatility_by_alias = {}
            self._latest_payoff_loaded_at = None
            self._latest_payoff_expiry_at = None
            self._reset_payoff_simulation_controls()

        for leg, instrument, quote in imported:
            self._legs.append(leg)
            self._instrument_map[instrument.inst_id] = instrument
            if quote is not None:
                self._quotes_by_inst_id[instrument.inst_id] = quote
                if self._current_underlying_price is None and quote.index_price is not None:
                    self._current_underlying_price = quote.index_price

        self._family_instruments_cache[family] = list(family_instruments)
        self._family_tickers_cache[family] = dict(tickers_by_inst_id)
        self._all_option_instruments = list(family_instruments)
        for instrument in family_instruments:
            self._instrument_map[instrument.inst_id] = instrument

        self._alias_counter = max(
            self._alias_counter,
            max((int(leg.alias[1:]) for leg, *_ in imported if leg.alias.startswith("L") and leg.alias[1:].isdigit()), default=0),
        )

        should_reset_formula = not self.formula.get().strip() or self.formula.get().strip() == old_default_formula
        if should_reset_formula:
            self.formula.set(build_default_formula(self._legs))

        self._refresh_leg_greeks()
        self._render_legs()
        self._refresh_strategy_summary()
        scope_text = "当前到期日" if scope == "expiry" else "当前系列"
        imported_count = len(imported)
        context = f"{family} {expiry}" if scope == "expiry" and expiry else family
        self.status_text.set(f"已导入 {scope_text} 持仓：{context} | {imported_count} 条策略腿")

    def _show_import_positions_error(self, request_id: int, exc: Exception) -> None:
        if request_id != self._position_import_request_id:
            return
        self.status_text.set("导入持仓失败")
        messagebox.showerror("导入持仓失败", str(exc), parent=self.window)

    def _render_chain_rows(self) -> None:
        if self._chain_tree is None:
            return
        self._chain_tree.delete(*self._chain_tree.get_children())
        for index, row in enumerate(self._chain_rows):
            call_tick = row.call_quote.instrument.tick_size if row.call_quote is not None else None
            put_tick = row.put_quote.instrument.tick_size if row.put_quote is not None else None
            self._chain_tree.insert(
                "",
                END,
                iid=f"R{index:04d}",
                values=(
                    _format_price(row.call_quote.mark_price if row.call_quote is not None else None, call_tick),
                    _format_price(row.call_quote.bid_price if row.call_quote is not None else None, call_tick),
                    _format_price(row.call_quote.ask_price if row.call_quote is not None else None, call_tick),
                    format_decimal(row.strike),
                    _format_price(row.put_quote.bid_price if row.put_quote is not None else None, put_tick),
                    _format_price(row.put_quote.ask_price if row.put_quote is not None else None, put_tick),
                    _format_price(row.put_quote.mark_price if row.put_quote is not None else None, put_tick),
                ),
            )
        if self._chain_rows:
            first = self._chain_tree.get_children()[0]
            self._chain_tree.selection_set(first)
            self._chain_tree.focus(first)
            self._on_chain_selected()
        else:
            self._set_chain_selection_text("当前到期日没有拿到可用期权链数据。")

    def _on_chain_selected(self, *_: object) -> None:
        row = self._selected_chain_row()
        if row is None:
            self._set_chain_selection_text("选择一个行权价后，可把认购 / 认沽直接加入策略腿。")
            return
        call_inst_id = row.call_quote.instrument.inst_id if row.call_quote is not None else "-"
        put_inst_id = row.put_quote.instrument.inst_id if row.put_quote is not None else "-"
        current_price = f" | 标的指数≈{format_decimal(self._current_underlying_price)}" if self._current_underlying_price else ""
        self._set_chain_selection_text(
            f"行权价 {format_decimal(row.strike)} | 认购 {call_inst_id} | 认沽 {put_inst_id}{current_price}"
        )

    def _selected_chain_row(self) -> OptionChainRow | None:
        if self._chain_tree is None:
            return None
        selection = self._chain_tree.selection()
        if not selection:
            return None
        try:
            index = int(selection[0][1:])
        except Exception:
            return None
        if index < 0 or index >= len(self._chain_rows):
            return None
        return self._chain_rows[index]

    def _selected_leg_index(self) -> int | None:
        if self._legs_tree is None:
            return None
        selection = self._legs_tree.selection()
        if not selection:
            return None
        try:
            return int(selection[0][1:])
        except Exception:
            return None

    def _on_legs_tree_double_click(self, event) -> None:
        if self._legs_tree is None:
            return
        row_id = self._legs_tree.identify_row(event.y)
        column_id = self._legs_tree.identify_column(event.x)
        if not row_id:
            return
        self._legs_tree.selection_set(row_id)
        self._legs_tree.focus(row_id)
        if column_id == "#7":
            self.edit_selected_leg_quantity()
        elif column_id == "#8":
            self.edit_selected_leg_premium()

    def add_selected_chain_leg(self, option_type: str, side: str) -> None:
        row = self._selected_chain_row()
        if row is None:
            messagebox.showinfo("添加策略腿", "请先在期权链里选择一个行权价。", parent=self.window)
            return
        quote = row.call_quote if option_type == "C" else row.put_quote
        if quote is None:
            messagebox.showinfo("添加策略腿", "当前行权价没有对应的可用合约。", parent=self.window)
            return
        try:
            quantity = self._parse_positive_decimal(self.default_quantity.get(), "默认数量")
        except Exception as exc:
            messagebox.showerror("数量错误", str(exc), parent=self.window)
            return
        self._alias_counter += 1
        alias = f"L{self._alias_counter}"
        leg = StrategyLegDefinition(
            alias=alias,
            inst_id=quote.instrument.inst_id,
            side="buy" if side == "buy" else "sell",
            quantity=quantity,
            premium=quote.reference_price,
            enabled=True,
        )
        self._legs.append(leg)
        self._instrument_map[quote.instrument.inst_id] = quote.instrument
        if quote.reference_price is not None:
            self._quotes_by_inst_id[quote.instrument.inst_id] = quote
        if not self.formula.get().strip():
            self.formula.set(build_default_formula(self._legs))
        self._refresh_leg_greeks()
        self._render_legs()
        self._refresh_strategy_summary()

    def remove_selected_leg(self) -> None:
        index = self._selected_leg_index()
        if index is None:
            messagebox.showinfo("删除策略腿", "请先选择一条策略腿。", parent=self.window)
            return
        if index < 0 or index >= len(self._legs):
            return
        self._legs.pop(index)
        self._render_legs()
        self._refresh_strategy_summary()

    def edit_selected_leg_quantity(self) -> None:
        index = self._selected_leg_index()
        if index is None:
            messagebox.showinfo("修改数量", "请先选择一条策略腿。", parent=self.window)
            return
        if index < 0 or index >= len(self._legs):
            return

        leg = self._legs[index]
        old_default_formula = build_default_formula(self._legs)
        response = simpledialog.askstring(
            "修改数量",
            f"请输入 {leg.alias} 的新数量：",
            initialvalue=format_decimal(leg.quantity),
            parent=self.window,
        )
        if response is None:
            return
        try:
            quantity = self._parse_positive_decimal(response, "策略腿数量")
        except Exception as exc:
            messagebox.showerror("修改数量失败", str(exc), parent=self.window)
            return

        leg.quantity = quantity
        if self.formula.get().strip() == old_default_formula:
            self.formula.set(build_default_formula(self._legs))
        self._render_legs()
        self._refresh_strategy_summary()
        if self._latest_payoff_snapshot is not None or self._latest_combo_candles:
            self.refresh_charts()

    def edit_selected_leg_premium(self) -> None:
        index = self._selected_leg_index()
        if index is None:
            messagebox.showinfo("修改持仓价", "请先选择一条策略腿。", parent=self.window)
            return
        if index < 0 or index >= len(self._legs):
            return

        leg = self._legs[index]
        instrument = self._instrument_map.get(leg.inst_id)
        initial_value = format_decimal(leg.premium) if leg.premium is not None else ""
        response = simpledialog.askstring(
            "修改持仓价",
            f"请输入 {leg.alias} 的持仓价：",
            initialvalue=initial_value,
            parent=self.window,
        )
        if response is None:
            return
        text = response.strip()
        if not text:
            leg.premium = None
        else:
            try:
                premium = self._parse_positive_decimal(text, "持仓价")
            except Exception as exc:
                messagebox.showerror("修改持仓价失败", str(exc), parent=self.window)
                return
            tick_size = instrument.tick_size if instrument is not None else None
            if tick_size is not None and premium <= 0:
                messagebox.showerror("修改持仓价失败", "持仓价必须大于 0。", parent=self.window)
                return
            leg.premium = premium
        self._render_legs()
        self._refresh_strategy_summary()
        if self._latest_payoff_snapshot is not None or self._latest_combo_candles:
            self._refresh_payoff_simulation()
            self._refresh_chart_display(combo_only=False)

    def clear_legs(self) -> None:
        self._legs.clear()
        self._latest_payoff_snapshot = None
        self._latest_expiry_payoff_snapshot = None
        self._latest_combo_candles = []
        self._latest_combo_value = None
        self._latest_spot_usdt_price = None
        self._latest_spot_usdt_candles = []
        self._latest_chart_formula = ""
        self._latest_resolved_legs = []
        self._latest_implied_volatility_by_alias = {}
        self._latest_payoff_loaded_at = None
        self._latest_payoff_expiry_at = None
        self._reset_payoff_simulation_controls()
        self._render_legs()
        self._refresh_strategy_summary()
        if self._payoff_canvas is not None:
            self._clear_canvas(self._payoff_canvas, "加入策略腿后，可生成到期盈亏图。")
        if self._combo_canvas is not None:
            self._clear_canvas(self._combo_canvas, "组合 K 线使用期权标记价格；先加入策略腿再生成。")
        self.payoff_summary_text.set("加入策略腿后，可生成到期盈亏图。")
        self.combo_summary_text.set("组合 K 线采用 OKX 期权标记价格。")

    def _render_legs(self) -> None:
        if self._legs_tree is None:
            return
        self._legs_tree.delete(*self._legs_tree.get_children())
        for index, leg in enumerate(self._legs):
            instrument = self._instrument_map.get(leg.inst_id)
            parsed = parse_option_contract(leg.inst_id)
            premium = leg.premium
            mark_price = self._leg_mark_price(leg.inst_id)
            contract_value = option_contract_value(instrument) if instrument is not None else Decimal("1")
            premium_total = premium * contract_value * leg.quantity if premium is not None else None
            self._legs_tree.insert(
                "",
                END,
                iid=f"L{index:04d}",
                values=(
                    leg.alias,
                    leg.inst_id,
                    "认购" if parsed.option_type == "C" else "认沽",
                    parsed.expiry_label,
                    format_decimal(parsed.strike),
                    "买入" if leg.side == "buy" else "卖出",
                    format_decimal(leg.quantity),
                    _format_price(premium, instrument.tick_size if instrument is not None else None),
                    _format_price(mark_price, instrument.tick_size if instrument is not None else None),
                    format_decimal(contract_value),
                    format_decimal(premium_total) if premium_total is not None else "-",
                    _format_compact_number(leg.delta),
                    _format_compact_number(leg.gamma),
                    _format_compact_number(leg.theta),
                    _format_compact_number(leg.vega),
                ),
            )

    def _leg_mark_price(self, inst_id: str) -> Decimal | None:
        quote = self._quotes_by_inst_id.get(inst_id)
        if quote is None:
            return None
        return quote.reference_price

    def _refresh_leg_greeks(self) -> None:
        valuation_time = datetime.now()
        settlement_price = self._current_underlying_price
        for leg in self._legs:
            leg.delta = None
            leg.gamma = None
            leg.theta = None
            leg.vega = None
            instrument = self._instrument_map.get(leg.inst_id)
            quote = self._quotes_by_inst_id.get(leg.inst_id)
            if (
                instrument is None
                or quote is None
                or quote.reference_price is None
                or settlement_price is None
                or settlement_price <= 0
                or leg.premium is None
            ):
                continue
            try:
                resolved_leg = resolve_strategy_leg(leg, instrument)
                implied_volatility = infer_implied_volatility_for_leg(
                    resolved_leg,
                    settlement_price=settlement_price,
                    valuation_time=valuation_time,
                    option_price=quote.reference_price,
                )
                greeks = estimate_leg_greeks(
                    resolved_leg,
                    settlement_price=settlement_price,
                    valuation_time=valuation_time,
                    base_implied_volatility=implied_volatility,
                )
                direction = Decimal("1") if leg.side == "buy" else Decimal("-1")
                leg.delta = greeks["delta"] * direction * leg.quantity
                leg.gamma = greeks["gamma"] * direction * leg.quantity
                leg.theta = greeks["theta"] * direction * leg.quantity
                leg.vega = greeks["vega"] * direction * leg.quantity
            except Exception:
                continue

    def use_default_formula(self) -> None:
        self.formula.set(build_default_formula(self._legs))
        self._refresh_strategy_summary()

    def refresh_leg_quotes(self) -> None:
        if not self._legs:
            messagebox.showinfo("刷新腿报价", "当前没有策略腿。", parent=self.window)
            return
        self.status_text.set("正在刷新策略腿报价...")
        threading.Thread(target=self._refresh_leg_quotes_worker, daemon=True).start()

    def _refresh_leg_quotes_worker(self) -> None:
        try:
            refreshed: list[tuple[str, Instrument, OptionQuote]] = []
            for leg in self._legs:
                instrument = self._instrument_map.get(leg.inst_id) or self.client.get_instrument(leg.inst_id)
                ticker = self.client.get_ticker(leg.inst_id)
                quote = _build_option_quote(instrument, ticker)
                refreshed.append((leg.inst_id, instrument, quote))
            self.window.after(0, lambda: self._apply_refreshed_leg_quotes(refreshed))
        except Exception as exc:  # noqa: BLE001
            self.window.after(0, lambda: messagebox.showerror("刷新腿报价失败", str(exc), parent=self.window))

    def _apply_refreshed_leg_quotes(
        self,
        refreshed: list[tuple[str, Instrument, OptionQuote]],
    ) -> None:
        for inst_id, instrument, quote in refreshed:
            self._instrument_map[inst_id] = instrument
            self._quotes_by_inst_id[inst_id] = quote
            if self._current_underlying_price is None and quote.index_price is not None:
                self._current_underlying_price = quote.index_price
        self._render_legs()
        self._refresh_strategy_summary()
        self.status_text.set("策略腿报价已刷新。")

    def refresh_charts(self) -> None:
        if not self._legs:
            messagebox.showinfo("刷新图表", "请先至少加入一条策略腿。", parent=self.window)
            return
        try:
            candle_limit = self._parse_positive_int(self.candle_limit.get(), "K线数量")
        except Exception as exc:
            messagebox.showerror("图表参数错误", str(exc), parent=self.window)
            return
        if candle_limit > MAX_OPTION_COMBO_CANDLES:
            messagebox.showerror(
                "图表参数错误",
                f"组合 K 线当前最多支持 {MAX_OPTION_COMBO_CANDLES} 根标记价格 K 线。",
                parent=self.window,
            )
            return
        aliases = {item.alias for item in self._legs if item.alias.strip()}
        formula = self.formula.get().strip() or build_default_formula(self._legs)
        if not formula:
            messagebox.showerror("图表参数错误", "请先加入有效策略腿，再生成组合公式。", parent=self.window)
            return
        try:
            parse_linear_formula(formula, allowed_names=aliases)
        except Exception as exc:
            messagebox.showerror("组合公式错误", str(exc), parent=self.window)
            return

        self._chart_request_id += 1
        request_id = self._chart_request_id
        self.status_text.set("正在生成到期盈亏图和组合 K 线...")
        threading.Thread(
            target=self._load_chart_worker,
            args=(request_id, candle_limit, formula),
            daemon=True,
        ).start()

    def refresh_combo_chart(self, *, silent: bool = False) -> None:
        if not self._legs:
            if not silent:
                messagebox.showinfo("刷新组合K线", "请先至少加入一条策略腿。", parent=self.window)
            return
        try:
            candle_limit = self._parse_positive_int(self.candle_limit.get(), "K线数量")
        except Exception as exc:
            if not silent:
                messagebox.showerror("图表参数错误", str(exc), parent=self.window)
            return
        if candle_limit > MAX_OPTION_COMBO_CANDLES:
            if not silent:
                messagebox.showerror(
                    "图表参数错误",
                    f"组合 K 线当前最多支持 {MAX_OPTION_COMBO_CANDLES} 根标记价格 K 线。",
                    parent=self.window,
                )
            return
        aliases = {item.alias for item in self._legs if item.alias.strip()}
        formula = self.formula.get().strip() or build_default_formula(self._legs)
        if not formula:
            if not silent:
                messagebox.showerror("图表参数错误", "请先加入有效策略腿，再生成组合公式。", parent=self.window)
            return
        try:
            parse_linear_formula(formula, allowed_names=aliases)
        except Exception as exc:
            if not silent:
                messagebox.showerror("组合公式错误", str(exc), parent=self.window)
            return

        self._chart_request_id += 1
        request_id = self._chart_request_id
        self.status_text.set("正在刷新组合 K 线...")
        threading.Thread(
            target=self._load_combo_chart_worker,
            args=(request_id, candle_limit, formula),
            daemon=True,
        ).start()

    def open_big_chart_window(self) -> None:
        window = self._ensure_big_chart_window()
        if not window.winfo_exists():
            return
        window.deiconify()
        window.lift()
        window.focus_force()
        self._refresh_big_chart_window()

    def _ensure_big_chart_window(self) -> Toplevel:
        if self._big_chart_window is not None and self._big_chart_window.winfo_exists():
            return self._big_chart_window

        window = Toplevel(self.window)
        window.title("期权策略图表大窗")
        apply_adaptive_window_geometry(
            window,
            width_ratio=0.92,
            height_ratio=0.9,
            min_width=1320,
            min_height=880,
            max_width=1920,
            max_height=1280,
        )
        window.protocol("WM_DELETE_WINDOW", window.withdraw)
        window.columnconfigure(0, weight=1)
        window.rowconfigure(0, weight=1)

        notebook = ttk.Notebook(window)
        notebook.grid(row=0, column=0, sticky="nsew", padx=12, pady=12)
        notebook.bind("<<NotebookTabChanged>>", self._schedule_big_chart_redraw)

        payoff_tab = ttk.Frame(notebook, padding=12)
        payoff_tab.columnconfigure(0, weight=1)
        payoff_tab.rowconfigure(1, weight=1)
        ttk.Label(payoff_tab, textvariable=self.payoff_summary_text, justify="left", wraplength=1260).grid(
            row=0, column=0, sticky="w", pady=(0, 8)
        )
        payoff_canvas = Canvas(payoff_tab, background="#ffffff", highlightthickness=0, cursor="crosshair")
        payoff_canvas.grid(row=1, column=0, sticky="nsew")
        payoff_canvas.bind("<Configure>", self._schedule_big_chart_redraw)
        payoff_canvas.bind("<Motion>", self._on_big_payoff_canvas_motion)
        payoff_canvas.bind("<Leave>", lambda _event: self._clear_chart_hover(payoff_canvas))
        notebook.add(payoff_tab, text="到期盈亏图")

        combo_tab = ttk.Frame(notebook, padding=12)
        combo_tab.columnconfigure(0, weight=1)
        combo_tab.rowconfigure(1, weight=1)
        ttk.Label(combo_tab, textvariable=self.combo_summary_text, justify="left", wraplength=1260).grid(
            row=0, column=0, sticky="w", pady=(0, 8)
        )
        combo_canvas = Canvas(combo_tab, background="#ffffff", highlightthickness=0, cursor="crosshair")
        combo_canvas.grid(row=1, column=0, sticky="nsew")
        combo_canvas.bind("<Configure>", self._schedule_big_chart_redraw)
        combo_canvas.bind("<Motion>", self._on_big_combo_canvas_motion)
        combo_canvas.bind("<Leave>", lambda _event: self._clear_chart_hover(combo_canvas))
        self._bind_kline_chart_interactions(combo_canvas, "big_combo")
        notebook.add(combo_tab, text="组合K线")

        volatility_tab = ttk.Frame(notebook, padding=12)
        volatility_tab.columnconfigure(0, weight=1)
        volatility_tab.rowconfigure(1, weight=1)
        ttk.Label(volatility_tab, textvariable=self.volatility_summary_text, justify="left", wraplength=1260).grid(
            row=0, column=0, sticky="w", pady=(0, 8)
        )
        volatility_canvas = Canvas(volatility_tab, background="#ffffff", highlightthickness=0, cursor="crosshair")
        volatility_canvas.grid(row=1, column=0, sticky="nsew")
        volatility_canvas.bind("<Configure>", self._schedule_big_chart_redraw)
        volatility_canvas.bind("<Motion>", self._on_big_volatility_canvas_motion)
        volatility_canvas.bind("<Leave>", lambda _event: self._clear_chart_hover(volatility_canvas))
        self._bind_kline_chart_interactions(volatility_canvas, "big_volatility")
        notebook.add(volatility_tab, text="波动率K线")

        overlay_tab = ttk.Frame(notebook, padding=12)
        overlay_tab.columnconfigure(0, weight=1)
        overlay_tab.rowconfigure(1, weight=3)
        overlay_tab.rowconfigure(2, weight=2)
        ttk.Label(overlay_tab, textvariable=self.overlay_summary_text, justify="left", wraplength=1260).grid(
            row=0, column=0, sticky="w", pady=(0, 8)
        )
        overlay_combo_canvas = Canvas(overlay_tab, background="#ffffff", highlightthickness=0, cursor="crosshair")
        overlay_combo_canvas.grid(row=1, column=0, sticky="nsew", pady=(0, 8))
        overlay_combo_canvas.bind("<Configure>", self._schedule_big_chart_redraw)
        overlay_combo_canvas.bind("<Motion>", self._on_big_overlay_combo_canvas_motion)
        overlay_combo_canvas.bind("<Leave>", self._clear_overlay_chart_hover)
        self._bind_kline_chart_interactions(overlay_combo_canvas, "big_overlay")
        overlay_volatility_canvas = Canvas(overlay_tab, background="#ffffff", highlightthickness=0, cursor="crosshair")
        overlay_volatility_canvas.grid(row=2, column=0, sticky="nsew")
        overlay_volatility_canvas.bind("<Configure>", self._schedule_big_chart_redraw)
        overlay_volatility_canvas.bind("<Motion>", self._on_big_overlay_volatility_canvas_motion)
        overlay_volatility_canvas.bind("<Leave>", self._clear_overlay_chart_hover)
        self._bind_kline_chart_interactions(overlay_volatility_canvas, "big_overlay")
        notebook.add(overlay_tab, text="叠加对比")

        self._big_chart_window = window
        self._big_chart_notebook = notebook
        self._big_payoff_canvas = payoff_canvas
        self._big_combo_canvas = combo_canvas
        self._big_volatility_canvas = volatility_canvas
        self._big_overlay_combo_canvas = overlay_combo_canvas
        self._big_overlay_volatility_canvas = overlay_volatility_canvas
        return window

    def _selected_big_chart_tab(self) -> str:
        notebook = self._big_chart_notebook
        if notebook is None or not notebook.winfo_exists():
            return "到期盈亏图"
        try:
            selected = notebook.select()
            if not selected:
                return "到期盈亏图"
            return str(notebook.tab(selected, "text") or "到期盈亏图")
        except Exception:
            return "到期盈亏图"

    def _bind_kline_chart_interactions(self, canvas: Canvas, key: str) -> None:
        canvas.bind("<MouseWheel>", lambda event, chart_key=key: self._on_kline_chart_mousewheel(chart_key, event))
        canvas.bind("<Button-4>", lambda event, chart_key=key: self._on_kline_chart_mousewheel(chart_key, event))
        canvas.bind("<Button-5>", lambda event, chart_key=key: self._on_kline_chart_mousewheel(chart_key, event))
        canvas.bind("<ButtonPress-1>", lambda event, chart_key=key: self._on_kline_chart_drag_start(chart_key, event), add="+")
        canvas.bind("<B1-Motion>", lambda event, chart_key=key: self._on_kline_chart_drag_motion(chart_key, event), add="+")
        canvas.bind("<ButtonRelease-1>", lambda _event, chart_key=key: self._on_kline_chart_drag_end(chart_key), add="+")

    def _on_kline_chart_mousewheel(self, key: str, event) -> None:
        items = self._kline_items_for_key(key)
        if len(items) < 2:
            return
        delta = getattr(event, "delta", 0)
        if delta == 0:
            num = getattr(event, "num", 0)
            delta = 120 if num == 4 else -120 if num == 5 else 0
        if delta == 0:
            return
        canvas = self._canvas_for_key(key)
        width = max(canvas.winfo_width() if canvas is not None else 0, 1)
        focus_ratio = min(1.0, max(0.0, float(getattr(event, "x", 0)) / width))
        view_state = self._kline_view_states.setdefault(key, KlineChartViewState())
        start, visible = _normalized_kline_view(len(items), view_state.start_index, view_state.visible_count, auto_full=view_state.auto_full)
        new_start, new_visible = _zoom_kline_view(len(items), start, visible, focus_ratio=focus_ratio, zoom_in=delta > 0)
        view_state.start_index = new_start
        view_state.visible_count = new_visible
        view_state.auto_full = new_visible >= len(items) and new_start == 0
        self._redraw_kline_chart(key)

    def _on_kline_chart_drag_start(self, key: str, event) -> None:
        items = self._kline_items_for_key(key)
        if len(items) < 2:
            return
        view_state = self._kline_view_states.setdefault(key, KlineChartViewState())
        start, visible = _normalized_kline_view(len(items), view_state.start_index, view_state.visible_count, auto_full=view_state.auto_full)
        self._kline_drag_states[key] = (float(getattr(event, "x", 0.0)), start, visible)

    def _on_kline_chart_drag_motion(self, key: str, event) -> None:
        drag_state = self._kline_drag_states.get(key)
        if drag_state is None:
            return
        items = self._kline_items_for_key(key)
        if len(items) < 2:
            return
        canvas = self._canvas_for_key(key)
        width = max(canvas.winfo_width() if canvas is not None else 0, 1)
        start_x, base_start, visible = drag_state
        delta_items = int(round(-(float(getattr(event, "x", 0.0)) - start_x) * visible / width))
        new_start, new_visible = _pan_kline_view(len(items), base_start, visible, delta_items=delta_items)
        view_state = self._kline_view_states.setdefault(key, KlineChartViewState())
        view_state.start_index = new_start
        view_state.visible_count = new_visible
        view_state.auto_full = new_visible >= len(items) and new_start == 0
        self._redraw_kline_chart(key)

    def _on_kline_chart_drag_end(self, key: str) -> None:
        self._kline_drag_states.pop(key, None)

    def _canvas_for_key(self, key: str) -> Canvas | None:
        if key == "main_combo":
            return self._combo_canvas
        if key == "big_combo":
            return self._big_combo_canvas
        if key == "big_volatility":
            return self._big_volatility_canvas
        if key == "big_overlay":
            return self._big_overlay_combo_canvas
        return None

    def _kline_items_for_key(self, key: str) -> list[Any]:
        if key in {"main_combo", "big_combo"}:
            combo_candles, _combo_ccy, _converted = self._combo_candles_for_display(self._latest_combo_candles)
            return combo_candles
        if key == "big_volatility":
            return self._current_volatility_candles()
        if key == "big_overlay":
            combo_candles, _combo_ccy, _converted = self._combo_candles_for_display(self._latest_combo_candles)
            return _align_overlay_candles(combo_candles, self._current_volatility_candles())
        return []

    def _apply_kline_view(self, key: str, items: list[Any]) -> list[Any]:
        if not items:
            return items
        view_state = self._kline_view_states.setdefault(key, KlineChartViewState())
        start, visible = _normalized_kline_view(len(items), view_state.start_index, view_state.visible_count, auto_full=view_state.auto_full)
        view_state.start_index = start
        view_state.visible_count = visible
        view_state.auto_full = visible >= len(items) and start == 0
        return items[start : start + visible]

    def _current_volatility_candles(self) -> list[Candle]:
        return _build_volatility_candles_from_reference(
            self._latest_spot_usdt_candles,
            bar=self.bar.get().strip(),
        )

    def _redraw_kline_chart(self, key: str) -> None:
        if key == "main_combo":
            if self._combo_canvas is None or not self._latest_combo_candles:
                return
            combo_candles, combo_ccy, _converted = self._combo_candles_for_display(self._latest_combo_candles)
            visible = self._apply_kline_view(key, combo_candles)
            self._draw_combo_chart(self._combo_canvas, visible, combo_ccy)
            return
        if key == "big_combo":
            if self._big_combo_canvas is None or not self._latest_combo_candles:
                return
            combo_candles, combo_ccy, _converted = self._combo_candles_for_display(self._latest_combo_candles)
            visible = self._apply_kline_view(key, combo_candles)
            self._draw_combo_chart(self._big_combo_canvas, visible, combo_ccy, enable_hover=True, hover_target="big_combo")
            return
        if key == "big_volatility":
            if self._big_volatility_canvas is None:
                return
            volatility_candles = self._current_volatility_candles()
            if not volatility_candles:
                self._clear_canvas(self._big_volatility_canvas, "暂无可用的波动率 K 线数据。")
                return
            visible = self._apply_kline_view(key, volatility_candles)
            self._draw_volatility_chart(self._big_volatility_canvas, visible, enable_hover=True, hover_target="big_volatility")
            return
        if key == "big_overlay":
            if self._big_overlay_combo_canvas is None or self._big_overlay_volatility_canvas is None or not self._latest_combo_candles:
                return
            combo_candles, combo_ccy, _converted = self._combo_candles_for_display(self._latest_combo_candles)
            aligned = _align_overlay_candles(combo_candles, self._current_volatility_candles())
            if not aligned:
                self._clear_canvas(self._big_overlay_combo_canvas, "暂无可用的叠加对比数据。")
                self._clear_canvas(self._big_overlay_volatility_canvas, "暂无可用的叠加对比数据。")
                return
            visible = self._apply_kline_view(key, aligned)
            visible_combo = [item[0] for item in visible]
            visible_volatility = [item[1] for item in visible]
            self._draw_combo_chart(
                self._big_overlay_combo_canvas,
                visible_combo,
                combo_ccy,
                enable_hover=True,
                hover_target="big_overlay_combo",
            )
            self._draw_volatility_chart(
                self._big_overlay_volatility_canvas,
                visible_volatility,
                enable_hover=True,
                hover_target="big_overlay_volatility",
            )

    def _schedule_big_chart_redraw(self, _event=None) -> None:
        window = self._big_chart_window
        if window is None or not window.winfo_exists():
            return
        if self._big_chart_redraw_after_id is not None:
            try:
                window.after_cancel(self._big_chart_redraw_after_id)
            except Exception:
                pass
        self._big_chart_redraw_after_id = window.after(90, self._refresh_big_chart_window)

    def _refresh_big_chart_window(self) -> None:
        window = self._big_chart_window
        if window is None or not window.winfo_exists():
            return
        self._big_chart_redraw_after_id = None
        selected_tab = self._selected_big_chart_tab()

        if selected_tab == "到期盈亏图" and self._big_payoff_canvas is not None:
            if self._latest_payoff_snapshot is not None:
                mode_label = self._payoff_chart_mode_label()
                payoff_snapshot, payoff_ccy = self._payoff_snapshot_for_display(self._latest_payoff_snapshot)
                reference_snapshot: StrategyPayoffSnapshot | None = None
                if (
                    mode_label != "到期盈亏"
                    and self._latest_expiry_payoff_snapshot is not None
                    and self._latest_expiry_payoff_snapshot.points
                ):
                    reference_snapshot, _ = self._payoff_snapshot_for_display(self._latest_expiry_payoff_snapshot)
                self._draw_payoff_chart(
                    self._big_payoff_canvas,
                    payoff_snapshot,
                    payoff_ccy,
                    mode_label,
                    reference_snapshot=reference_snapshot,
                    reference_label="到期盈亏",
                    enable_hover=True,
                    hover_target="big",
                )
            else:
                self._clear_canvas(self._big_payoff_canvas, "加入策略腿后，可生成到期盈亏图。")
            return

        if selected_tab == "组合K线" and self._big_combo_canvas is not None:
            if self._latest_combo_candles:
                combo_candles, combo_ccy, _converted = self._combo_candles_for_display(self._latest_combo_candles)
                visible_combo_candles = self._apply_kline_view("big_combo", combo_candles)
                self._draw_combo_chart(
                    self._big_combo_canvas,
                    visible_combo_candles,
                    combo_ccy,
                    enable_hover=True,
                    hover_target="big_combo",
                )
            else:
                self._clear_canvas(self._big_combo_canvas, "组合 K 线使用期权标记价格；先加入策略腿再生成。")
            return

        volatility_candles = self._current_volatility_candles()
        if selected_tab == "波动率K线" and self._big_volatility_canvas is not None:
            if volatility_candles:
                self.volatility_summary_text.set(
                    f"历史波动率 K 线 | 标的现货 {self.bar.get().strip()} | 根数 {len(volatility_candles)} | 周期与组合K线一致"
                )
                visible_volatility = self._apply_kline_view("big_volatility", volatility_candles)
                self._draw_volatility_chart(
                    self._big_volatility_canvas,
                    visible_volatility,
                    enable_hover=True,
                    hover_target="big_volatility",
                )
            else:
                self.volatility_summary_text.set("历史波动率 K 线需要足够的标的现货历史；当前数据不足。")
                self._clear_canvas(self._big_volatility_canvas, "暂无可用的波动率 K 线数据。")
            return

        if (
            selected_tab == "叠加对比"
            and self._big_overlay_combo_canvas is not None
            and self._big_overlay_volatility_canvas is not None
        ):
            if self._latest_combo_candles and volatility_candles:
                combo_candles, combo_ccy, _converted = self._combo_candles_for_display(self._latest_combo_candles)
                aligned = _align_overlay_candles(combo_candles, volatility_candles)
                self.overlay_summary_text.set(
                    f"叠加对比 | 上图=组合K线({combo_ccy}) | 下图=历史波动率(%) | 共用时间轴 | 周期 {self.bar.get().strip()}"
                )
                visible_aligned = self._apply_kline_view("big_overlay", aligned)
                visible_combo = [item[0] for item in visible_aligned]
                visible_volatility = [item[1] for item in visible_aligned]
                self._draw_combo_chart(
                    self._big_overlay_combo_canvas,
                    visible_combo,
                    combo_ccy,
                    enable_hover=True,
                    hover_target="big_overlay_combo",
                )
                self._draw_volatility_chart(
                    self._big_overlay_volatility_canvas,
                    visible_volatility,
                    enable_hover=True,
                    hover_target="big_overlay_volatility",
                )
            else:
                self.overlay_summary_text.set("叠加对比需要同时具备组合K线与历史波动率K线数据。")
                self._clear_canvas(self._big_overlay_combo_canvas, "暂无可用的叠加对比数据。")
                self._clear_canvas(self._big_overlay_volatility_canvas, "暂无可用的叠加对比数据。")

    def _load_chart_worker(self, request_id: int, candle_limit: int, formula: str) -> None:
        try:
            active_legs = [StrategyLegDefinition(**leg.__dict__) for leg in self._legs if leg.enabled]
            if not active_legs:
                raise ValueError("请先启用至少一条策略腿。")

            family_set = {parse_option_contract(item.inst_id).inst_family for item in active_legs}
            if len(family_set) != 1:
                raise ValueError("当前到期盈亏图只支持同一标的系列的期权组合。")

            latest_quotes: dict[str, OptionQuote] = {}
            resolved_legs: list[ResolvedStrategyLeg] = []
            candles_by_alias: dict[str, list[Candle]] = {}
            current_underlying_price = self._current_underlying_price
            payoff_loaded_at = datetime.now()

            for leg in active_legs:
                instrument = self._instrument_map.get(leg.inst_id) or self.client.get_instrument(leg.inst_id)
                ticker = self.client.get_ticker(leg.inst_id)
                quote = _build_option_quote(instrument, ticker)
                latest_quotes[leg.inst_id] = quote
                if quote.reference_price is None:
                    raise ValueError(f"{leg.inst_id} 当前缺少标记价 / 最新价，无法计算。")
                if current_underlying_price is None and quote.index_price is not None:
                    current_underlying_price = quote.index_price
                resolved_legs.append(resolve_strategy_leg(leg, instrument))
                candles = self.client.get_mark_price_candles(leg.inst_id, self.bar.get().strip(), limit=candle_limit)
                candles_by_alias[leg.alias] = [item for item in candles if item.confirmed]

            spot_usdt_price, spot_usdt_candles = self._load_usdt_reference_context(
                active_legs,
                bar=self.bar.get().strip(),
                limit=candle_limit,
            )
            if current_underlying_price is None and spot_usdt_price is not None:
                current_underlying_price = spot_usdt_price

            payoff_snapshot = build_payoff_snapshot(
                resolved_legs,
                current_underlying_price=current_underlying_price,
            )
            implied_volatility_by_alias = {
                leg.alias: (
                    infer_implied_volatility_for_leg(
                        leg,
                        settlement_price=current_underlying_price,
                        valuation_time=payoff_loaded_at,
                    )
                    if current_underlying_price is not None
                    else None
                )
                for leg in resolved_legs
            }
            normalized_implied_volatility_by_alias = {
                alias: (value if value is not None else Decimal("0.6"))
                for alias, value in implied_volatility_by_alias.items()
            }
            latest_values = {
                leg.alias: latest_quotes[leg.inst_id].reference_price or Decimal("0")
                for leg in active_legs
            }
            combo_candles = build_composite_candles(
                formula,
                candles_by_alias,
                allowed_names=set(latest_values.keys()),
            )
            combo_last = evaluate_linear_formula(formula, latest_values, allowed_names=set(latest_values.keys()))
            self.window.after(
                0,
                lambda: self._apply_chart_snapshot(
                    request_id=request_id,
                    combo_candles=combo_candles,
                    requested_limit=candle_limit,
                    source_counts={alias: len(items) for alias, items in candles_by_alias.items()},
                    payoff_snapshot=payoff_snapshot,
                    latest_quotes=latest_quotes,
                    latest_combo_value=combo_last,
                    spot_usdt_price=spot_usdt_price,
                    spot_usdt_candles=spot_usdt_candles,
                    formula=formula,
                    current_underlying_price=current_underlying_price,
                    resolved_legs=resolved_legs,
                    implied_volatility_by_alias=normalized_implied_volatility_by_alias,
                    payoff_loaded_at=payoff_loaded_at,
                ),
            )
        except Exception as exc:  # noqa: BLE001
            self.window.after(0, lambda error=exc: self._show_chart_error(request_id, error))

    def _load_combo_chart_worker(self, request_id: int, candle_limit: int, formula: str) -> None:
        try:
            active_legs = [StrategyLegDefinition(**leg.__dict__) for leg in self._legs if leg.enabled]
            if not active_legs:
                raise ValueError("请先启用至少一条策略腿。")

            latest_quotes: dict[str, OptionQuote] = {}
            candles_by_alias: dict[str, list[Candle]] = {}
            current_underlying_price = self._current_underlying_price

            for leg in active_legs:
                instrument = self._instrument_map.get(leg.inst_id) or self.client.get_instrument(leg.inst_id)
                ticker = self.client.get_ticker(leg.inst_id)
                quote = _build_option_quote(instrument, ticker)
                latest_quotes[leg.inst_id] = quote
                if quote.reference_price is None:
                    raise ValueError(f"{leg.inst_id} 当前缺少标记价 / 最新价，无法计算。")
                if current_underlying_price is None and quote.index_price is not None:
                    current_underlying_price = quote.index_price
                candles = self.client.get_mark_price_candles(leg.inst_id, self.bar.get().strip(), limit=candle_limit)
                candles_by_alias[leg.alias] = [item for item in candles if item.confirmed]

            latest_values = {
                leg.alias: latest_quotes[leg.inst_id].reference_price or Decimal("0")
                for leg in active_legs
            }
            combo_candles = build_composite_candles(
                formula,
                candles_by_alias,
                allowed_names=set(latest_values.keys()),
            )
            combo_last = evaluate_linear_formula(formula, latest_values, allowed_names=set(latest_values.keys()))
            spot_usdt_price, spot_usdt_candles = self._load_usdt_reference_context(
                active_legs,
                bar=self.bar.get().strip(),
                limit=candle_limit,
            )
            self.window.after(
                0,
                lambda: self._apply_combo_chart_snapshot(
                    request_id=request_id,
                    combo_candles=combo_candles,
                    requested_limit=candle_limit,
                    source_counts={alias: len(items) for alias, items in candles_by_alias.items()},
                    latest_quotes=latest_quotes,
                    latest_combo_value=combo_last,
                    spot_usdt_price=spot_usdt_price,
                    spot_usdt_candles=spot_usdt_candles,
                    formula=formula,
                    current_underlying_price=current_underlying_price,
                ),
            )
        except Exception as exc:  # noqa: BLE001
            self.window.after(0, lambda error=exc: self._show_chart_error(request_id, error))

    def _apply_chart_snapshot(
        self,
        *,
        request_id: int,
        combo_candles: list[Candle],
        requested_limit: int,
        source_counts: dict[str, int],
        payoff_snapshot: StrategyPayoffSnapshot,
        latest_quotes: dict[str, OptionQuote],
        latest_combo_value: Decimal,
        spot_usdt_price: Decimal | None,
        spot_usdt_candles: list[Candle],
        formula: str,
        current_underlying_price: Decimal | None,
        resolved_legs: list[ResolvedStrategyLeg],
        implied_volatility_by_alias: dict[str, Decimal],
        payoff_loaded_at: datetime,
    ) -> None:
        if request_id != self._chart_request_id or not self.window.winfo_exists():
            return
        self._latest_combo_candles = combo_candles
        self._latest_combo_requested_limit = requested_limit
        self._latest_combo_source_counts = dict(source_counts)
        self._latest_expiry_payoff_snapshot = payoff_snapshot
        self._latest_payoff_snapshot = payoff_snapshot
        self._latest_combo_value = latest_combo_value
        self._latest_spot_usdt_price = spot_usdt_price or current_underlying_price
        self._latest_spot_usdt_candles = list(spot_usdt_candles)
        self._latest_chart_formula = formula
        self._latest_resolved_legs = list(resolved_legs)
        self._latest_implied_volatility_by_alias = dict(implied_volatility_by_alias)
        self._latest_payoff_loaded_at = payoff_loaded_at
        self._latest_payoff_expiry_at = (
            max(parse_option_expiry_datetime(item.expiry_code) for item in resolved_legs)
            if resolved_legs
            else None
        )
        self._current_underlying_price = current_underlying_price
        for inst_id, quote in latest_quotes.items():
            self._quotes_by_inst_id[inst_id] = quote
        self._refresh_leg_greeks()
        self._render_legs()
        self._refresh_strategy_summary()
        self._refresh_payoff_simulation()
        self._refresh_chart_display(combo_only=True)
        self.status_text.set("期权策略图表已更新。")

    def _apply_combo_chart_snapshot(
        self,
        *,
        request_id: int,
        combo_candles: list[Candle],
        requested_limit: int,
        source_counts: dict[str, int],
        latest_quotes: dict[str, OptionQuote],
        latest_combo_value: Decimal,
        spot_usdt_price: Decimal | None,
        spot_usdt_candles: list[Candle],
        formula: str,
        current_underlying_price: Decimal | None,
    ) -> None:
        if request_id != self._chart_request_id or not self.window.winfo_exists():
            return
        self._latest_combo_candles = combo_candles
        self._latest_combo_requested_limit = requested_limit
        self._latest_combo_source_counts = dict(source_counts)
        self._latest_combo_value = latest_combo_value
        self._latest_spot_usdt_price = spot_usdt_price or current_underlying_price
        self._latest_spot_usdt_candles = list(spot_usdt_candles)
        self._latest_chart_formula = formula
        self._current_underlying_price = current_underlying_price
        for inst_id, quote in latest_quotes.items():
            self._quotes_by_inst_id[inst_id] = quote
        self._refresh_leg_greeks()
        self._render_legs()
        self._refresh_strategy_summary()
        self._refresh_chart_display(combo_only=True)
        self.status_text.set("组合 K 线已更新。")

    def _on_payoff_time_slider_changed(self, _value=None) -> None:
        self._update_payoff_simulation_labels()
        self._refresh_payoff_simulation()

    def _on_payoff_vol_shift_changed(self, _value=None) -> None:
        self._update_payoff_simulation_labels()
        self._refresh_payoff_simulation()

    def _reset_payoff_simulation_controls(self) -> None:
        self.payoff_time_progress.set(100.0)
        self.payoff_vol_shift_percent.set(0.0)
        self.payoff_sim_date_text.set("估值日 -")
        self.payoff_vol_shift_text.set("波动率平移 0%")

    def _refresh_payoff_simulation(self) -> None:
        if not self._latest_resolved_legs or self._latest_payoff_loaded_at is None:
            self._update_payoff_simulation_labels()
            return
        valuation_time = self._current_payoff_valuation_time()
        if valuation_time is None:
            return
        snapshot = build_simulated_payoff_snapshot(
            self._latest_resolved_legs,
            implied_volatility_by_alias=self._latest_implied_volatility_by_alias,
            valuation_time=valuation_time,
            volatility_shift=self._current_volatility_shift_decimal(),
            current_underlying_price=self._current_underlying_price,
        )
        self._latest_payoff_snapshot = snapshot
        self._update_payoff_simulation_labels()
        self._refresh_chart_display(combo_only=False)

    def _current_payoff_valuation_time(self) -> datetime | None:
        if self._latest_payoff_loaded_at is None or self._latest_payoff_expiry_at is None:
            return None
        start_time = self._latest_payoff_loaded_at
        end_time = self._latest_payoff_expiry_at
        if end_time <= start_time:
            return end_time
        progress = max(0.0, min(float(self.payoff_time_progress.get()), 100.0)) / 100.0
        total_seconds = (end_time - start_time).total_seconds()
        return start_time + ((end_time - start_time) * progress if total_seconds > 0 else end_time - start_time)

    def _current_volatility_shift_decimal(self) -> Decimal:
        return Decimal(str(self.payoff_vol_shift_percent.get() / 100.0))

    def _update_payoff_simulation_labels(self) -> None:
        valuation_time = self._current_payoff_valuation_time()
        progress = max(0.0, min(float(self.payoff_time_progress.get()), 100.0))
        if valuation_time is None:
            self.payoff_sim_date_text.set("估值日 -")
        else:
            self.payoff_sim_date_text.set(
                f"估值日 {valuation_time.strftime('%Y-%m-%d')} | 时间进度 {int(round(progress))}%"
            )
        self.payoff_vol_shift_text.set(
            f"波动率平移 {_format_signed_percent(Decimal(str(self.payoff_vol_shift_percent.get())))}"
        )

    def _payoff_chart_mode_label(self) -> str:
        progress = max(0.0, min(float(self.payoff_time_progress.get()), 100.0))
        vol_shift = abs(float(self.payoff_vol_shift_percent.get()))
        if progress >= 99.999 and vol_shift < 0.0001:
            return "到期盈亏"
        valuation_time = self._current_payoff_valuation_time()
        if (
            valuation_time is not None
            and self._latest_payoff_loaded_at is not None
            and valuation_time.date() == self._latest_payoff_loaded_at.date()
        ):
            return "当日模拟盈亏"
        return "模拟盈亏"

    def _refresh_chart_display(self, *, combo_only: bool = False) -> None:
        formula = self._latest_chart_formula or self.formula.get().strip() or build_default_formula(self._legs)

        if not combo_only and self._latest_payoff_snapshot is not None:
            mode_label = self._payoff_chart_mode_label()
            payoff_snapshot, payoff_ccy = self._payoff_snapshot_for_display(self._latest_payoff_snapshot)
            reference_snapshot: StrategyPayoffSnapshot | None = None
            if (
                mode_label != "到期盈亏"
                and self._latest_expiry_payoff_snapshot is not None
                and self._latest_expiry_payoff_snapshot.points
            ):
                reference_snapshot, _ = self._payoff_snapshot_for_display(self._latest_expiry_payoff_snapshot)
            break_even_text = (
                " / ".join(_format_compact_number(item) for item in self._latest_payoff_snapshot.break_even_prices)
                if self._latest_payoff_snapshot.break_even_prices
                else "无"
            )
            underlying_text = (
                f"当前标的≈{_format_compact_number(self._current_underlying_price)}"
                if self._current_underlying_price is not None
                else "当前标的指数暂不可用"
            )
            valuation_time = self._current_payoff_valuation_time()
            valuation_text = valuation_time.strftime("%Y-%m-%d") if valuation_time is not None else "-"
            compare_text = " | 叠加到期盈亏对比" if reference_snapshot is not None else ""
            self.payoff_summary_text.set(
                f"{underlying_text} | 单位 {payoff_ccy} | 估值日 {valuation_text} | 波动率平移 {_format_signed_percent(self._current_volatility_shift_decimal() * Decimal('100'))}\n"
                f"净权利金 {_format_compact_number(payoff_snapshot.net_premium)} | 盈亏平衡点 {break_even_text}{compare_text}"
            )
            if self._payoff_canvas is not None:
                self._draw_payoff_chart(
                    self._payoff_canvas,
                    payoff_snapshot,
                    payoff_ccy,
                    mode_label,
                    reference_snapshot=reference_snapshot,
                    reference_label="到期盈亏",
                )

        if self._latest_combo_candles:
            combo_candles, combo_ccy, converted = self._combo_candles_for_display(self._latest_combo_candles)
            latest_candle = combo_candles[-1] if combo_candles else None
            latest_value_text = (
                _format_compact_number(latest_candle.close)
                if latest_candle is not None
                else _format_compact_number(self._latest_combo_value)
            )
            latest_candle_text = (
                f"O {_format_compact_number(latest_candle.open)} / H {_format_compact_number(latest_candle.high)} / "
                f"L {_format_compact_number(latest_candle.low)} / C {_format_compact_number(latest_candle.close)}"
                if latest_candle is not None
                else "暂无组合 K 线"
            )
            note = ""
            if self._display_in_usdt() and not converted:
                note = f" | 缺少 {_native_display_currency(self._legs, self._instrument_map)}-USDT 历史，当前按结算币显示"
            alignment_note = ""
            if self._latest_combo_requested_limit is not None:
                requested_text = f"请求 {self._latest_combo_requested_limit} 根"
                actual_text = f"实际 {len(combo_candles)} 根"
                if self._latest_combo_source_counts:
                    counts_text = " / ".join(
                        f"{alias}={count}" for alias, count in sorted(self._latest_combo_source_counts.items())
                    )
                    alignment_note = f" | 多腿共同时间对齐（{requested_text}，{actual_text}；各腿 {counts_text}）"
                else:
                    alignment_note = f" | {requested_text}，{actual_text}"
            self.combo_summary_text.set(
                f"公式: {formula}\n"
                f"周期 {self.bar.get().strip()} | 根数 {len(combo_candles)} | 单位 {combo_ccy} | 最新组合值 {latest_value_text} | {latest_candle_text}{note}{alignment_note}"
            )
            if self._combo_canvas is not None:
                visible_combo_candles = self._apply_kline_view("main_combo", combo_candles)
                self._draw_combo_chart(self._combo_canvas, visible_combo_candles, combo_ccy)
        if self._big_chart_window is not None and self._big_chart_window.winfo_exists():
            self._refresh_big_chart_window()

    def _display_in_usdt(self) -> bool:
        return self.chart_display_ccy.get().strip().upper() == "USDT"

    def _payoff_snapshot_for_display(
        self,
        snapshot: StrategyPayoffSnapshot,
    ) -> tuple[StrategyPayoffSnapshot, str]:
        if not self._display_in_usdt():
            return snapshot, _native_display_currency(self._legs, self._instrument_map)
        reference_price = self._latest_spot_usdt_price or snapshot.current_underlying_price
        return convert_payoff_snapshot_to_usdt(snapshot, reference_price=reference_price), "USDT"

    def _combo_candles_for_display(self, candles: list[Candle]) -> tuple[list[Candle], str, bool]:
        native_ccy = _native_display_currency(self._legs, self._instrument_map)
        if not self._display_in_usdt():
            return candles, native_ccy, True
        if not self._latest_spot_usdt_candles:
            return candles, native_ccy, False
        converted = convert_candles_by_reference(candles, self._latest_spot_usdt_candles)
        if not converted:
            return candles, native_ccy, False
        return converted, "USDT", True

    def _load_usdt_reference_context(
        self,
        active_legs: list[StrategyLegDefinition],
        *,
        bar: str,
        limit: int,
    ) -> tuple[Decimal | None, list[Candle]]:
        families = {parse_option_contract(item.inst_id).inst_family for item in active_legs}
        if len(families) != 1:
            return None, []
        spot_inst_id = _spot_usdt_inst_id(next(iter(families)))
        if not spot_inst_id:
            return None, []

        spot_price: Decimal | None = None
        spot_candles: list[Candle] = []
        try:
            spot_ticker = self.client.get_ticker(spot_inst_id)
            spot_price = spot_ticker.last or spot_ticker.bid or spot_ticker.ask
        except Exception:
            spot_price = None
        try:
            spot_candles = [
                item
                for item in self.client.get_candles_history(spot_inst_id, bar, limit=limit)
                if item.confirmed
            ]
        except Exception:
            spot_candles = []
        return spot_price, spot_candles

    def _show_chart_error(self, request_id: int, exc: Exception) -> None:
        if request_id != self._chart_request_id:
            return
        self.status_text.set("图表生成失败")
        messagebox.showerror("图表生成失败", str(exc), parent=self.window)

    def save_current_strategy(self) -> None:
        name = self.strategy_name.get().strip()
        if not name:
            messagebox.showerror("保存策略失败", "请先填写策略名称。", parent=self.window)
            return
        if not self._legs:
            messagebox.showerror("保存策略失败", "当前没有可保存的策略腿。", parent=self.window)
            return

        records = list(self._saved_strategies)
        existing_index = next((index for index, item in enumerate(records) if str(item.get("name", "")).strip() == name), None)
        if existing_index is not None:
            confirmed = messagebox.askyesno("保存策略", f"策略 {name} 已存在，是否覆盖？", parent=self.window)
            if not confirmed:
                return

        payload = {
            "name": name,
            "option_family": self.option_family.get().strip().upper(),
            "expiry_code": self._selected_expiry_code(),
            "bar": self.bar.get().strip(),
            "candle_limit": self.candle_limit.get().strip(),
            "chart_display_ccy": self.chart_display_ccy.get().strip(),
            "formula": self.formula.get().strip(),
            "legs": [
                {
                    "alias": item.alias,
                    "inst_id": item.inst_id,
                    "side": item.side,
                    "quantity": format_decimal(item.quantity),
                    "premium": format_decimal(item.premium) if item.premium is not None else "",
                    "delta": format_decimal(item.delta) if item.delta is not None else "",
                    "gamma": format_decimal(item.gamma) if item.gamma is not None else "",
                    "theta": format_decimal(item.theta) if item.theta is not None else "",
                    "vega": format_decimal(item.vega) if item.vega is not None else "",
                    "enabled": item.enabled,
                }
                for item in self._legs
            ],
        }
        if existing_index is not None:
            records[existing_index] = payload
        else:
            records.append(payload)
        try:
            save_option_strategies_snapshot(records)
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("保存策略失败", str(exc), parent=self.window)
            return
        self._saved_strategies = records
        self.saved_strategy_name.set(name)
        self._refresh_saved_strategy_options()
        self.status_text.set(f"策略 {name} 已保存。")

    def load_selected_strategy(self) -> None:
        name = self.saved_strategy_name.get().strip()
        if not name:
            messagebox.showinfo("加载策略", "请先从已保存策略里选择一个名称。", parent=self.window)
            return
        record = next((item for item in self._saved_strategies if str(item.get("name", "")).strip() == name), None)
        if record is None:
            messagebox.showerror("加载策略失败", "没有找到对应的策略记录。", parent=self.window)
            return
        try:
            self._apply_saved_strategy(record)
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("加载策略失败", str(exc), parent=self.window)
            return
        self.status_text.set(f"策略 {name} 已加载。")

    def delete_selected_strategy(self) -> None:
        name = self.saved_strategy_name.get().strip()
        if not name:
            messagebox.showinfo("删除策略", "请先从已保存策略里选择一个名称。", parent=self.window)
            return
        record = next((item for item in self._saved_strategies if str(item.get("name", "")).strip() == name), None)
        if record is None:
            messagebox.showerror("删除策略失败", "没有找到对应的策略记录。", parent=self.window)
            return
        confirmed = messagebox.askyesno("删除策略", f"确定删除策略 {name} 吗？", parent=self.window)
        if not confirmed:
            return

        records = [item for item in self._saved_strategies if str(item.get("name", "")).strip() != name]
        try:
            save_option_strategies_snapshot(records)
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("删除策略失败", str(exc), parent=self.window)
            return

        self._saved_strategies = records
        if self.strategy_name.get().strip() == name:
            self.strategy_name.set("")
        self.saved_strategy_name.set("")
        self._refresh_saved_strategy_options()
        self.status_text.set(f"策略 {name} 已删除。")

    def _apply_saved_strategy(self, record: dict[str, object]) -> None:
        self.strategy_name.set(str(record.get("name", "")))
        self.option_family.set(str(record.get("option_family", "")).strip().upper())
        self.expiry_code.set(str(record.get("expiry_code", "")).strip())
        self.bar.set(str(record.get("bar", self.bar.get())).strip() or "15m")
        self.candle_limit.set(str(record.get("candle_limit", self.candle_limit.get())).strip() or "2000")
        self.chart_display_ccy.set(str(record.get("chart_display_ccy", self.chart_display_ccy.get())).strip() or "USDT")
        self.formula.set(str(record.get("formula", "")).strip())

        raw_legs = record.get("legs", [])
        if not isinstance(raw_legs, list):
            raise ValueError("策略腿数据格式无效。")
        restored: list[StrategyLegDefinition] = []
        max_alias_index = 0
        for raw in raw_legs:
            if not isinstance(raw, dict):
                continue
            alias = str(raw.get("alias", "")).strip()
            inst_id = str(raw.get("inst_id", "")).strip().upper()
            side = str(raw.get("side", "buy")).strip().lower()
            enabled = bool(raw.get("enabled", True))
            quantity = self._parse_positive_decimal(str(raw.get("quantity", "1")), "策略腿数量")
            premium_text = str(raw.get("premium", "")).strip()
            premium = Decimal(premium_text) if premium_text else None
            delta_text = str(raw.get("delta", "")).strip()
            gamma_text = str(raw.get("gamma", "")).strip()
            theta_text = str(raw.get("theta", "")).strip()
            vega_text = str(raw.get("vega", "")).strip()
            if not alias or not inst_id or side not in {"buy", "sell"}:
                continue
            restored.append(
                StrategyLegDefinition(
                    alias=alias,
                    inst_id=inst_id,
                    side="buy" if side == "buy" else "sell",
                    quantity=quantity,
                    premium=premium,
                    delta=Decimal(delta_text) if delta_text else None,
                    gamma=Decimal(gamma_text) if gamma_text else None,
                    theta=Decimal(theta_text) if theta_text else None,
                    vega=Decimal(vega_text) if vega_text else None,
                    enabled=enabled,
                )
            )
            if alias.startswith("L") and alias[1:].isdigit():
                max_alias_index = max(max_alias_index, int(alias[1:]))

        if not restored:
            raise ValueError("策略里没有可用的策略腿。")
        self._alias_counter = max(self._alias_counter, max_alias_index)
        self._legs = restored
        for leg in restored:
            if leg.inst_id in self._instrument_map:
                continue
            try:
                self._instrument_map[leg.inst_id] = self.client.get_instrument(leg.inst_id)
            except Exception:
                pass
        self._sync_expiry_options(preferred=self.expiry_code.get().strip())
        self._refresh_leg_greeks()
        self._render_legs()
        self._refresh_strategy_summary()
        if self.option_family.get().strip() and self._selected_expiry_code():
            self.refresh_chain()
        self.refresh_charts()

    def _selected_expiry_code(self) -> str:
        raw = self.expiry_code.get().strip()
        if " " in raw:
            raw = raw.split(" ", 1)[0]
        if "(" in raw:
            raw = raw.split("(", 1)[0].strip()
        return raw

    def _apply_cached_expiry_selection(self) -> bool:
        family = self.option_family.get().strip().upper()
        expiry = self._selected_expiry_code()
        if not family or not expiry:
            return False

        family_instruments = self._family_instruments_cache.get(family)
        tickers_by_inst_id = self._family_tickers_cache.get(family)
        if not family_instruments or not tickers_by_inst_id:
            return False

        selected_instruments = [
            item for item in family_instruments if parse_option_contract(item.inst_id).expiry_code == expiry
        ]
        if not selected_instruments:
            return False

        quotes = [_build_option_quote(instrument, tickers_by_inst_id.get(instrument.inst_id)) for instrument in selected_instruments]
        chain_rows = build_option_chain_rows(quotes)
        underlying_price = next(
            (item.index_price for item in quotes if item.index_price is not None),
            self._current_underlying_price,
        )
        expiries = sorted({parse_option_contract(item.inst_id).expiry_code for item in family_instruments})
        self._chain_request_id += 1
        self._apply_chain_snapshot(
            request_id=self._chain_request_id,
            family=family,
            expiry=expiry,
            expiries=expiries,
            chain_rows=chain_rows,
            quotes=quotes,
            tickers_by_inst_id=None,
            family_instruments=list(family_instruments),
            underlying_price=underlying_price,
        )
        self.status_text.set(f"已切换到 {family} {expiry}，期权链已按当前到期日更新。")
        return True

    def _current_chain_context_text(self) -> str:
        family = self.option_family.get().strip().upper()
        expiry = self._selected_expiry_code()
        parts: list[str] = []
        if family:
            parts.append(family)
        if expiry:
            parts.append(f"{expiry} ({format_option_expiry_label(expiry)})")
        return " | ".join(parts)

    def _set_chain_selection_text(self, detail: str) -> None:
        context = self._current_chain_context_text()
        self.chain_selection_text.set(f"{context} | {detail}" if context else detail)

    def _update_chain_context_ui(self, *, row_count: int | None = None) -> None:
        context = self._current_chain_context_text()
        title = "期权链"
        if context:
            title = f"{title} | {context}"
        if row_count is not None:
            title = f"{title} | {row_count} 个行权价"
        if self._chain_frame is not None:
            self._chain_frame.configure(text=title)
        if self._chain_tree is not None:
            expiry = self._selected_expiry_code()
            strike_heading = f"行权价 ({expiry})" if expiry else "行权价"
            self._chain_tree.heading("strike", text=strike_heading)

    def _refresh_strategy_summary(self) -> None:
        if not self._legs:
            self.strategy_summary_text.set("暂无策略腿。")
            return
        formula = self.formula.get().strip() or build_default_formula(self._legs)
        aliases = {item.alias for item in self._legs if item.alias.strip()}
        combo_value: str = "-"
        try:
            latest_values = {
                leg.alias: (self._quotes_by_inst_id.get(leg.inst_id).reference_price if self._quotes_by_inst_id.get(leg.inst_id) else leg.premium)
                for leg in self._legs
            }
            if all(value is not None for value in latest_values.values()):
                combo_value = _format_compact_number(
                    evaluate_linear_formula(
                        formula,
                        {name: value for name, value in latest_values.items() if isinstance(value, Decimal)},
                        allowed_names=aliases,
                    )
                )
        except Exception:
            combo_value = "-"

        net_premium: Decimal | None = Decimal("0")
        premium_ccy: str | None = None
        for leg in self._legs:
            instrument = self._instrument_map.get(leg.inst_id)
            if instrument is None or leg.premium is None:
                net_premium = None
                break
            currency = instrument.ct_val_ccy or leg.inst_id.split("-", 1)[0]
            if premium_ccy is None:
                premium_ccy = currency
            elif premium_ccy != currency:
                net_premium = None
                break
            direction = Decimal("1") if leg.side == "buy" else Decimal("-1")
            premium_cost = leg.premium * option_contract_value(instrument) * leg.quantity
            net_premium += -direction * premium_cost

        premium_text = (
            f"{_format_compact_number(net_premium)} {premium_ccy or ''}".strip()
            if net_premium is not None
            else "跨币种/待刷新"
        )
        underlying_text = (
            f" | 标的≈{_format_compact_number(self._current_underlying_price)}"
            if self._current_underlying_price
            else ""
        )
        self.strategy_summary_text.set(
            f"策略腿 {len(self._legs)} 条 | 净权利金 {premium_text} | 当前组合值 {combo_value}{underlying_text}\n"
            f"组合公式 {formula or '-'}"
        )

    def _draw_payoff_chart(
        self,
        canvas: Canvas,
        snapshot: StrategyPayoffSnapshot,
        value_ccy: str,
        mode_label: str,
        *,
        reference_snapshot: StrategyPayoffSnapshot | None = None,
        reference_label: str = "到期盈亏",
        enable_hover: bool = True,
        hover_target: str = "main",
    ) -> None:
        points = list(snapshot.points)
        if not points:
            self._clear_canvas(canvas, "暂无到期盈亏数据。")
            return
        reference_points = list(reference_snapshot.points) if reference_snapshot is not None else []
        show_reference = bool(reference_points) and mode_label != reference_label

        canvas.delete("all")
        canvas.update_idletasks()
        width = max(canvas.winfo_width(), 960)
        height = max(canvas.winfo_height(), 420)
        left = 66
        right = 24
        top = 22
        bottom = 40
        inner_width = width - left - right
        inner_height = height - top - bottom
        if inner_width <= 0 or inner_height <= 0:
            return

        all_points = points + reference_points
        pnl_values = [item.pnl for item in all_points]
        min_pnl = min(pnl_values)
        max_pnl = max(pnl_values)
        if min_pnl == max_pnl:
            min_pnl -= Decimal("1")
            max_pnl += Decimal("1")
        if min_pnl > 0:
            min_pnl = Decimal("0")
        if max_pnl < 0:
            max_pnl = Decimal("0")

        price_min = min(item.underlying_price for item in all_points)
        price_max = max(item.underlying_price for item in all_points)

        def x_for(price: Decimal) -> float:
            ratio = (price - price_min) / max(price_max - price_min, Decimal("0.00000001"))
            return left + float(ratio) * inner_width

        def y_for(pnl: Decimal) -> float:
            ratio = (max_pnl - pnl) / max(max_pnl - min_pnl, Decimal("0.00000001"))
            return top + float(ratio) * inner_height

        bounds = ChartBounds(left=float(left), top=float(top), right=float(width - right), bottom=float(height - bottom))
        canvas.create_rectangle(left, top, width - right, height - bottom, outline="#d0d7de")
        zero_y = y_for(Decimal("0"))
        canvas.create_line(left, zero_y, width - right, zero_y, fill="#8c959f", dash=(4, 4))

        for value in _axis_values(min_pnl, max_pnl, steps=4):
            y = y_for(value)
            canvas.create_line(left, y, width - right, y, fill="#eaeef2", dash=(2, 4))
            canvas.create_text(
                left - 8,
                y,
                text=_format_axis_value(value),
                anchor="e",
                fill="#57606a",
                font=("Microsoft YaHei UI", 9),
            )

        self._draw_payoff_fill(canvas, points, x_for, y_for, zero_y)

        x_positions = tuple(x_for(point.underlying_price) for point in points)
        primary_y_positions = tuple(y_for(point.pnl) for point in points)
        primary_line_color = "#0f766e" if show_reference else "#0969da"
        reference_y_positions: tuple[float, ...] = ()
        reference_line_color = "#8256d0"
        if show_reference:
            reference_y_positions = tuple(y_for(point.pnl) for point in reference_points)
            reference_line_points: list[float] = []
            for point in reference_points:
                reference_line_points.extend((x_for(point.underlying_price), y_for(point.pnl)))
            if len(reference_line_points) >= 4:
                canvas.create_line(*reference_line_points, fill=reference_line_color, width=2)

        line_points: list[float] = []
        for x, y in zip(x_positions, primary_y_positions):
            line_points.extend((x, y))
        if len(line_points) >= 4:
            canvas.create_line(*line_points, fill=primary_line_color, width=2)

        if snapshot.current_underlying_price is not None:
            current_x = x_for(snapshot.current_underlying_price)
            canvas.create_line(current_x, top, current_x, height - bottom, fill="#bf8700", width=2)
            canvas.create_text(
                current_x + 6,
                top + 8,
                text=f"当前 {_format_compact_number(snapshot.current_underlying_price)}",
                anchor="nw",
                fill="#9a6700",
                font=("Microsoft YaHei UI", 9, "bold"),
            )

        for break_even in snapshot.break_even_prices:
            x = x_for(break_even)
            canvas.create_line(x, top, x, height - bottom, fill="#cf222e", dash=(3, 4))
            canvas.create_line(x, zero_y - 6, x, zero_y + 6, fill="#cf222e", width=2)

        for index in _index_markers(len(points), target_count=6):
            point = points[index]
            x = x_for(point.underlying_price)
            canvas.create_line(x, top, x, height - bottom, fill="#f3f4f6", dash=(2, 4))
            is_first = index == 0
            is_last = index == len(points) - 1
            canvas.create_text(
                x,
                height - bottom + 8,
                text=_format_compact_number(point.underlying_price),
                anchor="nw" if is_first else "ne" if is_last else "n",
                fill="#57606a",
                font=("Microsoft YaHei UI", 9),
            )

        legend_text = f"{mode_label} ({value_ccy}) | 绿色=盈利 | 红色=亏损 | 红虚线=盈亏平衡点"
        if show_reference:
            legend_text = (
                f"{mode_label}/{reference_label} ({value_ccy}) | 绿线={mode_label} | 紫线={reference_label} "
                f"| 绿色=盈利 | 红色=亏损"
            )
        canvas.create_text(
            width - right,
            top + 10,
            text=legend_text,
            anchor="ne",
            fill="#57606a",
            font=("Microsoft YaHei UI", 9, "bold"),
        )
        if enable_hover:
            hover_state = PayoffChartHoverState(
                bounds=bounds,
                primary_points=tuple(points),
                reference_points=tuple(reference_points) if show_reference else tuple(),
                x_positions=x_positions,
                primary_y_positions=primary_y_positions,
                reference_y_positions=reference_y_positions,
                value_ccy=value_ccy,
                primary_label=mode_label,
                reference_label=reference_label if show_reference else "",
                primary_color=primary_line_color,
                reference_color=reference_line_color if show_reference else "",
            )
            if hover_target == "big":
                self._big_payoff_hover_state = hover_state
            else:
                self._payoff_hover_state = hover_state

    def _draw_payoff_fill(
        self,
        canvas: Canvas,
        points: list[StrategyPayoffPoint],
        x_for,
        y_for,
        zero_y: float,
    ) -> None:
        for previous, current in zip(points, points[1:]):
            x1 = x_for(previous.underlying_price)
            y1 = y_for(previous.pnl)
            x2 = x_for(current.underlying_price)
            y2 = y_for(current.pnl)
            if (previous.pnl >= 0 and current.pnl >= 0) or (previous.pnl <= 0 and current.pnl <= 0):
                fill = "#c6f6d5" if previous.pnl >= 0 and current.pnl >= 0 else "#fecaca"
                canvas.create_polygon(x1, zero_y, x1, y1, x2, y2, x2, zero_y, outline="", fill=fill)
                continue
            delta = current.pnl - previous.pnl
            if delta == 0:
                continue
            ratio = -previous.pnl / delta
            cross_price = previous.underlying_price + ((current.underlying_price - previous.underlying_price) * ratio)
            cross_x = x_for(cross_price)
            if previous.pnl > 0:
                canvas.create_polygon(x1, zero_y, x1, y1, cross_x, zero_y, outline="", fill="#c6f6d5")
                canvas.create_polygon(cross_x, zero_y, x2, y2, x2, zero_y, outline="", fill="#fecaca")
            else:
                canvas.create_polygon(x1, zero_y, x1, y1, cross_x, zero_y, outline="", fill="#fecaca")
                canvas.create_polygon(cross_x, zero_y, x2, y2, x2, zero_y, outline="", fill="#c6f6d5")

    def _draw_combo_chart(
        self,
        canvas: Canvas,
        candles: list[Candle],
        value_ccy: str,
        *,
        enable_hover: bool = True,
        hover_target: str = "main_combo",
    ) -> None:
        if not candles:
            self._clear_canvas(canvas, "没有可用的组合 K 线数据。")
            return

        hide_wicks = bool(self.combo_hide_wicks.get())
        display_candles = [
            Candle(
                ts=candle.ts,
                open=candle.open,
                high=max(candle.open, candle.close) if hide_wicks else candle.high,
                low=min(candle.open, candle.close) if hide_wicks else candle.low,
                close=candle.close,
                volume=candle.volume,
                confirmed=candle.confirmed,
            )
            for candle in candles
        ]

        canvas.delete("all")
        width = max(canvas.winfo_width(), 960)
        height = max(canvas.winfo_height(), 420)
        left = 62
        right = 24
        top = 22
        bottom = 38
        inner_width = width - left - right
        inner_height = height - top - bottom
        if inner_width <= 0 or inner_height <= 0:
            return

        price_max = max(item.high for item in display_candles)
        price_min = min(item.low for item in display_candles)
        if price_max == price_min:
            price_max += Decimal("1")
            price_min -= Decimal("1")

        candle_step = inner_width / max(len(candles), 1)
        body_width = max(2.0, min(10.0, candle_step * 0.62))

        def x_for(index: int) -> float:
            return left + (index * candle_step) + (candle_step / 2)

        def y_for(price: Decimal) -> float:
            ratio = (price_max - price) / max(price_max - price_min, Decimal("0.00000001"))
            return top + float(ratio) * inner_height

        bounds = ChartBounds(left=float(left), top=float(top), right=float(width - right), bottom=float(height - bottom))
        canvas.create_rectangle(left, top, width - right, height - bottom, outline="#d0d7de")

        for price_value in _axis_values(price_min, price_max, steps=4):
            y = y_for(price_value)
            canvas.create_line(left, y, width - right, y, fill="#eaeef2", dash=(2, 4))
            canvas.create_text(
                left - 8,
                y,
                text=_format_axis_value(price_value),
                anchor="e",
                fill="#57606a",
                font=("Microsoft YaHei UI", 9),
            )

        x_positions: list[float] = []
        close_y_positions: list[float] = []
        for index, candle in enumerate(candles):
            display_candle = display_candles[index]
            x = x_for(index)
            open_y = y_for(candle.open)
            close_y = y_for(candle.close)
            high_y = y_for(display_candle.high)
            low_y = y_for(display_candle.low)
            x_positions.append(x)
            close_y_positions.append(close_y)
            color = "#1a7f37" if candle.close >= candle.open else "#cf222e"
            if not hide_wicks:
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

        for index in _index_markers(len(candles), target_count=6):
            x = x_for(index)
            canvas.create_line(x, top, x, height - bottom, fill="#f3f4f6", dash=(2, 4))
            is_first = index == 0
            is_last = index == len(candles) - 1
            canvas.create_text(
                x,
                height - bottom + 8,
                text=_format_chart_ts(candles[index].ts),
                anchor="nw" if is_first else "ne" if is_last else "n",
                fill="#57606a",
                font=("Microsoft YaHei UI", 9),
            )

        latest = candles[-1]
        canvas.create_text(
            width - right,
            top + 10,
            text=(
                f"标记价格组合K线 ({value_ccy}) | 最新 O {_format_compact_number(latest.open)} "
                f"H {_format_compact_number(latest.high)} L {_format_compact_number(latest.low)} C {_format_compact_number(latest.close)}"
            ),
            anchor="ne",
            fill="#57606a",
            font=("Microsoft YaHei UI", 9, "bold"),
        )
        if enable_hover:
            hover_state = ComboChartHoverState(
                bounds=bounds,
                candles=tuple(candles),
                x_positions=tuple(x_positions),
                close_y_positions=tuple(close_y_positions),
                candle_step=candle_step,
                value_ccy=value_ccy,
            )
            if hover_target == "main_combo":
                self._combo_hover_state = hover_state
            elif hover_target == "big_combo":
                self._big_combo_hover_state = hover_state
            elif hover_target == "big_overlay_combo":
                self._big_overlay_combo_hover_state = hover_state

    def _draw_volatility_chart(
        self,
        canvas: Canvas,
        candles: list[Candle],
        *,
        enable_hover: bool = False,
        hover_target: str = "big_volatility",
    ) -> None:
        if not candles:
            self._clear_canvas(canvas, "暂无可用的波动率 K 线数据。")
            return

        canvas.delete("all")
        width = max(canvas.winfo_width(), 960)
        height = max(canvas.winfo_height(), 420)
        left = 62
        right = 24
        top = 22
        bottom = 38
        inner_width = width - left - right
        inner_height = height - top - bottom
        if inner_width <= 0 or inner_height <= 0:
            return

        value_max = max(item.high for item in candles)
        value_min = min(item.low for item in candles)
        if value_max == value_min:
            value_max += Decimal("1")
            value_min -= Decimal("1")

        candle_step = inner_width / max(len(candles), 1)
        body_width = max(2.0, min(10.0, candle_step * 0.62))

        def x_for(index: int) -> float:
            return left + (index * candle_step) + (candle_step / 2)

        def y_for(value: Decimal) -> float:
            ratio = (value_max - value) / max(value_max - value_min, Decimal("0.00000001"))
            return top + float(ratio) * inner_height

        bounds = ChartBounds(left=float(left), top=float(top), right=float(width - right), bottom=float(height - bottom))
        canvas.create_rectangle(left, top, width - right, height - bottom, outline="#d0d7de")

        for price_value in _axis_values(value_min, value_max, steps=4):
            y = y_for(price_value)
            canvas.create_line(left, y, width - right, y, fill="#eaeef2", dash=(2, 4))
            canvas.create_text(
                left - 8,
                y,
                text=f"{_format_axis_value(price_value)}%",
                anchor="e",
                fill="#57606a",
                font=("Microsoft YaHei UI", 9),
            )

        x_positions: list[float] = []
        close_y_positions: list[float] = []
        for index, candle in enumerate(candles):
            x = x_for(index)
            open_y = y_for(candle.open)
            close_y = y_for(candle.close)
            high_y = y_for(candle.high)
            low_y = y_for(candle.low)
            x_positions.append(x)
            close_y_positions.append(close_y)
            color = "#1a7f37" if candle.close >= candle.open else "#cf222e"
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

        for index in _index_markers(len(candles), target_count=6):
            x = x_for(index)
            canvas.create_line(x, top, x, height - bottom, fill="#f3f4f6", dash=(2, 4))
            is_first = index == 0
            is_last = index == len(candles) - 1
            canvas.create_text(
                x,
                height - bottom + 8,
                text=_format_chart_ts(candles[index].ts),
                anchor="nw" if is_first else "ne" if is_last else "n",
                fill="#57606a",
                font=("Microsoft YaHei UI", 9),
            )

        latest = candles[-1]
        canvas.create_text(
            width - right,
            top + 10,
            text=(
                f"历史波动率K线 (%) | 最新 O {_format_compact_number(latest.open)} "
                f"H {_format_compact_number(latest.high)} L {_format_compact_number(latest.low)} C {_format_compact_number(latest.close)}"
            ),
            anchor="ne",
            fill="#57606a",
            font=("Microsoft YaHei UI", 9, "bold"),
        )
        if enable_hover:
            hover_state = ComboChartHoverState(
                bounds=bounds,
                candles=tuple(candles),
                x_positions=tuple(x_positions),
                close_y_positions=tuple(close_y_positions),
                candle_step=candle_step,
                value_ccy="%",
            )
            if hover_target == "big_volatility":
                self._big_volatility_hover_state = hover_state
            elif hover_target == "big_overlay_volatility":
                self._big_overlay_volatility_hover_state = hover_state

    def _draw_overlay_chart(
        self,
        canvas: Canvas,
        combo_candles: list[Candle],
        combo_ccy: str,
        volatility_candles: list[Candle],
    ) -> None:
        aligned = _align_overlay_candles(combo_candles, volatility_candles)
        if len(aligned) < 2:
            self._clear_canvas(canvas, "组合K线与波动率K线缺少重叠时间，无法叠加显示。")
            return

        canvas.delete("all")
        width = max(canvas.winfo_width(), 960)
        height = max(canvas.winfo_height(), 420)
        left = 62
        right = 68
        top = 22
        bottom = 38
        inner_width = width - left - right
        inner_height = height - top - bottom
        if inner_width <= 0 or inner_height <= 0:
            return

        combo_only = [item[0] for item in aligned]
        vol_only = [item[1] for item in aligned]
        combo_max = max(item.high for item in combo_only)
        combo_min = min(item.low for item in combo_only)
        vol_max = max(item.high for item in vol_only)
        vol_min = min(item.low for item in vol_only)
        if combo_max == combo_min:
            combo_max += Decimal("1")
            combo_min -= Decimal("1")
        if vol_max == vol_min:
            vol_max += Decimal("1")
            vol_min -= Decimal("1")

        candle_step = inner_width / max(len(aligned), 1)
        combo_body_width = max(3.0, min(11.0, candle_step * 0.46))
        vol_body_width = max(2.0, min(7.0, candle_step * 0.24))
        vol_offset = max(combo_body_width * 0.38, 2.5)

        def x_for(index: int) -> float:
            return left + (index * candle_step) + (candle_step / 2)

        def y_combo(price: Decimal) -> float:
            ratio = (combo_max - price) / max(combo_max - combo_min, Decimal("0.00000001"))
            return top + float(ratio) * inner_height

        def y_vol(value: Decimal) -> float:
            ratio = (vol_max - value) / max(vol_max - vol_min, Decimal("0.00000001"))
            return top + float(ratio) * inner_height

        canvas.create_rectangle(left, top, width - right, height - bottom, outline="#d0d7de")

        for index in range(5):
            ratio = index / 4
            y = top + (ratio * inner_height)
            combo_value = combo_max - ((combo_max - combo_min) * Decimal(str(ratio)))
            vol_value = vol_max - ((vol_max - vol_min) * Decimal(str(ratio)))
            canvas.create_line(left, y, width - right, y, fill="#eaeef2", dash=(2, 4))
            canvas.create_text(
                left - 8,
                y,
                text=_format_axis_value(combo_value),
                anchor="e",
                fill="#2563eb",
                font=("Microsoft YaHei UI", 9),
            )
            canvas.create_text(
                width - right + 8,
                y,
                text=f"{_format_axis_value(vol_value)}%",
                anchor="w",
                fill="#ea580c",
                font=("Microsoft YaHei UI", 9),
            )

        for index, (combo_candle, vol_candle) in enumerate(aligned):
            x = x_for(index)

            combo_open_y = y_combo(combo_candle.open)
            combo_close_y = y_combo(combo_candle.close)
            combo_high_y = y_combo(combo_candle.high)
            combo_low_y = y_combo(combo_candle.low)
            combo_color = "#2563eb" if combo_candle.close >= combo_candle.open else "#60a5fa"
            canvas.create_line(x, combo_high_y, x, combo_low_y, fill=combo_color, width=1)
            combo_top = min(combo_open_y, combo_close_y)
            combo_bottom = max(combo_open_y, combo_close_y)
            if abs(combo_bottom - combo_top) < 1:
                combo_bottom = combo_top + 1
            canvas.create_rectangle(
                x - (combo_body_width / 2),
                combo_top,
                x + (combo_body_width / 2),
                combo_bottom,
                outline=combo_color,
                fill=combo_color,
            )

            vol_x = x + vol_offset
            vol_open_y = y_vol(vol_candle.open)
            vol_close_y = y_vol(vol_candle.close)
            vol_high_y = y_vol(vol_candle.high)
            vol_low_y = y_vol(vol_candle.low)
            vol_color = "#ea580c" if vol_candle.close >= vol_candle.open else "#fdba74"
            canvas.create_line(vol_x, vol_high_y, vol_x, vol_low_y, fill=vol_color, width=1)
            vol_top = min(vol_open_y, vol_close_y)
            vol_bottom = max(vol_open_y, vol_close_y)
            if abs(vol_bottom - vol_top) < 1:
                vol_bottom = vol_top + 1
            canvas.create_rectangle(
                vol_x - (vol_body_width / 2),
                vol_top,
                vol_x + (vol_body_width / 2),
                vol_bottom,
                outline=vol_color,
                fill=vol_color,
            )

        for index in _index_markers(len(aligned), target_count=6):
            x = x_for(index)
            canvas.create_line(x, top, x, height - bottom, fill="#f3f4f6", dash=(2, 4))
            is_first = index == 0
            is_last = index == len(aligned) - 1
            canvas.create_text(
                x,
                height - bottom + 8,
                text=_format_chart_ts(aligned[index][0].ts),
                anchor="nw" if is_first else "ne" if is_last else "n",
                fill="#57606a",
                font=("Microsoft YaHei UI", 9),
            )

        latest_combo, latest_vol = aligned[-1]
        canvas.create_text(
            width - right,
            top + 10,
            text=(
                f"叠加对比 | 左轴=组合K线({combo_ccy}) C {_format_compact_number(latest_combo.close)} "
                f"| 右轴=历史波动率(%) C {_format_compact_number(latest_vol.close)}"
            ),
            anchor="ne",
            fill="#57606a",
            font=("Microsoft YaHei UI", 9, "bold"),
        )

    def _on_payoff_canvas_motion(self, event) -> None:
        self._handle_payoff_canvas_motion(self._payoff_canvas, self._payoff_hover_state, event)

    def _on_big_payoff_canvas_motion(self, event) -> None:
        self._handle_payoff_canvas_motion(self._big_payoff_canvas, self._big_payoff_hover_state, event)

    def _handle_payoff_canvas_motion(
        self,
        canvas: Canvas | None,
        state: PayoffChartHoverState | None,
        event,
    ) -> None:
        if canvas is None or state is None or not state.primary_points:
            return
        if not state.bounds.contains(float(event.x), float(event.y)):
            self._clear_chart_hover(canvas)
            return
        index = _nearest_linear_index(float(event.x), state.bounds.left, state.bounds.right, len(state.primary_points))
        point = state.primary_points[index]
        lines = [
            f"标的 {_format_compact_number(point.underlying_price)}",
            f"{state.primary_label} {_format_compact_number(point.pnl)} {state.value_ccy}",
        ]
        marker_positions: list[tuple[float, str]] = [(state.primary_y_positions[index], state.primary_color)]
        tooltip_y = state.primary_y_positions[index]
        if state.reference_points and index < len(state.reference_points) and index < len(state.reference_y_positions):
            reference_point = state.reference_points[index]
            lines.append(f"{state.reference_label} {_format_compact_number(reference_point.pnl)} {state.value_ccy}")
            marker_positions.append((state.reference_y_positions[index], state.reference_color))
            tooltip_y = min(tooltip_y, state.reference_y_positions[index])
        self._draw_chart_hover_overlay(
            canvas,
            bounds=state.bounds,
            x=state.x_positions[index],
            y=tooltip_y,
            marker_color=state.primary_color,
            lines=tuple(lines),
            marker_positions=tuple(marker_positions),
        )

    def _on_combo_canvas_motion(self, event) -> None:
        self._handle_kline_canvas_motion(self._combo_canvas, self._combo_hover_state, event)

    def _on_big_combo_canvas_motion(self, event) -> None:
        self._handle_kline_canvas_motion(self._big_combo_canvas, self._big_combo_hover_state, event)

    def _on_big_volatility_canvas_motion(self, event) -> None:
        self._handle_kline_canvas_motion(self._big_volatility_canvas, self._big_volatility_hover_state, event)

    def _on_big_overlay_combo_canvas_motion(self, event) -> None:
        self._handle_overlay_kline_motion(
            self._big_overlay_combo_canvas,
            self._big_overlay_combo_hover_state,
            self._big_overlay_volatility_canvas,
            self._big_overlay_volatility_hover_state,
            event,
        )

    def _on_big_overlay_volatility_canvas_motion(self, event) -> None:
        self._handle_overlay_kline_motion(
            self._big_overlay_volatility_canvas,
            self._big_overlay_volatility_hover_state,
            self._big_overlay_combo_canvas,
            self._big_overlay_combo_hover_state,
            event,
        )

    def _handle_kline_canvas_motion(self, canvas: Canvas | None, state: ComboChartHoverState | None, event) -> None:
        if canvas is None or state is None or not state.candles:
            return
        if not state.bounds.contains(float(event.x), float(event.y)):
            self._clear_chart_hover(canvas)
            return
        index = _nearest_candle_index(float(event.x), state.bounds.left, state.candle_step, len(state.candles))
        self._draw_kline_hover_for_index(canvas, state, index)

    def _handle_overlay_kline_motion(
        self,
        canvas: Canvas | None,
        state: ComboChartHoverState | None,
        paired_canvas: Canvas | None,
        paired_state: ComboChartHoverState | None,
        event,
    ) -> None:
        if canvas is None or state is None or not state.candles:
            return
        if not state.bounds.contains(float(event.x), float(event.y)):
            self._clear_overlay_chart_hover()
            return
        index = _nearest_candle_index(float(event.x), state.bounds.left, state.candle_step, len(state.candles))
        self._draw_kline_hover_for_index(canvas, state, index)
        if paired_canvas is not None and paired_state is not None and paired_state.candles:
            paired_index = min(index, len(paired_state.candles) - 1)
            self._draw_kline_hover_for_index(paired_canvas, paired_state, paired_index)

    def _draw_kline_hover_for_index(self, canvas: Canvas, state: ComboChartHoverState, index: int) -> None:
        candle = state.candles[index]
        marker_color = "#cf222e" if candle.close >= candle.open else "#1a7f37"
        self._draw_chart_hover_overlay(
            canvas,
            bounds=state.bounds,
            x=state.x_positions[index],
            y=state.close_y_positions[index],
            marker_color=marker_color,
            lines=(
                _format_chart_ts(candle.ts),
                (
                    f"O {_format_compact_number(candle.open)}  H {_format_compact_number(candle.high)}"
                    f"\nL {_format_compact_number(candle.low)}  C {_format_compact_number(candle.close)} {state.value_ccy}"
                ),
            ),
            marker_positions=((state.close_y_positions[index], marker_color),),
        )

    def _clear_overlay_chart_hover(self, _event=None) -> None:
        if self._big_overlay_combo_canvas is not None:
            self._clear_chart_hover(self._big_overlay_combo_canvas)
        if self._big_overlay_volatility_canvas is not None:
            self._clear_chart_hover(self._big_overlay_volatility_canvas)

    def _draw_chart_hover_overlay(
        self,
        canvas: Canvas,
        *,
        bounds: ChartBounds,
        x: float,
        y: float,
        marker_color: str,
        lines: tuple[str, ...],
        marker_positions: tuple[tuple[float, str], ...],
    ) -> None:
        self._clear_chart_hover(canvas)
        canvas.create_line(
            x,
            bounds.top,
            x,
            bounds.bottom,
            fill="#6e7781",
            dash=(4, 4),
            width=1,
            tags="chart-hover",
        )
        canvas.create_line(
            bounds.left,
            y,
            bounds.right,
            y,
            fill="#6e7781",
            dash=(4, 4),
            width=1,
            tags="chart-hover",
        )
        for marker_y, marker_outline in marker_positions:
            canvas.create_oval(
                x - 4,
                marker_y - 4,
                x + 4,
                marker_y + 4,
                fill="#ffffff",
                outline=marker_outline,
                width=2,
                tags="chart-hover",
            )
        self._draw_chart_tooltip(canvas, bounds=bounds, x=x, y=y, marker_color=marker_color, lines=lines)

    def _draw_chart_tooltip(
        self,
        canvas: Canvas,
        *,
        bounds: ChartBounds,
        x: float,
        y: float,
        marker_color: str,
        lines: tuple[str, ...],
    ) -> None:
        place_on_right = x <= ((bounds.left + bounds.right) / 2)
        place_above = y > ((bounds.top + bounds.bottom) / 2)
        anchor = (
            "sw"
            if place_on_right and place_above
            else "se"
            if (not place_on_right and place_above)
            else "nw"
            if place_on_right
            else "ne"
        )
        tooltip_x = x + 12 if place_on_right else x - 12
        tooltip_y = y - 12 if place_above else y + 12
        text_id = canvas.create_text(
            tooltip_x,
            tooltip_y,
            text="\n".join(lines),
            anchor=anchor,
            justify="left",
            fill="#ffffff",
            font=("Microsoft YaHei UI", 9, "bold"),
            tags="chart-hover",
        )
        bbox = canvas.bbox(text_id)
        if bbox is None:
            return
        padding = 6
        dx = 0.0
        dy = 0.0
        if (bbox[0] - padding) < (bounds.left + 6):
            dx = (bounds.left + 6) - (bbox[0] - padding)
        elif (bbox[2] + padding) > (bounds.right - 6):
            dx = (bounds.right - 6) - (bbox[2] + padding)
        if (bbox[1] - padding) < (bounds.top + 6):
            dy = (bounds.top + 6) - (bbox[1] - padding)
        elif (bbox[3] + padding) > (bounds.bottom - 6):
            dy = (bounds.bottom - 6) - (bbox[3] + padding)
        if dx or dy:
            canvas.move(text_id, dx, dy)
            bbox = canvas.bbox(text_id)
            if bbox is None:
                return
        box_id = canvas.create_rectangle(
            bbox[0] - padding,
            bbox[1] - padding,
            bbox[2] + padding,
            bbox[3] + padding,
            fill="#1f2328",
            outline=marker_color,
            width=1,
            tags="chart-hover",
        )
        canvas.tag_lower(box_id, text_id)

    def _clear_chart_hover(self, canvas: Canvas) -> None:
        canvas.delete("chart-hover")

    def _clear_canvas(self, canvas: Canvas, message: str) -> None:
        canvas.delete("all")
        if canvas is self._payoff_canvas:
            self._payoff_hover_state = None
        elif canvas is self._big_payoff_canvas:
            self._big_payoff_hover_state = None
        elif canvas is self._combo_canvas:
            self._combo_hover_state = None
        elif canvas is self._big_combo_canvas:
            self._big_combo_hover_state = None
        elif canvas is self._big_volatility_canvas:
            self._big_volatility_hover_state = None
        elif canvas is self._big_overlay_combo_canvas:
            self._big_overlay_combo_hover_state = None
        elif canvas is self._big_overlay_volatility_canvas:
            self._big_overlay_volatility_hover_state = None
        width = max(canvas.winfo_width(), 900)
        height = max(canvas.winfo_height(), 360)
        canvas.create_rectangle(0, 0, width, height, outline="", fill="#ffffff")
        canvas.create_text(
            width / 2,
            height / 2,
            text=message,
            fill="#6e7781",
            font=("Microsoft YaHei UI", 11),
        )

    def _parse_positive_decimal(self, raw: str, field_name: str) -> Decimal:
        cleaned = raw.strip()
        try:
            value = Decimal(cleaned)
        except InvalidOperation as exc:
            raise ValueError(f"{field_name} 不是有效数字。") from exc
        if value <= 0:
            raise ValueError(f"{field_name} 必须大于 0。")
        return value

    def _parse_positive_int(self, raw: str, field_name: str) -> int:
        cleaned = raw.strip()
        try:
            value = int(cleaned)
        except Exception as exc:
            raise ValueError(f"{field_name} 不是有效整数。") from exc
        if value <= 0:
            raise ValueError(f"{field_name} 必须大于 0。")
        return value

    def _log(self, message: str) -> None:
        if self.logger is not None:
            self.logger(ensure_log_timestamp(message))


def _build_option_quote(instrument: Instrument, ticker: OkxTicker | None) -> OptionQuote:
    return OptionQuote(
        instrument=instrument,
        mark_price=ticker.mark if ticker is not None else None,
        bid_price=ticker.bid if ticker is not None else None,
        ask_price=ticker.ask if ticker is not None else None,
        last_price=ticker.last if ticker is not None else None,
        index_price=ticker.index if ticker is not None else None,
    )


def _spot_usdt_inst_id(inst_family: str | None) -> str | None:
    if not inst_family:
        return None
    base = inst_family.strip().upper().split("-", 1)[0]
    if not base or base == "USDT":
        return None
    return f"{base}-USDT"


def _filter_option_positions(
    positions: list[OkxPosition],
    *,
    family: str,
    expiry_code: str | None = None,
) -> list[OkxPosition]:
    normalized_family = family.strip().upper()
    normalized_expiry = expiry_code.strip() if expiry_code else None
    filtered: list[OkxPosition] = []
    for position in positions:
        try:
            parsed = parse_option_contract(position.inst_id)
        except Exception:
            continue
        if parsed.inst_family != normalized_family:
            continue
        if normalized_expiry and parsed.expiry_code != normalized_expiry:
            continue
        filtered.append(position)
    filtered.sort(
        key=lambda item: (
            parse_option_contract(item.inst_id).expiry_code,
            parse_option_contract(item.inst_id).strike,
            parse_option_contract(item.inst_id).option_type,
            item.inst_id,
        )
    )
    return filtered


def _position_side_and_quantity(position: OkxPosition) -> tuple[str, Decimal]:
    pos_side = position.pos_side.strip().lower()
    if pos_side == "short":
        return "sell", abs(position.position)
    if pos_side == "long":
        return "buy", abs(position.position)
    return ("buy", position.position) if position.position >= 0 else ("sell", abs(position.position))


def _native_display_currency(
    legs: list[StrategyLegDefinition],
    instrument_map: dict[str, Instrument],
) -> str:
    for leg in legs:
        instrument = instrument_map.get(leg.inst_id)
        if instrument is not None and instrument.ct_val_ccy:
            return instrument.ct_val_ccy.upper()
    for instrument in instrument_map.values():
        if instrument.ct_val_ccy:
            return instrument.ct_val_ccy.upper()
    return "结算币"


def _format_price(value: Decimal | None, tick_size: Decimal | None) -> str:
    if value is None:
        return "-"
    if tick_size is None:
        return format_decimal(value)
    places = decimal_places_for_increment(tick_size)
    if places is None:
        return format_decimal(value)
    if places <= 8:
        return format_decimal_by_increment(value, tick_size)
    return format_decimal_fixed(value, min(places, 10))


def _axis_values(min_value: Decimal, max_value: Decimal, *, steps: int) -> list[Decimal]:
    if steps <= 0:
        return [min_value, max_value]
    interval = (max_value - min_value) / Decimal(steps)
    return [min_value + (interval * Decimal(index)) for index in range(steps + 1)]


def _format_axis_value(value: Decimal) -> str:
    magnitude = abs(value)
    if magnitude >= 1000:
        return format_decimal_fixed(value, 2)
    if magnitude >= 1:
        return format_decimal_fixed(value, 4)
    return format_decimal_fixed(value, 6)


def _format_compact_number(value: Decimal | None) -> str:
    if value is None:
        return "-"
    magnitude = abs(value)
    if magnitude >= 1000:
        return format_decimal_fixed(value, 2)
    if magnitude >= 1:
        return format_decimal_fixed(value, 4)
    if magnitude >= Decimal("0.01"):
        return format_decimal_fixed(value, 5)
    return format_decimal_fixed(value, 6)


def _format_signed_percent(value: Decimal) -> str:
    prefix = "+" if value > 0 else ""
    return f"{prefix}{format_decimal_fixed(value, 1)}%"


def _index_markers(length: int, *, target_count: int) -> list[int]:
    if length <= 0:
        return []
    if length <= target_count:
        return list(range(length))
    step = max((length - 1) // max(target_count - 1, 1), 1)
    values = list(range(0, length, step))
    if values[-1] != length - 1:
        values.append(length - 1)
    return values


def _nearest_linear_index(x: float, left: float, right: float, length: int) -> int:
    if length <= 1:
        return 0
    span = max(right - left, 1.0)
    ratio = (x - left) / span
    index = int(round(ratio * (length - 1)))
    return max(0, min(length - 1, index))


def _nearest_candle_index(x: float, left: float, candle_step: float, length: int) -> int:
    if length <= 1:
        return 0
    effective_step = max(candle_step, 1.0)
    index = int(round((x - left - (effective_step / 2)) / effective_step))
    return max(0, min(length - 1, index))


def _format_chart_ts(ts: int) -> str:
    return datetime.fromtimestamp(ts / 1000).strftime("%m-%d %H:%M")


def _annualization_factor_for_bar(bar: str) -> float:
    normalized = bar.strip()
    periods_per_year = {
        "1m": 365 * 24 * 60,
        "3m": (365 * 24 * 60) / 3,
        "5m": (365 * 24 * 60) / 5,
        "15m": (365 * 24 * 60) / 15,
        "1H": 365 * 24,
        "4H": 365 * 6,
    }.get(normalized)
    if periods_per_year is None:
        return 0.0
    return math.sqrt(periods_per_year)


def _build_volatility_candles_from_reference(
    reference_candles: list[Candle],
    *,
    bar: str,
    lookback: int = 20,
) -> list[Candle]:
    confirmed = [item for item in reference_candles if item.confirmed]
    if len(confirmed) < lookback + 1:
        return []

    annualization = _annualization_factor_for_bar(bar)
    if annualization <= 0:
        return []

    volatility_candles: list[Candle] = []
    previous_close_vol: float | None = None
    for index in range(lookback, len(confirmed)):
        closes = [float(item.close) for item in confirmed[index - lookback : index + 1]]
        if any(value <= 0 for value in closes):
            continue
        returns = [math.log(closes[offset] / closes[offset - 1]) for offset in range(1, len(closes))]
        if not returns:
            continue
        mean_return = sum(returns) / len(returns)
        variance = sum((value - mean_return) ** 2 for value in returns) / len(returns)
        close_vol = math.sqrt(max(variance, 0.0)) * annualization * 100.0
        open_vol = previous_close_vol if previous_close_vol is not None else close_vol
        high_vol = max(open_vol, close_vol)
        low_vol = min(open_vol, close_vol)
        candle = confirmed[index]
        volatility_candles.append(
            Candle(
                ts=candle.ts,
                open=Decimal(str(open_vol)),
                high=Decimal(str(high_vol)),
                low=Decimal(str(low_vol)),
                close=Decimal(str(close_vol)),
                volume=Decimal("0"),
                confirmed=candle.confirmed,
            )
        )
        previous_close_vol = close_vol
    return volatility_candles


def _align_overlay_candles(
    combo_candles: list[Candle],
    volatility_candles: list[Candle],
) -> list[tuple[Candle, Candle]]:
    if not combo_candles or not volatility_candles:
        return []
    volatility_by_ts = {item.ts: item for item in volatility_candles}
    aligned: list[tuple[Candle, Candle]] = []
    for combo_candle in combo_candles:
        volatility_candle = volatility_by_ts.get(combo_candle.ts)
        if volatility_candle is not None:
            aligned.append((combo_candle, volatility_candle))
    return aligned


def _normalized_kline_view(
    length: int,
    start_index: int,
    visible_count: int | None,
    *,
    auto_full: bool,
    min_visible: int = 30,
) -> tuple[int, int]:
    if length <= 0:
        return 0, 0
    if auto_full or visible_count is None or visible_count >= length:
        return 0, length
    effective_min = min(length, max(min_visible, 1))
    visible = max(effective_min, min(length, visible_count))
    start = max(0, min(start_index, max(length - visible, 0)))
    return start, visible


def _zoom_kline_view(
    length: int,
    start_index: int,
    visible_count: int,
    *,
    focus_ratio: float,
    zoom_in: bool,
    min_visible: int = 30,
) -> tuple[int, int]:
    if length <= 0:
        return 0, 0
    focus_ratio = min(1.0, max(0.0, focus_ratio))
    current_visible = max(1, min(length, visible_count))
    effective_min = min(length, max(min_visible, 1))
    next_visible = max(effective_min, int(round(current_visible * (0.8 if zoom_in else 1.25))))
    next_visible = max(effective_min, min(length, next_visible))
    focus_index = start_index + int(round((current_visible - 1) * focus_ratio))
    next_start = focus_index - int(round((next_visible - 1) * focus_ratio))
    next_start = max(0, min(next_start, max(length - next_visible, 0)))
    return next_start, next_visible


def _pan_kline_view(length: int, start_index: int, visible_count: int, *, delta_items: int) -> tuple[int, int]:
    if length <= 0:
        return 0, 0
    visible = max(1, min(length, visible_count))
    next_start = start_index + delta_items
    next_start = max(0, min(next_start, max(length - visible, 0)))
    return next_start, visible
