from __future__ import annotations

import time
from decimal import Decimal, InvalidOperation
from tkinter import BooleanVar, END, StringVar, Text, Toplevel
from tkinter import messagebox, ttk
from typing import Callable

from okx_quant.models import Instrument
from okx_quant.persistence import (
    load_smart_order_favorites_snapshot,
    save_smart_order_favorites_snapshot,
)
from okx_quant.pricing import format_decimal, format_decimal_by_increment, snap_to_increment
from okx_quant.smart_order import (
    CycleMode,
    ExecutionMode,
    SmartOrderManager,
    SmartOrderRuntimeConfig,
    TriggerDirection,
    resolve_best_quote_price,
)
from okx_quant.window_layout import apply_adaptive_window_geometry


RuntimeConfigProvider = Callable[[], SmartOrderRuntimeConfig | None]
Logger = Callable[[str], None]
REFRESH_INTERVAL_MS = 1200
POSITION_LIMIT_REFRESH_SECONDS = 4.0
LADDER_INTERACTION_PAUSE_SECONDS = 1.5

INSTRUMENT_TYPE_OPTIONS = {
    "现货 SPOT": "SPOT",
    "永续 SWAP": "SWAP",
    "期权 OPTION": "OPTION",
}
MANUAL_ORDER_TYPE_OPTIONS = {
    "限价": "limit",
    "最优价": "best_quote",
    "市价(激进IOC)": "aggressive_ioc",
    "IOC": "ioc",
    "FOK": "fok",
    "Post Only": "post_only",
}
TRIGGER_SOURCE_OPTIONS = {
    "当前合约最新价": ("current", "last"),
    "当前合约标记价": ("current", "mark"),
    "当前合约指数价": ("current", "index"),
    "自定义标的最新价": ("custom", "last"),
    "自定义标的标记价": ("custom", "mark"),
    "自定义标的指数价": ("custom", "index"),
}


class SmartOrderWindow:
    def __init__(
        self,
        parent,
        client,
        runtime_config_provider: RuntimeConfigProvider,
        logger: Logger,
    ) -> None:
        self.client = client
        self._runtime_config_provider = runtime_config_provider
        self._logger = logger
        self.manager = SmartOrderManager(client, logger=logger)
        self._refresh_job: str | None = None
        self._destroying = False
        self._instrument: Instrument | None = None
        self._last_log_signature: tuple[str, ...] = ()
        self._last_task_signature: tuple[tuple[object, ...], ...] = ()
        self._last_ladder_signature: tuple[tuple[object, ...], ...] = ()
        self._last_instrument_status: str = ""
        self._last_position_limit_status: str = ""
        self._next_position_limit_refresh_at: float = 0.0
        self._ladder_refresh_paused_until: float = 0.0
        self._ladder_row_ids: tuple[str, ...] = ()
        self._favorites = list(load_smart_order_favorites_snapshot().get("favorites", []))
        self.favorite_selection = StringVar()
        self._favorite_combo: ttk.Combobox | None = None

        self.window = Toplevel(parent)
        self.window.title("无限下单")
        apply_adaptive_window_geometry(
            self.window,
            width_ratio=0.84,
            height_ratio=0.86,
            min_width=1380,
            min_height=920,
            max_width=1880,
            max_height=1260,
        )
        self.window.protocol("WM_DELETE_WINDOW", self._on_close)

        self.instrument_type_label = StringVar(value="期权 OPTION")
        self.instrument_id = StringVar()
        self.instrument_status = StringVar(value="请选择并加载一个合约。")
        self.window_lock_text = StringVar(value="未锁定")

        self.manual_side = StringVar(value="buy")
        self.manual_order_type_label = StringVar(value="限价")
        self.manual_price = StringVar()
        self.manual_size = StringVar(value="0.01")

        self.grid_enabled = BooleanVar(value=False)
        self.grid_order_size = StringVar(value="0.01")
        self.grid_long_step = StringVar(value="0.005")
        self.grid_short_step = StringVar(value="0.005")
        self.grid_cycle_label = StringVar(value="连续")
        self.ladder_price_filter = StringVar(value="自动")
        self._ladder_filter_values: list[str] = ["自动"]

        limit_enabled, long_limit, short_limit = self.manager.get_position_limit_config()
        self.position_limit_enabled = BooleanVar(value=limit_enabled)
        self.position_long_limit = StringVar(value="" if long_limit is None else format_decimal(long_limit))
        self.position_short_limit = StringVar(value="" if short_limit is None else format_decimal(short_limit))
        self.position_limit_status = StringVar(value="未启用总仓位限制。")
        self.position_long_limit_label_text = StringVar(value="多头总仓位上限")
        self.position_short_limit_label_text = StringVar(value="空头总仓位上限")
        self.position_limit_hint_text = StringVar(value="当前按交易所原始下单单位限制。")

        self.condition_side = StringVar(value="buy")
        self.condition_trigger_source_label = StringVar(value="当前合约最新价")
        self.condition_custom_inst = StringVar()
        self.condition_trigger_direction_label = StringVar(value="上穿触发")
        self.condition_trigger_price = StringVar()
        self.condition_exec_mode_label = StringVar(value="限价")
        self.condition_exec_price = StringVar()
        self.condition_size = StringVar(value="0.01")
        self.condition_take_profit = StringVar()
        self.condition_stop_loss = StringVar()

        self.tp_sl_position_side_label = StringVar(value="多仓")
        self.tp_sl_trigger_source_label = StringVar(value="当前合约最新价")
        self.tp_sl_custom_inst = StringVar()
        self.tp_sl_size = StringVar(value="0.01")
        self.tp_sl_take_profit = StringVar()
        self.tp_sl_stop_loss = StringVar()

        self.manual_size_label_text = StringVar(value="数量（币）")
        self.condition_size_label_text = StringVar(value="数量（币）")
        self.tp_sl_size_label_text = StringVar(value="保护数量（币）")
        self.grid_size_label_text = StringVar(value="下单数量（币）")
        self.quantity_hint_text = StringVar(value="期权数量按币数输入，系统会自动换算成张数。")

        self._ladder_trees: dict[str, ttk.Treeview] = {}
        self._ladder_scroll: ttk.Scrollbar | None = None
        self._task_tree: ttk.Treeview | None = None
        self._log_text: Text | None = None
        self._ladder_filter_combo: ttk.Combobox | None = None

        self._build_layout()
        self._bootstrap_locked_contract()
        self._refresh_quantity_labels()
        self._sync_position_limit_fields_from_manager()
        self._refresh_favorite_options()
        self._schedule_refresh()

    def show(self) -> None:
        if not self.window.winfo_exists():
            return
        self.window.deiconify()
        self.window.lift()
        self.window.focus_force()
        self._refresh_views(force_position_limit=True)

    def _bootstrap_locked_contract(self) -> None:
        instrument = self.manager.locked_instrument
        if instrument is None:
            return
        self._instrument = instrument
        self.instrument_id.set(instrument.inst_id)
        self.instrument_type_label.set(self._instrument_type_label(instrument.inst_type))
        self._refresh_ladder_filter_options(instrument)
        self._refresh_quantity_labels()
        self._sync_position_limit_fields_from_manager()
        self._refresh_favorite_options(preferred_inst_id=instrument.inst_id)
        try:
            self.manager.ensure_market_snapshot(instrument, force=True)
        except Exception:
            pass

    def _instrument_type_label(self, inst_type: str) -> str:
        normalized = inst_type.strip().upper()
        for label, value in INSTRUMENT_TYPE_OPTIONS.items():
            if value == normalized:
                return label
        return next(iter(INSTRUMENT_TYPE_OPTIONS))

    def _selected_inst_type(self) -> str:
        if self._instrument is not None:
            return self._instrument.inst_type
        return INSTRUMENT_TYPE_OPTIONS.get(self.instrument_type_label.get(), "OPTION")

    def _uses_coin_quantity(self) -> bool:
        return self._selected_inst_type() == "OPTION"

    def _option_contract_coin_size(self, instrument: Instrument) -> tuple[Decimal | None, str | None]:
        if instrument.inst_type != "OPTION":
            return None, None
        if instrument.ct_val is None or instrument.ct_val <= 0:
            return None, None
        multiplier = instrument.ct_mult if instrument.ct_mult is not None and instrument.ct_mult > 0 else Decimal("1")
        return instrument.ct_val * multiplier, instrument.ct_val_ccy

    def _refresh_quantity_labels(self) -> None:
        if self._uses_coin_quantity():
            self.manual_size_label_text.set("数量（币）")
            self.condition_size_label_text.set("数量（币）")
            self.tp_sl_size_label_text.set("保护数量（币）")
            self.grid_size_label_text.set("下单数量（币）")
            self.position_long_limit_label_text.set("多头总仓位上限（币）")
            self.position_short_limit_label_text.set("空头总仓位上限（币）")
            if self._instrument is not None:
                contract_size, contract_ccy = self._option_contract_coin_size(self._instrument)
                if contract_size is not None and contract_size > 0:
                    self.quantity_hint_text.set(
                        f"期权数量按币数输入，系统会自动换算成张数。当前约每张 {format_decimal(contract_size)} {contract_ccy or '币'}。"
                    )
                    self.position_limit_hint_text.set(
                        f"仓位限制也按币数输入，系统会自动换算成张数。当前约每张 {format_decimal(contract_size)} {contract_ccy or '币'}。"
                    )
                else:
                    self.quantity_hint_text.set("期权数量按币数输入，系统会自动换算成张数。")
                    self.position_limit_hint_text.set("仓位限制按币数输入，系统会自动换算成张数。")
            else:
                self.quantity_hint_text.set("期权数量按币数输入，系统会自动换算成张数。")
                self.position_limit_hint_text.set("仓位限制按币数输入，系统会自动换算成张数。")
        else:
            self.manual_size_label_text.set("数量")
            self.condition_size_label_text.set("数量")
            self.tp_sl_size_label_text.set("保护数量")
            self.grid_size_label_text.set("下单数量")
            self.position_long_limit_label_text.set("多头总仓位上限")
            self.position_short_limit_label_text.set("空头总仓位上限")
            self.quantity_hint_text.set("当前类型按交易所原始下单单位输入。")
            self.position_limit_hint_text.set("当前按交易所原始下单单位限制。")

    def _on_instrument_type_changed(self, _event=None) -> None:
        if self._instrument is None:
            self._refresh_quantity_labels()
            self._sync_position_limit_fields_from_manager()
        self._refresh_favorite_options()

    def _current_type_favorites(self) -> list[dict[str, str]]:
        selected_type = INSTRUMENT_TYPE_OPTIONS.get(self.instrument_type_label.get(), "OPTION")
        return [item for item in self._favorites if item.get("inst_type") == selected_type]

    def _refresh_favorite_options(self, preferred_inst_id: str | None = None) -> None:
        values = [item["inst_id"] for item in self._current_type_favorites()]
        if self._favorite_combo is not None:
            self._favorite_combo.configure(values=values)
        if preferred_inst_id and preferred_inst_id in values:
            self.favorite_selection.set(preferred_inst_id)
        elif self.favorite_selection.get() not in values:
            self.favorite_selection.set(values[0] if values else "")

    def _save_favorites(self) -> None:
        save_smart_order_favorites_snapshot(self._favorites)

    def _favorite_entry_by_inst_id(self, inst_id: str) -> dict[str, str] | None:
        normalized = inst_id.strip().upper()
        for item in self._favorites:
            if item.get("inst_id") == normalized:
                return item
        return None

    def _on_favorite_selected(self, _event=None) -> None:
        inst_id = self.favorite_selection.get().strip().upper()
        if not inst_id:
            return
        entry = self._favorite_entry_by_inst_id(inst_id)
        if entry is None:
            return
        self.instrument_type_label.set(self._instrument_type_label(entry["inst_type"]))
        self.instrument_id.set(entry["inst_id"])
        self._refresh_quantity_labels()
        self._sync_position_limit_fields_from_manager()
        self._refresh_favorite_options(preferred_inst_id=entry["inst_id"])

    def add_favorite_instrument(self) -> None:
        inst_id = self.instrument_id.get().strip().upper()
        if not inst_id:
            messagebox.showerror("收藏失败", "请先填写或加载一个合约。", parent=self.window)
            return
        expected_type = INSTRUMENT_TYPE_OPTIONS[self.instrument_type_label.get()]
        try:
            instrument = self.client.get_instrument(inst_id)
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("收藏失败", str(exc), parent=self.window)
            return
        if instrument.inst_type != expected_type:
            messagebox.showerror(
                "收藏失败",
                f"{instrument.inst_id} 实际是 {instrument.inst_type}，和当前类型不一致。",
                parent=self.window,
            )
            return
        entry = {"inst_id": instrument.inst_id, "inst_type": instrument.inst_type}
        key = (entry["inst_type"], entry["inst_id"])
        if any((item["inst_type"], item["inst_id"]) == key for item in self._favorites):
            self._refresh_favorite_options(preferred_inst_id=entry["inst_id"])
            messagebox.showinfo("提示", f"{instrument.inst_id} 已在自选合约里。", parent=self.window)
            return
        self._favorites.append(entry)
        self._save_favorites()
        self._refresh_favorite_options(preferred_inst_id=entry["inst_id"])
        messagebox.showinfo("提示", f"已添加自选合约：{instrument.inst_id}", parent=self.window)

    def remove_favorite_instrument(self) -> None:
        inst_id = self.favorite_selection.get().strip().upper() or self.instrument_id.get().strip().upper()
        if not inst_id:
            messagebox.showerror("删除失败", "请先选择一个自选合约。", parent=self.window)
            return
        selected_type = INSTRUMENT_TYPE_OPTIONS.get(self.instrument_type_label.get(), "OPTION")
        before = len(self._favorites)
        self._favorites = [
            item
            for item in self._favorites
            if not (item.get("inst_type") == selected_type and item.get("inst_id") == inst_id)
        ]
        if len(self._favorites) == before:
            messagebox.showinfo("提示", "当前类型下没有这个自选合约。", parent=self.window)
            return
        self._save_favorites()
        self._refresh_favorite_options()
        messagebox.showinfo("提示", f"已删除自选合约：{inst_id}", parent=self.window)

    def _convert_input_size_to_order_size(self, raw_value: str, field_name: str, instrument: Instrument) -> Decimal:
        size = self._parse_positive_decimal(raw_value, field_name)
        if instrument.inst_type != "OPTION":
            return size
        contract_size, contract_ccy = self._option_contract_coin_size(instrument)
        if contract_size is None or contract_size <= 0:
            return size
        order_size = snap_to_increment(size / contract_size, instrument.lot_size, "down")
        if order_size < instrument.min_size:
            raise RuntimeError(
                f"{field_name} {format_decimal(size)}（币）换算后为 {format_decimal(order_size)} 张，小于最小下单量 "
                f"{format_decimal(instrument.min_size)} 张。当前每张约 {format_decimal(contract_size)} {contract_ccy or '币'}。"
            )
        return order_size

    def _convert_internal_size_to_display_size(self, size: Decimal | None) -> str:
        if size is None:
            return ""
        if self._instrument is None or self._instrument.inst_type != "OPTION":
            return format_decimal(size)
        contract_size, _ = self._option_contract_coin_size(self._instrument)
        if contract_size is None or contract_size <= 0:
            return format_decimal(size)
        return format_decimal(size * contract_size)

    def _convert_display_limit_to_internal(self, raw_value: str, field_name: str) -> Decimal | None:
        value = self._parse_optional_positive_decimal(raw_value, field_name)
        if value is None:
            return None
        if self._instrument is None or self._instrument.inst_type != "OPTION":
            return value
        contract_size, contract_ccy = self._option_contract_coin_size(self._instrument)
        if contract_size is None or contract_size <= 0:
            raise RuntimeError("请先加载期权合约后再设置币数口径的仓位限制。")
        internal = snap_to_increment(value / contract_size, self._instrument.lot_size, "down")
        if internal < self._instrument.min_size:
            raise RuntimeError(
                f"{field_name} {format_decimal(value)}（币）换算后为 {format_decimal(internal)} 张，小于最小下单量 "
                f"{format_decimal(self._instrument.min_size)} 张。当前每张约 {format_decimal(contract_size)} {contract_ccy or '币'}。"
            )
        return internal

    def _sync_position_limit_fields_from_manager(self) -> None:
        enabled, long_limit, short_limit = self.manager.get_position_limit_config()
        self.position_limit_enabled.set(enabled)
        self.position_long_limit.set(self._convert_internal_size_to_display_size(long_limit))
        self.position_short_limit.set(self._convert_internal_size_to_display_size(short_limit))

    def destroy(self) -> None:
        self._destroying = True
        if self._refresh_job is not None:
            try:
                self.window.after_cancel(self._refresh_job)
            except Exception:
                pass
            self._refresh_job = None
        if self.manager.has_active_or_pending_tasks():
            try:
                runtime = self._runtime_config_provider()
                self.manager.close_all_and_unlock(runtime)
            except Exception as exc:  # noqa: BLE001
                self._logger(f"[无限下单] 程序关闭时未能完全清理任务：{exc}")
        self.manager.destroy()
        if self.window.winfo_exists():
            self.window.destroy()

    def _build_layout(self) -> None:
        self.window.columnconfigure(0, weight=1)
        self.window.rowconfigure(1, weight=1)

        header = ttk.Frame(self.window, padding=(16, 16, 16, 10))
        header.grid(row=0, column=0, sticky="ew")
        header.columnconfigure(1, weight=1)
        ttk.Label(header, text="无限下单", font=("Microsoft YaHei UI", 18, "bold")).grid(row=0, column=0, sticky="w")
        ttk.Label(header, textvariable=self.window_lock_text).grid(row=0, column=1, sticky="e")
        ttk.Label(header, textvariable=self.instrument_status).grid(row=1, column=0, columnspan=2, sticky="w", pady=(8, 0))

        body = ttk.Panedwindow(self.window, orient="horizontal")
        body.grid(row=1, column=0, sticky="nsew", padx=16, pady=(0, 12))

        left = ttk.Frame(body, padding=12)
        right = ttk.Frame(body, padding=12)
        body.add(left, weight=2)
        body.add(right, weight=3)

        left.columnconfigure(0, weight=1)
        left.rowconfigure(2, weight=1)
        right.columnconfigure(0, weight=1)
        right.rowconfigure(1, weight=1)
        right.rowconfigure(2, weight=1)

        instrument_frame = ttk.LabelFrame(left, text="合约工作台", padding=12)
        instrument_frame.grid(row=0, column=0, sticky="ew")
        instrument_frame.columnconfigure(1, weight=1)
        instrument_frame.columnconfigure(3, weight=1)
        instrument_frame.columnconfigure(6, weight=1)

        ttk.Label(instrument_frame, text="类型").grid(row=0, column=0, sticky="w")
        instrument_type_combo = ttk.Combobox(
            instrument_frame,
            textvariable=self.instrument_type_label,
            values=list(INSTRUMENT_TYPE_OPTIONS.keys()),
            state="readonly",
        )
        instrument_type_combo.grid(row=0, column=1, sticky="ew", padx=(0, 12))
        instrument_type_combo.bind("<<ComboboxSelected>>", self._on_instrument_type_changed)
        ttk.Label(instrument_frame, text="合约").grid(row=0, column=2, sticky="w")
        ttk.Entry(instrument_frame, textvariable=self.instrument_id).grid(row=0, column=3, sticky="ew", padx=(0, 12))
        ttk.Button(instrument_frame, text="加载合约", command=self.load_instrument).grid(row=0, column=4, sticky="e")
        ttk.Button(instrument_frame, text="加入自选", command=self.add_favorite_instrument).grid(row=0, column=5, sticky="e", padx=(8, 8))
        ttk.Label(instrument_frame, text="自选").grid(row=1, column=0, sticky="w", pady=(10, 0))
        favorite_combo = ttk.Combobox(
            instrument_frame,
            textvariable=self.favorite_selection,
            state="readonly",
        )
        favorite_combo.grid(row=1, column=1, columnspan=3, sticky="ew", pady=(10, 0), padx=(0, 12))
        favorite_combo.bind("<<ComboboxSelected>>", self._on_favorite_selected)
        self._favorite_combo = favorite_combo
        ttk.Button(instrument_frame, text="带入自选", command=self._on_favorite_selected).grid(row=1, column=4, sticky="e", pady=(10, 0))
        ttk.Button(instrument_frame, text="删除自选", command=self.remove_favorite_instrument).grid(row=1, column=5, sticky="e", padx=(8, 8), pady=(10, 0))

        notebook = ttk.Notebook(left)
        notebook.grid(row=1, column=0, sticky="nsew", pady=(12, 0))

        manual_tab = ttk.Frame(notebook, padding=12)
        condition_tab = ttk.Frame(notebook, padding=12)
        tp_sl_tab = ttk.Frame(notebook, padding=12)
        grid_tab = ttk.Frame(notebook, padding=12)
        position_limit_tab = ttk.Frame(notebook, padding=12)
        notebook.add(manual_tab, text="手工下单")
        notebook.add(condition_tab, text="条件单")
        notebook.add(tp_sl_tab, text="止盈止损")
        notebook.add(grid_tab, text="网格策略")

        self._build_manual_tab(manual_tab)
        self._build_condition_tab(condition_tab)
        self._build_tp_sl_tab(tp_sl_tab)
        self._build_grid_tab(grid_tab)
        notebook.add(position_limit_tab, text="仓位限制")
        self._build_position_limit_tab(position_limit_tab)

        ladder_frame = ttk.LabelFrame(right, text="规则盘口", padding=12)
        ladder_frame.grid(row=0, column=0, sticky="nsew")
        ladder_frame.columnconfigure(0, weight=1)
        ladder_frame.rowconfigure(1, weight=1)
        ladder_toolbar = ttk.Frame(ladder_frame)
        ladder_toolbar.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        ladder_toolbar.columnconfigure(3, weight=1)
        ttk.Checkbutton(
            ladder_toolbar,
            text="启用网格策略点击模式",
            variable=self.grid_enabled,
        ).grid(row=0, column=0, sticky="w")
        ttk.Label(ladder_toolbar, text="价格筛选").grid(row=0, column=1, sticky="e", padx=(12, 6))
        ladder_filter_combo = ttk.Combobox(
            ladder_toolbar,
            textvariable=self.ladder_price_filter,
            values=self._ladder_filter_values,
            state="readonly",
            width=10,
        )
        ladder_filter_combo.grid(row=0, column=2, sticky="w")
        ladder_filter_combo.bind("<<ComboboxSelected>>", lambda _event: self._on_ladder_filter_changed())
        self._ladder_filter_combo = ladder_filter_combo
        ttk.Label(
            ladder_toolbar,
            text="关闭时，点击规则盘口只会带入价格；打开后，点击买/卖列会直接创建网格任务。",
        ).grid(row=0, column=3, sticky="e", padx=(12, 0))

        ladder_style = ttk.Style(self.window)
        ladder_style.configure(
            "SmartOrder.Treeview",
            rowheight=28,
            borderwidth=1,
            relief="solid",
            background="#ffffff",
            fieldbackground="#ffffff",
            foreground="#1f2937",
            font=("Consolas", 10),
            bordercolor="#c9d2dc",
            lightcolor="#c9d2dc",
            darkcolor="#c9d2dc",
        )
        ladder_style.configure(
            "SmartOrder.Treeview.Heading",
            background="#eef2f7",
            foreground="#111827",
            font=("Microsoft YaHei UI", 10, "bold"),
            borderwidth=1,
            relief="solid",
        )
        ladder_style.map(
            "SmartOrder.Treeview",
            background=[("selected", "#1e7ed0")],
            foreground=[("selected", "#ffffff")],
        )

        ladder_tree = ttk.Treeview(
            ladder_frame,
            columns=("buy", "price", "sell", "working"),
            show="headings",
            selectmode="browse",
            height=24,
            style="SmartOrder.Treeview",
        )
        ladder_tree.heading("buy", text="买入")
        ladder_tree.heading("price", text="价格")
        ladder_tree.heading("sell", text="卖出")
        ladder_tree.heading("working", text="委托映射")
        ladder_tree.column("buy", width=120, anchor="e")
        ladder_tree.column("price", width=120, anchor="center")
        ladder_tree.column("sell", width=120, anchor="w")
        ladder_tree.column("working", width=260, anchor="w")
        ladder_tree.tag_configure("ladder_last_price", background="#fff4bf", foreground="#7c5200")
        ladder_tree.tag_configure("ladder_best_bid", background="#dff6e6", foreground="#0f5132")
        ladder_tree.tag_configure("ladder_best_ask", background="#fde2e1", foreground="#9f1239")
        ladder_tree.tag_configure("ladder_working", background="#e0ecff", foreground="#1d4ed8")
        ladder_tree.tag_configure("ladder_working_bid", background="#d2f4dc", foreground="#166534")
        ladder_tree.tag_configure("ladder_working_ask", background="#ffd9d6", foreground="#b42318")
        ladder_tree.tag_configure("ladder_even", background="#ffffff")
        ladder_tree.tag_configure("ladder_odd", background="#f1f5f9")
        ladder_tree.grid(row=1, column=0, sticky="nsew")
        ladder_scroll = ttk.Scrollbar(ladder_frame, orient="vertical", command=ladder_tree.yview)
        ladder_scroll.grid(row=1, column=1, sticky="ns")
        ladder_tree.configure(yscrollcommand=ladder_scroll.set)
        ladder_tree.bind("<ButtonRelease-1>", self._on_ladder_click)
        self._ladder_tree = ladder_tree
        ladder_tree.grid_remove()
        ladder_scroll.grid_remove()


        ladder_grid = ttk.Frame(ladder_frame)
        ladder_grid.grid(row=1, column=0, sticky="nsew")
        ladder_grid.columnconfigure(0, weight=2)
        ladder_grid.columnconfigure(1, weight=2)
        ladder_grid.columnconfigure(2, weight=2)
        ladder_grid.columnconfigure(3, weight=3)
        ladder_grid.rowconfigure(0, weight=1)

        for column_index, (key, title, width, anchor) in enumerate(
            (
                ("buy", "\u4e70\u5165", 120, "e"),
                ("price", "\u4ef7\u683c", 120, "center"),
                ("sell", "\u5356\u51fa", 120, "w"),
                ("working", "\u59d4\u6258\u6620\u5c04", 260, "w"),
            )
        ):
            tree = ttk.Treeview(
                ladder_grid,
                columns=(key,),
                show="headings",
                selectmode="browse",
                height=24,
                style="SmartOrder.Treeview",
            )
            tree.heading(key, text=title)
            tree.column(key, width=width, anchor=anchor)
            tree.grid(row=0, column=column_index, sticky="nsew")
            tree.bind("<ButtonRelease-1>", self._on_ladder_click)
            tree.bind("<MouseWheel>", self._on_ladder_mousewheel)
            tree.tag_configure("buy_even", background="#edf9f0", foreground="#166534")
            tree.tag_configure("buy_odd", background="#e1f4e7", foreground="#166534")
            tree.tag_configure("buy_best", background="#ccefd7", foreground="#14532d")
            tree.tag_configure("buy_working", background="#bde7ca", foreground="#14532d")
            tree.tag_configure("price_even", background="#ffffff", foreground="#1f2937")
            tree.tag_configure("price_odd", background="#f1f5f9", foreground="#1f2937")
            tree.tag_configure("price_last", background="#fff4bf", foreground="#7c5200")
            tree.tag_configure("sell_even", background="#fff0f0", foreground="#b42318")
            tree.tag_configure("sell_odd", background="#ffe3e1", foreground="#b42318")
            tree.tag_configure("sell_best", background="#ffd2cf", foreground="#9f1239")
            tree.tag_configure("sell_working", background="#ffc5c0", foreground="#9f1239")
            tree.tag_configure("work_even", background="#ffffff", foreground="#1f2937")
            tree.tag_configure("work_odd", background="#f1f5f9", foreground="#1f2937")
            tree.tag_configure("work_last", background="#fff4bf", foreground="#7c5200")
            tree.tag_configure("work_bid", background="#dff6e6", foreground="#0f5132")
            tree.tag_configure("work_ask", background="#fde2e1", foreground="#9f1239")
            tree.tag_configure("work_active", background="#e0ecff", foreground="#1d4ed8")
            tree.tag_configure("work_active_bid", background="#d2f4dc", foreground="#166534")
            tree.tag_configure("work_active_ask", background="#ffd9d6", foreground="#b42318")
            self._ladder_trees[key] = tree

        self._ladder_scroll = ttk.Scrollbar(ladder_frame, orient="vertical", command=self._on_ladder_scroll)
        self._ladder_scroll.grid(row=1, column=1, sticky="ns")
        for tree in self._ladder_trees.values():
            tree.configure(yscrollcommand=self._on_ladder_yview)

        task_frame = ttk.LabelFrame(right, text="活动任务与日志", padding=12)
        task_frame.grid(row=1, column=0, sticky="nsew", pady=(12, 0))
        task_frame.columnconfigure(0, weight=1)
        task_frame.rowconfigure(1, weight=1)
        task_frame.rowconfigure(3, weight=1)

        task_toolbar = ttk.Frame(task_frame)
        task_toolbar.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        task_toolbar.columnconfigure(0, weight=1)
        ttk.Button(task_toolbar, text="重新启动选中任务", command=self.restart_selected_task).grid(row=0, column=1, padx=(0, 8))
        ttk.Button(task_toolbar, text="停止选中任务", command=self.stop_selected_task).grid(row=0, column=2, padx=(0, 8))
        ttk.Button(task_toolbar, text="删除选中任务", command=self.remove_selected_task).grid(row=0, column=3, padx=(0, 8))
        ttk.Button(task_toolbar, text="停止全部任务", command=self.stop_all_tasks).grid(row=0, column=4)

        task_tree = ttk.Treeview(
            task_frame,
            columns=("id", "type", "side", "status", "price", "size", "cycle", "message"),
            show="headings",
            selectmode="browse",
            height=8,
        )
        for key, title, width in (
            ("id", "编号", 70),
            ("type", "类型", 90),
            ("side", "方向", 70),
            ("status", "状态", 100),
            ("price", "委托价", 110),
            ("size", "数量", 90),
            ("cycle", "循环", 70),
            ("message", "最新状态", 380),
        ):
            task_tree.heading(key, text=title)
            task_tree.column(key, width=width, anchor="center" if key != "message" else "w")
        task_tree.grid(row=1, column=0, sticky="nsew")
        task_scroll = ttk.Scrollbar(task_frame, orient="vertical", command=task_tree.yview)
        task_scroll.grid(row=1, column=1, sticky="ns")
        task_tree.configure(yscrollcommand=task_scroll.set)
        self._task_tree = task_tree

        ttk.Label(task_frame, text="日志").grid(row=2, column=0, sticky="w", pady=(12, 6))
        self._log_text = Text(task_frame, height=10, wrap="word", font=("Microsoft YaHei UI", 10))
        self._log_text.grid(row=3, column=0, sticky="nsew")
        log_scroll = ttk.Scrollbar(task_frame, orient="vertical", command=self._log_text.yview)
        log_scroll.grid(row=3, column=1, sticky="ns")
        self._log_text.configure(yscrollcommand=log_scroll.set)

    def _build_manual_tab(self, tab: ttk.Frame) -> None:
        tab.columnconfigure(1, weight=1)
        ttk.Label(tab, text="方向").grid(row=0, column=0, sticky="w")
        ttk.Combobox(tab, textvariable=self.manual_side, values=["buy", "sell"], state="readonly").grid(row=0, column=1, sticky="ew")
        ttk.Label(tab, text="订单类型").grid(row=1, column=0, sticky="w", pady=(10, 0))
        ttk.Combobox(tab, textvariable=self.manual_order_type_label, values=list(MANUAL_ORDER_TYPE_OPTIONS.keys()), state="readonly").grid(row=1, column=1, sticky="ew", pady=(10, 0))
        ttk.Label(tab, text="价格").grid(row=2, column=0, sticky="w", pady=(10, 0))
        ttk.Entry(tab, textvariable=self.manual_price).grid(row=2, column=1, sticky="ew", pady=(10, 0))
        ttk.Label(tab, textvariable=self.manual_size_label_text).grid(row=3, column=0, sticky="w", pady=(10, 0))
        ttk.Entry(tab, textvariable=self.manual_size).grid(row=3, column=1, sticky="ew", pady=(10, 0))
        ttk.Label(tab, textvariable=self.quantity_hint_text, wraplength=320, justify="left").grid(
            row=4, column=0, columnspan=2, sticky="w", pady=(10, 0)
        )
        ttk.Button(tab, text="提交手工委托", command=self.submit_manual_order).grid(row=5, column=0, columnspan=2, sticky="ew", pady=(14, 0))

    def _build_condition_tab(self, tab: ttk.Frame) -> None:
        tab.columnconfigure(1, weight=1)
        ttk.Label(tab, text="方向").grid(row=0, column=0, sticky="w")
        ttk.Combobox(tab, textvariable=self.condition_side, values=["buy", "sell"], state="readonly").grid(row=0, column=1, sticky="ew")
        ttk.Label(tab, text="触发源").grid(row=1, column=0, sticky="w", pady=(10, 0))
        ttk.Combobox(tab, textvariable=self.condition_trigger_source_label, values=list(TRIGGER_SOURCE_OPTIONS.keys()), state="readonly").grid(row=1, column=1, sticky="ew", pady=(10, 0))
        ttk.Label(tab, text="自定义标的").grid(row=2, column=0, sticky="w", pady=(10, 0))
        ttk.Entry(tab, textvariable=self.condition_custom_inst).grid(row=2, column=1, sticky="ew", pady=(10, 0))
        ttk.Label(tab, text="触发方向").grid(row=3, column=0, sticky="w", pady=(10, 0))
        ttk.Combobox(tab, textvariable=self.condition_trigger_direction_label, values=["上穿触发", "下穿触发"], state="readonly").grid(row=3, column=1, sticky="ew", pady=(10, 0))
        ttk.Label(tab, text="触发价格").grid(row=4, column=0, sticky="w", pady=(10, 0))
        ttk.Entry(tab, textvariable=self.condition_trigger_price).grid(row=4, column=1, sticky="ew", pady=(10, 0))
        ttk.Label(tab, text="执行方式").grid(row=5, column=0, sticky="w", pady=(10, 0))
        ttk.Combobox(tab, textvariable=self.condition_exec_mode_label, values=["限价", "激进IOC"], state="readonly").grid(row=5, column=1, sticky="ew", pady=(10, 0))
        ttk.Label(tab, text="执行价格").grid(row=6, column=0, sticky="w", pady=(10, 0))
        ttk.Entry(tab, textvariable=self.condition_exec_price).grid(row=6, column=1, sticky="ew", pady=(10, 0))
        ttk.Label(tab, textvariable=self.condition_size_label_text).grid(row=7, column=0, sticky="w", pady=(10, 0))
        ttk.Entry(tab, textvariable=self.condition_size).grid(row=7, column=1, sticky="ew", pady=(10, 0))
        ttk.Label(tab, text="止盈触发").grid(row=8, column=0, sticky="w", pady=(10, 0))
        ttk.Entry(tab, textvariable=self.condition_take_profit).grid(row=8, column=1, sticky="ew", pady=(10, 0))
        ttk.Label(tab, text="止损触发").grid(row=9, column=0, sticky="w", pady=(10, 0))
        ttk.Entry(tab, textvariable=self.condition_stop_loss).grid(row=9, column=1, sticky="ew", pady=(10, 0))
        ttk.Label(tab, textvariable=self.quantity_hint_text, wraplength=320, justify="left").grid(
            row=10, column=0, columnspan=2, sticky="w", pady=(10, 0)
        )
        ttk.Button(tab, text="创建条件单", command=self.start_condition_task).grid(row=11, column=0, columnspan=2, sticky="ew", pady=(14, 0))

    def _build_tp_sl_tab(self, tab: ttk.Frame) -> None:
        tab.columnconfigure(1, weight=1)
        ttk.Label(tab, text="持仓方向").grid(row=0, column=0, sticky="w")
        ttk.Combobox(tab, textvariable=self.tp_sl_position_side_label, values=["多仓", "空仓"], state="readonly").grid(row=0, column=1, sticky="ew")
        ttk.Label(tab, text="触发源").grid(row=1, column=0, sticky="w", pady=(10, 0))
        ttk.Combobox(tab, textvariable=self.tp_sl_trigger_source_label, values=list(TRIGGER_SOURCE_OPTIONS.keys()), state="readonly").grid(row=1, column=1, sticky="ew", pady=(10, 0))
        ttk.Label(tab, text="自定义标的").grid(row=2, column=0, sticky="w", pady=(10, 0))
        ttk.Entry(tab, textvariable=self.tp_sl_custom_inst).grid(row=2, column=1, sticky="ew", pady=(10, 0))
        ttk.Label(tab, textvariable=self.tp_sl_size_label_text).grid(row=3, column=0, sticky="w", pady=(10, 0))
        ttk.Entry(tab, textvariable=self.tp_sl_size).grid(row=3, column=1, sticky="ew", pady=(10, 0))
        ttk.Label(tab, text="止盈触发").grid(row=4, column=0, sticky="w", pady=(10, 0))
        ttk.Entry(tab, textvariable=self.tp_sl_take_profit).grid(row=4, column=1, sticky="ew", pady=(10, 0))
        ttk.Label(tab, text="止损触发").grid(row=5, column=0, sticky="w", pady=(10, 0))
        ttk.Entry(tab, textvariable=self.tp_sl_stop_loss).grid(row=5, column=1, sticky="ew", pady=(10, 0))
        ttk.Label(tab, textvariable=self.quantity_hint_text, wraplength=320, justify="left").grid(
            row=6, column=0, columnspan=2, sticky="w", pady=(10, 0)
        )
        ttk.Button(tab, text="启动止盈止损", command=self.start_tp_sl_task).grid(row=7, column=0, columnspan=2, sticky="ew", pady=(14, 0))

    def _build_grid_tab(self, tab: ttk.Frame) -> None:
        tab.columnconfigure(1, weight=1)
        ttk.Label(tab, text="网格开关").grid(row=0, column=0, sticky="w")
        ttk.Checkbutton(tab, text="点击盘口时创建网格任务", variable=self.grid_enabled).grid(row=0, column=1, sticky="w")
        ttk.Label(tab, textvariable=self.grid_size_label_text).grid(row=1, column=0, sticky="w", pady=(10, 0))
        ttk.Entry(tab, textvariable=self.grid_order_size).grid(row=1, column=1, sticky="ew", pady=(10, 0))
        ttk.Label(tab, text="多单参数").grid(row=2, column=0, sticky="w", pady=(10, 0))
        ttk.Entry(tab, textvariable=self.grid_long_step).grid(row=2, column=1, sticky="ew", pady=(10, 0))
        ttk.Label(tab, text="空单参数").grid(row=3, column=0, sticky="w", pady=(10, 0))
        ttk.Entry(tab, textvariable=self.grid_short_step).grid(row=3, column=1, sticky="ew", pady=(10, 0))
        ttk.Label(tab, text="循环次数").grid(row=4, column=0, sticky="w", pady=(10, 0))
        ttk.Combobox(tab, textvariable=self.grid_cycle_label, values=["连续", "1", "3", "5", "10"], state="readonly").grid(row=4, column=1, sticky="ew", pady=(10, 0))
        ttk.Label(tab, textvariable=self.quantity_hint_text, wraplength=320, justify="left").grid(
            row=5, column=0, columnspan=2, sticky="w", pady=(10, 0)
        )
        ttk.Label(
            tab,
            text="只有打开网格开关后，点击右侧规则盘口的买/卖列，才会创建多条独立往返网格任务。",
            wraplength=320,
            justify="left",
        ).grid(row=6, column=0, columnspan=2, sticky="w", pady=(12, 0))

    def _build_position_limit_tab(self, tab: ttk.Frame) -> None:
        tab.columnconfigure(1, weight=1)
        ttk.Label(tab, text="总仓位限制").grid(row=0, column=0, sticky="w")
        ttk.Checkbutton(tab, text="启用总仓位限制", variable=self.position_limit_enabled).grid(row=0, column=1, sticky="w")
        ttk.Label(tab, textvariable=self.position_long_limit_label_text).grid(row=1, column=0, sticky="w", pady=(10, 0))
        ttk.Entry(tab, textvariable=self.position_long_limit).grid(row=1, column=1, sticky="ew", pady=(10, 0))
        ttk.Label(tab, textvariable=self.position_short_limit_label_text).grid(row=2, column=0, sticky="w", pady=(10, 0))
        ttk.Entry(tab, textvariable=self.position_short_limit).grid(row=2, column=1, sticky="ew", pady=(10, 0))
        ttk.Button(tab, text="应用限制", command=self.apply_position_limit_settings).grid(
            row=3, column=0, columnspan=2, sticky="ew", pady=(14, 0)
        )
        ttk.Label(tab, textvariable=self.position_limit_hint_text, wraplength=360, justify="left").grid(
            row=4, column=0, columnspan=2, sticky="w", pady=(10, 0)
        )
        ttk.Label(tab, textvariable=self.position_limit_status, wraplength=360, justify="left").grid(
            row=5, column=0, columnspan=2, sticky="w", pady=(12, 0)
        )

    def _refresh_ladder_filter_options(self, instrument: Instrument) -> None:
        tick = instrument.tick_size
        values = [
            "自动",
            format_decimal_by_increment(tick, tick),
            format_decimal(tick * Decimal("10")),
            format_decimal(tick * Decimal("100")),
        ]
        deduped: list[str] = []
        seen: set[str] = set()
        for value in values:
            if value in seen:
                continue
            deduped.append(value)
            seen.add(value)
        self._ladder_filter_values = deduped
        if self._ladder_filter_combo is not None:
            self._ladder_filter_combo.configure(values=deduped)
        if self.ladder_price_filter.get() not in seen:
            self.ladder_price_filter.set(deduped[0])

    def _resolve_ladder_price_increment(self) -> Decimal | None:
        if self._instrument is None:
            return None
        raw = self.ladder_price_filter.get().strip()
        if not raw or raw == "自动":
            return None
        try:
            value = Decimal(raw)
        except InvalidOperation:
            return self._instrument.tick_size
        if value <= 0:
            return self._instrument.tick_size
        return max(value, self._instrument.tick_size)

    def _on_ladder_filter_changed(self) -> None:
        self._last_ladder_signature = ()
        self._refresh_ladder(force_ticker=False)

    def load_instrument(self) -> None:
        inst_id = self.instrument_id.get().strip().upper()
        if not inst_id:
            messagebox.showerror("加载失败", "请先填写合约。", parent=self.window)
            return
        locked_inst_id = self.manager.locked_inst_id
        if locked_inst_id and locked_inst_id != inst_id:
            messagebox.showinfo("提示", f"当前窗口已锁定 {locked_inst_id}，请先停止并撤掉全部任务。", parent=self.window)
            return
        try:
            instrument = self.client.get_instrument(inst_id)
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("加载失败", str(exc), parent=self.window)
            return
        expected_type = INSTRUMENT_TYPE_OPTIONS[self.instrument_type_label.get()]
        if instrument.inst_type != expected_type:
            messagebox.showerror("加载失败", f"{inst_id} 实际是 {instrument.inst_type}，和当前类型不一致。", parent=self.window)
            return
        try:
            self.manager.set_contract(instrument)
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("加载失败", str(exc), parent=self.window)
            return
        self._instrument = instrument
        self.instrument_id.set(instrument.inst_id)
        self._refresh_ladder_filter_options(instrument)
        self._refresh_quantity_labels()
        self._refresh_favorite_options(preferred_inst_id=instrument.inst_id)
        self.manager.ensure_market_snapshot(instrument, force=True)
        self._last_ladder_signature = ()
        self._last_instrument_status = ""
        self._refresh_views(force_ticker=True, force_position_limit=True)

    def submit_manual_order(self) -> None:
        instrument = self._require_instrument()
        runtime = self._require_runtime()
        try:
            size = self._convert_input_size_to_order_size(self.manual_size.get(), self.manual_size_label_text.get(), instrument)
            side = self._parse_side(self.manual_side.get())
            order_type_value = MANUAL_ORDER_TYPE_OPTIONS[self.manual_order_type_label.get()]
            self.manager.validate_opening_capacity(
                instrument=instrument,
                runtime=runtime,
                side=side,
                size=size,
            )
            if order_type_value == "limit":
                price = self._parse_positive_decimal(self.manual_price.get(), "价格")
                self.client.place_simple_order(
                    runtime.credentials,
                    self._build_runtime_strategy_config(instrument.inst_id, runtime),
                    inst_id=instrument.inst_id,
                    side=side,
                    size=size,
                    ord_type="limit",
                    pos_side=self._manual_pos_side(instrument, runtime, side),
                    price=price,
                )
            elif order_type_value == "best_quote":
                ticker, order_book = self.manager.ensure_market_snapshot(instrument, force=True)
                price = resolve_best_quote_price(
                    side=side,
                    ticker=ticker,
                    order_book=order_book,
                    tick_size=instrument.tick_size,
                )
                self.manual_price.set(format_decimal_by_increment(price, instrument.tick_size))
                self.client.place_simple_order(
                    runtime.credentials,
                    self._build_runtime_strategy_config(instrument.inst_id, runtime),
                    inst_id=instrument.inst_id,
                    side=side,
                    size=size,
                    ord_type="limit",
                    pos_side=self._manual_pos_side(instrument, runtime, side),
                    price=price,
                )
            elif order_type_value == "aggressive_ioc":
                self.client.place_aggressive_limit_order(
                    runtime.credentials,
                    self._build_runtime_strategy_config(instrument.inst_id, runtime),
                    instrument,
                    side=side,
                    size=size,
                    pos_side=self._manual_pos_side(instrument, runtime, side),
                )
            else:
                price = self._parse_positive_decimal(self.manual_price.get(), "价格")
                self.client.place_simple_order(
                    runtime.credentials,
                    self._build_runtime_strategy_config(instrument.inst_id, runtime),
                    inst_id=instrument.inst_id,
                    side=side,
                    size=size,
                    ord_type=order_type_value,
                    pos_side=self._manual_pos_side(instrument, runtime, side),
                    price=price,
                )
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("下单失败", str(exc), parent=self.window)
            return
        messagebox.showinfo("提示", "委托已提交。", parent=self.window)
        self._refresh_views(force_ticker=True, force_position_limit=True)

    def start_condition_task(self) -> None:
        instrument = self._require_instrument()
        runtime = self._require_runtime()
        try:
            trigger_inst_id, trigger_price_type = self._resolve_trigger_source(
                self.condition_trigger_source_label.get(),
                self.condition_custom_inst.get(),
            )
            task_id = self.manager.start_condition_task(
                instrument=instrument,
                runtime=runtime,
                side=self._parse_side(self.condition_side.get()),
                size=self._convert_input_size_to_order_size(self.condition_size.get(), self.condition_size_label_text.get(), instrument),
                trigger_inst_id=trigger_inst_id,
                trigger_price_type=trigger_price_type,
                trigger_direction=self._parse_trigger_direction(self.condition_trigger_direction_label.get()),
                trigger_price=self._parse_positive_decimal(self.condition_trigger_price.get(), "触发价格"),
                exec_mode=self._parse_exec_mode(self.condition_exec_mode_label.get()),
                exec_price=self._parse_optional_positive_decimal(self.condition_exec_price.get(), "执行价格"),
                take_profit=self._parse_optional_positive_decimal(self.condition_take_profit.get(), "止盈触发"),
                stop_loss=self._parse_optional_positive_decimal(self.condition_stop_loss.get(), "止损触发"),
            )
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("创建条件单失败", str(exc), parent=self.window)
            return
        messagebox.showinfo("提示", f"条件单任务 {task_id} 已创建。", parent=self.window)
        self._refresh_views(force_position_limit=True)

    def start_tp_sl_task(self) -> None:
        instrument = self._require_instrument()
        runtime = self._require_runtime()
        try:
            trigger_inst_id, trigger_price_type = self._resolve_trigger_source(
                self.tp_sl_trigger_source_label.get(),
                self.tp_sl_custom_inst.get(),
            )
            task_id = self.manager.start_tp_sl_task(
                instrument=instrument,
                runtime=runtime,
                position_side="long" if self.tp_sl_position_side_label.get() == "多仓" else "short",
                size=self._convert_input_size_to_order_size(self.tp_sl_size.get(), self.tp_sl_size_label_text.get(), instrument),
                trigger_inst_id=trigger_inst_id,
                trigger_price_type=trigger_price_type,
                take_profit=self._parse_optional_positive_decimal(self.tp_sl_take_profit.get(), "止盈触发"),
                stop_loss=self._parse_optional_positive_decimal(self.tp_sl_stop_loss.get(), "止损触发"),
            )
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("启动止盈止损失败", str(exc), parent=self.window)
            return
        messagebox.showinfo("提示", f"止盈止损任务 {task_id} 已创建。", parent=self.window)
        self._refresh_views(force_position_limit=True)

    def restart_selected_task(self) -> None:
        if self._task_tree is None:
            return
        selection = self._task_tree.selection()
        if not selection:
            messagebox.showinfo("提示", "请先选中一个任务。", parent=self.window)
            return
        try:
            runtime = self._require_runtime()
            self.manager.restart_task(selection[0], runtime)
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("重新启动失败", str(exc), parent=self.window)
            return
        messagebox.showinfo("提示", f"任务 {selection[0]} 已重新启动。", parent=self.window)
        self._refresh_views(force_ticker=True, force_position_limit=True)

    def stop_selected_task(self) -> None:
        if self._task_tree is None:
            return
        selection = self._task_tree.selection()
        if not selection:
            messagebox.showinfo("提示", "请先选中一个任务。", parent=self.window)
            return
        try:
            runtime = self._require_runtime()
            self.manager.stop_task(selection[0], runtime)
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("停止失败", str(exc), parent=self.window)
            return
        self._refresh_views(force_position_limit=True)

    def stop_all_tasks(self) -> None:
        try:
            runtime = self._require_runtime()
            self.manager.stop_all(runtime)
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("停止失败", str(exc), parent=self.window)
            return
        self._refresh_views(force_position_limit=True)

    def apply_position_limit_settings(self) -> None:
        try:
            long_limit = self._convert_display_limit_to_internal(self.position_long_limit.get(), self.position_long_limit_label_text.get())
            short_limit = self._convert_display_limit_to_internal(self.position_short_limit.get(), self.position_short_limit_label_text.get())
            if self.position_limit_enabled.get() and long_limit is None and short_limit is None:
                raise RuntimeError("启用总仓位限制后，至少填写一个上限。")
            self.manager.set_position_limits(
                enabled=self.position_limit_enabled.get(),
                long_limit=long_limit,
                short_limit=short_limit,
            )
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("应用限制失败", str(exc), parent=self.window)
            return
        self._sync_position_limit_fields_from_manager()
        self._refresh_views(force_position_limit=True)

    def remove_selected_task(self) -> None:
        if self._task_tree is None:
            return
        selection = self._task_tree.selection()
        if not selection:
            messagebox.showinfo("提示", "请先选择一个任务。", parent=self.window)
            return
        task_id = selection[0]
        try:
            self.manager.remove_task(task_id)
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("删除失败", str(exc), parent=self.window)
            return
        self._last_task_signature = ()
        self._refresh_views(force_position_limit=True)

    def _schedule_refresh(self) -> None:
        if self._destroying:
            return
        self._refresh_views()
        self._refresh_job = self.window.after(REFRESH_INTERVAL_MS, self._schedule_refresh)

    def _refresh_views(self, *, force_ticker: bool = False, force_position_limit: bool = False) -> None:
        self._refresh_lock_state()
        now = time.monotonic()
        should_refresh_position_limit = force_position_limit or now >= self._next_position_limit_refresh_at
        if should_refresh_position_limit:
            self._refresh_position_limit_status(force=force_position_limit)
            self._next_position_limit_refresh_at = now + POSITION_LIMIT_REFRESH_SECONDS
        self._refresh_task_tree()
        self._refresh_logs()
        should_refresh_ladder = force_ticker or now >= self._ladder_refresh_paused_until
        if should_refresh_ladder:
            self._refresh_ladder(force_ticker=force_ticker)

    def _refresh_position_limit_status(self, *, force: bool = False) -> None:
        enabled, long_limit, short_limit = self.manager.get_position_limit_config()
        if not enabled:
            status_text = "未启用总仓位限制。"
            if status_text != self._last_position_limit_status:
                self.position_limit_status.set(status_text)
                self._last_position_limit_status = status_text
            return
        if self._instrument is None:
            long_limit_text = "-" if long_limit is None else format_decimal(long_limit)
            short_limit_text = "-" if short_limit is None else format_decimal(short_limit)
            status_text = f"已启用总仓位限制 | 多头上限={long_limit_text} | 空头上限={short_limit_text}"
            if status_text != self._last_position_limit_status:
                self.position_limit_status.set(status_text)
                self._last_position_limit_status = status_text
            return
        try:
            runtime = self._require_runtime()
            state = self.manager.get_position_limit_state(self._instrument, runtime, force=force)
        except Exception as exc:  # noqa: BLE001
            status_text = f"仓位限制状态获取失败：{exc}"
            if status_text != self._last_position_limit_status:
                self.position_limit_status.set(status_text)
                self._last_position_limit_status = status_text
            return
        long_limit_text = "-" if state.long_limit is None else self._convert_internal_size_to_display_size(state.long_limit)
        short_limit_text = "-" if state.short_limit is None else self._convert_internal_size_to_display_size(state.short_limit)
        long_available = "-" if state.available_long is None else self._convert_internal_size_to_display_size(state.available_long)
        short_available = "-" if state.available_short is None else self._convert_internal_size_to_display_size(state.available_short)
        status_text = (
            "多头：实际 {actual_long} + 预留 {reserved_long} = 占用 {used_long} / 上限 {long_limit} / 可用 {long_available}\n"
            "空头：实际 {actual_short} + 预留 {reserved_short} = 占用 {used_short} / 上限 {short_limit} / 可用 {short_available}".format(
                actual_long=self._convert_internal_size_to_display_size(state.actual_long) or "0",
                reserved_long=self._convert_internal_size_to_display_size(state.reserved_long) or "0",
                used_long=self._convert_internal_size_to_display_size(state.used_long) or "0",
                long_limit=long_limit_text,
                long_available=long_available,
                actual_short=self._convert_internal_size_to_display_size(state.actual_short) or "0",
                reserved_short=self._convert_internal_size_to_display_size(state.reserved_short) or "0",
                used_short=self._convert_internal_size_to_display_size(state.used_short) or "0",
                short_limit=short_limit_text,
                short_available=short_available,
            )
        )
        if status_text != self._last_position_limit_status:
            self.position_limit_status.set(status_text)
            self._last_position_limit_status = status_text

    def _refresh_lock_state(self) -> None:
        lock_id = self.manager.locked_inst_id
        self.window_lock_text.set(f"锁定合约：{lock_id}" if lock_id else "未锁定")

    def _refresh_task_tree(self) -> None:
        if self._task_tree is None:
            return
        tasks = self.manager.list_tasks()
        signature = tuple(
            (
                item.task_id,
                item.task_type,
                item.side,
                item.status,
                item.active_order_price,
                item.active_order_size,
                item.completed_cycles,
                item.cycle_limit_label,
                item.last_message,
            )
            for item in tasks
        )
        if signature == self._last_task_signature:
            return
        self._last_task_signature = signature
        selection = self._task_tree.selection()
        selected_id = selection[0] if selection else None
        task_ids = tuple(item.task_id for item in tasks)
        if tuple(self._task_tree.get_children()) != task_ids:
            self._task_tree.delete(*self._task_tree.get_children())
            for item in tasks:
                self._task_tree.insert("", END, iid=item.task_id, values=("",))
        for item in tasks:
            self._task_tree.item(
                item.task_id,
                values=(
                    item.task_id,
                    item.task_type,
                    item.side,
                    item.status,
                    "-" if item.active_order_price is None else format_decimal_by_increment(item.active_order_price, self._instrument.tick_size if self._instrument else None),
                    "-" if item.active_order_size is None else format_decimal(item.active_order_size),
                    item.cycle_limit_label,
                    item.last_message,
                ),
            )
        if selected_id and self._task_tree.exists(selected_id):
            self._task_tree.selection_set(selected_id)
            self._task_tree.focus(selected_id)

    def _refresh_logs(self) -> None:
        if self._log_text is None:
            return
        logs = tuple(self.manager.list_logs())
        if logs == self._last_log_signature:
            return
        previous_logs = self._last_log_signature
        self._last_log_signature = logs
        if not previous_logs:
            self._log_text.delete("1.0", END)
            if logs:
                self._log_text.insert(END, "\n".join(logs))
                self._log_text.see(END)
            return
        if len(logs) >= len(previous_logs) and logs[: len(previous_logs)] == previous_logs:
            appended = logs[len(previous_logs) :]
            if appended:
                prefix = "\n" if self._log_text.index("end-1c") != "1.0" else ""
                self._log_text.insert(END, prefix + "\n".join(appended))
                self._log_text.see(END)
            return
        self._log_text.delete("1.0", END)
        if logs:
            self._log_text.insert(END, "\n".join(logs))
            self._log_text.see(END)

    def _refresh_ladder(self, *, force_ticker: bool = False) -> None:
        if not self._ladder_trees or self._instrument is None:
            return
        try:
            if force_ticker:
                ticker, order_book = self.manager.ensure_market_snapshot(self._instrument, force=True)
            else:
                ticker, order_book = self.manager.get_cached_market_snapshot(self._instrument)
                if ticker is None:
                    return
            ladder = self.manager.build_ladder(
                self._instrument,
                price_increment=self._resolve_ladder_price_increment(),
            )
        except Exception as exc:  # noqa: BLE001
            self.instrument_status.set(f"{self._instrument.inst_id} | 获取盘口失败：{exc}")
            return
        bid1 = order_book.bids[0][0] if order_book is not None and order_book.bids else ticker.bid
        ask1 = order_book.asks[0][0] if order_book is not None and order_book.asks else ticker.ask
        latest = ticker.last or ticker.mark or ticker.index
        status_text = (
            f"{self._instrument.inst_id} | 最新={format_decimal_by_increment(latest, self._instrument.tick_size) if latest is not None else '-'}"
            f" | 买一={format_decimal_by_increment(bid1, self._instrument.tick_size) if bid1 is not None else '-'}"
            f" | 卖一={format_decimal_by_increment(ask1, self._instrument.tick_size) if ask1 is not None else '-'}"
            f" | {self._format_ladder_tick_status()} | 最小下单量={format_decimal(self._instrument.min_size)}"
        )
        if status_text != self._last_instrument_status:
            self.instrument_status.set(status_text)
            self._last_instrument_status = status_text
        signature = tuple(
            (
                level.price,
                level.buy_working,
                level.sell_working,
                level.working_labels,
                level.is_last_price,
                level.is_best_bid,
                level.is_best_ask,
            )
            for level in ladder
        )
        if signature == self._last_ladder_signature:
            return
        self._last_ladder_signature = signature
        selected_id = self._selected_ladder_row_id()
        row_payloads: list[dict[str, object]] = []
        for row_index, level in enumerate(ladder):
            iid = format_decimal_by_increment(level.price, self._instrument.tick_size)
            marker_labels: list[str] = []
            if level.is_best_ask:
                marker_labels.append("---- 卖一")
            if level.is_last_price:
                marker_labels.append("---- 最新价")
            if level.is_best_bid:
                marker_labels.append("---- 买一")
            working_text = " | ".join([*marker_labels, *level.working_labels]) if marker_labels or level.working_labels else ""
            is_even = row_index % 2 == 0
            row_payloads.append(
                {
                    "iid": iid,
                    "buy": "-" if level.buy_working is None else format_decimal(level.buy_working),
                    "price": iid,
                    "sell": "-" if level.sell_working is None else format_decimal(level.sell_working),
                    "working": working_text,
                    "buy_tags": self._ladder_buy_tags(level, is_even),
                    "price_tags": self._ladder_price_tags(level, is_even),
                    "sell_tags": self._ladder_sell_tags(level, is_even),
                    "working_tags": self._ladder_working_tags(level, is_even),
                }
            )
        self._apply_ladder_rows(row_payloads)
        if selected_id and selected_id in self._ladder_row_ids:
            self._select_ladder_row(selected_id)

    def _selected_ladder_row_id(self) -> str | None:
        for tree in self._ladder_trees.values():
            selected = tree.selection()
            if selected:
                return selected[0]
        return None

    def _select_ladder_row(self, row_id: str) -> None:
        for tree in self._ladder_trees.values():
            if tree.exists(row_id):
                tree.selection_set(row_id)
                tree.focus(row_id)

    def _pause_ladder_refresh(self, *, seconds: float = LADDER_INTERACTION_PAUSE_SECONDS) -> None:
        self._ladder_refresh_paused_until = max(self._ladder_refresh_paused_until, time.monotonic() + seconds)

    def _on_ladder_yview(self, first, last) -> None:
        if self._ladder_scroll is not None:
            self._ladder_scroll.set(first, last)

    def _on_ladder_scroll(self, *args) -> None:
        self._pause_ladder_refresh()
        for tree in self._ladder_trees.values():
            tree.yview(*args)

    def _on_ladder_mousewheel(self, event):
        delta = -1 if event.delta > 0 else 1
        self._pause_ladder_refresh()
        self._on_ladder_scroll("scroll", delta, "units")
        return "break"

    def _apply_ladder_rows(self, row_payloads: list[dict[str, object]]) -> None:
        row_ids = tuple(str(item["iid"]) for item in row_payloads)
        sample_tree = next(iter(self._ladder_trees.values()))
        current_top = sample_tree.yview()[0] if sample_tree.yview() else 0.0
        if row_ids != self._ladder_row_ids:
            for tree in self._ladder_trees.values():
                tree.delete(*tree.get_children())
            for item in row_payloads:
                iid = str(item["iid"])
                self._ladder_trees["buy"].insert("", END, iid=iid, values=(item["buy"],), tags=tuple(item["buy_tags"]))
                self._ladder_trees["price"].insert("", END, iid=iid, values=(item["price"],), tags=tuple(item["price_tags"]))
                self._ladder_trees["sell"].insert("", END, iid=iid, values=(item["sell"],), tags=tuple(item["sell_tags"]))
                self._ladder_trees["working"].insert("", END, iid=iid, values=(item["working"],), tags=tuple(item["working_tags"]))
            self._ladder_row_ids = row_ids
            if current_top > 0:
                for tree in self._ladder_trees.values():
                    tree.yview_moveto(current_top)
            return
        for item in row_payloads:
            iid = str(item["iid"])
            self._ladder_trees["buy"].item(iid, values=(item["buy"],), tags=tuple(item["buy_tags"]))
            self._ladder_trees["price"].item(iid, values=(item["price"],), tags=tuple(item["price_tags"]))
            self._ladder_trees["sell"].item(iid, values=(item["sell"],), tags=tuple(item["sell_tags"]))
            self._ladder_trees["working"].item(iid, values=(item["working"],), tags=tuple(item["working_tags"]))

    def _ladder_buy_tags(self, level, is_even: bool) -> tuple[str, ...]:
        if level.buy_working is not None:
            return ("buy_working",)
        if level.is_best_bid:
            return ("buy_best",)
        return ("buy_even" if is_even else "buy_odd",)

    def _ladder_price_tags(self, level, is_even: bool) -> tuple[str, ...]:
        if level.is_last_price:
            return ("price_last",)
        return ("price_even" if is_even else "price_odd",)

    def _ladder_sell_tags(self, level, is_even: bool) -> tuple[str, ...]:
        if level.sell_working is not None:
            return ("sell_working",)
        if level.is_best_ask:
            return ("sell_best",)
        return ("sell_even" if is_even else "sell_odd",)

    def _ladder_working_tags(self, level, is_even: bool) -> tuple[str, ...]:
        if level.working_labels and level.is_best_bid:
            return ("work_active_bid",)
        if level.working_labels and level.is_best_ask:
            return ("work_active_ask",)
        if level.working_labels:
            return ("work_active",)
        if level.is_best_bid:
            return ("work_bid",)
        if level.is_best_ask:
            return ("work_ask",)
        if level.is_last_price:
            return ("work_last",)
        return ("work_even" if is_even else "work_odd",)

    def _format_ladder_tick_status(self) -> str:
        if self._instrument is None:
            return "tick=-"
        raw = self.ladder_price_filter.get().strip()
        if self._instrument.inst_type == "OPTION" and (not raw or raw == "自动"):
            return "盘口步长=0.0005(>=0.0050) / 0.0001(<0.0050)"
        return f"tick={format_decimal(self._instrument.tick_size)}"

    def _on_ladder_click(self, event) -> None:
        if self._instrument is None:
            return
        column_key = next((key for key, tree in self._ladder_trees.items() if tree is event.widget), None)
        if column_key is None:
            return
        row_id = event.widget.identify_row(event.y)
        if not row_id:
            return
        self._pause_ladder_refresh()
        self._select_ladder_row(row_id)
        price = Decimal(row_id)
        if not self.grid_enabled.get() or column_key not in {"buy", "sell"}:
            self.manual_price.set(format_decimal_by_increment(price, self._instrument.tick_size))
            if column_key == "buy":
                self.manual_side.set("buy")
            elif column_key == "sell":
                self.manual_side.set("sell")
            return
        try:
            runtime = self._require_runtime()
            task_id = self.manager.start_grid_task(
                instrument=self._instrument,
                runtime=runtime,
                side="buy" if column_key == "buy" else "sell",
                entry_price=price,
                size=self._convert_input_size_to_order_size(self.grid_order_size.get(), self.grid_size_label_text.get(), self._instrument),
                long_step=self._parse_positive_decimal(self.grid_long_step.get(), "多单参数"),
                short_step=self._parse_positive_decimal(self.grid_short_step.get(), "空单参数"),
                cycle_mode=self._parse_cycle_mode(self.grid_cycle_label.get()),
                cycle_limit=self._parse_cycle_limit(self.grid_cycle_label.get()),
            )
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("创建网格任务失败", str(exc), parent=self.window)
            return
        self._logger(f"[无限下单] 规则盘口点击创建网格任务 {task_id} | {self._instrument.inst_id} | 价格={format_decimal_by_increment(price, self._instrument.tick_size)}")
        self._refresh_views(force_position_limit=True)

    def _on_close(self) -> None:
        if self.manager.has_active_or_pending_tasks():
            should_stop = messagebox.askyesno(
                "关闭确认",
                "当前窗口还有活动条件单/网格任务。关闭前必须全部停止并撤单，是否现在执行？",
                parent=self.window,
            )
            if not should_stop:
                return
            try:
                runtime = self._require_runtime()
                self.manager.close_all_and_unlock(runtime)
            except Exception as exc:  # noqa: BLE001
                messagebox.showerror("无法关闭", str(exc), parent=self.window)
                return
        self.destroy()

    def _require_instrument(self) -> Instrument:
        if self._instrument is None:
            raise RuntimeError("请先加载一个合约。")
        return self._instrument

    def _require_runtime(self) -> SmartOrderRuntimeConfig:
        runtime = self._runtime_config_provider()
        if runtime is None:
            raise RuntimeError("请先在主界面配置并保存 API 信息。")
        return runtime

    def _resolve_trigger_source(self, label: str, custom_inst_id: str) -> tuple[str, str]:
        instrument = self._require_instrument()
        source_mode, price_type = TRIGGER_SOURCE_OPTIONS[label]
        if source_mode == "current":
            return instrument.inst_id, price_type
        inst_id = custom_inst_id.strip().upper()
        if not inst_id:
            raise RuntimeError("自定义触发模式下，请填写自定义标的。")
        return inst_id, price_type

    def _build_runtime_strategy_config(self, inst_id: str, runtime: SmartOrderRuntimeConfig):
        return self.manager._build_config(inst_id, runtime)

    def _manual_pos_side(self, instrument: Instrument, runtime: SmartOrderRuntimeConfig, side: str):
        if instrument.inst_type in {"SPOT", "OPTION"} or runtime.position_mode != "long_short":
            return None
        return "long" if side == "buy" else "short"

    def _parse_side(self, raw: str) -> str:
        cleaned = raw.strip().lower()
        if cleaned not in {"buy", "sell"}:
            raise RuntimeError("方向只能是 buy 或 sell。")
        return cleaned

    def _parse_trigger_direction(self, raw: str) -> TriggerDirection:
        return "above" if "上" in raw else "below"

    def _parse_exec_mode(self, raw: str) -> ExecutionMode:
        return "limit" if "限价" in raw else "aggressive_ioc"

    def _parse_cycle_mode(self, raw: str) -> CycleMode:
        return "continuous" if raw == "连续" else "counted"

    def _parse_cycle_limit(self, raw: str) -> int | None:
        if raw == "连续":
            return None
        return int(raw)

    def _parse_positive_decimal(self, raw: str, field_name: str) -> Decimal:
        try:
            value = Decimal(raw.strip())
        except (InvalidOperation, ValueError) as exc:
            raise RuntimeError(f"{field_name} 不是有效数字。") from exc
        if value <= 0:
            raise RuntimeError(f"{field_name} 必须大于 0。")
        return value

    def _parse_optional_positive_decimal(self, raw: str, field_name: str) -> Decimal | None:
        cleaned = raw.strip()
        if not cleaned:
            return None
        return self._parse_positive_decimal(cleaned, field_name)
