from __future__ import annotations

import threading
from decimal import Decimal, InvalidOperation
from tkinter import BooleanVar, END, StringVar, Text, Toplevel
from tkinter import messagebox, ttk
from typing import Callable

from okx_quant.arbitrage.arbitrage_executor import ArbitrageCloseRequest, ArbitrageOpenRequest
from okx_quant.arbitrage.arbitrage_manager import ArbitrageManager
from okx_quant.arbitrage.models import ArbitrageOpportunity, ArbitrageTradeRuntime
from okx_quant.persistence import load_arbitrage_settings_snapshot, save_arbitrage_settings_snapshot
from okx_quant.pricing import format_decimal, format_decimal_fixed
from okx_quant.window_layout import apply_adaptive_window_geometry, apply_window_icon


Logger = Callable[[str], None]
RuntimeConfigProvider = Callable[[], ArbitrageTradeRuntime | None]
REFRESH_INTERVAL_MS = 5000
MONITOR_UI_REFRESH_MS = 1000
_SIZE_UNIT_OPTIONS = {
    "币数": "coin",
    "USDT金额": "usdt",
    "合约张数": "contracts",
}
_TRIGGER_MODE_OPTIONS = {
    "价差率触发": "spread",
    "限价触发": "limit_price",
}


class ArbitrageWindow:
    def __init__(
        self,
        parent,
        client,
        *,
        runtime_config_provider: RuntimeConfigProvider | None = None,
        logger: Logger | None = None,
    ) -> None:
        self.client = client
        self._runtime_config_provider = runtime_config_provider
        self._logger = logger or (lambda _message: None)
        self.manager = ArbitrageManager(client, logger=self._append_log)
        self._scan_thread: threading.Thread | None = None
        self._scan_busy = False
        self._destroying = False
        self._refresh_job: str | None = None
        self._monitor_job: str | None = None
        self._opportunities: list[ArbitrageOpportunity] = []
        self._selected_opportunity: ArbitrageOpportunity | None = None
        self._ledger_entries: list = []
        self._derivative_inst_id = StringVar(value="")

        settings = load_arbitrage_settings_snapshot()
        self.auto_refresh_enabled = BooleanVar(value=bool(settings.get("auto_refresh_enabled", True)))
        self.alert_enabled = BooleanVar(value=bool(settings.get("alert_enabled", True)))
        self.min_annual_threshold = StringVar(value=str(settings.get("min_annual_threshold", "5")))
        self.base_ccy = StringVar(value=str(settings.get("base_ccy", "BTC")))
        self.size_value = StringVar(value=str(settings.get("size_value", "1000")))
        self.size_unit_label = StringVar(value=str(settings.get("size_unit_label", "USDT金额")))
        self.max_slippage_percent = StringVar(value=str(settings.get("max_slippage_percent", "0.15")))
        self.trigger_mode_label = StringVar(value=str(settings.get("trigger_mode_label", "价差率触发")))
        self.open_spread_pct_max = StringVar(value=str(settings.get("open_spread_pct_max", "0.05")))
        self.close_spread_pct_min = StringVar(value=str(settings.get("close_spread_pct_min", "0.10")))
        self.spot_limit_price = StringVar(value=str(settings.get("spot_limit_price", "")))
        self.derivative_limit_price = StringVar(value=str(settings.get("derivative_limit_price", "")))
        self.use_limit_orders = BooleanVar(value=bool(settings.get("use_limit_orders", False)))

        self.status_text = StringVar(value="套利模块已就绪。")
        self.scan_status_text = StringVar(value="尚未扫描。")
        self.preview_text = StringVar(value="选择机会或填写参数后，可预览现货/合约换算。")
        self.trade_status_text = StringVar(value="未启动自动开仓。")
        self.monitor_status_text = StringVar(value="—")

        self.window = Toplevel(parent)
        self.window.title("现货套利")
        apply_window_icon(self.window)
        apply_adaptive_window_geometry(
            self.window,
            width_ratio=0.86,
            height_ratio=0.84,
            min_width=1280,
            min_height=820,
            max_width=1800,
            max_height=1100,
        )
        self.window.protocol("WM_DELETE_WINDOW", self._on_close)
        self._build_layout()
        self._reload_ledger()
        self._schedule_scan(refresh_only=False)
        if self.auto_refresh_enabled.get():
            self._schedule_auto_refresh()
        self._schedule_monitor_refresh()

    def show(self) -> None:
        self.window.deiconify()
        self.window.lift()
        self.window.focus_force()

    def destroy(self) -> None:
        self._destroying = True
        try:
            self.manager.stop_auto_open()
            self.manager.stop_auto_close()
        except Exception:
            pass
        if self._refresh_job is not None:
            try:
                self.window.after_cancel(self._refresh_job)
            except Exception:
                pass
            self._refresh_job = None
        if self._monitor_job is not None:
            try:
                self.window.after_cancel(self._monitor_job)
            except Exception:
                pass
            self._monitor_job = None
        if self.window.winfo_exists():
            self.window.destroy()

    def _on_close(self) -> None:
        self._save_settings()
        self.destroy()

    def _save_settings(self) -> None:
        save_arbitrage_settings_snapshot(
            {
                "auto_refresh_enabled": self.auto_refresh_enabled.get(),
                "alert_enabled": self.alert_enabled.get(),
                "min_annual_threshold": self.min_annual_threshold.get().strip(),
                "base_ccy": self.base_ccy.get().strip().upper(),
                "size_value": self.size_value.get().strip(),
                "size_unit_label": self.size_unit_label.get().strip(),
                "max_slippage_percent": self.max_slippage_percent.get().strip(),
                "trigger_mode_label": self.trigger_mode_label.get().strip(),
                "open_spread_pct_max": self.open_spread_pct_max.get().strip(),
                "close_spread_pct_min": self.close_spread_pct_min.get().strip(),
                "spot_limit_price": self.spot_limit_price.get().strip(),
                "derivative_limit_price": self.derivative_limit_price.get().strip(),
                "use_limit_orders": self.use_limit_orders.get(),
            }
        )

    def _build_layout(self) -> None:
        self.window.columnconfigure(0, weight=1)
        self.window.rowconfigure(1, weight=1)

        header = ttk.Frame(self.window, padding=(14, 12, 14, 8))
        header.grid(row=0, column=0, sticky="ew")
        header.columnconfigure(0, weight=1)
        ttk.Label(header, text="现货套利 V1.1", font=("Microsoft YaHei UI", 16, "bold")).grid(row=0, column=0, sticky="w")
        ttk.Label(header, textvariable=self.status_text).grid(row=0, column=1, sticky="e")

        notebook = ttk.Notebook(self.window)
        notebook.grid(row=1, column=0, sticky="nsew", padx=14, pady=(0, 12))
        self._notebook = notebook

        self._build_scan_tab(notebook)
        self._build_trade_tab(notebook)
        self._build_ledger_tab(notebook)
        self._build_log_tab(notebook)

    def _build_scan_tab(self, notebook: ttk.Notebook) -> None:
        frame = ttk.Frame(notebook, padding=12)
        notebook.add(frame, text="机会扫描")
        frame.columnconfigure(0, weight=1)
        frame.rowconfigure(2, weight=1)

        controls = ttk.Frame(frame)
        controls.grid(row=0, column=0, sticky="ew")
        for col in range(8):
            controls.columnconfigure(col, weight=1 if col in {1, 3} else 0)

        ttk.Button(controls, text="立即扫描", command=lambda: self._schedule_scan(refresh_only=False)).grid(
            row=0, column=0, sticky="w", padx=(0, 8)
        )
        ttk.Checkbutton(controls, text="自动刷新(5s)", variable=self.auto_refresh_enabled).grid(
            row=0, column=2, sticky="w", padx=(0, 8)
        )
        ttk.Checkbutton(controls, text="超阈值弹窗", variable=self.alert_enabled).grid(row=0, column=4, sticky="w")
        ttk.Label(controls, text="净年化阈值(%)").grid(row=0, column=5, sticky="e", padx=(12, 4))
        ttk.Entry(controls, textvariable=self.min_annual_threshold, width=8).grid(row=0, column=6, sticky="w")
        ttk.Button(controls, text="一键建仓", command=self._open_trade_from_selection).grid(
            row=0, column=7, sticky="e"
        )

        ttk.Label(frame, textvariable=self.scan_status_text).grid(row=1, column=0, sticky="w", pady=(8, 6))

        columns = (
            "base",
            "kind",
            "spot",
            "derivative",
            "basis",
            "funding",
            "fee",
            "slippage",
            "net",
            "expiry",
        )
        tree_wrap = ttk.Frame(frame)
        tree_wrap.grid(row=2, column=0, sticky="nsew")
        tree_wrap.columnconfigure(0, weight=1)
        tree_wrap.rowconfigure(0, weight=1)
        self.scan_tree = ttk.Treeview(tree_wrap, columns=columns, show="headings", height=18)
        headings = {
            "base": "币种",
            "kind": "类型",
            "spot": "现货",
            "derivative": "衍生品",
            "basis": "基差%",
            "funding": "资金费年化%",
            "fee": "手续费%",
            "slippage": "滑点%",
            "net": "净年化%",
            "expiry": "到期天数",
        }
        for key, label in headings.items():
            self.scan_tree.heading(key, text=label)
        widths = {
            "base": 70,
            "kind": 100,
            "spot": 120,
            "derivative": 150,
            "basis": 80,
            "funding": 100,
            "fee": 80,
            "slippage": 80,
            "net": 90,
            "expiry": 80,
        }
        for key, width in widths.items():
            anchor = "e" if key in {"basis", "funding", "fee", "slippage", "net", "expiry"} else "w"
            self.scan_tree.column(key, width=width, anchor=anchor, stretch=key in {"spot", "derivative"})
        scroll = ttk.Scrollbar(tree_wrap, orient="vertical", command=self.scan_tree.yview)
        self.scan_tree.configure(yscrollcommand=scroll.set)
        self.scan_tree.grid(row=0, column=0, sticky="nsew")
        scroll.grid(row=0, column=1, sticky="ns")
        self.scan_tree.bind("<<TreeviewSelect>>", self._on_scan_select)
        self.scan_tree.bind("<Double-1>", lambda _event: self._open_trade_from_selection())

    def _build_trade_tab(self, notebook: ttk.Notebook) -> None:
        frame = ttk.Frame(notebook, padding=12)
        notebook.add(frame, text="套利开仓")
        frame.columnconfigure(1, weight=1)

        row = 0
        ttk.Label(frame, text="币种").grid(row=row, column=0, sticky="w", pady=4)
        ttk.Entry(frame, textvariable=self.base_ccy, width=16).grid(row=row, column=1, sticky="w", pady=4)
        row += 1

        ttk.Label(frame, text="衍生品").grid(row=row, column=0, sticky="w", pady=4)
        ttk.Entry(frame, textvariable=self._derivative_inst_id, width=36).grid(row=row, column=1, sticky="w", pady=4)
        row += 1

        ttk.Label(frame, text="投入数量").grid(row=row, column=0, sticky="w", pady=4)
        qty_row = ttk.Frame(frame)
        qty_row.grid(row=row, column=1, sticky="ew", pady=4)
        ttk.Entry(qty_row, textvariable=self.size_value, width=16).pack(side="left")
        ttk.Combobox(
            qty_row,
            textvariable=self.size_unit_label,
            values=list(_SIZE_UNIT_OPTIONS.keys()),
            state="readonly",
            width=12,
        ).pack(side="left", padx=(8, 0))
        row += 1

        ttk.Label(frame, text="触发方式").grid(row=row, column=0, sticky="w", pady=4)
        trigger_row = ttk.Frame(frame)
        trigger_row.grid(row=row, column=1, sticky="w", pady=4)
        ttk.Combobox(
            trigger_row,
            textvariable=self.trigger_mode_label,
            values=list(_TRIGGER_MODE_OPTIONS.keys()),
            state="readonly",
            width=14,
        ).pack(side="left")
        ttk.Label(trigger_row, text="  开仓价差率 <").pack(side="left", padx=(12, 4))
        ttk.Entry(trigger_row, textvariable=self.open_spread_pct_max, width=8).pack(side="left")
        ttk.Label(trigger_row, text="%").pack(side="left", padx=(2, 0))
        ttk.Label(trigger_row, text="  平仓价差率 >").pack(side="left", padx=(12, 4))
        ttk.Entry(trigger_row, textvariable=self.close_spread_pct_min, width=8).pack(side="left")
        ttk.Label(trigger_row, text="%").pack(side="left", padx=(2, 0))
        row += 1

        ttk.Label(frame, text="限价条件").grid(row=row, column=0, sticky="w", pady=4)
        limit_row = ttk.Frame(frame)
        limit_row.grid(row=row, column=1, sticky="w", pady=4)
        ttk.Label(limit_row, text="现货买入 ≤").pack(side="left")
        ttk.Entry(limit_row, textvariable=self.spot_limit_price, width=12).pack(side="left", padx=(4, 12))
        ttk.Label(limit_row, text="合约卖出 ≥").pack(side="left")
        ttk.Entry(limit_row, textvariable=self.derivative_limit_price, width=12).pack(side="left", padx=(4, 0))
        row += 1

        ttk.Label(frame, text="执行方式").grid(row=row, column=0, sticky="w", pady=4)
        exec_row = ttk.Frame(frame)
        exec_row.grid(row=row, column=1, sticky="w", pady=4)
        ttk.Checkbutton(exec_row, text="触发后按限价挂单", variable=self.use_limit_orders).pack(side="left", padx=(0, 12))
        ttk.Label(exec_row, text="最大滑点(%)").pack(side="left")
        ttk.Entry(exec_row, textvariable=self.max_slippage_percent, width=8).pack(side="left", padx=(4, 0))
        row += 1

        ttk.Label(frame, text="方向").grid(row=row, column=0, sticky="w", pady=4)
        ttk.Label(frame, text="正向套利：买现货 + 空衍生品（Spot → Swap，Delta 中性）").grid(
            row=row, column=1, sticky="w", pady=4
        )
        row += 1

        ttk.Label(frame, text="换算预览").grid(row=row, column=0, sticky="nw", pady=(12, 4))
        ttk.Label(frame, textvariable=self.preview_text, wraplength=760, justify="left").grid(
            row=row, column=1, sticky="w", pady=(12, 4)
        )
        row += 1

        ttk.Label(frame, text="监控状态").grid(row=row, column=0, sticky="w", pady=4)
        ttk.Label(frame, textvariable=self.monitor_status_text).grid(row=row, column=1, sticky="w", pady=4)
        row += 1

        btn_row = ttk.Frame(frame)
        btn_row.grid(row=row, column=1, sticky="w", pady=(8, 4))
        ttk.Button(btn_row, text="刷新预览", command=self._refresh_preview).pack(side="left", padx=(0, 8))
        ttk.Button(btn_row, text="启动自动开仓", command=self._start_auto_open).pack(side="left", padx=(0, 8))
        ttk.Button(btn_row, text="立即开仓", command=self._open_now).pack(side="left", padx=(0, 8))
        ttk.Button(btn_row, text="停止监控", command=self._stop_auto_open).pack(side="left", padx=(0, 8))
        ttk.Button(btn_row, text="启动自动平仓", command=self._start_auto_close).pack(side="left", padx=(0, 8))
        ttk.Button(btn_row, text="全部平仓", command=self._close_all).pack(side="left")
        row += 1

        ttk.Label(frame, textvariable=self.trade_status_text, wraplength=760, justify="left").grid(
            row=row, column=0, columnspan=2, sticky="w", pady=(12, 0)
        )
        self._derivative_inst_id.set(f"{self.base_ccy.get().strip().upper() or 'BTC'}-USDT-SWAP")

    def _build_ledger_tab(self, notebook: ttk.Notebook) -> None:
        frame = ttk.Frame(notebook, padding=12)
        notebook.add(frame, text="套利账本")
        frame.columnconfigure(0, weight=1)
        frame.rowconfigure(1, weight=1)

        toolbar = ttk.Frame(frame)
        toolbar.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        ttk.Button(toolbar, text="刷新账本", command=self._reload_ledger).pack(side="left", padx=(0, 8))
        ttk.Button(toolbar, text="平仓选中", command=self._close_selected).pack(side="left")

        columns = ("base", "kind", "spot_qty", "swap_qty", "open_basis", "fee", "funding", "pnl", "status", "opened")
        tree_wrap = ttk.Frame(frame)
        tree_wrap.grid(row=1, column=0, sticky="nsew")
        tree_wrap.columnconfigure(0, weight=1)
        tree_wrap.rowconfigure(0, weight=1)
        self.ledger_tree = ttk.Treeview(tree_wrap, columns=columns, show="headings", height=16)
        for key, label in {
            "base": "币种",
            "kind": "类型",
            "spot_qty": "现货数量",
            "swap_qty": "合约数量",
            "open_basis": "开仓基差%",
            "fee": "手续费",
            "funding": "资金费",
            "pnl": "盈亏",
            "status": "状态",
            "opened": "开仓时间",
        }.items():
            self.ledger_tree.heading(key, text=label)
            anchor = "e" if key in {"spot_qty", "swap_qty", "open_basis", "fee", "funding", "pnl"} else "w"
            self.ledger_tree.column(key, width=100, anchor=anchor, stretch=key in {"opened"})
        scroll = ttk.Scrollbar(tree_wrap, orient="vertical", command=self.ledger_tree.yview)
        self.ledger_tree.configure(yscrollcommand=scroll.set)
        self.ledger_tree.grid(row=0, column=0, sticky="nsew")
        scroll.grid(row=0, column=1, sticky="ns")

    def _build_log_tab(self, notebook: ttk.Notebook) -> None:
        frame = ttk.Frame(notebook, padding=12)
        notebook.add(frame, text="执行日志")
        frame.columnconfigure(0, weight=1)
        frame.rowconfigure(0, weight=1)
        self.log_text = Text(frame, wrap="word", height=24)
        self.log_text.grid(row=0, column=0, sticky="nsew")
        scroll = ttk.Scrollbar(frame, orient="vertical", command=self.log_text.yview)
        self.log_text.configure(yscrollcommand=scroll.set)
        scroll.grid(row=0, column=1, sticky="ns")

    def _append_log(self, message: str) -> None:
        if self._destroying:
            return

        def _write() -> None:
            if self._destroying or not self.window.winfo_exists():
                return
            self.log_text.insert(END, message + "\n")
            self.log_text.see(END)
            self.status_text.set(message)

        try:
            self.window.after(0, _write)
        except Exception:
            pass
        self._logger(message)

    def _schedule_auto_refresh(self) -> None:
        if self._destroying or not self.window.winfo_exists():
            return
        if self.auto_refresh_enabled.get():
            self._schedule_scan(refresh_only=True)
        self._refresh_job = self.window.after(REFRESH_INTERVAL_MS, self._schedule_auto_refresh)

    def _schedule_scan(self, *, refresh_only: bool) -> None:
        if self._scan_busy:
            return
        self._scan_busy = True
        if not refresh_only:
            self.scan_status_text.set("扫描中…")

        def _worker() -> None:
            error: str | None = None
            rows: list[ArbitrageOpportunity] = []
            try:
                rows = self.manager.scan_opportunities()
            except Exception as exc:
                error = str(exc)
            if self._destroying:
                return

            def _apply() -> None:
                self._scan_busy = False
                if error is not None:
                    self.scan_status_text.set(f"扫描失败：{error}")
                    self._append_log(f"套利扫描失败：{error}")
                    return
                self._opportunities = rows
                self._render_scan_rows(rows)
                self.scan_status_text.set(f"共 {len(rows)} 条机会，按净年化降序。")
                self._maybe_alert(rows)

            try:
                self.window.after(0, _apply)
            except Exception:
                self._scan_busy = False

        self._scan_thread = threading.Thread(target=_worker, name="arbitrage-scan", daemon=True)
        self._scan_thread.start()

    def _render_scan_rows(self, rows: list[ArbitrageOpportunity]) -> None:
        self.scan_tree.delete(*self.scan_tree.get_children())
        for index, item in enumerate(rows):
            self.scan_tree.insert(
                "",
                END,
                iid=str(index),
                values=(
                    item.base_ccy,
                    item.pair_kind_label,
                    item.spot_inst_id,
                    item.derivative_inst_id,
                    format_decimal_fixed(item.basis_pct, 4),
                    "-" if item.funding_annual_pct is None else format_decimal_fixed(item.funding_annual_pct, 4),
                    format_decimal_fixed(item.fee_round_trip_pct, 4),
                    format_decimal_fixed(item.slippage_est_pct, 4),
                    format_decimal_fixed(item.net_annual_pct, 4),
                    "-" if item.days_to_expiry is None else str(item.days_to_expiry),
                ),
            )

    def _maybe_alert(self, rows: list[ArbitrageOpportunity]) -> None:
        if not self.alert_enabled.get() or not rows:
            return
        try:
            threshold = Decimal(self.min_annual_threshold.get().strip() or "0")
        except InvalidOperation:
            return
        best = rows[0]
        if best.net_annual_pct >= threshold:
            messagebox.showinfo(
                "套利机会提醒",
                (
                    f"{best.base_ccy} {best.pair_kind_label}\n"
                    f"净年化：{format_decimal_fixed(best.net_annual_pct, 2)}%\n"
                    f"基差：{format_decimal_fixed(best.basis_pct, 4)}%\n"
                    f"可在「建仓/平仓」页一键带入参数。"
                ),
                parent=self.window,
            )

    def _on_scan_select(self, _event=None) -> None:
        selected = self.scan_tree.selection()
        if not selected:
            self._selected_opportunity = None
            return
        try:
            index = int(selected[0])
        except ValueError:
            return
        if 0 <= index < len(self._opportunities):
            self._selected_opportunity = self._opportunities[index]

    def _open_trade_from_selection(self) -> None:
        if self._selected_opportunity is None:
            selected = self.scan_tree.selection()
            if not selected:
                messagebox.showwarning("提示", "请先在扫描列表中选择一条机会。", parent=self.window)
                return
            try:
                index = int(selected[0])
                self._selected_opportunity = self._opportunities[index]
            except (ValueError, IndexError):
                messagebox.showwarning("提示", "当前选择无效，请重新扫描。", parent=self.window)
                return
        opp = self._selected_opportunity
        self.base_ccy.set(opp.base_ccy)
        self._derivative_inst_id.set(opp.derivative_inst_id)
        self._notebook.select(1)
        self._refresh_preview(derivative_inst_id=opp.derivative_inst_id)
        self._append_log(f"已带入 {opp.base_ccy} {opp.pair_kind_label} 建仓参数。")

    def _runtime_or_warn(self) -> ArbitrageTradeRuntime | None:
        if self._runtime_config_provider is None:
            messagebox.showwarning("提示", "未绑定 API 凭证，请先在主界面登录并保存 API。", parent=self.window)
            return None
        runtime = self._runtime_config_provider()
        if runtime is None:
            messagebox.showwarning("提示", "请先在主界面配置并保存 API 凭证。", parent=self.window)
            return None
        return runtime

    def _parse_optional_decimal(self, text: str) -> Decimal | None:
        normalized = text.strip()
        if not normalized:
            return None
        return Decimal(normalized)

    def _parse_max_slippage(self) -> Decimal:
        return Decimal(self.max_slippage_percent.get().strip() or "0.15") / Decimal("100")

    def _build_open_request(self) -> ArbitrageOpenRequest:
        base = self.base_ccy.get().strip().upper()
        if not base:
            raise ValueError("请填写币种。")
        derivative_inst_id = self._derivative_inst_id.get().strip().upper()
        if not derivative_inst_id:
            derivative_inst_id = f"{base}-USDT-SWAP"
        trigger_mode = _TRIGGER_MODE_OPTIONS.get(self.trigger_mode_label.get().strip(), "spread")
        spread_text = self.open_spread_pct_max.get().strip()
        open_spread = Decimal(spread_text) if spread_text else None
        return ArbitrageOpenRequest(
            base_ccy=base,
            spot_inst_id=f"{base}-USDT",
            derivative_inst_id=derivative_inst_id,
            size=self._parse_size(),
            size_unit=self._parse_size_unit(),  # type: ignore[arg-type]
            trigger_mode=trigger_mode,
            open_spread_pct_max=open_spread,
            spot_limit_price=self._parse_optional_decimal(self.spot_limit_price.get()),
            derivative_limit_price=self._parse_optional_decimal(self.derivative_limit_price.get()),
            use_limit_orders=self.use_limit_orders.get(),
            max_slippage=self._parse_max_slippage(),
        )

    def _schedule_monitor_refresh(self) -> None:
        if self._destroying or not self.window.winfo_exists():
            return
        session = self.manager.auto_open.session
        if session is not None:
            parts = [session.status]
            if session.last_spread_pct is not None:
                parts.append(f"价差 {format_decimal_fixed(session.last_spread_pct, 4)}%")
            self.monitor_status_text.set(" | ".join(parts))
            self.trade_status_text.set(session.status)
            if session.result is not None:
                self._reload_ledger()
        else:
            close_session = self.manager.auto_close.session
            if close_session is not None:
                parts = [close_session.status]
                if close_session.last_spread_pct is not None:
                    parts.append(f"价差 {format_decimal_fixed(close_session.last_spread_pct, 4)}%")
                self.monitor_status_text.set(" | ".join(parts))
                self.trade_status_text.set(close_session.status)
                if close_session.result is not None:
                    self._reload_ledger()
            elif not self.manager.auto_open.is_running and not self.manager.auto_close.is_running:
                self.monitor_status_text.set("未监控")
        self._monitor_job = self.window.after(MONITOR_UI_REFRESH_MS, self._schedule_monitor_refresh)

    def _start_auto_open(self) -> None:
        runtime = self._runtime_or_warn()
        if runtime is None:
            return
        try:
            request = self._build_open_request()
        except (InvalidOperation, ValueError) as exc:
            messagebox.showwarning("参数错误", str(exc), parent=self.window)
            return
        if request.trigger_mode == "spread" and request.open_spread_pct_max is None:
            messagebox.showwarning("参数错误", "价差率触发需要填写「开仓价差率」。", parent=self.window)
            return
        if request.trigger_mode == "limit_price" and (
            request.spot_limit_price is None and request.derivative_limit_price is None
        ):
            messagebox.showwarning("参数错误", "限价触发至少填写一侧限价。", parent=self.window)
            return
        if self.manager.auto_open.is_running:
            messagebox.showwarning("提示", "已有监控任务在运行，请先停止。", parent=self.window)
            return
        if self.manager.auto_close.is_running:
            messagebox.showwarning("提示", "自动平仓监控运行中，请先停止。", parent=self.window)
            return
        try:
            self.manager.start_auto_open(request, runtime=runtime)
        except Exception as exc:
            messagebox.showerror("启动失败", str(exc), parent=self.window)
            return
        self.trade_status_text.set("自动开仓监控已启动。")
        self._append_log("已启动自动开仓监控。")

    def _stop_auto_open(self) -> None:
        self.manager.stop_auto_open()
        self.manager.stop_auto_close()
        self.trade_status_text.set("监控已停止。")
        self.monitor_status_text.set("已停止")
        self._append_log("已停止套利监控。")

    def _open_now(self) -> None:
        runtime = self._runtime_or_warn()
        if runtime is None:
            return
        if not messagebox.askyesno(
            "确认立即开仓",
            "将按当前价格与数量立即执行：买现货 → 空合约。\n确认继续？",
            parent=self.window,
        ):
            return
        try:
            request = self._build_open_request()
        except (InvalidOperation, ValueError) as exc:
            messagebox.showwarning("参数错误", str(exc), parent=self.window)
            return

        self.trade_status_text.set("正在开仓…")
        self._append_log("立即开仓：提交中…")

        def _worker() -> None:
            result = self.manager.open_now(request, runtime=runtime)

            def _apply() -> None:
                self.trade_status_text.set(result.message)
                self._append_log(result.message)
                if result.success:
                    self._reload_ledger()
                    messagebox.showinfo("开仓完成", result.message, parent=self.window)
                else:
                    messagebox.showerror("开仓失败", result.message, parent=self.window)

            try:
                self.window.after(0, _apply)
            except Exception:
                pass

        threading.Thread(target=_worker, name="arbitrage-open-now", daemon=True).start()

    def _build_close_request(self, *, entry_id: str | None = None) -> ArbitrageCloseRequest:
        return ArbitrageCloseRequest(
            entry_id=entry_id,
            max_slippage=self._parse_max_slippage(),
            use_limit_orders=self.use_limit_orders.get(),
            spot_limit_price=self._parse_optional_decimal(self.spot_limit_price.get()),
            derivative_limit_price=self._parse_optional_decimal(self.derivative_limit_price.get()),
        )

    def _start_auto_close(self) -> None:
        runtime = self._runtime_or_warn()
        if runtime is None:
            return
        open_entries = self.manager.load_open_ledger()
        if not open_entries:
            messagebox.showwarning("提示", "当前没有未平仓的套利持仓。", parent=self.window)
            return
        try:
            close_spread = Decimal(self.close_spread_pct_min.get().strip() or "0")
        except InvalidOperation:
            messagebox.showwarning("参数错误", "平仓价差率无效。", parent=self.window)
            return
        selected_entry_id = self._selected_ledger_entry_id()
        if self.manager.auto_open.is_running:
            messagebox.showwarning("提示", "已有监控任务在运行，请先停止。", parent=self.window)
            return
        if self.manager.auto_close.is_running:
            messagebox.showwarning("提示", "自动平仓监控运行中，请先停止。", parent=self.window)
            return
        try:
            request = self._build_close_request(entry_id=selected_entry_id)
            self.manager.start_auto_close(
                request=request,
                runtime=runtime,
                close_spread_pct_min=close_spread,
                entry_id=selected_entry_id,
            )
        except Exception as exc:
            messagebox.showerror("启动失败", str(exc), parent=self.window)
            return
        self.trade_status_text.set("自动平仓监控已启动。")
        self._append_log("已启动自动平仓监控。")

    def _close_all(self) -> None:
        self._close_entries(entry_id=None, label="全部平仓")

    def _close_selected(self) -> None:
        entry_id = self._selected_ledger_entry_id()
        if not entry_id:
            messagebox.showwarning("提示", "请先在账本中选择一条未平仓记录。", parent=self.window)
            return
        self._close_entries(entry_id=entry_id, label="平仓选中")

    def _selected_ledger_entry_id(self) -> str | None:
        selected = self.ledger_tree.selection()
        if not selected:
            return None
        entry_id = selected[0]
        if entry_id.startswith("placeholder"):
            return None
        return entry_id

    def _close_entries(self, *, entry_id: str | None, label: str) -> None:
        runtime = self._runtime_or_warn()
        if runtime is None:
            return
        if not messagebox.askyesno("确认平仓", f"将执行{label}：平合约 → 卖现货。\n确认继续？", parent=self.window):
            return
        try:
            request = self._build_close_request(entry_id=entry_id)
        except InvalidOperation as exc:
            messagebox.showwarning("参数错误", str(exc), parent=self.window)
            return
        self.trade_status_text.set("正在平仓…")
        self._append_log(f"{label}：提交中…")

        def _worker() -> None:
            result = self.manager.close_now(request, runtime=runtime)

            def _apply() -> None:
                self.trade_status_text.set(result.message)
                self._append_log(result.message)
                self._reload_ledger()
                if result.success:
                    messagebox.showinfo("平仓完成", result.message, parent=self.window)
                else:
                    messagebox.showerror("平仓失败", result.message, parent=self.window)

            try:
                self.window.after(0, _apply)
            except Exception:
                pass

        threading.Thread(target=_worker, name="arbitrage-close", daemon=True).start()

    def _parse_size(self) -> Decimal:
        return Decimal(self.size_value.get().strip())

    def _parse_size_unit(self) -> str:
        label = self.size_unit_label.get().strip()
        return _SIZE_UNIT_OPTIONS.get(label, "usdt")

    def _refresh_preview(self, *, derivative_inst_id: str | None = None) -> None:
        base = self.base_ccy.get().strip().upper()
        if not base:
            self.preview_text.set("请先填写币种。")
            return
        if derivative_inst_id is None:
            derivative_inst_id = self._derivative_inst_id.get().strip().upper()
            if not derivative_inst_id:
                if self._selected_opportunity is not None and self._selected_opportunity.base_ccy == base:
                    derivative_inst_id = self._selected_opportunity.derivative_inst_id
                else:
                    derivative_inst_id = f"{base}-USDT-SWAP"
        try:
            preview = self.manager.preview_size(
                base_ccy=base,
                derivative_inst_id=derivative_inst_id,
                size=self._parse_size(),
                unit=self._parse_size_unit(),  # type: ignore[arg-type]
            )
        except Exception as exc:
            self.preview_text.set(f"预览失败：{exc}")
            return
        self.preview_text.set(
            "\n".join(
                [
                    f"现货 {base}-USDT：{format_decimal(preview.spot_base_qty)}",
                    f"衍生品 {derivative_inst_id}：{format_decimal(preview.swap_contracts)} 张",
                    f"名义价值约：{format_decimal_fixed(preview.notional_usdt, 2)} USDT",
                    f"最大滑点：{self.max_slippage_percent.get().strip()}%",
                ]
            )
        )

    def _reload_ledger(self) -> None:
        self.ledger_tree.delete(*self.ledger_tree.get_children())
        entries = self.manager.load_ledger()
        self._ledger_entries = entries
        if not entries:
            self.ledger_tree.insert("", END, iid="placeholder", values=("—", "暂无套利账本记录", "", "", "", "", "", "", "", ""))
            return
        for item in entries:
            self.ledger_tree.insert(
                item.entry_id,
                END,
                values=(
                    item.base_ccy,
                    item.pair_kind,
                    format_decimal(item.spot_qty),
                    format_decimal(item.derivative_qty),
                    "-" if item.basis_at_open_pct is None else format_decimal_fixed(item.basis_at_open_pct, 4),
                    format_decimal_fixed(item.fee_total, 4),
                    format_decimal_fixed(item.funding_total, 4),
                    "-" if item.realized_pnl is None else format_decimal_fixed(item.realized_pnl, 4),
                    item.close_mode,
                    item.opened_at,
                ),
            )
