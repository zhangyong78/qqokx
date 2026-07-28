from __future__ import annotations

from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from tkinter import END, StringVar, Text, Toplevel, messagebox, ttk

from okx_quant.semi_auto_desk import SemiAutoTaskRecord, semi_auto_pool_ledger_records


def _format_optional_datetime(value: object) -> str:
    return value.strftime("%m-%d %H:%M") if isinstance(value, datetime) else "-"


def _format_optional_decimal(value: object, *, signed: bool = False) -> str:
    if not isinstance(value, Decimal):
        return "-"
    rounded = value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return f"{'+' if signed and rounded > 0 else ''}{rounded:.2f}"


def _task_strategy_name(task: SemiAutoTaskRecord) -> str:
    return str(task.template_payload.get("strategy_name") or task.template_payload.get("strategy_id") or "未命名策略").strip()


def build_semi_auto_task_rows(tasks: list[SemiAutoTaskRecord]) -> list[tuple[str, tuple[object, ...]]]:
    mode_label = {"evaluate_once": "单次判断", "wait_one": "等待一单"}
    rows: list[tuple[str, tuple[object, ...]]] = []
    for task in sorted(tasks, key=lambda item: (item.created_at, item.task_id), reverse=True):
        rows.append(
            (
                task.task_id,
                (
                    task.task_id,
                    _task_strategy_name(task),
                    task.symbol or "-",
                    task.direction_label or "-",
                    mode_label.get(task.mode, task.mode or "-"),
                    task.status or "-",
                    task.bar or "-",
                    task.session_id or "-",
                    task.ended_reason or "-",
                ),
            )
        )
    return rows


def build_semi_auto_pool_ledger_rows(pool_id: str, ledger_records: list[object]) -> list[tuple[str, tuple[object, ...]]]:
    rows: list[tuple[str, tuple[object, ...]]] = []
    for record in reversed(semi_auto_pool_ledger_records(pool_id, ledger_records)):
        rows.append(
            (
                str(getattr(record, "record_id", "") or ""),
                (
                    _format_optional_datetime(getattr(record, "closed_at", None)),
                    str(getattr(record, "strategy_name", "") or "-").strip() or "-",
                    str(getattr(record, "symbol", "") or "-").strip() or "-",
                    str(getattr(record, "direction_label", "") or "-").strip() or "-",
                    _format_optional_datetime(getattr(record, "opened_at", None)),
                    _format_optional_decimal(getattr(record, "entry_price", None)),
                    _format_optional_decimal(getattr(record, "exit_price", None)),
                    _format_optional_decimal(getattr(record, "net_pnl", None), signed=True),
                    str(getattr(record, "close_reason", "") or "-").strip() or "-",
                ),
            )
        )
    return rows


class SemiAutoDeskWindow:
    def __init__(
        self,
        parent,
        *,
        snapshot_provider,
        current_template_factory,
        template_serializer,
        pool_creator,
        task_adder,
        task_starter,
        task_canceller,
        ledger_provider,
        summary_provider,
        default_api_name: str = "",
    ) -> None:
        self._snapshot_provider = snapshot_provider
        self._current_template_factory = current_template_factory
        self._template_serializer = template_serializer
        self._pool_creator = pool_creator
        self._task_adder = task_adder
        self._task_starter = task_starter
        self._task_canceller = task_canceller
        self._ledger_provider = ledger_provider
        self._summary_provider = summary_provider
        self._snapshot = None
        self._selected_pool_id = ""
        self.pool_name_var = StringVar(value="半自动主操盘")
        self.api_name_var = StringVar(value=default_api_name)
        self.initial_capital_var = StringVar(value="1000")
        self.mode_var = StringVar(value="wait_one")
        self.summary_var = StringVar(value="")

        self.window = Toplevel(parent)
        self.window.title("半自动操盘台")
        self.window.geometry("1360x820")
        self.window.minsize(1120, 700)
        self.window.protocol("WM_DELETE_WINDOW", self.window.withdraw)

        root = ttk.Frame(self.window, padding=12)
        root.pack(fill="both", expand=True)
        root.columnconfigure(0, weight=1)
        root.columnconfigure(1, weight=2)
        root.rowconfigure(2, weight=1)

        create_row = ttk.LabelFrame(root, text="操盘组合", padding=8)
        create_row.grid(row=0, column=0, columnspan=2, sticky="ew")
        for index in range(7):
            create_row.columnconfigure(index, weight=1 if index in {1, 3, 5} else 0)
        ttk.Label(create_row, text="名称").grid(row=0, column=0, sticky="w")
        ttk.Entry(create_row, textvariable=self.pool_name_var, width=18).grid(row=0, column=1, sticky="ew", padx=(4, 10))
        ttk.Label(create_row, text="API").grid(row=0, column=2, sticky="w")
        ttk.Entry(create_row, textvariable=self.api_name_var, width=14).grid(row=0, column=3, sticky="ew", padx=(4, 10))
        ttk.Label(create_row, text="初始虚拟资金").grid(row=0, column=4, sticky="w")
        ttk.Entry(create_row, textvariable=self.initial_capital_var, width=12).grid(row=0, column=5, sticky="ew", padx=(4, 10))
        ttk.Button(create_row, text="新建组合", command=self._create_pool).grid(row=0, column=6, sticky="e")

        action_row = ttk.Frame(root)
        action_row.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(10, 8))
        ttk.Label(action_row, text="一次性模式").pack(side="left")
        ttk.Combobox(
            action_row,
            textvariable=self.mode_var,
            values=("wait_one", "evaluate_once"),
            state="readonly",
            width=14,
        ).pack(side="left", padx=(4, 10))
        ttk.Button(action_row, text="加入当前策略", command=self._add_current_strategy).pack(side="left")
        ttk.Button(action_row, text="启动选中任务", command=self._start_selected_task).pack(side="left", padx=(8, 0))
        ttk.Button(action_row, text="取消选中任务", command=self._cancel_selected_task).pack(side="left", padx=(8, 0))
        ttk.Button(action_row, text="打开组合总账本", command=self._open_book).pack(side="left", padx=(8, 0))

        pool_frame = ttk.LabelFrame(root, text="操盘组合", padding=6)
        pool_frame.grid(row=2, column=0, sticky="nsew", padx=(0, 8))
        pool_frame.columnconfigure(0, weight=1)
        pool_frame.rowconfigure(0, weight=1)
        self.pool_tree = ttk.Treeview(pool_frame, columns=("id", "name", "api", "capital", "status"), show="headings", height=18)
        for key, text, width in (("id", "编号", 70), ("name", "名称", 160), ("api", "API", 110), ("capital", "初始资金", 100), ("status", "状态", 110)):
            self.pool_tree.heading(key, text=text)
            self.pool_tree.column(key, width=width, anchor="center")
        self.pool_tree.grid(row=0, column=0, sticky="nsew")
        self.pool_tree.bind("<<TreeviewSelect>>", self._on_pool_select)

        task_frame = ttk.LabelFrame(root, text="一次性策略任务", padding=6)
        task_frame.grid(row=2, column=1, sticky="nsew")
        task_frame.columnconfigure(0, weight=1)
        task_frame.rowconfigure(0, weight=1)
        columns = ("id", "strategy", "symbol", "direction", "mode", "status", "bar", "session", "reason")
        self.task_tree = ttk.Treeview(task_frame, columns=columns, show="headings", height=18)
        headings = {
            "id": ("任务", 105), "strategy": ("策略", 180), "symbol": ("币种", 130), "direction": ("方向", 75),
            "mode": ("模式", 90), "status": ("状态", 105), "bar": ("周期", 70), "session": ("会话", 70), "reason": ("结束原因", 180),
        }
        for key in columns:
            self.task_tree.heading(key, text=headings[key][0])
            self.task_tree.column(key, width=headings[key][1], anchor="center")
        self.task_tree.grid(row=0, column=0, sticky="nsew")
        ttk.Label(root, textvariable=self.summary_var, justify="left").grid(row=3, column=0, columnspan=2, sticky="w", pady=(8, 0))
        self.refresh()

    def show(self) -> None:
        self.window.deiconify()
        self.window.lift()
        self.window.focus_force()
        self.refresh()

    def _selected_task_id(self) -> str:
        selected = self.task_tree.selection()
        return str(selected[0]) if selected else ""

    def _create_pool(self) -> None:
        try:
            pool = self._pool_creator(
                self.pool_name_var.get(),
                self.api_name_var.get(),
                Decimal(self.initial_capital_var.get().strip()),
            )
        except Exception as exc:
            messagebox.showerror("新建组合失败", str(exc), parent=self.window)
            return
        self._selected_pool_id = pool.pool_id
        self.refresh()

    def _add_current_strategy(self) -> None:
        if not self._selected_pool_id:
            messagebox.showinfo("提示", "请先选择操盘组合。", parent=self.window)
            return
        try:
            payload = self._template_serializer(self._current_template_factory())
            self._task_adder(self._selected_pool_id, payload, self.mode_var.get())
        except Exception as exc:
            messagebox.showerror("加入策略失败", str(exc), parent=self.window)
            return
        self.refresh()

    def _start_selected_task(self) -> None:
        task_id = self._selected_task_id()
        if not task_id:
            messagebox.showinfo("提示", "请先选择任务。", parent=self.window)
            return
        try:
            self._task_starter(task_id)
        except Exception as exc:
            messagebox.showerror("启动失败", str(exc), parent=self.window)
            return
        self.refresh()

    def _cancel_selected_task(self) -> None:
        task_id = self._selected_task_id()
        if not task_id:
            messagebox.showinfo("提示", "请先选择任务。", parent=self.window)
            return
        try:
            self._task_canceller(task_id)
        except Exception as exc:
            messagebox.showerror("取消失败", str(exc), parent=self.window)
            return
        self.refresh()

    def _on_pool_select(self, _event=None) -> None:
        selected = self.pool_tree.selection()
        if selected:
            self._selected_pool_id = str(selected[0])
            self.refresh()

    def refresh(self) -> None:
        self._snapshot = self._snapshot_provider()
        pools = self._snapshot.pools
        if not self._selected_pool_id and pools:
            self._selected_pool_id = pools[0].pool_id
        self.pool_tree.delete(*self.pool_tree.get_children())
        for pool in pools:
            self.pool_tree.insert("", END, iid=pool.pool_id, values=(pool.pool_id, pool.name, pool.api_name, _format_optional_decimal(pool.initial_capital), pool.status))
        if self._selected_pool_id and self.pool_tree.exists(self._selected_pool_id):
            self.pool_tree.selection_set(self._selected_pool_id)
        tasks = [task for task in self._snapshot.tasks if task.pool_id == self._selected_pool_id]
        self.task_tree.delete(*self.task_tree.get_children())
        for row_id, values in build_semi_auto_task_rows(tasks):
            self.task_tree.insert("", END, iid=row_id, values=values)
        pool = next((item for item in pools if item.pool_id == self._selected_pool_id), None)
        if pool is None:
            self.summary_var.set("请新建或选择一个操盘组合。")
            return
        summary = self._summary_provider(pool)
        self.summary_var.set(
            f"组合 {pool.name} | 已结算 {summary.realized_count} 笔 | 胜率 {summary.win_rate:.2f}% | "
            f"累计净盈亏 {summary.net_pnl:+.2f} USDT | 虚拟资金 {summary.virtual_equity:.2f} USDT | "
            f"盈亏比 {'-' if summary.profit_loss_ratio is None else format(summary.profit_loss_ratio, '.2f')}"
        )

    def _open_book(self) -> None:
        if not self._selected_pool_id:
            return
        window = Toplevel(self.window)
        window.title("半自动操盘组合总账本")
        window.geometry("1280x620")
        frame = ttk.Frame(window, padding=12)
        frame.pack(fill="both", expand=True)
        columns = ("closed", "strategy", "symbol", "direction", "opened", "entry", "exit", "pnl", "reason")
        tree = ttk.Treeview(frame, columns=columns, show="headings")
        labels = ("平仓时间", "策略", "币种", "方向", "开仓时间", "开仓价", "平仓价", "净盈亏", "平仓原因")
        for key, label in zip(columns, labels, strict=True):
            tree.heading(key, text=label)
            tree.column(key, width=125, anchor="center")
        tree.pack(fill="both", expand=True)
        for row_id, values in build_semi_auto_pool_ledger_rows(self._selected_pool_id, self._ledger_provider()):
            tree.insert("", END, iid=row_id, values=values)
