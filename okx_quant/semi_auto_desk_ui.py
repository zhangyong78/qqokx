from __future__ import annotations

from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from tkinter import END, StringVar, Text, Toplevel, messagebox, ttk

from okx_quant.semi_auto_desk import SemiAutoTaskRecord, semi_auto_pool_ledger_records
from okx_quant.strategy_live_chart import StrategyLiveChartTimeMarker


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
    status_label = {
        "queued": "待启动",
        "running": "等待信号",
        "opened": "已开仓",
        "settling": "结算中",
        "completed_no_signal": "已结束（无信号）",
        "completed_closed": "已结束（已平仓）",
        "blocked_conflict": "未执行（仓位冲突）",
        "cancelled": "已取消",
        "failed": "失败",
    }
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
                    status_label.get(task.status, task.status or "-"),
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


def build_semi_auto_pool_replay_time_markers(
    pool_id: str,
    symbol: str,
    ledger_records: list[object],
) -> tuple[StrategyLiveChartTimeMarker, ...]:
    normalized_symbol = str(symbol or "").strip().upper()
    markers: list[StrategyLiveChartTimeMarker] = []
    for record in semi_auto_pool_ledger_records(pool_id, ledger_records):
        if str(getattr(record, "symbol", "") or "").strip().upper() != normalized_symbol:
            continue
        record_id = str(getattr(record, "record_id", "") or "").strip()
        if not record_id:
            continue
        strategy_name = str(getattr(record, "strategy_name", "") or "未命名策略").strip() or "未命名策略"
        direction = str(getattr(record, "direction_label", "") or "").strip()
        suffix = f" {direction}" if direction else ""
        opened_at = getattr(record, "opened_at", None)
        if isinstance(opened_at, datetime):
            markers.append(
                StrategyLiveChartTimeMarker(
                    key=f"open:{record_id}",
                    label=(
                        f"开仓 {strategy_name}{suffix}\n"
                        f"{opened_at.strftime('%m-%d %H:%M')} | 价格={_format_optional_decimal(getattr(record, 'entry_price', None))}"
                    ),
                    at=opened_at,
                    color="#6f42c1",
                    dash=(4, 3),
                    width=2,
                    vertical_anchor="below",
                )
            )
        closed_at = getattr(record, "closed_at", None)
        if isinstance(closed_at, datetime):
            markers.append(
                StrategyLiveChartTimeMarker(
                    key=f"close:{record_id}",
                    label=(
                        f"平仓 {strategy_name}{suffix}\n"
                        f"{closed_at.strftime('%m-%d %H:%M')} | 价格={_format_optional_decimal(getattr(record, 'exit_price', None))}\n"
                        f"本次盈亏={_format_optional_decimal(getattr(record, 'net_pnl', None), signed=True)} USDT"
                    ),
                    at=closed_at,
                    color="#cf222e",
                    dash=(4, 3),
                    width=2,
                    vertical_anchor="above",
                )
            )
    return tuple(markers)


def build_semi_auto_pool_performance_rows(pool_id: str, ledger_records: list[object]) -> list[tuple[str, tuple[str, ...]]]:
    groups: dict[tuple[str, str, str], list[Decimal]] = {}
    for record in semi_auto_pool_ledger_records(pool_id, ledger_records):
        raw_pnl = getattr(record, "net_pnl", None)
        if raw_pnl is None:
            continue
        try:
            pnl = Decimal(str(raw_pnl))
        except Exception:
            continue
        strategy = str(getattr(record, "strategy_name", "") or "未命名策略").strip() or "未命名策略"
        symbol = str(getattr(record, "symbol", "") or "-").strip() or "-"
        direction = str(getattr(record, "direction_label", "") or "-").strip() or "-"
        groups.setdefault((strategy, symbol, direction), []).append(pnl)

    rows: list[tuple[str, tuple[str, ...]]] = []
    for index, ((strategy, symbol, direction), pnls) in enumerate(sorted(groups.items()), start=1):
        wins = [item for item in pnls if item > 0]
        losses = [item for item in pnls if item < 0]
        net_pnl = sum(pnls, Decimal("0"))
        win_rate = Decimal(len(wins)) * Decimal("100") / Decimal(len(pnls)) if pnls else Decimal("0")
        average_win = sum(wins, Decimal("0")) / Decimal(len(wins)) if wins else None
        average_loss = sum(losses, Decimal("0")) / Decimal(len(losses)) if losses else None
        ratio = average_win / abs(average_loss) if average_win is not None and average_loss not in {None, Decimal("0")} else None
        rows.append(
            (
                f"performance:{index}",
                (
                    strategy,
                    symbol,
                    direction,
                    str(len(pnls)),
                    f"{win_rate:.2f}%",
                    _format_optional_decimal(net_pnl, signed=True),
                    "-" if ratio is None else f"{ratio:.2f}",
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
        strategy_library_opener,
        pool_creator,
        task_adder,
        task_starter,
        task_canceller,
        replay_opener,
        ledger_provider,
        summary_provider,
        default_api_name: str = "",
    ) -> None:
        self._snapshot_provider = snapshot_provider
        self._strategy_library_opener = strategy_library_opener
        self._pool_creator = pool_creator
        self._task_adder = task_adder
        self._task_starter = task_starter
        self._task_canceller = task_canceller
        self._replay_opener = replay_opener
        self._ledger_provider = ledger_provider
        self._summary_provider = summary_provider
        self._snapshot = None
        self._selected_pool_id = ""
        self.pool_name_var = StringVar(value="半自动主操盘")
        self.api_name_var = StringVar(value=default_api_name)
        self.initial_capital_var = StringVar(value="1000")
        self.mode_var = StringVar(value="等待一单")
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
            values=("等待一单", "单次判断"),
            state="readonly",
            width=14,
        ).pack(side="left", padx=(4, 10))
        ttk.Button(action_row, text="从策略库添加", command=self._open_strategy_library).pack(side="left")
        ttk.Button(action_row, text="启动选中任务", command=self._start_selected_task).pack(side="left", padx=(8, 0))
        ttk.Button(action_row, text="取消选中任务", command=self._cancel_selected_task).pack(side="left", padx=(8, 0))
        ttk.Button(action_row, text="复盘选中任务币种", command=self._open_replay).pack(side="left", padx=(8, 0))
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

    def _open_strategy_library(self) -> None:
        if not self._selected_pool_id:
            messagebox.showinfo("提示", "请先选择操盘组合。", parent=self.window)
            return
        mode = {"等待一单": "wait_one", "单次判断": "evaluate_once"}[self.mode_var.get()]
        self._strategy_library_opener(self._selected_pool_id, mode)

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

    def _open_replay(self) -> None:
        task_id = self._selected_task_id()
        task = next((item for item in self._snapshot.tasks if item.task_id == task_id), None) if self._snapshot else None
        if task is None:
            messagebox.showinfo("提示", "请先选择一个任务，以确定复盘币种和周期。", parent=self.window)
            return
        try:
            self._replay_opener(task.pool_id, task.symbol, task.bar)
        except Exception as exc:
            messagebox.showerror("打开复盘失败", str(exc), parent=self.window)

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
        window.geometry("1280x760")
        frame = ttk.Frame(window, padding=12)
        frame.pack(fill="both", expand=True)
        frame.columnconfigure(0, weight=1)
        frame.rowconfigure(1, weight=1)
        performance_frame = ttk.LabelFrame(frame, text="按策略 / 币种 / 方向统计", padding=6)
        performance_frame.grid(row=0, column=0, sticky="ew", pady=(0, 10))
        performance_columns = ("strategy", "symbol", "direction", "count", "win_rate", "pnl", "ratio")
        performance_tree = ttk.Treeview(performance_frame, columns=performance_columns, show="headings", height=5)
        performance_labels = ("策略", "币种", "方向", "已结算", "胜率", "净盈亏", "盈亏比")
        for key, label in zip(performance_columns, performance_labels, strict=True):
            performance_tree.heading(key, text=label)
            performance_tree.column(key, width=140 if key == "strategy" else 105, anchor="center")
        performance_tree.pack(fill="x", expand=True)
        for row_id, values in build_semi_auto_pool_performance_rows(self._selected_pool_id, self._ledger_provider()):
            performance_tree.insert("", END, iid=row_id, values=values)

        ledger_frame = ttk.LabelFrame(frame, text="连续交易总账本", padding=6)
        ledger_frame.grid(row=1, column=0, sticky="nsew")
        ledger_frame.columnconfigure(0, weight=1)
        ledger_frame.rowconfigure(0, weight=1)
        columns = ("closed", "strategy", "symbol", "direction", "opened", "entry", "exit", "pnl", "reason")
        tree = ttk.Treeview(ledger_frame, columns=columns, show="headings")
        labels = ("平仓时间", "策略", "币种", "方向", "开仓时间", "开仓价", "平仓价", "净盈亏", "平仓原因")
        for key, label in zip(columns, labels, strict=True):
            tree.heading(key, text=label)
            tree.column(key, width=125, anchor="center")
        tree.grid(row=0, column=0, sticky="nsew")
        for row_id, values in build_semi_auto_pool_ledger_rows(self._selected_pool_id, self._ledger_provider()):
            tree.insert("", END, iid=row_id, values=values)
