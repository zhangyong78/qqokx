from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from tkinter import BooleanVar, END, StringVar, Text, Toplevel
from tkinter import messagebox, ttk
from typing import Callable

from okx_quant.persistence import signal_observer_templates_file_path
from okx_quant.window_layout import apply_adaptive_window_geometry, apply_window_icon


Logger = Callable[[str], None]
CurrentTemplateFactory = Callable[[], object]
TemplateSerializer = Callable[[object], dict[str, object]]
TemplateDeserializer = Callable[[dict[str, object]], object | None]
TemplateCloner = Callable[[object, str], object]
TemplateLauncher = Callable[[object, str], str]
SessionProvider = Callable[[], list[dict[str, str]]]
SessionStopper = Callable[[list[str]], None]

DEFAULT_SIGNAL_OBSERVER_SYMBOLS: tuple[str, ...] = (
    "BTC-USDT-SWAP",
    "ETH-USDT-SWAP",
    "SOL-USDT-SWAP",
    "BNB-USDT-SWAP",
    "DOGE-USDT-SWAP",
)


@dataclass
class _ObserverDraft:
    draft_id: str
    template_payload: dict[str, object]
    created_at: datetime
    updated_at: datetime


def _parse_time(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


class SignalMonitorWindow:
    def __init__(
        self,
        parent,
        *,
        logger: Logger,
        current_template_factory: CurrentTemplateFactory,
        template_serializer: TemplateSerializer,
        template_deserializer: TemplateDeserializer,
        template_symbol_cloner: TemplateCloner,
        template_launcher: TemplateLauncher,
        session_provider: SessionProvider,
        session_stopper: SessionStopper,
    ) -> None:
        self._logger = logger
        self._current_template_factory = current_template_factory
        self._template_serializer = template_serializer
        self._template_deserializer = template_deserializer
        self._template_symbol_cloner = template_symbol_cloner
        self._template_launcher = template_launcher
        self._session_provider = session_provider
        self._session_stopper = session_stopper
        self._drafts: list[_ObserverDraft] = []
        self._draft_counter = 0
        self._refresh_job: str | None = None

        self._symbol_vars: dict[str, BooleanVar] = {
            symbol: BooleanVar(value=symbol in {"BTC-USDT-SWAP", "ETH-USDT-SWAP"})
            for symbol in DEFAULT_SIGNAL_OBSERVER_SYMBOLS
        }
        self._custom_symbols = StringVar(value="")
        self._status_text = StringVar(value="草稿 0 条 | 运行中 0 条")

        self.window = Toplevel(parent)
        self.window.title("信号观察台")
        apply_window_icon(self.window)
        apply_adaptive_window_geometry(
            self.window,
            width_ratio=0.78,
            height_ratio=0.78,
            min_width=1160,
            min_height=760,
            max_width=1700,
            max_height=1100,
        )
        self.window.protocol("WM_DELETE_WINDOW", self._on_close)

        self.draft_tree: ttk.Treeview | None = None
        self.session_tree: ttk.Treeview | None = None
        self.log_text: Text | None = None

        self._build_layout()
        self._load_drafts()
        self._refresh_views()
        self._schedule_refresh()

    def show(self) -> None:
        if not self.window.winfo_exists():
            return
        self.window.deiconify()
        self.window.lift()
        self.window.focus_force()
        self._refresh_views()

    def destroy(self) -> None:
        if self._refresh_job is not None:
            try:
                self.window.after_cancel(self._refresh_job)
            except Exception:
                pass
            self._refresh_job = None
        if self.window.winfo_exists():
            self.window.destroy()

    def _on_close(self) -> None:
        self.window.withdraw()

    def _build_layout(self) -> None:
        self.window.columnconfigure(0, weight=1)
        self.window.rowconfigure(1, weight=1)
        self.window.rowconfigure(2, weight=0)

        header = ttk.Frame(self.window, padding=(16, 14, 16, 8))
        header.grid(row=0, column=0, sticky="ew")
        header.columnconfigure(0, weight=1)
        ttk.Label(header, text="信号观察台", font=("Microsoft YaHei UI", 18, "bold")).grid(
            row=0,
            column=0,
            sticky="w",
        )
        ttk.Label(header, textvariable=self._status_text).grid(row=0, column=1, sticky="e")
        ttk.Label(
            header,
            text="统一管理 signal_only 策略草稿，支持多币种批量启动，只观察不下单。",
            foreground="#556070",
        ).grid(row=1, column=0, columnspan=2, sticky="w", pady=(6, 0))

        body = ttk.Frame(self.window, padding=(16, 0, 16, 12))
        body.grid(row=1, column=0, sticky="nsew")
        body.columnconfigure(0, weight=3)
        body.columnconfigure(1, weight=2)
        body.rowconfigure(0, weight=1)
        body.rowconfigure(1, weight=1)

        left = ttk.LabelFrame(body, text="观察草稿", padding=12)
        left.grid(row=0, column=0, rowspan=2, sticky="nsew", padx=(0, 12))
        left.columnconfigure(0, weight=1)
        left.rowconfigure(2, weight=1)

        toolbar = ttk.Frame(left)
        toolbar.grid(row=0, column=0, sticky="ew")
        ttk.Button(toolbar, text="加入当前参数", command=self.add_current_template).grid(row=0, column=0)
        ttk.Button(toolbar, text="复制到勾选币种", command=self.clone_selected_to_symbols).grid(row=0, column=1, padx=(8, 0))
        ttk.Button(toolbar, text="启动选中草稿", command=self.start_selected_drafts).grid(row=0, column=2, padx=(8, 0))
        ttk.Button(toolbar, text="启动全部草稿", command=self.start_all_drafts).grid(row=0, column=3, padx=(8, 0))
        ttk.Button(toolbar, text="删除选中草稿", command=self.delete_selected_drafts).grid(row=0, column=4, padx=(8, 0))

        symbol_frame = ttk.LabelFrame(left, text="默认批量币种", padding=10)
        symbol_frame.grid(row=1, column=0, sticky="ew", pady=(10, 10))
        for idx, symbol in enumerate(DEFAULT_SIGNAL_OBSERVER_SYMBOLS):
            ttk.Checkbutton(symbol_frame, text=symbol, variable=self._symbol_vars[symbol]).grid(
                row=idx // 3,
                column=idx % 3,
                sticky="w",
                padx=(0, 14),
                pady=(0, 6),
            )
        ttk.Label(symbol_frame, text="额外币种（逗号分隔）").grid(row=2, column=0, sticky="w", pady=(4, 0))
        ttk.Entry(symbol_frame, textvariable=self._custom_symbols).grid(row=2, column=1, columnspan=2, sticky="ew", pady=(4, 0))
        symbol_frame.columnconfigure(1, weight=1)

        self.draft_tree = ttk.Treeview(
            left,
            columns=("draft", "strategy", "symbol", "api", "mode", "updated"),
            show="headings",
            selectmode="extended",
            height=18,
        )
        self.draft_tree.grid(row=2, column=0, sticky="nsew")
        for column, text, width, anchor in (
            ("draft", "草稿", 90, "center"),
            ("strategy", "策略", 180, "w"),
            ("symbol", "标的", 160, "w"),
            ("api", "API", 90, "center"),
            ("mode", "模式", 90, "center"),
            ("updated", "更新时间", 150, "center"),
        ):
            self.draft_tree.heading(column, text=text)
            self.draft_tree.column(column, width=width, anchor=anchor)
        draft_scroll = ttk.Scrollbar(left, orient="vertical", command=self.draft_tree.yview)
        draft_scroll.grid(row=2, column=1, sticky="ns")
        self.draft_tree.configure(yscrollcommand=draft_scroll.set)

        upper_right = ttk.LabelFrame(body, text="运行中的 signal_only 会话", padding=12)
        upper_right.grid(row=0, column=1, sticky="nsew")
        upper_right.columnconfigure(0, weight=1)
        upper_right.rowconfigure(1, weight=1)
        run_toolbar = ttk.Frame(upper_right)
        run_toolbar.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        ttk.Button(run_toolbar, text="刷新", command=self._refresh_views).grid(row=0, column=0)
        ttk.Button(run_toolbar, text="停止选中会话", command=self.stop_selected_sessions).grid(row=0, column=1, padx=(8, 0))
        self.session_tree = ttk.Treeview(
            upper_right,
            columns=("session", "strategy", "symbol", "api", "status", "last"),
            show="headings",
            selectmode="extended",
            height=14,
        )
        self.session_tree.grid(row=1, column=0, sticky="nsew")
        for column, text, width, anchor in (
            ("session", "会话", 80, "center"),
            ("strategy", "策略", 150, "w"),
            ("symbol", "标的", 150, "w"),
            ("api", "API", 90, "center"),
            ("status", "状态", 100, "center"),
            ("last", "最近消息", 280, "w"),
        ):
            self.session_tree.heading(column, text=text)
            self.session_tree.column(column, width=width, anchor=anchor)
        session_scroll = ttk.Scrollbar(upper_right, orient="vertical", command=self.session_tree.yview)
        session_scroll.grid(row=1, column=1, sticky="ns")
        self.session_tree.configure(yscrollcommand=session_scroll.set)

        lower_right = ttk.LabelFrame(body, text="使用说明", padding=12)
        lower_right.grid(row=1, column=1, sticky="nsew", pady=(12, 0))
        lower_right.columnconfigure(0, weight=1)
        ttk.Label(
            lower_right,
            justify="left",
            text="\n".join(
                [
                    "1. 先在主界面把参数调好，再点“加入当前参数”保存成观察草稿。",
                    "2. 这里启动的都是 signal_only，会沿用策略本体逻辑，不再维护独立信号算法。",
                    "3. 可以把同一草稿复制到多个币种，适合一键启动邮件提醒。",
                    "4. 真正的额度托管与审批，会放到独立的交易员管理台。",
                ]
            ),
        ).grid(row=0, column=0, sticky="w")

        log_frame = ttk.LabelFrame(self.window, text="操作日志", padding=12)
        log_frame.grid(row=2, column=0, sticky="nsew", padx=16, pady=(0, 16))
        log_frame.columnconfigure(0, weight=1)
        log_frame.rowconfigure(0, weight=1)
        self.log_text = Text(log_frame, height=8, wrap="word", font=("Consolas", 10), relief="flat")
        self.log_text.grid(row=0, column=0, sticky="nsew")

    def _store_path(self) -> Path:
        return signal_observer_templates_file_path()

    def _load_drafts(self) -> None:
        path = self._store_path()
        self._drafts = []
        self._draft_counter = 0
        if not path.exists():
            return
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            self._append_log(f"读取草稿失败：{exc}")
            return
        if not isinstance(payload, list):
            return
        for item in payload:
            if not isinstance(item, dict):
                continue
            draft_id = str(item.get("draft_id") or "").strip()
            template_payload = item.get("template_payload")
            if not draft_id or not isinstance(template_payload, dict):
                continue
            created_at = _parse_time(item.get("created_at")) or datetime.now()
            updated_at = _parse_time(item.get("updated_at")) or created_at
            self._drafts.append(
                _ObserverDraft(
                    draft_id=draft_id,
                    template_payload=template_payload,
                    created_at=created_at,
                    updated_at=updated_at,
                )
            )
            digits = "".join(ch for ch in draft_id if ch.isdigit())
            if digits:
                self._draft_counter = max(self._draft_counter, int(digits))

    def _save_drafts(self) -> None:
        payload = [
            {
                "draft_id": item.draft_id,
                "template_payload": item.template_payload,
                "created_at": item.created_at.isoformat(timespec="seconds"),
                "updated_at": item.updated_at.isoformat(timespec="seconds"),
            }
            for item in self._drafts
        ]
        path = self._store_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def _next_draft_id(self) -> str:
        self._draft_counter += 1
        return f"D{self._draft_counter:03d}"

    def _selected_symbols(self) -> list[str]:
        selected = [symbol for symbol, var in self._symbol_vars.items() if var.get()]
        custom = [item.strip().upper() for item in self._custom_symbols.get().split(",") if item.strip()]
        return list(dict.fromkeys(selected + custom))

    def _selected_draft_ids(self) -> list[str]:
        if self.draft_tree is None:
            return []
        return [str(item) for item in self.draft_tree.selection()]

    def _selected_drafts(self) -> list[_ObserverDraft]:
        selected_ids = set(self._selected_draft_ids())
        return [item for item in self._drafts if item.draft_id in selected_ids]

    def _selected_session_ids(self) -> list[str]:
        if self.session_tree is None:
            return []
        return [str(item) for item in self.session_tree.selection()]

    def add_current_template(self) -> None:
        try:
            template = self._current_template_factory()
            payload = self._template_serializer(template)
        except Exception as exc:
            messagebox.showerror("加入失败", str(exc), parent=self.window)
            return
        now = datetime.now()
        draft = _ObserverDraft(
            draft_id=self._next_draft_id(),
            template_payload=payload,
            created_at=now,
            updated_at=now,
        )
        self._drafts.append(draft)
        self._save_drafts()
        self._refresh_views()
        self._append_log(f"[{draft.draft_id}] 已加入当前 signal_only 草稿。")

    def clone_selected_to_symbols(self) -> None:
        drafts = self._selected_drafts()
        if not drafts:
            messagebox.showinfo("提示", "请先选中一条草稿。", parent=self.window)
            return
        symbols = self._selected_symbols()
        if not symbols:
            messagebox.showinfo("提示", "请先勾选至少一个币种。", parent=self.window)
            return
        created = 0
        now = datetime.now()
        for draft in drafts:
            template = self._template_deserializer(draft.template_payload)
            if template is None:
                continue
            for symbol in symbols:
                cloned = self._template_symbol_cloner(template, symbol)
                self._drafts.append(
                    _ObserverDraft(
                        draft_id=self._next_draft_id(),
                        template_payload=self._template_serializer(cloned),
                        created_at=now,
                        updated_at=now,
                    )
                )
                created += 1
        if created <= 0:
            messagebox.showwarning("提示", "没有生成新的草稿。", parent=self.window)
            return
        self._save_drafts()
        self._refresh_views()
        self._append_log(f"已复制 {created} 条草稿到批量币种。")

    def delete_selected_drafts(self) -> None:
        selected_ids = set(self._selected_draft_ids())
        if not selected_ids:
            messagebox.showinfo("提示", "请先选中要删除的草稿。", parent=self.window)
            return
        self._drafts = [item for item in self._drafts if item.draft_id not in selected_ids]
        self._save_drafts()
        self._refresh_views()
        self._append_log(f"已删除 {len(selected_ids)} 条观察草稿。")

    def _start_drafts(self, drafts: list[_ObserverDraft], source_label: str) -> None:
        if not drafts:
            messagebox.showinfo("提示", "没有可启动的草稿。", parent=self.window)
            return
        started = 0
        failures: list[str] = []
        for draft in drafts:
            template = self._template_deserializer(draft.template_payload)
            if template is None:
                failures.append(f"{draft.draft_id}: 草稿已损坏")
                continue
            try:
                session_id = self._template_launcher(template, source_label)
            except Exception as exc:
                failures.append(f"{draft.draft_id}: {exc}")
            else:
                started += 1
                self._append_log(f"[{draft.draft_id}] 已启动 signal_only 会话：{session_id}")
        self._refresh_views()
        if failures:
            messagebox.showwarning("部分启动失败", "\n".join(failures[:12]), parent=self.window)
        if started:
            self._append_log(f"本次共启动 {started} 条草稿。")

    def start_selected_drafts(self) -> None:
        self._start_drafts(self._selected_drafts(), "selected")

    def start_all_drafts(self) -> None:
        self._start_drafts(list(self._drafts), "all")

    def stop_selected_sessions(self) -> None:
        session_ids = self._selected_session_ids()
        if not session_ids:
            messagebox.showinfo("提示", "请先选中要停止的会话。", parent=self.window)
            return
        try:
            self._session_stopper(session_ids)
        except Exception as exc:
            messagebox.showerror("停止失败", str(exc), parent=self.window)
            return
        self._append_log(f"已请求停止 {len(session_ids)} 个 signal_only 会话。")
        self._refresh_views()

    def _refresh_draft_tree(self) -> None:
        if self.draft_tree is None:
            return
        selected = set(self.draft_tree.selection())
        for item_id in self.draft_tree.get_children():
            self.draft_tree.delete(item_id)
        for draft in self._drafts:
            payload = draft.template_payload
            self.draft_tree.insert(
                "",
                END,
                iid=draft.draft_id,
                values=(
                    draft.draft_id,
                    str(payload.get("strategy_name") or payload.get("strategy_id") or "-"),
                    str(payload.get("symbol") or "-"),
                    str(payload.get("api_name") or "-"),
                    str(payload.get("run_mode_label") or "只发邮件"),
                    draft.updated_at.strftime("%Y-%m-%d %H:%M:%S"),
                ),
            )
        for item_id in selected:
            if self.draft_tree.exists(item_id):
                self.draft_tree.selection_add(item_id)

    def _refresh_session_tree(self) -> int:
        if self.session_tree is None:
            return 0
        selected = set(self.session_tree.selection())
        rows = self._session_provider()
        for item_id in self.session_tree.get_children():
            self.session_tree.delete(item_id)
        for row in rows:
            session_id = str(row.get("session_id") or "")
            if not session_id:
                continue
            self.session_tree.insert(
                "",
                END,
                iid=session_id,
                values=(
                    session_id,
                    row.get("strategy_name", "-"),
                    row.get("symbol", "-"),
                    row.get("api_name", "-"),
                    row.get("status", "-"),
                    row.get("last_message", "-"),
                ),
            )
        for item_id in selected:
            if self.session_tree.exists(item_id):
                self.session_tree.selection_add(item_id)
        return len(rows)

    def _refresh_views(self) -> None:
        self._refresh_draft_tree()
        session_count = self._refresh_session_tree()
        self._status_text.set(f"草稿 {len(self._drafts)} 条 | 运行中 {session_count} 条")

    def _append_log(self, message: str) -> None:
        timestamped = f"[信号观察台] {message}"
        self._logger(timestamped)
        if self.log_text is not None:
            self.log_text.insert(END, f"{datetime.now():%H:%M:%S} {message}\n")
            self.log_text.see(END)

    def _schedule_refresh(self) -> None:
        if not self.window.winfo_exists():
            return
        self._refresh_job = self.window.after(2500, self._refresh_tick)

    def _refresh_tick(self) -> None:
        self._refresh_job = None
        if not self.window.winfo_exists():
            return
        self._refresh_views()
        self._schedule_refresh()
