from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from tkinter import END, StringVar, Text, Toplevel, filedialog
from tkinter import messagebox, ttk
from typing import Callable

from okx_quant.journal import (
    JournalEntry,
    JournalExtractionResult,
    build_ai_extraction_prompt,
    create_journal_entry,
    extract_journal_locally,
    parse_ai_extraction_paste,
)
from okx_quant.persistence import load_journal_entries_snapshot, save_journal_entries_snapshot
from okx_quant.window_layout import apply_adaptive_window_geometry, apply_window_icon


Logger = Callable[[str], None]


class JournalWindow:
    def __init__(
        self,
        parent: Toplevel,
        *,
        logger: Logger | None = None,
    ) -> None:
        self._logger = logger or (lambda _message: None)
        self._entries: list[JournalEntry] = []
        self._selected_entry_id = ""
        self._current_extraction: JournalExtractionResult | None = None
        self._attachment_paths: list[str] = []

        self.window = Toplevel(parent)
        self.window.title("行情日记")
        self.window.transient(parent)
        self.window.protocol("WM_DELETE_WINDOW", self.destroy)
        apply_window_icon(self.window)
        apply_adaptive_window_geometry(
            self.window,
            width_ratio=0.82,
            height_ratio=0.82,
            min_width=1180,
            min_height=760,
            max_width=1500,
            max_height=980,
        )

        self.status_text = StringVar(value="行情日记已就绪。")
        self.preview_summary_text = StringVar(value="")
        self.preview_status_text = StringVar(value="未提炼")
        self.preview_symbol_text = StringVar(value="-")
        self.preview_timeframes_text = StringVar(value="-")
        self.preview_bias_text = StringVar(value="-")
        self.preview_entry_text = StringVar(value="-")
        self.preview_invalid_text = StringVar(value="-")
        self.preview_action_text = StringVar(value="-")
        self.preview_position_text = StringVar(value="-")
        self.preview_source_text = StringVar(value="-")
        self.preview_record_type_text = StringVar(value="-")
        self.preview_hypothesis_text = StringVar(value="-")
        self.preview_verification_text = StringVar(value="-")

        self._build_layout()
        self._load_entries(select_latest=False)

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
        ttk.Label(header, text="行情日记", font=("Microsoft YaHei UI", 16, "bold")).grid(row=0, column=0, sticky="w")
        ttk.Label(header, textvariable=self.status_text).grid(row=0, column=1, sticky="e")

        body = ttk.Panedwindow(self.window, orient="horizontal")
        body.grid(row=1, column=0, sticky="nsew", padx=14, pady=(0, 12))

        left = ttk.Frame(body, padding=(0, 0, 8, 0))
        left.columnconfigure(0, weight=1)
        left.rowconfigure(1, weight=1)
        body.add(left, weight=28)

        right = ttk.Frame(body)
        right.columnconfigure(0, weight=1)
        right.rowconfigure(0, weight=1)
        body.add(right, weight=72)

        list_header = ttk.Frame(left)
        list_header.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        list_header.columnconfigure(0, weight=1)
        ttk.Label(list_header, text="已保存日记", font=("Microsoft YaHei UI", 11, "bold")).grid(row=0, column=0, sticky="w")
        ttk.Button(list_header, text="刷新", command=lambda: self._load_entries(select_latest=False)).grid(row=0, column=1, padx=(8, 0))
        ttk.Button(list_header, text="新建", command=self._new_entry).grid(row=0, column=2, padx=(8, 0))

        self.entry_tree = ttk.Treeview(
            left,
            columns=("created_at", "symbol", "bias", "status"),
            show="headings",
            selectmode="browse",
            height=24,
        )
        self.entry_tree.heading("created_at", text="时间")
        self.entry_tree.heading("symbol", text="标的")
        self.entry_tree.heading("bias", text="方向")
        self.entry_tree.heading("status", text="状态")
        self.entry_tree.column("created_at", width=126, anchor="center")
        self.entry_tree.column("symbol", width=120, anchor="w")
        self.entry_tree.column("bias", width=72, anchor="center")
        self.entry_tree.column("status", width=76, anchor="center")
        self.entry_tree.grid(row=1, column=0, sticky="nsew")
        self.entry_tree.bind("<<TreeviewSelect>>", self._on_tree_select)
        left_scroll = ttk.Scrollbar(left, orient="vertical", command=self.entry_tree.yview)
        left_scroll.grid(row=1, column=1, sticky="ns")
        self.entry_tree.configure(yscrollcommand=left_scroll.set)

        editor = ttk.Frame(right)
        editor.grid(row=0, column=0, sticky="nsew")
        editor.columnconfigure(0, weight=1)
        editor.rowconfigure(1, weight=4)
        editor.rowconfigure(3, weight=3)
        editor.rowconfigure(5, weight=3)

        raw_box = ttk.LabelFrame(editor, text="原始随笔", padding=10)
        raw_box.grid(row=0, column=0, sticky="nsew")
        raw_box.columnconfigure(0, weight=1)
        raw_box.rowconfigure(1, weight=1)
        raw_actions = ttk.Frame(raw_box)
        raw_actions.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        ttk.Button(raw_actions, text="本地提炼", command=self._extract_local).grid(row=0, column=0)
        ttk.Button(raw_actions, text="生成 AI 提示词", command=self._copy_ai_prompt).grid(row=0, column=1, padx=(8, 0))
        ttk.Button(raw_actions, text="添加截图", command=self._add_attachment).grid(row=0, column=2, padx=(8, 0))
        ttk.Button(raw_actions, text="保存日记", command=self._save_current_entry).grid(row=0, column=3, padx=(8, 0))
        self.raw_text = Text(raw_box, wrap="word", height=10, font=("Microsoft YaHei UI", 10))
        self.raw_text.grid(row=1, column=0, sticky="nsew")
        raw_scroll = ttk.Scrollbar(raw_box, orient="vertical", command=self.raw_text.yview)
        raw_scroll.grid(row=1, column=1, sticky="ns")
        self.raw_text.configure(yscrollcommand=raw_scroll.set)
        self.attachment_label = ttk.Label(raw_box, text="附件：-")
        self.attachment_label.grid(row=2, column=0, sticky="w", pady=(8, 0))

        ai_box = ttk.LabelFrame(editor, text="AI 提炼粘贴区", padding=10)
        ai_box.grid(row=2, column=0, sticky="nsew", pady=(10, 0))
        ai_box.columnconfigure(0, weight=1)
        ai_box.rowconfigure(1, weight=1)
        ai_actions = ttk.Frame(ai_box)
        ai_actions.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        ttk.Button(ai_actions, text="导入 AI JSON", command=self._import_ai_paste).grid(row=0, column=0)
        ttk.Button(ai_actions, text="清空", command=lambda: self._replace_text(self.ai_text, "")).grid(row=0, column=1, padx=(8, 0))
        self.ai_text = Text(ai_box, wrap="word", height=8, font=("Consolas", 10))
        self.ai_text.grid(row=1, column=0, sticky="nsew")
        ai_scroll = ttk.Scrollbar(ai_box, orient="vertical", command=self.ai_text.yview)
        ai_scroll.grid(row=1, column=1, sticky="ns")
        self.ai_text.configure(yscrollcommand=ai_scroll.set)

        preview_box = ttk.LabelFrame(editor, text="提炼结果", padding=10)
        preview_box.grid(row=4, column=0, sticky="nsew", pady=(10, 0))
        preview_box.columnconfigure(1, weight=1)
        preview_box.rowconfigure(8, weight=1)
        preview_box.rowconfigure(15, weight=1)

        preview_rows = (
            ("状态", self.preview_status_text),
            ("来源", self.preview_source_text),
            ("标的", self.preview_symbol_text),
            ("周期", self.preview_timeframes_text),
            ("方向", self.preview_bias_text),
            ("观察区", self.preview_entry_text),
            ("失效位", self.preview_invalid_text),
            ("动作", self.preview_action_text),
            ("仓位", self.preview_position_text),
        )
        for index, (label, variable) in enumerate(preview_rows):
            ttk.Label(preview_box, text=label).grid(row=index, column=0, sticky="nw", padx=(0, 8), pady=2)
            ttk.Label(preview_box, textvariable=variable, justify="left").grid(row=index, column=1, sticky="nw", pady=2)
        ttk.Label(preview_box, text="记录类型").grid(row=11, column=0, sticky="nw", padx=(0, 8), pady=2)
        ttk.Label(preview_box, textvariable=self.preview_record_type_text, justify="left").grid(row=11, column=1, sticky="nw", pady=2)
        ttk.Label(preview_box, text="核心假设").grid(row=12, column=0, sticky="nw", padx=(0, 8), pady=2)
        ttk.Label(
            preview_box,
            textvariable=self.preview_hypothesis_text,
            justify="left",
            wraplength=680,
        ).grid(row=12, column=1, sticky="nw", pady=2)
        ttk.Label(preview_box, text="验证窗口").grid(row=13, column=0, sticky="nw", padx=(0, 8), pady=2)
        ttk.Label(preview_box, textvariable=self.preview_verification_text, justify="left").grid(row=13, column=1, sticky="nw", pady=2)
        ttk.Label(preview_box, text="摘要").grid(row=9, column=0, sticky="nw", padx=(0, 8), pady=(8, 2))
        ttk.Label(
            preview_box,
            textvariable=self.preview_summary_text,
            justify="left",
            wraplength=680,
        ).grid(row=9, column=1, sticky="nw", pady=(8, 2))

        ttk.Label(preview_box, text="待确认").grid(row=10, column=0, sticky="nw", padx=(0, 8), pady=(8, 2))
        self.review_text = Text(preview_box, height=5, wrap="word", font=("Microsoft YaHei UI", 10))
        self.review_text.grid(row=10, column=1, sticky="nsew", pady=(8, 2))
        review_scroll = ttk.Scrollbar(preview_box, orient="vertical", command=self.review_text.yview)
        review_scroll.grid(row=10, column=2, sticky="ns", pady=(8, 2))
        self.review_text.configure(yscrollcommand=review_scroll.set)

        json_actions = ttk.Frame(preview_box)
        json_actions.grid(row=14, column=1, sticky="ew", pady=(8, 2))
        ttk.Button(json_actions, text="澶嶅埗瀹屾暣 JSON", command=self._copy_structured_json).grid(row=0, column=0)
        self.structured_json_text = Text(preview_box, height=7, wrap="none", font=("Consolas", 9))
        self.structured_json_text.grid(row=15, column=1, sticky="nsew", pady=(2, 0))
        structured_scroll = ttk.Scrollbar(preview_box, orient="vertical", command=self.structured_json_text.yview)
        structured_scroll.grid(row=15, column=2, sticky="ns", pady=(2, 0))
        self.structured_json_text.configure(yscrollcommand=structured_scroll.set)

    def _load_entries(self, *, select_latest: bool) -> None:
        snapshot = load_journal_entries_snapshot()
        self._entries = [JournalEntry.from_dict(item) for item in snapshot.get("entries", []) or [] if isinstance(item, dict)]
        for item_id in self.entry_tree.get_children():
            self.entry_tree.delete(item_id)
        for entry in self._entries:
            extraction = entry.extraction
            symbol = extraction.inst_id if extraction and extraction.inst_id else (extraction.symbol if extraction else "-")
            bias = _bias_label(extraction.bias if extraction else "unknown")
            created_label = _format_local_time(entry.created_at)
            self.entry_tree.insert("", END, iid=entry.entry_id, values=(created_label, symbol or "-", bias, _status_label(entry.status)))
        if select_latest and self._entries:
            self.entry_tree.selection_set(self._entries[0].entry_id)
            self._load_entry_into_form(self._entries[0])
        self.status_text.set(f"已加载 {len(self._entries)} 条日记。")

    def _new_entry(self) -> None:
        self._selected_entry_id = ""
        self._current_extraction = None
        self._attachment_paths = []
        self._replace_text(self.raw_text, "")
        self._replace_text(self.ai_text, "")
        self._replace_text(self.review_text, "")
        self._set_preview(None)
        self.attachment_label.configure(text="附件：-")
        self.status_text.set("已新建空白日记。")

    def _on_tree_select(self, _event=None) -> None:
        selected = self.entry_tree.selection()
        if not selected:
            return
        selected_id = selected[0]
        entry = next((item for item in self._entries if item.entry_id == selected_id), None)
        if entry is None:
            return
        self._load_entry_into_form(entry)

    def _load_entry_into_form(self, entry: JournalEntry) -> None:
        self._selected_entry_id = entry.entry_id
        self._current_extraction = entry.extraction
        self._attachment_paths = list(entry.attachments)
        self._replace_text(self.raw_text, entry.raw_text)
        self._replace_text(self.ai_text, "")
        self._set_preview(entry.extraction)
        self.attachment_label.configure(text=_format_attachment_text(self._attachment_paths))
        self.status_text.set(f"已载入日记：{_format_local_time(entry.created_at)}")

    def _extract_local(self) -> None:
        raw_text = self._current_raw_text()
        if not raw_text.strip():
            messagebox.showinfo("提示", "请先输入行情随笔。", parent=self.window)
            return
        extraction = extract_journal_locally(raw_text)
        self._current_extraction = extraction
        self._set_preview(extraction)
        self.status_text.set("已完成本地提炼。")
        self._logger(f"[行情日记] 本地提炼完成 | {extraction.inst_id or extraction.symbol or '-'}")

    def _copy_ai_prompt(self) -> None:
        raw_text = self._current_raw_text()
        if not raw_text.strip():
            messagebox.showinfo("提示", "请先输入行情随笔。", parent=self.window)
            return
        prompt = build_ai_extraction_prompt(raw_text)
        self.window.clipboard_clear()
        self.window.clipboard_append(prompt)
        self.status_text.set("AI 提示词已复制到剪贴板。")

    def _copy_structured_json(self) -> None:
        content = self.structured_json_text.get("1.0", END).strip()
        if not content:
            messagebox.showinfo("Info", "No structured JSON to copy yet.", parent=self.window)
            return
        self.window.clipboard_clear()
        self.window.clipboard_append(content)
        self.status_text.set("Structured JSON copied to clipboard.")

    def _import_ai_paste(self) -> None:
        content = self.ai_text.get("1.0", END).strip()
        if not content:
            messagebox.showinfo("提示", "请先粘贴 AI 输出的 JSON。", parent=self.window)
            return
        try:
            extraction = parse_ai_extraction_paste(content)
        except Exception as exc:
            messagebox.showerror("导入失败", f"AI 输出解析失败：{exc}", parent=self.window)
            return
        self._current_extraction = extraction
        self._set_preview(extraction)
        self.status_text.set("已导入 AI 提炼结果。")
        self._logger(f"[行情日记] 已导入 AI 提炼 | {extraction.inst_id or extraction.symbol or '-'}")

    def _save_current_entry(self) -> None:
        raw_text = self._current_raw_text()
        if not raw_text.strip():
            messagebox.showinfo("提示", "请先输入行情随笔。", parent=self.window)
            return
        now = datetime.now(timezone.utc)
        if self._selected_entry_id:
            existing = next((item for item in self._entries if item.entry_id == self._selected_entry_id), None)
            if existing is not None:
                entry = JournalEntry(
                    entry_id=existing.entry_id,
                    raw_text=raw_text,
                    created_at=existing.created_at,
                    updated_at=now,
                    attachments=tuple(self._attachment_paths),
                    status="review" if self._current_extraction else existing.status,
                    extraction=self._current_extraction,
                    notes="",
                )
                self._entries = [entry if item.entry_id == existing.entry_id else item for item in self._entries]
            else:
                entry = create_journal_entry(
                    raw_text,
                    attachments=tuple(self._attachment_paths),
                    extraction=self._current_extraction,
                )
                self._entries.insert(0, entry)
        else:
            entry = create_journal_entry(
                raw_text,
                attachments=tuple(self._attachment_paths),
                extraction=self._current_extraction,
            )
            self._entries.insert(0, entry)
            self._selected_entry_id = entry.entry_id
        save_journal_entries_snapshot([item.to_dict() for item in self._entries])
        self._load_entries(select_latest=False)
        if self._selected_entry_id:
            self.entry_tree.selection_set(self._selected_entry_id)
            self.entry_tree.see(self._selected_entry_id)
        self.status_text.set("日记已保存。")
        self._logger("[行情日记] 日记已保存")

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
        self.attachment_label.configure(text=_format_attachment_text(self._attachment_paths))
        self.status_text.set(f"已添加 {len(selected)} 个附件。")

    def _set_preview(self, extraction: JournalExtractionResult | None) -> None:
        if extraction is None:
            self.preview_status_text.set("未提炼")
            self.preview_source_text.set("-")
            self.preview_symbol_text.set("-")
            self.preview_timeframes_text.set("-")
            self.preview_bias_text.set("-")
            self.preview_entry_text.set("-")
            self.preview_invalid_text.set("-")
            self.preview_action_text.set("-")
            self.preview_position_text.set("-")
            self.preview_record_type_text.set("-")
            self.preview_hypothesis_text.set("-")
            self.preview_verification_text.set("-")
            self.preview_summary_text.set("")
            self._replace_text(self.review_text, "")
            self._replace_text(self.structured_json_text, "")
            return
        self.preview_status_text.set("待确认" if extraction.needs_review else "已结构化")
        self.preview_source_text.set(_source_label(extraction.source))
        self.preview_symbol_text.set(extraction.inst_id or extraction.symbol or "-")
        self.preview_timeframes_text.set(" / ".join(extraction.timeframes) or "-")
        self.preview_bias_text.set(_bias_label(extraction.bias))
        self.preview_entry_text.set(
            extraction.entry_zone_text
            or (f"{extraction.entry_zone_price}" if extraction.entry_zone_price is not None else "-")
        )
        self.preview_invalid_text.set(
            extraction.invalidation_text
            or (f"{extraction.invalidation_price}" if extraction.invalidation_price is not None else "-")
        )
        self.preview_action_text.set(_action_label(extraction.planned_action))
        self.preview_position_text.set(extraction.position_size_text or "-")
        research_payload = extraction.raw_payload if isinstance(extraction.raw_payload, dict) else {}
        self.preview_record_type_text.set(_record_type_label(str(research_payload.get("record_type", "") or "")))
        self.preview_hypothesis_text.set(_hypothesis_statement(research_payload) or "-")
        self.preview_verification_text.set(_verification_windows(research_payload) or "-")
        self.preview_summary_text.set(extraction.summary or "")
        self._replace_text(self.review_text, "\n".join(f"- {item}" for item in extraction.review_questions))
        structured_json = json.dumps(extraction.raw_payload, ensure_ascii=False, indent=2) if research_payload else ""
        self._replace_text(self.structured_json_text, structured_json)

    def _replace_text(self, widget: Text, content: str) -> None:
        widget.delete("1.0", END)
        widget.insert("1.0", content)

    def _current_raw_text(self) -> str:
        return self.raw_text.get("1.0", END).strip()


def _format_local_time(value: datetime) -> str:
    target = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return target.astimezone().strftime("%Y-%m-%d %H:%M")


def _bias_label(value: str) -> str:
    return {
        "long": "偏多",
        "short": "偏空",
        "neutral": "震荡/观望",
        "unknown": "待确认",
    }.get(value, "待确认")


def _status_label(value: str) -> str:
    return {
        "draft": "草稿",
        "review": "待确认",
        "confirmed": "已确认",
        "monitoring": "监控中",
        "archived": "已归档",
    }.get(value, value or "-")


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


def _record_type_label(value: str) -> str:
    return {
        "trade_plan": "交易计划",
        "market_view": "市场观点",
        "research_hypothesis": "研究假设",
        "post_trade_review": "事后复盘",
        "unknown": "待确认",
    }.get(value, value or "-")


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
