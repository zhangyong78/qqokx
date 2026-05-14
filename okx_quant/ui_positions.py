from __future__ import annotations

import time
import traceback
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from okx_quant.okx_client import OkxPosition, OkxTradeOrderItem


class UiPositionsMixin:
    def _schedule_protection_window_refresh(self) -> None:
        try:
            if self.root.winfo_exists():
                self.root.after(0, self._refresh_protection_window_view)
        except Exception:
            return

    def _position_refresh_interval_ms(self) -> int:
        return POSITION_REFRESH_INTERVAL_OPTIONS.get(self.position_refresh_interval_label.get(), 15_000)

    def toggle_position_auto_refresh(self) -> None:
        self.position_auto_refresh_enabled = not self.position_auto_refresh_enabled
        if self.position_auto_refresh_enabled:
            self.position_auto_refresh_button_text.set("暂停自动刷新")
            self._enqueue_log(f"账户持仓已恢复自动刷新，当前间隔：{self.position_refresh_interval_label.get()}")
            self.refresh_positions()
        else:
            self.position_auto_refresh_button_text.set("恢复自动刷新")
            self._enqueue_log("账户持仓已暂停自动刷新。需要更新时可以手动点“刷新”。")
            self._update_position_summary(self._positions_zoom_visible_positions())

    def _on_position_refresh_interval_changed(self, *_: object) -> None:
        visible_positions = self._positions_zoom_visible_positions()
        self._update_position_summary(visible_positions)
        self._enqueue_log(f"账户持仓自动刷新间隔已切换为：{self.position_refresh_interval_label.get()}")

    def _on_position_filter_changed(self, *_: object) -> None:
        self._render_positions_view()

    def reset_position_filters(self) -> None:
        self.positions_zoom_type_filter.set("全部类型")
        self.positions_zoom_keyword.set("")
        self._render_positions_view()

    def _positions_zoom_visible_positions(self) -> list[OkxPosition]:
        return _filter_positions(
            self._latest_positions,
            inst_type=POSITION_TYPE_OPTIONS[self.positions_zoom_type_filter.get()],
            keyword=self.positions_zoom_keyword.get(),
            note_texts=self._current_position_note_text_map(),
        )

    def expand_all_position_groups(self) -> None:
        for item_id in self.position_tree.get_children():
            self.position_tree.item(item_id, open=True)
            for child_id in self.position_tree.get_children(item_id):
                self.position_tree.item(child_id, open=True)

    def collapse_position_groups(self) -> None:
        for item_id in self.position_tree.get_children():
            for child_id in self.position_tree.get_children(item_id):
                self.position_tree.item(child_id, open=False)
            self.position_tree.item(item_id, open=False)

    def copy_selected_position_symbol(self) -> None:
        payload = self._selected_position_payload()
        if payload is None or payload["kind"] != "position":
            messagebox.showinfo("提示", "请先在持仓列表中选中一条具体持仓。")
            return
        position = payload["item"]
        if not isinstance(position, OkxPosition):
            return
        self.root.clipboard_clear()
        self.root.clipboard_append(position.inst_id)
        self._enqueue_log(f"已复制合约代码：{position.inst_id}")

    def _current_position_note_context(self) -> tuple[str, str]:
        profile_name = (self._positions_context_profile_name or self._current_credential_profile()).strip()
        environment = self._positions_effective_environment or ENV_OPTIONS[self.environment_label.get()]
        return profile_name, environment

    def _position_history_note_context(self) -> tuple[str, str]:
        profile_name = (self._position_history_profile_name or self._current_credential_profile()).strip()
        environment = self._position_history_effective_environment or ENV_OPTIONS[self.environment_label.get()]
        return profile_name, environment

    def _current_position_note_text(self, position: OkxPosition) -> str:
        profile_name, environment = self._current_position_note_context()
        record = self._position_current_notes.get(_position_note_current_key(profile_name, environment, position))
        return _normalize_position_note_text(record.get("note", "")) if isinstance(record, dict) else ""

    def _current_position_note_summary(self, position: OkxPosition) -> str:
        return _format_position_note_summary(self._current_position_note_text(position))

    def _position_history_note_text(self, item: OkxPositionHistoryItem) -> str:
        profile_name, environment = self._position_history_note_context()
        record = self._position_history_notes.get(_position_history_note_key(profile_name, environment, item))
        return _normalize_position_note_text(record.get("note", "")) if isinstance(record, dict) else ""

    def _position_history_note_summary(self, item: OkxPositionHistoryItem) -> str:
        return _format_position_note_summary(self._position_history_note_text(item))

    def _current_position_note_text_map(self) -> dict[str, str]:
        return {
            _position_tree_row_id(position): self._current_position_note_text(position)
            for position in self._latest_positions
        }

    def _position_history_note_text_map_by_index(self) -> dict[int, str]:
        return {
            index: self._position_history_note_text(item)
            for index, item in enumerate(self._latest_position_history)
        }

    def _selected_position_history_item(self) -> OkxPositionHistoryItem | None:
        if self._positions_zoom_position_history_tree is None:
            return None
        selection = self._positions_zoom_position_history_tree.selection()
        if not selection:
            return None
        index = _history_tree_index(selection[0], "ph")
        if index is None or index >= len(self._latest_position_history):
            return None
        return self._latest_position_history[index]

    def _sync_position_note_state_for_positions(
        self,
        *,
        profile_name: str,
        environment: str,
        positions: list[OkxPosition],
    ) -> None:
        now_ms = _now_epoch_ms()
        changed = _reconcile_current_position_note_records(
            self._position_current_notes,
            profile_name=profile_name,
            environment=environment,
            positions=positions,
            now_ms=now_ms,
        )
        if (
            self._position_history_profile_name == profile_name
            and self._position_history_effective_environment == environment
            and self._latest_position_history
        ):
            changed = _inherit_position_history_notes(
                self._position_current_notes,
                self._position_history_notes,
                profile_name=profile_name,
                environment=environment,
                position_history=self._latest_position_history,
                now_ms=now_ms,
            ) or changed
            changed = _prune_closed_current_position_notes(
                self._position_current_notes,
                self._position_history_notes,
                profile_name=profile_name,
                environment=environment,
            ) or changed
        if changed:
            self._save_position_notes()

    def _sync_position_note_state_for_history(
        self,
        *,
        profile_name: str,
        environment: str,
        position_history: list[OkxPositionHistoryItem],
    ) -> None:
        now_ms = _now_epoch_ms()
        changed = _inherit_position_history_notes(
            self._position_current_notes,
            self._position_history_notes,
            profile_name=profile_name,
            environment=environment,
            position_history=position_history,
            now_ms=now_ms,
        )
        changed = _prune_closed_current_position_notes(
            self._position_current_notes,
            self._position_history_notes,
            profile_name=profile_name,
            environment=environment,
        ) or changed
        if changed:
            self._save_position_notes()

    def edit_selected_position_note(self) -> None:
        payload = self._selected_position_payload()
        if payload is None or payload.get("kind") != "position":
            messagebox.showinfo("备注", "请先在当前持仓里选中一条具体持仓。", parent=self._positions_zoom_window or self.root)
            return
        position = payload.get("item")
        if not isinstance(position, OkxPosition):
            return
        dialog = PositionNoteEditorDialog(
            self._positions_zoom_window or self.root,
            title="编辑持仓备注",
            prompt=f"为 {position.inst_id} 填写备注。留空后保存会清空当前持仓备注。",
            initial_value=self._current_position_note_text(position),
        )
        if dialog.result_text is None:
            return
        profile_name, environment = self._current_position_note_context()
        record_key = _position_note_current_key(profile_name, environment, position)
        if dialog.result_text:
            previous = self._position_current_notes.get(record_key)
            record = _build_current_position_note_record(
                profile_name=profile_name,
                environment=environment,
                position=position,
                note=dialog.result_text,
                now_ms=_now_epoch_ms(),
                previous=previous,
            )
            if record is not None:
                self._position_current_notes[record_key] = record
                self._save_position_notes()
                self._render_positions_view()
                self._enqueue_log(f"已更新持仓备注：{position.inst_id}")
            return
        if record_key in self._position_current_notes:
            del self._position_current_notes[record_key]
            self._save_position_notes()
            self._render_positions_view()
            self._enqueue_log(f"已清空持仓备注：{position.inst_id}")

    def clear_selected_position_note(self) -> None:
        payload = self._selected_position_payload()
        if payload is None or payload.get("kind") != "position":
            messagebox.showinfo("备注", "请先在当前持仓里选中一条具体持仓。", parent=self._positions_zoom_window or self.root)
            return
        position = payload.get("item")
        if not isinstance(position, OkxPosition):
            return
        profile_name, environment = self._current_position_note_context()
        record_key = _position_note_current_key(profile_name, environment, position)
        if record_key not in self._position_current_notes:
            messagebox.showinfo("备注", "当前持仓还没有备注。", parent=self._positions_zoom_window or self.root)
            return
        del self._position_current_notes[record_key]
        self._save_position_notes()
        self._render_positions_view()
        self._enqueue_log(f"已清空持仓备注：{position.inst_id}")

    def edit_selected_position_history_note(self) -> None:
        item = self._selected_position_history_item()
        if item is None:
            messagebox.showinfo("备注", "请先在历史仓位里选中一条记录。", parent=self._positions_zoom_window or self.root)
            return
        dialog = PositionNoteEditorDialog(
            self._positions_zoom_window or self.root,
            title="编辑历史仓位备注",
            prompt=f"为 {item.inst_id} 的这条历史仓位填写备注。留空后保存会清空历史仓位备注。",
            initial_value=self._position_history_note_text(item),
        )
        if dialog.result_text is None:
            return
        profile_name, environment = self._position_history_note_context()
        record_key = _position_history_note_key(profile_name, environment, item)
        if dialog.result_text:
            previous = self._position_history_notes.get(record_key)
            record = _build_history_position_note_record(
                profile_name=profile_name,
                environment=environment,
                item=item,
                note=dialog.result_text,
                now_ms=_now_epoch_ms(),
                source_current_key=(
                    str(previous.get("source_current_key", ""))
                    if isinstance(previous, dict)
                    else ""
                ),
                previous=previous,
            )
            if record is not None:
                self._position_history_notes[record_key] = record
                self._save_position_notes()
                self._render_positions_zoom_position_history_view()
                self._enqueue_log(f"已更新历史仓位备注：{item.inst_id}")
            return
        if record_key in self._position_history_notes:
            del self._position_history_notes[record_key]
            self._save_position_notes()
            self._render_positions_zoom_position_history_view()
            self._enqueue_log(f"已清空历史仓位备注：{item.inst_id}")

    def _expand_to_screen(self, window: Toplevel, *, margin: int = 20) -> None:
        try:
            apply_fill_window_geometry(window, min_width=1200, min_height=800, margin=margin)
        except Exception:
            return

    def _schedule_positions_zoom_sync(self, delay_ms: int = 10) -> None:
        if self._positions_zoom_sync_job is not None:
            try:
                self.root.after_cancel(self._positions_zoom_sync_job)
            except Exception:
                pass
            self._positions_zoom_sync_job = None
        self._positions_zoom_sync_job = self.root.after(delay_ms, self._sync_positions_zoom_window)

    def open_positions_zoom_window(self) -> None:
        if self._positions_zoom_window is not None and self._positions_zoom_window.winfo_exists():
            self._positions_zoom_window.focus_force()
            self._schedule_positions_zoom_sync()
            self._load_local_history_cache()
            self._refresh_positions_zoom_all()
            return

        window = Toplevel(self.root)
        window.title("账户持仓大窗")
        apply_adaptive_window_geometry(
            window,
            width_ratio=0.84,
            height_ratio=0.82,
            min_width=1280,
            min_height=860,
            max_width=1800,
            max_height=1120,
        )
        self._positions_zoom_window = window
        window.protocol("WM_DELETE_WINDOW", self._close_positions_zoom_window)

        container = ttk.Frame(window, padding=12)
        container.pack(fill="both", expand=True)
        container.columnconfigure(0, weight=1)
        container.rowconfigure(2, weight=3)
        container.rowconfigure(3, weight=1)
        container.rowconfigure(4, weight=2)

        header = ttk.Frame(container)
        header.grid(row=0, column=0, sticky="ew", pady=(0, 10))
        header.columnconfigure(1, weight=1)
        zoom_positions_badge = self._create_refresh_badge(
            header,
            self._positions_refresh_badge_text,
            self._positions_refresh_badges,
        )
        zoom_positions_badge.grid(row=0, column=0, sticky="w", padx=(0, 8))
        ttk.Label(header, textvariable=self._positions_zoom_summary_text).grid(row=0, column=1, sticky="w")
        zoom_actions = ttk.Frame(header)
        zoom_actions.grid(row=0, column=2, sticky="e")
        zoom_api = ttk.Frame(zoom_actions)
        ttk.Label(zoom_api, text="API").grid(row=0, column=0, sticky="w", padx=(0, 4))
        self._positions_zoom_credential_profile_combo = ttk.Combobox(
            zoom_api,
            textvariable=self.api_profile_name,
            values=self._credential_profile_names(),
            state="readonly",
            width=12,
        )
        self._positions_zoom_credential_profile_combo.grid(row=0, column=1, sticky="w")
        self._positions_zoom_credential_profile_combo.bind("<<ComboboxSelected>>", self._on_api_profile_selected)
        self._sync_credential_profile_combo()
        zoom_api.grid(row=0, column=0, sticky="w", padx=(0, 10))
        # 子控件创建顺序决定最终列序：末尾会对 zoom_actions 子控件按顺序重设 column（0,1,2,…）。
        # 布局逻辑：API → 同步与账户 → 列表/详情可见性（便于先定位合约）→ 对选中持仓的手工与程序操作
        # （平仓、备注、接管、停止接管）→ 期权工具 → 列设置 → 关窗。
        ttk.Button(zoom_actions, text="刷新", command=self._refresh_positions_zoom_all).grid(row=0, column=0, padx=(0, 6))
        ttk.Button(zoom_actions, text="账户信息", command=self.open_account_info_window).grid(row=0, column=1, padx=(0, 6))
        ttk.Button(zoom_actions, textvariable=self._positions_zoom_detail_toggle_text, command=self.toggle_positions_zoom_detail).grid(
            row=0, column=2, padx=(0, 6)
        )
        ttk.Button(
            zoom_actions,
            textvariable=self._positions_zoom_history_toggle_text,
            command=self.toggle_positions_zoom_history,
        ).grid(row=0, column=3, padx=(0, 6))
        ttk.Button(zoom_actions, text="平仓选中", command=self.flatten_selected_position).grid(row=0, column=4, padx=(0, 6))
        ttk.Button(zoom_actions, text="编辑备注", command=self.edit_selected_position_note).grid(row=0, column=5, padx=(0, 6))
        ttk.Button(zoom_actions, text="从选中持仓接管", command=self.open_position_takeover_dynamic_stop_dialog).grid(
            row=0, column=6, padx=(0, 6)
        )
        ttk.Button(zoom_actions, text="停止接管", command=self.stop_position_takeover_dynamic_stop).grid(
            row=0, column=7, padx=(0, 6)
        )
        ttk.Button(zoom_actions, text="设置期权保护", command=self.open_position_protection_window).grid(
            row=0, column=8, padx=(0, 6)
        )
        ttk.Button(zoom_actions, text="展期建议", command=self.open_option_roll_window).grid(row=0, column=9, padx=(0, 6))
        ttk.Button(zoom_actions, text="列设置", command=self.open_positions_zoom_column_window).grid(
            row=0, column=10, padx=(0, 6)
        )
        ttk.Button(zoom_actions, text="关闭", command=self._close_positions_zoom_window).grid(row=0, column=11, padx=(0, 0))
        for column_index, child in enumerate(zoom_actions.winfo_children()):
            child.grid_configure(column=column_index)

        filter_row = ttk.Frame(container)
        filter_row.grid(row=1, column=0, sticky="ew", pady=(0, 10))
        filter_row.columnconfigure(3, weight=1)
        ttk.Label(filter_row, text="类型").grid(row=0, column=0, sticky="w")
        zoom_position_type_combo = ttk.Combobox(
            filter_row,
            textvariable=self.positions_zoom_type_filter,
            values=list(POSITION_TYPE_OPTIONS.keys()),
            state="readonly",
            width=16,
        )
        zoom_position_type_combo.grid(row=0, column=1, sticky="w", padx=(6, 12))
        zoom_position_type_combo.bind("<<ComboboxSelected>>", self._on_position_filter_changed)
        ttk.Label(filter_row, text="搜索").grid(row=0, column=2, sticky="w")
        zoom_position_keyword_entry = ttk.Entry(filter_row, textvariable=self.positions_zoom_keyword)
        zoom_position_keyword_entry.grid(row=0, column=3, sticky="ew", padx=(6, 12))
        zoom_position_keyword_entry.bind("<KeyRelease>", self._on_position_filter_changed)
        self._positions_zoom_apply_contract_button = ttk.Button(
            filter_row,
            text="\u5e26\u5165\u5408\u7ea6",
            command=self.apply_selected_option_to_position_search,
        )
        self._positions_zoom_apply_contract_button.grid(row=0, column=4, padx=(0, 6))
        self._positions_zoom_apply_expiry_prefix_button = ttk.Button(
            filter_row,
            text="\u5e26\u5165\u5230\u671f\u524d\u7f00",
            command=self.apply_selected_option_expiry_prefix_to_position_search,
        )
        self._positions_zoom_apply_expiry_prefix_button.grid(row=0, column=5, padx=(0, 6))
        ttk.Button(filter_row, text="应用筛选", command=self._render_positions_view).grid(
            row=0, column=6, padx=(0, 6)
        )
        ttk.Button(filter_row, text="清空筛选", command=self.reset_position_filters).grid(row=0, column=7)
        ttk.Label(
            filter_row,
            textvariable=self._positions_zoom_option_search_hint_text,
            foreground="#6b7280",
        ).grid(row=1, column=2, columnspan=6, sticky="w", pady=(6, 0))

        tree_frame = ttk.Frame(container)
        tree_frame.grid(row=2, column=0, sticky="nsew")
        tree_frame.columnconfigure(0, weight=1)
        tree_frame.rowconfigure(0, weight=1)

        columns = tuple(self.position_tree["columns"])
        zoom_tree = ttk.Treeview(tree_frame, columns=columns, show="tree headings", selectmode="browse")
        self._positions_zoom_tree = zoom_tree
        self._sync_positions_zoom_columns_from_main()
        zoom_tree.grid(row=0, column=0, sticky="nsew")
        zoom_tree.bind("<<TreeviewSelect>>", self._on_positions_zoom_selected)
        zoom_tree.tag_configure("profit", foreground="#13803d")
        zoom_tree.tag_configure("loss", foreground="#c23b3b")
        zoom_tree.tag_configure("group", foreground="#2f3a4a")
        zoom_tree.tag_configure("isolated_mode", background="#fff4e5")
        zoom_tree.tag_configure("cross_mode", background="#f4f8ff")
        zoom_scroll_y = ttk.Scrollbar(tree_frame, orient="vertical", command=zoom_tree.yview)
        zoom_scroll_y.grid(row=0, column=1, sticky="ns")
        zoom_scroll_x = ttk.Scrollbar(tree_frame, orient="horizontal", command=zoom_tree.xview)
        zoom_scroll_x.grid(row=1, column=0, sticky="ew")
        zoom_tree.configure(yscrollcommand=zoom_scroll_y.set, xscrollcommand=zoom_scroll_x.set)
        self._register_positions_zoom_columns("positions", "当前持仓", zoom_tree, columns)

        detail_frame = ttk.LabelFrame(container, text="大窗持仓详情", padding=12)
        detail_frame.grid(row=3, column=0, sticky="nsew")
        self._positions_zoom_detail_frame = detail_frame
        detail_frame.columnconfigure(0, weight=1)
        detail_frame.rowconfigure(0, weight=1)
        self._positions_zoom_detail = Text(
            detail_frame,
            height=10,
            wrap="word",
            font=("Microsoft YaHei UI", 10),
            relief="flat",
        )
        self._positions_zoom_detail.grid(row=0, column=0, sticky="nsew")
        detail_scroll = ttk.Scrollbar(detail_frame, orient="vertical", command=self._positions_zoom_detail.yview)
        detail_scroll.grid(row=0, column=1, sticky="ns")
        self._positions_zoom_detail.configure(yscrollcommand=detail_scroll.set)
        self._set_readonly_text(self._positions_zoom_detail, self.position_detail_text.get())
        self._positions_zoom_summary_text.set("正在打开持仓大窗...")
        self._set_readonly_text(
            self._positions_zoom_detail,
            "大窗已经创建，正在同步当前持仓视图。若你的持仓较多，会在一瞬间完成填充。",
        )
        history_notebook = ttk.Notebook(container)
        history_notebook.grid(row=4, column=0, sticky="nsew", pady=(10, 0))
        self._positions_zoom_notebook = history_notebook

        pending_orders_tab = ttk.Frame(history_notebook, padding=10)
        pending_orders_tab.columnconfigure(0, weight=1)
        pending_orders_tab.rowconfigure(2, weight=1)
        pending_orders_tab.rowconfigure(3, weight=1)
        history_notebook.add(pending_orders_tab, text="当前委托")
        self._build_positions_zoom_pending_orders_tab(pending_orders_tab)

        takeover_tab = ttk.Frame(history_notebook, padding=10)
        takeover_tab.columnconfigure(0, weight=1)
        takeover_tab.rowconfigure(3, weight=1)
        # 使用 add 追加；部分 Windows Tk 上 insert(1, …) 会报 Slave index out of bounds
        history_notebook.add(takeover_tab, text="动态止盈接管")
        self._build_positions_zoom_takeover_tab(takeover_tab)

        order_history_tab = ttk.Frame(history_notebook, padding=10)
        order_history_tab.columnconfigure(0, weight=1)
        order_history_tab.rowconfigure(2, weight=1)
        order_history_tab.rowconfigure(3, weight=1)
        history_notebook.add(order_history_tab, text="历史委托")
        self._build_positions_zoom_order_history_tab(order_history_tab)

        fills_tab = ttk.Frame(history_notebook, padding=10)
        fills_tab.columnconfigure(0, weight=1)
        fills_tab.rowconfigure(1, weight=1)
        fills_tab.rowconfigure(2, weight=1)
        history_notebook.add(fills_tab, text="历史成交")
        self._build_positions_zoom_fills_tab(fills_tab)

        position_history_tab = ttk.Frame(history_notebook, padding=10)
        position_history_tab.columnconfigure(0, weight=1)
        position_history_tab.rowconfigure(2, weight=1)
        position_history_tab.rowconfigure(3, weight=1)
        history_notebook.add(position_history_tab, text="历史仓位")
        self._build_positions_zoom_position_history_tab(position_history_tab)
        history_actions = ttk.Frame(container)
        history_actions.grid(row=5, column=0, sticky="e", pady=(8, 0))
        ttk.Button(history_actions, text="同步历史委托", command=self.refresh_order_history).grid(row=0, column=0, padx=(0, 6))
        ttk.Button(
            history_actions,
            text="同步历史成交",
            command=lambda: self.refresh_fill_history(sync_order_history=True),
        ).grid(row=0, column=1, padx=(0, 6))
        ttk.Button(history_actions, text="同步历史仓位", command=self.refresh_position_histories).grid(row=0, column=2, padx=(0, 6))
        ttk.Button(history_actions, text="同步全部历史", command=self.sync_all_histories).grid(row=0, column=3)
        if not self._positions_zoom_detail_collapsed:
            self.toggle_positions_zoom_detail()
        if not self._positions_zoom_pending_orders_detail_collapsed:
            self.toggle_positions_zoom_pending_orders_detail()
        if not self._positions_zoom_order_history_detail_collapsed:
            self.toggle_positions_zoom_order_history_detail()
        if not self._positions_zoom_fills_detail_collapsed:
            self.toggle_positions_zoom_fills_detail()
        if not self._positions_zoom_position_history_detail_collapsed:
            self.toggle_positions_zoom_position_history_detail()
        self._load_local_history_cache()
        self.refresh_positions()
        self.sync_positions_zoom_data()
        self._expand_to_screen(window)
        self._refresh_all_refresh_badges()
        self._update_positions_zoom_search_shortcuts()
        self._update_position_history_search_shortcuts()
        self._schedule_positions_zoom_sync(30)
        self._refresh_positions_zoom_takeover_panel()

    def _build_positions_zoom_pending_orders_tab(self, parent: ttk.Frame) -> None:
        header = ttk.Frame(parent)
        header.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        header.columnconfigure(1, weight=1)
        pending_badge = self._create_refresh_badge(
            header,
            self._pending_orders_refresh_badge_text,
            self._pending_orders_refresh_badges,
        )
        pending_badge.grid(row=0, column=0, sticky="w", padx=(0, 8))
        ttk.Label(header, textvariable=self._positions_zoom_pending_orders_summary_text).grid(row=0, column=1, sticky="w")
        ttk.Button(header, text="刷新", command=self.refresh_pending_orders).grid(row=0, column=2, sticky="e", padx=(0, 6))
        ttk.Button(
            header,
            text="从选中条件单接管动态止盈",
            command=self.open_takeover_from_selected_conditional_order,
        ).grid(row=0, column=3, sticky="e", padx=(0, 6))
        ttk.Button(header, text="撤单选中", command=lambda: self.cancel_selected_pending_order("positions_zoom")).grid(
            row=0, column=4, sticky="e", padx=(0, 6)
        )
        ttk.Button(header, text="批量撤当前筛选", command=lambda: self.cancel_filtered_pending_orders("positions_zoom")).grid(
            row=0, column=5, sticky="e", padx=(0, 6)
        )
        ttk.Button(
            header,
            textvariable=self._positions_zoom_pending_orders_detail_toggle_text,
            command=self.toggle_positions_zoom_pending_orders_detail,
        ).grid(row=0, column=6, sticky="e")

        filter_row = ttk.Frame(parent)
        filter_row.grid(row=1, column=0, sticky="ew", pady=(0, 8))
        filter_row.columnconfigure(11, weight=1)
        ttk.Label(filter_row, text="类型").grid(row=0, column=0, sticky="w")
        type_combo = ttk.Combobox(
            filter_row,
            textvariable=self.pending_order_type_filter,
            values=list(POSITION_TYPE_OPTIONS.keys()),
            state="readonly",
            width=16,
        )
        type_combo.grid(row=0, column=1, sticky="w", padx=(6, 12))
        type_combo.bind("<<ComboboxSelected>>", self._on_pending_order_filter_changed)
        ttk.Label(filter_row, text="来源").grid(row=0, column=2, sticky="w")
        source_combo = ttk.Combobox(
            filter_row,
            textvariable=self.pending_order_source_filter,
            values=list(ORDER_SOURCE_FILTER_OPTIONS.keys()),
            state="readonly",
            width=16,
        )
        source_combo.grid(row=0, column=3, sticky="w", padx=(6, 12))
        source_combo.bind("<<ComboboxSelected>>", self._on_pending_order_filter_changed)
        ttk.Label(filter_row, text="状态").grid(row=0, column=4, sticky="w")
        state_combo = ttk.Combobox(
            filter_row,
            textvariable=self.pending_order_state_filter,
            values=list(ORDER_STATE_FILTER_OPTIONS.keys()),
            state="readonly",
            width=20,
        )
        state_combo.grid(row=0, column=5, sticky="w", padx=(6, 12))
        state_combo.bind("<<ComboboxSelected>>", self._on_pending_order_filter_changed)
        ttk.Label(filter_row, text="标的").grid(row=0, column=6, sticky="w")
        asset_entry = ttk.Entry(filter_row, textvariable=self.pending_order_asset_filter, width=10)
        asset_entry.grid(row=0, column=7, sticky="w", padx=(6, 12))
        asset_entry.bind("<KeyRelease>", self._on_pending_order_filter_changed)
        ttk.Label(filter_row, text="到期前缀").grid(row=0, column=8, sticky="w")
        expiry_entry = ttk.Entry(filter_row, textvariable=self.pending_order_expiry_prefix_filter, width=14)
        expiry_entry.grid(row=0, column=9, sticky="w", padx=(6, 12))
        expiry_entry.bind("<KeyRelease>", self._on_pending_order_filter_changed)
        ttk.Label(filter_row, text="搜索").grid(row=0, column=10, sticky="w")
        keyword_entry = ttk.Entry(filter_row, textvariable=self.pending_order_keyword)
        keyword_entry.grid(row=0, column=11, sticky="ew", padx=(6, 12))
        keyword_entry.bind("<KeyRelease>", self._on_pending_order_filter_changed)
        ttk.Button(filter_row, text="应用筛选", command=self._render_pending_orders_view).grid(row=0, column=12, padx=(0, 6))
        ttk.Button(filter_row, text="清空筛选", command=self.reset_pending_order_filters).grid(row=0, column=13)

        tree_frame = ttk.Frame(parent)
        tree_frame.grid(row=2, column=0, sticky="nsew")
        tree_frame.columnconfigure(0, weight=1)
        tree_frame.rowconfigure(0, weight=1)

        columns = ("time", "source", "inst_type", "inst_id", "state", "side", "ord_type", "price", "size", "filled", "fee", "tp_sl", "order_id", "cl_ord_id")
        tree = ttk.Treeview(tree_frame, columns=columns, show="headings", selectmode="browse")
        self._positions_zoom_pending_orders_tree = tree
        headings = {
            "time": "时间",
            "source": "来源",
            "inst_type": "类型",
            "inst_id": "合约",
            "state": "状态",
            "side": "方向",
            "ord_type": "委托类型",
            "price": "委托价",
            "size": "委托量",
            "filled": "已成交",
            "fee": "手续费",
            "tp_sl": "TP/SL",
            "order_id": "订单ID",
            "cl_ord_id": "clOrdId",
        }
        for column_id, width in (
            ("time", 150),
            ("source", 82),
            ("inst_type", 72),
            ("inst_id", 240),
            ("state", 120),
            ("side", 96),
            ("ord_type", 110),
            ("price", 100),
            ("size", 100),
            ("filled", 100),
            ("fee", 110),
            ("tp_sl", 180),
            ("order_id", 120),
            ("cl_ord_id", 150),
        ):
            tree.heading(column_id, text=headings[column_id])
            tree.column(column_id, width=width, anchor="e" if column_id in {"price", "size", "filled", "fee"} else "center")
        tree.column("inst_id", anchor="w")
        tree.column("tp_sl", anchor="w")
        tree.column("cl_ord_id", anchor="w")
        tree.grid(row=0, column=0, sticky="nsew")
        tree.bind("<<TreeviewSelect>>", self._on_pending_orders_selected)
        tree.tag_configure("profit", foreground="#13803d")
        tree.tag_configure("loss", foreground="#c23b3b")
        scroll_y = ttk.Scrollbar(tree_frame, orient="vertical", command=tree.yview)
        scroll_y.grid(row=0, column=1, sticky="ns")
        scroll_x = ttk.Scrollbar(tree_frame, orient="horizontal", command=tree.xview)
        scroll_x.grid(row=1, column=0, sticky="ew")
        tree.configure(yscrollcommand=scroll_y.set, xscrollcommand=scroll_x.set)
        self._register_positions_zoom_columns("pending_orders", "当前委托", tree, columns)

        detail_frame = ttk.LabelFrame(parent, text="委托详情", padding=12)
        detail_frame.grid(row=3, column=0, sticky="nsew", pady=(10, 0))
        self._positions_zoom_pending_orders_detail_frame = detail_frame
        detail_frame.columnconfigure(0, weight=1)
        detail_frame.rowconfigure(0, weight=1)
        self._positions_zoom_pending_orders_detail = Text(detail_frame, height=8, wrap="word", font=("Microsoft YaHei UI", 10), relief="flat")
        self._positions_zoom_pending_orders_detail.grid(row=0, column=0, sticky="nsew")
        detail_scroll = ttk.Scrollbar(detail_frame, orient="vertical", command=self._positions_zoom_pending_orders_detail.yview)
        detail_scroll.grid(row=0, column=1, sticky="ns")
        self._positions_zoom_pending_orders_detail.configure(yscrollcommand=detail_scroll.set)
        self._set_readonly_text(self._positions_zoom_pending_orders_detail, "这里会显示选中当前委托的详情。")

    def _build_positions_zoom_takeover_tab(self, parent: ttk.Frame) -> None:
        intro = (
            "推荐：在「当前委托」里选中一条「条件 / 算法」止损委托，再点「从选中条件单接管动态止盈」。"
            "程序会按该单的合约与平仓方向匹配持仓，并核对条件单数量不超过当前可平持仓后，再启动 OKX 止损动态上移。"
            "（上方工具栏「从选中持仓接管」仍可在拉取候选后手工挑选止损单。）"
        )
        ttk.Label(parent, text=intro, justify="left", wraplength=1000).grid(row=0, column=0, sticky="w", pady=(0, 8))
        ttk.Label(parent, textvariable=self._positions_zoom_takeover_status_text, foreground="#374151").grid(
            row=1, column=0, sticky="w", pady=(0, 6)
        )
        btn_row = ttk.Frame(parent)
        btn_row.grid(row=2, column=0, sticky="w", pady=(0, 8))
        ttk.Button(
            btn_row,
            text="从选中条件单接管动态止盈",
            command=self.open_takeover_from_selected_conditional_order,
        ).pack(side="left", padx=(0, 10))
        ttk.Button(btn_row, text="停止接管", command=self.stop_position_takeover_dynamic_stop).pack(side="left")

        tree_frame = ttk.Frame(parent)
        tree_frame.grid(row=3, column=0, sticky="nsew", pady=(4, 0))
        tree_frame.columnconfigure(0, weight=1)
        tree_frame.rowconfigure(0, weight=1)
        parent.rowconfigure(3, weight=1)
        cols = ("session", "api", "inst", "template", "entry", "qty", "stop", "status")
        tree = ttk.Treeview(tree_frame, columns=cols, show="headings", height=6, selectmode="browse")
        self._positions_zoom_takeover_tree = tree
        heads = {
            "session": "会话",
            "api": "API",
            "inst": "合约",
            "template": "模板",
            "entry": "开仓均价",
            "qty": "数量",
            "stop": "初始止损",
            "status": "状态",
        }
        widths = {"session": 72, "api": 110, "inst": 200, "template": 200, "entry": 120, "qty": 220, "stop": 120, "status": 300}
        for cid in cols:
            tree.heading(cid, text=heads[cid])
            tree.column(cid, width=widths[cid], anchor="w")
        tree.grid(row=0, column=0, sticky="nsew")
        sy = ttk.Scrollbar(tree_frame, orient="vertical", command=tree.yview)
        sy.grid(row=0, column=1, sticky="ns")
        tree.configure(yscrollcommand=sy.set)
        self._positions_zoom_takeover_status_text.set(self._positions_zoom_takeover_idle_caption())
        self._refresh_positions_zoom_takeover_panel()

    def _build_positions_zoom_order_history_tab(self, parent: ttk.Frame) -> None:
        header = ttk.Frame(parent)
        header.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        header.columnconfigure(1, weight=1)
        order_history_badge = self._create_refresh_badge(
            header,
            self._order_history_refresh_badge_text,
            self._order_history_refresh_badges,
        )
        order_history_badge.grid(row=0, column=0, sticky="w", padx=(0, 8))
        ttk.Label(header, textvariable=self._positions_zoom_order_history_summary_text).grid(row=0, column=1, sticky="w")
        ttk.Button(header, text="同步", command=self.refresh_order_history).grid(row=0, column=2, sticky="e", padx=(0, 6))
        ttk.Button(
            header,
            textvariable=self._positions_zoom_order_history_detail_toggle_text,
            command=self.toggle_positions_zoom_order_history_detail,
        ).grid(row=0, column=3, sticky="e")

        filter_row = ttk.Frame(parent)
        filter_row.grid(row=1, column=0, sticky="ew", pady=(0, 8))
        filter_row.columnconfigure(11, weight=1)
        ttk.Label(filter_row, text="类型").grid(row=0, column=0, sticky="w")
        type_combo = ttk.Combobox(
            filter_row,
            textvariable=self.order_history_type_filter,
            values=list(POSITION_TYPE_OPTIONS.keys()),
            state="readonly",
            width=16,
        )
        type_combo.grid(row=0, column=1, sticky="w", padx=(6, 12))
        type_combo.bind("<<ComboboxSelected>>", self._on_order_history_filter_changed)
        ttk.Label(filter_row, text="来源").grid(row=0, column=2, sticky="w")
        source_combo = ttk.Combobox(
            filter_row,
            textvariable=self.order_history_source_filter,
            values=list(ORDER_SOURCE_FILTER_OPTIONS.keys()),
            state="readonly",
            width=16,
        )
        source_combo.grid(row=0, column=3, sticky="w", padx=(6, 12))
        source_combo.bind("<<ComboboxSelected>>", self._on_order_history_filter_changed)
        ttk.Label(filter_row, text="状态").grid(row=0, column=4, sticky="w")
        state_combo = ttk.Combobox(
            filter_row,
            textvariable=self.order_history_state_filter,
            values=list(ORDER_STATE_FILTER_OPTIONS.keys()),
            state="readonly",
            width=20,
        )
        state_combo.grid(row=0, column=5, sticky="w", padx=(6, 12))
        state_combo.bind("<<ComboboxSelected>>", self._on_order_history_filter_changed)
        ttk.Label(filter_row, text="标的").grid(row=0, column=6, sticky="w")
        asset_entry = ttk.Entry(filter_row, textvariable=self.order_history_asset_filter, width=10)
        asset_entry.grid(row=0, column=7, sticky="w", padx=(6, 12))
        asset_entry.bind("<KeyRelease>", self._on_order_history_filter_changed)
        ttk.Label(filter_row, text="到期前缀").grid(row=0, column=8, sticky="w")
        expiry_entry = ttk.Entry(filter_row, textvariable=self.order_history_expiry_prefix_filter, width=14)
        expiry_entry.grid(row=0, column=9, sticky="w", padx=(6, 12))
        expiry_entry.bind("<KeyRelease>", self._on_order_history_filter_changed)
        ttk.Label(filter_row, text="搜索").grid(row=0, column=10, sticky="w")
        keyword_entry = ttk.Entry(filter_row, textvariable=self.order_history_keyword)
        keyword_entry.grid(row=0, column=11, sticky="ew", padx=(6, 12))
        keyword_entry.bind("<KeyRelease>", self._on_order_history_filter_changed)
        ttk.Button(filter_row, text="应用筛选", command=self._render_order_history_view).grid(row=0, column=12, padx=(0, 6))
        ttk.Button(filter_row, text="清空筛选", command=self.reset_order_history_filters).grid(row=0, column=13)

        tree_frame = ttk.Frame(parent)
        tree_frame.grid(row=2, column=0, sticky="nsew")
        tree_frame.columnconfigure(0, weight=1)
        tree_frame.rowconfigure(0, weight=1)

        columns = ("time", "source", "inst_type", "inst_id", "state", "side", "ord_type", "price", "size", "filled", "fee", "tp_sl", "order_id", "cl_ord_id")
        tree = ttk.Treeview(tree_frame, columns=columns, show="headings", selectmode="browse")
        self._positions_zoom_order_history_tree = tree
        headings = {
            "time": "时间",
            "source": "来源",
            "inst_type": "类型",
            "inst_id": "合约",
            "state": "状态",
            "side": "方向",
            "ord_type": "委托类型",
            "price": "委托价",
            "size": "委托量",
            "filled": "已成交",
            "fee": "手续费",
            "tp_sl": "TP/SL",
            "order_id": "订单ID",
            "cl_ord_id": "clOrdId",
        }
        for column_id, width in (
            ("time", 150),
            ("source", 82),
            ("inst_type", 72),
            ("inst_id", 240),
            ("state", 120),
            ("side", 96),
            ("ord_type", 110),
            ("price", 100),
            ("size", 100),
            ("filled", 100),
            ("fee", 220),
            ("tp_sl", 180),
            ("order_id", 120),
            ("cl_ord_id", 150),
        ):
            tree.heading(column_id, text=headings[column_id])
            tree.column(column_id, width=width, anchor="e" if column_id in {"price", "size", "filled", "fee"} else "center")
        tree.column("inst_id", anchor="w")
        tree.column("tp_sl", anchor="w")
        tree.column("cl_ord_id", anchor="w")
        tree.grid(row=0, column=0, sticky="nsew")
        tree.bind("<<TreeviewSelect>>", self._on_order_history_selected)
        tree.tag_configure("profit", foreground="#13803d")
        tree.tag_configure("loss", foreground="#c23b3b")
        scroll_y = ttk.Scrollbar(tree_frame, orient="vertical", command=tree.yview)
        scroll_y.grid(row=0, column=1, sticky="ns")
        scroll_x = ttk.Scrollbar(tree_frame, orient="horizontal", command=tree.xview)
        scroll_x.grid(row=1, column=0, sticky="ew")
        tree.configure(yscrollcommand=scroll_y.set, xscrollcommand=scroll_x.set)
        self._register_positions_zoom_columns("order_history", "历史委托", tree, columns)

        detail_frame = ttk.LabelFrame(parent, text="委托详情", padding=12)
        detail_frame.grid(row=3, column=0, sticky="nsew", pady=(10, 0))
        self._positions_zoom_order_history_detail_frame = detail_frame
        detail_frame.columnconfigure(0, weight=1)
        detail_frame.rowconfigure(0, weight=1)
        self._positions_zoom_order_history_detail = Text(detail_frame, height=8, wrap="word", font=("Microsoft YaHei UI", 10), relief="flat")
        self._positions_zoom_order_history_detail.grid(row=0, column=0, sticky="nsew")
        detail_scroll = ttk.Scrollbar(detail_frame, orient="vertical", command=self._positions_zoom_order_history_detail.yview)
        detail_scroll.grid(row=0, column=1, sticky="ns")
        self._positions_zoom_order_history_detail.configure(yscrollcommand=detail_scroll.set)
        self._set_readonly_text(self._positions_zoom_order_history_detail, "这里会显示选中历史委托的详情。")

    def _build_positions_zoom_fills_tab(self, parent: ttk.Frame) -> None:
        header = ttk.Frame(parent)
        header.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        header.columnconfigure(0, weight=1)
        ttk.Label(header, textvariable=self._positions_zoom_fills_summary_text).grid(row=0, column=0, sticky="w")
        ttk.Button(
            header,
            textvariable=self._positions_zoom_fills_load_more_text,
            command=self.expand_fill_history_limit,
        ).grid(row=0, column=1, sticky="e", padx=(0, 6))
        ttk.Button(header, textvariable=self._positions_zoom_fills_detail_toggle_text, command=self.toggle_positions_zoom_fills_detail).grid(
            row=0, column=2, sticky="e"
        )
        filter_row = ttk.Frame(parent)
        filter_row.grid(row=1, column=0, sticky="ew", pady=(0, 8))
        filter_row.columnconfigure(9, weight=1)
        ttk.Label(filter_row, text="类型").grid(row=0, column=0, sticky="w")
        type_combo = ttk.Combobox(
            filter_row,
            textvariable=self.fill_history_type_filter,
            values=list(POSITION_TYPE_OPTIONS.keys()),
            state="readonly",
            width=16,
        )
        type_combo.grid(row=0, column=1, sticky="w", padx=(6, 12))
        type_combo.bind("<<ComboboxSelected>>", self._on_fill_history_filter_changed)
        ttk.Label(filter_row, text="方向").grid(row=0, column=2, sticky="w")
        side_combo = ttk.Combobox(
            filter_row,
            textvariable=self.fill_history_side_filter,
            values=list(HISTORY_FILL_SIDE_FILTER_OPTIONS.keys()),
            state="readonly",
            width=16,
        )
        side_combo.grid(row=0, column=3, sticky="w", padx=(6, 12))
        side_combo.bind("<<ComboboxSelected>>", self._on_fill_history_filter_changed)
        ttk.Label(filter_row, text="标的").grid(row=0, column=4, sticky="w")
        asset_entry = ttk.Entry(filter_row, textvariable=self.fill_history_asset_filter, width=10)
        asset_entry.grid(row=0, column=5, sticky="w", padx=(6, 12))
        asset_entry.bind("<KeyRelease>", self._on_fill_history_filter_changed)
        ttk.Label(filter_row, text="到期前缀").grid(row=0, column=6, sticky="w")
        expiry_entry = ttk.Entry(filter_row, textvariable=self.fill_history_expiry_prefix_filter, width=14)
        expiry_entry.grid(row=0, column=7, sticky="w", padx=(6, 12))
        expiry_entry.bind("<KeyRelease>", self._on_fill_history_filter_changed)
        ttk.Label(filter_row, text="搜索").grid(row=0, column=8, sticky="w")
        keyword_entry = ttk.Entry(filter_row, textvariable=self.fill_history_keyword)
        keyword_entry.grid(row=0, column=9, sticky="ew", padx=(6, 12))
        keyword_entry.bind("<KeyRelease>", self._on_fill_history_filter_changed)
        self._positions_zoom_fills_apply_contract_button = ttk.Button(
            filter_row,
            text="\u5e26\u5165\u5408\u7ea6",
            command=self.apply_selected_option_to_fill_history_search,
        )
        self._positions_zoom_fills_apply_contract_button.grid(row=0, column=10, padx=(0, 6))
        self._positions_zoom_fills_apply_expiry_prefix_button = ttk.Button(
            filter_row,
            text="\u5e26\u5165\u5230\u671f\u524d\u7f00",
            command=self.apply_selected_option_expiry_prefix_to_fill_history_search,
        )
        self._positions_zoom_fills_apply_expiry_prefix_button.grid(row=0, column=11, padx=(0, 6))
        ttk.Button(filter_row, text="应用筛选", command=self._render_positions_zoom_fills_view).grid(
            row=0, column=12, padx=(0, 6)
        )
        ttk.Button(filter_row, text="清空筛选", command=self.reset_fill_history_filters).grid(row=0, column=13)
        ttk.Label(
            filter_row,
            textvariable=self._positions_zoom_fill_history_search_hint_text,
            foreground="#6b7280",
        ).grid(row=1, column=8, columnspan=6, sticky="w", pady=(6, 0))
        tree_frame = ttk.Frame(parent)
        tree_frame.grid(row=2, column=0, sticky="nsew")
        tree_frame.columnconfigure(0, weight=1)
        tree_frame.rowconfigure(0, weight=1)

        columns = ("time", "inst_type", "inst_id", "side", "price", "size", "fee", "pnl", "exec_type")
        tree = ttk.Treeview(tree_frame, columns=columns, show="headings", selectmode="browse")
        self._positions_zoom_fills_tree = tree
        headings = {
            "time": "时间",
            "inst_type": "类型",
            "inst_id": "合约",
            "side": "方向",
            "price": "成交价",
            "size": "成交量",
            "fee": "手续费",
            "pnl": "已实现盈亏",
            "exec_type": "成交类型",
        }
        for column_id, width in (
            ("time", 150),
            ("inst_type", 72),
            ("inst_id", 240),
            ("side", 96),
            ("price", 100),
            ("size", 100),
            ("fee", 220),
            ("pnl", 220),
            ("exec_type", 108),
        ):
            tree.heading(column_id, text=headings[column_id])
            tree.column(column_id, width=width, anchor="e" if column_id in {"price", "size", "fee", "pnl"} else "center")
        tree.column("inst_id", anchor="w")
        tree.grid(row=0, column=0, sticky="nsew")
        tree.bind("<<TreeviewSelect>>", self._on_positions_zoom_fills_selected)
        tree.tag_configure("profit", foreground="#13803d")
        tree.tag_configure("loss", foreground="#c23b3b")
        scroll_y = ttk.Scrollbar(tree_frame, orient="vertical", command=tree.yview)
        scroll_y.grid(row=0, column=1, sticky="ns")
        scroll_x = ttk.Scrollbar(tree_frame, orient="horizontal", command=tree.xview)
        scroll_x.grid(row=1, column=0, sticky="ew")
        tree.configure(yscrollcommand=scroll_y.set, xscrollcommand=scroll_x.set)
        self._register_positions_zoom_columns("fills", "历史成交", tree, columns)

        detail_frame = ttk.LabelFrame(parent, text="成交详情", padding=12)
        detail_frame.grid(row=3, column=0, sticky="nsew", pady=(10, 0))
        self._positions_zoom_fills_detail_frame = detail_frame
        detail_frame.columnconfigure(0, weight=1)
        detail_frame.rowconfigure(0, weight=1)
        self._positions_zoom_fills_detail = Text(detail_frame, height=8, wrap="word", font=("Microsoft YaHei UI", 10), relief="flat")
        self._positions_zoom_fills_detail.grid(row=0, column=0, sticky="nsew")
        detail_scroll = ttk.Scrollbar(detail_frame, orient="vertical", command=self._positions_zoom_fills_detail.yview)
        detail_scroll.grid(row=0, column=1, sticky="ns")
        self._positions_zoom_fills_detail.configure(yscrollcommand=detail_scroll.set)
        self._set_readonly_text(self._positions_zoom_fills_detail, "这里会显示选中历史成交单的详情。")

    def _build_positions_zoom_position_history_tab(self, parent: ttk.Frame) -> None:
        header = ttk.Frame(parent)
        header.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        header.columnconfigure(0, weight=1)
        ttk.Label(header, textvariable=self._positions_zoom_position_history_summary_text).grid(row=0, column=0, sticky="w")
        ttk.Button(
            header,
            textvariable=self._positions_zoom_position_history_load_more_text,
            command=self.expand_position_history_limit,
        ).grid(row=0, column=1, sticky="e", padx=(0, 6))
        ttk.Button(header, text="编辑备注", command=self.edit_selected_position_history_note).grid(
            row=0, column=2, sticky="e", padx=(0, 6)
        )
        ttk.Button(
            header,
            textvariable=self._positions_zoom_position_history_detail_toggle_text,
            command=self.toggle_positions_zoom_position_history_detail,
        ).grid(row=0, column=3, sticky="e")
        filter_row = ttk.Frame(parent)
        filter_row.grid(row=1, column=0, sticky="ew", pady=(0, 8))
        filter_row.columnconfigure(9, weight=1)
        ttk.Label(filter_row, text="类型").grid(row=0, column=0, sticky="w")
        type_combo = ttk.Combobox(
            filter_row,
            textvariable=self.position_history_type_filter,
            values=list(POSITION_TYPE_OPTIONS.keys()),
            state="readonly",
            width=16,
        )
        type_combo.grid(row=0, column=1, sticky="w", padx=(6, 12))
        type_combo.bind("<<ComboboxSelected>>", self._on_position_history_filter_changed)
        ttk.Label(filter_row, text="保证金模式").grid(row=0, column=2, sticky="w")
        margin_combo = ttk.Combobox(
            filter_row,
            textvariable=self.position_history_margin_filter,
            values=list(HISTORY_MARGIN_MODE_FILTER_OPTIONS.keys()),
            state="readonly",
            width=16,
        )
        margin_combo.grid(row=0, column=3, sticky="w", padx=(6, 12))
        margin_combo.bind("<<ComboboxSelected>>", self._on_position_history_filter_changed)
        ttk.Label(filter_row, text="标的").grid(row=0, column=4, sticky="w")
        asset_entry = ttk.Entry(filter_row, textvariable=self.position_history_asset_filter, width=10)
        asset_entry.grid(row=0, column=5, sticky="w", padx=(6, 12))
        asset_entry.bind("<KeyRelease>", self._on_position_history_filter_changed)
        ttk.Label(filter_row, text="到期前缀").grid(row=0, column=6, sticky="w")
        expiry_entry = ttk.Entry(filter_row, textvariable=self.position_history_expiry_prefix_filter, width=14)
        expiry_entry.grid(row=0, column=7, sticky="w", padx=(6, 12))
        expiry_entry.bind("<KeyRelease>", self._on_position_history_filter_changed)
        ttk.Label(filter_row, text="搜索").grid(row=0, column=8, sticky="w")
        keyword_entry = ttk.Entry(filter_row, textvariable=self.position_history_keyword)
        keyword_entry.grid(row=0, column=9, sticky="ew", padx=(6, 12))
        keyword_entry.bind("<KeyRelease>", self._on_position_history_filter_changed)
        self._positions_zoom_position_history_apply_contract_button = ttk.Button(
            filter_row,
            text="\u5e26\u5165\u5408\u7ea6",
            command=self.apply_selected_option_to_position_history_search,
        )
        self._positions_zoom_position_history_apply_contract_button.grid(row=0, column=10, padx=(0, 6))
        self._positions_zoom_position_history_apply_expiry_prefix_button = ttk.Button(
            filter_row,
            text="\u5e26\u5165\u5230\u671f\u524d\u7f00",
            command=self.apply_selected_option_expiry_prefix_to_position_history_search,
        )
        self._positions_zoom_position_history_apply_expiry_prefix_button.grid(row=0, column=11, padx=(0, 6))
        ttk.Button(filter_row, text="应用筛选", command=self._render_positions_zoom_position_history_view).grid(
            row=0, column=12, padx=(0, 6)
        )
        ttk.Button(filter_row, text="清空筛选", command=self.reset_position_history_filters).grid(row=0, column=13)
        ttk.Label(
            filter_row,
            textvariable=self._positions_zoom_position_history_search_hint_text,
            foreground="#6b7280",
        ).grid(row=1, column=0, columnspan=14, sticky="w", pady=(6, 0))
        ttk.Label(filter_row, text="本地开始").grid(row=2, column=0, sticky="w", pady=(6, 0))
        range_start_entry = ttk.Entry(filter_row, textvariable=self.position_history_range_start, width=12)
        range_start_entry.grid(row=2, column=1, sticky="w", padx=(6, 8), pady=(6, 0))
        range_start_entry.bind("<KeyRelease>", self._on_position_history_filter_changed)
        ttk.Label(filter_row, text="本地结束").grid(row=2, column=2, sticky="w", padx=(8, 0), pady=(6, 0))
        range_end_entry = ttk.Entry(filter_row, textvariable=self.position_history_range_end, width=12)
        range_end_entry.grid(row=2, column=3, sticky="w", padx=(6, 12), pady=(6, 0))
        range_end_entry.bind("<KeyRelease>", self._on_position_history_filter_changed)
        ttk.Label(filter_row, text="YYYY-MM-DD，默认本年；两端留空则不过滤", foreground="#6b7280").grid(
            row=2, column=4, columnspan=10, sticky="w", pady=(6, 0)
        )
        tree_frame = ttk.Frame(parent)
        tree_frame.grid(row=2, column=0, sticky="nsew")
        tree_frame.columnconfigure(0, weight=1)
        tree_frame.rowconfigure(0, weight=1)

        columns = (
            "time",
            "inst_type",
            "inst_id",
            "mgn_mode",
            "side",
            "open_avg",
            "close_avg",
            "close_size",
            "fee",
            "pnl",
            "realized",
            "note",
        )
        tree = ttk.Treeview(tree_frame, columns=columns, show="headings", selectmode="browse")
        self._positions_zoom_position_history_tree = tree
        headings = {
            "time": "更新时间",
            "inst_type": "类型",
            "inst_id": "合约",
            "mgn_mode": "保证金模式",
            "side": "方向",
            "open_avg": "开仓均价",
            "close_avg": "平仓均价",
            "close_size": "平仓数量",
            "fee": "手续费",
            "pnl": "盈亏",
            "realized": "已实现盈亏",
            "note": "备注",
        }
        for column_id, width in (
            ("time", 150),
            ("inst_type", 72),
            ("inst_id", 240),
            ("mgn_mode", 96),
            ("side", 96),
            ("open_avg", 100),
            ("close_avg", 100),
            ("close_size", 100),
            ("fee", 220),
            ("pnl", 220),
            ("realized", 240),
            ("note", 220),
        ):
            tree.heading(column_id, text=headings[column_id])
            tree.column(
                column_id,
                width=width,
                anchor="e"
                if column_id in {"open_avg", "close_avg", "close_size", "fee", "pnl", "realized"}
                else "center",
            )
        tree.column("inst_id", anchor="w")
        tree.column("note", anchor="w")
        tree.grid(row=0, column=0, sticky="nsew")
        tree.bind("<<TreeviewSelect>>", self._on_positions_zoom_position_history_selected)
        tree.tag_configure("profit", foreground="#13803d")
        tree.tag_configure("loss", foreground="#c23b3b")
        scroll_y = ttk.Scrollbar(tree_frame, orient="vertical", command=tree.yview)
        scroll_y.grid(row=0, column=1, sticky="ns")
        scroll_x = ttk.Scrollbar(tree_frame, orient="horizontal", command=tree.xview)
        scroll_x.grid(row=1, column=0, sticky="ew")
        tree.configure(yscrollcommand=scroll_y.set, xscrollcommand=scroll_x.set)
        self._register_positions_zoom_columns("position_history", "历史仓位", tree, columns)

        detail_frame = ttk.LabelFrame(parent, text="历史仓位详情", padding=12)
        detail_frame.grid(row=3, column=0, sticky="nsew", pady=(10, 0))
        self._positions_zoom_position_history_detail_frame = detail_frame
        detail_frame.columnconfigure(0, weight=1)
        detail_frame.rowconfigure(0, weight=1)
        self._positions_zoom_position_history_detail = Text(
            detail_frame,
            height=8,
            wrap="word",
            font=("Microsoft YaHei UI", 10),
            relief="flat",
        )
        self._positions_zoom_position_history_detail.grid(row=0, column=0, sticky="nsew")
        detail_scroll = ttk.Scrollbar(detail_frame, orient="vertical", command=self._positions_zoom_position_history_detail.yview)
        detail_scroll.grid(row=0, column=1, sticky="ns")
        self._positions_zoom_position_history_detail.configure(yscrollcommand=detail_scroll.set)
        self._set_readonly_text(self._positions_zoom_position_history_detail, "这里会显示选中历史仓位的详情。")

    def _close_positions_zoom_window(self) -> None:
        if self._positions_zoom_sync_job is not None:
            try:
                self.root.after_cancel(self._positions_zoom_sync_job)
            except Exception:
                pass
        self._positions_zoom_sync_job = None
        if self._positions_zoom_column_window is not None and self._positions_zoom_column_window.winfo_exists():
            self._positions_zoom_column_window.destroy()
        self._positions_zoom_column_window = None
        if self._positions_zoom_window is not None and self._positions_zoom_window.winfo_exists():
            self._positions_zoom_window.destroy()
        self._positions_zoom_window = None
        self._positions_zoom_credential_profile_combo = None
        self._positions_zoom_tree = None
        self._positions_zoom_detail = None
        self._positions_zoom_notebook = None
        self._positions_zoom_selection_suppressed_item_id = None
        self._positions_zoom_pending_orders_tree = None
        self._positions_zoom_pending_orders_detail = None
        self._positions_zoom_takeover_tree = None
        self._positions_zoom_order_history_tree = None
        self._positions_zoom_order_history_detail = None
        self._positions_zoom_fills_tree = None
        self._positions_zoom_fills_detail = None
        self._positions_zoom_position_history_tree = None
        self._positions_zoom_position_history_detail = None
        self._positions_zoom_column_groups = {}
        self._positions_zoom_column_vars = {}
        self._position_history_usdt_prices = {}
        self._positions_zoom_detail_frame = None
        self._positions_zoom_pending_orders_detail_frame = None
        self._positions_zoom_order_history_detail_frame = None
        self._positions_zoom_fills_detail_frame = None
        self._positions_zoom_position_history_detail_frame = None
        self._positions_zoom_selected_item_id = None
        self._positions_zoom_apply_contract_button = None
        self._positions_zoom_apply_expiry_prefix_button = None
        self._positions_zoom_fills_apply_contract_button = None
        self._positions_zoom_fills_apply_expiry_prefix_button = None
        self._positions_zoom_position_history_apply_contract_button = None
        self._positions_zoom_position_history_apply_expiry_prefix_button = None
        self._pending_order_canceling = False
        self._positions_zoom_detail_collapsed = False
        self._positions_zoom_history_collapsed = False
        self._positions_zoom_pending_orders_detail_collapsed = False
        self._positions_zoom_order_history_detail_collapsed = False
        self._positions_zoom_fills_detail_collapsed = False
        self._positions_zoom_position_history_detail_collapsed = False
        self._positions_zoom_detail_toggle_text.set("\u5c55\u5f00\u6301\u4ed3\u8be6\u60c5")
        self._positions_zoom_pending_orders_detail_toggle_text.set("\u5c55\u5f00\u59d4\u6258\u8be6\u60c5")
        self._positions_zoom_order_history_detail_toggle_text.set("\u5c55\u5f00\u59d4\u6258\u8be6\u60c5")
        self._positions_zoom_fills_detail_toggle_text.set("\u5c55\u5f00\u6210\u4ea4\u8be6\u60c5")
        self._positions_zoom_position_history_detail_toggle_text.set("\u5c55\u5f00\u4ed3\u4f4d\u8be6\u60c5")
        self._positions_zoom_detail_toggle_text.set("折叠持仓详情")
        self._positions_zoom_history_toggle_text.set("折叠历史区域")
        self._positions_zoom_pending_orders_detail_toggle_text.set("折叠委托详情")
        self._positions_zoom_order_history_detail_toggle_text.set("折叠委托详情")
        self._positions_zoom_fills_detail_toggle_text.set("折叠成交详情")
        self._positions_zoom_position_history_detail_toggle_text.set("折叠仓位详情")
        self._positions_zoom_fills_load_more_text.set("增加100条")
        self._positions_zoom_position_history_load_more_text.set("增加100条")
        self._fill_history_fetch_limit = 100
        self._fill_history_load_more_clicks = 0
        self._position_history_fetch_limit = 300
        self._position_history_load_more_clicks = 0
        self._positions_zoom_option_search_hint_text.set("选中期权后，可一键带入合约或到期前缀。")
        self._positions_zoom_position_history_search_hint_text.set("选中历史期权后，可一键带入合约或到期前缀。")

    def _register_positions_zoom_columns(
        self,
        group_key: str,
        title: str,
        tree: ttk.Treeview,
        columns: tuple[str, ...],
    ) -> None:
        default_visible_columns = POSITIONS_ZOOM_DEFAULT_VISIBLE_COLUMNS.get(group_key)
        if default_visible_columns:
            tree.configure(
                displaycolumns=tuple(column_id for column_id in columns if column_id in default_visible_columns)
            )
        self._positions_zoom_column_groups[group_key] = {
            "title": title,
            "tree": tree,
            "columns": tuple(columns),
            "headings": {column_id: tree.heading(column_id).get("text", column_id) for column_id in columns},
        }

    def open_positions_zoom_column_window(self) -> None:
        if self._positions_zoom_window is None or not self._positions_zoom_window.winfo_exists():
            return
        if self._positions_zoom_column_window is not None and self._positions_zoom_column_window.winfo_exists():
            self._positions_zoom_column_window.focus_force()
            return

        window = Toplevel(self._positions_zoom_window)
        window.title("持仓大窗列设置")
        apply_adaptive_window_geometry(
            window,
            width_ratio=0.34,
            height_ratio=0.48,
            min_width=480,
            min_height=420,
            max_width=700,
            max_height=760,
        )
        window.transient(self._positions_zoom_window)
        self._positions_zoom_column_window = window
        window.protocol("WM_DELETE_WINDOW", self._close_positions_zoom_column_window)

        container = ttk.Frame(window, padding=12)
        container.pack(fill="both", expand=True)
        container.columnconfigure(0, weight=1)
        container.rowconfigure(1, weight=1)

        ttk.Label(
            container,
            text="可按区域勾选显示/隐藏列。'合约/分组' 为结构列，当前固定显示。",
        ).grid(row=0, column=0, sticky="w", pady=(0, 10))

        notebook = ttk.Notebook(container)
        notebook.grid(row=1, column=0, sticky="nsew")

        self._positions_zoom_column_vars = {}
        group_order = ("positions", "pending_orders", "order_history", "fills", "position_history")
        for group_key in group_order:
            group = self._positions_zoom_column_groups.get(group_key)
            if not group:
                continue
            title = str(group["title"])
            columns = tuple(group["columns"])
            headings = dict(group["headings"])
            tree = group["tree"]
            if not isinstance(tree, ttk.Treeview):
                continue
            visible_columns = set(_tree_display_columns(tree, columns))
            tab = ttk.Frame(notebook, padding=12)
            tab.columnconfigure(0, weight=1)
            notebook.add(tab, text=title)

            actions = ttk.Frame(tab)
            actions.grid(row=0, column=0, sticky="ew", pady=(0, 8))
            actions.columnconfigure(0, weight=1)
            ttk.Label(actions, text=f"{title} 列").grid(row=0, column=0, sticky="w")
            ttk.Button(
                actions,
                text="全部显示",
                command=lambda key=group_key: self._set_positions_zoom_columns_visible(key, True),
            ).grid(row=0, column=1, padx=(0, 6))
            ttk.Button(
                actions,
                text="恢复默认",
                command=lambda key=group_key: self._reset_positions_zoom_columns(key),
            ).grid(row=0, column=2)

            checks = ttk.Frame(tab)
            checks.grid(row=1, column=0, sticky="nsew")
            for column_index in range(2):
                checks.columnconfigure(column_index, weight=1)

            group_vars: dict[str, BooleanVar] = {}
            for index, column_id in enumerate(columns):
                var = BooleanVar(value=column_id in visible_columns)
                group_vars[column_id] = var
                ttk.Checkbutton(
                    checks,
                    text=headings.get(column_id, column_id),
                    variable=var,
                    command=lambda key=group_key: self._apply_positions_zoom_column_visibility(key),
                ).grid(row=index // 2, column=index % 2, sticky="w", padx=(0, 12), pady=4)
            self._positions_zoom_column_vars[group_key] = group_vars

    def _close_positions_zoom_column_window(self) -> None:
        if self._positions_zoom_column_window is not None and self._positions_zoom_column_window.winfo_exists():
            self._positions_zoom_column_window.destroy()
        self._positions_zoom_column_window = None

    def _apply_positions_zoom_column_visibility(self, group_key: str) -> None:
        group = self._positions_zoom_column_groups.get(group_key)
        variables = self._positions_zoom_column_vars.get(group_key)
        if not group or not variables:
            return
        tree = group["tree"]
        columns = tuple(group["columns"])
        if not isinstance(tree, ttk.Treeview):
            return
        visible_columns = [column_id for column_id in columns if variables[column_id].get()]
        if not visible_columns:
            fallback = columns[0]
            variables[fallback].set(True)
            visible_columns = [fallback]
        tree.configure(displaycolumns=tuple(visible_columns))

    def _set_positions_zoom_columns_visible(self, group_key: str, visible: bool) -> None:
        variables = self._positions_zoom_column_vars.get(group_key)
        group = self._positions_zoom_column_groups.get(group_key)
        if not variables or not group:
            return
        columns = tuple(group["columns"])
        if not columns:
            return
        for column_id in columns:
            variables[column_id].set(visible)
        if not visible:
            variables[columns[0]].set(True)
        self._apply_positions_zoom_column_visibility(group_key)

    def _reset_positions_zoom_columns(self, group_key: str) -> None:
        variables = self._positions_zoom_column_vars.get(group_key)
        group = self._positions_zoom_column_groups.get(group_key)
        if not variables or not group:
            return
        columns = tuple(group["columns"])
        default_visible_columns = POSITIONS_ZOOM_DEFAULT_VISIBLE_COLUMNS.get(group_key)
        if default_visible_columns:
            default_set = set(default_visible_columns)
            for column_id in columns:
                variables[column_id].set(column_id in default_set)
            self._apply_positions_zoom_column_visibility(group_key)
            return
        self._set_positions_zoom_columns_visible(group_key, True)

    def toggle_positions_zoom_detail(self) -> None:
        if self._positions_zoom_detail_frame is None:
            return
        if self._positions_zoom_detail_collapsed:
            self._positions_zoom_detail_frame.grid()
            self._positions_zoom_detail_toggle_text.set("折叠持仓详情")
        else:
            self._positions_zoom_detail_frame.grid_remove()
            self._positions_zoom_detail_toggle_text.set("展开持仓详情")
        self._positions_zoom_detail_collapsed = not self._positions_zoom_detail_collapsed

    def toggle_positions_zoom_history(self) -> None:
        if self._positions_zoom_notebook is None:
            return
        if self._positions_zoom_history_collapsed:
            self._positions_zoom_notebook.grid()
            self._positions_zoom_history_toggle_text.set("折叠历史区域")
        else:
            self._positions_zoom_notebook.grid_remove()
            self._positions_zoom_history_toggle_text.set("展开历史区域")
        self._positions_zoom_history_collapsed = not self._positions_zoom_history_collapsed

    def toggle_positions_zoom_pending_orders_detail(self) -> None:
        if self._positions_zoom_pending_orders_detail_frame is None:
            return
        if self._positions_zoom_pending_orders_detail_collapsed:
            self._positions_zoom_pending_orders_detail_frame.grid()
            self._positions_zoom_pending_orders_detail_toggle_text.set("折叠委托详情")
        else:
            self._positions_zoom_pending_orders_detail_frame.grid_remove()
            self._positions_zoom_pending_orders_detail_toggle_text.set("展开委托详情")
        self._positions_zoom_pending_orders_detail_collapsed = not self._positions_zoom_pending_orders_detail_collapsed

    def toggle_positions_zoom_order_history_detail(self) -> None:
        if self._positions_zoom_order_history_detail_frame is None:
            return
        if self._positions_zoom_order_history_detail_collapsed:
            self._positions_zoom_order_history_detail_frame.grid()
            self._positions_zoom_order_history_detail_toggle_text.set("折叠委托详情")
        else:
            self._positions_zoom_order_history_detail_frame.grid_remove()
            self._positions_zoom_order_history_detail_toggle_text.set("展开委托详情")
        self._positions_zoom_order_history_detail_collapsed = not self._positions_zoom_order_history_detail_collapsed

    def toggle_positions_zoom_fills_detail(self) -> None:
        if self._positions_zoom_fills_detail_frame is None:
            return
        if self._positions_zoom_fills_detail_collapsed:
            self._positions_zoom_fills_detail_frame.grid()
            self._positions_zoom_fills_detail_toggle_text.set("折叠成交详情")
        else:
            self._positions_zoom_fills_detail_frame.grid_remove()
            self._positions_zoom_fills_detail_toggle_text.set("展开成交详情")
        self._positions_zoom_fills_detail_collapsed = not self._positions_zoom_fills_detail_collapsed

    def toggle_positions_zoom_position_history_detail(self) -> None:
        if self._positions_zoom_position_history_detail_frame is None:
            return
        if self._positions_zoom_position_history_detail_collapsed:
            self._positions_zoom_position_history_detail_frame.grid()
            self._positions_zoom_position_history_detail_toggle_text.set("折叠仓位详情")
        else:
            self._positions_zoom_position_history_detail_frame.grid_remove()
            self._positions_zoom_position_history_detail_toggle_text.set("展开仓位详情")
        self._positions_zoom_position_history_detail_collapsed = not self._positions_zoom_position_history_detail_collapsed

    def _sync_positions_zoom_window(self) -> None:
        self._positions_zoom_sync_job = None
        if (
            self._positions_zoom_window is None
            or not self._positions_zoom_window.winfo_exists()
            or self._positions_zoom_tree is None
        ):
            return

        self._positions_zoom_summary_text.set(self.positions_summary_text.get())
        zoom_tree = self._positions_zoom_tree
        self._sync_positions_zoom_columns_from_main()
        zoom_tree.delete(*zoom_tree.get_children())
        zoom_visible = self._positions_zoom_visible_positions()
        self._populate_positions_tree_from_groups(zoom_tree, zoom_visible)
        selected = self.position_tree.selection()
        if selected and zoom_tree.exists(selected[0]):
            if zoom_tree.selection() != (selected[0],):
                self._positions_zoom_selection_suppressed_item_id = selected[0]
                self._position_selection_syncing = True
                try:
                    zoom_tree.selection_set(selected[0])
                finally:
                    self._position_selection_syncing = False
            try:
                zoom_tree.see(selected[0])
            except Exception:
                pass
            self._positions_zoom_selected_item_id = selected[0]
        else:
            self._positions_zoom_selected_item_id = None
        self._refresh_positions_zoom_detail()
        self._update_positions_zoom_search_shortcuts()
        self._refresh_positions_zoom_takeover_panel()

    def _sync_position_tree_selection(self, item_id: str) -> None:
        if self.position_tree is None or not self.position_tree.exists(item_id):
            return
        if self.position_tree.selection() == (item_id,):
            return
        self._position_selection_suppressed_item_id = item_id
        self._position_selection_syncing = True
        try:
            self.position_tree.selection_set(item_id)
        finally:
            self._position_selection_syncing = False
        try:
            self.position_tree.see(item_id)
        except Exception:
            pass

    def _sync_positions_zoom_selection(self, item_id: str) -> None:
        if self._positions_zoom_tree is None or not self._positions_zoom_tree.exists(item_id):
            return
        if self._positions_zoom_tree.selection() == (item_id,):
            return
        self._positions_zoom_selection_suppressed_item_id = item_id
        self._position_selection_syncing = True
        try:
            self._positions_zoom_tree.selection_set(item_id)
        finally:
            self._position_selection_syncing = False
        try:
            self._positions_zoom_tree.see(item_id)
        except Exception:
            pass

    def _on_positions_zoom_selected(self, *_: object) -> None:
        if self._positions_zoom_tree is None or self._positions_view_rendering or self._position_selection_syncing:
            return
        selection = self._positions_zoom_tree.selection()
        if not selection:
            self._positions_zoom_selection_suppressed_item_id = None
            self._positions_zoom_selected_item_id = None
            self._refresh_positions_zoom_detail()
            self._update_positions_zoom_search_shortcuts()
            return
        selected_item_id = selection[0]
        if self._positions_zoom_selection_suppressed_item_id == selected_item_id:
            self._positions_zoom_selection_suppressed_item_id = None
            self._positions_zoom_selected_item_id = selected_item_id
            return
        self._positions_zoom_selection_suppressed_item_id = None
        self._positions_zoom_selected_item_id = selected_item_id
        if self.position_tree is not None and self.position_tree.exists(selected_item_id):
            self._sync_position_tree_selection(selected_item_id)
            self._refresh_position_detail_panel()
        else:
            self._refresh_positions_zoom_detail()
        self._update_positions_zoom_search_shortcuts()

    def _selected_positions_zoom_option_for_search(self) -> OkxPosition | None:
        payload = None
        if self._positions_zoom_selected_item_id:
            payload = self._position_row_payloads.get(self._positions_zoom_selected_item_id)
        if payload is None:
            payload = self._selected_position_payload()
        if payload is None or payload.get("kind") != "position":
            return None
        position = payload.get("item")
        if isinstance(position, OkxPosition) and position.inst_type == "OPTION":
            return position
        return None

    def _sync_positions_zoom_columns_from_main(self) -> None:
        if self.position_tree is None or self._positions_zoom_tree is None:
            return
        zoom_tree = self._positions_zoom_tree
        columns = tuple(self.position_tree["columns"])
        approx_heading_columns = {
            "bid_usdt",
            "ask_usdt",
            "mark_usdt",
            "avg_usdt",
            "open_value_usdt",
            "upl_usdt",
            "realized_usdt",
            "theta_usdt",
        }
        compact_zoom_columns = {
            "time_value": 88,
            "time_value_usdt": 72,
            "intrinsic_value": 88,
            "intrinsic_usdt": 72,
            "bid_price": 72,
            "bid_usdt": 78,
            "ask_price": 72,
            "ask_usdt": 78,
        }
        heading_font_name = ttk.Style().lookup("Treeview.Heading", "font") or "TkDefaultFont"
        try:
            heading_font = tkfont.nametofont(heading_font_name)
        except Exception:
            heading_font = tkfont.nametofont("TkDefaultFont")
        for column_id in ("#0", *columns):
            heading = self.position_tree.heading(column_id)
            column = self.position_tree.column(column_id)
            width = column.get("width")
            stretch = column.get("stretch")
            if column_id in compact_zoom_columns:
                width = compact_zoom_columns[column_id]
                stretch = False
            elif column_id in approx_heading_columns:
                heading_text = str(heading.get("text", ""))
                width = max(heading_font.measure(heading_text) + 20, 84)
                stretch = False
            zoom_tree.heading(column_id, text=heading.get("text", ""))
            zoom_tree.column(
                column_id,
                width=width,
                anchor=column.get("anchor"),
                stretch=stretch,
            )

    def _selected_position_history_option_for_search(self) -> OkxPositionHistoryItem | None:
        if self._positions_zoom_position_history_tree is None:
            return None
        selection = self._positions_zoom_position_history_tree.selection()
        if not selection:
            return None
        index = _history_tree_index(selection[0], "ph")
        if index is None or index >= len(self._latest_position_history):
            return None
        item = self._latest_position_history[index]
        if item.inst_type == "OPTION":
            return item
        return None

    def _set_optional_button_enabled(self, button: ttk.Button | None, enabled: bool) -> None:
        if button is None:
            return
        if enabled:
            button.state(["!disabled"])
        else:
            button.state(["disabled"])

    def _update_positions_zoom_search_shortcuts(self) -> None:
        position = self._selected_positions_zoom_option_for_search()
        contract, expiry_prefix = _option_search_shortcuts(position.inst_id if position else "")
        enabled = bool(contract)
        self._set_optional_button_enabled(self._positions_zoom_apply_contract_button, enabled)
        self._set_optional_button_enabled(self._positions_zoom_apply_expiry_prefix_button, enabled)
        if not enabled:
            self._positions_zoom_option_search_hint_text.set("选中期权后，可一键带入合约或到期前缀。")
            return
        self._positions_zoom_option_search_hint_text.set(
            f"已选期权：{contract} | 快捷筛选：合约={contract} | 到期前缀={expiry_prefix}"
        )

    def _update_position_history_search_shortcuts(self) -> None:
        item = self._selected_position_history_option_for_search()
        contract, expiry_prefix = _option_search_shortcuts(item.inst_id if item else "")
        enabled = bool(contract)
        self._set_optional_button_enabled(self._positions_zoom_position_history_apply_contract_button, enabled)
        self._set_optional_button_enabled(self._positions_zoom_position_history_apply_expiry_prefix_button, enabled)
        if not enabled:
            self._positions_zoom_position_history_search_hint_text.set("选中历史期权后，可一键带入合约或到期前缀。")
            return
        self._positions_zoom_position_history_search_hint_text.set(
            f"已选历史期权：{contract} | 快捷筛选：合约={contract} | 到期前缀={expiry_prefix}"
        )

    def _selected_fill_history_option_for_search(self) -> OkxFillHistoryItem | None:
        if self._positions_zoom_fills_tree is None:
            return None
        selection = self._positions_zoom_fills_tree.selection()
        if not selection:
            return None
        index = _history_tree_index(selection[0], "fill")
        if index is None or index >= len(self._latest_fill_history):
            return None
        item = self._latest_fill_history[index]
        if item.inst_type == "OPTION":
            return item
        return None

    def _update_fill_history_search_shortcuts(self) -> None:
        item = self._selected_fill_history_option_for_search()
        contract, expiry_prefix = _option_search_shortcuts(item.inst_id if item else "")
        enabled = bool(contract)
        self._set_optional_button_enabled(self._positions_zoom_fills_apply_contract_button, enabled)
        self._set_optional_button_enabled(self._positions_zoom_fills_apply_expiry_prefix_button, enabled)
        if not enabled:
            self._positions_zoom_fill_history_search_hint_text.set("选中历史期权成交后，可一键带入合约或到期前缀。")
            return
        self._positions_zoom_fill_history_search_hint_text.set(
            f"已选历史期权成交：{contract} | 快捷筛选：合约={contract} | 到期前缀={expiry_prefix}"
        )

    def apply_selected_option_to_position_search(self) -> None:
        position = self._selected_positions_zoom_option_for_search()
        contract, _ = _option_search_shortcuts(position.inst_id if position else "")
        if not contract:
            messagebox.showinfo("快捷筛选", "请先在当前持仓里选中一条期权合约。")
            return
        self.positions_zoom_keyword.set(contract)
        self._render_positions_view()

    def apply_selected_option_expiry_prefix_to_position_search(self) -> None:
        position = self._selected_positions_zoom_option_for_search()
        _, expiry_prefix = _option_search_shortcuts(position.inst_id if position else "")
        if not expiry_prefix:
            messagebox.showinfo("快捷筛选", "请先在当前持仓里选中一条期权合约。")
            return
        self.positions_zoom_keyword.set(expiry_prefix)
        self._render_positions_view()

    def apply_selected_option_to_position_history_search(self) -> None:
        item = self._selected_position_history_option_for_search()
        contract, _ = _option_search_shortcuts(item.inst_id if item else "")
        if not contract:
            messagebox.showinfo("快捷筛选", "请先在历史仓位里选中一条期权合约。")
            return
        self.position_history_keyword.set(contract)
        self._render_positions_zoom_position_history_view()

    def apply_selected_option_expiry_prefix_to_position_history_search(self) -> None:
        item = self._selected_position_history_option_for_search()
        _, expiry_prefix = _option_search_shortcuts(item.inst_id if item else "")
        if not expiry_prefix:
            messagebox.showinfo("快捷筛选", "请先在历史仓位里选中一条期权合约。")
            return
        self.position_history_expiry_prefix_filter.set(expiry_prefix.rstrip("-").split("-")[-1])
        self._render_positions_zoom_position_history_view()

    def apply_selected_option_to_fill_history_search(self) -> None:
        item = self._selected_fill_history_option_for_search()
        contract, _ = _option_search_shortcuts(item.inst_id if item else "")
        if not contract:
            messagebox.showinfo("快捷筛选", "请先在历史成交里选中一条期权合约。")
            return
        self.fill_history_keyword.set(contract)
        self._render_positions_zoom_fills_view()

    def apply_selected_option_expiry_prefix_to_fill_history_search(self) -> None:
        item = self._selected_fill_history_option_for_search()
        _, expiry_prefix = _option_search_shortcuts(item.inst_id if item else "")
        if not expiry_prefix:
            messagebox.showinfo("快捷筛选", "请先在历史成交里选中一条期权合约。")
            return
        self.fill_history_expiry_prefix_filter.set(expiry_prefix.rstrip("-").split("-")[-1])
        self._render_positions_zoom_fills_view()

    def _refresh_positions_zoom_detail(self) -> None:
        if self._positions_zoom_detail is None:
            return
        payload = None
        if self._positions_zoom_selected_item_id:
            payload = self._position_row_payloads.get(self._positions_zoom_selected_item_id)
        if payload is None:
            self._set_readonly_text(self._positions_zoom_detail, self.position_detail_text.get())
            return
        if payload["kind"] == "position":
            position = payload["item"]
            if isinstance(position, OkxPosition):
                self._set_readonly_text(
                    self._positions_zoom_detail,
                    _build_position_detail_text(
                        position,
                        self._upl_usdt_prices,
                        self._position_instruments,
                        note=self._current_position_note_text(position),
                    ),
                )
                return
        label = payload["label"]
        positions = payload["item"]
        metrics = payload["metrics"]
        if isinstance(label, str) and isinstance(positions, list) and isinstance(metrics, dict):
            self._set_readonly_text(
                self._positions_zoom_detail,
                _build_group_detail_text(
                    label,
                    positions,
                    metrics,
                    self._upl_usdt_prices,
                    self._position_instruments,
                ),
            )
            return
        self._set_readonly_text(self._positions_zoom_detail, self.position_detail_text.get())

    def refresh_order_views(self) -> None:
        credentials = self._current_credentials_or_none()
        if credentials is None:
            _reset_refresh_health(self._pending_orders_refresh_health)
            _reset_refresh_health(self._order_history_refresh_health)
            self._positions_zoom_pending_orders_base_summary = "未配置 API 凭证，无法读取当前委托。"
            self._positions_zoom_pending_orders_summary_text.set(self._positions_zoom_pending_orders_base_summary)
            self._positions_zoom_order_history_base_summary = "未配置 API 凭证，无法读取历史委托。"
            self._positions_zoom_order_history_summary_text.set(self._positions_zoom_order_history_base_summary)
            self._latest_pending_orders = []
            self._latest_order_history = []
            self._render_pending_orders_view()
            self._render_order_history_view()
            self._refresh_all_refresh_badges()
            return
        environment = self._positions_effective_environment or ENV_OPTIONS[self.environment_label.get()]
        self._start_pending_orders_refresh(credentials, environment)
        profile_name = credentials.profile_name or self._current_credential_profile()
        self._start_order_history_refresh(credentials, environment, profile_name)
        self._start_fill_history_refresh(credentials, environment, profile_name)

    def refresh_pending_orders(self) -> None:
        credentials = self._current_credentials_or_none()
        if credentials is None:
            _reset_refresh_health(self._pending_orders_refresh_health)
            self._positions_zoom_pending_orders_base_summary = "未配置 API 凭证，无法读取当前委托。"
            self._positions_zoom_pending_orders_summary_text.set(self._positions_zoom_pending_orders_base_summary)
            self._latest_pending_orders = []
            self._render_pending_orders_view()
            self._refresh_all_refresh_badges()
            return
        environment = self._positions_effective_environment or ENV_OPTIONS[self.environment_label.get()]
        self._start_pending_orders_refresh(credentials, environment)

    def refresh_order_history(self) -> None:
        credentials = self._current_credentials_or_none()
        if credentials is None:
            _reset_refresh_health(self._order_history_refresh_health)
            self._positions_zoom_order_history_base_summary = "未配置 API 凭证，无法读取历史委托。"
            self._positions_zoom_order_history_summary_text.set(self._positions_zoom_order_history_base_summary)
            self._latest_order_history = []
            self._render_order_history_view()
            self._refresh_all_refresh_badges()
            return
        environment = self._positions_effective_environment or ENV_OPTIONS[self.environment_label.get()]
        profile_name = credentials.profile_name or self._current_credential_profile()
        self._start_order_history_refresh(credentials, environment, profile_name)
        self._start_fill_history_refresh(credentials, environment, profile_name)

    def _active_history_scope(self) -> tuple[str, str]:
        credentials = self._current_credentials_or_none()
        profile_name = credentials.profile_name if credentials and credentials.profile_name else self._current_credential_profile()
        environment = self._positions_effective_environment or ENV_OPTIONS[self.environment_label.get()]
        return profile_name or DEFAULT_CREDENTIAL_PROFILE_NAME, environment

    def _refresh_positions_zoom_all(self) -> None:
        """持仓大窗「刷新」：持仓 + 下方各历史/委托标签页一并从服务端同步。"""
        self.refresh_positions()
        self.sync_positions_zoom_data()

    def _refresh_account_views_after_credential_profile_switch(self) -> None:
        """切换 API 配置后刷新主界面与持仓大窗共用的委托/历史缓存与远端数据。"""
        if getattr(self, "position_tree", None) is None:
            return
        try:
            self._load_local_history_cache()
        except Exception:
            pass
        try:
            self._refresh_positions_zoom_all()
        except Exception:
            pass
        account_win = getattr(self, "_account_info_window", None)
        if account_win is not None:
            try:
                if account_win.winfo_exists():
                    self.refresh_account_dashboard()
            except Exception:
                pass
        zoom_win = getattr(self, "_positions_zoom_window", None)
        if zoom_win is not None:
            try:
                if zoom_win.winfo_exists():
                    self._schedule_positions_zoom_sync(15)
            except Exception:
                pass

    def _load_local_history_cache(self) -> None:
        profile_name, environment = self._active_history_scope()
        order_records = load_history_cache_records("orders", profile_name, environment)
        fill_records = load_history_cache_records("fills", profile_name, environment)
        position_records = load_history_cache_records("positions", profile_name, environment)
        self._latest_order_history = [item for record in order_records if (item := _order_item_from_cache(record)) is not None]
        self._latest_fill_history = [item for record in fill_records if (item := _fill_item_from_cache(record)) is not None]
        self._latest_position_history = [
            item for record in position_records if (item := _position_history_item_from_cache(record)) is not None
        ]
        order_fee_ccys: set[str] = {
            o.fee_currency.strip().upper()
            for o in self._latest_order_history
            if o.fee is not None and o.fee_currency and str(o.fee_currency).strip()
        }
        self._order_history_usdt_prices = (
            _build_usdt_price_snapshot(self.client, order_fee_ccys) if order_fee_ccys else {}
        )
        fill_ccys_local: set[str] = set()
        for fill in self._latest_fill_history:
            if fill.fill_fee is not None and fill.fee_currency and str(fill.fee_currency).strip():
                fill_ccys_local.add(fill.fee_currency.strip().upper())
            if fill.pnl is not None:
                fill_ccys_local.add(_infer_fill_history_pnl_currency(fill))
        self._fill_history_usdt_prices = (
            _build_usdt_price_snapshot(self.client, fill_ccys_local) if fill_ccys_local else {}
        )
        pos_ccys_local: set[str] = set()
        for it in self._latest_position_history:
            if it.realized_pnl is not None:
                pos_ccys_local.add(_infer_position_history_pnl_currency(it))
            if it.pnl is not None:
                pos_ccys_local.add(_infer_position_history_pnl_currency(it))
            if it.fee is not None and it.fee_currency and str(it.fee_currency).strip():
                pos_ccys_local.add(it.fee_currency.strip().upper())
        self._position_history_usdt_prices = (
            _build_usdt_price_snapshot(self.client, pos_ccys_local) if pos_ccys_local else {}
        )
        self._fills_history_from_local_only = True
        self._fills_history_last_refresh_at = None
        self._positions_zoom_order_history_base_summary = f"历史委托：{len(self._latest_order_history)} 条 | 本地缓存"
        self._positions_zoom_fills_summary_text.set(f"历史成交：{len(self._latest_fill_history)} 条 | 本地缓存")
        self._positions_zoom_position_history_base_summary = f"历史仓位：{len(self._latest_position_history)} 条 | 本地缓存"
        self._positions_zoom_position_history_summary_text.set(self._positions_zoom_position_history_base_summary)
        self._render_order_history_view()
        self._render_positions_zoom_fills_view()
        self._render_positions_zoom_position_history_view()

    def _pending_order_tree_for_view(self, view_name: str | None = None) -> ttk.Treeview | None:
        if view_name == "account_info":
            return self._account_info_pending_orders_tree
        if view_name == "positions_zoom":
            return self._positions_zoom_pending_orders_tree
        if self._positions_zoom_pending_orders_tree is not None and _widget_exists(self._positions_zoom_pending_orders_tree):
            return self._positions_zoom_pending_orders_tree
        if self._account_info_pending_orders_tree is not None and _widget_exists(self._account_info_pending_orders_tree):
            return self._account_info_pending_orders_tree
        return None

    def _pending_order_parent_for_view(self, view_name: str | None = None):
        if view_name == "account_info" and _widget_exists(self._account_info_window):
            return self._account_info_window
        if view_name == "positions_zoom" and _widget_exists(self._positions_zoom_window):
            return self._positions_zoom_window
        if _widget_exists(self._positions_zoom_window):
            return self._positions_zoom_window
        if _widget_exists(self._account_info_window):
            return self._account_info_window
        return self.root

    def _create_refresh_badge(self, parent, textvariable: StringVar, store: list[Label]) -> Label:
        badge = Label(
            parent,
            textvariable=textvariable,
            font=("Microsoft YaHei UI", 9, "bold"),
            padx=8,
            pady=2,
            bd=0,
            relief="flat",
        )
        store.append(badge)
        return badge

    def _apply_refresh_badge_state(
        self,
        store: list[Label],
        textvariable: StringVar,
        state: RefreshHealthState,
    ) -> None:
        textvariable.set(_refresh_indicator_badge_text(state))
        palette = REFRESH_BADGE_PALETTES[_refresh_indicator_level(state)]
        alive: list[Label] = []
        for badge in store:
            if not _widget_exists(badge):
                continue
            badge.configure(
                bg=palette["bg"],
                fg=palette["fg"],
                activebackground=palette["bg"],
                activeforeground=palette["fg"],
            )
            alive.append(badge)
        store[:] = alive

    def _refresh_all_refresh_badges(self) -> None:
        self._apply_refresh_badge_state(
            self._positions_refresh_badges,
            self._positions_refresh_badge_text,
            self._positions_refresh_health,
        )
        self._apply_refresh_badge_state(
            self._account_info_refresh_badges,
            self._account_info_refresh_badge_text,
            self._account_info_refresh_health,
        )
        self._apply_refresh_badge_state(
            self._pending_orders_refresh_badges,
            self._pending_orders_refresh_badge_text,
            self._pending_orders_refresh_health,
        )
        self._apply_refresh_badge_state(
            self._order_history_refresh_badges,
            self._order_history_refresh_badge_text,
            self._order_history_refresh_health,
        )

    def _guard_live_action_against_stale_cache(
        self,
        state: RefreshHealthState,
        *,
        action_label: str,
        data_label: str,
        parent,
        refresh_callback=None,
    ) -> bool:
        if not _refresh_health_is_stale(state):
            return True
        if callable(refresh_callback):
            try:
                refresh_callback()
            except Exception:
                pass
        messagebox.showwarning(
            "数据可能已过期",
            (
                f"当前{data_label}已经连续刷新失败，为避免基于旧缓存执行{action_label}，本次操作已拦截。\n\n"
                f"{_describe_refresh_health(state)}\n\n"
                "系统已尝试重新发起刷新，请等待下一次刷新成功后再重试。"
            ),
            parent=parent,
        )
        return False

    def cancel_selected_pending_order(self, view_name: str | None = None) -> None:
        parent = self._pending_order_parent_for_view(view_name)
        if self._pending_order_canceling:
            messagebox.showinfo("撤单中", "当前已有一笔撤单请求在处理中，请稍等。", parent=parent)
            return
        item = self._selected_pending_order_item(view_name)
        if item is None:
            messagebox.showinfo("撤单", "请先在当前委托里选中一条要撤销的委托。", parent=parent)
            return
        owner_label = _trade_order_program_owner_label(item)
        if owner_label is None:
            messagebox.showinfo(
                "撤单限制",
                "当前只允许撤销本程序发出的委托。\n这条委托没有识别到本程序 clOrdId 规则，已拦截。",
                parent=parent,
            )
            return
        credentials = self._current_credentials_or_none()
        if credentials is None:
            messagebox.showinfo("撤单", "当前未配置 API 凭证，无法发起撤单。", parent=parent)
            return
        if not self._guard_live_action_against_stale_cache(
            self._pending_orders_refresh_health,
            action_label="撤单",
            data_label="当前委托",
            parent=parent,
            refresh_callback=self.refresh_pending_orders,
        ):
            return
        cancel_id = _trade_order_cancel_reference(item)
        if not cancel_id:
            messagebox.showinfo("撤单", "这条委托缺少可用订单 ID，暂时无法撤单。", parent=parent)
            return
        confirm_message = (
            f"确认撤销这条{item.source_label or '委托'}吗？\n\n"
            f"程序来源：{owner_label}\n"
            f"合约：{item.inst_id or '-'}\n"
            f"方向：{_format_history_side(item.side, item.pos_side)}\n"
            f"状态：{_format_trade_order_state(item.state)}\n"
            f"标识：{cancel_id}"
        )
        if not messagebox.askyesno("撤单确认", confirm_message, parent=parent):
            return
        environment = self._positions_effective_environment or ENV_OPTIONS[self.environment_label.get()]
        self._pending_order_canceling = True
        self._positions_zoom_pending_orders_summary_text.set(
            f"正在撤单：{item.source_label or '委托'} | {item.inst_id or '-'} | {cancel_id}"
        )
        threading.Thread(
            target=self._cancel_selected_pending_order_worker,
            args=(credentials, environment, item, owner_label, view_name),
            daemon=True,
        ).start()

    def cancel_filtered_pending_orders(self, view_name: str | None = None) -> None:
        parent = self._pending_order_parent_for_view(view_name)
        if self._pending_order_canceling:
            messagebox.showinfo("撤单中", "当前已有撤单请求在处理中，请稍等。", parent=parent)
            return
        filtered_items = [item for _, item in self._filtered_pending_order_items()]
        if not filtered_items:
            messagebox.showinfo("批量撤单", "当前筛选结果为空，没有可撤的委托。", parent=parent)
            return
        cancelable_items = [
            item for item in filtered_items if _trade_order_program_owner_label(item) is not None and _trade_order_cancel_reference(item)
        ]
        skipped_manual = sum(1 for item in filtered_items if _trade_order_program_owner_label(item) is None)
        skipped_missing_id = sum(
            1 for item in filtered_items if _trade_order_program_owner_label(item) is not None and not _trade_order_cancel_reference(item)
        )
        if not cancelable_items:
            messagebox.showinfo(
                "批量撤单",
                "当前筛选结果里没有可识别为本程序发出的可撤委托。\n不会撤销手工单或来源不明的委托。",
                parent=parent,
            )
            return
        filter_warning = ""
        if not _trade_order_filter_enabled(
            POSITION_TYPE_OPTIONS.get(self.pending_order_type_filter.get(), ""),
            ORDER_SOURCE_FILTER_OPTIONS.get(self.pending_order_source_filter.get(), ""),
            ORDER_STATE_FILTER_OPTIONS.get(self.pending_order_state_filter.get(), ""),
            self.pending_order_asset_filter.get(),
            self.pending_order_expiry_prefix_filter.get(),
            self.pending_order_keyword.get(),
        ):
            filter_warning = "当前未启用任何筛选，本次会按当前页全部可识别程序单执行。\n\n"
        confirm_message = (
            f"{filter_warning}"
            f"确认批量撤销当前筛选结果中的程序委托吗？\n\n"
            f"筛选结果总数：{len(filtered_items)}\n"
            f"将尝试撤销：{len(cancelable_items)}\n"
            f"跳过非程序单：{skipped_manual}\n"
            f"跳过缺少ID：{skipped_missing_id}"
        )
        if not messagebox.askyesno("批量撤单确认", confirm_message, parent=parent):
            return
        credentials = self._current_credentials_or_none()
        if credentials is None:
            messagebox.showinfo("批量撤单", "当前未配置 API 凭证，无法发起批量撤单。", parent=parent)
            return
        if not self._guard_live_action_against_stale_cache(
            self._pending_orders_refresh_health,
            action_label="批量撤单",
            data_label="当前委托",
            parent=parent,
            refresh_callback=self.refresh_pending_orders,
        ):
            return
        environment = self._positions_effective_environment or ENV_OPTIONS[self.environment_label.get()]
        self._pending_order_canceling = True
        self._positions_zoom_pending_orders_summary_text.set(
            f"正在批量撤单：准备处理 {len(cancelable_items)} / {len(filtered_items)} 条"
        )
        threading.Thread(
            target=self._cancel_filtered_pending_orders_worker,
            args=(credentials, environment, cancelable_items, skipped_manual, skipped_missing_id, view_name),
            daemon=True,
        ).start()

    def _cancel_selected_pending_order_worker(
        self,
        credentials: Credentials,
        environment: str,
        item: OkxTradeOrderItem,
        owner_label: str,
        view_name: str | None,
    ) -> None:
        try:
            result = self._cancel_pending_order_request(credentials, environment=environment, item=item)
            note = None
            effective_environment = environment
        except Exception as exc:
            message = str(exc)
            if "50101" in message and "current environment" in message:
                alternate = "live" if environment == "demo" else "demo"
                try:
                    result = self._cancel_pending_order_request(credentials, environment=alternate, item=item)
                    note = f"撤单自动切换到 {'实盘' if alternate == 'live' else '模拟'} 环境执行。"
                    effective_environment = alternate
                except Exception:
                    self.root.after(
                        0,
                        lambda: self._apply_pending_order_cancel_error(item, message, owner_label, environment, view_name),
                    )
                    return
            else:
                self.root.after(
                    0,
                    lambda: self._apply_pending_order_cancel_error(item, message, owner_label, environment, view_name),
                )
                return
        self.root.after(
            0,
            lambda: self._apply_pending_order_cancel_result(item, result, owner_label, note, effective_environment, view_name),
        )

    def _cancel_filtered_pending_orders_worker(
        self,
        credentials: Credentials,
        environment: str,
        items: list[OkxTradeOrderItem],
        skipped_manual: int,
        skipped_missing_id: int,
        view_name: str | None,
    ) -> None:
        success_items: list[tuple[OkxTradeOrderItem, OkxOrderResult, str]] = []
        failed_items: list[tuple[OkxTradeOrderItem, str, str]] = []
        active_environment = environment
        note: str | None = None
        switched = False
        for item in items:
            owner_label = _trade_order_program_owner_label(item) or "本程序委托"
            try:
                result = self._cancel_pending_order_request(credentials, environment=active_environment, item=item)
            except Exception as exc:
                message = str(exc)
                if not switched and "50101" in message and "current environment" in message:
                    alternate = "live" if active_environment == "demo" else "demo"
                    try:
                        result = self._cancel_pending_order_request(credentials, environment=alternate, item=item)
                        active_environment = alternate
                        note = f"批量撤单自动切换到 {'实盘' if alternate == 'live' else '模拟'} 环境执行。"
                        switched = True
                    except Exception as retry_exc:
                        failed_items.append((item, owner_label, str(retry_exc)))
                        continue
                else:
                    failed_items.append((item, owner_label, message))
                    continue
            success_items.append((item, result, owner_label))
        self.root.after(
            0,
            lambda: self._apply_bulk_pending_order_cancel_result(
                success_items,
                failed_items,
                skipped_manual,
                skipped_missing_id,
                note,
                active_environment,
                view_name,
            ),
        )

    def _cancel_pending_order_request(
        self,
        credentials: Credentials,
        *,
        environment: str,
        item: OkxTradeOrderItem,
    ) -> OkxOrderResult:
        if item.source_kind == "algo":
            return self.client.cancel_algo_order(
                credentials,
                environment=environment,
                inst_id=item.inst_id,
                algo_id=item.algo_id or None,
                algo_cl_ord_id=item.algo_client_order_id or item.client_order_id or None,
            )
        return self.client.cancel_order_by_id(
            credentials,
            environment=environment,
            inst_id=item.inst_id,
            ord_id=item.order_id or None,
            cl_ord_id=item.client_order_id or None,
        )

    def _apply_pending_order_cancel_result(
        self,
        item: OkxTradeOrderItem,
        result: OkxOrderResult,
        owner_label: str,
        note: str | None = None,
        effective_environment: str | None = None,
        view_name: str | None = None,
    ) -> None:
        self._pending_order_canceling = False
        if effective_environment:
            self._positions_effective_environment = effective_environment
        cancel_id = _trade_order_cancel_reference(item) or result.ord_id or result.cl_ord_id or "-"
        summary = f"撤单请求已提交：{item.inst_id or '-'} | {cancel_id}"
        if note:
            summary = f"{summary} | {note}"
        self._positions_zoom_pending_orders_base_summary = summary
        self._positions_zoom_pending_orders_summary_text.set(summary)
        self._enqueue_log(
            "实盘撤单"
            f" | 模式=单笔"
            f" | 环境={effective_environment or '-'}"
            f" | 程序来源={owner_label}"
            f" | 来源={item.source_label or '-'}"
            f" | 合约={item.inst_id or '-'}"
            f" | 标识={cancel_id}"
            f" | 结果=已提交"
            f" | sCode={result.s_code}"
            f" | sMsg={result.s_msg or 'accepted'}"
        )
        parent = self._pending_order_parent_for_view(view_name)
        messagebox.showinfo(
            "撤单结果",
            (
                "撤单请求已提交。\n\n"
                f"程序来源：{owner_label}\n"
                f"来源：{item.source_label or '-'}\n"
                f"合约：{item.inst_id or '-'}\n"
                f"标识：{cancel_id}\n"
                f"返回：sCode={result.s_code} | sMsg={result.s_msg or 'accepted'}"
            ),
            parent=parent,
        )
        self.refresh_pending_orders()
        self.refresh_order_history()

    def _apply_pending_order_cancel_error(
        self,
        item: OkxTradeOrderItem,
        message: str,
        owner_label: str,
        environment: str,
        view_name: str | None = None,
    ) -> None:
        self._pending_order_canceling = False
        friendly_message = _format_network_error_message(message)
        self._positions_zoom_pending_orders_base_summary = f"撤单失败：{friendly_message}"
        self._positions_zoom_pending_orders_summary_text.set(self._positions_zoom_pending_orders_base_summary)
        self._enqueue_log(
            "实盘撤单"
            f" | 模式=单笔"
            f" | 环境={environment}"
            f" | 程序来源={owner_label}"
            f" | 来源={item.source_label or '-'}"
            f" | 合约={item.inst_id or '-'}"
            f" | 标识={_trade_order_cancel_reference(item) or '-'}"
            f" | 结果=失败"
            f" | 原因={friendly_message}"
        )
        parent = self._pending_order_parent_for_view(view_name)
        messagebox.showerror(
            "撤单失败",
            (
                f"{item.source_label or '委托'} 撤单失败。\n\n"
                f"程序来源：{owner_label}\n"
                f"合约：{item.inst_id or '-'}\n"
                f"原因：{friendly_message}"
            ),
            parent=parent,
        )

    def _apply_bulk_pending_order_cancel_result(
        self,
        success_items: list[tuple[OkxTradeOrderItem, OkxOrderResult, str]],
        failed_items: list[tuple[OkxTradeOrderItem, str, str]],
        skipped_manual: int,
        skipped_missing_id: int,
        note: str | None = None,
        effective_environment: str | None = None,
        view_name: str | None = None,
    ) -> None:
        self._pending_order_canceling = False
        if effective_environment:
            self._positions_effective_environment = effective_environment
        success_count = len(success_items)
        failed_count = len(failed_items)
        summary = (
            f"批量撤单完成：提交 {success_count} 条"
            f" | 失败 {failed_count} 条"
            f" | 跳过非程序单 {skipped_manual} 条"
            f" | 跳过缺少ID {skipped_missing_id} 条"
        )
        if note:
            summary = f"{summary} | {note}"
        self._positions_zoom_pending_orders_base_summary = summary
        self._positions_zoom_pending_orders_summary_text.set(summary)
        self._enqueue_log(
            "实盘撤单"
            f" | 模式=批量"
            f" | 环境={effective_environment or '-'}"
            f" | 提交={success_count}"
            f" | 失败={failed_count}"
            f" | 跳过非程序单={skipped_manual}"
            f" | 跳过缺少ID={skipped_missing_id}"
            f"{' | 备注=' + note if note else ''}"
        )
        for item, result, owner_label in success_items:
            self._enqueue_log(
                "实盘撤单明细"
                f" | 结果=已提交"
                f" | 程序来源={owner_label}"
                f" | 来源={item.source_label or '-'}"
                f" | 合约={item.inst_id or '-'}"
                f" | 标识={_trade_order_cancel_reference(item) or result.ord_id or result.cl_ord_id or '-'}"
                f" | sCode={result.s_code}"
                f" | sMsg={result.s_msg or 'accepted'}"
            )
        for item, owner_label, message in failed_items:
            self._enqueue_log(
                "实盘撤单明细"
                f" | 结果=失败"
                f" | 程序来源={owner_label}"
                f" | 来源={item.source_label or '-'}"
                f" | 合约={item.inst_id or '-'}"
                f" | 标识={_trade_order_cancel_reference(item) or '-'}"
                f" | 原因={_format_network_error_message(message)}"
            )
        parent = self._pending_order_parent_for_view(view_name)
        messagebox.showinfo(
            "批量撤单结果",
            (
                f"{summary}\n\n"
                f"已提交：{success_count}\n"
                f"失败：{failed_count}\n"
                f"跳过非程序单：{skipped_manual}\n"
                f"跳过缺少ID：{skipped_missing_id}"
            ),
            parent=parent,
        )
        self.refresh_pending_orders()
        self.refresh_order_history()

    def _start_pending_orders_refresh(self, credentials: Credentials, environment: str) -> None:
        if self._pending_orders_refreshing:
            self._pending_orders_refresh_queue = (credentials, environment)
            return
        self._pending_orders_refreshing = True
        self._positions_zoom_pending_orders_summary_text.set("正在刷新当前委托...")
        threading.Thread(
            target=self._refresh_pending_orders_worker,
            args=(credentials, environment),
            daemon=True,
        ).start()

    def _drain_pending_orders_refresh_queue(self) -> None:
        pending = self._pending_orders_refresh_queue
        self._pending_orders_refresh_queue = None
        if pending is None:
            return
        cred, env = pending
        self._start_pending_orders_refresh(cred, env)

    def _start_order_history_refresh(self, credentials: Credentials, environment: str, profile_name: str) -> None:
        if self._order_history_refreshing:
            self._order_history_refresh_request = (credentials, environment, profile_name)
            return
        self._order_history_refreshing = True
        self._order_history_refresh_request = None
        self._positions_zoom_order_history_summary_text.set("正在同步历史委托...")
        threading.Thread(
            target=self._refresh_order_history_worker,
            args=(credentials, environment, profile_name),
            daemon=True,
        ).start()

    def _refresh_pending_orders_worker(self, credentials: Credentials, environment: str) -> None:
        try:
            items = self.client.get_pending_orders(credentials, environment=environment, limit=200)
            instruments = _build_history_instrument_map(self.client, [item.inst_id for item in items])
            note = None
            effective_environment = environment
        except Exception as exc:
            message = str(exc)
            if "50101" in message and "current environment" in message:
                alternate = "live" if environment == "demo" else "demo"
                try:
                    items = self.client.get_pending_orders(credentials, environment=alternate, limit=200)
                    instruments = _build_history_instrument_map(self.client, [item.inst_id for item in items])
                    note = f"委托数据自动切换到 {'实盘' if alternate == 'live' else '模拟'} 环境读取。"
                    effective_environment = alternate
                except Exception:
                    self.root.after(0, lambda: self._apply_pending_orders_error(message))
                    return
            else:
                self.root.after(0, lambda: self._apply_pending_orders_error(message))
                return
        self.root.after(0, lambda: self._apply_pending_orders(items, instruments, note, effective_environment))

    def _refresh_order_history_worker(self, credentials: Credentials, environment: str, profile_name: str) -> None:
        try:
            items = self.client.get_order_history(credentials, environment=environment, limit=200)
            instruments = _safe_build_history_instrument_map(self.client, [item.inst_id for item in items])
            note = None
            effective_environment = environment
        except Exception as exc:
            message = str(exc)
            if "50101" in message and "current environment" in message:
                alternate = "live" if environment == "demo" else "demo"
                try:
                    items = self.client.get_order_history(credentials, environment=alternate, limit=200)
                    instruments = _safe_build_history_instrument_map(self.client, [item.inst_id for item in items])
                    note = f"委托数据自动切换到 {'实盘' if alternate == 'live' else '模拟'} 环境读取。"
                    effective_environment = alternate
                except Exception:
                    self.root.after(0, lambda m=message: self._apply_order_history_error(m))
                    return
            else:
                self.root.after(0, lambda m=message: self._apply_order_history_error(m))
                return
        self.root.after(
            0,
            lambda i=items, ins=instruments, n=note, env=effective_environment, prof=profile_name: self._apply_order_history(
                i, ins, n, env, prof
            ),
        )

    def _apply_pending_orders(
        self,
        items: list[OkxTradeOrderItem],
        instruments: dict[str, Instrument],
        note: str | None = None,
        effective_environment: str | None = None,
    ) -> None:
        self._pending_orders_refreshing = False
        self._latest_pending_orders = list(items)
        self._pending_order_instruments = dict(instruments)
        self._pending_orders_last_refresh_at = datetime.now()
        _mark_refresh_health_success(self._pending_orders_refresh_health, at=self._pending_orders_last_refresh_at)
        self._refresh_all_refresh_badges()
        if effective_environment:
            self._positions_effective_environment = effective_environment
        timestamp = self._pending_orders_last_refresh_at.strftime("%H:%M:%S")
        summary = f"当前委托：{len(items)} 条 | 最近刷新：{timestamp}"
        if note:
            summary = f"{summary} | {note}"
        self._positions_zoom_pending_orders_base_summary = summary
        self._positions_zoom_pending_orders_summary_text.set(summary)
        self._render_pending_orders_view()
        self._drain_pending_orders_refresh_queue()

    def _apply_order_history(
        self,
        items: list[OkxTradeOrderItem],
        instruments: dict[str, Instrument],
        note: str | None = None,
        effective_environment: str | None = None,
        profile_name: str | None = None,
    ) -> None:
        self._order_history_refreshing = False
        try:
            active_profile = (profile_name or self._current_credential_profile()).strip() or DEFAULT_CREDENTIAL_PROFILE_NAME
            active_environment = effective_environment or self._positions_effective_environment or ENV_OPTIONS[self.environment_label.get()]
            local_records = load_history_cache_records("orders", active_profile, active_environment)
            merged_records = _merge_history_cache_records(
                local_records=local_records,
                remote_records=[_serialize_history_item(item) for item in items],
                dedup_fields=("source_kind", "order_id", "algo_id", "client_order_id", "algo_client_order_id", "inst_id", "created_time"),
            )
            save_history_cache_records("orders", active_profile, active_environment, merged_records)
            self._latest_order_history = [item for record in merged_records if (item := _order_item_from_cache(record)) is not None]
            self._latest_order_history.sort(key=lambda it: (it.update_time or it.created_time or 0), reverse=True)
            self._order_history_instruments = dict(instruments)
            fee_ccys: set[str] = {
                o.fee_currency.strip().upper()
                for o in self._latest_order_history
                if o.fee is not None and o.fee_currency and str(o.fee_currency).strip()
            }
            self._order_history_usdt_prices = (
                _build_usdt_price_snapshot(self.client, fee_ccys) if fee_ccys else {}
            )
            self._order_history_last_refresh_at = datetime.now()
            _mark_refresh_health_success(self._order_history_refresh_health, at=self._order_history_last_refresh_at)
            self._refresh_all_refresh_badges()
            if effective_environment:
                self._positions_effective_environment = effective_environment
            timestamp = self._order_history_last_refresh_at.strftime("%H:%M:%S")
            summary = f"历史委托：{len(self._latest_order_history)} 条 | 最近同步：{timestamp}"
            if not items:
                summary = f"{summary} | 本次 API 返回 0 条（请核对模拟/实盘环境与 API 权限）"
            if note:
                summary = f"{summary} | {note}"
            self._positions_zoom_order_history_base_summary = summary
            self._positions_zoom_order_history_summary_text.set(summary)
            self._render_order_history_view()
        except Exception as exc:
            self._apply_order_history_error(str(exc))
            return
        pending_order_refresh = self._order_history_refresh_request
        self._order_history_refresh_request = None
        if pending_order_refresh is not None:
            pcred, penv, pprof = pending_order_refresh
            self.root.after(0, lambda c=pcred, e=penv, p=pprof: self._start_order_history_refresh(c, e, p))

    def _apply_pending_orders_error(self, message: str) -> None:
        self._pending_orders_refreshing = False
        friendly_message = _format_network_error_message(message)
        _mark_refresh_health_failure(self._pending_orders_refresh_health, friendly_message)
        self._refresh_all_refresh_badges()
        suffix = _format_refresh_health_suffix(self._pending_orders_refresh_health)
        has_previous_items = bool(self._latest_pending_orders) or self._pending_orders_last_refresh_at is not None
        if has_previous_items:
            summary = f"当前委托刷新失败，继续显示上一份缓存：{friendly_message}{suffix}"
        else:
            summary = f"当前委托读取失败：{friendly_message}{suffix}"
        self._positions_zoom_pending_orders_base_summary = summary
        self._positions_zoom_pending_orders_summary_text.set(self._positions_zoom_pending_orders_base_summary)
        self._enqueue_log(summary)
        self._drain_pending_orders_refresh_queue()

    def _apply_order_history_error(self, message: str) -> None:
        self._order_history_refreshing = False
        self._order_history_refresh_request = None
        friendly_message = _format_network_error_message(message)
        _mark_refresh_health_failure(self._order_history_refresh_health, friendly_message)
        self._refresh_all_refresh_badges()
        suffix = _format_refresh_health_suffix(self._order_history_refresh_health)
        has_previous_items = bool(self._latest_order_history) or self._order_history_last_refresh_at is not None
        if has_previous_items:
            summary = f"历史委托刷新失败，继续显示上一份缓存：{friendly_message}{suffix}"
        else:
            summary = f"历史委托读取失败：{friendly_message}{suffix}"
        self._positions_zoom_order_history_base_summary = summary
        self._positions_zoom_order_history_summary_text.set(self._positions_zoom_order_history_base_summary)
        self._enqueue_log(summary)

    def _on_pending_order_filter_changed(self, *_: object) -> None:
        self._render_pending_orders_view()

    def reset_pending_order_filters(self) -> None:
        self.pending_order_type_filter.set("全部类型")
        self.pending_order_source_filter.set("全部来源")
        self.pending_order_state_filter.set("全部状态")
        self.pending_order_asset_filter.set("")
        self.pending_order_expiry_prefix_filter.set("")
        self.pending_order_keyword.set("")
        self._render_pending_orders_view()

    def _on_order_history_filter_changed(self, *_: object) -> None:
        self._render_order_history_view()

    def reset_order_history_filters(self) -> None:
        self.order_history_type_filter.set("全部类型")
        self.order_history_source_filter.set("全部来源")
        self.order_history_state_filter.set("全部状态")
        self.order_history_asset_filter.set("")
        self.order_history_expiry_prefix_filter.set("")
        self.order_history_keyword.set("")
        self._render_order_history_view()

    def _trade_order_views(self, *pairs: tuple[str, str]) -> list[tuple[str, str, ttk.Treeview, Text | None]]:
        views: list[tuple[str, str, ttk.Treeview, Text | None]] = []
        for tree_attr, detail_attr in pairs:
            tree = getattr(self, tree_attr)
            detail = getattr(self, detail_attr)
            if tree is not None and not _widget_exists(tree):
                setattr(self, tree_attr, None)
                tree = None
            if detail is not None and not _widget_exists(detail):
                setattr(self, detail_attr, None)
                detail = None
            if tree is not None:
                views.append((tree_attr, detail_attr, tree, detail))
        return views

    def _pending_order_views(self) -> list[tuple[str, str, ttk.Treeview, Text | None]]:
        return self._trade_order_views(
            ("_positions_zoom_pending_orders_tree", "_positions_zoom_pending_orders_detail"),
            ("_account_info_pending_orders_tree", "_account_info_pending_orders_detail"),
        )

    def _order_history_views(self) -> list[tuple[str, str, ttk.Treeview, Text | None]]:
        return self._trade_order_views(
            ("_positions_zoom_order_history_tree", "_positions_zoom_order_history_detail"),
            ("_account_info_order_history_tree", "_account_info_order_history_detail"),
        )

    def _render_pending_orders_view(self) -> None:
        filtered_items = self._filtered_pending_order_items()
        summary = self._positions_zoom_pending_orders_base_summary
        cancelable_count = sum(
            1 for _, item in filtered_items if _trade_order_program_owner_label(item) is not None and _trade_order_cancel_reference(item)
        )
        if _trade_order_filter_enabled(
            POSITION_TYPE_OPTIONS.get(self.pending_order_type_filter.get(), ""),
            ORDER_SOURCE_FILTER_OPTIONS.get(self.pending_order_source_filter.get(), ""),
            ORDER_STATE_FILTER_OPTIONS.get(self.pending_order_state_filter.get(), ""),
            self.pending_order_asset_filter.get(),
            self.pending_order_expiry_prefix_filter.get(),
            self.pending_order_keyword.get(),
        ):
            summary = f"{summary} | 当前显示：{len(filtered_items)}/{len(self._latest_pending_orders)}"
        if filtered_items:
            summary = f"{summary} | 可撤程序单：{cancelable_count}/{len(filtered_items)}"
        self._positions_zoom_pending_orders_summary_text.set(summary)
        for tree_attr, _, tree, _ in self._pending_order_views():
            try:
                selection = tree.selection()
                selected_before = selection[0] if selection else None
                tree.delete(*tree.get_children())
                for index, item in filtered_items:
                    iid = f"po-{index}"
                    tree.insert(
                        "",
                        END,
                        iid=iid,
                        values=(
                            _format_trade_order_timestamp(item),
                            item.source_label,
                            item.inst_type or "-",
                            item.inst_id or "-",
                            _format_trade_order_state(item.state),
                            _format_history_side(item.side, item.pos_side),
                            item.ord_type or "-",
                            _format_trade_order_price(item.price, item.inst_id, item.inst_type),
                            _format_trade_order_coin_size(item, self._pending_order_instruments),
                            _format_trade_order_coin_filled_size(item, self._pending_order_instruments),
                            _format_trade_order_fee_cell(item),
                            _format_trade_order_tp_sl(item),
                            item.order_id or item.algo_id or "-",
                            item.client_order_id or item.algo_client_order_id or "-",
                        ),
                        tags=tuple(tag for tag in (_pnl_tag(item.pnl),) if tag),
                    )
                if selected_before and tree.exists(selected_before):
                    tree.selection_set(selected_before)
                    tree.focus(selected_before)
                elif tree.get_children():
                    first = tree.get_children()[0]
                    tree.selection_set(first)
                    tree.focus(first)
            except TclError:
                setattr(self, tree_attr, None)
        self._refresh_pending_orders_detail()

    def _render_order_history_view(self) -> None:
        filtered_items = _filter_trade_order_items(
            self._latest_order_history,
            inst_type=POSITION_TYPE_OPTIONS.get(self.order_history_type_filter.get(), ""),
            source=ORDER_SOURCE_FILTER_OPTIONS.get(self.order_history_source_filter.get(), ""),
            state=ORDER_STATE_FILTER_OPTIONS.get(self.order_history_state_filter.get(), ""),
            asset=self.order_history_asset_filter.get(),
            expiry_prefix=self.order_history_expiry_prefix_filter.get(),
            keyword=self.order_history_keyword.get(),
        )
        summary = self._positions_zoom_order_history_base_summary
        if _trade_order_filter_enabled(
            POSITION_TYPE_OPTIONS.get(self.order_history_type_filter.get(), ""),
            ORDER_SOURCE_FILTER_OPTIONS.get(self.order_history_source_filter.get(), ""),
            ORDER_STATE_FILTER_OPTIONS.get(self.order_history_state_filter.get(), ""),
            self.order_history_asset_filter.get(),
            self.order_history_expiry_prefix_filter.get(),
            self.order_history_keyword.get(),
        ):
            summary = f"{summary} | 当前显示：{len(filtered_items)}/{len(self._latest_order_history)}"
        self._positions_zoom_order_history_summary_text.set(summary)
        for tree_attr, _, tree, _ in self._order_history_views():
            try:
                selection = tree.selection()
                selected_before = selection[0] if selection else None
                tree.delete(*tree.get_children())
                for index, item in filtered_items:
                    iid = f"oh-{index}"
                    tree.insert(
                        "",
                        END,
                        iid=iid,
                        values=(
                            _format_trade_order_timestamp(item),
                            item.source_label,
                            item.inst_type or "-",
                            item.inst_id or "-",
                            _format_trade_order_state(item.state),
                            _format_history_side(item.side, item.pos_side),
                            item.ord_type or "-",
                            _format_trade_order_price(item.price, item.inst_id, item.inst_type),
                            _format_trade_order_coin_size(item, self._order_history_instruments),
                            _format_trade_order_coin_filled_size(item, self._order_history_instruments),
                            _format_trade_order_fee_cell(item, self._order_history_usdt_prices),
                            _format_trade_order_tp_sl(item),
                            item.order_id or item.algo_id or "-",
                            item.client_order_id or item.algo_client_order_id or "-",
                        ),
                        tags=tuple(tag for tag in (_pnl_tag(item.pnl),) if tag),
                    )
                if selected_before and tree.exists(selected_before):
                    tree.selection_set(selected_before)
                    tree.focus(selected_before)
                elif tree.get_children():
                    first = tree.get_children()[0]
                    tree.selection_set(first)
                    tree.focus(first)
            except TclError:
                setattr(self, tree_attr, None)
        self._refresh_order_history_detail()

    def _on_pending_orders_selected(self, *_: object) -> None:
        self._refresh_pending_orders_detail()

    def _on_order_history_selected(self, *_: object) -> None:
        self._refresh_order_history_detail()

    def _refresh_pending_orders_detail(self) -> None:
        for tree_attr, detail_attr, tree, detail in self._pending_order_views():
            if detail is None:
                continue
            try:
                selection = tree.selection()
            except TclError:
                setattr(self, tree_attr, None)
                setattr(self, detail_attr, None)
                continue
            if not selection:
                self._set_readonly_text(detail, "这里会显示选中当前委托的详情。")
                continue
            index = _history_tree_index(selection[0], "po")
            if index is None or index >= len(self._latest_pending_orders):
                self._set_readonly_text(detail, "这里会显示选中当前委托的详情。")
                continue
            self._set_readonly_text(detail, _build_trade_order_detail_text(self._latest_pending_orders[index]))

    def _refresh_order_history_detail(self) -> None:
        for tree_attr, detail_attr, tree, detail in self._order_history_views():
            if detail is None:
                continue
            try:
                selection = tree.selection()
            except TclError:
                setattr(self, tree_attr, None)
                setattr(self, detail_attr, None)
                continue
            if not selection:
                self._set_readonly_text(detail, "这里会显示选中历史委托的详情。")
                continue
            index = _history_tree_index(selection[0], "oh")
            if index is None or index >= len(self._latest_order_history):
                self._set_readonly_text(detail, "这里会显示选中历史委托的详情。")
                continue
            self._set_readonly_text(detail, _build_trade_order_detail_text(self._latest_order_history[index]))

    def _selected_pending_order_item(self, view_name: str | None = None) -> OkxTradeOrderItem | None:
        candidate_trees: list[ttk.Treeview] = []
        tree = self._pending_order_tree_for_view(view_name)
        if tree is not None and _widget_exists(tree):
            candidate_trees.append(tree)
        for fallback_tree in (self._positions_zoom_pending_orders_tree, self._account_info_pending_orders_tree):
            if fallback_tree is not None and fallback_tree not in candidate_trees and _widget_exists(fallback_tree):
                candidate_trees.append(fallback_tree)
        for active_tree in candidate_trees:
            try:
                selection = active_tree.selection()
            except TclError:
                continue
            if not selection:
                continue
            index = _history_tree_index(selection[0], "po")
            if index is None or index >= len(self._latest_pending_orders):
                continue
            return self._latest_pending_orders[index]
        return None

    def _filtered_pending_order_items(self) -> list[tuple[int, OkxTradeOrderItem]]:
        return _filter_trade_order_items(
            self._latest_pending_orders,
            inst_type=POSITION_TYPE_OPTIONS.get(self.pending_order_type_filter.get(), ""),
            source=ORDER_SOURCE_FILTER_OPTIONS.get(self.pending_order_source_filter.get(), ""),
            state=ORDER_STATE_FILTER_OPTIONS.get(self.pending_order_state_filter.get(), ""),
            asset=self.pending_order_asset_filter.get(),
            expiry_prefix=self.pending_order_expiry_prefix_filter.get(),
            keyword=self.pending_order_keyword.get(),
        )

    def refresh_position_histories(self) -> None:
        credentials = self._current_credentials_or_none()
        if credentials is None:
            self._positions_zoom_fills_summary_text.set("未配置 API 凭证，无法读取历史成交。")
            self._positions_zoom_position_history_base_summary = "未配置 API 凭证，无法读取历史仓位。"
            self._positions_zoom_position_history_summary_text.set(self._positions_zoom_position_history_base_summary)
            return
        environment = self._positions_effective_environment or ENV_OPTIONS[self.environment_label.get()]
        profile_name = credentials.profile_name or self._current_credential_profile()
        self._start_position_history_refresh(credentials, environment, profile_name)
        self._start_fill_history_refresh(credentials, environment, profile_name)

    def sync_all_histories(self) -> None:
        self.refresh_position_histories()
        self.refresh_order_history()

    def sync_positions_zoom_data(self) -> None:
        self.refresh_pending_orders()
        self.refresh_position_histories()
        self.refresh_order_history()

    def refresh_fill_history(self, *, sync_order_history: bool = False) -> None:
        credentials = self._current_credentials_or_none()
        if credentials is None:
            self._positions_zoom_fills_summary_text.set("未配置 API 凭证，无法读取历史成交。")
            return
        environment = self._positions_effective_environment or ENV_OPTIONS[self.environment_label.get()]
        profile_name = credentials.profile_name or self._current_credential_profile()
        self._start_fill_history_refresh(credentials, environment, profile_name)
        if sync_order_history:
            self._start_order_history_refresh(credentials, environment, profile_name)

    def expand_fill_history_limit(self) -> None:
        self._fill_history_fetch_limit, self._fill_history_load_more_clicks, next_label = _advance_fill_history_limit(
            self._fill_history_fetch_limit,
            self._fill_history_load_more_clicks,
        )
        self._positions_zoom_fills_load_more_text.set(next_label)
        self.refresh_fill_history()

    def expand_position_history_limit(self) -> None:
        self._position_history_fetch_limit, self._position_history_load_more_clicks, next_label = _advance_fill_history_limit(
            self._position_history_fetch_limit,
            self._position_history_load_more_clicks,
        )
        self._positions_zoom_position_history_load_more_text.set(next_label)
        credentials = self._current_credentials_or_none()
        if credentials is None:
            self._positions_zoom_position_history_summary_text.set("未配置 API 凭证，无法读取历史仓位。")
            return
        environment = self._positions_effective_environment or ENV_OPTIONS[self.environment_label.get()]
        profile_name = credentials.profile_name or self._current_credential_profile()
        self._start_position_history_refresh(credentials, environment, profile_name)

    def _start_fill_history_refresh(self, credentials: Credentials, environment: str, profile_name: str) -> None:
        if self._fills_history_refreshing:
            self._fills_history_refresh_request = (credentials, environment, profile_name)
            return
        self._fills_history_refreshing = True
        self._fills_history_refresh_request = None
        self._positions_zoom_fills_summary_text.set("正在同步历史成交...")
        threading.Thread(
            target=self._refresh_fill_history_worker,
            args=(credentials, environment, profile_name),
            daemon=True,
        ).start()

    def _start_position_history_refresh(self, credentials: Credentials, environment: str, profile_name: str) -> None:
        if self._position_history_refreshing:
            return
        self._position_history_refreshing = True
        self._positions_zoom_position_history_summary_text.set("正在同步历史仓位...")
        threading.Thread(
            target=self._refresh_position_history_worker,
            args=(credentials, environment, profile_name),
            daemon=True,
        ).start()

    def _refresh_fill_history_worker(self, credentials: Credentials, environment: str, profile_name: str) -> None:
        try:
            fills = self.client.get_fills_history(credentials, environment=environment, limit=self._fill_history_fetch_limit)
            instruments = _safe_build_history_instrument_map(self.client, [item.inst_id for item in fills])
            note = None
            effective_environment = environment
        except Exception as exc:
            message = str(exc)
            if "50101" in message and "current environment" in message:
                alternate = "live" if environment == "demo" else "demo"
                try:
                    fills = self.client.get_fills_history(credentials, environment=alternate, limit=self._fill_history_fetch_limit)
                    instruments = _safe_build_history_instrument_map(self.client, [item.inst_id for item in fills])
                    note = f"历史数据自动切换到 {'实盘' if alternate == 'live' else '模拟'} 环境读取。"
                    effective_environment = alternate
                except Exception:
                    self.root.after(0, lambda m=message: self._apply_fill_history_error(m))
                    return
            else:
                self.root.after(0, lambda m=message: self._apply_fill_history_error(m))
                return
        self.root.after(
            0,
            lambda f=fills, ins=instruments, n=note, env=effective_environment, prof=profile_name: self._apply_fill_history(
                f, ins, n, env, prof
            ),
        )

    def _refresh_position_history_worker(self, credentials: Credentials, environment: str, profile_name: str) -> None:
        try:
            position_history = self.client.get_positions_history(credentials, environment=environment, limit=self._position_history_fetch_limit)
            usdt_prices = _build_position_history_usdt_price_map(self.client, position_history)
            instruments = _safe_build_history_instrument_map(self.client, [item.inst_id for item in position_history])
            note = None
            effective_environment = environment
        except Exception as exc:
            message = str(exc)
            if "50101" in message and "current environment" in message:
                alternate = "live" if environment == "demo" else "demo"
                try:
                    position_history = self.client.get_positions_history(credentials, environment=alternate, limit=self._position_history_fetch_limit)
                    usdt_prices = _build_position_history_usdt_price_map(self.client, position_history)
                    instruments = _safe_build_history_instrument_map(self.client, [item.inst_id for item in position_history])
                    note = f"历史数据自动切换到 {'实盘' if alternate == 'live' else '模拟'} 环境读取。"
                    effective_environment = alternate
                except Exception:
                    self.root.after(0, lambda: self._apply_position_history_error(message))
                    return
            else:
                self.root.after(0, lambda: self._apply_position_history_error(message))
                return
        self.root.after(
            0,
            lambda: self._apply_position_history(
                position_history,
                usdt_prices,
                instruments,
                note,
                effective_environment,
                profile_name,
            ),
        )

    def _apply_fill_history(
        self,
        fills: list[OkxFillHistoryItem],
        instruments: dict[str, Instrument],
        note: str | None = None,
        effective_environment: str | None = None,
        profile_name: str | None = None,
    ) -> None:
        self._fills_history_refreshing = False
        self._fills_history_from_local_only = False
        try:
            active_profile = (profile_name or self._current_credential_profile()).strip() or DEFAULT_CREDENTIAL_PROFILE_NAME
            active_environment = effective_environment or self._positions_effective_environment or ENV_OPTIONS[self.environment_label.get()]
            local_records = load_history_cache_records("fills", active_profile, active_environment)
            merged_records = _merge_history_cache_records(
                local_records=local_records,
                remote_records=[_serialize_history_item(item) for item in fills],
                dedup_fields=("trade_id", "order_id", "inst_id", "fill_time", "side", "fill_size", "fill_price"),
            )
            save_history_cache_records("fills", active_profile, active_environment, merged_records)
            self._latest_fill_history = [item for record in merged_records if (item := _fill_item_from_cache(record)) is not None]
            self._latest_fill_history.sort(key=lambda it: (it.fill_time or 0), reverse=True)
            self._fill_history_instruments = dict(instruments)
            fill_ccys: set[str] = set()
            for fill in self._latest_fill_history:
                if fill.fill_fee is not None and fill.fee_currency and str(fill.fee_currency).strip():
                    fill_ccys.add(fill.fee_currency.strip().upper())
                if fill.pnl is not None:
                    fill_ccys.add(_infer_fill_history_pnl_currency(fill))
            self._fill_history_usdt_prices = (
                _build_usdt_price_snapshot(self.client, fill_ccys) if fill_ccys else {}
            )
            self._fills_history_last_refresh_at = datetime.now()
            self._positions_history_last_refresh_at = self._fills_history_last_refresh_at
            if effective_environment:
                self._positions_effective_environment = effective_environment
            timestamp = self._fills_history_last_refresh_at.strftime("%H:%M:%S")
            fill_summary = f"历史成交：{len(self._latest_fill_history)} 条 | 最近同步：{timestamp}"
            if not fills:
                fill_summary = f"{fill_summary} | 本次 API 返回 0 条（请核对模拟/实盘环境与 API 权限）"
            if note:
                fill_summary = f"{fill_summary} | {note}"
            self._positions_zoom_fills_summary_text.set(fill_summary)
            self._render_positions_zoom_fills_view()
        except Exception as exc:
            self._apply_fill_history_error(str(exc))
            return
        pending_fill_refresh = self._fills_history_refresh_request
        self._fills_history_refresh_request = None
        if pending_fill_refresh is not None:
            pcred, penv, pprof = pending_fill_refresh
            self.root.after(0, lambda c=pcred, e=penv, p=pprof: self._start_fill_history_refresh(c, e, p))

    def _apply_position_history(
        self,
        position_history: list[OkxPositionHistoryItem],
        usdt_prices: dict[str, Decimal],
        instruments: dict[str, Instrument],
        note: str | None = None,
        effective_environment: str | None = None,
        credential_profile_name: str | None = None,
    ) -> None:
        self._position_history_refreshing = False
        self._position_history_usdt_prices = dict(usdt_prices)
        # Merge freshly fetched specs with local swap/option cache so legacy rows
        # can still be converted to coin amounts instead of falling back to contracts.
        instrument_map = {item.inst_id: item for item in self.instruments if item.inst_id}
        instrument_map.update(instruments)
        self._position_history_instruments = instrument_map
        self._position_history_last_refresh_at = datetime.now()
        self._positions_history_last_refresh_at = self._position_history_last_refresh_at
        self._position_history_profile_name = (credential_profile_name or self._current_credential_profile()).strip()
        self._position_history_effective_environment = effective_environment
        if effective_environment:
            self._positions_effective_environment = effective_environment
        timestamp = self._position_history_last_refresh_at.strftime("%H:%M:%S")
        active_profile = (credential_profile_name or self._current_credential_profile()).strip() or DEFAULT_CREDENTIAL_PROFILE_NAME
        active_environment = effective_environment or self._positions_effective_environment or ENV_OPTIONS[self.environment_label.get()]
        local_records = load_history_cache_records("positions", active_profile, active_environment)
        merged_records = _merge_history_cache_records(
            local_records=local_records,
            remote_records=[_serialize_history_item(item) for item in position_history],
            dedup_fields=("update_time", "inst_id", "pos_side", "direction", "close_size", "close_avg_price"),
        )
        save_history_cache_records("positions", active_profile, active_environment, merged_records)
        parsed_items = [item for record in merged_records if (item := _position_history_item_from_cache(record)) is not None]
        parsed_items.sort(key=lambda item: item.update_time or 0, reverse=True)
        self._latest_position_history = parsed_items[: self._position_history_fetch_limit]
        extra_ccys: set[str] = set()
        for it in self._latest_position_history:
            if it.realized_pnl is not None:
                extra_ccys.add(_infer_position_history_pnl_currency(it))
            if it.pnl is not None:
                extra_ccys.add(_infer_position_history_pnl_currency(it))
            if it.fee is not None and it.fee_currency and str(it.fee_currency).strip():
                extra_ccys.add(it.fee_currency.strip().upper())
        missing_ccys = {
            c
            for c in extra_ccys
            if c not in {"USDT", "USD", "USDC"} and c not in self._position_history_usdt_prices
        }
        if missing_ccys:
            self._position_history_usdt_prices.update(_build_usdt_price_snapshot(self.client, missing_ccys))
        history_summary = f"历史仓位：{len(self._latest_position_history)} 条 | 最近同步：{timestamp}"
        if note:
            history_summary = f"{history_summary} | {note}"
        self._positions_zoom_position_history_base_summary = history_summary
        self._positions_zoom_position_history_summary_text.set(history_summary)
        if self._position_history_profile_name and self._position_history_effective_environment:
            self._sync_position_note_state_for_history(
                profile_name=self._position_history_profile_name,
                environment=self._position_history_effective_environment,
                position_history=position_history,
            )
        self._render_positions_zoom_position_history_view()

    def _apply_fill_history_error(self, message: str) -> None:
        self._fills_history_refreshing = False
        self._fills_history_refresh_request = None
        self._positions_zoom_fills_summary_text.set(f"历史成交读取失败：{message}")

    def _apply_position_history_error(self, message: str) -> None:
        self._position_history_refreshing = False
        self._positions_zoom_position_history_base_summary = f"历史仓位读取失败：{message}"
        self._positions_zoom_position_history_summary_text.set(self._positions_zoom_position_history_base_summary)

    def _render_positions_zoom_fills_view(self) -> None:
        if self._positions_zoom_fills_tree is None:
            return
        tree = self._positions_zoom_fills_tree
        selected_before = tree.selection()[0] if tree.selection() else None
        tree.delete(*tree.get_children())
        filtered_items = _filter_fill_history_items(
            self._latest_fill_history,
            inst_type=POSITION_TYPE_OPTIONS.get(self.fill_history_type_filter.get(), ""),
            side=HISTORY_FILL_SIDE_FILTER_OPTIONS.get(self.fill_history_side_filter.get(), ""),
            asset=self.fill_history_asset_filter.get(),
            expiry_prefix=self.fill_history_expiry_prefix_filter.get(),
            keyword=self.fill_history_keyword.get(),
        )
        for index, item in filtered_items:
            iid = f"fill-{index}"
            tree.insert(
                "",
                END,
                iid=iid,
                values=(
                    _format_okx_ms_timestamp(item.fill_time),
                    item.inst_type or "-",
                    item.inst_id or "-",
                    _format_history_side(item.side, item.pos_side),
                    _format_fill_history_price(item),
                    _format_fill_history_size(item, self._fill_history_instruments),
                    _format_fill_history_fee_cell(item, self._fill_history_usdt_prices),
                    _format_fill_history_pnl(item, self._fill_history_usdt_prices),
                    _format_fill_history_exec_type(item.exec_type),
                  ),
                  tags=tuple(tag for tag in (_pnl_tag(item.pnl),) if tag),
              )
        if self._fills_history_from_local_only:
            summary = f"历史成交：{len(self._latest_fill_history)} 条 | 本地缓存"
        elif self._fills_history_last_refresh_at is not None:
            timestamp = self._fills_history_last_refresh_at.strftime("%H:%M:%S")
            summary = f"历史成交：{len(self._latest_fill_history)} 条 | 最近刷新：{timestamp}"
        else:
            summary = f"历史成交：{len(self._latest_fill_history)} 条"
        if (
            POSITION_TYPE_OPTIONS.get(self.fill_history_type_filter.get(), "")
            or HISTORY_FILL_SIDE_FILTER_OPTIONS.get(self.fill_history_side_filter.get(), "")
            or self.fill_history_asset_filter.get().strip()
            or self.fill_history_expiry_prefix_filter.get().strip()
            or self.fill_history_keyword.get().strip()
        ):
            summary = f"{summary} | 当前显示：{len(filtered_items)}/{len(self._latest_fill_history)}"
        self._positions_zoom_fills_summary_text.set(summary)
        if selected_before and tree.exists(selected_before):
            tree.selection_set(selected_before)
            tree.focus(selected_before)
        elif tree.get_children():
            first = tree.get_children()[0]
            tree.selection_set(first)
            tree.focus(first)
        self._refresh_positions_zoom_fills_detail()
        self._update_fill_history_search_shortcuts()

    def _on_fill_history_filter_changed(self, *_: object) -> None:
        self._render_positions_zoom_fills_view()

    def reset_fill_history_filters(self) -> None:
        self.fill_history_type_filter.set("全部类型")
        self.fill_history_side_filter.set("全部方向")
        self.fill_history_asset_filter.set("")
        self.fill_history_expiry_prefix_filter.set("")
        self.fill_history_keyword.set("")
        self._render_positions_zoom_fills_view()

    def _render_positions_zoom_position_history_view(self) -> None:
        if self._positions_zoom_position_history_tree is None:
            return
        tree = self._positions_zoom_position_history_tree
        selected_before = tree.selection()[0] if tree.selection() else None
        tree.delete(*tree.get_children())
        start_d = _parse_position_history_local_date(self.position_history_range_start.get())
        end_d = _parse_position_history_local_date(self.position_history_range_end.get())
        if start_d is not None and end_d is not None and start_d > end_d:
            start_d, end_d = end_d, start_d
        filter_kwargs = dict(
            inst_type=POSITION_TYPE_OPTIONS.get(self.position_history_type_filter.get(), ""),
            margin_mode=HISTORY_MARGIN_MODE_FILTER_OPTIONS.get(self.position_history_margin_filter.get(), ""),
            asset=self.position_history_asset_filter.get(),
            expiry_prefix=self.position_history_expiry_prefix_filter.get(),
            keyword=self.position_history_keyword.get(),
            note_texts_by_index=self._position_history_note_text_map_by_index(),
        )
        date_range_active = start_d is not None or end_d is not None
        without_date_range = _filter_position_history_items(
            self._latest_position_history,
            **filter_kwargs,
            range_start_local=None,
            range_end_local=None,
        )
        filtered_items = _filter_position_history_items(
            self._latest_position_history,
            **filter_kwargs,
            range_start_local=start_d,
            range_end_local=end_d,
        )
        excluded_outside_range = (len(without_date_range) - len(filtered_items)) if date_range_active else 0
        for index, item in filtered_items:
            iid = f"ph-{index}"
            tree.insert(
                "",
                END,
                iid=iid,
                values=(
                    _format_okx_ms_timestamp(item.update_time),
                    item.inst_type or "-",
                    item.inst_id or "-",
                    _format_margin_mode(item.mgn_mode or ""),
                    _format_history_side(None, item.pos_side or item.direction),
                    _format_position_history_price(item.open_avg_price, item.inst_id, item.inst_type),
                    _format_position_history_price(item.close_avg_price, item.inst_id, item.inst_type),
                    _format_position_history_size(item, self._position_history_instruments),
                    _format_position_history_fee_cell(item, self._position_history_usdt_prices),
                    _format_position_history_pnl(item.pnl, item, usdt_prices=self._position_history_usdt_prices),
                    _format_position_history_pnl(
                        item.realized_pnl,
                        item,
                        with_sign=True,
                        usdt_prices=self._position_history_usdt_prices,
                    ),
                    self._position_history_note_summary(item),
                ),
                tags=tuple(tag for tag in (_pnl_tag(item.pnl),) if tag),
            )
        summary = self._positions_zoom_position_history_base_summary
        any_position_history_filter = bool(
            POSITION_TYPE_OPTIONS.get(self.position_history_type_filter.get(), "")
            or HISTORY_MARGIN_MODE_FILTER_OPTIONS.get(self.position_history_margin_filter.get(), "")
            or self.position_history_asset_filter.get().strip()
            or self.position_history_expiry_prefix_filter.get().strip()
            or self.position_history_keyword.get().strip()
            or date_range_active
        )
        if any_position_history_filter:
            summary = f"{summary} | 当前显示：{len(filtered_items)}/{len(self._latest_position_history)}"
        if excluded_outside_range:
            summary = f"{summary} | 日期范围外 {excluded_outside_range} 条未显示"
        if (
            POSITION_TYPE_OPTIONS.get(self.position_history_type_filter.get(), "")
            or HISTORY_MARGIN_MODE_FILTER_OPTIONS.get(self.position_history_margin_filter.get(), "")
            or self.position_history_asset_filter.get().strip()
            or self.position_history_expiry_prefix_filter.get().strip()
            or self.position_history_keyword.get().strip()
            or date_range_active
        ):
            summary = (
                f"{self._positions_zoom_position_history_base_summary} | \u5f53\u524d\u663e\u793a\uff1a{len(filtered_items)}/{len(self._latest_position_history)}"
                + (f" | \u65e5\u671f\u8303\u56f4\u5916 {excluded_outside_range} \u6761\u672a\u663e\u793a" if excluded_outside_range else "")
                + f"\n\u7b5b\u9009\u7edf\u8ba1\uff1a{_format_position_history_filter_stats(filtered_items, self._position_history_usdt_prices)}"
            )
        self._positions_zoom_position_history_summary_text.set(summary)
        if selected_before and tree.exists(selected_before):
            tree.selection_set(selected_before)
            tree.focus(selected_before)
        elif tree.get_children():
            first = tree.get_children()[0]
            tree.selection_set(first)
            tree.focus(first)
        self._refresh_positions_zoom_position_history_detail()
        self._update_position_history_search_shortcuts()

    def _on_positions_zoom_fills_selected(self, *_: object) -> None:
        self._refresh_positions_zoom_fills_detail()
        self._update_fill_history_search_shortcuts()

    def _on_position_history_filter_changed(self, *_: object) -> None:
        self._render_positions_zoom_position_history_view()

    def reset_position_history_filters(self) -> None:
        self.position_history_type_filter.set("全部类型")
        self.position_history_margin_filter.set("全部模式")
        self.position_history_asset_filter.set("")
        self.position_history_expiry_prefix_filter.set("")
        self.position_history_keyword.set("")
        range_start, range_end = _default_position_history_local_year_range_strings()
        self.position_history_range_start.set(range_start)
        self.position_history_range_end.set(range_end)
        self._render_positions_zoom_position_history_view()

    def _on_positions_zoom_position_history_selected(self, *_: object) -> None:
        self._refresh_positions_zoom_position_history_detail()
        self._update_position_history_search_shortcuts()

    def _refresh_positions_zoom_fills_detail(self) -> None:
        if self._positions_zoom_fills_tree is None or self._positions_zoom_fills_detail is None:
            return
        selection = self._positions_zoom_fills_tree.selection()
        if not selection:
            self._set_readonly_text(self._positions_zoom_fills_detail, "这里会显示选中历史成交单的详情。")
            return
        index = _history_tree_index(selection[0], "fill")
        if index is None or index >= len(self._latest_fill_history):
            self._set_readonly_text(self._positions_zoom_fills_detail, "这里会显示选中历史成交单的详情。")
            return
        self._set_readonly_text(
            self._positions_zoom_fills_detail,
            _build_fill_history_detail_text(self._latest_fill_history[index], self._fill_history_instruments),
        )

    def _refresh_positions_zoom_position_history_detail(self) -> None:
        if self._positions_zoom_position_history_tree is None or self._positions_zoom_position_history_detail is None:
            return
        selection = self._positions_zoom_position_history_tree.selection()
        if not selection:
            self._set_readonly_text(self._positions_zoom_position_history_detail, "这里会显示选中历史仓位的详情。")
            return
        index = _history_tree_index(selection[0], "ph")
        if index is None or index >= len(self._latest_position_history):
            self._set_readonly_text(self._positions_zoom_position_history_detail, "这里会显示选中历史仓位的详情。")
            return
        self._set_readonly_text(
            self._positions_zoom_position_history_detail,
            _build_position_history_detail_text(
                self._latest_position_history[index],
                self._position_history_usdt_prices,
                self._position_history_instruments,
                note=self._position_history_note_text(self._latest_position_history[index]),
            ),
        )

    def _selected_option_position(self, *, prefer_protection_form: bool = False) -> OkxPosition | None:
        if prefer_protection_form and self._protection_form_position_key:
            fallback = _find_position_by_key(self._latest_positions, self._protection_form_position_key)
            if fallback is not None and fallback.inst_type == "OPTION":
                return fallback
        payload = None
        if self._positions_zoom_window is not None and self._positions_zoom_window.winfo_exists():
            if self._positions_zoom_selected_item_id:
                payload = self._position_row_payloads.get(self._positions_zoom_selected_item_id)
        if payload is None:
            payload = self._selected_position_payload()
        if payload is not None and payload["kind"] == "position":
            position = payload["item"]
            if isinstance(position, OkxPosition) and position.inst_type == "OPTION":
                return position
        if self._protection_form_position_key:
            fallback = _find_position_by_key(self._latest_positions, self._protection_form_position_key)
            if fallback is not None and fallback.inst_type == "OPTION":
                return fallback
        return None

    def refresh_positions(self) -> None:
        if self._positions_refreshing:
            return

        credentials = self._current_credentials_or_none()
        if credentials is None:
            self._apply_positions([], "未配置 API 凭证，无法读取持仓。")
            _reset_refresh_health(self._positions_refresh_health)
            self._refresh_all_refresh_badges()
            return

        self._positions_refreshing = True
        self.positions_summary_text.set("正在刷新账户持仓...")
        environment = ENV_OPTIONS[self.environment_label.get()]
        profile_name = credentials.profile_name or self._current_credential_profile()
        threading.Thread(
            target=self._refresh_positions_worker,
            args=(credentials, environment, profile_name),
            daemon=True,
        ).start()

    def _refresh_positions_worker(self, credentials: Credentials, environment: str, profile_name: str) -> None:
        try:
            positions = self.client.get_positions(credentials, environment=environment)
            upl_usdt_prices = _build_upl_usdt_price_map(self.client, positions)
            position_instruments = _build_position_instrument_map(self.client, positions)
            position_tickers = _build_position_ticker_map(self.client, positions)
        except Exception as exc:
            message = str(exc)
            if "50101" in message and "current environment" in message:
                alternate = "live" if environment == "demo" else "demo"
                try:
                    positions = self.client.get_positions(credentials, environment=alternate)
                    upl_usdt_prices = _build_upl_usdt_price_map(self.client, positions)
                    position_instruments = _build_position_instrument_map(self.client, positions)
                    position_tickers = _build_position_ticker_map(self.client, positions)
                except Exception:
                    self.root.after(0, lambda: self._apply_positions_error(message))
                    return
                summary = (
                    f"当前 API Key 与 {alternate} 环境匹配，已自动按 "
                    f"{'实盘' if alternate == 'live' else '模拟盘'} 读取持仓。"
                )
                self.root.after(
                    0,
                    lambda: self._apply_positions(
                        positions=positions,
                        summary=summary,
                        effective_environment=alternate,
                        credential_profile_name=profile_name,
                        upl_usdt_prices=upl_usdt_prices,
                        position_instruments=position_instruments,
                        position_tickers=position_tickers,
                    ),
                )
                return
            self.root.after(0, lambda: self._apply_positions_error(message))
            return
        self.root.after(
            0,
            lambda: self._apply_positions(
                positions=positions,
                summary=None,
                effective_environment=environment,
                credential_profile_name=profile_name,
                upl_usdt_prices=upl_usdt_prices,
                position_instruments=position_instruments,
                position_tickers=position_tickers,
            ),
        )

    def _apply_positions(
        self,
        positions: list[OkxPosition],
        summary: str | None = None,
        effective_environment: str | None = None,
        credential_profile_name: str | None = None,
        upl_usdt_prices: dict[str, Decimal] | None = None,
        position_instruments: dict[str, Instrument] | None = None,
        position_tickers: dict[str, OkxTicker] | None = None,
    ) -> None:
        self._positions_refreshing = False
        self._latest_positions = list(positions)
        self._positions_context_note = summary
        self._positions_last_refresh_at = datetime.now()
        self._positions_effective_environment = effective_environment
        self._positions_context_profile_name = (credential_profile_name or self._current_credential_profile()).strip()
        _mark_refresh_health_success(self._positions_refresh_health, at=self._positions_last_refresh_at)
        self._refresh_all_refresh_badges()
        self._upl_usdt_prices = dict(upl_usdt_prices or {})
        self._position_instruments = dict(position_instruments or {})
        self._position_tickers = dict(position_tickers or {})
        profile_name = self._positions_context_profile_name
        if profile_name:
            self._positions_snapshot_by_profile[profile_name] = ProfilePositionSnapshot(
                api_name=profile_name,
                effective_environment=effective_environment,
                positions=list(positions),
                upl_usdt_prices=dict(upl_usdt_prices or {}),
                refreshed_at=self._positions_last_refresh_at,
                position_instruments=dict(position_instruments or {}),
            )
        if profile_name and effective_environment:
            self._sync_position_note_state_for_positions(
                profile_name=profile_name,
                environment=effective_environment,
                positions=positions,
            )
        self._render_positions_view()
        self._refresh_session_live_pnl_cache()
        for session in self.sessions.values():
            self._upsert_session_row(session)
        self._refresh_running_session_summary()
        self._refresh_selected_session_details()

    @staticmethod
    def _session_counts_toward_running_summary(session: StrategySession) -> bool:
        return bool(
            session.engine.is_running
            or session.stop_cleanup_in_progress
            or session.status in {"运行中", "停止中", "待恢复", "恢复中"}
        )

    def _positions_snapshot_for_session(self, session: StrategySession) -> ProfilePositionSnapshot | None:
        profile_name = (session.api_name or "").strip()
        if not profile_name:
            return None
        snapshot = self._positions_snapshot_by_profile.get(profile_name)
        if snapshot is None:
            return None
        expected_environment = str(getattr(session.config, "environment", "") or "").strip().lower()
        effective_environment = str(snapshot.effective_environment or "").strip().lower()
        if expected_environment and effective_environment and expected_environment != effective_environment:
            return None
        return snapshot

    def _refresh_session_live_pnl_cache(self) -> None:
        cache: dict[str, tuple[Decimal | None, datetime | None]] = {
            session.session_id: (None, None) for session in self.sessions.values()
        }
        sessions_by_snapshot_key: dict[tuple[str, str], tuple[ProfilePositionSnapshot, list[StrategySession]]] = {}
        for session in self.sessions.values():
            if session.active_trade is None:
                continue
            snapshot = self._positions_snapshot_for_session(session)
            if snapshot is None:
                continue
            snapshot_key = (
                snapshot.api_name,
                str(snapshot.effective_environment or "").strip().lower(),
            )
            bucket = sessions_by_snapshot_key.get(snapshot_key)
            if bucket is None:
                sessions_by_snapshot_key[snapshot_key] = (snapshot, [session])
            else:
                bucket[1].append(session)

        for snapshot, sessions in sessions_by_snapshot_key.values():
            for position in snapshot.positions:
                candidate_sessions = [
                    session
                    for session in sessions
                    if _position_matches_session_live_pnl(
                        position,
                        trade_inst_id=_session_trade_inst_id(session),
                        expected_sides=_session_expected_position_sides(session),
                    )
                ]
                if not candidate_sessions:
                    continue
                pnl_value = _position_unrealized_pnl_usdt(position, snapshot.upl_usdt_prices)
                if pnl_value is None and _infer_upl_currency(position) in {"USDT", "USD", "USDC"}:
                    pnl_value = position.unrealized_pnl
                if pnl_value is None:
                    for session in candidate_sessions:
                        cache[session.session_id] = (cache[session.session_id][0], snapshot.refreshed_at)
                    continue

                allocations: dict[str, Decimal] = {}
                weighted_sizes: list[Decimal] = []
                for session in candidate_sessions:
                    trade_size = session.active_trade.size if session.active_trade is not None else None
                    if trade_size is None or trade_size <= 0:
                        weighted_sizes = []
                        break
                    weighted_sizes.append(trade_size)

                if len(candidate_sessions) == 1:
                    allocations[candidate_sessions[0].session_id] = pnl_value
                elif weighted_sizes:
                    total_size = sum(weighted_sizes, Decimal("0"))
                    if total_size > 0:
                        for session, trade_size in zip(candidate_sessions, weighted_sizes):
                            allocations[session.session_id] = pnl_value * trade_size / total_size
                if not allocations:
                    shared_value = pnl_value / Decimal(len(candidate_sessions))
                    for session in candidate_sessions:
                        allocations[session.session_id] = shared_value

                for session in candidate_sessions:
                    previous_value, _ = cache[session.session_id]
                    allocated = allocations.get(session.session_id)
                    if allocated is None:
                        cache[session.session_id] = (previous_value, snapshot.refreshed_at)
                        continue
                    cache[session.session_id] = (
                        (previous_value or Decimal("0")) + allocated,
                        snapshot.refreshed_at,
                    )

        self._session_live_pnl_cache = cache

    def _session_live_pnl_snapshot(self, session: StrategySession) -> tuple[Decimal | None, datetime | None]:
        return self._session_live_pnl_cache.get(session.session_id, (None, None))

    def _active_duplicate_strategy_groups(self) -> dict[tuple[str, StrategyConfig], list[StrategySession]]:
        groups: dict[tuple[str, StrategyConfig], list[StrategySession]] = {}
        sessions = getattr(self, "sessions", {})
        items = sessions.values() if isinstance(sessions, dict) else ()
        for session in items:
            api_name = str(getattr(session, "api_name", "") or "").strip()
            config = getattr(session, "config", None)
            if not api_name or not isinstance(config, StrategyConfig):
                continue
            if not QuantApp._session_blocks_duplicate_launch(session):
                continue
            key = (api_name, config)
            groups.setdefault(key, []).append(session)
        return {key: items for key, items in groups.items() if len(items) > 1}

    def _duplicate_launch_conflicts_for(self, session: StrategySession) -> list[StrategySession]:
        key = (session.api_name.strip(), session.config)
        groups = QuantApp._active_duplicate_strategy_groups(self)
        items = groups.get(key, [])
        return [item for item in items if item.session_id != session.session_id]

    def _session_has_duplicate_launch_conflict(self, session: StrategySession) -> bool:
        return bool(QuantApp._duplicate_launch_conflicts_for(self, session))

    @staticmethod
    def _session_category_label(session: StrategySession) -> str:
        trader_id = str(getattr(session, "trader_id", "") or "").strip()
        if trader_id:
            return "交易员策略"
        config = getattr(session, "config", None)
        run_mode = str(getattr(config, "run_mode", "") or "").strip().lower()
        if run_mode == "signal_only":
            return "信号观察台"
        return "普通量化"

    def _session_trader_label(self, session: StrategySession) -> str:
        trader_id = str(getattr(session, "trader_id", "") or "").strip()
        if not trader_id:
            return "-"
        draft = self._trader_desk_draft_by_id(trader_id)
        if draft is None:
            return trader_id
        return str(getattr(draft, "trader_id", "") or "").strip() or trader_id

    def _current_running_session_filter_label(self) -> str:
        selected_filter: object = getattr(self, "running_session_filter", "全部")
        if hasattr(selected_filter, "get"):
            label = str(selected_filter.get() or "").strip()
        else:
            label = str(selected_filter or "").strip()
        if label in RUNNING_SESSION_FILTER_OPTIONS:
            return label
        return "全部"

    def _session_matches_running_filter(self, session: StrategySession) -> bool:
        selected_filter = QuantApp._current_running_session_filter_label(self)
        if selected_filter == "全部":
            return True
        return QuantApp._session_category_label(session) == selected_filter

    @staticmethod
    def _build_duplicate_launch_conflict_warning(
        session: StrategySession,
        conflicts: list[StrategySession],
    ) -> str:
        if not conflicts:
            return ""
        ordered = sorted(conflicts, key=lambda item: (item.started_at, item.session_id))
        session_refs = ", ".join(item.session_id for item in ordered)
        return (
            f"重复风险：与 {session_refs} 参数完全相同（同 API）。"
            " 如需复制参数开新策略，请先修改标的或切换 API 后再启动。"
        )

    def _refresh_running_session_summary(self) -> None:
        self._refresh_session_live_pnl_cache()
        active_sessions = [
            session for session in self.sessions.values() if self._session_counts_toward_running_summary(session)
        ]
        if not active_sessions:
            self.session_summary_text.set("多策略合计：当前没有运行中的策略。")
            return

        net_total = Decimal("0")
        live_total = Decimal("0")
        live_covered = 0
        latest_refresh_at: datetime | None = None
        for session in active_sessions:
            net_total += session.net_pnl_total or Decimal("0")
            live_pnl, refreshed_at = self._session_live_pnl_snapshot(session)
            if refreshed_at is not None and (latest_refresh_at is None or refreshed_at > latest_refresh_at):
                latest_refresh_at = refreshed_at
            if live_pnl is None:
                continue
            live_total += live_pnl
            live_covered += 1

        parts = [
            f"多策略合计：{len(active_sessions)} 个策略",
            f"实时浮盈亏={_format_optional_usdt_precise(live_total, places=2) if live_covered else '-'}",
            f"净盈亏={_format_optional_usdt_precise(net_total, places=2)}",
        ]
        duplicate_groups = QuantApp._active_duplicate_strategy_groups(self)
        if duplicate_groups:
            duplicate_sessions = sum(len(items) for items in duplicate_groups.values())
            parts.append(f"重复风险 {len(duplicate_groups)}组/{duplicate_sessions}条")
        if live_covered < len(active_sessions):
            parts.append(f"浮盈覆盖 {live_covered}/{len(active_sessions)}")
        selected_filter = QuantApp._current_running_session_filter_label(self)
        if selected_filter != "全部":
            visible_count = sum(1 for session in active_sessions if QuantApp._session_matches_running_filter(self, session))
            parts.append(f"当前筛选 {selected_filter} {visible_count}条")
        if latest_refresh_at is not None:
            parts.append(f"参考持仓 {latest_refresh_at.strftime('%H:%M:%S')}")
        else:
            parts.append("实时浮盈待持仓刷新")
        self.session_summary_text.set(" | ".join(parts))

    def _refresh_running_session_tree(self) -> None:
        tree = self.session_tree
        selected_before = tree.selection()[0] if tree.selection() else None
        for session in self.sessions.values():
            self._upsert_session_row(session)

        remaining = tuple(tree.get_children())
        next_selection = None
        if selected_before and tree.exists(selected_before):
            next_selection = selected_before
        elif remaining:
            next_selection = remaining[0]
        if next_selection is not None:
            tree.selection_set(next_selection)
            tree.focus(next_selection)
            tree.see(next_selection)

    def _on_running_session_filter_changed(self, *_: object) -> None:
        self._refresh_running_session_summary()
        self._refresh_running_session_tree()
        self._refresh_selected_session_details()

    def _populate_positions_tree_from_groups(self, tree, visible_positions: list[OkxPosition]) -> None:
        groups = _group_positions_for_tree(visible_positions)
        for asset_label, buckets in groups.items():
            asset_id = _asset_group_row_id(asset_label)
            asset_positions = [item for bucket in buckets.values() for item in bucket]
            asset_metrics = _aggregate_position_metrics(asset_positions, self._upl_usdt_prices, self._position_instruments)
            asset_label_text = f"{asset_label} 风险单元"
            tree.insert(
                "",
                END,
                iid=asset_id,
                text=asset_label_text,
                values=_build_group_row_values("组合", asset_metrics),
                open=True,
                tags=("group", _pnl_tag(asset_metrics["upl"])),
            )
            self._position_row_payloads[asset_id] = {
                "kind": "group",
                "label": asset_label_text,
                "item": asset_positions,
                "metrics": asset_metrics,
            }

            for bucket_label, bucket_positions in buckets.items():
                if bucket_label == "__DIRECT__":
                    for position in bucket_positions:
                        self._insert_position_row(tree, asset_id, position, _position_tree_row_id(position))
                    continue

                bucket_id = _bucket_group_row_id(asset_label, bucket_label)
                bucket_metrics = _aggregate_position_metrics(
                    bucket_positions,
                    self._upl_usdt_prices,
                    self._position_instruments,
                )
                tree.insert(
                    asset_id,
                    END,
                    iid=bucket_id,
                    text=bucket_label,
                    values=_build_group_row_values("分组", bucket_metrics),
                    open=True,
                    tags=("group", _pnl_tag(bucket_metrics["upl"])),
                )
                self._position_row_payloads[bucket_id] = {
                    "kind": "group",
                    "label": bucket_label,
                    "item": bucket_positions,
                    "metrics": bucket_metrics,
                }
                for position in bucket_positions:
                    self._insert_position_row(tree, bucket_id, position, _position_tree_row_id(position))

    def _render_positions_view(self) -> None:
        selected_before = self.position_tree.selection()[0] if self.position_tree.selection() else None
        selected_payload = self._selected_position_payload()
        selected_position_key = None
        if selected_payload is not None and selected_payload["kind"] == "position":
            item = selected_payload["item"]
            if isinstance(item, OkxPosition):
                selected_position_key = _position_tree_row_id(item)

        self._positions_view_rendering = True
        try:
            self.position_tree.delete(*self.position_tree.get_children())
            self._position_row_payloads.clear()
            visible_positions = self._positions_zoom_visible_positions()
            self._populate_positions_tree_from_groups(self.position_tree, visible_positions)
            self._update_position_summary(visible_positions)
            self._update_position_metrics(visible_positions)

            target = _resolve_position_selection_target(
                existing_ids=set(self._position_row_payloads.keys()),
                selected_position_key=selected_position_key,
                protection_position_key=self._protection_form_position_key,
                selected_before=selected_before,
                top_items=self.position_tree.get_children(),
            )

            if target is not None:
                self.position_tree.selection_set(target)
                self.position_tree.focus(target)
        finally:
            self._positions_view_rendering = False
        self._refresh_position_detail_panel()
        self._sync_positions_zoom_window()
        self._refresh_protection_window_view()

    def _insert_position_row(self, tree, parent_id: str, position: OkxPosition, row_id: str) -> None:
        label = position.inst_id
        if position.pos_side and position.pos_side.lower() != "net":
            label = f"{label} [{position.pos_side}]"
        tags = [tag for tag in (_pnl_tag(position.unrealized_pnl), _margin_mode_tag(position.mgn_mode)) if tag]
        tree.insert(
            parent_id,
            END,
            iid=row_id,
            text=label,
            values=(
                position.inst_type,
                _format_margin_mode(position.mgn_mode),
                _format_position_option_price_component(position, self._upl_usdt_prices, component="time_value"),
                _format_position_option_component_usdt(position, self._upl_usdt_prices, component="time_value"),
                _format_position_option_price_component(position, self._upl_usdt_prices, component="intrinsic_value"),
                _format_position_option_component_usdt(position, self._upl_usdt_prices, component="intrinsic_value"),
                _format_position_quote_price(
                    position,
                    self._position_instruments,
                    self._position_tickers,
                    side="bid",
                ),
                _format_position_quote_price_usdt(
                    position,
                    self._position_tickers,
                    self._upl_usdt_prices,
                    side="bid",
                ),
                _format_position_quote_price(
                    position,
                    self._position_instruments,
                    self._position_tickers,
                    side="ask",
                ),
                _format_position_quote_price_usdt(
                    position,
                    self._position_tickers,
                    self._upl_usdt_prices,
                    side="ask",
                ),
                _format_mark_price(position),
                _format_position_mark_price_usdt(position, self._upl_usdt_prices),
                _format_position_avg_price(position, self._position_instruments),
                _format_position_avg_price_usdt(position, self._upl_usdt_prices),
                _format_optional_approx_usdt(
                    _position_signed_open_value_approx_usdt(position, self._position_instruments, self._upl_usdt_prices)
                ),
                _format_position_size(position, self._position_instruments),
                _format_option_trade_side_display(position),
                _format_position_unrealized_pnl(position),
                _format_optional_usdt(_position_unrealized_pnl_usdt(position, self._upl_usdt_prices)),
                _format_optional_decimal_fixed(position.realized_pnl, places=5, with_sign=True),
                _format_optional_usdt(_position_realized_pnl_usdt(position, self._upl_usdt_prices)),
                _format_position_market_value(position, self._position_instruments, self._upl_usdt_prices),
                _format_optional_decimal(position.liquidation_price),
                _format_ratio(position.margin_ratio, places=2),
                _format_optional_integer(position.initial_margin),
                _format_optional_integer(position.maintenance_margin),
                _format_optional_decimal_fixed(_position_delta_value(position, self._position_instruments), places=5),
                _format_optional_decimal_fixed(position.gamma, places=5),
                _format_optional_decimal_fixed(position.vega, places=5),
                _format_optional_decimal_fixed(position.theta, places=5),
                _format_optional_usdt_precise(_position_theta_usdt(position, self._upl_usdt_prices), places=2),
                self._current_position_note_summary(position),
            ),
            tags=tuple(tags),
        )
        self._position_row_payloads[row_id] = {
            "kind": "position",
            "label": label,
            "item": position,
            "metrics": None,
        }

    def _update_position_summary(self, visible_positions: list[OkxPosition]) -> None:
        timestamp = self._positions_last_refresh_at.strftime("%H:%M:%S") if self._positions_last_refresh_at else "--:--:--"
        parts: list[str] = []
        if self._positions_context_note:
            parts.append(self._positions_context_note)
        parts.append(f"API配置：{self._current_credential_profile()}")

        total_count = len(self._latest_positions)
        visible_count = len(visible_positions)
        if total_count:
            summary = f"当前仓位（{total_count}）"
            if visible_count != total_count:
                summary += f"，当前显示 {visible_count}"
            parts.append(summary)
        else:
            parts.append("当前没有持仓")

        filter_text = _format_position_filter_summary(
            self.positions_zoom_type_filter.get(),
            self.positions_zoom_keyword.get(),
        )
        if filter_text:
            parts.append(f"筛选：{filter_text}")
        if self.position_auto_refresh_enabled:
            parts.append(f"自动刷新：{self.position_refresh_interval_label.get()}")
        else:
            parts.append("自动刷新：已暂停")
        parts.append(f"最近刷新：{timestamp}")
        self.positions_summary_text.set(" | ".join(parts))

    def _update_position_metrics(self, visible_positions: list[OkxPosition]) -> None:
        metrics = _aggregate_position_metrics(visible_positions, self._upl_usdt_prices, self._position_instruments)
        visible_count = len(visible_positions)
        total_count = len(self._latest_positions)
        self.position_total_text.set(
            f"{visible_count} 笔" if visible_count == total_count else f"{visible_count} / {total_count} 笔"
        )
        self.position_upl_text.set(
            _format_optional_usdt(metrics["upl_usdt"] if isinstance(metrics["upl_usdt"], Decimal) else None)
        )
        self.position_realized_text.set(
            _format_optional_decimal_fixed(
                metrics["realized"] if isinstance(metrics["realized"], Decimal) else None,
                places=2,
                with_sign=True,
            )
        )
        self.position_margin_text.set(
            _format_optional_integer(metrics["imr"] if isinstance(metrics["imr"], Decimal) else None)
        )
        self.position_delta_text.set(
            _format_summary_delta(metrics["delta"] if isinstance(metrics["delta"], Decimal) else None)
        )
        self.position_short_call_text.set(
            _format_filtered_option_position_size(
                visible_positions,
                self._position_instruments,
                option_kind="C",
                direction="short",
            )
        )
        self.position_short_put_text.set(
            _format_filtered_option_position_size(
                visible_positions,
                self._position_instruments,
                option_kind="P",
                direction="short",
            )
        )
        self.position_long_call_text.set(
            _format_filtered_option_position_size(
                visible_positions,
                self._position_instruments,
                option_kind="C",
                direction="long",
            )
        )
        self.position_long_put_text.set(
            _format_filtered_option_position_size(
                visible_positions,
                self._position_instruments,
                option_kind="P",
                direction="long",
            )
        )

    def _selected_position_payload(self) -> dict[str, object] | None:
        selection = self.position_tree.selection()
        if not selection:
            return None
        return self._position_row_payloads.get(selection[0])

    def _selected_position_item(self) -> OkxPosition | None:
        payload = self._selected_position_payload()
        if payload is None or payload.get("kind") != "position":
            return None
        position = payload.get("item")
        return position if isinstance(position, OkxPosition) else None

    def _position_action_parent(self):
        return self._positions_zoom_window or self.root

    @staticmethod
    def _normalize_position_manual_flatten_mode(flatten_mode: str) -> str:
        normalized = str(flatten_mode or "").strip().lower()
        if normalized == "best_quote":
            return "best_quote"
        return "market"

    @staticmethod
    def _position_manual_flatten_mode_label(flatten_mode: str) -> str:
        if QuantApp._normalize_position_manual_flatten_mode(flatten_mode) == "best_quote":
            return "挂买一/卖一平仓"
        return "市价平仓"

    def _build_selected_position_manual_flatten_config(self, position: OkxPosition) -> StrategyConfig:
        environment = self._positions_effective_environment or ENV_OPTIONS[self.environment_label.get()]
        normalized_mgn_mode = (position.mgn_mode or "").strip().lower()
        trade_mode = normalized_mgn_mode if normalized_mgn_mode in {"cross", "isolated", "cash"} else TRADE_MODE_OPTIONS[self.trade_mode_label.get()]
        position_mode = "long_short" if position.pos_side and position.pos_side.lower() != "net" else "net"
        direction = derive_position_direction(position)
        return StrategyConfig(
            inst_id=position.inst_id,
            bar="1m",
            ema_period=1,
            atr_period=1,
            atr_stop_multiplier=Decimal("1"),
            atr_take_multiplier=Decimal("1"),
            order_size=abs(position.position),
            trade_mode=trade_mode,
            signal_mode="long_only" if direction == "long" else "short_only",
            position_mode=position_mode,
            environment=environment,
            tp_sl_trigger_type="last",
            strategy_id="manual_position_flatten",
            poll_seconds=10.0,
            risk_amount=None,
            trade_inst_id=position.inst_id,
            tp_sl_mode="local_trade",
            local_tp_sl_inst_id=position.inst_id,
            entry_side_mode="follow_signal",
            run_mode="trade",
        )

    def _selected_position_close_size(self, position: OkxPosition) -> Decimal:
        base = position.avail_position
        if base is None or base == 0:
            base = position.position
        return abs(base)

    def _prepare_selected_position_manual_flatten(
        self,
        position: OkxPosition,
        flatten_mode: str,
        *,
        close_size: Decimal | None = None,
    ) -> tuple[Credentials, StrategyConfig, Instrument, Decimal, str, str | None, str, str]:
        profile_name = (self._positions_context_profile_name or self._current_credential_profile()).strip()
        credentials = self._credentials_for_profile_or_none(profile_name)
        if credentials is None:
            raise ValueError("当前持仓所属 API 未配置有效凭证，无法执行选中持仓平仓。")
        normalized_flatten_mode = self._normalize_position_manual_flatten_mode(flatten_mode)
        config = self._build_selected_position_manual_flatten_config(position)
        instrument = self.client.get_instrument(position.inst_id)
        max_close = snap_to_increment(
            self._selected_position_close_size(position),
            instrument.lot_size,
            "down",
        )
        if max_close < instrument.min_size:
            raise ValueError("当前选中持仓的可平数量不足最小下单量，无法直接平仓。")
        if close_size is not None:
            if close_size <= 0:
                raise ValueError("平仓数量必须大于 0。")
            req = snap_to_increment(close_size, instrument.lot_size, "down")
            if req <= 0:
                raise ValueError("平仓数量按合约最小变动单位向下取整后为 0，请加大数量。")
            if req > max_close:
                raise ValueError(f"平仓数量不能超过可平数量 {max_close}。")
            closeable_size = req
        else:
            closeable_size = max_close
        if closeable_size < instrument.min_size:
            raise ValueError("当前选中持仓的可平数量不足最小下单量，无法直接平仓。")
        direction = derive_position_direction(position)
        close_side = "sell" if direction == "long" else "buy"
        pos_side = None
        if config.position_mode == "long_short":
            normalized_pos_side = (position.pos_side or "").strip().lower()
            pos_side = normalized_pos_side if normalized_pos_side in {"long", "short"} else direction
        return (
            credentials,
            config,
            instrument,
            closeable_size,
            close_side,
            pos_side,
            direction,
            normalized_flatten_mode,
        )

    def _submit_selected_position_manual_flatten(
        self,
        position: OkxPosition,
        flatten_mode: str,
        *,
        close_size: Decimal | None = None,
    ) -> tuple[OkxOrderResult, Decimal | None, str]:
        profile_name = (self._positions_context_profile_name or self._current_credential_profile()).strip()
        credentials = self._credentials_for_profile_or_none(profile_name)
        if credentials is None:
            raise ValueError("当前持仓所属 API 未配置有效凭证，无法执行选中持仓平仓。")
        normalized_flatten_mode = self._normalize_position_manual_flatten_mode(flatten_mode)
        config = self._build_selected_position_manual_flatten_config(position)
        instrument = self.client.get_instrument(position.inst_id)
        max_close = snap_to_increment(
            self._selected_position_close_size(position),
            instrument.lot_size,
            "down",
        )
        if max_close < instrument.min_size:
            raise ValueError("当前选中持仓的可平数量不足最小下单量，无法直接平仓。")
        if close_size is not None:
            if close_size <= 0:
                raise ValueError("平仓数量必须大于 0。")
            req = snap_to_increment(close_size, instrument.lot_size, "down")
            if req <= 0:
                raise ValueError("平仓数量按合约最小变动单位向下取整后为 0，请加大数量。")
            if req > max_close:
                raise ValueError(f"平仓数量不能超过可平数量 {max_close}。")
            closeable_size = req
        else:
            closeable_size = max_close
        if closeable_size < instrument.min_size:
            raise ValueError("当前选中持仓的可平数量不足最小下单量，无法直接平仓。")
        direction = derive_position_direction(position)
        close_side = "sell" if direction == "long" else "buy"
        pos_side = None
        if config.position_mode == "long_short":
            normalized_pos_side = (position.pos_side or "").strip().lower()
            pos_side = normalized_pos_side if normalized_pos_side in {"long", "short"} else direction
        if normalized_flatten_mode == "best_quote":
            price = self._resolve_trader_best_quote_flatten_price(instrument, side=close_side)
            result = self.client.place_simple_order(
                credentials,
                config,
                inst_id=position.inst_id,
                side=close_side,
                size=closeable_size,
                ord_type="limit",
                pos_side=pos_side,
                price=price,
                reduce_only=True,
            )
            return result, price, normalized_flatten_mode
        result = self.client.place_simple_order(
            credentials,
            config,
            inst_id=position.inst_id,
            side=close_side,
            size=closeable_size,
            ord_type="market",
            pos_side=pos_side,
            reduce_only=True,
        )
        return result, None, normalized_flatten_mode

    def flatten_selected_position(self) -> None:
        position = self._selected_position_item()
        parent = self._position_action_parent()
        if position is None:
            messagebox.showinfo("平仓", "请先在当前持仓里选中一条具体持仓。", parent=parent)
            return
        choice = messagebox.askyesnocancel(
            "选中持仓平仓方式",
            "请选择这次对选中持仓的平仓方式。\n\n"
            "是：市价平仓\n"
            "否：挂买一/卖一平仓\n"
            "取消：不执行\n\n"
            "说明：平空会按买一挂单，平多会按卖一挂单；未成交前持仓不会消失。",
            parent=parent,
        )
        if choice is None:
            return
        flatten_mode = "market" if choice else "best_quote"
        mode_label = self._position_manual_flatten_mode_label(flatten_mode)
        try:
            result, price, normalized_flatten_mode = self._submit_selected_position_manual_flatten(position, flatten_mode)
        except Exception as exc:
            messagebox.showerror("平仓失败", str(exc), parent=parent)
            return
        order_id = (result.ord_id or "-").strip() or "-"
        client_order_id = (result.cl_ord_id or "-").strip() or "-"
        message = (
            f"已提交选中持仓平仓。\n\n"
            f"合约：{position.inst_id}\n"
            f"方式：{mode_label}\n"
            f"订单ID：{order_id}\n"
            f"客户端单号：{client_order_id}"
        )
        if normalized_flatten_mode == "best_quote" and price is not None:
            message = f"{message}\n挂单价：{format_decimal(price)}"
        messagebox.showinfo("平仓已提交", message, parent=parent)
        profile_name = (self._positions_context_profile_name or self._current_credential_profile()).strip()
        log_prefix = f"[{profile_name}] [当前持仓]" if profile_name else "[当前持仓]"
        self._enqueue_log(
            f"{log_prefix} 已提交选中持仓平仓 | {position.inst_id} | 方式={mode_label} | ordId={order_id}"
        )
        self.refresh_positions()
        self.refresh_order_views()

    def _submit_selected_position_manual_flatten(
        self,
        position: OkxPosition,
        flatten_mode: str,
        *,
        close_size: Decimal | None = None,
    ) -> tuple[OkxOrderResult, Decimal | None, str]:
        (
            credentials,
            config,
            instrument,
            closeable_size,
            close_side,
            pos_side,
            _direction,
            normalized_flatten_mode,
        ) = QuantApp._prepare_selected_position_manual_flatten(
            self,
            position,
            flatten_mode,
            close_size=close_size,
        )
        if normalized_flatten_mode == "best_quote":
            price = self._resolve_trader_best_quote_flatten_price(instrument, side=close_side)
            result = self.client.place_simple_order(
                credentials,
                config,
                inst_id=position.inst_id,
                side=close_side,
                size=closeable_size,
                ord_type="limit",
                pos_side=pos_side,
                price=price,
                reduce_only=True,
            )
            return result, price, normalized_flatten_mode
        result = self.client.place_simple_order(
            credentials,
            config,
            inst_id=position.inst_id,
            side=close_side,
            size=closeable_size,
            ord_type="market",
            pos_side=pos_side,
            reduce_only=True,
        )
        return result, None, normalized_flatten_mode

    def flatten_selected_position(self) -> None:
        position = self._selected_position_item()
        parent = self._position_action_parent()
        if position is None:
            messagebox.showinfo("平仓", "请先在当前持仓里选中一条具体持仓。", parent=parent)
            return
        try:
            (
                _credentials,
                _config,
                _instrument,
                preview_close_size,
                preview_close_side,
                _pos_side,
                preview_direction,
                _normalized_mode,
            ) = QuantApp._prepare_selected_position_manual_flatten(self, position, "market")
        except Exception as exc:
            messagebox.showerror("平仓失败", str(exc), parent=parent)
            return

        direction_label = "多头" if preview_direction == "long" else "空头"
        close_side_label = "SELL 卖出平仓" if preview_close_side == "sell" else "BUY 买入平仓"
        hold_size_text = format_decimal(abs(position.position))
        closeable_size_text = format_decimal(self._selected_position_close_size(position))
        submit_size_text = format_decimal(preview_close_size)
        choice = messagebox.askyesnocancel(
            "选中持仓平仓方式",
            "请选择这次对选中持仓的平仓方式。\n\n"
            f"合约：{position.inst_id}\n"
            f"方向：{direction_label}\n"
            f"当前持仓：{hold_size_text}\n"
            f"当前可平：{closeable_size_text}\n"
            f"本次将报单平仓数量：{submit_size_text}\n"
            f"实际报单方向：{close_side_label}\n\n"
            "是：市价平仓\n"
            "否：挂买一/卖一平仓\n"
            "取消：不执行\n\n"
            "说明：\n"
            "1. 市价平仓会立即按市场可成交价格报单。\n"
            "2. 挂买一/卖一平仓会先挂单，未成交前持仓不会消失。\n"
            "3. 挂买一/卖一时，平多按卖一挂单，平空按买一挂单。",
            parent=parent,
        )
        if choice is None:
            return
        flatten_mode = "market" if choice else "best_quote"
        mode_label = self._position_manual_flatten_mode_label(flatten_mode)
        try:
            result, price, normalized_flatten_mode = self._submit_selected_position_manual_flatten(position, flatten_mode)
        except Exception as exc:
            messagebox.showerror("平仓失败", str(exc), parent=parent)
            return
        order_id = (result.ord_id or "-").strip() or "-"
        client_order_id = (result.cl_ord_id or "-").strip() or "-"
        message = (
            f"已提交选中持仓平仓。\n\n"
            f"合约：{position.inst_id}\n"
            f"方向：{direction_label}\n"
            f"平仓数量：{submit_size_text}\n"
            f"报单方向：{close_side_label}\n"
            f"方式：{mode_label}\n"
            f"订单ID：{order_id}\n"
            f"客户端单号：{client_order_id}"
        )
        if normalized_flatten_mode == "best_quote" and price is not None:
            message = f"{message}\n挂单价：{format_decimal(price)}"
        messagebox.showinfo("平仓已提交", message, parent=parent)
        profile_name = (self._positions_context_profile_name or self._current_credential_profile()).strip()
        log_prefix = f"[{profile_name}] [当前持仓]" if profile_name else "[当前持仓]"
        self._enqueue_log(
            f"{log_prefix} 已提交选中持仓平仓 | {position.inst_id} | 方向={direction_label} | "
            f"数量={submit_size_text} | 报单方向={close_side_label} | 方式={mode_label} | ordId={order_id}"
        )
        self.refresh_positions()
        self.refresh_order_views()

    def _on_position_selected(self, *_: object) -> None:
        if self._position_selection_syncing or self._positions_view_rendering:
            return
        selection = self.position_tree.selection()
        if selection and self._position_selection_suppressed_item_id == selection[0]:
            self._position_selection_suppressed_item_id = None
            return
        self._position_selection_suppressed_item_id = None
        self._refresh_position_detail_panel()

    def _refresh_position_detail_panel(self) -> None:
        payload = self._selected_position_payload()
        if payload is None:
            self.position_detail_text.set(self._default_position_detail_text())
        elif payload["kind"] == "position":
            position = payload["item"]
            if isinstance(position, OkxPosition):
                self.position_detail_text.set(
                    _build_position_detail_text(
                        position,
                        self._upl_usdt_prices,
                        self._position_instruments,
                        note=self._current_position_note_text(position),
                    )
                )
        else:
            label = payload["label"]
            positions = payload["item"]
            metrics = payload["metrics"]
            if isinstance(label, str) and isinstance(positions, list) and isinstance(metrics, dict):
                self.position_detail_text.set(
                    _build_group_detail_text(
                        label,
                        positions,
                        metrics,
                        self._upl_usdt_prices,
                        self._position_instruments,
                    )
                )
        self._set_readonly_text(self._position_detail_panel, self.position_detail_text.get())
        if self._positions_zoom_tree is not None:
            selection = self.position_tree.selection()
            if selection and self._positions_zoom_tree.exists(selection[0]):
                self._sync_positions_zoom_selection(selection[0])
        self._refresh_positions_zoom_detail()
        self._refresh_protection_window_view()

    def _apply_positions_error(self, message: str) -> None:
        friendly_message = _format_network_error_message(message)
        _mark_refresh_health_failure(self._positions_refresh_health, friendly_message)
        self._refresh_all_refresh_badges()
        suffix = _format_refresh_health_suffix(self._positions_refresh_health)
        has_previous_positions = (
            bool(self._latest_positions)
            or bool(self._position_row_payloads)
            or self._positions_last_refresh_at is not None
        )
        self._positions_refreshing = False
        if has_previous_positions:
            summary = f"持仓刷新失败，继续显示上一份缓存：{friendly_message}{suffix}"
            self.positions_summary_text.set(summary)
            self._sync_positions_zoom_window()
            self._refresh_positions_zoom_detail()
            self._enqueue_log(summary)
            return
        self._latest_positions = []
        self._positions_context_note = None
        self._positions_context_profile_name = None
        self._positions_last_refresh_at = None
        self._positions_effective_environment = None
        self._upl_usdt_prices = {}
        self._position_instruments = {}
        self._position_tickers = {}
        self.position_tree.delete(*self.position_tree.get_children())
        self._position_row_payloads.clear()
        self.position_total_text.set("-")
        self.position_upl_text.set("-")
        self.position_realized_text.set("-")
        self.position_margin_text.set("-")
        self.position_delta_text.set("-")
        self.position_short_call_text.set("-")
        self.position_short_put_text.set("-")
        self.position_long_call_text.set("-")
        self.position_long_put_text.set("-")
        self.position_detail_text.set(self._default_position_detail_text())
        self._set_readonly_text(self._position_detail_panel, self.position_detail_text.get())
        self._sync_positions_zoom_window()
        self._refresh_positions_zoom_detail()
        summary = f"持仓读取失败：{friendly_message}{suffix}"
        self.positions_summary_text.set(summary)
        self._enqueue_log(summary)

    @staticmethod
    def _position_takeover_entry_ts_ms(position: OkxPosition) -> int:
        """优先使用 OKX 返回的持仓创建时间（毫秒），供动态止盈里 holding_bars / 时间保本等逻辑使用。"""
        raw = position.raw if isinstance(getattr(position, "raw", None), dict) else {}
        for key in ("cTime", "CTime"):
            val = raw.get(key)
            if val is None:
                continue
            try:
                ms = int(str(val).strip())
            except (TypeError, ValueError):
                continue
            if ms <= 0:
                continue
            # OKX 账户持仓 cTime 一般为毫秒；若误为秒级时间戳则放大
            if ms < 1_000_000_000_000:
                ms *= 1000
            return ms
        return int(time.time() * 1000)

    def _position_takeover_stop_algo_candidates(
        self,
        position: OkxPosition,
        pending_orders: list,
        *,
        entry_price: Decimal,
    ) -> list:
        """筛选与当前持仓方向一致、疑似止损平仓的 OKX 算法委托（永续/交割）。"""
        close_side = "sell" if derive_position_direction(position) == "long" else "buy"
        pos_norm = (position.pos_side or "").strip().lower()
        out: list = []
        for item in pending_orders:
            if not hasattr(item, "source_kind") or getattr(item, "source_kind", "") != "algo":
                continue
            if (getattr(item, "inst_id", "") or "").strip().upper() != position.inst_id.strip().upper():
                continue
            sl_px = getattr(item, "stop_loss_trigger_price", None) or getattr(item, "trigger_price", None)
            if sl_px is None:
                continue
            side_raw = (getattr(item, "side", None) or "").strip().lower()
            if side_raw and side_raw != close_side:
                continue
            item_ps = (getattr(item, "pos_side", None) or "").strip().lower()
            if pos_norm in {"long", "short"} and item_ps and item_ps != pos_norm:
                continue
            if derive_position_direction(position) == "long":
                if sl_px >= entry_price:
                    continue
            else:
                if sl_px <= entry_price:
                    continue
            aid = (getattr(item, "algo_id", None) or "").strip()
            if not aid and not (getattr(item, "algo_client_order_id", None) or "").strip():
                continue
            out.append(item)
        return out

    def _find_position_for_takeover_pending_algo(
        self,
        item: OkxTradeOrderItem,
    ) -> tuple[OkxPosition, Decimal] | None:
        """按算法止损委托反查账户中与之规则一致的永续/交割持仓（含止损价相对开仓方向校验）。"""
        inst_id_item = (item.inst_id or "").strip().upper()
        if not inst_id_item:
            return None
        for position in self._latest_positions:
            if (position.inst_id or "").strip().upper() != inst_id_item:
                continue
            if infer_inst_type(position.inst_id) not in {"SWAP", "FUTURES"}:
                continue
            entry = position.avg_price
            if entry is None or entry <= 0:
                continue
            if self._position_takeover_stop_algo_candidates(position, [item], entry_price=entry):
                return position, entry
        return None

    def _position_takeover_algo_label(self, item) -> str:
        aid = (getattr(item, "algo_id", None) or "").strip() or "-"
        acl = (getattr(item, "algo_client_order_id", None) or "").strip() or "-"
        sl_px = getattr(item, "stop_loss_trigger_price", None) or getattr(item, "trigger_price", None)
        sl_txt = format_decimal(sl_px) if sl_px is not None else "-"
        ot = (getattr(item, "ord_type", None) or "").strip() or "-"
        return f"{ot} | 止损触发≈{sl_txt} | algoId={aid} | algoClOrdId={acl}"

    def _positions_zoom_takeover_idle_caption(self) -> str:
        return (
            "当前无运行中的动态止盈接管。可在「当前委托」选中条件止损后点「从选中条件单接管」，"
            "或使用工具栏「从选中持仓接管」在候选算法单中挑选；不同算法止损单可并行接管多条，"
            "同一算法单仅允许一条；拉取委托在后台执行，避免大窗长时间卡住。"
        )

    @staticmethod
    def _takeover_algo_busy_slot_key(inst_id: str, algo_id: str | None, algo_cl: str | None) -> str:
        iu = (inst_id or "").strip().upper()
        return f"{iu}|{(algo_id or '').strip()}|{(algo_cl or '').strip()}"

    def _prune_dead_takeover_sessions(self) -> None:
        """清理已结束但未从字典移除的接管会话（防御性，避免表格残留僵尸行）。"""
        sessions = getattr(self, "_position_takeover_sessions", None)
        if not isinstance(sessions, dict) or not sessions:
            return
        for sid in list(sessions.keys()):
            st = sessions.get(sid)
            if not isinstance(st, dict):
                sessions.pop(sid, None)
                continue
            if not st.get("thread_started"):
                continue
            th = st.get("thread")
            if th is None:
                continue
            if getattr(th, "is_alive", lambda: False)():
                continue
            sessions.pop(sid, None)

    def _takeover_running_algo_session_id(
        self,
        inst_id: str,
        algo_id: str | None,
        algo_cl: str | None,
    ) -> str | None:
        """若同一合约、同一 algoId 或同一 algoClOrdId 已有监控线程在跑，返回其会话 id。"""
        inst_u = (inst_id or "").strip().upper()
        aid = (algo_id or "").strip()
        acl = (algo_cl or "").strip()
        sessions = getattr(self, "_position_takeover_sessions", None) or {}
        for sid, st in sessions.items():
            if not isinstance(st, dict):
                continue
            th = st.get("thread")
            if th is None or not getattr(th, "is_alive", lambda: False)():
                continue
            if (st.get("inst_id") or "").strip().upper() != inst_u:
                continue
            st_aid = (st.get("algo_id") or "").strip()
            st_acl = (st.get("algo_cl") or "").strip()
            if aid and st_aid == aid:
                return sid
            if acl and st_acl == acl:
                return sid
        return None

    def _takeover_running_session_ids(self) -> list[str]:
        sessions = getattr(self, "_position_takeover_sessions", None) or {}
        out: list[str] = []
        for sid, st in sessions.items():
            if not isinstance(st, dict):
                continue
            th = st.get("thread")
            if th is not None and getattr(th, "is_alive", lambda: False)():
                out.append(sid)
        return out

    def _refresh_positions_zoom_takeover_panel(self) -> None:
        win = getattr(self, "_positions_zoom_window", None)
        if win is None:
            return
        try:
            if not win.winfo_exists():
                return
        except Exception:
            return
        self._prune_dead_takeover_sessions()
        tv = getattr(self, "_positions_zoom_takeover_status_text", None)
        tree = getattr(self, "_positions_zoom_takeover_tree", None)
        sessions = getattr(self, "_position_takeover_sessions", None) or {}
        if tree is not None and _widget_exists(tree):
            try:
                tree.delete(*tree.get_children())
            except Exception:
                pass
        rows = 0
        if tree is not None and _widget_exists(tree):
            for sid, st in list(sessions.items()):
                if not isinstance(st, dict):
                    continue
                th = st.get("thread")
                if th is None or not getattr(th, "is_alive", lambda: False)():
                    continue
                summ = st.get("summary")
                if not isinstance(summ, dict):
                    continue
                try:
                    tree.insert(
                        "",
                        END,
                        iid=sid,
                        values=(
                            str(summ.get("session_id") or sid),
                            str(summ.get("api") or "-"),
                            str(summ.get("inst_id") or "-"),
                            str(summ.get("template") or "-"),
                            str(summ.get("entry") or "-"),
                            str(summ.get("size") or "-"),
                            str(summ.get("stop") or "-"),
                            str(summ.get("status") or "-"),
                        ),
                    )
                    rows += 1
                except Exception:
                    pass
        if tv is not None:
            if rows == 0:
                tv.set(self._positions_zoom_takeover_idle_caption())
            elif rows == 1:
                tv.set("当前有 1 条动态止盈接管在运行（见下表）；停止时请先选中该行再点「停止接管」。")
            else:
                tv.set(f"当前有 {rows} 条动态止盈接管在并行运行（见下表）；停止时请先选中要停的那一行再点「停止接管」。")

    def _takeover_prefetch_worker(self, request_id: int) -> None:
        ctx = getattr(self, "_takeover_prefetch_context", None)
        if not isinstance(ctx, dict):
            return
        credentials = ctx["credentials"]
        desk_env = ctx["desk_env"]
        err: BaseException | None = None
        pending: list | None = None
        try:
            pending = self.client.get_pending_orders(
                credentials,
                environment=desk_env,
                inst_types=("SWAP", "FUTURES"),
                limit=200,
            )
        except BaseException as exc:
            err = exc
        self.root.after(0, lambda: self._takeover_prefetch_apply_on_main(request_id, pending, err))

    def _takeover_prefetch_apply_on_main(
        self,
        request_id: int,
        pending: list | None,
        err: BaseException | None,
    ) -> None:
        self._takeover_open_flow_busy = False
        if request_id != getattr(self, "_takeover_prefetch_request_id", 0):
            return
        ctx = self._takeover_prefetch_context
        self._takeover_prefetch_context = None
        parent = self._position_action_parent()
        if not isinstance(ctx, dict):
            self._refresh_positions_zoom_takeover_panel()
            return
        position = ctx["position"]
        entry = ctx["entry"]
        record = ctx["record"]
        config = ctx["config"]
        if err is not None:
            messagebox.showerror("接管动态止盈", f"读取当前委托失败：{err}", parent=parent)
            self._refresh_positions_zoom_takeover_panel()
            return
        candidates = self._position_takeover_stop_algo_candidates(position, pending or [], entry_price=entry)
        if not candidates:
            messagebox.showinfo(
                "接管动态止盈",
                "未找到与当前持仓匹配的待触发止损类算法单。请确认已在 OKX 挂好止损，"
                "或先在本窗口点「刷新」再试。",
                parent=parent,
            )
            self._refresh_positions_zoom_takeover_panel()
            return
        self._positions_zoom_takeover_status_text.set(
            f"已拉取委托列表（{len(candidates)} 条候选）。请在弹出窗口中选择要接管的止损算法单。"
        )
        self._takeover_show_pick_dialog_and_maybe_start(ctx, candidates)

    def _takeover_fill_contract_size(self, position: OkxPosition, *, order_contracts: Decimal | None) -> Decimal:
        """FilledPosition.size：默认整仓；若指定条件/算法单 sz（与 OKX 持仓同一合约单位），则不超过该单与当前可平张数。"""
        pos_abs = abs(Decimal(str(position.position)))
        if order_contracts is None or order_contracts <= 0:
            return pos_abs
        max_close = self._selected_position_close_size(position)
        cap = min(pos_abs, max_close)
        return min(order_contracts, cap)

    def _takeover_enqueue_instrument_fetch(
        self,
        *,
        credentials,
        config,
        record,
        profile_name: str,
        position: OkxPosition,
        entry: Decimal,
        sl_px: Decimal,
        algo_id: str | None,
        algo_cl: str | None,
        order_contract_size: Decimal | None = None,
    ) -> None:
        direction = derive_position_direction(position)
        open_side: str = "buy" if direction == "long" else "sell"
        pos_side = resolve_open_pos_side(config, open_side)
        close_side = "sell" if open_side == "buy" else "buy"
        sz = self._takeover_fill_contract_size(position, order_contracts=order_contract_size)
        parent = self._position_action_parent()
        dup_sid = self._takeover_running_algo_session_id(position.inst_id, algo_id, algo_cl)
        if dup_sid:
            messagebox.showwarning(
                "接管动态止盈",
                f"该止损算法单已在接管中（会话 {dup_sid}）。请先停止该会话，再为同一算法委托启动新的接管。",
                parent=parent,
            )
            return
        slot = self._takeover_algo_busy_slot_key(position.inst_id, algo_id, algo_cl)
        pending = getattr(self, "_takeover_instrument_pending_slots", None)
        if not isinstance(pending, set):
            pending = set()
            self._takeover_instrument_pending_slots = pending
        if slot in pending:
            messagebox.showwarning(
                "接管动态止盈",
                "该止损算法单正在后台加载合约元数据，请稍候完成后再试。",
                parent=parent,
            )
            return
        pending.add(slot)
        self._positions_zoom_takeover_status_text.set(
            f"正在加载合约 {position.inst_id} 的元数据（后台请求），完成后自动启动接管线程…"
        )

        def _instrument_worker() -> None:
            inst_err: BaseException | None = None
            instrument = None
            try:
                instrument = self.client.get_instrument(position.inst_id)
            except BaseException as exc:
                inst_err = exc

            def _apply() -> None:
                try:
                    self._takeover_commit_after_instrument(
                        credentials=credentials,
                        config=config,
                        record=record,
                        profile_name=profile_name,
                        position=position,
                        entry=entry,
                        sl_px=sl_px,
                        algo_id=algo_id,
                        algo_cl=algo_cl,
                        open_side=open_side,
                        pos_side=pos_side,
                        close_side=close_side,
                        sz=sz,
                        instrument=instrument,
                        inst_err=inst_err,
                    )
                finally:
                    pend = getattr(self, "_takeover_instrument_pending_slots", None)
                    if isinstance(pend, set):
                        pend.discard(slot)

            self.root.after(0, _apply)

        threading.Thread(target=_instrument_worker, name="position-takeover-instrument", daemon=True).start()

    def _takeover_show_pick_dialog_and_maybe_start(self, ctx: dict, candidates: list) -> None:
        position = ctx["position"]
        parent = ctx["parent"]
        record = ctx["record"]
        config = ctx["config"]
        entry = ctx["entry"]
        credentials = ctx["credentials"]
        profile_name = ctx["profile_name"]

        dialog = Toplevel(parent)
        dialog.title("接管动态止盈 — 选择止损单")
        dialog.transient(parent)
        dialog.grab_set()
        ttk.Label(
            dialog,
            text=(
                f"合约：{position.inst_id}  开仓均价≈{format_decimal(entry)}\n"
                f"将沿用所选算法止损单，并按模板：{record.strategy_name} "
                f"（{config.tp_sl_trigger_type} 触发、{config.bar}、2R保本与手续费偏移等）自动上移止损。"
            ),
            wraplength=520,
            justify="left",
        ).pack(anchor="w", padx=12, pady=(12, 6))
        frame = ttk.Frame(dialog)
        frame.pack(fill="both", expand=True, padx=12, pady=6)
        scrollbar = ttk.Scrollbar(frame)
        scrollbar.pack(side="right", fill="y")
        lb = Listbox(frame, height=min(12, max(4, len(candidates))), width=96, yscrollcommand=scrollbar.set)
        lb.pack(side="left", fill="both", expand=True)
        scrollbar.config(command=lb.yview)
        for idx, item in enumerate(candidates):
            lb.insert(END, f"{idx + 1}. {self._position_takeover_algo_label(item)}")
        lb.selection_set(0)
        chosen: list[int | None] = [None]

        def on_ok() -> None:
            sel = lb.curselection()
            if not sel:
                messagebox.showinfo("接管动态止盈", "请先在列表中选择一条止损算法委托。", parent=dialog)
                return
            chosen[0] = int(sel[0])
            dialog.destroy()

        def on_cancel() -> None:
            chosen[0] = None
            dialog.destroy()

        btn_row = ttk.Frame(dialog)
        btn_row.pack(fill="x", padx=12, pady=(0, 12))
        ttk.Button(btn_row, text="开始接管", command=on_ok).pack(side="right", padx=(6, 0))
        ttk.Button(btn_row, text="取消", command=on_cancel).pack(side="right")
        dialog.protocol("WM_DELETE_WINDOW", on_cancel)
        dialog.wait_window()

        pick = chosen[0]
        if pick is None:
            self._refresh_positions_zoom_takeover_panel()
            return
        item = candidates[pick]
        sl_px = item.stop_loss_trigger_price or item.trigger_price
        if sl_px is None:
            messagebox.showerror("接管动态止盈", "所选委托缺少止损触发价。", parent=parent)
            self._refresh_positions_zoom_takeover_panel()
            return
        algo_id = (item.algo_id or "").strip() or None
        algo_cl = (item.algo_client_order_id or "").strip() or None
        if not algo_id and not algo_cl:
            messagebox.showerror("接管动态止盈", "所选委托缺少 algoId / algoClOrdId，无法改价。", parent=parent)
            self._refresh_positions_zoom_takeover_panel()
            return

        order_contracts = item.size if item.size is not None and item.size > 0 else None
        self._takeover_enqueue_instrument_fetch(
            credentials=credentials,
            config=config,
            record=record,
            profile_name=profile_name,
            position=position,
            entry=entry,
            sl_px=sl_px,
            algo_id=algo_id,
            algo_cl=algo_cl,
            order_contract_size=order_contracts,
        )

    def _takeover_commit_after_instrument(
        self,
        *,
        credentials,
        config,
        record,
        profile_name: str,
        position: OkxPosition,
        entry: Decimal,
        sl_px: Decimal,
        algo_id: str | None,
        algo_cl: str | None,
        open_side: str,
        pos_side,
        close_side: str,
        sz: Decimal,
        instrument,
        inst_err: BaseException | None,
    ) -> None:
        parent = self._position_action_parent()
        if inst_err is not None:
            messagebox.showerror("接管动态止盈", f"加载合约信息失败：{inst_err}", parent=parent)
            self._refresh_positions_zoom_takeover_panel()
            return
        if instrument is None:
            messagebox.showerror("接管动态止盈", "加载合约信息失败：未返回合约数据。", parent=parent)
            self._refresh_positions_zoom_takeover_panel()
            return

        dup_sid = self._takeover_running_algo_session_id(position.inst_id, algo_id, algo_cl)
        if dup_sid:
            messagebox.showwarning(
                "接管动态止盈",
                f"该止损算法单已在接管中（会话 {dup_sid}）。请先停止该会话，再为其它委托启动新的接管。",
                parent=parent,
            )
            self._refresh_positions_zoom_takeover_panel()
            return

        filled = FilledPosition(
            ord_id=f"takeover-{position.inst_id}",
            cl_ord_id=None,
            inst_id=position.inst_id,
            side=open_side,
            close_side=close_side,
            pos_side=pos_side,
            size=sz,
            entry_price=entry,
            entry_ts=self._position_takeover_entry_ts_ms(position),
        )

        api_log_name = (profile_name or getattr(credentials, "profile_name", "") or "").strip() or "-"
        api_part = "" if api_log_name == "-" else api_log_name
        takeover_session_id = self._next_session_id()
        if api_part:
            log_prefix = f"[{api_part}] [{takeover_session_id} {record.strategy_name} {position.inst_id}]"
        else:
            log_prefix = f"[{takeover_session_id} {record.strategy_name} {position.inst_id}]"

        def _log_takeover(msg: str) -> None:
            text = str(msg or "").strip()
            if text:
                self._enqueue_log(f"{log_prefix} {text}")

        engine = StrategyEngine(
            self.client,
            _log_takeover,
            strategy_name=record.strategy_name,
            session_id=takeover_session_id,
            api_name=api_part,
        )

        r_spread = abs(entry - sl_px)
        side_zh = "多" if open_side == "buy" else "空"
        size_text = _format_size_with_contract_equivalent(instrument, sz)
        summary = {
            "inst_id": position.inst_id,
            "session_id": takeover_session_id,
            "template": record.strategy_name,
            "api": api_log_name,
            "entry": format_decimal(entry),
            "size": size_text,
            "stop": format_decimal(sl_px),
            "status": "OKX 动态止损监控中（轮询改价）",
        }
        if not hasattr(self, "_position_takeover_sessions") or self._position_takeover_sessions is None:
            self._position_takeover_sessions = {}
        self._position_takeover_sessions[takeover_session_id] = {
            "thread": None,
            "engine": engine,
            "summary": summary,
            "log_prefix": log_prefix,
            "inst_id": position.inst_id,
            "algo_id": algo_id,
            "algo_cl": algo_cl,
        }

        def _run() -> None:
            import traceback

            try:
                engine.run_takeover_exchange_dynamic_stop(
                    credentials,
                    config,
                    trade_instrument=instrument,
                    position=filled,
                    initial_stop_loss=sl_px,
                    stop_loss_algo_id=algo_id,
                    stop_loss_algo_cl_ord_id=algo_cl,
                )
            except Exception as exc:
                detail = traceback.format_exc()
                self.root.after(
                    0,
                    lambda e=exc, d=detail, lp=log_prefix: self._enqueue_log(f"{lp} 异常结束：{e}\n{d}"),
                )
            finally:
                self.root.after(0, lambda sid=takeover_session_id: self._position_takeover_finished(sid))

        th = threading.Thread(
            target=_run,
            name=f"takeover-{takeover_session_id}",
            daemon=True,
        )
        self._position_takeover_sessions[takeover_session_id]["thread"] = th
        th.start()
        self._position_takeover_sessions[takeover_session_id]["thread_started"] = True
        self._refresh_positions_zoom_takeover_panel()
        self._enqueue_log(
            f"{log_prefix} 已启动 | 方向={side_zh}({open_side.upper()}) | posSide={(pos_side or '-')} | "
            f"开仓均价={format_decimal(entry)} | 数量={size_text} | R价差={format_decimal(r_spread)} | "
            f"止损触发={format_decimal(sl_px)} | algoId={algo_id or '-'} | algoClOrdId={algo_cl or '-'}"
        )

    def open_takeover_from_selected_conditional_order(self) -> None:
        """从「当前委托」选中的条件/触发类算法止损单出发，反查持仓、核对数量后启动交易所动态止盈接管。"""
        parent = self._pending_order_parent_for_view("positions_zoom")
        item = self._selected_pending_order_item("positions_zoom")
        if item is None:
            item = self._selected_pending_order_item(None)
        if item is None:
            messagebox.showinfo(
                "从条件单接管动态止盈",
                "请先在「当前委托」里选中一条委托。",
                parent=parent,
            )
            return
        if (getattr(item, "source_kind", "") or "").strip().lower() != "algo":
            messagebox.showinfo(
                "从条件单接管动态止盈",
                "请选中来源为「算法」的止损类委托（OKX 条件/触发单通常在此类）。",
                parent=parent,
            )
            return
        ot = (item.ord_type or "").strip().lower()
        if ot and "conditional" not in ot and "trigger" not in ot and "oco" not in ot:
            messagebox.showinfo(
                "从条件单接管动态止盈",
                f"当前委托类型为「{item.ord_type or '-'}」。此入口面向条件/触发/OCO 类止损；"
                "其它类型请改用工具栏「从选中持仓接管」。",
                parent=parent,
            )
            return
        sl_px = item.stop_loss_trigger_price or item.trigger_price
        if sl_px is None:
            messagebox.showerror("从条件单接管动态止盈", "选中委托缺少止损或触发价。", parent=parent)
            return
        algo_id = (item.algo_id or "").strip() or None
        algo_cl = (item.algo_client_order_id or "").strip() or None
        if not algo_id and not algo_cl:
            messagebox.showerror(
                "从条件单接管动态止盈",
                "选中委托缺少 algoId / algoClOrdId，无法在交易所侧改价。",
                parent=parent,
            )
            return
        if getattr(self, "_takeover_open_flow_busy", False):
            messagebox.showinfo(
                "从条件单接管动态止盈",
                "正在拉取委托或等待上一步完成，请稍候再试。",
                parent=parent,
            )
            return

        found = self._find_position_for_takeover_pending_algo(item)
        if found is None:
            messagebox.showinfo(
                "从条件单接管动态止盈",
                "未找到与该算法止损委托匹配的永续/交割持仓（合约、平仓方向、止损价相对开仓等）。"
                "请先确认已有对应方向的仓位与止损单，必要时刷新持仓与委托后再试。",
                parent=parent,
            )
            return
        position, entry = found

        inst_kind = infer_inst_type(position.inst_id)
        if inst_kind not in {"SWAP", "FUTURES"}:
            messagebox.showinfo("从条件单接管动态止盈", "当前仅支持永续或交割合约对应的条件止损。", parent=parent)
            return
        profile_name = (self._positions_context_profile_name or self._current_credential_profile()).strip()
        credentials = self._credentials_for_profile_or_none(profile_name)
        if credentials is None:
            messagebox.showerror("从条件单接管动态止盈", "当前 API 未配置有效凭证。", parent=parent)
            return
        try:
            record = self._clone_template_record_for_symbol(self._template_record_from_launcher(), position.inst_id)
        except Exception as exc:
            messagebox.showerror("从条件单接管动态止盈", f"读取主界面策略模板失败：{exc}", parent=parent)
            return
        env_label = self._environment_label_for_profile(profile_name or self._current_credential_profile())
        desk_env = ENV_OPTIONS[self._normalized_environment_label(env_label)]
        position_mode = (
            "long_short" if position.pos_side and str(position.pos_side).strip().lower() != "net" else "net"
        )
        config = replace(record.config, environment=desk_env, position_mode=position_mode)
        if not live_exchange_dynamic_take_profit_template_enabled(config):
            messagebox.showinfo(
                "从条件单接管动态止盈",
                "主界面当前策略模板须为「动态止盈」且非交易员虚拟止损，"
                "并属于 EMA 动态委托或 EMA 突破类策略，才能与交易所止损改价逻辑对齐。",
                parent=parent,
            )
            return
        direction = derive_position_direction(position)
        effective_mode = resolve_dynamic_signal_mode(record.strategy_id, config.signal_mode)
        if effective_mode == "long_only" and direction != "long":
            messagebox.showinfo("从条件单接管动态止盈", "模板为只做多，与匹配到的持仓方向不一致。", parent=parent)
            return
        if effective_mode == "short_only" and direction != "short":
            messagebox.showinfo("从条件单接管动态止盈", "模板为只做空，与匹配到的持仓方向不一致。", parent=parent)
            return

        pos_sz = abs(Decimal(str(position.position)))
        max_close = self._selected_position_close_size(position)
        ord_sz = item.size
        if ord_sz is not None and ord_sz > 0 and ord_sz > max_close:
            messagebox.showerror(
                "从条件单接管动态止盈",
                f"该条件单委托量（{format_decimal(ord_sz)}，与 OKX 持仓同一合约单位）大于当前可平数量（{format_decimal(max_close)}；"
                f"持仓总量 {format_decimal(pos_sz)}），请先核对仓位或其它挂单冻结后再试。",
                parent=parent,
            )
            return

        self._positions_zoom_takeover_status_text.set(
            f"已根据条件单匹配 {position.inst_id} 持仓，正在拉取合约元数据并启动动态止盈接管…"
        )
        order_contracts = ord_sz if ord_sz is not None and ord_sz > 0 else None
        self._takeover_enqueue_instrument_fetch(
            credentials=credentials,
            config=config,
            record=record,
            profile_name=profile_name,
            position=position,
            entry=entry,
            sl_px=sl_px,
            algo_id=algo_id,
            algo_cl=algo_cl,
            order_contract_size=order_contracts,
        )

    def open_position_takeover_dynamic_stop_dialog(self) -> None:
        """从持仓大窗：选中永续/交割仓位，选择已有止损算法单，按主界面模板动态止盈规则接管改价。"""
        position = self._selected_position_item()
        parent = self._position_action_parent()
        if position is None:
            messagebox.showinfo("接管动态止盈", "请先在当前持仓里选中一条具体持仓。", parent=parent)
            return
        if getattr(self, "_takeover_open_flow_busy", False):
            messagebox.showinfo(
                "接管动态止盈",
                "正在拉取委托或等待上一步完成，请稍候再试。",
                parent=parent,
            )
            return
        inst_kind = infer_inst_type(position.inst_id)
        if inst_kind not in {"SWAP", "FUTURES"}:
            messagebox.showinfo("接管动态止盈", "当前仅支持永续或交割合约持仓。", parent=parent)
            return
        profile_name = (self._positions_context_profile_name or self._current_credential_profile()).strip()
        credentials = self._credentials_for_profile_or_none(profile_name)
        if credentials is None:
            messagebox.showerror("接管动态止盈", "当前 API 未配置有效凭证。", parent=parent)
            return
        try:
            record = self._clone_template_record_for_symbol(self._template_record_from_launcher(), position.inst_id)
        except Exception as exc:
            messagebox.showerror("接管动态止盈", f"读取主界面策略模板失败：{exc}", parent=parent)
            return
        env_label = self._environment_label_for_profile(profile_name or self._current_credential_profile())
        desk_env = ENV_OPTIONS[self._normalized_environment_label(env_label)]
        position_mode = (
            "long_short" if position.pos_side and str(position.pos_side).strip().lower() != "net" else "net"
        )
        config = replace(record.config, environment=desk_env, position_mode=position_mode)
        if not live_exchange_dynamic_take_profit_template_enabled(config):
            messagebox.showinfo(
                "接管动态止盈",
                "主界面当前策略模板须为「动态止盈」且非交易员虚拟止损，"
                "并属于 EMA 动态委托或 EMA 突破类策略，才能与交易所止损改价逻辑对齐。",
                parent=parent,
            )
            return
        direction = derive_position_direction(position)
        effective_mode = resolve_dynamic_signal_mode(record.strategy_id, config.signal_mode)
        if effective_mode == "long_only" and direction != "long":
            messagebox.showinfo("接管动态止盈", "模板为只做多，与当前空头持仓不一致。", parent=parent)
            return
        if effective_mode == "short_only" and direction != "short":
            messagebox.showinfo("接管动态止盈", "模板为只做空，与当前多头持仓不一致。", parent=parent)
            return
        entry = position.avg_price
        if entry is None or entry <= 0:
            messagebox.showerror("接管动态止盈", "持仓缺少有效开仓均价，无法计算 R 与动态止损。", parent=parent)
            return

        self._takeover_prefetch_request_id = int(getattr(self, "_takeover_prefetch_request_id", 0)) + 1
        rid = self._takeover_prefetch_request_id
        self._takeover_prefetch_context = {
            "position": position,
            "record": record,
            "config": config,
            "credentials": credentials,
            "desk_env": desk_env,
            "profile_name": profile_name,
            "parent": parent,
            "entry": entry,
            "direction": direction,
        }
        self._takeover_open_flow_busy = True
        self._positions_zoom_takeover_status_text.set(
            "正在从 OKX 拉取永续/交割待触发委托（后台线程）…\n"
            "界面可继续操作；完成后会弹出选择止损单的窗口。"
        )
        threading.Thread(
            target=self._takeover_prefetch_worker,
            args=(rid,),
            name="position-takeover-prefetch",
            daemon=True,
        ).start()

    def _position_takeover_finished(self, session_id: str) -> None:
        if hasattr(self, "_position_takeover_sessions") and isinstance(self._position_takeover_sessions, dict):
            self._position_takeover_sessions.pop(session_id, None)
        self._refresh_positions_zoom_takeover_panel()

    def _stop_takeover_session(self, session_id: str, *, parent) -> None:
        sessions = getattr(self, "_position_takeover_sessions", None) or {}
        st = sessions.get(session_id)
        if not isinstance(st, dict):
            messagebox.showinfo("停止接管", "未找到该接管会话。", parent=parent)
            return
        eng = st.get("engine")
        th = st.get("thread")
        if eng is not None:
            eng.interrupt_takeover_monitor()
        st["engine"] = None
        if th is None or not getattr(th, "is_alive", lambda: False)():
            sessions.pop(session_id, None)
            self._refresh_positions_zoom_takeover_panel()
            return
        th.join(timeout=8.0)
        if th.is_alive():
            pfx = str(st.get("log_prefix") or "").strip() or "[接管动态止盈]"
            self._enqueue_log(f"{pfx} 停止信号已发送，后台线程仍在退出中；可稍后再次点击停止。")
            summ = st.get("summary")
            if isinstance(summ, dict):
                st["summary"] = {**summ, "status": "已发送停止，等待后台线程退出…"}
            self._refresh_positions_zoom_takeover_panel()
            return
        pfx = str(st.get("log_prefix") or "").strip() or "[接管动态止盈]"
        self._enqueue_log(f"{pfx} 已停止接管，OKX 上保留当前止损触发价。")
        sessions.pop(session_id, None)
        self._refresh_positions_zoom_takeover_panel()

    def stop_position_takeover_dynamic_stop(self) -> None:
        parent = self._position_action_parent()
        sessions = getattr(self, "_position_takeover_sessions", None) or {}
        if not sessions:
            messagebox.showinfo("停止接管", "当前没有动态止盈接管任务。", parent=parent)
            return
        alive = self._takeover_running_session_ids()
        if not alive:
            messagebox.showinfo("停止接管", "当前没有运行中的动态止盈接管任务。", parent=parent)
            return
        tree = getattr(self, "_positions_zoom_takeover_tree", None)
        chosen_sid: str | None = None
        if tree is not None and _widget_exists(tree):
            try:
                sel = tree.selection()
                if sel:
                    chosen_sid = sel[0]
            except Exception:
                chosen_sid = None
        if chosen_sid:
            if chosen_sid not in alive:
                messagebox.showinfo(
                    "停止接管",
                    "所选行已非运行中任务；请在「动态止盈接管」表格中选中一条监控中的会话后再试。",
                    parent=parent,
                )
                return
            self._stop_takeover_session(chosen_sid, parent=parent)
            return
        if len(alive) == 1:
            self._stop_takeover_session(alive[0], parent=parent)
            return
        messagebox.showinfo(
            "停止接管",
            f"当前有 {len(alive)} 条接管在并行运行。请在下方「动态止盈接管」表格中选中要停止的一行，再点「停止接管」。",
            parent=parent,
        )

    def _refresh_positions_periodic(self) -> None:
        if self.position_auto_refresh_enabled:
            self.refresh_positions()
        self.root.after(self._position_refresh_interval_ms(), self._refresh_positions_periodic)
