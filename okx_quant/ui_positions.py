from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from pathlib import Path


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
            self._update_position_summary(_filter_positions(
                self._latest_positions,
                inst_type=POSITION_TYPE_OPTIONS[self.position_type_filter.get()],
                keyword=self.position_keyword.get(),
                note_texts=self._current_position_note_text_map(),
            ))

    def _on_position_refresh_interval_changed(self, *_: object) -> None:
        visible_positions = _filter_positions(
            self._latest_positions,
            inst_type=POSITION_TYPE_OPTIONS[self.position_type_filter.get()],
            keyword=self.position_keyword.get(),
            note_texts=self._current_position_note_text_map(),
        )
        self._update_position_summary(visible_positions)
        self._enqueue_log(f"账户持仓自动刷新间隔已切换为：{self.position_refresh_interval_label.get()}")

    def _on_position_filter_changed(self, *_: object) -> None:
        self._render_positions_view()

    def reset_position_filters(self) -> None:
        self.position_type_filter.set("全部类型")
        self.position_keyword.set("")
        self._render_positions_view()

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
            self.refresh_positions()
            self.sync_positions_zoom_data()
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
        ttk.Button(zoom_actions, text="刷新", command=self.refresh_positions).grid(row=0, column=0, padx=(0, 6))
        ttk.Button(zoom_actions, text="账户信息", command=self.open_account_info_window).grid(row=0, column=1, padx=(0, 6))
        ttk.Button(zoom_actions, text="平仓选中", command=self.flatten_selected_position).grid(row=0, column=2, padx=(0, 6))
        ttk.Button(zoom_actions, text="编辑备注", command=self.edit_selected_position_note).grid(row=0, column=3, padx=(0, 6))
        ttk.Button(zoom_actions, text="设置期权保护", command=self.open_position_protection_window).grid(
            row=0, column=4, padx=(0, 6)
        )
        ttk.Button(zoom_actions, text="展期建议", command=self.open_option_roll_window).grid(
            row=0, column=5, padx=(0, 6)
        )
        ttk.Button(zoom_actions, text="关闭", command=self._close_positions_zoom_window).grid(row=0, column=6)
        ttk.Button(zoom_actions, text="列设置", command=self.open_positions_zoom_column_window).grid(
            row=0, column=7, padx=(0, 6)
        )

        ttk.Button(zoom_actions, textvariable=self._positions_zoom_detail_toggle_text, command=self.toggle_positions_zoom_detail).grid(
            row=0, column=8, padx=(0, 6)
        )
        ttk.Button(zoom_actions, textvariable=self._positions_zoom_history_toggle_text, command=self.toggle_positions_zoom_history).grid(
            row=0, column=9
        )
        for column_index, child in enumerate(zoom_actions.winfo_children()):
            child.grid_configure(column=column_index)

        filter_row = ttk.Frame(container)
        filter_row.grid(row=1, column=0, sticky="ew", pady=(0, 10))
        filter_row.columnconfigure(3, weight=1)
        ttk.Label(filter_row, text="类型").grid(row=0, column=0, sticky="w")
        zoom_position_type_combo = ttk.Combobox(
            filter_row,
            textvariable=self.position_type_filter,
            values=list(POSITION_TYPE_OPTIONS.keys()),
            state="readonly",
            width=16,
        )
        zoom_position_type_combo.grid(row=0, column=1, sticky="w", padx=(6, 12))
        zoom_position_type_combo.bind("<<ComboboxSelected>>", self._on_position_filter_changed)
        ttk.Label(filter_row, text="搜索").grid(row=0, column=2, sticky="w")
        zoom_position_keyword_entry = ttk.Entry(filter_row, textvariable=self.position_keyword)
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
        ttk.Button(header, text="撤单选中", command=lambda: self.cancel_selected_pending_order("positions_zoom")).grid(
            row=0, column=3, sticky="e", padx=(0, 6)
        )
        ttk.Button(header, text="批量撤当前筛选", command=lambda: self.cancel_filtered_pending_orders("positions_zoom")).grid(
            row=0, column=4, sticky="e", padx=(0, 6)
        )
        ttk.Button(
            header,
            textvariable=self._positions_zoom_pending_orders_detail_toggle_text,
            command=self.toggle_positions_zoom_pending_orders_detail,
        ).grid(row=0, column=5, sticky="e")

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

        columns = ("time", "source", "inst_type", "inst_id", "state", "side", "ord_type", "price", "size", "filled", "tp_sl", "order_id", "cl_ord_id")
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
            ("tp_sl", 180),
            ("order_id", 120),
            ("cl_ord_id", 150),
        ):
            tree.heading(column_id, text=headings[column_id])
            tree.column(column_id, width=width, anchor="e" if column_id in {"price", "size", "filled"} else "center")
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

        columns = ("time", "source", "inst_type", "inst_id", "state", "side", "ord_type", "price", "size", "filled", "tp_sl", "order_id", "cl_ord_id")
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
            ("tp_sl", 180),
            ("order_id", 120),
            ("cl_ord_id", 150),
        ):
            tree.heading(column_id, text=headings[column_id])
            tree.column(column_id, width=width, anchor="e" if column_id in {"price", "size", "filled"} else "center")
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
            "realized_usdt": "鎶樺悎USDT",
        }
        for column_id, width in (
            ("time", 150),
            ("inst_type", 72),
            ("inst_id", 240),
            ("side", 96),
            ("price", 100),
            ("size", 100),
            ("fee", 100),
            ("pnl", 110),
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
        ).grid(row=1, column=8, columnspan=6, sticky="w", pady=(6, 0))
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
            "pnl",
            "realized",
            "realized_usdt",
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
            "pnl": "盈亏",
            "realized": "已实现盈亏",
            "note": "备注",
        }
        headings["realized_usdt"] = "折合USDT"
        for column_id, width in (
            ("time", 150),
            ("inst_type", 72),
            ("inst_id", 240),
            ("mgn_mode", 96),
            ("side", 96),
            ("open_avg", 100),
            ("close_avg", 100),
            ("close_size", 100),
            ("pnl", 100),
            ("realized", 110),
            ("realized_usdt", 110),
            ("note", 220),
        ):
            tree.heading(column_id, text=headings[column_id])
            tree.column(
                column_id,
                width=width,
                anchor="e" if column_id in {"open_avg", "close_avg", "close_size", "pnl", "realized", "realized_usdt"} else "center",
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
        self._position_history_fetch_limit = 100
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

        def _copy_branch(source_parent: str, target_parent: str) -> None:
            for item_id in self.position_tree.get_children(source_parent):
                item = self.position_tree.item(item_id)
                zoom_tree.insert(
                    target_parent,
                    END,
                    iid=item_id,
                    text=item.get("text", ""),
                    values=item.get("values", ()),
                    open=bool(item.get("open")),
                    tags=item.get("tags", ()),
                )
                _copy_branch(item_id, item_id)

        _copy_branch("", "")
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
        self.position_keyword.set(contract)
        self._render_positions_view()

    def apply_selected_option_expiry_prefix_to_position_search(self) -> None:
        position = self._selected_positions_zoom_option_for_search()
        _, expiry_prefix = _option_search_shortcuts(position.inst_id if position else "")
        if not expiry_prefix:
            messagebox.showinfo("快捷筛选", "请先在当前持仓里选中一条期权合约。")
            return
        self.position_keyword.set(expiry_prefix)
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
            return
        self._pending_orders_refreshing = True
        self._positions_zoom_pending_orders_summary_text.set("正在刷新当前委托...")
        threading.Thread(
            target=self._refresh_pending_orders_worker,
            args=(credentials, environment),
            daemon=True,
        ).start()

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
                    _format_optional_decimal(item.fill_fee),
                    _format_fill_history_pnl(item),
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
        filtered_items = _filter_position_history_items(
            self._latest_position_history,
            inst_type=POSITION_TYPE_OPTIONS.get(self.position_history_type_filter.get(), ""),
            margin_mode=HISTORY_MARGIN_MODE_FILTER_OPTIONS.get(self.position_history_margin_filter.get(), ""),
            asset=self.position_history_asset_filter.get(),
            expiry_prefix=self.position_history_expiry_prefix_filter.get(),
            keyword=self.position_history_keyword.get(),
            note_texts_by_index=self._position_history_note_text_map_by_index(),
        )
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
                    _format_position_history_pnl(item.pnl, item),
                    _format_position_history_pnl(item.realized_pnl, item, with_sign=True),
                    _format_optional_usdt(
                        _position_history_realized_pnl_usdt(item, self._position_history_usdt_prices),
                        with_sign=True,
                    ),
                    self._position_history_note_summary(item),
                ),
                tags=tuple(tag for tag in (_pnl_tag(item.pnl),) if tag),
            )
        summary = self._positions_zoom_position_history_base_summary
        if (
            POSITION_TYPE_OPTIONS.get(self.position_history_type_filter.get(), "")
            or HISTORY_MARGIN_MODE_FILTER_OPTIONS.get(self.position_history_margin_filter.get(), "")
            or self.position_history_asset_filter.get().strip()
            or self.position_history_expiry_prefix_filter.get().strip()
            or self.position_history_keyword.get().strip()
        ):
            summary = f"{summary} | 当前显示：{len(filtered_items)}/{len(self._latest_position_history)}"
        if (
            POSITION_TYPE_OPTIONS.get(self.position_history_type_filter.get(), "")
            or HISTORY_MARGIN_MODE_FILTER_OPTIONS.get(self.position_history_margin_filter.get(), "")
            or self.position_history_asset_filter.get().strip()
            or self.position_history_expiry_prefix_filter.get().strip()
            or self.position_history_keyword.get().strip()
        ):
            summary = (
                f"{self._positions_zoom_position_history_base_summary} | \u5f53\u524d\u663e\u793a\uff1a{len(filtered_items)}/{len(self._latest_position_history)}"
                f"\n\u7b5b\u9009\u7edf\u8ba1\uff1a"
                f"{_format_position_history_filter_stats(filtered_items, self._position_history_usdt_prices)}"
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
            visible_positions = _filter_positions(
                self._latest_positions,
                inst_type=POSITION_TYPE_OPTIONS[self.position_type_filter.get()],
                keyword=self.position_keyword.get(),
                note_texts=self._current_position_note_text_map(),
            )
            groups = _group_positions_for_tree(visible_positions)
            for asset_label, buckets in groups.items():
                asset_id = _asset_group_row_id(asset_label)
                asset_positions = [item for bucket in buckets.values() for item in bucket]
                asset_metrics = _aggregate_position_metrics(asset_positions, self._upl_usdt_prices, self._position_instruments)
                asset_label_text = f"{asset_label} 风险单元"
                self.position_tree.insert(
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
                            self._insert_position_row(asset_id, position, _position_tree_row_id(position))
                        continue

                    bucket_id = _bucket_group_row_id(asset_label, bucket_label)
                    bucket_metrics = _aggregate_position_metrics(
                        bucket_positions,
                        self._upl_usdt_prices,
                        self._position_instruments,
                    )
                    self.position_tree.insert(
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
                        self._insert_position_row(bucket_id, position, _position_tree_row_id(position))

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

    def _insert_position_row(self, parent_id: str, position: OkxPosition, row_id: str) -> None:
        label = position.inst_id
        if position.pos_side and position.pos_side.lower() != "net":
            label = f"{label} [{position.pos_side}]"
        tags = [tag for tag in (_pnl_tag(position.unrealized_pnl), _margin_mode_tag(position.mgn_mode)) if tag]
        self.position_tree.insert(
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
            self.position_type_filter.get(),
            self.position_keyword.get(),
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

    def _refresh_positions_periodic(self) -> None:
        if self.position_auto_refresh_enabled:
            self.refresh_positions()
        self.root.after(self._position_refresh_interval_ms(), self._refresh_positions_periodic)
