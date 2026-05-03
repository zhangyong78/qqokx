from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from pathlib import Path


class UiProtectionMixin:
    def open_position_protection_window(self) -> None:
        if self._protection_window is not None and self._protection_window.winfo_exists():
            self._populate_protection_form_from_selection(force=True)
            self._refresh_protection_window_view()
            self._protection_window.focus_force()
            return

        window = Toplevel(self.root)
        window.title("期权持仓保护")
        apply_adaptive_window_geometry(
            window,
            width_ratio=0.76,
            height_ratio=0.78,
            min_width=980,
            min_height=760,
            max_width=1520,
            max_height=1060,
        )
        self._protection_window = window
        window.protocol("WM_DELETE_WINDOW", self._close_position_protection_window)

        container = ttk.Frame(window, padding=16)
        container.pack(fill="both", expand=True)
        container.columnconfigure(0, weight=1)
        container.rowconfigure(1, weight=1)
        container.rowconfigure(2, weight=1)

        form_frame = ttk.LabelFrame(container, text="选中期权持仓保护", padding=16)
        form_frame.grid(row=0, column=0, sticky="ew")
        for column in range(4):
            form_frame.columnconfigure(column, weight=1)

        ttk.Label(
            form_frame,
            textvariable=self._protection_form_title_text,
            justify="left",
            wraplength=960,
            font=("Microsoft YaHei UI", 10, "bold"),
        ).grid(row=0, column=0, columnspan=4, sticky="w", pady=(0, 12))
        ttk.Label(
            form_frame,
            textvariable=self._protection_logic_hint_text,
            justify="left",
            wraplength=960,
            foreground="#8a4600",
        ).grid(row=1, column=0, columnspan=4, sticky="w", pady=(0, 8))

        row = 2
        ttk.Label(form_frame, text="触发条件").grid(row=row, column=0, sticky="w")
        protection_trigger_combo = ttk.Combobox(
            form_frame,
            textvariable=self.protection_trigger_source_label,
            values=list(PROTECTION_TRIGGER_SOURCE_OPTIONS.keys()),
            state="readonly",
        )
        protection_trigger_combo.grid(row=row, column=1, sticky="ew", padx=(0, 16))
        protection_trigger_combo.bind("<<ComboboxSelected>>", self._on_protection_trigger_source_changed)
        ttk.Label(form_frame, text="现货标的").grid(row=row, column=2, sticky="w")
        self._protection_spot_symbol_entry = ttk.Entry(form_frame, textvariable=self.protection_spot_symbol)
        self._protection_spot_symbol_entry.grid(row=row, column=3, sticky="ew")

        row += 1
        ttk.Label(form_frame, text="止盈触发价").grid(row=row, column=0, sticky="w", pady=(12, 0))
        ttk.Entry(form_frame, textvariable=self.protection_take_profit_trigger).grid(
            row=row, column=1, sticky="ew", padx=(0, 16), pady=(12, 0)
        )
        ttk.Label(form_frame, text="止损触发价").grid(row=row, column=2, sticky="w", pady=(12, 0))
        ttk.Entry(form_frame, textvariable=self.protection_stop_loss_trigger).grid(
            row=row, column=3, sticky="ew", pady=(12, 0)
        )

        row += 1
        ttk.Label(form_frame, text="止盈报单方式").grid(row=row, column=0, sticky="w", pady=(12, 0))
        take_profit_mode_combo = ttk.Combobox(
            form_frame,
            textvariable=self.protection_take_profit_order_mode_label,
            values=list(PROTECTION_ORDER_MODE_OPTIONS.keys()),
            state="readonly",
        )
        take_profit_mode_combo.grid(row=row, column=1, sticky="ew", padx=(0, 16), pady=(12, 0))
        take_profit_mode_combo.bind("<<ComboboxSelected>>", self._on_protection_order_mode_changed)
        ttk.Label(form_frame, text="止盈报单价格").grid(row=row, column=2, sticky="w", pady=(12, 0))
        self._protection_take_profit_order_price_entry = ttk.Entry(
            form_frame,
            textvariable=self.protection_take_profit_order_price,
        )
        self._protection_take_profit_order_price_entry.grid(row=row, column=3, sticky="ew", pady=(12, 0))

        row += 1
        ttk.Label(form_frame, text="止盈滑点").grid(row=row, column=0, sticky="w", pady=(12, 0))
        self._protection_take_profit_slippage_entry = ttk.Entry(
            form_frame,
            textvariable=self.protection_take_profit_slippage,
        )
        self._protection_take_profit_slippage_entry.grid(row=row, column=1, sticky="ew", padx=(0, 16), pady=(12, 0))
        ttk.Label(form_frame, text="轮询秒数").grid(row=row, column=2, sticky="w", pady=(12, 0))
        ttk.Entry(form_frame, textvariable=self.protection_poll_seconds).grid(row=row, column=3, sticky="ew", pady=(12, 0))

        row += 1
        ttk.Label(form_frame, text="止损报单方式").grid(row=row, column=0, sticky="w", pady=(12, 0))
        stop_loss_mode_combo = ttk.Combobox(
            form_frame,
            textvariable=self.protection_stop_loss_order_mode_label,
            values=list(PROTECTION_ORDER_MODE_OPTIONS.keys()),
            state="readonly",
        )
        stop_loss_mode_combo.grid(row=row, column=1, sticky="ew", padx=(0, 16), pady=(12, 0))
        stop_loss_mode_combo.bind("<<ComboboxSelected>>", self._on_protection_order_mode_changed)
        ttk.Label(form_frame, text="止损报单价格").grid(row=row, column=2, sticky="w", pady=(12, 0))
        self._protection_stop_loss_order_price_entry = ttk.Entry(
            form_frame,
            textvariable=self.protection_stop_loss_order_price,
        )
        self._protection_stop_loss_order_price_entry.grid(row=row, column=3, sticky="ew", pady=(12, 0))

        row += 1
        ttk.Label(form_frame, text="止损滑点").grid(row=row, column=0, sticky="w", pady=(12, 0))
        self._protection_stop_loss_slippage_entry = ttk.Entry(
            form_frame,
            textvariable=self.protection_stop_loss_slippage,
        )
        self._protection_stop_loss_slippage_entry.grid(row=row, column=1, sticky="ew", padx=(0, 16), pady=(12, 0))
        ttk.Label(
            form_frame,
            text="说明：触发条件可用“期权标记价格”或“现货最新价”；报单价格可用“设定价格”或“标记价格加减滑点”。",
            justify="left",
            wraplength=520,
        ).grid(row=row, column=2, columnspan=2, sticky="w", pady=(12, 0))

        action_frame = ttk.Frame(form_frame)
        action_frame.grid(row=row + 1, column=0, columnspan=4, sticky="e", pady=(16, 0))
        ttk.Button(action_frame, text="启动保护", command=self.start_selected_position_protection).grid(
            row=0, column=0, padx=(0, 8)
        )
        ttk.Button(action_frame, text="回放模拟", command=self.open_position_protection_replay_window).grid(
            row=0, column=1, padx=(0, 8)
        )
        ttk.Button(action_frame, text="停止选中任务", command=self.stop_selected_position_protection).grid(
            row=0, column=2, padx=(0, 8)
        )
        ttk.Button(action_frame, text="关闭", command=self._close_position_protection_window).grid(row=0, column=3)

        sessions_frame = ttk.LabelFrame(container, text="运行中的期权保护任务", padding=12)
        sessions_frame.grid(row=1, column=0, sticky="nsew", pady=(12, 0))
        sessions_frame.columnconfigure(0, weight=1)
        sessions_frame.rowconfigure(1, weight=1)
        sessions_header = ttk.Frame(sessions_frame)
        sessions_header.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 8))
        sessions_header.columnconfigure(0, weight=1)
        ttk.Label(sessions_header, textvariable=self._protection_status_text).grid(row=0, column=0, sticky="w")
        ttk.Button(sessions_header, text="清除已结束", command=self.clear_finished_position_protections).grid(
            row=0, column=1, sticky="e"
        )
        task_tree = ttk.Treeview(
            sessions_frame,
            columns=("api", "option", "trigger", "direction", "status", "started"),
            show="headings",
            selectmode="browse",
        )
        task_tree.heading("api", text="API")
        task_tree.heading("option", text="期权合约")
        task_tree.heading("trigger", text="触发条件")
        task_tree.heading("direction", text="方向")
        task_tree.heading("status", text="状态")
        task_tree.heading("started", text="启动时间")
        task_tree.column("api", width=96, anchor="center")
        task_tree.column("option", width=250, anchor="w")
        task_tree.column("trigger", width=180, anchor="w")
        task_tree.column("direction", width=80, anchor="center")
        task_tree.column("status", width=100, anchor="center")
        task_tree.column("started", width=120, anchor="center")
        task_tree.grid(row=1, column=0, sticky="nsew")
        task_tree.bind("<<TreeviewSelect>>", self._on_protection_session_selected)
        task_scroll = ttk.Scrollbar(sessions_frame, orient="vertical", command=task_tree.yview)
        task_scroll.grid(row=1, column=1, sticky="ns")
        task_tree.configure(yscrollcommand=task_scroll.set)
        self._protection_sessions_tree = task_tree

        detail_frame = ttk.LabelFrame(container, text="保护任务详情", padding=12)
        detail_frame.grid(row=2, column=0, sticky="nsew", pady=(12, 0))
        detail_frame.columnconfigure(0, weight=1)
        detail_frame.rowconfigure(0, weight=1)
        self._protection_detail_text = Text(
            detail_frame,
            height=8,
            wrap="word",
            font=("Microsoft YaHei UI", 10),
            relief="flat",
        )
        self._protection_detail_text.grid(row=0, column=0, sticky="nsew")
        detail_scroll = ttk.Scrollbar(detail_frame, orient="vertical", command=self._protection_detail_text.yview)
        detail_scroll.grid(row=0, column=1, sticky="ns")
        self._protection_detail_text.configure(yscrollcommand=detail_scroll.set)
        self._set_readonly_text(self._protection_detail_text, "请选择一个保护任务查看详情。")

        self._populate_protection_form_from_selection(force=True)
        self._refresh_protection_window_view()

    def _close_position_protection_window(self) -> None:
        if self._protection_order_mode_job is not None:
            try:
                self.root.after_cancel(self._protection_order_mode_job)
            except Exception:
                pass
            self._protection_order_mode_job = None
        if self._protection_window is not None and self._protection_window.winfo_exists():
            self._protection_window.destroy()
        self._protection_window = None
        self._protection_sessions_tree = None
        self._protection_detail_text = None
        self._protection_selected_session_id = None
        self._protection_form_position_id = None
        self._protection_form_position_key = None

    def _populate_protection_form_from_selection(self, *, force: bool = False) -> None:
        position = self._selected_option_position(prefer_protection_form=not force)
        if position is None:
            if self._positions_view_rendering and self._protection_form_position_key:
                return
            self._protection_form_position_id = None
            self._protection_form_position_key = None
            self._protection_form_title_text.set("当前没有选中期权持仓。请先在账户持仓里选中一条期权仓位。")
            self._protection_logic_hint_text.set("请先选择一条期权持仓，系统会显示当前组合下的止盈止损方向。")
            return

        direction = derive_position_direction(position)
        self._protection_form_title_text.set(
            f"当前选中：{position.inst_id} | 方向={direction.upper()} | 持仓量={format_decimal(position.position)} | "
            f"开仓均价={_format_optional_decimal(position.avg_price)}"
        )
        position_key = _position_tree_row_id(position)
        if force or self._protection_form_position_key != position_key:
            self._protection_form_position_id = position.inst_id
            self._protection_form_position_key = position_key
            self.protection_trigger_source_label.set("期权标记价格")
            self.protection_spot_symbol.set(infer_default_spot_inst_id(position.inst_id))
            self.protection_take_profit_trigger.set("")
            self.protection_stop_loss_trigger.set("")
            self.protection_take_profit_order_mode_label.set("标记价格加减滑点")
            self.protection_take_profit_order_price.set("")
            self.protection_take_profit_slippage.set("0")
            self.protection_stop_loss_order_mode_label.set("标记价格加减滑点")
            self.protection_stop_loss_order_price.set("")
            self.protection_stop_loss_slippage.set("0")
            self.protection_poll_seconds.set("2")
        self._on_protection_trigger_source_changed()
        self._update_protection_order_mode_widgets()
        self._update_protection_logic_hint()

    def _on_protection_trigger_source_changed(self, *_: object) -> None:
        if hasattr(self, "_protection_spot_symbol_entry"):
            state = "normal" if self.protection_trigger_source_label.get() == "现货最新价" else "disabled"
            self._protection_spot_symbol_entry.configure(state=state)
        self._update_protection_logic_hint()

    def _update_protection_logic_hint(self) -> None:
        position = self._selected_option_position(prefer_protection_form=True)
        if position is None:
            self._protection_logic_hint_text.set("请先选择一条期权持仓，系统会显示当前组合下的止盈止损方向。")
            return
        trigger_source = PROTECTION_TRIGGER_SOURCE_OPTIONS.get(self.protection_trigger_source_label.get(), "option_mark")
        if trigger_source == "option_mark":
            trigger_inst_id = position.inst_id
            trigger_price_type = "mark"
        else:
            trigger_inst_id = normalize_spot_inst_id(self.protection_spot_symbol.get()) or infer_default_spot_inst_id(position.inst_id)
            trigger_price_type = "last"
        self._protection_logic_hint_text.set(
            describe_protection_price_logic(
                option_inst_id=position.inst_id,
                direction=derive_position_direction(position),
                trigger_inst_id=trigger_inst_id,
                trigger_price_type=trigger_price_type,  # type: ignore[arg-type]
            )
        )

    def _on_protection_order_mode_changed(self, *_: object) -> None:
        if self._protection_order_mode_job is not None:
            try:
                self.root.after_cancel(self._protection_order_mode_job)
            except Exception:
                pass
        self._protection_order_mode_job = self.root.after(1, self._apply_protection_order_mode_widgets)

    def _apply_protection_order_mode_widgets(self) -> None:
        self._protection_order_mode_job = None
        self._update_protection_order_mode_widgets()

    def _update_protection_order_mode_widgets(self) -> None:
        self._sync_protection_order_mode_widget(
            mode_label=self.protection_take_profit_order_mode_label.get(),
            price_var=self.protection_take_profit_order_price,
            price_entry=self._protection_take_profit_order_price_entry,
            slippage_entry=self._protection_take_profit_slippage_entry,
            memory_attr="_protection_take_profit_fixed_price_memory",
        )
        self._sync_protection_order_mode_widget(
            mode_label=self.protection_stop_loss_order_mode_label.get(),
            price_var=self.protection_stop_loss_order_price,
            price_entry=self._protection_stop_loss_order_price_entry,
            slippage_entry=self._protection_stop_loss_slippage_entry,
            memory_attr="_protection_stop_loss_fixed_price_memory",
        )

    def _sync_protection_order_mode_widget(
        self,
        *,
        mode_label: str,
        price_var: StringVar,
        price_entry: ttk.Entry | None,
        slippage_entry: ttk.Entry | None,
        memory_attr: str,
    ) -> None:
        placeholder = "自动按标记价与滑点计算"
        fixed_mode = _resolve_protection_order_mode_value(mode_label) == "fixed_price"
        if fixed_mode:
            if price_var.get() == placeholder:
                price_var.set(getattr(self, memory_attr, ""))
            if price_entry is not None:
                price_entry.configure(state="normal")
            if slippage_entry is not None:
                slippage_entry.configure(state="disabled")
        else:
            current = price_var.get().strip()
            if current and current != placeholder:
                setattr(self, memory_attr, current)
            price_var.set(placeholder)
            if price_entry is not None:
                price_entry.configure(state="readonly")
            if slippage_entry is not None:
                slippage_entry.configure(state="normal")

    def _refresh_protection_window_view(self) -> None:
        if self._protection_window is None or not self._protection_window.winfo_exists():
            return
        self._populate_protection_form_from_selection(force=False)
        sessions = self._protection_manager.list_sessions()
        self._protection_status_text.set(
            f"当前保护任务：{len(sessions)}"
            if sessions
            else "当前没有运行中的期权持仓保护任务。"
        )
        if self._protection_sessions_tree is not None:
            selected_before = self._protection_sessions_tree.selection()
            self._protection_sessions_tree.delete(*self._protection_sessions_tree.get_children())
            for session in sessions:
                self._protection_sessions_tree.insert(
                    "",
                    END,
                    iid=session.session_id,
                    values=(
                        session.api_name or "-",
                        session.option_inst_id,
                        session.trigger_label,
                        session.direction,
                        session.status,
                        session.started_at.strftime("%H:%M:%S"),
                    ),
                )
            if selected_before and self._protection_sessions_tree.exists(selected_before[0]):
                target = selected_before[0]
            else:
                target = sessions[0].session_id if sessions else None
            if target is not None:
                self._protection_sessions_tree.selection_set(target)
                self._protection_sessions_tree.focus(target)
                self._protection_selected_session_id = target
            else:
                self._protection_selected_session_id = None
        self._refresh_protection_detail_panel()

    def _on_protection_session_selected(self, *_: object) -> None:
        if self._protection_sessions_tree is None:
            return
        selection = self._protection_sessions_tree.selection()
        self._protection_selected_session_id = selection[0] if selection else None
        self._refresh_protection_detail_panel()

    def _refresh_protection_detail_panel(self) -> None:
        if self._protection_detail_text is None:
            return
        sessions = {item.session_id: item for item in self._protection_manager.list_sessions()}
        session = sessions.get(self._protection_selected_session_id or "")
        if session is None:
            self._set_readonly_text(self._protection_detail_text, "请选择一个保护任务查看详情。")
            return
        self._set_readonly_text(
            self._protection_detail_text,
            "\n".join(
                [
                    f"任务：{session.session_id}",
                    f"API配置：{session.api_name or '-'}",
                    f"期权合约：{session.option_inst_id}",
                    f"触发条件：{session.trigger_label}",
                    f"触发标的：{session.trigger_inst_id}",
                    f"触发价格类型：{_format_protection_trigger_price_type(session.trigger_price_type)}",
                    f"方向：{session.direction}",
                    f"持仓方向：{session.pos_side or '-'}",
                    f"止盈触发：{_format_optional_decimal(session.take_profit_trigger)}",
                    f"止盈报单方式：{_format_protection_order_mode_label(session.take_profit_order_mode)}",
                    f"止盈报单价格：{_format_protection_order_price_detail(session.take_profit_order_mode, session.take_profit_order_price)}",
                    f"止盈滑点：{_format_optional_decimal(session.take_profit_slippage)}",
                    f"止损触发：{_format_optional_decimal(session.stop_loss_trigger)}",
                    f"止损报单方式：{_format_protection_order_mode_label(session.stop_loss_order_mode)}",
                    f"止损报单价格：{_format_protection_order_price_detail(session.stop_loss_order_mode, session.stop_loss_order_price)}",
                    f"止损滑点：{_format_optional_decimal(session.stop_loss_slippage)}",
                    f"轮询秒数：{session.poll_seconds:g}",
                    f"状态：{session.status}",
                    f"启动时间：{session.started_at.strftime('%Y-%m-%d %H:%M:%S')}",
                    "",
                    f"最新状态：{session.last_message}",
                ]
            ),
        )

    def start_selected_position_protection(self) -> None:
        position = self._selected_option_position(prefer_protection_form=True)
        if position is None:
            messagebox.showinfo("提示", "请先在账户持仓中选中一条期权仓位。", parent=self._protection_window or self.root)
            return
        credentials = self._current_credentials_or_none()
        if credentials is None:
            messagebox.showerror("启动失败", "请先在设置里配置 API 凭证。", parent=self._protection_window or self.root)
            return
        if not self._guard_live_action_against_stale_cache(
            self._positions_refresh_health,
            action_label="启动持仓保护",
            data_label="持仓",
            parent=self._protection_window or self.root,
            refresh_callback=self.refresh_positions,
        ):
            return

        try:
            notifier = self._build_optional_protection_notifier()
            self._protection_manager.set_notifier(notifier)
            protection = self._build_selected_position_protection(position)
            _validate_protection_live_price_availability(self.client, protection, position)
            config = self._build_manual_protection_strategy_config(position, protection)
            session_id = self._protection_manager.start(credentials, config, protection)
            if credentials.profile_name:
                self._enqueue_log(f"[持仓保护 {credentials.profile_name} {session_id}] 已启动 {position.inst_id} 的期权保护任务。")
            else:
                self._enqueue_log(f"[持仓保护 {session_id}] 已启动 {position.inst_id} 的期权保护任务。")
            self._refresh_protection_window_view()
        except Exception as exc:
            messagebox.showerror("启动保护失败", str(exc), parent=self._protection_window or self.root)

    def stop_selected_position_protection(self) -> None:
        if not self._protection_selected_session_id:
            messagebox.showinfo("提示", "请先在下方列表中选中一个保护任务。", parent=self._protection_window or self.root)
            return
        try:
            self._protection_manager.stop(self._protection_selected_session_id)
            self._refresh_protection_window_view()
        except Exception as exc:
            messagebox.showerror("停止保护失败", str(exc), parent=self._protection_window or self.root)

    def open_position_protection_replay_window(self) -> None:
        position = self._selected_option_position(prefer_protection_form=True)
        if position is None:
            messagebox.showinfo("提示", "请先在账户持仓中选中一条期权仓位。", parent=self._protection_window or self.root)
            return
        try:
            protection = self._build_selected_position_protection(position)
        except Exception as exc:
            messagebox.showerror("回放参数错误", str(exc), parent=self._protection_window or self.root)
            return

        if self._protection_replay_window is not None and self._protection_replay_window.window.winfo_exists():
            self._protection_replay_window.window.destroy()

        self._protection_replay_window = ProtectionReplayWindow(
            self.root,
            self.client,
            position,
            protection,
            initial_state=ProtectionReplayLaunchState(bar=self.bar.get(), candle_limit="120"),
        )

    def clear_finished_position_protections(self) -> None:
        cleared = self._protection_manager.clear_finished()
        self._refresh_protection_window_view()
        if cleared <= 0:
            messagebox.showinfo("提示", "当前没有可清除的已结束任务。", parent=self._protection_window or self.root)

    def _build_selected_position_protection(self, position: OkxPosition) -> OptionProtectionConfig:
        trigger_source = PROTECTION_TRIGGER_SOURCE_OPTIONS[self.protection_trigger_source_label.get()]
        if trigger_source == "option_mark":
            trigger_inst_id = position.inst_id
            trigger_price_type = "mark"
            trigger_label = f"{position.inst_id} 标记价"
        else:
            trigger_inst_id = normalize_spot_inst_id(self.protection_spot_symbol.get())
            if not trigger_inst_id:
                raise ValueError("现货触发模式下，请填写现货标的。")
            trigger_instrument = self.client.get_instrument(trigger_inst_id)
            if trigger_instrument.inst_type != "SPOT":
                raise ValueError("现货触发模式下，请填写现货交易对，例如 BTC-USDT。")
            trigger_price_type = "last"
            trigger_label = f"{trigger_inst_id} 最新价"

        take_profit_trigger = self._parse_optional_positive_decimal(self.protection_take_profit_trigger.get(), "止盈触发价")
        stop_loss_trigger = self._parse_optional_positive_decimal(self.protection_stop_loss_trigger.get(), "止损触发价")
        if take_profit_trigger is None and stop_loss_trigger is None:
            raise ValueError("止盈触发价和止损触发价至少要填写一个。")

        direction = derive_position_direction(position)
        _validate_protection_price_relationship(
            option_inst_id=position.inst_id,
            direction=direction,
            trigger_inst_id=trigger_inst_id,
            trigger_price_type=trigger_price_type,
            take_profit=take_profit_trigger,
            stop_loss=stop_loss_trigger,
        )

        take_profit_order_mode = PROTECTION_ORDER_MODE_OPTIONS[self.protection_take_profit_order_mode_label.get()]
        stop_loss_order_mode = PROTECTION_ORDER_MODE_OPTIONS[self.protection_stop_loss_order_mode_label.get()]
        take_profit_order_price = self._parse_protection_order_price(
            self.protection_take_profit_order_price.get(),
            "止盈报单价格",
            take_profit_order_mode,
        )
        stop_loss_order_price = self._parse_protection_order_price(
            self.protection_stop_loss_order_price.get(),
            "止损报单价格",
            stop_loss_order_mode,
        )
        return OptionProtectionConfig(
            option_inst_id=position.inst_id,
            trigger_inst_id=trigger_inst_id,
            trigger_price_type=trigger_price_type,
            direction=direction,
            pos_side=position.pos_side if position.pos_side and position.pos_side.lower() != "net" else None,
            take_profit_trigger=take_profit_trigger,
            stop_loss_trigger=stop_loss_trigger,
            take_profit_order_mode=take_profit_order_mode,
            take_profit_order_price=take_profit_order_price,
            take_profit_slippage=self._parse_nonnegative_decimal(self.protection_take_profit_slippage.get(), "止盈滑点"),
            stop_loss_order_mode=stop_loss_order_mode,
            stop_loss_order_price=stop_loss_order_price,
            stop_loss_slippage=self._parse_nonnegative_decimal(self.protection_stop_loss_slippage.get(), "止损滑点"),
            poll_seconds=float(self._parse_positive_decimal(self.protection_poll_seconds.get(), "轮询秒数")),
            trigger_label=trigger_label,
        )

    def _parse_protection_order_price(self, raw: str, field_name: str, order_mode: str) -> Decimal | None:
        if order_mode != "fixed_price":
            return None
        return self._parse_positive_decimal(raw, field_name)

    def _parse_nonnegative_decimal(self, raw: str, field_name: str) -> Decimal:
        cleaned = raw.strip()
        if not cleaned:
            return Decimal("0")
        try:
            value = Decimal(cleaned)
        except InvalidOperation as exc:
            raise ValueError(f"{field_name} 不是有效数字") from exc
        if value < 0:
            raise ValueError(f"{field_name} 不能小于 0")
        return value

    def _build_manual_protection_strategy_config(
        self,
        position: OkxPosition,
        protection: OptionProtectionConfig,
    ) -> StrategyConfig:
        environment = self._positions_effective_environment or ENV_OPTIONS[self.environment_label.get()]
        trade_mode = position.mgn_mode if position.mgn_mode in {"cross", "isolated"} else TRADE_MODE_OPTIONS[self.trade_mode_label.get()]
        position_mode = "long_short" if position.pos_side and position.pos_side.lower() != "net" else "net"
        return StrategyConfig(
            inst_id=protection.trigger_inst_id,
            bar=self.bar.get(),
            ema_period=1,
            atr_period=1,
            atr_stop_multiplier=Decimal("1"),
            atr_take_multiplier=Decimal("1"),
            order_size=abs(position.position),
            trade_mode=trade_mode,
            signal_mode="long_only" if protection.direction == "long" else "short_only",
            position_mode=position_mode,
            environment=environment,
            tp_sl_trigger_type=protection.trigger_price_type,
            strategy_id="manual_option_protection",
            poll_seconds=protection.poll_seconds,
            risk_amount=None,
            trade_inst_id=position.inst_id,
            tp_sl_mode="local_trade",
            local_tp_sl_inst_id=protection.trigger_inst_id,
            entry_side_mode="follow_signal",
            run_mode="trade",
        )

    def _build_optional_protection_notifier(self) -> EmailNotifier | None:
        notification_config = self._collect_notification_config(validate_if_enabled=False)
        if not notification_config.enabled:
            return None
        return EmailNotifier(notification_config, logger=self._make_system_logger("邮件 持仓保护"))
