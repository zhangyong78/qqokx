from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from pathlib import Path


class UiStrategySessionsMixin:
    @staticmethod
    def _entry_reference_ema_caption(strategy_id: str) -> str:
        if is_dynamic_strategy_id(strategy_id):
            return "挂单参考EMA"
        if strategy_id == STRATEGY_EMA_BREAKDOWN_SHORT_ID or is_ema_atr_breakout_strategy(strategy_id):
            return "突破参考EMA"
        return "参考EMA周期"

    @staticmethod
    def _format_strategy_symbol_display(signal_symbol: str, trade_symbol: str | None) -> str:
        normalized_signal = signal_symbol.strip().upper()
        normalized_trade = (trade_symbol or normalized_signal).strip().upper()
        if not normalized_signal:
            return normalized_trade
        if normalized_trade == normalized_signal:
            return normalized_signal
        return f"{normalized_signal} -> {normalized_trade}"

    @staticmethod
    def _default_strategy_template_filename(record: StrategyTemplateRecord) -> str:
        raw = f"{record.strategy_name or record.strategy_id}_{record.symbol or 'strategy'}"
        sanitized = re.sub(r'[\\/:*?"<>|]+', "_", raw).strip(" ._")
        return f"{sanitized or 'strategy_template'}.json"

    def _resolve_strategy_template_definition(self, record: StrategyTemplateRecord) -> StrategyDefinition:
        try:
            return get_strategy_definition(record.strategy_id)
        except KeyError:
            if record.strategy_name and record.strategy_name in self._strategy_name_to_id:
                return get_strategy_definition(self._strategy_name_to_id[record.strategy_name])
        raise ValueError(f"当前版本不认识这个策略：{record.strategy_id}")

    def _ensure_importable_strategy_symbols(self, symbol: str, local_tp_sl_symbol: str | None = None) -> None:
        normalized_symbol = _normalize_symbol_input(symbol)
        if normalized_symbol and normalized_symbol not in self._default_symbol_values:
            self._default_symbol_values.append(normalized_symbol)
        merged = list(dict.fromkeys(self._default_symbol_values))
        custom_values = ["", *merged]
        normalized_local = _normalize_symbol_input(local_tp_sl_symbol or "")
        if normalized_local and normalized_local not in custom_values:
            custom_values.append(normalized_local)
        self._default_symbol_values = merged
        self._custom_trigger_symbol_values = custom_values
        if hasattr(self, "symbol_combo") and self.symbol_combo is not None:
            self.symbol_combo["values"] = merged
        if hasattr(self, "local_tp_sl_symbol_combo") and self.local_tp_sl_symbol_combo is not None:
            self.local_tp_sl_symbol_combo["values"] = custom_values

    def _apply_strategy_template_record(self, record: StrategyTemplateRecord) -> tuple[StrategyDefinition, str, str]:
        definition = self._resolve_strategy_template_definition(record)
        resolved_api_name, api_note = _resolve_import_api_profile(
            record.api_name,
            self._current_credential_profile(),
            set(self._credential_profiles.keys()),
        )
        if (
            resolved_api_name
            and resolved_api_name in self._credential_profiles
            and resolved_api_name != self._current_credential_profile()
        ):
            self._apply_credentials_profile(resolved_api_name, log_change=False)

        self.strategy_name.set(definition.name)
        self._on_strategy_selected()

        launch_symbol = _launcher_symbol_from_strategy_config(definition.strategy_id, record.config)
        custom_symbol = (record.config.local_tp_sl_inst_id or "").strip().upper()
        self._ensure_importable_strategy_symbols(launch_symbol, custom_symbol)

        self.symbol.set(launch_symbol)
        self.trade_symbol.set(launch_symbol)
        self.local_tp_sl_symbol.set(custom_symbol)
        self.bar.set(record.config.bar)
        self.ema_period.set(str(record.config.ema_period))
        self.trend_ema_period.set(str(record.config.trend_ema_period))
        self.big_ema_period.set(str(record.config.big_ema_period))
        self.entry_reference_ema_period.set(str(record.config.entry_reference_ema_period))
        self.atr_period.set(str(record.config.atr_period))
        self.stop_atr.set(_format_entry_decimal(record.config.atr_stop_multiplier))
        self.take_atr.set(_format_entry_decimal(record.config.atr_take_multiplier))
        self.risk_amount.set(_format_entry_decimal(record.config.risk_amount))
        self.order_size.set(_format_entry_decimal(record.config.order_size))
        self.poll_seconds.set(_format_entry_float(record.config.poll_seconds))
        self.signal_mode_label.set(
            _strategy_template_direction_label(
                definition.strategy_id,
                record.config,
                fallback=record.direction_label or definition.default_signal_label,
            )
        )
        self.take_profit_mode_label.set(
            _reverse_lookup_label(TAKE_PROFIT_MODE_OPTIONS, record.config.take_profit_mode, "动态止盈")
        )
        self.max_entries_per_trend.set(str(record.config.max_entries_per_trend))
        self.startup_chase_window_seconds.set(str(record.config.startup_chase_window_seconds))
        self.dynamic_two_r_break_even.set(record.config.dynamic_two_r_break_even)
        self.dynamic_fee_offset_enabled.set(record.config.dynamic_fee_offset_enabled)
        self.time_stop_break_even_enabled.set(record.config.time_stop_break_even_enabled)
        self.time_stop_break_even_bars.set(str(record.config.time_stop_break_even_bars))
        self.run_mode_label.set(_reverse_lookup_label(RUN_MODE_OPTIONS, record.config.run_mode, record.run_mode_label))
        self.trade_mode_label.set(_reverse_lookup_label(TRADE_MODE_OPTIONS, record.config.trade_mode, "全仓 cross"))
        self.position_mode_label.set(
            _reverse_lookup_label(POSITION_MODE_OPTIONS, record.config.position_mode, "净持仓 net")
        )
        self.trigger_type_label.set(
            _reverse_lookup_label(TRIGGER_TYPE_OPTIONS, record.config.tp_sl_trigger_type, "标记价格 mark")
        )
        self.tp_sl_mode_label.set(_launcher_tp_sl_mode_label(record.config.tp_sl_mode))
        self.entry_side_mode_label.set(
            _reverse_lookup_label(ENTRY_SIDE_MODE_OPTIONS, record.config.entry_side_mode, "跟随信号")
        )
        self.environment_label.set(_reverse_lookup_label(ENV_OPTIONS, record.config.environment, "模拟盘 demo"))
        self._sync_dynamic_take_profit_controls()
        QuantApp._sync_entry_side_mode_controls(self)
        return definition, resolved_api_name, api_note

    def export_selected_session_template(self) -> None:
        session = self._selected_session()
        if session is None:
            messagebox.showinfo("提示", "请先在运行中策略列表中选中一条策略。")
            return
        record = _strategy_template_record_from_payload(_build_strategy_template_payload(session))
        if record is None:
            messagebox.showerror("导出失败", "当前选中策略缺少可导出的有效配置。")
            return
        target = filedialog.asksaveasfilename(
            parent=self.root,
            title="导出策略参数",
            defaultextension=".json",
            filetypes=(("JSON 文件", "*.json"), ("所有文件", "*.*")),
            initialfile=self._default_strategy_template_filename(record),
        )
        if not target:
            return
        payload = _build_strategy_template_payload(session)
        try:
            Path(target).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception as exc:
            messagebox.showerror("导出失败", f"写入策略参数文件失败：{exc}")
            return
        self._enqueue_log(f"{session.log_prefix} 已导出策略参数：{target}")
        messagebox.showinfo(
            "导出完成",
            "已导出策略参数文件。\n\n文件不包含 API 密钥，可在其他机器导入后复用当前参数。",
        )

    def import_strategy_template(self) -> None:
        source = filedialog.askopenfilename(
            parent=self.root,
            title="导入策略参数",
            filetypes=(("JSON 文件", "*.json"), ("所有文件", "*.*")),
        )
        if not source:
            return
        try:
            payload = json.loads(Path(source).read_text(encoding="utf-8"))
            record = _strategy_template_record_from_payload(payload)
            if record is None:
                raise ValueError("文件缺少有效的策略配置快照。")
            definition, resolved_api_name, api_note = self._apply_strategy_template_record(record)
        except Exception as exc:
            messagebox.showerror("导入失败", str(exc))
            return

        applied_api = resolved_api_name or self._current_credential_profile()
        self._finish_strategy_template_import(
            source=source,
            record=record,
            definition=definition,
            applied_api=applied_api,
            api_note=api_note,
        )

    @staticmethod
    def _session_blocks_duplicate_launch(session: StrategySession) -> bool:
        return session.engine.is_running or session.status in {"运行中", "停止中"}

    @staticmethod
    def _format_duplicate_launch_block_message(session: StrategySession, *, imported: bool) -> str:
        headline = "已导入参数，但检测到重复策略：" if imported else "检测到重复策略启动："
        guidance = (
            "当前参数已经回填到启动区。\n如需复制参数开新策略，请先修改标的或切换 API 后再启动。"
            if imported
            else "当前已经存在同 API、同参数、同标的的策略会话。\n请先停止、恢复或清理原会话；如需复制参数开新策略，请先修改标的或切换 API 后再启动。"
        )
        return (
            f"{headline}\n\n"
            f"API：{session.api_name or '-'}\n"
            f"会话：{session.session_id}\n"
            f"状态：{session.display_status}\n"
            f"启动时间：{session.started_at.strftime('%H:%M:%S')}\n"
            f"标的：{session.symbol or '-'}\n\n"
            f"{guidance}"
        )

    def _find_duplicate_strategy_session(self, *, api_name: str, config: StrategyConfig) -> StrategySession | None:
        target_api_name = api_name.strip()
        for session in self.sessions.values():
            if session.api_name.strip() != target_api_name:
                continue
            if session.config != config:
                continue
            if not self._session_blocks_duplicate_launch(session):
                continue
            return session
        return None

    def _focus_session_row(self, session_id: str) -> None:
        if not self.session_tree.exists(session_id):
            return
        self.session_tree.selection_set(session_id)
        self.session_tree.focus(session_id)
        self._refresh_selected_session_details()

    def open_selected_strategy_live_chart(self) -> None:
        session = self._selected_session()
        if session is None:
            messagebox.showinfo("\u63d0\u793a", "\u8bf7\u5148\u5728\u8fd0\u884c\u4e2d\u7b56\u7565\u5217\u8868\u4e2d\u9009\u4e2d\u4e00\u6761\u7b56\u7565\u3002")
            return
        self.open_strategy_live_chart_window(session.session_id)

    def open_strategy_session_log(self, session_id: str) -> None:
        session = self.sessions.get(session_id)
        if session is None:
            messagebox.showwarning("\u63d0\u793a", "\u5f53\u524d\u4f1a\u8bdd\u5df2\u4e0d\u5b58\u5728\uff0c\u8bf7\u91cd\u65b0\u9009\u62e9\u3002", parent=self.root)
            return
        log_path = _coerce_log_file_path(session.log_file_path)
        if log_path is None:
            messagebox.showinfo("\u63d0\u793a", f"\u4f1a\u8bdd {session.session_id} \u8fd8\u6ca1\u6709\u72ec\u7acb\u65e5\u5fd7\u8def\u5f84\u3002", parent=self.root)
            return
        if not log_path.exists():
            messagebox.showerror("\u6253\u5f00\u5931\u8d25", f"\u65e5\u5fd7\u6587\u4ef6\u4e0d\u5b58\u5728\uff1a\\n{log_path}", parent=self.root)
            return
        startfile = getattr(os, "startfile", None)
        if not callable(startfile):
            messagebox.showerror("\u6253\u5f00\u5931\u8d25", "\u5f53\u524d\u7cfb\u7edf\u4e0d\u652f\u6301\u76f4\u63a5\u6253\u5f00\u65e5\u5fd7\u6587\u4ef6\u3002", parent=self.root)
            return
        startfile(str(log_path))
        self._enqueue_log(f"[\u4f1a\u8bdd {session.session_id}] \u5df2\u6253\u5f00\u72ec\u7acb\u65e5\u5fd7\uff1a{log_path}")

    def open_strategy_live_chart_window(self, session_id: str) -> None:
        session = self.sessions.get(session_id)
        if session is None:
            messagebox.showwarning("\u63d0\u793a", "\u5f53\u524d\u4f1a\u8bdd\u5df2\u4e0d\u5b58\u5728\uff0c\u8bf7\u91cd\u65b0\u9009\u62e9\u3002")
            return
        existing = self._strategy_live_chart_windows.get(session_id)
        if existing is not None and _widget_exists(existing.window):
            existing.window.focus_force()
            self._request_strategy_live_chart_refresh(session_id, immediate=True)
            return

        window = Toplevel(self.root)
        apply_window_icon(window)
        window.title(self._strategy_live_chart_window_title(session))
        apply_adaptive_window_geometry(
            window,
            width_ratio=0.76,
            height_ratio=0.7,
            min_width=1080,
            min_height=700,
            max_width=1760,
            max_height=1180,
        )
        window.transient(self.root)

        container = ttk.Frame(window, padding=12)
        container.pack(fill="both", expand=True)
        container.columnconfigure(0, weight=1)
        container.rowconfigure(1, weight=1)

        headline_text = StringVar(value=self._strategy_live_chart_headline(session))
        status_text = StringVar(value="\u6b63\u5728\u51c6\u5907\u5b9e\u65f6K\u7ebf\u56fe...")
        footer_text = StringVar(
            value=(
                f"\u53ea\u8bfb\u76d1\u63a7\u7a97\uff1a\u9ed8\u8ba4\u6bcf {DEFAULT_STRATEGY_LIVE_CHART_REFRESH_MS // 1000} \u79d2\u5237\u65b0\u4e00\u6b21\uff0c"
                "\u7a97\u53e3\u5173\u95ed\u540e\u81ea\u52a8\u505c\u6b62\u3002"
            )
        )

        header = ttk.Frame(container)
        header.grid(row=0, column=0, sticky="ew", pady=(0, 10))
        header.columnconfigure(0, weight=1)
        ttk.Label(
            header,
            textvariable=headline_text,
            font=("Microsoft YaHei UI", 11, "bold"),
            justify="left",
        ).grid(row=0, column=0, sticky="w")
        ttk.Label(
            header,
            textvariable=status_text,
            justify="left",
            wraplength=980,
        ).grid(row=1, column=0, sticky="w", pady=(6, 0))

        chart_frame = ttk.Frame(container)
        chart_frame.grid(row=1, column=0, sticky="nsew")
        chart_frame.columnconfigure(0, weight=1)
        chart_frame.rowconfigure(0, weight=1)
        canvas = Canvas(chart_frame, background="#ffffff", highlightthickness=0, width=1120, height=620)
        canvas.grid(row=0, column=0, sticky="nsew")

        tools_frame = ttk.Frame(container)
        tools_frame.grid(row=2, column=0, sticky="ew", pady=(8, 4))
        tools_frame.columnconfigure(11, weight=1)

        ttk.Label(tools_frame, text="画线工具").grid(row=0, column=0, sticky="w")
        ttk.Button(tools_frame, text="趋势线", command=lambda target_session_id=session_id: self._set_live_chart_tool(target_session_id, "line")).grid(row=0, column=1, padx=(6, 2))
        ttk.Button(tools_frame, text="水平线", command=lambda target_session_id=session_id: self._set_live_chart_tool(target_session_id, "horizontal")).grid(row=0, column=2, padx=2)
        ttk.Button(tools_frame, text="止损线", command=lambda target_session_id=session_id: self._set_live_chart_tool(target_session_id, "stop")).grid(row=0, column=3, padx=2)
        ttk.Button(tools_frame, text="清空线", command=lambda target_session_id=session_id: self._clear_live_chart_annotations(target_session_id)).grid(row=0, column=4, padx=(2, 10))

        ttk.Label(tools_frame, text="价格基准").grid(row=0, column=5, sticky="e")
        trade_price_basis = StringVar(value="最新价格")
        ttk.Combobox(
            tools_frame,
            width=9,
            state="readonly",
            values=("最新价格", "最新K线", "上一根K线"),
            textvariable=trade_price_basis,
        ).grid(row=0, column=6, padx=(4, 8))
        ttk.Label(tools_frame, text="止损基准").grid(row=0, column=7, sticky="e")
        trade_stop_basis = StringVar(value="上一根ATR")
        ttk.Combobox(
            tools_frame,
            width=11,
            state="readonly",
            values=("上一根ATR", "前三根高低", "止损线"),
            textvariable=trade_stop_basis,
        ).grid(row=0, column=8, padx=(4, 8))
        trade_order_mode = StringVar(value="限价挂单")
        ttk.Combobox(
            tools_frame,
            width=9,
            state="readonly",
            values=("限价挂单", "对手价"),
            textvariable=trade_order_mode,
        ).grid(row=0, column=9, padx=(0, 8))
        ttk.Button(tools_frame, text="开多", command=lambda target_session_id=session_id: self._submit_live_chart_trade(target_session_id, "long")).grid(row=0, column=10, padx=(0, 4))
        ttk.Button(tools_frame, text="开空", command=lambda target_session_id=session_id: self._submit_live_chart_trade(target_session_id, "short")).grid(row=0, column=11, sticky="w")

        trade_config_frame = ttk.Frame(container)
        trade_config_frame.grid(row=3, column=0, sticky="ew", pady=(0, 6))
        ttk.Label(trade_config_frame, text="定量").grid(row=0, column=0, sticky="w")
        trade_risk_mode = StringVar(value="以损定量")
        ttk.Combobox(
            trade_config_frame,
            width=9,
            state="readonly",
            values=("以损定量", "固定张数"),
            textvariable=trade_risk_mode,
        ).grid(row=0, column=1, padx=(4, 8))
        ttk.Label(trade_config_frame, text="风险金").grid(row=0, column=2, sticky="e")
        trade_risk_amount = StringVar(value=format_decimal(session.config.risk_amount or Decimal("20")))
        ttk.Entry(trade_config_frame, width=10, textvariable=trade_risk_amount).grid(row=0, column=3, padx=(4, 8))
        ttk.Label(trade_config_frame, text="固定张数").grid(row=0, column=4, sticky="e")
        trade_fixed_size = StringVar(value=format_decimal(session.config.order_size or Decimal("1")))
        ttk.Entry(trade_config_frame, width=10, textvariable=trade_fixed_size).grid(row=0, column=5, padx=(4, 8))
        trade_status_text = StringVar(value="划线交易待命。")
        ttk.Label(trade_config_frame, textvariable=trade_status_text).grid(row=0, column=6, sticky="w")

        footer = ttk.Frame(container)
        footer.grid(row=4, column=0, sticky="ew", pady=(6, 0))
        footer.columnconfigure(0, weight=1)
        ttk.Label(footer, textvariable=footer_text, justify="left", wraplength=980).grid(row=0, column=0, sticky="w")
        action_row = ttk.Frame(footer)
        action_row.grid(row=0, column=1, sticky="e")
        ttk.Button(
            action_row,
            text="\u7acb\u5373\u5237\u65b0",
            command=lambda target_session_id=session_id: self._request_strategy_live_chart_refresh(
                target_session_id, immediate=True
            ),
        ).grid(row=0, column=0, padx=(0, 6))
        ttk.Button(
            action_row,
            text="\u5173\u95ed",
            command=lambda target_session_id=session_id: self._close_strategy_live_chart_window(target_session_id),
        ).grid(row=0, column=1)

        state = StrategyLiveChartWindowState(
            session_id=session_id,
            window=window,
            canvas=canvas,
            headline_text=headline_text,
            status_text=status_text,
            footer_text=footer_text,
            trade_price_basis=trade_price_basis,
            trade_stop_basis=trade_stop_basis,
            trade_order_mode=trade_order_mode,
            trade_risk_mode=trade_risk_mode,
            trade_risk_amount=trade_risk_amount,
            trade_fixed_size=trade_fixed_size,
            trade_status_text=trade_status_text,
        )
        self._strategy_live_chart_windows[session_id] = state
        window.protocol("WM_DELETE_WINDOW", lambda target_session_id=session_id: self._close_strategy_live_chart_window(target_session_id))
        canvas.bind("<Configure>", lambda *_args, target_session_id=session_id: self._render_strategy_live_chart_window(target_session_id))
        canvas.bind("<ButtonPress-1>", lambda event, target_session_id=session_id: self._on_live_chart_mouse_down(target_session_id, event.x, event.y))
        canvas.bind("<B1-Motion>", lambda event, target_session_id=session_id: self._on_live_chart_mouse_move(target_session_id, event.x, event.y))
        canvas.bind("<ButtonRelease-1>", lambda event, target_session_id=session_id: self._on_live_chart_mouse_up(target_session_id, event.x, event.y))
        self._render_strategy_live_chart_window(session_id)
        self._request_strategy_live_chart_refresh(session_id, immediate=True)

    def _strategy_live_chart_window_title(self, session: StrategySession) -> str:
        trade_inst_id = _session_trade_inst_id(session) or session.symbol
        return f"\u5b9e\u65f6K\u7ebf\u56fe - {session.session_id} {trade_inst_id}"

    def _set_live_chart_tool(self, session_id: str, tool: str) -> None:
        state = self._strategy_live_chart_windows.get(session_id)
        if state is None:
            return
        state.active_tool = tool
        if state.trade_status_text is not None:
            labels = {"line": "趋势线", "horizontal": "水平线", "stop": "止损线"}
            state.trade_status_text.set(f"当前工具：{labels.get(tool, '无')}。")

    def _clear_live_chart_annotations(self, session_id: str) -> None:
        state = self._strategy_live_chart_windows.get(session_id)
        if state is None:
            return
        state.line_annotations.clear()
        state.draft_line_start = None
        state.draft_line_current = None
        self._render_strategy_live_chart_window(session_id)
        if state.trade_status_text is not None:
            state.trade_status_text.set("已清空画线。")

    def _on_live_chart_mouse_down(self, session_id: str, x: float, y: float) -> None:
        state = self._strategy_live_chart_windows.get(session_id)
        if state is None or state.active_tool == "none":
            return
        state.draft_line_start = (x, y)
        state.draft_line_current = (x, y)
        self._render_strategy_live_chart_window(session_id)

    def _on_live_chart_mouse_move(self, session_id: str, x: float, y: float) -> None:
        state = self._strategy_live_chart_windows.get(session_id)
        if state is None or state.draft_line_start is None:
            return
        state.draft_line_current = (x, y)
        self._render_strategy_live_chart_window(session_id)

    def _on_live_chart_mouse_up(self, session_id: str, x: float, y: float) -> None:
        state = self._strategy_live_chart_windows.get(session_id)
        if state is None or state.draft_line_start is None:
            return
        start = state.draft_line_start
        end = (x, y)
        color = "#1d4ed8"
        label = ""
        if state.active_tool == "stop":
            color = "#cf222e"
            label = "止损线"
        elif state.active_tool == "horizontal":
            end = (x, start[1])
            color = "#7c3aed"
            label = "水平线"
        else:
            label = "趋势线"
        state.line_annotations.append(
            LiveChartLineAnnotation(
                kind=state.active_tool,
                x1=start[0],
                y1=start[1],
                x2=end[0],
                y2=end[1],
                color=color,
                label=label,
            )
        )
        state.draft_line_start = None
        state.draft_line_current = None
        self._render_strategy_live_chart_window(session_id)
        if state.trade_status_text is not None:
            state.trade_status_text.set(f"已添加{label}。")

    def _draw_live_chart_annotations(self, state: StrategyLiveChartWindowState) -> None:
        self._draw_line_annotations(
            state.canvas,
            state.line_annotations,
            state.draft_line_start,
            state.draft_line_current,
        )

    def _draw_line_annotations(
        self,
        canvas: Canvas,
        annotations: list[LiveChartLineAnnotation],
        draft_start: tuple[float, float] | None,
        draft_current: tuple[float, float] | None,
    ) -> None:
        canvas.delete("line_anno")
        for item in annotations:
            canvas.create_line(item.x1, item.y1, item.x2, item.y2, fill=item.color, width=2, tags=("line_anno",))
            if item.label:
                canvas.create_text(
                    item.x2 + 4,
                    item.y2 - 4,
                    anchor="sw",
                    text=item.label,
                    fill=item.color,
                    font=("Microsoft YaHei UI", 9),
                    tags=("line_anno",),
                )
        if draft_start is not None and draft_current is not None:
            x1, y1 = draft_start
            x2, y2 = draft_current
            canvas.create_line(x1, y1, x2, y2, fill="#6b7280", width=1, dash=(4, 3), tags=("line_anno",))

    def _live_chart_stop_price_from_annotation(self, state: StrategyLiveChartWindowState) -> Decimal | None:
        snapshot = state.last_snapshot
        if snapshot is None or not snapshot.candles:
            return None
        low, high = strategy_live_chart_price_bounds(snapshot)
        canvas_height = max(state.canvas.winfo_height(), 1)
        top = 26
        bottom = max(canvas_height - 44, top + 1)
        chart_height = max(bottom - top, 1)
        stop_lines = [line for line in state.line_annotations if line.kind == "stop"]
        if not stop_lines:
            return None
        y = stop_lines[-1].y1
        ratio = min(max((y - top) / chart_height, 0.0), 1.0)
        return high - (high - low) * Decimal(str(ratio))

    def _resolve_live_chart_trade_reference_price(self, state: StrategyLiveChartWindowState) -> Decimal | None:
        snapshot = state.last_snapshot
        if snapshot is None or not snapshot.candles:
            return None
        basis = state.trade_price_basis.get() if state.trade_price_basis is not None else "最新价格"
        if basis == "最新价格":
            return snapshot.latest_price or snapshot.candles[-1].close
        if basis == "上一根K线" and len(snapshot.candles) >= 2:
            return snapshot.candles[-2].close
        return snapshot.candles[-1].close

    def _resolve_live_chart_trade_stop_price(
        self,
        state: StrategyLiveChartWindowState,
        entry_price: Decimal,
        direction: str,
    ) -> Decimal | None:
        snapshot = state.last_snapshot
        if snapshot is None or not snapshot.candles:
            return None
        stop_basis = state.trade_stop_basis.get() if state.trade_stop_basis is not None else "上一根ATR"
        is_long = direction == "long"
        if stop_basis == "止损线":
            return self._live_chart_stop_price_from_annotation(state)
        if stop_basis == "前三根高低" and len(snapshot.candles) >= 3:
            window = snapshot.candles[-3:]
            return min(item.low for item in window) if is_long else max(item.high for item in window)
        if len(snapshot.candles) < 12:
            return None
        atr_values = atr(list(snapshot.candles), 10)
        if not atr_values or atr_values[-1] is None or atr_values[-1] <= 0:
            return None
        atr_value = atr_values[-1]
        return entry_price - atr_value if is_long else entry_price + atr_value

    def _strategy_chart_line_trade_log_prefix(self, session_id: str, inst_id: str = "") -> str:
        """实盘图划线交易主日志前缀，与 `_make_session_logger` 形态一致：``[api] [sid 划线交易 标的]``。"""
        sess = self.sessions.get(session_id)
        api = (getattr(sess, "api_name", None) or "").strip() if sess is not None else ""
        sym = (inst_id or "").strip()
        if not sym and sess is not None:
            sym = (_session_trade_inst_id(sess) or getattr(sess, "symbol", "") or "").strip()
        core = f"{session_id} 划线交易 {sym}".strip() if sym else f"{session_id} 划线交易"
        return f"[{api}] [{core}]" if api else f"[{core}]"

    def _submit_live_chart_trade(self, session_id: str, direction: str) -> None:
        session = self.sessions.get(session_id)
        state = self._strategy_live_chart_windows.get(session_id)
        if session is None or state is None:
            return
        credentials = self._credentials_for_profile_or_none(session.api_name)
        if credentials is None:
            if state.trade_status_text is not None:
                state.trade_status_text.set("缺少 API 凭证，无法下单。")
            return
        trade_inst_id = _session_trade_inst_id(session) or session.symbol
        try:
            instrument = self.client.get_instrument(trade_inst_id)
        except Exception as exc:
            if state.trade_status_text is not None:
                state.trade_status_text.set(f"读取标的失败：{exc}")
            return
        entry_price = self._resolve_live_chart_trade_reference_price(state)
        if entry_price is None or entry_price <= 0:
            if state.trade_status_text is not None:
                state.trade_status_text.set("无法计算开仓价。")
            return
        stop_price = self._resolve_live_chart_trade_stop_price(state, entry_price, direction)
        if stop_price is None or stop_price <= 0 or stop_price == entry_price:
            if state.trade_status_text is not None:
                state.trade_status_text.set("止损价无效，请检查止损基准。")
            return

        config = replace(session.config)
        risk_mode = state.trade_risk_mode.get() if state.trade_risk_mode is not None else "以损定量"
        if risk_mode == "固定张数":
            fixed_size = _parse_decimal_or_none(state.trade_fixed_size.get() if state.trade_fixed_size is not None else "")
            if fixed_size is None or fixed_size <= 0:
                if state.trade_status_text is not None:
                    state.trade_status_text.set("固定张数无效。")
                return
            size = fixed_size
            config = replace(config, order_size=fixed_size, risk_amount=None)
        else:
            risk_amount = _parse_decimal_or_none(state.trade_risk_amount.get() if state.trade_risk_amount is not None else "")
            if risk_amount is None or risk_amount <= 0:
                if state.trade_status_text is not None:
                    state.trade_status_text.set("风险金无效。")
                return
            config = replace(config, risk_amount=risk_amount, order_size=None)
            try:
                size = determine_order_size(
                    instrument=instrument,
                    config=config,
                    entry_price=entry_price,
                    stop_loss=stop_price,
                    risk_price_compatible=True,
                )
            except Exception as exc:
                if state.trade_status_text is not None:
                    state.trade_status_text.set(f"以损定量失败：{exc}")
                return

        side = "buy" if direction == "long" else "sell"
        pos_side = resolve_open_pos_side(config, side)
        order_mode = state.trade_order_mode.get() if state.trade_order_mode is not None else "限价挂单"
        cl_ord_id = f"lt{session.session_id.lower()}{datetime.utcnow().strftime('%m%d%H%M%S%f')[-12:]}"
        try:
            if order_mode == "对手价":
                result = self.client.place_aggressive_limit_order(
                    credentials,
                    config,
                    instrument,
                    side=side,
                    size=size,
                    pos_side=pos_side,
                    cl_ord_id=cl_ord_id,
                )
            else:
                result = self.client.place_simple_order(
                    credentials,
                    config,
                    inst_id=instrument.inst_id,
                    side=side,
                    size=size,
                    ord_type="limit",
                    pos_side=pos_side,
                    price=entry_price,
                    cl_ord_id=cl_ord_id,
                )
        except Exception as exc:
            if state.trade_status_text is not None:
                state.trade_status_text.set(f"下单失败：{exc}")
            return

        if state.trade_status_text is not None:
            state.trade_status_text.set(
                f"已提交{('多' if direction == 'long' else '空')}单 | ordId={result.ord_id or '-'} | size={format_decimal(size)}"
            )
        lt_pre = self._strategy_chart_line_trade_log_prefix(session.session_id, instrument.inst_id)
        self._enqueue_log(
            f"{lt_pre} 已下单 | 方向={direction.upper()} | 标的={instrument.inst_id} | "
            f"开仓价={format_decimal(entry_price)} | 止损={format_decimal(stop_price)} | 数量={format_decimal(size)} | ordId={result.ord_id or '-'}"
        )
        threading.Thread(
            target=self._line_trade_post_entry_worker,
            args=(
                session,
                credentials,
                config,
                instrument,
                result.ord_id or "",
                cl_ord_id,
                side,
                pos_side,
                size,
                entry_price,
                stop_price,
            ),
            daemon=True,
        ).start()

    def _line_trade_post_entry_worker(
        self,
        session: StrategySession,
        credentials: Credentials,
        config: StrategyConfig,
        instrument: Instrument,
        ord_id: str,
        cl_ord_id: str,
        side: str,
        pos_side: str | None,
        size: Decimal,
        entry_price: Decimal,
        stop_price: Decimal,
    ) -> None:
        try:
            if ord_id:
                for _ in range(40):
                    status = self.client.get_order(credentials, config, inst_id=instrument.inst_id, ord_id=ord_id, cl_ord_id=cl_ord_id)
                    if status.state in {"filled", "partially_filled"}:
                        fill_price = status.avg_price or status.price or entry_price
                        fill_size = status.filled_size if status.filled_size and status.filled_size > 0 else size
                        self.root.after(
                            0,
                            lambda sid=session.session_id: self._mark_line_trade_started(
                                sid,
                                instrument=instrument,
                                side=side,
                                pos_side=pos_side,
                                size=fill_size,
                                entry_price=fill_price,
                                stop_price=stop_price,
                                credentials=credentials,
                                config=config,
                                ord_id=ord_id,
                                cl_ord_id=cl_ord_id,
                            ),
                        )
                        return
                    if status.state in {"canceled", "order_failed"}:
                        self.root.after(0, lambda sid=session.session_id, st=status.state: self._mark_line_trade_status(sid, f"委托结束：{st}"))
                        return
                    threading.Event().wait(1.0)
                self.root.after(0, lambda sid=session.session_id: self._mark_line_trade_status(sid, "委托未成交，等待手动处理。"))
                return
            self.root.after(0, lambda sid=session.session_id: self._mark_line_trade_status(sid, "委托缺少订单号，无法追踪。"))
        except Exception as exc:
            self.root.after(0, lambda sid=session.session_id, msg=str(exc): self._mark_line_trade_status(sid, f"委托追踪失败：{msg}"))

    def _mark_line_trade_status(self, session_id: str, text: str) -> None:
        state = self._strategy_live_chart_windows.get(session_id)
        if state is not None and state.trade_status_text is not None:
            state.trade_status_text.set(text)
        lt_pre = self._strategy_chart_line_trade_log_prefix(session_id)
        self._enqueue_log(f"{lt_pre} {text}")

    def _mark_line_trade_started(
        self,
        session_id: str,
        *,
        instrument: Instrument,
        side: str,
        pos_side: str | None,
        size: Decimal,
        entry_price: Decimal,
        stop_price: Decimal,
        credentials: Credentials,
        config: StrategyConfig,
        ord_id: str,
        cl_ord_id: str,
    ) -> None:
        state = self._strategy_live_chart_windows.get(session_id)
        if state is not None and state.trade_status_text is not None:
            state.trade_status_text.set(
                f"已成交，启动动态管理 | entry={format_decimal(entry_price)} stop={format_decimal(stop_price)}"
            )
        lt_pre = self._strategy_chart_line_trade_log_prefix(session_id, instrument.inst_id)
        self._enqueue_log(
            f"{lt_pre} 已成交 | ordId={ord_id or '-'} | side={side} | entry={format_decimal(entry_price)} | stop={format_decimal(stop_price)}"
        )
        direction = "long" if side == "buy" else "short"
        risk_per_unit = abs(entry_price - stop_price)
        take_profit = entry_price + (risk_per_unit * Decimal("4")) if direction == "long" else entry_price - (risk_per_unit * Decimal("4"))
        protection = ProtectionPlan(
            trigger_inst_id=instrument.inst_id,
            trigger_price_type=config.tp_sl_trigger_type,
            take_profit=take_profit,
            stop_loss=stop_price,
            entry_reference=entry_price,
            atr_value=risk_per_unit,
            direction=direction,
            candle_ts=int(time.time() * 1000),
        )
        sess = self.sessions.get(session_id)
        chart_line_logger = self._make_session_logger(
            session_id,
            "划线交易",
            instrument.inst_id,
            api_name=(sess.api_name if sess else "") or "",
            log_file_path=(sess.log_file_path if sess else None),
        )
        engine = StrategyEngine(
            self.client,
            chart_line_logger,
            notifier=self._build_notifier(config),
            strategy_name="划线交易",
            session_id=session_id,
            direction_label="只做多" if direction == "long" else "只做空",
            run_mode_label="交易并下单",
            trader_id="LINE",
            api_name=(sess.api_name if sess else "") or "",
        )
        position = FilledPosition(
            ord_id=ord_id or cl_ord_id,
            cl_ord_id=cl_ord_id,
            inst_id=instrument.inst_id,
            side=side,  # type: ignore[arg-type]
            close_side="sell" if side == "buy" else "buy",  # type: ignore[arg-type]
            pos_side=pos_side,  # type: ignore[arg-type]
            size=size,
            entry_price=entry_price,
            entry_ts=int(time.time() * 1000),
        )
        threading.Thread(
            target=lambda: engine._monitor_local_exit_v2(credentials, config, instrument, position, protection),
            daemon=True,
        ).start()

    def _mark_line_desk_trade_status(self, desk_ref: LineTradingDeskWindowState, text: str) -> None:
        st = self._line_trading_desk_window
        if st is not None and st is desk_ref and _widget_exists(st.window):
            st.status_text.set(text)
        self._line_trading_desk_dual_log(desk_ref, f"开仓后续 | {text}")

    def _mark_line_desk_trade_started(
        self,
        desk_ref: LineTradingDeskWindowState,
        *,
        session_log_tag: str,
        instrument: Instrument,
        side: str,
        pos_side: str | None,
        size: Decimal,
        entry_price: Decimal,
        stop_price: Decimal,
        credentials: Credentials,
        config: StrategyConfig,
        ord_id: str,
        cl_ord_id: str,
        api_profile: str,
        tp_r_multiple: Decimal | None = None,
    ) -> None:
        st = self._line_trading_desk_window
        if st is not None and st is desk_ref and _widget_exists(st.window):
            st.status_text.set(
                f"已成交，启动动态管理 | entry={format_decimal(entry_price)} stop={format_decimal(stop_price)}"
            )
        pr = self._line_trading_desk_log_prefix(desk_ref)

        def _desk_local_exit_log(message: str) -> None:
            self._enqueue_log(f"{pr} 划线交易台监控 | [{session_log_tag}] {message}")
            self._line_trading_desk_local_log(desk_ref, f"监控 | [{session_log_tag}] {message}")

        self._line_trading_desk_dual_log(
            desk_ref,
            f"成交监控 | [{session_log_tag}] ordId={ord_id or '-'} | side={side} | "
            f"entry={format_decimal(entry_price)} | stop={format_decimal(stop_price)}",
        )
        direction = "long" if side == "buy" else "short"
        risk_per_unit = abs(entry_price - stop_price)
        r_mult = tp_r_multiple if tp_r_multiple is not None and tp_r_multiple > 0 else Decimal("4")
        take_profit = (
            entry_price + (risk_per_unit * r_mult) if direction == "long" else entry_price - (risk_per_unit * r_mult)
        )
        protection = ProtectionPlan(
            trigger_inst_id=instrument.inst_id,
            trigger_price_type=config.tp_sl_trigger_type,
            take_profit=take_profit,
            stop_loss=stop_price,
            entry_reference=entry_price,
            atr_value=risk_per_unit,
            direction=direction,
            candle_ts=int(time.time() * 1000),
        )
        engine = StrategyEngine(
            self.client,
            lambda message: self.root.after(0, lambda m=message: _desk_local_exit_log(m)),
            notifier=self._build_notifier(config),
            strategy_name=f"{session_log_tag} 划线交易台",
            session_id=session_log_tag,
            direction_label="只做多" if direction == "long" else "只做空",
            run_mode_label="交易并下单",
            trader_id="LINE_DESK",
            api_name=api_profile,
        )
        position = FilledPosition(
            ord_id=ord_id or cl_ord_id,
            cl_ord_id=cl_ord_id,
            inst_id=instrument.inst_id,
            side=side,  # type: ignore[arg-type]
            close_side="sell" if side == "buy" else "buy",  # type: ignore[arg-type]
            pos_side=pos_side,  # type: ignore[arg-type]
            size=size,
            entry_price=entry_price,
            entry_ts=int(time.time() * 1000),
        )
        threading.Thread(
            target=lambda: engine._monitor_local_exit_v2(credentials, config, instrument, position, protection),
            daemon=True,
        ).start()

    def _line_desk_post_entry_worker(
        self,
        desk_ref: LineTradingDeskWindowState,
        credentials: Credentials,
        config: StrategyConfig,
        instrument: Instrument,
        ord_id: str,
        cl_ord_id: str,
        side: str,
        pos_side: str | None,
        size: Decimal,
        entry_price: Decimal,
        stop_price: Decimal,
        session_log_tag: str,
        api_profile: str,
        tp_r_multiple: Decimal | None = None,
    ) -> None:
        try:
            q_ord = ord_id.strip() if ord_id else ""
            q_cl = cl_ord_id.strip() if cl_ord_id else ""
            if q_ord or q_cl:
                for _ in range(40):
                    status = self.client.get_order(
                        credentials,
                        config,
                        inst_id=instrument.inst_id,
                        ord_id=q_ord or None,
                        cl_ord_id=q_cl or None,
                    )
                    if status.state in {"filled", "partially_filled"}:
                        fill_price = status.avg_price or status.price or entry_price
                        fill_size = status.filled_size if status.filled_size and status.filled_size > 0 else size
                        self.root.after(
                            0,
                            lambda dr=desk_ref,
                            slt=session_log_tag,
                            ins=instrument,
                            sd=side,
                            ps=pos_side,
                            fs=fill_size,
                            fp=fill_price,
                            sp=stop_price,
                            cred=credentials,
                            cfg=config,
                            oid=q_ord,
                            cid=q_cl,
                            ap=api_profile,
                            tpr=tp_r_multiple: self._mark_line_desk_trade_started(
                                dr,
                                session_log_tag=slt,
                                instrument=ins,
                                side=sd,
                                pos_side=ps,
                                size=fs,
                                entry_price=fp,
                                stop_price=sp,
                                credentials=cred,
                                config=cfg,
                                ord_id=oid,
                                cl_ord_id=cid,
                                api_profile=ap,
                                tp_r_multiple=tpr,
                            ),
                        )
                        return
                    if status.state in {"canceled", "order_failed"}:
                        self.root.after(
                            0,
                            lambda dr=desk_ref, stv=status.state: self._mark_line_desk_trade_status(dr, f"委托结束：{stv}"),
                        )
                        return
                    threading.Event().wait(1.0)
                self.root.after(
                    0,
                    lambda dr=desk_ref: self._mark_line_desk_trade_status(dr, "委托未成交，等待手动处理。"),
                )
                return
            self.root.after(
                0,
                lambda dr=desk_ref: self._mark_line_desk_trade_status(dr, "委托缺少订单号，无法追踪。"),
            )
        except Exception as exc:
            self.root.after(
                0,
                lambda dr=desk_ref, msg=str(exc): self._mark_line_desk_trade_status(dr, f"委托追踪失败：{msg}"),
            )

    def _strategy_live_chart_headline(self, session: StrategySession) -> str:
        trade_inst_id = _session_trade_inst_id(session) or session.symbol
        return (
            f"{session.session_id} | {session.strategy_name} | {trade_inst_id} | "
            f"周期 {session.config.bar} | API {session.api_name} | 模式 {session.run_mode_label}"
        )

    def _latest_strategy_trade_ledger_record(self, session: StrategySession) -> StrategyTradeLedgerRecord | None:
        matched: list[StrategyTradeLedgerRecord] = []
        if session.history_record_id:
            matched = [
                item
                for item in self._strategy_trade_ledger_records
                if item.history_record_id == session.history_record_id
            ]
        if not matched:
            matched = [item for item in self._strategy_trade_ledger_records if item.session_id == session.session_id]
        if not matched:
            return None
        return max(matched, key=lambda item: (item.closed_at, item.record_id))

    def _strategy_live_chart_event_time_markers(
        self,
        session: StrategySession,
        trade_inst_id: str,
    ) -> tuple[StrategyLiveChartTimeMarker, ...]:
        markers: list[StrategyLiveChartTimeMarker] = []
        trade = session.active_trade
        latest_ledger = self._latest_strategy_trade_ledger_record(session)
        if latest_ledger is not None and latest_ledger.opened_at and latest_ledger.closed_at:
            if latest_ledger.closed_at >= latest_ledger.opened_at:
                markers.append(
                    StrategyLiveChartTimeMarker(
                        key=f"close:{latest_ledger.record_id}",
                        label=f"平仓 {latest_ledger.closed_at.strftime('%m-%d %H:%M')}",
                        at=latest_ledger.closed_at,
                        color="#cf222e",
                        dash=(6, 3),
                        width=2,
                    )
                )

        credentials = self._credentials_for_profile_or_none(session.api_name)
        if credentials is None:
            return tuple(markers)

        open_anchor = None
        if trade is not None and trade.opened_logged_at is not None:
            open_anchor = trade.opened_logged_at
        elif latest_ledger is not None and latest_ledger.opened_at is not None:
            open_anchor = latest_ledger.opened_at
        if open_anchor is None:
            return tuple(markers)

        inst_type = infer_inst_type(trade_inst_id)
        inst_types = (inst_type,) if inst_type else ("SWAP", "FUTURES", "OPTION", "SPOT")
        try:
            fills = self.client.get_fills_history(
                credentials,
                environment=session.config.environment,
                inst_types=inst_types,
                limit=120,
            )
        except Exception:
            return tuple(markers)

        lower_ms = int((open_anchor - timedelta(minutes=2)).timestamp() * 1000)
        relevant_fills = [
            item
            for item in fills
            if item.fill_time is not None
            and item.fill_time >= lower_ms
            and item.inst_id.strip().upper() == trade_inst_id.strip().upper()
        ]
        if not relevant_fills:
            return tuple(markers)
        relevant_fills.sort(key=lambda item: (item.fill_time or 0, item.trade_id or "", item.order_id or ""))

        entry_order_ids = {
            value.strip()
            for value in (
                trade.entry_order_id if trade is not None else "",
                latest_ledger.entry_order_id if latest_ledger is not None else "",
            )
            if str(value).strip()
        }
        close_order_ids = {
            value.strip()
            for value in (latest_ledger.exit_order_id if latest_ledger is not None else "",)
            if str(value).strip()
        }

        open_side = ""
        for fill in relevant_fills:
            order_id = str(fill.order_id or "").strip()
            if order_id and order_id in entry_order_ids and fill.side:
                open_side = str(fill.side).strip().lower()
                break
        if not open_side:
            open_side = str(relevant_fills[0].side or "").strip().lower()

        seen_events: set[str] = set()
        same_side_seen = 0
        close_marker_exists = any(marker.key.startswith("close:") for marker in markers)
        for fill in relevant_fills:
            fill_time = fill.fill_time
            side = str(fill.side or "").strip().lower()
            if fill_time is None or not side:
                continue
            order_id = str(fill.order_id or "").strip()
            dedupe_key = order_id or f"{fill_time}:{side}"
            if dedupe_key in seen_events:
                continue
            seen_events.add(dedupe_key)
            event_at = datetime.fromtimestamp(fill_time / 1000)
            if side == open_side:
                if same_side_seen == 0:
                    same_side_seen += 1
                    continue
                same_side_seen += 1
                markers.append(
                    StrategyLiveChartTimeMarker(
                        key=f"add:{dedupe_key}",
                        label=f"加仓 {event_at.strftime('%m-%d %H:%M')}",
                        at=event_at,
                        color="#1d4ed8",
                        dash=(2, 2),
                    )
                )
                continue
            if order_id and order_id in close_order_ids:
                if close_marker_exists:
                    continue
                markers.append(
                    StrategyLiveChartTimeMarker(
                        key=f"close-order:{dedupe_key}",
                        label=f"平仓 {event_at.strftime('%m-%d %H:%M')}",
                        at=event_at,
                        color="#cf222e",
                        dash=(6, 3),
                        width=2,
                    )
                )
                close_marker_exists = True
                continue
            markers.append(
                StrategyLiveChartTimeMarker(
                    key=f"reduce:{dedupe_key}",
                    label=f"减仓 {event_at.strftime('%m-%d %H:%M')}",
                    at=event_at,
                    color="#d97706",
                    dash=(3, 3),
                )
            )
        return tuple(markers)

    def _close_strategy_live_chart_window(self, session_id: str) -> None:
        state = self._strategy_live_chart_windows.pop(session_id, None)
        if state is None:
            return
        if state.refresh_job is not None:
            try:
                self.root.after_cancel(state.refresh_job)
            except TclError:
                pass
            state.refresh_job = None
        if _widget_exists(state.window):
            state.window.destroy()

    def _close_all_strategy_live_chart_windows(self) -> None:
        for session_id in tuple(self._strategy_live_chart_windows):
            self._close_strategy_live_chart_window(session_id)

    def _request_strategy_live_chart_refresh(self, session_id: str, *, immediate: bool = False) -> None:
        state = self._strategy_live_chart_windows.get(session_id)
        if state is None or not _widget_exists(state.window):
            return
        if state.refresh_job is not None:
            try:
                self.root.after_cancel(state.refresh_job)
            except TclError:
                pass
            state.refresh_job = None
        delay = 0 if immediate else DEFAULT_STRATEGY_LIVE_CHART_REFRESH_MS
        state.refresh_job = self.root.after(delay, lambda target_session_id=session_id: self._run_strategy_live_chart_refresh(target_session_id))

    def _run_strategy_live_chart_refresh(self, session_id: str) -> None:
        state = self._strategy_live_chart_windows.get(session_id)
        if state is None or not _widget_exists(state.window):
            return
        state.refresh_job = None
        if state.refresh_inflight:
            self._request_strategy_live_chart_refresh(session_id, immediate=False)
            return
        session = self.sessions.get(session_id)
        if session is not None:
            state.headline_text.set(self._strategy_live_chart_headline(session))
            state.status_text.set(f"\u72b6\u6001 {session.display_status} | \u6b63\u5728\u5237\u65b0\u5b9e\u65f6K\u7ebf\u56fe...")
        state.refresh_inflight = True
        threading.Thread(target=self._refresh_strategy_live_chart_worker, args=(session_id,), daemon=True).start()

    def _refresh_strategy_live_chart_worker(self, session_id: str) -> None:
        session = self.sessions.get(session_id)
        if session is None:
            self.root.after(0, lambda target_session_id=session_id: self._apply_strategy_live_chart_missing_session(target_session_id))
            return

        trade_inst_id = _session_trade_inst_id(session) or session.symbol.strip().upper()
        if not trade_inst_id:
            self.root.after(
                0,
                lambda target_session_id=session_id: self._apply_strategy_live_chart_error(
                    target_session_id, "\u7f3a\u5c11\u53ef\u67e5\u8be2\u7684\u4ea4\u6613\u6807\u7684\u3002"
                ),
            )
            return

        try:
            candles = self.client.get_candles(
                trade_inst_id,
                session.config.bar,
                limit=DEFAULT_STRATEGY_LIVE_CHART_CANDLE_LIMIT,
            )
        except Exception as exc:
            self.root.after(
                0,
                lambda target_session_id=session_id, message=str(exc): self._apply_strategy_live_chart_error(
                    target_session_id, message
                ),
            )
            return

        pending_entry_prices = self._strategy_live_chart_pending_entry_prices(session)
        position_avg_price, position_refreshed_at = self._strategy_live_chart_position_avg_price(session)
        live_pnl, live_pnl_refreshed_at = self._session_live_pnl_snapshot(session)
        stop_price = self._strategy_live_chart_stop_price(session)
        entry_price = session.active_trade.entry_price if session.active_trade is not None else None
        entry_time = session.active_trade.opened_logged_at if session.active_trade is not None else None
        time_markers = self._strategy_live_chart_event_time_markers(session, trade_inst_id)
        chart_refreshed_at = datetime.now()
        snapshot = build_strategy_live_chart_snapshot(
            session_id=session.session_id,
            candles=candles,
            ema_period=session.config.ema_period,
            trend_ema_period=session.config.trend_ema_period,
            reference_ema_period=session.config.resolved_entry_reference_ema_period(),
            pending_entry_prices=pending_entry_prices,
            entry_price=entry_price,
            entry_time=entry_time,
            time_markers=time_markers,
            position_avg_price=position_avg_price,
            stop_price=stop_price,
            latest_price=candles[-1].close if candles else None,
            note=self._strategy_live_chart_canvas_note(
                pending_entry_count=len(pending_entry_prices),
                position_refreshed_at=position_refreshed_at,
                live_pnl_refreshed_at=live_pnl_refreshed_at,
                stop_price=stop_price,
            ),
        )
        status_text = self._strategy_live_chart_status_text(
            session,
            live_pnl=live_pnl,
            pending_entry_count=len(pending_entry_prices),
            has_position=position_avg_price is not None or entry_price is not None,
            stop_price=stop_price,
        )
        footer_text = self._strategy_live_chart_footer_text(
            session=session,
            trade_inst_id=trade_inst_id,
            chart_refreshed_at=chart_refreshed_at,
            position_refreshed_at=position_refreshed_at,
            live_pnl_refreshed_at=live_pnl_refreshed_at,
            candle_count=len(candles),
            latest_candle_confirmed=bool(candles[-1].confirmed) if candles else True,
        )
        self.root.after(
            0,
            lambda target_session_id=session_id, chart_snapshot=snapshot, status_line=status_text, footer_line=footer_text: self._apply_strategy_live_chart_snapshot(
                target_session_id,
                chart_snapshot,
                status_line,
                footer_line,
            ),
        )

    def _apply_strategy_live_chart_snapshot(
        self,
        session_id: str,
        snapshot: StrategyLiveChartSnapshot,
        status_text: str,
        footer_text: str,
    ) -> None:
        state = self._strategy_live_chart_windows.get(session_id)
        if state is None or not _widget_exists(state.window):
            return
        session = self.sessions.get(session_id)
        state.refresh_inflight = False
        state.last_snapshot = snapshot
        if session is not None:
            state.window.title(self._strategy_live_chart_window_title(session))
            state.headline_text.set(self._strategy_live_chart_headline(session))
        state.status_text.set(status_text)
        state.footer_text.set(footer_text)
        self._render_strategy_live_chart_window(session_id)
        if session is not None:
            self._request_strategy_live_chart_refresh(session_id, immediate=False)

    def _apply_strategy_live_chart_missing_session(self, session_id: str) -> None:
        state = self._strategy_live_chart_windows.get(session_id)
        if state is None or not _widget_exists(state.window):
            return
        state.refresh_inflight = False
        state.status_text.set("\u4f1a\u8bdd\u5df2\u4ece\u8fd0\u884c\u5217\u8868\u79fb\u9664\uff0c\u56fe\u7a97\u505c\u6b62\u81ea\u52a8\u5237\u65b0\u3002")
        state.footer_text.set("\u5982\u5df2\u6e05\u7a7a\u505c\u6b62\u7b56\u7565\uff0c\u53ef\u76f4\u63a5\u5173\u95ed\u8fd9\u4e2a\u56fe\u7a97\u3002")

    def _apply_strategy_live_chart_error(self, session_id: str, message: str) -> None:
        state = self._strategy_live_chart_windows.get(session_id)
        if state is None or not _widget_exists(state.window):
            return
        state.refresh_inflight = False
        friendly_message = _format_network_error_message(message)
        if state.last_snapshot is None:
            state.last_snapshot = StrategyLiveChartSnapshot(
                session_id=session_id,
                candles=(),
                note=friendly_message,
            )
            self._render_strategy_live_chart_window(session_id)
            state.status_text.set(f"\u5b9e\u65f6K\u7ebf\u56fe\u8bfb\u53d6\u5931\u8d25\uff1a{friendly_message}")
        else:
            state.status_text.set(f"\u5b9e\u65f6K\u7ebf\u56fe\u5237\u65b0\u5931\u8d25\uff0c\u7ee7\u7eed\u663e\u793a\u4e0a\u4e00\u5f20\u56fe\uff1a{friendly_message}")
        state.footer_text.set(
            f"\u5c06\u4e8e {DEFAULT_STRATEGY_LIVE_CHART_REFRESH_MS // 1000} \u79d2\u540e\u81ea\u52a8\u91cd\u8bd5\u3002"
        )
        self._request_strategy_live_chart_refresh(session_id, immediate=False)

    def _render_strategy_live_chart_window(self, session_id: str) -> None:
        state = self._strategy_live_chart_windows.get(session_id)
        if state is None or not _widget_exists(state.canvas):
            return
        snapshot = state.last_snapshot
        if snapshot is None:
            snapshot = StrategyLiveChartSnapshot(
                session_id=session_id,
                candles=(),
                note="\u6b63\u5728\u52a0\u8f7dK\u7ebf\u6570\u636e...",
            )
        render_strategy_live_chart(state.canvas, snapshot)
        self._draw_live_chart_annotations(state)

    def _strategy_live_chart_pending_entry_prices(self, session: StrategySession) -> tuple[Decimal, ...]:
        prices: list[Decimal] = []
        seen: set[Decimal] = set()
        for item in self._latest_pending_orders:
            if _trade_order_session_role(item, session) != "ent":
                continue
            for candidate in (item.price, item.order_price, item.trigger_price):
                if candidate is None or candidate in seen:
                    continue
                seen.add(candidate)
                prices.append(candidate)
                break
        return tuple(prices)

    def _strategy_live_chart_stop_price(self, session: StrategySession) -> Decimal | None:
        active_trade = session.active_trade
        if active_trade is not None and active_trade.current_stop_price is not None:
            return active_trade.current_stop_price
        for item in self._latest_pending_orders:
            if _trade_order_session_role(item, session) != "slg":
                continue
            for candidate in (
                item.stop_loss_trigger_price,
                item.trigger_price,
                item.stop_loss_order_price,
                item.order_price,
            ):
                if candidate is not None:
                    return candidate
        return None

    def _strategy_live_chart_position_avg_price(self, session: StrategySession) -> tuple[Decimal | None, datetime | None]:
        snapshot = self._positions_snapshot_for_session(session)
        if snapshot is None:
            return None, None
        positions = [
            position
            for position in snapshot.positions
            if (
                position.position != 0
                and position.avg_price is not None
                and _position_matches_session_live_pnl(
                    position,
                    trade_inst_id=_session_trade_inst_id(session),
                    expected_sides=_session_expected_position_sides(session),
                )
            )
        ]
        if not positions:
            return None, snapshot.refreshed_at
        if len(positions) == 1:
            return positions[0].avg_price, snapshot.refreshed_at
        total_size = sum((abs(position.position) for position in positions), Decimal("0"))
        if total_size <= 0:
            return positions[0].avg_price, snapshot.refreshed_at
        weighted_value = sum((abs(position.position) * (position.avg_price or Decimal("0")) for position in positions), Decimal("0"))
        return weighted_value / total_size, snapshot.refreshed_at

    @staticmethod
    def _strategy_live_chart_canvas_note(
        *,
        pending_entry_count: int,
        position_refreshed_at: datetime | None,
        live_pnl_refreshed_at: datetime | None,
        stop_price: Decimal | None,
    ) -> str:
        parts: list[str] = []
        if pending_entry_count > 0:
            parts.append(f"\u6302\u5355 {pending_entry_count} \u6761")
        if stop_price is not None:
            parts.append("\u6b62\u635f\u5df2\u540c\u6b65")
        if position_refreshed_at is not None:
            parts.append(f"\u6301\u4ed3\u7f13\u5b58 {position_refreshed_at.strftime('%H:%M:%S')}")
        if live_pnl_refreshed_at is not None and live_pnl_refreshed_at != position_refreshed_at:
            parts.append(f"\u6d6e\u76c8\u7f13\u5b58 {live_pnl_refreshed_at.strftime('%H:%M:%S')}")
        return " | ".join(parts)

    def _strategy_live_chart_status_text(
        self,
        session: StrategySession,
        *,
        live_pnl: Decimal | None,
        pending_entry_count: int,
        has_position: bool,
        stop_price: Decimal | None,
    ) -> str:
        parts = [
            f"\u72b6\u6001 {session.display_status}",
            f"\u5b9e\u65f6\u6d6e\u76c8\u4e8f {_format_optional_usdt_precise(live_pnl, places=2)} USDT",
            f"\u51c0\u76c8\u4e8f {_format_optional_usdt_precise(session.net_pnl_total, places=2)} USDT",
        ]
        if has_position:
            parts.append("\u5f53\u524d\u6709\u6301\u4ed3")
        elif pending_entry_count > 0:
            parts.append(f"\u5f53\u524d\u6709\u6302\u5355 x{pending_entry_count}")
        else:
            parts.append("\u5f53\u524d\u65e0\u6301\u4ed3/\u6302\u5355")
        if stop_price is not None:
            parts.append(f"\u6b62\u635f {format_decimal(stop_price)}")
        last_message = (session.last_message or "").strip()
        if last_message:
            if len(last_message) > 72:
                last_message = f"{last_message[:72]}..."
            parts.append(f"\u6700\u8fd1\u6d88\u606f {last_message}")
        return " | ".join(parts)

    @staticmethod
    def _strategy_live_chart_footer_text(
        *,
        session: StrategySession,
        trade_inst_id: str,
        chart_refreshed_at: datetime,
        position_refreshed_at: datetime | None,
        live_pnl_refreshed_at: datetime | None,
        candle_count: int,
        latest_candle_confirmed: bool,
    ) -> str:
        candle_state = "\u5df2\u6536\u76d8" if latest_candle_confirmed else "\u672a\u6536\u76d8"
        parts = [
            f"\u5408\u7ea6 {trade_inst_id}",
            f"\u5468\u671f {session.config.bar}",
            f"\u6700\u8fd1\u5237\u65b0 {chart_refreshed_at.strftime('%H:%M:%S')}",
            f"K\u7ebf {candle_count} \u6839",
            f"\u5f53\u524dK\u7ebf {candle_state}",
            f"\u81ea\u52a8\u5237\u65b0 {DEFAULT_STRATEGY_LIVE_CHART_REFRESH_MS // 1000} \u79d2",
        ]
        if position_refreshed_at is not None:
            parts.append(f"\u6301\u4ed3\u7f13\u5b58 {position_refreshed_at.strftime('%H:%M:%S')}")
        if live_pnl_refreshed_at is not None and live_pnl_refreshed_at != position_refreshed_at:
            parts.append(f"\u6d6e\u76c8\u7f13\u5b58 {live_pnl_refreshed_at.strftime('%H:%M:%S')}")
        return " | ".join(parts)

    def _finish_strategy_template_import(
        self,
        *,
        source: str,
        record: StrategyTemplateRecord,
        definition: StrategyDefinition,
        applied_api: str,
        api_note: str,
    ) -> None:
        self._enqueue_log(
            f"已导入策略参数：{source} | 策略={definition.name} | API={applied_api or '-'} | {api_note}"
        )
        duplicate_session = self._find_duplicate_strategy_session(api_name=applied_api, config=record.config) if applied_api else None
        if duplicate_session is not None:
            self._focus_session_row(duplicate_session.session_id)
            messagebox.showwarning(
                "导入完成",
                self._format_duplicate_launch_block_message(duplicate_session, imported=True),
            )
            return

        summary = "\n".join(
            [
                f"已导入：{definition.name}",
                f"交易标的：{self.symbol.get()}",
                f"API：{applied_api or '-'}",
                api_note,
                "导入文件不包含 API 密钥，只会复用本机当前或同名 API 配置。",
                "如需复制参数开新策略，请先改标的或切换 API，再启动。",
                "",
                "是否现在就按这套参数启动？",
            ]
        )
        if messagebox.askyesno("导入完成", summary):
            self.start()

    def _template_record_from_launcher(self, *, force_run_mode: str | None = None) -> StrategyTemplateRecord:
        definition = self._selected_strategy_definition()
        original_run_mode_label = self.run_mode_label.get()
        if force_run_mode is not None:
            self.run_mode_label.set(_reverse_lookup_label(RUN_MODE_OPTIONS, force_run_mode, original_run_mode_label))
        try:
            _, config = self._collect_inputs(definition)
        finally:
            if force_run_mode is not None:
                self.run_mode_label.set(original_run_mode_label)
        if force_run_mode == "signal_only" and not supports_signal_only(definition.strategy_id):
            raise ValueError(f"{definition.name} 当前不支持只发信号邮件模式。")
        return StrategyTemplateRecord(
            strategy_id=definition.strategy_id,
            strategy_name=definition.name,
            api_name=self._current_credential_profile(),
            direction_label=_strategy_template_direction_label(
                definition.strategy_id,
                config,
                fallback=self.signal_mode_label.get() or definition.default_signal_label,
            ),
            run_mode_label=_reverse_lookup_label(RUN_MODE_OPTIONS, config.run_mode, self.run_mode_label.get()),
            symbol=_launcher_symbol_from_strategy_config(definition.strategy_id, config),
            config=config,
        )

    def _clone_template_record_for_symbol(self, record: StrategyTemplateRecord, symbol: str) -> StrategyTemplateRecord:
        normalized_symbol = _normalize_symbol_input(symbol)
        if not normalized_symbol:
            raise ValueError("复制草稿时缺少有效标的。")
        cloned_config = replace(
            record.config,
            inst_id=normalized_symbol,
            trade_inst_id=normalized_symbol if record.config.trade_inst_id is not None else None,
            local_tp_sl_inst_id=None,
        )
        return StrategyTemplateRecord(
            strategy_id=record.strategy_id,
            strategy_name=record.strategy_name,
            api_name=record.api_name,
            direction_label=record.direction_label,
            run_mode_label=record.run_mode_label,
            symbol=normalized_symbol,
            config=cloned_config,
            exported_at=record.exported_at,
        )

    def _clone_template_record_for_targets(
        self,
        record: StrategyTemplateRecord,
        trade_symbol: str,
        trigger_symbol: str = "",
    ) -> StrategyTemplateRecord:
        normalized_trade = _normalize_symbol_input(trade_symbol)
        if not normalized_trade:
            raise ValueError("复制草稿时缺少有效的交易标的。")
        normalized_trigger = _normalize_symbol_input(trigger_symbol) or normalized_trade
        trigger_matches_trade = normalized_trigger == normalized_trade
        cloned_config = replace(
            record.config,
            inst_id=normalized_trigger,
            trade_inst_id=normalized_trade if (not trigger_matches_trade or record.config.trade_inst_id is not None) else None,
            local_tp_sl_inst_id=None if trigger_matches_trade else normalized_trigger,
            tp_sl_mode="exchange" if trigger_matches_trade else "local_custom",
        )
        return StrategyTemplateRecord(
            strategy_id=record.strategy_id,
            strategy_name=record.strategy_name,
            api_name=record.api_name,
            direction_label=record.direction_label,
            run_mode_label=record.run_mode_label,
            symbol=self._format_strategy_symbol_display(normalized_trigger, normalized_trade),
            config=cloned_config,
            exported_at=record.exported_at,
        )

    def _load_trader_desk_snapshot(self) -> None:
        try:
            snapshot = load_trader_desk_snapshot()
        except Exception as exc:
            self._enqueue_log(f"读取交易员管理台数据失败：{exc}")
            return
        self._trader_desk_drafts = list(snapshot.drafts)
        self._trader_desk_runs = list(snapshot.runs)
        self._trader_desk_slots = list(snapshot.slots)
        self._trader_desk_events = list(snapshot.events)
        for slot in self._trader_desk_slots:
            self._update_session_counter_from_session_id(slot.session_id)
        self._repair_trader_desk_slots_from_trade_ledger()

    def _save_trader_desk_snapshot(self) -> None:
        try:
            save_trader_desk_snapshot(self._trader_desk_snapshot_for_ui())
        except Exception as exc:
            self._enqueue_log(f"保存交易员管理台数据失败：{exc}")

    def _trader_desk_snapshot_for_ui(self) -> TraderDeskSnapshot:
        return TraderDeskSnapshot(
            drafts=list(self._trader_desk_drafts),
            runs=list(self._trader_desk_runs),
            slots=list(self._trader_desk_slots),
            events=list(self._trader_desk_events),
        )

    def _repair_trader_desk_slots_from_trade_ledger(self) -> None:
        ledger_by_history_id = {
            record.history_record_id: record
            for record in self._strategy_trade_ledger_records
            if record.history_record_id
        }
        changed = False
        for slot in self._trader_desk_slots:
            if slot.status not in {"closed_profit", "closed_loss", "closed_manual"}:
                continue
            history_record_id = str(slot.history_record_id or "").strip()
            if not history_record_id:
                continue
            ledger_record = ledger_by_history_id.get(history_record_id)
            if ledger_record is None:
                continue
            updated_fields = {
                "opened_at": ledger_record.opened_at,
                "closed_at": ledger_record.closed_at,
                "released_at": ledger_record.closed_at,
                "entry_price": ledger_record.entry_price,
                "exit_price": ledger_record.exit_price,
                "size": ledger_record.size,
                "net_pnl": ledger_record.net_pnl or Decimal("0"),
                "close_reason": ledger_record.close_reason or slot.close_reason,
            }
            for field_name, field_value in updated_fields.items():
                if getattr(slot, field_name) != field_value:
                    setattr(slot, field_name, field_value)
                    changed = True
        if changed:
            self._save_trader_desk_snapshot()

    @staticmethod
    def _session_runtime_snapshot_for_ui(session: StrategySession) -> dict[str, object]:
        return {
            "session_id": session.session_id,
            "runtime_status": session.display_status or session.status,
            "last_message": session.last_message,
            "started_at": session.started_at,
            "ended_reason": session.ended_reason,
            "is_running": bool(session.engine.is_running or session.stop_cleanup_in_progress),
            "log_file_path": str(session.log_file_path) if session.log_file_path is not None else "",
        }

    def _trader_runtime_snapshot_for_ui(self, trader_id: str) -> dict[str, object] | None:
        normalized = trader_id.strip()
        if not normalized:
            return None
        run = self._trader_desk_run_by_id(normalized)
        preferred_session_id = run.armed_session_id if run is not None else ""
        sessions = [session for session in self.sessions.values() if session.trader_id == normalized]
        if preferred_session_id:
            for session in sessions:
                if session.session_id == preferred_session_id:
                    return self._session_runtime_snapshot_for_ui(session)
        if sessions:
            sessions.sort(
                key=lambda item: (
                    1 if (item.engine.is_running or item.stop_cleanup_in_progress) else 0,
                    item.started_at,
                    item.session_id,
                ),
                reverse=True,
            )
            return self._session_runtime_snapshot_for_ui(sessions[0])
        if preferred_session_id:
            return {
                "session_id": preferred_session_id,
                "runtime_status": "未找到活动会话",
                "last_message": "",
                "started_at": None,
                "ended_reason": "当前交易员记录里保留了 watcher 会话号，但主界面里已经找不到这条会话。",
                "is_running": False,
                "log_file_path": "",
            }
        return None

    def _trader_desk_draft_by_id(self, trader_id: str) -> TraderDraftRecord | None:
        normalized = trader_id.strip()
        if not normalized:
            return None
        for draft in self._trader_desk_drafts:
            if draft.trader_id == normalized:
                return draft
        return None

    def _trader_desk_run_by_id(self, trader_id: str, *, create: bool = False) -> TraderRunState | None:
        normalized = trader_id.strip()
        if not normalized:
            return None
        for run in self._trader_desk_runs:
            if run.trader_id == normalized:
                return run
        if not create:
            return None
        run = TraderRunState(trader_id=normalized, updated_at=datetime.now())
        self._trader_desk_runs.append(run)
        return run

    def _trader_desk_slot_for_session(
        self,
        session_id: str,
        trader_slot_id: str = "",
    ) -> TraderSlotRecord | None:
        normalized_slot_id = trader_slot_id.strip()
        if normalized_slot_id:
            for slot in self._trader_desk_slots:
                if slot.slot_id == normalized_slot_id:
                    return slot
        normalized = session_id.strip()
        if not normalized:
            return None
        active_statuses = {"watching", "open"}
        active_matches = [
            slot
            for slot in self._trader_desk_slots
            if slot.session_id == normalized and slot.status in active_statuses
        ]
        if active_matches:
            return max(active_matches, key=lambda item: (item.created_at, item.slot_id))
        all_matches = [slot for slot in self._trader_desk_slots if slot.session_id == normalized]
        if not all_matches:
            return None
        return max(all_matches, key=lambda item: (item.created_at, item.slot_id))

    def _trader_desk_slots_for_statuses(self, trader_id: str, statuses: set[str]) -> list[TraderSlotRecord]:
        return [slot for slot in trader_slots_for(self._trader_desk_slots, trader_id) if slot.status in statuses]

    def _trader_desk_next_slot_id(self, trader_id: str) -> str:
        base = f"{trader_id}-{datetime.now():%Y%m%d%H%M%S%f}"
        slot_id = base
        suffix = 2
        known_ids = {slot.slot_id for slot in self._trader_desk_slots}
        while slot_id in known_ids:
            slot_id = f"{base}-{suffix}"
            suffix += 1
        return slot_id

    def _trader_desk_next_event_id(self, trader_id: str) -> str:
        base = f"{trader_id}-{datetime.now():%Y%m%d%H%M%S%f}"
        event_id = base
        suffix = 2
        known_ids = {event.event_id for event in self._trader_desk_events}
        while event_id in known_ids:
            event_id = f"{base}-{suffix}"
            suffix += 1
        return event_id

    def _update_session_counter_from_session_id(self, session_id: str) -> None:
        normalized = str(session_id or "").strip().upper()
        match = re.fullmatch(r"S(\d+)", normalized)
        if match is None:
            return
        self._session_counter = max(self._session_counter, int(match.group(1)))

    def _trader_desk_add_event(self, trader_id: str, message: str, *, level: str = "info") -> None:
        text = str(message or "").strip()
        normalized = trader_id.strip()
        if not normalized or not text:
            return
        self._trader_desk_events.append(
            TraderEventRecord(
                event_id=self._trader_desk_next_event_id(normalized),
                trader_id=normalized,
                created_at=datetime.now(),
                level=level,
                message=text,
            )
        )
        self._trader_desk_events.sort(key=lambda item: (item.created_at, item.event_id), reverse=True)
        self._trader_desk_events = self._trader_desk_events[:400]
        QuantApp._log_trader_desk_message(self, normalized, text)

    def _trader_desk_log_api_name(self, trader_id: str) -> str:
        normalized = str(trader_id or "").strip()
        if not normalized:
            return ""
        draft = self._trader_desk_draft_by_id(normalized)
        if draft is not None:
            payload = draft.template_payload if isinstance(draft.template_payload, dict) else {}
            api_name = str(payload.get("api_name") or "").strip()
            if api_name:
                return api_name
        for slot in getattr(self, "_trader_desk_slots", []):
            if slot.trader_id == normalized:
                api_name = str(getattr(slot, "api_name", "") or "").strip()
                if api_name:
                    return api_name
        sessions = getattr(self, "sessions", {})
        for session in sessions.values() if isinstance(sessions, dict) else []:
            if str(getattr(session, "trader_id", "") or "").strip() == normalized:
                api_name = str(getattr(session, "api_name", "") or "").strip()
                if api_name:
                    return api_name
        profile_getter = getattr(self, "_current_credential_profile", None)
        if callable(profile_getter):
            return str(profile_getter() or "").strip()
        return ""

    def _log_trader_desk_message(self, trader_id: str, message: str) -> None:
        normalized = str(trader_id or "").strip()
        text = str(message or "").strip()
        if not normalized or not text:
            return
        api_name = QuantApp._trader_desk_log_api_name(self, normalized)
        if api_name:
            self._enqueue_log(f"[{api_name}] [交易员管理台] [{normalized}] {text}")
        else:
            self._enqueue_log(f"[交易员管理台] [{normalized}] {text}")

    @staticmethod
    def _expected_trader_stop_reason(reason: str) -> bool:
        normalized = str(reason or "").strip()
        if not normalized:
            return False
        markers = (
            "人工暂停",
            "手动平仓",
            "用户手动停止",
            "信号观察台手动停止",
            "应用关闭",
            "待恢复",
            "恢复启动失败",
            "停止清理失败",
        )
        return any(marker in normalized for marker in markers)

    @staticmethod
    def _session_stop_reason_text(session: StrategySession) -> str:
        ended_reason = str(getattr(session, "ended_reason", "") or "").strip()
        if ended_reason:
            return ended_reason
        last_message = str(getattr(session, "last_message", "") or "").strip()
        if last_message.startswith("策略停止，原因："):
            detail = last_message.partition("：")[2].strip()
            return detail or last_message
        return "策略线程结束"

    def _save_trader_desk_draft(self, draft: TraderDraftRecord) -> None:
        for index, current in enumerate(self._trader_desk_drafts):
            if current.trader_id == draft.trader_id:
                self._trader_desk_drafts[index] = draft
                break
        else:
            self._trader_desk_drafts.append(draft)
        self._save_trader_desk_snapshot()

    def _delete_trader_desk_draft(self, trader_id: str) -> None:
        draft = self._trader_desk_draft_by_id(trader_id)
        if draft is None:
            raise ValueError("未找到对应的交易员草稿。")
        api_name = QuantApp._trader_desk_log_api_name(self, trader_id)
        self._cleanup_stale_trader_watchers(trader_id)
        active_sessions = [
            session
            for session in self.sessions.values()
            if session.trader_id == trader_id and (session.engine.is_running or session.stop_cleanup_in_progress)
        ]
        if active_sessions:
            raise ValueError("该交易员仍有关联会话在运行，请先暂停或平仓。")
        active_slots = self._trader_desk_slots_for_statuses(trader_id, {"watching", "open"})
        if active_slots:
            raise ValueError("该交易员仍有活动中的额度格，请先暂停或平仓。")
        self._trader_desk_drafts = [item for item in self._trader_desk_drafts if item.trader_id != trader_id]
        self._trader_desk_runs = [item for item in self._trader_desk_runs if item.trader_id != trader_id]
        self._trader_desk_slots = [item for item in self._trader_desk_slots if item.trader_id != trader_id]
        self._trader_desk_events = [item for item in self._trader_desk_events if item.trader_id != trader_id]
        self._save_trader_desk_snapshot()
        if api_name:
            self._enqueue_log(f"[{api_name}] [交易员管理台] [{trader_id}] 已删除。")
        else:
            QuantApp._log_trader_desk_message(self, trader_id, "已删除。")

    def _trader_desk_symbol_choices(self) -> list[str]:
        return list(dict.fromkeys(self._custom_trigger_symbol_values + self._default_symbol_values))

    def _trader_desk_market_price(self, inst_id: str, price_type: str) -> Decimal | None:
        normalized_inst_id = inst_id.strip().upper()
        if not normalized_inst_id:
            return None
        cached = self._trader_gate_price_cache.get(normalized_inst_id)
        now = datetime.now()
        ticker: OkxTicker
        if cached is not None and (now - cached[0]).total_seconds() < 3:
            ticker = cached[1]
        else:
            ticker = self.client.get_ticker(normalized_inst_id)
            self._trader_gate_price_cache[normalized_inst_id] = (now, ticker)
        normalized_type = str(price_type or "mark").strip().lower()
        candidate = (
            ticker.last
            if normalized_type == "last"
            else ticker.index
            if normalized_type == "index"
            else ticker.mark
        )
        return candidate or ticker.last or ticker.mark or ticker.index

    def _trader_desk_gate_passes(self, draft: TraderDraftRecord) -> bool:
        if not draft.gate.enabled:
            return True
        current_price = self._trader_desk_market_price(draft.gate.trigger_inst_id, draft.gate.trigger_price_type)
        if current_price is None:
            return False
        return trader_gate_allows_price(draft.gate, current_price)

    @staticmethod
    def _trader_wave_lock_signal_from_session(session: StrategySession) -> str:
        signal_mode = str(getattr(getattr(session, "config", None), "signal_mode", "") or "").strip().lower()
        if signal_mode == "long_only":
            return "long"
        if signal_mode == "short_only":
            return "short"
        direction_label = str(getattr(session, "direction_label", "") or "").strip()
        if "只做多" in direction_label:
            return "long"
        if "只做空" in direction_label:
            return "short"
        return ""

    def _is_trader_wave_lock_active(self, draft: TraderDraftRecord, run: TraderRunState) -> bool:
        locked_signal = str(run.wave_lock_signal or "").strip().lower()
        if locked_signal not in {"long", "short"}:
            return False
        record = _strategy_template_record_from_payload(draft.template_payload)
        if record is None:
            return False
        config = replace(record.config, run_mode="trade")
        strategy_id = str(config.strategy_id or "").strip()
        if not strategy_id:
            return False
        if is_dynamic_strategy_id(strategy_id):
            strategy = EmaDynamicOrderStrategy()
            lookback = recommended_indicator_lookback(
                config.ema_period,
                config.trend_ema_period,
                config.atr_period,
                config.resolved_entry_reference_ema_period(),
                DEFAULT_DEBUG_ATR_PERIOD,
            )
        elif is_ema_atr_breakout_strategy(strategy_id):
            strategy = EmaAtrStrategy()
            lookback = recommended_indicator_lookback(
                config.ema_period,
                config.trend_ema_period,
                config.big_ema_period,
                config.atr_period,
                DEFAULT_DEBUG_ATR_PERIOD,
            )
        elif strategy_id == STRATEGY_EMA5_EMA8_ID:
            strategy = EmaCrossEmaStopStrategy()
            lookback = recommended_indicator_lookback(
                config.ema_period,
                config.trend_ema_period,
                config.big_ema_period,
                config.atr_period,
                DEFAULT_DEBUG_ATR_PERIOD,
            )
        else:
            return False

        try:
            candles = self.client.get_candles(config.inst_id, config.bar, limit=lookback)
        except Exception as exc:
            QuantApp._log_trader_desk_message(
                self,
                draft.trader_id,
                f"波段锁检查失败：{_format_network_error_message(str(exc))}",
            )
            return True
        confirmed = [candle for candle in candles if candle.confirmed]
        if len(confirmed) < 2:
            return True
        hint_inst = self._find_instrument_for_fixed_order_size_hint(config.inst_id, fetch_if_missing=False)
        price_inc = hint_inst.tick_size if hint_inst is not None else None
        decision = strategy.evaluate(confirmed, config, price_increment=price_inc)
        current_signal = str(decision.signal or "").strip().lower()
        if current_signal == locked_signal:
            return True
        run.wave_lock_signal = ""
        run.updated_at = datetime.now()
        self._trader_desk_add_event(
            draft.trader_id,
            f"波段锁已解除 | 原锁方向={locked_signal.upper()} | 当前信号={current_signal.upper() or 'NONE'}",
        )
        self._save_trader_desk_snapshot()
        return False

    def _trader_desk_start_slot(self, trader_id: str) -> bool:
        draft = self._trader_desk_draft_by_id(trader_id)
        run = self._trader_desk_run_by_id(trader_id, create=True)
        if draft is None or run is None:
            raise ValueError("未找到对应的交易员草稿。")
        if run.status not in {"running", "quota_exhausted"}:
            return False
        if run.armed_session_id:
            return False
        if trader_has_watching_slot(self._trader_desk_slots, trader_id):
            return False
        remaining = trader_remaining_quota_steps(draft, self._trader_desk_slots)
        if remaining <= 0:
            if run.status != "quota_exhausted":
                run.status = "quota_exhausted"
                run.paused_reason = "额度已满，等待释放。"
                run.updated_at = datetime.now()
                self._trader_desk_add_event(trader_id, "额度已满，暂停补位等待释放。", level="warning")
                self._save_trader_desk_snapshot()
            return False
        if not self._trader_desk_gate_passes(draft):
            if run.paused_reason != "价格开关未满足。":
                run.paused_reason = "价格开关未满足。"
                run.updated_at = datetime.now()
                self._save_trader_desk_snapshot()
            return False

        record = _strategy_template_record_from_payload(draft.template_payload)
        if record is None:
            run.status = "stopped"
            run.paused_reason = "草稿缺少有效模板。"
            run.updated_at = datetime.now()
            self._trader_desk_add_event(trader_id, "启动 watcher 失败：草稿缺少有效模板。", level="error")
            self._save_trader_desk_snapshot()
            return False
        definition = self._resolve_strategy_template_definition(record)
        resolved_api_name, _ = _resolve_import_api_profile(
            record.api_name,
            self._current_credential_profile(),
            set(self._credential_profiles.keys()),
        )
        target_api_name = resolved_api_name or self._current_credential_profile()
        credentials = self._credentials_for_profile_or_none(target_api_name)
        if credentials is None:
            run.status = "stopped"
            run.paused_reason = f"未找到 API：{target_api_name}"
            run.updated_at = datetime.now()
            self._trader_desk_add_event(trader_id, f"启动 watcher 失败：未找到 API {target_api_name}。", level="error")
            self._save_trader_desk_snapshot()
            return False

        slot_id = self._trader_desk_next_slot_id(trader_id)
        config = replace(
            record.config,
            run_mode="trade",
            risk_amount=None,
            order_size=draft.unit_quota,
            trader_virtual_stop_loss=True,
        )
        notifier = self._build_notifier(config)
        run.armed_session_id = "__starting__"
        try:
            session_id = self._start_strategy_session(
                definition=definition,
                credentials=credentials,
                config=config,
                notifier=notifier,
                api_name=target_api_name,
                direction_label=_strategy_template_direction_label(
                    definition.strategy_id,
                    config,
                    fallback=record.direction_label or definition.default_signal_label,
                ),
                run_mode_label=_reverse_lookup_label(RUN_MODE_OPTIONS, config.run_mode, record.run_mode_label),
                source_label=f"交易员 {trader_id}",
                select_session=False,
                allow_duplicate_launch=True,
                trader_id=trader_id,
                trader_slot_id=slot_id,
            )
        except Exception as exc:
            run.armed_session_id = ""
            run.status = "stopped"
            run.paused_reason = str(exc)
            run.updated_at = datetime.now()
            self._trader_desk_add_event(trader_id, f"启动 watcher 失败：{exc}", level="error")
            self._save_trader_desk_snapshot()
            return False

        now = datetime.now()
        self._trader_desk_slots.append(
            TraderSlotRecord(
                slot_id=slot_id,
                trader_id=trader_id,
                session_id=session_id,
                api_name=target_api_name,
                strategy_name=definition.name,
                symbol=self._format_strategy_symbol_display(config.inst_id, config.trade_inst_id),
                bar=str(config.bar or "").strip(),
                direction_label=record.direction_label or definition.default_signal_label,
                status="watching",
                quota_occupied=False,
                created_at=now,
            )
        )
        run.status = "running"
        run.paused_reason = ""
        run.armed_session_id = session_id
        run.last_started_at = now
        run.last_event_at = now
        run.updated_at = now
        draft.updated_at = now
        self._trader_desk_add_event(
            trader_id,
            "已启动 watcher"
            f" | 会话={session_id}"
            f" | 周期={config.bar}"
            f" | 固定数量={format_decimal(draft.unit_quota)}"
            f" | 剩余额度格={trader_remaining_quota_steps(draft, self._trader_desk_slots)}",
        )
        self._save_trader_desk_snapshot()
        return True

    def _ensure_trader_watcher(self, trader_id: str) -> None:
        draft = self._trader_desk_draft_by_id(trader_id)
        run = self._trader_desk_run_by_id(trader_id)
        if draft is None or run is None:
            return
        if run.status not in {"running", "quota_exhausted"}:
            return
        self._cleanup_stale_trader_watchers(trader_id)
        if trader_has_watching_slot(self._trader_desk_slots, trader_id):
            return
        if self._is_trader_wave_lock_active(draft, run):
            lock_signal = str(run.wave_lock_signal or "").strip().lower()
            QuantApp._log_trader_desk_message(
                self,
                trader_id,
                f"波段锁生效，跳过补位 | 锁方向={lock_signal.upper() or '-'} | 等待当前波失效后再开新单",
            )
            return
        remaining = trader_remaining_quota_steps(draft, self._trader_desk_slots)
        if remaining <= 0:
            if run.status != "quota_exhausted":
                run.status = "quota_exhausted"
                run.paused_reason = "额度已满，等待释放。"
                run.updated_at = datetime.now()
                self._trader_desk_add_event(trader_id, "额度已满，暂停补位等待释放。", level="warning")
                self._save_trader_desk_snapshot()
            return
        if run.status == "quota_exhausted":
            run.status = "running"
            run.paused_reason = ""
            run.updated_at = datetime.now()
        self._trader_desk_start_slot(trader_id)

    def _cleanup_stale_trader_watchers(self, trader_id: str) -> None:
        run = self._trader_desk_run_by_id(trader_id, create=True)
        if run is None:
            return
        changed = False
        now = datetime.now()
        for slot in self._trader_desk_slots_for_statuses(trader_id, {"watching"}):
            session = self.sessions.get(slot.session_id)
            if session is not None and (session.engine.is_running or session.stop_cleanup_in_progress or session.status in {"运行中", "停止中", "恢复中"}):
                continue
            slot.status = "stopped"
            slot.closed_at = slot.closed_at or now
            slot.released_at = slot.released_at or now
            slot.close_reason = slot.close_reason or "watcher 会话不存在或已停止"
            if run.armed_session_id == slot.session_id:
                run.armed_session_id = ""
                run.last_event_at = now
                run.updated_at = now
            self._trader_desk_add_event(
                trader_id,
                f"检测到失效 watcher，已清理 | 会话={slot.session_id} | 原因={slot.close_reason}",
                level="warning",
            )
            changed = True
        if changed:
            self._save_trader_desk_snapshot()

    def start_trader_draft(self, trader_id: str) -> None:
        draft = self._trader_desk_draft_by_id(trader_id)
        run = self._trader_desk_run_by_id(trader_id, create=True)
        if draft is None or run is None:
            raise ValueError("未找到对应的交易员草稿。")
        now = datetime.now()
        draft.status = "ready"
        draft.updated_at = now
        run.status = "running"
        run.paused_reason = ""
        run.updated_at = now
        run.last_started_at = now
        run.wave_lock_signal = ""
        self._trader_desk_add_event(trader_id, "已启动交易员。")
        self._save_trader_desk_snapshot()
        self._ensure_trader_watcher(trader_id)

    def pause_trader_draft(self, trader_id: str) -> None:
        draft = self._trader_desk_draft_by_id(trader_id)
        run = self._trader_desk_run_by_id(trader_id, create=True)
        if draft is None or run is None:
            raise ValueError("未找到对应的交易员草稿。")
        now = datetime.now()
        draft.status = "paused"
        draft.updated_at = now
        run.status = "paused_manual"
        run.paused_reason = "人工暂停。"
        run.updated_at = now
        session_ids = [slot.session_id for slot in self._trader_desk_slots_for_statuses(trader_id, {"watching"})]
        for session_id in session_ids:
            self._request_stop_strategy_session(
                session_id,
                ended_reason="交易员人工暂停",
                source_label=f"交易员 {trader_id} 人工暂停",
                show_dialog=False,
            )
        self._trader_desk_add_event(trader_id, f"已人工暂停交易员，停止 watcher {len(session_ids)} 个。", level="warning")
        self._save_trader_desk_snapshot()

    def resume_trader_draft(self, trader_id: str) -> None:
        draft = self._trader_desk_draft_by_id(trader_id)
        run = self._trader_desk_run_by_id(trader_id, create=True)
        if draft is None or run is None:
            raise ValueError("未找到对应的交易员草稿。")
        now = datetime.now()
        draft.status = "ready"
        draft.updated_at = now
        run.status = "running"
        run.paused_reason = ""
        run.updated_at = now
        self._trader_desk_add_event(trader_id, "已恢复交易员。")
        self._save_trader_desk_snapshot()
        self._ensure_trader_watcher(trader_id)

    def flatten_trader_draft(self, trader_id: str, flatten_mode: str = "market") -> None:
        draft = self._trader_desk_draft_by_id(trader_id)
        run = self._trader_desk_run_by_id(trader_id, create=True)
        if draft is None or run is None:
            raise ValueError("未找到对应的交易员草稿。")
        normalized_flatten_mode = self._normalize_trader_manual_flatten_mode(flatten_mode)
        now = datetime.now()
        draft.status = "paused"
        draft.updated_at = now
        run.status = "paused_manual"
        run.paused_reason = "人工平仓。"
        run.armed_session_id = ""
        run.updated_at = now
        active_slots = self._trader_desk_slots_for_statuses(trader_id, {"watching", "open"})
        session_ids = sorted({slot.session_id for slot in active_slots if slot.session_id})
        for session_id in session_ids:
            self._request_stop_strategy_session(
                session_id,
                ended_reason="交易员手动平仓",
                source_label=f"交易员 {trader_id} 手动平仓",
                show_dialog=False,
            )
        watching_slots = [slot for slot in active_slots if slot.status == "watching"]
        for slot in watching_slots:
            slot.status = "stopped"
            slot.quota_occupied = False
            slot.closed_at = slot.closed_at or now
            slot.released_at = slot.released_at or now
            slot.close_reason = slot.close_reason or "人工平仓停止 watcher"
        open_slots = [slot for slot in active_slots if slot.status == "open"]
        submitted_count, stale_count, failed_count = self._submit_trader_manual_flatten_orders(
            draft,
            open_slots,
            now,
            flatten_mode=normalized_flatten_mode,
        )
        if submitted_count or stale_count or failed_count:
            self._trader_desk_add_event(
                trader_id,
                "手动平仓结果 | "
                f"方式={self._trader_manual_flatten_mode_label(normalized_flatten_mode)} | "
                f"已提交平仓单 {submitted_count} 个 | "
                f"已清理无真实持仓槽位 {stale_count} 个 | "
                f"提交失败 {failed_count} 个",
                level="warning",
            )
        self._trader_desk_add_event(
            trader_id,
            f"已请求手动平仓/停止 {len(session_ids)} 个额度格 | 方式={self._trader_manual_flatten_mode_label(normalized_flatten_mode)}。",
            level="warning",
        )
        self._save_trader_desk_snapshot()

    @staticmethod
    def _trader_manual_flatten_open_side(
        config: StrategyConfig,
    ) -> tuple[str, str, str | None]:
        long_pos_side = "long" if config.position_mode == "long_short" else None
        short_pos_side = "short" if config.position_mode == "long_short" else None
        signal_mode = str(config.signal_mode or "").strip().lower()
        if signal_mode == "long_only":
            return ("sell", "long", long_pos_side)
        if signal_mode == "short_only":
            return ("buy", "short", short_pos_side)
        raise ValueError("交易员手动平仓仅支持只做多或只做空策略。")

    @staticmethod
    def _trader_position_closeable_size(position: OkxPosition) -> Decimal:
        base = position.avail_position
        if base is None or base == 0:
            base = position.position
        return abs(base)

    @staticmethod
    def _trader_slot_flatten_size(slot: TraderSlotRecord, draft: TraderDraftRecord) -> Decimal:
        if slot.size is not None and slot.size > 0:
            return slot.size
        return draft.unit_quota

    @staticmethod
    def _build_trader_manual_flatten_cl_ord_id(slot: TraderSlotRecord) -> str:
        session_token = "".join(ch for ch in slot.session_id.lower() if ch.isascii() and ch.isalnum())[:4] or "sess"
        strategy_token = "".join(ch for ch in slot.strategy_name.lower() if ch.isascii() and ch.isalnum())[:4] or "trdr"
        suffix = datetime.now().strftime("%m%d%H%M%S%f")[-15:]
        return f"{session_token}{strategy_token}exi{suffix}"[:32]

    @staticmethod
    def _normalize_trader_manual_flatten_mode(flatten_mode: str) -> str:
        normalized = str(flatten_mode or "").strip().lower()
        if normalized == "best_quote":
            return "best_quote"
        return "market"

    @staticmethod
    def _trader_manual_flatten_mode_label(flatten_mode: str) -> str:
        if QuantApp._normalize_trader_manual_flatten_mode(flatten_mode) == "best_quote":
            return "挂买一/卖一平仓"
        return "市价平仓"

    def _lookup_trader_manual_flatten_order_status(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        *,
        inst_id: str,
        result: OkxOrderResult,
    ):
        order_id = (result.ord_id or "").strip()
        client_order_id = (result.cl_ord_id or "").strip()
        if not order_id and not client_order_id:
            return None
        for _ in range(3):
            try:
                status = self.client.get_order(
                    credentials,
                    config,
                    inst_id=inst_id,
                    ord_id=order_id or None,
                    cl_ord_id=client_order_id or None,
                )
            except Exception:
                threading.Event().wait(0.2)
                continue
            return status
        return None

    def _lookup_trader_manual_flatten_exit_price(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        *,
        inst_id: str,
        result: OkxOrderResult,
    ) -> Decimal | None:
        status = self._lookup_trader_manual_flatten_order_status(
            credentials,
            config,
            inst_id=inst_id,
            result=result,
        )
        if status is None:
            return None
        return status.avg_price or status.price

    def _resolve_trader_best_quote_flatten_price(
        self,
        instrument: Instrument,
        *,
        side: str,
    ) -> Decimal:
        order_book = None
        try:
            order_book = self.client.get_order_book(instrument.inst_id, depth=5)
        except Exception:
            order_book = None
        ticker = self.client.get_ticker(instrument.inst_id)
        if side == "buy":
            raw_price = order_book.bids[0][0] if order_book is not None and order_book.bids else ticker.bid
            if raw_price is None or raw_price <= 0:
                raise ValueError(f"{instrument.inst_id} 当前缺少买一价，无法按买一挂平空单。")
            return snap_to_increment(raw_price, instrument.tick_size, "down")
        raw_price = order_book.asks[0][0] if order_book is not None and order_book.asks else ticker.ask
        if raw_price is None or raw_price <= 0:
            raise ValueError(f"{instrument.inst_id} 当前缺少卖一价，无法按卖一挂平多单。")
        return snap_to_increment(raw_price, instrument.tick_size, "up")

    @staticmethod
    def _clear_trader_manual_flatten_pending(slot: TraderSlotRecord) -> None:
        slot.pending_manual_exit_mode = ""
        slot.pending_manual_exit_inst_id = ""
        slot.pending_manual_exit_order_id = ""
        slot.pending_manual_exit_cl_ord_id = ""

    def _mark_trader_slot_manual_flatten_closed(
        self,
        slot: TraderSlotRecord,
        *,
        now: datetime,
        exit_price: Decimal | None,
        flatten_mode: str,
    ) -> None:
        normalized_flatten_mode = self._normalize_trader_manual_flatten_mode(flatten_mode)
        slot.status = "closed_manual"
        slot.quota_occupied = False
        slot.closed_at = slot.closed_at or now
        slot.released_at = slot.released_at or now
        if normalized_flatten_mode == "best_quote":
            slot.close_reason = "人工最优价挂单平仓已成交"
        else:
            slot.close_reason = "人工手动平仓"
        slot.exit_price = exit_price
        self._clear_trader_manual_flatten_pending(slot)

    def _submit_trader_manual_flatten_orders(
        self,
        draft: TraderDraftRecord,
        open_slots: list[TraderSlotRecord],
        now: datetime,
        *,
        flatten_mode: str = "market",
    ) -> tuple[int, int, int]:
        normalized_flatten_mode = self._normalize_trader_manual_flatten_mode(flatten_mode)
        if not open_slots:
            return (0, 0, 0)
        config = _deserialize_strategy_config_snapshot(draft.template_payload.get("config_snapshot"))
        if config is None:
            raise ValueError("交易员草稿缺少可用的策略配置快照，无法执行手动平仓。")
        credentials = self._credentials_for_profile_or_none(str(draft.template_payload.get("api_name") or ""))
        if credentials is None:
            raise ValueError("当前找不到该交易员对应的 API 凭证，无法执行手动平仓。")
        trade_inst_id = (
            config.trade_inst_id
            or config.inst_id
            or str(draft.template_payload.get("symbol") or "")
        ).strip().upper()
        if not trade_inst_id:
            raise ValueError("交易员草稿缺少交易标的，无法执行手动平仓。")
        close_side, expected_position_side, pos_side = self._trader_manual_flatten_open_side(config)
        instrument = self.client.get_instrument(trade_inst_id)
        positions = self.client.get_positions(credentials, environment=config.environment)
        remaining_live_size = sum(
            self._trader_position_closeable_size(position)
            for position in positions
            if position.inst_id.strip().upper() == trade_inst_id
            and _format_pos_side(position.pos_side, position.position).strip().lower() == expected_position_side
            and ((position.mgn_mode or "").strip().lower() or config.trade_mode) == config.trade_mode
        )

        submitted_count = 0
        stale_count = 0
        failed_count = 0
        for slot in sorted(open_slots, key=lambda item: (item.created_at, item.slot_id)):
            if slot.pending_manual_exit_order_id or slot.pending_manual_exit_cl_ord_id:
                self._trader_desk_add_event(
                    draft.trader_id,
                    f"跳过重复手动平仓提交 | 会话={slot.session_id} | 方式={self._trader_manual_flatten_mode_label(slot.pending_manual_exit_mode)} | 已有待成交平仓单",
                    level="warning",
                )
                continue
            requested_size = self._trader_slot_flatten_size(slot, draft)
            if remaining_live_size <= 0:
                slot.status = "stopped"
                slot.quota_occupied = False
                slot.closed_at = slot.closed_at or now
                slot.released_at = slot.released_at or now
                slot.close_reason = "人工平仓时未检测到交易所持仓"
                self._clear_trader_manual_flatten_pending(slot)
                stale_count += 1
                continue

            close_size = snap_to_increment(min(requested_size, remaining_live_size), instrument.lot_size, "down")
            if close_size < instrument.min_size:
                slot.status = "stopped"
                slot.quota_occupied = False
                slot.closed_at = slot.closed_at or now
                slot.released_at = slot.released_at or now
                slot.close_reason = "人工平仓时剩余交易所持仓不足最小下单量"
                self._clear_trader_manual_flatten_pending(slot)
                stale_count += 1
                continue

            try:
                if normalized_flatten_mode == "best_quote":
                    best_quote_price = self._resolve_trader_best_quote_flatten_price(instrument, side=close_side)
                    result = self.client.place_simple_order(
                        credentials,
                        config,
                        inst_id=trade_inst_id,
                        side=close_side,
                        size=close_size,
                        ord_type="limit",
                        pos_side=pos_side,
                        price=best_quote_price,
                        cl_ord_id=self._build_trader_manual_flatten_cl_ord_id(slot),
                    )
                else:
                    best_quote_price = None
                    result = self.client.place_simple_order(
                        credentials,
                        config,
                        inst_id=trade_inst_id,
                        side=close_side,
                        size=close_size,
                        ord_type="market",
                        pos_side=pos_side,
                        cl_ord_id=self._build_trader_manual_flatten_cl_ord_id(slot),
                    )
            except Exception as exc:
                failed_count += 1
                slot.note = _format_network_error_message(str(exc))
                self._trader_desk_add_event(
                    draft.trader_id,
                    f"手动平仓提交失败 | 会话={slot.session_id} | 合约={trade_inst_id} | 方式={self._trader_manual_flatten_mode_label(normalized_flatten_mode)} | 原因={slot.note}",
                    level="warning",
                )
                continue

            order_status = self._lookup_trader_manual_flatten_order_status(
                credentials,
                config,
                inst_id=trade_inst_id,
                result=result,
            )
            latest_exit_price = None if order_status is None else (order_status.avg_price or order_status.price)
            latest_state = "" if order_status is None else str(order_status.state or "").strip().lower()

            slot.pending_manual_exit_mode = normalized_flatten_mode
            slot.pending_manual_exit_inst_id = trade_inst_id
            slot.pending_manual_exit_order_id = (result.ord_id or "").strip()
            slot.pending_manual_exit_cl_ord_id = (result.cl_ord_id or "").strip()

            if latest_state == "filled":
                self._mark_trader_slot_manual_flatten_closed(
                    slot,
                    now=now,
                    exit_price=latest_exit_price,
                    flatten_mode=normalized_flatten_mode,
                )
                slot.note = (
                    f"人工平仓已成交 | 方式={self._trader_manual_flatten_mode_label(normalized_flatten_mode)} | "
                    f"ordId={(result.ord_id or '-').strip() or '-'}"
                )
                remaining_live_size = max(remaining_live_size - close_size, Decimal("0"))
            else:
                slot.note = (
                    f"人工平仓单已提交 | 方式={self._trader_manual_flatten_mode_label(normalized_flatten_mode)} | "
                    f"ordId={(result.ord_id or '-').strip() or '-'} | "
                    f"clOrdId={(result.cl_ord_id or '-').strip() or '-'}"
                )
                if normalized_flatten_mode == "best_quote" and best_quote_price is not None:
                    slot.note = f"{slot.note} | 挂单价={format_decimal(best_quote_price)}"
                slot.close_reason = f"人工{self._trader_manual_flatten_mode_label(normalized_flatten_mode)}待成交"
                self._trader_desk_add_event(
                    draft.trader_id,
                    f"人工平仓单已提交待成交 | 会话={slot.session_id} | 合约={trade_inst_id} | 方式={self._trader_manual_flatten_mode_label(normalized_flatten_mode)} | 状态={latest_state or 'unknown'}",
                    level="warning",
                )
            submitted_count += 1

        return (submitted_count, stale_count, failed_count)

    def force_clear_trader_draft(self, trader_id: str) -> None:
        draft = self._trader_desk_draft_by_id(trader_id)
        run = self._trader_desk_run_by_id(trader_id, create=True)
        if draft is None or run is None:
            raise ValueError("未找到对应的交易员草稿。")
        now = datetime.now()
        draft.status = "paused"
        draft.updated_at = now
        run.status = "paused_manual"
        run.paused_reason = "人工强制清格。"
        run.armed_session_id = ""
        run.last_event_at = now
        run.updated_at = now

        stop_requested = 0
        for session in [item for item in self.sessions.values() if item.trader_id == trader_id]:
            if session.stop_cleanup_in_progress or not session.engine.is_running:
                continue
            if self._request_stop_strategy_session(
                session.session_id,
                ended_reason="交易员强制清格",
                source_label=f"交易员 {trader_id} 强制清格",
                show_dialog=False,
            ):
                stop_requested += 1

        cleared_slots = 0
        for slot in trader_slots_for(self._trader_desk_slots, trader_id):
            if slot.status in {"closed_profit", "closed_loss", "closed_manual"}:
                continue
            previous_status = slot.status
            slot.status = "stopped"
            slot.quota_occupied = False
            slot.closed_at = slot.closed_at or now
            slot.released_at = slot.released_at or now
            if previous_status == "open":
                slot.close_reason = "人工强制清格（未同步平仓结果）"
            else:
                slot.close_reason = slot.close_reason or "人工强制清格"
            cleared_slots += 1

        self._trader_desk_add_event(
            trader_id,
            f"已强制清理 {cleared_slots} 个额度格 | 已请求停止 {stop_requested} 个关联会话 | "
            "本地额度占用已释放，请人工确认交易所真实仓位/委托。",
            level="warning",
        )
        self._save_trader_desk_snapshot()

    def _signal_observer_session_rows(self) -> list[dict[str, str]]:
        rows: list[dict[str, str]] = []
        ordered = sorted(self.sessions.values(), key=lambda item: (item.started_at, item.session_id), reverse=True)
        for session in ordered:
            if session.config.run_mode != "signal_only":
                continue
            rows.append(
                {
                    "session_id": session.session_id,
                    "api_name": session.api_name or "-",
                    "strategy_name": session.strategy_name,
                    "symbol": session.symbol,
                    "status": session.display_status,
                    "last_message": session.last_message or "-",
                }
            )
        return rows

    def _stop_sessions_by_id(self, session_ids: list[str]) -> None:
        for session_id in session_ids:
            session = self.sessions.get(session_id)
            if session is None:
                continue
            if session.config.run_mode != "signal_only":
                self._enqueue_log(f"{session.log_prefix} 仅支持由信号观察台停止 signal_only 会话。")
                continue
            if session.stop_cleanup_in_progress:
                continue
            if not session.engine.is_running:
                continue
            session.stop_cleanup_in_progress = True
            session.status = "停止中"
            session.runtime_status = "停止中"
            session.ended_reason = "信号观察台手动停止"
            session.engine.stop()
            session.engine.wait_stopped(timeout=1.5)
            session.stop_cleanup_in_progress = False
            session.status = "已停止"
            session.runtime_status = "已停止"
            session.stopped_at = datetime.now()
            self._upsert_session_row(session)
            self._refresh_selected_session_details()
            self._sync_strategy_history_from_session(session)
            self._log_session_message(session, "信号观察台已停止该会话。")

    def _delete_signal_observer_sessions_by_id(self, session_ids: list[str]) -> tuple[int, list[str]]:
        tree = getattr(self, "session_tree", None)
        tree_exists = tree is not None and tree.winfo_exists()
        selected_before = tree.selection()[0] if tree_exists and tree.selection() else None
        deleted_ids: list[str] = []
        blocked_ids: list[str] = []
        for session_id in session_ids:
            session = self.sessions.get(session_id)
            if session is None:
                continue
            if session.config.run_mode != "signal_only" or not QuantApp._session_can_be_cleared(session):
                blocked_ids.append(session_id)
                continue
            self.sessions.pop(session_id, None)
            self._remove_recoverable_strategy_session(session_id)
            if tree_exists and tree.exists(session_id):
                tree.delete(session_id)
            deleted_ids.append(session_id)
        if deleted_ids:
            if tree_exists:
                remaining = tuple(tree.get_children())
                next_selection = QuantApp._next_session_selection_after_clear(selected_before, remaining)
                if next_selection is not None:
                    tree.selection_set(next_selection)
                    tree.focus(next_selection)
                    tree.see(next_selection)
            self._refresh_selected_session_details()
            self._refresh_running_session_summary()
        return len(deleted_ids), blocked_ids

    def _launch_strategy_template_record(
        self,
        record: StrategyTemplateRecord,
        *,
        source_label: str,
        ask_confirm: bool,
    ) -> str:
        definition = self._resolve_strategy_template_definition(record)
        resolved_api_name, _ = _resolve_import_api_profile(
            record.api_name,
            self._current_credential_profile(),
            set(self._credential_profiles.keys()),
        )
        target_api_name = resolved_api_name or self._current_credential_profile()
        credentials = self._credentials_for_profile_or_none(target_api_name)
        if credentials is None:
            raise ValueError(f"未找到 API 配置：{target_api_name}")
        config = record.config
        if config.run_mode == "signal_only" and not supports_signal_only(definition.strategy_id):
            raise ValueError(f"{definition.name} 当前不支持只发信号邮件模式。")
        if ask_confirm and not self._confirm_start(definition, config):
            raise ValueError("已取消启动。")
        notifier = self._build_notifier(config)
        self._save_credentials_now(silent=True)
        self._save_notification_settings_now(silent=True)
        return self._start_strategy_session(
            definition=definition,
            credentials=credentials,
            config=config,
            notifier=notifier,
            api_name=target_api_name,
            direction_label=_strategy_template_direction_label(
                definition.strategy_id,
                config,
                fallback=record.direction_label or definition.default_signal_label,
            ),
            run_mode_label=_reverse_lookup_label(RUN_MODE_OPTIONS, config.run_mode, record.run_mode_label),
            source_label=source_label,
            select_session=ask_confirm,
        )

    def _start_strategy_session(
        self,
        *,
        definition: StrategyDefinition,
        credentials: Credentials,
        config: StrategyConfig,
        notifier: EmailNotifier | None,
        api_name: str,
        direction_label: str,
        run_mode_label: str,
        source_label: str = "",
        select_session: bool = True,
        allow_duplicate_launch: bool = False,
        trader_id: str = "",
        trader_slot_id: str = "",
    ) -> str:
        if not allow_duplicate_launch:
            duplicate_session = self._find_duplicate_strategy_session(api_name=api_name, config=config)
            if duplicate_session is not None:
                if select_session:
                    self._focus_session_row(duplicate_session.session_id)
                raise ValueError(self._format_duplicate_launch_block_message(duplicate_session, imported=False))

        session_id = self._next_session_id()
        session_symbol = self._format_strategy_symbol_display(config.inst_id, config.trade_inst_id)
        session_started_at = datetime.now()
        session_log_path = strategy_session_log_file_path(
            started_at=session_started_at,
            session_id=session_id,
            strategy_name=definition.name,
            symbol=session_symbol,
            api_name=api_name,
        ).resolve()
        session_notifier = self._build_session_notifier(config, session_id) if notifier is not None else None
        engine = self._create_session_engine(
            strategy_id=definition.strategy_id,
            strategy_name=definition.name,
            session_id=session_id,
            symbol=session_symbol,
            api_name=api_name,
            log_file_path=session_log_path,
            notifier=session_notifier,
            direction_label=direction_label,
            run_mode_label=run_mode_label,
            trader_id=trader_id.strip(),
        )
        session = StrategySession(
            session_id=session_id,
            api_name=api_name,
            strategy_id=definition.strategy_id,
            strategy_name=definition.name,
            symbol=session_symbol,
            direction_label=direction_label,
            run_mode_label=run_mode_label,
            engine=engine,
            config=config,
            started_at=session_started_at,
            log_file_path=session_log_path,
            recovery_root_dir=session_log_path.parent,
            recovery_supported=self._strategy_session_supports_recovery(config),
            trader_id=trader_id.strip(),
            trader_slot_id=trader_slot_id.strip(),
        )

        self.sessions[session_id] = session
        self._upsert_session_row(session)
        try:
            engine.start(credentials, config)
        except Exception:
            self.sessions.pop(session_id, None)
            if self.session_tree.exists(session_id):
                self.session_tree.delete(session_id)
            raise
        self._record_strategy_session_started(session)
        if select_session:
            self.session_tree.selection_set(session_id)
            self.session_tree.focus(session_id)
        self._refresh_selected_session_details()
        if source_label:
            self._log_session_message(session, f"{source_label} 已提交启动请求。")
        else:
            self._log_session_message(session, "已提交启动请求。")
        return session_id

    def start(self) -> None:
        try:
            definition = self._selected_strategy_definition()
            credentials, config = self._collect_inputs(definition)
            notifier = self._build_notifier(config)
            if not self._confirm_start(definition, config):
                return

            self._save_credentials_now(silent=True)
            self._save_notification_settings_now(silent=True)
            api_name = credentials.profile_name or self._current_credential_profile()
            self._start_strategy_session(
                definition=definition,
                credentials=credentials,
                config=config,
                notifier=notifier,
                api_name=api_name,
                direction_label=self.signal_mode_label.get(),
                run_mode_label=self.run_mode_label.get(),
                select_session=True,
            )
        except Exception as exc:
            messagebox.showerror("启动失败", str(exc))

    def stop_selected_session(self) -> None:
        session = self._selected_session()
        if session is None:
            messagebox.showinfo("提示", "请先在右侧选择一个策略会话。")
            return
        self._request_stop_strategy_session(
            session.session_id,
            ended_reason="用户手动停止",
            source_label="用户手动停止",
            show_dialog=True,
        )

    def _request_stop_strategy_session(
        self,
        session_id: str,
        *,
        ended_reason: str,
        source_label: str,
        show_dialog: bool,
    ) -> bool:
        session = self.sessions.get(session_id)
        if session is None:
            return False
        if session.stop_cleanup_in_progress:
            if show_dialog:
                messagebox.showinfo("提示", "这个策略正在执行停止清理，请稍等。")
            return False
        if not session.engine.is_running:
            if show_dialog:
                messagebox.showinfo("提示", "这个策略已经停止了。")
            return False
        if session.config.run_mode == "signal_only":
            self._stop_sessions_by_id([session.session_id])
            return True

        credentials = self._credentials_for_profile_or_none(session.api_name)
        session.status = "停止中"
        session.stop_cleanup_in_progress = True
        session.stop_result_show_dialog = show_dialog
        session.ended_reason = ended_reason
        session.engine.stop()
        self._upsert_session_row(session)
        self._refresh_selected_session_details()
        self._sync_strategy_history_from_session(session)
        self._log_session_message(session, f"{source_label}，正在检查本策略委托与持仓。")
        if credentials is None:
            session.stop_cleanup_in_progress = False
            session.status = "已停止"
            session.stopped_at = datetime.now()
            session.ended_reason = f"{ended_reason}（未找到对应API凭证，未执行撤单检查）"
            self._remove_recoverable_strategy_session(session.session_id)
            self._upsert_session_row(session)
            self._refresh_selected_session_details()
            self._sync_strategy_history_from_session(session)
            self._log_session_message(session, "停止清理失败：未找到该会话对应的 API 凭证，请人工检查委托与仓位。")
            if show_dialog:
                messagebox.showwarning(
                    "停止提醒",
                    "策略线程已收到停止请求，但当前找不到该会话对应的 API 凭证。\n\n请人工检查：\n- 当前委托是否还有残留\n- 是否已经成交并留下仓位",
                )
            return False
        threading.Thread(
            target=self._stop_session_cleanup_worker,
            args=(session.session_id, credentials),
            daemon=True,
        ).start()
        return True

    def _credentials_for_profile_or_none(self, profile_name: str) -> Credentials | None:
        target = profile_name.strip() or self._current_credential_profile()
        current_profile = self._current_credential_profile()
        if target == current_profile:
            current_credentials = self._current_credentials_or_none()
            if current_credentials is not None:
                return Credentials(
                    api_key=current_credentials.api_key,
                    secret_key=current_credentials.secret_key,
                    passphrase=current_credentials.passphrase,
                    profile_name=target,
                )
        snapshot = self._credential_profiles.get(target)
        if not snapshot:
            return None
        api_key = str(snapshot.get("api_key", "")).strip()
        secret_key = str(snapshot.get("secret_key", "")).strip()
        passphrase = str(snapshot.get("passphrase", "")).strip()
        if not api_key or not secret_key or not passphrase:
            return None
        return Credentials(
            api_key=api_key,
            secret_key=secret_key,
            passphrase=passphrase,
            profile_name=target,
        )

    def _load_strategy_stop_cleanup_snapshot(
        self,
        session: StrategySession,
        credentials: Credentials,
        environment: str,
    ) -> StrategyStopCleanupSnapshot:
        pending_orders = self.client.get_pending_orders(credentials, environment=environment, limit=200)
        order_history = self.client.get_order_history(credentials, environment=environment, limit=200)
        positions = self.client.get_positions(credentials, environment=environment)
        return StrategyStopCleanupSnapshot(
            effective_environment=environment,
            pending_orders=pending_orders,
            order_history=order_history,
            positions=positions,
        )

    def _load_strategy_stop_cleanup_snapshot_with_fallback(
        self,
        session: StrategySession,
        credentials: Credentials,
    ) -> StrategyStopCleanupSnapshot:
        environment = session.config.environment
        try:
            return self._load_strategy_stop_cleanup_snapshot(session, credentials, environment)
        except Exception as exc:
            message = str(exc)
            if "50101" not in message or "current environment" not in message:
                raise
            alternate = "live" if environment == "demo" else "demo"
            snapshot = self._load_strategy_stop_cleanup_snapshot(session, credentials, alternate)
            snapshot.environment_note = f"停止检查自动切换到 {'实盘' if alternate == 'live' else '模拟'} 环境执行。"
            return snapshot

    def _perform_stop_session_cleanup(
        self,
        session: StrategySession,
        credentials: Credentials,
    ) -> StrategyStopCleanupResult:
        initial_snapshot = self._load_strategy_stop_cleanup_snapshot_with_fallback(session, credentials)
        effective_environment = initial_snapshot.effective_environment
        initial_pending = [item for item in initial_snapshot.pending_orders if _trade_order_belongs_to_session(item, session)]
        cancelable_pending = [
            item for item in initial_pending if _trade_order_session_role(item, session) in {"ent", "exi"}
        ]

        cancel_requested_summaries: list[str] = []
        cancel_failed_summaries: list[str] = []
        for item in cancelable_pending:
            try:
                result = self._cancel_pending_order_request(credentials, environment=effective_environment, item=item)
                cancel_requested_summaries.append(
                    f"{_trade_order_cancel_summary(item)} | sCode={result.s_code} | sMsg={result.s_msg or 'accepted'}"
                )
            except Exception as exc:
                cancel_failed_summaries.append(
                    f"{_trade_order_cancel_summary(item)} | 原因={_format_network_error_message(str(exc))}"
                )

        final_snapshot = initial_snapshot
        wait_rounds = 3 if cancelable_pending else 1
        for attempt in range(wait_rounds):
            if attempt > 0:
                threading.Event().wait(0.6)
            final_snapshot = self._load_strategy_stop_cleanup_snapshot(session, credentials, effective_environment)
            remaining_cancelable = [
                item
                for item in final_snapshot.pending_orders
                if _trade_order_session_role(item, session) in {"ent", "exi"}
            ]
            if not remaining_cancelable:
                break

        for _ in range(20):
            if not session.engine.is_running:
                break
            threading.Event().wait(0.25)

        remaining_cancelable = [
            item
            for item in final_snapshot.pending_orders
            if _trade_order_session_role(item, session) in {"ent", "exi"}
        ]
        protective_pending = [
            item
            for item in final_snapshot.pending_orders
            if _trade_order_session_role(item, session) == "slg"
        ]
        session_history = [item for item in final_snapshot.order_history if _trade_order_belongs_to_session(item, session)]
        filled_orders = [
            item
            for item in session_history
            if (item.filled_size or Decimal("0")) > 0 or (item.state or "").strip().lower() in {"filled", "partially_filled"}
        ]
        trade_inst_id = (session.config.trade_inst_id or session.config.inst_id or "").strip().upper()
        open_positions = [
            position
            for position in final_snapshot.positions
            if position.inst_id.strip().upper() == trade_inst_id and position.position != 0
        ]

        needs_manual_review = bool(cancel_failed_summaries or remaining_cancelable)
        if open_positions and (filled_orders or protective_pending):
            needs_manual_review = True
        if protective_pending and not open_positions:
            needs_manual_review = True

        final_reason_parts: list[str] = []
        if cancel_failed_summaries or remaining_cancelable:
            final_reason_parts.append("撤单未完全确认，需人工检查")
        if open_positions and filled_orders:
            final_reason_parts.append("检测到已成交仓位，需人工判断")
        elif protective_pending:
            final_reason_parts.append("检测到保护委托，需人工确认")
        final_reason = "用户手动停止"
        if final_reason_parts:
            final_reason = f"用户手动停止（{'；'.join(final_reason_parts)}）"

        return StrategyStopCleanupResult(
            session_id=session.session_id,
            effective_environment=effective_environment,
            environment_note=initial_snapshot.environment_note,
            cancel_requested_summaries=tuple(cancel_requested_summaries),
            cancel_failed_summaries=tuple(cancel_failed_summaries),
            remaining_pending_summaries=tuple(_trade_order_cancel_summary(item) for item in remaining_cancelable),
            protective_pending_summaries=tuple(_trade_order_cancel_summary(item) for item in protective_pending),
            filled_order_summaries=tuple(_trade_order_fill_summary(item) for item in filled_orders),
            open_position_summaries=tuple(_position_manual_review_summary(item) for item in open_positions),
            needs_manual_review=needs_manual_review,
            final_reason=final_reason,
        )

    def _stop_session_cleanup_worker(self, session_id: str, credentials: Credentials) -> None:
        session = self.sessions.get(session_id)
        if session is None:
            return
        try:
            result = self._perform_stop_session_cleanup(session, credentials)
        except Exception as exc:
            self.root.after(
                0,
                lambda sid=session_id, msg=str(exc): self._apply_stop_session_cleanup_error(sid, msg),
            )
            return
        self.root.after(0, lambda: self._apply_stop_session_cleanup_result(result))

    def _apply_stop_session_cleanup_result(self, result: StrategyStopCleanupResult) -> None:
        session = self.sessions.get(result.session_id)
        if session is None:
            return
        show_dialog = bool(getattr(session, "stop_result_show_dialog", True))
        session.stop_result_show_dialog = True
        session.stop_cleanup_in_progress = False
        session.status = "已停止"
        if session.stopped_at is None:
            session.stopped_at = datetime.now()
        session.ended_reason = result.final_reason
        self._remove_recoverable_strategy_session(session.session_id)
        self._upsert_session_row(session)
        self._refresh_selected_session_details()
        self._sync_strategy_history_from_session(session)

        if result.environment_note:
            self._log_session_message(session, result.environment_note)
        if result.cancel_requested_summaries:
            self._log_session_message(session, f"停止清理：已提交撤单 {len(result.cancel_requested_summaries)} 条。")
            for summary in result.cancel_requested_summaries:
                self._log_session_message(session, f"停止清理 | 已提交撤单 | {summary}")
        else:
            self._log_session_message(session, "停止清理：未发现需要自动撤销的程序挂单。")
        for summary in result.cancel_failed_summaries:
            self._log_session_message(session, f"停止清理 | 撤单失败 | {summary}")
        for summary in result.remaining_pending_summaries:
            self._log_session_message(session, f"停止清理 | 残留未撤委托 | {summary}")
        for summary in result.filled_order_summaries:
            self._log_session_message(session, f"停止清理 | 检测到已成交委托 | {summary}")
        for summary in result.open_position_summaries:
            self._log_session_message(session, f"停止清理 | 检测到仍有仓位 | {summary}")
        for summary in result.protective_pending_summaries:
            self._log_session_message(session, f"停止清理 | 检测到保护委托 | {summary}")
        self._log_session_message(session, f"停止流程结束 | 结论={result.final_reason}")

        if session.api_name.strip() == self._current_credential_profile():
            self.refresh_positions()
            self.refresh_order_views()

        if result.needs_manual_review:
            details: list[str] = ["策略已停止，但检测到需要人工接管的情况。"]
            if result.cancel_failed_summaries or result.remaining_pending_summaries:
                details.append("")
                details.append("委托检查：")
                details.append(
                    f"- 撤单失败 {len(result.cancel_failed_summaries)} 条，残留未确认撤销 {len(result.remaining_pending_summaries)} 条"
                )
            if result.filled_order_summaries and result.open_position_summaries:
                details.append("")
                details.append("已成交且仍有仓位：")
                for summary in result.open_position_summaries[:3]:
                    details.append(f"- {summary}")
            if result.protective_pending_summaries:
                details.append("")
                details.append("保护委托仍在交易所：")
                for summary in result.protective_pending_summaries[:3]:
                    details.append(f"- {summary}")
            details.append("")
            details.append("请人工检查“当前委托 / 账户持仓 / OKX 托管止损”，再决定是否保留、撤单或平仓。")
            if show_dialog:
                messagebox.showwarning("停止提醒", "\n".join(details))
            return

        if show_dialog:
            messagebox.showinfo(
                "停止结果",
                (
                    "策略已停止。\n\n"
                    f"自动撤单：{len(result.cancel_requested_summaries)} 条\n"
                    "未发现残留仓位或需人工接管的问题。"
                ),
            )

    def _apply_stop_session_cleanup_error(self, session_id: str, message: str) -> None:
        session = self.sessions.get(session_id)
        if session is None:
            return
        show_dialog = bool(getattr(session, "stop_result_show_dialog", True))
        session.stop_result_show_dialog = True
        session.stop_cleanup_in_progress = False
        session.status = "已停止"
        if session.stopped_at is None:
            session.stopped_at = datetime.now()
        friendly_message = _format_network_error_message(message)
        session.ended_reason = "用户手动停止（停止清理失败，需人工检查）"
        self._remove_recoverable_strategy_session(session.session_id)
        self._upsert_session_row(session)
        self._refresh_selected_session_details()
        self._sync_strategy_history_from_session(session)
        self._log_session_message(session, f"停止清理失败：{friendly_message}")
        self._log_session_message(session, "请人工检查当前委托、历史委托与账户持仓。")
        if show_dialog:
            messagebox.showwarning(
                "停止提醒",
                "策略线程已收到停止请求，但停止清理阶段失败。\n\n"
                f"原因：{friendly_message}\n\n"
                "请人工检查：\n- 当前委托是否仍有残留\n- 是否已经成交并留下仓位\n- OKX 托管止损是否仍在",
            )

    @staticmethod
    def _session_can_be_cleared(session: StrategySession) -> bool:
        return session.status == "已停止" and not session.engine.is_running

    @staticmethod
    def _next_session_selection_after_clear(
        selected_before: str | None,
        remaining_session_ids: tuple[str, ...] | list[str],
    ) -> str | None:
        if selected_before and selected_before in remaining_session_ids:
            return selected_before
        return remaining_session_ids[0] if remaining_session_ids else None

    @staticmethod
    def _next_history_selection_after_mutation(
        selected_before: str | None,
        remaining_record_ids: tuple[str, ...] | list[str],
    ) -> str | None:
        if selected_before and selected_before in remaining_record_ids:
            return selected_before
        return remaining_record_ids[0] if remaining_record_ids else None

    def clear_stopped_sessions(self) -> None:
        stopped_ids = [
            session_id
            for session_id, session in self.sessions.items()
            if self._session_can_be_cleared(session)
        ]
        if not stopped_ids:
            messagebox.showinfo("提示", "当前没有可清空的已停止策略。")
            return

        confirmed = messagebox.askyesno(
            "确认清空",
            f"确认从运行中策略列表清空 {len(stopped_ids)} 条已停止会话吗？\n\n历史策略记录和独立日志会保留。",
            parent=self.root,
        )
        if not confirmed:
            return

        tree = self.session_tree
        selected_before = tree.selection()[0] if tree.selection() else None
        for session_id in stopped_ids:
            self.sessions.pop(session_id, None)
            if tree.exists(session_id):
                tree.delete(session_id)

        remaining = tuple(tree.get_children())
        next_selection = self._next_session_selection_after_clear(selected_before, remaining)
        if next_selection is not None:
            tree.selection_set(next_selection)
            tree.focus(next_selection)
            tree.see(next_selection)
        self._refresh_selected_session_details()
        self._enqueue_log(f"已从运行中策略列表清空 {len(stopped_ids)} 条已停止会话；历史策略记录保留。")

    def debug_hourly_values(self) -> None:
        symbol = _normalize_symbol_input(self.symbol.get())
        if not symbol:
            messagebox.showerror("提示", "请先选择交易标的")
            return
        ema_period = self._parse_positive_int(self.ema_period.get(), "EMA小周期")
        trend_ema_period = self._parse_positive_int(self.trend_ema_period.get(), "EMA中周期")
        strategy_id = self._selected_strategy_definition().strategy_id
        entry_reference_ema_period = 0
        if strategy_uses_parameter(strategy_id, "entry_reference_ema_period"):
            entry_reference_ema_period = self._parse_nonnegative_int(
                self.entry_reference_ema_period.get(),
                self._entry_reference_ema_caption(strategy_id),
            )
        if entry_reference_ema_period <= 0:
            entry_reference_ema_period = ema_period
        self._enqueue_log(
            f"正在获取 {symbol} 的 1 小时调试值，EMA小周期={ema_period}，趋势EMA={trend_ema_period}，"
            f"{self._entry_reference_ema_caption(strategy_id)}={entry_reference_ema_period} ..."
        )
        threading.Thread(
            target=self._debug_hourly_values_worker,
            args=(symbol, ema_period, trend_ema_period, entry_reference_ema_period),
            daemon=True,
        ).start()

    def _debug_hourly_values_worker(
        self,
        symbol: str,
        ema_period: int,
        trend_ema_period: int,
        entry_reference_ema_period: int,
    ) -> None:
        try:
            snapshot = fetch_hourly_ema_debug(
                self.client,
                symbol,
                ema_period=ema_period,
                trend_ema_period=trend_ema_period,
                entry_reference_ema_period=entry_reference_ema_period,
            )
            self._enqueue_log(format_hourly_debug(symbol, snapshot))
        except Exception as exc:
            self._enqueue_log(f"获取 1 小时调试值失败：{exc}")

    def _on_strategy_selected(self, *_: object) -> None:
        self._apply_selected_strategy_definition()

    def _apply_selected_strategy_definition(self) -> None:
        definition = self._selected_strategy_definition()
        strategy_id = definition.strategy_id
        previous_strategy_id = self._last_strategy_parameter_strategy_id
        if previous_strategy_id and previous_strategy_id != strategy_id:
            self._save_strategy_parameter_draft(previous_strategy_id)
        self._restore_strategy_parameter_draft(strategy_id)
        dynamic_strategy = is_dynamic_strategy_id(strategy_id)
        breakout_strategy = is_ema_atr_breakout_strategy(strategy_id)
        dynamic_tp_controls = dynamic_strategy or breakout_strategy
        self.signal_combo["values"] = definition.allowed_signal_labels
        fixed_signal_mode = strategy_fixed_value(strategy_id, "signal_mode")
        if fixed_signal_mode is not None:
            self.signal_mode_label.set(_reverse_lookup_label(SIGNAL_LABEL_TO_VALUE, str(fixed_signal_mode), definition.default_signal_label))
        elif self.signal_mode_label.get() not in definition.allowed_signal_labels:
            self.signal_mode_label.set(definition.default_signal_label)
        if strategy_id == STRATEGY_EMA5_EMA8_ID:
            self.entry_reference_ema_period.set("0")
            self.risk_amount.set("10")
            self.take_profit_mode_label.set("固定止盈")
            self.max_entries_per_trend.set("0")
            self.entry_side_mode_label.set("跟随信号")
            self.tp_sl_mode_label.set("按交易标的价格（本地）")
        if dynamic_tp_controls:
            self._take_profit_mode_label.grid()
            self._take_profit_mode_combo.grid()
            self._max_entries_per_trend_label.grid()
            self._max_entries_per_trend_entry.grid()
            self._dynamic_two_r_break_even_check.grid()
            self._dynamic_fee_offset_check.grid()
            self._dynamic_fee_offset_hint_label.grid()
            self._time_stop_break_even_check.grid()
            self._time_stop_break_even_bars_label.grid()
            self._time_stop_break_even_bars_entry.grid()
        else:
            self._take_profit_mode_label.grid_remove()
            self._take_profit_mode_combo.grid_remove()
            self._max_entries_per_trend_label.grid_remove()
            self._max_entries_per_trend_entry.grid_remove()
            self._dynamic_two_r_break_even_check.grid_remove()
            self._dynamic_fee_offset_check.grid_remove()
            self._dynamic_fee_offset_hint_label.grid_remove()
            self._time_stop_break_even_check.grid_remove()
            self._time_stop_break_even_bars_label.grid_remove()
            self._time_stop_break_even_bars_entry.grid_remove()
        if dynamic_strategy or breakout_strategy:
            self._startup_chase_window_label.grid()
            self._startup_chase_window_entry.grid()
            self._startup_chase_window_hint_label.grid()
        else:
            self._startup_chase_window_label.grid_remove()
            self._startup_chase_window_entry.grid_remove()
            self._startup_chase_window_hint_label.grid_remove()
        if strategy_uses_parameter(strategy_id, "entry_reference_ema_period"):
            self._entry_reference_ema_label.configure(text=self._entry_reference_ema_caption(strategy_id))
            self._entry_reference_ema_label.grid()
            self._entry_reference_ema_entry.grid()
        else:
            self._entry_reference_ema_label.grid_remove()
            self._entry_reference_ema_entry.grid_remove()
        if self._strategy_uses_big_ema(strategy_id):
            self._big_ema_label.grid()
            self._big_ema_entry.grid()
        else:
            self._big_ema_label.grid_remove()
            self._big_ema_entry.grid_remove()
        self._set_field_state(self._bar_combo, editable=strategy_is_parameter_editable(strategy_id, "bar", "launcher"))
        self._set_field_state(self._ema_entry, editable=strategy_is_parameter_editable(strategy_id, "ema_period", "launcher"))
        self._set_field_state(self._trend_ema_entry, editable=strategy_is_parameter_editable(strategy_id, "trend_ema_period", "launcher"))
        self._set_field_state(self._big_ema_entry, editable=strategy_is_parameter_editable(strategy_id, "big_ema_period", "launcher"))
        self._set_field_state(self.signal_combo, editable=strategy_is_parameter_editable(strategy_id, "signal_mode", "launcher"))
        self._apply_strategy_parameter_fixed_labels(strategy_id)
        if strategy_uses_parameter(strategy_id, "entry_reference_ema_period") and not self.entry_reference_ema_period.get().strip():
            self.entry_reference_ema_period.set("55")
        if (dynamic_strategy or breakout_strategy) and not self.startup_chase_window_seconds.get().strip():
            self.startup_chase_window_seconds.set("0")
        self._last_strategy_parameter_strategy_id = strategy_id
        self._sync_dynamic_take_profit_controls()
        QuantApp._sync_entry_side_mode_controls(self)
        self.strategy_summary_text.set(definition.summary)
        self.strategy_rule_text.set(definition.rule_description)
        self.strategy_hint_text.set(definition.parameter_hint)
        self._update_trend_parameter_hint()
        self._update_launch_parameter_hint()
        self._update_dynamic_protection_hint()
        self._on_fixed_order_size_symbol_changed()
        self._schedule_minimum_order_risk_hint_update()

    def _sync_dynamic_take_profit_controls(self) -> None:
        if not hasattr(self, "_dynamic_two_r_break_even_check"):
            return
        definition = self._selected_strategy_definition()
        dynamic_tp_eligible = is_dynamic_strategy_id(definition.strategy_id) or is_ema_atr_breakout_strategy(
            definition.strategy_id
        )
        dynamic_take_profit = (
            dynamic_tp_eligible and TAKE_PROFIT_MODE_OPTIONS.get(self.take_profit_mode_label.get(), "fixed") == "dynamic"
        )
        self._dynamic_two_r_break_even_check.configure(state="normal" if dynamic_take_profit else "disabled")
        self._dynamic_fee_offset_check.configure(state="normal" if dynamic_take_profit else "disabled")
        self._time_stop_break_even_check.configure(state="normal" if dynamic_take_profit else "disabled")
        self._time_stop_break_even_bars_label.configure(state="normal" if dynamic_take_profit else "disabled")
        self._time_stop_break_even_bars_entry.configure(
            state="normal" if dynamic_take_profit and self.time_stop_break_even_enabled.get() else "disabled"
        )

    def _sync_entry_side_mode_controls(self) -> None:
        if not hasattr(self, "_entry_side_mode_combo"):
            return
        definition = self._selected_strategy_definition()
        run_mode = RUN_MODE_OPTIONS.get(self.run_mode_label.get(), "trade")
        tp_sl_mode = TP_SL_MODE_OPTIONS.get(self.tp_sl_mode_label.get(), "exchange")
        if supports_fixed_entry_side_mode(definition.strategy_id, run_mode, tp_sl_mode):
            self._entry_side_mode_combo.configure(values=list(ENTRY_SIDE_MODE_OPTIONS.keys()), state="readonly")
            self.entry_side_mode_hint_text.set("当前模式支持跟随信号、固定买入、固定卖出。")
            if self.entry_side_mode_label.get() not in ENTRY_SIDE_MODE_OPTIONS:
                self.entry_side_mode_label.set("跟随信号")
            return
        self._entry_side_mode_combo.configure(values=("跟随信号",), state="disabled")
        if self.entry_side_mode_label.get() != "跟随信号":
            self.entry_side_mode_label.set("跟随信号")
        self.entry_side_mode_hint_text.set(
            fixed_entry_side_mode_support_reason(definition.strategy_id, run_mode, tp_sl_mode) or "当前模式仅支持跟随信号。"
        )

    def _selected_strategy_definition(self) -> StrategyDefinition:
        strategy_id = self._strategy_name_to_id[self.strategy_name.get()]
        return get_strategy_definition(strategy_id)

    def _confirm_start(self, definition: StrategyDefinition, config: StrategyConfig) -> bool:
        strategy_symbol = self._format_strategy_symbol_display(config.inst_id, config.trade_inst_id)
        risk_value = self.risk_amount.get().strip() or "-"
        fixed_size = self.order_size.get().strip() or "-"
        instrument = self._find_instrument_for_fixed_order_size_hint(
            _normalize_symbol_input(config.trade_inst_id or config.inst_id),
        )
        message = _build_strategy_start_confirmation_message(
            strategy_name=definition.name,
            rule_description=definition.rule_description,
            strategy_symbol=strategy_symbol,
            config=config,
            run_mode_label=self.run_mode_label.get(),
            environment_label=self.environment_label.get(),
            trade_mode_label=self.trade_mode_label.get(),
            position_mode_label=self.position_mode_label.get(),
            signal_mode_label=self.signal_mode_label.get(),
            entry_side_mode_label=self.entry_side_mode_label.get(),
            tp_sl_mode_label=self.tp_sl_mode_label.get(),
            trigger_type_label=self.trigger_type_label.get(),
            take_profit_mode_label=self.take_profit_mode_label.get(),
            risk_value=risk_value,
            fixed_size=fixed_size,
            custom_trigger_symbol=self.local_tp_sl_symbol.get().strip().upper(),
            instrument=instrument,
            api_label=self._current_credential_profile(),
        )
        return messagebox.askokcancel(f"确认启动 {definition.name}", message)

    def _collect_inputs(self, definition: StrategyDefinition) -> tuple[Credentials, StrategyConfig]:
        api_key = self.api_key.get().strip()
        secret_key = self.secret_key.get().strip()
        passphrase = self.passphrase.get().strip()
        symbol = _normalize_symbol_input(self.symbol.get())
        trade_symbol = symbol
        local_tp_sl_symbol = _normalize_symbol_input(self.local_tp_sl_symbol.get()) or None
        tp_sl_mode = TP_SL_MODE_OPTIONS[self.tp_sl_mode_label.get()]
        run_mode = RUN_MODE_OPTIONS[self.run_mode_label.get()]
        entry_side_mode = ENTRY_SIDE_MODE_OPTIONS[self.entry_side_mode_label.get()]
        if not supports_fixed_entry_side_mode(definition.strategy_id, run_mode, tp_sl_mode):
            entry_side_mode = "follow_signal"
        strategy_id = definition.strategy_id
        effective_signal_mode = resolve_dynamic_signal_mode(
            strategy_id,
            SIGNAL_LABEL_TO_VALUE[self.signal_mode_label.get()],
        )
        fixed_signal_mode = strategy_fixed_value(strategy_id, "signal_mode")
        if fixed_signal_mode is not None:
            effective_signal_mode = str(fixed_signal_mode)
        risk_amount = self._parse_optional_positive_decimal(self.risk_amount.get(), "风险金")
        order_size = self._parse_optional_positive_decimal(self.order_size.get(), "固定数量") or Decimal("0")
        max_entries_per_trend = self._parse_nonnegative_int(self.max_entries_per_trend.get(), "每波最多开仓次数")
        startup_chase_window_seconds = 0
        entry_reference_ema_period = 0
        if strategy_uses_parameter(strategy_id, "entry_reference_ema_period"):
            entry_reference_ema_period = self._parse_nonnegative_int(
                self.entry_reference_ema_period.get(),
                self._entry_reference_ema_caption(strategy_id),
            )
        if is_dynamic_strategy_id(strategy_id) or is_ema_atr_breakout_strategy(strategy_id):
            startup_chase_window_seconds = parse_nonnegative_duration_seconds(
                self.startup_chase_window_seconds.get(),
                field_name="启动追单窗口",
            )

        if not api_key or not secret_key or not passphrase:
            raise ValueError("请先在 菜单 > 设置 > API 与通知设置 中填写 API 凭证")
        if not symbol:
            raise ValueError("请选择交易标的")
        if run_mode == "trade":
            if tp_sl_mode == "exchange":
                if trade_symbol != symbol:
                    raise ValueError("OKX 托管止盈止损只支持同一交易标的")
                if infer_inst_type(trade_symbol) != "SWAP":
                    raise ValueError("OKX 托管止盈止损当前只支持永续合约")
            if tp_sl_mode == "local_custom" and not local_tp_sl_symbol:
                raise ValueError("已选择自定义本地止盈止损，请填写触发标的")
            if risk_amount is None and order_size <= 0:
                raise ValueError("交易并下单模式下，风险金和固定数量至少填写一个")

        notification_config = self._collect_notification_config(validate_if_enabled=True)
        if run_mode == "signal_only":
            if not notification_config.enabled:
                raise ValueError("只发信号邮件模式需要先在设置里启用邮件通知")
            if not notification_config.notify_signals:
                raise ValueError("只发信号邮件模式需要勾选“信号邮件”")

        if strategy_id == STRATEGY_EMA5_EMA8_ID:
            trade_symbol = symbol
            local_tp_sl_symbol = symbol
            tp_sl_mode = "local_trade"
            risk_amount = Decimal("10")
            order_size = Decimal("0")

        credentials = Credentials(
            api_key=api_key,
            secret_key=secret_key,
            passphrase=passphrase,
            profile_name=self._current_credential_profile(),
        )
        config = StrategyConfig(
            inst_id=symbol,
            bar=str(self._resolve_strategy_parameter_value(strategy_id, "bar", self.bar.get())),
            ema_period=int(
                self._resolve_strategy_parameter_value(
                    strategy_id,
                    "ema_period",
                    self._parse_positive_int(self.ema_period.get(), "EMA小周期"),
                )
            ),
            trend_ema_period=int(
                self._resolve_strategy_parameter_value(
                    strategy_id,
                    "trend_ema_period",
                    self._parse_positive_int(self.trend_ema_period.get(), "EMA中周期"),
                )
            ),
            big_ema_period=int(
                self._resolve_strategy_parameter_value(
                    strategy_id,
                    "big_ema_period",
                    self._parse_positive_int(self.big_ema_period.get(), "EMA大周期"),
                )
            ),
            entry_reference_ema_period=entry_reference_ema_period,
            atr_period=self._parse_positive_int(self.atr_period.get(), "ATR 周期"),
            atr_stop_multiplier=self._parse_positive_decimal(self.stop_atr.get(), "止损 ATR 倍数"),
            atr_take_multiplier=self._parse_positive_decimal(self.take_atr.get(), "止盈 ATR 倍数"),
            order_size=order_size,
            trade_mode=TRADE_MODE_OPTIONS[self.trade_mode_label.get()],
            signal_mode=effective_signal_mode,
            position_mode=POSITION_MODE_OPTIONS[self.position_mode_label.get()],
            environment=ENV_OPTIONS[self.environment_label.get()],
            tp_sl_trigger_type=TRIGGER_TYPE_OPTIONS[self.trigger_type_label.get()],
            strategy_id=strategy_id,
            poll_seconds=float(self._parse_positive_decimal(self.poll_seconds.get(), "轮询秒数")),
            risk_amount=risk_amount,
            trade_inst_id=trade_symbol,
            tp_sl_mode=tp_sl_mode,
            local_tp_sl_inst_id=local_tp_sl_symbol,
            entry_side_mode=entry_side_mode,
            run_mode=run_mode,
            take_profit_mode=TAKE_PROFIT_MODE_OPTIONS[self.take_profit_mode_label.get()],
            max_entries_per_trend=max_entries_per_trend,
            startup_chase_window_seconds=startup_chase_window_seconds if is_dynamic_strategy_id(strategy_id) else 0,
            dynamic_two_r_break_even=self.dynamic_two_r_break_even.get()
            if is_dynamic_strategy_id(strategy_id)
            else False,
            dynamic_fee_offset_enabled=self.dynamic_fee_offset_enabled.get()
            if is_dynamic_strategy_id(strategy_id)
            else False,
            time_stop_break_even_enabled=self.time_stop_break_even_enabled.get()
            if is_dynamic_strategy_id(strategy_id)
            else False,
            time_stop_break_even_bars=(
                self._parse_positive_int(self.time_stop_break_even_bars.get(), "时间保本K线数")
                if is_dynamic_strategy_id(strategy_id) and self.time_stop_break_even_enabled.get()
                else 0
            ),
        )
        return credentials, config

    def _refresh_global_email_toggle_text(self) -> None:
        self.global_email_toggle_text.set("发邮件：开" if self.notify_enabled.get() else "发邮件：关")

    def _session_email_runtime_enabled(self, session_id: str, kind: str) -> bool:
        if not self.notify_enabled.get():
            return False
        if kind == "signal" and not self.notify_signals.get():
            return False
        if kind == "trade_fill" and not self.notify_trade_fills.get():
            return False
        if kind == "error" and not self.notify_errors.get():
            return False
        session = self.sessions.get(session_id)
        if session is None:
            return False
        return bool(getattr(session, "email_notifications_enabled", True))

    def _session_email_status_label(self, session: StrategySession) -> str:
        if not self.notify_enabled.get():
            return "总关"
        return "开" if bool(getattr(session, "email_notifications_enabled", True)) else "关"

    def toggle_global_email_notifications(self) -> None:
        enabled = not self.notify_enabled.get()
        self.notify_enabled.set(enabled)
        self._refresh_global_email_toggle_text()
        self._refresh_running_session_tree()
        self._refresh_selected_session_details()
        self._save_notification_settings_now(silent=True)
        self._enqueue_log(f"已{'开启' if enabled else '关闭'}全局发邮件。")

    def _set_selected_session_email_notifications(self, enabled: bool) -> None:
        session = self._selected_session()
        if session is None:
            messagebox.showinfo("提示", "请先选择一个运行中策略。", parent=self.root)
            return
        current = bool(getattr(session, "email_notifications_enabled", True))
        if current == enabled:
            status_text = "开启" if enabled else "关闭"
            self._enqueue_log(f"会话 {session.session_id} 发邮件已是{status_text}状态。")
            return
        session.email_notifications_enabled = enabled
        self._upsert_session_row(session)
        self._refresh_selected_session_details()
        self._enqueue_log(f"已{'开启' if enabled else '关闭'}会话 {session.session_id} 发邮件。")

    def enable_selected_session_email_notifications(self) -> None:
        self._set_selected_session_email_notifications(True)

    def disable_selected_session_email_notifications(self) -> None:
        self._set_selected_session_email_notifications(False)

    def _build_session_notifier(self, config: StrategyConfig, session_id: str) -> EmailNotifier | None:
        notification_config = self._collect_notification_config(validate_if_enabled=True)
        if not notification_config.enabled:
            return None
        return EmailNotifier(
            notification_config,
            logger=self._make_system_logger(f"邮件 {config.strategy_id}"),
            delivery_policy=lambda kind, sid=session_id: self._session_email_runtime_enabled(sid, kind),
        )

    def _collect_notification_config(self, *, validate_if_enabled: bool) -> EmailNotificationConfig:
        smtp_port = self._parse_optional_port(self.smtp_port.get())
        recipients = tuple(self._split_recipients(self.recipient_emails.get()))
        config = EmailNotificationConfig(
            enabled=self.notify_enabled.get(),
            smtp_host=self.smtp_host.get().strip(),
            smtp_port=smtp_port,
            smtp_username=self.smtp_username.get().strip(),
            smtp_password=self.smtp_password.get(),
            sender_email=self.sender_email.get().strip(),
            recipient_emails=recipients,
            use_ssl=self.use_ssl.get(),
            notify_trade_fills=self.notify_trade_fills.get(),
            notify_signals=self.notify_signals.get(),
            notify_errors=self.notify_errors.get(),
        )
        if validate_if_enabled and config.enabled:
            if not config.smtp_host:
                raise ValueError("已启用邮件通知，请填写 SMTP 主机")
            if not recipients:
                raise ValueError("已启用邮件通知，请填写至少一个收件邮箱")
            if not (config.sender_email or config.smtp_username):
                raise ValueError("已启用邮件通知，请填写发件邮箱或 SMTP 用户名")
        return config

    def _build_notifier(self, config: StrategyConfig) -> EmailNotifier | None:
        notification_config = self._collect_notification_config(validate_if_enabled=True)
        if not notification_config.enabled:
            return None
        return EmailNotifier(
            notification_config,
            logger=self._make_system_logger(f"邮件 {config.strategy_id}"),
        )

    def _build_signal_monitor_notifier(self) -> EmailNotifier | None:
        notification_config = self._collect_notification_config(validate_if_enabled=True)
        if not notification_config.enabled:
            return None
        return EmailNotifier(notification_config, logger=self._make_system_logger("邮件 信号监控"))

    def send_test_email(self) -> None:
        try:
            notifier = self._build_signal_monitor_notifier()
        except Exception as exc:
            messagebox.showerror("测试邮件失败", str(exc), parent=self._settings_window or self.root)
            return

        if notifier is None:
            messagebox.showinfo("提示", "当前未启用邮件通知。", parent=self._settings_window or self.root)
            return

        subject = f"[QQOKX] 测试邮件 | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        body = "\n".join(
            [
                "这是一封来自 QQOKX 的测试邮件。",
                f"当前环境：{self.environment_label.get()}",
                f"发送时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            ]
        )
        notifier.send_signal(
            strategy_name="测试邮件",
            config=StrategyConfig(
                inst_id=self.symbol.get().strip().upper() or "-",
                bar=self.bar.get().strip() or "-",
                ema_period=0,
                atr_period=0,
                atr_stop_multiplier=Decimal("0"),
                atr_take_multiplier=Decimal("0"),
                order_size=Decimal("0"),
                trade_mode=RUN_MODE_OPTIONS[self.run_mode_label.get()],
                signal_mode=SIGNAL_LABEL_TO_VALUE[self.signal_mode_label.get()],
                position_mode=POSITION_MODE_OPTIONS[self.position_mode_label.get()],
                environment=ENV_OPTIONS[self.environment_label.get()],
                tp_sl_trigger_type=TRIGGER_TYPE_OPTIONS[self.trigger_type_label.get()],
            ),
            signal="long",
            trigger_symbol=self.symbol.get().strip().upper() or "-",
            entry_reference="-",
            reason="仅用于验证邮件通道",
            api_name=self._current_credential_profile(),
            session_id="TEST",
            trader_id="",
            direction_label=self.direction_label.get() if hasattr(self, "direction_label") else "",
            run_mode_label=self.run_mode_label.get(),
        )
        self._enqueue_log("已提交测试邮件发送请求。")
        messagebox.showinfo("提示", "测试邮件已提交，请检查收件箱。", parent=self._settings_window or self.root)

    def _update_settings_summary(self) -> None:
        api_status = "已配置" if all(
            [self.api_key.get().strip(), self.secret_key.get().strip(), self.passphrase.get().strip()]
        ) else "未配置"
        mail_status = "邮件已启用" if self.notify_enabled.get() else "邮件未启用"
        self.settings_summary_text.set(
            f"{api_status} | {mail_status} | {self.environment_label.get()} | {self.trade_mode_label.get()} | "
            f"{self.position_mode_label.get()}"
        )

    def _append_logged_message(
        self,
        message: str,
        *,
        extra_log_path: Path | None = None,
        extra_log_owner: str = "",
    ) -> None:
        line = append_log_line(message)
        if extra_log_path is not None:
            try:
                append_preformatted_log_line(line, path=extra_log_path)
            except Exception as exc:
                failure_key = str(extra_log_path)
                if failure_key not in self._strategy_log_write_failures:
                    self._strategy_log_write_failures.add(failure_key)
                    owner_prefix = f"{extra_log_owner} " if extra_log_owner else ""
                    self._enqueue_log(f"{owner_prefix}独立日志写入失败：{exc}")
        self.log_queue.put(line)

    def _log_session_message(self, session: StrategySession, message: str) -> None:
        self._record_session_runtime_message(session.session_id, message)
        self._append_logged_message(
            f"{session.log_prefix} {message}",
            extra_log_path=session.log_file_path,
            extra_log_owner=session.log_prefix,
        )

    def _make_session_logger(
        self,
        session_id: str,
        strategy_name: str,
        symbol: str,
        api_name: str = "",
        log_file_path: Path | None = None,
    ):
        prefix = f"[{api_name}] [{session_id} {strategy_name} {symbol}]" if api_name else f"[{session_id} {strategy_name} {symbol}]"

        def _logger(message: str) -> None:
            self._record_session_runtime_message(session_id, message)
            self._append_logged_message(
                f"{prefix} {message}",
                extra_log_path=log_file_path,
                extra_log_owner=prefix,
            )

        return _logger

    def _make_system_logger(self, name: str):
        prefix = f"[{name}]"

        def _logger(message: str) -> None:
            self._enqueue_log(f"{prefix} {message}")

        return _logger

    def _create_session_engine(
        self,
        *,
        strategy_id: str,
        strategy_name: str,
        session_id: str,
        symbol: str,
        api_name: str,
        log_file_path: Path | None,
        notifier: EmailNotifier | None,
        direction_label: str,
        run_mode_label: str,
        trader_id: str = "",
    ) -> StrategyEngine:
        session_logger = self._make_session_logger(
            session_id,
            strategy_name,
            symbol,
            api_name,
            log_file_path,
        )
        return StrategyEngine(
            self.client,
            session_logger,
            notifier=notifier,
            strategy_name=strategy_name,
            session_id=session_id,
            direction_label=direction_label,
            run_mode_label=run_mode_label,
            trader_id=trader_id,
            api_name=api_name,
        )

    def _next_session_id(self) -> str:
        while True:
            self._session_counter += 1
            session_id = f"S{self._session_counter:02d}"
            if session_id not in self.sessions:
                return session_id

    def _record_session_runtime_message(self, session_id: str, message: str) -> None:
        session = self.sessions.get(session_id)
        if session is None:
            return
        text = str(message or "").strip()
        if not text:
            return
        session.last_message = text
        inferred = _infer_session_runtime_status(text, session.runtime_status)
        if inferred:
            session.runtime_status = inferred
        self._track_session_trade_runtime(session, text)

    def _ensure_session_trade_runtime(
        self,
        session: StrategySession,
        *,
        observed_at: datetime,
        signal_bar_at: datetime | None = None,
    ) -> StrategyTradeRuntimeState:
        current = session.active_trade
        if current is None or current.reconciliation_started:
            current = StrategyTradeRuntimeState(
                round_id=f"{session.session_id}-{observed_at.strftime('%Y%m%d%H%M%S%f')}",
                signal_bar_at=signal_bar_at,
            )
            session.active_trade = current
        elif signal_bar_at is not None and current.signal_bar_at is None:
            current.signal_bar_at = signal_bar_at
        return current

    def _track_session_trade_runtime(self, session: StrategySession, message: str) -> None:
        observed_at = datetime.now()
        signal_bar_at = _extract_session_bar_time(message)
        if "挂单已提交到 OKX" in message:
            trade = self._ensure_session_trade_runtime(session, observed_at=observed_at, signal_bar_at=signal_bar_at)
            entry_order_id = _extract_log_field(message, "ordId")
            if entry_order_id:
                trade.entry_order_id = entry_order_id
            return
        if "委托追踪" in message:
            trade = self._ensure_session_trade_runtime(session, observed_at=observed_at, signal_bar_at=signal_bar_at)
            client_order_id = _extract_log_field(message, "clOrdId")
            if client_order_id:
                trade.entry_client_order_id = client_order_id
            return
        if "挂单已成交" in message:
            trade = self._ensure_session_trade_runtime(session, observed_at=observed_at, signal_bar_at=signal_bar_at)
            trade.opened_logged_at = observed_at
            entry_order_id = _extract_log_field(message, "ordId")
            if entry_order_id:
                trade.entry_order_id = entry_order_id
            entry_price = _extract_log_field_decimal(message, "开仓价")
            if entry_price is not None:
                trade.entry_price = entry_price
            size = _extract_log_field_decimal(message, "数量")
            if size is not None:
                trade.size = size
            QuantApp._trader_desk_sync_open_trade_state(self, session)
            return
        if "交易员虚拟止损监控启动" in message:
            trade = session.active_trade
            if trade is None:
                return
            stop_price = _extract_log_field_decimal(message, "策略止损")
            if stop_price is not None:
                trade.current_stop_price = stop_price
            return
        if "交易员动态止盈保护价已上移" in message:
            trade = session.active_trade
            if trade is None:
                return
            new_stop = _extract_log_field_decimal(message, "新保护价") or _extract_log_field_decimal(message, "保护价")
            if new_stop is not None:
                trade.current_stop_price = new_stop
            return
        if "交易员虚拟止损已触发（不平仓）" in message:
            trade = session.active_trade
            if trade is None:
                return
            stop_price = _extract_log_field_decimal(message, "策略止损")
            if stop_price is not None:
                trade.current_stop_price = stop_price
            return
        if "初始 OKX 止损已提交" in message:
            trade = session.active_trade
            if trade is None:
                return
            algo_cl_ord_id = _extract_log_field(message, "algoClOrdId")
            if algo_cl_ord_id:
                trade.protective_algo_cl_ord_id = algo_cl_ord_id
            stop_price = _extract_log_field_decimal(message, "止损")
            if stop_price is not None:
                trade.current_stop_price = stop_price
            return
        if "OKX 动态止损已上移" in message:
            trade = session.active_trade
            if trade is None:
                return
            new_stop = _extract_log_field_decimal(message, "新止损") or _extract_log_field_decimal(message, "止损")
            if new_stop is not None:
                trade.current_stop_price = new_stop
            return
        if "本轮持仓已结束，继续监控下一次信号" in message:
            trade = session.active_trade
            if trade is None or trade.reconciliation_started:
                return
            trade.reconciliation_started = True
            self._start_session_trade_reconciliation(session, trade)

    def _trader_desk_sync_open_trade_state(self, session: StrategySession) -> None:
        trader_id = getattr(session, "trader_id", "").strip()
        trader_slot_id = getattr(session, "trader_slot_id", "").strip()
        if not trader_id or not trader_slot_id:
            return
        slot = self._trader_desk_slot_for_session(session.session_id, trader_slot_id)
        trade = session.active_trade
        if slot is None or trade is None or trade.opened_logged_at is None:
            return
        changed = False
        if slot.status == "watching":
            slot.status = "open"
            slot.quota_occupied = True
            slot.opened_at = trade.opened_logged_at or datetime.now()
            changed = True
        if slot.entry_price is None and trade.entry_price is not None:
            slot.entry_price = trade.entry_price
            changed = True
        if slot.size is None and trade.size is not None:
            slot.size = trade.size
            changed = True
        if not changed:
            return
        run = self._trader_desk_run_by_id(trader_id, create=True)
        if run is not None and run.armed_session_id == session.session_id:
            run.armed_session_id = ""
            lock_signal = QuantApp._trader_wave_lock_signal_from_session(session)
            if lock_signal:
                run.wave_lock_signal = lock_signal
            run.last_event_at = datetime.now()
            run.updated_at = datetime.now()
        self._trader_desk_add_event(
            trader_id,
            f"额度格已开仓 | 会话={session.session_id} | 开仓价={_format_optional_decimal(slot.entry_price)} | 数量={_format_optional_decimal(slot.size)}",
        )
        self._save_trader_desk_snapshot()
        self._ensure_trader_watcher(trader_id)

    def _start_session_trade_reconciliation(
        self,
        session: StrategySession,
        trade: StrategyTradeRuntimeState,
    ) -> None:
        credentials = self._credentials_for_profile_or_none(session.api_name)
        if credentials is None:
            self._log_session_message(
                session,
                "检测到仓位已关闭，但未找到该会话对应的 API 凭证，无法自动归因与结算。",
            )
            if session.active_trade is not None and session.active_trade.round_id == trade.round_id:
                session.active_trade = None
            return
        self._log_session_message(
            session,
            f"检测到仓位已关闭，开始归因 | 最近保护单={trade.protective_algo_cl_ord_id or '-'}",
        )
        trade_snapshot = StrategyTradeRuntimeState(
            round_id=trade.round_id,
            signal_bar_at=trade.signal_bar_at,
            opened_logged_at=trade.opened_logged_at,
            entry_order_id=trade.entry_order_id,
            entry_client_order_id=trade.entry_client_order_id,
            entry_price=trade.entry_price,
            size=trade.size,
            protective_algo_id=trade.protective_algo_id,
            protective_algo_cl_ord_id=trade.protective_algo_cl_ord_id,
            current_stop_price=trade.current_stop_price,
            reconciliation_started=True,
        )
        threading.Thread(
            target=self._reconcile_session_trade_worker,
            args=(session.session_id, trade_snapshot, credentials),
            daemon=True,
        ).start()

    def _load_strategy_trade_reconciliation_snapshot(
        self,
        session: StrategySession,
        credentials: Credentials,
        environment: str,
    ) -> StrategyTradeReconciliationSnapshot:
        trade_inst_id = (session.config.trade_inst_id or session.config.inst_id or session.symbol).strip().upper()
        inst_type = infer_inst_type(trade_inst_id) if trade_inst_id else "SWAP"
        inst_types = (inst_type,)
        return StrategyTradeReconciliationSnapshot(
            effective_environment=environment,
            order_history=self.client.get_order_history(credentials, environment=environment, inst_types=inst_types, limit=400),
            fills=self.client.get_fills_history(credentials, environment=environment, inst_types=inst_types, limit=400),
            position_history=self.client.get_positions_history(
                credentials,
                environment=environment,
                inst_types=inst_types if inst_type != "SPOT" else ("SPOT",),
                limit=200,
            ),
            account_bills=self.client.get_account_bills_history(
                credentials,
                environment=environment,
                inst_types=inst_types,
                limit=300,
            ),
        )

    def _load_strategy_trade_reconciliation_snapshot_with_fallback(
        self,
        session: StrategySession,
        credentials: Credentials,
    ) -> StrategyTradeReconciliationSnapshot:
        environment = session.config.environment
        try:
            return self._load_strategy_trade_reconciliation_snapshot(session, credentials, environment)
        except Exception as exc:
            message = str(exc)
            if "50101" not in message or "current environment" not in message:
                raise
            alternate = "live" if environment == "demo" else "demo"
            snapshot = self._load_strategy_trade_reconciliation_snapshot(session, credentials, alternate)
            snapshot.environment_note = f"本轮归因自动切换到 {'实盘' if alternate == 'live' else '模拟'} 环境读取。"
            return snapshot

    @staticmethod
    def _is_funding_fee_bill(item: OkxAccountBillItem) -> bool:
        if (item.bill_sub_type or "").strip() in FUNDING_FEE_BILL_SUBTYPES:
            return True
        text = " ".join(
            value.strip().lower()
            for value in (item.bill_type or "", item.bill_sub_type or "", item.business_type or "", item.event_type or "")
            if value
        )
        return any(marker in text for marker in FUNDING_FEE_BILL_MARKERS)

    def _build_strategy_trade_reconciliation_result(
        self,
        session: StrategySession,
        trade: StrategyTradeRuntimeState,
        snapshot: StrategyTradeReconciliationSnapshot,
    ) -> StrategyTradeReconciliationResult:
        trade_inst_id = (session.config.trade_inst_id or session.config.inst_id or session.symbol).strip().upper()
        open_anchor = trade.opened_logged_at or datetime.now()
        open_ms = int((open_anchor - timedelta(minutes=2)).timestamp() * 1000)

        session_orders = [
            item
            for item in snapshot.order_history
            if _trade_order_belongs_to_session(item, session)
            and (not trade_inst_id or item.inst_id.strip().upper() == trade_inst_id)
        ]
        recent_orders = [item for item in session_orders if _trade_order_event_time(item) >= open_ms]
        if not recent_orders:
            recent_orders = session_orders

        entry_order = next(
            (
                item
                for item in recent_orders
                if trade.entry_order_id and (item.order_id or "").strip() == trade.entry_order_id
            ),
            None,
        )
        if entry_order is None:
            entry_order = next(
                (
                    item
                    for item in recent_orders
                    if trade.entry_client_order_id and (item.client_order_id or "").strip() == trade.entry_client_order_id
                ),
                None,
            )
        if entry_order is None:
            entry_candidates = [item for item in recent_orders if _trade_order_session_role(item, session) == "ent"]
            entry_candidates.sort(key=_trade_order_event_time)
            entry_order = next(
                (
                    item
                    for item in entry_candidates
                    if (item.filled_size or Decimal("0")) > 0 or (item.state or "").strip().lower() == "filled"
                ),
                entry_candidates[0] if entry_candidates else None,
            )

        protective_orders = [item for item in recent_orders if _trade_order_session_role(item, session) == "slg"]
        protective_orders.sort(
            key=lambda item: (
                0
                if trade.protective_algo_cl_ord_id
                and (item.algo_client_order_id or "").strip() == trade.protective_algo_cl_ord_id
                else 1,
                -_trade_order_event_time(item),
            )
        )
        protective_order = protective_orders[0] if protective_orders else None

        exit_orders = [item for item in recent_orders if _trade_order_session_role(item, session) == "exi"]
        exit_orders.sort(key=_trade_order_event_time, reverse=True)
        filled_exit_order = next(
            (
                item
                for item in exit_orders
                if (item.filled_size or Decimal("0")) > 0
                or (item.actual_size or Decimal("0")) > 0
                or (item.state or "").strip().lower() == "filled"
            ),
            None,
        )

        relevant_fills = [
            item
            for item in snapshot.fills
            if (not trade_inst_id or item.inst_id.strip().upper() == trade_inst_id)
            and (item.fill_time or 0) >= open_ms
        ]
        entry_order_ids = {
            value
            for value in (
                trade.entry_order_id,
                entry_order.order_id if entry_order is not None else "",
            )
            if value
        }
        close_order_ids = {
            value
            for value in (
                filled_exit_order.order_id if filled_exit_order is not None else "",
                protective_order.order_id if protective_order is not None else "",
            )
            if value
        }
        entry_fills = [item for item in relevant_fills if (item.order_id or "") in entry_order_ids]
        close_fills = [item for item in relevant_fills if (item.order_id or "") in close_order_ids]

        relevant_position_history = [
            item
            for item in snapshot.position_history
            if (not trade_inst_id or item.inst_id.strip().upper() == trade_inst_id)
            and (item.update_time or 0) >= open_ms
        ]
        relevant_position_history.sort(key=lambda item: item.update_time or 0, reverse=True)
        matched_position_history = relevant_position_history[0] if relevant_position_history else None

        close_reason = "持仓已关闭（原因待确认）"
        reason_confidence = "low"
        close_order = filled_exit_order
        protective_executed = False
        if protective_order is not None:
            protective_executed = (
                (protective_order.actual_size or Decimal("0")) > 0
                or (protective_order.filled_size or Decimal("0")) > 0
                or protective_order.actual_price is not None
                or (
                    protective_order.order_id is not None
                    and any((item.order_id or "") == protective_order.order_id for item in close_fills)
                )
            )
        if protective_executed:
            close_reason = "OKX止损触发"
            reason_confidence = "high"
            close_order = protective_order
        elif filled_exit_order is not None:
            close_reason = "策略主动平仓"
            reason_confidence = "high"
        elif close_fills:
            close_reason = "外部成交平仓"
            reason_confidence = "medium"
        elif matched_position_history is not None:
            close_reason = "持仓已关闭（原因待确认）"
            reason_confidence = "medium"

        entry_price = (
            _weighted_average_fill_price(entry_fills)
            or trade.entry_price
            or (entry_order.avg_price if entry_order is not None else None)
            or (entry_order.actual_price if entry_order is not None else None)
            or (entry_order.price if entry_order is not None else None)
        )
        size = (
            _sum_fill_size(entry_fills)
            or trade.size
            or (entry_order.filled_size if entry_order is not None else None)
            or (entry_order.actual_size if entry_order is not None else None)
            or (entry_order.size if entry_order is not None else None)
        )
        exit_price = (
            _weighted_average_fill_price(close_fills)
            or (close_order.actual_price if close_order is not None else None)
            or (close_order.avg_price if close_order is not None else None)
            or (close_order.price if close_order is not None else None)
            or (matched_position_history.close_avg_price if matched_position_history is not None else None)
            or trade.current_stop_price
        )
        entry_fee = _sum_fill_fee(entry_fills)
        if entry_fee is None and entry_order is not None:
            entry_fee = entry_order.fee
        exit_fee = _sum_fill_fee(close_fills)
        if exit_fee is None and close_order is not None:
            exit_fee = close_order.fee
        gross_pnl = _sum_fill_pnl(close_fills)
        if gross_pnl is None and close_order is not None:
            gross_pnl = close_order.pnl
        if gross_pnl is None and matched_position_history is not None:
            gross_pnl = matched_position_history.pnl

        close_time_ms = max(
            [item.fill_time or 0 for item in close_fills]
            + [
                _trade_order_event_time(close_order) if close_order is not None else 0,
                matched_position_history.update_time if matched_position_history is not None else 0,
            ]
        )
        if close_time_ms > 0:
            closed_at = datetime.fromtimestamp(close_time_ms / 1000)
        else:
            closed_at = datetime.now()

        funding_fee = None
        if snapshot.account_bills:
            funding_total = Decimal("0")
            funding_seen = False
            close_window_ms = int((closed_at + timedelta(minutes=2)).timestamp() * 1000)
            for bill in snapshot.account_bills:
                if trade_inst_id and bill.inst_id.strip().upper() != trade_inst_id:
                    continue
                if (bill.bill_time or 0) < open_ms or (bill.bill_time or 0) > close_window_ms:
                    continue
                if not self._is_funding_fee_bill(bill):
                    continue
                amount = bill.amount
                if amount is None:
                    amount = bill.pnl if bill.pnl is not None else bill.balance_change
                if amount is None:
                    continue
                funding_total += amount
                funding_seen = True
            if funding_seen:
                funding_fee = funding_total

        net_pnl = None
        if gross_pnl is not None:
            net_pnl = gross_pnl + (entry_fee or Decimal("0")) + (exit_fee or Decimal("0")) + (funding_fee or Decimal("0"))
        elif matched_position_history is not None and matched_position_history.realized_pnl is not None:
            net_pnl = matched_position_history.realized_pnl

        ledger_record = StrategyTradeLedgerRecord(
            record_id=self._next_strategy_trade_ledger_record_id(session, closed_at),
            history_record_id=session.history_record_id or "",
            session_id=session.session_id,
            api_name=session.api_name,
            strategy_id=session.strategy_id,
            strategy_name=session.strategy_name,
            symbol=trade_inst_id or session.symbol,
            direction_label=session.direction_label,
            run_mode_label=session.run_mode_label,
            environment=snapshot.effective_environment,
            signal_bar_at=trade.signal_bar_at,
            opened_at=trade.opened_logged_at,
            closed_at=closed_at,
            entry_order_id=trade.entry_order_id or (entry_order.order_id if entry_order is not None else ""),
            entry_client_order_id=trade.entry_client_order_id,
            exit_order_id=close_order.order_id if close_order is not None and close_order.order_id is not None else "",
            protective_algo_id=protective_order.algo_id if protective_order is not None and protective_order.algo_id is not None else "",
            protective_algo_cl_ord_id=trade.protective_algo_cl_ord_id or (
                protective_order.algo_client_order_id if protective_order is not None else ""
            ),
            entry_price=entry_price,
            exit_price=exit_price,
            size=size,
            entry_fee=entry_fee,
            exit_fee=exit_fee,
            funding_fee=funding_fee,
            gross_pnl=gross_pnl,
            net_pnl=net_pnl,
            close_reason=close_reason,
            reason_confidence=reason_confidence,
            summary_note=snapshot.environment_note or "",
            updated_at=datetime.now(),
        )

        projected_trade_count = session.trade_count + 1
        projected_win_count = session.win_count + (1 if (net_pnl or Decimal("0")) > 0 else 0)
        projected_net_pnl = session.net_pnl_total + (net_pnl or Decimal("0"))
        win_rate_text = (
            _format_ratio(Decimal(projected_win_count) / Decimal(projected_trade_count), places=2)
            if projected_trade_count
            else "-"
        )
        attribution_summary = (
            f"本轮结束 | 原因={close_reason} | 开仓均价={_format_optional_decimal(entry_price)} | "
            f"平仓均价={_format_optional_decimal(exit_price)} | 数量={_format_optional_decimal(size)} | "
            f"开仓手续费={_format_optional_usdt_precise(entry_fee, places=2)} | "
            f"平仓手续费={_format_optional_usdt_precise(exit_fee, places=2)} | "
            f"资金费={_format_optional_usdt_precise(funding_fee, places=2)} | "
            f"毛盈亏={_format_optional_usdt_precise(gross_pnl, places=2)} | "
            f"净盈亏={_format_optional_usdt_precise(net_pnl, places=2)}"
        )
        cumulative_summary = (
            f"会话累计 | 交易次数={projected_trade_count} | 胜率={win_rate_text} | "
            f"累计净盈亏={_format_optional_usdt_precise(projected_net_pnl, places=2)}"
        )
        return StrategyTradeReconciliationResult(
            session_id=session.session_id,
            round_id=trade.round_id,
            ledger_record=ledger_record,
            environment_note=snapshot.environment_note,
            attribution_summary=attribution_summary,
            cumulative_summary=cumulative_summary,
        )

    def _reconcile_session_trade_worker(
        self,
        session_id: str,
        trade: StrategyTradeRuntimeState,
        credentials: Credentials,
    ) -> None:
        session = self.sessions.get(session_id)
        if session is None:
            return
        try:
            snapshot = self._load_strategy_trade_reconciliation_snapshot_with_fallback(session, credentials)
            result = self._build_strategy_trade_reconciliation_result(session, trade, snapshot)
        except Exception as exc:
            result = StrategyTradeReconciliationResult(
                session_id=session_id,
                round_id=trade.round_id,
                error_message=_format_network_error_message(str(exc)),
            )
        self.root.after(0, lambda: self._apply_strategy_trade_reconciliation_result(result))

    def _apply_strategy_trade_reconciliation_result(self, result: StrategyTradeReconciliationResult) -> None:
        session = self.sessions.get(result.session_id)
        if session is None:
            return
        if session.active_trade is not None and session.active_trade.round_id == result.round_id:
            session.active_trade = None
        if result.environment_note:
            self._log_session_message(session, result.environment_note)
        if result.error_message:
            self._log_session_message(session, f"本轮结束归因失败：{result.error_message}")
            return
        if result.ledger_record is None:
            return
        self._upsert_strategy_trade_ledger_record(result.ledger_record)
        self._refresh_session_financials_from_trade_ledger(session)
        session.last_close_reason = result.ledger_record.close_reason
        self._refresh_running_session_summary()
        self._log_session_message(session, result.attribution_summary)
        self._log_session_message(session, result.cumulative_summary)
        self._apply_trader_desk_reconciliation(session, result.ledger_record)

    def _apply_trader_desk_reconciliation(
        self,
        session: StrategySession,
        ledger_record: StrategyTradeLedgerRecord,
    ) -> None:
        trader_id = getattr(session, "trader_id", "").strip()
        trader_slot_id = getattr(session, "trader_slot_id", "").strip()
        if not trader_id or not trader_slot_id:
            return
        slot = self._trader_desk_slot_for_session(session.session_id, trader_slot_id)
        draft = self._trader_desk_draft_by_id(trader_id)
        run = self._trader_desk_run_by_id(trader_id, create=True)
        if slot is None or draft is None or run is None:
            return
        now = datetime.now()
        close_reason = ledger_record.close_reason or session.ended_reason or ""
        net_pnl = ledger_record.net_pnl or Decimal("0")
        is_profit = net_pnl > 0
        if "手动" in close_reason:
            slot.status = "closed_manual"
        else:
            slot.status = "closed_profit" if is_profit else "closed_loss"
        slot.quota_occupied = False
        slot.opened_at = ledger_record.opened_at or slot.opened_at
        slot.closed_at = ledger_record.closed_at
        slot.released_at = ledger_record.closed_at
        slot.entry_price = ledger_record.entry_price if ledger_record.entry_price is not None else slot.entry_price
        slot.exit_price = ledger_record.exit_price
        slot.size = ledger_record.size if ledger_record.size is not None else slot.size
        slot.net_pnl = net_pnl
        slot.close_reason = close_reason
        slot.history_record_id = ledger_record.history_record_id or session.history_record_id or ""
        run.armed_session_id = ""
        run.last_event_at = now
        run.updated_at = now
        if slot.status == "closed_loss" and draft.pause_on_stop_loss:
            run.status = "paused_loss"
            run.paused_reason = close_reason or "亏损后暂停"
            self._trader_desk_add_event(
                trader_id,
                f"亏损单已记录并暂停 | 会话={session.session_id} | 净盈亏={_format_optional_usdt_precise(net_pnl, places=2)} | 原因={close_reason or '-'}",
                level="warning",
            )
            self._save_trader_desk_snapshot()
            return
        if slot.status == "closed_profit":
            self._trader_desk_add_event(
                trader_id,
                f"盈利单已释放额度 | 会话={session.session_id} | 净盈亏={_format_optional_usdt_precise(net_pnl, places=2)}",
            )
            if not draft.auto_restart_on_profit:
                run.status = "idle"
                run.paused_reason = "盈利后等待人工继续。"
                self._save_trader_desk_snapshot()
                return
        else:
            self._trader_desk_add_event(
                trader_id,
                f"额度格已结束 | 会话={session.session_id} | 状态={slot.status} | 原因={close_reason or '-'}",
                level="warning" if slot.status == "closed_loss" else "info",
            )
        if run.status not in {"paused_manual", "paused_loss", "stopped"}:
            run.status = "running"
            run.paused_reason = ""
        self._save_trader_desk_snapshot()
        self._ensure_trader_watcher(trader_id)

    def _upsert_session_row(self, session: StrategySession) -> None:
        if not QuantApp._session_matches_running_filter(self, session):
            if self.session_tree.exists(session.session_id):
                self.session_tree.delete(session.session_id)
            return
        live_pnl, _ = self._session_live_pnl_snapshot(session)
        source_type = QuantApp._session_category_label(session)
        trader_label = QuantApp._session_trader_label(self, session)
        email_label = QuantApp._session_email_status_label(self, session)
        bar_label = str(getattr(getattr(session, "config", None), "bar", "") or "").strip() or "-"
        tags = ("duplicate_conflict",) if QuantApp._session_has_duplicate_launch_conflict(self, session) else ()
        values = (
            session.session_id,
            trader_label,
            email_label,
            session.api_name or "-",
            source_type,
            session.strategy_name,
            session.symbol,
            bar_label,
            session.direction_label,
            session.run_mode_label,
            _format_optional_usdt_precise(live_pnl, places=2),
            _format_optional_usdt_precise(session.net_pnl_total, places=2),
            session.display_status,
            session.started_at.strftime("%H:%M:%S"),
        )
        if self.session_tree.exists(session.session_id):
            self.session_tree.item(session.session_id, values=values, tags=tags)
        else:
            self.session_tree.insert("", END, iid=session.session_id, values=values, tags=tags)

    def _selected_session(self) -> StrategySession | None:
        selected = self.session_tree.selection()
        if not selected:
            return None
        return self.sessions.get(selected[0])

    def _on_session_selected(self, *_: object) -> None:
        self._refresh_selected_session_details()

    @staticmethod
    def _session_tree_double_click_hint(column_id: str) -> str:
        return {
            "#1": "双击打开这条会话的独立日志",
            "#2": "双击打开并定位对应交易员",
            "#3": "双击切换当前会话发邮件开关",
            "#7": "双击打开这条策略的实时K线图",
        }.get(str(column_id or "").strip(), "")

    def _show_session_tree_hover_tip(self, text: str, *, x_root: int, y_root: int) -> None:
        if not text:
            self._hide_session_tree_hover_tip()
            return
        if self._session_tree_hover_tip_window is None or not self._session_tree_hover_tip_window.winfo_exists():
            window = Toplevel(self.root)
            window.withdraw()
            window.overrideredirect(True)
            window.attributes("-topmost", True)
            label = ttk.Label(window, text=text, padding=(8, 4), relief="solid", borderwidth=1)
            label.pack()
            self._session_tree_hover_tip_window = window
            self._session_tree_hover_tip_label = label
        else:
            window = self._session_tree_hover_tip_window
            label = self._session_tree_hover_tip_label
            if label is not None:
                label.configure(text=text)
        if window is None:
            return
        window.geometry(f"+{x_root + 12}+{y_root + 16}")
        window.deiconify()

    def _hide_session_tree_hover_tip(self) -> None:
        window = self._session_tree_hover_tip_window
        if window is not None and window.winfo_exists():
            window.withdraw()
        self._session_tree_hover_tip_column = ""

    def _on_session_tree_hover(self, event: object) -> None:
        tree = self.session_tree
        try:
            x = int(getattr(event, "x", 0) or 0)
            y = int(getattr(event, "y", 0) or 0)
            x_root = int(getattr(event, "x_root", 0) or 0)
            y_root = int(getattr(event, "y_root", 0) or 0)
            region = str(tree.identify_region(x, y))
            column_id = str(tree.identify_column(x))
        except Exception:
            self._hide_session_tree_hover_tip()
            return
        if region != "heading":
            self._hide_session_tree_hover_tip()
            return
        tip_text = QuantApp._session_tree_double_click_hint(column_id)
        if not tip_text:
            self._hide_session_tree_hover_tip()
            return
        self._session_tree_hover_tip_column = column_id
        self._show_session_tree_hover_tip(tip_text, x_root=x_root, y_root=y_root)

    def _on_session_tree_hover_leave(self, *_: object) -> None:
        self._hide_session_tree_hover_tip()

    def _on_session_tree_double_click(self, event: object) -> str | None:
        tree = self.session_tree
        try:
            column_id = str(tree.identify_column(getattr(event, "x", 0) or 0))
            row_id = str(tree.identify_row(getattr(event, "y", 0) or 0)).strip()
        except Exception:
            return None
        if not row_id:
            return None
        session = self.sessions.get(row_id)
        if session is None:
            return None
        if column_id == "#1":
            self.open_strategy_session_log(session.session_id)
            return "break"
        if column_id == "#2":
            trader_id = str(getattr(session, "trader_id", "") or "").strip()
            if trader_id:
                self.open_trader_desk_window_for_trader(trader_id)
                return "break"
            return None
        if column_id == "#3":
            self._set_selected_session_email_notifications(not bool(getattr(session, "email_notifications_enabled", True)))
            return "break"
        if column_id == "#7":
            self.open_strategy_live_chart_window(session.session_id)
            return "break"
        return None

    def _refresh_selected_session_details(self) -> None:
        session = self._selected_session()
        if session is None:
            self.selected_session_text.set(self._default_selected_session_text())
            self._set_readonly_text(self._selected_session_detail, self.selected_session_text.get())
            self._selected_session_detail_session_id = None
            return

        preserve_scroll = session.session_id == self._selected_session_detail_session_id
        live_pnl, live_pnl_refreshed_at = self._session_live_pnl_snapshot(session)
        duplicate_warning = QuantApp._build_duplicate_launch_conflict_warning(
            session,
            QuantApp._duplicate_launch_conflicts_for(self, session),
        )
        self.selected_session_text.set(
            self._build_strategy_detail_text(
                session_id=session.session_id,
                api_name=session.api_name,
                status=session.status,
                runtime_status=session.runtime_status,
                strategy_id=session.strategy_id,
                strategy_name=session.strategy_name,
                symbol=session.symbol,
                direction_label=session.direction_label,
                run_mode_label=session.run_mode_label,
                started_at=session.started_at,
                stopped_at=session.stopped_at,
                ended_reason=session.ended_reason,
                config_snapshot=_serialize_strategy_config_snapshot(session.config),
                log_file_path=session.log_file_path,
                last_message=session.last_message,
                trade_count=session.trade_count,
                win_count=session.win_count,
                gross_pnl_total=session.gross_pnl_total,
                fee_total=session.fee_total,
                funding_total=session.funding_total,
                net_pnl_total=session.net_pnl_total,
                last_close_reason=session.last_close_reason,
                live_pnl=live_pnl,
                live_pnl_refreshed_at=live_pnl_refreshed_at,
                duplicate_warning=duplicate_warning,
                email_status_label=QuantApp._session_email_status_label(self, session),
                global_email_enabled=self.notify_enabled.get(),
            )
        )
        self._set_readonly_text(
            self._selected_session_detail,
            self.selected_session_text.get(),
            preserve_scroll=preserve_scroll,
        )
        self._selected_session_detail_session_id = session.session_id

    @staticmethod
    def _snapshot_optional_text(snapshot: dict[str, object], key: str) -> str | None:
        value = snapshot.get(key)
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    @staticmethod
    def _snapshot_text(snapshot: dict[str, object], key: str, default: str = "-") -> str:
        value = QuantApp._snapshot_optional_text(snapshot, key)
        return value if value is not None else default

    @staticmethod
    def _snapshot_int(snapshot: dict[str, object], key: str, default: int = 0) -> int:
        value = snapshot.get(key, default)
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _bool_label(value: object) -> str:
        if isinstance(value, str):
            normalized = value.strip().lower()
            return "开启" if normalized in {"1", "true", "yes", "on", "开启"} else "关闭"
        return "开启" if bool(value) else "关闭"

    def _build_strategy_detail_text(
        self,
        *,
        session_id: str,
        api_name: str,
        status: str,
        strategy_id: str,
        strategy_name: str,
        symbol: str,
        direction_label: str,
        run_mode_label: str,
        started_at: datetime,
        stopped_at: datetime | None,
        ended_reason: str,
        config_snapshot: dict[str, object],
        log_file_path: str | Path | None = None,
        record_id: str | None = None,
        updated_at: datetime | None = None,
        runtime_status: str | None = None,
        last_message: str = "",
        trade_count: int = 0,
        win_count: int = 0,
        gross_pnl_total: Decimal = Decimal("0"),
        fee_total: Decimal = Decimal("0"),
        funding_total: Decimal = Decimal("0"),
        net_pnl_total: Decimal = Decimal("0"),
        last_close_reason: str = "",
        live_pnl: Decimal | None = None,
        live_pnl_refreshed_at: datetime | None = None,
        duplicate_warning: str = "",
        email_status_label: str = "-",
        global_email_enabled: bool = False,
    ) -> str:
        try:
            definition = get_strategy_definition(strategy_id)
            summary = definition.summary
            rule_description = definition.rule_description
            parameter_hint = definition.parameter_hint
        except KeyError:
            summary = "历史记录中的策略定义已不存在，保留原始参数供溯源。"
            rule_description = "-"
            parameter_hint = "-"
        snapshot = dict(config_snapshot or {})
        signal_inst_id = self._snapshot_optional_text(snapshot, "inst_id") or ""
        trade_inst_id = self._snapshot_optional_text(snapshot, "trade_inst_id")
        display_symbol = symbol or self._format_strategy_symbol_display(signal_inst_id, trade_inst_id)
        ema_period = self._snapshot_int(snapshot, "ema_period")
        trend_ema_period = self._snapshot_int(snapshot, "trend_ema_period")
        entry_reference_ema_period = self._snapshot_int(snapshot, "entry_reference_ema_period")
        lines = []
        if record_id:
            lines.append(f"记录ID：{record_id}")
        lines.extend(
            [
                f"会话：{session_id or '-'}",
                f"API配置：{api_name or '-'}",
                f"状态：{status or '-'}",
                f"独立日志：{_coerce_log_file_path(log_file_path) or '-'}",
            ]
        )
        if runtime_status and status == "运行中":
            lines.append(f"最近运行状态：{runtime_status}")
        if duplicate_warning:
            lines.append(duplicate_warning)
        if last_message:
            lines.append(f"最近日志：{last_message}")
        if ended_reason:
            lines.append(f"结束原因：{ended_reason}")
        lines.extend(
            [
                f"交易次数：{trade_count}",
                f"胜率：{_format_ratio(Decimal(win_count) / Decimal(trade_count), places=2) if trade_count else '-'}",
            ]
        )
        if record_id is None:
            live_pnl_text = _format_optional_usdt_precise(live_pnl, places=2)
            if live_pnl_refreshed_at is not None:
                live_pnl_text += f"（参考持仓 {live_pnl_refreshed_at.strftime('%H:%M:%S')}）"
            lines.append(f"实时浮盈亏：{live_pnl_text}")
        lines.extend(
            [
                f"毛盈亏：{_format_optional_usdt_precise(gross_pnl_total, places=2)}",
                f"手续费：{_format_optional_usdt_precise(fee_total, places=2)}",
                f"资金费：{_format_optional_usdt_precise(funding_total, places=2)}",
                f"净盈亏：{_format_optional_usdt_precise(net_pnl_total, places=2)}",
            ]
        )
        if last_close_reason:
            lines.append(f"最近结论：{last_close_reason}")
        lines.extend(
            [
                f"策略：{strategy_name}",
                f"运行模式：{run_mode_label or '-'}",
                f"交易标的：{display_symbol or '-'}",
                f"方向：{direction_label or '-'}",
                f"K线周期：{self._snapshot_text(snapshot, 'bar')}",
                f"EMA小周期：{ema_period or '-'}",
                f"EMA中周期：{trend_ema_period or '-'}",
            ]
        )
        if strategy_uses_parameter(strategy_id, "entry_reference_ema_period"):
            if entry_reference_ema_period > 0:
                entry_reference_label = f"EMA{entry_reference_ema_period}"
            else:
                entry_reference_label = f"跟随EMA小周期(EMA{ema_period or '-'})"
            lines.append(f"{self._entry_reference_ema_caption(strategy_id)}：{entry_reference_label}")
        if is_dynamic_strategy_id(strategy_id) or is_ema_atr_breakout_strategy(strategy_id):
            startup_window_seconds = self._snapshot_int(snapshot, "startup_chase_window_seconds") or 0
            lines.append(
                "启动追单窗口："
                + ("关闭（启动不追老信号）" if startup_window_seconds <= 0 else f"{startup_window_seconds}秒")
            )
            if self._snapshot_text(snapshot, "take_profit_mode", "dynamic") == "dynamic":
                lines.append(f"2R保本开关：{self._bool_label(snapshot.get('dynamic_two_r_break_even', True))}")
                lines.append(f"手续费偏移开关：{self._bool_label(snapshot.get('dynamic_fee_offset_enabled', True))}")
                lines.append(
                    "时间保本："
                    f"{self._bool_label(snapshot.get('time_stop_break_even_enabled', False))} / "
                    f"{self._snapshot_text(snapshot, 'time_stop_break_even_bars', '10')}根"
                )
        if self._strategy_uses_big_ema(strategy_id):
            lines.append(f"EMA大周期：{self._snapshot_int(snapshot, 'big_ema_period') or '-'}")
        lines.extend(
            [
                f"ATR 周期：{self._snapshot_text(snapshot, 'atr_period')}",
                f"止损 ATR 倍数：{self._snapshot_text(snapshot, 'atr_stop_multiplier')}",
                f"止盈 ATR 倍数：{self._snapshot_text(snapshot, 'atr_take_multiplier')}",
                f"风险金：{self._snapshot_text(snapshot, 'risk_amount')}",
                f"固定数量：{self._snapshot_text(snapshot, 'order_size')}",
                f"下单方向模式：{self._snapshot_text(snapshot, 'entry_side_mode')}",
                f"止盈止损模式：{self._snapshot_text(snapshot, 'tp_sl_mode')}",
                f"自定义触发标的：{self._snapshot_text(snapshot, 'local_tp_sl_inst_id')}",
                f"轮询秒数：{self._snapshot_text(snapshot, 'poll_seconds')}",
                f"启动时间：{_format_history_datetime(started_at)}",
            ]
        )
        if stopped_at is not None:
            lines.append(f"停止时间：{_format_history_datetime(stopped_at)}")
        if updated_at is not None:
            lines.append(f"最近更新：{_format_history_datetime(updated_at)}")
        lines.extend(
            [
                "",
                f"策略简介：{summary}",
                "",
                f"规则说明：{rule_description}",
                "",
                f"参数提示：{parameter_hint}",
            ]
        )
        return "\n".join(lines)

    def _recoverable_strategy_record_from_payload(
        self,
        payload: dict[str, object],
    ) -> RecoverableStrategySessionRecord | None:
        started_at = _parse_datetime_snapshot(payload.get("started_at"))
        if started_at is None:
            return None
        recovery_root_dir = _coerce_log_file_path(payload.get("recovery_root_dir"))
        if recovery_root_dir is None:
            return None
        config_snapshot = payload.get("config_snapshot")
        return RecoverableStrategySessionRecord(
            session_id=str(payload.get("session_id", "")).strip(),
            api_name=str(payload.get("api_name", "")).strip(),
            strategy_id=str(payload.get("strategy_id", "")).strip(),
            strategy_name=str(payload.get("strategy_name", "")).strip(),
            symbol=str(payload.get("symbol", "")).strip(),
            direction_label=str(payload.get("direction_label", "")).strip(),
            run_mode_label=str(payload.get("run_mode_label", "")).strip(),
            started_at=started_at,
            history_record_id=str(payload.get("history_record_id", "")).strip(),
            log_file_path=_coerce_log_file_path(payload.get("log_file_path")),
            recovery_root_dir=recovery_root_dir,
            config_snapshot=dict(config_snapshot) if isinstance(config_snapshot, dict) else {},
            updated_at=_parse_datetime_snapshot(payload.get("updated_at")),
        )

    @staticmethod
    def _recoverable_strategy_record_payload(record: RecoverableStrategySessionRecord) -> dict[str, object]:
        return {
            "session_id": record.session_id,
            "api_name": record.api_name,
            "strategy_id": record.strategy_id,
            "strategy_name": record.strategy_name,
            "symbol": record.symbol,
            "direction_label": record.direction_label,
            "run_mode_label": record.run_mode_label,
            "started_at": record.started_at.isoformat(timespec="seconds"),
            "history_record_id": record.history_record_id,
            "log_file_path": str(record.log_file_path) if record.log_file_path is not None else "",
            "recovery_root_dir": str(record.recovery_root_dir) if record.recovery_root_dir is not None else "",
            "config_snapshot": dict(record.config_snapshot),
            "updated_at": record.updated_at.isoformat(timespec="seconds") if record.updated_at is not None else None,
        }

    def _load_recoverable_strategy_sessions_registry(self) -> None:
        self._recoverable_strategy_sessions = {}

    def _save_recoverable_strategy_sessions_registry(self) -> None:
        return

    def _build_recoverable_strategy_session_record(
        self,
        session: StrategySession,
    ) -> RecoverableStrategySessionRecord | None:
        return None

    def _upsert_recoverable_strategy_session(self, session: StrategySession) -> None:
        return

    def _remove_recoverable_strategy_session(self, session_id: str) -> None:
        self._recoverable_strategy_sessions.pop(session_id, None)

    def _hydrate_recoverable_strategy_sessions(self) -> None:
        return

    def _attempt_auto_restore_recoverable_sessions(self) -> None:
        return

    def recover_selected_session(self) -> None:
        messagebox.showinfo("提示", "旧版恢复接管逻辑已下线。请使用信号观察台或交易员管理台的新流程。")

    def _recover_session(self, session_id: str, *, auto: bool) -> bool:
        if not auto:
            sess = self.sessions.get(session_id)
            pre = sess.log_prefix if sess is not None else f"[{session_id}]"
            self._enqueue_log(f"{pre} 旧版恢复接管逻辑已下线。")
        return False

    @staticmethod
    def _strategy_session_supports_recovery(config: StrategyConfig) -> bool:
        return (
            str(getattr(config, "run_mode", "") or "").strip().lower() == "trade"
            and str(getattr(config, "tp_sl_mode", "") or "").strip().lower() == "exchange"
        )

    @staticmethod
    def _parse_strategy_log_observed_at(line: str) -> datetime | None:
        match = re.match(r"^\[(?P<ts>(?:\d{4}-)?\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]", str(line or "").strip())
        if match is None:
            return None
        raw = match.group("ts")
        for fmt in ("%Y-%m-%d %H:%M:%S", "%m-%d %H:%M:%S"):
            try:
                parsed = datetime.strptime(raw, fmt)
            except ValueError:
                continue
            if fmt == "%m-%d %H:%M:%S":
                return parsed.replace(year=datetime.now().year)
            return parsed
        return None

    def _track_session_trade_runtime_with_observed_at(
        self,
        session: StrategySession,
        message: str,
        *,
        observed_at: datetime,
    ) -> None:
        signal_bar_at = _extract_session_bar_time(message)
        if "挂单已提交到 OKX" in message:
            trade = self._ensure_session_trade_runtime(session, observed_at=observed_at, signal_bar_at=signal_bar_at)
            entry_order_id = _extract_log_field(message, "ordId")
            if entry_order_id:
                trade.entry_order_id = entry_order_id
            return
        if "委托追踪" in message:
            trade = self._ensure_session_trade_runtime(session, observed_at=observed_at, signal_bar_at=signal_bar_at)
            client_order_id = _extract_log_field(message, "clOrdId")
            if client_order_id:
                trade.entry_client_order_id = client_order_id
            return
        if "挂单已成交" in message:
            trade = self._ensure_session_trade_runtime(session, observed_at=observed_at, signal_bar_at=signal_bar_at)
            trade.opened_logged_at = observed_at
            entry_order_id = _extract_log_field(message, "ordId")
            if entry_order_id:
                trade.entry_order_id = entry_order_id
            entry_price = _extract_log_field_decimal(message, "开仓价")
            if entry_price is not None:
                trade.entry_price = entry_price
            size = _extract_log_field_decimal(message, "数量")
            if size is not None:
                trade.size = size
            QuantApp._trader_desk_sync_open_trade_state(self, session)
            return
        if "交易员虚拟止损监控启动" in message:
            trade = session.active_trade
            if trade is None:
                return
            stop_price = _extract_log_field_decimal(message, "策略止损")
            if stop_price is not None:
                if trade.initial_stop_price is None:
                    trade.initial_stop_price = stop_price
                trade.current_stop_price = stop_price
            return
        if "交易员动态止盈保护价已上移" in message:
            trade = session.active_trade
            if trade is None:
                return
            new_stop = _extract_log_field_decimal(message, "新保护价") or _extract_log_field_decimal(message, "保护价")
            if new_stop is not None:
                trade.current_stop_price = new_stop
            return
        if "交易员虚拟止损已触发（不平仓）" in message:
            trade = session.active_trade
            if trade is None:
                return
            stop_price = _extract_log_field_decimal(message, "策略止损")
            if stop_price is not None:
                trade.current_stop_price = stop_price
            return
        if "初始 OKX 止损已提交" in message:
            trade = session.active_trade
            if trade is None:
                return
            algo_cl_ord_id = _extract_log_field(message, "algoClOrdId")
            if algo_cl_ord_id:
                trade.protective_algo_cl_ord_id = algo_cl_ord_id
            stop_price = _extract_log_field_decimal(message, "止损")
            if stop_price is not None:
                if trade.initial_stop_price is None:
                    trade.initial_stop_price = stop_price
                trade.current_stop_price = stop_price
            return
        if "OKX 动态止损已上移" in message:
            trade = session.active_trade
            if trade is None:
                return
            new_stop = _extract_log_field_decimal(message, "新止损") or _extract_log_field_decimal(message, "止损")
            if new_stop is not None:
                trade.current_stop_price = new_stop
            return
        if "本轮持仓已结束，继续监控下一次信号。" in message:
            trade = session.active_trade
            if trade is None or trade.reconciliation_started:
                return
            trade.reconciliation_started = True
            self._start_session_trade_reconciliation(session, trade)

    def _track_session_trade_runtime(self, session: StrategySession, message: str) -> None:
        self._track_session_trade_runtime_with_observed_at(
            session,
            message,
            observed_at=datetime.now(),
        )

    def _start_session_trade_reconciliation(
        self,
        session: StrategySession,
        trade: StrategyTradeRuntimeState,
    ) -> None:
        credentials = self._credentials_for_profile_or_none(session.api_name)
        if credentials is None:
            self._log_session_message(
                session,
                "检测到仓位已关闭，但未找到该会话对应的 API 凭证，无法自动归因与结算。",
            )
            if session.active_trade is not None and session.active_trade.round_id == trade.round_id:
                session.active_trade = None
            return
        self._log_session_message(
            session,
            f"检测到仓位已关闭，开始归因 | 最近保护单={trade.protective_algo_cl_ord_id or '-'}",
        )
        trade_snapshot = StrategyTradeRuntimeState(
            round_id=trade.round_id,
            signal_bar_at=trade.signal_bar_at,
            opened_logged_at=trade.opened_logged_at,
            entry_order_id=trade.entry_order_id,
            entry_client_order_id=trade.entry_client_order_id,
            entry_price=trade.entry_price,
            size=trade.size,
            protective_algo_id=trade.protective_algo_id,
            protective_algo_cl_ord_id=trade.protective_algo_cl_ord_id,
            initial_stop_price=trade.initial_stop_price,
            current_stop_price=trade.current_stop_price,
            reconciliation_started=True,
        )
        threading.Thread(
            target=self._reconcile_session_trade_worker,
            args=(session.session_id, trade_snapshot, credentials),
            daemon=True,
        ).start()

    def _restore_session_trade_runtime_from_log(self, session: StrategySession) -> StrategyTradeRuntimeState | None:
        log_path = _coerce_log_file_path(session.log_file_path)
        if log_path is None or not log_path.exists() or not log_path.is_file():
            return session.active_trade
        try:
            lines = log_path.read_text(encoding="utf-8").splitlines()
        except Exception as exc:
            self._enqueue_log(f"{session.log_prefix} 读取独立日志失败：{exc}")
            return session.active_trade
        session.active_trade = None
        for line in lines:
            text = str(line or "").strip()
            if not text:
                continue
            observed_at = self._parse_strategy_log_observed_at(text) or session.started_at or datetime.now()
            self._track_session_trade_runtime_with_observed_at(
                session,
                text,
                observed_at=observed_at,
            )
        return session.active_trade

    @staticmethod
    def _recoverable_position_direction(position: OkxPosition) -> Literal["long", "short"] | None:
        pos_side = str(position.pos_side or "").strip().lower()
        if pos_side == "long":
            return "long"
        if pos_side == "short":
            return "short"
        if position.position > 0:
            return "long"
        if position.position < 0:
            return "short"
        return None

    def _load_recoverable_live_positions(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        *,
        inst_type: str,
    ) -> list[OkxPosition]:
        try:
            return self.client.get_positions(credentials, environment=config.environment, inst_type=inst_type)
        except Exception as exc:
            message = str(exc)
            if "50101" not in message or "current environment" not in message:
                raise
            alternate = "live" if config.environment == "demo" else "demo"
            return self.client.get_positions(credentials, environment=alternate, inst_type=inst_type)

    @staticmethod
    def _recoverable_protective_order_direction(order: OkxTradeOrderItem) -> Literal["long", "short"] | None:
        pos_side = str(order.pos_side or "").strip().lower()
        if pos_side == "long":
            return "long"
        if pos_side == "short":
            return "short"
        side = str(order.side or order.actual_side or "").strip().lower()
        if side == "sell":
            return "long"
        if side == "buy":
            return "short"
        return None

    def _load_recoverable_pending_orders(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        *,
        inst_type: str,
    ) -> list[OkxTradeOrderItem]:
        try:
            return self.client.get_pending_orders(
                credentials,
                environment=config.environment,
                inst_types=(inst_type,),
                limit=200,
            )
        except Exception as exc:
            message = str(exc)
            if "50101" not in message or "current environment" not in message:
                raise
            alternate = "live" if config.environment == "demo" else "demo"
            return self.client.get_pending_orders(
                credentials,
                environment=alternate,
                inst_types=(inst_type,),
                limit=200,
            )

    def _find_recoverable_protective_order(
        self,
        credentials: Credentials,
        session: StrategySession,
        *,
        inst_type: str,
        inst_id: str,
        direction: Literal["long", "short"],
    ) -> OkxTradeOrderItem | None:
        try:
            pending_orders = self._load_recoverable_pending_orders(
                credentials,
                session.config,
                inst_type=inst_type,
            )
        except Exception:
            return None
        candidates = []
        for item in pending_orders:
            if str(item.inst_id or "").strip().upper() != inst_id:
                continue
            if (str(item.source_kind or "").strip().lower() != "algo"):
                continue
            if (item.stop_loss_trigger_price or item.trigger_price) is None:
                continue
            if not ((item.algo_id or "").strip() or (item.algo_client_order_id or "").strip()):
                continue
            item_direction = self._recoverable_protective_order_direction(item)
            if item_direction is not None and item_direction != direction:
                continue
            candidates.append(item)
        if not candidates:
            return None
        candidates.sort(key=lambda item: item.update_time or item.created_time or 0, reverse=True)
        return candidates[0]

    def _select_recoverable_live_position(
        self,
        session: StrategySession,
        positions: list[OkxPosition],
    ) -> OkxPosition | None:
        trade_inst_id = (session.config.trade_inst_id or session.config.inst_id or "").strip().upper()
        candidates = [
            item
            for item in positions
            if item.inst_id.strip().upper() == trade_inst_id
            and (item.avail_position if item.avail_position not in {None, Decimal("0")} else item.position) != 0
        ]
        if not candidates:
            return None
        preferred_direction: Literal["long", "short"] | None = None
        strategy_id = str(session.strategy_id or "").strip()
        if strategy_id in {STRATEGY_DYNAMIC_LONG_ID, STRATEGY_EMA_BREAKOUT_LONG_ID}:
            preferred_direction = "long"
        elif strategy_id in {STRATEGY_DYNAMIC_SHORT_ID, STRATEGY_EMA_BREAKDOWN_SHORT_ID}:
            preferred_direction = "short"
        else:
            signal_mode = resolve_dynamic_signal_mode(strategy_id, session.config.signal_mode)
            if signal_mode == "long_only":
                preferred_direction = "long"
            elif signal_mode == "short_only":
                preferred_direction = "short"
        if preferred_direction is not None:
            directional = [item for item in candidates if self._recoverable_position_direction(item) == preferred_direction]
            if not directional:
                return None
            candidates = directional
        if len(candidates) == 1:
            return candidates[0]
        candidates.sort(
            key=lambda item: abs(item.avail_position if item.avail_position not in {None, Decimal("0")} else item.position),
            reverse=True,
        )
        return candidates[0] if candidates else None

    def _load_recoverable_strategy_sessions_registry(self) -> None:
        self._recoverable_strategy_sessions = {}
        try:
            snapshot = load_recoverable_strategy_sessions_snapshot()
        except Exception as exc:
            self._enqueue_log(f"加载可恢复策略注册表失败：{exc}")
            return
        for payload in snapshot.get("sessions", []):
            if not isinstance(payload, dict):
                continue
            record = self._recoverable_strategy_record_from_payload(payload)
            if record is None or not record.session_id:
                continue
            self._recoverable_strategy_sessions[record.session_id] = record

    def _save_recoverable_strategy_sessions_registry(self) -> None:
        try:
            save_recoverable_strategy_sessions_snapshot(
                [self._recoverable_strategy_record_payload(record) for record in self._recoverable_strategy_sessions.values()]
            )
        except Exception as exc:
            self._enqueue_log(f"保存可恢复策略注册表失败：{exc}")

    def _build_recoverable_strategy_session_record(
        self,
        session: StrategySession,
    ) -> RecoverableStrategySessionRecord | None:
        if not session.recovery_supported:
            return None
        recovery_root_dir = _coerce_log_file_path(session.recovery_root_dir) or (
            session.log_file_path.parent if session.log_file_path is not None else None
        )
        if recovery_root_dir is None:
            return None
        return RecoverableStrategySessionRecord(
            session_id=session.session_id,
            api_name=session.api_name,
            strategy_id=session.strategy_id,
            strategy_name=session.strategy_name,
            symbol=session.symbol,
            direction_label=session.direction_label,
            run_mode_label=session.run_mode_label,
            started_at=session.started_at,
            history_record_id=session.history_record_id or "",
            log_file_path=_coerce_log_file_path(session.log_file_path),
            recovery_root_dir=recovery_root_dir,
            config_snapshot=_serialize_strategy_config_snapshot(session.config),
            updated_at=datetime.now(),
        )

    def _upsert_recoverable_strategy_session(self, session: StrategySession) -> None:
        record = self._build_recoverable_strategy_session_record(session)
        if record is None:
            return
        self._recoverable_strategy_sessions[record.session_id] = record
        self._save_recoverable_strategy_sessions_registry()

    def _remove_recoverable_strategy_session(self, session_id: str) -> None:
        if self._recoverable_strategy_sessions.pop(session_id, None) is not None:
            self._save_recoverable_strategy_sessions_registry()

    def _hydrate_recoverable_strategy_sessions(self) -> None:
        for record in list(self._recoverable_strategy_sessions.values()):
            if record.session_id in self.sessions:
                continue
            config = _deserialize_strategy_config_snapshot(record.config_snapshot)
            if config is None:
                continue
            try:
                definition = get_strategy_definition(record.strategy_id)
            except Exception:
                continue
            notifier = self._build_notifier(config)
            session_notifier = self._build_session_notifier(config, record.session_id) if notifier is not None else None
            session_symbol = record.symbol or self._format_strategy_symbol_display(config.inst_id, config.trade_inst_id)
            engine = self._create_session_engine(
                strategy_id=record.strategy_id,
                strategy_name=record.strategy_name or definition.name,
                session_id=record.session_id,
                symbol=session_symbol,
                api_name=record.api_name,
                log_file_path=record.log_file_path,
                notifier=session_notifier,
                direction_label=record.direction_label or definition.default_signal_label,
                run_mode_label=record.run_mode_label or _reverse_lookup_label(RUN_MODE_OPTIONS, config.run_mode, ""),
            )
            session = StrategySession(
                session_id=record.session_id,
                api_name=record.api_name,
                strategy_id=record.strategy_id,
                strategy_name=record.strategy_name or definition.name,
                symbol=session_symbol,
                direction_label=record.direction_label or definition.default_signal_label,
                run_mode_label=record.run_mode_label or _reverse_lookup_label(RUN_MODE_OPTIONS, config.run_mode, ""),
                engine=engine,
                config=config,
                started_at=record.started_at,
                status="待恢复",
                history_record_id=record.history_record_id or None,
                stopped_at=record.updated_at or record.started_at,
                ended_reason="应用关闭后待恢复接管",
                log_file_path=record.log_file_path,
                runtime_status="待恢复",
                recovery_root_dir=record.recovery_root_dir,
                recovery_supported=self._strategy_session_supports_recovery(config),
            )
            self.sessions[record.session_id] = session
            self._update_session_counter_from_session_id(record.session_id)
            self._upsert_session_row(session)
            self._sync_strategy_history_from_session(session)

    def _attempt_auto_restore_recoverable_sessions(self) -> None:
        for session_id in list(self._recoverable_strategy_sessions.keys()):
            self._recover_session(session_id, auto=True)

    def recover_selected_session(self) -> None:
        session = self._selected_session()
        if session is None:
            messagebox.showinfo("提示", "请先在右侧选择一条待恢复会话。", parent=self.root)
            return
        if not self._recover_session(session.session_id, auto=False):
            self._refresh_selected_session_details()

    def _recover_session(self, session_id: str, *, auto: bool) -> bool:
        session = self.sessions.get(session_id)
        if session is None:
            return False
        record = self._recoverable_strategy_sessions.get(session_id)
        if record is None:
            record = self._build_recoverable_strategy_session_record(session)
            if record is not None:
                self._recoverable_strategy_sessions[session_id] = record
        if not session.recovery_supported:
            if not auto:
                self._enqueue_log(f"{session.log_prefix} 当前会话不支持恢复接管。")
            return False
        if session.engine.is_running:
            return True
        credentials = self._credentials_for_profile_or_none(session.api_name or (record.api_name if record is not None else ""))
        if credentials is None:
            if not auto:
                self._enqueue_log(f"{session.log_prefix} 未找到该会话对应的 API 凭证，无法恢复接管。")
            return False
        trade_inst_id = (session.config.trade_inst_id or session.config.inst_id or session.symbol).strip().upper()
        if not trade_inst_id:
            if not auto:
                self._enqueue_log(f"{session.log_prefix} 缺少交易标的，无法恢复接管。")
            return False
        try:
            inst_type = infer_inst_type(trade_inst_id)
            positions = self._load_recoverable_live_positions(credentials, session.config, inst_type=inst_type)
            live_position = self._select_recoverable_live_position(session, positions)
        except Exception as exc:
            if not auto:
                self._enqueue_log(f"{session.log_prefix} 读取当前持仓失败，暂时无法恢复接管：{exc}")
            return False
        trade = self._restore_session_trade_runtime_from_log(session) or session.active_trade
        if live_position is None:
            pending_signal = str(trade.pending_signal or "").strip().lower() if trade is not None else ""
            pending_side = str(trade.pending_side or "").strip().lower() if trade is not None else ""
            can_recover_pending_entry = (
                trade is not None
                and trade.opened_logged_at is None
                and bool((trade.entry_order_id or "").strip() or (trade.entry_client_order_id or "").strip())
                and trade.pending_entry_reference is not None
                and trade.pending_stop_price is not None
                and trade.size is not None
                and pending_signal in {"long", "short"}
                and pending_side in {"buy", "sell"}
            )
            if can_recover_pending_entry:
                try:
                    pending_status = session.engine._get_order_with_retry(
                        credentials,
                        session.config,
                        inst_id=trade_inst_id,
                        ord_id=(trade.entry_order_id or "").strip() or None,
                        cl_ord_id=(trade.entry_client_order_id or "").strip() or None,
                    )
                except Exception as exc:
                    if not auto:
                        self._enqueue_log(f"{session.log_prefix} 回查未成交挂单失败，暂时无法恢复接管：{exc}")
                    return False
                pending_state = str(pending_status.state or "").strip().lower()
                if pending_state == "filled":
                    try:
                        positions = self._load_recoverable_live_positions(credentials, session.config, inst_type=inst_type)
                        live_position = self._select_recoverable_live_position(session, positions)
                    except Exception:
                        live_position = None
                if pending_state == "live":
                    try:
                        trade_instrument = session.engine._get_instrument_with_retry(trade_inst_id)
                    except Exception as exc:
                        if not auto:
                            self._enqueue_log(f"{session.log_prefix} 读取交易标的信息失败，无法恢复挂单接管：{exc}")
                        return False

                    def _monitor_pending() -> None:
                        session.engine._logger(
                            "检测到现有 OKX 未成交挂单，开始恢复动态挂单接管"
                            f" | 标的={trade_inst_id}"
                            f" | ordId={trade.entry_order_id or '-'}"
                            f" | clOrdId={trade.entry_client_order_id or '-'}"
                            f" | 方向={pending_signal.upper()}"
                            f" | 数量={format_decimal(trade.size)}"
                            f" | 挂单价={format_decimal(trade.pending_entry_reference)}"
                        )
                        session.engine.resume_dynamic_exchange_pending_order(
                            credentials,
                            session.config,
                            trade_instrument,
                            ord_id=(pending_status.ord_id or trade.entry_order_id or "").strip(),
                            cl_ord_id=(trade.entry_client_order_id or "").strip() or None,
                            candle_ts=int((trade.signal_bar_at or session.started_at).timestamp() * 1000),
                            entry_reference=trade.pending_entry_reference,
                            stop_loss=trade.pending_stop_price,
                            take_profit=trade.pending_take_profit or trade.pending_entry_reference,
                            size=trade.size,
                            side=pending_side,
                            signal=pending_signal,
                            stop_loss_algo_cl_ord_id=(trade.protective_algo_cl_ord_id or "").strip() or None,
                            stop_loss_algo_id=(trade.protective_algo_id or "").strip() or None,
                        )

                    def _run_pending_recovery() -> None:
                        try:
                            _monitor_pending()
                        except Exception as exc:
                            session.engine._notify_error(session.config, str(exc))
                            session.engine._logger(f"策略停止，原因：{exc}")
                        finally:
                            session.engine._stop_event.set()

                    session.active_trade = trade
                    session.status = "恢复中"
                    session.runtime_status = "恢复中"
                    session.stopped_at = None
                    session.ended_reason = "恢复中"
                    self._upsert_recoverable_strategy_session(session)
                    self._upsert_session_row(session)
                    self._sync_strategy_history_from_session(session)
                    try:
                        session.engine.start_custom(
                            _run_pending_recovery,
                            thread_name=f"okx-{session.config.strategy_id}-recover-pending",
                        )
                    except Exception as exc:
                        session.status = "待恢复"
                        session.runtime_status = "待恢复"
                        session.stopped_at = datetime.now()
                        session.ended_reason = "恢复启动失败"
                        self._upsert_session_row(session)
                        self._sync_strategy_history_from_session(session)
                        if not auto:
                            self._enqueue_log(f"{session.log_prefix} 恢复挂单接管启动失败：{exc}")
                        return False
                    return True
            if trade is None and not auto:
                self._enqueue_log(f"{session.log_prefix} 未能从独立日志恢复最近一笔持仓快照，且当前未检测到可接管仓位。")
            session.status = "已停止"
            session.runtime_status = "已停止"
            session.stopped_at = datetime.now()
            session.ended_reason = "恢复时未检测到可接管仓位"
            self._remove_recoverable_strategy_session(session.session_id)
            self._upsert_session_row(session)
            self._sync_strategy_history_from_session(session)
            self._log_session_message(session, "恢复接管结束：当前未检测到可接管仓位。")
            return False
        try:
            trade_instrument = session.engine._get_instrument_with_retry(trade_inst_id)
        except Exception as exc:
            if not auto:
                self._enqueue_log(f"{session.log_prefix} 读取交易标的信息失败，无法恢复接管：{exc}")
            return False
        direction = self._recoverable_position_direction(live_position)
        if direction is None:
            if not auto:
                self._enqueue_log(f"{session.log_prefix} 无法判断当前持仓方向，恢复接管已跳过。")
            return False
        if trade is None:
            trade = StrategyTradeRuntimeState(
                round_id=f"{session.session_id}-recovered-{datetime.now().strftime('%Y%m%d%H%M%S%f')}",
                opened_logged_at=session.started_at or datetime.now(),
                entry_price=live_position.avg_price or live_position.mark_price or live_position.last_price,
                size=abs(
                    live_position.avail_position
                    if live_position.avail_position not in {None, Decimal("0")}
                    else live_position.position
                ),
            )
        entry_price = trade.entry_price or live_position.avg_price or live_position.mark_price or live_position.last_price
        if entry_price is None or entry_price <= 0:
            if not auto:
                self._enqueue_log(f"{session.log_prefix} 缺少有效开仓价，恢复接管已跳过。")
            return False
        size = abs(live_position.avail_position if live_position.avail_position not in {None, Decimal('0')} else live_position.position)
        position = FilledPosition(
            ord_id=trade.entry_order_id or f"recovered-{session.session_id}",
            cl_ord_id=trade.entry_client_order_id or None,
            inst_id=trade_inst_id,
            side="buy" if direction == "long" else "sell",
            close_side="sell" if direction == "long" else "buy",
            pos_side=direction if session.config.position_mode == "long_short" else None,
            size=size,
            entry_price=entry_price,
            entry_ts=int((trade.opened_logged_at or session.started_at).timestamp() * 1000),
        )
        take_profit_mode = str(session.config.take_profit_mode or "").strip().lower()
        fallback_to_managed_monitor = False
        if take_profit_mode == "dynamic":
            protective_order = self._find_recoverable_protective_order(
                credentials,
                session,
                inst_type=inst_type,
                inst_id=trade_inst_id,
                direction=direction,
            )
            if protective_order is not None:
                stop_price = protective_order.stop_loss_trigger_price or protective_order.trigger_price
                if stop_price is not None:
                    if trade.initial_stop_price is None:
                        trade.initial_stop_price = stop_price
                    if trade.current_stop_price is None:
                        trade.current_stop_price = stop_price
                if not trade.protective_algo_id:
                    trade.protective_algo_id = (protective_order.algo_id or "").strip() or None
                if not trade.protective_algo_cl_ord_id:
                    trade.protective_algo_cl_ord_id = (
                        (protective_order.algo_client_order_id or protective_order.client_order_id or "").strip() or None
                    )
            if trade.initial_stop_price is None or not (trade.protective_algo_id or trade.protective_algo_cl_ord_id):
                fallback_to_managed_monitor = True
                start_message = (
                    "检测到现有 OKX 仓位，但未找到可接管的止损算法单，恢复为托管持仓监控"
                    f" | 标的={trade_inst_id}"
                    f" | 方向={direction.upper()}"
                    f" | 数量={format_decimal(size)}"
                )

                def _monitor() -> None:
                    session.engine._monitor_exchange_managed_position_until_closed(
                        credentials,
                        session.config,
                        trade_instrument=trade_instrument,
                        position=position,
                    )
            else:
                start_message = (
                    "检测到现有 OKX 仓位，开始恢复动态止损接管"
                    f" | 标的={trade_inst_id}"
                    f" | 方向={direction.upper()}"
                    f" | 数量={format_decimal(size)}"
                    f" | 当前止损={format_decimal(trade.current_stop_price or trade.initial_stop_price)}"
                    f" | algoClOrdId={trade.protective_algo_cl_ord_id or '-'}"
                )

                def _monitor() -> None:
                    session.engine._monitor_exchange_dynamic_stop(
                        credentials,
                        session.config,
                        trade_instrument=trade_instrument,
                        position=position,
                        initial_stop_loss=trade.initial_stop_price,
                        stop_loss_algo_cl_ord_id=trade.protective_algo_cl_ord_id,
                        stop_loss_algo_id=trade.protective_algo_id,
                    )
        else:
            start_message = (
                "检测到现有 OKX 仓位，开始恢复托管持仓监控"
                f" | 标的={trade_inst_id}"
                f" | 方向={direction.upper()}"
                f" | 数量={format_decimal(size)}"
            )

            def _monitor() -> None:
                session.engine._monitor_exchange_managed_position_until_closed(
                    credentials,
                    session.config,
                    trade_instrument=trade_instrument,
                    position=position,
                )

        def _run_recovery() -> None:
            try:
                session.engine._logger(start_message)
                _monitor()
            except Exception as exc:
                session.engine._notify_error(session.config, str(exc))
                session.engine._logger(f"策略停止，原因：{exc}")
            finally:
                session.engine._stop_event.set()

        session.active_trade = trade
        session.status = "恢复中"
        session.runtime_status = "恢复中"
        session.stopped_at = None
        session.ended_reason = "恢复为托管持仓监控" if fallback_to_managed_monitor else "恢复中"
        self._upsert_recoverable_strategy_session(session)
        self._upsert_session_row(session)
        self._sync_strategy_history_from_session(session)
        try:
            session.engine.start_custom(
                _run_recovery,
                thread_name=f"okx-{session.config.strategy_id}-recover",
            )
        except Exception as exc:
            session.status = "待恢复"
            session.runtime_status = "待恢复"
            session.stopped_at = datetime.now()
            session.ended_reason = "恢复启动失败"
            self._upsert_session_row(session)
            self._sync_strategy_history_from_session(session)
            if not auto:
                self._enqueue_log(f"{session.log_prefix} 恢复接管启动失败：{exc}")
            return False
        return True

    def _track_session_trade_runtime_with_observed_at(
        self,
        session: StrategySession,
        message: str,
        *,
        observed_at: datetime,
    ) -> None:
        signal_bar_at = _extract_session_bar_time(message)
        if "准备挂单" in message:
            trade = self._ensure_session_trade_runtime(session, observed_at=observed_at, signal_bar_at=signal_bar_at)
            entry_reference = _extract_log_field_decimal(message, "开仓价")
            if entry_reference is not None:
                trade.pending_entry_reference = entry_reference
            stop_price = _extract_log_field_decimal(message, "止损")
            if stop_price is not None:
                trade.pending_stop_price = stop_price
            take_profit = _extract_log_field_decimal(message, "止盈")
            if take_profit is not None:
                trade.pending_take_profit = take_profit
            size = _extract_log_field_decimal(message, "数量")
            if size is not None:
                trade.size = size
            direction = _extract_log_field(message, "方向")
            if direction:
                direction_norm = direction.strip().lower()
                if direction_norm in {"long", "short"}:
                    trade.pending_signal = direction_norm
                    trade.pending_side = "buy" if direction_norm == "long" else "sell"
            return
        if "挂单已提交到 OKX" in message:
            trade = self._ensure_session_trade_runtime(session, observed_at=observed_at, signal_bar_at=signal_bar_at)
            entry_order_id = _extract_log_field(message, "ordId")
            if entry_order_id:
                trade.entry_order_id = entry_order_id
            return
        if "委托追踪" in message:
            trade = self._ensure_session_trade_runtime(session, observed_at=observed_at, signal_bar_at=signal_bar_at)
            client_order_id = _extract_log_field(message, "clOrdId")
            if client_order_id:
                trade.entry_client_order_id = client_order_id
            return
        if "挂单已成交" in message:
            trade = self._ensure_session_trade_runtime(session, observed_at=observed_at, signal_bar_at=signal_bar_at)
            trade.opened_logged_at = observed_at
            entry_order_id = _extract_log_field(message, "ordId")
            if entry_order_id:
                trade.entry_order_id = entry_order_id
            entry_price = _extract_log_field_decimal(message, "开仓价")
            if entry_price is not None:
                trade.entry_price = entry_price
            size = _extract_log_field_decimal(message, "数量")
            if size is not None:
                trade.size = size
            if trade.pending_entry_reference is None and entry_price is not None:
                trade.pending_entry_reference = entry_price
            QuantApp._trader_desk_sync_open_trade_state(self, session)
            return
        if "交易员虚拟止损监控启动" in message:
            trade = session.active_trade
            if trade is None:
                return
            stop_price = _extract_log_field_decimal(message, "策略止损")
            if stop_price is not None:
                if trade.initial_stop_price is None:
                    trade.initial_stop_price = stop_price
                trade.current_stop_price = stop_price
            return
        if "交易员动态止盈保护价已上移" in message:
            trade = session.active_trade
            if trade is None:
                return
            new_stop = _extract_log_field_decimal(message, "新保护价") or _extract_log_field_decimal(message, "保护价")
            if new_stop is not None:
                trade.current_stop_price = new_stop
            return
        if "交易员虚拟止损已触发（不平仓）" in message:
            trade = session.active_trade
            if trade is None:
                return
            stop_price = _extract_log_field_decimal(message, "策略止损")
            if stop_price is not None:
                trade.current_stop_price = stop_price
            return
        if "初始 OKX 止损已提交" in message:
            trade = session.active_trade
            if trade is None:
                return
            algo_cl_ord_id = _extract_log_field(message, "algoClOrdId")
            if algo_cl_ord_id:
                trade.protective_algo_cl_ord_id = algo_cl_ord_id
            stop_price = _extract_log_field_decimal(message, "止损")
            if stop_price is not None:
                if trade.initial_stop_price is None:
                    trade.initial_stop_price = stop_price
                trade.current_stop_price = stop_price
            return
        if "OKX 动态止损已上移" in message:
            trade = session.active_trade
            if trade is None:
                return
            new_stop = _extract_log_field_decimal(message, "新止损") or _extract_log_field_decimal(message, "止损")
            if new_stop is not None:
                trade.current_stop_price = new_stop
            return
        if "本轮持仓已结束，继续监控下一次信号。" in message:
            trade = session.active_trade
            if trade is None or trade.reconciliation_started:
                return
            trade.reconciliation_started = True
            self._start_session_trade_reconciliation(session, trade)

    def _track_session_trade_runtime(self, session: StrategySession, message: str) -> None:
        self._track_session_trade_runtime_with_observed_at(
            session,
            message,
            observed_at=datetime.now(),
        )

    def _request_stop_strategy_session(
        self,
        session_id: str,
        *,
        ended_reason: str,
        source_label: str,
        show_dialog: bool,
    ) -> bool:
        session = self.sessions.get(session_id)
        if session is None:
            return False
        if session.stop_cleanup_in_progress:
            if show_dialog:
                messagebox.showinfo("提示", "这个策略正在执行停止清理，请稍等。")
            return False
        if not session.engine.is_running:
            if session.status in {"待恢复", "恢复中"}:
                session.stop_cleanup_in_progress = False
                session.status = "已停止"
                session.runtime_status = "已停止"
                session.stopped_at = datetime.now()
                session.ended_reason = ended_reason
                session.active_trade = None
                self._remove_recoverable_strategy_session(session.session_id)
                self._upsert_session_row(session)
                self._refresh_selected_session_details()
                self._sync_strategy_history_from_session(session)
                self._log_session_message(session, f"{source_label}，已放弃恢复接管并标记为已停止。")
                return True
            if show_dialog:
                messagebox.showinfo("提示", "这个策略已经停止了。")
            return False
        if session.config.run_mode == "signal_only":
            self._stop_sessions_by_id([session.session_id])
            return True

        credentials = self._credentials_for_profile_or_none(session.api_name)
        session.status = "停止中"
        session.stop_cleanup_in_progress = True
        session.stop_result_show_dialog = show_dialog
        session.ended_reason = ended_reason
        session.engine.stop()
        self._upsert_session_row(session)
        self._refresh_selected_session_details()
        self._sync_strategy_history_from_session(session)
        self._log_session_message(session, f"{source_label}，正在检查本策略委托与持仓。")
        if credentials is None:
            session.stop_cleanup_in_progress = False
            session.status = "已停止"
            session.stopped_at = datetime.now()
            session.ended_reason = f"{ended_reason}（未找到对应API凭证，未执行撤单检查）"
            self._remove_recoverable_strategy_session(session.session_id)
            self._upsert_session_row(session)
            self._refresh_selected_session_details()
            self._sync_strategy_history_from_session(session)
            self._log_session_message(session, "停止清理失败：未找到该会话对应的 API 凭证，请人工检查委托与仓位。")
            if show_dialog:
                messagebox.showwarning(
                    "停止提醒",
                    "策略线程已收到停止请求，但当前找不到该会话对应的 API 凭证。\n\n请人工检查：\n- 当前委托是否还有残留\n- 是否已经成交并留下仓位",
                )
            return False
        threading.Thread(
            target=self._stop_session_cleanup_worker,
            args=(session.session_id, credentials),
            daemon=True,
        ).start()
        return True

    def _history_record_from_payload(self, payload: dict[str, object]) -> StrategyHistoryRecord | None:
        started_at = _parse_datetime_snapshot(payload.get("started_at"))
        if started_at is None:
            return None
        config_snapshot = payload.get("config_snapshot")
        return StrategyHistoryRecord(
            record_id=str(payload.get("record_id", "")).strip(),
            session_id=str(payload.get("session_id", "")).strip(),
            api_name=str(payload.get("api_name", "")).strip(),
            strategy_id=str(payload.get("strategy_id", "")).strip(),
            strategy_name=str(payload.get("strategy_name", "")).strip(),
            symbol=str(payload.get("symbol", "")).strip(),
            direction_label=str(payload.get("direction_label", "")).strip(),
            run_mode_label=str(payload.get("run_mode_label", "")).strip(),
            status=str(payload.get("status", "")).strip() or "已停止",
            started_at=started_at,
            stopped_at=_parse_datetime_snapshot(payload.get("stopped_at")),
            ended_reason=str(payload.get("ended_reason", "")).strip(),
            log_file_path=str(payload.get("log_file_path", "")).strip(),
            updated_at=_parse_datetime_snapshot(payload.get("updated_at")),
            config_snapshot=dict(config_snapshot) if isinstance(config_snapshot, dict) else {},
            trade_count=max(0, int(payload.get("trade_count", 0) or 0)),
            win_count=max(0, int(payload.get("win_count", 0) or 0)),
            gross_pnl_total=_parse_decimal_snapshot(payload.get("gross_pnl_total")),
            fee_total=_parse_decimal_snapshot(payload.get("fee_total")),
            funding_total=_parse_decimal_snapshot(payload.get("funding_total")),
            net_pnl_total=_parse_decimal_snapshot(payload.get("net_pnl_total")),
            last_close_reason=str(payload.get("last_close_reason", "")).strip(),
        )

    @staticmethod
    def _history_record_payload(record: StrategyHistoryRecord) -> dict[str, object]:
        return {
            "record_id": record.record_id,
            "session_id": record.session_id,
            "api_name": record.api_name,
            "strategy_id": record.strategy_id,
            "strategy_name": record.strategy_name,
            "symbol": record.symbol,
            "direction_label": record.direction_label,
            "run_mode_label": record.run_mode_label,
            "status": record.status,
            "started_at": record.started_at.isoformat(timespec="seconds"),
            "stopped_at": record.stopped_at.isoformat(timespec="seconds") if record.stopped_at is not None else None,
            "ended_reason": record.ended_reason,
            "log_file_path": record.log_file_path,
            "updated_at": record.updated_at.isoformat(timespec="seconds") if record.updated_at is not None else None,
            "config_snapshot": dict(record.config_snapshot),
            "trade_count": record.trade_count,
            "win_count": record.win_count,
            "gross_pnl_total": format(record.gross_pnl_total, "f"),
            "fee_total": format(record.fee_total, "f"),
            "funding_total": format(record.funding_total, "f"),
            "net_pnl_total": format(record.net_pnl_total, "f"),
            "last_close_reason": record.last_close_reason,
        }

    def _sort_strategy_history_records(self) -> None:
        self._strategy_history_records.sort(
            key=lambda item: (item.started_at.isoformat(timespec="seconds"), item.record_id),
            reverse=True,
        )

    def _save_strategy_history_records(self) -> None:
        self._sort_strategy_history_records()
        try:
            save_strategy_history_snapshot(
                [self._history_record_payload(record) for record in self._strategy_history_records]
            )
        except Exception as exc:
            self._enqueue_log(f"保存策略历史失败：{exc}")

    def _trade_ledger_record_from_payload(self, payload: dict[str, object]) -> StrategyTradeLedgerRecord | None:
        closed_at = _parse_datetime_snapshot(payload.get("closed_at"))
        if closed_at is None:
            return None
        return StrategyTradeLedgerRecord(
            record_id=str(payload.get("record_id", "")).strip(),
            history_record_id=str(payload.get("history_record_id", "")).strip(),
            session_id=str(payload.get("session_id", "")).strip(),
            api_name=str(payload.get("api_name", "")).strip(),
            strategy_id=str(payload.get("strategy_id", "")).strip(),
            strategy_name=str(payload.get("strategy_name", "")).strip(),
            symbol=str(payload.get("symbol", "")).strip(),
            direction_label=str(payload.get("direction_label", "")).strip(),
            run_mode_label=str(payload.get("run_mode_label", "")).strip(),
            environment=str(payload.get("environment", "")).strip(),
            signal_bar_at=_parse_datetime_snapshot(payload.get("signal_bar_at")),
            opened_at=_parse_datetime_snapshot(payload.get("opened_at")),
            closed_at=closed_at,
            entry_order_id=str(payload.get("entry_order_id", "")).strip(),
            entry_client_order_id=str(payload.get("entry_client_order_id", "")).strip(),
            exit_order_id=str(payload.get("exit_order_id", "")).strip(),
            protective_algo_id=str(payload.get("protective_algo_id", "")).strip(),
            protective_algo_cl_ord_id=str(payload.get("protective_algo_cl_ord_id", "")).strip(),
            entry_price=_parse_decimal_snapshot(payload.get("entry_price"), default=None) if payload.get("entry_price") else None,
            exit_price=_parse_decimal_snapshot(payload.get("exit_price"), default=None) if payload.get("exit_price") else None,
            size=_parse_decimal_snapshot(payload.get("size"), default=None) if payload.get("size") else None,
            entry_fee=_parse_decimal_snapshot(payload.get("entry_fee"), default=None) if payload.get("entry_fee") else None,
            exit_fee=_parse_decimal_snapshot(payload.get("exit_fee"), default=None) if payload.get("exit_fee") else None,
            funding_fee=_parse_decimal_snapshot(payload.get("funding_fee"), default=None) if payload.get("funding_fee") else None,
            gross_pnl=_parse_decimal_snapshot(payload.get("gross_pnl"), default=None) if payload.get("gross_pnl") else None,
            net_pnl=_parse_decimal_snapshot(payload.get("net_pnl"), default=None) if payload.get("net_pnl") else None,
            close_reason=str(payload.get("close_reason", "")).strip(),
            reason_confidence=str(payload.get("reason_confidence", "")).strip() or "low",
            summary_note=str(payload.get("summary_note", "")).strip(),
            updated_at=_parse_datetime_snapshot(payload.get("updated_at")),
        )

    @staticmethod
    def _trade_ledger_payload(record: StrategyTradeLedgerRecord) -> dict[str, object]:
        return {
            "record_id": record.record_id,
            "history_record_id": record.history_record_id,
            "session_id": record.session_id,
            "api_name": record.api_name,
            "strategy_id": record.strategy_id,
            "strategy_name": record.strategy_name,
            "symbol": record.symbol,
            "direction_label": record.direction_label,
            "run_mode_label": record.run_mode_label,
            "environment": record.environment,
            "signal_bar_at": record.signal_bar_at.isoformat(timespec="seconds") if record.signal_bar_at is not None else None,
            "opened_at": record.opened_at.isoformat(timespec="seconds") if record.opened_at is not None else None,
            "closed_at": record.closed_at.isoformat(timespec="seconds"),
            "entry_order_id": record.entry_order_id,
            "entry_client_order_id": record.entry_client_order_id,
            "exit_order_id": record.exit_order_id,
            "protective_algo_id": record.protective_algo_id,
            "protective_algo_cl_ord_id": record.protective_algo_cl_ord_id,
            "entry_price": format(record.entry_price, "f") if record.entry_price is not None else None,
            "exit_price": format(record.exit_price, "f") if record.exit_price is not None else None,
            "size": format(record.size, "f") if record.size is not None else None,
            "entry_fee": format(record.entry_fee, "f") if record.entry_fee is not None else None,
            "exit_fee": format(record.exit_fee, "f") if record.exit_fee is not None else None,
            "funding_fee": format(record.funding_fee, "f") if record.funding_fee is not None else None,
            "gross_pnl": format(record.gross_pnl, "f") if record.gross_pnl is not None else None,
            "net_pnl": format(record.net_pnl, "f") if record.net_pnl is not None else None,
            "close_reason": record.close_reason,
            "reason_confidence": record.reason_confidence,
            "summary_note": record.summary_note,
            "updated_at": record.updated_at.isoformat(timespec="seconds") if record.updated_at is not None else None,
        }

    def _sort_strategy_trade_ledger_records(self) -> None:
        self._strategy_trade_ledger_records.sort(
            key=lambda item: (item.closed_at.isoformat(timespec="seconds"), item.record_id),
            reverse=True,
        )

    def _save_strategy_trade_ledger_records(self) -> None:
        self._sort_strategy_trade_ledger_records()
        try:
            save_strategy_trade_ledger_snapshot(
                [self._trade_ledger_payload(record) for record in self._strategy_trade_ledger_records]
            )
        except Exception as exc:
            self._enqueue_log(f"保存策略交易账本失败：{exc}")

    def _load_strategy_trade_ledger(self) -> None:
        try:
            snapshot = load_strategy_trade_ledger_snapshot()
        except Exception as exc:
            self._enqueue_log(f"读取策略交易账本失败：{exc}")
            return
        records: list[StrategyTradeLedgerRecord] = []
        raw_records = snapshot.get("records", [])
        if isinstance(raw_records, list):
            for item in raw_records:
                if not isinstance(item, dict):
                    continue
                record = self._trade_ledger_record_from_payload(item)
                if record is not None:
                    records.append(record)
        self._strategy_trade_ledger_records = records
        self._strategy_trade_ledger_by_id = {record.record_id: record for record in records}
        self._rebuild_history_financials_from_trade_ledger()

    def _next_strategy_trade_ledger_record_id(self, session: StrategySession, closed_at: datetime) -> str:
        base = f"{closed_at.strftime('%Y%m%d%H%M%S%f')}-{session.session_id}"
        record_id = base
        suffix = 2
        while record_id in self._strategy_trade_ledger_by_id:
            record_id = f"{base}-{suffix}"
            suffix += 1
        return record_id

    def _upsert_strategy_trade_ledger_record(self, record: StrategyTradeLedgerRecord) -> None:
        existing = self._strategy_trade_ledger_by_id.get(record.record_id)
        self._strategy_trade_ledger_by_id[record.record_id] = record
        if existing is None:
            self._strategy_trade_ledger_records.append(record)
        else:
            for index, item in enumerate(self._strategy_trade_ledger_records):
                if item.record_id == record.record_id:
                    self._strategy_trade_ledger_records[index] = record
                    break
        self._save_strategy_trade_ledger_records()

    def _apply_financial_totals(self, target: StrategySession | StrategyHistoryRecord, records: list[StrategyTradeLedgerRecord]) -> None:
        target.trade_count = len(records)
        target.win_count = sum(1 for item in records if (item.net_pnl or Decimal("0")) > 0)
        target.gross_pnl_total = sum(((item.gross_pnl or Decimal("0")) for item in records), Decimal("0"))
        target.fee_total = sum(
            (((item.entry_fee or Decimal("0")) + (item.exit_fee or Decimal("0"))) for item in records),
            Decimal("0"),
        )
        target.funding_total = sum(((item.funding_fee or Decimal("0")) for item in records), Decimal("0"))
        target.net_pnl_total = sum(((item.net_pnl or Decimal("0")) for item in records), Decimal("0"))
        target.last_close_reason = records[0].close_reason if records else ""

    def _rebuild_history_financials_from_trade_ledger(self) -> None:
        grouped: dict[str, list[StrategyTradeLedgerRecord]] = {}
        for record in self._strategy_trade_ledger_records:
            key = record.history_record_id.strip()
            if not key:
                continue
            grouped.setdefault(key, []).append(record)
        changed = False
        for record in self._strategy_history_records:
            matched = grouped.get(record.record_id, [])
            previous = (
                record.trade_count,
                record.win_count,
                record.gross_pnl_total,
                record.fee_total,
                record.funding_total,
                record.net_pnl_total,
                record.last_close_reason,
            )
            self._apply_financial_totals(record, matched)
            current = (
                record.trade_count,
                record.win_count,
                record.gross_pnl_total,
                record.fee_total,
                record.funding_total,
                record.net_pnl_total,
                record.last_close_reason,
            )
            if current != previous:
                record.updated_at = datetime.now()
                changed = True
        if changed:
            self._save_strategy_history_records()

    def _refresh_session_financials_from_trade_ledger(self, session: StrategySession) -> None:
        if session.history_record_id:
            matched = [
                record
                for record in self._strategy_trade_ledger_records
                if record.history_record_id == session.history_record_id
            ]
        else:
            matched = [record for record in self._strategy_trade_ledger_records if record.session_id == session.session_id]
        self._apply_financial_totals(session, matched)
        self._upsert_session_row(session)
        self._refresh_selected_session_details()
        self._sync_strategy_history_from_session(session)

    def _load_strategy_history(self) -> None:
        try:
            snapshot = load_strategy_history_snapshot()
        except Exception as exc:
            self._enqueue_log(f"读取策略历史失败：{exc}")
            return
        records: list[StrategyHistoryRecord] = []
        raw_records = snapshot.get("records", [])
        if isinstance(raw_records, list):
            for item in raw_records:
                if not isinstance(item, dict):
                    continue
                record = self._history_record_from_payload(item)
                if record is not None:
                    records.append(record)
        self._strategy_history_records = records
        self._strategy_history_by_id = {record.record_id: record for record in records}
        for record in records:
            self._update_session_counter_from_session_id(record.session_id)
        recoverable_count, abnormal_count = self._mark_unfinished_strategy_history_records()
        if recoverable_count:
            self._enqueue_log(f"检测到 {recoverable_count} 条可恢复历史策略，已标记为待恢复。")
        if abnormal_count:
            self._enqueue_log(f"检测到 {abnormal_count} 条未正常结束的历史策略，已自动标记为异常结束。")

    def _mark_unfinished_strategy_history_records(self) -> tuple[int, int]:
        recovered_at = datetime.now()
        recoverable_count = 0
        abnormal_count = 0
        for record in self._strategy_history_records:
            if record.status not in {"运行中", "停止中"}:
                continue
            if record.session_id in self._recoverable_strategy_sessions:
                record.status = "待恢复"
                if not record.ended_reason:
                    record.ended_reason = "应用关闭后待恢复接管"
                recoverable_count += 1
            else:
                record.status = "异常结束"
                if not record.ended_reason:
                    record.ended_reason = "应用异常退出"
                abnormal_count += 1
            if record.stopped_at is None:
                record.stopped_at = recovered_at
            record.updated_at = recovered_at
        if recoverable_count or abnormal_count:
            self._save_strategy_history_records()
        return recoverable_count, abnormal_count

    def _next_strategy_history_record_id(self, session: StrategySession) -> str:
        base = f"{session.started_at.strftime('%Y%m%d%H%M%S%f')}-{session.session_id}"
        record_id = base
        suffix = 2
        while record_id in self._strategy_history_by_id:
            record_id = f"{base}-{suffix}"
            suffix += 1
        return record_id

    def _build_strategy_history_record(self, session: StrategySession) -> StrategyHistoryRecord:
        record_id = session.history_record_id or self._next_strategy_history_record_id(session)
        return StrategyHistoryRecord(
            record_id=record_id,
            session_id=session.session_id,
            api_name=session.api_name,
            strategy_id=session.strategy_id,
            strategy_name=session.strategy_name,
            symbol=session.symbol,
            direction_label=session.direction_label,
            run_mode_label=session.run_mode_label,
            status=session.status,
            started_at=session.started_at,
            stopped_at=session.stopped_at,
            ended_reason=session.ended_reason,
            log_file_path=str(session.log_file_path) if session.log_file_path is not None else "",
            updated_at=datetime.now(),
            config_snapshot=_serialize_strategy_config_snapshot(session.config),
            trade_count=session.trade_count,
            win_count=session.win_count,
            gross_pnl_total=session.gross_pnl_total,
            fee_total=session.fee_total,
            funding_total=session.funding_total,
            net_pnl_total=session.net_pnl_total,
            last_close_reason=session.last_close_reason,
        )

    def _upsert_strategy_history_record(self, record: StrategyHistoryRecord) -> None:
        existing = self._strategy_history_by_id.get(record.record_id)
        self._strategy_history_by_id[record.record_id] = record
        if existing is None:
            self._strategy_history_records.append(record)
        else:
            for index, item in enumerate(self._strategy_history_records):
                if item.record_id == record.record_id:
                    self._strategy_history_records[index] = record
                    break
        self._save_strategy_history_records()
        self._render_strategy_history_view()

    def _record_strategy_session_started(self, session: StrategySession) -> None:
        record = self._build_strategy_history_record(session)
        session.history_record_id = record.record_id
        self._upsert_strategy_history_record(record)

    def _sync_strategy_history_from_session(self, session: StrategySession) -> None:
        if not session.history_record_id:
            return
        record = self._strategy_history_by_id.get(session.history_record_id)
        if record is None:
            self._record_strategy_session_started(session)
            return
        desired_snapshot = _serialize_strategy_config_snapshot(session.config)
        changed = False
        for attr, desired in (
            ("session_id", session.session_id),
            ("api_name", session.api_name),
            ("strategy_id", session.strategy_id),
            ("strategy_name", session.strategy_name),
            ("symbol", session.symbol),
            ("direction_label", session.direction_label),
            ("run_mode_label", session.run_mode_label),
            ("status", session.status),
            ("started_at", session.started_at),
            ("stopped_at", session.stopped_at),
            ("ended_reason", session.ended_reason),
            ("log_file_path", str(session.log_file_path) if session.log_file_path is not None else ""),
            ("trade_count", session.trade_count),
            ("win_count", session.win_count),
            ("gross_pnl_total", session.gross_pnl_total),
            ("fee_total", session.fee_total),
            ("funding_total", session.funding_total),
            ("net_pnl_total", session.net_pnl_total),
            ("last_close_reason", session.last_close_reason),
        ):
            if getattr(record, attr) != desired:
                setattr(record, attr, desired)
                changed = True
        if record.config_snapshot != desired_snapshot:
            record.config_snapshot = desired_snapshot
            changed = True
        if not changed:
            return
        record.updated_at = datetime.now()
        self._upsert_strategy_history_record(record)

    def open_strategy_history_window(self) -> None:
        if self._strategy_history_window is not None and _widget_exists(self._strategy_history_window):
            self._strategy_history_window.focus_force()
            self._render_strategy_history_view()
            return

        window = Toplevel(self.root)
        apply_window_icon(window)
        window.title("历史策略")
        apply_adaptive_window_geometry(
            window,
            width_ratio=0.76,
            height_ratio=0.72,
            min_width=1080,
            min_height=640,
            max_width=1680,
            max_height=1080,
        )
        self._strategy_history_window = window
        window.protocol("WM_DELETE_WINDOW", self._close_strategy_history_window)

        container = ttk.Frame(window, padding=12)
        container.pack(fill="both", expand=True)
        container.columnconfigure(0, weight=1)
        container.rowconfigure(1, weight=1)
        container.rowconfigure(2, weight=1)

        header = ttk.Frame(container)
        header.grid(row=0, column=0, sticky="ew", pady=(0, 10))
        header.columnconfigure(0, weight=1)
        ttk.Label(
            header,
            text=f"历史策略会永久保存到：{strategy_history_file_path()}",
            justify="left",
        ).grid(row=0, column=0, sticky="w")
        action_row = ttk.Frame(header)
        action_row.grid(row=0, column=1, sticky="e")
        ttk.Button(action_row, text="删除选中", command=self.delete_selected_strategy_history_record).grid(
            row=0, column=0, padx=(0, 6)
        )
        ttk.Button(action_row, text="清空历史", command=self.clear_strategy_history_records).grid(
            row=0, column=1, padx=(0, 6)
        )
        ttk.Button(action_row, text="打开日志", command=self.open_selected_strategy_history_log).grid(
            row=0, column=2, padx=(0, 6)
        )
        ttk.Button(action_row, text="复制日志路径", command=self.copy_selected_strategy_history_log_path).grid(
            row=0, column=3, padx=(0, 6)
        )
        ttk.Button(action_row, text="刷新", command=self._render_strategy_history_view).grid(
            row=0, column=4, padx=(0, 6)
        )
        ttk.Button(action_row, text="关闭", command=self._close_strategy_history_window).grid(row=0, column=5)

        list_frame = ttk.LabelFrame(container, text="历史策略列表", padding=12)
        list_frame.grid(row=1, column=0, sticky="nsew")
        list_frame.columnconfigure(0, weight=1)
        list_frame.rowconfigure(0, weight=1)

        self._strategy_history_tree = ttk.Treeview(
            list_frame,
            columns=("session", "api", "strategy", "symbol", "direction", "mode", "pnl", "status", "started", "stopped"),
            show="headings",
            selectmode="browse",
        )
        tree = self._strategy_history_tree
        tree.heading("session", text="会话(双击日志)")
        tree.heading("api", text="API")
        tree.heading("strategy", text="策略")
        tree.heading("symbol", text="标的(双击K线)")
        tree.heading("direction", text="方向")
        tree.heading("mode", text="模式")
        tree.heading("pnl", text="净盈亏")
        tree.heading("status", text="状态")
        tree.heading("started", text="启动时间")
        tree.heading("stopped", text="停止时间")
        tree.column("session", width=76, anchor="center")
        tree.column("api", width=88, anchor="center")
        tree.column("strategy", width=138, anchor="w")
        tree.column("symbol", width=178, anchor="w")
        tree.column("direction", width=82, anchor="center")
        tree.column("mode", width=102, anchor="center")
        tree.column("pnl", width=110, anchor="e")
        tree.column("status", width=92, anchor="center")
        tree.column("started", width=150, anchor="center")
        tree.column("stopped", width=150, anchor="center")
        tree.grid(row=0, column=0, sticky="nsew")
        tree.bind("<<TreeviewSelect>>", self._on_strategy_history_selected)
        tree.bind("<Double-1>", self._on_strategy_history_tree_double_click)
        tree.bind("<Motion>", self._on_strategy_history_tree_hover)
        tree.bind("<Leave>", self._on_strategy_history_tree_hover_leave)
        history_scroll = ttk.Scrollbar(list_frame, orient="vertical", command=tree.yview)
        history_scroll.grid(row=0, column=1, sticky="ns")
        tree.configure(yscrollcommand=history_scroll.set)

        detail_frame = ttk.LabelFrame(container, text="选中历史记录详情", padding=12)
        detail_frame.grid(row=2, column=0, sticky="nsew", pady=(10, 0))
        detail_frame.columnconfigure(0, weight=1)
        detail_frame.rowconfigure(0, weight=1)

        self._strategy_history_detail = Text(
            detail_frame,
            height=12,
            wrap="word",
            font=("Microsoft YaHei UI", 10),
            relief="flat",
        )
        self._strategy_history_detail.grid(row=0, column=0, sticky="nsew")
        detail_scroll = ttk.Scrollbar(detail_frame, orient="vertical", command=self._strategy_history_detail.yview)
        detail_scroll.grid(row=0, column=1, sticky="ns")
        self._strategy_history_detail.configure(yscrollcommand=detail_scroll.set)
        self._set_readonly_text(self._strategy_history_detail, self.strategy_history_text.get())
        self._render_strategy_history_view()

    def _close_strategy_history_window(self) -> None:
        if self._strategy_history_window is not None and _widget_exists(self._strategy_history_window):
            self._strategy_history_window.destroy()
        self._strategy_history_window = None
        self._strategy_history_tree = None
        self._strategy_history_detail = None
        self._strategy_history_selected_record_id = None

    def _selected_strategy_history_record(self) -> StrategyHistoryRecord | None:
        tree = self._strategy_history_tree
        if tree is None or not _widget_exists(tree):
            if self._strategy_history_selected_record_id is None:
                return None
            return self._strategy_history_by_id.get(self._strategy_history_selected_record_id)
        try:
            selection = tree.selection()
        except TclError:
            selection = ()
        if selection:
            self._strategy_history_selected_record_id = selection[0]
        if self._strategy_history_selected_record_id is None:
            return None
        return self._strategy_history_by_id.get(self._strategy_history_selected_record_id)

    def _session_by_history_record_id(self, record_id: str) -> StrategySession | None:
        for session in self.sessions.values():
            if session.history_record_id == record_id:
                return session
        return None

    def open_strategy_book_window(self) -> None:
        if self._strategy_book_window is not None and _widget_exists(self._strategy_book_window):
            self._strategy_book_window.focus_force()
            self._refresh_strategy_book_window()
            return

        window = Toplevel(self.root)
        apply_window_icon(window)
        window.title("普通量化策略总账本")
        apply_adaptive_window_geometry(
            window,
            width_ratio=0.82,
            height_ratio=0.76,
            min_width=1240,
            min_height=720,
            max_width=1780,
            max_height=1160,
        )
        self._strategy_book_window = window
        window.protocol("WM_DELETE_WINDOW", self._close_strategy_book_window)

        container = ttk.Frame(window, padding=12)
        container.pack(fill="both", expand=True)
        container.columnconfigure(0, weight=1)
        container.rowconfigure(2, weight=1)
        container.rowconfigure(3, weight=2)

        header = ttk.Frame(container)
        header.grid(row=0, column=0, sticky="ew", pady=(0, 10))
        header.columnconfigure(0, weight=1)
        ttk.Label(
            header,
            text=f"普通量化总账本来源：{strategy_trade_ledger_file_path()}",
            justify="left",
        ).grid(row=0, column=0, sticky="w")
        ttk.Label(header, textvariable=self.strategy_book_summary_text, justify="left").grid(
            row=1,
            column=0,
            sticky="w",
            pady=(6, 0),
        )
        action_row = ttk.Frame(header)
        action_row.grid(row=0, column=1, rowspan=2, sticky="e")
        ttk.Button(action_row, text="刷新", command=self._refresh_strategy_book_window).grid(row=0, column=0, padx=(0, 6))
        ttk.Button(action_row, text="关闭", command=self._close_strategy_book_window).grid(row=0, column=1)

        filter_frame = ttk.LabelFrame(container, text="筛选条件", padding=12)
        filter_frame.grid(row=1, column=0, sticky="ew", pady=(0, 10))
        for column in range(7):
            filter_frame.columnconfigure(column, weight=1 if column in {1, 3, 5} else 0)

        ttk.Label(filter_frame, text="API").grid(row=0, column=0, sticky="w")
        self._strategy_book_api_combo = ttk.Combobox(
            filter_frame,
            textvariable=self.strategy_book_api_filter,
            state="readonly",
            width=14,
        )
        self._strategy_book_api_combo.grid(row=0, column=1, sticky="ew", padx=(6, 12))

        ttk.Label(filter_frame, text="交易员").grid(row=0, column=2, sticky="w")
        self._strategy_book_trader_combo = ttk.Combobox(
            filter_frame,
            textvariable=self.strategy_book_trader_filter,
            state="readonly",
            width=14,
        )
        self._strategy_book_trader_combo.grid(row=0, column=3, sticky="ew", padx=(6, 12))

        ttk.Label(filter_frame, text="策略").grid(row=0, column=4, sticky="w")
        self._strategy_book_strategy_combo = ttk.Combobox(
            filter_frame,
            textvariable=self.strategy_book_strategy_filter,
            state="readonly",
            width=20,
        )
        self._strategy_book_strategy_combo.grid(row=0, column=5, sticky="ew", padx=(6, 12))

        ttk.Label(filter_frame, text="标的").grid(row=0, column=6, sticky="w")
        self._strategy_book_symbol_combo = ttk.Combobox(
            filter_frame,
            textvariable=self.strategy_book_symbol_filter,
            state="readonly",
            width=18,
        )
        self._strategy_book_symbol_combo.grid(row=0, column=7, sticky="ew", padx=(6, 0))

        filter_frame.columnconfigure(7, weight=1)
        ttk.Label(filter_frame, text="周期").grid(row=1, column=0, sticky="w", pady=(8, 0))
        self._strategy_book_bar_combo = ttk.Combobox(
            filter_frame,
            textvariable=self.strategy_book_bar_filter,
            state="readonly",
            width=14,
        )
        self._strategy_book_bar_combo.grid(row=1, column=1, sticky="ew", padx=(6, 12), pady=(8, 0))

        ttk.Label(filter_frame, text="方向").grid(row=1, column=2, sticky="w", pady=(8, 0))
        self._strategy_book_direction_combo = ttk.Combobox(
            filter_frame,
            textvariable=self.strategy_book_direction_filter,
            state="readonly",
            width=14,
        )
        self._strategy_book_direction_combo.grid(row=1, column=3, sticky="ew", padx=(6, 12), pady=(8, 0))

        ttk.Label(filter_frame, text="状态").grid(row=1, column=4, sticky="w", pady=(8, 0))
        self._strategy_book_status_combo = ttk.Combobox(
            filter_frame,
            textvariable=self.strategy_book_status_filter,
            state="readonly",
            width=20,
        )
        self._strategy_book_status_combo.grid(row=1, column=5, sticky="ew", padx=(6, 12), pady=(8, 0))

        ttk.Button(filter_frame, text="重置筛选", command=self._reset_strategy_book_filters).grid(
            row=1,
            column=7,
            sticky="e",
            pady=(8, 0),
        )

        for combo in (
            self._strategy_book_api_combo,
            self._strategy_book_trader_combo,
            self._strategy_book_strategy_combo,
            self._strategy_book_symbol_combo,
            self._strategy_book_bar_combo,
            self._strategy_book_direction_combo,
            self._strategy_book_status_combo,
        ):
            combo.bind("<<ComboboxSelected>>", self._on_strategy_book_filter_changed)

        summary_frame = ttk.LabelFrame(container, text="策略汇总", padding=12)
        summary_frame.grid(row=2, column=0, sticky="nsew")
        summary_frame.columnconfigure(0, weight=1)
        summary_frame.rowconfigure(0, weight=1)
        self._strategy_book_group_tree = ttk.Treeview(
            summary_frame,
            columns=(
                "api",
                "trader",
                "strategy",
                "symbol",
                "bar",
                "direction",
                "status",
                "trades",
                "wins",
                "losses",
                "rate",
                "gross",
                "fee",
                "funding",
                "net",
                "closed",
            ),
            show="headings",
            selectmode="browse",
        )
        group_tree = self._strategy_book_group_tree
        group_tree.heading("api", text="API")
        group_tree.heading("trader", text="交易员(双击打开)")
        group_tree.heading("strategy", text="策略")
        group_tree.heading("symbol", text="标的(双击K线)")
        group_tree.heading("bar", text="周期")
        group_tree.heading("direction", text="方向")
        group_tree.heading("status", text="状态")
        group_tree.heading("trades", text="平仓单")
        group_tree.heading("wins", text="盈利")
        group_tree.heading("losses", text="亏损")
        group_tree.heading("rate", text="胜率")
        group_tree.heading("gross", text="毛盈亏")
        group_tree.heading("fee", text="手续费")
        group_tree.heading("funding", text="资金费")
        group_tree.heading("net", text="净盈亏")
        group_tree.heading("closed", text="最近平仓")
        group_tree.column("api", width=84, anchor="center")
        group_tree.column("trader", width=92, anchor="center")
        group_tree.column("strategy", width=150, anchor="w")
        group_tree.column("symbol", width=170, anchor="w")
        group_tree.column("bar", width=68, anchor="center")
        group_tree.column("direction", width=82, anchor="center")
        group_tree.column("status", width=92, anchor="center")
        group_tree.column("trades", width=72, anchor="center")
        group_tree.column("wins", width=66, anchor="center")
        group_tree.column("losses", width=66, anchor="center")
        group_tree.column("rate", width=72, anchor="center")
        group_tree.column("gross", width=98, anchor="e")
        group_tree.column("fee", width=98, anchor="e")
        group_tree.column("funding", width=98, anchor="e")
        group_tree.column("net", width=98, anchor="e")
        group_tree.column("closed", width=150, anchor="center")
        group_tree.grid(row=0, column=0, sticky="nsew")
        group_tree.bind("<Double-1>", self._on_strategy_book_group_tree_double_click)
        group_tree.bind("<Motion>", self._on_strategy_book_tree_hover)
        group_tree.bind("<Leave>", self._on_strategy_book_tree_hover_leave)
        group_scroll = ttk.Scrollbar(summary_frame, orient="vertical", command=group_tree.yview)
        group_scroll.grid(row=0, column=1, sticky="ns")
        group_tree.configure(yscrollcommand=group_scroll.set)

        ledger_frame = ttk.LabelFrame(container, text="账本流水", padding=12)
        ledger_frame.grid(row=3, column=0, sticky="nsew", pady=(10, 0))
        ledger_frame.columnconfigure(0, weight=1)
        ledger_frame.rowconfigure(0, weight=1)
        self._strategy_book_ledger_tree = ttk.Treeview(
            ledger_frame,
            columns=(
                "closed",
                "api",
                "trader",
                "strategy",
                "symbol",
                "bar",
                "direction",
                "status",
                "session",
                "opened",
                "entry",
                "exit",
                "size",
                "gross",
                "fee",
                "funding",
                "net",
                "reason",
            ),
            show="headings",
            selectmode="browse",
        )
        ledger_tree = self._strategy_book_ledger_tree
        ledger_tree.heading("closed", text="平仓时间")
        ledger_tree.heading("api", text="API")
        ledger_tree.heading("trader", text="交易员(双击打开)")
        ledger_tree.heading("strategy", text="策略")
        ledger_tree.heading("symbol", text="标的(双击K线)")
        ledger_tree.heading("bar", text="周期")
        ledger_tree.heading("direction", text="方向")
        ledger_tree.heading("status", text="状态")
        ledger_tree.heading("session", text="会话(双击日志)")
        ledger_tree.heading("opened", text="开仓时间")
        ledger_tree.heading("entry", text="开仓价")
        ledger_tree.heading("exit", text="平仓价")
        ledger_tree.heading("size", text="数量")
        ledger_tree.heading("gross", text="毛盈亏")
        ledger_tree.heading("fee", text="手续费")
        ledger_tree.heading("funding", text="资金费")
        ledger_tree.heading("net", text="净盈亏")
        ledger_tree.heading("reason", text="原因")
        ledger_tree.column("closed", width=150, anchor="center")
        ledger_tree.column("api", width=84, anchor="center")
        ledger_tree.column("trader", width=92, anchor="center")
        ledger_tree.column("strategy", width=150, anchor="w")
        ledger_tree.column("symbol", width=170, anchor="w")
        ledger_tree.column("bar", width=68, anchor="center")
        ledger_tree.column("direction", width=82, anchor="center")
        ledger_tree.column("status", width=92, anchor="center")
        ledger_tree.column("session", width=72, anchor="center")
        ledger_tree.column("opened", width=150, anchor="center")
        ledger_tree.column("entry", width=90, anchor="e")
        ledger_tree.column("exit", width=90, anchor="e")
        ledger_tree.column("size", width=82, anchor="e")
        ledger_tree.column("gross", width=98, anchor="e")
        ledger_tree.column("fee", width=98, anchor="e")
        ledger_tree.column("funding", width=98, anchor="e")
        ledger_tree.column("net", width=98, anchor="e")
        ledger_tree.column("reason", width=220, anchor="w")
        ledger_tree.grid(row=0, column=0, sticky="nsew")
        ledger_tree.bind("<<TreeviewSelect>>", self._on_strategy_book_ledger_selected)
        ledger_tree.bind("<Double-1>", self._on_strategy_book_ledger_tree_double_click)
        ledger_tree.bind("<Motion>", self._on_strategy_book_tree_hover)
        ledger_tree.bind("<Leave>", self._on_strategy_book_tree_hover_leave)
        ledger_v_scroll = ttk.Scrollbar(ledger_frame, orient="vertical", command=ledger_tree.yview)
        ledger_v_scroll.grid(row=0, column=1, sticky="ns")
        ledger_x_scroll = ttk.Scrollbar(ledger_frame, orient="horizontal", command=ledger_tree.xview)
        ledger_x_scroll.grid(row=1, column=0, sticky="ew", pady=(6, 0))
        ledger_tree.configure(yscrollcommand=ledger_v_scroll.set, xscrollcommand=ledger_x_scroll.set)

        self._refresh_strategy_book_window()

    def _close_strategy_book_window(self) -> None:
        if self._strategy_book_window is not None and _widget_exists(self._strategy_book_window):
            self._strategy_book_window.destroy()
        self._strategy_book_window = None
        self._strategy_book_group_tree = None
        self._strategy_book_ledger_tree = None
        self._strategy_book_api_combo = None
        self._strategy_book_trader_combo = None
        self._strategy_book_strategy_combo = None
        self._strategy_book_symbol_combo = None
        self._strategy_book_bar_combo = None
        self._strategy_book_direction_combo = None
        self._strategy_book_status_combo = None

    def _current_strategy_book_filters(self) -> NormalStrategyBookFilters:
        return NormalStrategyBookFilters(
            api_name=_strategy_book_filter_normalized(
                self.strategy_book_api_filter.get(),
                STRATEGY_BOOK_FILTER_ALL_API,
            ),
            trader_label=_strategy_book_filter_normalized(
                self.strategy_book_trader_filter.get(),
                STRATEGY_BOOK_FILTER_ALL_TRADER,
            ),
            strategy_name=_strategy_book_filter_normalized(
                self.strategy_book_strategy_filter.get(),
                STRATEGY_BOOK_FILTER_ALL_STRATEGY,
            ),
            symbol=_strategy_book_filter_normalized(
                self.strategy_book_symbol_filter.get(),
                STRATEGY_BOOK_FILTER_ALL_SYMBOL,
            ),
            bar=_strategy_book_filter_normalized(
                self.strategy_book_bar_filter.get(),
                STRATEGY_BOOK_FILTER_ALL_BAR,
            ),
            direction_label=_strategy_book_filter_normalized(
                self.strategy_book_direction_filter.get(),
                STRATEGY_BOOK_FILTER_ALL_DIRECTION,
            ),
            status=_strategy_book_filter_normalized(
                self.strategy_book_status_filter.get(),
                STRATEGY_BOOK_FILTER_ALL_STATUS,
            ),
        )

    def _reset_strategy_book_filters(self) -> None:
        self.strategy_book_api_filter.set(STRATEGY_BOOK_FILTER_ALL_API)
        self.strategy_book_trader_filter.set(STRATEGY_BOOK_FILTER_ALL_TRADER)
        self.strategy_book_strategy_filter.set(STRATEGY_BOOK_FILTER_ALL_STRATEGY)
        self.strategy_book_symbol_filter.set(STRATEGY_BOOK_FILTER_ALL_SYMBOL)
        self.strategy_book_bar_filter.set(STRATEGY_BOOK_FILTER_ALL_BAR)
        self.strategy_book_direction_filter.set(STRATEGY_BOOK_FILTER_ALL_DIRECTION)
        self.strategy_book_status_filter.set(STRATEGY_BOOK_FILTER_ALL_STATUS)
        self._refresh_strategy_book_window()

    def _on_strategy_book_filter_changed(self, *_: object) -> None:
        self._refresh_strategy_book_window()

    def _refresh_strategy_book_filter_controls(self) -> None:
        options = _build_normal_strategy_book_filter_options(
            self._strategy_trade_ledger_records,
            self._strategy_history_records,
        )
        control_specs = (
            (
                self._strategy_book_api_combo,
                self.strategy_book_api_filter,
                options["api_name"],
                STRATEGY_BOOK_FILTER_ALL_API,
            ),
            (
                self._strategy_book_trader_combo,
                self.strategy_book_trader_filter,
                options["trader_label"],
                STRATEGY_BOOK_FILTER_ALL_TRADER,
            ),
            (
                self._strategy_book_strategy_combo,
                self.strategy_book_strategy_filter,
                options["strategy_name"],
                STRATEGY_BOOK_FILTER_ALL_STRATEGY,
            ),
            (
                self._strategy_book_symbol_combo,
                self.strategy_book_symbol_filter,
                options["symbol"],
                STRATEGY_BOOK_FILTER_ALL_SYMBOL,
            ),
            (
                self._strategy_book_bar_combo,
                self.strategy_book_bar_filter,
                options["bar"],
                STRATEGY_BOOK_FILTER_ALL_BAR,
            ),
            (
                self._strategy_book_direction_combo,
                self.strategy_book_direction_filter,
                options["direction_label"],
                STRATEGY_BOOK_FILTER_ALL_DIRECTION,
            ),
            (
                self._strategy_book_status_combo,
                self.strategy_book_status_filter,
                options["status"],
                STRATEGY_BOOK_FILTER_ALL_STATUS,
            ),
        )
        for combo, variable, values, default_value in control_specs:
            if combo is None or not _widget_exists(combo):
                continue
            combo.configure(values=values)
            current_value = variable.get()
            variable.set(current_value if current_value in values else default_value)

    def _refresh_strategy_book_window(self) -> None:
        if self._strategy_book_window is None or not _widget_exists(self._strategy_book_window):
            return
        self._refresh_strategy_book_filter_controls()
        filters = self._current_strategy_book_filters()
        summary = _build_normal_strategy_book_summary(
            self._strategy_trade_ledger_records,
            self._strategy_history_records,
            filters=filters,
        )
        self.strategy_book_summary_text.set(_normal_strategy_book_summary_text(summary))
        self._refresh_strategy_book_group_tree(filters=filters)
        self._refresh_strategy_book_ledger_tree(filters=filters)

    def _refresh_strategy_book_group_tree(self, *, filters: NormalStrategyBookFilters | None = None) -> None:
        tree = self._strategy_book_group_tree
        if tree is None or not _widget_exists(tree):
            return
        try:
            selected = tree.selection()
        except TclError:
            selected = ()
        selected_id = selected[0] if selected else None
        for item_id in tree.get_children():
            tree.delete(item_id)
        for row_id, values in _build_normal_strategy_book_group_rows(
            self._strategy_trade_ledger_records,
            self._strategy_history_records,
            filters=filters,
        ):
            tree.insert("", END, iid=row_id, values=values)
        if selected_id is not None and tree.exists(selected_id):
            tree.selection_set(selected_id)
            tree.focus(selected_id)
            tree.see(selected_id)

    def _refresh_strategy_book_ledger_tree(self, *, filters: NormalStrategyBookFilters | None = None) -> None:
        tree = self._strategy_book_ledger_tree
        if tree is None or not _widget_exists(tree):
            return
        try:
            selected = tree.selection()
        except TclError:
            selected = ()
        selected_id = selected[0] if selected else None
        for item_id in tree.get_children():
            tree.delete(item_id)
        for row_id, values in _build_normal_strategy_book_ledger_rows(
            self._strategy_trade_ledger_records,
            self._strategy_history_records,
            filters=filters,
        ):
            session_id = str(values[8] or "").strip()
            tree.insert("", END, iid=row_id, values=values, tags=(session_id,))
        if selected_id is not None and tree.exists(selected_id):
            tree.selection_set(selected_id)
            tree.focus(selected_id)
            tree.see(selected_id)

    def _on_strategy_book_ledger_selected(self, *_: object) -> None:
        tree = self._strategy_book_ledger_tree
        if tree is None or not _widget_exists(tree):
            return
        try:
            selection = tree.selection()
        except TclError:
            selection = ()
        if not selection:
            return
        tags = tree.item(selection[0], "tags")
        session_id = tags[0] if tags else ""
        if session_id:
            self._focus_session_row(session_id)

    @staticmethod
    def _strategy_book_tree_double_click_hint(column_id: str) -> str:
        return {
            "#2": "双击打开并定位对应交易员",
            "#4": "双击打开对应会话的实时K线图（若仍存在）",
            "#9": "双击打开对应会话的独立日志（若仍存在）",
        }.get(str(column_id or "").strip(), "")

    def _show_strategy_book_tree_hover_tip(self, text: str, *, x_root: int, y_root: int) -> None:
        if not text:
            self._hide_strategy_book_tree_hover_tip()
            return
        if self._strategy_book_tree_hover_tip_window is None or not self._strategy_book_tree_hover_tip_window.winfo_exists():
            window = Toplevel(self.root)
            window.withdraw()
            window.overrideredirect(True)
            window.attributes("-topmost", True)
            label = ttk.Label(window, text=text, padding=(8, 4), relief="solid", borderwidth=1)
            label.pack()
            self._strategy_book_tree_hover_tip_window = window
            self._strategy_book_tree_hover_tip_label = label
        else:
            window = self._strategy_book_tree_hover_tip_window
            label = self._strategy_book_tree_hover_tip_label
            if label is not None:
                label.configure(text=text)
        if window is None:
            return
        window.geometry(f"+{x_root + 12}+{y_root + 16}")
        window.deiconify()

    def _hide_strategy_book_tree_hover_tip(self) -> None:
        window = self._strategy_book_tree_hover_tip_window
        if window is not None and window.winfo_exists():
            window.withdraw()
        self._strategy_book_tree_hover_tip_column = ""

    def _on_strategy_book_tree_hover(self, event: object) -> None:
        tree = getattr(event, "widget", None)
        if tree is None:
            self._hide_strategy_book_tree_hover_tip()
            return
        try:
            x = int(getattr(event, "x", 0) or 0)
            y = int(getattr(event, "y", 0) or 0)
            x_root = int(getattr(event, "x_root", 0) or 0)
            y_root = int(getattr(event, "y_root", 0) or 0)
            region = str(tree.identify_region(x, y))
            column_id = str(tree.identify_column(x))
        except Exception:
            self._hide_strategy_book_tree_hover_tip()
            return
        if region != "heading":
            self._hide_strategy_book_tree_hover_tip()
            return
        tip_text = QuantApp._strategy_book_tree_double_click_hint(column_id)
        if not tip_text:
            self._hide_strategy_book_tree_hover_tip()
            return
        self._strategy_book_tree_hover_tip_column = column_id
        self._show_strategy_book_tree_hover_tip(tip_text, x_root=x_root, y_root=y_root)

    def _on_strategy_book_tree_hover_leave(self, *_: object) -> None:
        self._hide_strategy_book_tree_hover_tip()

    def _on_strategy_book_group_tree_double_click(self, event: object) -> str | None:
        tree = self._strategy_book_group_tree
        if tree is None or not _widget_exists(tree):
            return None
        try:
            column_id = str(tree.identify_column(getattr(event, "x", 0) or 0))
            row_id = str(tree.identify_row(getattr(event, "y", 0) or 0)).strip()
        except Exception:
            return None
        if not row_id:
            return None
        values = tree.item(row_id, "values")
        if column_id == "#2":
            trader_id = str(values[1] if len(values) > 1 else "").strip()
            if trader_id and trader_id != "-":
                self.open_trader_desk_window_for_trader(trader_id)
                return "break"
        if column_id == "#4":
            strategy_name = str(values[2] if len(values) > 2 else "").strip()
            symbol = str(values[3] if len(values) > 3 else "").strip()
            bar = str(values[4] if len(values) > 4 else "").strip()
            direction = str(values[5] if len(values) > 5 else "").strip()
            for session in self.sessions.values():
                if (
                    (session.strategy_name or "").strip() == strategy_name
                    and (session.symbol or "").strip() == symbol
                    and str(getattr(getattr(session, "config", None), "bar", "") or "").strip() == bar
                    and (session.direction_label or "").strip() == direction
                ):
                    self.open_strategy_live_chart_window(session.session_id)
                    return "break"
        return None

    def _on_strategy_book_ledger_tree_double_click(self, event: object) -> str | None:
        tree = self._strategy_book_ledger_tree
        if tree is None or not _widget_exists(tree):
            return None
        try:
            column_id = str(tree.identify_column(getattr(event, "x", 0) or 0))
            row_id = str(tree.identify_row(getattr(event, "y", 0) or 0)).strip()
        except Exception:
            return None
        if not row_id:
            return None
        values = tree.item(row_id, "values")
        tags = tree.item(row_id, "tags")
        session_id = str(tags[0] if tags else "").strip()
        if column_id == "#3":
            trader_id = str(values[2] if len(values) > 2 else "").strip()
            if trader_id and trader_id != "-":
                self.open_trader_desk_window_for_trader(trader_id)
                return "break"
        if column_id == "#5" and session_id:
            self.open_strategy_live_chart_window(session_id)
            return "break"
        if column_id == "#9" and session_id:
            self.open_strategy_session_log(session_id)
            return "break"
        return None

    @staticmethod
    def _session_blocks_history_deletion(session: StrategySession) -> bool:
        return session.engine.is_running or session.status in {"运行中", "停止中", "待恢复", "恢复中"}

    def _remove_strategy_history_records(self, record_ids: list[str], *, selected_before: str | None = None) -> tuple[int, int]:
        removed_count = 0
        blocked_count = 0
        blocked_ids: set[str] = set()
        removed_ids: set[str] = set()
        for record_id in record_ids:
            record = self._strategy_history_by_id.get(record_id)
            if record is None:
                continue
            session = self._session_by_history_record_id(record_id)
            if session is not None and self._session_blocks_history_deletion(session):
                blocked_count += 1
                blocked_ids.add(record_id)
                continue
            if session is not None:
                session.history_record_id = None
            self._strategy_history_by_id.pop(record_id, None)
            removed_count += 1
            removed_ids.add(record_id)
        if removed_count:
            self._strategy_history_records = [
                record for record in self._strategy_history_records if record.record_id not in set(record_ids) - blocked_ids
            ]
            if removed_ids:
                self._strategy_trade_ledger_records = [
                    record
                    for record in self._strategy_trade_ledger_records
                    if record.history_record_id not in removed_ids
                ]
                self._strategy_trade_ledger_by_id = {
                    record.record_id: record for record in self._strategy_trade_ledger_records
                }
                self._save_strategy_trade_ledger_records()
            self._save_strategy_history_records()
        self._render_strategy_history_view(selected_before=selected_before)
        return removed_count, blocked_count

    def delete_selected_strategy_history_record(self) -> None:
        parent = self._strategy_history_window if _widget_exists(self._strategy_history_window) else self.root
        record = self._selected_strategy_history_record()
        if record is None:
            messagebox.showinfo("提示", "请先在历史策略列表中选中一条记录。", parent=parent)
            return
        session = self._session_by_history_record_id(record.record_id)
        if session is not None and self._session_blocks_history_deletion(session):
            messagebox.showinfo(
                "提示",
                "这条历史记录对应的策略仍在运行或停止中，暂时不能删除。\n请先在运行中策略列表处理完成后再删。",
                parent=parent,
            )
            return
        confirmed = messagebox.askyesno(
            "确认删除",
            "确认删除当前选中的历史策略记录吗？\n\n只删除历史记录，不删除独立日志文件。",
            parent=parent,
        )
        if not confirmed:
            return
        removed_count, _ = self._remove_strategy_history_records([record.record_id], selected_before=record.record_id)
        if removed_count:
            self._enqueue_log(f"[历史策略 {record.record_id}] 已删除选中历史记录；独立日志文件保留。")

    def clear_strategy_history_records(self) -> None:
        parent = self._strategy_history_window if _widget_exists(self._strategy_history_window) else self.root
        if not self._strategy_history_records:
            messagebox.showinfo("提示", "当前没有可清空的历史策略记录。", parent=parent)
            return
        deletable_count = 0
        for record in self._strategy_history_records:
            session = self._session_by_history_record_id(record.record_id)
            if session is None or not self._session_blocks_history_deletion(session):
                deletable_count += 1
        if deletable_count <= 0:
            messagebox.showinfo(
                "提示",
                "当前历史列表里的记录都仍被运行中的策略占用，暂时不能清空。",
                parent=parent,
            )
            return
        confirmed = messagebox.askyesno(
            "确认清空",
            f"确认清空 {deletable_count} 条历史策略记录吗？\n\n运行中的策略记录会保留，独立日志文件不会删除。",
            parent=parent,
        )
        if not confirmed:
            return
        selected_before = self._strategy_history_selected_record_id
        record_ids = [record.record_id for record in self._strategy_history_records]
        removed_count, blocked_count = self._remove_strategy_history_records(record_ids, selected_before=selected_before)
        if removed_count:
            message = f"已清空 {removed_count} 条历史策略记录；独立日志文件保留。"
            if blocked_count:
                message += f" 另有 {blocked_count} 条运行中的记录已保留。"
            self._enqueue_log(message)

    def _selected_strategy_history_log_path(self) -> Path | None:
        record = self._selected_strategy_history_record()
        if record is None:
            return None
        return _coerce_log_file_path(record.log_file_path)

    def copy_selected_strategy_history_log_path(self) -> None:
        parent = self._strategy_history_window if _widget_exists(self._strategy_history_window) else self.root
        record = self._selected_strategy_history_record()
        if record is None:
            messagebox.showinfo("提示", "请先在历史策略列表中选中一条记录。", parent=parent)
            return
        log_path = self._selected_strategy_history_log_path()
        if log_path is None:
            messagebox.showinfo("提示", "这条历史策略还没有记录独立日志路径。", parent=parent)
            return
        self.root.clipboard_clear()
        self.root.clipboard_append(str(log_path))
        self._enqueue_log(f"[历史策略 {record.record_id}] 已复制独立日志路径：{log_path}")

    def open_selected_strategy_history_log(self) -> None:
        parent = self._strategy_history_window if _widget_exists(self._strategy_history_window) else self.root
        record = self._selected_strategy_history_record()
        if record is None:
            messagebox.showinfo("提示", "请先在历史策略列表中选中一条记录。", parent=parent)
            return
        log_path = self._selected_strategy_history_log_path()
        if log_path is None:
            messagebox.showinfo("提示", "这条历史策略还没有记录独立日志路径。", parent=parent)
            return
        if not log_path.exists():
            messagebox.showerror("打开失败", f"日志文件不存在：\n{log_path}", parent=parent)
            return
        startfile = getattr(os, "startfile", None)
        if not callable(startfile):
            messagebox.showerror("打开失败", "当前系统不支持直接打开日志文件。", parent=parent)
            return
        startfile(str(log_path))
        self._enqueue_log(f"[历史策略 {record.record_id}] 已打开独立日志：{log_path}")

    def _on_strategy_history_selected(self, *_: object) -> None:
        record = self._selected_strategy_history_record()
        self._strategy_history_selected_record_id = record.record_id if record is not None else None
        self._refresh_selected_strategy_history_details()

    @staticmethod
    def _strategy_history_tree_double_click_hint(column_id: str) -> str:
        return {
            "#1": "双击打开这条历史策略的独立日志",
            "#4": "双击打开对应会话的实时K线图（若仍存在）",
        }.get(str(column_id or "").strip(), "")

    def _show_strategy_history_tree_hover_tip(self, text: str, *, x_root: int, y_root: int) -> None:
        if not text:
            self._hide_strategy_history_tree_hover_tip()
            return
        if self._history_tree_hover_tip_window is None or not self._history_tree_hover_tip_window.winfo_exists():
            window = Toplevel(self.root)
            window.withdraw()
            window.overrideredirect(True)
            window.attributes("-topmost", True)
            label = ttk.Label(window, text=text, padding=(8, 4), relief="solid", borderwidth=1)
            label.pack()
            self._history_tree_hover_tip_window = window
            self._history_tree_hover_tip_label = label
        else:
            window = self._history_tree_hover_tip_window
            label = self._history_tree_hover_tip_label
            if label is not None:
                label.configure(text=text)
        if window is None:
            return
        window.geometry(f"+{x_root + 12}+{y_root + 16}")
        window.deiconify()

    def _hide_strategy_history_tree_hover_tip(self) -> None:
        window = self._history_tree_hover_tip_window
        if window is not None and window.winfo_exists():
            window.withdraw()
        self._history_tree_hover_tip_column = ""

    def _on_strategy_history_tree_hover(self, event: object) -> None:
        tree = self._strategy_history_tree
        if tree is None or not _widget_exists(tree):
            self._hide_strategy_history_tree_hover_tip()
            return
        try:
            x = int(getattr(event, "x", 0) or 0)
            y = int(getattr(event, "y", 0) or 0)
            x_root = int(getattr(event, "x_root", 0) or 0)
            y_root = int(getattr(event, "y_root", 0) or 0)
            region = str(tree.identify_region(x, y))
            column_id = str(tree.identify_column(x))
        except Exception:
            self._hide_strategy_history_tree_hover_tip()
            return
        if region != "heading":
            self._hide_strategy_history_tree_hover_tip()
            return
        tip_text = QuantApp._strategy_history_tree_double_click_hint(column_id)
        if not tip_text:
            self._hide_strategy_history_tree_hover_tip()
            return
        self._history_tree_hover_tip_column = column_id
        self._show_strategy_history_tree_hover_tip(tip_text, x_root=x_root, y_root=y_root)

    def _on_strategy_history_tree_hover_leave(self, *_: object) -> None:
        self._hide_strategy_history_tree_hover_tip()

    def _on_strategy_history_tree_double_click(self, event: object) -> str | None:
        tree = self._strategy_history_tree
        if tree is None or not _widget_exists(tree):
            return None
        try:
            column_id = str(tree.identify_column(getattr(event, "x", 0) or 0))
            row_id = str(tree.identify_row(getattr(event, "y", 0) or 0)).strip()
        except Exception:
            return None
        if not row_id:
            return None
        record = self._strategy_history_by_id.get(row_id)
        if record is None:
            return None
        self._strategy_history_selected_record_id = record.record_id
        try:
            tree.selection_set(record.record_id)
        except Exception:
            pass
        if column_id == "#1":
            self.open_selected_strategy_history_log()
            return "break"
        if column_id == "#4":
            session = self._session_by_history_record_id(record.record_id)
            if session is None:
                return None
            self.open_strategy_live_chart_window(session.session_id)
            return "break"
        return None

    def _refresh_selected_strategy_history_details(self) -> None:
        record = self._selected_strategy_history_record()
        if record is None:
            self.strategy_history_text.set(self._default_strategy_history_text())
            self._set_readonly_text(self._strategy_history_detail, self.strategy_history_text.get())
            return
        self.strategy_history_text.set(
            self._build_strategy_detail_text(
                record_id=record.record_id,
                session_id=record.session_id,
                api_name=record.api_name,
                status=record.status,
                strategy_id=record.strategy_id,
                strategy_name=record.strategy_name,
                symbol=record.symbol,
                direction_label=record.direction_label,
                run_mode_label=record.run_mode_label,
                started_at=record.started_at,
                stopped_at=record.stopped_at,
                ended_reason=record.ended_reason,
                updated_at=record.updated_at,
                config_snapshot=record.config_snapshot,
                log_file_path=record.log_file_path,
                trade_count=record.trade_count,
                win_count=record.win_count,
                gross_pnl_total=record.gross_pnl_total,
                fee_total=record.fee_total,
                funding_total=record.funding_total,
                net_pnl_total=record.net_pnl_total,
                last_close_reason=record.last_close_reason,
            )
        )
        self._set_readonly_text(self._strategy_history_detail, self.strategy_history_text.get())

    def _render_strategy_history_view(self, *, selected_before: str | None = None) -> None:
        tree = self._strategy_history_tree
        if tree is None or not _widget_exists(tree):
            return
        if selected_before is None:
            try:
                current_selection = tree.selection()
                selected_before = current_selection[0] if current_selection else self._strategy_history_selected_record_id
            except TclError:
                selected_before = self._strategy_history_selected_record_id
        for item_id in tree.get_children():
            tree.delete(item_id)
        for record in self._strategy_history_records:
            tree.insert(
                "",
                END,
                iid=record.record_id,
                values=(
                    record.session_id or "-",
                    record.api_name or "-",
                    record.strategy_name,
                    record.symbol,
                    record.direction_label,
                    record.run_mode_label,
                    _format_optional_usdt_precise(record.net_pnl_total, places=2),
                    record.status,
                    _format_history_datetime(record.started_at),
                    _format_history_datetime(record.stopped_at),
                ),
            )
        remaining_ids = tuple(record.record_id for record in self._strategy_history_records)
        target = self._next_history_selection_after_mutation(selected_before, remaining_ids)
        if target is not None and tree.exists(target):
            tree.selection_set(target)
            tree.focus(target)
            tree.see(target)
            self._strategy_history_selected_record_id = target
        else:
            self._strategy_history_selected_record_id = None
        self._refresh_selected_strategy_history_details()
