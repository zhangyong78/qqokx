from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QDoubleSpinBox,
    QFormLayout,
    QLabel,
    QListWidget,
    QListWidgetItem,
    QScrollArea,
    QSpinBox,
    QToolButton,
    QVBoxLayout,
    QWidget,
)

from okx_quant.area_shape_trade import AREA_DYNAMIC_TAKE_PROFIT_R, AREA_TAKER_FEE_RATE, AreaShapeConfig


_PATTERNS = (
    ("大阳线", "big_bullish", "long"),
    ("长下影", "long_lower_shadow", "long"),
    ("双线向上反转", "double_reversal_up", "long"),
    ("孕线", "inside_bar", "either"),
    ("底分型（3根）", "bottom_fractal", "long"),
    ("大阴线", "big_bearish", "short"),
    ("长上影", "long_upper_shadow", "short"),
    ("双线向下反转", "double_reversal_down", "short"),
    ("顶分型（3根）", "top_fractal", "short"),
)


class _WheelSafeDoubleSpinBox(QDoubleSpinBox):
    def wheelEvent(self, event) -> None:  # noqa: ANN001
        event.ignore()


class _WheelSafeSpinBox(QSpinBox):
    def wheelEvent(self, event) -> None:  # noqa: ANN001
        event.ignore()


class AreaShapeConfigDialog(QDialog):
    def __init__(
        self, config: AreaShapeConfig | None = None, parent=None, *,
        support_price: Decimal | None = None, pressure_price: Decimal | None = None,
    ) -> None:  # noqa: ANN001
        super().__init__(parent)
        self.setWindowTitle("智能辅助交易参数")
        self.setMinimumWidth(420)
        self._initial = config or AreaShapeConfig()
        layout = QVBoxLayout(self)
        self.scroll_area = QScrollArea()
        self.scroll_area.setWidgetResizable(True)
        self.scroll_area.setFrameShape(QScrollArea.Shape.NoFrame)
        content = QWidget()
        content_layout = QVBoxLayout(content)
        content_layout.setContentsMargins(0, 0, 6, 0)
        self.scroll_area.setWidget(content)
        layout.addWidget(self.scroll_area)
        form = QFormLayout()
        form.setFieldGrowthPolicy(QFormLayout.FieldGrowthPolicy.AllNonFixedFieldsGrow)
        form.setRowWrapPolicy(QFormLayout.RowWrapPolicy.WrapLongRows)
        content_layout.addLayout(form)
        self._pattern_selections: dict[str, set[str]] = {}
        self._pattern_direction: str | None = None

        self.direction = QComboBox()
        self.direction.addItem("做多（下方支撑入场）", "long")
        self.direction.addItem("做空（上方压力入场）", "short")
        self._set_combo(self.direction, self._initial.direction)
        self.direction.currentIndexChanged.connect(self._refresh_patterns)
        self.support_price = support_price
        self.pressure_price = pressure_price
        self.direction_note = QLabel()
        self.direction_note.setWordWrap(True)
        self.direction.currentIndexChanged.connect(self._refresh_direction_note)
        form.addRow("方向", self.direction)
        form.addRow("两线用途", self.direction_note)
        self._refresh_direction_note()

        self.entry_trigger_mode = QComboBox()
        self.entry_trigger_mode.addItem("所选1H形态触发", "pattern")
        self.entry_trigger_mode.addItem("价格触发（直接入场）", "direct_touch")
        self._set_combo(self.entry_trigger_mode, self._initial.entry_trigger_mode)
        form.addRow("入场触发", self.entry_trigger_mode)

        self.patterns = QListWidget()
        self.patterns.setMaximumHeight(126)
        self._refresh_patterns()
        self.trigger_content = QWidget()
        trigger_layout = QVBoxLayout(self.trigger_content)
        trigger_layout.setContentsMargins(0, 0, 0, 0)
        trigger_layout.addWidget(self.patterns)
        self.direct_touch_note = QLabel("OKX实盘/模拟盘：启用交易时立即向 OKX 提交入场线 GTC 限价单，不等待形态；价格已越线时可能立即成交。纸面复盘按方向触线，以画线价模拟入场。")
        self.direct_touch_note.setWordWrap(True)
        trigger_layout.addWidget(self.direct_touch_note)
        self.trigger_content_label = QLabel("1H形态（可多选）")
        form.addRow(self.trigger_content_label, self.trigger_content)

        self.risk_amount = self._number(self._initial.risk_amount, 0.01, 1_000_000, 2)
        self.min_space_r = self._number(self._initial.min_space_r, 0.1, 100, 2)
        self.min_space_r.setToolTip("另一条线用于核算可用空间；低于此R门槛不入场。此筛选门槛与固定止盈目标独立。")
        self.atr_period = _WheelSafeSpinBox()
        self.atr_period.setRange(2, 200)
        self.atr_period.setValue(self._initial.atr_period)
        self.entry_offset = self._number(self._initial.entry_offset_atr, 0, 10, 3)
        self.stop_offset = self._number(self._initial.stop_offset_atr, 0, 10, 3)
        self.target_buffer = self._number(self._initial.target_buffer_atr, 0, 10, 3)
        self.fee = self._number(self._initial.round_trip_fee_rate, 0, 0.1, 6)
        self.slippage = self._number(self._initial.slippage_rate * 100, 0, 10, 4)
        self.slippage.setSuffix(" %")
        self._show_legacy_slippage = self._initial.slippage_rate != 0
        self._show_target_buffer = self._initial.target_buffer_atr != 0
        self.entry_mode = QComboBox()
        self.entry_mode.addItem("触线+形态确认后立即FOK", "next_open")
        self.entry_mode.addItem("形态后再等价格突破（OKX条件单）", "breakout")
        self.entry_mode.setToolTip("FOK方式在实际触线与1H形态确认后立即提交价格受限订单；突破方式另挂OKX条件单，不能并行预挂入场线GTC。")
        self._set_combo(self.entry_mode, self._initial.entry_mode)
        self.stop_basis = QComboBox()
        self.stop_basis.addItem("信号K线高低点 + ATR偏移", "signal_candle")
        self.stop_basis.addItem("最近三根K线高低点 + ATR偏移", "three_candle")
        self.stop_basis.addItem("固定价差（相对入场线）", "fixed_distance")
        self._set_combo(self.stop_basis, self._initial.stop_basis)
        self.fixed_stop_distance = self._number(self._initial.fixed_stop_distance, 0.01, 10_000_000, 2)
        self.fee_offset_enabled = QCheckBox("计入双边 Taker 手续费")
        self.fee_offset_enabled.setChecked(self._initial.fee_offset_enabled)
        self.fee_offset_enabled.setToolTip("勾选后，保本和锁盈触发R按扣除入场与退出两侧手续费后的净R判断，保护止损价也按双边手续费计算。纸面估算使用固定Taker费率，OKX实盘/模拟盘使用对应账户费率。")
        self.resting_entry_enabled = QCheckBox("形态模式：并行挂入场线 GTC 限价单")
        self.resting_entry_enabled.setChecked(self._initial.resting_entry_enabled)
        self.resting_entry_enabled.setToolTip("形态模式下，GTC订单可能在1H形态确认前成交；成交后会停止后续形态入场并直接保护该笔成交。暂停、取消、到期或退出时会撤单并核实成交。")
        self.parallel_entry_note = QLabel("开启后，形态信号和入场线 GTC 限价单并行等待；任一路成交后停止另一路。")
        self.parallel_entry_note.setWordWrap(True)
        self.entry_trigger_mode.currentIndexChanged.connect(self._refresh_entry_mode)
        self.entry_mode.currentIndexChanged.connect(self._refresh_entry_mode)
        self.resting_entry_enabled.toggled.connect(self._refresh_entry_mode)
        self.exit_template = QComboBox()
        self.exit_template.addItem("固定止盈", "fixed_1r")
        self.exit_template.addItem("动态止盈", "lock_2r_at_3r")
        self._set_combo(
            self.exit_template,
            "fixed_1r" if self._initial.exit_template == "fixed_1r" else "lock_2r_at_3r",
        )
        self.fixed_tp_r = self._number(self._initial.fixed_tp_r, 0.1, 100, 2)
        self.fixed_tp_r.setToolTip("固定止盈价按此 R 计算；若同时启用保本，保本触发 R 应低于固定止盈 R。")
        self.break_even_enabled = QCheckBox("启用保本保护")
        self.break_even_enabled.setChecked(self._initial.break_even_enabled)
        self.be_trigger_r = self._number(self._initial.be_trigger_r, 0.1, 100, 2)
        self.time_break_even_enabled = QCheckBox("启用时间保本")
        self.time_break_even_enabled.setToolTip("持仓达到所设1H根数，且当前利润足以覆盖保本价时才尝试保本；与价格保本任一条件满足即可。")
        self.time_break_even_enabled.setChecked(self._initial.time_break_even_enabled)
        self.time_break_even_bars = _WheelSafeSpinBox()
        self.time_break_even_bars.setRange(1, 10_000)
        self.time_break_even_bars.setValue(max(1, self._initial.time_break_even_bars))
        self.lock_trigger_r = self._number(self._initial.lock_trigger_r, 0.2, 200, 2)
        self.lock_profit_r = self._number(self._initial.lock_profit_r, 0, 100, 2)
        self.lock_trail_every_r = self._number(self._initial.lock_trail_every_r, 0.1, 100, 2)
        self.lock_trail_add_r = self._number(self._initial.lock_trail_add_r, 0.1, 100, 2)
        self.be_trigger_r.setToolTip("浮盈达到此 R 后，止损移到入场价（手续费偏移按上方选项处理）。")
        self.lock_trigger_r.setToolTip("浮盈达到此 R 后，启动阶梯锁盈并移动止损到锁盈位置。")
        self.lock_profit_r.setToolTip("首次启动阶梯锁盈时，止损移动到入场价有利方向的 R 倍数。")
        self.lock_trail_every_r.setToolTip("启动锁盈后，浮盈每增加这么多 R，就再上移一次止损。")
        self.lock_trail_add_r.setToolTip("每次阶梯上移时，锁住的利润再增加这么多 R。")
        self.exit_template.currentIndexChanged.connect(self._refresh_exit_controls)
        self.break_even_enabled.toggled.connect(self._refresh_exit_controls)
        self.time_break_even_enabled.toggled.connect(self._refresh_exit_controls)
        self.fixed_tp_r.valueChanged.connect(self._refresh_exit_controls)
        self.be_trigger_r.valueChanged.connect(self._refresh_exit_controls)
        self.exit_note = QLabel()
        self.exit_note.setWordWrap(True)
        self.valid_bars = _WheelSafeSpinBox()
        self.valid_bars.setRange(1, 10_000)
        self.valid_bars.setValue(self._initial.valid_for_bars)

        for label, widget in (
            ("单笔风险预算（U）", self.risk_amount), ("最低可用空间（R）", self.min_space_r),
            ("止损基准", self.stop_basis), ("固定止损价差（U）", self.fixed_stop_distance),
            ("K线止损ATR偏移", self.stop_offset), ("止盈方式", self.exit_template),
        ):
            form.addRow(label, widget)
        form.addRow("固定止盈目标（R）", self.fixed_tp_r)
        form.addRow("", self.break_even_enabled)
        form.addRow("保本触发（R）", self.be_trigger_r)
        form.addRow("", self.time_break_even_enabled)
        form.addRow("持仓时长（1H根数）", self.time_break_even_bars)
        form.addRow("手续费偏移", self.fee_offset_enabled)
        form.addRow("", self.exit_note)
        self._lock_parameter_widgets = (
            self.lock_trigger_r, self.lock_profit_r,
            self.lock_trail_every_r, self.lock_trail_add_r,
        )
        form.addRow("锁盈启动（R）", self.lock_trigger_r)
        form.addRow("首次锁盈位置（R）", self.lock_profit_r)
        form.addRow("每增加浮盈（R）", self.lock_trail_every_r)
        form.addRow("止损再上移（R）", self.lock_trail_add_r)
        self._main_form = form
        self._refresh_exit_controls()
        form.addRow("并行限价入场", self.resting_entry_enabled)
        form.addRow("", self.parallel_entry_note)

        self.advanced_toggle = QToolButton()
        self.advanced_toggle.setText("展开高级参数")
        self.advanced_toggle.setCheckable(True)
        self.advanced_toggle.setChecked(False)
        self.advanced_panel = QWidget()
        advanced_form = QFormLayout(self.advanced_panel)
        advanced_form.setRowWrapPolicy(QFormLayout.RowWrapPolicy.WrapLongRows)
        self.atr_note = QLabel()
        self.atr_note.setWordWrap(True)
        for label, widget in (
            ("ATR周期", self.atr_period),
            ("ATR用途", self.atr_note),
            ("形态确认后执行", self.entry_mode),
            ("突破入场偏移（ATR）", self.entry_offset),
            ("目标空间缓冲（ATR）", self.target_buffer),
            ("滑点预算", self.slippage),
            ("计划有效期（1H根数）", self.valid_bars),
        ):
            advanced_form.addRow(label, widget)
        self._advanced_form = advanced_form
        self.target_buffer.setToolTip("从另一条线提供的目标空间扣除此ATR缓冲，再检查最低可用空间。")
        self.entry_offset.setToolTip("形态突破模式：做多在信号K线高点之上加偏移，做空在低点之下减偏移。")
        self.valid_bars.setToolTip("按启用监控后的1H K线计算计划有效期；不是时间保本的持仓时长。")
        self.stop_basis.currentIndexChanged.connect(self._refresh_stop_controls)
        for field in (self.stop_offset, self.entry_offset, self.target_buffer):
            field.valueChanged.connect(self._refresh_stop_controls)
        self._refresh_stop_controls()
        self.advanced_panel.setVisible(False)
        self.advanced_toggle.toggled.connect(self._toggle_advanced)
        content_layout.addWidget(self.advanced_toggle)
        content_layout.addWidget(self.advanced_panel)

        note = QLabel("每个计划只做一个方向。另一条线用于检查可用空间；纸面复盘按1H K线模拟，OKX实盘/模拟盘按交易所实际成交管理。止盈止损由交易所执行，动态保护升级需要程序持续运行。")
        note.setWordWrap(True)
        content_layout.addWidget(note)
        content_layout.addStretch()
        self.validation_message = QLabel()
        self.validation_message.setWordWrap(True)
        self.validation_message.setStyleSheet("color: #b42318;")
        layout.addWidget(self.validation_message)
        self.buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        self.buttons.button(QDialogButtonBox.StandardButton.Ok).setText("保存参数")
        self.buttons.button(QDialogButtonBox.StandardButton.Cancel).setText("取消")
        self.buttons.accepted.connect(self.accept)
        self.buttons.rejected.connect(self.reject)
        layout.addWidget(self.buttons)
        for combo in (self.entry_trigger_mode, self.entry_mode, self.stop_basis, self.exit_template):
            combo.currentIndexChanged.connect(self._refresh_validation)
        for field in (self.fixed_tp_r, self.min_space_r, self.be_trigger_r, self.lock_trigger_r, self.lock_profit_r, self.lock_trail_every_r,
                      self.lock_trail_add_r, self.stop_offset):
            field.valueChanged.connect(self._refresh_validation)
        self.patterns.itemChanged.connect(self._refresh_validation)
        self.break_even_enabled.toggled.connect(self._refresh_validation)
        self.resting_entry_enabled.toggled.connect(self._refresh_validation)
        self.fee_offset_enabled.toggled.connect(self._refresh_exit_controls)
        for field in (self.lock_trigger_r, self.lock_profit_r, self.lock_trail_every_r, self.lock_trail_add_r):
            field.valueChanged.connect(self._refresh_exit_controls)
        self._refresh_entry_mode()
        self._refresh_validation()
        available = self.screen().availableGeometry()
        self.resize(min(560, available.width()), min(760, available.height() - 80))

    def _refresh_direction_note(self) -> None:
        def price_text(value: Decimal) -> str:
            rounded = format(Decimal(format(value, ".8g")), "f")
            return rounded.rstrip("0").rstrip(".") if "." in rounded else rounded

        support = f"下方支撑 {price_text(self.support_price)}" if self.support_price is not None else "下方支撑线"
        pressure = f"上方压力 {price_text(self.pressure_price)}" if self.pressure_price is not None else "上方压力线"
        if self.direction.currentData() == "long":
            text = f"做多：{support} 是入场参考；{pressure} 用于核算目标空间和盈亏比。"
        else:
            text = f"做空：{pressure} 是入场参考；{support} 用于核算目标空间和盈亏比。"
        self.direction_note.setText(text)
        self.direction_note.setToolTip(f"支撑原价：{self.support_price}\n压力原价：{self.pressure_price}")

    def _refresh_exit_controls(self) -> None:
        dynamic = str(self.exit_template.currentData()) != "fixed_1r"
        be_enabled = self.break_even_enabled.isChecked()
        self.fixed_tp_r.setVisible(not dynamic)
        self._main_form.labelForField(self.fixed_tp_r).setVisible(not dynamic)
        if dynamic:
            start, lock = self.lock_trigger_r.value(), self.lock_profit_r.value()
            next_trigger = start + self.lock_trail_every_r.value()
            next_lock = lock + self.lock_trail_add_r.value()
            be_text = f"{self.be_trigger_r.value():g}R尝试保本；" if be_enabled else ""
            self.exit_note.setText(
                f"{be_text}{start:g}R先锁{lock:g}R，{next_trigger:g}R锁{next_lock:g}R，之后按阶梯继续推进。"
                f"远端{AREA_DYNAMIC_TAKE_PROFIT_R}R止盈随入场提交，后续只推进止损。"
            )
        elif be_enabled and self.be_trigger_r.value() >= self.fixed_tp_r.value():
            self.exit_note.setText("价格保本门槛不低于固定止盈目标，价格保本不会先触发。")
        elif be_enabled:
            self.exit_note.setText("保本触发后移动止损；固定止盈单继续保留。")
        else:
            self.exit_note.setText("固定止盈到价后退出；可单独启用保本保护。")
        if be_enabled and self.time_break_even_enabled.isChecked():
            self.exit_note.setText(self.exit_note.text() + " 时间保本：达到所设持仓时长且当前价允许保本时尝试推进。")
        if self.fee_offset_enabled.isChecked() and (dynamic or be_enabled):
            self.exit_note.setText(self.exit_note.text() + " 保本/锁盈的触发和保护位置均按双边手续费后的净R计算。")
        self.be_trigger_r.setVisible(be_enabled)
        self._main_form.labelForField(self.be_trigger_r).setVisible(be_enabled)
        self.time_break_even_enabled.setVisible(be_enabled)
        self.time_break_even_bars.setVisible(be_enabled and self.time_break_even_enabled.isChecked())
        self._main_form.labelForField(self.time_break_even_bars).setVisible(
            be_enabled and self.time_break_even_enabled.isChecked()
        )
        for widget in self._lock_parameter_widgets:
            widget.setVisible(dynamic)
            label = self._main_form.labelForField(widget)
            if label is not None:
                label.setVisible(dynamic)

    def _toggle_advanced(self, checked: bool) -> None:
        self.advanced_panel.setVisible(checked)
        self.advanced_toggle.setText("收起高级参数" if checked else "展开高级参数")

    def _refresh_stop_controls(self) -> None:
        fixed = self.stop_basis.currentData() == "fixed_distance"
        self._set_row_visible(self._main_form, self.fixed_stop_distance, fixed)
        self._set_row_visible(self._main_form, self.stop_offset, not fixed)
        direct = self.entry_trigger_mode.currentData() == "direct_touch"
        self.stop_basis.setItemText(0, "最近已收盘K线高低点 + ATR偏移" if direct else "信号K线高低点 + ATR偏移")
        atr_config = replace(
            self._initial, entry_trigger_mode=str(self.entry_trigger_mode.currentData()),
            entry_mode=str(self.entry_mode.currentData()), stop_basis=str(self.stop_basis.currentData()),
            stop_offset_atr=Decimal(str(self.stop_offset.value())),
            entry_offset_atr=Decimal(str(self.entry_offset.value())),
            target_buffer_atr=Decimal(str(self.target_buffer.value())),
        )
        needs_atr = atr_config.requires_atr()
        self._set_row_visible(self._advanced_form, self.atr_period, needs_atr)
        if not needs_atr:
            text = "当前入场、止损与目标缓冲不使用ATR。入场要求实际触线，无需等待ATR历史数据预热。"
        elif direct and fixed:
            text = "固定价差止损不用ATR；当前ATR用于非零目标空间缓冲，不会扩大触线范围。"
        elif direct:
            text = "ATR用于K线止损偏移及非零目标缓冲，不扩大触线范围；模拟盘GTC止损按启用时已收盘K线计算并冻结。"
        else:
            text = "形态信号要求实际触线。ATR仅用于K线止损偏移、非零目标缓冲或突破入场偏移。"
        self.atr_note.setText(text)

    @staticmethod
    def _set_row_visible(form: QFormLayout, widget: QWidget, visible: bool) -> None:
        widget.setVisible(visible)
        label = form.labelForField(widget)
        if label is not None:
            label.setVisible(visible)

    def _refresh_validation(self) -> None:
        if not hasattr(self, "validation_message"):
            return
        dynamic = self.exit_template.currentData() != "fixed_1r"
        pattern = self.entry_trigger_mode.currentData() == "pattern"
        gtc = not pattern or (self.entry_mode.currentData() == "next_open" and self.resting_entry_enabled.isChecked())
        error = ""
        if pattern and not self._selected_patterns():
            error = "请至少选择一种1H形态，或改用价格触发。"
        elif dynamic and self.break_even_enabled.isChecked() and self.lock_trigger_r.value() <= self.be_trigger_r.value():
            error = "锁盈启动R必须大于保本触发R。"
        elif dynamic and self.lock_profit_r.value() >= self.lock_trigger_r.value():
            error = "首次锁盈位置必须低于锁盈启动R。"
        if not error:
            try:
                self.config()
            except ValueError:
                error = "参数组合无效，请检查止盈、保本和止损设置。"
        warning = ""
        if not error and gtc and self.stop_basis.currentData() != "fixed_distance" and self.stop_offset.value() <= 0:
            warning = "模拟盘GTC入场要求K线止损ATR偏移大于0；当前值可保存为纸面计划，启用模拟盘前需调整。"
        elif not error and dynamic and self.lock_trail_add_r.value() > self.lock_trail_every_r.value():
            warning = "止损上移快于浮盈增长，后续候选止损可能越过当前价而暂缓推进；请核对阶梯步长。"
        elif not error and dynamic and self.lock_trigger_r.value() >= float(AREA_DYNAMIC_TAKE_PROFIT_R):
            warning = "锁盈启动不早于远端15R止盈，可能先止盈退出；请核对锁盈起点。"
        elif not error and not dynamic and self.fixed_tp_r.value() > self.min_space_r.value():
            warning = "固定止盈目标高于最低空间门槛，可能越过对侧线；请核对是否需要提高最低可用空间。"
        self.validation_message.setStyleSheet("color: #b42318;" if error else "color: #8a5300;")
        self.validation_message.setText(error or warning)
        self.validation_message.setVisible(bool(error or warning))
        self.buttons.button(QDialogButtonBox.StandardButton.Ok).setEnabled(not error)

    def accept(self) -> None:
        self._refresh_validation()
        if self.buttons.button(QDialogButtonBox.StandardButton.Ok).isEnabled():
            super().accept()

    def config(self) -> AreaShapeConfig:
        result = AreaShapeConfig(
            direction=str(self.direction.currentData()),
            risk_amount=Decimal(str(self.risk_amount.value())),
            min_space_r=Decimal(str(self.min_space_r.value())),
            atr_period=self.atr_period.value(),
            area_half_width_atr=self._initial.area_half_width_atr,
            entry_offset_atr=Decimal(str(self.entry_offset.value())),
            stop_offset_atr=Decimal(str(self.stop_offset.value())),
            target_buffer_atr=Decimal(str(self.target_buffer.value())),
            round_trip_fee_rate=AREA_TAKER_FEE_RATE if self.fee_offset_enabled.isChecked() else Decimal("0"),
            slippage_rate=Decimal(str(self.slippage.value())) / 100,
            fixed_tp_r=Decimal(str(self.fixed_tp_r.value())),
            break_even_enabled=self.break_even_enabled.isChecked(),
            be_trigger_r=Decimal(str(self.be_trigger_r.value())),
            time_break_even_enabled=(self.break_even_enabled.isChecked()
                                     and self.time_break_even_enabled.isChecked()),
            time_break_even_bars=(self.time_break_even_bars.value()
                                  if self.break_even_enabled.isChecked() and self.time_break_even_enabled.isChecked()
                                  else 0),
            lock_trigger_r=Decimal(str(self.lock_trigger_r.value())),
            lock_profit_r=Decimal(str(self.lock_profit_r.value())),
            lock_trail_every_r=Decimal(str(self.lock_trail_every_r.value())),
            lock_trail_add_r=Decimal(str(self.lock_trail_add_r.value())),
            entry_mode="next_open" if self.entry_trigger_mode.currentData() == "direct_touch" else str(self.entry_mode.currentData()),
            entry_trigger_mode=str(self.entry_trigger_mode.currentData()),
            stop_basis=str(self.stop_basis.currentData()),
            fixed_stop_distance=Decimal(str(self.fixed_stop_distance.value())),
            fee_offset_enabled=self.fee_offset_enabled.isChecked(),
            resting_entry_enabled=(self.resting_entry_enabled.isChecked()
                                   and self.entry_trigger_mode.currentData() == "pattern"
                                   and self.entry_mode.currentData() == "next_open"),
            exit_template=str(self.exit_template.currentData()),
            trigger_price_type="last",
            valid_for_bars=self.valid_bars.value(),
            signal_patterns=self._selected_patterns(),
        )
        result.validate()
        return result

    def _refresh_patterns(self) -> None:
        if self._pattern_direction is not None:
            self._pattern_selections[self._pattern_direction] = set(self._selected_patterns())
        direction = str(self.direction.currentData())
        available_ids = {pattern_id for _label, pattern_id, side in _PATTERNS if side in {direction, "either"}}
        if direction in self._pattern_selections:
            old = self._pattern_selections[direction]
        else:
            old = set(self._initial.signal_patterns).intersection(available_ids) or available_ids
        self._pattern_direction = direction
        self.patterns.clear()
        for label, pattern_id, side in _PATTERNS:
            if side not in {direction, "either"}:
                continue
            item = QListWidgetItem(label, self.patterns)
            item.setData(Qt.ItemDataRole.UserRole, pattern_id)
            item.setFlags(item.flags() | Qt.ItemFlag.ItemIsUserCheckable)
            item.setCheckState(Qt.CheckState.Checked if pattern_id in old else Qt.CheckState.Unchecked)
        self._refresh_validation()

    def _refresh_entry_mode(self) -> None:
        direct = self.entry_trigger_mode.currentData() == "direct_touch"
        parallel_supported = not direct and self.entry_mode.currentData() == "next_open"
        self.patterns.setVisible(not direct)
        self.direct_touch_note.setVisible(direct)
        self.trigger_content_label.setText("价格触发" if direct else "1H形态（可多选）")
        self.patterns.setEnabled(not direct)
        self.resting_entry_enabled.setEnabled(parallel_supported)
        self.entry_mode.setEnabled(not direct)
        self._set_row_visible(self._main_form, self.resting_entry_enabled, parallel_supported)
        self.parallel_entry_note.setVisible(parallel_supported)
        self._set_row_visible(self._advanced_form, self.entry_mode, not direct)
        self._set_row_visible(self._advanced_form, self.entry_offset, not direct and not parallel_supported)
        self._set_row_visible(self._advanced_form, self.target_buffer, self._show_target_buffer)
        self._set_row_visible(self._advanced_form, self.slippage, self._show_legacy_slippage)
        if self.resting_entry_enabled.isChecked():
            self.parallel_entry_note.setText("形态信号与GTC并行等待；GTC可在形态出现前成交，任一路成交后停止另一路入场。")
        else:
            self.parallel_entry_note.setText("仅等待实际触线与所选形态确认，不会预挂入场线GTC限价单。")
        self._refresh_stop_controls()
        self._refresh_validation()

    def _selected_patterns(self) -> tuple[str, ...]:
        selected = []
        if not hasattr(self, "patterns"):
            return tuple()
        for index in range(self.patterns.count()):
            item = self.patterns.item(index)
            if item.checkState() == Qt.CheckState.Checked:
                selected.append(str(item.data(Qt.ItemDataRole.UserRole)))
        return tuple(selected)

    @staticmethod
    def _number(value: Decimal, minimum: float, maximum: float, decimals: int) -> QDoubleSpinBox:
        field = _WheelSafeDoubleSpinBox()
        field.setRange(minimum, maximum)
        field.setDecimals(decimals)
        field.setValue(float(value))
        return field

    @staticmethod
    def _set_combo(combo: QComboBox, value: object) -> None:
        index = combo.findData(value)
        if index >= 0:
            combo.setCurrentIndex(index)
