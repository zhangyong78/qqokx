from __future__ import annotations

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
    QSpinBox,
    QToolButton,
    QVBoxLayout,
    QWidget,
)

from okx_quant.area_shape_trade import AreaShapeConfig


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


class AreaShapeConfigDialog(QDialog):
    def __init__(self, config: AreaShapeConfig | None = None, parent=None) -> None:  # noqa: ANN001
        super().__init__(parent)
        self.setWindowTitle("区域形态参数")
        self.setMinimumWidth(360)
        self._initial = config or AreaShapeConfig()
        layout = QVBoxLayout(self)
        form = QFormLayout()
        layout.addLayout(form)

        self.direction = QComboBox()
        self.direction.addItem("做多：支撑区等待看涨反转", "long")
        self.direction.addItem("做空：压力区等待看跌反转", "short")
        self._set_combo(self.direction, self._initial.direction)
        self.direction.currentIndexChanged.connect(self._refresh_patterns)
        form.addRow("方向", self.direction)

        self.patterns = QListWidget()
        self.patterns.setMaximumHeight(126)
        self._refresh_patterns()
        form.addRow("1H形态（可多选）", self.patterns)

        self.risk_amount = self._number(self._initial.risk_amount, 0.01, 1_000_000, 2)
        self.min_space_r = self._number(self._initial.min_space_r, 0.1, 100, 2)
        self.atr_period = QSpinBox()
        self.atr_period.setRange(2, 200)
        self.atr_period.setValue(self._initial.atr_period)
        self.entry_offset = self._number(self._initial.entry_offset_atr, 0, 10, 3)
        self.stop_offset = self._number(self._initial.stop_offset_atr, 0, 10, 3)
        self.target_buffer = self._number(self._initial.target_buffer_atr, 0, 10, 3)
        self.fee = self._number(self._initial.round_trip_fee_rate, 0, 0.1, 6)
        self.slippage = self._number(self._initial.slippage_rate, 0, 0.1, 6)
        self.entry_mode = QComboBox()
        self.entry_mode.addItem("触线+形态确认后立即FOK", "next_open")
        self.entry_mode.addItem("形态后再等价格突破（OKX条件单）", "breakout")
        self.entry_mode.setToolTip("首选方式由本地监控线触碰和1H形态，确认后立即向OKX模拟盘提交带滑点上限的FOK；突破方式才会另挂OKX原生条件单。")
        self._set_combo(self.entry_mode, self._initial.entry_mode)
        self.resting_entry_enabled = QCheckBox("监控开始时同时在入场线挂 GTC 限价单")
        self.resting_entry_enabled.setChecked(self._initial.resting_entry_enabled)
        self.resting_entry_enabled.setToolTip("GTC订单可能在1H形态确认前成交；若GTC先成交，会取消后续FOK/条件入场并直接保护该笔成交。暂停、取消、到期或退出时会撤单并核实成交。")
        self.exit_template = QComboBox()
        self.exit_template.addItem("固定止盈", "fixed_1r")
        self.exit_template.addItem("保本后锁盈", "lock_2r_at_3r")
        self.exit_template.addItem("保本后锁盈（高档）", "lock_3r_at_4r")
        self._set_combo(self.exit_template, self._initial.exit_template)
        self.fixed_tp_r = self._number(self._initial.fixed_tp_r, 0.1, 100, 2)
        self.be_trigger_r = self._number(self._initial.be_trigger_r, 0.1, 100, 2)
        self.lock_trigger_r = self._number(self._initial.lock_trigger_r, 0.2, 200, 2)
        self.lock_profit_r = self._number(self._initial.lock_profit_r, 0, 100, 2)
        self.exit_template.currentIndexChanged.connect(self._apply_exit_preset)
        self.valid_bars = QSpinBox()
        self.valid_bars.setRange(1, 10_000)
        self.valid_bars.setValue(self._initial.valid_for_bars)

        for label, widget in (
            ("单笔风险预算（U）", self.risk_amount), ("最低可用空间（R）", self.min_space_r),
            ("离场模板", self.exit_template),
        ):
            form.addRow(label, widget)

        self.advanced_toggle = QToolButton()
        self.advanced_toggle.setText("展开高级参数")
        self.advanced_toggle.setCheckable(True)
        self.advanced_toggle.setChecked(False)
        self.advanced_panel = QWidget()
        advanced_form = QFormLayout(self.advanced_panel)
        for label, widget in (
            ("ATR周期", self.atr_period),
            ("入场偏移（ATR）", self.entry_offset), ("止损偏移（ATR）", self.stop_offset),
            ("目标缓冲（ATR）", self.target_buffer), ("纸面往返手续费估算", self.fee),
            ("滑点上限", self.slippage), ("入场方式", self.entry_mode),
            ("并行入场", self.resting_entry_enabled),
            ("固定止盈R", self.fixed_tp_r),
            ("保本触发R", self.be_trigger_r), ("锁盈触发R", self.lock_trigger_r),
            ("净锁盈R", self.lock_profit_r), ("计划有效1H根数", self.valid_bars),
        ):
            advanced_form.addRow(label, widget)
        self.advanced_panel.setVisible(False)
        self.advanced_toggle.toggled.connect(self._toggle_advanced)
        layout.addWidget(self.advanced_toggle)
        layout.addWidget(self.advanced_panel)

        note = QLabel("做多等支撑线、做空等压力线；模拟盘默认监控开始就在线位挂GTC，同时保留触线+所选已收盘1H形态后的价格受限FOK。GTC可先于形态成交；任一路径有成交就取消另一入场并保护实际成交，部分成交需人工接管。另一条线用于空间核算，默认不足2R跳过。")
        note.setWordWrap(True)
        layout.addWidget(note)
        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def _apply_exit_preset(self) -> None:
        mode = str(self.exit_template.currentData())
        values = {"lock_2r_at_3r": (2, 3, 2), "lock_3r_at_4r": (2, 4, 3)}.get(mode)
        if values is not None:
            self.be_trigger_r.setValue(values[0])
            self.lock_trigger_r.setValue(values[1])
            self.lock_profit_r.setValue(values[2])

    def _toggle_advanced(self, checked: bool) -> None:
        self.advanced_panel.setVisible(checked)
        self.advanced_toggle.setText("收起高级参数" if checked else "展开高级参数")

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
            round_trip_fee_rate=Decimal(str(self.fee.value())),
            slippage_rate=Decimal(str(self.slippage.value())),
            fixed_tp_r=Decimal(str(self.fixed_tp_r.value())),
            be_trigger_r=Decimal(str(self.be_trigger_r.value())),
            lock_trigger_r=Decimal(str(self.lock_trigger_r.value())),
            lock_profit_r=Decimal(str(self.lock_profit_r.value())),
            entry_mode=str(self.entry_mode.currentData()),
            resting_entry_enabled=self.resting_entry_enabled.isChecked(),
            exit_template=str(self.exit_template.currentData()),
            trigger_price_type="last",
            valid_for_bars=self.valid_bars.value(),
            signal_patterns=self._selected_patterns(),
        )
        result.validate()
        return result

    def _refresh_patterns(self) -> None:
        old = set(self._selected_patterns()) if hasattr(self, "patterns") and self.patterns.count() else set(self._initial.signal_patterns)
        direction = str(self.direction.currentData())
        self.patterns.clear()
        available = []
        for label, pattern_id, side in _PATTERNS:
            if side not in {direction, "either"}:
                continue
            available.append((label, pattern_id))
            item = QListWidgetItem(label, self.patterns)
            item.setData(Qt.ItemDataRole.UserRole, pattern_id)
            item.setFlags(item.flags() | Qt.ItemFlag.ItemIsUserCheckable)
            item.setCheckState(Qt.CheckState.Checked if pattern_id in old else Qt.CheckState.Unchecked)
        if not any(pattern_id in old for _label, pattern_id in available):
            for index in range(self.patterns.count()):
                self.patterns.item(index).setCheckState(Qt.CheckState.Checked)

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
        field = QDoubleSpinBox()
        field.setRange(minimum, maximum)
        field.setDecimals(decimals)
        field.setValue(float(value))
        return field

    @staticmethod
    def _set_combo(combo: QComboBox, value: object) -> None:
        index = combo.findData(value)
        if index >= 0:
            combo.setCurrentIndex(index)
